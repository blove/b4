use crate::{
    control::{RunControl, RunIdentity, RunMetadata},
    host::Shared,
    transport::{
        approval_error, approval_failure_is_infrastructure, error, json_response, now_ms,
        store_error, unavailable,
    },
    *,
};
use axum::{http::StatusCode, response::Response};
use b4_contract::*;
use b4_runtime::{
    approval::ApprovalError, drive_execution, ControlledNativeOutcome, DriverConfig, DriverError,
    DriverFailure, DriverHost, EffectInvocation, EffectOutcome, NativeOutcome, PreparationError,
    PreparedEffect, Runtime, RuntimeError, Store, StoredExecution, ThreadSeed, ThreadView,
};
use serde_json::{json, Map, Value};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit};
pub(crate) struct Terminal {
    pub wait_status: StatusCode,
    pub wait: Value,
    pub done: Value,
}

pub(crate) enum WorkerError {
    Runtime(RuntimeError),
    Approval(ApprovalError),
    Rejected(&'static str),
    InvalidApplicationInput,
    Draining,
}
impl From<RuntimeError> for WorkerError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<b4_kernel::KernelError> for WorkerError {
    fn from(error: b4_kernel::KernelError) -> Self {
        Self::Runtime(RuntimeError::Kernel(error))
    }
}
impl From<ApprovalError> for WorkerError {
    fn from(error: ApprovalError) -> Self {
        Self::Approval(error)
    }
}
enum Work {
    New {
        view: Option<ThreadView>,
        run: RunRequest,
    },
    Resume {
        view: ThreadView,
        resume: ResumeRequest,
    },
    Recover {
        view: ThreadView,
        target: RecoveryTarget,
    },
}

pub(crate) struct RecoveryLaunch {
    pub admission: RecoveryAdmission,
    pub presentation: Option<Response>,
}
pub(crate) async fn launch_recovery<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    target: RecoveryTarget,
    view: ThreadView,
    route: crate::route::RegisteredRoute,
    presentation: bool,
) -> Result<RecoveryLaunch, RecoveryAdmissionError> {
    let permit = shared
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| RecoveryAdmissionError::new(RecoveryRefusal::CapacityExhausted, None))?;
    let claim = crate::claim::ThreadClaim::acquire(shared.claims.clone(), target.thread_id.clone())
        .map_err(|reason| {
            let reason = match reason {
                crate::claim::ClaimError::Busy => RecoveryRefusal::ThreadBusy,
                crate::claim::ClaimError::Poisoned => {
                    shared.fault.store(true, Ordering::Release);
                    RecoveryRefusal::Unavailable
                }
            };
            RecoveryAdmissionError::new(reason, None)
        })?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (terminal_tx, _terminal_rx) = oneshot::channel();
    let (viewer_tx, viewer_rx) = crate::viewer::channel(
        shared.config.viewer_queue_frames,
        shared.config.viewer_queue_bytes,
    );
    spawn_owned(
        shared.clone(),
        principal,
        target.thread_id.clone(),
        route,
        Operation::Recover,
        Work::Recover { view, target },
        permit,
        claim,
        ReadySender::Recovery(ready_tx),
        terminal_tx,
        viewer_tx,
        presentation,
    );
    let admission = ready_rx.await.unwrap_or_else(|_| {
        shared.fault.store(true, Ordering::Release);
        Err(RecoveryAdmissionError::new(
            RecoveryRefusal::Unavailable,
            None,
        ))
    })?;
    let response = if presentation
        && !matches!(
            admission.disposition,
            RecoveryDisposition::Blocked | RecoveryDisposition::AttemptLimit
        ) {
        Some(crate::viewer::response(
            viewer_rx,
            shared.config.heartbeat_ms,
        ))
    } else {
        None
    };
    Ok(RecoveryLaunch {
        admission,
        presentation: response,
    })
}

pub(crate) async fn launch_resume<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    view: ThreadView,
    resume: ResumeRequest,
    route: crate::route::RegisteredRoute,
) -> Response {
    let id = view.thread.id.clone();
    launch_owned(
        shared,
        principal,
        id,
        route,
        Operation::Resume,
        Work::Resume { view, resume },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn launch<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    id: ThreadId,
    view: Option<ThreadView>,
    run: RunRequest,
    route: crate::route::RegisteredRoute,
    operation: Operation,
) -> Response {
    launch_owned(
        shared,
        principal,
        id,
        route,
        operation,
        Work::New { view, run },
    )
    .await
}

async fn launch_owned<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    id: ThreadId,
    route: crate::route::RegisteredRoute,
    operation: Operation,
    work: Work,
) -> Response {
    let permit = match shared.capacity.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "capacity_exhausted"),
    };
    let claim = match crate::claim::ThreadClaim::acquire(shared.claims.clone(), id.clone()) {
        Ok(claim) => claim,
        Err(crate::claim::ClaimError::Busy) => return error(StatusCode::CONFLICT, "thread_busy"),
        Err(crate::claim::ClaimError::Poisoned) => return unavailable(&shared),
    };
    let (ready_tx, ready_rx) = oneshot::channel();
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let (viewer_tx, viewer_rx) = crate::viewer::channel(
        shared.config.viewer_queue_frames,
        shared.config.viewer_queue_bytes,
    );
    spawn_owned(
        shared.clone(),
        principal,
        id,
        route,
        operation,
        work,
        permit,
        claim,
        ReadySender::Ordinary(ready_tx),
        terminal_tx,
        viewer_tx,
        true,
    );
    match ready_rx.await {
        Ok(Ok(())) => {}
        Ok(Err(WorkerError::Runtime(RuntimeError::Store(e)))) => return store_error(&shared, e),
        Ok(Err(WorkerError::Approval(e))) => return approval_error(&shared, e),
        Ok(Err(WorkerError::Draining)) => {
            return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining")
        }
        Ok(Err(WorkerError::Rejected(code))) => return error(StatusCode::CONFLICT, code),
        Ok(Err(WorkerError::InvalidApplicationInput)) => {
            return error(StatusCode::BAD_REQUEST, "invalid_application_input")
        }
        _ => return unavailable(&shared),
    }
    if streaming(operation) {
        return crate::viewer::response(viewer_rx, shared.config.heartbeat_ms);
    }
    match terminal_rx.await {
        Ok(Ok(terminal)) => json_response(terminal.wait_status, terminal.wait),
        Ok(Err(WorkerError::Approval(error))) => approval_error(&shared, error),
        Ok(Err(WorkerError::Draining)) => error(StatusCode::SERVICE_UNAVAILABLE, "host_draining"),
        Ok(Err(WorkerError::Rejected(code))) => error(StatusCode::CONFLICT, code),
        Ok(Err(WorkerError::InvalidApplicationInput)) => {
            error(StatusCode::BAD_REQUEST, "invalid_application_input")
        }
        _ => unavailable(&shared),
    }
}
enum ReadySender {
    Ordinary(oneshot::Sender<Result<(), WorkerError>>),
    Recovery(oneshot::Sender<Result<RecoveryAdmission, RecoveryAdmissionError>>),
}
impl ReadySender {
    fn success<S>(
        self,
        shared: &Shared<S>,
        adoption: Option<RecoveryAdoption>,
        disposition: RecoveryDisposition,
    ) {
        match self {
            Self::Ordinary(tx) => {
                let _ = tx.send(Ok(()));
            }
            Self::Recovery(tx) => {
                let result = adoption
                    .map(|adoption| RecoveryAdmission {
                        adoption,
                        disposition,
                    })
                    .ok_or_else(|| {
                        shared.fault.store(true, Ordering::Release);
                        RecoveryAdmissionError::new(RecoveryRefusal::Unavailable, None)
                    });
                let _ = tx.send(result);
            }
        }
    }
    fn failure<S>(
        self,
        shared: &Shared<S>,
        error: WorkerError,
        adoption: Option<RecoveryAdoption>,
    ) {
        match self {
            Self::Ordinary(tx) => {
                let _ = tx.send(Err(error));
            }
            Self::Recovery(tx) => {
                let _ = tx.send(Err(crate::recovery::mapping::worker(
                    shared, error, adoption,
                )));
            }
        }
    }
    fn panicked(self, adoption: Option<RecoveryAdoption>) {
        if let Self::Recovery(tx) = self {
            let _ = tx.send(Err(RecoveryAdmissionError::new(
                RecoveryRefusal::Unavailable,
                adoption,
            )));
        }
    }
}
#[allow(clippy::too_many_arguments)]
fn spawn_owned<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    id: ThreadId,
    route: crate::route::RegisteredRoute,
    operation: Operation,
    work: Work,
    permit: OwnedSemaphorePermit,
    claim: crate::claim::ThreadClaim,
    ready_tx: ReadySender,
    terminal_tx: oneshot::Sender<Result<Terminal, WorkerError>>,
    viewer_tx: crate::viewer::ViewerSender,
    presentation: bool,
) {
    let is_resume = matches!(&work, Work::Resume { .. });
    let is_recovered = matches!(&work, Work::Recover { .. });
    shared.spawn(move |s| {
        let _permit: OwnedSemaphorePermit = permit;
        let claim = claim;
        let mut ready_tx = Some(ready_tx);
        let mut terminal_tx = Some(terminal_tx);
        let mut control: Option<Arc<RunControl>> = None;
        let mut adoption: Option<RecoveryAdoption> = None;
        let mut disposition = RecoveryDisposition::Runnable;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let runtime = Runtime::new(s.store.clone());
            let _lease;
            let mut continuation = crate::approval::AcceptedContinuation::Ordinary;
            let accepted = match work {
                Work::New { view, run } => {
                    preflight_new(s, &principal, operation, &id, &view, &route)?;
                    if let Some(application) = route.application() {
                        application
                            .validate_input(&route.definition, &run.input)
                            .map_err(|reason| match reason {
                                b4_runtime::ApplicationInputError::InvalidInput => {
                                    WorkerError::InvalidApplicationInput
                                }
                                b4_runtime::ApplicationInputError::DefinitionUnavailable => {
                                    WorkerError::Rejected("route_mismatch")
                                }
                            })?;
                        // The validator is trusted blocking application code. This
                        // producer owns its guards until it returns, then renews
                        // authority and the exact captured target before mutation.
                        preflight_new(s, &principal, operation, &id, &view, &route)?;
                    }
                    _lease = s.try_admit().ok_or(WorkerError::Draining)?;
                    let thread = match view {
                        Some(v) => v.thread,
                        None => s
                            .store
                            .create_thread_with(ThreadSeed {
                                id: id.clone(),
                                created_at_ms: now_ms(),
                                metadata: Map::new(),
                                origin: Some(b4_runtime::ThreadOrigin {
                                    application: principal.application.clone(),
                                    tenant: principal.tenant.clone(),
                                    principal: principal.principal.clone(),
                                }),
                            })
                            .map_err(RuntimeError::Store)?,
                    };
                    let initial = b4_kernel::new_execution(
                        ExecutionId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
                        TurnId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
                        s.store.owner_generation(),
                        route.definition.clone(),
                        Map::from_iter([("input".into(), run.input)]),
                        TransitionFacts { now_ms: now_ms() },
                    )?;
                    runtime.admit_fenced(
                        &id,
                        &thread.generation,
                        thread.resource_revision,
                        initial,
                    )?
                }
                Work::Resume { view, resume } => {
                    _lease = s.try_admit().ok_or(WorkerError::Draining)?;
                    let accepted = s
                        .approval
                        .as_ref()
                        .ok_or(ApprovalError::InvalidContext)?
                        .accept(&principal, &view, resume)?;
                    continuation = accepted.continuation;
                    accepted.current
                }
                Work::Recover { view, target } => {
                    crate::recovery::preflight(s, &principal, &target, Some(&view))?;
                    _lease = s.try_admit().ok_or(WorkerError::Draining)?;
                    let recovered = s
                        .approval
                        .as_ref()
                        .ok_or(ApprovalError::InvalidContext)?
                        .recover(&principal, &target, &route)?;
                    // Capture confirmed bridge-return evidence outside every
                    // subsequent fallible or panicking publication operation.
                    adoption = Some(RecoveryAdoption {
                        thread_id: recovered.thread_id.clone(),
                        thread_generation: target.thread_generation.clone(),
                        execution_id: recovered.snapshot.execution_id.clone(),
                        turn_id: recovered.snapshot.turn_id.clone(),
                        execution_fence: (&recovered.snapshot).into(),
                    });
                    if recovered.thread_id != target.thread_id
                        || recovered.snapshot.execution_id != target.execution_id
                        || recovered.snapshot.owner_generation != s.store.owner_generation()
                        || recovered.snapshot.definition != route.definition
                    {
                        return Err(WorkerError::Runtime(RuntimeError::EffectFailed(
                            "recovery_return_identity".into(),
                        )));
                    }
                    disposition = match recovered.snapshot.status {
                        ExecutionStatus::Accepted | ExecutionStatus::Running => {
                            RecoveryDisposition::Runnable
                        }
                        ExecutionStatus::WaitingForApproval => RecoveryDisposition::Parked,
                        ExecutionStatus::Completed => RecoveryDisposition::Completed,
                        ExecutionStatus::Cancelled => RecoveryDisposition::Cancelled,
                        ExecutionStatus::Failed => RecoveryDisposition::Failed,
                        ExecutionStatus::RecoveryBlocked => RecoveryDisposition::Blocked,
                        ExecutionStatus::CancelRequested => {
                            return Err(WorkerError::Runtime(RuntimeError::EffectFailed(
                                "recovery_state".into(),
                            )))
                        }
                    };
                    let dispatches = recovered
                        .snapshot
                        .tasks
                        .iter()
                        .filter(|task| {
                            task.status == TaskStatus::Ready
                                && (!task.spec.requires_approval || task.approved)
                        })
                        .count();
                    let remaining = recovered
                        .snapshot
                        .definition
                        .max_attempts
                        .saturating_sub(recovered.snapshot.attempts_used)
                        as usize;
                    if disposition == RecoveryDisposition::Runnable && dispatches > remaining {
                        disposition = RecoveryDisposition::AttemptLimit;
                    }
                    if matches!(
                        disposition,
                        RecoveryDisposition::Blocked | RecoveryDisposition::AttemptLimit
                    ) || (!presentation && disposition != RecoveryDisposition::Runnable)
                    {
                        if let Some(tx) = ready_tx.take() {
                            tx.success(s, adoption.clone(), disposition);
                        }
                        return Ok(None);
                    }
                    recovered
                }
            };
            let view = s
                .store
                .thread_view(&accepted.thread_id)
                .map_err(RuntimeError::Store)?
                .ok_or(RuntimeError::Store(b4_runtime::StoreError::ThreadNotFound))?;
            if is_recovered
                && (view.thread.id != accepted.thread_id
                    || view.thread.generation != adoption.as_ref().unwrap().thread_generation
                    || view.execution.as_ref() != Some(&accepted)
                    || view.thread.active_execution_id.as_ref()
                        != Some(&accepted.snapshot.execution_id)
                    || view.thread.origin.as_ref()
                        != Some(&b4_runtime::ThreadOrigin {
                            application: principal.application.clone(),
                            tenant: principal.tenant.clone(),
                            principal: principal.principal.clone(),
                        }))
            {
                return Err(WorkerError::Runtime(RuntimeError::EffectFailed(
                    "recovery_publication_identity".into(),
                )));
            }
            let original_origin = view
                .thread
                .origin
                .clone()
                .ok_or(ApprovalError::Unauthorized)?;
            let active = Arc::new(RunControl::new(
                RunIdentity {
                    thread_id: accepted.thread_id.clone(),
                    thread_generation: view.thread.generation,
                    execution_id: accepted.snapshot.execution_id.clone(),
                    turn_id: accepted.snapshot.turn_id.clone(),
                    owner_generation: accepted.snapshot.owner_generation,
                },
                s.fault.clone(),
                terminal_tx.take().unwrap(),
                if presentation && streaming(operation) {
                    Some(viewer_tx)
                } else {
                    None
                },
                RunMetadata {
                    anchor: accepted.snapshot.execution_id.clone(),
                    values: Value::Object(accepted.snapshot.values.clone()),
                    input: accepted
                        .snapshot
                        .values
                        .get("input")
                        .cloned()
                        .unwrap_or(Value::Null),
                    started_at: crate::transport::timestamp(now_ms())
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                    resume: is_resume,
                    recovered: is_recovered,
                },
                &s.config,
            ));
            control = Some(active.clone());
            if matches!(
                accepted.snapshot.status,
                ExecutionStatus::Accepted | ExecutionStatus::Running
            ) {
                claim
                    .publish(active.clone())
                    .map_err(|_| RuntimeError::EffectFailed("claim_registry".into()))?;
            }
            if let Some(tx) = ready_tx.take() {
                tx.success(s, adoption.clone(), disposition);
            }
            execute(
                s,
                &runtime,
                accepted,
                &route,
                &principal,
                operation,
                &active,
                original_origin,
                presentation,
                !is_resume && !is_recovered,
                continuation,
            )
            .map(Some)
        }));
        match outcome {
            Ok(Ok(Some(terminal))) => {
                control.as_ref().unwrap().finish(terminal);
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => {
                let admission_error = ready_tx.is_some();
                if std::env::var_os("B4_WORKER_DIAGNOSTIC").is_some() {
                    eprintln!(
                        "worker error operation={operation:?} admission_error={admission_error} error={}",
                        match &e {
                            WorkerError::Runtime(error) => format!("runtime: {error:?}"),
                            WorkerError::Approval(error) => format!("approval: {error:?}"),
                            WorkerError::Rejected(code) => format!("rejected: {code}"),
                            WorkerError::InvalidApplicationInput => "invalid_application_input".into(),
                            WorkerError::Draining => "draining".into(),
                        }
                    );
                }
                let benign = match &e {
                    WorkerError::Rejected(_)
                    | WorkerError::InvalidApplicationInput
                    | WorkerError::Draining => true,
                    WorkerError::Approval(error) => !approval_failure_is_infrastructure(error),
                    WorkerError::Runtime(error) => {
                        admission_error
                            && matches!(
                                error,
                                RuntimeError::Store(
                                    b4_runtime::StoreError::ThreadBusy
                                        | b4_runtime::StoreError::RevisionConflict
                                        | b4_runtime::StoreError::ThreadExists
                                        | b4_runtime::StoreError::ThreadNotFound
                                )
                            )
                    }
                };
                if !benign {
                    s.fault.store(true, Ordering::Release);
                    if let Some(control) = &control {
                        control.fault();
                    }
                }
                if let Some(tx) = ready_tx.take() {
                    tx.failure(s, e, adoption.clone());
                } else if let Some(control) = &control {
                    control.reject(e);
                }
            }
            Err(panic) => {
                s.fault.store(true, Ordering::Release);
                if let Some(tx) = ready_tx.take() {
                    tx.panicked(adoption);
                }
                crate::lifecycle::dispose_panic(panic);
                if let Some(control) = &control {
                    control.fault();
                }
            }
        }
    });
}

fn streaming(operation: Operation) -> bool {
    matches!(
        operation,
        Operation::RunStream | Operation::Resume | Operation::Recover
    )
}
fn preflight_new<S: Store>(
    shared: &Shared<S>,
    principal: &Principal,
    operation: Operation,
    id: &ThreadId,
    captured: &Option<ThreadView>,
    route: &crate::route::RegisteredRoute,
) -> Result<(), ApprovalError> {
    // Renew ordinary authority before any implicit thread creation or execution
    // admission. The transaction enforces generation and resource revision.
    if !crate::application::scope_allows(shared, principal) {
        return Err(ApprovalError::Unauthorized);
    }
    let current = shared.store.thread_view(id)?;
    let origin = b4_runtime::ThreadOrigin {
        application: principal.application.clone(),
        tenant: principal.tenant.clone(),
        principal: principal.principal.clone(),
    };
    let context = ResourceContext {
        operation,
        thread_id: Some(id.clone()),
        origin: current.as_ref().and_then(|view| view.thread.origin.clone()),
        stored_route: current
            .as_ref()
            .and_then(|view| view.execution.as_ref())
            .map(|execution| RouteProvenance {
                route: execution.snapshot.definition.route.clone(),
                revision: execution.snapshot.definition.revision.clone(),
            }),
        requested_route: Some(RouteProvenance {
            route: route.definition.route.clone(),
            revision: route.definition.revision.clone(),
        }),
    };
    if !shared.policy.authorize(principal, &context)
        || current
            .as_ref()
            .is_some_and(|view| view.thread.origin.as_ref() != Some(&origin))
    {
        return Err(ApprovalError::Unauthorized);
    }
    let renewed = if route.application().is_some() {
        shared.store.thread_view(id)?
    } else {
        current
    };
    if &renewed != captured {
        return Err(b4_runtime::StoreError::RevisionConflict.into());
    }
    Ok(())
}
fn facts(current: &StoredExecution) -> TransitionFacts {
    TransitionFacts {
        now_ms: now_ms().max(current.snapshot.updated_at_ms),
    }
}
fn expected_cancel_race(error: &WorkerError) -> bool {
    matches!(
        error,
        WorkerError::Runtime(RuntimeError::Store(
            b4_runtime::StoreError::RevisionConflict
        )) | WorkerError::Runtime(RuntimeError::Kernel(
            b4_kernel::KernelError::InvalidTransition(
                "execution_status" | "task_not_authorized_ready"
            )
        )) | WorkerError::Approval(ApprovalError::Unauthorized)
            | WorkerError::Approval(ApprovalError::Store(
                b4_runtime::StoreError::RevisionConflict
            ))
            | WorkerError::Approval(ApprovalError::Kernel(
                b4_kernel::KernelError::InvalidTransition(
                    "execution_status" | "task_not_authorized_ready"
                )
            ))
    )
}
#[allow(clippy::too_many_arguments)]
fn execute<S: Store>(
    shared: &Shared<S>,
    runtime: &Runtime<Arc<S>>,
    accepted: StoredExecution,
    route: &crate::route::RegisteredRoute,
    principal: &Principal,
    operation: Operation,
    control: &Arc<RunControl>,
    original_origin: b4_runtime::ThreadOrigin,
    presentation: bool,
    new_run: bool,
    continuation: crate::approval::AcceptedContinuation,
) -> Result<Terminal, WorkerError> {
    let port = ScriptDriverHost {
        shared,
        runtime,
        route,
        principal,
        operation,
        control,
        native_streaming: crate::native_stream::selected(
            shared.native_streaming,
            operation,
            new_run,
            &continuation,
        ),
        invocation_context: route
            .application()
            .map(|application| b4_runtime::InvocationContext {
                thread_id: control.identity.thread_id.clone(),
                thread_generation: control.identity.thread_generation.clone(),
                origin: original_origin,
                application_revision: application.identity().revision.clone(),
                execution_revision: route.definition.revision.clone(),
                policy_revision: shared
                    .approval
                    .as_ref()
                    .unwrap()
                    .policy_revision()
                    .to_owned(),
            }),
    };
    let mut ordinary = Some(accepted);
    let mut completed = None;
    if matches!(
        continuation,
        crate::approval::AcceptedContinuation::OriginalAlways
    ) {
        let bridge = shared
            .permission_uses
            .as_ref()
            .ok_or(ApprovalError::InvalidContext)?;
        completed = Some(bridge.execute_original_always(
            principal,
            operation,
            ordinary.take().unwrap(),
            control,
            &shared.task_capacity,
        )?);
    } else if new_run {
        if let Some(bridge) = &shared.permission_uses {
            match bridge.execute(
                principal,
                operation,
                ordinary.take().unwrap(),
                control,
                &shared.task_capacity,
            )? {
                crate::permissions::PermissionUseResult::NotMatched(started) => {
                    ordinary = Some(started)
                }
                crate::permissions::PermissionUseResult::Completed(current) => {
                    completed = Some(current)
                }
            }
        }
    }
    let current = match completed {
        Some(current) => current,
        None => drive_execution(
            runtime,
            ordinary.unwrap(),
            &port,
            &shared.task_capacity,
            DriverConfig {
                max_parallel_tasks: shared.config.max_tasks_per_execution,
                completion_attempts: 8,
                max_outcome_bytes: shared.config.max_output_bytes,
            },
        )
        .map_err(|failure| driver_error(failure, operation))?,
    };
    if current.snapshot.status == ExecutionStatus::WaitingForApproval {
        if presentation && streaming(operation) {
            let view = shared
                .store
                .thread_view(&current.thread_id)
                .map_err(RuntimeError::Store)?
                .ok_or(RuntimeError::Store(b4_runtime::StoreError::ThreadNotFound))?;
            let interrupts = shared
                .approval
                .as_ref()
                .ok_or(ApprovalError::InvalidContext)?
                .pending(principal, Operation::ReadPending, &view)?;
            for interrupt in interrupts {
                control.publish("interrupt", interrupt);
            }
        }
        return Ok(bounded_terminal(
            shared,
            Terminal {
                wait_status: StatusCode::OK,
                wait: Value::Null,
                done: json!({"output":null}),
            },
        )?);
    }
    if current.snapshot.status == ExecutionStatus::Cancelled {
        return Ok(bounded_terminal(
            shared,
            Terminal {
                wait_status: StatusCode::OK,
                wait: json!({"cancelled":true}),
                done: json!({"output":{"cancelled":true}}),
            },
        )?);
    }
    if current.snapshot.status == ExecutionStatus::Failed {
        // Expected terminals come only from the committed snapshot. Runtime
        // errors and uncertain callbacks never become application messages.
        let failure = current
            .snapshot
            .failure
            .as_ref()
            .ok_or_else(|| RuntimeError::EffectFailed("committed_failure_missing".into()))?;
        return Ok(bounded_terminal(
            shared,
            Terminal {
                wait_status: StatusCode::INTERNAL_SERVER_ERROR,
                wait: json!({"error":{"kind":"execution_error","message":failure}}),
                done: json!({"output":{"error":failure}}),
            },
        )?);
    }
    if current.snapshot.status != ExecutionStatus::Completed {
        return Err(RuntimeError::EffectFailed("unexpected_terminal_state".into()).into());
    }
    let last = current.snapshot.tasks.last();
    let output_kind = last.and_then(|t| route.output_kind(&t.spec.effect_id));
    let terminal = if last.is_some_and(|task| task.status == TaskStatus::Denied) {
        Terminal {
            wait_status: StatusCode::OK,
            wait: json!({"denied":true}),
            done: json!({"output":{"denied":true}}),
        }
    } else {
        match output_kind {
            Some(b4_runtime::OutputKind::Void) | None => Terminal {
                wait_status: StatusCode::OK,
                wait: Value::Null,
                done: json!({}),
            },
            _ => {
                let output = last
                    .and_then(|t| t.result.as_ref())
                    .map(|r| r.output.clone())
                    .ok_or_else(|| RuntimeError::EffectFailed("committed_output_missing".into()))?;
                Terminal {
                    wait_status: StatusCode::OK,
                    wait: output.clone(),
                    done: json!({"output":output}),
                }
            }
        }
    };
    Ok(bounded_terminal(shared, terminal)?)
}

struct ScriptDriverHost<'a, S> {
    shared: &'a Shared<S>,
    runtime: &'a Runtime<Arc<S>>,
    route: &'a crate::route::RegisteredRoute,
    principal: &'a Principal,
    operation: Operation,
    control: &'a Arc<RunControl>,
    invocation_context: Option<b4_runtime::InvocationContext>,
    native_streaming: bool,
}
impl<S> ScriptDriverHost<'_, S> {
    fn invocation_matches(&self, call: &EffectInvocation) -> bool {
        call.execution_id == self.control.identity.execution_id
            && call.turn_id == self.control.identity.turn_id
            && call.owner_generation == self.control.identity.owner_generation
            && self
                .route
                .definition
                .batches
                .iter()
                .flatten()
                .any(|task| task.id == call.task_id && task.effect_id == call.effect_id)
    }
}
impl<S: Store> DriverHost for ScriptDriverHost<'_, S> {
    type Error = WorkerError;
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        facts(current)
    }
    fn attempt_id(&self) -> AttemptId {
        AttemptId::new(uuid::Uuid::new_v4().to_string()).unwrap()
    }
    fn prepare_dispatch(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<Self::Error>> {
        if let Some(approval) = &self.shared.approval {
            approval
                .prepare_dispatch(self.principal, self.operation, current, task_id, attempt_id)
                .map_err(|failure| PreparationError {
                    cause: WorkerError::Approval(failure.cause),
                    claimed: failure.claimed,
                })
        } else {
            self.runtime
                .prepare_dispatch(
                    &current.snapshot.execution_id,
                    b4_runtime::ExecutionFence::from(&current.snapshot),
                    task_id,
                    attempt_id,
                    facts(current),
                )
                .map_err(|failure| PreparationError {
                    cause: WorkerError::Runtime(failure.cause),
                    claimed: failure.claimed,
                })
        }
    }
    fn park(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
    ) -> Result<StoredExecution, Self::Error> {
        self.shared
            .approval
            .as_ref()
            .ok_or(ApprovalError::InvalidContext)?
            .park(self.principal, self.operation, current, task_id)
            .map_err(WorkerError::Approval)
    }
    fn invoke(&self, call: &EffectInvocation) -> Result<EffectOutcome, String> {
        if !self.invocation_matches(call) {
            return Err("invocation_identity".into());
        }
        let admitted = if self.native_streaming && self.route.application().is_some() {
            self.control
                .enter_native_callback(crate::native_stream::tool_call(
                    call,
                    self.shared.config.max_output_bytes,
                ))
        } else {
            self.control.enter_callback()
        }
        .map_err(str::to_owned)?;
        if !admitted {
            return Ok(EffectOutcome::Cancelled);
        }
        if let Some(application) = self.route.application() {
            let context = self
                .invocation_context
                .as_ref()
                .ok_or("invocation_context")?;
            let observer = self.control.native_cancellation_observer();
            let outcome = if self.native_streaming {
                let mut sink = crate::native_stream::Sink {
                    control: self.control,
                    cap: self.shared.config.max_output_bytes,
                };
                application.invoke_streaming(
                    &self.route.definition,
                    call,
                    context,
                    &observer,
                    &mut sink,
                )
            } else {
                application.invoke_controlled(&self.route.definition, call, context, &observer)
            }
            .map_err(|_| "native_invocation_failed".to_owned())?;
            match outcome {
                ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(result)) => {
                    Ok(EffectOutcome::Succeeded(result))
                }
                ControlledNativeOutcome::Settled(NativeOutcome::Failed(reason)) => {
                    Ok(EffectOutcome::Failed(reason))
                }
                ControlledNativeOutcome::Cancelled
                    if observer.cancellation_requested() && self.invocation_matches(call) =>
                {
                    Ok(EffectOutcome::Cancelled)
                }
                ControlledNativeOutcome::Cancelled => Err("native_invocation_failed".into()),
            }
        } else {
            let effect = self
                .route
                .script_effect(&call.effect_id)
                .ok_or("fixture_missing")?;
            scripted_outcome(self.shared, effect, call)
        }
    }
    fn after_commit(
        &self,
        call: &EffectInvocation,
        outcome: &EffectOutcome,
        committed: &StoredExecution,
    ) -> Result<(), Self::Error> {
        if self.native_streaming
            && self.route.application().is_some()
            && matches!(outcome, EffectOutcome::Succeeded(_))
            && self.invocation_matches(call)
            && self.control.identity.matches_execution(committed)
        {
            if let Some(result) = committed
                .snapshot
                .tasks
                .iter()
                .find(|task| {
                    task.spec.id == call.task_id
                        && task.spec.effect_id == call.effect_id
                        && task.attempt_id.as_ref() == Some(&call.attempt_id)
                        && task.status == TaskStatus::Succeeded
                })
                .and_then(|task| task.result.as_ref())
            {
                self.control
                    .native_result(crate::native_stream::tool_result(
                        call,
                        result,
                        self.shared.config.max_output_bytes,
                    ));
            }
        }
        if matches!(outcome, EffectOutcome::Succeeded(_)) {
            if let Some(ScriptEffect::Chunks { chunks, .. }) =
                self.route.script_effect(&call.effect_id)
            {
                for chunk in chunks {
                    self.control.publish("chunk", chunk.clone());
                }
            }
        }
        Ok(())
    }
    fn cancellation_committed(&self) -> bool {
        self.control.committed()
    }
    fn expected_cancel_race(&self, error: &Self::Error) -> bool {
        expected_cancel_race(error)
    }
    fn host_error_is_fault(&self, error: &Self::Error) -> bool {
        match error {
            WorkerError::Rejected(_)
            | WorkerError::InvalidApplicationInput
            | WorkerError::Draining => false,
            WorkerError::Approval(ApprovalError::Kernel(
                b4_kernel::KernelError::BudgetExceeded,
            ))
            | WorkerError::Runtime(RuntimeError::Kernel(b4_kernel::KernelError::BudgetExceeded))
                if self.operation == Operation::Recover =>
            {
                false
            }
            WorkerError::Approval(error) => approval_failure_is_infrastructure(error),
            WorkerError::Runtime(_) => true,
        }
    }
    fn on_fault(&self) {
        self.control.fault();
    }
}
fn driver_error(failure: DriverFailure<WorkerError>, operation: Operation) -> WorkerError {
    match failure.errors.into_iter().next() {
        Some(DriverError::Host(WorkerError::Approval(ApprovalError::Kernel(
            b4_kernel::KernelError::BudgetExceeded,
        ))))
        | Some(DriverError::Host(WorkerError::Runtime(RuntimeError::Kernel(
            b4_kernel::KernelError::BudgetExceeded,
        ))))
        | Some(DriverError::Runtime(RuntimeError::Kernel(
            b4_kernel::KernelError::BudgetExceeded,
        ))) if operation == Operation::Recover => WorkerError::Rejected("recovery_attempt_limit"),
        Some(DriverError::Host(error)) => error,
        Some(DriverError::Runtime(error)) => WorkerError::Runtime(error),
        _ => WorkerError::Runtime(RuntimeError::EffectFailed("shared_driver_fault".into())),
    }
}

fn scripted_outcome<S>(
    shared: &Shared<S>,
    effect: &ScriptEffect,
    call: &EffectInvocation,
) -> Result<EffectOutcome, String> {
    let input = call
        .input
        .get("input")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let result = match effect {
        ScriptEffect::Result(result) | ScriptEffect::Chunks { result, .. } => result.clone(),
        ScriptEffect::Delayed {
            duration_ms,
            result,
        } => {
            std::thread::sleep(Duration::from_millis(*duration_ms));
            result.clone()
        }
        ScriptEffect::Input => TaskResult {
            output: input,
            writes: vec![],
        },
        ScriptEffect::ProbeInput => {
            let kind = match &input {
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Number(_) => "number",
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            TaskResult {
                output: json!({"input":input,"kind":kind}),
                writes: vec![],
            }
        }
        ScriptEffect::Void => TaskResult {
            output: Value::Null,
            writes: vec![],
        },
        ScriptEffect::Failure(reason) => return Ok(EffectOutcome::Failed(reason.clone())),
    };
    if serde_json::to_vec(&result)
        .map_err(|_| "result_encoding")?
        .len()
        > shared.config.max_output_bytes
    {
        return Err("result_limit".into());
    }
    Ok(EffectOutcome::Succeeded(result))
}

fn bounded_terminal<S>(shared: &Shared<S>, terminal: Terminal) -> Result<Terminal, RuntimeError> {
    for envelope in [&terminal.wait, &terminal.done] {
        if serde_json::to_vec(envelope)
            .map_err(|_| RuntimeError::EffectFailed("terminal_encoding".into()))?
            .len()
            > shared.config.max_output_bytes
        {
            return Err(RuntimeError::EffectFailed("terminal_limit".into()));
        }
    }
    Ok(terminal)
}
