use crate::{host::Shared, parser::MAX_BODY_BYTES, *};
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{header, Method, Request, StatusCode},
    response::{IntoResponse, Response},
};
use b4_contract::*;
use b4_runtime::{approval::ApprovalError, Store, StoreError, ThreadSeed, ThreadView};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::{
    sync::{atomic::Ordering, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRequest {
    #[serde(default)]
    metadata: Map<String, Value>,
}

pub(crate) fn json_response(status: StatusCode, value: Value) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        value.to_string(),
    )
        .into_response()
}
pub(crate) fn error(status: StatusCode, code: &'static str) -> Response {
    json_response(
        status,
        json!({"error":{"kind":"request_error","message":code,"details":{"code":code}}}),
    )
}
#[track_caller]
pub(crate) fn unavailable<S>(shared: &Shared<S>) -> Response {
    let location = std::panic::Location::caller();
    if std::env::var_os("B4_WORKER_DIAGNOSTIC").is_some() {
        eprintln!(
            "host_unready caller={}:{}",
            location.file(),
            location.line()
        );
    }
    shared.fault.store(true, Ordering::Release);
    error(StatusCode::SERVICE_UNAVAILABLE, "host_unready")
}
pub(crate) fn store_error<S>(shared: &Shared<S>, e: StoreError) -> Response {
    match e {
        StoreError::ThreadNotFound => error(StatusCode::NOT_FOUND, "resource_unavailable"),
        StoreError::ThreadBusy => error(StatusCode::CONFLICT, "thread_busy"),
        StoreError::RevisionConflict | StoreError::ThreadExists => {
            error(StatusCode::CONFLICT, "resource_changed")
        }
        _ => unavailable(shared),
    }
}
pub(crate) fn approval_failure_is_infrastructure(error: &ApprovalError) -> bool {
    match error {
        ApprovalError::Store(
            StoreError::ThreadNotFound
            | StoreError::ExecutionNotFound
            | StoreError::ExecutionNotActive
            | StoreError::ThreadBusy
            | StoreError::RevisionConflict
            | StoreError::OwnerConflict
            | StoreError::ThreadExists,
        ) => false,
        ApprovalError::Store(_)
        | ApprovalError::Runtime(_)
        | ApprovalError::Kernel(_)
        | ApprovalError::RandomUnavailable
        | ApprovalError::DurabilityRequired => true,
        _ => false,
    }
}
pub(crate) fn approval_error<S>(shared: &Shared<S>, e: ApprovalError) -> Response {
    if approval_failure_is_infrastructure(&e) {
        return unavailable(shared);
    }
    match e {
        ApprovalError::Unauthorized => error(StatusCode::NOT_FOUND, "resource_unavailable"),
        ApprovalError::Store(StoreError::OwnerConflict) => {
            error(StatusCode::CONFLICT, "recovery_required")
        }
        ApprovalError::Store(StoreError::ExecutionNotFound | StoreError::ExecutionNotActive) => {
            error(StatusCode::CONFLICT, "execution_changed")
        }
        ApprovalError::Store(e) => store_error(shared, e),
        ApprovalError::NotStablePark => error(StatusCode::CONFLICT, "resume_required"),
        ApprovalError::RouteMismatch => error(StatusCode::CONFLICT, "route_mismatch"),
        ApprovalError::RecoveryNotNeeded => error(StatusCode::CONFLICT, "recovery_not_needed"),
        ApprovalError::RecoveryDefinitionMismatch => {
            error(StatusCode::CONFLICT, "recovery_definition_unavailable")
        }
        ApprovalError::InterruptSetMismatch => {
            error(StatusCode::CONFLICT, "interrupt_set_mismatch")
        }
        ApprovalError::UnsupportedAlways => error(StatusCode::BAD_REQUEST, "always_unsupported"),
        ApprovalError::MissingToken => error(StatusCode::BAD_REQUEST, "grant_required"),
        ApprovalError::Expired => error(StatusCode::CONFLICT, "grant_expired"),
        ApprovalError::Consumed => error(StatusCode::CONFLICT, "grant_consumed"),
        ApprovalError::GrantUnavailable => error(StatusCode::CONFLICT, "grant_unavailable"),
        ApprovalError::ProposalTooLarge | ApprovalError::InvalidProposal => {
            error(StatusCode::BAD_REQUEST, "approval_proposal_limit")
        }
        _ => error(StatusCode::FORBIDDEN, "grant_invalid"),
    }
}
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
pub(crate) fn timestamp(ms: u64) -> Option<String> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .ok()?
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}
pub(crate) fn thread_projection(view: &ThreadView) -> Option<Value> {
    let status = view.execution.as_ref().map(|e| e.snapshot.status);
    let status = match status {
        Some(ExecutionStatus::WaitingForApproval) => "interrupted",
        Some(
            ExecutionStatus::Accepted
            | ExecutionStatus::Running
            | ExecutionStatus::CancelRequested
            | ExecutionStatus::RecoveryBlocked,
        ) => "busy",
        _ => "idle",
    };
    let updated = view
        .execution
        .as_ref()
        .map_or(view.thread.created_at_ms, |e| {
            e.snapshot.updated_at_ms.max(view.thread.created_at_ms)
        });
    Some(
        json!({"thread_id":view.thread.id,"created_at":timestamp(view.thread.created_at_ms)?,"updated_at":timestamp(updated)?,"metadata":view.thread.metadata,"status":status}),
    )
}
fn state_projection(view: &ThreadView) -> Option<Value> {
    let s = &view.execution.as_ref()?.snapshot;
    Some(json!({
        "config":{"configurable":{"thread_id":view.thread.id,"checkpoint_id":s.execution_id,"checkpoint_ns":""}},
        "created_at":timestamp(s.updated_at_ms)?,
        "metadata":{"source":"native_scripted","revision":s.revision,"route":s.definition.route,"execution_revision":s.definition.revision},
        "next":[], "parent_config":null, "values":s.values
    }))
}
pub(crate) fn valid_principal(p: &Principal) -> bool {
    [&p.application, &p.tenant, &p.principal]
        .into_iter()
        .all(|s| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control))
}
fn decode_id(value: &str) -> Option<ThreadId> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut cursor = value.bytes();
    while let Some(byte) = cursor.next() {
        if byte == b'%' {
            let high = char::from(cursor.next()?).to_digit(16)?;
            let low = char::from(cursor.next()?).to_digit(16)?;
            bytes.push((high * 16 + low) as u8);
        } else {
            bytes.push(byte);
        }
    }
    ThreadId::new(String::from_utf8(bytes).ok()?).ok()
}

pub(crate) async fn endpoint<S: Store + 'static>(
    State(shared): State<Arc<Shared<S>>>,
    request: Request<Body>,
) -> Response {
    let observation = shared.diagnostics.start(request.method());
    // Move the original capture into counted handling; observation retains only
    // its independent diagnostics state across await and response construction.
    let mut response = endpoint_inner(State(shared), request).await;
    observation.finish(&mut response);
    response
}

async fn endpoint_inner<S: Store + 'static>(
    State(shared): State<Arc<Shared<S>>>,
    request: Request<Body>,
) -> Response {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    if path == "/healthz" && method == Method::GET {
        return json_response(StatusCode::OK, json!({"status":"ok"}));
    }
    if path == "/readyz" && method == Method::GET {
        let Some(shared) = crate::lifecycle::OwnedShared::register(shared) else {
            return json_response(StatusCode::SERVICE_UNAVAILABLE, json!({"status":"unready"}));
        };
        let s = shared.clone();
        let ready = s
            .spawn(move |s| {
                if s.fault.load(Ordering::Acquire) {
                    return false;
                }
                match s.store.readiness() {
                    Ok(()) => true,
                    Err(_) => {
                        s.fault.store(true, Ordering::Release);
                        false
                    }
                }
            })
            .await
            .unwrap_or(false)
            && shared.supervisor.is_serving()
            && !shared.fault.load(Ordering::Acquire);
        return json_response(
            if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            json!({"status":if ready {"ready"} else {"unready"}}),
        );
    }
    let Some(shared) = crate::lifecycle::OwnedShared::register(shared) else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
    };
    let headers = request.headers().clone();
    let s = shared.clone();
    let principal = match s.spawn(move |s| s.auth.authenticate(&headers)).await {
        Ok(Ok(p)) if valid_principal(&p) => p,
        Ok(_) => return error(StatusCode::UNAUTHORIZED, "authentication_required"),
        Err(_) => return unavailable(&shared),
    };
    if !crate::application::scope_allows(&shared, &principal) {
        return error(StatusCode::NOT_FOUND, "resource_unavailable");
    }
    if path == "/b4/capabilities" && method == Method::GET {
        return crate::capabilities::read(shared, principal).await;
    }
    if shared.fault.load(Ordering::Acquire) {
        return error(StatusCode::SERVICE_UNAVAILABLE, "host_unready");
    }
    let parts: Vec<_> = path.split('/').collect();
    let operation = match (method.as_str(), parts.as_slice()) {
        ("POST", ["", "threads"]) => Operation::CreateThread,
        ("GET", ["", "threads", _]) => Operation::ReadThread,
        ("DELETE", ["", "threads", _]) => Operation::DeleteThread,
        ("GET", ["", "threads", _, "state"]) => Operation::ReadState,
        ("GET", ["", "threads", _, "pending_interrupts"]) => Operation::ReadPending,
        ("POST", ["", "threads", _, "runs", "wait"]) => Operation::RunWait,
        ("POST", ["", "threads", _, "runs", "stream"]) => Operation::RunStream,
        ("POST", ["", "threads", _, "resume"]) => Operation::Resume,
        ("POST", ["", "threads", _, "cancel"]) => Operation::CancelRun,
        ("GET", ["", "threads", _, "runs", "stream"]) => Operation::Attach,
        _ => Operation::Unsupported,
    };
    let id = if parts.len() >= 3 && parts[1] == "threads" {
        match decode_id(parts[2]) {
            Some(id) => Some(id),
            None => return error(StatusCode::BAD_REQUEST, "invalid_thread_id"),
        }
    } else {
        None
    };
    if operation == Operation::Attach {
        return crate::attachment::attach(shared, principal, id.unwrap()).await;
    }
    let parsed = if matches!(
        operation,
        Operation::CreateThread | Operation::RunWait | Operation::RunStream | Operation::Resume
    ) {
        let bytes = match to_bytes(request.into_body(), MAX_BODY_BYTES).await {
            Ok(b) => b,
            Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large"),
        };
        if operation == Operation::CreateThread && bytes.iter().all(u8::is_ascii_whitespace) {
            json!({})
        } else {
            match parse_json(&bytes) {
                Ok(v) => v,
                Err(code) => return error(StatusCode::BAD_REQUEST, code),
            }
        }
    } else {
        Value::Null
    };
    let create = if operation == Operation::CreateThread {
        match serde_json::from_value::<CreateRequest>(parsed.clone()) {
            Ok(c) => Some(c),
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_request"),
        }
    } else {
        None
    };
    let run = if matches!(operation, Operation::RunWait | Operation::RunStream) {
        match serde_json::from_value::<RunRequest>(parsed.clone()) {
            Ok(r) => Some(r),
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_request"),
        }
    } else {
        None
    };
    let resume = if operation == Operation::Resume {
        match serde_json::from_value::<ResumeRequest>(parsed) {
            Ok(r) => Some(r),
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_request"),
        }
    } else {
        None
    };
    if let Some(run) = &run {
        if serde_json::to_vec(&run.input).map_or(true, |b| b.len() > shared.config.max_input_bytes)
            || (shared.permission_uses.is_some()
                && !crate::permissions::input_within_profile(&run.input))
        {
            return error(StatusCode::BAD_REQUEST, "input_limit");
        }
    }
    let route = run
        .as_ref()
        .and_then(|r| shared.routes.get(&r.route))
        .or_else(|| resume.as_ref().and_then(|r| shared.routes.get(&r.route)))
        .cloned();
    let s = shared.clone();
    let thread_id = id.clone();
    let requested_route = route.as_ref().map(|r| RouteProvenance {
        route: r.definition.route.clone(),
        revision: r.definition.revision.clone(),
    });
    let resume_route = resume.as_ref().map(|request| request.route.clone());
    let p = principal.clone();
    let authorized = s
        .spawn(move |s| {
            let view = thread_id
                .as_ref()
                .map(|id| s.store.thread_view(id))
                .transpose()
                .inspect_err(|error| {
                    crate::lifecycle::record_store_failure(s, error);
                })?
                .flatten();
            // Resolve matching Resume provenance from the original ordinary
            // view inside counted ownership. Only pure metadata leaves this job.
            let recorded_resume = view
                .as_ref()
                .and_then(|view| view.execution.as_ref())
                .filter(|execution| {
                    s.application.is_some()
                        && resume_route.as_ref() == Some(&execution.snapshot.definition.route)
                });
            let resume_supported = recorded_resume.map(|execution| {
                crate::route::recorded(
                    s.application.as_ref(),
                    &s.routes,
                    &execution.snapshot.definition,
                )
                .is_some()
            });
            let requested_route = recorded_resume
                .map(|execution| RouteProvenance {
                    route: execution.snapshot.definition.route.clone(),
                    revision: execution.snapshot.definition.revision.clone(),
                })
                .or(requested_route);
            // Capture the original locally owned turn before policy can wait or
            // admit a successor. A later cancellation never looks it up by name.
            let captured_run = if operation == Operation::CancelRun {
                match s.claims.lock() {
                    Ok(claims) => {
                        thread_id
                            .as_ref()
                            .and_then(|id| claims.capture(id))
                            .filter(|run| {
                                view.as_ref()
                                    .is_some_and(|view| run.control.identity.matches(view))
                            })
                    }
                    Err(_) => {
                        s.fault.store(true, Ordering::Release);
                        return Err(StoreError::Unavailable("claim_registry".into()));
                    }
                }
            } else {
                None
            };
            let context = ResourceContext {
                operation,
                thread_id,
                origin: view.as_ref().and_then(|v| v.thread.origin.clone()),
                stored_route: view.as_ref().and_then(|v| v.execution.as_ref()).map(|e| {
                    RouteProvenance {
                        route: e.snapshot.definition.route.clone(),
                        revision: e.snapshot.definition.revision.clone(),
                    }
                }),
                requested_route: requested_route.clone(),
            };
            let allowed = s.policy.authorize(&p, &context)
                && view.as_ref().is_none_or(|v| v.thread.origin.is_some());
            Ok::<_, StoreError>((
                view,
                allowed,
                captured_run,
                requested_route,
                resume_supported,
            ))
        })
        .await;
    let (view, allowed, captured_run, _requested_route, resume_supported) = match authorized {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return store_error(&shared, e),
        Err(_) => return unavailable(&shared),
    };
    if !allowed {
        return error(
            if id.is_some() {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::FORBIDDEN
            },
            "resource_unavailable",
        );
    }
    if (matches!(
        operation,
        Operation::DeleteThread | Operation::RunWait | Operation::RunStream | Operation::CancelRun
    ) || shared.approval.is_some()
        && matches!(
            operation,
            Operation::RunWait | Operation::RunStream | Operation::Resume | Operation::ReadPending
        ))
        && view.as_ref().is_some_and(|view| {
            view.thread.origin.as_ref().is_none_or(|origin| {
                origin.application != principal.application
                    || origin.tenant != principal.tenant
                    || origin.principal != principal.principal
            })
        })
    {
        return error(StatusCode::NOT_FOUND, "resource_unavailable");
    }
    if shared.permission_uses.is_some()
        && ((operation == Operation::DeleteThread && shared.permission_deletion.is_none())
            || (operation == Operation::CancelRun && shared.permission_cancellation.is_none()))
    {
        return error(
            StatusCode::NOT_IMPLEMENTED,
            "permission_operation_unsupported",
        );
    }
    if let Some(create) = create {
        let s = shared.clone();
        let seed = ThreadSeed {
            id: ThreadId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            created_at_ms: now_ms(),
            metadata: create.metadata,
            origin: Some(b4_runtime::ThreadOrigin {
                application: principal.application,
                tenant: principal.tenant,
                principal: principal.principal,
            }),
        };
        return s
            .spawn(move |s| {
                let Some(_lease) = s.try_admit() else {
                    return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
                };
                match s.store.create_thread_with(seed) {
                    Ok(thread) => thread_projection(&ThreadView {
                        thread,
                        execution: None,
                    })
                    .map(|v| json_response(StatusCode::OK, v))
                    .unwrap_or_else(|| unavailable(s)),
                    Err(e) => store_error(s, e),
                }
            })
            .await
            .unwrap_or_else(|_| unavailable(&shared));
    }
    if let Some(run) = run {
        let Some(route) = route else {
            return error(
                StatusCode::NOT_FOUND,
                if shared.application.is_some() {
                    "resource_unavailable"
                } else {
                    "route_not_found"
                },
            );
        };
        if let Some(approval) = &shared.approval {
            if let Err(e) = approval.validate_input(&route, &run.input) {
                return approval_error(&shared, e);
            }
        }
        return crate::worker::launch(shared, principal, id.unwrap(), view, run, route, operation)
            .await;
    }
    if operation == Operation::Unsupported && id.is_none() {
        return error(StatusCode::NOT_IMPLEMENTED, "unsupported_operation");
    }
    if operation == Operation::DeleteThread {
        return match view {
            Some(view) => crate::deletion::delete(shared, principal, view).await,
            None => crate::deletion::empty(),
        };
    }
    let Some(view) = view else {
        return error(StatusCode::NOT_FOUND, "resource_unavailable");
    };
    if operation == Operation::CancelRun {
        return crate::cancellation::cancel(shared, principal, id.unwrap(), captured_run).await;
    }
    if let Some(resume) = resume {
        if shared.approval.is_none() {
            return error(StatusCode::NOT_IMPLEMENTED, "unsupported_operation");
        }
        let Some(current) = &view.execution else {
            return approval_error(&shared, ApprovalError::NotStablePark);
        };
        if resume_supported == Some(false) {
            return approval_error(&shared, ApprovalError::RouteMismatch);
        }
        let Some(route) = crate::route::recorded(
            shared.application.as_ref(),
            &shared.routes,
            &current.snapshot.definition,
        ) else {
            return approval_error(&shared, ApprovalError::RouteMismatch);
        };
        return crate::worker::launch_resume(shared, principal, view, resume, route).await;
    }
    if matches!(operation, Operation::ReadState | Operation::ReadPending)
        && view.execution.as_ref().is_some_and(|e| {
            (e.snapshot.owner_generation != shared.store.owner_generation()
                && !b4_runtime::releases_slot(e.snapshot.status))
                || e.snapshot.status == ExecutionStatus::RecoveryBlocked
                || (shared.approval.is_none()
                    && (e.snapshot.status == ExecutionStatus::WaitingForApproval
                        || e.snapshot
                            .definition
                            .batches
                            .iter()
                            .flatten()
                            .any(|task| task.requires_approval)))
        })
    {
        return error(StatusCode::NOT_IMPLEMENTED, "unsupported_execution_state");
    }
    match operation {
        Operation::ReadThread => thread_projection(&view)
            .map(|v| json_response(StatusCode::OK, v))
            .unwrap_or_else(|| unavailable(&shared)),
        Operation::ReadState => {
            if view.execution.is_none() {
                return error(StatusCode::NOT_FOUND, "checkpoint_unavailable");
            }
            state_projection(&view)
                .map(|v| json_response(StatusCode::OK, v))
                .unwrap_or_else(|| unavailable(&shared))
        }
        Operation::ReadPending => {
            if let Some(approval) = shared.approval.clone() {
                match shared
                    .clone()
                    .spawn(move |s| {
                        let result = approval.pending(&principal, Operation::ReadPending, &view);
                        if let Err(error) = &result {
                            crate::lifecycle::record_approval_failure(s, error);
                        }
                        result
                    })
                    .await
                {
                    Ok(Ok(pending)) => json_response(StatusCode::OK, json!({"interrupts":pending})),
                    Ok(Err(ApprovalError::NotStablePark)) => {
                        error(StatusCode::CONFLICT, "resume_in_progress")
                    }
                    Ok(Err(e)) => approval_error(&shared, e),
                    Err(_) => unavailable(&shared),
                }
            } else {
                json_response(StatusCode::OK, json!({"interrupts":[]}))
            }
        }
        _ => error(StatusCode::NOT_IMPLEMENTED, "unsupported_operation"),
    }
}
