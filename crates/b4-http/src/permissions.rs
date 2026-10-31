use crate::{
    ApprovalHostConfig, Authenticator, ConfigError, HostConfig, HostLimits, HttpHost, Operation,
    Principal, ResourcePolicy,
};
use b4_runtime::permissions::{PermissionCatalog, PermissionStore};
use std::sync::Arc;

#[derive(Clone)]
pub struct PermissionUseHostConfig {
    pub catalog: Arc<PermissionCatalog>,
    pub limits: HostLimits,
    pub approval: ApprovalHostConfig,
}

impl PermissionUseHostConfig {
    pub fn new(catalog: Arc<PermissionCatalog>, approval: ApprovalHostConfig) -> Self {
        Self {
            catalog,
            limits: HostLimits {
                max_active_runs: 1,
                max_concurrent_tasks: 1,
                max_tasks_per_execution: 1,
                max_input_bytes: 4096,
                ..HostLimits::default()
            },
            approval,
        }
    }
}

/// Opt-in HTTP permission approvals for one native route, one batch and one
/// required-approval Pure task, with one active run/task and a 65,536-byte
/// outcome bound. Supports original Always creation/completion and later reuse;
/// general permission lifecycle, cancellation, deletion and recovery are excluded.
#[derive(Clone)]
pub struct PermissionHostConfig {
    pub catalog: Arc<PermissionCatalog>,
    pub limits: HostLimits,
    pub approval: ApprovalHostConfig,
}
impl PermissionHostConfig {
    pub fn new(catalog: Arc<PermissionCatalog>, approval: ApprovalHostConfig) -> Self {
        let config = PermissionUseHostConfig::new(catalog, approval);
        Self {
            catalog: config.catalog,
            limits: config.limits,
            approval: config.approval,
        }
    }
}

pub(crate) enum PermissionUseResult {
    NotMatched(b4_runtime::StoredExecution),
    Completed(b4_runtime::StoredExecution),
}
pub(crate) trait PermissionUseBridge: Send + Sync {
    fn execute_original_always(
        &self,
        principal: &Principal,
        operation: Operation,
        accepted: b4_runtime::StoredExecution,
        control: &Arc<crate::control::RunControl>,
        capacity: &b4_runtime::TaskCapacity,
    ) -> Result<b4_runtime::StoredExecution, crate::worker::WorkerError>;

    fn execute(
        &self,
        principal: &Principal,
        operation: Operation,
        accepted: b4_runtime::StoredExecution,
        control: &Arc<crate::control::RunControl>,
        capacity: &b4_runtime::TaskCapacity,
    ) -> Result<PermissionUseResult, crate::worker::WorkerError>;
}
struct Bridge<S> {
    store: Arc<S>,
    catalog: Arc<PermissionCatalog>,
    policy: Arc<dyn ResourcePolicy>,
    routes: Arc<std::collections::BTreeMap<b4_contract::RouteKey, crate::route::RegisteredRoute>>,
    policy_revision: String,
    tracker: Option<Arc<crate::permission_cancellation::Tracker>>,
    ready_cancellation: bool,
    withheld_cancellation: bool,
}
impl<S: PermissionStore + 'static> HttpHost<S> {
    /// Enables the bounded native original Always and reusable permission profile.
    /// Requires explicit atomic durable original Always and Ready-use writers.
    pub fn new_with_permissions(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: PermissionHostConfig,
    ) -> Result<Self, ConfigError> {
        Self::build_permission_host(
            store,
            auth,
            policy,
            PermissionUseHostConfig {
                catalog: config.catalog,
                limits: config.limits,
                approval: config.approval,
            },
            true,
            false,
            false,
            false,
        )
    }

    /// Enables cancellation only for actually entered original Always and NewUse callbacks.
    pub fn new_with_permission_cancellation(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: PermissionHostConfig,
    ) -> Result<Self, ConfigError> {
        Self::build_permission_host(
            store,
            auth,
            policy,
            PermissionUseHostConfig {
                catalog: config.catalog,
                limits: config.limits,
                approval: config.approval,
            },
            true,
            true,
            false,
            false,
        )
    }

    /// Enables confirmed cancellation of registered Ready tasks and entered callbacks.
    pub fn new_with_permission_ready_cancellation(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: PermissionHostConfig,
    ) -> Result<Self, ConfigError> {
        Self::build_permission_host(
            store,
            auth,
            policy,
            PermissionUseHostConfig {
                catalog: config.catalog,
                limits: config.limits,
                approval: config.approval,
            },
            true,
            true,
            true,
            false,
        )
    }

    /// Enables confirmed withdrawal before local callback admission, Ready cancellation,
    /// and cancellation after actual callback entry.
    pub fn new_with_permission_withheld_cancellation(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: PermissionHostConfig,
    ) -> Result<Self, ConfigError> {
        Self::build_permission_host(
            store,
            auth,
            policy,
            PermissionUseHostConfig {
                catalog: config.catalog,
                limits: config.limits,
                approval: config.approval,
            },
            true,
            true,
            true,
            true,
        )
    }

    pub fn new_with_permission_uses(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: PermissionUseHostConfig,
    ) -> Result<Self, ConfigError> {
        Self::build_permission_host(store, auth, policy, config, false, false, false, false)
    }
    #[allow(clippy::too_many_arguments)] // Explicit additive profile flags preserve all prior constructor selections.
    fn build_permission_host(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: PermissionUseHostConfig,
        original_always: bool,
        entered_cancellation: bool,
        ready_cancellation: bool,
        withheld_cancellation: bool,
    ) -> Result<Self, ConfigError> {
        use b4_contract::RecoveryPolicy;
        use b4_runtime::approval::ApprovalServiceConfig;
        let tracker = entered_cancellation
            .then(|| Arc::new(crate::permission_cancellation::Tracker::default()));
        let catalog = match &tracker {
            Some(tracker) => Arc::new(
                config
                    .catalog
                    .map_native_callbacks(|callback| tracker.wrap(callback)),
            ),
            None => config.catalog.clone(),
        };
        let application = catalog.application();
        let definitions: Vec<_> = application.definitions().collect();
        if definitions.len() != 1
            || definitions[0].batches.len() != 1
            || definitions[0].batches[0].len() != 1
            || !definitions[0].batches[0][0].requires_approval
            || definitions[0].batches[0][0].recovery != RecoveryPolicy::Pure
            || definitions[0].max_attempts == 0
            || definitions[0].max_attempts > 64
            || config.limits.max_active_runs != 1
            || config.limits.max_concurrent_tasks != 1
            || config.limits.max_tasks_per_execution != 1
        {
            return Err(ConfigError("permission_use_profile"));
        }
        // The receipt-backed adapter has one fixed outcome bound. Keep this
        // profile's configured and advertised limit equal to its commit gate.
        if config.limits.max_output_bytes != 65_536 {
            return Err(ConfigError("permission_use_output_limit"));
        }
        if !store.capabilities().durable_runtime
            || !store.approval_durable()
            || !store.ready_authorization_atomic_durable()
            || (original_always && !store.original_always_atomic_durable())
            || (entered_cancellation
                && (!store.permission_use_cancellation_atomic_durable()
                    || !store.permission_original_always_cancellation_atomic_durable()))
            || (ready_cancellation
                && (!store.permission_original_ready_cancellation_atomic_durable()
                    || !store.permission_use_ready_cancellation_atomic_durable()))
            || (withheld_cancellation
                && (!store.permission_original_always_withheld_cancellation_atomic_durable()
                    || !store.permission_use_withheld_cancellation_atomic_durable()))
            || config.approval.service_config != ApprovalServiceConfig::RequiredDurable
        {
            return Err(ConfigError("permission_use_durability_required"));
        }
        let routes: std::collections::BTreeMap<_, _> = application
            .definitions()
            .map(|definition| {
                (
                    definition.route.clone(),
                    crate::route::RegisteredRoute::native(application.clone(), definition.clone()),
                )
            })
            .collect();
        let limits = HostConfig::with_limits(config.limits);
        crate::host::validate_limits(&limits)?;
        let permission_uses = Arc::new(Bridge {
            store: store.clone(),
            catalog: catalog.clone(),
            policy: policy.clone(),
            routes: Arc::new(routes.clone()),
            policy_revision: config.approval.policy_revision.clone(),
            tracker,
            ready_cancellation,
            withheld_cancellation,
        });
        let approval = if original_always {
            crate::approval::native_permission_bridge(
                store.clone(),
                policy.clone(),
                &routes,
                limits.max_output_bytes,
                config.approval,
                application.clone(),
                permission_uses.clone(),
            )?
        } else {
            crate::approval::native_bridge(
                store.clone(),
                policy.clone(),
                &routes,
                limits.max_output_bytes,
                config.approval,
                application.clone(),
            )?
        };
        let mut host = Self::build_registered(
            store,
            auth,
            policy,
            limits,
            routes,
            Some(approval),
            Some(application.clone()),
            Some(true),
        )?;
        let shared =
            Arc::get_mut(&mut host.shared).ok_or(ConfigError("permission_use_ownership"))?;
        if entered_cancellation {
            shared.permission_cancellation = Some(permission_uses.clone());
        }
        shared.permission_ready_cancellation = ready_cancellation;
        shared.permission_withheld_cancellation = withheld_cancellation;
        shared.permission_uses = Some(permission_uses);
        Ok(host)
    }
}
fn facts(current: &b4_runtime::StoredExecution) -> b4_contract::TransitionFacts {
    b4_contract::TransitionFacts {
        now_ms: crate::transport::now_ms().max(current.snapshot.updated_at_ms),
    }
}
struct Hooks<'a>(&'a crate::control::RunControl, bool);
impl b4_runtime::permissions::PermissionUseDriverHooks for Hooks<'_> {
    fn enter_callback(&self) -> bool {
        if self.1 {
            return match self.0.permission_withheld_enter() {
                crate::control::PermissionEntry::Admitted => true,
                crate::control::PermissionEntry::ReservedRefusal => false,
                crate::control::PermissionEntry::Fault => {
                    self.0.fault();
                    false
                }
            };
        }
        match self.0.enter_callback() {
            Ok(true) => true,
            Ok(false) | Err(_) => {
                self.0.fault();
                false
            }
        }
    }
    fn facts(&self, current: &b4_runtime::StoredExecution) -> b4_contract::TransitionFacts {
        facts(current)
    }
    fn attempt_id(&self) -> b4_contract::AttemptId {
        b4_contract::AttemptId::new(uuid::Uuid::new_v4().to_string()).unwrap()
    }
}
fn permission_error(error: b4_runtime::permissions::PermissionError) -> crate::worker::WorkerError {
    use crate::worker::WorkerError;
    use b4_runtime::permissions::PermissionError;
    match error {
        PermissionError::Approval(error) => WorkerError::Approval(error),
        PermissionError::Store(error) => {
            WorkerError::Runtime(b4_runtime::RuntimeError::Store(error))
        }
        PermissionError::Kernel(error) => {
            WorkerError::Runtime(b4_runtime::RuntimeError::Kernel(error))
        }
        PermissionError::ConstraintDenied => WorkerError::Rejected("permission_constraint_denied"),
        PermissionError::ConstraintUnavailable => {
            WorkerError::Rejected("permission_constraint_unavailable")
        }
        PermissionError::LimitExceeded => WorkerError::Rejected("permission_limit_exceeded"),
        error => WorkerError::Runtime(b4_runtime::RuntimeError::EffectFailed(error.code().into())),
    }
}
impl<S: PermissionStore + 'static> crate::approval::PermissionAcceptance for Bridge<S> {
    fn accept(
        &self,
        authority: Arc<crate::approval::Authority>,
        execution_id: &b4_contract::ExecutionId,
        fence: b4_runtime::approval::ApprovalFence,
        request: b4_runtime::approval::ApprovalAcceptanceRequest,
    ) -> Result<b4_runtime::StoredExecution, crate::worker::WorkerError> {
        b4_runtime::permissions::accept_permission_always_with_normal_inputs(
            self.store.clone(),
            authority,
            self.catalog.clone(),
            execution_id,
            fence,
            request,
        )
        .map_err(permission_error)
    }
}
impl<S> Bridge<S> {
    fn cancellation_authority(
        &self,
        principal: &Principal,
    ) -> (
        crate::permission_cancellation::Authority,
        b4_runtime::approval::ApprovalContext,
    ) {
        (
            crate::permission_cancellation::Authority(crate::approval::Authority {
                principal: principal.clone(),
                operation: Operation::CancelRun,
                policy: self.policy.clone(),
                routes: self.routes.clone(),
                application: Some(self.catalog.application().clone()),
                policy_revision: self.policy_revision.clone(),
            }),
            b4_runtime::approval::ApprovalContext {
                actor: crate::approval::actor(principal),
                policy_revision: self.policy_revision.clone(),
            },
        )
    }
}
impl<S: PermissionStore> crate::permission_cancellation::CancellationBridge for Bridge<S> {
    fn classify(
        &self,
        control: &Arc<crate::control::RunControl>,
        view: &b4_runtime::ThreadView,
    ) -> Option<crate::permission_cancellation::CancellationKind> {
        self.tracker.as_ref()?.classify(
            control,
            view,
            self.ready_cancellation,
            self.withheld_cancellation,
        )
    }
    fn authorize_withheld(
        &self,
        principal: &Principal,
        view: &b4_runtime::ThreadView,
        target: &b4_runtime::RunCancellationTarget,
    ) -> Result<(), b4_runtime::permissions::PermissionError> {
        use b4_runtime::{approval::*, permissions::*};
        let (authority, context) = self.cancellation_authority(principal);
        authority.authorize_cancel(
            &context,
            &ApprovalResource {
                thread: view.thread.clone(),
                execution: view
                    .execution
                    .clone()
                    .ok_or(PermissionError::UnsupportedProfile)?,
            },
            target,
        )
    }
    fn cancel(
        &self,
        principal: &Principal,
        kind: crate::permission_cancellation::CancellationKind,
        target: &b4_runtime::RunCancellationTarget,
        facts: b4_contract::TransitionFacts,
    ) -> Result<
        crate::permission_cancellation::CancellationCommit,
        b4_runtime::permissions::PermissionError,
    > {
        use b4_runtime::{approval::*, permissions::*};
        let authority = crate::permission_cancellation::Authority(crate::approval::Authority {
            principal: principal.clone(),
            operation: Operation::CancelRun,
            policy: self.policy.clone(),
            routes: self.routes.clone(),
            application: Some(self.catalog.application().clone()),
            policy_revision: self.policy_revision.clone(),
        });
        let context = ApprovalContext {
            actor: crate::approval::actor(principal),
            policy_revision: self.policy_revision.clone(),
        };
        use crate::permission_cancellation::{CancellationCommit, CancellationKind, OriginKind};
        if let CancellationKind::Ready(origin) = kind {
            let mut budget = PermissionEvidenceBudget::for_ready_cancellation();
            return match origin {
                OriginKind::OriginalAlways => cancel_permission_ready_with_normal_inputs_confirmed(
                    self.store.as_ref(),
                    &authority,
                    &self.catalog,
                    &PermissionReadyCancellationRequest {
                        context: &context,
                        target,
                        facts,
                    },
                    &mut budget,
                ),
                OriginKind::NewUse => cancel_permission_use_ready_confirmed(
                    self.store.as_ref(),
                    &authority,
                    &self.catalog,
                    &PermissionUseReadyCancellationRequest {
                        context: &context,
                        target,
                        facts,
                    },
                    &mut budget,
                ),
            }
            .map(CancellationCommit::Ready);
        }
        let CancellationKind::Entered(kind) = kind else {
            // Withdrawal requires the actual worker-local failure, never this
            // manager-side port or a new evidence account.
            return Err(PermissionError::UnsupportedProfile);
        };
        let mut budget = PermissionEvidenceBudget::for_entered_cancellation();
        match kind {
            crate::permission_cancellation::OriginKind::OriginalAlways => {
                request_permission_original_always_cancellation(
                    self.store.as_ref(),
                    &authority,
                    &self.catalog,
                    &PermissionOriginalAlwaysCancellationRequest {
                        context: &context,
                        target,
                        facts,
                    },
                    &mut budget,
                )
            }
            crate::permission_cancellation::OriginKind::NewUse => {
                request_permission_use_cancellation(
                    self.store.as_ref(),
                    &authority,
                    &self.catalog,
                    &PermissionUseCancellationRequest {
                        context: &context,
                        target,
                        facts,
                    },
                    &mut budget,
                )
            }
        }
        .map(CancellationCommit::Entered)
    }
}
impl<S: PermissionStore> PermissionUseBridge for Bridge<S> {
    fn execute_original_always(
        &self,
        principal: &Principal,
        operation: Operation,
        accepted: b4_runtime::StoredExecution,
        control: &Arc<crate::control::RunControl>,
        capacity: &b4_runtime::TaskCapacity,
    ) -> Result<b4_runtime::StoredExecution, crate::worker::WorkerError> {
        use b4_runtime::{approval::*, permissions::*, RuntimeError};
        let authority = crate::approval::Authority {
            principal: principal.clone(),
            operation,
            policy: self.policy.clone(),
            routes: self.routes.clone(),
            application: Some(self.catalog.application().clone()),
            policy_revision: self.policy_revision.clone(),
        };
        let context = ApprovalContext {
            actor: crate::approval::actor(principal),
            policy_revision: self.policy_revision.clone(),
        };
        let hooks = Hooks(control, self.withheld_cancellation);
        let _registration = self
            .tracker
            .as_ref()
            .map(|tracker| {
                tracker.register(
                    control,
                    b4_runtime::InvocationContext {
                        thread_id: control.identity.thread_id.clone(),
                        thread_generation: control.identity.thread_generation.clone(),
                        origin: context.actor.clone(),
                        application_revision: self
                            .catalog
                            .application()
                            .identity()
                            .revision
                            .clone(),
                        execution_revision: accepted.snapshot.definition.revision.clone(),
                        policy_revision: context.policy_revision.clone(),
                    },
                    crate::permission_cancellation::OriginKind::OriginalAlways,
                    self.ready_cancellation.then_some(&accepted),
                )
            })
            .transpose()?;
        let bindings = PermissionOriginalAlwaysDriverBindings {
            store: self.store.as_ref(),
            authority: &authority,
            producer_catalog: &self.catalog,
            current_catalog: &self.catalog,
            context: &context,
            hooks: &hooks,
            capacity,
        };
        let _withheld_worker = crate::control::WithheldWorkerGuard(control.clone());
        let observer = control.native_cancellation_observer();
        let driven = if self.tracker.is_some() {
            drive_permission_original_always_cancellable_execution(bindings, accepted, &observer)
        } else {
            drive_permission_original_always_execution(bindings, accepted)
        };
        match driven {
            Ok(current) => Ok(current),
            Err(mut failure) => {
                if let Some(command) = self
                    .withheld_cancellation
                    .then(|| control.permission_withheld_command())
                    .flatten()
                {
                    let (cancel_authority, cancel_context) =
                        self.cancellation_authority(&command.principal);
                    let result = if command.kind
                        == crate::permission_cancellation::OriginKind::OriginalAlways
                    {
                        failure.cancel_withheld(
                            &cancel_authority,
                            &self.catalog,
                            &PermissionOriginalAlwaysWithheldCancellationRequest {
                                context: &cancel_context,
                                target: &command.target,
                                facts: command.facts,
                            },
                        )
                    } else {
                        Err(PermissionError::UnsupportedProfile)
                    };
                    let current = result.as_ref().ok().cloned();
                    let committed = control.permission_withheld_outcome(result);
                    if committed {
                        if let Some(current) = current {
                            return Ok(current);
                        }
                    }
                    control.fault();
                    drop(failure);
                    return Err(RuntimeError::EffectFailed("permission_driver_fault".into()).into());
                }
                let failure = if let Some(confirmation) = control.ready_confirmation() {
                    match failure.reconcile_ready_cancellation(&confirmation) {
                        Ok(current) => return Ok(current),
                        Err(failure) => failure,
                    }
                } else {
                    failure
                };
                // Retire actual drained work with all worker/admission/capacity guards live.
                // Observe returned evidence without replay or automatic storage retry.
                let _status = failure.callback_status();
                let _outcome = failure.settled_outcome();
                let _usage = failure.usage();
                control.fault();
                drop(failure);
                Err(RuntimeError::EffectFailed("permission_driver_fault".into()).into())
            }
        }
    }

    fn execute(
        &self,
        principal: &Principal,
        operation: Operation,
        accepted: b4_runtime::StoredExecution,
        control: &Arc<crate::control::RunControl>,
        capacity: &b4_runtime::TaskCapacity,
    ) -> Result<PermissionUseResult, crate::worker::WorkerError> {
        use b4_runtime::{approval::*, permissions::*, *};
        let runtime = Runtime::new(self.store.clone());
        let started = runtime.apply(
            &accepted.snapshot.execution_id,
            (&accepted.snapshot).into(),
            b4_contract::KernelCommand::Start,
            facts(&accepted),
        )?;
        let authority = crate::approval::Authority {
            principal: principal.clone(),
            operation,
            policy: self.policy.clone(),
            routes: self.routes.clone(),
            application: Some(self.catalog.application().clone()),
            policy_revision: self.policy_revision.clone(),
        };
        let context = ApprovalContext {
            actor: crate::approval::actor(principal),
            policy_revision: self.policy_revision.clone(),
        };
        let view = self
            .store
            .thread_view(&started.thread_id)
            .map_err(RuntimeError::Store)?
            .ok_or(RuntimeError::Store(StoreError::ThreadNotFound))?;
        if view.execution.as_ref() != Some(&started) {
            return Err(RuntimeError::Store(StoreError::RevisionConflict).into());
        }
        let command = b4_kernel::PermissionAuthorizationCommand::capture(
            &started.snapshot,
            started.snapshot.tasks[0].spec.id.clone(),
            b4_contract::permissions::PermissionUseId::new(format!(
                "b4pu_{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple(),
            ))
            .expect("fresh permission use identity has fixed prefix and 64 lowercase hex digits"),
        )?;
        let mut budget = PermissionEvidenceBudget::new();
        let ready = try_authorize_permission_ready(
            self.store.as_ref(),
            &authority,
            &self.catalog,
            ApprovalFence {
                execution: (&started.snapshot).into(),
                resource_revision: view.thread.resource_revision,
            },
            &PermissionReadyAuthorizationRequest {
                context: &context,
                command: &command,
                facts: facts(&started),
            },
            &mut budget,
        )
        .map_err(permission_error)?;
        let Some(ready) = ready else {
            return Ok(PermissionUseResult::NotMatched(started));
        };
        let hooks = Hooks(control, self.withheld_cancellation);
        let _registration = self
            .tracker
            .as_ref()
            .map(|tracker| {
                tracker.register(
                    control,
                    b4_runtime::InvocationContext {
                        thread_id: control.identity.thread_id.clone(),
                        thread_generation: control.identity.thread_generation.clone(),
                        origin: context.actor.clone(),
                        application_revision: self
                            .catalog
                            .application()
                            .identity()
                            .revision
                            .clone(),
                        execution_revision: ready.snapshot.definition.revision.clone(),
                        policy_revision: context.policy_revision.clone(),
                    },
                    crate::permission_cancellation::OriginKind::NewUse,
                    self.ready_cancellation.then_some(&ready),
                )
            })
            .transpose()?;
        let bindings = PermissionUseDriverBindings {
            store: self.store.as_ref(),
            authority: &authority,
            producer_catalog: &self.catalog,
            current_catalog: &self.catalog,
            context: &context,
            hooks: &hooks,
            capacity,
        };
        let _withheld_worker = crate::control::WithheldWorkerGuard(control.clone());
        let observer = control.native_cancellation_observer();
        let driven = if self.tracker.is_some() {
            drive_permission_use_cancellable_execution(bindings, ready, &observer)
        } else {
            drive_permission_use_execution(bindings, ready)
        };
        match driven {
            Ok(current) => Ok(PermissionUseResult::Completed(current)),
            Err(mut failure) => {
                if let Some(command) = self
                    .withheld_cancellation
                    .then(|| control.permission_withheld_command())
                    .flatten()
                {
                    let (cancel_authority, cancel_context) =
                        self.cancellation_authority(&command.principal);
                    let result =
                        if command.kind == crate::permission_cancellation::OriginKind::NewUse {
                            failure.cancel_withheld(
                                &cancel_authority,
                                &self.catalog,
                                &PermissionUseWithheldCancellationRequest {
                                    context: &cancel_context,
                                    target: &command.target,
                                    facts: command.facts,
                                },
                            )
                        } else {
                            Err(PermissionError::UnsupportedProfile)
                        };
                    let current = result.as_ref().ok().cloned();
                    let committed = control.permission_withheld_outcome(result);
                    if committed {
                        if let Some(current) = current {
                            return Ok(PermissionUseResult::Completed(current));
                        }
                    }
                    control.fault();
                    drop(failure);
                    return Err(RuntimeError::EffectFailed("permission_driver_fault".into()).into());
                }
                let failure = if let Some(confirmation) = control.ready_confirmation() {
                    match failure.reconcile_ready_cancellation(&confirmation) {
                        Ok(current) => return Ok(PermissionUseResult::Completed(current)),
                        Err(failure) => failure,
                    }
                } else {
                    failure
                };
                // The scoped Driver has drained before returning this actual
                // Work owner. Observe it while the worker/claim/capacity live;
                // never replay a callback or manufacture settlement evidence.
                let _status = failure.callback_status();
                let _outcome = failure.settled_outcome();
                let _usage = failure.usage();
                control.fault();
                drop(failure);
                Err(RuntimeError::EffectFailed("permission_driver_fault".into()).into())
            }
        }
    }
}
/// Bound actual input structure before admission/capture. The descriptor's
/// json_depth=64 describes the request-body parser; these additional value
/// bounds narrow the reusable lane's capture work. They do not promise that
/// every combination of individual maxima fits later evidence accounts.
/// Source/account refusals remain independent.
pub(crate) fn input_within_profile(input: &serde_json::Value) -> bool {
    fn walk(value: &serde_json::Value, depth: usize, nodes: &mut usize) -> bool {
        *nodes += 1;
        if depth > 16 || *nodes > 256 {
            return false;
        }
        match value {
            serde_json::Value::Array(values) => values.iter().all(|v| walk(v, depth + 1, nodes)),
            serde_json::Value::Object(values) => values.values().all(|v| walk(v, depth + 1, nodes)),
            _ => true,
        }
    }
    walk(input, 0, &mut 0)
}
