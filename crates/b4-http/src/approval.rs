use crate::{transport::now_ms, *};
use b4_contract::*;
use b4_runtime::{
    approval::*, ExecutionFence, PreparationError, PreparedEffect, StoreError, StoredExecution,
    ThreadOrigin, ThreadView,
};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Required native approvals. Prompt values are authored by the embedding
/// application and keyed by the immutable registered route and task.
#[derive(Debug, Clone)]
pub struct ApprovalHostConfig {
    pub policy_revision: String,
    pub ttl_ms: u64,
    pub prompts: BTreeMap<RouteKey, BTreeMap<TaskId, Value>>,
    pub service_config: ApprovalServiceConfig,
}
impl ApprovalHostConfig {
    pub fn new(
        policy_revision: String,
        prompts: BTreeMap<RouteKey, BTreeMap<TaskId, Value>>,
    ) -> Self {
        Self {
            policy_revision,
            ttl_ms: 300_000,
            prompts,
            service_config: ApprovalServiceConfig::RequiredDurable,
        }
    }
}
/// Selects a continuation only; the driver independently verifies sealed authority.
pub(crate) enum AcceptedContinuation {
    Ordinary,
    OriginalAlways,
}
pub(crate) struct AcceptedExecution {
    pub current: StoredExecution,
    pub continuation: AcceptedContinuation,
}
pub(crate) trait PermissionAcceptance: Send + Sync {
    fn accept(
        &self,
        authority: Arc<Authority>,
        execution_id: &ExecutionId,
        fence: ApprovalFence,
        request: ApprovalAcceptanceRequest,
    ) -> Result<StoredExecution, crate::worker::WorkerError>;
}
pub(crate) trait ApprovalBridge: Send + Sync {
    fn permission_approvals(&self) -> bool;
    fn policy_revision(&self) -> &str;
    fn service_config(&self) -> ApprovalServiceConfig;
    fn approval_ttl_ms(&self) -> u64;
    fn validate_input(
        &self,
        route: &crate::route::RegisteredRoute,
        input: &Value,
    ) -> Result<(), ApprovalError>;
    fn park(
        &self,
        principal: &Principal,
        operation: Operation,
        current: &StoredExecution,
        task_id: TaskId,
    ) -> Result<StoredExecution, ApprovalError>;
    fn pending(
        &self,
        principal: &Principal,
        operation: Operation,
        view: &ThreadView,
    ) -> Result<Vec<Value>, ApprovalError>;
    fn attachment_pending(
        &self,
        principal: &Principal,
        view: &ThreadView,
    ) -> Result<Vec<Value>, ApprovalError>;
    fn accept(
        &self,
        principal: &Principal,
        view: &ThreadView,
        request: ResumeRequest,
    ) -> Result<AcceptedExecution, crate::worker::WorkerError>;
    fn recover(
        &self,
        principal: &Principal,
        target: &RecoveryTarget,
        route: &crate::route::RegisteredRoute,
    ) -> Result<StoredExecution, ApprovalError>;
    fn prepare_dispatch(
        &self,
        principal: &Principal,
        operation: Operation,
        current: &StoredExecution,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<ApprovalError>>;
}
struct Bridge<S> {
    store: Arc<S>,
    policy: Arc<dyn ResourcePolicy>,
    routes: Arc<BTreeMap<RouteKey, crate::route::RegisteredRoute>>,
    application: Option<Arc<b4_runtime::NativeApplication>>,
    config: ApprovalHostConfig,
    max_output_bytes: usize,
    permission_acceptance: Option<Arc<dyn PermissionAcceptance>>,
}
pub(crate) struct Authority {
    pub(crate) principal: Principal,
    pub(crate) operation: Operation,
    pub(crate) policy: Arc<dyn ResourcePolicy>,
    pub(crate) routes: Arc<BTreeMap<RouteKey, crate::route::RegisteredRoute>>,
    pub(crate) application: Option<Arc<b4_runtime::NativeApplication>>,
    pub(crate) policy_revision: String,
}
pub(crate) fn actor(principal: &Principal) -> ThreadOrigin {
    ThreadOrigin {
        application: principal.application.clone(),
        tenant: principal.tenant.clone(),
        principal: principal.principal.clone(),
    }
}
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        let definition = &resource.execution.snapshot.definition;
        if context.actor != actor(&self.principal)
            || context.policy_revision != self.policy_revision
            || resource.thread.origin.as_ref() != Some(&context.actor)
        {
            return Err(ApprovalError::Unauthorized);
        }
        let provenance = RouteProvenance {
            route: definition.route.clone(),
            revision: definition.revision.clone(),
        };
        let resource_context = ResourceContext {
            operation: self.operation,
            thread_id: Some(resource.thread.id.clone()),
            origin: resource.thread.origin.clone(),
            stored_route: Some(provenance.clone()),
            requested_route: Some(provenance),
        };
        if !self.policy.authorize(&self.principal, &resource_context) {
            return Err(ApprovalError::Unauthorized);
        }
        crate::route::recorded(self.application.as_ref(), &self.routes, definition)
            .ok_or(ApprovalError::RouteMismatch)?;
        Ok(())
    }
}
pub(crate) fn bridge<S: ApprovalStore + 'static>(
    store: Arc<S>,
    policy: Arc<dyn ResourcePolicy>,
    routes: &BTreeMap<RouteKey, crate::route::RegisteredRoute>,
    max_output_bytes: usize,
    config: ApprovalHostConfig,
) -> Result<Arc<dyn ApprovalBridge>, ConfigError> {
    build_bridge(store, policy, routes, max_output_bytes, config, None, None)
}
pub(crate) fn native_bridge<S: ApprovalStore + 'static>(
    store: Arc<S>,
    policy: Arc<dyn ResourcePolicy>,
    routes: &BTreeMap<RouteKey, crate::route::RegisteredRoute>,
    max_output_bytes: usize,
    config: ApprovalHostConfig,
    application: Arc<b4_runtime::NativeApplication>,
) -> Result<Arc<dyn ApprovalBridge>, ConfigError> {
    build_bridge(
        store,
        policy,
        routes,
        max_output_bytes,
        config,
        Some(application),
        None,
    )
}
pub(crate) fn native_permission_bridge<S: ApprovalStore + 'static>(
    store: Arc<S>,
    policy: Arc<dyn ResourcePolicy>,
    routes: &BTreeMap<RouteKey, crate::route::RegisteredRoute>,
    max_output_bytes: usize,
    config: ApprovalHostConfig,
    application: Arc<b4_runtime::NativeApplication>,
    permission_acceptance: Arc<dyn PermissionAcceptance>,
) -> Result<Arc<dyn ApprovalBridge>, ConfigError> {
    build_bridge(
        store,
        policy,
        routes,
        max_output_bytes,
        config,
        Some(application),
        Some(permission_acceptance),
    )
}
#[allow(clippy::too_many_arguments)]
fn build_bridge<S: ApprovalStore + 'static>(
    store: Arc<S>,
    policy: Arc<dyn ResourcePolicy>,
    routes: &BTreeMap<RouteKey, crate::route::RegisteredRoute>,
    max_output_bytes: usize,
    config: ApprovalHostConfig,
    application: Option<Arc<b4_runtime::NativeApplication>>,
    permission_acceptance: Option<Arc<dyn PermissionAcceptance>>,
) -> Result<Arc<dyn ApprovalBridge>, ConfigError> {
    if config.service_config == ApprovalServiceConfig::RequiredDurable && !store.approval_durable()
    {
        return Err(ConfigError("approval_durability_required"));
    }
    validate_approval_context(&ApprovalContext {
        actor: ThreadOrigin {
            application: "config".into(),
            tenant: "config".into(),
            principal: "config".into(),
        },
        policy_revision: config.policy_revision.clone(),
    })
    .map_err(|_| ConfigError("invalid_approval_revision"))?;
    if config.ttl_ms == 0 || config.ttl_ms > MAX_APPROVAL_TTL_MS {
        return Err(ConfigError("invalid_approval_ttl"));
    }
    let expected: BTreeMap<_, BTreeSet<_>> = routes
        .values()
        .filter_map(|route| {
            let tasks: BTreeSet<_> = route
                .definition
                .batches
                .iter()
                .flatten()
                .filter(|task| task.requires_approval)
                .map(|task| task.id.clone())
                .collect();
            (!tasks.is_empty()).then(|| (route.definition.route.clone(), tasks))
        })
        .collect();
    let submitted: BTreeMap<_, BTreeSet<_>> = config
        .prompts
        .iter()
        .map(|(route, tasks)| (route.clone(), tasks.keys().cloned().collect()))
        .collect();
    if expected != submitted {
        return Err(ConfigError("approval_prompt_coverage"));
    }
    for route in routes.values() {
        let mut envelopes = Vec::new();
        for spec in route
            .definition
            .batches
            .iter()
            .flatten()
            .filter(|spec| spec.requires_approval)
        {
            let prompt = &config.prompts[&route.definition.route][&spec.id];
            canonical_proposal(
                &proposal_task(spec.clone(), &Value::Object(Map::new())),
                prompt,
            )
            .map_err(|_| ConfigError("approval_prompt_limit"))?;
            // Trusted explicit recovery may restore an opaque identifier minted
            // by the embedded service rather than this host's UUID generator.
            envelopes.push(json!({"interruptId":"0".repeat(256),"resumeKey":"0".repeat(64),"value":prompt,"grant":format!("b4ag_{}", "0".repeat(64))}));
        }
        if serde_json::to_vec(&json!({"interrupts":envelopes}))
            .map_err(|_| ConfigError("approval_response_limit"))?
            .len()
            > max_output_bytes
        {
            return Err(ConfigError("approval_response_limit"));
        }
    }
    Ok(Arc::new(Bridge {
        store,
        policy,
        routes: Arc::new(routes.clone()),
        application,
        config,
        max_output_bytes,
        permission_acceptance,
    }))
}
fn proposal_task(spec: TaskSpec, input: &Value) -> TaskRecord {
    TaskRecord {
        spec,
        status: TaskStatus::Ready,
        input: Map::from_iter([("input".into(), input.clone())]),
        attempt_id: None,
        result: None,
        failure: None,
        interrupt: None,
        approved: false,
    }
}
impl<S: ApprovalStore> Bridge<S> {
    fn prepare_acceptance(
        &self,
        principal: &Principal,
        view: &ThreadView,
        request: ResumeRequest,
    ) -> Result<(ExecutionId, ApprovalFence, ApprovalAcceptanceRequest), ApprovalError> {
        let current = view
            .execution
            .as_ref()
            .ok_or(ApprovalError::NotStablePark)?;
        let fresh = self
            .store
            .thread_view(&view.thread.id)?
            .ok_or(StoreError::ThreadNotFound)?;
        let fresh_execution = fresh
            .execution
            .as_ref()
            .ok_or(StoreError::ExecutionNotActive)?;
        self.authority(principal, Operation::Resume).authorize(
            &self.context(principal),
            ApprovalOperation::Accept,
            &ApprovalResource {
                thread: fresh.thread.clone(),
                execution: fresh_execution.clone(),
            },
        )?;
        if fresh != *view {
            return Err(StoreError::RevisionConflict.into());
        }
        if current.snapshot.owner_generation != self.store.owner_generation() {
            return Err(StoreError::OwnerConflict.into());
        }
        if current.snapshot.status != ExecutionStatus::WaitingForApproval {
            return Err(ApprovalError::NotStablePark);
        }
        if request.route != current.snapshot.definition.route {
            return Err(ApprovalError::RouteMismatch);
        }
        crate::route::recorded(
            self.application.as_ref(),
            &self.routes,
            &current.snapshot.definition,
        )
        .ok_or(ApprovalError::RouteMismatch)?;
        let pending: BTreeSet<_> = current
            .snapshot
            .tasks
            .iter()
            .filter(|task| task.status == TaskStatus::WaitingForApproval)
            .filter_map(|task| {
                task.interrupt
                    .as_ref()
                    .map(|interrupt| interrupt.id.clone())
            })
            .collect();
        let submitted: BTreeSet<_> = request
            .resume
            .iter()
            .map(|entry| entry.interrupt_id().clone())
            .collect();
        if pending.is_empty() || submitted != pending || submitted.len() != request.resume.len() {
            return Err(ApprovalError::InterruptSetMismatch);
        }
        if self.permission_acceptance.is_none()
            && request
                .resume
                .iter()
                .any(|entry| entry.decision() == ApprovalDecision::Always)
        {
            return Err(ApprovalError::UnsupportedAlways);
        }
        // The declared task order determines conversion and missing-token
        // precedence, even when a caller permutes the complete decision set.
        let decisions = current
            .snapshot
            .tasks
            .iter()
            .filter(|task| task.status == TaskStatus::WaitingForApproval)
            .map(|task| {
                let interrupt = &task
                    .interrupt
                    .as_ref()
                    .ok_or(ApprovalError::InvalidGrant)?
                    .id;
                let entry = request
                    .resume
                    .iter()
                    .find(|entry| entry.interrupt_id() == interrupt)
                    .ok_or(ApprovalError::InterruptSetMismatch)?;
                let (interrupt_id, decision, cancelled, grant) = match entry {
                    ResumeEntry::Resolved {
                        interrupt_id,
                        payload,
                        grant,
                    } => (interrupt_id.clone(), *payload, false, grant),
                    ResumeEntry::Cancelled {
                        interrupt_id,
                        grant,
                    } => (interrupt_id.clone(), ApprovalDecision::Deny, true, grant),
                };
                let token = ApprovalToken::new(
                    grant.as_ref().ok_or(ApprovalError::MissingToken)?.expose(),
                )?;
                Ok(GrantDecision {
                    interrupt_id,
                    decision,
                    cancelled,
                    token: Some(token),
                })
            })
            .collect::<Result<Vec<_>, ApprovalError>>()?;
        Ok((
            current.snapshot.execution_id.clone(),
            ApprovalFence {
                execution: ExecutionFence::from(&current.snapshot),
                resource_revision: view.thread.resource_revision,
            },
            ApprovalAcceptanceRequest {
                context: self.context(principal),
                route: request.route,
                definition_revision: current.snapshot.definition.revision.clone(),
                turn_id: TurnId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
                now_ms: now_ms().max(current.snapshot.updated_at_ms),
                decisions,
            },
        ))
    }
    fn context(&self, principal: &Principal) -> ApprovalContext {
        ApprovalContext {
            actor: actor(principal),
            policy_revision: self.config.policy_revision.clone(),
        }
    }
    fn service(
        &self,
        principal: &Principal,
        operation: Operation,
    ) -> Result<ApprovalService<Arc<S>, Authority>, ApprovalError> {
        ApprovalService::with_config(
            self.store.clone(),
            self.authority(principal, operation),
            self.config.service_config,
        )
    }
    fn authority(&self, principal: &Principal, operation: Operation) -> Authority {
        Authority {
            principal: principal.clone(),
            operation,
            policy: self.policy.clone(),
            routes: self.routes.clone(),
            application: self.application.clone(),
            policy_revision: self.config.policy_revision.clone(),
        }
    }
    fn fence(&self, current: &StoredExecution) -> Result<ApprovalFence, ApprovalError> {
        let view = self
            .store
            .thread_view(&current.thread_id)?
            .ok_or(StoreError::ThreadNotFound)?;
        if view.execution.as_ref() != Some(current) {
            return Err(StoreError::RevisionConflict.into());
        }
        Ok(ApprovalFence {
            execution: ExecutionFence::from(&current.snapshot),
            resource_revision: view.thread.resource_revision,
        })
    }
    fn current_definition(&self, definition: &Definition) -> Result<(), ApprovalError> {
        self.routes
            .get(&definition.route)
            .filter(|route| route.definition == *definition)
            .map(|_| ())
            .ok_or(ApprovalError::RouteMismatch)
    }
}
impl<S: ApprovalStore> ApprovalBridge for Bridge<S> {
    fn permission_approvals(&self) -> bool {
        self.permission_acceptance.is_some()
    }

    fn policy_revision(&self) -> &str {
        &self.config.policy_revision
    }
    fn service_config(&self) -> ApprovalServiceConfig {
        self.config.service_config
    }
    fn approval_ttl_ms(&self) -> u64 {
        self.config.ttl_ms
    }
    fn attachment_pending(
        &self,
        principal: &Principal,
        view: &ThreadView,
    ) -> Result<Vec<Value>, ApprovalError> {
        let pending = self
            .service(principal, Operation::Attach)?
            .attachment_pending(&self.context(principal), view)?;
        Ok(pending.into_iter().map(|pending| json!({"interruptId":pending.interrupt_id,"resumeKey":pending.resume_key,"value":pending.prompt,"grant":pending.token.as_str()})).collect())
    }
    fn validate_input(
        &self,
        route: &crate::route::RegisteredRoute,
        input: &Value,
    ) -> Result<(), ApprovalError> {
        self.current_definition(&route.definition)?;
        for spec in route
            .definition
            .batches
            .iter()
            .flatten()
            .filter(|task| task.requires_approval)
        {
            canonical_proposal(
                &proposal_task(spec.clone(), input),
                &self.config.prompts[&route.definition.route][&spec.id],
            )?;
        }
        Ok(())
    }
    fn park(
        &self,
        principal: &Principal,
        operation: Operation,
        current: &StoredExecution,
        task_id: TaskId,
    ) -> Result<StoredExecution, ApprovalError> {
        self.current_definition(&current.snapshot.definition)?;
        let prompt = self
            .config
            .prompts
            .get(&current.snapshot.definition.route)
            .and_then(|tasks| tasks.get(&task_id))
            .ok_or(ApprovalError::InvalidProposal)?
            .clone();
        self.service(principal, operation)?.park(
            &self.context(principal),
            &current.snapshot.execution_id,
            self.fence(current)?,
            task_id,
            InterruptId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            prompt,
            self.config.ttl_ms,
            TransitionFacts {
                now_ms: now_ms().max(current.snapshot.updated_at_ms),
            },
        )
    }
    fn pending(
        &self,
        principal: &Principal,
        operation: Operation,
        view: &ThreadView,
    ) -> Result<Vec<Value>, ApprovalError> {
        let Some(current) = &view.execution else {
            return Ok(vec![]);
        };
        let fresh = self
            .store
            .thread_view(&view.thread.id)?
            .ok_or(StoreError::ThreadNotFound)?;
        let fresh_execution = fresh
            .execution
            .as_ref()
            .ok_or(StoreError::ExecutionNotActive)?;
        self.authority(principal, operation)
            .authorize(
                &self.context(principal),
                ApprovalOperation::Pending,
                &ApprovalResource {
                    thread: fresh.thread.clone(),
                    execution: fresh_execution.clone(),
                },
            )
            .map_err(|_| ApprovalError::Unauthorized)?;
        if fresh != *view {
            return Err(StoreError::RevisionConflict.into());
        }
        if current.snapshot.owner_generation != self.store.owner_generation()
            && !b4_runtime::releases_slot(current.snapshot.status)
        {
            return Err(StoreError::OwnerConflict.into());
        }
        if current.snapshot.status != ExecutionStatus::WaitingForApproval {
            if current
                .snapshot
                .tasks
                .iter()
                .any(|task| task.status == TaskStatus::WaitingForApproval)
            {
                return Err(ApprovalError::NotStablePark);
            }
            return Ok(vec![]);
        }
        let pending = self.service(principal, operation)?.pending(
            &self.context(principal),
            &current.snapshot.execution_id,
            ApprovalFence {
                execution: ExecutionFence::from(&current.snapshot),
                resource_revision: view.thread.resource_revision,
            },
        )?;
        let projected: Vec<_> = pending.into_iter().map(|pending| json!({"interruptId":pending.interrupt_id,"resumeKey":pending.resume_key,"value":pending.prompt,"grant":pending.token.as_str()})).collect();
        if serde_json::to_vec(&json!({"interrupts":projected}))
            .map_err(|_| ApprovalError::InvalidProposal)?
            .len()
            > self.max_output_bytes
        {
            return Err(ApprovalError::ProposalTooLarge);
        }
        Ok(projected)
    }
    fn accept(
        &self,
        principal: &Principal,
        view: &ThreadView,
        request: ResumeRequest,
    ) -> Result<AcceptedExecution, crate::worker::WorkerError> {
        let (execution_id, fence, request) = self.prepare_acceptance(principal, view, request)?;
        if request
            .decisions
            .iter()
            .any(|decision| decision.decision == ApprovalDecision::Always)
        {
            let extension = self
                .permission_acceptance
                .as_ref()
                .ok_or(ApprovalError::UnsupportedAlways)?;
            let current = extension.accept(
                Arc::new(self.authority(principal, Operation::Resume)),
                &execution_id,
                fence,
                request,
            )?;
            return Ok(AcceptedExecution {
                current,
                continuation: AcceptedContinuation::OriginalAlways,
            });
        }
        let current =
            self.service(principal, Operation::Resume)?
                .accept(&execution_id, fence, request)?;
        Ok(AcceptedExecution {
            current,
            continuation: AcceptedContinuation::Ordinary,
        })
    }
    fn recover(
        &self,
        principal: &Principal,
        target: &RecoveryTarget,
        route: &crate::route::RegisteredRoute,
    ) -> Result<StoredExecution, ApprovalError> {
        let current = self
            .store
            .thread_view(&target.thread_id)?
            .ok_or(ApprovalError::Unauthorized)?;
        let execution = current
            .execution
            .as_ref()
            .ok_or(ApprovalError::RecoveryNotNeeded)?;
        self.service(principal, Operation::Recover)?.recover(
            &target.execution_id,
            target.fence,
            ApprovalRecoveryRequest {
                context: self.context(principal),
                thread_id: target.thread_id.clone(),
                thread_generation: target.thread_generation.clone(),
                execution_id: target.execution_id.clone(),
                available_definition: route.definition.clone(),
                turn_id: TurnId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
                facts: TransitionFacts {
                    now_ms: now_ms().max(execution.snapshot.updated_at_ms),
                },
            },
        )
    }
    fn prepare_dispatch(
        &self,
        principal: &Principal,
        operation: Operation,
        current: &StoredExecution,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<ApprovalError>> {
        let service = self
            .service(principal, operation)
            .map_err(|cause| PreparationError {
                cause,
                claimed: None,
            })?;
        let fence = self.fence(current).map_err(|cause| PreparationError {
            cause,
            claimed: None,
        })?;
        service.prepare_dispatch(
            &self.context(principal),
            &current.snapshot.execution_id,
            fence,
            task_id,
            attempt_id,
            TransitionFacts {
                now_ms: now_ms().max(current.snapshot.updated_at_ms),
            },
        )
    }
}
