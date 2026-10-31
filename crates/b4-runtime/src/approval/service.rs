use super::*;
use crate::{
    EffectInvocation, EffectOutcome, PreparationError, PreparedEffect, Runtime, RuntimeError,
    StoreError, StoredExecution, ThreadView,
};
use b4_contract::*;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApprovalServiceConfig {
    #[default]
    RequiredDurable,
    VolatileFixture,
}

pub struct ApprovalService<S, A> {
    store: S,
    authority: A,
}
impl<S: ApprovalStore, A: ApprovalAuthority> ApprovalService<S, A> {
    pub fn new(store: S, authority: A) -> Result<Self, ApprovalError> {
        Self::with_config(store, authority, ApprovalServiceConfig::RequiredDurable)
    }
    pub fn with_config(
        store: S,
        authority: A,
        config: ApprovalServiceConfig,
    ) -> Result<Self, ApprovalError> {
        if config == ApprovalServiceConfig::RequiredDurable && !store.approval_durable() {
            return Err(ApprovalError::DurabilityRequired);
        }
        Ok(Self { store, authority })
    }
    pub fn store(&self) -> &S {
        &self.store
    }
    fn ordinary_resource(&self, id: &ExecutionId) -> Result<ApprovalResource, ApprovalError> {
        let execution = self
            .store
            .execution(id)?
            .ok_or(StoreError::ExecutionNotFound)?;
        let view = self
            .store
            .thread_view(&execution.thread_id)?
            .ok_or(StoreError::ThreadNotFound)?;
        let current = view.execution.ok_or(StoreError::ExecutionNotActive)?;
        if &current.snapshot.execution_id != id {
            return Err(StoreError::ExecutionNotActive.into());
        }
        Ok(ApprovalResource {
            thread: view.thread,
            execution: current,
        })
    }
    fn authorized(
        &self,
        context: &ApprovalContext,
        operation: ApprovalOperation,
        id: &ExecutionId,
        fence: ApprovalFence,
    ) -> Result<ApprovalView, ApprovalError> {
        validate_approval_context(context)?;
        let resource = self.ordinary_resource(id)?;
        if resource.thread.origin.as_ref() != Some(&context.actor) {
            return Err(ApprovalError::Unauthorized);
        }
        self.authority
            .authorize(context, operation, &resource)
            .map_err(|_| ApprovalError::Unauthorized)?;
        let refreshed = self.ordinary_resource(id)?;
        if refreshed != resource {
            return Err(StoreError::RevisionConflict.into());
        }
        let token_free_view = ApprovalView {
            thread: resource.thread.clone(),
            execution: resource.execution.clone(),
            grants: vec![],
            acceptances: vec![],
        };
        validate_approval_fence(&token_free_view, fence, self.store.owner_generation())?;
        // Decode sensitive rows only after authority. A coherent view and the
        // captured resource fence are checked again after the read.
        let view = self
            .store
            .approval_view(id)?
            .ok_or(StoreError::ExecutionNotFound)?;
        validate_approval_fence(&view, fence, self.store.owner_generation())?;
        if view.thread != resource.thread || view.execution != resource.execution {
            return Err(StoreError::RevisionConflict.into());
        }
        Ok(view)
    }
    #[allow(clippy::too_many_arguments)] // Explicit current fence, identities and trusted clock.
    pub fn park(
        &self,
        context: &ApprovalContext,
        id: &ExecutionId,
        fence: ApprovalFence,
        task_id: TaskId,
        interrupt_id: InterruptId,
        prompt: Value,
        ttl_ms: u64,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, ApprovalError> {
        let view = self.authorized(context, ApprovalOperation::Park, id, fence)?;
        if ttl_ms == 0 || ttl_ms > MAX_APPROVAL_TTL_MS {
            return Err(ApprovalError::InvalidTtl);
        }
        let expires_at_ms = facts
            .now_ms
            .checked_add(ttl_ms)
            .ok_or(ApprovalError::InvalidTtl)?;
        let current = &view.execution.snapshot;
        let task = current
            .tasks
            .iter()
            .find(|task| task.spec.id == task_id)
            .ok_or(b4_kernel::KernelError::UnknownTask)?;
        let digest = proposal_digest(task, &prompt)?;
        let plan = b4_kernel::transition(
            current,
            KernelCommand::Park {
                task_id: task_id.clone(),
                interrupt_id: interrupt_id.clone(),
                prompt: prompt.clone(),
            },
            facts.clone(),
        )?;
        let mut token_bytes = [0_u8; 32];
        getrandom::fill(&mut token_bytes).map_err(|_| ApprovalError::RandomUnavailable)?;
        let token = ApprovalToken::new(format!("b4ag_{}", hex_bytes(&token_bytes)))?;
        let mut resume_bytes = [0_u8; 32];
        getrandom::fill(&mut resume_bytes).map_err(|_| ApprovalError::RandomUnavailable)?;
        let grant = GrantRecord {
            thread_id: view.thread.id.clone(),
            thread_generation: view.thread.generation.clone(),
            origin: context.actor.clone(),
            execution_id: id.clone(),
            route: current.definition.route.clone(),
            definition_revision: current.definition.revision.clone(),
            task_id,
            effect_id: task.spec.effect_id.clone(),
            interrupt_id,
            resume_key: hex_bytes(&resume_bytes),
            task_parking_revision: plan.snapshot.revision,
            policy_revision: context.policy_revision.clone(),
            proposal_digest: digest,
            prompt,
            verifier: token_verifier(&token),
            token,
            issued_at_ms: facts.now_ms,
            expires_at_ms,
            consumed: false,
        };
        self.store
            .commit_approval(id, fence, plan, ApprovalMutation::Issue(grant))
    }
    pub fn pending(
        &self,
        context: &ApprovalContext,
        id: &ExecutionId,
        fence: ApprovalFence,
    ) -> Result<Vec<SensitivePendingApproval>, ApprovalError> {
        let view = self.authorized(context, ApprovalOperation::Pending, id, fence)?;
        if view.execution.snapshot.status != ExecutionStatus::WaitingForApproval {
            return Err(ApprovalError::NotStablePark);
        }
        let mut pending = Vec::new();
        for task in &view.execution.snapshot.tasks {
            if task.status != TaskStatus::WaitingForApproval {
                continue;
            }
            let interrupt = &task
                .interrupt
                .as_ref()
                .ok_or(ApprovalError::InvalidGrant)?
                .id;
            let mut matching = view
                .grants
                .iter()
                .filter(|grant| &grant.interrupt_id == interrupt);
            let grant = matching.next().ok_or(ApprovalError::GrantUnavailable)?;
            if matching.next().is_some() {
                return Err(ApprovalError::InvalidGrant);
            }
            valid_binding(&view, grant, task, &context.policy_revision)?;
            if grant.consumed {
                return Err(ApprovalError::Consumed);
            }
            pending.push(SensitivePendingApproval {
                task_id: grant.task_id.clone(),
                interrupt_id: grant.interrupt_id.clone(),
                prompt: grant.prompt.clone(),
                token: grant.token.clone(),
                resume_key: grant.resume_key.clone(),
                expires_at_ms: grant.expires_at_ms,
            });
        }
        if pending.is_empty() {
            return Err(ApprovalError::InterruptSetMismatch);
        }
        Ok(pending)
    }
    /// Read a captured stable park without adopting its execution owner. Unlike
    /// ordinary pending/resume, this operation also permits a prior owner.
    pub fn attachment_pending(
        &self,
        context: &ApprovalContext,
        captured: &ThreadView,
    ) -> Result<Vec<SensitivePendingApproval>, ApprovalError> {
        validate_approval_context(context)?;
        let ordinary = self
            .store
            .thread_view(&captured.thread.id)?
            .ok_or(ApprovalError::Unauthorized)?;
        if ordinary.thread.origin.as_ref() != Some(&context.actor) {
            return Err(ApprovalError::Unauthorized);
        }
        let resource = ApprovalResource {
            thread: ordinary.thread.clone(),
            execution: ordinary
                .execution
                .clone()
                .ok_or(ApprovalError::Unauthorized)?,
        };
        self.authority
            .authorize(context, ApprovalOperation::AttachmentPending, &resource)
            .map_err(|_| ApprovalError::Unauthorized)?;
        // Policy may wait while the resource changes. Reload token-free state
        // before even decoding sensitive rows, and never adopt a replacement.
        let renewed = self
            .store
            .thread_view(&captured.thread.id)?
            .ok_or(StoreError::RevisionConflict)?;
        if renewed != ordinary {
            return Err(StoreError::RevisionConflict.into());
        }
        validate_attachment_pending_resource(&renewed, captured, self.store.owner_generation())?;
        // Sensitive rows are decoded only after fresh token-free authority and
        // the complete captured identity have passed.
        let view = self
            .store
            .approval_view(&resource.execution.snapshot.execution_id)?
            .ok_or(StoreError::RevisionConflict)?;
        if view.thread != ordinary.thread || Some(&view.execution) != ordinary.execution.as_ref() {
            return Err(StoreError::RevisionConflict.into());
        }
        validate_approval_fence(
            &view,
            ApprovalFence {
                execution: (&resource.execution.snapshot).into(),
                resource_revision: ordinary.thread.resource_revision,
            },
            resource.execution.snapshot.owner_generation,
        )?;
        attachment_pending_records(&view, context)
    }
    pub fn accept(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        request: ApprovalAcceptanceRequest,
    ) -> Result<StoredExecution, ApprovalError> {
        let view = self.authorized(&request.context, ApprovalOperation::Accept, id, fence)?;
        let (plan, _) = prepare_acceptance(&view, &request)?;
        self.store
            .commit_approval(id, fence, plan, ApprovalMutation::Accept(request))
    }

    /// Evaluate an additional pure acceptance check before renewing the same
    /// authorized view and committing the ordinary real grant transaction.
    pub(crate) fn accept_checked<E, F>(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        request: ApprovalAcceptanceRequest,
        check: F,
    ) -> Result<StoredExecution, E>
    where
        E: From<ApprovalError>,
        F: FnOnce(&ApprovalView, &ApprovalAcceptanceRequest) -> Result<(), E>,
    {
        let view = self.authorized(&request.context, ApprovalOperation::Accept, id, fence)?;
        let _ = prepare_acceptance(&view, &request)?;
        check(&view, &request)?;
        let renewed = self.authorized(&request.context, ApprovalOperation::Accept, id, fence)?;
        if renewed != view {
            return Err(ApprovalError::Store(StoreError::RevisionConflict).into());
        }
        let (plan, _) = prepare_acceptance(&renewed, &request)?;
        self.store
            .commit_approval(id, fence, plan, ApprovalMutation::Accept(request))
            .map_err(E::from)
    }

    /// Adopt a known unfinished execution from a strictly older store owner.
    /// Authorization resolves through the requested token-free thread before
    /// execution identity diagnostics or any grant/acceptance decoding.
    pub fn recover(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        request: ApprovalRecoveryRequest,
    ) -> Result<StoredExecution, ApprovalError> {
        validate_approval_context(&request.context)?;
        let ordinary = self
            .store
            .thread_view(&request.thread_id)?
            .ok_or(ApprovalError::Unauthorized)?;
        if ordinary.thread.origin.as_ref() != Some(&request.context.actor) {
            return Err(ApprovalError::Unauthorized);
        }
        let resource = ApprovalResource {
            thread: ordinary.thread,
            execution: ordinary.execution.ok_or(ApprovalError::Unauthorized)?,
        };
        self.authority
            .authorize(&request.context, ApprovalOperation::Recover, &resource)
            .map_err(|_| ApprovalError::Unauthorized)?;
        if id != &request.execution_id {
            return Err(StoreError::ExecutionNotActive.into());
        }
        let token_free_view = ApprovalView {
            thread: resource.thread.clone(),
            execution: resource.execution.clone(),
            grants: vec![],
            acceptances: vec![],
        };
        prepare_recovery(
            &token_free_view,
            fence,
            &request,
            self.store.owner_generation(),
        )?;
        let view = self
            .store
            .approval_view(id)?
            .ok_or(StoreError::ExecutionNotFound)?;
        if view.thread != resource.thread || view.execution != resource.execution {
            return Err(StoreError::RevisionConflict.into());
        }
        let plan = prepare_recovery(&view, fence, &request, self.store.owner_generation())?;
        self.store
            .commit_approval(id, fence, plan, ApprovalMutation::Recover(request))
    }

    fn accepted_once(
        &self,
        view: &ApprovalView,
        context: &ApprovalContext,
        task_id: &TaskId,
    ) -> Result<(), ApprovalError> {
        let task = view
            .execution
            .snapshot
            .tasks
            .iter()
            .find(|task| &task.spec.id == task_id)
            .ok_or(b4_kernel::KernelError::UnknownTask)?;
        if !task.spec.requires_approval {
            return Ok(());
        }
        if !task.approved {
            return Err(ApprovalError::InvalidGrant);
        }
        let interrupt = task.interrupt.as_ref().ok_or(ApprovalError::InvalidGrant)?;
        let grant = view
            .grants
            .iter()
            .find(|grant| grant.task_id == *task_id && grant.interrupt_id == interrupt.id)
            .ok_or(ApprovalError::GrantUnavailable)?;
        valid_binding(view, grant, task, &context.policy_revision)?;
        if !grant.consumed {
            return Err(ApprovalError::InvalidGrant);
        }
        let current = &view.execution.snapshot;
        let matches = view.acceptances.iter().any(|acceptance| {
            validate_acceptance_record(acceptance).is_ok()
                && acceptance.execution_id == current.execution_id
                && acceptance.thread_id == view.thread.id
                && acceptance.thread_generation == view.thread.generation
                && acceptance.context == *context
                && acceptance.definition_revision == current.definition.revision
                && acceptance.previous_revision >= grant.task_parking_revision
                && acceptance.previous_revision < current.revision
                && current.turn_history.contains(&acceptance.turn_id)
                && acceptance.decisions.iter().any(|entry| {
                    entry.task_id == *task_id
                        && entry.interrupt_id == grant.interrupt_id
                        && entry.decision == ApprovalDecision::Once
                        && entry.proposal_digest == grant.proposal_digest
                        && entry.task_parking_revision == grant.task_parking_revision
                })
        });
        if !matches {
            return Err(ApprovalError::InvalidGrant);
        }
        Ok(())
    }
    /// Prepare a durable attempt through both authority checks without invoking.
    /// Any postclaim refusal retains the exact attempt as ownership evidence;
    /// that evidence does not authorize invocation after the refusal.
    pub fn prepare_dispatch(
        &self,
        context: &ApprovalContext,
        id: &ExecutionId,
        fence: ApprovalFence,
        task_id: TaskId,
        attempt_id: AttemptId,
        facts: TransitionFacts,
    ) -> Result<PreparedEffect, PreparationError<ApprovalError>> {
        let claim = || -> Result<(PreparedEffect, crate::ThreadRecord), ApprovalError> {
            let view = self.authorized(context, ApprovalOperation::Dispatch, id, fence)?;
            self.accepted_once(&view, context, &task_id)?;
            let plan = b4_kernel::transition(
                &view.execution.snapshot,
                KernelCommand::Dispatch {
                    task_id: task_id.clone(),
                    attempt_id: attempt_id.clone(),
                },
                facts,
            )?;
            let invocation =
                PreparedEffect::invocation_for(&plan.snapshot, task_id.clone(), attempt_id)?;
            let execution = self.store.commit(id, fence.execution, plan)?;
            Ok((
                PreparedEffect {
                    execution,
                    invocation,
                },
                view.thread,
            ))
        };
        let (prepared, authorized_thread) = claim().map_err(|cause| PreparationError {
            cause,
            claimed: None,
        })?;
        let authorize_claim = || -> Result<(), ApprovalError> {
            let resource = self.ordinary_resource(id)?;
            self.authority
                .authorize(context, ApprovalOperation::Dispatch, &resource)
                .map_err(|_| ApprovalError::Unauthorized)?;
            let refreshed = self.ordinary_resource(id)?;
            if refreshed != resource
                || resource.thread.id != authorized_thread.id
                || resource.thread.generation != authorized_thread.generation
                || resource.thread.origin != authorized_thread.origin
                || resource.thread.created_at_ms != authorized_thread.created_at_ms
                || resource.execution != prepared.execution
            {
                return Err(StoreError::RevisionConflict.into());
            }
            let claimed_fence = ApprovalFence {
                execution: crate::ExecutionFence::from(&prepared.execution.snapshot),
                resource_revision: resource.thread.resource_revision,
            };
            let token_free_view = ApprovalView {
                thread: resource.thread.clone(),
                execution: resource.execution.clone(),
                grants: vec![],
                acceptances: vec![],
            };
            validate_approval_fence(
                &token_free_view,
                claimed_fence,
                self.store.owner_generation(),
            )?;
            let current = self
                .store
                .approval_view(id)?
                .ok_or(StoreError::ExecutionNotFound)?;
            validate_approval_fence(&current, claimed_fence, self.store.owner_generation())?;
            if current.execution != prepared.execution
                || current.thread != resource.thread
                || resource.execution != prepared.execution
            {
                return Err(StoreError::RevisionConflict.into());
            }
            self.accepted_once(&current, context, &task_id)
        };
        if let Err(cause) = authorize_claim() {
            return Err(PreparationError {
                cause,
                claimed: Some(Box::new(prepared)),
            });
        }
        Ok(prepared)
    }

    #[allow(clippy::too_many_arguments)] // Explicit authority, fresh fences and host callback ports.
    pub fn dispatch_outcome<F, C>(
        &self,
        context: &ApprovalContext,
        id: &ExecutionId,
        fence: ApprovalFence,
        task_id: TaskId,
        attempt_id: AttemptId,
        facts: TransitionFacts,
        completion_facts: C,
        effect: F,
    ) -> Result<StoredExecution, ApprovalError>
    where
        F: FnOnce(&EffectInvocation) -> Result<EffectOutcome, String>,
        C: FnOnce() -> TransitionFacts,
    {
        let prepared = self
            .prepare_dispatch(context, id, fence, task_id, attempt_id, facts)
            .map_err(|error| error.cause)?;
        let invocation = prepared.invocation;
        let outcome = effect(&invocation)
            .map_err(|error| ApprovalError::Runtime(RuntimeError::EffectFailed(error)))?;
        Runtime::new(&self.store)
            .complete_outcome(
                &invocation.execution_id,
                invocation.owner_generation,
                invocation.task_id,
                invocation.attempt_id,
                &outcome,
                completion_facts(),
            )
            .map_err(|cause| {
                ApprovalError::Runtime(RuntimeError::OutcomeNotCommitted {
                    outcome,
                    cause: Box::new(cause),
                })
            })
    }
}
