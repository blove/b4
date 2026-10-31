//! Failure-owned withdrawal of genuine uninvoked renewed NewUse Work.
use super::runtime_mutation::PermissionRuntimeOrigin;
use super::runtime_use::{
    PermissionRuntimeUseEvidence, PermissionRuntimeUseSession, PermissionRuntimeUseWitness,
};
use super::*;
use crate::approval::{validate_approval_context, ApprovalContext, ApprovalResource};
use crate::{InvocationContext, RunCancellationTarget, StoreError, StoredExecution};
use b4_contract::{
    CommitPlan, ExecutionId, ExecutionStatus, KernelCommand, TaskStatus, TransitionFacts,
};
use std::sync::Arc;

/// Management request; only an actual drained failure can supply withheld Work.
pub struct PermissionUseWithheldCancellationRequest<'a> {
    pub context: &'a ApprovalContext,
    pub target: &'a RunCancellationTarget,
    pub facts: TransitionFacts,
}
/// Sealed source selection; no generic store mutation authority.
pub struct PermissionUseWithheldCancellationSelector<'a> {
    request: &'a PermissionUseWithheldCancellationRequest<'a>,
    origin: &'a Arc<PermissionRuntimeOrigin>,
    expected: Option<&'a PermissionUseWithheldCancellationCapture>,
    claim: &'a super::runtime_mutation::PermissionUseClaimLoan<'a>,
}
pub struct PermissionUseWithheldCancellationCapture {
    resource: ApprovalResource,
    witness: Option<PermissionRuntimeUseWitness>,
    origin: Arc<PermissionRuntimeOrigin>,
    matched: bool,
}
pub struct PermissionPreparedUseWithheldCancellation<'a> {
    request: &'a PermissionUseWithheldCancellationRequest<'a>,
    capture: PermissionUseWithheldCancellationCapture,
    plan: CommitPlan,
    terminal: CommitPlan,
    claim: &'a super::runtime_mutation::PermissionUseClaimLoan<'a>,
}
pub struct PermissionUseWithheldCancellationCommit {
    execution: StoredExecution,
}
fn target_matches(
    resource: &ApprovalResource,
    target: &RunCancellationTarget,
) -> Result<(), PermissionError> {
    let t = &resource.thread;
    let e = &resource.execution;
    let s = &e.snapshot;
    if t.id != target.thread_id
        || t.generation != target.thread_generation
        || t.resource_revision != target.resource_revision
        || e.thread_id != target.thread_id
        || t.active_execution_id.as_ref() != Some(&target.execution_id)
        || s.execution_id != target.execution_id
        || s.turn_id != target.turn_id
        || crate::ExecutionFence::from(s) != target.execution_fence
    {
        return Err(StoreError::RevisionConflict.into());
    }
    if s.status != ExecutionStatus::Running
        || s.revision != 3
        || s.tasks.len() != 1
        || s.tasks[0].status != TaskStatus::InFlight
        || s.tasks[0].attempt_id.is_none()
        || s.definition.batches.len() != 1
        || s.definition.batches[0].len() != 1
        || s.tasks[0].spec != s.definition.batches[0][0]
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    Ok(())
}
impl PermissionUseWithheldCancellationSelector<'_> {
    pub fn execution_id(&self) -> &ExecutionId {
        &self.request.target.execution_id
    }
    pub fn begin_read<'a>(
        &'a self,
        resource: &'a ApprovalResource,
        budget: &'a mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeUseSession<'a>, PermissionError> {
        target_matches(resource, self.request.target)?;
        if resource != self.claim.resource {
            return Err(PermissionError::CorruptRecord);
        }
        if resource.thread.origin.as_ref() != Some(&self.request.context.actor) {
            return Err(crate::approval::ApprovalError::Unauthorized.into());
        }
        let expected = match self.expected {
            Some(old) => {
                if old.resource != *resource || !Arc::ptr_eq(&old.origin, self.origin) {
                    return Err(PermissionError::CorruptRecord);
                }
                Some(old.witness.as_ref().ok_or(PermissionError::CorruptRecord)?)
            }
            None => None,
        };
        PermissionRuntimeUseSession::for_entered_cancellation(
            budget,
            &resource.execution,
            &resource.thread,
            self.origin,
            expected,
        )
    }
    pub fn finish_read(
        &self,
        resource: ApprovalResource,
        evidence: PermissionRuntimeUseEvidence,
        owner: u64,
    ) -> Result<PermissionUseWithheldCancellationCapture, PermissionError> {
        target_matches(&resource, self.request.target)?;
        if resource != *self.claim.resource {
            return Err(PermissionError::CorruptRecord);
        }
        if resource.execution.snapshot.owner_generation != owner {
            return Err(StoreError::OwnerConflict.into());
        }
        let witness = if self.expected.is_some() {
            evidence.require_match(self.origin)?;
            None
        } else {
            Some(evidence.into_capture(self.origin)?)
        };
        Ok(PermissionUseWithheldCancellationCapture {
            resource,
            witness,
            origin: Arc::clone(self.origin),
            matched: self.expected.is_some(),
        })
    }
}
impl PermissionPreparedUseWithheldCancellation<'_> {
    pub fn selector(&self) -> PermissionUseWithheldCancellationSelector<'_> {
        PermissionUseWithheldCancellationSelector {
            request: self.request,
            origin: &self.capture.origin,
            expected: Some(&self.capture),
            claim: self.claim,
        }
    }
    pub fn plan(&self) -> &CommitPlan {
        &self.plan
    }
    pub fn terminal_plan(&self) -> &CommitPlan {
        &self.terminal
    }
    pub fn resource(&self) -> &ApprovalResource {
        &self.capture.resource
    }
    /// The typed writer calls this only after exact immediate source comparison.
    pub fn acknowledge(
        &self,
        observed: PermissionUseWithheldCancellationCapture,
    ) -> Result<PermissionUseWithheldCancellationCommit, PermissionError> {
        if !observed.matched
            || observed.witness.is_some()
            || observed.resource != self.capture.resource
            || !Arc::ptr_eq(&observed.origin, &self.capture.origin)
        {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionUseWithheldCancellationCommit {
            execution: StoredExecution {
                thread_id: observed.resource.execution.thread_id,
                snapshot: self.terminal.snapshot.clone(),
            },
        })
    }
}

pub(super) fn cancel<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    request: &PermissionUseWithheldCancellationRequest<'_>,
    claim: super::runtime_mutation::PermissionUseClaimLoan<'_>,
    invocation_context: &InvocationContext,
    account: &mut WithheldAccount,
) -> Result<StoredExecution, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: PermissionCancellationAuthority + ?Sized,
{
    if !store.permission_use_withheld_cancellation_atomic_durable() {
        return Err(PermissionError::UnsupportedProfile);
    }
    validate_approval_context(request.context)?;
    target_matches(claim.resource, request.target)?;
    if claim.historical_context != invocation_context
        || invocation_context.origin != request.context.actor
        || invocation_context.policy_revision != request.context.policy_revision
    {
        return Err(crate::approval::ApprovalError::Unauthorized.into());
    }
    let snapshot = &claim.resource.execution.snapshot;
    catalog.ready_registration(
        &snapshot.definition,
        &snapshot.tasks[0].spec.effect_id,
        &mut account.budget,
    )?;
    account.begin(request)?;
    let budget = &mut account.budget;
    let origin = account
        .origin
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let selector = PermissionUseWithheldCancellationSelector {
        request,
        origin,
        expected: None,
        claim: &claim,
    };
    let captured = store.capture_permission_use_withheld_cancellation(&selector, budget)?;
    if !Arc::ptr_eq(origin, &captured.origin) || captured.matched {
        return Err(PermissionError::CorruptRecord);
    }
    target_matches(&captured.resource, request.target)?;
    if captured.resource.execution.snapshot.owner_generation != store.owner_generation() {
        return Err(StoreError::OwnerConflict.into());
    }
    let resource = &captured.resource;
    if resource.thread.origin.as_ref() != Some(&request.context.actor) {
        return Err(crate::approval::ApprovalError::Unauthorized.into());
    }
    let snapshot = &resource.execution.snapshot;
    let task = &snapshot.tasks[0];
    // Existing normal resource/record bounds also cap these complete context,
    // context and kernel projection owners in the same cumulative account.
    budget.runtime_reserve(262_144, 4096)?;
    let registration =
        catalog.ready_registration(&snapshot.definition, &task.spec.effect_id, budget)?;
    let context = InvocationContext {
        thread_id: resource.thread.id.clone(),
        thread_generation: resource.thread.generation.clone(),
        origin: request.context.actor.clone(),
        application_revision: catalog.application_identity().revision.clone(),
        execution_revision: snapshot.definition.revision.clone(),
        policy_revision: request.context.policy_revision.clone(),
    };
    let witness = captured
        .witness
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    // The completed evidence must also bind the final opaque capture resource;
    // an observed snapshot is not independently a cancellation permit.
    witness.validate_entered_resource(resource, budget)?;
    witness.matches_withheld_ready(claim.witness, claim.origin, budget)?;
    if &context != invocation_context || resource != claim.resource {
        return Err(PermissionError::CorruptRecord);
    }
    let task = &resource.execution.snapshot.tasks[0];
    if claim.invocation.execution_id != snapshot.execution_id
        || claim.invocation.turn_id != snapshot.turn_id
        || claim.invocation.owner_generation != snapshot.owner_generation
        || claim.invocation.task_id != task.spec.id
        || claim.invocation.effect_id != task.spec.effect_id
        || task.attempt_id.as_ref() != Some(&claim.invocation.attempt_id)
        || claim.invocation.input != task.input
    {
        return Err(PermissionError::CorruptRecord);
    }
    if !witness.validate_source(
        resource,
        &context,
        registration,
        catalog.application_identity(),
        budget,
    )? {
        return Err(PermissionError::ConstraintDenied);
    }
    authority.authorize_cancel(request.context, resource, request.target)?;
    let plan = b4_kernel::transition(
        snapshot,
        KernelCommand::Cancel {
            turn_id: snapshot.turn_id.clone(),
            owner_generation: snapshot.owner_generation,
        },
        request.facts.clone(),
    )?;
    if !super::use_cancellation_relation::snapshot_matches(snapshot, &plan.snapshot) {
        return Err(PermissionError::CorruptRecord);
    }
    let terminal = b4_kernel::transition(
        &plan.snapshot,
        KernelCommand::CancelAttempt {
            task_id: claim.invocation.task_id.clone(),
            attempt_id: claim.invocation.attempt_id.clone(),
        },
        request.facts.clone(),
    )?;
    let prepared = PermissionPreparedUseWithheldCancellation {
        request,
        capture: captured,
        plan,
        terminal,
        claim: &claim,
    };
    let committed = store.commit_permission_use_withheld_cancellation(&prepared, budget)?;
    if committed.execution.thread_id != prepared.capture.resource.execution.thread_id
        || committed.execution.snapshot != prepared.terminal.snapshot
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(committed.execution)
}

pub(super) struct WithheldAccount {
    pub(super) budget: PermissionEvidenceBudget,
    origin: Option<Arc<PermissionRuntimeOrigin>>,
    initialization_attempted: bool,
}
impl Default for WithheldAccount {
    fn default() -> Self {
        Self {
            budget: PermissionEvidenceBudget::fresh_use_withheld_functional(),
            origin: None,
            initialization_attempted: false,
        }
    }
}
impl WithheldAccount {
    pub(super) fn begin(
        &mut self,
        request: &PermissionUseWithheldCancellationRequest<'_>,
    ) -> Result<(), PermissionError> {
        validate_approval_context(request.context)?;
        if self.origin.is_none() {
            if self.initialization_attempted {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.initialization_attempted = true;
            self.origin =
                Some(self.budget.begin_runtime_use_cancellation(
                    &request.target.execution_id,
                    request.context,
                )?);
        }
        self.budget.runtime_reserve(512, 8)
    }
}
