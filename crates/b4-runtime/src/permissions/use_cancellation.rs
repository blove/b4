//! Dedicated management request for an already entered NewUse callback.
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

/// Caller sequencing must establish actual callback entry before this request.
/// A Dispatch snapshot alone is not proof that native code has entered.
pub struct PermissionUseCancellationRequest<'a> {
    pub context: &'a ApprovalContext,
    pub target: &'a RunCancellationTarget,
    pub facts: TransitionFacts,
}
/// Sealed source selection; no generic store mutation authority.
pub struct PermissionUseCancellationSelector<'a> {
    request: &'a PermissionUseCancellationRequest<'a>,
    origin: &'a Arc<PermissionRuntimeOrigin>,
    expected: Option<&'a PermissionUseCancellationCapture>,
}
pub struct PermissionUseCancellationCapture {
    resource: ApprovalResource,
    witness: Option<PermissionRuntimeUseWitness>,
    origin: Arc<PermissionRuntimeOrigin>,
    matched: bool,
}
pub struct PermissionPreparedUseCancellation<'a> {
    request: &'a PermissionUseCancellationRequest<'a>,
    capture: PermissionUseCancellationCapture,
    plan: CommitPlan,
}
pub struct PermissionUseCancellationCommit {
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
impl PermissionUseCancellationSelector<'_> {
    pub fn execution_id(&self) -> &ExecutionId {
        &self.request.target.execution_id
    }
    pub fn begin_read<'a>(
        &'a self,
        resource: &'a ApprovalResource,
        budget: &'a mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeUseSession<'a>, PermissionError> {
        target_matches(resource, self.request.target)?;
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
    ) -> Result<PermissionUseCancellationCapture, PermissionError> {
        target_matches(&resource, self.request.target)?;
        if resource.execution.snapshot.owner_generation != owner {
            return Err(StoreError::OwnerConflict.into());
        }
        let witness = if self.expected.is_some() {
            evidence.require_match(self.origin)?;
            None
        } else {
            Some(evidence.into_capture(self.origin)?)
        };
        Ok(PermissionUseCancellationCapture {
            resource,
            witness,
            origin: Arc::clone(self.origin),
            matched: self.expected.is_some(),
        })
    }
}
impl PermissionPreparedUseCancellation<'_> {
    pub fn selector(&self) -> PermissionUseCancellationSelector<'_> {
        PermissionUseCancellationSelector {
            request: self.request,
            origin: &self.capture.origin,
            expected: Some(&self.capture),
        }
    }
    pub fn plan(&self) -> &CommitPlan {
        &self.plan
    }
    pub fn resource(&self) -> &ApprovalResource {
        &self.capture.resource
    }
    /// The typed writer calls this only after exact immediate source comparison.
    pub fn acknowledge(
        &self,
        observed: PermissionUseCancellationCapture,
    ) -> Result<PermissionUseCancellationCommit, PermissionError> {
        if !observed.matched
            || observed.witness.is_some()
            || observed.resource != self.capture.resource
            || !Arc::ptr_eq(&observed.origin, &self.capture.origin)
        {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionUseCancellationCommit {
            execution: StoredExecution {
                thread_id: observed.resource.execution.thread_id,
                snapshot: self.plan.snapshot.clone(),
            },
        })
    }
}

pub fn request_permission_use_cancellation<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    request: &PermissionUseCancellationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<StoredExecution, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: PermissionCancellationAuthority + ?Sized,
{
    if !store.permission_use_cancellation_atomic_durable() {
        return Err(PermissionError::UnsupportedProfile);
    }
    validate_approval_context(request.context)?;
    let origin =
        budget.begin_runtime_use_cancellation(&request.target.execution_id, request.context)?;
    let selector = PermissionUseCancellationSelector {
        request,
        origin: &origin,
        expected: None,
    };
    let captured = store.capture_permission_use_cancellation(&selector, budget)?;
    if !Arc::ptr_eq(&origin, &captured.origin) || captured.matched {
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
    let prepared = PermissionPreparedUseCancellation {
        request,
        capture: captured,
        plan,
    };
    let committed = store.commit_permission_use_cancellation(&prepared, budget)?;
    if committed.execution.thread_id != prepared.capture.resource.execution.thread_id
        || committed.execution.snapshot != prepared.plan.snapshot
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(committed.execution)
}
