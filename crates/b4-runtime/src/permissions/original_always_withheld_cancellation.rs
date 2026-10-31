//! Failure-owned original cancellation borrowing the genuine NewHuman claim.
use super::super::{PermissionCancellationAuthority, PermissionStore};
use super::original_always::{
    PermissionOriginalAlwaysObservation, PermissionOriginalAlwaysSelector,
};
use super::*;
use crate::approval::{validate_approval_context, ApprovalContext, ApprovalResource};
use crate::{RunCancellationTarget, StoreError};
use b4_contract::{KernelCommand, TaskStatus, TransitionFacts};

/// Management intent; the returned Driver failure supplies actual Work authority.
pub struct PermissionOriginalAlwaysWithheldCancellationRequest<'a> {
    pub context: &'a ApprovalContext,
    pub target: &'a RunCancellationTarget,
    pub facts: TransitionFacts,
}
/// Sealed two-transition mutation borrowing the actual original claim.
pub struct PermissionPreparedOriginalAlwaysWithheldCancellation<'a> {
    claim: &'a PermissionDispatchClaim<'a>,
    plan: CommitPlan,
    terminal: CommitPlan,
}
pub struct PermissionOriginalAlwaysWithheldCancellationCommit {
    execution: StoredExecution,
}
impl PermissionPreparedOriginalAlwaysWithheldCancellation<'_> {
    pub fn selector(&self) -> PermissionOriginalAlwaysSelector<'_> {
        PermissionOriginalAlwaysSelector {
            claim: self.claim,
            current: None,
        }
    }
    pub fn resource(&self) -> &ApprovalResource {
        &self.claim.resource
    }
    pub fn plan(&self) -> &CommitPlan {
        &self.plan
    }
    pub fn terminal_plan(&self) -> &CommitPlan {
        &self.terminal
    }
    /// Prepay bounded logical source/check work in the same retained allowance.
    /// The existing original reader keeps its own physical row/value limits.
    pub fn before_source_read(
        &self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        quote_source(budget)
    }
    pub fn acknowledge(
        &self,
        observed: PermissionOriginalAlwaysObservation,
    ) -> Result<PermissionOriginalAlwaysWithheldCancellationCommit, PermissionError> {
        if observed.resource != self.claim.resource {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionOriginalAlwaysWithheldCancellationCommit {
            execution: StoredExecution {
                thread_id: observed.resource.execution.thread_id,
                snapshot: self.terminal.snapshot.clone(),
            },
        })
    }
}
fn quote_source(budget: &mut PermissionEvidenceBudget) -> Result<(), PermissionError> {
    // Complete bounded 64KiB resource, three <=8KiB immutable records,
    // five <=1KiB journals, exact comparisons and their temporary owners.
    budget.runtime_reserve(1_048_576, 16_384)
}
fn target_matches(
    resource: &ApprovalResource,
    target: &RunCancellationTarget,
) -> Result<(), PermissionError> {
    let t = &resource.thread;
    let s = &resource.execution.snapshot;
    if t.id != target.thread_id
        || t.generation != target.thread_generation
        || t.resource_revision != target.resource_revision
        || resource.execution.thread_id != target.thread_id
        || t.active_execution_id.as_ref() != Some(&target.execution_id)
        || s.execution_id != target.execution_id
        || s.turn_id != target.turn_id
        || crate::ExecutionFence::from(s) != target.execution_fence
    {
        return Err(StoreError::RevisionConflict.into());
    }
    if s.revision != 4
        || s.status != b4_contract::ExecutionStatus::Running
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
pub(super) fn cancel<S: PermissionStore + ?Sized>(
    store: &S,
    authority: &dyn PermissionCancellationAuthority,
    catalog: &PermissionCatalog,
    request: &PermissionOriginalAlwaysWithheldCancellationRequest<'_>,
    claim: &PermissionDispatchClaim<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<StoredExecution, PermissionError> {
    if !store.permission_original_always_withheld_cancellation_atomic_durable() {
        return Err(PermissionError::UnsupportedProfile);
    }
    validate_approval_context(request.context)?;
    budget.runtime_reserve(1024, 32)?;
    target_matches(&claim.resource, request.target)?;
    let PermissionClaimWitness::NewHuman(w) = &claim.witness else {
        return Err(PermissionError::UnsupportedProfile);
    };
    if !Arc::ptr_eq(&claim.origin, &w.origin) {
        return Err(PermissionError::CorruptRecord);
    }
    let resource = &claim.resource;
    let s = &resource.execution.snapshot;
    if s.owner_generation != store.owner_generation() {
        return Err(StoreError::OwnerConflict.into());
    }
    let ctx = &claim.context;
    if ctx.origin != request.context.actor
        || ctx.policy_revision != request.context.policy_revision
        || resource.thread.origin.as_ref() != Some(&ctx.origin)
        || ctx.thread_id != resource.thread.id
        || ctx.thread_generation != resource.thread.generation
        || ctx.execution_revision != s.definition.revision
        || ctx.application_revision != catalog.application_identity().revision
    {
        return Err(crate::approval::ApprovalError::Unauthorized.into());
    }
    let task = &s.tasks[0];
    if claim.invocation.execution_id != s.execution_id
        || claim.invocation.turn_id != s.turn_id
        || claim.invocation.owner_generation != s.owner_generation
        || claim.invocation.task_id != task.spec.id
        || claim.invocation.effect_id != task.spec.effect_id
        || task.attempt_id.as_ref() != Some(&claim.invocation.attempt_id)
        || claim.invocation.input != task.input
    {
        return Err(PermissionError::CorruptRecord);
    }
    // Full definition and identity lookup only: stopping never evaluates the effect gate.
    budget.runtime_reserve(262_144, 4096)?;
    if catalog.current_definition(&s.definition.route) != Some(&s.definition) {
        return Err(PermissionError::InvalidCatalog);
    }
    let registration = catalog.original_always_registration(&s.definition, task)?;
    let scope = w.decision.scope();
    scope.canonical_digest(budget)?;
    if scope.thread_id() != &resource.thread.id
        || scope.thread_generation() != &resource.thread.generation
        || scope.origin() != &ctx.origin
        || scope.application().application() != catalog.application_identity().application
        || scope.application().revision() != catalog.application_identity().revision
        || scope.route() != &s.definition.route
        || scope.definition() != &s.definition
        || scope.effect_id() != &registration.effect_id
        || scope.tool_id() != &registration.tool_id
        || scope.tool_revision() != &registration.tool_revision
        || scope.constraint_revision() != &registration.constraint_revision
        || scope.policy_revision() != ctx.policy_revision
    {
        return Err(PermissionError::ConstraintDenied);
    }
    let selector = PermissionOriginalAlwaysSelector {
        claim,
        current: None,
    };
    quote_source(budget)?;
    let observed = store.capture_permission_original_always(&selector)?;
    if observed.resource != *resource {
        return Err(PermissionError::CorruptRecord);
    }
    // The source capture has returned and released its Deferred guard.
    authority.authorize_cancel(request.context, resource, request.target)?;
    budget.runtime_reserve(1_048_576, 16_384)?;
    let plan = b4_kernel::transition(
        s,
        KernelCommand::Cancel {
            turn_id: s.turn_id.clone(),
            owner_generation: s.owner_generation,
        },
        request.facts.clone(),
    )?;
    if !super::super::original_always_cancellation_relation::snapshot_matches(s, &plan.snapshot) {
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
    let prepared = PermissionPreparedOriginalAlwaysWithheldCancellation {
        claim,
        plan,
        terminal,
    };
    let committed =
        store.commit_permission_original_always_withheld_cancellation(&prepared, budget)?;
    if committed.execution.thread_id != resource.execution.thread_id
        || committed.execution.snapshot != prepared.terminal.snapshot
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(committed.execution)
}
