//! Authorized bounded reads and staged human permission acceptance.

use super::{
    validation::{check_permission_read_request, prepare_permission_read_context},
    PermissionCatalog, PermissionError, PermissionEvidenceBudget, PermissionReadPurpose,
    PermissionStore, PermissionView, PermissionViewSelector,
};
use crate::approval::{ApprovalAuthority, ApprovalContext, ApprovalError, ApprovalFence};
use b4_contract::{ExecutionId, TaskId};

/// A reader/policy projection, not human acceptance or dispatch authority.
/// The supplied authority represents the trusted current caller; context alone
/// authenticates nobody. Policy executes after the resource transaction closes.
#[allow(clippy::too_many_arguments)] // Explicit identity, purpose, fence and shared account.
pub fn read_permission_view<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    execution_id: &ExecutionId,
    fence: ApprovalFence,
    context: &ApprovalContext,
    purpose: PermissionReadPurpose,
    task_id: Option<&TaskId>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionView, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    check_permission_read_request(context, execution_id, task_id, budget)?;
    let resource = store.permission_resource(execution_id, budget)?;
    let invocation = prepare_permission_read_context(
        &resource, fence, context, catalog, purpose, task_id, budget,
    )?;
    // This comparison was prepaid with the context/fence quote above.
    if &resource.execution.snapshot.execution_id != execution_id {
        return Err(PermissionError::CorruptRecord);
    }
    authority
        .authorize(context, purpose.approval_operation(), &resource)
        .map_err(|_| ApprovalError::Unauthorized)?;
    let selector = PermissionViewSelector::new(
        &invocation,
        &resource.execution.snapshot.definition,
        purpose,
        task_id,
        &resource,
        None,
    );
    // No durable_permissions gate: false denotes the uninstalled full profile.
    // The combined port validates its separately qualified read profiles.
    store.permission_view(execution_id, fence, &selector, budget)
}

// This partial route hands the actual matched C2 to the sealed typed port.
pub(super) fn accept_permission_always_stage<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    execution_id: &ExecutionId,
    fence: ApprovalFence,
    request: &crate::approval::ApprovalAcceptanceRequest,
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::StoredExecution, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    super::validation::begin_permission_always_stage(budget)?;
    if !store.approval_durable() {
        return Err(ApprovalError::DurabilityRequired.into());
    }
    let first = read_permission_acceptance_view(
        store,
        authority,
        catalog,
        execution_id,
        fence,
        &request.context,
        None,
        budget,
    )?;
    super::validation::check_permission_always_eligibility(&first, request, budget)?;
    // The public read intentionally retained no transient InvocationContext.
    // Reconstruct it through the same paid current-resource helper for the gate.
    let invocation = prepare_permission_read_context(
        first.resource(),
        fence,
        &request.context,
        catalog,
        PermissionReadPurpose::Accept,
        None,
        budget,
    )?;
    super::validation::quote_permission_always_gate_selection(&first, request, budget)?;
    let mut captured_binding = None;
    let current = &first.execution().snapshot;
    for task in current
        .tasks
        .iter()
        .filter(|task| task.status == b4_contract::TaskStatus::WaitingForApproval)
    {
        let interrupt = task.interrupt.as_ref().ok_or(ApprovalError::InvalidGrant)?;
        let decision = request
            .decisions
            .iter()
            .find(|entry| entry.interrupt_id == interrupt.id)
            .ok_or(ApprovalError::InterruptSetMismatch)?;
        if !decision.cancelled
            && matches!(
                decision.decision,
                b4_contract::ApprovalDecision::Once | b4_contract::ApprovalDecision::Always
            )
        {
            captured_binding = Some(catalog.validate_call_capture(
                &invocation,
                &current.definition,
                task,
                budget,
            )?);
        }
    }
    let second = read_permission_acceptance_view(
        store,
        authority,
        catalog,
        execution_id,
        fence,
        &request.context,
        Some(&first),
        budget,
    )?;
    // Pay the C2 View argument transfer before entering the owning factory.
    budget.quote_permission_prepared_handoff()?;
    let prepared = super::PermissionPreparedAcceptance::from_matched_view(
        second,
        request,
        catalog,
        execution_id,
        fence,
        captured_binding.ok_or(PermissionError::UnsupportedProfile)?,
        budget,
    )?;
    // Exactly one same-account handoff to the separately qualified backend.
    // The stable trait default continues to refuse this port.
    store.commit_permission_acceptance(prepared, budget)
}

#[allow(clippy::too_many_arguments)] // Exact identities, prior capture and one shared account.
pub(super) fn read_permission_acceptance_view<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    execution_id: &ExecutionId,
    fence: ApprovalFence,
    context: &ApprovalContext,
    prior: Option<&PermissionView>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionView, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    check_permission_read_request(context, execution_id, None, budget)?;
    let resource = store.permission_resource(execution_id, budget)?;
    let invocation = prepare_permission_read_context(
        &resource,
        fence,
        context,
        catalog,
        PermissionReadPurpose::Accept,
        None,
        budget,
    )?;
    if &resource.execution.snapshot.execution_id != execution_id {
        return Err(PermissionError::CorruptRecord);
    }
    authority
        .authorize(
            context,
            crate::approval::ApprovalOperation::Accept,
            &resource,
        )
        .map_err(|_| ApprovalError::Unauthorized)?;
    let selector = PermissionViewSelector::new_acceptance(
        &invocation,
        &resource.execution.snapshot.definition,
        &resource,
        prior,
    );
    // Only this private Accept capture can match a prior whole View.
    store.permission_view(execution_id, fence, &selector, budget)
}
