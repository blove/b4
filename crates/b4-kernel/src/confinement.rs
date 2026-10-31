use crate::{validate_snapshot, KernelError};
use b4_contract::{CommitPlan, ExecutionStatus, Snapshot, TransitionFacts, TurnId};

mod shape;

#[cfg(test)]
mod tests;

/// The fixed observation for an explicitly authorized prior-owner block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryConfinementReason {
    ExactDefinitionUnavailable,
    RequiredAuthorityUnavailable,
    DispatchNotAuthorized,
}

/// Propose a no-dispatch recovery block without changing ordinary recovery.
pub fn propose_recovery_confinement(
    snapshot: &Snapshot,
    reason: RecoveryConfinementReason,
    turn_id: TurnId,
    current_owner: u64,
    facts: TransitionFacts,
) -> Result<CommitPlan, KernelError> {
    // Guard the borrowed representation before recursive clone or validation.
    // Backend raw input and aggregate accounting remain separate obligations.
    shape::check_snapshot(snapshot, 8_388_608)?;
    validate_snapshot(snapshot)?;
    if facts.now_ms < snapshot.updated_at_ms {
        return Err(KernelError::ClockMovedBackwards);
    }
    if snapshot.status == ExecutionStatus::RecoveryBlocked {
        return Err(KernelError::InvalidTransition("execution_already_blocked"));
    }
    if !matches!(
        snapshot.status,
        ExecutionStatus::Accepted
            | ExecutionStatus::Running
            | ExecutionStatus::WaitingForApproval
            | ExecutionStatus::CancelRequested
    ) {
        return Err(KernelError::InvalidTransition("execution_is_terminal"));
    }
    if current_owner <= snapshot.owner_generation || snapshot.turn_history.contains(&turn_id) {
        return Err(KernelError::StaleTurn);
    }
    let revision = snapshot
        .revision
        .checked_add(1)
        .ok_or(KernelError::RevisionOverflow)?;
    let mut next = snapshot.clone();
    next.status = ExecutionStatus::RecoveryBlocked;
    next.failure = Some(
        match reason {
            RecoveryConfinementReason::ExactDefinitionUnavailable => {
                "recovery_definition_unavailable"
            }
            RecoveryConfinementReason::RequiredAuthorityUnavailable => {
                "recovery_authority_unavailable"
            }
            RecoveryConfinementReason::DispatchNotAuthorized => "recovery_dispatch_not_authorized",
        }
        .into(),
    );
    next.owner_generation = current_owner;
    next.turn_history.push(turn_id.clone());
    next.turn_id = turn_id;
    next.revision = revision;
    next.updated_at_ms = facts.now_ms;
    // The added turn can exceed either the node or encoded replacement cap.
    shape::check_snapshot(&next, 1_048_576)?;
    validate_snapshot(&next)?;
    Ok(CommitPlan {
        expected_revision: snapshot.revision,
        snapshot: next,
        dispatchable: vec![],
    })
}
