//! Exact cancellation projection of an already sealed original Always invocation.
use super::PermissionError;
use crate::approval::ApprovalResource;
use b4_contract::{ExecutionStatus, KernelCommand, Snapshot, TransitionFacts};

pub(super) fn snapshot_matches(running: &Snapshot, current: &Snapshot) -> bool {
    if running.revision != 4
        || running.status != ExecutionStatus::Running
        || current.status != ExecutionStatus::CancelRequested
        || current.updated_at_ms < running.updated_at_ms
    {
        return false;
    }
    b4_kernel::transition(
        running,
        KernelCommand::Cancel {
            turn_id: running.turn_id.clone(),
            owner_generation: running.owner_generation,
        },
        TransitionFacts {
            now_ms: current.updated_at_ms,
        },
    )
    .is_ok_and(|plan| plan.snapshot == *current)
}

pub(super) fn require_current(
    running: &ApprovalResource,
    current: &ApprovalResource,
    controlled: bool,
) -> Result<(), PermissionError> {
    if current == running {
        return Ok(());
    }
    let a = &running.thread;
    let b = &current.thread;
    if !controlled
        || !snapshot_matches(&running.execution.snapshot, &current.execution.snapshot)
        || current.execution.thread_id != running.execution.thread_id
        || a.resource_revision.checked_add(1) != Some(b.resource_revision)
        || a.id != b.id
        || a.generation != b.generation
        || a.active_execution_id != b.active_execution_id
        || a.created_at_ms != b.created_at_ms
        || a.metadata != b.metadata
        || a.origin != b.origin
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(())
}
