//! A successful typed Ready writer is the sole confirmation producer.
use super::*;
use crate::{approval::ApprovalResource, StoredExecution, ThreadRecord};
use b4_contract::{ExecutionStatus, KernelCommand, TaskStatus, TransitionFacts};
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadyOrigin {
    OriginalAlways,
    NewUse,
}
/// Opaque evidence of an actual validated atomic Ready cancellation.
pub struct PermissionReadyCancellationConfirmation {
    pub(super) origin: ReadyOrigin,
    pub(super) before: ApprovalResource,
    pub(super) execution: StoredExecution,
    pub(super) after_thread: ThreadRecord,
}
impl PermissionReadyCancellationConfirmation {
    pub fn execution(&self) -> &StoredExecution {
        &self.execution
    }
    pub(super) fn from_commit(
        origin: ReadyOrigin,
        before: &ApprovalResource,
        execution: StoredExecution,
        facts: &TransitionFacts,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Self, PermissionError> {
        // Additional confirmation owners, Kernel projection and comparisons are paid
        // only on this new path, before any copy or transition.
        let snapshot_graph = super::validation::runtime_compat::capture_dispatch_snapshot(
            &before.execution.snapshot,
            budget,
        )?;
        let thread_graph =
            super::validation::runtime_compat::measure_post_thread(&before.thread, budget)?;
        budget.runtime_ready_confirmation_owners(snapshot_graph[0], thread_graph)?;
        let s = &before.execution.snapshot;
        if s.status != ExecutionStatus::Running
            || s.revision
                != match origin {
                    ReadyOrigin::OriginalAlways => 3,
                    ReadyOrigin::NewUse => 2,
                }
            || s.tasks.len() != 1
            || s.tasks[0].status != TaskStatus::Ready
            || s.tasks[0].attempt_id.is_some()
            || s.attempts_used != 0
            || !s.attempt_history.is_empty()
        {
            return Err(PermissionError::CorruptRecord);
        }
        let expected = b4_kernel::transition(
            s,
            KernelCommand::Cancel {
                turn_id: s.turn_id.clone(),
                owner_generation: s.owner_generation,
            },
            facts.clone(),
        )?;
        super::validation::runtime_compat::capture_dispatch_snapshot(&execution.snapshot, budget)?;
        if execution.thread_id != before.execution.thread_id
            || execution.snapshot != expected.snapshot
            || execution.snapshot.status != ExecutionStatus::Cancelled
        {
            return Err(PermissionError::CorruptRecord);
        }
        let mut after_thread = before.thread.clone();
        after_thread.resource_revision = after_thread
            .resource_revision
            .checked_add(1)
            .ok_or(PermissionError::LimitExceeded)?;
        Ok(Self {
            origin,
            before: before.clone(),
            execution,
            after_thread,
        })
    }
}
