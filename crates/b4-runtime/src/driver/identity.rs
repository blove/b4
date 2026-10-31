use crate::{
    EffectInvocation, PreparedEffect, RuntimeError, Store, StoreError, StoredExecution,
    ThreadRecord,
};
use b4_contract::{Definition, ExecutionId, TaskStatus, TurnId};

pub(super) struct OwnedTurn {
    thread: ThreadRecord,
    execution_id: ExecutionId,
    turn_id: TurnId,
    owner: u64,
    definition: Definition,
}
impl OwnedTurn {
    pub(super) fn capture<S: Store>(
        store: &S,
        initial: &StoredExecution,
    ) -> Result<Self, RuntimeError> {
        let view = store
            .thread_view(&initial.thread_id)?
            .ok_or(StoreError::ThreadNotFound)?;
        let identity = Self {
            thread: view.thread,
            execution_id: initial.snapshot.execution_id.clone(),
            turn_id: initial.snapshot.turn_id.clone(),
            owner: initial.snapshot.owner_generation,
            definition: initial.snapshot.definition.clone(),
        };
        identity.reload(store)?;
        Ok(identity)
    }
    pub(super) fn reload<S: Store>(&self, store: &S) -> Result<StoredExecution, RuntimeError> {
        let view = store
            .thread_view(&self.thread.id)?
            .ok_or(StoreError::ThreadNotFound)?;
        let current = view.execution.ok_or(StoreError::ExecutionNotFound)?;
        if view.thread.id != self.thread.id
            || view.thread.generation != self.thread.generation
            || view.thread.origin != self.thread.origin
            || view.thread.created_at_ms != self.thread.created_at_ms
            || view.thread.metadata != self.thread.metadata
            || view.thread.active_execution_id.as_ref() != Some(&self.execution_id)
            || current.thread_id != self.thread.id
            || current.snapshot.execution_id != self.execution_id
            || current.snapshot.turn_id != self.turn_id
            || current.snapshot.definition != self.definition
        {
            return Err(StoreError::Corrupt("driver_owned_turn_changed".into()).into());
        }
        if store.owner_generation() != self.owner || current.snapshot.owner_generation != self.owner
        {
            return Err(StoreError::OwnerConflict.into());
        }
        b4_kernel::validate_snapshot(&current.snapshot)?;
        Ok(current)
    }
    pub(super) fn invocation_matches(
        &self,
        current: &StoredExecution,
        call: &EffectInvocation,
    ) -> bool {
        call.execution_id == self.execution_id
            && call.turn_id == self.turn_id
            && call.owner_generation == self.owner
            && current.thread_id == self.thread.id
            && current.snapshot.execution_id == self.execution_id
            && current.snapshot.turn_id == self.turn_id
            && current.snapshot.owner_generation == self.owner
            && current.snapshot.definition == self.definition
            && current.snapshot.tasks.iter().any(|task| {
                task.spec.id == call.task_id
                    && task.spec.effect_id == call.effect_id
                    && task.attempt_id.as_ref() == Some(&call.attempt_id)
                    && task.input == call.input
            })
    }
    pub(super) fn prepared_matches(&self, prepared: &PreparedEffect) -> bool {
        self.invocation_matches(&prepared.execution, &prepared.invocation)
            && prepared.execution.snapshot.tasks.iter().any(|t| {
                t.spec.id == prepared.invocation.task_id && t.status == TaskStatus::InFlight
            })
    }
}
