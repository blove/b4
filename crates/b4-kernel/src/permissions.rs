//! Capture caller intent for approving an actual ready task.
//!
//! This command grants no authority. The runtime must authorize and persist
//! an exact proposal before an effect may be dispatched.
use crate::{validate_snapshot, KernelError};
use b4_contract::{permissions::PermissionUseId, *};
use serde_json::{Map, Value};
use std::fmt;

/// Owned bindings to one actual required, unapproved ready task.
///
/// Callers must budget the capture scans and owned copies. A known use identity
/// is a marker identity, not evidence of permission or persisted authority.
pub struct PermissionAuthorizationCommand {
    execution_id: ExecutionId,
    expected_revision: u64,
    owner_generation: u64,
    turn_id: TurnId,
    definition: Definition,
    batch_index: usize,
    task_spec: TaskSpec,
    input: Map<String, Value>,
    use_id: PermissionUseId,
}

impl fmt::Debug for PermissionAuthorizationCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PermissionAuthorizationCommand([REDACTED])")
    }
}

impl PermissionAuthorizationCommand {
    /// Validate actual source state before capturing its full bindings.
    pub fn capture(
        before: &Snapshot,
        task_id: TaskId,
        use_id: PermissionUseId,
    ) -> Result<Self, KernelError> {
        validate_snapshot(before)?;
        let task = before
            .tasks
            .iter()
            .find(checked_ready_authorization_callable(
                |task: &&TaskRecord| task.spec.id == task_id,
            ))
            .ok_or(KernelError::UnknownTask)?;
        if before.status != ExecutionStatus::Running
            || before.failure.is_some()
            || !before
                .definition
                .batches
                .get(before.batch_index)
                .is_some_and(checked_ready_authorization_callable(
                    |batch: &Vec<TaskSpec>| batch.contains(&task.spec),
                ))
            || task.input != before.values
            || !task.spec.requires_approval
            || task.status != TaskStatus::Ready
            || task.approved
            || task.attempt_id.is_some()
            || task.result.is_some()
            || task.failure.is_some()
            || task.interrupt.is_some()
        {
            return Err(KernelError::InvalidTransition(
                "permission_task_not_unapproved_ready",
            ));
        }
        // Every checked use ID is 69 printable ASCII bytes, within the ordinary
        // interrupt ID bound. Keep conversion fallible if that contract changes.
        let marker = InterruptId::new(use_id.as_str()).map_err(
            checked_ready_authorization_callable(|_: b4_contract::IdError| {
                KernelError::InvalidTransition("permission_task_not_unapproved_ready")
            }),
        )?;
        if before
            .tasks
            .iter()
            .any(checked_ready_authorization_callable(|task: &TaskRecord| {
                task.interrupt
                    .as_ref()
                    .is_some_and(checked_ready_authorization_callable(
                        |pending: &PendingApproval| pending.id == marker,
                    ))
            }))
        {
            return Err(KernelError::IdCollision);
        }
        Ok(Self {
            execution_id: before.execution_id.clone(),
            expected_revision: before.revision,
            owner_generation: before.owner_generation,
            turn_id: before.turn_id.clone(),
            definition: before.definition.clone(),
            batch_index: before.batch_index,
            task_spec: task.spec.clone(),
            input: task.input.clone(),
            use_id,
        })
    }

    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    pub fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    pub fn owner_generation(&self) -> u64 {
        self.owner_generation
    }

    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    pub fn definition(&self) -> &Definition {
        &self.definition
    }

    pub fn batch_index(&self) -> usize {
        self.batch_index
    }

    pub fn task_spec(&self) -> &TaskSpec {
        &self.task_spec
    }

    pub fn input(&self) -> &Map<String, Value> {
        &self.input
    }

    pub fn use_id(&self) -> &PermissionUseId {
        &self.use_id
    }
}

/// Propose approval of the captured ready task without dispatching an effect.
///
/// The runtime must authorize and persist the exact proposal before dispatch.
/// Callers must budget the validation, comparisons and replacement snapshot.
pub fn authorize_ready(
    before: &Snapshot,
    command: &PermissionAuthorizationCommand,
    facts: TransitionFacts,
) -> Result<CommitPlan, KernelError> {
    validate_snapshot(before)?;
    if facts.now_ms < before.updated_at_ms {
        return Err(KernelError::ClockMovedBackwards);
    }
    if before.execution_id != command.execution_id
        || before.revision != command.expected_revision
        || before.owner_generation != command.owner_generation
        || before.turn_id != command.turn_id
        || before.definition != command.definition
        || before.batch_index != command.batch_index
    {
        return Err(KernelError::InvalidTransition(
            "permission_authorization_stale",
        ));
    }
    let index = before
        .tasks
        .iter()
        .position(checked_ready_authorization_callable(|task: &TaskRecord| {
            task.spec.id == command.task_spec.id
        }))
        .ok_or(KernelError::UnknownTask)?;
    let task = &before.tasks[index];
    if task.spec != command.task_spec || task.input != command.input {
        return Err(KernelError::InvalidTransition(
            "permission_authorization_stale",
        ));
    }
    if before.status != ExecutionStatus::Running
        || before.failure.is_some()
        || !before
            .definition
            .batches
            .get(before.batch_index)
            .is_some_and(checked_ready_authorization_callable(
                |batch: &Vec<TaskSpec>| batch.contains(&task.spec),
            ))
        || task.input != before.values
        || !task.spec.requires_approval
        || task.status != TaskStatus::Ready
        || task.approved
        || task.attempt_id.is_some()
        || task.result.is_some()
        || task.failure.is_some()
        || task.interrupt.is_some()
    {
        return Err(KernelError::InvalidTransition(
            "permission_task_not_unapproved_ready",
        ));
    }
    let marker = InterruptId::new(command.use_id.as_str()).map_err(
        checked_ready_authorization_callable(|_: b4_contract::IdError| {
            KernelError::InvalidTransition("permission_task_not_unapproved_ready")
        }),
    )?;
    if before
        .tasks
        .iter()
        .any(checked_ready_authorization_callable(|task: &TaskRecord| {
            task.interrupt
                .as_ref()
                .is_some_and(checked_ready_authorization_callable(
                    |pending: &PendingApproval| pending.id == marker,
                ))
        }))
    {
        return Err(KernelError::IdCollision);
    }
    let revision = before
        .revision
        .checked_add(1)
        .ok_or(KernelError::RevisionOverflow)?;
    let mut next = before.clone();
    next.tasks[index].approved = true;
    next.tasks[index].interrupt = Some(PendingApproval {
        id: marker,
        prompt: Value::Null,
    });
    next.revision = revision;
    next.updated_at_ms = facts.now_ms;
    validate_snapshot(&next)?;
    Ok(CommitPlan {
        expected_revision: before.revision,
        snapshot: next,
        dispatchable: Vec::new(),
    })
}

// Qualify each actual authorization callback value without invoking it.
fn checked_ready_authorization_callable<F>(value: F) -> F {
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}
