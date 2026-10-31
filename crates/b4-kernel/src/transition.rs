use crate::{reducer, validation};
use b4_contract::*;
use serde_json::{Map, Value};
use std::{collections::BTreeSet, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KernelError {
    InvalidSnapshot(String),
    InvalidTransition(&'static str),
    UnknownTask,
    AttemptMismatch,
    ResultConflict,
    InterruptSetMismatch,
    RouteMismatch,
    StaleTurn,
    BudgetExceeded,
    IdCollision,
    RevisionOverflow,
    ClockMovedBackwards,
}

impl fmt::Display for KernelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for KernelError {}

/// Construct an accepted execution from caller-supplied identities and time.
pub fn new_execution(
    execution_id: ExecutionId,
    turn_id: TurnId,
    owner_generation: u64,
    definition: Definition,
    values: Map<String, Value>,
    facts: TransitionFacts,
) -> Result<Snapshot, KernelError> {
    let snapshot = Snapshot {
        schema_version: SNAPSHOT_VERSION,
        revision: 0,
        execution_id,
        turn_history: vec![turn_id.clone()],
        attempt_history: vec![],
        turn_id,
        owner_generation,
        definition,
        status: ExecutionStatus::Accepted,
        batch_index: 0,
        values,
        tasks: vec![],
        attempts_used: 0,
        updated_at_ms: facts.now_ms,
        output: None,
        failure: None,
    };
    validation::validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

/// Return a proposed atomic change. This function performs no effect or commit.
///
/// The caller must persist each replacement using `expected_revision`.
/// `dispatchable` lists ready candidates: it does not authorize invocation.
/// For each candidate the host must authorize, propose a separate `Dispatch`
/// command with a fresh attempt, commit that task's `InFlight` snapshot, then
/// invoke the effect. Decisions have already passed authorization and grant
/// checks at the runtime boundary; the kernel does not confer access.
pub fn transition(
    snapshot: &Snapshot,
    command: KernelCommand,
    facts: TransitionFacts,
) -> Result<CommitPlan, KernelError> {
    validation::validate_snapshot(snapshot)?;
    if facts.now_ms < snapshot.updated_at_ms {
        return Err(KernelError::ClockMovedBackwards);
    }
    let mut next = snapshot.clone();
    match command {
        KernelCommand::Start => {
            require(&next, ExecutionStatus::Accepted)?;
            next.status = ExecutionStatus::Running;
            initialize_batch(&mut next);
            settle(&mut next);
        }
        KernelCommand::Dispatch {
            task_id,
            attempt_id,
        } => {
            require(&next, ExecutionStatus::Running)?;
            require_scheduling(&next)?;
            let index = task_index(&next, &task_id)?;
            let task = &next.tasks[index];
            if task.status != TaskStatus::Ready || (task.spec.requires_approval && !task.approved) {
                return Err(KernelError::InvalidTransition("task_not_authorized_ready"));
            }
            if next.attempts_used >= next.definition.max_attempts {
                return Err(KernelError::BudgetExceeded);
            }
            if next.attempt_history.contains(&attempt_id) {
                return Err(KernelError::IdCollision);
            }
            next.attempt_history.push(attempt_id.clone());
            next.tasks[index].status = TaskStatus::InFlight;
            next.tasks[index].attempt_id = Some(attempt_id);
            next.attempts_used += 1;
        }
        KernelCommand::Complete {
            task_id,
            attempt_id,
            result,
        } => {
            let index = task_index(&next, &task_id)?;
            let task = &next.tasks[index];
            if task.attempt_id.as_ref() != Some(&attempt_id) {
                return Err(KernelError::AttemptMismatch);
            }
            if task.failure.is_some() {
                return Err(KernelError::ResultConflict);
            }
            if let Some(previous) = &task.result {
                if previous != &result {
                    return Err(KernelError::ResultConflict);
                }
                return Ok(CommitPlan {
                    expected_revision: snapshot.revision,
                    snapshot: snapshot.clone(),
                    dispatchable: vec![],
                });
            }
            if task.status != TaskStatus::InFlight
                || !matches!(
                    next.status,
                    ExecutionStatus::Running | ExecutionStatus::CancelRequested
                )
            {
                return Err(KernelError::InvalidTransition("task_not_in_flight"));
            }
            next.tasks[index].status = TaskStatus::Succeeded;
            next.tasks[index].result = Some(result);
            settle(&mut next);
        }
        KernelCommand::Park {
            task_id,
            interrupt_id,
            prompt,
        } => {
            require(&next, ExecutionStatus::Running)?;
            require_scheduling(&next)?;
            let index = task_index(&next, &task_id)?;
            let task = &next.tasks[index];
            if task.status != TaskStatus::Ready || !task.spec.requires_approval || task.approved {
                return Err(KernelError::InvalidTransition(
                    "task_not_awaiting_permission",
                ));
            }
            if next.tasks.iter().any(|task| {
                task.interrupt
                    .as_ref()
                    .is_some_and(|pending| pending.id == interrupt_id)
            }) {
                return Err(KernelError::IdCollision);
            }
            next.tasks[index].status = TaskStatus::WaitingForApproval;
            next.tasks[index].interrupt = Some(PendingApproval {
                id: interrupt_id,
                prompt,
            });
            settle(&mut next);
        }
        KernelCommand::FailAttempt {
            task_id,
            attempt_id,
            failure,
        } => {
            if failure.is_empty() || failure.len() > MAX_TASK_FAILURE_BYTES {
                return Err(KernelError::InvalidTransition("invalid_task_failure"));
            }
            let index = task_index(&next, &task_id)?;
            let task = &next.tasks[index];
            if task.attempt_id.as_ref() != Some(&attempt_id) {
                return Err(KernelError::AttemptMismatch);
            }
            if let Some(previous) = &task.failure {
                if previous != &failure {
                    return Err(KernelError::ResultConflict);
                }
                return Ok(CommitPlan {
                    expected_revision: snapshot.revision,
                    snapshot: snapshot.clone(),
                    dispatchable: vec![],
                });
            }
            if task.result.is_some() {
                return Err(KernelError::ResultConflict);
            }
            if task.status != TaskStatus::InFlight
                || !matches!(
                    next.status,
                    ExecutionStatus::Running | ExecutionStatus::CancelRequested
                )
            {
                return Err(KernelError::InvalidTransition("task_not_in_flight"));
            }
            next.tasks[index].status = TaskStatus::Failed;
            next.tasks[index].failure = Some(failure.clone());
            if next.failure.is_none() {
                next.failure = Some(failure);
            }
            next.status = ExecutionStatus::Running;
            for task in &mut next.tasks {
                if matches!(
                    task.status,
                    TaskStatus::Ready | TaskStatus::WaitingForApproval
                ) {
                    task.status = TaskStatus::Cancelled;
                }
            }
            settle(&mut next);
        }
        KernelCommand::Resume {
            route,
            decisions,
            turn_id,
        } => {
            require(&next, ExecutionStatus::WaitingForApproval)?;
            if route != next.definition.route {
                return Err(KernelError::RouteMismatch);
            }
            if next.turn_history.contains(&turn_id) {
                return Err(KernelError::IdCollision);
            }
            let pending: BTreeSet<_> = next
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::WaitingForApproval)
                .map(|task| {
                    task.interrupt
                        .as_ref()
                        .expect("validated approval")
                        .id
                        .clone()
                })
                .collect();
            let submitted: BTreeSet<_> = decisions
                .iter()
                .map(|entry| entry.interrupt_id.clone())
                .collect();
            if pending != submitted || submitted.len() != decisions.len() || submitted.is_empty() {
                return Err(KernelError::InterruptSetMismatch);
            }
            for task in &mut next.tasks {
                if task.status != TaskStatus::WaitingForApproval {
                    continue;
                }
                let id = &task.interrupt.as_ref().expect("validated approval").id;
                let decision = &decisions
                    .iter()
                    .find(|entry| &entry.interrupt_id == id)
                    .expect("complete set")
                    .decision;
                match decision {
                    ApprovalDecision::Once | ApprovalDecision::Always => {
                        task.approved = true;
                        task.status = TaskStatus::Ready;
                    }
                    ApprovalDecision::Deny => task.status = TaskStatus::Denied,
                }
            }
            next.turn_history.push(turn_id.clone());
            next.turn_id = turn_id;
            next.status = ExecutionStatus::Running;
            settle(&mut next);
        }
        KernelCommand::Cancel {
            turn_id,
            owner_generation,
        } => {
            if turn_id != next.turn_id || owner_generation != next.owner_generation {
                return Err(KernelError::StaleTurn);
            }
            if next.status == ExecutionStatus::Accepted {
                initialize_batch(&mut next);
            } else {
                require(&next, ExecutionStatus::Running)?;
            }
            require_scheduling(&next)?;
            next.status = ExecutionStatus::CancelRequested;
            for task in &mut next.tasks {
                if matches!(
                    task.status,
                    TaskStatus::Ready | TaskStatus::WaitingForApproval
                ) {
                    task.status = TaskStatus::Cancelled;
                }
            }
            settle(&mut next);
        }
        KernelCommand::CancelAttempt {
            task_id,
            attempt_id,
        } => {
            let index = task_index(&next, &task_id)?;
            let task = &next.tasks[index];
            if task.attempt_id.as_ref() != Some(&attempt_id) {
                return Err(KernelError::AttemptMismatch);
            }
            if task.result.is_some() || task.failure.is_some() {
                return Err(KernelError::ResultConflict);
            }
            let draining = next.status == ExecutionStatus::CancelRequested
                || (next.status == ExecutionStatus::Running && next.failure.is_some());
            if task.status == TaskStatus::Cancelled
                && (draining
                    || matches!(
                        next.status,
                        ExecutionStatus::Cancelled | ExecutionStatus::Failed
                    ))
            {
                return Ok(CommitPlan {
                    expected_revision: snapshot.revision,
                    snapshot: snapshot.clone(),
                    dispatchable: vec![],
                });
            }
            if !draining || task.status != TaskStatus::InFlight {
                return Err(KernelError::InvalidTransition("task_not_cancelling"));
            }
            next.tasks[index].status = TaskStatus::Cancelled;
            settle(&mut next);
        }
        KernelCommand::Recover {
            available_revision,
            turn_id,
            owner_generation,
        } => {
            if !matches!(
                next.status,
                ExecutionStatus::Accepted
                    | ExecutionStatus::Running
                    | ExecutionStatus::WaitingForApproval
                    | ExecutionStatus::CancelRequested
                    | ExecutionStatus::RecoveryBlocked
            ) {
                return Err(KernelError::InvalidTransition("execution_is_terminal"));
            }
            if owner_generation <= next.owner_generation || next.turn_history.contains(&turn_id) {
                return Err(KernelError::StaleTurn);
            }
            next.owner_generation = owner_generation;
            next.turn_history.push(turn_id.clone());
            next.turn_id = turn_id;
            if available_revision != next.definition.revision {
                next.status = ExecutionStatus::RecoveryBlocked;
                next.failure = Some("execution_revision_unavailable".into());
            } else if next.status != ExecutionStatus::RecoveryBlocked {
                let cancelling = next.status == ExecutionStatus::CancelRequested;
                let draining_failure = next
                    .tasks
                    .iter()
                    .any(|task| task.status == TaskStatus::Failed);
                let mut unknown = false;
                for task in &mut next.tasks {
                    if task.status != TaskStatus::InFlight {
                        continue;
                    }
                    match task.spec.recovery {
                        RecoveryPolicy::Pure | RecoveryPolicy::Idempotent => {
                            task.status = if cancelling || draining_failure {
                                TaskStatus::Cancelled
                            } else {
                                TaskStatus::Ready
                            };
                        }
                        RecoveryPolicy::Reconcilable | RecoveryPolicy::Unsafe => {
                            task.status = TaskStatus::OutcomeUnknown;
                            unknown = true;
                        }
                    }
                }
                if unknown {
                    next.status = ExecutionStatus::RecoveryBlocked;
                    next.failure = Some("effect_outcome_unknown".into());
                } else {
                    settle(&mut next);
                }
            }
        }
    }
    next.revision = snapshot
        .revision
        .checked_add(1)
        .ok_or(KernelError::RevisionOverflow)?;
    next.updated_at_ms = facts.now_ms;
    validation::validate_snapshot(&next)?;
    let dispatchable = ready_tasks(&next);
    Ok(CommitPlan {
        expected_revision: snapshot.revision,
        snapshot: next,
        dispatchable,
    })
}

fn require(snapshot: &Snapshot, status: ExecutionStatus) -> Result<(), KernelError> {
    if snapshot.status == status {
        Ok(())
    } else {
        Err(KernelError::InvalidTransition("execution_status"))
    }
}

fn task_index(snapshot: &Snapshot, id: &TaskId) -> Result<usize, KernelError> {
    snapshot
        .tasks
        .iter()
        .position(|task| &task.spec.id == id)
        .ok_or(KernelError::UnknownTask)
}

fn require_scheduling(snapshot: &Snapshot) -> Result<(), KernelError> {
    if snapshot.failure.is_some() {
        Err(KernelError::InvalidTransition("execution_failure_drain"))
    } else {
        Ok(())
    }
}

fn initialize_batch(snapshot: &mut Snapshot) {
    if let Some(batch) = snapshot.definition.batches.get(snapshot.batch_index) {
        snapshot.tasks.extend(batch.iter().map(|spec| TaskRecord {
            spec: spec.clone(),
            status: TaskStatus::Ready,
            input: snapshot.values.clone(),
            attempt_id: None,
            result: None,
            failure: None,
            interrupt: None,
            approved: false,
        }));
    }
}

// The cancellation scan uses this exact function-pointer predicate type.
// Its constant construction adds no runtime captured owner or loan.
const CANCEL_TASK_IN_FLIGHT: fn(&TaskRecord) -> bool = |task| task.status == TaskStatus::InFlight;
const _: () = assert!(std::mem::size_of::<fn(&TaskRecord) -> bool>() <= 8);

// The selected shared scan returns this actual Option from Iter::next.
const _: () = assert!(std::mem::size_of::<Option<&'static TaskRecord>>() <= 8);
// The selected FnMut call passes this actual one-element argument tuple.
const _: () = assert!(std::mem::size_of::<(&'static TaskRecord,)>() <= 8);

fn settle(snapshot: &mut Snapshot) {
    if snapshot.status == ExecutionStatus::CancelRequested {
        if !snapshot.tasks.iter().any(CANCEL_TASK_IN_FLIGHT) {
            snapshot.status = ExecutionStatus::Cancelled;
        }
        return;
    }
    if snapshot.status != ExecutionStatus::Running {
        return;
    }
    if snapshot.failure.is_some() {
        if !snapshot
            .tasks
            .iter()
            .any(|task| task.status == TaskStatus::InFlight)
        {
            snapshot.status = ExecutionStatus::Failed;
        }
        return;
    }
    if snapshot.batch_index == snapshot.definition.batches.len() {
        snapshot.status = ExecutionStatus::Completed;
        snapshot.output = Some(Value::Object(snapshot.values.clone()));
        return;
    }
    let batch = &snapshot.definition.batches[snapshot.batch_index];
    let tasks: Vec<_> = batch
        .iter()
        .map(|spec| &snapshot.tasks[task_index(snapshot, &spec.id).expect("validated batch")])
        .collect();
    if tasks
        .iter()
        .all(|task| matches!(task.status, TaskStatus::Succeeded | TaskStatus::Denied))
    {
        match reducer::reduce(&snapshot.values, &tasks) {
            Ok(values) => snapshot.values = values,
            Err(reason) => {
                snapshot.status = ExecutionStatus::Failed;
                snapshot.failure = Some(reason.into());
                return;
            }
        }
        snapshot.batch_index += 1;
        initialize_batch(snapshot);
        if snapshot.batch_index == snapshot.definition.batches.len() {
            snapshot.status = ExecutionStatus::Completed;
            snapshot.output = Some(Value::Object(snapshot.values.clone()));
        }
    } else if tasks.iter().all(|task| {
        matches!(
            task.status,
            TaskStatus::Succeeded | TaskStatus::Denied | TaskStatus::WaitingForApproval
        )
    }) {
        snapshot.status = ExecutionStatus::WaitingForApproval;
    }
}

fn ready_tasks(snapshot: &Snapshot) -> Vec<TaskId> {
    if snapshot.status != ExecutionStatus::Running
        || snapshot.failure.is_some()
        || snapshot.attempts_used >= snapshot.definition.max_attempts
    {
        return vec![];
    }
    snapshot
        .tasks
        .iter()
        .filter(|task| {
            task.status == TaskStatus::Ready && (!task.spec.requires_approval || task.approved)
        })
        .map(|task| task.spec.id.clone())
        .collect()
}

// Genuine carriers on the selected cancellation success and error paths.
const _: () = assert!(std::mem::size_of::<Result<(), KernelError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, KernelError>, ()>>()
        <= 128
);
const _: () = assert!(std::mem::size_of::<Option<u64>>() <= 16);
