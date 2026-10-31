use crate::{reducer, KernelError};
use b4_contract::*;
use std::collections::BTreeSet;

fn checked_finite_callback<F>(callback: F) -> F {
    const { assert!(std::mem::size_of::<F>() <= 4_096) };
    callback
}
fn checked_spec_id_map<'a, F>(callback: F) -> F
where
    F: for<'b> FnMut(&'b &'a TaskSpec) -> &'a TaskId,
{
    checked_finite_callback(callback)
}
fn checked_spec_effect_map<'a, F>(callback: F) -> F
where
    F: for<'b> FnMut(&'b &'a TaskSpec) -> &'a EffectId,
{
    checked_finite_callback(callback)
}

fn invalid(message: &str) -> KernelError {
    KernelError::InvalidSnapshot(message.into())
}

/// Fail closed before using a restored snapshot as a scheduling authority.
pub fn validate_snapshot(snapshot: &Snapshot) -> Result<(), KernelError> {
    snapshot
        .validate_version()
        .map_err(checked_finite_callback(|_: SnapshotVersionError| {
            invalid("unsupported_snapshot_version")
        }))?;
    if snapshot.owner_generation == 0 || snapshot.definition.max_attempts == 0 {
        return Err(invalid("invalid_owner_or_budget"));
    }
    let turns: BTreeSet<_> = snapshot.turn_history.iter().collect();
    let attempt_history: BTreeSet<_> = snapshot.attempt_history.iter().collect();
    if turns.len() != snapshot.turn_history.len()
        || snapshot.turn_history.last() != Some(&snapshot.turn_id)
        || attempt_history.len() != snapshot.attempt_history.len()
        || snapshot.attempt_history.len() != snapshot.attempts_used as usize
    {
        return Err(invalid("invalid_identity_history"));
    }
    if snapshot.definition.batches.len() > 1024
        || snapshot
            .definition
            .batches
            .iter()
            .any(checked_finite_callback(Vec::<TaskSpec>::is_empty))
    {
        return Err(invalid("invalid_batches"));
    }
    let specs: Vec<_> = snapshot.definition.batches.iter().flatten().collect();
    if specs.len() > 4096 {
        return Err(invalid("task_limit"));
    }
    let ids: BTreeSet<_> = specs
        .iter()
        .map(checked_spec_id_map(|spec| &spec.id))
        .collect();
    let effects: BTreeSet<_> = specs
        .iter()
        .map(checked_spec_effect_map(|spec| &spec.effect_id))
        .collect();
    if ids.len() != specs.len() || effects.len() != specs.len() {
        return Err(invalid("duplicate_task_or_effect"));
    }
    if snapshot.batch_index > snapshot.definition.batches.len()
        || snapshot.attempts_used > snapshot.definition.max_attempts
    {
        return Err(invalid("invalid_cursor_or_budget"));
    }
    if snapshot
        .failure
        .as_ref()
        .is_some_and(checked_finite_callback(|failure: &String| {
            failure.is_empty() || failure.len() > MAX_TASK_FAILURE_BYTES
        }))
    {
        return Err(invalid("invalid_failure_reason"));
    }
    if snapshot.status == ExecutionStatus::Accepted {
        if snapshot.batch_index != 0
            || !snapshot.tasks.is_empty()
            || snapshot.attempts_used != 0
            || snapshot.output.is_some()
            || snapshot.failure.is_some()
        {
            return Err(invalid("invalid_accepted_state"));
        }
        return Ok(());
    }
    // A revision-blocked accepted execution can have no initialized tasks.
    if snapshot.status == ExecutionStatus::RecoveryBlocked
        && snapshot.tasks.is_empty()
        && snapshot.batch_index == 0
    {
        if snapshot.failure.is_none() || snapshot.attempts_used != 0 || snapshot.output.is_some() {
            return Err(invalid("invalid_unstarted_blocked_state"));
        }
        return Ok(());
    }
    let expected: Vec<_> = snapshot
        .definition
        .batches
        .iter()
        .take(snapshot.batch_index.saturating_add(1))
        .flatten()
        .collect();
    if expected.len() != snapshot.tasks.len() {
        return Err(invalid("missing_or_extra_tasks"));
    }
    let mut attempts = BTreeSet::new();
    let mut interrupts = BTreeSet::new();
    let mut batch_input = None;
    for (record, spec) in snapshot.tasks.iter().zip(expected) {
        if &record.spec != spec {
            return Err(invalid("task_definition_changed"));
        }
        if let Some(attempt) = &record.attempt_id {
            if !attempts.insert(attempt) || !attempt_history.contains(attempt) {
                return Err(invalid("duplicate_attempt"));
            }
        }
        if let Some(interrupt) = &record.interrupt {
            if !interrupts.insert(&interrupt.id) || !record.spec.requires_approval {
                return Err(invalid("duplicate_interrupt"));
            }
        }
        if record.status == TaskStatus::Ready
            && record.attempt_id.is_some()
            && !matches!(
                record.spec.recovery,
                RecoveryPolicy::Pure | RecoveryPolicy::Idempotent
            )
        {
            return Err(invalid("unsafe_attempt_marked_ready"));
        }
        if record.status == TaskStatus::Denied
            && (!record.spec.requires_approval
                || record.interrupt.is_none()
                || record.approved
                || record.attempt_id.is_some())
        {
            return Err(invalid("invalid_denial_record"));
        }
        if record.status == TaskStatus::Succeeded
            && (record.result.is_none() || record.attempt_id.is_none())
        {
            return Err(invalid("successful_task_missing_result"));
        }
        if record.status != TaskStatus::Succeeded && record.result.is_some() {
            return Err(invalid("unexpected_result"));
        }
        if record.status == TaskStatus::Failed {
            if record.attempt_id.is_none()
                || record.failure.as_ref().is_none_or(checked_finite_callback(
                    |failure: &String| failure.is_empty() || failure.len() > MAX_TASK_FAILURE_BYTES,
                ))
            {
                return Err(invalid("invalid_failed_task"));
            }
        } else if record.failure.is_some() {
            return Err(invalid("unexpected_task_failure"));
        }
        if matches!(
            record.status,
            TaskStatus::InFlight | TaskStatus::OutcomeUnknown
        ) && record.attempt_id.is_none()
        {
            return Err(invalid("missing_attempt"));
        }
        if record.status == TaskStatus::WaitingForApproval
            && (record.interrupt.is_none()
                || !record.spec.requires_approval
                || record.approved
                || record.attempt_id.is_some())
        {
            return Err(invalid("invalid_pending_approval"));
        }
        if record.approved && (!record.spec.requires_approval || record.interrupt.is_none()) {
            return Err(invalid("invalid_approval_record"));
        }
        if record.attempt_id.is_some() && record.spec.requires_approval && !record.approved {
            return Err(invalid("effect_without_approval"));
        }
        if snapshot
            .definition
            .batches
            .get(snapshot.batch_index)
            .is_some_and(checked_finite_callback(|batch: &Vec<TaskSpec>| {
                batch.iter().any(checked_finite_callback(|task: &TaskSpec| {
                    task.id == record.spec.id
                }))
            }))
        {
            if batch_input.is_some_and(checked_finite_callback(
                |input: &serde_json::Map<String, serde_json::Value>| input != &record.input,
            )) {
                return Err(invalid("batch_inputs_disagree"));
            }
            batch_input = Some(&record.input);
        } else if !matches!(record.status, TaskStatus::Succeeded | TaskStatus::Denied) {
            return Err(invalid("unsettled_prior_batch"));
        }
    }
    if snapshot.attempts_used < attempts.len() as u32 {
        return Err(invalid("attempt_budget_underreported"));
    }
    if batch_input.is_some_and(checked_finite_callback(
        |input: &serde_json::Map<String, serde_json::Value>| input != &snapshot.values,
    )) {
        return Err(invalid("batch_input_not_committed_state"));
    }
    let inflight = snapshot
        .tasks
        .iter()
        .any(checked_finite_callback(|task: &TaskRecord| {
            task.status == TaskStatus::InFlight
        }));
    let waiting = snapshot
        .tasks
        .iter()
        .any(checked_finite_callback(|task: &TaskRecord| {
            task.status == TaskStatus::WaitingForApproval
        }));
    let ready = snapshot
        .tasks
        .iter()
        .any(checked_finite_callback(|task: &TaskRecord| {
            task.status == TaskStatus::Ready
        }));
    let unknown = snapshot
        .tasks
        .iter()
        .any(checked_finite_callback(|task: &TaskRecord| {
            task.status == TaskStatus::OutcomeUnknown
        }));
    let application_failure =
        snapshot
            .tasks
            .iter()
            .any(checked_finite_callback(|task: &TaskRecord| {
                task.status == TaskStatus::Failed
            }));
    if application_failure {
        if ready
            || waiting
            || !matches!(
                snapshot.status,
                ExecutionStatus::Running
                    | ExecutionStatus::Failed
                    | ExecutionStatus::RecoveryBlocked
            )
        {
            return Err(invalid("invalid_application_failure_state"));
        }
        if snapshot.status != ExecutionStatus::RecoveryBlocked
            && !snapshot
                .tasks
                .iter()
                .any(checked_finite_callback(|task: &TaskRecord| {
                    task.failure.is_some() && task.failure == snapshot.failure
                }))
        {
            return Err(invalid("application_failure_reason_mismatch"));
        }
        if snapshot.status == ExecutionStatus::Running && (!inflight || unknown) {
            return Err(invalid("invalid_failure_drain"));
        }
        if snapshot.status == ExecutionStatus::Failed && (inflight || unknown) {
            return Err(invalid("failed_effect_still_active"));
        }
    } else if snapshot.failure.is_some()
        && !matches!(
            snapshot.status,
            ExecutionStatus::Failed | ExecutionStatus::RecoveryBlocked
        )
    {
        return Err(invalid("unexpected_execution_failure"));
    }
    match snapshot.status {
        ExecutionStatus::Failed
            if !application_failure
                && (snapshot.batch_index == snapshot.definition.batches.len()
                    || snapshot.tasks.iter().any(checked_finite_callback(
                        |task: &TaskRecord| {
                            !matches!(task.status, TaskStatus::Succeeded | TaskStatus::Denied)
                        },
                    ))) =>
        {
            return Err(invalid("failed_join_not_settled"));
        }
        ExecutionStatus::WaitingForApproval if !waiting || ready || inflight || unknown => {
            return Err(invalid("unstable_approval_park"))
        }
        ExecutionStatus::Completed
            if snapshot.batch_index != snapshot.definition.batches.len()
                || snapshot.output.as_ref()
                    != Some(&serde_json::Value::Object(snapshot.values.clone())) =>
        {
            return Err(invalid("incomplete_terminal_state"))
        }
        ExecutionStatus::Failed | ExecutionStatus::RecoveryBlocked
            if snapshot.failure.is_none() =>
        {
            return Err(invalid("missing_failure_reason"))
        }
        ExecutionStatus::CancelRequested if !inflight || ready || waiting => {
            return Err(invalid("invalid_cancellation_drain"))
        }
        ExecutionStatus::Cancelled if inflight || ready || waiting || unknown => {
            return Err(invalid("cancelled_effect_still_active"))
        }
        ExecutionStatus::Running
            if snapshot.batch_index == snapshot.definition.batches.len()
                || unknown
                || (!ready && !inflight) =>
        {
            return Err(invalid("invalid_running_state"))
        }
        _ => {}
    }
    if snapshot.status == ExecutionStatus::Failed && !application_failure {
        let tasks: Vec<_> = snapshot.definition.batches[snapshot.batch_index]
            .iter()
            .map(checked_finite_callback(|spec: &TaskSpec| {
                snapshot
                    .tasks
                    .iter()
                    .find(checked_finite_callback(|task: &&TaskRecord| {
                        task.spec.id == spec.id
                    }))
                    .expect("validated batch")
            }))
            .collect();
        if reducer::reduce(&snapshot.values, &tasks).err() != snapshot.failure.as_deref() {
            return Err(invalid("failed_join_reason_mismatch"));
        }
    }
    if snapshot.status != ExecutionStatus::Completed && snapshot.output.is_some() {
        return Err(invalid("nonterminal_output"));
    }
    Ok(())
}
