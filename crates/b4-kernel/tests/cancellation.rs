use b4_contract::*;
use b4_kernel::{new_execution, transition, validate_snapshot, KernelError};
use serde_json::{json, Map};

fn task(id: &str) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        effect_id: EffectId::new(format!("effect-{id}")).unwrap(),
        recovery: RecoveryPolicy::Pure,
        requires_approval: false,
    }
}
fn initial(batches: Vec<Vec<TaskSpec>>) -> Snapshot {
    new_execution(
        ExecutionId::new("execution").unwrap(),
        TurnId::new("turn").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/cancel#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches,
            max_attempts: 8,
        },
        Map::from_iter([("saved".into(), json!(true))]),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap()
}
fn apply(snapshot: &Snapshot, command: KernelCommand) -> Snapshot {
    transition(
        snapshot,
        command,
        TransitionFacts {
            now_ms: snapshot.updated_at_ms + 1,
        },
    )
    .unwrap()
    .snapshot
}
fn cancel(snapshot: &Snapshot) -> KernelCommand {
    KernelCommand::Cancel {
        turn_id: snapshot.turn_id.clone(),
        owner_generation: snapshot.owner_generation,
    }
}
fn dispatch(snapshot: &Snapshot, id: &str) -> Snapshot {
    apply(
        snapshot,
        KernelCommand::Dispatch {
            task_id: TaskId::new(id).unwrap(),
            attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
        },
    )
}
fn acknowledge(id: &str) -> KernelCommand {
    KernelCommand::CancelAttempt {
        task_id: TaskId::new(id).unwrap(),
        attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
    }
}

#[test]
fn accepted_cancellation_initializes_only_first_batch_without_attempts_or_output() {
    for batches in [
        vec![],
        vec![vec![task("a")]],
        vec![vec![task("a"), task("b")], vec![task("later")]],
    ] {
        let before = initial(batches);
        let cancelled = apply(&before, cancel(&before));
        assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
        assert_eq!(cancelled.batch_index, 0);
        assert_eq!(
            cancelled.tasks.len(),
            before.definition.batches.first().map_or(0, Vec::len)
        );
        assert!(cancelled
            .tasks
            .iter()
            .all(|task| task.status == TaskStatus::Cancelled
                && task.attempt_id.is_none()
                && task.result.is_none()));
        assert_eq!(cancelled.attempts_used, 0);
        assert_eq!(cancelled.attempt_history, before.attempt_history);
        assert_eq!(cancelled.turn_history, before.turn_history);
        assert_eq!(cancelled.values, before.values);
        assert!(cancelled.output.is_none());
        assert!(cancelled.failure.is_none());
        validate_snapshot(&cancelled).unwrap();
    }
}

#[test]
fn never_entered_attempt_acknowledgement_preserves_attempt_budget_and_history() {
    let running = apply(
        &initial(vec![vec![task("a")], vec![task("later")]]),
        KernelCommand::Start,
    );
    let inflight = dispatch(&running, "a");
    let cancelling = apply(&inflight, cancel(&inflight));
    assert_eq!(cancelling.status, ExecutionStatus::CancelRequested);
    let cancelled = apply(&cancelling, acknowledge("a"));
    assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
    assert_eq!(cancelled.tasks[0].status, TaskStatus::Cancelled);
    assert_eq!(
        cancelled.tasks[0].attempt_id,
        cancelling.tasks[0].attempt_id
    );
    assert!(cancelled.tasks[0].result.is_none());
    assert!(cancelled.tasks[0].failure.is_none());
    assert_eq!(cancelled.attempts_used, 1);
    assert_eq!(cancelled.attempt_history, inflight.attempt_history);
    assert_eq!(cancelled.turn_history, inflight.turn_history);
    assert_eq!(cancelled.values, inflight.values);
    assert_eq!(cancelled.tasks.len(), 1);
    let duplicate = transition(
        &cancelled,
        acknowledge("a"),
        TransitionFacts {
            now_ms: cancelled.updated_at_ms + 1,
        },
    )
    .unwrap();
    assert_eq!(duplicate.snapshot, cancelled);
    assert!(duplicate.dispatchable.is_empty());
}

#[test]
fn acknowledgements_require_exact_attempt_and_committed_cancellation_or_failure_drain() {
    let running = apply(
        &initial(vec![vec![task("a"), task("b")]]),
        KernelCommand::Start,
    );
    let inflight = dispatch(&running, "a");
    assert!(matches!(
        transition(&inflight, acknowledge("a"), TransitionFacts { now_ms: 10 }),
        Err(KernelError::InvalidTransition(_))
    ));
    let cancelling = apply(&inflight, cancel(&inflight));
    assert_eq!(
        transition(
            &cancelling,
            acknowledge("b"),
            TransitionFacts { now_ms: 10 }
        ),
        Err(KernelError::AttemptMismatch)
    );
    assert_eq!(
        transition(
            &cancelling,
            KernelCommand::CancelAttempt {
                task_id: TaskId::new("a").unwrap(),
                attempt_id: AttemptId::new("different").unwrap()
            },
            TransitionFacts { now_ms: 10 }
        ),
        Err(KernelError::AttemptMismatch)
    );
    let succeeded = apply(
        &inflight,
        KernelCommand::Complete {
            task_id: TaskId::new("a").unwrap(),
            attempt_id: AttemptId::new("attempt-a").unwrap(),
            result: TaskResult {
                output: json!(true),
                writes: vec![],
            },
        },
    );
    assert_eq!(
        transition(&succeeded, acknowledge("a"), TransitionFacts { now_ms: 10 }),
        Err(KernelError::ResultConflict)
    );
}

#[test]
fn late_failure_wins_while_never_entered_sibling_acknowledgement_finishes_failure_drain() {
    let running = apply(
        &initial(vec![vec![task("a"), task("b")]]),
        KernelCommand::Start,
    );
    let inflight = dispatch(&dispatch(&running, "a"), "b");
    let cancelling = apply(&inflight, cancel(&inflight));
    let failure = apply(
        &cancelling,
        KernelCommand::FailAttempt {
            task_id: TaskId::new("a").unwrap(),
            attempt_id: AttemptId::new("attempt-a").unwrap(),
            failure: "application failure".into(),
        },
    );
    assert_eq!(failure.status, ExecutionStatus::Running);
    let failed = apply(&failure, acknowledge("b"));
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(failed.failure.as_deref(), Some("application failure"));
    assert_eq!(failed.tasks[0].status, TaskStatus::Failed);
    assert_eq!(failed.tasks[1].status, TaskStatus::Cancelled);
    assert_eq!(failed.attempt_history, inflight.attempt_history);
    assert_eq!(failed.attempts_used, 2);
    assert_eq!(failed.values, inflight.values);
    assert_eq!(
        transition(&failed, acknowledge("b"), TransitionFacts { now_ms: 20 })
            .unwrap()
            .snapshot,
        failed
    );
    assert_eq!(
        transition(&failed, acknowledge("a"), TransitionFacts { now_ms: 20 }),
        Err(KernelError::ResultConflict)
    );
}
