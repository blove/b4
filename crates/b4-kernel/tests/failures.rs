use b4_contract::*;
use b4_kernel::{new_execution, transition, validate_snapshot, KernelError};
use serde_json::{json, Map};

fn spec(id: &str, approval: bool, recovery: RecoveryPolicy) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        effect_id: EffectId::new(format!("effect-{id}")).unwrap(),
        recovery,
        requires_approval: approval,
    }
}

fn start(batches: Vec<Vec<TaskSpec>>) -> Snapshot {
    let accepted = new_execution(
        ExecutionId::new("execution-failure").unwrap(),
        TurnId::new("turn-first").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/failure#workflow").unwrap(),
            revision: ExecutionRevision::new("revision-first").unwrap(),
            batches,
            max_attempts: 16,
        },
        Map::new(),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    apply(&accepted, KernelCommand::Start)
}

fn apply(snapshot: &Snapshot, command: KernelCommand) -> Snapshot {
    transition(snapshot, command, TransitionFacts { now_ms: 100 })
        .unwrap()
        .snapshot
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

fn failure(id: &str, reason: &str) -> KernelCommand {
    KernelCommand::FailAttempt {
        task_id: TaskId::new(id).unwrap(),
        attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
        failure: reason.into(),
    }
}

fn success(id: &str) -> KernelCommand {
    KernelCommand::Complete {
        task_id: TaskId::new(id).unwrap(),
        attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
        result: TaskResult {
            output: json!(false),
            writes: vec![StateWrite::Replace {
                field: id.into(),
                value: json!(true),
            }],
        },
    }
}

fn cancel(snapshot: &Snapshot) -> KernelCommand {
    KernelCommand::Cancel {
        turn_id: snapshot.turn_id.clone(),
        owner_generation: snapshot.owner_generation,
    }
}

fn recover(snapshot: &Snapshot, revision: ExecutionRevision) -> KernelCommand {
    KernelCommand::Recover {
        available_revision: revision,
        turn_id: TurnId::new("turn-recovered").unwrap(),
        owner_generation: snapshot.owner_generation + 1,
    }
}

fn draining(policy: RecoveryPolicy) -> Snapshot {
    let running = dispatch(
        &dispatch(
            &start(vec![vec![
                spec("failed", false, RecoveryPolicy::Pure),
                spec("sibling", false, policy),
            ]]),
            "failed",
        ),
        "sibling",
    );
    apply(&running, failure("failed", "known failure"))
}

#[test]
fn known_failure_is_terminal_without_success_output_and_redelivery_is_idempotent() {
    let running = dispatch(
        &start(vec![vec![spec("failed", false, RecoveryPolicy::Pure)]]),
        "failed",
    );
    let failed = apply(&running, failure("failed", "known failure"));
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(failed.failure.as_deref(), Some("known failure"));
    assert_eq!(failed.tasks[0].status, TaskStatus::Failed);
    assert_eq!(failed.tasks[0].failure.as_deref(), Some("known failure"));
    assert!(failed.tasks[0].result.is_none());
    assert!(failed.output.is_none());
    let repeated = transition(
        &failed,
        failure("failed", "known failure"),
        TransitionFacts { now_ms: 101 },
    )
    .unwrap();
    assert_eq!(repeated.snapshot, failed);
    assert!(repeated.dispatchable.is_empty());
    assert_eq!(apply(&failed, failure("failed", "known failure")), failed);
    assert_eq!(
        transition(
            &failed,
            failure("failed", "changed failure"),
            TransitionFacts { now_ms: 100 }
        ),
        Err(KernelError::ResultConflict)
    );
    assert!(transition(&failed, success("failed"), TransitionFacts { now_ms: 100 }).is_err());
}

#[test]
fn failure_accepts_only_matching_inflight_attempt_and_bounded_nonempty_utf8_reason() {
    let ready = start(vec![vec![spec("failed", false, RecoveryPolicy::Pure)]]);
    assert!(transition(
        &ready,
        failure("failed", "known failure"),
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
    let running = dispatch(&ready, "failed");
    let mut wrong = failure("failed", "known failure");
    if let KernelCommand::FailAttempt { attempt_id, .. } = &mut wrong {
        *attempt_id = AttemptId::new("other-attempt").unwrap();
    }
    assert_eq!(
        transition(&running, wrong, TransitionFacts { now_ms: 100 }),
        Err(KernelError::AttemptMismatch)
    );
    for reason in [
        String::new(),
        "x".repeat(MAX_TASK_FAILURE_BYTES + 1),
        "é".repeat(MAX_TASK_FAILURE_BYTES / 2 + 1),
    ] {
        assert!(transition(
            &running,
            failure("failed", &reason),
            TransitionFacts { now_ms: 100 }
        )
        .is_err());
    }
    let bounded = "é".repeat(MAX_TASK_FAILURE_BYTES / 2);
    assert_eq!(
        apply(&running, failure("failed", &bounded))
            .failure
            .as_deref(),
        Some(bounded.as_str())
    );
    let succeeded = apply(&running, success("failed"));
    assert!(transition(
        &succeeded,
        failure("failed", "known failure"),
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
}

#[test]
fn failure_cancels_unsent_siblings_retains_results_and_never_reduces_or_starts_later_batch() {
    let running = start(vec![
        vec![
            spec("succeeded", false, RecoveryPolicy::Pure),
            spec("failed", false, RecoveryPolicy::Pure),
            spec("inflight", false, RecoveryPolicy::Pure),
            spec("ready", false, RecoveryPolicy::Pure),
            spec("waiting", true, RecoveryPolicy::Pure),
        ],
        vec![spec("later", false, RecoveryPolicy::Pure)],
    ]);
    let running = apply(&dispatch(&running, "succeeded"), success("succeeded"));
    let running = dispatch(&dispatch(&running, "failed"), "inflight");
    let running = apply(
        &running,
        KernelCommand::Park {
            task_id: TaskId::new("waiting").unwrap(),
            interrupt_id: InterruptId::new("approval-waiting").unwrap(),
            prompt: json!("Proceed?"),
        },
    );
    let plan = transition(
        &running,
        failure("failed", "known failure"),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    assert!(plan.dispatchable.is_empty());
    let draining = plan.snapshot;
    assert_eq!(draining.status, ExecutionStatus::Running);
    assert_eq!(draining.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(draining.tasks[3].status, TaskStatus::Cancelled);
    assert_eq!(draining.tasks[4].status, TaskStatus::Cancelled);
    assert!(transition(
        &draining,
        KernelCommand::Dispatch {
            task_id: TaskId::new("ready").unwrap(),
            attempt_id: AttemptId::new("attempt-ready").unwrap()
        },
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
    assert!(transition(
        &draining,
        KernelCommand::Park {
            task_id: TaskId::new("waiting").unwrap(),
            interrupt_id: InterruptId::new("approval-other").unwrap(),
            prompt: json!(null)
        },
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
    let failed = apply(&draining, success("inflight"));
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(
        failed.tasks[2].result.as_ref().unwrap().output,
        json!(false)
    );
    assert!(failed.values.is_empty());
    assert_eq!(failed.batch_index, 0);
    assert_eq!(failed.tasks.len(), 5);
    assert!(failed.output.is_none());
}

#[test]
fn second_failure_keeps_first_committed_reason() {
    let draining = draining(RecoveryPolicy::Pure);
    let duplicate = apply(&draining, failure("failed", "known failure"));
    assert_eq!(duplicate, draining);
    let failed = apply(&draining, failure("sibling", "other failure"));
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(failed.failure.as_deref(), Some("known failure"));
    assert_eq!(failed.tasks[1].failure.as_deref(), Some("other failure"));

    let running = dispatch(
        &dispatch(
            &start(vec![vec![
                spec("failed", false, RecoveryPolicy::Pure),
                spec("sibling", false, RecoveryPolicy::Pure),
            ]]),
            "failed",
        ),
        "sibling",
    );
    let other_first = apply(&running, failure("sibling", "other failure"));
    assert_eq!(
        apply(&other_first, failure("failed", "known failure"))
            .failure
            .as_deref(),
        Some("other failure")
    );
}

#[test]
fn known_failure_preserves_prior_batch_state_and_denied_siblings() {
    let running = start(vec![
        vec![spec("prior", false, RecoveryPolicy::Pure)],
        vec![
            spec("failed", true, RecoveryPolicy::Pure),
            spec("denied", true, RecoveryPolicy::Pure),
        ],
    ]);
    let mut running = apply(&dispatch(&running, "prior"), success("prior"));
    for id in ["failed", "denied"] {
        running = apply(
            &running,
            KernelCommand::Park {
                task_id: TaskId::new(id).unwrap(),
                interrupt_id: InterruptId::new(format!("approval-{id}")).unwrap(),
                prompt: json!("Proceed?"),
            },
        );
    }
    let running = apply(
        &running,
        KernelCommand::Resume {
            route: running.definition.route.clone(),
            decisions: vec![
                Decision {
                    interrupt_id: InterruptId::new("approval-failed").unwrap(),
                    decision: ApprovalDecision::Once,
                },
                Decision {
                    interrupt_id: InterruptId::new("approval-denied").unwrap(),
                    decision: ApprovalDecision::Deny,
                },
            ],
            turn_id: TurnId::new("turn-approved").unwrap(),
        },
    );
    let failed = apply(
        &dispatch(&running, "failed"),
        failure("failed", "known failure"),
    );
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(failed.batch_index, 1);
    assert_eq!(failed.values.get("prior"), Some(&json!(true)));
    assert_eq!(failed.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(failed.tasks[2].status, TaskStatus::Denied);
    assert_eq!(failed.attempts_used, 2);
}

#[test]
fn application_failure_wins_cooperative_cancellation_and_rejects_cancel_during_drain() {
    let running = dispatch(
        &dispatch(
            &start(vec![vec![
                spec("failed", false, RecoveryPolicy::Pure),
                spec("sibling", false, RecoveryPolicy::Pure),
            ]]),
            "failed",
        ),
        "sibling",
    );
    let cancelling = apply(&running, cancel(&running));
    let draining = apply(&cancelling, failure("failed", "known failure"));
    assert_eq!(draining.status, ExecutionStatus::Running);
    assert!(transition(
        &draining,
        cancel(&draining),
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
    assert_eq!(
        apply(&draining, success("sibling")).status,
        ExecutionStatus::Failed
    );
}

#[test]
fn safe_recovery_of_failure_drain_cancels_uncertain_sibling_without_replay() {
    for policy in [RecoveryPolicy::Pure, RecoveryPolicy::Idempotent] {
        let draining = draining(policy);
        let plan = transition(
            &draining,
            recover(&draining, draining.definition.revision.clone()),
            TransitionFacts { now_ms: 100 },
        )
        .unwrap();
        assert_eq!(plan.snapshot.status, ExecutionStatus::Failed);
        assert_eq!(plan.snapshot.tasks[1].status, TaskStatus::Cancelled);
        assert_eq!(
            plan.snapshot.tasks[0].failure.as_deref(),
            Some("known failure")
        );
        assert_eq!(plan.snapshot.failure.as_deref(), Some("known failure"));
        assert!(plan.dispatchable.is_empty());
        assert!(transition(
            &plan.snapshot,
            success("sibling"),
            TransitionFacts { now_ms: 100 }
        )
        .is_err());
    }
}

#[test]
fn unsafe_failure_drain_recovery_blocks_and_preserves_per_task_application_cause() {
    for policy in [RecoveryPolicy::Unsafe, RecoveryPolicy::Reconcilable] {
        let draining = draining(policy);
        let blocked = apply(
            &draining,
            recover(&draining, draining.definition.revision.clone()),
        );
        assert_eq!(blocked.status, ExecutionStatus::RecoveryBlocked);
        assert_eq!(blocked.tasks[1].status, TaskStatus::OutcomeUnknown);
        assert_eq!(blocked.failure.as_deref(), Some("effect_outcome_unknown"));
        assert_eq!(blocked.tasks[0].failure.as_deref(), Some("known failure"));
        validate_snapshot(&blocked).unwrap();
    }
    let draining = draining(RecoveryPolicy::Pure);
    let blocked = apply(
        &draining,
        recover(&draining, ExecutionRevision::new("unavailable").unwrap()),
    );
    assert_eq!(blocked.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(blocked.tasks[0].failure.as_deref(), Some("known failure"));
    assert_eq!(
        blocked.failure.as_deref(),
        Some("execution_revision_unavailable")
    );
}

#[test]
fn restored_failure_snapshots_reject_invalid_task_records_and_scheduler_states() {
    let draining = draining(RecoveryPolicy::Pure);
    let failed = apply(&draining, success("sibling"));
    let mut corrupted = Vec::new();
    let mut value = draining.clone();
    value.tasks[0].attempt_id = None;
    corrupted.push(value);
    let mut value = draining.clone();
    value.tasks[0].failure = None;
    corrupted.push(value);
    let mut value = draining.clone();
    value.tasks[0].failure = Some(String::new());
    corrupted.push(value);
    let mut value = draining.clone();
    value.tasks[0].failure = Some("x".repeat(MAX_TASK_FAILURE_BYTES + 1));
    corrupted.push(value);
    let mut value = draining.clone();
    value.tasks[0].result = Some(TaskResult {
        output: json!(null),
        writes: vec![],
    });
    corrupted.push(value);
    let mut value = draining.clone();
    value.tasks[1].failure = Some("invented failure".into());
    corrupted.push(value);
    let mut value = draining.clone();
    value.tasks[1].status = TaskStatus::Ready;
    corrupted.push(value);
    let mut value = draining.clone();
    value.tasks[1].status = TaskStatus::Cancelled;
    corrupted.push(value);
    let mut value = draining.clone();
    value.status = ExecutionStatus::CancelRequested;
    corrupted.push(value);
    let mut value = draining.clone();
    value.status = ExecutionStatus::Failed;
    corrupted.push(value);
    let mut value = failed.clone();
    value.failure = None;
    corrupted.push(value);
    let mut value = failed;
    value.failure = Some("different reason".into());
    corrupted.push(value);
    let mut value = start(vec![vec![spec("ready", false, RecoveryPolicy::Pure)]]);
    value.failure = Some("invented drain".into());
    corrupted.push(value);
    for (index, snapshot) in corrupted.iter().enumerate() {
        assert!(
            validate_snapshot(snapshot).is_err(),
            "accepted corrupted failure snapshot {index}"
        );
    }
    let restored: Snapshot =
        serde_json::from_slice(&serde_json::to_vec(&draining).unwrap()).unwrap();
    assert_eq!(restored, draining);
    validate_snapshot(&restored).unwrap();
}
