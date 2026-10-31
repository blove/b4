use b4_contract::*;
use b4_kernel::{new_execution, transition, KernelError};
use serde_json::{json, Map, Value};

fn spec(id: &str, approval: bool, recovery: RecoveryPolicy) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        effect_id: EffectId::new(format!("effect-{id}")).unwrap(),
        recovery,
        requires_approval: approval,
    }
}

fn snapshot(batches: Vec<Vec<TaskSpec>>) -> Snapshot {
    new_execution(
        ExecutionId::new("execution-1").unwrap(),
        TurnId::new("turn-1").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/parallel-approval#agent").unwrap(),
            revision: ExecutionRevision::new("revision-1").unwrap(),
            batches,
            max_attempts: 8,
        },
        Map::new(),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap()
}

fn apply(s: &Snapshot, command: KernelCommand) -> Snapshot {
    let plan = transition(s, command, TransitionFacts { now_ms: 100 }).unwrap();
    assert_eq!(plan.expected_revision, s.revision);
    plan.snapshot
}

fn start(batches: Vec<Vec<TaskSpec>>) -> Snapshot {
    apply(&snapshot(batches), KernelCommand::Start)
}

fn dispatch(s: &Snapshot, id: &str) -> Snapshot {
    apply(
        s,
        KernelCommand::Dispatch {
            task_id: TaskId::new(id).unwrap(),
            attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
        },
    )
}

fn complete_command(id: &str, writes: Vec<StateWrite>) -> KernelCommand {
    KernelCommand::Complete {
        task_id: TaskId::new(id).unwrap(),
        attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
        result: TaskResult {
            output: json!({"task": id}),
            writes,
        },
    }
}

fn complete(s: &Snapshot, id: &str, writes: Vec<StateWrite>) -> Snapshot {
    apply(s, complete_command(id, writes))
}

fn park(s: &Snapshot, id: &str) -> Snapshot {
    apply(
        s,
        KernelCommand::Park {
            task_id: TaskId::new(id).unwrap(),
            interrupt_id: InterruptId::new(format!("permission-{id}")).unwrap(),
            prompt: json!({"tool": id}),
        },
    )
}

fn resume(s: &Snapshot, entries: &[(&str, ApprovalDecision)]) -> KernelCommand {
    KernelCommand::Resume {
        route: s.definition.route.clone(),
        decisions: entries
            .iter()
            .map(|(id, decision)| Decision {
                interrupt_id: InterruptId::new(format!("permission-{id}")).unwrap(),
                decision: *decision,
            })
            .collect(),
        turn_id: TurnId::new("turn-2").unwrap(),
    }
}

#[test]
fn sequential_batches_share_committed_input() {
    let s = start(vec![
        vec![spec("a", false, RecoveryPolicy::Pure)],
        vec![spec("b", false, RecoveryPolicy::Pure)],
    ]);
    let s = complete(
        &dispatch(&s, "a"),
        "a",
        vec![StateWrite::Replace {
            field: "count".into(),
            value: json!(1),
        }],
    );
    assert_eq!(s.status, ExecutionStatus::Running);
    assert_eq!(s.batch_index, 1);
    assert_eq!(s.tasks[1].input.get("count"), Some(&json!(1)));
    let s = complete(&dispatch(&s, "b"), "b", vec![]);
    assert_eq!(s.status, ExecutionStatus::Completed);
    assert_eq!(s.output, Some(json!({"count":1})));
    assert_eq!(s.tasks[0].status, TaskStatus::Succeeded);
}

#[test]
fn parallel_appends_reduce_in_definition_order() {
    for completion_order in [["a", "b"], ["b", "a"]] {
        let s = start(vec![vec![
            spec("a", false, RecoveryPolicy::Pure),
            spec("b", false, RecoveryPolicy::Pure),
        ]]);
        let mut s = dispatch(&dispatch(&s, "a"), "b");
        assert_eq!(s.tasks[0].input, s.tasks[1].input);
        for id in completion_order {
            s = complete(
                &s,
                id,
                vec![StateWrite::Append {
                    field: "items".into(),
                    items: vec![json!(id)],
                }],
            );
        }
        assert_eq!(s.values.get("items"), Some(&json!(["a", "b"])));
    }
}

#[test]
fn conflicting_parallel_replacements_fail() {
    let s = start(vec![vec![
        spec("a", false, RecoveryPolicy::Pure),
        spec("b", false, RecoveryPolicy::Pure),
    ]]);
    let s = dispatch(&dispatch(&s, "a"), "b");
    let s = complete(
        &s,
        "b",
        vec![StateWrite::Replace {
            field: "x".into(),
            value: json!(2),
        }],
    );
    let s = complete(
        &s,
        "a",
        vec![StateWrite::Replace {
            field: "x".into(),
            value: json!(1),
        }],
    );
    assert_eq!(s.status, ExecutionStatus::Failed);
    assert!(s.values.is_empty());
    assert_eq!(s.failure.as_deref(), Some("conflicting_state_writes"));
}

#[test]
fn invalid_append_fails_without_partial_reduction() {
    let mut s = snapshot(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]);
    s.values.insert("items".into(), json!(42));
    let s = apply(&s, KernelCommand::Start);
    let s = complete(
        &dispatch(&s, "a"),
        "a",
        vec![
            StateWrite::Replace {
                field: "uncommitted".into(),
                value: json!(true),
            },
            StateWrite::Append {
                field: "items".into(),
                items: vec![json!(1)],
            },
        ],
    );
    assert_eq!(s.status, ExecutionStatus::Failed);
    assert_eq!(s.values.get("items"), Some(&json!(42)));
    assert!(!s.values.contains_key("uncommitted"));
}

#[test]
fn approval_parks_only_after_siblings_settle() {
    let s = start(vec![vec![
        spec("a", false, RecoveryPolicy::Pure),
        spec("b", true, RecoveryPolicy::Unsafe),
    ]]);
    let s = park(&dispatch(&s, "a"), "b");
    assert_eq!(s.status, ExecutionStatus::Running);
    assert!(transition(
        &s,
        resume(&s, &[("b", ApprovalDecision::Once)]),
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
    let s = complete(&s, "a", vec![]);
    assert_eq!(s.status, ExecutionStatus::WaitingForApproval);
}

#[test]
fn resume_is_full_set_atomic_and_preserves_sibling() {
    let s = start(vec![vec![
        spec("a", false, RecoveryPolicy::Pure),
        spec("b", true, RecoveryPolicy::Unsafe),
        spec("c", true, RecoveryPolicy::Unsafe),
    ]]);
    let s = complete(
        &dispatch(&s, "a"),
        "a",
        vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!("a")],
        }],
    );
    let s = park(&park(&s, "b"), "c");
    let restored: Snapshot = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
    for entries in [
        vec![("b", ApprovalDecision::Once)],
        vec![("b", ApprovalDecision::Once), ("b", ApprovalDecision::Once)],
        vec![
            ("b", ApprovalDecision::Once),
            ("c", ApprovalDecision::Once),
            ("d", ApprovalDecision::Once),
        ],
    ] {
        assert_eq!(
            transition(
                &restored,
                resume(&restored, &entries),
                TransitionFacts { now_ms: 100 }
            ),
            Err(KernelError::InterruptSetMismatch)
        );
        assert_eq!(restored, s);
    }
    let plan = transition(
        &restored,
        resume(
            &restored,
            &[("b", ApprovalDecision::Once), ("c", ApprovalDecision::Deny)],
        ),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    assert_eq!(plan.dispatchable, vec![TaskId::new("b").unwrap()]);
    let s = plan.snapshot;
    assert_eq!(s.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(s.tasks[2].status, TaskStatus::Denied);
    let s = complete(
        &dispatch(&s, "b"),
        "b",
        vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!("b")],
        }],
    );
    assert_eq!(s.values.get("items"), Some(&json!(["a", "b"])));
    assert_eq!(s.status, ExecutionStatus::Completed);
}

#[test]
fn denial_never_dispatches_tool() {
    let s = park(
        &start(vec![vec![spec("a", true, RecoveryPolicy::Unsafe)]]),
        "a",
    );
    let plan = transition(
        &s,
        resume(&s, &[("a", ApprovalDecision::Deny)]),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    assert!(plan.dispatchable.is_empty());
    assert_eq!(plan.snapshot.attempts_used, 0);
    assert_eq!(plan.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(plan.snapshot.tasks[0].status, TaskStatus::Denied);
}

#[test]
fn duplicate_result_is_idempotent() {
    let s = complete(
        &dispatch(
            &start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]),
            "a",
        ),
        "a",
        vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!(1)],
        }],
    );
    let duplicate = transition(
        &s,
        complete_command(
            "a",
            vec![StateWrite::Append {
                field: "items".into(),
                items: vec![json!(1)],
            }],
        ),
        TransitionFacts { now_ms: 101 },
    )
    .unwrap();
    assert_eq!(duplicate.snapshot, s);
    assert!(duplicate.dispatchable.is_empty());
}

#[test]
fn conflicting_redelivery_fails() {
    let s = complete(
        &dispatch(
            &start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]),
            "a",
        ),
        "a",
        vec![],
    );
    assert_eq!(
        transition(
            &s,
            complete_command(
                "a",
                vec![StateWrite::Replace {
                    field: "x".into(),
                    value: Value::Null
                }]
            ),
            TransitionFacts { now_ms: 100 }
        ),
        Err(KernelError::ResultConflict)
    );
}

#[test]
fn cancel_retains_inflight_slot_and_recorded_results() {
    let s = start(vec![vec![
        spec("a", false, RecoveryPolicy::Pure),
        spec("b", false, RecoveryPolicy::Unsafe),
    ]]);
    let s = dispatch(&dispatch(&s, "a"), "b");
    let s = complete(&s, "a", vec![]);
    let s = apply(
        &s,
        KernelCommand::Cancel {
            turn_id: s.turn_id.clone(),
            owner_generation: s.owner_generation,
        },
    );
    assert_eq!(s.status, ExecutionStatus::CancelRequested);
    assert_eq!(s.tasks[0].status, TaskStatus::Succeeded);
    let s = complete(
        &s,
        "b",
        vec![StateWrite::Replace {
            field: "late".into(),
            value: json!(true),
        }],
    );
    assert_eq!(s.status, ExecutionStatus::Cancelled);
    assert_eq!(s.tasks[1].status, TaskStatus::Succeeded);
    assert!(!s.values.contains_key("late"));
}

#[test]
fn stale_cancel_is_rejected() {
    let s = park(
        &start(vec![vec![spec("a", true, RecoveryPolicy::Pure)]]),
        "a",
    );
    let s = apply(&s, resume(&s, &[("a", ApprovalDecision::Once)]));
    assert_eq!(
        transition(
            &s,
            KernelCommand::Cancel {
                turn_id: TurnId::new("turn-1").unwrap(),
                owner_generation: 1
            },
            TransitionFacts { now_ms: 100 }
        ),
        Err(KernelError::StaleTurn)
    );
}

#[test]
fn safe_recovery_preserves_effect_id_and_budget() {
    for policy in [RecoveryPolicy::Pure, RecoveryPolicy::Idempotent] {
        let s = dispatch(&start(vec![vec![spec("a", false, policy)]]), "a");
        let s = apply(
            &s,
            KernelCommand::Recover {
                available_revision: s.definition.revision.clone(),
                turn_id: TurnId::new("recovered-turn").unwrap(),
                owner_generation: 2,
            },
        );
        assert_eq!(s.status, ExecutionStatus::Running);
        assert_eq!(
            s.tasks[0].spec.effect_id,
            EffectId::new("effect-a").unwrap()
        );
        assert_eq!(s.tasks[0].status, TaskStatus::Ready);
        assert_eq!(s.attempts_used, 1);
        assert!(transition(
            &s,
            KernelCommand::Dispatch {
                task_id: TaskId::new("a").unwrap(),
                attempt_id: AttemptId::new("attempt-a").unwrap()
            },
            TransitionFacts { now_ms: 100 }
        )
        .is_err());
        let s = apply(
            &s,
            KernelCommand::Dispatch {
                task_id: TaskId::new("a").unwrap(),
                attempt_id: AttemptId::new("attempt-recovered").unwrap(),
            },
        );
        assert_eq!(s.attempts_used, 2);
    }
}

#[test]
fn unsafe_recovery_blocks() {
    for policy in [RecoveryPolicy::Unsafe, RecoveryPolicy::Reconcilable] {
        let s = dispatch(&start(vec![vec![spec("a", false, policy)]]), "a");
        let plan = transition(
            &s,
            KernelCommand::Recover {
                available_revision: s.definition.revision.clone(),
                turn_id: TurnId::new("recovered-turn").unwrap(),
                owner_generation: 2,
            },
            TransitionFacts { now_ms: 100 },
        )
        .unwrap();
        assert_eq!(plan.snapshot.status, ExecutionStatus::RecoveryBlocked);
        assert_eq!(plan.snapshot.tasks[0].status, TaskStatus::OutcomeUnknown);
        assert!(plan.dispatchable.is_empty());
    }
}

#[test]
fn unavailable_revision_blocks() {
    let s = start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]);
    let s = apply(
        &s,
        KernelCommand::Recover {
            available_revision: ExecutionRevision::new("revision-2").unwrap(),
            turn_id: TurnId::new("recovered-turn").unwrap(),
            owner_generation: 2,
        },
    );
    assert_eq!(s.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(s.failure.as_deref(), Some("execution_revision_unavailable"));
}

#[test]
fn malformed_snapshot_fails_closed() {
    let mut s = start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]);
    s.tasks[0].status = TaskStatus::Succeeded;
    assert!(matches!(
        transition(&s, KernelCommand::Start, TransitionFacts { now_ms: 100 }),
        Err(KernelError::InvalidSnapshot(_))
    ));
    s.schema_version = 99;
    assert!(transition(&s, KernelCommand::Start, TransitionFacts { now_ms: 100 }).is_err());
}

#[test]
fn budget_is_not_reset_by_resume() {
    let mut s = snapshot(vec![vec![
        spec("a", false, RecoveryPolicy::Pure),
        spec("b", true, RecoveryPolicy::Unsafe),
    ]]);
    s.definition.max_attempts = 1;
    let s = apply(&s, KernelCommand::Start);
    let s = park(&complete(&dispatch(&s, "a"), "a", vec![]), "b");
    let s = apply(&s, resume(&s, &[("b", ApprovalDecision::Once)]));
    assert_eq!(s.attempts_used, 1);
    assert_eq!(
        transition(
            &s,
            KernelCommand::Dispatch {
                task_id: TaskId::new("b").unwrap(),
                attempt_id: AttemptId::new("attempt-b").unwrap()
            },
            TransitionFacts { now_ms: 100 }
        ),
        Err(KernelError::BudgetExceeded)
    );
}

#[test]
fn proposed_plan_does_not_mutate_its_input() {
    let s = snapshot(vec![]);
    let before = s.clone();
    let plan = transition(&s, KernelCommand::Start, TransitionFacts { now_ms: 101 }).unwrap();
    assert_eq!(s, before);
    assert_eq!(plan.snapshot.revision, 1);
    assert_eq!(plan.snapshot.updated_at_ms, 101);
    assert_eq!(plan.snapshot.status, ExecutionStatus::Completed);
}

#[test]
fn unapproved_task_cannot_dispatch() {
    let s = start(vec![vec![spec("a", true, RecoveryPolicy::Unsafe)]]);
    assert!(transition(
        &s,
        KernelCommand::Dispatch {
            task_id: TaskId::new("a").unwrap(),
            attempt_id: AttemptId::new("a1").unwrap()
        },
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
}

#[test]
fn resume_cannot_redirect_the_execution() {
    let s = park(
        &start(vec![vec![spec("a", true, RecoveryPolicy::Pure)]]),
        "a",
    );
    let mut command = resume(&s, &[("a", ApprovalDecision::Once)]);
    if let KernelCommand::Resume { route, .. } = &mut command {
        *route = RouteKey::new("/other#agent").unwrap();
    }
    assert_eq!(
        transition(&s, command, TransitionFacts { now_ms: 100 }),
        Err(KernelError::RouteMismatch)
    );
}

#[test]
fn cancellation_remains_cancelled_on_recovery() {
    let s = dispatch(
        &start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]),
        "a",
    );
    let s = apply(
        &s,
        KernelCommand::Cancel {
            turn_id: s.turn_id.clone(),
            owner_generation: 1,
        },
    );
    let plan = transition(
        &s,
        KernelCommand::Recover {
            available_revision: s.definition.revision.clone(),
            turn_id: TurnId::new("recovered-turn").unwrap(),
            owner_generation: 2,
        },
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    assert_eq!(plan.snapshot.status, ExecutionStatus::Cancelled);
    assert!(plan.dispatchable.is_empty());
    assert!(transition(
        &plan.snapshot,
        complete_command("a", vec![]),
        TransitionFacts { now_ms: 100 }
    )
    .is_err());
}

#[test]
fn inconsistent_batch_input_fails_closed() {
    let mut s = start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]);
    s.tasks[0].input.insert("forged".into(), json!(true));
    assert!(matches!(
        transition(
            &s,
            KernelCommand::Dispatch {
                task_id: TaskId::new("a").unwrap(),
                attempt_id: AttemptId::new("a1").unwrap()
            },
            TransitionFacts { now_ms: 100 }
        ),
        Err(KernelError::InvalidSnapshot(_))
    ));
}

#[test]
fn invalid_terminal_output_fails_closed() {
    let mut s = apply(&snapshot(vec![]), KernelCommand::Start);
    s.output = Some(json!({"fabricated":true}));
    assert!(matches!(
        b4_kernel::validate_snapshot(&s),
        Err(KernelError::InvalidSnapshot(_))
    ));
}

#[test]
fn forged_settled_running_batch_fails_closed() {
    let mut s = complete(
        &dispatch(
            &start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]),
            "a",
        ),
        "a",
        vec![],
    );
    s.status = ExecutionStatus::Running;
    s.batch_index = 0;
    s.output = None;
    assert!(matches!(
        b4_kernel::validate_snapshot(&s),
        Err(KernelError::InvalidSnapshot(_))
    ));
}

#[test]
fn fabricated_denial_cannot_skip_an_unapproved_effect() {
    let mut s = start(vec![vec![
        spec("a", false, RecoveryPolicy::Unsafe),
        spec("b", false, RecoveryPolicy::Pure),
    ]]);
    s.tasks[0].status = TaskStatus::Denied;
    assert!(matches!(
        b4_kernel::validate_snapshot(&s),
        Err(KernelError::InvalidSnapshot(_))
    ));
}

#[test]
fn nonapproval_task_cannot_carry_a_permission_record() {
    let mut s = start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]);
    s.tasks[0].interrupt = Some(PendingApproval {
        id: InterruptId::new("injected").unwrap(),
        prompt: json!(null),
    });
    assert!(matches!(
        b4_kernel::validate_snapshot(&s),
        Err(KernelError::InvalidSnapshot(_))
    ));
}

#[test]
fn unsafe_attempt_cannot_be_restored_as_ready() {
    for policy in [RecoveryPolicy::Unsafe, RecoveryPolicy::Reconcilable] {
        let mut s = dispatch(&start(vec![vec![spec("a", false, policy)]]), "a");
        s.tasks[0].status = TaskStatus::Ready;
        assert!(matches!(
            b4_kernel::validate_snapshot(&s),
            Err(KernelError::InvalidSnapshot(_))
        ));
    }
}

#[test]
fn historical_attempt_identity_cannot_be_reused() {
    let s = dispatch(
        &start(vec![vec![spec("a", false, RecoveryPolicy::Pure)]]),
        "a",
    );
    let s = apply(
        &s,
        KernelCommand::Recover {
            available_revision: s.definition.revision.clone(),
            turn_id: TurnId::new("recovered-2").unwrap(),
            owner_generation: 2,
        },
    );
    let s = apply(
        &s,
        KernelCommand::Dispatch {
            task_id: TaskId::new("a").unwrap(),
            attempt_id: AttemptId::new("attempt-a2").unwrap(),
        },
    );
    let s = apply(
        &s,
        KernelCommand::Recover {
            available_revision: s.definition.revision.clone(),
            turn_id: TurnId::new("recovered-3").unwrap(),
            owner_generation: 3,
        },
    );
    assert_eq!(
        transition(
            &s,
            KernelCommand::Dispatch {
                task_id: TaskId::new("a").unwrap(),
                attempt_id: AttemptId::new("attempt-a").unwrap()
            },
            TransitionFacts { now_ms: 100 }
        ),
        Err(KernelError::IdCollision)
    );
    assert_eq!(
        transition(
            &s,
            complete_command("a", vec![]),
            TransitionFacts { now_ms: 100 }
        ),
        Err(KernelError::AttemptMismatch)
    );
}

#[test]
fn historical_turn_identity_cannot_be_reused() {
    let s = park(
        &start(vec![
            vec![spec("a", true, RecoveryPolicy::Pure)],
            vec![spec("b", true, RecoveryPolicy::Pure)],
        ]),
        "a",
    );
    let s = apply(&s, resume(&s, &[("a", ApprovalDecision::Once)]));
    let s = park(&complete(&dispatch(&s, "a"), "a", vec![]), "b");
    let command = KernelCommand::Resume {
        route: s.definition.route.clone(),
        decisions: vec![Decision {
            interrupt_id: InterruptId::new("permission-b").unwrap(),
            decision: ApprovalDecision::Once,
        }],
        turn_id: TurnId::new("turn-1").unwrap(),
    };
    assert_eq!(
        transition(&s, command, TransitionFacts { now_ms: 100 }),
        Err(KernelError::IdCollision)
    );
}

#[test]
fn append_then_replace_cannot_hide_a_parallel_writer() {
    let s = dispatch(
        &dispatch(
            &start(vec![vec![
                spec("a", false, RecoveryPolicy::Pure),
                spec("b", false, RecoveryPolicy::Pure),
            ]]),
            "a",
        ),
        "b",
    );
    let s = complete(
        &s,
        "a",
        vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!("a")],
        }],
    );
    let s = complete(
        &s,
        "b",
        vec![
            StateWrite::Append {
                field: "items".into(),
                items: vec![json!("b")],
            },
            StateWrite::Replace {
                field: "items".into(),
                value: json!(["b"]),
            },
        ],
    );
    assert_eq!(s.status, ExecutionStatus::Failed);
    assert_eq!(s.failure.as_deref(), Some("conflicting_state_writes"));
    assert!(s.values.is_empty());
}

#[test]
fn failed_join_cannot_hide_an_inflight_effect() {
    let mut s = dispatch(
        &start(vec![vec![spec("a", false, RecoveryPolicy::Unsafe)]]),
        "a",
    );
    s.status = ExecutionStatus::Failed;
    s.failure = Some("conflicting_state_writes".into());
    assert!(matches!(
        b4_kernel::validate_snapshot(&s),
        Err(KernelError::InvalidSnapshot(_))
    ));
}

#[test]
fn blocked_unstarted_execution_cannot_fabricate_attempts_or_output() {
    let s = snapshot(vec![vec![spec("a", false, RecoveryPolicy::Unsafe)]]);
    let s = apply(
        &s,
        KernelCommand::Recover {
            available_revision: ExecutionRevision::new("unavailable").unwrap(),
            turn_id: TurnId::new("recovered-turn").unwrap(),
            owner_generation: 2,
        },
    );
    let mut output = s.clone();
    output.output = Some(Value::Null);
    assert!(matches!(
        b4_kernel::validate_snapshot(&output),
        Err(KernelError::InvalidSnapshot(_))
    ));
    let mut attempts = s;
    attempts.attempts_used = 1;
    attempts
        .attempt_history
        .push(AttemptId::new("unused").unwrap());
    assert!(matches!(
        b4_kernel::validate_snapshot(&attempts),
        Err(KernelError::InvalidSnapshot(_))
    ));
}
