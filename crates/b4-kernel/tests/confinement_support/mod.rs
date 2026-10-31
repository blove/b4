use b4_contract::*;
use b4_kernel::{
    new_execution, transition, validate_snapshot, KernelError, RecoveryConfinementReason,
};
use serde_json::{json, Map, Value};

pub fn spec(id: &str, approval: bool, recovery: RecoveryPolicy) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        effect_id: EffectId::new(format!("independent-effect-{id}")).unwrap(),
        recovery,
        requires_approval: approval,
    }
}

pub fn accepted(batches: Vec<Vec<TaskSpec>>) -> Snapshot {
    new_execution(
        ExecutionId::new("independent-confinement-execution").unwrap(),
        TurnId::new("independent-original-turn").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/independent-confinement#workflow").unwrap(),
            revision: ExecutionRevision::new("recorded-v1").unwrap(),
            batches,
            max_attempts: 16,
        },
        Map::from_iter([("original_state".into(), json!({"retained": true}))]),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap()
}

pub fn step(original: &Snapshot, command: KernelCommand) -> Snapshot {
    let plan = transition(
        original,
        command,
        TransitionFacts {
            now_ms: original.updated_at_ms.checked_add(1).unwrap(),
        },
    )
    .unwrap();
    assert_eq!(plan.expected_revision, original.revision);
    validate_snapshot(&plan.snapshot).unwrap();
    plan.snapshot
}

pub fn dispatch(original: &Snapshot, id: &str) -> Snapshot {
    step(
        original,
        KernelCommand::Dispatch {
            task_id: TaskId::new(id).unwrap(),
            attempt_id: AttemptId::new(format!("independent-attempt-{id}")).unwrap(),
        },
    )
}

pub fn complete(original: &Snapshot, id: &str) -> Snapshot {
    step(
        original,
        KernelCommand::Complete {
            task_id: TaskId::new(id).unwrap(),
            attempt_id: AttemptId::new(format!("independent-attempt-{id}")).unwrap(),
            result: TaskResult {
                output: json!({"completed_task": id}),
                writes: vec![StateWrite::Append {
                    field: "committed_history".into(),
                    items: vec![json!(id)],
                }],
            },
        },
    )
}

pub fn park(original: &Snapshot, id: &str) -> Snapshot {
    step(
        original,
        KernelCommand::Park {
            task_id: TaskId::new(id).unwrap(),
            interrupt_id: InterruptId::new(format!("independent-interrupt-{id}")).unwrap(),
            prompt: json!({"original_prompt": id, "audit": [true, null]}),
        },
    )
}

pub fn historical_ready() -> Snapshot {
    let initial = accepted(vec![
        vec![spec("history", false, RecoveryPolicy::Pure)],
        vec![
            spec("once", true, RecoveryPolicy::Pure),
            spec("deny", true, RecoveryPolicy::Unsafe),
        ],
        vec![
            spec("left", false, RecoveryPolicy::Pure),
            spec("right", false, RecoveryPolicy::Unsafe),
            spec("unsent", true, RecoveryPolicy::Reconcilable),
        ],
        vec![spec("future", false, RecoveryPolicy::Idempotent)],
    ]);
    let running = step(&initial, KernelCommand::Start);
    let running = complete(&dispatch(&running, "history"), "history");
    let waiting = park(&park(&running, "once"), "deny");
    assert_eq!(waiting.status, ExecutionStatus::WaitingForApproval);
    let resumed = step(
        &waiting,
        KernelCommand::Resume {
            route: waiting.definition.route.clone(),
            decisions: vec![
                Decision {
                    interrupt_id: InterruptId::new("independent-interrupt-once").unwrap(),
                    decision: ApprovalDecision::Once,
                },
                Decision {
                    interrupt_id: InterruptId::new("independent-interrupt-deny").unwrap(),
                    decision: ApprovalDecision::Deny,
                },
            ],
            turn_id: TurnId::new("independent-resumed-turn").unwrap(),
        },
    );
    let ready = complete(&dispatch(&resumed, "once"), "once");
    assert_eq!(ready.status, ExecutionStatus::Running);
    assert_eq!(ready.batch_index, 2);
    assert_eq!(ready.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(ready.tasks[1].status, TaskStatus::Succeeded);
    assert!(ready.tasks[1].approved);
    assert_eq!(ready.tasks[2].status, TaskStatus::Denied);
    assert_eq!(
        ready.values["committed_history"],
        json!(["history", "once"])
    );
    assert_eq!(ready.turn_history.len(), 2);
    ready
}

pub fn historical_inflight() -> Snapshot {
    let ready = historical_ready();
    let inflight = dispatch(&dispatch(&ready, "left"), "right");
    park(&inflight, "unsent")
}

pub fn failure_drain() -> Snapshot {
    let original = historical_inflight();
    let draining = step(
        &original,
        KernelCommand::FailAttempt {
            task_id: TaskId::new("left").unwrap(),
            attempt_id: AttemptId::new("independent-attempt-left").unwrap(),
            failure: "source-committed-task-failure".into(),
        },
    );
    assert_eq!(draining.status, ExecutionStatus::Running);
    assert_eq!(draining.tasks[3].status, TaskStatus::Failed);
    assert_eq!(draining.tasks[4].status, TaskStatus::InFlight);
    assert_eq!(draining.tasks[5].status, TaskStatus::Cancelled);
    draining
}

pub fn cancelled_drain() -> Snapshot {
    let original = historical_inflight();
    let cancelling = step(
        &original,
        KernelCommand::Cancel {
            turn_id: original.turn_id.clone(),
            owner_generation: original.owner_generation,
        },
    );
    assert_eq!(cancelling.status, ExecutionStatus::CancelRequested);
    assert_eq!(cancelling.tasks[3].status, TaskStatus::InFlight);
    assert_eq!(cancelling.tasks[4].status, TaskStatus::InFlight);
    assert_eq!(cancelling.tasks[5].status, TaskStatus::Cancelled);
    cancelling
}

pub fn failure_text(reason: RecoveryConfinementReason) -> &'static str {
    match reason {
        RecoveryConfinementReason::ExactDefinitionUnavailable => "recovery_definition_unavailable",
        RecoveryConfinementReason::RequiredAuthorityUnavailable => "recovery_authority_unavailable",
        RecoveryConfinementReason::DispatchNotAuthorized => "recovery_dispatch_not_authorized",
    }
}

pub fn expected(
    original: &Snapshot,
    reason: RecoveryConfinementReason,
    fresh_turn: &TurnId,
    owner: u64,
    now_ms: u64,
) -> Snapshot {
    let mut next = original.clone();
    next.status = ExecutionStatus::RecoveryBlocked;
    next.failure = Some(failure_text(reason).into());
    next.owner_generation = owner;
    next.turn_id = fresh_turn.clone();
    next.turn_history.push(fresh_turn.clone());
    next.revision = original.revision.checked_add(1).unwrap();
    next.updated_at_ms = now_ms;
    next
}

pub fn assert_exact_plan(
    original: &Snapshot,
    reason: RecoveryConfinementReason,
    owner: u64,
    now_ms: u64,
) -> Snapshot {
    validate_snapshot(original).unwrap();
    let unchanged = original.clone();
    let turn = TurnId::new("independent-confinement-turn").unwrap();
    let plan = b4_kernel::propose_recovery_confinement(
        original,
        reason,
        turn.clone(),
        owner,
        TransitionFacts { now_ms },
    )
    .unwrap();
    assert_eq!(original, &unchanged);
    assert_eq!(plan.expected_revision, original.revision);
    assert!(plan.dispatchable.is_empty());
    assert_eq!(
        plan.snapshot,
        expected(original, reason, &turn, owner, now_ms)
    );
    validate_snapshot(&plan.snapshot).unwrap();
    plan.snapshot
}

pub fn assert_refused(
    original: &Snapshot,
    turn: TurnId,
    owner: u64,
    now_ms: u64,
    refusal: KernelError,
) {
    let before = serde_json::to_vec(original).unwrap();
    assert_eq!(
        b4_kernel::propose_recovery_confinement(
            original,
            RecoveryConfinementReason::ExactDefinitionUnavailable,
            turn,
            owner,
            TransitionFacts { now_ms },
        ),
        Err(refusal)
    );
    assert_eq!(serde_json::to_vec(original).unwrap(), before);
}

pub fn fresh_turn() -> TurnId {
    TurnId::new("independent-confinement-turn").unwrap()
}

pub fn encoded_len(snapshot: &Snapshot) -> usize {
    serde_json::to_vec(snapshot).unwrap().len()
}

/// Independent JSON document accounting: keys and each value count as nodes.
/// This fixture helper does not call or copy the private lexical sink.
pub fn shape_nodes(snapshot: &Snapshot) -> usize {
    let document = serde_json::to_value(snapshot).unwrap();
    let mut count = 0;
    let mut work = vec![&document];
    while let Some(value) = work.pop() {
        count += 1;
        match value {
            Value::Object(object) => {
                count += object.len();
                work.extend(object.values());
            }
            Value::Array(array) => work.extend(array),
            _ => {}
        }
    }
    count
}
