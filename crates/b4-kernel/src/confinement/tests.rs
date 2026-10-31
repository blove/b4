use super::{propose_recovery_confinement, RecoveryConfinementReason};
use crate::{new_execution, transition, validate_snapshot, KernelError};
use b4_contract::*;
use serde_json::{json, Map, Value};

const OUTPUT_BYTES: usize = 1_048_576;
const CLONE_BYTES: usize = 8_388_608;

fn spec(id: &str, approval: bool, recovery: RecoveryPolicy) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        effect_id: EffectId::new(format!("effect-{id}")).unwrap(),
        recovery,
        requires_approval: approval,
    }
}

fn accepted(batches: Vec<Vec<TaskSpec>>, values: Map<String, Value>) -> Snapshot {
    new_execution(
        ExecutionId::new("confinement-execution").unwrap(),
        TurnId::new("original-turn").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/confinement#workflow").unwrap(),
            revision: ExecutionRevision::new("original-definition").unwrap(),
            batches,
            max_attempts: 16,
        },
        values,
        TransitionFacts { now_ms: 100 },
    )
    .unwrap()
}

fn basic() -> Snapshot {
    accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        Map::new(),
    )
}

fn apply(snapshot: &Snapshot, command: KernelCommand) -> Snapshot {
    transition(snapshot, command, TransitionFacts { now_ms: 100 })
        .unwrap()
        .snapshot
}

fn dispatch(snapshot: &Snapshot, task: &str) -> Snapshot {
    apply(
        snapshot,
        KernelCommand::Dispatch {
            task_id: TaskId::new(task).unwrap(),
            attempt_id: AttemptId::new(format!("attempt-{task}")).unwrap(),
        },
    )
}

fn park(snapshot: &Snapshot, task: &str) -> Snapshot {
    apply(
        snapshot,
        KernelCommand::Park {
            task_id: TaskId::new(task).unwrap(),
            interrupt_id: InterruptId::new(format!("interrupt-{task}")).unwrap(),
            prompt: json!({"instruction": "retain this original prompt"}),
        },
    )
}

fn fresh_turn() -> TurnId {
    TurnId::new("confinement-turn").unwrap()
}

fn expected(snapshot: &Snapshot, reason: &str, owner: u64) -> Snapshot {
    let mut next = snapshot.clone();
    next.status = ExecutionStatus::RecoveryBlocked;
    next.failure = Some(reason.into());
    next.owner_generation = owner;
    next.turn_id = fresh_turn();
    next.turn_history.push(fresh_turn());
    next.revision += 1;
    next.updated_at_ms = 250;
    next
}

fn check(snapshot: &Snapshot, reason: RecoveryConfinementReason, code: &str, owner: u64) {
    validate_snapshot(snapshot).expect("legal original from kernel transitions");
    let before = serde_json::to_vec(snapshot).unwrap();
    let plan = propose_recovery_confinement(
        snapshot,
        reason,
        fresh_turn(),
        owner,
        TransitionFacts { now_ms: 250 },
    )
    .expect("eligible prior-owner work must be confined");
    assert_eq!(plan.expected_revision, snapshot.revision);
    assert!(plan.dispatchable.is_empty());
    assert_eq!(plan.snapshot, expected(snapshot, code, owner));
    validate_snapshot(&plan.snapshot).unwrap();
    assert_eq!(serde_json::to_vec(snapshot).unwrap(), before);
}

fn proposal(
    snapshot: &Snapshot,
    turn: TurnId,
    owner: u64,
    now_ms: u64,
) -> Result<CommitPlan, KernelError> {
    propose_recovery_confinement(
        snapshot,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        turn,
        owner,
        TransitionFacts { now_ms },
    )
}

fn limit() -> KernelError {
    KernelError::InvalidTransition("recovery_confinement_limit_exceeded")
}

fn failure_drain(values: Map<String, Value>) -> Snapshot {
    let original = accepted(
        vec![vec![
            spec("failed", false, RecoveryPolicy::Pure),
            spec("sibling", false, RecoveryPolicy::Unsafe),
        ]],
        values,
    );
    let running = dispatch(
        &dispatch(&apply(&original, KernelCommand::Start), "failed"),
        "sibling",
    );
    let drain = apply(
        &running,
        KernelCommand::FailAttempt {
            task_id: TaskId::new("failed").unwrap(),
            attempt_id: AttemptId::new("attempt-failed").unwrap(),
            failure: "x".repeat(MAX_TASK_FAILURE_BYTES),
        },
    );
    assert_eq!(drain.status, ExecutionStatus::Running);
    assert_eq!(drain.tasks[0].status, TaskStatus::Failed);
    assert_eq!(drain.tasks[1].status, TaskStatus::InFlight);
    drain
}

#[test]
fn missing_definition_blocks_unstarted_work_with_full_definition_and_values() {
    let mut values = Map::new();
    values.insert(
        "retained".into(),
        json!({"values": [null, true, "original"]}),
    );
    let original = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Unsafe)]],
        values,
    );
    check(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        "recovery_definition_unavailable",
        2,
    );
}

#[test]
fn missing_authority_preserves_prior_batch_results_and_current_input() {
    let original = accepted(
        vec![
            vec![spec("first", false, RecoveryPolicy::Pure)],
            vec![spec("second", false, RecoveryPolicy::Idempotent)],
        ],
        Map::new(),
    );
    let running = dispatch(&apply(&original, KernelCommand::Start), "first");
    let second = apply(
        &running,
        KernelCommand::Complete {
            task_id: TaskId::new("first").unwrap(),
            attempt_id: AttemptId::new("attempt-first").unwrap(),
            result: TaskResult {
                output: json!({"kept": true}),
                writes: vec![StateWrite::Replace {
                    field: "kept".into(),
                    value: json!(17),
                }],
            },
        },
    );
    assert_eq!(second.batch_index, 1);
    assert_eq!(second.tasks[0].status, TaskStatus::Succeeded);
    check(
        &second,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
        "recovery_authority_unavailable",
        2,
    );
}

#[test]
fn policy_denial_preserves_waiting_tasks_and_original_prompts() {
    let original = accepted(
        vec![vec![spec("approval", true, RecoveryPolicy::Unsafe)]],
        Map::new(),
    );
    let waiting = park(&apply(&original, KernelCommand::Start), "approval");
    assert_eq!(waiting.status, ExecutionStatus::WaitingForApproval);
    check(
        &waiting,
        RecoveryConfinementReason::DispatchNotAuthorized,
        "recovery_dispatch_not_authorized",
        2,
    );
}

#[test]
fn in_flight_effects_of_every_recovery_kind_are_preserved_without_settlement() {
    for recovery in [
        RecoveryPolicy::Pure,
        RecoveryPolicy::Idempotent,
        RecoveryPolicy::Reconcilable,
        RecoveryPolicy::Unsafe,
    ] {
        let original = accepted(vec![vec![spec("first", false, recovery)]], Map::new());
        let running = dispatch(&apply(&original, KernelCommand::Start), "first");
        check(
            &running,
            RecoveryConfinementReason::ExactDefinitionUnavailable,
            "recovery_definition_unavailable",
            2,
        );
    }
}

#[test]
fn cancellation_drain_keeps_in_flight_effect_and_cancelled_prompt() {
    let original = accepted(
        vec![vec![
            spec("first", false, RecoveryPolicy::Unsafe),
            spec("approval", true, RecoveryPolicy::Pure),
        ]],
        Map::new(),
    );
    let running = park(
        &dispatch(&apply(&original, KernelCommand::Start), "first"),
        "approval",
    );
    assert_eq!(running.status, ExecutionStatus::Running);
    let cancelling = apply(
        &running,
        KernelCommand::Cancel {
            turn_id: running.turn_id.clone(),
            owner_generation: running.owner_generation,
        },
    );
    assert_eq!(cancelling.status, ExecutionStatus::CancelRequested);
    assert_eq!(cancelling.tasks[1].status, TaskStatus::Cancelled);
    assert!(cancelling.tasks[1].interrupt.is_some());
    check(
        &cancelling,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
        "recovery_authority_unavailable",
        2,
    );
}

#[test]
fn failure_drain_replaces_only_execution_reason_and_keeps_failed_task_evidence() {
    let drain = failure_drain(Map::new());
    check(
        &drain,
        RecoveryConfinementReason::DispatchNotAuthorized,
        "recovery_dispatch_not_authorized",
        2,
    );
}

#[test]
fn adoption_uses_actual_owner_even_after_multiple_epochs() {
    check(
        &basic(),
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        "recovery_definition_unavailable",
        7,
    );
}

#[test]
fn all_terminal_states_refuse_without_changing_the_original() {
    let accepted = accepted(vec![], Map::new());
    let completed = apply(&accepted, KernelCommand::Start);
    let initial = basic();
    let cancelled = apply(
        &initial,
        KernelCommand::Cancel {
            turn_id: initial.turn_id.clone(),
            owner_generation: 1,
        },
    );
    let running = dispatch(&apply(&initial, KernelCommand::Start), "first");
    let failed = apply(
        &running,
        KernelCommand::FailAttempt {
            task_id: TaskId::new("first").unwrap(),
            attempt_id: AttemptId::new("attempt-first").unwrap(),
            failure: "failure".into(),
        },
    );
    for original in [completed, cancelled, failed] {
        validate_snapshot(&original).unwrap();
        let before = original.clone();
        assert_eq!(
            proposal(&original, fresh_turn(), 2, 250),
            Err(KernelError::InvalidTransition("execution_is_terminal"))
        );
        assert_eq!(original, before);
    }
}

#[test]
fn prior_block_is_an_unchanged_backend_observation_not_a_second_kernel_adoption() {
    let original = basic();
    let blocked = apply(
        &original,
        KernelCommand::Recover {
            available_revision: ExecutionRevision::new("unavailable-definition").unwrap(),
            turn_id: TurnId::new("old-block-turn").unwrap(),
            owner_generation: 2,
        },
    );
    assert_eq!(blocked.status, ExecutionStatus::RecoveryBlocked);
    let before = blocked.clone();
    assert_eq!(
        proposal(&blocked, fresh_turn(), 3, 250),
        Err(KernelError::InvalidTransition("execution_already_blocked"))
    );
    assert_eq!(blocked, before);
}

#[test]
fn zero_current_owner_current_or_future_snapshot_and_reused_turn_refuse() {
    let original = basic();
    for owner in [0, 1] {
        assert_eq!(
            proposal(&original, fresh_turn(), owner, 250),
            Err(KernelError::StaleTurn)
        );
    }
    let mut future = original.clone();
    future.owner_generation = 3;
    validate_snapshot(&future).unwrap();
    assert_eq!(
        proposal(&future, fresh_turn(), 2, 250),
        Err(KernelError::StaleTurn)
    );
    assert_eq!(
        proposal(&original, original.turn_id.clone(), 2, 250),
        Err(KernelError::StaleTurn)
    );
}

#[test]
fn backwards_time_refuses_without_a_proposal() {
    assert_eq!(
        proposal(&basic(), fresh_turn(), 2, 99),
        Err(KernelError::ClockMovedBackwards)
    );
}

#[test]
fn exhausted_revision_refuses_without_wrapping() {
    let mut original = basic();
    original.revision = u64::MAX;
    validate_snapshot(&original).unwrap();
    assert_eq!(
        proposal(&original, fresh_turn(), 2, 250),
        Err(KernelError::RevisionOverflow)
    );
}

#[test]
fn zero_stored_owner_and_unsupported_version_remain_invalid_snapshots() {
    let mut original = basic();
    original.owner_generation = 0;
    assert!(matches!(
        proposal(&original, fresh_turn(), 2, 250),
        Err(KernelError::InvalidSnapshot(_))
    ));
    original.owner_generation = 1;
    original.schema_version += 1;
    assert_eq!(
        proposal(&original, fresh_turn(), 2, 250),
        Err(KernelError::InvalidSnapshot(
            "unsupported_snapshot_version".into()
        ))
    );
}

fn nested(depth: usize) -> Value {
    let mut value = json!(0);
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    value
}

#[test]
fn depth64_inclusive_input_is_confined() {
    let mut values = Map::new();
    values.insert("nested".into(), nested(62));
    let original = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    check(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        "recovery_definition_unavailable",
        2,
    );
}

#[test]
fn depth65_is_refused_before_recursive_clone_or_validation() {
    let mut values = Map::new();
    values.insert("nested".into(), nested(63));
    let original = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    assert_eq!(proposal(&original, fresh_turn(), 2, 250), Err(limit()));
}

#[test]
fn excessive_nodes_refuse_even_when_encoded_bytes_are_small() {
    let mut values = Map::new();
    values.insert("nodes".into(), Value::Array(vec![json!(0); 16_384]));
    let original = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    assert!(serde_json::to_vec(&original).unwrap().len() < OUTPUT_BYTES);
    assert_eq!(proposal(&original, fresh_turn(), 2, 250), Err(limit()));
}

#[test]
fn standalone_typed_input_over8_mib_refuses() {
    let mut values = Map::new();
    values.insert("resident".into(), Value::String("x".repeat(CLONE_BYTES)));
    let original = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    assert_eq!(proposal(&original, fresh_turn(), 2, 250), Err(limit()));
}

fn output_boundary(extra: usize) -> Snapshot {
    let mut values = Map::new();
    values.insert("padding".into(), json!(""));
    let base = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    let bytes = serde_json::to_vec(&expected(&base, "recovery_definition_unavailable", 2))
        .unwrap()
        .len();
    let mut values = Map::new();
    values.insert(
        "padding".into(),
        Value::String("x".repeat(OUTPUT_BYTES - bytes + extra)),
    );
    accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    )
}

#[test]
fn replacement_at1_mib_inclusive_is_accepted() {
    let original = output_boundary(0);
    assert_eq!(
        serde_json::to_vec(&expected(&original, "recovery_definition_unavailable", 2))
            .unwrap()
            .len(),
        OUTPUT_BYTES
    );
    check(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        "recovery_definition_unavailable",
        2,
    );
}

#[test]
fn replacement_one_byte_over1_mib_refuses() {
    let original = output_boundary(1);
    assert_eq!(
        serde_json::to_vec(&expected(&original, "recovery_definition_unavailable", 2))
            .unwrap()
            .len(),
        OUTPUT_BYTES + 1
    );
    assert_eq!(proposal(&original, fresh_turn(), 2, 250), Err(limit()));
}

#[test]
fn canonical_input_above1_mib_can_shrink_to_an_eligible_replacement() {
    let mut values = Map::new();
    values.insert("padding".into(), json!(""));
    values.insert("numbers".into(), Value::Array(vec![json!(10.0); 400]));
    let base = failure_drain(values.clone());
    let base_bytes = serde_json::to_vec(&base).unwrap().len();
    values.insert(
        "padding".into(),
        Value::String("x".repeat((OUTPUT_BYTES - base_bytes) / 3 + 100)),
    );
    let original = failure_drain(values);
    assert!(serde_json::to_vec(&original).unwrap().len() > OUTPUT_BYTES);
    assert!(
        serde_json::to_vec(&expected(&original, "recovery_definition_unavailable", 2))
            .unwrap()
            .len()
            < OUTPUT_BYTES
    );
    check(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        "recovery_definition_unavailable",
        2,
    );
}

#[test]
fn escaped_container_text_utf8_and_numeric_spellings_do_not_invent_shape() {
    let mut values = Map::new();
    values.insert(
        "escaped\"[{\\key".into(),
        json!({"kept": "\"[]{}\\\n😀".repeat(1000), "number": -1.5e20}),
    );
    let original = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    check(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        "recovery_definition_unavailable",
        2,
    );
}

// Independent structural counting in tests; serialized object keys count too.
fn encoded_nodes(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(encoded_nodes).sum::<usize>(),
        Value::Object(values) => {
            1 + values
                .values()
                .map(|value| 1 + encoded_nodes(value))
                .sum::<usize>()
        }
        _ => 1,
    }
}

fn node_boundary(nodes: usize) -> Snapshot {
    let mut values = Map::new();
    values.insert("nodes".into(), Value::Array(vec![]));
    let base = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    let base_nodes = encoded_nodes(&serde_json::to_value(&base).unwrap());
    let mut values = Map::new();
    values.insert(
        "nodes".into(),
        Value::Array(vec![json!(0); nodes - base_nodes]),
    );
    let original = accepted(
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
        values,
    );
    assert_eq!(
        encoded_nodes(&serde_json::to_value(&original).unwrap()),
        nodes
    );
    original
}

#[test]
fn fresh_turn_that_reaches_output_node_limit_is_accepted() {
    let original = node_boundary(16_383);
    assert_eq!(
        encoded_nodes(
            &serde_json::to_value(expected(&original, "recovery_definition_unavailable", 2))
                .unwrap()
        ),
        16_384
    );
    check(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        "recovery_definition_unavailable",
        2,
    );
}

#[test]
fn fresh_turn_that_exceeds_output_node_limit_is_refused() {
    let original = node_boundary(16_384);
    assert_eq!(
        encoded_nodes(
            &serde_json::to_value(expected(&original, "recovery_definition_unavailable", 2))
                .unwrap()
        ),
        16_385
    );
    assert_eq!(proposal(&original, fresh_turn(), 2, 250), Err(limit()));
}

#[test]
fn reused_older_turn_is_refused_even_when_current_turn_is_different() {
    let original = basic();
    let recovered = apply(
        &original,
        KernelCommand::Recover {
            available_revision: original.definition.revision.clone(),
            turn_id: TurnId::new("distinct-current-turn").unwrap(),
            owner_generation: 2,
        },
    );
    assert_ne!(recovered.turn_id, original.turn_id);
    assert!(recovered.turn_history.contains(&original.turn_id));
    validate_snapshot(&recovered).unwrap();
    assert_eq!(
        proposal(&recovered, original.turn_id.clone(), 3, 250),
        Err(KernelError::StaleTurn)
    );
}
