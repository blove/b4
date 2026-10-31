mod confinement_support;

use b4_contract::*;
use b4_kernel::{
    propose_recovery_confinement, validate_snapshot, KernelError, RecoveryConfinementReason,
};
use confinement_support::*;
use serde_json::{json, Value};

#[test]
fn accepted_proposal_preserves_all_fields_for_each_reason_and_actual_owner_jump() {
    let original = accepted(vec![vec![spec("unstarted", true, RecoveryPolicy::Unsafe)]]);
    assert!(original.tasks.is_empty());
    assert_eq!(original.batch_index, 0);
    assert!(original.attempt_history.is_empty());
    for reason in [
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
        RecoveryConfinementReason::DispatchNotAuthorized,
    ] {
        let blocked = assert_exact_plan(&original, reason, 19, 100);
        assert!(blocked.tasks.is_empty());
        assert_eq!(blocked.failure.as_deref(), Some(failure_text(reason)));
    }
}

#[test]
fn running_proposal_keeps_ready_and_all_four_inflight_recovery_kinds_unchanged() {
    let original = accepted(vec![vec![
        spec("pure", false, RecoveryPolicy::Pure),
        spec("idempotent", false, RecoveryPolicy::Idempotent),
        spec("reconcilable", false, RecoveryPolicy::Reconcilable),
        spec("unsafe", false, RecoveryPolicy::Unsafe),
        spec("ready", false, RecoveryPolicy::Pure),
    ]]);
    let mut running = step(&original, KernelCommand::Start);
    for id in ["pure", "idempotent", "reconcilable", "unsafe"] {
        running = dispatch(&running, id);
    }
    assert!(running.tasks[..4]
        .iter()
        .all(|task| task.status == TaskStatus::InFlight));
    assert_eq!(running.tasks[4].status, TaskStatus::Ready);
    let blocked = assert_exact_plan(
        &running,
        RecoveryConfinementReason::DispatchNotAuthorized,
        7,
        400,
    );
    assert_eq!(blocked.tasks, running.tasks);
    assert_eq!(blocked.attempts_used, 4);
    assert!(blocked
        .tasks
        .iter()
        .all(|task| task.status != TaskStatus::OutcomeUnknown));
}

#[test]
fn waiting_proposal_retains_pending_interrupt_input_and_completed_sibling_result() {
    let original = accepted(vec![vec![
        spec("sibling", false, RecoveryPolicy::Pure),
        spec("permission", true, RecoveryPolicy::Unsafe),
    ]]);
    let running = step(&original, KernelCommand::Start);
    let running = complete(&dispatch(&running, "sibling"), "sibling");
    let waiting = park(&running, "permission");
    assert_eq!(waiting.status, ExecutionStatus::WaitingForApproval);
    assert!(waiting.tasks[0].result.is_some());
    assert!(waiting.tasks[1].interrupt.is_some());
    assert!(!waiting.tasks[1].approved);
    let blocked = assert_exact_plan(
        &waiting,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
        8,
        400,
    );
    assert_eq!(blocked.tasks, waiting.tasks);
    assert_eq!(blocked.values, waiting.values);
}

#[test]
fn cancellation_drain_keeps_real_inflight_attempts_and_prior_approval_history() {
    let cancelling = cancelled_drain();
    let blocked = assert_exact_plan(
        &cancelling,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        11,
        400,
    );
    assert_eq!(blocked.tasks[3].status, TaskStatus::InFlight);
    assert_eq!(blocked.tasks[4].status, TaskStatus::InFlight);
    assert_eq!(blocked.tasks[5].status, TaskStatus::Cancelled);
    assert!(blocked.tasks[5].interrupt.is_some());
    assert_eq!(blocked.tasks[2].status, TaskStatus::Denied);
    assert!(blocked.tasks[1].approved);
    assert_eq!(
        blocked.values["committed_history"],
        json!(["history", "once"])
    );
    assert_eq!(blocked.attempts_used, 4);
    assert_eq!(
        &blocked.turn_history[..2],
        cancelling.turn_history.as_slice()
    );
}

#[test]
fn failure_drain_keeps_task_cause_results_and_state_without_reduction_or_settlement() {
    let draining = failure_drain();
    let blocked = assert_exact_plan(
        &draining,
        RecoveryConfinementReason::DispatchNotAuthorized,
        5,
        400,
    );
    assert_eq!(
        blocked.tasks[3].failure.as_deref(),
        Some("source-committed-task-failure")
    );
    assert_eq!(blocked.tasks[4].status, TaskStatus::InFlight);
    assert_eq!(blocked.tasks[5].status, TaskStatus::Cancelled);
    assert_eq!(blocked.tasks[0].result, draining.tasks[0].result);
    assert_eq!(blocked.tasks[1].result, draining.tasks[1].result);
    assert_eq!(blocked.values, draining.values);
    assert_eq!(blocked.batch_index, 2);
    assert_eq!(blocked.tasks.len(), 6);
    assert!(blocked.output.is_none());
}

#[test]
fn terminal_and_old_recovery_blocked_inputs_refuse_without_rewriting_their_history() {
    let initial = accepted(vec![vec![spec("terminal", false, RecoveryPolicy::Pure)]]);
    let running = dispatch(&step(&initial, KernelCommand::Start), "terminal");
    let completed = complete(&running, "terminal");
    let failed = step(
        &running,
        KernelCommand::FailAttempt {
            task_id: TaskId::new("terminal").unwrap(),
            attempt_id: AttemptId::new("independent-attempt-terminal").unwrap(),
            failure: "terminal-known-failure".into(),
        },
    );
    let cancelled = step(
        &initial,
        KernelCommand::Cancel {
            turn_id: initial.turn_id.clone(),
            owner_generation: initial.owner_generation,
        },
    );
    for original in [completed, failed, cancelled] {
        assert_refused(
            &original,
            fresh_turn(),
            9,
            400,
            KernelError::InvalidTransition("execution_is_terminal"),
        );
    }
    let old_block = step(
        &initial,
        KernelCommand::Recover {
            available_revision: ExecutionRevision::new("missing-old-revision").unwrap(),
            turn_id: TurnId::new("old-recovery-block-turn").unwrap(),
            owner_generation: 2,
        },
    );
    assert_eq!(old_block.status, ExecutionStatus::RecoveryBlocked);
    assert_refused(
        &old_block,
        fresh_turn(),
        9,
        400,
        KernelError::InvalidTransition("execution_already_blocked"),
    );
}

#[test]
fn owner_turn_clock_and_revision_guards_preserve_original_on_refusal() {
    let original = historical_ready();
    for owner in [0, original.owner_generation] {
        assert_refused(&original, fresh_turn(), owner, 400, KernelError::StaleTurn);
    }
    let mut future_owned = original.clone();
    future_owned.owner_generation = 20;
    validate_snapshot(&future_owned).unwrap();
    assert_refused(&future_owned, fresh_turn(), 9, 400, KernelError::StaleTurn);
    for old_turn in &original.turn_history {
        assert_refused(&original, old_turn.clone(), 9, 400, KernelError::StaleTurn);
    }
    assert_refused(
        &original,
        fresh_turn(),
        9,
        original.updated_at_ms - 1,
        KernelError::ClockMovedBackwards,
    );
    assert_exact_plan(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        9,
        original.updated_at_ms,
    );
    let mut exhausted = original;
    exhausted.revision = u64::MAX;
    validate_snapshot(&exhausted).unwrap();
    assert_refused(
        &exhausted,
        fresh_turn(),
        9,
        400,
        KernelError::RevisionOverflow,
    );
}

#[test]
fn replacement_encoded_limit_is_inclusive_and_one_extra_byte_refuses_unchanged() {
    const OUTPUT_BYTES: usize = 1_048_576;
    let mut original = accepted(vec![vec![spec("not-entered", false, RecoveryPolicy::Pure)]]);
    original
        .values
        .insert("padding".into(), Value::String(String::new()));
    let reason = RecoveryConfinementReason::ExactDefinitionUnavailable;
    let base = expected(&original, reason, &fresh_turn(), 9, 400);
    let padding = OUTPUT_BYTES.checked_sub(encoded_len(&base)).unwrap();
    original
        .values
        .insert("padding".into(), Value::String("x".repeat(padding)));
    let blocked = assert_exact_plan(&original, reason, 9, 400);
    assert_eq!(encoded_len(&blocked), OUTPUT_BYTES);
    let Value::String(padding) = original.values.get_mut("padding").unwrap() else {
        unreachable!()
    };
    padding.push('x');
    validate_snapshot(&original).unwrap();
    assert_eq!(
        encoded_len(&expected(&original, reason, &fresh_turn(), 9, 400)),
        OUTPUT_BYTES + 1
    );
    assert_refused(
        &original,
        fresh_turn(),
        9,
        400,
        KernelError::InvalidTransition("recovery_confinement_limit_exceeded"),
    );
}

#[test]
fn compact_decoded_failure_drain_can_shrink_below_replacement_cap_without_second_read_cap() {
    const OUTPUT_BYTES: usize = 1_048_576;
    let build = |padding: usize| {
        let mut initial = accepted(vec![vec![
            spec("failed", false, RecoveryPolicy::Pure),
            spec("inflight", false, RecoveryPolicy::Unsafe),
        ]]);
        initial
            .values
            .insert("padding".into(), Value::String("x".repeat(padding)));
        initial
            .values
            .insert("numbers".into(), json!(vec![100_000_000.0_f64; 500]));
        let running = step(&initial, KernelCommand::Start);
        let running = dispatch(&dispatch(&running, "failed"), "inflight");
        step(
            &running,
            KernelCommand::FailAttempt {
                task_id: TaskId::new("failed").unwrap(),
                attempt_id: AttemptId::new("independent-attempt-failed").unwrap(),
                failure: "r".repeat(MAX_TASK_FAILURE_BYTES),
            },
        )
    };
    // Values are retained in the execution and each of the two actual inputs.
    let base = encoded_len(&build(0));
    let padding = (OUTPUT_BYTES + 100 - base) / 3;
    let original = build(padding);
    let canonical = serde_json::to_string(&original).unwrap();
    assert!(canonical.len() > OUTPUT_BYTES);
    assert!(canonical.len() <= OUTPUT_BYTES + 100);
    assert_eq!(canonical.matches("100000000.0").count(), 1_500);
    let compact = canonical.replace("100000000.0", "1e8");
    assert!(compact.len() <= OUTPUT_BYTES);
    let decoded: Snapshot = serde_json::from_str(&compact).unwrap();
    assert_eq!(decoded, original);
    validate_snapshot(&decoded).unwrap();
    let blocked = assert_exact_plan(
        &decoded,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        17,
        400,
    );
    assert!(encoded_len(&blocked) <= OUTPUT_BYTES);
    assert_eq!(
        blocked.tasks[0].failure.as_deref(),
        Some("r".repeat(MAX_TASK_FAILURE_BYTES).as_str())
    );
    assert_eq!(blocked.tasks[1].status, TaskStatus::InFlight);
    assert_eq!(blocked.values, decoded.values);
    // This pure proposal proves composition, not an actual SQLite transaction.
}

#[test]
fn depth_and_node_boundaries_check_input_and_the_fresh_turn_output() {
    const NODES: usize = 16_384;
    let mut original = accepted(vec![vec![spec("unentered", false, RecoveryPolicy::Pure)]]);
    let nested =
        |arrays: usize| (0..arrays).fold(Value::Null, |value, _| Value::Array(vec![value]));
    // Root Snapshot object plus values object plus62 arrays is container depth64.
    original.values.insert("nested".into(), nested(62));
    validate_snapshot(&original).unwrap();
    assert_exact_plan(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        9,
        400,
    );
    original.values.insert("nested".into(), nested(63));
    validate_snapshot(&original).unwrap();
    assert_refused(
        &original,
        fresh_turn(),
        9,
        400,
        KernelError::InvalidTransition("recovery_confinement_limit_exceeded"),
    );
    original.values.remove("nested");
    original.values.insert("wide".into(), Value::Array(vec![]));
    let values = NODES - 1 - shape_nodes(&original);
    original
        .values
        .insert("wide".into(), Value::Array(vec![Value::Null; values]));
    assert_eq!(shape_nodes(&original), NODES - 1);
    let blocked = assert_exact_plan(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        9,
        400,
    );
    assert_eq!(shape_nodes(&blocked), NODES);
    let Value::Array(wide) = original.values.get_mut("wide").unwrap() else {
        unreachable!()
    };
    wide.push(Value::Null);
    assert_eq!(shape_nodes(&original), NODES);
    validate_snapshot(&original).unwrap();
    assert_refused(
        &original,
        fresh_turn(),
        9,
        400,
        KernelError::InvalidTransition("recovery_confinement_limit_exceeded"),
    );
}

#[test]
fn standalone_safe_clone_byte_guard_precedes_kernel_validation_without_claiming_output_success() {
    const INPUT_BYTES: usize = 8_388_608;
    let mut original = accepted(vec![vec![spec("unentered", false, RecoveryPolicy::Pure)]]);
    original.owner_generation = 0;
    original
        .values
        .insert("padding".into(), Value::String(String::new()));
    let padding = INPUT_BYTES - encoded_len(&original);
    original
        .values
        .insert("padding".into(), Value::String("x".repeat(padding)));
    assert_eq!(encoded_len(&original), INPUT_BYTES);
    assert!(matches!(
        validate_snapshot(&original),
        Err(KernelError::InvalidSnapshot(_))
    ));
    let at_limit = propose_recovery_confinement(
        &original,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        fresh_turn(),
        9,
        TransitionFacts { now_ms: 400 },
    );
    assert!(matches!(at_limit, Err(KernelError::InvalidSnapshot(_))));
    let Value::String(padding) = original.values.get_mut("padding").unwrap() else {
        unreachable!()
    };
    padding.push('x');
    assert_eq!(encoded_len(&original), INPUT_BYTES + 1);
    assert_refused(
        &original,
        fresh_turn(),
        9,
        400,
        KernelError::InvalidTransition("recovery_confinement_limit_exceeded"),
    );
}
