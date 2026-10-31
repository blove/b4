use b4_contract::permissions::PermissionUseId;
use b4_contract::*;
use b4_kernel::{
    authorize_ready, new_execution, transition, validate_snapshot, KernelError,
    PermissionAuthorizationCommand,
};
use serde_json::{json, Map, Value};

fn kernel_error_code(error: &KernelError) -> &'static str {
    match error {
        KernelError::InvalidSnapshot(_) => "invalid_snapshot",
        KernelError::InvalidTransition("permission_authorization_unsupported") => {
            "permission_authorization_unsupported"
        }
        KernelError::InvalidTransition(_) => "other_invalid_transition",
        KernelError::UnknownTask => "unknown_task",
        KernelError::AttemptMismatch => "attempt_mismatch",
        KernelError::ResultConflict => "result_conflict",
        KernelError::InterruptSetMismatch => "interrupt_set_mismatch",
        KernelError::RouteMismatch => "route_mismatch",
        KernelError::StaleTurn => "stale_turn",
        KernelError::BudgetExceeded => "budget_exceeded",
        KernelError::IdCollision => "id_collision",
        KernelError::RevisionOverflow => "revision_overflow",
        KernelError::ClockMovedBackwards => "clock_moved_backwards",
    }
}

#[test]
fn permission_authorization_approves_actual_ready_and_preserves_waiting_sibling() {
    let selected_id = TaskId::new("selected").expect("fixed selected identity");
    let sibling_id = TaskId::new("sibling").expect("fixed sibling identity");
    let selected_spec = TaskSpec {
        id: selected_id.clone(),
        effect_id: EffectId::new("selected-effect").expect("fixed selected effect"),
        recovery: RecoveryPolicy::Pure,
        requires_approval: true,
    };
    let sibling_spec = TaskSpec {
        id: sibling_id.clone(),
        effect_id: EffectId::new("sibling-effect").expect("fixed sibling effect"),
        recovery: RecoveryPolicy::Pure,
        requires_approval: true,
    };
    let definition = Definition {
        route: RouteKey::new("/permission-ready#agent").expect("fixed route"),
        revision: ExecutionRevision::new("definition-1").expect("fixed definition revision"),
        batches: vec![vec![selected_spec.clone(), sibling_spec.clone()]],
        max_attempts: 4,
    };
    let mut values = Map::new();
    values.insert("items".into(), json!([1, 2]));
    values.insert("label".into(), Value::String("ready".into()));
    let initial = new_execution(
        ExecutionId::new("pure-ready-execution").expect("fixed execution identity"),
        TurnId::new("pure-ready-turn").expect("fixed turn identity"),
        1,
        definition.clone(),
        values.clone(),
        TransitionFacts { now_ms: 100 },
    )
    .expect("public new_execution must accept the fixed fixture");
    validate_snapshot(&initial).expect("strict initial fixture validation");
    let started = transition(
        &initial,
        KernelCommand::Start,
        TransitionFacts { now_ms: 101 },
    )
    .expect("public Start must initialize the fixed fixture")
    .snapshot;
    validate_snapshot(&started).expect("strict started fixture validation");
    let sibling_interrupt =
        InterruptId::new("sibling-human-interrupt").expect("fixed sibling interrupt identity");
    let sibling_prompt = json!({"approval": "sibling"});
    let before = transition(
        &started,
        KernelCommand::Park {
            task_id: sibling_id.clone(),
            interrupt_id: sibling_interrupt.clone(),
            prompt: sibling_prompt.clone(),
        },
        TransitionFacts { now_ms: 102 },
    )
    .expect("public Park of only the sibling must remain legal")
    .snapshot;
    validate_snapshot(&before).expect("strict mixed Ready and Waiting fixture validation");
    assert_eq!(before.status, ExecutionStatus::Running);
    assert_eq!(before.revision, 2);
    assert_eq!(before.batch_index, 0);
    assert_eq!(before.updated_at_ms, 102);
    assert_eq!(before.definition, definition);
    assert_eq!(before.values, values);
    assert_eq!(before.execution_id, initial.execution_id);
    assert_eq!(before.owner_generation, initial.owner_generation);
    assert_eq!(before.turn_id, initial.turn_id);
    assert_eq!(before.turn_history, initial.turn_history);
    assert_eq!(before.attempts_used, 0);
    assert!(before.attempt_history.is_empty());
    assert!(before.output.is_none());
    assert!(before.failure.is_none());
    assert_eq!(before.tasks.len(), 2);
    let selected_before = &before.tasks[0];
    let sibling_before = &before.tasks[1];
    assert_eq!(selected_before.spec, selected_spec);
    assert_eq!(selected_before.status, TaskStatus::Ready);
    assert_eq!(selected_before.input, before.values);
    assert!(!selected_before.approved);
    assert!(selected_before.attempt_id.is_none());
    assert!(selected_before.result.is_none());
    assert!(selected_before.failure.is_none());
    assert!(selected_before.interrupt.is_none());
    assert_eq!(sibling_before.spec, sibling_spec);
    assert_eq!(sibling_before.status, TaskStatus::WaitingForApproval);
    assert_eq!(sibling_before.input, before.values);
    assert!(!sibling_before.approved);
    assert!(sibling_before.attempt_id.is_none());
    assert!(sibling_before.result.is_none());
    assert!(sibling_before.failure.is_none());
    assert_eq!(
        sibling_before.interrupt.as_ref(),
        Some(&PendingApproval {
            id: sibling_interrupt,
            prompt: sibling_prompt,
        })
    );
    let use_id = PermissionUseId::new(
        "b4pu_0000000000000000000000000000000000000000000000000000000000000000",
    )
    .expect("fixed checked use intent identity");
    let command = PermissionAuthorizationCommand::capture(&before, selected_id, use_id.clone())
        .expect("capture must accept the actual required unapproved Ready fixture");
    assert_eq!(command.execution_id(), &before.execution_id);
    assert_eq!(command.expected_revision(), before.revision);
    assert_eq!(command.owner_generation(), before.owner_generation);
    assert_eq!(command.turn_id(), &before.turn_id);
    assert_eq!(command.definition(), &before.definition);
    assert_eq!(command.batch_index(), before.batch_index);
    assert_eq!(command.task_spec(), &selected_before.spec);
    assert_eq!(command.input(), &selected_before.input);
    assert_eq!(command.use_id(), &use_id);

    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 103 });
    assert!(
        result.is_ok(),
        "pure Ready authorization must produce a plan: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );

    let plan = result.expect("the preceding primary established a successful plan");
    assert_eq!(plan.expected_revision, before.revision);
    assert!(plan.dispatchable.is_empty());
    let after = &plan.snapshot;
    assert_eq!(
        after.revision,
        before.revision.checked_add(1).expect("tiny revision")
    );
    assert_eq!(after.updated_at_ms, 103);
    assert_eq!(after.schema_version, before.schema_version);
    assert_eq!(after.execution_id, before.execution_id);
    assert_eq!(after.turn_id, before.turn_id);
    assert_eq!(after.turn_history, before.turn_history);
    assert_eq!(after.attempt_history, before.attempt_history);
    assert_eq!(after.owner_generation, before.owner_generation);
    assert_eq!(after.definition, before.definition);
    assert_eq!(after.status, before.status);
    assert_eq!(after.batch_index, before.batch_index);
    assert_eq!(after.values, before.values);
    assert_eq!(after.attempts_used, before.attempts_used);
    assert_eq!(after.output, before.output);
    assert_eq!(after.failure, before.failure);
    assert_eq!(after.tasks.len(), before.tasks.len());
    assert_eq!(&after.tasks[1], sibling_before);
    let selected_after = &after.tasks[0];
    assert_eq!(selected_after.spec, selected_before.spec);
    assert_eq!(selected_after.status, selected_before.status);
    assert_eq!(selected_after.input, selected_before.input);
    assert_eq!(selected_after.attempt_id, selected_before.attempt_id);
    assert_eq!(selected_after.result, selected_before.result);
    assert_eq!(selected_after.failure, selected_before.failure);
    assert!(selected_after.approved);
    assert_eq!(
        selected_after.interrupt.as_ref(),
        Some(&PendingApproval {
            id: InterruptId::new(use_id.as_str()).expect("checked69 ASCII marker identity"),
            prompt: Value::Null,
        })
    );
    validate_snapshot(after).expect("unchanged strict validator must accept the exact after plan");
}

const REFUSAL_USE_ID: &str =
    "b4pu_0000000000000000000000000000000000000000000000000000000000000000";

fn refusal_use_id() -> PermissionUseId {
    PermissionUseId::new(REFUSAL_USE_ID).expect("fixed checked refusal intent identity")
}

fn refusal_started(owner: u64, label: &str) -> Snapshot {
    assert!(matches!(owner, 1 | 2));
    assert!(matches!(label, "ready" | "changed"));
    let definition = Definition {
        route: RouteKey::new("/permission-ready#agent").expect("fixed refusal route"),
        revision: ExecutionRevision::new("definition-1")
            .expect("fixed refusal definition revision"),
        batches: vec![vec![
            TaskSpec {
                id: TaskId::new("selected").expect("fixed refusal selected task"),
                effect_id: EffectId::new("selected-effect").expect("fixed refusal selected effect"),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
            TaskSpec {
                id: TaskId::new("sibling").expect("fixed refusal sibling task"),
                effect_id: EffectId::new("sibling-effect").expect("fixed refusal sibling effect"),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
        ]],
        max_attempts: 4,
    };
    let mut values = Map::new();
    values.insert("items".into(), json!([1, 2]));
    values.insert("label".into(), Value::String(label.into()));
    let initial = new_execution(
        ExecutionId::new("pure-ready-execution").expect("fixed refusal execution"),
        TurnId::new("pure-ready-turn").expect("fixed refusal turn"),
        owner,
        definition,
        values,
        TransitionFacts { now_ms: 100 },
    )
    .expect("public refusal new_execution must succeed");
    validate_snapshot(&initial).expect("strict public refusal initial validation");
    let started = transition(
        &initial,
        KernelCommand::Start,
        TransitionFacts { now_ms: 101 },
    )
    .expect("public refusal Start must succeed")
    .snapshot;
    validate_snapshot(&started).expect("strict public refusal started validation");
    assert_eq!(started.revision, 1);
    assert_eq!(started.status, ExecutionStatus::Running);
    assert_eq!(started.tasks.len(), 2);
    assert!(started.tasks.iter().all(|task| {
        task.status == TaskStatus::Ready
            && !task.approved
            && task.interrupt.is_none()
            && task.attempt_id.is_none()
            && task.result.is_none()
            && task.failure.is_none()
    }));
    started
}

fn refusal_ordinary_marker(selected: bool) -> InterruptId {
    InterruptId::new(if selected {
        "selected-human-interrupt"
    } else {
        "sibling-human-interrupt"
    })
    .expect("fixed ordinary refusal marker")
}

fn refusal_park(before: &Snapshot, selected: bool, marker: InterruptId, now_ms: u64) -> Snapshot {
    assert!(matches!(now_ms, 102 | 103));
    assert_eq!(before.tasks.len(), 2);
    validate_snapshot(before).expect("strict public refusal Park source");
    let index = if selected { 0 } else { 1 };
    let after = transition(
        before,
        KernelCommand::Park {
            task_id: before.tasks[index].spec.id.clone(),
            interrupt_id: marker,
            prompt: json!({"approval": "sibling"}),
        },
        TransitionFacts { now_ms },
    )
    .expect("public refusal Park branch must succeed")
    .snapshot;
    validate_snapshot(&after).expect("strict actual public refusal Park result");
    after
}

fn refusal_mixed(owner: u64, label: &str) -> Snapshot {
    let started = refusal_started(owner, label);
    let mixed = refusal_park(&started, false, refusal_ordinary_marker(false), 102);
    assert_eq!(mixed.status, ExecutionStatus::Running);
    assert_eq!(mixed.revision, 2);
    assert_eq!(mixed.updated_at_ms, 102);
    assert_eq!(mixed.tasks[0].status, TaskStatus::Ready);
    assert_eq!(mixed.tasks[1].status, TaskStatus::WaitingForApproval);
    mixed
}

fn refusal_capture(before: &Snapshot, use_id: &PermissionUseId) -> PermissionAuthorizationCommand {
    validate_snapshot(before).expect("strict actual or explicit restored capture source");
    assert_eq!(before.tasks.len(), 2);
    let selected = &before.tasks[0];
    assert_eq!(selected.spec.id.as_str(), "selected");
    let command =
        PermissionAuthorizationCommand::capture(before, selected.spec.id.clone(), use_id.clone())
            .expect("actual eligible refusal-source capture must succeed");
    assert_eq!(command.execution_id(), &before.execution_id);
    assert_eq!(command.expected_revision(), before.revision);
    assert_eq!(command.owner_generation(), before.owner_generation);
    assert_eq!(command.turn_id(), &before.turn_id);
    assert_eq!(command.definition(), &before.definition);
    assert_eq!(command.batch_index(), before.batch_index);
    assert_eq!(command.task_spec(), &selected.spec);
    assert_eq!(command.input(), &selected.input);
    assert_eq!(command.use_id(), use_id);
    command
}

fn assert_refusal_common_bindings(
    command: &PermissionAuthorizationCommand,
    before: &Snapshot,
    use_id: &PermissionUseId,
) {
    assert_eq!(before.tasks.len(), 2);
    assert_eq!(command.execution_id(), &before.execution_id);
    assert_eq!(command.turn_id(), &before.turn_id);
    assert_eq!(command.definition(), &before.definition);
    assert_eq!(command.batch_index(), before.batch_index);
    assert_eq!(command.task_spec(), &before.tasks[0].spec);
    assert_eq!(command.use_id(), use_id);
}

#[test]
fn permission_authorization_rejects_backwards_clock() {
    let before = refusal_mixed(1, "ready");
    let use_id = refusal_use_id();
    let command = refusal_capture(&before, &use_id);
    assert!(101 < before.updated_at_ms);
    let unchanged = before.clone();
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 101 });
    assert!(
        matches!(result.as_ref(), Err(KernelError::ClockMovedBackwards)),
        "pure Ready backwards-clock refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_clock_precedes_stale_binding() {
    let source = refusal_mixed(1, "ready");
    let use_id = refusal_use_id();
    let command = refusal_capture(&source, &use_id);
    let before = refusal_park(&source, true, refusal_ordinary_marker(true), 103);
    assert_refusal_common_bindings(&command, &before, &use_id);
    assert_eq!(before.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(before.revision, 3);
    assert_ne!(command.expected_revision(), before.revision);
    assert_eq!(command.owner_generation(), before.owner_generation);
    assert_eq!(command.input(), &before.tasks[0].input);
    assert_eq!(before.updated_at_ms, 103);
    assert!(101 < before.updated_at_ms);
    let unchanged = before.clone();
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 101 });
    assert!(
        matches!(result.as_ref(), Err(KernelError::ClockMovedBackwards)),
        "pure Ready clock-before-stale refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_rejects_stale_revision() {
    let source = refusal_mixed(1, "ready");
    let use_id = refusal_use_id();
    let command = refusal_capture(&source, &use_id);
    let before = refusal_park(&source, true, refusal_ordinary_marker(true), 103);
    assert_refusal_common_bindings(&command, &before, &use_id);
    assert_eq!(before.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(before.revision, 3);
    assert_ne!(command.expected_revision(), before.revision);
    assert_eq!(command.owner_generation(), before.owner_generation);
    assert_eq!(command.input(), &before.tasks[0].input);
    assert!(104 >= before.updated_at_ms);
    let unchanged = before.clone();
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 104 });
    assert!(
        matches!(
            result.as_ref(),
            Err(KernelError::InvalidTransition(
                "permission_authorization_stale"
            ))
        ),
        "pure Ready stale-revision refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_rejects_stale_owner() {
    let source = refusal_mixed(1, "ready");
    let before = refusal_mixed(2, "ready");
    let use_id = refusal_use_id();
    let command = refusal_capture(&source, &use_id);
    assert_refusal_common_bindings(&command, &before, &use_id);
    assert_eq!(command.expected_revision(), before.revision);
    assert_ne!(command.owner_generation(), before.owner_generation);
    assert_eq!(command.input(), &before.tasks[0].input);
    assert_eq!(source.owner_generation, 1);
    assert_eq!(before.owner_generation, 2);
    assert_eq!(source.updated_at_ms, before.updated_at_ms);
    let unchanged = before.clone();
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 103 });
    assert!(
        matches!(
            result.as_ref(),
            Err(KernelError::InvalidTransition(
                "permission_authorization_stale"
            ))
        ),
        "pure Ready stale-owner refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_rejects_stale_input() {
    let source = refusal_mixed(1, "ready");
    let before = refusal_mixed(1, "changed");
    let use_id = refusal_use_id();
    let command = refusal_capture(&source, &use_id);
    assert_refusal_common_bindings(&command, &before, &use_id);
    assert_eq!(command.expected_revision(), before.revision);
    assert_eq!(command.owner_generation(), before.owner_generation);
    assert_ne!(command.input(), &before.tasks[0].input);
    assert_eq!(source.updated_at_ms, before.updated_at_ms);
    assert_eq!(before.tasks[0].input, before.values);
    let unchanged = before.clone();
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 103 });
    assert!(
        matches!(
            result.as_ref(),
            Err(KernelError::InvalidTransition(
                "permission_authorization_stale"
            ))
        ),
        "pure Ready stale-input refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_rejects_selected_waiting() {
    let started = refusal_started(1, "ready");
    let source = refusal_park(&started, false, refusal_ordinary_marker(false), 102);
    let before = refusal_park(&started, true, refusal_ordinary_marker(true), 102);
    let use_id = refusal_use_id();
    let command = refusal_capture(&source, &use_id);
    assert_refusal_common_bindings(&command, &before, &use_id);
    assert_eq!(command.expected_revision(), before.revision);
    assert_eq!(command.owner_generation(), before.owner_generation);
    assert_eq!(command.input(), &before.tasks[0].input);
    assert_eq!(source.status, ExecutionStatus::Running);
    assert_eq!(before.status, ExecutionStatus::Running);
    assert_eq!(source.revision, 2);
    assert_eq!(before.revision, 2);
    assert_eq!(source.tasks[0].status, TaskStatus::Ready);
    assert_eq!(before.tasks[0].status, TaskStatus::WaitingForApproval);
    assert_eq!(before.tasks[1].status, TaskStatus::Ready);
    assert_eq!(source.updated_at_ms, before.updated_at_ms);
    let unchanged = before.clone();
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 103 });
    assert!(
        matches!(
            result.as_ref(),
            Err(KernelError::InvalidTransition(
                "permission_task_not_unapproved_ready"
            ))
        ),
        "pure Ready selected-Waiting refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_rejects_fresh_marker_collision() {
    let started = refusal_started(1, "ready");
    let use_id = refusal_use_id();
    let ordinary = refusal_ordinary_marker(false);
    assert_ne!(ordinary.as_str(), use_id.as_str());
    let source = refusal_park(&started, false, ordinary, 102);
    let marker = InterruptId::new(use_id.as_str()).expect("checked69 ASCII collision marker");
    let before = refusal_park(&started, false, marker, 102);
    let command = refusal_capture(&source, &use_id);
    assert_refusal_common_bindings(&command, &before, &use_id);
    assert_eq!(command.expected_revision(), before.revision);
    assert_eq!(command.owner_generation(), before.owner_generation);
    assert_eq!(command.input(), &before.tasks[0].input);
    assert_eq!(source.status, ExecutionStatus::Running);
    assert_eq!(before.status, ExecutionStatus::Running);
    assert_eq!(source.revision, 2);
    assert_eq!(before.revision, 2);
    assert_eq!(before.tasks[0].status, TaskStatus::Ready);
    assert!(!before.tasks[0].approved);
    assert!(before.tasks[0].interrupt.is_none());
    assert_eq!(
        before.tasks[1]
            .interrupt
            .as_ref()
            .expect("real public Park marker")
            .id
            .as_str(),
        use_id.as_str()
    );
    assert_eq!(source.updated_at_ms, before.updated_at_ms);
    let unchanged = before.clone();
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 103 });
    assert!(
        matches!(result.as_ref(), Err(KernelError::IdCollision)),
        "pure Ready fresh-marker collision refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_invalid_snapshot_precedes_clock() {
    let genuine = refusal_mixed(1, "ready");
    let use_id = refusal_use_id();
    let command = refusal_capture(&genuine, &use_id);
    // This cloned DTO is deliberately malformed; the public source remains valid.
    let mut before = genuine.clone();
    before.schema_version = 0;
    assert_eq!(
        validate_snapshot(&before),
        Err(KernelError::InvalidSnapshot(
            "unsupported_snapshot_version".into()
        ))
    );
    assert!(101 < before.updated_at_ms);
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 101 });
    assert!(
        matches!(result.as_ref(), Err(KernelError::InvalidSnapshot(reason)) if reason.as_str() == "unsupported_snapshot_version"),
        "pure Ready invalid-Snapshot-before-clock refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    let mut unchanged = genuine;
    unchanged.schema_version = 0;
    assert_eq!(before, unchanged);
}

#[test]
fn permission_authorization_rejects_revision_overflow() {
    let genuine = refusal_mixed(1, "ready");
    // This restored counter is explicit pure data, not a public MAX transition.
    let mut before = genuine.clone();
    before.revision = u64::MAX;
    validate_snapshot(&before).expect("strict restored MAX counter must remain valid");
    let use_id = refusal_use_id();
    let command = refusal_capture(&before, &use_id);
    assert_eq!(command.expected_revision(), u64::MAX);
    assert!(before.revision.checked_add(1).is_none());
    let result = authorize_ready(&before, &command, TransitionFacts { now_ms: 103 });
    assert!(
        matches!(result.as_ref(), Err(KernelError::RevisionOverflow)),
        "pure Ready revision-overflow refusal: {}",
        result.as_ref().err().map_or("ok", kernel_error_code)
    );
    let mut unchanged = genuine;
    unchanged.revision = u64::MAX;
    assert_eq!(before, unchanged);
}
