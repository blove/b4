use b4_contract::{
    ApprovalDecision, AttemptId, CommitPlan, Decision, Definition, EffectId, ExecutionId,
    ExecutionRevision, ExecutionStatus, InterruptId, KernelCommand, PendingApproval,
    RecoveryPolicy, RouteKey, Snapshot, StateWrite, TaskId, TaskRecord, TaskResult, TaskSpec,
    TaskStatus, ThreadId, TransitionFacts, TurnId, SNAPSHOT_VERSION,
};
use serde_json::{json, Map};

fn sample_snapshot() -> Snapshot {
    let spec = TaskSpec {
        id: TaskId::new("task-1").unwrap(),
        effect_id: EffectId::new("effect-1").unwrap(),
        recovery: RecoveryPolicy::Unsafe,
        requires_approval: true,
    };
    Snapshot {
        schema_version: SNAPSHOT_VERSION,
        revision: 2,
        execution_id: ExecutionId::new("execution-1").unwrap(),
        turn_id: TurnId::new("turn-1").unwrap(),
        turn_history: vec![TurnId::new("turn-1").unwrap()],
        attempt_history: vec![],
        owner_generation: 7,
        definition: Definition {
            route: RouteKey::new("/test#agent").unwrap(),
            revision: ExecutionRevision::new("app-v1").unwrap(),
            batches: vec![vec![spec.clone()]],
            max_attempts: 4,
        },
        status: ExecutionStatus::WaitingForApproval,
        batch_index: 0,
        values: Map::from_iter([("messages".into(), json!(["初期"]))]),
        tasks: vec![TaskRecord {
            spec,
            status: TaskStatus::WaitingForApproval,
            input: Map::from_iter([("messages".into(), json!(["初期"]))]),
            attempt_id: None,
            result: None,
            failure: None,
            interrupt: Some(PendingApproval {
                id: InterruptId::new("interrupt-1").unwrap(),
                prompt: json!({"question": "Proceed?\n続行"}),
            }),
            approved: false,
        }],
        attempts_used: 0,
        updated_at_ms: 100,
        output: None,
        failure: None,
    }
}

#[test]
fn typed_ids_validate_constructor_and_deserializer() {
    macro_rules! check_id {
        ($id:ty) => {
            for invalid in ["", "newline\n", "nul\0", "delete\u{7f}", "c1\u{85}"] {
                assert!(<$id>::new(invalid).is_err(), "accepted {invalid:?}");
                assert!(serde_json::from_value::<$id>(json!(invalid)).is_err());
            }
            assert!(<$id>::new("x".repeat(257)).is_err());
            assert!(<$id>::new("é".repeat(129)).is_err());
            let valid = <$id>::new("é".repeat(128)).unwrap();
            assert_eq!(valid.as_str().len(), 256);
            assert_eq!(valid.to_string(), valid.as_str());
            assert_eq!(serde_json::to_value(&valid).unwrap(), json!(valid.as_str()));
            assert_eq!(
                serde_json::from_value::<$id>(json!(valid.as_str())).unwrap(),
                valid
            );
        };
    }
    check_id!(ThreadId);
    check_id!(ExecutionId);
    check_id!(TurnId);
    check_id!(TaskId);
    check_id!(EffectId);
    check_id!(AttemptId);
    check_id!(InterruptId);
    check_id!(ExecutionRevision);
}

#[test]
fn routes_require_a_literal_path_and_registered_kind_suffix() {
    for invalid in [
        "",
        "research#agent",
        "/research",
        "/research#",
        "/research#tool",
        "/a#b#agent",
        "/a\n#agent",
    ] {
        assert!(RouteKey::new(invalid).is_err(), "accepted {invalid:?}");
        assert!(serde_json::from_value::<RouteKey>(json!(invalid)).is_err());
    }
    for valid in [
        "/research#agent",
        "/process#workflow",
        "/#agent",
        "/検索#agent",
    ] {
        let route = RouteKey::new(valid).unwrap();
        assert_eq!(route.as_str(), valid);
        assert_eq!(route.to_string(), valid);
        assert_eq!(serde_json::to_value(&route).unwrap(), json!(valid));
    }
}

#[test]
fn supported_snapshot_round_trips_without_losing_task_identity() {
    let snapshot = sample_snapshot();
    snapshot.validate_version().unwrap();
    let encoded = serde_json::to_string(&snapshot).unwrap();
    assert_eq!(
        serde_json::from_str::<Snapshot>(&encoded).unwrap(),
        snapshot
    );
}

#[test]
fn snapshot_identity_histories_round_trip() {
    let mut encoded = serde_json::to_value(sample_snapshot()).unwrap();
    encoded["turn_history"] = json!(["turn-previous", "turn-1"]);
    encoded["attempt_history"] = json!(["attempt-previous"]);
    encoded["attempts_used"] = json!(1);
    let snapshot: Snapshot = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(serde_json::to_value(snapshot).unwrap(), encoded);
}

#[test]
fn snapshot_identity_histories_are_required_on_load() {
    let encoded = serde_json::to_value(sample_snapshot()).unwrap();
    for field in ["turn_history", "attempt_history"] {
        let mut missing = encoded.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<Snapshot>(missing).is_err(),
            "accepted missing {field}"
        );
    }
}

#[test]
fn snapshot_output_distinguishes_absent_from_explicit_null() {
    let snapshot = sample_snapshot();
    let encoded = serde_json::to_value(&snapshot).unwrap();
    assert!(!encoded.as_object().unwrap().contains_key("output"));
    assert_eq!(
        serde_json::from_value::<Snapshot>(encoded).unwrap().output,
        None
    );
    let mut completed = snapshot;
    completed.status = ExecutionStatus::Completed;
    completed.output = Some(json!(null));
    let encoded = serde_json::to_value(&completed).unwrap();
    assert_eq!(encoded.get("output"), Some(&json!(null)));
    assert_eq!(
        serde_json::from_value::<Snapshot>(encoded).unwrap(),
        completed
    );
}

#[test]
fn unsupported_snapshot_version_fails_closed_on_load() {
    let mut snapshot = sample_snapshot();
    snapshot.schema_version = SNAPSHOT_VERSION + 1;
    let error = snapshot.validate_version().unwrap_err();
    assert_eq!(error.found, 2);
    assert_eq!(error.supported, 1);
    assert!(error.to_string().contains("unsupported snapshot version"));
    let encoded = serde_json::to_value(snapshot).unwrap();
    assert!(serde_json::from_value::<Snapshot>(encoded)
        .unwrap_err()
        .to_string()
        .contains("unsupported snapshot version"));
}

#[test]
fn persisted_structs_reject_unknown_fields() {
    let snapshot = serde_json::to_value(sample_snapshot()).unwrap();
    for pointer in [
        "",
        "/definition",
        "/tasks/0",
        "/tasks/0/spec",
        "/tasks/0/interrupt",
    ] {
        let mut value = snapshot.clone();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("untrusted_control".into(), json!(true));
        assert!(
            serde_json::from_value::<Snapshot>(value).is_err(),
            "accepted unknown field at {pointer}"
        );
    }
}

#[test]
fn failure_commands_and_task_records_round_trip_without_changing_old_snapshots() {
    let command = json!({
        "type":"fail_attempt", "task_id":"task-1", "attempt_id":"attempt-1",
        "failure":"fixture execution failure"
    });
    let parsed = serde_json::from_value::<KernelCommand>(command.clone());
    assert!(parsed.is_ok(), "explicit failure command must be supported");
    assert_eq!(serde_json::to_value(parsed.unwrap()).unwrap(), command);

    let previous = serde_json::to_value(sample_snapshot()).unwrap();
    assert!(!previous["tasks"][0]
        .as_object()
        .unwrap()
        .contains_key("failure"));
    let restored: Snapshot = serde_json::from_value(previous.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), previous);

    let mut failed = previous;
    failed["status"] = json!("failed");
    failed["failure"] = json!("fixture execution failure");
    failed["tasks"][0]["status"] = json!("failed");
    failed["tasks"][0]["attempt_id"] = json!("attempt-1");
    failed["tasks"][0]["failure"] = json!("fixture execution failure");
    let parsed = serde_json::from_value::<Snapshot>(failed.clone());
    assert!(parsed.is_ok(), "known task failure must be restored");
    assert_eq!(serde_json::to_value(parsed.unwrap()).unwrap(), failed);
}

#[test]
fn state_writes_use_explicit_tags_and_preserve_json_values() {
    let replacement = StateWrite::Replace {
        field: "answer".into(),
        value: json!(null),
    };
    let append = StateWrite::Append {
        field: "messages".into(),
        items: vec![json!("line\n雪"), json!(42)],
    };
    assert_eq!(
        serde_json::to_value(&replacement).unwrap(),
        json!({"type": "replace", "field": "answer", "value": null})
    );
    assert_eq!(
        serde_json::to_value(&append).unwrap(),
        json!({"type": "append", "field": "messages", "items": ["line\n雪", 42]})
    );
    assert_eq!(
        serde_json::from_value::<StateWrite>(serde_json::to_value(&append).unwrap()).unwrap(),
        append
    );
    assert!(serde_json::from_value::<StateWrite>(
        json!({"type":"append","field":"messages","items":[],"overwrite":true})
    )
    .is_err());
}

#[test]
fn kernel_commands_and_commit_plans_round_trip() {
    let snapshot = sample_snapshot();
    let task_id = snapshot.tasks[0].spec.id.clone();
    let attempt_id = AttemptId::new("attempt-1").unwrap();
    let interrupt_id = InterruptId::new("interrupt-1").unwrap();
    let result = TaskResult {
        output: json!({"ok": true}),
        writes: vec![StateWrite::Append {
            field: "messages".into(),
            items: vec![json!("done")],
        }],
    };
    let commands = vec![
        KernelCommand::Start,
        KernelCommand::Dispatch {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        },
        KernelCommand::Complete {
            task_id: task_id.clone(),
            attempt_id,
            result,
        },
        KernelCommand::Park {
            task_id: task_id.clone(),
            interrupt_id: interrupt_id.clone(),
            prompt: json!("Proceed?"),
        },
        KernelCommand::Resume {
            route: snapshot.definition.route.clone(),
            decisions: vec![Decision {
                interrupt_id,
                decision: ApprovalDecision::Once,
            }],
            turn_id: TurnId::new("turn-2").unwrap(),
        },
        KernelCommand::Cancel {
            turn_id: snapshot.turn_id.clone(),
            owner_generation: 7,
        },
        KernelCommand::Recover {
            available_revision: snapshot.definition.revision.clone(),
            turn_id: TurnId::new("turn-recovery").unwrap(),
            owner_generation: 8,
        },
    ];
    for command in commands {
        assert_eq!(
            serde_json::from_value::<KernelCommand>(serde_json::to_value(&command).unwrap())
                .unwrap(),
            command
        );
    }
    let plan = CommitPlan {
        expected_revision: 2,
        snapshot,
        dispatchable: vec![task_id],
    };
    assert_eq!(
        serde_json::from_value::<CommitPlan>(serde_json::to_value(&plan).unwrap()).unwrap(),
        plan
    );
    let facts = TransitionFacts { now_ms: 120 };
    assert_eq!(
        serde_json::from_value::<TransitionFacts>(json!({"now_ms":120})).unwrap(),
        facts
    );
}

#[test]
fn thread_generations_require_exact_random_width() {
    use b4_contract::ThreadGeneration;
    let valid = "a7".repeat(32);
    let generation = ThreadGeneration::new(&valid).unwrap();
    assert_eq!(generation.as_str(), valid);
    assert_eq!(
        serde_json::from_value::<ThreadGeneration>(json!(valid)).unwrap(),
        generation
    );
    for invalid in [
        "",
        "a",
        &"0".repeat(63),
        &"0".repeat(65),
        &"A".repeat(64),
        &"g".repeat(64),
    ] {
        assert!(ThreadGeneration::new(invalid).is_err());
        assert!(serde_json::from_value::<ThreadGeneration>(json!(invalid)).is_err());
    }
}
