use b4_contract::{EffectId, ExecutionStatus, TaskResult};
use b4_testkit::*;
use serde_json::json;
use std::collections::BTreeMap;

const APPROVAL: &str =
    include_str!("../../../conformance/scenarios/parallel_approval_snapshot_round_trip.json");
const UNCERTAIN: &str = include_str!("../../../conformance/scenarios/uncertain_unsafe_effect.json");

#[test]
fn scripted_adapter_uses_exact_effect_fixture_and_counts_invocations() {
    let effect = EffectId::new("effect-a").unwrap();
    let result = TaskResult {
        output: json!({"task": "a"}),
        writes: vec![],
    };
    let mut adapter = ScriptedAdapter::new(BTreeMap::from([(effect.clone(), result.clone())]));
    assert_eq!(adapter.invoke(&effect).unwrap(), result);
    assert_eq!(adapter.invoke(&effect).unwrap(), result);
    assert_eq!(adapter.invocation_counts(), BTreeMap::from([(effect, 2)]));
}

#[test]
fn scripted_adapter_fails_for_an_unlisted_effect_identity() {
    let effect = EffectId::new("effect-a").unwrap();
    let unknown = EffectId::new("effect-a-extra").unwrap();
    let result = TaskResult {
        output: json!("a"),
        writes: vec![],
    };
    let mut adapter = ScriptedAdapter::new(BTreeMap::from([(effect, result)]));
    let error = adapter.invoke(&unknown).unwrap_err();
    assert!(error.contains("missing effect fixture"), "{error}");
    assert_eq!(adapter.invocation_counts().get(&unknown), Some(&1));
}

#[test]
fn approval_scenario_completes_without_replaying_recorded_sibling() {
    let observation = run_scenario_json(APPROVAL).unwrap();
    assert_eq!(observation.status, ExecutionStatus::Completed);
    assert_eq!(observation.effect_counts.values().sum::<u64>(), 2);
    assert_eq!(observation.snapshot_round_trips, 1);
    assert_eq!(observation.discarded_pending_results, 0);
}

#[test]
fn uncertain_unsafe_scenario_discards_result_and_blocks_without_replay() {
    let observation = run_scenario_json(UNCERTAIN).unwrap();
    assert_eq!(observation.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(observation.effect_counts.values().sum::<u64>(), 1);
    assert_eq!(observation.snapshot_round_trips, 1);
    assert_eq!(observation.discarded_pending_results, 1);
}

#[test]
fn unsupported_fixture_version_fails_closed() {
    let mut fixture: serde_json::Value = serde_json::from_str(APPROVAL).unwrap();
    fixture["version"] = json!(2);
    let error = run_scenario_json(&fixture.to_string()).unwrap_err();
    assert!(error.contains("unsupported scenario version"), "{error}");
}

#[test]
fn unknown_command_fails_closed() {
    let mut fixture: serde_json::Value = serde_json::from_str(APPROVAL).unwrap();
    fixture["steps"][0]["type"] = json!("fetch_live_provider");
    let error = run_scenario_json(&fixture.to_string()).unwrap_err();
    assert!(error.contains("unknown variant"), "{error}");
}

#[test]
fn incorrect_expectation_is_a_conformance_failure() {
    let mut fixture: serde_json::Value = serde_json::from_str(APPROVAL).unwrap();
    fixture["expect"]["values"] = json!({"items": ["B", "A"]});
    let error = run_scenario_json(&fixture.to_string()).unwrap_err();
    assert!(error.contains("expectation mismatch: values"), "{error}");
}

#[test]
fn each_reported_expectation_is_checked_independently() {
    for (field, replacement) in [
        ("status", json!("running")),
        ("effect_counts", json!({"effect-A": 2, "effect-B": 1})),
        ("attempts_used", json!(1)),
        ("failure", json!("invented_failure")),
        ("tasks", json!({})),
        ("snapshot_round_trips", json!(0)),
        ("discarded_pending_results", json!(1)),
    ] {
        let mut fixture: serde_json::Value = serde_json::from_str(APPROVAL).unwrap();
        fixture["expect"][field] = replacement;
        let error = run_scenario_json(&fixture.to_string()).unwrap_err();
        assert!(
            error.contains(&format!("expectation mismatch: {field}")),
            "{error}"
        );
    }
}

#[test]
fn completion_after_snapshot_restore_cannot_use_a_transient_result() {
    let mut fixture: serde_json::Value = serde_json::from_str(UNCERTAIN).unwrap();
    fixture["steps"][3] = json!({"type": "complete", "task_id": "A", "now_ms": 103});
    let error = run_scenario_json(&fixture.to_string()).unwrap_err();
    assert!(error.contains("missing pending result"), "{error}");
}

#[test]
fn scenario_execution_fails_when_effect_fixture_is_missing() {
    let mut fixture: serde_json::Value = serde_json::from_str(APPROVAL).unwrap();
    fixture["effects"]
        .as_object_mut()
        .unwrap()
        .remove("effect-A");
    let error = run_scenario_json(&fixture.to_string()).unwrap_err();
    assert!(
        error.contains("missing effect fixture: effect-A"),
        "{error}"
    );
}

#[test]
fn fixture_unknown_fields_are_rejected() {
    let mut fixture: serde_json::Value = serde_json::from_str(APPROVAL).unwrap();
    fixture["allow_network_fallback"] = json!(true);
    let error = run_scenario_json(&fixture.to_string()).unwrap_err();
    assert!(error.contains("unknown field"), "{error}");
}

#[test]
fn scenario_task_count_is_bounded() {
    let mut fixture: serde_json::Value = serde_json::from_str(APPROVAL).unwrap();
    let tasks = fixture["initial"]["definition"]["batches"][0]
        .as_array_mut()
        .unwrap();
    for id in 0..7 {
        tasks.push(json!({"id": format!("extra-{id}"), "effect_id": format!("extra-effect-{id}"), "recovery": "pure", "requires_approval": false}));
    }
    let error = run_scenario_json(&fixture.to_string()).unwrap_err();
    assert!(error.contains("scenario task limit"), "{error}");
}
