use b4_contract::CapabilityDescriptor;
use serde_json::{json, Value};

fn native() -> Value {
    serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-native.json"
    ))
    .unwrap()
}
fn rejects(value: Value) {
    assert!(
        serde_json::from_value::<CapabilityDescriptor>(value.clone()).is_err(),
        "accepted {value}"
    );
}

#[test]
fn authored_descriptors_round_trip_without_private_inventory() {
    for source in [
        include_str!("../../../contracts/agent-protocol/examples/capabilities-native.json"),
        include_str!("../../../contracts/agent-protocol/examples/capabilities-scripted.json"),
    ] {
        let value: Value = serde_json::from_str(source).unwrap();
        let parsed: CapabilityDescriptor = serde_json::from_str(source).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), value);
        assert!(source.len() < 65536);
        for key in [
            "routes",
            "route_count",
            "effects",
            "prompts",
            "grant",
            "token",
            "path",
            "policy_revision",
        ] {
            assert!(value.get(key).is_none());
        }
    }
}

#[test]
fn unknown_fields_at_every_object_boundary_are_refused() {
    for pointer in [
        "",
        "/application",
        "/execution",
        "/features",
        "/store",
        "/readiness",
        "/limits",
        "/controls",
    ] {
        let mut value = native();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("routes".into(), json!(["private-route"]));
        rejects(value);
    }
}

#[test]
fn effective_limits_are_positive_and_match_constructor_ranges() {
    for (field, max) in [
        ("active_runs", 256),
        ("concurrent_tasks", 256),
        ("concurrent_tasks_per_execution", 64),
        ("viewer_queue_frames", 512),
        ("heartbeat_ms", 60000),
        ("input_bytes", 65536),
        ("output_bytes", 65536),
        ("max_attached_viewers", 64),
        ("turn_buffer_bytes", 8388608),
        ("turn_buffer_frames", 4096),
        ("viewer_queue_bytes", 8388608),
        ("attachment_snapshot_bytes", 8388608),
        ("approval_ttl_ms", 86400000),
    ] {
        for invalid in [json!(0), json!(max + 1), json!(-1), json!(1.5)] {
            let mut value = native();
            value["limits"][field] = invalid;
            rejects(value);
        }
        for valid in [1, max] {
            let mut value = native();
            value["limits"][field] = json!(valid);
            serde_json::from_value::<CapabilityDescriptor>(value).unwrap();
        }
    }
}

#[test]
fn frozen_constants_unsupported_features_and_absent_controls_cannot_drift() {
    for field in [
        "body_bytes",
        "json_depth",
        "program_routes",
        "program_tasks",
        "program_attempts",
        "completion_attempts",
        "task_failure_bytes",
        "script_chunks",
        "script_delay_ms",
        "approval_bytes",
    ] {
        let mut value = native();
        value["limits"][field] = json!(0);
        rejects(value);
    }
    for field in [
        "automatic_recovery_scan",
        "always_permissions",
        "durable_event_replay",
        "native_live_chunks",
        "built_in_model_providers",
        "memory_candidates",
        "managed_workspaces",
        "ag_ui",
        "dynamic_graphs",
        "graceful_shutdown",
        "turn_deadline",
        "native_callback_sandbox",
    ] {
        let mut value = native();
        value["features"][field] = json!(true);
        rejects(value);
    }
    for field in [
        "callback_timeout_ms",
        "validator_timeout_ms",
        "turn_deadline_ms",
        "max_threads",
        "max_history_bytes",
        "parked_retention_ms",
        "connection_limit",
        "whole_process_memory_bytes",
    ] {
        let mut value = native();
        value["controls"][field] = json!(100);
        rejects(value);
    }
    let mut value = native();
    value["descriptor_version"] = json!(2);
    rejects(value);
    let mut value = native();
    value["core_profile_frozen"] = json!(true);
    rejects(value);
    let mut value = native();
    value["readiness"]["checks"] = json!(["owned_worker_fault", "store_readiness"]);
    rejects(value);
    for (pointer, invalid) in [
        ("/target_profile", "b4-ap-core/1"),
        ("/profile_status", "full"),
        ("/execution/mode", "distributed"),
        ("/execution/registration", "dynamic"),
        ("/execution/approval_service_mode", "always"),
        ("/store/kind", "private-backend-path"),
        ("/readiness/response", "checks"),
        ("/readiness/aggregation", "healthy"),
    ] {
        let mut value = native();
        *value.pointer_mut(pointer).unwrap() = json!(invalid);
        rejects(value);
    }
}

#[test]
fn descriptor_identity_labels_are_bounded_in_utf8_bytes() {
    for pointer in [
        "/application/id",
        "/application/revision",
        "/runtime_version",
    ] {
        for invalid in [
            "".to_owned(),
            "bad\nlabel".to_owned(),
            "bad\u{0085}label".to_owned(),
            "x".repeat(257),
            "é".repeat(129),
        ] {
            let mut value = native();
            *value.pointer_mut(pointer).unwrap() = json!(invalid);
            rejects(value);
        }
        let mut value = native();
        *value.pointer_mut(pointer).unwrap() = json!("é".repeat(128));
        serde_json::from_value::<CapabilityDescriptor>(value).unwrap();
    }
}

#[test]
fn assembly_identity_bridge_and_ttl_declarations_are_consistent() {
    let mut value = native();
    value["application"] = Value::Null;
    rejects(value);
    let mut value = native();
    value["execution"]["native_callbacks"] = json!(false);
    rejects(value);
    let mut value = native();
    value["limits"]["approval_ttl_ms"] = Value::Null;
    rejects(value);
    let mut value = native();
    value["features"]["required_once_deny"] = json!(false);
    rejects(value);
    let mut value = native();
    value["features"]["trusted_known_id_recovery"] = json!(false);
    rejects(value);
    let mut value = native();
    value["execution"]["approval_service_mode"] = json!("disabled");
    rejects(value);
    let mut value = native();
    value["store"]["durable_approvals"] = Value::Null;
    rejects(value);
    let mut value = native();
    value["store"]["durable_approvals"] = json!(false);
    rejects(value);
}

#[test]
fn volatile_service_mode_preserves_independent_physical_storage_declaration() {
    for (kind, runtime, approvals) in [
        ("memory", false, false),
        ("sqlite", true, true),
        ("unspecified", false, false),
    ] {
        let mut value = native();
        value["execution"]["approval_service_mode"] = json!("volatile_fixture");
        value["store"] =
            json!({"kind":kind,"durable_runtime":runtime,"durable_approvals":approvals});
        serde_json::from_value::<CapabilityDescriptor>(value).unwrap();
    }
    let mut value = native();
    value["execution"]["approval_service_mode"] = json!("volatile_fixture");
    value["store"]["durable_approvals"] = Value::Null;
    rejects(value);
}

#[test]
fn disabled_ordinary_store_can_report_unknown_approval_storage() {
    let value: Value = serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-scripted.json"
    ))
    .unwrap();
    let parsed: CapabilityDescriptor = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(parsed).unwrap()["store"]["durable_approvals"],
        Value::Null
    );
    let mut invalid = value;
    invalid["limits"]["approval_ttl_ms"] = json!(300000);
    rejects(invalid);
}

#[test]
fn required_nullable_fields_must_be_present() {
    for (parent, field) in [
        ("", "application"),
        ("/store", "durable_approvals"),
        ("/limits", "approval_ttl_ms"),
        ("/controls", "callback_timeout_ms"),
        ("/controls", "validator_timeout_ms"),
        ("/controls", "turn_deadline_ms"),
        ("/controls", "max_threads"),
        ("/controls", "max_history_bytes"),
        ("/controls", "parked_retention_ms"),
        ("/controls", "connection_limit"),
        ("/controls", "whole_process_memory_bytes"),
    ] {
        let mut value: Value = serde_json::from_str(include_str!(
            "../../../contracts/agent-protocol/examples/capabilities-scripted.json"
        ))
        .unwrap();
        value
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        rejects(value);
    }
}
