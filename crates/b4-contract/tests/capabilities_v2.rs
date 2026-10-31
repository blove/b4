use b4_contract::*;
use serde_json::{json, Value};

fn native() -> Value {
    serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v2-native.json"
    ))
    .unwrap()
}

fn rejects(value: Value) {
    assert!(
        serde_json::from_value::<CapabilityDescriptorV2>(value).is_err(),
        "invalid capability v2 metadata was accepted"
    );
}

#[test]
fn authored_v2_descriptors_round_trip_and_convert_exact_v1_values() {
    for (v1, v2) in [
        (
            include_str!("../../../contracts/agent-protocol/examples/capabilities-native.json"),
            include_str!("../../../contracts/agent-protocol/examples/capabilities-v2-native.json"),
        ),
        (
            include_str!("../../../contracts/agent-protocol/examples/capabilities-scripted.json"),
            include_str!(
                "../../../contracts/agent-protocol/examples/capabilities-v2-scripted.json"
            ),
        ),
    ] {
        let expected: Value = serde_json::from_str(v2).unwrap();
        let parsed: CapabilityDescriptorV2 = serde_json::from_str(v2).unwrap();
        parsed.validate().unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), expected);
        let old: CapabilityDescriptor = serde_json::from_str(v1).unwrap();
        let converted = CapabilityDescriptorV2::try_from(old).unwrap();
        assert_eq!(serde_json::to_value(converted).unwrap(), expected);
        assert!(serde_json::from_str::<CapabilityDescriptor>(v2).is_err());
        assert!(serde_json::from_str::<CapabilityDescriptorV2>(v1).is_err());
        assert!(v2.len() < 65536);
        for key in [
            "routes",
            "effects",
            "prompts",
            "grant",
            "token",
            "policy_revision",
        ] {
            assert!(expected.get(key).is_none());
        }
    }
}

#[test]
fn v2_unknown_fields_at_every_object_boundary_are_refused() {
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
            .insert("private_inventory".into(), json!(["private-route"]));
        rejects(value);
    }
}

#[test]
fn v2_all_wire_fields_are_required_including_nullable_controls() {
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
        let value = native();
        let keys: Vec<_> = value
            .pointer(pointer)
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for key in keys {
            let mut missing = value.clone();
            missing
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(&key);
            rejects(missing);
        }
    }
    let scripted: Value = serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v2-scripted.json"
    ))
    .unwrap();
    for (pointer, key) in [
        ("", "application"),
        ("/store", "durable_approvals"),
        ("/limits", "approval_ttl_ms"),
    ] {
        let mut missing = scripted.clone();
        missing
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        rejects(missing);
    }
}

#[test]
fn v2_version_and_supervised_mechanism_are_exact() {
    for version in [
        json!(0),
        json!(1),
        json!(3),
        Value::Null,
        json!("2"),
        json!(2.5),
    ] {
        let mut value = native();
        value["descriptor_version"] = version;
        rejects(value);
    }
    for field in ["supervised_admission_drain", "live_attach"] {
        for invalid in [json!(false), Value::Null, json!(1), json!("true")] {
            let mut value = native();
            value["features"][field] = invalid;
            rejects(value);
        }
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
}

#[test]
fn v2_readiness_has_three_exact_ordered_checks() {
    for checks in [
        json!([]),
        json!(["store_readiness", "owned_worker_fault"]),
        json!(["serving_admission", "store_readiness", "owned_worker_fault"]),
        json!([
            "store_readiness",
            "owned_worker_fault",
            "owned_worker_fault"
        ]),
        json!([
            "store_readiness",
            "owned_worker_fault",
            "serving_admission",
            "serving_admission"
        ]),
        json!([
            "store_readiness",
            "owned_worker_fault",
            "private_store_details"
        ]),
    ] {
        let mut value = native();
        value["readiness"]["checks"] = checks;
        rejects(value);
    }
    for (key, invalid) in [
        ("aggregation", "store_readiness_and_no_owned_worker_fault"),
        ("aggregation", "healthy"),
        ("response", "checks"),
    ] {
        let mut value = native();
        value["readiness"][key] = json!(invalid);
        rejects(value);
    }
}

#[test]
fn v2_wait_controls_are_fixed_and_old_controls_remain_null() {
    for (key, expected) in [
        ("shutdown_default_wait_ms", 30000),
        ("shutdown_max_wait_ms", 300000),
    ] {
        for invalid in [
            json!(0),
            json!(expected - 1),
            json!(expected + 1),
            json!(-1),
            json!(1.5),
            Value::Null,
        ] {
            let mut value = native();
            value["controls"][key] = invalid;
            rejects(value);
        }
    }
    for key in [
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
        value["controls"][key] = json!(100);
        rejects(value);
    }
}

#[test]
fn v2_inherits_all_fixed_and_effective_v1_limit_bounds() {
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
    for (field, maximum) in [
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
        for invalid in [json!(0), json!(maximum + 1), json!(-1), json!(1.5)] {
            let mut value = native();
            value["limits"][field] = invalid;
            rejects(value);
        }
        for valid in [1, maximum] {
            let mut value = native();
            value["limits"][field] = json!(valid);
            serde_json::from_value::<CapabilityDescriptorV2>(value).unwrap();
        }
    }
}

#[test]
fn v2_inherits_identity_enum_and_utf8_byte_bounds() {
    for pointer in [
        "/application/id",
        "/application/revision",
        "/runtime_version",
    ] {
        for invalid in [
            "".into(),
            "bad\nlabel".into(),
            "bad\u{0085}label".into(),
            "x".repeat(257),
            "é".repeat(129),
        ] {
            let mut value = native();
            *value.pointer_mut(pointer).unwrap() = json!(invalid);
            rejects(value);
        }
        let mut value = native();
        *value.pointer_mut(pointer).unwrap() = json!("é".repeat(128));
        serde_json::from_value::<CapabilityDescriptorV2>(value).unwrap();
    }
    for (pointer, invalid) in [
        ("/target_profile", "b4-ap-core/1"),
        ("/profile_status", "full"),
        ("/execution/mode", "distributed"),
        ("/execution/registration", "dynamic"),
        ("/execution/approval_service_mode", "always"),
        ("/store/kind", "path"),
    ] {
        let mut value = native();
        *value.pointer_mut(pointer).unwrap() = json!(invalid);
        rejects(value);
    }
    let mut value = native();
    value["core_profile_frozen"] = json!(true);
    rejects(value);
}

#[test]
fn v2_inherits_bridge_assembly_and_independent_approval_durability() {
    for (pointer, invalid) in [
        ("/application", Value::Null),
        ("/execution/native_callbacks", json!(false)),
        ("/execution/approval_service_mode", json!("disabled")),
        ("/limits/approval_ttl_ms", Value::Null),
        ("/features/required_once_deny", json!(false)),
        ("/features/trusted_known_id_recovery", json!(false)),
        ("/store/durable_approvals", Value::Null),
        ("/store/durable_approvals", json!(false)),
    ] {
        let mut value = native();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects(value);
    }
    for (kind, durable) in [("memory", false), ("sqlite", true), ("unspecified", false)] {
        let mut value = native();
        value["execution"]["approval_service_mode"] = json!("volatile_fixture");
        value["store"] = json!({"kind":kind,"durable_runtime":durable,"durable_approvals":durable});
        serde_json::from_value::<CapabilityDescriptorV2>(value).unwrap();
    }
    let mut value = native();
    value["execution"]["approval_service_mode"] = json!("volatile_fixture");
    value["store"]["durable_approvals"] = Value::Null;
    rejects(value);
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v2-scripted.json"
    ))
    .unwrap();
    assert!(value["store"]["durable_approvals"].is_null());
    value["limits"]["approval_ttl_ms"] = json!(300000);
    rejects(value);
}

#[test]
fn v2_publicly_assembled_values_and_v1_conversion_are_validated() {
    let good: CapabilityDescriptorV2 = serde_json::from_value(native()).unwrap();
    let mut bad = good.clone();
    bad.descriptor_version = 1;
    assert!(bad.validate().is_err());
    bad = good.clone();
    bad.features.supervised_admission_drain = false;
    assert!(bad.validate().is_err());
    bad = good.clone();
    bad.controls.shutdown_max_wait_ms = 0;
    assert!(bad.validate().is_err());
    bad = good.clone();
    bad.readiness.checks.swap(0, 2);
    assert!(bad.validate().is_err());
    bad = good;
    bad.features.graceful_shutdown = true;
    assert!(bad.validate().is_err());
    let mut old: CapabilityDescriptor = serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-native.json"
    ))
    .unwrap();
    old.limits.active_runs = 0;
    assert!(CapabilityDescriptorV2::try_from(old).is_err());
}

#[test]
fn v2_duplicate_wire_fields_are_refused() {
    let source =
        include_str!("../../../contracts/agent-protocol/examples/capabilities-v2-native.json");
    for (needle, replacement) in [
        (
            "\"descriptor_version\": 2",
            "\"descriptor_version\": 2, \"descriptor_version\": 2",
        ),
        (
            "\"id\": \"native-demo\"",
            "\"id\": \"native-demo\", \"id\": \"native-demo\"",
        ),
        (
            "\"supervised_admission_drain\": true",
            "\"supervised_admission_drain\": true, \"supervised_admission_drain\": true",
        ),
        (
            "\"shutdown_default_wait_ms\": 30000",
            "\"shutdown_default_wait_ms\": 30000, \"shutdown_default_wait_ms\": 30000",
        ),
    ] {
        let malformed = source.replace(needle, replacement);
        assert_ne!(malformed, source);
        assert!(serde_json::from_str::<CapabilityDescriptorV2>(&malformed).is_err());
    }
}
