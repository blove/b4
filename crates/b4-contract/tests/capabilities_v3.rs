use b4_contract::*;
use serde_json::{json, Value};

fn native_v2() -> Value {
    serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v2-native.json"
    ))
    .unwrap()
}

fn single_native_v2() -> Value {
    let mut value = native_v2();
    for field in [
        "active_runs",
        "concurrent_tasks",
        "concurrent_tasks_per_execution",
    ] {
        value["limits"][field] = json!(1);
    }
    value
}

// This expected wire value is authored independently of the V3 constructor.
// V1/V2 keep their original native recovery and fencing declarations.
fn permission_reuse() -> Value {
    let mut value = single_native_v2();
    value["descriptor_version"] = json!(3);
    value["features"]["durable_permission_reuse"] = json!(true);
    for field in [
        "trusted_known_id_recovery",
        "fenced_cancellation",
        "fenced_deletion",
    ] {
        value["features"][field] = json!(false);
    }
    value
}

fn rejects(value: Value) {
    assert!(
        serde_json::from_value::<CapabilityDescriptorV3>(value.clone()).is_err(),
        "invalid V3 descriptor was accepted: {value}"
    );
}

#[test]
fn v3_narrow_permission_reuse_wire_round_trips() {
    let expected = permission_reuse();
    let descriptor: CapabilityDescriptorV3 = serde_json::from_value(expected.clone()).unwrap();
    descriptor.validate().unwrap();
    assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
    assert!(serde_json::from_value::<CapabilityDescriptor>(expected.clone()).is_err());
    assert!(serde_json::from_value::<CapabilityDescriptorV2>(expected).is_err());
}

#[test]
fn v3_opt_in_conversion_preserves_base_metadata_and_lowers_unsupported_promises() {
    let mut base = single_native_v2();
    base["application"]["id"] = json!("permission-demo");
    base["application"]["revision"] = json!("revision-2");
    base["runtime_version"] = json!("test-runtime");
    base["limits"]["heartbeat_ms"] = json!(1234);
    base["limits"]["approval_ttl_ms"] = json!(42);
    let descriptor: CapabilityDescriptorV2 = serde_json::from_value(base.clone()).unwrap();
    let converted = CapabilityDescriptorV3::permission_reuse(descriptor).unwrap();
    converted.validate().unwrap();
    let mut expected = permission_reuse();
    for field in ["application", "runtime_version", "limits"] {
        expected[field] = base[field].clone();
    }
    assert_eq!(serde_json::to_value(converted).unwrap(), expected);
}

#[test]
fn v3_requires_durable_native_approval_assembly() {
    for (pointer, invalid) in [
        ("/application", Value::Null),
        ("/execution/native_callbacks", json!(false)),
        ("/execution/approval_service_mode", json!("disabled")),
        (
            "/execution/approval_service_mode",
            json!("volatile_fixture"),
        ),
        ("/execution/mode", json!("distributed")),
        ("/execution/registration", json!("dynamic")),
        ("/store/durable_runtime", json!(false)),
        ("/store/durable_approvals", json!(false)),
        ("/store/durable_approvals", Value::Null),
        ("/limits/approval_ttl_ms", Value::Null),
    ] {
        let mut value = permission_reuse();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects(value);
    }
}

#[test]
fn v3_exact_version_and_feature_promises_are_enforced() {
    for invalid in [
        json!(0),
        json!(1),
        json!(2),
        json!(4),
        json!("3"),
        Value::Null,
    ] {
        let mut value = permission_reuse();
        value["descriptor_version"] = invalid;
        rejects(value);
    }
    for field in [
        "live_attach",
        "required_once_deny",
        "supervised_admission_drain",
        "durable_permission_reuse",
    ] {
        let mut value = permission_reuse();
        value["features"][field] = json!(false);
        rejects(value);
    }
    for field in [
        "trusted_known_id_recovery",
        "fenced_cancellation",
        "fenced_deletion",
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
        let mut value = permission_reuse();
        value["features"][field] = json!(true);
        rejects(value);
    }
}

#[test]
fn v3_is_closed_and_requires_every_field_including_nullable_controls() {
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
        let value = permission_reuse();
        let mut unknown = value.clone();
        unknown
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("private_inventory".into(), json!(["private-route"]));
        rejects(unknown);
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
}

#[test]
fn v3_requires_single_run_and_task_limits_without_clamping() {
    for field in [
        "active_runs",
        "concurrent_tasks",
        "concurrent_tasks_per_execution",
    ] {
        for invalid in [json!(0), json!(2), json!(-1), json!(1.5), Value::Null] {
            let mut value = permission_reuse();
            value["limits"][field] = invalid;
            rejects(value);
        }
        let mut base: CapabilityDescriptorV2 = serde_json::from_value(single_native_v2()).unwrap();
        match field {
            "active_runs" => base.limits.active_runs = 2,
            "concurrent_tasks" => base.limits.concurrent_tasks = 2,
            _ => base.limits.concurrent_tasks_per_execution = 2,
        }
        base.validate().unwrap();
        assert!(CapabilityDescriptorV3::permission_reuse(base).is_err());
    }
}

#[test]
fn v3_preserves_fixed_limits_and_remaining_effective_bounds() {
    for (field, fixed) in [
        ("body_bytes", 65536),
        ("json_depth", 64),
        ("program_routes", 64),
        ("program_tasks", 64),
        ("program_attempts", 64),
        ("completion_attempts", 8),
        ("task_failure_bytes", 1024),
        ("script_chunks", 64),
        ("script_delay_ms", 5000),
        ("approval_bytes", 65536),
    ] {
        for invalid in [0, fixed - 1, fixed + 1] {
            let mut value = permission_reuse();
            value["limits"][field] = json!(invalid);
            rejects(value);
        }
    }
    for (field, maximum) in [
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
            let mut value = permission_reuse();
            value["limits"][field] = invalid;
            rejects(value);
        }
        for valid in [1, maximum] {
            let mut value = permission_reuse();
            value["limits"][field] = json!(valid);
            serde_json::from_value::<CapabilityDescriptorV3>(value).unwrap();
        }
    }
}

#[test]
fn v3_keeps_v2_supervised_readiness_and_controls() {
    for checks in [
        json!(["store_readiness", "owned_worker_fault"]),
        json!(["serving_admission", "owned_worker_fault", "store_readiness"]),
        json!([
            "store_readiness",
            "owned_worker_fault",
            "owned_worker_fault"
        ]),
    ] {
        let mut value = permission_reuse();
        value["readiness"]["checks"] = checks;
        rejects(value);
    }
    for (pointer, invalid) in [
        (
            "/readiness/aggregation",
            json!("store_readiness_and_no_owned_worker_fault"),
        ),
        ("/readiness/response", json!("checks")),
        ("/controls/shutdown_default_wait_ms", json!(30001)),
        ("/controls/shutdown_max_wait_ms", json!(300001)),
    ] {
        let mut value = permission_reuse();
        *value.pointer_mut(pointer).unwrap() = invalid;
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
        let mut value = permission_reuse();
        value["controls"][field] = json!(100);
        rejects(value);
    }
}

#[test]
fn v3_constructor_validates_public_v2_assembly_before_conversion() {
    let good: CapabilityDescriptorV2 = serde_json::from_value(single_native_v2()).unwrap();
    for bad in [
        {
            let mut v = good.clone();
            v.descriptor_version = 3;
            v
        },
        {
            let mut v = good.clone();
            v.controls.shutdown_max_wait_ms = 0;
            v
        },
        {
            let mut v = good.clone();
            v.features.trusted_known_id_recovery = false;
            v
        },
        {
            let mut v = good.clone();
            v.store.durable_runtime = false;
            v
        },
        {
            let mut v = good;
            v.execution.approval_service_mode = CapabilityApprovalMode::VolatileFixture;
            v
        },
    ] {
        assert!(CapabilityDescriptorV3::permission_reuse(bad).is_err());
    }
}

#[test]
fn v3_public_assembly_is_validated_before_publication() {
    let good: CapabilityDescriptorV3 = serde_json::from_value(permission_reuse()).unwrap();
    let mut bad = good.clone();
    bad.features.durable_permission_reuse = false;
    assert!(bad.validate().is_err());
    bad = good.clone();
    bad.features.trusted_known_id_recovery = true;
    assert!(bad.validate().is_err());
    bad = good.clone();
    bad.limits.active_runs = 2;
    assert!(bad.validate().is_err());
    bad = good;
    bad.store.durable_runtime = false;
    assert!(bad.validate().is_err());
}

#[test]
fn v3_retains_identity_bounds_and_refuses_legacy_wire_versions() {
    for pointer in [
        "/application/id",
        "/application/revision",
        "/runtime_version",
    ] {
        for invalid in [
            "".into(),
            "bad\nlabel".into(),
            "bad\u{0085}label".into(),
            "é".repeat(129),
        ] {
            let mut value = permission_reuse();
            *value.pointer_mut(pointer).unwrap() = json!(invalid);
            rejects(value);
        }
        let mut value = permission_reuse();
        *value.pointer_mut(pointer).unwrap() = json!("é".repeat(128));
        serde_json::from_value::<CapabilityDescriptorV3>(value).unwrap();
    }
    for (pointer, invalid) in [
        ("/target_profile", json!("b4-ap-core/1")),
        ("/profile_status", json!("full")),
        ("/core_profile_frozen", json!(true)),
    ] {
        let mut value = permission_reuse();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects(value);
    }
    rejects(native_v2());
    rejects(
        serde_json::from_str(include_str!(
            "../../../contracts/agent-protocol/examples/capabilities-native.json"
        ))
        .unwrap(),
    );
}

#[test]
fn v3_duplicate_feature_fields_are_refused() {
    let source = serde_json::to_string(&permission_reuse()).unwrap();
    let malformed = source.replace(
        "\"durable_permission_reuse\":true",
        "\"durable_permission_reuse\":true,\"durable_permission_reuse\":true",
    );
    assert_ne!(malformed, source);
    assert!(serde_json::from_str::<CapabilityDescriptorV3>(&malformed).is_err());
}
