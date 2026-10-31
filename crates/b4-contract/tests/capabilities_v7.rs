use b4_contract::{
    CapabilityApprovalMode, CapabilityDescriptor, CapabilityDescriptorV2, CapabilityDescriptorV3,
    CapabilityDescriptorV4, CapabilityDescriptorV5, CapabilityDescriptorV6, CapabilityDescriptorV7,
    CapabilityFeaturesV7,
};
use serde_json::{json, Value};

fn base_wire_v6() -> Value {
    serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v6-permission-ready-cancellation.json"
    ))
    .unwrap()
}

// Independently derive the two changes to the frozen V6 contract. This expected
// value deliberately depends on neither a V7 fixture nor the V7 constructor.
fn v7_wire() -> Value {
    let mut wire = base_wire_v6();
    wire["descriptor_version"] = json!(7);
    wire["features"]["durable_permission_withheld_cancellation"] = json!(true);
    wire
}

fn rejects(wire: Value) {
    assert!(
        serde_json::from_value::<CapabilityDescriptorV7>(wire.clone()).is_err(),
        "invalid V7 descriptor accepted: {wire}"
    );
}

#[test]
fn v7_round_trips_only_withheld_ready_and_entered_permission_cancellation() {
    let expected = v7_wire();
    let descriptor: CapabilityDescriptorV7 = serde_json::from_value(expected.clone()).unwrap();
    descriptor.validate().unwrap();
    let features: CapabilityFeaturesV7 = descriptor.features.clone();
    assert!(features.durable_permission_withheld_cancellation);
    assert!(features.durable_permission_ready_cancellation);
    assert!(features.durable_permission_entered_cancellation);
    assert!(features.durable_permission_creation);
    assert!(features.durable_permission_reuse);
    assert!(!features.fenced_cancellation);
    assert!(!features.always_permissions);
    assert!(!features.trusted_known_id_recovery);
    assert!(!features.fenced_deletion);
    assert!(!features.automatic_recovery_scan);
    let mut unchanged_features = serde_json::to_value(features).unwrap();
    unchanged_features
        .as_object_mut()
        .unwrap()
        .remove("durable_permission_withheld_cancellation");
    assert_eq!(unchanged_features, base_wire_v6()["features"]);
    assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
}

#[test]
fn opt_in_preserves_every_valid_v6_field_and_the_original_base() {
    // These are the inherited inclusive bounds in CapabilityDescriptor::validate;
    // the V3+ mechanism keeps the three concurrency limits fixed at one.
    for (heartbeat, ttl, viewers, label_len) in [(1, 1, 1, 1), (60000, 86400000, 64, 256)] {
        let mut base_wire = base_wire_v6();
        base_wire["runtime_version"] = json!("r".repeat(label_len));
        base_wire["application"]["id"] = json!("a".repeat(label_len));
        base_wire["application"]["revision"] = json!("v".repeat(label_len));
        base_wire["limits"]["heartbeat_ms"] = json!(heartbeat);
        base_wire["limits"]["approval_ttl_ms"] = json!(ttl);
        base_wire["limits"]["max_attached_viewers"] = json!(viewers);
        let base: CapabilityDescriptorV6 = serde_json::from_value(base_wire.clone()).unwrap();
        base.validate().unwrap();
        let descriptor =
            CapabilityDescriptorV7::permission_withheld_cancellation(base.clone()).unwrap();
        descriptor.validate().unwrap();
        let mut expected = base_wire.clone();
        expected["descriptor_version"] = json!(7);
        expected["features"]["durable_permission_withheld_cancellation"] = json!(true);
        assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
        assert_eq!(serde_json::to_value(base).unwrap(), base_wire);
    }
}

#[test]
fn v7_wire_is_closed_required_typed_and_rejects_duplicate_declarations() {
    for field in [
        "durable_permission_withheld_cancellation",
        "durable_permission_ready_cancellation",
        "durable_permission_entered_cancellation",
    ] {
        for invalid in [json!(false), json!("true"), json!(1), Value::Null] {
            let mut wire = v7_wire();
            wire["features"][field] = invalid;
            rejects(wire);
        }
        let mut wire = v7_wire();
        wire["features"].as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<CapabilityFeaturesV7>(wire["features"].clone()).is_err());
        rejects(wire);
    }
    for invalid in [json!(6), json!(8), json!("7"), Value::Null] {
        let mut wire = v7_wire();
        wire["descriptor_version"] = invalid;
        rejects(wire);
    }
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
        let mut wire = v7_wire();
        wire.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("private_inventory".into(), json!([]));
        rejects(wire);
    }
    // Withheld Work is narrow; no general pre-entry capability is declared.
    let mut broader = v7_wire();
    broader["features"]["durable_permission_pre_entry_cancellation"] = json!(true);
    rejects(broader);
    for (parent, field) in [
        ("", "descriptor_version"),
        ("", "application"),
        ("/execution", "approval_service_mode"),
        ("/features", "durable_permission_creation"),
        ("/store", "durable_approvals"),
        ("/readiness", "checks"),
        ("/limits", "approval_ttl_ms"),
        ("/controls", "callback_timeout_ms"),
        ("/controls", "shutdown_max_wait_ms"),
    ] {
        let mut wire = v7_wire();
        wire.pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        rejects(wire);
    }
    let source = serde_json::to_string(&v7_wire()).unwrap();
    for declaration in [
        "\"descriptor_version\":7",
        "\"durable_permission_withheld_cancellation\":true",
        "\"durable_permission_ready_cancellation\":true",
        "\"durable_permission_entered_cancellation\":true",
        "\"callback_timeout_ms\":null",
    ] {
        let duplicate = source.replace(declaration, &format!("{declaration},{declaration}"));
        assert_ne!(source, duplicate);
        assert!(serde_json::from_str::<CapabilityDescriptorV7>(&duplicate).is_err());
    }
}

#[test]
fn v7_retains_v6_validation_of_features_limits_readiness_and_controls() {
    for (pointer, invalid) in [
        ("/features/fenced_cancellation", json!(true)),
        ("/features/always_permissions", json!(true)),
        ("/features/trusted_known_id_recovery", json!(true)),
        ("/features/fenced_deletion", json!(true)),
        ("/features/automatic_recovery_scan", json!(true)),
        ("/features/durable_permission_creation", json!(false)),
        ("/features/durable_permission_reuse", json!(false)),
        ("/features/required_once_deny", json!(false)),
        ("/features/supervised_admission_drain", json!(false)),
        ("/application", Value::Null),
        ("/application/id", json!("")),
        ("/application/revision", json!("r".repeat(257))),
        ("/runtime_version", json!("r".repeat(257))),
        ("/runtime_version", json!("bad\nlabel")),
        ("/execution/native_callbacks", json!(false)),
        (
            "/execution/approval_service_mode",
            json!("volatile_fixture"),
        ),
        ("/store/durable_runtime", json!(false)),
        ("/store/durable_approvals", json!(false)),
        ("/limits/active_runs", json!(2)),
        ("/limits/concurrent_tasks", json!(2)),
        ("/limits/concurrent_tasks_per_execution", json!(2)),
        ("/limits/body_bytes", json!(65535)),
        ("/limits/heartbeat_ms", json!(0)),
        ("/limits/heartbeat_ms", json!(60001)),
        ("/limits/approval_ttl_ms", json!(0)),
        ("/limits/approval_ttl_ms", json!(86400001)),
        ("/limits/max_attached_viewers", json!(0)),
        ("/limits/max_attached_viewers", json!(65)),
        ("/controls/callback_timeout_ms", json!(100)),
        ("/controls/shutdown_default_wait_ms", json!(30001)),
        ("/controls/shutdown_max_wait_ms", json!(300001)),
        (
            "/readiness/checks",
            json!(["store_readiness", "owned_worker_fault"]),
        ),
        (
            "/readiness/aggregation",
            json!("store_readiness_and_no_owned_worker_fault"),
        ),
    ] {
        let mut base_wire = base_wire_v6();
        *base_wire.pointer_mut(pointer).unwrap() = invalid.clone();
        assert!(serde_json::from_value::<CapabilityDescriptorV6>(base_wire).is_err());
        let mut wire = v7_wire();
        *wire.pointer_mut(pointer).unwrap() = invalid;
        rejects(wire);
    }
}

#[test]
fn factory_and_public_validation_reject_invalid_assemblies_without_repair() {
    let good: CapabilityDescriptorV6 = serde_json::from_value(base_wire_v6()).unwrap();
    let invalid_bases: [fn(&mut CapabilityDescriptorV6); 10] = [
        |v| v.descriptor_version = 7,
        |v| v.features.durable_permission_ready_cancellation = false,
        |v| v.features.durable_permission_entered_cancellation = false,
        |v| v.features.durable_permission_creation = false,
        |v| v.features.fenced_cancellation = true,
        |v| v.application = None,
        |v| v.limits.active_runs = 2,
        |v| v.controls.shutdown_max_wait_ms = 0,
        |v| v.readiness.checks.swap(0, 2),
        |v| v.execution.approval_service_mode = CapabilityApprovalMode::VolatileFixture,
    ];
    for invalidate in invalid_bases {
        let mut bad = good.clone();
        invalidate(&mut bad);
        assert!(bad.validate().is_err());
        assert!(CapabilityDescriptorV7::permission_withheld_cancellation(bad).is_err());
    }
    let good: CapabilityDescriptorV7 = serde_json::from_value(v7_wire()).unwrap();
    let invalid_assemblies: [fn(&mut CapabilityDescriptorV7); 16] = [
        |v| v.descriptor_version = 6,
        |v| v.features.durable_permission_withheld_cancellation = false,
        |v| v.features.durable_permission_ready_cancellation = false,
        |v| v.features.durable_permission_entered_cancellation = false,
        |v| v.features.fenced_cancellation = true,
        |v| v.features.always_permissions = true,
        |v| v.features.trusted_known_id_recovery = true,
        |v| v.features.fenced_deletion = true,
        |v| v.features.automatic_recovery_scan = true,
        |v| v.features.durable_permission_reuse = false,
        |v| v.runtime_version = "r".repeat(257),
        |v| v.application = None,
        |v| v.limits.concurrent_tasks = 2,
        |v| v.controls.shutdown_max_wait_ms = 0,
        |v| v.readiness.checks.swap(0, 2),
        |v| v.execution.approval_service_mode = CapabilityApprovalMode::VolatileFixture,
    ];
    for invalidate in invalid_assemblies {
        let mut bad = good.clone();
        invalidate(&mut bad);
        assert!(bad.validate().is_err());
    }
}

#[test]
fn v1_through_v6_fixture_bytes_and_round_trips_stay_frozen_and_reject_v7() {
    macro_rules! frozen {
        ($ty:ty, $source:expr, $length:expr, $fingerprint:expr) => {{
            let source = $source;
            // Fixed FNV-1a byte fingerprints also catch formatting-only changes.
            let fingerprint = source
                .as_bytes()
                .iter()
                .fold(0xcbf29ce484222325u64, |h, b| {
                    (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
                });
            assert_eq!(source.len(), $length);
            assert_eq!(fingerprint, $fingerprint);
            let expected: Value = serde_json::from_str(source).unwrap();
            let descriptor: $ty = serde_json::from_str(source).unwrap();
            descriptor.validate().unwrap();
            assert!(expected["features"]
                .get("durable_permission_withheld_cancellation")
                .is_none());
            assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
            assert!(serde_json::from_value::<$ty>(v7_wire()).is_err());
            rejects(expected);
        }};
    }
    frozen!(
        CapabilityDescriptor,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-native.json"),
        2240,
        0xb0944782311d5345u64
    );
    frozen!(
        CapabilityDescriptor,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-scripted.json"),
        2174,
        0x39ffbd59ee945319u64
    );
    frozen!(
        CapabilityDescriptorV2,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-v2-native.json"),
        2404,
        0x3a8530ddfb1e8d3fu64
    );
    frozen!(
        CapabilityDescriptorV2,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-v2-scripted.json"),
        2338,
        0xf45fc0fbe350ddc3u64
    );
    frozen!(
        CapabilityDescriptorV3,
        include_str!(
            "../../../contracts/agent-protocol/examples/capabilities-v3-permission-use.json"
        ),
        2445,
        0x56293857525ea7acu64
    );
    frozen!(
        CapabilityDescriptorV4,
        include_str!(
            "../../../contracts/agent-protocol/examples/capabilities-v4-permission-approvals.json"
        ),
        2486,
        0xb9d35f10ee90f5e2u64
    );
    frozen!(CapabilityDescriptorV5, include_str!("../../../contracts/agent-protocol/examples/capabilities-v5-permission-entered-cancellation.json"), 2539, 0x9abe85979a4a4332u64);
    frozen!(CapabilityDescriptorV6, include_str!("../../../contracts/agent-protocol/examples/capabilities-v6-permission-ready-cancellation.json"), 2590, 0x8aa914113f711c6eu64);
}
