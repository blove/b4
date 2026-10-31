use b4_contract::{
    CapabilityApprovalMode, CapabilityDescriptor, CapabilityDescriptorV2, CapabilityDescriptorV3,
    CapabilityDescriptorV4, CapabilityDescriptorV5, CapabilityDescriptorV6, CapabilityFeaturesV6,
};
use serde_json::{json, Value};

fn v5_wire() -> Value {
    serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v5-permission-entered-cancellation.json"
    ))
    .unwrap()
}

// Independently derive the two changes to the frozen V5 contract. This expected
// value deliberately depends on neither a V6 fixture nor the V6 constructor.
fn v6_wire() -> Value {
    let mut wire = v5_wire();
    wire["descriptor_version"] = json!(6);
    wire["features"]["durable_permission_ready_cancellation"] = json!(true);
    wire
}

fn rejects(wire: Value) {
    assert!(
        serde_json::from_value::<CapabilityDescriptorV6>(wire.clone()).is_err(),
        "invalid V6 descriptor accepted: {wire}"
    );
}

#[test]
fn v6_round_trips_only_ready_and_entered_permission_cancellation() {
    let expected = v6_wire();
    let descriptor: CapabilityDescriptorV6 = serde_json::from_value(expected.clone()).unwrap();
    descriptor.validate().unwrap();
    let features: CapabilityFeaturesV6 = descriptor.features.clone();
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
        .remove("durable_permission_ready_cancellation");
    assert_eq!(unchanged_features, v5_wire()["features"]);
    assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
}

#[test]
fn opt_in_preserves_every_valid_v5_field_and_the_original_base() {
    for (heartbeat, ttl, viewers) in [(1, 1, 1), (60000, 86400000, 64)] {
        let mut base_wire = v5_wire();
        base_wire["runtime_version"] = json!("test-runtime-v6");
        base_wire["application"]["id"] = json!("ready-demo");
        base_wire["application"]["revision"] = json!("ready-v6");
        base_wire["limits"]["heartbeat_ms"] = json!(heartbeat);
        base_wire["limits"]["approval_ttl_ms"] = json!(ttl);
        base_wire["limits"]["max_attached_viewers"] = json!(viewers);
        let base: CapabilityDescriptorV5 = serde_json::from_value(base_wire.clone()).unwrap();
        base.validate().unwrap();
        let descriptor =
            CapabilityDescriptorV6::permission_ready_cancellation(base.clone()).unwrap();
        descriptor.validate().unwrap();
        let mut expected = base_wire.clone();
        expected["descriptor_version"] = json!(6);
        expected["features"]["durable_permission_ready_cancellation"] = json!(true);
        assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
        assert_eq!(serde_json::to_value(base).unwrap(), base_wire);
    }
}

#[test]
fn v6_wire_is_closed_required_typed_and_rejects_duplicate_declarations() {
    for field in [
        "durable_permission_ready_cancellation",
        "durable_permission_entered_cancellation",
    ] {
        for invalid in [json!(false), json!("true"), json!(1), Value::Null] {
            let mut wire = v6_wire();
            wire["features"][field] = invalid;
            rejects(wire);
        }
        let mut wire = v6_wire();
        wire["features"].as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<CapabilityFeaturesV6>(wire["features"].clone()).is_err());
        rejects(wire);
    }
    for invalid in [json!(5), json!(7), json!("6"), Value::Null] {
        let mut wire = v6_wire();
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
        let mut wire = v6_wire();
        wire.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("private_inventory".into(), json!([]));
        rejects(wire);
    }
    // Ready is an explicit state; no broader pre-entry capability is declared.
    let mut broader = v6_wire();
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
        let mut wire = v6_wire();
        wire.pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        rejects(wire);
    }
    let source = serde_json::to_string(&v6_wire()).unwrap();
    for declaration in [
        "\"descriptor_version\":6",
        "\"durable_permission_ready_cancellation\":true",
        "\"durable_permission_entered_cancellation\":true",
        "\"callback_timeout_ms\":null",
    ] {
        let duplicate = source.replace(declaration, &format!("{declaration},{declaration}"));
        assert_ne!(source, duplicate);
        assert!(serde_json::from_str::<CapabilityDescriptorV6>(&duplicate).is_err());
    }
}

#[test]
fn v6_retains_v5_validation_of_features_limits_readiness_and_controls() {
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
        let mut base_wire = v5_wire();
        *base_wire.pointer_mut(pointer).unwrap() = invalid.clone();
        assert!(serde_json::from_value::<CapabilityDescriptorV5>(base_wire).is_err());
        let mut wire = v6_wire();
        *wire.pointer_mut(pointer).unwrap() = invalid;
        rejects(wire);
    }
}

#[test]
fn factory_and_public_validation_reject_invalid_assemblies_without_repair() {
    let good: CapabilityDescriptorV5 = serde_json::from_value(v5_wire()).unwrap();
    let invalid_bases: [fn(&mut CapabilityDescriptorV5); 9] = [
        |v| v.descriptor_version = 6,
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
        assert!(CapabilityDescriptorV6::permission_ready_cancellation(bad).is_err());
    }
    let good: CapabilityDescriptorV6 = serde_json::from_value(v6_wire()).unwrap();
    let invalid_assemblies: [fn(&mut CapabilityDescriptorV6); 12] = [
        |v| v.descriptor_version = 5,
        |v| v.features.durable_permission_ready_cancellation = false,
        |v| v.features.durable_permission_entered_cancellation = false,
        |v| v.features.fenced_cancellation = true,
        |v| v.features.always_permissions = true,
        |v| v.features.trusted_known_id_recovery = true,
        |v| v.features.fenced_deletion = true,
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
fn v1_through_v5_fixture_bytes_and_round_trips_stay_frozen_and_reject_v6() {
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
                .get("durable_permission_ready_cancellation")
                .is_none());
            assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
            assert!(serde_json::from_value::<$ty>(v6_wire()).is_err());
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
}
