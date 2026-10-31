use b4_contract::{
    CapabilityApprovalMode, CapabilityDescriptor, CapabilityDescriptorV2, CapabilityDescriptorV3,
    CapabilityDescriptorV4, CapabilityDescriptorV5, CapabilityFeaturesV5,
};
use serde_json::{json, Value};

fn v4_wire() -> Value {
    serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v4-permission-approvals.json"
    ))
    .unwrap()
}

// Independently author the only two changes to the frozen V4 wire contract.
fn v5_wire() -> Value {
    let mut wire = v4_wire();
    wire["descriptor_version"] = json!(5);
    wire["features"]["durable_permission_entered_cancellation"] = json!(true);
    wire
}

fn rejects(wire: Value) {
    assert!(
        serde_json::from_value::<CapabilityDescriptorV5>(wire.clone()).is_err(),
        "invalid V5 descriptor accepted: {wire}"
    );
}

#[test]
fn v5_round_trips_only_the_narrow_entered_permission_cancellation_promise() {
    let expected = v5_wire();
    let descriptor: CapabilityDescriptorV5 = serde_json::from_value(expected.clone()).unwrap();
    descriptor.validate().unwrap();
    let features: CapabilityFeaturesV5 = descriptor.features.clone();
    assert!(features.durable_permission_entered_cancellation);
    assert!(features.durable_permission_creation);
    assert!(features.durable_permission_reuse);
    assert!(!features.fenced_cancellation);
    assert!(!features.always_permissions);
    assert!(!features.trusted_known_id_recovery);
    assert!(!features.fenced_deletion);
    assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
}

#[test]
fn opt_in_preserves_the_entire_valid_v4_assembly_and_original_value() {
    let mut base_wire = v4_wire();
    base_wire["runtime_version"] = json!("test-runtime-v5");
    base_wire["application"]["id"] = json!("entered-demo");
    base_wire["application"]["revision"] = json!("entered-v5");
    base_wire["limits"]["heartbeat_ms"] = json!(1234);
    base_wire["limits"]["approval_ttl_ms"] = json!(42);
    base_wire["limits"]["max_attached_viewers"] = json!(7);
    let base: CapabilityDescriptorV4 = serde_json::from_value(base_wire.clone()).unwrap();
    let descriptor = CapabilityDescriptorV5::permission_entered_cancellation(base.clone()).unwrap();
    descriptor.validate().unwrap();
    let mut expected = base_wire.clone();
    expected["descriptor_version"] = json!(5);
    expected["features"]["durable_permission_entered_cancellation"] = json!(true);
    assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
    assert_eq!(serde_json::to_value(base).unwrap(), base_wire);
}

#[test]
fn v5_is_closed_and_requires_a_typed_narrow_promise() {
    for invalid in [json!(false), json!("true"), Value::Null] {
        let mut wire = v5_wire();
        wire["features"]["durable_permission_entered_cancellation"] = invalid;
        rejects(wire);
    }
    let mut missing = v5_wire();
    missing["features"]
        .as_object_mut()
        .unwrap()
        .remove("durable_permission_entered_cancellation");
    rejects(missing.clone());
    assert!(serde_json::from_value::<CapabilityFeaturesV5>(missing["features"].clone()).is_err());
    for invalid in [json!(4), json!(6), json!("5"), Value::Null] {
        let mut wire = v5_wire();
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
        let mut wire = v5_wire();
        wire.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("private_inventory".into(), json!([]));
        rejects(wire);
    }
    for (pointer, field) in [
        ("/controls", "callback_timeout_ms"),
        ("/limits", "approval_ttl_ms"),
        ("/features", "durable_permission_creation"),
    ] {
        let mut wire = v5_wire();
        wire.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        rejects(wire);
    }
    let source = serde_json::to_string(&v5_wire()).unwrap();
    let duplicate = source.replace(
        "\"durable_permission_entered_cancellation\":true",
        "\"durable_permission_entered_cancellation\":true,\"durable_permission_entered_cancellation\":true",
    );
    assert_ne!(source, duplicate);
    assert!(serde_json::from_str::<CapabilityDescriptorV5>(&duplicate).is_err());
}

#[test]
fn v5_retains_v4_limits_readiness_controls_and_refuses_broader_mechanisms() {
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
        ("/limits/heartbeat_ms", json!(0)),
        ("/limits/approval_ttl_ms", json!(86400001)),
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
        let mut wire = v5_wire();
        *wire.pointer_mut(pointer).unwrap() = invalid;
        rejects(wire);
    }
}

#[test]
fn constructors_and_public_validation_reject_invalid_bases_instead_of_repairing_them() {
    let good: CapabilityDescriptorV4 = serde_json::from_value(v4_wire()).unwrap();
    for bad in [
        {
            let mut v = good.clone();
            v.descriptor_version = 5;
            v
        },
        {
            let mut v = good.clone();
            v.features.durable_permission_creation = false;
            v
        },
        {
            let mut v = good.clone();
            v.features.fenced_cancellation = true;
            v
        },
        {
            let mut v = good.clone();
            v.application = None;
            v
        },
        {
            let mut v = good.clone();
            v.limits.active_runs = 2;
            v
        },
        {
            let mut v = good.clone();
            v.controls.shutdown_max_wait_ms = 0;
            v
        },
        {
            let mut v = good;
            v.execution.approval_service_mode = CapabilityApprovalMode::VolatileFixture;
            v
        },
    ] {
        assert!(CapabilityDescriptorV5::permission_entered_cancellation(bad).is_err());
    }
    let good: CapabilityDescriptorV5 = serde_json::from_value(v5_wire()).unwrap();
    for bad in [
        {
            let mut v = good.clone();
            v.descriptor_version = 4;
            v
        },
        {
            let mut v = good.clone();
            v.features.durable_permission_entered_cancellation = false;
            v
        },
        {
            let mut v = good.clone();
            v.features.fenced_cancellation = true;
            v
        },
        {
            let mut v = good.clone();
            v.features.always_permissions = true;
            v
        },
        {
            let mut v = good.clone();
            v.features.trusted_known_id_recovery = true;
            v
        },
        {
            let mut v = good.clone();
            v.features.fenced_deletion = true;
            v
        },
        {
            let mut v = good.clone();
            v.limits.concurrent_tasks = 2;
            v
        },
        {
            let mut v = good;
            v.controls.shutdown_max_wait_ms = 0;
            v
        },
    ] {
        assert!(bad.validate().is_err());
    }
}

#[test]
fn v1_through_v4_fixture_contracts_stay_frozen_and_reject_v5() {
    macro_rules! frozen {
        ($ty:ty, $source:expr) => {{
            let source = $source;
            let expected: Value = serde_json::from_str(source).unwrap();
            let descriptor: $ty = serde_json::from_str(source).unwrap();
            assert!(expected["features"]
                .get("durable_permission_entered_cancellation")
                .is_none());
            assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
            assert!(serde_json::from_value::<$ty>(v5_wire()).is_err());
        }};
    }
    frozen!(
        CapabilityDescriptor,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-native.json")
    );
    frozen!(
        CapabilityDescriptor,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-scripted.json")
    );
    frozen!(
        CapabilityDescriptorV2,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-v2-native.json")
    );
    frozen!(
        CapabilityDescriptorV2,
        include_str!("../../../contracts/agent-protocol/examples/capabilities-v2-scripted.json")
    );
    frozen!(
        CapabilityDescriptorV3,
        include_str!(
            "../../../contracts/agent-protocol/examples/capabilities-v3-permission-use.json"
        )
    );
    frozen!(
        CapabilityDescriptorV4,
        include_str!(
            "../../../contracts/agent-protocol/examples/capabilities-v4-permission-approvals.json"
        )
    );
    rejects(v4_wire());
}
