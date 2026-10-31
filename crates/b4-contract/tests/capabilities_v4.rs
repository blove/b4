use b4_contract::{
    CapabilityApprovalMode, CapabilityDescriptor, CapabilityDescriptorV2, CapabilityDescriptorV3,
    CapabilityDescriptorV4, CapabilityFeaturesV4,
};
use serde_json::{json, Value};

fn permission_reuse_v3() -> Value {
    serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/capabilities-v3-permission-use.json"
    ))
    .unwrap()
}

// Author the expected wire value independently of the V4 constructor. No new
// fixture is needed to demonstrate the missing V4 public API.
fn permission_approvals_v4() -> Value {
    let mut value = permission_reuse_v3();
    value["descriptor_version"] = json!(4);
    value["features"]["durable_permission_creation"] = json!(true);
    value
}

fn rejects(value: Value) {
    assert!(
        serde_json::from_value::<CapabilityDescriptorV4>(value.clone()).is_err(),
        "invalid V4 descriptor was accepted: {value}"
    );
}

#[test]
fn v4_narrow_creation_and_reuse_wire_round_trips() {
    let expected = permission_approvals_v4();
    let descriptor: CapabilityDescriptorV4 = serde_json::from_value(expected.clone()).unwrap();
    descriptor.validate().unwrap();
    let features: CapabilityFeaturesV4 = descriptor.features.clone();
    assert!(features.durable_permission_creation);
    assert!(features.durable_permission_reuse);
    assert!(!features.always_permissions);
    assert!(!features.trusted_known_id_recovery);
    assert!(!features.fenced_cancellation);
    assert!(!features.fenced_deletion);
    assert_eq!(serde_json::to_value(descriptor).unwrap(), expected);
}

#[test]
fn v4_opt_in_preserves_every_base_field_except_version_and_creation_declaration() {
    let mut base_wire = permission_reuse_v3();
    base_wire["application"]["id"] = json!("approval-demo");
    base_wire["application"]["revision"] = json!("approval-revision-4");
    base_wire["runtime_version"] = json!("test-runtime-4");
    base_wire["limits"]["heartbeat_ms"] = json!(1234);
    base_wire["limits"]["approval_ttl_ms"] = json!(42);
    base_wire["limits"]["max_attached_viewers"] = json!(7);
    let base: CapabilityDescriptorV3 = serde_json::from_value(base_wire.clone()).unwrap();
    let converted = CapabilityDescriptorV4::permission_approvals(base.clone()).unwrap();
    converted.validate().unwrap();
    let mut expected = base_wire.clone();
    expected["descriptor_version"] = json!(4);
    expected["features"]["durable_permission_creation"] = json!(true);
    assert_eq!(serde_json::to_value(converted).unwrap(), expected);
    assert_eq!(serde_json::to_value(base).unwrap(), base_wire);
}

#[test]
fn v3_remains_version_three_without_a_creation_field_and_legacy_types_reject_v4() {
    let wire = permission_reuse_v3();
    let descriptor: CapabilityDescriptorV3 = serde_json::from_value(wire.clone()).unwrap();
    descriptor.validate().unwrap();
    assert_eq!(serde_json::to_value(descriptor).unwrap(), wire);
    assert_eq!(wire["descriptor_version"], json!(3));
    assert!(wire["features"]
        .get("durable_permission_creation")
        .is_none());
    let v4 = permission_approvals_v4();
    assert!(serde_json::from_value::<CapabilityDescriptor>(v4.clone()).is_err());
    assert!(serde_json::from_value::<CapabilityDescriptorV2>(v4.clone()).is_err());
    assert!(serde_json::from_value::<CapabilityDescriptorV3>(v4).is_err());
    rejects(wire);
}

#[test]
fn v4_requires_exact_version_and_typed_creation_and_reuse_promises() {
    for invalid in [json!(0), json!(3), json!(5), json!("4"), Value::Null] {
        let mut value = permission_approvals_v4();
        value["descriptor_version"] = invalid;
        rejects(value);
    }
    for field in ["durable_permission_creation", "durable_permission_reuse"] {
        for invalid in [json!(false), json!("true"), Value::Null] {
            let mut value = permission_approvals_v4();
            value["features"][field] = invalid;
            rejects(value);
        }
        let mut value = permission_approvals_v4();
        value["features"].as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<CapabilityFeaturesV4>(value["features"].clone()).is_err());
        rejects(value);
    }
}

#[test]
fn v4_is_closed_and_does_not_default_required_or_nullable_fields() {
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
        let mut value = permission_approvals_v4();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("private_inventory".into(), json!(["private-route"]));
        rejects(value);
    }
    for (parent, field) in [
        ("", "descriptor_version"),
        ("", "application"),
        ("/execution", "approval_service_mode"),
        ("/features", "required_once_deny"),
        ("/features", "supervised_admission_drain"),
        ("/store", "durable_approvals"),
        ("/limits", "approval_ttl_ms"),
        ("/controls", "callback_timeout_ms"),
        ("/controls", "shutdown_max_wait_ms"),
    ] {
        let mut value = permission_approvals_v4();
        value
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        rejects(value);
    }
    let mut features = permission_approvals_v4()["features"].clone();
    features["unexpected_creation_mode"] = json!(true);
    assert!(serde_json::from_value::<CapabilityFeaturesV4>(features).is_err());
}

#[test]
fn v4_rejects_duplicate_creation_declarations() {
    let source = serde_json::to_string(&permission_approvals_v4()).unwrap();
    let duplicate = source.replace(
        "\"durable_permission_creation\":true",
        "\"durable_permission_creation\":true,\"durable_permission_creation\":true",
    );
    assert_ne!(duplicate, source);
    assert!(serde_json::from_str::<CapabilityDescriptorV4>(&duplicate).is_err());
}

#[test]
fn v4_keeps_required_native_durable_approval_posture() {
    for (pointer, invalid) in [
        ("/application", Value::Null),
        ("/application/id", json!("")),
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
        ("/features/required_once_deny", json!(false)),
        ("/features/live_attach", json!(false)),
        ("/limits/approval_ttl_ms", Value::Null),
    ] {
        let mut value = permission_approvals_v4();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects(value);
    }
}

#[test]
fn v4_rejects_broader_permission_recovery_and_fencing_promises() {
    for field in [
        "always_permissions",
        "trusted_known_id_recovery",
        "fenced_cancellation",
        "fenced_deletion",
        "automatic_recovery_scan",
        "durable_event_replay",
        "graceful_shutdown",
        "native_callback_sandbox",
    ] {
        let mut value = permission_approvals_v4();
        value["features"][field] = json!(true);
        rejects(value);
    }
}

#[test]
fn v4_keeps_single_task_limits_and_existing_effective_bounds() {
    for field in [
        "active_runs",
        "concurrent_tasks",
        "concurrent_tasks_per_execution",
    ] {
        for invalid in [json!(0), json!(2), Value::Null] {
            let mut value = permission_approvals_v4();
            value["limits"][field] = invalid;
            rejects(value);
        }
    }
    for (field, invalid) in [
        ("body_bytes", json!(65535)),
        ("heartbeat_ms", json!(0)),
        ("heartbeat_ms", json!(60001)),
        ("approval_ttl_ms", json!(86400001)),
    ] {
        let mut value = permission_approvals_v4();
        value["limits"][field] = invalid;
        rejects(value);
    }
}

#[test]
fn v4_keeps_supervised_drain_readiness_and_control_validation() {
    for (pointer, invalid) in [
        ("/features/supervised_admission_drain", json!(false)),
        (
            "/readiness/checks",
            json!(["store_readiness", "owned_worker_fault"]),
        ),
        (
            "/readiness/checks",
            json!(["serving_admission", "owned_worker_fault", "store_readiness"]),
        ),
        (
            "/readiness/aggregation",
            json!("store_readiness_and_no_owned_worker_fault"),
        ),
        ("/controls/shutdown_default_wait_ms", json!(30001)),
        ("/controls/shutdown_max_wait_ms", json!(300001)),
        ("/controls/callback_timeout_ms", json!(100)),
    ] {
        let mut value = permission_approvals_v4();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects(value);
    }
}

#[test]
fn v4_constructor_rejects_invalid_public_v3_bases_instead_of_repairing_them() {
    let good: CapabilityDescriptorV3 = serde_json::from_value(permission_reuse_v3()).unwrap();
    for bad in [
        {
            let mut v = good.clone();
            v.descriptor_version = 4;
            v
        },
        {
            let mut v = good.clone();
            v.features.durable_permission_reuse = false;
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
        assert!(CapabilityDescriptorV4::permission_approvals(bad).is_err());
    }
}

#[test]
fn v4_public_assembly_is_validated_before_publication() {
    let good: CapabilityDescriptorV4 = serde_json::from_value(permission_approvals_v4()).unwrap();
    for bad in [
        {
            let mut v = good.clone();
            v.descriptor_version = 3;
            v
        },
        {
            let mut v = good.clone();
            v.features.durable_permission_creation = false;
            v
        },
        {
            let mut v = good.clone();
            v.features.durable_permission_reuse = false;
            v
        },
        {
            let mut v = good.clone();
            v.features.required_once_deny = false;
            v
        },
        {
            let mut v = good.clone();
            v.features.supervised_admission_drain = false;
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
            v.features.fenced_cancellation = true;
            v
        },
        {
            let mut v = good.clone();
            v.features.fenced_deletion = true;
            v
        },
        {
            let mut v = good.clone();
            v.application = None;
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
