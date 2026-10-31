use b4_contract::{
    CapabilityApprovalMode, CapabilityDescriptorV7, CapabilityDescriptorV8, CapabilityFeaturesV8,
};
use serde_json::{json, Value};

fn base() -> Value {
    serde_json::from_str(include_str!("../../../contracts/agent-protocol/examples/capabilities-v7-permission-withheld-cancellation.json")).unwrap()
}
fn wire() -> Value {
    let mut value = base();
    value["descriptor_version"] = json!(8);
    value["features"]["durable_permission_settled_thread_deletion"] = json!(true);
    value
}
fn rejects(value: Value) {
    assert!(serde_json::from_value::<CapabilityDescriptorV8>(value).is_err());
}
#[test]
fn v8_inherits_every_v7_field_and_adds_only_the_narrow_deletion_declaration() {
    for (heartbeat, ttl, viewers, length) in [(1, 1, 1, 1), (60000, 86400000, 64, 256)] {
        let mut previous = base();
        previous["runtime_version"] = json!("r".repeat(length));
        previous["application"]["id"] = json!("a".repeat(length));
        previous["application"]["revision"] = json!("v".repeat(length));
        previous["limits"]["heartbeat_ms"] = json!(heartbeat);
        previous["limits"]["approval_ttl_ms"] = json!(ttl);
        previous["limits"]["max_attached_viewers"] = json!(viewers);
        let v7: CapabilityDescriptorV7 = serde_json::from_value(previous.clone()).unwrap();
        let v8 = CapabilityDescriptorV8::permission_settled_thread_deletion(v7.clone()).unwrap();
        v8.validate().unwrap();
        let features: CapabilityFeaturesV8 = v8.features.clone();
        assert!(features.durable_permission_settled_thread_deletion);
        assert!(
            !features.fenced_deletion
                && !features.always_permissions
                && !features.trusted_known_id_recovery
                && !features.automatic_recovery_scan
        );
        let mut expected = previous.clone();
        expected["descriptor_version"] = json!(8);
        expected["features"]["durable_permission_settled_thread_deletion"] = json!(true);
        assert_eq!(serde_json::to_value(v8).unwrap(), expected);
        let parsed: CapabilityDescriptorV8 = serde_json::from_value(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), expected);
        assert_eq!(serde_json::to_value(v7).unwrap(), previous);
        assert!(serde_json::from_value::<CapabilityDescriptorV7>(expected).is_err());
        rejects(previous);
    }
}
#[test]
fn v8_wire_is_closed_required_typed_and_duplicate_free() {
    for flag in [
        "durable_permission_settled_thread_deletion",
        "durable_permission_withheld_cancellation",
        "durable_permission_ready_cancellation",
        "durable_permission_entered_cancellation",
        "durable_permission_creation",
        "durable_permission_reuse",
    ] {
        for invalid in [json!(false), json!("true"), json!(1), Value::Null] {
            let mut value = wire();
            value["features"][flag] = invalid;
            rejects(value);
        }
        let mut value = wire();
        value["features"].as_object_mut().unwrap().remove(flag);
        assert!(serde_json::from_value::<CapabilityFeaturesV8>(value["features"].clone()).is_err());
        rejects(value);
    }
    for invalid in [json!(7), json!(9), json!("8"), Value::Null] {
        let mut value = wire();
        value["descriptor_version"] = invalid;
        rejects(value);
    }
    for parent in [
        "",
        "/features",
        "/application",
        "/execution",
        "/store",
        "/readiness",
        "/limits",
        "/controls",
    ] {
        let mut value = wire();
        value
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("undeclared".into(), json!(true));
        rejects(value);
    }
    for (parent, field) in [
        ("", "application"),
        ("/execution", "approval_service_mode"),
        ("/store", "durable_approvals"),
        ("/readiness", "checks"),
        ("/limits", "approval_ttl_ms"),
        ("/controls", "callback_timeout_ms"),
        ("/controls", "shutdown_max_wait_ms"),
    ] {
        let mut value = wire();
        value
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        rejects(value);
    }
    let source = serde_json::to_string(&wire()).unwrap();
    for declaration in [
        "\"descriptor_version\":8",
        "\"durable_permission_settled_thread_deletion\":true",
        "\"durable_permission_withheld_cancellation\":true",
        "\"callback_timeout_ms\":null",
    ] {
        let duplicate = source.replace(declaration, &format!("{declaration},{declaration}"));
        assert_ne!(duplicate, source);
        assert!(serde_json::from_str::<CapabilityDescriptorV8>(&duplicate).is_err());
    }
}
#[test]
fn v8_retains_inherited_wire_validation() {
    for (pointer, invalid) in [
        ("/application", Value::Null),
        ("/application/id", json!("")),
        ("/application/revision", json!("v".repeat(257))),
        ("/runtime_version", json!("bad\nlabel")),
        ("/runtime_version", json!("r".repeat(257))),
        ("/execution/native_callbacks", json!(false)),
        (
            "/execution/approval_service_mode",
            json!("volatile_fixture"),
        ),
        ("/store/durable_runtime", json!(false)),
        ("/store/durable_approvals", json!(false)),
        ("/features/fenced_deletion", json!(true)),
        ("/features/fenced_cancellation", json!(true)),
        ("/features/always_permissions", json!(true)),
        ("/features/trusted_known_id_recovery", json!(true)),
        ("/features/automatic_recovery_scan", json!(true)),
        ("/features/required_once_deny", json!(false)),
        ("/features/supervised_admission_drain", json!(false)),
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
        let mut old = base();
        *old.pointer_mut(pointer).unwrap() = invalid.clone();
        assert!(serde_json::from_value::<CapabilityDescriptorV7>(old).is_err());
        let mut value = wire();
        *value.pointer_mut(pointer).unwrap() = invalid;
        rejects(value);
    }
}
#[test]
fn constructors_and_public_validation_do_not_repair_invalid_inherited_assemblies() {
    let valid: CapabilityDescriptorV7 = serde_json::from_value(base()).unwrap();
    let invalid: &[fn(&mut CapabilityDescriptorV7)] = &[
        |v| v.descriptor_version = 8,
        |v| v.features.durable_permission_withheld_cancellation = false,
        |v| v.features.durable_permission_ready_cancellation = false,
        |v| v.features.durable_permission_entered_cancellation = false,
        |v| v.features.durable_permission_creation = false,
        |v| v.features.durable_permission_reuse = false,
        |v| v.features.fenced_deletion = true,
        |v| v.features.always_permissions = true,
        |v| v.application = None,
        |v| v.limits.active_runs = 2,
        |v| v.controls.shutdown_max_wait_ms = 0,
        |v| v.readiness.checks.swap(0, 2),
        |v| v.execution.approval_service_mode = CapabilityApprovalMode::VolatileFixture,
    ];
    for change in invalid {
        let mut bad = valid.clone();
        change(&mut bad);
        assert!(bad.validate().is_err());
        assert!(CapabilityDescriptorV8::permission_settled_thread_deletion(bad).is_err());
    }
    let valid: CapabilityDescriptorV8 = serde_json::from_value(wire()).unwrap();
    let invalid: &[fn(&mut CapabilityDescriptorV8)] = &[
        |v| v.descriptor_version = 7,
        |v| v.features.durable_permission_settled_thread_deletion = false,
        |v| v.features.durable_permission_withheld_cancellation = false,
        |v| v.features.fenced_deletion = true,
        |v| v.features.automatic_recovery_scan = true,
        |v| v.application = None,
        |v| v.limits.active_runs = 2,
        |v| v.controls.shutdown_max_wait_ms = 0,
        |v| v.readiness.checks.swap(0, 2),
        |v| v.execution.approval_service_mode = CapabilityApprovalMode::VolatileFixture,
    ];
    for change in invalid {
        let mut bad = valid.clone();
        change(&mut bad);
        assert!(bad.validate().is_err());
    }
}
