use super::empty_report;
use serde_json::{json, Value};

fn validates(value: &Value, schema: &Value) -> bool {
    if let Some(choices) = schema.get("oneOf").and_then(Value::as_array) {
        return choices
            .iter()
            .filter(|choice| validates(value, choice))
            .count()
            == 1;
    }
    if schema
        .get("const")
        .is_some_and(|constant| constant != value)
        || schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|choices| !choices.contains(value))
    {
        return false;
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("null") => value.is_null(),
        Some("boolean") => value.is_boolean(),
        Some("integer") => value.as_u64().is_some_and(|number| {
            schema
                .get("minimum")
                .and_then(Value::as_u64)
                .is_none_or(|minimum| number >= minimum)
                && schema
                    .get("maximum")
                    .and_then(Value::as_u64)
                    .is_none_or(|maximum| number <= maximum)
        }),
        Some("array") => value.as_array().is_some_and(|items| {
            schema
                .get("maxItems")
                .and_then(Value::as_u64)
                .is_none_or(|maximum| items.len() as u64 <= maximum)
                && schema
                    .get("items")
                    .is_none_or(|item_schema| items.iter().all(|item| validates(item, item_schema)))
        }),
        Some("object") => value.as_object().is_some_and(|object| {
            let properties = schema["properties"].as_object().unwrap();
            schema["required"]
                .as_array()
                .unwrap()
                .iter()
                .all(|key| object.contains_key(key.as_str().unwrap()))
                && (schema["additionalProperties"] != false
                    || object.keys().all(|key| properties.contains_key(key)))
                && object.iter().all(|(key, child)| {
                    properties
                        .get(key)
                        .is_none_or(|child_schema| validates(child, child_schema))
                })
        }),
        None => true,
        _ => panic!("authored report schema must use the supported fixed validation vocabulary"),
    }
}

pub fn assert_report_schema(value: &Value) {
    let schema: Value = serde_json::from_str(include_str!(
        "../../../../spec/runtime/stopped-inspection-report-v2.schema.json"
    ))
    .unwrap();
    assert!(
        validates(value, &schema),
        "actual report must satisfy the authored exact JSON schema"
    );
}

pub fn accepted_report(count: usize, blocked: bool) -> Value {
    let mut report = empty_report();
    report["table_rows"]["threads"] = json!(count);
    report["table_rows"]["executions"] = json!(count);
    report["table_rows"]["journal"] = json!(count * 2);
    report["executions_by_status"]["accepted"] = json!(count);
    report["declared_definition_tasks_by_recovery_policy"]["pure"] = json!(count);
    if blocked {
        let rows: Vec<_> = (1..=count)
            .map(|ordinal| {
                json!({
                    "ordinal": ordinal,
                    "status": "accepted",
                    "recorded_recovery_blocked": false,
                    "recorded_approval_park": false,
                    "has_uncertain_task": false,
                    "initialized_tasks_by_status": empty_report()["initialized_tasks_by_status"],
                    "declared_definition_tasks_by_recovery_policy": {
                        "pure": 1, "idempotent": 0, "reconcilable": 0, "unsafe": 0
                    }
                })
            })
            .collect();
        report["blocked"] = json!({"selection": "recorded_unfinished_or_uncertain", "rows": rows});
    }
    report
}

pub fn rich_report(blocked: bool) -> Value {
    let mut report = empty_report();
    report["table_rows"] = json!({
        "metadata": 1, "threads": 9, "executions": 9, "journal": 37,
        "approval_grants": 2, "approval_acceptances": 1,
        "permission_acceptances": 0, "permission_decisions": 0, "permission_uses": 0
    });
    report["executions_by_status"] = json!({
        "accepted": 1, "running": 1, "waiting_for_approval": 1,
        "completed": 2, "failed": 1, "cancel_requested": 1,
        "cancelled": 1, "recovery_blocked": 1
    });
    report["initialized_tasks_by_status"] = json!({
        "ready": 1, "waiting_for_approval": 1, "in_flight": 1,
        "succeeded": 1, "failed": 1, "denied": 1, "cancelled": 1,
        "outcome_unknown": 1
    });
    report["declared_definition_tasks_by_recovery_policy"] = json!({
        "pure": 3, "idempotent": 2, "reconcilable": 2, "unsafe": 3
    });
    report["recorded_uncertain_tasks_by_policy"] = json!({
        "pure": 1, "idempotent": 0, "reconcilable": 0, "unsafe": 1
    });
    if blocked {
        let rows: Vec<_> = [
            ("accepted", None, "pure", false, false, false),
            ("running", Some("ready"), "pure", false, false, false),
            (
                "waiting_for_approval",
                Some("waiting_for_approval"),
                "idempotent",
                false,
                true,
                false,
            ),
            (
                "cancel_requested",
                Some("in_flight"),
                "pure",
                false,
                false,
                true,
            ),
            (
                "recovery_blocked",
                Some("outcome_unknown"),
                "unsafe",
                true,
                false,
                true,
            ),
        ]
        .into_iter()
        .enumerate()
        .map(
            |(index, (status, task_status, policy, recovery, approval, uncertain))| {
                let mut initialized = empty_report()["initialized_tasks_by_status"].clone();
                if let Some(status) = task_status {
                    initialized[status] = json!(1);
                }
                let mut declared =
                    empty_report()["declared_definition_tasks_by_recovery_policy"].clone();
                declared[policy] = json!(1);
                if index == 0 {
                    declared["unsafe"] = json!(1);
                }
                json!({
                    "ordinal": index + 1, "status": status,
                    "recorded_recovery_blocked": recovery,
                    "recorded_approval_park": approval,
                    "has_uncertain_task": uncertain,
                    "initialized_tasks_by_status": initialized,
                    "declared_definition_tasks_by_recovery_policy": declared
                })
            },
        )
        .collect();
        report["blocked"] = json!({"selection": "recorded_unfinished_or_uncertain", "rows": rows});
    }
    report
}
