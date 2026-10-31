"""Validate fixed operator report contracts and authored/native-derived examples."""

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

REPO = Path(__file__).resolve().parents[1]
EXAMPLES = REPO / "spec/runtime/inspection-examples"
SCHEMA = REPO / "spec/runtime/stopped-inspection-report.schema.json"
SCHEMA_V2 = REPO / "spec/runtime/stopped-inspection-report-v2.schema.json"
UNFINISHED = {"accepted", "running", "waiting_for_approval", "cancel_requested",
              "recovery_blocked"}


class ReportError(ValueError):
    def __init__(self):
        super().__init__("b4run_inspection_report_invalid")


def invalid_number(_value):
    raise ReportError()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReportError()
        result[key] = value
    return result


def require(condition):
    if not condition:
        raise ReportError()


def validate_report_bytes(raw):
    """Validate bounded safe output; raw invalid values never enter diagnostics."""
    require(isinstance(raw, bytes) and len(raw) <= 65537 and raw.endswith(b"\n"))
    try:
        value = json.loads(raw.decode("utf-8"), parse_float=invalid_number,
                           parse_constant=invalid_number, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError, RecursionError):
        raise ReportError() from None
    require(isinstance(value, dict))
    version = value.get("inspection_report_version")
    require(type(version) is int and version in (1, 2))
    schema_path = SCHEMA if version == 1 else SCHEMA_V2
    validator = Draft202012Validator(json.loads(schema_path.read_text()))
    require(next(validator.iter_errors(value), None) is None)

    executions = value["executions_by_status"]
    initialized = value["initialized_tasks_by_status"]
    declared = value["declared_definition_tasks_by_recovery_policy"]
    uncertain = value["recorded_uncertain_tasks_by_policy"]
    require(value["table_rows"]["metadata"] == 1)
    require(sum(executions.values()) == value["table_rows"]["executions"] <= 256)
    # Snapshot1 definitions contain at most 4096 tasks per execution.
    require(sum(declared.values()) <= value["table_rows"]["executions"] * 4096)
    require(sum(initialized.values()) <= sum(declared.values()))
    require(sum(uncertain.values()) == initialized["in_flight"] + initialized["outcome_unknown"])
    require(all(uncertain[key] <= declared[key] for key in uncertain))

    blocked = value["blocked"]
    if blocked is None:
        return
    rows = blocked["rows"]
    # Under the qualified Snapshot1 kernel, terminal rows cannot retain uncertain
    # tasks; every selected row therefore has one of these five stored statuses.
    require(len(rows) == sum(executions[key] for key in UNFINISHED))
    selected_statuses = {key: 0 for key in executions}
    selected_tasks = {key: 0 for key in initialized}
    selected_policies = {key: 0 for key in declared}
    for ordinal, row in enumerate(rows, 1):
        require(row["ordinal"] == ordinal)
        require(row["status"] in UNFINISHED)
        require(row["recorded_recovery_blocked"] == (row["status"] == "recovery_blocked"))
        require(row["recorded_approval_park"] == (row["status"] == "waiting_for_approval"))
        task_counts = row["initialized_tasks_by_status"]
        policy_counts = row["declared_definition_tasks_by_recovery_policy"]
        require(row["has_uncertain_task"] ==
                (task_counts["in_flight"] + task_counts["outcome_unknown"] > 0))
        require(sum(task_counts.values()) <= sum(policy_counts.values()))
        require(sum(policy_counts.values()) <= 4096)
        if row["status"] == "accepted":
            require(sum(task_counts.values()) == 0)
        if row["status"] == "waiting_for_approval":
            require(task_counts["waiting_for_approval"] > 0)
            require(all(task_counts[key] == 0 for key in
                        ("ready", "failed", "in_flight", "outcome_unknown")))
        selected_statuses[row["status"]] += 1
        for key, count in task_counts.items():
            selected_tasks[key] += count
        for key, count in policy_counts.items():
            selected_policies[key] += count
    require(all(selected_statuses[key] == executions[key] for key in UNFINISHED))
    require(all(selected_tasks[key] <= initialized[key] for key in initialized))
    require(all(selected_policies[key] <= declared[key] for key in declared))
    require(selected_tasks["in_flight"] == initialized["in_flight"])
    require(selected_tasks["outcome_unknown"] == initialized["outcome_unknown"])


def main():
    try:
        for schema_path in (SCHEMA, SCHEMA_V2):
            schema = json.loads(schema_path.read_text())
            Draft202012Validator.check_schema(schema)
        for name in ("summary.json", "blocked.json", "summary-v2.json", "blocked-v2.json"):
            validate_report_bytes((EXAMPLES / name).read_bytes())
    except (OSError, ValueError, RecursionError, SchemaError):
        print("b4run_inspection_report_invalid", file=sys.stderr)
        return 1
    print("Validated authored version1 and native-derived version2 stopped-inspection reports "
          "and separate operator schemas; AP HTTP schema inventory is unchanged.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
