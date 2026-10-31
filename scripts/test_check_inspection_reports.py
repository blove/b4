"""Operator report validation protects fixed disclosure and counter semantics."""

import copy
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import check_inspection_reports as reports


class InspectionReportTests(unittest.TestCase):
    def setUp(self):
        self.summary = json.loads((reports.EXAMPLES / "summary.json").read_text())
        self.blocked = json.loads((reports.EXAMPLES / "blocked.json").read_text())

    def encoded(self, value):
        return json.dumps(value, separators=(",", ":")).encode() + b"\n"

    def refuses(self, value):
        with self.assertRaises(reports.ReportError) as caught:
            reports.validate_report_bytes(self.encoded(value))
        self.assertEqual(str(caught.exception), "b4run_inspection_report_invalid")

    def accepts(self, value):
        try:
            reports.validate_report_bytes(self.encoded(value))
        except reports.ReportError:
            self.fail("valid fixed operator report must be accepted")

    def test_empty_summary_and_blocked_reports_are_accepted(self):
        self.accepts(self.summary)
        self.accepts(self.blocked)

    def test_fixed_disclosure_rejects_extra_fields_at_every_object(self):
        for path in [(), ("reader",), ("table_rows",), ("executions_by_status",),
                     ("initialized_tasks_by_status",),
                     ("declared_definition_tasks_by_recovery_policy",),
                     ("recorded_uncertain_tasks_by_policy",), ("blocked",)]:
            value = copy.deepcopy(self.blocked)
            target = value
            for key in path:
                target = target[key]
            target["private_identity"] = "private-value-marker"
            self.refuses(value)

    def test_reader_constants_and_counter_integer_bounds_are_strict(self):
        for field in self.summary["reader"]:
            value = copy.deepcopy(self.summary)
            value["reader"][field] = "private-version-marker"
            self.refuses(value)
        for count in [-1, True, 18446744073709551616]:
            value = copy.deepcopy(self.summary)
            value["table_rows"]["metadata"] = count
            self.refuses(value)

    def test_metadata_execution_and_uncertainty_totals_must_agree(self):
        for group, field in [("table_rows", "metadata"),
                             ("table_rows", "executions"),
                             ("recorded_uncertain_tasks_by_policy", "unsafe")]:
            value = copy.deepcopy(self.summary)
            value[group][field] += 1
            self.refuses(value)

    def test_initialized_and_uncertain_counts_cannot_exceed_declared_policies(self):
        value = self.selected()
        value["executions_by_status"]["accepted"] = 0
        value["executions_by_status"]["running"] = 1
        value["blocked"]["rows"][0]["status"] = "running"
        for target in (value, value["blocked"]["rows"][0]):
            target["initialized_tasks_by_status"]["in_flight"] = 1
            target["declared_definition_tasks_by_recovery_policy"]["pure"] = 1
        value["recorded_uncertain_tasks_by_policy"]["pure"] = 1
        value["blocked"]["rows"][0]["has_uncertain_task"] = True
        self.accepts(value)
        for group, key in [("declared_definition_tasks_by_recovery_policy", "pure"),
                           ("recorded_uncertain_tasks_by_policy", "pure")]:
            broken = copy.deepcopy(value)
            broken[group][key] = 0
            self.refuses(broken)
        broken = copy.deepcopy(value)
        broken["blocked"]["rows"][0]["initialized_tasks_by_status"]["in_flight"] = 2
        self.refuses(broken)

    def test_missing_fixed_fields_and_unknown_status_are_rejected(self):
        for field in self.summary:
            value = copy.deepcopy(self.summary)
            del value[field]
            self.refuses(value)
        value = self.selected()
        value["blocked"]["rows"][0]["status"] = "private-status-marker"
        self.refuses(value)

    def selected(self):
        value = copy.deepcopy(self.blocked)
        value["table_rows"]["executions"] = 1
        value["executions_by_status"]["accepted"] = 1
        value["blocked"]["rows"] = [{
            "ordinal": 1, "status": "accepted",
            "recorded_recovery_blocked": False, "recorded_approval_park": False,
            "has_uncertain_task": False,
            "initialized_tasks_by_status": copy.deepcopy(value["initialized_tasks_by_status"]),
            "declared_definition_tasks_by_recovery_policy": copy.deepcopy(
                value["declared_definition_tasks_by_recovery_policy"]),
        }]
        return value

    def test_blocked_rows_require_contiguous_ordinals_exact_flags_and_complete_selection(self):
        self.accepts(self.selected())
        for field, replacement in [("ordinal", 2), ("recorded_recovery_blocked", True),
                                   ("recorded_approval_park", True),
                                   ("has_uncertain_task", True), ("status", "completed")]:
            value = self.selected()
            value["blocked"]["rows"][0][field] = replacement
            self.refuses(value)
        value = self.selected()
        value["blocked"]["rows"] = []
        self.refuses(value)
        value = self.selected()
        value["blocked"]["rows"][0]["source_id"] = "private-id-marker"
        self.refuses(value)

    def test_default_summary_allows_more_than_64_unfinished_rows_but_blocked_refuses(self):
        value = copy.deepcopy(self.summary)
        value["table_rows"]["executions"] = 65
        value["executions_by_status"]["accepted"] = 65
        self.accepts(value)
        value["blocked"] = {"selection": "recorded_unfinished_or_uncertain", "rows": []}
        self.refuses(value)

    def test_declared_task_totals_require_execution_rows_and_stay_within_kernel_bound(self):
        for executions, tasks in [(0, 1), (1, 4097), (256, 256 * 4096 + 1)]:
            value = copy.deepcopy(self.summary)
            value["table_rows"]["executions"] = executions
            value["executions_by_status"]["accepted"] = executions
            value["declared_definition_tasks_by_recovery_policy"]["pure"] = tasks
            self.refuses(value)
        value = self.selected()
        for target in (value, value["blocked"]["rows"][0]):
            target["declared_definition_tasks_by_recovery_policy"]["pure"] = 4096
        self.accepts(value)
        # The aggregate budget also permits 4097 when two rows are present;
        # this selected row must still obey the independent per-row bound.
        value["table_rows"]["executions"] = 2
        value["executions_by_status"]["completed"] = 1
        for target in (value, value["blocked"]["rows"][0]):
            target["declared_definition_tasks_by_recovery_policy"]["pure"] = 4097
        self.refuses(value)

    def test_accepted_selected_rows_have_no_initialized_tasks(self):
        for status in self.summary["initialized_tasks_by_status"]:
            value = self.selected()
            for target in (value, value["blocked"]["rows"][0]):
                target["initialized_tasks_by_status"][status] = 1
                target["declared_definition_tasks_by_recovery_policy"]["pure"] = 1
            if status in ("in_flight", "outcome_unknown"):
                value["recorded_uncertain_tasks_by_policy"]["pure"] = 1
                value["blocked"]["rows"][0]["has_uncertain_task"] = True
            self.refuses(value)

    def waiting(self):
        value = self.selected()
        value["executions_by_status"]["accepted"] = 0
        value["executions_by_status"]["waiting_for_approval"] = 1
        row = value["blocked"]["rows"][0]
        row["status"] = "waiting_for_approval"
        row["recorded_approval_park"] = True
        for target in (value, row):
            target["initialized_tasks_by_status"]["waiting_for_approval"] = 1
            target["declared_definition_tasks_by_recovery_policy"]["pure"] = 1
        return value

    def test_approval_park_requires_waiting_tasks_without_ready_failed_or_uncertain_tasks(self):
        self.accepts(self.waiting())
        value = self.waiting()
        for target in (value, value["blocked"]["rows"][0]):
            target["initialized_tasks_by_status"]["waiting_for_approval"] = 0
        self.refuses(value)
        for status in ("ready", "failed", "in_flight", "outcome_unknown"):
            value = self.waiting()
            for target in (value, value["blocked"]["rows"][0]):
                target["initialized_tasks_by_status"][status] = 1
                target["declared_definition_tasks_by_recovery_policy"]["pure"] = 2
            if status in ("in_flight", "outcome_unknown"):
                value["recorded_uncertain_tasks_by_policy"]["pure"] = 1
                value["blocked"]["rows"][0]["has_uncertain_task"] = True
            self.refuses(value)

    def test_invalid_authored_schema_has_only_a_static_standalone_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            schema = Path(directory) / "schema.json"
            schema.write_text('{"type":"private-schema-marker"}')
            stderr = io.StringIO()
            with patch.object(reports, "SCHEMA", schema), contextlib.redirect_stderr(stderr):
                try:
                    result = reports.main()
                except Exception:
                    self.fail("schema failure must return the static error instead of escaping")
            self.assertEqual(result, 1)
            self.assertEqual(stderr.getvalue(), "b4run_inspection_report_invalid\n")

    def test_malformed_duplicate_noninteger_and_oversized_bytes_refuse_statically(self):
        valid = self.encoded(self.summary)
        invalid = [b"private-malformed-marker", b'{"private":NaN}',
                   valid.replace(b'"metadata":1', b'"metadata":1.0'),
                   valid.replace(b'"metadata":1', b'"metadata":1,"metadata":1'),
                   b" " * 65538, b"\xff", valid.rstrip(b"\n"),
                   valid.decode().encode("utf-16-be")]
        for raw in invalid:
            with self.assertRaises(reports.ReportError) as caught:
                reports.validate_report_bytes(raw)
            self.assertEqual(str(caught.exception), "b4run_inspection_report_invalid")


if __name__ == "__main__":
    unittest.main()
