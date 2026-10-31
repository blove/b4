"""Two-lane restart metadata checks do not qualify Rust or process behavior.

Private source draft: adopt only this NEW file for metadata RED first, while
the checker and actual YAML remain unchanged. Pending synthetic records check
wiring. A later scoped pass requires independently reviewed recorded evidence.
"""
import contextlib
import copy
import io
import unittest
from unittest.mock import patch

import check_contracts


GROUP = "process_startup_restart_requirements"
SOURCE = "spec/runtime/startup-process-restart-design.md"
SCOPE = "bounded_two_lane_process_startup_restart_only"
SCOPED_PASS = "passed_bounded_two_lane_native_macos_linux_scope"
CASE_FILE = "crates/b4-http/tests/startup_restart.rs"
REQUIRED_CASES = (
    ("RESTART-001", "process_startup_park_preserves_once_and_sibling_without_replay"),
    ("RESTART-002", "process_startup_unsafe_effect_is_blocked_before_readiness_without_replay"),
)
PRIOR_GROUPS = (
    "foundation_requirements", "unchanged_ap_client_requirements",
    "native_cooperative_cancellation_requirements", "unfinished_discovery_requirements",
    "startup_recovery_preparation_requirements", "historical_native_definitions_requirements",
    "prior_owner_confinement_requirements",
)


def pending_group():
    return [{
        "id": identity,
        "classification": "NATIVE",
        "statement": "Pending synthetic two-lane process restart metadata-hook record.",
        "source": SOURCE,
        "component": "process-startup-restart",
        "acceptance_tests": [{"file": CASE_FILE, "name": name}],
        "implementation_status": "pending",
        "verification_status": "not_run",
        "release_scope": SCOPE,
    } for identity, name in REQUIRED_CASES]


class ProcessStartupRestartTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, mutate=None):
        original_load = check_contracts.yaml.safe_load
        retained = {}

        def with_restart_group(source):
            value = original_load(source)
            if isinstance(value, dict) and "foundation_requirements" in value:
                value = copy.deepcopy(value)
                retained["before"] = copy.deepcopy(value)
                value[GROUP] = pending_group()
                if mutate:
                    mutate(value)
                retained["checked"] = value
            return value

        output = io.StringIO()
        with patch.object(check_contracts.yaml, "safe_load", with_restart_group), \
                contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        self.assertTrue(retained, "Actual requirements YAML was not checked")
        self.assertEqual(
            {key: value for key, value in retained["checked"].items() if key != GROUP},
            {key: value for key, value in retained["before"].items() if key != GROUP},
            "Checking the restart group must preserve every old field/group/status/requirement",
        )
        return output.getvalue()

    def test_two_restart_cases_are_separate_and_preserve_every_prior_scope(self):
        requirements_path = check_contracts.REPO / "spec/runtime/requirements.yaml"
        before = check_contracts.yaml.safe_load(requirements_path.read_text())
        self.assertTrue(GROUP in before, "Actual YAML must contain the separate process restart group")
        self.assertEqual(sorted(item["id"] for item in before[GROUP]),
                         [identity for identity, _ in REQUIRED_CASES])
        for item in before[GROUP]:
            self.assertEqual(item["acceptance_tests"], [{
                "file": CASE_FILE, "name": dict(REQUIRED_CASES)[item["id"]],
            }])
        output_buffer = io.StringIO()
        with contextlib.redirect_stdout(output_buffer):
            check_contracts.check_contracts()
        output = output_buffer.getvalue()
        after = check_contracts.yaml.safe_load(requirements_path.read_text())
        self.assertEqual({key: value for key, value in after.items() if key != GROUP},
                         {key: value for key, value in before.items() if key != GROUP},
                         "Every existing root field/group/status/requirement must remain exact")
        self.assertIn("Process startup restart: 2 named local requirements; "
                      "bounded two-lane process scope only; metadata is not runtime evidence.", output)
        for prior in (
            "Traceability: 338 named local requirements",
            "Unchanged client corpus: 8 named local requirements",
            "Native cooperative cancellation: 15 named local requirements",
            "Unfinished discovery: 19 named local requirements",
            "Startup recovery preparation: 28 named local requirements",
            "Historical native Definitions: 31 named local requirements",
            "Prior-owner confinement: 17 named local requirements",
            "eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending",
            "CP-010 unresolved; M0 open",
            "named-test existence do not assert fresh test execution",
        ):
            self.assertIn(prior, output)

    def test_restart_group_requires_narrow_scope_and_recorded_json_for_scoped_pass(self):
        changes = (
            {"classification": "COMPAT"},
            {"source": "spec/runtime/0001-agent-protocol-core.md"},
            {"component": "automatic-full-restart"},
            {"release_scope": "full_rfc11_4_or_core_profile"},
            {"implementation_status": "complete"},
            {"verification_status": "passed"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "README.md"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "conformance/expected/missing-process-startup-restart.json"},
        )
        for change in changes:
            with self.subTest(change=change):
                def mutate(value):
                    value[GROUP][0].update(change)
                with self.assertRaisesRegex(ValueError, "Process startup restart|Missing evidence file"):
                    self.checked_requirements(mutate)

    def test_restart_group_refuses_missing_cases_names_and_cross_group_duplicates(self):
        for identity, _ in REQUIRED_CASES:
            with self.subTest(missing_case=identity):
                def mutate(value):
                    value[GROUP] = [item for item in value[GROUP] if item["id"] != identity]
                with self.assertRaisesRegex(ValueError, "Process startup restart scoped case inventory"):
                    self.checked_requirements(mutate)

        def empty_case(value):
            value[GROUP][0]["acceptance_tests"] = []
        with self.assertRaisesRegex(ValueError, "Process startup restart.*acceptance"):
            self.checked_requirements(empty_case)

        def missing_name(value):
            value[GROUP][0]["acceptance_tests"][0]["name"] = "missing_restart_parent_qualification"
        with self.assertRaisesRegex(ValueError, "Process startup restart.*acceptance"):
            self.checked_requirements(missing_name)

        def substituted_existing_case(value):
            value[GROUP][0]["acceptance_tests"][0]["name"] = REQUIRED_CASES[1][1]
        with self.assertRaisesRegex(ValueError, "Process startup restart.*acceptance"):
            self.checked_requirements(substituted_existing_case)

        for previous in PRIOR_GROUPS:
            with self.subTest(previous=previous):
                def mutate(value):
                    value[GROUP][0]["id"] = value[previous][0]["id"]
                with self.assertRaisesRegex(ValueError, "Duplicate named local requirement IDs"):
                    self.checked_requirements(mutate)


if __name__ == "__main__":
    unittest.main()
