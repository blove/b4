"""Accepted Once startup metadata checks; these do not qualify runtime behavior."""
import contextlib
import copy
import io
import unittest
from unittest.mock import patch

import check_contracts

GROUP = "accepted_once_startup_requirements"
SOURCE = "spec/runtime/startup-accepted-once-design.md"
SCOPE = "bounded_accepted_once_process_startup_only"
SCOPED_PASS = "passed_bounded_accepted_once_native_macos_linux_scope"
CASE_FILE = "crates/b4-http/tests/startup_accepted_once.rs"
IDENTITY = "ONCE-STARTUP-001"
CASE = "process_startup_accepted_once_after_expiry_preserves_intent_and_sibling"
PRIOR_GROUPS = (
    "foundation_requirements", "unchanged_ap_client_requirements",
    "native_cooperative_cancellation_requirements", "unfinished_discovery_requirements",
    "startup_recovery_preparation_requirements", "historical_native_definitions_requirements",
    "prior_owner_confinement_requirements", "process_startup_restart_requirements",
)


def pending_group():
    return [{
        "id": IDENTITY, "classification": "NATIVE",
        "statement": "Synthetic pending Accepted Once startup metadata record.",
        "source": SOURCE, "component": "startup-accepted-once",
        "acceptance_tests": [{"file": CASE_FILE, "name": CASE}],
        "implementation_status": "pending", "verification_status": "not_run",
        "release_scope": SCOPE,
    }]


class AcceptedOnceStartupTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, mutate=None):
        original_load = check_contracts.yaml.safe_load
        retained = {}

        def with_once_group(source):
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
        with patch.object(check_contracts.yaml, "safe_load", with_once_group), \
                contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        self.assertTrue(retained, "Actual requirements YAML was not checked")
        self.assertEqual(
            {key: value for key, value in retained["checked"].items() if key != GROUP},
            {key: value for key, value in retained["before"].items() if key != GROUP},
            "The new group must preserve every prior field, requirement and status",
        )
        return output.getvalue()

    def test_one_parent_is_separate_and_preserves_every_prior_scope(self):
        path = check_contracts.REPO / "spec/runtime/requirements.yaml"
        before = check_contracts.yaml.safe_load(path.read_text())
        self.assertIn(GROUP, before, "Actual YAML needs the separate Accepted Once startup group")
        self.assertEqual([item["id"] for item in before[GROUP]], [IDENTITY])
        self.assertEqual(before[GROUP][0]["acceptance_tests"], [{"file": CASE_FILE, "name": CASE}])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        after = check_contracts.yaml.safe_load(path.read_text())
        self.assertEqual({k: v for k, v in after.items() if k != GROUP},
                         {k: v for k, v in before.items() if k != GROUP})
        self.assertIn("Accepted Once startup: 1 named local requirements; "
                      "bounded accepted-Once process scope only; metadata is not runtime evidence.",
                      output.getvalue())
        for prior in (
            "Traceability: 338 named local requirements",
            "Unchanged client corpus: 8 named local requirements",
            "Native cooperative cancellation: 15 named local requirements",
            "Unfinished discovery: 19 named local requirements",
            "Startup recovery preparation: 28 named local requirements",
            "Historical native Definitions: 31 named local requirements",
            "Prior-owner confinement: 17 named local requirements",
            "Process startup restart: 2 named local requirements",
            "eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending", "CP-010 unresolved; M0 open",
            "named-test existence do not assert fresh test execution",
        ):
            self.assertIn(prior, output.getvalue())

    def test_scoped_pass_requires_narrow_scope_and_recorded_json(self):
        changes = (
            {"classification": "COMPAT"}, {"source": "spec/runtime/0001-agent-protocol-core.md"},
            {"component": "full-automatic-startup"}, {"release_scope": "full_rfc11_4"},
            {"implementation_status": "complete"}, {"verification_status": "passed"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "README.md"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "conformance/expected/missing-startup-accepted-once.json"},
        )
        for change in changes:
            with self.subTest(change=change):
                def mutate(value):
                    value[GROUP][0].update(change)
                with self.assertRaisesRegex(ValueError, "Accepted Once startup|Missing evidence file"):
                    self.checked_requirements(mutate)

    def test_missing_parent_helper_substitution_and_cross_group_duplicates_are_refused(self):
        def missing(value):
            value[GROUP] = []
        with self.assertRaisesRegex(ValueError, "Accepted Once startup scoped case inventory"):
            self.checked_requirements(missing)
        for tests in (
            [], [{"file": CASE_FILE, "name": "missing_accepted_once_parent"}],
            [{"file": CASE_FILE, "name": "startup_accepted_once_child_helper"}],
            [{"file": "crates/b4-http/tests/startup_restart.rs",
              "name": "process_startup_park_preserves_once_and_sibling_without_replay"}],
        ):
            with self.subTest(tests=tests):
                def mutate(value):
                    value[GROUP][0]["acceptance_tests"] = tests
                with self.assertRaisesRegex(ValueError, "Accepted Once startup.*acceptance"):
                    self.checked_requirements(mutate)
        for previous in PRIOR_GROUPS:
            with self.subTest(previous=previous):
                def mutate(value):
                    value[GROUP][0]["id"] = value[previous][0]["id"]
                with self.assertRaisesRegex(ValueError, "Duplicate named local requirement IDs"):
                    self.checked_requirements(mutate)


if __name__ == "__main__":
    unittest.main()
