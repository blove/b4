"""Startup confinement metadata checks; these do not qualify runtime behavior."""
import contextlib
import copy
import io
import re
import unittest
from unittest.mock import patch

import check_contracts

GROUP = "startup_confinement_requirements"
SOURCE = "spec/runtime/startup-confinement-design.md"
SCOPE = "explicit_bounded_opt_in_native_startup_confinement_only"
SCOPED_PASS = "passed_bounded_startup_confinement_native_macos_linux_scope"
EVIDENCE = "conformance/expected/startup-confinement-verification.json"
IDENTITY = "STARTUP-CONFINE-001"
PRIVATE_FILE = "crates/b4-http/src/startup/confinement/tests.rs"
API_FILE = "crates/b4-http/tests/startup_confinement.rs"
TCP_FILE = "crates/b4-http/tests/startup_confinement_tcp.rs"
CASES = (
    (PRIVATE_FILE, "borrowed_start_future_has_no_framework_alias_before_poll"),
    (PRIVATE_FILE, "start_limits_and_single_attempt_precede_argument_clones"),
    (PRIVATE_FILE, "startup_job_requires_whole_original_and_exact_principal"),
    (PRIVATE_FILE, "startup_arguments_dispose_two_panicking_handles_under_one_anchor"),
    (PRIVATE_FILE, "startup_progress_counts_verified_receipt_but_not_ambiguous_commit"),
    (PRIVATE_FILE, "final_startup_capture_drop_cannot_activate_or_escape_ownership"),
    (API_FILE, "opt_in_missing_exact_definition_confines_original_prior_owner"),
    (API_FILE, "opt_in_denied_recover_policy_preserves_accepted_once"),
    (API_FILE, "default_preparation_refusal_remains_unchanged"),
    (API_FILE, "unresolved_principal_and_denied_confinement_authority_refuse"),
    (API_FILE, "audited_prior_block_counts_without_readoption"),
    (API_FILE, "available_historical_definition_uses_original_typed_recovery"),
    (API_FILE, "unsupported_and_changed_custom_store_refuse_before_write"),
    (API_FILE, "scan_limit_preserves_two_confirmed_blocks_and_stops_third_candidate"),
    (TCP_FILE, "preparing_listener_stays_closed_through_confinement_authority_and_cleanup"),
    (TCP_FILE, "mixed_startup_opens_after_confinement_before_runnable_callback_settlement"),
)


def acceptance_tests():
    return [{"file": file, "name": name} for file, name in CASES]


def pending_group():
    return [{
        "id": IDENTITY, "classification": "NATIVE",
        "statement": "Synthetic pending startup confinement metadata record.",
        "source": SOURCE, "component": "startup-confinement",
        "acceptance_tests": acceptance_tests(),
        "implementation_status": "pending", "verification_status": "not_run",
        "release_scope": SCOPE,
    }]


class StartupConfinementTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, mutate=None):
        original_load = check_contracts.yaml.safe_load
        retained = {}

        def with_group(source):
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
        with patch.object(check_contracts.yaml, "safe_load", with_group), \
                contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        self.assertTrue(retained, "Actual requirements YAML was not checked")
        self.assertEqual(
            {key: value for key, value in retained["checked"].items() if key != GROUP},
            {key: value for key, value in retained["before"].items() if key != GROUP},
            "The new group must preserve every prior field, requirement and status",
        )
        return output.getvalue()

    def test_six_private_eight_api_two_tcp_are_separate_from_prior_scopes(self):
        path = check_contracts.REPO / "spec/runtime/requirements.yaml"
        before = check_contracts.yaml.safe_load(path.read_text())
        self.assertIn(GROUP, before, "Actual YAML needs the separate startup confinement group")
        self.assertEqual([item["id"] for item in before[GROUP]], [IDENTITY])
        self.assertEqual(before[GROUP][0]["acceptance_tests"], acceptance_tests())
        for file, expected in ((PRIVATE_FILE, 6), (API_FILE, 8), (TCP_FILE, 2)):
            source = (check_contracts.REPO / file).read_text()
            declarations = re.findall(
                r"#\[(?:tokio::)?test(?:\([^\]]*\))?\]\s*(?:async\s+)?fn\s+(\w+)\s*\(",
                source,
            )
            self.assertEqual(len(declarations), expected)
            self.assertEqual(set(declarations), {name for item, name in CASES if item == file})
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        after = check_contracts.yaml.safe_load(path.read_text())
        self.assertEqual({key: value for key, value in after.items() if key != GROUP},
                         {key: value for key, value in before.items() if key != GROUP})
        self.assertIn("Startup confinement: 1 named local requirements; "
                      "explicit bounded opt-in native scope only; metadata is not runtime evidence.",
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
            "Accepted Once startup: 1 named local requirements",
            "eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending", "CP-010 unresolved; M0 open",
            "named-test existence do not assert fresh test execution",
        ):
            self.assertIn(prior, output.getvalue())

    def test_scoped_pass_requires_own_scope_status_and_recorded_json(self):
        changes = (
            {"classification": "COMPAT"}, {"source": "spec/runtime/0001-agent-protocol-core.md"},
            {"component": "full-automatic-startup"}, {"release_scope": "full_rfc11_4"},
            {"implementation_status": "complete"}, {"verification_status": "passed"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "README.md"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "conformance/expected/missing-startup-confinement.json"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "conformance/expected/startup-accepted-once-verification.json"},
        )
        for change in changes:
            with self.subTest(change=change):
                def mutate(value):
                    value[GROUP][0].update(change)
                with self.assertRaisesRegex(ValueError, "Startup confinement|Missing evidence file"):
                    self.checked_requirements(mutate)

    def test_missing_cases_helper_substitution_and_cross_group_duplicates_are_refused(self):
        def missing(value):
            value[GROUP] = []
        with self.assertRaisesRegex(ValueError, "Startup confinement scoped case inventory"):
            self.checked_requirements(missing)
        variants = (
            [], acceptance_tests()[:-1],
            [{"file": "crates/b4-http/tests/startup_confinement_support/fixtures.rs",
              "name": "run_positive"}, *acceptance_tests()[1:]],
            [{"file": "crates/b4-http/tests/startup_accepted_once.rs",
              "name": "process_startup_accepted_once_after_expiry_preserves_intent_and_sibling"},
             *acceptance_tests()[1:]],
            [acceptance_tests()[0], *acceptance_tests()[:-1]],
        )
        for tests in variants:
            with self.subTest(tests=tests):
                def mutate(value):
                    value[GROUP][0]["acceptance_tests"] = tests
                with self.assertRaisesRegex(ValueError, "Startup confinement.*acceptance"):
                    self.checked_requirements(mutate)
        before = check_contracts.yaml.safe_load(
            (check_contracts.REPO / "spec/runtime/requirements.yaml").read_text())
        prior_groups = [key for key, value in before.items()
                        if key != GROUP and key.endswith("_requirements") and value]
        for previous in prior_groups:
            with self.subTest(previous=previous):
                def mutate(value):
                    value[GROUP][0]["id"] = value[previous][0]["id"]
                with self.assertRaisesRegex(ValueError, "Duplicate named local requirement IDs"):
                    self.checked_requirements(mutate)


if __name__ == "__main__":
    unittest.main()
