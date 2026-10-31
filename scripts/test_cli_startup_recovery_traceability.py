"""Explicit native demo CLI recovery metadata; these checks are not runtime evidence."""
import contextlib
import copy
import io
import re
import unittest
from unittest.mock import patch

import check_contracts

GROUP = "cli_startup_recovery_requirements"
SOURCE = "spec/runtime/cli-startup-recovery-design.md"
SCOPE = "explicit_fixed_native_demo_cli_startup_recovery_only"
SCOPED_PASS = "passed_explicit_native_demo_cli_macos_linux_scope"
EVIDENCE = "conformance/expected/cli-startup-recovery-verification.json"
IDENTITY = "CLI-RECOVERY-001"
CASES = (
    ("crates/b4-cli/src/config_recovery_tests.rs", "explicit_recovery_selects_distinct_serve_command_in_both_option_orders"),
    ("crates/b4-cli/src/config_recovery_tests.rs", "recovery_config_check_accepts_both_orders_without_a_serve_command"),
    ("crates/b4-cli/src/config_recovery_tests.rs", "recovery_grammar_refuses_malformed_options_before_environment_access"),
    ("crates/b4-cli/src/config_recovery_tests.rs", "recovery_selection_preserves_defaults_and_redacts_private_configuration"),
    ("crates/b4-cli/src/process_startup_tests.rs", "startup_resolver_accepts_only_the_fixed_trusted_demo_tuple"),
    ("crates/b4-cli/src/process_startup_tests.rs", "signal_during_held_recovery_keeps_ownership_until_normal_return"),
    ("crates/b4-cli/src/process_startup_tests.rs", "opaque_lost_server_during_preparation_keeps_payload_and_executor_owned"),
    ("crates/b4-cli/src/process_startup_tests.rs", "readiness_precedes_settlement_of_a_legally_accepted_recovered_callback"),
    ("crates/b4-cli/tests/cli_startup_recovery.rs", "explicit_cli_restart_adopts_the_original_park_and_resumes_only_approved_work"),
    ("crates/b4-cli/tests/cli_startup_missing_definition.rs", "explicit_cli_missing_historical_definition_refuses_after_normal_retirement"),
    ("crates/b4-cli/tests/cli_startup_controls.rs", "recovery_config_check_both_orders_validate_without_storage_or_listener_acquisition"),
    ("crates/b4-cli/tests/cli_startup_controls.rs", "explicit_empty_store_startup_preserves_empty_work_and_releases_its_real_owner"),
)


def acceptance_tests():
    return [{"file": file, "name": name} for file, name in CASES]


def pending_group():
    return [{
        "id": IDENTITY, "classification": "NATIVE",
        "statement": "Synthetic pending explicit native demo CLI recovery metadata record.",
        "source": SOURCE, "component": "cli-startup-recovery",
        "acceptance_tests": acceptance_tests(), "implementation_status": "pending",
        "verification_status": "not_run", "release_scope": SCOPE,
    }]


class CliStartupRecoveryTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, mutate):
        original_load = check_contracts.yaml.safe_load
        retained = {}

        def with_group(source):
            value = original_load(source)
            if isinstance(value, dict) and "foundation_requirements" in value:
                value = copy.deepcopy(value)
                retained["before"] = copy.deepcopy(value)
                value[GROUP] = pending_group()
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
        )
        return output.getvalue()

    def test_actual_group_names_twelve_parents_and_preserves_prior_scopes(self):
        path = check_contracts.REPO / "spec/runtime/requirements.yaml"
        before = check_contracts.yaml.safe_load(path.read_text())
        self.assertTrue(GROUP in before, "Actual YAML needs a separate explicit CLI recovery group")
        self.assertEqual([item["id"] for item in before[GROUP]], [IDENTITY])
        self.assertEqual(before[GROUP][0]["acceptance_tests"], acceptance_tests())
        for file in dict.fromkeys(file for file, _ in CASES):
            source = (check_contracts.REPO / file).read_text()
            declared = set()
            for attributes, name in re.findall(
                r"((?:#\[[^\n]*\]\s*)+)(?:async\s+)?fn\s+(\w+)\s*\(", source
            ):
                if re.search(r"#\[(?:tokio::)?test(?:\([^\]]*\))?\]", attributes) \
                        and not re.search(r"#\[ignore(?:\s*=|\])", attributes):
                    declared.add(name)
            self.assertEqual(declared, {name for item, name in CASES if item == file})
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        after = check_contracts.yaml.safe_load(path.read_text())
        self.assertEqual(before, after)
        self.assertIn("CLI startup recovery: 1 named local requirements; "
                      "explicit fixed native demo scope only; metadata is not runtime evidence.",
                      output.getvalue())
        for prior in (
            "Traceability: 338 named local requirements", "eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending",
            "CP-010 unresolved; M0 open", "Accepted Once startup: 1 named local requirements",
            "Startup confinement: 1 named local requirements",
            "named-test existence do not assert fresh test execution",
        ):
            self.assertIn(prior, output.getvalue())

    def test_scoped_pass_refuses_foreign_authority_status_and_evidence(self):
        for change in (
            {"classification": "COMPAT"}, {"source": "README.md"},
            {"component": "arbitrary-application-reconstruction"},
            {"release_scope": "full_rfc11_4"}, {"implementation_status": "complete"},
            {"verification_status": "passed"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "README.md"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "conformance/expected/missing-cli-startup-recovery.json"},
            {"implementation_status": "implemented", "verification_status": SCOPED_PASS,
             "evidence": "conformance/expected/startup-confinement-verification.json"},
        ):
            with self.subTest(change=change):
                def mutate(value):
                    value[GROUP][0].update(change)
                with self.assertRaisesRegex(ValueError, "CLI startup recovery|Missing evidence file"):
                    self.checked_requirements(mutate)

    def test_missing_cases_helper_substitution_and_prior_group_ids_are_refused(self):
        def missing(value):
            value[GROUP] = []
        with self.assertRaisesRegex(ValueError, "CLI startup recovery scoped case inventory"):
            self.checked_requirements(missing)
        for tests in (
            [], acceptance_tests()[:-1],
            [{"file": "crates/b4-cli/tests/cli_support/mod.rs", "name": "private_modes"},
             *acceptance_tests()[1:]],
            [{"file": "crates/b4-cli/src/process_startup_tests.rs", "name": "startup_child_fixture"},
             *acceptance_tests()[1:]],
            [acceptance_tests()[0], *acceptance_tests()[:-1]],
        ):
            with self.subTest(tests=tests):
                def mutate(value):
                    value[GROUP][0]["acceptance_tests"] = tests
                with self.assertRaisesRegex(ValueError, "CLI startup recovery.*acceptance"):
                    self.checked_requirements(mutate)
        before = check_contracts.yaml.safe_load(
            (check_contracts.REPO / "spec/runtime/requirements.yaml").read_text())
        for previous, records in before.items():
            if previous != GROUP and previous.endswith("_requirements") and records:
                with self.subTest(previous=previous):
                    def mutate(value):
                        value[GROUP][0]["id"] = value[previous][0]["id"]
                    with self.assertRaisesRegex(ValueError, "Duplicate named local requirement IDs"):
                        self.checked_requirements(mutate)


if __name__ == "__main__":
    unittest.main()
