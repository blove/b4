"""Startup preparation trace checks validate metadata, independently of runtime execution."""
import contextlib
import copy
import io
import unittest
from unittest.mock import patch

import check_contracts


class StartupRecoveryPreparationTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, missing=False, duplicate_group=None):
        original_load = check_contracts.yaml.safe_load

        def with_startup_group(source):
            value = original_load(source)
            if isinstance(value, dict) and "foundation_requirements" in value:
                value = copy.deepcopy(value)
                identity = (value[duplicate_group][0]["id"] if duplicate_group
                            else "STARTUP-HOOK-001")
                value["startup_recovery_preparation_requirements"] = [{
                    "id": identity,
                    "verification_status": "not_run",
                    "acceptance_tests": [{
                        "file": "crates/b4-runtime/tests/application.rs",
                        "name": ("missing_startup_recovery_preparation_acceptance" if missing else
                                 "valid_registry_keeps_original_immutable_definition_and_exact_callback_context"),
                    }],
                }]
            return value

        output = io.StringIO()
        with patch.object(check_contracts.yaml, "safe_load", with_startup_group), \
                contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        return output.getvalue()

    def test_new_group_has_separate_count_and_preserves_previous_groups(self):
        output = self.checked_requirements()
        self.assertIn("Startup recovery preparation: 1 named local requirements", output)
        self.assertIn("Traceability: 338 named local requirements", output)
        self.assertIn("Unchanged client corpus: 8 named local requirements", output)
        self.assertIn("Native cooperative cancellation: 15 named local requirements", output)
        self.assertIn("Unfinished discovery: 19 named local requirements", output)
        self.assertIn("eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending", output)
        self.assertIn("CP-010 unresolved; M0 open", output)
        self.assertIn("named-test existence do not assert fresh test execution", output)

    def test_new_group_refuses_missing_acceptance_function(self):
        with self.assertRaisesRegex(ValueError, "Missing named local test"):
            self.checked_requirements(missing=True)

    def test_duplicate_between_startup_and_each_previous_group_is_refused(self):
        for group in ["foundation_requirements", "unchanged_ap_client_requirements",
                      "native_cooperative_cancellation_requirements",
                      "unfinished_discovery_requirements"]:
            with self.subTest(group=group):
                with self.assertRaisesRegex(ValueError, "Duplicate named local requirement IDs"):
                    self.checked_requirements(duplicate_group=group)


if __name__ == "__main__":
    unittest.main()
