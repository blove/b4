"""Requirement-group enforcement checks, separate from native execution proof."""
import contextlib
import copy
import io
import unittest
from unittest.mock import patch

import check_contracts


class NativeCooperativeTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, missing=False, duplicate=False):
        original_load = check_contracts.yaml.safe_load

        def with_native_group(source):
            value = original_load(source)
            if isinstance(value, dict) and "foundation_requirements" in value:
                value = copy.deepcopy(value)
                value["native_cooperative_cancellation_requirements"] = [{
                    "id": "APCLIENT-001" if duplicate else "COOP-HOOK-001",
                    "verification_status": "not_run",
                    "acceptance_tests": [{
                        "file": "crates/b4-runtime/tests/application.rs",
                        "name": ("missing_native_cancellation_acceptance" if missing else
                                 "valid_registry_keeps_original_immutable_definition_and_exact_callback_context"),
                    }],
                }]
            return value

        output = io.StringIO()
        with patch.object(check_contracts.yaml, "safe_load", with_native_group), \
                contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        return output.getvalue()

    def test_new_group_has_separate_count_and_preserves_existing_scope(self):
        output = self.checked_requirements()
        self.assertIn("Native cooperative cancellation: 1 named local requirements", output)
        self.assertIn("Traceability: 338 named local requirements", output)
        self.assertIn("Unchanged client corpus: 8 named local requirements", output)
        self.assertIn("eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending", output)
        self.assertIn("CP-010 unresolved; M0 open", output)
        self.assertIn("named-test existence do not assert fresh test execution", output)

    def test_new_group_refuses_missing_acceptance_function(self):
        with self.assertRaisesRegex(ValueError, "Missing named local test"):
            self.checked_requirements(missing=True)

    def test_duplicate_between_native_and_client_groups_is_refused(self):
        with self.assertRaisesRegex(ValueError, "Duplicate named local requirement IDs"):
            self.checked_requirements(duplicate=True)


if __name__ == "__main__":
    unittest.main()
