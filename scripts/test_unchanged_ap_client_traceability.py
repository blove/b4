"""Named caller-group traceability checks do not assert actual execution."""
import contextlib
import copy
import io
from pathlib import Path
import unittest
from unittest.mock import patch

import check_contracts

ROOT = Path(__file__).resolve().parents[1]
GROUPS = [
    ("C1_factory_method_map", "controller-cases.mjs"),
    ("C2_controller_auth", "controller-cases.mjs"),
    ("C3_wait_terminals", "controller-cases.mjs"),
    ("C4_held_controller", "controller-cases.mjs"),
    ("W1_worker_crud_auth", "worker-cases.mjs"),
    ("W2_finite_sse", "worker-cases.mjs"),
    ("W3_attachment_cancel", "worker-cases.mjs"),
    ("W4_durable_park_refusal", "worker-cases.mjs"),
]


class UnchangedClientTraceabilityChecks(unittest.TestCase):
    def checked_requirements(self, missing=False):
        original_load = check_contracts.yaml.safe_load

        def with_client_groups(source):
            value = original_load(source)
            if isinstance(value, dict) and "foundation_requirements" in value:
                value = copy.deepcopy(value)
                value["unchanged_ap_client_requirements"] = [
                    {"id": f"APCLIENT-{index:03}", "verification_status": "not_run",
                     "acceptance_tests": [{
                         "file": "conformance/reference/client-fixture/" + filename,
                         "name": name,
                     }]}
                    for index, (name, filename) in enumerate(GROUPS, 1)
                ]
                if missing:
                    value["unchanged_ap_client_requirements"][0]["acceptance_tests"][0]["name"] = (
                        "private_missing_original_client_group")
            return value

        output = io.StringIO()
        with patch.object(check_contracts.yaml, "safe_load", with_client_groups), \
                contextlib.redirect_stdout(output):
            check_contracts.check_contracts()
        return output.getvalue()

    def test_full_checker_recognizes_eight_named_original_mjs_caller_groups(self):
        output = self.checked_requirements()
        self.assertIn("Unchanged client corpus: 8 named local requirements", output)
        self.assertIn("Traceability: 338 named local requirements", output)
        self.assertIn("eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending", output)
        self.assertIn("CP-010 unresolved; M0 open", output)
        self.assertIn("named-test existence do not assert fresh test execution", output)

    def test_full_checker_refuses_a_missing_named_original_caller_group(self):
        with self.assertRaisesRegex(ValueError, "Missing named local test"):
            self.checked_requirements(missing=True)


if __name__ == "__main__":
    unittest.main()
