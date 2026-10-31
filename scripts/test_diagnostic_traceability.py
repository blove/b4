"""Qualify actual full-checker traceability of the new request-boundary scope."""
import contextlib
import copy
import io
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

import check_contracts

ROOT = Path(__file__).resolve().parents[1]


class DiagnosticTraceabilityChecks(unittest.TestCase):
    def test_full_checker_counts_the_separate_request_diagnostic_requirements(self):
        requirements = check_contracts.yaml.safe_load(
            (ROOT / "spec/runtime/requirements.yaml").read_text())
        self.assertEqual(len(requirements["request_diagnostic_requirements"]), 20)
        checked = subprocess.run(
            [sys.executable, "-B", str(ROOT / "scripts/check_contracts.py")],
            cwd=ROOT, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertIn("Traceability: 338 named local requirements", checked.stdout)
        self.assertIn("eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending", checked.stdout)
        self.assertIn("CP-010 unresolved; M0 open", checked.stdout)

    def test_full_checker_refuses_a_missing_named_diagnostic_acceptance_test(self):
        original_load = check_contracts.yaml.safe_load

        def altered_requirements(source):
            value = original_load(source)
            if isinstance(value, dict) and "request_diagnostic_requirements" in value:
                value = copy.deepcopy(value)
                value["request_diagnostic_requirements"][0]["acceptance_tests"][0]["name"] = (
                    "private_missing_diagnostic_test")
            return value

        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(check_contracts.yaml, "safe_load", altered_requirements), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with self.assertRaisesRegex(ValueError, "Missing named local test"):
                check_contracts.check_contracts()
        self.assertNotIn("Traceability:", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
