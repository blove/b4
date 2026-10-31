"""Qualify local reference validation through the actual contract checker."""
from pathlib import Path
import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import check_contracts
import check_inspection_reports

ROOT = Path(__file__).resolve().parents[1]


class ContractReferenceChecks(unittest.TestCase):
    def test_v2_refs_into_frozen_v1_assembly_arrays_pass_the_full_checker(self):
        checked = subprocess.run(
            [sys.executable, "-B", str(ROOT / "scripts/check_contracts.py")],
            cwd=ROOT, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertIn("20 Draft202012 schemas", checked.stdout)
        self.assertIn("plus one native operational endpoint", checked.stdout)

    def test_full_checker_validates_separate_stopped_inspection_examples(self):
        checked = subprocess.run(
            [sys.executable, "-B", str(ROOT / "scripts/check_contracts.py")],
            cwd=ROOT, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertIn("Validated authored version1 and native-derived version2 stopped-inspection reports", checked.stdout)
        self.assertIn("20 Draft202012 schemas", checked.stdout)

    def test_full_checker_refuses_invalid_operator_report_without_changing_ap_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            examples = Path(directory)
            (examples / "summary.json").write_bytes(b'{"private-marker":"secret"}\n')
            (examples / "blocked.json").write_bytes(
                (check_inspection_reports.EXAMPLES / "blocked.json").read_bytes())
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch.object(check_inspection_reports, "EXAMPLES", examples), \
                    contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(ValueError, "Invalid stopped-inspection operator contract"):
                    check_contracts.check_contracts()
            self.assertEqual(stderr.getvalue(), "b4run_inspection_report_invalid\n")
            self.assertNotIn("private-marker", stdout.getvalue() + stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
