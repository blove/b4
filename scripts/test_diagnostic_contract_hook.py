"""Qualify the actual full checker's separate embedding-diagnostics hook."""
import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import check_contracts
import check_diagnostic_batches

ROOT = Path(__file__).resolve().parents[1]


class DiagnosticContractHookChecks(unittest.TestCase):
    def test_full_checker_validates_separate_authored_diagnostic_batches(self):
        checked = subprocess.run(
            [sys.executable, "-B", str(ROOT / "scripts/check_contracts.py")],
            cwd=ROOT, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertIn("Validated three authored request-diagnostic batches", checked.stdout)
        self.assertIn("20 Draft202012 schemas", checked.stdout)
        self.assertIn("23 authored examples", checked.stdout)
        self.assertIn("plus one native operational endpoint", checked.stdout)

    def test_full_checker_refuses_invalid_diagnostic_batch_with_static_output(self):
        with tempfile.TemporaryDirectory() as directory:
            examples = Path(directory)
            for name in ["empty.json", "response.json", "dropped.json"]:
                (examples / name).write_bytes(
                    (check_diagnostic_batches.EXAMPLES / name).read_bytes())
            (examples / "response.json").write_bytes(
                b'{"private-hook-marker":"secret-value"}')
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch.object(check_diagnostic_batches, "EXAMPLES", examples), \
                    contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(ValueError, "Invalid request-diagnostic embedding contract"):
                    check_contracts.check_contracts()
            self.assertEqual(stderr.getvalue(), "b4run_diagnostic_batch_invalid\n")
            self.assertNotIn("private-hook-marker", stdout.getvalue() + stderr.getvalue())
            self.assertNotIn("secret-value", stdout.getvalue() + stderr.getvalue())
            self.assertNotIn("20 Draft202012 schemas", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
