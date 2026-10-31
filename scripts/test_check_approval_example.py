"""Disclosure regression checks for the durable approval example verifier."""
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from check_approval_example import verify


class ApprovalExampleVerifierTests(unittest.TestCase):
    def receipt(self):
        return {
            "passed": True,
            "status": "completed",
            "effect_calls": 1,
            "attempts_used": 1,
            "pending_count": 1,
            "grant_and_prompt_restored": True,
            "thread_generation_restored": True,
            "accepted_before_reopen": True,
            "accepted_decisions": 1,
            "tokens_recorded": False,
            "limitations": ["Embedded SQLite fixture."],
        }

    def run_fixture(self, stdout, stderr=""):
        result = SimpleNamespace(returncode=0, stdout=stdout, stderr=stderr)
        with patch("check_approval_example.subprocess.run", return_value=result):
            return verify(Path("fixture-binary"))

    def test_valid_receipt_preserves_non_sensitive_output(self):
        receipt = self.receipt()
        self.assertEqual(self.run_fixture(json.dumps(receipt)), receipt)

    def test_escaped_token_is_rejected_after_json_decoding(self):
        for position in ("value", "key", "nested"):
            with self.subTest(position=position):
                receipt = self.receipt()
                token = "b4ag_" + "a" * 64
                if position == "value":
                    receipt["extra"] = token
                elif position == "key":
                    receipt[token] = "extra"
                else:
                    receipt["limitations"] = [{"extra": [token]}]
                stdout = json.dumps(receipt).replace("b4ag_", "b4ag\\u005f")
                with self.assertRaisesRegex(RuntimeError, "disclosed a grant token"):
                    self.run_fixture(stdout)

    def test_raw_token_in_stdout_or_stderr_is_rejected(self):
        receipt = self.receipt()
        receipt["extra"] = "b4ag_" + "a" * 64
        with self.assertRaisesRegex(RuntimeError, "disclosed a grant token"):
            self.run_fixture(json.dumps(receipt))
        with self.assertRaisesRegex(RuntimeError, "disclosed a grant token"):
            self.run_fixture(json.dumps(self.receipt()), "b4ag_" + "a" * 64)

    def test_incorrect_execution_evidence_is_rejected(self):
        receipt = self.receipt()
        receipt["effect_calls"] = 2
        with self.assertRaisesRegex(RuntimeError, "failed expectation: effect_calls"):
            self.run_fixture(json.dumps(receipt))


if __name__ == "__main__":
    unittest.main()
