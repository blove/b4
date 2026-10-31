#!/usr/bin/env python3
"""Verify the built, no-provider durable approval example without recording tokens."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def verify(binary):
    with tempfile.TemporaryDirectory(prefix="b4-approval-example-") as directory:
        result = subprocess.run([str(binary), directory, "qualified-approval-run"],
                                capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError("Approval example failed; sensitive output suppressed")
        if "b4ag_" in result.stdout or "b4ag_" in result.stderr:
            raise RuntimeError("Approval example disclosed a grant token")
        receipt = json.loads(result.stdout)
        if "b4ag_" in json.dumps(receipt, ensure_ascii=False):
            raise RuntimeError("Approval example disclosed a grant token")
        expected = {
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
        }
        for field, value in expected.items():
            if receipt.get(field) != value:
                raise RuntimeError(f"Approval example failed expectation: {field}")
        return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/examples/approvals")
    args = parser.parse_args()
    print(json.dumps(verify(args.binary.resolve()), indent=2))
