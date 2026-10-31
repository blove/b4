#!/usr/bin/env python3
"""Verify trusted embedding restart continuation without disclosing grant output."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def verify(binary):
    with tempfile.TemporaryDirectory(prefix="b4-host-recovery-example-") as directory:
        result = subprocess.run([str(binary), directory], capture_output=True,
                                text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError("Recovery example failed; sensitive output suppressed")
        if "b4ag_" in result.stdout or "b4ag_" in result.stderr:
            raise RuntimeError("Recovery example disclosed authority")
        receipt = json.loads(result.stdout)
        if "b4ag_" in json.dumps(receipt, ensure_ascii=False):
            raise RuntimeError("Recovery example disclosed authority")
        expected = {
            "profile":"trusted_known_id_host_recovery_example",
            "passed":True, "status":"completed", "attempts_used":1,
            "accepted_before_reopen":True,
            "original_grant_and_audit_preserved":True,
            "continued_after_original_ttl":True,
            "native_done_count":1, "tokens_recorded":False,
            "transport":"embedding_response_body",
        }
        for field, value in expected.items():
            if receipt.get(field) != value:
                raise RuntimeError(f"Recovery example failed expectation: {field}")
        return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/examples/recover_approvals")
    args = parser.parse_args()
    print(json.dumps(verify(args.binary.resolve()), indent=2))
