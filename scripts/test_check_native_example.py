"""The example verifier must suppress invalid descriptor contents in failures."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
from check_native_http_example import stop_child, capabilities

ROOT = Path(__file__).resolve().parents[1]


def v2_descriptor():
    value = json.loads((ROOT / "contracts/agent-protocol/examples/capabilities-native.json").read_text())
    value["descriptor_version"] = 2
    value["features"]["supervised_admission_drain"] = True
    value["readiness"]["aggregation"] = "store_readiness_and_no_owned_worker_fault_and_serving_admission"
    value["readiness"]["checks"].append("serving_admission")
    value["controls"].update(shutdown_default_wait_ms=30000, shutdown_max_wait_ms=300000)
    return value


class NativeVerifierPrivacy(unittest.TestCase):
    def test_current_example_verifier_accepts_strict_v2_with_local_schema_references(self):
        value = v2_descriptor()
        try:
            checked = capabilities(json.dumps(value).encode(), [])
        except RuntimeError:
            self.fail("Verifier refused the strict current V2 descriptor")
        self.assertEqual(checked, value)

    def test_current_example_verifier_rejects_historical_v1(self):
        value = (ROOT / "contracts/agent-protocol/examples/capabilities-native.json").read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Native descriptor schema differs"):
            capabilities(value, [])

    def test_cleanup_kills_and_waits_for_child_that_ignores_termination(self):
        child = subprocess.Popen([sys.executable, "-B", "-c",
                                  "import signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(30)"],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(child.stdout.readline().strip(), "ready")
        try:
            try:
                stop_child(child, "", "private-test-token")
            except subprocess.TimeoutExpired:
                pass
            self.assertIsNotNone(child.poll(), "Verifier left a live child after cleanup")
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=8)
            child.stdout.close()
            child.stderr.close()

    def test_schema_refusal_does_not_print_private_instance_in_stdout_or_stderr(self):
        descriptor = v2_descriptor()
        marker = "b4ag_PRIVATE_VERIFIER_SENTINEL"
        descriptor["unexpected_private"] = marker
        run = subprocess.run([sys.executable, "-B", "-c",
                              "import sys; sys.path.insert(0,'scripts'); from check_native_http_example import capabilities; capabilities(sys.stdin.buffer.read(), [])"],
                             input=json.dumps(descriptor), capture_output=True, text=True,
                             cwd=ROOT, timeout=8)
        self.assertNotEqual(run.returncode, 0)
        self.assertNotIn(marker, run.stdout + run.stderr)
        self.assertIn("Native descriptor schema differs", run.stderr)


if __name__ == "__main__":
    unittest.main()
