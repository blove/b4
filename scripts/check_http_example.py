#!/usr/bin/env python3
"""Check the built SQLite example over loopback, including a process restart."""
import argparse
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import select
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def verify(binary):
    if not __debug__:
        raise RuntimeError("Run this verifier without Python assertion optimization")
    token = secrets.token_urlsafe(32)
    settings = dict(os.environ)
    settings.pop("B4RUN_TOKEN", None)
    missing = subprocess.run([binary], env=settings, capture_output=True, text=True, timeout=5)
    assert missing.returncode != 0 and "B4RUN_TOKEN is required" in missing.stderr

    def start(data):
        env = dict(settings, B4RUN_TOKEN=token, B4RUN_PORT="0", B4RUN_DATA_DIR=data)
        child = subprocess.Popen([binary], env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        if not select.select([child.stdout], [], [], 8)[0]:
            child.terminate()
            child.wait(timeout=5)
            raise RuntimeError("Example startup timed out")
        line = child.stdout.readline()
        match = re.search(r"http://(127\.0\.0\.1):(\d+)", line)
        if not match:
            child.terminate()
            child.wait(timeout=5)
            raise RuntimeError("Example failed to start: " + line + child.stderr.read())
        return child, match[1], int(match[2])

    def stop(child):
        child.terminate()
        child.wait(timeout=8)
        child.stdout.close()
        child.stderr.close()

    def request(host, port, method, path, body=None, auth=True, extra=None):
        connection = http.client.HTTPConnection(host, port, timeout=5)
        headers = {"Authorization": "Bearer " + token} if auth else {}
        headers.update(extra or {})
        try:
            connection.request(method, path, body=None if body is None else json.dumps(body),
                               headers=headers)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    with tempfile.TemporaryDirectory(prefix="b4-http-example-") as data:
        child, host, port = start(data)
        try:
            assert request(host, port, "POST", "/threads", {}, auth=False)[0] == 401
            assert request(host, port, "POST", "/threads", {}, extra={"Origin": "https://foreign.invalid"})[0] == 403
            assert request(host, port, "POST", "/threads", {}, extra={"Host": "foreign.invalid"})[0] == 403
            assert request(host, port, "GET", "/readyz", auth=False)[0] == 200
            status, _, raw = request(host, port, "POST", "/threads", {"metadata": {"note": "persisted-example"}})
            assert status == 200
            path = "/threads/" + json.loads(raw)["thread_id"]
            status, _, raw = request(host, port, "POST", path + "/runs/wait", {"route": "/probe#workflow", "input": False})
            assert status == 200 and json.loads(raw) == {"input": False, "kind": "boolean"}
            status, _, raw = request(host, port, "POST", path + "/runs/stream", {"route": "/void#workflow"})
            assert status == 200 and raw == b"event: done\ndata: {}\n\n"
            status, _, failed_wait = request(host, port, "POST", path + "/runs/wait", {"route": "/fail#workflow"})
            failure_body = {"error": {"kind": "execution_error", "message": "fixture execution failure"}}
            assert status == 500 and json.loads(failed_wait) == failure_body
            assert request(host, port, "GET", "/readyz", auth=False)[0] == 200
            status, _, raw = request(host, port, "POST", path + "/runs/wait", {"route": "/zero#workflow"})
            assert status == 200 and json.loads(raw) == 0
            status, _, failed_stream = request(host, port, "POST", path + "/runs/stream", {"route": "/fail#workflow"})
            assert status == 200 and failed_stream == b'event: done\ndata: {"output":{"error":"fixture execution failure"}}\n\n'
            status, _, before = request(host, port, "GET", path)
            assert status == 200 and json.loads(before)["status"] == "idle"
            status, _, state = request(host, port, "GET", path + "/state")
            assert status == 200
            status, headers, pending = request(host, port, "GET", path + "/pending_interrupts")
            assert status == 200 and json.loads(pending) == {"interrupts": []} and headers["cache-control"] == "no-store"
        finally:
            stop(child)
        child, host, port = start(data)
        try:
            status, _, after = request(host, port, "GET", path)
            assert status == 200 and json.loads(before) == json.loads(after)
            status, _, reopened = request(host, port, "GET", path + "/state")
            assert status == 200 and json.loads(reopened) == json.loads(state)
        finally:
            stop(child)
    return {
        "profile": "authenticated_scripted_http_example", "passed": True,
        "transport": "real_loopback_tcp", "backend": "sqlite",
        "checks": ["missing_token_configuration_rejected", "unauthenticated_401", "origin_403",
                   "unexpected_host_403", "ready_200", "metadata_persisted", "probe_false_preserved",
                   "void_done_empty_object", "expected_failure_wait_500", "expected_failure_stream_done",
                   "expected_failure_keeps_readiness_healthy", "successor_after_failed_run",
                   "failed_thread_idle", "pending_no_store", "thread_and_state_preserved_after_process_restart"],
        "wait_output": {"input": False, "kind": "boolean"},
        "void_stream_utf8": "event: done\ndata: {}\n\n",
        "failure_wait_output": failure_body,
        "failure_stream_utf8": failed_stream.decode("utf-8"),
        "reopened_metadata": json.loads(after)["metadata"], "tokens_recorded": False,
        "limitations": ["Settled scripted execution restart only; no unfinished-run automatic recovery or crash qualification."],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/examples/serve")
    args = parser.parse_args()
    print(json.dumps(verify(str(args.binary.resolve())), indent=2))
