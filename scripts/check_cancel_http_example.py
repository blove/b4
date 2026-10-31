#!/usr/bin/env python3
"""Verify owned cancellation through the built local SQLite example."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import select
import sqlite3
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify(binary):
    token = secrets.token_urlsafe(32)
    with tempfile.TemporaryDirectory(prefix="b4-cancel-example-") as directory:
        env = dict(os.environ, B4RUN_TOKEN=token, B4RUN_PORT="0", B4RUN_DATA_DIR=directory)
        child = subprocess.Popen([binary], env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        startup_line = ""
        try:
            require(bool(select.select([child.stdout], [], [], 8)[0]), "Example startup timed out")
            startup_line = child.stdout.readline()
            match = re.search(r"http://(127\.0\.0\.1):(\d+)", startup_line)
            require(match is not None, "Example startup failed; sensitive output suppressed")
            host, port = match[1], int(match[2])

            def request(method, path, body=None):
                connection = http.client.HTTPConnection(host, port, timeout=8)
                try:
                    connection.request(method, path, None if body is None else json.dumps(body),
                                       {"Authorization": "Bearer " + token})
                    response = connection.getresponse()
                    return response.status, dict(response.getheaders()), response.read()
                finally:
                    connection.close()

            def snapshot(thread):
                with sqlite3.connect(Path(directory) / "runtime.sqlite3") as database:
                    row = database.execute("SELECT e.snapshot FROM executions e JOIN threads t "
                                           "ON e.id=t.active_execution_id WHERE t.id=?", (thread,)).fetchone()
                    return None if row is None else json.loads(row[0])

            def until(predicate):
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if predicate():
                        return
                    time.sleep(0.005)
                raise RuntimeError("Cancellation example condition timed out")

            for mode in ("wait", "stream"):
                status, _, raw = request("POST", "/threads", {})
                require(status == 200, "Thread creation failed")
                thread = json.loads(raw)["thread_id"]
                path = "/threads/" + thread
                with ThreadPoolExecutor(max_workers=1) as pool:
                    producer = pool.submit(request, "POST", path + "/runs/" + mode,
                                           {"route": "/delay#workflow", "input": mode})
                    until(lambda: producer.done() or (snapshot(thread) or {}).get("attempts_used") == 1)
                    require(not producer.done(), "Delayed run failed to remain active")
                    time.sleep(0.03)
                    status, headers, raw = request("POST", path + "/cancel")
                    require(status == 200 and json.loads(raw) == {"status":"interrupted", "thread_id":thread},
                            "Cancellation receipt differs")
                    require(headers.get("cache-control") == "no-store", "Cancellation receipt is cacheable")
                    status, _, raw = producer.result(timeout=1)
                    if mode == "wait":
                        require(status == 409 and json.loads(raw)["error"]["details"]["code"] == "run_cancelled",
                                "Wait cancellation terminal differs")
                    else:
                        require(status == 200 and raw == b'event: done\ndata: {"output":{"cancelled":true}}\n\n',
                                "Stream cancellation terminal differs")
                    require(snapshot(thread)["status"] == "cancel_requested", "Prompt terminal falsely settled live work")
                    require(request("DELETE", path)[0] == 409, "Deletion released a draining worker")
                    require(request("POST", path + "/runs/wait", {"route":"/void#workflow"})[0] == 409,
                            "Admission released a draining worker")
                    until(lambda: snapshot(thread)["status"] == "cancelled")
                    settled = snapshot(thread)
                    require(settled["tasks"][0]["status"] == "succeeded" and settled["tasks"][0]["result"] is not None,
                            "Actual late outcome evidence was lost")
                    require("calls" not in settled["values"] and settled.get("output") is None,
                            "Late result changed cancelled state")
                    require(request("GET", "/readyz")[0] == 200, "Cancellation poisoned readiness")

                    def deletion_finished():
                        response = request("DELETE", path)
                        require(response[0] in (204, 409), "Deletion after drain failed")
                        if response[0] == 204:
                            require(response[2] == b"", "Deletion body is not empty")
                            return True
                        return False

                    until(deletion_finished)
        finally:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=8)
            logs = startup_line + child.stdout.read() + child.stderr.read()
            child.stdout.close()
            child.stderr.close()
            require(token not in logs, "Example logged credentials")
    return {"profile":"owned_run_cancellation_http_example", "passed":True,
            "transport":"real_loopback_tcp", "backend":"sqlite", "tokens_recorded":False,
            "checks":["wait_409_run_cancelled", "stream_one_cancelled_done", "prompt_receipt_before_quiescence",
                      "draining_deletion_and_admission_refused", "late_result_evidence_retained_without_state_reduction",
                      "healthy_readiness", "deletion_after_quiescence"],
            "limitations":["Finite delayed Pure script; no arbitrary callback interruption, abrupt restart or full protocol qualification."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/examples/serve")
    args = parser.parse_args()
    print(json.dumps(verify(str(args.binary.resolve())), indent=2))
