#!/usr/bin/env python3
"""Qualify bounded attachment using the built local HTTP/SQLite example."""
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


def events(raw):
    result = []
    for block in raw.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
        names = [line[7:] for line in block.splitlines() if line.startswith("event: ")]
        data = [line[6:] for line in block.splitlines() if line.startswith("data: ")]
        if names:
            require(len(names) == 1, "Duplicate SSE event name")
            result.append((names[0], json.loads("\n".join(data))))
    return result



def inactive_frames(raw):
    frames = events(raw)
    require(len(frames) == 2 and frames[0][0] == "state" and frames[0][1]["live"] is False
            and frames[0][1]["turn"] is None and frames[-1] == ("done", {"output": None}),
            "Inactive attachment shape differs")
    blocks = [block for block in raw.decode("utf-8").replace("\r\n", "\n").split("\n\n")
              if block and not all(not line or line.startswith(":") for line in block.splitlines())]
    require(len(blocks) == 3 and blocks[1] == "retry: 2000"
            and not any(line.startswith("retry:") for block in [blocks[0], blocks[2]] for line in block.splitlines()),
            "Inactive attachment must order state, separate retry, then done")
    return frames[0][1]


def verify(binary):
    token = secrets.token_urlsafe(32)
    with tempfile.TemporaryDirectory(prefix="b4-attach-example-") as directory:
        def start():
            child = subprocess.Popen([binary], env=dict(os.environ, B4RUN_TOKEN=token,
                                     B4RUN_PORT="0", B4RUN_DATA_DIR=directory),
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            startup_line = ""
            try:
                require(bool(select.select([child.stdout], [], [], 8)[0]), "Example startup timed out")
                startup_line = child.stdout.readline()
                match = re.search(r"http://(127\.0\.0\.1):(\d+)", startup_line)
                require(match is not None, "Example startup failed; sensitive output suppressed")
                return child, startup_line, match[1], int(match[2])
            except BaseException:
                stop(child, startup_line)
                raise

        def stop(child, startup_line):
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=8)
            logs = startup_line + child.stdout.read() + child.stderr.read()
            child.stdout.close()
            child.stderr.close()
            require(token not in logs, "Example logged credentials")

        def request(method, path, body=None, auth=True):
            connection = http.client.HTTPConnection(host, port, timeout=8)
            try:
                connection.request(method, path, None if body is None else json.dumps(body),
                                   {"Authorization": "Bearer " + token} if auth else {})
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
            raise RuntimeError("Attachment example condition timed out")

        def inactive(path):
            status, headers, raw = request("GET", path + "/runs/stream")
            require(status == 200, "Inactive attachment refused")
            require(headers.get("cache-control") == "no-store, no-transform", "Attachment is cacheable")
            require(headers.get("x-accel-buffering") == "no", "Attachment permits proxy buffering")
            return inactive_frames(raw)

        child, startup_line, host, port = start()
        try:
            status, _, raw = request("POST", "/threads", {})
            require(status == 200, "Thread creation failed")
            thread = json.loads(raw)["thread_id"]
            path = "/threads/" + thread
            require(request("GET", path + "/runs/stream", auth=False)[0] == 401, "Unauthenticated attach allowed")
            require(request("GET", "/threads/missing/runs/stream")[0] == 404, "Missing attach differs")
            require(request("GET", path + "/runs/stream")[0] == 409, "Idle attach differs")
            with ThreadPoolExecutor(max_workers=1) as pool:
                producer = pool.submit(request, "POST", path + "/runs/wait",
                                       {"route": "/delay#workflow", "input": {"text": "雪"}})
                until(lambda: producer.done() or (snapshot(thread) or {}).get("attempts_used") == 1)
                require(not producer.done(), "Delayed producer settled before attachment")
                time.sleep(0.03)
                connection = http.client.HTTPConnection(host, port, timeout=2)
                try:
                    connection.request("GET", path + "/runs/stream", headers={"Authorization": "Bearer " + token})
                    response = connection.getresponse()
                    require(response.status == 200, "Live attachment refused")
                    initial = b""
                    while not initial.endswith(b"\n\n"):
                        line = response.readline()
                        require(bool(line), "Live attachment ended before initial state")
                        initial += line
                    state = events(initial)
                    require(len(state) == 1 and state[0][0] == "state" and state[0][1]["live"] is True
                            and state[0][1]["input"] == {"text": "雪"}, "Live state differs")
                    require(request("POST", path + "/cancel")[0] == 200, "Cancellation failed")
                    terminal = events(response.read())
                    require(terminal == [("done", {"output": {"cancelled": True}})], "Attached cancellation terminal differs")
                finally:
                    connection.close()
                require(producer.result(timeout=1)[0] == 409, "Wait cancellation did not settle promptly")
                require(snapshot(thread)["status"] == "cancel_requested", "Attachment falsely quiesced effect")
                require(inactive(path)["status"] == "busy", "Post-terminal drain advertised live")
                require(request("DELETE", path)[0] == 409, "Attachment released producer exclusion")
                until(lambda: snapshot(thread)["status"] == "cancelled")
                settled = snapshot(thread)
                require(settled["tasks"][0]["status"] == "succeeded" and settled["tasks"][0]["result"] is not None
                        and "calls" not in settled["values"] and settled.get("output") is None,
                        "Late outcome lost evidence or changed cancelled values")

            status, _, raw = request("POST", "/threads", {})
            require(status == 200, "Second thread creation failed")
            completed = json.loads(raw)["thread_id"]
            completed_path = "/threads/" + completed
            status, _, raw = request("POST", completed_path + "/runs/wait", {"route": "/false#workflow", "input": "persistent"})
            require(status == 200 and json.loads(raw) is False, "Falsy original output was lost")
            before = inactive(completed_path)
            persisted = snapshot(completed)
            require(request("GET", "/readyz")[0] == 200, "Attachment poisoned readiness")
        finally:
            stop(child, startup_line)
        child, startup_line, host, port = start()
        try:
            after = inactive(completed_path)
            require(after == before and snapshot(completed) == persisted, "Read-only completed attachment changed durable state")
            require(request("GET", "/readyz")[0] == 200, "Reopened attachment poisoned readiness")
        finally:
            stop(child, startup_line)
    return {"profile": "bounded_run_attachment_http_example", "passed": True,
            "transport": "real_loopback_tcp", "backend": "sqlite", "tokens_recorded": False,
            "checks": ["unauthenticated_missing_idle_refusals", "state_before_live_tail", "utf8_input_preserved",
                       "exact_attached_cancelled_done_before_quiescence", "postterminal_drain_nonlive_null_done",
                       "producer_exclusion_retained", "late_outcome_without_cancelled_state_reduction",
                       "inactive_retry_null_done", "completed_process_restart_readonly_state", "healthy_readiness",
                       "no_store_no_transform_no_proxy_buffering", "startup_and_stderr_credential_scan"],
            "limitations": ["Finite Pure script; viewer queue/capacity races qualified separately; no provider or full protocol claim."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/examples/serve")
    args = parser.parse_args()
    print(json.dumps(verify(str(args.binary.resolve())), indent=2))
