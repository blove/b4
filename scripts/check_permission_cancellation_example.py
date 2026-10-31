#!/usr/bin/env python3
"""Qualify an already-built serve-cancellation example using owned local data.

Usage: python3 -B scripts/check_permission_cancellation_example.py --example /absolute/binary
Standard library only. Never builds, fetches, seeds, or writes SQL. SQLite is
inspected only after normal SIGINT retirement and listener/owner-lock release.
This proves actual entered cancellation, not deterministic pre-entry withdrawal.
"""
import argparse
import errno
import fcntl
import http.client
import json
import os
from pathlib import Path
import secrets
import selectors
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid

MODE = "serve-cancellation"
WAIT = 10
HTTP_WAIT = 40
DRAIN = 45  # A normal interrupt may need the full 30-second callback expiry.
LIMIT = 1024 * 1024
APP = "07d7d468-c4f9-47e1-b421-ff889251df80"
APPLICATION = {"application": APP, "revision": "application-cancellation-1"}
ROUTE = "/reports#agent"
TASK = "publish-monthly-report"
EFFECT = "reports.publish-monthly"
ORIGIN = {"application": APP, "tenant": "tenant", "principal": "owner"}
PROMPT = {"question": "Publish monthly report?"}
SPEC = {"id": TASK, "effect_id": EFFECT, "recovery": "pure", "requires_approval": True}
DEFINITION = {"route": ROUTE, "revision": "definition-1", "batches": [[SPEC]], "max_attempts": 1}
BINDING = {"effect_id": EFFECT, "tool_id": "release-tool", "tool_revision": "tool-1",
           "constraint_revision": "constraint-1"}
PROVENANCE = ("approval_grants", "approval_acceptances", "permission_acceptances", "permission_decisions")
QUERIES = {
    "metadata": "SELECT id,schema_version,migration_checksum,owner_epoch FROM metadata ORDER BY id",
    "threads": "SELECT id,active_execution_id,created_at_ms,metadata,origin,resource_revision,generation FROM threads ORDER BY id",
    "executions": "SELECT id,thread_id,revision,owner_epoch,snapshot FROM executions ORDER BY id",
    "journal": "SELECT sequence,kind,thread_id,execution_id,revision,owner_epoch,status,updated_at_ms,payload FROM journal ORDER BY sequence",
    "approval_grants": "SELECT execution_id,interrupt_id,task_id,thread_id,thread_generation,task_parking_revision,consumed,record FROM approval_grants ORDER BY execution_id,interrupt_id",
    "approval_acceptances": "SELECT execution_id,turn_id,thread_id,thread_generation,previous_revision,definition_revision,accepted_at_ms,record FROM approval_acceptances ORDER BY execution_id,turn_id",
    "permission_acceptances": "SELECT execution_id,turn_id,thread_id,thread_generation,previous_revision,committed_revision,record_version,record FROM permission_acceptances ORDER BY execution_id,turn_id",
    "permission_decisions": "SELECT permission_id,thread_id,thread_generation,scope_digest,record_version,record FROM permission_decisions ORDER BY permission_id",
    "permission_uses": "SELECT execution_id,task_id,use_id,thread_id,thread_generation,permission_id,turn_id,authorization_revision,record_version,record FROM permission_uses ORDER BY execution_id,task_id",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def integer(value):
    return int.from_bytes(value, "big") if isinstance(value, bytes) else value


def resource_id(value):
    require(isinstance(value, str) and str(uuid.UUID(value)) == value, "Expected canonical resource UUID")
    return value


class Child:
    """Drain both pipes concurrently, retain bounded bytes, consume each JSON line."""

    def __init__(self, binary, mode, env):
        self.process = subprocess.Popen([str(binary), mode], env=env, start_new_session=True,
                                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
        self.selector = selectors.DefaultSelector()
        self.buffers = {"stdout": bytearray(), "stderr": bytearray()}
        self.cursor = 0
        self.closed = False
        self.normal = False
        for name in self.buffers:
            pipe = getattr(self.process, name)
            os.set_blocking(pipe.fileno(), False)
            self.selector.register(pipe, selectors.EVENT_READ, name)

    def pump(self, deadline):
        remaining = deadline - time.monotonic()
        require(remaining > 0, "Child operation exceeded its finite deadline")
        for key, _ in self.selector.select(min(remaining, 0.1)):
            data = os.read(key.fileobj.fileno(), 65536)
            if not data:
                self.selector.unregister(key.fileobj)
            else:
                available = LIMIT - sum(map(len, self.buffers.values()))
                self.buffers[key.data].extend(data[:max(0, available)])
                require(len(data) <= available, "Child output exceeded byte bound; output suppressed")

    def event(self):
        deadline = time.monotonic() + WAIT
        while True:
            end = self.buffers["stdout"].find(b"\n", self.cursor)
            if end >= 0:
                raw = bytes(self.buffers["stdout"][self.cursor:end])
                self.cursor = end + 1
                require(len(raw) <= 4096, "Child event exceeded line bound")
                try:
                    event = json.loads(raw)
                except (ValueError, UnicodeError):
                    raise RuntimeError("Child event is invalid JSON; output suppressed") from None
                require(isinstance(event, dict), "Child event must be a JSON object")
                return event
            if self.process.poll() is not None and not self.selector.get_map():
                # Exact static diagnostic permits an old binary's intended RED without leaking output.
                unknown = b"usage: serve_permission_reuse seed|serve|serve-approvals"
                require(unknown not in self.buffers["stderr"],
                        "serve-cancellation mode unavailable: example rejected unknown mode before readiness")
                raise RuntimeError("Child exited before expected event; output suppressed")
            self.pump(deadline)

    def finish(self, interrupt=False):
        if interrupt:
            require(self.process.poll() is None, "Owner exited before normal SIGINT retirement")
            self.process.send_signal(signal.SIGINT)
        deadline = time.monotonic() + (DRAIN if interrupt else WAIT)
        while self.process.poll() is None or self.selector.get_map():
            self.pump(deadline)
        code = self.process.wait(timeout=max(0.01, deadline - time.monotonic()))
        self.normal = True
        return code

    def cleanup(self):
        forced = not self.normal
        try:
            if forced:
                # Session was created by this probe. Kill its entire group even if leader exited.
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            self.process.wait(timeout=WAIT)
        finally:
            self.selector.close()
            self.process.stdout.close()
            self.process.stderr.close()
            self.closed = True
        return forced


class DeadlineSocket(socket.socket):
    """Every socket receive uses the same absolute deadline, including header reads."""

    def __init__(self):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM)
        self.deadline = time.monotonic() + HTTP_WAIT

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("HTTP total deadline expired")
        self.settimeout(remaining)

    def connect(self, address):
        self.remaining()
        return super().connect(address)

    def sendall(self, data, flags=0):
        self.remaining()
        return super().sendall(data, flags)

    def recv_into(self, buffer, nbytes=0, flags=0):
        self.remaining()
        return super().recv_into(buffer, nbytes, flags)


class Exchange:
    """Finite HTTP worker; explicit socket closure and finite joins on every path."""

    def __init__(self, address, method, path, body=None, token=None, background=False):
        self.sock = DeadlineSocket()
        self.address, self.method, self.path = address, method, path
        self.body, self.token = body, token
        self.result = None
        self.failed = False
        self.thread = None
        if background:
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
        else:
            self.run()

    def run(self):
        connection = http.client.HTTPConnection(*self.address, timeout=HTTP_WAIT)
        connection.sock = self.sock
        try:
            self.sock.connect(self.address)  # Numeric loopback; no DNS or unbounded resolver.
            headers = {"Accept": "text/event-stream, application/json", "Connection": "close"}
            if self.token is not None:
                headers["Authorization"] = "Bearer " + self.token
            if self.body is not None:
                headers["Content-Type"] = "application/json"
            connection.request(self.method, self.path,
                               None if self.body is None else json.dumps(self.body), headers)
            response = connection.getresponse()
            raw = response.read(LIMIT + 1)
            require(len(raw) <= LIMIT, "HTTP response exceeded byte bound")
            self.result = (response.status, {k.lower(): v for k, v in response.getheaders()}, raw)
        except Exception:
            # Worker errors must never carry HTTP payloads, input or credentials into logs.
            self.failed = True
        finally:
            connection.close()
            self.sock.close()

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        if self.thread is not None:
            self.thread.join(WAIT)
            require(not self.thread.is_alive(), "HTTP cleanup worker failed finite join")

    def get(self):
        if self.thread is not None:
            self.thread.join(max(0.01, self.sock.deadline - time.monotonic()) + 1)
            if self.thread.is_alive():
                self.close()
                raise RuntimeError("HTTP worker exceeded total deadline")
        require(not self.failed and self.result is not None, "HTTP exchange failed; payload suppressed")
        return self.result


def decoded(exchange):
    status, headers, raw = exchange.get()
    return status, headers, json.loads(raw)


def ready_address(child):
    event = child.event()
    require(set(event) == {"status", "address"} and event["status"] == "ready",
            "Readiness JSON contract differs")
    host, separator, port = event["address"].rpartition(":")
    require(separator and host == "127.0.0.1" and port.isdigit() and 0 < int(port) <= 65535,
            "Readiness must report actual ephemeral loopback address")
    return host, int(port)


def listener_retired(address):
    try:
        with socket.create_connection(address, timeout=1):
            pass
    except OSError as error:
        require(error.errno == errno.ECONNREFUSED, "Retired listener did not refuse connection")
    else:
        raise RuntimeError("Listener still accepts after normal owner exit")


def capture(directory, child):
    require(child.normal and child.process.returncode == 0, "SQL inspection requires normal successful retirement")
    with (directory / "owner.lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            database = (directory / "runtime.sqlite3").resolve()
            require(database.is_file(), "Retired database is absent")
            connection = sqlite3.connect(str(database), timeout=1)
            try:
                deadline = time.monotonic() + WAIT
                connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
                connection.execute("PRAGMA query_only=ON")
                require(connection.execute("PRAGMA query_only").fetchall() == [(1,)], "Query-only inspection unavailable")
                require(connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)], "Database integrity failed")
                tables = {}
                for name, sql in QUERIES.items():
                    rows = connection.execute(sql).fetchmany(101)
                    require(len(rows) <= 100, "Durable table exceeded focused acceptance row bound")
                    tables[name] = rows
                return tables
            finally:
                connection.close()
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def journal(raw, execution, owner, statuses):
    rows = [row for row in raw["journal"] if row[3] == execution]
    require(len(rows) == len(statuses), "Execution journal version count differs")
    times = []
    for revision, (row, status) in enumerate(zip(rows, statuses)):
        timestamp = integer(row[7])
        require(row[1:4] == ("admit" if revision == 0 else "commit", raw["threads"][0][0], execution)
                and integer(row[4]) == revision and integer(row[5]) == owner
                and json.loads(row[6]) == status
                and json.loads(row[8]) == {"execution_id": execution, "revision": revision,
                     "owner_generation": owner, "status": status, "updated_at_ms": timestamp},
                "Journal typed body/columns differ from exact execution versions")
        if revision:
            require(row[0] == rows[revision - 1][0] + 1, "Execution journal sequences must be consecutive")
        times.append(timestamp)
    require(times == sorted(times), "Journal timestamps regressed")
    return times


def execution_state(raw, execution, thread, owner, input_value, original=False, cancelled=True):
    row = next(row for row in raw["executions"] if row[0] == execution)
    s = json.loads(row[4])
    revision = 6 if original else (5 if cancelled else 4)
    statuses = ["accepted", "running"] + (["waiting_for_approval"] if original else []) + ["running", "running"]
    statuses += ["cancel_requested", "cancelled"] if cancelled else ["completed"]
    times = journal(raw, execution, owner, statuses)
    require(row[1] == thread and integer(row[2]) == s["revision"] == revision
            and integer(row[3]) == s["owner_generation"] == owner
            and s["execution_id"] == execution and s["definition"] == DEFINITION
            and s["status"] == ("cancelled" if cancelled else "completed")
            and s["attempts_used"] == 1 and len(s["attempt_history"]) == 1
            and s["values"] == {"input": input_value} and s["failure"] is None
            and s["updated_at_ms"] == times[-1], "Execution snapshot differs from exact terminal state")
    resource_id(s["turn_id"])
    resource_id(s["attempt_history"][0])
    require(len(s["tasks"]) == 1, "Expected exactly one real task")
    task = s["tasks"][0]
    require(task["spec"] == SPEC and task["approved"] is True
            and task["input"] == {"input": input_value}
            and task["attempt_id"] == s["attempt_history"][0]
            and task.get("failure") is None, "Task lost its genuine input/attempt/approval")
    if cancelled:
        # Actual typed CancelAttempt acknowledgement: cancelled task, retained attempt, no receipt/result.
        require(s.get("output") is None and task["status"] == "cancelled" and task["result"] is None,
                "Cancellation fabricated output/result or failed to retain CancelAttempt acknowledgement")
    else:
        require(s["output"] == {"input": input_value} and task["status"] == "succeeded"
                and task["result"] == {"output": {"published": True}, "writes": []},
                "Normal saved reuse must retain actual literal success")
    return s, times


def original_state(raw, thread, pending, parked, input_value):
    require(set(raw) == set(QUERIES) and len(raw["metadata"]) == len(raw["threads"])
            == len(raw["executions"]) == len(raw["approval_grants"])
            == len(raw["permission_acceptances"]) == len(raw["permission_decisions"]) == 1
            and len(raw["journal"]) == 8 and not raw["approval_acceptances"]
            and not raw["permission_uses"], "Original table cardinalities differ")
    t = raw["threads"][0]
    execution = resource_id(raw["executions"][0][0])
    s, times = execution_state(raw, execution, thread, 1, input_value, original=True)
    require(integer(raw["metadata"][0][3]) == 1 and t[0] == thread and t[1] == execution
            and integer(t[5]) == 7 and json.loads(t[3]) == {} and json.loads(t[4]) == ORIGIN
            and isinstance(t[6], str) and len(t[6]) == 64
            and all(c in "0123456789abcdef" for c in t[6])
            and parked["config"]["configurable"]["checkpoint_id"] == execution
            and parked["config"]["configurable"]["thread_id"] == thread,
            "Original resource/generation/parked execution differs")
    creation = raw["journal"][0]
    require(creation[1:5] == ("thread", thread, None, None) and integer(creation[5]) == 1
            and creation[6] is None and creation[8] is None
            and integer(creation[7]) == integer(t[2]) and times[0] >= integer(t[2])
            and raw["journal"][1][0] == creation[0] + 1, "Thread creation journal differs")
    grow, arow, drow = (raw[name][0] for name in ("approval_grants", "permission_acceptances", "permission_decisions"))
    g, a, d = json.loads(grow[7]), json.loads(arow[7]), json.loads(drow[5])
    require(grow[:5] == (execution, pending["interruptId"], TASK, thread, t[6])
            and integer(grow[5]) == 2 and grow[6] == 1 and g["consumed"] is True
            and g["execution_id"] == execution and g["interrupt_id"] == pending["interruptId"]
            and g["token"] == pending["grant"] and g["resume_key"] == pending["resumeKey"]
            and g["thread_id"] == thread and g["thread_generation"] == t[6]
            and g["task_id"] == TASK and g["effect_id"] == EFFECT and g["route"] == ROUTE
            and g["definition_revision"] == "definition-1" and g["policy_revision"] == "policy-1"
            and g["origin"] == ORIGIN and g["task_parking_revision"] == 2 and g["prompt"] == PROMPT,
            "Consumed grant no longer matches actual HTTP pending approval")
    before = a["before_snapshot"]
    require(arow[:4] == (execution, s["turn_id"], thread, t[6]) and integer(arow[4]) == 2
            and integer(arow[5]) == 3 and arow[6] == 1 and a["record_version"] == 1
            and a["thread_id"] == thread and a["thread_generation"] == t[6]
            and a["turn_id"] == s["turn_id"] and a["origin"] == ORIGIN
            and a["application"] == APPLICATION and a["policy_revision"] == "policy-1"
            and a["committed_revision"] == 3 and a["facts"] == {"now_ms": times[3]}
            and before["execution_id"] == execution and before["revision"] == 2
            and before["status"] == "waiting_for_approval" and before["owner_generation"] == 1
            and before["definition"] == DEFINITION and before["values"] == s["values"]
            and before["attempts_used"] == 0 and before["attempt_history"] == []
            and before["updated_at_ms"] == times[2] and len(before["tasks"]) == 1,
            "Always acceptance lost original Waiting2/commit3 provenance")
    resource_id(before["turn_id"])
    require(before["turn_id"] != s["turn_id"] and before["turn_history"] == [before["turn_id"]]
            and s["turn_history"] == [before["turn_id"], s["turn_id"]], "Resume must create a fresh actual turn")
    task = before["tasks"][0]
    require(task["spec"] == SPEC and task["status"] == "waiting_for_approval"
            and task["approved"] is False and task["attempt_id"] is None and task["result"] is None
            and task["input"] == {"input": input_value}
            and task["interrupt"] == {"id": pending["interruptId"], "prompt": PROMPT}
            and s["tasks"][0]["interrupt"] == task["interrupt"], "Original parked task provenance differs")
    require(a["decisions"] == [{"task_id": TASK, "interrupt_id": pending["interruptId"],
                "grant_ref": {"execution_id": execution, "interrupt_id": pending["interruptId"]},
                "decision": "always", "proposal_digest": g["proposal_digest"],
                "task_parking_revision": 2, "binding": BINDING, "permission_id": d["permission_id"]}],
            "Always acceptance must link the actual saved decision and binding")
    require(drow[:3] == (d["permission_id"], thread, t[6]) and drow[3] == d["scope_digest"]
            and drow[4] == d["record_version"] == 1 and d["creator_execution_id"] == execution
            and d["creator_turn_id"] == s["turn_id"] and d["creator_task_id"] == TASK
            and d["creator_interrupt_id"] == pending["interruptId"]
            and d["scope"] == {"thread_id": thread, "thread_generation": t[6], "origin": ORIGIN,
                "application": APPLICATION, "route": ROUTE, "definition": DEFINITION,
                **BINDING, "policy_revision": "policy-1"}, "Saved permission scope/creator provenance differs")
    return d


def reused_state(seed, previous, current, thread, decision, owner, input_value, cancelled):
    require(all(current[name] == seed[name] for name in PROVENANCE), "Original immutable provenance changed")
    require(len(current["metadata"]) == len(current["threads"]) == 1
            and current["metadata"][0][:3] == seed["metadata"][0][:3]
            and integer(current["metadata"][0][3]) == owner, "Reopened owner epoch differs")
    require(current["journal"][:len(previous["journal"])] == previous["journal"], "Existing journal changed")
    old = {r[0]: r for r in previous["executions"]}
    new = {r[0]: r for r in current["executions"]}
    require(len(new) == len(old) + 1 and all(new.get(k) == v for k, v in old.items()), "Previous execution changed")
    execution = resource_id(next(iter(new.keys() - old.keys())))
    s, times = execution_state(current, execution, thread, owner, input_value, cancelled=cancelled)
    require(s["turn_history"] == [s["turn_id"]], "NewUse must begin a fresh actual turn")
    old_uses = {r[2]: r for r in previous["permission_uses"]}
    uses = {r[2]: r for r in current["permission_uses"]}
    require(len(uses) == len(old_uses) + 1 and all(uses.get(k) == v for k, v in old_uses.items()),
            "NewUse did not append exactly one immutable fresh Use")
    u = uses[next(iter(uses.keys() - old_uses.keys()))]
    record = json.loads(u[9])
    before = record["before_snapshot"]
    require(u[:2] == (execution, TASK) and u[3:7] == (thread, seed["threads"][0][6], decision["permission_id"], s["turn_id"])
            and integer(u[7]) == 2 and u[8] == 1 and record["use_id"] == u[2]
            and record["permission_id"] == u[5] and record["record_version"] == record["permission_version"] == 1
            and record["thread_id"] == thread and record["thread_generation"] == u[4]
            and record["execution_id"] == execution and record["task_id"] == TASK
            and record["committed_revision"] == 2 and record["application"] == APPLICATION
            and record["policy_revision"] == "policy-1" and record["tool_id"] == "release-tool"
            and record["tool_revision"] == "tool-1" and record["constraint_revision"] == "constraint-1"
            and record["facts"]["now_ms"] == times[2] and s["tasks"][0]["interrupt"] == record["marker"],
            "Use must bind actual saved permission, application, task and turn")
    require(before["revision"] == 1 and before["status"] == "running"
            and before["execution_id"] == execution and before["turn_id"] == s["turn_id"]
            and before["owner_generation"] == owner and before["attempt_history"] == []
            and before["attempts_used"] == 0 and before["definition"] == DEFINITION
            and before["values"] == {"input": input_value} and before["updated_at_ms"] == times[1]
            and len(before["tasks"]) == 1 and before["tasks"][0]["status"] == "ready"
            and before["tasks"][0]["approved"] is False and before["tasks"][0]["interrupt"] is None,
            "Use before_snapshot must retain the real pre-authorization Start1")
    expected = list(previous["threads"][0])
    expected[1] = execution
    expected[5] = (integer(expected[5]) + (6 if cancelled else 5)).to_bytes(8, "big")
    require(current["threads"] == [tuple(expected)]
            and len(current["journal"]) == len(previous["journal"]) + (6 if cancelled else 5),
            "Thread and journal must advance exactly through NewUse terminal revision")
    snapshots = [json.loads(r[4]) for r in current["executions"]]
    for key in ("turn_id", "execution_id"):
        require(len({s[key] for s in snapshots}) == len(snapshots), "Separate runs reused identity")
    require(len({s["attempt_history"][0] for s in snapshots}) == len(snapshots), "Separate runs reused attempt")


def cancelled_stream(exchange):
    status, headers, raw = exchange.get()
    require(status == 200 and headers.get("content-type", "").split(";", 1)[0] == "text/event-stream",
            "Always resume must return actual SSE")
    frames = []
    for frame in raw.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
        event, data = "message", []
        for line in frame.splitlines():
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip(" "))
        if data:
            frames.append((event, json.loads("\n".join(data))))
    require([v for e, v in frames if e == "done"] == [{"output": {"cancelled": True}}]
            and not any(e in ("error", "interrupt", "detached", "chunk") for e, _ in frames),
            "Cancellation SSE must have exactly one cancelled Done and no late output/error/interrupt/detached")


def verify(binary):
    require(binary.is_absolute() and binary.is_file() and os.access(binary, os.X_OK),
            "--example must name an absolute already-built executable")
    children, exchanges = [], []
    temporary = tempfile.TemporaryDirectory(prefix="b4-permission-cancellation-example-")
    root = Path(temporary.name).resolve()
    token = secrets.token_urlsafe(32)
    sensitive = [token.encode()]
    env = dict(os.environ)
    env.pop("B4RUN_TOKEN", None)
    env.update(B4RUN_PORT="0", B4RUN_DATA_DIR=str(root / "data"))

    def start(mode=MODE, extra=None):
        child = Child(binary, mode, dict(env, **(extra or {})))
        children.append(child)
        return child

    def request(address, method, path, body=None, auth=token, background=False):
        exchange = Exchange(address, method, path, body, auth, background)
        exchanges.append(exchange)
        return exchange

    def retire(child, address):
        require(child.finish(interrupt=True) == 0, "Normal SIGINT retirement failed")
        listener_retired(address)
        require(not child.cleanup(), "Forced cleanup is not normal retirement")
        return capture(root / "data", child)

    def healthy(address):
        for path, value in (("/healthz", "ok"), ("/readyz", "ready")):
            status, _, body = decoded(request(address, "GET", path))
            require(status == 200 and body == {"status": value}, "Owner health/readiness failed")

    try:
        # Launch the valid mode first so an old binary's RED is the missing mode, not invalid-config checks.
        child = start(extra={"B4RUN_TOKEN": token})
        address = ready_address(child)
        healthy(address)
        require(decoded(request(address, "GET", "/b4/capabilities", auth=None))[0] == 401,
                "Anonymous discovery was accepted")
        status, _, capabilities = decoded(request(address, "GET", "/b4/capabilities"))
        features = capabilities["features"]
        require(status == 200 and capabilities["descriptor_version"] == 7
                and capabilities["application"] == {"id": APP, "revision": APPLICATION["revision"]}
                and all(features[k] is True for k in ("durable_permission_creation", "durable_permission_reuse",
                    "durable_permission_entered_cancellation", "durable_permission_ready_cancellation",
                    "durable_permission_withheld_cancellation"))
                and all(features[k] is False for k in ("always_permissions", "trusted_known_id_recovery",
                    "fenced_cancellation", "fenced_deletion", "automatic_recovery_scan", "durable_event_replay")),
                "Discovery must advertise exact narrow V7 cancellation profile")
        status, _, created = decoded(request(address, "POST", "/threads", {"metadata": {}}))
        require(status == 200 and created["status"] == "idle" and created["metadata"] == {}, "Thread creation failed")
        thread = resource_id(created["thread_id"])
        path = "/threads/" + thread
        waiting = {"month": "2026-09", "wait_for_cancel": True}
        body = {"route": ROUTE, "input": waiting}
        require(decoded(request(address, "POST", path + "/runs/wait", body, auth=None))[0] == 401,
                "Anonymous run was accepted")
        status, _, value = decoded(request(address, "POST", path + "/runs/wait", body))
        require(status == 200 and value is None, "Original run did not genuinely park")
        status, _, parked = decoded(request(address, "GET", path + "/state"))
        require(status == 200 and parked["metadata"]["revision"] == 2
                and parked["values"] == {"input": waiting}, "Original Waiting2 snapshot differs")
        status, _, pending = decoded(request(address, "GET", path + "/pending_interrupts"))
        require(status == 200 and len(pending["interrupts"]) == 1, "Expected one actual pending approval")
        interrupt = pending["interrupts"][0]
        require(isinstance(interrupt["interruptId"], str) and bool(interrupt["interruptId"])
                and interrupt["value"] == PROMPT and isinstance(interrupt["grant"], str)
                and interrupt["grant"].startswith("b4ag_") and isinstance(interrupt["resumeKey"], str),
                "Pending endpoint omitted actual approval material")
        sensitive.extend([interrupt["grant"].encode(), interrupt["resumeKey"].encode(), b'2026-09', b'2026-10', b'wait_for_cancel'])
        resume = {"route": ROUTE, "resume": [{"status": "resolved", "interruptId": interrupt["interruptId"],
                    "payload": "always", "grant": interrupt["grant"]}]}
        stream = request(address, "POST", path + "/resume", resume, background=True)
        require(child.event() == {"status": "callback_entered", "thread_id": thread},
                "Expected actual bounded callback entry event for original thread")
        status, headers, value = decoded(request(address, "POST", path + "/cancel"))
        require(status == 200 and value == {"status": "interrupted", "thread_id": thread}
                and headers.get("cache-control") == "no-store", "Authenticated cancel contract differs")
        cancelled_stream(stream)
        healthy(address)
        seed = retire(child, address)
        decision = original_state(seed, thread, interrupt, parked, waiting)
        previous = seed
        for owner in (2, 3):
            child = start(extra={"B4RUN_TOKEN": token})
            address = ready_address(child)
            healthy(address)
            input_value = waiting if owner == 2 else {"month": "2026-10"}
            run = request(address, "POST", path + "/runs/wait", {"route": ROUTE, "input": input_value},
                          background=owner == 2)
            if owner == 2:
                require(child.event() == {"status": "callback_entered", "thread_id": thread},
                        "Expected actual saved-use callback entry event")
                status, _, pending = decoded(request(address, "GET", path + "/pending_interrupts"))
                require(status == 200 and pending["interrupts"] == [], "Saved use requested new approval")
                status, headers, value = decoded(request(address, "POST", path + "/cancel"))
                require(status == 200 and value == {"status": "interrupted", "thread_id": thread}
                        and headers.get("cache-control") == "no-store", "Saved-use cancel contract differs")
                status, _, value = decoded(run)
                require(status == 409 and value["error"]["details"]["code"] == "run_cancelled",
                        "Saved-use waiting response did not report run_cancelled")
            else:
                status, _, value = decoded(run)
                require(status == 200 and value == {"published": True}, "Later saved-use normal run failed")
            status, _, pending = decoded(request(address, "GET", path + "/pending_interrupts"))
            require(status == 200 and pending["interrupts"] == [], "Saved use parked replacement approval")
            healthy(address)
            current = retire(child, address)
            reused_state(seed, previous, current, thread, decision, owner, input_value, owner == 2)
            previous = current
        require(len(previous["executions"]) == 3 and len(previous["permission_uses"]) == 2
                and len(previous["journal"]) == 19, "Three-owner final cardinalities differ")
        # The only storage/listeners touched by invalid launches are distinct owned paths.
        for index, (mode, extra) in enumerate([
            ("unknown-mode", {"B4RUN_TOKEN": token}), (MODE, {}),
            (MODE, {"B4RUN_TOKEN": ""}), (MODE, {"B4RUN_TOKEN": " \t "}),
            (MODE, {"B4RUN_TOKEN": token, "B4RUN_PORT": "invalid"}),
            (MODE, {"B4RUN_TOKEN": token, "B4RUN_PORT": "65536"}),
        ]):
            directory = root / ("invalid-" + str(index))
            invalid = start(mode, dict(extra, B4RUN_DATA_DIR=str(directory)))
            require(invalid.finish() != 0, "Invalid config/mode was accepted")
            require(not directory.exists() and b'"ready"' not in invalid.buffers["stdout"],
                    "Invalid config/mode opened storage or reported listener readiness")
            require(not invalid.cleanup(), "Invalid-config retirement required forced cleanup")
        require(all(secret not in bytes(buffer) for secret in sensitive for c in children
                    for buffer in c.buffers.values()), "Child disclosed input/authorization material; output suppressed")
    finally:
        cleanup_failed = False
        # Close sockets before joining workers; a failed assertion must not wait for server completion.
        for exchange in exchanges:
            try:
                exchange.close()
            except Exception:
                cleanup_failed = True
        for child in children:
            if not child.closed:
                try:
                    cleanup_failed = child.cleanup() or cleanup_failed
                except Exception:
                    cleanup_failed = True
        temporary.cleanup()
        # Preserve the original expected RED when unwinding; never count emergency cleanup as success.
        if sys.exc_info()[0] is None:
            require(not cleanup_failed, "Emergency cleanup was required; acceptance failed")
    require(not root.exists(), "Owned temporary data was not removed")
    print("permission-cancellation-example: original=Cancelled6 reused=Cancelled5,Complete4 owners=1,2,3 uses=2 normal_shutdowns=3 integrity=ok removed=true entered_only=true")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--example", required=True, type=Path, help="Absolute already-built example executable")
    args = parser.parse_args()
    try:
        verify(args.example)
    except RuntimeError as error:
        print("permission-cancellation-example: FAILED: " + str(error), file=sys.stderr)
        raise SystemExit(1) from None
    except (OSError, ValueError, KeyError, IndexError, TypeError, sqlite3.Error, subprocess.SubprocessError):
        print("permission-cancellation-example: FAILED (sensitive output suppressed)", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
