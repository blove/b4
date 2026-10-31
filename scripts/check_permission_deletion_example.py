#!/usr/bin/env python3
"""Qualify an absolute already-built serve-deletion binary using owned local data.

Standard library only; no build, seed, SQL writes, or live SQL inspection.
Selected bounded transport/process/capture and execution validation primitives
adapted from check_permission_cancellation_example.py; no old suite is imported.
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

MODE = "serve-deletion"
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
                unknown = b"usage: serve_permission_reuse"
                require(unknown not in self.buffers["stderr"],
                        "serve-deletion mode unavailable: example rejected unknown mode before readiness")
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
        self.normal = (not interrupt or code == 0)
        return code

    def cleanup(self):
        forced = not self.normal
        try:
            # Own the session even after its leader exits. A surviving group requires
            # emergency cleanup and cannot qualify any positive normal retirement.
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
                forced = True
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

    def __init__(self, address, method, path, body=None, token=None, background=False, extra_headers=None):
        self.sock = DeadlineSocket()
        self.address, self.method, self.path = address, method, path
        self.body, self.token = body, token
        self.extra_headers = extra_headers or {}
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
            headers.update(self.extra_headers)
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
                connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, LIMIT)
                deadline = time.monotonic() + WAIT
                connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
                connection.execute("PRAGMA query_only=ON")
                require(connection.execute("PRAGMA query_only").fetchall() == [(1,)], "Query-only inspection unavailable")
                require(connection.execute("PRAGMA integrity_check").fetchmany(2) == [("ok",)], "Database integrity failed")
                tables = {}
                for name, sql in QUERIES.items():
                    rows = connection.execute(sql).fetchmany(101)
                    require(sum(len(v) if isinstance(v, (str, bytes)) else 8 for row in rows for v in row) <= LIMIT,
                            "Durable table exceeded byte bound")
                    require(len(rows) <= 100, "Durable table exceeded focused acceptance row bound")
                    tables[name] = rows
                return tables
            finally:
                connection.close()
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def journal(raw, execution, thread, owner, statuses):
    rows = [row for row in raw["journal"] if row[3] == execution]
    require(len(rows) == len(statuses), "Execution journal version count differs")
    times = []
    for revision, (row, status) in enumerate(zip(rows, statuses)):
        timestamp = integer(row[7])
        require(row[1:4] == ("admit" if revision == 0 else "commit", thread, execution)
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
    revision = (6 if cancelled else 5) if original else (5 if cancelled else 4)
    statuses = ["accepted", "running"] + (["waiting_for_approval"] if original else []) + ["running", "running"]
    statuses += ["cancel_requested", "cancelled"] if cancelled else ["completed"]
    times = journal(raw, execution, thread, owner, statuses)
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


OWNED = tuple(name for name in QUERIES if name != "metadata")
THREAD_COLUMN = {"threads": 0, "executions": 1, "journal": 2, "approval_grants": 3,
                 "approval_acceptances": 2, "permission_acceptances": 2,
                 "permission_decisions": 1, "permission_uses": 3}


def partition(raw, expected_threads):
    """Account for every physical row; reject unknown or cross-thread references."""
    require(set(raw) == set(QUERIES) and len(raw["metadata"]) == 1,
            "Expected exactly nine captured tables and one metadata row")
    threads = {r[0]: r for r in raw["threads"]}
    require(len(threads) == len(raw["threads"]) and set(threads) == set(expected_threads),
            "Unexpected or duplicate physical thread")
    executions = {r[0]: r for r in raw["executions"]}
    permissions = {r[0]: r for r in raw["permission_decisions"]}
    require(len(executions) == len(raw["executions"])
            and len(permissions) == len(raw["permission_decisions"]), "Duplicate physical identity")
    result = {t: {name: [] for name in OWNED} for t in threads}
    for name in OWNED:
        for row in raw[name]:
            thread = row[THREAD_COLUMN[name]]
            require(thread in threads, "Unaccounted physical row thread")
            result[thread][name].append(row)
            if name == "threads":
                require(row[1] in executions and executions[row[1]][1] == thread,
                        "Active execution must belong to its actual thread")
                require(json.loads(row[3]) == {} and json.loads(row[4]) == ORIGIN,
                        "Thread metadata or origin differs")
            elif name == "journal":
                if row[1] == "thread":
                    require(row[3:5] == (None, None) and row[6] is None and row[8] is None
                            and integer(row[7]) == integer(threads[thread][2]),
                            "Invalid thread creation journal")
                else:
                    require(row[1] in ("admit", "commit") and row[3] in executions
                            and executions[row[3]][1] == thread, "Unknown journal execution relationship")
            elif name in ("approval_grants", "approval_acceptances", "permission_acceptances", "permission_uses"):
                require(row[0] in executions and executions[row[0]][1] == thread,
                        "Cross-thread approval or permission execution")
                generation = row[4] if name in ("approval_grants", "permission_uses") else row[3]
                require(generation == threads[thread][6], "Historical row generation differs")
                if name == "permission_uses":
                    require(row[5] in permissions and permissions[row[5]][1] == thread,
                            "Unknown or cross-thread saved permission")
            elif name == "permission_decisions":
                d = json.loads(row[5])
                require(row[2] == threads[thread][6] and d["creator_execution_id"] in executions
                        and executions[d["creator_execution_id"]][1] == thread,
                        "Permission creator relationship differs")
    for thread, tables in result.items():
        require(len([r for r in tables["journal"] if r[1] == "thread"]) == 1,
                "Each real thread needs exactly one creation journal")
    return result


def provenance(raw, thread, histories, pending_by_execution):
    """Check actual recorded executions, grants, acceptances, decisions, and Uses."""
    t = next(r for r in raw["threads"] if r[0] == thread)
    rows = {name: [r for r in raw[name] if r[THREAD_COLUMN[name]] == thread] for name in OWNED}
    snapshots = {}
    for execution, kind, owner, input_value in histories:
        s, _ = execution_state(raw, execution, thread, owner, input_value,
                               original=kind in ("once", "always"), cancelled=kind == "cancelled")
        snapshots[execution] = s
    require(set(snapshots) == {r[0] for r in rows["executions"]}, "Unaccounted execution history")
    require(len({s["turn_id"] for s in snapshots.values()}) == len(snapshots)
            and len({s["attempt_history"][0] for s in snapshots.values()}) == len(snapshots),
            "Separate executions reused turn or attempt identity")
    require(t[1] == histories[-1][0] and integer(t[5]) == sum(s["revision"] + 1 for s in snapshots.values()),
            "Thread active execution or exact resource revision differs")
    require(len(rows["journal"]) == 1 + sum(s["revision"] + 1 for s in snapshots.values()),
            "Thread journal has unaccounted physical revisions")
    expected_grants = {e: pending_by_execution[e] for e, kind, _, _ in histories if kind in ("once", "always")}
    require(len(rows["approval_grants"]) == len(expected_grants), "Actual grant count differs")
    grants = {}
    for row in rows["approval_grants"]:
        require(row[0] in expected_grants, "Unknown grant execution")
        p, s = expected_grants[row[0]], snapshots[row[0]]
        g = json.loads(row[7])
        require(row[:5] == (row[0], p["interruptId"], TASK, thread, t[6])
                and integer(row[5]) == 2 and row[6] == 1
                and g["consumed"] is True and g["execution_id"] == row[0]
                and g["interrupt_id"] == p["interruptId"] and g["token"] == p["grant"]
                and g["resume_key"] == p["resumeKey"] and g["thread_id"] == thread
                and g["thread_generation"] == t[6] and g["task_id"] == TASK
                and g["effect_id"] == EFFECT and g["route"] == ROUTE
                and g["definition_revision"] == "definition-1" and g["policy_revision"] == "policy-1"
                and g["origin"] == ORIGIN and g["task_parking_revision"] == 2 and g["prompt"] == PROMPT,
                "Grant differs from actual HTTP approval provenance")
        require(s["turn_history"][0] != s["turn_id"] and len(s["turn_history"]) == 2,
                "Approval resume did not preserve a fresh turn")
        grants[row[0]] = g
    once = {e for e, kind, _, _ in histories if kind == "once"}
    always = {e for e, kind, _, _ in histories if kind == "always"}
    require(len(rows["approval_acceptances"]) == len(once)
            and len(rows["permission_acceptances"]) == len(always)
            and len(rows["permission_decisions"]) == len(always), "Acceptance/decision counts differ")
    for row in rows["approval_acceptances"]:
        a = json.loads(row[7])
        require(row[0] in once and row[1] == snapshots[row[0]]["turn_id"]
                and integer(row[4]) == 2 and row[5] == "definition-1"
                and a["execution_id"] == row[0] and a["thread_id"] == thread
                and a["thread_generation"] == t[6] and a["definition_revision"] == row[5]
                and a["accepted_at_ms"] == integer(row[6])
                and a["turn_id"] == row[1] and a["previous_revision"] == 2
                and a["context"] == {"actor": ORIGIN, "policy_revision": "policy-1"}
                and a["decisions"] == [{"task_id": TASK, "interrupt_id": grants[row[0]]["interrupt_id"],
                    "decision": "once", "proposal_digest": grants[row[0]]["proposal_digest"], "task_parking_revision": 2}],
                "Ordinary Once acceptance lost genuine provenance")
    decisions = {}
    for row in rows["permission_decisions"]:
        d = json.loads(row[5])
        e = d["creator_execution_id"]
        require(e in always and row[0] == d["permission_id"] and row[3] == d["scope_digest"]
                and row[4] == d["record_version"] == 1 and d["creator_turn_id"] == snapshots[e]["turn_id"]
                and d["creator_task_id"] == TASK and d["creator_interrupt_id"] == grants[e]["interrupt_id"]
                and d["scope"] == {"thread_id": thread, "thread_generation": t[6], "origin": ORIGIN,
                    "application": APPLICATION, "route": ROUTE, "definition": DEFINITION,
                    **BINDING, "policy_revision": "policy-1"}, "Saved permission scope/creator differs")
        decisions[e] = d
    for row in rows["permission_acceptances"]:
        a = json.loads(row[7])
        e = row[0]
        require(e in always and row[1] == snapshots[e]["turn_id"] and integer(row[4]) == 2
                and integer(row[5]) == 3 and row[6] == a["record_version"] == 1
                and a["application"] == APPLICATION and a["origin"] == ORIGIN
                and a["thread_id"] == thread and a["thread_generation"] == t[6]
                and a["turn_id"] == row[1] and a["committed_revision"] == 3
                and a["before_snapshot"]["execution_id"] == e
                and a["before_snapshot"]["revision"] == 2
                and a["before_snapshot"]["status"] == "waiting_for_approval"
                and a["decisions"] == [{"task_id": TASK, "interrupt_id": grants[e]["interrupt_id"],
                    "grant_ref": {"execution_id": e, "interrupt_id": grants[e]["interrupt_id"]},
                    "decision": "always", "proposal_digest": grants[e]["proposal_digest"],
                    "task_parking_revision": 2, "binding": BINDING, "permission_id": decisions[e]["permission_id"]}],
                "Always acceptance lost actual saved permission provenance")
    uses = {e for e, kind, _, _ in histories if kind in ("saved", "cancelled")}
    require(len(rows["permission_uses"]) == len(uses), "Saved Use count differs")
    permission_ids = {d["permission_id"] for d in decisions.values()}
    for row in rows["permission_uses"]:
        u = json.loads(row[9])
        e = row[0]
        require(e in uses and row[1] == TASK and row[5] in permission_ids
                and row[6] == snapshots[e]["turn_id"] and integer(row[7]) == 2 and row[8] == 1
                and u["use_id"] == row[2] and u["execution_id"] == e and u["permission_id"] == row[5]
                and u["thread_id"] == thread and u["thread_generation"] == t[6]
                and u["before_snapshot"]["turn_id"] == row[6] and u["task_id"] == TASK
                and u["record_version"] == u["permission_version"] == 1
                and u["committed_revision"] == 2 and u["application"] == APPLICATION
                and u["policy_revision"] == "policy-1" and u["tool_id"] == "release-tool"
                and u["tool_revision"] == "tool-1" and u["constraint_revision"] == "constraint-1"
                and u["before_snapshot"]["execution_id"] == e
                and u["before_snapshot"]["revision"] == 1
                and u["before_snapshot"]["status"] == "running"
                and snapshots[e]["tasks"][0]["interrupt"] == u["marker"]
                and snapshots[e]["turn_history"] == [snapshots[e]["turn_id"]],
                "Saved Use lost actual authorization/input/result relationship")


def terminal_stream(exchange, cancelled=False):
    status, headers, raw = exchange.get()
    require(status == 200 and headers.get("content-type", "").split(";", 1)[0] == "text/event-stream",
            "Expected actual SSE response")
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
    output = {"cancelled": True} if cancelled else {"published": True}
    require([v for e, v in frames if e == "done"] == [{"output": output}]
            and not any(e in ("error", "interrupt", "detached", "chunk") for e, _ in frames),
            "Stream must contain exactly one actual terminal result without late output")


def metadata_advance(before, after, epoch):
    require(before["metadata"][0][:3] == after["metadata"][0][:3]
            and integer(after["metadata"][0][3]) == epoch,
            "Metadata changed beyond normal owner epoch advancement")


def verify(binary):
    require(binary.is_absolute() and binary.is_file() and os.access(binary, os.X_OK),
            "--example must name an absolute already-built executable")
    children, exchanges, sensitive = [], [], []
    temporary = tempfile.TemporaryDirectory(prefix="b4-permission-deletion-example-")
    root = Path(temporary.name).resolve()
    token = secrets.token_urlsafe(32)
    sensitive.append(token.encode())
    env = dict(os.environ)
    env.pop("B4RUN_TOKEN", None)
    env.update(B4RUN_PORT="0", B4RUN_DATA_DIR=str(root / "data"))
    normal_owners = 0
    cleanup_ok = False
    report = {}

    def start(extra=None):
        child = Child(binary, MODE, dict(env, **(extra or {})))
        children.append(child)
        return child

    def request(address, method, path, body=None, auth=token, background=False, headers=None):
        exchange = Exchange(address, method, path, body, auth, background, headers)
        exchanges.append(exchange)
        return exchange

    def retire(child, address):
        nonlocal normal_owners
        require(child.finish(interrupt=True) == 0, "Normal SIGINT retirement failed")
        listener_retired(address)
        require(not child.cleanup(), "Forced cleanup cannot count as normal retirement")
        captured = capture(root / "data", child)
        normal_owners += 1
        return captured

    def discovery(address):
        for path, value in (("/healthz", "ok"), ("/readyz", "ready")):
            status, _, body = decoded(request(address, "GET", path))
            require(status == 200 and body == {"status": value}, "Owner health/readiness failed")
        status, _, capabilities = decoded(request(address, "GET", "/b4/capabilities"))
        features = capabilities["features"]
        require(status == 200 and capabilities["descriptor_version"] == 8
                and capabilities["application"] == {"id": APP, "revision": APPLICATION["revision"]}
                and all(features[k] is True for k in ("live_attach", "required_once_deny", "durable_permission_creation", "durable_permission_reuse",
                    "durable_permission_entered_cancellation", "durable_permission_ready_cancellation",
                    "durable_permission_withheld_cancellation", "durable_permission_settled_thread_deletion"))
                and all(features[k] is False for k in ("always_permissions", "trusted_known_id_recovery",
                    "fenced_cancellation", "fenced_deletion", "automatic_recovery_scan", "durable_event_replay")),
                "Discovery must advertise exact narrow V8 settled deletion profile")

    def create(address):
        status, _, value = decoded(request(address, "POST", "/threads", {"metadata": {}}))
        require(status == 200 and value["status"] == "idle" and value["metadata"] == {},
                "Actual HTTP thread creation failed")
        return resource_id(value["thread_id"])

    def state_execution(address, path, value):
        status, _, state = decoded(request(address, "GET", path + "/state"))
        require(status == 200 and state["values"] == {"input": value}, "Actual execution input differs")
        require(state["config"]["configurable"]["thread_id"] == path.rsplit("/", 1)[1],
                "Actual execution belongs to wrong thread")
        return resource_id(state["config"]["configurable"]["checkpoint_id"])

    def no_pending(address, path):
        status, _, pending = decoded(request(address, "GET", path + "/pending_interrupts"))
        require(status == 200 and pending["interrupts"] == [], "Saved Use requested a replacement approval")

    def approval(address, path, value, decision, pending_by_execution):
        status, _, result = decoded(request(address, "POST", path + "/runs/wait", {"route": ROUTE, "input": value}))
        require(status == 200 and result is None, "New approval run did not genuinely park")
        execution = state_execution(address, path, value)
        status, _, pending = decoded(request(address, "GET", path + "/pending_interrupts"))
        require(status == 200 and len(pending["interrupts"]) == 1, "Expected one genuine pending approval")
        p = pending["interrupts"][0]
        require(isinstance(p["interruptId"], str) and bool(p["interruptId"]) and p["value"] == PROMPT
                and isinstance(p["grant"], str) and p["grant"].startswith("b4ag_")
                and isinstance(p["resumeKey"], str) and bool(p["resumeKey"]),
                "Pending response omitted genuine approval material")
        sensitive.extend([p["grant"].encode(), p["resumeKey"].encode()])
        pending_by_execution[execution] = p
        terminal_stream(request(address, "POST", path + "/resume", {"route": ROUTE,
            "resume": [{"status": "resolved", "interruptId": p["interruptId"],
                        "payload": decision, "grant": p["grant"]}]}))
        no_pending(address, path)
        require(state_execution(address, path, value) == execution, "Resume replaced original execution")
        return execution

    def saved(address, path, value):
        status, _, result = decoded(request(address, "POST", path + "/runs/wait", {"route": ROUTE, "input": value}))
        require(status == 200 and result == {"published": True}, "Saved run did not return actual committed result")
        no_pending(address, path)
        return state_execution(address, path, value)

    try:
        # FIRST launch is valid: unchanged binaries fail here on the absent mode.
        child = start({"B4RUN_TOKEN": token})
        address = ready_address(child)
        discovery(address)
        target, control = create(address), create(address)
        require(target != control, "Separate actual thread creations reused identity")
        target_path, control_path = "/threads/" + target, "/threads/" + control
        month = {"month": "2026-09"}
        pending_by_execution = {}
        once = approval(address, target_path, month, "once", pending_by_execution)
        always = approval(address, target_path, month, "always", pending_by_execution)
        completed = saved(address, target_path, month)
        control_always = approval(address, control_path, month, "always", pending_by_execution)
        waiting = {"month": "2026-09", "wait_for_cancel": True}
        stream = request(address, "POST", target_path + "/runs/stream",
                         {"route": ROUTE, "input": waiting}, background=True)
        require(child.event() == {"status": "callback_entered", "thread_id": target},
                "Actual live callback entry must identify target thread")
        status, _, _ = request(address, "DELETE", target_path).get()
        require(status == 409, "Deletion must refuse actual entered live target")
        no_pending(address, target_path)
        status, headers, result = decoded(request(address, "POST", target_path + "/cancel"))
        require(status == 200 and headers.get("cache-control") == "no-store"
                and result == {"status": "interrupted", "thread_id": target}, "Actual HTTP cancellation failed")
        terminal_stream(stream, cancelled=True)
        cancelled = state_execution(address, target_path, waiting)
        first = retire(child, address)  # Terminal observation alone is not drain proof.
        first_parts = partition(first, (target, control))
        target_history = [(once, "once", 1, month), (always, "always", 1, month),
                          (completed, "saved", 1, month), (cancelled, "cancelled", 1, waiting)]
        control_history = [(control_always, "always", 1, month)]
        provenance(first, target, target_history, pending_by_execution)
        provenance(first, control, control_history, pending_by_execution)
        target_counts = {name: len(first_parts[target][name]) for name in OWNED}
        require(target_counts == {"threads": 1, "executions": 4, "journal": 24, "approval_grants": 2,
                "approval_acceptances": 1, "permission_acceptances": 1, "permission_decisions": 1,
                "permission_uses": 2} and integer(first["metadata"][0][3]) == 1,
                "Genuine target must populate exact all-eight-table history")
        control_counts = {name: len(first_parts[control][name]) for name in OWNED}
        require(control_counts == {"threads": 1, "executions": 1, "journal": 7, "approval_grants": 1,
                "approval_acceptances": 0, "permission_acceptances": 1, "permission_decisions": 1,
                "permission_uses": 0}, "Genuine control history count differs")

        child = start({"B4RUN_TOKEN": token})
        address = ready_address(child)
        discovery(address)
        for auth, headers, expected in ((None, None, 401), ("wrong-fixture-token", None, 401),
                                        (token, {"Origin": "https://browser.invalid"}, 403)):
            status, _, _ = request(address, "DELETE", target_path, auth=auth, headers=headers).get()
            require(status == expected, "Deletion authorization/browser guard failed")
        status, headers, body = request(address, "DELETE", target_path).get()
        require(status == 204 and headers.get("cache-control") == "no-store" and body == b"",
                "Actual settled DELETE must return 204/no-store/empty body")
        require(request(address, "GET", target_path + "/state").get()[0] == 404,
                "Deleted target must be masked absent by fixed OwnerPolicy")
        require(request(address, "GET", control_path + "/state").get()[0] == 200,
                "Preserved control became unreadable")
        second = retire(child, address)
        second_parts = partition(second, (control,))
        metadata_advance(first, second, 2)
        require(second_parts[control] == first_parts[control]
                and all(second[name] == first_parts[control][name] for name in OWNED),
                "Deletion must remove exactly target rows and preserve all other physical bytes")

        child = start({"B4RUN_TOKEN": token})
        address = ready_address(child)
        discovery(address)
        require(request(address, "GET", target_path + "/state").get()[0] == 404
                and request(address, "DELETE", target_path).get()[0] == 404,
                "Later reopen must observe actual earlier deletion under fixed OwnerPolicy")
        later_input = {"month": "2026-10"}
        later = saved(address, control_path, later_input)
        third = retire(child, address)
        third_parts = partition(third, (control,))
        metadata_advance(second, third, 3)
        provenance(third, control, control_history + [(later, "saved", 3, later_input)], pending_by_execution)
        require(all(third[name] == second[name] for name in PROVENANCE),
                "Survivor reuse changed original permission provenance")
        require(len(third["permission_uses"]) == len(second["permission_uses"]) + 1
                and all(row in third["executions"] for row in second["executions"])
                and third["journal"][:len(second["journal"])] == second["journal"],
                "Survivor reuse changed old execution/journal or failed to add one actual Use")
        final_counts = {name: len(third_parts[control][name]) for name in OWNED}
        require(final_counts == dict(control_counts, executions=2, journal=12, permission_uses=1),
                "Final actual survivor history counts differ")

        for index, extra in enumerate(({}, {"B4RUN_TOKEN": ""}, {"B4RUN_TOKEN": " \t "},
                                      {"B4RUN_TOKEN": token, "B4RUN_PORT": "invalid"},
                                      {"B4RUN_TOKEN": token, "B4RUN_PORT": "65536"})):
            directory = root / ("invalid-" + str(index))
            invalid = start(dict(extra, B4RUN_DATA_DIR=str(directory)))
            require(invalid.finish() != 0, "Invalid new-mode configuration was accepted")
            require(not directory.exists() and b'"ready"' not in invalid.buffers["stdout"],
                    "Invalid new-mode configuration caused storage/listener side effects")
            require(not invalid.cleanup(), "Invalid config required forced cleanup")
        require(all(secret not in bytes(buffer) for secret in sensitive for c in children
                    for buffer in c.buffers.values()), "Child disclosed authorization material; output suppressed")
        report = {"status": "ok", "mode": MODE, "normal_owners": normal_owners,
                  "target_before": target_counts, "control_before": control_counts,
                  "control_after_reuse": final_counts, "live_delete_refused": True,
                  "actual_delete_committed": True, "target_removed_all_eight_tables": True,
                  "other_rows_byte_identical": True, "permission_provenance_unchanged": True,
                  "survivor_genuine_additional_use": 1, "integrity_ok": True,
                  "owner_locks_released": True, "listeners_released": True}
    finally:
        active_error = sys.exc_info()[0] is not None
        # Once unwinding begins, repeated termination must not interrupt owned cleanup.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        forced, cleanup_failed = False, False
        for exchange in exchanges:
            try:
                exchange.close()
            except Exception:
                cleanup_failed = True
        for child in children:
            if not child.closed:
                try:
                    forced = child.cleanup() or forced
                except Exception:
                    cleanup_failed = True
        try:
            temporary.cleanup()
        except Exception:
            cleanup_failed = True
        cleanup_ok = (not cleanup_failed and not root.exists()
                      and all(c.closed and c.process.returncode is not None for c in children)
                      and all(e.thread is None or not e.thread.is_alive() for e in exchanges))
        receipt = {"cleanup_ok": cleanup_ok, "children_reaped": sum(c.process.returncode is not None for c in children),
                   "children_started": len(children), "forced_cleanup": forced,
                   "normal_owners": normal_owners, "owned_temporary_removed": not root.exists(),
                   "http_workers_joined": all(e.thread is None or not e.thread.is_alive() for e in exchanges)}
        if active_error:
            print(json.dumps({"status": "failed", **receipt}, sort_keys=True), file=sys.stderr)
        else:
            require(cleanup_ok and not forced and normal_owners == 3,
                    "Acceptance requires three normal owners and complete unforced cleanup")
            report.update(receipt)
    print(json.dumps(report, sort_keys=True))


def terminated(signum, frame):
    raise RuntimeError("Probe terminated; owned cleanup requested")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--example", required=True, type=Path, help="Absolute already-built executable")
    args = parser.parse_args()
    previous = signal.signal(signal.SIGTERM, terminated)
    try:
        verify(args.example)
    except RuntimeError as error:
        print("permission-deletion-example: FAILED: " + str(error), file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        print("permission-deletion-example: FAILED (sensitive output suppressed)", file=sys.stderr)
        raise SystemExit(1) from None
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    main()
