#!/usr/bin/env python3
"""Accept an explicitly built example; never build, fetch, or modify source.

Usage: python3 scripts/check_permission_reuse_example.py --example /absolute/binary
Only the supplied child and HTTP over its reported loopback listener are executed.
SQL inspection uses an existing canonical database path with query_only enabled
and verified before inspection, after the relevant child has retired. The
connection itself is not opened in OS read-only mode.
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
import tempfile
import time
import uuid

WAIT = 10
LIMIT = 1024 * 1024
APP = "07d7d468-c4f9-47e1-b421-ff889251df80"
ROUTE = "/reports#agent"
TASK = "publish-monthly-report"
EFFECT = "reports.publish-monthly"
ORIGIN = {"application": APP, "tenant": "tenant", "principal": "owner"}
APPLICATION = {"application": APP, "revision": "application-1"}
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
    require(isinstance(value, str) and str(uuid.UUID(value)) == value,
            "Expected a canonical UUID resource")
    return value


class Child:
    """Nonblocking pipe reads bound readiness and shutdown even without a newline."""

    def __init__(self, binary, mode, env):
        self.process = subprocess.Popen([str(binary), mode], env=env,
                                        stdin=subprocess.DEVNULL,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.selector = selectors.DefaultSelector()
        self.buffers = {"stdout": bytearray(), "stderr": bytearray()}
        self.closed = False
        for name in self.buffers:
            pipe = getattr(self.process, name)
            os.set_blocking(pipe.fileno(), False)
            self.selector.register(pipe, selectors.EVENT_READ, name)

    def pump(self, deadline):
        remaining = deadline - time.monotonic()
        require(remaining > 0, "Child operation exceeded its deadline")
        for key, _ in self.selector.select(min(remaining, 0.1)):
            data = os.read(key.fileobj.fileno(), 65536)
            if not data:
                self.selector.unregister(key.fileobj)
            else:
                self.buffers[key.data].extend(data)
                require(sum(map(len, self.buffers.values())) <= LIMIT,
                        "Child output exceeded bound; sensitive output suppressed")

    def message(self):
        deadline = time.monotonic() + WAIT
        while b"\n" not in self.buffers["stdout"]:
            self.pump(deadline)
            require(self.process.poll() is None or b"\n" in self.buffers["stdout"],
                    "Child exited before JSON readiness; sensitive output suppressed")
        raw = bytes(self.buffers["stdout"]).split(b"\n", 1)[0]
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            raise RuntimeError("Child emitted invalid JSON; sensitive output suppressed") from None
        require(isinstance(value, dict), "Child JSON must be an object")
        return value

    def finish(self, interrupt=False):
        if interrupt:
            require(self.process.poll() is None, "Server exited before normal shutdown")
            self.process.send_signal(signal.SIGINT)
        deadline = time.monotonic() + WAIT
        while self.process.poll() is None or self.selector.get_map():
            self.pump(deadline)
        code = self.process.wait(timeout=max(0.01, deadline - time.monotonic()))
        return code

    def cleanup(self):
        # Only failed retirement uses forced kill. This is never success evidence.
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=WAIT)
        self.selector.close()
        self.process.stdout.close()
        self.process.stderr.close()
        self.closed = True


def capture(directory):
    # The actual store flock must be available after each normal child exit.
    with (directory / "owner.lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            database = (directory / "runtime.sqlite3").resolve()
            require(database.is_file(), "Retired child database is absent")
            connection = sqlite3.connect(str(database))
            try:
                connection.execute("PRAGMA query_only=ON")
                require(connection.execute("PRAGMA query_only").fetchall() == [(1,)],
                        "SQLite inspection query-only mode was not enabled")
                require(connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)],
                        "SQLite integrity failed after child retirement")
                return {name: connection.execute(sql).fetchall() for name, sql in QUERIES.items()}
            finally:
                connection.close()
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def journal(raw, execution, owner, statuses):
    rows = [row for row in raw["journal"] if row[3] == execution]
    require(len(rows) == 5, "Execution must have exactly five real journal versions")
    payloads = []
    for revision, (row, status) in enumerate(zip(rows, statuses)):
        payload = json.loads(row[8])
        require(row[1] == ("admit" if revision == 0 else "commit")
                and row[2] == raw["threads"][0][0]
                and integer(row[4]) == revision and integer(row[5]) == owner
                and json.loads(row[6]) == status
                and payload == {"execution_id": execution, "revision": revision,
                                "owner_generation": owner, "status": status,
                                "updated_at_ms": integer(row[7])},
                "Journal columns and typed JSON must match actual revision and owner")
        payloads.append(payload)
    return payloads


def seed_state(raw, thread):
    require(len(raw["metadata"]) == len(raw["threads"]) == len(raw["executions"]) == 1,
            "Seed must create one real thread and execution")
    require(integer(raw["metadata"][0][3]) == 1, "Seed must close owner one")
    t = raw["threads"][0]
    e = raw["executions"][0]
    s = json.loads(e[4])
    resource_id(e[0])
    resource_id(s["turn_id"])
    require(t[0] == thread and t[1] == e[0] and e[1] == thread
            and integer(e[2]) == s["revision"] == 4
            and integer(e[3]) == s["owner_generation"] == 1
            and s["execution_id"] == e[0] and s["status"] == "cancelled"
            and s["attempts_used"] == 0 and s["attempt_history"] == []
            and json.loads(t[4]) == ORIGIN,
            "Seed must persist original Cancelled4 without invoking a task")
    definition = s["definition"]
    require(definition["route"] == ROUTE and definition["revision"] == "definition-1"
            and definition["batches"] == [[{"id": TASK, "effect_id": EFFECT,
                  "recovery": "pure", "requires_approval": True}]],
            "Seed application definition differs from fixed harmless example")
    j = journal(raw, e[0], 1, ["accepted", "running", "waiting_for_approval", "running", "cancelled"])
    require(len(raw["approval_grants"]) == len(raw["permission_acceptances"])
            == len(raw["permission_decisions"]) == 1
            and not raw["approval_acceptances"] and not raw["permission_uses"],
            "Seed must use genuine Always acceptance with no permission use")
    g = json.loads(raw["approval_grants"][0][7])
    a = json.loads(raw["permission_acceptances"][0][7])
    d = json.loads(raw["permission_decisions"][0][5])
    require(g["consumed"] and g["execution_id"] == e[0] and g["task_id"] == TASK
            and g["effect_id"] == EFFECT and g["task_parking_revision"] == 2
            and a["before_snapshot"]["execution_id"] == e[0]
            and a["before_snapshot"]["revision"] == 2
            and a["before_snapshot"]["status"] == "waiting_for_approval"
            and a["before_snapshot"]["updated_at_ms"] == j[2]["updated_at_ms"]
            and a["committed_revision"] == 3 and a["origin"] == ORIGIN
            and a["application"] == APPLICATION and a["policy_revision"] == "policy-1"
            and len(a["decisions"]) == 1
            and a["decisions"][0]["decision"] == "always"
            and a["decisions"][0]["permission_id"] == d["permission_id"]
            and d["creator_execution_id"] == e[0] and d["creator_task_id"] == TASK
            and d["creator_interrupt_id"] == g["interrupt_id"]
            and d["scope"]["thread_id"] == thread
            and d["scope"]["thread_generation"] == t[6]
            and d["scope"]["definition"] == definition
            and d["scope"]["origin"] == ORIGIN,
            "Seed permission provenance must link actual parked grant and Always decision")
    return e[0], d


def reused_state(seed, previous, current, thread, original, decision, owner, month, *, expected_input=None):
    if expected_input is None:
        expected_input = {"month": month}
    expected_values = {"input": expected_input}
    for name in ("approval_grants", "approval_acceptances", "permission_acceptances", "permission_decisions"):
        require(current[name] == seed[name], "Original permission provenance changed")
    require(current["metadata"][0][:3] == seed["metadata"][0][:3]
            and integer(current["metadata"][0][3]) == owner, "Unexpected reopened owner")
    require(current["journal"][:len(previous["journal"])] == previous["journal"],
            "Existing journal provenance changed")
    old_exec = {row[0]: row for row in previous["executions"]}
    new_exec = {row[0]: row for row in current["executions"]}
    require(all(new_exec.get(key) == value for key, value in old_exec.items())
            and len(new_exec) == len(old_exec) + 1, "Previous executions changed or extra run appeared")
    execution = next(iter(new_exec.keys() - old_exec.keys()))
    resource_id(execution)
    require(execution != original, "Reuse must create a new execution")
    row = new_exec[execution]
    s = json.loads(row[4])
    require(row[1] == thread and integer(row[2]) == s["revision"] == 4
            and integer(row[3]) == s["owner_generation"] == owner
            and s["execution_id"] == execution and s["status"] == "completed"
            and s["attempts_used"] == 1 and len(s["attempt_history"]) == 1
            and s["output"] == expected_values and s["failure"] is None
            and s["values"] == expected_values
            and s["definition"] == decision["scope"]["definition"],
            "Reused execution must persist exact Complete4 and literal output")
    resource_id(s["turn_id"])
    resource_id(s["attempt_history"][0])
    ts = s["tasks"]
    require(len(ts) == 1 and ts[0]["spec"]["id"] == TASK
            and ts[0]["status"] == "succeeded" and ts[0]["approved"]
            and ts[0]["attempt_id"] == s["attempt_history"][0]
            and ts[0]["result"] == {"output": {"published": True}, "writes": []}
            and ts[0]["input"] == expected_values,
            "Real task attempt must succeed with modest input and no state writes")
    j = journal(current, execution, owner, ["accepted", "running", "running", "running", "completed"])
    require(s["updated_at_ms"] == j[4]["updated_at_ms"], "Complete time differs from actual journal")
    old_uses = {r[2]: r for r in previous["permission_uses"]}
    uses = {r[2]: r for r in current["permission_uses"]}
    require(all(uses.get(k) == v for k, v in old_uses.items()) and len(uses) == len(old_uses) + 1,
            "Reuse must append one distinct actual Use without changing older Uses")
    u = uses[next(iter(uses.keys() - old_uses.keys()))]
    record = json.loads(u[9])
    before = record["before_snapshot"]
    require(u[:2] == (execution, TASK) and u[3:7] == (thread, seed["threads"][0][6], decision["permission_id"], s["turn_id"])
            and integer(u[7]) == 2 and u[8] == 1
            and record["use_id"] == u[2] and record["permission_id"] == u[5]
            and record["record_version"] == record["permission_version"] == 1
            and record["thread_id"] == thread and record["thread_generation"] == u[4]
            and record["execution_id"] == execution and record["task_id"] == TASK
            and record["committed_revision"] == 2
            and record["application"] == APPLICATION and record["policy_revision"] == "policy-1"
            and record["tool_id"] == "release-tool" and record["tool_revision"] == "tool-1"
            and record["constraint_revision"] == "constraint-1"
            and record["facts"]["now_ms"] == j[2]["updated_at_ms"]
            and ts[0]["interrupt"] == record["marker"], "Use must link the actual execution and saved permission")
    require(before["revision"] == 1 and before["status"] == "running"
            and before["execution_id"] == execution and before["turn_id"] == s["turn_id"]
            and before["owner_generation"] == owner and before["attempt_history"] == []
            and before["attempts_used"] == 0 and before["definition"] == s["definition"]
            and before["values"] == expected_values
            and before["updated_at_ms"] == j[1]["updated_at_ms"]
            and before["tasks"][0]["status"] == "ready"
            and not before["tasks"][0]["approved"]
            and before["tasks"][0]["interrupt"] is None,
            "Use before_snapshot must be the actual Start1 under the reopened owner")
    previous_thread = previous["threads"][0]
    expected_thread = list(previous_thread)
    expected_thread[1] = execution
    expected_thread[5] = (integer(previous_thread[5]) + 5).to_bytes(8, "big")
    require(current["threads"] == [tuple(expected_thread)], "Thread resource must advance exactly five revisions")
    attempts = [json.loads(r[4])["attempt_history"][0] for r in current["executions"] if r[0] != original]
    require(len(set(attempts)) == len(attempts), "Separate runs reused an attempt identity")


def request(address, path, body, token=None):
    def expired(_signal, _frame):
        raise RuntimeError("HTTP exchange exceeded its total deadline")

    old_handler = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, WAIT)
    host, port = address
    connection = http.client.HTTPConnection(host, port, timeout=WAIT)
    try:
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        connection.request("POST", path, json.dumps(body), headers)
        response = connection.getresponse()
        raw = response.read(LIMIT + 1)
        require(len(raw) <= LIMIT, "HTTP response exceeded bound")
        return response.status, json.loads(raw)
    finally:
        connection.close()
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


def listener_retired(address):
    try:
        with socket.create_connection(address, timeout=1):
            pass
    except OSError as error:
        require(error.errno == errno.ECONNREFUSED, "Listener closure lacked connection-refused evidence")
    else:
        raise RuntimeError("Listener still accepts connections after normal server exit")


def verify(binary):
    require(binary.is_file() and os.access(binary, os.X_OK), "Explicit example executable is absent or not executable")
    children = []
    temporary = tempfile.TemporaryDirectory(prefix="b4-permission-reuse-example-")
    root = Path(temporary.name).resolve()
    token = secrets.token_urlsafe(32)
    env = dict(os.environ)
    env.pop("B4RUN_TOKEN", None)
    env.update(B4RUN_PORT="0", B4RUN_DATA_DIR=str(root / "data"))

    def start(mode, settings):
        child = Child(binary, mode, settings)
        children.append(child)
        return child

    try:
        for index, (mode, extra) in enumerate([
            ("invalid", {}), ("serve", {}), ("serve", {"B4RUN_TOKEN": ""}),
            ("serve", {"B4RUN_TOKEN": token, "B4RUN_PORT": "invalid"}),
        ]):
            settings = dict(env, **extra)
            directory = root / ("invalid-" + str(index))
            settings["B4RUN_DATA_DIR"] = str(directory)
            child = start(mode, settings)
            require(child.finish() != 0, "Invalid mode/config was accepted")
            require(not directory.exists() and b'"ready"' not in child.buffers["stdout"],
                    "Invalid mode/config opened storage or reported readiness")
            child.cleanup()
        seed = start("seed", env)
        info = seed.message()
        require(set(info) == {"thread_id", "route"} and info["route"] == ROUTE,
                "Seed JSON contract differs")
        thread = resource_id(info["thread_id"])
        require(seed.finish() == 0, "Seed did not close normally")
        seed.cleanup()
        data = Path(env["B4RUN_DATA_DIR"])
        baseline = capture(data)
        original, decision = seed_state(baseline, thread)
        previous = baseline
        for owner, month in [(2, "2026-09"), (3, "2026-10")]:
            child = start("serve", dict(env, B4RUN_TOKEN=token))
            ready = child.message()
            require(set(ready) == {"status", "address"} and ready["status"] == "ready",
                    "Serve readiness JSON contract differs")
            host, separator, port = ready["address"].rpartition(":")
            require(separator and host == "127.0.0.1" and port.isdigit() and 0 < int(port) <= 65535,
                    "Readiness must identify the actual ephemeral loopback listener")
            address = (host, int(port))
            path = "/threads/" + thread + "/runs/wait"
            body = {"route": ROUTE, "input": {"month": month}}
            require(request(address, path, body)[0] == 401, "Anonymous run was accepted")
            status, result = request(address, path, body, token)
            require(status == 200 and result == {"published": True}, "Authenticated reuse did not return literal output")
            require(child.finish(interrupt=True) == 0, "Server interrupt shutdown failed")
            listener_retired(address)
            child.cleanup()
            current = capture(data)
            reused_state(baseline, previous, current, thread, original, decision, owner, month)
            previous = current
        require(len(previous["permission_uses"]) == 2, "Expected exactly two genuine permission Uses")
        require(all(token.encode() not in bytes(buffer)
                    for child in children for buffer in child.buffers.values()),
                "Child disclosed bearer token; sensitive output suppressed")
    finally:
        try:
            for child in children:
                if not child.closed:
                    child.cleanup()
        finally:
            temporary.cleanup()
    require(not root.exists(), "Temporary database directory was not removed")
    print("permission-reuse-example: seed=Cancelled4 owners=1,2,3 reused=Complete4,Complete4 uses=2 normal_shutdowns=2 integrity=ok removed=true")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--example", required=True, type=Path, help="Explicit path to an already-built executable")
    args = parser.parse_args()
    try:
        verify(args.example.expanduser().resolve())
    except RuntimeError as error:
        print("permission-reuse-example: FAILED: " + str(error), file=__import__("sys").stderr)
        raise SystemExit(1) from None
    except (OSError, ValueError, KeyError, IndexError, TypeError, sqlite3.Error, subprocess.SubprocessError):
        # Never echo child output, HTTP payloads, approval material, or credentials.
        # Fixed diagnostic messages remain safe; detailed source checks identify the stage.
        print("permission-reuse-example: FAILED (sensitive child output suppressed)", file=__import__("sys").stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
