#!/usr/bin/env python3
"""Qualify an explicitly built HTTP Always example using only owned local data.

Usage: python3 -B scripts/check_http_always_example.py --example /absolute/binary
Never builds, fetches, seeds, or writes SQL. The supplied binary must implement
serve-approvals. SQLite inspection occurs only after normal child retirement.
The committed attempt/result is process evidence, not callback instrumentation.
"""
import argparse
import http.client
import json
import os
from pathlib import Path
import secrets
import signal
import sqlite3
import subprocess
import tempfile

from check_permission_reuse_example import (
    APP, APPLICATION, Child, EFFECT, LIMIT, ORIGIN, ROUTE, TASK, WAIT,
    capture, integer, listener_retired, require, resource_id, reused_state,
)

MODE = "serve-approvals"
PROMPT = {"question": "Publish monthly report?"}


def exchange(address, method, path, body=None, token=None, sse=False):
    """http.client removes chunk framing; SIGALRM bounds the entire exchange."""
    def expired(_signal, _frame):
        raise RuntimeError("HTTP exchange exceeded its total deadline")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, WAIT)
    connection = http.client.HTTPConnection(*address, timeout=WAIT)
    try:
        headers = {"Accept": "text/event-stream" if sse else "application/json"}
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        if body is not None:
            headers["Content-Type"] = "application/json"
        connection.request(method, path, None if body is None else json.dumps(body), headers)
        response = connection.getresponse()
        raw = response.read(LIMIT + 1)
        require(len(raw) <= LIMIT, "HTTP response exceeded bound")
        if sse:
            require(response.status == 200
                    and response.getheader("Content-Type", "").split(";", 1)[0] == "text/event-stream",
                    "Always resume did not return an SSE stream")
            frames = []
            for frame in raw.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
                event = "message"
                data = []
                for line in frame.splitlines():
                    if line.startswith("event:"):
                        event = line[6:].strip()
                    elif line.startswith("data:"):
                        data.append(line[5:].lstrip(" "))
                if data:
                    frames.append((event, json.loads("\n".join(data))))
            require([value for event, value in frames if event == "done"]
                    == [{"output": {"published": True}}],
                    "Always SSE must contain exactly one committed literal done result")
            require(not any(event in ("error", "interrupt", "detached") for event, _ in frames),
                    "Always resume did not complete normally")
            return response.status, frames
        return response.status, json.loads(raw)
    finally:
        connection.close()
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def ready_address(child):
    ready = child.message()
    require(set(ready) == {"status", "address"} and ready["status"] == "ready",
            "Serve readiness JSON contract differs")
    host, separator, port = ready["address"].rpartition(":")
    require(separator and host == "127.0.0.1" and port.isdigit() and 0 < int(port) <= 65535,
            "Readiness must identify the actual ephemeral loopback listener")
    return host, int(port)


def original_state(raw, thread, pending, parked_state, *, expected_input=None):
    if expected_input is None:
        expected_input = {"month": "2026-09"}
    expected_values = {"input": expected_input}
    require(set(raw) == {"metadata", "threads", "executions", "journal", "approval_grants",
                         "approval_acceptances", "permission_acceptances", "permission_decisions", "permission_uses"},
            "Expected all nine durable tables")
    require(len(raw["metadata"]) == len(raw["threads"]) == len(raw["executions"])
            == len(raw["approval_grants"]) == len(raw["permission_acceptances"])
            == len(raw["permission_decisions"]) == 1
            and len(raw["journal"]) == 7
            and not raw["approval_acceptances"] and not raw["permission_uses"],
            "Original HTTP Always table cardinalities differ")
    require(integer(raw["metadata"][0][3]) == 1, "Original server must own generation one")
    t = raw["threads"][0]
    e = raw["executions"][0]
    s = json.loads(e[4])
    execution = resource_id(e[0])
    require(isinstance(t[6], str) and len(t[6]) == 64
            and all(character in "0123456789abcdef" for character in t[6]),
            "Thread generation must retain its actual random incarnation")
    resource_id(s["turn_id"])
    require(t[0] == thread and t[1] == execution and e[1] == thread
            and integer(t[5]) == 6 and json.loads(t[3]) == {} and json.loads(t[4]) == ORIGIN
            and integer(e[2]) == s["revision"] == 5
            and integer(e[3]) == s["owner_generation"] == 1
            and s["execution_id"] == execution
            and parked_state["config"]["configurable"]["checkpoint_id"] == execution
            and parked_state["config"]["configurable"]["thread_id"] == thread
            and s["status"] == "completed"
            and s["attempts_used"] == 1 and len(s["attempt_history"]) == 1
            and s["output"] == s["values"] == expected_values
            and s["failure"] is None,
            "Original HTTP execution must persist Complete5 with literal snapshot input")
    resource_id(s["attempt_history"][0])
    definition = s["definition"]
    spec = {"id": TASK, "effect_id": EFFECT, "recovery": "pure", "requires_approval": True}
    require(definition == {"route": ROUTE, "revision": "definition-1", "batches": [[spec]], "max_attempts": 1},
            "Application must retain the fixed harmless definition")
    require(len(s["tasks"]) == 1, "Original must contain one task")
    task = s["tasks"][0]
    require(task["spec"] == spec and task["status"] == "succeeded" and task["approved"]
            and task["attempt_id"] == s["attempt_history"][0]
            and task["result"] == {"output": {"published": True}, "writes": []}
            and task["input"] == expected_values,
            "Original must persist one actual successful attempt and result")
    creation = raw["journal"][0]
    require(creation[1:5] == ("thread", thread, None, None)
            and integer(creation[5]) == 1 and creation[6] is None
            and integer(creation[7]) == integer(t[2]) and creation[8] is None,
            "Thread creation journal must describe the actual HTTP-created thread")
    statuses = ["accepted", "running", "waiting_for_approval", "running", "running", "completed"]
    journals = raw["journal"][1:]
    times = []
    for revision, (row, status) in enumerate(zip(journals, statuses)):
        timestamp = integer(row[7])
        require(row[0] == creation[0] + revision + 1
                and row[1:4] == ("admit" if revision == 0 else "commit", thread, execution)
                and integer(row[4]) == revision and integer(row[5]) == 1
                and json.loads(row[6]) == status
                and json.loads(row[8]) == {"execution_id": execution, "revision": revision,
                     "owner_generation": 1, "status": status, "updated_at_ms": timestamp},
                "Original journal columns and typed bodies must match all six real versions")
        times.append(timestamp)
    require(times == sorted(times) and times[0] >= integer(t[2]) and s["updated_at_ms"] == times[5],
            "Original journal times must match creation and completion")
    grow = raw["approval_grants"][0]
    arow = raw["permission_acceptances"][0]
    drow = raw["permission_decisions"][0]
    g, a, d = json.loads(grow[7]), json.loads(arow[7]), json.loads(drow[5])
    require(grow[:5] == (execution, pending["interruptId"], TASK, thread, t[6])
            and integer(grow[5]) == 2 and grow[6] == 1 and g["consumed"] is True
            and g["execution_id"] == execution and g["interrupt_id"] == pending["interruptId"]
            and g["token"] == pending["grant"] and g["resume_key"] == pending["resumeKey"]
            and g["thread_id"] == thread and g["thread_generation"] == t[6]
            and g["task_id"] == TASK and g["effect_id"] == EFFECT
            and g["route"] == ROUTE and g["definition_revision"] == "definition-1"
            and g["policy_revision"] == "policy-1" and g["origin"] == ORIGIN
            and g["task_parking_revision"] == 2 and g["prompt"] == PROMPT,
            "Consumed grant must match the actual pending HTTP approval")
    before = a["before_snapshot"]
    require(arow[:4] == (execution, s["turn_id"], thread, t[6])
            and integer(arow[4]) == 2 and integer(arow[5]) == 3 and arow[6] == 1
            and a["record_version"] == 1 and a["thread_id"] == thread
            and a["thread_generation"] == t[6] and a["turn_id"] == s["turn_id"]
            and a["origin"] == ORIGIN and a["application"] == APPLICATION
            and a["policy_revision"] == "policy-1" and a["committed_revision"] == 3
            and a["facts"] == {"now_ms": times[3]}
            and before["execution_id"] == execution and before["revision"] == 2
            and before["status"] == "waiting_for_approval" and before["owner_generation"] == 1
            and before["definition"] == definition and before["values"] == s["values"]
            and before["attempts_used"] == 0 and before["attempt_history"] == []
            and before["updated_at_ms"] == times[2]
            and len(before["tasks"]) == 1,
            "Permission acceptance must retain the actual Waiting2 and Always commit3")
    resource_id(before["turn_id"])
    require(before["turn_id"] != s["turn_id"]
            and before["turn_history"] == [before["turn_id"]]
            and s["turn_history"] == [before["turn_id"], s["turn_id"]],
            "HTTP resume must append a distinct actual turn")
    parked = before["tasks"][0]
    require(parked["spec"] == spec and parked["status"] == "waiting_for_approval"
            and not parked["approved"] and parked["attempt_id"] is None
            and parked["result"] is None and parked["input"] == task["input"]
            and parked["interrupt"]["id"] == pending["interruptId"]
            and parked["interrupt"]["prompt"] == PROMPT,
            "Waiting2 task must retain the real parked approval without an attempt")
    binding = {"effect_id": EFFECT, "tool_id": "release-tool", "tool_revision": "tool-1",
               "constraint_revision": "constraint-1"}
    require(a["decisions"] == [{"task_id": TASK, "interrupt_id": pending["interruptId"],
                "grant_ref": {"execution_id": execution, "interrupt_id": pending["interruptId"]},
                "decision": "always", "proposal_digest": g["proposal_digest"],
                "task_parking_revision": 2, "binding": binding, "permission_id": d["permission_id"]}],
            "Always acceptance must link the grant, binding, and saved decision")
    require(drow[:3] == (d["permission_id"], thread, t[6])
            and drow[3] == d["scope_digest"] and drow[4] == d["record_version"] == 1
            and d["creator_execution_id"] == execution and d["creator_turn_id"] == s["turn_id"]
            and d["creator_task_id"] == TASK and d["creator_interrupt_id"] == g["interrupt_id"]
            and d["scope"] == {"thread_id": thread, "thread_generation": t[6], "origin": ORIGIN,
                "application": APPLICATION, "route": ROUTE, "definition": definition,
                "effect_id": EFFECT, "tool_id": "release-tool", "tool_revision": "tool-1",
                "policy_revision": "policy-1", "constraint_revision": "constraint-1"},
            "Saved permission SQL keys and creator/scope provenance must match Always acceptance")
    require(task["interrupt"] is not None and task["interrupt"]["id"] == pending["interruptId"],
            "Completed task must retain its original approval identity")
    return execution, d


def geometry(value):
    """Compact UTF-8 bytes, values, canonical nodes (including keys), depth."""
    def walk(item, depth):
        values, nodes, deepest = 1, 1, depth
        if isinstance(item, dict):
            nodes += len(item)
            children = item.values()
        elif isinstance(item, list):
            children = item
        else:
            children = ()
        for child in children:
            child_values, child_nodes, child_depth = walk(child, depth + 1)
            values += child_values
            nodes += child_nodes
            deepest = max(deepest, child_depth)
        return values, nodes, deepest

    values, nodes, depth = walk(value, 0)
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")), values, nodes, depth


def verify(binary, nested_inputs=False, text_inputs=False):
    require(not (nested_inputs and text_inputs),
            "Nested and text input profiles are mutually exclusive")
    require(binary.is_file() and os.access(binary, os.X_OK),
            "Explicit example executable is absent or not executable")
    original_input = {"month": "2026-09"}
    reuse_input = {"month": "2026-10"}
    if nested_inputs:
        # Independent literals from the structured HTTP integration scenario.
        original_input = {
            "report": {
                "title": "月次 Café 🚀",
                "sections": [
                    {"name": "north", "values": [1, 2, None],
                     "options": {"enabled": True, "limit": 3, "cursor": None}},
                    {"name": "南", "values": [3, 4, 5],
                     "options": {"enabled": False, "limit": None, "cursor": "次"}},
                ],
            },
            "options": {"notify": ["ops", "分析"], "dryRun": False, "note": None},
        }
        reuse_input = {"report": {"month": "十一月", "publish": True},
                       "options": {"notify": ["分析", None], "cursor": None}}
        original_geometry = geometry(original_input)
        reuse_geometry = geometry(reuse_input)
        require((original_geometry[0], original_geometry[1], original_geometry[3]) == (294, 30, 5),
                "Nested original input geometry differs")
        require((reuse_geometry[0], reuse_geometry[1], reuse_geometry[3]) == (98, 9, 3),
                "Nested reuse input geometry differs")
    elif text_inputs:
        # Independent literals from the finite ASCII text HTTP scenario.
        original_input = {"notes": "a" * 2048}
        reuse_input = {"notes": "b" * 2048}
        require(original_input != reuse_input
                and all(value["notes"].isascii() and len(value["notes"]) == 2048
                        and len(value["notes"].encode("utf-8")) == 2048
                        for value in (original_input, reuse_input)),
                "Text inputs must retain distinct literal 2 KiB ASCII notes")
        original_geometry = geometry(original_input)
        reuse_geometry = geometry(reuse_input)
        require(all((measured[0], measured[1], measured[3]) == (2060, 2, 1)
                    for measured in (original_geometry, reuse_geometry)),
                "Text input geometry differs")
    children = []
    temporary = tempfile.TemporaryDirectory(prefix="b4-http-always-example-")
    root = Path(temporary.name).resolve()
    token = secrets.token_urlsafe(32)
    env = dict(os.environ)
    env.pop("B4RUN_TOKEN", None)
    env.update(B4RUN_PORT="0", B4RUN_DATA_DIR=str(root / "data"))

    def start(settings):
        child = Child(binary, MODE, settings)
        children.append(child)
        return child

    def retire(child, address):
        require(child.finish(interrupt=True) == 0, "Server normal interrupt shutdown failed")
        listener_retired(address)
        child.cleanup()

    try:
        for index, extra in enumerate([{}, {"B4RUN_TOKEN": ""}, {"B4RUN_TOKEN": " \t "},
                                       {"B4RUN_TOKEN": token, "B4RUN_PORT": "invalid"}]):
            directory = root / ("invalid-" + str(index))
            child = start(dict(env, **extra, B4RUN_DATA_DIR=str(directory)))
            require(child.finish() != 0, "Invalid serve-approvals config was accepted")
            require(not directory.exists() and b'"ready"' not in child.buffers["stdout"],
                    "Invalid config opened storage or reported readiness")
            child.cleanup()
        data = Path(env["B4RUN_DATA_DIR"])
        require(not data.exists(), "Initial data directory must be fresh and absent")
        child = start(dict(env, B4RUN_TOKEN=token))
        try:
            address = ready_address(child)
        except RuntimeError:
            raise RuntimeError("Valid serve-approvals launch failed before JSON readiness; sensitive output suppressed") from None
        require(exchange(address, "GET", "/b4/capabilities")[0] == 401,
                "Anonymous capabilities request was accepted")
        status, capabilities = exchange(address, "GET", "/b4/capabilities", token=token)
        features = capabilities["features"]
        require(status == 200 and capabilities["descriptor_version"] == 4
                and features["durable_permission_creation"] is True
                and features["durable_permission_reuse"] is True
                and all(features[key] is False for key in ("always_permissions", "trusted_known_id_recovery",
                                                           "fenced_cancellation", "fenced_deletion"))
                and capabilities["application"] == {"id": APP, "revision": "application-1"},
                "Authenticated discovery must advertise the scoped V4 permission host")
        status, created = exchange(address, "POST", "/threads", {"metadata": {}}, token)
        require(status == 200 and created["status"] == "idle" and created["metadata"] == {},
                "HTTP must create a genuine owner thread")
        thread = resource_id(created["thread_id"])
        path = "/threads/" + thread
        body = {"route": ROUTE, "input": original_input}
        require(exchange(address, "POST", path + "/runs/wait", body)[0] == 401,
                "Anonymous run request was accepted")
        require(exchange(address, "POST", path + "/runs/wait", body, token) == (200, None),
                "Original HTTP run must park and return null")
        status, state = exchange(address, "GET", path + "/state", token=token)
        require(status == 200 and state["metadata"]["revision"] == 2
                and state["values"] == {"input": original_input},
                "Original HTTP run must expose actual Waiting2 input")
        status, pending = exchange(address, "GET", path + "/pending_interrupts", token=token)
        require(status == 200 and len(pending["interrupts"]) == 1,
                "Expected one actual pending interrupt")
        interrupt = pending["interrupts"][0]
        require(isinstance(interrupt["interruptId"], str) and bool(interrupt["interruptId"])
                and interrupt["value"] == PROMPT and isinstance(interrupt["grant"], str)
                and interrupt["grant"].startswith("b4ag_") and isinstance(interrupt["resumeKey"], str),
                "Pending endpoint must supply the genuine approval token and prompt")
        resume = {"route": ROUTE, "resume": [{"status": "resolved", "interruptId": interrupt["interruptId"],
                                             "payload": "always", "grant": interrupt["grant"]}]}
        exchange(address, "POST", path + "/resume", resume, token, sse=True)
        retire(child, address)
        baseline = capture(data)
        original, decision = original_state(baseline, thread, interrupt, state, expected_input=original_input)
        if nested_inputs:
            # Inspect the real acceptance only after owner1 has normally retired.
            parked = json.loads(baseline["permission_acceptances"][0][7])["before_snapshot"]
            require(parked["values"] == parked["tasks"][0]["input"] == {"input": original_input},
                    "Nested parked snapshot must retain the independent literal input")
            parked_geometry = geometry(parked)
            require(parked_geometry[0] <= 4096 and 91 < parked_geometry[2] <= 512,
                    "Actual nested parked snapshot exceeds qualified geometry")
        elif text_inputs:
            # Inspect the real acceptance only after owner1 has normally retired.
            parked = json.loads(baseline["permission_acceptances"][0][7])["before_snapshot"]
            require(parked["values"] == parked["tasks"][0]["input"] == {"input": original_input},
                    "Text parked snapshot must retain the independent literal input")
            parked_geometry = geometry(parked)
            require(4096 < parked_geometry[0] <= 8192 and parked_geometry[2] <= 512
                    and parked_geometry[3] <= 16,
                    "Actual text parked snapshot exceeds qualified geometry")
        child = start(dict(env, B4RUN_TOKEN=token))
        address = ready_address(child)
        reuse_body = {"route": ROUTE, "input": reuse_input}
        require(exchange(address, "POST", path + "/runs/wait", reuse_body, token)
                == (200, {"published": True}), "Restarted server must reuse the saved permission")
        status, pending = exchange(address, "GET", path + "/pending_interrupts", token=token)
        require(status == 200 and pending["interrupts"] == [], "Reuse requested replacement approval")
        retire(child, address)
        current = capture(data)
        require(len(current["metadata"]) == len(current["threads"]) == len(current["permission_uses"]) == 1
                and len(current["executions"]) == 2 and len(current["journal"]) == 12,
                "Restart must add exactly one execution and one real permission Use")
        reused_state(baseline, baseline, current, thread, original, decision, 2, "2026-10",
                     expected_input=reuse_input)
        snapshots = [json.loads(row[4]) for row in current["executions"]]
        require(len({snapshot["turn_id"] for snapshot in snapshots}) == 2
                and len({snapshot["attempt_history"][0] for snapshot in snapshots}) == 2,
                "Original and reused runs must have distinct actual turn and attempt identities")
        require(all(token.encode() not in bytes(buffer) and interrupt["grant"].encode() not in bytes(buffer)
                    for child in children for buffer in child.buffers.values()),
                "Child disclosed sensitive authorization material; output suppressed")
    finally:
        try:
            for child in children:
                if not child.closed:
                    child.cleanup()
        finally:
            temporary.cleanup()
    require(not root.exists(), "Temporary database directory was not removed")
    if nested_inputs or text_inputs:
        # exchange uses these exact default-ASCII/spaced json.dumps bodies unchanged.
        original_wire_bytes = len(json.dumps(body).encode("ascii"))
        reuse_wire_bytes = len(json.dumps(reuse_body).encode("ascii"))
        print(json.dumps({
            "status": "ok", "profile": "http_text_inputs" if text_inputs else "http_structured_inputs",
            "original": "Complete5", "reuse": "Complete4", "permission_uses": 1,
            "normal_owners": 2, "integrity": "ok", "removed": True,
            "input_canonical_utf8_bytes": original_geometry[0],
            "input_value_nodes": original_geometry[1], "input_depth": original_geometry[3],
            "reuse_canonical_utf8_bytes": reuse_geometry[0],
            "reuse_value_nodes": reuse_geometry[1], "reuse_depth": reuse_geometry[3],
            "original_http_body_bytes": original_wire_bytes, "reuse_http_body_bytes": reuse_wire_bytes,
            "parked_canonical_utf8_bytes": parked_geometry[0],
            "parked_canonical_nodes": parked_geometry[2],
            "evidence": "committed-attempts-and-results",
        }, sort_keys=True))
    else:
        print("http-always-example: original=Complete5 owners=1,2 reused=Complete4 uses=1 normal_shutdowns=2 integrity=ok removed=true")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--example", required=True, type=Path, help="Explicit already-built executable")
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--nested-inputs", action="store_true",
                        help="Qualify distinct nested original/reuse inputs and measure their geometry")
    inputs.add_argument("--text-inputs", action="store_true",
                        help="Qualify distinct 2 KiB ASCII original/reuse notes and measure their geometry")
    args = parser.parse_args()
    try:
        verify(args.example.expanduser().resolve(), nested_inputs=args.nested_inputs,
               text_inputs=args.text_inputs)
    except RuntimeError as error:
        print("http-always-example: FAILED: " + str(error), file=__import__("sys").stderr)
        raise SystemExit(1) from None
    except (OSError, ValueError, KeyError, IndexError, TypeError, sqlite3.Error, subprocess.SubprocessError):
        print("http-always-example: FAILED (sensitive child and HTTP output suppressed)",
              file=__import__("sys").stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
