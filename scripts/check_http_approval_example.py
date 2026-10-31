#!/usr/bin/env python3
"""Verify the local SQLite approval example without logging grants or credentials."""
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


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def frames(raw):
    events = []
    for frame in raw.decode("utf-8").split("\n\n"):
        name = None
        data = []
        for line in frame.splitlines():
            if line.startswith("event:"):
                name = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
        if name is not None:
            events.append((name, json.loads("\n".join(data))))
    return events


def verify(binary):
    token = secrets.token_urlsafe(32)
    settings = dict(os.environ)
    settings.pop("B4RUN_TOKEN", None)
    missing = subprocess.run([binary], env=settings, capture_output=True, text=True, timeout=5)
    require(missing.returncode != 0, "Missing token configuration was accepted")
    require("B4RUN_TOKEN is required" in missing.stderr, "Missing token configuration lacks its fixed error")
    child = None

    def request(host, port, method, path, body=None, auth=True, extra=None):
        connection = http.client.HTTPConnection(host, port, timeout=8)
        headers = {"Authorization": "Bearer " + token} if auth else {}
        headers.update(extra or {})
        try:
            connection.request(method, path, body=None if body is None else json.dumps(body), headers=headers)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    with tempfile.TemporaryDirectory(prefix="b4-http-approval-example-") as directory:
        env = dict(settings, B4RUN_TOKEN=token, B4RUN_PORT="0", B4RUN_DATA_DIR=directory)
        child = subprocess.Popen([binary], env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            require(bool(select.select([child.stdout], [], [], 8)[0]), "Approval example startup timed out")
            startup = child.stdout.readline()
            match = re.search(r"http://(127\.0\.0\.1):(\d+)", startup)
            require(match is not None, "Approval example failed to start; sensitive output suppressed")
            host, port = match[1], int(match[2])
            require(request(host, port, "POST", "/threads", {}, auth=False)[0] == 401, "Anonymous creation was accepted")
            require(request(host, port, "POST", "/threads", {}, extra={"Origin":"https://foreign.invalid"})[0] == 403, "Browser origin was accepted")
            require(request(host, port, "POST", "/threads", {}, extra={"Host":"foreign.invalid"})[0] == 403, "Unexpected host was accepted")

            def create():
                status, _, raw = request(host, port, "POST", "/threads", {"metadata":{"fixture":"approval"}})
                require(status == 200, "Thread creation failed")
                return "/threads/" + json.loads(raw)["thread_id"]

            def pending(path):
                status, headers, raw = request(host, port, "GET", path + "/pending_interrupts")
                require(status == 200 and headers.get("cache-control") == "no-store", "Pending disclosure failed")
                return json.loads(raw)["interrupts"]

            def decision(entry, payload, include_grant=True):
                value = {"status":"resolved", "interruptId":entry["interruptId"], "payload":payload}
                if include_grant:
                    value["grant"] = entry["grant"]
                return {"route":"/approve#workflow", "resume":[value]}

            path = create()
            status, _, raw = request(host, port, "POST", path + "/runs/wait", {"route":"/approve#workflow", "input":False})
            require(status == 200 and json.loads(raw) is None, "Wait did not settle at a durable park")
            prompts = pending(path)
            require(len(prompts) == 1 and prompts == pending(path), "Pending identity is unstable")
            require(all(key in prompts[0] for key in ("interruptId","resumeKey","value","grant")), "Pending projection lacks required fields")
            status, _, state_raw = request(host, port, "GET", path + "/state")
            require(status == 200 and b"b4ag_" not in state_raw, "Ordinary state disclosed a grant")
            status, _, missing_raw = request(host, port, "POST", path + "/resume", decision(prompts[0], "once", False))
            require(status == 400 and json.loads(missing_raw)["error"]["details"]["code"] == "grant_required", "Missing grant was not rejected")
            require(prompts == pending(path), "Rejected decision changed pending grants")

            approved = decision(prompts[0], "once")
            status, _, raw = request(host, port, "POST", path + "/resume", approved)
            require(status == 200 and frames(raw) == [("done", {"output":{"released":True}})], "Accepted continuation did not settle once")
            require(b"b4ag_" not in raw, "Completion disclosed a grant")
            require(pending(path) == [], "Consumed grant remained pending")
            status, _, raw = request(host, port, "GET", path + "/state")
            require(status == 200 and json.loads(raw)["values"]["items"] == ["prepared","released"], "Sibling state was replayed or lost")
            require(b"b4ag_" not in raw, "Completed state disclosed a grant")
            require(request(host, port, "POST", path + "/resume", approved)[0] == 409, "Approval replay was accepted")

            denied_path = create()
            status, _, raw = request(host, port, "POST", denied_path + "/runs/stream", {"route":"/approve#workflow"})
            events = frames(raw)
            require(status == 200 and [name for name, _ in events] == ["interrupt","done"], "Park stream has incorrect events")
            require(events[-1] == ("done", {"output":None}), "Park done has incorrect output")
            entry = pending(denied_path)[0]
            require(events[0][1] == entry, "Stream and pending projections disagree")
            status, _, raw = request(host, port, "POST", denied_path + "/resume", decision(entry, "deny"))
            require(status == 200 and frames(raw) == [("done", {"output":{"denied":True}})], "Denial did not settle without effect")
            status, _, raw = request(host, port, "GET", denied_path + "/state")
            require(status == 200 and json.loads(raw)["values"]["items"] == ["prepared"], "Denied effect changed state")
            require(request(host, port, "GET", "/readyz", auth=False)[0] == 200, "Expected decisions poisoned readiness")
        finally:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=8)
            logs = child.stdout.read() + child.stderr.read()
            child.stdout.close()
            child.stderr.close()
            require("b4ag_" not in logs and token not in logs, "Example logged sensitive authority")

    receipt = {
        "profile":"authenticated_http_required_approval_example",
        "passed":True,
        "transport":"real_loopback_tcp",
        "backend":"sqlite",
        "checks":["missing_token_rejected","authentication_and_browser_guards",
                  "wait_stable_park_null","identical_authorized_pending_no_store",
                  "ordinary_state_no_grants","missing_grant_spends_nothing",
                  "once_continuation","successful_sibling_preserved","replay_rejected",
                  "interrupt_before_null_done","deny_without_effect","healthy_readiness"],
        "tokens_recorded":False,
        "limitations":["Finite local scripted effects; no provider, automatic owner recovery, accepted-ready HTTP restart launch or full protocol compatibility."],
    }
    require("b4ag_" not in json.dumps(receipt, ensure_ascii=False) and token not in json.dumps(receipt), "Receipt disclosed sensitive authority")
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/examples/serve_approvals")
    args = parser.parse_args()
    print(json.dumps(verify(str(args.binary.resolve())), indent=2))
