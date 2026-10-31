#!/usr/bin/env python3
"""Qualify the compiled native SQLite application without recording authority."""
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

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from check_http_approval_example import frames, require
from check_attach_http_example import inactive_frames

ROOT = Path(__file__).resolve().parents[1]


def stop_child(child, startup, token):
    try:
        if child.poll() is None:
            child.terminate()
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill()
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                raise RuntimeError("Native example child cleanup failed") from None
        logs = startup + child.stdout.read() + child.stderr.read()
        require(token not in logs and "b4ag_" not in logs,
                "Native example logged sensitive authority")
    finally:
        child.stdout.close()
        child.stderr.close()


def capabilities(raw, private):
    require(len(raw) <= 65536, "Native descriptor exceeded its fixed response bound")
    value = json.loads(raw)
    schemas = ROOT / "contracts/agent-protocol/schemas"
    registry = Registry()
    for name in ("capabilities.json", "capabilities-v2.json"):
        path = schemas / name
        registry = registry.with_resource(path.as_uri(), Resource.from_contents(json.loads(path.read_text())))
    validator = Draft202012Validator({"$ref": (schemas / "capabilities-v2.json").as_uri()}, registry=registry)
    require(validator.is_valid(value), "Native descriptor schema differs")
    require(value["application"] == {"id": "native-demo", "revision": "native-demo-v1"}
            and value["execution"]["native_callbacks"] is True
            and value["execution"]["approval_service_mode"] == "durable_required"
            and value["store"] == {"kind": "sqlite", "durable_runtime": True, "durable_approvals": True},
            "Native example assembly descriptor differs")
    require(all(item.encode("utf-8") not in raw for item in private),
            "Descriptor disclosed private application or authority data")
    return value


def verify(binary):
    token = secrets.token_urlsafe(32)
    settings = dict(os.environ)
    settings.pop("B4RUN_TOKEN", None)
    missing = subprocess.run([binary], env=settings, capture_output=True, text=True, timeout=5)
    require(missing.returncode != 0 and "B4RUN_TOKEN is required" in missing.stderr,
            "Native example accepted missing token configuration")

    with tempfile.TemporaryDirectory(prefix="b4-native-example-") as directory:
        def stop(child, startup=""):
            stop_child(child, startup, token)

        def start():
            env = dict(settings, B4RUN_TOKEN=token, B4RUN_PORT="0", B4RUN_DATA_DIR=directory)
            child = subprocess.Popen([binary], env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, text=True)
            startup = ""
            try:
                require(bool(select.select([child.stdout], [], [], 8)[0]), "Native example startup timed out")
                startup = child.stdout.readline()
                match = re.search(r"http://(127\.0\.0\.1):(\d+)", startup)
                require(match is not None, "Native example startup failed; sensitive output suppressed")
                return child, startup, match[1], int(match[2])
            except BaseException:
                stop(child, startup)
                raise

        def request(method, path, body=None, auth=True, extra=None):
            connection = http.client.HTTPConnection(host, port, timeout=8)
            headers = {"Authorization": "Bearer " + token} if auth else {}
            headers.update(extra or {})
            try:
                connection.request(method, path, None if body is None else json.dumps(body), headers)
                response = connection.getresponse()
                return response.status, dict(response.getheaders()), response.read()
            finally:
                connection.close()

        def create():
            status, _, raw = request("POST", "/threads", {"metadata": {"note": "native-example"}})
            require(status == 200, "Native thread creation failed")
            return "/threads/" + json.loads(raw)["thread_id"]

        def state(path):
            status, _, raw = request("GET", path + "/state")
            require(status == 200 and b"b4ag_" not in raw, "Native ordinary state failed or exposed authority")
            return json.loads(raw)

        def pending(path):
            status, headers, raw = request("GET", path + "/pending_interrupts")
            require(status == 200 and headers.get("cache-control") == "no-store", "Native pending disclosure failed")
            return json.loads(raw)["interrupts"]

        child, startup, host, port = start()
        try:
            require(request("GET", "/b4/capabilities", auth=False)[0] == 401, "Anonymous discovery succeeded")
            for extra in [{"Origin": "https://foreign.invalid"}, {"Host": "foreign.invalid"}]:
                require(request("GET", "/b4/capabilities", extra=extra)[0] == 403, "Native browser guard failed")
            status, headers, raw = request("GET", "/b4/capabilities")
            require(status == 200 and headers.get("cache-control") == "no-store", "Protected native discovery failed")
            descriptor = capabilities(raw, [token, directory, "/native#workflow", "/approve#workflow", "b4ag_", "native-demo-policy-v1"])

            path = create()
            for submitted, stream in [({"text": "雪"}, False), (None, True)]:
                suffix = "/runs/stream" if stream else "/runs/wait"
                status, _, raw = request("POST", path + suffix, {"route": "/native#workflow", "input": submitted})
                expected = {"task": "second", "input": submitted}
                require(status == 200 and (frames(raw) == [("done", {"output": expected})] if stream else json.loads(raw) == expected),
                        "Native callback output/input or definition-order terminal differs")
            status, _, raw = request("POST", path + "/runs/wait", {"route": "/native#workflow"})
            require(status == 200 and json.loads(raw) == {"task": "second", "input": {}}, "Omitted native input differs")
            completed_state = state(path)
            require(completed_state["values"]["items"] == ["first", "second"], "Native joins lost definition order")
            require(request("POST", path + "/runs/wait", {"route": "/native#workflow", "input": []})[0] == 400,
                    "Native application validator accepted invalid input")
            require(state(path) == completed_state, "Rejected native input mutated the execution")
            rejected = "/threads/native-rejected"
            status, _, raw = request("POST", rejected + "/runs/wait", {"route": "/native#workflow", "input": False})
            require(status == 400 and json.loads(raw)["error"]["details"]["code"] == "invalid_application_input"
                    and request("GET", rejected)[0] == 404, "Invalid input created an implicit thread")
            require(request("POST", rejected + "/runs/wait", {"route": "/missing#workflow"})[0] == 404,
                    "Missing native route visibility differs")

            approved_path = create()
            status, _, raw = request("POST", approved_path + "/runs/wait", {"route": "/approve#workflow"})
            require(status == 200 and json.loads(raw) is None, "Native approval did not park")
            entry = pending(approved_path)[0]
            decision = {"route": "/approve#workflow", "resume": [{"status": "resolved", "interruptId": entry["interruptId"], "payload": "once", "grant": entry["grant"]}]}
            status, _, raw = request("POST", approved_path + "/resume", decision)
            require(status == 200 and frames(raw) == [("done", {"output": {"released": True}})]
                    and b"b4ag_" not in raw, "Native Once did not settle without authority disclosure")
            approved_state = state(approved_path)
            require(approved_state["values"]["items"] == ["prepared", "released"]
                    and pending(approved_path) == [], "Native approval repeated a sibling or left a pending token")
            require(request("POST", approved_path + "/resume", decision)[0] == 409, "Native grant replay succeeded")

            parked_path = create()
            status, _, raw = request("POST", parked_path + "/runs/stream", {"route": "/approve#workflow"})
            events = frames(raw)
            require(status == 200 and [name for name, _ in events] == ["interrupt", "done"]
                    and events[-1] == ("done", {"output": None}), "Native approval stream did not park")
            parked_pending = pending(parked_path)
            parked_state = state(parked_path)
            require(events[0][1] == parked_pending[0], "Native stream/pending grant identities differ")
            require(request("GET", "/readyz", auth=False)[0] == 200, "Expected native refusals faulted the host")
        finally:
            stop(child, startup)

        child, startup, host, port = start()
        try:
            status, headers, raw = request("GET", "/b4/capabilities")
            require(status == 200 and headers.get("cache-control") == "no-store"
                    and capabilities(raw, [token, directory, "b4ag_"]) == descriptor, "Descriptor changed after reopening the example")
            for stored_path, stored_state in [(path, completed_state), (approved_path, approved_state)]:
                require(state(stored_path) == stored_state, "Native persisted state changed after process reopen")
                status, _, raw = request("GET", stored_path + "/runs/stream")
                require(status == 200, "Native reopened inactive attachment failed")
                inactive_frames(raw)
            require(request("GET", parked_path + "/state")[0] == 501
                    and request("GET", parked_path + "/pending_interrupts")[0] == 501,
                    "Prior-owner ordinary reads bypassed the existing recovery gate")
            status, _, raw = request("GET", parked_path + "/runs/stream")
            require(status == 200, "Native prior-owner parked attachment failed")
            parked_attachment = inactive_frames(raw)
            require(parked_attachment["interrupts"] == parked_pending
                    and parked_attachment["values"] == parked_state["values"]
                    and parked_attachment["status"] == "interrupted",
                    "Native read-only prior-owner attachment changed pending identity or state")
            require(request("GET", "/readyz", auth=False)[0] == 200, "Native reopened host was unready")
        finally:
            stop(child, startup)

    return {"profile": "immutable_native_application_sqlite_http_example", "passed": True,
            "transport": "real_loopback_tcp", "backend": "sqlite", "provider_calls": 0,
            "capability_descriptor_version": descriptor["descriptor_version"],
            "shutdown_metadata": {
                "supervised_admission_drain": descriptor["features"]["supervised_admission_drain"],
                "graceful_shutdown": descriptor["features"]["graceful_shutdown"],
                "default_wait_ms": descriptor["controls"]["shutdown_default_wait_ms"],
                "max_wait_ms": descriptor["controls"]["shutdown_max_wait_ms"],
            },
            "checks": ["missing_token_rejected", "protected_discovery_browser_guards", "route_free_bounded_no_store_descriptor",
                       "native_wait_and_stream_callbacks", "null_and_omitted_input", "definition_order_parallel_join",
                       "invalid_input_no_admission_or_implicit_thread", "missing_native_route_uniform_refusal",
                       "required_once_continuation", "completed_sibling_preserved", "grant_replay_refused",
                       "interrupt_then_null_done", "healthy_expected_refusals", "descriptor_and_settled_states_preserved_after_process_reopen",
                       "read_only_prior_owner_attachment_pending_unchanged", "prior_owner_ordinary_reads_require_recovery", "reopened_inactive_state_retry_null_done"],
            "tokens_recorded": False, "reference_execution": False,
            "limitations": ["Provider-free native callbacks only; exact trusted known-ID unfinished recovery and actual crash boundaries are separately qualified. No automatic scan, native live chunks, sandbox or full core-profile compatibility."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/examples/serve_native")
    args = parser.parse_args()
    print(json.dumps(verify(str(args.binary.resolve())), indent=2))
