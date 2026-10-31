#!/usr/bin/env python3
"""Independent local Linux image qualification with private authority handling."""

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import tempfile
import time
import uuid

from container_snapshot import (SnapshotError, compare_single_owner_reopen,
                                compare_unchanged, decode_private_snapshot,
                                public_snapshot_summary)

LABEL = "org.b4.qualification.task"
DESTINATION = "/var/lib/b4run"
SNAPSHOT_HELPER = "/opt/b4/container_snapshot.py"
INITIALIZER = "/opt/b4/container_initialize_volume.sh"
AUXILIARY_ENTRYPOINT = "/bin/sh"
AUXILIARY_ARGUMENTS = (
    "-c", "trap 'kill \"$child\" 2>/dev/null; wait \"$child\"; exit 0' INT TERM; "
    "sleep 120 & child=$!; wait \"$child\"",
)
CASE_NAMES = (
    "case_nonroot_native_runtime_identity", "case_fresh_volume_initialization",
    "case_hidden_nonempty_initializer_refusal", "case_private_parent_preflight",
    "case_public_commands_and_readonly_config", "case_public_probes_and_docker_health",
    "case_browser_bearer_capability_guards", "case_native_order_and_required_park",
    "case_live_owner_exclusion_and_coherent_rows", "case_sigterm_positive_retirement",
    "case_same_volume_epoch_only_restart", "case_readonly_prior_owner_parked_projection",
    "case_sigint_positive_retirement_and_reusable_volume",
)


class QualificationError(Exception):
    """Public failures contain fixed codes only."""


def require(condition, code):
    if not condition:
        raise QualificationError(code)


@dataclass(repr=False)
class Result:
    returncode: int
    stdout: bytes = b""
    stderr: bytes = b""


def validate_volume(volume, expected_name, task):
    require(set(volume) == {"name", "driver", "options", "label", "source"}
            and volume["name"] == expected_name and volume["label"] == task
            and volume["driver"] == "local" and volume["options"] in (None, {})
            and isinstance(volume["source"], str) and volume["source"].startswith("/"),
            "b4_container_volume_refused")


def validate_mount(container, volume, read_only=False):
    require(container.get("mounts") == [{"type": "volume", "name": volume["name"],
                                         "source": volume["source"], "destination": DESTINATION,
                                         "rw": not read_only}]
            and container.get("configured_mounts") == [{"type": "volume", "source": volume["name"],
                                                        "target": DESTINATION, "read_only": read_only,
                                                        "nocopy": True}]
            and container.get("network") == "none" and container.get("privileged") is False
            and container.get("readonly_root") is True,
            "b4_container_mount_refused")


def require_private_parent(metadata):
    require(metadata == {"kind": "directory", "mode": 0o700, "uid": 10001, "gid": 10001},
            "b4_container_parent_refused")


def positive_retirement(state, stdout, stderr):
    if not (state.get("status") == "exited" and state.get("running") is False
            and state.get("exit_code") == 0 and state.get("oom_killed") is False
            and not stderr and stdout.endswith(b"b4run_stopped\n")):
        return False
    try:
        lines = stdout.decode("ascii").splitlines()
    except UnicodeError:
        return False
    allowed = {"b4run_listening http://127.0.0.1:3001", "b4run_draining", "b4run_drain_incomplete", "b4run_stopped"}
    return (all(line in allowed for line in lines)
            and lines.count("b4run_listening http://127.0.0.1:3001") == 1
            and lines.count("b4run_draining") == 1 and lines.count("b4run_stopped") == 1)


def curl_config(method, path, token=None, headers=(), body=None):
    require(method in ("GET", "POST") and re.fullmatch(r"/[a-zA-Z0-9_/-]*", path) is not None,
            "b4_container_request_refused")
    lines = ["disable", "silent", "include", 'noproxy = "*"', "connect-timeout = 2", "max-time = 8", "max-filesize = 1048576",
             "request = " + json.dumps(method), "url = " + json.dumps("http://127.0.0.1:3001" + path)]
    if token is not None:
        require(isinstance(token, str) and token and all(33 <= ord(char) <= 126 for char in token),
                "b4_container_request_refused")
        lines.append("header = " + json.dumps("Authorization: Bearer " + token))
    for header in headers:
        require(isinstance(header, str) and "\n" not in header and "\r" not in header,
                "b4_container_request_refused")
        lines.append("header = " + json.dumps(header))
    if body is not None:
        lines.append("header = \"Content-Type: application/json\"")
        lines.append("data = " + json.dumps(json.dumps(body, ensure_ascii=True, separators=(",", ":"))))
    return ("\n".join(lines) + "\n").encode("ascii")


# Formats project fields at the daemon boundary. No unfiltered inspect object,
# Config.Env, health log, process error string or arbitrary label is collected.
INSPECT_FORMATS = {
    "volume": '{"name":{{json .Name}},"driver":{{json .Driver}},"options":{{json .Options}},"label":{{json (index .Labels "' + LABEL + '")}},"source":{{json .Mountpoint}}}',
    "image": (
        '{{$health := index .Config "Healthcheck"}}'
        '{"id":{{json .Id}},"os":{{json .Os}},"architecture":{{json .Architecture}},'
        '"user":{{json .Config.User}},"entrypoint":{{json (index .Config "Entrypoint")}},'
        '"command":{{json (index .Config "Cmd")}},"stop_signal":{{json (index .Config "StopSignal")}},'
        '"health_test":{{if $health}}{{with index $health "Test"}}{{json .}}{{else}}[]{{end}}{{else}}[]{{end}},'
        '"health_timeout_ns":{{if $health}}{{with index $health "Timeout"}}{{json .}}{{else}}0{{end}}{{else}}0{{end}}}'
    ),
    "container": '{{$health := index .State "Health"}}{"id":{{json .Id}},"name":{{json .Name}},"label":{{json (index .Config.Labels "' + LABEL + '")}},"image":{{json .Image}},"user":{{json .Config.User}},"working_dir":{{json .Config.WorkingDir}},"entrypoint":{{json (index .Config "Entrypoint")}},"status":{{json .State.Status}},"running":{{json .State.Running}},"exit_code":{{json .State.ExitCode}},"oom_killed":{{json .State.OOMKilled}},"health":{{if $health}}{{json (index $health "Status")}}{{else}}"none"{{end}},"network":{{json .HostConfig.NetworkMode}},"privileged":{{json .HostConfig.Privileged}},"readonly_root":{{json .HostConfig.ReadonlyRootfs}},"cap_add":{{json (index .HostConfig "CapAdd")}},"cap_drop":{{json (index .HostConfig "CapDrop")}},"mounts":[{{range $i,$m := .Mounts}}{{if $i}},{{end}}{"type":{{json $m.Type}},"name":{{json $m.Name}},"source":{{json $m.Source}},"destination":{{json $m.Destination}},"rw":{{json $m.RW}}}{{end}}],"configured_mounts":[{{range $i,$m := (index .HostConfig "Mounts")}}{{if $i}},{{end}}{"type":{{json $m.Type}},"source":{{json $m.Source}},"target":{{json $m.Target}},"read_only":{{with index $m "ReadOnly"}}{{json .}}{{else}}false{{end}},"nocopy":{{if $m.VolumeOptions}}{{json $m.VolumeOptions.NoCopy}}{{else}}false{{end}}}{{end}}]}',
}
INSPECT_KEYS = {
    "volume": {"name", "driver", "options", "label", "source"},
    "image": {"id", "os", "architecture", "user", "entrypoint", "command", "stop_signal", "health_test", "health_timeout_ns"},
    "container": {"id", "name", "label", "image", "user", "working_dir", "entrypoint", "status", "running", "exit_code", "oom_killed", "health", "network", "privileged", "readonly_root", "cap_add", "cap_drop", "mounts", "configured_mounts"},
}


class Docker:
    def __init__(self, runner=None, timeout=480):
        self.runner = runner
        self.deadline = time.monotonic() + timeout

    def run(self, arguments, input_bytes=None, allowed=(0,), timeout=30):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "b4_container_deadline")
        args = ["docker", *arguments]
        try:
            if self.runner is None:
                completed = subprocess.run(args, input=input_bytes, capture_output=True,
                                           timeout=min(timeout, remaining), check=False)
                result = Result(completed.returncode, completed.stdout, completed.stderr)
            else:
                result = self.runner(args, input=input_bytes, timeout=min(timeout, remaining))
        except Exception:
            raise QualificationError("b4_container_command_failed") from None
        require(result.returncode in allowed, "b4_container_command_failed")
        return result

    def inspect(self, kind, identity):
        require(kind in INSPECT_FORMATS, "b4_container_inspection_refused")
        command = ["volume", "inspect"] if kind == "volume" else (["image", "inspect"] if kind == "image" else ["inspect"])
        result = self.run([*command, "--format", INSPECT_FORMATS[kind], identity])
        try:
            value = json.loads(result.stdout)
            require(type(value) is dict and set(value) == INSPECT_KEYS[kind], "b4_container_inspection_failed")
            return value
        except Exception:
            raise QualificationError("b4_container_inspection_failed") from None


def stopped_source_allowed(states, observers):
    return bool(states) and observers == 0 and all(
        state.get("status") == "exited" and state.get("running") is False
        and state.get("oom_killed") is False and state.get("positive_exit") is True
        for state in states)


def public_receipt(cases, metadata):
    allowed = {"runtime_image_id", "qualification_image_id", "os", "architecture", "uid", "gid",
               "executable_sha256", "libraries", "packages", "package_verification", "volume_driver",
               "volume_nocopy", "parent_mode", "data_mode", "file_mode", "cleanup_completed"}
    require(set(metadata) <= allowed and set(cases) <= set(CASE_NAMES)
            and all(type(passed) is bool for passed in cases.values()), "b4_container_receipt_refused")
    for key, value in metadata.items():
        if key in ("runtime_image_id", "qualification_image_id", "executable_sha256"):
            pattern = r"[0-9a-f]{64}" if key == "executable_sha256" else r"sha256:[0-9a-f]{64}"
            valid = type(value) is str and re.fullmatch(pattern, value) is not None
        elif key in ("os", "architecture", "volume_driver", "parent_mode", "data_mode", "file_mode"):
            valid = value == {"os": "linux", "architecture": "arm64", "volume_driver": "local",
                              "parent_mode": "0700", "data_mode": "0700", "file_mode": "0600"}[key]
        elif key in ("uid", "gid"):
            valid = type(value) is int and value == 10001
        elif key in ("volume_nocopy", "cleanup_completed"):
            valid = type(value) is bool
        elif key == "package_verification":
            valid = _public_package_verification(value)
        elif key == "libraries":
            valid = type(value) is list and bool(value) and all(_public_library(item) for item in value)
        elif key == "packages":
            valid = type(value) is list and bool(value) and all(_public_package(item) for item in value)
        else:
            valid = False
        require(valid, "b4_container_receipt_refused")
    return {"profile": "local_linux_arm64_nonroot_loopback_container", "passed": len(cases) == len(CASE_NAMES) and all(cases.values()),
            "cases": dict(cases), "metadata": dict(metadata), "tokens_recorded": False,
            "raw_authority_recorded": False, "snapshot_digests_recorded": False, "logical_rows_only": True,
            "limitations": ["Local Linux arm64 only; no host ingress, deployment or Docker daemon secret isolation.",
                            "Logical SQLite rows only; no WAL/SHM byte preservation or forced-stop retirement claim.",
                            "Provider-free native demo; no automatic adoption, full operations, M0/CP or broad core parity."]}


def _public_library(item):
    if type(item) is not dict or set(item) != {"name", "path"}:
        return False
    if item == {"name": "linux-vdso.so.1", "path": None}:
        return True
    name, path = item["name"], item["path"]
    if type(name) is not str or type(path) is not str:
        return False
    return (re.fullmatch(r"(?:lib[a-zA-Z0-9_+.-]+\.so(?:\.[0-9]+)*|ld-linux-aarch64\.so\.1)", name) is not None
            and path in ("/lib/" + name, "/lib/aarch64-linux-gnu/" + name,
                         "/usr/lib/aarch64-linux-gnu/" + name))


def _public_package(item):
    return (type(item) is dict and set(item) == {"name", "version"}
            and type(item["name"]) is str and type(item["version"]) is str
            and re.fullmatch(r"[a-z0-9][a-z0-9+.-]{0,127}(?::arm64)?", item["name"]) is not None
            and re.fullmatch(r"[0-9][a-zA-Z0-9.+:~()-]{0,127}", item["version"]) is not None)


def classify_package_verification(result):
    code = "b4_container_package_verification_failed"
    require(result.returncode == 0 and not result.stderr and len(result.stdout) <= 1_048_576, code)
    counts = {category: 0 for category in PACKAGE_OMISSION_CATEGORIES}
    try:
        text = result.stdout.decode("ascii")
    except UnicodeError:
        raise QualificationError(code) from None
    require(not text or text.endswith("\n"), code)
    for line in text.splitlines():
        require(line.startswith("missing     "), code)
        path = line[len("missing     "):]
        require(re.fullmatch(r"/[a-zA-Z0-9_./+@:\[\]-]+", path) is not None
                and not any(part in ("", ".", "..") for part in path[1:].split("/")), code)
        category = None
        for candidate, root in PACKAGE_OMISSION_CATEGORIES.items():
            if candidate == "apt_lists":
                matched = path == root
            else:
                matched = path == root or path.startswith(root + "/")
            if matched:
                category = candidate
                break
        require(category is not None, code)
        counts[category] += 1
    total = sum(counts.values())
    return {"status": "slim_expected_omissions_only" if total else "no_reported_discrepancies",
            "expected_missing_count": total, "expected_missing_by_category": counts,
            "unexpected_missing_count": 0, "nonmissing_discrepancy_count": 0,
            "pristine_packages_claimed": False}


# These are omission categories in the reviewed pinned slim profile, not an
# exemption for arbitrary package failures or for runtime executable/library paths.
PACKAGE_OMISSION_CATEGORIES = {
    "documentation": "/usr/share/doc", "locale": "/usr/share/locale",
    "manuals": "/usr/share/man", "info": "/usr/share/info",
    "lintian": "/usr/share/lintian/overrides", "apt_lists": "/var/lib/apt/lists/partial",
}


def _public_package_verification(value):
    keys = {"status", "expected_missing_count", "expected_missing_by_category",
            "unexpected_missing_count", "nonmissing_discrepancy_count", "pristine_packages_claimed"}
    if type(value) is not dict or set(value) != keys:
        return False
    counts, total = value["expected_missing_by_category"], value["expected_missing_count"]
    if (type(counts) is not dict or set(counts) != set(PACKAGE_OMISSION_CATEGORIES)
            or any(type(count) is not int or not 0 <= count <= 100_000 for count in counts.values())
            or type(total) is not int or total != sum(counts.values())):
        return False
    status = "slim_expected_omissions_only" if total else "no_reported_discrepancies"
    return (value["status"] == status and type(value["unexpected_missing_count"]) is int
            and value["unexpected_missing_count"] == 0 and type(value["nonmissing_discrepancy_count"]) is int
            and value["nonmissing_discrepancy_count"] == 0 and value["pristine_packages_claimed"] is False)


@dataclass(repr=False)
class Resource:
    identity: str
    name: str
    role: str
    volume: str | None
    rw: bool
    positive_exit: bool = False
    removed: bool = False


def _json_private(raw, code):
    try:
        return json.loads(raw)
    except Exception:
        raise QualificationError(code) from None


def _http_response(raw):
    require(len(raw) <= 1_048_576, "b4_container_response_limit")
    head, separator, body = raw.partition(b"\r\n\r\n")
    require(bool(separator), "b4_container_response_failed")
    try:
        lines = head.decode("ascii").split("\r\n")
        status_code = int(lines[0].split()[1])
        headers = {}
        for line in lines[1:]:
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip().lower()
        return status_code, headers, body
    except Exception:
        raise QualificationError("b4_container_response_failed") from None


def _frames(raw):
    try:
        events = []
        for block in raw.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
            name = next((line[7:] for line in block.splitlines() if line.startswith("event: ")), None)
            if name is not None:
                payload = "\n".join(line[6:] for line in block.splitlines() if line.startswith("data: "))
                events.append((name, json.loads(payload)))
        return events
    except Exception:
        raise QualificationError("b4_container_stream_failed") from None


def _private_execution(captured, thread):
    try:
        for row in captured["tables"][2][2]:
            if row[1] == ["text", thread]:
                return json.loads(row[4][1])
    except Exception:
        pass
    raise QualificationError("b4_container_execution_missing")


def _stat_program():
    # Only fixed expected paths are inspected, with no source content returned.
    return '''import json,os,stat
def metadata(path):
 if not os.path.lexists(path): return None
 s=os.lstat(path)
 kind="directory" if stat.S_ISDIR(s.st_mode) else ("file" if stat.S_ISREG(s.st_mode) else "other")
 return {"kind":kind,"mode":stat.S_IMODE(s.st_mode),"uid":s.st_uid,"gid":s.st_gid}
root="/var/lib/b4run"
names=("runtime.sqlite3","owner.lock","runtime.sqlite3-wal","runtime.sqlite3-shm","runtime.sqlite3-journal")
print(json.dumps({"parent":metadata(root),"data":metadata(root+"/data"),"files":{n:metadata(root+"/data/"+n) for n in names}}))'''


class Qualification:
    """Own only identified resources created by this run; keep responses private."""

    def __init__(self, runtime_image, qualification_image, receipt, docker=None):
        self.docker = docker or Docker()
        self.task = str(uuid.uuid4())
        self.prefix = "b4q-" + self.task
        self.sequence = 0
        self.containers = []
        self.volumes = []
        self.container_intents = []
        self.observers = 0
        self.cases = {}
        self.metadata = {}
        self.runtime = self.docker.inspect("image", runtime_image)
        self.qualification = self.docker.inspect("image", qualification_image)
        for image in (self.runtime, self.qualification):
            require(image["os"] == "linux" and image["architecture"] == "arm64"
                    and image["user"] == "10001:10001"
                    and re.fullmatch(r"sha256:[0-9a-f]{64}", image["id"]) is not None,
                    "b4_container_image_refused")
        self.metadata.update(runtime_image_id=self.runtime["id"], qualification_image_id=self.qualification["id"],
                             os="linux", architecture="arm64", uid=10001, gid=10001)
        self.private = tempfile.TemporaryDirectory(prefix="b4-container-private-")
        os.chmod(self.private.name, 0o700)
        self.token = secrets.token_urlsafe(32)
        self.env_file = Path(self.private.name) / "runtime.env"
        self._write_private(self.env_file, ("B4RUN_TOKEN=" + self.token + "\nB4RUN_SHUTDOWN_WAIT_MS=50\n").encode("ascii"))
        try:
            self.receipt_fd = os.open(receipt, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.fchmod(self.receipt_fd, 0o600)
        except Exception:
            self.private.cleanup()
            raise QualificationError("b4_container_receipt_create_failed") from None
        self.owner = None
        self.volume = None

    @staticmethod
    def _write_private(path, raw):
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(raw)

    def _name(self, role):
        self.sequence += 1
        return f"{self.prefix}-{role}-{self.sequence}"

    def _owned_volume(self, name):
        value = self.docker.inspect("volume", name)
        validate_volume(value, name, self.task)
        require(name in self.volumes, "b4_container_volume_refused")
        return value

    def _new_volume(self, role):
        name = self._name(role)
        absent = self.docker.run(["volume", "inspect", "--format", "{{json .Name}}", name], allowed=(0, 1))
        require(absent.returncode == 1, "b4_container_volume_exists")
        # Record intent before the command: an unknown create outcome can still
        # be recovered by exact generated name and task label during cleanup.
        self.volumes.append(name)
        created = self.docker.run(["volume", "create", "--driver", "local", "--label", LABEL + "=" + self.task, name])
        require(created.stdout.strip() == name.encode("ascii"), "b4_container_volume_create_failed")
        return self._owned_volume(name)

    def _new_container(self, image, role, volume=None, read_only_mount=False,
                       user="10001:10001", entrypoint=None, arguments=(), token=False,
                       extra_environment=()):
        if volume is not None:
            volume = self._owned_volume(volume["name"])
        name = self._name(role)
        args = ["create", "--platform", "linux/arm64", "--pull", "never", "--name", name,
                "--label", LABEL + "=" + self.task, "--user", user, "--network", "none",
                "--workdir", "/", "--read-only", "--cap-drop", "ALL"]
        if user == "0:0" and role == "initialize":
            args.extend(["--cap-add", "CHOWN"])
        if volume is not None:
            mount = "type=volume,source=" + volume["name"] + ",target=" + DESTINATION + ",volume-nocopy"
            if read_only_mount:
                mount += ",readonly"
            args.extend(["--mount", mount])
        if token:
            args.extend(["--env-file", str(self.env_file)])
        for key, value in extra_environment:
            require(key == "B4RUN_PORT" and value == "invalid", "b4_container_environment_refused")
            args.extend(["--env", key + "=" + value])
        if entrypoint is not None:
            args.extend(["--entrypoint", entrypoint])
        args.extend([image["id"], *arguments])
        intent = Resource("", name, role, None if volume is None else volume["name"],
                          volume is not None and not read_only_mount)
        self.container_intents.append((intent, image["id"], user))
        created = self.docker.run(args)
        identity = created.stdout.strip().decode("ascii", errors="replace")
        require(re.fullmatch(r"[0-9a-f]{64}", identity) is not None, "b4_container_create_failed")
        resource = Resource(identity, name, role, None if volume is None else volume["name"],
                            volume is not None and not read_only_mount)
        self.containers.append(resource)
        selected = self._state(resource)
        require(selected["image"] == image["id"] and selected["user"] == user
                and selected["working_dir"] == "/"
                and selected["network"] == "none" and selected["privileged"] is False
                and selected["readonly_root"] is True and selected["cap_drop"] == ["ALL"]
                and (selected["cap_add"] == ["CHOWN"] if role == "initialize"
                     else selected["cap_add"] in ([], None)),
                "b4_container_process_refused")
        if volume is not None:
            validate_mount(selected, volume, read_only_mount)
        else:
            require(not selected["mounts"] and not selected["configured_mounts"], "b4_container_mount_refused")
        return resource

    def _state(self, resource):
        value = self.docker.inspect("container", resource.identity)
        require(value["id"] == resource.identity and value["name"] == "/" + resource.name
                and value["label"] == self.task and value["working_dir"] == "/",
                "b4_container_identity_refused")
        return value

    def _start(self, resource):
        self._state(resource)
        self.docker.run(["start", resource.identity])

    def _wait_exited(self, resource, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self._state(resource)
            if state["status"] == "exited" and state["running"] is False:
                return state
            time.sleep(0.1)
        raise QualificationError("b4_container_exit_timeout")

    def _logs(self, resource):
        self._state(resource)
        return self.docker.run(["logs", resource.identity])

    def _command(self, image, arguments, volume=None, token=False, entrypoint=None,
                 user="10001:10001", role="command", expected=0, extra_environment=()):
        if role == "initialize":
            require(volume is not None and user == "0:0", "b4_container_initializer_refused")
            self._no_active_writers(volume)
        resource = self._new_container(image, role, volume, user=user, entrypoint=entrypoint,
                                       arguments=arguments, token=token, extra_environment=extra_environment)
        self._start(resource)
        state = self._wait_exited(resource)
        logs = self._logs(resource)
        require(state["exit_code"] == expected and state["oom_killed"] is False,
                "b4_container_command_exit_failed")
        return resource, logs

    def _exec(self, resource, arguments, input_bytes=None, allowed=(0,)):
        require(self._state(resource)["running"] is True, "b4_container_exec_refused")
        return self.docker.run(["exec", "-i", resource.identity, *arguments], input_bytes=input_bytes, allowed=allowed)

    def _remove_auxiliary(self, resource):
        state = self._state(resource)
        if state["running"]:
            self.docker.run(["kill", "--signal", "SIGTERM", resource.identity])
            state = self._wait_exited(resource, timeout=12)
        require(state["status"] == "exited" and state["running"] is False
                and state["exit_code"] == 0 and state["oom_killed"] is False,
                "b4_container_observer_exit_failed")
        resource.positive_exit = True
        self.docker.run(["rm", resource.identity])
        resource.removed = True

    def _observe(self, arguments, volume=None, user="10001:10001"):
        helper = self._new_container(self.qualification, "observer", volume, read_only_mount=True,
                                     user=user, entrypoint=AUXILIARY_ENTRYPOINT, arguments=AUXILIARY_ARGUMENTS)
        self.observers += 1
        try:
            self._start(helper)
            result = self._exec(helper, arguments)
        finally:
            # No SQLite connection survives the completed exec. Do not retire
            # any source writer while an observer or its transport remains live.
            self._remove_auxiliary(helper)
            self.observers -= 1
        return result

    def _storage_metadata(self, volume):
        result = self._observe(["python3", "-B", "-c", _stat_program()], volume)
        return _json_private(result.stdout, "b4_container_metadata_failed")

    def _volume_container_set(self, volume):
        result = self.docker.run(["ps", "--all", "--no-trunc", "--filter", "volume=" + volume["name"], "--format", "{{.ID}}"])
        actual = set(result.stdout.decode("ascii").split())
        known = {resource.identity for resource in self.containers if not resource.removed and resource.volume == volume["name"]}
        require(actual == known, "b4_container_foreign_writer_refused")

    def _no_active_writers(self, volume):
        self._volume_container_set(volume)
        for resource in self.containers:
            if not resource.removed and resource.volume == volume["name"] and resource.rw:
                require(self._state(resource)["running"] is False, "b4_container_active_writer_refused")

    def _snapshot(self, mode="live"):
        if mode == "stopped":
            self._volume_container_set(self.volume)
            states = []
            for resource in self.containers:
                if not resource.removed and resource.volume == self.volume["name"] and resource.rw:
                    state = self._state(resource)
                    state["positive_exit"] = resource.positive_exit
                    states.append(state)
            require(stopped_source_allowed(states, self.observers), "b4_container_stopped_source_refused")
        result = self._observe(["python3", "-B", SNAPSHOT_HELPER, "--mode", mode], self.volume)
        try:
            return decode_private_snapshot(result.stdout)
        except SnapshotError:
            raise QualificationError("b4_container_snapshot_failed") from None

    def _request(self, method, path, auth=True, headers=(), body=None):
        configuration = curl_config(method, path, self.token if auth else None, headers, body)
        result = self._exec(self.owner, ["curl", "--disable", "--config", "-"], configuration)
        return _http_response(result.stdout)

    def _wait_ready(self, resource):
        self.owner = resource
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            state = self._state(resource)
            require(state["running"] is True, "b4_container_startup_failed")
            try:
                status_code, _, raw = self._request("GET", "/readyz", auth=False)
                if status_code == 200 and _json_private(raw, "b4_container_readiness_failed") == {"status": "ready"}:
                    return
            except QualificationError:
                pass
            time.sleep(0.2)
        raise QualificationError("b4_container_readiness_timeout")

    def _start_writer(self):
        require_private_parent(self._storage_metadata(self.volume)["parent"])
        resource = self._new_container(self.runtime, "writer", self.volume, token=True)
        require(self._state(resource)["entrypoint"] == ["/usr/local/bin/b4run"], "b4_container_pid1_refused")
        self._start(resource)
        self._wait_ready(resource)
        return resource

    def _check_private_storage(self):
        metadata = self._storage_metadata(self.volume)
        require_private_parent(metadata["parent"])
        require(metadata["data"] == {"kind": "directory", "mode": 0o700, "uid": 10001, "gid": 10001}, "b4_container_storage_modes_failed")
        require(metadata["files"]["runtime.sqlite3"] is not None and metadata["files"]["owner.lock"] is not None, "b4_container_storage_modes_failed")
        for value in metadata["files"].values():
            if value is not None:
                require(value == {"kind": "file", "mode": 0o600, "uid": 10001, "gid": 10001}, "b4_container_storage_modes_failed")
        self.metadata.update(parent_mode="0700", data_mode="0700", file_mode="0600")

    def _retire_writer(self, signal):
        require(signal in ("SIGINT", "SIGTERM") and self.observers == 0, "b4_container_signal_refused")
        resource = self.owner
        require(self._state(resource)["running"] is True, "b4_container_signal_refused")
        self.docker.run(["kill", "--signal", signal, resource.identity])
        state = self._wait_exited(resource)
        logs = self._logs(resource)
        require(positive_retirement(state, logs.stdout, logs.stderr), "b4_container_retirement_failed")
        resource.positive_exit = True

    def case_nonroot_native_runtime_identity(self):
        require(self.runtime["entrypoint"] == ["/usr/local/bin/b4run"]
                and self.runtime["command"] == ["serve", "--demo"] and self.runtime["stop_signal"] == "SIGTERM",
                "b4_container_runtime_config_failed")
        health = self.runtime["health_test"]
        require(self.runtime["health_timeout_ns"] == 4_000_000_000
                and health == ["CMD", "curl", "--disable", "--fail", "--silent", "--output", "/dev/null",
                           "--noproxy", "*", "--connect-timeout", "2", "--max-time", "3", "http://127.0.0.1:3001/readyz"],
                "b4_container_health_config_failed")
        probe = self._new_container(self.runtime, "image-probe", entrypoint=AUXILIARY_ENTRYPOINT,
                                    arguments=AUXILIARY_ARGUMENTS)
        self._start(probe)
        try:
            require(self._exec(probe, ["id", "-u"]).stdout == b"10001\n", "b4_container_nonroot_failed")
            minimal = 'for p in node nodejs cargo rustc python3; do if command -v "$p" >/dev/null 2>&1; then exit 1; fi; done; test ! -e /opt/b4/container_snapshot.py; test ! -e /opt/b4/container_initialize_volume.sh'
            require(self._exec(probe, ["/bin/sh", "-c", minimal]).returncode == 0, "b4_container_runtime_tools_failed")
            hashed = self._exec(probe, ["sha256sum", "/usr/local/bin/b4run"]).stdout.decode("ascii").split()
            require(len(hashed) == 2 and hashed[1] == "/usr/local/bin/b4run"
                    and re.fullmatch(r"[0-9a-f]{64}", hashed[0]) is not None, "b4_container_binary_hash_failed")
            self.metadata["executable_sha256"] = hashed[0]
            dependency_output = self._exec(probe, ["ldd", "/usr/local/bin/b4run"]).stdout.decode("ascii")
            libraries = []
            for line in dependency_output.splitlines():
                linked = re.fullmatch(r"\s*([a-z0-9_+.-]+) => (/[^ ]+) \(0x[0-9a-f]+\)", line)
                direct = re.fullmatch(r"\s*(/[^ ]+) \(0x[0-9a-f]+\)", line)
                virtual = re.fullmatch(r"\s*(linux-vdso\.so\.1) \(0x[0-9a-f]+\)", line)
                require(linked is not None or direct is not None or virtual is not None, "b4_container_linkage_failed")
                if linked:
                    libraries.append({"name": linked[1], "path": linked[2]})
                elif direct:
                    libraries.append({"name": Path(direct[1]).name, "path": direct[1]})
                else:
                    libraries.append({"name": virtual[1], "path": None})
            require(bool(libraries) and all(_public_library(item) for item in libraries),
                    "b4_container_linkage_failed")
            self.metadata["libraries"] = libraries
            packages = self._exec(probe, ["dpkg-query", "-W", "-f=${binary:Package}\t${Version}\n"]).stdout.decode("ascii")
            inventory = []
            for line in packages.splitlines():
                name, version = line.split("\t")
                require(_public_package({"name": name, "version": version}), "b4_container_package_inventory_failed")
                require(not name.split(":")[0].startswith(("nodejs", "cargo", "rustc", "python3")), "b4_container_runtime_tools_failed")
                inventory.append({"name": name, "version": version})
            require(bool(inventory), "b4_container_package_inventory_failed")
            self.metadata["packages"] = inventory
            verified = self._exec(probe, ["dpkg", "--verify"])
            self.metadata["package_verification"] = classify_package_verification(verified)
        finally:
            self._remove_auxiliary(probe)

    def case_fresh_volume_initialization(self):
        self.volume = self._new_volume("data")
        self._no_active_writers(self.volume)
        before = self._storage_metadata(self.volume)
        require(before["parent"]["kind"] == "directory" and before["parent"]["uid"] == 0
                and before["parent"]["gid"] == 0 and before["data"] is None
                and all(value is None for value in before["files"].values()), "b4_container_fresh_volume_failed")
        initialized, logs = self._command(self.qualification, (), self.volume, entrypoint=INITIALIZER, user="0:0", role="initialize")
        require(logs.stdout == b"b4run_volume_initialized\n" and not logs.stderr, "b4_container_initializer_failed")
        initialized.positive_exit = True
        after = self._storage_metadata(self.volume)
        require_private_parent(after["parent"])
        require(after["data"] is None, "b4_container_initializer_failed")
        self.metadata.update(volume_driver="local", volume_nocopy=True, parent_mode="0700")

    def case_hidden_nonempty_initializer_refusal(self):
        volume = self._new_volume("nonempty")
        write = 'import os; p="/var/lib/b4run/.b4-hidden-sentinel"; f=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); os.write(f,b"private-unit-free-fixture"); os.close(f)'
        fixture, result = self._command(self.qualification, ("-B", "-c", write), volume,
                                        entrypoint="python3", user="0:0", role="fixture")
        require(not result.stdout and not result.stderr, "b4_container_fixture_failed")
        fixture.positive_exit = True
        read = 'import os,stat,hashlib,json; p="/var/lib/b4run"; f=p+"/.b4-hidden-sentinel"; s=os.lstat(p); t=os.lstat(f); print(json.dumps([stat.S_IMODE(s.st_mode),s.st_uid,s.st_gid,stat.S_IMODE(t.st_mode),t.st_uid,t.st_gid,hashlib.sha256(open(f,"rb").read()).hexdigest()]))'
        before = self._observe(["python3", "-B", "-c", read], volume, user="0:0").stdout
        refused, logs = self._command(self.qualification, (), volume, entrypoint=INITIALIZER,
                                      user="0:0", role="initialize", expected=2)
        require(not logs.stdout and logs.stderr == b"b4run_volume_not_empty\n", "b4_container_nonempty_refusal_failed")
        refused.positive_exit = True
        after = self._observe(["python3", "-B", "-c", read], volume, user="0:0").stdout
        require(before == after, "b4_container_nonempty_mutated")

    def case_private_parent_preflight(self):
        require_private_parent(self._storage_metadata(self.volume)["parent"])
        broad = self._new_volume("broad-parent")
        before = self._storage_metadata(broad)
        try:
            require_private_parent(before["parent"])
        except QualificationError:
            pass
        else:
            raise QualificationError("b4_container_parent_refusal_failed")
        require(before == self._storage_metadata(broad), "b4_container_parent_preflight_mutated")

    def case_public_commands_and_readonly_config(self):
        for args, expected in ((("--version",), b"b4run 0.1.0\n"), (("--help",), None)):
            resource, logs = self._command(self.runtime, args)
            require(not logs.stderr and (logs.stdout == expected if expected else logs.stdout.startswith(b"b4run 0.1.0\nUsage: ")),
                    "b4_container_public_command_failed")
            resource.positive_exit = True
        resource, logs = self._command(self.runtime, ("serve", "--demo"), expected=2)
        require(not logs.stdout and logs.stderr == b"b4run_invalid_configuration\n", "b4_container_missing_token_failed")
        resource.positive_exit = True
        before = self._storage_metadata(self.volume)
        resource, logs = self._command(self.runtime, ("config-check", "--demo"), self.volume, token=True)
        require(logs.stdout == b"b4run_config_valid\n" and not logs.stderr, "b4_container_config_check_failed")
        resource.positive_exit = True
        require(before == self._storage_metadata(self.volume), "b4_container_config_check_mutated")

    def case_public_probes_and_docker_health(self):
        self._start_writer()
        require(self._exec(self.owner, ["id", "-u"]).stdout == b"10001\n"
                and self._exec(self.owner, ["readlink", "/proc/1/exe"]).stdout == b"/usr/local/bin/b4run\n",
                "b4_container_pid1_failed")
        for path, expected in (("/healthz", {"status": "ok"}), ("/readyz", {"status": "ready"})):
            code, _, raw = self._request("GET", path, auth=False)
            require(code == 200 and _json_private(raw, "b4_container_probe_failed") == expected, "b4_container_probe_failed")
        deadline = time.monotonic() + 60
        while self._state(self.owner)["health"] != "healthy":
            require(time.monotonic() < deadline, "b4_container_health_timeout")
            time.sleep(0.3)
        result = self._exec(self.owner, ["curl", "--disable", "--fail", "--silent", "--output", "/dev/null",
                                      "--noproxy", "*", "--connect-timeout", "2", "--max-time", "3",
                                      "http://127.0.0.1:3001/b4/capabilities"], allowed=(22,))
        require(result.returncode == 22 and not result.stdout and not result.stderr, "b4_container_probe_http_error_failed")
        self._check_private_storage()

    def case_browser_bearer_capability_guards(self):
        for headers in (("Host: localhost:3001",), ("Origin: null",)):
            require(self._request("GET", "/b4/capabilities", headers=headers)[0] == 403, "b4_container_browser_guard_failed")
        for headers in ((), ("Authorization: Bearer incorrect",), ("Authorization: bearer incorrect",)):
            require(self._request("GET", "/b4/capabilities", auth=False, headers=headers)[0] == 401, "b4_container_authentication_failed")
        code, headers, raw = self._request("GET", "/b4/capabilities")
        value = _json_private(raw, "b4_container_capabilities_failed")
        require(code == 200 and headers.get("cache-control") == "no-store"
                and value.get("descriptor_version") == 2
                and value.get("application") == {"id": "native-demo", "revision": "native-demo-v1"}
                and value.get("store") == {"kind": "sqlite", "durable_runtime": True, "durable_approvals": True}
                and value["features"]["supervised_admission_drain"] is True
                and value["features"]["graceful_shutdown"] is False
                and value["features"]["required_once_deny"] is True, "b4_container_capabilities_failed")
        for path in ("/fixture", "/fixture/control", "/control", "/shutdown", "/signal"):
            code, _, raw = self._request("POST", path, body={})
            require(code == 501 and _json_private(raw, "b4_container_control_route_failed")["error"]["details"]["code"] == "unsupported_operation",
                    "b4_container_control_route_failed")

    def case_native_order_and_required_park(self):
        code, _, raw = self._request("POST", "/threads/native/runs/wait", body={"route": "/native#workflow", "input": None})
        require(code == 200 and _json_private(raw, "b4_container_native_failed") == {"task": "second", "input": None}, "b4_container_native_failed")
        code, _, raw = self._request("POST", "/threads/parked/runs/wait", body={"route": "/approve#workflow", "input": None})
        require(code == 200 and raw == b"null", "b4_container_approval_park_failed")
        code, headers, raw = self._request("GET", "/threads/parked/pending_interrupts")
        pending = _json_private(raw, "b4_container_approval_park_failed")["interrupts"]
        require(code == 200 and headers.get("cache-control") == "no-store" and len(pending) == 1
                and isinstance(pending[0]["grant"], str) and pending[0]["grant"].startswith("b4ag_"), "b4_container_approval_park_failed")
        self.authority = pending[0]
        self.base = self._snapshot()
        native = _private_execution(self.base, "native")
        parked = _private_execution(self.base, "parked")
        require(native["status"] == "completed" and native["values"]["items"] == ["first", "second"], "b4_container_native_order_failed")
        require(parked["status"] == "waiting_for_approval" and parked["tasks"][0]["status"] == "succeeded"
                and parked["tasks"][0]["result"]["writes"] == [{"type": "append", "field": "items", "items": ["prepared"]}]
                and parked["tasks"][1]["status"] == "waiting_for_approval" and parked["tasks"][1]["result"] is None,
                "b4_container_prepared_park_failed")
        counts = public_snapshot_summary(self.base)["table_counts"]
        require(counts["approval_grants"] == 1 and counts["approval_acceptances"] == 0, "b4_container_approval_records_failed")
        self._check_private_storage()

    def case_live_owner_exclusion_and_coherent_rows(self):
        contender, logs = self._command(self.runtime, ("serve", "--demo"), self.volume, token=True, role="contender", expected=1)
        require(not logs.stdout and logs.stderr == b"b4run_storage_owned\n", "b4_container_owner_exclusion_failed")
        contender.positive_exit = True
        require(compare_unchanged(self.base, self._snapshot()) and self._state(self.owner)["running"] is True,
                "b4_container_contender_mutated")
        checked, logs = self._command(self.runtime, ("config-check", "--demo"), self.volume, token=True)
        require(logs.stdout == b"b4run_config_valid\n" and not logs.stderr, "b4_container_live_config_failed")
        checked.positive_exit = True
        invalid, logs = self._command(self.runtime, ("serve", "--demo"), self.volume, token=True,
                                      expected=2, extra_environment=(("B4RUN_PORT", "invalid"),))
        require(not logs.stdout and logs.stderr == b"b4run_invalid_configuration\n", "b4_container_static_configuration_failed")
        invalid.positive_exit = True
        bound = self._exec(self.owner, ["/usr/local/bin/b4run", "serve", "--demo"], allowed=(1,))
        require(not bound.stdout and bound.stderr == b"b4run_listener_failed\n", "b4_container_bind_refusal_failed")
        require(compare_unchanged(self.base, self._snapshot()) and self._request("GET", "/b4/capabilities")[0] == 200,
                "b4_container_refusal_mutated")

    def case_sigterm_positive_retirement(self):
        self._retire_writer("SIGTERM")
        self.stopped_base = self._snapshot("stopped")
        require(compare_unchanged(self.base, self.stopped_base), "b4_container_shutdown_mutated")

    def case_same_volume_epoch_only_restart(self):
        self._start_writer()
        self.reopened = self._snapshot()
        require(compare_single_owner_reopen(self.stopped_base, self.reopened), "b4_container_reopen_mutated")
        self._check_private_storage()

    def case_readonly_prior_owner_parked_projection(self):
        code, _, raw = self._request("GET", "/threads/parked/pending_interrupts")
        require(code == 501 and _json_private(raw, "b4_container_prior_owner_failed")["error"]["details"]["code"] == "unsupported_execution_state",
                "b4_container_prior_owner_failed")
        code, headers, raw = self._request("GET", "/threads/parked/runs/stream")
        events = _frames(raw)
        require(code == 200 and headers.get("cache-control") == "no-store, no-transform" and len(events) == 2
                and events[0][0] == "state" and events[0][1]["status"] == "interrupted"
                and events[0][1]["live"] is False and events[0][1]["interrupts"] == [self.authority]
                and events[1] == ("done", {"output": None}), "b4_container_parked_attachment_failed")
        require(compare_unchanged(self.reopened, self._snapshot()), "b4_container_attachment_adopted")

    def case_sigint_positive_retirement_and_reusable_volume(self):
        self._retire_writer("SIGINT")
        require(compare_unchanged(self.reopened, self._snapshot("stopped")), "b4_container_second_shutdown_mutated")
        self._start_writer()
        third = self._snapshot()
        require(compare_single_owner_reopen(self.reopened, third), "b4_container_third_reuse_failed")
        self._check_private_storage()
        self._retire_writer("SIGTERM")
        require(compare_unchanged(third, self._snapshot("stopped")), "b4_container_third_shutdown_mutated")

    def _save_receipt(self, failure=None):
        receipt = public_receipt(self.cases, self.metadata)
        if failure is not None:
            require(type(failure) is str and re.fullmatch(r"b4_container_[a-z_]+", failure) is not None,
                    "b4_container_receipt_refused")
            receipt["passed"] = False
            receipt["failure"] = failure
        raw = (json.dumps(receipt, indent=2, sort_keys=True) + "\n").encode("ascii")
        os.lseek(self.receipt_fd, 0, os.SEEK_SET)
        os.ftruncate(self.receipt_fd, 0)
        offset = 0
        while offset < len(raw):
            offset += os.write(self.receipt_fd, raw[offset:])
        os.fsync(self.receipt_fd)

    def cleanup(self):
        # A separate finite cleanup allowance cannot convert an expired run
        # into success. Caller records failure before any forced termination.
        self.docker.deadline = time.monotonic() + 90
        failed = False
        try:
            self._recover_container_intents()
        except Exception:
            failed = True
        for resource in reversed(self.containers):
            if resource.removed:
                continue
            try:
                state = self._state(resource)
                if state["running"]:
                    self.docker.run(["kill", "--signal", "SIGKILL", resource.identity])
                    self._wait_exited(resource, timeout=12)
                self.docker.run(["rm", resource.identity])
                resource.removed = True
            except Exception:
                failed = True
        for name in reversed(self.volumes):
            try:
                selected = self.docker.run(["volume", "inspect", "--format", INSPECT_FORMATS["volume"], name], allowed=(0, 1))
                if selected.returncode == 1:
                    continue
                volume = _json_private(selected.stdout, "b4_container_cleanup_failed")
                validate_volume(volume, name, self.task)
                self.docker.run(["volume", "rm", name])
            except Exception:
                failed = True
        require(not failed, "b4_container_cleanup_failed")
        self.metadata["cleanup_completed"] = True

    def _recover_container_intents(self):
        known_names = {resource.name for resource in self.containers}
        for intent, image, user in self.container_intents:
            if intent.name in known_names:
                continue
            result = self.docker.run(["inspect", "--format", INSPECT_FORMATS["container"], intent.name], allowed=(0, 1))
            if result.returncode == 1:
                continue
            state = _json_private(result.stdout, "b4_container_cleanup_failed")
            require(type(state) is dict and set(state) == INSPECT_KEYS["container"]
                    and type(state["id"]) is str and re.fullmatch(r"[0-9a-f]{64}", state["id"]) is not None
                    and state["name"] == "/" + intent.name and state["label"] == self.task
                    and state["image"] == image and state["user"] == user and state["working_dir"] == "/",
                    "b4_container_identity_refused")
            intent.identity = state["id"]
            self.containers.append(intent)
            known_names.add(intent.name)

    def _assert_positive_resources(self):
        require(self.observers == 0, "b4_container_observer_still_active")
        for resource in self.containers:
            if resource.removed:
                continue
            state = self._state(resource)
            require(resource.positive_exit and state["status"] == "exited"
                    and state["running"] is False and state["oom_killed"] is False,
                    "b4_container_resource_not_retired")

    def run(self):
        failure = None
        try:
            for name in CASE_NAMES:
                getattr(self, name)()
                self.cases[name] = True
            self._assert_positive_resources()
        except QualificationError as error:
            failure = str(error)
        except Exception:
            failure = "b4_container_qualification_failed"
        if failure is not None:
            # This receipt precedes SIGKILL cleanup, preserving the failed
            # qualification rather than presenting cleanup as a shutdown proof.
            self._save_receipt(failure)
        try:
            self.cleanup()
        except Exception:
            failure = failure or "b4_container_cleanup_failed"
            self.metadata["cleanup_completed"] = False
        self._save_receipt(failure)
        os.close(self.receipt_fd)
        self.private.cleanup()
        if failure is not None:
            raise QualificationError(failure)
        return public_receipt(self.cases, self.metadata)


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        raise QualificationError("b4_container_invalid_arguments")


def main(argv=None):
    parser = _Parser(add_help=False)
    parser.add_argument("--runtime-image", default="b4run-linux-runtime:local")
    parser.add_argument("--qualification-image", default="b4run-linux-qualification:local")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    Qualification(args.runtime_image, args.qualification_image, args.receipt).run()
    print("b4_container_qualification_passed")


if __name__ == "__main__":
    try:
        main()
    except QualificationError as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("b4_container_qualification_failed", file=sys.stderr)
        sys.exit(1)
