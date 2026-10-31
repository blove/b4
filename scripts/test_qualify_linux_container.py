"""Private driver boundaries plus actual coherent SQLite qualification."""

import copy
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

import container_snapshot as snapshot
import qualify_linux_container as driver

ROOT = Path(__file__).resolve().parents[1]
SECRET = "b4ag_PRIVATE_CONTAINER_SENTINEL"


class Database:
    def __enter__(self):
        self.directory = tempfile.TemporaryDirectory(prefix="b4-container-snapshot-")
        self.path = Path(self.directory.name)
        self.connection = sqlite3.connect(self.path / "runtime.sqlite3", isolation_level=None)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript((ROOT / "crates/b4-sqlite/src/schema-v3.sql").read_text())
        self.connection.execute("INSERT INTO metadata VALUES(1,3,'checksum',?)", (bytes.fromhex("0000000000000001"),))
        self.connection.execute("INSERT INTO threads(id, metadata) VALUES('thread', ?)", (json.dumps({"private": SECRET}),))
        return self

    def close_clean(self):
        # macOS system SQLite may persist zero-length sidecars. This creates a
        # clean *unit fixture* only; production inspection never checkpoints or
        # removes source sidecars. The actual Linux run must satisfy absence.
        cursor = self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        result = cursor.fetchone()
        cursor.close()
        if result != (0, 0, 0):
            raise AssertionError("unit fixture checkpoint must include all commits")
        self.connection.close()
        wal = self.path / "runtime.sqlite3-wal"
        if wal.exists() and wal.stat().st_size != 0:
            raise AssertionError("unit fixture WAL must be empty before fixture cleanup")
        for suffix in ("-wal", "-shm"):
            (self.path / ("runtime.sqlite3" + suffix)).unlink(missing_ok=True)

    def __exit__(self, *_):
        self.connection.close()
        self.directory.cleanup()


def volume():
    return {"name": "b4-test-volume", "driver": "local", "options": None,
            "label": "task", "source": "/var/lib/docker/volumes/b4-test-volume/_data"}


def container():
    return {"mounts": [{"type": "volume", "name": "b4-test-volume",
                        "source": volume()["source"], "destination": driver.DESTINATION, "rw": True}],
            "configured_mounts": [{"type": "volume", "source": "b4-test-volume",
                                   "target": driver.DESTINATION, "read_only": False, "nocopy": True}],
            "network": "none", "privileged": False, "readonly_root": True,
            "cap_add": None, "cap_drop": ["ALL"]}


class SnapshotQualification(unittest.TestCase):
    def test_live_wal_snapshot_sees_uncheckpointed_commits(self):
        with Database() as database:
            self.assertTrue((database.path / "runtime.sqlite3-wal").is_file())
            captured = snapshot.capture_private_snapshot(database.path)
            self.assertEqual(snapshot.public_snapshot_summary(captured)["table_counts"]["threads"], 1)

    def test_writer_commit_between_table_reads_cannot_split_snapshot(self):
        with Database() as database:
            def commit_after_metadata(table):
                if table == "metadata":
                    database.connection.execute("BEGIN IMMEDIATE")
                    database.connection.execute("UPDATE metadata SET owner_epoch=?", (bytes.fromhex("0000000000000002"),))
                    database.connection.execute("INSERT INTO threads(id) VALUES('second')")
                    database.connection.execute("COMMIT")
            before = snapshot.capture_private_snapshot(database.path)
            interleaved = snapshot.capture_private_snapshot(database.path, after_table=commit_after_metadata)
            self.assertTrue(snapshot.compare_unchanged(before, interleaved), "all six tables must share the transaction's first-read WAL snapshot")
            after = snapshot.capture_private_snapshot(database.path)
            self.assertFalse(snapshot.compare_unchanged(before, after))

    def test_readonly_observer_preserves_all_rows_and_epoch(self):
        with Database() as database:
            before = list(database.connection.iterdump())
            snapshot.capture_private_snapshot(database.path)
            self.assertTrue(before == list(database.connection.iterdump()), "readonly observation must not mutate any logical record")

    def test_missing_database_is_not_created(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(snapshot.SnapshotError):
                snapshot.capture_private_snapshot(Path(directory))
            self.assertFalse((Path(directory) / "runtime.sqlite3").exists())

    def test_stopped_mode_refuses_every_existing_sidecar(self):
        for suffix in ("-wal", "-shm", "-journal"):
            with Database() as database:
                database.close_clean()
                sidecar = database.path / ("runtime.sqlite3" + suffix)
                sidecar.write_bytes(b"private-sidecar")
                with self.assertRaises(snapshot.SnapshotError):
                    snapshot.capture_private_snapshot(database.path, mode="stopped")

    def test_stopped_mode_reads_clean_wal_header_without_creating_sidecars(self):
        with Database() as database:
            before = snapshot.capture_private_snapshot(database.path)
            database.close_clean()
            after = snapshot.capture_private_snapshot(database.path, mode="stopped")
            self.assertTrue(snapshot.compare_unchanged(before, after))
            self.assertFalse(any((database.path / ("runtime.sqlite3" + suffix)).exists() for suffix in ("-wal", "-shm", "-journal")))

    def test_canonical_cells_preserve_types_boundaries_and_raw_json(self):
        encoded = [snapshot.encode_canonical_cell(value) for value in (None, 1, "1", b"1", "{\"x\":1}", "{ \"x\" : 1 }")]
        self.assertEqual(len({json.dumps(value) for value in encoded}), len(encoded))

    def test_readonly_and_query_only_reject_actual_writes(self):
        with Database() as database:
            before = snapshot.capture_private_snapshot(database.path)
            connection = snapshot.open_fixed_readonly_database(database.path)
            try:
                self.assertEqual(connection.execute("PRAGMA query_only").fetchone(), (1,))
                with self.assertRaises(sqlite3.OperationalError):
                    connection.execute("UPDATE metadata SET schema_version=2")
            finally:
                connection.close()
            self.assertTrue(snapshot.compare_unchanged(before, snapshot.capture_private_snapshot(database.path)))

    def test_reopen_preserves_original_execution_owner_and_authority(self):
        with Database() as database:
            epoch = bytes.fromhex("0000000000000001")
            database.connection.execute("INSERT INTO executions VALUES('execution','thread',?,?,?)", (epoch, epoch, '{"owner_generation":1,"private":"' + SECRET + '"}'))
            database.connection.execute("INSERT INTO approval_grants VALUES('execution','interrupt','release','thread',?,?,?,?)", ("0" * 64, epoch, 0, '{"token":"' + SECRET + '","expires_at_ms":300000}'))
            before = snapshot.capture_private_snapshot(database.path)
            database.connection.execute("UPDATE metadata SET owner_epoch=?", ((2).to_bytes(8, "big"),))
            self.assertTrue(snapshot.compare_single_owner_reopen(before, snapshot.capture_private_snapshot(database.path)))
            database.connection.execute("UPDATE executions SET owner_epoch=?", ((2).to_bytes(8, "big"),))
            self.assertFalse(snapshot.compare_single_owner_reopen(before, snapshot.capture_private_snapshot(database.path)))

    def test_reopen_changes_only_metadata_epoch_by_exactly_one(self):
        with Database() as database:
            before = snapshot.capture_private_snapshot(database.path)
            database.connection.execute("UPDATE metadata SET owner_epoch=?", (bytes.fromhex("0000000000000002"),))
            after = snapshot.capture_private_snapshot(database.path)
            self.assertTrue(snapshot.compare_single_owner_reopen(before, after))
            self.assertFalse(snapshot.compare_unchanged(before, after))
            database.connection.execute("UPDATE metadata SET owner_epoch=?", (bytes.fromhex("0000000000000003"),))
            self.assertFalse(snapshot.compare_single_owner_reopen(before, snapshot.capture_private_snapshot(database.path)))

    def test_reopen_refuses_any_additional_row_or_raw_json_change(self):
        with Database() as database:
            before = snapshot.capture_private_snapshot(database.path)
            database.connection.execute("UPDATE metadata SET owner_epoch=?", (bytes.fromhex("0000000000000002"),))
            database.connection.execute("UPDATE threads SET metadata=?", (json.dumps({"private": SECRET}, separators=(",", ":")),))
            self.assertFalse(snapshot.compare_single_owner_reopen(before, snapshot.capture_private_snapshot(database.path)))

    def test_public_snapshot_summary_contains_counts_without_authority_or_digest(self):
        with Database() as database:
            value = snapshot.public_snapshot_summary(snapshot.capture_private_snapshot(database.path))
            text = json.dumps(value)
            self.assertNotIn(SECRET, text)
            self.assertEqual(set(value), {"schema_version", "table_counts"})

    def test_snapshot_process_failures_suppress_source_path_and_private_payload(self):
        result = subprocess.run([sys.executable, "-B", str(ROOT / "scripts/container_snapshot.py"), "--private=" + SECRET], capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(SECRET, result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stderr)


class DriverBoundaries(unittest.TestCase):
    def constructor_probe(self, working_dir):
        commands = []
        selected = driver.Qualification.__new__(driver.Qualification)
        def runner(args, **kwargs):
            commands.append(args)
            return driver.Result(0, ("b" * 64 + "\n").encode())
        selected.docker = driver.Docker(runner)
        selected.task, selected.prefix, selected.sequence = "task", "b4-constructor-test", 0
        selected.containers, selected.container_intents, selected.volumes = [], [], []
        state = {"image": "sha256:" + "a" * 64, "user": "10001:10001", "network": "none",
                 "privileged": False, "readonly_root": True, "cap_drop": ["ALL"], "cap_add": None,
                 "mounts": [], "configured_mounts": [], "working_dir": working_dir}
        selected._state = lambda _resource: state
        return selected, commands

    def test_created_containers_explicitly_use_fixed_root_working_directory(self):
        selected, commands = self.constructor_probe("/")
        selected._new_container({"id": "sha256:" + "a" * 64}, "image-probe")
        self.assertEqual(commands[0][1], "create")
        self.assertIn("--workdir", commands[0], "container cwd must not inherit the private builder directory")
        self.assertEqual(commands[0][commands[0].index("--workdir") + 1], "/")

    def test_created_container_refuses_projected_nonroot_private_working_directory(self):
        selected, _commands = self.constructor_probe("/workspace")
        with self.assertRaises(driver.QualificationError):
            selected._new_container({"id": "sha256:" + "a" * 64}, "image-probe")

    def test_valid_owned_local_volume_and_nocopy_mount_are_accepted(self):
        driver.validate_volume(volume(), "b4-test-volume", "task")
        driver.validate_mount(container(), volume())

    def test_foreign_labels_nonlocal_options_or_names_are_refused(self):
        for key, value in (("label", "foreign"), ("driver", "nfs"), ("options", {"device": SECRET}), ("name", "foreign")):
            selected = volume()
            selected[key] = value
            with self.assertRaises(driver.QualificationError) as error:
                driver.validate_volume(selected, "b4-test-volume", "task")
            self.assertNotIn(SECRET, str(error.exception))

    def test_wrong_mount_nocopy_readonly_or_host_access_is_refused(self):
        variants = []
        for key, value in (("source", "/private/foreign"), ("name", "foreign"), ("type", "bind"), ("destination", "/foreign"), ("rw", False)):
            selected = container()
            selected["mounts"][0][key] = value
            variants.append(selected)
        selected = container()
        selected["configured_mounts"][0]["nocopy"] = False
        variants.append(selected)
        for key, value in (("network", "host"), ("privileged", True), ("readonly_root", False)):
            selected = container()
            selected[key] = value
            variants.append(selected)
        for selected in variants:
            with self.assertRaises(driver.QualificationError):
                driver.validate_mount(selected, volume())

    def test_parent_preflight_accepts_only_exact_private_owned_directory(self):
        driver.require_private_parent({"kind": "directory", "mode": 0o700, "uid": 10001, "gid": 10001})
        for key, value in (("mode", 0o755), ("uid", 0), ("gid", 0), ("kind", "symlink")):
            metadata = {"kind": "directory", "mode": 0o700, "uid": 10001, "gid": 10001}
            metadata[key] = value
            with self.assertRaises(driver.QualificationError):
                driver.require_private_parent(metadata)

    def test_positive_retirement_requires_actual_clean_exit_stopped_and_not_oom(self):
        state = {"status": "exited", "running": False, "exit_code": 0, "oom_killed": False}
        self.assertTrue(driver.positive_retirement(state, b"b4run_listening http://127.0.0.1:3001\nb4run_draining\nb4run_stopped\n", b""))
        for key, value in (("status", "running"), ("running", True), ("exit_code", 137), ("oom_killed", True)):
            selected = copy.deepcopy(state)
            selected[key] = value
            self.assertFalse(driver.positive_retirement(selected, b"b4run_stopped\n", b""))
        self.assertFalse(driver.positive_retirement(state, b"b4run_draining\n", b""))
        self.assertFalse(driver.positive_retirement(state, b"b4run_stopped\n" + SECRET.encode(), b""))

    def test_stopped_source_requires_all_exited_writers_and_no_open_observers(self):
        exited = {"status": "exited", "running": False, "oom_killed": False, "positive_exit": True}
        self.assertTrue(driver.stopped_source_allowed([exited], 0))
        self.assertFalse(driver.stopped_source_allowed([exited], 1))
        self.assertFalse(driver.stopped_source_allowed([{**exited, "positive_exit": False}], 0))
        self.assertFalse(driver.stopped_source_allowed([{**exited, "running": True}], 0))

    def test_curl_authority_is_only_in_private_stdin_and_quoted_losslessly(self):
        raw = driver.curl_config("POST", "/threads/private/runs/wait", SECRET, body={"private": SECRET})
        self.assertIsInstance(raw, bytes)
        self.assertIn(("Authorization: Bearer " + SECRET).encode(), raw)
        self.assertTrue(raw.startswith(b"disable\n"))
        self.assertIn(b"noproxy = \"*\"", raw)

    def test_inspect_is_explicit_allowlist_and_never_collects_environment(self):
        commands = []
        def runner(args, **kwargs):
            commands.append(args)
            return driver.Result(0, json.dumps(volume()).encode())
        selected = driver.Docker(runner).inspect("volume", "b4-test-volume")
        self.assertEqual(selected, volume())
        self.assertEqual(len(commands), 1)
        self.assertIn("--format", commands[0])
        self.assertNotIn(".Env", " ".join(commands[0]))
        self.assertNotIn("{{json .}}", " ".join(commands[0]))

    def test_inspection_and_runner_failures_never_echo_raw_private_data(self):
        for result in (driver.Result(1, SECRET.encode(), SECRET.encode()), driver.Result(0, json.dumps({"env": SECRET}).encode())):
            with self.assertRaises(driver.QualificationError) as error:
                driver.Docker(lambda *_args, **_kwargs: result).inspect("volume", "b4-test-volume")
            self.assertNotIn(SECRET, str(error.exception))

    def test_public_receipt_refuses_private_or_unknown_metadata(self):
        with self.assertRaises(driver.QualificationError) as error:
            driver.public_receipt({"case_private": True}, {"env": SECRET})
        self.assertNotIn(SECRET, str(error.exception))

    def test_public_receipt_refuses_private_values_inside_allowed_fields(self):
        variants = ({"os": SECRET}, {"architecture": SECRET},
                    {"uid": SECRET}, {"parent_mode": SECRET},
                    {"libraries": [{"name": "libc.so.6", "path": "/private/" + SECRET}]},
                    {"packages": [{"name": "curl", "version": SECRET}]},
                    {"package_verification": {"status": SECRET}},
                    {"cleanup_completed": SECRET})
        for metadata in variants:
            with self.assertRaises(driver.QualificationError) as error:
                driver.public_receipt({}, metadata)
            self.assertNotIn(SECRET, str(error.exception))

    def test_package_verification_accepts_only_known_slim_missing_categories(self):
        raw = ("missing     /usr/share/doc/bash/README.gz\n"
               "missing     /usr/share/locale/af/LC_MESSAGES/bash.mo\n"
               "missing     /usr/share/man/man1/[.1.gz\n"
               "missing     /usr/share/info/dir\n"
               "missing     /usr/share/lintian/overrides/bash\n"
               "missing     /var/lib/apt/lists/partial\n").encode()
        try:
            value = driver.classify_package_verification(driver.Result(0, raw))
        except driver.QualificationError:
            value = None
        self.assertIsInstance(value, dict, "known slim omissions need a precise public classification")
        self.assertEqual(value["status"], "slim_expected_omissions_only")
        self.assertEqual(value["expected_missing_count"], 6)
        self.assertEqual(value["unexpected_missing_count"], 0)
        self.assertEqual(value["nonmissing_discrepancy_count"], 0)
        self.assertFalse(value["pristine_packages_claimed"])
        public = driver.public_receipt({}, {"package_verification": value})
        self.assertNotIn("/usr/share", json.dumps(public))

    def test_package_verification_empty_reports_no_discrepancies_without_pristine_claim(self):
        value = driver.classify_package_verification(driver.Result(0))
        self.assertIsInstance(value, dict, "empty verification output needs a non-pristine classification")
        self.assertEqual(value["status"], "no_reported_discrepancies")
        self.assertEqual(value["expected_missing_count"], 0)
        self.assertFalse(value["pristine_packages_claimed"])

    def test_package_verification_rejects_unexpected_missing_or_nonmissing_reports(self):
        reports = ("missing     /usr/bin/curl", "missing     /usr/share/document/" + SECRET,
                   "missing     /usr/share/doc/../bin/" + SECRET,
                   "missing     /usr/share/doc//" + SECRET,
                   "missing     /var/lib/apt/lists/" + SECRET,
                   "??5??????   /usr/bin/" + SECRET)
        for report in reports:
            with self.assertRaises(driver.QualificationError) as error:
                driver.classify_package_verification(driver.Result(0, (report + "\n").encode()))
            self.assertNotIn(SECRET, str(error.exception))
        for value in (driver.Result(1), driver.Result(0, b"", SECRET.encode())):
            with self.assertRaises(driver.QualificationError):
                driver.classify_package_verification(value)

    def test_readonly_parked_attachment_accepts_exact_sse_header_and_preserves_all_guards(self):
        def fixture(live=False, authority_equal=True, done=None, rows_equal=True):
            selected = driver.Qualification.__new__(driver.Qualification)
            selected.authority = {"grant": SECRET}
            selected.reopened = {"private": SECRET}
            state = {"status": "interrupted", "live": live,
                     "interrupts": [selected.authority] if authority_equal else []}
            raw = ("event: state\ndata: " + json.dumps(state) + "\n\n"
                   "event: done\ndata: " + json.dumps({"output": done}) + "\n\n").encode()
            pending = json.dumps({"error": {"details": {"code": "unsupported_execution_state"}}}).encode()
            selected._request = lambda _method, path: (
                (501, {}, pending) if path.endswith("pending_interrupts")
                else (200, {"cache-control": "no-store, no-transform"}, raw))
            selected._snapshot = lambda: selected.reopened if rows_equal else {"changed": True}
            return selected
        try:
            fixture().case_readonly_prior_owner_parked_projection()
            passed = True
        except driver.QualificationError:
            passed = False
        self.assertTrue(passed, "existing SSE cache policy must allow an exact readonly parked attachment")
        for variant in ({"live": True}, {"authority_equal": False}, {"done": "unexpected"}, {"rows_equal": False}):
            with self.assertRaises(driver.QualificationError):
                fixture(**variant).case_readonly_prior_owner_parked_projection()

    def test_foreign_volume_is_refused_before_container_creation(self):
        commands = []
        def runner(args, **kwargs):
            commands.append(args)
            return driver.Result(0, json.dumps({**volume(), "label": "foreign"}).encode())
        selected = driver.Qualification.__new__(driver.Qualification)
        selected.docker = driver.Docker(runner)
        selected.task = "task"
        selected.volumes = ["b4-test-volume"]
        with self.assertRaises(driver.QualificationError):
            selected._new_container({"id": "sha256:" + "a" * 64}, "writer", volume())
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][1:3], ["volume", "inspect"])

    def test_unknown_create_outcome_recovers_only_exact_owned_identity(self):
        selected = driver.Qualification.__new__(driver.Qualification)
        selected.task = "task"
        selected.containers = []
        selected.container_intents = [
            (driver.Resource("", "created-name", "command", None, False),
             "sha256:" + "a" * 64, "10001:10001")]
        state = {key: None for key in driver.INSPECT_KEYS["container"]}
        state.update(id="b" * 64, name="/created-name", label="task",
                     image="sha256:" + "a" * 64, user="10001:10001", working_dir="/")
        selected.docker = driver.Docker(lambda *_args, **_kwargs: driver.Result(0, json.dumps(state).encode()))
        selected._recover_container_intents()
        self.assertEqual(len(selected.containers), 1)
        self.assertEqual(selected.containers[0].identity, "b" * 64)
        selected.containers = []
        state["label"] = "foreign"
        with self.assertRaises(driver.QualificationError):
            selected._recover_container_intents()
        self.assertEqual(selected.containers, [])

    def test_live_resource_cannot_count_as_complete_before_cleanup(self):
        selected = driver.Qualification.__new__(driver.Qualification)
        selected.observers = 0
        selected.containers = [driver.Resource("b" * 64, "name", "writer", None, False, True)]
        selected._state = lambda _resource: {"status": "running", "running": True, "oom_killed": False}
        with self.assertRaises(driver.QualificationError):
            selected._assert_positive_resources()

    def test_run_records_live_resource_failure_before_forced_cleanup(self):
        selected = driver.Qualification.__new__(driver.Qualification)
        selected.observers = 0
        selected.containers = [driver.Resource("b" * 64, "name", "writer", None, False, True)]
        selected._state = lambda _resource: {"status": "running", "running": True, "oom_killed": False}
        selected.cases, selected.metadata = {}, {}
        events = []
        for name in driver.CASE_NAMES:
            setattr(selected, name, lambda: None)
        selected._save_receipt = lambda failure=None: events.append(("receipt", failure))
        selected.cleanup = lambda: events.append(("forced_cleanup", None))
        selected.private = SimpleNamespace(cleanup=lambda: None)
        with tempfile.TemporaryFile() as output:
            selected.receipt_fd = os.dup(output.fileno())
            with self.assertRaises(driver.QualificationError):
                selected.run()
        self.assertEqual(events[0], ("receipt", "b4_container_resource_not_retired"))
        self.assertEqual(events[1], ("forced_cleanup", None))


if __name__ == "__main__":
    unittest.main()
