#!/usr/bin/env python3
"""Validate scoped contract/evidence artifacts, without asserting release conformance."""

import base64
import copy
import hashlib
import json
import re
import subprocess
from pathlib import Path

import yaml
import check_inspection_reports
import check_diagnostic_batches
from jsonschema import Draft202012Validator
from referencing import Registry, Resource


REPO = Path(__file__).resolve().parents[1]
CONTRACTS = REPO / "contracts" / "agent-protocol"


def require(condition, message):
    # Keep checks enabled when Python is run with -O.
    if not condition:
        raise ValueError(message)


def check_core_release_traceability(requirements):
    """Check the eighteen current release qualifications, not runtime behavior itself.

    The native qualifier delivers its receipt after the frozen native checks.
    Whole public metadata validation runs only after that delivery; no current
    checkout digest is computed here (which would create a receipt hash cycle).
    """
    inventory = requirements["release_acceptance_inventory"]
    require([scenario.get("id") for scenario in inventory] == [f"T-{n:03d}" for n in range(1, 57)],
            "Release scenario inventory is incomplete or out of order")
    # Each proof binds its own historical source map; descriptors may differ.
    qualifications = (
        ("T-001", "crates/b4-http/tests/native_tcp.rs",
         "release_t001_create_read_thread_round_trips_metadata_and_timestamps_after_reopen",
         "native_create_read_thread_tcp_sqlite", "conformance/release/t001-thread-roundtrip/qualification.json"),
        ("T-002", "crates/b4-http/tests/thread_state_tcp.rs",
         "release_t002_empty_thread_state_stays_unavailable_after_reopen",
         "native_empty_thread_state_tcp_sqlite", "conformance/release/t002-empty-thread-state/qualification.json"),
        ("T-003", "crates/b4-http/tests/thread_admission_tcp.rs",
         "release_t003_implicit_thread_creation_requires_authorization",
         "native_authorized_implicit_thread_tcp_sqlite", "conformance/release/t003-authorized-implicit-thread/qualification.json"),
        ("T-004", "crates/b4-http/tests/thread_route_tcp.rs",
         "release_t004_unknown_and_missing_kind_routes_do_not_execute",
         "native_invalid_route_rejection_tcp_sqlite", "conformance/release/t004-invalid-routes/qualification.json"),
        ("T-005", "crates/b4-http/tests/thread_json_tcp.rs",
         "release_t005_malformed_json_uses_ap_error_without_execution",
         "native_malformed_json_error_tcp_sqlite", "conformance/release/t005-malformed-json/qualification.json"),
        ("T-006", "crates/b4-http/tests/input_shape_tcp.rs",
         "release_t006_omitted_null_and_false_inputs_round_trip",
         "native_input_shape_preservation_tcp_sqlite", "conformance/release/t006-input-shapes/qualification.json"),
        ("T-007", "crates/b4-http/tests/thread_concurrency_tcp.rs",
         "release_t007_same_thread_overlap_has_one_winner",
         "native_same_thread_admission_tcp_sqlite", "conformance/release/t007-same-thread-admission/qualification.json"),
        ("T-008", "crates/b4-http/tests/different_thread_tcp.rs",
         "release_t008_different_threads_execute_concurrently",
         "native_different_thread_concurrency_tcp_sqlite", "conformance/release/t008-different-thread-concurrency/qualification.json"),
        ("T-009", "crates/b4-http/tests/wait_output_tcp.rs",
         "release_t009_wait_returns_direct_value_and_void_null",
         "native_wait_output_tcp_sqlite", "conformance/release/t009-wait-output/qualification.json"),
        ("T-010", "crates/b4-http/tests/stream_payload_tcp.rs",
         "release_t010_native_stream_payloads_are_direct_after_commit",
         "native_stream_event_payloads_tcp_sqlite", "conformance/release/t010-native-stream-payloads/qualification.json"),
        ("T-011", "crates/b4-http/tests/terminal_frame_tcp.rs",
         "release_t011_normal_native_streams_end_with_one_done",
         "native_normal_stream_terminal_tcp_sqlite", "conformance/release/t011-normal-stream-terminal/qualification.json"),
        ("T-012", "crates/b4-http/tests/disconnect_completion_tcp.rs",
         "release_t012_disconnected_stream_and_wait_preserve_admitted_work",
         "native_disconnected_run_completion_tcp_sqlite", "conformance/release/t012-disconnect-completion/qualification.json"),
        ("T-013", "crates/b4-http/tests/attachment_state_tcp.rs",
         "release_t013_live_and_settled_attach_start_with_state_without_execution",
         "native_read_only_attachment_state_tcp_sqlite", "conformance/release/t013-attachment-state/qualification.json"),
        ("T-014", "crates/b4-http/tests/attachment_cut_tcp.rs",
         "release_t014_attachment_cut_and_live_tail_preserve_each_event_once",
         "native_attachment_cut_live_tail_tcp_sqlite", "conformance/release/t014-attachment-cut/qualification.json"),
        ("T-015", "crates/b4-http/tests/attachment_truncation_tcp.rs",
         "release_t015_native_digest_overflow_attaches_with_explicit_truncation",
         "native_truncated_attachment_after_projection_limit_tcp_sqlite",
         "conformance/release/t015-attachment-truncation/qualification.json"),
        ("T-016", "crates/b4-http/tests/viewer_overflow_tcp.rs",
         "release_t016_viewer_capacity_and_queue_overflow_do_not_stop_run",
         "native_attached_viewer_capacity_and_byte_overflow_tcp_sqlite",
         "conformance/release/t016-viewer-overflow/qualification.json"),
        ("T-017", "crates/b4-http/tests/heartbeat_splits_tcp.rs",
         "release_t017_heartbeat_and_split_utf8_tcp_preserve_one_terminal",
         "native_heartbeat_and_split_utf8_tcp_sqlite",
         "conformance/release/t017-heartbeat-splits/qualification.json"),
        ("T-018", "crates/b4-http/tests/approval_durable_disclosure_tcp.rs",
         "release_t018_approval_park_is_durable_before_tcp_disclosure",
         "native_approval_park_durable_before_disclosure_tcp_sqlite",
         "conformance/release/t018-durable-disclosure/qualification.json"),
    )
    require(all(scenario.get("implementation_status") == "pending"
                and scenario.get("verification_status") == "not_run"
                and scenario.get("acceptance_test_status") == "future_release_scenario"
                and not any(key in scenario for key in
                            ("classification", "release_scope", "verification_receipt", "acceptance_tests"))
                for scenario in inventory[18:]),
            "T-019 through T-056 must remain pending, not run, and unqualified")
    for index, (scenario_id, test_file, test_name, scope, receipt_path) in enumerate(qualifications):
        expected = {
            "classification": "NATIVE", "implementation_status": "implemented",
            "verification_status": "passed_native_scope",
            "acceptance_test_status": "implemented_native_scope", "release_scope": scope,
            "acceptance_tests": [{"file": test_file, "name": test_name}],
            "verification_receipt": receipt_path,
        }
        require(all(inventory[index].get(key) == value for key, value in expected.items()),
                "Release scenario must retain its exact native TCP/SQLite qualification")
        source = repo_file(test_file)
        require(re.search(rf"(?m)^\s*(?:async\s+)?fn\s+{re.escape(test_name)}\s*\(", source.read_text()),
                f"Missing named {scenario_id} native acceptance test declaration")
        receipt = read_json(repo_file(receipt_path))
        require(isinstance(receipt, dict), f"{scenario_id} qualification must be an object")
        for key, value in {
            "record_version": 1, "status": "qualified", "scenario_id": scenario_id,
            "evidence_class": "NATIVE", "release_scope": scope,
            "acceptance_test": {"file": test_file, "name": test_name,
                                "sha256": sha256(source.read_bytes())},
            "core_profile_qualified": False, "compatibility_qualified": False,
            "other_release_scenarios_qualified": [], "public_metadata_required_after_delivery": True,
        }.items():
            require(key in receipt and receipt[key] == value and type(receipt[key]) is type(value),
                    f"{scenario_id} qualification field differs: {key}")
        descriptor = receipt.get("native_source_descriptor_sha256")
        require(isinstance(descriptor, str) and re.fullmatch(r"[0-9a-f]{64}", descriptor),
                f"Missing {scenario_id} native source descriptor digest")
        count = receipt.get("native_source_file_count")
        require(type(count) is int and count > 0, f"Missing {scenario_id} native source file count")
        initial = receipt.get("initial_proof")
        require(isinstance(initial, dict) and initial.get("additional_runtime_credit") is False,
                f"Initial {scenario_id} proof must not add runtime credit")
        commands = {
            "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "native_tcp", test_name,
                              "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
            "native-tcp-tests": ["cargo", "test", "-p", "b4-http", "--test", "native_tcp", "--locked",
                                 "--offline", "--", "--test-threads=1"],
            "format-check": ["cargo", "fmt", "--all", "--", "--check"],
            "scoped-lint": ["cargo", "clippy", "-p", "b4-http", "--all-targets", "--locked", "--offline",
                            "--", "-D", "warnings"],
            "guard-tests": ["python3", "-B", "scripts/test_core_release_traceability.py"],
        }
        runtime_steps = ("scenario-test", "native-tcp-tests")
        if scenario_id == "T-002":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "thread-state-tests": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                       "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "thread-state-tests", "t001-regression")
        if scenario_id == "T-003":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "thread-admission-tests": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp",
                                           "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "t002-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "thread-admission-tests", "t001-regression", "t002-regression")
        if scenario_id == "T-004":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "thread_route_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "thread-route-tests": ["cargo", "test", "-p", "b4-http", "--test", "thread_route_tcp",
                                       "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "t002-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t003-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "thread-route-tests", "t001-regression", "t002-regression", "t003-regression")
        if scenario_id == "T-005":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "thread_json_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "thread-json-tests": ["cargo", "test", "-p", "b4-http", "--test", "thread_json_tcp",
                                      "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "t002-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t003-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t004-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_route_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "thread-json-tests", "t001-regression", "t002-regression",
                             "t003-regression", "t004-regression")
        if scenario_id == "T-006":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "input_shape_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "input-shape-tests": ["cargo", "test", "-p", "b4-http", "--test", "input_shape_tcp",
                                      "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "t002-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t003-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t004-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_route_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t005-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_json_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "input-shape-tests", "t001-regression", "t002-regression",
                             "t003-regression", "t004-regression", "t005-regression")
        if scenario_id == "T-007":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "thread_concurrency_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "thread-concurrency-tests": ["cargo", "test", "-p", "b4-http", "--test", "thread_concurrency_tcp",
                                      "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "t002-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t003-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t004-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_route_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t005-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_json_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t006-regression": ["cargo", "test", "-p", "b4-http", "--test", "input_shape_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "thread-concurrency-tests", "t001-regression", "t002-regression",
                             "t003-regression", "t004-regression", "t005-regression", "t006-regression")
        if scenario_id == "T-008":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "different_thread_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "different-thread-tests": ["cargo", "test", "-p", "b4-http", "--test", "different_thread_tcp",
                                      "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "t002-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t003-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t004-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_route_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t005-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_json_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t006-regression": ["cargo", "test", "-p", "b4-http", "--test", "input_shape_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t007-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_concurrency_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "different-thread-tests", "t001-regression", "t002-regression",
                             "t003-regression", "t004-regression", "t005-regression", "t006-regression", "t007-regression")
        if scenario_id == "T-009":
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "wait_output_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "wait-output-tests": ["cargo", "test", "-p", "b4-http", "--test", "wait_output_tcp",
                                      "--locked", "--offline", "--", "--test-threads=1"],
                "t001-regression": commands["native-tcp-tests"],
                "t002-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_state_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t003-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_admission_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t004-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_route_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t005-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_json_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t006-regression": ["cargo", "test", "-p", "b4-http", "--test", "input_shape_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t007-regression": ["cargo", "test", "-p", "b4-http", "--test", "thread_concurrency_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "t008-regression": ["cargo", "test", "-p", "b4-http", "--test", "different_thread_tcp",
                                    "--locked", "--offline", "--", "--test-threads=1"],
                "format-check": commands["format-check"],
                "scoped-lint": commands["scoped-lint"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = ("scenario-test", "wait-output-tests", "t001-regression", "t002-regression",
                             "t003-regression", "t004-regression", "t005-regression", "t006-regression", "t007-regression", "t008-regression")
        if scenario_id == "T-010":
            # The private qualifier binds actual maps/logs/receipts. Public checking
            # validates the delivered bounded schema, without recomputing private
            # evidence or claiming a full-suite pass on the final metadata map.
            feature = receipt.get("feature_validation")
            require(isinstance(feature, dict), "Missing T-010 separate feature validation")
            for key in ("source_descriptor_sha256", "receipt_sha256", "log_sha256"):
                require(isinstance(feature.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", feature[key]),
                        f"Invalid T-010 feature validation digest: {key}")
            require(feature["source_descriptor_sha256"] != descriptor,
                    "T-010 feature validation must retain its separate earlier source descriptor")
            for key in ("source_file_count", "reported_passes", "reported_summary_executions"):
                require(type(feature.get(key)) is int and feature[key] > 0,
                        f"Invalid T-010 feature validation count: {key}")
            require(type(feature.get("reported_ignored")) is int and feature["reported_ignored"] >= 0,
                    "Invalid T-010 feature ignored count")
            require(isinstance(feature.get("run"), str)
                    and re.fullmatch(r"verification/feature-workspace-[0-9]+", feature["run"]),
                    "Invalid T-010 feature validation run")
            feature_command = feature.get("command")
            require(isinstance(feature_command, list) and feature_command
                    and isinstance(feature_command[0], str)
                    and [Path(feature_command[0]).name, *feature_command[1:]] ==
                    ["cargo", "test", "--workspace", "--locked", "--offline", "--", "--test-threads=1"],
                    "T-010 feature command differs")
            require(type(feature.get("exit_code")) is int and feature["exit_code"] == 0
                    and feature.get("reaped") is True and feature.get("timed_out") is False
                    and feature.get("source_unchanged") is True
                    and type(feature.get("deadline_seconds")) is int and feature["deadline_seconds"] == 1800
                    and feature.get("additional_runtime_credit") is False,
                    "T-010 separate feature validation must succeed without final runtime credit")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "stream_payload_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "stream-payload-tests": ["cargo", "test", "-p", "b4-http", "--test", "stream_payload_tcp",
                                         "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test", "native_streaming",
                                            "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test", "native_streaming",
                                                "--locked", "--offline", "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp"), 1)},
                "format-check": commands["format-check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = tuple(commands)[:13]
        if scenario_id == "T-011":
            # This proof has its own initial map, with no new feature-workspace claim.
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-011 initial proof digest: {key}")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-011 initial proof run")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "terminal_frame_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "terminal-frame-tests": ["cargo", "test", "-p", "b4-http", "--test", "terminal_frame_tcp",
                                         "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test", "native_streaming",
                                            "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test", "native_streaming",
                                                "--locked", "--offline", "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp"), 1)},
                "format-check": commands["format-check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = tuple(commands)[:14]
        if scenario_id == "T-012":
            # This proof has its own initial map, with no new feature-workspace claim.
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-012 initial proof digest: {key}")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-012 initial proof run")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "disconnect_completion_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "disconnect-completion-tests": ["cargo", "test", "-p", "b4-http", "--test", "disconnect_completion_tcp",
                                         "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test", "native_streaming",
                                            "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test", "native_streaming",
                                                "--locked", "--offline", "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp", "terminal_frame_tcp"), 1)},
                "format-check": commands["format-check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = tuple(commands)[:15]
        if scenario_id == "T-013":
            # This proof has its own initial map, with no new feature-workspace claim.
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-013 initial proof digest: {key}")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-013 initial proof run")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "attachment_state_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "attachment-state-tests": ["cargo", "test", "-p", "b4-http", "--test", "attachment_state_tcp",
                                         "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test", "native_streaming",
                                            "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test", "native_streaming",
                                                "--locked", "--offline", "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp", "terminal_frame_tcp",
                       "disconnect_completion_tcp"), 1)},
                "format-check": commands["format-check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": commands["guard-tests"],
            }
            runtime_steps = tuple(commands)[:16]
        if scenario_id == "T-014":
            # This proof has its own initial map, with no new feature-workspace claim.
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-014 initial proof digest: {key}")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-014 initial proof run")
            require(initial["source_snapshot_sha256"] != descriptor,
                    "T-014 initial proof must retain its own earlier source map")
            require("feature_validation" not in receipt,
                    "T-014 must not inherit separate feature validation")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "attachment_cut_tcp", test_name,
                                  "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "attachment-cut-tests": ["cargo", "test", "-p", "b4-http", "--test", "attachment_cut_tcp",
                                         "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test", "native_streaming",
                                            "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test", "native_streaming",
                                                "--locked", "--offline", "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp", "terminal_frame_tcp",
                       "disconnect_completion_tcp", "attachment_state_tcp"), 1)},
                "format-check": commands["format-check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": ["/Users/blove/.pyenv/versions/3.12.12/bin/python3", "-B",
                                "scripts/test_core_release_traceability.py"],
            }
            runtime_steps = tuple(commands)[:17]
        if scenario_id == "T-015":
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-015 initial proof digest: {key}")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-015 initial proof run")
            pre_fix = receipt.get("pre_fix_proof")
            require(isinstance(pre_fix, dict), "Missing T-015 pre-fix proof")
            require(set(pre_fix) == {"run", "receipt_sha256", "log_sha256",
                                     "source_descriptor_sha256", "exit_code",
                                     "expected_failure", "additional_runtime_credit"},
                    "T-015 pre-fix proof fields differ")
            require(isinstance(pre_fix.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", pre_fix["run"]),
                    "Invalid T-015 pre-fix proof run")
            for key in ("receipt_sha256", "log_sha256", "source_descriptor_sha256"):
                require(isinstance(pre_fix.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", pre_fix[key]),
                        f"Invalid T-015 pre-fix proof digest: {key}")
            require(type(pre_fix.get("exit_code")) is int and pre_fix["exit_code"] == 101,
                    "T-015 pre-fix proof needs exact exit 101")
            require(pre_fix.get("expected_failure") == "late_attach_409_instead_of_200",
                    "T-015 pre-fix proof failure differs")
            require(pre_fix.get("additional_runtime_credit") is False,
                    "T-015 pre-fix proof must not add runtime credit")
            require(len({descriptor, initial["source_snapshot_sha256"],
                         pre_fix["source_descriptor_sha256"]}) == 3,
                    "T-015 final, initial and pre-fix proofs need distinct own maps")
            require("feature_validation" not in receipt,
                    "T-015 must not inherit separate feature validation")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test",
                                  "attachment_truncation_tcp", test_name, "--locked", "--offline", "--",
                                  "--nocapture", "--test-threads=1"],
                "attachment-truncation-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                                "attachment_truncation_tcp", "--locked", "--offline", "--",
                                                "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test", "native_streaming",
                                            "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test", "native_streaming",
                                                "--locked", "--offline", "--", "--test-threads=1"],
                "http-control-tests": ["cargo", "test", "-p", "b4-http", "--lib", "--locked", "--offline",
                                       "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp", "terminal_frame_tcp",
                       "disconnect_completion_tcp", "attachment_state_tcp", "attachment_cut_tcp"), 1)},
                "format-check": commands["format-check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": ["/Users/blove/.pyenv/versions/3.12.12/bin/python3", "-B",
                                "scripts/test_core_release_traceability.py"],
            }
            runtime_steps = tuple(commands)[:19]
        if scenario_id == "T-016":
            require(set(initial) == {"run", "receipt_sha256", "source_snapshot_sha256",
                                     "additional_runtime_credit"},
                    "T-016 initial proof fields differ")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-016 initial proof run")
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-016 initial proof digest: {key}")
            require(initial["source_snapshot_sha256"] != descriptor,
                    "T-016 initial and final proofs need distinct own maps")
            require("feature_validation" not in receipt,
                    "T-016 must not inherit separate feature validation")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "viewer_overflow_tcp",
                                  test_name, "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "viewer-overflow-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                          "viewer_overflow_tcp", "--locked", "--offline", "--",
                                          "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test",
                                            "native_streaming", "--locked", "--offline", "--",
                                            "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                                "native_streaming", "--locked", "--offline", "--",
                                                "--test-threads=1"],
                "http-control-tests": ["cargo", "test", "-p", "b4-http", "--lib", "--locked", "--offline",
                                       "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp", "terminal_frame_tcp",
                       "disconnect_completion_tcp", "attachment_state_tcp", "attachment_cut_tcp",
                       "attachment_truncation_tcp"), 1)},
                "format-check": commands["format-check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": ["/Users/blove/.pyenv/versions/3.12.12/bin/python3", "-B",
                                "scripts/test_core_release_traceability.py"],
            }
            runtime_steps = tuple(commands)[:20]
        if scenario_id == "T-017":
            require(set(initial) == {"run", "receipt_sha256", "source_snapshot_sha256",
                                     "additional_runtime_credit"},
                    "T-017 initial proof fields differ")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-017 initial proof run")
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-017 initial proof digest: {key}")
            require(initial["source_snapshot_sha256"] != descriptor,
                    "T-017 initial and final proofs need distinct own maps")
            require("feature_validation" not in receipt,
                    "T-017 must not inherit separate feature validation")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test", "heartbeat_splits_tcp",
                                  test_name, "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
                "heartbeat-splits-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                            "heartbeat_splits_tcp", "--locked", "--offline", "--",
                                            "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test",
                                            "native_streaming", "--locked", "--offline", "--",
                                            "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                                "native_streaming", "--locked", "--offline", "--",
                                                "--test-threads=1"],
                "http-control-tests": ["cargo", "test", "-p", "b4-http", "--lib", "--locked", "--offline",
                                       "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp", "terminal_frame_tcp",
                       "disconnect_completion_tcp", "attachment_state_tcp", "attachment_cut_tcp",
                       "attachment_truncation_tcp", "viewer_overflow_tcp"), 1)},
                "format-check": ["cargo", "fmt", "--all", "--", "--check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "--all-targets",
                                "--locked", "--offline", "--", "-D", "warnings"],
                "guard-tests": ["/Users/blove/.pyenv/versions/3.12.12/bin/python3", "-B",
                                "scripts/test_core_release_traceability.py"],
            }
            runtime_steps = tuple(commands)[:21]
        if scenario_id == "T-018":
            require(set(initial) == {"run", "receipt_sha256", "source_snapshot_sha256",
                                     "additional_runtime_credit"},
                    "T-018 initial proof fields differ")
            require(isinstance(initial.get("run"), str)
                    and re.fullmatch(r"verification/scenario-test-[0-9]+", initial["run"]),
                    "Invalid T-018 initial proof run")
            for key in ("receipt_sha256", "source_snapshot_sha256"):
                require(isinstance(initial.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", initial[key]),
                        f"Invalid T-018 initial proof digest: {key}")
            require(initial["source_snapshot_sha256"] != descriptor,
                    "T-018 initial and final proofs need distinct own maps")
            require("feature_validation" not in receipt,
                    "T-018 must not inherit separate feature validation")
            commands = {
                "scenario-test": ["cargo", "test", "-p", "b4-http", "--test",
                                  "approval_durable_disclosure_tcp", test_name, "--locked",
                                  "--offline", "--", "--nocapture", "--test-threads=1"],
                "durable-disclosure-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                             "approval_durable_disclosure_tcp", "--locked", "--offline",
                                             "--", "--test-threads=1"],
                "approval-http-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                        "approval_tcp", "--locked", "--offline", "--", "--test-threads=1"],
                "approval-sqlite-tests": ["cargo", "test", "-p", "b4-sqlite", "--test",
                                          "approvals", "--locked", "--offline", "--", "--test-threads=1"],
                "native-stream-api-tests": ["cargo", "test", "-p", "b4-runtime", "--test",
                                            "native_streaming", "--locked", "--offline", "--",
                                            "--test-threads=1"],
                "native-stream-control-tests": ["cargo", "test", "-p", "b4-http", "--test",
                                                "native_streaming", "--locked", "--offline", "--",
                                                "--test-threads=1"],
                "http-control-tests": ["cargo", "test", "-p", "b4-http", "--lib", "--locked",
                                       "--offline", "--", "--test-threads=1"],
                **{f"t{number:03d}-regression":
                   ["cargo", "test", "-p", "b4-http", "--test", target,
                    "--locked", "--offline", "--", "--test-threads=1"]
                   for number, target in enumerate(("native_tcp", "thread_state_tcp", "thread_admission_tcp",
                       "thread_route_tcp", "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
                       "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp", "terminal_frame_tcp",
                       "disconnect_completion_tcp", "attachment_state_tcp", "attachment_cut_tcp",
                       "attachment_truncation_tcp", "viewer_overflow_tcp", "heartbeat_splits_tcp"), 1)},
                "format-check": ["cargo", "fmt", "--all", "--", "--check"],
                "scoped-lint": ["cargo", "clippy", "-p", "b4-runtime", "-p", "b4-http", "-p",
                                "b4-sqlite", "--all-targets", "--locked", "--offline", "--", "-D",
                                "warnings"],
                "guard-tests": ["/Users/blove/.pyenv/versions/3.12.12/bin/python3", "-B",
                                "scripts/test_core_release_traceability.py"],
            }
            runtime_steps = tuple(commands)[:24]
        checks = receipt.get("checks")
        require(isinstance(checks, list) and all(isinstance(check, dict) for check in checks)
                and [check.get("step") for check in checks] == list(commands),
                f"{scenario_id} qualification needs the exact ordered current checks")
        for check in checks:
            step = check["step"]
            if scenario_id in ("T-010", "T-011", "T-012", "T-013", "T-014", "T-015", "T-016", "T-017", "T-018"):
                require(check.get("metadata_only") is (step == "guard-tests"),
                        f"{scenario_id} guard checks alone must be metadata-only")
            command = check.get("command")
            require(isinstance(command, list) and command and isinstance(command[0], str)
                    and (command if scenario_id in ("T-014", "T-015", "T-016", "T-017", "T-018") and step == "guard-tests"
                         else [Path(command[0]).name, *command[1:]]) == commands[step],
                    f"{scenario_id} check command differs: {step}")
            if scenario_id in ("T-017", "T-018"):
                require(type(check.get("pid")) is int and check["pid"] > 0
                        and type(check.get("process_group")) is int
                        and check["process_group"] == check["pid"]
                        and check.get("start_new_session") is True
                        and check.get("process_identity_verified") is True
                        and check.get("process_group_retired") is True,
                        f"{scenario_id} child identity/group retirement incomplete")
            require(type(check.get("exit_code")) is int and check["exit_code"] == 0
                    and check.get("reaped") is True and check.get("timed_out") is False
                    and check.get("historical_result_transfer") is False
                    and all(check.get(key) == [] for key in
                            ("changed_source_files", "added_source_files", "removed_source_files"))
                    and check.get("source_descriptor_sha256") == descriptor
                    and check.get("runtime_credit") is (step in runtime_steps),
                    f"{scenario_id} current check is incomplete, changed, or unsuccessful: {step}")


def reject_non_json_number(value):
    raise ValueError(f"Non-JSON numeric literal: {value}")


def read_json(path):
    return json.loads(path.read_text(), parse_constant=reject_non_json_number)


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def compact_json(value):
    # The recorded inventory/projection contains ordinary JSON numbers. Preserve
    # insertion order and Unicode for the fixture's JSON.stringify digests.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def repo_file(value):
    require(isinstance(value, str) and not Path(value).is_absolute(),
            f"Evidence path must be repository-relative: {value!r}")
    path = (REPO / value).resolve()
    require(path.is_relative_to(REPO) and path.is_file(), f"Missing evidence file: {value}")
    return path


def check_file_digest(item):
    path = repo_file(item["path"])
    require(sha256(path.read_bytes()) == item["sha256"], f"Evidence hash changed: {item['path']}")
    return path


def normalize_body(body, mode):
    if mode != "thread":
        return body
    try:
        value = json.loads(body, parse_constant=reject_non_json_number)
    except ValueError:
        return body
    if not isinstance(value, dict):
        return body
    if isinstance(value.get("thread_id"), str):
        value["thread_id"] = "<generated-thread-id>"
    for key in ["created_at", "updated_at"]:
        if isinstance(value.get(key), str):
            value[key] = "<generated-timestamp>"
    return compact_json(value).decode("utf-8")


def sse_frames(body):
    frames = []
    for block in body.replace("\r\n", "\n").split("\n\n"):
        event, data = "message", []
        for line in block.split("\n"):
            if line.startswith("event:"):
                event = line[6:].lstrip()
            if line.startswith("data:"):
                value = line[5:]
                data.append(value[1:] if value.startswith(" ") else value)
        if not data:
            continue
        data_text = "\n".join(data)
        try:
            decoded = json.loads(data_text, parse_constant=reject_non_json_number)
        except ValueError:
            decoded = data_text
        frames.append({"event": event, "data": decoded, "data_text": data_text})
    return frames


def check_body(record):
    raw = base64.b64decode(record["body_base64"], validate=True)
    require(base64.b64encode(raw).decode("ascii") == record["body_base64"],
            "Noncanonical recorded body base64")
    require(raw.decode("utf-8") == record["body_utf8"] and sha256(raw) == record["body_sha256"],
            "Recorded body UTF-8/base64/SHA-256 disagree")


def check_response(response, mode):
    check_body(response)
    require(response["normalized_body_utf8"] == normalize_body(response["body_utf8"], mode),
            "Recorded normalization altered fields outside its declared scope")
    if any(key == "content-type" and "text/event-stream" in value
           for key, value in response["headers"]):
        require(response.get("sse_frames") == sse_frames(response["body_utf8"]),
                "Recorded SSE decoding differs from raw bytes")


def response_projection(response):
    value = {"status": response["status"], "status_text": response["status_text"],
             "headers": response["headers"], "body": response["normalized_body_utf8"]}
    if "sse_frames" in response:
        value["sse_frames"] = response["sse_frames"]
    return value


def recording_projection(recording):
    result = []
    for observation in recording["observations"]:
        value = {"id": observation["id"], "request": observation["request"],
                 "response": response_projection(observation["response"])}
        if "thread_after" in observation:
            value["thread_after"] = response_projection(observation["thread_after"])
        result.append(value)
    return result


def check_reference_recordings():
    root = REPO / "conformance/reference/recordings/2026-09-30-workflows"
    receipt = read_json(root / "workflow-repeatability.json")
    require(receipt["format_version"] == 2 and receipt["transport"] == "in_process_fetch",
            "Expected retained in-process reference repeatability evidence")
    paths = [repo_file(receipt[f"{name}_recording_path"]) for name in ["first", "second"]]
    require(paths[0] != paths[1], "Repeatability requires two distinct retained recordings")
    fixture = REPO / "conformance/reference/fixture"
    cases_bytes = (fixture / "cases.json").read_bytes()
    cases = read_json(fixture / "cases.json")
    require(len(cases) == 62 and len({case["id"] for case in cases}) == 62,
            "Selected independently authored request inventory must retain 62 cases")
    recordings = []
    for name, path in zip(["first", "second"], paths):
        require(sha256(path.read_bytes()) == receipt[f"{name}_recording_sha256"],
                f"Retained {name} reference recording hash differs")
        recording = read_json(path)
        require(recording["evidence_classification"] == "runtime_observation"
                and recording["transport"] == "in_process_fetch", "Reference evidence class changed")
        require(recording["reference"]["commit"] == "8acac534ba6eb960b34de217b0da45b8405023af",
                "Reference commit pin differs")
        lockfile = REPO / "conformance/reference/source/pnpm-lock.yaml"
        require(recording["reference"]["lockfile_sha256"] == sha256(lockfile.read_bytes()),
                "Reference lockfile pin differs from captured source")
        require(recording["execution"]["external_fetch_attempts"] == 0,
                "Deterministic reference fixture attempted an external Fetch")
        application = recording["fixture_application"]
        require(application["cases_sha256"] == sha256(cases_bytes), "Authored request corpus changed")
        for item in application["files"]:
            app_path = (fixture / "app" / item["path"]).resolve()
            require(app_path.is_relative_to(fixture / "app") and app_path.is_file(),
                    "Fixture application path missing or escaped")
            raw = app_path.read_bytes()
            require(sha256(raw) == item["sha256"] and len(raw) == item["bytes"],
                    f"Fixture application artifact changed: {item['path']}")
        revision = sha256(compact_json({"app": application["files"],
                                       "cases_sha256": application["cases_sha256"]}))
        require(application["revision"] == revision == receipt["fixture_revision"],
                "Fixture application revision differs")
        observations = recording["observations"]
        require([item["id"] for item in observations] == [case["id"] for case in cases],
                "Reference observations do not cover the independent corpus in order")
        for observation, case in zip(observations, cases):
            request = observation["request"]
            check_body(request)
            require(all(request[key] == case[key] for key in ["method", "path", "headers"])
                    and request["body_utf8"] == case["body"]
                    and observation["characterization"] == case["characterization"],
                    "Captured request differs from independently authored request bytes")
            check_response(observation["response"], case["normalization"])
            require(("thread_after" in observation) == case["path"].startswith("/threads/"),
                    "Post-run thread-read inventory differs")
            if "thread_after" in observation:
                check_response(observation["thread_after"], "thread")
        recordings.append(recording)
    for key in ["reference", "fixture_application", "runtime_entry", "effective_dependency_versions"]:
        require(recordings[0][key] == recordings[1][key], f"Reference {key} changed between boots")
    first = recording_projection(recordings[0])
    require(first == recording_projection(recordings[1]), "Independent repeat projections differ")
    require(receipt["compared_cases"] == receipt["matching_cases"] == 62
            and receipt["normalized_projection_sha256"] == sha256(compact_json(first)),
            "Reference repeatability receipt digest/count differs")
    require(receipt["reference_commit"] == recordings[0]["reference"]["commit"],
            "Repeatability receipt pin differs")
    for item in [receipt["comparison_script"], *receipt["comparison_dependencies"]]:
        check_file_digest(item)

    artifacts = read_json(root / "runtime-artifacts.json")
    require(artifacts["evidence_classification"] == "built_runtime_artifact_inventory"
            and artifacts["reference"] == recordings[0]["reference"], "Runtime provenance pin differs")
    require(artifacts["observation_timing"] == "post_capture_inventory",
            "Legacy recordings cannot gain contemporaneous transitive-artifact evidence")
    files = artifacts["files"]
    require(len(files) == 414 and [item["path"] for item in files] == sorted(item["path"] for item in files)
            and len({item["path"] for item in files}) == len(files), "Built artifact inventory differs")
    require(sha256(compact_json(files)) == artifacts["inventory_sha256"],
            "Built artifact inventory digest differs")
    for key in ["runtime_wrapper", "runtime_core"]:
        require(artifacts[key] in files, f"Missing built {key} inventory entry")
    require(artifacts["runtime_wrapper"]["sha256"] == recordings[0]["runtime_entry"]["sha256"],
            "Observed wrapper differs from capture wrapper")
    require([item["path"] for item in artifacts["recording_links"]]
            == [path.relative_to(REPO).as_posix() for path in paths], "Provenance recording links differ")
    for item in artifacts["recording_links"]:
        check_file_digest(item)
        require(item["runtime_wrapper_sha256"] == artifacts["runtime_wrapper"]["sha256"],
                "Provenance link wrapper differs")
    for item in [artifacts["inventory_script"], *artifacts["inventory_dependencies"]]:
        check_file_digest(item)
    return 62


def check_capability_schema(validator, version=1):
    """Authored operational contract checks, separate from core AP conformance."""
    prefix = "capabilities" if version == 1 else "capabilities-v2"
    native = read_json(CONTRACTS / "examples" / f"{prefix}-native.json")
    scripted = read_json(CONTRACTS / "examples" / f"{prefix}-scripted.json")
    for value in [native, scripted]:
        validator.validate(value)
    invalid = []
    for parent in [None, "application", "execution", "features", "store", "readiness", "limits", "controls"]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])["routes"] = ["private-route"]
        invalid.append(value)
    configurable = {"active_runs": 256, "concurrent_tasks": 256,
                    "concurrent_tasks_per_execution": 64, "viewer_queue_frames": 512,
                    "heartbeat_ms": 60000, "input_bytes": 65536, "output_bytes": 65536,
                    "max_attached_viewers": 64, "turn_buffer_bytes": 8388608,
                    "turn_buffer_frames": 4096, "viewer_queue_bytes": 8388608,
                    "attachment_snapshot_bytes": 8388608, "approval_ttl_ms": 86400000}
    for field, maximum in configurable.items():
        for limit in [0, maximum + 1, -1, 1.5]:
            value = copy.deepcopy(native)
            value["limits"][field] = limit
            invalid.append(value)
        for limit in [1, maximum]:
            value = copy.deepcopy(native)
            value["limits"][field] = limit
            validator.validate(value)
    for field in ["body_bytes", "json_depth", "program_routes", "program_tasks",
                  "program_attempts", "completion_attempts", "task_failure_bytes",
                  "script_chunks", "script_delay_ms", "approval_bytes"]:
        value = copy.deepcopy(native)
        value["limits"][field] = 0
        invalid.append(value)
    for field in ["automatic_recovery_scan", "always_permissions", "durable_event_replay",
                  "native_live_chunks", "built_in_model_providers", "memory_candidates",
                  "managed_workspaces", "ag_ui", "dynamic_graphs", "graceful_shutdown",
                  "turn_deadline", "native_callback_sandbox"]:
        value = copy.deepcopy(native)
        value["features"][field] = True
        invalid.append(value)
    for field in native["controls"]:
        value = copy.deepcopy(native)
        value["controls"][field] = 100
        invalid.append(value)
    for parent, field in [(None, "application"), ("store", "durable_approvals"),
                          ("limits", "approval_ttl_ms"),
                          *[("controls", field) for field in native["controls"]]]:
        value = copy.deepcopy(scripted)
        del (value if parent is None else value[parent])[field]
        invalid.append(value)
    for parent, field in [("application", "id"), ("application", "revision"), (None, "runtime_version")]:
        for label in ["", "bad\nlabel", "bad\u0085label", "x" * 257]:
            value = copy.deepcopy(native)
            (value if parent is None else value[parent])[field] = label
            invalid.append(value)
    for parent, field, data in [(None, "application", None), (None, "descriptor_version", 2 if version == 1 else 1),
                               (None, "core_profile_frozen", True), (None, "target_profile", "b4-ap-core/1"),
                               (None, "profile_status", "full"), ("execution", "native_callbacks", False),
                               ("execution", "mode", "distributed"), ("execution", "registration", "dynamic"),
                               ("execution", "approval_service_mode", "disabled"),
                               ("execution", "approval_service_mode", "always"),
                               ("features", "required_once_deny", False),
                               ("features", "trusted_known_id_recovery", False),
                               ("limits", "approval_ttl_ms", None), ("store", "durable_approvals", None),
                               ("store", "durable_approvals", False), ("store", "kind", "private-backend"),
                               ("readiness", "response", "checks"), ("readiness", "aggregation", "healthy"),
                               ("readiness", "checks", ["owned_worker_fault", "store_readiness"])]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])[field] = data
        invalid.append(value)
    value = copy.deepcopy(scripted)
    value["limits"]["approval_ttl_ms"] = 300000
    invalid.append(value)
    value = copy.deepcopy(native)
    value["execution"]["approval_service_mode"] = "volatile_fixture"
    value["store"]["durable_approvals"] = None
    invalid.append(value)
    if version == 2:
        for pointer in ["", "/application", "/execution", "/features", "/store", "/readiness", "/limits", "/controls"]:
            original = native
            for key in pointer.lstrip("/").split("/") if pointer else []:
                original = original[key]
            for field in original:
                value = copy.deepcopy(native)
                node = value
                for key in pointer.lstrip("/").split("/") if pointer else []:
                    node = node[key]
                del node[field]
                invalid.append(value)
        for wrong in [False, None, 1, "true"]:
            value = copy.deepcopy(native)
            value["features"]["supervised_admission_drain"] = wrong
            invalid.append(value)
        for checks in [[], ["store_readiness", "owned_worker_fault"],
                       ["serving_admission", "store_readiness", "owned_worker_fault"],
                       ["store_readiness", "owned_worker_fault", "owned_worker_fault"],
                       ["store_readiness", "owned_worker_fault", "serving_admission", "serving_admission"]]:
            value = copy.deepcopy(native)
            value["readiness"]["checks"] = checks
            invalid.append(value)
        for field, fixed in [("shutdown_default_wait_ms", 30000), ("shutdown_max_wait_ms", 300000)]:
            for wrong in [0, fixed - 1, fixed + 1, -1, 1.5, None]:
                value = copy.deepcopy(native)
                value["controls"][field] = wrong
                invalid.append(value)
        value = copy.deepcopy(native)
        value["readiness"]["aggregation"] = "store_readiness_and_no_owned_worker_fault"
        invalid.append(value)
    for value in invalid:
        require(not validator.is_valid(value), "Capability schema accepted invalid or private metadata")
    for kind, runtime, approvals in [("memory", False, False), ("sqlite", True, True),
                                     ("unspecified", False, False)]:
        value = copy.deepcopy(native)
        value["execution"]["approval_service_mode"] = "volatile_fixture"
        value["store"] = {"kind": kind, "durable_runtime": runtime, "durable_approvals": approvals}
        validator.validate(value)
    # Standard JSON Schema counts code points. Rust additionally enforces the
    # annotated UTF-8 byte bound; do not claim that ordinary schema tools do so.
    value = copy.deepcopy(native)
    value["application"]["id"] = "é" * 129
    validator.validate(value)
    return len(invalid)


def check_permission_reuse_capability_schema(validator):
    """Authored V3 profile checks; no scripted V3 or runtime execution claim."""
    native = read_json(CONTRACTS / "examples" / "capabilities-v3-permission-use.json")
    validator.validate(native)
    invalid = []
    for parent in [None, "application", "execution", "features", "store", "readiness", "limits", "controls"]:
        original = native if parent is None else native[parent]
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])["routes"] = ["private-route"]
        invalid.append(value)
        for field in original:
            value = copy.deepcopy(native)
            del (value if parent is None else value[parent])[field]
            invalid.append(value)
    for field, expected in native["features"].items():
        value = copy.deepcopy(native)
        value["features"][field] = not expected
        invalid.append(value)
    for field in ["active_runs", "concurrent_tasks", "concurrent_tasks_per_execution"]:
        for wrong in [0, 2, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
    for field, maximum in {
        "viewer_queue_frames": 512, "heartbeat_ms": 60000,
        "input_bytes": 65536, "output_bytes": 65536, "max_attached_viewers": 64,
        "turn_buffer_bytes": 8388608, "turn_buffer_frames": 4096,
        "viewer_queue_bytes": 8388608, "attachment_snapshot_bytes": 8388608,
        "approval_ttl_ms": 86400000,
    }.items():
        for wrong in [0, maximum + 1, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
        for valid in [1, maximum]:
            value = copy.deepcopy(native)
            value["limits"][field] = valid
            validator.validate(value)
    for field in ["body_bytes", "json_depth", "program_routes", "program_tasks",
                  "program_attempts", "completion_attempts", "task_failure_bytes",
                  "script_chunks", "script_delay_ms", "approval_bytes"]:
        value = copy.deepcopy(native)
        value["limits"][field] += 1
        invalid.append(value)
    for field in native["controls"]:
        value = copy.deepcopy(native)
        value["controls"][field] = 100
        invalid.append(value)
    for parent, field, wrong in [
        (None, "descriptor_version", 1), (None, "descriptor_version", 2),
        (None, "descriptor_version", 4), (None, "application", None),
        (None, "target_profile", "b4-ap-core/1"), (None, "profile_status", "full"),
        (None, "core_profile_frozen", True),
        ("execution", "native_callbacks", False),
        ("execution", "approval_service_mode", "disabled"),
        ("execution", "approval_service_mode", "volatile_fixture"),
        ("execution", "mode", "distributed"), ("execution", "registration", "dynamic"),
        ("store", "durable_runtime", False), ("store", "durable_approvals", False),
        ("store", "durable_approvals", None), ("store", "kind", "private-backend"),
        ("readiness", "response", "checks"),
        ("readiness", "aggregation", "store_readiness_and_no_owned_worker_fault"),
        ("readiness", "checks", ["store_readiness", "owned_worker_fault"]),
        ("readiness", "checks", ["serving_admission", "owned_worker_fault", "store_readiness"]),
    ]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])[field] = wrong
        invalid.append(value)
    for parent, field in [(None, "runtime_version"), ("application", "id"), ("application", "revision")]:
        for wrong in ["", "bad\nlabel", "bad\u0085label", "x" * 257]:
            value = copy.deepcopy(native)
            (value if parent is None else value[parent])[field] = wrong
            invalid.append(value)
    for value in invalid:
        require(not validator.is_valid(value), f"V3 accepted invalid permission reuse metadata: {value!r}")
    return len(invalid)


def check_permission_approvals_capability_schema(validator):
    """Authored V4 profile checks; no HTTP or runtime execution claim."""
    native = read_json(CONTRACTS / "examples" / "capabilities-v4-permission-approvals.json")
    validator.validate(native)
    expected = read_json(CONTRACTS / "examples" / "capabilities-v3-permission-use.json")
    expected["descriptor_version"] = 4
    expected["features"]["durable_permission_creation"] = True
    require(native == expected, "V4 example must preserve the V3 base except version and creation")
    invalid = []
    for parent in [None, "application", "execution", "features", "store", "readiness", "limits", "controls"]:
        original = native if parent is None else native[parent]
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])["routes"] = ["private-route"]
        invalid.append(value)
        for field in original:
            value = copy.deepcopy(native)
            del (value if parent is None else value[parent])[field]
            invalid.append(value)
    for field, expected in native["features"].items():
        for wrong in [not expected, "true", None, 1]:
            value = copy.deepcopy(native)
            value["features"][field] = wrong
            invalid.append(value)
    for field in ["active_runs", "concurrent_tasks", "concurrent_tasks_per_execution"]:
        for wrong in [0, 2, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
    for field, maximum in {
        "viewer_queue_frames": 512, "heartbeat_ms": 60000,
        "input_bytes": 65536, "output_bytes": 65536, "max_attached_viewers": 64,
        "turn_buffer_bytes": 8388608, "turn_buffer_frames": 4096,
        "viewer_queue_bytes": 8388608, "attachment_snapshot_bytes": 8388608,
        "approval_ttl_ms": 86400000,
    }.items():
        for wrong in [0, maximum + 1, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
        for valid in [1, maximum]:
            value = copy.deepcopy(native)
            value["limits"][field] = valid
            validator.validate(value)
    for field in ["body_bytes", "json_depth", "program_routes", "program_tasks",
                  "program_attempts", "completion_attempts", "task_failure_bytes",
                  "script_chunks", "script_delay_ms", "approval_bytes"]:
        value = copy.deepcopy(native)
        value["limits"][field] += 1
        invalid.append(value)
    for field in native["controls"]:
        value = copy.deepcopy(native)
        value["controls"][field] = 100
        invalid.append(value)
    for parent, field, wrong in [
        (None, "descriptor_version", 1), (None, "descriptor_version", 2),
        (None, "descriptor_version", 3), (None, "descriptor_version", 5),
        (None, "descriptor_version", "4"), (None, "descriptor_version", None),
        (None, "application", None),
        (None, "target_profile", "b4-ap-core/1"), (None, "profile_status", "full"),
        (None, "core_profile_frozen", True),
        ("execution", "native_callbacks", False),
        ("execution", "approval_service_mode", "disabled"),
        ("execution", "approval_service_mode", "volatile_fixture"),
        ("execution", "mode", "distributed"), ("execution", "registration", "dynamic"),
        ("store", "durable_runtime", False), ("store", "durable_approvals", False),
        ("store", "durable_approvals", None), ("store", "kind", "private-backend"),
        ("readiness", "response", "checks"),
        ("readiness", "aggregation", "store_readiness_and_no_owned_worker_fault"),
        ("readiness", "checks", ["store_readiness", "owned_worker_fault"]),
        ("readiness", "checks", ["serving_admission", "owned_worker_fault", "store_readiness"]),
    ]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])[field] = wrong
        invalid.append(value)
    for parent, field in [(None, "runtime_version"), ("application", "id"), ("application", "revision")]:
        for wrong in ["", "bad\nlabel", "bad\u0085label", "x" * 257]:
            value = copy.deepcopy(native)
            (value if parent is None else value[parent])[field] = wrong
            invalid.append(value)
    for value in invalid:
        require(not validator.is_valid(value), f"V4 accepted invalid permission approval metadata: {value!r}")
    return len(invalid)


def check_permission_entered_cancellation_capability_schema(validator):
    """Authored V5 profile checks; no HTTP or runtime execution claim."""
    native = read_json(CONTRACTS / "examples" / "capabilities-v5-permission-entered-cancellation.json")
    validator.validate(native)
    expected = read_json(CONTRACTS / "examples" / "capabilities-v4-permission-approvals.json")
    expected["descriptor_version"] = 5
    expected["features"]["durable_permission_entered_cancellation"] = True
    require(native == expected, "V5 example must preserve the V4 base except version and entered cancellation")
    invalid = []
    for parent in [None, "application", "execution", "features", "store", "readiness", "limits", "controls"]:
        original = native if parent is None else native[parent]
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])["routes"] = ["private-route"]
        invalid.append(value)
        for field in original:
            value = copy.deepcopy(native)
            del (value if parent is None else value[parent])[field]
            invalid.append(value)
    for field, expected in native["features"].items():
        for wrong in [not expected, "true", None, 1]:
            value = copy.deepcopy(native)
            value["features"][field] = wrong
            invalid.append(value)
    for field in ["active_runs", "concurrent_tasks", "concurrent_tasks_per_execution"]:
        for wrong in [0, 2, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
    for field, maximum in {
        "viewer_queue_frames": 512, "heartbeat_ms": 60000,
        "input_bytes": 65536, "output_bytes": 65536, "max_attached_viewers": 64,
        "turn_buffer_bytes": 8388608, "turn_buffer_frames": 4096,
        "viewer_queue_bytes": 8388608, "attachment_snapshot_bytes": 8388608,
        "approval_ttl_ms": 86400000,
    }.items():
        for wrong in [0, maximum + 1, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
        for valid in [1, maximum]:
            value = copy.deepcopy(native)
            value["limits"][field] = valid
            validator.validate(value)
    for field in ["body_bytes", "json_depth", "program_routes", "program_tasks",
                  "program_attempts", "completion_attempts", "task_failure_bytes",
                  "script_chunks", "script_delay_ms", "approval_bytes"]:
        value = copy.deepcopy(native)
        value["limits"][field] += 1
        invalid.append(value)
    for field in native["controls"]:
        value = copy.deepcopy(native)
        value["controls"][field] = 100
        invalid.append(value)
    for parent, field, wrong in [
        (None, "descriptor_version", 1), (None, "descriptor_version", 2),
        (None, "descriptor_version", 3), (None, "descriptor_version", 4),
        (None, "descriptor_version", 6),
        (None, "descriptor_version", "5"), (None, "descriptor_version", None),
        (None, "application", None),
        (None, "target_profile", "b4-ap-core/1"), (None, "profile_status", "full"),
        (None, "core_profile_frozen", True),
        ("execution", "native_callbacks", False),
        ("execution", "approval_service_mode", "disabled"),
        ("execution", "approval_service_mode", "volatile_fixture"),
        ("execution", "mode", "distributed"), ("execution", "registration", "dynamic"),
        ("store", "durable_runtime", False), ("store", "durable_approvals", False),
        ("store", "durable_approvals", None), ("store", "kind", "private-backend"),
        ("readiness", "response", "checks"),
        ("readiness", "aggregation", "store_readiness_and_no_owned_worker_fault"),
        ("readiness", "checks", ["store_readiness", "owned_worker_fault"]),
        ("readiness", "checks", ["serving_admission", "owned_worker_fault", "store_readiness"]),
    ]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])[field] = wrong
        invalid.append(value)
    for parent, field in [(None, "runtime_version"), ("application", "id"), ("application", "revision")]:
        for wrong in ["", "bad\nlabel", "bad\u0085label", "x" * 257]:
            value = copy.deepcopy(native)
            (value if parent is None else value[parent])[field] = wrong
            invalid.append(value)
    for value in invalid:
        require(not validator.is_valid(value), f"V5 accepted invalid entered permission cancellation metadata: {value!r}")
    return len(invalid)


def check_permission_ready_cancellation_capability_schema(validator):
    """Authored V6 registered Ready/entered metadata; not execution evidence."""
    native = read_json(CONTRACTS / "examples" / "capabilities-v6-permission-ready-cancellation.json")
    validator.validate(native)
    expected = read_json(CONTRACTS / "examples" / "capabilities-v5-permission-entered-cancellation.json")
    expected["descriptor_version"] = 6
    expected["features"]["durable_permission_ready_cancellation"] = True
    require(native == expected, "V6 example must preserve the V5 base except version and registered Ready cancellation")
    invalid = []
    for parent in [None, "application", "execution", "features", "store", "readiness", "limits", "controls"]:
        original = native if parent is None else native[parent]
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])["routes"] = ["private-route"]
        invalid.append(value)
        for field in original:
            value = copy.deepcopy(native)
            del (value if parent is None else value[parent])[field]
            invalid.append(value)
    for field, expected in native["features"].items():
        for wrong in [not expected, "true", None, 1]:
            value = copy.deepcopy(native)
            value["features"][field] = wrong
            invalid.append(value)
    for field in ["active_runs", "concurrent_tasks", "concurrent_tasks_per_execution"]:
        for wrong in [0, 2, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
    for field, maximum in {
        "viewer_queue_frames": 512, "heartbeat_ms": 60000,
        "input_bytes": 65536, "output_bytes": 65536, "max_attached_viewers": 64,
        "turn_buffer_bytes": 8388608, "turn_buffer_frames": 4096,
        "viewer_queue_bytes": 8388608, "attachment_snapshot_bytes": 8388608,
        "approval_ttl_ms": 86400000,
    }.items():
        for wrong in [0, maximum + 1, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
        for valid in [1, maximum]:
            value = copy.deepcopy(native)
            value["limits"][field] = valid
            validator.validate(value)
    for field in ["body_bytes", "json_depth", "program_routes", "program_tasks",
                  "program_attempts", "completion_attempts", "task_failure_bytes",
                  "script_chunks", "script_delay_ms", "approval_bytes"]:
        value = copy.deepcopy(native)
        value["limits"][field] += 1
        invalid.append(value)
    for field in native["controls"]:
        value = copy.deepcopy(native)
        value["controls"][field] = 100
        invalid.append(value)
    for parent, field, wrong in [
        (None, "descriptor_version", 1), (None, "descriptor_version", 2),
        (None, "descriptor_version", 3), (None, "descriptor_version", 4),
        (None, "descriptor_version", 5), (None, "descriptor_version", 7),
        (None, "descriptor_version", "6"), (None, "descriptor_version", None),
        (None, "application", None),
        (None, "target_profile", "b4-ap-core/1"), (None, "profile_status", "full"),
        (None, "core_profile_frozen", True),
        ("execution", "native_callbacks", False),
        ("execution", "approval_service_mode", "disabled"),
        ("execution", "approval_service_mode", "volatile_fixture"),
        ("execution", "mode", "distributed"), ("execution", "registration", "dynamic"),
        ("store", "durable_runtime", False), ("store", "durable_approvals", False),
        ("store", "durable_approvals", None), ("store", "kind", "private-backend"),
        ("readiness", "response", "checks"),
        ("readiness", "aggregation", "store_readiness_and_no_owned_worker_fault"),
        ("readiness", "checks", ["store_readiness", "owned_worker_fault"]),
        ("readiness", "checks", ["serving_admission", "owned_worker_fault", "store_readiness"]),
    ]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])[field] = wrong
        invalid.append(value)
    for parent, field in [(None, "runtime_version"), ("application", "id"), ("application", "revision")]:
        for wrong in ["", "bad\nlabel", "bad\u0085label", "x" * 257]:
            value = copy.deepcopy(native)
            (value if parent is None else value[parent])[field] = wrong
            invalid.append(value)
    for value in invalid:
        require(not validator.is_valid(value), f"V6 accepted invalid registered Ready/entered permission cancellation metadata: {value!r}")
    return len(invalid)


def check_permission_withheld_cancellation_capability_schema(validator):
    """Authored V7 withheld/Ready/entered metadata; not execution evidence."""
    native = read_json(CONTRACTS / "examples" / "capabilities-v7-permission-withheld-cancellation.json")
    validator.validate(native)
    expected = read_json(CONTRACTS / "examples" / "capabilities-v6-permission-ready-cancellation.json")
    expected["descriptor_version"] = 7
    expected["features"]["durable_permission_withheld_cancellation"] = True
    require(native == expected, "V7 example must preserve the V6 base except version and withheld cancellation")
    invalid = []
    for parent in [None, "application", "execution", "features", "store", "readiness", "limits", "controls"]:
        original = native if parent is None else native[parent]
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])["routes"] = ["private-route"]
        invalid.append(value)
        for field in original:
            value = copy.deepcopy(native)
            del (value if parent is None else value[parent])[field]
            invalid.append(value)
    value = copy.deepcopy(native)
    value["features"]["durable_permission_pre_entry_cancellation"] = True
    invalid.append(value)
    for field, expected in native["features"].items():
        for wrong in [not expected, "true", None, 1]:
            value = copy.deepcopy(native)
            value["features"][field] = wrong
            invalid.append(value)
    for field in ["active_runs", "concurrent_tasks", "concurrent_tasks_per_execution"]:
        for wrong in [0, 2, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
    for field, maximum in {
        "viewer_queue_frames": 512, "heartbeat_ms": 60000,
        "input_bytes": 65536, "output_bytes": 65536, "max_attached_viewers": 64,
        "turn_buffer_bytes": 8388608, "turn_buffer_frames": 4096,
        "viewer_queue_bytes": 8388608, "attachment_snapshot_bytes": 8388608,
        "approval_ttl_ms": 86400000,
    }.items():
        for wrong in [0, maximum + 1, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
        for valid in [1, maximum]:
            value = copy.deepcopy(native)
            value["limits"][field] = valid
            validator.validate(value)
    for field in ["body_bytes", "json_depth", "program_routes", "program_tasks",
                  "program_attempts", "completion_attempts", "task_failure_bytes",
                  "script_chunks", "script_delay_ms", "approval_bytes"]:
        value = copy.deepcopy(native)
        value["limits"][field] += 1
        invalid.append(value)
    for field in native["controls"]:
        value = copy.deepcopy(native)
        value["controls"][field] = 100
        invalid.append(value)
    for parent, field, wrong in [
        (None, "descriptor_version", 1), (None, "descriptor_version", 2),
        (None, "descriptor_version", 3), (None, "descriptor_version", 4),
        (None, "descriptor_version", 5), (None, "descriptor_version", 6),
        (None, "descriptor_version", 8),
        (None, "descriptor_version", "7"), (None, "descriptor_version", None),
        (None, "application", None),
        (None, "target_profile", "b4-ap-core/1"), (None, "profile_status", "full"),
        (None, "core_profile_frozen", True),
        ("execution", "native_callbacks", False),
        ("execution", "approval_service_mode", "disabled"),
        ("execution", "approval_service_mode", "volatile_fixture"),
        ("execution", "mode", "distributed"), ("execution", "registration", "dynamic"),
        ("store", "durable_runtime", False), ("store", "durable_approvals", False),
        ("store", "durable_approvals", None), ("store", "kind", "private-backend"),
        ("readiness", "response", "checks"),
        ("readiness", "aggregation", "store_readiness_and_no_owned_worker_fault"),
        ("readiness", "checks", ["store_readiness", "owned_worker_fault"]),
        ("readiness", "checks", ["serving_admission", "owned_worker_fault", "store_readiness"]),
    ]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])[field] = wrong
        invalid.append(value)
    for parent, field in [(None, "runtime_version"), ("application", "id"), ("application", "revision")]:
        for wrong in ["", "bad\nlabel", "bad\u0085label", "x" * 257]:
            value = copy.deepcopy(native)
            (value if parent is None else value[parent])[field] = wrong
            invalid.append(value)
    for length in [1, 256]:
        value = copy.deepcopy(native)
        value["runtime_version"] = "r" * length
        value["application"] = {"id": "a" * length, "revision": "v" * length}
        validator.validate(value)
    source = repo_file("crates/b4-contract/tests/capabilities_v7.rs").read_text()
    for name in [
        "v7_round_trips_only_withheld_ready_and_entered_permission_cancellation",
        "opt_in_preserves_every_valid_v6_field_and_the_original_base",
        "v7_wire_is_closed_required_typed_and_rejects_duplicate_declarations",
        "v7_retains_v6_validation_of_features_limits_readiness_and_controls",
        "factory_and_public_validation_reject_invalid_assemblies_without_repair",
        "v1_through_v6_fixture_bytes_and_round_trips_stay_frozen_and_reject_v7",
    ]:
        require(re.search(rf"fn\s+{re.escape(name)}\s*\(", source),
                f"Missing named V7 metadata test: {name}")
    for value in invalid:
        require(not validator.is_valid(value), f"V7 accepted invalid withheld/Ready/entered permission cancellation metadata: {value!r}")
    return len(invalid)


def check_permission_settled_thread_deletion_capability_schema(validator):
    """Authored V8 settled deletion metadata; not execution evidence."""
    native = read_json(CONTRACTS / "examples" / "capabilities-v8-permission-settled-thread-deletion.json")
    validator.validate(native)
    expected = read_json(CONTRACTS / "examples" / "capabilities-v7-permission-withheld-cancellation.json")
    expected["descriptor_version"] = 8
    expected["features"]["durable_permission_settled_thread_deletion"] = True
    require(native == expected, "V8 example must preserve the V7 base except version and settled deletion")
    invalid = []
    for parent in [None, "application", "execution", "features", "store", "readiness", "limits", "controls"]:
        original = native if parent is None else native[parent]
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])["routes"] = ["private-route"]
        invalid.append(value)
        for field in original:
            value = copy.deepcopy(native)
            del (value if parent is None else value[parent])[field]
            invalid.append(value)
    value = copy.deepcopy(native)
    value["features"]["durable_permission_pre_entry_cancellation"] = True
    invalid.append(value)
    for field, expected in native["features"].items():
        for wrong in [not expected, "true", None, 1]:
            value = copy.deepcopy(native)
            value["features"][field] = wrong
            invalid.append(value)
    for field in ["active_runs", "concurrent_tasks", "concurrent_tasks_per_execution"]:
        for wrong in [0, 2, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
    for field, maximum in {
        "viewer_queue_frames": 512, "heartbeat_ms": 60000,
        "input_bytes": 65536, "output_bytes": 65536, "max_attached_viewers": 64,
        "turn_buffer_bytes": 8388608, "turn_buffer_frames": 4096,
        "viewer_queue_bytes": 8388608, "attachment_snapshot_bytes": 8388608,
        "approval_ttl_ms": 86400000,
    }.items():
        for wrong in [0, maximum + 1, -1, 1.5, None]:
            value = copy.deepcopy(native)
            value["limits"][field] = wrong
            invalid.append(value)
        for valid in [1, maximum]:
            value = copy.deepcopy(native)
            value["limits"][field] = valid
            validator.validate(value)
    for field in ["body_bytes", "json_depth", "program_routes", "program_tasks",
                  "program_attempts", "completion_attempts", "task_failure_bytes",
                  "script_chunks", "script_delay_ms", "approval_bytes"]:
        value = copy.deepcopy(native)
        value["limits"][field] += 1
        invalid.append(value)
    for field in native["controls"]:
        value = copy.deepcopy(native)
        value["controls"][field] = 100
        invalid.append(value)
    for parent, field, wrong in [
        (None, "descriptor_version", 1), (None, "descriptor_version", 2),
        (None, "descriptor_version", 3), (None, "descriptor_version", 4),
        (None, "descriptor_version", 5), (None, "descriptor_version", 6),
        (None, "descriptor_version", 7), (None, "descriptor_version", 9),
        (None, "descriptor_version", "8"), (None, "descriptor_version", None),
        (None, "application", None),
        (None, "target_profile", "b4-ap-core/1"), (None, "profile_status", "full"),
        (None, "core_profile_frozen", True),
        ("execution", "native_callbacks", False),
        ("execution", "approval_service_mode", "disabled"),
        ("execution", "approval_service_mode", "volatile_fixture"),
        ("execution", "mode", "distributed"), ("execution", "registration", "dynamic"),
        ("store", "durable_runtime", False), ("store", "durable_approvals", False),
        ("store", "durable_approvals", None), ("store", "kind", "private-backend"),
        ("readiness", "response", "checks"),
        ("readiness", "aggregation", "store_readiness_and_no_owned_worker_fault"),
        ("readiness", "checks", ["store_readiness", "owned_worker_fault"]),
        ("readiness", "checks", ["serving_admission", "owned_worker_fault", "store_readiness"]),
    ]:
        value = copy.deepcopy(native)
        (value if parent is None else value[parent])[field] = wrong
        invalid.append(value)
    for parent, field in [(None, "runtime_version"), ("application", "id"), ("application", "revision")]:
        for wrong in ["", "bad\nlabel", "bad\u0085label", "x" * 257]:
            value = copy.deepcopy(native)
            (value if parent is None else value[parent])[field] = wrong
            invalid.append(value)
    for length in [1, 256]:
        value = copy.deepcopy(native)
        value["runtime_version"] = "r" * length
        value["application"] = {"id": "a" * length, "revision": "v" * length}
        validator.validate(value)
    source = repo_file("crates/b4-contract/tests/capabilities_v8.rs").read_text()
    for name in [
        "v8_inherits_every_v7_field_and_adds_only_the_narrow_deletion_declaration",
        "v8_wire_is_closed_required_typed_and_duplicate_free",
        "v8_retains_inherited_wire_validation",
        "constructors_and_public_validation_do_not_repair_invalid_inherited_assemblies",
    ]:
        require(re.search(rf"fn\s+{re.escape(name)}\s*\(", source),
                f"Missing named V8 metadata test: {name}")
    for value in invalid:
        require(not validator.is_valid(value), f"V8 accepted invalid settled permission-thread deletion metadata: {value!r}")
    return len(invalid)


def check_contracts():
    require(check_inspection_reports.main() == 0,
            "Invalid stopped-inspection operator contract")
    require(check_diagnostic_batches.main() == 0,
            "Invalid request-diagnostic embedding contract")
    schema_paths = sorted((CONTRACTS / "schemas").glob("*.json"))
    require(bool(schema_paths), "No schema artifacts found")
    schemas = {path.stem: read_json(path) for path in schema_paths}
    registry = Registry()
    for path in schema_paths:
        schema = schemas[path.stem]
        Draft202012Validator.check_schema(schema)
        registry = registry.with_resource(path.as_uri(), Resource.from_contents(schema))

    def validator(name):
        require(name in schemas, f"Missing schema: {name}")
        uri = (CONTRACTS / "schemas" / f"{name}.json").as_uri()
        return Draft202012Validator({"$ref": uri}, registry=registry)

    manifest = read_json(CONTRACTS / "examples" / "manifest.json")
    require(manifest["evidence_kind"] == "NATIVE", "Examples must be authored native inputs")
    require(manifest["reference_execution"] is False, "Examples must not claim reference execution")
    for item in manifest["examples"]:
        validator(Path(item["schema"]).stem).validate(
            read_json(CONTRACTS / "examples" / item["file"])
        )

    additional_examples = [
        {"schema": "capabilities-v7",
         "file": "capabilities-v7-permission-withheld-cancellation.json"},
        {"schema": "capabilities-v8",
         "file": "capabilities-v8-permission-settled-thread-deletion.json"},
    ]
    for item in additional_examples:
        validator(item["schema"]).validate(read_json(CONTRACTS / "examples" / item["file"]))

    capability_rejections = check_capability_schema(validator("capabilities"))
    capability_rejections += check_capability_schema(validator("capabilities-v2"), version=2)
    capability_rejections += check_permission_reuse_capability_schema(validator("capabilities-v3"))
    capability_rejections += check_permission_approvals_capability_schema(validator("capabilities-v4"))
    capability_rejections += check_permission_entered_cancellation_capability_schema(validator("capabilities-v5"))
    capability_rejections += check_permission_ready_cancellation_capability_schema(validator("capabilities-v6"))
    capability_rejections += check_permission_withheld_cancellation_capability_schema(validator("capabilities-v7"))
    capability_rejections += check_permission_settled_thread_deletion_capability_schema(validator("capabilities-v8"))

    run_invalid = [
        {}, {"route": None}, {"route": "/test"},
        {"route": "/test#agent", "owner_generation": 3}, [], None,
        {"route": "/test#agent#workflow"}, {"route": "test#agent"},
        {"route": "/test#agent\n"},
    ]
    resume_invalid = [
        {"interrupt_id": "i-1", "decision": "once"}, {"resume": []},
        {"route": "/test#agent", "resume": []},
        {"route": "/test#agent", "resume": [], "execution_id": "e-1"},
    ]
    invalid_entries = [
        {"interruptId": "i-1", "status": "resolved"},
        {"interruptId": "i-1", "status": "resolved", "payload": None},
        {"interruptId": "i-1", "status": "resolved", "payload": "yes"},
        {"interruptId": "i-1", "status": "cancelled", "payload": "deny"},
        {"interruptId": "i-1", "status": "cancelled", "payload": None},
        {"interruptId": "i-1", "status": "resolved", "payload": "once", "grant": None},
        {"interruptId": "i-1", "status": "resolved", "payload": "once", "owner": 1},
    ]
    resume_invalid.extend(
        {"route": "/test#agent", "resume": [entry]} for entry in invalid_entries
    )
    rejection_count = capability_rejections
    for name, cases in [("run-request", run_invalid), ("resume-request", resume_invalid)]:
        for value in cases:
            require(not validator(name).is_valid(value), f"{name} accepted {value!r}")
            rejection_count += 1
    for input_value in [None, "snow\n雪", 42, True, [1, 2], {"x": 1}]:
        validator("run-request").validate({"route": "/test#agent", "input": input_value})
    for decision in ["once", "always", "deny"]:
        validator("resume-request").validate({
            "route": "/test#agent",
            "resume": [{"interruptId": "i-1", "status": "resolved",
                        "payload": decision, "grant": "b4ag_unissued_example"}],
        })
    validator("resume-request").validate({
        "route": "/test#agent", "resume": [{"interruptId": "i-1", "status": "cancelled"}]
    })

    for name in ["opaque-id", "grant"]:
        require(schemas[name]["x-max-utf8-bytes"] == 256, "Native byte bound changed")
        for value in ["", "newline\n", "nul\0", "delete\x7f", "c1\x85", "x" * 257]:
            require(not validator(name).is_valid(value), f"{name} accepted {value!r}")
            rejection_count += 1
    validator("opaque-id").validate("é" * 128)
    # Standard JSON Schema maxLength counts code points, unlike native UTF-8 bytes.
    # Native constructor/deserializer rejection is independently tested in Rust.
    validator("opaque-id").validate("é" * 129)
    require(not validator("sse-done").is_valid({}), "Native done must contain output")
    rejection_count += 1
    live = read_json(CONTRACTS / "examples" / "sse-state-live.json")
    malformed_states = [dict(live, turn=None), dict(live, turn=None, turn_truncated=False),
                        dict(live, turn=[{"event": "unqualified", "data": None}]),
                        dict(live, turn=[{"event": "chunk"}]), dict(live, values=None),
                        dict(live, live=False), dict(live, untrusted_control=True)]
    for value in malformed_states:
        require(not validator("sse-state").is_valid(value), "Native attachment accepted malformed state")
        rejection_count += 1
    for value in [{}, {"reason":"unknown"}, {"reason":"capacity", "thread_id":"private"}]:
        require(not validator("sse-detached").is_valid(value), "Native detachment accepted malformed payload")
        rejection_count += 1
    thread = read_json(CONTRACTS / "examples" / "thread.json")
    thread["created_at"] = "unvalidated DTO timestamp string"
    validator("thread").validate(thread)

    snapshot = read_json(CONTRACTS / "examples" / "internal-execution-snapshot.json")
    for value in [snapshot, dict(snapshot, output=None)]:
        validator("internal-execution-snapshot").validate(value)
    for pointer in ["", "/definition", "/tasks/0", "/tasks/0/spec", "/tasks/0/interrupt"]:
        value = copy.deepcopy(snapshot)
        node = value
        if pointer:
            for key in pointer.lstrip("/").split("/"):
                node = node[int(key)] if isinstance(node, list) else node[key]
        node["untrusted_control"] = True
        require(not validator("internal-execution-snapshot").is_valid(value),
                f"Snapshot accepted unknown field at {pointer}")
        rejection_count += 1
    bad_snapshots = [dict(snapshot, schema_version=2)]
    for field in ["turn_history", "attempt_history"]:
        value = copy.deepcopy(snapshot)
        del value[field]
        bad_snapshots.append(value)
    for value in bad_snapshots:
        require(not validator("internal-execution-snapshot").is_valid(value),
                "Snapshot accepted unsupported version or missing identity history")
        rejection_count += 1

    failed = copy.deepcopy(snapshot)
    failed["status"] = "failed"
    failed["failure"] = "fixture execution failure"
    failed["tasks"][0].update(status="failed", attempt_id="failed-attempt",
                               result=None, failure="fixture execution failure")
    validator("internal-execution-snapshot").validate(failed)
    malformed_failures = []
    for reason in [None, "", "x" * 1025]:
        value = copy.deepcopy(failed)
        value["tasks"][0]["failure"] = reason
        malformed_failures.append(value)
    value = copy.deepcopy(failed)
    del value["tasks"][0]["failure"]
    malformed_failures.append(value)
    value = copy.deepcopy(failed)
    value["tasks"][0]["attempt_id"] = None
    malformed_failures.append(value)
    value = copy.deepcopy(failed)
    value["tasks"][0]["result"] = {"output": {"present": True, "value": None}, "writes": []}
    malformed_failures.append(value)
    value = copy.deepcopy(snapshot)
    value["tasks"][0]["failure"] = "unexpected cause"
    malformed_failures.append(value)
    for value in malformed_failures:
        require(not validator("internal-execution-snapshot").is_valid(value),
                "Snapshot schema accepted a malformed failure record")
        rejection_count += 1
    # JSON Schema counts code points; the Rust kernel enforces the byte bound.
    failed["tasks"][0]["failure"] = "é" * 513
    validator("internal-execution-snapshot").validate(failed)

    openapi = yaml.safe_load((CONTRACTS / "openapi.yaml").read_text())
    reference_count = 0

    def check_refs(value, base, document):
        nonlocal reference_count
        if isinstance(value, dict):
            if "$ref" in value:
                reference_count += 1
                target = value["$ref"]
                filename, separator, fragment = target.partition("#")
                if filename:
                    path = (base / filename).resolve()
                    require(path.is_relative_to(REPO) and path.is_file(),
                            f"Missing/outside-repository local reference: {target}")
                    referenced = read_json(path) if path.suffix == ".json" else yaml.safe_load(path.read_text())
                else:
                    referenced = document
                if separator and fragment:
                    node = referenced
                    for part in fragment.lstrip("/").split("/"):
                        key = part.replace("~1", "/").replace("~0", "~")
                        node = node[int(key)] if isinstance(node, list) else node[key]
            for item in value.values():
                check_refs(item, base, document)
        elif isinstance(value, list):
            for item in value:
                check_refs(item, base, document)

    check_refs(openapi, CONTRACTS, openapi)
    for schema in schemas.values():
        check_refs(schema, CONTRACTS / "schemas", schema)
    all_operations = [
        operation for path in openapi["paths"].values()
        for method, operation in path.items()
        if method in ["get", "post", "put", "patch", "delete"]
    ]
    operations = [operation for operation in all_operations
                  if operation.get("x-operation-id", "").startswith("AP-")]
    operational = [operation for operation in all_operations if operation not in operations]
    require(len(operations) == 10
            and {item["x-operation-id"] for item in operations} == {f"AP-{n:03d}" for n in range(1, 11)},
            "Expected ten separately identified core draft operations")
    require(len(operational) == 1
            and operational[0]["operationId"] == "getCapabilities"
            and operational[0]["x-operation-id"] == "NATIVE-OP-001"
            and operational[0]["x-implementation-status"] == "native-subset"
            and openapi["paths"]["/b4/capabilities"]["get"] == operational[0],
            "Expected separate protected native capability operation")
    implemented = {"createThread", "getThread", "getThreadState", "getPendingInterrupts",
                   "runWait", "runStream", "resumeExecution", "deleteThread", "cancelTurn", "attachStream"}
    require(all(operation["x-implementation-status"]
                == ("scripted-subset" if operation["operationId"] in implemented else "planned")
                for operation in operations), "OpenAPI implemented subset differs from ten host operations")
    require({operation["operationId"] for operation in operations
             if operation["x-implementation-status"] == "scripted-subset"} == implemented,
            "OpenAPI ten-operation inventory differs")
    require(openapi["x-milestone-M0"] == "open"
            and openapi["x-transport-limits"] == {"body_bytes": 65536, "json_depth": 64,
                                               "implementation_status": "scripted-subset"},
            "OpenAPI scope or transport limits changed")

    requirements = yaml.safe_load((REPO / "spec/runtime/requirements.yaml").read_text())
    check_core_release_traceability(requirements)
    characterization = requirements["characterization"]
    require([item["id"] for item in characterization] == [f"CP-{n:03d}" for n in range(1, 11)],
            "Characterization inventory is incomplete")
    require([item["status"] for item in characterization]
            == ["partial"] * 9 + ["unresolved"],
            "Selected workflow/grant/lifecycle evidence is partial; characterization must remain incomplete")
    recorded_cases = check_reference_recordings()
    require(all(repo_file(item["evidence"]).name == "workflow-observations.json"
                and bool(item["remaining"]) for item in characterization[:4]),
            "Partial characterization needs scoped retained evidence and remaining work")
    grant_check = subprocess.run(["node", str(REPO / "scripts/check_grant_recordings.mjs")],
                                 check=True, capture_output=True, text=True)
    grant_evidence = json.loads(grant_check.stdout)
    require(grant_evidence == {"cases_each": 39, "ordered_matches": 37,
                              "pending_set_matches": 39, "runtime_artifacts": 385},
            "Scoped grant evidence integrity differs")
    require(all(repo_file(characterization[index]["evidence"]).name == "grant-observations.json"
                and bool(characterization[index]["remaining"]) for index in [4, 8]),
            "Partial grant characterization needs retained evidence and remaining work")
    lifecycle_check = subprocess.run(["node", str(REPO / "scripts/check_lifecycle_recordings.mjs")],
                                     check=True, capture_output=True, text=True)
    lifecycle_evidence = json.loads(lifecycle_check.stdout)
    require(lifecycle_evidence == {"cases_each": 77, "assertions_each": 11,
                                  "ordered_matches": 77, "fixture_files": 10,
                                  "runtime_artifacts": 385, "incomplete_captures_each": 1},
            "Scoped lifecycle evidence integrity differs")
    require(all(repo_file(characterization[index]["evidence"]).name == "lifecycle-observations.json"
                and bool(characterization[index]["remaining"]) for index in [5, 6, 7]),
            "Partial lifecycle characterization needs retained evidence and remaining work")
    require(requirements["milestones"]["M0"]["status"] == "open", "M0 must remain open")
    local_requirements = (requirements["foundation_requirements"]
                          + requirements.get("transactional_requirements", [])
                          + requirements.get("http_requirements", [])
                          + requirements.get("failure_requirements", [])
                          + requirements.get("approval_requirements", []))
    local_requirements += requirements.get("http_approval_requirements", [])
    local_requirements += requirements.get("recovery_requirements", [])
    local_requirements += requirements.get("deletion_requirements", [])
    local_requirements += requirements.get("cancellation_requirements", [])
    local_requirements += requirements.get("attachment_requirements", [])
    local_requirements += requirements.get("shared_driver_requirements", [])
    local_requirements += requirements.get("native_application_requirements", [])
    local_requirements += requirements.get("supervised_drain_requirements", [])
    local_requirements += requirements.get("sqlite_backup_requirements", [])
    local_requirements += requirements.get("native_launcher_requirements", [])
    local_requirements += requirements.get("linux_container_requirements", [])
    local_requirements += requirements.get("stopped_inspection_requirements", [])
    local_requirements += requirements.get("request_diagnostic_requirements", [])
    client_requirements = requirements.get("unchanged_ap_client_requirements", [])
    native_requirements = requirements.get("native_cooperative_cancellation_requirements", [])
    discovery_requirements = requirements.get("unfinished_discovery_requirements", [])
    startup_requirements = requirements.get("startup_recovery_preparation_requirements", [])
    history_requirements = requirements.get("historical_native_definitions_requirements", [])
    confinement_requirements = requirements.get("prior_owner_confinement_requirements", [])
    require(isinstance(confinement_requirements, list),
            "Prior-owner confinement requirements must be a list")
    restart_requirements = requirements.get("process_startup_restart_requirements", [])
    require(isinstance(restart_requirements, list),
            "Process startup restart requirements must be a list")
    accepted_once_requirements = requirements.get("accepted_once_startup_requirements", [])
    require(isinstance(accepted_once_requirements, list),
            "Accepted Once startup requirements must be a list")
    startup_confinement_requirements = requirements.get("startup_confinement_requirements", [])
    require(isinstance(startup_confinement_requirements, list),
            "Startup confinement requirements must be a list")
    cli_startup_requirements = requirements.get("cli_startup_recovery_requirements", [])
    require(isinstance(cli_startup_requirements, list),
            "CLI startup recovery requirements must be a list")
    named_requirements = (local_requirements + client_requirements + native_requirements
                          + discovery_requirements + startup_requirements + history_requirements
                          + confinement_requirements + restart_requirements
                          + accepted_once_requirements + startup_confinement_requirements)
    named_requirements += cli_startup_requirements
    require(len({item["id"] for item in named_requirements}) == len(named_requirements),
            "Duplicate named local requirement IDs")
    if "prior_owner_confinement_requirements" in requirements:
        require(sorted(item["id"] for item in confinement_requirements)
                == [f"CONFINE-{number:03d}" for number in range(1, 18)],
                "Prior-owner confinement scoped case inventory is incomplete")
        for item in confinement_requirements:
            require(item.get("classification") == "NATIVE"
                    and item.get("source") == "spec/runtime/prior-owner-confinement-design.md"
                    and item.get("component") == "prior-owner-confinement"
                    and item.get("release_scope")
                    == "explicit_bounded_native_prior_owner_confinement_only",
                    "Prior-owner confinement scope is not the explicit bounded native slice")
            require((item.get("implementation_status"), item.get("verification_status")) in {
                ("pending", "not_run"), ("implemented", "not_run"),
                ("implemented", "passed_bounded_prior_owner_native_macos_linux_scope"),
            }, "Prior-owner confinement status exceeds recorded native scope")
            tests = item.get("acceptance_tests")
            require(isinstance(tests, list) and tests and all(
                isinstance(test, dict) and isinstance(test.get("file"), str)
                and bool(test["file"]) and isinstance(test.get("name"), str)
                and bool(test["name"]) for test in tests),
                "Prior-owner confinement must name scoped acceptance tests")
            if item["verification_status"] == "passed_bounded_prior_owner_native_macos_linux_scope":
                require(isinstance(item.get("evidence"), str) and bool(item["evidence"]),
                        "Prior-owner confinement passed metadata needs recorded evidence")
                require(repo_file(item["evidence"]).suffix == ".json",
                        "Prior-owner confinement passed metadata needs recorded evidence JSON")
    if "process_startup_restart_requirements" in requirements:
        required_restart_cases = {
            "RESTART-001": "process_startup_park_preserves_once_and_sibling_without_replay",
            "RESTART-002": "process_startup_unsafe_effect_is_blocked_before_readiness_without_replay",
        }
        require(sorted(item["id"] for item in restart_requirements)
                == sorted(required_restart_cases),
                "Process startup restart scoped case inventory is incomplete")
        for item in restart_requirements:
            require(item.get("classification") == "NATIVE"
                    and item.get("source") == "spec/runtime/startup-process-restart-design.md"
                    and item.get("component") == "process-startup-restart"
                    and item.get("release_scope")
                    == "bounded_two_lane_process_startup_restart_only",
                    "Process startup restart scope is not the bounded two-lane process slice")
            require((item.get("implementation_status"), item.get("verification_status")) in {
                ("pending", "not_run"), ("implemented", "not_run"),
                ("implemented", "passed_bounded_two_lane_native_macos_linux_scope"),
            }, "Process startup restart status exceeds recorded two-lane native scope")
            require(item.get("acceptance_tests") == [{
                "file": "crates/b4-http/tests/startup_restart.rs",
                "name": required_restart_cases[item["id"]],
            }], "Process startup restart must name its exact credited parent acceptance case")
            if item["verification_status"] == "passed_bounded_two_lane_native_macos_linux_scope":
                require(isinstance(item.get("evidence"), str) and bool(item["evidence"]),
                        "Process startup restart passed metadata needs recorded evidence")
                require(repo_file(item["evidence"]).suffix == ".json",
                        "Process startup restart passed metadata needs recorded evidence JSON")
    if "accepted_once_startup_requirements" in requirements:
        required_accepted_once_cases = {
            "ONCE-STARTUP-001":
                "process_startup_accepted_once_after_expiry_preserves_intent_and_sibling",
        }
        require(sorted(item["id"] for item in accepted_once_requirements)
                == sorted(required_accepted_once_cases),
                "Accepted Once startup scoped case inventory is incomplete")
        for item in accepted_once_requirements:
            require(item.get("classification") == "NATIVE"
                    and item.get("source") == "spec/runtime/startup-accepted-once-design.md"
                    and item.get("component") == "startup-accepted-once"
                    and item.get("release_scope")
                    == "bounded_accepted_once_process_startup_only",
                    "Accepted Once startup scope is not the bounded accepted-Once process slice")
            require((item.get("implementation_status"), item.get("verification_status")) in {
                ("pending", "not_run"), ("implemented", "not_run"),
                ("implemented", "passed_bounded_accepted_once_native_macos_linux_scope"),
            }, "Accepted Once startup status exceeds recorded native scope")
            require(item.get("acceptance_tests") == [{
                "file": "crates/b4-http/tests/startup_accepted_once.rs",
                "name": required_accepted_once_cases[item["id"]],
            }], "Accepted Once startup must name its exact credited parent acceptance case")
            if item["verification_status"] == "passed_bounded_accepted_once_native_macos_linux_scope":
                require(isinstance(item.get("evidence"), str) and bool(item["evidence"]),
                        "Accepted Once startup passed metadata needs recorded evidence")
                require(repo_file(item["evidence"]).suffix == ".json",
                        "Accepted Once startup passed metadata needs recorded evidence JSON")
    if "startup_confinement_requirements" in requirements:
        require([item["id"] for item in startup_confinement_requirements]
                == ["STARTUP-CONFINE-001"],
                "Startup confinement scoped case inventory is incomplete")
        required_startup_confinement_cases = [
            ('crates/b4-http/src/startup/confinement/tests.rs',
             'borrowed_start_future_has_no_framework_alias_before_poll'),
            ('crates/b4-http/src/startup/confinement/tests.rs',
             'start_limits_and_single_attempt_precede_argument_clones'),
            ('crates/b4-http/src/startup/confinement/tests.rs',
             'startup_job_requires_whole_original_and_exact_principal'),
            ('crates/b4-http/src/startup/confinement/tests.rs',
             'startup_arguments_dispose_two_panicking_handles_under_one_anchor'),
            ('crates/b4-http/src/startup/confinement/tests.rs',
             'startup_progress_counts_verified_receipt_but_not_ambiguous_commit'),
            ('crates/b4-http/src/startup/confinement/tests.rs',
             'final_startup_capture_drop_cannot_activate_or_escape_ownership'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'opt_in_missing_exact_definition_confines_original_prior_owner'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'opt_in_denied_recover_policy_preserves_accepted_once'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'default_preparation_refusal_remains_unchanged'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'unresolved_principal_and_denied_confinement_authority_refuse'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'audited_prior_block_counts_without_readoption'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'available_historical_definition_uses_original_typed_recovery'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'unsupported_and_changed_custom_store_refuse_before_write'),
            ('crates/b4-http/tests/startup_confinement.rs',
             'scan_limit_preserves_two_confirmed_blocks_and_stops_third_candidate'),
            ('crates/b4-http/tests/startup_confinement_tcp.rs',
             'preparing_listener_stays_closed_through_confinement_authority_and_cleanup'),
            ('crates/b4-http/tests/startup_confinement_tcp.rs',
             'mixed_startup_opens_after_confinement_before_runnable_callback_settlement'),
        ]
        for item in startup_confinement_requirements:
            require(item.get("classification") == "NATIVE"
                    and item.get("source") == "spec/runtime/startup-confinement-design.md"
                    and item.get("component") == "startup-confinement"
                    and item.get("release_scope")
                    == "explicit_bounded_opt_in_native_startup_confinement_only",
                    "Startup confinement scope is not the explicit bounded opt-in native slice")
            require((item.get("implementation_status"), item.get("verification_status")) in {
                ("pending", "not_run"), ("implemented", "not_run"),
                ("implemented", "passed_bounded_startup_confinement_native_macos_linux_scope"),
            }, "Startup confinement status exceeds recorded native scope")
            require(item.get("acceptance_tests") == [
                {"file": file, "name": name}
                for file, name in required_startup_confinement_cases
            ], "Startup confinement must name its exact sixteen acceptance cases")
            if item["verification_status"] == "passed_bounded_startup_confinement_native_macos_linux_scope":
                require(item.get("evidence")
                        == "conformance/expected/startup-confinement-verification.json",
                        "Startup confinement passed metadata needs its exact recorded evidence JSON")
                require(repo_file(item["evidence"]).suffix == ".json",
                        "Startup confinement passed metadata needs recorded evidence JSON")
    if "cli_startup_recovery_requirements" in requirements:
        require([item["id"] for item in cli_startup_requirements] == ["CLI-RECOVERY-001"],
                "CLI startup recovery scoped case inventory is incomplete")
        required_cli_startup_cases = [
            ('crates/b4-cli/src/config_recovery_tests.rs',
             'explicit_recovery_selects_distinct_serve_command_in_both_option_orders'),
            ('crates/b4-cli/src/config_recovery_tests.rs',
             'recovery_config_check_accepts_both_orders_without_a_serve_command'),
            ('crates/b4-cli/src/config_recovery_tests.rs',
             'recovery_grammar_refuses_malformed_options_before_environment_access'),
            ('crates/b4-cli/src/config_recovery_tests.rs',
             'recovery_selection_preserves_defaults_and_redacts_private_configuration'),
            ('crates/b4-cli/src/process_startup_tests.rs',
             'startup_resolver_accepts_only_the_fixed_trusted_demo_tuple'),
            ('crates/b4-cli/src/process_startup_tests.rs',
             'signal_during_held_recovery_keeps_ownership_until_normal_return'),
            ('crates/b4-cli/src/process_startup_tests.rs',
             'opaque_lost_server_during_preparation_keeps_payload_and_executor_owned'),
            ('crates/b4-cli/src/process_startup_tests.rs',
             'readiness_precedes_settlement_of_a_legally_accepted_recovered_callback'),
            ('crates/b4-cli/tests/cli_startup_recovery.rs',
             'explicit_cli_restart_adopts_the_original_park_and_resumes_only_approved_work'),
            ('crates/b4-cli/tests/cli_startup_missing_definition.rs',
             'explicit_cli_missing_historical_definition_refuses_after_normal_retirement'),
            ('crates/b4-cli/tests/cli_startup_controls.rs',
             'recovery_config_check_both_orders_validate_without_storage_or_listener_acquisition'),
            ('crates/b4-cli/tests/cli_startup_controls.rs',
             'explicit_empty_store_startup_preserves_empty_work_and_releases_its_real_owner'),
        ]
        for item in cli_startup_requirements:
            require(item.get("classification") == "NATIVE"
                    and item.get("source") == "spec/runtime/cli-startup-recovery-design.md"
                    and item.get("component") == "cli-startup-recovery"
                    and item.get("release_scope")
                    == "explicit_fixed_native_demo_cli_startup_recovery_only",
                    "CLI startup recovery scope is not the explicit fixed native demo slice")
            require((item.get("implementation_status"), item.get("verification_status")) in {
                ("pending", "not_run"), ("implemented", "not_run"),
                ("implemented", "passed_explicit_native_demo_cli_macos_linux_scope"),
            }, "CLI startup recovery status exceeds recorded native demo scope")
            require(item.get("acceptance_tests") == [
                {"file": file, "name": name} for file, name in required_cli_startup_cases
            ], "CLI startup recovery must name its exact twelve acceptance cases")
            if item["verification_status"] == "passed_explicit_native_demo_cli_macos_linux_scope":
                require(item.get("evidence")
                        == "conformance/expected/cli-startup-recovery-verification.json",
                        "CLI startup recovery passed metadata needs its exact recorded evidence JSON")
                require(repo_file(item["evidence"]).suffix == ".json",
                        "CLI startup recovery passed metadata needs recorded evidence JSON")
    for requirement in named_requirements:
        for test in requirement["acceptance_tests"]:
            if "file" in test:
                path = REPO / test["file"]
                require(path.is_file(), f"Missing test file: {path}")
                declaration = {".py": "def", ".mjs": "function"}.get(path.suffix, "fn")
                require(re.search(rf"{declaration}\s+{re.escape(test['name'])}\s*\(", path.read_text()),
                        f"Missing named local test: {test}")
    print(f"Validated {len(schemas)} Draft202012 schemas, {len(manifest['examples']) + len(additional_examples)} authored "
          f"examples, {rejection_count} rejection cases, {reference_count} local refs and ten "
          "core operations (ten scripted-subset, none planned), plus one native operational endpoint.")
    print(f"Evidence integrity: two retained {recorded_cases}-case reference recordings, "
          "matching normalized projections and 414 built-artifact inventory entries.")
    print("Grant evidence: two 39-case recordings, 37 ordered projections and 39 with only two "
          "pending arrays compared as sets; 385 captured build artifacts.")
    print("Lifecycle evidence: two 77-case recordings with 11 assertions each, 77 limited ordered "
          "matches and one explicitly incomplete viewer cut per capture; reused 385 build artifacts.")
    print(f"Traceability: {len(local_requirements)} named local requirements; eighteen native-qualified release scenarios (T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017, T-018), 38 pending; "
          "CP-001–009 partial, CP-010 unresolved; M0 open; core profile open; COMPAT unverified.")
    print(f"Unchanged client corpus: {len(client_requirements)} named local requirements; "
          "selected original caller scope only.")
    print(f"Native cooperative cancellation: {len(native_requirements)} named local requirements; "
          "controlled native callback scope only.")
    print(f"Unfinished discovery: {len(discovery_requirements)} named local requirements; "
          "trusted read-only discovery scope only.")
    print(f"Startup recovery preparation: {len(startup_requirements)} named local requirements; "
          "opt-in bounded native preparation scope only.")
    print(f"Historical native Definitions: {len(history_requirements)} named local requirements; "
          "explicit approval-free retained native scope only.")
    print(f"Prior-owner confinement: {len(confinement_requirements)} named local requirements; "
          "explicit bounded native scope only; metadata is not runtime evidence.")
    print(f"Process startup restart: {len(restart_requirements)} named local requirements; "
          "bounded two-lane process scope only; metadata is not runtime evidence.")
    print(f"Accepted Once startup: {len(accepted_once_requirements)} named local requirements; "
          "bounded accepted-Once process scope only; metadata is not runtime evidence.")
    print(f"Startup confinement: {len(startup_confinement_requirements)} named local requirements; "
          "explicit bounded opt-in native scope only; metadata is not runtime evidence.")
    print(f"CLI startup recovery: {len(cli_startup_requirements)} named local requirements; "
          "explicit fixed native demo scope only; metadata is not runtime evidence.")
    print("Schema validation and named-test existence do not assert fresh test execution or full HTTP conformance.")
    print("Native byte bounds need x-max-utf8-bytes enforcement; thread timestamp strings have no format validation.")


if __name__ == "__main__":
    check_contracts()
