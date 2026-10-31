#!/usr/bin/env python3
"""Metadata guard tests only; authored temporary fixtures carry no runtime credit."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_contracts


TEST_FILE = "crates/b4-http/tests/native_tcp.rs"
TEST_NAME = "release_t001_create_read_thread_round_trips_metadata_and_timestamps_after_reopen"
SCOPE = "native_create_read_thread_tcp_sqlite"
RECEIPT = "conformance/release/t001-thread-roundtrip/qualification.json"
T002_FILE = "crates/b4-http/tests/thread_state_tcp.rs"
T002_NAME = "release_t002_empty_thread_state_stays_unavailable_after_reopen"
T002_SCOPE = "native_empty_thread_state_tcp_sqlite"
T002_RECEIPT = "conformance/release/t002-empty-thread-state/qualification.json"
T003_FILE = "crates/b4-http/tests/thread_admission_tcp.rs"
T003_NAME = "release_t003_implicit_thread_creation_requires_authorization"
T003_SCOPE = "native_authorized_implicit_thread_tcp_sqlite"
T003_RECEIPT = "conformance/release/t003-authorized-implicit-thread/qualification.json"
T004_FILE = "crates/b4-http/tests/thread_route_tcp.rs"
T004_NAME = "release_t004_unknown_and_missing_kind_routes_do_not_execute"
T004_SCOPE = "native_invalid_route_rejection_tcp_sqlite"
T004_RECEIPT = "conformance/release/t004-invalid-routes/qualification.json"

T005_FILE = "crates/b4-http/tests/thread_json_tcp.rs"
T005_NAME = "release_t005_malformed_json_uses_ap_error_without_execution"
T005_SCOPE = "native_malformed_json_error_tcp_sqlite"
T005_RECEIPT = "conformance/release/t005-malformed-json/qualification.json"

T006_FILE = "crates/b4-http/tests/input_shape_tcp.rs"
T006_NAME = "release_t006_omitted_null_and_false_inputs_round_trip"
T006_SCOPE = "native_input_shape_preservation_tcp_sqlite"
T006_RECEIPT = "conformance/release/t006-input-shapes/qualification.json"

T007_FILE = "crates/b4-http/tests/thread_concurrency_tcp.rs"
T007_NAME = "release_t007_same_thread_overlap_has_one_winner"
T007_SCOPE = "native_same_thread_admission_tcp_sqlite"
T007_RECEIPT = "conformance/release/t007-same-thread-admission/qualification.json"

T008_FILE = "crates/b4-http/tests/different_thread_tcp.rs"
T008_NAME = "release_t008_different_threads_execute_concurrently"
T008_SCOPE = "native_different_thread_concurrency_tcp_sqlite"
T008_RECEIPT = "conformance/release/t008-different-thread-concurrency/qualification.json"

T009_FILE = "crates/b4-http/tests/wait_output_tcp.rs"
T009_NAME = "release_t009_wait_returns_direct_value_and_void_null"
T009_SCOPE = "native_wait_output_tcp_sqlite"
T009_RECEIPT = "conformance/release/t009-wait-output/qualification.json"


T010_FILE = "crates/b4-http/tests/stream_payload_tcp.rs"
T010_NAME = "release_t010_native_stream_payloads_are_direct_after_commit"
T010_SCOPE = "native_stream_event_payloads_tcp_sqlite"
T010_RECEIPT = "conformance/release/t010-native-stream-payloads/qualification.json"

T011_FILE = "crates/b4-http/tests/terminal_frame_tcp.rs"
T011_NAME = "release_t011_normal_native_streams_end_with_one_done"
T011_SCOPE = "native_normal_stream_terminal_tcp_sqlite"
T011_RECEIPT = "conformance/release/t011-normal-stream-terminal/qualification.json"

T012_FILE = "crates/b4-http/tests/disconnect_completion_tcp.rs"
T012_NAME = "release_t012_disconnected_stream_and_wait_preserve_admitted_work"
T012_SCOPE = "native_disconnected_run_completion_tcp_sqlite"
T012_RECEIPT = "conformance/release/t012-disconnect-completion/qualification.json"

T013_FILE = "crates/b4-http/tests/attachment_state_tcp.rs"
T013_NAME = "release_t013_live_and_settled_attach_start_with_state_without_execution"
T013_SCOPE = "native_read_only_attachment_state_tcp_sqlite"
T013_RECEIPT = "conformance/release/t013-attachment-state/qualification.json"

T014_FILE = "crates/b4-http/tests/attachment_cut_tcp.rs"
T014_NAME = "release_t014_attachment_cut_and_live_tail_preserve_each_event_once"
T014_SCOPE = "native_attachment_cut_live_tail_tcp_sqlite"
T014_RECEIPT = "conformance/release/t014-attachment-cut/qualification.json"

T015_FILE = "crates/b4-http/tests/attachment_truncation_tcp.rs"
T015_NAME = "release_t015_native_digest_overflow_attaches_with_explicit_truncation"
T015_SCOPE = "native_truncated_attachment_after_projection_limit_tcp_sqlite"
T015_RECEIPT = "conformance/release/t015-attachment-truncation/qualification.json"
T016_FILE = "crates/b4-http/tests/viewer_overflow_tcp.rs"
T016_NAME = "release_t016_viewer_capacity_and_queue_overflow_do_not_stop_run"
T016_SCOPE = "native_attached_viewer_capacity_and_byte_overflow_tcp_sqlite"
T016_RECEIPT = "conformance/release/t016-viewer-overflow/qualification.json"
T017_FILE = "crates/b4-http/tests/heartbeat_splits_tcp.rs"
T017_NAME = "release_t017_heartbeat_and_split_utf8_tcp_preserve_one_terminal"
T017_SCOPE = "native_heartbeat_and_split_utf8_tcp_sqlite"
T017_RECEIPT = "conformance/release/t017-heartbeat-splits/qualification.json"
T018_FILE = "crates/b4-http/tests/approval_durable_disclosure_tcp.rs"
T018_NAME = "release_t018_approval_park_is_durable_before_tcp_disclosure"
T018_SCOPE = "native_approval_park_durable_before_disclosure_tcp_sqlite"
T018_RECEIPT = "conformance/release/t018-durable-disclosure/qualification.json"

class CoreReleaseTraceabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="b4-metadata-", dir="/private/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo_patch = patch.object(check_contracts, "REPO", self.root)
        self.repo_patch.start()
        self.addCleanup(self.repo_patch.stop)
        source = self.root / TEST_FILE
        source.parent.mkdir(parents=True)
        source.write_text(f"#[test]\nfn {TEST_NAME}() {{}}\n")
        self.requirements = {"release_acceptance_inventory": [
            {"id": f"T-{n:03d}", "implementation_status": "pending",
             "verification_status": "not_run", "acceptance_test_status": "future_release_scenario"}
            for n in range(1, 57)]}
        self.requirements["release_acceptance_inventory"][0].update({
            "classification": "NATIVE", "implementation_status": "implemented",
            "verification_status": "passed_native_scope",
            "acceptance_test_status": "implemented_native_scope", "release_scope": SCOPE,
            "acceptance_tests": [{"file": TEST_FILE, "name": TEST_NAME}],
            "verification_receipt": RECEIPT})
        descriptor = "a" * 64
        commands = [
            ["cargo", "test", "-p", "b4-http", "--test", "native_tcp", TEST_NAME,
             "--locked", "--offline", "--", "--nocapture", "--test-threads=1"],
            ["cargo", "test", "-p", "b4-http", "--test", "native_tcp", "--locked",
             "--offline", "--", "--test-threads=1"],
            ["cargo", "fmt", "--all", "--", "--check"],
            ["cargo", "clippy", "-p", "b4-http", "--all-targets", "--locked", "--offline",
             "--", "-D", "warnings"],
            ["python3", "-B", "scripts/test_core_release_traceability.py"]]
        self.receipt = {
            "record_version": 1, "status": "qualified", "scenario_id": "T-001",
            "evidence_class": "NATIVE", "release_scope": SCOPE,
            "acceptance_test": {"file": TEST_FILE, "name": TEST_NAME,
                                "sha256": hashlib.sha256(source.read_bytes()).hexdigest()},
            "native_source_descriptor_sha256": descriptor, "native_source_file_count": 1,
            "checks": [{"step": step, "command": command, "exit_code": 0, "reaped": True,
                        "timed_out": False, "changed_source_files": [], "added_source_files": [],
                        "removed_source_files": [], "historical_result_transfer": False,
                        "source_descriptor_sha256": descriptor, "runtime_credit": index < 2}
                       for index, (step, command) in enumerate(zip(
                           ["scenario-test", "native-tcp-tests", "format-check", "scoped-lint", "guard-tests"],
                           commands))],
            "core_profile_qualified": False, "compatibility_qualified": False,
            "other_release_scenarios_qualified": [], "public_metadata_required_after_delivery": True,
            "initial_proof": {"additional_runtime_credit": False}}
        self.save_receipt()
        source2 = self.root / T002_FILE
        source2.write_text(f"#[test]\nfn {T002_NAME}() {{}}\n")
        row2 = copy.deepcopy(self.requirements["release_acceptance_inventory"][0])
        row2.update(id="T-002", release_scope=T002_SCOPE,
                    acceptance_tests=[{"file": T002_FILE, "name": T002_NAME}],
                    verification_receipt=T002_RECEIPT)
        self.requirements["release_acceptance_inventory"][1] = row2
        self.receipt2 = copy.deepcopy(self.receipt)
        self.receipt2.update(scenario_id="T-002", release_scope=T002_SCOPE,
                             native_source_descriptor_sha256="b" * 64,
                             acceptance_test={"file": T002_FILE, "name": T002_NAME,
                                              "sha256": hashlib.sha256(source2.read_bytes()).hexdigest()})
        checks = self.receipt2["checks"]
        checks[0]["command"][5:7] = ["thread_state_tcp", T002_NAME]
        checks[1]["step"] = "thread-state-tests"
        checks[1]["command"][5] = "thread_state_tcp"
        regression = copy.deepcopy(self.receipt["checks"][1])
        regression["step"] = "t001-regression"
        checks.insert(2, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "b" * 64
            check["runtime_credit"] = index < 3
        self.save_receipt2()
        source3 = self.root / T003_FILE
        source3.write_text(f"#[test]\nfn {T003_NAME}() {{}}\n")
        row3 = copy.deepcopy(row2)
        row3.update(id="T-003", release_scope=T003_SCOPE,
                    acceptance_tests=[{"file": T003_FILE, "name": T003_NAME}],
                    verification_receipt=T003_RECEIPT)
        self.requirements["release_acceptance_inventory"][2] = row3
        self.receipt3 = copy.deepcopy(self.receipt2)
        self.receipt3.update(scenario_id="T-003", release_scope=T003_SCOPE,
                             native_source_descriptor_sha256="c" * 64,
                             acceptance_test={"file": T003_FILE, "name": T003_NAME,
                                              "sha256": hashlib.sha256(source3.read_bytes()).hexdigest()})
        checks = self.receipt3["checks"]
        checks[0]["command"][5:7] = ["thread_admission_tcp", T003_NAME]
        checks[1]["step"] = "thread-admission-tests"
        checks[1]["command"][5] = "thread_admission_tcp"
        regression = copy.deepcopy(self.receipt2["checks"][1])
        regression["step"] = "t002-regression"
        checks.insert(3, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "c" * 64
            check["runtime_credit"] = index < 4
        self.save_receipt3()

        source4 = self.root / T004_FILE
        source4.write_text(f"#[test]\nfn {T004_NAME}() {{}}\n")
        row4 = copy.deepcopy(row3)
        row4.update(id="T-004", release_scope=T004_SCOPE,
                    acceptance_tests=[{"file": T004_FILE, "name": T004_NAME}],
                    verification_receipt=T004_RECEIPT)
        self.requirements["release_acceptance_inventory"][3] = row4
        self.receipt4 = copy.deepcopy(self.receipt3)
        self.receipt4.update(scenario_id="T-004", release_scope=T004_SCOPE,
                             native_source_descriptor_sha256="d" * 64,
                             acceptance_test={"file": T004_FILE, "name": T004_NAME,
                                              "sha256": hashlib.sha256(source4.read_bytes()).hexdigest()})
        checks = self.receipt4["checks"]
        checks[0]["command"][5:7] = ["thread_route_tcp", T004_NAME]
        checks[1]["step"] = "thread-route-tests"
        checks[1]["command"][5] = "thread_route_tcp"
        regression = copy.deepcopy(self.receipt3["checks"][1])
        regression["step"] = "t003-regression"
        checks.insert(4, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "d" * 64
            check["runtime_credit"] = index < 5
        self.save_receipt4()

        source5 = self.root / T005_FILE
        source5.write_text(f"#[test]\nfn {T005_NAME}() {{}}\n")
        row5 = copy.deepcopy(row4)
        row5.update(id="T-005", release_scope=T005_SCOPE,
                    acceptance_tests=[{"file": T005_FILE, "name": T005_NAME}],
                    verification_receipt=T005_RECEIPT)
        self.requirements["release_acceptance_inventory"][4] = row5
        self.receipt5 = copy.deepcopy(self.receipt4)
        self.receipt5.update(scenario_id="T-005", release_scope=T005_SCOPE,
                             native_source_descriptor_sha256="e" * 64,
                             acceptance_test={"file": T005_FILE, "name": T005_NAME,
                                              "sha256": hashlib.sha256(source5.read_bytes()).hexdigest()})
        checks = self.receipt5["checks"]
        checks[0]["command"][5:7] = ["thread_json_tcp", T005_NAME]
        checks[1]["step"] = "thread-json-tests"
        checks[1]["command"][5] = "thread_json_tcp"
        regression = copy.deepcopy(self.receipt4["checks"][1])
        regression["step"] = "t004-regression"
        checks.insert(5, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "e" * 64
            check["runtime_credit"] = index < 6
        self.save_receipt5()

        source6 = self.root / T006_FILE
        source6.write_text(f"#[test]\nfn {T006_NAME}() {{}}\n")
        row6 = copy.deepcopy(row5)
        row6.update(id="T-006", release_scope=T006_SCOPE,
                    acceptance_tests=[{"file": T006_FILE, "name": T006_NAME}],
                    verification_receipt=T006_RECEIPT)
        self.requirements["release_acceptance_inventory"][5] = row6
        self.receipt6 = copy.deepcopy(self.receipt5)
        self.receipt6.update(scenario_id="T-006", release_scope=T006_SCOPE,
                             native_source_descriptor_sha256="f" * 64,
                             acceptance_test={"file": T006_FILE, "name": T006_NAME,
                                              "sha256": hashlib.sha256(source6.read_bytes()).hexdigest()})
        checks = self.receipt6["checks"]
        checks[0]["command"][5:7] = ["input_shape_tcp", T006_NAME]
        checks[1]["step"] = "input-shape-tests"
        checks[1]["command"][5] = "input_shape_tcp"
        regression = copy.deepcopy(self.receipt5["checks"][1])
        regression["step"] = "t005-regression"
        checks.insert(6, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "f" * 64
            check["runtime_credit"] = index < 7
        self.save_receipt6()

        source7 = self.root / T007_FILE
        source7.write_text(f"#[test]\nfn {T007_NAME}() {{}}\n")
        row7 = copy.deepcopy(row6)
        row7.update(id="T-007", release_scope=T007_SCOPE,
                    acceptance_tests=[{"file": T007_FILE, "name": T007_NAME}],
                    verification_receipt=T007_RECEIPT)
        self.requirements["release_acceptance_inventory"][6] = row7
        self.receipt7 = copy.deepcopy(self.receipt6)
        self.receipt7.update(scenario_id="T-007", release_scope=T007_SCOPE,
                             native_source_descriptor_sha256="7" * 64,
                             acceptance_test={"file": T007_FILE, "name": T007_NAME,
                                              "sha256": hashlib.sha256(source7.read_bytes()).hexdigest()})
        checks = self.receipt7["checks"]
        checks[0]["command"][5:7] = ["thread_concurrency_tcp", T007_NAME]
        checks[1]["step"] = "thread-concurrency-tests"
        checks[1]["command"][5] = "thread_concurrency_tcp"
        regression = copy.deepcopy(self.receipt6["checks"][1])
        regression["step"] = "t006-regression"
        checks.insert(7, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "7" * 64
            check["runtime_credit"] = index < 8
        self.save_receipt7()

        source8 = self.root / T008_FILE
        source8.write_text(f"#[test]\nfn {T008_NAME}() {{}}\n")
        row8 = copy.deepcopy(row7)
        row8.update(id="T-008", release_scope=T008_SCOPE,
                    acceptance_tests=[{"file": T008_FILE, "name": T008_NAME}],
                    verification_receipt=T008_RECEIPT)
        self.requirements["release_acceptance_inventory"][7] = row8
        self.receipt8 = copy.deepcopy(self.receipt7)
        self.receipt8.update(scenario_id="T-008", release_scope=T008_SCOPE,
                             native_source_descriptor_sha256="8" * 64,
                             acceptance_test={"file": T008_FILE, "name": T008_NAME,
                                              "sha256": hashlib.sha256(source8.read_bytes()).hexdigest()})
        checks = self.receipt8["checks"]
        checks[0]["command"][5:7] = ["different_thread_tcp", T008_NAME]
        checks[1]["step"] = "different-thread-tests"
        checks[1]["command"][5] = "different_thread_tcp"
        regression = copy.deepcopy(self.receipt7["checks"][1])
        regression["step"] = "t007-regression"
        checks.insert(8, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "8" * 64
            check["runtime_credit"] = index < 9
        self.save_receipt8()

        source9 = self.root / T009_FILE
        source9.write_text(f"#[test]\nfn {T009_NAME}() {{}}\n")
        row9 = copy.deepcopy(row8)
        row9.update(id="T-009", release_scope=T009_SCOPE,
                    acceptance_tests=[{"file": T009_FILE, "name": T009_NAME}],
                    verification_receipt=T009_RECEIPT)
        self.requirements["release_acceptance_inventory"][8] = row9
        self.receipt9 = copy.deepcopy(self.receipt8)
        self.receipt9.update(scenario_id="T-009", release_scope=T009_SCOPE,
                             native_source_descriptor_sha256="9" * 64,
                             acceptance_test={"file": T009_FILE, "name": T009_NAME,
                                              "sha256": hashlib.sha256(source9.read_bytes()).hexdigest()})
        checks = self.receipt9["checks"]
        checks[0]["command"][5:7] = ["wait_output_tcp", T009_NAME]
        checks[1]["step"] = "wait-output-tests"
        checks[1]["command"][5] = "wait_output_tcp"
        regression = copy.deepcopy(self.receipt8["checks"][1])
        regression["step"] = "t008-regression"
        checks.insert(9, regression)
        for index, check in enumerate(checks):
            check["source_descriptor_sha256"] = "9" * 64
            check["runtime_credit"] = index < 10
        self.save_receipt9()


        source10 = self.root / T010_FILE
        source10.write_text(f"#[test]\nfn {T010_NAME}() {{}}\n")
        row10 = copy.deepcopy(row9)
        row10.update(id="T-010", release_scope=T010_SCOPE,
                     acceptance_tests=[{"file": T010_FILE, "name": T010_NAME}],
                     verification_receipt=T010_RECEIPT)
        self.requirements["release_acceptance_inventory"][9] = row10
        self.receipt10 = copy.deepcopy(self.receipt9)
        self.receipt10.update(scenario_id="T-010", release_scope=T010_SCOPE,
                              native_source_descriptor_sha256="0" * 64,
                              native_source_file_count=1156,
                              acceptance_test={"file": T010_FILE, "name": T010_NAME,
                                               "sha256": hashlib.sha256(source10.read_bytes()).hexdigest()})
        checks = self.receipt10["checks"]
        checks[0]["command"][5:7] = ["stream_payload_tcp", T010_NAME]
        checks[1]["step"] = "stream-payload-tests"
        checks[1]["command"][5] = "stream_payload_tcp"
        for offset, (step, package) in enumerate([
                ("native-stream-api-tests", "b4-runtime"),
                ("native-stream-control-tests", "b4-http")]):
            check = copy.deepcopy(checks[1])
            check.update(step=step, command=["cargo", "test", "-p", package, "--test",
                         "native_streaming", "--locked", "--offline", "--", "--test-threads=1"])
            checks.insert(2 + offset, check)
        regression = copy.deepcopy(self.receipt9["checks"][1])
        regression["step"] = "t009-regression"
        checks.insert(12, regression)
        checks[14]["command"][2:2] = ["-p", "b4-runtime"]
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="0" * 64, runtime_credit=index < 13,
                         metadata_only=index == 15)
        # This evidence describes its own earlier feature map, not the final map.
        self.receipt10["feature_validation"] = {
            "source_descriptor_sha256": "f" * 64, "source_file_count": 1155,
            "run": "verification/feature-workspace-123", "receipt_sha256": "c" * 64,
            "command": ["cargo", "test", "--workspace", "--locked", "--offline", "--", "--test-threads=1"],
            "exit_code": 0, "reaped": True, "timed_out": False, "source_unchanged": True,
            "deadline_seconds": 1800, "log_sha256": "d" * 64,
            "reported_passes": 1694, "reported_ignored": 11, "reported_summary_executions": 163,
            "additional_runtime_credit": False}
        self.save_receipt10()

        source11 = self.root / T011_FILE
        source11.write_text(f"#[test]\nfn {T011_NAME}() {{}}\n")
        row11 = copy.deepcopy(row10)
        row11.update(id="T-011", release_scope=T011_SCOPE,
                     acceptance_tests=[{"file": T011_FILE, "name": T011_NAME}],
                     verification_receipt=T011_RECEIPT)
        self.requirements["release_acceptance_inventory"][10] = row11
        self.receipt11 = copy.deepcopy(self.receipt10)
        # T-010's separate feature proof remains its own historical evidence.
        del self.receipt11["feature_validation"]
        self.receipt11.update(scenario_id="T-011", release_scope=T011_SCOPE,
                              native_source_descriptor_sha256="1" * 64,
                              native_source_file_count=1157,
                              acceptance_test={"file": T011_FILE, "name": T011_NAME,
                                               "sha256": hashlib.sha256(source11.read_bytes()).hexdigest()})
        checks = self.receipt11["checks"]
        checks[0]["command"][5:7] = ["terminal_frame_tcp", T011_NAME]
        checks[1]["step"] = "terminal-frame-tests"
        checks[1]["command"][5] = "terminal_frame_tcp"
        regression = copy.deepcopy(self.receipt10["checks"][1])
        regression["step"] = "t010-regression"
        checks.insert(13, regression)
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="1" * 64, runtime_credit=index < 14,
                         metadata_only=index == 16)
        self.receipt11["initial_proof"] = {
            "run": "verification/scenario-test-123", "receipt_sha256": "2" * 64,
            "source_snapshot_sha256": "3" * 64, "additional_runtime_credit": False}
        self.save_receipt11()

        source12 = self.root / T012_FILE
        source12.write_text(f"#[test]\nfn {T012_NAME}() {{}}\n")
        row12 = copy.deepcopy(row11)
        row12.update(id="T-012", release_scope=T012_SCOPE,
                     acceptance_tests=[{"file": T012_FILE, "name": T012_NAME}],
                     verification_receipt=T012_RECEIPT)
        self.requirements["release_acceptance_inventory"][11] = row12
        self.receipt12 = copy.deepcopy(self.receipt11)
        # The predecessor has no feature-validation object to inherit.
        self.receipt12.update(scenario_id="T-012", release_scope=T012_SCOPE,
                              native_source_descriptor_sha256="4" * 64,
                              native_source_file_count=1166,
                              acceptance_test={"file": T012_FILE, "name": T012_NAME,
                                               "sha256": hashlib.sha256(source12.read_bytes()).hexdigest()})
        checks = self.receipt12["checks"]
        checks[0]["command"][5:7] = ["disconnect_completion_tcp", T012_NAME]
        checks[1]["step"] = "disconnect-completion-tests"
        checks[1]["command"][5] = "disconnect_completion_tcp"
        regression = copy.deepcopy(self.receipt11["checks"][1])
        regression["step"] = "t011-regression"
        checks.insert(14, regression)
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="4" * 64, runtime_credit=index < 15,
                         metadata_only=index == 17)
        self.receipt12["initial_proof"] = {
            "run": "verification/scenario-test-123", "receipt_sha256": "5" * 64,
            "source_snapshot_sha256": "6" * 64, "additional_runtime_credit": False}
        self.save_receipt12()

        source13 = self.root / T013_FILE
        source13.write_text(f"#[test]\nfn {T013_NAME}() {{}}\n")
        row13 = copy.deepcopy(row12)
        row13.update(id="T-013", release_scope=T013_SCOPE,
                     acceptance_tests=[{"file": T013_FILE, "name": T013_NAME}],
                     verification_receipt=T013_RECEIPT)
        self.requirements["release_acceptance_inventory"][12] = row13
        self.receipt13 = copy.deepcopy(self.receipt12)
        # The predecessor has no feature-validation object to inherit.
        self.receipt13.update(scenario_id="T-013", release_scope=T013_SCOPE,
                              native_source_descriptor_sha256="7" * 64,
                              native_source_file_count=1173,
                              acceptance_test={"file": T013_FILE, "name": T013_NAME,
                                               "sha256": hashlib.sha256(source13.read_bytes()).hexdigest()})
        checks = self.receipt13["checks"]
        checks[0]["command"][5:7] = ["attachment_state_tcp", T013_NAME]
        checks[1]["step"] = "attachment-state-tests"
        checks[1]["command"][5] = "attachment_state_tcp"
        regression = copy.deepcopy(self.receipt12["checks"][1])
        regression["step"] = "t012-regression"
        checks.insert(15, regression)
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="7" * 64, runtime_credit=index < 16,
                         metadata_only=index == 18)
        self.receipt13["initial_proof"] = {
            "run": "verification/scenario-test-123", "receipt_sha256": "8" * 64,
            "source_snapshot_sha256": "9" * 64, "additional_runtime_credit": False}
        self.save_receipt13()

        source14 = self.root / T014_FILE
        source14.write_text(f"#[test]\nfn {T014_NAME}() {{}}\n")
        row14 = copy.deepcopy(row13)
        row14.update(id="T-014", release_scope=T014_SCOPE,
                     acceptance_tests=[{"file": T014_FILE, "name": T014_NAME}],
                     verification_receipt=T014_RECEIPT)
        self.requirements["release_acceptance_inventory"][13] = row14
        self.receipt14 = copy.deepcopy(self.receipt13)
        self.receipt14.update(scenario_id="T-014", release_scope=T014_SCOPE,
                              native_source_descriptor_sha256="a1" * 32,
                              native_source_file_count=1176,
                              acceptance_test={"file": T014_FILE, "name": T014_NAME,
                                               "sha256": hashlib.sha256(source14.read_bytes()).hexdigest()})
        checks = self.receipt14["checks"]
        checks[0]["command"][5:7] = ["attachment_cut_tcp", T014_NAME]
        checks[1]["step"] = "attachment-cut-tests"
        checks[1]["command"][5] = "attachment_cut_tcp"
        regression = copy.deepcopy(self.receipt13["checks"][1])
        regression["step"] = "t013-regression"
        checks.insert(16, regression)
        checks[-1]["command"][0] = "/Users/blove/.pyenv/versions/3.12.12/bin/python3"
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="a1" * 32, runtime_credit=index < 17,
                         metadata_only=index == 19)
        self.receipt14["initial_proof"] = {
            "run": "verification/scenario-test-123", "receipt_sha256": "b1" * 32,
            "source_snapshot_sha256": "c1" * 32, "additional_runtime_credit": False}
        self.save_receipt14()

        source15 = self.root / T015_FILE
        source15.write_text(f"#[test]\nfn {T015_NAME}() {{}}\n")
        row15 = copy.deepcopy(row14)
        row15.update(id="T-015", release_scope=T015_SCOPE,
                     acceptance_tests=[{"file": T015_FILE, "name": T015_NAME}],
                     verification_receipt=T015_RECEIPT)
        self.requirements["release_acceptance_inventory"][14] = row15
        self.receipt15 = copy.deepcopy(self.receipt14)
        self.receipt15.update(scenario_id="T-015", release_scope=T015_SCOPE,
                              native_source_descriptor_sha256="d1" * 32,
                              native_source_file_count=1183,
                              acceptance_test={"file": T015_FILE, "name": T015_NAME,
                                               "sha256": hashlib.sha256(source15.read_bytes()).hexdigest()})
        checks = self.receipt15["checks"]
        checks[0]["command"][5:7] = ["attachment_truncation_tcp", T015_NAME]
        checks[1]["step"] = "attachment-truncation-tests"
        checks[1]["command"][5] = "attachment_truncation_tcp"
        http_control = copy.deepcopy(checks[3])
        http_control.update(step="http-control-tests",
                            command=["cargo", "test", "-p", "b4-http", "--lib", "--locked",
                                     "--offline", "--", "--test-threads=1"])
        checks.insert(4, http_control)
        regression = copy.deepcopy(self.receipt14["checks"][1])
        regression["step"] = "t014-regression"
        checks.insert(18, regression)
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="d1" * 32, runtime_credit=index < 19,
                         metadata_only=index == 21)
        self.receipt15["initial_proof"] = {
            "run": "verification/scenario-test-456", "receipt_sha256": "e1" * 32,
            "source_snapshot_sha256": "f1" * 32, "additional_runtime_credit": False}
        self.receipt15["pre_fix_proof"] = {
            "run": "verification/scenario-test-123", "receipt_sha256": "12" * 32,
            "log_sha256": "23" * 32, "source_descriptor_sha256": "34" * 32,
            "exit_code": 101, "expected_failure": "late_attach_409_instead_of_200",
            "additional_runtime_credit": False}
        self.save_receipt15()

        source16 = self.root / T016_FILE
        source16.write_text(f"#[test]\nfn {T016_NAME}() {{}}\n")
        row16 = copy.deepcopy(row15)
        row16.update(id="T-016", release_scope=T016_SCOPE,
                     acceptance_tests=[{"file": T016_FILE, "name": T016_NAME}],
                     verification_receipt=T016_RECEIPT)
        self.requirements["release_acceptance_inventory"][15] = row16
        self.receipt16 = copy.deepcopy(self.receipt15)
        self.receipt16.pop("pre_fix_proof")
        self.receipt16.update(scenario_id="T-016", release_scope=T016_SCOPE,
                              native_source_descriptor_sha256="16" * 32,
                              native_source_file_count=1186,
                              acceptance_test={"file": T016_FILE, "name": T016_NAME,
                                               "sha256": hashlib.sha256(source16.read_bytes()).hexdigest()})
        checks = self.receipt16["checks"]
        checks[0]["command"][5:7] = ["viewer_overflow_tcp", T016_NAME]
        checks[1]["step"] = "viewer-overflow-tests"
        checks[1]["command"][5] = "viewer_overflow_tcp"
        regression = copy.deepcopy(self.receipt15["checks"][1])
        regression["step"] = "t015-regression"
        checks.insert(19, regression)
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="16" * 32,
                         runtime_credit=index < 20, metadata_only=index == 22)
        self.receipt16["initial_proof"] = {
            "run": "verification/scenario-test-789", "receipt_sha256": "17" * 32,
            "source_snapshot_sha256": "18" * 32, "additional_runtime_credit": False}
        self.save_receipt16()

        source17 = self.root / T017_FILE
        source17.write_text(f"#[test]\nfn {T017_NAME}() {{}}\n")
        row17 = copy.deepcopy(row16)
        row17.update(id="T-017", release_scope=T017_SCOPE,
                     acceptance_tests=[{"file": T017_FILE, "name": T017_NAME}],
                     verification_receipt=T017_RECEIPT)
        self.requirements["release_acceptance_inventory"][16] = row17
        self.receipt17 = copy.deepcopy(self.receipt16)
        self.receipt17.update(scenario_id="T-017", release_scope=T017_SCOPE,
                              native_source_descriptor_sha256="19" * 32,
                              native_source_file_count=1193,
                              acceptance_test={"file": T017_FILE, "name": T017_NAME,
                                               "sha256": hashlib.sha256(source17.read_bytes()).hexdigest()})
        checks = self.receipt17["checks"]
        checks[0]["command"][5:7] = ["heartbeat_splits_tcp", T017_NAME]
        checks[1]["step"] = "heartbeat-splits-tests"
        checks[1]["command"][5] = "heartbeat_splits_tcp"
        regression = copy.deepcopy(self.receipt16["checks"][1])
        regression["step"] = "t016-regression"
        checks.insert(20, regression)
        for index, check in enumerate(checks):
            check.update(source_descriptor_sha256="19" * 32,
                         runtime_credit=index < 21, metadata_only=index == 23,
                         pid=123, process_group=123, start_new_session=True,
                         process_identity_verified=True, process_group_retired=True)
        self.receipt17["initial_proof"] = {
            "run": "verification/scenario-test-790", "receipt_sha256": "20" * 32,
            "source_snapshot_sha256": "21" * 32, "additional_runtime_credit": False}
        self.save_receipt17()
        self.receipt18 = self.prepare_t018_durable_disclosure_policy()

    def save_receipt17(self):
        path = self.root / T017_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt17))

    def save_receipt16(self):
        path = self.root / T016_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt16))

    def save_receipt15(self):
        path = self.root / T015_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt15))

    def save_receipt14(self):
        path = self.root / T014_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt14))

    def save_receipt13(self):
        path = self.root / T013_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt13))

    def save_receipt12(self):
        path = self.root / T012_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt12))

    def save_receipt11(self):
        path = self.root / T011_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt11))

    def save_receipt10(self):
        path = self.root / T010_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt10))

    def save_receipt9(self):
        path = self.root / T009_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt9))

    def save_receipt8(self):
        path = self.root / T008_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt8))

    def save_receipt7(self):
        path = self.root / T007_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt7))

    def save_receipt6(self):
        path = self.root / T006_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt6))

    def save_receipt5(self):
        path = self.root / T005_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt5))

    def save_receipt4(self):
        path = self.root / T004_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt4))

    def save_receipt3(self):
        path = self.root / T003_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt3))

    def save_receipt2(self):
        path = self.root / T002_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt2))

    def save_receipt(self):
        path = self.root / RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.receipt))

    def check(self):
        check_contracts.check_core_release_traceability(self.requirements)

    def test_accepts_prior_proofs_with_distinct_source_descriptors(self):
        self.check()

    def test_rejects_old_single_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][1] = {
            "id": "T-002", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t002_receipt(self):
        (self.root / T002_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t002_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][1][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t002_proof(self):
        original = copy.deepcopy(self.receipt2)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt2 = copy.deepcopy(original)
                self.receipt2[key] = value
                self.save_receipt2()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t002_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt2)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T002_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "a" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(6):
            for key, value in mutations + [("runtime_credit", index >= 3)]:
                with self.subTest(index=index, field=key):
                    self.receipt2 = copy.deepcopy(original)
                    self.receipt2["checks"][index][key] = value
                    self.save_receipt2()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt2 = copy.deepcopy(original)
            self.receipt2["checks"] = checks
            self.save_receipt2()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t002_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt2)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "c" * 64)]:
            self.receipt2 = copy.deepcopy(original)
            self.receipt2["acceptance_test"][key] = value
            self.save_receipt2()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t002_source_and_missing_declaration(self):
        source = self.root / T002_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt2["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt2()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t003_receipt(self):
        (self.root / T003_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t003_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][2][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t003_proof(self):
        original = copy.deepcopy(self.receipt3)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt3 = copy.deepcopy(original)
                self.receipt3[key] = value
                self.save_receipt3()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t003_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt3)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T003_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "b" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(7):
            for key, value in mutations + [("runtime_credit", index >= 4)]:
                with self.subTest(index=index, field=key):
                    self.receipt3 = copy.deepcopy(original)
                    self.receipt3["checks"][index][key] = value
                    self.save_receipt3()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt3 = copy.deepcopy(original)
            self.receipt3["checks"] = checks
            self.save_receipt3()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t003_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt3)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "d" * 64)]:
            self.receipt3 = copy.deepcopy(original)
            self.receipt3["acceptance_test"][key] = value
            self.save_receipt3()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t003_source_and_missing_declaration(self):
        source = self.root / T003_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt3["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt3()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t004_receipt(self):
        (self.root / T004_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t004_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][3][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t004_proof(self):
        original = copy.deepcopy(self.receipt4)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt4 = copy.deepcopy(original)
                self.receipt4[key] = value
                self.save_receipt4()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t004_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt4)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T004_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "c" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(8):
            for key, value in mutations + [("runtime_credit", index >= 5)]:
                with self.subTest(index=index, field=key):
                    self.receipt4 = copy.deepcopy(original)
                    self.receipt4["checks"][index][key] = value
                    self.save_receipt4()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt4 = copy.deepcopy(original)
            self.receipt4["checks"] = checks
            self.save_receipt4()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t004_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt4)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "e" * 64)]:
            self.receipt4 = copy.deepcopy(original)
            self.receipt4["acceptance_test"][key] = value
            self.save_receipt4()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t004_source_and_missing_declaration(self):
        source = self.root / T004_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt4["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt4()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t005_receipt(self):
        (self.root / T005_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t005_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][4][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t005_proof(self):
        original = copy.deepcopy(self.receipt5)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt5 = copy.deepcopy(original)
                self.receipt5[key] = value
                self.save_receipt5()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t005_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt5)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T005_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "d" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(9):
            for key, value in mutations + [("runtime_credit", index >= 6)]:
                with self.subTest(index=index, field=key):
                    self.receipt5 = copy.deepcopy(original)
                    self.receipt5["checks"][index][key] = value
                    self.save_receipt5()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt5 = copy.deepcopy(original)
            self.receipt5["checks"] = checks
            self.save_receipt5()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t005_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt5)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "f" * 64)]:
            self.receipt5 = copy.deepcopy(original)
            self.receipt5["acceptance_test"][key] = value
            self.save_receipt5()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t005_source_and_missing_declaration(self):
        source = self.root / T005_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt5["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt5()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_old_four_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][4] = {
            "id": "T-005", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t006_receipt(self):
        (self.root / T006_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t006_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][5][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t006_proof(self):
        original = copy.deepcopy(self.receipt6)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt6 = copy.deepcopy(original)
                self.receipt6[key] = value
                self.save_receipt6()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t006_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt6)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T006_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "e" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(10):
            for key, value in mutations + [("runtime_credit", index >= 7)]:
                with self.subTest(index=index, field=key):
                    self.receipt6 = copy.deepcopy(original)
                    self.receipt6["checks"][index][key] = value
                    self.save_receipt6()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt6 = copy.deepcopy(original)
            self.receipt6["checks"] = checks
            self.save_receipt6()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t006_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt6)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "a" * 64)]:
            self.receipt6 = copy.deepcopy(original)
            self.receipt6["acceptance_test"][key] = value
            self.save_receipt6()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t006_source_and_missing_declaration(self):
        source = self.root / T006_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt6["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt6()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_old_five_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][5] = {
            "id": "T-006", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t007_receipt(self):
        (self.root / T007_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t007_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][6][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t007_proof(self):
        original = copy.deepcopy(self.receipt7)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt7 = copy.deepcopy(original)
                self.receipt7[key] = value
                self.save_receipt7()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t007_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt7)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T007_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "e" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(11):
            for key, value in mutations + [("runtime_credit", index >= 8)]:
                with self.subTest(index=index, field=key):
                    self.receipt7 = copy.deepcopy(original)
                    self.receipt7["checks"][index][key] = value
                    self.save_receipt7()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt7 = copy.deepcopy(original)
            self.receipt7["checks"] = checks
            self.save_receipt7()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t007_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt7)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "a" * 64)]:
            self.receipt7 = copy.deepcopy(original)
            self.receipt7["acceptance_test"][key] = value
            self.save_receipt7()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t007_source_and_missing_declaration(self):
        source = self.root / T007_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt7["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt7()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_old_six_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][6] = {
            "id": "T-007", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t008_receipt(self):
        (self.root / T008_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t008_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][7][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t008_proof(self):
        original = copy.deepcopy(self.receipt8)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt8 = copy.deepcopy(original)
                self.receipt8[key] = value
                self.save_receipt8()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t008_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt8)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T008_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "e" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(12):
            for key, value in mutations + [("runtime_credit", index >= 9)]:
                with self.subTest(index=index, field=key):
                    self.receipt8 = copy.deepcopy(original)
                    self.receipt8["checks"][index][key] = value
                    self.save_receipt8()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt8 = copy.deepcopy(original)
            self.receipt8["checks"] = checks
            self.save_receipt8()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t008_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt8)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "a" * 64)]:
            self.receipt8 = copy.deepcopy(original)
            self.receipt8["acceptance_test"][key] = value
            self.save_receipt8()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t008_source_and_missing_declaration(self):
        source = self.root / T008_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt8["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt8()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_old_seven_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][7] = {
            "id": "T-008", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()


    def test_accepts_exact_t010_policy_and_separate_feature_evidence(self):
        self.assertEqual(len(self.receipt10["checks"]), 16)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt10["checks"]), 13)
        self.assertNotEqual(self.receipt10["native_source_descriptor_sha256"],
                            self.receipt10["feature_validation"]["source_descriptor_sha256"])
        self.check()

    def test_rejects_missing_t010_receipt(self):
        (self.root / T010_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t010_row_and_receipt_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][9][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt10)
        for key, value in [("status", "pending"), ("scenario_id", "T-009"),
                           ("release_scope", "core"), ("core_profile_qualified", True),
                           ("compatibility_qualified", True), ("evidence_class", "COMPAT")]:
            self.receipt10 = copy.deepcopy(proof)
            self.receipt10[key] = value
            self.save_receipt10()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_invalid_t010_proof_and_bounded_feature_evidence(self):
        original = copy.deepcopy(self.receipt10)
        mutations = [(key, None) for key in ["native_source_descriptor_sha256", "native_source_file_count",
                     "initial_proof", "feature_validation"]]
        mutations += [("native_source_file_count", True),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            self.receipt10 = copy.deepcopy(original)
            self.receipt10[key] = value
            self.save_receipt10()
            with self.assertRaises(ValueError):
                self.check()
        feature = original["feature_validation"]
        negatives = [(key, None) for key in feature]
        negatives += [("source_file_count", True), ("source_file_count", 0),
                      ("source_descriptor_sha256", original["native_source_descriptor_sha256"]),
                      ("source_descriptor_sha256", "bad"), ("receipt_sha256", "bad"),
                      ("log_sha256", "bad"), ("run", "../feature-workspace-123"),
                      ("command", ["cargo", "test"]), ("exit_code", False), ("exit_code", 1),
                      ("reaped", False), ("timed_out", True), ("source_unchanged", False),
                      ("deadline_seconds", 600), ("reported_passes", 0),
                      ("reported_ignored", -1), ("reported_summary_executions", 0),
                      ("additional_runtime_credit", True)]
        for key, value in negatives:
            with self.subTest(feature_field=key, value=value):
                self.receipt10 = copy.deepcopy(original)
                self.receipt10["feature_validation"][key] = value
                self.save_receipt10()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t010_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt10)
        # Exercise the new command categories and each credit boundary.
        for index, key, value in [(0, "command", ["cargo", "test"]),
                (2, "command", ["cargo", "test"]), (3, "command", ["cargo", "test"]),
                (12, "runtime_credit", False), (13, "runtime_credit", True),
                (14, "command", self.receipt9["checks"][-2]["command"]),
                (15, "runtime_credit", True), (15, "metadata_only", False),
                (0, "source_descriptor_sha256", "f" * 64), (0, "exit_code", 1),
                (0, "changed_source_files", [T010_FILE])]:
            with self.subTest(index=index, field=key):
                self.receipt10 = copy.deepcopy(original)
                self.receipt10["checks"][index][key] = value
                self.save_receipt10()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt10 = copy.deepcopy(original)
            self.receipt10["checks"] = checks
            self.save_receipt10()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t010_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt10)
        for key, value in [("file", T009_FILE), ("name", T009_NAME), ("sha256", "a" * 64)]:
            self.receipt10 = copy.deepcopy(original)
            self.receipt10["acceptance_test"][key] = value
            self.save_receipt10()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t010_source_and_missing_declaration(self):
        source = self.root / T010_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt10["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt10()
        with self.assertRaises(ValueError):
            self.check()

    def test_accepts_exact_t011_policy_and_distinct_descriptors(self):
        self.assertEqual(len(self.receipt11["checks"]), 17)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt11["checks"]), 14)
        self.assertNotIn("feature_validation", self.receipt11)
        self.assertNotEqual(self.receipt10["native_source_descriptor_sha256"],
                            self.receipt11["native_source_descriptor_sha256"])
        self.check()

    def test_rejects_missing_t011_receipt(self):
        (self.root / T011_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t011_row_and_receipt_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][10][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt11)
        for key, value in [("status", "pending"), ("scenario_id", "T-009"),
                           ("release_scope", "core"), ("core_profile_qualified", True),
                           ("compatibility_qualified", True), ("evidence_class", "COMPAT")]:
            self.receipt11 = copy.deepcopy(proof)
            self.receipt11[key] = value
            self.save_receipt11()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_invalid_t011_proof(self):
        original = copy.deepcopy(self.receipt11)
        mutations = [(key, None) for key in ["native_source_descriptor_sha256",
                     "native_source_file_count", "initial_proof"]]
        mutations += [("native_source_file_count", True), ("native_source_file_count", 0),
                      ("native_source_descriptor_sha256", "bad"),
                      ("initial_proof", {"additional_runtime_credit": True}),
                      ("other_release_scenarios_qualified", ["T-012"])]
        for key, value in mutations:
            with self.subTest(field=key):
                self.receipt11 = copy.deepcopy(original)
                self.receipt11[key] = value
                self.save_receipt11()
                with self.assertRaises(ValueError):
                    self.check()

        for key, value in [("run", "../verification/scenario-test-123"),
                           ("run", "verification/feature-workspace-123"),
                           ("receipt_sha256", "bad"), ("source_snapshot_sha256", None)]:
            with self.subTest(initial_field=key):
                self.receipt11 = copy.deepcopy(original)
                self.receipt11["initial_proof"][key] = value
                self.save_receipt11()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t011_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt11)
        # Exercise the new command categories and each credit boundary.
        for index, key, value in [(0, "command", ["cargo", "test"]),
                (1, "command", ["cargo", "test"]), (13, "command", ["cargo", "test"]),
                (2, "command", ["cargo", "test"]), (3, "command", ["cargo", "test"]),
                (13, "runtime_credit", False), (14, "runtime_credit", True),
                (15, "command", self.receipt9["checks"][-2]["command"]),
                (16, "runtime_credit", True), (16, "metadata_only", False),
                (0, "metadata_only", True), (0, "reaped", False), (0, "timed_out", True),
                (0, "historical_result_transfer", True), (0, "added_source_files", ["new"]),
                (0, "removed_source_files", ["old"]),
                (0, "source_descriptor_sha256", "f" * 64), (0, "exit_code", 1),
                (0, "changed_source_files", [T011_FILE])]:
            with self.subTest(index=index, field=key):
                self.receipt11 = copy.deepcopy(original)
                self.receipt11["checks"][index][key] = value
                self.save_receipt11()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt11 = copy.deepcopy(original)
            self.receipt11["checks"] = checks
            self.save_receipt11()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t011_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt11)
        for key, value in [("file", T009_FILE), ("name", T009_NAME), ("sha256", "a" * 64)]:
            self.receipt11 = copy.deepcopy(original)
            self.receipt11["acceptance_test"][key] = value
            self.save_receipt11()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t011_source_and_missing_declaration(self):
        source = self.root / T011_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt11["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt11()
        with self.assertRaises(ValueError):
            self.check()

    def test_accepts_exact_t012_policy_and_distinct_descriptors(self):
        self.assertEqual(len(self.receipt12["checks"]), 18)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt12["checks"]), 15)
        self.assertNotIn("feature_validation", self.receipt12)
        self.assertNotEqual(self.receipt11["native_source_descriptor_sha256"],
                            self.receipt12["native_source_descriptor_sha256"])
        self.check()

    def test_rejects_missing_t012_receipt(self):
        (self.root / T012_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t012_row_and_receipt_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][11][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt12)
        for key, value in [("status", "pending"), ("scenario_id", "T-009"),
                           ("release_scope", "core"), ("core_profile_qualified", True),
                           ("compatibility_qualified", True), ("evidence_class", "COMPAT")]:
            self.receipt12 = copy.deepcopy(proof)
            self.receipt12[key] = value
            self.save_receipt12()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_invalid_t012_proof(self):
        original = copy.deepcopy(self.receipt12)
        mutations = [(key, None) for key in ["native_source_descriptor_sha256",
                     "native_source_file_count", "initial_proof"]]
        mutations += [("native_source_file_count", True), ("native_source_file_count", 0), ("record_version", True),
                      ("public_metadata_required_after_delivery", False),
                      ("native_source_descriptor_sha256", "bad"),
                      ("initial_proof", {"additional_runtime_credit": True}),
                      ("other_release_scenarios_qualified", ["T-013"])]
        for key, value in mutations:
            with self.subTest(field=key):
                self.receipt12 = copy.deepcopy(original)
                self.receipt12[key] = value
                self.save_receipt12()
                with self.assertRaises(ValueError):
                    self.check()

        for key, value in [("run", "../verification/scenario-test-123"),
                           ("run", "verification/feature-workspace-123"),
                           ("receipt_sha256", "bad"), ("receipt_sha256", None),
                           ("source_snapshot_sha256", "bad"), ("source_snapshot_sha256", None)]:
            with self.subTest(initial_field=key):
                self.receipt12 = copy.deepcopy(original)
                self.receipt12["initial_proof"][key] = value
                self.save_receipt12()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t012_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt12)
        # Exercise the new command categories and each credit boundary.
        for index, key, value in [(0, "command", ["cargo", "test"]),
                (1, "command", ["cargo", "test"]), (14, "command", ["cargo", "test"]),
                (2, "command", ["cargo", "test"]), (3, "command", ["cargo", "test"]),
                (14, "runtime_credit", False), (15, "runtime_credit", True),
                (16, "command", self.receipt9["checks"][-2]["command"]),
                (17, "runtime_credit", True), (17, "metadata_only", False),
                (0, "metadata_only", True), (0, "reaped", False), (0, "timed_out", True),
                (0, "historical_result_transfer", True), (0, "added_source_files", ["new"]),
                (0, "removed_source_files", ["old"]),
                (0, "source_descriptor_sha256", "f" * 64), (0, "exit_code", 1), (0, "exit_code", False),
                (0, "runtime_credit", 1), (17, "metadata_only", 1),
                (0, "changed_source_files", [T012_FILE])]:
            with self.subTest(index=index, field=key):
                self.receipt12 = copy.deepcopy(original)
                self.receipt12["checks"][index][key] = value
                self.save_receipt12()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt12 = copy.deepcopy(original)
            self.receipt12["checks"] = checks
            self.save_receipt12()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t012_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt12)
        for key, value in [("file", T009_FILE), ("name", T009_NAME), ("sha256", "a" * 64)]:
            self.receipt12 = copy.deepcopy(original)
            self.receipt12["acceptance_test"][key] = value
            self.save_receipt12()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t012_source_and_missing_declaration(self):
        source = self.root / T012_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt12["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt12()
        with self.assertRaises(ValueError):
            self.check()

    def test_accepts_exact_t013_policy_and_distinct_descriptors(self):
        self.assertEqual(len(self.receipt13["checks"]), 19)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt13["checks"]), 16)
        self.assertNotIn("feature_validation", self.receipt13)
        self.assertNotEqual(self.receipt12["native_source_descriptor_sha256"],
                            self.receipt13["native_source_descriptor_sha256"])
        self.assertNotEqual(self.receipt13["initial_proof"]["source_snapshot_sha256"],
                            self.receipt13["native_source_descriptor_sha256"])
        self.check()

    def test_rejects_missing_t013_receipt(self):
        (self.root / T013_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t013_row_and_receipt_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][12][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt13)
        for key, value in [("status", "pending"), ("scenario_id", "T-009"),
                           ("release_scope", "core"), ("core_profile_qualified", True),
                           ("compatibility_qualified", True), ("evidence_class", "COMPAT")]:
            self.receipt13 = copy.deepcopy(proof)
            self.receipt13[key] = value
            self.save_receipt13()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_invalid_t013_proof(self):
        original = copy.deepcopy(self.receipt13)
        mutations = [(key, None) for key in ["native_source_descriptor_sha256",
                     "native_source_file_count", "initial_proof"]]
        mutations += [("native_source_file_count", True), ("native_source_file_count", 0), ("record_version", True),
                      ("public_metadata_required_after_delivery", False),
                      ("native_source_descriptor_sha256", "bad"),
                      ("initial_proof", {"additional_runtime_credit": True}),
                      ("other_release_scenarios_qualified", ["T-014"])]
        for key, value in mutations:
            with self.subTest(field=key):
                self.receipt13 = copy.deepcopy(original)
                self.receipt13[key] = value
                self.save_receipt13()
                with self.assertRaises(ValueError):
                    self.check()

        for key, value in [("run", "../verification/scenario-test-123"),
                           ("run", "verification/feature-workspace-123"),
                           ("receipt_sha256", "bad"), ("receipt_sha256", None),
                           ("source_snapshot_sha256", "bad"), ("source_snapshot_sha256", None)]:
            with self.subTest(initial_field=key):
                self.receipt13 = copy.deepcopy(original)
                self.receipt13["initial_proof"][key] = value
                self.save_receipt13()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t013_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt13)
        # Exercise the new command categories and each credit boundary.
        for index, key, value in [(i, "command", ["cargo", "test"]) for i in range(19)] + [(0, "command", ["cargo", "test"]),
                (1, "command", ["cargo", "test"]), (15, "command", ["cargo", "test"]),
                (2, "command", ["cargo", "test"]), (3, "command", ["cargo", "test"]),
                (15, "runtime_credit", False), (16, "runtime_credit", True),
                (17, "command", self.receipt9["checks"][-2]["command"]),
                (18, "runtime_credit", True), (18, "metadata_only", False),
                (0, "metadata_only", True), (0, "reaped", False), (0, "timed_out", True),
                (0, "historical_result_transfer", True), (0, "added_source_files", ["new"]),
                (0, "removed_source_files", ["old"]),
                (0, "source_descriptor_sha256", "f" * 64), (0, "exit_code", 1), (0, "exit_code", False),
                (0, "runtime_credit", 1), (18, "metadata_only", 1),
                (0, "changed_source_files", [T013_FILE])]:
            with self.subTest(index=index, field=key):
                self.receipt13 = copy.deepcopy(original)
                self.receipt13["checks"][index][key] = value
                self.save_receipt13()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt13 = copy.deepcopy(original)
            self.receipt13["checks"] = checks
            self.save_receipt13()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t013_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt13)
        for key, value in [("file", T009_FILE), ("name", T009_NAME), ("sha256", "a" * 64)]:
            self.receipt13 = copy.deepcopy(original)
            self.receipt13["acceptance_test"][key] = value
            self.save_receipt13()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t013_source_and_missing_declaration(self):
        source = self.root / T013_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt13["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt13()
        with self.assertRaises(ValueError):
            self.check()

    def test_accepts_exact_t014_policy_and_distinct_descriptors(self):
        self.assertEqual(len(self.receipt14["checks"]), 20)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt14["checks"]), 17)
        self.assertNotIn("feature_validation", self.receipt14)
        self.assertNotEqual(self.receipt13["native_source_descriptor_sha256"],
                            self.receipt14["native_source_descriptor_sha256"])
        self.assertNotEqual(self.receipt14["initial_proof"]["source_snapshot_sha256"],
                            self.receipt14["native_source_descriptor_sha256"])
        self.check()

    def test_rejects_missing_t014_receipt(self):
        (self.root / T014_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t014_row_and_receipt_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][13][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt14)
        for key, value in [("status", "pending"), ("scenario_id", "T-009"),
                           ("release_scope", "core"), ("core_profile_qualified", True),
                           ("compatibility_qualified", True), ("evidence_class", "COMPAT")]:
            self.receipt14 = copy.deepcopy(proof)
            self.receipt14[key] = value
            self.save_receipt14()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_invalid_t014_proof(self):
        original = copy.deepcopy(self.receipt14)
        mutations = [(key, None) for key in ["native_source_descriptor_sha256",
                     "native_source_file_count", "initial_proof"]]
        mutations += [("native_source_file_count", True), ("native_source_file_count", 0), ("record_version", True),
                      ("public_metadata_required_after_delivery", False),
                      ("feature_validation", copy.deepcopy(self.receipt10["feature_validation"])),
                      ("native_source_descriptor_sha256", "bad"),
                      ("initial_proof", {"additional_runtime_credit": True}),
                      ("other_release_scenarios_qualified", ["T-015"])]
        for key, value in mutations:
            with self.subTest(field=key):
                self.receipt14 = copy.deepcopy(original)
                self.receipt14[key] = value
                self.save_receipt14()
                with self.assertRaises(ValueError):
                    self.check()

        for key, value in [("run", "../verification/scenario-test-123"),
                           ("run", "verification/feature-workspace-123"),
                           ("receipt_sha256", "bad"), ("receipt_sha256", None),
                           ("source_snapshot_sha256", original["native_source_descriptor_sha256"]),
                           ("source_snapshot_sha256", "bad"), ("source_snapshot_sha256", None)]:
            with self.subTest(initial_field=key):
                self.receipt14 = copy.deepcopy(original)
                self.receipt14["initial_proof"][key] = value
                self.save_receipt14()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t014_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt14)
        # Exercise the new command categories and each credit boundary.
        for index, key, value in [(i, "command", ["cargo", "test"]) for i in range(20)] + [(0, "command", ["cargo", "test"]),
                (1, "command", ["cargo", "test"]), (15, "command", ["cargo", "test"]),
                (2, "command", ["cargo", "test"]), (3, "command", ["cargo", "test"]),
                (16, "runtime_credit", False), (17, "runtime_credit", True),
                (18, "command", self.receipt9["checks"][-2]["command"]),
                (19, "command", ["python3", "-B", "scripts/test_core_release_traceability.py"]),
                (19, "runtime_credit", True), (19, "metadata_only", False),
                (0, "metadata_only", True), (0, "reaped", False), (0, "timed_out", True),
                (0, "historical_result_transfer", True), (0, "added_source_files", ["new"]),
                (0, "removed_source_files", ["old"]),
                (0, "source_descriptor_sha256", "f" * 64), (0, "exit_code", 1), (0, "exit_code", False),
                (0, "runtime_credit", 1), (19, "metadata_only", 1),
                (0, "changed_source_files", [T014_FILE])]:
            with self.subTest(index=index, field=key):
                self.receipt14 = copy.deepcopy(original)
                self.receipt14["checks"][index][key] = value
                self.save_receipt14()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt14 = copy.deepcopy(original)
            self.receipt14["checks"] = checks
            self.save_receipt14()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t014_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt14)
        for key, value in [("file", T009_FILE), ("name", T009_NAME), ("sha256", "a" * 64)]:
            self.receipt14 = copy.deepcopy(original)
            self.receipt14["acceptance_test"][key] = value
            self.save_receipt14()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t014_source_and_missing_declaration(self):
        source = self.root / T014_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt14["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt14()
        with self.assertRaises(ValueError):
            self.check()

    def test_accepts_exact_t015_policy_and_distinct_own_maps(self):
        self.assertEqual(len(self.receipt15["checks"]), 22)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt15["checks"]), 19)
        self.assertNotIn("feature_validation", self.receipt15)
        maps = {self.receipt15["native_source_descriptor_sha256"],
                self.receipt15["initial_proof"]["source_snapshot_sha256"],
                self.receipt15["pre_fix_proof"]["source_descriptor_sha256"]}
        self.assertEqual(len(maps), 3)
        self.check()

    def test_rejects_missing_t015_proof(self):
        (self.root / T015_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t015_identity_and_qualification_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(row_field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][14][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt15)
        for key, value in [("record_version", True), ("status", "pending"),
                           ("scenario_id", "T-014"), ("release_scope", "core"),
                           ("evidence_class", "COMPAT"), ("native_source_file_count", True),
                           ("core_profile_qualified", True), ("compatibility_qualified", True),
                           ("other_release_scenarios_qualified", ["T-014"]),
                           ("public_metadata_required_after_delivery", False),
                           ("feature_validation", copy.deepcopy(self.receipt10["feature_validation"]))]:
            with self.subTest(receipt_field=key):
                self.receipt15 = copy.deepcopy(proof)
                self.receipt15[key] = value
                self.save_receipt15()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_untyped_or_aliased_t015_initial_and_pre_fix_proofs(self):
        original = copy.deepcopy(self.receipt15)
        initial_changes = [("run", "../verification/scenario-test-456"),
                           ("run", "verification/feature-workspace-456"),
                           ("receipt_sha256", "bad"),
                           ("source_snapshot_sha256", original["native_source_descriptor_sha256"]),
                           ("source_snapshot_sha256", "bad"),
                           ("additional_runtime_credit", True)]
        for key, value in initial_changes:
            with self.subTest(initial_field=key):
                self.receipt15 = copy.deepcopy(original)
                self.receipt15["initial_proof"][key] = value
                self.save_receipt15()
                with self.assertRaises(ValueError):
                    self.check()
        pre_fix_changes = [("run", "../verification/scenario-test-123"),
                           ("run", "verification/guard-tests-123"),
                           ("receipt_sha256", "bad"), ("log_sha256", None),
                           ("source_descriptor_sha256", original["native_source_descriptor_sha256"]),
                           ("source_descriptor_sha256", "bad"), ("exit_code", True),
                           ("exit_code", 0), ("expected_failure", "compile_failure"),
                           ("additional_runtime_credit", True)]
        for key, value in pre_fix_changes:
            with self.subTest(pre_fix_field=key):
                self.receipt15 = copy.deepcopy(original)
                self.receipt15["pre_fix_proof"][key] = value
                self.save_receipt15()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t015_command_order_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt15)
        for index, key, value in ([(i, "command", ["cargo", "test"]) for i in range(22)] +
                                  [(18, "runtime_credit", False), (19, "runtime_credit", True),
                                   (21, "runtime_credit", True), (21, "metadata_only", False),
                                   (0, "metadata_only", True), (0, "reaped", False),
                                   (0, "timed_out", True), (0, "historical_result_transfer", True),
                                   (0, "changed_source_files", [T015_FILE]),
                                   (0, "added_source_files", ["new"]),
                                   (0, "removed_source_files", ["old"]),
                                   (0, "source_descriptor_sha256", "ab" * 32),
                                   (0, "exit_code", 1), (0, "exit_code", False),
                                   (0, "runtime_credit", 1), (21, "metadata_only", 1)]):
            with self.subTest(index=index, field=key):
                self.receipt15 = copy.deepcopy(original)
                self.receipt15["checks"][index][key] = value
                self.save_receipt15()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt15 = copy.deepcopy(original)
            self.receipt15["checks"] = checks
            self.save_receipt15()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t015_wrong_acceptance_path_name_or_hash(self):
        original = copy.deepcopy(self.receipt15)
        for key, value in [("file", T014_FILE), ("name", T014_NAME), ("sha256", "cd" * 32)]:
            with self.subTest(field=key):
                self.receipt15 = copy.deepcopy(original)
                self.receipt15["acceptance_test"][key] = value
                self.save_receipt15()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_changed_t015_source_and_missing_declaration(self):
        source = self.root / T015_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt15["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt15()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_t009_receipt(self):
        (self.root / T009_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t009_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][8][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_t009_proof(self):
        original = copy.deepcopy(self.receipt9)
        mutations = [(key, None) for key in original]
        mutations += [("status", "pending"), ("scenario_id", "T-001"),
                      ("release_scope", "core"), ("evidence_class", "COMPAT"),
                      ("native_source_descriptor_sha256", "invalid"),
                      ("native_source_file_count", True), ("core_profile_qualified", True),
                      ("compatibility_qualified", True),
                      ("other_release_scenarios_qualified", ["T-001"]),
                      ("initial_proof", {"additional_runtime_credit": True})]
        for key, value in mutations:
            with self.subTest(field=key, value=value):
                self.receipt9 = copy.deepcopy(original)
                self.receipt9[key] = value
                self.save_receipt9()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_t009_check_drift_and_inflated_credit(self):
        original = copy.deepcopy(self.receipt9)
        mutations = [("exit_code", 1), ("exit_code", False), ("reaped", False),
                     ("timed_out", True), ("historical_result_transfer", True),
                     ("changed_source_files", [T009_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("source_descriptor_sha256", "e" * 64),
                     ("command", ["cargo", "test"])]
        for index in range(13):
            for key, value in mutations + [("runtime_credit", index >= 10)]:
                with self.subTest(index=index, field=key):
                    self.receipt9 = copy.deepcopy(original)
                    self.receipt9["checks"][index][key] = value
                    self.save_receipt9()
                    with self.assertRaises(ValueError):
                        self.check()
        for checks in [original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]]:
            self.receipt9 = copy.deepcopy(original)
            self.receipt9["checks"] = checks
            self.save_receipt9()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t009_wrong_path_or_hash(self):
        original = copy.deepcopy(self.receipt9)
        for key, value in [("file", TEST_FILE), ("name", TEST_NAME), ("sha256", "a" * 64)]:
            self.receipt9 = copy.deepcopy(original)
            self.receipt9["acceptance_test"][key] = value
            self.save_receipt9()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_changed_t009_source_and_missing_declaration(self):
        source = self.root / T009_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt9["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt9()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_old_eight_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][8] = {
            "id": "T-009", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_old_three_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][3] = {
            "id": "T-004", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_old_two_case_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][2] = {
            "id": "T-003", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_all_pending_reversion(self):
        self.requirements["release_acceptance_inventory"][0] = {
            "id": "T-001", "implementation_status": "pending", "verification_status": "not_run",
            "acceptance_test_status": "future_release_scenario"}
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_another_scenario_promotion(self):
        self.requirements["release_acceptance_inventory"][18]["implementation_status"] = "implemented"
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_duplicate_or_reordered_inventory(self):
        original = copy.deepcopy(self.requirements)
        for inventory in [original["release_acceptance_inventory"][:-1],
                          [original["release_acceptance_inventory"][0]] * 56,
                          list(reversed(original["release_acceptance_inventory"]))]:
            with self.subTest(inventory=inventory[0]["id"]):
                self.requirements["release_acceptance_inventory"] = inventory
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_wrong_t001_fields(self):
        original = copy.deepcopy(self.requirements)
        for key in ["classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"]:
            with self.subTest(field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][0][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_missing_receipt(self):
        (self.root / RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_proof_fields(self):
        original = copy.deepcopy(self.receipt)
        for key in original:
            with self.subTest(field=key):
                self.receipt = copy.deepcopy(original)
                del self.receipt[key]
                self.save_receipt()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_wrong_receipt_scope_and_broad_credit(self):
        original = copy.deepcopy(self.receipt)
        for key, value in [("status", "pending"), ("scenario_id", "T-002"),
                           ("evidence_class", "COMPAT"), ("release_scope", "core"),
                           ("core_profile_qualified", True), ("compatibility_qualified", True),
                           ("other_release_scenarios_qualified", ["T-002"]),
                           ("initial_proof", {"additional_runtime_credit": True})]:
            with self.subTest(field=key):
                self.receipt = copy.deepcopy(original)
                self.receipt[key] = value
                self.save_receipt()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_missing_or_unsuccessful_current_checks(self):
        original = copy.deepcopy(self.receipt)
        mutations = [("exit_code", 1), ("reaped", False), ("timed_out", True),
                     ("changed_source_files", [TEST_FILE]), ("added_source_files", ["new"]),
                     ("removed_source_files", ["old"]), ("historical_result_transfer", True),
                     ("source_descriptor_sha256", "b" * 64), ("runtime_credit", False),
                     ("command", ["cargo", "test"])]
        for key, value in mutations:
            with self.subTest(field=key):
                self.receipt = copy.deepcopy(original)
                self.receipt["checks"][0][key] = value
                self.save_receipt()
                with self.assertRaises(ValueError):
                    self.check()
        self.receipt = copy.deepcopy(original)
        self.receipt["checks"].pop()
        self.save_receipt()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_guard_runtime_credit(self):
        self.receipt["checks"][-1]["runtime_credit"] = True
        self.save_receipt()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_changed_test_source_even_with_named_declaration(self):
        with (self.root / TEST_FILE).open("a") as source:
            source.write("// altered\n")
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_missing_test_declaration_even_with_matching_hash(self):
        (self.root / TEST_FILE).write_text("// no acceptance test\n")
        self.receipt["acceptance_test"]["sha256"] = hashlib.sha256(
            (self.root / TEST_FILE).read_bytes()).hexdigest()
        self.save_receipt()
        with self.assertRaises(ValueError):
            self.check()


    def test_accepts_exact_t016_policy_and_distinct_own_maps(self):
        self.assertEqual(len(self.receipt16["checks"]), 23)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt16["checks"]), 20)
        self.assertNotIn("feature_validation", self.receipt16)
        self.assertNotEqual(self.receipt16["native_source_descriptor_sha256"],
                            self.receipt16["initial_proof"]["source_snapshot_sha256"])
        self.check()

    def test_rejects_missing_t016_receipt(self):
        (self.root / T016_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t016_identity_qualification_and_broad_credit(self):
        original = copy.deepcopy(self.requirements)
        for key in ("classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"):
            with self.subTest(row_field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][15][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt16)
        for key, value in (("record_version", True), ("status", "pending"),
                           ("scenario_id", "T-015"), ("release_scope", "core"),
                           ("evidence_class", "COMPAT"), ("native_source_file_count", True),
                           ("core_profile_qualified", True), ("compatibility_qualified", True),
                           ("other_release_scenarios_qualified", ["T-015"]),
                           ("public_metadata_required_after_delivery", False),
                           ("feature_validation", {"additional_runtime_credit": True})):
            with self.subTest(receipt_field=key):
                self.receipt16 = copy.deepcopy(proof)
                self.receipt16[key] = value
                self.save_receipt16()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_or_aliased_t016_initial_proof_and_maps(self):
        original = copy.deepcopy(self.receipt16)
        for key, value in (("run", "../verification/scenario-test-789"),
                           ("run", "verification/guard-tests-789"),
                           ("receipt_sha256", "bad"),
                           ("source_snapshot_sha256", "bad"),
                           ("source_snapshot_sha256", original["native_source_descriptor_sha256"]),
                           ("additional_runtime_credit", True)):
            with self.subTest(field=key, value=value):
                self.receipt16 = copy.deepcopy(original)
                self.receipt16["initial_proof"][key] = value
                self.save_receipt16()
                with self.assertRaises(ValueError):
                    self.check()
        self.receipt16 = copy.deepcopy(original)
        self.receipt16["native_source_descriptor_sha256"] = "bad"
        self.save_receipt16()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_t016_command_order_process_drift_metadata_and_credit(self):
        original = copy.deepcopy(self.receipt16)
        mutations = ([(i, "command", ["cargo", "test"]) for i in range(23)] +
                     [(19, "runtime_credit", False), (20, "runtime_credit", True),
                      (22, "runtime_credit", True), (22, "metadata_only", False),
                      (0, "metadata_only", True), (0, "reaped", False),
                      (0, "timed_out", True), (0, "historical_result_transfer", True),
                      (0, "changed_source_files", [T016_FILE]),
                      (0, "added_source_files", ["new"]),
                      (0, "removed_source_files", ["old"]),
                      (0, "source_descriptor_sha256", "ab" * 32),
                      (0, "exit_code", 1), (0, "exit_code", False),
                      (0, "runtime_credit", 1), (22, "metadata_only", 1)])
        for index, key, value in mutations:
            with self.subTest(index=index, field=key):
                self.receipt16 = copy.deepcopy(original)
                self.receipt16["checks"][index][key] = value
                self.save_receipt16()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in (original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]):
            self.receipt16 = copy.deepcopy(original)
            self.receipt16["checks"] = checks
            self.save_receipt16()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t016_wrong_acceptance_path_name_or_hash(self):
        original = copy.deepcopy(self.receipt16)
        for key, value in (("file", T015_FILE), ("name", T015_NAME), ("sha256", "cd" * 32)):
            with self.subTest(field=key):
                self.receipt16 = copy.deepcopy(original)
                self.receipt16["acceptance_test"][key] = value
                self.save_receipt16()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_changed_t016_source_and_missing_declaration(self):
        source = self.root / T016_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt16["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt16()
        with self.assertRaises(ValueError):
            self.check()


    def test_accepts_exact_t017_policy_and_distinct_own_maps(self):
        self.assertEqual(len(self.receipt17["checks"]), 24)
        self.assertEqual(sum(c["runtime_credit"] for c in self.receipt17["checks"]), 21)
        self.assertNotIn("feature_validation", self.receipt17)
        self.assertNotEqual(self.receipt17["native_source_descriptor_sha256"],
                            self.receipt17["initial_proof"]["source_snapshot_sha256"])
        self.check()

    def test_rejects_missing_t017_receipt(self):
        (self.root / T017_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t017_identity_qualification_and_broad_credit(self):
        original = copy.deepcopy(self.requirements)
        for key in ("classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt", "acceptance_tests"):
            with self.subTest(row_field=key):
                self.requirements = copy.deepcopy(original)
                self.requirements["release_acceptance_inventory"][16][key] = "wrong"
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements = original
        proof = copy.deepcopy(self.receipt17)
        for key, value in (("record_version", True), ("status", "pending"),
                           ("scenario_id", "T-016"), ("release_scope", "core"),
                           ("evidence_class", "COMPAT"), ("native_source_file_count", True),
                           ("core_profile_qualified", True), ("compatibility_qualified", True),
                           ("other_release_scenarios_qualified", ["T-016"]),
                           ("public_metadata_required_after_delivery", False),
                           ("feature_validation", {"additional_runtime_credit": True})):
            with self.subTest(receipt_field=key):
                self.receipt17 = copy.deepcopy(proof)
                self.receipt17[key] = value
                self.save_receipt17()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_invalid_or_aliased_t017_initial_proof_and_maps(self):
        original = copy.deepcopy(self.receipt17)
        for key, value in (("run", "../verification/scenario-test-790"),
                           ("run", "verification/guard-tests-790"),
                           ("receipt_sha256", "bad"), ("source_snapshot_sha256", "bad"),
                           ("source_snapshot_sha256", original["native_source_descriptor_sha256"]),
                           ("additional_runtime_credit", True)):
            with self.subTest(field=key, value=value):
                self.receipt17 = copy.deepcopy(original)
                self.receipt17["initial_proof"][key] = value
                self.save_receipt17()
                with self.assertRaises(ValueError):
                    self.check()
        self.receipt17 = copy.deepcopy(original)
        self.receipt17["native_source_descriptor_sha256"] = "bad"
        self.save_receipt17()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_t017_command_process_drift_metadata_and_credit(self):
        original = copy.deepcopy(self.receipt17)
        mutations = ([(i, "command", ["cargo", "test"]) for i in range(24)] +
                     [(20, "runtime_credit", False), (21, "runtime_credit", True),
                      (23, "runtime_credit", True), (23, "metadata_only", False),
                      (0, "metadata_only", True), (0, "reaped", False),
                      (0, "timed_out", True), (0, "historical_result_transfer", True),
                      (0, "changed_source_files", [T017_FILE]),
                      (0, "added_source_files", ["new"]),
                      (0, "removed_source_files", ["old"]),
                      (0, "source_descriptor_sha256", "ab" * 32),
                      (0, "exit_code", 1), (0, "exit_code", False),
                      (0, "runtime_credit", 1), (23, "metadata_only", 1),
                      (0, "pid", -1), (0, "process_group", 0),
                      (0, "process_identity_verified", False),
                      (0, "process_group_retired", False),
                      (0, "start_new_session", False)])
        for index, key, value in mutations:
            with self.subTest(index=index, field=key):
                self.receipt17 = copy.deepcopy(original)
                self.receipt17["checks"][index][key] = value
                self.save_receipt17()
                with self.assertRaises(ValueError):
                    self.check()
        for checks in (original["checks"][:-1], list(reversed(original["checks"])),
                       original["checks"] + [original["checks"][0]]):
            self.receipt17 = copy.deepcopy(original)
            self.receipt17["checks"] = checks
            self.save_receipt17()
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t017_wrong_acceptance_path_name_or_hash(self):
        original = copy.deepcopy(self.receipt17)
        for key, value in (("file", T016_FILE), ("name", T016_NAME), ("sha256", "cd" * 32)):
            with self.subTest(field=key):
                self.receipt17 = copy.deepcopy(original)
                self.receipt17["acceptance_test"][key] = value
                self.save_receipt17()
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_changed_t017_source_and_missing_declaration(self):
        source = self.root / T017_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        self.receipt17["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_receipt17()
        with self.assertRaises(ValueError):
            self.check()

    def prepare_t018_durable_disclosure_policy(self):
        source = self.root / T018_FILE
        source.write_text(f"#[test]\nfn {T018_NAME}() {{}}\n")
        row = copy.deepcopy(self.requirements["release_acceptance_inventory"][16])
        row.update(id="T-018", release_scope=T018_SCOPE,
                   acceptance_tests=[{"file": T018_FILE, "name": T018_NAME}],
                   verification_receipt=T018_RECEIPT)
        self.requirements["release_acceptance_inventory"][17] = row
        self.assertEqual(row["classification"], "NATIVE")
        self.assertEqual(row["release_scope"], T018_SCOPE)
        self.assertEqual(row["acceptance_tests"], [{"file": T018_FILE, "name": T018_NAME}])
        self.assertEqual(row["verification_receipt"], T018_RECEIPT)

        cargo = "cargo"
        python = "/Users/blove/.pyenv/versions/3.12.12/bin/python3"
        test_command = lambda package, target: [
            cargo, "test", "-p", package, "--test", target, "--locked", "--offline",
            "--", "--test-threads=1"]
        commands = [
            ("scenario-test", [cargo, "test", "-p", "b4-http", "--test",
                               "approval_durable_disclosure_tcp", T018_NAME, "--locked",
                               "--offline", "--", "--nocapture", "--test-threads=1"]),
            ("durable-disclosure-tests", test_command("b4-http", "approval_durable_disclosure_tcp")),
            ("approval-http-tests", test_command("b4-http", "approval_tcp")),
            ("approval-sqlite-tests", test_command("b4-sqlite", "approvals")),
            ("native-stream-api-tests", test_command("b4-runtime", "native_streaming")),
            ("native-stream-control-tests", test_command("b4-http", "native_streaming")),
            ("http-control-tests", [cargo, "test", "-p", "b4-http", "--lib", "--locked",
                                    "--offline", "--", "--test-threads=1"]),
        ]
        regressions = (
            "native_tcp", "thread_state_tcp", "thread_admission_tcp", "thread_route_tcp",
            "thread_json_tcp", "input_shape_tcp", "thread_concurrency_tcp",
            "different_thread_tcp", "wait_output_tcp", "stream_payload_tcp",
            "terminal_frame_tcp", "disconnect_completion_tcp", "attachment_state_tcp",
            "attachment_cut_tcp", "attachment_truncation_tcp", "viewer_overflow_tcp",
            "heartbeat_splits_tcp")
        commands.extend((f"t{number:03d}-regression", test_command("b4-http", target))
                        for number, target in enumerate(regressions, 1))
        commands.extend((
            ("format-check", [cargo, "fmt", "--all", "--", "--check"]),
            ("scoped-lint", [cargo, "clippy", "-p", "b4-runtime", "-p", "b4-http",
                             "-p", "b4-sqlite", "--all-targets", "--locked", "--offline",
                             "--", "-D", "warnings"]),
            ("guard-tests", [python, "-B", "scripts/test_core_release_traceability.py"]),
        ))
        descriptor = "22" * 32
        receipt = copy.deepcopy(self.receipt17)
        receipt.update(scenario_id="T-018", release_scope=T018_SCOPE,
                       native_source_descriptor_sha256=descriptor,
                       native_source_file_count=1200,
                       acceptance_test={"file": T018_FILE, "name": T018_NAME,
                                        "sha256": hashlib.sha256(source.read_bytes()).hexdigest()},
                       initial_proof={"run": "verification/scenario-test-791",
                                      "receipt_sha256": "23" * 32,
                                      "source_snapshot_sha256": "24" * 32,
                                      "additional_runtime_credit": False})
        receipt["checks"] = []
        for index, (step, command) in enumerate(commands):
            check = copy.deepcopy(self.receipt17["checks"][0])
            check.update(step=step, command=command, source_descriptor_sha256=descriptor,
                         runtime_credit=index < 24, metadata_only=step == "guard-tests")
            receipt["checks"].append(check)
        self.assertEqual(len(receipt["checks"]), 27)
        self.assertEqual([check["step"] for check in receipt["checks"]],
                         [step for step, _ in commands])
        self.assertEqual([check["command"] for check in receipt["checks"]],
                         [command for _, command in commands])
        self.assertEqual(sum(check["runtime_credit"] for check in receipt["checks"]), 24)
        self.assertEqual([check["runtime_credit"] for check in receipt["checks"]],
                         [index < 24 for index in range(27)])
        self.assertNotEqual(receipt["native_source_descriptor_sha256"],
                            receipt["initial_proof"]["source_snapshot_sha256"])
        self.assertFalse(receipt["initial_proof"]["additional_runtime_credit"])
        self.assertNotIn("feature_validation", receipt)
        path = self.root / T018_RECEIPT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(receipt))
        return receipt

    def save_t018_receipt(self, receipt):
        (self.root / T018_RECEIPT).write_text(json.dumps(receipt))

    def test_accepts_exact_t018_durable_disclosure_policy(self):
        self.assertEqual(len(self.receipt18["checks"]), 27)
        self.check()

    def test_rejects_missing_t018_receipt(self):
        (self.root / T018_RECEIPT).unlink()
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_wrong_t018_identity_scope_and_broad_credit(self):
        receipt = self.receipt18
        original_row = copy.deepcopy(self.requirements["release_acceptance_inventory"][17])
        for key in ("classification", "implementation_status", "verification_status",
                    "acceptance_test_status", "release_scope", "verification_receipt",
                    "acceptance_tests"):
            with self.subTest(row_field=key):
                row = copy.deepcopy(original_row)
                row[key] = "wrong"
                self.requirements["release_acceptance_inventory"][17] = row
                with self.assertRaises(ValueError):
                    self.check()
        self.requirements["release_acceptance_inventory"][17] = original_row
        for key, value in (("record_version", True), ("status", "pending"),
                           ("scenario_id", "T-017"), ("release_scope", "core"),
                           ("evidence_class", "COMPAT"), ("native_source_file_count", True),
                           ("core_profile_qualified", True), ("compatibility_qualified", True),
                           ("other_release_scenarios_qualified", ["T-017"]),
                           ("public_metadata_required_after_delivery", False),
                           ("feature_validation", {"additional_runtime_credit": True})):
            with self.subTest(receipt_field=key):
                changed = copy.deepcopy(receipt)
                changed[key] = value
                self.save_t018_receipt(changed)
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_aliased_t018_initial_and_final_maps(self):
        receipt = self.receipt18
        for key, value in (("run", "../verification/scenario-test-791"),
                           ("run", "verification/guard-tests-791"),
                           ("receipt_sha256", "bad"), ("source_snapshot_sha256", "bad"),
                           ("source_snapshot_sha256", receipt["native_source_descriptor_sha256"]),
                           ("additional_runtime_credit", True)):
            with self.subTest(field=key, value=value):
                changed = copy.deepcopy(receipt)
                changed["initial_proof"][key] = value
                self.save_t018_receipt(changed)
                with self.assertRaises(ValueError):
                    self.check()
        changed = copy.deepcopy(receipt)
        changed["native_source_descriptor_sha256"] = "bad"
        self.save_t018_receipt(changed)
        with self.assertRaises(ValueError):
            self.check()

    def test_rejects_t018_command_process_drift_and_credit(self):
        receipt = self.receipt18
        mutations = ([(index, "command", ["cargo", "test"]) for index in range(27)] +
                     [(23, "runtime_credit", False), (24, "runtime_credit", True),
                      (25, "runtime_credit", True), (26, "runtime_credit", True),
                      (26, "metadata_only", False), (0, "metadata_only", True),
                      (0, "reaped", False), (0, "timed_out", True),
                      (0, "historical_result_transfer", True),
                      (0, "changed_source_files", [T018_FILE]),
                      (0, "added_source_files", ["new"]),
                      (0, "removed_source_files", ["old"]),
                      (0, "source_descriptor_sha256", "ab" * 32),
                      (0, "exit_code", 1), (0, "exit_code", False),
                      (0, "runtime_credit", 1), (26, "metadata_only", 1),
                      (0, "pid", -1), (0, "process_group", 0),
                      (0, "process_identity_verified", False),
                      (0, "process_group_retired", False),
                      (0, "start_new_session", False)])
        for index, key, value in mutations:
            with self.subTest(index=index, field=key):
                changed = copy.deepcopy(receipt)
                changed["checks"][index][key] = value
                self.save_t018_receipt(changed)
                with self.assertRaises(ValueError):
                    self.check()
        for checks in (receipt["checks"][:-1], list(reversed(receipt["checks"])),
                       receipt["checks"] + [receipt["checks"][0]]):
            changed = copy.deepcopy(receipt)
            changed["checks"] = checks
            self.save_t018_receipt(changed)
            with self.assertRaises(ValueError):
                self.check()

    def test_rejects_t018_wrong_acceptance_path_name_or_hash(self):
        receipt = self.receipt18
        for key, value in (("file", T017_FILE), ("name", T017_NAME), ("sha256", "cd" * 32)):
            with self.subTest(field=key):
                changed = copy.deepcopy(receipt)
                changed["acceptance_test"][key] = value
                self.save_t018_receipt(changed)
                with self.assertRaises(ValueError):
                    self.check()

    def test_rejects_changed_t018_source_and_missing_declaration(self):
        receipt = copy.deepcopy(self.receipt18)
        source = self.root / T018_FILE
        source.write_text(source.read_text() + "// altered\n")
        with self.assertRaises(ValueError):
            self.check()
        source.write_text("// missing test declaration\n")
        receipt["acceptance_test"]["sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        self.save_t018_receipt(receipt)
        with self.assertRaises(ValueError):
            self.check()


if __name__ == "__main__":
    unittest.main()
