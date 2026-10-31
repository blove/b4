#![cfg(unix)]

mod inspection_support;

use b4_runtime::{Store, StoreError};
use b4_sqlite::SqliteStore;
use inspection_support::*;
use serde_json::{json, Value};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::process::Stdio;
use std::time::Duration;

fn static_failure(output: &Captured, code: i32, diagnostic: &str) {
    assert!(
        output.status.code() == Some(code),
        "inspection refusal must use the exact public exit code"
    );
    assert!(
        output.stdout.is_empty(),
        "inspection refusal must emit no report"
    );
    assert!(
        output.stderr == format!("{diagnostic}\n").as_bytes(),
        "inspection refusal must emit exactly the static public diagnostic"
    );
}

fn unchanged_refusal(data: &std::path::Path, sandbox: &Sandbox, code: i32, diagnostic: &str) {
    let before = witness(data);
    let output = run(command(data).arg("inspect"), sandbox);
    static_failure(&output, code, diagnostic);
    assert!(
        before == witness(data),
        "refusal must preserve source bytes, entries, inodes, modes and mtimes"
    );
}

#[test]
fn rich_report_counts_all_recorded_states_without_disclosing_authority() {
    let sandbox = Sandbox::new();
    let fixture = rich_fixture(&sandbox);
    let before = witness(&fixture.data);
    let epoch = owner_epoch(&fixture.data);
    for blocked in [false, true] {
        let mut cmd = command(&fixture.data);
        cmd.arg("inspect");
        if blocked {
            cmd.arg("--blocked");
        }
        let output = run(&mut cmd, &sandbox);
        successful_report(&output, &rich_report(blocked));
        let text = std::str::from_utf8(&output.stdout).unwrap();
        assert!(
            fixture.secrets.iter().all(|secret| !text.contains(secret)),
            "report must redact all real fixture identities, values, tokens and authority digests"
        );
        assert!(
            before == witness(&fixture.data),
            "inspection must preserve exact source bytes, entries, identities, modes and mtimes"
        );
        assert!(
            epoch == owner_epoch(&fixture.data),
            "inspection must preserve recorded owner epoch"
        );
    }
}

#[test]
fn stopped_store_report() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-state");
    let output = run(command(&data).arg("inspect"), &sandbox);
    if output.status.code() == Some(2)
        && output.stdout.is_empty()
        && output.stderr == b"b4run_invalid_arguments\n"
    {
        eprintln!("observed existing usage refusal: exit 2, empty stdout, b4run_invalid_arguments");
    }
    successful_report(&output, &empty_report());
}

#[test]
fn report_counts_terminal_history_after_a_new_active_admission() {
    let sandbox = Sandbox::new();
    let data = terminal_history_fixture(&sandbox);
    let mut expected = empty_report();
    expected["table_rows"]["threads"] = json!(1);
    expected["table_rows"]["executions"] = json!(2);
    expected["table_rows"]["journal"] = json!(4);
    expected["executions_by_status"]["accepted"] = json!(1);
    expected["executions_by_status"]["completed"] = json!(1);
    expected["declared_definition_tasks_by_recovery_policy"]["pure"] = json!(1);
    successful_report(&run(command(&data).arg("inspect"), &sandbox), &expected);
    expected["blocked"] = accepted_report(1, true)["blocked"].clone();
    successful_report(
        &run(command(&data).args(["inspect", "--blocked"]), &sandbox),
        &expected,
    );
}

#[test]
fn recorded_running_failure_drain_is_selected_with_uncertain_declared_policy() {
    let sandbox = Sandbox::new();
    let data = failure_drain_fixture(&sandbox);
    let mut expected = empty_report();
    expected["table_rows"]["threads"] = json!(1);
    expected["table_rows"]["executions"] = json!(1);
    expected["table_rows"]["journal"] = json!(6);
    expected["executions_by_status"]["running"] = json!(1);
    expected["initialized_tasks_by_status"]["failed"] = json!(1);
    expected["initialized_tasks_by_status"]["in_flight"] = json!(1);
    expected["declared_definition_tasks_by_recovery_policy"]["unsafe"] = json!(1);
    expected["declared_definition_tasks_by_recovery_policy"]["reconcilable"] = json!(1);
    expected["recorded_uncertain_tasks_by_policy"]["reconcilable"] = json!(1);
    expected["blocked"] = json!({
        "selection": "recorded_unfinished_or_uncertain",
        "rows": [{
            "ordinal": 1, "status": "running", "recorded_recovery_blocked": false,
            "recorded_approval_park": false, "has_uncertain_task": true,
            "initialized_tasks_by_status": expected["initialized_tasks_by_status"],
            "declared_definition_tasks_by_recovery_policy": expected["declared_definition_tasks_by_recovery_policy"]
        }]
    });
    successful_report(
        &run(command(&data).args(["inspect", "--blocked"]), &sandbox),
        &expected,
    );
}

#[test]
fn two_stopped_inspections_preserve_bytes_epoch_modes_mtimes_and_entries() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-state");
    let before = witness(&data);
    let epoch = owner_epoch(&data);
    let mut first = None;
    for _ in 0..2 {
        let output = run(command(&data).arg("inspect"), &sandbox);
        successful_report(&output, &empty_report());
        if let Some(bytes) = &first {
            assert!(
                bytes == &output.stdout,
                "unchanged source must produce identical complete public reports"
            );
        }
        first = Some(output.stdout);
        assert!(
            before == witness(&data),
            "inspection must preserve source bytes and physical metadata"
        );
        assert!(
            epoch == owner_epoch(&data),
            "inspection must not increment owner epoch"
        );
    }
}

#[test]
fn live_real_writer_refuses_inspection_without_mutating_source() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-state");
    let writer = SqliteStore::open(&data).unwrap();
    let before = witness(&data);
    let epoch = writer.owner_generation();
    let output = run(command(&data).args(["inspect", "--blocked"]), &sandbox);
    static_failure(&output, 1, "b4run_storage_owned");
    assert!(
        before == witness(&data),
        "live owner refusal must preserve exact real writer state"
    );
    assert!(
        writer.owner_generation() == epoch,
        "inspection must not advance a real writer epoch"
    );
    writer.readiness().unwrap();
}

#[test]
fn inspection_ignores_token_port_and_shutdown_configuration_without_listener() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-state");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    for token in [
        OsString::from("private invalid token\n"),
        OsString::from_vec(vec![0xff]),
    ] {
        let output = run(
            command(&data)
                .arg("inspect")
                .env("B4RUN_TOKEN", token)
                .env("B4RUN_PORT", "private-port-secret")
                .env("B4RUN_SHUTDOWN_WAIT_MS", OsString::from_vec(vec![0xfe])),
            &sandbox,
        );
        successful_report(&output, &empty_report());
    }
    let output = run(
        command(&data).arg("inspect").env(
            "B4RUN_PORT",
            listener.local_addr().unwrap().port().to_string(),
        ),
        &sandbox,
    );
    successful_report(&output, &empty_report());
}

#[test]
fn inspection_uses_existing_default_directory_without_token() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store(".b4run-data");
    let output = run(
        command(&data)
            .arg("inspect")
            .env_remove("B4RUN_DATA_DIR")
            .current_dir(&sandbox.0),
        &sandbox,
    );
    successful_report(&output, &empty_report());
}

#[test]
fn exact_inspection_grammar_refuses_unknown_duplicate_positional_and_non_utf8_arguments() {
    let sandbox = Sandbox::new();
    let data = sandbox.0.join("private-absent-state");
    for args in [
        vec!["inspect", "--blocked", "--blocked"],
        vec!["inspect", "--demo"],
        vec!["inspect", "--live"],
        vec!["inspect", "--force"],
        vec!["inspect", "private-positional-secret"],
        vec!["inspect", "--sql=private-sql-secret"],
    ] {
        let output = run(
            command(&data).args(args).env("B4RUN_DATA_DIR", ""),
            &sandbox,
        );
        static_failure(&output, 2, "b4run_invalid_arguments");
        assert!(
            !data.exists(),
            "invalid grammar must precede source access or creation"
        );
    }
    let output = run(
        command(&data).args([OsString::from("inspect"), OsString::from_vec(vec![0xff])]),
        &sandbox,
    );
    static_failure(&output, 2, "b4run_invalid_arguments");
}

#[test]
fn inspection_handles_raw_non_utf8_paths_and_preserves_uri_metacharacter_sources() {
    let sandbox = Sandbox::new();
    for (index, name) in [
        OsString::from("private space ?mode=rw&immutable=0#secret%state"),
        OsString::from_vec(b"private-raw-\xff-state".to_vec()),
        OsString::from("private-é-state"),
    ]
    .into_iter()
    .enumerate()
    {
        let original = sandbox.clean_store(&format!("original-{index}"));
        let data = sandbox.0.join(name);
        let before_move = witness(&original);
        match std::fs::rename(&original, &data) {
            Ok(()) => {}
            Err(error) if cfg!(target_os = "macos") && error.raw_os_error() == Some(92) => {
                // This host filesystem actually rejects invalid UTF8 filenames.
                // The raw environment still reaches source validation, rather
                // than being silently recoded or routed through serve parsing.
                static_failure(
                    &run(command(&data).arg("inspect"), &sandbox),
                    2,
                    "b4run_inspection_invalid_source",
                );
                assert!(
                    before_move == witness(&original) && !data.exists(),
                    "unrepresentable raw-path refusal must preserve the real source"
                );
                continue;
            }
            Err(_) => panic!("filesystem must accept the required valid fixture path"),
        }
        let before = witness(&data);
        let output = run(command(&data).arg("inspect"), &sandbox);
        successful_report(&output, &empty_report());
        assert!(
            before == witness(&data),
            "raw path inspection must preserve exact source"
        );
    }
}

#[test]
fn missing_source_and_invalid_data_directory_configuration_refuse_without_creation() {
    let sandbox = Sandbox::new();
    let data = sandbox.0.join("private-absent-state");
    static_failure(
        &run(command(&data).arg("inspect"), &sandbox),
        2,
        "b4run_inspection_invalid_source",
    );
    assert!(!data.exists(), "missing source must stay missing");
    let empty = sandbox.clean_store("private-empty-database");
    OpenOptions::new()
        .write(true)
        .open(empty.join("runtime.sqlite3"))
        .unwrap()
        .set_len(0)
        .unwrap();
    unchanged_refusal(&empty, &sandbox, 2, "b4run_inspection_invalid_source");
    let missing_lock = sandbox.clean_store("private-missing-owner-lock");
    std::fs::remove_file(missing_lock.join("owner.lock")).unwrap();
    unchanged_refusal(
        &missing_lock,
        &sandbox,
        2,
        "b4run_inspection_invalid_source",
    );
    for value in ["".to_owned(), "x".repeat(4097)] {
        static_failure(
            &run(
                command(&data).arg("inspect").env("B4RUN_DATA_DIR", value),
                &sandbox,
            ),
            2,
            "b4run_inspection_invalid_source",
        );
    }
    assert!(
        !data.exists(),
        "invalid configuration must not create source"
    );
}

#[test]
fn wrong_modes_and_symlink_sources_refuse_without_repair() {
    let sandbox = Sandbox::new();
    for (index, name, mode) in [
        (0, "", 0o755),
        (1, "runtime.sqlite3", 0o644),
        (2, "owner.lock", 0o644),
    ] {
        let data = sandbox.clean_store(&format!("private-mode-{index}"));
        std::fs::set_permissions(
            if name.is_empty() {
                data.clone()
            } else {
                data.join(name)
            },
            std::fs::Permissions::from_mode(mode),
        )
        .unwrap();
        unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_invalid_source");
    }
    for name in ["runtime.sqlite3", "owner.lock"] {
        let data = sandbox.clean_store(&format!("private-link-{name}"));
        let target = sandbox.0.join(format!("private-target-{name}"));
        std::fs::rename(data.join(name), &target).unwrap();
        symlink(&target, data.join(name)).unwrap();
        unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_invalid_source");
    }
    let data = sandbox.clean_store("private-real-directory");
    let link = sandbox.0.join("private-directory-symlink");
    symlink(&data, &link).unwrap();
    let before = witness(&data);
    static_failure(
        &run(command(&link).arg("inspect"), &sandbox),
        2,
        "b4run_inspection_invalid_source",
    );
    assert!(
        before == witness(&data),
        "symlink refusal must preserve target directory"
    );
}

#[test]
fn every_sidecar_entry_kind_refuses_without_cleanup() {
    let sandbox = Sandbox::new();
    for name in [
        "runtime.sqlite3-wal",
        "runtime.sqlite3-shm",
        "runtime.sqlite3-journal",
    ] {
        for kind in ["empty", "bytes", "symlink", "directory"] {
            let data = sandbox.clean_store(&format!("private-sidecar-{name}-{kind}"));
            let sidecar = data.join(name);
            match kind {
                "empty" => {
                    File::create(&sidecar).unwrap();
                }
                "bytes" => {
                    std::fs::write(&sidecar, b"private-sidecar-secret").unwrap();
                }
                "symlink" => symlink("private-missing-target", &sidecar).unwrap(),
                "directory" => std::fs::create_dir(&sidecar).unwrap(),
                _ => unreachable!(),
            }
            unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_sidecars_present");
        }
    }
}

#[test]
fn unsupported_store_schema_and_future_snapshot_have_static_refusals() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-future-schema");
    mutate(&data, |connection| {
        connection
            .execute("UPDATE metadata SET schema_version = 5", [])
            .unwrap();
    });
    unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_unsupported_schema");
    let data = accepted_fixture(&sandbox, 1);
    mutate(&data, |connection| {
        let text: String = connection
            .query_row("SELECT snapshot FROM executions", [], |row| row.get(0))
            .unwrap();
        let mut snapshot: Value = serde_json::from_str(&text).unwrap();
        snapshot["schema_version"] = json!(2);
        connection
            .execute(
                "UPDATE executions SET snapshot = ?1",
                [snapshot.to_string()],
            )
            .unwrap();
    });
    unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_unsupported_snapshot");
}

#[test]
fn corrupt_database_and_contradictory_failed_uncertainty_have_static_refusals() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-corrupt-state");
    std::fs::write(
        data.join("runtime.sqlite3"),
        b"private-invalid-database-secret",
    )
    .unwrap();
    unchanged_refusal(&data, &sandbox, 1, "b4run_inspection_corrupt_source");
    let fixture = rich_fixture(&sandbox);
    mutate(&fixture.data, |connection| {
        let text: String = connection
            .query_row(
                "SELECT snapshot FROM executions WHERE id = 'private-execution-008-secret'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut snapshot: Value = serde_json::from_str(&text).unwrap();
        snapshot["status"] = json!("failed");
        connection
            .execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = 'private-execution-008-secret'",
                [snapshot.to_string()],
            )
            .unwrap();
    });
    unchanged_refusal(
        &fixture.data,
        &sandbox,
        1,
        "b4run_inspection_corrupt_source",
    );
}

#[test]
fn blocked_64_is_complete_65_refuses_and_aggregate_remains_available() {
    let sandbox = Sandbox::new();
    let data = accepted_fixture(&sandbox, 64);
    successful_report(
        &run(command(&data).args(["inspect", "--blocked"]), &sandbox),
        &accepted_report(64, true),
    );
    let data = accepted_fixture(&sandbox, 65);
    let before = witness(&data);
    static_failure(
        &run(command(&data).args(["inspect", "--blocked"]), &sandbox),
        2,
        "b4run_inspection_limit_exceeded",
    );
    assert!(
        before == witness(&data),
        "blocked overflow must refuse without truncation or mutation"
    );
    successful_report(
        &run(command(&data).arg("inspect"), &sandbox),
        &accepted_report(65, false),
    );
}

#[test]
fn source_execution_and_snapshot_limits_have_static_refusals() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-oversized-main");
    OpenOptions::new()
        .write(true)
        .open(data.join("runtime.sqlite3"))
        .unwrap()
        .set_len(64 * 1024 * 1024 + 1)
        .unwrap();
    unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_limit_exceeded");
    let data = accepted_fixture(&sandbox, 257);
    unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_limit_exceeded");
    let data = accepted_fixture(&sandbox, 1);
    mutate(&data, |connection| {
        let text: String = connection
            .query_row("SELECT snapshot FROM executions", [], |row| row.get(0))
            .unwrap();
        let mut snapshot: Value = serde_json::from_str(&text).unwrap();
        snapshot["values"]["private-oversized-value-secret"] = json!("x".repeat(1024 * 1024));
        connection
            .execute(
                "UPDATE executions SET snapshot = ?1",
                [snapshot.to_string()],
            )
            .unwrap();
    });
    unchanged_refusal(&data, &sandbox, 2, "b4run_inspection_limit_exceeded");
}

#[test]
fn closed_stdout_returns_static_nonpanic_failure_and_releases_writer_ownership() {
    let sandbox = Sandbox::new();
    let data = sandbox.clean_store("private-closed-output");
    let before = witness(&data);
    let (reader, writer) = UnixStream::pair().unwrap();
    drop(reader);
    let fd: OwnedFd = writer.into();
    let (stderr, stderr_file) = private_file(&sandbox, "closed-stderr");
    let mut cmd = command(&data);
    cmd.arg("inspect")
        .stdin(Stdio::null())
        .stdout(Stdio::from(fd))
        .stderr(Stdio::from(stderr_file));
    let mut child = ChildGuard(Some(cmd.spawn().unwrap()));
    drop(cmd);
    let status = child.wait();
    assert!(
        status.code() == Some(1),
        "closed stdout must produce normal infrastructure exit 1, not a signal or panic"
    );
    assert!(
        std::fs::read(stderr).unwrap() == b"b4run_inspection_output_failed\n",
        "closed output must emit only the static output failure"
    );
    assert!(
        before == witness(&data),
        "closed output failure must preserve source before the real writer reopens"
    );
    let writer = SqliteStore::open(&data).expect("real writer must reopen after output refusal");
    writer.readiness().unwrap();
}

#[test]
fn actual_stalled_output_has_started_and_releases_lock_while_inspector_lives() {
    let sandbox = Sandbox::new();
    let data = accepted_fixture(&sandbox, 64);
    let expected = accepted_report(64, true);
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    socket2::SockRef::from(&writer)
        .set_send_buffer_size(1024)
        .unwrap();
    writer.set_nonblocking(true).unwrap();
    let safe_prefill = [b'x'; 512];
    let mut filled = 0;
    loop {
        match writer.write(&safe_prefill) {
            Ok(count) => {
                assert!(count > 0);
                filled += count;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => panic!(
                "safe qualification socket prefill must succeed or reach actual backpressure"
            ),
        }
    }
    assert!(
        filled > 0,
        "actual full sink must contain known safe prefill"
    );
    writer.set_nonblocking(false).unwrap();
    reader
        .set_read_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    let fd: OwnedFd = writer.into();
    let (stderr, stderr_file) = private_file(&sandbox, "stalled-stderr");
    let mut cmd = command(&data);
    cmd.args(["inspect", "--blocked"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(fd))
        .stderr(Stdio::from(stderr_file));
    let mut child = ChildGuard(Some(cmd.spawn().unwrap()));
    drop(cmd);
    // Drain exactly the parent-owned safe prefill, then observe a real report byte.
    let mut prefill = vec![0; filled];
    reader.read_exact(&mut prefill).unwrap();
    assert!(
        prefill.iter().all(|byte| *byte == b'x'),
        "only known safe prefill precedes the actual report"
    );
    let mut first = [0];
    reader
        .read_exact(&mut first)
        .expect("actual inspector report output must positively begin");
    assert!(
        first == *b"{",
        "first real output byte must belong to safe JSON report"
    );
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
        "withheld reads must demonstrate actual stalled output while inspector lives"
    );
    let real_writer = SqliteStore::open(&data).expect(
        "a real writer must acquire ownership after output has begun while inspector remains alive",
    );
    real_writer.readiness().unwrap();
    assert!(
        matches!(SqliteStore::open(&data), Err(StoreError::AlreadyOwned)),
        "reopened real Store must physically exclude another cooperating writer"
    );
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
        "inspector must remain alive and blocked after real writer acquisition"
    );
    drop(real_writer);
    let mut bytes = first.to_vec();
    reader.read_to_end(&mut bytes).unwrap();
    let output = Captured {
        status: child.wait(),
        stdout: bytes,
        stderr: std::fs::read(stderr).unwrap(),
    };
    successful_report(&output, &expected);
}
