#![cfg(any(target_os = "macos", target_os = "linux"))]

mod inspection_support;

use b4_contract::*;
use b4_sqlite::{inspect_stopped, InspectionError, InspectionOptions, SqliteStore};
use inspection_support::Directory;
use serde_json::{json, Value};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, PermissionsExt};

fn report(directory: &Directory, blocked: bool) -> Value {
    let encoded = inspect_stopped(directory.path(), InspectionOptions { blocked })
        .unwrap_or_else(|error| panic!("inspection report: {error}"));
    serde_json::from_slice(encoded.as_ref()).unwrap()
}

fn apply(snapshot: &Snapshot, command: KernelCommand) -> Snapshot {
    b4_kernel::transition(snapshot, command, TransitionFacts { now_ms: 123457 })
        .unwrap()
        .snapshot
}

fn spec(index: usize, recovery: RecoveryPolicy, approval: bool) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(format!("secret-task-{index}")).unwrap(),
        effect_id: EffectId::new(format!("secret-effect-{index}")).unwrap(),
        recovery,
        requires_approval: approval,
    }
}

fn dispatch(snapshot: &Snapshot, index: usize) -> Snapshot {
    apply(
        snapshot,
        KernelCommand::Dispatch {
            task_id: snapshot.tasks[index].spec.id.clone(),
            attempt_id: AttemptId::new(format!("secret-attempt-{index}")).unwrap(),
        },
    )
}

fn refusal(directory: &Directory, expected: InspectionError) {
    let result = inspect_stopped(directory.path(), InspectionOptions::default());
    assert_eq!(result.err(), Some(expected));
}

#[test]
fn symlink_directory_with_trailing_slash_refuses() {
    let directory = Directory::clean();
    let link = directory.path().with_extension("root-link");
    symlink(directory.path(), &link).unwrap();
    let result = inspect_stopped(link.join("."), InspectionOptions::default());
    std::fs::remove_file(link).unwrap();
    assert_eq!(result.err(), Some(InspectionError::InvalidSource));
}

#[test]
fn valid_maximum_thread_identity_is_accepted() {
    let directory = Directory::clean();
    directory.populate(1);
    directory.sql(&format!("PRAGMA foreign_keys=OFF; UPDATE threads SET id='{}'; UPDATE executions SET thread_id='{}'; UPDATE journal SET thread_id='{}'", "t".repeat(256), "t".repeat(256), "t".repeat(256)));
    assert!(inspect_stopped(directory.path(), InspectionOptions::default()).is_ok());
}

#[test]
fn stopped_store_returns_complete_redacted_report_without_mutation() {
    let directory = Directory::clean();
    let before = directory.witness();
    let report =
        inspect_stopped(directory.path(), InspectionOptions::default()).unwrap_or_else(|error| {
            panic!("clean stopped Store must produce a complete report: {error}")
        });
    let value: serde_json::Value = serde_json::from_slice(report.as_ref()).unwrap();
    assert_eq!(value["inspection_report_version"], 2);
    assert_eq!(value["mode"], "stopped_readonly");
    assert_eq!(value["table_rows"]["metadata"], 1);
    assert!(value["blocked"].is_null());
    assert_eq!(before, directory.witness());
    assert!(inspect_stopped(directory.path(), InspectionOptions { blocked: true }).is_ok());
    assert_eq!(before, directory.witness());
}

#[test]
fn live_writer_refuses_without_mutation() {
    let directory = Directory::clean();
    let writer = SqliteStore::open(directory.path()).unwrap();
    let before = directory.witness();
    refusal(&directory, InspectionError::AlreadyOwned);
    assert_eq!(before, directory.witness());
    drop(writer);
}

#[test]
fn missing_files_refuse_without_creation() {
    for name in ["runtime.sqlite3", "owner.lock"] {
        let directory = Directory::clean();
        std::fs::remove_file(directory.path().join(name)).unwrap();
        let before = directory.witness();
        refusal(&directory, InspectionError::InvalidSource);
        assert_eq!(before, directory.witness());
    }
}

#[test]
fn incorrect_permissions_refuse() {
    for (name, mode) in [
        ("", 0o755),
        ("runtime.sqlite3", 0o644),
        ("owner.lock", 0o644),
    ] {
        let directory = Directory::clean();
        std::fs::set_permissions(
            directory.path().join(name),
            std::fs::Permissions::from_mode(mode),
        )
        .unwrap();
        refusal(&directory, InspectionError::InvalidSource);
    }
}

#[test]
fn symlink_source_files_refuse() {
    for name in ["runtime.sqlite3", "owner.lock"] {
        let directory = Directory::clean();
        let path = directory.path().join(name);
        let saved = directory.path().join("saved");
        std::fs::rename(&path, &saved).unwrap();
        symlink(saved, path).unwrap();
        refusal(&directory, InspectionError::InvalidSource);
    }
}

#[test]
fn any_sidecar_entry_refuses_without_cleanup() {
    for name in [
        "runtime.sqlite3-wal",
        "runtime.sqlite3-shm",
        "runtime.sqlite3-journal",
    ] {
        for kind in 0..4 {
            let directory = Directory::clean();
            let path = directory.path().join(name);
            match kind {
                0 => std::fs::write(&path, []).unwrap(),
                1 => std::fs::write(&path, b"secret-sidecar").unwrap(),
                2 => symlink("missing", &path).unwrap(),
                _ => std::fs::create_dir(&path).unwrap(),
            }
            refusal(&directory, InspectionError::SidecarsPresent);
            assert!(std::fs::symlink_metadata(path).is_ok());
        }
    }
}

#[test]
fn uri_metacharacters_and_non_ascii_are_encoded_without_source_mutation() {
    let mut directory = Directory::clean();
    let mut name = directory
        .path()
        .file_name()
        .unwrap()
        .as_encoded_bytes()
        .to_vec();
    name.extend_from_slice(b"-?#%&\xc3\xa9");
    #[cfg(target_os = "linux")]
    name.push(0xff);
    directory.rename(std::ffi::OsString::from_vec(name));
    let before = directory.witness();
    assert_eq!(report(&directory, true)["blocked"]["rows"], json!([]));
    assert_eq!(before, directory.witness());
}

#[test]
fn aggregate_and_blocked_projection_cover_all_statuses_tasks_and_policies() {
    let directory = Directory::clean();
    let mut snapshots = directory.populate(10);
    let policies = [
        RecoveryPolicy::Pure,
        RecoveryPolicy::Idempotent,
        RecoveryPolicy::Reconcilable,
        RecoveryPolicy::Unsafe,
    ];
    // Accepted includes all full-definition declarations, none initialized.
    snapshots[0].definition.batches = vec![policies
        .into_iter()
        .enumerate()
        .map(|(i, p)| spec(i, p, false))
        .collect()];
    // Running ready.
    snapshots[1].definition.batches = vec![vec![spec(0, RecoveryPolicy::Pure, false)]];
    snapshots[1] = apply(&snapshots[1], KernelCommand::Start);
    // Stable approval park.
    snapshots[2].definition.batches = vec![vec![spec(0, RecoveryPolicy::Idempotent, true)]];
    snapshots[2] = apply(&snapshots[2], KernelCommand::Start);
    snapshots[2] = apply(
        &snapshots[2],
        KernelCommand::Park {
            task_id: snapshots[2].tasks[0].spec.id.clone(),
            interrupt_id: InterruptId::new("secret-interrupt").unwrap(),
            prompt: json!("secret-prompt"),
        },
    );
    // Empty completed definition.
    snapshots[3] = apply(&snapshots[3], KernelCommand::Start);
    // Failed settled application error.
    snapshots[4].definition.batches = vec![vec![spec(0, RecoveryPolicy::Pure, false)]];
    snapshots[4] = dispatch(&apply(&snapshots[4], KernelCommand::Start), 0);
    snapshots[4] = apply(
        &snapshots[4],
        KernelCommand::FailAttempt {
            task_id: snapshots[4].tasks[0].spec.id.clone(),
            attempt_id: snapshots[4].tasks[0].attempt_id.clone().unwrap(),
            failure: "secret-failure".into(),
        },
    );
    // Cancelling in-flight drain.
    snapshots[5].definition.batches = vec![vec![spec(0, RecoveryPolicy::Reconcilable, false)]];
    snapshots[5] = dispatch(&apply(&snapshots[5], KernelCommand::Start), 0);
    snapshots[5] = apply(
        &snapshots[5],
        KernelCommand::Cancel {
            turn_id: snapshots[5].turn_id.clone(),
            owner_generation: snapshots[5].owner_generation,
        },
    );
    // Cancelled ready task.
    snapshots[6].definition.batches = vec![vec![spec(0, RecoveryPolicy::Unsafe, false)]];
    snapshots[6] = apply(&snapshots[6], KernelCommand::Start);
    snapshots[6] = apply(
        &snapshots[6],
        KernelCommand::Cancel {
            turn_id: snapshots[6].turn_id.clone(),
            owner_generation: snapshots[6].owner_generation,
        },
    );
    // Recorded uncertainty after unsafe recovery.
    snapshots[7].definition.batches = vec![vec![spec(0, RecoveryPolicy::Unsafe, false)]];
    snapshots[7] = dispatch(&apply(&snapshots[7], KernelCommand::Start), 0);
    snapshots[7] = apply(
        &snapshots[7],
        KernelCommand::Recover {
            available_revision: snapshots[7].definition.revision.clone(),
            turn_id: TurnId::new("secret-recovered").unwrap(),
            owner_generation: snapshots[7].owner_generation + 1,
        },
    );
    // A succeeded task with a future batch still ready.
    snapshots[8].definition.batches = vec![
        vec![spec(0, RecoveryPolicy::Pure, false)],
        vec![spec(1, RecoveryPolicy::Idempotent, false)],
    ];
    snapshots[8] = dispatch(&apply(&snapshots[8], KernelCommand::Start), 0);
    snapshots[8] = apply(
        &snapshots[8],
        KernelCommand::Complete {
            task_id: snapshots[8].tasks[0].spec.id.clone(),
            attempt_id: snapshots[8].tasks[0].attempt_id.clone().unwrap(),
            result: TaskResult {
                output: json!("secret-result"),
                writes: Vec::new(),
            },
        },
    );
    // Denial record while another task stays ready.
    snapshots[9].definition.batches = vec![vec![
        spec(0, RecoveryPolicy::Pure, true),
        spec(1, RecoveryPolicy::Pure, false),
    ]];
    snapshots[9] = apply(&snapshots[9], KernelCommand::Start);
    snapshots[9].tasks[0].status = TaskStatus::Denied;
    snapshots[9].tasks[0].interrupt = Some(PendingApproval {
        id: InterruptId::new("secret-denied-interrupt").unwrap(),
        prompt: json!("secret-denied-prompt"),
    });
    for snapshot in &mut snapshots {
        snapshot
            .values
            .insert("secret-value-key".into(), json!("secret-value"));
        for task in &mut snapshot.tasks {
            task.input = snapshot.values.clone();
        }
        if snapshot.status == ExecutionStatus::Completed {
            snapshot.output = Some(Value::Object(snapshot.values.clone()));
        }
        directory.replace(snapshot);
    }
    let before = directory.witness();
    let aggregate = report(&directory, false);
    let blocked = report(&directory, true);
    assert_eq!(before, directory.witness());
    for key in [
        "accepted",
        "running",
        "waiting_for_approval",
        "completed",
        "failed",
        "cancel_requested",
        "cancelled",
        "recovery_blocked",
    ] {
        assert!(aggregate["executions_by_status"][key].as_u64().unwrap() > 0);
    }
    for key in [
        "ready",
        "waiting_for_approval",
        "in_flight",
        "succeeded",
        "failed",
        "denied",
        "cancelled",
        "outcome_unknown",
    ] {
        assert!(
            aggregate["initialized_tasks_by_status"][key]
                .as_u64()
                .unwrap()
                > 0
        );
    }
    for key in ["pure", "idempotent", "reconcilable", "unsafe"] {
        assert!(
            aggregate["declared_definition_tasks_by_recovery_policy"][key]
                .as_u64()
                .unwrap()
                > 0
        );
    }
    assert_eq!(
        aggregate["recorded_uncertain_tasks_by_policy"]["reconcilable"],
        1
    );
    assert_eq!(aggregate["recorded_uncertain_tasks_by_policy"]["unsafe"], 1);
    let rows = blocked["blocked"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 7);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["ordinal"], index + 1);
    }
    assert_eq!(rows[0]["initialized_tasks_by_status"]["ready"], 0);
    for key in ["pure", "idempotent", "reconcilable", "unsafe"] {
        assert_eq!(
            rows[0]["declared_definition_tasks_by_recovery_policy"][key],
            1
        );
    }
    assert!(!serde_json::to_string(&aggregate)
        .unwrap()
        .contains("secret"));
    assert!(!serde_json::to_string(&blocked).unwrap().contains("secret"));
}

#[test]
fn schema_version_checksum_epoch_and_unknown_objects_refuse_statically() {
    for (sql, error) in [
        (
            "UPDATE metadata SET schema_version=1",
            InspectionError::UnsupportedSchema,
        ),
        (
            "UPDATE metadata SET schema_version=2",
            InspectionError::UnsupportedSchema,
        ),
        (
            "UPDATE metadata SET schema_version=999",
            InspectionError::UnsupportedSchema,
        ),
        (
            "UPDATE metadata SET migration_checksum='secret-checksum'",
            InspectionError::UnsupportedSchema,
        ),
        (
            "UPDATE metadata SET owner_epoch=x'0000000000000000'",
            InspectionError::CorruptSource,
        ),
        ("DELETE FROM metadata", InspectionError::CorruptSource),
        (
            "CREATE TABLE secret_extra(value TEXT)",
            InspectionError::UnsupportedSchema,
        ),
        (
            "CREATE VIEW secret_view AS SELECT 1",
            InspectionError::UnsupportedSchema,
        ),
        (
            "CREATE TRIGGER secret_trigger AFTER INSERT ON threads BEGIN SELECT 1; END",
            InspectionError::UnsupportedSchema,
        ),
        (
            "DROP TABLE approval_grants",
            InspectionError::UnsupportedSchema,
        ),
    ] {
        let directory = Directory::clean();
        directory.sql(sql);
        let before = directory.witness();
        refusal(&directory, error);
        assert_eq!(before, directory.witness());
    }
}

#[test]
fn snapshot_version_syntax_index_owner_and_foreign_active_pointer_refuse() {
    for kind in 0..8 {
        let directory = Directory::clean();
        let snapshots = directory.populate(2);
        match kind {
            0 => { let mut value = serde_json::to_value(&snapshots[0]).unwrap(); value["schema_version"] = json!(2); directory.replace_json(snapshots[0].execution_id.as_str(), &value.to_string()); }
            1 => directory.replace_json(snapshots[0].execution_id.as_str(), "{secret-malformed-json"),
            2 => directory.sql("UPDATE executions SET revision=x'0000000000000008'"),
            3 => directory.sql("UPDATE executions SET owner_epoch=x'0000000000000000'"),
            4 => directory.sql("UPDATE executions SET owner_epoch=x'0000000000000009'"),
            5 => directory.sql("PRAGMA foreign_keys=OFF; DELETE FROM threads WHERE id='secret-thread-0000'"),
            6 => directory.sql("UPDATE threads SET active_execution_id='secret-execution-0001' WHERE id='secret-thread-0000'"),
            _ => directory.sql("PRAGMA foreign_keys=OFF; UPDATE threads SET active_execution_id='secret-missing-execution'"),
        }
        let before = directory.witness();
        refusal(
            &directory,
            if kind == 0 {
                InspectionError::UnsupportedSnapshot
            } else {
                InspectionError::CorruptSource
            },
        );
        assert_eq!(before, directory.witness());
    }
}

#[test]
fn blocked_64_succeeds_65_refuses_and_aggregate_still_succeeds() {
    let directory = Directory::clean();
    directory.populate(64);
    assert_eq!(
        report(&directory, true)["blocked"]["rows"]
            .as_array()
            .unwrap()
            .len(),
        64
    );
    // Populate uses deterministic IDs; append a separate valid row directly to
    // the same schema, preserving thread FK and active-pointer relationships.
    let snapshot = inspection_support::initial("secret-execution-extra", 2);
    let connection = rusqlite::Connection::open(directory.path().join("runtime.sqlite3")).unwrap();
    connection
        .execute(
            "INSERT INTO threads(id,generation) VALUES('secret-extra-thread',?1)",
            ["0".repeat(64)],
        )
        .unwrap();
    connection.execute("INSERT INTO executions(id,thread_id,revision,owner_epoch,snapshot) VALUES(?1,'secret-extra-thread',?2,?3,?4)", rusqlite::params![snapshot.execution_id.as_str(), snapshot.revision.to_be_bytes().as_slice(), snapshot.owner_generation.to_be_bytes().as_slice(), serde_json::to_string(&snapshot).unwrap()]).unwrap();
    connection.close().unwrap();
    assert_eq!(
        inspect_stopped(directory.path(), InspectionOptions { blocked: true }).err(),
        Some(InspectionError::LimitExceeded)
    );
    assert_eq!(report(&directory, false)["table_rows"]["executions"], 65);
}

#[test]
fn execution_256_succeeds_257_refuses() {
    for count in [256, 257] {
        let directory = Directory::clean();
        directory.populate(count);
        if count == 256 {
            assert_eq!(report(&directory, false)["table_rows"]["executions"], 256);
        } else {
            refusal(&directory, InspectionError::LimitExceeded);
        }
    }
}

fn padded(snapshot: &Snapshot, bytes: usize) -> String {
    let mut snapshot = snapshot.clone();
    snapshot.values.insert("padding".into(), json!(""));
    let base = serde_json::to_string(&snapshot).unwrap().len();
    snapshot
        .values
        .insert("padding".into(), json!("p".repeat(bytes - base)));
    let payload = serde_json::to_string(&snapshot).unwrap();
    assert_eq!(payload.len(), bytes);
    payload
}

#[test]
fn snapshot_byte_boundary_and_actual_engine_oversized_source_cell_refuse() {
    let directory = Directory::clean();
    let snapshot = directory.populate(1).remove(0);
    for size in [1024 * 1024, 1024 * 1024 + 1, 2 * 1024 * 1024 + 1] {
        directory.replace_json(snapshot.execution_id.as_str(), &padded(&snapshot, size));
        if size == 1024 * 1024 {
            assert_eq!(report(&directory, false)["table_rows"]["executions"], 1);
        } else {
            refusal(&directory, InspectionError::LimitExceeded);
        }
    }
}

#[test]
fn snapshot_cumulative_16_mib_boundary_refuses_excess() {
    let directory = Directory::clean();
    let snapshots = directory.populate(17);
    for snapshot in &snapshots[..16] {
        directory.replace_json(
            snapshot.execution_id.as_str(),
            &padded(snapshot, 1024 * 1024),
        );
    }
    // Temporarily remove the final row to establish the exact cumulative cap.
    directory.sql("DELETE FROM journal WHERE execution_id='secret-execution-0016'; UPDATE threads SET active_execution_id=NULL WHERE id='secret-thread-0016'; DELETE FROM executions WHERE id='secret-execution-0016'");
    assert_eq!(report(&directory, false)["table_rows"]["executions"], 16);
    let snapshot = &snapshots[16];
    let connection = rusqlite::Connection::open(directory.path().join("runtime.sqlite3")).unwrap();
    connection.execute("INSERT INTO executions(id,thread_id,revision,owner_epoch,snapshot) VALUES(?1,'secret-thread-0016',?2,?3,?4)", rusqlite::params![snapshot.execution_id.as_str(), snapshot.revision.to_be_bytes().as_slice(), snapshot.owner_generation.to_be_bytes().as_slice(), serde_json::to_string(snapshot).unwrap()]).unwrap();
    connection.close().unwrap();
    refusal(&directory, InspectionError::LimitExceeded);
}

#[test]
fn source_size_64_mib_boundary_and_empty_database_refuse_correctly() {
    let directory = Directory::clean();
    let database = directory.path().join("runtime.sqlite3");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(database)
        .unwrap();
    file.set_len(64 * 1024 * 1024).unwrap();
    assert!(inspect_stopped(directory.path(), InspectionOptions::default()).is_ok());
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    refusal(&directory, InspectionError::LimitExceeded);
    file.set_len(0).unwrap();
    refusal(&directory, InspectionError::InvalidSource);
}

#[test]
fn missing_directory_nul_and_oversized_raw_paths_do_not_access_or_create_source() {
    let directory = Directory::clean();
    let missing = directory.path().join("secret-missing-directory");
    assert_eq!(
        inspect_stopped(&missing, InspectionOptions::default()).err(),
        Some(InspectionError::InvalidSource)
    );
    assert!(!missing.exists());
    let nul = std::path::PathBuf::from(std::ffi::OsString::from_vec(
        b"/private/tmp/secret\0path".to_vec(),
    ));
    assert_eq!(
        inspect_stopped(nul, InspectionOptions::default()).err(),
        Some(InspectionError::InvalidSource)
    );
    let over = std::path::PathBuf::from(std::ffi::OsString::from_vec(vec![b'x'; 4097]));
    assert_eq!(
        inspect_stopped(over, InspectionOptions::default()).err(),
        Some(InspectionError::LimitExceeded)
    );
}

#[test]
fn invalid_truncated_database_and_metadata_shape_refuse_without_changes() {
    for kind in 0..4 {
        let directory = Directory::clean();
        match kind {
            0 => std::fs::write(directory.path().join("runtime.sqlite3"), b"secret-invalid-database").unwrap(),
            1 => std::fs::OpenOptions::new().write(true).open(directory.path().join("runtime.sqlite3")).unwrap().set_len(100).unwrap(),
            2 => directory.sql("PRAGMA ignore_check_constraints=ON; UPDATE metadata SET owner_epoch=x'01'"),
            _ => directory.sql("PRAGMA ignore_check_constraints=ON; INSERT INTO metadata VALUES(2,3,'secret',x'0000000000000001')"),
        }
        let before = directory.witness();
        let error = inspect_stopped(directory.path(), InspectionOptions::default())
            .err()
            .unwrap();
        assert!(matches!(
            error,
            InspectionError::CorruptSource | InspectionError::UnsupportedSchema
        ));
        assert_eq!(before, directory.witness());
    }
}

#[test]
fn malformed_deferred_payloads_are_counted_without_broader_validation_claim() {
    let directory = Directory::clean();
    directory.populate(1);
    directory.sql("UPDATE threads SET metadata='secret-malformed-thread-json', origin='secret-malformed-origin-json'; UPDATE journal SET payload='secret-malformed-journal-json' WHERE kind='admit'; INSERT INTO approval_grants SELECT 'secret-execution-0000','secret-interrupt','secret-task',id,generation,x'0000000000000001',0,'secret-malformed-grant-json' FROM threads; INSERT INTO approval_acceptances SELECT 'secret-execution-0000','secret-turn',id,generation,x'0000000000000001','secret-definition',x'0000000000000001','secret-malformed-acceptance-json' FROM threads;");
    let before = directory.witness();
    let value = report(&directory, true);
    assert_eq!(
        value["validation_scope"],
        "schema_structure_execution_snapshots_and_bounded_permission_records"
    );
    assert_eq!(value["table_rows"]["approval_grants"], 1);
    assert_eq!(value["table_rows"]["approval_acceptances"], 1);
    assert!(!value.to_string().contains("secret"));
    assert_eq!(before, directory.witness());
}

#[test]
fn valid_failure_drain_and_uncertainty_block_are_selected_but_failed_active_is_corrupt() {
    let directory = Directory::clean();
    let mut snapshot = directory.populate(1).remove(0);
    snapshot.definition.batches = vec![vec![
        spec(0, RecoveryPolicy::Pure, false),
        spec(1, RecoveryPolicy::Unsafe, false),
    ]];
    snapshot = dispatch(&dispatch(&apply(&snapshot, KernelCommand::Start), 0), 1);
    snapshot = apply(
        &snapshot,
        KernelCommand::FailAttempt {
            task_id: snapshot.tasks[0].spec.id.clone(),
            attempt_id: snapshot.tasks[0].attempt_id.clone().unwrap(),
            failure: "secret-drain-failure".into(),
        },
    );
    assert_eq!(snapshot.status, ExecutionStatus::Running);
    directory.replace(&snapshot);
    let value = report(&directory, true);
    assert_eq!(value["blocked"]["rows"][0]["status"], "running");
    assert_eq!(value["blocked"]["rows"][0]["has_uncertain_task"], true);
    let mut invalid = snapshot.clone();
    invalid.status = ExecutionStatus::Failed;
    assert!(b4_kernel::validate_snapshot(&invalid).is_err());
    directory.replace_json(
        invalid.execution_id.as_str(),
        &serde_json::to_string(&invalid).unwrap(),
    );
    refusal(&directory, InspectionError::CorruptSource);
    snapshot = apply(
        &snapshot,
        KernelCommand::Recover {
            available_revision: snapshot.definition.revision.clone(),
            turn_id: TurnId::new("secret-drain-recovered").unwrap(),
            owner_generation: snapshot.owner_generation + 1,
        },
    );
    assert_eq!(snapshot.status, ExecutionStatus::RecoveryBlocked);
    directory.replace(&snapshot);
    let value = report(&directory, true);
    assert_eq!(
        value["blocked"]["rows"][0]["recorded_recovery_blocked"],
        true
    );
    assert_eq!(value["recorded_uncertain_tasks_by_policy"]["unsafe"], 1);
}

#[test]
fn recorded_inflight_uncertainty_counts_each_declared_policy() {
    let directory = Directory::clean();
    let mut snapshots = directory.populate(4);
    for (snapshot, policy) in snapshots.iter_mut().zip([
        RecoveryPolicy::Pure,
        RecoveryPolicy::Idempotent,
        RecoveryPolicy::Reconcilable,
        RecoveryPolicy::Unsafe,
    ]) {
        snapshot.definition.batches = vec![vec![spec(0, policy, false)]];
        *snapshot = dispatch(&apply(snapshot, KernelCommand::Start), 0);
        directory.replace(snapshot);
    }
    let value = report(&directory, true);
    for key in ["pure", "idempotent", "reconcilable", "unsafe"] {
        assert_eq!(value["recorded_uncertain_tasks_by_policy"][key], 1);
    }
    for row in value["blocked"]["rows"].as_array().unwrap() {
        assert_eq!(row["has_uncertain_task"], true);
    }
}

#[test]
fn snapshot_shape_limits_precede_owned_snapshot_decode() {
    for kind in 0..2 {
        let directory = Directory::clean();
        let snapshot = directory.populate(1).remove(0);
        let mut value = serde_json::to_value(&snapshot).unwrap();
        if kind == 0 {
            let nested = format!("{}0{}", "[".repeat(63), "]".repeat(63));
            value["values"]["nested"] = serde_json::from_str(&nested).unwrap();
        } else {
            value["values"]["many"] = json!(vec![0; 16384]);
        }
        directory.replace_json(snapshot.execution_id.as_str(), &value.to_string());
        refusal(&directory, InspectionError::LimitExceeded);
    }
}

#[test]
fn every_inspection_error_is_static_copy_and_has_the_frozen_exit() {
    for (error, code, exit) in [
        (
            InspectionError::InvalidSource,
            "b4run_inspection_invalid_source",
            2,
        ),
        (InspectionError::AlreadyOwned, "b4run_storage_owned", 1),
        (
            InspectionError::SidecarsPresent,
            "b4run_inspection_sidecars_present",
            2,
        ),
        (
            InspectionError::UnsupportedSchema,
            "b4run_inspection_unsupported_schema",
            2,
        ),
        (
            InspectionError::UnsupportedSnapshot,
            "b4run_inspection_unsupported_snapshot",
            2,
        ),
        (
            InspectionError::LimitExceeded,
            "b4run_inspection_limit_exceeded",
            2,
        ),
        (
            InspectionError::CorruptSource,
            "b4run_inspection_corrupt_source",
            1,
        ),
        (
            InspectionError::Unavailable,
            "b4run_inspection_unavailable",
            1,
        ),
        (
            InspectionError::CloseUncertain,
            "b4run_inspection_close_uncertain",
            1,
        ),
        (
            InspectionError::UnsupportedPlatform,
            "b4run_unsupported_platform",
            2,
        ),
    ] {
        let copied = error;
        assert_eq!(copied.code(), code);
        assert_eq!(copied.exit_code(), exit);
        assert_eq!(format!("{error}"), code);
        assert_eq!(format!("{error:?}"), code);
    }
}

#[test]
fn actual_legacy_schema_sources_refuse_without_migration_or_epoch_change() {
    use sha2::{Digest, Sha256};
    for version in [1, 2] {
        let directory = Directory::clean();
        std::fs::remove_file(directory.path().join("runtime.sqlite3")).unwrap();
        let connection =
            rusqlite::Connection::open(directory.path().join("runtime.sqlite3")).unwrap();
        connection
            .execute_batch(include_str!("../src/schema-v1.sql"))
            .unwrap();
        if version == 2 {
            connection
                .execute_batch(include_str!("../src/migration-v2.sql"))
                .unwrap();
        }
        let schema = if version == 1 {
            include_str!("../src/schema-v1.sql")
        } else {
            include_str!("../src/schema-v2.sql")
        };
        let checksum = format!("{:x}", Sha256::digest(schema.as_bytes()));
        connection
            .execute(
                "INSERT INTO metadata VALUES(1,?1,?2,?3)",
                rusqlite::params![version, checksum, 7u64.to_be_bytes().as_slice()],
            )
            .unwrap();
        connection.close().unwrap();
        std::fs::set_permissions(
            directory.path().join("runtime.sqlite3"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let before = directory.witness();
        refusal(&directory, InspectionError::UnsupportedSchema);
        assert_eq!(before, directory.witness());
    }
}

#[test]
fn unknown_virtual_table_and_changed_ddl_are_refused_before_snapshot_reads() {
    for sql in [
        "CREATE VIRTUAL TABLE secret_virtual USING fts5(value)",
        "ALTER TABLE threads ADD COLUMN secret_extra TEXT",
    ] {
        let directory = Directory::clean();
        directory.sql(sql);
        let before = directory.witness();
        refusal(&directory, InspectionError::UnsupportedSchema);
        assert_eq!(before, directory.witness());
    }
}

#[test]
fn exact_report_and_counter_keys_match_the_authored_schema() {
    let directory = Directory::clean();
    directory.populate(1);
    let schema: Value = serde_json::from_str(include_str!(
        "../../../spec/runtime/stopped-inspection-report-v2.schema.json"
    ))
    .unwrap();
    for blocked in [false, true] {
        let value = report(&directory, blocked);
        let expected = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        for key in [
            "reader",
            "table_rows",
            "permission_authority",
            "executions_by_status",
            "initialized_tasks_by_status",
            "declared_definition_tasks_by_recovery_policy",
            "recorded_uncertain_tasks_by_policy",
        ] {
            let expected = schema["properties"][key]["required"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                value[key]
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(String::as_str)
                    .collect::<std::collections::BTreeSet<_>>(),
                expected
            );
        }
        if blocked {
            let expected = schema["properties"]["blocked"]["oneOf"][1]["properties"]["rows"]
                ["items"]["required"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                value["blocked"]["rows"][0]
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(String::as_str)
                    .collect::<std::collections::BTreeSet<_>>(),
                expected
            );
        }
    }
}
