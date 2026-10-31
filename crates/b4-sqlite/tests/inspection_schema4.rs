//! Report2 empty-authority feature parents and genuine archived schema3 controls.
//! No normal authority is SQL-manufactured. Shape checks follow first acceptance.
//! Legacy controls never open the archive through the current schema4 writer.

#![cfg(any(target_os = "macos", target_os = "linux"))]

use b4_contract::ThreadId;
use b4_runtime::Store;
use b4_sqlite::{
    inspect_stopped, InspectionError, InspectionOptions, InspectionReport, SqliteStore,
};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::os::unix::ffi::OsStrExt;
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const ARCHIVE: &[u8] = include_bytes!("fixtures/schema3-genuine-approvals.sqlite3");
const ARCHIVE_SHA256: &str = "a7906755f97299091a21835e7fef97d3ea971084ff8190c947f71790e9d7ef15";
const TABLES: [&str; 9] = [
    "metadata",
    "threads",
    "executions",
    "journal",
    "approval_grants",
    "approval_acceptances",
    "permission_acceptances",
    "permission_decisions",
    "permission_uses",
];
const COUNT_QUERIES: [&str; 9] = [
    "SELECT count(*) FROM metadata",
    "SELECT count(*) FROM threads",
    "SELECT count(*) FROM executions",
    "SELECT count(*) FROM journal",
    "SELECT count(*) FROM approval_grants",
    "SELECT count(*) FROM approval_acceptances",
    "SELECT count(*) FROM permission_acceptances",
    "SELECT count(*) FROM permission_decisions",
    "SELECT count(*) FROM permission_uses",
];
const EXECUTION_KEYS: [&str; 8] = [
    "accepted",
    "running",
    "waiting_for_approval",
    "completed",
    "failed",
    "cancel_requested",
    "cancelled",
    "recovery_blocked",
];
const TASK_KEYS: [&str; 8] = [
    "ready",
    "waiting_for_approval",
    "in_flight",
    "succeeded",
    "failed",
    "denied",
    "cancelled",
    "outcome_unknown",
];
const POLICY_KEYS: [&str; 4] = ["pure", "idempotent", "reconcilable", "unsafe"];

// All errors/assertions use fixed safe text, without paths, row data or tokens.
fn must<T, E>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|_| panic!("stopped report qualifier operation failed"))
}
fn present<T>(value: Option<T>) -> T {
    value.unwrap_or_else(|| panic!("stopped report qualifier required field missing"))
}
fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn fresh() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "b4-report2-qualifier-{}-{sequence}",
            std::process::id()
        ));
        let mut builder = DirBuilder::new();
        builder.mode(0o700);
        must(builder.create(&path));
        must(fs::set_permissions(
            &path,
            fs::Permissions::from_mode(0o700),
        ));
        Self(path)
    }
    fn database(&self) -> PathBuf {
        self.0.join("runtime.sqlite3")
    }
    fn lock(&self) -> PathBuf {
        self.0.join("owner.lock")
    }
    fn make_created_files_private(&self) {
        // Legality setup for our fresh public-writer files; no content repair.
        for path in [self.database(), self.lock()] {
            must(fs::set_permissions(path, fs::Permissions::from_mode(0o600)));
        }
    }
    fn assert_stopped(&self) {
        let root = must(fs::symlink_metadata(&self.0));
        assert!(
            root.is_dir() && root.mode() & 0o7777 == 0o700,
            "stopped source requires an owned regular directory700"
        );
        for path in [self.database(), self.lock()] {
            let metadata = must(fs::symlink_metadata(path));
            assert!(
                metadata.is_file() && metadata.mode() & 0o7777 == 0o600,
                "stopped source requires exact regular files600"
            );
        }
        let mut names = must(fs::read_dir(&self.0))
            .map(|entry| must(entry).file_name())
            .collect::<Vec<_>>();
        names.sort();
        assert!(
            names == ["owner.lock", "runtime.sqlite3"],
            "normal close must leave only database and owner lock, without sidecar repair"
        );
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn readonly(path: &Path) -> Connection {
    must(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    ))
}
fn stopped_readonly(directory: &Directory) -> Connection {
    // Use the primary stopped reader's native-byte URI encoding on a canonical root.
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let path = must(fs::canonicalize(&directory.0)).join("runtime.sqlite3");
    let mut uri = String::with_capacity(path.as_os_str().as_bytes().len() * 3 + 32);
    uri.push_str("file:");
    for byte in path.as_os_str().as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'/' | b'-' | b'_' | b'.' | b'~') {
            uri.push(char::from(*byte));
        } else {
            uri.push('%');
            uri.push(char::from(HEX[usize::from(*byte >> 4)]));
            uri.push(char::from(HEX[usize::from(*byte & 15)]));
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
        | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE;
    let db = must(Connection::open_with_flags(uri, flags));
    assert!(
        must(db.is_readonly(rusqlite::MAIN_DB)),
        "stopped setup observer must be an immutable read-only connection"
    );
    db
}
#[derive(PartialEq)]
struct DatabaseState {
    version: i64,
    checksum: String,
    owner: u64,
    counts: Vec<i64>,
}
fn state(directory: &Directory, schema4: bool) -> DatabaseState {
    // Active writers may have committed WAL contents; retain plain read-only here.
    read_state(readonly(&directory.database()), schema4)
}
fn stopped_state(directory: &Directory, schema4: bool) -> DatabaseState {
    directory.assert_stopped();
    let observed = read_state(stopped_readonly(directory), schema4);
    directory.assert_stopped();
    observed
}
fn read_state(db: Connection, schema4: bool) -> DatabaseState {
    let row: (i64, i64, String, Vec<u8>) = must(db.query_row(
        "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ));
    let count: i64 = must(db.query_row("SELECT count(*) FROM metadata", [], |row| row.get(0)));
    assert!(
        count == 1 && row.0 == 1 && row.3.len() == 8,
        "legal source must retain one fixed metadata row and BLOB8 owner"
    );
    let owner = u64::from_be_bytes(must(row.3.as_slice().try_into()));
    let take = if schema4 { 9 } else { 6 };
    let mut counts = Vec::new();
    for query in COUNT_QUERIES.iter().take(take) {
        counts.push(must(db.query_row(query, [], |row| row.get(0))));
    }
    let mut statement = must(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name"));
    let names = must(statement.query_map([], |row| row.get::<_, String>(0)));
    let names = must(names.collect::<Result<Vec<_>, _>>());
    let mut expected = TABLES[..take].to_vec();
    expected.sort();
    assert!(
        names == expected,
        "legal source requires the exact supported table inventory"
    );
    drop(statement);
    must(db.close());
    DatabaseState {
        version: row.1,
        checksum: row.2,
        owner,
        counts,
    }
}
fn ordinary_absent_read(store: &SqliteStore) {
    assert!(
        must(store.thread_view(&must(ThreadId::new("report2-empty-thread")))).is_none(),
        "real writer must perform an ordinary absent-thread projection"
    );
}
fn fresh_schema4() -> Directory {
    let directory = Directory::fresh();
    let first = must(SqliteStore::open(&directory.0));
    directory.make_created_files_private();
    ordinary_absent_read(&first);
    let original = state(&directory, true);
    assert!(
        first.owner_generation() == 1
            && original.owner == 1
            && original.version == 4
            && original.checksum == sha256(include_bytes!("../src/schema.sql"))
            && original.counts == [1, 0, 0, 0, 0, 0, 0, 0, 0],
        "first real qualified schema4 writer must have empty authority and owner1"
    );
    drop(first);
    let next = must(SqliteStore::open(&directory.0));
    ordinary_absent_read(&next);
    let reopened = state(&directory, true);
    assert!(
        next.owner_generation() == 2
            && reopened.owner == 2
            && reopened.version == original.version
            && reopened.checksum == original.checksum
            && reopened.counts == original.counts,
        "genuine next writer must retain the empty schema4 cut and acquire owner2"
    );
    drop(next);
    directory.assert_stopped();
    assert!(
        stopped_state(&directory, true) == reopened,
        "normal next writer close must retain the exact legal stopped cut"
    );
    directory
}
fn write_private_new(path: &Path, bytes: &[u8]) {
    let mut file = must(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path),
    );
    must(file.write_all(bytes));
    must(file.sync_all());
    drop(file);
    must(fs::set_permissions(path, fs::Permissions::from_mode(0o600)));
}
fn genuine_schema3() -> Directory {
    let directory = Directory::fresh();
    assert!(
        ARCHIVE.len() == 45056 && sha256(ARCHIVE) == ARCHIVE_SHA256,
        "legacy control must use the exact qualified genuine schema3 archive"
    );
    write_private_new(&directory.database(), ARCHIVE);
    write_private_new(&directory.lock(), &[]);
    directory.assert_stopped();
    let original = stopped_state(&directory, false);
    assert!(
        original.version == 3
            && original.owner == 1
            && original.checksum == sha256(include_bytes!("../src/schema-v3.sql"))
            && original.counts == [1, 2, 2, 9, 2, 1],
        "legacy control must preserve the genuine original pending and Once schema3 cut"
    );
    // Never SqliteStore::open here: the current writer would migrate this input.
    directory
}

#[derive(PartialEq)]
struct FileWitness {
    name: std::ffi::OsString,
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    modified: i64,
    modified_ns: i64,
    bytes: Vec<u8>,
}
fn witness(directory: &Directory) -> Vec<FileWitness> {
    let mut paths = must(fs::read_dir(&directory.0))
        .map(|entry| must(entry).path())
        .collect::<Vec<_>>();
    paths.sort();
    paths.push(directory.0.clone());
    paths
        .into_iter()
        .map(|path| {
            let metadata = must(fs::symlink_metadata(&path));
            FileWitness {
                name: present(path.file_name()).to_os_string(),
                device: metadata.dev(),
                inode: metadata.ino(),
                mode: metadata.mode(),
                size: metadata.len(),
                modified: metadata.mtime(),
                modified_ns: metadata.mtime_nsec(),
                bytes: if metadata.is_dir() {
                    Vec::new()
                } else {
                    must(fs::read(&path))
                },
            }
        })
        .collect()
}
fn reader_release_probe(directory: &Directory, before: &[FileWitness]) {
    let original = present(
        before
            .iter()
            .find(|entry| entry.name.as_os_str() == std::ffi::OsStr::new("owner.lock")),
    );
    let lock = must(File::open(directory.lock()));
    let metadata = must(lock.metadata());
    assert!(
        metadata.dev() == original.device && metadata.ino() == original.inode,
        "reader release probe must use the original witnessed owner-lock inode"
    );
    must(FileExt::try_lock_exclusive(&lock));
    must(FileExt::unlock(&lock));
    drop(lock);
}
fn inspect_preserved(
    directory: &Directory,
    blocked: bool,
) -> Result<InspectionReport, InspectionError> {
    directory.assert_stopped();
    let before = witness(directory);
    let result = inspect_stopped(&directory.0, InspectionOptions { blocked });
    reader_release_probe(directory, &before);
    directory.assert_stopped();
    assert!(witness(directory) == before, "stopped inspection must normally release resources and preserve the complete source witness");
    result
}

fn keys(value: &Value, expected: &[&str]) {
    let object = present(value.as_object());
    let mut actual = object.keys().map(String::as_str).collect::<Vec<_>>();
    actual.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert!(
        actual == expected,
        "redacted report object must have exactly the public contract keys"
    );
}
fn counters(value: &Value, names: &[&str], expected: &[u64]) {
    assert!(
        names.len() == expected.len(),
        "qualifier counter specification must be exact"
    );
    keys(value, names);
    for (name, count) in names.iter().zip(expected) {
        assert!(
            value[*name].as_u64() == Some(*count),
            "report counters must be exact unsigned integers"
        );
    }
}
fn parse(report: &InspectionReport) -> Value {
    let bytes = report.as_ref();
    assert!(
        !bytes.is_empty()
            && bytes.len() <= 65_536
            && bytes.first() == Some(&b'{')
            && bytes.last() == Some(&b'}')
            && !bytes.ends_with(b"\n"),
        "native report must be complete bounded JSON without CLI framing"
    );
    must(serde_json::from_slice(bytes))
}
fn header(value: &Value, version2: bool) {
    let mut root = vec![
        "inspection_report_version",
        "mode",
        "redacted",
        "reader",
        "validation_scope",
        "table_rows",
        "executions_by_status",
        "initialized_tasks_by_status",
        "declared_definition_tasks_by_recovery_policy",
        "recorded_uncertain_tasks_by_policy",
        "blocked",
    ];
    if version2 {
        root.push("permission_authority");
    }
    keys(value, &root);
    assert!(
        value["inspection_report_version"].as_u64() == Some(if version2 { 2 } else { 1 })
            && value["mode"] == json!("stopped_readonly")
            && value["redacted"] == json!(true),
        "report must use the intended version and exact stopped redacted mode"
    );
    let mut reader = vec![
        "package_version",
        "sqlite_library_version",
        "snapshot_version",
        "store_schema_version",
        "target_profile",
        "profile_status",
        "core_profile_frozen",
    ];
    reader.push(if version2 {
        "capability_descriptor_versions"
    } else {
        "capability_descriptor_version"
    });
    keys(&value["reader"], &reader);
    assert!(
        value["reader"]["package_version"] == json!(env!("CARGO_PKG_VERSION"))
            && value["reader"]["sqlite_library_version"] == json!(rusqlite::version())
            && value["reader"]["snapshot_version"].as_u64() == Some(1)
            && value["reader"]["store_schema_version"].as_u64()
                == Some(if version2 { 4 } else { 3 })
            && value["reader"]["target_profile"] == json!("b4-ap-core/0")
            && value["reader"]["profile_status"] == json!("native_subset")
            && value["reader"]["core_profile_frozen"] == json!(false),
        "reader metadata must be exact public contract information without source identity"
    );
    if version2 {
        assert!(
            value["reader"]["capability_descriptor_versions"] == json!([1, 2, 3])
                && value["validation_scope"] == json!("schema_structure_execution_snapshots_and_bounded_permission_records"),
            "report2 reader versions and stored validation scope must be exact without claiming host authorization"
        );
    } else {
        assert!(
            value["reader"]["capability_descriptor_version"].as_u64() == Some(2)
                && value["validation_scope"] == json!("schema_structure_and_execution_snapshots"),
            "legacy report1 schema3 decoder contract must remain exact"
        );
    }
}
fn blocked_row(value: &Value, ordinal: u64, approval: bool) {
    keys(
        value,
        &[
            "ordinal",
            "status",
            "recorded_recovery_blocked",
            "recorded_approval_park",
            "has_uncertain_task",
            "initialized_tasks_by_status",
            "declared_definition_tasks_by_recovery_policy",
        ],
    );
    assert!(
        value["ordinal"].as_u64() == Some(ordinal)
            && value["status"]
                == json!(if approval {
                    "waiting_for_approval"
                } else {
                    "running"
                })
            && value["recorded_recovery_blocked"] == json!(false)
            && value["recorded_approval_park"] == json!(approval)
            && value["has_uncertain_task"] == json!(false),
        "legacy blocked row must truthfully project the real prior snapshot"
    );
    counters(
        &value["initialized_tasks_by_status"],
        &TASK_KEYS,
        if approval {
            &[0, 1, 0, 0, 0, 0, 0, 0]
        } else {
            &[1, 0, 0, 0, 0, 0, 0, 0]
        },
    );
    counters(
        &value["declared_definition_tasks_by_recovery_policy"],
        &POLICY_KEYS,
        &[1, 0, 0, 0],
    );
}
fn shape(report: &InspectionReport, version2: bool, blocked: bool) {
    let value = parse(report);
    header(&value, version2);
    if version2 {
        counters(&value["table_rows"], &TABLES, &[1, 0, 0, 0, 0, 0, 0, 0, 0]);
        counters(
            &value["permission_authority"],
            &["human_always_decisions", "token_free_uses"],
            &[0, 0],
        );
        counters(&value["executions_by_status"], &EXECUTION_KEYS, &[0; 8]);
        counters(&value["initialized_tasks_by_status"], &TASK_KEYS, &[0; 8]);
        counters(
            &value["declared_definition_tasks_by_recovery_policy"],
            &POLICY_KEYS,
            &[0; 4],
        );
    } else {
        counters(&value["table_rows"], &TABLES[..6], &[1, 2, 2, 9, 2, 1]);
        counters(
            &value["executions_by_status"],
            &EXECUTION_KEYS,
            &[0, 1, 1, 0, 0, 0, 0, 0],
        );
        counters(
            &value["initialized_tasks_by_status"],
            &TASK_KEYS,
            &[1, 1, 0, 0, 0, 0, 0, 0],
        );
        counters(
            &value["declared_definition_tasks_by_recovery_policy"],
            &POLICY_KEYS,
            &[2, 0, 0, 0],
        );
    }
    counters(
        &value["recorded_uncertain_tasks_by_policy"],
        &POLICY_KEYS,
        &[0; 4],
    );
    if blocked {
        keys(&value["blocked"], &["selection", "rows"]);
        assert!(
            value["blocked"]["selection"] == json!("recorded_unfinished_or_uncertain"),
            "blocked selection must retain the exact public contract"
        );
        let rows = present(value["blocked"]["rows"].as_array());
        if version2 {
            assert!(
                rows.is_empty(),
                "empty schema4 authority must produce no recorded blocked rows"
            );
        } else {
            assert!(
                rows.len() == 2,
                "genuine archive must expose its two original recorded unfinished rows"
            );
            blocked_row(&rows[0], 1, false);
            blocked_row(&rows[1], 2, true);
        }
    } else {
        assert!(
            value["blocked"].is_null(),
            "summary mode must preserve null blocked projection"
        );
    }
}
fn feature(blocked: bool) {
    let directory = fresh_schema4();
    let result = inspect_preserved(&directory, blocked);
    assert!(
        matches!(&result, Ok(_) | Err(InspectionError::UnsupportedSchema)),
        "legal schema4 baseline may only return a report or exact unsupported schema profile refusal"
    );
    // This is the sole initial positive feature oracle, before any JSON parsing.
    assert!(
        result.is_ok(),
        "legal stopped schema4 must accept native report2 after genuine writer retirement"
    );
    shape(&must(result), true, blocked);
}
fn legacy_control(blocked: bool) {
    let directory = genuine_schema3();
    let result = inspect_preserved(&directory, blocked);
    assert!(
        result.is_ok(),
        "qualified historical schema3 must retain native report1 without writer migration"
    );
    shape(&must(result), false, blocked);
    assert!(
        must(fs::read(directory.database())) == ARCHIVE,
        "legacy report must retain the complete historical archive bytes"
    );
}

#[test]
fn schema4_empty_summary_accepts_report2_after_normal_writer_reopen() {
    feature(false);
}
#[test]
fn schema4_empty_blocked_accepts_report2_after_normal_writer_reopen() {
    feature(true);
}
#[test]
fn genuine_schema3_summary_retains_report1_without_writer_open() {
    legacy_control(false);
}
#[test]
fn genuine_schema3_blocked_retains_report1_without_writer_open() {
    legacy_control(true);
}
