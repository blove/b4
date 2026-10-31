//! Explicit ignored infrastructure for native reports from a genuine migrated archive.
//! No permission authority or checker feature case is credited by this producer.
//! Caller-owned private source and output roots survive normal return.

#![cfg(any(target_os = "macos", target_os = "linux"))]

use b4_contract::{Snapshot, ThreadId};
use b4_runtime::{Store, ThreadView};
use b4_sqlite::{inspect_stopped, InspectionOptions, InspectionReport, SqliteStore};
use fs2::FileExt;
use rusqlite::{types::Value as SqlValue, Connection, OpenFlags};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

const ARCHIVE: &[u8] = include_bytes!("fixtures/schema3-genuine-approvals.sqlite3");
const ARCHIVE_SHA256: &str = "a7906755f97299091a21835e7fef97d3ea971084ff8190c947f71790e9d7ef15";
const LEGACY_QUERIES: [(&str, &str); 5] = [
    ("threads", "SELECT * FROM threads ORDER BY id"),
    ("executions", "SELECT * FROM executions ORDER BY id"),
    ("journal", "SELECT * FROM journal ORDER BY sequence"),
    (
        "approval_grants",
        "SELECT * FROM approval_grants ORDER BY execution_id, interrupt_id",
    ),
    (
        "approval_acceptances",
        "SELECT * FROM approval_acceptances ORDER BY execution_id, turn_id",
    ),
];
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
type LogicalCells = Vec<(String, Vec<Vec<SqlValue>>)>;

// All diagnostics are finite; no error Debug, paths, tokens or row values escape.
fn must<T, E>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|_| panic!("schema4 report producer operation failed"))
}
fn present<T>(value: Option<T>) -> T {
    value.unwrap_or_else(|| panic!("schema4 report producer required value missing"))
}
fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn names(root: &Path) -> Vec<OsString> {
    let mut names = must(fs::read_dir(root))
        .map(|entry| must(entry).file_name())
        .collect::<Vec<_>>();
    names.sort();
    names
}
fn caller_root(variable: &str) -> PathBuf {
    let root = PathBuf::from(present(std::env::var_os(variable)));
    let metadata = must(fs::symlink_metadata(&root));
    assert!(
        root.is_absolute()
            && metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.mode() & 0o7777 == 0o700
            && must(fs::canonicalize(&root)) == root
            && names(&root).is_empty(),
        "caller must reserve a fresh canonical absolute private directory700"
    );
    root
}
fn write_private_new(root: &Path, name: &str, bytes: &[u8], newline: bool) {
    let path = root.join(name);
    let mut file = must(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path),
    );
    let root_metadata = must(fs::symlink_metadata(root));
    let metadata = must(file.metadata());
    assert!(
        metadata.is_file()
            && metadata.mode() & 0o7777 == 0o600
            && metadata.uid() == root_metadata.uid(),
        "new descriptor must prove private600 and caller-owned root identity"
    );
    must(file.write_all(bytes));
    if newline {
        must(file.write_all(b"\n"));
    }
    must(file.sync_all());
    drop(file);
    let metadata = must(fs::symlink_metadata(&path));
    assert!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.mode() & 0o7777 == 0o600
            && metadata.uid() == root_metadata.uid()
            && metadata.len() == bytes.len() as u64 + u64::from(newline),
        "normally closed private output must retain exact descriptor type and size"
    );
    let saved = must(fs::read(&path));
    assert!(
        saved.starts_with(bytes)
            && saved.len() == bytes.len() + usize::from(newline)
            && (!newline || saved.last() == Some(&b'\n')),
        "new file must retain exactly original bytes and only the requested LF"
    );
}
fn stopped_source(root: &Path) {
    let directory = must(fs::symlink_metadata(root));
    assert!(
        directory.is_dir() && directory.mode() & 0o7777 == 0o700,
        "stopped source root must retain directory700"
    );
    for name in ["runtime.sqlite3", "owner.lock"] {
        let metadata = must(fs::symlink_metadata(root.join(name)));
        assert!(
            metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.mode() & 0o7777 == 0o600
                && metadata.uid() == directory.uid(),
            "stopped source requires original private regular files600"
        );
    }
    assert!(
        names(root) == ["owner.lock", "runtime.sqlite3"],
        "normal source retirement must leave no sidecars or unqualified entries"
    );
}
fn observer(root: &Path, stopped: bool) -> Connection {
    if !stopped {
        // A live writer may have committed WAL data; do not use immutable here.
        return must(Connection::open_with_flags(
            root.join("runtime.sqlite3"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        ));
    }
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let path = must(fs::canonicalize(root)).join("runtime.sqlite3");
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
    let db = must(Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NOFOLLOW
            | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
    ));
    assert!(
        must(db.is_readonly(rusqlite::MAIN_DB)),
        "stopped observer must use immutable read-only native-path encoding"
    );
    db
}
struct DatabaseState {
    version: i64,
    checksum: String,
    owner: u64,
    counts: Vec<i64>,
    cells: LogicalCells,
    snapshots: Vec<Snapshot>,
}
fn database_state(root: &Path, schema4: bool, stopped: bool) -> DatabaseState {
    let db = observer(root, stopped);
    let metadata: (i64, i64, String, Vec<u8>) = must(db.query_row(
        "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ));
    let take = if schema4 { 9 } else { 6 };
    let mut counts = Vec::new();
    for query in COUNT_QUERIES.iter().take(take) {
        counts.push(must(db.query_row(query, [], |row| row.get(0))));
    }
    assert!(
        counts[0] == 1 && metadata.0 == 1 && metadata.3.len() == 8,
        "source must retain one exact metadata row with a BLOB8 owner"
    );
    let mut statement = must(db.prepare(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    ));
    let rows = must(statement.query_map([], |row| row.get::<_, String>(0)));
    let actual = must(rows.collect::<Result<Vec<_>, _>>());
    let mut expected = TABLES[..take].to_vec();
    expected.sort();
    assert!(
        actual == expected,
        "source must retain the exact supported table set"
    );
    drop(statement);
    let mut cells = Vec::new();
    for (name, query) in LEGACY_QUERIES {
        let mut statement = must(db.prepare(query));
        let columns = statement.column_count();
        let rows = must(statement.query_map([], |row| {
            (0..columns)
                .map(|column| row.get(column))
                .collect::<Result<Vec<SqlValue>, _>>()
        }));
        cells.push((name.to_owned(), must(rows.collect::<Result<Vec<_>, _>>())));
    }
    must(db.close());
    let snapshots = cells[1]
        .1
        .iter()
        .map(|row| {
            let SqlValue::Text(raw) = &row[4] else {
                panic!("genuine execution snapshot must remain a TEXT cell");
            };
            must(serde_json::from_str(raw))
        })
        .collect();
    DatabaseState {
        version: metadata.1,
        checksum: metadata.2,
        owner: u64::from_be_bytes(must(metadata.3.as_slice().try_into())),
        counts,
        cells,
        snapshots,
    }
}
fn ordinary_views(store: &SqliteStore, original: &DatabaseState) -> Vec<ThreadView> {
    ["schema3-once-thread", "schema3-pending-thread"]
        .into_iter()
        .map(|name| {
            let id = must(ThreadId::new(name));
            let view = present(must(store.thread_view(&id)));
            let execution = present(view.execution.as_ref());
            let original_snapshot = present(
                original
                    .snapshots
                    .iter()
                    .find(|snapshot| snapshot.execution_id == execution.snapshot.execution_id),
            );
            assert!(
            view.thread.id == id
                && execution.thread_id == id
                && execution.snapshot.owner_generation == 1
                && &execution.snapshot == original_snapshot,
            "public projection must preserve genuine original execution owner and full snapshot"
        );
            view
        })
        .collect()
}
fn migrated_state(state: &DatabaseState, original: &DatabaseState, owner: u64) {
    assert!(
        state.version == 4
            && state.checksum == sha256(include_bytes!("../src/schema.sql"))
            && state.owner == owner
            && state.counts == [1, 2, 2, 9, 2, 1, 0, 0, 0]
            && state.cells == original.cells
            && state.snapshots == original.snapshots,
        "migration and next writer may change only qualified metadata and empty new tables"
    );
}
#[derive(PartialEq)]
struct FileWitness {
    name: OsString,
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    size: u64,
    modified: i64,
    modified_ns: i64,
    bytes: Vec<u8>,
}
fn witness(root: &Path) -> Vec<FileWitness> {
    let mut paths = must(fs::read_dir(root))
        .map(|entry| must(entry).path())
        .collect::<Vec<_>>();
    paths.sort();
    paths.push(root.to_path_buf());
    paths
        .into_iter()
        .map(|path| {
            let metadata = must(fs::symlink_metadata(&path));
            FileWitness {
                name: present(path.file_name()).to_os_string(),
                device: metadata.dev(),
                inode: metadata.ino(),
                mode: metadata.mode(),
                uid: metadata.uid(),
                size: metadata.len(),
                modified: metadata.mtime(),
                modified_ns: metadata.mtime_nsec(),
                bytes: if metadata.is_dir() {
                    Vec::new()
                } else {
                    must(fs::read(path))
                },
            }
        })
        .collect()
}
fn reader_release_probe(root: &Path, before: &[FileWitness]) {
    let original = present(
        before
            .iter()
            .find(|entry| entry.name == OsStr::new("owner.lock")),
    );
    let lock = must(File::open(root.join("owner.lock")));
    let metadata = must(lock.metadata());
    assert!(
        metadata.dev() == original.device && metadata.ino() == original.inode,
        "release probe must retain the original witnessed owner-lock inode"
    );
    must(FileExt::try_lock_exclusive(&lock));
    must(FileExt::unlock(&lock));
    drop(lock);
}
fn counters(names: &[&str], counts: &[u64]) -> Value {
    assert!(
        names.len() == counts.len(),
        "expected counter shape must be exact"
    );
    Value::Object(
        names
            .iter()
            .zip(counts)
            .map(|(name, count)| ((*name).to_owned(), Value::from(*count)))
            .collect(),
    )
}
fn expected_report(blocked: bool) -> Value {
    let task_keys = [
        "ready",
        "waiting_for_approval",
        "in_flight",
        "succeeded",
        "failed",
        "denied",
        "cancelled",
        "outcome_unknown",
    ];
    let policy_keys = ["pure", "idempotent", "reconcilable", "unsafe"];
    let blocked_value = if blocked {
        json!({
            "selection": "recorded_unfinished_or_uncertain",
            "rows": [
                {"ordinal":1,"status":"running","recorded_recovery_blocked":false,"recorded_approval_park":false,"has_uncertain_task":false,
                 "initialized_tasks_by_status":counters(&task_keys,&[1,0,0,0,0,0,0,0]),
                 "declared_definition_tasks_by_recovery_policy":counters(&policy_keys,&[1,0,0,0])},
                {"ordinal":2,"status":"waiting_for_approval","recorded_recovery_blocked":false,"recorded_approval_park":true,"has_uncertain_task":false,
                 "initialized_tasks_by_status":counters(&task_keys,&[0,1,0,0,0,0,0,0]),
                 "declared_definition_tasks_by_recovery_policy":counters(&policy_keys,&[1,0,0,0])}
            ]
        })
    } else {
        Value::Null
    };
    json!({
        "inspection_report_version":2,"mode":"stopped_readonly","redacted":true,
        "reader":{"package_version":env!("CARGO_PKG_VERSION"),"sqlite_library_version":rusqlite::version(),
            "snapshot_version":1,"store_schema_version":4,"target_profile":"b4-ap-core/0","profile_status":"native_subset",
            "capability_descriptor_versions":[1,2,3],"core_profile_frozen":false},
        "validation_scope":"schema_structure_execution_snapshots_and_bounded_permission_records",
        "table_rows":counters(&TABLES,&[1,2,2,9,2,1,0,0,0]),
        "executions_by_status":counters(&["accepted","running","waiting_for_approval","completed","failed","cancel_requested","cancelled","recovery_blocked"],&[0,1,1,0,0,0,0,0]),
        "initialized_tasks_by_status":counters(&task_keys,&[1,1,0,0,0,0,0,0]),
        "declared_definition_tasks_by_recovery_policy":counters(&policy_keys,&[2,0,0,0]),
        "recorded_uncertain_tasks_by_policy":counters(&policy_keys,&[0,0,0,0]),
        "permission_authority":{"human_always_decisions":0,"token_free_uses":0},
        "blocked":blocked_value
    })
}
fn produce_report(root: &Path, output: &Path, before: &[FileWitness], blocked: bool) {
    stopped_source(root);
    assert!(
        witness(root) == before,
        "stopped source must retain its complete pre-call witness"
    );
    let result = inspect_stopped(root, InspectionOptions { blocked });
    // Physical retirement and full preservation precede the first report oracle.
    reader_release_probe(root, before);
    stopped_source(root);
    assert!(
        witness(root) == before,
        "inspection must normally retire and preserve the complete stopped source"
    );
    let report: InspectionReport = must(result);
    let native = report.as_ref();
    assert!(
        !native.is_empty()
            && native.len() <= 65_536
            && native.first() == Some(&b'{')
            && native.last() == Some(&b'}')
            && !native.ends_with(b"\n"),
        "native report must be bounded complete JSON without newline framing"
    );
    let expected = must(serde_json::to_vec(&expected_report(blocked)));
    assert!(
        native == expected,
        "native report must have exactly safe public fields and genuine archived counts"
    );
    write_private_new(
        output,
        if blocked {
            "blocked-v2.json"
        } else {
            "summary-v2.json"
        },
        native,
        true,
    );
    stopped_source(root);
    assert!(
        witness(root) == before,
        "private output publication must preserve the source witness"
    );
}

#[test]
#[ignore = "private native report2 producer infrastructure; explicit source-bound invocation only"]
fn produce_genuine_migrated_schema4_reports() {
    let root = caller_root("B4_SCHEMA4_REPORT_ROOT");
    let output = caller_root("B4_SCHEMA4_REPORT_OUTPUT");
    assert!(
        root != output && root.parent() == output.parent(),
        "private source and output must be distinct sibling roots"
    );
    assert!(
        ARCHIVE.len() == 45_056 && sha256(ARCHIVE) == ARCHIVE_SHA256,
        "producer requires the exact genuinely qualified schema3 archive"
    );
    write_private_new(&root, "runtime.sqlite3", ARCHIVE, false);
    write_private_new(&root, "owner.lock", &[], false);
    stopped_source(&root);
    let archive_witness = witness(&root);
    let original = database_state(&root, false, true);
    reader_release_probe(&root, &archive_witness);
    stopped_source(&root);
    assert!(
        witness(&root) == archive_witness,
        "initial immutable observation must normally close without source mutation"
    );
    assert!(
        original.version == 3
            && original.owner == 1
            && original.checksum == sha256(include_bytes!("../src/schema-v3.sql"))
            && original.counts == [1, 2, 2, 9, 2, 1]
            && original.snapshots.len() == 2
            && original
                .snapshots
                .iter()
                .all(|snapshot| snapshot.owner_generation == 1),
        "seed controls require the original genuine schema3 cut, never recreated authority"
    );
    let first = must(SqliteStore::open(&root));
    let first_views = ordinary_views(&first, &original);
    let first_state = database_state(&root, true, false);
    assert!(
        first.owner_generation() == 2,
        "real migrating writer must acquire owner2"
    );
    migrated_state(&first_state, &original, 2);
    drop(first);
    stopped_source(&root);
    let next = must(SqliteStore::open(&root));
    let next_views = ordinary_views(&next, &original);
    let next_state = database_state(&root, true, false);
    assert!(
        next.owner_generation() == 3,
        "genuine next writer must acquire owner3 after normal first retirement"
    );
    assert!(
        next_views == first_views,
        "genuine next public views must preserve original owner1 and every stored field"
    );
    migrated_state(&next_state, &original, 3);
    drop(next);
    stopped_source(&root);
    let before = witness(&root);
    let closed = database_state(&root, true, true);
    reader_release_probe(&root, &before);
    stopped_source(&root);
    assert!(
        witness(&root) == before,
        "post-writer immutable observation must close and preserve the entire source"
    );
    migrated_state(&closed, &original, 3);
    produce_report(&root, &output, &before, false);
    produce_report(&root, &output, &before, true);
    assert!(
        names(&output) == ["blocked-v2.json", "summary-v2.json"],
        "producer output must contain only the two actual native reports"
    );
    stopped_source(&root);
    reader_release_probe(&root, &before);
    assert!(
        witness(&root) == before,
        "producer must finish with complete source preservation and released lock"
    );
    // Both caller roots and actual native byte files are retained without cleanup.
}
