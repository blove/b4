//! Export/restore qualifiers for qualified schema4 subjects with no new authority.
//! The first positive export assertion follows normal source retirement/reopen.
#![cfg(unix)]

use b4_contract::ThreadId;
use b4_runtime::{Store, ThreadView};
use b4_sqlite::{BackupError, SqliteStore};
use rusqlite::{types::Value, Connection, OpenFlags};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, DirBuilder, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const ARCHIVE3: &[u8] = include_bytes!("fixtures/schema3-genuine-approvals.sqlite3");
const ARCHIVE3_SHA: &str = "a7906755f97299091a21835e7fef97d3ea971084ff8190c947f71790e9d7ef15";
const QUALIFIED4_SHA: &str = "ab41b50d17c2556a0732ef3c1fc9e8632708c24c80827a30225ff117da6d7167";
const DATA_QUERIES: [&str; 8] = [
    "SELECT * FROM threads ORDER BY id",
    "SELECT * FROM executions ORDER BY id",
    "SELECT * FROM journal ORDER BY sequence",
    "SELECT * FROM approval_grants ORDER BY execution_id, interrupt_id",
    "SELECT * FROM approval_acceptances ORDER BY execution_id, turn_id",
    "SELECT * FROM permission_acceptances ORDER BY execution_id, turn_id",
    "SELECT * FROM permission_decisions ORDER BY permission_id",
    "SELECT * FROM permission_uses ORDER BY use_id",
];
type Cells = Vec<Vec<Vec<Value>>>;
type Schema = Vec<(String, String, String, Option<String>)>;

// Fixed diagnostics do not format private paths, rows, grant data or SQL errors.
fn must<T, E>(value: Result<T, E>) -> T {
    value.unwrap_or_else(|_| panic!("schema4 backup qualifier operation failed"))
}
fn present<T>(value: Option<T>) -> T {
    value.unwrap_or_else(|| panic!("schema4 backup qualifier projection missing"))
}
fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-schema4-backup-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        private_directory(&path);
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn private_directory(path: &Path) {
    must(DirBuilder::new().mode(0o700).create(path));
    must(fs::set_permissions(path, fs::Permissions::from_mode(0o700)));
    assert!(
        must(fs::symlink_metadata(path)).is_dir()
            && must(fs::metadata(path)).permissions().mode() & 0o777 == 0o700,
        "fresh qualifier directory is private700"
    );
}
fn install(path: &Path, bytes: &[u8]) {
    let mut file = must(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path),
    );
    must(file.write_all(bytes));
    must(file.sync_all());
    must(file.set_permissions(fs::Permissions::from_mode(0o600)));
    drop(file);
}
fn readonly(path: &Path) -> Connection {
    must(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    ))
}

#[derive(PartialEq)]
struct Cut {
    version: i64,
    checksum: String,
    owner: u64,
    data: Cells,
    schema: Schema,
}
fn cut(path: &Path) -> Cut {
    let db = readonly(path);
    let count: i64 = must(db.query_row("SELECT count(*) FROM metadata", [], |row| row.get(0)));
    let (id, version, checksum, owner): (i64, i64, String, Vec<u8>) = must(db.query_row(
        "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ));
    assert!(
        count == 1 && id == 1 && owner.len() == 8,
        "one fixed metadata identity and full owner encoding"
    );
    let owner = u64::from_be_bytes(must(owner.as_slice().try_into()));
    let mut data = Vec::new();
    for sql in DATA_QUERIES {
        let mut statement = must(db.prepare(sql));
        let columns = statement.column_count();
        let rows = must(statement.query_map([], |row| {
            (0..columns)
                .map(|column| row.get(column))
                .collect::<Result<Vec<Value>, _>>()
        }));
        data.push(must(rows.collect::<Result<Vec<_>, _>>()));
    }
    let mut statement =
        must(db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name"));
    let schema = must(
        must(statement.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        }))
        .collect::<Result<Schema, _>>(),
    );
    drop(statement);
    must(db.close());
    Cut {
        version,
        checksum,
        owner,
        data,
        schema,
    }
}
fn views(store: &SqliteStore, migrated: bool) -> Vec<ThreadView> {
    if migrated {
        ["schema3-pending-thread", "schema3-once-thread"]
            .into_iter()
            .map(|id| present(must(store.thread_view(&must(ThreadId::new(id))))))
            .collect()
    } else {
        assert!(
            must(store.thread_view(&must(ThreadId::new("backup-absent-thread")))).is_none(),
            "ordinary absent-thread read succeeds"
        );
        Vec::new()
    }
}
fn modes(source: &Path) -> [u32; 2] {
    ["runtime.sqlite3", "owner.lock"]
        .map(|name| must(fs::metadata(source.join(name))).permissions().mode() & 0o777)
}
fn absent(path: &Path) {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        _ => panic!("qualifier expected an absent file"),
    }
}
fn no_sidecars(path: &Path) {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        absent(&PathBuf::from(name));
    }
}
fn archive_cut(path: &Path) -> Cut {
    let metadata = must(fs::symlink_metadata(path));
    assert!(
        metadata.is_file() && metadata.permissions().mode() & 0o777 == 0o600,
        "export is a regular private600 archive"
    );
    no_sidecars(path);
    let db = readonly(path);
    let journal: String = must(db.pragma_query_value(None, "journal_mode", |row| row.get(0)));
    assert!(
        journal == "delete",
        "archive is standalone delete-journal format"
    );
    must(db.close());
    let result = cut(path);
    no_sidecars(path);
    result
}
fn assert_successor(before: &Cut, after: &Cut) {
    assert!(
        after.owner == present(before.owner.checked_add(1))
            && after.version == before.version
            && after.checksum == before.checksum
            && after.schema == before.schema
            && after.data == before.data,
        "normal public reopen only advances global owner and preserves all eight data groups"
    );
}

fn export_subject(migrated: bool) {
    let directory = Directory::new();
    let source = directory.0.join("source");
    private_directory(&source);
    if migrated {
        assert!(
            ARCHIVE3.len() == 45056 && sha(ARCHIVE3) == ARCHIVE3_SHA,
            "migration setup uses the exact qualified genuine historical archive"
        );
        install(&source.join("runtime.sqlite3"), ARCHIVE3);
    }
    let mut store = must(SqliteStore::open(&source));
    let original_owner = if migrated { 2 } else { 1 };
    assert!(
        store.owner_generation() == original_owner,
        "public writer acquires the genuine expected owner"
    );
    let before_views = views(&store, migrated);
    let before = cut(&source.join("runtime.sqlite3"));
    assert!(
        before.version == 4 && before.checksum == QUALIFIED4_SHA && before.owner == original_owner,
        "already qualified writer4 is legal backup setup"
    );
    assert!(
        before.data[5..].iter().all(Vec::is_empty),
        "subject has no fabricated new permission authority"
    );
    let expected_counts = if migrated {
        [2, 2, 9, 2, 1, 0, 0, 0]
    } else {
        [0; 8]
    };
    assert!(
        before.data.iter().map(Vec::len).collect::<Vec<_>>() == expected_counts,
        "subject retains genuine old rows or honest empty store"
    );
    let original_modes = modes(&source);
    let exports = directory.0.join("exports");
    private_directory(&exports);
    let archive = exports.join("archive.sqlite3");
    let result = store.export_backup(&archive);
    assert!(
        matches!(&result, Ok(()) | Err(BackupError::InvalidDatabase)),
        "only the pinned legacy profile refusal is an expected baseline outcome"
    );
    assert!(
        cut(&source.join("runtime.sqlite3")) == before
            && store.owner_generation() == original_owner
            && modes(&source) == original_modes
            && views(&store, migrated) == before_views,
        "export preserves complete live source rows and owner and ordinary projections"
    );
    let exported = if result.is_ok() {
        assert!(
            must(fs::read_dir(&exports)).count() == 1,
            "successful export leaves only final archive"
        );
        Some(archive_cut(&archive))
    } else {
        absent(&archive);
        assert!(
            must(fs::read_dir(&exports)).count() == 0,
            "profile refusal creates no output or stage"
        );
        None
    };
    // No service, application or Arc aliases exist. All read-only observers above closed normally.
    drop(store);
    let next = must(SqliteStore::open(&source));
    assert!(
        next.owner_generation() == original_owner + 1,
        "genuine next writer proves original normal ownership retirement"
    );
    assert!(
        views(&next, migrated) == before_views,
        "ordinary views survive genuine reopen"
    );
    let after = cut(&source.join("runtime.sqlite3"));
    assert_successor(&before, &after);
    drop(next);
    drop(before_views);
    // First positive feature oracle follows actual returns, normal closes and genuine successor read/drop.
    assert!(
        result.is_ok(),
        "qualified schema4 backup export must succeed after normal source retirement"
    );
    let exported = present(exported);
    assert!(
        exported == before,
        "archive preserves exact nine-table cut and qualified DDL/checksum"
    );
    let archive_bytes = must(fs::read(&archive));
    let restoration = directory.0.join("restored");
    private_directory(&restoration);
    install(&restoration.join("runtime.sqlite3"), &archive_bytes);
    let restored = must(SqliteStore::open(&restoration));
    assert!(
        restored.owner_generation() == original_owner + 1,
        "archive restoration acquires a genuine successor owner"
    );
    let restored_views = views(&restored, migrated);
    let restored_cut = cut(&restoration.join("runtime.sqlite3"));
    assert_successor(&before, &restored_cut);
    drop(restored);
    let successor = must(SqliteStore::open(&restoration));
    assert!(
        views(&successor, migrated) == restored_views,
        "restoration normally retires and retains ordinary views"
    );
    assert_successor(&restored_cut, &cut(&restoration.join("runtime.sqlite3")));
    drop(successor);
    drop(restored_views);
    assert!(
        must(fs::read(&archive)) == archive_bytes && archive_cut(&archive) == before,
        "restoration never changes retained archive bytes or owner"
    );
}

#[test]
fn fresh_schema4_backup_exports_after_normal_source_retirement() {
    export_subject(false);
}

#[test]
fn genuine_schema3_migrated_to4_backup_preserves_old_rows_and_restores() {
    export_subject(true);
}
