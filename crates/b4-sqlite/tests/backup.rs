#![cfg(unix)]

use b4_contract::ThreadId;
use b4_runtime::{Store, StoreError, ThreadOrigin, ThreadSeed};
use b4_sqlite::{BackupError, SqliteStore};
use rusqlite::{types::Value, Connection, OpenFlags};
use serde_json::json;
use std::{
    ffi::OsString,
    os::unix::ffi::OsStringExt,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-backup-api-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
    fn source(&self) -> PathBuf {
        self.0.join("source")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn source(directory: &Directory) -> SqliteStore {
    let store = SqliteStore::open(directory.source()).unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: ThreadId::new("thread").unwrap(),
            created_at_ms: 100,
            metadata: json!({"label":"saved","nested":[1,null,false]})
                .as_object()
                .unwrap()
                .clone(),
            origin: Some(ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    store
}
fn readonly(path: &Path) -> Connection {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
}
fn logical_rows(connection: &Connection) -> Vec<Vec<Vec<Value>>> {
    [
        ("metadata", "id"),
        ("threads", "id"),
        ("executions", "id"),
        ("journal", "sequence"),
        ("approval_grants", "execution_id, interrupt_id"),
        ("approval_acceptances", "execution_id, turn_id"),
    ]
    .iter()
    .map(|(table, order)| {
        // Only fixed, trusted table names are interpolated here.
        let mut statement = connection
            .prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))
            .unwrap();
        let columns = statement.column_count();
        statement
            .query_map([], |row| {
                (0..columns)
                    .map(|column| row.get::<_, Value>(column))
                    .collect()
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    })
    .collect()
}

#[test]
fn fresh_export_includes_committed_wal_without_changing_source_or_owner() {
    let directory = Directory::new();
    let mut store = source(&directory);
    let before = logical_rows(&readonly(&directory.source().join("runtime.sqlite3")));
    let owner = store.owner_generation();
    assert!(
        std::fs::metadata(directory.source().join("runtime.sqlite3-wal"))
            .unwrap()
            .len()
            > 0
    );
    let blind = directory.0.join("blind-main-copy.sqlite3");
    std::fs::copy(directory.source().join("runtime.sqlite3"), &blind).unwrap();
    let missing: Result<i64, _> = readonly(&blind).query_row(
        "SELECT count(*) FROM threads WHERE id = 'thread'",
        [],
        |row| row.get(0),
    );
    assert!(
        missing.is_err() || missing == Ok(0),
        "main copy includes WAL cut"
    );
    let archive = directory.0.join("archive.sqlite3");
    assert_eq!(store.export_backup(&archive), Ok(()));
    let copied = readonly(&archive);
    assert!(
        logical_rows(&copied) == before,
        "archive logical rows differ"
    );
    assert!(
        logical_rows(&readonly(&directory.source().join("runtime.sqlite3"))) == before,
        "source logical rows changed"
    );
    assert_eq!(store.owner_generation(), owner);
    assert!(matches!(
        SqliteStore::open(directory.source()),
        Err(StoreError::AlreadyOwned)
    ));
    let journal: String = copied
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    assert_eq!(journal, "delete");
    assert!(!archive.with_extension("sqlite3-wal").exists());
}

#[test]
fn existing_destination_variants_are_never_replaced() {
    let directory = Directory::new();
    let mut store = source(&directory);
    let empty = directory.0.join("empty");
    let filled = directory.0.join("filled");
    let folder = directory.0.join("folder");
    let alias = directory.0.join("alias");
    let dangling = directory.0.join("dangling");
    std::fs::write(&empty, []).unwrap();
    std::fs::write(&filled, b"original").unwrap();
    std::fs::create_dir(&folder).unwrap();
    std::os::unix::fs::symlink(&filled, &alias).unwrap();
    std::os::unix::fs::symlink(directory.0.join("absent"), &dangling).unwrap();
    for destination in [&empty, &filled, &folder, &alias, &dangling] {
        assert_eq!(
            store.export_backup(destination),
            Err(BackupError::DestinationExists)
        );
    }
    assert_eq!(std::fs::read(&empty).unwrap(), b"");
    assert_eq!(std::fs::read(&filled).unwrap(), b"original");
    assert!(folder.is_dir());
    assert_eq!(std::fs::read_link(&alias).unwrap(), filled);
    assert_eq!(
        std::fs::read_link(&dangling).unwrap(),
        directory.0.join("absent")
    );
}

#[test]
fn repeated_fresh_exports_are_private_and_leave_no_staging_files() {
    let directory = Directory::new();
    let mut store = source(&directory);
    let before = logical_rows(&readonly(&directory.source().join("runtime.sqlite3")));
    for name in ["first.sqlite3", "second.sqlite3"] {
        let archive = directory.0.join(name);
        assert_eq!(store.export_backup(&archive), Ok(()));
        assert_eq!(
            std::fs::metadata(&archive).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(
            logical_rows(&readonly(&archive)) == before,
            "fresh export differs"
        );
    }
    assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 3);
}

#[test]
fn quoted_and_unicode_destination_paths_are_literal() {
    let directory = Directory::new();
    let mut store = source(&directory);
    let parent = directory.0.join("quót'ed parent");
    std::fs::create_dir(&parent).unwrap();
    let archive = parent.join("file:quote's?mode=rw.sqlite3");
    assert_eq!(store.export_backup(&archive), Ok(()));
    let count: i64 = readonly(&archive)
        .query_row("SELECT count(*) FROM threads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(std::fs::read_dir(parent).unwrap().count(), 1);
}

#[test]
fn invalid_destinations_never_create_parent_or_output() {
    let directory = Directory::new();
    let mut store = source(&directory);
    let not_directory = directory.0.join("parent-file");
    std::fs::write(&not_directory, b"unchanged").unwrap();
    let bad_encoding = directory.0.join(OsString::from_vec(vec![b'n', 0xff]));
    let bad_nul = directory.0.join("private\0name");
    for destination in [
        PathBuf::new(),
        PathBuf::from("/"),
        directory.0.join("missing-parent/archive.sqlite3"),
        not_directory.join("archive.sqlite3"),
        bad_encoding,
        bad_nul,
    ] {
        assert_eq!(
            store.export_backup(&destination),
            Err(BackupError::InvalidDestination)
        );
    }
    assert!(!directory.0.join("missing-parent").exists());
    assert_eq!(std::fs::read(not_directory).unwrap(), b"unchanged");
    assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 2);
}

#[test]
fn canonical_source_subtrees_and_aliases_refuse_while_siblings_are_allowed() {
    let directory = Directory::new();
    let mut store = source(&directory);
    let source = directory.source();
    let child = source.join("child");
    std::fs::create_dir(&child).unwrap();
    let alias = directory.0.join("alias");
    std::os::unix::fs::symlink(&source, &alias).unwrap();
    for destination in [
        source.join("archive.sqlite3"),
        child.join("archive.sqlite3"),
        alias.join("archive.sqlite3"),
        alias.join("child/archive.sqlite3"),
        source.join("runtime.sqlite3"),
    ] {
        assert_eq!(
            store.export_backup(destination),
            Err(BackupError::InvalidDestination)
        );
    }
    let sibling = directory.0.join("source-other");
    std::fs::create_dir(&sibling).unwrap();
    assert_eq!(store.export_backup(sibling.join("archive.sqlite3")), Ok(()));
    assert!(!source.join("archive.sqlite3").exists());
    assert!(!child.join("archive.sqlite3").exists());
}

#[test]
fn source_opened_through_alias_keeps_its_canonical_backup_exclusion() {
    let directory = Directory::new();
    let actual = directory.source();
    std::fs::create_dir(&actual).unwrap();
    let alias = directory.0.join("source-alias");
    std::os::unix::fs::symlink(&actual, &alias).unwrap();
    let mut store = SqliteStore::open(&alias).unwrap();
    assert_eq!(
        store.export_backup(actual.join("archive.sqlite3")),
        Err(BackupError::InvalidDestination)
    );
    assert_eq!(
        store.export_backup(alias.join("archive.sqlite3")),
        Err(BackupError::InvalidDestination)
    );
    assert_eq!(
        store.export_backup(directory.0.join("archive.sqlite3")),
        Ok(())
    );
}

#[test]
fn corrupted_source_schema_checksum_and_owner_are_not_published() {
    for change in [
        "CREATE TABLE unexpected(value TEXT)",
        "UPDATE metadata SET migration_checksum = 'private-sql-marker'",
        "UPDATE metadata SET schema_version = 99",
        "UPDATE metadata SET owner_epoch = x'0000000000000002'",
    ] {
        let directory = Directory::new();
        let mut store = source(&directory);
        let direct = Connection::open(directory.source().join("runtime.sqlite3")).unwrap();
        direct.execute_batch(change).unwrap();
        drop(direct);
        let archive = directory.0.join("archive.sqlite3");
        assert_eq!(
            store.export_backup(&archive),
            Err(BackupError::InvalidDatabase)
        );
        assert!(!archive.exists());
        assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 1);
    }
}

#[test]
fn copied_foreign_key_corruption_is_refused_and_stage_is_removed() {
    let directory = Directory::new();
    let mut store = source(&directory);
    let direct = Connection::open(directory.source().join("runtime.sqlite3")).unwrap();
    direct
        .execute_batch("PRAGMA foreign_keys=OFF; UPDATE threads SET active_execution_id='absent'")
        .unwrap();
    drop(direct);
    let archive = directory.0.join("archive.sqlite3");
    assert_eq!(
        store.export_backup(&archive),
        Err(BackupError::InvalidDatabase)
    );
    assert!(!archive.exists());
    assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 1);
}

#[test]
fn backup_errors_have_only_fixed_non_sensitive_diagnostics() {
    let codes = [
        (
            BackupError::InvalidDestination,
            "backup_destination_invalid",
        ),
        (BackupError::DestinationExists, "backup_destination_exists"),
        (BackupError::InvalidDatabase, "backup_database_invalid"),
        (BackupError::Unavailable, "backup_unavailable"),
        (
            BackupError::PublicationUncertain,
            "backup_publication_uncertain",
        ),
        (
            BackupError::UnsupportedPlatform,
            "backup_platform_unsupported",
        ),
    ];
    for (error, expected) in codes {
        assert_eq!(error.to_string(), expected);
        assert!(std::error::Error::source(&error).is_none());
        assert!(!format!("{error:?}").contains("private-sql-marker"));
    }
}

#[test]
fn restrictive_umask_still_produces_owner_read_write_archive() {
    const CHILD: &str = "B4_BACKUP_RESTRICTIVE_UMASK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        // Every prerequisite is established explicitly; only the export's
        // stage creation must handle the inherited restrictive umask.
        let directory = Directory::new();
        std::fs::set_permissions(&directory.0, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::create_dir(directory.source()).unwrap();
        std::fs::set_permissions(directory.source(), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        for name in ["owner.lock", "runtime.sqlite3"] {
            let path = directory.source().join(name);
            std::fs::write(&path, []).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut store = SqliteStore::open(directory.source()).unwrap();
        let archive = directory.0.join("archive.sqlite3");
        assert_eq!(store.export_backup(&archive), Ok(()));
        assert_eq!(
            std::fs::metadata(archive).unwrap().permissions().mode() & 0o777,
            0o600
        );
        return;
    }
    // Umask is process-global, so isolate it from concurrently running tests.
    let status = std::process::Command::new("/bin/sh")
        .args(["-c", "umask 0777; exec \"$@\"", "b4-backup-umask"])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "restrictive_umask_still_produces_owner_read_write_archive",
        ])
        .env(CHILD, "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "restrictive umask child failed");
}
