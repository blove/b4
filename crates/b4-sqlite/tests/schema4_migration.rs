//! Native fresh/migration schema seam qualifiers using a genuine prior archive.
//! The first version/presence assertion in each parent is its initial oracle.
//! Later schema shape assertions have no initial RED credit when unreachable.
//! No new permission record, continuation, grant, or migration fault is fabricated.

#![cfg(unix)]

use b4_contract::ThreadId;
use b4_runtime::{Store, ThreadView};
use b4_sqlite::SqliteStore;
use rusqlite::{params, types::Value as SqlValue, Connection, OpenFlags};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, DirBuilder, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const ARCHIVE: &[u8] = include_bytes!("fixtures/schema3-genuine-approvals.sqlite3");
const ARCHIVE_SHA256: &str = "a7906755f97299091a21835e7fef97d3ea971084ff8190c947f71790e9d7ef15";
const SCHEMA3_SHA256: &str = "98dd75f10f1560859f13742a0059f4e0c4f9f8fdcf7a701c731e0a49837353c6";
// Frozen prior Source, not an include of future schema.sql or a fabricated writer.
const SCHEMA3_SQL: &str = r#"CREATE TABLE metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version INTEGER NOT NULL,
    migration_checksum TEXT NOT NULL,
    owner_epoch BLOB NOT NULL CHECK (length(owner_epoch) = 8)
) STRICT;

CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    active_execution_id TEXT REFERENCES executions(id)
) STRICT;

CREATE TABLE executions (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id),
    revision BLOB NOT NULL CHECK (length(revision) = 8),
    owner_epoch BLOB NOT NULL CHECK (length(owner_epoch) = 8),
    snapshot TEXT NOT NULL
) STRICT;

CREATE TABLE journal (
    sequence INTEGER PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('thread', 'admit', 'commit')),
    thread_id TEXT NOT NULL REFERENCES threads(id),
    execution_id TEXT REFERENCES executions(id),
    revision BLOB CHECK (revision IS NULL OR length(revision) = 8),
    owner_epoch BLOB NOT NULL CHECK (length(owner_epoch) = 8),
    status TEXT,
    updated_at_ms BLOB NOT NULL CHECK (length(updated_at_ms) = 8),
    payload TEXT,
    CHECK ((kind = 'thread' AND execution_id IS NULL AND revision IS NULL AND status IS NULL AND payload IS NULL)
        OR (kind IN ('admit', 'commit') AND execution_id IS NOT NULL AND revision IS NOT NULL AND status IS NOT NULL AND payload IS NOT NULL))
) STRICT;

ALTER TABLE threads ADD COLUMN created_at_ms BLOB NOT NULL DEFAULT x'0000000000000000' CHECK (length(created_at_ms) = 8);
ALTER TABLE threads ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}';
ALTER TABLE threads ADD COLUMN origin TEXT;
ALTER TABLE threads ADD COLUMN resource_revision BLOB NOT NULL DEFAULT x'0000000000000000' CHECK (length(resource_revision) = 8);
ALTER TABLE threads ADD COLUMN generation TEXT NOT NULL DEFAULT '0000000000000000000000000000000000000000000000000000000000000000' CHECK (length(generation) = 64 AND generation NOT GLOB '*[^0-9a-f]*');

CREATE TABLE approval_grants (
    execution_id TEXT NOT NULL REFERENCES executions(id),
    interrupt_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    thread_id TEXT NOT NULL REFERENCES threads(id),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    task_parking_revision BLOB NOT NULL CHECK (length(task_parking_revision) = 8),
    consumed INTEGER NOT NULL CHECK (consumed IN (0, 1)),
    record TEXT NOT NULL,
    PRIMARY KEY (execution_id, interrupt_id)
) STRICT;

CREATE TABLE approval_acceptances (
    execution_id TEXT NOT NULL REFERENCES executions(id),
    turn_id TEXT NOT NULL,
    thread_id TEXT NOT NULL REFERENCES threads(id),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    previous_revision BLOB NOT NULL CHECK (length(previous_revision) = 8),
    definition_revision TEXT NOT NULL,
    accepted_at_ms BLOB NOT NULL CHECK (length(accepted_at_ms) = 8),
    record TEXT NOT NULL,
    PRIMARY KEY (execution_id, turn_id)
) STRICT;
"#;
// Proposed reviewed schema4 contract; used only as a post-oracle DDL reference.
const SCHEMA4_EXTENSION: &str = r#"CREATE TABLE permission_acceptances (
    execution_id TEXT NOT NULL REFERENCES executions(id) CHECK (length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256),
    turn_id TEXT NOT NULL CHECK (length(CAST(turn_id AS BLOB)) BETWEEN 1 AND 256),
    thread_id TEXT NOT NULL REFERENCES threads(id) CHECK (length(CAST(thread_id AS BLOB)) BETWEEN 1 AND 256),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    previous_revision BLOB NOT NULL CHECK (length(previous_revision) = 8),
    committed_revision BLOB NOT NULL CHECK (length(committed_revision) = 8),
    record_version INTEGER NOT NULL CHECK (record_version = 1),
    record TEXT NOT NULL CHECK (length(CAST(record AS BLOB)) BETWEEN 1 AND 65536),
    PRIMARY KEY (execution_id, turn_id),
    UNIQUE (execution_id, previous_revision)
) STRICT;
CREATE INDEX permission_acceptances_by_thread ON permission_acceptances(thread_id, execution_id, turn_id);

CREATE TABLE permission_decisions (
    permission_id TEXT NOT NULL CHECK (length(CAST(permission_id AS BLOB)) BETWEEN 1 AND 256),
    thread_id TEXT NOT NULL REFERENCES threads(id) CHECK (length(CAST(thread_id AS BLOB)) BETWEEN 1 AND 256),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    scope_digest TEXT NOT NULL CHECK (length(scope_digest) = 64 AND scope_digest NOT GLOB '*[^0-9a-f]*'),
    record_version INTEGER NOT NULL CHECK (record_version = 1),
    record TEXT NOT NULL CHECK (length(CAST(record AS BLOB)) BETWEEN 1 AND 65536),
    PRIMARY KEY (permission_id),
    UNIQUE (thread_id, thread_generation, scope_digest)
) STRICT;
CREATE INDEX permission_decisions_by_thread ON permission_decisions(thread_id, thread_generation, permission_id);

CREATE TABLE permission_uses (
    execution_id TEXT NOT NULL REFERENCES executions(id) CHECK (length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256),
    task_id TEXT NOT NULL CHECK (length(CAST(task_id AS BLOB)) BETWEEN 1 AND 256),
    use_id TEXT NOT NULL CHECK (length(CAST(use_id AS BLOB)) BETWEEN 1 AND 256),
    thread_id TEXT NOT NULL REFERENCES threads(id) CHECK (length(CAST(thread_id AS BLOB)) BETWEEN 1 AND 256),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    permission_id TEXT NOT NULL REFERENCES permission_decisions(permission_id) CHECK (length(CAST(permission_id AS BLOB)) BETWEEN 1 AND 256),
    turn_id TEXT NOT NULL CHECK (length(CAST(turn_id AS BLOB)) BETWEEN 1 AND 256),
    authorization_revision BLOB NOT NULL CHECK (length(authorization_revision) = 8),
    record_version INTEGER NOT NULL CHECK (record_version = 1),
    record TEXT NOT NULL CHECK (length(CAST(record AS BLOB)) BETWEEN 1 AND 16384),
    PRIMARY KEY (execution_id, task_id),
    UNIQUE (use_id)
) STRICT;
CREATE INDEX permission_uses_by_thread ON permission_uses(thread_id, execution_id, task_id);
CREATE INDEX permission_uses_by_permission ON permission_uses(permission_id, execution_id, task_id);
"#;

const OLD_TABLE_QUERIES: [(&str, &str); 5] = [
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
const NEW_TABLES: [&str; 3] = [
    "permission_acceptances",
    "permission_decisions",
    "permission_uses",
];
const SEED_THREADS: [&str; 2] = ["schema3-pending-thread", "schema3-once-thread"];
type LogicalCells = Vec<(String, Vec<Vec<SqlValue>>)>;
type SchemaObjects = Vec<(String, String, String, Option<String>)>;
type Columns = Vec<(String, String, i64, i64)>;
type ForeignKeys = Vec<(String, String, String, String, String, String)>;
type Indexes = Vec<(String, i64, String, i64, Vec<String>)>;

// Safe fixed diagnostics never format SQL errors, paths, raw rows, or grants.
fn must<T, E>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|_| panic!("schema4 qualifier operation failed"))
}
fn present<T>(value: Option<T>) -> T {
    value.unwrap_or_else(|| panic!("schema4 qualifier expected projection missing"))
}
fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn fresh() -> Self {
        let sequence = DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "b4-schema4-qualifier-{}-{sequence}",
            std::process::id()
        ));
        let mut builder = DirBuilder::new();
        builder.mode(0o700);
        must(builder.create(&path));
        must(fs::set_permissions(
            &path,
            fs::Permissions::from_mode(0o700),
        ));
        assert!(
            must(fs::symlink_metadata(&path)).is_dir()
                && must(fs::metadata(&path)).permissions().mode() & 0o777 == 0o700,
            "qualifier owns a fresh private directory700"
        );
        Self(path)
    }
    fn database(&self) -> PathBuf {
        self.0.join("runtime.sqlite3")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn install_archive(directory: &Directory) {
    assert!(
        ARCHIVE.len() == 45056
            && sha256(ARCHIVE) == ARCHIVE_SHA256
            && sha256(SCHEMA3_SQL.as_bytes()) == SCHEMA3_SHA256,
        "migration input must be the exact qualified prior public-writer archive"
    );
    let mut file = must(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.database()),
    );
    must(file.write_all(ARCHIVE));
    must(file.sync_all());
    drop(file);
    must(fs::set_permissions(
        directory.database(),
        fs::Permissions::from_mode(0o600),
    ));
    assert!(
        must(fs::metadata(directory.database()))
            .permissions()
            .mode()
            & 0o777
            == 0o600,
        "installed historical fixture must remain a private file600"
    );
}
fn readonly(path: &Path) -> Connection {
    must(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    ))
}

#[derive(PartialEq)]
struct Metadata {
    id: i64,
    version: i64,
    checksum: String,
    owner: Vec<u8>,
}
impl Metadata {
    fn owner_number(&self) -> u64 {
        assert!(
            self.owner.len() == 8,
            "owner encoding must be exactly eight bytes"
        );
        u64::from_be_bytes(must(self.owner.as_slice().try_into()))
    }
}
struct Cut {
    metadata: Metadata,
    old_cells: LogicalCells,
    schema: SchemaObjects,
    new_tables_present: [bool; 3],
}
fn schema_objects(db: &Connection) -> SchemaObjects {
    let mut statement =
        must(db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name"));
    let rows = must(statement.query_map([], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    }));
    must(rows.collect::<Result<Vec<_>, _>>())
}
// Value retains SQLite TEXT/JSON bytes and BLOB bytes with no reserialization.
fn old_cells(db: &Connection) -> LogicalCells {
    let mut tables = Vec::new();
    for (table, query) in OLD_TABLE_QUERIES {
        let mut statement = must(db.prepare(query));
        let count = statement.column_count();
        let rows = must(statement.query_map([], |row| {
            (0..count)
                .map(|column| row.get(column))
                .collect::<Result<Vec<SqlValue>, _>>()
        }));
        tables.push((table.to_owned(), must(rows.collect::<Result<Vec<_>, _>>())));
    }
    tables
}
fn cut(path: &Path) -> Cut {
    let db = readonly(path);
    let count: i64 = must(db.query_row("SELECT count(*) FROM metadata", [], |row| row.get(0)));
    let metadata = must(db.query_row(
        "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata",
        [],
        |row| {
            Ok(Metadata {
                id: row.get(0)?,
                version: row.get(1)?,
                checksum: row.get(2)?,
                owner: row.get(3)?,
            })
        },
    ));
    assert!(
        count == 1 && metadata.id == 1,
        "metadata must retain its sole fixed identity"
    );
    let mut statement = must(db.prepare("PRAGMA table_info(metadata)"));
    let names = must(statement.query_map([], |row| row.get::<_, String>(1)));
    let names = must(names.collect::<Result<Vec<_>, _>>());
    assert!(
        names == ["id", "schema_version", "migration_checksum", "owner_epoch"],
        "schema seam must preserve exact prior metadata columns"
    );
    drop(statement);
    // A missing permission table returns false at schema3 rather than SQL error.
    let mut new_tables_present = [false; 3];
    for (index, table) in NEW_TABLES.iter().enumerate() {
        let count: i64 = must(db.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get(0),
        ));
        new_tables_present[index] = count == 1;
    }
    let result = Cut {
        metadata,
        old_cells: old_cells(&db),
        schema: schema_objects(&db),
        new_tables_present,
    };
    must(db.close());
    result
}
fn ordinary_views(store: &SqliteStore, migrated: bool) -> Vec<ThreadView> {
    if migrated {
        SEED_THREADS
            .iter()
            .map(|id| present(must(store.thread_view(&must(ThreadId::new(*id))))))
            .collect()
    } else {
        assert!(
            must(store.thread_view(&must(ThreadId::new("schema4-absent-thread")))).is_none(),
            "fresh public writer must support an ordinary absent-thread projection"
        );
        Vec::new()
    }
}
struct Subject {
    directory: Directory,
    observed: Cut,
}
fn subject(migrated: bool) -> Subject {
    let directory = Directory::fresh();
    let original = if migrated {
        install_archive(&directory);
        let original = cut(&directory.database());
        assert!(
            original.metadata.version == 3
                && original.metadata.checksum == SCHEMA3_SHA256
                && original.metadata.owner_number() == 1
                && original.new_tables_present == [false; 3]
                && original
                    .old_cells
                    .iter()
                    .map(|(_, rows)| rows.len())
                    .collect::<Vec<_>>()
                    == [2, 2, 9, 2, 1],
            "migration setup must be the qualified real pending and Once schema3 cut"
        );
        Some(original)
    } else {
        None
    };
    let previous_owner = original
        .as_ref()
        .map_or(0, |cut| cut.metadata.owner_number());
    let first_owner = present(previous_owner.checked_add(1));
    let next_owner = present(first_owner.checked_add(1));
    // Direct owned stores have no service, application, or Arc aliases to retire.
    let first = must(SqliteStore::open(&directory.0));
    assert!(
        first.owner_generation() == first_owner,
        "first writer must acquire the genuine successor epoch"
    );
    let first_views = ordinary_views(&first, migrated);
    let first_cut = cut(&directory.database());
    assert!(
        first_cut.metadata.owner_number() == first_owner,
        "first writer metadata must carry its acquired epoch"
    );
    if let Some(original) = &original {
        assert!(
            first_cut.old_cells == original.old_cells && first_cut.metadata.id == original.metadata.id,
            "migration may only change prior metadata version checksum and owner; old five tables stay byte exact"
        );
    } else {
        assert!(
            first_cut.old_cells.iter().all(|(_, rows)| rows.is_empty()),
            "fresh schema-only subject must have no synthetic authority rows"
        );
    }
    drop(first);
    // Genuine next writer acquisition and ordinary reads prove normal ownership closure.
    let next = must(SqliteStore::open(&directory.0));
    assert!(
        next.owner_generation() == next_owner,
        "next writer must acquire the second genuine successor epoch"
    );
    let next_views = ordinary_views(&next, migrated);
    let next_cut = cut(&directory.database());
    assert!(
        next_cut.metadata.owner_number() == next_owner
            && next_cut.metadata.id == first_cut.metadata.id
            && next_cut.metadata.version == first_cut.metadata.version
            && next_cut.metadata.checksum == first_cut.metadata.checksum
            && next_cut.old_cells == first_cut.old_cells
            && next_cut.schema == first_cut.schema
            && next_cut.new_tables_present == first_cut.new_tables_present
            && next_views == first_views,
        "normal reopen may only advance global owner while retaining exact prior rows and projections"
    );
    drop(next_views);
    drop(first_views);
    drop(next);
    drop(original);
    // Only plain captured data and a private directory survive the writer retirements.
    Subject {
        directory,
        observed: next_cut,
    }
}

fn columns(db: &Connection, table: &str) -> Columns {
    let mut statement = must(db.prepare(&format!("PRAGMA table_info({table})")));
    let rows = must(statement.query_map([], |row| {
        Ok((row.get(1)?, row.get(2)?, row.get(3)?, row.get(5)?))
    }));
    must(rows.collect::<Result<Vec<_>, _>>())
}
fn foreign_keys(db: &Connection, table: &str) -> ForeignKeys {
    let mut statement = must(db.prepare(&format!("PRAGMA foreign_key_list({table})")));
    let rows = must(statement.query_map([], |row| {
        Ok((
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
        ))
    }));
    let mut result = must(rows.collect::<Result<Vec<_>, _>>());
    result.sort();
    result
}
fn indexes(db: &Connection, table: &str) -> Indexes {
    let mut statement = must(db.prepare(&format!("PRAGMA index_list({table})")));
    let rows = must(statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
        ))
    }));
    let rows = must(rows.collect::<Result<Vec<_>, _>>());
    drop(statement);
    let mut result = Vec::new();
    for (name, unique, origin, partial) in rows {
        let mut statement = must(db.prepare(&format!("PRAGMA index_info({name})")));
        let columns = must(statement.query_map([], |row| row.get::<_, String>(2)));
        result.push((
            name,
            unique,
            origin,
            partial,
            must(columns.collect::<Result<Vec<_>, _>>()),
        ));
    }
    result.sort();
    result
}
fn expected_columns(table: &str) -> Columns {
    let values: &[(&str, &str, i64)] = match table {
        "permission_acceptances" => &[
            ("execution_id", "TEXT", 1),
            ("turn_id", "TEXT", 2),
            ("thread_id", "TEXT", 0),
            ("thread_generation", "TEXT", 0),
            ("previous_revision", "BLOB", 0),
            ("committed_revision", "BLOB", 0),
            ("record_version", "INTEGER", 0),
            ("record", "TEXT", 0),
        ],
        "permission_decisions" => &[
            ("permission_id", "TEXT", 1),
            ("thread_id", "TEXT", 0),
            ("thread_generation", "TEXT", 0),
            ("scope_digest", "TEXT", 0),
            ("record_version", "INTEGER", 0),
            ("record", "TEXT", 0),
        ],
        "permission_uses" => &[
            ("execution_id", "TEXT", 1),
            ("task_id", "TEXT", 2),
            ("use_id", "TEXT", 0),
            ("thread_id", "TEXT", 0),
            ("thread_generation", "TEXT", 0),
            ("permission_id", "TEXT", 0),
            ("turn_id", "TEXT", 0),
            ("authorization_revision", "BLOB", 0),
            ("record_version", "INTEGER", 0),
            ("record", "TEXT", 0),
        ],
        _ => panic!("schema4 qualifier unexpected fixed table"),
    };
    values
        .iter()
        .map(|(name, kind, pk)| (name.to_string(), kind.to_string(), 1, *pk))
        .collect()
}
// Called ONLY after this parent's first feature oracle succeeds. No rows inserted.
fn assert_schema4_shape(subject: &Subject) {
    let expected = must(Connection::open_in_memory());
    must(expected.execute_batch(SCHEMA3_SQL));
    must(expected.execute_batch(SCHEMA4_EXTENSION));
    let expected_catalog = schema_objects(&expected);
    assert!(
        subject.observed.schema == expected_catalog,
        "schema4 must preserve full old DDL and match exact new STRICT checks keys references and indexes"
    );
    let db = readonly(&subject.directory.database());
    for table in NEW_TABLES {
        let count_query = match table {
            "permission_acceptances" => "SELECT count(*) FROM permission_acceptances",
            "permission_decisions" => "SELECT count(*) FROM permission_decisions",
            "permission_uses" => "SELECT count(*) FROM permission_uses",
            _ => panic!("schema4 qualifier unexpected fixed table"),
        };
        let count: i64 = must(db.query_row(count_query, [], |row| row.get(0)));
        assert!(
            count == 0,
            "schema-only fresh or migrated subject must have no new permission authority rows"
        );
        assert!(
            columns(&db, table) == expected_columns(table),
            "schema4 column types not-null and primary-key positions must be exact"
        );
        let strict: i64 = must(db.query_row(
            "SELECT strict FROM pragma_table_list WHERE schema = 'main' AND name = ?1",
            params![table],
            |row| row.get(0),
        ));
        assert!(strict == 1, "all new permission tables must be STRICT");
        assert!(
            foreign_keys(&db, table) == foreign_keys(&expected, table),
            "schema4 foreign keys must be exact without cascading authority"
        );
        assert!(
            indexes(&db, table) == indexes(&expected, table),
            "schema4 primary unique and explicit lookup indexes must be exact"
        );
    }
    let current_schema_checksum = sha256(include_bytes!("../src/schema.sql"));
    assert!(
        subject.observed.metadata.checksum == current_schema_checksum
            && subject.observed.metadata.checksum != SCHEMA3_SHA256,
        "schema4 metadata checksum must bind the actual current writer schema Source"
    );
    must(db.close());
    must(expected.close());
}

#[test]
fn fresh_writer_reports_schema4_after_normal_reopen() {
    let subject = subject(false);
    assert!(
        subject.observed.metadata.version == 4,
        "fresh public writer must report schema4 after normal reopen"
    );
    assert_schema4_shape(&subject);
}
#[test]
fn genuine_schema3_migrates_to4_without_rewriting_old_rows() {
    let subject = subject(true);
    assert!(
        subject.observed.metadata.version == 4,
        "genuine prior schema3 archive must migrate to schema4 after normal reopen"
    );
    assert_schema4_shape(&subject);
}
#[test]
fn fresh_writer_has_permission_acceptances_table() {
    let subject = subject(false);
    assert!(
        subject.observed.new_tables_present[0],
        "fresh public writer must create permission_acceptances"
    );
    assert_schema4_shape(&subject);
}
#[test]
fn fresh_writer_has_permission_decisions_table() {
    let subject = subject(false);
    assert!(
        subject.observed.new_tables_present[1],
        "fresh public writer must create permission_decisions"
    );
    assert_schema4_shape(&subject);
}
#[test]
fn fresh_writer_has_permission_uses_table() {
    let subject = subject(false);
    assert!(
        subject.observed.new_tables_present[2],
        "fresh public writer must create permission_uses"
    );
    assert_schema4_shape(&subject);
}
#[test]
fn genuine_schema3_migration_has_permission_acceptances_table() {
    let subject = subject(true);
    assert!(
        subject.observed.new_tables_present[0],
        "genuine schema3 migration must create permission_acceptances"
    );
    assert_schema4_shape(&subject);
}
#[test]
fn genuine_schema3_migration_has_permission_decisions_table() {
    let subject = subject(true);
    assert!(
        subject.observed.new_tables_present[1],
        "genuine schema3 migration must create permission_decisions"
    );
    assert_schema4_shape(&subject);
}
#[test]
fn genuine_schema3_migration_has_permission_uses_table() {
    let subject = subject(true);
    assert!(
        subject.observed.new_tables_present[2],
        "genuine schema3 migration must create permission_uses"
    );
    assert_schema4_shape(&subject);
}
