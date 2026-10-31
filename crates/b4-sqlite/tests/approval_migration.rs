use b4_runtime::{Store, StoreError};
use b4_sqlite::SqliteStore;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const V1: &str = include_str!("../src/schema-v1.sql");
const V2: &str = include_str!("../src/schema-v2.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-approval-migration-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn legacy(directory: &Directory, version: i64, owner: u64) {
    let connection = directory.connection();
    let schema = if version == 1 { V1 } else { V2 };
    connection.execute_batch(schema).unwrap();
    connection
        .execute(
            "INSERT INTO metadata VALUES (1, ?1, ?2, ?3)",
            params![
                version,
                format!("{:x}", Sha256::digest(schema)),
                owner.to_be_bytes().as_slice()
            ],
        )
        .unwrap();
    connection
        .execute("INSERT INTO threads (id) VALUES ('first'), ('second')", [])
        .unwrap();
}

#[test]
fn known_legacy_schemas_upgrade_to_v3_with_stable_distinct_generations() {
    for version in [1, 2] {
        let directory = Directory::new();
        legacy(&directory, version, 7);
        let store = SqliteStore::open(&directory.0).unwrap();
        assert_eq!(store.owner_generation(), 8);
        store.readiness().unwrap();
        let connection = directory.connection();
        let version: i64 = connection
            .query_row("SELECT schema_version FROM metadata", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 4);
        let generations: Vec<String> = connection
            .prepare("SELECT generation FROM threads ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(generations.len(), 2);
        assert_ne!(generations[0], generations[1]);
        for generation in &generations {
            assert_eq!(generation.len(), 64);
            assert!(generation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        }
        drop(connection);
        drop(store);
        let reopened = SqliteStore::open(&directory.0).unwrap();
        assert_eq!(reopened.owner_generation(), 9);
        let connection = directory.connection();
        let actual: Vec<String> = connection
            .prepare("SELECT generation FROM threads ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(actual, generations);
    }
}

#[test]
fn failed_owner_advance_rolls_back_all_legacy_migrations() {
    for version in [1, 2] {
        let directory = Directory::new();
        legacy(&directory, version, u64::MAX);
        let connection = directory.connection();
        let before: Vec<(String, Option<String>)> = connection
            .prepare("SELECT name, sql FROM sqlite_schema ORDER BY name")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(matches!(
            SqliteStore::open(&directory.0),
            Err(StoreError::Unavailable(_))
        ));
        let after: Vec<(String, Option<String>)> = connection
            .prepare("SELECT name, sql FROM sqlite_schema ORDER BY name")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(after, before);
        let metadata: (i64, String, Vec<u8>) = connection
            .query_row(
                "SELECT schema_version, migration_checksum, owner_epoch FROM metadata",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(metadata.0, version);
        assert_eq!(
            metadata.1,
            format!("{:x}", Sha256::digest(if version == 1 { V1 } else { V2 }))
        );
        assert_eq!(metadata.2, u64::MAX.to_be_bytes());
    }
}

#[test]
fn preserved_v2_schema_checksum_is_exact() {
    assert_eq!(
        format!("{:x}", Sha256::digest(V2)),
        "61f7f0a50b9cdfa886e26a2371281a203abdc46574e9be8bd48b843fc0c910f2"
    );
}

#[test]
fn unknown_legacy_layout_or_checksum_fails_before_upgrading_or_advancing_owner() {
    for version in [1, 2] {
        for mutation in [
            "CREATE TABLE unexpected (value TEXT) STRICT",
            "UPDATE metadata SET migration_checksum = 'forged'",
        ] {
            let directory = Directory::new();
            legacy(&directory, version, 7);
            let connection = directory.connection();
            connection.execute_batch(mutation).unwrap();
            assert!(matches!(
                SqliteStore::open(&directory.0),
                Err(StoreError::UnsupportedSchema)
            ));
            let metadata: (i64, Vec<u8>) = connection
                .query_row(
                    "SELECT schema_version, owner_epoch FROM metadata",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(metadata.0, version);
            assert_eq!(metadata.1, 7_u64.to_be_bytes());
            let generation_count: i64 = connection
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('threads') WHERE name = 'generation'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(generation_count, 0);
        }
    }
}
