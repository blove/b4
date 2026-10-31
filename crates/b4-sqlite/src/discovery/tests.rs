use super::*;
use b4_contract::{
    Definition, EffectId, ExecutionId, ExecutionRevision, RecoveryPolicy, Snapshot, TaskId,
    TaskSpec, ThreadId, TransitionFacts, TurnId,
};
use b4_runtime::Store;
use rusqlite::params;
use serde_json::{Map, Value};
use std::fs::{DirBuilder, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-discovery-private-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(&path).unwrap();
        for name in ["owner.lock", "runtime.sqlite3"] {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            drop(options.open(path.join(name)).unwrap());
        }
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    store: crate::SqliteStore,
    _directory: Directory,
    snapshot: Snapshot,
}

impl Fixture {
    fn new() -> Self {
        let directory = Directory::new();
        let store = crate::SqliteStore::open(&directory.0).unwrap();
        let thread = ThreadId::new("private-thread").unwrap();
        store.create_thread(thread.clone()).unwrap();
        let snapshot = b4_kernel::new_execution(
            ExecutionId::new("private-execution").unwrap(),
            TurnId::new("private-turn").unwrap(),
            store.owner_generation(),
            Definition {
                route: b4_contract::RouteKey::new("/private#workflow").unwrap(),
                revision: ExecutionRevision::new("private-v1").unwrap(),
                batches: vec![vec![TaskSpec {
                    id: TaskId::new("private-task").unwrap(),
                    effect_id: EffectId::new("private-effect").unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: false,
                }]],
                max_attempts: 1,
            },
            Map::new(),
            TransitionFacts { now_ms: 1 },
        )
        .unwrap();
        store.admit(&thread, snapshot.clone()).unwrap();
        store.readiness().unwrap();
        assert_eq!(
            store
                .execution(&snapshot.execution_id)
                .unwrap()
                .unwrap()
                .snapshot,
            snapshot
        );
        Self {
            store,
            _directory: directory,
            snapshot,
        }
    }

    fn replace_snapshot(&self, snapshot: &Snapshot) {
        let connection = self.store.connection.lock().unwrap();
        let changed = connection
            .execute(
                "UPDATE executions SET snapshot = ?1, owner_epoch = ?2 WHERE id = ?3",
                params![
                    serde_json::to_string(snapshot).unwrap(),
                    snapshot.owner_generation.to_be_bytes().as_slice(),
                    snapshot.execution_id.as_str()
                ],
            )
            .unwrap();
        assert_eq!(changed, 1);
    }
}

#[test]
fn metadata_zero_or_replaced_owner_is_refused() {
    for owner in [0_u64, 2] {
        let fixture = Fixture::new();
        fixture
            .store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE metadata SET owner_epoch = ?1",
                [owner.to_be_bytes()],
            )
            .unwrap();
        assert_eq!(
            read(&fixture.store, None).unwrap_err(),
            DiscoveryError::OwnerConflict
        );
    }
}

#[test]
fn metadata_version_or_checksum_is_refused() {
    for sql in [
        "UPDATE metadata SET schema_version = 2",
        "UPDATE metadata SET migration_checksum = 'wrong'",
    ] {
        let fixture = Fixture::new();
        fixture
            .store
            .connection
            .lock()
            .unwrap()
            .execute(sql, [])
            .unwrap();
        assert_eq!(
            read(&fixture.store, None).unwrap_err(),
            DiscoveryError::UnsupportedSchema
        );
    }
}

#[test]
fn missing_metadata_row_is_refused() {
    let fixture = Fixture::new();
    fixture
        .store
        .connection
        .lock()
        .unwrap()
        .execute("DELETE FROM metadata", [])
        .unwrap();
    assert_eq!(
        read(&fixture.store, None).unwrap_err(),
        DiscoveryError::UnsupportedSchema
    );
}

#[test]
fn otherwise_valid_snapshot_over_one_mib_is_a_read_limit() {
    let fixture = Fixture::new();
    let mut large = fixture.snapshot.clone();
    large.values.insert(
        "large".into(),
        Value::String("x".repeat(b4_runtime::DISCOVERY_SNAPSHOT_BYTES)),
    );
    b4_kernel::validate_snapshot(&large).unwrap();
    fixture.replace_snapshot(&large);
    // The original known-ID reader still accepts this valid large record.
    assert_eq!(
        fixture
            .store
            .execution(&large.execution_id)
            .unwrap()
            .unwrap()
            .snapshot,
        large
    );
    assert_eq!(
        read(&fixture.store, None).unwrap_err(),
        DiscoveryError::LimitExceeded
    );
}

#[test]
fn raw_snapshot_depth_and_nodes_are_checked_before_decode() {
    for nodes in [false, true] {
        let fixture = Fixture::new();
        let mut large = fixture.snapshot.clone();
        let mut value = Value::Null;
        if nodes {
            value = Value::Array(vec![Value::Null; b4_runtime::DISCOVERY_JSON_NODES]);
        } else {
            for _ in 0..70 {
                value = Value::Array(vec![value]);
            }
        }
        large.values.insert("bounded".into(), value);
        b4_kernel::validate_snapshot(&large).unwrap();
        fixture.replace_snapshot(&large);
        assert_eq!(
            read(&fixture.store, None).unwrap_err(),
            DiscoveryError::LimitExceeded
        );
    }
}

#[test]
fn indexed_revision_mismatch_is_refused() {
    let fixture = Fixture::new();
    fixture
        .store
        .connection
        .lock()
        .unwrap()
        .execute("UPDATE executions SET revision = ?1", [1_u64.to_be_bytes()])
        .unwrap();
    assert_eq!(
        read(&fixture.store, None).unwrap_err(),
        DiscoveryError::CorruptRecord
    );
}

#[test]
fn future_snapshot_owner_is_an_owner_conflict() {
    let fixture = Fixture::new();
    let mut future = fixture.snapshot.clone();
    future.owner_generation = 2;
    b4_kernel::validate_snapshot(&future).unwrap();
    fixture.replace_snapshot(&future);
    assert_eq!(
        read(&fixture.store, None).unwrap_err(),
        DiscoveryError::OwnerConflict
    );
}

#[test]
fn zero_snapshot_owner_is_kernel_corruption() {
    let fixture = Fixture::new();
    let mut zero = fixture.snapshot.clone();
    zero.owner_generation = 0;
    assert!(b4_kernel::validate_snapshot(&zero).is_err());
    fixture.replace_snapshot(&zero);
    assert_eq!(
        read(&fixture.store, None).unwrap_err(),
        DiscoveryError::CorruptRecord
    );
}

#[test]
fn foreign_pointer_is_refused() {
    let fixture = Fixture::new();
    let foreign = ThreadId::new("private-foreign").unwrap();
    fixture.store.create_thread(foreign.clone()).unwrap();
    fixture
        .store
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE threads SET active_execution_id = ?1 WHERE id = ?2",
            params![fixture.snapshot.execution_id.as_str(), foreign.as_str()],
        )
        .unwrap();
    assert_eq!(
        read(&fixture.store, None).unwrap_err(),
        DiscoveryError::CorruptRecord
    );
}

#[test]
fn persisted_byte_limit_does_not_apply_a_second_canonical_limit() {
    let fixture = Fixture::new();
    let mut snapshot = fixture.snapshot.clone();
    snapshot.values.insert("number".into(), Value::from(100.0));
    snapshot
        .values
        .insert("padding".into(), Value::String(String::new()));
    let empty_len = serde_json::to_string(&snapshot).unwrap().len();
    snapshot.values.insert(
        "padding".into(),
        Value::String("x".repeat(b4_runtime::DISCOVERY_SNAPSHOT_BYTES + 1 - empty_len)),
    );
    b4_kernel::validate_snapshot(&snapshot).unwrap();
    let canonical = serde_json::to_string(&snapshot).unwrap();
    assert_eq!(canonical.len(), b4_runtime::DISCOVERY_SNAPSHOT_BYTES + 1);
    assert_eq!(canonical.matches("\"number\":100.0").count(), 1);
    let raw = canonical.replacen("\"number\":100.0", "\"number\":1e2", 1);
    assert_eq!(raw.len(), b4_runtime::DISCOVERY_SNAPSHOT_BYTES - 1);
    assert_eq!(serde_json::from_str::<Snapshot>(&raw).unwrap(), snapshot);
    fixture
        .store
        .connection
        .lock()
        .unwrap()
        .execute("UPDATE executions SET snapshot = ?1", [raw])
        .unwrap();
    assert_eq!(
        fixture
            .store
            .execution(&snapshot.execution_id)
            .unwrap()
            .unwrap()
            .snapshot,
        snapshot
    );
    let page = read(&fixture.store, None).unwrap();
    assert_eq!(page.scanned_rows, 1);
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].execution_id, snapshot.execution_id);
}
