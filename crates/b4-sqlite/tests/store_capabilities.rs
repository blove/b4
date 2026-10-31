use b4_contract::*;
use b4_runtime::{approval::ApprovalStore, *};
use b4_sqlite::SqliteStore;
use rusqlite::Connection;
use serde_json::Map;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-sqlite-capabilities-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
    fn metadata(&self) -> (i64, String, Vec<u8>) {
        self.connection()
            .query_row(
                "SELECT schema_version, migration_checksum, owner_epoch FROM metadata",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const SQLITE_DECLARATION: StoreCapabilities = StoreCapabilities {
    kind: StoreKind::Sqlite,
    durable_runtime: true,
    fenced_cancellation: true,
    fenced_deletion: true,
};

fn accepted(store: &SqliteStore) -> StoredExecution {
    let thread = ThreadId::new("capability-thread").unwrap();
    store.create_thread(thread.clone()).unwrap();
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new("capability-execution").unwrap(),
        TurnId::new("capability-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/capabilities#workflow").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("pure-task").unwrap(),
                effect_id: EffectId::new("pure-effect").unwrap(),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            }]],
            max_attempts: 1,
        },
        Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    store.admit(&thread, snapshot).unwrap()
}

#[test]
fn sqlite_declaration_is_observational_and_stable_across_reopen() {
    let directory = Directory::new();
    let store = SqliteStore::open(&directory.0).unwrap();
    let current = accepted(&store);
    let before = store.thread_view(&current.thread_id).unwrap();
    let journal = store.journal(&current.snapshot.execution_id).unwrap();
    let metadata = directory.metadata();
    let owner = store.owner_generation();
    for _ in 0..3 {
        assert_eq!(store.capabilities(), SQLITE_DECLARATION);
    }
    assert!(store.approval_durable());
    assert_eq!(store.thread_view(&current.thread_id).unwrap(), before);
    assert_eq!(
        store.journal(&current.snapshot.execution_id).unwrap(),
        journal
    );
    assert_eq!(directory.metadata(), metadata);
    assert_eq!(store.owner_generation(), owner);
    drop(store);

    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(reopened.owner_generation(), owner + 1);
    assert_eq!(reopened.capabilities(), SQLITE_DECLARATION);
    assert!(reopened.approval_durable());
    assert_eq!(reopened.thread_view(&current.thread_id).unwrap(), before);
    assert_eq!(
        reopened.journal(&current.snapshot.execution_id).unwrap(),
        journal
    );
    reopened.readiness().unwrap();
}

#[test]
fn sqlite_advertised_cancellation_and_deletion_use_real_fenced_ports() {
    let directory = Directory::new();
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(store.capabilities(), SQLITE_DECLARATION);
    let current = accepted(&store);
    let view = store.thread_view(&current.thread_id).unwrap().unwrap();
    let target = RunCancellationTarget::from_view(&view).unwrap();
    let cancelled = store
        .cancel_run_fenced(&target, TransitionFacts { now_ms: 2 })
        .unwrap();
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert!(cancelled.snapshot.attempt_history.is_empty());
    assert!(cancelled.snapshot.output.is_none());
    let view = store.thread_view(&current.thread_id).unwrap().unwrap();
    store
        .delete_thread_fenced(&ThreadDeletionTarget::from_view(&view).unwrap())
        .unwrap();
    assert!(store.thread(&current.thread_id).unwrap().is_none());
    assert!(store
        .execution(&current.snapshot.execution_id)
        .unwrap()
        .is_none());
    assert!(store
        .journal(&current.snapshot.execution_id)
        .unwrap()
        .is_empty());
    assert_eq!(store.capabilities(), SQLITE_DECLARATION);
    store.readiness().unwrap();
}

#[test]
fn capability_declarations_do_not_replace_runtime_health_checks() {
    let directory = Directory::new();
    let store = SqliteStore::open(&directory.0).unwrap();
    let (version, _, _) = directory.metadata();
    let connection = directory.connection();
    connection
        .execute("UPDATE metadata SET schema_version = 999", [])
        .unwrap();
    assert_eq!(store.readiness(), Err(StoreError::UnsupportedSchema));
    let damaged_metadata = directory.metadata();
    assert_eq!(store.capabilities(), SQLITE_DECLARATION);
    assert!(store.approval_durable());
    assert_eq!(directory.metadata(), damaged_metadata);
    connection
        .execute("UPDATE metadata SET schema_version = ?1", [version])
        .unwrap();
    store.readiness().unwrap();
}
