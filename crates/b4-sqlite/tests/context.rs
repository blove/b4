use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{MemoryStore, Runtime, RuntimeError, Store, StoreError, ThreadOrigin, ThreadSeed};
use b4_sqlite::SqliteStore;
use rusqlite::{params, Connection};
use serde_json::{json, Map};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const V1: &str = include_str!("../src/schema-v1.sql");
const V2: &str = include_str!("../src/schema-v2.sql");
const MIGRATION_V2: &str = include_str!("../src/migration-v2.sql");
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-context-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn database(&self) -> PathBuf {
        self.0.join("runtime.sqlite3")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn thread() -> ThreadId {
    ThreadId::new("thread").unwrap()
}
fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: "app".into(),
        tenant: "tenant".into(),
        principal: "principal".into(),
    }
}
fn seed() -> ThreadSeed {
    ThreadSeed {
        id: thread(),
        created_at_ms: u64::MAX,
        metadata: json!({"origin":{"tenant":"forged"},"application":"forged","tag":[1,false]})
            .as_object()
            .unwrap()
            .clone(),
        origin: Some(origin()),
    }
}
fn initial(name: &str, owner: u64) -> Snapshot {
    new_execution(
        ExecutionId::new(name).unwrap(),
        TurnId::new(format!("turn-{name}")).unwrap(),
        owner,
        Definition {
            route: RouteKey::new("/context#agent").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![],
            max_attempts: 1,
        },
        Map::new(),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap()
}
fn with_backends(f: impl Fn(&dyn Store)) {
    f(&MemoryStore::new());
    let directory = Directory::new();
    f(&SqliteStore::open(&directory.0).unwrap());
}

#[test]
fn both_backends_preserve_server_origin_and_user_metadata_in_coherent_views() {
    with_backends(|store| {
        store.readiness().unwrap();
        assert_eq!(store.thread_view(&thread()).unwrap(), None);
        let created = store.create_thread_with(seed()).unwrap();
        assert_eq!(created.created_at_ms, u64::MAX);
        assert_eq!(created.metadata, seed().metadata);
        assert_eq!(created.origin, Some(origin()));
        assert_eq!(created.resource_revision, 0);
        let empty = store.thread_view(&thread()).unwrap().unwrap();
        assert_eq!(empty.thread, created);
        assert_eq!(empty.execution, None);
        let accepted = store
            .admit_fenced(
                &thread(),
                &created.generation,
                0,
                initial("execution", store.owner_generation()),
            )
            .unwrap();
        let view = store.thread_view(&thread()).unwrap().unwrap();
        assert_eq!(view.thread.resource_revision, 1);
        assert_eq!(view.execution, Some(accepted));
        assert_eq!(view.thread.origin, Some(origin()));
        assert_eq!(view.thread.metadata, seed().metadata);
    });
}

#[test]
fn legacy_native_creation_retains_unknown_origin_and_empty_context() {
    with_backends(|store| {
        let created = store.create_thread(thread()).unwrap();
        assert_eq!(created.created_at_ms, 0);
        assert_eq!(created.metadata, Map::new());
        assert_eq!(created.origin, None);
        assert_eq!(created.resource_revision, 0);
        assert!(store
            .admit(&thread(), initial("execution", store.owner_generation()))
            .is_ok());
    });
}

#[test]
fn origin_components_are_bounded_nonempty_and_control_free() {
    with_backends(|store| {
        for bad in [
            "".to_owned(),
            "a".repeat(257),
            "é".repeat(129),
            "tenant\n".into(),
            "tenant\u{7f}".into(),
            "tenant\u{85}".into(),
        ] {
            for field in 0..3 {
                let mut candidate = seed();
                let origin = candidate.origin.as_mut().unwrap();
                match field {
                    0 => origin.application = bad.clone(),
                    1 => origin.tenant = bad.clone(),
                    _ => origin.principal = bad.clone(),
                }
                assert!(matches!(
                    store.create_thread_with(candidate),
                    Err(StoreError::InvalidThread(_))
                ));
                assert_eq!(store.thread(&thread()).unwrap(), None);
            }
        }
        let mut valid = seed();
        valid.origin.as_mut().unwrap().tenant = "é".repeat(128);
        store.create_thread_with(valid).unwrap();
    });
}

#[test]
fn captured_resource_revision_rejects_admission_after_execution_changes() {
    with_backends(|store| {
        store.create_thread_with(seed()).unwrap();
        let captured = store.thread_view(&thread()).unwrap().unwrap().thread;
        let admitted = store
            .admit(&thread(), initial("first", store.owner_generation()))
            .unwrap();
        let plan = transition(
            &admitted.snapshot,
            KernelCommand::Start,
            TransitionFacts { now_ms: 101 },
        )
        .unwrap();
        store
            .commit(
                &admitted.snapshot.execution_id,
                (&admitted.snapshot).into(),
                plan,
            )
            .unwrap();
        assert_eq!(
            store.admit_fenced(
                &thread(),
                &captured.generation,
                captured.resource_revision,
                initial("stale", store.owner_generation())
            ),
            Err(StoreError::RevisionConflict)
        );
        assert_eq!(
            store
                .execution(&ExecutionId::new("stale").unwrap())
                .unwrap(),
            None
        );
        let revision = store
            .thread_view(&thread())
            .unwrap()
            .unwrap()
            .thread
            .resource_revision;
        assert_eq!(revision, 2);
        store
            .admit_fenced(
                &thread(),
                &captured.generation,
                revision,
                initial("fresh", store.owner_generation()),
            )
            .unwrap();
        assert_eq!(
            store.thread(&thread()).unwrap().unwrap().resource_revision,
            3
        );
    });
}

#[test]
fn competing_policy_captures_allow_only_one_fenced_admission() {
    with_backends(|store| {
        let created = store.create_thread_with(seed()).unwrap();
        let barrier = std::sync::Barrier::new(2);
        let outcomes = std::thread::scope(|scope| {
            (0..2)
                .map(|index| {
                    let barrier = &barrier;
                    let generation = &created.generation;
                    scope.spawn(move || {
                        barrier.wait();
                        store.admit_fenced(
                            &thread(),
                            generation,
                            0,
                            initial(&format!("execution-{index}"), store.owner_generation()),
                        )
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| **result == Err(StoreError::RevisionConflict))
                .count(),
            1
        );
        assert_eq!(
            store.thread(&thread()).unwrap().unwrap().resource_revision,
            1
        );
    });
}

#[test]
fn concurrent_thread_views_keep_latest_execution_and_revision_coherent() {
    with_backends(|store| {
        store.create_thread_with(seed()).unwrap();
        let barrier = std::sync::Barrier::new(2);
        let finished = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                for index in 0..20 {
                    let admitted = store
                        .admit(
                            &thread(),
                            initial(&format!("execution-{index}"), store.owner_generation()),
                        )
                        .unwrap();
                    let plan = transition(
                        &admitted.snapshot,
                        KernelCommand::Start,
                        TransitionFacts { now_ms: 101 },
                    )
                    .unwrap();
                    store
                        .commit(
                            &admitted.snapshot.execution_id,
                            (&admitted.snapshot).into(),
                            plan,
                        )
                        .unwrap();
                }
                finished.store(true, Ordering::Release);
            });
            barrier.wait();
            loop {
                let view = store.thread_view(&thread()).unwrap().unwrap();
                assert_eq!(view.thread.origin, Some(origin()));
                if let Some(execution) = view.execution {
                    let index: u64 = execution
                        .snapshot
                        .execution_id
                        .as_str()
                        .strip_prefix("execution-")
                        .unwrap()
                        .parse()
                        .unwrap();
                    assert_eq!(
                        view.thread.active_execution_id,
                        Some(execution.snapshot.execution_id)
                    );
                    assert_eq!(
                        view.thread.resource_revision,
                        index * 2 + execution.snapshot.revision + 1
                    );
                } else {
                    assert_eq!(view.thread.resource_revision, 0);
                }
                if finished.load(Ordering::Acquire) {
                    break;
                }
                std::thread::yield_now();
            }
        });
        assert_eq!(
            store.thread(&thread()).unwrap().unwrap().resource_revision,
            40
        );
    });
}

#[test]
fn no_op_commit_keeps_resource_revision_and_context() {
    with_backends(|store| {
        store.create_thread_with(seed()).unwrap();
        let admitted = store
            .admit(&thread(), initial("first", store.owner_generation()))
            .unwrap();
        let plan = CommitPlan {
            expected_revision: admitted.snapshot.revision,
            snapshot: admitted.snapshot.clone(),
            dispatchable: vec![],
        };
        let before = store.thread_view(&thread()).unwrap();
        store
            .commit(
                &admitted.snapshot.execution_id,
                (&admitted.snapshot).into(),
                plan,
            )
            .unwrap();
        assert_eq!(store.thread_view(&thread()).unwrap(), before);
    });
}

#[test]
fn runtime_delegates_fenced_admission() {
    let runtime = Runtime::new(Arc::new(MemoryStore::new()));
    let created = runtime.store().create_thread_with(seed()).unwrap();
    assert_eq!(
        runtime.admit_fenced(&thread(), &created.generation, 1, initial("stale", 1)),
        Err(RuntimeError::Store(StoreError::RevisionConflict))
    );
    runtime
        .admit_fenced(&thread(), &created.generation, 0, initial("fresh", 1))
        .unwrap();
}

fn install_v1(directory: &Directory, epoch: u64, mutation: Option<&str>) -> Snapshot {
    let connection = Connection::open(directory.database()).unwrap();
    connection.execute_batch(V1).unwrap();
    connection
        .execute(
            "INSERT INTO metadata VALUES (1, 1, ?1, ?2)",
            params![
                format!("{:x}", Sha256::digest(V1.as_bytes())),
                epoch.to_be_bytes().as_slice()
            ],
        )
        .unwrap();
    let accepted = initial("old-execution", epoch);
    let completed = transition(
        &accepted,
        KernelCommand::Start,
        TransitionFacts { now_ms: 101 },
    )
    .unwrap()
    .snapshot;
    connection
        .execute("INSERT INTO threads (id) VALUES ('thread')", [])
        .unwrap();
    connection
        .execute(
            "INSERT INTO executions VALUES (?1, 'thread', ?2, ?3, ?4)",
            params![
                completed.execution_id.as_str(),
                completed.revision.to_be_bytes().as_slice(),
                epoch.to_be_bytes().as_slice(),
                serde_json::to_string(&completed).unwrap()
            ],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE threads SET active_execution_id = 'old-execution'",
            [],
        )
        .unwrap();
    for (kind, snapshot) in [("admit", accepted), ("commit", completed.clone())] {
        let entry = b4_runtime::JournalEntry::from(&snapshot);
        connection.execute("INSERT INTO journal (kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload) VALUES (?1, 'thread', ?2, ?3, ?4, ?5, ?6, ?7)", params![kind, snapshot.execution_id.as_str(), snapshot.revision.to_be_bytes().as_slice(), epoch.to_be_bytes().as_slice(), serde_json::to_string(&snapshot.status).unwrap(), snapshot.updated_at_ms.to_be_bytes().as_slice(), serde_json::to_string(&entry).unwrap()]).unwrap();
    }
    if let Some(mutation) = mutation {
        connection.execute_batch(mutation).unwrap();
    }
    completed
}

#[test]
fn v1_upgrade_preserves_snapshot_history_epoch_and_unknown_origin() {
    let directory = Directory::new();
    let snapshot = install_v1(&directory, 7, None);
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(store.owner_generation(), 8);
    store.readiness().unwrap();
    let view = store.thread_view(&thread()).unwrap().unwrap();
    assert_eq!(view.execution.unwrap().snapshot, snapshot);
    assert_eq!(view.thread.created_at_ms, 0);
    assert_eq!(view.thread.metadata, Map::new());
    assert_eq!(view.thread.origin, None);
    assert_eq!(view.thread.resource_revision, 0);
    assert_eq!(store.journal(&snapshot.execution_id).unwrap().len(), 2);
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(reopened.owner_generation(), 9);
    let connection = Connection::open(directory.database()).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT schema_version FROM metadata", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        4
    );
}

#[test]
fn v2_upgrade_preserves_existing_context_snapshot_and_history() {
    let directory = Directory::new();
    let snapshot = install_v1(&directory, 7, None);
    let connection = Connection::open(directory.database()).unwrap();
    connection.execute_batch(MIGRATION_V2).unwrap();
    connection
        .execute(
            "UPDATE metadata SET schema_version = 2, migration_checksum = ?1",
            [format!("{:x}", Sha256::digest(V2))],
        )
        .unwrap();
    connection.execute("UPDATE threads SET created_at_ms = ?1, metadata = ?2, origin = ?3, resource_revision = ?4", params![seed().created_at_ms.to_be_bytes().as_slice(), serde_json::to_string(&seed().metadata).unwrap(), serde_json::to_string(&origin()).unwrap(), 42_u64.to_be_bytes().as_slice()]).unwrap();
    drop(connection);
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(store.owner_generation(), 8);
    let view = store.thread_view(&thread()).unwrap().unwrap();
    assert_eq!(view.execution.unwrap().snapshot, snapshot);
    assert_eq!(view.thread.created_at_ms, seed().created_at_ms);
    assert_eq!(view.thread.metadata, seed().metadata);
    assert_eq!(view.thread.origin, Some(origin()));
    assert_eq!(view.thread.resource_revision, 42);
    assert_eq!(store.journal(&snapshot.execution_id).unwrap().len(), 2);
    let generation = view.thread.generation;
    drop(store);
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(
        store.thread(&thread()).unwrap().unwrap().generation,
        generation
    );
}

#[test]
fn unknown_v1_schema_and_checksum_are_rejected_before_upgrade_or_epoch_change() {
    for mutation in [
        "UPDATE metadata SET migration_checksum = 'forged'",
        "UPDATE metadata SET schema_version = 99",
        "ALTER TABLE threads ADD COLUMN surprise TEXT",
        "CREATE INDEX unexpected ON threads(id)",
        "CREATE TRIGGER unexpected BEFORE UPDATE ON metadata BEGIN SELECT RAISE(IGNORE); END",
    ] {
        let directory = Directory::new();
        install_v1(&directory, 7, Some(mutation));
        assert!(matches!(
            SqliteStore::open(&directory.0),
            Err(StoreError::UnsupportedSchema)
        ));
        let connection = Connection::open(directory.database()).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT owner_epoch FROM metadata", [], |r| r
                    .get::<_, Vec<u8>>(0))
                .unwrap(),
            7_u64.to_be_bytes()
        );
        assert!(connection
            .prepare("SELECT created_at_ms FROM threads")
            .is_err());
    }
}

#[test]
fn exhausted_owner_rolls_back_the_entire_v1_upgrade() {
    let directory = Directory::new();
    install_v1(&directory, u64::MAX, None);
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::Unavailable(_))
    ));
    let connection = Connection::open(directory.database()).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT schema_version FROM metadata", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(connection
        .prepare("SELECT created_at_ms FROM threads")
        .is_err());
}

#[test]
fn readiness_detects_changed_schema_checksum_and_durable_owner() {
    for mutation in [
        "UPDATE metadata SET migration_checksum = 'forged'",
        "UPDATE metadata SET schema_version = 99",
        "CREATE TABLE unexpected (x TEXT)",
        "CREATE VIEW unexpected AS SELECT 1",
        "CREATE TRIGGER unexpected BEFORE UPDATE ON metadata BEGIN SELECT RAISE(IGNORE); END",
        "UPDATE metadata SET owner_epoch = x'0000000000000002'",
        "DELETE FROM metadata",
    ] {
        let directory = Directory::new();
        let store = SqliteStore::open(&directory.0).unwrap();
        store.readiness().unwrap();
        let connection = Connection::open(directory.database()).unwrap();
        connection.execute_batch(mutation).unwrap();
        assert!(store.readiness().is_err());
    }
}

#[test]
fn resource_revision_overflow_rolls_back_admission_and_commit() {
    let directory = Directory::new();
    let store = SqliteStore::open(&directory.0).unwrap();
    store.create_thread_with(seed()).unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute(
            "UPDATE threads SET resource_revision = ?1",
            [u64::MAX.to_be_bytes().as_slice()],
        )
        .unwrap();
    assert!(matches!(
        store.admit(&thread(), initial("overflow", 1)),
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(
        store
            .execution(&ExecutionId::new("overflow").unwrap())
            .unwrap(),
        None
    );
    connection
        .execute(
            "UPDATE threads SET resource_revision = ?1",
            [0_u64.to_be_bytes().as_slice()],
        )
        .unwrap();
    let admitted = store.admit(&thread(), initial("execution", 1)).unwrap();
    connection
        .execute(
            "UPDATE threads SET resource_revision = ?1",
            [u64::MAX.to_be_bytes().as_slice()],
        )
        .unwrap();
    let before = store.thread_view(&thread()).unwrap();
    let journal = store.journal(&admitted.snapshot.execution_id).unwrap();
    let no_op = CommitPlan {
        expected_revision: admitted.snapshot.revision,
        snapshot: admitted.snapshot.clone(),
        dispatchable: vec![],
    };
    store
        .commit(
            &admitted.snapshot.execution_id,
            (&admitted.snapshot).into(),
            no_op,
        )
        .unwrap();
    assert_eq!(store.thread_view(&thread()).unwrap(), before);
    let plan = transition(
        &admitted.snapshot,
        KernelCommand::Start,
        TransitionFacts { now_ms: 101 },
    )
    .unwrap();
    assert!(matches!(
        store.commit(
            &admitted.snapshot.execution_id,
            (&admitted.snapshot).into(),
            plan
        ),
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(store.thread_view(&thread()).unwrap(), before);
    assert_eq!(
        store.journal(&admitted.snapshot.execution_id).unwrap(),
        journal
    );
}

#[test]
fn new_context_survives_reopen() {
    let directory = Directory::new();
    let store = SqliteStore::open(&directory.0).unwrap();
    let created = store.create_thread_with(seed()).unwrap();
    store
        .admit_fenced(&thread(), &created.generation, 0, initial("execution", 1))
        .unwrap();
    let view = store.thread_view(&thread()).unwrap();
    drop(store);
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(store.thread_view(&thread()).unwrap(), view);
}
