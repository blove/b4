//! A fabricated direct plan is a fault probe, never a normal permission producer.
use b4_contract::*;
use b4_kernel::{authorize_ready, new_execution, transition, PermissionAuthorizationCommand};
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use rusqlite::types::{Value, ValueRef};
use serde_json::{json, Map};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-ready-plan-guard-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(PartialEq)]
struct RawNine([Vec<Vec<Value>>; 9]);
fn cells(
    connection: &rusqlite::Connection,
    sql: &str,
    columns: usize,
    max: usize,
) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let mut cursor = statement.query([]).unwrap();
    let mut result = Vec::new();
    while let Some(row) = cursor.next().unwrap() {
        assert!(result.len() < max, "bounded observer overflow sentinel");
        let mut values = Vec::new();
        for column in 0..columns {
            match row.get_ref(column).unwrap() {
                ValueRef::Text(bytes) => {
                    assert!(bytes.len() <= 16_384 && std::str::from_utf8(bytes).is_ok())
                }
                ValueRef::Blob(bytes) => assert_eq!(bytes.len(), 8),
                ValueRef::Null | ValueRef::Integer(_) => {}
                ValueRef::Real(_) => panic!("legal fixed fixture has no REAL values"),
            }
            values.push(row.get::<_, Value>(column).unwrap());
        }
        result.push(values);
    }
    result
}
fn raw(path: &Path) -> RawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let result = RawNine([
        cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 2", 5, 1),
        cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 5", 9, 4),
        cells(&connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        cells(&connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        cells(&connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ]);
    assert!(connection.close().is_ok());
    result
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
#[test]
fn ordinary_commit_refuses_ready_approval_without_atomic_permission_use() {
    let directory = Directory::new();
    let store = SqliteStore::open(&directory.0).unwrap();
    let owner = store.owner_generation();
    let thread = ThreadId::new("ready-guard-thread").unwrap();
    let execution = ExecutionId::new("ready-guard-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let definition = Definition {
        route: RouteKey::new("/ready-guard#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: EffectId::new("release-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(ThreadOrigin {
                application: "permission-app".into(),
                tenant: "tenant".into(),
                principal: "alice".into(),
            }),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        owner,
        definition,
        Map::from_iter([("input".into(), json!({"release":"fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    let started = store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let before_view = store.thread_view(&thread).unwrap().unwrap();
    let before_execution = store.execution(&execution).unwrap().unwrap();
    let before_journal = store.journal(&execution).unwrap();
    let before_raw = raw(&directory.0);
    assert_eq!(started, before_execution);
    assert_eq!(started.snapshot.status, ExecutionStatus::Running);
    assert_eq!(started.snapshot.revision, 1);
    assert_eq!(started.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(!started.snapshot.tasks[0].approved);
    assert!(started.snapshot.tasks[0].interrupt.is_none());
    assert_eq!(
        before_raw.0.each_ref().map(Vec::len),
        [1, 1, 1, 3, 0, 0, 0, 0, 0]
    );
    let command = PermissionAuthorizationCommand::capture(
        &started.snapshot,
        task,
        b4_contract::permissions::PermissionUseId::new(
            "b4pu_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        )
        .unwrap(),
    )
    .unwrap();
    let direct_plan = authorize_ready(&started.snapshot, &command, facts(102)).unwrap();
    b4_kernel::validate_snapshot(&direct_plan.snapshot).unwrap();
    assert!(direct_plan.dispatchable.is_empty());
    assert!(direct_plan.snapshot.tasks[0].approved);
    let result = store.commit(&execution, (&started.snapshot).into(), direct_plan.clone());
    // Capture actual effects before selecting the result or claiming refusal.
    let after_view = store.thread_view(&thread).unwrap().unwrap();
    let after_execution = store.execution(&execution).unwrap();
    let after_journal = store.journal(&execution).unwrap();
    let after_raw = raw(&directory.0);
    store.readiness().unwrap();
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    let reopened_owner = reopened.owner_generation();
    let persisted_view = reopened.thread_view(&thread).unwrap().unwrap();
    let persisted_execution = reopened.execution(&execution).unwrap();
    let persisted_journal = reopened.journal(&execution).unwrap();
    let persisted_raw = raw(&directory.0);
    reopened.readiness().unwrap();
    drop(reopened);
    drop(command);
    let removed = directory.0.clone();
    drop(directory);
    assert!(
        !removed.exists(),
        "all owners and Directory retire before primary"
    );
    assert_eq!(reopened_owner, owner.checked_add(1).unwrap());
    assert_eq!(persisted_view, after_view);
    assert_eq!(persisted_execution, after_execution);
    assert_eq!(persisted_journal, after_journal);
    assert_eq!(persisted_raw.0[1..], after_raw.0[1..]);
    assert_eq!(persisted_raw.0[0][0][..3], after_raw.0[0][0][..3]);
    if let Ok(stored) = &result {
        assert_eq!(stored.snapshot, direct_plan.snapshot);
        assert_eq!(after_execution.as_ref(), Some(stored));
        assert_eq!(after_view.execution.as_ref(), Some(stored));
        assert_ne!(after_raw.0[2], before_raw.0[2]);
        assert_eq!(after_journal.len(), before_journal.len() + 1);
        assert!(after_raw.0[8].is_empty());
    }
    eprintln!("direct Ready plan evidence: accepted={}; before_revision={}; after_revision={}; use_rows={}; owners={owner},{reopened_owner}; directory_removed={}", result.is_ok(), before_execution.snapshot.revision, after_execution.as_ref().unwrap().snapshot.revision, after_raw.0[8].len(), !removed.exists());
    // First missing-guard primary, after complete actual observations and cleanup.
    assert!(
        matches!(&result, Err(StoreError::InvalidCommit(message)) if message == "required_permission_authorization_transaction"),
        "ordinary commit must refuse Ready approval without an atomic PermissionUse"
    );
    assert_eq!(after_view, before_view);
    assert_eq!(after_execution.as_ref(), Some(&before_execution));
    assert_eq!(after_journal, before_journal);
    assert!(
        after_raw == before_raw,
        "ordinary-plan refusal preserves all nine groups"
    );
}
