use super::*;
use b4_runtime::{Store, ThreadSeed};
use rusqlite::types::Value;
use serde_json::Map;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-private-confinement-rollback-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn raw(directory: &Path) -> Vec<Vec<Vec<Value>>> {
    let connection = Connection::open(directory.join("runtime.sqlite3")).unwrap();
    [
        "metadata",
        "threads",
        "executions",
        "journal",
        "approval_grants",
        "approval_acceptances",
    ]
    .into_iter()
    .map(|table| {
        let mut statement = connection
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1, 2"))
            .unwrap();
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()
            })
            .unwrap();
        rows.collect::<rusqlite::Result<Vec<_>>>().unwrap()
    })
    .collect()
}

fn exercise_trigger(kind: &str) {
    let directory = Directory::new();
    let thread = ThreadId::new("rollback-thread").unwrap();
    let first = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(first.owner_generation(), 1);
    first
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 10,
            metadata: Map::new(),
            origin: None,
        })
        .unwrap();
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new("rollback-execution").unwrap(),
        TurnId::new("rollback-original").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/rollback#workflow").unwrap(),
            revision: ExecutionRevision::new("rollback-v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("rollback-task").unwrap(),
                effect_id: EffectId::new("rollback-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }]],
            max_attempts: 3,
        },
        Map::new(),
        TransitionFacts { now_ms: 10 },
    )
    .unwrap();
    first.admit(&thread, snapshot).unwrap();
    let original = first.thread_view(&thread).unwrap().unwrap();
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    drop(first);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    assert_eq!(store.confinement_view_fenced(&target).unwrap(), original);
    // Store assembly and normal ownership advance finish before the deliberately
    // injected trigger. It is removed only after recording full rollback data,
    // restoring the original schema for the real successor writer check.
    let connection = Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    let trigger = match kind {
        "ignore" => "CREATE TRIGGER private_confinement_trigger BEFORE INSERT ON journal WHEN NEW.kind='commit' BEGIN SELECT RAISE(IGNORE); END",
        "abort" => "CREATE TRIGGER private_confinement_trigger BEFORE INSERT ON journal WHEN NEW.kind='commit' BEGIN SELECT RAISE(ABORT,'private injected rollback'); END",
        _ => unreachable!(),
    };
    connection.execute_batch(trigger).unwrap();
    let before = raw(&directory.0);
    let observed = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &TurnId::new("rollback-fresh").unwrap(),
        TransitionFacts { now_ms: 20 },
    );
    let after = raw(&directory.0);
    let captured = store.thread_view(&thread).unwrap().unwrap();
    let raw_unchanged = before == after;
    let ordinary_unchanged = captured == original;
    connection
        .execute_batch("DROP TRIGGER private_confinement_trigger")
        .unwrap();
    drop(connection);
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(reopened.owner_generation(), 3);
    let retained = reopened.thread_view(&thread).unwrap().unwrap();
    drop(reopened);
    // These static booleans disclose no stored fields, IDs, tokens or raw bytes.
    eprintln!(
        "private_raw_preserved={raw_unchanged};private_ordinary_preserved={ordinary_unchanged}"
    );
    assert!(
        matches!(observed, Err(Error::Unavailable)),
        "journal refusal must not return a confirmed receipt"
    );
    assert!(
        raw_unchanged,
        "all six stored tables must remain exact on refusal"
    );
    assert!(
        ordinary_unchanged && retained == original,
        "the full ordinary view must remain exact"
    );
}

#[test]
fn ignored_journal_insert_refuses_and_rolls_back_all_six_tables_before_receipt() {
    exercise_trigger("ignore");
}

#[test]
fn aborting_journal_insert_refuses_and_rolls_back_all_six_tables_before_receipt() {
    exercise_trigger("abort");
}
