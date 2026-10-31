#[path = "../../b4-runtime/tests/support/failures.rs"]
mod qualifications;

use b4_contract::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use qualifications::*;
use rusqlite::{params, Connection};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-settled-failure-{}-{}",
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
fn with_sqlite(test: impl FnOnce(SqliteStore)) {
    let directory = Directory::new();
    test(SqliteStore::open(&directory.0).unwrap());
}

#[test]
fn explicit_failure_is_committed_with_journal_and_releases_admission() {
    with_sqlite(known_failure);
}
#[test]
fn succeeded_outcome_preserves_success_semantics() {
    with_sqlite(success_outcome);
}
#[test]
fn outer_error_retains_uncertain_attempt() {
    with_sqlite(uncertain_error);
}
#[test]
fn rejected_attempt_commit_invokes_no_callback() {
    with_sqlite(claim_failure);
}
#[test]
fn failed_outcome_commit_retains_borrowed_outcome_without_replay() {
    with_sqlite(completion_failure);
}
#[test]
fn outcome_completion_checks_owner_attempt_conflicts_and_reason_bounds() {
    with_sqlite(completion_fences);
}
#[test]
fn failed_batch_drains_in_flight_sibling_before_releasing_admission() {
    with_sqlite(drain);
}
#[test]
fn outcome_completion_refreshes_revision_and_host_facts() {
    with_sqlite(fresh_completion);
}
#[test]
fn settled_application_failure_takes_precedence_over_requested_cancel() {
    with_sqlite(cancel_precedence);
}
#[test]
fn concurrent_outcome_conflict_retains_completion_without_callback_replay() {
    with_sqlite(concurrent_conflict);
}

#[test]
fn reopened_settled_failure_preserves_cause_journal_and_schema() {
    let directory = Directory::new();
    let (runtime, running) = start(
        SqliteStore::open(&directory.0).unwrap(),
        vec![vec![spec("a")]],
    );
    let connection = Connection::open(directory.database()).unwrap();
    let schema = schema_objects(&connection);
    let done = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            facts,
            |_| Ok(failed()),
        )
        .unwrap();
    let journal = runtime.store().journal(&execution()).unwrap();
    let thread_record = runtime.store().thread(&thread()).unwrap();
    assert_eq!(schema_objects(&connection), schema);
    drop(connection);
    drop(runtime);
    let runtime = Runtime::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(runtime.store().owner_generation(), 2);
    assert_eq!(runtime.store().execution(&execution()).unwrap(), Some(done));
    assert_eq!(runtime.store().journal(&execution()).unwrap(), journal);
    assert_eq!(runtime.store().thread(&thread()).unwrap(), thread_record);
    assert_eq!(
        runtime.complete_outcome(&execution(), 1, task("a"), attempt("a"), &failed(), facts()),
        Err(RuntimeError::Store(StoreError::OwnerConflict))
    );
    runtime.store().readiness().unwrap();
    runtime
        .admit(&thread(), initial("successor", 2, vec![]))
        .unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    assert_eq!(schema_objects(&connection), schema);
}

fn schema_objects(connection: &Connection) -> Vec<(String, String, String)> {
    connection.prepare("SELECT type, name, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name")
        .unwrap().query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap().map(Result::unwrap).collect()
}

fn interrupted_failure(directory: &Directory, recovery: RecoveryPolicy) -> StoredExecution {
    let mut sibling = spec("b");
    sibling.recovery = recovery;
    let (runtime, running) = start(
        SqliteStore::open(&directory.0).unwrap(),
        vec![vec![spec("a"), sibling]],
    );
    runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            facts,
            |_| {
                let current = runtime.store().execution(&execution()).unwrap().unwrap();
                runtime
                    .apply(
                        &execution(),
                        (&current.snapshot).into(),
                        KernelCommand::Dispatch {
                            task_id: task("b"),
                            attempt_id: attempt("b"),
                        },
                        facts(),
                    )
                    .unwrap();
                Ok(failed())
            },
        )
        .unwrap()
}

#[test]
fn reopened_safe_failure_drain_cancels_sibling_without_replay() {
    for policy in [RecoveryPolicy::Pure, RecoveryPolicy::Idempotent] {
        let directory = Directory::new();
        let draining = interrupted_failure(&directory, policy);
        let runtime = Runtime::new(SqliteStore::open(&directory.0).unwrap());
        assert_eq!(
            runtime.complete_outcome(
                &execution(),
                1,
                task("b"),
                attempt("b"),
                &EffectOutcome::Succeeded(result("b")),
                facts()
            ),
            Err(RuntimeError::Store(StoreError::OwnerConflict))
        );
        assert_eq!(
            runtime.admit(&thread(), initial("successor", 2, vec![])),
            Err(RuntimeError::Store(StoreError::ThreadBusy))
        );
        let done = runtime
            .recover(
                &execution(),
                (&draining.snapshot).into(),
                ExecutionRevision::new("v1").unwrap(),
                TurnId::new("recovered").unwrap(),
                facts(),
            )
            .unwrap();
        assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
        assert_eq!(
            done.snapshot.failure.as_deref(),
            Some("application failure")
        );
        assert_eq!(
            done.snapshot.tasks[0].failure.as_deref(),
            Some("application failure")
        );
        assert_eq!(done.snapshot.tasks[1].status, TaskStatus::Cancelled);
        assert_eq!(done.snapshot.attempts_used, 2);
        assert!(done.snapshot.values.is_empty());
        assert!(done.snapshot.output.is_none());
        assert!(runtime
            .dispatch_outcome(
                &execution(),
                (&done.snapshot).into(),
                task("b"),
                attempt("replay"),
                facts(),
                facts,
                |_| panic!("failure recovery replayed sibling")
            )
            .is_err());
        runtime
            .admit(&thread(), initial("successor", 2, vec![]))
            .unwrap();
    }
}

#[test]
fn reopened_unsafe_failure_drain_blocks_and_retains_task_cause() {
    for policy in [RecoveryPolicy::Unsafe, RecoveryPolicy::Reconcilable] {
        let directory = Directory::new();
        let draining = interrupted_failure(&directory, policy);
        let runtime = Runtime::new(SqliteStore::open(&directory.0).unwrap());
        let blocked = runtime
            .recover(
                &execution(),
                (&draining.snapshot).into(),
                ExecutionRevision::new("v1").unwrap(),
                TurnId::new("recovered").unwrap(),
                facts(),
            )
            .unwrap();
        assert_eq!(blocked.snapshot.status, ExecutionStatus::RecoveryBlocked);
        assert_eq!(blocked.snapshot.tasks[0].status, TaskStatus::Failed);
        assert_eq!(
            blocked.snapshot.tasks[0].failure.as_deref(),
            Some("application failure")
        );
        assert_eq!(blocked.snapshot.tasks[1].status, TaskStatus::OutcomeUnknown);
        assert_eq!(
            runtime.admit(&thread(), initial("successor", 2, vec![])),
            Err(RuntimeError::Store(StoreError::ThreadBusy))
        );
        assert!(runtime
            .dispatch_outcome(
                &execution(),
                (&blocked.snapshot).into(),
                task("b"),
                attempt("replay"),
                facts(),
                facts,
                |_| panic!("unsafe failure recovery replayed sibling")
            )
            .is_err());
        drop(runtime);
        let reopened = SqliteStore::open(&directory.0).unwrap();
        assert_eq!(reopened.execution(&execution()).unwrap(), Some(blocked));
    }
}

#[test]
fn actual_failed_journal_insert_rolls_back_outcome_and_thread_context() {
    let directory = Directory::new();
    let (runtime, running) = start(
        SqliteStore::open(&directory.0).unwrap(),
        vec![vec![spec("a")]],
    );
    let error = runtime.dispatch_outcome(
        &execution(), (&running.snapshot).into(), task("a"), attempt("a"), facts(), facts,
        |_| {
            let connection = Connection::open(directory.database()).unwrap();
            connection.execute_batch("CREATE TRIGGER abort_failed_outcome BEFORE INSERT ON journal WHEN NEW.status = '\"failed\"' BEGIN SELECT RAISE(ABORT, 'injected failed journal'); END;").unwrap();
            Ok(failed())
        },
    ).unwrap_err();
    let RuntimeError::OutcomeNotCommitted { outcome, cause } = error else {
        panic!("outcome not retained")
    };
    assert_eq!(outcome, failed());
    assert!(matches!(
        *cause,
        RuntimeError::Store(StoreError::Unavailable(_))
    ));
    let claimed = runtime.store().execution(&execution()).unwrap().unwrap();
    assert_eq!(claimed.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(claimed.snapshot.failure.is_none());
    let journal = runtime.store().journal(&execution()).unwrap();
    assert_eq!(journal.len(), 3);
    let resource = runtime.store().thread(&thread()).unwrap();
    assert!(runtime
        .complete_outcome(&execution(), 1, task("a"), attempt("a"), &outcome, facts())
        .is_err());
    assert_eq!(
        runtime.store().execution(&execution()).unwrap(),
        Some(claimed)
    );
    assert_eq!(runtime.store().thread(&thread()).unwrap(), resource);
    assert_eq!(runtime.store().journal(&execution()).unwrap(), journal);
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch("DROP TRIGGER abort_failed_outcome")
        .unwrap();
    drop(connection);
    let done = runtime
        .complete_outcome(&execution(), 1, task("a"), attempt("a"), &outcome, facts())
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(
        runtime.store().journal(&execution()).unwrap().len(),
        journal.len() + 1
    );
    runtime.store().readiness().unwrap();
}

#[test]
fn legacy_version_one_snapshot_without_task_failure_remains_readable() {
    let directory = Directory::new();
    let (runtime, _) = start(
        SqliteStore::open(&directory.0).unwrap(),
        vec![vec![spec("a")]],
    );
    drop(runtime);
    let mut legacy: serde_json::Value = serde_json::from_str(include_str!(
        "../../../contracts/agent-protocol/examples/internal-execution-snapshot.json"
    ))
    .unwrap();
    assert!(legacy["tasks"][0].get("failure").is_none());
    legacy["execution_id"] = json!(execution().as_str());
    legacy["owner_generation"] = json!(1);
    let expected: Snapshot = serde_json::from_value(legacy.clone()).unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute(
            "UPDATE executions SET revision = ?1, snapshot = ?2",
            params![2u64.to_be_bytes().as_slice(), legacy.to_string()],
        )
        .unwrap();
    drop(connection);
    let store = SqliteStore::open(&directory.0).unwrap();
    let restored = store.execution(&execution()).unwrap().unwrap().snapshot;
    assert_eq!(restored, expected);
    assert!(restored.tasks[0].failure.is_none());
    assert!(serde_json::to_value(&restored).unwrap()["tasks"][0]
        .get("failure")
        .is_none());
}

#[test]
fn corrupt_persisted_failure_reasons_reject_load_and_successor_admission() {
    for reason in [String::new(), "é".repeat(513)] {
        let directory = Directory::new();
        let (runtime, running) = start(
            SqliteStore::open(&directory.0).unwrap(),
            vec![vec![spec("a")]],
        );
        let done = runtime
            .dispatch_outcome(
                &execution(),
                (&running.snapshot).into(),
                task("a"),
                attempt("a"),
                facts(),
                facts,
                |_| Ok(failed()),
            )
            .unwrap();
        drop(runtime);
        let mut value = serde_json::to_value(done.snapshot).unwrap();
        value["tasks"][0]["failure"] = json!(reason);
        let connection = Connection::open(directory.database()).unwrap();
        connection
            .execute("UPDATE executions SET snapshot = ?1", [value.to_string()])
            .unwrap();
        drop(connection);
        let store = SqliteStore::open(&directory.0).unwrap();
        assert!(matches!(
            store.execution(&execution()),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            store.thread_view(&thread()),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            store.admit(&thread(), initial("successor", 2, vec![])),
            Err(StoreError::Corrupt(_))
        ));
    }
}
