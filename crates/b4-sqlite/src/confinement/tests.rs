use super::super::SqliteStore;
use b4_contract::*;
use b4_runtime::{
    RecoveryConfinementBackendError, RecoveryConfinementOutcome, RecoveryConfinementReason,
    RecoveryConfinementTarget, Store, ThreadOrigin, ThreadSeed, ThreadView,
};
use rusqlite::types::Value as SqlValue;
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-private-confinement-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn seed(directory: &Directory, values: Map<String, Value>, approval: bool) -> Arc<SqliteStore> {
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let thread_id = ThreadId::new("private-sqlite-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(ThreadOrigin {
                application: "private-app".into(),
                tenant: "private-tenant".into(),
                principal: "private-principal".into(),
            }),
        })
        .unwrap();
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new("private-sqlite-execution").unwrap(),
        TurnId::new("private-original-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/private-confinement#workflow").unwrap(),
            revision: ExecutionRevision::new("private-definition").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("private-task").unwrap(),
                effect_id: EffectId::new("private-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: approval,
            }]],
            max_attempts: 3,
        },
        values,
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    store.admit(&thread_id, snapshot).unwrap();
    store
}

fn view(store: &SqliteStore) -> ThreadView {
    store
        .thread_view(&ThreadId::new("private-sqlite-thread").unwrap())
        .unwrap()
        .unwrap()
}

fn advance(directory: &Directory, store: Arc<SqliteStore>) -> Arc<SqliteStore> {
    let owner = store.owner_generation();
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(reopened.owner_generation(), owner + 1);
    reopened.readiness().unwrap();
    reopened
}

fn commit(store: &SqliteStore, command: KernelCommand) {
    let current = view(store).execution.unwrap();
    let plan =
        b4_kernel::transition(&current.snapshot, command, TransitionFacts { now_ms: 150 }).unwrap();
    store
        .commit(
            &current.snapshot.execution_id,
            (&current.snapshot).into(),
            plan,
        )
        .unwrap();
}

fn raw_tables(store: &SqliteStore) -> Vec<Vec<Vec<SqlValue>>> {
    let connection = store.connection().unwrap();
    [
        "metadata",
        "threads",
        "executions",
        "journal",
        "approval_grants",
        "approval_acceptances",
    ]
    .iter()
    .map(|table| {
        let mut statement = connection
            .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
            .unwrap();
        let count = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..count)
                    .map(|index| row.get(index))
                    .collect::<Result<Vec<SqlValue>, _>>()
            })
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    })
    .collect()
}

fn retire_and_verify(directory: &Directory, store: Arc<SqliteStore>, actual: &ThreadView) {
    let reopened = advance(directory, store);
    assert!(view(&reopened) == *actual);
    drop(reopened);
}

#[test]
fn prior_owner_capture_returns_the_full_original_after_real_writer_reopen() {
    let directory = Directory::new();
    let store = advance(&directory, seed(&directory, Map::new(), false));
    let original = view(&store);
    assert!(
        original
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .owner_generation
            < store.owner_generation()
    );
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = raw_tables(&store);
    let observed = store.confinement_view_fenced(&target);
    assert!(before == raw_tables(&store));
    retire_and_verify(&directory, store, &original);
    assert!(observed.expect("bounded prior capture must be supported") == original);
}

#[test]
fn prior_accepted_mutation_confirms_exact_no_dispatch_change_after_normal_retirement() {
    let directory = Directory::new();
    let store = advance(&directory, seed(&directory, Map::new(), false));
    let original = view(&store);
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let current_owner = store.owner_generation();
    let fresh = TurnId::new("private-fresh-turn").unwrap();
    let journal_before = store.journal(&target.execution_id).unwrap();
    let observed = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &fresh,
        TransitionFacts { now_ms: 250 },
    );
    let actual = view(&store);
    let journal_after = store.journal(&target.execution_id).unwrap();
    retire_and_verify(&directory, store, &actual);
    let receipt = match observed.expect("prior accepted confinement must be supported") {
        RecoveryConfinementOutcome::Confined(receipt) => receipt,
        RecoveryConfinementOutcome::AlreadyBlocked => {
            panic!("accepted work cannot be already blocked")
        }
    };
    let mut expected = original.clone();
    expected.thread.resource_revision += 1;
    let snapshot = &mut expected.execution.as_mut().unwrap().snapshot;
    snapshot.status = ExecutionStatus::RecoveryBlocked;
    snapshot.failure = Some("recovery_definition_unavailable".into());
    snapshot.owner_generation = current_owner;
    snapshot.turn_id = fresh.clone();
    snapshot.turn_history.push(fresh.clone());
    snapshot.revision += 1;
    snapshot.updated_at_ms = 250;
    assert!(actual == expected);
    assert_eq!(receipt.thread_id, target.thread_id);
    assert_eq!(receipt.thread_generation, target.thread_generation);
    assert_eq!(receipt.execution_id, target.execution_id);
    assert_eq!(receipt.original_fence, target.execution_fence);
    assert_eq!(
        receipt.new_fence,
        (&expected.execution.as_ref().unwrap().snapshot).into()
    );
    assert_eq!(receipt.original_resource_revision, target.resource_revision);
    assert_eq!(receipt.new_resource_revision, target.resource_revision + 1);
    assert_eq!(receipt.original_turn_id, target.turn_id);
    assert_eq!(receipt.fresh_turn_id, fresh);
    assert_eq!(
        receipt.route,
        expected
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .definition
            .route
    );
    assert_eq!(
        receipt.definition_revision,
        expected
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .definition
            .revision
    );
    assert_eq!(
        receipt.reason,
        RecoveryConfinementReason::ExactDefinitionUnavailable
    );
    assert!(journal_after.starts_with(&journal_before));
    assert_eq!(journal_after.len(), journal_before.len() + 1);
    assert_eq!(
        journal_after.last(),
        Some(&b4_runtime::JournalEntry::from(
            &expected.execution.as_ref().unwrap().snapshot
        ))
    );
}

#[test]
fn current_owner_capture_refuses_with_all_six_raw_tables_unchanged() {
    let directory = Directory::new();
    let store = seed(&directory, Map::new(), false);
    let original = view(&store);
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = raw_tables(&store);
    let observed = store.confinement_view_fenced(&target);
    assert!(before == raw_tables(&store));
    retire_and_verify(&directory, store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::OwnerConflict
    );
}

#[test]
fn stale_target_refuses_without_refresh_or_storage_mutation() {
    let directory = Directory::new();
    let store = advance(&directory, seed(&directory, Map::new(), false));
    let original = view(&store);
    let mut target = RecoveryConfinementTarget::from_view(&original).unwrap();
    target.resource_revision += 1;
    let before = raw_tables(&store);
    let observed = store.confinement_view_fenced(&target);
    assert!(before == raw_tables(&store));
    retire_and_verify(&directory, store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::TargetChanged
    );
}

#[test]
fn valid_legacy_waiting_without_grants_is_unsupported_not_corrupt_or_normalized() {
    let directory = Directory::new();
    let store = seed(&directory, Map::new(), true);
    commit(&store, KernelCommand::Start);
    commit(
        &store,
        KernelCommand::Park {
            task_id: TaskId::new("private-task").unwrap(),
            interrupt_id: InterruptId::new("private-interrupt").unwrap(),
            prompt: serde_json::json!({"original": "private-prompt"}),
        },
    );
    let store = advance(&directory, store);
    let original = view(&store);
    assert_eq!(
        original.execution.as_ref().unwrap().snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = raw_tables(&store);
    let observed = store.confinement_view_fenced(&target);
    assert!(before == raw_tables(&store));
    retire_and_verify(&directory, store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::UnsupportedAuthorityProfile
    );
}

#[test]
fn otherwise_valid_raw_snapshot_over_one_mib_is_static_limit_exceeded() {
    let directory = Directory::new();
    let values = Map::from_iter([("padding".into(), Value::String("x".repeat(1_048_576)))]);
    let store = advance(&directory, seed(&directory, values, false));
    let original = view(&store);
    b4_kernel::validate_snapshot(&original.execution.as_ref().unwrap().snapshot).unwrap();
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = raw_tables(&store);
    let observed = store.confinement_view_fenced(&target);
    assert!(before == raw_tables(&store));
    retire_and_verify(&directory, store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::LimitExceeded
    );
}

#[test]
fn prior_terminal_capture_refuses_without_rewriting_the_completed_result() {
    let directory = Directory::new();
    let store = seed(&directory, Map::new(), false);
    commit(&store, KernelCommand::Start);
    commit(
        &store,
        KernelCommand::Dispatch {
            task_id: TaskId::new("private-task").unwrap(),
            attempt_id: AttemptId::new("private-attempt").unwrap(),
        },
    );
    commit(
        &store,
        KernelCommand::Complete {
            task_id: TaskId::new("private-task").unwrap(),
            attempt_id: AttemptId::new("private-attempt").unwrap(),
            result: TaskResult {
                output: serde_json::json!({"private": true}),
                writes: vec![],
            },
        },
    );
    let store = advance(&directory, store);
    let original = view(&store);
    assert_eq!(
        original.execution.as_ref().unwrap().snapshot.status,
        ExecutionStatus::Completed
    );
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = raw_tables(&store);
    let observed = store.confinement_view_fenced(&target);
    assert!(before == raw_tables(&store));
    retire_and_verify(&directory, store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::Terminal
    );
}

#[test]
fn prior_already_blocked_mutation_returns_unchanged_without_a_second_journal_entry() {
    let directory = Directory::new();
    let store = advance(&directory, seed(&directory, Map::new(), false));
    commit(
        &store,
        KernelCommand::Recover {
            available_revision: ExecutionRevision::new("private-unavailable-definition").unwrap(),
            turn_id: TurnId::new("private-blocked-turn").unwrap(),
            owner_generation: store.owner_generation(),
        },
    );
    let store = advance(&directory, store);
    let original = view(&store);
    assert_eq!(
        original.execution.as_ref().unwrap().snapshot.status,
        ExecutionStatus::RecoveryBlocked
    );
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = raw_tables(&store);
    let observed = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
        &TurnId::new("private-fresh-turn").unwrap(),
        TransitionFacts { now_ms: 250 },
    );
    assert!(before == raw_tables(&store));
    retire_and_verify(&directory, store, &original);
    assert_eq!(
        observed.unwrap(),
        RecoveryConfinementOutcome::AlreadyBlocked
    );
}
