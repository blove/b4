use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::{types::Value as SqlValue, Connection};
use serde_json::{json, Map};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier,
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const TABLES: [&str; 5] = [
    "threads",
    "executions",
    "journal",
    "approval_grants",
    "approval_acceptances",
];
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-sqlite-cancel-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
    fn rows(&self) -> Vec<Vec<Vec<SqlValue>>> {
        let connection = self.connection();
        TABLES
            .iter()
            .map(|table| {
                let mut statement = connection
                    .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                    .unwrap();
                let columns = statement.column_count();
                statement
                    .query_map([], |row| {
                        (0..columns)
                            .map(|column| row.get(column))
                            .collect::<Result<Vec<SqlValue>, _>>()
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            })
            .collect()
    }
    fn schema(&self) -> (i64, String) {
        self.connection()
            .query_row(
                "SELECT schema_version, migration_checksum FROM metadata",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Allow;
impl ApprovalAuthority for Allow {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    }
}
fn thread(name: &str) -> ThreadId {
    ThreadId::new(name).unwrap()
}
fn task(name: &str) -> TaskId {
    TaskId::new(name).unwrap()
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn spec(name: &str, required: bool, recovery: RecoveryPolicy) -> TaskSpec {
    TaskSpec {
        id: task(name),
        effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
        requires_approval: required,
        recovery,
    }
}
fn admitted(
    store: &Arc<SqliteStore>,
    name: &str,
    execution: &str,
    batches: Vec<Vec<TaskSpec>>,
) -> StoredExecution {
    if store.thread(&thread(name)).unwrap().is_none() {
        store
            .create_thread_with(ThreadSeed {
                id: thread(name),
                created_at_ms: 1,
                metadata: Map::new(),
                origin: Some(context().actor),
            })
            .unwrap();
    }
    let initial = b4_kernel::new_execution(
        ExecutionId::new(execution).unwrap(),
        TurnId::new(format!("turn-{execution}")).unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/cancel#workflow").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches,
            max_attempts: 5,
        },
        Map::from_iter([("retained".into(), json!("committed"))]),
        facts(1),
    )
    .unwrap();
    store.admit(&thread(name), initial).unwrap()
}
fn apply(
    store: &Arc<SqliteStore>,
    current: &StoredExecution,
    command: KernelCommand,
) -> StoredExecution {
    Runtime::new(store)
        .apply(
            &current.snapshot.execution_id,
            (&current.snapshot).into(),
            command,
            facts(current.snapshot.updated_at_ms + 1),
        )
        .unwrap()
}
fn started(
    store: &Arc<SqliteStore>,
    name: &str,
    execution: &str,
    batches: Vec<Vec<TaskSpec>>,
) -> StoredExecution {
    let accepted = admitted(store, name, execution, batches);
    apply(store, &accepted, KernelCommand::Start)
}
fn approval_fence(store: &Arc<SqliteStore>, current: &StoredExecution) -> ApprovalFence {
    ApprovalFence {
        execution: (&current.snapshot).into(),
        resource_revision: store
            .thread(&current.thread_id)
            .unwrap()
            .unwrap()
            .resource_revision,
    }
}
fn park(store: &Arc<SqliteStore>, current: &StoredExecution, name: &str) -> StoredExecution {
    ApprovalService::new(store.clone(), Allow)
        .unwrap()
        .park(
            &context(),
            &current.snapshot.execution_id,
            approval_fence(store, current),
            task(name),
            InterruptId::new(format!("{}-{name}", current.snapshot.execution_id.as_str())).unwrap(),
            json!({"question":"Cancel fixture?"}),
            1000,
            facts(current.snapshot.updated_at_ms + 1),
        )
        .unwrap()
}
fn accept(store: &Arc<SqliteStore>, current: &StoredExecution) -> StoredExecution {
    let grants = store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap()
        .grants;
    ApprovalService::new(store.clone(), Allow)
        .unwrap()
        .accept(
            &current.snapshot.execution_id,
            approval_fence(store, current),
            ApprovalAcceptanceRequest {
                context: context(),
                route: current.snapshot.definition.route.clone(),
                definition_revision: current.snapshot.definition.revision.clone(),
                turn_id: TurnId::new(format!(
                    "{}-resumed",
                    current.snapshot.execution_id.as_str()
                ))
                .unwrap(),
                now_ms: current.snapshot.updated_at_ms + 1,
                decisions: grants
                    .into_iter()
                    .filter(|grant| !grant.consumed)
                    .map(|grant| GrantDecision {
                        interrupt_id: grant.interrupt_id,
                        decision: ApprovalDecision::Once,
                        cancelled: false,
                        token: Some(grant.token),
                    })
                    .collect(),
            },
        )
        .unwrap()
}
fn claimed(store: &Arc<SqliteStore>, current: &StoredExecution, name: &str) -> StoredExecution {
    apply(
        store,
        current,
        KernelCommand::Dispatch {
            task_id: task(name),
            attempt_id: AttemptId::new(format!(
                "{}-{name}-attempt",
                current.snapshot.execution_id.as_str()
            ))
            .unwrap(),
        },
    )
}
fn mixed(store: &Arc<SqliteStore>, name: &str, execution: &str) -> StoredExecution {
    let running = started(
        store,
        name,
        execution,
        vec![
            vec![spec("gate", true, RecoveryPolicy::Pure)],
            vec![
                spec("pending-a", true, RecoveryPolicy::Pure),
                spec("pending-b", true, RecoveryPolicy::Pure),
                spec("held", false, RecoveryPolicy::Pure),
            ],
        ],
    );
    let parked = park(store, &running, "gate");
    let approved = accept(store, &parked);
    let next = ApprovalService::new(store.clone(), Allow)
        .unwrap()
        .dispatch_outcome(
            &context(),
            &approved.snapshot.execution_id,
            approval_fence(store, &approved),
            task("gate"),
            AttemptId::new(format!("{execution}-gate-attempt")).unwrap(),
            facts(approved.snapshot.updated_at_ms + 1),
            || facts(approved.snapshot.updated_at_ms + 2),
            |_| {
                Ok(EffectOutcome::Succeeded(TaskResult {
                    output: json!("approved"),
                    writes: vec![],
                }))
            },
        )
        .unwrap();
    let first = park(store, &next, "pending-a");
    let second = park(store, &first, "pending-b");
    claimed(store, &second, "held")
}
fn target(store: &Arc<SqliteStore>, name: &str) -> RunCancellationTarget {
    RunCancellationTarget::from_view(&store.thread_view(&thread(name)).unwrap().unwrap()).unwrap()
}
fn cancel(store: &Arc<SqliteStore>, name: &str, current: &StoredExecution) -> StoredExecution {
    store
        .cancel_run_fenced(
            &target(store, name),
            facts(current.snapshot.updated_at_ms + 1),
        )
        .unwrap()
}
fn unchanged(directory: &Directory, before: &[Vec<Vec<SqlValue>>]) {
    assert!(
        directory.rows() == before,
        "Refused cancellation changed durable rows"
    );
}
fn recover(store: &Arc<SqliteStore>, current: &StoredExecution, turn: &str) -> StoredExecution {
    Runtime::new(store)
        .recover(
            &current.snapshot.execution_id,
            (&current.snapshot).into(),
            current.snapshot.definition.revision.clone(),
            TurnId::new(turn).unwrap(),
            facts(current.snapshot.updated_at_ms + 1),
        )
        .unwrap()
}

#[test]
fn accepted_cancellation_initializes_only_first_batch_without_dispatch() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let accepted = admitted(
        &store,
        "thread",
        "execution",
        vec![
            vec![spec("first", false, RecoveryPolicy::Pure)],
            vec![spec("later", false, RecoveryPolicy::Pure)],
        ],
    );
    let captured = target(&store, "thread");
    let schema = directory.schema();
    let cancelled = cancel(&store, "thread", &accepted);
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(cancelled.snapshot.tasks.len(), 1);
    assert_eq!(cancelled.snapshot.tasks[0].spec.id, task("first"));
    assert_eq!(cancelled.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert_eq!(cancelled.snapshot.attempts_used, 0);
    assert!(cancelled.snapshot.attempt_history.is_empty());
    assert_eq!(cancelled.snapshot.values, accepted.snapshot.values);
    assert!(cancelled.snapshot.output.is_none());
    assert_eq!(cancelled.snapshot.revision, accepted.snapshot.revision + 1);
    assert_eq!(cancelled.snapshot.turn_id, captured.turn_id);
    assert_eq!(
        store
            .thread(&thread("thread"))
            .unwrap()
            .unwrap()
            .resource_revision,
        captured.resource_revision + 1
    );
    assert_eq!(
        store.journal(&captured.execution_id).unwrap().last(),
        Some(&JournalEntry::from(&cancelled.snapshot))
    );
    assert_eq!(directory.schema(), schema);
    let before = directory.rows();
    assert_eq!(
        store.cancel_run_fenced(&target(&store, "thread"), facts(3)),
        Err(StoreError::NoRunInFlight)
    );
    unchanged(&directory, &before);
    assert!(Runtime::new(&store)
        .apply(
            &captured.execution_id,
            captured.execution_fence,
            KernelCommand::Start,
            facts(3)
        )
        .is_err());
    unchanged(&directory, &before);
}

#[test]
fn cancellation_of_running_ready_work_prevents_later_dispatch() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let running = started(
        &store,
        "thread",
        "execution",
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
    );
    let cancelled = cancel(&store, "thread", &running);
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert!(cancelled
        .snapshot
        .tasks
        .iter()
        .all(|task| task.status == TaskStatus::Cancelled));
    assert_eq!(cancelled.snapshot.attempts_used, 0);
    let before = directory.rows();
    assert!(Runtime::new(&store)
        .apply(
            &cancelled.snapshot.execution_id,
            (&cancelled.snapshot).into(),
            KernelCommand::Dispatch {
                task_id: task("first"),
                attempt_id: AttemptId::new("forbidden").unwrap()
            },
            facts(4)
        )
        .is_err());
    unchanged(&directory, &before);
}

#[test]
fn inflight_cancellation_retains_slot_and_actual_late_success_without_reduction() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let running = started(
        &store,
        "thread",
        "execution",
        vec![vec![spec("held", false, RecoveryPolicy::Pure)]],
    );
    let inflight = claimed(&store, &running, "held");
    let cancelled = cancel(&store, "thread", &inflight);
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::CancelRequested);
    assert_eq!(
        cancelled.snapshot.attempt_history,
        inflight.snapshot.attempt_history
    );
    assert_eq!(cancelled.snapshot.attempts_used, 1);
    let initial = b4_kernel::new_execution(
        ExecutionId::new("successor").unwrap(),
        TurnId::new("successor-turn").unwrap(),
        store.owner_generation(),
        running.snapshot.definition.clone(),
        Map::new(),
        facts(5),
    )
    .unwrap();
    assert_eq!(
        store.admit(&thread("thread"), initial),
        Err(StoreError::ThreadBusy)
    );
    let deletion =
        ThreadDeletionTarget::from_view(&store.thread_view(&thread("thread")).unwrap().unwrap())
            .unwrap();
    assert_eq!(
        store.delete_thread_fenced(&deletion),
        Err(StoreError::ThreadBusy)
    );
    let result = TaskResult {
        output: json!("real-late-result"),
        writes: vec![StateWrite::Replace {
            field: "late".into(),
            value: json!("must-not-apply"),
        }],
    };
    let done = Runtime::new(&store)
        .complete(
            &cancelled.snapshot.execution_id,
            cancelled.snapshot.owner_generation,
            task("held"),
            inflight.snapshot.tasks[0].attempt_id.clone().unwrap(),
            &result,
            facts(6),
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(done.snapshot.values, inflight.snapshot.values);
    assert!(done.snapshot.output.is_none());
    assert_eq!(done.snapshot.tasks[0].result, Some(result));
    store
        .delete_thread_fenced(
            &ThreadDeletionTarget::from_view(
                &store.thread_view(&thread("thread")).unwrap().unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
}

#[test]
fn mixed_partial_park_removes_only_unconsumed_grants_and_preserves_once_receipts() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let current = mixed(&store, "thread", "execution");
    mixed(&store, "other", "foreign");
    let before = store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let foreign = store
        .approval_view(&ExecutionId::new("foreign").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(before.grants.len(), 3);
    assert_eq!(before.acceptances.len(), 1);
    let cancelled = cancel(&store, "thread", &current);
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::CancelRequested);
    assert!(cancelled
        .snapshot
        .tasks
        .iter()
        .filter(|task| task.spec.requires_approval && !task.approved)
        .all(|task| task.status == TaskStatus::Cancelled));
    assert_eq!(
        cancelled.snapshot.attempts_used,
        current.snapshot.attempts_used
    );
    let after = store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(
        after.grants
            == before
                .grants
                .into_iter()
                .filter(|grant| grant.consumed)
                .collect::<Vec<_>>(),
        "Cancellation changed consumed authority records"
    );
    assert!(
        after.acceptances == before.acceptances,
        "Cancellation changed or invented an acceptance receipt"
    );
    assert!(
        store
            .approval_view(&ExecutionId::new("foreign").unwrap())
            .unwrap()
            .unwrap()
            == foreign,
        "Cancellation changed foreign authority"
    );
}

#[test]
fn every_captured_cancellation_field_is_compared_before_mutation() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let current = mixed(&store, "thread", "execution");
    let captured = target(&store, "thread");
    let mut cases = Vec::new();
    let mut changed = captured.clone();
    changed.thread_id = thread("missing");
    cases.push(changed);
    let mut changed = captured.clone();
    changed.thread_generation = ThreadGeneration::new("f".repeat(64)).unwrap();
    cases.push(changed);
    let mut changed = captured.clone();
    changed.resource_revision += 1;
    cases.push(changed);
    let mut changed = captured.clone();
    changed.execution_id = ExecutionId::new("other").unwrap();
    cases.push(changed);
    let mut changed = captured.clone();
    changed.execution_fence.revision += 1;
    cases.push(changed);
    let mut changed = captured.clone();
    changed.execution_fence.owner_generation += 1;
    cases.push(changed);
    let mut changed = captured;
    changed.turn_id = TurnId::new("other-turn").unwrap();
    cases.push(changed);
    let before = directory.rows();
    for changed in cases {
        assert_eq!(
            store.cancel_run_fenced(&changed, facts(current.snapshot.updated_at_ms + 1)),
            Err(StoreError::RevisionConflict)
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn disappearance_recreation_and_a_successor_never_retarget_cancellation() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let running = started(
        &store,
        "thread",
        "old",
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
    );
    let captured = target(&store, "thread");
    let cancelled = cancel(&store, "thread", &running);
    let settled = target(&store, "thread");
    store
        .delete_thread_fenced(
            &ThreadDeletionTarget::from_view(
                &store.thread_view(&thread("thread")).unwrap().unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    let before = directory.rows();
    assert_eq!(
        store.cancel_run_fenced(&settled, facts(cancelled.snapshot.updated_at_ms + 1)),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &before);
    let recreated = started(
        &store,
        "thread",
        "old",
        running.snapshot.definition.batches.clone(),
    );
    let replacement = target(&store, "thread");
    assert_eq!(replacement.execution_id, captured.execution_id);
    assert_eq!(replacement.execution_fence, captured.execution_fence);
    assert_eq!(replacement.turn_id, captured.turn_id);
    assert_eq!(replacement.resource_revision, captured.resource_revision);
    assert_ne!(replacement.thread_generation, captured.thread_generation);
    let before = directory.rows();
    assert_eq!(
        store.cancel_run_fenced(&captured, facts(recreated.snapshot.updated_at_ms + 1)),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        store.cancel_run_fenced(&settled, facts(recreated.snapshot.updated_at_ms + 1)),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &before);
    cancel(&store, "thread", &recreated);
    let successor = started(
        &store,
        "thread",
        "successor",
        running.snapshot.definition.batches,
    );
    let before = directory.rows();
    assert_eq!(
        store.cancel_run_fenced(&replacement, facts(successor.snapshot.updated_at_ms + 1)),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &before);
}

#[test]
fn changed_resource_alone_refuses_cancellation_without_mutating_authority() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let current = mixed(&store, "thread", "execution");
    let captured = target(&store, "thread");
    directory
        .connection()
        .execute(
            "UPDATE threads SET resource_revision = ?1 WHERE id = 'thread'",
            [(captured.resource_revision + 1).to_be_bytes().as_slice()],
        )
        .unwrap();
    assert_eq!(
        target(&store, "thread").execution_fence,
        captured.execution_fence
    );
    let before = directory.rows();
    assert_eq!(
        store.cancel_run_fenced(&captured, facts(current.snapshot.updated_at_ms + 1)),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &before);
}

#[test]
fn known_never_invoked_attempt_acknowledges_cancellation_without_fabricated_outcome() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let running = started(
        &store,
        "thread",
        "execution",
        vec![vec![spec("held", false, RecoveryPolicy::Pure)]],
    );
    let done = Runtime::new(&store)
        .dispatch_outcome(
            &running.snapshot.execution_id,
            (&running.snapshot).into(),
            task("held"),
            AttemptId::new("never-invoked").unwrap(),
            facts(3),
            || facts(5),
            |_| {
                let current = store
                    .execution(&running.snapshot.execution_id)
                    .unwrap()
                    .unwrap();
                let cancelled = cancel(&store, "thread", &current);
                assert_eq!(cancelled.snapshot.status, ExecutionStatus::CancelRequested);
                Ok(EffectOutcome::Cancelled)
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(done.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert!(done.snapshot.tasks[0].result.is_none());
    assert!(done.snapshot.tasks[0].failure.is_none());
    assert_eq!(done.snapshot.attempts_used, 1);
    assert_eq!(
        done.snapshot.attempt_history,
        vec![AttemptId::new("never-invoked").unwrap()]
    );
    assert_eq!(done.snapshot.values, running.snapshot.values);
    assert!(done.snapshot.output.is_none());
    drop(store);
    let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(
        reopened.execution(&done.snapshot.execution_id).unwrap(),
        Some(done)
    );
}

#[test]
fn late_known_failure_wins_cancellation_and_existing_failure_drain_refuses_new_cancel() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let running = started(
        &store,
        "thread",
        "execution",
        vec![vec![
            spec("failed", false, RecoveryPolicy::Pure),
            spec("sibling", false, RecoveryPolicy::Pure),
        ]],
    );
    let first = claimed(&store, &running, "failed");
    let both = claimed(&store, &first, "sibling");
    let cancelled = cancel(&store, "thread", &both);
    let draining = Runtime::new(&store)
        .complete_outcome(
            &cancelled.snapshot.execution_id,
            cancelled.snapshot.owner_generation,
            task("failed"),
            both.snapshot.tasks[0].attempt_id.clone().unwrap(),
            &EffectOutcome::Failed("actual late failure".into()),
            facts(cancelled.snapshot.updated_at_ms + 1),
        )
        .unwrap();
    assert_eq!(draining.snapshot.status, ExecutionStatus::Running);
    let before = directory.rows();
    assert_eq!(
        store.cancel_run_fenced(
            &target(&store, "thread"),
            facts(draining.snapshot.updated_at_ms + 1)
        ),
        Err(StoreError::NoRunInFlight)
    );
    unchanged(&directory, &before);
    let done = Runtime::new(&store)
        .complete_outcome(
            &draining.snapshot.execution_id,
            draining.snapshot.owner_generation,
            task("sibling"),
            both.snapshot.tasks[1].attempt_id.clone().unwrap(),
            &EffectOutcome::Succeeded(TaskResult {
                output: json!("late sibling"),
                writes: vec![],
            }),
            facts(draining.snapshot.updated_at_ms + 1),
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(
        done.snapshot.failure.as_deref(),
        Some("actual late failure")
    );
    assert_eq!(done.snapshot.values, running.snapshot.values);
    assert!(done.snapshot.output.is_none());
    assert!(store
        .journal(&done.snapshot.execution_id)
        .unwrap()
        .iter()
        .any(|entry| entry.status == ExecutionStatus::CancelRequested));
}

#[test]
fn exhausted_resource_revision_rolls_back_cancellation_and_grant_invalidation() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let current = mixed(&store, "thread", "execution");
    directory
        .connection()
        .execute(
            "UPDATE threads SET resource_revision = ?1 WHERE id = 'thread'",
            [u64::MAX.to_be_bytes().as_slice()],
        )
        .unwrap();
    let before = directory.rows();
    assert!(matches!(
        store.cancel_run_fenced(
            &target(&store, "thread"),
            facts(current.snapshot.updated_at_ms + 1)
        ),
        Err(StoreError::Unavailable(_))
    ));
    unchanged(&directory, &before);
}

#[test]
fn stable_park_and_completed_execution_have_no_active_cancel_target() {
    for disposition in ["parked", "completed"] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let running = started(
            &store,
            "thread",
            "execution",
            if disposition == "parked" {
                vec![vec![spec("gate", true, RecoveryPolicy::Pure)]]
            } else {
                vec![]
            },
        );
        let current = if disposition == "parked" {
            park(&store, &running, "gate")
        } else {
            running
        };
        let before = directory.rows();
        assert_eq!(
            store.cancel_run_fenced(
                &target(&store, "thread"),
                facts(current.snapshot.updated_at_ms + 1)
            ),
            Err(StoreError::NoRunInFlight)
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn cancellation_never_adopts_an_execution_from_a_prior_owner() {
    for disposition in ["accepted", "running", "inflight"] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let initial = admitted(
            &store,
            "thread",
            "execution",
            vec![vec![spec("held", false, RecoveryPolicy::Pure)]],
        );
        let current = if disposition == "accepted" {
            initial
        } else {
            let running = apply(&store, &initial, KernelCommand::Start);
            if disposition == "inflight" {
                claimed(&store, &running, "held")
            } else {
                running
            }
        };
        let captured = target(&store, "thread");
        drop(store);
        let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let before = directory.rows();
        assert_eq!(
            reopened.cancel_run_fenced(&captured, facts(current.snapshot.updated_at_ms + 1)),
            Err(StoreError::OwnerConflict)
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn foreign_or_inconsistent_authority_relationships_refuse_cancellation() {
    for mutation in [
        "grant_foreign_thread",
        "audit_foreign_thread",
        "grant_payload_task",
        "audit_payload_turn",
        "grant_payload_origin",
        "audit_payload_origin",
        "grant_parking_future",
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let current = mixed(&store, "thread", "execution");
        mixed(&store, "other", "foreign");
        let connection = directory.connection();
        if mutation.ends_with("foreign_thread") {
            let table = if mutation.starts_with("grant") {
                "approval_grants"
            } else {
                "approval_acceptances"
            };
            connection
                .execute(
                    &format!(
                        "UPDATE {table} SET thread_id = 'other' WHERE execution_id = 'execution'"
                    ),
                    [],
                )
                .unwrap();
        } else {
            let table = if mutation.starts_with("grant") {
                "approval_grants"
            } else {
                "approval_acceptances"
            };
            let record: String = connection
                .query_row(
                    &format!(
                        "SELECT record FROM {table} WHERE execution_id = 'execution' {}",
                        if table == "approval_grants" {
                            "AND task_id = 'gate'"
                        } else {
                            ""
                        }
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let mut value: serde_json::Value = serde_json::from_str(&record).unwrap();
            match mutation {
                "grant_payload_task" => {
                    value["task_id"] = json!("foreign-task");
                    connection.execute("UPDATE approval_grants SET task_id = 'foreign-task' WHERE execution_id = 'execution' AND task_id = 'gate'", []).unwrap();
                }
                "audit_payload_turn" => {
                    value["turn_id"] = json!("foreign-turn");
                    connection.execute("UPDATE approval_acceptances SET turn_id = 'foreign-turn' WHERE execution_id = 'execution'", []).unwrap();
                }
                "grant_payload_origin" => value["origin"]["principal"] = json!("foreign"),
                "audit_payload_origin" => value["context"]["actor"]["principal"] = json!("foreign"),
                "grant_parking_future" => {
                    let revision = current.snapshot.revision + 1;
                    value["task_parking_revision"] = json!(revision);
                    connection.execute("UPDATE approval_grants SET task_parking_revision = ?1 WHERE execution_id = 'execution' AND task_id = 'gate'", [revision.to_be_bytes().as_slice()]).unwrap();
                }
                _ => unreachable!(),
            }
            connection
                .execute(
                    &format!(
                        "UPDATE {table} SET record = ?1 WHERE execution_id = 'execution' {}",
                        if table == "approval_grants" {
                            if mutation == "grant_payload_task" {
                                "AND task_id = 'foreign-task'"
                            } else {
                                "AND task_id = 'gate'"
                            }
                        } else {
                            ""
                        }
                    ),
                    [serde_json::to_string(&value).unwrap()],
                )
                .unwrap();
        }
        let before = directory.rows();
        assert!(
            matches!(
                store.cancel_run_fenced(
                    &target(&store, "thread"),
                    facts(current.snapshot.updated_at_ms + 1)
                ),
                Err(StoreError::Corrupt(_))
            ),
            "Corrupt authority {mutation} was not rejected"
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn cancellation_write_faults_roll_back_all_records_and_survive_real_reopen() {
    for (point, trigger) in [
        ("second grant", "CREATE TRIGGER cancellation_fault BEFORE DELETE ON approval_grants WHEN OLD.interrupt_id = 'execution-pending-b' BEGIN SELECT CASE WHEN EXISTS (SELECT 1 FROM approval_grants WHERE interrupt_id = 'execution-pending-a') THEN RAISE(ABORT, 'first grant still exists') ELSE RAISE(ABORT, 'second grant deleted after first') END; END"),
        ("execution", "CREATE TRIGGER cancellation_fault BEFORE UPDATE ON executions BEGIN SELECT RAISE(ABORT, 'execution fault'); END"),
        ("resource", "CREATE TRIGGER cancellation_fault BEFORE UPDATE ON threads BEGIN SELECT RAISE(ABORT, 'resource fault'); END"),
        ("journal", "CREATE TRIGGER cancellation_fault BEFORE INSERT ON journal BEGIN SELECT RAISE(ABORT, 'journal fault'); END"),
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let current = mixed(&store, "thread", "execution");
        let captured = target(&store, "thread");
        let schema = directory.schema();
        let before = directory.rows();
        directory.connection().execute_batch(trigger).unwrap();
        let error = store.cancel_run_fenced(&captured, facts(current.snapshot.updated_at_ms + 1)).unwrap_err();
        assert!(matches!(&error, StoreError::Unavailable(_)), "SQL {point} failure was not reported");
        if point == "second grant" { assert!(matches!(&error, StoreError::Unavailable(message) if message.contains("second grant deleted after first")), "The fault did not exercise partial grant deletion"); }
        unchanged(&directory, &before);
        directory.connection().execute_batch("DROP TRIGGER cancellation_fault").unwrap();
        drop(store);
        let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert_eq!(reopened.owner_generation(), 2);
        assert_eq!(directory.schema(), schema);
        unchanged(&directory, &before);
        assert_eq!(reopened.execution(&current.snapshot.execution_id).unwrap(), Some(current.clone()));
        let resumed = recover(&reopened, &current, "recovered-after-fault");
        let cancelled = cancel(&reopened, "thread", &resumed);
        assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
        let view = reopened.approval_view(&current.snapshot.execution_id).unwrap().unwrap();
        assert_eq!(view.grants.len(), 1);
        assert!(view.grants[0].consumed);
        assert_eq!(view.acceptances.len(), 1);
        reopened.readiness().unwrap();
    }
}

#[test]
fn reopened_cancellation_recovers_safe_work_without_retry_and_blocks_unsafe_uncertainty() {
    for policy in [
        RecoveryPolicy::Pure,
        RecoveryPolicy::Idempotent,
        RecoveryPolicy::Unsafe,
        RecoveryPolicy::Reconcilable,
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let running = started(
            &store,
            "thread",
            "execution",
            vec![vec![spec("held", false, policy)]],
        );
        let inflight = claimed(&store, &running, "held");
        let cancelled = cancel(&store, "thread", &inflight);
        let schema = directory.schema();
        drop(store);
        let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert_eq!(
            reopened
                .execution(&cancelled.snapshot.execution_id)
                .unwrap(),
            Some(cancelled.clone())
        );
        let recovered = recover(&reopened, &cancelled, "recovered-cancellation");
        assert_eq!(
            recovered.snapshot.attempts_used,
            inflight.snapshot.attempts_used
        );
        assert_eq!(
            recovered.snapshot.attempt_history,
            inflight.snapshot.attempt_history
        );
        if matches!(policy, RecoveryPolicy::Pure | RecoveryPolicy::Idempotent) {
            assert_eq!(recovered.snapshot.status, ExecutionStatus::Cancelled);
            assert_eq!(recovered.snapshot.tasks[0].status, TaskStatus::Cancelled);
        } else {
            assert_eq!(recovered.snapshot.status, ExecutionStatus::RecoveryBlocked);
            assert_eq!(
                recovered.snapshot.tasks[0].status,
                TaskStatus::OutcomeUnknown
            );
            let before = directory.rows();
            assert_eq!(
                reopened.cancel_run_fenced(
                    &target(&reopened, "thread"),
                    facts(recovered.snapshot.updated_at_ms + 1)
                ),
                Err(StoreError::NoRunInFlight)
            );
            unchanged(&directory, &before);
        }
        assert!(reopened
            .journal(&recovered.snapshot.execution_id)
            .unwrap()
            .iter()
            .any(|entry| entry.status == ExecutionStatus::CancelRequested));
        assert_eq!(directory.schema(), schema);
        drop(reopened);
        let third = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert_eq!(
            third.execution(&recovered.snapshot.execution_id).unwrap(),
            Some(recovered)
        );
    }
}

#[test]
fn concurrent_cancellation_targets_allow_only_one_atomic_commit() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let running = started(
        &store,
        "thread",
        "execution",
        vec![vec![spec("first", false, RecoveryPolicy::Pure)]],
    );
    let captured = target(&store, "thread");
    let barrier = Barrier::new(2);
    let results = std::thread::scope(|scope| {
        (0..2)
            .map(|_| {
                let store = &store;
                let captured = &captured;
                let barrier = &barrier;
                let running = &running;
                scope.spawn(move || {
                    barrier.wait();
                    store.cancel_run_fenced(captured, facts(running.snapshot.updated_at_ms + 1))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(StoreError::RevisionConflict))
            .count(),
        1
    );
    assert_eq!(store.journal(&captured.execution_id).unwrap().len(), 3);
}
