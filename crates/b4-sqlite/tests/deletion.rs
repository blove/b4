use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::{params, types::Value as SqlValue, Connection};
use serde_json::{json, Map};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
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
            "b4-sqlite-delete-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
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
fn thread(name: &str) -> ThreadId {
    ThreadId::new(name).unwrap()
}
fn task() -> TaskId {
    TaskId::new("task").unwrap()
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        },
        policy_revision: "policy-v1".into(),
    }
}
fn create(store: &Arc<SqliteStore>, name: &str) {
    store
        .create_thread_with(ThreadSeed {
            id: thread(name),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
}
fn admitted(
    store: &Arc<SqliteStore>,
    name: &str,
    execution: &str,
    required: bool,
    recovery: RecoveryPolicy,
) -> StoredExecution {
    if store.thread(&thread(name)).unwrap().is_none() {
        create(store, name);
    }
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new(execution).unwrap(),
        TurnId::new(format!("turn-{execution}")).unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/delete#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task(),
                effect_id: EffectId::new("effect").unwrap(),
                recovery,
                requires_approval: required,
            }]],
            max_attempts: 3,
        },
        Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    store.admit(&thread(name), snapshot).unwrap()
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
            TransitionFacts {
                now_ms: current.snapshot.updated_at_ms + 1,
            },
        )
        .unwrap()
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
fn parked(store: &Arc<SqliteStore>, name: &str, execution: &str) -> StoredExecution {
    let initial = admitted(store, name, execution, true, RecoveryPolicy::Pure);
    let running = apply(store, &initial, KernelCommand::Start);
    ApprovalService::new(store.clone(), Allow)
        .unwrap()
        .park(
            &context(),
            &running.snapshot.execution_id,
            approval_fence(store, &running),
            task(),
            InterruptId::new(format!("interrupt-{execution}")).unwrap(),
            json!({"question":"Delete fixture?"}),
            100,
            TransitionFacts { now_ms: 3 },
        )
        .unwrap()
}
fn approved(store: &Arc<SqliteStore>, name: &str, execution: &str) -> StoredExecution {
    let parked = parked(store, name, execution);
    let grant = store
        .approval_view(&parked.snapshot.execution_id)
        .unwrap()
        .unwrap()
        .grants
        .remove(0);
    ApprovalService::new(store.clone(), Allow)
        .unwrap()
        .accept(
            &parked.snapshot.execution_id,
            approval_fence(store, &parked),
            ApprovalAcceptanceRequest {
                context: context(),
                route: parked.snapshot.definition.route.clone(),
                definition_revision: parked.snapshot.definition.revision.clone(),
                turn_id: TurnId::new(format!("approved-{execution}")).unwrap(),
                now_ms: 4,
                decisions: vec![GrantDecision {
                    interrupt_id: grant.interrupt_id,
                    decision: ApprovalDecision::Once,
                    cancelled: false,
                    token: Some(grant.token),
                }],
            },
        )
        .unwrap()
}
fn completed(
    store: &Arc<SqliteStore>,
    name: &str,
    execution: &str,
    required: bool,
) -> StoredExecution {
    let current = if required {
        approved(store, name, execution)
    } else {
        let initial = admitted(store, name, execution, false, RecoveryPolicy::Pure);
        apply(store, &initial, KernelCommand::Start)
    };
    let outcome = || {
        EffectOutcome::Succeeded(TaskResult {
            output: json!("complete"),
            writes: vec![],
        })
    };
    if required {
        ApprovalService::new(store.clone(), Allow)
            .unwrap()
            .dispatch_outcome(
                &context(),
                &current.snapshot.execution_id,
                approval_fence(store, &current),
                task(),
                AttemptId::new(format!("attempt-{execution}")).unwrap(),
                TransitionFacts { now_ms: 5 },
                || TransitionFacts { now_ms: 6 },
                |_| Ok(outcome()),
            )
            .unwrap()
    } else {
        Runtime::new(store)
            .dispatch_outcome(
                &current.snapshot.execution_id,
                (&current.snapshot).into(),
                task(),
                AttemptId::new(format!("attempt-{execution}")).unwrap(),
                TransitionFacts { now_ms: 5 },
                || TransitionFacts { now_ms: 6 },
                |_| Ok(outcome()),
            )
            .unwrap()
    }
}
fn target(store: &Arc<SqliteStore>, name: &str) -> ThreadDeletionTarget {
    ThreadDeletionTarget::from_view(&store.thread_view(&thread(name)).unwrap().unwrap()).unwrap()
}
fn unchanged(directory: &Directory, before: &[Vec<Vec<SqlValue>>]) {
    assert!(
        directory.rows() == before,
        "Refused deletion changed durable rows"
    );
}
fn assert_empty(directory: &Directory) {
    assert!(
        directory.rows().iter().all(Vec::is_empty),
        "Deletion left durable thread records"
    );
}

#[test]
fn idle_deletion_removes_creation_journal_and_rejects_recreated_incarnation() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    create(&store, "thread");
    let old = target(&store, "thread");
    store.delete_thread_fenced(&old).unwrap();
    assert_empty(&directory);
    assert_eq!(
        store.delete_thread_fenced(&old),
        Err(StoreError::RevisionConflict)
    );
    create(&store, "thread");
    let fresh = target(&store, "thread");
    assert_ne!(fresh.thread_generation, old.thread_generation);
    assert_eq!(fresh.resource_revision, 0);
    let before = directory.rows();
    assert_eq!(
        store.delete_thread_fenced(&old),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &before);
    store.delete_thread_fenced(&fresh).unwrap();
    assert_empty(&directory);
}

#[test]
fn deletion_atomically_removes_all_history_grants_and_audit_while_retaining_other_thread() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let first = completed(&store, "thread", "old", true);
    let second = completed(&store, "thread", "latest", false);
    let other = completed(&store, "other", "foreign", true);
    let retained = store.approval_view(&other.snapshot.execution_id).unwrap();
    let retained_journal = store.journal(&other.snapshot.execution_id).unwrap();
    let schema = directory.schema();
    assert!(!store
        .approval_view(&first.snapshot.execution_id)
        .unwrap()
        .unwrap()
        .acceptances
        .is_empty());
    store
        .delete_thread_fenced(&target(&store, "thread"))
        .unwrap();
    for execution in [first, second] {
        assert!(store
            .execution(&execution.snapshot.execution_id)
            .unwrap()
            .is_none());
        assert!(store
            .journal(&execution.snapshot.execution_id)
            .unwrap()
            .is_empty());
        assert!(store
            .approval_view(&execution.snapshot.execution_id)
            .unwrap()
            .is_none());
    }
    assert!(store.thread(&thread("thread")).unwrap().is_none());
    assert_eq!(
        store.approval_view(&other.snapshot.execution_id).unwrap(),
        retained
    );
    assert_eq!(
        store.journal(&other.snapshot.execution_id).unwrap(),
        retained_journal
    );
    assert_eq!(directory.schema(), schema);
    drop(store);
    let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert!(reopened.thread(&thread("thread")).unwrap().is_none());
    assert_eq!(
        reopened
            .approval_view(&other.snapshot.execution_id)
            .unwrap(),
        retained
    );
    reopened.readiness().unwrap();
}

#[test]
fn prior_owner_stable_park_is_deleted_without_recovery_or_effect() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let current = parked(&store, "thread", "parked");
    let captured = target(&store, "thread");
    assert_eq!(current.snapshot.status, ExecutionStatus::WaitingForApproval);
    drop(store);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    store.delete_thread_fenced(&captured).unwrap();
    assert_empty(&directory);
    drop(store);
    let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(reopened.owner_generation(), 3);
    assert!(reopened
        .execution(&current.snapshot.execution_id)
        .unwrap()
        .is_none());
    assert_empty(&directory);
}

#[test]
fn prior_owner_completed_failed_and_cancelled_snapshots_can_be_deleted() {
    for terminal in ["completed", "failed", "cancelled"] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let current = if terminal == "completed" {
            completed(&store, "thread", "terminal", false)
        } else {
            let initial = admitted(&store, "thread", "terminal", false, RecoveryPolicy::Pure);
            let running = apply(&store, &initial, KernelCommand::Start);
            if terminal == "cancelled" {
                apply(
                    &store,
                    &running,
                    KernelCommand::Cancel {
                        turn_id: running.snapshot.turn_id.clone(),
                        owner_generation: store.owner_generation(),
                    },
                )
            } else {
                Runtime::new(&store)
                    .dispatch_outcome(
                        &running.snapshot.execution_id,
                        (&running.snapshot).into(),
                        task(),
                        AttemptId::new("failed-attempt").unwrap(),
                        TransitionFacts { now_ms: 5 },
                        || TransitionFacts { now_ms: 6 },
                        |_| Ok(EffectOutcome::Failed("Public fixture failure".into())),
                    )
                    .unwrap()
            }
        };
        assert!(releases_slot(current.snapshot.status));
        let captured = target(&store, "thread");
        drop(store);
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert_eq!(store.owner_generation(), 2);
        store.delete_thread_fenced(&captured).unwrap();
        assert_empty(&directory);
    }
}

#[test]
fn deletion_compares_every_target_field_before_mutation() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    completed(&store, "thread", "completed", false);
    let captured = target(&store, "thread");
    let before = directory.rows();
    let mut variants = Vec::new();
    let mut value = captured.clone();
    value.thread_id = thread("absent");
    variants.push(value);
    let mut value = captured.clone();
    value.thread_generation = ThreadGeneration::new("0".repeat(64)).unwrap();
    variants.push(value);
    let mut value = captured.clone();
    value.resource_revision += 1;
    variants.push(value);
    let mut value = captured.clone();
    value.active_execution_id = Some(ExecutionId::new("wrong").unwrap());
    variants.push(value);
    let mut value = captured.clone();
    value.active_execution_id = None;
    variants.push(value);
    let mut value = captured.clone();
    value.execution_fence = None;
    variants.push(value);
    let mut value = captured.clone();
    value.execution_fence.as_mut().unwrap().revision += 1;
    variants.push(value);
    let mut value = captured.clone();
    value.execution_fence.as_mut().unwrap().owner_generation += 1;
    variants.push(value);
    for changed in variants {
        assert_eq!(
            store.delete_thread_fenced(&changed),
            Err(StoreError::RevisionConflict)
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn captured_admission_cannot_enter_a_recreated_thread_at_the_same_revision() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    create(&store, "thread");
    let captured = target(&store, "thread");
    store.delete_thread_fenced(&captured).unwrap();
    let replacement = store
        .create_thread_with(ThreadSeed {
            id: thread("thread"),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(ThreadOrigin {
                principal: "replacement-owner".into(),
                ..context().actor
            }),
        })
        .unwrap();
    assert_eq!(replacement.resource_revision, captured.resource_revision);
    assert_ne!(replacement.generation, captured.thread_generation);
    let initial = b4_kernel::new_execution(
        ExecutionId::new("stale").unwrap(),
        TurnId::new("stale-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/delete#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task(),
                effect_id: EffectId::new("effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }]],
            max_attempts: 3,
        },
        Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    let before = directory.rows();
    assert_eq!(
        store.admit_fenced(
            &thread("thread"),
            &captured.thread_generation,
            captured.resource_revision,
            initial
        ),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &before);
}

#[test]
fn captured_admission_on_a_deleted_thread_conflicts_while_unfenced_admission_is_missing() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    create(&store, "thread");
    let captured = target(&store, "thread");
    store.delete_thread_fenced(&captured).unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("stale").unwrap(),
        TurnId::new("stale-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/delete#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![],
            max_attempts: 1,
        },
        Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    let before = directory.rows();
    assert_eq!(
        store.admit_fenced(
            &thread("thread"),
            &captured.thread_generation,
            captured.resource_revision,
            initial.clone()
        ),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        store.admit(&thread("thread"), initial),
        Err(StoreError::ThreadNotFound)
    );
    unchanged(&directory, &before);
}

#[test]
fn changed_resource_or_successor_execution_is_not_retargeted() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    completed(&store, "thread", "first", false);
    let captured = target(&store, "thread");
    directory
        .connection()
        .execute(
            "UPDATE threads SET resource_revision = ?1 WHERE id = 'thread'",
            [(captured.resource_revision + 1).to_be_bytes().as_slice()],
        )
        .unwrap();
    let changed = directory.rows();
    assert_eq!(
        store.delete_thread_fenced(&captured),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &changed);
    let current = target(&store, "thread");
    admitted(&store, "thread", "successor", false, RecoveryPolicy::Pure);
    let successor = directory.rows();
    assert_eq!(
        store.delete_thread_fenced(&current),
        Err(StoreError::RevisionConflict)
    );
    unchanged(&directory, &successor);
}

#[test]
fn unfinished_and_uncertain_dispositions_refuse_deletion_across_owner_reopen() {
    for disposition in [
        "accepted",
        "running",
        "inflight",
        "cancel_requested",
        "recovery_blocked",
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let initial = admitted(
            &store,
            "thread",
            "execution",
            false,
            if disposition == "recovery_blocked" {
                RecoveryPolicy::Unsafe
            } else {
                RecoveryPolicy::Pure
            },
        );
        if disposition != "accepted" {
            let running = apply(&store, &initial, KernelCommand::Start);
            if !matches!(disposition, "running") {
                let inflight = apply(
                    &store,
                    &running,
                    KernelCommand::Dispatch {
                        task_id: task(),
                        attempt_id: AttemptId::new("lost-attempt").unwrap(),
                    },
                );
                if disposition == "cancel_requested" {
                    apply(
                        &store,
                        &inflight,
                        KernelCommand::Cancel {
                            turn_id: inflight.snapshot.turn_id.clone(),
                            owner_generation: store.owner_generation(),
                        },
                    );
                }
            }
        }
        drop(store);
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        if disposition == "recovery_blocked" {
            let current = store
                .execution(&ExecutionId::new("execution").unwrap())
                .unwrap()
                .unwrap();
            let blocked = Runtime::new(&store)
                .recover(
                    &current.snapshot.execution_id,
                    (&current.snapshot).into(),
                    current.snapshot.definition.revision.clone(),
                    TurnId::new("recovered").unwrap(),
                    TransitionFacts { now_ms: 10 },
                )
                .unwrap();
            assert_eq!(blocked.snapshot.status, ExecutionStatus::RecoveryBlocked);
        }
        let before = directory.rows();
        assert_eq!(
            store.delete_thread_fenced(&target(&store, "thread")),
            Err(StoreError::ThreadBusy)
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn historical_unfinished_or_corrupt_snapshots_fail_closed() {
    for corrupt in [false, true] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let original = admitted(&store, "thread", "old", false, RecoveryPolicy::Pure);
        let running = apply(&store, &original, KernelCommand::Start);
        Runtime::new(&store)
            .dispatch_outcome(
                &running.snapshot.execution_id,
                (&running.snapshot).into(),
                task(),
                AttemptId::new("old-attempt").unwrap(),
                TransitionFacts { now_ms: 5 },
                || TransitionFacts { now_ms: 6 },
                |_| {
                    Ok(EffectOutcome::Succeeded(TaskResult {
                        output: json!("old"),
                        writes: vec![],
                    }))
                },
            )
            .unwrap();
        completed(&store, "thread", "latest", false);
        let payload = if corrupt {
            "{\"malformed\":true}".into()
        } else {
            serde_json::to_string(&original.snapshot).unwrap()
        };
        directory
            .connection()
            .execute(
                "UPDATE executions SET revision = ?1, snapshot = ?2 WHERE id = 'old'",
                params![original.snapshot.revision.to_be_bytes().as_slice(), payload],
            )
            .unwrap();
        let before = directory.rows();
        assert!(matches!(
            store.delete_thread_fenced(&target(&store, "thread")),
            Err(StoreError::Corrupt(_))
        ));
        unchanged(&directory, &before);
    }
}

#[test]
fn every_cross_thread_dependent_index_relationship_is_rejected_in_both_directions() {
    for table in ["journal", "approval_grants", "approval_acceptances"] {
        for execution in ["owned", "foreign"] {
            let directory = Directory::new();
            let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
            completed(&store, "thread", "owned", true);
            completed(&store, "other", "foreign", true);
            let destination = if execution == "owned" {
                "other"
            } else {
                "thread"
            };
            directory
                .connection()
                .execute(
                    &format!("UPDATE {table} SET thread_id = ?1 WHERE execution_id = ?2"),
                    params![destination, execution],
                )
                .unwrap();
            let before = directory.rows();
            assert!(
                matches!(
                    store.delete_thread_fenced(&target(&store, "thread")),
                    Err(StoreError::Corrupt(_))
                ),
                "Cross-thread {table} relationship was not rejected"
            );
            unchanged(&directory, &before);
        }
    }
}

#[test]
fn related_payload_and_index_corruption_is_rejected_without_cleanup() {
    for (table, field, value) in [
        ("journal", "payload", "{}"),
        ("approval_grants", "record", "{}"),
        ("approval_acceptances", "record", "{}"),
        ("journal", "status", "wrong-status"),
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        completed(&store, "thread", "old", true);
        completed(&store, "thread", "latest", false);
        directory
            .connection()
            .execute(
                &format!("UPDATE {table} SET {field} = ?1 WHERE execution_id = 'old'"),
                [value],
            )
            .unwrap();
        let before = directory.rows();
        assert!(
            matches!(
                store.delete_thread_fenced(&target(&store, "thread")),
                Err(StoreError::Corrupt(_))
            ),
            "Corrupt {table} payload/index was not rejected"
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn historical_journal_must_be_complete_ordered_and_monotonic() {
    for mutation in ["gap", "duplicate", "order", "clock"] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        completed(&store, "thread", "old", false);
        completed(&store, "thread", "latest", false);
        let connection = directory.connection();
        match mutation {
            "gap" => {
                connection
                    .execute(
                        "DELETE FROM journal WHERE execution_id = 'old' AND revision = ?1",
                        [1_u64.to_be_bytes().as_slice()],
                    )
                    .unwrap();
            }
            "duplicate" => {
                connection.execute("INSERT INTO journal (kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload) SELECT kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal WHERE execution_id = 'old' AND revision = ?1", [1_u64.to_be_bytes().as_slice()]).unwrap();
            }
            "order" => {
                let sequences = [1_u64, 2_u64].map(|revision| connection.query_row("SELECT sequence FROM journal WHERE execution_id = 'old' AND revision = ?1", [revision.to_be_bytes().as_slice()], |row| row.get::<_, i64>(0)).unwrap());
                for (from, to) in [
                    (sequences[0], -1),
                    (sequences[1], sequences[0]),
                    (-1, sequences[1]),
                ] {
                    connection
                        .execute(
                            "UPDATE journal SET sequence = ?1 WHERE sequence = ?2",
                            params![to, from],
                        )
                        .unwrap();
                }
            }
            "clock" => {
                let payload: String = connection
                    .query_row(
                        "SELECT payload FROM journal WHERE execution_id = 'old' AND revision = ?1",
                        [2_u64.to_be_bytes().as_slice()],
                        |row| row.get(0),
                    )
                    .unwrap();
                let mut entry: JournalEntry = serde_json::from_str(&payload).unwrap();
                entry.updated_at_ms = 1;
                connection.execute("UPDATE journal SET updated_at_ms = ?1, payload = ?2 WHERE execution_id = 'old' AND revision = ?3", params![entry.updated_at_ms.to_be_bytes().as_slice(), serde_json::to_string(&entry).unwrap(), 2_u64.to_be_bytes().as_slice()]).unwrap();
            }
            _ => unreachable!(),
        }
        let before = directory.rows();
        assert!(
            matches!(
                store.delete_thread_fenced(&target(&store, "thread")),
                Err(StoreError::Corrupt(_))
            ),
            "Historical journal {mutation} was not rejected"
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn historical_authority_must_bind_tasks_parking_and_accepted_turn() {
    for mutation in [
        "grant_task",
        "grant_effect",
        "grant_revision",
        "acceptance_revision",
        "acceptance_turn",
        "acceptance_task",
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let old = completed(&store, "thread", "old", true);
        completed(&store, "thread", "latest", false);
        let connection = directory.connection();
        let table = if mutation.starts_with("grant_") {
            "approval_grants"
        } else {
            "approval_acceptances"
        };
        let record: String = connection
            .query_row(
                &format!("SELECT record FROM {table} WHERE execution_id = 'old'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&record).unwrap();
        match mutation {
            "grant_task" => {
                value["task_id"] = json!("foreign-task");
                connection.execute("UPDATE approval_grants SET task_id = 'foreign-task' WHERE execution_id = 'old'", []).unwrap();
            }
            "grant_effect" => value["effect_id"] = json!("foreign-effect"),
            "grant_revision" => {
                let revision = old.snapshot.revision + 1;
                value["task_parking_revision"] = json!(revision);
                connection.execute("UPDATE approval_grants SET task_parking_revision = ?1 WHERE execution_id = 'old'", [revision.to_be_bytes().as_slice()]).unwrap();
            }
            "acceptance_revision" => {
                value["previous_revision"] = json!(old.snapshot.revision);
                connection.execute("UPDATE approval_acceptances SET previous_revision = ?1 WHERE execution_id = 'old'", [old.snapshot.revision.to_be_bytes().as_slice()]).unwrap();
            }
            "acceptance_turn" => {
                value["turn_id"] = json!("foreign-turn");
                connection.execute("UPDATE approval_acceptances SET turn_id = 'foreign-turn' WHERE execution_id = 'old'", []).unwrap();
            }
            "acceptance_task" => value["decisions"][0]["task_id"] = json!("foreign-task"),
            _ => unreachable!(),
        }
        connection
            .execute(
                &format!("UPDATE {table} SET record = ?1 WHERE execution_id = 'old'"),
                [serde_json::to_string(&value).unwrap()],
            )
            .unwrap();
        let before = directory.rows();
        assert!(
            matches!(
                store.delete_thread_fenced(&target(&store, "thread")),
                Err(StoreError::Corrupt(_))
            ),
            "Historical authority {mutation} was not rejected"
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn authority_payload_origin_must_match_the_owned_thread() {
    for table in ["approval_grants", "approval_acceptances"] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        completed(&store, "thread", "owned", true);
        let connection = directory.connection();
        let record: String = connection
            .query_row(
                &format!("SELECT record FROM {table} WHERE execution_id = 'owned'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&record).unwrap();
        if table == "approval_grants" {
            value["origin"]["principal"] = json!("foreign");
        } else {
            value["context"]["actor"]["principal"] = json!("foreign");
        }
        connection
            .execute(
                &format!("UPDATE {table} SET record = ?1 WHERE execution_id = 'owned'"),
                [serde_json::to_string(&value).unwrap()],
            )
            .unwrap();
        let before = directory.rows();
        assert!(
            matches!(
                store.delete_thread_fenced(&target(&store, "thread")),
                Err(StoreError::Corrupt(_))
            ),
            "Foreign authority payload identity was not rejected"
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn historical_authority_definition_provenance_must_match_its_execution() {
    for (table, field) in [
        ("approval_grants", "route"),
        ("approval_grants", "definition_revision"),
        ("approval_acceptances", "definition_revision"),
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        completed(&store, "thread", "old", true);
        completed(&store, "thread", "latest", false);
        let connection = directory.connection();
        let record: String = connection
            .query_row(
                &format!("SELECT record FROM {table} WHERE execution_id = 'old'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&record).unwrap();
        value[field] = json!(if field == "route" {
            "/foreign#agent"
        } else {
            "foreign-definition"
        });
        connection
            .execute(
                &format!("UPDATE {table} SET record = ?1 WHERE execution_id = 'old'"),
                [serde_json::to_string(&value).unwrap()],
            )
            .unwrap();
        if table == "approval_acceptances" {
            connection.execute("UPDATE approval_acceptances SET definition_revision = 'foreign-definition' WHERE execution_id = 'old'", []).unwrap();
        }
        let before = directory.rows();
        assert!(
            matches!(
                store.delete_thread_fenced(&target(&store, "thread")),
                Err(StoreError::Corrupt(_))
            ),
            "Historical authority definition mismatch was not rejected"
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn missing_related_execution_fails_closed_for_each_dependent_table() {
    for table in ["journal", "approval_grants", "approval_acceptances"] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        completed(&store, "thread", "owned", true);
        let connection = directory.connection();
        connection
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        connection.execute(&format!("UPDATE {table} SET execution_id = 'missing-execution' WHERE execution_id = 'owned'"), []).unwrap();
        let before = directory.rows();
        assert!(
            matches!(
                store.delete_thread_fenced(&target(&store, "thread")),
                Err(StoreError::Corrupt(_))
            ),
            "Missing {table} execution relationship was not rejected"
        );
        unchanged(&directory, &before);
    }
}

#[test]
fn another_threads_active_pointer_cannot_bind_an_owned_historical_execution() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    completed(&store, "thread", "owned", false);
    completed(&store, "other", "foreign", false);
    directory
        .connection()
        .execute(
            "UPDATE threads SET active_execution_id = 'owned' WHERE id = 'other'",
            [],
        )
        .unwrap();
    let before = directory.rows();
    assert!(matches!(
        store.delete_thread_fenced(&target(&store, "thread")),
        Err(StoreError::Corrupt(_))
    ));
    unchanged(&directory, &before);
}

#[test]
fn each_sql_cleanup_failure_rolls_back_pointer_history_and_authority_then_reopens() {
    for (operation, table) in [
        ("UPDATE", "threads"),
        ("DELETE", "approval_grants"),
        ("DELETE", "approval_acceptances"),
        ("DELETE", "journal"),
        ("DELETE", "executions"),
        ("DELETE", "threads"),
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        completed(&store, "thread", "old", true);
        completed(&store, "thread", "latest", false);
        let captured = target(&store, "thread");
        let before = directory.rows();
        let schema = directory.schema();
        directory.connection().execute_batch(&format!("CREATE TRIGGER reject_deletion BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'controlled-delete-failure'); END;")).unwrap();
        assert!(
            matches!(
                store.delete_thread_fenced(&captured),
                Err(StoreError::Unavailable(_))
            ),
            "Expected controlled {operation} {table} failure"
        );
        unchanged(&directory, &before);
        directory
            .connection()
            .execute_batch("DROP TRIGGER reject_deletion;")
            .unwrap();
        drop(store);
        let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert_eq!(reopened.owner_generation(), 2);
        unchanged(&directory, &before);
        assert_eq!(directory.schema(), schema);
        reopened.delete_thread_fenced(&captured).unwrap();
        assert_empty(&directory);
        reopened.readiness().unwrap();
    }
}
