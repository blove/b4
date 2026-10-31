mod support;

use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::Arc;
use support::{initial, plan, task};

fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn cancel<S: Store>(
    store: S,
    target: &RunCancellationTarget,
    now_ms: u64,
) -> Result<StoredExecution, StoreError> {
    store.cancel_run_fenced(target, facts(now_ms))
}
fn view(store: &MemoryStore, id: &ThreadId) -> ThreadView {
    store.thread_view(id).unwrap().unwrap()
}
fn target(store: &MemoryStore, id: &ThreadId) -> RunCancellationTarget {
    RunCancellationTarget::from_view(&view(store, id)).unwrap()
}
fn admitted(store: &MemoryStore, name: &str, batches: Vec<Vec<TaskSpec>>) -> StoredExecution {
    let thread = ThreadId::new(name).unwrap();
    store.create_thread(thread.clone()).unwrap();
    store.admit(&thread, initial(name, 1, batches)).unwrap()
}
fn commit(
    store: &MemoryStore,
    current: &StoredExecution,
    command: KernelCommand,
) -> StoredExecution {
    store
        .commit(
            &current.snapshot.execution_id,
            (&current.snapshot).into(),
            plan(&current.snapshot, command),
        )
        .unwrap()
}

#[test]
fn accepted_cancellation_is_atomic_and_keeps_turn_and_history() {
    let store = Arc::new(MemoryStore::new());
    let admitted = admitted(
        &store,
        "accepted",
        vec![vec![task("first", false)], vec![task("later", false)]],
    );
    let captured = target(&store, &admitted.thread_id);
    let before = view(&store, &admitted.thread_id);
    let cancelled = cancel(&store, &captured, 101).unwrap();
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(cancelled.snapshot.revision, admitted.snapshot.revision + 1);
    assert_eq!(cancelled.snapshot.turn_id, admitted.snapshot.turn_id);
    assert_eq!(
        cancelled.snapshot.turn_history,
        admitted.snapshot.turn_history
    );
    assert_eq!(
        cancelled.snapshot.attempt_history,
        admitted.snapshot.attempt_history
    );
    assert_eq!(cancelled.snapshot.attempts_used, 0);
    assert_eq!(cancelled.snapshot.tasks.len(), 1);
    assert!(cancelled.snapshot.output.is_none());
    assert_eq!(
        view(&store, &admitted.thread_id).thread.resource_revision,
        before.thread.resource_revision + 1
    );
    assert_eq!(
        store.journal(&captured.execution_id).unwrap(),
        vec![
            JournalEntry::from(&admitted.snapshot),
            JournalEntry::from(&cancelled.snapshot)
        ]
    );
    let latest = target(&store, &admitted.thread_id);
    assert_eq!(
        store.cancel_run_fenced(&latest, facts(102)),
        Err(StoreError::NoRunInFlight)
    );
    assert_eq!(
        store.cancel_run_fenced(&captured, facts(102)),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        store.execution(&captured.execution_id).unwrap(),
        Some(cancelled)
    );
}

#[test]
fn cancellation_compares_every_captured_field_without_mutation() {
    let store = MemoryStore::new();
    let admitted = admitted(&store, "fences", vec![vec![task("a", false)]]);
    let captured = target(&store, &admitted.thread_id);
    let before = view(&store, &admitted.thread_id);
    let journal = store.journal(&captured.execution_id).unwrap();
    let mut bad = Vec::new();
    let mut value = captured.clone();
    value.thread_id = ThreadId::new("absent").unwrap();
    bad.push(value);
    let mut value = captured.clone();
    value.thread_generation = ThreadGeneration::new("f".repeat(64)).unwrap();
    bad.push(value);
    let mut value = captured.clone();
    value.resource_revision += 1;
    bad.push(value);
    let mut value = captured.clone();
    value.execution_id = ExecutionId::new("other-execution").unwrap();
    bad.push(value);
    let mut value = captured.clone();
    value.execution_fence.revision += 1;
    bad.push(value);
    let mut value = captured.clone();
    value.execution_fence.owner_generation += 1;
    bad.push(value);
    let mut value = captured.clone();
    value.turn_id = TurnId::new("other-turn").unwrap();
    bad.push(value);
    for value in bad {
        assert_eq!(
            store.cancel_run_fenced(&value, facts(101)),
            Err(StoreError::RevisionConflict)
        );
        assert_eq!(view(&store, &admitted.thread_id), before);
        assert_eq!(store.journal(&captured.execution_id).unwrap(), journal);
    }
    store.readiness().unwrap();
}

#[test]
fn cancellation_never_retargets_recreated_or_successor_execution() {
    let store = MemoryStore::new();
    let accepted = admitted(&store, "aba", vec![]);
    let captured = target(&store, &accepted.thread_id);
    let completed = commit(&store, &accepted, KernelCommand::Start);
    let successor = store
        .admit(
            &accepted.thread_id,
            initial("successor", 1, vec![vec![task("a", false)]]),
        )
        .unwrap();
    assert_eq!(
        store.cancel_run_fenced(&captured, facts(110)),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        store.execution(&successor.snapshot.execution_id).unwrap(),
        Some(successor.clone())
    );
    let newer = target(&store, &accepted.thread_id);
    store.cancel_run_fenced(&newer, facts(110)).unwrap();
    let deletion = ThreadDeletionTarget::from_view(&view(&store, &accepted.thread_id)).unwrap();
    store.delete_thread_fenced(&deletion).unwrap();
    store.create_thread(accepted.thread_id.clone()).unwrap();
    let recreated = store
        .admit(&accepted.thread_id, initial("recreated", 1, vec![]))
        .unwrap();
    assert_eq!(
        store.cancel_run_fenced(&newer, facts(120)),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        store.execution(&recreated.snapshot.execution_id).unwrap(),
        Some(recreated)
    );
    assert_eq!(completed.snapshot.status, ExecutionStatus::Completed);
}

#[test]
fn shared_validation_recomputes_cancel_and_rejects_prior_owner_and_corruption() {
    let store = MemoryStore::new();
    let accepted = admitted(&store, "shared", vec![vec![task("a", false)]]);
    let ordinary = view(&store, &accepted.thread_id);
    let captured = target(&store, &accepted.thread_id);
    assert_eq!(
        validate_run_cancellation(&ordinary, &captured, 1, facts(101)).unwrap(),
        plan(
            &accepted.snapshot,
            KernelCommand::Cancel {
                turn_id: accepted.snapshot.turn_id.clone(),
                owner_generation: 1
            }
        )
    );
    assert_eq!(
        validate_run_cancellation(&ordinary, &captured, 2, facts(101)),
        Err(StoreError::OwnerConflict)
    );
    let mut corrupt = ordinary.clone();
    corrupt
        .execution
        .as_mut()
        .unwrap()
        .snapshot
        .turn_history
        .clear();
    assert!(matches!(
        validate_run_cancellation(&corrupt, &captured, 1, facts(101)),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        validate_run_cancellation(&ordinary, &captured, 1, facts(99)),
        Err(StoreError::InvalidCommit(_))
    ));
    assert_eq!(view(&store, &accepted.thread_id), ordinary);
}

#[test]
fn target_capture_rejects_incoherent_view_and_idle_has_no_run() {
    let store = MemoryStore::new();
    let id = ThreadId::new("idle").unwrap();
    store.create_thread(id.clone()).unwrap();
    let idle = view(&store, &id);
    assert_eq!(
        RunCancellationTarget::from_view(&idle),
        Err(StoreError::NoRunInFlight)
    );
    let accepted = store.admit(&id, initial("active", 1, vec![])).unwrap();
    let mut missing = view(&store, &id);
    missing.execution = None;
    assert!(matches!(
        RunCancellationTarget::from_view(&missing),
        Err(StoreError::Corrupt(_))
    ));
    let mut crossed = view(&store, &id);
    crossed.execution.as_mut().unwrap().thread_id = ThreadId::new("foreign").unwrap();
    assert!(matches!(
        RunCancellationTarget::from_view(&crossed),
        Err(StoreError::Corrupt(_))
    ));
    assert_eq!(
        store.execution(&accepted.snapshot.execution_id).unwrap(),
        Some(accepted)
    );
}

#[test]
fn cancelled_outcome_acknowledges_only_committed_cancellation_and_preserves_budget() {
    let store = Arc::new(MemoryStore::new());
    let runtime = Runtime::new(store.clone());
    let accepted = admitted(&store, "outcome", vec![vec![task("a", false)]]);
    let running = commit(&store, &accepted, KernelCommand::Start);
    let task_id = TaskId::new("a").unwrap();
    let attempt_id = AttemptId::new("attempt-a").unwrap();
    let claimed = commit(
        &store,
        &running,
        KernelCommand::Dispatch {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        },
    );
    assert!(matches!(
        runtime.complete_outcome(
            &claimed.snapshot.execution_id,
            1,
            task_id.clone(),
            attempt_id.clone(),
            &EffectOutcome::Cancelled,
            facts(103)
        ),
        Err(RuntimeError::Kernel(_))
    ));
    let cancelled = store
        .cancel_run_fenced(&target(&store, &accepted.thread_id), facts(103))
        .unwrap();
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::CancelRequested);
    assert_eq!(
        store.admit(&accepted.thread_id, initial("blocked", 1, vec![])),
        Err(StoreError::ThreadBusy)
    );
    let settled = runtime
        .complete_outcome(
            &claimed.snapshot.execution_id,
            1,
            task_id.clone(),
            attempt_id.clone(),
            &EffectOutcome::Cancelled,
            facts(104),
        )
        .unwrap();
    assert_eq!(settled.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(settled.snapshot.attempts_used, 1);
    assert_eq!(
        settled.snapshot.attempt_history,
        claimed.snapshot.attempt_history
    );
    assert!(settled.snapshot.tasks[0].result.is_none());
    assert!(settled.snapshot.failure.is_none());
    let journal = store.journal(&claimed.snapshot.execution_id).unwrap();
    assert_eq!(
        runtime
            .complete_outcome(
                &claimed.snapshot.execution_id,
                1,
                task_id,
                attempt_id,
                &EffectOutcome::Cancelled,
                facts(105)
            )
            .unwrap(),
        settled
    );
    assert_eq!(
        store.journal(&claimed.snapshot.execution_id).unwrap(),
        journal
    );
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
struct Authority;
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
fn approval_fence(store: &MemoryStore, id: &ExecutionId) -> ApprovalFence {
    let v = store.approval_view(id).unwrap().unwrap();
    ApprovalFence {
        execution: (&v.execution.snapshot).into(),
        resource_revision: v.thread.resource_revision,
    }
}
fn park(
    service: &ApprovalService<Arc<MemoryStore>, Authority>,
    id: &ExecutionId,
    name: &str,
    now: u64,
) {
    service
        .park(
            &context(),
            id,
            approval_fence(service.store(), id),
            TaskId::new(name).unwrap(),
            InterruptId::new(format!("interrupt-{name}")).unwrap(),
            json!({"question":name}),
            1000,
            facts(now),
        )
        .unwrap();
}

#[test]
fn atomic_cancel_invalidates_pending_grants_and_preserves_consumed_receipt_history() {
    let store = Arc::new(MemoryStore::new());
    let id = ThreadId::new("authority").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let accepted = store
        .admit(
            &id,
            initial(
                "authority-execution",
                1,
                vec![
                    vec![task("old", true)],
                    vec![task("a", true), task("b", true), task("c", false)],
                ],
            ),
        )
        .unwrap();
    commit(&store, &accepted, KernelCommand::Start);
    let service = ApprovalService::with_config(
        store.clone(),
        Authority,
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap();
    let execution = &accepted.snapshot.execution_id;
    park(&service, execution, "old", 102);
    let pending = service
        .pending(&context(), execution, approval_fence(&store, execution))
        .unwrap();
    service
        .accept(
            execution,
            approval_fence(&store, execution),
            ApprovalAcceptanceRequest {
                context: context(),
                route: accepted.snapshot.definition.route.clone(),
                definition_revision: accepted.snapshot.definition.revision.clone(),
                turn_id: TurnId::new("resumed-turn").unwrap(),
                now_ms: 103,
                decisions: pending
                    .iter()
                    .map(|entry| GrantDecision {
                        interrupt_id: entry.interrupt_id.clone(),
                        decision: ApprovalDecision::Once,
                        cancelled: false,
                        token: Some(entry.token.clone()),
                    })
                    .collect(),
            },
        )
        .unwrap();
    service
        .dispatch_outcome(
            &context(),
            execution,
            approval_fence(&store, execution),
            TaskId::new("old").unwrap(),
            AttemptId::new("old-attempt").unwrap(),
            facts(104),
            || facts(105),
            |_| {
                Ok(EffectOutcome::Succeeded(TaskResult {
                    output: json!(true),
                    writes: vec![],
                }))
            },
        )
        .unwrap();
    park(&service, execution, "a", 106);
    park(&service, execution, "b", 107);
    let running = store.execution(execution).unwrap().unwrap();
    let claimed = commit(
        &store,
        &running,
        KernelCommand::Dispatch {
            task_id: TaskId::new("c").unwrap(),
            attempt_id: AttemptId::new("c-attempt").unwrap(),
        },
    );
    let before = store.approval_view(execution).unwrap().unwrap();
    assert_eq!(before.grants.iter().filter(|g| !g.consumed).count(), 2);
    let consumed: Vec<_> = before
        .grants
        .iter()
        .filter(|g| g.consumed)
        .cloned()
        .collect();
    let cancelled = store
        .cancel_run_fenced(&target(&store, &id), facts(109))
        .unwrap();
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::CancelRequested);
    let after = store.approval_view(execution).unwrap().unwrap();
    assert_eq!(after.grants, consumed);
    assert_eq!(after.acceptances, before.acceptances);
    assert_eq!(
        after.thread.resource_revision,
        before.thread.resource_revision + 1
    );
    assert_eq!(
        after.execution.snapshot.attempt_history,
        claimed.snapshot.attempt_history
    );
    assert!(after
        .execution
        .snapshot
        .tasks
        .iter()
        .filter(|t| t.spec.requires_approval && t.spec.id != TaskId::new("old").unwrap())
        .all(|t| t.status == TaskStatus::Cancelled));
    assert_eq!(
        service.pending(&context(), execution, approval_fence(&store, execution)),
        Err(ApprovalError::NotStablePark)
    );
    let runtime = Runtime::new(store.clone());
    let settled = runtime
        .complete_outcome(
            execution,
            1,
            TaskId::new("c").unwrap(),
            AttemptId::new("c-attempt").unwrap(),
            &EffectOutcome::Succeeded(TaskResult {
                output: json!("late"),
                writes: vec![StateWrite::Replace {
                    field: "late".into(),
                    value: json!(true),
                }],
            }),
            facts(110),
        )
        .unwrap();
    assert_eq!(settled.snapshot.status, ExecutionStatus::Cancelled);
    assert!(settled.snapshot.output.is_none());
    assert!(!settled.snapshot.values.contains_key("late"));
    assert_eq!(
        store.approval_view(execution).unwrap().unwrap().grants,
        consumed
    );
    assert_eq!(
        store.approval_view(execution).unwrap().unwrap().acceptances,
        before.acceptances
    );
}

#[test]
fn stable_approval_park_and_failure_drain_refuse_new_cancellation() {
    let store = MemoryStore::new();
    let accepted = admitted(&store, "parked", vec![vec![task("a", true)]]);
    let running = commit(&store, &accepted, KernelCommand::Start);
    commit(
        &store,
        &running,
        KernelCommand::Park {
            task_id: TaskId::new("a").unwrap(),
            interrupt_id: InterruptId::new("a-approval").unwrap(),
            prompt: json!("approve"),
        },
    );
    let before = view(&store, &accepted.thread_id);
    assert_eq!(
        store.cancel_run_fenced(&target(&store, &accepted.thread_id), facts(103)),
        Err(StoreError::NoRunInFlight)
    );
    assert_eq!(view(&store, &accepted.thread_id), before);
    let accepted = admitted(
        &store,
        "failure",
        vec![vec![task("a", false), task("b", false)]],
    );
    let mut current = commit(&store, &accepted, KernelCommand::Start);
    for name in ["a", "b"] {
        current = commit(
            &store,
            &current,
            KernelCommand::Dispatch {
                task_id: TaskId::new(name).unwrap(),
                attempt_id: AttemptId::new(format!("attempt-{name}")).unwrap(),
            },
        );
    }
    let failed = commit(
        &store,
        &current,
        KernelCommand::FailAttempt {
            task_id: TaskId::new("a").unwrap(),
            attempt_id: AttemptId::new("attempt-a").unwrap(),
            failure: "real failure".into(),
        },
    );
    assert_eq!(failed.snapshot.status, ExecutionStatus::Running);
    assert_eq!(
        store.cancel_run_fenced(&target(&store, &accepted.thread_id), facts(110)),
        Err(StoreError::NoRunInFlight)
    );
    assert_eq!(
        store.execution(&failed.snapshot.execution_id).unwrap(),
        Some(failed)
    );
}

struct LegacyStore(MemoryStore);
impl Store for LegacyStore {
    fn owner_generation(&self) -> u64 {
        self.0.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.0.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.0.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.0.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.0.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.0.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.0.admit_fenced(id, generation, revision, snapshot)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.0.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.0.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.0.journal(id)
    }
}

#[test]
fn custom_store_defaults_to_unsupported_without_mutation() {
    let store = Arc::new(LegacyStore(MemoryStore::new()));
    let accepted = admitted(&store.0, "legacy", vec![vec![task("a", false)]]);
    let before = view(&store.0, &accepted.thread_id);
    let target = RunCancellationTarget::from_view(&before).unwrap();
    assert_eq!(
        cancel(&store, &target, 101),
        Err(StoreError::UnsupportedOperation)
    );
    assert_eq!(view(&store.0, &accepted.thread_id), before);
    store.readiness().unwrap();
}
