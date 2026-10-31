mod support;

use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::Arc;
use support::{initial, noop, plan, task};

fn thread(name: &str) -> ThreadId {
    ThreadId::new(name).unwrap()
}
fn view(store: &MemoryStore, id: &ThreadId) -> ThreadView {
    store.thread_view(id).unwrap().unwrap()
}
fn target(store: &MemoryStore, id: &ThreadId) -> ThreadDeletionTarget {
    ThreadDeletionTarget::from_view(&view(store, id)).unwrap()
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
fn admitted(store: &MemoryStore, name: &str, tasks: Vec<Vec<TaskSpec>>) -> StoredExecution {
    let id = thread(name);
    store.create_thread(id.clone()).unwrap();
    store.admit(&id, initial(name, 1, tasks)).unwrap()
}

#[test]
fn idle_deletion_is_fenced_and_recreation_changes_generation() {
    let store = MemoryStore::new();
    let id = thread("idle");
    store.create_thread(id.clone()).unwrap();
    let captured = target(&store, &id);
    assert_eq!(store.delete_thread_fenced(&captured), Ok(()));
    assert_eq!(store.thread_view(&id).unwrap(), None);
    assert_eq!(
        store.delete_thread_fenced(&captured),
        Err(StoreError::RevisionConflict)
    );
    let replacement = store.create_thread(id.clone()).unwrap();
    assert_ne!(replacement.generation, captured.thread_generation);
    assert_eq!(replacement.resource_revision, captured.resource_revision);
    assert_eq!(
        store.delete_thread_fenced(&captured),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(store.thread(&id).unwrap(), Some(replacement));
}

#[test]
fn fenced_admission_never_retargets_recreated_thread() {
    let store = Arc::new(MemoryStore::new());
    let id = thread("admission-aba");
    let old = store.create_thread(id.clone()).unwrap();
    store.delete_thread_fenced(&target(&store, &id)).unwrap();
    let replacement = store.create_thread(id.clone()).unwrap();
    assert_ne!(old.generation, replacement.generation);
    assert_eq!(old.resource_revision, replacement.resource_revision);
    let runtime = Runtime::new(store.clone());
    let candidate = initial("stale-admission", 1, vec![]);
    assert_eq!(
        runtime.admit_fenced(
            &id,
            &old.generation,
            old.resource_revision,
            candidate.clone()
        ),
        Err(RuntimeError::Store(StoreError::RevisionConflict))
    );
    assert_eq!(store.thread(&id).unwrap(), Some(replacement.clone()));
    assert!(store.execution(&candidate.execution_id).unwrap().is_none());
    assert!(store.journal(&candidate.execution_id).unwrap().is_empty());
    assert!(runtime
        .admit_fenced(
            &id,
            &replacement.generation,
            replacement.resource_revision,
            candidate
        )
        .is_ok());
    store.readiness().unwrap();
}

#[test]
fn disappearance_after_capture_is_a_fenced_admission_conflict() {
    let store = MemoryStore::new();
    let id = thread("admission-missing");
    let old = store.create_thread(id.clone()).unwrap();
    store.delete_thread_fenced(&target(&store, &id)).unwrap();
    let candidate = initial("missing-admission", 1, vec![]);
    assert_eq!(
        store.admit_fenced(
            &id,
            &old.generation,
            old.resource_revision,
            candidate.clone()
        ),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        store.admit(&id, candidate.clone()),
        Err(StoreError::ThreadNotFound)
    );
    assert!(store.execution(&candidate.execution_id).unwrap().is_none());
    assert!(store.journal(&candidate.execution_id).unwrap().is_empty());
    store.readiness().unwrap();
}

#[test]
fn deletion_compares_every_captured_field_before_safety_checks() {
    let store = MemoryStore::new();
    let current = admitted(&store, "exact", vec![vec![task("a", false)]]);
    let id = current.thread_id.clone();
    let captured = target(&store, &id);
    let before = view(&store, &id);
    let journal = store.journal(&current.snapshot.execution_id).unwrap();
    let mut variants = vec![];
    let mut changed = captured.clone();
    changed.thread_id = thread("missing");
    variants.push(changed);
    let mut changed = captured.clone();
    changed.thread_generation = ThreadGeneration::new("0".repeat(64)).unwrap();
    variants.push(changed);
    let mut changed = captured.clone();
    changed.resource_revision += 1;
    variants.push(changed);
    let mut changed = captured.clone();
    changed.active_execution_id = None;
    variants.push(changed);
    let mut changed = captured.clone();
    changed.active_execution_id = Some(ExecutionId::new("other").unwrap());
    variants.push(changed);
    let mut changed = captured.clone();
    changed.execution_fence = None;
    variants.push(changed);
    let mut changed = captured.clone();
    changed.execution_fence.as_mut().unwrap().revision += 1;
    variants.push(changed);
    let mut changed = captured;
    changed.execution_fence.as_mut().unwrap().owner_generation += 1;
    variants.push(changed);
    for changed in variants {
        assert_eq!(
            store.delete_thread_fenced(&changed),
            Err(StoreError::RevisionConflict)
        );
        assert_eq!(view(&store, &id), before);
        assert_eq!(
            store.journal(&current.snapshot.execution_id).unwrap(),
            journal
        );
    }
    store.readiness().unwrap();
}

#[test]
fn target_capture_rejects_partial_or_cross_thread_active_relationships() {
    let store = MemoryStore::new();
    let current = admitted(&store, "coherent", vec![]);
    let coherent = view(&store, &current.thread_id);
    for kind in 0..4 {
        let mut broken = coherent.clone();
        match kind {
            0 => broken.execution = None,
            1 => broken.thread.active_execution_id = None,
            2 => broken.execution.as_mut().unwrap().thread_id = thread("foreign"),
            _ => {
                broken.execution.as_mut().unwrap().snapshot.execution_id =
                    ExecutionId::new("foreign").unwrap()
            }
        }
        assert!(matches!(
            ThreadDeletionTarget::from_view(&broken),
            Err(StoreError::Corrupt(_))
        ));
    }
}

#[test]
fn accepted_running_cancel_draining_and_recovery_blocked_are_busy() {
    for status in [
        ExecutionStatus::Accepted,
        ExecutionStatus::Running,
        ExecutionStatus::CancelRequested,
        ExecutionStatus::RecoveryBlocked,
    ] {
        let store = MemoryStore::new();
        let mut current = admitted(&store, "busy", vec![vec![task("a", false)]]);
        if status != ExecutionStatus::Accepted {
            current = commit(&store, &current, KernelCommand::Start);
        }
        if status == ExecutionStatus::CancelRequested {
            current = commit(
                &store,
                &current,
                KernelCommand::Dispatch {
                    task_id: TaskId::new("a").unwrap(),
                    attempt_id: AttemptId::new("attempt").unwrap(),
                },
            );
            current = commit(
                &store,
                &current,
                KernelCommand::Cancel {
                    turn_id: current.snapshot.turn_id.clone(),
                    owner_generation: 1,
                },
            );
        } else if status == ExecutionStatus::RecoveryBlocked {
            let mut blocked = noop(&current.snapshot);
            blocked.snapshot.revision += 1;
            blocked.snapshot.status = status;
            blocked.snapshot.failure = Some("execution_revision_unavailable".into());
            current = store
                .commit(
                    &current.snapshot.execution_id,
                    (&current.snapshot).into(),
                    blocked,
                )
                .unwrap();
        }
        assert_eq!(current.snapshot.status, status);
        let before = view(&store, &current.thread_id);
        let journal = store.journal(&current.snapshot.execution_id).unwrap();
        assert_eq!(
            store.delete_thread_fenced(&target(&store, &current.thread_id)),
            Err(StoreError::ThreadBusy)
        );
        assert_eq!(view(&store, &current.thread_id), before);
        assert_eq!(
            store.journal(&current.snapshot.execution_id).unwrap(),
            journal
        );
        store.readiness().unwrap();
    }
}

#[test]
fn settled_failed_and_cancelled_executions_can_be_deleted() {
    for status in [ExecutionStatus::Failed, ExecutionStatus::Cancelled] {
        let store = MemoryStore::new();
        let accepted = admitted(&store, "settled", vec![vec![task("a", false)]]);
        let mut current = commit(&store, &accepted, KernelCommand::Start);
        if status == ExecutionStatus::Failed {
            current = commit(
                &store,
                &current,
                KernelCommand::Dispatch {
                    task_id: TaskId::new("a").unwrap(),
                    attempt_id: AttemptId::new("attempt").unwrap(),
                },
            );
            current = commit(
                &store,
                &current,
                KernelCommand::FailAttempt {
                    task_id: TaskId::new("a").unwrap(),
                    attempt_id: AttemptId::new("attempt").unwrap(),
                    failure: "application failure".into(),
                },
            );
        } else {
            current = commit(
                &store,
                &current,
                KernelCommand::Cancel {
                    turn_id: current.snapshot.turn_id.clone(),
                    owner_generation: 1,
                },
            );
        }
        assert_eq!(current.snapshot.status, status);
        assert_eq!(
            store.delete_thread_fenced(&target(&store, &current.thread_id)),
            Ok(())
        );
        assert_eq!(
            store.execution(&current.snapshot.execution_id).unwrap(),
            None
        );
        assert!(store
            .journal(&current.snapshot.execution_id)
            .unwrap()
            .is_empty());
        store.readiness().unwrap();
    }
}

#[test]
fn uncertain_prior_owner_effects_are_busy_and_cannot_be_relabelled_as_a_park() {
    let store = MemoryStore::new();
    let mut unsafe_task = task("a", false);
    unsafe_task.recovery = RecoveryPolicy::Unsafe;
    let accepted = admitted(&store, "uncertain", vec![vec![unsafe_task]]);
    let running = commit(&store, &accepted, KernelCommand::Start);
    let inflight = commit(
        &store,
        &running,
        KernelCommand::Dispatch {
            task_id: TaskId::new("a").unwrap(),
            attempt_id: AttemptId::new("attempt").unwrap(),
        },
    );
    let recovered = plan(
        &inflight.snapshot,
        KernelCommand::Recover {
            available_revision: inflight.snapshot.definition.revision.clone(),
            turn_id: TurnId::new("recovery-turn").unwrap(),
            owner_generation: 2,
        },
    )
    .snapshot;
    assert_eq!(recovered.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(recovered.tasks[0].status, TaskStatus::OutcomeUnknown);
    let execution = StoredExecution {
        snapshot: recovered,
        ..inflight
    };
    let current_view = ThreadView {
        thread: store.thread(&execution.thread_id).unwrap().unwrap(),
        execution: Some(execution.clone()),
    };
    assert_eq!(
        validate_thread_deletion(
            &current_view,
            &ThreadDeletionTarget::from_view(&current_view).unwrap(),
            std::slice::from_ref(&execution)
        ),
        Err(StoreError::ThreadBusy)
    );
    let mut corrupted = execution;
    corrupted.snapshot.status = ExecutionStatus::WaitingForApproval;
    corrupted.snapshot.failure = None;
    let corrupted_view = ThreadView {
        execution: Some(corrupted.clone()),
        ..current_view
    };
    assert!(matches!(
        validate_thread_deletion(
            &corrupted_view,
            &ThreadDeletionTarget::from_view(&corrupted_view).unwrap(),
            &[corrupted]
        ),
        Err(StoreError::Corrupt(_))
    ));
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
fn parked(
    store: &Arc<MemoryStore>,
    thread_id: &ThreadId,
    execution: &str,
    create: bool,
) -> ApprovalView {
    if create {
        store
            .create_thread_with(ThreadSeed {
                id: thread_id.clone(),
                created_at_ms: 1,
                metadata: Map::new(),
                origin: Some(context().actor),
            })
            .unwrap();
    }
    let current = store
        .admit(
            thread_id,
            initial(execution, 1, vec![vec![task("a", true)]]),
        )
        .unwrap();
    let current = commit(store, &current, KernelCommand::Start);
    let service =
        ApprovalService::with_config(store.clone(), Allow, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    service
        .park(
            &context(),
            &current.snapshot.execution_id,
            ApprovalFence {
                execution: (&current.snapshot).into(),
                resource_revision: store.thread(thread_id).unwrap().unwrap().resource_revision,
            },
            TaskId::new("a").unwrap(),
            InterruptId::new(format!("interrupt-{execution}")).unwrap(),
            json!({"question":"Proceed?"}),
            1000,
            TransitionFacts { now_ms: 102 },
        )
        .unwrap();
    store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap()
}

#[test]
fn deletion_removes_history_grants_and_acceptance_and_preserves_other_threads() {
    let store = Arc::new(MemoryStore::new());
    let id = thread("history");
    let old = parked(&store, &id, "old", true);
    let service =
        ApprovalService::with_config(store.clone(), Allow, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    let completed = service
        .accept(
            &old.execution.snapshot.execution_id,
            ApprovalFence {
                execution: (&old.execution.snapshot).into(),
                resource_revision: old.thread.resource_revision,
            },
            ApprovalAcceptanceRequest {
                context: context(),
                route: old.execution.snapshot.definition.route.clone(),
                definition_revision: old.execution.snapshot.definition.revision.clone(),
                turn_id: TurnId::new("accept-turn").unwrap(),
                now_ms: 103,
                decisions: old
                    .grants
                    .iter()
                    .map(|grant| GrantDecision {
                        interrupt_id: grant.interrupt_id.clone(),
                        decision: ApprovalDecision::Deny,
                        cancelled: false,
                        token: Some(grant.token.clone()),
                    })
                    .collect(),
            },
        )
        .unwrap();
    assert_eq!(completed.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(
        store
            .approval_view(&completed.snapshot.execution_id)
            .unwrap()
            .unwrap()
            .acceptances
            .len(),
        1
    );
    let active = parked(&store, &id, "active", false);
    let foreign_id = thread("preserved");
    let foreign = parked(&store, &foreign_id, "foreign", true);
    let foreign_journal = store
        .journal(&foreign.execution.snapshot.execution_id)
        .unwrap();
    assert_eq!(store.delete_thread_fenced(&target(&store, &id)), Ok(()));
    assert_eq!(store.thread(&id).unwrap(), None);
    for execution in [
        completed.snapshot.execution_id,
        active.execution.snapshot.execution_id,
    ] {
        assert_eq!(store.execution(&execution).unwrap(), None);
        assert_eq!(store.approval_view(&execution).unwrap(), None);
        assert!(store.journal(&execution).unwrap().is_empty());
    }
    assert_eq!(
        store
            .approval_view(&foreign.execution.snapshot.execution_id)
            .unwrap(),
        Some(foreign.clone())
    );
    assert_eq!(
        store
            .journal(&foreign.execution.snapshot.execution_id)
            .unwrap(),
        foreign_journal
    );
    store.readiness().unwrap();
}

#[test]
fn admission_or_commit_after_capture_never_retargets_deletion() {
    let store = MemoryStore::new();
    let current = admitted(&store, "changed", vec![]);
    let captured = target(&store, &current.thread_id);
    let completed = commit(&store, &current, KernelCommand::Start);
    assert_eq!(
        store.delete_thread_fenced(&captured),
        Err(StoreError::RevisionConflict)
    );
    let captured = target(&store, &current.thread_id);
    let successor = store
        .admit(&current.thread_id, initial("successor", 1, vec![]))
        .unwrap();
    assert_eq!(
        store.delete_thread_fenced(&captured),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        store.execution(&completed.snapshot.execution_id).unwrap(),
        Some(completed)
    );
    assert_eq!(
        store.execution(&successor.snapshot.execution_id).unwrap(),
        Some(successor)
    );
}

#[test]
fn shared_validator_checks_every_historical_execution_and_exact_active_membership() {
    let store = MemoryStore::new();
    let active = admitted(&store, "history-validation", vec![]);
    let active = commit(&store, &active, KernelCommand::Start);
    let current_view = view(&store, &active.thread_id);
    let captured = ThreadDeletionTarget::from_view(&current_view).unwrap();
    assert_eq!(
        validate_thread_deletion(&current_view, &captured, std::slice::from_ref(&active)),
        Ok(())
    );
    assert!(matches!(
        validate_thread_deletion(&current_view, &captured, &[]),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        validate_thread_deletion(&current_view, &captured, &[active.clone(), active.clone()]),
        Err(StoreError::Corrupt(_))
    ));
    let mut foreign = active.clone();
    foreign.thread_id = thread("foreign");
    assert!(matches!(
        validate_thread_deletion(&current_view, &captured, &[foreign]),
        Err(StoreError::Corrupt(_))
    ));
    let unfinished = StoredExecution {
        thread_id: active.thread_id.clone(),
        snapshot: initial("unfinished-history", 1, vec![vec![task("a", false)]]),
    };
    assert!(matches!(
        validate_thread_deletion(
            &current_view,
            &captured,
            &[active.clone(), unfinished.clone()]
        ),
        Err(StoreError::Corrupt(_))
    ));
    let idle_view = ThreadView {
        thread: ThreadRecord {
            active_execution_id: None,
            ..current_view.thread.clone()
        },
        execution: None,
    };
    let idle_target = ThreadDeletionTarget::from_view(&idle_view).unwrap();
    assert!(matches!(
        validate_thread_deletion(&idle_view, &idle_target, &[unfinished]),
        Err(StoreError::Corrupt(_))
    ));
    let mut corrupt = active.clone();
    corrupt.snapshot.turn_history.clear();
    let corrupt_view = ThreadView {
        execution: Some(corrupt.clone()),
        ..current_view
    };
    assert!(matches!(
        validate_thread_deletion(&corrupt_view, &captured, &[corrupt]),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn shared_validator_allows_prior_owner_stable_park_but_not_historical_park() {
    let store = Arc::new(MemoryStore::new());
    let parked = parked(&store, &thread("prior-park"), "prior-park", true);
    let mut prior_execution = parked.execution;
    prior_execution.snapshot.owner_generation = 7;
    let current_view = ThreadView {
        thread: parked.thread,
        execution: Some(prior_execution.clone()),
    };
    let captured = ThreadDeletionTarget::from_view(&current_view).unwrap();
    assert_eq!(
        validate_thread_deletion(
            &current_view,
            &captured,
            std::slice::from_ref(&prior_execution)
        ),
        Ok(())
    );
    let idle_view = ThreadView {
        thread: ThreadRecord {
            active_execution_id: None,
            ..current_view.thread
        },
        execution: None,
    };
    assert!(matches!(
        validate_thread_deletion(
            &idle_view,
            &ThreadDeletionTarget::from_view(&idle_view).unwrap(),
            &[prior_execution]
        ),
        Err(StoreError::Corrupt(_))
    ));
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
    fn admit(&self, id: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.0.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.0.admit_fenced(id, generation, revision, initial)
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
fn default_capability_is_explicit_and_arc_and_reference_forward_deletion() {
    let legacy = Arc::new(LegacyStore(MemoryStore::new()));
    let id = thread("legacy");
    legacy.create_thread(id.clone()).unwrap();
    let captured =
        ThreadDeletionTarget::from_view(&legacy.thread_view(&id).unwrap().unwrap()).unwrap();
    assert_eq!(
        legacy.delete_thread_fenced(&captured),
        Err(StoreError::UnsupportedOperation)
    );
    assert_eq!(
        Store::delete_thread_fenced(&&*legacy, &captured),
        Err(StoreError::UnsupportedOperation)
    );
    assert!(legacy.thread(&id).unwrap().is_some());
    let supported = Arc::new(MemoryStore::new());
    supported.create_thread(id.clone()).unwrap();
    assert_eq!(
        supported.delete_thread_fenced(&target(&supported, &id)),
        Ok(())
    );
    supported.create_thread(id.clone()).unwrap();
    assert_eq!(
        Store::delete_thread_fenced(&&*supported, &target(&supported, &id)),
        Ok(())
    );
}
