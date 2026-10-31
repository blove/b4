use b4_contract::*;
use b4_runtime::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
#[path = "driver/support.rs"]
mod support;
use support::*;

struct UnreadableView {
    inner: MemoryStore,
    failed: AtomicBool,
}
impl Store for UnreadableView {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        if self.failed.load(Ordering::SeqCst) {
            Err(StoreError::Unavailable("prepared_view_unavailable".into()))
        } else {
            self.inner.thread_view(id)
        }
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, initial)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}

fn exercise(
    commit: bool,
) -> (
    Arc<UnreadableView>,
    StoredExecution,
    DriverFailure<RuntimeError>,
) {
    let store = Arc::new(UnreadableView {
        inner: MemoryStore::new(),
        failed: AtomicBool::new(false),
    });
    let initial = accepted(&store, "unreadable-prepared", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    let prepare_store = store.clone();
    host.prepare = Some(Box::new(move |current, task_id, attempt_id| {
        let prepared = if commit {
            Runtime::new(&prepare_store).prepare_dispatch(
                &current.snapshot.execution_id,
                (&current.snapshot).into(),
                task_id,
                attempt_id,
                TransitionFacts {
                    now_ms: current.snapshot.updated_at_ms + 1,
                },
            )?
        } else {
            let plan = b4_kernel::transition(
                &current.snapshot,
                KernelCommand::Dispatch {
                    task_id: task_id.clone(),
                    attempt_id: attempt_id.clone(),
                },
                TransitionFacts {
                    now_ms: current.snapshot.updated_at_ms + 1,
                },
            )
            .unwrap();
            let task = &plan.snapshot.tasks[0];
            PreparedEffect {
                invocation: EffectInvocation {
                    execution_id: plan.snapshot.execution_id.clone(),
                    turn_id: plan.snapshot.turn_id.clone(),
                    owner_generation: plan.snapshot.owner_generation,
                    task_id: TaskId::new("foreign-task").unwrap(),
                    attempt_id,
                    effect_id: task.spec.effect_id.clone(),
                    input: task.input.clone(),
                },
                execution: StoredExecution {
                    thread_id: current.thread_id.clone(),
                    snapshot: plan.snapshot,
                },
            }
        };
        prepare_store.failed.store(true, Ordering::SeqCst);
        Ok(prepared)
    }));
    let failure = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(1).unwrap(),
        DriverConfig::default(),
    )
    .unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(host.fault.load(Ordering::SeqCst) > 0);
    assert!(failure.retained_outcomes.is_empty());
    (store, initial, failure)
}

#[test]
fn malformed_success_cannot_report_foreign_claim_evidence_when_fresh_store_view_fails() {
    let (store, initial, failure) = exercise(false);
    let durable = store
        .inner
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.snapshot.attempts_used, 0);
    assert_eq!(durable.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(failure
        .errors
        .iter()
        .any(|error| matches!(error, DriverError::InvalidPreparedEffect)));
    assert!(
        failure.unsettled_invocations.is_empty(),
        "foreign successful return became ownership evidence before validation"
    );
}

#[test]
fn exact_durable_success_preserves_never_entered_claim_when_fresh_store_view_fails() {
    let (store, initial, failure) = exercise(true);
    let durable = store
        .inner
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.snapshot.attempts_used, 1);
    let task = &durable.snapshot.tasks[0];
    assert_eq!(task.status, TaskStatus::InFlight);
    assert!(task.result.is_none() && task.failure.is_none());
    assert!(failure.errors.iter().any(|error| matches!(
        error,
        DriverError::Runtime(RuntimeError::Store(StoreError::Unavailable(_)))
    )));
    assert_eq!(
        failure.unsettled_invocations,
        vec![EffectInvocation {
            execution_id: durable.snapshot.execution_id.clone(),
            turn_id: durable.snapshot.turn_id.clone(),
            owner_generation: durable.snapshot.owner_generation,
            task_id: task.spec.id.clone(),
            effect_id: task.spec.effect_id.clone(),
            attempt_id: task.attempt_id.clone().unwrap(),
            input: task.input.clone(),
        }]
    );
}
