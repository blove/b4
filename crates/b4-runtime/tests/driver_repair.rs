use b4_contract::*;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct RepairStore(MemoryStore);
impl Store for RepairStore {
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
    fn admit(&self, id: &ThreadId, s: Snapshot) -> Result<StoredExecution, StoreError> {
        self.0.admit(id, s)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        s: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.0.admit_fenced(id, generation, revision, s)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.0.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        f: ExecutionFence,
        p: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        if p.snapshot.status == ExecutionStatus::Cancelled {
            return Err(StoreError::Unavailable("ack_commit_failed".into()));
        }
        self.0.commit(id, f, p)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.0.journal(id)
    }
}
struct Host {
    store: Arc<RepairStore>,
    faults: AtomicUsize,
    callbacks: AtomicUsize,
}
impl DriverHost for Host {
    type Error = RuntimeError;
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        TransitionFacts {
            now_ms: current.snapshot.updated_at_ms + 1,
        }
    }
    fn attempt_id(&self) -> AttemptId {
        AttemptId::new("attempt").unwrap()
    }
    fn prepare_dispatch(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<RuntimeError>> {
        let prepared = Runtime::new(&self.store).prepare_dispatch(
            &current.snapshot.execution_id,
            (&current.snapshot).into(),
            task_id,
            attempt_id,
            self.facts(current),
        )?;
        let view = self.store.thread_view(&current.thread_id).unwrap().unwrap();
        self.store
            .0
            .cancel_run_fenced(
                &RunCancellationTarget::from_view(&view).unwrap(),
                self.facts(&prepared.execution),
            )
            .unwrap();
        Err(PreparationError {
            cause: RuntimeError::Store(StoreError::RevisionConflict),
            claimed: Some(Box::new(prepared)),
        })
    }
    fn park(&self, _: &StoredExecution, _: TaskId) -> Result<StoredExecution, RuntimeError> {
        unreachable!()
    }
    fn invoke(&self, _: &EffectInvocation) -> Result<EffectOutcome, String> {
        self.callbacks.fetch_add(1, Ordering::SeqCst);
        panic!("never entered claim invoked")
    }
    fn cancellation_committed(&self) -> bool {
        true
    }
    fn expected_cancel_race(&self, e: &RuntimeError) -> bool {
        matches!(e, RuntimeError::Store(StoreError::RevisionConflict))
    }
    fn host_error_is_fault(&self, e: &RuntimeError) -> bool {
        !matches!(e, RuntimeError::Store(StoreError::RevisionConflict))
    }
    fn on_fault(&self) {
        self.faults.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn failed_expected_cancel_repair_must_fault_and_retain_positive_ack() {
    let store = Arc::new(RepairStore(MemoryStore::new()));
    let thread = ThreadId::new("thread").unwrap();
    store.create_thread(thread.clone()).unwrap();
    let initial = store
        .admit(
            &thread,
            b4_kernel::new_execution(
                ExecutionId::new("execution").unwrap(),
                TurnId::new("turn").unwrap(),
                1,
                Definition {
                    route: RouteKey::new("/driver#agent").unwrap(),
                    revision: ExecutionRevision::new("v1").unwrap(),
                    max_attempts: 8,
                    batches: vec![vec![TaskSpec {
                        id: TaskId::new("a").unwrap(),
                        effect_id: EffectId::new("effect-a").unwrap(),
                        requires_approval: false,
                        recovery: RecoveryPolicy::Pure,
                    }]],
                },
                Map::from_iter([("input".into(), json!(true))]),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let host = Host {
        store: store.clone(),
        faults: AtomicUsize::new(0),
        callbacks: AtomicUsize::new(0),
    };
    let failure = drive_execution(
        &Runtime::new(&store),
        initial,
        &host,
        &TaskCapacity::new(1).unwrap(),
        DriverConfig::default(),
    )
    .unwrap_err();
    assert!(
        host.faults.load(Ordering::SeqCst) > 0,
        "positive cancellation repair commit failed without fault notification"
    );
    assert_eq!(host.callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(failure.unsettled_invocations.len(), 1);
    assert!(failure
        .retained_outcomes
        .iter()
        .any(|r| r.outcome == EffectOutcome::Cancelled));
    assert!(failure
        .errors
        .iter()
        .any(|e| matches!(e, DriverError::Runtime(_))));
    assert_eq!(
        store
            .execution(&ExecutionId::new("execution").unwrap())
            .unwrap()
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::CancelRequested
    );
}
