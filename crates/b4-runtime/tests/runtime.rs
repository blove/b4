use b4_contract::*;
use b4_kernel::new_execution;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct FaultStore {
    inner: MemoryStore,
    dispatch: AtomicBool,
    completion: AtomicBool,
    race: Option<std::sync::Barrier>,
    race_commits: AtomicUsize,
}
impl FaultStore {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            dispatch: AtomicBool::new(false),
            completion: AtomicBool::new(false),
            race: None,
            race_commits: AtomicUsize::new(0),
        }
    }
}
impl Store for FaultStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, s: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, s)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        s: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, s)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        f: ExecutionFence,
        p: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        if p.snapshot
            .tasks
            .iter()
            .any(|t| t.status == TaskStatus::Succeeded)
        {
            if let Some(barrier) = &self.race {
                if self.race_commits.fetch_add(1, Ordering::SeqCst) < 2 {
                    barrier.wait();
                }
            }
        }
        if (self.dispatch.load(Ordering::SeqCst)
            && p.snapshot
                .tasks
                .iter()
                .any(|t| t.status == TaskStatus::InFlight))
            || (self.completion.load(Ordering::SeqCst)
                && p.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.status == TaskStatus::Succeeded))
        {
            return Err(StoreError::Unavailable(
                "injected transaction failure".into(),
            ));
        }
        self.inner.commit(id, f, p)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}
fn id() -> ExecutionId {
    ExecutionId::new("execution").unwrap()
}
fn task(id: &str) -> TaskId {
    TaskId::new(id).unwrap()
}
fn attempt(id: &str) -> AttemptId {
    AttemptId::new(id).unwrap()
}
fn facts() -> TransitionFacts {
    TransitionFacts { now_ms: 100 }
}
fn initial(owner: u64, specs: Vec<TaskSpec>) -> Snapshot {
    new_execution(
        id(),
        TurnId::new("turn-1").unwrap(),
        owner,
        Definition {
            route: RouteKey::new("/test#agent").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![specs],
            max_attempts: 8,
        },
        Map::new(),
        facts(),
    )
    .unwrap()
}
fn spec(id: &str, approval: bool) -> TaskSpec {
    TaskSpec {
        id: task(id),
        effect_id: EffectId::new(format!("effect-{id}")).unwrap(),
        requires_approval: approval,
        recovery: RecoveryPolicy::Unsafe,
    }
}
fn result(id: &str) -> TaskResult {
    TaskResult {
        output: json!(id),
        writes: vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!(id)],
        }],
    }
}
fn start<S: Store>(store: S, specs: Vec<TaskSpec>) -> (Runtime<S>, StoredExecution) {
    let rt = Runtime::new(store);
    let t = ThreadId::new("thread").unwrap();
    rt.store().create_thread(t.clone()).unwrap();
    let accepted = rt
        .admit(&t, initial(rt.store().owner_generation(), specs))
        .unwrap();
    let running = rt
        .apply(
            &id(),
            ExecutionFence::from(&accepted.snapshot),
            KernelCommand::Start,
            facts(),
        )
        .unwrap();
    (rt, running)
}

#[test]
fn effect_observes_committed_attempt_before_invocation() {
    let (rt, running) = start(MemoryStore::new(), vec![spec("a", false)]);
    let calls = AtomicUsize::new(0);
    let done = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |call| {
                calls.fetch_add(1, Ordering::SeqCst);
                let stored = rt.store().execution(&id()).unwrap().unwrap();
                assert_eq!(stored.snapshot.tasks[0].status, TaskStatus::InFlight);
                assert_eq!(
                    stored.snapshot.tasks[0].attempt_id,
                    Some(call.attempt_id.clone())
                );
                assert_eq!(call.effect_id.as_str(), "effect-a");
                assert_eq!(
                    rt.store().journal(&id()).unwrap().last().unwrap().revision,
                    stored.snapshot.revision
                );
                Ok(result("a"))
            },
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(done.snapshot.values.get("items"), Some(&json!(["a"])));
}

#[test]
fn failed_attempt_commit_invokes_nothing() {
    let (rt, running) = start(FaultStore::new(), vec![spec("a", false)]);
    rt.store().dispatch.store(true, Ordering::SeqCst);
    let calls = AtomicUsize::new(0);
    let error = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(result("a"))
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::Store(StoreError::Unavailable(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(rt.store().execution(&id()).unwrap().unwrap(), running);
}

#[test]
fn failed_result_commit_retains_result_without_reinvoking_effect() {
    let (rt, running) = start(FaultStore::new(), vec![spec("a", false)]);
    rt.store().completion.store(true, Ordering::SeqCst);
    let calls = AtomicUsize::new(0);
    let error = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(result("a"))
            },
        )
        .unwrap_err();
    let RuntimeError::CompletionNotCommitted {
        result: retained, ..
    } = error
    else {
        panic!("result not retained")
    };
    assert_eq!(retained, result("a"));
    assert!(matches!(
        rt.complete(&id(), 1, task("a"), attempt("try-a"), &retained, facts()),
        Err(RuntimeError::Store(StoreError::Unavailable(_)))
    ));
    assert_eq!(retained, result("a"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let stored = rt.store().execution(&id()).unwrap().unwrap();
    assert_eq!(stored.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(stored.snapshot.output.is_none());
    rt.store().completion.store(false, Ordering::SeqCst);
    let done = rt
        .complete(&id(), 1, task("a"), attempt("try-a"), &retained, facts())
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn callback_failure_preserves_uncertain_in_flight_attempt() {
    let (rt, running) = start(MemoryStore::new(), vec![spec("a", false)]);
    assert_eq!(
        rt.dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |_| Err("lost response".into())
        )
        .unwrap_err(),
        RuntimeError::EffectFailed("lost response".into())
    );
    let s = rt.store().execution(&id()).unwrap().unwrap().snapshot;
    assert_eq!(s.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(s.attempts_used, 1);
}

#[test]
fn stale_fence_rejects_dispatch_without_calling_effect() {
    let (rt, running) = start(MemoryStore::new(), vec![spec("a", false)]);
    let fence = ExecutionFence {
        revision: running.snapshot.revision - 1,
        owner_generation: 1,
    };
    let error = rt
        .dispatch(
            &id(),
            fence,
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |_| panic!("stale dispatch invoked"),
        )
        .unwrap_err();
    assert_eq!(error, RuntimeError::Store(StoreError::RevisionConflict));
}

#[test]
fn sibling_completion_can_commit_during_effect_without_replaying_either() {
    let (rt, running) = start(MemoryStore::new(), vec![spec("a", false), spec("b", false)]);
    let done = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |_| {
                let current = rt.store().execution(&id()).unwrap().unwrap();
                let partial = rt
                    .dispatch(
                        &id(),
                        ExecutionFence::from(&current.snapshot),
                        task("b"),
                        attempt("try-b"),
                        facts(),
                        facts,
                        |_| Ok(result("b")),
                    )
                    .unwrap();
                assert_eq!(partial.snapshot.tasks[1].status, TaskStatus::Succeeded);
                Ok(result("a"))
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.values.get("items"), Some(&json!(["a", "b"])));
    assert_eq!(done.snapshot.attempts_used, 2);
}

#[test]
fn approval_resume_retains_successful_sibling_and_attempt_budget() {
    let (rt, running) = start(MemoryStore::new(), vec![spec("a", false), spec("b", true)]);
    let partial = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |_| Ok(result("a")),
        )
        .unwrap();
    let parked = rt
        .apply(
            &id(),
            ExecutionFence::from(&partial.snapshot),
            KernelCommand::Park {
                task_id: task("b"),
                interrupt_id: InterruptId::new("permission-b").unwrap(),
                prompt: json!({"tool":"b"}),
            },
            facts(),
        )
        .unwrap();
    assert_eq!(parked.snapshot.status, ExecutionStatus::WaitingForApproval);
    let resumed = rt
        .apply(
            &id(),
            ExecutionFence::from(&parked.snapshot),
            KernelCommand::Resume {
                route: parked.snapshot.definition.route.clone(),
                turn_id: TurnId::new("turn-2").unwrap(),
                decisions: vec![Decision {
                    interrupt_id: InterruptId::new("permission-b").unwrap(),
                    decision: ApprovalDecision::Once,
                }],
            },
            facts(),
        )
        .unwrap();
    assert_eq!(resumed.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(resumed.snapshot.attempts_used, 1);
    let done = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&resumed.snapshot),
            task("b"),
            attempt("try-b"),
            facts(),
            facts,
            |_| Ok(result("b")),
        )
        .unwrap();
    assert_eq!(done.snapshot.values.get("items"), Some(&json!(["a", "b"])));
}

#[test]
fn cancellation_during_effect_keeps_slot_and_discards_late_writes() {
    let (rt, running) = start(MemoryStore::new(), vec![spec("a", false)]);
    let done = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            facts,
            |call| {
                let current = rt.store().execution(&id()).unwrap().unwrap();
                let cancelled = rt
                    .apply(
                        &id(),
                        ExecutionFence::from(&current.snapshot),
                        KernelCommand::Cancel {
                            turn_id: call.turn_id.clone(),
                            owner_generation: call.owner_generation,
                        },
                        facts(),
                    )
                    .unwrap();
                assert_eq!(cancelled.snapshot.status, ExecutionStatus::CancelRequested);
                let successor = initial(1, vec![spec("a", false)]);
                let mut successor = successor;
                successor.execution_id = ExecutionId::new("successor").unwrap();
                assert_eq!(
                    rt.admit(&ThreadId::new("thread").unwrap(), successor)
                        .unwrap_err(),
                    RuntimeError::Store(StoreError::ThreadBusy)
                );
                Ok(result("a"))
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Cancelled);
    assert!(done.snapshot.values.is_empty());
    assert!(done.snapshot.output.is_none());
    assert_eq!(done.snapshot.tasks[0].status, TaskStatus::Succeeded);
}

#[test]
fn completion_uses_fresh_host_facts_after_sibling_progress() {
    let (rt, running) = start(MemoryStore::new(), vec![spec("a", false), spec("b", false)]);
    let done = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task("a"),
            attempt("try-a"),
            facts(),
            || TransitionFacts { now_ms: 102 },
            |_| {
                let current = rt.store().execution(&id()).unwrap().unwrap();
                rt.dispatch(
                    &id(),
                    ExecutionFence::from(&current.snapshot),
                    task("b"),
                    attempt("try-b"),
                    TransitionFacts { now_ms: 101 },
                    || TransitionFacts { now_ms: 101 },
                    |_| Ok(result("b")),
                )
                .unwrap();
                Ok(result("a"))
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.updated_at_ms, 102);
    assert_eq!(done.snapshot.values.get("items"), Some(&json!(["a", "b"])));
    assert_eq!(done.snapshot.attempts_used, 2);
}

#[test]
fn concurrent_completion_conflict_retains_result_for_retry_without_effect_replay() {
    let mut store = FaultStore::new();
    store.race = Some(std::sync::Barrier::new(2));
    let (rt, running) = start(store, vec![spec("a", false), spec("b", false)]);
    let effects = std::sync::Barrier::new(2);
    let calls = AtomicUsize::new(0);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let results = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            rt.dispatch(
                &id(),
                ExecutionFence::from(&running.snapshot),
                task("a"),
                attempt("try-a"),
                facts(),
                facts,
                |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    ready_tx.send(()).unwrap();
                    effects.wait();
                    Ok(result("a"))
                },
            )
        });
        ready_rx.recv().unwrap();
        let current = rt.store().execution(&id()).unwrap().unwrap();
        let second = rt.dispatch(
            &id(),
            ExecutionFence::from(&current.snapshot),
            task("b"),
            attempt("try-b"),
            facts(),
            facts,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                effects.wait();
                Ok(result("b"))
            },
        );
        [first.join().unwrap(), second]
    });
    let mut conflicts = 0;
    for (task_name, outcome) in ["a", "b"].into_iter().zip(results) {
        if let Err(RuntimeError::CompletionNotCommitted {
            result: retained,
            cause,
        }) = outcome
        {
            assert_eq!(*cause, RuntimeError::Store(StoreError::RevisionConflict));
            conflicts += 1;
            rt.complete(
                &id(),
                1,
                task(task_name),
                attempt(&format!("try-{task_name}")),
                &retained,
                facts(),
            )
            .unwrap();
        } else {
            assert!(outcome.is_ok());
        }
    }
    assert_eq!(conflicts, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let done = rt.store().execution(&id()).unwrap().unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(done.snapshot.values.get("items"), Some(&json!(["a", "b"])));
    assert_eq!(done.snapshot.attempts_used, 2);
}
