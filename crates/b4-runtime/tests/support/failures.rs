//! Store qualifications shared by the memory and SQLite integration tests.
#![allow(dead_code)]

use b4_contract::*;
use b4_kernel::new_execution;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Barrier;

pub fn execution() -> ExecutionId {
    ExecutionId::new("failure-execution").unwrap()
}
pub fn thread() -> ThreadId {
    ThreadId::new("failure-thread").unwrap()
}
pub fn task(name: &str) -> TaskId {
    TaskId::new(name).unwrap()
}
pub fn attempt(name: &str) -> AttemptId {
    AttemptId::new(format!("attempt-{name}")).unwrap()
}
pub fn facts() -> TransitionFacts {
    TransitionFacts { now_ms: 100 }
}
pub fn spec(name: &str) -> TaskSpec {
    TaskSpec {
        id: task(name),
        effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    }
}
pub fn result(name: &str) -> TaskResult {
    TaskResult {
        output: json!(name),
        writes: vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!(name)],
        }],
    }
}
pub fn failed() -> EffectOutcome {
    EffectOutcome::Failed("application failure".into())
}
pub fn initial(name: &str, owner: u64, batches: Vec<Vec<TaskSpec>>) -> Snapshot {
    new_execution(
        ExecutionId::new(name).unwrap(),
        TurnId::new(format!("turn-{name}")).unwrap(),
        owner,
        Definition {
            route: RouteKey::new("/failure#agent").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches,
            max_attempts: 8,
        },
        Map::new(),
        facts(),
    )
    .unwrap()
}
pub fn start<S: Store>(store: S, batches: Vec<Vec<TaskSpec>>) -> (Runtime<S>, StoredExecution) {
    let runtime = Runtime::new(store);
    runtime.store().create_thread(thread()).unwrap();
    let accepted = runtime
        .admit(
            &thread(),
            initial(
                execution().as_str(),
                runtime.store().owner_generation(),
                batches,
            ),
        )
        .unwrap();
    let running = runtime
        .apply(
            &execution(),
            (&accepted.snapshot).into(),
            KernelCommand::Start,
            facts(),
        )
        .unwrap();
    (runtime, running)
}
fn successor<S: Store>(runtime: &Runtime<S>) -> Snapshot {
    initial("successor", runtime.store().owner_generation(), vec![])
}

pub fn known_failure<S: Store>(store: S) {
    let (runtime, running) = start(store, vec![vec![spec("a")]]);
    let calls = AtomicUsize::new(0);
    let done = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            facts,
            |invocation| {
                calls.fetch_add(1, Ordering::SeqCst);
                let persisted = runtime.store().execution(&execution()).unwrap().unwrap();
                assert_eq!(persisted.snapshot.tasks[0].status, TaskStatus::InFlight);
                assert_eq!(
                    persisted.snapshot.tasks[0].attempt_id.as_ref(),
                    Some(&invocation.attempt_id)
                );
                assert_eq!(
                    runtime
                        .store()
                        .journal(&execution())
                        .unwrap()
                        .last()
                        .unwrap()
                        .revision,
                    persisted.snapshot.revision
                );
                Ok(failed())
            },
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(
        done.snapshot.failure.as_deref(),
        Some("application failure")
    );
    assert_eq!(done.snapshot.tasks[0].status, TaskStatus::Failed);
    assert_eq!(
        done.snapshot.tasks[0].failure.as_deref(),
        Some("application failure")
    );
    assert!(done.snapshot.tasks[0].result.is_none());
    assert!(done.snapshot.output.is_none());
    assert_eq!(
        runtime.store().execution(&execution()).unwrap(),
        Some(done.clone())
    );
    let journal = runtime.store().journal(&execution()).unwrap();
    assert_eq!(journal.len(), 4);
    assert_eq!(journal.last().unwrap(), &JournalEntry::from(&done.snapshot));
    let duplicate = runtime
        .complete_outcome(
            &execution(),
            runtime.store().owner_generation(),
            task("a"),
            attempt("a"),
            &failed(),
            facts(),
        )
        .unwrap();
    assert_eq!(duplicate, done);
    assert_eq!(runtime.store().journal(&execution()).unwrap(), journal);
    runtime.store().readiness().unwrap();
    runtime.admit(&thread(), successor(&runtime)).unwrap();
    assert_eq!(
        runtime.complete_outcome(
            &execution(),
            runtime.store().owner_generation(),
            task("a"),
            attempt("a"),
            &failed(),
            facts(),
        ),
        Err(RuntimeError::Store(StoreError::ExecutionNotActive))
    );
}

pub fn success_outcome<S: Store>(store: S) {
    let (runtime, running) = start(store, vec![vec![spec("a")]]);
    let done = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            facts,
            |_| Ok(EffectOutcome::Succeeded(result("a"))),
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(done.snapshot.values.get("items"), Some(&json!(["a"])));
    assert_eq!(done.snapshot.output, Some(json!({"items":["a"]})));
    assert!(done.snapshot.failure.is_none());
    let journal = runtime.store().journal(&execution()).unwrap();
    let duplicate = runtime
        .complete_outcome(
            &execution(),
            runtime.store().owner_generation(),
            task("a"),
            attempt("a"),
            &EffectOutcome::Succeeded(result("a")),
            facts(),
        )
        .unwrap();
    assert_eq!(duplicate, done);
    assert!(matches!(
        runtime.complete_outcome(
            &execution(),
            runtime.store().owner_generation(),
            task("a"),
            attempt("a"),
            &failed(),
            facts(),
        ),
        Err(RuntimeError::Kernel(_))
    ));
    assert_eq!(runtime.store().execution(&execution()).unwrap(), Some(done));
    assert_eq!(runtime.store().journal(&execution()).unwrap(), journal);
}

pub fn uncertain_error<S: Store>(store: S) {
    let (runtime, running) = start(store, vec![vec![spec("a")]]);
    let error = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            || panic!("uncertain outcome must not complete"),
            |_| Err("response lost".into()),
        )
        .unwrap_err();
    assert_eq!(error, RuntimeError::EffectFailed("response lost".into()));
    let current = runtime.store().execution(&execution()).unwrap().unwrap();
    assert_eq!(current.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(current.snapshot.attempts_used, 1);
    assert!(current.snapshot.failure.is_none());
    assert_eq!(
        runtime
            .store()
            .journal(&execution())
            .unwrap()
            .last()
            .unwrap()
            .revision,
        current.snapshot.revision
    );
    assert_eq!(
        runtime.admit(&thread(), successor(&runtime)),
        Err(RuntimeError::Store(StoreError::ThreadBusy))
    );
}

struct FaultStore<S> {
    inner: S,
    claim: AtomicBool,
    completion: AtomicBool,
    race: Option<Barrier>,
    race_commits: AtomicUsize,
}
impl<S> FaultStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            claim: AtomicBool::new(false),
            completion: AtomicBool::new(false),
            race: None,
            race_commits: AtomicUsize::new(0),
        }
    }
}
impl<S: Store> Store for FaultStore<S> {
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
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, snapshot)
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
        let completing = plan
            .snapshot
            .tasks
            .iter()
            .any(|task| matches!(task.status, TaskStatus::Failed | TaskStatus::Succeeded));
        if completing {
            if let Some(barrier) = &self.race {
                if self.race_commits.fetch_add(1, Ordering::SeqCst) < 2 {
                    barrier.wait();
                }
            }
        }
        if (completing && self.completion.load(Ordering::SeqCst))
            || (!completing
                && self.claim.load(Ordering::SeqCst)
                && plan
                    .snapshot
                    .tasks
                    .iter()
                    .any(|task| task.status == TaskStatus::InFlight))
        {
            return Err(StoreError::Unavailable(
                "injected transaction failure".into(),
            ));
        }
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}

pub fn claim_failure<S: Store>(store: S) {
    let (runtime, running) = start(FaultStore::new(store), vec![vec![spec("a")]]);
    runtime.store().claim.store(true, Ordering::SeqCst);
    let error = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            facts,
            |_| panic!("rejected claim invoked effect"),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::Store(StoreError::Unavailable(_))
    ));
    assert_eq!(
        runtime.store().execution(&execution()).unwrap(),
        Some(running)
    );
}

pub fn completion_failure<S: Store>(store: S) {
    let (runtime, running) = start(FaultStore::new(store), vec![vec![spec("a")]]);
    runtime.store().completion.store(true, Ordering::SeqCst);
    let calls = AtomicUsize::new(0);
    let error = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            facts,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(failed())
            },
        )
        .unwrap_err();
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
    for _ in 0..2 {
        assert!(matches!(
            runtime.complete_outcome(
                &execution(),
                runtime.store().owner_generation(),
                task("a"),
                attempt("a"),
                &outcome,
                facts()
            ),
            Err(RuntimeError::Store(StoreError::Unavailable(_)))
        ));
        assert_eq!(outcome, failed());
        assert_eq!(
            runtime.store().execution(&execution()).unwrap(),
            Some(claimed.clone())
        );
        assert_eq!(runtime.store().journal(&execution()).unwrap(), journal);
    }
    runtime.store().completion.store(false, Ordering::SeqCst);
    let done = runtime
        .complete_outcome(
            &execution(),
            runtime.store().owner_generation(),
            task("a"),
            attempt("a"),
            &outcome,
            facts(),
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime.store().journal(&execution()).unwrap().len(),
        journal.len() + 1
    );
}

pub fn completion_fences<S: Store>(store: S) {
    let (runtime, running) = start(store, vec![vec![spec("a")]]);
    let claimed = runtime
        .apply(
            &execution(),
            (&running.snapshot).into(),
            KernelCommand::Dispatch {
                task_id: task("a"),
                attempt_id: attempt("a"),
            },
            facts(),
        )
        .unwrap();
    let owner = runtime.store().owner_generation();
    assert_eq!(
        runtime.complete_outcome(
            &execution(),
            owner + 1,
            task("a"),
            attempt("a"),
            &failed(),
            facts()
        ),
        Err(RuntimeError::Store(StoreError::OwnerConflict))
    );
    assert!(matches!(
        runtime.complete_outcome(
            &execution(),
            owner,
            task("a"),
            attempt("wrong"),
            &failed(),
            facts()
        ),
        Err(RuntimeError::Kernel(_))
    ));
    assert!(matches!(
        runtime.complete_outcome(
            &execution(),
            owner,
            task("missing"),
            attempt("a"),
            &failed(),
            facts()
        ),
        Err(RuntimeError::Kernel(_))
    ));
    for reason in [String::new(), "é".repeat(513)] {
        assert!(matches!(
            runtime.complete_outcome(
                &execution(),
                owner,
                task("a"),
                attempt("a"),
                &EffectOutcome::Failed(reason),
                facts()
            ),
            Err(RuntimeError::Kernel(_))
        ));
        assert_eq!(
            runtime.store().execution(&execution()).unwrap(),
            Some(claimed.clone())
        );
    }
    let outcome = EffectOutcome::Failed("é".repeat(512));
    let done = runtime
        .complete_outcome(
            &execution(),
            owner,
            task("a"),
            attempt("a"),
            &outcome,
            facts(),
        )
        .unwrap();
    for conflicting in [failed(), EffectOutcome::Succeeded(result("a"))] {
        assert!(matches!(
            runtime.complete_outcome(
                &execution(),
                owner,
                task("a"),
                attempt("a"),
                &conflicting,
                facts()
            ),
            Err(RuntimeError::Kernel(_))
        ));
        assert_eq!(
            runtime.store().execution(&execution()).unwrap(),
            Some(done.clone())
        );
    }
    for reason in [String::new(), "x".repeat(1025)] {
        let mut corrupted = done.snapshot.clone();
        corrupted.revision += 1;
        corrupted.failure = Some(reason.clone());
        corrupted.tasks[0].failure = Some(reason);
        assert!(matches!(
            runtime.store().commit(
                &execution(),
                (&done.snapshot).into(),
                CommitPlan {
                    expected_revision: done.snapshot.revision,
                    snapshot: corrupted,
                    dispatchable: vec![]
                }
            ),
            Err(StoreError::InvalidSnapshot(_))
        ));
        assert_eq!(
            runtime.store().execution(&execution()).unwrap(),
            Some(done.clone())
        );
    }
}

pub fn drain<S: Store>(store: S) {
    let (runtime, running) = start(
        store,
        vec![vec![spec("a"), spec("b"), spec("c")], vec![spec("later")]],
    );
    let draining = runtime
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
        .unwrap();
    assert_eq!(draining.snapshot.status, ExecutionStatus::Running);
    assert_eq!(
        draining.snapshot.failure.as_deref(),
        Some("application failure")
    );
    assert_eq!(draining.snapshot.tasks[1].status, TaskStatus::InFlight);
    assert_eq!(draining.snapshot.tasks[2].status, TaskStatus::Cancelled);
    assert_eq!(
        runtime.admit(&thread(), successor(&runtime)),
        Err(RuntimeError::Store(StoreError::ThreadBusy))
    );
    assert!(runtime
        .dispatch_outcome(
            &execution(),
            (&draining.snapshot).into(),
            task("c"),
            attempt("c"),
            facts(),
            facts,
            |_| panic!("failure drain scheduled sibling")
        )
        .is_err());
    let done = runtime
        .complete_outcome(
            &execution(),
            runtime.store().owner_generation(),
            task("b"),
            attempt("b"),
            &EffectOutcome::Succeeded(result("b")),
            facts(),
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert!(done.snapshot.values.is_empty());
    assert!(done.snapshot.output.is_none());
    assert_eq!(done.snapshot.tasks[1].result.as_ref(), Some(&result("b")));
    assert_eq!(done.snapshot.batch_index, 0);
    assert_eq!(done.snapshot.attempts_used, 2);
    runtime.admit(&thread(), successor(&runtime)).unwrap();
}

pub fn fresh_completion<S: Store>(store: S) {
    let (runtime, running) = start(store, vec![vec![spec("a"), spec("b")]]);
    let done = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            || TransitionFacts { now_ms: 102 },
            |_| {
                let current = runtime.store().execution(&execution()).unwrap().unwrap();
                runtime
                    .dispatch_outcome(
                        &execution(),
                        (&current.snapshot).into(),
                        task("b"),
                        attempt("b"),
                        TransitionFacts { now_ms: 101 },
                        || TransitionFacts { now_ms: 101 },
                        |_| Ok(EffectOutcome::Succeeded(result("b"))),
                    )
                    .unwrap();
                Ok(failed())
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(done.snapshot.updated_at_ms, 102);
    assert_eq!(done.snapshot.tasks[1].result.as_ref(), Some(&result("b")));
    assert!(done.snapshot.values.is_empty());
}

pub fn cancel_precedence<S: Store>(store: S) {
    let (runtime, running) = start(store, vec![vec![spec("a")]]);
    let done = runtime
        .dispatch_outcome(
            &execution(),
            (&running.snapshot).into(),
            task("a"),
            attempt("a"),
            facts(),
            facts,
            |call| {
                let current = runtime.store().execution(&execution()).unwrap().unwrap();
                let cancelling = runtime
                    .apply(
                        &execution(),
                        (&current.snapshot).into(),
                        KernelCommand::Cancel {
                            turn_id: call.turn_id.clone(),
                            owner_generation: call.owner_generation,
                        },
                        facts(),
                    )
                    .unwrap();
                assert_eq!(cancelling.snapshot.status, ExecutionStatus::CancelRequested);
                Ok(failed())
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(
        done.snapshot.failure.as_deref(),
        Some("application failure")
    );
    assert!(done.snapshot.output.is_none());
}

pub fn concurrent_conflict<S: Store>(store: S) {
    let mut store = FaultStore::new(store);
    store.race = Some(Barrier::new(2));
    let (runtime, running) = start(store, vec![vec![spec("a"), spec("b")]]);
    let effects = Barrier::new(2);
    let calls = AtomicUsize::new(0);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let outcomes = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            runtime.dispatch_outcome(
                &execution(),
                (&running.snapshot).into(),
                task("a"),
                attempt("a"),
                facts(),
                facts,
                |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    ready_tx.send(()).unwrap();
                    effects.wait();
                    Ok(failed())
                },
            )
        });
        ready_rx.recv().unwrap();
        let current = runtime.store().execution(&execution()).unwrap().unwrap();
        let second = runtime.dispatch_outcome(
            &execution(),
            (&current.snapshot).into(),
            task("b"),
            attempt("b"),
            facts(),
            facts,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                effects.wait();
                Ok(EffectOutcome::Succeeded(result("b")))
            },
        );
        [first.join().unwrap(), second]
    });
    let mut conflicts = 0;
    for (name, value) in ["a", "b"].into_iter().zip(outcomes) {
        match value {
            Err(RuntimeError::OutcomeNotCommitted { outcome, cause }) => {
                assert_eq!(*cause, RuntimeError::Store(StoreError::RevisionConflict));
                conflicts += 1;
                runtime
                    .complete_outcome(
                        &execution(),
                        runtime.store().owner_generation(),
                        task(name),
                        attempt(name),
                        &outcome,
                        facts(),
                    )
                    .unwrap();
            }
            Ok(_) => (),
            value => panic!("unexpected dispatch result {value:?}"),
        }
    }
    assert_eq!(conflicts, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let done = runtime.store().execution(&execution()).unwrap().unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(
        done.snapshot.failure.as_deref(),
        Some("application failure")
    );
    assert!(done.snapshot.values.is_empty());
    assert_eq!(done.snapshot.attempts_used, 2);
}
