use b4_contract::*;
use b4_runtime::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone, Copy)]
pub(super) enum Fault {
    None,
    CasOnce,
    ConcurrentCancel,
    CasAlways,
    Unavailable,
    Corrupt,
    Generation,
    Turn,
    Owner,
    Task,
    Attempt,
}

pub(super) struct FaultStore {
    pub(super) inner: MemoryStore,
    pub(super) completion_commits: AtomicUsize,
    pub(super) ready_views: AtomicUsize,
    fault: Fault,
    changed: AtomicBool,
}
impl FaultStore {
    pub(super) fn new(fault: Fault) -> Self {
        Self {
            inner: MemoryStore::new(),
            completion_commits: AtomicUsize::new(0),
            ready_views: AtomicUsize::new(0),
            fault,
            changed: AtomicBool::new(false),
        }
    }
}
impl Store for FaultStore {
    fn owner_generation(&self) -> u64 {
        if self.changed.load(Ordering::SeqCst) && matches!(self.fault, Fault::Owner) {
            2
        } else {
            self.inner.owner_generation()
        }
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        let mut view = self.inner.thread_view(id)?;
        if let Some(view) = &mut view {
            if let Some(execution) = &mut view.execution {
                let snapshot = &mut execution.snapshot;
                if snapshot.status == ExecutionStatus::Running
                    && snapshot.attempts_used == 0
                    && snapshot
                        .tasks
                        .iter()
                        .all(|task| task.status == TaskStatus::Ready)
                {
                    self.ready_views.fetch_add(1, Ordering::SeqCst);
                }
                if self.changed.load(Ordering::SeqCst) {
                    match self.fault {
                        Fault::Generation => {
                            let value = if view.thread.generation.as_str().starts_with('0') {
                                "1".repeat(64)
                            } else {
                                "0".repeat(64)
                            };
                            view.thread.generation = ThreadGeneration::new(value).unwrap();
                        }
                        Fault::Turn => {
                            let turn = TurnId::new("replacement-turn").unwrap();
                            snapshot.turn_history.push(turn.clone());
                            snapshot.turn_id = turn;
                        }
                        Fault::Task => {
                            let task = TaskId::new("replacement-task").unwrap();
                            snapshot.definition.batches[0][0].id = task.clone();
                            snapshot.tasks[0].spec.id = task;
                        }
                        Fault::Attempt => {
                            let attempt = AttemptId::new("replacement-attempt").unwrap();
                            snapshot.attempt_history[0] = attempt.clone();
                            snapshot.tasks[0].attempt_id = Some(attempt);
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(view)
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
        if plan.snapshot.tasks.iter().any(|task| task.result.is_some()) {
            let count = self.completion_commits.fetch_add(1, Ordering::SeqCst);
            match self.fault {
                Fault::CasOnce if count == 0 => return Err(StoreError::RevisionConflict),
                Fault::ConcurrentCancel if count == 0 => {
                    let current = self
                        .inner
                        .execution(id)?
                        .ok_or(StoreError::ExecutionNotFound)?;
                    let view = self
                        .inner
                        .thread_view(&current.thread_id)?
                        .ok_or(StoreError::ThreadNotFound)?;
                    self.inner.cancel_run_fenced(
                        &RunCancellationTarget::from_view(&view)?,
                        TransitionFacts {
                            now_ms: plan.snapshot.updated_at_ms,
                        },
                    )?;
                    // Exercise the real Memory CAS against a separately committed
                    // cancellation, rather than manufacturing its conflict.
                    return self.inner.commit(id, fence, plan);
                }
                Fault::CasAlways => return Err(StoreError::RevisionConflict),
                Fault::Unavailable => {
                    return Err(StoreError::Unavailable("completion_unavailable".into()))
                }
                Fault::Corrupt => return Err(StoreError::Corrupt("completion_corrupt".into())),
                Fault::Generation | Fault::Turn | Fault::Owner | Fault::Task | Fault::Attempt => {
                    self.changed.store(true, Ordering::SeqCst);
                    return Err(StoreError::RevisionConflict);
                }
                _ => {}
            }
        }
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}
