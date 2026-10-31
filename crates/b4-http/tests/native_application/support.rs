use b4_contract::*;
use b4_runtime::{approval::*, *};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
pub struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    pub fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, Duration::from_secs(8))
                .unwrap();
            state = next;
            assert!(!timeout.timed_out(), "native application gate not released");
        }
    }
    pub async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.state.lock().unwrap().0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
    pub fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}
pub struct Release(pub Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub struct CountStore {
    pub inner: MemoryStore,
    pub reads: AtomicUsize,
    pub sensitive: AtomicUsize,
    pub mutations: AtomicUsize,
}
impl CountStore {
    pub fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            reads: AtomicUsize::new(0),
            sensitive: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
        }
    }
    pub fn reset(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.sensitive.store(0, Ordering::SeqCst);
        self.mutations.store(0, Ordering::SeqCst);
    }
    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.reads.load(Ordering::SeqCst),
            self.sensitive.load(Ordering::SeqCst),
            self.mutations.load(Ordering::SeqCst),
        )
    }
}
impl Store for CountStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.admit_fenced(id, generation, revision, snapshot)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.cancel_run_fenced(target, facts)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_thread_fenced(target)
    }
}
impl ApprovalStore for CountStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.sensitive.fetch_add(1, Ordering::SeqCst);
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
