use b4_contract::*;
use b4_runtime::{approval::*, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

type ViewHook = dyn Fn(&ThreadId) -> Result<(), StoreError> + Send + Sync;

/// Instrument a real store without replacing its persistence or validation.
/// Only the explicitly armed ordinary view boundary can wait or return a fault.
pub struct ObservedStore<S> {
    pub inner: Arc<S>,
    pub views: AtomicUsize,
    pub probes: AtomicUsize,
    pub creates: Arc<AtomicUsize>,
    pub view_hook: Mutex<Option<Arc<ViewHook>>>,
}
impl<S> ObservedStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
            views: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
            creates: Arc::new(AtomicUsize::new(0)),
            view_hook: Mutex::new(None),
        }
    }
    pub fn counters(&self) -> (usize, usize, usize) {
        (
            self.views.load(Ordering::SeqCst),
            self.probes.load(Ordering::SeqCst),
            self.creates.load(Ordering::SeqCst),
        )
    }
}
impl<S: Store> Store for ObservedStore<S> {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.views.fetch_add(1, Ordering::SeqCst);
        let hook = self.view_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(id)?;
        }
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
    fn readiness(&self) -> Result<(), StoreError> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        self.inner.readiness()
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
}
impl<S: ApprovalStore> ApprovalStore for ObservedStore<S> {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
