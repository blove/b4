use super::Gate;
use b4_contract::*;
use b4_runtime::{approval::*, *};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone, Copy, Default)]
pub enum RecoveryBehavior {
    #[default]
    Delegate,
    CommitThenError,
    CommitThenPanic,
    FailViewAfterReceipt,
    ForeignThreadAfterReceipt,
    MissingActiveAfterReceipt,
}
pub struct BoundaryStore<S> {
    pub inner: Arc<S>,
    pub probes: AtomicUsize,
    pub recoveries: AtomicUsize,
    pub approval_reads: AtomicUsize,
    pub before_recovery: Option<Arc<Gate>>,
    pub after_commit: Option<Arc<Gate>>,
    pub behavior: RecoveryBehavior,
    pub recovered: AtomicBool,
    pub discover_refused: bool,
    pub recovery_arguments: Mutex<Vec<(ExecutionId, ApprovalFence, CommitPlan, ApprovalMutation)>>,
}
impl<S> BoundaryStore<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            probes: AtomicUsize::new(0),
            recoveries: AtomicUsize::new(0),
            approval_reads: AtomicUsize::new(0),
            before_recovery: None,
            after_commit: None,
            behavior: RecoveryBehavior::Delegate,
            recovered: AtomicBool::new(false),
            discover_refused: false,
            recovery_arguments: Mutex::new(Vec::new()),
        }
    }
    fn probe(&self) {
        self.probes.fetch_add(1, Ordering::AcqRel);
    }
    fn hold(gate: &Option<Arc<Gate>>) -> Result<(), StoreError> {
        if gate.as_ref().is_some_and(|g| !g.hold()) {
            Err(StoreError::Unavailable(
                "fixture-boundary-expired-or-cleanup".into(),
            ))
        } else {
            Ok(())
        }
    }
}
impl<S: Store> Store for BoundaryStore<S> {
    fn capabilities(&self) -> StoreCapabilities {
        self.probe();
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.probe();
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.probe();
        self.inner.create_thread(id)
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.probe();
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.probe();
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.probe();
        if self.recovered.load(Ordering::Acquire)
            && matches!(self.behavior, RecoveryBehavior::FailViewAfterReceipt)
        {
            return Err(StoreError::Unavailable(
                "private-after-confirmed-receipt".into(),
            ));
        }
        let mut view = self.inner.thread_view(id)?;
        if self.recovered.load(Ordering::Acquire) {
            if let Some(view) = view.as_mut() {
                match self.behavior {
                    RecoveryBehavior::ForeignThreadAfterReceipt => {
                        view.thread.id = ThreadId::new("foreign-returned-thread").unwrap()
                    }
                    RecoveryBehavior::MissingActiveAfterReceipt => {
                        view.thread.active_execution_id = None
                    }
                    _ => {}
                }
            }
        }
        Ok(view)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.probe();
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.probe();
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.probe();
        self.inner.admit_fenced(id, generation, revision, snapshot)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.probe();
        self.inner.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.probe();
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.probe();
        self.inner.journal(id)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.probe();
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.probe();
        self.inner.cancel_run_fenced(target, facts)
    }
    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.probe();
        if self.discover_refused {
            Err(DiscoveryError::UnsupportedOperation)
        } else {
            self.inner.discover_unfinished(cursor)
        }
    }
}
impl<S: ApprovalStore> ApprovalStore for BoundaryStore<S> {
    fn approval_durable(&self) -> bool {
        self.probe();
        self.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.probe();
        self.approval_reads.fetch_add(1, Ordering::AcqRel);
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.probe();
        if !matches!(&mutation, ApprovalMutation::Recover(_)) {
            return self.inner.commit_approval(id, fence, plan, mutation);
        }
        self.recoveries.fetch_add(1, Ordering::AcqRel);
        self.recovery_arguments.lock().unwrap().push((
            id.clone(),
            fence,
            plan.clone(),
            mutation.clone(),
        ));
        Self::hold(&self.before_recovery)?;
        let committed = self.inner.commit_approval(id, fence, plan, mutation)?;
        self.recovered.store(true, Ordering::Release);
        Self::hold(&self.after_commit)?;
        match self.behavior {
            RecoveryBehavior::CommitThenError => {
                Err(StoreError::Unavailable("private-committed-then-error".into()).into())
            }
            RecoveryBehavior::CommitThenPanic => panic!("private-committed-then-panic"),
            _ => Ok(committed),
        }
    }
}
