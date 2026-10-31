use super::Gate;
use b4_contract::*;
use b4_runtime::{approval::*, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone, Copy)]
pub enum CancelBehavior {
    Delegate,
    Conflict,
    Unsupported,
    OwnerConflict,
    Fail,
    Panic,
}
pub struct BoundaryStore<S> {
    pub inner: Arc<S>,
    pub before_cancel: Option<Arc<Gate>>,
    pub after_cancel: Option<Arc<Gate>>,
    pub after_dispatch: Option<Arc<Gate>>,
    pub before_ack: Option<Arc<Gate>>,
    pub cancel_behavior: CancelBehavior,
    pub cancel_calls: AtomicUsize,
    pub ack_calls: AtomicUsize,
    pub completion_conflicts: AtomicUsize,
    pub cancellation_arguments: Mutex<Vec<(RunCancellationTarget, TransitionFacts)>>,
    pub acknowledgement_arguments: Mutex<Vec<(ExecutionId, ExecutionFence, CommitPlan)>>,
}
impl<S> BoundaryStore<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            before_cancel: None,
            after_cancel: None,
            after_dispatch: None,
            before_ack: None,
            cancel_behavior: CancelBehavior::Delegate,
            cancel_calls: AtomicUsize::new(0),
            ack_calls: AtomicUsize::new(0),
            completion_conflicts: AtomicUsize::new(0),
            cancellation_arguments: Mutex::new(Vec::new()),
            acknowledgement_arguments: Mutex::new(Vec::new()),
        }
    }
    fn hold(gate: &Option<Arc<Gate>>) -> Result<(), StoreError> {
        if gate.as_ref().is_some_and(|g| !g.hold()) {
            Err(StoreError::Unavailable(
                "fixture_boundary_expired_or_cleanup".into(),
            ))
        } else {
            Ok(())
        }
    }
}
impl<S: Store> Store for BoundaryStore<S> {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
    }
    fn create_thread_with(&self, s: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(s)
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
        g: &ThreadGeneration,
        r: u64,
        s: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, g, r, s)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn delete_thread_fenced(&self, t: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(t)
    }
    fn cancel_run_fenced(
        &self,
        t: &RunCancellationTarget,
        f: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.cancel_calls.fetch_add(1, Ordering::AcqRel);
        self.cancellation_arguments
            .lock()
            .unwrap()
            .push((t.clone(), f.clone()));
        Self::hold(&self.before_cancel)?;
        match self.cancel_behavior {
            CancelBehavior::Conflict => return Err(StoreError::RevisionConflict),
            CancelBehavior::Unsupported => return Err(StoreError::UnsupportedOperation),
            CancelBehavior::OwnerConflict => return Err(StoreError::OwnerConflict),
            CancelBehavior::Fail => {
                return Err(StoreError::Unavailable("fixture-only-store-error".into()))
            }
            CancelBehavior::Panic => panic!("fixture-only-store-panic"),
            CancelBehavior::Delegate => {}
        }
        let committed = self.inner.cancel_run_fenced(t, f)?;
        Self::hold(&self.after_cancel)?;
        Ok(committed)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        f: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let old = self.inner.execution(id)?;
        let is_ack = old.as_ref().is_some_and(|old| {
            plan.snapshot.tasks.iter().any(|next| {
                next.status == TaskStatus::Cancelled
                    && old.snapshot.tasks.iter().any(|prior| {
                        prior.spec.id == next.spec.id
                            && prior.attempt_id == next.attempt_id
                            && prior.status == TaskStatus::InFlight
                    })
            })
        });
        let dispatch = plan
            .snapshot
            .tasks
            .iter()
            .any(|t| t.status == TaskStatus::InFlight)
            && old.is_some_and(|old| old.snapshot.attempts_used < plan.snapshot.attempts_used);
        if is_ack {
            self.ack_calls.fetch_add(1, Ordering::AcqRel);
            self.acknowledgement_arguments
                .lock()
                .unwrap()
                .push((id.clone(), f, plan.clone()));
            Self::hold(&self.before_ack)?;
            if self
                .completion_conflicts
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(StoreError::RevisionConflict);
            }
        }
        let current = self.inner.commit(id, f, plan)?;
        if dispatch {
            Self::hold(&self.after_dispatch)?;
        }
        Ok(current)
    }
}
impl<S: ApprovalStore> ApprovalStore for BoundaryStore<S> {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        f: ApprovalFence,
        p: CommitPlan,
        m: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, f, p, m)
    }
}
