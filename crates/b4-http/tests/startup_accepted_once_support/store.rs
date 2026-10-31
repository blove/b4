use super::{
    evidence::now_ms,
    gates::Gate,
    model::{Failure, Result},
};
use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// Pure successful-return observation: no Store, callback, or host resource.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptReturn {
    pub execution: StoredExecution,
    pub request: ApprovalAcceptanceRequest,
    pub returned_at_ms: Option<u64>,
}
pub struct AcceptStore {
    pub inner: Arc<SqliteStore>,
    pub gate: Arc<Gate>,
    returned: Mutex<Option<AcceptReturn>>,
}
impl AcceptStore {
    pub fn new(inner: Arc<SqliteStore>, gate: Arc<Gate>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            gate,
            returned: Mutex::new(None),
        })
    }
    pub fn observation(&self) -> Result<AcceptReturn> {
        self.returned
            .lock()
            .map_err(|_| Failure::Setup)?
            .clone()
            .ok_or(Failure::Setup)
    }
}
impl Store for AcceptStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> std::result::Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
    }
    fn create_thread_with(
        &self,
        seed: ThreadSeed,
    ) -> std::result::Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> std::result::Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> std::result::Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn delete_thread_fenced(
        &self,
        target: &ThreadDeletionTarget,
    ) -> std::result::Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> std::result::Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn execution(
        &self,
        id: &ExecutionId,
    ) -> std::result::Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(
        &self,
        id: &ThreadId,
        initial: Snapshot,
    ) -> std::result::Result<StoredExecution, StoreError> {
        self.inner.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        resource_revision: u64,
        initial: Snapshot,
    ) -> std::result::Result<StoredExecution, StoreError> {
        self.inner
            .admit_fenced(id, generation, resource_revision, initial)
    }
    fn readiness(&self) -> std::result::Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> std::result::Result<StoredExecution, StoreError> {
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> std::result::Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> std::result::Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.inner.discover_unfinished(cursor)
    }
    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> std::result::Result<ThreadView, RecoveryConfinementBackendError> {
        self.inner.confinement_view_fenced(target)
    }
    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> std::result::Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        self.inner
            .confine_recovery_fenced(original, target, reason, fresh_turn, facts)
    }
}
impl ApprovalStore for AcceptStore {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
    }
    fn approval_view(
        &self,
        id: &ExecutionId,
    ) -> std::result::Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> std::result::Result<StoredExecution, ApprovalError> {
        let accepted = match &mutation {
            ApprovalMutation::Accept(request) => Some(request.clone()),
            _ => None,
        };
        // Returning from this production call has retired its transaction and
        // Connection guard. The hook cannot hold the database transaction open.
        let execution = self.inner.commit_approval(id, fence, plan, mutation)?;
        if let Some(request) = accepted {
            if let Ok(mut observed) = self.returned.lock() {
                if observed.is_none() {
                    *observed = Some(AcceptReturn {
                        execution: execution.clone(),
                        request,
                        returned_at_ms: now_ms().ok(),
                    });
                    drop(observed);
                    // A timeout/cleanup fallback returns the unchanged result,
                    // but the parent rejects that fixture before intended kill.
                    let _positive = self.gate.hold();
                }
            }
        }
        Ok(execution)
    }
}
