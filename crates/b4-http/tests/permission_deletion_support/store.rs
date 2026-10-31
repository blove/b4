//! Narrow real-port delegation. Gates never fabricate captures or commit confirmations.
use crate::cooperative::Gate;
use b4_contract::*;
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
pub struct Boundary {
    pub inner: Arc<SqliteStore>,
    pub disable_deletion: bool,
    pub after_view: Mutex<Option<Arc<Gate>>>,
    pub after_capture: Mutex<Option<Arc<Gate>>>,
    pub after_terminal: Mutex<Option<Arc<Gate>>>,
    pub before_delete: Mutex<Option<Arc<Gate>>>,
    pub after_delete: Mutex<Option<Arc<Gate>>>,
    pub deletion_writes: AtomicUsize,
    pub final_delete_failed: AtomicBool,
}
impl Boundary {
    pub fn new(inner: Arc<SqliteStore>) -> Self {
        Self {
            inner,
            disable_deletion: false,
            after_view: Mutex::new(None),
            after_capture: Mutex::new(None),
            after_terminal: Mutex::new(None),
            before_delete: Mutex::new(None),
            after_delete: Mutex::new(None),
            deletion_writes: AtomicUsize::new(0),
            final_delete_failed: AtomicBool::new(false),
        }
    }
    fn hold(&self, gate: Option<Arc<Gate>>) -> Result<(), PermissionError> {
        if gate.is_some_and(|g| !g.hold()) {
            Err(PermissionError::Store(StoreError::Unavailable(
                "finite delegated-writer gate".into(),
            )))
        } else {
            Ok(())
        }
    }
}
impl Store for Boundary {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
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
        let actual = self.inner.thread_view(id)?;
        let gate = self.after_view.lock().unwrap().take();
        if gate.is_some_and(|g| !g.hold()) {
            return Err(StoreError::Unavailable("finite captured-view gate".into()));
        }
        Ok(actual)
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
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit(thread_id, initial)
    }
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(execution_id, fence, plan)
    }
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(execution_id)
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.inner.discover_unfinished(cursor)
    }

    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        self.inner.confinement_view_fenced(target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        self.inner
            .confine_recovery_fenced(original, target, reason, fresh_turn, facts)
    }
}
impl ApprovalStore for Boundary {
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
impl PermissionStore for Boundary {
    fn permission_settled_thread_deletion_atomic_durable(&self) -> bool {
        !self.disable_deletion
            && self
                .inner
                .permission_settled_thread_deletion_atomic_durable()
    }
    fn capture_permission_settled_thread_deletion(
        &self,
        target: &b4_runtime::ThreadDeletionTarget,
        account: &mut b4_runtime::permissions::PermissionThreadDeletionAccount,
    ) -> Result<b4_runtime::permissions::PermissionThreadDeletionCapture, PermissionError> {
        let actual = self
            .inner
            .capture_permission_settled_thread_deletion(target, account)?;
        let gate = self.after_capture.lock().unwrap().take();
        self.hold(gate)?;
        Ok(actual)
    }
    fn commit_permission_settled_thread_deletion(
        &self,
        capture: &b4_runtime::permissions::PermissionThreadDeletionCapture,
        target: &b4_runtime::ThreadDeletionTarget,
        account: &mut b4_runtime::permissions::PermissionThreadDeletionAccount,
    ) -> Result<b4_runtime::permissions::PermissionThreadDeletionCommit, PermissionError> {
        self.deletion_writes.fetch_add(1, Ordering::AcqRel);
        let before = self.before_delete.lock().unwrap().take();
        self.hold(before)?;
        let actual = match self
            .inner
            .commit_permission_settled_thread_deletion(capture, target, account)
        {
            Ok(actual) => actual,
            Err(error) => {
                self.final_delete_failed.store(matches!(&error, PermissionError::Store(StoreError::Unavailable(message)) if message == "permission_settled_thread_deletion_final_thread_delete_failed"), Ordering::Release);
                return Err(error);
            }
        };
        let after = self.after_delete.lock().unwrap().take();
        self.hold(after)?;
        Ok(actual)
    }

    fn permission_original_ready_cancellation_atomic_durable(&self) -> bool {
        self.inner
            .permission_original_ready_cancellation_atomic_durable()
    }
    fn permission_original_always_cancellation_atomic_durable(&self) -> bool {
        self.inner
            .permission_original_always_cancellation_atomic_durable()
    }
    fn capture_permission_original_always_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionOriginalAlwaysCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionOriginalAlwaysCancellationCapture, PermissionError>
    {
        self.inner
            .capture_permission_original_always_cancellation(selector, budget)
    }
    fn commit_permission_original_always_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedOriginalAlwaysCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionOriginalAlwaysCancellationCommit, PermissionError>
    {
        self.inner
            .commit_permission_original_always_cancellation(prepared, budget)
    }
    fn permission_original_always_withheld_cancellation_atomic_durable(&self) -> bool {
        self.inner
            .permission_original_always_withheld_cancellation_atomic_durable()
    }
    fn commit_permission_original_always_withheld_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedOriginalAlwaysWithheldCancellation<
            '_,
        >,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<
        b4_runtime::permissions::PermissionOriginalAlwaysWithheldCancellationCommit,
        PermissionError,
    > {
        self.inner
            .commit_permission_original_always_withheld_cancellation(prepared, budget)
    }
    fn permission_use_withheld_cancellation_atomic_durable(&self) -> bool {
        self.inner
            .permission_use_withheld_cancellation_atomic_durable()
    }
    fn capture_permission_use_withheld_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionUseWithheldCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseWithheldCancellationCapture, PermissionError>
    {
        self.inner
            .capture_permission_use_withheld_cancellation(selector, budget)
    }
    fn commit_permission_use_withheld_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedUseWithheldCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseWithheldCancellationCommit, PermissionError>
    {
        self.inner
            .commit_permission_use_withheld_cancellation(prepared, budget)
    }

    fn permission_use_cancellation_atomic_durable(&self) -> bool {
        self.inner.permission_use_cancellation_atomic_durable()
    }
    fn capture_permission_use_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionUseCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseCancellationCapture, PermissionError> {
        self.inner
            .capture_permission_use_cancellation(selector, budget)
    }
    fn commit_permission_use_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedUseCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseCancellationCommit, PermissionError> {
        self.inner
            .commit_permission_use_cancellation(prepared, budget)
    }

    fn permission_use_ready_cancellation_atomic_durable(&self) -> bool {
        self.inner
            .permission_use_ready_cancellation_atomic_durable()
    }
    fn capture_permission_use_ready_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionUseReadyCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseReadyCancellationCapture, PermissionError>
    {
        self.inner
            .capture_permission_use_ready_cancellation(selector, budget)
    }
    fn commit_permission_use_ready_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedUseReadyCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseReadyCancellationCommit, PermissionError>
    {
        self.inner
            .commit_permission_use_ready_cancellation(prepared, budget)
    }

    fn capture_permission_original_always_current(
        &self,
        selector: &b4_runtime::permissions::PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<ApprovalResource, PermissionError> {
        self.inner
            .capture_permission_original_always_current(selector)
    }
    fn original_always_atomic_durable(&self) -> bool {
        self.inner.original_always_atomic_durable()
    }

    fn capture_permission_original_always(
        &self,
        selector: &b4_runtime::permissions::PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<b4_runtime::permissions::PermissionOriginalAlwaysObservation, PermissionError> {
        self.inner.capture_permission_original_always(selector)
    }
    fn commit_permission_original_always_outcome(
        &self,
        request: &b4_runtime::permissions::PermissionOriginalAlwaysOutcomeRequest<'_>,
    ) -> Result<b4_runtime::permissions::PermissionOriginalAlwaysCommit, PermissionError> {
        let actual = self
            .inner
            .commit_permission_original_always_outcome(request)?;
        let gate = self.after_terminal.lock().unwrap().take();
        self.hold(gate)?;
        Ok(actual)
    }

    fn capture_permission_use_outcome_current(
        &self,
        request: &b4_runtime::permissions::PermissionUseOutcomeRequest<'_>,
        account: &mut b4_runtime::permissions::PermissionUseOutcomeAccount,
    ) -> Result<ApprovalResource, PermissionError> {
        self.inner
            .capture_permission_use_outcome_current(request, account)
    }

    fn capture_permission_use_outcome_source(
        &self,
        selector: &b4_runtime::permissions::PermissionUseOutcomeSourceSelector<'_>,
        account: &mut b4_runtime::permissions::PermissionUseOutcomeAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseOutcomePrepared, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &b4_runtime::permissions::PermissionUseOutcomeSourceSelector<'_>,
                    &mut b4_runtime::permissions::PermissionUseOutcomeAccount
                )>() <= 128
            );
        }
        self.inner
            .capture_permission_use_outcome_source(selector, account)
    }

    fn commit_permission_use_outcome(
        &self,
        request: &b4_runtime::permissions::PermissionUseOutcomeRequest<'_>,
        account: &mut b4_runtime::permissions::PermissionUseOutcomeAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseOutcomeCommit, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<
                    Result<b4_runtime::permissions::PermissionUseOutcomeCommit, PermissionError>,
                >() <= 8192
            );
            assert!(
                std::mem::size_of::<
                    std::ops::ControlFlow<
                        Result<std::convert::Infallible, PermissionError>,
                        b4_runtime::permissions::PermissionUseOutcomeCommit,
                    >,
                >() <= 8192
            );
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &b4_runtime::permissions::PermissionUseOutcomeRequest<'_>,
                    &mut b4_runtime::permissions::PermissionUseOutcomeAccount
                )>() <= 128
            );
        }
        let actual = self.inner.commit_permission_use_outcome(request, account)?;
        let gate = self.after_terminal.lock().unwrap().take();
        self.hold(gate)?;
        Ok(actual)
    }

    fn durable_permissions(&self) -> bool {
        self.inner.durable_permissions()
    }

    fn ready_authorization_atomic_durable(&self) -> bool {
        self.inner.ready_authorization_atomic_durable()
    }

    fn permission_ready_authorization_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        self.inner
            .permission_ready_authorization_resource(id, budget)
    }

    fn try_commit_permission_ready_authorization(
        &self,
        intent: &b4_runtime::permissions::PermissionReadyAuthorizationIntent<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Option<b4_runtime::StoredExecution>, PermissionError> {
        self.inner
            .try_commit_permission_ready_authorization(intent, budget)
    }

    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        self.inner.permission_resource(id, budget)
    }

    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        self.inner.permission_view(id, fence, selector, budget)
    }

    fn commit_permission_acceptance(
        &self,
        prepared: PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::StoredExecution, PermissionError> {
        self.inner.commit_permission_acceptance(prepared, budget)
    }

    fn capture_permission_use_renewal_resource(
        &self,
        selector: &b4_runtime::permissions::PermissionUseRenewalResourceSelector<'_>,
        account: &mut b4_runtime::permissions::PermissionUseRenewalAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseRenewalObservation, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &b4_runtime::permissions::PermissionUseRenewalResourceSelector<'_>,
                    &mut b4_runtime::permissions::PermissionUseRenewalAccount
                )>() <= 128
            );
        }
        self.inner
            .capture_permission_use_renewal_resource(selector, account)
    }
    fn capture_permission_use_renewal_source(
        &self,
        selector: &b4_runtime::permissions::PermissionUseRenewalSourceSelector<'_>,
        account: &mut b4_runtime::permissions::PermissionUseRenewalAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseRenewalSourceCapture, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &b4_runtime::permissions::PermissionUseRenewalSourceSelector<'_>,
                    &mut b4_runtime::permissions::PermissionUseRenewalAccount
                )>() <= 128
            );
        }
        self.inner
            .capture_permission_use_renewal_source(selector, account)
    }

    fn capture_permission_runtime(
        &self,
        selector: &b4_runtime::permissions::PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCapture, PermissionError> {
        self.inner.capture_permission_runtime(selector, budget)
    }

    fn capture_permission_ready_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCapture, PermissionError> {
        self.inner
            .capture_permission_ready_cancellation(selector, budget)
    }

    fn commit_permission_ready_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedReadyCancellation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::StoredExecution, PermissionError> {
        self.inner
            .commit_permission_ready_cancellation(prepared, budget)
    }

    fn commit_permission_runtime(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedRuntimeMutation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCommit, PermissionError> {
        self.inner.commit_permission_runtime(prepared, budget)
    }
}

// Constructor-only legacy profile: deliberately inherits the new default-false declaration.
// No runtime operation is dispatched through this fixture.
pub struct DefaultDeclaration(pub Arc<SqliteStore>);
impl Store for DefaultDeclaration {
    fn capabilities(&self) -> StoreCapabilities {
        self.0.capabilities()
    }
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
        g: &ThreadGeneration,
        r: u64,
        s: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.0.admit_fenced(id, g, r, s)
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
        self.0.commit(id, f, p)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.0.journal(id)
    }
}
impl ApprovalStore for DefaultDeclaration {
    fn approval_durable(&self) -> bool {
        self.0.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.0.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        f: ApprovalFence,
        p: CommitPlan,
        m: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.0.commit_approval(id, f, p, m)
    }
}
impl PermissionStore for DefaultDeclaration {
    fn durable_permissions(&self) -> bool {
        self.0.durable_permissions()
    }
    fn ready_authorization_atomic_durable(&self) -> bool {
        self.0.ready_authorization_atomic_durable()
    }
    fn original_always_atomic_durable(&self) -> bool {
        self.0.original_always_atomic_durable()
    }
    fn permission_original_always_cancellation_atomic_durable(&self) -> bool {
        self.0
            .permission_original_always_cancellation_atomic_durable()
    }
    fn permission_original_ready_cancellation_atomic_durable(&self) -> bool {
        self.0
            .permission_original_ready_cancellation_atomic_durable()
    }
    fn permission_use_cancellation_atomic_durable(&self) -> bool {
        self.0.permission_use_cancellation_atomic_durable()
    }
    fn permission_use_ready_cancellation_atomic_durable(&self) -> bool {
        self.0.permission_use_ready_cancellation_atomic_durable()
    }
    fn permission_original_always_withheld_cancellation_atomic_durable(&self) -> bool {
        self.0
            .permission_original_always_withheld_cancellation_atomic_durable()
    }
    fn permission_use_withheld_cancellation_atomic_durable(&self) -> bool {
        self.0.permission_use_withheld_cancellation_atomic_durable()
    }
}
