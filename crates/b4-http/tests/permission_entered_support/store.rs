//! Delegates real SQLite ports; holds only existing renewal/request return seams.
use crate::cooperative::Gate;
use b4_contract::*;
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
/// A finite one-shot read selector, armed only once the real renewal is paused.
pub struct WithheldRead {
    pub thread: ThreadId,
    pub execution: ExecutionId,
    pub remaining: usize,
    pub gate: Arc<Gate>,
}
pub struct Boundary {
    pub withheld_preflight: Mutex<Option<WithheldRead>>,
    pub original_withheld_renewal: Mutex<Option<(usize, Arc<Gate>)>>,
    pub disable_original_withheld: bool,
    pub disable_use_withheld: bool,
    pub before_withheld: Option<Arc<Gate>>,
    pub after_withheld: Option<Arc<Gate>>,
    pub withheld_writes: AtomicUsize,
    pub inner: Arc<SqliteStore>,
    pub before_entry: Option<Arc<Gate>>,
    /// One-shot barriers at an actual Driver preclaim read, while durable Ready.
    pub before_ready: Mutex<Option<Arc<Gate>>>,
    pub before_ready_cancel: Option<Arc<Gate>>,
    pub after_ready_cancel: Option<Arc<Gate>>,
    pub disable_original_ready: bool,
    /// Inject one bounded preclaim port refusal after a genuine SQLite capture.
    pub refuse_ready_capture: AtomicBool,
    pub thread_view_reads: AtomicUsize,
    pub before_cancel: Option<Arc<Gate>>,
    pub after_cancel: Option<Arc<Gate>>,
}
impl Boundary {
    pub fn new(inner: Arc<SqliteStore>) -> Self {
        Self {
            inner,
            withheld_preflight: Mutex::new(None),
            original_withheld_renewal: Mutex::new(None),
            disable_original_withheld: false,
            disable_use_withheld: false,
            before_withheld: None,
            after_withheld: None,
            withheld_writes: AtomicUsize::new(0),
            before_entry: None,
            before_ready: Mutex::new(None),
            before_ready_cancel: None,
            after_ready_cancel: None,
            disable_original_ready: false,
            refuse_ready_capture: AtomicBool::new(false),
            thread_view_reads: AtomicUsize::new(0),
            before_cancel: None,
            after_cancel: None,
        }
    }
    fn hold(&self, gate: &Option<Arc<Gate>>) -> Result<(), PermissionError> {
        if gate.as_ref().is_some_and(|g| !g.hold()) {
            Err(PermissionError::Store(StoreError::Unavailable(
                "finite fixture barrier".into(),
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
        self.thread_view_reads.fetch_add(1, Ordering::SeqCst);
        let actual = self.inner.thread_view(id)?;
        let gate = {
            let mut selected = self.withheld_preflight.lock().unwrap();
            if let Some(read) = selected.as_mut() {
                if &read.thread == id
                    && actual
                        .as_ref()
                        .and_then(|v| v.execution.as_ref())
                        .is_some_and(|e| e.snapshot.execution_id == read.execution)
                {
                    read.remaining -= 1;
                    if read.remaining == 0 {
                        selected.take().map(|r| r.gate)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        if gate.as_ref().is_some_and(|g| !g.hold()) {
            return Err(StoreError::Unavailable("finite withheld preflight".into()));
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
    fn permission_original_ready_cancellation_atomic_durable(&self) -> bool {
        !self.disable_original_ready
            && self
                .inner
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
        self.hold(&self.before_cancel)?;
        let result = self
            .inner
            .commit_permission_original_always_cancellation(prepared, budget)?;
        self.hold(&self.after_cancel)?;
        Ok(result)
    }
    fn permission_original_always_withheld_cancellation_atomic_durable(&self) -> bool {
        !self.disable_original_withheld
            && self
                .inner
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
        self.withheld_writes.fetch_add(1, Ordering::SeqCst);
        self.hold(&self.before_withheld)?;
        let actual = self
            .inner
            .commit_permission_original_always_withheld_cancellation(prepared, budget)?;
        self.hold(&self.after_withheld)?;
        Ok(actual)
    }
    fn permission_use_withheld_cancellation_atomic_durable(&self) -> bool {
        !self.disable_use_withheld
            && self
                .inner
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
        self.withheld_writes.fetch_add(1, Ordering::SeqCst);
        self.hold(&self.before_withheld)?;
        let actual = self
            .inner
            .commit_permission_use_withheld_cancellation(prepared, budget)?;
        self.hold(&self.after_withheld)?;
        Ok(actual)
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
        self.hold(&self.before_cancel)?;
        let result = self
            .inner
            .commit_permission_use_cancellation(prepared, budget)?;
        self.hold(&self.after_cancel)?;
        Ok(result)
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
        self.hold(&self.before_ready_cancel)?;
        let result = self
            .inner
            .commit_permission_use_ready_cancellation(prepared, budget)?;
        self.hold(&self.after_ready_cancel)?;
        Ok(result)
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
        let actual = self.inner.capture_permission_original_always(selector)?;
        let gate = {
            let mut armed = self.original_withheld_renewal.lock().unwrap();
            if selector.current_resource().execution.snapshot.revision == 4 {
                if let Some((remaining, _)) = armed.as_mut() {
                    *remaining -= 1;
                    if *remaining == 0 {
                        armed.take().map(|(_, gate)| gate)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        self.hold(&gate)?;
        Ok(actual)
    }
    fn commit_permission_original_always_outcome(
        &self,
        request: &b4_runtime::permissions::PermissionOriginalAlwaysOutcomeRequest<'_>,
    ) -> Result<b4_runtime::permissions::PermissionOriginalAlwaysCommit, PermissionError> {
        self.inner
            .commit_permission_original_always_outcome(request)
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
        self.inner.commit_permission_use_outcome(request, account)
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
    ) -> Result<Option<crate::StoredExecution>, PermissionError> {
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
    ) -> Result<crate::StoredExecution, PermissionError> {
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
        let result = self
            .inner
            .capture_permission_use_renewal_source(selector, account)?;
        self.hold(&self.before_entry)?;
        Ok(result)
    }

    fn capture_permission_runtime(
        &self,
        selector: &b4_runtime::permissions::PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCapture, PermissionError> {
        // Take before holding: no fixture mutex is held across actual Store work.
        let gate = self.before_ready.lock().unwrap().take();
        self.hold(&gate)?;
        let actual = self.inner.capture_permission_runtime(selector, budget)?;
        if self.refuse_ready_capture.swap(false, Ordering::SeqCst) {
            return Err(PermissionError::Store(StoreError::RevisionConflict));
        }
        Ok(actual)
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
    ) -> Result<crate::StoredExecution, PermissionError> {
        self.hold(&self.before_ready_cancel)?;
        let result = self
            .inner
            .commit_permission_ready_cancellation(prepared, budget)?;
        self.hold(&self.after_ready_cancel)?;
        Ok(result)
    }

    fn commit_permission_runtime(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedRuntimeMutation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCommit, PermissionError> {
        self.inner.commit_permission_runtime(prepared, budget)
    }
}
