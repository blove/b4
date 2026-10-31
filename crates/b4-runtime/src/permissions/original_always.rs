//! Genuine original Always continuation. No reusable-use witness is synthesized.
use super::super::{
    PermissionCallPhase, PermissionCancellationAuthority, PermissionStore,
    PermissionUseCallbackStatus,
};
use super::*;
use crate::approval::{ApprovalAuthority, ApprovalContext, ApprovalOperation};
use crate::{EffectOutcome, ExecutionFence, NativeInvocationError, StoreError};
use b4_contract::{KernelCommand, TransitionFacts};
use std::panic::{catch_unwind, AssertUnwindSafe};

/// A sealed selector borrowing the actual NewHuman claim.
pub struct PermissionOriginalAlwaysSelector<'a> {
    pub(super) claim: &'a PermissionDispatchClaim<'a>,
    pub(super) current: Option<&'a ApprovalResource>,
}
pub struct PermissionOriginalAlwaysRead<'a> {
    selector: &'a PermissionOriginalAlwaysSelector<'a>,
    records: [bool; 3],
    absent: bool,
    journals: usize,
    last_time: u64,
}
pub struct PermissionOriginalAlwaysObservation {
    pub(super) resource: ApprovalResource,
}
pub struct PermissionOriginalAlwaysOutcomeRequest<'a> {
    selector: PermissionOriginalAlwaysSelector<'a>,
    plan: CommitPlan,
}
pub struct PermissionOriginalAlwaysCommit {
    execution: StoredExecution,
}
impl PermissionOriginalAlwaysCommit {
    pub(super) fn into_execution(self) -> StoredExecution {
        self.execution
    }
}
impl PermissionOriginalAlwaysSelector<'_> {
    pub fn current_resource(&self) -> &ApprovalResource {
        self.current.unwrap_or(&self.claim.resource)
    }
    pub fn journal_count(&self) -> usize {
        if self.current_resource() == &self.claim.resource {
            5
        } else {
            6
        }
    }
    pub fn validate_current(&self, resource: &ApprovalResource) -> Result<(), PermissionError> {
        super::super::original_always_cancellation_relation::require_current(
            &self.claim.resource,
            resource,
            true,
        )
    }

    pub fn execution_id(&self) -> &b4_contract::ExecutionId {
        &self.claim.invocation.execution_id
    }
    pub fn thread_id(&self) -> &b4_contract::ThreadId {
        &self.claim.resource.thread.id
    }
    pub fn begin(&self) -> Result<PermissionOriginalAlwaysRead<'_>, PermissionError> {
        let PermissionClaimWitness::NewHuman(w) = &self.claim.witness else {
            return Err(PermissionError::UnsupportedProfile);
        };
        if !Arc::ptr_eq(&w.origin, &self.claim.origin) {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionOriginalAlwaysRead {
            selector: self,
            records: [false; 3],
            absent: false,
            journals: 0,
            last_time: 0,
        })
    }
}
impl PermissionOriginalAlwaysRead<'_> {
    pub fn execution_id(&self) -> &b4_contract::ExecutionId {
        self.selector.execution_id()
    }
    pub fn thread_id(&self) -> &b4_contract::ThreadId {
        self.selector.thread_id()
    }
    /// Compare every physical index and the immutable bytes captured at real approval.
    /// Indices: acceptance=0, decision=1, consumed grant=2.
    pub fn record(
        &mut self,
        source: usize,
        fields: &[&[u8]],
        scalar: i64,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        let PermissionClaimWitness::NewHuman(w) = &self.selector.claim.witness else {
            return Err(PermissionError::UnsupportedProfile);
        };
        if source > 2 || self.records[source] || raw != w.raw[source] {
            return Err(PermissionError::CorruptRecord);
        }
        let a = &w.acceptance;
        let d = &w.decision;
        let g = &w.grant;
        let previous = a.before_snapshot().revision.to_be_bytes();
        let committed = a.committed_revision().to_be_bytes();
        let parking = g.task_parking_revision.to_be_bytes();
        let (expected, value): (Vec<&[u8]>, i64) = match source {
            0 => (
                vec![
                    a.before_snapshot().execution_id.as_str().as_bytes(),
                    a.turn_id().as_str().as_bytes(),
                    a.thread_id().as_str().as_bytes(),
                    a.thread_generation().as_str().as_bytes(),
                    &previous,
                    &committed,
                ],
                i64::from(a.record_version()),
            ),
            1 => (
                vec![
                    d.permission_id().as_str().as_bytes(),
                    d.scope().thread_id().as_str().as_bytes(),
                    d.scope().thread_generation().as_str().as_bytes(),
                    d.scope_digest().as_bytes(),
                ],
                i64::from(d.record_version()),
            ),
            _ => (
                vec![
                    g.execution_id.as_str().as_bytes(),
                    g.interrupt_id.as_str().as_bytes(),
                    g.task_id.as_str().as_bytes(),
                    g.thread_id.as_str().as_bytes(),
                    g.thread_generation.as_str().as_bytes(),
                    &parking,
                ],
                1,
            ),
        };
        if expected.as_slice() != fields || scalar != value {
            return Err(PermissionError::CorruptRecord);
        }
        self.records[source] = true;
        Ok(())
    }
    pub fn absence(&mut self, uses: bool, legacy_acceptances: bool) -> Result<(), PermissionError> {
        if uses || legacy_acceptances || self.absent {
            return Err(PermissionError::CorruptRecord);
        }
        self.absent = true;
        Ok(())
    }
    pub fn journal(&mut self, entry: &JournalEntry) -> Result<(), PermissionError> {
        let claim = self.selector.claim;
        let PermissionClaimWitness::NewHuman(w) = &claim.witness else {
            return Err(PermissionError::UnsupportedProfile);
        };
        if self.journals >= self.selector.journal_count()
            || entry.execution_id != claim.invocation.execution_id
            || entry.revision != self.journals as u64
            || entry.owner_generation != claim.resource.execution.snapshot.owner_generation
            || entry.updated_at_ms < self.last_time
        {
            return Err(PermissionError::CorruptRecord);
        }
        let valid = match self.journals {
            0 => entry.status == b4_contract::ExecutionStatus::Accepted,
            1 => entry.status == b4_contract::ExecutionStatus::Running,
            2 => entry == &w.park,
            3 => entry == &w.resume,
            4 => entry == &JournalEntry::from(&claim.resource.execution.snapshot),
            5 => entry == &JournalEntry::from(&self.selector.current_resource().execution.snapshot),
            _ => false,
        };
        if !valid {
            return Err(PermissionError::CorruptRecord);
        }
        self.journals += 1;
        self.last_time = entry.updated_at_ms;
        Ok(())
    }
    pub fn finish(
        self,
        owner: u64,
        resource: ApprovalResource,
    ) -> Result<PermissionOriginalAlwaysObservation, PermissionError> {
        let expected = self.selector.current_resource();
        super::super::original_always_cancellation_relation::require_current(
            &self.selector.claim.resource,
            expected,
            self.selector.current.is_some(),
        )?;
        if self.records != [true; 3]
            || !self.absent
            || self.journals != self.selector.journal_count()
            || &resource != expected
            || owner != expected.execution.snapshot.owner_generation
        {
            return Err(StoreError::RevisionConflict.into());
        }
        Ok(PermissionOriginalAlwaysObservation { resource })
    }
}
impl PermissionOriginalAlwaysOutcomeRequest<'_> {
    pub fn selector(&self) -> &PermissionOriginalAlwaysSelector<'_> {
        &self.selector
    }
    pub fn plan(&self) -> &CommitPlan {
        &self.plan
    }
    /// Called only after the same immediate transaction has matched real evidence.
    pub fn acknowledge_commit(
        &self,
        observation: PermissionOriginalAlwaysObservation,
    ) -> Result<PermissionOriginalAlwaysCommit, PermissionError> {
        if &observation.resource != self.selector.current_resource() {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionOriginalAlwaysCommit {
            execution: StoredExecution {
                thread_id: observation.resource.thread.id,
                snapshot: self.plan.snapshot.clone(),
            },
        })
    }
}

/// One consuming carrier, retained through local hook, callback and storage retry.
pub(in crate::permissions) struct OriginalAlwaysWork<
    'env,
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
> {
    claim: PermissionDispatchClaim<'env>,
    store: &'env S,
    current: &'env PermissionCatalog,
    _authority: &'env A,
    status: Option<PermissionUseCallbackStatus>,
    actual: Option<EffectOutcome>,
    _panic: Option<Box<dyn std::any::Any + Send>>,
    completed: bool,
    controlled: bool,
    native_error: Option<NativeInvocationError>,
}
impl<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    OriginalAlwaysWork<'env, S, A>
{
    pub(in crate::permissions) fn renew(
        claim: PermissionDispatchClaim<'env>,
        store: &'env S,
        authority: &'env A,
        current: &'env PermissionCatalog,
        context: &ApprovalContext,
    ) -> Result<Self, PermissionError> {
        let PermissionClaimWitness::NewHuman(w) = &claim.witness else {
            return Err(PermissionError::UnsupportedProfile);
        };
        if !Arc::ptr_eq(&claim.origin, &w.origin)
            || context.actor != claim.context.origin
            || context.policy_revision != claim.context.policy_revision
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let selector = PermissionOriginalAlwaysSelector {
            claim: &claim,
            current: None,
        };

        let observed = store.capture_permission_original_always(&selector)?;

        authority
            .authorize(context, ApprovalOperation::Dispatch, &observed.resource)
            .map_err(|_| approval::ApprovalError::Unauthorized)?;
        let definition = &claim.resource.execution.snapshot.definition;
        let scope = w.decision.scope();
        if current.current_definition(&definition.route) != Some(definition) {
            return Err(PermissionError::InvalidCatalog);
        }
        // Exact current tool/constraint identity and caught claimed constraint.

        let registration = current.original_always_registration(
            definition,
            &claim.resource.execution.snapshot.tasks[0],
        )?;
        if scope.application().application() != current.application_identity().application
            || scope.application().revision() != current.application_identity().revision
            || scope.tool_id() != &registration.tool_id
            || scope.tool_revision() != &registration.tool_revision
            || scope.constraint_revision() != &registration.constraint_revision
        {
            return Err(PermissionError::ConstraintDenied);
        }

        current.validate_call(
            &claim.context,
            definition,
            &claim.resource.execution.snapshot.tasks[0],
            PermissionCallPhase::Claimed(&claim.invocation),
        )?;
        // A final independent resource and immutable evidence observation fences policy work.

        store.capture_permission_original_always(&selector)?;
        Ok(Self {
            claim,
            store,
            current,
            _authority: authority,
            status: None,
            actual: None,
            _panic: None,
            completed: false,
            controlled: false,
            native_error: None,
        })
    }
    pub(in crate::permissions) fn cancel_withheld(
        &self,
        authority: &dyn PermissionCancellationAuthority,
        catalog: &PermissionCatalog,
        request: &PermissionOriginalAlwaysWithheldCancellationRequest<'_>,
        account: &mut super::super::use_withheld_cancellation::WithheldAccount,
    ) -> Result<StoredExecution, PermissionError> {
        if self.status.is_some()
            || self.actual.is_some()
            || self.completed
            || self.controlled
            || self.native_error.is_some()
            || self._panic.is_some()
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        super::original_always_withheld_cancellation::cancel(
            self.store,
            authority,
            catalog,
            request,
            &self.claim,
            &mut account.budget,
        )
    }
    pub(in crate::permissions) fn invoke(self) -> Self {
        self.invoke_entry(None)
    }
    pub(in crate::permissions) fn invoke_controlled(
        self,
        observer: &crate::NativeCancellationObserver,
    ) -> Self {
        self.invoke_entry(Some(observer))
    }
    fn invoke_entry(mut self, observer: Option<&crate::NativeCancellationObserver>) -> Self {
        self.controlled = observer.is_some();
        match catch_unwind(AssertUnwindSafe(|| match observer {
            None => self.current.application().invoke(
                &self.claim.resource.execution.snapshot.definition,
                &self.claim.invocation,
                &self.claim.context,
            ),
            Some(observer) => self
                .current
                .application()
                .invoke_controlled(
                    &self.claim.resource.execution.snapshot.definition,
                    &self.claim.invocation,
                    &self.claim.context,
                    observer,
                )
                .map(|actual| match actual {
                    crate::ControlledNativeOutcome::Settled(crate::NativeOutcome::Succeeded(
                        result,
                    )) => EffectOutcome::Succeeded(result),
                    crate::ControlledNativeOutcome::Settled(crate::NativeOutcome::Failed(
                        reason,
                    )) => EffectOutcome::Failed(reason),
                    crate::ControlledNativeOutcome::Cancelled => EffectOutcome::Cancelled,
                }),
        })) {
            Ok(Ok(actual)) => {
                self.status = Some(if crate::validate_effect_outcome(&actual, 65_536).is_ok() {
                    if matches!(actual, EffectOutcome::Cancelled)
                        && !observer.is_some_and(|o| o.cancellation_requested())
                    {
                        PermissionUseCallbackStatus::Uncertain
                    } else {
                        PermissionUseCallbackStatus::Settled
                    }
                } else {
                    PermissionUseCallbackStatus::OutcomeRejected
                });
                self.actual = Some(actual);
            }
            Ok(Err(error)) => {
                self.status = Some(match &error {
                    NativeInvocationError::Uncertain => PermissionUseCallbackStatus::Uncertain,
                    NativeInvocationError::InvalidOutput => {
                        PermissionUseCallbackStatus::InvalidOutput
                    }
                    _ => PermissionUseCallbackStatus::NativeRejected,
                });
                self.native_error = Some(error);
            }
            Err(payload) => {
                self.status = Some(PermissionUseCallbackStatus::Panicked);
                self._panic = Some(payload);
            }
        }
        self
    }
    pub(in crate::permissions) fn status(&self) -> PermissionUseCallbackStatus {
        self.status
            .unwrap_or(PermissionUseCallbackStatus::PreEntryRefused)
    }
    pub(in crate::permissions) fn callback_status(&self) -> PermissionUseCallbackStatus {
        self.status()
    }
    pub(in crate::permissions) fn settled_outcome(&self) -> Option<&EffectOutcome> {
        if self.status == Some(PermissionUseCallbackStatus::Settled) {
            self.actual.as_ref()
        } else {
            None
        }
    }
    pub(in crate::permissions) fn commit(
        &mut self,
        facts: &TransitionFacts,
    ) -> Result<StoredExecution, PermissionError> {
        self.commit_candidate(facts, None)
    }
    pub(in crate::permissions) fn commit_driver_candidate(
        &mut self,
        facts: &TransitionFacts,
        id: &b4_contract::ExecutionId,
        fence: ExecutionFence,
        plan: &CommitPlan,
        account: &mut super::super::driver_adapter_account::DriverAdapterAccount,
    ) -> Result<StoredExecution, PermissionError> {
        account.snapshot(&self.claim.resource.execution)?;
        self.commit_candidate(facts, Some((id, fence, plan, account)))
    }
    fn commit_candidate(
        &mut self,
        facts: &TransitionFacts,
        candidate: Option<(
            &b4_contract::ExecutionId,
            ExecutionFence,
            &CommitPlan,
            &mut super::super::driver_adapter_account::DriverAdapterAccount,
        )>,
    ) -> Result<StoredExecution, PermissionError> {
        if self.completed {
            return Err(PermissionError::UnsupportedProfile);
        }
        let selector = PermissionOriginalAlwaysSelector {
            claim: &self.claim,
            current: None,
        };
        let current = if self.controlled {
            self.store
                .capture_permission_original_always_current(&selector)?
        } else {
            self.claim.resource.clone()
        };
        super::super::original_always_cancellation_relation::require_current(
            &self.claim.resource,
            &current,
            self.controlled,
        )?;
        if self.controlled {
            // The source read independently matches all original immutable evidence
            // and the exact current projection before deriving any terminal plan.
            self.store
                .capture_permission_original_always(&PermissionOriginalAlwaysSelector {
                    claim: &self.claim,
                    current: Some(&current),
                })?;
        }
        let command = match self
            .settled_outcome()
            .ok_or(PermissionError::UnsupportedProfile)?
        {
            EffectOutcome::Succeeded(result) => KernelCommand::Complete {
                task_id: self.claim.invocation.task_id.clone(),
                attempt_id: self.claim.invocation.attempt_id.clone(),
                result: result.clone(),
            },
            EffectOutcome::Failed(message) => KernelCommand::FailAttempt {
                task_id: self.claim.invocation.task_id.clone(),
                attempt_id: self.claim.invocation.attempt_id.clone(),
                failure: message.clone(),
            },
            EffectOutcome::Cancelled => {
                if !self.controlled || current == self.claim.resource {
                    return Err(PermissionError::UnsupportedProfile);
                }
                KernelCommand::CancelAttempt {
                    task_id: self.claim.invocation.task_id.clone(),
                    attempt_id: self.claim.invocation.attempt_id.clone(),
                }
            }
        };
        let before = &current.execution.snapshot;
        let plan = b4_kernel::transition(before, command, facts.clone())?;
        if let Some((id, fence, actual, account)) = candidate {
            account.plan(&plan)?;
            account.plan(actual)?;
            account.before_candidate()?;
            if !super::super::driver_adapter::candidate_matches(
                &before.execution_id,
                ExecutionFence::from(before),
                &plan,
                id,
                fence,
                actual,
            ) {
                account.refused_candidate();
                return Err(PermissionError::UnsupportedProfile);
            }
        }
        let request = PermissionOriginalAlwaysOutcomeRequest {
            selector: PermissionOriginalAlwaysSelector {
                claim: &self.claim,
                current: if self.controlled {
                    Some(&current)
                } else {
                    None
                },
            },
            plan,
        };

        let commit = self
            .store
            .commit_permission_original_always_outcome(&request)?;
        self.completed = true;
        Ok(commit.into_execution())
    }
}
