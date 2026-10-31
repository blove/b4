use super::{PermissionEvidenceBudget, PermissionView, PermissionViewSelector};
use crate::approval::{ApprovalError, ApprovalFence, ApprovalResource, ApprovalStore};
use crate::StoreError;
use b4_contract::ExecutionId;
use std::{fmt, sync::Arc};

/// Opaque matched-view handoff; construction alone authorizes no mutation.
/// The private factory owns the actual matched C2 view and borrows the same
/// immutable request, catalog and execution identity. The mutable evidence
/// account is supplied separately and is never reconstructed from the receipt.
#[allow(dead_code)] // Stored fields await the separately qualified writer consumer.
pub struct PermissionPreparedAcceptance<'a> {
    matched: PermissionView,
    request: &'a crate::approval::ApprovalAcceptanceRequest,
    catalog: &'a super::PermissionCatalog,
    execution_id: &'a ExecutionId,
    fence: ApprovalFence,
    captured_binding: super::catalog::PermissionCatalogCapture<'a>,
}

impl<'a> PermissionPreparedAcceptance<'a> {
    pub(super) fn from_matched_view(
        matched: PermissionView,
        request: &'a crate::approval::ApprovalAcceptanceRequest,
        catalog: &'a super::PermissionCatalog,
        execution_id: &'a ExecutionId,
        fence: ApprovalFence,
        captured_binding: super::catalog::PermissionCatalogCapture<'a>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Self, PermissionError> {
        // This fixed quote precedes every scalar, capture, layout and ownership
        // check below. Actual C2 lineage and full matching remain service/read
        // proofs; neither counters nor receipt usage create authority.
        budget.quote_permission_prepared_factory()?;
        super::validation::check_permission_prepared_identity(
            matched
                .acceptance_capture()
                .ok_or(PermissionError::CorruptRecord)?,
            &captured_binding.identity,
            budget,
        )?;
        if matched.receipt().purpose() != super::PermissionReadPurpose::Accept
            || matched.acceptance_capture().is_none()
            || !matched.permission_acceptances().is_empty()
            || !matched.permission_decisions().is_empty()
            || !matched.permission_uses().is_empty()
            || !matched.creator_acceptances().is_empty()
            || !matched.referenced_grants().is_empty()
            || !matched.referenced_acceptances().is_empty()
            || !matched.journal_entries().is_empty()
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if request.decisions.is_empty()
            || request.decisions.len() > 64
            || matched.grants().len() > 64
            || matched.acceptances().len() > 64
        {
            return Err(PermissionError::CorruptRecord);
        }
        let captured_usage = matched.receipt().usage_at_capture();
        let usage = budget.usage();
        if captured_usage.encoded_bytes() > usage.encoded_bytes()
            || captured_usage.structural_nodes() > usage.structural_nodes()
        {
            return Err(PermissionError::CorruptRecord);
        }
        let resource = matched.resource();
        let current = &resource.execution.snapshot;
        let origin = resource
            .thread
            .origin
            .as_ref()
            .ok_or(ApprovalError::Unauthorized)?;
        let identity = catalog.application_identity();
        let labels = [
            origin.application.as_str(),
            origin.tenant.as_str(),
            origin.principal.as_str(),
            request.context.actor.application.as_str(),
            request.context.actor.tenant.as_str(),
            request.context.actor.principal.as_str(),
            request.context.policy_revision.as_str(),
            identity.application.as_str(),
            identity.revision.as_str(),
        ];
        if labels.iter().any(|label| {
            label.is_empty() || label.len() > 256 || label.chars().any(char::is_control)
        }) {
            return Err(PermissionError::CorruptRecord);
        }
        // Checked contract identities are <=256 UTF-8 bytes. No Definition,
        // task, grant, acceptance or metadata traversal is repeated here.
        if &current.execution_id != execution_id {
            return Err(PermissionError::CorruptRecord);
        }
        if resource.thread.id != resource.execution.thread_id
            || resource.thread.active_execution_id.as_ref() != Some(&current.execution_id)
        {
            return Err(StoreError::ExecutionNotActive.into());
        }
        if origin != &request.context.actor {
            return Err(ApprovalError::Unauthorized.into());
        }
        if identity.application != origin.application {
            return Err(PermissionError::InvalidCatalog);
        }
        if fence.execution.owner_generation != current.owner_generation {
            return Err(StoreError::OwnerConflict.into());
        }
        if fence.execution.revision != current.revision
            || fence.resource_revision != resource.thread.resource_revision
        {
            return Err(StoreError::RevisionConflict.into());
        }
        if current.status != b4_contract::ExecutionStatus::WaitingForApproval {
            return Err(ApprovalError::NotStablePark.into());
        }
        if request.route != current.definition.route
            || request.definition_revision != current.definition.revision
        {
            return Err(ApprovalError::RouteMismatch.into());
        }
        // No binding clone or lookup: the same immutable catalog/request were
        // gated before R2, and C2 compared the complete prior/current view.
        Ok(Self {
            matched,
            request,
            catalog,
            execution_id,
            fence,
            captured_binding,
        })
    }
}

// Inline handoffs are paid without assuming compiler move elision.
const _: () = assert!(std::mem::size_of::<PermissionPreparedAcceptance<'static>>() <= 2_048);
const _: () = assert!(
    std::mem::size_of::<Result<PermissionPreparedAcceptance<'static>, PermissionError>>() <= 2_048
);

impl fmt::Debug for PermissionPreparedAcceptance<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionPreparedAcceptance")
    }
}

/// Dedicated management authorization; this callback grants no effect permit.
pub trait PermissionCancellationAuthority: Send + Sync {
    fn authorize_cancel(
        &self,
        context: &crate::approval::ApprovalContext,
        resource: &crate::approval::ApprovalResource,
        target: &crate::RunCancellationTarget,
    ) -> Result<(), PermissionError>;
}

impl<A: PermissionCancellationAuthority + ?Sized> PermissionCancellationAuthority for Arc<A> {
    fn authorize_cancel(
        &self,
        context: &crate::approval::ApprovalContext,
        resource: &crate::approval::ApprovalResource,
        target: &crate::RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        (**self).authorize_cancel(context, resource, target)
    }
}

/// Immutable cancellation observation and transition facts; it is not a permit.
pub struct PermissionReadyCancellationRequest<'request> {
    pub context: &'request crate::approval::ApprovalContext,
    pub target: &'request crate::RunCancellationTarget,
    pub facts: b4_contract::TransitionFacts,
}

/// Separate opt-in declaration; approval durability alone is insufficient.
pub trait PermissionStore: ApprovalStore {
    /// Separate opt-in: deletion must validate and remove complete settled history atomically.
    fn permission_settled_thread_deletion_atomic_durable(&self) -> bool {
        false
    }
    fn capture_permission_settled_thread_deletion(
        &self,
        _target: &crate::ThreadDeletionTarget,
        _account: &mut super::PermissionThreadDeletionAccount,
    ) -> Result<super::PermissionThreadDeletionCapture, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn commit_permission_settled_thread_deletion(
        &self,
        _capture: &super::PermissionThreadDeletionCapture,
        _target: &crate::ThreadDeletionTarget,
        _account: &mut super::PermissionThreadDeletionAccount,
    ) -> Result<super::PermissionThreadDeletionCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    /// Existing typed original Ready cancellation is atomic and durable.
    fn permission_original_ready_cancellation_atomic_durable(&self) -> bool {
        false
    }
    /// Narrow opt-in for atomic durable original Always capture and settlement.
    /// This does not advertise general permission lifecycle support.
    fn permission_original_always_cancellation_atomic_durable(&self) -> bool {
        false
    }
    fn capture_permission_original_always_cancellation(
        &self,
        _selector: &super::PermissionOriginalAlwaysCancellationSelector<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionOriginalAlwaysCancellationCapture, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn commit_permission_original_always_cancellation(
        &self,
        _prepared: &super::PermissionPreparedOriginalAlwaysCancellation<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionOriginalAlwaysCancellationCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn permission_original_always_withheld_cancellation_atomic_durable(&self) -> bool {
        false
    }
    fn commit_permission_original_always_withheld_cancellation(
        &self,
        _prepared: &super::PermissionPreparedOriginalAlwaysWithheldCancellation<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionOriginalAlwaysWithheldCancellationCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn permission_use_withheld_cancellation_atomic_durable(&self) -> bool {
        false
    }
    fn capture_permission_use_withheld_cancellation(
        &self,
        _selector: &super::PermissionUseWithheldCancellationSelector<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseWithheldCancellationCapture, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn commit_permission_use_withheld_cancellation(
        &self,
        _prepared: &super::PermissionPreparedUseWithheldCancellation<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseWithheldCancellationCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn permission_use_cancellation_atomic_durable(&self) -> bool {
        false
    }
    fn capture_permission_use_cancellation(
        &self,
        _selector: &super::PermissionUseCancellationSelector<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseCancellationCapture, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn commit_permission_use_cancellation(
        &self,
        _prepared: &super::PermissionPreparedUseCancellation<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseCancellationCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn permission_use_ready_cancellation_atomic_durable(&self) -> bool {
        false
    }
    fn capture_permission_use_ready_cancellation(
        &self,
        _selector: &super::PermissionUseReadyCancellationSelector<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseReadyCancellationCapture, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn commit_permission_use_ready_cancellation(
        &self,
        _prepared: &super::PermissionPreparedUseReadyCancellation<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseReadyCancellationCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn capture_permission_original_always_current(
        &self,
        _selector: &super::PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<ApprovalResource, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn original_always_atomic_durable(&self) -> bool {
        false
    }

    fn capture_permission_original_always(
        &self,
        _selector: &super::PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<super::PermissionOriginalAlwaysObservation, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn commit_permission_original_always_outcome(
        &self,
        _request: &super::PermissionOriginalAlwaysOutcomeRequest<'_>,
    ) -> Result<super::PermissionOriginalAlwaysCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    /// Read-only outcome provenance preparation; distinct from renewal/Dispatch.
    fn capture_permission_use_outcome_current(
        &self,
        _request: &super::PermissionUseOutcomeRequest<'_>,
        _account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<ApprovalResource, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn capture_permission_use_outcome_source(
        &self,
        _selector: &super::PermissionUseOutcomeSourceSelector<'_>,
        account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<super::PermissionUseOutcomePrepared, PermissionError> {
        account.source_port_inception()?;
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseOutcomeSourceSelector<'_>,
                    &mut super::PermissionUseOutcomeAccount
                )>() <= 128
            );
        }
        Err(PermissionError::UnsupportedProfile)
    }

    /// Receipt-derived typed settlement; unsupported unless a reviewed writer opts in.
    fn commit_permission_use_outcome(
        &self,
        _request: &super::PermissionUseOutcomeRequest<'_>,
        _account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<super::PermissionUseOutcomeCommit, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<Result<super::PermissionUseOutcomeCommit, PermissionError>>()
                    <= 8192
            );
            assert!(
                std::mem::size_of::<
                    std::ops::ControlFlow<
                        Result<std::convert::Infallible, PermissionError>,
                        super::PermissionUseOutcomeCommit,
                    >,
                >() <= 8192
            );
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseOutcomeRequest<'_>,
                    &mut super::PermissionUseOutcomeAccount
                )>() <= 128
            );
        }
        Err(PermissionError::UnsupportedProfile)
    }

    fn durable_permissions(&self) -> bool {
        false
    }

    /// Narrow opt-in for the typed atomic Ready authorization writer only.
    fn ready_authorization_atomic_durable(&self) -> bool {
        false
    }

    /// Separate opt-in bounded authorization resource reader; this grants no use.
    fn permission_ready_authorization_resource(
        &self,
        _id: &ExecutionId,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    /// A validated borrowed intent alone cannot authorize an atomic use.
    fn try_commit_permission_ready_authorization(
        &self,
        _intent: &super::PermissionReadyAuthorizationIntent<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<Option<crate::StoredExecution>, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn permission_resource(
        &self,
        _id: &ExecutionId,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn permission_view(
        &self,
        _id: &ExecutionId,
        _fence: ApprovalFence,
        _selector: &PermissionViewSelector<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn commit_permission_acceptance(
        &self,
        _prepared: PermissionPreparedAcceptance<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<crate::StoredExecution, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn capture_permission_use_renewal_resource(
        &self,
        _selector: &super::PermissionUseRenewalResourceSelector<'_>,
        _account: &mut super::PermissionUseRenewalAccount,
    ) -> Result<super::PermissionUseRenewalObservation, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseRenewalResourceSelector<'_>,
                    &mut super::PermissionUseRenewalAccount
                )>() <= 128
            );
        }
        Err(PermissionError::UnsupportedProfile)
    }
    fn capture_permission_use_renewal_source(
        &self,
        _selector: &super::PermissionUseRenewalSourceSelector<'_>,
        _account: &mut super::PermissionUseRenewalAccount,
    ) -> Result<super::PermissionUseRenewalSourceCapture, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseRenewalSourceSelector<'_>,
                    &mut super::PermissionUseRenewalAccount
                )>() <= 128
            );
        }
        Err(PermissionError::UnsupportedProfile)
    }

    fn capture_permission_runtime(
        &self,
        _selector: &super::PermissionRuntimeSelector<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionRuntimeCapture, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn capture_permission_ready_cancellation(
        &self,
        _selector: &super::PermissionRuntimeSelector<'_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionRuntimeCapture, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }

    fn commit_permission_ready_cancellation(
        &self,
        prepared: &super::PermissionPreparedReadyCancellation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<crate::StoredExecution, PermissionError> {
        super::runtime_mutation::refuse_permission_ready_cancellation(prepared, budget)
    }

    fn commit_permission_runtime(
        &self,
        _prepared: &super::PermissionPreparedRuntimeMutation<'_, '_>,
        _budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionRuntimeCommit, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
}

impl<S: PermissionStore + ?Sized> PermissionStore for Arc<S> {
    fn permission_settled_thread_deletion_atomic_durable(&self) -> bool {
        (**self).permission_settled_thread_deletion_atomic_durable()
    }
    fn capture_permission_settled_thread_deletion(
        &self,
        target: &crate::ThreadDeletionTarget,
        account: &mut super::PermissionThreadDeletionAccount,
    ) -> Result<super::PermissionThreadDeletionCapture, PermissionError> {
        (**self).capture_permission_settled_thread_deletion(target, account)
    }
    fn commit_permission_settled_thread_deletion(
        &self,
        capture: &super::PermissionThreadDeletionCapture,
        target: &crate::ThreadDeletionTarget,
        account: &mut super::PermissionThreadDeletionAccount,
    ) -> Result<super::PermissionThreadDeletionCommit, PermissionError> {
        (**self).commit_permission_settled_thread_deletion(capture, target, account)
    }

    fn permission_original_ready_cancellation_atomic_durable(&self) -> bool {
        (**self).permission_original_ready_cancellation_atomic_durable()
    }
    fn permission_original_always_cancellation_atomic_durable(&self) -> bool {
        (**self).permission_original_always_cancellation_atomic_durable()
    }
    fn capture_permission_original_always_cancellation(
        &self,
        selector: &super::PermissionOriginalAlwaysCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionOriginalAlwaysCancellationCapture, PermissionError> {
        (**self).capture_permission_original_always_cancellation(selector, budget)
    }
    fn commit_permission_original_always_cancellation(
        &self,
        prepared: &super::PermissionPreparedOriginalAlwaysCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionOriginalAlwaysCancellationCommit, PermissionError> {
        (**self).commit_permission_original_always_cancellation(prepared, budget)
    }
    fn permission_original_always_withheld_cancellation_atomic_durable(&self) -> bool {
        (**self).permission_original_always_withheld_cancellation_atomic_durable()
    }
    fn commit_permission_original_always_withheld_cancellation(
        &self,
        prepared: &super::PermissionPreparedOriginalAlwaysWithheldCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionOriginalAlwaysWithheldCancellationCommit, PermissionError> {
        (**self).commit_permission_original_always_withheld_cancellation(prepared, budget)
    }
    fn permission_use_withheld_cancellation_atomic_durable(&self) -> bool {
        (**self).permission_use_withheld_cancellation_atomic_durable()
    }
    fn capture_permission_use_withheld_cancellation(
        &self,
        selector: &super::PermissionUseWithheldCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseWithheldCancellationCapture, PermissionError> {
        (**self).capture_permission_use_withheld_cancellation(selector, budget)
    }
    fn commit_permission_use_withheld_cancellation(
        &self,
        prepared: &super::PermissionPreparedUseWithheldCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseWithheldCancellationCommit, PermissionError> {
        (**self).commit_permission_use_withheld_cancellation(prepared, budget)
    }

    fn permission_use_cancellation_atomic_durable(&self) -> bool {
        (**self).permission_use_cancellation_atomic_durable()
    }
    fn capture_permission_use_cancellation(
        &self,
        selector: &super::PermissionUseCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseCancellationCapture, PermissionError> {
        (**self).capture_permission_use_cancellation(selector, budget)
    }
    fn commit_permission_use_cancellation(
        &self,
        prepared: &super::PermissionPreparedUseCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseCancellationCommit, PermissionError> {
        (**self).commit_permission_use_cancellation(prepared, budget)
    }

    fn permission_use_ready_cancellation_atomic_durable(&self) -> bool {
        (**self).permission_use_ready_cancellation_atomic_durable()
    }
    fn capture_permission_use_ready_cancellation(
        &self,
        selector: &super::PermissionUseReadyCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseReadyCancellationCapture, PermissionError> {
        (**self).capture_permission_use_ready_cancellation(selector, budget)
    }
    fn commit_permission_use_ready_cancellation(
        &self,
        prepared: &super::PermissionPreparedUseReadyCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionUseReadyCancellationCommit, PermissionError> {
        (**self).commit_permission_use_ready_cancellation(prepared, budget)
    }

    fn capture_permission_original_always_current(
        &self,
        selector: &super::PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<ApprovalResource, PermissionError> {
        (**self).capture_permission_original_always_current(selector)
    }
    fn original_always_atomic_durable(&self) -> bool {
        (**self).original_always_atomic_durable()
    }

    fn capture_permission_original_always(
        &self,
        selector: &super::PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<super::PermissionOriginalAlwaysObservation, PermissionError> {
        (**self).capture_permission_original_always(selector)
    }
    fn commit_permission_original_always_outcome(
        &self,
        request: &super::PermissionOriginalAlwaysOutcomeRequest<'_>,
    ) -> Result<super::PermissionOriginalAlwaysCommit, PermissionError> {
        (**self).commit_permission_original_always_outcome(request)
    }

    fn capture_permission_use_outcome_current(
        &self,
        request: &super::PermissionUseOutcomeRequest<'_>,
        account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<ApprovalResource, PermissionError> {
        (**self).capture_permission_use_outcome_current(request, account)
    }

    fn capture_permission_use_outcome_source(
        &self,
        selector: &super::PermissionUseOutcomeSourceSelector<'_>,
        account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<super::PermissionUseOutcomePrepared, PermissionError> {
        account.source_port_inception()?;
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseOutcomeSourceSelector<'_>,
                    &mut super::PermissionUseOutcomeAccount
                )>() <= 128
            );
        }
        (**self).capture_permission_use_outcome_source(selector, account)
    }

    fn commit_permission_use_outcome(
        &self,
        request: &super::PermissionUseOutcomeRequest<'_>,
        account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<super::PermissionUseOutcomeCommit, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<Result<super::PermissionUseOutcomeCommit, PermissionError>>()
                    <= 8192
            );
            assert!(
                std::mem::size_of::<
                    std::ops::ControlFlow<
                        Result<std::convert::Infallible, PermissionError>,
                        super::PermissionUseOutcomeCommit,
                    >,
                >() <= 8192
            );
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseOutcomeRequest<'_>,
                    &mut super::PermissionUseOutcomeAccount
                )>() <= 128
            );
        }
        (**self).commit_permission_use_outcome(request, account)
    }

    fn durable_permissions(&self) -> bool {
        (**self).durable_permissions()
    }

    fn ready_authorization_atomic_durable(&self) -> bool {
        (**self).ready_authorization_atomic_durable()
    }

    fn permission_ready_authorization_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        (**self).permission_ready_authorization_resource(id, budget)
    }

    fn try_commit_permission_ready_authorization(
        &self,
        intent: &super::PermissionReadyAuthorizationIntent<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Option<crate::StoredExecution>, PermissionError> {
        (**self).try_commit_permission_ready_authorization(intent, budget)
    }

    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        (**self).permission_resource(id, budget)
    }

    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        (**self).permission_view(id, fence, selector, budget)
    }

    fn commit_permission_acceptance(
        &self,
        prepared: PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<crate::StoredExecution, PermissionError> {
        (**self).commit_permission_acceptance(prepared, budget)
    }

    fn capture_permission_use_renewal_resource(
        &self,
        selector: &super::PermissionUseRenewalResourceSelector<'_>,
        account: &mut super::PermissionUseRenewalAccount,
    ) -> Result<super::PermissionUseRenewalObservation, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseRenewalResourceSelector<'_>,
                    &mut super::PermissionUseRenewalAccount
                )>() <= 128
            );
        }
        (**self).capture_permission_use_renewal_resource(selector, account)
    }
    fn capture_permission_use_renewal_source(
        &self,
        selector: &super::PermissionUseRenewalSourceSelector<'_>,
        account: &mut super::PermissionUseRenewalAccount,
    ) -> Result<super::PermissionUseRenewalSourceCapture, PermissionError> {
        const {
            assert!(
                std::mem::size_of::<(
                    &Self,
                    &super::PermissionUseRenewalSourceSelector<'_>,
                    &mut super::PermissionUseRenewalAccount
                )>() <= 128
            );
        }
        (**self).capture_permission_use_renewal_source(selector, account)
    }

    fn capture_permission_runtime(
        &self,
        selector: &super::PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionRuntimeCapture, PermissionError> {
        (**self).capture_permission_runtime(selector, budget)
    }

    fn capture_permission_ready_cancellation(
        &self,
        selector: &super::PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionRuntimeCapture, PermissionError> {
        (**self).capture_permission_ready_cancellation(selector, budget)
    }

    fn commit_permission_ready_cancellation(
        &self,
        prepared: &super::PermissionPreparedReadyCancellation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<crate::StoredExecution, PermissionError> {
        (**self).commit_permission_ready_cancellation(prepared, budget)
    }

    fn commit_permission_runtime(
        &self,
        prepared: &super::PermissionPreparedRuntimeMutation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<super::PermissionRuntimeCommit, PermissionError> {
        (**self).commit_permission_runtime(prepared, budget)
    }
}

/// The nested approval value preserves the actual legacy refusal for the first
/// meaningful qualifier. Formatting emits only a fixed public code.
pub enum PermissionError {
    Approval(ApprovalError),
    Store(StoreError),
    Kernel(b4_kernel::KernelError),
    UnsupportedProfile,
    InvalidCatalog,
    ConstraintDenied,
    ConstraintUnavailable,
    CorruptRecord,
    LimitExceeded,
}

impl PermissionError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Approval(ApprovalError::UnsupportedAlways) => "permission_unsupported_always",
            Self::Approval(_) => "permission_approval_failed",
            Self::Store(_) => "permission_store_failed",
            Self::Kernel(_) => "permission_kernel_failed",
            Self::UnsupportedProfile => "permission_unsupported_profile",
            Self::InvalidCatalog => "permission_invalid_catalog",
            Self::ConstraintDenied => "permission_constraint_denied",
            Self::ConstraintUnavailable => "permission_constraint_unavailable",
            Self::CorruptRecord => "permission_corrupt_record",
            Self::LimitExceeded => "permission_limit_exceeded",
        }
    }
}

impl fmt::Debug for PermissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PermissionError")
            .field(&self.code())
            .finish()
    }
}

impl fmt::Display for PermissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for PermissionError {}

impl From<ApprovalError> for PermissionError {
    fn from(error: ApprovalError) -> Self {
        Self::Approval(error)
    }
}

impl From<StoreError> for PermissionError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<b4_kernel::KernelError> for PermissionError {
    fn from(error: b4_kernel::KernelError) -> Self {
        Self::Kernel(error)
    }
}

// Backend access is through the paid session; there are no public Prepared getters.
pub(super) struct PermissionPreparedParts<'outer, 'data> {
    pub(super) matched: &'outer PermissionView,
    pub(super) request: &'data crate::approval::ApprovalAcceptanceRequest,
    pub(super) catalog: &'data super::PermissionCatalog,
    pub(super) execution_id: &'data ExecutionId,
    pub(super) fence: ApprovalFence,
    pub(super) binding: &'outer super::catalog::PermissionCatalogCapture<'data>,
}
impl<'data> PermissionPreparedAcceptance<'data> {
    pub(super) fn writer_parts<'outer>(&'outer self) -> PermissionPreparedParts<'outer, 'data> {
        PermissionPreparedParts {
            matched: &self.matched,
            request: self.request,
            catalog: self.catalog,
            execution_id: self.execution_id,
            fence: self.fence,
            binding: &self.captured_binding,
        }
    }
}
