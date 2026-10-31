//! Catalog constraints and bounded staged permission operations.
//!
//! Once acceptance uses the ordinary durable grant transaction. The staged
//! Always route persists its first acceptance and permission decision, and
//! reads the resulting Ready state on the original writer or after reopening.
//! The durable permission capability remains false.

mod catalog;
mod model;
mod ready_authorization;
mod service;
mod validation;
mod view;

pub use b4_contract::permissions::{
    PermissionId, PermissionIdentityError, PermissionUseId, StableToolId, StableToolRevision,
    ToolConstraintRevision,
};
pub use catalog::*;
pub use model::*;
pub use ready_authorization::{
    try_authorize_permission_ready, PermissionReadyAuthorizationCurrentObservation,
    PermissionReadyAuthorizationCurrentRead, PermissionReadyAuthorizationIntent,
    PermissionReadyAuthorizationReadPhase, PermissionReadyAuthorizationRequest,
    PermissionReadyAuthorizationSource, PermissionReadyAuthorizationSqlBindings,
    PermissionReadyAuthorizationWork, PermissionReadyAuthorizationWriteSql,
    PermissionReadyAuthorizationWriterSession,
};
pub use service::read_permission_view;
pub use validation::*;
pub use view::*;

use crate::approval::{
    ApprovalAcceptanceRequest, ApprovalAuthority, ApprovalError, ApprovalFence, ApprovalService,
};
use crate::{InvocationContext, StoredExecution};
use b4_contract::{ApprovalDecision, ExecutionId, TaskStatus};
use std::sync::Arc;

/// Accept an original Always grant with bounded normal resource IDs.
/// Uses the same current authority, catalogue gate and typed atomic writer as
/// the staged human entry. This entry never falls back to Once acceptance.
pub fn accept_permission_always_with_normal_inputs<S, A>(
    store: Arc<S>,
    authority: Arc<A>,
    catalog: Arc<PermissionCatalog>,
    execution_id: &ExecutionId,
    fence: ApprovalFence,
    request: ApprovalAcceptanceRequest,
) -> Result<StoredExecution, PermissionError>
where
    S: PermissionStore + 'static,
    A: ApprovalAuthority + ?Sized + 'static,
{
    let mut budget = PermissionEvidenceBudget::for_normal_original_always();
    if !validation::permission_human_uses_always_stage(&request, &mut budget)? {
        return Err(PermissionError::UnsupportedProfile);
    }
    service::accept_permission_always_stage(
        store.as_ref(),
        authority.as_ref(),
        catalog.as_ref(),
        execution_id,
        fence,
        &request,
        &mut budget,
    )
}

pub fn accept_permission_human<S, A>(
    store: Arc<S>,
    authority: Arc<A>,
    catalog: Arc<PermissionCatalog>,
    execution_id: &ExecutionId,
    fence: ApprovalFence,
    request: ApprovalAcceptanceRequest,
) -> Result<StoredExecution, PermissionError>
where
    S: PermissionStore + 'static,
    A: ApprovalAuthority + ?Sized + 'static,
{
    // Always uses one shared account for the staged acceptance transaction.
    let mut budget = PermissionEvidenceBudget::new();
    if validation::permission_human_uses_always_stage(&request, &mut budget)? {
        return service::accept_permission_always_stage(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            execution_id,
            fence,
            &request,
            &mut budget,
        );
    }
    ApprovalService::new(store, authority)?.accept_checked(
        execution_id,
        fence,
        request,
        |view, request| {
            let current = &view.execution.snapshot;
            let context = InvocationContext {
                thread_id: view.thread.id.clone(),
                thread_generation: view.thread.generation.clone(),
                origin: view
                    .thread
                    .origin
                    .as_ref()
                    .ok_or(ApprovalError::Unauthorized)?
                    .clone(),
                application_revision: catalog.application_identity().revision.clone(),
                execution_revision: current.definition.revision.clone(),
                policy_revision: request.context.policy_revision.clone(),
            };
            for task in current
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::WaitingForApproval)
            {
                let interrupt = task.interrupt.as_ref().ok_or(ApprovalError::InvalidGrant)?;
                let decision = request
                    .decisions
                    .iter()
                    .find(|entry| entry.interrupt_id == interrupt.id)
                    .ok_or(ApprovalError::InterruptSetMismatch)?;
                if !decision.cancelled && decision.decision == ApprovalDecision::Once {
                    catalog.validate_call(
                        &context,
                        &current.definition,
                        task,
                        PermissionCallPhase::Unclaimed,
                    )?;
                }
            }
            Ok(())
        },
    )
}

mod original_always_cancellation;
mod original_always_cancellation_relation;
mod runtime_mutation;
mod runtime_use;
mod use_cancellation;
mod use_cancellation_relation;
mod use_ready_cancellation;
pub use original_always_cancellation::*;
pub use runtime_mutation::{
    PermissionDispatchClaim, PermissionDispatchPermit, PermissionPreparedRuntimeMutation,
    PermissionRuntimeBindings, PermissionRuntimeCapture, PermissionRuntimeCommit,
    PermissionRuntimeJournalPoint, PermissionRuntimeReadSession, PermissionRuntimeReadSql,
    PermissionRuntimeSelector, PermissionRuntimeWork, PermissionRuntimeWriteSession,
    PermissionRuntimeWriteSql,
};
pub use runtime_use::{
    PermissionRuntimeEvidenceKind, PermissionRuntimeUseEvidence, PermissionRuntimeUseHeaderCells,
    PermissionRuntimeUseReadSql, PermissionRuntimeUseSession, PermissionRuntimeUseSource,
};
pub use use_cancellation::*;
pub use use_ready_cancellation::*;

pub use runtime_mutation::prepare_permission_dispatch_claim;

pub use runtime_mutation::{
    cancel_permission_ready, cancel_permission_ready_with_normal_inputs,
    cancel_permission_ready_with_normal_inputs_confirmed, PermissionPreparedReadyCancellation,
};

pub use runtime_mutation::{
    PermissionReadyCancellationSensitiveRead, PermissionReadyCancellationWriteSql,
    PermissionRuntimeReadyCancellationWork, PermissionRuntimeReadyCancellationWriteSession,
};

mod runtime_use_renewal;
pub use runtime_use_renewal::{
    renew_permission_use_dispatch, PermissionUseDispatchPermit, PermissionUseRenewalAccount,
    PermissionUseRenewalObservation, PermissionUseRenewalResourceSelector,
    PermissionUseRenewalResourceSession, PermissionUseRenewalSource,
    PermissionUseRenewalSourceCapture, PermissionUseRenewalSourceSelector,
    PermissionUseRenewalSourceSession,
};

mod runtime_use_callback;
pub use runtime_use_callback::{
    PermissionUseCallbackReceipt, PermissionUseCallbackStatus, PermissionUseCallbackWork,
};

mod runtime_use_outcome;
pub use runtime_use_outcome::{
    PermissionUseOutcomeAccount, PermissionUseOutcomeCommit, PermissionUseOutcomeRequest,
    PermissionUseOutcomeWork,
};

mod runtime_use_outcome_source;
pub use runtime_use_outcome_source::{
    PermissionUseOutcomePrepared, PermissionUseOutcomeSource, PermissionUseOutcomeSourceSelector,
    PermissionUseOutcomeSourceSession,
};

mod runtime_use_outcome_writer;
pub use runtime_use_outcome_writer::{
    PermissionUseOutcomeEncodedJournal, PermissionUseOutcomeEncodedSnapshot,
    PermissionUseOutcomeExecutionCasLoan, PermissionUseOutcomeImmediateSourceCapture,
    PermissionUseOutcomeImmediateSourceSession, PermissionUseOutcomeJournalInsertLoan,
    PermissionUseOutcomePendingCommit, PermissionUseOutcomeThreadCasLoan,
    PermissionUseOutcomeWriteSelector, PermissionUseOutcomeWriteSession,
};

mod driver_adapter;
mod driver_adapter_account;
pub use driver_adapter::{
    drive_permission_use_cancellable_execution, drive_permission_use_execution,
    PermissionUseDriverBindings, PermissionUseDriverFailure, PermissionUseDriverHooks,
};
pub use driver_adapter_account::PermissionUseDriverUsage;

pub use driver_adapter::{
    drive_permission_original_always_cancellable_execution,
    drive_permission_original_always_execution, PermissionOriginalAlwaysDriverBindings,
    PermissionOriginalAlwaysDriverFailure,
};
pub use runtime_mutation::{
    PermissionOriginalAlwaysCommit, PermissionOriginalAlwaysObservation,
    PermissionOriginalAlwaysOutcomeRequest, PermissionOriginalAlwaysRead,
    PermissionOriginalAlwaysSelector,
};

mod ready_confirmation;
pub use ready_confirmation::PermissionReadyCancellationConfirmation;
mod use_withheld_cancellation;
pub use use_withheld_cancellation::*;

pub use runtime_mutation::{
    PermissionOriginalAlwaysWithheldCancellationCommit,
    PermissionOriginalAlwaysWithheldCancellationRequest,
    PermissionPreparedOriginalAlwaysWithheldCancellation,
};

mod thread_deletion;
pub use thread_deletion::*;
