use super::{RecoveryAdmissionError, RecoveryAdoption, RecoveryRefusal};
use crate::{host::Shared, transport, worker::WorkerError};
use axum::{http::StatusCode, response::Response};
use b4_runtime::{approval::ApprovalError, RuntimeError, StoreError};
use std::sync::atomic::Ordering;

pub(crate) fn approval<S>(
    shared: &Shared<S>,
    error: ApprovalError,
    adoption: Option<RecoveryAdoption>,
) -> RecoveryAdmissionError {
    if transport::approval_failure_is_infrastructure(&error) {
        shared.fault.store(true, Ordering::Release);
        return RecoveryAdmissionError::new(RecoveryRefusal::Unavailable, adoption);
    }
    let reason = match error {
        ApprovalError::Unauthorized => RecoveryRefusal::ResourceUnavailable,
        ApprovalError::Store(StoreError::OwnerConflict) => RecoveryRefusal::RecoveryRequired,
        ApprovalError::Store(StoreError::ExecutionNotFound | StoreError::ExecutionNotActive) => {
            RecoveryRefusal::ExecutionChanged
        }
        ApprovalError::Store(error) => return store(shared, error, adoption),
        ApprovalError::NotStablePark => RecoveryRefusal::ResumeRequired,
        ApprovalError::RouteMismatch => RecoveryRefusal::RouteMismatch,
        ApprovalError::RecoveryNotNeeded => RecoveryRefusal::RecoveryNotNeeded,
        ApprovalError::RecoveryDefinitionMismatch => RecoveryRefusal::DefinitionUnavailable,
        ApprovalError::InterruptSetMismatch => RecoveryRefusal::InterruptSetMismatch,
        ApprovalError::UnsupportedAlways => RecoveryRefusal::AlwaysUnsupported,
        ApprovalError::MissingToken => RecoveryRefusal::GrantRequired,
        ApprovalError::Expired => RecoveryRefusal::GrantExpired,
        ApprovalError::Consumed => RecoveryRefusal::GrantConsumed,
        ApprovalError::GrantUnavailable => RecoveryRefusal::GrantUnavailable,
        ApprovalError::ProposalTooLarge | ApprovalError::InvalidProposal => {
            RecoveryRefusal::ProposalLimit
        }
        _ => RecoveryRefusal::GrantInvalid,
    };
    RecoveryAdmissionError::new(reason, adoption)
}
fn store<S>(
    shared: &Shared<S>,
    error: StoreError,
    adoption: Option<RecoveryAdoption>,
) -> RecoveryAdmissionError {
    let reason = match error {
        StoreError::ThreadNotFound => RecoveryRefusal::ResourceUnavailable,
        StoreError::ThreadBusy => RecoveryRefusal::ThreadBusy,
        StoreError::RevisionConflict | StoreError::ThreadExists => RecoveryRefusal::ResourceChanged,
        _ => {
            shared.fault.store(true, Ordering::Release);
            RecoveryRefusal::Unavailable
        }
    };
    RecoveryAdmissionError::new(reason, adoption)
}
pub(crate) fn worker<S>(
    shared: &Shared<S>,
    error: WorkerError,
    adoption: Option<RecoveryAdoption>,
) -> RecoveryAdmissionError {
    match error {
        WorkerError::Approval(error) => approval(shared, error, adoption),
        WorkerError::Runtime(RuntimeError::Store(error)) => store(shared, error, adoption),
        WorkerError::Draining => {
            RecoveryAdmissionError::new(RecoveryRefusal::AdmissionClosed, adoption)
        }
        _ => {
            shared.fault.store(true, Ordering::Release);
            RecoveryAdmissionError::new(RecoveryRefusal::Unavailable, adoption)
        }
    }
}
pub(crate) fn response(error: RecoveryAdmissionError) -> Response {
    use RecoveryRefusal::*;
    let status = match error.reason() {
        AdmissionClosed | CapacityExhausted | Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Unsupported => StatusCode::NOT_IMPLEMENTED,
        ResourceUnavailable => StatusCode::NOT_FOUND,
        AlwaysUnsupported | GrantRequired | ProposalLimit => StatusCode::BAD_REQUEST,
        GrantInvalid => StatusCode::FORBIDDEN,
        _ => StatusCode::CONFLICT,
    };
    // Display is selected from the same finite static mapping; no underlying
    // store/error string or sensitive adoption identity enters the envelope.
    let code = match error.reason() {
        AdmissionClosed => "host_draining",
        Unsupported => "recovery_unsupported",
        ResourceUnavailable => "resource_unavailable",
        CapacityExhausted => "capacity_exhausted",
        ThreadBusy => "thread_busy",
        ResourceChanged => "resource_changed",
        ExecutionChanged => "execution_changed",
        RecoveryRequired => "recovery_required",
        RecoveryNotNeeded => "recovery_not_needed",
        DefinitionUnavailable => "recovery_definition_unavailable",
        RouteMismatch => "route_mismatch",
        ResumeRequired => "resume_required",
        InterruptSetMismatch => "interrupt_set_mismatch",
        AlwaysUnsupported => "always_unsupported",
        GrantRequired => "grant_required",
        GrantExpired => "grant_expired",
        GrantConsumed => "grant_consumed",
        GrantUnavailable => "grant_unavailable",
        ProposalLimit => "approval_proposal_limit",
        GrantInvalid => "grant_invalid",
        Unavailable => "host_unready",
    };
    transport::error(status, code)
}
