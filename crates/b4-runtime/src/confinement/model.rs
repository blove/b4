use super::RecoveryConfinementReason;
use crate::{ExecutionFence, ThreadView};
use b4_contract::{ExecutionId, ExecutionRevision, RouteKey, ThreadGeneration, ThreadId, TurnId};
use std::fmt;

/// Sensitive, resource-free identity captured from an ordinary coherent view.
/// Capturing a target neither bounds the supplied view nor authorizes mutation.
#[derive(Clone, PartialEq, Eq)]
pub struct RecoveryConfinementTarget {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub resource_revision: u64,
    pub execution_id: ExecutionId,
    pub execution_fence: ExecutionFence,
    pub turn_id: TurnId,
}

impl RecoveryConfinementTarget {
    pub fn from_view(view: &ThreadView) -> Result<Self, RecoveryConfinementBackendError> {
        match (&view.thread.active_execution_id, &view.execution) {
            (None, None) => Err(RecoveryConfinementBackendError::NotActive),
            (Some(id), Some(execution))
                if execution.thread_id == view.thread.id
                    && &execution.snapshot.execution_id == id =>
            {
                Ok(Self {
                    thread_id: view.thread.id.clone(),
                    thread_generation: view.thread.generation.clone(),
                    resource_revision: view.thread.resource_revision,
                    execution_id: id.clone(),
                    execution_fence: (&execution.snapshot).into(),
                    turn_id: execution.snapshot.turn_id.clone(),
                })
            }
            _ => Err(RecoveryConfinementBackendError::CorruptRecord),
        }
    }
}

impl fmt::Debug for RecoveryConfinementTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecoveryConfinementTarget")
    }
}

/// Confirmed identities returned by the actual durable confinement operation.
/// This pure receipt does not establish readiness or physical retirement.
#[derive(Clone, PartialEq, Eq)]
pub struct RecoveryConfinementReceipt {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub original_fence: ExecutionFence,
    pub new_fence: ExecutionFence,
    pub original_resource_revision: u64,
    pub new_resource_revision: u64,
    pub original_turn_id: TurnId,
    pub fresh_turn_id: TurnId,
    pub route: RouteKey,
    pub definition_revision: ExecutionRevision,
    pub reason: RecoveryConfinementReason,
}

impl fmt::Debug for RecoveryConfinementReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecoveryConfinementReceipt")
    }
}

/// A returned durable change or a fully audited unchanged prior-owner block.
#[derive(Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // The confirmed receipt stays owned and resource-free.
pub enum RecoveryConfinementOutcome {
    Confined(RecoveryConfinementReceipt),
    AlreadyBlocked,
}

impl fmt::Debug for RecoveryConfinementOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Confined(_) => "RecoveryConfinementOutcome::Confined",
            Self::AlreadyBlocked => "RecoveryConfinementOutcome::AlreadyBlocked",
        })
    }
}

/// Static backend refusals without source data or underlying diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryConfinementBackendError {
    UnsupportedOperation,
    NotFound,
    NotActive,
    TargetChanged,
    OwnerConflict,
    Terminal,
    InvalidTurn,
    ClockRegression,
    LimitExceeded,
    UnsupportedSnapshot,
    UnsupportedAuthorityProfile,
    CorruptRecord,
    Unavailable,
}

impl fmt::Display for RecoveryConfinementBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsupportedOperation => "confinement_unsupported_operation",
            Self::NotFound => "confinement_not_found",
            Self::NotActive => "confinement_not_active",
            Self::TargetChanged => "confinement_target_changed",
            Self::OwnerConflict => "confinement_owner_conflict",
            Self::Terminal => "confinement_terminal",
            Self::InvalidTurn => "confinement_invalid_turn",
            Self::ClockRegression => "confinement_clock_regression",
            Self::LimitExceeded => "confinement_limit_exceeded",
            Self::UnsupportedSnapshot => "confinement_unsupported_snapshot",
            Self::UnsupportedAuthorityProfile => "confinement_unsupported_authority_profile",
            Self::CorruptRecord => "confinement_corrupt_record",
            Self::Unavailable => "confinement_unavailable",
        })
    }
}

impl std::error::Error for RecoveryConfinementBackendError {}
