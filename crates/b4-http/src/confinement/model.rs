use super::{
    RecoveryConfinementBackendError, RecoveryConfinementReason, RecoveryConfinementReceipt,
};
use b4_runtime::ThreadView;
use std::fmt;

/// Trusted permission to confine this captured ordinary resource. This does
/// not supply a dispatch principal or repair missing durable approval evidence.
pub trait RecoveryConfinementAuthority: Send + Sync {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryConfinementAuthorityRefusal {
    Denied,
    Unavailable,
}
impl fmt::Display for RecoveryConfinementAuthorityRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Denied => "confinement_authority_denied",
            Self::Unavailable => "confinement_authority_unavailable",
        })
    }
}
impl std::error::Error for RecoveryConfinementAuthorityRefusal {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryConfinementRefusal {
    ExecutorUnavailable,
    AdmissionClosed,
    CapacityExhausted,
    ThreadBusy,
    UnsupportedHost,
    ResourceUnavailable,
    NotApplicable,
    ObservationChanged,
    AuthorityDenied,
    AuthorityUnavailable,
    Backend(RecoveryConfinementBackendError),
    Unavailable,
}
impl fmt::Display for RecoveryConfinementRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ExecutorUnavailable => "confinement_executor_unavailable",
            Self::AdmissionClosed => "confinement_host_draining",
            Self::CapacityExhausted => "confinement_capacity_exhausted",
            Self::ThreadBusy => "confinement_thread_busy",
            Self::UnsupportedHost => "confinement_unsupported_host",
            Self::ResourceUnavailable => "confinement_resource_unavailable",
            Self::NotApplicable => "confinement_not_applicable",
            Self::ObservationChanged => "confinement_observation_changed",
            Self::AuthorityDenied => "confinement_authority_denied",
            Self::AuthorityUnavailable => "confinement_authority_unavailable",
            Self::Backend(error) => return error.fmt(f),
            Self::Unavailable => "confinement_unavailable",
        })
    }
}
impl std::error::Error for RecoveryConfinementRefusal {}

/// Static failure plus optional verified returned mutation evidence. Absence
/// of a receipt never proves rollback of an ambiguous backend failure.
#[derive(Clone, PartialEq, Eq)]
pub struct RecoveryConfinementError {
    reason: RecoveryConfinementRefusal,
    confirmed_receipt: Option<RecoveryConfinementReceipt>,
}
impl RecoveryConfinementError {
    pub(super) fn new(
        reason: RecoveryConfinementRefusal,
        confirmed_receipt: Option<RecoveryConfinementReceipt>,
    ) -> Self {
        Self {
            reason,
            confirmed_receipt,
        }
    }

    pub fn reason(&self) -> RecoveryConfinementRefusal {
        self.reason
    }

    pub fn confirmed_receipt(&self) -> Option<&RecoveryConfinementReceipt> {
        self.confirmed_receipt.as_ref()
    }
}
impl fmt::Debug for RecoveryConfinementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryConfinementError")
            .field("reason", &self.reason)
            .field("has_confirmed_receipt", &self.confirmed_receipt.is_some())
            .finish()
    }
}
impl fmt::Display for RecoveryConfinementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}
impl std::error::Error for RecoveryConfinementError {}
