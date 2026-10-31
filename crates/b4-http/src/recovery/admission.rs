use b4_contract::{ExecutionId, ThreadGeneration, ThreadId, TurnId};
use b4_runtime::ExecutionFence;

/// Confirmed evidence returned by the actual fenced durable recovery operation.
/// These identities are sensitive embedding data; no application resource is
/// retained. Admission does not establish callback entry or physical retirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryAdoption {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub turn_id: TurnId,
    pub execution_fence: ExecutionFence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDisposition {
    Runnable,
    Parked,
    Completed,
    Cancelled,
    Failed,
    Blocked,
    AttemptLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryAdmission {
    pub adoption: RecoveryAdoption,
    pub disposition: RecoveryDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryRefusal {
    AdmissionClosed,
    Unsupported,
    ResourceUnavailable,
    CapacityExhausted,
    ThreadBusy,
    ResourceChanged,
    ExecutionChanged,
    RecoveryRequired,
    RecoveryNotNeeded,
    DefinitionUnavailable,
    RouteMismatch,
    ResumeRequired,
    InterruptSetMismatch,
    AlwaysUnsupported,
    GrantRequired,
    GrantExpired,
    GrantConsumed,
    GrantUnavailable,
    ProposalLimit,
    GrantInvalid,
    Unavailable,
}

/// A static refusal plus optional confirmed adoption evidence.
/// Absence of a receipt never proves rollback or absence of a durable commit.
#[derive(Clone, PartialEq, Eq)]
pub struct RecoveryAdmissionError {
    reason: RecoveryRefusal,
    adoption: Option<RecoveryAdoption>,
}
impl RecoveryAdmissionError {
    pub(crate) fn new(reason: RecoveryRefusal, adoption: Option<RecoveryAdoption>) -> Self {
        Self { reason, adoption }
    }
    pub fn reason(&self) -> RecoveryRefusal {
        self.reason
    }
    /// Return sensitive confirmed identities explicitly, without logging them.
    pub fn adoption(&self) -> Option<&RecoveryAdoption> {
        self.adoption.as_ref()
    }
}
impl std::fmt::Debug for RecoveryAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecoveryAdmissionError")
    }
}
impl std::fmt::Display for RecoveryAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.reason {
            RecoveryRefusal::AdmissionClosed => "host_draining",
            RecoveryRefusal::Unsupported => "recovery_unsupported",
            RecoveryRefusal::ResourceUnavailable => "resource_unavailable",
            RecoveryRefusal::CapacityExhausted => "capacity_exhausted",
            RecoveryRefusal::ThreadBusy => "thread_busy",
            RecoveryRefusal::ResourceChanged => "resource_changed",
            RecoveryRefusal::ExecutionChanged => "execution_changed",
            RecoveryRefusal::RecoveryRequired => "recovery_required",
            RecoveryRefusal::RecoveryNotNeeded => "recovery_not_needed",
            RecoveryRefusal::DefinitionUnavailable => "recovery_definition_unavailable",
            RecoveryRefusal::RouteMismatch => "route_mismatch",
            RecoveryRefusal::ResumeRequired => "resume_required",
            RecoveryRefusal::InterruptSetMismatch => "interrupt_set_mismatch",
            RecoveryRefusal::AlwaysUnsupported => "always_unsupported",
            RecoveryRefusal::GrantRequired => "grant_required",
            RecoveryRefusal::GrantExpired => "grant_expired",
            RecoveryRefusal::GrantConsumed => "grant_consumed",
            RecoveryRefusal::GrantUnavailable => "grant_unavailable",
            RecoveryRefusal::ProposalLimit => "approval_proposal_limit",
            RecoveryRefusal::GrantInvalid => "grant_invalid",
            RecoveryRefusal::Unavailable => "host_unready",
        })
    }
}
impl std::error::Error for RecoveryAdmissionError {}
