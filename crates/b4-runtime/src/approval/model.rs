use crate::{
    ExecutionFence, RuntimeError, StoreError, StoredExecution, ThreadOrigin, ThreadRecord,
};
use b4_contract::*;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt;

pub const MAX_APPROVAL_BYTES: usize = 65_536;
/// Maximum nested object/array containers, excluding scalar leaves.
pub const MAX_APPROVAL_JSON_DEPTH: usize = 64;
pub const MAX_APPROVAL_TTL_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ApprovalToken(String);
impl ApprovalToken {
    pub fn new(value: impl Into<String>) -> Result<Self, ApprovalError> {
        let value = value.into();
        if value.len() != 69
            || !value.starts_with("b4ag_")
            || !value.as_bytes()[5..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(ApprovalError::InvalidGrant);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for ApprovalToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApprovalToken([REDACTED])")
    }
}
impl<'de> Deserialize<'de> for ApprovalToken {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?)
            .map_err(|_| serde::de::Error::custom("invalid_approval_token"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalContext {
    pub actor: ThreadOrigin,
    pub policy_revision: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalOperation {
    Park,
    Pending,
    AttachmentPending,
    Accept,
    Dispatch,
    Recover,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalResource {
    pub thread: ThreadRecord,
    pub execution: StoredExecution,
}
pub trait ApprovalAuthority: Send + Sync {
    fn authorize(
        &self,
        context: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError>;
}
impl<A: ApprovalAuthority + ?Sized> ApprovalAuthority for std::sync::Arc<A> {
    fn authorize(
        &self,
        context: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        (**self).authorize(context, operation, resource)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalFence {
    pub execution: ExecutionFence,
    pub resource_revision: u64,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantRecord {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub origin: ThreadOrigin,
    pub execution_id: ExecutionId,
    pub route: RouteKey,
    pub definition_revision: ExecutionRevision,
    pub task_id: TaskId,
    pub effect_id: EffectId,
    pub interrupt_id: InterruptId,
    pub resume_key: String,
    pub task_parking_revision: u64,
    pub policy_revision: String,
    pub proposal_digest: String,
    pub prompt: Value,
    pub token: ApprovalToken,
    pub verifier: [u8; 32],
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub consumed: bool,
}
impl fmt::Debug for GrantRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrantRecord")
            .field("execution_id", &self.execution_id)
            .field("task_id", &self.task_id)
            .field("interrupt_id", &self.interrupt_id)
            .field("consumed", &self.consumed)
            .field("sensitive", &"[REDACTED]")
            .finish()
    }
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SensitivePendingApproval {
    pub task_id: TaskId,
    pub interrupt_id: InterruptId,
    pub prompt: Value,
    pub token: ApprovalToken,
    pub resume_key: String,
    pub expires_at_ms: u64,
}
impl fmt::Debug for SensitivePendingApproval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SensitivePendingApproval")
            .field("task_id", &self.task_id)
            .field("interrupt_id", &self.interrupt_id)
            .field("sensitive", &"[REDACTED]")
            .finish()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantDecision {
    pub interrupt_id: InterruptId,
    pub decision: ApprovalDecision,
    pub cancelled: bool,
    pub token: Option<ApprovalToken>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalAcceptanceRequest {
    pub context: ApprovalContext,
    pub route: RouteKey,
    pub definition_revision: ExecutionRevision,
    pub turn_id: TurnId,
    pub now_ms: u64,
    pub decisions: Vec<GrantDecision>,
}
/// Trusted known-ID recovery of a prior store owner. Ordinary token-free thread
/// identity and both captured fences are mandatory; user metadata is not authority.
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalRecoveryRequest {
    pub context: ApprovalContext,
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub available_definition: Definition,
    pub turn_id: TurnId,
    pub facts: TransitionFacts,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedDecision {
    pub task_id: TaskId,
    pub interrupt_id: InterruptId,
    pub decision: ApprovalDecision,
    pub proposal_digest: String,
    pub task_parking_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalAcceptance {
    pub execution_id: ExecutionId,
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub context: ApprovalContext,
    pub previous_revision: u64,
    pub turn_id: TurnId,
    pub definition_revision: ExecutionRevision,
    pub accepted_at_ms: u64,
    pub decisions: Vec<AcceptedDecision>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalView {
    pub thread: ThreadRecord,
    pub execution: StoredExecution,
    pub grants: Vec<GrantRecord>,
    pub acceptances: Vec<ApprovalAcceptance>,
}
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // A single transaction owns its bounded sensitive record.
pub enum ApprovalMutation {
    Issue(GrantRecord),
    Accept(ApprovalAcceptanceRequest),
    Recover(ApprovalRecoveryRequest),
}
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalChanges {
    pub grants: Vec<GrantRecord>,
    pub acceptance: Option<ApprovalAcceptance>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ApprovalError {
    Store(StoreError),
    Kernel(b4_kernel::KernelError),
    Runtime(RuntimeError),
    Unauthorized,
    InvalidContext,
    DurabilityRequired,
    NotStablePark,
    RouteMismatch,
    InterruptSetMismatch,
    UnsupportedAlways,
    GrantUnavailable,
    MissingToken,
    InvalidGrant,
    Consumed,
    Expired,
    InvalidTtl,
    ProposalTooLarge,
    InvalidProposal,
    RandomUnavailable,
    RecoveryNotNeeded,
    RecoveryDefinitionMismatch,
}
impl fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ApprovalError {}
impl From<StoreError> for ApprovalError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}
impl From<b4_kernel::KernelError> for ApprovalError {
    fn from(error: b4_kernel::KernelError) -> Self {
        Self::Kernel(error)
    }
}
impl From<RuntimeError> for ApprovalError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
