//! Bounded, canonical permission-record structure and retained-call witnesses.
//!
//! These pure records confer no store, principal, policy or callback authority.
//! New metadata is closed; old nested null and omission semantics are preserved.

use super::{
    PermissionError, PermissionId, PermissionUseId, StableToolId, StableToolRevision,
    ToolConstraintRevision,
};
use crate::{ApplicationIdentity, ThreadOrigin};
use b4_contract::{
    ApprovalDecision, Definition, EffectId, ExecutionId, InterruptId, PendingApproval, RouteKey,
    Snapshot, TaskId, TaskRecord, TaskSpec, ThreadGeneration, ThreadId, TransitionFacts, TurnId,
};
use serde::Deserialize;
use std::fmt;

/// Cumulative charged evidence bytes and structural work nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionEvidenceUsage {
    encoded_bytes: usize,
    structural_nodes: usize,
}

impl PermissionEvidenceUsage {
    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub fn structural_nodes(&self) -> usize {
        self.structural_nodes
    }
}

// An invocation-local input shape, never a public capability or mutable budget knob.
#[derive(Clone, Copy)]
enum OriginalAlwaysProfile {
    Strict,
    NormalOriginalAlways,
}

struct OriginalAlwaysLimits {
    before: usize,
    before_nodes: usize,
    resume: usize,
    resume_nodes: usize,
    acceptance: usize,
    acceptance_nodes: usize,
    decision: usize,
    grant: usize,
    scope: usize,
    thread: usize,
    journal: usize,
    equality: usize,
    paired_equality_nodes: usize,
    encoded_total: usize,
}

impl OriginalAlwaysProfile {
    fn limits(self) -> OriginalAlwaysLimits {
        match self {
            Self::Strict => OriginalAlwaysLimits {
                before: 848,
                before_nodes: 91,
                resume: 960,
                resume_nodes: 96,
                acceptance: 1_766,
                acceptance_nodes: 154,
                decision: 998,
                grant: 962,
                scope: 648,
                thread: 512,
                journal: 160,
                equality: 14_000,
                paired_equality_nodes: 1_200,
                encoded_total: 6_500,
            },
            Self::NormalOriginalAlways => OriginalAlwaysLimits {
                before: 8_192,
                before_nodes: 512,
                resume: 8_192,
                resume_nodes: 512,
                acceptance: 16_384,
                acceptance_nodes: 768,
                decision: 4_096,
                grant: 4_096,
                scope: 2_048,
                thread: 1_024,
                journal: 1_024,
                // Independent bound on the nine complete paired graph operands.
                equality: 57_600,
                paired_equality_nodes: 4_096,
                encoded_total: 16_384,
            },
        }
    }
}

/// Fixed, non-cloneable cumulative evidence account.
pub struct PermissionEvidenceBudget {
    pub(super) ready_preclaim_fence_refused: bool,
    pub(super) ready_driver_fence: Option<crate::approval::ApprovalFence>,
    usage: PermissionEvidenceUsage,
    // Immutable operation purpose and byte ceiling; only fresh constructors set it.
    bytes_limit: usize,
    // Constructor-sealed deletion purpose; never exposed through a mutable account.
    settled_thread_deletion: bool,
    original_always_profile: OriginalAlwaysProfile,
    exhausted: bool,
    resource: ResourceLedger,
    // Private move-stable provenance for paid Accept measurements only.
    acceptance_identity: Option<std::sync::Arc<()>>,
    runtime_state: Option<super::runtime_mutation::PermissionRuntimeState>,
    // A single monotone local use-entry session; it carries no authority/seal.
    ready_authorization_entered: bool,
    ready_authorization_writer_entered: bool,
    runtime_use_entered: bool,
    // Constructor-sealed renewal purpose; only private renewal entry activates it.
    use_renewal_scaffold_entered: bool,
    use_renewal_production_entered: bool,
}

impl PermissionEvidenceBudget {
    // Default is intentionally excluded from the accepted account API.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            ready_preclaim_fence_refused: false,
            ready_driver_fence: None,
            bytes_limit: ACCOUNT_BYTES,
            settled_thread_deletion: false,
            original_always_profile: OriginalAlwaysProfile::Strict,
            usage: PermissionEvidenceUsage {
                encoded_bytes: 0,
                structural_nodes: 0,
            },
            exhausted: false,
            resource: ResourceLedger::new(),
            acceptance_identity: None,
            runtime_state: None,
            ready_authorization_entered: false,
            ready_authorization_writer_entered: false,
            runtime_use_entered: false,
            use_renewal_scaffold_entered: false,
            use_renewal_production_entered: false,
        }
    }

    /// Creates a fresh, bounded evidence allowance for entered cancellation.
    /// This grants no Store, policy or mutation authority. It cannot reset or
    /// clone a spent account; all existing evidence and node bounds still apply.
    pub fn for_entered_cancellation() -> Self {
        Self {
            bytes_limit: ENTERED_CANCELLATION_ACCOUNT_BYTES,
            ..Self::new()
        }
    }

    /// Fresh bounded account for typed Ready cancellation.
    pub fn for_ready_cancellation() -> Self {
        Self::for_entered_cancellation()
    }

    // The genuine normal Always producer and original Driver select this profile.
    // Unrelated larger accounts do not select it through their numeric ceilings.
    pub(super) fn for_normal_original_always() -> Self {
        Self {
            original_always_profile: OriginalAlwaysProfile::NormalOriginalAlways,
            ..Self::new()
        }
    }

    fn original_always_limits(&self) -> OriginalAlwaysLimits {
        self.original_always_profile.limits()
    }

    // Additional variable work for this profile is purchased from actual paid
    // measurements, before the corresponding owner/copy/comparison work. Fixed
    // Strict quotations below remain literal and are never refunded or replaced.
    fn reserve_normal_original_work(
        &mut self,
        measures: &[CanonicalMeasure],
        byte_passes: usize,
        node_passes: usize,
    ) -> Result<(), PermissionError> {
        if matches!(
            self.original_always_profile,
            OriginalAlwaysProfile::NormalOriginalAlways
        ) {
            let (bytes, nodes) = measures.iter().try_fold((0, 0), |(bytes, nodes), q| {
                Ok::<_, PermissionError>((add(bytes, q.bytes)?, add(nodes, q.nodes)?))
            })?;
            self.reserve(mul(bytes, byte_passes)?, mul(nodes, node_passes)?)?;
        }
        Ok(())
    }

    /// Construct a fresh account for the separately bounded Use Dispatch profile.
    /// General permission operations refuse this account before lending authority.
    pub fn for_use_dispatch() -> Self {
        Self {
            bytes_limit: USE_DISPATCH_ACCOUNT_BYTES,
            ..Self::new()
        }
    }

    // A larger account grants no store, policy or mutation authority.
    pub(super) fn is_use_dispatch_account(&self) -> bool {
        self.bytes_limit == USE_DISPATCH_ACCOUNT_BYTES
    }

    // The complete current-source primitive buys its normal metadata and original
    // account loan before inspection; trusted inception covers a refused purchase.
    pub(super) fn observe_use_dispatch_account(&mut self) -> Result<bool, PermissionError> {
        self.reserve(392, 2)?;
        Ok(self.is_use_dispatch_account())
    }

    pub(super) fn require_standard_account(&mut self) -> Result<(), PermissionError> {
        if self.observe_use_dispatch_account()? {
            Err(PermissionError::UnsupportedProfile)
        } else {
            Ok(())
        }
    }

    pub(super) fn require_dispatch_resource_account(&mut self) -> Result<(), PermissionError> {
        if self.observe_use_dispatch_account()? {
            let state = self
                .runtime_state
                .as_ref()
                .ok_or(PermissionError::UnsupportedProfile)?;
            if !matches!(
                state.kind,
                super::runtime_mutation::PermissionRuntimeOperationKind::Dispatch
            ) {
                return Err(PermissionError::UnsupportedProfile);
            }
        }
        Ok(())
    }

    pub fn usage(&self) -> PermissionEvidenceUsage {
        self.usage
    }
}

impl fmt::Debug for PermissionEvidenceBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermissionEvidenceBudget")
            .field("usage", &self.usage)
            .finish()
    }
}

// A customized field decoder without a default requires its key to be present.
// An explicitly present null remains None; old nested optional rules are unchanged.
fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(PartialEq)]
pub struct PermissionApplicationIdentity {
    application: String,
    revision: String,
}

impl PermissionApplicationIdentity {
    pub fn application(&self) -> &str {
        &self.application
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub fn from_native(identity: &ApplicationIdentity) -> Result<Self, PermissionError> {
        if !valid_label(&identity.application) || !valid_label(&identity.revision) {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(Self {
            application: identity.application.clone(),
            revision: identity.revision.clone(),
        })
    }
}

impl fmt::Debug for PermissionApplicationIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionApplicationIdentity([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationWire {
    application: String,
    revision: String,
}

impl From<ApplicationWire> for PermissionApplicationIdentity {
    fn from(wire: ApplicationWire) -> Self {
        Self {
            application: wire.application,
            revision: wire.revision,
        }
    }
}

#[derive(PartialEq)]
pub struct PermissionBindingIdentity {
    effect_id: EffectId,
    tool_id: StableToolId,
    tool_revision: StableToolRevision,
    constraint_revision: ToolConstraintRevision,
}

impl PermissionBindingIdentity {
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }

    pub fn tool_id(&self) -> &StableToolId {
        &self.tool_id
    }

    pub fn tool_revision(&self) -> &StableToolRevision {
        &self.tool_revision
    }

    pub fn constraint_revision(&self) -> &ToolConstraintRevision {
        &self.constraint_revision
    }
}

impl fmt::Debug for PermissionBindingIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionBindingIdentity([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingWire {
    effect_id: EffectId,
    tool_id: StableToolId,
    tool_revision: StableToolRevision,
    constraint_revision: ToolConstraintRevision,
}

impl From<BindingWire> for PermissionBindingIdentity {
    fn from(wire: BindingWire) -> Self {
        Self {
            effect_id: wire.effect_id,
            tool_id: wire.tool_id,
            tool_revision: wire.tool_revision,
            constraint_revision: wire.constraint_revision,
        }
    }
}

#[derive(PartialEq)]
pub struct PermissionGrantReference {
    execution_id: ExecutionId,
    interrupt_id: InterruptId,
}

impl PermissionGrantReference {
    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    pub fn interrupt_id(&self) -> &InterruptId {
        &self.interrupt_id
    }
}

impl fmt::Debug for PermissionGrantReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionGrantReference([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantReferenceWire {
    execution_id: ExecutionId,
    interrupt_id: InterruptId,
}

impl From<GrantReferenceWire> for PermissionGrantReference {
    fn from(wire: GrantReferenceWire) -> Self {
        Self {
            execution_id: wire.execution_id,
            interrupt_id: wire.interrupt_id,
        }
    }
}

#[derive(PartialEq)]
pub struct PermissionAcceptedDecision {
    task_id: TaskId,
    interrupt_id: InterruptId,
    grant_ref: PermissionGrantReference,
    decision: ApprovalDecision,
    proposal_digest: String,
    task_parking_revision: u64,
    binding: Option<PermissionBindingIdentity>,
    permission_id: Option<PermissionId>,
}

impl PermissionAcceptedDecision {
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    pub fn interrupt_id(&self) -> &InterruptId {
        &self.interrupt_id
    }

    pub fn grant_ref(&self) -> &PermissionGrantReference {
        &self.grant_ref
    }

    pub fn decision(&self) -> ApprovalDecision {
        self.decision
    }

    pub fn proposal_digest(&self) -> &str {
        &self.proposal_digest
    }

    pub fn task_parking_revision(&self) -> u64 {
        self.task_parking_revision
    }

    pub fn binding(&self) -> Option<&PermissionBindingIdentity> {
        self.binding.as_ref()
    }

    pub fn permission_id(&self) -> Option<&PermissionId> {
        self.permission_id.as_ref()
    }
}

impl fmt::Debug for PermissionAcceptedDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionAcceptedDecision([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedDecisionWire {
    task_id: TaskId,
    interrupt_id: InterruptId,
    grant_ref: GrantReferenceWire,
    decision: ApprovalDecision,
    proposal_digest: String,
    task_parking_revision: u64,
    #[serde(deserialize_with = "required_nullable")]
    binding: Option<BindingWire>,
    #[serde(deserialize_with = "required_nullable")]
    permission_id: Option<PermissionId>,
}

impl From<AcceptedDecisionWire> for PermissionAcceptedDecision {
    fn from(wire: AcceptedDecisionWire) -> Self {
        Self {
            task_id: wire.task_id,
            interrupt_id: wire.interrupt_id,
            grant_ref: wire.grant_ref.into(),
            decision: wire.decision,
            proposal_digest: wire.proposal_digest,
            task_parking_revision: wire.task_parking_revision,
            binding: wire.binding.map(Into::into),
            permission_id: wire.permission_id,
        }
    }
}

#[derive(PartialEq)]
pub struct PermissionScope {
    thread_id: ThreadId,
    thread_generation: ThreadGeneration,
    origin: ThreadOrigin,
    application: PermissionApplicationIdentity,
    route: RouteKey,
    definition: Definition,
    effect_id: EffectId,
    tool_id: StableToolId,
    tool_revision: StableToolRevision,
    policy_revision: String,
    constraint_revision: ToolConstraintRevision,
}

impl PermissionScope {
    pub fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    pub fn thread_generation(&self) -> &ThreadGeneration {
        &self.thread_generation
    }

    pub fn origin(&self) -> &ThreadOrigin {
        &self.origin
    }

    pub fn application(&self) -> &PermissionApplicationIdentity {
        &self.application
    }

    pub fn route(&self) -> &RouteKey {
        &self.route
    }

    pub fn definition(&self) -> &Definition {
        &self.definition
    }

    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }

    pub fn tool_id(&self) -> &StableToolId {
        &self.tool_id
    }

    pub fn tool_revision(&self) -> &StableToolRevision {
        &self.tool_revision
    }

    pub fn policy_revision(&self) -> &str {
        &self.policy_revision
    }

    pub fn constraint_revision(&self) -> &ToolConstraintRevision {
        &self.constraint_revision
    }

    /// Exact versioned scope digest with a cumulative bounded account.
    pub fn canonical_digest(
        &self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<String, PermissionError> {
        let result = hash_canonical(
            CanonicalValue::Scope(ScopeParts::from_scope(self)),
            CanonicalLimit::Evidence64KiB,
            budget,
        );
        budget.finish(result)
    }
}

impl fmt::Debug for PermissionScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionScope([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeWire {
    thread_id: ThreadId,
    thread_generation: ThreadGeneration,
    origin: ThreadOrigin,
    application: ApplicationWire,
    route: RouteKey,
    definition: Definition,
    effect_id: EffectId,
    tool_id: StableToolId,
    tool_revision: StableToolRevision,
    policy_revision: String,
    constraint_revision: ToolConstraintRevision,
}

impl From<ScopeWire> for PermissionScope {
    fn from(wire: ScopeWire) -> Self {
        Self {
            thread_id: wire.thread_id,
            thread_generation: wire.thread_generation,
            origin: wire.origin,
            application: wire.application.into(),
            route: wire.route,
            definition: wire.definition,
            effect_id: wire.effect_id,
            tool_id: wire.tool_id,
            tool_revision: wire.tool_revision,
            policy_revision: wire.policy_revision,
            constraint_revision: wire.constraint_revision,
        }
    }
}

#[derive(PartialEq)]
pub struct PermissionAcceptance {
    record_version: u32,
    thread_id: ThreadId,
    thread_generation: ThreadGeneration,
    origin: ThreadOrigin,
    application: PermissionApplicationIdentity,
    policy_revision: String,
    before_snapshot: Snapshot,
    turn_id: TurnId,
    facts: TransitionFacts,
    committed_revision: u64,
    decisions: Vec<PermissionAcceptedDecision>,
}

impl PermissionAcceptance {
    pub fn record_version(&self) -> u32 {
        self.record_version
    }

    pub fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    pub fn thread_generation(&self) -> &ThreadGeneration {
        &self.thread_generation
    }

    pub fn origin(&self) -> &ThreadOrigin {
        &self.origin
    }

    pub fn application(&self) -> &PermissionApplicationIdentity {
        &self.application
    }

    pub fn policy_revision(&self) -> &str {
        &self.policy_revision
    }

    pub fn before_snapshot(&self) -> &Snapshot {
        &self.before_snapshot
    }

    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    pub fn facts(&self) -> &TransitionFacts {
        &self.facts
    }

    pub fn committed_revision(&self) -> u64 {
        self.committed_revision
    }

    pub fn decisions(&self) -> &[PermissionAcceptedDecision] {
        &self.decisions
    }

    /// Decode bounded structure and intrinsic retained-call witnesses.
    pub fn decode_record(
        record: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Self, PermissionError> {
        let result = ready_use_checked_codec_iife(|| {
            preflight(record, RecordKind::Acceptance, budget)?;
            let wire = serde_json::from_slice::<AcceptanceWire>(record).map_err(
                ready_authorization_checked_metadata_value(|_: serde_json::Error| {
                    PermissionError::CorruptRecord
                }),
            )?;
            let value = finish_acceptance(wire, record.len(), budget)?;
            let borrowed = BorrowedRecord::Acceptance(&value);
            let measured = measure(
                CanonicalValue::Record(borrowed),
                CanonicalLimit::Record(RecordKind::Acceptance),
                budget,
            )?;
            check_inherent(borrowed, &measured, budget)?;
            Ok(value)
        });
        budget.finish(result)
    }

    /// Canonical bounded encoding, repeating all paid intrinsic checks.
    pub fn encode_record(
        &self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Vec<u8>, PermissionError> {
        let result = encode(
            BorrowedRecord::Acceptance(self),
            RecordKind::Acceptance,
            budget,
        );
        budget.finish(result)
    }
}

impl fmt::Debug for PermissionAcceptance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionAcceptance([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptanceWire {
    record_version: u32,
    thread_id: ThreadId,
    thread_generation: ThreadGeneration,
    origin: ThreadOrigin,
    application: ApplicationWire,
    policy_revision: String,
    before_snapshot: Snapshot,
    turn_id: TurnId,
    facts: TransitionFacts,
    committed_revision: u64,
    decisions: Vec<AcceptedDecisionWire>,
}

fn finish_acceptance(
    wire: AcceptanceWire,
    raw_bytes: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionAcceptance, PermissionError> {
    let count = wire.decisions.len();
    // Each permitting entry costs 29 nodes; Deny is smaller. This conservative
    // quote uses only the O(1) length before performing the conversion walk.
    let nodes = add(1, mul(29, count)?)?;
    budget.reserve(
        add(
            raw_bytes,
            mul(count, std::mem::size_of::<PermissionAcceptedDecision>())?,
        )?,
        nodes,
    )?;
    let mut decisions = Vec::new();
    decisions
        .try_reserve_exact(count)
        .map_err(runtime_checked_mandatory_callable(|_| {
            PermissionError::LimitExceeded
        }))?;
    for entry in wire.decisions {
        decisions.push(entry.into());
    }
    Ok(PermissionAcceptance {
        record_version: wire.record_version,
        thread_id: wire.thread_id,
        thread_generation: wire.thread_generation,
        origin: wire.origin,
        application: wire.application.into(),
        policy_revision: wire.policy_revision,
        before_snapshot: wire.before_snapshot,
        turn_id: wire.turn_id,
        facts: wire.facts,
        committed_revision: wire.committed_revision,
        decisions,
    })
}
#[derive(PartialEq)]
pub struct PermissionDecision {
    record_version: u32,
    permission_id: PermissionId,
    scope_digest: String,
    scope: PermissionScope,
    creator_execution_id: ExecutionId,
    creator_turn_id: TurnId,
    creator_task_id: TaskId,
    creator_interrupt_id: InterruptId,
}

impl PermissionDecision {
    pub fn record_version(&self) -> u32 {
        self.record_version
    }

    pub fn permission_id(&self) -> &PermissionId {
        &self.permission_id
    }

    pub fn scope_digest(&self) -> &str {
        &self.scope_digest
    }

    pub fn scope(&self) -> &PermissionScope {
        &self.scope
    }

    pub fn creator_execution_id(&self) -> &ExecutionId {
        &self.creator_execution_id
    }

    pub fn creator_turn_id(&self) -> &TurnId {
        &self.creator_turn_id
    }

    pub fn creator_task_id(&self) -> &TaskId {
        &self.creator_task_id
    }

    pub fn creator_interrupt_id(&self) -> &InterruptId {
        &self.creator_interrupt_id
    }

    /// Decode bounded structure and intrinsic retained-call witnesses.
    pub fn decode_record(
        record: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Self, PermissionError> {
        let result = ready_use_checked_codec_iife(|| {
            preflight(record, RecordKind::Decision, budget)?;
            let wire = serde_json::from_slice::<DecisionWire>(record).map_err(
                ready_authorization_checked_metadata_value(|_: serde_json::Error| {
                    PermissionError::CorruptRecord
                }),
            )?;
            let value = Self::from(wire);
            let borrowed = BorrowedRecord::Decision(&value);
            let measured = measure(
                CanonicalValue::Record(borrowed),
                CanonicalLimit::Record(RecordKind::Decision),
                budget,
            )?;
            check_inherent(borrowed, &measured, budget)?;
            Ok(value)
        });
        budget.finish(result)
    }

    /// Canonical bounded encoding, repeating all paid intrinsic checks.
    pub fn encode_record(
        &self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Vec<u8>, PermissionError> {
        let result = encode(BorrowedRecord::Decision(self), RecordKind::Decision, budget);
        budget.finish(result)
    }
}

impl fmt::Debug for PermissionDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionDecision([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionWire {
    record_version: u32,
    permission_id: PermissionId,
    scope_digest: String,
    scope: ScopeWire,
    creator_execution_id: ExecutionId,
    creator_turn_id: TurnId,
    creator_task_id: TaskId,
    creator_interrupt_id: InterruptId,
}

impl From<DecisionWire> for PermissionDecision {
    fn from(wire: DecisionWire) -> Self {
        Self {
            record_version: wire.record_version,
            permission_id: wire.permission_id,
            scope_digest: wire.scope_digest,
            scope: wire.scope.into(),
            creator_execution_id: wire.creator_execution_id,
            creator_turn_id: wire.creator_turn_id,
            creator_task_id: wire.creator_task_id,
            creator_interrupt_id: wire.creator_interrupt_id,
        }
    }
}

#[derive(PartialEq)]
pub struct PermissionUse {
    record_version: u32,
    permission_version: u32,
    use_id: PermissionUseId,
    permission_id: PermissionId,
    thread_id: ThreadId,
    thread_generation: ThreadGeneration,
    execution_id: ExecutionId,
    task_id: TaskId,
    before_snapshot: Snapshot,
    facts: TransitionFacts,
    committed_revision: u64,
    marker: PendingApproval,
    call_digest: String,
    application: PermissionApplicationIdentity,
    tool_id: StableToolId,
    tool_revision: StableToolRevision,
    policy_revision: String,
    constraint_revision: ToolConstraintRevision,
}

impl PermissionUse {
    pub fn record_version(&self) -> u32 {
        self.record_version
    }

    pub fn permission_version(&self) -> u32 {
        self.permission_version
    }

    pub fn use_id(&self) -> &PermissionUseId {
        &self.use_id
    }

    pub fn permission_id(&self) -> &PermissionId {
        &self.permission_id
    }

    pub fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }

    pub fn thread_generation(&self) -> &ThreadGeneration {
        &self.thread_generation
    }

    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    pub fn before_snapshot(&self) -> &Snapshot {
        &self.before_snapshot
    }

    pub fn facts(&self) -> &TransitionFacts {
        &self.facts
    }

    pub fn committed_revision(&self) -> u64 {
        self.committed_revision
    }

    pub fn marker(&self) -> &PendingApproval {
        &self.marker
    }

    pub fn call_digest(&self) -> &str {
        &self.call_digest
    }

    pub fn application(&self) -> &PermissionApplicationIdentity {
        &self.application
    }

    pub fn tool_id(&self) -> &StableToolId {
        &self.tool_id
    }

    pub fn tool_revision(&self) -> &StableToolRevision {
        &self.tool_revision
    }

    pub fn policy_revision(&self) -> &str {
        &self.policy_revision
    }

    pub fn constraint_revision(&self) -> &ToolConstraintRevision {
        &self.constraint_revision
    }

    /// Decode bounded structure and intrinsic retained-call witnesses.
    pub fn decode_record(
        record: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Self, PermissionError> {
        let result = ready_use_checked_codec_iife(|| {
            preflight(record, RecordKind::Use, budget)?;
            let wire = serde_json::from_slice::<UseWire>(record).map_err(
                ready_authorization_checked_metadata_value(|_: serde_json::Error| {
                    PermissionError::CorruptRecord
                }),
            )?;
            let value = Self::from(wire);
            let borrowed = BorrowedRecord::Use(&value);
            let measured = measure(
                CanonicalValue::Record(borrowed),
                CanonicalLimit::Record(RecordKind::Use),
                budget,
            )?;
            check_inherent(borrowed, &measured, budget)?;
            Ok(value)
        });
        budget.finish(result)
    }

    /// Canonical bounded encoding, repeating all paid intrinsic checks.
    pub fn encode_record(
        &self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Vec<u8>, PermissionError> {
        let result = encode(BorrowedRecord::Use(self), RecordKind::Use, budget);
        budget.finish(result)
    }
}

impl fmt::Debug for PermissionUse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionUse([REDACTED])")
    }
}

// New authority metadata has a closed keyset; old nested contracts and
// checked identities keep their existing serde behavior.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UseWire {
    record_version: u32,
    permission_version: u32,
    use_id: PermissionUseId,
    permission_id: PermissionId,
    thread_id: ThreadId,
    thread_generation: ThreadGeneration,
    execution_id: ExecutionId,
    task_id: TaskId,
    before_snapshot: Snapshot,
    facts: TransitionFacts,
    committed_revision: u64,
    marker: PendingApproval,
    call_digest: String,
    application: ApplicationWire,
    tool_id: StableToolId,
    tool_revision: StableToolRevision,
    policy_revision: String,
    constraint_revision: ToolConstraintRevision,
}

impl From<UseWire> for PermissionUse {
    fn from(wire: UseWire) -> Self {
        Self {
            record_version: wire.record_version,
            permission_version: wire.permission_version,
            use_id: wire.use_id,
            permission_id: wire.permission_id,
            thread_id: wire.thread_id,
            thread_generation: wire.thread_generation,
            execution_id: wire.execution_id,
            task_id: wire.task_id,
            before_snapshot: wire.before_snapshot,
            facts: wire.facts,
            committed_revision: wire.committed_revision,
            marker: wire.marker,
            call_digest: wire.call_digest,
            application: wire.application.into(),
            tool_id: wire.tool_id,
            tool_revision: wire.tool_revision,
            policy_revision: wire.policy_revision,
            constraint_revision: wire.constraint_revision,
        }
    }
}

/// Exact versioned TaskSpec/input digest, without execution authority.
pub fn permission_call_digest(
    task: &TaskRecord,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    let result = hash_canonical(
        CanonicalValue::TaskCall(task),
        CanonicalLimit::Call16KiB,
        budget,
    );
    budget.finish(result)
}

// Exact old approval proposal namespace through the bounded borrowed walker.
pub(crate) fn bounded_permission_proposal_digest(
    task: &TaskRecord,
    prompt: &serde_json::Value,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    let result = hash_canonical(
        CanonicalValue::LegacyProposal { task, prompt },
        CanonicalLimit::Evidence64KiB,
        budget,
    );
    budget.finish(result)
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .chars()
            .any(ready_authorization_checked_metadata_value(char::is_control))
}

const ACCOUNT_BYTES: usize = 8 * 1024 * 1024;
const READY_AUTHORIZATION_WRITER_ACCOUNT_BYTES: usize = 16 * 1024 * 1024;
const NORMAL_ORIGINAL_ALWAYS_ACCOUNT_BYTES: usize = 16 * 1024 * 1024;
const NORMAL_ORIGINAL_ALWAYS_ACCOUNT_NODES: usize = 131_072;
const SETTLED_THREAD_DELETION_ACCOUNT_BYTES: usize = 64 * 1024 * 1024;
const USE_DISPATCH_ACCOUNT_BYTES: usize = 16 * 1024 * 1024;
const ENTERED_CANCELLATION_ACCOUNT_BYTES: usize = 24 * 1024 * 1024;
// Existing private purpose guards identify accounts by their byte ceilings.
const _: () = {
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != ACCOUNT_BYTES);
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != USE_DISPATCH_ACCOUNT_BYTES);
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != USE_RENEWAL_SCAFFOLD_BYTES);
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != USE_RENEWAL_PRODUCTION_BYTES);
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != USE_CALLBACK_ACCOUNT_BYTES);
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != USE_OUTCOME_SCAFFOLD_BYTES);
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != USE_OUTCOME_FUNCTIONAL_BYTES);
    assert!(ENTERED_CANCELLATION_ACCOUNT_BYTES != USE_WITHHELD_FUNCTIONAL_BYTES);
};
const ACCOUNT_NODES: usize = 65_536;
const ROW_NODES: usize = 16_384;
const MAX_DEPTH: usize = 64;

fn add(a: usize, b: usize) -> Result<usize, PermissionError> {
    a.checked_add(b).ok_or(PermissionError::LimitExceeded)
}

fn mul(a: usize, b: usize) -> Result<usize, PermissionError> {
    a.checked_mul(b).ok_or(PermissionError::LimitExceeded)
}

impl PermissionEvidenceBudget {
    fn finish<T>(&mut self, result: Result<T, PermissionError>) -> Result<T, PermissionError> {
        if matches!(&result, Err(PermissionError::LimitExceeded)) {
            self.exhausted = true;
        }
        result
    }

    pub(super) fn reserve(&mut self, bytes: usize, nodes: usize) -> Result<(), PermissionError> {
        if self.bytes_limit == USE_OUTCOME_FUNCTIONAL_BYTES {
            return self.outcome_functional_reserve(bytes, nodes);
        }
        if self.bytes_limit == USE_WITHHELD_FUNCTIONAL_BYTES {
            return self.withheld_functional_reserve(bytes, nodes);
        }
        if self.exhausted {
            return Err(PermissionError::LimitExceeded);
        }
        let next_bytes = self.usage.encoded_bytes.checked_add(bytes);
        let next_nodes = self.usage.structural_nodes.checked_add(nodes);
        let byte_ceiling = if self.settled_thread_deletion {
            SETTLED_THREAD_DELETION_ACCOUNT_BYTES
        } else if matches!(
            self.original_always_profile,
            OriginalAlwaysProfile::NormalOriginalAlways
        ) {
            // Preserve stored charging-path identity; only sealed purpose grows
            // the effective allowance for the complete measured original work.
            NORMAL_ORIGINAL_ALWAYS_ACCOUNT_BYTES
        } else if self.ready_authorization_writer_entered {
            READY_AUTHORIZATION_WRITER_ACCOUNT_BYTES
        } else {
            self.bytes_limit
        };
        let node_ceiling = if !self.settled_thread_deletion
            && matches!(
                self.original_always_profile,
                OriginalAlwaysProfile::NormalOriginalAlways
            ) {
            NORMAL_ORIGINAL_ALWAYS_ACCOUNT_NODES
        } else {
            ACCOUNT_NODES
        };
        match (next_bytes, next_nodes) {
            (Some(bytes), Some(nodes)) if bytes <= byte_ceiling && nodes <= node_ceiling => {
                self.usage.encoded_bytes = bytes;
                self.usage.structural_nodes = nodes;
                Ok(())
            }
            _ => {
                self.exhausted = true;
                Err(PermissionError::LimitExceeded)
            }
        }
    }

    fn limit<T>(&mut self) -> Result<T, PermissionError> {
        self.exhausted = true;
        Err(PermissionError::LimitExceeded)
    }
}

#[derive(Clone, Copy)]
enum RecordKind {
    Acceptance,
    Decision,
    Use,
}

impl RecordKind {
    fn bytes(self) -> usize {
        match self {
            Self::Acceptance | Self::Decision => 65_536,
            Self::Use => 16_384,
        }
    }
}

#[derive(Clone, Copy)]
enum CanonicalLimit {
    Record(RecordKind),
    Evidence64KiB,
    Call16KiB,
    ResourceSnapshot,
    ResourceThread,
    LegacyRecord,
    LegacyPrompt,
}

impl CanonicalLimit {
    fn bytes(self) -> usize {
        match self {
            Self::Record(kind) => kind.bytes(),
            Self::Evidence64KiB => 65_536,
            Self::Call16KiB => 16_384,
            Self::ResourceSnapshot => 1_048_576,
            Self::ResourceThread => 262_144,
            Self::LegacyRecord | Self::LegacyPrompt => 65_536,
        }
    }
    fn depth(self) -> usize {
        match self {
            Self::ResourceSnapshot | Self::ResourceThread => 127,
            Self::LegacyRecord => 65,
            _ => MAX_DEPTH,
        }
    }
    fn nodes(self) -> usize {
        match self {
            Self::ResourceSnapshot
            | Self::ResourceThread
            | Self::LegacyRecord
            | Self::LegacyPrompt => ACCOUNT_NODES,
            _ => ROW_NODES,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct CanonicalMeasure {
    bytes: usize,
    nodes: usize,
    depth: usize,
}

#[derive(Clone, Copy)]
struct KeySpan {
    begin: usize,
    end: usize,
    decoded_bytes: usize,
}

struct RawContainer {
    parent: Option<usize>,
    via_key: Option<usize>,
    array: bool,
    keys: Vec<KeySpan>,
}

#[derive(Clone, Copy)]
struct RawFrame {
    container: usize,
    state: u8,
    pending_key: Option<usize>,
}

fn grow<T>(
    values: &mut Vec<T>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    if values.len() == values.capacity() {
        let target = if values.capacity() == 0 {
            4
        } else {
            mul(values.capacity(), 2)?
        };
        // Requested slots and possible relocation of all existing elements are paid.
        budget.reserve(
            mul(add(target, values.len())?, std::mem::size_of::<T>())?,
            0,
        )?;
        values
            .try_reserve_exact(target - values.len())
            .map_err(|_| PermissionError::LimitExceeded)?;
    }
    Ok(())
}

fn whitespace(raw: &[u8], position: &mut usize) {
    while raw
        .get(*position)
        .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
    {
        *position += 1;
    }
}

fn hex(raw: &[u8], position: &mut usize) -> Result<u16, PermissionError> {
    let mut value = 0u16;
    for _ in 0..4 {
        let digit = *raw.get(*position).ok_or(PermissionError::CorruptRecord)?;
        let digit = match digit {
            b'0'..=b'9' => digit - b'0',
            b'a'..=b'f' => digit - b'a' + 10,
            b'A'..=b'F' => digit - b'A' + 10,
            _ => return Err(PermissionError::CorruptRecord),
        };
        value = value * 16 + u16::from(digit);
        *position += 1;
    }
    Ok(value)
}

fn raw_string(raw: &[u8], position: &mut usize) -> Result<KeySpan, PermissionError> {
    let begin = *position;
    let mut decoded_bytes = 0;
    if raw.get(*position) != Some(&b'"') {
        return Err(PermissionError::CorruptRecord);
    }
    *position += 1;
    loop {
        let byte = *raw.get(*position).ok_or(PermissionError::CorruptRecord)?;
        *position += 1;
        match byte {
            b'"' => {
                return Ok(KeySpan {
                    begin,
                    end: *position,
                    decoded_bytes,
                })
            }
            0..=31 => return Err(PermissionError::CorruptRecord),
            b'\\' => {
                let escape = *raw.get(*position).ok_or(PermissionError::CorruptRecord)?;
                *position += 1;
                match escape {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => decoded_bytes += 1,
                    b'u' => {
                        let first = hex(raw, position)?;
                        if (0xd800..=0xdbff).contains(&first) {
                            if raw.get(*position) != Some(&b'\\')
                                || raw.get(*position + 1) != Some(&b'u')
                            {
                                return Err(PermissionError::CorruptRecord);
                            }
                            *position += 2;
                            let second = hex(raw, position)?;
                            if !(0xdc00..=0xdfff).contains(&second) {
                                return Err(PermissionError::CorruptRecord);
                            }
                            decoded_bytes += 4;
                        } else if (0xdc00..=0xdfff).contains(&first) {
                            return Err(PermissionError::CorruptRecord);
                        } else {
                            decoded_bytes += if first <= 0x7f {
                                1
                            } else if first <= 0x7ff {
                                2
                            } else {
                                3
                            };
                        }
                    }
                    _ => return Err(PermissionError::CorruptRecord),
                }
            }
            0x80..=0xff => {
                // Validate UTF-8 in the same paid lexical walk, without a later full scan.
                let (count, low, high) = match byte {
                    0xc2..=0xdf => (1, 0x80, 0xbf),
                    0xe0 => (2, 0xa0, 0xbf),
                    0xe1..=0xec | 0xee..=0xef => (2, 0x80, 0xbf),
                    0xed => (2, 0x80, 0x9f),
                    0xf0 => (3, 0x90, 0xbf),
                    0xf1..=0xf3 => (3, 0x80, 0xbf),
                    0xf4 => (3, 0x80, 0x8f),
                    _ => return Err(PermissionError::CorruptRecord),
                };
                for index in 0..count {
                    let continuation = *raw.get(*position).ok_or(PermissionError::CorruptRecord)?;
                    if index == 0 {
                        if continuation < low || continuation > high {
                            return Err(PermissionError::CorruptRecord);
                        }
                    } else if !(0x80..=0xbf).contains(&continuation) {
                        return Err(PermissionError::CorruptRecord);
                    }
                    *position += 1;
                }
                decoded_bytes += count + 1;
            }
            _ => decoded_bytes += 1,
        }
    }
}

fn decode_key(
    raw: &[u8],
    span: KeySpan,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    // The raw lexer already proved syntax/UTF-8 and the exact decoded length.
    // Pay this separate decoding pass, retained string and scalar scratch first.
    budget.reserve(add(mul(span.end - span.begin, 6)?, span.decoded_bytes)?, 0)?;
    let mut key = String::new();
    key.try_reserve_exact(span.decoded_bytes)
        .map_err(|_| PermissionError::LimitExceeded)?;
    let mut position = span.begin + 1;
    while position + 1 < span.end {
        let byte = raw[position];
        position += 1;
        let scalar = if byte == b'\\' {
            let escape = raw[position];
            position += 1;
            match escape {
                b'"' => '"',
                b'\\' => '\\',
                b'/' => '/',
                b'b' => '\u{8}',
                b'f' => '\u{c}',
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'u' => {
                    let first = hex(raw, &mut position)?;
                    let code = if (0xd800..=0xdbff).contains(&first) {
                        position += 2;
                        let second = hex(raw, &mut position)?;
                        0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
                    } else {
                        u32::from(first)
                    };
                    char::from_u32(code).ok_or(PermissionError::CorruptRecord)?
                }
                _ => return Err(PermissionError::CorruptRecord),
            }
        } else if byte >= 0x80 {
            let (count, mask) = if byte < 0xe0 {
                (1, 0x1f)
            } else if byte < 0xf0 {
                (2, 0xf)
            } else {
                (3, 7)
            };
            let mut code = u32::from(byte & mask);
            for _ in 0..count {
                code = (code << 6) | u32::from(raw[position] & 0x3f);
                position += 1;
            }
            char::from_u32(code).ok_or(PermissionError::CorruptRecord)?
        } else {
            char::from(byte)
        };
        key.push(scalar);
    }
    if key.len() != span.decoded_bytes {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(key)
}

fn raw_number(raw: &[u8], position: &mut usize) -> Result<(), PermissionError> {
    if raw.get(*position) == Some(&b'-') {
        *position += 1;
    }
    match raw.get(*position) {
        Some(b'0') => *position += 1,
        Some(b'1'..=b'9') => {
            *position += 1;
            while raw.get(*position).is_some_and(u8::is_ascii_digit) {
                *position += 1;
            }
        }
        _ => return Err(PermissionError::CorruptRecord),
    }
    if raw.get(*position) == Some(&b'.') {
        *position += 1;
        if !raw.get(*position).is_some_and(u8::is_ascii_digit) {
            return Err(PermissionError::CorruptRecord);
        }
        while raw.get(*position).is_some_and(u8::is_ascii_digit) {
            *position += 1;
        }
    }
    if matches!(raw.get(*position), Some(b'e' | b'E')) {
        *position += 1;
        if matches!(raw.get(*position), Some(b'+' | b'-')) {
            *position += 1;
        }
        if !raw.get(*position).is_some_and(u8::is_ascii_digit) {
            return Err(PermissionError::CorruptRecord);
        }
        while raw.get(*position).is_some_and(u8::is_ascii_digit) {
            *position += 1;
        }
    }
    Ok(())
}

fn raw_node(
    nodes: &mut usize,
    maximum: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    if *nodes == maximum {
        return budget.limit();
    }
    budget.reserve(0, 1)?;
    *nodes += 1;
    Ok(())
}

#[derive(Clone, Copy)]
enum RawProfile {
    Record(RecordKind),
    Snapshot,
    ThreadJson,
    Legacy,
}
impl RawProfile {
    fn limit(self) -> CanonicalLimit {
        match self {
            Self::Record(kind) => CanonicalLimit::Record(kind),
            Self::Snapshot => CanonicalLimit::ResourceSnapshot,
            Self::ThreadJson => CanonicalLimit::ResourceThread,
            Self::Legacy => CanonicalLimit::LegacyRecord,
        }
    }
}
fn preflight(
    raw: &[u8],
    kind: RecordKind,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    preflight_profile(raw, RawProfile::Record(kind), budget)
}
fn preflight_profile(
    raw: &[u8],
    profile: RawProfile,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let limit = profile.limit();
    if raw.len() > limit.bytes() {
        return budget.limit();
    }
    budget.reserve(raw.len(), 0)?;
    budget.reserve(mul(add(limit.depth(), 1)?, 512)?, 0)?;
    let mut frames = Vec::<RawFrame>::new();
    frames
        .try_reserve_exact(add(limit.depth(), 1)?)
        .map_err(|_| PermissionError::LimitExceeded)?;
    let mut containers = Vec::<RawContainer>::new();
    let mut position = 0;
    let mut nodes = 0;
    let mut numbers = 0;
    let mut root = false;
    loop {
        whitespace(raw, &mut position);
        if let Some(frame) = frames.last_mut() {
            let object = !containers[frame.container].array;
            match frame.state {
                0 | 4 if object => {
                    if raw.get(position) == Some(&b'}') && frame.state == 0 {
                        position += 1;
                        frames.pop();
                        continue;
                    }
                    raw_node(&mut nodes, limit.nodes(), budget)?;
                    let key = raw_string(raw, &mut position)?;
                    let container = &mut containers[frame.container];
                    grow(&mut container.keys, budget)?;
                    frame.pending_key = Some(container.keys.len());
                    container.keys.push(key);
                    frame.state = 1;
                    continue;
                }
                1 => {
                    if raw.get(position) != Some(&b':') {
                        return Err(PermissionError::CorruptRecord);
                    }
                    position += 1;
                    frame.state = 2;
                    continue;
                }
                3 => {
                    let close = if object { b'}' } else { b']' };
                    if raw.get(position) == Some(&close) {
                        position += 1;
                        frames.pop();
                        continue;
                    }
                    if raw.get(position) != Some(&b',') {
                        return Err(PermissionError::CorruptRecord);
                    }
                    position += 1;
                    frame.state = 4;
                    continue;
                }
                0 if !object && raw.get(position) == Some(&b']') => {
                    position += 1;
                    frames.pop();
                    continue;
                }
                _ => {}
            }
        } else if root {
            if position != raw.len() {
                return Err(PermissionError::CorruptRecord);
            }
            break;
        }
        let (parent, via_key) = if let Some(frame) = frames.last_mut() {
            let values = (Some(frame.container), frame.pending_key.take());
            frame.state = 3;
            values
        } else {
            root = true;
            (None, None)
        };
        raw_node(&mut nodes, limit.nodes(), budget)?;
        match raw.get(position).copied() {
            Some(b'{' | b'[') => {
                if frames.len() == limit.depth() {
                    return budget.limit();
                }
                let array = raw[position] == b'[';
                position += 1;
                grow(&mut containers, budget)?;
                let index = containers.len();
                containers.push(RawContainer {
                    parent,
                    via_key,
                    array,
                    keys: Vec::new(),
                });
                frames.push(RawFrame {
                    container: index,
                    state: 0,
                    pending_key: None,
                });
            }
            Some(b'"') => {
                raw_string(raw, &mut position)?;
            }
            Some(b't') if raw.get(position..position + 4) == Some(b"true") => position += 4,
            Some(b'f') if raw.get(position..position + 5) == Some(b"false") => position += 5,
            Some(b'n') if raw.get(position..position + 4) == Some(b"null") => position += 4,
            Some(b'-' | b'0'..=b'9') => {
                raw_number(raw, &mut position)?;
                numbers += 1;
            }
            _ => return Err(PermissionError::CorruptRecord),
        }
    }
    // The raw grammar/value-key pass is complete before any decoded-key work.
    // In particular, the 16,385th raw node fails with exactly 16,384 charged nodes.
    budget.reserve(mul(containers.len(), 128)?, 0)?;
    let mut decoded = Vec::<Vec<String>>::new();
    decoded
        .try_reserve_exact(containers.len())
        .map_err(|_| PermissionError::LimitExceeded)?;
    let mut all_key_bytes = 0;
    let mut all_keys = 0;
    for container in &containers {
        budget.reserve(mul(container.keys.len(), 128)?, 0)?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(container.keys.len())
            .map_err(|_| PermissionError::LimitExceeded)?;
        let mut key_bytes = 0;
        for span in &container.keys {
            let key = decode_key(raw, *span, budget)?;
            key_bytes = add(key_bytes, key.len())?;
            keys.push(key);
        }
        // Pay one complete comparison bound, after k/K are known, before sorting.
        budget.reserve(mul(mul(2, keys.len())?, key_bytes)?, 0)?;
        budget.reserve(
            add(mul(keys.len(), 16)?, mul(16, mul(keys.len(), keys.len())?)?)?,
            0,
        )?;
        let mut ordered = Vec::<usize>::new();
        ordered
            .try_reserve_exact(keys.len())
            .map_err(|_| PermissionError::LimitExceeded)?;
        for index in 0..keys.len() {
            let mut position = ordered.len();
            for (slot, previous) in ordered.iter().enumerate() {
                match keys[*previous].cmp(&keys[index]) {
                    std::cmp::Ordering::Equal => return Err(PermissionError::CorruptRecord),
                    std::cmp::Ordering::Greater => {
                        position = slot;
                        break;
                    }
                    std::cmp::Ordering::Less => {}
                }
            }
            ordered.insert(position, index);
        }
        all_key_bytes = add(all_key_bytes, key_bytes)?;
        all_keys = add(all_keys, keys.len())?;
        decoded.push(keys);
    }
    let mut inserted_bytes = 0;
    let mut inserted_nodes = 0;
    // Pay ancestry/field-name/presence lookup scans before performing them.
    budget.reserve(
        add(
            mul(32, all_key_bytes)?,
            add(mul(2048, all_keys)?, mul(128, containers.len())?)?,
        )?,
        0,
    )?;
    for (index, container) in containers.iter().enumerate() {
        let is_snapshot = match profile {
            RawProfile::Record(_) => {
                container.parent == Some(0)
                    && container
                        .via_key
                        .is_some_and(|key| decoded[0][key] == "before_snapshot")
            }
            RawProfile::Snapshot => index == 0 && !container.array,
            RawProfile::ThreadJson | RawProfile::Legacy => false,
        };
        let is_task = container.parent.is_some_and(|parent| {
            let array = &containers[parent];
            array.array
                && array.parent.is_some_and(|snapshot| {
                    let owner = &containers[snapshot];
                    let snapshot_owner = match profile {
                        RawProfile::Record(_) => {
                            owner.parent == Some(0)
                                && owner
                                    .via_key
                                    .is_some_and(|key| decoded[0][key] == "before_snapshot")
                        }
                        RawProfile::Snapshot => snapshot == 0 && !owner.array,
                        RawProfile::ThreadJson | RawProfile::Legacy => false,
                    };
                    snapshot_owner
                        && array
                            .via_key
                            .is_some_and(|key| decoded[snapshot][key] == "tasks")
                })
        });
        if is_snapshot && !decoded[index].iter().any(|key| key == "failure") {
            inserted_bytes = add(inserted_bytes, 15)?;
            inserted_nodes = add(inserted_nodes, 2)?;
        }
        if is_task {
            for (key, bytes) in [("attempt_id", 18), ("result", 14), ("interrupt", 17)] {
                if !decoded[index].iter().any(|present| present == key) {
                    inserted_bytes = add(inserted_bytes, bytes)?;
                    inserted_nodes = add(inserted_nodes, 2)?;
                }
            }
        }
    }
    // Typed materialization and serde's own paid second lexical/error-scratch walk.
    budget.reserve(
        add(
            add(add(raw.len(), inserted_bytes)?, mul(numbers, 32)?)?,
            mul(128, add(nodes, inserted_nodes)?)?,
        )?,
        add(nodes, inserted_nodes)?,
    )?;
    budget.reserve(add(mul(raw.len(), 2)?, 4096)?, 0)?;
    if budget.runtime_ready_cancellation_has_fixed_purchase() {
        // Preserve the old cancellation ticket and its original short circuit.
        if matches!(profile, RawProfile::Snapshot | RawProfile::ThreadJson) {
            let owner_nodes = add(nodes, inserted_nodes)?;
            let owner_bytes = add(add(raw.len(), inserted_bytes)?, mul(32, numbers)?)?;
            budget.reserve(
                add(mul(128, owner_nodes)?, mul(6, owner_bytes)?)?,
                owner_nodes,
            )?;
        }
    } else if budget.use_renewal_scaffold_entered || budget.use_renewal_production_entered {
        // Fresh renewal-only owner ticket before serde materialization; old
        // cancellation true and ordinary false branches remain literal below.
        budget.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        let owner_nodes = add(nodes, inserted_nodes)?;
        let owner_bytes = add(add(raw.len(), inserted_bytes)?, mul(32, numbers)?)?;
        budget.reserve(
            add(mul(128, owner_nodes)?, mul(6, owner_bytes)?)?,
            owner_nodes,
        )?;
    } else if budget.bytes_limit == USE_OUTCOME_FUNCTIONAL_BYTES {
        budget.outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
        let owner_nodes = add(nodes, inserted_nodes)?;
        let owner_bytes = add(add(raw.len(), inserted_bytes)?, mul(32, numbers)?)?;
        budget.reserve(
            add(mul(128, owner_nodes)?, mul(6, owner_bytes)?)?,
            owner_nodes,
        )?;
    } else {
        // A distinct complete metadata protocol is proposed for this arm.
        // Its first-refusal allowance is an application prerequisite.
        budget.reserve(
            READY_AUTHORIZATION_SHARED_CHECK_BYTES,
            READY_AUTHORIZATION_SHARED_CHECK_NODES,
        )?;
        if budget.runtime_use_entered
            || (budget.ready_authorization_entered
                && (matches!(profile, RawProfile::Snapshot | RawProfile::ThreadJson)
                    || budget.ready_authorization_writer_entered))
            || budget.observe_use_dispatch_account()?
        {
            let owner_nodes = add(nodes, inserted_nodes)?;
            let owner_bytes = add(add(raw.len(), inserted_bytes)?, mul(32, numbers)?)?;
            budget.reserve(
                add(mul(128, owner_nodes)?, mul(6, owner_bytes)?)?,
                owner_nodes,
            )?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum BorrowedRecord<'a> {
    Acceptance(&'a PermissionAcceptance),
    Decision(&'a PermissionDecision),
    Use(&'a PermissionUse),
}
#[derive(Clone, Copy)]
struct ScopeParts<'a> {
    thread_id: &'a ThreadId,
    thread_generation: &'a ThreadGeneration,
    origin: &'a ThreadOrigin,
    application: &'a PermissionApplicationIdentity,
    route: &'a RouteKey,
    definition: &'a Definition,
    effect_id: &'a EffectId,
    tool_id: &'a StableToolId,
    tool_revision: &'a StableToolRevision,
    policy_revision: &'a str,
    constraint_revision: &'a ToolConstraintRevision,
}
impl<'a> ScopeParts<'a> {
    fn from_scope(scope: &'a PermissionScope) -> Self {
        Self {
            thread_id: &scope.thread_id,
            thread_generation: &scope.thread_generation,
            origin: &scope.origin,
            application: &scope.application,
            route: &scope.route,
            definition: &scope.definition,
            effect_id: &scope.effect_id,
            tool_id: &scope.tool_id,
            tool_revision: &scope.tool_revision,
            policy_revision: &scope.policy_revision,
            constraint_revision: &scope.constraint_revision,
        }
    }
}
#[derive(Clone, Copy)]
enum Sequence<'a> {
    Values(&'a [serde_json::Value]),
    Tasks(&'a [TaskRecord]),
    Batches(&'a [Vec<b4_contract::TaskSpec>]),
    Specs(&'a [b4_contract::TaskSpec]),
    Turns(&'a [TurnId]),
    Attempts(&'a [b4_contract::AttemptId]),
    Entries(&'a [PermissionAcceptedDecision]),
    Writes(&'a [b4_contract::StateWrite]),
    LegacyDecisions(&'a [crate::approval::AcceptedDecision]),
    Bytes(&'a [u8]),
}
impl<'a> Sequence<'a> {
    fn child(self, index: usize) -> Option<CanonicalValue<'a>> {
        use CanonicalValue as V;
        match self {
            Self::Values(v) => v.get(index).map(V::Json),
            Self::Tasks(v) => v.get(index).map(V::TaskRecord),
            Self::Batches(v) => v.get(index).map(|v| V::Sequence(Self::Specs(v))),
            Self::Specs(v) => v.get(index).map(V::TaskSpec),
            Self::Turns(v) => v.get(index).map(|v| V::String(v.as_str())),
            Self::Attempts(v) => v.get(index).map(|v| V::String(v.as_str())),
            Self::Entries(v) => v.get(index).map(V::Entry),
            Self::Writes(v) => v.get(index).map(V::Write),
            Self::LegacyDecisions(v) => v.get(index).map(V::LegacyDecision),
            Self::Bytes(v) => v.get(index).map(|v| V::U64(u64::from(*v))),
        }
    }
}
#[derive(Clone, Copy)]
enum CanonicalValue<'a> {
    Null,
    Bool(bool),
    U64(u64),
    Usize(usize),
    String(&'a str),
    Number(&'a serde_json::Number),
    Json(&'a serde_json::Value),
    Input(&'a serde_json::Map<String, serde_json::Value>),
    Snapshot(&'a Snapshot),
    Thread(&'a crate::ThreadRecord),
    Definition(&'a Definition),
    TaskSpec(&'a b4_contract::TaskSpec),
    TaskRecord(&'a TaskRecord),
    TaskResult(&'a b4_contract::TaskResult),
    Pending(&'a PendingApproval),
    Write(&'a b4_contract::StateWrite),
    Origin(&'a ThreadOrigin),
    Application(&'a PermissionApplicationIdentity),
    Binding(&'a PermissionBindingIdentity),
    Grant(&'a PermissionGrantReference),
    Entry(&'a PermissionAcceptedDecision),
    Facts(&'a TransitionFacts),
    Scope(ScopeParts<'a>),
    ScopeBody(ScopeParts<'a>),
    Record(BorrowedRecord<'a>),
    TaskCall(&'a TaskRecord),
    LegacyGrant(&'a crate::approval::GrantRecord),
    LegacyAcceptance(&'a crate::approval::ApprovalAcceptance),
    LegacyDecision(&'a crate::approval::AcceptedDecision),
    LegacyContext(&'a crate::approval::ApprovalContext),
    LegacyProposal {
        task: &'a TaskRecord,
        prompt: &'a serde_json::Value,
    },
    Sequence(Sequence<'a>),
}
fn decision_name(v: ApprovalDecision) -> &'static str {
    match v {
        ApprovalDecision::Once => "once",
        ApprovalDecision::Always => "always",
        ApprovalDecision::Deny => "deny",
    }
}
fn execution_status(v: b4_contract::ExecutionStatus) -> &'static str {
    use b4_contract::ExecutionStatus as S;
    match v {
        S::Accepted => "accepted",
        S::Running => "running",
        S::WaitingForApproval => "waiting_for_approval",
        S::Completed => "completed",
        S::Failed => "failed",
        S::CancelRequested => "cancel_requested",
        S::Cancelled => "cancelled",
        S::RecoveryBlocked => "recovery_blocked",
    }
}
fn task_status(v: b4_contract::TaskStatus) -> &'static str {
    use b4_contract::TaskStatus as S;
    match v {
        S::Ready => "ready",
        S::WaitingForApproval => "waiting_for_approval",
        S::InFlight => "in_flight",
        S::Succeeded => "succeeded",
        S::Failed => "failed",
        S::Denied => "denied",
        S::Cancelled => "cancelled",
        S::OutcomeUnknown => "outcome_unknown",
    }
}
fn recovery_name(v: b4_contract::RecoveryPolicy) -> &'static str {
    use b4_contract::RecoveryPolicy as P;
    match v {
        P::Pure => "pure",
        P::Idempotent => "idempotent",
        P::Reconcilable => "reconcilable",
        P::Unsafe => "unsafe",
    }
}
fn closed_child<'a>(
    value: CanonicalValue<'a>,
    index: usize,
) -> Option<(&'static str, CanonicalValue<'a>)> {
    use CanonicalValue as V;
    match value {
        V::LegacyGrant(v) => match index {
            0 => Some(("consumed", V::Bool(v.consumed))),
            1 => Some((
                "definition_revision",
                V::String(v.definition_revision.as_str()),
            )),
            2 => Some(("effect_id", V::String(v.effect_id.as_str()))),
            3 => Some(("execution_id", V::String(v.execution_id.as_str()))),
            4 => Some(("expires_at_ms", V::U64(v.expires_at_ms))),
            5 => Some(("interrupt_id", V::String(v.interrupt_id.as_str()))),
            6 => Some(("issued_at_ms", V::U64(v.issued_at_ms))),
            7 => Some(("origin", V::Origin(&v.origin))),
            8 => Some(("policy_revision", V::String(&v.policy_revision))),
            9 => Some(("prompt", V::Json(&v.prompt))),
            10 => Some(("proposal_digest", V::String(&v.proposal_digest))),
            11 => Some(("resume_key", V::String(&v.resume_key))),
            12 => Some(("route", V::String(v.route.as_str()))),
            13 => Some(("task_id", V::String(v.task_id.as_str()))),
            14 => Some(("task_parking_revision", V::U64(v.task_parking_revision))),
            15 => Some(("thread_generation", V::String(v.thread_generation.as_str()))),
            16 => Some(("thread_id", V::String(v.thread_id.as_str()))),
            17 => Some(("token", V::String(v.token.as_str()))),
            18 => Some(("verifier", V::Sequence(Sequence::Bytes(&v.verifier)))),
            _ => None,
        },
        V::LegacyAcceptance(v) => match index {
            0 => Some(("accepted_at_ms", V::U64(v.accepted_at_ms))),
            1 => Some(("context", V::LegacyContext(&v.context))),
            2 => Some((
                "decisions",
                V::Sequence(Sequence::LegacyDecisions(&v.decisions)),
            )),
            3 => Some((
                "definition_revision",
                V::String(v.definition_revision.as_str()),
            )),
            4 => Some(("execution_id", V::String(v.execution_id.as_str()))),
            5 => Some(("previous_revision", V::U64(v.previous_revision))),
            6 => Some(("thread_generation", V::String(v.thread_generation.as_str()))),
            7 => Some(("thread_id", V::String(v.thread_id.as_str()))),
            8 => Some(("turn_id", V::String(v.turn_id.as_str()))),
            _ => None,
        },
        V::LegacyDecision(v) => match index {
            0 => Some(("decision", V::String(decision_name(v.decision)))),
            1 => Some(("interrupt_id", V::String(v.interrupt_id.as_str()))),
            2 => Some(("proposal_digest", V::String(&v.proposal_digest))),
            3 => Some(("task_id", V::String(v.task_id.as_str()))),
            4 => Some(("task_parking_revision", V::U64(v.task_parking_revision))),
            _ => None,
        },
        V::LegacyContext(v) => match index {
            0 => Some(("actor", V::Origin(&v.actor))),
            1 => Some(("policy_revision", V::String(&v.policy_revision))),
            _ => None,
        },
        V::Application(v) => match index {
            0 => Some(("application", V::String(&v.application))),
            1 => Some(("revision", V::String(&v.revision))),
            _ => None,
        },
        V::Thread(v) => match index {
            0 => Some((
                "active_execution_id",
                v.active_execution_id
                    .as_ref()
                    .map_or(V::Null, |id| V::String(id.as_str())),
            )),
            1 => Some(("created_at_ms", V::U64(v.created_at_ms))),
            2 => Some(("generation", V::String(v.generation.as_str()))),
            3 => Some(("id", V::String(v.id.as_str()))),
            4 => Some(("metadata", V::Input(&v.metadata))),
            5 => Some(("origin", v.origin.as_ref().map_or(V::Null, V::Origin))),
            6 => Some(("resource_revision", V::U64(v.resource_revision))),
            _ => None,
        },
        V::Origin(v) => match index {
            0 => Some(("application", V::String(&v.application))),
            1 => Some(("principal", V::String(&v.principal))),
            2 => Some(("tenant", V::String(&v.tenant))),
            _ => None,
        },
        V::Binding(v) => match index {
            0 => Some((
                "constraint_revision",
                V::String(v.constraint_revision.as_str()),
            )),
            1 => Some(("effect_id", V::String(v.effect_id.as_str()))),
            2 => Some(("tool_id", V::String(v.tool_id.as_str()))),
            3 => Some(("tool_revision", V::String(v.tool_revision.as_str()))),
            _ => None,
        },
        V::Grant(v) => match index {
            0 => Some(("execution_id", V::String(v.execution_id.as_str()))),
            1 => Some(("interrupt_id", V::String(v.interrupt_id.as_str()))),
            _ => None,
        },
        V::Facts(v) => match index {
            0 => Some(("now_ms", V::U64(v.now_ms))),
            _ => None,
        },
        V::ScopeBody(v) => match index {
            0 => Some(("application", V::Application(v.application))),
            1 => Some((
                "constraint_revision",
                V::String(v.constraint_revision.as_str()),
            )),
            2 => Some(("definition", V::Definition(v.definition))),
            3 => Some(("effect_id", V::String(v.effect_id.as_str()))),
            4 => Some(("origin", V::Origin(v.origin))),
            5 => Some(("policy_revision", V::String(v.policy_revision))),
            6 => Some(("route", V::String(v.route.as_str()))),
            7 => Some(("thread_generation", V::String(v.thread_generation.as_str()))),
            8 => Some(("thread_id", V::String(v.thread_id.as_str()))),
            9 => Some(("tool_id", V::String(v.tool_id.as_str()))),
            10 => Some(("tool_revision", V::String(v.tool_revision.as_str()))),
            _ => None,
        },
        V::Scope(v) => match index {
            0 => Some(("scope", V::ScopeBody(v))),
            1 => Some(("version", V::U64(1))),
            _ => None,
        },
        V::TaskSpec(v) => match index {
            0 => Some(("effect_id", V::String(v.effect_id.as_str()))),
            1 => Some(("id", V::String(v.id.as_str()))),
            2 => Some(("recovery", V::String(recovery_name(v.recovery)))),
            3 => Some(("requires_approval", V::Bool(v.requires_approval))),
            _ => None,
        },
        V::Definition(v) => match index {
            0 => Some(("batches", V::Sequence(Sequence::Batches(&v.batches)))),
            1 => Some(("max_attempts", V::U64(u64::from(v.max_attempts)))),
            2 => Some(("revision", V::String(v.revision.as_str()))),
            3 => Some(("route", V::String(v.route.as_str()))),
            _ => None,
        },
        V::Pending(v) => match index {
            0 => Some(("id", V::String(v.id.as_str()))),
            1 => Some(("prompt", V::Json(&v.prompt))),
            _ => None,
        },
        V::TaskResult(v) => match index {
            0 => Some(("output", V::Json(&v.output))),
            1 => Some(("writes", V::Sequence(Sequence::Writes(&v.writes)))),
            _ => None,
        },
        V::TaskCall(v) => match index {
            0 => Some(("input", V::Input(&v.input))),
            1 => Some(("task", V::TaskSpec(&v.spec))),
            2 => Some(("version", V::U64(1))),
            _ => None,
        },
        V::LegacyProposal { task, prompt } => match index {
            0 => Some(("input", V::Input(&task.input))),
            1 => Some(("prompt", V::Json(prompt))),
            2 => Some(("task", V::TaskSpec(&task.spec))),
            3 => Some(("version", V::U64(1))),
            _ => None,
        },
        V::Record(BorrowedRecord::Acceptance(v)) => match index {
            0 => Some(("application", V::Application(&v.application))),
            1 => Some(("before_snapshot", V::Snapshot(&v.before_snapshot))),
            2 => Some(("committed_revision", V::U64(v.committed_revision))),
            3 => Some(("decisions", V::Sequence(Sequence::Entries(&v.decisions)))),
            4 => Some(("facts", V::Facts(&v.facts))),
            5 => Some(("origin", V::Origin(&v.origin))),
            6 => Some(("policy_revision", V::String(&v.policy_revision))),
            7 => Some(("record_version", V::U64(u64::from(v.record_version)))),
            8 => Some(("thread_generation", V::String(v.thread_generation.as_str()))),
            9 => Some(("thread_id", V::String(v.thread_id.as_str()))),
            10 => Some(("turn_id", V::String(v.turn_id.as_str()))),
            _ => None,
        },
        V::Record(BorrowedRecord::Decision(v)) => match index {
            0 => Some((
                "creator_execution_id",
                V::String(v.creator_execution_id.as_str()),
            )),
            1 => Some((
                "creator_interrupt_id",
                V::String(v.creator_interrupt_id.as_str()),
            )),
            2 => Some(("creator_task_id", V::String(v.creator_task_id.as_str()))),
            3 => Some(("creator_turn_id", V::String(v.creator_turn_id.as_str()))),
            4 => Some(("permission_id", V::String(v.permission_id.as_str()))),
            5 => Some(("record_version", V::U64(u64::from(v.record_version)))),
            6 => Some(("scope", V::ScopeBody(ScopeParts::from_scope(&v.scope)))),
            7 => Some(("scope_digest", V::String(&v.scope_digest))),
            _ => None,
        },
        V::Record(BorrowedRecord::Use(v)) => match index {
            0 => Some(("application", V::Application(&v.application))),
            1 => Some(("before_snapshot", V::Snapshot(&v.before_snapshot))),
            2 => Some(("call_digest", V::String(&v.call_digest))),
            3 => Some(("committed_revision", V::U64(v.committed_revision))),
            4 => Some((
                "constraint_revision",
                V::String(v.constraint_revision.as_str()),
            )),
            5 => Some(("execution_id", V::String(v.execution_id.as_str()))),
            6 => Some(("facts", V::Facts(&v.facts))),
            7 => Some(("marker", V::Pending(&v.marker))),
            8 => Some(("permission_id", V::String(v.permission_id.as_str()))),
            9 => Some((
                "permission_version",
                V::U64(u64::from(v.permission_version)),
            )),
            10 => Some(("policy_revision", V::String(&v.policy_revision))),
            11 => Some(("record_version", V::U64(u64::from(v.record_version)))),
            12 => Some(("task_id", V::String(v.task_id.as_str()))),
            13 => Some(("thread_generation", V::String(v.thread_generation.as_str()))),
            14 => Some(("thread_id", V::String(v.thread_id.as_str()))),
            15 => Some(("tool_id", V::String(v.tool_id.as_str()))),
            16 => Some(("tool_revision", V::String(v.tool_revision.as_str()))),
            17 => Some(("use_id", V::String(v.use_id.as_str()))),
            _ => None,
        },
        V::Entry(v) => match index {
            0 => Some(("binding", v.binding.as_ref().map_or(V::Null, V::Binding))),
            1 => Some(("decision", V::String(decision_name(v.decision)))),
            2 => Some(("grant_ref", V::Grant(&v.grant_ref))),
            3 => Some(("interrupt_id", V::String(v.interrupt_id.as_str()))),
            4 => Some((
                "permission_id",
                v.permission_id
                    .as_ref()
                    .map_or(V::Null, |id| V::String(id.as_str())),
            )),
            5 => Some(("proposal_digest", V::String(&v.proposal_digest))),
            6 => Some(("task_id", V::String(v.task_id.as_str()))),
            7 => Some(("task_parking_revision", V::U64(v.task_parking_revision))),
            _ => None,
        },
        V::Write(b4_contract::StateWrite::Replace { field, value }) => match index {
            0 => Some(("field", V::String(field))),
            1 => Some(("type", V::String("replace"))),
            2 => Some(("value", V::Json(value))),
            _ => None,
        },
        V::Write(b4_contract::StateWrite::Append { field, items }) => match index {
            0 => Some(("field", V::String(field))),
            1 => Some(("items", V::Sequence(Sequence::Values(items)))),
            2 => Some(("type", V::String("append"))),
            _ => None,
        },

        V::Snapshot(v) => {
            let adjusted = if v.output.is_none() && index >= 6 {
                index + 1
            } else {
                index
            };
            match adjusted {
                0 => Some((
                    "attempt_history",
                    V::Sequence(Sequence::Attempts(&v.attempt_history)),
                )),
                1 => Some(("attempts_used", V::U64(u64::from(v.attempts_used)))),
                2 => Some(("batch_index", V::Usize(v.batch_index))),
                3 => Some(("definition", V::Definition(&v.definition))),
                4 => Some(("execution_id", V::String(v.execution_id.as_str()))),
                5 => Some(("failure", v.failure.as_deref().map_or(V::Null, V::String))),
                6 => v.output.as_ref().map(|output| ("output", V::Json(output))),
                7 => Some(("owner_generation", V::U64(v.owner_generation))),
                8 => Some(("revision", V::U64(v.revision))),
                9 => Some(("schema_version", V::U64(u64::from(v.schema_version)))),
                10 => Some(("status", V::String(execution_status(v.status)))),
                11 => Some(("tasks", V::Sequence(Sequence::Tasks(&v.tasks)))),
                12 => Some((
                    "turn_history",
                    V::Sequence(Sequence::Turns(&v.turn_history)),
                )),
                13 => Some(("turn_id", V::String(v.turn_id.as_str()))),
                14 => Some(("updated_at_ms", V::U64(v.updated_at_ms))),
                15 => Some(("values", V::Input(&v.values))),
                _ => None,
            }
        }
        V::TaskRecord(v) => {
            let adjusted = if v.failure.is_none() && index >= 2 {
                index + 1
            } else {
                index
            };
            match adjusted {
                0 => Some(("approved", V::Bool(v.approved))),
                1 => Some((
                    "attempt_id",
                    v.attempt_id
                        .as_ref()
                        .map_or(V::Null, |id| V::String(id.as_str())),
                )),
                2 => v
                    .failure
                    .as_deref()
                    .map(|failure| ("failure", V::String(failure))),
                3 => Some(("input", V::Input(&v.input))),
                4 => Some((
                    "interrupt",
                    v.interrupt.as_ref().map_or(V::Null, V::Pending),
                )),
                5 => Some(("result", v.result.as_ref().map_or(V::Null, V::TaskResult))),
                6 => Some(("spec", V::TaskSpec(&v.spec))),
                7 => Some(("status", V::String(task_status(v.status)))),
                _ => None,
            }
        }
        _ => None,
    }
}

enum Cursor<'a> {
    Closed {
        value: CanonicalValue<'a>,
        index: usize,
    },
    Map {
        values: serde_json::map::Iter<'a>,
        index: usize,
    },
    Array {
        values: Sequence<'a>,
        index: usize,
    },
}

impl<'a> Cursor<'a> {
    fn next(&mut self) -> Option<(Option<&'a str>, CanonicalValue<'a>, bool)> {
        match self {
            Self::Closed { value, index } => {
                let (key, child) = closed_child(*value, *index)?;
                let comma = *index > 0;
                *index += 1;
                Some((Some(key), child, comma))
            }
            Self::Map { values, index } => {
                let (key, value) = values.next()?;
                let comma = *index > 0;
                *index += 1;
                Some((Some(key.as_str()), CanonicalValue::Json(value), comma))
            }
            Self::Array { values, index } => {
                let child = values.child(*index)?;
                let comma = *index > 0;
                *index += 1;
                Some((None, child, comma))
            }
        }
    }

    fn close(&self) -> &'static [u8] {
        match self {
            Self::Array { .. } => b"]",
            _ => b"}",
        }
    }
}

struct CanonicalFrame<'a> {
    cursor: Cursor<'a>,
    depth: usize,
}

struct WalkState {
    measured: CanonicalMeasure,
    maximum: usize,
    maximum_nodes: usize,
    pay_output: bool,
    failed: bool,
}

struct ScalarWriter<'a> {
    output: &'a mut dyn std::io::Write,
    budget: &'a mut PermissionEvidenceBudget,
    state: &'a mut WalkState,
}

impl std::io::Write for ScalarWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self.state.measured.bytes.checked_add(bytes.len());
        if next.is_none_or(|length| length > self.state.maximum) {
            self.state.failed = true;
            self.budget.exhausted = true;
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        }
        if self.state.pay_output && self.budget.reserve(bytes.len(), 0).is_err() {
            self.state.failed = true;
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        }
        self.output.write_all(bytes)?;
        self.state.measured.bytes = next.expect("checked bounded byte count");
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn piece(
    output: &mut dyn std::io::Write,
    state: &mut WalkState,
    budget: &mut PermissionEvidenceBudget,
    bytes: &[u8],
) -> Result<(), PermissionError> {
    std::io::Write::write_all(
        &mut ScalarWriter {
            output,
            state,
            budget,
        },
        bytes,
    )
    .map_err(|_| {
        if state.failed {
            PermissionError::LimitExceeded
        } else {
            PermissionError::CorruptRecord
        }
    })
}

fn scalar<T: serde::Serialize + ?Sized>(
    value: &T,
    string_bytes: Option<usize>,
    output: &mut dyn std::io::Write,
    state: &mut WalkState,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    // serde's escaped-string formatter can scan before its first output callback.
    // This prepayment protects arbitrary external borrowed strings and keys.
    budget.reserve(
        match string_bytes {
            Some(length) => mul(6, length)?,
            None => 32,
        },
        0,
    )?;
    serde_json::to_writer(
        ScalarWriter {
            output,
            state,
            budget,
        },
        value,
    )
    .map_err(|_| {
        if state.failed {
            PermissionError::LimitExceeded
        } else {
            PermissionError::CorruptRecord
        }
    })
}

fn canonical_node(
    state: &mut WalkState,
    pay: bool,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    if state.measured.nodes == state.maximum_nodes {
        return budget.limit();
    }
    if pay {
        budget.reserve(0, 1)?;
    }
    state.measured.nodes += 1;
    Ok(())
}

fn walk(
    value: CanonicalValue<'_>,
    limit: CanonicalLimit,
    output: &mut dyn std::io::Write,
    budget: &mut PermissionEvidenceBudget,
    pay_output: bool,
) -> Result<CanonicalMeasure, PermissionError> {
    if std::mem::size_of::<CanonicalFrame<'_>>() > 512 {
        return Err(PermissionError::CorruptRecord);
    }
    budget.reserve(mul(add(limit.depth(), 1)?, 512)?, 0)?;
    let mut stack = Vec::<CanonicalFrame<'_>>::new();
    stack
        .try_reserve_exact(add(limit.depth(), 1)?)
        .map_err(|_| PermissionError::LimitExceeded)?;
    walk_with_stack(value, limit, output, budget, pay_output, &mut stack)
}

// The one canonical loop serves both the retained allocating wrapper and the
// private pair workspace. Stack ownership and payment belong to each caller.
fn walk_with_stack<'data>(
    value: CanonicalValue<'data>,
    limit: CanonicalLimit,
    output: &mut dyn std::io::Write,
    budget: &mut PermissionEvidenceBudget,
    pay_output: bool,
    stack: &mut Vec<CanonicalFrame<'data>>,
) -> Result<CanonicalMeasure, PermissionError> {
    let mut state = WalkState {
        measured: CanonicalMeasure {
            bytes: 0,
            nodes: 0,
            depth: 0,
        },
        maximum: limit.bytes(),
        maximum_nodes: limit.nodes(),
        pay_output,
        failed: false,
    };
    let mut pending = Some((value, 0usize));
    loop {
        if let Some((mut value, parent_depth)) = pending.take() {
            canonical_node(&mut state, pay_output, budget)?;
            if let CanonicalValue::Json(json) = value {
                value = match json {
                    serde_json::Value::Null => CanonicalValue::Null,
                    serde_json::Value::Bool(value) => CanonicalValue::Bool(*value),
                    serde_json::Value::Number(value) => CanonicalValue::Number(value),
                    serde_json::Value::String(value) => CanonicalValue::String(value),
                    serde_json::Value::Array(value) => {
                        CanonicalValue::Sequence(Sequence::Values(value))
                    }
                    serde_json::Value::Object(value) => CanonicalValue::Input(value),
                };
            }
            let cursor = match value {
                CanonicalValue::Null => {
                    scalar(&(), None, output, &mut state, budget)?;
                    None
                }
                CanonicalValue::Bool(value) => {
                    scalar(&value, None, output, &mut state, budget)?;
                    None
                }
                CanonicalValue::U64(value) => {
                    scalar(&value, None, output, &mut state, budget)?;
                    None
                }
                CanonicalValue::Usize(value) => {
                    scalar(&value, None, output, &mut state, budget)?;
                    None
                }
                CanonicalValue::Number(value) => {
                    scalar(value, None, output, &mut state, budget)?;
                    None
                }
                CanonicalValue::String(value) => {
                    scalar(value, Some(value.len()), output, &mut state, budget)?;
                    None
                }
                CanonicalValue::Sequence(values) => {
                    piece(output, &mut state, budget, b"[")?;
                    Some(Cursor::Array { values, index: 0 })
                }
                CanonicalValue::Input(values) => {
                    piece(output, &mut state, budget, b"{")?;
                    Some(Cursor::Map {
                        values: values.iter(),
                        index: 0,
                    })
                }
                value => {
                    piece(output, &mut state, budget, b"{")?;
                    Some(Cursor::Closed { value, index: 0 })
                }
            };
            if let Some(cursor) = cursor {
                let depth = add(parent_depth, 1)?;
                if depth > limit.depth() {
                    return budget.limit();
                }
                state.measured.depth = state.measured.depth.max(depth);
                stack.push(CanonicalFrame { cursor, depth });
            }
        } else if let Some(frame) = stack.last_mut() {
            if let Some((key, value, comma)) = frame.cursor.next() {
                if comma {
                    piece(output, &mut state, budget, b",")?;
                }
                if let Some(key) = key {
                    canonical_node(&mut state, pay_output, budget)?;
                    scalar(key, Some(key.len()), output, &mut state, budget)?;
                    piece(output, &mut state, budget, b":")?;
                }
                pending = Some((value, frame.depth));
            } else {
                piece(output, &mut state, budget, frame.cursor.close())?;
                stack.pop();
            }
        } else {
            break;
        }
    }
    Ok(state.measured)
}

fn measure(
    value: CanonicalValue<'_>,
    limit: CanonicalLimit,
    budget: &mut PermissionEvidenceBudget,
) -> Result<CanonicalMeasure, PermissionError> {
    walk(value, limit, &mut std::io::sink(), budget, true)
}

struct HashWriter(sha2::Sha256);
impl std::io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        sha2::Digest::update(&mut self.0, bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn hash_canonical(
    value: CanonicalValue<'_>,
    limit: CanonicalLimit,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    let measured = measure(value, limit, budget)?;
    budget.reserve(measured.bytes, measured.nodes)?;
    let mut output = HashWriter(<sha2::Sha256 as sha2::Digest>::new());
    let emitted = walk(value, limit, &mut output, budget, false)?;
    if emitted.bytes != measured.bytes
        || emitted.nodes != measured.nodes
        || emitted.depth != measured.depth
    {
        return Err(PermissionError::CorruptRecord);
    }
    budget.reserve(96, 0)?;
    if budget.ready_authorization_writer_entered || budget.runtime_use_entered {
        // New intrinsic hex output: copy/allocation plus its unique String end.
        // The old 96-byte request quote remains a separate positive debit.
        budget.reserve(add(mul(13, 64)?, 256)?, 2)?;
    }
    let digest = sha2::Digest::finalize(output.0);
    let mut result = String::new();
    result
        .try_reserve_exact(64)
        .map_err(ready_authorization_checked_metadata_value(|_| {
            PermissionError::LimitExceeded
        }))?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    Ok(result)
}

struct ExactWriter {
    bytes: Vec<u8>,
    maximum: usize,
}
impl std::io::Write for ExactWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().checked_add(bytes.len()).is_none_or(
            ready_authorization_checked_metadata_value(|length: usize| length > self.maximum),
        ) {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode(
    record: BorrowedRecord<'_>,
    kind: RecordKind,
    budget: &mut PermissionEvidenceBudget,
) -> Result<Vec<u8>, PermissionError> {
    let value = CanonicalValue::Record(record);
    let limit = CanonicalLimit::Record(kind);
    let measured = measure(value, limit, budget)?;
    check_inherent(record, &measured, budget)?;
    budget.reserve(measured.bytes, measured.nodes)?;
    let mut output = ExactWriter {
        bytes: Vec::new(),
        maximum: measured.bytes,
    };
    output.bytes.try_reserve_exact(measured.bytes).map_err(
        ready_authorization_checked_metadata_value(|_| PermissionError::LimitExceeded),
    )?;
    let emitted = walk(value, limit, &mut output, budget, false)?;
    if emitted.bytes != measured.bytes
        || emitted.nodes != measured.nodes
        || emitted.depth != measured.depth
        || output.bytes.len() != measured.bytes
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(output.bytes)
}

fn corrupt<T>() -> Result<T, PermissionError> {
    Err(PermissionError::CorruptRecord)
}

fn reserve_measure(
    measured: CanonicalMeasure,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(measured.bytes, measured.nodes)
}

fn checked_definition(
    definition: &Definition,
    budget: &mut PermissionEvidenceBudget,
) -> Result<usize, PermissionError> {
    let measured = measure(
        CanonicalValue::Definition(definition),
        CanonicalLimit::Evidence64KiB,
        budget,
    )?;
    reserve_measure(measured, budget)?;
    if definition.batches.is_empty()
        || definition.batches.len() > 64
        || definition
            .batches
            .iter()
            .any(ready_authorization_checked_metadata_value(
                Vec::<TaskSpec>::is_empty,
            ))
    {
        return corrupt();
    }
    let count = definition
        .batches
        .iter()
        .map(ready_authorization_checked_metadata_value(
            Vec::<TaskSpec>::len,
        ))
        .try_fold(
            0usize,
            ready_authorization_checked_metadata_value(
                add as fn(usize, usize) -> Result<usize, PermissionError>,
            ),
        )?;
    if count == 0
        || count > 64
        || definition.max_attempts == 0
        || definition.max_attempts > 64
        || u64::from(definition.max_attempts) < count as u64
    {
        return corrupt();
    }
    let mut bytes = 0;
    for task in definition.batches.iter().flatten() {
        bytes = add(
            bytes,
            add(task.id.as_str().len(), task.effect_id.as_str().len())?,
        )?;
    }
    budget.reserve(mul(mul(2, count)?, bytes)?, mul(4, mul(count, count)?)?)?;
    for (index, task) in definition.batches.iter().flatten().enumerate() {
        for other in definition.batches.iter().flatten().skip(index + 1) {
            if task.id == other.id || task.effect_id == other.effect_id {
                return corrupt();
            }
        }
    }
    Ok(count)
}

fn id_family(
    k: usize,
    bytes: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(mul(mul(2, k)?, bytes)?, mul(2, mul(k, k)?)?)
}

fn equal_input(
    left: &serde_json::Map<String, serde_json::Value>,
    right: &serde_json::Map<String, serde_json::Value>,
    budget: &mut PermissionEvidenceBudget,
    limit: CanonicalLimit,
) -> Result<(), PermissionError> {
    let left_quote = measure(CanonicalValue::Input(left), limit, budget)?;
    let right_quote = measure(CanonicalValue::Input(right), limit, budget)?;
    budget.reserve(
        add(left_quote.bytes, right_quote.bytes)?,
        add(left_quote.nodes, right_quote.nodes)?,
    )?;
    // The actual comparison is performed by old validate_snapshot, after these fees.
    Ok(())
}

fn checked_snapshot(
    snapshot: &Snapshot,
    definition_count: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    checked_snapshot_profile(
        snapshot,
        definition_count,
        budget,
        CanonicalLimit::Evidence64KiB,
        false,
    )
}
fn checked_snapshot_profile(
    snapshot: &Snapshot,
    definition_count: usize,
    budget: &mut PermissionEvidenceBudget,
    limit: CanonicalLimit,
    generic_resource: bool,
) -> Result<(), PermissionError> {
    checked_snapshot_profile_capture(snapshot, definition_count, budget, limit, generic_resource)
        .map(ready_authorization_checked_metadata_value(
            |_: CanonicalMeasure| (),
        ))
}
fn checked_snapshot_profile_capture(
    snapshot: &Snapshot,
    definition_count: usize,
    budget: &mut PermissionEvidenceBudget,
    limit: CanonicalLimit,
    generic_resource: bool,
) -> Result<CanonicalMeasure, PermissionError> {
    let measured = measure(CanonicalValue::Snapshot(snapshot), limit, budget)?;
    reserve_measure(measured, budget)?;
    let h = snapshot.turn_history.len();
    let a = snapshot.attempt_history.len();
    let t = snapshot.tasks.len();
    // This fee precedes the quote scans/counts and kernel's repeated shallow scans.
    budget.reserve(0, mul(25, add(add(h, a)?, add(definition_count, t)?)?)?)?;
    let u = snapshot
        .tasks
        .iter()
        .filter(ready_authorization_checked_metadata_value(
            |task: &&TaskRecord| task.attempt_id.is_some(),
        ))
        .count();
    let p = snapshot
        .tasks
        .iter()
        .filter(ready_authorization_checked_metadata_value(
            |task: &&TaskRecord| task.interrupt.is_some(),
        ))
        .count();
    let batch = snapshot
        .definition
        .batches
        .get(snapshot.batch_index)
        .map(ready_authorization_checked_metadata_value(
            Vec::<b4_contract::TaskSpec>::as_slice,
        ))
        .unwrap_or(&[]);
    let b = batch.len();
    // expected may have d entries even before a malformed shorter tasks vector
    // is rejected; reserve d for that Vec independently of t.
    let slots = add(add(add(h, a)?, mul(4, definition_count)?)?, add(u, p)?)?;
    budget.reserve(mul(128, slots)?, 0)?;
    let turn_bytes = snapshot
        .turn_history
        .iter()
        .map(ready_authorization_checked_metadata_value(|id: &TurnId| {
            id.as_str().len()
        }))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    let attempt_bytes = snapshot
        .attempt_history
        .iter()
        .map(ready_authorization_checked_metadata_value(
            |id: &b4_contract::AttemptId| id.as_str().len(),
        ))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    let spec_id_bytes = snapshot
        .definition
        .batches
        .iter()
        .flatten()
        .map(ready_authorization_checked_metadata_value(
            |spec: &b4_contract::TaskSpec| spec.id.as_str().len(),
        ))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    let spec_effect_bytes = snapshot
        .definition
        .batches
        .iter()
        .flatten()
        .map(ready_authorization_checked_metadata_value(
            |spec: &b4_contract::TaskSpec| spec.effect_id.as_str().len(),
        ))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    let task_attempt_bytes = snapshot
        .tasks
        .iter()
        .filter_map(ready_authorization_checked_task_attempt(|task| {
            task.attempt_id.as_ref()
        }))
        .map(ready_authorization_checked_metadata_value(
            |id: &b4_contract::AttemptId| id.as_str().len(),
        ))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    let interrupt_bytes = snapshot
        .tasks
        .iter()
        .filter_map(ready_authorization_checked_task_interrupt(|task| {
            task.interrupt.as_ref()
        }))
        .map(ready_authorization_checked_metadata_value(
            |value: &PendingApproval| value.id.as_str().len(),
        ))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    id_family(h, turn_bytes, budget)?;
    id_family(a, attempt_bytes, budget)?;
    id_family(definition_count, spec_id_bytes, budget)?;
    id_family(definition_count, spec_effect_bytes, budget)?;
    id_family(u, task_attempt_bytes, budget)?;
    id_family(p, interrupt_bytes, budget)?;
    budget.reserve(
        add(mul(u, attempt_bytes)?, mul(a, task_attempt_bytes)?)?,
        mul(2, mul(u, a)?)?,
    )?;
    let task_ids = snapshot
        .tasks
        .iter()
        .map(ready_authorization_checked_metadata_value(
            |task: &TaskRecord| task.spec.id.as_str().len(),
        ))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    let batch_ids = batch
        .iter()
        .map(ready_authorization_checked_metadata_value(
            |spec: &b4_contract::TaskSpec| spec.id.as_str().len(),
        ))
        .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
    // Both this quote loop and old kernel perform the current-batch membership scan.
    budget.reserve(
        mul(2, add(mul(b, task_ids)?, mul(t, batch_ids)?)?)?,
        mul(4, mul(t, b)?)?,
    )?;
    let mut previous_input = None;
    for (task, spec) in snapshot.tasks.iter().zip(
        snapshot
            .definition
            .batches
            .iter()
            .take(snapshot.batch_index.saturating_add(1))
            .flatten(),
    ) {
        let task_quote = measure(CanonicalValue::TaskSpec(&task.spec), limit, budget)?;
        let spec_quote = measure(CanonicalValue::TaskSpec(spec), limit, budget)?;
        budget.reserve(
            add(task_quote.bytes, spec_quote.bytes)?,
            add(task_quote.nodes, spec_quote.nodes)?,
        )?;
        if batch.iter().any(ready_authorization_checked_metadata_value(
            |member: &b4_contract::TaskSpec| member.id == task.spec.id,
        )) {
            if let Some(previous) = previous_input {
                equal_input(previous, &task.input, budget, limit)?;
            }
            previous_input = Some(&task.input);
        }
    }
    if let Some(input) = previous_input {
        equal_input(input, &snapshot.values, budget, limit)?;
    }
    if generic_resource {
        resource_terminal_quote(snapshot, limit, budget)?;
    }
    budget.reserve(4096, 0)?;
    b4_kernel::validate_snapshot(snapshot).map_err(ready_authorization_checked_metadata_value(
        |_: b4_kernel::KernelError| PermissionError::CorruptRecord,
    ))?;
    Ok(measured)
}

fn labels(
    application: &PermissionApplicationIdentity,
    policy: &str,
    origin: Option<&ThreadOrigin>,
) -> Result<(), PermissionError> {
    if !valid_label(&application.application)
        || !valid_label(&application.revision)
        || !valid_label(policy)
    {
        return corrupt();
    }
    if let Some(origin) = origin {
        if !valid_label(&origin.application)
            || !valid_label(&origin.tenant)
            || !valid_label(&origin.principal)
            || origin.application != application.application
        {
            return corrupt();
        }
    }
    Ok(())
}

fn digest_shape(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(ready_authorization_checked_metadata_value(|byte: u8| {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }))
}

fn check_inherent(
    record: BorrowedRecord<'_>,
    measured: &CanonicalMeasure,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    // Pay the root/shallow counting/label/selection scans before any such scans.
    reserve_measure(*measured, budget)?;
    match record {
        BorrowedRecord::Decision(value) => {
            if value.record_version != 1 {
                return corrupt();
            }
            let scope = &value.scope;
            labels(
                &scope.application,
                &scope.policy_revision,
                Some(&scope.origin),
            )?;
            let d = checked_definition(&scope.definition, budget)?;
            budget.reserve(mul(512, d)?, mul(2, d)?)?;
            if scope.route != scope.definition.route
                || !scope.definition.batches.iter().flatten().any(
                    ready_authorization_checked_metadata_value(|spec: &TaskSpec| {
                        spec.effect_id == scope.effect_id && spec.requires_approval
                    }),
                )
            {
                return corrupt();
            }
            let digest = hash_canonical(
                CanonicalValue::Scope(ScopeParts::from_scope(scope)),
                CanonicalLimit::Evidence64KiB,
                budget,
            )?;
            budget.reserve(512, 8)?;
            if !digest_shape(&value.scope_digest)
                || digest != value.scope_digest
                || value.permission_id.as_str().strip_prefix("b4pd_") != Some(digest.as_str())
            {
                return corrupt();
            }
        }
        BorrowedRecord::Acceptance(value) => {
            use b4_contract::{ExecutionStatus as E, TaskStatus as T};
            let before = &value.before_snapshot;
            labels(
                &value.application,
                &value.policy_revision,
                Some(&value.origin),
            )?;
            if value.record_version != 1
                || before.status != E::WaitingForApproval
                || before.failure.is_some()
            {
                return corrupt();
            }
            let count = checked_definition(&before.definition, budget)?;
            checked_snapshot(before, count, budget)?;
            let revision = before
                .revision
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?;
            budget.reserve(
                mul(512, add(before.turn_history.len(), 1)?)?,
                mul(2, add(before.turn_history.len(), 1)?)?,
            )?;
            if value.committed_revision != revision
                || value.facts.now_ms < before.updated_at_ms
                || value.turn_id == before.turn_id
                || before.turn_history.contains(&value.turn_id)
                || value.decisions.is_empty()
                || value.decisions.len() > 64
            {
                return corrupt();
            }
            // Eight conservative 512-byte pair units cover repeated typed-ID,
            // binding, digest shape/equality and permission suffix comparisons.
            budget.reserve(
                mul(4096, value.decisions.len())?,
                mul(32, add(before.tasks.len(), value.decisions.len())?)?,
            )?;
            let waiting = before
                .tasks
                .iter()
                .filter(ready_authorization_checked_metadata_value(
                    |task: &&TaskRecord| task.status == T::WaitingForApproval,
                ));
            if waiting.clone().count() != value.decisions.len() {
                return corrupt();
            }
            let mut always = false;
            for (task, entry) in waiting.zip(&value.decisions) {
                let Some(interrupt) = &task.interrupt else {
                    return corrupt();
                };
                if entry.task_id != task.spec.id
                    || entry.interrupt_id != interrupt.id
                    || entry.grant_ref.execution_id != before.execution_id
                    || entry.grant_ref.interrupt_id != interrupt.id
                    || entry.task_parking_revision == 0
                    || entry.task_parking_revision > before.revision
                {
                    return corrupt();
                }
                let proposal = bounded_permission_proposal_digest(task, &interrupt.prompt, budget)?;
                if !digest_shape(&entry.proposal_digest) || proposal != entry.proposal_digest {
                    return corrupt();
                }
                match entry.decision {
                    ApprovalDecision::Deny => {
                        if entry.binding.is_some() || entry.permission_id.is_some() {
                            return corrupt();
                        }
                    }
                    ApprovalDecision::Once | ApprovalDecision::Always => {
                        let Some(binding) = &entry.binding else {
                            return corrupt();
                        };
                        if binding.effect_id != task.spec.effect_id {
                            return corrupt();
                        }
                        if entry.decision == ApprovalDecision::Once {
                            if entry.permission_id.is_some() {
                                return corrupt();
                            }
                        } else {
                            always = true;
                            let parts = ScopeParts {
                                thread_id: &value.thread_id,
                                thread_generation: &value.thread_generation,
                                origin: &value.origin,
                                application: &value.application,
                                route: &before.definition.route,
                                definition: &before.definition,
                                effect_id: &binding.effect_id,
                                tool_id: &binding.tool_id,
                                tool_revision: &binding.tool_revision,
                                policy_revision: &value.policy_revision,
                                constraint_revision: &binding.constraint_revision,
                            };
                            let digest = hash_canonical(
                                CanonicalValue::Scope(parts),
                                CanonicalLimit::Evidence64KiB,
                                budget,
                            )?;
                            if entry.permission_id.as_ref().map(
                                ready_use_checked_permission_suffix(|id: &PermissionId| {
                                    id.as_str().strip_prefix("b4pd_")
                                }),
                            ) != Some(Some(digest.as_str()))
                            {
                                return corrupt();
                            }
                        }
                    }
                }
            }
            if !always {
                return corrupt();
            }
            // Borrowed Resume derivation: complete exact-set decisions make each
            // waiting record Ready/approved or Denied/unapproved, retaining its
            // actual interrupt/spec/input/history. At least one Always keeps
            // this batch Ready, so settle cannot reduce/advance/clone output.
            // Fresh turn, incremented revision and checked time were verified.
            // No owned command, proposed snapshot or dispatchable vector is built.
            // At an exhausted attempt budget the derived dispatchable set is
            // empty even though the approved Ready records remain legal.
        }
        BorrowedRecord::Use(value) => {
            use b4_contract::{ExecutionStatus as E, TaskStatus as T};
            let before = &value.before_snapshot;
            labels(&value.application, &value.policy_revision, None)?;
            if value.record_version != 1
                || value.permission_version != 1
                || before.status != E::Running
                || before.failure.is_some()
            {
                return corrupt();
            }
            let count = checked_definition(&before.definition, budget)?;
            checked_snapshot(before, count, budget)?;
            let revision = before
                .revision
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?;
            budget.reserve(2048, 16)?;
            if value.committed_revision != revision
                || value.facts.now_ms < before.updated_at_ms
                || value.execution_id != before.execution_id
                || value.marker.id.as_str() != value.use_id.as_str()
                || !value.marker.prompt.is_null()
            {
                return corrupt();
            }
            budget.reserve(mul(512, before.tasks.len())?, mul(2, before.tasks.len())?)?;
            let Some(task) = before
                .tasks
                .iter()
                .find(ready_authorization_checked_metadata_value(
                    |task: &&TaskRecord| task.spec.id == value.task_id,
                ))
            else {
                return corrupt();
            };
            let Some(batch) = before.definition.batches.get(before.batch_index) else {
                return corrupt();
            };
            let selected = measure(
                CanonicalValue::TaskSpec(&task.spec),
                CanonicalLimit::Evidence64KiB,
                budget,
            )?;
            for spec in batch {
                let candidate = measure(
                    CanonicalValue::TaskSpec(spec),
                    CanonicalLimit::Evidence64KiB,
                    budget,
                )?;
                budget.reserve(
                    add(selected.bytes, candidate.bytes)?,
                    add(selected.nodes, candidate.nodes)?,
                )?;
            }
            if !batch.iter().any(ready_authorization_checked_metadata_value(
                |spec: &TaskSpec| spec == &task.spec,
            )) || !task.spec.requires_approval
                || task.status != T::Ready
                || task.approved
                || task.attempt_id.is_some()
                || task.result.is_some()
                || task.failure.is_some()
                || task.interrupt.is_some()
            {
                return corrupt();
            }
            let digest = hash_canonical(
                CanonicalValue::TaskCall(task),
                CanonicalLimit::Call16KiB,
                budget,
            )?;
            if !digest_shape(&value.call_digest) || value.call_digest != digest {
                return corrupt();
            }
        }
    }
    Ok(())
}

// Private setup assertions bind the requested frame/slot fee ceilings.
const _: () = assert!(std::mem::size_of::<RawFrame>() <= 512);
const _: () = assert!(std::mem::size_of::<CanonicalFrame<'static>>() <= 512);
const _: () = assert!(std::mem::size_of::<RawContainer>() <= 128);
const _: () = assert!(std::mem::size_of::<KeySpan>() <= 32);
const _: () = assert!(std::mem::size_of::<PermissionAcceptedDecision>() <= 512);

// Fixed resource-side account state. It is never owned by a transient session.
struct ResourceLedger {
    reads: usize,
    combined_reads: usize,
    immediate_writes: usize,
    referenced_grants: usize,
    sql_capacity: usize,
    attempted_keys: usize,
    executions: usize,
    key_bytes: usize,
    keys: Vec<Vec<u8>>,
}
impl ResourceLedger {
    fn new() -> Self {
        Self {
            reads: 0,
            combined_reads: 0,
            immediate_writes: 0,
            referenced_grants: 0,
            sql_capacity: 0,
            attempted_keys: 0,
            executions: 0,
            key_bytes: 0,
            keys: Vec::new(),
        }
    }
}

/// Closed statement admissions for a single coherent token-free resource read.
#[derive(Clone, Copy)]
pub enum PermissionResourceSql {
    BeginDeferred,
    Metadata,
    ExecutionLengths,
    ExecutionPayload,
    ThreadLengths,
    ThreadPayload,
    CommitRead,
}

/// These borrowed lookup identities confer no authority.
#[derive(Clone, Copy)]
pub enum PermissionResourceKey<'a> {
    Metadata,
    Execution(&'a ExecutionId),
    Thread(&'a ThreadId),
}

/// Borrowed, already type/length-guarded SQLite cells; no owned JSON is accepted.
pub struct PermissionExecutionCells<'a> {
    pub execution_id: &'a ExecutionId,
    pub thread_id: &'a [u8],
    pub revision: &'a [u8],
    pub owner_generation: &'a [u8],
    pub snapshot: &'a [u8],
}

pub struct PermissionThreadCells<'a> {
    pub id: &'a ThreadId,
    pub active_execution_id: Option<&'a [u8]>,
    pub created_at_ms: &'a [u8],
    pub metadata: &'a [u8],
    pub origin: Option<&'a [u8]>,
    pub resource_revision: &'a [u8],
    pub generation: &'a [u8],
}

/// A checked borrow of one persistent account, not a fresh ledger.
pub struct PermissionResourceRead<'a> {
    budget: &'a mut PermissionEvidenceBudget,
}

impl PermissionEvidenceBudget {
    pub fn resource_read(&mut self) -> Result<PermissionResourceRead<'_>, PermissionError> {
        self.require_standard_account()?;
        let result = ready_authorization_checked_metadata_value(|| {
            self.reserve(0, 0)?;
            if self.resource.reads == 6 {
                return self.limit();
            }
            self.reserve(128, 1)?;
            self.resource.reads += 1;
            Ok(())
        })();
        self.finish(result)?;
        Ok(PermissionResourceRead { budget: self })
    }
}

fn resource_error(error: PermissionError) -> PermissionError {
    match error {
        PermissionError::CorruptRecord | PermissionError::Kernel(_) => {
            crate::StoreError::Corrupt("permission_resource_invalid".into()).into()
        }
        other => other,
    }
}

impl PermissionResourceRead<'_> {
    pub fn before_sql(
        &mut self,
        statement: PermissionResourceSql,
        key: Option<PermissionResourceKey<'_>>,
    ) -> Result<(), PermissionError> {
        let result = ready_authorization_checked_metadata_value(|| {
            self.budget.reserve(0, 0)?;
            // BEGIN also admits the one possible implicit Transaction::drop rollback.
            // The extra debit is capacity, never a claim of an actual unused attempt.
            let capacity = if matches!(statement, PermissionResourceSql::BeginDeferred) {
                2
            } else {
                1
            };
            let next = add(self.budget.resource.sql_capacity, capacity)?;
            if next > 512 {
                return self.budget.limit();
            }
            // Each statement: <=2048 fixed SQL/bind work +8192 finite Rust call/error scratch.
            self.budget
                .runtime_prepaid_ready_cancellation_fixed(mul(capacity, 10_240)?, capacity)?;
            self.budget.resource.sql_capacity = next;
            if let Some(key) = key {
                self.admit_key(key)?;
            }
            Ok(())
        })();
        self.budget.finish(result)
    }

    fn admit_key(&mut self, key: PermissionResourceKey<'_>) -> Result<(), PermissionError> {
        if self.budget.resource.attempted_keys == 16_384 {
            return self.budget.limit();
        }
        let (domain, label) = match key {
            PermissionResourceKey::Metadata => (0u8, None),
            PermissionResourceKey::Execution(id) => (1, Some(id.as_str())),
            PermissionResourceKey::Thread(id) => (2, Some(id.as_str())),
        };
        let bytes = match label {
            None => 1,
            Some(v) => add(3, v.len())?,
        };
        if bytes > 1024 {
            return self.budget.limit();
        }
        // Fresh production renewal owns each encoded-key copy and its later
        // normal/partial end. Retain the original6B+128 purchase literally;
        // this7B+128 supplement partitions into B copy top-up +6B+128 end.
        if self.budget.use_renewal_production_entered {
            self.budget.reserve(add(mul(7, bytes)?, 128)?, 1)?;
        }
        self.budget.reserve(add(mul(6, bytes)?, 128)?, 1)?;
        self.budget.resource.attempted_keys += 1;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(bytes)
            .map_err(ready_authorization_checked_metadata_value(
                |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
            ))?;
        encoded.push(domain);
        if let Some(label) = label {
            let length =
                u16::try_from(label.len()).map_err(ready_authorization_checked_metadata_value(
                    |_: std::num::TryFromIntError| PermissionError::LimitExceeded,
                ))?;
            encoded.extend_from_slice(&length.to_le_bytes());
            encoded.extend_from_slice(label.as_bytes());
        }
        // Stored totals make this quote O(1), before the entire key search.
        let k = self.budget.resource.keys.len();
        self.budget.reserve(
            add(mul(k, bytes)?, self.budget.resource.key_bytes)?,
            mul(2, k)?,
        )?;
        if self
            .budget
            .resource
            .keys
            .iter()
            .any(ready_authorization_checked_metadata_value(
                |old: &Vec<u8>| old == &encoded,
            ))
        {
            return Ok(());
        }
        if k == 8192 || (domain == 1 && self.budget.resource.executions == 65) {
            return self.budget.limit();
        }
        if k == self.budget.resource.keys.capacity() {
            let capacity = self.budget.resource.keys.capacity();
            let target = if capacity == 0 { 4 } else { mul(capacity, 2)? };
            self.budget
                .reserve(mul(add(target, k)?, std::mem::size_of::<Vec<u8>>())?, 0)?;
            self.budget
                .resource
                .keys
                .try_reserve_exact(target - k)
                .map_err(ready_authorization_checked_metadata_value(
                    |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
                ))?;
        }
        self.budget.resource.key_bytes = add(self.budget.resource.key_bytes, bytes)?;
        if domain == 1 {
            self.budget.resource.executions += 1;
        }
        self.budget.resource.keys.push(encoded);
        Ok(())
    }

    pub fn check_metadata(
        &mut self,
        version: i64,
        checksum: &[u8],
        owner: &[u8],
        expected_owner: u64,
        expected_schema: &[u8],
    ) -> Result<(), PermissionError> {
        let result = ready_authorization_checked_metadata_value(|| {
            self.budget.reserve(add(expected_schema.len(), 512)?, 16)?;
            if version != 4 || checksum.len() != 64 {
                return Err(crate::StoreError::UnsupportedSchema.into());
            }
            let digest = <sha2::Sha256 as sha2::Digest>::digest(expected_schema);
            let hex = b"0123456789abcdef";
            for (index, byte) in digest.iter().enumerate() {
                if checksum[index * 2] != hex[usize::from(*byte >> 4)]
                    || checksum[index * 2 + 1] != hex[usize::from(*byte & 15)]
                {
                    return Err(crate::StoreError::UnsupportedSchema.into());
                }
            }
            let owner = resource_u64(owner)?;
            if owner == 0 || owner != expected_owner {
                return Err(crate::StoreError::OwnerConflict.into());
            }
            Ok(())
        })();
        self.budget
            .finish(result)
            .map_err(ready_authorization_checked_metadata_value(resource_error))
    }

    /// Admit payload bytes before the backend's second guarded point query.
    pub fn admit_execution_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        let result = ready_authorization_checked_metadata_value(|| {
            self.budget.reserve(0, 0)?;
            if thread > 256 || snapshot > 1_048_576 {
                return self.budget.limit();
            }
            self.budget.reserve(add(add(thread, snapshot)?, 512)?, 8)
        })();
        self.budget.finish(result)
    }

    pub fn admit_thread_lengths(
        &mut self,
        id: usize,
        active: usize,
        generation: usize,
        metadata: usize,
        origin: usize,
    ) -> Result<(), PermissionError> {
        let result = ready_authorization_checked_metadata_value(|| {
            self.budget.reserve(0, 0)?;
            let total = add(
                512,
                add(add(id, active)?, add(generation, add(metadata, origin)?)?)?,
            )?;
            if generation != 64 {
                return Err(resource_error(PermissionError::CorruptRecord));
            }
            if id > 256 || active > 256 || total > 262_144 {
                return self.budget.limit();
            }
            self.budget.reserve(total, 12)
        })();
        self.budget.finish(result)
    }

    pub fn decode_execution(
        &mut self,
        cells: PermissionExecutionCells<'_>,
    ) -> Result<crate::StoredExecution, PermissionError> {
        let result = resource_execution(cells, self.budget);
        self.budget
            .finish(result)
            .map_err(ready_authorization_checked_metadata_value(resource_error))
    }
    pub fn decode_thread(
        &mut self,
        cells: PermissionThreadCells<'_>,
    ) -> Result<crate::ThreadRecord, PermissionError> {
        let result = resource_thread(cells, self.budget);
        self.budget
            .finish(result)
            .map_err(ready_authorization_checked_metadata_value(resource_error))
    }
    pub fn check_linkage(
        &mut self,
        id: &ExecutionId,
        thread: &crate::ThreadRecord,
        execution: &crate::StoredExecution,
    ) -> Result<(), PermissionError> {
        let result = ready_authorization_checked_metadata_value(|| {
            self.budget.reserve(
                add(
                    mul(2, thread.id.as_str().len())?,
                    add(
                        execution.thread_id.as_str().len(),
                        mul(2, id.as_str().len())?,
                    )?,
                )?,
                8,
            )?;
            if thread.id != execution.thread_id || thread.active_execution_id.as_ref() != Some(id) {
                return Err(crate::StoreError::ExecutionNotActive.into());
            }
            Ok(())
        })();
        self.budget.finish(result)
    }
}

fn resource_u64(bytes: &[u8]) -> Result<u64, PermissionError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(ready_authorization_checked_metadata_value(
            |_: std::array::TryFromSliceError| PermissionError::CorruptRecord,
        ))?;
    Ok(u64::from_be_bytes(bytes))
}

fn resource_string(
    raw: &[u8],
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    budget.runtime_ready_cancellation_string_failure_cleanup(raw.len())?;
    budget.reserve(add(mul(7, raw.len())?, 128)?, 1)?;
    let value = std::str::from_utf8(raw).map_err(ready_authorization_checked_metadata_value(
        |_: std::str::Utf8Error| PermissionError::CorruptRecord,
    ))?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(raw.len())
        .map_err(ready_authorization_checked_metadata_value(
            |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
        ))?;
    owned.push_str(value);
    Ok(owned)
}

pub(super) fn resource_execution(
    cells: PermissionExecutionCells<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::StoredExecution, PermissionError> {
    resource_execution_capture(cells, budget).map(ready_authorization_checked_metadata_value(
        |(owned, _): (crate::StoredExecution, CanonicalMeasure)| owned,
    ))
}
fn resource_execution_capture(
    cells: PermissionExecutionCells<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(crate::StoredExecution, CanonicalMeasure), PermissionError> {
    budget.reserve(0, 0)?;
    if cells.thread_id.len() > 256 || cells.snapshot.len() > 1_048_576 {
        return budget.limit();
    }
    budget.reserve(1024, 8)?;
    let revision = resource_u64(cells.revision)?;
    let owner = resource_u64(cells.owner_generation)?;
    let thread_id = ThreadId::new(resource_string(cells.thread_id, budget)?).map_err(
        ready_authorization_checked_metadata_value(|_: b4_contract::IdError| {
            PermissionError::CorruptRecord
        }),
    )?;
    preflight_profile(cells.snapshot, RawProfile::Snapshot, budget)?;
    let snapshot: Snapshot = serde_json::from_slice(cells.snapshot).map_err(
        ready_authorization_checked_metadata_value(|_: serde_json::Error| {
            PermissionError::CorruptRecord
        }),
    )?;
    budget.reserve(
        add(
            snapshot.execution_id.as_str().len(),
            cells.execution_id.as_str().len(),
        )?,
        8,
    )?;
    if &snapshot.execution_id != cells.execution_id
        || snapshot.revision != revision
        || snapshot.owner_generation != owner
    {
        return corrupt();
    }
    // The separate count traversal is paid before iterating. It imposes no native64 cap.
    budget.reserve(0, snapshot.definition.batches.len())?;
    let count = snapshot
        .definition
        .batches
        .iter()
        .map(ready_authorization_checked_metadata_value(
            Vec::<b4_contract::TaskSpec>::len,
        ))
        .try_fold(0, ready_authorization_checked_metadata_value(add))?;
    let measured = checked_snapshot_profile_capture(
        &snapshot,
        count,
        budget,
        CanonicalLimit::ResourceSnapshot,
        true,
    )?;
    budget.reserve(std::mem::size_of::<crate::StoredExecution>(), 1)?;
    Ok((
        crate::StoredExecution {
            thread_id,
            snapshot,
        },
        measured,
    ))
}

pub(super) fn resource_thread(
    cells: PermissionThreadCells<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::ThreadRecord, PermissionError> {
    resource_thread_capture(cells, budget).map(ready_authorization_checked_metadata_value(
        |(owned, _): (crate::ThreadRecord, CanonicalMeasure)| owned,
    ))
}
fn resource_thread_capture(
    cells: PermissionThreadCells<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(crate::ThreadRecord, CanonicalMeasure), PermissionError> {
    budget.reserve(0, 0)?;
    let active_len = cells
        .active_execution_id
        .map_or(0, ready_authorization_checked_metadata_value(<[u8]>::len));
    let origin_len = cells
        .origin
        .map_or(0, ready_authorization_checked_metadata_value(<[u8]>::len));
    let total = add(
        512,
        add(
            add(cells.id.as_str().len(), active_len)?,
            add(
                cells.generation.len(),
                add(cells.metadata.len(), origin_len)?,
            )?,
        )?,
    )?;
    if cells.id.as_str().len() > 256 || active_len > 256 || total > 262_144 {
        return budget.limit();
    }
    if cells.generation.len() != 64 {
        return corrupt();
    }
    budget.reserve(1024, 16)?;
    let created_at_ms = resource_u64(cells.created_at_ms)?;
    let resource_revision = resource_u64(cells.resource_revision)?;
    let id = ThreadId::new(resource_string(cells.id.as_str().as_bytes(), budget)?).map_err(
        ready_authorization_checked_metadata_value(|_: b4_contract::IdError| {
            PermissionError::CorruptRecord
        }),
    )?;
    let generation = ThreadGeneration::new(resource_string(cells.generation, budget)?).map_err(
        ready_authorization_checked_metadata_value(|_: b4_contract::IdError| {
            PermissionError::CorruptRecord
        }),
    )?;
    let active_execution_id = cells
        .active_execution_id
        .map(ready_authorization_checked_metadata_value(
            |raw: &[u8]| {
                ExecutionId::new(resource_string(raw, budget)?).map_err(
                    ready_authorization_checked_metadata_value(|_: b4_contract::IdError| {
                        PermissionError::CorruptRecord
                    }),
                )
            },
        ))
        .transpose()?;
    preflight_profile(cells.metadata, RawProfile::ThreadJson, budget)?;
    let metadata: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(
        cells.metadata,
    )
    .map_err(ready_authorization_checked_metadata_value(
        |_: serde_json::Error| PermissionError::CorruptRecord,
    ))?;
    let origin = cells
        .origin
        .map(ready_authorization_checked_metadata_value(
            |raw: &[u8]| {
                preflight_profile(raw, RawProfile::ThreadJson, budget)?;
                let origin: ThreadOrigin = serde_json::from_slice(raw).map_err(
                    ready_authorization_checked_metadata_value(|_: serde_json::Error| {
                        PermissionError::CorruptRecord
                    }),
                )?;
                let bytes = add(
                    origin.application.len(),
                    add(origin.tenant.len(), origin.principal.len())?,
                )?;
                budget.reserve(mul(6, bytes)?, 3)?;
                for label in [&origin.application, &origin.tenant, &origin.principal] {
                    if !valid_label(label) {
                        return corrupt();
                    }
                }
                Ok(origin)
            },
        ))
        .transpose()?;
    budget.reserve(std::mem::size_of::<crate::ThreadRecord>(), 1)?;
    let thread = crate::ThreadRecord {
        id,
        generation,
        active_execution_id,
        created_at_ms,
        metadata,
        origin,
        resource_revision,
    };
    let measured = measure(
        CanonicalValue::Thread(&thread),
        CanonicalLimit::ResourceThread,
        budget,
    )?;
    Ok((thread, measured))
}

fn clone_quote(
    measured: CanonicalMeasure,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(
        add(mul(128, measured.nodes)?, mul(6, measured.bytes)?)?,
        measured.nodes,
    )
}

fn resource_terminal_quote(
    snapshot: &Snapshot,
    limit: CanonicalLimit,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    use b4_contract::{ExecutionStatus, StateWrite, TaskStatus};
    // Quoting the terminal-specific status/task scans is paid separately from kernel scans.
    budget.reserve(0, add(snapshot.tasks.len(), 1)?)?;
    if snapshot.status == ExecutionStatus::Completed {
        let state = measure(CanonicalValue::Input(&snapshot.values), limit, budget)?;
        clone_quote(state, budget)?;
        if let Some(output) = &snapshot.output {
            let output = measure(CanonicalValue::Json(output), limit, budget)?;
            budget.reserve(
                add(state.bytes, output.bytes)?,
                add(state.nodes, output.nodes)?,
            )?;
        }
        // No original-task-input/current-final-state equality is added here.
        return Ok(());
    }
    if snapshot.status != ExecutionStatus::Failed
        || snapshot
            .tasks
            .iter()
            .any(ready_authorization_checked_metadata_value(
                |task: &TaskRecord| task.status == TaskStatus::Failed,
            ))
    {
        return Ok(());
    }
    let whole = measure(CanonicalValue::Snapshot(snapshot), limit, budget)?;
    reserve_measure(whole, budget)?; // Full independent quote scan before counting writes/keys.
    let batch = snapshot
        .definition
        .batches
        .get(snapshot.batch_index)
        .map(ready_authorization_checked_metadata_value(
            Vec::<b4_contract::TaskSpec>::as_slice,
        ))
        .unwrap_or(&[]);
    let b = batch.len();
    let t = snapshot.tasks.len();
    let task_ids = snapshot
        .tasks
        .iter()
        .map(ready_authorization_checked_metadata_value(
            |task: &TaskRecord| task.spec.id.as_str().len(),
        ))
        .try_fold(0, ready_authorization_checked_metadata_value(add))?;
    let batch_ids = batch
        .iter()
        .map(ready_authorization_checked_metadata_value(
            |spec: &b4_contract::TaskSpec| spec.id.as_str().len(),
        ))
        .try_fold(0, ready_authorization_checked_metadata_value(add))?;
    // This quote search and the reducer-task collection search each require payment.
    budget.reserve(
        mul(2, add(mul(b, task_ids)?, mul(t, batch_ids)?)?)?,
        mul(4, mul(t, b)?)?,
    )?;
    budget.reserve(mul(128, b)?, 0)?;
    let state = measure(CanonicalValue::Input(&snapshot.values), limit, budget)?;
    reserve_measure(state, budget)?; // Key-total query precedes the reducer state lookup quote.
    let k0 = snapshot.values.len();
    let k0_bytes = snapshot
        .values
        .keys()
        .map(ready_authorization_checked_metadata_value(String::len))
        .try_fold(0, ready_authorization_checked_metadata_value(add))?;
    let mut writes = 0;
    let mut fields = 0;
    let mut write_bytes = 0;
    let mut write_nodes = 0;
    for spec in batch {
        if let Some(task) = snapshot
            .tasks
            .iter()
            .find(ready_authorization_checked_metadata_value(
                |task: &&TaskRecord| task.spec.id == spec.id,
            ))
        {
            if task.status != TaskStatus::Succeeded {
                continue;
            }
            if let Some(result) = &task.result {
                for write in &result.writes {
                    writes = add(writes, 1)?;
                    let field = match write {
                        StateWrite::Replace { field, .. } | StateWrite::Append { field, .. } => {
                            field
                        }
                    };
                    fields = add(fields, field.len())?;
                    let measured = measure(CanonicalValue::Write(write), limit, budget)?;
                    write_bytes = add(write_bytes, measured.bytes)?;
                    write_nodes = add(write_nodes, measured.nodes)?;
                }
            }
        }
    }
    let square = mul(writes, writes)?;
    // Writer conflict map/usize sets, including complete comparison and retained slot quotes.
    budget.reserve(
        add(
            mul(256, writes)?,
            add(mul(mul(2, writes)?, fields)?, mul(16, square)?)?,
        )?,
        add(mul(2, square)?, mul(2, writes)?)?,
    )?;
    // State entry keys grow by at most one per write; pay every prospective later lookup.
    budget.reserve(
        add(
            mul(writes, k0_bytes)?,
            mul(add(k0, mul(2, writes)?)?, fields)?,
        )?,
        mul(mul(2, writes)?, add(k0, writes)?)?,
    )?;
    budget.reserve(mul(256, add(k0, writes)?)?, 0)?;
    let combined = CanonicalMeasure {
        bytes: add(state.bytes, write_bytes)?,
        nodes: add(state.nodes, write_nodes)?,
        depth: state.depth,
    };
    clone_quote(combined, budget)?;
    budget.reserve(
        mul(mul(128, writes)?, combined.nodes)?,
        mul(writes, combined.nodes)?,
    )?;
    // Static returned failure-code comparisons and status/result inspections.
    budget.reserve(
        add(
            snapshot
                .failure
                .as_ref()
                .map_or(0, ready_authorization_checked_metadata_value(String::len)),
            128,
        )?,
        add(writes, 8)?,
    )?;
    Ok(())
}

// Resource DTO slots are admitted before constructing their owned projection.
const _: () = assert!(std::mem::size_of::<crate::StoredExecution>() <= 4096);
const _: () = assert!(std::mem::size_of::<crate::ThreadRecord>() <= 4096);

// Private preflight for the partial combined-read scaffold. Existing public
// record/resource algorithms and their fees above remain unchanged.
pub(crate) fn check_permission_read_request(
    context: &crate::approval::ApprovalContext,
    execution_id: &ExecutionId,
    task_id: Option<&TaskId>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.require_standard_account()?;
    let result: Result<(), PermissionError> = ready_authorization_checked_metadata_value(|| {
        // Four bounded labels, two opaque IDs, character work and finite errors.
        // Length rejection precedes character iteration in the old validator.
        budget.reserve(16_384, 64)?;
        crate::approval::validate_approval_context(context)?;
        if execution_id.as_str().len() > 256
            || task_id.is_some_and(ready_authorization_checked_metadata_value(|id: &TaskId| {
                id.as_str().len() > 256
            }))
        {
            return Err(crate::approval::ApprovalError::InvalidContext.into());
        }
        Ok(())
    })();
    budget.finish(result)
}

fn permission_read_definition_quote(
    definition: &Definition,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    // Pay the complete batch count scan before collecting no slots or strings.
    budget.reserve(
        add(mul(128, definition.batches.len())?, 4_096)?,
        add(mul(4, definition.batches.len())?, 16)?,
    )?;
    let mut tasks = 0;
    for batch in &definition.batches {
        tasks = add(tasks, batch.len())?;
    }
    // Each closed TaskSpec has two <=256-byte checked IDs and two scalars.
    // Both actual Definitions are quoted separately before their later Eq.
    budget.reserve(
        add(
            mul(1_024, tasks)?,
            add(
                definition.route.as_str().len(),
                definition.revision.as_str().len(),
            )?,
        )?,
        mul(16, tasks)?,
    )
}

fn permission_read_owned(value: &str) -> Result<String, PermissionError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(ready_authorization_checked_metadata_value(
            |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
        ))?;
    owned.push_str(value);
    Ok(owned)
}

pub(crate) fn prepare_permission_read_context(
    resource: &crate::approval::ApprovalResource,
    fence: crate::approval::ApprovalFence,
    context: &crate::approval::ApprovalContext,
    catalog: &super::PermissionCatalog,
    purpose: super::PermissionReadPurpose,
    task_id: Option<&TaskId>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::InvocationContext, PermissionError> {
    let result: Result<crate::InvocationContext, PermissionError> =
        ready_authorization_checked_metadata_value(|| {
            // Origin/link/fence comparisons, owned fields and finite error scratch.
            // The service's requested execution-ID comparison also uses this quote.
            budget.reserve(16_384, 64)?;
            let current = &resource.execution.snapshot;
            let origin = resource
                .thread
                .origin
                .as_ref()
                .ok_or(crate::approval::ApprovalError::Unauthorized)?;
            if origin != &context.actor {
                return Err(crate::approval::ApprovalError::Unauthorized.into());
            }
            if resource.thread.id != resource.execution.thread_id
                || resource.thread.active_execution_id.as_ref() != Some(&current.execution_id)
            {
                return Err(crate::StoreError::ExecutionNotActive.into());
            }
            if fence.execution.owner_generation != current.owner_generation {
                return Err(crate::StoreError::OwnerConflict.into());
            }
            if fence.execution.revision != current.revision
                || fence.resource_revision != resource.thread.resource_revision
            {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let identity = catalog.application_identity();
            if identity.application != origin.application {
                return Err(PermissionError::InvalidCatalog);
            }
            // NativeApplication has <=64 current routes and checked <=256-byte keys.
            // This complete both-operand search quote precedes its BTreeMap lookup.
            budget.reserve(32_768, 128)?;
            let selected = catalog
                .current_definition(&current.definition.route)
                .ok_or(PermissionError::InvalidCatalog)?;
            permission_read_definition_quote(&current.definition, budget)?;
            permission_read_definition_quote(selected, budget)?;
            if selected != &current.definition {
                return Err(PermissionError::InvalidCatalog);
            }
            if matches!(purpose, super::PermissionReadPurpose::Dispatch) && task_id.is_none() {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            if let Some(id) = task_id {
                budget.reserve(mul(512, current.tasks.len())?, mul(2, current.tasks.len())?)?;
                if !current
                    .tasks
                    .iter()
                    .any(ready_authorization_checked_metadata_value(
                        |task: &TaskRecord| &task.spec.id == id,
                    ))
                {
                    return Err(crate::approval::ApprovalError::InvalidGrant.into());
                }
            }
            // No DTO/snapshot clone: reserve-backed, fallible copies of only the
            // finite trusted transient fields. Checked-ID constructors stay exact.
            Ok(crate::InvocationContext {
                thread_id: ThreadId::new(permission_read_owned(resource.thread.id.as_str())?)
                    .map_err(ready_authorization_checked_metadata_value(
                        |_: b4_contract::IdError| PermissionError::CorruptRecord,
                    ))?,
                thread_generation: ThreadGeneration::new(permission_read_owned(
                    resource.thread.generation.as_str(),
                )?)
                .map_err(ready_authorization_checked_metadata_value(
                    |_: b4_contract::IdError| PermissionError::CorruptRecord,
                ))?,
                origin: crate::ThreadOrigin {
                    application: permission_read_owned(&origin.application)?,
                    tenant: permission_read_owned(&origin.tenant)?,
                    principal: permission_read_owned(&origin.principal)?,
                },
                application_revision: permission_read_owned(&identity.revision)?,
                execution_revision: b4_contract::ExecutionRevision::new(permission_read_owned(
                    current.definition.revision.as_str(),
                )?)
                .map_err(ready_authorization_checked_metadata_value(
                    |_: b4_contract::IdError| PermissionError::CorruptRecord,
                ))?,
                policy_revision: permission_read_owned(&context.policy_revision)?,
            })
        })();
    budget.finish(result)
}

// Bounded legacy codecs retain old typed/structural semantics. They confer no
// current grant eligibility or permission authority and create no raw Value clone.
pub(super) fn decode_legacy_grant(
    raw: &[u8],
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::approval::GrantRecord, PermissionError> {
    decode_legacy_grant_capture(raw, budget).map(|(grant, _)| grant)
}
fn decode_legacy_grant_capture(
    raw: &[u8],
    budget: &mut PermissionEvidenceBudget,
) -> Result<(crate::approval::GrantRecord, CanonicalMeasure), PermissionError> {
    preflight_profile(raw, RawProfile::Legacy, budget)?;
    let grant: crate::approval::GrantRecord =
        serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
    let measured = measure(
        CanonicalValue::LegacyGrant(&grant),
        CanonicalLimit::LegacyRecord,
        budget,
    )?;
    reserve_measure(measured, budget)?;
    // Complete finite label, digest, TTL, SHA69 and verifier comparison allowance.
    budget.reserve(32_768, 128)?;
    if !valid_label(&grant.origin.application)
        || !valid_label(&grant.origin.tenant)
        || !valid_label(&grant.origin.principal)
        || !valid_label(&grant.policy_revision)
        || grant.task_parking_revision == 0
        || !digest_shape(&grant.proposal_digest)
        || !digest_shape(&grant.resume_key)
    {
        return corrupt();
    }
    let ttl = grant
        .expires_at_ms
        .checked_sub(grant.issued_at_ms)
        .ok_or(PermissionError::CorruptRecord)?;
    if ttl == 0 || ttl > crate::approval::MAX_APPROVAL_TTL_MS {
        return corrupt();
    }
    let verifier = <sha2::Sha256 as sha2::Digest>::digest(grant.token.as_str().as_bytes());
    let difference = grant
        .verifier
        .iter()
        .zip(verifier.iter())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right));
    if difference != 0 {
        return corrupt();
    }
    // Old prompt depth64 counts scalar leaves as zero. Canonical byte length
    // matches pinned serde without its old full allocation/sort construction.
    measure(
        CanonicalValue::Json(&grant.prompt),
        CanonicalLimit::LegacyPrompt,
        budget,
    )?;
    Ok((grant, measured))
}
pub(super) fn decode_legacy_acceptance(
    raw: &[u8],
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::approval::ApprovalAcceptance, PermissionError> {
    decode_legacy_acceptance_capture(raw, budget).map(|(acceptance, _)| acceptance)
}
fn decode_legacy_acceptance_capture(
    raw: &[u8],
    budget: &mut PermissionEvidenceBudget,
) -> Result<(crate::approval::ApprovalAcceptance, CanonicalMeasure), PermissionError> {
    preflight_profile(raw, RawProfile::Legacy, budget)?;
    let acceptance: crate::approval::ApprovalAcceptance =
        serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
    let measured = measure(
        CanonicalValue::LegacyAcceptance(&acceptance),
        CanonicalLimit::LegacyRecord,
        budget,
    )?;
    reserve_measure(measured, budget)?;
    budget.reserve(16_384, 64)?;
    let actor = &acceptance.context.actor;
    if !valid_label(&actor.application)
        || !valid_label(&actor.tenant)
        || !valid_label(&actor.principal)
        || !valid_label(&acceptance.context.policy_revision)
        || acceptance.decisions.is_empty()
    {
        return corrupt();
    }
    let m = acceptance.decisions.len();
    // Pay count/ID-sum scans before iterating; old rows gain no invented64 entry cap.
    budget.reserve(mul(512, m)?, mul(2, m)?)?;
    let mut tasks = 0;
    let mut interrupts = 0;
    for entry in &acceptance.decisions {
        tasks = add(tasks, entry.task_id.as_str().len())?;
        interrupts = add(interrupts, entry.interrupt_id.as_str().len())?;
    }
    id_family(m, tasks, budget)?;
    id_family(m, interrupts, budget)?;
    budget.reserve(mul(256, m)?, mul(8, m)?)?;
    for (index, entry) in acceptance.decisions.iter().enumerate() {
        if !matches!(
            entry.decision,
            ApprovalDecision::Once | ApprovalDecision::Deny
        ) || entry.task_parking_revision == 0
            || entry.task_parking_revision > acceptance.previous_revision
            || !digest_shape(&entry.proposal_digest)
        {
            return corrupt();
        }
        for previous in &acceptance.decisions[..index] {
            if entry.task_id == previous.task_id || entry.interrupt_id == previous.interrupt_id {
                return corrupt();
            }
        }
    }
    Ok((acceptance, measured))
}

/// Closed stages of one coherent legacy-only permission-view read.
#[derive(Clone, Copy)]
pub enum PermissionViewSql {
    Resource(PermissionResourceSql),
    NewHeaders(PermissionNewGroup),
    GrantHeaders,
    GrantPayload,
    AcceptanceHeaders,
    AcceptancePayload,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PermissionNewGroup {
    Acceptances,
    Decisions,
    Uses,
}

/// Borrowed, guarded index cells. New payloads are never decoded in this profile.
pub struct PermissionNewHeaderCells<'a> {
    pub group: PermissionNewGroup,
    pub execution_id: Option<&'a [u8]>,
    pub primary: &'a [u8],
    pub thread_id: &'a [u8],
    pub generation: &'a [u8],
    pub secondary: Option<&'a [u8]>,
    pub permission_id: Option<&'a [u8]>,
    pub turn_id: Option<&'a [u8]>,
    pub digest: Option<&'a [u8]>,
    pub previous_revision: Option<&'a [u8]>,
    pub committed_revision: Option<&'a [u8]>,
    pub record_version: i64,
    pub record_bytes: usize,
}
pub struct PermissionGrantHeaderCells<'a> {
    pub execution_id: &'a [u8],
    pub interrupt_id: &'a [u8],
    pub task_id: &'a [u8],
    pub thread_id: &'a [u8],
    pub generation: &'a [u8],
    pub parking_revision: &'a [u8],
    pub consumed: i64,
    pub record_bytes: usize,
}
pub struct PermissionAcceptanceHeaderCells<'a> {
    pub execution_id: &'a [u8],
    pub turn_id: &'a [u8],
    pub thread_id: &'a [u8],
    pub generation: &'a [u8],
    pub previous_revision: &'a [u8],
    pub definition_revision: &'a [u8],
    pub accepted_at_ms: &'a [u8],
    pub record_bytes: usize,
}

// Numeric evidence retained only by successful private Accept captures. These
// dimensions are paid again before comparisons, never used to reset an account.
struct CanonicalMeasureAggregate {
    bytes: usize,
    nodes: usize,
    rows: usize,
}

pub(super) struct PermissionAcceptanceCapture {
    identity: std::sync::Arc<()>,
    snapshot: CanonicalMeasure,
    thread: CanonicalMeasure,
    records: CanonicalMeasureAggregate,
}
impl PermissionAcceptanceCapture {
    fn include(
        &mut self,
        measured: CanonicalMeasure,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        // The payload method prepays64B/1N before receiving this measurement.
        budget.reserve(0, 0)?;
        let bytes = add(self.records.bytes, measured.bytes)?;
        let nodes = add(self.records.nodes, measured.nodes)?;
        let rows = add(self.records.rows, 1)?;
        self.records = CanonicalMeasureAggregate { bytes, nodes, rows };
        Ok(())
    }
    fn reserve_resource_eq(
        &self,
        prior: &Self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        reserve_measure(self.snapshot, budget)?;
        reserve_measure(self.thread, budget)?;
        reserve_measure(prior.snapshot, budget)?;
        reserve_measure(prior.thread, budget)?;
        // StoredExecution.thread_id lies outside its Snapshot. Fixed scalar,
        // error and immutable match-proof bookkeeping also has its own fee.
        budget.reserve(4_096, 32)
    }
    fn reserve_records_eq(
        &self,
        prior: &Self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        budget.reserve(
            add(self.records.bytes, prior.records.bytes)?,
            add(self.records.nodes, prior.records.nodes)?,
        )?;
        budget.reserve(512, 32)
    }
}
const _: () = assert!(std::mem::size_of::<PermissionAcceptanceCapture>() <= 80);
const _: () = assert!(std::mem::size_of::<Option<PermissionAcceptanceCapture>>() <= 80);
const _: () = assert!(std::mem::size_of::<PermissionAcceptanceCapture>() <= 128);

/// A checked borrow, never a reset account or a publicly constructible capture.
pub struct PermissionCombinedRead<'b, 's, 'd> {
    budget: &'b mut PermissionEvidenceBudget,
    id: &'s ExecutionId,
    fence: crate::approval::ApprovalFence,
    selector: &'s super::PermissionViewSelector<'d>,
    metadata_checked: bool,
    execution: Option<(crate::StoredExecution, CanonicalMeasure)>,
    thread: Option<(crate::ThreadRecord, CanonicalMeasure)>,
}
impl fmt::Debug for PermissionCombinedRead<'_, '_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionCombinedRead([REDACTED])")
    }
}
impl PermissionEvidenceBudget {
    pub fn combined_read<'b, 's, 'd>(
        &'b mut self,
        id: &'s ExecutionId,
        fence: crate::approval::ApprovalFence,
        selector: &'s super::PermissionViewSelector<'d>,
    ) -> Result<PermissionCombinedRead<'b, 's, 'd>, PermissionError> {
        self.require_standard_account()?;
        let result = (|| {
            self.reserve(0, 0)?;
            if self.resource.combined_reads == 2 {
                return self.limit();
            }
            self.reserve(128, 1)?;
            self.resource.combined_reads += 1;
            // Ordinary Some remains refused before mutex/SQL. Only a private
            // Accept selector and a prior actual sealed capture reach matching.
            let acceptance = selector.is_acceptance_capture();
            if acceptance
                && (selector.purpose() != super::PermissionReadPurpose::Accept
                    || selector.task_id().is_some())
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            if let Some(prior) = selector.expected_view() {
                if !acceptance
                    || prior.receipt().purpose() != super::PermissionReadPurpose::Accept
                    || prior.acceptance_capture().is_none()
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
            }
            if acceptance {
                self.reserve(128, 2)?; // Fixed retained scalar quote slots.
                self.reserve(2_048, 32)?; // Up to24 bounded scalar handoffs.
                                          // Establish origin before any capture measurements can escape.
                                          // The retained Arc prevents reuse after a dropped/replaced account.
                self.reserve(256, 4)?;
                if self.acceptance_identity.is_none() {
                    self.acceptance_identity = Some(std::sync::Arc::new(()));
                }
                if let Some(prior) = selector.expected_view() {
                    let prior = prior
                        .acceptance_capture()
                        .ok_or(PermissionError::CorruptRecord)?;
                    let actual = self
                        .acceptance_identity
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?;
                    if !std::sync::Arc::ptr_eq(actual, &prior.identity) {
                        return corrupt();
                    }
                }
            }
            Ok(())
        })();
        self.finish(result)?;
        Ok(PermissionCombinedRead {
            budget: self,
            id,
            fence,
            selector,
            metadata_checked: false,
            execution: None,
            thread: None,
        })
    }
}
impl PermissionCombinedRead<'_, '_, '_> {
    pub fn before_resource_sql(
        &mut self,
        statement: PermissionResourceSql,
        key: Option<PermissionResourceKey<'_>>,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead {
            budget: &mut *self.budget,
        }
        .before_sql(statement, key)
    }
    pub fn check_metadata(
        &mut self,
        version: i64,
        checksum: &[u8],
        owner: &[u8],
        expected_owner: u64,
        expected_schema: &[u8],
    ) -> Result<(), PermissionError> {
        self.budget.reserve(0, 0)?;
        if self.metadata_checked {
            return corrupt();
        }
        PermissionResourceRead {
            budget: &mut *self.budget,
        }
        .check_metadata(version, checksum, owner, expected_owner, expected_schema)?;
        self.metadata_checked = true;
        Ok(())
    }
    pub fn admit_execution_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead {
            budget: &mut *self.budget,
        }
        .admit_execution_lengths(thread, snapshot)
    }
    pub fn capture_execution(
        &mut self,
        cells: PermissionExecutionCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if !self.metadata_checked || self.execution.is_some() {
                return corrupt();
            }
            let owned = resource_execution_capture(cells, self.budget)?;
            self.execution = Some(owned);
            Ok(())
        })();
        self.budget.finish(result).map_err(resource_error)
    }
    pub fn thread_query_id(&mut self) -> Result<ThreadId, PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            let execution = self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            ThreadId::new(resource_string(
                execution.0.thread_id.as_str().as_bytes(),
                self.budget,
            )?)
            .map_err(|_| PermissionError::CorruptRecord)
        })();
        self.budget.finish(result).map_err(resource_error)
    }
    pub fn admit_thread_lengths(
        &mut self,
        id: usize,
        active: usize,
        generation: usize,
        metadata: usize,
        origin: usize,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead {
            budget: &mut *self.budget,
        }
        .admit_thread_lengths(id, active, generation, metadata, origin)
    }
    pub fn capture_thread(
        &mut self,
        cells: PermissionThreadCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if self.execution.is_none() || self.thread.is_some() {
                return corrupt();
            }
            self.thread = Some(resource_thread_capture(cells, self.budget)?);
            Ok(())
        })();
        self.budget.finish(result).map_err(resource_error)
    }
}
impl<'b, 's, 'd> PermissionCombinedRead<'b, 's, 'd> {
    pub fn into_legacy(self) -> Result<PermissionLegacyViewRead<'b, 's, 'd>, PermissionError> {
        let Self {
            budget,
            id,
            fence,
            selector,
            metadata_checked,
            execution,
            thread,
        } = self;
        let result = (|| {
            budget.reserve(0, 0)?;
            if !metadata_checked {
                return corrupt();
            }
            let (execution, execution_measure) = execution.ok_or(PermissionError::CorruptRecord)?;
            let (thread, thread_measure) = thread.ok_or(PermissionError::CorruptRecord)?;
            PermissionResourceRead {
                budget: &mut *budget,
            }
            .check_linkage(id, &thread, &execution)?;
            let expected = selector.expected_resource();
            // The original resource has no public quote. Pay its own two walks.
            let original_execution = measure(
                CanonicalValue::Snapshot(&expected.execution.snapshot),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            let original_thread = measure(
                CanonicalValue::Thread(&expected.thread),
                CanonicalLimit::ResourceThread,
                budget,
            )?;
            reserve_measure(execution_measure, budget)?;
            reserve_measure(thread_measure, budget)?;
            reserve_measure(original_execution, budget)?;
            reserve_measure(original_thread, budget)?;
            // Full resource Eq, selector origins/fences/labels and both Definitions
            // are independent work. No sensitive-record operation is exposed yet.
            budget.reserve(16_384, 512)?;
            let snapshot = &execution.snapshot;
            if fence.execution.owner_generation != snapshot.owner_generation {
                return Err(crate::StoreError::OwnerConflict.into());
            }
            if fence.execution.revision != snapshot.revision
                || fence.resource_revision != thread.resource_revision
                || execution != expected.execution
                || thread != expected.thread
            {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let context = selector.context();
            permission_read_definition_quote(&snapshot.definition, budget)?;
            permission_read_definition_quote(selector.definition(), budget)?;
            if &snapshot.execution_id != id
                || context.thread_id != thread.id
                || context.thread_generation != thread.generation
                || thread.origin.as_ref() != Some(&context.origin)
                || context.execution_revision != snapshot.definition.revision
                || selector.definition() != &snapshot.definition
                || !valid_label(&context.application_revision)
                || !valid_label(&context.policy_revision)
            {
                return corrupt();
            }
            if matches!(selector.purpose(), super::PermissionReadPurpose::Dispatch)
                && selector.task_id().is_none()
            {
                return corrupt();
            }
            if let Some(id) = selector.task_id() {
                budget.reserve(
                    mul(512, snapshot.tasks.len())?,
                    mul(2, snapshot.tasks.len())?,
                )?;
                if !snapshot.tasks.iter().any(|task| &task.spec.id == id) {
                    return corrupt();
                }
            }
            let resource = crate::approval::ApprovalResource { thread, execution };
            let capture_quote = if selector.is_acceptance_capture() {
                Some(PermissionAcceptanceCapture {
                    identity: std::sync::Arc::clone(
                        budget
                            .acceptance_identity
                            .as_ref()
                            .ok_or(PermissionError::CorruptRecord)?,
                    ),
                    snapshot: execution_measure,
                    thread: thread_measure,
                    records: CanonicalMeasureAggregate {
                        bytes: 0,
                        nodes: 0,
                        rows: 0,
                    },
                })
            } else {
                None
            };
            let mut prior_resource_matched = false;
            if let Some(prior) = selector.expected_view() {
                let actual_quote = capture_quote
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                let prior_quote = prior
                    .acceptance_capture()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                actual_quote.reserve_resource_eq(prior_quote, budget)?;
                if &resource != prior.resource() {
                    return Err(crate::StoreError::RevisionConflict.into());
                }
                // Both owned current resource and borrowed prior View remain
                // immutable throughout all sensitive methods and the final Eq.
                prior_resource_matched = true;
            }
            Ok((resource, capture_quote, prior_resource_matched))
        })();
        let (resource, capture_quote, prior_resource_matched) =
            budget.finish(result).map_err(view_error)?;
        budget.reserve(512, 8)?;
        Ok(PermissionLegacyViewRead {
            budget,
            selector,
            resource,
            capture_quote,
            prior_resource_matched,
            new_group: 0,
            new_rows: 0,
            new_nonempty: false,
            grants_done: false,
            acceptances_done: false,
            grant_header: None,
            acceptance_header: None,
            grants: Vec::new(),
            acceptances: Vec::new(),
        })
    }
}

struct GrantHeader {
    interrupt: String,
    task: String,
    execution: String,
    thread: String,
    generation: String,
    parking: u64,
    consumed: bool,
    bytes: usize,
}
struct AcceptanceHeader {
    execution: String,
    turn: String,
    thread: String,
    generation: String,
    definition: String,
    previous: u64,
    accepted: u64,
    bytes: usize,
}
/// Sensitive methods require the paid, matched whole resource state above.
pub struct PermissionLegacyViewRead<'b, 's, 'd> {
    budget: &'b mut PermissionEvidenceBudget,
    selector: &'s super::PermissionViewSelector<'d>,
    resource: crate::approval::ApprovalResource,
    capture_quote: Option<PermissionAcceptanceCapture>,
    prior_resource_matched: bool,
    new_group: usize,
    new_rows: usize,
    new_nonempty: bool,
    grants_done: bool,
    acceptances_done: bool,
    grant_header: Option<GrantHeader>,
    acceptance_header: Option<AcceptanceHeader>,
    grants: Vec<crate::approval::GrantRecord>,
    acceptances: Vec<crate::approval::ApprovalAcceptance>,
}
impl fmt::Debug for PermissionLegacyViewRead<'_, '_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionLegacyViewRead([REDACTED])")
    }
}
fn view_error(error: PermissionError) -> PermissionError {
    match error {
        PermissionError::CorruptRecord | PermissionError::Kernel(_) => {
            crate::StoreError::Corrupt("permission_view_invalid".into()).into()
        }
        other => other,
    }
}
fn opaque(raw: &[u8]) -> Result<&str, PermissionError> {
    if raw.is_empty() || raw.len() > 256 {
        return corrupt();
    }
    let value = std::str::from_utf8(raw).map_err(|_| PermissionError::CorruptRecord)?;
    if value.chars().any(char::is_control) {
        return corrupt();
    }
    Ok(value)
}
fn generation(raw: &[u8]) -> Result<&str, PermissionError> {
    let value = std::str::from_utf8(raw).map_err(|_| PermissionError::CorruptRecord)?;
    if !digest_shape(value) {
        return corrupt();
    }
    Ok(value)
}
fn record_identity(raw: &[u8], prefix: &[u8]) -> bool {
    raw.len() == 69
        && raw.starts_with(prefix)
        && raw[5..]
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
}
fn push_legacy<T>(
    value: T,
    values: &mut Vec<T>,
    slot: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let k = values.len();
    if k == 64 {
        return budget.limit();
    }
    if k == values.capacity() {
        let target = if k == 0 {
            4
        } else {
            mul(2, values.capacity())?
        };
        budget.reserve(mul(add(target, k)?, slot)?, 0)?;
        values
            .try_reserve_exact(target - k)
            .map_err(|_| PermissionError::LimitExceeded)?;
    }
    budget.reserve(slot, 1)?;
    values.push(value);
    Ok(())
}
impl PermissionLegacyViewRead<'_, '_, '_> {
    fn empty_new(&self) -> Result<(), PermissionError> {
        if self.new_group != 3 || self.new_nonempty {
            return corrupt();
        }
        Ok(())
    }
    /// These IDs borrow immutable owned captures; no projection is copied.
    pub fn execution_id(&self) -> &ExecutionId {
        &self.resource.execution.snapshot.execution_id
    }
    pub fn thread_id(&self) -> &ThreadId {
        &self.resource.thread.id
    }
    pub fn before_sql(&mut self, statement: PermissionViewSql) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            match statement {
                PermissionViewSql::Resource(PermissionResourceSql::CommitRead) => {
                    if !self.grants_done
                        || !self.acceptances_done
                        || self.grant_header.is_some()
                        || self.acceptance_header.is_some()
                    {
                        return corrupt();
                    }
                    self.empty_new()?;
                    return PermissionResourceRead {
                        budget: &mut *self.budget,
                    }
                    .before_sql(PermissionResourceSql::CommitRead, None);
                }
                PermissionViewSql::Resource(_) => return corrupt(),
                PermissionViewSql::NewHeaders(group) => {
                    if group_index(group) != self.new_group || self.new_rows != 0 {
                        return corrupt();
                    }
                    admit_composite_key(
                        self.budget,
                        10,
                        &[
                            group_tag(group),
                            self.resource.thread.id.as_str().as_bytes(),
                            self.resource
                                .execution
                                .snapshot
                                .execution_id
                                .as_str()
                                .as_bytes(),
                        ],
                    )?;
                }
                PermissionViewSql::GrantHeaders => {
                    self.empty_new()?;
                    if self.grants_done || self.grant_header.is_some() || !self.grants.is_empty() {
                        return corrupt();
                    }
                    admit_composite_key(
                        self.budget,
                        10,
                        &[
                            b"g",
                            self.resource
                                .execution
                                .snapshot
                                .execution_id
                                .as_str()
                                .as_bytes(),
                        ],
                    )?;
                }
                PermissionViewSql::AcceptanceHeaders => {
                    self.empty_new()?;
                    if !self.grants_done
                        || self.acceptances_done
                        || self.acceptance_header.is_some()
                        || !self.acceptances.is_empty()
                    {
                        return corrupt();
                    }
                    admit_composite_key(
                        self.budget,
                        10,
                        &[
                            b"a",
                            self.resource
                                .execution
                                .snapshot
                                .execution_id
                                .as_str()
                                .as_bytes(),
                        ],
                    )?;
                }
                PermissionViewSql::GrantPayload => {
                    let h = self
                        .grant_header
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?;
                    admit_composite_key(
                        self.budget,
                        3,
                        &[h.execution.as_bytes(), h.interrupt.as_bytes()],
                    )?;
                }
                PermissionViewSql::AcceptancePayload => {
                    let h = self
                        .acceptance_header
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?;
                    admit_composite_key(
                        self.budget,
                        4,
                        &[h.execution.as_bytes(), h.turn.as_bytes()],
                    )?;
                }
            }
            PermissionResourceRead {
                budget: &mut *self.budget,
            }
            .before_sql(PermissionResourceSql::Metadata, None)
        })();
        self.budget.finish(result).map_err(view_error)
    }
    /// Before each cursor.next, including the final absent row and all cell checks.
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        let result = self.budget.reserve(2_048, 16);
        self.budget.finish(result)
    }
    pub fn admit_new_header(
        &mut self,
        cells: PermissionNewHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(4_096, 32)?;
            if group_index(cells.group) != self.new_group {
                return corrupt();
            }
            if self.new_rows == 64 {
                return self.budget.limit();
            }
            self.new_rows += 1;
            let thread = opaque(cells.thread_id)?;
            let epoch = generation(cells.generation)?;
            if thread != self.resource.thread.id.as_str()
                || epoch != self.resource.thread.generation.as_str()
                || cells.record_version != 1
                || cells.record_bytes == 0
            {
                return corrupt();
            }
            let limit = if cells.group == PermissionNewGroup::Uses {
                16_384
            } else {
                65_536
            };
            if cells.record_bytes > limit {
                return self.budget.limit();
            }
            match cells.group {
                PermissionNewGroup::Acceptances => {
                    if opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != self.execution_id().as_str()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.digest.is_some()
                    {
                        return corrupt();
                    }
                    opaque(cells.primary)?;
                    let previous = resource_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    let committed = resource_u64(
                        cells
                            .committed_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    if previous.checked_add(1) != Some(committed)
                        || committed > self.resource.execution.snapshot.revision
                    {
                        return corrupt();
                    }
                }
                PermissionNewGroup::Decisions => {
                    if cells.execution_id.is_some()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.previous_revision.is_some()
                        || cells.committed_revision.is_some()
                        || !record_identity(cells.primary, b"b4pd_")
                    {
                        return corrupt();
                    }
                    generation(cells.digest.ok_or(PermissionError::CorruptRecord)?)?;
                }
                PermissionNewGroup::Uses => {
                    if opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != self.execution_id().as_str()
                        || cells.digest.is_some()
                        || cells.committed_revision.is_some()
                        || !record_identity(
                            cells.secondary.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pu_",
                        )
                        || !record_identity(
                            cells.permission_id.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pd_",
                        )
                    {
                        return corrupt();
                    }
                    opaque(cells.primary)?;
                    opaque(cells.turn_id.ok_or(PermissionError::CorruptRecord)?)?;
                    let authorization = resource_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    if authorization > self.resource.execution.snapshot.revision {
                        return corrupt();
                    }
                }
            }
            self.new_nonempty = true;
            Ok(())
        })();
        self.budget.finish(result).map_err(view_error)
    }
    pub fn end_new_range(&mut self, group: PermissionNewGroup) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(128, 1)?;
            if group_index(group) != self.new_group {
                return corrupt();
            }
            self.new_group += 1;
            self.new_rows = 0;
            if self.new_group == 3 && self.new_nonempty {
                return Err(PermissionError::UnsupportedProfile);
            }
            Ok(())
        })();
        self.budget.finish(result).map_err(view_error)
    }
    pub fn admit_grant_header(
        &mut self,
        cells: PermissionGrantHeaderCells<'_>,
    ) -> Result<String, PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            self.empty_new()?;
            self.budget.reserve(8_192, 32)?;
            if self.grants_done || self.grant_header.is_some() {
                return corrupt();
            }
            if self.grants.len() == 64 {
                return self.budget.limit();
            }
            let execution = opaque(cells.execution_id)?;
            let interrupt = opaque(cells.interrupt_id)?;
            let task = opaque(cells.task_id)?;
            let thread = opaque(cells.thread_id)?;
            let epoch = generation(cells.generation)?;
            let parking = resource_u64(cells.parking_revision)?;
            if execution != self.execution_id().as_str()
                || thread != self.thread_id().as_str()
                || epoch != self.resource.thread.generation.as_str()
                || parking == 0
                || !matches!(cells.consumed, 0 | 1)
                || cells.record_bytes == 0
            {
                return corrupt();
            }
            if cells.record_bytes > 65_536 {
                return self.budget.limit();
            }
            self.budget.reserve(cells.record_bytes, 1)?;
            let query = resource_string(interrupt.as_bytes(), self.budget)?;
            self.grant_header = Some(GrantHeader {
                interrupt: resource_string(interrupt.as_bytes(), self.budget)?,
                task: resource_string(task.as_bytes(), self.budget)?,
                execution: resource_string(execution.as_bytes(), self.budget)?,
                thread: resource_string(thread.as_bytes(), self.budget)?,
                generation: resource_string(epoch.as_bytes(), self.budget)?,
                parking,
                consumed: cells.consumed == 1,
                bytes: cells.record_bytes,
            });
            Ok(query)
        })();
        self.budget.finish(result).map_err(view_error)
    }
    pub fn decode_grant_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            let header = self
                .grant_header
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return corrupt();
            }
            let (grant, measure) = if self.capture_quote.is_some() {
                self.budget.reserve(64, 1)?;
                let (grant, measured) = decode_legacy_grant_capture(raw, self.budget)?;
                (grant, Some(measured))
            } else {
                (decode_legacy_grant(raw, self.budget)?, None)
            };
            self.budget.reserve(8_192, 32)?;
            if grant.execution_id.as_str() != header.execution
                || grant.interrupt_id.as_str() != header.interrupt
                || grant.task_id.as_str() != header.task
                || grant.thread_id.as_str() != header.thread
                || grant.thread_generation.as_str() != header.generation
                || grant.task_parking_revision != header.parking
                || grant.consumed != header.consumed
            {
                return corrupt();
            }
            // Pay a distinct quote for the retained primary-ID uniqueness scan.
            let k = self.grants.len();
            self.budget.reserve(mul(512, k)?, mul(2, k)?)?;
            let bytes = self
                .grants
                .iter()
                .map(|g| g.interrupt_id.as_str().len())
                .try_fold(0, add)?;
            self.budget.reserve(
                add(mul(k, grant.interrupt_id.as_str().len())?, bytes)?,
                mul(2, k)?,
            )?;
            if self
                .grants
                .iter()
                .any(|g| g.interrupt_id == grant.interrupt_id)
            {
                return corrupt();
            }
            if let Some(measured) = measure {
                self.capture_quote
                    .as_mut()
                    .ok_or(PermissionError::CorruptRecord)?
                    .include(measured, self.budget)?;
            }
            push_legacy(grant, &mut self.grants, 1_024, self.budget)
        })();
        self.budget.finish(result).map_err(view_error)
    }
    pub fn end_grant_range(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(128, 1)?;
            self.empty_new()?;
            if self.grants_done || self.grant_header.is_some() {
                return corrupt();
            }
            self.grants_done = true;
            Ok(())
        })();
        self.budget.finish(result).map_err(view_error)
    }
    pub fn admit_acceptance_header(
        &mut self,
        cells: PermissionAcceptanceHeaderCells<'_>,
    ) -> Result<String, PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            self.empty_new()?;
            self.budget.reserve(8_192, 32)?;
            if !self.grants_done || self.acceptances_done || self.acceptance_header.is_some() {
                return corrupt();
            }
            if self.acceptances.len() == 64 {
                return self.budget.limit();
            }
            let execution = opaque(cells.execution_id)?;
            let turn = opaque(cells.turn_id)?;
            let thread = opaque(cells.thread_id)?;
            let epoch = generation(cells.generation)?;
            let definition = opaque(cells.definition_revision)?;
            let previous = resource_u64(cells.previous_revision)?;
            let accepted = resource_u64(cells.accepted_at_ms)?;
            if execution != self.execution_id().as_str()
                || thread != self.thread_id().as_str()
                || epoch != self.resource.thread.generation.as_str()
                || cells.record_bytes == 0
            {
                return corrupt();
            }
            if cells.record_bytes > 65_536 {
                return self.budget.limit();
            }
            self.budget.reserve(cells.record_bytes, 1)?;
            let query = resource_string(turn.as_bytes(), self.budget)?;
            self.acceptance_header = Some(AcceptanceHeader {
                execution: resource_string(execution.as_bytes(), self.budget)?,
                turn: resource_string(turn.as_bytes(), self.budget)?,
                thread: resource_string(thread.as_bytes(), self.budget)?,
                generation: resource_string(epoch.as_bytes(), self.budget)?,
                definition: resource_string(definition.as_bytes(), self.budget)?,
                previous,
                accepted,
                bytes: cells.record_bytes,
            });
            Ok(query)
        })();
        self.budget.finish(result).map_err(view_error)
    }
    pub fn decode_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            let header = self
                .acceptance_header
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return corrupt();
            }
            let (acceptance, measure) = if self.capture_quote.is_some() {
                self.budget.reserve(64, 1)?;
                let (acceptance, measured) = decode_legacy_acceptance_capture(raw, self.budget)?;
                (acceptance, Some(measured))
            } else {
                (decode_legacy_acceptance(raw, self.budget)?, None)
            };
            self.budget.reserve(8_192, 32)?;
            if acceptance.execution_id.as_str() != header.execution
                || acceptance.turn_id.as_str() != header.turn
                || acceptance.thread_id.as_str() != header.thread
                || acceptance.thread_generation.as_str() != header.generation
                || acceptance.previous_revision != header.previous
                || acceptance.definition_revision.as_str() != header.definition
                || acceptance.accepted_at_ms != header.accepted
            {
                return corrupt();
            }
            let k = self.acceptances.len();
            self.budget.reserve(mul(512, k)?, mul(2, k)?)?;
            let bytes = self
                .acceptances
                .iter()
                .map(|a| a.turn_id.as_str().len())
                .try_fold(0, add)?;
            self.budget.reserve(
                add(mul(k, acceptance.turn_id.as_str().len())?, bytes)?,
                mul(2, k)?,
            )?;
            if self
                .acceptances
                .iter()
                .any(|a| a.turn_id == acceptance.turn_id)
            {
                return corrupt();
            }
            if let Some(measured) = measure {
                self.capture_quote
                    .as_mut()
                    .ok_or(PermissionError::CorruptRecord)?
                    .include(measured, self.budget)?;
            }
            push_legacy(acceptance, &mut self.acceptances, 512, self.budget)
        })();
        self.budget.finish(result).map_err(view_error)
    }
    pub fn end_acceptance_range(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(128, 1)?;
            self.empty_new()?;
            if !self.grants_done || self.acceptances_done || self.acceptance_header.is_some() {
                return corrupt();
            }
            if let Some(prior) = self.selector.expected_view() {
                let actual_quote = self
                    .capture_quote
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                let prior_quote = prior
                    .acceptance_capture()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                actual_quote.reserve_records_eq(prior_quote, self.budget)?;
                if !self.prior_resource_matched
                    || self.selector.purpose() != super::PermissionReadPurpose::Accept
                    || prior.receipt().purpose() != super::PermissionReadPurpose::Accept
                    || actual_quote.records.rows != add(self.grants.len(), self.acceptances.len())?
                    || prior_quote.records.rows
                        != add(prior.grants().len(), prior.acceptances().len())?
                    || self.grants.as_slice() != prior.grants()
                    || self.acceptances.as_slice() != prior.acceptances()
                    || !prior.permission_acceptances().is_empty()
                    || !prior.permission_decisions().is_empty()
                    || !prior.permission_uses().is_empty()
                    || !prior.creator_acceptances().is_empty()
                    || !prior.referenced_grants().is_empty()
                    || !prior.referenced_acceptances().is_empty()
                    || !prior.journal_entries().is_empty()
                {
                    return Err(crate::StoreError::RevisionConflict.into());
                }
                // The checked legacy session constructs seven empty new/ref/
                // journal vectors. Their full ordered content equals prior iff
                // all seven prior vectors above are empty. Resource Eq was paid
                // before sensitive rows and preserved by immutable ownership.
            }
            self.acceptances_done = true;
            Ok(())
        })();
        self.budget.finish(result).map_err(view_error)
    }
    /// The trusted backend calls this only after its actual read COMMIT succeeds.
    pub fn finish_after_commit(self) -> Result<super::PermissionView, PermissionError> {
        let Self {
            budget,
            selector,
            resource,
            capture_quote,
            new_group,
            new_nonempty,
            grants_done,
            acceptances_done,
            grant_header,
            acceptance_header,
            grants,
            acceptances,
            ..
        } = self;
        let result = (|| {
            budget.reserve(4_096, 64)?;
            if new_group != 3
                || new_nonempty
                || !grants_done
                || !acceptances_done
                || grant_header.is_some()
                || acceptance_header.is_some()
            {
                return corrupt();
            }
            Ok(FinishedPermissionLegacyView {
                resource,
                grants,
                acceptances,
                purpose: selector.purpose(),
                usage: budget.usage(),
                capture_quote,
            })
        })();
        let capture = budget.finish(result).map_err(view_error)?;
        Ok(super::PermissionView::from_checked_legacy(capture))
    }
}
fn group_index(group: PermissionNewGroup) -> usize {
    match group {
        PermissionNewGroup::Acceptances => 0,
        PermissionNewGroup::Decisions => 1,
        PermissionNewGroup::Uses => 2,
    }
}
fn group_tag(group: PermissionNewGroup) -> &'static [u8] {
    match group {
        PermissionNewGroup::Acceptances => b"pa",
        PermissionNewGroup::Decisions => b"pd",
        PermissionNewGroup::Uses => b"pu",
    }
}
fn admit_composite_key(
    budget: &mut PermissionEvidenceBudget,
    domain: u8,
    parts: &[&[u8]],
) -> Result<(), PermissionError> {
    budget.reserve(128, parts.len())?;
    if budget.resource.attempted_keys == 16_384 {
        return budget.limit();
    }
    let bytes = parts
        .iter()
        .try_fold(1, |n, part| add(n, add(2, part.len())?))?;
    if bytes > 1_024 {
        return budget.limit();
    }
    budget.reserve(add(mul(6, bytes)?, 128)?, 1)?;
    budget.resource.attempted_keys += 1;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(bytes)
        .map_err(|_| PermissionError::LimitExceeded)?;
    encoded.push(domain);
    for part in parts {
        let len = u16::try_from(part.len()).map_err(|_| PermissionError::LimitExceeded)?;
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(part);
    }
    let k = budget.resource.keys.len();
    budget.reserve(add(mul(k, bytes)?, budget.resource.key_bytes)?, mul(2, k)?)?;
    if budget.resource.keys.iter().any(|old| old == &encoded) {
        return Ok(());
    }
    if k == 8_192 {
        return budget.limit();
    }
    if k == budget.resource.keys.capacity() {
        let target = if k == 0 {
            4
        } else {
            mul(2, budget.resource.keys.capacity())?
        };
        budget.reserve(mul(add(target, k)?, std::mem::size_of::<Vec<u8>>())?, 0)?;
        budget
            .resource
            .keys
            .try_reserve_exact(target - k)
            .map_err(|_| PermissionError::LimitExceeded)?;
    }
    budget.resource.key_bytes = add(budget.resource.key_bytes, bytes)?;
    budget.resource.keys.push(encoded);
    Ok(())
}
// Only this module can construct the checked capture; sibling view.rs moves it.
pub(super) struct FinishedPermissionLegacyView {
    resource: crate::approval::ApprovalResource,
    grants: Vec<crate::approval::GrantRecord>,
    acceptances: Vec<crate::approval::ApprovalAcceptance>,
    purpose: super::PermissionReadPurpose,
    usage: PermissionEvidenceUsage,
    capture_quote: Option<PermissionAcceptanceCapture>,
}
impl FinishedPermissionLegacyView {
    pub(super) fn into_parts(
        self,
    ) -> (
        crate::approval::ApprovalResource,
        Vec<crate::approval::GrantRecord>,
        Vec<crate::approval::ApprovalAcceptance>,
        super::PermissionReadPurpose,
        PermissionEvidenceUsage,
        Option<PermissionAcceptanceCapture>,
    ) {
        (
            self.resource,
            self.grants,
            self.acceptances,
            self.purpose,
            self.usage,
            self.capture_quote,
        )
    }
}
const _: () =
    assert!(std::mem::size_of::<(crate::approval::GrantRecord, CanonicalMeasure)>() <= 1_024);
const _: () =
    assert!(std::mem::size_of::<(crate::approval::ApprovalAcceptance, CanonicalMeasure)>() <= 512);
const _: () = assert!(std::mem::size_of::<crate::approval::GrantRecord>() <= 1_024);
const _: () = assert!(std::mem::size_of::<crate::approval::ApprovalAcceptance>() <= 512);
const _: () = assert!(std::mem::size_of::<super::PermissionView>() <= 4_096);

// Only enum/cancelled fields are inspected here. Oversized requests and literal
// cancelled Always retain the complete ordinary facade's existing handling.
pub(crate) fn permission_human_uses_always_stage(
    request: &crate::approval::ApprovalAcceptanceRequest,
    budget: &mut PermissionEvidenceBudget,
) -> Result<bool, PermissionError> {
    let result = (|| {
        budget.reserve(0, 0)?;
        let count = request.decisions.len();
        if count > 64 {
            return Ok(false);
        }
        budget.reserve(add(4_096, mul(64, count)?)?, mul(4, count)?)?;
        Ok(request
            .decisions
            .iter()
            .any(|entry| !entry.cancelled && entry.decision == ApprovalDecision::Always))
    })();
    budget.finish(result)
}

// Stage2-only current approval checks. Existing historical codecs and the
// record-witness digest helper above keep their exact namespace, limits and fees.
pub(crate) fn begin_permission_always_stage(
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.require_standard_account()?;
    budget.reserve(16_384, 128)
}

fn current_permission_approval_proposal_digest(
    task: &TaskRecord,
    prompt: &serde_json::Value,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    let result = hash_canonical(
        CanonicalValue::LegacyProposal { task, prompt },
        CanonicalLimit::LegacyRecord,
        budget,
    );
    budget.finish(result)
}

pub(crate) fn check_permission_always_eligibility(
    view: &super::PermissionView,
    request: &crate::approval::ApprovalAcceptanceRequest,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let result = (|| {
        let current = &view.execution().snapshot;
        let measured = measure(
            CanonicalValue::Snapshot(current),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        reserve_measure(measured, budget)?;
        let t = current.tasks.len();
        let m = request.decisions.len();
        let g = view.grants().len();
        let pairs = add(mul(m, m)?, add(mul(t, m)?, mul(t, g)?)?)?;
        budget.reserve(
            add(16_384, add(mul(16_384, m)?, mul(1_024, pairs)?)?)?,
            add(64, add(mul(128, m)?, mul(4, pairs)?)?)?,
        )?;
        if view.thread().origin.as_ref() != Some(&request.context.actor) {
            return Err(crate::approval::ApprovalError::Unauthorized.into());
        }
        if current.status != b4_contract::ExecutionStatus::WaitingForApproval {
            return Err(crate::approval::ApprovalError::NotStablePark.into());
        }
        if request.route != current.definition.route
            || request.definition_revision != current.definition.revision
        {
            return Err(crate::approval::ApprovalError::RouteMismatch.into());
        }
        let pending_count = current
            .tasks
            .iter()
            .filter(|task| task.status == b4_contract::TaskStatus::WaitingForApproval)
            .count();
        if pending_count == 0 || pending_count != m {
            return Err(crate::approval::ApprovalError::InterruptSetMismatch.into());
        }
        // Human submission order is arbitrary. Uniqueness plus every waiting
        // member and equal cardinality prove the exact submitted SET.
        for (index, entry) in request.decisions.iter().enumerate() {
            if request.decisions[..index]
                .iter()
                .any(|earlier| earlier.interrupt_id == entry.interrupt_id)
            {
                return Err(crate::approval::ApprovalError::InterruptSetMismatch.into());
            }
        }
        for task in current
            .tasks
            .iter()
            .filter(|task| task.status == b4_contract::TaskStatus::WaitingForApproval)
        {
            let pending = task
                .interrupt
                .as_ref()
                .ok_or(crate::approval::ApprovalError::InvalidGrant)?;
            let submitted = request
                .decisions
                .iter()
                .find(|entry| entry.interrupt_id == pending.id)
                .ok_or(crate::approval::ApprovalError::InterruptSetMismatch)?;
            let mut matching = view
                .grants()
                .iter()
                .filter(|grant| grant.interrupt_id == pending.id);
            let grant = matching
                .next()
                .ok_or(crate::approval::ApprovalError::GrantUnavailable)?;
            if matching.next().is_some() {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            let token = submitted
                .token
                .as_ref()
                .ok_or(crate::approval::ApprovalError::MissingToken)?;
            if grant.thread_id != view.thread().id
                || grant.thread_generation != view.thread().generation
                || view.thread().origin.as_ref() != Some(&grant.origin)
                || grant.execution_id != current.execution_id
                || grant.route != current.definition.route
                || grant.definition_revision != current.definition.revision
                || grant.task_id != task.spec.id
                || grant.effect_id != task.spec.effect_id
                || grant.interrupt_id != pending.id
                || grant.policy_revision != request.context.policy_revision
                || grant.task_parking_revision == 0
                || grant.task_parking_revision > current.revision
            {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            let original = measure(
                CanonicalValue::Json(&grant.prompt),
                CanonicalLimit::LegacyPrompt,
                budget,
            )?;
            let parked = measure(
                CanonicalValue::Json(&pending.prompt),
                CanonicalLimit::LegacyPrompt,
                budget,
            )?;
            reserve_measure(original, budget)?;
            reserve_measure(parked, budget)?;
            if grant.prompt != pending.prompt {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            let proposal =
                current_permission_approval_proposal_digest(task, &pending.prompt, budget)?;
            if grant.proposal_digest != proposal {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            let verifier = <sha2::Sha256 as sha2::Digest>::digest(token.as_str().as_bytes());
            let difference = grant
                .verifier
                .iter()
                .zip(verifier.iter())
                .fold(0u8, |difference, (left, right)| difference | (left ^ right));
            if difference != 0 {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            if grant.consumed {
                return Err(crate::approval::ApprovalError::Consumed.into());
            }
            if request.now_ms >= grant.expires_at_ms {
                return Err(crate::approval::ApprovalError::Expired.into());
            }
        }
        Ok(())
    })();
    budget.finish(result)
}

pub(crate) fn quote_permission_always_gate_selection(
    view: &super::PermissionView,
    request: &crate::approval::ApprovalAcceptanceRequest,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let result = (|| {
        let t = view.execution().snapshot.tasks.len();
        let work = add(t, mul(t, request.decisions.len())?)?;
        budget.reserve(mul(1_024, work)?, mul(4, work)?)
    })();
    budget.finish(result)
}

pub(crate) fn begin_permission_budgeted_unclaimed_gate(
    claimed: bool,
    entries: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let result = ready_authorization_checked_metadata_value(|| {
        budget.reserve(0, 0)?;
        if claimed {
            return Err(PermissionError::UnsupportedProfile);
        }
        // Both actual route lookups, context/phase/errors and the quote loop.
        budget.reserve(65_536, 256)?;
        budget.reserve(16_384, 64)?;
        budget.reserve(mul(128, entries)?, mul(2, entries)?)
    })();
    budget.finish(result)
}

pub(crate) fn quote_permission_budgeted_unclaimed_gate(
    definition: &Definition,
    native: &Definition,
    task: &TaskRecord,
    identity: &ApplicationIdentity,
    entries: &[super::ToolBindingRegistration],
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let result = ready_authorization_checked_metadata_value(|| {
        let provided = measure(
            CanonicalValue::Definition(definition),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        let selected = measure(
            CanonicalValue::Definition(native),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        let spec = measure(
            CanonicalValue::TaskSpec(&task.spec),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        reserve_measure(provided, budget)?;
        let tasks = definition
            .batches
            .iter()
            .map(ready_authorization_checked_metadata_value(
                Vec::<b4_contract::TaskSpec>::len,
            ))
            .try_fold(0usize, ready_authorization_checked_metadata_value(add))?;
        reserve_measure(provided, budget)?;
        reserve_measure(selected, budget)?;
        // Every TaskSpec pair in the old .any search is paid before the search.
        budget.reserve(
            add(mul(tasks, spec.bytes)?, provided.bytes)?,
            add(mul(tasks, spec.nodes)?, provided.nodes)?,
        )?;
        for entry in entries {
            let entry_definition = measure(
                CanonicalValue::Definition(&entry.definition),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            reserve_measure(provided, budget)?;
            reserve_measure(entry_definition, budget)?;
            // Definition equality short-circuiting does not refund the EffectId pair.
            budget.reserve(
                add(
                    entry.effect_id.as_str().len(),
                    task.spec.effect_id.as_str().len(),
                )?,
                2,
            )?;
            // Conservatively quote every candidate owned binding, not only the
            // eventual selected registration. No clone or refund occurs here.
            budget.reserve(
                add(
                    mul(128, entry_definition.nodes)?,
                    mul(6, entry_definition.bytes)?,
                )?,
                entry_definition.nodes,
            )?;
            reserve_measure(entry_definition, budget)?;
            budget.reserve(4_096, 1)?;
            let labels = add(
                add(identity.application.len(), identity.revision.len())?,
                add(
                    entry.effect_id.as_str().len(),
                    add(
                        entry.tool_id.as_str().len(),
                        add(
                            entry.tool_revision.as_str().len(),
                            entry.constraint_revision.as_str().len(),
                        )?,
                    )?,
                )?,
            )?;
            budget.reserve(add(1_024, mul(6, labels)?)?, 6)?;
        }
        Ok(())
    })();
    budget.finish(result)
}

// Specific private handoff quote; this exposes no generic account mutation.
impl PermissionEvidenceBudget {
    pub(super) fn quote_permission_prepared_handoff(&mut self) -> Result<(), PermissionError> {
        self.reserve(4_096, 16)?;
        // Defensive finite phase facts only. Actual C2 service lineage and paid
        // full matching supply the handoff origin, never these counters alone.
        if self.resource.reads != 2 || self.resource.combined_reads != 2 {
            return Err(PermissionError::UnsupportedProfile);
        }
        Ok(())
    }

    pub(super) fn quote_permission_prepared_factory(&mut self) -> Result<(), PermissionError> {
        // The service already paid the owning argument transfer. Remaining
        // checks, construction, result/port handoffs and errors are paid here.
        self.reserve(28_672, 496)
    }
}

// Private pair encoding retains all canonical/inherent passes; only the
// physical depth64 frame allocation is reused. No public wrapper is discounted.
struct CanonicalWorkspace<'data> {
    stack: Vec<CanonicalFrame<'data>>,
}
impl<'data> CanonicalWorkspace<'data> {
    fn new(budget: &mut PermissionEvidenceBudget) -> Result<Self, PermissionError> {
        budget.reserve(mul(add(MAX_DEPTH, 1)?, 512)?, 0)?;
        let mut stack = Vec::new();
        stack
            .try_reserve_exact(add(MAX_DEPTH, 1)?)
            .map_err(runtime_checked_mandatory_callable(|_| {
                PermissionError::LimitExceeded
            }))?;
        Ok(Self { stack })
    }

    fn walk(
        &mut self,
        value: CanonicalValue<'data>,
        limit: CanonicalLimit,
        output: &mut dyn std::io::Write,
        budget: &mut PermissionEvidenceBudget,
        pay_output: bool,
    ) -> Result<CanonicalMeasure, PermissionError> {
        // This workspace is only for the existing depth64 pair profiles.
        // Every pass starts drained; on any failure the pair drops the workspace.
        if !self.stack.is_empty() || limit.depth() > MAX_DEPTH {
            return corrupt();
        }
        walk_with_stack(value, limit, output, budget, pay_output, &mut self.stack)
    }
}
const _: () = assert!(MAX_DEPTH == 64);
const _: () = assert!(std::mem::size_of::<CanonicalWorkspace<'static>>() <= 128);
const _: () = assert!(
    65 * std::mem::size_of::<CanonicalFrame<'static>>()
        + std::mem::size_of::<CanonicalWorkspace<'static>>()
        + std::mem::size_of::<WalkState>()
        + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
        <= 65 * 512
);

fn pair_measure<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    value: CanonicalValue<'data>,
    limit: CanonicalLimit,
    budget: &mut PermissionEvidenceBudget,
) -> Result<CanonicalMeasure, PermissionError> {
    workspace.walk(value, limit, &mut std::io::sink(), budget, true)
}

fn pair_hash_canonical<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    value: CanonicalValue<'data>,
    limit: CanonicalLimit,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    let measured = pair_measure(workspace, value, limit, budget)?;
    budget.reserve(measured.bytes, measured.nodes)?;
    let mut output = HashWriter(<sha2::Sha256 as sha2::Digest>::new());
    let emitted = workspace.walk(value, limit, &mut output, budget, false)?;
    if emitted.bytes != measured.bytes
        || emitted.nodes != measured.nodes
        || emitted.depth != measured.depth
    {
        return Err(PermissionError::CorruptRecord);
    }
    budget.reserve(96, 0)?;
    let digest = sha2::Digest::finalize(output.0);
    // The new64-byte output is a distinct temporary owner. Its
    // failure/unique disposal is prepaid before construction.
    if budget.runtime_ready_cancellation_has_fixed_purchase() {
        budget.reserve(512, 1)?;
    }
    let mut result = String::new();
    result
        .try_reserve_exact(64)
        .map_err(runtime_checked_mandatory_callable(|_| {
            PermissionError::LimitExceeded
        }))?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    Ok(result)
}

fn pair_permission_proposal_digest<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    task: &'data TaskRecord,
    prompt: &'data serde_json::Value,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    let result = pair_hash_canonical(
        workspace,
        CanonicalValue::LegacyProposal { task, prompt },
        CanonicalLimit::Evidence64KiB,
        budget,
    );
    budget.finish(result)
}

fn checked_pair_definition<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    definition: &'data Definition,
    budget: &mut PermissionEvidenceBudget,
) -> Result<usize, PermissionError> {
    let measured = pair_measure(
        workspace,
        CanonicalValue::Definition(definition),
        CanonicalLimit::Evidence64KiB,
        budget,
    )?;
    reserve_measure(measured, budget)?;
    if definition.batches.is_empty()
        || definition.batches.len() > 64
        || definition.batches.iter().any(Vec::is_empty)
    {
        return corrupt();
    }
    let count = definition
        .batches
        .iter()
        .map(Vec::len)
        .try_fold(0usize, add)?;
    if count == 0
        || count > 64
        || definition.max_attempts == 0
        || definition.max_attempts > 64
        || u64::from(definition.max_attempts) < count as u64
    {
        return corrupt();
    }
    let mut bytes = 0;
    for task in definition.batches.iter().flatten() {
        bytes = add(
            bytes,
            add(task.id.as_str().len(), task.effect_id.as_str().len())?,
        )?;
    }
    budget.reserve(mul(mul(2, count)?, bytes)?, mul(4, mul(count, count)?)?)?;
    for (index, task) in definition.batches.iter().flatten().enumerate() {
        for other in definition.batches.iter().flatten().skip(index + 1) {
            if task.id == other.id || task.effect_id == other.effect_id {
                return corrupt();
            }
        }
    }
    Ok(count)
}

fn pair_equal_input<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    left: &'data serde_json::Map<String, serde_json::Value>,
    right: &'data serde_json::Map<String, serde_json::Value>,
    budget: &mut PermissionEvidenceBudget,
    limit: CanonicalLimit,
) -> Result<(), PermissionError> {
    let left_quote = pair_measure(workspace, CanonicalValue::Input(left), limit, budget)?;
    let right_quote = pair_measure(workspace, CanonicalValue::Input(right), limit, budget)?;
    budget.reserve(
        add(left_quote.bytes, right_quote.bytes)?,
        add(left_quote.nodes, right_quote.nodes)?,
    )?;
    // The actual comparison is performed by old validate_snapshot, after these fees.
    Ok(())
}

fn checked_pair_snapshot<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    snapshot: &'data Snapshot,
    definition_count: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    checked_pair_snapshot_capture(workspace, snapshot, definition_count, budget).map(|_| ())
}

fn checked_pair_snapshot_capture<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    snapshot: &'data Snapshot,
    definition_count: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<CanonicalMeasure, PermissionError> {
    let limit = CanonicalLimit::Evidence64KiB;
    let measured = pair_measure(workspace, CanonicalValue::Snapshot(snapshot), limit, budget)?;
    reserve_measure(measured, budget)?;
    let h = snapshot.turn_history.len();
    let a = snapshot.attempt_history.len();
    let t = snapshot.tasks.len();
    // This fee precedes the quote scans/counts and kernel's repeated shallow scans.
    budget.reserve(0, mul(25, add(add(h, a)?, add(definition_count, t)?)?)?)?;
    let u = snapshot
        .tasks
        .iter()
        .filter(|task| task.attempt_id.is_some())
        .count();
    let p = snapshot
        .tasks
        .iter()
        .filter(|task| task.interrupt.is_some())
        .count();
    let batch = snapshot
        .definition
        .batches
        .get(snapshot.batch_index)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let b = batch.len();
    // expected may have d entries even before a malformed shorter tasks vector
    // is rejected; reserve d for that Vec independently of t.
    let slots = add(add(add(h, a)?, mul(4, definition_count)?)?, add(u, p)?)?;
    budget.reserve(mul(128, slots)?, 0)?;
    let turn_bytes = snapshot
        .turn_history
        .iter()
        .map(|id| id.as_str().len())
        .try_fold(0usize, add)?;
    let attempt_bytes = snapshot
        .attempt_history
        .iter()
        .map(|id| id.as_str().len())
        .try_fold(0usize, add)?;
    let spec_id_bytes = snapshot
        .definition
        .batches
        .iter()
        .flatten()
        .map(|spec| spec.id.as_str().len())
        .try_fold(0usize, add)?;
    let spec_effect_bytes = snapshot
        .definition
        .batches
        .iter()
        .flatten()
        .map(|spec| spec.effect_id.as_str().len())
        .try_fold(0usize, add)?;
    let task_attempt_bytes = snapshot
        .tasks
        .iter()
        .filter_map(|task| task.attempt_id.as_ref())
        .map(|id| id.as_str().len())
        .try_fold(0usize, add)?;
    let interrupt_bytes = snapshot
        .tasks
        .iter()
        .filter_map(|task| task.interrupt.as_ref())
        .map(|value| value.id.as_str().len())
        .try_fold(0usize, add)?;
    id_family(h, turn_bytes, budget)?;
    id_family(a, attempt_bytes, budget)?;
    id_family(definition_count, spec_id_bytes, budget)?;
    id_family(definition_count, spec_effect_bytes, budget)?;
    id_family(u, task_attempt_bytes, budget)?;
    id_family(p, interrupt_bytes, budget)?;
    budget.reserve(
        add(mul(u, attempt_bytes)?, mul(a, task_attempt_bytes)?)?,
        mul(2, mul(u, a)?)?,
    )?;
    let task_ids = snapshot
        .tasks
        .iter()
        .map(|task| task.spec.id.as_str().len())
        .try_fold(0usize, add)?;
    let batch_ids = batch
        .iter()
        .map(|spec| spec.id.as_str().len())
        .try_fold(0usize, add)?;
    // Both this quote loop and old kernel perform the current-batch membership scan.
    budget.reserve(
        mul(2, add(mul(b, task_ids)?, mul(t, batch_ids)?)?)?,
        mul(4, mul(t, b)?)?,
    )?;
    let mut previous_input = None;
    for (task, spec) in snapshot.tasks.iter().zip(
        snapshot
            .definition
            .batches
            .iter()
            .take(snapshot.batch_index.saturating_add(1))
            .flatten(),
    ) {
        let task_quote = pair_measure(
            workspace,
            CanonicalValue::TaskSpec(&task.spec),
            limit,
            budget,
        )?;
        let spec_quote = pair_measure(workspace, CanonicalValue::TaskSpec(spec), limit, budget)?;
        budget.reserve(
            add(task_quote.bytes, spec_quote.bytes)?,
            add(task_quote.nodes, spec_quote.nodes)?,
        )?;
        if batch.iter().any(|member| member.id == task.spec.id) {
            if let Some(previous) = previous_input {
                pair_equal_input(workspace, previous, &task.input, budget, limit)?;
            }
            previous_input = Some(&task.input);
        }
    }
    if let Some(input) = previous_input {
        pair_equal_input(workspace, input, &snapshot.values, budget, limit)?;
    }
    budget.reserve(4096, 0)?;
    b4_kernel::validate_snapshot(snapshot).map_err(runtime_checked_mandatory_callable(|_| {
        PermissionError::CorruptRecord
    }))?;
    Ok(measured)
}

fn check_pair_inherent<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    record: BorrowedRecord<'data>,
    measured: &CanonicalMeasure,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    // Pay the root/shallow counting/label/selection scans before any such scans.
    reserve_measure(*measured, budget)?;
    match record {
        BorrowedRecord::Decision(value) => {
            if value.record_version != 1 {
                return corrupt();
            }
            let scope = &value.scope;
            labels(
                &scope.application,
                &scope.policy_revision,
                Some(&scope.origin),
            )?;
            let d = checked_pair_definition(workspace, &scope.definition, budget)?;
            budget.reserve(mul(512, d)?, mul(2, d)?)?;
            if scope.route != scope.definition.route
                || !scope
                    .definition
                    .batches
                    .iter()
                    .flatten()
                    .any(|spec| spec.effect_id == scope.effect_id && spec.requires_approval)
            {
                return corrupt();
            }
            let digest = pair_hash_canonical(
                workspace,
                CanonicalValue::Scope(ScopeParts::from_scope(scope)),
                CanonicalLimit::Evidence64KiB,
                budget,
            )?;
            budget.reserve(512, 8)?;
            if !digest_shape(&value.scope_digest)
                || digest != value.scope_digest
                || value.permission_id.as_str().strip_prefix("b4pd_") != Some(digest.as_str())
            {
                return corrupt();
            }
        }
        BorrowedRecord::Acceptance(value) => {
            use b4_contract::{ExecutionStatus as E, TaskStatus as T};
            let before = &value.before_snapshot;
            labels(
                &value.application,
                &value.policy_revision,
                Some(&value.origin),
            )?;
            if value.record_version != 1
                || before.status != E::WaitingForApproval
                || before.failure.is_some()
            {
                return corrupt();
            }
            let count = checked_pair_definition(workspace, &before.definition, budget)?;
            checked_pair_snapshot(workspace, before, count, budget)?;
            let revision = before
                .revision
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?;
            budget.reserve(
                mul(512, add(before.turn_history.len(), 1)?)?,
                mul(2, add(before.turn_history.len(), 1)?)?,
            )?;
            if value.committed_revision != revision
                || value.facts.now_ms < before.updated_at_ms
                || value.turn_id == before.turn_id
                || before.turn_history.contains(&value.turn_id)
                || value.decisions.is_empty()
                || value.decisions.len() > 64
            {
                return corrupt();
            }
            // Eight conservative 512-byte pair units cover repeated typed-ID,
            // binding, digest shape/equality and permission suffix comparisons.
            budget.reserve(
                mul(4096, value.decisions.len())?,
                mul(32, add(before.tasks.len(), value.decisions.len())?)?,
            )?;
            let waiting = before
                .tasks
                .iter()
                .filter(|task| task.status == T::WaitingForApproval);
            if waiting.clone().count() != value.decisions.len() {
                return corrupt();
            }
            let mut always = false;
            for (task, entry) in waiting.zip(&value.decisions) {
                let Some(interrupt) = &task.interrupt else {
                    return corrupt();
                };
                if entry.task_id != task.spec.id
                    || entry.interrupt_id != interrupt.id
                    || entry.grant_ref.execution_id != before.execution_id
                    || entry.grant_ref.interrupt_id != interrupt.id
                    || entry.task_parking_revision == 0
                    || entry.task_parking_revision > before.revision
                {
                    return corrupt();
                }
                let proposal =
                    pair_permission_proposal_digest(workspace, task, &interrupt.prompt, budget)?;
                if !digest_shape(&entry.proposal_digest) || proposal != entry.proposal_digest {
                    return corrupt();
                }
                match entry.decision {
                    ApprovalDecision::Deny => {
                        if entry.binding.is_some() || entry.permission_id.is_some() {
                            return corrupt();
                        }
                    }
                    ApprovalDecision::Once | ApprovalDecision::Always => {
                        let Some(binding) = &entry.binding else {
                            return corrupt();
                        };
                        if binding.effect_id != task.spec.effect_id {
                            return corrupt();
                        }
                        if entry.decision == ApprovalDecision::Once {
                            if entry.permission_id.is_some() {
                                return corrupt();
                            }
                        } else {
                            always = true;
                            let parts = ScopeParts {
                                thread_id: &value.thread_id,
                                thread_generation: &value.thread_generation,
                                origin: &value.origin,
                                application: &value.application,
                                route: &before.definition.route,
                                definition: &before.definition,
                                effect_id: &binding.effect_id,
                                tool_id: &binding.tool_id,
                                tool_revision: &binding.tool_revision,
                                policy_revision: &value.policy_revision,
                                constraint_revision: &binding.constraint_revision,
                            };
                            let digest = pair_hash_canonical(
                                workspace,
                                CanonicalValue::Scope(parts),
                                CanonicalLimit::Evidence64KiB,
                                budget,
                            )?;
                            if entry
                                .permission_id
                                .as_ref()
                                .map(|id| id.as_str().strip_prefix("b4pd_"))
                                != Some(Some(digest.as_str()))
                            {
                                return corrupt();
                            }
                        }
                    }
                }
            }
            if !always {
                return corrupt();
            }
            // Borrowed Resume derivation: complete exact-set decisions make each
            // waiting record Ready/approved or Denied/unapproved, retaining its
            // actual interrupt/spec/input/history. At least one Always keeps
            // this batch Ready, so settle cannot reduce/advance/clone output.
            // Fresh turn, incremented revision and checked time were verified.
            // No owned command, proposed snapshot or dispatchable vector is built.
            // At an exhausted attempt budget the derived dispatchable set is
            // empty even though the approved Ready records remain legal.
        }
        BorrowedRecord::Use(_) => return corrupt(),
    }
    Ok(())
}

fn pair_encode_record<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    record: BorrowedRecord<'data>,
    kind: RecordKind,
    budget: &mut PermissionEvidenceBudget,
) -> Result<Vec<u8>, PermissionError> {
    let value = CanonicalValue::Record(record);
    let limit = CanonicalLimit::Record(kind);
    let measured = pair_measure(workspace, value, limit, budget)?;
    check_pair_inherent(workspace, record, &measured, budget)?;
    budget.reserve(measured.bytes, measured.nodes)?;
    let mut output = ExactWriter {
        bytes: Vec::new(),
        maximum: measured.bytes,
    };
    output
        .bytes
        .try_reserve_exact(measured.bytes)
        .map_err(|_| PermissionError::LimitExceeded)?;
    let emitted = workspace.walk(value, limit, &mut output, budget, false)?;
    if emitted.bytes != measured.bytes
        || emitted.nodes != measured.nodes
        || emitted.depth != measured.depth
        || output.bytes.len() != measured.bytes
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(output.bytes)
}

// The eventual writer consumes this private seam; the unit gate uses the same
// release implementation. Public A/D encoders remain independently allocating.
#[cfg_attr(not(test), allow(dead_code))]
fn encode_checked_acceptance_pair(
    acceptance: &PermissionAcceptance,
    decision: &PermissionDecision,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(Vec<u8>, Vec<u8>), PermissionError> {
    let result = (|| {
        // Both immutable record owners precede this local stack borrow. All17
        // original passes run; successful walks drain it completely.
        let mut workspace = CanonicalWorkspace::new(budget)?;
        let acceptance = pair_encode_record(
            &mut workspace,
            BorrowedRecord::Acceptance(acceptance),
            RecordKind::Acceptance,
            budget,
        )?;
        let decision = pair_encode_record(
            &mut workspace,
            BorrowedRecord::Decision(decision),
            RecordKind::Decision,
            budget,
        )?;
        // Frames borrow original records, never these owned output byte vectors.
        // Errors unwind this local workspace before the caller may move records.
        drop(workspace);
        Ok((acceptance, decision))
    })();
    budget.finish(result)
}

// Private proposed-data validation only. A later writer must bind its own paid
// lifecycle; these helpers do not construct durable permission authority.
#[cfg_attr(not(test), allow(dead_code))]
mod prospective_graph {
    use super::*;
    use crate::approval::{ApprovalAcceptance, ApprovalView, GrantRecord};
    use crate::{JournalEntry, StoredExecution, ThreadRecord};
    use b4_contract::{CommitPlan, ExecutionStatus, TaskStatus};

    pub(super) struct PermissionProspectiveGraph<'data> {
        pub(super) before: &'data ApprovalView,
        pub(super) acceptance: &'data PermissionAcceptance,
        pub(super) decision: &'data PermissionDecision,
        pub(super) consumed_grant: &'data GrantRecord,
        pub(super) resume_plan: &'data CommitPlan,
        pub(super) park_journal: &'data JournalEntry,
        pub(super) resume_journal: &'data JournalEntry,
    }

    struct GraphBefore<'data> {
        thread: &'data ThreadRecord,
        execution: &'data StoredExecution,
        grants: &'data [GrantRecord],
        acceptances: &'data [ApprovalAcceptance],
    }

    struct GraphOperands<'data> {
        before: GraphBefore<'data>,
        acceptance: &'data PermissionAcceptance,
        decision: &'data PermissionDecision,
        consumed_grant: &'data GrantRecord,
        resume_plan: &'data CommitPlan,
        park_journal: &'data JournalEntry,
        resume_journal: &'data JournalEntry,
    }

    struct GraphMeasures {
        before: CanonicalMeasure,
        acceptance_before: CanonicalMeasure,
        acceptance: CanonicalMeasure,
        decision: CanonicalMeasure,
        decision_scope: CanonicalMeasure,
        acceptance_scope: CanonicalMeasure,
        original_grant: CanonicalMeasure,
        consumed_grant: CanonicalMeasure,
        resume: CanonicalMeasure,
        overlay: CanonicalMeasure,
        grant_prompt: CanonicalMeasure,
        pending_prompt: CanonicalMeasure,
        park_journal: CanonicalMeasure,
        expected_park: CanonicalMeasure,
        resume_journal: CanonicalMeasure,
        expected_resume: CanonicalMeasure,
    }

    // Only the actual paid entry below can construct this loan. There is no
    // receipt/usage constructor and the core cannot accept a replacement account.
    struct GraphMeasured<'data, 'budget> {
        operands: GraphOperands<'data>,
        measures: GraphMeasures,
        budget: &'budget mut PermissionEvidenceBudget,
    }

    enum GraphProjection {
        FirstSelectedComplete,
    }

    const _: () = assert!(std::mem::size_of::<GraphMeasured<'_, '_>>() <= 1_024);

    pub(super) fn validate_permission_prospective_graph(
        graph: PermissionProspectiveGraph<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            let measured = admit_graph(graph, budget)?;
            check_graph(measured).map(|_| ())
        })();
        budget.finish(result)
    }

    fn admit_graph<'data, 'budget>(
        graph: PermissionProspectiveGraph<'data>,
        budget: &'budget mut PermissionEvidenceBudget,
    ) -> Result<GraphMeasured<'data, 'budget>, PermissionError> {
        // Retained references/measures, closed counting, arithmetic and error
        // work are paid before constructing them or consulting their contents.
        budget.reserve(4_096, 64)?;
        let before = &graph.before.execution.snapshot;
        let next = &graph.resume_plan.snapshot;
        if graph.before.grants.len() != 1
            || !graph.before.acceptances.is_empty()
            || graph.acceptance.decisions.len() != 1
            || before.tasks.len() != 1
            || next.tasks.len() != 1
            || before.definition.batches.len() != 1
            || before.definition.batches[0].len() != 1
            || before.turn_history.len() > 2
            || next.turn_history.len() > 3
            || !before.attempt_history.is_empty()
            || !next.attempt_history.is_empty()
            || graph.resume_plan.dispatchable.len() > 1
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        // Avoid all terminal reducer/output clone paths before invoking the paid
        // old strict validator. This first graph is a single immediate Resume.
        if before.status != ExecutionStatus::WaitingForApproval
            || before.failure.is_some()
            || next.status != ExecutionStatus::Running
            || next.failure.is_some()
        {
            return corrupt();
        }
        let entry = &graph.acceptance.decisions[0];
        let binding = entry
            .binding
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let pending = before.tasks[0]
            .interrupt
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let original = &graph.before.grants[0];
        if original.task_parking_revision != before.revision
            || original.issued_at_ms != before.updated_at_ms
        {
            return Err(PermissionError::UnsupportedProfile);
        }

        // Every allocating measure/check below uses this same actual account.
        // Public primitives keep their existing 65/66/128-frame profile charges.
        let acceptance = measure(
            CanonicalValue::Record(BorrowedRecord::Acceptance(graph.acceptance)),
            CanonicalLimit::Record(RecordKind::Acceptance),
            budget,
        )?;
        check_inherent(
            BorrowedRecord::Acceptance(graph.acceptance),
            &acceptance,
            budget,
        )?;
        let decision = measure(
            CanonicalValue::Record(BorrowedRecord::Decision(graph.decision)),
            CanonicalLimit::Record(RecordKind::Decision),
            budget,
        )?;
        check_inherent(BorrowedRecord::Decision(graph.decision), &decision, budget)?;
        let definition_count = checked_definition(&before.definition, budget)?;
        let before_measure = checked_snapshot_profile_capture(
            before,
            definition_count,
            budget,
            CanonicalLimit::Evidence64KiB,
            false,
        )?;
        let next_definition_count = checked_definition(&next.definition, budget)?;
        let resume = checked_snapshot_profile_capture(
            next,
            next_definition_count,
            budget,
            CanonicalLimit::Evidence64KiB,
            false,
        )?;
        let acceptance_before = measure(
            CanonicalValue::Snapshot(&graph.acceptance.before_snapshot),
            CanonicalLimit::Evidence64KiB,
            budget,
        )?;
        let decision_scope = measure(
            CanonicalValue::ScopeBody(ScopeParts::from_scope(&graph.decision.scope)),
            CanonicalLimit::Evidence64KiB,
            budget,
        )?;
        let parts = acceptance_scope(graph.acceptance, binding);
        let acceptance_scope = measure(
            CanonicalValue::ScopeBody(parts),
            CanonicalLimit::Evidence64KiB,
            budget,
        )?;
        let original_grant = quote_grant(original, budget)?;
        let consumed_grant = quote_grant(graph.consumed_grant, budget)?;
        let grant_prompt = measure(
            CanonicalValue::Json(&original.prompt),
            CanonicalLimit::LegacyPrompt,
            budget,
        )?;
        let pending_prompt = measure(
            CanonicalValue::Json(&pending.prompt),
            CanonicalLimit::LegacyPrompt,
            budget,
        )?;
        let overlay = quote_resume_overlay(before, graph.acceptance, before_measure, budget)?;
        let park_journal = quote_journal(
            &graph.park_journal.execution_id,
            graph.park_journal.revision,
            graph.park_journal.owner_generation,
            graph.park_journal.status,
            graph.park_journal.updated_at_ms,
            budget,
        )?;
        let expected_park = quote_journal(
            &before.execution_id,
            before.revision,
            before.owner_generation,
            before.status,
            before.updated_at_ms,
            budget,
        )?;
        let resume_journal = quote_journal(
            &graph.resume_journal.execution_id,
            graph.resume_journal.revision,
            graph.resume_journal.owner_generation,
            graph.resume_journal.status,
            graph.resume_journal.updated_at_ms,
            budget,
        )?;
        let expected_resume = quote_journal(
            &next.execution_id,
            next.revision,
            next.owner_generation,
            next.status,
            next.updated_at_ms,
            budget,
        )?;
        let operands = GraphOperands {
            before: GraphBefore {
                thread: &graph.before.thread,
                execution: &graph.before.execution,
                grants: &graph.before.grants,
                acceptances: &graph.before.acceptances,
            },
            acceptance: graph.acceptance,
            decision: graph.decision,
            consumed_grant: graph.consumed_grant,
            resume_plan: graph.resume_plan,
            park_journal: graph.park_journal,
            resume_journal: graph.resume_journal,
        };
        let measures = GraphMeasures {
            before: before_measure,
            acceptance_before,
            acceptance,
            decision,
            decision_scope,
            acceptance_scope,
            original_grant,
            consumed_grant,
            resume,
            overlay,
            grant_prompt,
            pending_prompt,
            park_journal,
            expected_park,
            resume_journal,
            expected_resume,
        };
        Ok(GraphMeasured {
            operands,
            measures,
            budget,
        })
    }

    pub(super) struct WriterGraphFacts {
        pub(super) before: CanonicalMeasure,
        pub(super) acceptance_before: CanonicalMeasure,
        pub(super) acceptance: CanonicalMeasure,
        pub(super) decision: CanonicalMeasure,
        pub(super) decision_scope: CanonicalMeasure,
        pub(super) acceptance_scope: CanonicalMeasure,
        pub(super) original_grant: CanonicalMeasure,
        pub(super) consumed_grant: CanonicalMeasure,
        pub(super) resume: CanonicalMeasure,
        pub(super) overlay: CanonicalMeasure,
        pub(super) grant_prompt: CanonicalMeasure,
        pub(super) pending_prompt: CanonicalMeasure,
        pub(super) park_journal: CanonicalMeasure,
        pub(super) expected_park: CanonicalMeasure,
        pub(super) resume_journal: CanonicalMeasure,
        pub(super) expected_resume: CanonicalMeasure,
    }

    #[allow(clippy::too_many_arguments)] // Closed actual owned lifecycle operands, no public factory.
    pub(super) fn validate_writer_graph(
        resource: &crate::approval::ApprovalResource,
        grants: &[GrantRecord],
        acceptance: &PermissionAcceptance,
        decision: &PermissionDecision,
        consumed_grant: &GrantRecord,
        resume_plan: &CommitPlan,
        park_journal: &JournalEntry,
        resume_journal: &JournalEntry,
        facts: WriterGraphFacts,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        // Only the private actual writer lifecycle supplies this bundle. All
        // facts were measured there on this same still-live account; receipts
        // have no constructor for it and no frame allocation is hidden here.
        let result = {
            let measures = GraphMeasures {
                before: facts.before,
                acceptance_before: facts.acceptance_before,
                acceptance: facts.acceptance,
                decision: facts.decision,
                decision_scope: facts.decision_scope,
                acceptance_scope: facts.acceptance_scope,
                original_grant: facts.original_grant,
                consumed_grant: facts.consumed_grant,
                resume: facts.resume,
                overlay: facts.overlay,
                grant_prompt: facts.grant_prompt,
                pending_prompt: facts.pending_prompt,
                park_journal: facts.park_journal,
                expected_park: facts.expected_park,
                resume_journal: facts.resume_journal,
                expected_resume: facts.expected_resume,
            };
            let operands = GraphOperands {
                before: GraphBefore {
                    thread: &resource.thread,
                    execution: &resource.execution,
                    grants,
                    acceptances: &[],
                },
                acceptance,
                decision,
                consumed_grant,
                resume_plan,
                park_journal,
                resume_journal,
            };
            check_graph(GraphMeasured {
                operands,
                measures,
                budget,
            })
            .map(|_| ())
        };
        budget.finish(result)
    }
    fn quote_grant(
        grant: &GrantRecord,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<CanonicalMeasure, PermissionError> {
        let measured = measure(
            CanonicalValue::LegacyGrant(grant),
            CanonicalLimit::LegacyRecord,
            budget,
        )?;
        reserve_measure(measured, budget)?;
        // The same paid inherited shape recipe as the bounded old decoder,
        // without serializing, cloning or materializing a second grant record.
        budget.reserve(32_768, 128)?;
        if !valid_label(&grant.origin.application)
            || !valid_label(&grant.origin.tenant)
            || !valid_label(&grant.origin.principal)
            || !valid_label(&grant.policy_revision)
            || grant.task_parking_revision == 0
            || !digest_shape(&grant.proposal_digest)
            || !digest_shape(&grant.resume_key)
        {
            return corrupt();
        }
        let ttl = grant
            .expires_at_ms
            .checked_sub(grant.issued_at_ms)
            .ok_or(PermissionError::CorruptRecord)?;
        if ttl == 0 || ttl > crate::approval::MAX_APPROVAL_TTL_MS {
            return corrupt();
        }
        let verifier = <sha2::Sha256 as sha2::Digest>::digest(grant.token.as_str().as_bytes());
        let difference = grant
            .verifier
            .iter()
            .zip(verifier.iter())
            .fold(0u8, |difference, (left, right)| difference | (left ^ right));
        if difference != 0 {
            return corrupt();
        }
        measure(
            CanonicalValue::Json(&grant.prompt),
            CanonicalLimit::LegacyPrompt,
            budget,
        )?;
        Ok(measured)
    }

    fn acceptance_scope<'data>(
        acceptance: &'data PermissionAcceptance,
        binding: &'data PermissionBindingIdentity,
    ) -> ScopeParts<'data> {
        ScopeParts {
            thread_id: &acceptance.thread_id,
            thread_generation: &acceptance.thread_generation,
            origin: &acceptance.origin,
            application: &acceptance.application,
            route: &acceptance.before_snapshot.definition.route,
            definition: &acceptance.before_snapshot.definition,
            effect_id: &binding.effect_id,
            tool_id: &binding.tool_id,
            tool_revision: &binding.tool_revision,
            policy_revision: &acceptance.policy_revision,
            constraint_revision: &binding.constraint_revision,
        }
    }

    fn decimal_width(value: u64) -> usize {
        if value == 0 {
            1
        } else {
            value.ilog10() as usize + 1
        }
    }

    fn replace_width(bytes: usize, old: usize, new: usize) -> Result<usize, PermissionError> {
        add(
            bytes
                .checked_sub(old)
                .ok_or(PermissionError::CorruptRecord)?,
            new,
        )
    }

    fn quote_resume_overlay(
        before: &Snapshot,
        acceptance: &PermissionAcceptance,
        before_measure: CanonicalMeasure,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<CanonicalMeasure, PermissionError> {
        // Closed scalar width arithmetic, retained measure and two separately
        // paid exact JSON-string measurements; no owned Snapshot is assembled.
        budget.reserve(1_024, 32)?;
        let old_turn = measure(
            CanonicalValue::String(before.turn_id.as_str()),
            CanonicalLimit::Evidence64KiB,
            budget,
        )?;
        let new_turn = measure(
            CanonicalValue::String(acceptance.turn_id.as_str()),
            CanonicalLimit::Evidence64KiB,
            budget,
        )?;
        let revision = before
            .revision
            .checked_add(1)
            .ok_or(PermissionError::CorruptRecord)?;
        let mut bytes = before_measure.bytes;
        bytes = replace_width(
            bytes,
            decimal_width(before.revision),
            decimal_width(revision),
        )?;
        bytes = replace_width(
            bytes,
            decimal_width(before.updated_at_ms),
            decimal_width(acceptance.facts.now_ms),
        )?;
        bytes = replace_width(bytes, old_turn.bytes, new_turn.bytes)?;
        // Waiting->Running root, Waiting->Ready task and false->true approval.
        bytes = replace_width(bytes, "waiting_for_approval".len(), "running".len())?;
        bytes = replace_width(bytes, "waiting_for_approval".len(), "ready".len())?;
        bytes = replace_width(bytes, "false".len(), "true".len())?;
        bytes = add(bytes, add(1, new_turn.bytes)?)?; // Existing nonempty history + comma + turn.
        Ok(CanonicalMeasure {
            bytes,
            nodes: add(before_measure.nodes, 1)?,
            depth: before_measure.depth,
        })
    }

    fn quote_journal(
        execution: &ExecutionId,
        revision: u64,
        owner: u64,
        status: ExecutionStatus,
        updated: u64,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<CanonicalMeasure, PermissionError> {
        // A closed five-field tuple: fixed keys/separators, three decimal
        // scalars and one finite enum. ID escaping is measured by the old walker.
        budget.reserve(1_024, 16)?;
        let execution = measure(
            CanonicalValue::String(execution.as_str()),
            CanonicalLimit::Evidence64KiB,
            budget,
        )?;
        let keys = b"{\"execution_id\":,\"owner_generation\":,\"revision\":,\"status\":,\"updated_at_ms\":}".len();
        let numbers = add(
            add(decimal_width(revision), decimal_width(owner))?,
            decimal_width(updated),
        )?;
        Ok(CanonicalMeasure {
            bytes: add(
                add(add(keys, execution.bytes)?, numbers)?,
                add(2, execution_status(status).len())?,
            )?,
            nodes: 11,
            depth: 1,
        })
    }

    fn bounded(measured: CanonicalMeasure, bytes: usize, nodes: usize) -> bool {
        measured.bytes <= bytes && measured.nodes <= nodes && measured.depth <= MAX_DEPTH
    }

    struct CoreWork {
        string_pairs: usize,
    }
    impl CoreWork {
        fn same(&mut self, left: &str, right: &str) -> Result<bool, PermissionError> {
            if self.string_pairs == 64 || left.len() > 256 || right.len() > 256 {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.string_pairs += 1;
            Ok(left == right)
        }
        fn origin(
            &mut self,
            left: &ThreadOrigin,
            right: &ThreadOrigin,
        ) -> Result<bool, PermissionError> {
            Ok(self.same(&left.application, &right.application)?
                && self.same(&left.tenant, &right.tenant)?
                && self.same(&left.principal, &right.principal)?)
        }
    }

    fn check_graph(measured: GraphMeasured<'_, '_>) -> Result<GraphProjection, PermissionError> {
        let GraphMeasured {
            operands: graph,
            measures: q,
            budget,
        } = measured;
        // Frozen core only. Every fresh entry measurement/allocation above is
        // separately paid, and this full debit precedes ALL link/Eq/core scans.
        budget.reserve(65_536, 2_048)?;
        let limits = budget.original_always_limits();
        if !bounded(q.before, limits.before, limits.before_nodes)
            || !bounded(q.acceptance_before, limits.before, limits.before_nodes)
            || !bounded(q.acceptance, limits.acceptance, limits.acceptance_nodes)
            || !bounded(q.decision, limits.decision, 67)
            || !bounded(q.decision_scope, limits.scope, 51)
            || !bounded(q.acceptance_scope, limits.scope, 51)
            || !bounded(q.original_grant, limits.grant, 83)
            || !bounded(q.consumed_grant, limits.grant, 83)
            || !bounded(q.resume, limits.resume, limits.resume_nodes)
            || !bounded(q.overlay, limits.resume, limits.resume_nodes)
            || !bounded(q.grant_prompt, 128, 16)
            || !bounded(q.pending_prompt, 128, 16)
            || !bounded(q.park_journal, limits.journal, 11)
            || !bounded(q.expected_park, limits.journal, 11)
            || !bounded(q.resume_journal, limits.journal, 11)
            || !bounded(q.expected_resume, limits.journal, 11)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let mut equality_bytes = 0;
        let mut equality_nodes = 0;
        // Logical creator/reference operands remain paid even when they alias
        // their singleton selected objects. Prompt binding is an extra paid Eq.
        for (left, right) in [
            (q.acceptance_before, q.before),
            (q.decision_scope, q.acceptance_scope),
            (q.consumed_grant, q.original_grant),
            (q.resume, q.overlay),
            (q.park_journal, q.expected_park),
            (q.resume_journal, q.expected_resume),
            (q.acceptance, q.acceptance),
            (q.consumed_grant, q.consumed_grant),
            (q.grant_prompt, q.pending_prompt),
        ] {
            equality_bytes = add(equality_bytes, add(left.bytes, right.bytes)?)?;
            equality_nodes = add(equality_nodes, add(left.nodes, right.nodes)?)?;
        }
        if equality_bytes > limits.equality || equality_nodes > limits.paired_equality_nodes {
            return Err(PermissionError::UnsupportedProfile);
        }

        // Full paired operands, including aliases, are paid before any Eq/core
        // scan. The constructor-selected profile keeps a finite structural ceiling.
        budget.reserve_normal_original_work(
            &[CanonicalMeasure {
                bytes: equality_bytes,
                nodes: equality_nodes,
                depth: 0,
            }],
            1,
            1,
        )?;

        let before = &graph.before.execution.snapshot;
        let acceptance = graph.acceptance;
        let decision = graph.decision;
        let entry = &acceptance.decisions[0];
        let task = &before.tasks[0];
        let pending = task
            .interrupt
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let binding = entry
            .binding
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let permission_id = entry
            .permission_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let original = &graph.before.grants[0];
        let thread_origin = graph
            .before
            .thread
            .origin
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let active = graph
            .before
            .thread
            .active_execution_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let mut work = CoreWork { string_pairs: 0 };

        // This independent creator link is absent from the intrinsic codec.
        if !work.same(
            decision.creator_execution_id.as_str(),
            acceptance.before_snapshot.execution_id.as_str(),
        )? || !work.same(
            decision.creator_turn_id.as_str(),
            acceptance.turn_id.as_str(),
        )? || !work.same(decision.creator_task_id.as_str(), entry.task_id.as_str())?
            || !work.same(
                decision.creator_interrupt_id.as_str(),
                entry.interrupt_id.as_str(),
            )?
            || !work.same(decision.permission_id.as_str(), permission_id.as_str())?
        {
            return corrupt();
        }
        if entry.decision != ApprovalDecision::Always
            || task.status != TaskStatus::WaitingForApproval
            || !task.spec.requires_approval
            || task.approved
            || !graph.before.acceptances.is_empty()
            || !work.same(
                acceptance.thread_id.as_str(),
                graph.before.thread.id.as_str(),
            )?
            || !work.same(
                acceptance.thread_generation.as_str(),
                graph.before.thread.generation.as_str(),
            )?
            || !work.origin(&acceptance.origin, thread_origin)?
            || !work.same(
                acceptance.application.application.as_str(),
                thread_origin.application.as_str(),
            )?
            || !work.same(
                graph.before.execution.thread_id.as_str(),
                graph.before.thread.id.as_str(),
            )?
            || !work.same(active.as_str(), before.execution_id.as_str())?
            || acceptance.before_snapshot != *before
        {
            return corrupt();
        }
        if !work.same(original.thread_id.as_str(), acceptance.thread_id.as_str())?
            || !work.same(
                original.thread_generation.as_str(),
                acceptance.thread_generation.as_str(),
            )?
            || !work.origin(&original.origin, &acceptance.origin)?
            || !work.same(original.execution_id.as_str(), before.execution_id.as_str())?
            || !work.same(original.route.as_str(), before.definition.route.as_str())?
            || !work.same(
                original.definition_revision.as_str(),
                before.definition.revision.as_str(),
            )?
            || !work.same(original.task_id.as_str(), task.spec.id.as_str())?
            || !work.same(original.effect_id.as_str(), task.spec.effect_id.as_str())?
            || !work.same(original.interrupt_id.as_str(), pending.id.as_str())?
            || !work.same(
                original.policy_revision.as_str(),
                acceptance.policy_revision.as_str(),
            )?
            || !work.same(
                original.proposal_digest.as_str(),
                entry.proposal_digest.as_str(),
            )?
            || !work.same(
                entry.grant_ref.execution_id.as_str(),
                original.execution_id.as_str(),
            )?
            || !work.same(
                entry.grant_ref.interrupt_id.as_str(),
                original.interrupt_id.as_str(),
            )?
            || !work.same(entry.task_id.as_str(), original.task_id.as_str())?
            || !work.same(entry.interrupt_id.as_str(), original.interrupt_id.as_str())?
            || !work.same(binding.effect_id.as_str(), task.spec.effect_id.as_str())?
            || original.prompt != pending.prompt
            || original.consumed
            || !graph.consumed_grant.consumed
            || entry.task_parking_revision != original.task_parking_revision
            || original.issued_at_ms > acceptance.facts.now_ms
            || acceptance.facts.now_ms >= original.expires_at_ms
            || !same_consumed_overlay(original, graph.consumed_grant)
            || !same_scope(&decision.scope, acceptance_scope(acceptance, binding))
        {
            return corrupt();
        }
        if !same_resume_overlay(before, acceptance, graph.resume_plan, &mut work)?
            || !same_journal(graph.park_journal, before, &mut work)?
            || !same_journal(graph.resume_journal, &graph.resume_plan.snapshot, &mut work)?
        {
            return corrupt();
        }

        // The supported graph's entire selected sets each have one object.
        // Resolve each required role from those complete sets, then compare
        // the full logical operands; there are no hidden external-only vectors.
        let selected_acceptances = std::slice::from_ref(acceptance);
        let selected_grants = std::slice::from_ref(graph.consumed_grant);
        let creator = selected_acceptances
            .first()
            .ok_or(PermissionError::CorruptRecord)?;
        let referenced = selected_grants
            .first()
            .ok_or(PermissionError::CorruptRecord)?;
        if selected_acceptances.len() != 1
            || selected_grants.len() != 1
            || acceptance != creator
            || graph.consumed_grant != referenced
        {
            return corrupt();
        }
        Ok(GraphProjection::FirstSelectedComplete)
    }

    fn same_scope(scope: &PermissionScope, parts: ScopeParts<'_>) -> bool {
        &scope.thread_id == parts.thread_id
            && &scope.thread_generation == parts.thread_generation
            && &scope.origin == parts.origin
            && &scope.application == parts.application
            && &scope.route == parts.route
            && &scope.definition == parts.definition
            && &scope.effect_id == parts.effect_id
            && &scope.tool_id == parts.tool_id
            && &scope.tool_revision == parts.tool_revision
            && scope.policy_revision.as_str() == parts.policy_revision
            && &scope.constraint_revision == parts.constraint_revision
    }

    fn same_consumed_overlay(original: &GrantRecord, consumed: &GrantRecord) -> bool {
        original.thread_id == consumed.thread_id
            && original.thread_generation == consumed.thread_generation
            && original.origin == consumed.origin
            && original.execution_id == consumed.execution_id
            && original.route == consumed.route
            && original.definition_revision == consumed.definition_revision
            && original.task_id == consumed.task_id
            && original.effect_id == consumed.effect_id
            && original.interrupt_id == consumed.interrupt_id
            && original.resume_key == consumed.resume_key
            && original.task_parking_revision == consumed.task_parking_revision
            && original.policy_revision == consumed.policy_revision
            && original.proposal_digest == consumed.proposal_digest
            && original.prompt == consumed.prompt
            && original.token == consumed.token
            && original.verifier == consumed.verifier
            && original.issued_at_ms == consumed.issued_at_ms
            && original.expires_at_ms == consumed.expires_at_ms
            && !original.consumed
            && consumed.consumed
    }

    fn same_resume_overlay(
        before: &Snapshot,
        acceptance: &PermissionAcceptance,
        plan: &CommitPlan,
        work: &mut CoreWork,
    ) -> Result<bool, PermissionError> {
        let next = &plan.snapshot;
        let task = &before.tasks[0];
        let next_task = &next.tasks[0];
        let revision = before
            .revision
            .checked_add(1)
            .ok_or(PermissionError::CorruptRecord)?;
        let history = before.turn_history.len();
        if plan.expected_revision != before.revision
            || next.schema_version != before.schema_version
            || next.revision != revision
            || acceptance.committed_revision != revision
            || !work.same(next.execution_id.as_str(), before.execution_id.as_str())?
            || !work.same(next.turn_id.as_str(), acceptance.turn_id.as_str())?
            || next.owner_generation != before.owner_generation
            || next.definition != before.definition
            || next.status != ExecutionStatus::Running
            || next.batch_index != before.batch_index
            || next.values != before.values
            || next.attempt_history != before.attempt_history
            || next.attempts_used != before.attempts_used
            || next.updated_at_ms != acceptance.facts.now_ms
            || next.output != before.output
            || next.failure != before.failure
            || next.turn_history.len() != history + 1
            || next.turn_history[..history] != before.turn_history[..]
            || next.turn_history.last() != Some(&acceptance.turn_id)
            || next_task.spec != task.spec
            || next_task.status != TaskStatus::Ready
            || !next_task.approved
            || next_task.input != task.input
            || next_task.attempt_id != task.attempt_id
            || next_task.result != task.result
            || next_task.failure != task.failure
            || next_task.interrupt != task.interrupt
        {
            return Ok(false);
        }
        if next.attempts_used >= next.definition.max_attempts {
            return Ok(plan.dispatchable.is_empty());
        }
        Ok(plan.dispatchable.len() == 1
            && work.same(plan.dispatchable[0].as_str(), task.spec.id.as_str())?)
    }

    fn same_journal(
        journal: &JournalEntry,
        snapshot: &Snapshot,
        work: &mut CoreWork,
    ) -> Result<bool, PermissionError> {
        Ok(work.same(
            journal.execution_id.as_str(),
            snapshot.execution_id.as_str(),
        )? && journal.revision == snapshot.revision
            && journal.owner_generation == snapshot.owner_generation
            && journal.status == snapshot.status
            && journal.updated_at_ms == snapshot.updated_at_ms)
    }
}

// These units exercise the private pair workspace and proposed-data graph.
// Durable permission writing requires its own paid lifecycle.
#[cfg(test)]
mod pair_graph_proposal_tests {
    use super::{
        encode_checked_acceptance_pair,
        prospective_graph::{validate_permission_prospective_graph, PermissionProspectiveGraph},
        *,
    };
    use crate::approval::*;
    use crate::permissions::*;
    use crate::*;
    use b4_contract::*;
    use serde_json::{json, Map, Value};
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const EXPECTED_BYTES: usize = 8_192;
    const EXPECTED_NODES: usize = 512;
    const EXPECTED_DEPTH: usize = 16;
    const SHARED_PAIR_BYTES: usize = 107_222;
    const REPEATED_PAIR_BYTES: usize = 639_702;
    const PAIR_NODES: usize = 1_424;
    const ORIGINAL_PROPOSAL: &str =
        "1f310be924370065c3a18764729b40e9b0f27789e948c20b99ede89f8ff41b18";
    type Pair = (Vec<u8>, Vec<u8>);

    struct Allow;
    impl ApprovalAuthority for Allow {
        fn authorize(
            &self,
            _: &ApprovalContext,
            _: ApprovalOperation,
            _: &ApprovalResource,
        ) -> Result<(), ApprovalError> {
            Ok(())
        }
    }
    struct CountConstraint(Arc<AtomicUsize>);
    impl ToolConstraint for CountConstraint {
        fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct CountNative(Arc<AtomicUsize>);
    impl NativeEffect for CountNative {
        fn invoke(
            &self,
            _: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(NativeOutcome::Succeeded(TaskResult {
                output: json!({"released": true}),
                writes: vec![],
            }))
        }
    }
    fn facts(now_ms: u64) -> TransitionFacts {
        TransitionFacts { now_ms }
    }
    fn actual_fence(store: &MemoryStore, id: &ExecutionId) -> ApprovalFence {
        let view = store.approval_view(id).unwrap().unwrap();
        ApprovalFence {
            execution: (&view.execution.snapshot).into(),
            resource_revision: view.thread.resource_revision,
        }
    }

    // Independent canonical expectation for only the finite known fixture.
    // Arbitrary strings, signed/floating numbers and excessive input refuse.
    fn expected_string(value: &str, out: &mut Vec<u8>) {
        assert!(
            value.len() <= 256
                && value
                    .bytes()
                    .all(|byte| { (0x20..=0x7e).contains(&byte) && byte != b'"' && byte != b'\\' }),
            "known fixture strings need no JSON escape"
        );
        expected_append(out, b"\"");
        expected_append(out, value.as_bytes());
        expected_append(out, b"\"");
    }
    fn expected_append(out: &mut Vec<u8>, bytes: &[u8]) {
        assert!(
            out.len()
                .checked_add(bytes.len())
                .is_some_and(|len| len <= EXPECTED_BYTES),
            "known canonical expectation has a fixed byte bound"
        );
        out.extend_from_slice(bytes);
    }
    fn expected_walk(value: &Value, depth: usize, nodes: &mut usize, out: &mut Vec<u8>) {
        *nodes = nodes.checked_add(1).unwrap();
        assert!(
            *nodes <= EXPECTED_NODES,
            "known canonical expectation has a fixed node bound"
        );
        match value {
            Value::Null => expected_append(out, b"null"),
            Value::Bool(true) => expected_append(out, b"true"),
            Value::Bool(false) => expected_append(out, b"false"),
            Value::Number(value) => {
                let number = value
                    .as_u64()
                    .expect("known fixture numbers are unsigned integers");
                expected_append(out, number.to_string().as_bytes());
            }
            Value::String(value) => expected_string(value, out),
            Value::Array(values) => {
                assert!(
                    depth < EXPECTED_DEPTH && values.len() <= 16,
                    "known fixture arrays have fixed depth and width"
                );
                expected_append(out, b"[");
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        expected_append(out, b",");
                    }
                    expected_walk(value, depth + 1, nodes, out);
                }
                expected_append(out, b"]");
            }
            Value::Object(values) => {
                assert!(
                    depth < EXPECTED_DEPTH && values.len() <= 16,
                    "known fixture objects have fixed depth and width"
                );
                let keys: BTreeMap<_, _> = values.iter().collect();
                expected_append(out, b"{");
                for (index, (key, value)) in keys.iter().enumerate() {
                    if index != 0 {
                        expected_append(out, b",");
                    }
                    *nodes = nodes.checked_add(1).unwrap();
                    assert!(*nodes <= EXPECTED_NODES, "known keys count once");
                    expected_string(key, out);
                    expected_append(out, b":");
                    expected_walk(value, depth + 1, nodes, out);
                }
                expected_append(out, b"}");
            }
        }
    }
    fn expected(value: &Value) -> (Vec<u8>, usize) {
        let mut out = Vec::new();
        let mut nodes = 0;
        expected_walk(value, 0, &mut nodes, &mut out);
        (out, nodes)
    }
    fn independent_digest(value: &Value) -> String {
        let (bytes, _) = expected(value);
        format!("{:x}", Sha256::digest(bytes))
    }
    fn pair_code(result: &Option<Result<Pair, PermissionError>>) -> &'static str {
        match result {
            Some(Ok(_)) => "ok",
            Some(Err(error)) => error.code(),
            None => "missing",
        }
    }
    fn graph_code(result: &Option<Result<(), PermissionError>>) -> &'static str {
        match result {
            Some(Ok(())) => "ok",
            Some(Err(error)) => error.code(),
            None => "missing",
        }
    }

    struct Captured {
        before: ApprovalView,
        after: ApprovalView,
        before_thread: ThreadView,
        after_thread: ThreadView,
        before_journal: Vec<JournalEntry>,
        after_journal: Vec<JournalEntry>,
        pending: Vec<SensitivePendingApproval>,
        request: ApprovalAcceptanceRequest,
        plan: CommitPlan,
        consumed_grant: GrantRecord,
        park_journal: JournalEntry,
        resume_journal: JournalEntry,
        strict_before: bool,
        strict_plan: bool,
        legal_commit: Result<bool, StoreError>,
        gate: Result<(), PermissionError>,
        acceptance: Result<PermissionAcceptance, PermissionError>,
        decision: Result<PermissionDecision, PermissionError>,
        wrong_decision: Result<PermissionDecision, PermissionError>,
        expected_acceptance: Vec<u8>,
        expected_decision: Vec<u8>,
        geometry: [(usize, usize); 3],
        public_pair: Option<Result<Pair, PermissionError>>,
        public_usage: PermissionEvidenceUsage,
        pair: Option<Result<Pair, PermissionError>>,
        pair_usage: PermissionEvidenceUsage,
        positive_graph: Option<Result<(), PermissionError>>,
        positive_graph_usage: PermissionEvidenceUsage,
        wrong_graph: Option<Result<(), PermissionError>>,
        wrong_graph_usage: PermissionEvidenceUsage,
        call_digest: Result<String, PermissionError>,
        expected_call_digest: String,
        expected_scope_digest: String,
        constraint_count: usize,
    }

    fn capture_after_normal_memory_retirement() -> Captured {
        let native_calls = Arc::new(AtomicUsize::new(0));
        let constraint_calls = Arc::new(AtomicUsize::new(0));
        let context = ApprovalContext {
            actor: ThreadOrigin {
                application: "permission-app".into(),
                tenant: "tenant".into(),
                principal: "alice".into(),
            },
            policy_revision: "policy-1".into(),
        };
        let thread_id = ThreadId::new("permission-thread").unwrap();
        let execution_id = ExecutionId::new("permission-execution").unwrap();
        let definition = Definition {
            route: RouteKey::new("/permission-first#agent").unwrap(),
            revision: b4_contract::ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![b4_contract::TaskSpec {
                id: TaskId::new("release").unwrap(),
                effect_id: EffectId::new("release-effect").unwrap(),
                recovery: b4_contract::RecoveryPolicy::Pure,
                requires_approval: true,
            }]],
            max_attempts: 3,
        };
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: context.actor.application.clone(),
                    revision: "application-1".into(),
                },
                vec![RouteRegistration {
                    definition: definition.clone(),
                    effects: BTreeMap::from([(
                        definition.batches[0][0].effect_id.clone(),
                        EffectBinding {
                            callback: Arc::new(CountNative(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    )]),
                    input_validator: None,
                }],
            )
            .unwrap(),
        );
        let registered = application.definition(&definition.route).unwrap().clone();
        assert!(
            registered == definition,
            "real native registration supplies the full Definition"
        );
        let constraint = Arc::new(CountConstraint(constraint_calls.clone()));
        let catalog = PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: constraint.clone(),
            }],
        )
        .unwrap();
        let store = Arc::new(MemoryStore::new());
        assert!(
            !store.approval_durable(),
            "the genuine Memory fixture is explicitly volatile"
        );
        let thread = store
            .create_thread_with(ThreadSeed {
                id: thread_id.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(context.actor.clone()),
            })
            .unwrap();
        let initial = b4_kernel::new_execution(
            execution_id.clone(),
            TurnId::new("original-turn").unwrap(),
            store.owner_generation(),
            registered,
            Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
            facts(100),
        )
        .unwrap();
        let admitted = store.admit(&thread_id, initial).unwrap();
        let started = store
            .commit(
                &execution_id,
                (&admitted.snapshot).into(),
                b4_kernel::transition(&admitted.snapshot, KernelCommand::Start, facts(101))
                    .unwrap(),
            )
            .unwrap();
        let authority = Arc::new(Allow);
        let service = ApprovalService::with_config(
            store.clone(),
            authority.clone(),
            ApprovalServiceConfig::VolatileFixture,
        )
        .unwrap();
        let parked = service
            .park(
                &context,
                &execution_id,
                actual_fence(store.as_ref(), &execution_id),
                definition.batches[0][0].id.clone(),
                InterruptId::new("release-interrupt").unwrap(),
                json!({"question": "Release fixture?", "arguments": {"nullable": null}}),
                1_000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(
                &context,
                &execution_id,
                actual_fence(store.as_ref(), &execution_id),
            )
            .unwrap();
        let before = store.approval_view(&execution_id).unwrap().unwrap();
        let before_thread = store.thread_view(&thread_id).unwrap().unwrap();
        let before_journal = store.journal(&execution_id).unwrap();
        // Fixed-size geometry precedes conversions into untrusted proposal JSON.
        assert!(
            before.execution == parked
                && before.execution.snapshot.tasks.len() == 1
                && before.grants.len() == 1
                && before.acceptances.is_empty()
                && pending.len() == 1
                && before_journal.len() == 3,
            "actual public Park has the fixed one-task source shape"
        );
        let snapshot = &before.execution.snapshot;
        let task = &snapshot.tasks[0];
        let grant = &before.grants[0];
        let strict_before = b4_kernel::validate_snapshot(snapshot).is_ok();
        let invocation = InvocationContext {
            thread_id: thread_id.clone(),
            thread_generation: before.thread.generation.clone(),
            origin: context.actor.clone(),
            application_revision: catalog.application_identity().revision.clone(),
            execution_revision: definition.revision.clone(),
            policy_revision: context.policy_revision.clone(),
        };
        let gate = catalog.validate_call(
            &invocation,
            &definition,
            task,
            PermissionCallPhase::Unclaimed,
        );
        let request = ApprovalAcceptanceRequest {
            context: context.clone(),
            route: definition.route.clone(),
            definition_revision: definition.revision.clone(),
            turn_id: TurnId::new("approved-turn").unwrap(),
            now_ms: 103,
            decisions: vec![GrantDecision {
                interrupt_id: pending[0].interrupt_id.clone(),
                decision: ApprovalDecision::Always,
                cancelled: false,
                token: Some(pending[0].token.clone()),
            }],
        };
        let plan = b4_kernel::transition(
            snapshot,
            KernelCommand::Resume {
                route: request.route.clone(),
                decisions: vec![b4_contract::Decision {
                    interrupt_id: pending[0].interrupt_id.clone(),
                    decision: ApprovalDecision::Always,
                }],
                turn_id: request.turn_id.clone(),
            },
            facts(request.now_ms),
        )
        .unwrap();
        let strict_plan = b4_kernel::validate_snapshot(&plan.snapshot).is_ok();
        let legal_commit =
            validate_commit(snapshot, snapshot.into(), &plan, store.owner_generation());
        // Neither this actual pure Resume nor the proposal grant is committed.
        let mut consumed_grant = grant.clone();
        consumed_grant.consumed = true;
        let park_journal = before_journal[2].clone();
        let resume_journal = JournalEntry::from(&plan.snapshot);
        let snapshot_value = serde_json::to_value(snapshot).unwrap();
        let snapshot_geometry = expected(&snapshot_value);
        let scope = json!({
            "thread_id": before.thread.id,
            "thread_generation": before.thread.generation,
            "origin": context.actor,
            "application": {"application": application.identity().application,
                "revision": application.identity().revision},
            "route": definition.route,
            "definition": definition,
            "effect_id": task.spec.effect_id,
            "tool_id": "release-tool",
            "tool_revision": "tool-1",
            "policy_revision": context.policy_revision,
            "constraint_revision": "constraint-1",
        });
        let expected_scope_digest = independent_digest(&json!({"scope": scope, "version": 1}));
        let permission_id = format!("b4pd_{expected_scope_digest}");
        let legacy_proposal = json!({
            "input": task.input, "prompt": grant.prompt, "task": task.spec, "version": 1,
        });
        let proposal_digest = independent_digest(&legacy_proposal);
        let expected_call_digest = independent_digest(&json!({
            "input": task.input, "task": task.spec, "version": 1,
        }));
        let acceptance_value = json!({
            "record_version": 1,
            "thread_id": before.thread.id,
            "thread_generation": before.thread.generation,
            "origin": context.actor,
            "application": {"application": application.identity().application,
                "revision": application.identity().revision},
            "policy_revision": context.policy_revision,
            "before_snapshot": snapshot_value,
            "turn_id": request.turn_id,
            "facts": facts(request.now_ms),
            "committed_revision": plan.snapshot.revision,
            "decisions": [{
                "task_id": task.spec.id,
                "interrupt_id": grant.interrupt_id,
                "grant_ref": {"execution_id": grant.execution_id,
                    "interrupt_id": grant.interrupt_id},
                "decision": "always",
                "proposal_digest": proposal_digest,
                "task_parking_revision": grant.task_parking_revision,
                "binding": {"effect_id": task.spec.effect_id, "tool_id": "release-tool",
                    "tool_revision": "tool-1", "constraint_revision": "constraint-1"},
                "permission_id": permission_id,
            }],
        });
        let decision_value = json!({
            "record_version": 1,
            "permission_id": permission_id,
            "scope_digest": expected_scope_digest,
            "scope": scope,
            "creator_execution_id": snapshot.execution_id,
            "creator_turn_id": request.turn_id,
            "creator_task_id": task.spec.id,
            "creator_interrupt_id": grant.interrupt_id,
        });
        let mut wrong_decision_value = decision_value.clone();
        wrong_decision_value.as_object_mut().unwrap().insert(
            "creator_turn_id".into(),
            json!(TurnId::new("creator-wrong").unwrap()),
        );
        let (expected_acceptance, acceptance_nodes) = expected(&acceptance_value);
        let (expected_decision, decision_nodes) = expected(&decision_value);
        let (wrong_bytes, wrong_nodes) = expected(&wrong_decision_value);
        assert!(
            wrong_bytes.len() == expected_decision.len() && wrong_nodes == decision_nodes,
            "the sole creator-turn mutant preserves fixed intrinsic geometry"
        );
        let geometry = [
            (snapshot_geometry.0.len(), snapshot_geometry.1),
            (expected_acceptance.len(), acceptance_nodes),
            (expected_decision.len(), decision_nodes),
        ];
        let acceptance = {
            let mut budget = PermissionEvidenceBudget::new();
            PermissionAcceptance::decode_record(&expected_acceptance, &mut budget)
        };
        let decision = {
            let mut budget = PermissionEvidenceBudget::new();
            PermissionDecision::decode_record(&expected_decision, &mut budget)
        };
        let wrong_decision = {
            let mut budget = PermissionEvidenceBudget::new();
            PermissionDecision::decode_record(&wrong_bytes, &mut budget)
        };
        let call_digest = {
            let mut budget = PermissionEvidenceBudget::new();
            permission_call_digest(task, &mut budget)
        };
        let mut public_budget = PermissionEvidenceBudget::new();
        let mut pair_budget = PermissionEvidenceBudget::new();
        let mut positive_graph_budget = PermissionEvidenceBudget::new();
        let mut wrong_graph_budget = PermissionEvidenceBudget::new();
        let (public_pair, pair, positive_graph, wrong_graph) =
            match (&acceptance, &decision, &wrong_decision) {
                (Ok(acceptance), Ok(decision), Ok(wrong_decision)) => {
                    let public_pair = (|| {
                        Ok((
                            acceptance.encode_record(&mut public_budget)?,
                            decision.encode_record(&mut public_budget)?,
                        ))
                    })();
                    let pair =
                        encode_checked_acceptance_pair(acceptance, decision, &mut pair_budget);
                    let positive_graph = validate_permission_prospective_graph(
                        PermissionProspectiveGraph {
                            before: &before,
                            acceptance,
                            decision,
                            consumed_grant: &consumed_grant,
                            resume_plan: &plan,
                            park_journal: &park_journal,
                            resume_journal: &resume_journal,
                        },
                        &mut positive_graph_budget,
                    );
                    let wrong_graph = validate_permission_prospective_graph(
                        PermissionProspectiveGraph {
                            before: &before,
                            acceptance,
                            decision: wrong_decision,
                            consumed_grant: &consumed_grant,
                            resume_plan: &plan,
                            park_journal: &park_journal,
                            resume_journal: &resume_journal,
                        },
                        &mut wrong_graph_budget,
                    );
                    (
                        Some(public_pair),
                        Some(pair),
                        Some(positive_graph),
                        Some(wrong_graph),
                    )
                }
                _ => (None, None, None, None),
            };
        let public_usage = public_budget.usage();
        let pair_usage = pair_budget.usage();
        let positive_graph_usage = positive_graph_budget.usage();
        let wrong_graph_usage = wrong_graph_budget.usage();
        let after = store.approval_view(&execution_id).unwrap().unwrap();
        let after_thread = store.thread_view(&thread_id).unwrap().unwrap();
        let after_journal = store.journal(&execution_id).unwrap();
        store.readiness().unwrap();
        drop(service);
        drop(authority);
        drop(parked);
        drop(started);
        drop(admitted);
        drop(thread);
        drop(catalog);
        drop(constraint);
        drop(application);
        assert!(
            Arc::strong_count(&store) == 1,
            "all actual Memory aliases retire normally"
        );
        drop(store);
        let native_count = native_calls.load(Ordering::SeqCst);
        let constraint_count = constraint_calls.load(Ordering::SeqCst);
        drop(native_calls);
        drop(constraint_calls);
        assert!(
            native_count == 0,
            "pure proposals invoke no native callback after normal close"
        );
        Captured {
            before,
            after,
            before_thread,
            after_thread,
            before_journal,
            after_journal,
            pending,
            request,
            plan,
            consumed_grant,
            park_journal,
            resume_journal,
            strict_before,
            strict_plan,
            legal_commit,
            gate,
            acceptance,
            decision,
            wrong_decision,
            expected_acceptance,
            expected_decision,
            geometry,
            public_pair,
            public_usage,
            pair,
            pair_usage,
            positive_graph,
            positive_graph_usage,
            wrong_graph,
            wrong_graph_usage,
            call_digest,
            expected_call_digest,
            expected_scope_digest,
            constraint_count,
        }
    }

    // Only inherited/genuine setup controls occur here. Each parent calls this
    // after the helper has disposed of all actual Memory aliases normally.
    fn assert_genuine_controls(value: &Captured) {
        assert!(
            value.before == value.after
                && value.before_thread == value.after_thread
                && value.before_journal == value.after_journal,
            "unpersisted pure proposals preserve the complete actual parked Memory state"
        );
        assert!(value.strict_before && value.strict_plan
            && matches!(value.legal_commit, Ok(true)) && value.gate.is_ok()
            && value.constraint_count == 1,
            "actual Park and real Kernel Resume are strict legal proposals with a permitting catalog");
        let snapshot = &value.before.execution.snapshot;
        let task = &snapshot.tasks[0];
        let grant = &value.before.grants[0];
        let pending = &value.pending[0];
        assert!(
            snapshot.status == b4_contract::ExecutionStatus::WaitingForApproval
                && snapshot.revision == 2
                && snapshot.updated_at_ms == 102
                && snapshot.owner_generation == 1
                && value.before.thread.resource_revision == 3
                && task.status == b4_contract::TaskStatus::WaitingForApproval
                && !task.approved
                && task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
                && task.input == snapshot.values
                && task.spec.requires_approval,
            "the actual stored witness retains its complete required waiting task"
        );
        assert!(grant.thread_id == value.before.thread.id
            && grant.thread_generation == value.before.thread.generation
            && grant.origin == value.request.context.actor
            && grant.execution_id == snapshot.execution_id && grant.route == snapshot.definition.route
            && grant.definition_revision == snapshot.definition.revision
            && grant.task_id == task.spec.id && grant.effect_id == task.spec.effect_id
            && grant.interrupt_id == pending.interrupt_id
            && grant.interrupt_id == task.interrupt.as_ref().unwrap().id
            && grant.prompt == pending.prompt && grant.prompt == task.interrupt.as_ref().unwrap().prompt
            && grant.token == pending.token && grant.token.as_str().len() == 69
            && grant.verifier == token_verifier(&pending.token)
            && grant.resume_key == pending.resume_key && grant.resume_key.len() == 64
            && grant.policy_revision == value.request.context.policy_revision
            && grant.task_parking_revision == 2 && !grant.consumed
            && grant.issued_at_ms == 102 && grant.expires_at_ms == 1_102
            && grant.proposal_digest == ORIGINAL_PROPOSAL
            && grant.proposal_digest == proposal_digest(task, &grant.prompt).unwrap()
            && pending.task_id == task.spec.id && pending.expires_at_ms == grant.expires_at_ms,
            "all genuine grant, token, prompt, proposal and incarnation facts survive normal disposal");
        assert!(value.plan.expected_revision == snapshot.revision
            && value.plan.snapshot.execution_id == snapshot.execution_id
            && value.plan.snapshot.definition == snapshot.definition
            && value.plan.snapshot.status == b4_contract::ExecutionStatus::Running
            && value.plan.snapshot.revision == 3 && value.plan.snapshot.updated_at_ms == 103
            && value.plan.snapshot.turn_id == value.request.turn_id
            && value.plan.snapshot.turn_history == vec![snapshot.turn_id.clone(), value.request.turn_id.clone()]
            && value.plan.snapshot.tasks.len() == 1
            && value.plan.snapshot.tasks[0].status == b4_contract::TaskStatus::Ready
            && value.plan.snapshot.tasks[0].approved
            && value.plan.snapshot.tasks[0].input == task.input
            && value.plan.snapshot.tasks[0].interrupt == task.interrupt
            && value.plan.dispatchable == vec![task.spec.id.clone()]
            && value.park_journal == JournalEntry::from(snapshot)
            && value.resume_journal == JournalEntry::from(&value.plan.snapshot),
            "the retained real Resume plan and both journal tuples are actual pure transition evidence");
        let mut expected_consumed = grant.clone();
        expected_consumed.consumed = true;
        assert!(
            value.consumed_grant == expected_consumed,
            "the explicitly unpersisted consumed proposal changes only the genuine grant flag"
        );
        assert!(
            value.geometry == [(848, 91), (1_766, 154), (998, 67)],
            "exact original fixture geometry precedes the fixed allocation primary"
        );
        assert!(
            value.acceptance.is_ok() && value.decision.is_ok() && value.wrong_decision.is_ok(),
            "installed opaque record decoders accept the genuine and sole-link-mutant proposals"
        );
        assert!(
            value
                .call_digest
                .as_ref()
                .is_ok_and(|hash| hash == &value.expected_call_digest),
            "the actual retained task agrees with the independent known-small call formula"
        );
        let acceptance = value.acceptance.as_ref().unwrap();
        let decision = value.decision.as_ref().unwrap();
        let wrong = value.wrong_decision.as_ref().unwrap();
        assert!(
            acceptance.before_snapshot() == snapshot
                && acceptance.committed_revision() == value.plan.snapshot.revision
                && acceptance.turn_id() == &value.request.turn_id
                && decision.scope_digest() == value.expected_scope_digest
                && decision.creator_execution_id() == &snapshot.execution_id
                && decision.creator_turn_id() == &value.request.turn_id
                && decision.creator_task_id() == &task.spec.id
                && decision.creator_interrupt_id() == &grant.interrupt_id,
            "decoded opaque proposals retain the actual creator and before-state facts"
        );
        assert!(
            wrong.record_version() == decision.record_version()
                && wrong.permission_id() == decision.permission_id()
                && wrong.scope_digest() == decision.scope_digest()
                && wrong.scope() == decision.scope()
                && wrong.creator_execution_id() == decision.creator_execution_id()
                && wrong.creator_task_id() == decision.creator_task_id()
                && wrong.creator_interrupt_id() == decision.creator_interrupt_id()
                && wrong.creator_turn_id() == &TurnId::new("creator-wrong").unwrap()
                && wrong.creator_turn_id() != decision.creator_turn_id(),
            "the installed codec accepts precisely one different creator-turn link"
        );
        let public = value.public_pair.as_ref().unwrap().as_ref().unwrap();
        assert!(
            public.0 == value.expected_acceptance && public.1 == value.expected_decision,
            "installed public encoders agree with independent fixed canonical bytes"
        );
        assert!(
            value.public_usage.encoded_bytes() > 0
                && value.public_usage.encoded_bytes() <= 8_388_608
                && value.public_usage.structural_nodes() > 0
                && value.public_usage.structural_nodes() <= 65_536,
            "the genuine public encoder control is finite on its own isolated account"
        );
    }

    #[test]
    fn pair_workspace_reuses_one_physical_stack_without_omitting_paid_passes() {
        let value = capture_after_normal_memory_retirement();
        assert_genuine_controls(&value);
        assert!(
            matches!(&value.pair, Some(Ok(pair))
            if pair.0 == value.expected_acceptance && pair.1 == value.expected_decision),
            "compatibility pair preserves public canonical bytes: code={}",
            pair_code(&value.pair)
        );
        // The private pair reuses one workspace while the public control
        // retains every allocating pass and its original accounting.
        assert!(
            value.pair_usage.encoded_bytes() <= SHARED_PAIR_BYTES,
            "shared pair allocation ceiling: bytes={}",
            value.pair_usage.encoded_bytes()
        );
        assert!(
            value.public_usage.encoded_bytes() == REPEATED_PAIR_BYTES
                && value.public_usage.structural_nodes() == PAIR_NODES
                && value.pair_usage.structural_nodes() == PAIR_NODES,
            "all old scalar and structural passes remain fully charged"
        );
    }

    #[test]
    fn prospective_first_always_graph_accepts_genuine_park_resume_proposals() {
        let value = capture_after_normal_memory_retirement();
        assert_genuine_controls(&value);
        // No stored healthy A/D producer is needed for this proposed-data gate.
        assert!(
            matches!(value.positive_graph, Some(Ok(()))),
            "prospective first graph must accept: code={}",
            graph_code(&value.positive_graph)
        );
        assert!(
            value.positive_graph_usage.encoded_bytes() > 0
                && value.positive_graph_usage.structural_nodes() > 0
                && value.positive_graph_usage.encoded_bytes() <= 8_388_608
                && value.positive_graph_usage.structural_nodes() <= 65_536,
            "reached graph checks use one finite actual operation account"
        );
    }

    #[test]
    fn prospective_graph_refuses_codec_valid_wrong_creator_turn() {
        let value = capture_after_normal_memory_retirement();
        assert_genuine_controls(&value);
        // The missing positive graph is never required as negative setup.
        assert!(
            matches!(value.wrong_graph, Some(Err(PermissionError::CorruptRecord))),
            "prospective creator-turn link must refuse: code={}",
            graph_code(&value.wrong_graph)
        );
        assert!(
            value.wrong_graph_usage.encoded_bytes() > 0
                && value.wrong_graph_usage.structural_nodes() > 0
                && value.wrong_graph_usage.encoded_bytes() <= 8_388_608
                && value.wrong_graph_usage.structural_nodes() <= 65_536,
            "reached mutant validation prepays its finite graph work"
        );
    }
}

pub(super) fn quote_permission_catalog_capture(
    budget: &mut PermissionEvidenceBudget,
) -> Result<std::sync::Arc<()>, PermissionError> {
    // V3 term20 first half: bounded capture selection, continuity, Arc clone
    // and transfers. This seal binds containers, never logical authority.
    budget.reserve(16_384, 256)?;
    Ok(std::sync::Arc::clone(
        budget
            .acceptance_identity
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?,
    ))
}
pub(super) fn check_permission_prepared_identity(
    capture: &PermissionAcceptanceCapture,
    identity: &std::sync::Arc<()>,
    budget: &PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    // Specific factory quote was paid before these fixed pointer comparisons.
    let actual = budget
        .acceptance_identity
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    if !std::sync::Arc::ptr_eq(actual, &capture.identity)
        || !std::sync::Arc::ptr_eq(actual, identity)
    {
        return corrupt();
    }
    Ok(())
}
pub(super) fn quote_permission_captured_unclaimed_gate(
    definition: &Definition,
    native: &Definition,
    task: &TaskRecord,
    identity: &ApplicationIdentity,
    entries: &[super::ToolBindingRegistration],
    budget: &mut PermissionEvidenceBudget,
) -> Result<CanonicalMeasure, PermissionError> {
    let result = (|| {
        let provided = measure(
            CanonicalValue::Definition(definition),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        let selected = measure(
            CanonicalValue::Definition(native),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        let spec = measure(
            CanonicalValue::TaskSpec(&task.spec),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        reserve_measure(provided, budget)?;
        let tasks = definition
            .batches
            .iter()
            .map(Vec::len)
            .try_fold(0usize, add)?;
        reserve_measure(provided, budget)?;
        reserve_measure(selected, budget)?;
        // Every TaskSpec pair in the old .any search is paid before the search.
        budget.reserve(
            add(mul(tasks, spec.bytes)?, provided.bytes)?,
            add(mul(tasks, spec.nodes)?, provided.nodes)?,
        )?;
        for entry in entries {
            let entry_definition = measure(
                CanonicalValue::Definition(&entry.definition),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            reserve_measure(provided, budget)?;
            reserve_measure(entry_definition, budget)?;
            // Definition equality short-circuiting does not refund the EffectId pair.
            budget.reserve(
                add(
                    entry.effect_id.as_str().len(),
                    task.spec.effect_id.as_str().len(),
                )?,
                2,
            )?;
            // Conservatively quote every candidate owned binding, not only the
            // eventual selected registration. No clone or refund occurs here.
            budget.reserve(
                add(
                    mul(128, entry_definition.nodes)?,
                    mul(6, entry_definition.bytes)?,
                )?,
                entry_definition.nodes,
            )?;
            reserve_measure(entry_definition, budget)?;
            budget.reserve(4_096, 1)?;
            let labels = add(
                add(identity.application.len(), identity.revision.len())?,
                add(
                    entry.effect_id.as_str().len(),
                    add(
                        entry.tool_id.as_str().len(),
                        add(
                            entry.tool_revision.as_str().len(),
                            entry.constraint_revision.as_str().len(),
                        )?,
                    )?,
                )?,
            )?;
            budget.reserve(add(1_024, mul(6, labels)?)?, 6)?;
        }
        if provided.bytes > 2_048 || selected.bytes > 2_048 || selected.nodes > 32 {
            return Err(PermissionError::UnsupportedProfile);
        }
        Ok(selected)
    })();
    budget.finish(result)
}

fn decode_writer_grant_capture(
    raw: &[u8],
    budget: &mut PermissionEvidenceBudget,
) -> Result<
    (
        crate::approval::GrantRecord,
        CanonicalMeasure,
        CanonicalMeasure,
    ),
    PermissionError,
> {
    preflight_profile(raw, RawProfile::Legacy, budget)?;
    let grant: crate::approval::GrantRecord =
        serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
    let measured = measure(
        CanonicalValue::LegacyGrant(&grant),
        CanonicalLimit::LegacyRecord,
        budget,
    )?;
    reserve_measure(measured, budget)?;
    // Complete finite label, digest, TTL, SHA69 and verifier comparison allowance.
    budget.reserve(32_768, 128)?;
    if !valid_label(&grant.origin.application)
        || !valid_label(&grant.origin.tenant)
        || !valid_label(&grant.origin.principal)
        || !valid_label(&grant.policy_revision)
        || grant.task_parking_revision == 0
        || !digest_shape(&grant.proposal_digest)
        || !digest_shape(&grant.resume_key)
    {
        return corrupt();
    }
    let ttl = grant
        .expires_at_ms
        .checked_sub(grant.issued_at_ms)
        .ok_or(PermissionError::CorruptRecord)?;
    if ttl == 0 || ttl > crate::approval::MAX_APPROVAL_TTL_MS {
        return corrupt();
    }
    let verifier = <sha2::Sha256 as sha2::Digest>::digest(grant.token.as_str().as_bytes());
    let difference = grant
        .verifier
        .iter()
        .zip(verifier.iter())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right));
    if difference != 0 {
        return corrupt();
    }
    // Old prompt depth64 counts scalar leaves as zero. Canonical byte length
    // matches pinned serde without its old full allocation/sort construction.
    let prompt = measure(
        CanonicalValue::Json(&grant.prompt),
        CanonicalLimit::LegacyPrompt,
        budget,
    )?;
    Ok((grant, measured, prompt))
}

// Closed fresh-singleton writer. Public reader/codec paths retain their fees.
#[derive(Clone, Copy)]
pub enum PermissionAcceptanceSql {
    BeginImmediate,
    ParkJournalHeader,
    ParkJournalPayload,
    GlobalRequestedPermission,
    SnapshotCas,
    ThreadCas,
    ConsumeGrant,
    InsertAcceptance,
    InsertDecision,
    InsertResumeJournal,
    Commit,
}
pub struct PermissionParkJournalHeaderCells<'a> {
    pub sequence: i64,
    pub kind: &'a [u8],
    pub thread_id: &'a [u8],
    pub execution_id: &'a [u8],
    pub revision: &'a [u8],
    pub owner_generation: &'a [u8],
    pub status: &'a [u8],
    pub updated_at_ms: &'a [u8],
    pub payload_bytes: usize,
}
pub struct PermissionAcceptanceBindings<'a> {
    pub execution_id: &'a str,
    pub thread_id: &'a str,
    pub interrupt_id: &'a str,
    pub turn_id: &'a str,
    pub permission_id: &'a str,
    pub scope_digest: &'a str,
    pub generation: &'a str,
    pub before_revision: u64,
    pub owner_generation: u64,
    pub before_resource_revision: u64,
    pub next_revision: u64,
    pub next_resource_revision: u64,
    pub updated_at_ms: u64,
    pub snapshot: &'a str,
    pub consumed_grant: &'a str,
    pub acceptance: &'a str,
    pub decision: &'a str,
    pub resume_journal: &'a str,
}
struct PermissionWriteState {
    resource: crate::approval::ApprovalResource,
    snapshot_quote: CanonicalMeasure,
    thread_quote: CanonicalMeasure,
    new_group: usize,
    new_rows: usize,
    new_nonempty: bool,
    grants_done: bool,
    acceptances_done: bool,
    grant_header: Option<GrantHeader>,
    grants: Vec<crate::approval::GrantRecord>,
    grant_quote: Option<CanonicalMeasure>,
    grant_prompt_quote: Option<CanonicalMeasure>,
}
pub struct PermissionAcceptanceWrite<'budget, 'outer> {
    budget: &'budget mut PermissionEvidenceBudget,
    prepared: &'outer super::PermissionPreparedAcceptance<'outer>,
    step: usize,
    metadata_checked: bool,
    current_owner: u64,
    execution: Option<(crate::StoredExecution, CanonicalMeasure)>,
    thread: Option<(crate::ThreadRecord, CanonicalMeasure)>,
    state: Option<PermissionWriteState>,
    park_header: Option<(i64, usize)>,
    park: Option<crate::JournalEntry>,
    park_rows: usize,
    park_done: bool,
    generated: bool,
    pid_absent: bool,
}
// All Work owners precede the short mutable backend admissions. The original
// account loan lifetime prevents replacing/reusing its storage while Work lives.
pub struct PermissionAcceptanceWork<'budget, 'outer> {
    prepared: &'outer super::PermissionPreparedAcceptance<'outer>,
    account: *const PermissionEvidenceBudget,
    _loan: std::marker::PhantomData<&'budget mut PermissionEvidenceBudget>,
    resource: crate::approval::ApprovalResource,
    plan: b4_contract::CommitPlan,
    acceptance: PermissionAcceptance,
    decision: PermissionDecision,
    consumed: crate::approval::GrantRecord,
    next_resource_revision: u64,
    snapshot_json: String,
    grant_json: String,
    acceptance_json: String,
    decision_json: String,
    journal_json: String,
}
impl fmt::Debug for PermissionAcceptanceWrite<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionAcceptanceWrite([REDACTED])")
    }
}
impl fmt::Debug for PermissionAcceptanceWork<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionAcceptanceWork([REDACTED])")
    }
}
const _: () = assert!(std::mem::size_of::<PermissionAcceptanceBindings<'_>>() <= 512);
const _: () = assert!(std::mem::size_of::<PermissionAcceptanceWork<'_, '_>>() <= 3_072);
impl PermissionEvidenceBudget {
    pub fn acceptance_write<'budget, 'outer>(
        &'budget mut self,
        prepared: &'outer super::PermissionPreparedAcceptance<'outer>,
    ) -> Result<PermissionAcceptanceWrite<'budget, 'outer>, PermissionError> {
        self.require_standard_account()?;
        let result = (|| {
            self.reserve(0, 0)?;
            if self.resource.immediate_writes != 0 {
                return self.limit();
            }
            self.reserve(128, 1)?;
            // Pay both inline Session handoffs and early Parts/state/error
            // layout before returning the session, not from later Work credit.
            self.reserve(8_448, 32)?;
            let parts = prepared.writer_parts();
            check_permission_prepared_identity(
                parts
                    .matched
                    .acceptance_capture()
                    .ok_or(PermissionError::CorruptRecord)?,
                &parts.binding.identity,
                self,
            )?;
            self.resource.immediate_writes = 1;
            // This is defensive state, not an account-derived authority proof.
            if self.resource.reads != 2 || self.resource.combined_reads != 2 {
                return Err(PermissionError::UnsupportedProfile);
            }
            Ok(())
        })();
        self.finish(result)?;
        Ok(PermissionAcceptanceWrite {
            budget: self,
            prepared,
            step: 0,
            metadata_checked: false,
            current_owner: 0,
            execution: None,
            thread: None,
            state: None,
            park_header: None,
            park: None,
            park_rows: 0,
            park_done: false,
            generated: false,
            pid_absent: false,
        })
    }
}
impl<'budget, 'outer> PermissionAcceptanceWrite<'budget, 'outer> {
    pub fn execution_id(&self) -> &'outer ExecutionId {
        self.prepared.writer_parts().execution_id
    }
    pub fn thread_id(&self) -> &'outer ThreadId {
        &self.prepared.writer_parts().matched.thread().id
    }
    pub fn parking_revision(&self) -> u64 {
        self.prepared
            .writer_parts()
            .matched
            .execution()
            .snapshot
            .revision
    }
    fn statement(&mut self, step: usize, capacity: usize) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if self.step.checked_add(1) != Some(step) {
                return corrupt();
            }
            let next = add(self.budget.resource.sql_capacity, capacity)?;
            if next > 512 {
                return self.budget.limit();
            }
            self.budget
                .reserve(mul(capacity, 10_240)?, mul(capacity, 8)?)?;
            self.budget.resource.sql_capacity = next;
            self.step = step;
            Ok(())
        })();
        self.budget.finish(result)
    }
    pub fn before_resource_sql(
        &mut self,
        sql: PermissionResourceSql,
        key: Option<PermissionResourceKey<'_>>,
    ) -> Result<(), PermissionError> {
        let step = match sql {
            PermissionResourceSql::Metadata => 2,
            PermissionResourceSql::ExecutionLengths => 3,
            PermissionResourceSql::ExecutionPayload => 4,
            PermissionResourceSql::ThreadLengths => 5,
            PermissionResourceSql::ThreadPayload => 6,
            _ => return corrupt(),
        };
        self.statement(step, 1)?;
        let result = if let Some(key) = key {
            PermissionResourceRead {
                budget: &mut *self.budget,
            }
            .admit_key(key)
        } else {
            corrupt()
        };
        self.budget.finish(result)
    }
    pub fn check_metadata(
        &mut self,
        version: i64,
        checksum: &[u8],
        owner: &[u8],
        expected_owner: u64,
        schema: &[u8],
    ) -> Result<(), PermissionError> {
        if self.step != 2 || self.metadata_checked {
            return corrupt();
        }
        PermissionResourceRead {
            budget: &mut *self.budget,
        }
        .check_metadata(version, checksum, owner, expected_owner, schema)?;
        self.current_owner = expected_owner;
        self.metadata_checked = true;
        Ok(())
    }
    pub fn admit_execution_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead {
            budget: &mut *self.budget,
        }
        .admit_execution_lengths(thread, snapshot)
    }
    pub fn capture_execution(
        &mut self,
        cells: PermissionExecutionCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if !self.metadata_checked || self.step != 4 || self.execution.is_some() {
                return corrupt();
            }
            self.execution = Some(resource_execution_capture(cells, self.budget)?);
            Ok(())
        })();
        self.budget.finish(result).map_err(resource_error)
    }
    pub fn thread_query_id(&mut self) -> Result<ThreadId, PermissionError> {
        let result = (|| {
            let owned = self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            ThreadId::new(resource_string(
                owned.0.thread_id.as_str().as_bytes(),
                self.budget,
            )?)
            .map_err(|_| PermissionError::CorruptRecord)
        })();
        self.budget.finish(result)
    }
    pub fn admit_thread_lengths(
        &mut self,
        id: usize,
        active: usize,
        generation: usize,
        metadata: usize,
        origin: usize,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead {
            budget: &mut *self.budget,
        }
        .admit_thread_lengths(id, active, generation, metadata, origin)
    }
    pub fn capture_thread(
        &mut self,
        cells: PermissionThreadCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if self.step != 6 || self.execution.is_none() || self.thread.is_some() {
                return corrupt();
            }
            self.thread = Some(resource_thread_capture(cells, self.budget)?);
            Ok(())
        })();
        self.budget.finish(result).map_err(resource_error)
    }
    pub fn begin_sensitive(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(512, 8)?;
            if self.step != 6 || self.state.is_some() {
                return corrupt();
            }
            let (execution, snapshot_quote) = self
                .execution
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            let (thread, thread_quote) =
                self.thread.take().ok_or(PermissionError::CorruptRecord)?;
            let parts = self.prepared.writer_parts();
            let prior = parts
                .matched
                .acceptance_capture()
                .ok_or(PermissionError::CorruptRecord)?;
            reserve_measure(snapshot_quote, self.budget)?;
            reserve_measure(thread_quote, self.budget)?;
            reserve_measure(prior.snapshot, self.budget)?;
            reserve_measure(prior.thread, self.budget)?;
            self.budget.reserve(4_096, 32)?;
            if parts.fence.execution.owner_generation != execution.snapshot.owner_generation
                || self.current_owner != execution.snapshot.owner_generation
            {
                return Err(crate::StoreError::OwnerConflict.into());
            }
            if parts.fence.execution.revision != execution.snapshot.revision
                || parts.fence.resource_revision != thread.resource_revision
                || execution != parts.matched.resource().execution
                || thread != parts.matched.resource().thread
            {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            if thread.active_execution_id.as_ref() != Some(parts.execution_id)
                || thread.id != execution.thread_id
            {
                return Err(crate::StoreError::ExecutionNotActive.into());
            }
            // The retained catalogue fact was measured by the actual C1 gate.
            // This is an independent paid pair against actual I1, before rows.
            reserve_measure(parts.binding.definition, self.budget)?;
            reserve_measure(snapshot_quote, self.budget)?;
            if execution.snapshot.definition != parts.binding.registration.definition
                || thread.origin.as_ref() != Some(&parts.request.context.actor)
                || parts.catalog.application_identity().application
                    != parts.request.context.actor.application
            {
                return Err(PermissionError::InvalidCatalog);
            }
            self.state = Some(PermissionWriteState {
                resource: crate::approval::ApprovalResource { thread, execution },
                snapshot_quote,
                thread_quote,
                new_group: 0,
                new_rows: 0,
                new_nonempty: false,
                grants_done: false,
                acceptances_done: false,
                grant_header: None,
                grants: Vec::new(),
                grant_quote: None,
                grant_prompt_quote: None,
            });
            Ok(())
        })();
        self.budget.finish(result)
    }
    pub fn before_view_sql(&mut self, sql: PermissionViewSql) -> Result<(), PermissionError> {
        let result = (|| {
            let step = match sql {
                PermissionViewSql::NewHeaders(PermissionNewGroup::Acceptances) => 7,
                PermissionViewSql::NewHeaders(PermissionNewGroup::Decisions) => 8,
                PermissionViewSql::NewHeaders(PermissionNewGroup::Uses) => 9,
                PermissionViewSql::GrantHeaders => 10,
                PermissionViewSql::GrantPayload => 11,
                PermissionViewSql::AcceptanceHeaders => 12,
                _ => return corrupt(),
            };
            self.statement(step, 1)?;
            let state = self.state.as_ref().ok_or(PermissionError::CorruptRecord)?;
            let id = state
                .resource
                .execution
                .snapshot
                .execution_id
                .as_str()
                .as_bytes();
            match sql {
                PermissionViewSql::NewHeaders(g) => {
                    if group_index(g) != state.new_group {
                        return corrupt();
                    }
                    admit_composite_key(
                        self.budget,
                        10,
                        &[
                            group_tag(g),
                            state.resource.thread.id.as_str().as_bytes(),
                            id,
                        ],
                    )?;
                }
                PermissionViewSql::GrantHeaders => {
                    if state.new_group != 3 || state.new_nonempty {
                        return corrupt();
                    }
                    admit_composite_key(self.budget, 10, &[b"g", id])?;
                }
                PermissionViewSql::GrantPayload => {
                    let h = state
                        .grant_header
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?;
                    admit_composite_key(
                        self.budget,
                        3,
                        &[h.execution.as_bytes(), h.interrupt.as_bytes()],
                    )?;
                }
                PermissionViewSql::AcceptanceHeaders => {
                    if !state.grants_done {
                        return corrupt();
                    }
                    admit_composite_key(self.budget, 10, &[b"a", id])?;
                }
                _ => return corrupt(),
            }
            Ok(())
        })();
        self.budget.finish(result)
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        self.budget.reserve(2_048, 16)
    }
    pub fn admit_new_header(
        &mut self,
        cells: PermissionNewHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let state = self.state.as_mut().ok_or(PermissionError::CorruptRecord)?;
        let budget = &mut *self.budget;
        let result = (|| {
            budget.reserve(4_096, 32)?;
            if group_index(cells.group) != state.new_group {
                return corrupt();
            }
            if state.new_rows == 64 {
                return budget.limit();
            }
            state.new_rows += 1;
            let thread = opaque(cells.thread_id)?;
            let epoch = generation(cells.generation)?;
            if thread != state.resource.thread.id.as_str()
                || epoch != state.resource.thread.generation.as_str()
                || cells.record_version != 1
                || cells.record_bytes == 0
            {
                return corrupt();
            }
            let limit = if cells.group == PermissionNewGroup::Uses {
                16_384
            } else {
                65_536
            };
            if cells.record_bytes > limit {
                return budget.limit();
            }
            match cells.group {
                PermissionNewGroup::Acceptances => {
                    if opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != state.resource.execution.snapshot.execution_id.as_str()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.digest.is_some()
                    {
                        return corrupt();
                    }
                    opaque(cells.primary)?;
                    let previous = resource_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    let committed = resource_u64(
                        cells
                            .committed_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    if previous.checked_add(1) != Some(committed)
                        || committed > state.resource.execution.snapshot.revision
                    {
                        return corrupt();
                    }
                }
                PermissionNewGroup::Decisions => {
                    if cells.execution_id.is_some()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.previous_revision.is_some()
                        || cells.committed_revision.is_some()
                        || !record_identity(cells.primary, b"b4pd_")
                    {
                        return corrupt();
                    }
                    generation(cells.digest.ok_or(PermissionError::CorruptRecord)?)?;
                }
                PermissionNewGroup::Uses => {
                    if opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != state.resource.execution.snapshot.execution_id.as_str()
                        || cells.digest.is_some()
                        || cells.committed_revision.is_some()
                        || !record_identity(
                            cells.secondary.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pu_",
                        )
                        || !record_identity(
                            cells.permission_id.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pd_",
                        )
                    {
                        return corrupt();
                    }
                    opaque(cells.primary)?;
                    opaque(cells.turn_id.ok_or(PermissionError::CorruptRecord)?)?;
                    let authorization = resource_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    if authorization > state.resource.execution.snapshot.revision {
                        return corrupt();
                    }
                }
            }
            state.new_nonempty = true;
            Ok(())
        })();
        budget.finish(result).map_err(view_error)
    }
    pub fn end_new_range(&mut self, group: PermissionNewGroup) -> Result<(), PermissionError> {
        let state = self.state.as_mut().ok_or(PermissionError::CorruptRecord)?;
        let budget = &mut *self.budget;
        let result = (|| {
            budget.reserve(128, 1)?;
            if group_index(group) != state.new_group {
                return corrupt();
            }
            state.new_group += 1;
            state.new_rows = 0;
            if state.new_group == 3 && state.new_nonempty {
                return Err(PermissionError::UnsupportedProfile);
            }
            Ok(())
        })();
        budget.finish(result).map_err(view_error)
    }

    pub fn admit_grant_header(
        &mut self,
        cells: PermissionGrantHeaderCells<'_>,
    ) -> Result<String, PermissionError> {
        let result = (|| {
            self.budget.reserve(8_192, 32)?;
            let state = self.state.as_mut().ok_or(PermissionError::CorruptRecord)?;
            if self.step != 10
                || state.new_group != 3
                || state.new_nonempty
                || state.grant_header.is_some()
                || state.grants_done
            {
                return corrupt();
            }
            if !state.grants.is_empty() {
                return Err(PermissionError::UnsupportedProfile);
            }
            let execution = opaque(cells.execution_id)?;
            let interrupt = opaque(cells.interrupt_id)?;
            let task = opaque(cells.task_id)?;
            let thread = opaque(cells.thread_id)?;
            let epoch = generation(cells.generation)?;
            let parking = resource_u64(cells.parking_revision)?;
            if execution != self.prepared.writer_parts().execution_id.as_str()
                || thread != state.resource.thread.id.as_str()
                || epoch != state.resource.thread.generation.as_str()
                || parking == 0
                || !matches!(cells.consumed, 0 | 1)
                || cells.record_bytes == 0
            {
                return corrupt();
            }
            if cells.record_bytes > 65_536 {
                return self.budget.limit();
            }
            self.budget.reserve(cells.record_bytes, 1)?;
            let query = resource_string(interrupt.as_bytes(), self.budget)?;
            state.grant_header = Some(GrantHeader {
                interrupt: resource_string(interrupt.as_bytes(), self.budget)?,
                task: resource_string(task.as_bytes(), self.budget)?,
                execution: resource_string(execution.as_bytes(), self.budget)?,
                thread: resource_string(thread.as_bytes(), self.budget)?,
                generation: resource_string(epoch.as_bytes(), self.budget)?,
                parking,
                consumed: cells.consumed == 1,
                bytes: cells.record_bytes,
            });
            Ok(query)
        })();
        self.budget.finish(result)
    }
    pub fn decode_grant_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = (|| {
            let state = self.state.as_mut().ok_or(PermissionError::CorruptRecord)?;
            let h = state
                .grant_header
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            if self.step != 11 || raw.len() != h.bytes {
                return corrupt();
            }
            self.budget.reserve(64, 1)?;
            let (g, q, prompt) = decode_writer_grant_capture(raw, self.budget)?;
            self.budget.reserve(8_192, 32)?;
            if g.execution_id.as_str() != h.execution
                || g.interrupt_id.as_str() != h.interrupt
                || g.task_id.as_str() != h.task
                || g.thread_id.as_str() != h.thread
                || g.thread_generation.as_str() != h.generation
                || g.task_parking_revision != h.parking
                || g.consumed != h.consumed
            {
                return corrupt();
            }
            if !state.grants.is_empty() {
                return Err(PermissionError::UnsupportedProfile);
            }
            state.grant_quote = Some(q);
            state.grant_prompt_quote = Some(prompt);
            push_legacy(g, &mut state.grants, 1_024, self.budget)
        })();
        self.budget.finish(result)
    }
    pub fn end_grant_range(&mut self) -> Result<(), PermissionError> {
        self.budget.reserve(128, 1)?;
        let s = self.state.as_mut().ok_or(PermissionError::CorruptRecord)?;
        if s.grants_done || s.grant_header.is_some() || s.grants.len() != 1 || self.step != 11 {
            return Err(PermissionError::UnsupportedProfile);
        }
        s.grants_done = true;
        Ok(())
    }
    pub fn admit_acceptance_header(
        &mut self,
        c: PermissionAcceptanceHeaderCells<'_>,
    ) -> Result<String, PermissionError> {
        self.budget.reserve(8_192, 32)?;
        let s = self.state.as_ref().ok_or(PermissionError::CorruptRecord)?;
        if opaque(c.execution_id)? != s.resource.execution.snapshot.execution_id.as_str()
            || opaque(c.thread_id)? != s.resource.thread.id.as_str()
            || generation(c.generation)? != s.resource.thread.generation.as_str()
            || c.record_bytes == 0
        {
            return corrupt();
        }
        opaque(c.turn_id)?;
        opaque(c.definition_revision)?;
        resource_u64(c.previous_revision)?;
        resource_u64(c.accepted_at_ms)?;
        if c.record_bytes > 65_536 {
            return self.budget.limit();
        }
        Err(PermissionError::UnsupportedProfile)
    }
    pub fn decode_acceptance_payload(&mut self, _raw: &[u8]) -> Result<(), PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    pub fn end_acceptance_range(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(128, 1)?;
            let s = self.state.as_mut().ok_or(PermissionError::CorruptRecord)?;
            let prior = self.prepared.writer_parts().matched;
            let g = s.grant_quote.ok_or(PermissionError::CorruptRecord)?;
            let p = prior
                .acceptance_capture()
                .ok_or(PermissionError::CorruptRecord)?;
            self.budget.reserve(
                add(g.bytes, p.records.bytes)?,
                add(g.nodes, p.records.nodes)?,
            )?;
            self.budget.reserve(512, 32)?;
            if self.step != 12
                || !s.grants_done
                || s.acceptances_done
                || s.grants.as_slice() != prior.grants()
                || !prior.acceptances().is_empty()
                || !prior.permission_acceptances().is_empty()
                || !prior.permission_decisions().is_empty()
                || !prior.permission_uses().is_empty()
                || !prior.creator_acceptances().is_empty()
                || !prior.referenced_grants().is_empty()
                || !prior.referenced_acceptances().is_empty()
                || !prior.journal_entries().is_empty()
                || prior.receipt().purpose() != super::PermissionReadPurpose::Accept
            {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            s.acceptances_done = true;
            Ok(())
        })();
        self.budget.finish(result)
    }
    pub fn admit_park_journal_header(
        &mut self,
        c: PermissionParkJournalHeaderCells<'_>,
    ) -> Result<(i64, usize), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if self.step != 13 || self.park_rows != 0 || self.park_header.is_some() {
                return corrupt();
            }
            if c.payload_bytes > 1_024 {
                return self.budget.limit();
            }
            let s = self.state.as_ref().ok_or(PermissionError::CorruptRecord)?;
            let before = &s.resource.execution.snapshot;
            if c.sequence <= 0
                || c.kind != b"commit"
                || opaque(c.thread_id)? != s.resource.thread.id.as_str()
                || opaque(c.execution_id)? != before.execution_id.as_str()
                || resource_u64(c.revision)? != before.revision
                || resource_u64(c.owner_generation)? != before.owner_generation
                || c.status != b"\"waiting_for_approval\""
                || resource_u64(c.updated_at_ms)? != before.updated_at_ms
                || c.payload_bytes == 0
            {
                return corrupt();
            }
            self.park_rows = 1;
            self.park_header = Some((c.sequence, c.payload_bytes));
            Ok((c.sequence, c.payload_bytes))
        })();
        self.budget.finish(result)
    }
    pub fn end_park_journal_range(&mut self) -> Result<(), PermissionError> {
        self.budget.reserve(128, 1)?;
        if self.step != 13 || self.park_rows != 1 || self.park_done {
            return corrupt();
        }
        self.park_done = true;
        Ok(())
    }
    pub fn capture_park_journal_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            let (_, n) = self.park_header.ok_or(PermissionError::CorruptRecord)?;
            if self.step != 14 || !self.park_done || self.park.is_some() || raw.len() != n {
                return corrupt();
            }
            // Closed typed five-field decoding (no Value): raw<=1024, denied
            // unknown/duplicate fields. The fixed J quote was paid before header.
            let j: crate::JournalEntry =
                serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
            let b = &self
                .state
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .resource
                .execution
                .snapshot;
            if j.execution_id != b.execution_id
                || j.revision != b.revision
                || j.owner_generation != b.owner_generation
                || j.status != b.status
                || j.updated_at_ms != b.updated_at_ms
            {
                return corrupt();
            }
            self.park = Some(j);
            Ok(())
        })();
        self.budget.finish(result)
    }
    pub fn before_write_sql<'key>(
        &mut self,
        sql: PermissionAcceptanceSql,
        work: Option<&'key PermissionAcceptanceWork<'budget, 'outer>>,
    ) -> Result<Option<PermissionAcceptanceBindings<'key>>, PermissionError> {
        let result = (|| {
            let step = match sql {
                PermissionAcceptanceSql::BeginImmediate => 1,
                PermissionAcceptanceSql::ParkJournalHeader => 13,
                PermissionAcceptanceSql::ParkJournalPayload => 14,
                PermissionAcceptanceSql::GlobalRequestedPermission => 15,
                PermissionAcceptanceSql::SnapshotCas => 16,
                PermissionAcceptanceSql::ThreadCas => 17,
                PermissionAcceptanceSql::ConsumeGrant => 18,
                PermissionAcceptanceSql::InsertAcceptance => 19,
                PermissionAcceptanceSql::InsertDecision => 20,
                PermissionAcceptanceSql::InsertResumeJournal => 21,
                PermissionAcceptanceSql::Commit => 22,
            };
            self.statement(step, if step == 1 { 2 } else { 1 })?;
            if step == 13 {
                let s = self.state.as_ref().ok_or(PermissionError::CorruptRecord)?;
                if !s.acceptances_done {
                    return corrupt();
                }
                // Entire closed Park/Resume lifecycle, scalar JSON scratch and
                // checks are prepaid before the first J cell/decoder is invoked.
                self.budget.reserve(65_536, 512)?;
                let execution = self.execution_id();
                let revision = self.parking_revision().to_be_bytes();
                admit_composite_key(self.budget, 11, &[execution.as_str().as_bytes(), &revision])?;
            } else if step == 14 {
                let (sequence, _) = self.park_header.ok_or(PermissionError::CorruptRecord)?;
                admit_composite_key(self.budget, 11, &[&sequence.to_be_bytes()])?;
            }
            if step < 15 {
                if work.is_some() {
                    return corrupt();
                }
                return Ok(None);
            }
            let w = work.ok_or(PermissionError::CorruptRecord)?;
            if !self.generated
                || !std::ptr::eq(w.account, self.budget as *const PermissionEvidenceBudget)
                || !std::ptr::eq(w.prepared, self.prepared)
            {
                return corrupt();
            }
            if step >= 16 && !self.pid_absent {
                return corrupt();
            }
            let b = w.bindings();
            let tag = match step {
                15 | 20 => 12,
                16 => 1,
                17 => 2,
                18 => 3,
                19 => 13,
                21 => 11,
                _ => 0,
            };
            if step == 16 {
                PermissionResourceRead {
                    budget: &mut *self.budget,
                }
                .admit_key(PermissionResourceKey::Execution(
                    &w.resource.execution.snapshot.execution_id,
                ))?;
            } else if step == 17 {
                PermissionResourceRead {
                    budget: &mut *self.budget,
                }
                .admit_key(PermissionResourceKey::Thread(&w.resource.thread.id))?;
            } else if step != 22 {
                // Fixed <=2 borrowed slots, paid before these key operations.
                match step {
                    15 | 20 => {
                        admit_composite_key(self.budget, tag, &[b.permission_id.as_bytes()])?
                    }
                    18 => admit_composite_key(
                        self.budget,
                        tag,
                        &[b.execution_id.as_bytes(), b.interrupt_id.as_bytes()],
                    )?,
                    19 | 21 => admit_composite_key(
                        self.budget,
                        tag,
                        &[b.execution_id.as_bytes(), b.turn_id.as_bytes()],
                    )?,
                    _ => return corrupt(),
                }
            }
            Ok(Some(b))
        })();
        self.budget.finish(result)
    }
    pub fn check_permission_absent(&mut self, present: bool) -> Result<(), PermissionError> {
        self.budget.reserve(128, 1)?;
        if self.step != 15 || !self.generated || self.pid_absent {
            return corrupt();
        }
        if present {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        self.pid_absent = true;
        Ok(())
    }
    pub fn prepare_work(
        &mut self,
    ) -> Result<PermissionAcceptanceWork<'budget, 'outer>, PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if self.step != 14 || self.generated || self.park.is_none() {
                return corrupt();
            }
            // Residual transfer/error envelope: actual Work/Result/argument
            // handoffs, UTF8 scans and sealed metadata, independent of term7.
            self.budget.reserve(32_768, 768)?;
            let s = self.state.take().ok_or(PermissionError::CorruptRecord)?;
            let parts = self.prepared.writer_parts();
            let request = parts.request;
            let before = &s.resource.execution.snapshot;
            let limits = self.budget.original_always_limits();
            // Closed original first shape. No recovery/history extension is
            // inferred from the original fixture's later normal reopen.
            if before.tasks.len() != 1
                || before.definition.batches.len() != 1
                || before.definition.batches[0].len() != 1
                || !before.attempt_history.is_empty()
                || before.turn_history.len() > 2
                || before.status != b4_contract::ExecutionStatus::WaitingForApproval
                || before.failure.is_some()
                || s.grants.len() != 1
                || request.decisions.len() != 1
                || !parts.matched.acceptances().is_empty()
                || s.snapshot_quote.bytes > limits.before
                || s.snapshot_quote.nodes > limits.before_nodes
                || s.thread_quote.bytes > limits.thread
                || s.thread_quote.nodes > 64
                || parts.binding.definition.bytes > 2_048
                || parts.binding.definition.nodes > 32
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            let task = &before.tasks[0];
            let pending = task
                .interrupt
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            let submitted = &request.decisions[0];
            let g = &s.grants[0];
            let gq = s.grant_quote.ok_or(PermissionError::CorruptRecord)?;
            let gp = s.grant_prompt_quote.ok_or(PermissionError::CorruptRecord)?;
            if gq.bytes > limits.grant || gq.nodes > 83 || gp.bytes > 128 || gp.nodes > 16 {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.budget.reserve_normal_original_work(
                &[s.snapshot_quote, s.thread_quote, gq],
                8,
                2,
            )?;
            // Paid scalar eligibility and shallow root scans; submitted SET
            // cardinality/uniqueness is trivial but still explicitly charged.
            self.budget.reserve(35_840, 204)?;
            reserve_measure(s.snapshot_quote, self.budget)?;
            if submitted.cancelled || submitted.decision != ApprovalDecision::Always {
                return Err(PermissionError::UnsupportedProfile);
            }
            let token = submitted
                .token
                .as_ref()
                .ok_or(crate::approval::ApprovalError::MissingToken)?;
            if submitted.interrupt_id != pending.id
                || task.spec.id != g.task_id
                || task.spec.effect_id != g.effect_id
                || !task.spec.requires_approval
                || task.approved
                || task.status != b4_contract::TaskStatus::WaitingForApproval
                || task.attempt_id.is_some()
                || task.result.is_some()
                || task.failure.is_some()
                || g.thread_id != s.resource.thread.id
                || g.thread_generation != s.resource.thread.generation
                || s.resource.thread.origin.as_ref() != Some(&g.origin)
                || g.execution_id != before.execution_id
                || g.route != before.definition.route
                || g.definition_revision != before.definition.revision
                || g.interrupt_id != pending.id
                || g.policy_revision != request.context.policy_revision
                || g.task_parking_revision != before.revision
                || g.issued_at_ms != before.updated_at_ms
                || request.route != before.definition.route
                || request.definition_revision != before.definition.revision
                || s.resource.thread.origin.as_ref() != Some(&request.context.actor)
            {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            let pp = measure(
                CanonicalValue::Json(&pending.prompt),
                CanonicalLimit::LegacyPrompt,
                self.budget,
            )?;
            reserve_measure(gp, self.budget)?;
            reserve_measure(pp, self.budget)?;
            if pp.bytes > 128 || pp.nodes > 16 {
                return Err(PermissionError::UnsupportedProfile);
            }
            if g.prompt != pending.prompt {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            let proposal =
                current_permission_approval_proposal_digest(task, &pending.prompt, self.budget)?;
            if proposal != g.proposal_digest {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            let verifier = <sha2::Sha256 as sha2::Digest>::digest(token.as_str().as_bytes());
            let difference = g
                .verifier
                .iter()
                .zip(verifier.iter())
                .fold(0u8, |d, (l, r)| d | (l ^ r));
            if difference != 0 {
                return Err(crate::approval::ApprovalError::InvalidGrant.into());
            }
            if g.consumed {
                return Err(crate::approval::ApprovalError::Consumed.into());
            }
            if request.now_ms >= g.expires_at_ms {
                return Err(crate::approval::ApprovalError::Expired.into());
            }
            let (prospective_a, prospective_d, prospective_scope) =
                writer_prospective_record_geometry(
                    &s.resource,
                    g,
                    request,
                    parts.catalog,
                    parts.binding.registration,
                    s.snapshot_quote,
                    parts.binding.definition,
                    self.budget,
                )?;
            // Prepay every actual deep/scalar clone before owned A/D construction.
            // The immutable catalogue owns the selected registration and fact.
            let registration = parts.binding.registration;
            self.budget.reserve(58_692, 221)?;
            // A owns the before snapshot; D owns its Scope. Four passes cover
            // their construction, selected scalar copies and Work transfers.
            self.budget.reserve_normal_original_work(
                &[prospective_a, prospective_d, prospective_scope, gq],
                4,
                1,
            )?;
            let application = PermissionApplicationIdentity {
                application: parts.catalog.application_identity().application.clone(),
                revision: parts.catalog.application_identity().revision.clone(),
            };
            let scope = PermissionScope {
                thread_id: s.resource.thread.id.clone(),
                thread_generation: s.resource.thread.generation.clone(),
                origin: g.origin.clone(),
                application: PermissionApplicationIdentity {
                    application: application.application.clone(),
                    revision: application.revision.clone(),
                },
                route: before.definition.route.clone(),
                definition: before.definition.clone(),
                effect_id: registration.effect_id.clone(),
                tool_id: registration.tool_id.clone(),
                tool_revision: registration.tool_revision.clone(),
                policy_revision: request.context.policy_revision.clone(),
                constraint_revision: registration.constraint_revision.clone(),
            };
            let (digest, scope_measure) = writer_scope_digest(&scope, self.budget)?;
            self.budget.reserve(512, 8)?;
            let mut pid = String::new();
            pid.try_reserve_exact(69)
                .map_err(|_| PermissionError::LimitExceeded)?;
            pid.push_str("b4pd_");
            pid.push_str(&digest);
            let permission_id =
                PermissionId::new(pid).map_err(|_| PermissionError::CorruptRecord)?;
            let binding = PermissionBindingIdentity {
                effect_id: registration.effect_id.clone(),
                tool_id: registration.tool_id.clone(),
                tool_revision: registration.tool_revision.clone(),
                constraint_revision: registration.constraint_revision.clone(),
            };
            let entry = PermissionAcceptedDecision {
                task_id: task.spec.id.clone(),
                interrupt_id: pending.id.clone(),
                decision: ApprovalDecision::Always,
                proposal_digest: g.proposal_digest.clone(),
                task_parking_revision: g.task_parking_revision,
                grant_ref: PermissionGrantReference {
                    execution_id: g.execution_id.clone(),
                    interrupt_id: g.interrupt_id.clone(),
                },
                binding: Some(binding),
                permission_id: Some(permission_id.clone()),
            };
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(1)
                .map_err(|_| PermissionError::LimitExceeded)?;
            entries.push(entry);
            let acceptance = PermissionAcceptance {
                record_version: 1,
                thread_id: s.resource.thread.id.clone(),
                thread_generation: s.resource.thread.generation.clone(),
                origin: g.origin.clone(),
                application,
                policy_revision: request.context.policy_revision.clone(),
                before_snapshot: before.clone(),
                turn_id: request.turn_id.clone(),
                facts: TransitionFacts {
                    now_ms: request.now_ms,
                },
                committed_revision: before
                    .revision
                    .checked_add(1)
                    .ok_or(PermissionError::CorruptRecord)?,
                decisions: entries,
            };
            let decision = PermissionDecision {
                record_version: 1,
                permission_id,
                scope_digest: digest,
                scope,
                creator_execution_id: before.execution_id.clone(),
                creator_turn_id: request.turn_id.clone(),
                creator_task_id: task.spec.id.clone(),
                creator_interrupt_id: pending.id.clone(),
            };
            // Actual Kernel Resume and full validate_commit happen ONCE. The
            // fixed closed quote covers their deep clones, validation sets,
            // command/plan/dispatchable allocations and full comparisons.
            self.budget.reserve(131_072, 2_048)?;
            // Kernel Resume and validate_commit retain/compare complete before
            // and planned snapshots, definitions and turn history. Purchase
            // sixteen full measured-byte passes before entering either helper.
            self.budget
                .reserve_normal_original_work(&[s.snapshot_quote], 16, 4)?;
            let mut decisions = Vec::new();
            decisions
                .try_reserve_exact(1)
                .map_err(|_| PermissionError::LimitExceeded)?;
            decisions.push(b4_contract::Decision {
                interrupt_id: pending.id.clone(),
                decision: ApprovalDecision::Always,
            });
            let plan = b4_kernel::transition(
                before,
                b4_contract::KernelCommand::Resume {
                    route: before.definition.route.clone(),
                    decisions,
                    turn_id: request.turn_id.clone(),
                },
                TransitionFacts {
                    now_ms: request.now_ms,
                },
            )?;
            if !crate::validate_commit(before, parts.fence.execution, &plan, self.current_owner)? {
                return corrupt();
            }
            let (a_bytes, d_bytes, aq, dq) =
                writer_pair_capture(&acceptance, &decision, self.budget)?;
            if aq.bytes != prospective_a.bytes
                || aq.nodes != prospective_a.nodes
                || aq.depth != prospective_a.depth
                || dq.bytes != prospective_d.bytes
                || dq.nodes != prospective_d.nodes
                || dq.depth != prospective_d.depth
                || scope_measure.bytes != prospective_scope.bytes
                || scope_measure.nodes != prospective_scope.nodes
                || scope_measure.depth != prospective_scope.depth
            {
                return corrupt();
            }
            let (snapshot_bytes, next_q) = writer_encode_value(
                CanonicalValue::Snapshot(&plan.snapshot),
                CanonicalLimit::ResourceSnapshot,
                self.budget,
            )?;
            if next_q.bytes > limits.resume || next_q.nodes > limits.resume_nodes {
                return Err(PermissionError::UnsupportedProfile);
            }
            // Separate consumed overlay is paid before cloning the actual G.
            clone_quote(gq, self.budget)?;
            let mut consumed = g.clone();
            consumed.consumed = true;
            let (g_bytes, consumed_q) = writer_encode_value(
                CanonicalValue::LegacyGrant(&consumed),
                CanonicalLimit::LegacyRecord,
                self.budget,
            )?;
            let park = self.park.take().ok_or(PermissionError::CorruptRecord)?;
            let resume = crate::JournalEntry::from(&plan.snapshot);
            let park_q = writer_journal_measure(&park)?;
            let resume_q = writer_journal_measure(&resume)?;
            let overlay = writer_resume_overlay_measure(before, &acceptance, s.snapshot_quote)?;
            // Facts come only from the actual paid I1 decoder, owned clone,
            // measurements, Scope hash and Kernel path above. No receipt can
            // construct this graph loan or reconstruct its account.
            prospective_graph::validate_writer_graph(
                &s.resource,
                &s.grants,
                &acceptance,
                &decision,
                &consumed,
                &plan,
                &park,
                &resume,
                prospective_graph::WriterGraphFacts {
                    before: s.snapshot_quote,
                    acceptance_before: s.snapshot_quote,
                    acceptance: aq,
                    decision: dq,
                    decision_scope: scope_measure,
                    acceptance_scope: scope_measure,
                    original_grant: gq,
                    consumed_grant: consumed_q,
                    resume: next_q,
                    overlay,
                    grant_prompt: gp,
                    pending_prompt: pp,
                    park_journal: park_q,
                    expected_park: park_q,
                    resume_journal: resume_q,
                    expected_resume: resume_q,
                },
                self.budget,
            )?;
            // A separate future READ capacity certificate does not reset the
            // current account. Full virtual rows were proved above; all fixed
            // first-shape dimensions/counts must fit the SAME future ceilings.
            self.budget.reserve(32_768, 512)?;
            self.budget.reserve_normal_original_work(
                &[aq, dq, next_q, consumed_q, park_q, resume_q],
                4,
                1,
            )?;
            writer_post_read_certificate(
                aq,
                dq,
                next_q,
                consumed_q,
                park_q,
                resume_q,
                self.budget.original_always_profile,
            )?;
            admit_composite_key(
                self.budget,
                20,
                &[
                    g.execution_id.as_str().as_bytes(),
                    g.interrupt_id.as_str().as_bytes(),
                ],
            )?;
            if self.budget.resource.referenced_grants == 256 {
                return self.budget.limit();
            }
            self.budget.resource.referenced_grants += 1;
            // Typed JournalEntry serialize has only five already bounded
            // fields. It is covered by the pre-header J lifecycle quote.
            let mut journal_output = ExactWriter {
                bytes: Vec::new(),
                maximum: resume_q.bytes,
            };
            journal_output
                .bytes
                .try_reserve_exact(resume_q.bytes)
                .map_err(|_| PermissionError::LimitExceeded)?;
            serde_json::to_writer(&mut journal_output, &resume)
                .map_err(|_| PermissionError::CorruptRecord)?;
            if journal_output.bytes.len() != resume_q.bytes {
                return corrupt();
            }
            let journal_bytes = journal_output.bytes;
            let total = add(
                a_bytes.len(),
                add(
                    d_bytes.len(),
                    add(
                        snapshot_bytes.len(),
                        add(g_bytes.len(), journal_bytes.len())?,
                    )?,
                )?,
            )?;
            if total > limits.encoded_total {
                return Err(PermissionError::UnsupportedProfile);
            }
            // UTF8 validation consumes each actual Vec without reallocating.
            // Preserve the full fixed residual quote and additionally purchase
            // normal validation and owned transfer/drop work for all five buffers.
            self.budget.reserve_normal_original_work(
                &[aq, dq, next_q, consumed_q, resume_q],
                2,
                1,
            )?;
            let snapshot_json =
                String::from_utf8(snapshot_bytes).map_err(|_| PermissionError::CorruptRecord)?;
            let grant_json =
                String::from_utf8(g_bytes).map_err(|_| PermissionError::CorruptRecord)?;
            let acceptance_json =
                String::from_utf8(a_bytes).map_err(|_| PermissionError::CorruptRecord)?;
            let decision_json =
                String::from_utf8(d_bytes).map_err(|_| PermissionError::CorruptRecord)?;
            let journal_json =
                String::from_utf8(journal_bytes).map_err(|_| PermissionError::CorruptRecord)?;
            let next_resource_revision = s
                .resource
                .thread
                .resource_revision
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?;
            self.generated = true;
            Ok(PermissionAcceptanceWork {
                prepared: self.prepared,
                account: self.budget as *const PermissionEvidenceBudget,
                _loan: std::marker::PhantomData,
                resource: s.resource,
                plan,
                acceptance,
                decision,
                consumed,
                next_resource_revision,
                snapshot_json,
                grant_json,
                acceptance_json,
                decision_json,
                journal_json,
            })
        })();
        self.budget.finish(result)
    }
    pub fn finish_after_commit(
        self,
        work: PermissionAcceptanceWork<'budget, 'outer>,
    ) -> Result<crate::StoredExecution, PermissionError> {
        let result = (|| {
            self.budget.reserve(0, 0)?;
            if self.step != 22
                || !self.pid_absent
                || !self.generated
                || !std::ptr::eq(work.account, self.budget as *const PermissionEvidenceBudget)
                || !std::ptr::eq(work.prepared, self.prepared)
            {
                return corrupt();
            }
            // Actual backend COMMIT precedes this trusted return. All owned
            // result/Work transfers and drops were prepaid before construction.
            Ok(crate::StoredExecution {
                thread_id: work.resource.execution.thread_id,
                snapshot: work.plan.snapshot,
            })
        })();
        self.budget.finish(result)
    }
}
impl PermissionAcceptanceWork<'_, '_> {
    fn bindings(&self) -> PermissionAcceptanceBindings<'_> {
        PermissionAcceptanceBindings {
            execution_id: self.resource.execution.snapshot.execution_id.as_str(),
            thread_id: self.resource.thread.id.as_str(),
            interrupt_id: self.consumed.interrupt_id.as_str(),
            turn_id: self.acceptance.turn_id.as_str(),
            permission_id: self.decision.permission_id.as_str(),
            scope_digest: &self.decision.scope_digest,
            generation: self.resource.thread.generation.as_str(),
            before_revision: self.resource.execution.snapshot.revision,
            owner_generation: self.resource.execution.snapshot.owner_generation,
            before_resource_revision: self.resource.thread.resource_revision,
            next_revision: self.plan.snapshot.revision,
            next_resource_revision: self.next_resource_revision,
            updated_at_ms: self.plan.snapshot.updated_at_ms,
            snapshot: &self.snapshot_json,
            consumed_grant: &self.grant_json,
            acceptance: &self.acceptance_json,
            decision: &self.decision_json,
            resume_journal: &self.journal_json,
        }
    }
}
fn writer_encode_value(
    value: CanonicalValue<'_>,
    limit: CanonicalLimit,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(Vec<u8>, CanonicalMeasure), PermissionError> {
    let measured = measure(value, limit, budget)?;
    budget.reserve(measured.bytes, measured.nodes)?;
    let mut out = ExactWriter {
        bytes: Vec::new(),
        maximum: measured.bytes,
    };
    out.bytes
        .try_reserve_exact(measured.bytes)
        .map_err(|_| PermissionError::LimitExceeded)?;
    let emitted = walk(value, limit, &mut out, budget, false)?;
    if emitted.bytes != measured.bytes
        || emitted.nodes != measured.nodes
        || emitted.depth != measured.depth
        || out.bytes.len() != measured.bytes
    {
        return corrupt();
    }
    Ok((out.bytes, measured))
}
fn writer_scope_digest(
    scope: &PermissionScope,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(String, CanonicalMeasure), PermissionError> {
    let value = CanonicalValue::Scope(ScopeParts::from_scope(scope));
    let limit = CanonicalLimit::Evidence64KiB;
    let measured = measure(value, limit, budget)?;
    budget.reserve(measured.bytes, measured.nodes)?;
    let mut output = HashWriter(<sha2::Sha256 as sha2::Digest>::new());
    let emitted = walk(value, limit, &mut output, budget, false)?;
    if emitted.bytes != measured.bytes
        || emitted.nodes != measured.nodes
        || emitted.depth != measured.depth
    {
        return Err(PermissionError::CorruptRecord);
    }
    budget.reserve(96, 0)?;
    let digest = sha2::Digest::finalize(output.0);
    let mut result = String::new();
    result
        .try_reserve_exact(64)
        .map_err(|_| PermissionError::LimitExceeded)?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    Ok((
        result,
        CanonicalMeasure {
            bytes: measured
                .bytes
                .checked_sub(22)
                .ok_or(PermissionError::CorruptRecord)?,
            nodes: measured
                .nodes
                .checked_sub(4)
                .ok_or(PermissionError::CorruptRecord)?,
            depth: measured
                .depth
                .checked_sub(1)
                .ok_or(PermissionError::CorruptRecord)?,
        },
    ))
}

fn writer_scalar_width(value: &str) -> Result<usize, PermissionError> {
    // Pinned serde_json1.0.138 ESCAPE/CompactFormatter widths, counted inline.
    // This is a prepaid borrowed byte scan with no serializer/owned scratch.
    if value.len() > 256 {
        return Err(PermissionError::UnsupportedProfile);
    }
    let mut bytes = 2usize;
    for byte in value.bytes() {
        let width = if matches!(byte, b'"' | b'\\' | 8 | 9 | 10 | 12 | 13) {
            2
        } else if byte < 32 {
            6
        } else {
            1
        };
        bytes = add(bytes, width)?;
    }
    Ok(bytes)
}
fn writer_journal_measure(j: &crate::JournalEntry) -> Result<CanonicalMeasure, PermissionError> {
    let digits = runtime_checked_output_digits(|n: u64| {
        if n == 0 {
            1usize
        } else {
            n.ilog10() as usize + 1
        }
    });
    let fixed =
        b"{\"execution_id\":,\"owner_generation\":,\"revision\":,\"status\":,\"updated_at_ms\":}"
            .len();
    Ok(CanonicalMeasure {
        bytes: add(
            add(fixed, writer_scalar_width(j.execution_id.as_str())?)?,
            add(
                add(digits(j.revision), digits(j.owner_generation))?,
                add(
                    digits(j.updated_at_ms),
                    add(2, execution_status(j.status).len())?,
                )?,
            )?,
        )?,
        nodes: 11,
        depth: 1,
    })
}
fn writer_resume_overlay_measure(
    before: &Snapshot,
    a: &PermissionAcceptance,
    q: CanonicalMeasure,
) -> Result<CanonicalMeasure, PermissionError> {
    let digits = |n: u64| {
        if n == 0 {
            1usize
        } else {
            n.ilog10() as usize + 1
        }
    };
    let replace = |b: usize, old: usize, new: usize| {
        add(
            b.checked_sub(old).ok_or(PermissionError::CorruptRecord)?,
            new,
        )
    };
    let revision = before
        .revision
        .checked_add(1)
        .ok_or(PermissionError::CorruptRecord)?;
    let old_turn = writer_scalar_width(before.turn_id.as_str())?;
    let new_turn = writer_scalar_width(a.turn_id.as_str())?;
    let mut bytes = replace(q.bytes, digits(before.revision), digits(revision))?;
    bytes = replace(bytes, digits(before.updated_at_ms), digits(a.facts.now_ms))?;
    bytes = replace(bytes, old_turn, new_turn)?;
    bytes = replace(bytes, "waiting_for_approval".len(), "running".len())?;
    bytes = replace(bytes, "waiting_for_approval".len(), "ready".len())?;
    bytes = replace(bytes, 5, 4)?;
    Ok(CanonicalMeasure {
        bytes: add(bytes, add(1, new_turn)?)?,
        nodes: add(q.nodes, 1)?,
        depth: q.depth,
    })
}
fn writer_post_read_certificate(
    a: CanonicalMeasure,
    d: CanonicalMeasure,
    s: CanonicalMeasure,
    g: CanonicalMeasure,
    p: CanonicalMeasure,
    r: CanonicalMeasure,
    profile: OriginalAlwaysProfile,
) -> Result<(), PermissionError> {
    // Exact first-shape limits plus a conservative complete fresh-read schedule:
    // external7 + populatedCombined19 normal, two implicit rollback capacities.
    let limits = profile.limits();
    if a.bytes > limits.acceptance
        || a.nodes > limits.acceptance_nodes
        || d.bytes > limits.decision
        || d.nodes > 67
        || s.bytes > limits.resume
        || s.nodes > limits.resume_nodes
        || g.bytes > limits.grant
        || g.nodes > 83
        || p.bytes > limits.journal
        || r.bytes > limits.journal
        || a.depth > 64
        || d.depth > 64
        || s.depth > 127
        || g.depth > 65
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let terms = [
        (2_097_152usize, 2_048usize),
        (1_065_472, 1_675),
        (273_461, 500),
        (65_536, 512),
        (133_120, 104),
        (100_000, 500),
        (98_304, 1_024),
        (152_000, 400),
        (16_384, 512),
        (65_536, 2_048),
        (32_768, 512),
        (2_240, 35),
    ];
    let (mut bytes, mut nodes) = terms.iter().try_fold((0usize, 0usize), |(b, n), (x, y)| {
        Ok::<_, PermissionError>((add(b, *x)?, add(n, *y)?))
    })?;
    if matches!(profile, OriginalAlwaysProfile::NormalOriginalAlways) {
        // Capacity only: no fresh reader has run. Preserve the complete Strict
        // schedule and add measured normal variable capacity for raw decoding,
        // retained owners, canonical passes, cloning, full graph comparisons and
        // handoffs. A embeds before; D embeds Scope. Sixty-four byte passes and
        // eight node passes conservatively cover those repeated operands while
        // retaining the same complete fixed/depth/rollback schedule above.
        let (actual_bytes, actual_nodes) =
            [a, d, s, g, p, r].iter().try_fold((0, 0), |(b, n), q| {
                Ok::<_, PermissionError>((add(b, q.bytes)?, add(n, q.nodes)?))
            })?;
        bytes = add(bytes, mul(64, actual_bytes)?)?;
        nodes = add(nodes, mul(8, actual_nodes)?)?;
    }
    if bytes > ACCOUNT_BYTES || nodes > ACCOUNT_NODES {
        return Err(PermissionError::LimitExceeded);
    }
    Ok(())
}

fn writer_object_geometry(
    keys: &[&str],
    values: &[CanonicalMeasure],
) -> Result<CanonicalMeasure, PermissionError> {
    if keys.len() != values.len() || keys.is_empty() {
        return corrupt();
    }
    let mut bytes = add(2, keys.len() - 1)?;
    let mut nodes = add(1, keys.len())?;
    let mut depth = 0;
    for (key, value) in keys.iter().zip(values) {
        bytes = add(bytes, add(add(key.len(), 3)?, value.bytes)?)?;
        nodes = add(nodes, value.nodes)?;
        depth = depth.max(add(value.depth, 1)?);
    }
    Ok(CanonicalMeasure {
        bytes,
        nodes,
        depth,
    })
}
fn writer_string_geometry(value: &str) -> Result<CanonicalMeasure, PermissionError> {
    Ok(CanonicalMeasure {
        bytes: writer_scalar_width(value)?,
        nodes: 1,
        depth: 0,
    })
}
fn writer_number_geometry(value: u64) -> CanonicalMeasure {
    CanonicalMeasure {
        bytes: if value == 0 {
            1
        } else {
            value.ilog10() as usize + 1
        },
        nodes: 1,
        depth: 0,
    }
}
#[allow(clippy::too_many_arguments)] // Closed borrowed producer fields and paid real dimensions.
fn writer_prospective_record_geometry(
    resource: &crate::approval::ApprovalResource,
    grant: &crate::approval::GrantRecord,
    request: &crate::approval::ApprovalAcceptanceRequest,
    catalog: &super::PermissionCatalog,
    registration: &super::ToolBindingRegistration,
    before: CanonicalMeasure,
    definition: CanonicalMeasure,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(CanonicalMeasure, CanonicalMeasure, CanonicalMeasure), PermissionError> {
    // Same V3 term9 conservative debit, now a closed borrowed scalar/metadata
    // geometry walk. It allocates no canonical stack or owned business clone.
    budget.reserve(82_758, 221)?;
    budget.reserve_normal_original_work(&[before, definition], 1, 1)?;
    let limits = budget.original_always_limits();
    let string = writer_string_geometry;
    let number = writer_number_geometry;
    let identity = catalog.application_identity();
    let origin = writer_object_geometry(
        &["application", "principal", "tenant"],
        &[
            string(&grant.origin.application)?,
            string(&grant.origin.principal)?,
            string(&grant.origin.tenant)?,
        ],
    )?;
    let application = writer_object_geometry(
        &["application", "revision"],
        &[string(&identity.application)?, string(&identity.revision)?],
    )?;
    let binding = writer_object_geometry(
        &[
            "constraint_revision",
            "effect_id",
            "tool_id",
            "tool_revision",
        ],
        &[
            string(registration.constraint_revision.as_str())?,
            string(registration.effect_id.as_str())?,
            string(registration.tool_id.as_str())?,
            string(registration.tool_revision.as_str())?,
        ],
    )?;
    let scope = writer_object_geometry(
        &[
            "application",
            "constraint_revision",
            "definition",
            "effect_id",
            "origin",
            "policy_revision",
            "route",
            "thread_generation",
            "thread_id",
            "tool_id",
            "tool_revision",
        ],
        &[
            application,
            string(registration.constraint_revision.as_str())?,
            definition,
            string(registration.effect_id.as_str())?,
            origin,
            string(&request.context.policy_revision)?,
            string(resource.execution.snapshot.definition.route.as_str())?,
            string(resource.thread.generation.as_str())?,
            string(resource.thread.id.as_str())?,
            string(registration.tool_id.as_str())?,
            string(registration.tool_revision.as_str())?,
        ],
    )?;
    // Only mathematical strict-hex widths are used here, never fake authority.
    let pid = CanonicalMeasure {
        bytes: 71,
        nodes: 1,
        depth: 0,
    };
    let digest = CanonicalMeasure {
        bytes: 66,
        nodes: 1,
        depth: 0,
    };
    let reference = writer_object_geometry(
        &["execution_id", "interrupt_id"],
        &[
            string(grant.execution_id.as_str())?,
            string(grant.interrupt_id.as_str())?,
        ],
    )?;
    let entry = writer_object_geometry(
        &[
            "binding",
            "decision",
            "grant_ref",
            "interrupt_id",
            "permission_id",
            "proposal_digest",
            "task_id",
            "task_parking_revision",
        ],
        &[
            binding,
            string("always")?,
            reference,
            string(grant.interrupt_id.as_str())?,
            pid,
            string(&grant.proposal_digest)?,
            string(grant.task_id.as_str())?,
            number(grant.task_parking_revision),
        ],
    )?;
    let entries = CanonicalMeasure {
        bytes: add(2, entry.bytes)?,
        nodes: add(1, entry.nodes)?,
        depth: add(1, entry.depth)?,
    };
    let facts = writer_object_geometry(&["now_ms"], &[number(request.now_ms)])?;
    let revision = resource
        .execution
        .snapshot
        .revision
        .checked_add(1)
        .ok_or(PermissionError::CorruptRecord)?;
    let acceptance = writer_object_geometry(
        &[
            "application",
            "before_snapshot",
            "committed_revision",
            "decisions",
            "facts",
            "origin",
            "policy_revision",
            "record_version",
            "thread_generation",
            "thread_id",
            "turn_id",
        ],
        &[
            application,
            before,
            number(revision),
            entries,
            facts,
            origin,
            string(&request.context.policy_revision)?,
            number(1),
            string(resource.thread.generation.as_str())?,
            string(resource.thread.id.as_str())?,
            string(request.turn_id.as_str())?,
        ],
    )?;
    let decision = writer_object_geometry(
        &[
            "creator_execution_id",
            "creator_interrupt_id",
            "creator_task_id",
            "creator_turn_id",
            "permission_id",
            "record_version",
            "scope",
            "scope_digest",
        ],
        &[
            string(grant.execution_id.as_str())?,
            string(grant.interrupt_id.as_str())?,
            string(grant.task_id.as_str())?,
            string(request.turn_id.as_str())?,
            pid,
            number(1),
            scope,
            digest,
        ],
    )?;
    if acceptance.bytes > limits.acceptance
        || acceptance.nodes > limits.acceptance_nodes
        || decision.bytes > limits.decision
        || decision.nodes > 67
        || acceptance.depth > 64
        || decision.depth > 64
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    Ok((acceptance, decision, scope))
}
// Writer retention variant: exactly the same old private record encoding loop,
// with its actual measured facts returned rather than reconstructed from bytes.
fn writer_pair_record<'data>(
    workspace: &mut CanonicalWorkspace<'data>,
    record: BorrowedRecord<'data>,
    kind: RecordKind,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(Vec<u8>, CanonicalMeasure), PermissionError> {
    let value = CanonicalValue::Record(record);
    let limit = CanonicalLimit::Record(kind);
    let measured = pair_measure(workspace, value, limit, budget)?;
    check_pair_inherent(workspace, record, &measured, budget)?;
    budget.reserve(measured.bytes, measured.nodes)?;
    let mut output = ExactWriter {
        bytes: Vec::new(),
        maximum: measured.bytes,
    };
    output
        .bytes
        .try_reserve_exact(measured.bytes)
        .map_err(|_| PermissionError::LimitExceeded)?;
    let emitted = workspace.walk(value, limit, &mut output, budget, false)?;
    if emitted.bytes != measured.bytes
        || emitted.nodes != measured.nodes
        || emitted.depth != measured.depth
        || output.bytes.len() != measured.bytes
    {
        return corrupt();
    }
    Ok((output.bytes, measured))
}
fn writer_pair_capture(
    acceptance: &PermissionAcceptance,
    decision: &PermissionDecision,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(Vec<u8>, Vec<u8>, CanonicalMeasure, CanonicalMeasure), PermissionError> {
    let result = (|| {
        let mut workspace = CanonicalWorkspace::new(budget)?;
        let (a, aq) = writer_pair_record(
            &mut workspace,
            BorrowedRecord::Acceptance(acceptance),
            RecordKind::Acceptance,
            budget,
        )?;
        let (d, dq) = writer_pair_record(
            &mut workspace,
            BorrowedRecord::Decision(decision),
            RecordKind::Decision,
            budget,
        )?;
        drop(workspace);
        Ok((a, d, aq, dq))
    })();
    budget.finish(result)
}

const _: () = assert!(
    std::mem::size_of::<Result<PermissionAcceptanceWork<'_, '_>, PermissionError>>() <= 3_080
);
const _: () = assert!(std::mem::size_of::<PermissionAcceptanceWrite<'_, '_>>() <= 2_048);

const _: () = assert!(std::mem::size_of::<super::model::PermissionPreparedParts<'_, '_>>() <= 80);

const _: () = assert!(
    std::mem::size_of::<Result<PermissionAcceptanceWrite<'_, '_>, PermissionError>>() <= 2_056
);

// The first populated read is a token-free historical projection. Its private
// facts are produced by the actual paid methods of one live session.
pub(super) use populated_projection::Finished as FinishedPermissionPopulatedView;
pub use populated_projection::{
    PermissionJournalPoint, PermissionPopulatedSql, PermissionPopulatedViewRead,
    PermissionProjectionBranch, PermissionProjectionJournalHeaderCells, PermissionProjectionRead,
};
mod populated_projection {
    use super::*;

    // Inert whole named bounds for the private mandatory-domain compiler prospect.
    // N41: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(std::mem::size_of::<InherentFacts>() <= 128);
        assert!(std::mem::align_of::<InherentFacts>() <= 16);
    };
    // N42: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(std::mem::size_of::<Result<InherentFacts, PermissionError>>() <= 128);
        assert!(std::mem::align_of::<Result<InherentFacts, PermissionError>>() <= 16);
    };
    // N43: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    InherentFacts,
                >,
            >() <= 128
        );
        assert!(
            std::mem::align_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    InherentFacts,
                >,
            >() <= 16
        );
    };
    // N44: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(std::mem::size_of::<(String, CanonicalMeasure)>() <= 128);
        assert!(std::mem::align_of::<(String, CanonicalMeasure)>() <= 16);
    };
    // N45: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(std::mem::size_of::<Result<(String, CanonicalMeasure), PermissionError>>() <= 128);
        assert!(std::mem::align_of::<Result<(String, CanonicalMeasure), PermissionError>>() <= 16);
    };
    // N46: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    (String, CanonicalMeasure),
                >,
            >() <= 128
        );
        assert!(
            std::mem::align_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    (String, CanonicalMeasure),
                >,
            >() <= 16
        );
    };
    use crate::approval::GrantRecord;
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};

    struct NewHeader {
        primary: String,
        digest: Option<String>,
        previous: Option<u64>,
        committed: Option<u64>,
        bytes: usize,
    }
    struct InherentFacts {
        before: Option<CanonicalMeasure>,
        scope: Option<CanonicalMeasure>,
    }
    struct RecordFacts {
        root: CanonicalMeasure,
        inherent: InherentFacts,
    }
    struct Facts {
        snapshot: CanonicalMeasure,
        a: CanonicalMeasure,
        before: CanonicalMeasure,
        a_scope: CanonicalMeasure,
        d: CanonicalMeasure,
        d_scope: CanonicalMeasure,
        grant: CanonicalMeasure,
        prompt: CanonicalMeasure,
        journals: [CanonicalMeasure; 2],
    }
    const ZERO: CanonicalMeasure = CanonicalMeasure {
        bytes: 0,
        nodes: 0,
        depth: 0,
    };
    // P0 is prepaid at the nonempty factory before any added metadata or
    // classifier event. The original Ready facts and graph remain separate.
    const FIRST_READER_P0_BYTES: usize = 65_536;
    const FIRST_READER_P0_NODES: usize = 2_048;
    const FIRST_READER_P3_BYTES: usize = 73_728;
    const FIRST_READER_P3_NODES: usize = 768;
    const FIRST_READER_P4_BYTES: usize = 196_608;
    const FIRST_READER_P4_NODES: usize = 8_192;
    const FIRST_READER_P5_BYTES: usize = 8_192;
    const FIRST_READER_P5_NODES: usize = 128;
    // Constant evaluation adds no runtime work before possibly denied P0.
    const FIRST_READER_FACTORY_BYTES: usize =
        16_384 + FIRST_READER_P0_BYTES + FIRST_READER_P5_BYTES;
    const FIRST_READER_FACTORY_NODES: usize = 256 + FIRST_READER_P0_NODES + FIRST_READER_P5_NODES;
    struct FirstDispatchReaderMetadata {
        selected: bool,
        dispatch_q: CanonicalMeasure,
        sequences: [i64; 3],
    }
    pub struct PermissionProjectionRead<'b, 's, 'd> {
        legacy: PermissionLegacyViewRead<'b, 's, 'd>,
        snapshot_q: CanonicalMeasure,
        headers: [Option<NewHeader>; 2],
        counts: [usize; 3],
    }
    // Inline variants keep one already paid owner; a Box would add another
    // allocation. Both closed alternatives are statically bounded below.
    #[allow(clippy::large_enum_variant)]
    pub enum PermissionProjectionBranch<'b, 's, 'd> {
        Legacy(PermissionLegacyViewRead<'b, 's, 'd>),
        First(PermissionPopulatedViewRead<'b, 's, 'd>),
    }
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum PermissionJournalPoint {
        Park,
        Resume,
        Dispatch,
    }
    #[derive(Clone, Copy)]
    pub enum PermissionPopulatedSql {
        AcceptancePayload,
        DecisionPayload,
        GrantHeaders,
        GrantPayload,
        OldAcceptanceHeaders,
        JournalHeader(PermissionJournalPoint),
        JournalPayload(PermissionJournalPoint),
        CommitRead,
    }
    pub struct PermissionProjectionJournalHeaderCells<'a> {
        pub sequence: i64,
        pub kind: &'a [u8],
        pub thread_id: &'a [u8],
        pub execution_id: &'a [u8],
        pub revision: &'a [u8],
        pub owner: &'a [u8],
        pub status: &'a [u8],
        pub time: &'a [u8],
        pub payload_bytes: usize,
    }
    struct JournalHeader {
        sequence: i64,
        revision: u64,
        owner: u64,
        status: E,
        time: u64,
        bytes: usize,
    }
    pub struct PermissionPopulatedViewRead<'b, 's, 'd> {
        legacy: PermissionLegacyViewRead<'b, 's, 'd>,
        headers: [Option<NewHeader>; 2],
        facts: Facts,
        first_dispatch: FirstDispatchReaderMetadata,
        acceptances: Vec<PermissionAcceptance>,
        decisions: Vec<PermissionDecision>,
        journals: Vec<crate::JournalEntry>,
        journal_header: Option<JournalHeader>,
        journal_rows: usize,
        stage: usize,
        evidence_done: bool,
        commit_admitted: bool,
    }
    pub(in super::super) struct Finished {
        resource: crate::approval::ApprovalResource,
        grants: Vec<GrantRecord>,
        acceptances: Vec<PermissionAcceptance>,
        decisions: Vec<PermissionDecision>,
        journals: Vec<crate::JournalEntry>,
        purpose: super::super::PermissionReadPurpose,
        usage: PermissionEvidenceUsage,
    }
    impl Finished {
        #[allow(clippy::type_complexity)]
        pub(in super::super) fn into_parts(
            self,
        ) -> (
            crate::approval::ApprovalResource,
            Vec<GrantRecord>,
            Vec<PermissionAcceptance>,
            Vec<PermissionDecision>,
            Vec<crate::JournalEntry>,
            super::super::PermissionReadPurpose,
            PermissionEvidenceUsage,
        ) {
            (
                self.resource,
                self.grants,
                self.acceptances,
                self.decisions,
                self.journals,
                self.purpose,
                self.usage,
            )
        }
    }
    macro_rules! redacted { ($($t:ident),+) => { $(impl fmt::Debug for $t<'_,'_,'_> { fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result { f.write_str(concat!(stringify!($t),"([REDACTED])")) } })+ }; }
    redacted!(
        PermissionProjectionRead,
        PermissionPopulatedViewRead,
        PermissionProjectionBranch
    );
    const _: () = assert!(
        std::mem::size_of::<PermissionProjectionRead<'static, 'static, 'static>>() <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<PermissionProjectionBranch<'static, 'static, 'static>>() <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<PermissionPopulatedViewRead<'static, 'static, 'static>>() <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<PermissionProjectionRead<'static, 'static, 'static>, PermissionError>,
        >() <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<PermissionProjectionBranch<'static, 'static, 'static>, PermissionError>,
        >() <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<PermissionPopulatedViewRead<'static, 'static, 'static>, PermissionError>,
        >() <= 2_048
    );
    const _: () = assert!(std::mem::size_of::<Result<Finished, PermissionError>>() <= 1_024);
    const _: () = assert!(
        std::mem::size_of::<Result<super::super::PermissionView, PermissionError>>() <= 1_024
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<
                (
                    crate::approval::ApprovalResource,
                    Option<PermissionAcceptanceCapture>,
                    bool,
                    CanonicalMeasure,
                    CanonicalMeasure
                ),
                PermissionError,
            >,
        >() <= 768
    );
    const _: () = assert!(std::mem::size_of::<NewHeader>() <= 256);
    const _: () = assert!(std::mem::size_of::<Option<NewHeader>>() <= 256);
    const _: () = assert!(
        std::mem::size_of::<PermissionLegacyViewRead<'static, 'static, 'static>>() <= 1_280
    );
    const _: () = assert!(
        std::mem::size_of::<PermissionProjectionRead<'static, 'static, 'static>>() <= 1_536
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<PermissionProjectionRead<'static, 'static, 'static>, PermissionError>,
        >() <= 1_536
    );
    const _: () = assert!(
        std::mem::size_of::<(CanonicalMeasure, [Option<NewHeader>; 2], [usize; 3])>() <= 256
    );
    // Shared old Ok/return/caller-? sites carry at most this extra metadata.
    // Their three copies are counted separately in the prepaid early ledger.
    const _: () = assert!(
        std::mem::size_of::<PermissionProjectionRead<'static, 'static, 'static>>()
            <= std::mem::size_of::<PermissionLegacyViewRead<'static, 'static, 'static>>() + 256
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<PermissionProjectionRead<'static, 'static, 'static>, PermissionError>,
        >() <= std::mem::size_of::<
            Result<PermissionLegacyViewRead<'static, 'static, 'static>, PermissionError>,
        >() + 256
    );

    const _: () = assert!(std::mem::size_of::<Facts>() <= 256);
    const _: () = assert!(
        std::mem::size_of::<(
            crate::approval::ApprovalResource,
            Option<PermissionAcceptanceCapture>,
            bool,
            CanonicalMeasure,
            CanonicalMeasure
        )>() <= 768
    );
    const _: () = assert!(std::mem::size_of::<PermissionAcceptance>() <= 4_096);
    const _: () = assert!(std::mem::size_of::<PermissionDecision>() <= 2_048);
    const _: () = assert!(std::mem::size_of::<crate::JournalEntry>() <= 512);
    const _: () = assert!(std::mem::size_of::<Finished>() <= 1_024);
    const _: () = assert!(
        std::mem::size_of::<(
            crate::approval::ApprovalResource,
            Vec<GrantRecord>,
            Vec<PermissionAcceptance>,
            Vec<PermissionDecision>,
            Vec<crate::JournalEntry>,
            super::super::PermissionReadPurpose,
            PermissionEvidenceUsage
        )>() <= 1_024
    );
    const _: () = assert!(
        std::mem::size_of::<GrantRecord>() + 2 * std::mem::size_of::<CanonicalMeasure>() <= 1_024
    );
    impl<'b, 's, 'd> PermissionCombinedRead<'b, 's, 'd> {
        pub fn into_projection(
            self,
        ) -> Result<PermissionProjectionRead<'b, 's, 'd>, PermissionError> {
            let Self {
                budget,
                id,
                fence,
                selector,
                metadata_checked,
                execution,
                thread,
            } = self;
            let result = (|| {
                budget.reserve(0, 0)?;
                if selector.is_acceptance_capture()
                    || selector.purpose() != super::super::PermissionReadPurpose::Dispatch
                    || selector.task_id().is_none()
                    || selector.expected_view().is_some()
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if !metadata_checked {
                    return corrupt();
                }
                let (execution, execution_measure) =
                    execution.ok_or(PermissionError::CorruptRecord)?;
                let (thread, thread_measure) = thread.ok_or(PermissionError::CorruptRecord)?;
                PermissionResourceRead {
                    budget: &mut *budget,
                }
                .check_linkage(id, &thread, &execution)?;
                let expected = selector.expected_resource();
                // The original resource has no public quote. Pay its own two walks.
                let original_execution = measure(
                    CanonicalValue::Snapshot(&expected.execution.snapshot),
                    CanonicalLimit::ResourceSnapshot,
                    budget,
                )?;
                let original_thread = measure(
                    CanonicalValue::Thread(&expected.thread),
                    CanonicalLimit::ResourceThread,
                    budget,
                )?;
                reserve_measure(execution_measure, budget)?;
                reserve_measure(thread_measure, budget)?;
                reserve_measure(original_execution, budget)?;
                reserve_measure(original_thread, budget)?;
                // Full resource Eq, selector origins/fences/labels and both Definitions
                // are independent work. No sensitive-record operation is exposed yet.
                budget.reserve(16_384, 512)?;
                let snapshot = &execution.snapshot;
                if fence.execution.owner_generation != snapshot.owner_generation {
                    return Err(crate::StoreError::OwnerConflict.into());
                }
                if fence.execution.revision != snapshot.revision
                    || fence.resource_revision != thread.resource_revision
                    || execution != expected.execution
                    || thread != expected.thread
                {
                    return Err(crate::StoreError::RevisionConflict.into());
                }
                let context = selector.context();
                permission_read_definition_quote(&snapshot.definition, budget)?;
                permission_read_definition_quote(selector.definition(), budget)?;
                if &snapshot.execution_id != id
                    || context.thread_id != thread.id
                    || context.thread_generation != thread.generation
                    || thread.origin.as_ref() != Some(&context.origin)
                    || context.execution_revision != snapshot.definition.revision
                    || selector.definition() != &snapshot.definition
                    || !valid_label(&context.application_revision)
                    || !valid_label(&context.policy_revision)
                {
                    return corrupt();
                }
                if matches!(
                    selector.purpose(),
                    super::super::PermissionReadPurpose::Dispatch
                ) && selector.task_id().is_none()
                {
                    return corrupt();
                }
                if let Some(id) = selector.task_id() {
                    budget.reserve(
                        mul(512, snapshot.tasks.len())?,
                        mul(2, snapshot.tasks.len())?,
                    )?;
                    if !snapshot.tasks.iter().any(|task| &task.spec.id == id) {
                        return corrupt();
                    }
                }
                let resource = crate::approval::ApprovalResource { thread, execution };
                let capture_quote = if selector.is_acceptance_capture() {
                    Some(PermissionAcceptanceCapture {
                        identity: std::sync::Arc::clone(
                            budget
                                .acceptance_identity
                                .as_ref()
                                .ok_or(PermissionError::CorruptRecord)?,
                        ),
                        snapshot: execution_measure,
                        thread: thread_measure,
                        records: CanonicalMeasureAggregate {
                            bytes: 0,
                            nodes: 0,
                            rows: 0,
                        },
                    })
                } else {
                    None
                };
                let mut prior_resource_matched = false;
                if let Some(prior) = selector.expected_view() {
                    let actual_quote = capture_quote
                        .as_ref()
                        .ok_or(PermissionError::UnsupportedProfile)?;
                    let prior_quote = prior
                        .acceptance_capture()
                        .ok_or(PermissionError::UnsupportedProfile)?;
                    actual_quote.reserve_resource_eq(prior_quote, budget)?;
                    if &resource != prior.resource() {
                        return Err(crate::StoreError::RevisionConflict.into());
                    }
                    // Both owned current resource and borrowed prior View remain
                    // immutable throughout all sensitive methods and the final Eq.
                    prior_resource_matched = true;
                }
                Ok((
                    resource,
                    capture_quote,
                    prior_resource_matched,
                    execution_measure,
                    thread_measure,
                ))
            })();
            let (resource, capture_quote, prior_resource_matched, snapshot_q, _thread_q) =
                budget.finish(result).map_err(view_error)?;
            budget.reserve(512, 8)?;
            Ok(PermissionProjectionRead {
                snapshot_q,
                headers: [None, None],
                counts: [0; 3],
                legacy: PermissionLegacyViewRead {
                    budget,
                    selector,
                    resource,
                    capture_quote,
                    prior_resource_matched,
                    new_group: 0,
                    new_rows: 0,
                    new_nonempty: false,
                    grants_done: false,
                    acceptances_done: false,
                    grant_header: None,
                    acceptance_header: None,
                    grants: Vec::new(),
                    acceptances: Vec::new(),
                },
            })
        }
    }
    impl PermissionProjectionRead<'_, '_, '_> {
        pub fn before_sql(&mut self, statement: PermissionViewSql) -> Result<(), PermissionError> {
            self.legacy.before_sql(statement)
        }
        pub fn before_row(&mut self) -> Result<(), PermissionError> {
            self.legacy.before_row()
        }
        pub fn admit_new_header(
            &mut self,
            cells: PermissionNewHeaderCells<'_>,
        ) -> Result<(), PermissionError> {
            // Existing finite index checks are completed before retaining any prefix.
            let group = cells.group;
            let primary = cells.primary;
            let digest = cells.digest;
            let previous = cells.previous_revision;
            let committed = cells.committed_revision;
            let bytes = cells.record_bytes;
            self.legacy.admit_new_header(cells)?;
            let result = (|| {
                self.legacy.budget.reserve(8_192, 64)?;
                let index = group_index(group);
                self.counts[index] = add(self.counts[index], 1)?;
                if index < 2 && self.counts[index] == 1 {
                    self.headers[index] = Some(NewHeader {
                        primary: resource_string(primary, self.legacy.budget)?,
                        digest: match digest {
                            Some(raw) => Some(resource_string(raw, self.legacy.budget)?),
                            None => None,
                        },
                        previous: previous.map(resource_u64).transpose()?,
                        committed: committed.map(resource_u64).transpose()?,
                        bytes,
                    });
                }
                Ok(())
            })();
            self.legacy.budget.finish(result)
        }
        pub fn end_new_range(&mut self, group: PermissionNewGroup) -> Result<(), PermissionError> {
            // Defer only the old deliberate healthy-nonempty refusal, after ALL
            // three ranges. Malformed rows and exhausted accounts are never hidden.
            match self.legacy.end_new_range(group) {
                Err(PermissionError::UnsupportedProfile)
                    if group == PermissionNewGroup::Uses
                        && self.legacy.new_group == 3
                        && self.legacy.new_nonempty =>
                {
                    Ok(())
                }
                result => result,
            }
        }
    }
    impl<'b, 's, 'd> PermissionProjectionRead<'b, 's, 'd> {
        /// Borrowed classification introduces no owning Branch on the empty path.
        pub fn headers_empty(&mut self) -> Result<bool, PermissionError> {
            self.legacy.budget.reserve(0, 0)?;
            if self.legacy.new_group != 3 {
                return corrupt();
            }
            Ok(self.counts == [0, 0, 0] && !self.legacy.new_nonempty)
        }
        pub fn empty_legacy_mut(
            &mut self,
        ) -> Result<&mut PermissionLegacyViewRead<'b, 's, 'd>, PermissionError> {
            if !self.headers_empty()? {
                return corrupt();
            }
            Ok(&mut self.legacy)
        }
        /// Trusted backend has committed; delegate the literal old paid factory.
        pub fn finish_empty_after_commit(
            mut self,
        ) -> Result<super::super::PermissionView, PermissionError> {
            if !self.headers_empty()? {
                return corrupt();
            }
            let Self { legacy, .. } = self;
            legacy.finish_after_commit()
        }
        pub fn finish_headers(
            self,
        ) -> Result<PermissionProjectionBranch<'b, 's, 'd>, PermissionError> {
            let Self {
                legacy,
                snapshot_q,
                headers,
                counts,
            } = self;
            legacy.budget.reserve(0, 0)?;
            if legacy.new_group != 3 {
                return corrupt();
            }
            // Empty projections use the borrow/delegation path above.
            if counts == [0, 0, 0] {
                return Err(PermissionError::UnsupportedProfile);
            }
            if counts != [1, 1, 0] {
                return Err(PermissionError::UnsupportedProfile);
            }
            // First-only owning states have no legacy end_range counterpart.
            // Prepay six <=2048 owner/Result handoffs plus scalar/error work;
            // do not rely on elision or future A/D payload admission credit.
            legacy
                .budget
                .reserve(FIRST_READER_FACTORY_BYTES, FIRST_READER_FACTORY_NODES)?;
            // Classification is data from the already fenced actual resource.
            // It is not authority; the full borrowed graph is checked later.
            let current = &legacy.resource.execution.snapshot;
            let selected = current.status == E::Running
                && current.tasks.len() == 1
                && current.tasks[0].status == T::InFlight;
            Ok(PermissionProjectionBranch::First(
                PermissionPopulatedViewRead {
                    legacy,
                    headers,
                    facts: Facts {
                        snapshot: snapshot_q,
                        a: ZERO,
                        before: ZERO,
                        a_scope: ZERO,
                        d: ZERO,
                        d_scope: ZERO,
                        grant: ZERO,
                        prompt: ZERO,
                        journals: [ZERO, ZERO],
                    },
                    first_dispatch: FirstDispatchReaderMetadata {
                        selected,
                        dispatch_q: ZERO,
                        sequences: [0; 3],
                    },
                    acceptances: Vec::new(),
                    decisions: Vec::new(),
                    journals: Vec::new(),
                    journal_header: None,
                    journal_rows: 0,
                    stage: 0,
                    evidence_done: false,
                    commit_admitted: false,
                },
            ))
        }
    }
    fn bounded(q: CanonicalMeasure, bytes: usize, nodes: usize, depth: usize) -> bool {
        q.bytes <= bytes && q.nodes <= nodes && q.depth <= depth
    }
    fn single_push<T>(values: &mut Vec<T>, value: T) -> Result<(), PermissionError> {
        if !values.is_empty() {
            return corrupt();
        }
        values
            .try_reserve_exact(1)
            .map_err(|_| PermissionError::LimitExceeded)?;
        values.push(value);
        Ok(())
    }
    fn decode_acceptance(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(PermissionAcceptance, RecordFacts), PermissionError> {
        preflight(raw, RecordKind::Acceptance, budget)?;
        let wire = serde_json::from_slice::<AcceptanceWire>(raw)
            .map_err(|_| PermissionError::CorruptRecord)?;
        let value = finish_acceptance(wire, raw.len(), budget)?;
        let borrowed = BorrowedRecord::Acceptance(&value);
        let root = measure(
            CanonicalValue::Record(borrowed),
            CanonicalLimit::Record(RecordKind::Acceptance),
            budget,
        )?;
        let inherent = captured_inherent(borrowed, &root, budget)?;
        Ok((value, RecordFacts { root, inherent }))
    }
    fn decode_decision(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(PermissionDecision, RecordFacts), PermissionError> {
        preflight(raw, RecordKind::Decision, budget)?;
        let wire = serde_json::from_slice::<DecisionWire>(raw)
            .map_err(|_| PermissionError::CorruptRecord)?;
        let value = PermissionDecision::from(wire);
        let borrowed = BorrowedRecord::Decision(&value);
        let root = measure(
            CanonicalValue::Record(borrowed),
            CanonicalLimit::Record(RecordKind::Decision),
            budget,
        )?;
        let inherent = captured_inherent(borrowed, &root, budget)?;
        Ok((value, RecordFacts { root, inherent }))
    }
    impl PermissionPopulatedViewRead<'_, '_, '_> {
        // The separate first-dispatch projection retains its qualified Strict
        // profile even if an internal caller holds the original-Always account.
        fn original_always_limits(&self) -> OriginalAlwaysLimits {
            if self.first_dispatch.selected {
                OriginalAlwaysProfile::Strict.limits()
            } else {
                self.legacy.budget.original_always_limits()
            }
        }
        pub fn execution_id(&self) -> &ExecutionId {
            self.legacy.execution_id()
        }
        pub fn thread_id(&self) -> &ThreadId {
            self.legacy.thread_id()
        }
        pub fn first_acceptance_turn(&self) -> &str {
            self.headers[0].as_ref().map_or("", |h| h.primary.as_str())
        }
        pub fn first_permission_id(&self) -> &str {
            self.headers[1].as_ref().map_or("", |h| h.primary.as_str())
        }
        pub fn acceptance_bytes(&self) -> usize {
            self.headers[0].as_ref().map_or(0, |h| h.bytes)
        }
        pub fn decision_bytes(&self) -> usize {
            self.headers[1].as_ref().map_or(0, |h| h.bytes)
        }
        pub fn journal_revision(
            &self,
            point: PermissionJournalPoint,
        ) -> Result<u64, PermissionError> {
            let a = self
                .acceptances
                .first()
                .ok_or(PermissionError::CorruptRecord)?;
            Ok(match point {
                PermissionJournalPoint::Park => a.before_snapshot.revision,
                PermissionJournalPoint::Resume => a.committed_revision,
                PermissionJournalPoint::Dispatch => {
                    if !self.first_dispatch.selected || self.stage != 7 {
                        return corrupt();
                    }
                    self.legacy.resource.execution.snapshot.revision
                }
            })
        }
        /// Paid by P0 before constructing this populated owner on either path.
        pub fn has_first_dispatch_journal(&mut self) -> Result<bool, PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                if self.stage != 7
                    || self.evidence_done
                    || self.journal_header.is_some()
                    || self.journal_rows != 0
                    || self.journals.len() != 2
                {
                    return corrupt();
                }
                Ok(self.first_dispatch.selected)
            })();
            self.legacy.budget.finish(result)
        }
        pub fn before_sql(
            &mut self,
            statement: PermissionPopulatedSql,
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                let id = self
                    .legacy
                    .resource
                    .execution
                    .snapshot
                    .execution_id
                    .as_str()
                    .as_bytes();
                match statement {
                    PermissionPopulatedSql::AcceptancePayload => {
                        if self.stage != 0 || !self.acceptances.is_empty() {
                            return corrupt();
                        }
                        // Metadata, extra owning states and A/D one-slot vectors
                        // are paid BEFORE receiving either decoded payload.
                        self.legacy.budget.reserve(65_536, 512)?;
                        let h = self.headers[0]
                            .as_ref()
                            .ok_or(PermissionError::CorruptRecord)?;
                        admit_composite_key(
                            self.legacy.budget,
                            11,
                            &[b"pa", id, h.primary.as_bytes()],
                        )?;
                    }
                    PermissionPopulatedSql::DecisionPayload => {
                        if self.stage != 1 || !self.decisions.is_empty() {
                            return corrupt();
                        }
                        let h = self.headers[1]
                            .as_ref()
                            .ok_or(PermissionError::CorruptRecord)?;
                        admit_composite_key(
                            self.legacy.budget,
                            12,
                            &[b"pd", h.primary.as_bytes()],
                        )?;
                    }
                    PermissionPopulatedSql::GrantHeaders => {
                        if self.stage != 2 || self.legacy.grants_done {
                            return corrupt();
                        }
                        admit_composite_key(self.legacy.budget, 10, &[b"g", id])?;
                        self.stage = 3;
                    }
                    PermissionPopulatedSql::GrantPayload => {
                        if self.stage != 3 {
                            return corrupt();
                        }
                        let h = self
                            .legacy
                            .grant_header
                            .as_ref()
                            .ok_or(PermissionError::CorruptRecord)?;
                        admit_composite_key(
                            self.legacy.budget,
                            3,
                            &[h.execution.as_bytes(), h.interrupt.as_bytes()],
                        )?;
                    }
                    PermissionPopulatedSql::OldAcceptanceHeaders => {
                        if self.stage != 4 || !self.legacy.grants_done {
                            return corrupt();
                        }
                        admit_composite_key(self.legacy.budget, 10, &[b"a", id])?;
                    }
                    PermissionPopulatedSql::JournalHeader(point) => {
                        let index = point_index(point);
                        if (index == 2 && !self.first_dispatch.selected)
                            || self.stage != 5 + index
                            || self.journal_header.is_some()
                            || self.journal_rows != 0
                        {
                            return corrupt();
                        }
                        if index == 0 {
                            self.legacy.budget.reserve(
                                65_536
                                    + if self.first_dispatch.selected {
                                        FIRST_READER_P3_BYTES
                                    } else {
                                        0
                                    },
                                512 + if self.first_dispatch.selected {
                                    FIRST_READER_P3_NODES
                                } else {
                                    0
                                },
                            )?;
                        }
                        let a = self
                            .acceptances
                            .first()
                            .ok_or(PermissionError::CorruptRecord)?;
                        let revision = match point {
                            PermissionJournalPoint::Park => a.before_snapshot.revision,
                            PermissionJournalPoint::Resume => a.committed_revision,
                            PermissionJournalPoint::Dispatch => {
                                self.legacy.resource.execution.snapshot.revision
                            }
                        };
                        admit_composite_key(
                            self.legacy.budget,
                            13,
                            &[id, &revision.to_be_bytes(), &[index as u8]],
                        )?;
                    }
                    PermissionPopulatedSql::JournalPayload(point) => {
                        if (point == PermissionJournalPoint::Dispatch
                            && !self.first_dispatch.selected)
                            || self.stage != 5 + point_index(point)
                        {
                            return corrupt();
                        }
                        let h = self
                            .journal_header
                            .as_ref()
                            .ok_or(PermissionError::CorruptRecord)?;
                        admit_composite_key(
                            self.legacy.budget,
                            14,
                            &[id, &h.sequence.to_be_bytes()],
                        )?;
                    }
                    PermissionPopulatedSql::CommitRead => {
                        let completed_stage = if self.first_dispatch.selected { 9 } else { 8 };
                        if self.stage != completed_stage
                            || !self.evidence_done
                            || self.commit_admitted
                        {
                            return corrupt();
                        }
                        PermissionResourceRead {
                            budget: &mut *self.legacy.budget,
                        }
                        .before_sql(PermissionResourceSql::CommitRead, None)?;
                        self.commit_admitted = true;
                        return Ok(());
                    }
                }
                PermissionResourceRead {
                    budget: &mut *self.legacy.budget,
                }
                .before_sql(PermissionResourceSql::Metadata, None)
            })();
            self.legacy.budget.finish(result)
        }
        pub fn before_row(&mut self) -> Result<(), PermissionError> {
            self.legacy.before_row()
        }
        pub fn capture_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                let h = self.headers[0]
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?;
                if self.stage != 0 || raw.len() != h.bytes {
                    return corrupt();
                }
                let (a, q) = decode_acceptance(raw, self.legacy.budget)?;
                if a.turn_id.as_str() != h.primary
                    || Some(a.before_snapshot.revision) != h.previous
                    || Some(a.committed_revision) != h.committed
                    || a.thread_id != self.legacy.resource.thread.id
                    || a.thread_generation != self.legacy.resource.thread.generation
                    || a.before_snapshot.execution_id
                        != self.legacy.resource.execution.snapshot.execution_id
                {
                    return corrupt();
                }
                let limits = self.original_always_limits();
                if !bounded(q.root, limits.acceptance, limits.acceptance_nodes, 64)
                    || !bounded(
                        q.inherent.before.ok_or(PermissionError::CorruptRecord)?,
                        limits.before,
                        limits.before_nodes,
                        64,
                    )
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                single_push(&mut self.acceptances, a)?;
                self.facts.a = q.root;
                self.facts.before = q.inherent.before.ok_or(PermissionError::CorruptRecord)?;
                self.facts.a_scope = q.inherent.scope.ok_or(PermissionError::CorruptRecord)?;
                self.stage = 1;
                Ok(())
            })();
            self.legacy.budget.finish(result)
        }
        pub fn capture_decision_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                let h = self.headers[1]
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?;
                if self.stage != 1 || raw.len() != h.bytes {
                    return corrupt();
                }
                let (d, q) = decode_decision(raw, self.legacy.budget)?;
                if d.permission_id.as_str() != h.primary
                    || Some(d.scope_digest.as_str()) != h.digest.as_deref()
                    || d.scope.thread_id != self.legacy.resource.thread.id
                    || d.scope.thread_generation != self.legacy.resource.thread.generation
                {
                    return corrupt();
                }
                if !bounded(q.root, self.original_always_limits().decision, 67, 64) {
                    return Err(PermissionError::UnsupportedProfile);
                }
                single_push(&mut self.decisions, d)?;
                self.facts.d = q.root;
                self.facts.d_scope = q.inherent.scope.ok_or(PermissionError::CorruptRecord)?;
                self.stage = 2;
                Ok(())
            })();
            self.legacy.budget.finish(result)
        }

        pub fn admit_grant_header(
            &mut self,
            cells: PermissionGrantHeaderCells<'_>,
        ) -> Result<String, PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                if self.stage != 3 {
                    return corrupt();
                }
                self.legacy.budget.reserve(8_192, 32)?;
                if self.legacy.grants_done || self.legacy.grant_header.is_some() {
                    return corrupt();
                }
                if !self.legacy.grants.is_empty() {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.legacy.grants.len() == 64 {
                    return self.legacy.budget.limit();
                }
                let execution = opaque(cells.execution_id)?;
                let interrupt = opaque(cells.interrupt_id)?;
                let task = opaque(cells.task_id)?;
                let thread = opaque(cells.thread_id)?;
                let epoch = generation(cells.generation)?;
                let parking = resource_u64(cells.parking_revision)?;
                if execution != self.execution_id().as_str()
                    || thread != self.thread_id().as_str()
                    || epoch != self.legacy.resource.thread.generation.as_str()
                    || parking == 0
                    || !matches!(cells.consumed, 0 | 1)
                    || cells.record_bytes == 0
                {
                    return corrupt();
                }
                if cells.record_bytes > 65_536 {
                    return self.legacy.budget.limit();
                }
                self.legacy.budget.reserve(cells.record_bytes, 1)?;
                let query = resource_string(interrupt.as_bytes(), self.legacy.budget)?;
                self.legacy.grant_header = Some(GrantHeader {
                    interrupt: resource_string(interrupt.as_bytes(), self.legacy.budget)?,
                    task: resource_string(task.as_bytes(), self.legacy.budget)?,
                    execution: resource_string(execution.as_bytes(), self.legacy.budget)?,
                    thread: resource_string(thread.as_bytes(), self.legacy.budget)?,
                    generation: resource_string(epoch.as_bytes(), self.legacy.budget)?,
                    parking,
                    consumed: cells.consumed == 1,
                    bytes: cells.record_bytes,
                });
                Ok(query)
            })();
            self.legacy.budget.finish(result).map_err(view_error)
        }
        pub fn capture_grant_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                let header = self
                    .legacy
                    .grant_header
                    .take()
                    .ok_or(PermissionError::CorruptRecord)?;
                if raw.len() != header.bytes {
                    return corrupt();
                }
                let (grant, root_q, prompt_q) = captured_grant(raw, self.legacy.budget)?;
                if !bounded(root_q, self.original_always_limits().grant, 83, 65) {
                    return Err(PermissionError::UnsupportedProfile);
                }
                self.facts.grant = root_q;
                self.facts.prompt = prompt_q;
                self.legacy.budget.reserve(8_192, 32)?;
                if grant.execution_id.as_str() != header.execution
                    || grant.interrupt_id.as_str() != header.interrupt
                    || grant.task_id.as_str() != header.task
                    || grant.thread_id.as_str() != header.thread
                    || grant.thread_generation.as_str() != header.generation
                    || grant.task_parking_revision != header.parking
                    || grant.consumed != header.consumed
                {
                    return corrupt();
                }
                // Pay a distinct quote for the retained primary-ID uniqueness scan.
                let k = self.legacy.grants.len();
                self.legacy.budget.reserve(mul(512, k)?, mul(2, k)?)?;
                let bytes = self
                    .legacy
                    .grants
                    .iter()
                    .map(|g| g.interrupt_id.as_str().len())
                    .try_fold(0, add)?;
                self.legacy.budget.reserve(
                    add(mul(k, grant.interrupt_id.as_str().len())?, bytes)?,
                    mul(2, k)?,
                )?;
                if self
                    .legacy
                    .grants
                    .iter()
                    .any(|g| g.interrupt_id == grant.interrupt_id)
                {
                    return corrupt();
                }
                admit_composite_key(
                    self.legacy.budget,
                    20,
                    &[
                        grant.execution_id.as_str().as_bytes(),
                        grant.interrupt_id.as_str().as_bytes(),
                    ],
                )?;
                if self.legacy.budget.resource.referenced_grants == 256 {
                    return self.legacy.budget.limit();
                }
                self.legacy.budget.resource.referenced_grants += 1;
                push_legacy(grant, &mut self.legacy.grants, 1_024, self.legacy.budget)
            })();
            self.legacy.budget.finish(result).map_err(view_error)
        }
        pub fn end_grant_range(&mut self) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(128, 1)?;
                if self.stage != 3 {
                    return corrupt();
                }
                if self.legacy.grants_done || self.legacy.grant_header.is_some() {
                    return corrupt();
                }
                if self.legacy.grants.len() != 1 {
                    return corrupt();
                }
                self.legacy.grants_done = true;
                self.stage = 4;
                Ok(())
            })();
            self.legacy.budget.finish(result).map_err(view_error)
        }
        pub fn admit_old_acceptance_header(
            &mut self,
            cells: PermissionAcceptanceHeaderCells<'_>,
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(8_192, 32)?;
                if self.stage != 4 {
                    return corrupt();
                }
                let execution = opaque(cells.execution_id)?;
                let _ = opaque(cells.turn_id)?;
                let thread = opaque(cells.thread_id)?;
                let epoch = generation(cells.generation)?;
                let _ = opaque(cells.definition_revision)?;
                let _ = resource_u64(cells.previous_revision)?;
                let _ = resource_u64(cells.accepted_at_ms)?;
                if execution != self.execution_id().as_str()
                    || thread != self.thread_id().as_str()
                    || epoch != self.legacy.resource.thread.generation.as_str()
                    || cells.record_bytes == 0
                {
                    return corrupt();
                }
                if cells.record_bytes > 65_536 {
                    return self.legacy.budget.limit();
                }
                Err(PermissionError::UnsupportedProfile)
            })();
            self.legacy.budget.finish(result)
        }
        pub fn end_old_acceptance_range(&mut self) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(128, 1)?;
                if self.stage != 4 {
                    return corrupt();
                }
                self.legacy.acceptances_done = true;
                self.stage = 5;
                Ok(())
            })();
            self.legacy.budget.finish(result)
        }
        pub fn admit_journal_header(
            &mut self,
            point: PermissionJournalPoint,
            cells: PermissionProjectionJournalHeaderCells<'_>,
        ) -> Result<(i64, usize), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                let index = point_index(point);
                if (index == 2 && !self.first_dispatch.selected)
                    || self.stage != 5 + index
                    || self.journal_rows != 0
                    || self.journal_header.is_some()
                {
                    return corrupt();
                }
                self.journal_rows += 1;
                let execution = opaque(cells.execution_id)?;
                let thread = opaque(cells.thread_id)?;
                let revision = resource_u64(cells.revision)?;
                let owner = resource_u64(cells.owner)?;
                let time = resource_u64(cells.time)?;
                let status: E = serde_json::from_slice(cells.status)
                    .map_err(|_| PermissionError::CorruptRecord)?;
                let a = self
                    .acceptances
                    .first()
                    .ok_or(PermissionError::CorruptRecord)?;
                if self.first_dispatch.selected {
                    let current = &self.legacy.resource.execution.snapshot;
                    let (expected_revision, expected_owner, expected_status, expected_time) =
                        match point {
                            PermissionJournalPoint::Park => (
                                a.before_snapshot.revision,
                                a.before_snapshot.owner_generation,
                                a.before_snapshot.status,
                                a.before_snapshot.updated_at_ms,
                            ),
                            PermissionJournalPoint::Resume => (
                                a.committed_revision,
                                a.before_snapshot.owner_generation,
                                E::Running,
                                a.facts.now_ms,
                            ),
                            PermissionJournalPoint::Dispatch => (
                                current.revision,
                                current.owner_generation,
                                current.status,
                                current.updated_at_ms,
                            ),
                        };
                    if cells.sequence <= 0
                        || cells.kind != b"commit"
                        || execution != a.before_snapshot.execution_id.as_str()
                        || thread != self.thread_id().as_str()
                        || revision != expected_revision
                        || owner != expected_owner
                        || time != expected_time
                        || status != expected_status
                        || cells.payload_bytes == 0
                    {
                        return corrupt();
                    }
                } else {
                    let expected = match point {
                        PermissionJournalPoint::Park => &a.before_snapshot,
                        PermissionJournalPoint::Resume => &self.legacy.resource.execution.snapshot,
                        PermissionJournalPoint::Dispatch => return corrupt(),
                    };
                    if cells.sequence <= 0
                        || cells.kind != b"commit"
                        || execution != expected.execution_id.as_str()
                        || thread != self.thread_id().as_str()
                        || revision != expected.revision
                        || owner != expected.owner_generation
                        || time != expected.updated_at_ms
                        || status != expected.status
                        || cells.payload_bytes == 0
                    {
                        return corrupt();
                    }
                }
                if cells.payload_bytes > 1_024 {
                    return self.legacy.budget.limit();
                }
                if self.first_dispatch.selected {
                    match point {
                        PermissionJournalPoint::Park => {
                            self.first_dispatch.sequences[0] = cells.sequence
                        }
                        PermissionJournalPoint::Resume => {
                            self.first_dispatch.sequences[1] = cells.sequence
                        }
                        PermissionJournalPoint::Dispatch => {
                            self.first_dispatch.sequences[2] = cells.sequence
                        }
                    }
                }
                self.journal_header = Some(JournalHeader {
                    sequence: cells.sequence,
                    revision,
                    owner,
                    status,
                    time,
                    bytes: cells.payload_bytes,
                });
                Ok((cells.sequence, cells.payload_bytes))
            })();
            self.legacy.budget.finish(result)
        }
        pub fn capture_journal_payload(
            &mut self,
            point: PermissionJournalPoint,
            raw: &[u8],
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(0, 0)?;
                let index = point_index(point);
                let h = self
                    .journal_header
                    .take()
                    .ok_or(PermissionError::CorruptRecord)?;
                if (index == 2 && !self.first_dispatch.selected)
                    || self.stage != 5 + index
                    || self.journals.len() != index
                    || raw.len() != h.bytes
                {
                    return corrupt();
                }
                // Guarded <=1024 JSON of a closed five-field typed journal.
                // The prepaid J lifecycle covers serde/error/UTF8 work and the
                // no-frame closed canonical width calculation, not a replay.
                let journal: crate::JournalEntry =
                    serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
                let q = writer_journal_measure(&journal)?;
                if !bounded(q, self.original_always_limits().journal, 11, 1) {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if journal.execution_id != *self.execution_id()
                    || journal.revision != h.revision
                    || journal.owner_generation != h.owner
                    || journal.status != h.status
                    || journal.updated_at_ms != h.time
                {
                    return corrupt();
                }
                self.journals
                    .try_reserve_exact(1)
                    .map_err(|_| PermissionError::LimitExceeded)?;
                self.journals.push(journal);
                match point {
                    PermissionJournalPoint::Park => self.facts.journals[0] = q,
                    PermissionJournalPoint::Resume => self.facts.journals[1] = q,
                    PermissionJournalPoint::Dispatch => self.first_dispatch.dispatch_q = q,
                }
                Ok(())
            })();
            self.legacy.budget.finish(result)
        }
        pub fn end_journal_range(
            &mut self,
            point: PermissionJournalPoint,
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.legacy.budget.reserve(128, 1)?;
                let index = point_index(point);
                if (index == 2 && !self.first_dispatch.selected)
                    || self.stage != 5 + index
                    || self.journal_rows != 1
                    || self.journal_header.is_some()
                    || self.journals.len() != index + 1
                {
                    return corrupt();
                }
                self.journal_rows = 0;
                self.stage += 1;
                Ok(())
            })();
            self.legacy.budget.finish(result)
        }
        pub fn end_evidence(&mut self) -> Result<(), PermissionError> {
            let result = (|| {
                // Paid before graph loan, dimensional bounds and every logical
                // operand/field comparison. Actual retained facts come only from
                // this owner; no receipt or public quote reconstruction.
                self.legacy.budget.reserve(
                    65_536
                        + if self.first_dispatch.selected {
                            FIRST_READER_P4_BYTES
                        } else {
                            0
                        },
                    2_048
                        + if self.first_dispatch.selected {
                            FIRST_READER_P4_NODES
                        } else {
                            0
                        },
                )?;
                if self.first_dispatch.selected {
                    if self.stage != 8 || self.evidence_done {
                        return corrupt();
                    }
                    historical_first_dispatch(self)?;
                    self.evidence_done = true;
                    self.stage = 9;
                } else {
                    if self.stage != 7 || self.evidence_done {
                        return corrupt();
                    }
                    // A embeds before; the paired wrapped scopes retain their
                    // own measured facts. Prepay normal graph/owner work before
                    // lending the immutable full historical graph.
                    self.legacy.budget.reserve_normal_original_work(
                        &[
                            self.facts.snapshot,
                            self.facts.a,
                            self.facts.before,
                            self.facts.d,
                            self.facts.a_scope,
                            self.facts.d_scope,
                            self.facts.grant,
                            self.facts.prompt,
                            self.facts.journals[0],
                            self.facts.journals[1],
                        ],
                        4,
                        2,
                    )?;
                    historical_graph(self)?;
                    self.evidence_done = true;
                    self.stage = 8;
                }
                Ok(())
            })();
            self.legacy.budget.finish(result)
        }
        /// Trusted backend calls only after its actual read COMMIT succeeds.
        pub fn finish_after_commit(self) -> Result<super::super::PermissionView, PermissionError> {
            self.legacy.budget.reserve(32_768, 512)?;
            let Self {
                legacy,
                acceptances,
                decisions,
                journals,
                evidence_done,
                commit_admitted,
                stage,
                first_dispatch,
                ..
            } = self;
            let PermissionLegacyViewRead {
                budget,
                selector,
                resource,
                grants,
                ..
            } = legacy;
            let result = (|| {
                // All result/factory/receipt/tag transfers and cleanup are paid
                // before this first complete projection is moved into View.
                let completed_stage = if first_dispatch.selected { 9 } else { 8 };
                if !evidence_done || !commit_admitted || stage != completed_stage {
                    return corrupt();
                }
                Ok(Finished {
                    resource,
                    grants,
                    acceptances,
                    decisions,
                    journals,
                    purpose: selector.purpose(),
                    usage: budget.usage(),
                })
            })();
            let finished = budget.finish(result)?;
            Ok(super::super::PermissionView::from_checked_populated(
                finished,
            ))
        }
    }
    fn point_index(point: PermissionJournalPoint) -> usize {
        match point {
            PermissionJournalPoint::Park => 0,
            PermissionJournalPoint::Resume => 1,
            PermissionJournalPoint::Dispatch => 2,
        }
    }
    fn captured_hash(
        value: CanonicalValue<'_>,
        limit: CanonicalLimit,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(String, CanonicalMeasure), PermissionError> {
        let measured = measure(value, limit, budget)?;
        budget.reserve(measured.bytes, measured.nodes)?;
        let mut output = HashWriter(<sha2::Sha256 as sha2::Digest>::new());
        let emitted = walk(value, limit, &mut output, budget, false)?;
        if emitted.bytes != measured.bytes
            || emitted.nodes != measured.nodes
            || emitted.depth != measured.depth
        {
            return Err(PermissionError::CorruptRecord);
        }
        budget.reserve(96, 0)?;
        let digest = sha2::Digest::finalize(output.0);
        let mut result = String::new();
        result
            .try_reserve_exact(64)
            .map_err(|_| PermissionError::LimitExceeded)?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in digest {
            result.push(char::from(HEX[usize::from(byte >> 4)]));
            result.push(char::from(HEX[usize::from(byte & 15)]));
        }
        Ok((result, measured))
    }
    fn captured_inherent(
        record: BorrowedRecord<'_>,
        measured: &CanonicalMeasure,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<InherentFacts, PermissionError> {
        let mut before_q = None;
        let mut scope_q = None;
        // Pay the root/shallow counting/label/selection scans before any such scans.
        reserve_measure(*measured, budget)?;
        match record {
            BorrowedRecord::Decision(value) => {
                if value.record_version != 1 {
                    return corrupt();
                }
                let scope = &value.scope;
                labels(
                    &scope.application,
                    &scope.policy_revision,
                    Some(&scope.origin),
                )?;
                let d = checked_definition(&scope.definition, budget)?;
                budget.reserve(mul(512, d)?, mul(2, d)?)?;
                if scope.route != scope.definition.route
                    || !scope
                        .definition
                        .batches
                        .iter()
                        .flatten()
                        .any(|spec| spec.effect_id == scope.effect_id && spec.requires_approval)
                {
                    return corrupt();
                }
                let (digest, q) = captured_hash(
                    CanonicalValue::Scope(ScopeParts::from_scope(scope)),
                    CanonicalLimit::Evidence64KiB,
                    budget,
                )?;
                scope_q = Some(q);
                budget.reserve(512, 8)?;
                if !digest_shape(&value.scope_digest)
                    || digest != value.scope_digest
                    || value.permission_id.as_str().strip_prefix("b4pd_") != Some(digest.as_str())
                {
                    return corrupt();
                }
            }
            BorrowedRecord::Acceptance(value) => {
                use b4_contract::{ExecutionStatus as E, TaskStatus as T};
                let before = &value.before_snapshot;
                labels(
                    &value.application,
                    &value.policy_revision,
                    Some(&value.origin),
                )?;
                if value.record_version != 1
                    || before.status != E::WaitingForApproval
                    || before.failure.is_some()
                {
                    return corrupt();
                }
                let count = checked_definition(&before.definition, budget)?;
                before_q = Some(checked_snapshot_profile_capture(
                    before,
                    count,
                    budget,
                    CanonicalLimit::Evidence64KiB,
                    false,
                )?);
                let revision = before
                    .revision
                    .checked_add(1)
                    .ok_or(PermissionError::CorruptRecord)?;
                budget.reserve(
                    mul(512, add(before.turn_history.len(), 1)?)?,
                    mul(2, add(before.turn_history.len(), 1)?)?,
                )?;
                if value.committed_revision != revision
                    || value.facts.now_ms < before.updated_at_ms
                    || value.turn_id == before.turn_id
                    || before.turn_history.contains(&value.turn_id)
                    || value.decisions.is_empty()
                    || value.decisions.len() > 64
                {
                    return corrupt();
                }
                // Eight conservative 512-byte pair units cover repeated typed-ID,
                // binding, digest shape/equality and permission suffix comparisons.
                budget.reserve(
                    mul(4096, value.decisions.len())?,
                    mul(32, add(before.tasks.len(), value.decisions.len())?)?,
                )?;
                let waiting = before
                    .tasks
                    .iter()
                    .filter(|task| task.status == T::WaitingForApproval);
                if waiting.clone().count() != value.decisions.len() {
                    return corrupt();
                }
                let mut always = false;
                for (task, entry) in waiting.zip(&value.decisions) {
                    let Some(interrupt) = &task.interrupt else {
                        return corrupt();
                    };
                    if entry.task_id != task.spec.id
                        || entry.interrupt_id != interrupt.id
                        || entry.grant_ref.execution_id != before.execution_id
                        || entry.grant_ref.interrupt_id != interrupt.id
                        || entry.task_parking_revision == 0
                        || entry.task_parking_revision > before.revision
                    {
                        return corrupt();
                    }
                    let proposal =
                        bounded_permission_proposal_digest(task, &interrupt.prompt, budget)?;
                    if !digest_shape(&entry.proposal_digest) || proposal != entry.proposal_digest {
                        return corrupt();
                    }
                    match entry.decision {
                        ApprovalDecision::Deny => {
                            if entry.binding.is_some() || entry.permission_id.is_some() {
                                return corrupt();
                            }
                        }
                        ApprovalDecision::Once | ApprovalDecision::Always => {
                            let Some(binding) = &entry.binding else {
                                return corrupt();
                            };
                            if binding.effect_id != task.spec.effect_id {
                                return corrupt();
                            }
                            if entry.decision == ApprovalDecision::Once {
                                if entry.permission_id.is_some() {
                                    return corrupt();
                                }
                            } else {
                                always = true;
                                let parts = ScopeParts {
                                    thread_id: &value.thread_id,
                                    thread_generation: &value.thread_generation,
                                    origin: &value.origin,
                                    application: &value.application,
                                    route: &before.definition.route,
                                    definition: &before.definition,
                                    effect_id: &binding.effect_id,
                                    tool_id: &binding.tool_id,
                                    tool_revision: &binding.tool_revision,
                                    policy_revision: &value.policy_revision,
                                    constraint_revision: &binding.constraint_revision,
                                };
                                let (digest, q) = captured_hash(
                                    CanonicalValue::Scope(parts),
                                    CanonicalLimit::Evidence64KiB,
                                    budget,
                                )?;
                                scope_q = Some(q);
                                if entry
                                    .permission_id
                                    .as_ref()
                                    .map(|id| id.as_str().strip_prefix("b4pd_"))
                                    != Some(Some(digest.as_str()))
                                {
                                    return corrupt();
                                }
                            }
                        }
                    }
                }
                if !always {
                    return corrupt();
                }
                // Borrowed Resume derivation: complete exact-set decisions make each
                // waiting record Ready/approved or Denied/unapproved, retaining its
                // actual interrupt/spec/input/history. At least one Always keeps
                // this batch Ready, so settle cannot reduce/advance/clone output.
                // Fresh turn, incremented revision and checked time were verified.
                // No owned command, proposed snapshot or dispatchable vector is built.
                // At an exhausted attempt budget the derived dispatchable set is
                // empty even though the approved Ready records remain legal.
            }
            BorrowedRecord::Use(_) => return Err(PermissionError::UnsupportedProfile),
        }
        Ok(InherentFacts {
            before: before_q,
            scope: scope_q,
        })
    }
    fn captured_grant(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<
        (
            crate::approval::GrantRecord,
            CanonicalMeasure,
            CanonicalMeasure,
        ),
        PermissionError,
    > {
        preflight_profile(raw, RawProfile::Legacy, budget)?;
        let grant: crate::approval::GrantRecord =
            serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
        let measured = measure(
            CanonicalValue::LegacyGrant(&grant),
            CanonicalLimit::LegacyRecord,
            budget,
        )?;
        reserve_measure(measured, budget)?;
        // Complete finite label, digest, TTL, SHA69 and verifier comparison allowance.
        budget.reserve(32_768, 128)?;
        if !valid_label(&grant.origin.application)
            || !valid_label(&grant.origin.tenant)
            || !valid_label(&grant.origin.principal)
            || !valid_label(&grant.policy_revision)
            || grant.task_parking_revision == 0
            || !digest_shape(&grant.proposal_digest)
            || !digest_shape(&grant.resume_key)
        {
            return corrupt();
        }
        let ttl = grant
            .expires_at_ms
            .checked_sub(grant.issued_at_ms)
            .ok_or(PermissionError::CorruptRecord)?;
        if ttl == 0 || ttl > crate::approval::MAX_APPROVAL_TTL_MS {
            return corrupt();
        }
        let verifier = <sha2::Sha256 as sha2::Digest>::digest(grant.token.as_str().as_bytes());
        let difference = grant
            .verifier
            .iter()
            .zip(verifier.iter())
            .fold(0u8, |difference, (left, right)| difference | (left ^ right));
        if difference != 0 {
            return corrupt();
        }
        // Old prompt depth64 counts scalar leaves as zero. Canonical byte length
        // matches pinned serde without its old full allocation/sort construction.
        let prompt_q = measure(
            CanonicalValue::Json(&grant.prompt),
            CanonicalLimit::LegacyPrompt,
            budget,
        )?;
        Ok((grant, measured, prompt_q))
    }
    // This loan contains only the exact values and facts produced by one owner.
    // Historical proof confers no present tool/constraint or dispatch authority.
    fn historical_graph(
        read: &PermissionPopulatedViewRead<'_, '_, '_>,
    ) -> Result<(), PermissionError> {
        let q = &read.facts;
        let limits = read.original_always_limits();
        if !bounded(q.snapshot, limits.resume, limits.resume_nodes, 127)
            || !bounded(q.a, limits.acceptance, limits.acceptance_nodes, 64)
            || !bounded(q.before, limits.before, limits.before_nodes, 64)
            || !bounded(q.d, limits.decision, 67, 64)
            || !bounded(q.a_scope, limits.scope + 22, 55, 64)
            || !bounded(q.d_scope, limits.scope + 22, 55, 64)
            || !bounded(q.grant, limits.grant, 83, 65)
            || !bounded(q.prompt, 128, 16, 64)
            || !bounded(q.journals[0], limits.journal, 11, 1)
            || !bounded(q.journals[1], limits.journal, 11, 1)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if read.acceptances.len() != 1
            || read.decisions.len() != 1
            || read.legacy.grants.len() != 1
            || !read.legacy.acceptances.is_empty()
            || read.journals.len() != 2
        {
            return corrupt();
        }
        let a = &read.acceptances[0];
        let d = &read.decisions[0];
        let g = &read.legacy.grants[0];
        let before = &a.before_snapshot;
        let next = &read.legacy.resource.execution.snapshot;
        if before.tasks.len() != 1
            || next.tasks.len() != 1
            || a.decisions.len() != 1
            || before.definition.batches.len() != 1
            || before.definition.batches[0].len() != 1
            || before.turn_history.len() > 2
            || !before.attempt_history.is_empty()
            || before.status != E::WaitingForApproval
            || before.failure.is_some()
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        // The closed arithmetic width helper scans only already paid immutable
        // scalars and constructs a borrowed Resume overlay, not a transition.
        let overlay = writer_resume_overlay_measure(before, a, q.before)?;
        let expected_park = snapshot_journal_measure(before)?;
        let expected_resume = snapshot_journal_measure(next)?;
        if !bounded(overlay, limits.resume, limits.resume_nodes, 127)
            || !bounded(expected_park, limits.journal, 11, 1)
            || !bounded(expected_resume, limits.journal, 11, 1)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let mut equality_bytes = 0;
        let mut equality_nodes = 0;
        for (left, right) in [
            (q.snapshot, overlay),
            (q.a_scope, q.d_scope),
            (q.a, q.a),
            (q.grant, q.grant),
            (q.prompt, q.before),
            (q.journals[0], expected_park),
            (q.journals[1], expected_resume),
        ] {
            equality_bytes = add(equality_bytes, add(left.bytes, right.bytes)?)?;
            equality_nodes = add(equality_nodes, add(left.nodes, right.nodes)?)?;
        }
        if equality_bytes > limits.equality || equality_nodes > limits.paired_equality_nodes {
            return Err(PermissionError::UnsupportedProfile);
        }
        let task = &before.tasks[0];
        let entry = &a.decisions[0];
        let pending = task
            .interrupt
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let binding = entry
            .binding
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let pid = entry
            .permission_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let origin = read
            .legacy
            .resource
            .thread
            .origin
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let active = read
            .legacy
            .resource
            .thread
            .active_execution_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let mut work = CoreWork { string_pairs: 0 };
        if entry.decision != ApprovalDecision::Always
            || task.status != T::WaitingForApproval
            || !task.spec.requires_approval
            || task.approved
            || task.attempt_id.is_some()
            || task.result.is_some()
            || task.failure.is_some()
            || !g.consumed
            || !work.same(a.thread_id.as_str(), read.thread_id().as_str())?
            || !work.same(
                a.thread_generation.as_str(),
                read.legacy.resource.thread.generation.as_str(),
            )?
            || !work.origin(&a.origin, origin)?
            || !work.same(
                a.application.application.as_str(),
                origin.application.as_str(),
            )?
            || !work.same(
                read.legacy.resource.execution.thread_id.as_str(),
                read.thread_id().as_str(),
            )?
            || !work.same(active.as_str(), before.execution_id.as_str())?
            || !work.same(
                d.creator_execution_id.as_str(),
                before.execution_id.as_str(),
            )?
            || !work.same(d.creator_turn_id.as_str(), a.turn_id.as_str())?
            || !work.same(d.creator_task_id.as_str(), entry.task_id.as_str())?
            || !work.same(d.creator_interrupt_id.as_str(), entry.interrupt_id.as_str())?
            || !work.same(d.permission_id.as_str(), pid.as_str())?
        {
            return corrupt();
        }
        if !work.same(g.thread_id.as_str(), a.thread_id.as_str())?
            || !work.same(g.thread_generation.as_str(), a.thread_generation.as_str())?
            || !work.origin(&g.origin, &a.origin)?
            || !work.same(g.execution_id.as_str(), before.execution_id.as_str())?
            || !work.same(g.route.as_str(), before.definition.route.as_str())?
            || !work.same(
                g.definition_revision.as_str(),
                before.definition.revision.as_str(),
            )?
            || !work.same(g.task_id.as_str(), task.spec.id.as_str())?
            || !work.same(g.effect_id.as_str(), task.spec.effect_id.as_str())?
            || !work.same(g.interrupt_id.as_str(), pending.id.as_str())?
            || !work.same(g.policy_revision.as_str(), a.policy_revision.as_str())?
            || !work.same(g.proposal_digest.as_str(), entry.proposal_digest.as_str())?
            || !work.same(
                entry.grant_ref.execution_id.as_str(),
                g.execution_id.as_str(),
            )?
            || !work.same(
                entry.grant_ref.interrupt_id.as_str(),
                g.interrupt_id.as_str(),
            )?
            || !work.same(entry.task_id.as_str(), g.task_id.as_str())?
            || !work.same(entry.interrupt_id.as_str(), g.interrupt_id.as_str())?
            || !work.same(binding.effect_id.as_str(), task.spec.effect_id.as_str())?
            || g.prompt != pending.prompt
            || entry.task_parking_revision != g.task_parking_revision
            || g.task_parking_revision != before.revision
            || g.issued_at_ms != before.updated_at_ms
            || g.issued_at_ms > a.facts.now_ms
            || a.facts.now_ms >= g.expires_at_ms
        {
            return corrupt();
        }
        // Historical bindings are checked against the actual stored A intent.
        // The ordinary selector carries no current tool registration or gate.
        let parts = ScopeParts {
            thread_id: &a.thread_id,
            thread_generation: &a.thread_generation,
            origin: &a.origin,
            application: &a.application,
            route: &before.definition.route,
            definition: &before.definition,
            effect_id: &binding.effect_id,
            tool_id: &binding.tool_id,
            tool_revision: &binding.tool_revision,
            policy_revision: &a.policy_revision,
            constraint_revision: &binding.constraint_revision,
        };
        if !same_scope(&d.scope, parts)
            || !same_resume(before, a, next, &mut work)?
            || !same_journal(&read.journals[0], before, &mut work)?
            || !same_journal(&read.journals[1], next, &mut work)?
        {
            return corrupt();
        }
        // Complete logical creator/reference collections alias the WHOLE selected
        // singleton sets. Their paid full operands are included above even when
        // the storage objects are identical. No external-only partition is exposed.
        let creators = read.acceptances.as_slice();
        let referenced = read.legacy.grants.as_slice();
        if creators.len() != 1 || referenced.len() != 1 || creators[0] != *a || referenced[0] != *g
        {
            return corrupt();
        }
        Ok(())
    }

    fn historical_first_dispatch(
        read: &PermissionPopulatedViewRead<'_, '_, '_>,
    ) -> Result<(), PermissionError> {
        let q = &read.facts;
        if !bounded(q.snapshot, 2_048, 100, 127)
            || !bounded(q.a, 1_766, 154, 64)
            || !bounded(q.before, 848, 91, 64)
            || !bounded(q.d, 998, 67, 64)
            || !bounded(q.a_scope, 670, 55, 64)
            || !bounded(q.d_scope, 670, 55, 64)
            || !bounded(q.grant, 962, 83, 65)
            || !bounded(q.prompt, 128, 16, 64)
            || !bounded(q.journals[0], 160, 11, 1)
            || !bounded(q.journals[1], 160, 11, 1)
            || !bounded(read.first_dispatch.dispatch_q, 160, 11, 1)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if read.acceptances.len() != 1
            || read.decisions.len() != 1
            || read.legacy.grants.len() != 1
            || !read.legacy.acceptances.is_empty()
            || read.journals.len() != 3
        {
            return corrupt();
        }
        let a = &read.acceptances[0];
        let d = &read.decisions[0];
        let g = &read.legacy.grants[0];
        let before = &a.before_snapshot;
        let next = &read.legacy.resource.execution.snapshot;
        if before.tasks.len() != 1
            || next.tasks.len() != 1
            || a.decisions.len() != 1
            || before.definition.batches.len() != 1
            || before.definition.batches[0].len() != 1
            || before.turn_history.len() > 2
            || !before.attempt_history.is_empty()
            || before.status != E::WaitingForApproval
            || before.failure.is_some()
            || before.output.is_some()
            || before.batch_index != 0
            || before.attempts_used != 0
            || next.attempt_history.len() != 1
            || next.batch_index != 0
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        // The closed arithmetic width helper scans only already paid immutable
        // scalars and constructs a borrowed Resume overlay, not a transition.
        let overlay = first_dispatch_overlay_measure(before, a, next, q.before)?;
        let expected_park = snapshot_journal_measure(before)?;
        let expected_resume = historical_resume_journal_measure(a)?;
        let expected_dispatch = snapshot_journal_measure(next)?;
        if !bounded(overlay, 2_048, 100, 127)
            || !bounded(expected_park, 160, 11, 1)
            || !bounded(expected_resume, 160, 11, 1)
            || !bounded(expected_dispatch, 160, 11, 1)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let mut equality_bytes = 0;
        let mut equality_nodes = 0;
        for (left, right) in [
            (q.snapshot, overlay),
            (q.a_scope, q.d_scope),
            (q.a, q.a),
            (q.grant, q.grant),
            (q.prompt, q.before),
            (q.journals[0], expected_park),
            (q.journals[1], expected_resume),
            (read.first_dispatch.dispatch_q, expected_dispatch),
        ] {
            equality_bytes = add(equality_bytes, add(left.bytes, right.bytes)?)?;
            equality_nodes = add(equality_nodes, add(left.nodes, right.nodes)?)?;
        }
        if equality_bytes > 16_496 || equality_nodes > 1_230 {
            return Err(PermissionError::UnsupportedProfile);
        }
        let task = &before.tasks[0];
        let entry = &a.decisions[0];
        let pending = task
            .interrupt
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let binding = entry
            .binding
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let pid = entry
            .permission_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let origin = read
            .legacy
            .resource
            .thread
            .origin
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let active = read
            .legacy
            .resource
            .thread
            .active_execution_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let mut work = CoreWork { string_pairs: 0 };
        if entry.decision != ApprovalDecision::Always
            || task.status != T::WaitingForApproval
            || !task.spec.requires_approval
            || task.approved
            || task.attempt_id.is_some()
            || task.result.is_some()
            || task.failure.is_some()
            || !g.consumed
            || !work.same(a.thread_id.as_str(), read.thread_id().as_str())?
            || !work.same(
                a.thread_generation.as_str(),
                read.legacy.resource.thread.generation.as_str(),
            )?
            || !work.origin(&a.origin, origin)?
            || !work.same(
                a.application.application.as_str(),
                origin.application.as_str(),
            )?
            || !work.same(
                read.legacy.resource.execution.thread_id.as_str(),
                read.thread_id().as_str(),
            )?
            || !work.same(active.as_str(), before.execution_id.as_str())?
            || !work.same(
                d.creator_execution_id.as_str(),
                before.execution_id.as_str(),
            )?
            || !work.same(d.creator_turn_id.as_str(), a.turn_id.as_str())?
            || !work.same(d.creator_task_id.as_str(), entry.task_id.as_str())?
            || !work.same(d.creator_interrupt_id.as_str(), entry.interrupt_id.as_str())?
            || !work.same(d.permission_id.as_str(), pid.as_str())?
        {
            return corrupt();
        }
        if !work.same(g.thread_id.as_str(), a.thread_id.as_str())?
            || !work.same(g.thread_generation.as_str(), a.thread_generation.as_str())?
            || !work.origin(&g.origin, &a.origin)?
            || !work.same(g.execution_id.as_str(), before.execution_id.as_str())?
            || !work.same(g.route.as_str(), before.definition.route.as_str())?
            || !work.same(
                g.definition_revision.as_str(),
                before.definition.revision.as_str(),
            )?
            || !work.same(g.task_id.as_str(), task.spec.id.as_str())?
            || !work.same(g.effect_id.as_str(), task.spec.effect_id.as_str())?
            || !work.same(g.interrupt_id.as_str(), pending.id.as_str())?
            || !work.same(g.policy_revision.as_str(), a.policy_revision.as_str())?
            || !work.same(g.proposal_digest.as_str(), entry.proposal_digest.as_str())?
            || !work.same(
                entry.grant_ref.execution_id.as_str(),
                g.execution_id.as_str(),
            )?
            || !work.same(
                entry.grant_ref.interrupt_id.as_str(),
                g.interrupt_id.as_str(),
            )?
            || !work.same(entry.task_id.as_str(), g.task_id.as_str())?
            || !work.same(entry.interrupt_id.as_str(), g.interrupt_id.as_str())?
            || !work.same(binding.effect_id.as_str(), task.spec.effect_id.as_str())?
            || g.prompt != pending.prompt
            || entry.task_parking_revision != g.task_parking_revision
            || g.task_parking_revision != before.revision
            || g.issued_at_ms != before.updated_at_ms
            || g.issued_at_ms > a.facts.now_ms
            || a.facts.now_ms >= g.expires_at_ms
        {
            return corrupt();
        }
        // Historical bindings are checked against the actual stored A intent.
        // The ordinary selector carries no current tool registration or gate.
        let parts = ScopeParts {
            thread_id: &a.thread_id,
            thread_generation: &a.thread_generation,
            origin: &a.origin,
            application: &a.application,
            route: &before.definition.route,
            definition: &before.definition,
            effect_id: &binding.effect_id,
            tool_id: &binding.tool_id,
            tool_revision: &binding.tool_revision,
            policy_revision: &a.policy_revision,
            constraint_revision: &binding.constraint_revision,
        };
        if !same_scope(&d.scope, parts)
            || !same_first_dispatch(before, a, next, &mut work)?
            || !same_journal(&read.journals[0], before, &mut work)?
            || !same_historical_resume_journal(&read.journals[1], a, &mut work)?
            || !same_journal(&read.journals[2], next, &mut work)?
            || read.first_dispatch.sequences[0] <= 0
            || read.first_dispatch.sequences[0] >= read.first_dispatch.sequences[1]
            || read.first_dispatch.sequences[1] >= read.first_dispatch.sequences[2]
        {
            return corrupt();
        }
        // Complete logical creator/reference collections alias the WHOLE selected
        // singleton sets. Their paid full operands are included above even when
        // the storage objects are identical. No external-only partition is exposed.
        let creators = read.acceptances.as_slice();
        let referenced = read.legacy.grants.as_slice();
        if creators.len() != 1 || referenced.len() != 1 || creators[0] != *a || referenced[0] != *g
        {
            return corrupt();
        }
        Ok(())
    }

    fn same_first_dispatch(
        before: &Snapshot,
        acceptance: &PermissionAcceptance,
        next: &Snapshot,
        work: &mut CoreWork,
    ) -> Result<bool, PermissionError> {
        let task = &before.tasks[0];
        let next_task = &next.tasks[0];
        let resume_revision = before
            .revision
            .checked_add(1)
            .ok_or(PermissionError::CorruptRecord)?;
        let dispatch_revision = resume_revision
            .checked_add(1)
            .ok_or(PermissionError::CorruptRecord)?;
        let attempt = next_task
            .attempt_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let history = before.turn_history.len();
        // These are the exact Resume then first Dispatch field deltas. No
        // owning Snapshot overlay, Kernel command or transition is created.
        if next.schema_version != before.schema_version
            || acceptance.committed_revision != resume_revision
            || next.revision != dispatch_revision
            || !work.same(next.execution_id.as_str(), before.execution_id.as_str())?
            || !work.same(next.turn_id.as_str(), acceptance.turn_id.as_str())?
            || next.owner_generation != before.owner_generation
            || next.definition != before.definition
            || next.status != E::Running
            || next.batch_index != before.batch_index
            || next.values != before.values
            || !before.attempt_history.is_empty()
            || next.attempt_history.len() != 1
            || next.attempt_history.first() != Some(attempt)
            || before.attempts_used != 0
            || next.attempts_used != 1
            || before.attempts_used >= before.definition.max_attempts
            || next.updated_at_ms < acceptance.facts.now_ms
            || next.output != before.output
            || next.failure != before.failure
            || next.turn_history.len() != history + 1
            || next.turn_history[..history] != before.turn_history[..]
            || next.turn_history.last() != Some(&acceptance.turn_id)
            || next_task.spec != task.spec
            || next_task.status != T::InFlight
            || !next_task.approved
            || next_task.input != task.input
            || task.attempt_id.is_some()
            || next_task.result != task.result
            || next_task.failure != task.failure
            || next_task.interrupt != task.interrupt
        {
            return Ok(false);
        }
        Ok(true)
    }
    fn same_historical_resume_journal(
        journal: &crate::JournalEntry,
        acceptance: &PermissionAcceptance,
        work: &mut CoreWork,
    ) -> Result<bool, PermissionError> {
        Ok(work.same(
            journal.execution_id.as_str(),
            acceptance.before_snapshot.execution_id.as_str(),
        )? && journal.revision == acceptance.committed_revision
            && journal.owner_generation == acceptance.before_snapshot.owner_generation
            && journal.status == E::Running
            && journal.updated_at_ms == acceptance.facts.now_ms)
    }
    fn historical_resume_journal_measure(
        acceptance: &PermissionAcceptance,
    ) -> Result<CanonicalMeasure, PermissionError> {
        let digits = |n: u64| {
            if n == 0 {
                1usize
            } else {
                n.ilog10() as usize + 1
            }
        };
        let fixed = b"{\"execution_id\":,\"owner_generation\":,\"revision\":,\"status\":,\"updated_at_ms\":}".len();
        Ok(CanonicalMeasure {
            bytes: add(
                add(
                    fixed,
                    writer_scalar_width(acceptance.before_snapshot.execution_id.as_str())?,
                )?,
                add(
                    add(
                        digits(acceptance.committed_revision),
                        digits(acceptance.before_snapshot.owner_generation),
                    )?,
                    add(
                        digits(acceptance.facts.now_ms),
                        add(2, execution_status(E::Running).len())?,
                    )?,
                )?,
            )?,
            nodes: 11,
            depth: 1,
        })
    }
    fn first_dispatch_overlay_measure(
        before: &Snapshot,
        acceptance: &PermissionAcceptance,
        next: &Snapshot,
        q: CanonicalMeasure,
    ) -> Result<CanonicalMeasure, PermissionError> {
        let digits = |n: u64| {
            if n == 0 {
                1usize
            } else {
                n.ilog10() as usize + 1
            }
        };
        let replace = |bytes: usize, old: usize, new: usize| {
            add(
                bytes
                    .checked_sub(old)
                    .ok_or(PermissionError::CorruptRecord)?,
                new,
            )
        };
        let ready = writer_resume_overlay_measure(before, acceptance, q)?;
        let attempt = next.tasks[0]
            .attempt_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let attempt_width = writer_scalar_width(attempt.as_str())?;
        let mut bytes = replace(
            ready.bytes,
            digits(acceptance.committed_revision),
            digits(next.revision),
        )?;
        bytes = replace(
            bytes,
            digits(acceptance.facts.now_ms),
            digits(next.updated_at_ms),
        )?;
        bytes = replace(bytes, "ready".len(), "in_flight".len())?;
        bytes = replace(bytes, 4, attempt_width)?;
        Ok(CanonicalMeasure {
            // Empty attempt history becomes exactly one quoted attempt; the
            // task's null attempt field becomes that same immutable ID.
            bytes: add(bytes, attempt_width)?,
            nodes: add(ready.nodes, 1)?,
            depth: ready.depth,
        })
    }

    struct CoreWork {
        string_pairs: usize,
    }
    impl CoreWork {
        fn same(&mut self, left: &str, right: &str) -> Result<bool, PermissionError> {
            if self.string_pairs == 64 || left.len() > 256 || right.len() > 256 {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.string_pairs += 1;
            Ok(left == right)
        }
        fn origin(
            &mut self,
            left: &ThreadOrigin,
            right: &ThreadOrigin,
        ) -> Result<bool, PermissionError> {
            Ok(self.same(&left.application, &right.application)?
                && self.same(&left.tenant, &right.tenant)?
                && self.same(&left.principal, &right.principal)?)
        }
    }

    fn same_scope(scope: &PermissionScope, parts: ScopeParts<'_>) -> bool {
        &scope.thread_id == parts.thread_id
            && &scope.thread_generation == parts.thread_generation
            && &scope.origin == parts.origin
            && &scope.application == parts.application
            && &scope.route == parts.route
            && &scope.definition == parts.definition
            && &scope.effect_id == parts.effect_id
            && &scope.tool_id == parts.tool_id
            && &scope.tool_revision == parts.tool_revision
            && scope.policy_revision.as_str() == parts.policy_revision
            && &scope.constraint_revision == parts.constraint_revision
    }
    fn same_resume(
        before: &Snapshot,
        acceptance: &PermissionAcceptance,
        next: &Snapshot,
        work: &mut CoreWork,
    ) -> Result<bool, PermissionError> {
        let task = &before.tasks[0];
        let next_task = &next.tasks[0];
        let revision = before
            .revision
            .checked_add(1)
            .ok_or(PermissionError::CorruptRecord)?;
        let history = before.turn_history.len();
        if next.schema_version != before.schema_version
            || next.revision != revision
            || acceptance.committed_revision != revision
            || !work.same(next.execution_id.as_str(), before.execution_id.as_str())?
            || !work.same(next.turn_id.as_str(), acceptance.turn_id.as_str())?
            || next.owner_generation != before.owner_generation
            || next.definition != before.definition
            || next.status != E::Running
            || next.batch_index != before.batch_index
            || next.values != before.values
            || next.attempt_history != before.attempt_history
            || next.attempts_used != before.attempts_used
            || next.updated_at_ms != acceptance.facts.now_ms
            || next.output != before.output
            || next.failure != before.failure
            || next.turn_history.len() != history + 1
            || next.turn_history[..history] != before.turn_history[..]
            || next.turn_history.last() != Some(&acceptance.turn_id)
            || next_task.spec != task.spec
            || next_task.status != T::Ready
            || !next_task.approved
            || next_task.input != task.input
            || next_task.attempt_id != task.attempt_id
            || next_task.result != task.result
            || next_task.failure != task.failure
            || next_task.interrupt != task.interrupt
        {
            return Ok(false);
        }
        // A read projects this legal Ready snapshot even if the attempt budget
        // is exhausted; no dispatchable vector or use authorization is inferred.
        Ok(true)
    }
    fn same_journal(
        journal: &crate::JournalEntry,
        snapshot: &Snapshot,
        work: &mut CoreWork,
    ) -> Result<bool, PermissionError> {
        Ok(work.same(
            journal.execution_id.as_str(),
            snapshot.execution_id.as_str(),
        )? && journal.revision == snapshot.revision
            && journal.owner_generation == snapshot.owner_generation
            && journal.status == snapshot.status
            && journal.updated_at_ms == snapshot.updated_at_ms)
    }
    fn snapshot_journal_measure(j: &Snapshot) -> Result<CanonicalMeasure, PermissionError> {
        let digits = |n: u64| {
            if n == 0 {
                1usize
            } else {
                n.ilog10() as usize + 1
            }
        };
        let fixed =
        b"{\"execution_id\":,\"owner_generation\":,\"revision\":,\"status\":,\"updated_at_ms\":}"
            .len();
        Ok(CanonicalMeasure {
            bytes: add(
                add(fixed, writer_scalar_width(j.execution_id.as_str())?)?,
                add(
                    add(digits(j.revision), digits(j.owner_generation))?,
                    add(
                        digits(j.updated_at_ms),
                        add(2, execution_status(j.status).len())?,
                    )?,
                )?,
            )?,
            nodes: 11,
            depth: 1,
        })
    }

    const _: () = assert!(std::mem::size_of::<FirstDispatchReaderMetadata>() <= 64);
    const _: () = assert!(std::mem::size_of::<JournalHeader>() <= 64);
    const _: () = assert!(std::mem::size_of::<Option<JournalHeader>>() <= 64);
    const _: () = assert!(std::mem::size_of::<&FirstDispatchReaderMetadata>() <= 8);
    const _: () = assert!(std::mem::size_of::<(bool, CanonicalMeasure, [i64; 3])>() <= 64);
    const _: () = assert!(std::mem::size_of::<PermissionPopulatedSql>() <= 16);
    const _: () = assert!(std::mem::size_of::<PermissionJournalPoint>() <= 8);
    const _: () = assert!(std::mem::size_of::<Result<bool, PermissionError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<Result<u64, PermissionError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<Result<(i64, usize), PermissionError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<(u64, u64, E, u64)>() <= 64);
    const _: () = assert!(std::mem::size_of::<Result<CanonicalMeasure, PermissionError>>() <= 128);
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, bool>,
        >() <= 128
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, u64>,
        >() <= 128
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                CanonicalMeasure,
            >,
        >() <= 128
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionProjectionBranch<'static, 'static, 'static>,
            >,
        >() <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionPopulatedViewRead<'static, 'static, 'static>,
            >,
        >() <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                super::super::PermissionView,
            >,
        >() <= 1_024
    );
    // Actual runtime facts use stable fully converted owners. All inherited
    // raw/shape/intrinsic/refusal work remains; only owner-local frames reuse.
    // Caller prepays A/D owned/result states and the bounded fact-array stores.
    #[allow(dead_code)]
    pub(in super::super) fn runtime_decode_acceptance(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(PermissionAcceptance, [CanonicalMeasure; 3]), PermissionError> {
        preflight(raw, RecordKind::Acceptance, budget)?;
        let wire = serde_json::from_slice::<AcceptanceWire>(raw)
            .map_err(|_| PermissionError::CorruptRecord)?;
        let value = finish_acceptance(wire, raw.len(), budget)?;
        let actual = {
            budget.reserve(0, 8)?;
            let mut workspace = CanonicalWorkspace::new(budget)?;
            let borrowed = BorrowedRecord::Acceptance(&value);
            let root = pair_measure(
                &mut workspace,
                CanonicalValue::Record(borrowed),
                CanonicalLimit::Record(RecordKind::Acceptance),
                budget,
            )?;
            let inherent = runtime_captured_inherent(&mut workspace, borrowed, &root, budget)?;
            budget.reserve(256, 8)?;
            let before = inherent.before.ok_or(PermissionError::CorruptRecord)?;
            let scope = inherent.scope.ok_or(PermissionError::CorruptRecord)?;
            [root, before, scope]
        };
        Ok((value, actual))
    }
    #[allow(dead_code)]
    pub(in super::super) fn runtime_decode_decision(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(PermissionDecision, [CanonicalMeasure; 2]), PermissionError> {
        preflight(raw, RecordKind::Decision, budget)?;
        let wire = serde_json::from_slice::<DecisionWire>(raw)
            .map_err(|_| PermissionError::CorruptRecord)?;
        let value = PermissionDecision::from(wire);
        let actual = {
            budget.reserve(0, 8)?;
            let mut workspace = CanonicalWorkspace::new(budget)?;
            let borrowed = BorrowedRecord::Decision(&value);
            let root = pair_measure(
                &mut workspace,
                CanonicalValue::Record(borrowed),
                CanonicalLimit::Record(RecordKind::Decision),
                budget,
            )?;
            let inherent = runtime_captured_inherent(&mut workspace, borrowed, &root, budget)?;
            budget.reserve(256, 8)?;
            [root, inherent.scope.ok_or(PermissionError::CorruptRecord)?]
        };
        Ok((value, actual))
    }
    // New A/D constructor carriers are separate from later fact-array charges.
    const _: () = assert!(
        65 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<CanonicalWorkspace<'static>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            + 4 * 128
            + 64
            <= 65 * 512
    );
    fn runtime_captured_hash<'data>(
        workspace: &mut CanonicalWorkspace<'data>,
        value: CanonicalValue<'data>,
        limit: CanonicalLimit,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(String, CanonicalMeasure), PermissionError> {
        let measured = pair_measure(workspace, value, limit, budget)?;
        budget.reserve(measured.bytes, measured.nodes)?;
        let mut output = HashWriter(<sha2::Sha256 as sha2::Digest>::new());
        let emitted = workspace.walk(value, limit, &mut output, budget, false)?;
        if emitted.bytes != measured.bytes
            || emitted.nodes != measured.nodes
            || emitted.depth != measured.depth
        {
            return Err(PermissionError::CorruptRecord);
        }
        budget.reserve(96, 0)?;
        let digest = sha2::Digest::finalize(output.0);
        // The new64-byte output is a distinct temporary owner. Its
        // failure/unique disposal is prepaid before construction.
        if budget.runtime_ready_cancellation_has_fixed_purchase() {
            budget.reserve(512, 1)?;
        }
        let mut result = String::new();
        result
            .try_reserve_exact(64)
            .map_err(runtime_checked_mandatory_callable(|_| {
                PermissionError::LimitExceeded
            }))?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in digest {
            result.push(char::from(HEX[usize::from(byte >> 4)]));
            result.push(char::from(HEX[usize::from(byte & 15)]));
        }
        Ok((result, measured))
    }
    fn runtime_captured_inherent<'data>(
        workspace: &mut CanonicalWorkspace<'data>,
        record: BorrowedRecord<'data>,
        measured: &CanonicalMeasure,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<InherentFacts, PermissionError> {
        let mut before_q = None;
        let mut scope_q = None;
        // Pay the root/shallow counting/label/selection scans before any such scans.
        reserve_measure(*measured, budget)?;
        match record {
            BorrowedRecord::Decision(value) => {
                if value.record_version != 1 {
                    return corrupt();
                }
                let scope = &value.scope;
                labels(
                    &scope.application,
                    &scope.policy_revision,
                    Some(&scope.origin),
                )?;
                let d = checked_pair_definition(workspace, &scope.definition, budget)?;
                budget.reserve(mul(512, d)?, mul(2, d)?)?;
                if scope.route != scope.definition.route
                    || !scope
                        .definition
                        .batches
                        .iter()
                        .flatten()
                        .any(|spec| spec.effect_id == scope.effect_id && spec.requires_approval)
                {
                    return corrupt();
                }
                let (digest, q) = runtime_captured_hash(
                    workspace,
                    CanonicalValue::Scope(ScopeParts::from_scope(scope)),
                    CanonicalLimit::Evidence64KiB,
                    budget,
                )?;
                scope_q = Some(q);
                budget.reserve(512, 8)?;
                if !digest_shape(&value.scope_digest)
                    || digest != value.scope_digest
                    || value.permission_id.as_str().strip_prefix("b4pd_") != Some(digest.as_str())
                {
                    return corrupt();
                }
            }
            BorrowedRecord::Acceptance(value) => {
                use b4_contract::{ExecutionStatus as E, TaskStatus as T};
                let before = &value.before_snapshot;
                labels(
                    &value.application,
                    &value.policy_revision,
                    Some(&value.origin),
                )?;
                if value.record_version != 1
                    || before.status != E::WaitingForApproval
                    || before.failure.is_some()
                {
                    return corrupt();
                }
                let count = checked_pair_definition(workspace, &before.definition, budget)?;
                before_q = Some(checked_pair_snapshot_capture(
                    workspace, before, count, budget,
                )?);
                let revision = before
                    .revision
                    .checked_add(1)
                    .ok_or(PermissionError::CorruptRecord)?;
                budget.reserve(
                    mul(512, add(before.turn_history.len(), 1)?)?,
                    mul(2, add(before.turn_history.len(), 1)?)?,
                )?;
                if value.committed_revision != revision
                    || value.facts.now_ms < before.updated_at_ms
                    || value.turn_id == before.turn_id
                    || before.turn_history.contains(&value.turn_id)
                    || value.decisions.is_empty()
                    || value.decisions.len() > 64
                {
                    return corrupt();
                }
                // Eight conservative 512-byte pair units cover repeated typed-ID,
                // binding, digest shape/equality and permission suffix comparisons.
                budget.reserve(
                    mul(4096, value.decisions.len())?,
                    mul(32, add(before.tasks.len(), value.decisions.len())?)?,
                )?;
                let waiting = before
                    .tasks
                    .iter()
                    .filter(|task| task.status == T::WaitingForApproval);
                if waiting.clone().count() != value.decisions.len() {
                    return corrupt();
                }
                let mut always = false;
                for (task, entry) in waiting.zip(&value.decisions) {
                    let Some(interrupt) = &task.interrupt else {
                        return corrupt();
                    };
                    if entry.task_id != task.spec.id
                        || entry.interrupt_id != interrupt.id
                        || entry.grant_ref.execution_id != before.execution_id
                        || entry.grant_ref.interrupt_id != interrupt.id
                        || entry.task_parking_revision == 0
                        || entry.task_parking_revision > before.revision
                    {
                        return corrupt();
                    }
                    let proposal = pair_permission_proposal_digest(
                        workspace,
                        task,
                        &interrupt.prompt,
                        budget,
                    )?;
                    if !digest_shape(&entry.proposal_digest) || proposal != entry.proposal_digest {
                        return corrupt();
                    }
                    match entry.decision {
                        ApprovalDecision::Deny => {
                            if entry.binding.is_some() || entry.permission_id.is_some() {
                                return corrupt();
                            }
                        }
                        ApprovalDecision::Once | ApprovalDecision::Always => {
                            let Some(binding) = &entry.binding else {
                                return corrupt();
                            };
                            if binding.effect_id != task.spec.effect_id {
                                return corrupt();
                            }
                            if entry.decision == ApprovalDecision::Once {
                                if entry.permission_id.is_some() {
                                    return corrupt();
                                }
                            } else {
                                always = true;
                                let parts = ScopeParts {
                                    thread_id: &value.thread_id,
                                    thread_generation: &value.thread_generation,
                                    origin: &value.origin,
                                    application: &value.application,
                                    route: &before.definition.route,
                                    definition: &before.definition,
                                    effect_id: &binding.effect_id,
                                    tool_id: &binding.tool_id,
                                    tool_revision: &binding.tool_revision,
                                    policy_revision: &value.policy_revision,
                                    constraint_revision: &binding.constraint_revision,
                                };
                                let (digest, q) = runtime_captured_hash(
                                    workspace,
                                    CanonicalValue::Scope(parts),
                                    CanonicalLimit::Evidence64KiB,
                                    budget,
                                )?;
                                scope_q = Some(q);
                                if entry
                                    .permission_id
                                    .as_ref()
                                    .map(|id| id.as_str().strip_prefix("b4pd_"))
                                    != Some(Some(digest.as_str()))
                                {
                                    return corrupt();
                                }
                            }
                        }
                    }
                }
                if !always {
                    return corrupt();
                }
                // Borrowed Resume derivation: complete exact-set decisions make each
                // waiting record Ready/approved or Denied/unapproved, retaining its
                // actual interrupt/spec/input/history. At least one Always keeps
                // this batch Ready, so settle cannot reduce/advance/clone output.
                // Fresh turn, incremented revision and checked time were verified.
                // No owned command, proposed snapshot or dispatchable vector is built.
                // At an exhausted attempt budget the derived dispatchable set is
                // empty even though the approved Ready records remain legal.
            }
            BorrowedRecord::Use(_) => return Err(PermissionError::UnsupportedProfile),
        }
        Ok(InherentFacts {
            before: before_q,
            scope: scope_q,
        })
    }

    const _: () = assert!(
        std::mem::size_of::<Result<(PermissionAcceptance, [CanonicalMeasure; 3]), PermissionError>>(
        ) <= std::mem::size_of::<Result<(PermissionAcceptance, RecordFacts), PermissionError>>()
    );
    const _: () = assert!(
        std::mem::size_of::<Result<(PermissionDecision, [CanonicalMeasure; 2]), PermissionError>>()
            <= std::mem::size_of::<Result<(PermissionDecision, RecordFacts), PermissionError>>()
    );

    // Direct lengths of the same immutable accepted owner; caller payment is separate.
    #[allow(dead_code)]
    pub(in super::super) fn runtime_human_acceptance_decisions_len(
        value: &PermissionAcceptance,
    ) -> usize {
        value.decisions.len()
    }

    #[allow(dead_code)]
    pub(in super::super) fn runtime_human_acceptance_policy_revision_len(
        value: &PermissionAcceptance,
    ) -> usize {
        value.policy_revision.len()
    }

    // Unused Human-only siblings: the future caller must prepay new fixed
    // tuple/Result/Flow and first-failed ingress before either call.
    #[allow(dead_code)]
    pub(in super::super) fn runtime_decode_human_acceptance_with_geometry(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<
        (
            PermissionAcceptance,
            [CanonicalMeasure; 3],
            super::runtime_raw_geometry::RuntimeHumanRawGeometry,
        ),
        PermissionError,
    > {
        let geometry = super::runtime_raw_geometry::preflight_human_with_geometry(
            raw,
            super::runtime_raw_geometry::RuntimeHumanCodecKind::Acceptance,
            budget,
        )?;
        let wire = serde_json::from_slice::<AcceptanceWire>(raw).map_err(
            runtime_checked_mandatory_callable(|_| PermissionError::CorruptRecord),
        )?;
        let value = finish_acceptance(wire, raw.len(), budget)?;
        let actual = {
            budget.reserve(0, 8)?;
            let mut workspace = CanonicalWorkspace::new(budget)?;
            let borrowed = BorrowedRecord::Acceptance(&value);
            let root = pair_measure(
                &mut workspace,
                CanonicalValue::Record(borrowed),
                CanonicalLimit::Record(RecordKind::Acceptance),
                budget,
            )?;
            let inherent = runtime_captured_inherent(&mut workspace, borrowed, &root, budget)?;
            budget.reserve(256, 8)?;
            let before = inherent.before.ok_or(PermissionError::CorruptRecord)?;
            let scope = inherent.scope.ok_or(PermissionError::CorruptRecord)?;
            [root, before, scope]
        };
        Ok((value, actual, geometry))
    }
    #[allow(dead_code)]
    pub(in super::super) fn runtime_decode_human_decision_with_geometry(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<
        (
            PermissionDecision,
            [CanonicalMeasure; 2],
            super::runtime_raw_geometry::RuntimeHumanRawGeometry,
        ),
        PermissionError,
    > {
        let geometry = super::runtime_raw_geometry::preflight_human_with_geometry(
            raw,
            super::runtime_raw_geometry::RuntimeHumanCodecKind::Decision,
            budget,
        )?;
        let wire = serde_json::from_slice::<DecisionWire>(raw).map_err(
            runtime_checked_mandatory_callable(|_| PermissionError::CorruptRecord),
        )?;
        let value = PermissionDecision::from(wire);
        let actual = {
            budget.reserve(0, 8)?;
            let mut workspace = CanonicalWorkspace::new(budget)?;
            let borrowed = BorrowedRecord::Decision(&value);
            let root = pair_measure(
                &mut workspace,
                CanonicalValue::Record(borrowed),
                CanonicalLimit::Record(RecordKind::Decision),
                budget,
            )?;
            let inherent = runtime_captured_inherent(&mut workspace, borrowed, &root, budget)?;
            budget.reserve(256, 8)?;
            [root, inherent.scope.ok_or(PermissionError::CorruptRecord)?]
        };
        Ok((value, actual, geometry))
    }
    const _: () = assert!(
        std::mem::size_of::<(
            PermissionAcceptance,
            [CanonicalMeasure; 3],
            super::runtime_raw_geometry::RuntimeHumanRawGeometry
        )>() <= 4_096
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<
                (
                    PermissionAcceptance,
                    [CanonicalMeasure; 3],
                    super::runtime_raw_geometry::RuntimeHumanRawGeometry
                ),
                PermissionError,
            >,
        >() <= 4_096
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (
                    PermissionAcceptance,
                    [CanonicalMeasure; 3],
                    super::runtime_raw_geometry::RuntimeHumanRawGeometry
                ),
            >,
        >() <= 4_096
    );
    const _: () = assert!(
        std::mem::size_of::<(
            PermissionDecision,
            [CanonicalMeasure; 2],
            super::runtime_raw_geometry::RuntimeHumanRawGeometry
        )>() <= 4_096
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<
                (
                    PermissionDecision,
                    [CanonicalMeasure; 2],
                    super::runtime_raw_geometry::RuntimeHumanRawGeometry
                ),
                PermissionError,
            >,
        >() <= 4_096
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (
                    PermissionDecision,
                    [CanonicalMeasure; 2],
                    super::runtime_raw_geometry::RuntimeHumanRawGeometry
                ),
            >,
        >() <= 4_096
    );
}

// Runtime compatibility helpers use actual guarded owners and the one canonical
// driver. Their consumers remain separately qualified; old wrappers stay exact.
#[allow(dead_code)] // This new private seam awaits its separately qualified consumers.
pub(super) mod runtime_compat {
    use super::*;

    // Inert whole named bounds for the private mandatory-domain compiler prospect.
    // N16: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    RuntimeScopeHistory<'static>,
                >,
            >() <= 128
        );
        assert!(
            std::mem::align_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    RuntimeScopeHistory<'static>,
                >,
            >() <= 16
        );
    };
    // N17: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    (CanonicalMeasure, bool),
                >,
            >() <= 128
        );
        assert!(
            std::mem::align_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    (CanonicalMeasure, bool),
                >,
            >() <= 16
        );
    };
    // N18: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    RuntimeWorkspace<'static, 5>,
                >,
            >() <= 128
        );
        assert!(
            std::mem::align_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    RuntimeWorkspace<'static, 5>,
                >,
            >() <= 16
        );
    };
    // N19: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(std::mem::size_of::<RuntimeWorkspace<'static, 66>>() <= 128);
        assert!(std::mem::align_of::<RuntimeWorkspace<'static, 66>>() <= 16);
    };
    // N20: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(
            std::mem::size_of::<Result<RuntimeWorkspace<'static, 66>, PermissionError>>() <= 128
        );
        assert!(
            std::mem::align_of::<Result<RuntimeWorkspace<'static, 66>, PermissionError>>() <= 16
        );
    };
    // N21: complete named type; static lifetime is a layout proposition only.
    const _: () = {
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    RuntimeWorkspace<'static, 66>,
                >,
            >() <= 128
        );
        assert!(
            std::mem::align_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    RuntimeWorkspace<'static, 66>,
                >,
            >() <= 16
        );
    };

    struct RuntimeWorkspace<'data, const N: usize> {
        stack: Vec<CanonicalFrame<'data>>,
    }
    fn runtime_measure<'data, const N: usize>(
        workspace: &mut RuntimeWorkspace<'data, N>,
        value: CanonicalValue<'data>,
        limit: CanonicalLimit,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<CanonicalMeasure, PermissionError> {
        // Stable data references, short workspace/account borrows. An Err drops
        // the lexical owner-local workspace; it is never cleared and reused.
        walk_with_stack(
            value,
            limit,
            &mut std::io::sink(),
            budget,
            true,
            &mut workspace.stack,
        )
    }

    // Optional slots retain only actual outputs from the existing pass family.
    // For the singleton first profile these are root, both Spec and both Input.
    pub(in super::super) struct RuntimeSnapshotFacts {
        actual: [Option<CanonicalMeasure>; 5],
    }
    impl RuntimeSnapshotFacts {
        pub(in super::super) fn into_actual(self) -> [Option<CanonicalMeasure>; 5] {
            self.actual
        }
    }

    pub(in super::super) fn capture_resource_execution(
        cells: PermissionExecutionCells<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::StoredExecution, RuntimeSnapshotFacts), PermissionError> {
        let result = capture_execution_inner(cells, budget);
        budget.finish(result)
    }
    pub(in super::super) fn capture_resource_thread(
        cells: PermissionThreadCells<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::ThreadRecord, CanonicalMeasure), PermissionError> {
        let result = capture_thread_inner(cells, budget);
        budget.finish(result)
    }

    // The only new branches retain the first actual paired operands; their
    // caller pays four controls per task before the old task/input loops.
    fn runtime_equal_input<'data>(
        workspace: &mut RuntimeWorkspace<'data, 128>,
        left: &'data serde_json::Map<String, serde_json::Value>,
        right: &'data serde_json::Map<String, serde_json::Value>,
        facts: &mut RuntimeSnapshotFacts,
        current_values: bool,
        budget: &mut PermissionEvidenceBudget,
        limit: CanonicalLimit,
    ) -> Result<(), PermissionError> {
        let left_quote = runtime_measure(workspace, CanonicalValue::Input(left), limit, budget)?;
        let right_quote = runtime_measure(workspace, CanonicalValue::Input(right), limit, budget)?;
        budget.reserve(
            add(left_quote.bytes, right_quote.bytes)?,
            add(left_quote.nodes, right_quote.nodes)?,
        )?;
        if facts.actual[3].is_none() {
            facts.actual[3] = Some(left_quote);
        }
        if current_values {
            facts.actual[4] = Some(right_quote);
        }
        // The unchanged kernel validator performs the actual equality later.
        Ok(())
    }

    fn capture_execution_inner(
        cells: PermissionExecutionCells<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::StoredExecution, RuntimeSnapshotFacts), PermissionError> {
        budget.reserve(0, 0)?;
        // O: five actual measure slots, one bundle and three control carriers.
        budget.runtime_prepaid_ready_cancellation_fixed(1_264, 19)?;
        if cells.thread_id.len() > 256 || cells.snapshot.len() > 1_048_576 {
            return budget.limit();
        }
        budget.runtime_prepaid_ready_cancellation_fixed(1024, 8)?;
        let revision = resource_u64(cells.revision)?;
        let owner = resource_u64(cells.owner_generation)?;
        let thread_id = ThreadId::new(resource_string(cells.thread_id, budget)?)
            .map_err(|_| PermissionError::CorruptRecord)?;
        preflight_profile(cells.snapshot, RawProfile::Snapshot, budget)?;
        let snapshot: Snapshot =
            serde_json::from_slice(cells.snapshot).map_err(|_| PermissionError::CorruptRecord)?;
        budget.reserve(
            add(
                snapshot.execution_id.as_str().len(),
                cells.execution_id.as_str().len(),
            )?,
            8,
        )?;
        if &snapshot.execution_id != cells.execution_id
            || snapshot.revision != revision
            || snapshot.owner_generation != owner
        {
            return corrupt();
        }
        // The separate count traversal is paid before iterating. It imposes no native64 cap.
        budget.reserve(0, snapshot.definition.batches.len())?;
        let count = snapshot
            .definition
            .batches
            .iter()
            .map(Vec::len)
            .try_fold(0, add)?;
        let mut facts = RuntimeSnapshotFacts { actual: [None; 5] };
        {
            // The actual Snapshot exists before its own stack. Thread decode is later.
            if std::mem::size_of::<CanonicalFrame<'_>>() > 512 {
                return Err(PermissionError::CorruptRecord);
            }
            budget.reserve(65_536, 0)?;
            let mut workspace = RuntimeWorkspace::<128> { stack: Vec::new() };
            workspace
                .stack
                .try_reserve_exact(128)
                .map_err(|_| PermissionError::LimitExceeded)?;
            checked_resource_snapshot_capture(
                &mut workspace,
                &snapshot,
                &mut facts,
                count,
                budget,
                CanonicalLimit::ResourceSnapshot,
                true,
            )?;
        }
        // Every successful pass drained; the stack has dropped before this owner move.
        budget.runtime_prepaid_ready_cancellation_fixed(
            std::mem::size_of::<crate::StoredExecution>(),
            1,
        )?;
        Ok((
            crate::StoredExecution {
                thread_id,
                snapshot,
            },
            facts,
        ))
    }

    fn capture_thread_inner(
        cells: PermissionThreadCells<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::ThreadRecord, CanonicalMeasure), PermissionError> {
        budget.reserve(0, 0)?;
        // O: the sixth actual measure slot and final control carrier.
        budget.runtime_prepaid_ready_cancellation_fixed(304, 6)?;
        let active_len = cells.active_execution_id.map_or(0, <[u8]>::len);
        let origin_len = cells.origin.map_or(0, <[u8]>::len);
        let total = add(
            512,
            add(
                add(cells.id.as_str().len(), active_len)?,
                add(
                    cells.generation.len(),
                    add(cells.metadata.len(), origin_len)?,
                )?,
            )?,
        )?;
        if cells.id.as_str().len() > 256 || active_len > 256 || total > 262_144 {
            return budget.limit();
        }
        if cells.generation.len() != 64 {
            return corrupt();
        }
        budget.runtime_prepaid_ready_cancellation_fixed(1024, 16)?;
        let created_at_ms = resource_u64(cells.created_at_ms)?;
        let resource_revision = resource_u64(cells.resource_revision)?;
        let id = ThreadId::new(resource_string(cells.id.as_str().as_bytes(), budget)?)
            .map_err(|_| PermissionError::CorruptRecord)?;
        let generation = ThreadGeneration::new(resource_string(cells.generation, budget)?)
            .map_err(|_| PermissionError::CorruptRecord)?;
        let active_execution_id = cells
            .active_execution_id
            .map(|raw| {
                ExecutionId::new(resource_string(raw, budget)?)
                    .map_err(|_| PermissionError::CorruptRecord)
            })
            .transpose()?;
        preflight_profile(cells.metadata, RawProfile::ThreadJson, budget)?;
        let metadata: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(cells.metadata).map_err(|_| PermissionError::CorruptRecord)?;
        let origin = cells
            .origin
            .map(|raw| {
                preflight_profile(raw, RawProfile::ThreadJson, budget)?;
                let origin: ThreadOrigin =
                    serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
                let bytes = add(
                    origin.application.len(),
                    add(origin.tenant.len(), origin.principal.len())?,
                )?;
                budget.reserve(mul(6, bytes)?, 3)?;
                for label in [&origin.application, &origin.tenant, &origin.principal] {
                    if !valid_label(label) {
                        return corrupt();
                    }
                }
                Ok(origin)
            })
            .transpose()?;
        budget.runtime_prepaid_ready_cancellation_fixed(
            std::mem::size_of::<crate::ThreadRecord>(),
            1,
        )?;
        let thread = crate::ThreadRecord {
            id,
            generation,
            active_execution_id,
            created_at_ms,
            metadata,
            origin,
            resource_revision,
        };
        let measured = {
            // A separate later owner requires a separate full depth127 workspace.
            if std::mem::size_of::<CanonicalFrame<'_>>() > 512 {
                return Err(PermissionError::CorruptRecord);
            }
            budget.reserve(65_536, 0)?;
            let mut workspace = RuntimeWorkspace::<128> { stack: Vec::new() };
            workspace
                .stack
                .try_reserve_exact(128)
                .map_err(|_| PermissionError::LimitExceeded)?;
            runtime_measure(
                &mut workspace,
                CanonicalValue::Thread(&thread),
                CanonicalLimit::ResourceThread,
                budget,
            )?
        };
        Ok((thread, measured))
    }

    fn checked_resource_snapshot_capture<'data>(
        workspace: &mut RuntimeWorkspace<'data, 128>,
        snapshot: &'data Snapshot,
        facts: &mut RuntimeSnapshotFacts,
        definition_count: usize,
        budget: &mut PermissionEvidenceBudget,
        limit: CanonicalLimit,
        generic_resource: bool,
    ) -> Result<CanonicalMeasure, PermissionError> {
        let measured =
            runtime_measure(workspace, CanonicalValue::Snapshot(snapshot), limit, budget)?;
        facts.actual[0] = Some(measured);
        reserve_measure(measured, budget)?;
        let h = snapshot.turn_history.len();
        let a = snapshot.attempt_history.len();
        let t = snapshot.tasks.len();
        // New retention-control branches are paid before the unchanged scans.
        budget.reserve(0, mul(4, t)?)?;
        // This fee precedes the quote scans/counts and kernel's repeated shallow scans.
        budget.reserve(0, mul(25, add(add(h, a)?, add(definition_count, t)?)?)?)?;
        let u = snapshot
            .tasks
            .iter()
            .filter(|task| task.attempt_id.is_some())
            .count();
        let p = snapshot
            .tasks
            .iter()
            .filter(|task| task.interrupt.is_some())
            .count();
        let batch = snapshot
            .definition
            .batches
            .get(snapshot.batch_index)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let b = batch.len();
        // expected may have d entries even before a malformed shorter tasks vector
        // is rejected; reserve d for that Vec independently of t.
        let slots = add(add(add(h, a)?, mul(4, definition_count)?)?, add(u, p)?)?;
        budget.reserve(mul(128, slots)?, 0)?;
        let turn_bytes = snapshot
            .turn_history
            .iter()
            .map(|id| id.as_str().len())
            .try_fold(0usize, add)?;
        let attempt_bytes = snapshot
            .attempt_history
            .iter()
            .map(|id| id.as_str().len())
            .try_fold(0usize, add)?;
        let spec_id_bytes = snapshot
            .definition
            .batches
            .iter()
            .flatten()
            .map(|spec| spec.id.as_str().len())
            .try_fold(0usize, add)?;
        let spec_effect_bytes = snapshot
            .definition
            .batches
            .iter()
            .flatten()
            .map(|spec| spec.effect_id.as_str().len())
            .try_fold(0usize, add)?;
        let task_attempt_bytes = snapshot
            .tasks
            .iter()
            .filter_map(|task| task.attempt_id.as_ref())
            .map(|id| id.as_str().len())
            .try_fold(0usize, add)?;
        let interrupt_bytes = snapshot
            .tasks
            .iter()
            .filter_map(|task| task.interrupt.as_ref())
            .map(|value| value.id.as_str().len())
            .try_fold(0usize, add)?;
        id_family(h, turn_bytes, budget)?;
        id_family(a, attempt_bytes, budget)?;
        id_family(definition_count, spec_id_bytes, budget)?;
        id_family(definition_count, spec_effect_bytes, budget)?;
        id_family(u, task_attempt_bytes, budget)?;
        id_family(p, interrupt_bytes, budget)?;
        budget.reserve(
            add(mul(u, attempt_bytes)?, mul(a, task_attempt_bytes)?)?,
            mul(2, mul(u, a)?)?,
        )?;
        let task_ids = snapshot
            .tasks
            .iter()
            .map(|task| task.spec.id.as_str().len())
            .try_fold(0usize, add)?;
        let batch_ids = batch
            .iter()
            .map(|spec| spec.id.as_str().len())
            .try_fold(0usize, add)?;
        // Both this quote loop and old kernel perform the current-batch membership scan.
        budget.reserve(
            mul(2, add(mul(b, task_ids)?, mul(t, batch_ids)?)?)?,
            mul(4, mul(t, b)?)?,
        )?;
        let mut previous_input = None;
        for (task, spec) in snapshot.tasks.iter().zip(
            snapshot
                .definition
                .batches
                .iter()
                .take(snapshot.batch_index.saturating_add(1))
                .flatten(),
        ) {
            let task_quote = runtime_measure(
                workspace,
                CanonicalValue::TaskSpec(&task.spec),
                limit,
                budget,
            )?;
            let spec_quote =
                runtime_measure(workspace, CanonicalValue::TaskSpec(spec), limit, budget)?;
            if facts.actual[1].is_none() {
                facts.actual[1] = Some(task_quote);
                facts.actual[2] = Some(spec_quote);
            }
            budget.reserve(
                add(task_quote.bytes, spec_quote.bytes)?,
                add(task_quote.nodes, spec_quote.nodes)?,
            )?;
            if batch.iter().any(|member| member.id == task.spec.id) {
                if let Some(previous) = previous_input {
                    runtime_equal_input(
                        workspace,
                        previous,
                        &task.input,
                        facts,
                        false,
                        budget,
                        limit,
                    )?;
                }
                previous_input = Some(&task.input);
            }
        }
        if let Some(input) = previous_input {
            runtime_equal_input(
                workspace,
                input,
                &snapshot.values,
                facts,
                true,
                budget,
                limit,
            )?;
        }
        if generic_resource {
            runtime_terminal_quote(workspace, snapshot, limit, budget)?;
        }
        budget.runtime_prepaid_ready_cancellation_fixed(4096, 0)?;
        if budget.runtime_ready_cancellation_has_fixed_purchase() {
            budget.reserve(mul(4, measured.bytes)?, mul(4, measured.nodes)?)?;
        }
        b4_kernel::validate_snapshot(snapshot).map_err(|_| PermissionError::CorruptRecord)?;
        Ok(measured)
    }

    fn runtime_terminal_quote<'data>(
        workspace: &mut RuntimeWorkspace<'data, 128>,
        snapshot: &'data Snapshot,
        limit: CanonicalLimit,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        use b4_contract::{ExecutionStatus, StateWrite, TaskStatus};
        // Quoting the terminal-specific status/task scans is paid separately from kernel scans.
        budget.reserve(0, add(snapshot.tasks.len(), 1)?)?;
        if snapshot.status == ExecutionStatus::Completed {
            let state = runtime_measure(
                workspace,
                CanonicalValue::Input(&snapshot.values),
                limit,
                budget,
            )?;
            clone_quote(state, budget)?;
            if let Some(output) = &snapshot.output {
                let output =
                    runtime_measure(workspace, CanonicalValue::Json(output), limit, budget)?;
                budget.reserve(
                    add(state.bytes, output.bytes)?,
                    add(state.nodes, output.nodes)?,
                )?;
            }
            // No original-task-input/current-final-state equality is added here.
            return Ok(());
        }
        if snapshot.status != ExecutionStatus::Failed
            || snapshot
                .tasks
                .iter()
                .any(|task| task.status == TaskStatus::Failed)
        {
            return Ok(());
        }
        let whole = runtime_measure(workspace, CanonicalValue::Snapshot(snapshot), limit, budget)?;
        reserve_measure(whole, budget)?; // Full independent quote scan before counting writes/keys.
        let batch = snapshot
            .definition
            .batches
            .get(snapshot.batch_index)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let b = batch.len();
        let t = snapshot.tasks.len();
        let task_ids = snapshot
            .tasks
            .iter()
            .map(|task| task.spec.id.as_str().len())
            .try_fold(0, add)?;
        let batch_ids = batch
            .iter()
            .map(|spec| spec.id.as_str().len())
            .try_fold(0, add)?;
        // This quote search and the reducer-task collection search each require payment.
        budget.reserve(
            mul(2, add(mul(b, task_ids)?, mul(t, batch_ids)?)?)?,
            mul(4, mul(t, b)?)?,
        )?;
        budget.reserve(mul(128, b)?, 0)?;
        let state = runtime_measure(
            workspace,
            CanonicalValue::Input(&snapshot.values),
            limit,
            budget,
        )?;
        reserve_measure(state, budget)?; // Key-total query precedes the reducer state lookup quote.
        let k0 = snapshot.values.len();
        let k0_bytes = snapshot.values.keys().map(String::len).try_fold(0, add)?;
        let mut writes = 0;
        let mut fields = 0;
        let mut write_bytes = 0;
        let mut write_nodes = 0;
        for spec in batch {
            if let Some(task) = snapshot.tasks.iter().find(|task| task.spec.id == spec.id) {
                if task.status != TaskStatus::Succeeded {
                    continue;
                }
                if let Some(result) = &task.result {
                    for write in &result.writes {
                        writes = add(writes, 1)?;
                        let field = match write {
                            StateWrite::Replace { field, .. }
                            | StateWrite::Append { field, .. } => field,
                        };
                        fields = add(fields, field.len())?;
                        let measured = runtime_measure(
                            workspace,
                            CanonicalValue::Write(write),
                            limit,
                            budget,
                        )?;
                        write_bytes = add(write_bytes, measured.bytes)?;
                        write_nodes = add(write_nodes, measured.nodes)?;
                    }
                }
            }
        }
        let square = mul(writes, writes)?;
        // Writer conflict map/usize sets, including complete comparison and retained slot quotes.
        budget.reserve(
            add(
                mul(256, writes)?,
                add(mul(mul(2, writes)?, fields)?, mul(16, square)?)?,
            )?,
            add(mul(2, square)?, mul(2, writes)?)?,
        )?;
        // State entry keys grow by at most one per write; pay every prospective later lookup.
        budget.reserve(
            add(
                mul(writes, k0_bytes)?,
                mul(add(k0, mul(2, writes)?)?, fields)?,
            )?,
            mul(mul(2, writes)?, add(k0, writes)?)?,
        )?;
        budget.reserve(mul(256, add(k0, writes)?)?, 0)?;
        let combined = CanonicalMeasure {
            bytes: add(state.bytes, write_bytes)?,
            nodes: add(state.nodes, write_nodes)?,
            depth: state.depth,
        };
        clone_quote(combined, budget)?;
        budget.reserve(
            mul(mul(128, writes)?, combined.nodes)?,
            mul(writes, combined.nodes)?,
        )?;
        // Static returned failure-code comparisons and status/result inspections.
        budget.reserve(
            add(snapshot.failure.as_ref().map_or(0, String::len), 128)?,
            add(writes, 8)?,
        )?;
        Ok(())
    }

    pub(in super::super) fn measure_post_thread(
        thread: &crate::ThreadRecord,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<CanonicalMeasure, PermissionError> {
        let result = runtime_checked_output_hook_mut::<_, _, 64>(|| {
            // E:512/7 controls +528/9 q/Result +42 error margin, before action.
            budget.runtime_prepaid_ready_cancellation_fixed(1_082, 16)?;
            if !thread.metadata.is_empty() {
                return Err(PermissionError::UnsupportedProfile);
            }
            budget.reserve(2_560, 0)?;
            let mut workspace = RuntimeWorkspace::<5> { stack: Vec::new() };
            workspace
                .stack
                .try_reserve_exact(5)
                .map_err(runtime_checked_output_mapper(|_| {
                    PermissionError::LimitExceeded
                }))?;
            runtime_measure(
                &mut workspace,
                CanonicalValue::Thread(thread),
                CanonicalLimit::ResourceThread,
                budget,
            )
        });
        budget.finish(result)
    }

    // Borrowed actual inputs only. This data projection confers no gate/use authority.
    pub(in super::super) struct RuntimeCurrentScope<'data> {
        pub(in super::super) thread_id: &'data ThreadId,
        pub(in super::super) thread_generation: &'data ThreadGeneration,
        pub(in super::super) origin: &'data ThreadOrigin,
        pub(in super::super) application: &'data ApplicationIdentity,
        pub(in super::super) route: &'data RouteKey,
        pub(in super::super) definition: &'data Definition,
        pub(in super::super) effect_id: &'data EffectId,
        pub(in super::super) tool_id: &'data StableToolId,
        pub(in super::super) tool_revision: &'data StableToolRevision,
        pub(in super::super) policy_revision: &'data str,
        pub(in super::super) constraint_revision: &'data ToolConstraintRevision,
    }
    // The unique budget loan is issued only after both debits. The caller then
    // constructs RuntimeCurrentScope; no descriptor can be paid retroactively.
    pub(in super::super) struct RuntimeScopeHistory<'budget> {
        budget: &'budget mut PermissionEvidenceBudget,
    }
    pub(in super::super) fn begin_scope_history(
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<RuntimeScopeHistory<'_>, PermissionError> {
        // One whole G core. Runtime/catalog callers must not duplicate it.
        budget.runtime_prepaid_ready_cancellation_fixed(65_536, 2_048)?;
        // New-only loan/descriptor/Result transfers, before their construction.
        budget.runtime_prepaid_ready_cancellation_fixed(2_560, 80)?;
        Ok(RuntimeScopeHistory { budget })
    }
    impl RuntimeScopeHistory<'_> {
        pub(in super::super) fn measure_current_scope_and_equal(
            self,
            current: RuntimeCurrentScope<'_>,
            historical: &PermissionScope,
            historical_measure: CanonicalMeasure,
        ) -> Result<(CanonicalMeasure, bool), PermissionError> {
            let budget = self.budget;
            let result = (|| {
                // Chosen residual1894/42 adapter +128/8 first-profile guard.
                let application_bytes = add(
                    current.application.application.len(),
                    current.application.revision.len(),
                )?;
                let application_limit = if budget.runtime_input_profile()
                    != super::super::runtime_mutation::PermissionRuntimeInputProfile::Strict
                {
                    512
                } else {
                    27
                };
                if application_bytes > application_limit {
                    return Err(PermissionError::UnsupportedProfile);
                }
                // Distinct G-local identity Strings: copy and failure/unique
                // disposal are paid before either constructor, after the
                // observed label length guard and selected initial purchase.
                if budget.runtime_ready_cancellation_has_fixed_purchase() {
                    budget.reserve(add(mul(13, application_bytes)?, 512)?, 4)?;
                }
                let application = PermissionApplicationIdentity::from_native(current.application)?;
                let parts = ScopeParts {
                    thread_id: current.thread_id,
                    thread_generation: current.thread_generation,
                    origin: current.origin,
                    application: &application,
                    route: current.route,
                    definition: current.definition,
                    effect_id: current.effect_id,
                    tool_id: current.tool_id,
                    tool_revision: current.tool_revision,
                    policy_revision: current.policy_revision,
                    constraint_revision: current.constraint_revision,
                };
                let measured = {
                    budget.reserve(3_584, 0)?;
                    let mut workspace = RuntimeWorkspace::<7> { stack: Vec::new() };
                    workspace
                        .stack
                        .try_reserve_exact(7)
                        .map_err(|_| PermissionError::LimitExceeded)?;
                    runtime_measure(
                        &mut workspace,
                        CanonicalValue::Scope(parts),
                        CanonicalLimit::Evidence64KiB,
                        budget,
                    )?
                };
                // Both actual retained operands precede every full field comparison.
                budget.reserve(
                    add(measured.bytes, historical_measure.bytes)?,
                    add(measured.nodes, historical_measure.nodes)?,
                )?;
                let matches = current.thread_id == &historical.thread_id
                    && current.thread_generation == &historical.thread_generation
                    && current.origin == &historical.origin
                    && application == historical.application
                    && current.route == &historical.route
                    && current.definition == &historical.definition
                    && current.effect_id == &historical.effect_id
                    && current.tool_id == &historical.tool_id
                    && current.tool_revision == &historical.tool_revision
                    && current.policy_revision == historical.policy_revision
                    && current.constraint_revision == &historical.constraint_revision;
                Ok((measured, matches))
            })();
            budget.finish(result)
        }
    }

    const _: () = assert!(std::mem::size_of::<CanonicalValue<'static>>() <= 128);
    const _: () = assert!(std::mem::size_of::<RuntimeScopeHistory<'static>>() <= 8);
    const _: () = assert!(
        std::mem::size_of::<Result<RuntimeScopeHistory<'static>, PermissionError>>() <= 128
    );
    const _: () = assert!(std::mem::size_of::<CanonicalMeasure>() <= 24);
    const _: () = assert!(std::mem::size_of::<Result<CanonicalMeasure, PermissionError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<RuntimeSnapshotFacts>() <= 256);
    const _: () =
        assert!(std::mem::size_of::<(crate::StoredExecution, RuntimeSnapshotFacts)>() <= 2_048);
    const _: () = assert!(
        std::mem::size_of::<Result<(crate::StoredExecution, RuntimeSnapshotFacts), PermissionError>>(
        ) <= 2_048
    );
    const _: () = assert!(
        std::mem::size_of::<Result<(crate::ThreadRecord, CanonicalMeasure), PermissionError>>()
            <= 2_048
    );
    const _: () = assert!(
        128 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<RuntimeWorkspace<'static, 128>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            <= 65_536
    );
    const _: () = assert!(
        5 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<RuntimeWorkspace<'static, 5>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            <= 2_560
    );
    const _: () = assert!(
        7 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<RuntimeWorkspace<'static, 7>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            <= 3_584
    );
    const _: () = assert!(std::mem::size_of::<PermissionApplicationIdentity>() <= 128);
    const _: () = assert!(
        std::mem::size_of::<Result<PermissionApplicationIdentity, PermissionError>>() <= 128
    );
    const _: () = assert!(std::mem::size_of::<ScopeParts<'static>>() <= 128);
    const _: () = assert!(std::mem::size_of::<RuntimeCurrentScope<'static>>() <= 96);
    const _: () = assert!(std::mem::size_of::<(CanonicalMeasure, bool)>() <= 32);
    const _: () =
        assert!(std::mem::size_of::<Result<(CanonicalMeasure, bool), PermissionError>>() <= 128);
    // New facts/output wrappers are permissions-private and expose no numeric
    // constructor. Every owner/result transfer is prepaid by its caller scope.
    fn runtime_workspace<'data, const N: usize>(
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<RuntimeWorkspace<'data, N>, PermissionError> {
        budget.reserve(mul(N, 512)?, 0)?;
        let mut stack = Vec::new();
        stack
            .try_reserve_exact(N)
            .map_err(runtime_checked_mandatory_callable(|_| {
                PermissionError::LimitExceeded
            }))?;
        Ok(RuntimeWorkspace { stack })
    }
    pub(in super::super) fn capture_old_acceptance(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::approval::ApprovalAcceptance, CanonicalMeasure), PermissionError> {
        preflight_profile(raw, RawProfile::Legacy, budget)?;
        let acceptance: crate::approval::ApprovalAcceptance =
            serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
        let measured = measure(
            CanonicalValue::LegacyAcceptance(&acceptance),
            CanonicalLimit::LegacyRecord,
            budget,
        )?;
        reserve_measure(measured, budget)?;
        budget.reserve(16_384, 64)?;
        let actor = &acceptance.context.actor;
        if !valid_label(&actor.application)
            || !valid_label(&actor.tenant)
            || !valid_label(&actor.principal)
            || !valid_label(&acceptance.context.policy_revision)
            || acceptance.decisions.is_empty()
        {
            return corrupt();
        }
        let m = acceptance.decisions.len();
        // Pay count/ID-sum scans before iterating; old rows gain no invented64 entry cap.
        budget.reserve(mul(512, m)?, mul(2, m)?)?;
        let mut tasks = 0;
        let mut interrupts = 0;
        for entry in &acceptance.decisions {
            tasks = add(tasks, entry.task_id.as_str().len())?;
            interrupts = add(interrupts, entry.interrupt_id.as_str().len())?;
        }
        id_family(m, tasks, budget)?;
        id_family(m, interrupts, budget)?;
        budget.reserve(mul(256, m)?, mul(8, m)?)?;
        for (index, entry) in acceptance.decisions.iter().enumerate() {
            if !matches!(
                entry.decision,
                ApprovalDecision::Once | ApprovalDecision::Deny
            ) || entry.task_parking_revision == 0
                || entry.task_parking_revision > acceptance.previous_revision
                || !digest_shape(&entry.proposal_digest)
            {
                return corrupt();
            }
            for previous in &acceptance.decisions[..index] {
                if entry.task_id == previous.task_id || entry.interrupt_id == previous.interrupt_id
                {
                    return corrupt();
                }
            }
        }
        Ok((acceptance, measured))
    }
    // The E caller prepays scalar formatting, the actual-sized buffer and
    // every owner/Result transfer before this actual generated-J bridge.
    pub(in super::super) fn emit_generated_journal(
        journal: &crate::JournalEntry,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(Vec<u8>, CanonicalMeasure), PermissionError> {
        // New closed guard/allocator/error control carriers, before all work.
        budget.reserve(1_024, 32)?;
        let actual = match writer_journal_measure(journal) {
            Ok(actual) => actual,
            Err(error) => return budget.finish(Err(error)),
        };
        if !budget.runtime_input_profile().journal(actual) {
            return budget.finish(Err(PermissionError::UnsupportedProfile));
        }
        if budget.runtime_input_profile()
            == super::super::runtime_mutation::PermissionRuntimeInputProfile::NormalNewUse
        {
            // Pay actual generated bytes and their owned handoffs before emission.
            budget.reserve(add(128, mul(6, actual.bytes)?)?, actual.nodes)?;
        }
        let mut output = Vec::new();
        if output.try_reserve_exact(actual.bytes).is_err() {
            return budget.limit();
        }
        if serde_json::to_writer(&mut output, journal).is_err() {
            return budget.finish(Err(PermissionError::CorruptRecord));
        }
        if output.len() != actual.bytes {
            return budget.finish(Err(PermissionError::CorruptRecord));
        }
        budget.finish(Ok((output, actual)))
    }

    pub(in super::super) fn emit_ready_cancellation_journal(
        journal: &crate::JournalEntry,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(Vec<u8>, CanonicalMeasure), PermissionError> {
        // New closed guard/allocator/error control carriers, before all work.
        budget.reserve(1_024, 32)?;
        if journal.status != b4_contract::ExecutionStatus::Cancelled {
            return budget.finish(Err(PermissionError::UnsupportedProfile));
        }
        let actual = match writer_journal_measure(journal) {
            Ok(actual) => actual,
            Err(error) => return budget.finish(Err(error)),
        };
        if actual.bytes > 256 || actual.nodes > 11 || actual.depth > 1 {
            return budget.finish(Err(PermissionError::UnsupportedProfile));
        }
        let mut output = Vec::new();
        if output.try_reserve_exact(actual.bytes).is_err() {
            return budget.limit();
        }
        if serde_json::to_writer(&mut output, journal).is_err() {
            return budget.finish(Err(PermissionError::CorruptRecord));
        }
        if output.len() != actual.bytes {
            return budget.finish(Err(PermissionError::CorruptRecord));
        }
        budget.finish(Ok((output, actual)))
    }
    const _: () = assert!(std::mem::size_of::<Vec<u8>>() <= 32);
    const _: () =
        assert!(std::mem::size_of::<Result<(), std::collections::TryReserveError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<Result<Vec<u8>, serde_json::Error>>() <= 128);
    const _: () = assert!(std::mem::size_of::<Result<(), serde_json::Error>>() <= 128);
    const _: () = assert!(std::mem::size_of::<Result<CanonicalMeasure, PermissionError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<(Vec<u8>, CanonicalMeasure)>() <= 128);
    const _: () =
        assert!(std::mem::size_of::<Result<(Vec<u8>, CanonicalMeasure), PermissionError>>() <= 128);

    pub(in super::super) fn capture_journal(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::JournalEntry, CanonicalMeasure), PermissionError> {
        capture_journal_with_profile(raw, budget.runtime_input_profile(), budget)
    }
    // Only trusted retained Use witnesses can supply inherited geometry later.
    pub(in super::super) fn capture_journal_with_profile(
        raw: &[u8],
        profile: super::super::runtime_mutation::PermissionRuntimeInputProfile,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::JournalEntry, CanonicalMeasure), PermissionError> {
        let result = runtime_checked_mandatory_iife(|| {
            budget.reserve(0, 0)?;
            if raw.len() > 1_024 {
                return budget.limit();
            }
            let owner: crate::JournalEntry =
                serde_json::from_slice(raw).map_err(runtime_checked_mandatory_callable(|_| {
                    PermissionError::CorruptRecord
                }))?;
            let actual = writer_journal_measure(&owner)?;
            if !profile.historical_journal(actual) {
                return Err(PermissionError::UnsupportedProfile);
            }
            Ok((owner, actual))
        });
        budget.finish(result)
    }
    pub(in super::super) fn capture_grant(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(crate::approval::GrantRecord, [CanonicalMeasure; 2]), PermissionError> {
        preflight_profile(raw, RawProfile::Legacy, budget)?;
        let grant: crate::approval::GrantRecord =
            serde_json::from_slice(raw).map_err(ready_authorization_checked_metadata_value(
                |_: serde_json::Error| PermissionError::CorruptRecord,
            ))?;
        budget.reserve(0, 8)?;
        let mut workspace = runtime_workspace::<66>(budget)?;
        let measured = runtime_measure(
            &mut workspace,
            CanonicalValue::LegacyGrant(&grant),
            CanonicalLimit::LegacyRecord,
            budget,
        )?;
        reserve_measure(measured, budget)?;
        // Complete finite label, digest, TTL, SHA69 and verifier comparison allowance.
        budget.reserve(32_768, 128)?;
        if !valid_label(&grant.origin.application)
            || !valid_label(&grant.origin.tenant)
            || !valid_label(&grant.origin.principal)
            || !valid_label(&grant.policy_revision)
            || grant.task_parking_revision == 0
            || !digest_shape(&grant.proposal_digest)
            || !digest_shape(&grant.resume_key)
        {
            return corrupt();
        }
        let ttl = grant
            .expires_at_ms
            .checked_sub(grant.issued_at_ms)
            .ok_or(PermissionError::CorruptRecord)?;
        if ttl == 0 || ttl > crate::approval::MAX_APPROVAL_TTL_MS {
            return corrupt();
        }
        let verifier = <sha2::Sha256 as sha2::Digest>::digest(grant.token.as_str().as_bytes());
        let difference = grant.verifier.iter().zip(verifier.iter()).fold(
            0u8,
            ready_authorization_checked_metadata_value(
                |difference: u8, (left, right): (&u8, &u8)| difference | (left ^ right),
            ),
        );
        if difference != 0 {
            return corrupt();
        }
        // Old prompt depth64 counts scalar leaves as zero. Canonical byte length
        // matches pinned serde without its old full allocation/sort construction.
        let prompt_q = runtime_measure(
            &mut workspace,
            CanonicalValue::Json(&grant.prompt),
            CanonicalLimit::LegacyPrompt,
            budget,
        )?;
        budget.reserve(256, 8)?;
        drop(workspace);
        Ok((grant, [measured, prompt_q]))
    }
    pub(in super::super) fn capture_dispatch_snapshot(
        snapshot: &Snapshot,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<[CanonicalMeasure; 3], PermissionError> {
        let result = runtime_checked_mandatory_iife(|| {
            budget.reserve(0, 0)?;
            // The independently prepaid KD scope bounds these actual selection
            // and output carriers; no projected root becomes a fact.
            if snapshot.tasks.len() != 1 {
                return Err(PermissionError::UnsupportedProfile);
            }
            let task = &snapshot.tasks[0];
            budget.reserve(0, 8)?;
            let mut workspace = runtime_workspace::<128>(budget)?;
            let root = runtime_measure(
                &mut workspace,
                CanonicalValue::Snapshot(snapshot),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            let spec = runtime_measure(
                &mut workspace,
                CanonicalValue::TaskSpec(&task.spec),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            let input = runtime_measure(
                &mut workspace,
                CanonicalValue::Input(&task.input),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            let profile = budget.runtime_input_profile();
            if !profile.dispatch_snapshot(root)
                || spec.bytes > profile.task_spec_bytes()
                || spec.nodes > 9
                || !profile.input(input)
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            Ok([root, spec, input])
        });
        budget.finish(result)
    }
    pub(in super::super) fn emit_ready_cancellation_snapshot_string(
        snapshot: &Snapshot,
        actual: CanonicalMeasure,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<String, PermissionError> {
        let result = runtime_checked_output_hook_mut::<_, _, 128>(|| {
            budget.runtime_check_ready_cancellation()?;
            if !budget.runtime_input_profile().cancelled_snapshot(actual) {
                return Err(PermissionError::UnsupportedProfile);
            }
            if budget.runtime_ready_cancellation_has_fixed_purchase() {
                budget.reserve(
                    add(mul(256, actual.nodes)?, mul(38, actual.bytes)?)?,
                    actual.nodes,
                )?;
                budget.reserve(add(128, mul(6, actual.bytes)?)?, 1)?;
            } else {
                budget.reserve(actual.bytes, actual.nodes)?;
            }
            let output =
                serde_json::to_string(snapshot).map_err(runtime_checked_output_mapper(|_| {
                    PermissionError::CorruptRecord
                }))?;
            if output.len() != actual.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            Ok(output)
        });
        budget.finish(result)
    }

    pub(in super::super) fn emit_ready_cancellation_journal_string(
        journal: &crate::JournalEntry,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(String, CanonicalMeasure), PermissionError> {
        budget.runtime_prepaid_ready_cancellation_fixed(1_024, 32)?;
        if journal.status != b4_contract::ExecutionStatus::Cancelled {
            return budget.finish(Err(PermissionError::UnsupportedProfile));
        }
        let actual = match writer_journal_measure(journal) {
            Ok(actual) => actual,
            Err(error) => return budget.finish(Err(error)),
        };
        if !budget.runtime_input_profile().cancellation_journal(actual) {
            return budget.finish(Err(PermissionError::UnsupportedProfile));
        }
        if budget.runtime_ready_cancellation_has_fixed_purchase() {
            budget.reserve(
                add(mul(256, actual.nodes)?, mul(38, actual.bytes)?)?,
                actual.nodes,
            )?;
            budget.reserve(add(128, mul(6, actual.bytes)?)?, 1)?;
        }
        let output = match serde_json::to_string(journal) {
            Ok(output) => output,
            Err(_) => return budget.finish(Err(PermissionError::CorruptRecord)),
        };
        if output.len() != actual.bytes {
            return budget.finish(Err(PermissionError::CorruptRecord));
        }
        budget.finish(Ok((output, actual)))
    }

    pub(in super::super) fn emit_dispatch_snapshot(
        snapshot: &Snapshot,
        actual: CanonicalMeasure,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Vec<u8>, PermissionError> {
        let result = (|| {
            budget.reserve(0, 0)?;
            // Caller supplies only this immutable owner's retained actual root;
            // the closed Runtime lifecycle proves its origin and both links.
            budget.reserve(actual.bytes, actual.nodes)?;
            let mut output = ExactWriter {
                bytes: Vec::new(),
                maximum: actual.bytes,
            };
            output
                .bytes
                .try_reserve_exact(actual.bytes)
                .map_err(|_| PermissionError::LimitExceeded)?;
            let emitted = walk(
                CanonicalValue::Snapshot(snapshot),
                CanonicalLimit::ResourceSnapshot,
                &mut output,
                budget,
                false,
            )?;
            if emitted.bytes != actual.bytes
                || emitted.nodes != actual.nodes
                || emitted.depth != actual.depth
                || output.bytes.len() != actual.bytes
            {
                return Err(PermissionError::CorruptRecord);
            }
            Ok(output.bytes)
        })();
        budget.finish(result)
    }
    pub(in super::super) fn quote_unclaimed_gate(
        definition: &Definition,
        native: &Definition,
        task: &TaskRecord,
        identity: &ApplicationIdentity,
        entries: &[super::super::ToolBindingRegistration],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<[CanonicalMeasure; 4], PermissionError> {
        let call = || {
            // New-only fact transport and closed singleton-entry guard, before work.
            budget.reserve(512, 32)?;
            budget.reserve(mul(64, entries.len())?, mul(2, entries.len())?)?;
            let mut workspace = runtime_workspace::<5>(budget)?;
            let provided = runtime_measure(
                &mut workspace,
                CanonicalValue::Definition(definition),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            let selected = runtime_measure(
                &mut workspace,
                CanonicalValue::Definition(native),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            let spec = runtime_measure(
                &mut workspace,
                CanonicalValue::TaskSpec(&task.spec),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            reserve_measure(provided, budget)?;
            let tasks = definition
                .batches
                .iter()
                .map(Vec::len)
                .try_fold(0usize, add)?;
            reserve_measure(provided, budget)?;
            reserve_measure(selected, budget)?;
            // Every TaskSpec pair in the old .any search is paid before the search.
            budget.reserve(
                add(mul(tasks, spec.bytes)?, provided.bytes)?,
                add(mul(tasks, spec.nodes)?, provided.nodes)?,
            )?;
            let mut retained_entry = None;
            for entry in entries {
                let entry_definition = runtime_measure(
                    &mut workspace,
                    CanonicalValue::Definition(&entry.definition),
                    CanonicalLimit::ResourceSnapshot,
                    budget,
                )?;
                reserve_measure(provided, budget)?;
                reserve_measure(entry_definition, budget)?;
                // Definition equality short-circuiting does not refund the EffectId pair.
                budget.reserve(
                    add(
                        entry.effect_id.as_str().len(),
                        task.spec.effect_id.as_str().len(),
                    )?,
                    2,
                )?;
                // Conservatively quote every candidate owned binding, not only the
                // eventual selected registration. No clone or refund occurs here.
                budget.reserve(
                    add(
                        mul(128, entry_definition.nodes)?,
                        mul(6, entry_definition.bytes)?,
                    )?,
                    entry_definition.nodes,
                )?;
                reserve_measure(entry_definition, budget)?;
                budget.reserve(4_096, 1)?;
                let labels = add(
                    add(identity.application.len(), identity.revision.len())?,
                    add(
                        entry.effect_id.as_str().len(),
                        add(
                            entry.tool_id.as_str().len(),
                            add(
                                entry.tool_revision.as_str().len(),
                                entry.constraint_revision.as_str().len(),
                            )?,
                        )?,
                    )?,
                )?;
                budget.reserve(add(1_024, mul(6, labels)?)?, 6)?;
                retained_entry = Some(entry_definition);
            }
            if provided.bytes > 2_048 || selected.bytes > 2_048 || selected.nodes > 32 {
                return Err(PermissionError::UnsupportedProfile);
            }
            if entries.len() != 1 {
                return Err(PermissionError::UnsupportedProfile);
            }
            let entry = retained_entry.ok_or(PermissionError::CorruptRecord)?;
            Ok([provided, selected, spec, entry])
        };
        let result =
            super::super::runtime_mutation::runtime_checked_mandatory_iife::<_, _, 128>(call);
        budget.finish(result)
    }

    pub(in super::super) fn measure_definition_pair(
        definition: &Definition,
        native: &Definition,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<[CanonicalMeasure; 2], PermissionError> {
        let result = (|| {
            // New-only actual fact carriers/control, separate from old P quotes.
            budget.reserve(256, 16)?;
            let mut workspace = runtime_workspace::<5>(budget)?;
            let provided = runtime_measure(
                &mut workspace,
                CanonicalValue::Definition(definition),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            let selected = runtime_measure(
                &mut workspace,
                CanonicalValue::Definition(native),
                CanonicalLimit::ResourceSnapshot,
                budget,
            )?;
            reserve_measure(provided, budget)?;
            reserve_measure(selected, budget)?;
            Ok([provided, selected])
        })();
        budget.finish(result)
    }
    // These exact owner aggregates remain unconditional in real production.
    const _: () = assert!(
        66 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<RuntimeWorkspace<'static, 66>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            <= 66 * 512
    );
    const _: () = assert!(std::mem::size_of::<[CanonicalMeasure; 2]>() <= 48);
    const _: () = assert!(std::mem::size_of::<[CanonicalMeasure; 3]>() <= 72);
    const _: () = assert!(std::mem::size_of::<[CanonicalMeasure; 4]>() <= 96);
    const _: () =
        assert!(std::mem::size_of::<Result<[CanonicalMeasure; 2], PermissionError>>() <= 128);
    const _: () =
        assert!(std::mem::size_of::<Result<[CanonicalMeasure; 3], PermissionError>>() <= 128);
    const _: () =
        assert!(std::mem::size_of::<Result<[CanonicalMeasure; 4], PermissionError>>() <= 128);
    const _: () = assert!(
        std::mem::size_of::<Result<(crate::JournalEntry, CanonicalMeasure), PermissionError>>()
            <= 256
    );

    // Complete constructor transport aggregates. The allocation debit precedes
    // Workspace/Ok/function-return/caller-extraction; caller-owned borrowed
    // inputs are separately prepaid by G/KD before calling this component.
    const _: () = assert!(
        66 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<RuntimeWorkspace<'static, 66>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            + 4 * 128
            + 64
            <= 66 * 512
    );
    const _: () = assert!(
        128 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<RuntimeWorkspace<'static, 128>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            + 4 * 128
            + 64
            <= 128 * 512
    );

    // Additional whole workspace bounds from the pinned compiler layout audit.
    const _: () = assert!(std::mem::size_of::<RuntimeWorkspace<'static, 5>>() <= 32);
    const _: () =
        assert!(std::mem::size_of::<Result<RuntimeWorkspace<'static, 5>, PermissionError>>() <= 80);
    const _: () = assert!(std::mem::size_of::<RuntimeWorkspace<'static, 128>>() <= 32);
    const _: () = assert!(std::mem::size_of::<RuntimeWorkspace<'static, 5>>() <= 32);
    const _: () = assert!(
        std::mem::size_of::<Result<RuntimeWorkspace<'static, 5>, PermissionError>>() <= 128
    );
    const _: () = assert!(
        5 * std::mem::size_of::<CanonicalFrame<'static>>()
        + std::mem::size_of::<RuntimeWorkspace<'static, 5>>()
        + std::mem::size_of::<WalkState>()
        + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
        // Workspace ctor/Ok/return/caller? and existing repeated scope borrows.
        + 4 * 128 + 64
        // Actual retained output/Option/Result metadata; no owning record here.
        + 6 * 128 + 2 * 32 + 4 * 24
            <= 5 * 512 + 512
    );
    const _: () = assert!(
        5 * std::mem::size_of::<CanonicalFrame<'static>>()
            + std::mem::size_of::<RuntimeWorkspace<'static, 5>>()
            + std::mem::size_of::<WalkState>()
            + std::mem::size_of::<Option<(CanonicalValue<'static>, usize)>>()
            + 4 * 128
            + 64
            + 6 * 128
            + 2 * 32
            + 2 * 24
            <= 5 * 512 + 256
    );

    const _: () = assert!(
        std::mem::size_of::<
            Result<(crate::approval::GrantRecord, [CanonicalMeasure; 2]), PermissionError>,
        >() <= std::mem::size_of::<
            Result<
                (
                    crate::approval::GrantRecord,
                    CanonicalMeasure,
                    CanonicalMeasure
                ),
                PermissionError,
            >,
        >()
    );

    // Private direct-length seams; their future Human-only calculator calls are separate.
    #[allow(unused_imports)]
    pub(in super::super) use super::populated_projection::{
        runtime_human_acceptance_decisions_len as human_acceptance_decisions_len,
        runtime_human_acceptance_policy_revision_len as human_acceptance_policy_revision_len,
    };

    // Unused permissions-private bridge names; Runtime wiring is a later gate.
    #[allow(unused_imports)]
    pub(in super::super) use super::populated_projection::{
        runtime_decode_human_acceptance_with_geometry as capture_human_acceptance_with_geometry,
        runtime_decode_human_decision_with_geometry as capture_human_decision_with_geometry,
    };
    #[allow(unused_imports)]
    pub(in super::super) use super::runtime_raw_geometry::RuntimeHumanRawGeometry;

    // Unused Human-only G sibling; the Once capture_grant above is literal.
    // Future caller prepays new fixed tuple/Result/Flow and failure ingress.
    pub(in super::super) fn capture_human_grant_with_geometry(
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<
        (
            crate::approval::GrantRecord,
            [CanonicalMeasure; 2],
            super::runtime_raw_geometry::RuntimeHumanRawGeometry,
        ),
        PermissionError,
    > {
        let geometry = super::runtime_raw_geometry::preflight_human_with_geometry(
            raw,
            super::runtime_raw_geometry::RuntimeHumanCodecKind::Grant,
            budget,
        )?;
        let grant: crate::approval::GrantRecord =
            serde_json::from_slice(raw).map_err(runtime_checked_mandatory_callable(|_| {
                PermissionError::CorruptRecord
            }))?;
        budget.reserve(0, 8)?;
        let mut workspace = runtime_workspace::<66>(budget)?;
        let measured = runtime_measure(
            &mut workspace,
            CanonicalValue::LegacyGrant(&grant),
            CanonicalLimit::LegacyRecord,
            budget,
        )?;
        reserve_measure(measured, budget)?;
        // Complete finite label, digest, TTL, SHA69 and verifier comparison allowance.
        budget.reserve(32_768, 128)?;
        if !valid_label(&grant.origin.application)
            || !valid_label(&grant.origin.tenant)
            || !valid_label(&grant.origin.principal)
            || !valid_label(&grant.policy_revision)
            || grant.task_parking_revision == 0
            || !digest_shape(&grant.proposal_digest)
            || !digest_shape(&grant.resume_key)
        {
            return corrupt();
        }
        let ttl = grant
            .expires_at_ms
            .checked_sub(grant.issued_at_ms)
            .ok_or(PermissionError::CorruptRecord)?;
        if ttl == 0 || ttl > crate::approval::MAX_APPROVAL_TTL_MS {
            return corrupt();
        }
        let verifier = <sha2::Sha256 as sha2::Digest>::digest(grant.token.as_str().as_bytes());
        let difference = grant.verifier.iter().zip(verifier.iter()).fold(
            0u8,
            runtime_checked_mandatory_callable(|difference, (left, right)| {
                difference | (left ^ right)
            }),
        );
        if difference != 0 {
            return corrupt();
        }
        // Old prompt depth64 counts scalar leaves as zero. Canonical byte length
        // matches pinned serde without its old full allocation/sort construction.
        let prompt_q = runtime_measure(
            &mut workspace,
            CanonicalValue::Json(&grant.prompt),
            CanonicalLimit::LegacyPrompt,
            budget,
        )?;
        budget.reserve(256, 8)?;
        drop(workspace);
        Ok((grant, [measured, prompt_q], geometry))
    }
    const _: () = assert!(
        std::mem::size_of::<(
            crate::approval::GrantRecord,
            [CanonicalMeasure; 2],
            super::runtime_raw_geometry::RuntimeHumanRawGeometry
        )>() <= 4_096
    );
    const _: () = assert!(
        std::mem::size_of::<
            Result<
                (
                    crate::approval::GrantRecord,
                    [CanonicalMeasure; 2],
                    super::runtime_raw_geometry::RuntimeHumanRawGeometry
                ),
                PermissionError,
            >,
        >() <= 4_096
    );
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (
                    crate::approval::GrantRecord,
                    [CanonicalMeasure; 2],
                    super::runtime_raw_geometry::RuntimeHumanRawGeometry
                ),
            >,
        >() <= 4_096
    );
}

#[allow(dead_code)] // Only private consumers may observe actual pass dimensions.
impl CanonicalMeasure {
    pub(super) fn encoded_bytes(&self) -> usize {
        self.bytes
    }
    pub(super) fn structural_nodes(&self) -> usize {
        self.nodes
    }
    pub(super) fn maximum_depth(&self) -> usize {
        self.depth
    }
}

// This private marker anchors one logical runtime Dispatch account. It does not
// attest a resource, graph, current catalog or successful mutation.
impl PermissionEvidenceBudget {
    #[allow(dead_code)] // The new Dispatch service wiring is a separate gate.
    pub(super) fn begin_runtime_dispatch(
        &mut self,
        _execution_id: &ExecutionId,
        _context: &crate::approval::ApprovalContext,
        input_profile: super::runtime_mutation::PermissionRuntimeInputProfile,
    ) -> Result<std::sync::Arc<super::runtime_mutation::PermissionRuntimeOrigin>, PermissionError>
    {
        let result = (|| {
            self.reserve(0, 0)?;
            // O:allocation256/4 +phase512/8 +clone64/1 +identity64/1
            // +both final strong-owner drops128/2, before any new guard/action.
            self.reserve(1_024, 16)?;
            // New-only fresh-ledger guards and closed Result/cleanup carriers.
            self.reserve(1_024, 64)?;
            if self.runtime_state.is_some()
                || self.resource.reads != 0
                || self.resource.combined_reads != 0
                || self.resource.immediate_writes != 0
                || self.resource.referenced_grants != 0
                || self.resource.sql_capacity != 0
                || self.resource.attempted_keys != 0
                || self.resource.executions != 0
                || self.resource.key_bytes != 0
                || !self.resource.keys.is_empty()
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            // Resource R1 exactly once, before the first port; its factory must
            // recognize this started phase and must not admit Resource again.
            self.reserve(128, 1)?;
            self.resource.reads += 1;
            let origin = std::sync::Arc::new(super::runtime_mutation::PermissionRuntimeOrigin);
            self.runtime_state = Some(super::runtime_mutation::PermissionRuntimeState {
                origin: std::sync::Arc::clone(&origin),
                input_profile,
                kind: super::runtime_mutation::PermissionRuntimeOperationKind::Dispatch,
                phase: super::runtime_mutation::PermissionRuntimePhase::ObserveR1Started,
            });
            Ok(origin)
        })();
        self.finish(result)
    }
}

// Trusted permissions-private debit/sticky facades. They expose no fact, reset,
// refund, origin constructor or public generic budget mutation surface.
#[allow(dead_code)]
impl PermissionEvidenceBudget {
    pub(super) fn runtime_is_normal_new_use_dispatch(&self) -> bool {
        use super::runtime_mutation::{
            PermissionRuntimeInputProfile as P, PermissionRuntimeOperationKind as K,
        };
        self.runtime_state.as_ref().is_some_and(|state| {
            matches!(state.kind, K::Dispatch) && state.input_profile == P::NormalNewUse
        })
    }

    // The profile is set only at fresh state inception beside the origin whose
    // identity every read/write admission checks; phase changes cannot switch it.
    // An unentered account and every other operation retain Strict.
    pub(super) fn runtime_input_profile(
        &self,
    ) -> super::runtime_mutation::PermissionRuntimeInputProfile {
        use super::runtime_mutation::{
            PermissionRuntimeInputProfile as P, PermissionRuntimeOperationKind as K,
        };
        match &self.runtime_state {
            Some(state) => match (&state.kind, state.input_profile) {
                (K::Dispatch | K::UseCancellation, P::NormalNewUse) => P::NormalNewUse,
                (K::Dispatch, P::NormalOriginalAlways) => P::NormalOriginalAlways,
                (K::ReadyCancellation, P::NormalReadyCancellation) => P::NormalReadyCancellation,
                _ => P::Strict,
            },
            None => P::Strict,
        }
    }

    // Fixed new-confirmation owner schedule, sized from the actual bounded graphs.
    pub(super) fn runtime_ready_confirmation_owners(
        &mut self,
        snapshot: CanonicalMeasure,
        thread: CanonicalMeasure,
    ) -> Result<(), PermissionError> {
        self.reserve(
            add(add(mul(snapshot.bytes, 12)?, mul(thread.bytes, 4)?)?, 4096)?,
            add(add(mul(snapshot.nodes, 4)?, mul(thread.nodes, 3)?)?, 64)?,
        )
    }

    pub(super) fn runtime_reserve(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        self.reserve(bytes, nodes)
    }

    pub(super) fn runtime_ready_cancellation_has_fixed_purchase(&self) -> bool {
        match &self.runtime_state {
            Some(state) => matches!(
                state.kind,
                super::runtime_mutation::PermissionRuntimeOperationKind::ReadyCancellation
            ),
            None => false,
        }
    }
    pub(super) fn runtime_prepaid_ready_cancellation_fixed(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        if self.runtime_ready_cancellation_has_fixed_purchase() {
            self.runtime_check_ready_cancellation()
        } else {
            self.reserve(bytes, nodes)
        }
    }
    pub(super) fn runtime_ready_cancellation_string_failure_cleanup(
        &mut self,
        length: usize,
    ) -> Result<(), PermissionError> {
        if self.runtime_ready_cancellation_has_fixed_purchase() {
            self.reserve(add(128, mul(6, length)?)?, 1)
        } else if self.use_renewal_scaffold_entered || self.use_renewal_production_entered {
            // Paid renewal String normal/partial cleanup precedes UTF-8/allocation.
            self.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
            self.reserve(add(128, mul(6, length)?)?, 1)
        } else {
            // The new session does not change the old true cancellation arm.
            self.reserve(
                READY_AUTHORIZATION_SHARED_CHECK_BYTES,
                READY_AUTHORIZATION_SHARED_CHECK_NODES,
            )?;
            if self.ready_authorization_entered
                || self.runtime_use_entered
                || self.observe_use_dispatch_account()?
            {
                self.reserve(add(128, mul(6, length)?)?, 1)
            } else {
                Ok(())
            }
        }
    }
    pub(super) fn runtime_check_ready_cancellation(&mut self) -> Result<(), PermissionError> {
        if self.exhausted {
            Err(PermissionError::LimitExceeded)
        } else {
            Ok(())
        }
    }
    pub(super) fn runtime_finish<T>(
        &mut self,
        result: Result<T, PermissionError>,
    ) -> Result<T, PermissionError> {
        self.finish(result)
    }
    pub(super) fn runtime_limit<T>(&mut self) -> Result<T, PermissionError> {
        self.limit()
    }
}
const _: () = assert!(
    std::mem::size_of::<
        Result<std::sync::Arc<super::runtime_mutation::PermissionRuntimeOrigin>, PermissionError>,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<super::runtime_mutation::PermissionRuntimeState>() <= 64);
const _: () =
    assert!(std::mem::size_of::<Option<super::runtime_mutation::PermissionRuntimeState>>() <= 64);

// Closed original Dispatch admissions. A live admission owns its entire account loan.
pub(super) use runtime_phase::RuntimeDispatchAdmission;
pub(super) mod runtime_phase {
    use super::super::runtime_mutation::{
        PermissionRuntimeOperationKind as Kind, PermissionRuntimeOriginOwner as Origin,
        PermissionRuntimePhase as Phase, PermissionRuntimeReadRole as Role,
        PermissionRuntimeReadSql as ReadSql, PermissionRuntimeWriteSql as WriteSql,
    };
    use super::*;

    pub(in super::super) struct RuntimeDispatchAdmission<'budget> {
        budget: &'budget mut PermissionEvidenceBudget,
        next: Phase,
    }
    impl RuntimeDispatchAdmission<'_> {
        pub(in super::super) fn budget_mut(&mut self) -> &mut PermissionEvidenceBudget {
            self.budget
        }
        pub(in super::super) fn check_use_dispatch_evidence(
            &self,
            is_new_use: bool,
        ) -> Result<(), PermissionError> {
            if self.budget.is_use_dispatch_account() && !is_new_use {
                Err(PermissionError::UnsupportedProfile)
            } else {
                Ok(())
            }
        }
        // Audited callers consume this only after the actual SQL COMMIT succeeded.
        // Dropping an unfinished loan deliberately leaves AdmissionInProgress.
        pub(in super::super) fn finish(self) {
            if let Some(state) = self.budget.runtime_state.as_mut() {
                state.phase = self.next;
            }
        }
    }
    const _: () = assert!(std::mem::size_of::<RuntimeDispatchAdmission<'static>>() <= 32);
    const _: () = assert!(
        std::mem::size_of::<Result<RuntimeDispatchAdmission<'static>, PermissionError>>() <= 128
    );

    impl PermissionEvidenceBudget {
        pub(in super::super) fn admit_runtime_dispatch_read<'budget>(
            &'budget mut self,
            origin: &Origin,
            role: &Role,
        ) -> Result<RuntimeDispatchAdmission<'budget>, PermissionError> {
            let result = (|| {
                self.reserve(0, 0)?;
                self.reserve(512, 8)?;
                self.reserve(640, 16)?;
                self.reserve(256, 24)?;
                let (expected, next, counts, admission) = match role {
                    Role::ObserveR1Preclaim => (0u8, Phase::ObserveR1Committed, (1, 0, 0), 0u8),
                    Role::ReadyAuthorityC1 => (1, Phase::ReadyAuthorityCommitted, (1, 0, 0), 2),
                    Role::ObserveR2Refence => (2, Phase::ObserveR2Committed, (1, 1, 0), 1),
                    _ => return Err(PermissionError::UnsupportedProfile),
                };
                // R1's actual identity comparison was prepaid by its initializer.
                if expected != 0 {
                    self.reserve(64, 1)?;
                }
                if expected == 1 {
                    self.reserve(640, 128)?;
                }
                let state = self
                    .runtime_state
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                let matches_phase = match expected {
                    0 => matches!(state.phase, Phase::ObserveR1Started),
                    1 => matches!(state.phase, Phase::ObserveR1Committed),
                    _ => matches!(state.phase, Phase::UnclaimedGated),
                };
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::Dispatch)
                    || !matches_phase
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.reads != counts.0
                    || self.resource.combined_reads != counts.1
                    || self.resource.immediate_writes != counts.2
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if admission == 1 {
                    if self.resource.reads == 6 {
                        return self.limit();
                    }
                    self.reserve(128, 1)?;
                    self.resource.reads += 1;
                } else if admission == 2 {
                    if self.resource.combined_reads == 2 {
                        return self.limit();
                    }
                    self.reserve(128, 1)?;
                    self.resource.combined_reads += 1;
                }
                if let Some(state) = self.runtime_state.as_mut() {
                    state.phase = Phase::AdmissionInProgress;
                }
                Ok(next)
            })();
            let next = self.finish(result)?;
            Ok(RuntimeDispatchAdmission { budget: self, next })
        }
        // Only the separately checked LegacyOnce writer can reach this factory.
        pub(in super::super) fn admit_runtime_dispatch_once<'budget>(
            &'budget mut self,
            origin: &Origin,
        ) -> Result<RuntimeDispatchAdmission<'budget>, PermissionError> {
            self.require_standard_account()?;
            let result = (|| {
                self.reserve(0, 0)?;
                self.reserve(512, 8)?;
                self.reserve(64, 1)?;
                self.reserve(640, 16)?;
                self.reserve(256, 24)?;
                self.reserve(1_024, 128)?;
                let state = self
                    .runtime_state
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::Dispatch)
                    || !matches!(state.phase, Phase::Prepared)
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.reads != 2
                    || self.resource.combined_reads != 1
                    || self.resource.immediate_writes != 0
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.immediate_writes == 1 {
                    return self.limit();
                }
                self.reserve(128, 1)?;
                self.resource.immediate_writes += 1;
                if let Some(state) = self.runtime_state.as_mut() {
                    state.phase = Phase::AdmissionInProgress;
                }
                Ok(Phase::ClaimedCommitted)
            })();
            let next = self.finish(result)?;
            Ok(RuntimeDispatchAdmission { budget: self, next })
        }
        // Only the fully matched NewUse writer can reach this distinct factory.
        pub(in super::super) fn admit_runtime_dispatch_use<'budget>(
            &'budget mut self,
            origin: &Origin,
        ) -> Result<RuntimeDispatchAdmission<'budget>, PermissionError> {
            let result = ready_use_checked_codec_iife(|| {
                self.reserve(0, 0)?;
                self.reserve(512, 8)?;
                self.reserve(64, 1)?;
                self.reserve(640, 16)?;
                self.reserve(256, 24)?;
                self.reserve(1_024, 128)?;
                let state = self
                    .runtime_state
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::Dispatch)
                    || !matches!(state.phase, Phase::Prepared)
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.reads != 2
                    || self.resource.combined_reads != 1
                    || self.resource.immediate_writes != 0
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.immediate_writes == 1 {
                    return self.limit();
                }
                self.reserve(128, 1)?;
                self.resource.immediate_writes += 1;
                if let Some(state) = self.runtime_state.as_mut() {
                    state.phase = Phase::AdmissionInProgress;
                }
                Ok(Phase::ClaimedCommitted)
            });
            let next = self.finish(result)?;
            Ok(RuntimeDispatchAdmission { budget: self, next })
        }
        // Only the separately checked NewHuman first-claim writer uses this loan.
        pub(in super::super) fn admit_runtime_dispatch_human_first_claim<'budget>(
            &'budget mut self,
            origin: &Origin,
        ) -> Result<RuntimeDispatchAdmission<'budget>, PermissionError> {
            self.require_standard_account()?;
            let result = (|| {
                self.reserve(0, 0)?;
                self.reserve(512, 8)?;
                self.reserve(64, 1)?;
                self.reserve(640, 16)?;
                self.reserve(256, 24)?;
                self.reserve(1_024, 128)?;
                let state = self
                    .runtime_state
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::Dispatch)
                    || !matches!(state.phase, Phase::Prepared)
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.reads != 2
                    || self.resource.combined_reads != 1
                    || self.resource.immediate_writes != 0
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.immediate_writes == 1 {
                    return self.limit();
                }
                self.reserve(128, 1)?;
                self.resource.immediate_writes += 1;
                if let Some(state) = self.runtime_state.as_mut() {
                    state.phase = Phase::AdmissionInProgress;
                }
                Ok(Phase::ClaimedCommitted)
            })();
            let next = self.finish(result)?;
            Ok(RuntimeDispatchAdmission { budget: self, next })
        }
        // The finite NewHuman floor verifies the actual Prepared/account lineage
        // without admitting a writer or changing any phase before Unsupported.
        pub(in super::super) fn check_runtime_dispatch_prepared(
            &mut self,
            origin: &Origin,
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.reserve(576, 9)?;
                let state = self
                    .runtime_state
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::Dispatch)
                    || !matches!(state.phase, Phase::Prepared)
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                Ok(())
            })();
            self.finish(result)
        }
        pub(in super::super) fn mark_runtime_dispatch_unclaimed(
            &mut self,
            origin: &Origin,
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.reserve(512, 8)?;
                self.reserve(64, 1)?;
                let state = self
                    .runtime_state
                    .as_mut()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::Dispatch)
                    || !matches!(state.phase, Phase::ReadyAuthorityCommitted)
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                state.phase = Phase::UnclaimedGated;
                Ok(())
            })();
            self.finish(result)
        }
        pub(in super::super) fn mark_runtime_dispatch_prepared(
            &mut self,
            origin: &Origin,
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.reserve(512, 8)?;
                self.reserve(64, 1)?;
                let state = self
                    .runtime_state
                    .as_mut()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::Dispatch)
                    || !matches!(state.phase, Phase::ObserveR2Committed)
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                state.phase = Phase::Prepared;
                Ok(())
            })();
            self.finish(result)
        }
    }

    // The actual selected immediate Read check owns only its real captured loans.
    // The compiler monomorphizes this bound for the genuine FnMut value below.
    fn run_ready_cancellation_read_check<F>(mut check: F) -> Result<Phase, PermissionError>
    where
        F: FnMut() -> Result<Phase, PermissionError>,
    {
        const { assert!(std::mem::size_of::<F>() <= 64) };
        check()
    }

    impl PermissionEvidenceBudget {
        pub(in super::super) fn admit_runtime_ready_cancellation_read<'budget>(
            &'budget mut self,
            origin: &Origin,
            role: &Role,
        ) -> Result<RuntimeDispatchAdmission<'budget>, PermissionError> {
            self.require_standard_account()?;
            let check = || {
                self.runtime_check_ready_cancellation()?;
                self.runtime_prepaid_ready_cancellation_fixed(512, 8)?;
                self.runtime_prepaid_ready_cancellation_fixed(640, 16)?;
                self.runtime_prepaid_ready_cancellation_fixed(256, 24)?;
                let (expected, next, counts, admission) = match role {
                    Role::ObserveR1Preclaim => {
                        (0u8, Phase::ReadyCancellationR1Committed, (1, 0, 0), 0u8)
                    }
                    Role::ReadyAuthorityC1 => {
                        (1, Phase::ReadyCancellationC1Committed, (1, 0, 0), 2)
                    }
                    Role::ObserveR2Refence => {
                        (2, Phase::ReadyCancellationR2Committed, (1, 1, 0), 1)
                    }
                    _ => return Err(PermissionError::UnsupportedProfile),
                };
                // R1's actual identity comparison was prepaid by its initializer.
                if expected != 0 {
                    self.runtime_prepaid_ready_cancellation_fixed(64, 1)?;
                }
                if expected == 1 {
                    self.runtime_prepaid_ready_cancellation_fixed(640, 128)?;
                }
                let state = self
                    .runtime_state
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                let matches_phase = match expected {
                    0 => matches!(state.phase, Phase::ReadyCancellationR1Started),
                    1 => matches!(state.phase, Phase::ReadyCancellationR1Authorized),
                    _ => matches!(state.phase, Phase::ReadyCancellationReadyVerified),
                };
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::ReadyCancellation)
                    || !matches_phase
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if self.resource.reads != counts.0
                    || self.resource.combined_reads != counts.1
                    || self.resource.immediate_writes != counts.2
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                if admission == 1 {
                    if self.resource.reads == 6 {
                        return self.limit();
                    }
                    self.runtime_prepaid_ready_cancellation_fixed(128, 1)?;
                    self.resource.reads += 1;
                } else if admission == 2 {
                    if self.resource.combined_reads == 2 {
                        return self.limit();
                    }
                    self.runtime_prepaid_ready_cancellation_fixed(128, 1)?;
                    self.resource.combined_reads += 1;
                }
                if let Some(state) = self.runtime_state.as_mut() {
                    state.phase = Phase::AdmissionInProgress;
                }
                Ok(next)
            };
            let result = run_ready_cancellation_read_check(check);
            let next = self.finish(result)?;
            Ok(RuntimeDispatchAdmission { budget: self, next })
        }
        pub(in super::super) fn mark_runtime_ready_cancellation_authorized(
            &mut self,
            origin: &Origin,
        ) -> Result<(), PermissionError> {
            let call = || {
                self.runtime_prepaid_ready_cancellation_fixed(512, 8)?;
                self.runtime_prepaid_ready_cancellation_fixed(64, 1)?;
                let state = self
                    .runtime_state
                    .as_mut()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::ReadyCancellation)
                    || !matches!(state.phase, Phase::ReadyCancellationR1Committed)
                    || self.resource.reads != 1
                    || self.resource.combined_reads != 0
                    || self.resource.immediate_writes != 0
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                state.phase = Phase::ReadyCancellationR1Authorized;
                Ok(())
            };
            let result =
                super::super::runtime_mutation::runtime_checked_mandatory_iife::<_, _, 128>(call);
            self.finish(result)
        }
        pub(in super::super) fn mark_runtime_ready_cancellation_verified(
            &mut self,
            origin: &Origin,
        ) -> Result<(), PermissionError> {
            let call = || {
                self.runtime_prepaid_ready_cancellation_fixed(512, 8)?;
                self.runtime_prepaid_ready_cancellation_fixed(64, 1)?;
                let state = self
                    .runtime_state
                    .as_mut()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::ReadyCancellation)
                    || !matches!(state.phase, Phase::ReadyCancellationC1Committed)
                    || self.resource.reads != 1
                    || self.resource.combined_reads != 1
                    || self.resource.immediate_writes != 0
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                state.phase = Phase::ReadyCancellationReadyVerified;
                Ok(())
            };
            let result =
                super::super::runtime_mutation::runtime_checked_mandatory_iife::<_, _, 128>(call);
            self.finish(result)
        }
        pub(in super::super) fn mark_runtime_ready_cancellation_prepared(
            &mut self,
            origin: &Origin,
        ) -> Result<(), PermissionError> {
            let call = || {
                self.runtime_prepaid_ready_cancellation_fixed(512, 8)?;
                self.runtime_prepaid_ready_cancellation_fixed(64, 1)?;
                let state = self
                    .runtime_state
                    .as_mut()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::ReadyCancellation)
                    || !matches!(state.phase, Phase::ReadyCancellationR2Committed)
                    || self.resource.reads != 2
                    || self.resource.combined_reads != 1
                    || self.resource.immediate_writes != 0
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                state.phase = Phase::ReadyCancellationPrepared;
                Ok(())
            };
            let result =
                super::super::runtime_mutation::runtime_checked_mandatory_iife::<_, _, 128>(call);
            self.finish(result)
        }
        pub(in super::super) fn check_runtime_ready_cancellation_prepared(
            &mut self,
            origin: &Origin,
        ) -> Result<(), PermissionError> {
            let result = (|| {
                self.runtime_prepaid_ready_cancellation_fixed(576, 9)?;
                let state = self
                    .runtime_state
                    .as_ref()
                    .ok_or(PermissionError::UnsupportedProfile)?;
                if !std::sync::Arc::ptr_eq(&state.origin, origin)
                    || !matches!(state.kind, Kind::ReadyCancellation)
                    || !matches!(state.phase, Phase::ReadyCancellationPrepared)
                    || self.resource.reads != 2
                    || self.resource.combined_reads != 1
                    || self.resource.immediate_writes != 0
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                Ok(())
            })();
            self.finish(result)
        }
    }

    // The caller prepays each short adapter/argument/Result carrier at its O site.
    // These forwarding seams never call resource_read or add another read admission.
    pub(in super::super) fn runtime_resource_before_sql(
        budget: &mut PermissionEvidenceBudget,
        statement: PermissionResourceSql,
        key: Option<PermissionResourceKey<'_>>,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead { budget }.before_sql(statement, key)
    }
    pub(in super::super) fn runtime_resource_check_metadata(
        budget: &mut PermissionEvidenceBudget,
        version: i64,
        checksum: &[u8],
        owner: &[u8],
        expected_owner: u64,
        expected_schema: &[u8],
    ) -> Result<(), PermissionError> {
        PermissionResourceRead { budget }.check_metadata(
            version,
            checksum,
            owner,
            expected_owner,
            expected_schema,
        )
    }
    pub(in super::super) fn runtime_admit_execution_lengths(
        budget: &mut PermissionEvidenceBudget,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead { budget }.admit_execution_lengths(thread, snapshot)
    }
    pub(in super::super) fn runtime_admit_thread_lengths(
        budget: &mut PermissionEvidenceBudget,
        id: usize,
        active: usize,
        generation: usize,
        metadata: usize,
        origin: usize,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead { budget }
            .admit_thread_lengths(id, active, generation, metadata, origin)
    }
    pub(in super::super) fn runtime_resource_check_linkage(
        budget: &mut PermissionEvidenceBudget,
        id: &ExecutionId,
        thread: &crate::ThreadRecord,
        execution: &crate::StoredExecution,
    ) -> Result<(), PermissionError> {
        PermissionResourceRead { budget }.check_linkage(id, thread, execution)
    }

    // Literal per-statement capacity timing; only the closed callers select BEGIN.
    fn sql_capacity(
        budget: &mut PermissionEvidenceBudget,
        begin: bool,
    ) -> Result<(), PermissionError> {
        budget.reserve(0, 0)?;
        let capacity = if begin { 2 } else { 1 };
        let next = add(budget.resource.sql_capacity, capacity)?;
        if next > 512 {
            return budget.limit();
        }
        budget.runtime_prepaid_ready_cancellation_fixed(mul(capacity, 10_240)?, capacity)?;
        budget.resource.sql_capacity = next;
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn runtime_read_before_sql(
        budget: &mut PermissionEvidenceBudget,
        statement: &ReadSql,
        execution: &ExecutionId,
        thread: &ThreadId,
        turn: Option<&str>,
        pid: Option<&str>,
        interrupt: Option<&InterruptId>,
        journal: Option<(u64, u64)>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            sql_capacity(budget, false)?;
            match statement {
                ReadSql::NewHeaders(group) => {
                    let target = if matches!(group, super::super::PermissionNewGroup::Decisions) {
                        thread.as_str()
                    } else {
                        execution.as_str()
                    };
                    admit_composite_key(budget, 32, &[group_tag(*group), target.as_bytes()])
                }
                ReadSql::AcceptancePayload => admit_composite_key(
                    budget,
                    33,
                    &[
                        b"pa",
                        execution.as_str().as_bytes(),
                        turn.ok_or(PermissionError::CorruptRecord)?.as_bytes(),
                    ],
                ),
                ReadSql::DecisionPayload => admit_composite_key(
                    budget,
                    34,
                    &[b"pd", pid.ok_or(PermissionError::CorruptRecord)?.as_bytes()],
                ),
                ReadSql::GrantHeaders => {
                    admit_composite_key(budget, 32, &[b"g", execution.as_str().as_bytes()])
                }
                ReadSql::GrantPayload => admit_composite_key(
                    budget,
                    35,
                    &[
                        b"g",
                        execution.as_str().as_bytes(),
                        interrupt
                            .ok_or(PermissionError::CorruptRecord)?
                            .as_str()
                            .as_bytes(),
                    ],
                ),
                ReadSql::OldAcceptanceHeaders => {
                    admit_composite_key(budget, 32, &[b"a", execution.as_str().as_bytes()])
                }
                ReadSql::OldAcceptancePayload => admit_composite_key(
                    budget,
                    33,
                    &[
                        b"a",
                        execution.as_str().as_bytes(),
                        turn.ok_or(PermissionError::CorruptRecord)?.as_bytes(),
                    ],
                ),
                ReadSql::JournalHeader(_) => {
                    let (_, revision) = journal.ok_or(PermissionError::CorruptRecord)?;
                    let revision = revision.to_be_bytes();
                    admit_composite_key(
                        budget,
                        36,
                        &[b"j", execution.as_str().as_bytes(), &revision],
                    )
                }
                ReadSql::JournalPayload(_) => {
                    let (sequence, revision) = journal.ok_or(PermissionError::CorruptRecord)?;
                    let sequence = sequence.to_be_bytes();
                    let revision = revision.to_be_bytes();
                    admit_composite_key(
                        budget,
                        37,
                        &[b"j", &sequence, execution.as_str().as_bytes(), &revision],
                    )
                }
                ReadSql::CommitRead => Ok(()),
            }
        })();
        budget.finish(result)
    }
    pub(in super::super) fn runtime_write_before_sql(
        budget: &mut PermissionEvidenceBudget,
        statement: &WriteSql,
        execution: &ExecutionId,
        thread: &ThreadId,
        next_revision: u64,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            sql_capacity(budget, matches!(statement, WriteSql::BeginImmediate))?;
            match statement {
                WriteSql::SnapshotCas => PermissionResourceRead { budget }
                    .admit_key(PermissionResourceKey::Execution(execution)),
                WriteSql::ThreadCas => PermissionResourceRead { budget }
                    .admit_key(PermissionResourceKey::Thread(thread)),
                WriteSql::InsertDispatchJournal => {
                    let revision = next_revision.to_be_bytes();
                    admit_composite_key(
                        budget,
                        38,
                        &[b"j", execution.as_str().as_bytes(), &revision],
                    )
                }
                WriteSql::BeginImmediate | WriteSql::Commit => Ok(()),
            }
        })();
        budget.finish(result)
    }

    pub(in super::super) fn runtime_admit_reference_grant(
        budget: &mut PermissionEvidenceBudget,
        execution: &ExecutionId,
        interrupt: &InterruptId,
    ) -> Result<(), PermissionError> {
        // New-only fixed registration control, before any derived key carrier.
        budget.reserve(256, 16)?;
        let domain = 20u8;
        let parts: &[&[u8]] = &[execution.as_str().as_bytes(), interrupt.as_str().as_bytes()];
        budget.reserve(128, parts.len())?;
        if budget.resource.attempted_keys == 16_384 {
            return budget.limit();
        }
        let bytes = parts
            .iter()
            .try_fold(1, |n, part| add(n, add(2, part.len())?))?;
        if bytes > 1_024 {
            return budget.limit();
        }
        budget.reserve(add(mul(6, bytes)?, 128)?, 1)?;
        budget.resource.attempted_keys += 1;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(bytes)
            .map_err(|_| PermissionError::LimitExceeded)?;
        encoded.push(domain);
        for part in parts {
            let len = u16::try_from(part.len()).map_err(|_| PermissionError::LimitExceeded)?;
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(part);
        }
        let k = budget.resource.keys.len();
        budget.reserve(add(mul(k, bytes)?, budget.resource.key_bytes)?, mul(2, k)?)?;
        if budget.resource.keys.iter().any(|old| old == &encoded) {
            return Ok(());
        }
        if k == 8_192 || budget.resource.referenced_grants == 256 {
            return budget.limit();
        }
        if k == budget.resource.keys.capacity() {
            let target = if k == 0 {
                4
            } else {
                mul(2, budget.resource.keys.capacity())?
            };
            budget.reserve(mul(add(target, k)?, std::mem::size_of::<Vec<u8>>())?, 0)?;
            budget
                .resource
                .keys
                .try_reserve_exact(target - k)
                .map_err(|_| PermissionError::LimitExceeded)?;
        }
        budget.resource.key_bytes = add(budget.resource.key_bytes, bytes)?;
        budget.resource.keys.push(encoded);
        budget.resource.referenced_grants += 1;
        Ok(())
    }
    const _: () = assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<Vec<u8>>() <= 32);

    impl RuntimeDispatchAdmission<'_> {
        pub(in super::super) fn is_ready_cancellation_c1(&self) -> bool {
            matches!(self.next, Phase::ReadyCancellationC1Committed)
        }
    }
    impl PermissionEvidenceBudget {
        pub(in super::super) fn admit_runtime_ready_cancellation_write<'budget>(
            &'budget mut self,
        ) -> Result<RuntimeDispatchAdmission<'budget>, PermissionError> {
            self.require_standard_account()?;
            let result = (|| {
                self.runtime_check_ready_cancellation()?;
                self.runtime_prepaid_ready_cancellation_fixed(512, 8)?;
                self.runtime_prepaid_ready_cancellation_fixed(64, 1)?;
                self.runtime_prepaid_ready_cancellation_fixed(640, 16)?;
                self.runtime_prepaid_ready_cancellation_fixed(256, 24)?;
                self.runtime_prepaid_ready_cancellation_fixed(1_024, 128)?;
                self.runtime_prepaid_ready_cancellation_fixed(128, 1)?;
                self.resource.immediate_writes += 1;
                if let Some(state) = self.runtime_state.as_mut() {
                    state.phase = Phase::AdmissionInProgress;
                }
                Ok(Phase::ReadyCancellationCommitted)
            })();
            let next = self.finish(result)?;
            Ok(RuntimeDispatchAdmission { budget: self, next })
        }
    }
    pub(in super::super) fn runtime_ready_cancellation_write_before_sql(
        budget: &mut PermissionEvidenceBudget,
        statement: &super::super::runtime_mutation::PermissionReadyCancellationWriteSql,
        execution: &ExecutionId,
        thread: &ThreadId,
        next_revision: u64,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            sql_capacity(budget, matches!(statement, super::super::runtime_mutation::PermissionReadyCancellationWriteSql::BeginImmediate))?;
            match statement {
                super::super::runtime_mutation::PermissionReadyCancellationWriteSql::SnapshotCas => PermissionResourceRead { budget }
                    .admit_key(PermissionResourceKey::Execution(execution)),
                super::super::runtime_mutation::PermissionReadyCancellationWriteSql::ThreadCas => PermissionResourceRead { budget }
                    .admit_key(PermissionResourceKey::Thread(thread)),
                super::super::runtime_mutation::PermissionReadyCancellationWriteSql::InsertCancelJournal => {
                    let revision = next_revision.to_be_bytes();
                    admit_composite_key(
                        budget,
                        38,
                        &[b"j", execution.as_str().as_bytes(), &revision],
                    )
                }
                super::super::runtime_mutation::PermissionReadyCancellationWriteSql::BeginImmediate | super::super::runtime_mutation::PermissionReadyCancellationWriteSql::Commit => Ok(()),
            }
        })();
        budget.finish(result)
    }
}

// Closed public legacy post-reader certificate uses these actual grow slots.
const _: () = assert!(std::mem::size_of::<RawContainer>() <= 128);
const _: () = assert!(std::mem::size_of::<KeySpan>() <= 128);
const _: () = assert!(std::mem::size_of::<Vec<u8>>() <= 32);

// Unused Human-only retained raw geometry. Its Runtime callers, quote replacement
// and certificate/account gates are separate; every earlier helper stays literal.
#[allow(dead_code)]
pub(super) mod runtime_raw_geometry {
    use super::*;

    // Only validation descendants can select a fixed codec brand.
    #[derive(Clone, Copy)]
    pub(super) enum RuntimeHumanCodecKind {
        Acceptance,
        Decision,
        Grant,
    }

    // No Default, Clone/Copy or externally supplied numeric constructor.
    // This opaque value publishes only with a complete actual codec success.
    pub(in crate::permissions) struct RuntimeHumanRawGeometry {
        kind: RuntimeHumanCodecKind,
        raw_len: usize,
        raw_nodes: usize,
        containers: usize,
        keys: usize,
        numbers: usize,
        preflight_depth_limit: usize,
        inserted_bytes: usize,
        inserted_nodes: usize,
    }

    impl RuntimeHumanRawGeometry {
        pub(in crate::permissions) fn raw_len(&self) -> usize {
            self.raw_len
        }
        pub(in crate::permissions) fn raw_nodes(&self) -> usize {
            self.raw_nodes
        }
        pub(in crate::permissions) fn containers(&self) -> usize {
            self.containers
        }
        pub(in crate::permissions) fn keys(&self) -> usize {
            self.keys
        }
        pub(in crate::permissions) fn numbers(&self) -> usize {
            self.numbers
        }
        pub(in crate::permissions) fn preflight_depth_limit(&self) -> usize {
            self.preflight_depth_limit
        }
        pub(in crate::permissions) fn inserted_bytes(&self) -> usize {
            self.inserted_bytes
        }
        pub(in crate::permissions) fn inserted_nodes(&self) -> usize {
            self.inserted_nodes
        }
        pub(in crate::permissions) fn is_acceptance(&self) -> bool {
            matches!(self.kind, RuntimeHumanCodecKind::Acceptance)
        }
        pub(in crate::permissions) fn is_decision(&self) -> bool {
            matches!(self.kind, RuntimeHumanCodecKind::Decision)
        }
        pub(in crate::permissions) fn is_grant(&self) -> bool {
            matches!(self.kind, RuntimeHumanCodecKind::Grant)
        }
    }

    pub(super) fn preflight_human_with_geometry(
        raw: &[u8],
        kind: RuntimeHumanCodecKind,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<RuntimeHumanRawGeometry, PermissionError> {
        let profile = match kind {
            RuntimeHumanCodecKind::Acceptance => RawProfile::Record(RecordKind::Acceptance),
            RuntimeHumanCodecKind::Decision => RawProfile::Record(RecordKind::Decision),
            RuntimeHumanCodecKind::Grant => RawProfile::Legacy,
        };
        let limit = profile.limit();
        if raw.len() > limit.bytes() {
            return budget.limit();
        }
        budget.reserve(raw.len(), 0)?;
        budget.reserve(mul(add(limit.depth(), 1)?, 512)?, 0)?;
        let mut frames = Vec::<RawFrame>::new();
        frames
            .try_reserve_exact(add(limit.depth(), 1)?)
            .map_err(|_| PermissionError::LimitExceeded)?;
        let mut containers = Vec::<RawContainer>::new();
        let mut position = 0;
        let mut nodes = 0;
        let mut numbers = 0;
        let mut root = false;
        loop {
            whitespace(raw, &mut position);
            if let Some(frame) = frames.last_mut() {
                let object = !containers[frame.container].array;
                match frame.state {
                    0 | 4 if object => {
                        if raw.get(position) == Some(&b'}') && frame.state == 0 {
                            position += 1;
                            frames.pop();
                            continue;
                        }
                        raw_node(&mut nodes, limit.nodes(), budget)?;
                        let key = raw_string(raw, &mut position)?;
                        let container = &mut containers[frame.container];
                        grow(&mut container.keys, budget)?;
                        frame.pending_key = Some(container.keys.len());
                        container.keys.push(key);
                        frame.state = 1;
                        continue;
                    }
                    1 => {
                        if raw.get(position) != Some(&b':') {
                            return Err(PermissionError::CorruptRecord);
                        }
                        position += 1;
                        frame.state = 2;
                        continue;
                    }
                    3 => {
                        let close = if object { b'}' } else { b']' };
                        if raw.get(position) == Some(&close) {
                            position += 1;
                            frames.pop();
                            continue;
                        }
                        if raw.get(position) != Some(&b',') {
                            return Err(PermissionError::CorruptRecord);
                        }
                        position += 1;
                        frame.state = 4;
                        continue;
                    }
                    0 if !object && raw.get(position) == Some(&b']') => {
                        position += 1;
                        frames.pop();
                        continue;
                    }
                    _ => {}
                }
            } else if root {
                if position != raw.len() {
                    return Err(PermissionError::CorruptRecord);
                }
                break;
            }
            let (parent, via_key) = if let Some(frame) = frames.last_mut() {
                let values = (Some(frame.container), frame.pending_key.take());
                frame.state = 3;
                values
            } else {
                root = true;
                (None, None)
            };
            raw_node(&mut nodes, limit.nodes(), budget)?;
            match raw.get(position).copied() {
                Some(b'{' | b'[') => {
                    if frames.len() == limit.depth() {
                        return budget.limit();
                    }
                    let array = raw[position] == b'[';
                    position += 1;
                    grow(&mut containers, budget)?;
                    let index = containers.len();
                    containers.push(RawContainer {
                        parent,
                        via_key,
                        array,
                        keys: Vec::new(),
                    });
                    frames.push(RawFrame {
                        container: index,
                        state: 0,
                        pending_key: None,
                    });
                }
                Some(b'"') => {
                    raw_string(raw, &mut position)?;
                }
                Some(b't') if raw.get(position..position + 4) == Some(b"true") => position += 4,
                Some(b'f') if raw.get(position..position + 5) == Some(b"false") => position += 5,
                Some(b'n') if raw.get(position..position + 4) == Some(b"null") => position += 4,
                Some(b'-' | b'0'..=b'9') => {
                    raw_number(raw, &mut position)?;
                    numbers += 1;
                }
                _ => return Err(PermissionError::CorruptRecord),
            }
        }
        // The raw grammar/value-key pass is complete before any decoded-key work.
        // In particular, the 16,385th raw node fails with exactly 16,384 charged nodes.
        budget.reserve(mul(containers.len(), 128)?, 0)?;
        let mut decoded = Vec::<Vec<String>>::new();
        decoded
            .try_reserve_exact(containers.len())
            .map_err(|_| PermissionError::LimitExceeded)?;
        let mut all_key_bytes = 0;
        let mut all_keys = 0;
        for container in &containers {
            budget.reserve(mul(container.keys.len(), 128)?, 0)?;
            let mut keys = Vec::new();
            keys.try_reserve_exact(container.keys.len())
                .map_err(|_| PermissionError::LimitExceeded)?;
            let mut key_bytes = 0;
            for span in &container.keys {
                let key = decode_key(raw, *span, budget)?;
                key_bytes = add(key_bytes, key.len())?;
                keys.push(key);
            }
            // Pay one complete comparison bound, after k/K are known, before sorting.
            budget.reserve(mul(mul(2, keys.len())?, key_bytes)?, 0)?;
            budget.reserve(
                add(mul(keys.len(), 16)?, mul(16, mul(keys.len(), keys.len())?)?)?,
                0,
            )?;
            let mut ordered = Vec::<usize>::new();
            ordered
                .try_reserve_exact(keys.len())
                .map_err(|_| PermissionError::LimitExceeded)?;
            for index in 0..keys.len() {
                let mut position = ordered.len();
                for (slot, previous) in ordered.iter().enumerate() {
                    match keys[*previous].cmp(&keys[index]) {
                        std::cmp::Ordering::Equal => return Err(PermissionError::CorruptRecord),
                        std::cmp::Ordering::Greater => {
                            position = slot;
                            break;
                        }
                        std::cmp::Ordering::Less => {}
                    }
                }
                ordered.insert(position, index);
            }
            all_key_bytes = add(all_key_bytes, key_bytes)?;
            all_keys = add(all_keys, keys.len())?;
            decoded.push(keys);
        }
        let mut inserted_bytes = 0;
        let mut inserted_nodes = 0;
        // Pay ancestry/field-name/presence lookup scans before performing them.
        budget.reserve(
            add(
                mul(32, all_key_bytes)?,
                add(mul(2048, all_keys)?, mul(128, containers.len())?)?,
            )?,
            0,
        )?;
        for (index, container) in containers.iter().enumerate() {
            let is_snapshot = match profile {
                RawProfile::Record(_) => {
                    container.parent == Some(0)
                        && container
                            .via_key
                            .is_some_and(|key| decoded[0][key] == "before_snapshot")
                }
                RawProfile::Snapshot => index == 0 && !container.array,
                RawProfile::ThreadJson | RawProfile::Legacy => false,
            };
            let is_task = container.parent.is_some_and(|parent| {
                let array = &containers[parent];
                array.array
                    && array.parent.is_some_and(|snapshot| {
                        let owner = &containers[snapshot];
                        let snapshot_owner = match profile {
                            RawProfile::Record(_) => {
                                owner.parent == Some(0)
                                    && owner
                                        .via_key
                                        .is_some_and(|key| decoded[0][key] == "before_snapshot")
                            }
                            RawProfile::Snapshot => snapshot == 0 && !owner.array,
                            RawProfile::ThreadJson | RawProfile::Legacy => false,
                        };
                        snapshot_owner
                            && array
                                .via_key
                                .is_some_and(|key| decoded[snapshot][key] == "tasks")
                    })
            });
            if is_snapshot && !decoded[index].iter().any(|key| key == "failure") {
                inserted_bytes = add(inserted_bytes, 15)?;
                inserted_nodes = add(inserted_nodes, 2)?;
            }
            if is_task {
                for (key, bytes) in [("attempt_id", 18), ("result", 14), ("interrupt", 17)] {
                    if !decoded[index].iter().any(|present| present == key) {
                        inserted_bytes = add(inserted_bytes, bytes)?;
                        inserted_nodes = add(inserted_nodes, 2)?;
                    }
                }
            }
        }
        // Typed materialization and serde's own paid second lexical/error-scratch walk.
        budget.reserve(
            add(
                add(add(raw.len(), inserted_bytes)?, mul(numbers, 32)?)?,
                mul(128, add(nodes, inserted_nodes)?)?,
            )?,
            add(nodes, inserted_nodes)?,
        )?;
        budget.reserve(add(mul(raw.len(), 2)?, 4096)?, 0)?;
        Ok(RuntimeHumanRawGeometry {
            kind,
            raw_len: raw.len(),
            raw_nodes: nodes,
            containers: containers.len(),
            keys: all_keys,
            numbers,
            preflight_depth_limit: limit.depth(),
            inserted_bytes,
            inserted_nodes,
        })
    }

    // Actual new layouts, not caller numeric bounds or lifetime probes.
    const _: () = assert!(std::mem::size_of::<RuntimeHumanCodecKind>() <= 16);
    const _: () = assert!(std::mem::size_of::<RuntimeHumanRawGeometry>() <= 96);
    const _: () =
        assert!(std::mem::size_of::<Result<RuntimeHumanRawGeometry, PermissionError>>() <= 128);
    const _: () = assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                RuntimeHumanRawGeometry,
            >,
        >() <= 128
    );
    const _: () = assert!(std::mem::size_of::<Vec<usize>>() <= 32);
    const _: () = assert!(std::mem::size_of::<Option<usize>>() <= 16);
    const _: () =
        assert!(std::mem::size_of::<Result<(), std::collections::TryReserveError>>() <= 128);
    const _: () = assert!(std::mem::size_of::<std::slice::Iter<'static, RawContainer>>() <= 32);
    const _: () = assert!(std::mem::size_of::<Option<&'static RawContainer>>() <= 16);
    const _: () = assert!(std::mem::size_of::<std::slice::Iter<'static, KeySpan>>() <= 32);
    const _: () = assert!(std::mem::size_of::<Option<&'static KeySpan>>() <= 16);
    const _: () = assert!(std::mem::size_of::<(Option<usize>, usize, usize)>() <= 32);
    const _: () = assert!(std::mem::size_of::<usize>() <= 8);
    const _: usize = 134_217_728;
}

// Exact retained-geometry std-interface layouts; these consts do not price events.
const _: () = assert!(std::mem::size_of::<&[RawContainer]>() <= 16);
const _: () = assert!(std::mem::size_of::<&[KeySpan]>() <= 16);
const _: () = assert!(std::mem::size_of::<&[usize]>() <= 16);
const _: () = assert!(std::mem::size_of::<std::alloc::Layout>() <= 16);
const _: () = assert!(std::mem::size_of::<std::alloc::LayoutError>() <= 8);
const _: () =
    assert!(std::mem::size_of::<Result<std::alloc::Layout, std::alloc::LayoutError>>() <= 32);
const _: () = assert!(std::mem::size_of::<(std::alloc::Layout, usize)>() <= 32);
const _: () = assert!(
    std::mem::size_of::<Result<(std::alloc::Layout, usize), std::alloc::LayoutError>>() <= 48
);
const _: () = assert!(std::mem::size_of::<std::collections::TryReserveError>() <= 128);
const _: () = assert!(
    std::mem::size_of::<Result<std::convert::Infallible, std::collections::TryReserveError>>()
        <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, std::collections::TryReserveError>,
            (),
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<u8>>() <= 8);
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<[u8]>>() <= 16);
const _: () = assert!(std::mem::size_of::<(std::ptr::NonNull<u8>, std::alloc::Layout)>() <= 32);
const _: () =
    assert!(std::mem::size_of::<Option<(std::ptr::NonNull<u8>, std::alloc::Layout)>>() <= 32);
const _: () = assert!(
    std::mem::size_of::<Result<std::alloc::Layout, std::collections::TryReserveError>>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, std::collections::TryReserveError>,
            std::alloc::Layout,
        >,
    >() <= 128
);
const _: () = assert!(
    std::mem::size_of::<Result<std::ptr::NonNull<[u8]>, std::collections::TryReserveError>>()
        <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, std::collections::TryReserveError>,
            std::ptr::NonNull<[u8]>,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<(usize, bool)>() <= 16);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            crate::permissions::runtime_mutation::PermissionRuntimeReadSession<
                'static,
                'static,
                'static,
            >,
            PermissionError,
        >,
    >() <= 8192
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            crate::permissions::runtime_mutation::PermissionRuntimeReadSession<
                'static,
                'static,
                'static,
            >,
        >,
    >() <= 8192
);

// Exact early current-resource optional execution identity carrier.
const _: () = assert!(std::mem::size_of::<Option<&'static b4_contract::ExecutionId>>() <= 8);

// Exact owned Option places used by current-profile and scope guards.
const _: () = assert!(std::mem::size_of::<Option<serde_json::Value>>() <= 128);
const _: () = assert!(std::mem::size_of::<Option<b4_contract::TaskResult>>() <= 128);
const _: () = assert!(std::mem::size_of::<Option<crate::store::ThreadOrigin>>() <= 128);
const _: () = assert!(std::mem::size_of::<Option<b4_contract::PendingApproval>>() <= 128);
const _: () = assert!(std::mem::size_of::<Option<b4_contract::AttemptId>>() <= 128);
const _: () = assert!(std::mem::size_of::<Option<b4_contract::ExecutionId>>() <= 128);

// Exact local standard-library carrier targets for retained geometry loops.
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<RawContainer>>() <= 8);
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<KeySpan>>() <= 8);
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<usize>>() <= 8);
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<[RawContainer]>>() <= 16);
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<[KeySpan]>>() <= 16);
const _: () = assert!(std::mem::size_of::<std::ptr::NonNull<[usize]>>() <= 16);
const _: () = assert!(std::mem::size_of::<*const RawContainer>() <= 8);
const _: () = assert!(std::mem::size_of::<*const KeySpan>() <= 8);
const _: () = assert!(std::mem::size_of::<*const usize>() <= 8);
const _: () = assert!(std::mem::size_of::<*mut usize>() <= 8);
const _: () = assert!(std::mem::size_of::<*mut [usize]>() <= 16);

// Exact private enum places used only by the new retained-profile output scope.
const _: () = assert!(std::mem::size_of::<CanonicalLimit>() <= 16);
const _: () = assert!(std::mem::size_of::<RawProfile>() <= 16);

// Concrete first-Dispatch equality-loop owning places; declaration-only.
const _: () = assert!(std::mem::size_of::<(CanonicalMeasure, CanonicalMeasure)>() <= 48);
const _: () = assert!(std::mem::size_of::<[(CanonicalMeasure, CanonicalMeasure); 7]>() <= 336);
const _: () = assert!(std::mem::size_of::<[(CanonicalMeasure, CanonicalMeasure); 8]>() <= 384);
const _: () = assert!(
    std::mem::size_of::<std::array::IntoIter<(CanonicalMeasure, CanonicalMeasure), 7>>() <= 352
);
const _: () = assert!(
    std::mem::size_of::<std::array::IntoIter<(CanonicalMeasure, CanonicalMeasure), 8>>() <= 400
);
const _: () = assert!(
    std::mem::size_of::<[(CanonicalMeasure, CanonicalMeasure); 8]>()
        == std::mem::size_of::<[(CanonicalMeasure, CanonicalMeasure); 7]>() + 48
);
const _: () = assert!(
    std::mem::size_of::<std::array::IntoIter<(CanonicalMeasure, CanonicalMeasure), 8>>()
        == std::mem::size_of::<std::array::IntoIter<(CanonicalMeasure, CanonicalMeasure), 7>>()
            + 48
);
const _: () = assert!(std::mem::size_of::<Option<(CanonicalMeasure, CanonicalMeasure)>>() <= 64);

// Concrete borrowed AttemptId carrier targets used by the first Dispatch reader.
const _: () =
    assert!(std::mem::size_of::<Result<&'static b4_contract::AttemptId, PermissionError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static b4_contract::AttemptId,
        >,
    >() <= 128
);

// Separate cancellation origin/domain; the original Dispatch initializer is literal.
impl PermissionEvidenceBudget {
    pub(super) fn begin_runtime_ready_cancellation(
        &mut self,
        input_profile: super::runtime_mutation::PermissionRuntimeInputProfile,
    ) -> Result<std::sync::Arc<super::runtime_mutation::PermissionRuntimeOrigin>, PermissionError>
    {
        self.require_standard_account()?;
        let result = super::runtime_mutation::runtime_checked_ready_cancellation_entry::<_, _, 64>(
            || {
                self.runtime_check_ready_cancellation()?;
                if !matches!(input_profile,
                    super::runtime_mutation::PermissionRuntimeInputProfile::Strict
                    | super::runtime_mutation::PermissionRuntimeInputProfile::NormalReadyCancellation)
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                // O:allocation256/4 +phase512/8 +clone64/1 +identity64/1
                // +both final strong-owner drops128/2, before any new guard/action.
                self.reserve(2_804_679, 35_910)?;
                // New-only fresh-ledger guards and closed Result/cleanup carriers.
                self.runtime_check_ready_cancellation()?;
                if self.runtime_state.is_some()
                    || self.resource.reads != 0
                    || self.resource.combined_reads != 0
                    || self.resource.immediate_writes != 0
                    || self.resource.referenced_grants != 0
                    || self.resource.sql_capacity != 0
                    || self.resource.attempted_keys != 0
                    || self.resource.executions != 0
                    || self.resource.key_bytes != 0
                    || !self.resource.keys.is_empty()
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
                // Resource R1 exactly once, before the first port; its factory must
                // recognize this started phase and must not admit Resource again.
                self.runtime_check_ready_cancellation()?;
                self.resource.reads += 1;
                let origin = std::sync::Arc::new(super::runtime_mutation::PermissionRuntimeOrigin);
                self.runtime_state = Some(super::runtime_mutation::PermissionRuntimeState {
                    origin: std::sync::Arc::clone(&origin),
                    input_profile,
                    kind:
                        super::runtime_mutation::PermissionRuntimeOperationKind::ReadyCancellation,
                    phase:
                        super::runtime_mutation::PermissionRuntimePhase::ReadyCancellationR1Started,
                });
                Ok(origin)
            },
        );
        self.finish(result)
    }
}

// Genuine selected Read role, phase, tuple and Admission publication types.
// Each value is used by the factory/admission/helper route above.
const _: () = assert!(
    std::mem::size_of::<(
        u8,
        super::runtime_mutation::PermissionRuntimePhase,
        (usize, usize, usize),
        u8
    )>() <= 256
);
const _: () = assert!(
    std::mem::size_of::<Result<super::runtime_mutation::PermissionRuntimeReadRole, PermissionError>>(
    ) <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            super::runtime_mutation::PermissionRuntimeReadRole,
        >,
    >() <= 256
);
const _: () = assert!(
    std::mem::size_of::<Result<super::runtime_mutation::PermissionRuntimePhase, PermissionError>>()
        <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            super::runtime_mutation::PermissionRuntimePhase,
        >,
    >() <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            RuntimeDispatchAdmission<'static>,
        >,
    >() <= 128
);

// Genuine initializer caller Try output; distinct from its existing Result cap.
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            std::sync::Arc<super::runtime_mutation::PermissionRuntimeOrigin>,
        >,
    >() <= 128
);

// Additional whole canonical-type bounds from the pinned compiler layout audit.
const _: () = assert!(std::mem::size_of::<CanonicalFrame<'static>>() <= 128);
const _: () = assert!(std::mem::size_of::<Option<CanonicalFrame<'static>>>() <= 128);
const _: () = assert!(std::mem::size_of::<CanonicalMeasure>() <= 32);
const _: () = assert!(std::mem::size_of::<Result<CanonicalMeasure, PermissionError>>() <= 80);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, CanonicalMeasure>,
    >() <= 80
);
const _: () = assert!(std::mem::size_of::<CanonicalValue<'static>>() <= 112);
const _: () = assert!(std::mem::size_of::<WalkState>() <= 48);

// Qualify the actual borrowed output callback, then invoke it exactly once.
fn runtime_checked_output_hook_mut<F, T, const F_BYTES: usize>(
    mut check: F,
) -> Result<T, PermissionError>
where
    F: FnMut() -> Result<T, PermissionError>,
{
    const {
        assert!(std::mem::size_of::<F>() <= F_BYTES);
    };
    check()
}

// Preserve the actual map_err callable; this helper does not invoke it.
fn runtime_checked_output_mapper<F, E>(mapper: F) -> F
where
    F: FnOnce(E) -> PermissionError,
{
    const {
        assert!(std::mem::size_of::<F>() <= 64);
    };
    mapper
}

// Preserve the actual immutable decimal-width callable and its selected type.
fn runtime_checked_output_digits<F>(digits: F) -> F
where
    F: Fn(u64) -> usize,
{
    const {
        assert!(std::mem::size_of::<F>() <= 64);
    };
    digits
}

// Inert whole named bounds for the private mandatory-domain compiler prospect.
// N22: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<CanonicalWorkspace<'static>, PermissionError>>() <= 128);
    assert!(std::mem::align_of::<Result<CanonicalWorkspace<'static>, PermissionError>>() <= 16);
};
// N23: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                CanonicalWorkspace<'static>,
            >,
        >() <= 128
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                CanonicalWorkspace<'static>,
            >,
        >() <= 16
    );
};
// N24: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<AcceptanceWire>() <= 4096);
    assert!(std::mem::align_of::<AcceptanceWire>() <= 16);
};
// N25: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<AcceptanceWire, serde_json::Error>>() <= 4096);
    assert!(std::mem::align_of::<Result<AcceptanceWire, serde_json::Error>>() <= 16);
};
// N26: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<AcceptanceWire, PermissionError>>() <= 4096);
    assert!(std::mem::align_of::<Result<AcceptanceWire, PermissionError>>() <= 16);
};
// N27: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                AcceptanceWire,
            >,
        >() <= 4096
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                AcceptanceWire,
            >,
        >() <= 16
    );
};
// N28: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<DecisionWire>() <= 4096);
    assert!(std::mem::align_of::<DecisionWire>() <= 16);
};
// N29: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<DecisionWire, serde_json::Error>>() <= 4096);
    assert!(std::mem::align_of::<Result<DecisionWire, serde_json::Error>>() <= 16);
};
// N30: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<DecisionWire, PermissionError>>() <= 4096);
    assert!(std::mem::align_of::<Result<DecisionWire, PermissionError>>() <= 16);
};
// N31: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, DecisionWire>,
        >() <= 4096
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, DecisionWire>,
        >() <= 16
    );
};
// N32: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<AcceptedDecisionWire>() <= 1024);
    assert!(std::mem::align_of::<AcceptedDecisionWire>() <= 16);
};
// N33: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<crate::approval::GrantRecord, serde_json::Error>>() <= 4096);
    assert!(std::mem::align_of::<Result<crate::approval::GrantRecord, serde_json::Error>>() <= 16);
};
// N34: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<crate::approval::GrantRecord, PermissionError>>() <= 4096);
    assert!(std::mem::align_of::<Result<crate::approval::GrantRecord, PermissionError>>() <= 16);
};
// N35: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                crate::approval::GrantRecord,
            >,
        >() <= 4096
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                crate::approval::GrantRecord,
            >,
        >() <= 16
    );
};
// N36: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<crate::JournalEntry, serde_json::Error>>() <= 512);
    assert!(std::mem::align_of::<Result<crate::JournalEntry, serde_json::Error>>() <= 16);
};
// N37: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<crate::JournalEntry, PermissionError>>() <= 512);
    assert!(std::mem::align_of::<Result<crate::JournalEntry, PermissionError>>() <= 16);
};
// N38: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                crate::JournalEntry,
            >,
        >() <= 512
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                crate::JournalEntry,
            >,
        >() <= 16
    );
};
// N39: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<PermissionAcceptance, PermissionError>>() <= 4096);
    assert!(std::mem::align_of::<Result<PermissionAcceptance, PermissionError>>() <= 16);
};
// N40: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionAcceptance,
            >,
        >() <= 4096
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionAcceptance,
            >,
        >() <= 16
    );
};

// Qualify the same actual callback value without invoking or replacing it.
fn runtime_checked_mandatory_callable<F>(callback: F) -> F {
    const { assert!(std::mem::size_of::<F>() <= 128) };
    callback
}

// Preserve the actual immediate callback and its existing full result.
fn runtime_checked_mandatory_iife<F, T>(mut callback: F) -> Result<T, PermissionError>
where
    F: FnMut() -> Result<T, PermissionError>,
{
    const { assert!(std::mem::size_of::<F>() <= 512) };
    callback()
}

// The actual decimal callable consumes this singleton argument tuple.
const _: () = assert!(std::mem::size_of::<(u64,)>() <= 8);

// Exact whole nominal carriers for the selected finite cancellation protocol.
const _: () = assert!(std::mem::size_of::<b4_contract::IdError>() <= 512);
const _: () = assert!(std::mem::size_of::<Result<(), b4_contract::IdError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, b4_contract::IdError>, ()>,
    >() <= 512
);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::ThreadId, b4_contract::IdError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, b4_contract::IdError>,
            b4_contract::ThreadId,
        >,
    >() <= 512
);
const _: () = assert!(std::mem::size_of::<Result<b4_contract::ThreadId, PermissionError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            b4_contract::ThreadId,
        >,
    >() <= 512
);
const _: () = assert!(
    std::mem::size_of::<Result<b4_contract::ThreadGeneration, b4_contract::IdError>>() <= 512
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, b4_contract::IdError>,
            b4_contract::ThreadGeneration,
        >,
    >() <= 512
);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::ThreadGeneration, PermissionError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            b4_contract::ThreadGeneration,
        >,
    >() <= 512
);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::ExecutionId, b4_contract::IdError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, b4_contract::IdError>,
            b4_contract::ExecutionId,
        >,
    >() <= 512
);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::ExecutionId, PermissionError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            b4_contract::ExecutionId,
        >,
    >() <= 512
);
const _: () = assert!(std::mem::size_of::<Result<Snapshot, serde_json::Error>>() <= 4_096);
const _: () = assert!(std::mem::size_of::<Result<Snapshot, PermissionError>>() <= 4_096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, Snapshot>,
    >() <= 4_096
);
const _: () = assert!(
    std::mem::size_of::<Result<serde_json::Map<String, serde_json::Value>, serde_json::Error>>()
        <= 4_096
);
const _: () = assert!(
    std::mem::size_of::<Result<serde_json::Map<String, serde_json::Value>, PermissionError>>()
        <= 4_096
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            serde_json::Map<String, serde_json::Value>,
        >,
    >() <= 4_096
);
const _: () =
    assert!(std::mem::size_of::<Result<crate::ThreadOrigin, serde_json::Error>>() <= 4_096);
const _: () = assert!(std::mem::size_of::<Result<crate::ThreadOrigin, PermissionError>>() <= 4_096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            crate::ThreadOrigin,
        >,
    >() <= 4_096
);
const _: () = assert!(
    std::mem::size_of::<Option<Result<b4_contract::ExecutionId, PermissionError>>>() <= 4_096
);
const _: () = assert!(
    std::mem::size_of::<Result<Option<b4_contract::ExecutionId>, PermissionError>>() <= 4_096
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            Option<b4_contract::ExecutionId>,
        >,
    >() <= 4_096
);
const _: () =
    assert!(std::mem::size_of::<Option<Result<crate::ThreadOrigin, PermissionError>>>() <= 4_096);
const _: () =
    assert!(std::mem::size_of::<Result<Option<crate::ThreadOrigin>, PermissionError>>() <= 4_096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            Option<crate::ThreadOrigin>,
        >,
    >() <= 4_096
);

// New ReadyAuthorization quote proposals. Their complete Source/application
// proof is separate from static compilation; no old cancellation fee is reused.
const READY_AUTHORIZATION_FIXED_BYTES: usize = 393_216;
const READY_AUTHORIZATION_FIXED_NODES: usize = 4_096;
const READY_AUTHORIZATION_WRITER_PREFIX_BYTES: usize = 9_920;
const READY_AUTHORIZATION_WRITER_PREFIX_NODES: usize = 11;
const READY_AUTHORIZATION_SHARED_CHECK_BYTES: usize = 384;
const READY_AUTHORIZATION_SHARED_CHECK_NODES: usize = 1;

impl PermissionEvidenceBudget {
    pub(super) fn begin_ready_authorization(&mut self) -> Result<(), PermissionError> {
        // The Source proposal enumerates the inception checks and first
        // reserve refusal separately. Its trusted inception selection is a
        // prerequisite to release; a failed reserve cannot fund itself.
        let fresh = !self.exhausted
            && !self.ready_authorization_entered
            && self.acceptance_identity.is_none()
            && self.runtime_state.is_none()
            && self.resource.reads == 0
            && self.usage.encoded_bytes == 0
            && self.usage.structural_nodes == 0;
        self.require_standard_account()?;
        self.reserve(
            READY_AUTHORIZATION_FIXED_BYTES,
            READY_AUTHORIZATION_FIXED_NODES,
        )?;
        if !fresh {
            return Err(PermissionError::UnsupportedProfile);
        }
        // New capability/registration and writer purchase-refusal graph, paid
        // before authorize_ready_inner can enter any of those new interfaces.
        self.reserve(
            READY_AUTHORIZATION_WRITER_PREFIX_BYTES,
            READY_AUTHORIZATION_WRITER_PREFIX_NODES,
        )?;
        self.ready_authorization_entered = true;
        Ok(())
    }

    /// Distinct fresh-session reader admission; it confers no permission use.
    pub fn ready_authorization_resource_read(
        &mut self,
    ) -> Result<PermissionResourceRead<'_>, PermissionError> {
        self.reserve(0, 0)?;
        if !self.ready_authorization_entered || self.resource.reads != 0 {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.resource_read()
    }

    pub(super) fn finish_ready_authorization<T>(
        &mut self,
        result: Result<T, PermissionError>,
    ) -> Result<T, PermissionError> {
        // The entered flag stays monotone. No second entry/refund/reset exists.
        self.finish(result)
    }
}

pub(super) fn quote_ready_authorization_context_owner(
    resource: &crate::approval::ApprovalResource,
    context: &crate::approval::ApprovalContext,
    catalog: &super::PermissionCatalog,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let origin = match &resource.thread.origin {
        Some(origin) => origin,
        None => return Err(crate::approval::ApprovalError::Unauthorized.into()),
    };
    let identity = catalog.application_identity();
    let fields = [
        resource.thread.id.as_str(),
        resource.thread.generation.as_str(),
        origin.application.as_str(),
        origin.tenant.as_str(),
        origin.principal.as_str(),
        identity.revision.as_str(),
        resource.execution.snapshot.definition.revision.as_str(),
        context.policy_revision.as_str(),
    ];
    let mut bytes = 0;
    for field in fields {
        bytes = add(bytes, field.len())?;
    }
    // Eight new independent invocation String owners, before the first copy.
    // The existing context quote retains its literal copy/API amounts.
    budget.reserve(add(mul(128, 8)?, mul(6, bytes)?)?, 8)
}

pub(super) fn quote_ready_authorization_search(
    snapshot: &Snapshot,
    command: &b4_kernel::PermissionAuthorizationCommand,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let tasks = snapshot.tasks.len();
    // One selected-ID search plus the complete potential marker-ID scan.
    // All relevant IDs are actual checked <=256-byte identities, use69.
    budget.reserve(mul(1_024, tasks)?, mul(4, tasks)?)?;
    budget.reserve(
        add(
            add(
                snapshot.execution_id.as_str().len(),
                command.execution_id().as_str().len(),
            )?,
            add(
                snapshot.turn_id.as_str().len(),
                command.turn_id().as_str().len(),
            )?,
        )?,
        4,
    )
}

pub(super) fn quote_ready_authorization_bindings(
    snapshot: &Snapshot,
    task: &TaskRecord,
    command: &b4_kernel::PermissionAuthorizationCommand,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let current_definition = measure(
        CanonicalValue::Definition(&snapshot.definition),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    let commanded_definition = measure(
        CanonicalValue::Definition(command.definition()),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(
        add(current_definition.bytes, commanded_definition.bytes)?,
        add(current_definition.nodes, commanded_definition.nodes)?,
    )?;
    let current_spec = measure(
        CanonicalValue::TaskSpec(&task.spec),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    let commanded_spec = measure(
        CanonicalValue::TaskSpec(command.task_spec()),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(
        add(current_spec.bytes, commanded_spec.bytes)?,
        add(current_spec.nodes, commanded_spec.nodes)?,
    )?;
    let current_input = measure(
        CanonicalValue::Input(&task.input),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    let commanded_input = measure(
        CanonicalValue::Input(command.input()),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    let current_values = measure(
        CanonicalValue::Input(&snapshot.values),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(
        add(current_input.bytes, commanded_input.bytes)?,
        add(current_input.nodes, commanded_input.nodes)?,
    )?;
    budget.reserve(
        add(current_input.bytes, current_values.bytes)?,
        add(current_input.nodes, current_values.nodes)?,
    )?;
    // Every actual TaskSpec comparison in the selected batch membership search.
    // The measured full current Definition includes all possible batch subjects.
    let batch_length = match snapshot.definition.batches.get(snapshot.batch_index) {
        Some(batch) => batch.len(),
        None => 0,
    };
    budget.reserve(
        add(
            mul(batch_length, current_spec.bytes)?,
            current_definition.bytes,
        )?,
        add(
            mul(batch_length, current_spec.nodes)?,
            current_definition.nodes,
        )?,
    )
}

// These are direct whole-type prospects, not payload/field/callback proxies.
const _: () = assert!(std::mem::size_of::<PermissionEvidenceBudget>() <= 256);
const _: () = assert!(std::mem::size_of::<PermissionResourceRead<'static>>() <= 16);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionResourceRead<'static>, PermissionError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionResourceRead<'static>,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<[&str; 8]>() <= 128);
const _: () = assert!(std::mem::size_of::<bool>() <= 1);

// Source-qualified metadata guard; the actual callback is returned unchanged.
// Its argument/formal/vacancy/return transports remain explicit protocol atoms.
pub(super) fn ready_authorization_checked_metadata_value<F>(value: F) -> F {
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}

// The encoding argument is a whole sixth sqlite3_bind_text64 parameter.
const _: () = assert!(std::mem::size_of::<std::ffi::c_uchar>() <= 8);

fn ready_authorization_checked_task_attempt<F>(value: F) -> F
where
    F: for<'a> FnMut(&'a TaskRecord) -> Option<&'a b4_contract::AttemptId>,
{
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}

fn ready_authorization_checked_task_interrupt<F>(value: F) -> F
where
    F: for<'a> FnMut(&'a TaskRecord) -> Option<&'a PendingApproval>,
{
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}

// Complete materialized Map carrier; no Result or field-layout proxy.
const _: () = assert!(std::mem::size_of::<serde_json::Map<String, serde_json::Value>>() <= 4_096);

// Exact reached Kernel validation collection headers and terminal value.
const _: () = {
    assert!(std::mem::size_of::<std::collections::BTreeSet<&b4_contract::TurnId>>() <= 512);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&b4_contract::AttemptId>>() <= 512);
    assert!(std::mem::size_of::<Vec<&b4_contract::TaskSpec>>() <= 512);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&b4_contract::TaskId>>() <= 512);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&b4_contract::EffectId>>() <= 512);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&b4_contract::InterruptId>>() <= 512);
    assert!(std::mem::size_of::<serde_json::Value>() <= 4_096);
};

// Exact late Failed-prefix reducer interfaces and temporary container headers.
const _: () = {
    assert!(std::mem::size_of::<Vec<&b4_contract::TaskRecord>>() <= 512);
    assert!(
        std::mem::size_of::<
            std::collections::BTreeMap<&str, (std::collections::BTreeSet<usize>, bool)>,
        >() <= 512
    );
    assert!(std::mem::size_of::<std::collections::BTreeSet<usize>>() <= 128);
    assert!(
        std::mem::size_of::<Result<serde_json::Map<String, serde_json::Value>, &'static str>>()
            <= 4_096
    );
    assert!(std::mem::size_of::<Option<&'static str>>() <= 128);
};

// New-role writer purchase. Old Ready cancellation and ordinary quotes remain literal.
const READY_AUTHORIZATION_WRITER_BYTES: usize = 1_048_576;
const READY_AUTHORIZATION_WRITER_NODES: usize = 24_576;
impl PermissionEvidenceBudget {
    pub(super) fn begin_ready_authorization_writer(&mut self) -> Result<(), PermissionError> {
        self.require_standard_account()?;
        self.reserve(0, 0)?;
        if !self.ready_authorization_entered
            || self.ready_authorization_writer_entered
            || self.runtime_state.is_some()
            || self.acceptance_identity.is_some()
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        // The Source-bound entry extension buys this exact refusal/purchase prefix
        // before entry here. P02 then buys only the newly entered writer graph.
        // Both applications remain prospective until independently qualified.
        self.reserve(
            READY_AUTHORIZATION_WRITER_BYTES,
            READY_AUTHORIZATION_WRITER_NODES,
        )?;
        self.ready_authorization_writer_entered = true;
        Ok(())
    }
}
pub(super) fn quote_ready_registration(
    definition: &Definition,
    entries: &[super::ToolBindingRegistration],
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let selected = measure(
        CanonicalValue::Definition(definition),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    for entry in entries {
        let candidate = measure(
            CanonicalValue::Definition(&entry.definition),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        budget.reserve(
            add(add(selected.bytes, candidate.bytes)?, 512)?,
            add(add(selected.nodes, candidate.nodes)?, 2)?,
        )?;
    }
    Ok(())
}
pub(super) fn ready_use_decode_creator(
    cells: PermissionExecutionCells<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::StoredExecution, PermissionError> {
    // An inactive source is a real stored execution. No active-execution or
    // current metadata-owner assertion is fabricated for it.
    resource_execution(cells, budget)
}
pub(super) fn ready_use_status(status: b4_contract::ExecutionStatus) -> &'static str {
    match status {
        b4_contract::ExecutionStatus::Accepted => "\"accepted\"",
        b4_contract::ExecutionStatus::Running => "\"running\"",
        b4_contract::ExecutionStatus::WaitingForApproval => "\"waiting_for_approval\"",
        b4_contract::ExecutionStatus::Completed => "\"completed\"",
        b4_contract::ExecutionStatus::Failed => "\"failed\"",
        b4_contract::ExecutionStatus::CancelRequested => "\"cancel_requested\"",
        b4_contract::ExecutionStatus::Cancelled => "\"cancelled\"",
        b4_contract::ExecutionStatus::RecoveryBlocked => "\"recovery_blocked\"",
    }
}
// Geometry is selected once by genuine NewUse inception; later accounts do
// not infer it from their ceilings or IDs. Strict and cancellation are unchanged.
#[allow(clippy::too_many_arguments)]
pub(super) fn runtime_use_inherited_geometry(
    profile: super::runtime_mutation::PermissionRuntimeInputProfile,
    normal_dispatch_origin: bool,
    decision: &PermissionDecision,
    acceptance: &PermissionAcceptance,
    grant: &crate::approval::GrantRecord,
    creator: &crate::StoredExecution,
    journals: &[crate::JournalEntry],
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    if profile != super::runtime_mutation::PermissionRuntimeInputProfile::NormalNewUse {
        return Ok(());
    }
    let cancellation = budget.runtime_state.as_ref().is_some_and(|state| {
        matches!(
            state.kind,
            super::runtime_mutation::PermissionRuntimeOperationKind::ReadyCancellation
                | super::runtime_mutation::PermissionRuntimeOperationKind::UseCancellation
        )
    });
    let larger_history = normal_dispatch_origin
        && !cancellation
        && (budget.runtime_is_normal_new_use_dispatch() || budget.use_renewal_production_entered);
    for (value, limit, bytes, nodes, depth) in [
        (
            CanonicalValue::Record(BorrowedRecord::Acceptance(acceptance)),
            CanonicalLimit::Evidence64KiB,
            if larger_history { 16_384 } else { 8_192 },
            768,
            64,
        ),
        (
            CanonicalValue::Snapshot(&acceptance.before_snapshot),
            CanonicalLimit::ResourceSnapshot,
            if larger_history { 8_192 } else { 4_096 },
            512,
            64,
        ),
        (
            CanonicalValue::Record(BorrowedRecord::Decision(decision)),
            CanonicalLimit::Evidence64KiB,
            4_096,
            67,
            64,
        ),
        (
            CanonicalValue::Scope(ScopeParts::from_scope(&decision.scope)),
            CanonicalLimit::Evidence64KiB,
            2_070,
            55,
            64,
        ),
        (
            CanonicalValue::ScopeBody(ScopeParts::from_scope(&decision.scope)),
            CanonicalLimit::Evidence64KiB,
            2_048,
            51,
            64,
        ),
        (
            CanonicalValue::LegacyGrant(grant),
            CanonicalLimit::LegacyRecord,
            4_096,
            83,
            65,
        ),
        (
            CanonicalValue::Json(&grant.prompt),
            CanonicalLimit::LegacyPrompt,
            128,
            16,
            64,
        ),
        (
            CanonicalValue::Snapshot(&creator.snapshot),
            CanonicalLimit::ResourceSnapshot,
            if larger_history
                && matches!(
                    creator.snapshot.status,
                    b4_contract::ExecutionStatus::Completed | b4_contract::ExecutionStatus::Failed
                )
            {
                8_192
            } else {
                4_096
            },
            if matches!(
                creator.snapshot.status,
                b4_contract::ExecutionStatus::Completed | b4_contract::ExecutionStatus::Failed
            ) {
                512
            } else if creator.snapshot.status == b4_contract::ExecutionStatus::Cancelled
                && creator.snapshot.revision == 6
            {
                256
            } else {
                100
            },
            64,
        ),
    ] {
        let q = measure(value, limit, budget)?;
        if q.bytes > bytes || q.nodes > nodes || q.depth > depth {
            return Err(PermissionError::UnsupportedProfile);
        }
        // Complete variable comparison/owner endpoints before the relation work.
        budget.reserve(add(mul(128, q.nodes)?, mul(6, q.bytes)?)?, q.nodes)?;
    }
    for journal in journals {
        let q = writer_journal_measure(journal)?;
        if !profile.historical_journal(q) {
            return Err(PermissionError::UnsupportedProfile);
        }
        budget.reserve(add(mul(128, q.nodes)?, mul(6, q.bytes)?)?, q.nodes)?;
    }
    Ok(())
}
/// Intrinsic A has proved the complete borrowed Resume. Prove every field of
/// its subsequent Cancelled projection without a historical Snapshot clone.
// Keep the complete borrowed historical evidence, current bindings and account explicit.
#[allow(clippy::too_many_arguments)]
pub(super) fn ready_use_validate_source(
    decision: &PermissionDecision,
    acceptance: &PermissionAcceptance,
    grant: &crate::approval::GrantRecord,
    creator: &crate::StoredExecution,
    journals: &[crate::JournalEntry],
    current: &crate::approval::ApprovalResource,
    invocation: &crate::InvocationContext,
    registration: &super::ToolBindingRegistration,
    application: &ApplicationIdentity,
    budget: &mut PermissionEvidenceBudget,
) -> Result<bool, PermissionError> {
    original_source_relation(
        decision,
        acceptance,
        grant,
        creator,
        journals,
        current,
        invocation,
        registration,
        application,
        budget,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn original_running_validate_source(
    decision: &PermissionDecision,
    acceptance: &PermissionAcceptance,
    grant: &crate::approval::GrantRecord,
    creator: &crate::StoredExecution,
    journals: &[crate::JournalEntry],
    current: &crate::approval::ApprovalResource,
    invocation: &crate::InvocationContext,
    registration: &super::ToolBindingRegistration,
    application: &ApplicationIdentity,
    budget: &mut PermissionEvidenceBudget,
) -> Result<bool, PermissionError> {
    original_source_relation(
        decision,
        acceptance,
        grant,
        creator,
        journals,
        current,
        invocation,
        registration,
        application,
        budget,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn original_source_relation(
    decision: &PermissionDecision,
    acceptance: &PermissionAcceptance,
    grant: &crate::approval::GrantRecord,
    creator: &crate::StoredExecution,
    journals: &[crate::JournalEntry],
    current: &crate::approval::ApprovalResource,
    invocation: &crate::InvocationContext,
    registration: &super::ToolBindingRegistration,
    application: &ApplicationIdentity,
    budget: &mut PermissionEvidenceBudget,
    original_running: bool,
) -> Result<bool, PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    let before = &acceptance.before_snapshot;
    let cancelled = &creator.snapshot;
    // The narrow body admits one complete task/batch. It does not drop siblings.
    if before.definition.batches.len() != 1
        || before.definition.batches[0].len() != 1
        || before.batch_index != 0
        || before.tasks.len() != 1
        || cancelled.tasks.len() != 1
        || acceptance.decisions.len() != 1
        || !matches!(journals.len(), 5..=7)
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let a = &acceptance.decisions[0];
    let task = &before.tasks[0];
    let final_task = &cancelled.tasks[0];
    let pending = task
        .interrupt
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let binding = a.binding.as_ref().ok_or(PermissionError::CorruptRecord)?;
    let scope = &decision.scope;
    let original_scope = ScopeParts {
        thread_id: &acceptance.thread_id,
        thread_generation: &acceptance.thread_generation,
        origin: &acceptance.origin,
        application: &acceptance.application,
        route: &before.definition.route,
        definition: &before.definition,
        effect_id: &binding.effect_id,
        tool_id: &binding.tool_id,
        tool_revision: &binding.tool_revision,
        policy_revision: &acceptance.policy_revision,
        constraint_revision: &binding.constraint_revision,
    };
    let source_q = measure(
        CanonicalValue::Scope(ScopeParts::from_scope(scope)),
        CanonicalLimit::Evidence64KiB,
        budget,
    )?;
    let original_q = measure(
        CanonicalValue::Scope(original_scope),
        CanonicalLimit::Evidence64KiB,
        budget,
    )?;
    budget.reserve(
        add(source_q.bytes, original_q.bytes)?,
        add(source_q.nodes, original_q.nodes)?,
    )?;
    for snapshot in [before, cancelled] {
        let q = measure(
            CanonicalValue::Snapshot(snapshot),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        budget.reserve(mul(3, q.bytes)?, mul(3, q.nodes)?)?;
    }
    budget.reserve(32_768, 128)?;
    if decision.record_version != 1
        || acceptance.record_version != 1
        || decision.creator_execution_id != before.execution_id
        || decision.creator_turn_id != acceptance.turn_id
        || decision.creator_task_id != task.spec.id
        || decision.creator_interrupt_id != pending.id
        || a.decision != ApprovalDecision::Always
        || a.permission_id.as_ref() != Some(&decision.permission_id)
        || a.task_id != task.spec.id
        || a.interrupt_id != pending.id
        || a.grant_ref.execution_id != grant.execution_id
        || a.grant_ref.interrupt_id != grant.interrupt_id
        || a.task_parking_revision != grant.task_parking_revision
        || a.proposal_digest != grant.proposal_digest
        || scope.thread_id != acceptance.thread_id
        || scope.thread_generation != acceptance.thread_generation
        || scope.origin != acceptance.origin
        || scope.application != acceptance.application
        || scope.route != before.definition.route
        || scope.definition != before.definition
        || scope.effect_id != binding.effect_id
        || scope.tool_id != binding.tool_id
        || scope.tool_revision != binding.tool_revision
        || scope.policy_revision != acceptance.policy_revision
        || scope.constraint_revision != binding.constraint_revision
        || !grant.consumed
        || grant.thread_id != acceptance.thread_id
        || grant.thread_generation != acceptance.thread_generation
        || grant.origin != acceptance.origin
        || grant.execution_id != before.execution_id
        || grant.route != before.definition.route
        || grant.definition_revision != before.definition.revision
        || grant.task_id != task.spec.id
        || grant.effect_id != task.spec.effect_id
        || grant.interrupt_id != pending.id
        || grant.prompt != pending.prompt
        || grant.task_parking_revision != before.revision
        || grant.policy_revision != acceptance.policy_revision
        || grant.issued_at_ms != before.updated_at_ms
        || grant.issued_at_ms > acceptance.facts.now_ms
        || acceptance.facts.now_ms >= grant.expires_at_ms
        || creator.thread_id != acceptance.thread_id
    {
        return corrupt();
    }
    if original_running
        || matches!(cancelled.status, E::Completed | E::Failed)
        || journals.len() == 7
    {
        if journals.len()
            != if original_running {
                5
            } else if cancelled.revision == 6 {
                7
            } else {
                6
            }
            || before.revision != 2
            || acceptance.committed_revision != 3
            || before.status != E::WaitingForApproval
            || before.attempts_used != 0
            || !before.attempt_history.is_empty()
            || task.status != T::WaitingForApproval
            || task.approved
            || task.attempt_id.is_some()
            || task.result.is_some()
            || task.failure.is_some()
        {
            return corrupt();
        }
        // Reconstruct every consumed kernel stage from the immutable approval.
        // The terminal full snapshot comparison includes values, writes, histories,
        // counters, all task fields and the actual bounded result or failure.
        let resumed = b4_kernel::transition(
            before,
            b4_contract::KernelCommand::Resume {
                route: before.definition.route.clone(),
                decisions: vec![b4_contract::Decision {
                    interrupt_id: pending.id.clone(),
                    decision: ApprovalDecision::Always,
                }],
                turn_id: acceptance.turn_id.clone(),
            },
            acceptance.facts.clone(),
        )?
        .snapshot;
        let attempt = final_task
            .attempt_id
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let dispatched = b4_kernel::transition(
            &resumed,
            b4_contract::KernelCommand::Dispatch {
                task_id: task.spec.id.clone(),
                attempt_id: attempt.clone(),
            },
            TransitionFacts {
                now_ms: journals[4].updated_at_ms,
            },
        )?
        .snapshot;
        let cancellation = if journals.len() == 7 {
            let projected = b4_kernel::transition(
                &dispatched,
                b4_contract::KernelCommand::Cancel {
                    turn_id: dispatched.turn_id.clone(),
                    owner_generation: dispatched.owner_generation,
                },
                TransitionFacts {
                    now_ms: journals[5].updated_at_ms,
                },
            )?
            .snapshot;
            if journals[5] != crate::JournalEntry::from(&projected) {
                return corrupt();
            }
            Some(projected)
        } else {
            None
        };
        let terminal_before = cancellation.as_ref().unwrap_or(&dispatched);
        let terminal_index = journals.len() - 1;
        let terminal = if original_running {
            dispatched.clone()
        } else {
            let command = match (&final_task.result, &final_task.failure, cancelled.status) {
                (Some(result), None, E::Completed | E::Cancelled) => {
                    if crate::validate_effect_outcome(
                        &crate::EffectOutcome::Succeeded(result.clone()),
                        65_536,
                    )
                    .is_err()
                    {
                        return corrupt();
                    }
                    b4_contract::KernelCommand::Complete {
                        task_id: task.spec.id.clone(),
                        attempt_id: attempt.clone(),
                        result: result.clone(),
                    }
                }
                (None, Some(failure), E::Failed) => {
                    if crate::validate_effect_outcome(
                        &crate::EffectOutcome::Failed(failure.clone()),
                        65_536,
                    )
                    .is_err()
                    {
                        return corrupt();
                    }
                    b4_contract::KernelCommand::FailAttempt {
                        task_id: task.spec.id.clone(),
                        attempt_id: attempt.clone(),
                        failure: failure.clone(),
                    }
                }
                (None, None, E::Cancelled) if cancellation.is_some() => {
                    b4_contract::KernelCommand::CancelAttempt {
                        task_id: task.spec.id.clone(),
                        attempt_id: attempt.clone(),
                    }
                }
                _ => return corrupt(),
            };
            b4_kernel::transition(
                terminal_before,
                command,
                TransitionFacts {
                    now_ms: journals[terminal_index].updated_at_ms,
                },
            )?
            .snapshot
        };
        if &terminal != cancelled
            || journals[2] != crate::JournalEntry::from(before)
            || journals[3] != crate::JournalEntry::from(&resumed)
            || journals[4] != crate::JournalEntry::from(&dispatched)
            || journals[terminal_index] != crate::JournalEntry::from(&terminal)
        {
            return corrupt();
        }
        let mut previous = 0;
        for (index, journal) in journals.iter().enumerate() {
            if journal.execution_id != before.execution_id
                || journal.revision != index as u64
                || journal.owner_generation != before.owner_generation
                || journal.updated_at_ms < previous
                || index == 0 && journal.status != E::Accepted
                || index == 1 && journal.status != E::Running
            {
                return corrupt();
            }
            previous = journal.updated_at_ms;
        }
    } else {
        if journals.len() != 5 {
            return Err(PermissionError::UnsupportedProfile);
        }
        // This exact source has no attempts or outcomes between Park, Resume, Cancel.
        if before.revision != 2
            || acceptance.committed_revision != 3
            || cancelled.revision != 4
            || before.status != E::WaitingForApproval
            || before.attempts_used != 0
            || !before.attempt_history.is_empty()
            || before.output.is_some()
            || before.failure.is_some()
            || task.status != T::WaitingForApproval
            || task.approved
            || task.attempt_id.is_some()
            || task.result.is_some()
            || task.failure.is_some()
            || cancelled.schema_version != before.schema_version
            || cancelled.execution_id != before.execution_id
            || cancelled.turn_id != acceptance.turn_id
            || cancelled.owner_generation != before.owner_generation
            || cancelled.definition != before.definition
            || cancelled.status != E::Cancelled
            || cancelled.batch_index != before.batch_index
            || cancelled.values != before.values
            || cancelled.attempt_history != before.attempt_history
            || cancelled.attempts_used != before.attempts_used
            || cancelled.output != before.output
            || cancelled.failure != before.failure
            || cancelled.turn_history.len() != add(before.turn_history.len(), 1)?
            || cancelled.turn_history[..before.turn_history.len()] != before.turn_history[..]
            || cancelled.turn_history.last() != Some(&acceptance.turn_id)
            || final_task.spec != task.spec
            || final_task.input != task.input
            || final_task.status != T::Cancelled
            || !final_task.approved
            || final_task.attempt_id != task.attempt_id
            || final_task.result != task.result
            || final_task.failure != task.failure
            || final_task.interrupt != task.interrupt
            || cancelled.updated_at_ms < acceptance.facts.now_ms
        {
            return corrupt();
        }
        let statuses = [
            E::Accepted,
            E::Running,
            E::WaitingForApproval,
            E::Running,
            E::Cancelled,
        ];
        let mut previous_time = 0;
        for (index, journal) in journals.iter().enumerate() {
            if journal.execution_id != before.execution_id
                || journal.revision != index as u64
                || journal.owner_generation != before.owner_generation
                || journal.status != statuses[index]
                || journal.updated_at_ms < previous_time
            {
                return corrupt();
            }
            previous_time = journal.updated_at_ms;
        }
        if journals[2].updated_at_ms != before.updated_at_ms
            || journals[3].updated_at_ms != acceptance.facts.now_ms
            || journals[4].updated_at_ms != cancelled.updated_at_ms
        {
            return corrupt();
        }
    }
    // Original historical owner is independent of the new current store epoch.
    if !original_running && before.execution_id == current.execution.snapshot.execution_id {
        return corrupt();
    }
    let origin = current
        .thread
        .origin
        .as_ref()
        .ok_or(crate::approval::ApprovalError::Unauthorized)?;
    budget.reserve(
        add(
            mul(
                13,
                add(application.application.len(), application.revision.len())?,
            )?,
            512,
        )?,
        4,
    )?;
    let native_application = PermissionApplicationIdentity::from_native(application)?;
    let current_parts = ScopeParts {
        thread_id: &current.thread.id,
        thread_generation: &current.thread.generation,
        origin,
        application: &native_application,
        route: &current.execution.snapshot.definition.route,
        definition: &current.execution.snapshot.definition,
        effect_id: &registration.effect_id,
        tool_id: &registration.tool_id,
        tool_revision: &registration.tool_revision,
        policy_revision: &invocation.policy_revision,
        constraint_revision: &registration.constraint_revision,
    };
    let current_q = measure(
        CanonicalValue::Scope(current_parts),
        CanonicalLimit::Evidence64KiB,
        budget,
    )?;
    budget.reserve(
        add(source_q.bytes, current_q.bytes)?,
        add(source_q.nodes, current_q.nodes)?,
    )?;
    Ok(scope.thread_id == current.thread.id
        && scope.thread_generation == current.thread.generation
        && scope.origin == *origin
        && scope.application == native_application
        && scope.route == current.execution.snapshot.definition.route
        && scope.definition == current.execution.snapshot.definition
        && scope.effect_id == registration.effect_id
        && scope.tool_id == registration.tool_id
        && scope.tool_revision == registration.tool_revision
        && scope.policy_revision == invocation.policy_revision
        && scope.constraint_revision == registration.constraint_revision)
}
pub(super) fn ready_use_quote_plan(
    before: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let q = measure(
        CanonicalValue::Snapshot(before),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    // Two new full snapshots: actual Kernel next and retained Use.before.
    // Each copy and unique disposal is prepaid; requested validation sets stay positive.
    budget.reserve(
        add(mul(26, q.bytes)?, mul(256, q.nodes)?)?,
        mul(4, q.nodes)?,
    )?;
    budget.reserve(16_384, 128)?;
    Ok(())
}
// Keep the borrowed source, current plan, bindings and shared account distinct.
#[allow(clippy::too_many_arguments)]
pub(super) fn ready_use_factory(
    resource: &crate::approval::ApprovalResource,
    command: &b4_kernel::PermissionAuthorizationCommand,
    facts: &TransitionFacts,
    source: &PermissionDecision,
    registration: &super::ToolBindingRegistration,
    application: &ApplicationIdentity,
    plan: &b4_contract::CommitPlan,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionUse, PermissionError> {
    let before = &resource.execution.snapshot;
    if before.tasks.len() != 1 || before.tasks[0].spec != *command.task_spec() {
        return Err(PermissionError::UnsupportedProfile);
    }
    let task = &before.tasks[0];
    let fields = [
        command.use_id().as_str(),
        source.permission_id.as_str(),
        resource.thread.id.as_str(),
        resource.thread.generation.as_str(),
        before.execution_id.as_str(),
        task.spec.id.as_str(),
        application.application.as_str(),
        application.revision.as_str(),
        registration.tool_id.as_str(),
        registration.tool_revision.as_str(),
        source.scope.policy_revision.as_str(),
        registration.constraint_revision.as_str(),
        command.use_id().as_str(),
    ];
    let mut sum = 0;
    for field in fields {
        sum = add(sum, field.len())?;
    }
    budget.reserve(
        add(mul(13, sum)?, mul(256, fields.len())?)?,
        mul(2, fields.len())?,
    )?;
    budget.reserve(512, 1)?; // new owned call digest, before its constructor
    let digest = hash_canonical(
        CanonicalValue::TaskCall(task),
        CanonicalLimit::Call16KiB,
        budget,
    )?;
    let marker = PendingApproval {
        id: InterruptId::new(command.use_id().as_str()).map_err(
            ready_authorization_checked_metadata_value(|_| PermissionError::CorruptRecord),
        )?,
        prompt: serde_json::Value::Null,
    };
    if plan.snapshot.tasks.len() != 1
        || plan.snapshot.tasks[0].interrupt.as_ref() != Some(&marker)
        || !plan.snapshot.tasks[0].approved
        || plan.snapshot.tasks[0].status != b4_contract::TaskStatus::Ready
    {
        return corrupt();
    }
    Ok(PermissionUse {
        record_version: 1,
        permission_version: source.record_version,
        use_id: command.use_id().clone(),
        permission_id: source.permission_id.clone(),
        thread_id: resource.thread.id.clone(),
        thread_generation: resource.thread.generation.clone(),
        execution_id: before.execution_id.clone(),
        task_id: task.spec.id.clone(),
        before_snapshot: before.clone(),
        facts: facts.clone(),
        committed_revision: plan.snapshot.revision,
        marker,
        call_digest: digest,
        application: PermissionApplicationIdentity::from_native(application)?,
        tool_id: registration.tool_id.clone(),
        tool_revision: registration.tool_revision.clone(),
        policy_revision: source.scope.policy_revision.clone(),
        constraint_revision: registration.constraint_revision.clone(),
    })
}
pub(super) fn ready_use_encode_snapshot(
    snapshot: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<Vec<u8>, PermissionError> {
    let q = measure(
        CanonicalValue::Snapshot(snapshot),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(add(mul(13, q.bytes)?, 256)?, 2)?;
    serde_json::to_vec(snapshot).map_err(ready_authorization_checked_metadata_value(|_| {
        PermissionError::CorruptRecord
    }))
}
pub(super) fn ready_use_journal(
    snapshot: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::JournalEntry, PermissionError> {
    budget.reserve(add(mul(13, snapshot.execution_id.as_str().len())?, 256)?, 2)?;
    Ok(crate::JournalEntry::from(snapshot))
}
pub(super) fn ready_use_encode_journal(
    journal: &crate::JournalEntry,
    budget: &mut PermissionEvidenceBudget,
) -> Result<Vec<u8>, PermissionError> {
    let q = writer_journal_measure(journal)?;
    budget.reserve(add(mul(13, q.bytes)?, 256)?, 2)?;
    serde_json::to_vec(journal).map_err(ready_authorization_checked_metadata_value(|_| {
        PermissionError::CorruptRecord
    }))
}

impl PermissionResourceRead<'_> {
    pub(super) fn ready_use_quote_raw_compare(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        self.budget.reserve(mul(2, bytes)?, nodes)
    }
}
// Keep the three independently paid owned witness buffers explicit.
#[allow(clippy::type_complexity)]
pub(super) fn ready_use_current_witnesses(
    resource: &crate::approval::ApprovalResource,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(Vec<u8>, Vec<u8>, Option<Vec<u8>>), PermissionError> {
    let snapshot = ready_use_encode_snapshot(&resource.execution.snapshot, budget)?;
    let q = measure(
        CanonicalValue::Input(&resource.thread.metadata),
        CanonicalLimit::ResourceThread,
        budget,
    )?;
    budget.reserve(add(mul(13, q.bytes)?, 256)?, 2)?;
    let metadata = serde_json::to_vec(&resource.thread.metadata).map_err(
        ready_authorization_checked_metadata_value(|_: serde_json::Error| {
            PermissionError::CorruptRecord
        }),
    )?;
    let origin = match &resource.thread.origin {
        None => None,
        Some(origin) => {
            let q = measure(
                CanonicalValue::Origin(origin),
                CanonicalLimit::ResourceThread,
                budget,
            )?;
            budget.reserve(add(mul(13, q.bytes)?, 256)?, 2)?;
            Some(
                serde_json::to_vec(origin).map_err(ready_authorization_checked_metadata_value(
                    |_: serde_json::Error| PermissionError::CorruptRecord,
                ))?,
            )
        }
    };
    Ok((snapshot, metadata, origin))
}

// Preserve exact codec callbacks while qualifying their actual value types.
fn ready_use_checked_codec_iife<F, R>(mut value: F) -> R
where
    F: FnMut() -> R,
{
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value()
}
fn ready_use_checked_permission_suffix<F>(value: F) -> F
where
    F: for<'a> FnOnce(&'a PermissionId) -> Option<&'a str>,
{
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}

// Complete reached writer codec carriers, independent of payload-only ceilings.
const _: () = assert!(std::mem::size_of::<PermissionAcceptance>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<PermissionAcceptance, PermissionError>>() <= 4096);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionAcceptance, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionAcceptance,
        >,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<PermissionDecision>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<PermissionDecision, PermissionError>>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<PermissionDecision, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionDecision,
        >,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<PermissionUse>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<PermissionUse, PermissionError>>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<PermissionUse, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, PermissionUse>,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<AcceptanceWire>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<AcceptanceWire, PermissionError>>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<AcceptanceWire, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, AcceptanceWire>,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<DecisionWire>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<DecisionWire, PermissionError>>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<DecisionWire, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, DecisionWire>,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<UseWire>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<UseWire, PermissionError>>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<UseWire, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, UseWire>,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<crate::approval::GrantRecord>() <= 4096);
const _: () =
    assert!(std::mem::size_of::<Result<crate::approval::GrantRecord, PermissionError>>() <= 4096);
const _: () =
    assert!(std::mem::size_of::<Result<crate::approval::GrantRecord, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            crate::approval::GrantRecord,
        >,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<crate::StoredExecution>() <= 4096);
const _: () =
    assert!(std::mem::size_of::<Result<crate::StoredExecution, PermissionError>>() <= 4096);
const _: () =
    assert!(std::mem::size_of::<Result<crate::StoredExecution, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            crate::StoredExecution,
        >,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<b4_contract::CommitPlan>() <= 4096);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::CommitPlan, PermissionError>>() <= 4096);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::CommitPlan, serde_json::Error>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            b4_contract::CommitPlan,
        >,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<crate::JournalEntry>() <= 256);
const _: () = assert!(std::mem::size_of::<Result<crate::JournalEntry, PermissionError>>() <= 256);
const _: () = assert!(std::mem::size_of::<Result<crate::JournalEntry, serde_json::Error>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            crate::JournalEntry,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)>() <= 128);
const _: () = assert!(
    std::mem::size_of::<Result<(Vec<u8>, Vec<u8>, Option<Vec<u8>>), PermissionError>>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<Result<(Vec<u8>, Vec<u8>, Option<Vec<u8>>), serde_json::Error>>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (Vec<u8>, Vec<u8>, Option<Vec<u8>>),
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<PermissionApplicationIdentity>() <= 128);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionApplicationIdentity, PermissionError>>() <= 128);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionApplicationIdentity, serde_json::Error>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionApplicationIdentity,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<b4_contract::PendingApproval>() <= 256);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::PendingApproval, PermissionError>>() <= 256);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::PendingApproval, serde_json::Error>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            b4_contract::PendingApproval,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<b4_contract::TransitionFacts>() <= 128);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::TransitionFacts, PermissionError>>() <= 128);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::TransitionFacts, serde_json::Error>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            b4_contract::TransitionFacts,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<Result<String, std::string::FromUtf8Error>>() <= 128);
const _: () = assert!(std::mem::size_of::<std::string::FromUtf8Error>() <= 128);
const _: () =
    assert!(std::mem::size_of::<Result<b4_contract::CommitPlan, b4_kernel::KernelError>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, b4_kernel::KernelError>,
            b4_contract::CommitPlan,
        >,
    >() <= 4096
);

impl PermissionEvidenceBudget {
    pub(super) fn begin_runtime_use_cancellation(
        &mut self,
        id: &ExecutionId,
        context: &crate::approval::ApprovalContext,
    ) -> Result<std::sync::Arc<super::runtime_mutation::PermissionRuntimeOrigin>, PermissionError>
    {
        let origin = self.begin_runtime_dispatch(
            id,
            context,
            super::runtime_mutation::PermissionRuntimeInputProfile::NormalNewUse,
        )?;
        let state = self
            .runtime_state
            .as_mut()
            .ok_or(PermissionError::UnsupportedProfile)?;
        state.kind = super::runtime_mutation::PermissionRuntimeOperationKind::UseCancellation;
        state.phase = super::runtime_mutation::PermissionRuntimePhase::AdmissionInProgress;
        Ok(origin)
    }
    pub(super) fn runtime_begin_use_source(&mut self) -> Result<(), PermissionError> {
        // A new finite source-constructor root, before its first owner. Failed
        // first purchase is scoped to a separately trusted finite inception.
        self.reserve(128 + 2 * 8192, 1)?;
        let state = self
            .runtime_state
            .as_ref()
            .ok_or(PermissionError::UnsupportedProfile)?;
        if !matches!(
            state.kind,
            super::runtime_mutation::PermissionRuntimeOperationKind::Dispatch
                | super::runtime_mutation::PermissionRuntimeOperationKind::UseCancellation
        ) || !matches!(
            state.phase,
            super::runtime_mutation::PermissionRuntimePhase::AdmissionInProgress
        ) {
            return Err(PermissionError::UnsupportedProfile);
        }
        if !self.runtime_use_entered {
            // Fresh helper interfaces, owner ends, original loans and first-error
            // graph. This purchase precedes the first added source Statement.
            self.reserve(405_945, 19_485)?;
        }
        self.runtime_use_entered = true;
        Ok(())
    }
}
const _: () = assert!(std::mem::size_of::<PermissionUse>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<PermissionUse, PermissionError>>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<PermissionUse, serde_json::Error>>() <= 4096);
const _: () = assert!(std::mem::size_of::<UseWire>() <= 4096);
const _: () = assert!(std::mem::size_of::<PermissionEvidenceBudget>() <= 4096);

pub(super) fn runtime_use_quote_projection(
    before: &Snapshot,
    current: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let a = measure(
        CanonicalValue::Snapshot(before),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    let b = measure(
        CanonicalValue::Snapshot(current),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(add(a.bytes, b.bytes)?, add(a.nodes, b.nodes)?)
}

const _: () = assert!(std::mem::size_of::<Result<UseWire, serde_json::Error>>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<UseWire, PermissionError>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, UseWire>,
    >() <= 4096
);

// Exact account-purpose API carriers; no Budget field-sum inference.
const _: () = assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ()>>(
    ) <= 128
);
const _: () = assert!(std::mem::size_of::<Result<bool, PermissionError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, bool>,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<usize>() <= 8);

// Direct whole borrowed-state carriers used by the typed purpose resource guard.
const _: () = assert!(std::mem::size_of::<&super::runtime_mutation::PermissionRuntimeState>() <= 8);
const _: () =
    assert!(std::mem::size_of::<Option<&super::runtime_mutation::PermissionRuntimeState>>() <= 16);
const _: () = assert!(
    std::mem::size_of::<Result<&super::runtime_mutation::PermissionRuntimeState, PermissionError>>(
    ) <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &super::runtime_mutation::PermissionRuntimeState,
        >,
    >() <= 128
);

// A private fresh constructor, never a conversion/lift of a spent old account.
// Ceiling is a scaffold-only source proposal, pending independent invoice review.
const USE_RENEWAL_SCAFFOLD_BYTES: usize = 8 * 1024 * 1024
    + super::runtime_use_renewal::CONSTRUCTOR_FIXED
    + super::runtime_use_renewal::OPERATION_FIXED
    + 8 * super::runtime_use_renewal::LEAF_FIXED
    + super::runtime_use_renewal::SOURCE_FIXED;
impl PermissionEvidenceBudget {
    #[allow(dead_code)] // Retained scaffold purpose and budget controls.
    pub(super) fn fresh_use_renewal_scaffold() -> Self {
        Self {
            bytes_limit: USE_RENEWAL_SCAFFOLD_BYTES,
            ..Self::new()
        }
    }
    pub(super) fn use_renewal_resource_read(
        &mut self,
    ) -> Result<PermissionResourceRead<'_>, PermissionError> {
        self.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        if !((self.bytes_limit == USE_RENEWAL_SCAFFOLD_BYTES && self.use_renewal_scaffold_entered)
            || (self.bytes_limit == USE_RENEWAL_PRODUCTION_BYTES
                && self.use_renewal_production_entered))
            || self.resource.reads >= 2
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.resource.reads += 1;
        Ok(PermissionResourceRead { budget: self })
    }
}
pub(super) fn use_renewal_quote_resource_pair(
    old: &crate::approval::ApprovalResource,
    fresh: &crate::approval::ApprovalResource,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
    for resource in [old, fresh] {
        let snapshot = measure(
            CanonicalValue::Snapshot(&resource.execution.snapshot),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        let thread = measure(
            CanonicalValue::Thread(&resource.thread),
            CanonicalLimit::ResourceThread,
            budget,
        )?;
        // Complete comparison on this fresh operation. Original owner endpoints
        // remain exterior producer obligations; fresh Resource endpoints need
        // the separately reviewed preconnection incarnation invoice.
        let bytes = add(snapshot.bytes, thread.bytes)?;
        let nodes = add(snapshot.nodes, thread.nodes)?;
        budget.reserve(add(mul(128, nodes)?, mul(6, bytes)?)?, nodes)?;
        budget.reserve(
            add(mul(2, resource.execution.thread_id.as_str().len())?, 128)?,
            2,
        )?;
    }
    Ok(())
}
pub(super) fn use_renewal_quote_context(
    context: &crate::approval::ApprovalContext,
    resource: &crate::approval::ApprovalResource,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
    let bytes = add(
        add(context.actor.application.len(), context.actor.tenant.len())?,
        add(context.actor.principal.len(), context.policy_revision.len())?,
    )?;
    budget.reserve(add(mul(2, bytes)?, 128)?, 8)?;
    crate::approval::validate_approval_context(context)?;
    if resource.thread.origin.as_ref() != Some(&context.actor) {
        return Err(crate::approval::ApprovalError::Unauthorized.into());
    }
    Ok(())
}
pub(super) fn use_renewal_quote_fresh_context(
    resource: &crate::approval::ApprovalResource,
    context: &crate::approval::ApprovalContext,
    application: &crate::ApplicationIdentity,
    definition: &Definition,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
    // Pay full current definition comparison, without calling any constraint.
    let old = measure(
        CanonicalValue::Definition(&resource.execution.snapshot.definition),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    let current = measure(
        CanonicalValue::Definition(definition),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(
        add(old.bytes, current.bytes)?,
        add(old.nodes, current.nodes)?,
    )?;
    let origin = resource
        .thread
        .origin
        .as_ref()
        .ok_or(crate::approval::ApprovalError::Unauthorized)?;
    let bytes = add(
        add(
            resource.thread.id.as_str().len(),
            resource.thread.generation.as_str().len(),
        )?,
        add(
            add(origin.application.len(), origin.tenant.len())?,
            add(
                add(origin.principal.len(), application.revision.len())?,
                add(
                    definition.revision.as_str().len(),
                    context.policy_revision.len(),
                )?,
            )?,
        )?,
    )?;
    // Actual initializer: thread ID, generation, three Origin Strings,
    // application revision, Definition revision, policy revision = eight owners.
    // Their independent copy/allocation (7L+128 each) and partial/normal end
    // (6L+128 each) tickets precede the first clone. Whole Context transfers
    // are separate positive512 envelopes; no header slack buys a data copy.
    budget.reserve(
        add(
            add(mul(7, bytes)?, 8 * 128)?,
            add(add(mul(6, bytes)?, 8 * 128)?, 3 * 512)?,
        )?,
        16,
    )?;
    Ok(())
}
const _: () = {
    assert!(std::mem::size_of::<[&crate::approval::ApprovalResource; 2]>() <= 128);
    assert!(std::mem::size_of::<[CanonicalMeasure; 2]>() <= 128);
    assert!(std::mem::size_of::<Result<CanonicalMeasure, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                CanonicalMeasure,
            >,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &crate::approval::ApprovalContext,
            &crate::approval::ApprovalResource,
            &mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &crate::approval::ApprovalResource,
            &crate::approval::ApprovalContext,
            &crate::ApplicationIdentity,
            &Definition,
            &mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &crate::approval::ApprovalResource,
            &crate::approval::ApprovalResource,
            &mut PermissionEvidenceBudget
        )>() <= 128
    );
};

impl PermissionResourceRead<'_> {
    pub fn decode_use_renewal_execution(
        &mut self,
        cells: PermissionExecutionCells<'_>,
    ) -> Result<crate::StoredExecution, PermissionError> {
        self.budget
            .reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        if !(self.budget.use_renewal_scaffold_entered || self.budget.use_renewal_production_entered)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.decode_execution(cells)
    }
    pub fn decode_use_renewal_thread(
        &mut self,
        cells: PermissionThreadCells<'_>,
    ) -> Result<crate::ThreadRecord, PermissionError> {
        self.budget
            .reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        if !(self.budget.use_renewal_scaffold_entered || self.budget.use_renewal_production_entered)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.decode_thread(cells)
    }
}
impl PermissionEvidenceBudget {
    #[allow(dead_code)] // Retained scaffold purpose and budget controls.
    pub(super) fn enter_use_renewal_scaffold(&mut self) -> Result<(), PermissionError> {
        self.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        if self.bytes_limit != USE_RENEWAL_SCAFFOLD_BYTES || self.use_renewal_scaffold_entered {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.use_renewal_scaffold_entered = true;
        Ok(())
    }
}

const _: () = {
    assert!(std::mem::size_of::<PermissionEvidenceUsage>() <= 16);
    assert!(
        std::mem::size_of::<(
            &mut PermissionResourceRead<'static>,
            PermissionExecutionCells<'static>
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &mut PermissionResourceRead<'static>,
            PermissionThreadCells<'static>
        )>() <= 128
    );
};

// New renewal-only borrowed projection/credential application. No old body changes.
pub(super) fn use_renewal_quote_claimed_projection(
    before: &Snapshot,
    current: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.runtime_reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
    budget.reserve(super::runtime_use_renewal::SOURCE_CALL_FIXED, 64)?;
    if !budget.use_renewal_production_entered {
        return Err(PermissionError::UnsupportedProfile);
    }
    for value in [before, current] {
        let q = measure(
            CanonicalValue::Snapshot(value),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        budget.reserve(mul(4, q.bytes)?, mul(4, q.nodes)?)?;
    }
    budget.reserve(16_384, 128)
}
pub(super) fn use_renewal_verify_original_grant(
    grant: &crate::approval::GrantRecord,
    facts: &TransitionFacts,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.runtime_reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
    budget.reserve(super::runtime_use_renewal::SOURCE_CALL_FIXED, 64)?;
    if !budget.use_renewal_production_entered {
        return Err(PermissionError::UnsupportedProfile);
    }
    // Four bounded label scans, two 64-byte digest-shape scans, the
    // 32-byte verifier fold and a maximum 69-byte sealed token feed each
    // retain independent Args/return/F/end cells. This is a fresh quote.
    budget.reserve(
        4 * 128 * (4 * 256 + 2 * 64 + 32 + 69),
        4 * 256 + 2 * 64 + 32 + 69,
    )?;
    budget.reserve(32_768, 128)?;
    if !valid_label(&grant.origin.application)
        || !valid_label(&grant.origin.tenant)
        || !valid_label(&grant.origin.principal)
        || !valid_label(&grant.policy_revision)
        || grant.task_parking_revision == 0
        || !digest_shape(&grant.proposal_digest)
        || !digest_shape(&grant.resume_key)
        || !grant.consumed
        || grant.issued_at_ms > facts.now_ms
        || facts.now_ms >= grant.expires_at_ms
    {
        return corrupt();
    }
    let ttl = grant
        .expires_at_ms
        .checked_sub(grant.issued_at_ms)
        .ok_or(PermissionError::CorruptRecord)?;
    if ttl == 0 || ttl > crate::approval::MAX_APPROVAL_TTL_MS {
        return corrupt();
    }
    // Conditional source-bound ARM64/default-std/no-asm SHA proposal.
    // Independent review adds8 overwritten padding slots4096/8 and
    // eighteen GenericArray default-call cohorts110592/18 to4886528/3116.
    // Every earlier32KiB and scan/feed ticket remains positive and distinct.
    if !cfg!(target_arch = "aarch64") {
        return Err(PermissionError::UnsupportedProfile);
    }
    budget.reserve(5_001_216, 3_142)?;
    let verifier = ready_authorization_checked_metadata_value(|| {
        <sha2::Sha256 as sha2::Digest>::digest(grant.token.as_str().as_bytes())
    })();
    let difference = grant.verifier.iter().zip(verifier.iter()).fold(
        0u8,
        ready_authorization_checked_metadata_value(|d: u8, (a, b): (&u8, &u8)| d | (a ^ b)),
    );
    if difference != 0 {
        return corrupt();
    }
    Ok(())
}

impl PermissionEvidenceBudget {
    // C2 reborrows one separately admitted combined read; R3/R4 have their own
    // selector counters. This never enters ordinary resource_read eligibility.
    pub(super) fn use_renewal_combined_resource_read(
        &mut self,
    ) -> Result<PermissionResourceRead<'_>, PermissionError> {
        self.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        if !self.use_renewal_production_entered {
            return Err(PermissionError::UnsupportedProfile);
        }
        Ok(PermissionResourceRead { budget: self })
    }
}

pub(super) fn begin_use_renewal_claimed_gate(
    entries: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
    budget.reserve(super::runtime_use_renewal::SOURCE_CALL_FIXED, 64)?;
    if !budget.use_renewal_production_entered {
        return Err(PermissionError::UnsupportedProfile);
    }
    // Independent actual method/Result/Try, route lookups, constraint host
    // interface/catch/payload replacement endpoints, and full catalog scans.
    budget.reserve(65_536, 256)?;
    budget.reserve(16_384, 64)?;
    budget.reserve(mul(128, entries)?, mul(2, entries)?)
}
pub(super) fn quote_use_renewal_claimed_gate(
    task: &TaskRecord,
    invocation: &crate::EffectInvocation,
    identity: &ApplicationIdentity,
    registration: &super::ToolBindingRegistration,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
    budget.reserve(super::runtime_use_renewal::SOURCE_CALL_FIXED, 64)?;
    if !budget.use_renewal_production_entered {
        return Err(PermissionError::UnsupportedProfile);
    }
    let left = measure(
        CanonicalValue::Input(&task.input),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    let right = measure(
        CanonicalValue::Input(&invocation.input),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(add(left.bytes, right.bytes)?, add(left.nodes, right.nodes)?)?;
    budget.reserve(
        add(
            add(
                task.spec.id.as_str().len(),
                invocation.task_id.as_str().len(),
            )?,
            add(
                add(
                    task.spec.effect_id.as_str().len(),
                    invocation.effect_id.as_str().len(),
                )?,
                add(
                    task.attempt_id.as_ref().map_or(
                        0,
                        ready_authorization_checked_metadata_value(|v: &b4_contract::AttemptId| {
                            v.as_str().len()
                        }),
                    ),
                    invocation.attempt_id.as_str().len(),
                )?,
            )?,
        )?,
        16,
    )?;
    // The old full entries quote includes candidate owned binding endpoint
    // bounds. This separate ticket pays the actual selected binding copy.
    let q = measure(
        CanonicalValue::Definition(&registration.definition),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(add(mul(128, q.nodes)?, mul(7, q.bytes)?)?, q.nodes)?;
    let labels = add(
        add(identity.application.len(), identity.revision.len())?,
        add(
            registration.effect_id.as_str().len(),
            add(
                registration.tool_id.as_str().len(),
                add(
                    registration.tool_revision.as_str().len(),
                    registration.constraint_revision.as_str().len(),
                )?,
            )?,
        )?,
    )?;
    budget.reserve(add(mul(7, labels)?, mul(128, 6)?)?, 6)?;
    budget.reserve(8 * 512 + 16 * 128, 64)
}
const _: () = {
    assert!(
        std::mem::size_of::<(
            &TaskRecord,
            &crate::EffectInvocation,
            &ApplicationIdentity,
            &super::ToolBindingRegistration,
            &mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(std::mem::size_of::<(&Snapshot, &Snapshot, &mut PermissionEvidenceBudget)>() <= 128);
    assert!(
        std::mem::size_of::<(
            &crate::approval::GrantRecord,
            &TransitionFacts,
            &mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 128);
};

// Fresh immutable production-purpose ceiling proposal. Root must accept the
// complete source-bound invoice and separate account-domain control before use.
const USE_RENEWAL_PRODUCTION_BYTES: usize = 64 * 1024 * 1024;
impl PermissionEvidenceBudget {
    pub(super) fn fresh_use_renewal_production() -> Self {
        Self {
            bytes_limit: USE_RENEWAL_PRODUCTION_BYTES,
            ..Self::new()
        }
    }
    pub(super) fn enter_use_renewal_production(&mut self) -> Result<(), PermissionError> {
        self.reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        if self.bytes_limit != USE_RENEWAL_PRODUCTION_BYTES
            || self.use_renewal_production_entered
            || self.use_renewal_scaffold_entered
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.use_renewal_production_entered = true;
        Ok(())
    }
}

// Public dependency types of the selected local SHA256 graph. Internal library
// closure bodies remain scoped to the independently reviewed source proposal.
const _: () = {
    use sha2::digest::{
        block_buffer::{BlockBuffer, Eager},
        core_api::CoreProxy,
        generic_array::{ArrayBuilder, GenericArray},
        typenum::{U32, U64},
    };
    type Core = <sha2::Sha256 as CoreProxy>::Core;
    assert!(std::mem::size_of::<sha2::Sha256>() <= 128);
    assert!(std::mem::size_of::<Core>() <= 128);
    assert!(std::mem::size_of::<sha2::Sha256VarCore>() <= 128);
    assert!(
        std::mem::size_of::<Result<sha2::Sha256VarCore, sha2::digest::InvalidOutputSize>>() <= 128
    );
    assert!(std::mem::size_of::<sha2::digest::Output<sha2::Sha256>>() <= 32);
    assert!(std::mem::size_of::<GenericArray<u8, U32>>() <= 32);
    assert!(std::mem::size_of::<GenericArray<u8, U64>>() <= 64);
    assert!(std::mem::size_of::<BlockBuffer<U64, Eager>>() <= 128);
    assert!(std::mem::size_of::<ArrayBuilder<u8, U32>>() <= 128);
    assert!(std::mem::size_of::<ArrayBuilder<u8, U64>>() <= 128);
    assert!(std::mem::size_of::<std::mem::MaybeUninit<GenericArray<u8, U32>>>() <= 128);
    assert!(std::mem::size_of::<std::mem::MaybeUninit<GenericArray<u8, U64>>>() <= 128);
    assert!(std::mem::size_of::<(&[u8],)>() <= 128);
    assert!(std::mem::size_of::<(&mut sha2::Sha256, &[u8])>() <= 128);
    assert!(std::mem::size_of::<(sha2::Sha256,)>() <= 128);
    assert!(
        std::mem::size_of::<(
            &mut Core,
            &mut BlockBuffer<U64, Eager>,
            &mut GenericArray<u8, U32>
        )>() <= 128
    );
    assert!(std::mem::size_of::<(&mut [u32; 8], &[[u8; 64]])>() <= 128);
    assert!(std::mem::size_of::<([u32; 4], [u32; 4], [u32; 4], [u32; 4])>() <= 128);
    assert!(std::mem::size_of::<(u32, u32)>() <= 32);
    assert!(std::mem::size_of::<[u8; 32]>() <= 32);
    assert!(std::mem::size_of::<[u8; 64]>() <= 64);
    assert!(std::mem::size_of::<[u32; 8]>() <= 32);
    assert!(std::mem::size_of::<[u32; 16]>() <= 64);
    assert!(std::mem::size_of::<[u32; 4]>() <= 16);
    assert!(
        std::mem::size_of::<(
            [u32; 4],
            [u32; 4],
            [u32; 4],
            [u32; 4],
            [u32; 4],
            [u32; 16],
            [u32; 8]
        )>() <= 256
    );
    assert!(std::mem::size_of::<Result<[u8; 4], std::array::TryFromSliceError>>() <= 128);
    assert!(std::mem::size_of::<std::slice::Iter<'static, u8>>() <= 128);
    assert!(std::mem::size_of::<std::slice::IterMut<'static, u8>>() <= 128);
    assert!(std::mem::size_of::<std::iter::Enumerate<std::slice::IterMut<'static, u8>>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::iter::Zip<std::slice::Iter<'static, u8>, std::slice::Iter<'static, u8>>,
        >() <= 128
    );
    assert!(std::mem::size_of::<std::slice::ChunksExact<'static, u8>>() <= 128);
    assert!(std::mem::size_of::<std::slice::ChunksExactMut<'static, u8>>() <= 128);
    assert!(std::mem::size_of::<(u8, (&u8, &u8))>() <= 128);
    assert!(std::mem::size_of::<ResourceLedger>() <= 512);
    assert!(std::mem::size_of::<Vec<Vec<u8>>>() <= 128);
    assert!(std::mem::size_of::<(Vec<Vec<u8>>, [Vec<u8>; 4])>() <= 128);
};

// Additional named layout checks for selected fresh renewal walker/codec.
// Assertions establish layout only; retained fresh purchases and actual callbacks
// require separate source application. No runtime action is introduced.
const _: () = {
    assert!(std::mem::size_of::<Cursor<'static>>() <= 128);
    assert!(std::mem::size_of::<Sequence<'static>>() <= 128);
    assert!(
        std::mem::size_of::<Option<(Option<&'static str>, CanonicalValue<'static>, bool)>>() <= 256
    );
    assert!(std::mem::size_of::<Option<(&'static str, CanonicalValue<'static>)>>() <= 256);
    assert!(std::mem::size_of::<ScalarWriter<'static>>() <= 128);
    assert!(std::mem::size_of::<std::io::Sink>() <= 128);
    assert!(std::mem::size_of::<Vec<CanonicalFrame<'static>>>() <= 128);
    assert!(std::mem::size_of::<Vec<RawFrame>>() <= 128);
    assert!(std::mem::size_of::<Vec<RawContainer>>() <= 128);
    assert!(std::mem::size_of::<Vec<KeySpan>>() <= 128);
    assert!(std::mem::size_of::<Vec<Vec<String>>>() <= 128);
    assert!(std::mem::size_of::<Vec<String>>() <= 128);
    assert!(std::mem::size_of::<Result<KeySpan, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, KeySpan>,
        >() <= 128
    );
    assert!(std::mem::size_of::<std::io::Error>() <= 128);
    assert!(std::mem::size_of::<std::io::Result<usize>>() <= 128);
    assert!(std::mem::size_of::<std::io::Result<()>>() <= 128);
    assert!(std::mem::size_of::<serde_json::Error>() <= 128);
    assert!(std::mem::size_of::<serde_json::Serializer<ScalarWriter<'static>>>() <= 128);
    assert!(
        std::mem::size_of::<serde_json::Deserializer<serde_json::de::SliceRead<'static>>>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            CanonicalValue<'static>,
            CanonicalLimit,
            &'static mut PermissionEvidenceBudget
        )>() <= 256
    );
    assert!(
        std::mem::size_of::<(
            CanonicalValue<'static>,
            CanonicalLimit,
            &'static mut dyn std::io::Write,
            &'static mut PermissionEvidenceBudget,
            bool
        )>() <= 256
    );
    assert!(
        std::mem::size_of::<(
            CanonicalValue<'static>,
            CanonicalLimit,
            &'static mut dyn std::io::Write,
            &'static mut PermissionEvidenceBudget,
            bool,
            &'static mut Vec<CanonicalFrame<'static>>
        )>() <= 256
    );
    assert!(
        std::mem::size_of::<(
            &'static mut dyn std::io::Write,
            &'static mut WalkState,
            &'static mut PermissionEvidenceBudget,
            &'static [u8]
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &'static str,
            Option<usize>,
            &'static mut dyn std::io::Write,
            &'static mut WalkState,
            &'static mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(std::mem::size_of::<(&'static mut ScalarWriter<'static>, &'static [u8])>() <= 128);
    assert!(
        std::mem::size_of::<(
            &'static [u8],
            RawProfile,
            &'static mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &'static [u8],
            KeySpan,
            &'static mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(std::mem::size_of::<(&'static [u8], &'static mut usize)>() <= 128);
    assert!(
        std::mem::size_of::<(
            &'static mut Vec<RawContainer>,
            &'static mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &'static mut Vec<KeySpan>,
            &'static mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, std::io::Error>, usize>,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, std::io::Error>, ()>,
        >() <= 128
    );
    assert!(std::mem::size_of::<(ScalarWriter<'static>, &'static str)>() <= 128);
    assert!(std::mem::size_of::<(ScalarWriter<'static>, &'static ())>() <= 128);
    assert!(std::mem::size_of::<(ScalarWriter<'static>, &'static bool)>() <= 128);
    assert!(std::mem::size_of::<(ScalarWriter<'static>, &'static u64)>() <= 128);
    assert!(std::mem::size_of::<(ScalarWriter<'static>, &'static usize)>() <= 128);
    assert!(std::mem::size_of::<(ScalarWriter<'static>, &'static serde_json::Number)>() <= 128);
    assert!(std::mem::size_of::<Result<u16, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, u16>,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<String, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, String>,
        >() <= 128
    );
};

// These numerical/purpose controls create no Claim, Account selector or Permit.
#[cfg(test)]
mod use_renewal_production_budget_controls {
    use super::{PermissionError, PermissionEvidenceBudget};

    #[test]
    fn fresh_factories_keep_literal_byte_domains_and_zero_usage() {
        let ordinary = PermissionEvidenceBudget::new();
        let first_claim = PermissionEvidenceBudget::for_use_dispatch();
        let renewal = PermissionEvidenceBudget::fresh_use_renewal_production();
        assert_eq!(ordinary.bytes_limit, 8 * 1024 * 1024);
        assert_eq!(first_claim.bytes_limit, 16 * 1024 * 1024);
        assert_eq!(renewal.bytes_limit, 64 * 1024 * 1024);
        assert!(!ordinary.is_use_dispatch_account());
        assert!(first_claim.is_use_dispatch_account());
        assert!(!renewal.is_use_dispatch_account());
        for budget in [ordinary, first_claim, renewal] {
            assert_eq!(budget.usage().encoded_bytes(), 0);
            assert_eq!(budget.usage().structural_nodes(), 0);
            assert!(!budget.exhausted);
            assert!(!budget.use_renewal_production_entered);
            assert!(!budget.use_renewal_scaffold_entered);
            assert!(budget.runtime_state.is_none());
            assert!(budget.acceptance_identity.is_none());
            assert!(budget.resource.keys.is_empty());
        }
    }

    #[test]
    fn ordinary_and_first_claim_byte_boundaries_stay_eight_and_sixteen_mib() {
        for (mut budget, limit) in [
            (PermissionEvidenceBudget::new(), 8 * 1024 * 1024),
            (
                PermissionEvidenceBudget::for_use_dispatch(),
                16 * 1024 * 1024,
            ),
        ] {
            assert!(budget.reserve(limit, 0).is_ok());
            let paid = budget.usage();
            assert!(matches!(
                budget.reserve(1, 0),
                Err(PermissionError::LimitExceeded)
            ));
            assert_eq!(budget.usage(), paid);
            assert!(matches!(
                budget.reserve(0, 0),
                Err(PermissionError::LimitExceeded)
            ));
            assert_eq!(budget.bytes_limit, limit);
        }
    }

    #[test]
    fn production_exact_sixty_four_mib_boundary_is_sticky_and_never_refunds() {
        let mut budget = PermissionEvidenceBudget::fresh_use_renewal_production();
        assert!(budget.reserve(64 * 1024 * 1024 - 1, 0).is_ok());
        assert!(budget.reserve(1, 0).is_ok());
        let paid = budget.usage();
        assert_eq!(paid.encoded_bytes(), 64 * 1024 * 1024);
        assert!(matches!(
            budget.reserve(1, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert!(budget.exhausted);
        assert_eq!(budget.usage(), paid);
        for request in [(0, 0), (0, 1), (1, 0)] {
            assert!(matches!(
                budget.reserve(request.0, request.1),
                Err(PermissionError::LimitExceeded)
            ));
            assert_eq!(budget.usage(), paid);
        }
        assert_eq!(budget.bytes_limit, 64 * 1024 * 1024);
    }

    #[test]
    fn production_exact_node_boundary_is_sixty_five_thousand_five_hundred_thirty_six() {
        let mut budget = PermissionEvidenceBudget::fresh_use_renewal_production();
        assert!(budget.reserve(0, 65_535).is_ok());
        assert!(budget.reserve(0, 1).is_ok());
        let paid = budget.usage();
        assert_eq!(paid.structural_nodes(), 65_536);
        assert_eq!(paid.encoded_bytes(), 0);
        assert!(matches!(
            budget.reserve(0, 1),
            Err(PermissionError::LimitExceeded)
        ));
        assert!(budget.exhausted);
        assert_eq!(budget.usage(), paid);
        assert!(matches!(
            budget.reserve(0, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage(), paid);
    }

    #[test]
    fn renewal_entry_rejects_each_other_sealed_factory_purpose() {
        for mut budget in [
            PermissionEvidenceBudget::new(),
            PermissionEvidenceBudget::for_use_dispatch(),
            PermissionEvidenceBudget::fresh_use_renewal_scaffold(),
        ] {
            let ceiling = budget.bytes_limit;
            assert!(matches!(
                budget.enter_use_renewal_production(),
                Err(PermissionError::UnsupportedProfile)
            ));
            assert!(!budget.use_renewal_production_entered);
            assert_eq!(budget.bytes_limit, ceiling);
            assert_eq!(budget.usage().encoded_bytes(), 1_024);
            assert_eq!(budget.usage().structural_nodes(), 8);
            assert!(!budget.exhausted);
        }
        let mut scaffold = PermissionEvidenceBudget::fresh_use_renewal_scaffold();
        assert!(scaffold.enter_use_renewal_scaffold().is_ok());
        assert!(matches!(
            scaffold.enter_use_renewal_production(),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(scaffold.use_renewal_scaffold_entered);
        assert!(!scaffold.use_renewal_production_entered);
    }

    #[test]
    fn renewal_entry_is_once_only_and_failed_reentry_keeps_its_paid_prefix() {
        let mut budget = PermissionEvidenceBudget::fresh_use_renewal_production();
        assert!(budget.enter_use_renewal_production().is_ok());
        assert!(budget.use_renewal_production_entered);
        assert_eq!(budget.usage().encoded_bytes(), 1_024);
        assert_eq!(budget.usage().structural_nodes(), 8);
        assert!(matches!(
            budget.enter_use_renewal_production(),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(budget.use_renewal_production_entered);
        assert_eq!(budget.usage().encoded_bytes(), 2_048);
        assert_eq!(budget.usage().structural_nodes(), 16);
        assert_eq!(budget.bytes_limit, 64 * 1024 * 1024);
        assert!(!budget.exhausted);
    }

    #[test]
    fn a_larger_fresh_budget_has_no_renewal_read_or_claimed_gate_entry() {
        let mut budget = PermissionEvidenceBudget::fresh_use_renewal_production();
        assert!(matches!(
            budget.use_renewal_resource_read(),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(matches!(
            budget.use_renewal_combined_resource_read(),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(matches!(
            super::begin_use_renewal_claimed_gate(0, &mut budget),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert_eq!(budget.resource.reads, 0);
        assert_eq!(budget.resource.combined_reads, 0);
        assert!(budget.runtime_state.is_none());
        assert!(budget.acceptance_identity.is_none());
        assert!(!budget.use_renewal_production_entered);
        assert!(!budget.ready_authorization_entered);
        assert!(!budget.ready_authorization_writer_entered);
        assert!(!budget.runtime_use_entered);
        // require_standard_account intentionally retains its old16Mi exclusion.
        // No assertion here treats a64Mi numeric ceiling as store/policy authority.
        // Private owning Account/selectors and their seals cannot be fabricated here.
    }
}

// Fresh private callback purpose. Existing account constructors, layout and
// ordinary debit limits remain unchanged. Numeric capacity grants no authority.
const USE_CALLBACK_ACCOUNT_BYTES: usize = 8 * 1024 * 1024 * 1024;
const USE_CALLBACK_ACCOUNT_NODES: usize = 67_108_864;

impl PermissionEvidenceBudget {
    pub(super) fn fresh_use_callback() -> Self {
        Self {
            bytes_limit: USE_CALLBACK_ACCOUNT_BYTES,
            ..Self::new()
        }
    }

    // Only the consuming callback brand calls this purpose-specific facade.
    // Ordinary, dispatch and renewal accounts cannot select its node ceiling.
    pub(super) fn callback_reserve(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        if self.bytes_limit != USE_CALLBACK_ACCOUNT_BYTES {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.reserve_callback(bytes, nodes)
    }

    fn reserve_callback(&mut self, bytes: usize, nodes: usize) -> Result<(), PermissionError> {
        if self.exhausted {
            return Err(PermissionError::LimitExceeded);
        }
        let next_bytes = self.usage.encoded_bytes.checked_add(bytes);
        let next_nodes = self.usage.structural_nodes.checked_add(nodes);
        match (next_bytes, next_nodes) {
            (Some(bytes), Some(nodes))
                if bytes <= self.bytes_limit && nodes <= USE_CALLBACK_ACCOUNT_NODES =>
            {
                self.usage.encoded_bytes = bytes;
                self.usage.structural_nodes = nodes;
                Ok(())
            }
            _ => {
                self.exhausted = true;
                Err(PermissionError::LimitExceeded)
            }
        }
    }
}

#[cfg(test)]
mod use_callback_budget_controls {
    use super::*;

    #[test]
    fn fresh_callback_account_has_zero_usage_and_no_authority() {
        let mut budget = PermissionEvidenceBudget::fresh_use_callback();
        assert_eq!(budget.bytes_limit, USE_CALLBACK_ACCOUNT_BYTES);
        assert_eq!(budget.usage.encoded_bytes, 0);
        assert_eq!(budget.usage.structural_nodes, 0);
        assert!(!budget.exhausted);
        assert!(budget.acceptance_identity.is_none());
        assert!(budget.runtime_state.is_none());
        assert!(!budget.ready_authorization_entered);
        assert!(!budget.ready_authorization_writer_entered);
        assert!(!budget.runtime_use_entered);
        assert!(!budget.use_renewal_scaffold_entered);
        assert!(!budget.use_renewal_production_entered);
        assert_eq!(budget.resource.reads, 0);
        assert_eq!(budget.resource.combined_reads, 0);
        assert_eq!(budget.resource.immediate_writes, 0);
        assert!(budget.resource.keys.is_empty());
        assert!(budget.callback_reserve(0, 0).is_ok());
        assert_eq!(budget.usage.encoded_bytes, 0);
        assert_eq!(budget.usage.structural_nodes, 0);
    }

    fn reject_other_purpose(mut budget: PermissionEvidenceBudget) {
        let before = budget.usage;
        let ceiling = budget.bytes_limit;
        assert!(matches!(
            budget.callback_reserve(0, 0),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(matches!(
            budget.callback_reserve(1, 1),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert_eq!(budget.usage, before);
        assert_eq!(budget.bytes_limit, ceiling);
        assert!(!budget.exhausted);
        assert!(budget.runtime_state.is_none());
        assert!(budget.acceptance_identity.is_none());
        assert!(!budget.use_renewal_scaffold_entered);
        assert!(!budget.use_renewal_production_entered);
        assert!(!budget.runtime_use_entered);
    }

    #[test]
    fn callback_debit_refuses_every_other_genuine_account_purpose() {
        reject_other_purpose(PermissionEvidenceBudget::new());
        reject_other_purpose(PermissionEvidenceBudget::for_use_dispatch());
        reject_other_purpose(PermissionEvidenceBudget::fresh_use_renewal_scaffold());
        reject_other_purpose(PermissionEvidenceBudget::fresh_use_renewal_production());
    }

    #[test]
    fn callback_byte_boundary_is_inclusive_and_exhaustion_is_sticky() {
        let mut budget = PermissionEvidenceBudget::fresh_use_callback();
        assert!(budget
            .callback_reserve(USE_CALLBACK_ACCOUNT_BYTES - 1, 0)
            .is_ok());
        assert!(budget.callback_reserve(1, 0).is_ok());
        let before = budget.usage;
        assert_eq!(before.encoded_bytes, USE_CALLBACK_ACCOUNT_BYTES);
        assert!(matches!(
            budget.callback_reserve(1, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage, before);
        assert!(budget.exhausted);
        assert!(matches!(
            budget.callback_reserve(0, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert!(matches!(
            budget.callback_reserve(0, 1),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage, before);
        assert_eq!(budget.bytes_limit, USE_CALLBACK_ACCOUNT_BYTES);
    }

    #[test]
    fn callback_node_boundary_is_inclusive_and_exhaustion_is_sticky() {
        let mut budget = PermissionEvidenceBudget::fresh_use_callback();
        assert!(budget
            .callback_reserve(0, USE_CALLBACK_ACCOUNT_NODES - 1)
            .is_ok());
        assert!(budget.callback_reserve(0, 1).is_ok());
        let before = budget.usage;
        assert_eq!(before.structural_nodes, USE_CALLBACK_ACCOUNT_NODES);
        assert!(matches!(
            budget.callback_reserve(0, 1),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage, before);
        assert!(budget.exhausted);
        assert!(matches!(
            budget.callback_reserve(0, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert!(matches!(
            budget.callback_reserve(1, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage, before);
        assert_eq!(budget.bytes_limit, USE_CALLBACK_ACCOUNT_BYTES);
    }

    #[test]
    fn callback_byte_addition_overflow_keeps_the_successful_prefix() {
        let mut budget = PermissionEvidenceBudget::fresh_use_callback();
        assert!(budget.callback_reserve(1, 1).is_ok());
        let before = budget.usage;
        assert!(matches!(
            budget.callback_reserve(usize::MAX, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert!(budget.exhausted);
        assert_eq!(budget.usage, before);
        assert!(matches!(
            budget.callback_reserve(0, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage, before);
    }

    #[test]
    fn callback_node_addition_overflow_keeps_the_successful_prefix() {
        let mut budget = PermissionEvidenceBudget::fresh_use_callback();
        assert!(budget.callback_reserve(1, 1).is_ok());
        let before = budget.usage;
        assert!(matches!(
            budget.callback_reserve(0, usize::MAX),
            Err(PermissionError::LimitExceeded)
        ));
        assert!(budget.exhausted);
        assert_eq!(budget.usage, before);
        assert!(matches!(
            budget.callback_reserve(0, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage, before);
    }
}

#[cfg(test)]
const _: () = {
    assert!(std::mem::size_of::<PermissionEvidenceBudget>() <= 512);
    assert!(std::mem::size_of::<(PermissionEvidenceBudget,)>() <= 512);
    assert!(std::mem::size_of::<fn(PermissionEvidenceBudget)>() <= 128);
    assert!(std::mem::size_of::<()>() <= 128);
    assert!(std::mem::size_of::<ResourceLedger>() <= 128);
    assert!(std::mem::size_of::<PermissionEvidenceUsage>() <= 128);
    assert!(std::mem::size_of::<Option<PermissionEvidenceUsage>>() <= 128);
    assert!(std::mem::size_of::<(&mut PermissionEvidenceBudget, usize, usize)>() <= 128);
    assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 128);
    assert!(std::mem::size_of::<Vec<Vec<u8>>>() <= 128);
    assert!(std::mem::size_of::<Vec<u8>>() == 24);
    assert!(std::mem::align_of::<Vec<u8>>() == 8);
    assert!(std::mem::size_of::<Option<Vec<Vec<u8>>>>() <= 128);
    assert!(std::mem::size_of::<std::ptr::NonNull<Vec<u8>>>() <= 128);
    assert!(std::mem::size_of::<*mut [Vec<u8>]>() <= 128);
};

// Unique immutable scaffold purpose; no existing cap or factory is changed.
const USE_OUTCOME_SCAFFOLD_BYTES: usize = 1_048_576;
const USE_OUTCOME_SCAFFOLD_NODES: usize = 65_536;
impl PermissionEvidenceBudget {
    #[cfg(test)]
    pub(super) fn fresh_use_outcome_scaffold() -> Self {
        Self {
            bytes_limit: USE_OUTCOME_SCAFFOLD_BYTES,
            ..Self::new()
        }
    }
    pub(super) fn outcome_scaffold_reserve(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        if self.bytes_limit == USE_OUTCOME_FUNCTIONAL_BYTES {
            return self.outcome_functional_reserve(bytes, nodes);
        }
        if self.bytes_limit != USE_OUTCOME_SCAFFOLD_BYTES {
            return Err(PermissionError::UnsupportedProfile);
        }
        if self.exhausted {
            return Err(PermissionError::LimitExceeded);
        }
        let next_bytes = self.usage.encoded_bytes.checked_add(bytes);
        let next_nodes = self.usage.structural_nodes.checked_add(nodes);
        match (next_bytes, next_nodes) {
            (Some(bytes), Some(nodes))
                if bytes <= USE_OUTCOME_SCAFFOLD_BYTES && nodes <= USE_OUTCOME_SCAFFOLD_NODES =>
            {
                self.usage.encoded_bytes = bytes;
                self.usage.structural_nodes = nodes;
                Ok(())
            }
            _ => {
                self.exhausted = true;
                Err(PermissionError::LimitExceeded)
            }
        }
    }
}
const _: () = {
    assert!(std::mem::size_of::<(Option<usize>, Option<usize>)>() <= 128);
    assert!(std::mem::size_of::<(&mut PermissionEvidenceBudget, usize, usize)>() <= 128);
    assert!(std::mem::size_of::<(PermissionEvidenceBudget,)>() <= 512);
    assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 128);
    assert!(std::mem::size_of::<(usize, usize)>() <= 128);
    assert!(std::mem::size_of::<(bool,)>() <= 128);
};

#[cfg(test)]
mod use_outcome_scaffold_budget_controls {
    use super::*;
    #[test]
    fn fresh_purpose_and_inclusive_caps_are_cumulative_and_sticky() {
        let mut budget = PermissionEvidenceBudget::fresh_use_outcome_scaffold();
        assert_eq!(budget.usage().encoded_bytes(), 0);
        assert_eq!(budget.usage().structural_nodes(), 0);
        assert!(budget
            .outcome_scaffold_reserve(
                USE_OUTCOME_SCAFFOLD_BYTES - 1,
                USE_OUTCOME_SCAFFOLD_NODES - 1
            )
            .is_ok());
        assert!(budget.outcome_scaffold_reserve(1, 1).is_ok());
        let paid = budget.usage();
        assert!(matches!(
            budget.outcome_scaffold_reserve(1, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(budget.usage(), paid);
        assert!(matches!(
            budget.outcome_scaffold_reserve(0, 0),
            Err(PermissionError::LimitExceeded)
        ));
    }
    #[test]
    fn other_genuine_factories_never_select_outcome_purpose() {
        for mut budget in [
            PermissionEvidenceBudget::new(),
            PermissionEvidenceBudget::for_use_dispatch(),
            PermissionEvidenceBudget::fresh_use_renewal_scaffold(),
            PermissionEvidenceBudget::fresh_use_renewal_production(),
            PermissionEvidenceBudget::fresh_use_callback(),
        ] {
            let before = budget.usage();
            assert!(matches!(
                budget.outcome_scaffold_reserve(0, 0),
                Err(PermissionError::UnsupportedProfile)
            ));
            assert_eq!(budget.usage(), before);
        }
    }
    #[test]
    fn checked_overflow_refuses_without_wrapping_or_replenishing() {
        for (bytes, nodes) in [(usize::MAX, 0), (0, usize::MAX)] {
            let mut budget = PermissionEvidenceBudget::fresh_use_outcome_scaffold();
            assert!(budget.outcome_scaffold_reserve(1, 1).is_ok());
            let paid = budget.usage();
            assert!(matches!(
                budget.outcome_scaffold_reserve(bytes, nodes),
                Err(PermissionError::LimitExceeded)
            ));
            assert_eq!(budget.usage(), paid);
            assert!(matches!(
                budget.outcome_scaffold_reserve(0, 0),
                Err(PermissionError::LimitExceeded)
            ));
        }
    }
}

// Admit only the distinct bounded functional outcome purpose. Scaffold and all
// previous purposes remain closed. This gate makes no exhaustive fee claim.
impl PermissionEvidenceBudget {
    pub(super) fn outcome_source_require_production(&mut self) -> Result<(), PermissionError> {
        self.outcome_source_inception_reserve(
            super::runtime_use_outcome_source::PRODUCTION_PROBE_FIXED,
            super::runtime_use_outcome_source::PRODUCTION_PROBE_NODES,
        )?;
        if self.bytes_limit == USE_OUTCOME_FUNCTIONAL_BYTES {
            Ok(())
        } else {
            Err(PermissionError::UnsupportedProfile)
        }
    }
    pub(super) fn outcome_source_reserve(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        self.outcome_source_require_production()?;
        self.reserve(bytes, nodes)
    }
    pub(super) fn use_outcome_resource_read(
        &mut self,
    ) -> Result<PermissionResourceRead<'_>, PermissionError> {
        self.outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
        Ok(PermissionResourceRead { budget: self })
    }
}
impl PermissionResourceRead<'_> {
    pub fn decode_use_outcome_execution(
        &mut self,
        cells: PermissionExecutionCells<'_>,
    ) -> Result<crate::StoredExecution, PermissionError> {
        self.budget
            .outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
        self.decode_execution(cells)
    }
    pub fn decode_use_outcome_thread(
        &mut self,
        cells: PermissionThreadCells<'_>,
    ) -> Result<crate::ThreadRecord, PermissionError> {
        self.budget
            .outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
        self.decode_thread(cells)
    }
}
pub(super) fn use_outcome_quote_resource_pair(
    old: &crate::approval::ApprovalResource,
    fresh: &crate::approval::ApprovalResource,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.outcome_source_require_production()?;
    budget.outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
    for resource in [old, fresh] {
        let snapshot = measure(
            CanonicalValue::Snapshot(&resource.execution.snapshot),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        let thread = measure(
            CanonicalValue::Thread(&resource.thread),
            CanonicalLimit::ResourceThread,
            budget,
        )?;
        // Complete comparison on this fresh operation. Original owner endpoints
        // remain exterior producer obligations; fresh Resource endpoints need
        // the separately reviewed preconnection incarnation invoice.
        let bytes = add(snapshot.bytes, thread.bytes)?;
        let nodes = add(snapshot.nodes, thread.nodes)?;
        budget.reserve(add(mul(128, nodes)?, mul(6, bytes)?)?, nodes)?;
        budget.reserve(
            add(mul(2, resource.execution.thread_id.as_str().len())?, 128)?,
            2,
        )?;
    }
    Ok(())
}
pub(super) fn use_outcome_quote_claimed_projection(
    before: &Snapshot,
    current: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
    budget.reserve(super::runtime_use_outcome_source::SOURCE_CALL_FIXED, 64)?;
    budget.outcome_source_require_production()?;
    for value in [before, current] {
        let q = measure(
            CanonicalValue::Snapshot(value),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        budget.reserve(mul(4, q.bytes)?, mul(4, q.nodes)?)?;
    }
    budget.reserve(16_384, 128)
}

pub(super) fn use_outcome_capture_journal(
    raw: &[u8],
    profile: super::runtime_mutation::PermissionRuntimeInputProfile,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(crate::JournalEntry, CanonicalMeasure), PermissionError> {
    budget.outcome_source_require_production()?;
    runtime_compat::capture_journal_with_profile(raw, profile, budget)
}
pub(super) fn use_outcome_quote_context(
    resource: &crate::approval::ApprovalResource,
    context: &crate::InvocationContext,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.outcome_source_reserve(super::runtime_use_outcome_source::SOURCE_CALL_FIXED, 64)?;
    let origin = resource
        .thread
        .origin
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let q = measure(
        CanonicalValue::Origin(origin),
        CanonicalLimit::ResourceThread,
        budget,
    )?;
    clone_quote(q, budget)?;
    budget.reserve(8 * 256 + 8 * 128, 16)?;
    if context.thread_id != resource.thread.id
        || context.thread_generation != resource.thread.generation
        || context.origin != *origin
        || context.execution_revision != resource.execution.snapshot.definition.revision
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(())
}
pub(super) fn use_outcome_quote_kernel(
    current: &Snapshot,
    invocation: &crate::EffectInvocation,
    actual: &crate::EffectOutcome,
    facts: &TransitionFacts,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    // Functional bounded profile: measure current resource and retained result
    // before invoking the existing kernel transition.
    budget.outcome_source_require_production()?;
    budget.outcome_source_reserve(super::runtime_use_outcome_source::SOURCE_CALL_FIXED, 64)?;
    // Conditional finite profile proposal: measured one-task turn history bound
    // for the actual insertion-sort graph; no restriction to Bool-only results.
    budget.outcome_source_reserve(OUTCOME_KERNEL_SHAPE_FIXED, OUTCOME_KERNEL_SHAPE_NODES)?;
    if current.tasks.len() != 1
        || current.turn_history.len() > 20
        || !cfg!(target_arch = "aarch64")
        || !cfg!(target_pointer_width = "64")
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    if facts.now_ms < current.updated_at_ms
        || invocation.owner_generation != current.owner_generation
    {
        return Err(PermissionError::CorruptRecord);
    }
    let before = measure(
        CanonicalValue::Snapshot(current),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    clone_quote(before, budget)?;
    let result = match actual {
        crate::EffectOutcome::Succeeded(result) => CanonicalValue::TaskResult(result),
        crate::EffectOutcome::Failed(failure) => CanonicalValue::String(failure),
        crate::EffectOutcome::Cancelled => {
            return budget.outcome_kernel_require_functional();
        }
    };
    let result = measure(result, CanonicalLimit::ResourceSnapshot, budget)?;
    clone_quote(result, budget)?;
    // The distinct outcome-purpose gate precedes the actual kernel transition.
    budget.outcome_kernel_require_functional()
}
pub(super) fn use_outcome_quote_plan_pair(
    a: &b4_contract::CommitPlan,
    b: &b4_contract::CommitPlan,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    budget.outcome_source_reserve(super::runtime_use_outcome_source::SOURCE_CALL_FIXED, 64)?;
    for plan in [a, b] {
        let q = measure(
            CanonicalValue::Snapshot(&plan.snapshot),
            CanonicalLimit::ResourceSnapshot,
            budget,
        )?;
        clone_quote(q, budget)?;
        budget.reserve(
            plan.dispatchable.len() * (4 * 256 + 4 * 128),
            plan.dispatchable.len(),
        )?;
    }
    Ok(())
}
#[cfg(test)]
mod outcome_production_closed_controls {
    use super::*;
    #[test]
    fn existing_accounts_never_fund_the_new_graph() {
        for mut budget in [
            PermissionEvidenceBudget::new(),
            PermissionEvidenceBudget::for_use_dispatch(),
            PermissionEvidenceBudget::fresh_use_outcome_scaffold(),
            PermissionEvidenceBudget::fresh_use_renewal_production(),
        ] {
            let before = budget.usage();
            assert!(budget.outcome_source_reserve(0, 0).is_err());
            if budget.bytes_limit == USE_OUTCOME_SCAFFOLD_BYTES {
                assert!(budget.usage().encoded_bytes() > before.encoded_bytes());
            } else {
                assert_eq!(budget.usage(), before);
            }
        }
    }
}

impl PermissionEvidenceBudget {
    // Kernel admission uses the same functional purpose and sticky ledger.
    // This is not exhaustive per-standard-library-operation qualification.
    pub(super) fn outcome_kernel_require_functional(&mut self) -> Result<(), PermissionError> {
        self.outcome_source_require_production()
    }
}

impl PermissionEvidenceBudget {
    // Scaffold controls retain their own ceiling. Functional outcome inception
    // delegates to the fresh cumulative outcome purpose before any debit.
    pub(super) fn outcome_source_inception_reserve(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        self.outcome_scaffold_reserve(bytes, nodes)
    }
}

// Outcome serialization retains canonical input/depth/node and encoded bounds.
// Its functional ledger does not claim exhaustive codec-operation qualification.
impl PermissionEvidenceBudget {
    pub(super) fn outcome_serializer_require_functional(&mut self) -> Result<(), PermissionError> {
        self.outcome_source_require_production()
    }
}
pub(super) fn use_outcome_encode_snapshot(
    snapshot: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    budget.outcome_serializer_require_functional()?;
    budget.outcome_source_reserve(
        super::runtime_use_outcome_writer::SERIALIZE_CALL_FIXED,
        super::runtime_use_outcome_writer::SERIALIZE_CALL_NODES,
    )?;
    let q = measure(
        CanonicalValue::Snapshot(snapshot),
        CanonicalLimit::ResourceSnapshot,
        budget,
    )?;
    budget.reserve(add(mul(13, q.bytes)?, 256)?, 2)?;
    let bytes = serde_json::to_string(snapshot).map_err(
        super::runtime_use_outcome_writer::serializer_checked_value(|_| {
            PermissionError::CorruptRecord
        }),
    )?;
    if bytes.len() > 1_048_576 {
        return Err(PermissionError::LimitExceeded);
    }
    Ok(bytes)
}
pub(super) fn use_outcome_journal(
    snapshot: &Snapshot,
    budget: &mut PermissionEvidenceBudget,
) -> Result<crate::JournalEntry, PermissionError> {
    budget.outcome_serializer_require_functional()?;
    budget.outcome_source_reserve(
        super::runtime_use_outcome_writer::SERIALIZE_CALL_FIXED,
        super::runtime_use_outcome_writer::SERIALIZE_CALL_NODES,
    )?;
    budget.reserve(add(mul(13, snapshot.execution_id.as_str().len())?, 256)?, 2)?;
    Ok(crate::JournalEntry::from(snapshot))
}
pub(super) fn use_outcome_encode_journal(
    journal: &crate::JournalEntry,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    budget.outcome_serializer_require_functional()?;
    budget.outcome_source_reserve(
        super::runtime_use_outcome_writer::SERIALIZE_CALL_FIXED,
        super::runtime_use_outcome_writer::SERIALIZE_CALL_NODES,
    )?;
    let q = writer_journal_measure(journal)?;
    if q.bytes > 1024 || q.nodes > 11 || q.depth > 1 {
        return Err(PermissionError::UnsupportedProfile);
    }
    budget.reserve(add(mul(13, q.bytes)?, 256)?, 2)?;
    let bytes = serde_json::to_string(journal).map_err(
        super::runtime_use_outcome_writer::serializer_checked_value(|_| {
            PermissionError::CorruptRecord
        }),
    )?;
    if bytes.len() > 1024 {
        return Err(PermissionError::LimitExceeded);
    }
    Ok(bytes)
}

// Fresh finite H/T/target discrimination Args/Result/Try/branch/end proposal,
// before either Unsupported or measured clone/strict/kernel traversal.
const OUTCOME_KERNEL_SHAPE_FIXED: usize = 4 * 128 + 19 * (128 + 2 * 128) + 12 * 128 + 6 * 128;
const OUTCOME_KERNEL_SHAPE_NODES: usize = 4 + 19 * 3 + 12 + 6;

// Functional continuation: bounded logical ledger, not exhaustive std-operation qualification.
const USE_OUTCOME_FUNCTIONAL_BYTES: usize = 4 * 1024 * 1024 * 1024;
const USE_OUTCOME_FUNCTIONAL_NODES: usize = 16 * 1024 * 1024;
impl PermissionEvidenceBudget {
    pub(super) fn fresh_use_outcome_functional() -> Self {
        Self {
            bytes_limit: USE_OUTCOME_FUNCTIONAL_BYTES,
            ..Self::new()
        }
    }
    fn outcome_functional_reserve(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        if self.bytes_limit != USE_OUTCOME_FUNCTIONAL_BYTES {
            return Err(PermissionError::UnsupportedProfile);
        }
        if self.exhausted {
            return Err(PermissionError::LimitExceeded);
        }
        match (
            self.usage.encoded_bytes.checked_add(bytes),
            self.usage.structural_nodes.checked_add(nodes),
        ) {
            (Some(bytes), Some(nodes))
                if bytes <= USE_OUTCOME_FUNCTIONAL_BYTES
                    && nodes <= USE_OUTCOME_FUNCTIONAL_NODES =>
            {
                self.usage.encoded_bytes = bytes;
                self.usage.structural_nodes = nodes;
                Ok(())
            }
            _ => {
                self.exhausted = true;
                Err(PermissionError::LimitExceeded)
            }
        }
    }
}
#[cfg(test)]
mod use_outcome_functional_budget_controls {
    use super::*;
    #[test]
    fn inclusive_caps_and_nested_reserve_are_sticky() {
        let mut budget = PermissionEvidenceBudget::fresh_use_outcome_functional();
        assert_eq!(budget.usage().encoded_bytes(), 0);
        assert_eq!(budget.usage().structural_nodes(), 0);
        assert!(budget
            .reserve(
                USE_OUTCOME_FUNCTIONAL_BYTES - 1,
                USE_OUTCOME_FUNCTIONAL_NODES - 1
            )
            .is_ok());
        assert!(budget.reserve(1, 1).is_ok());
        let paid = budget.usage();
        assert!(matches!(
            budget.reserve(1, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(paid, budget.usage());
        assert!(matches!(
            budget.reserve(0, 0),
            Err(PermissionError::LimitExceeded)
        ));
        let mut nodes = PermissionEvidenceBudget::fresh_use_outcome_functional();
        assert!(nodes.reserve(0, USE_OUTCOME_FUNCTIONAL_NODES).is_ok());
        assert!(matches!(
            nodes.reserve(0, 1),
            Err(PermissionError::LimitExceeded)
        ));
    }
    #[test]
    fn overflow_preserves_prefix_and_all_old_purposes_refuse() {
        for (bytes, nodes) in [(usize::MAX, 0), (0, usize::MAX)] {
            let mut budget = PermissionEvidenceBudget::fresh_use_outcome_functional();
            assert!(budget.reserve(1, 1).is_ok());
            let paid = budget.usage();
            assert!(matches!(
                budget.reserve(bytes, nodes),
                Err(PermissionError::LimitExceeded)
            ));
            assert_eq!(paid, budget.usage());
            assert!(matches!(
                budget.reserve(0, 0),
                Err(PermissionError::LimitExceeded)
            ));
        }
        for mut old in [
            PermissionEvidenceBudget::new(),
            PermissionEvidenceBudget::for_use_dispatch(),
            PermissionEvidenceBudget::fresh_use_renewal_scaffold(),
            PermissionEvidenceBudget::fresh_use_renewal_production(),
            PermissionEvidenceBudget::fresh_use_callback(),
            PermissionEvidenceBudget::fresh_use_outcome_scaffold(),
        ] {
            assert_ne!(old.bytes_limit, USE_OUTCOME_FUNCTIONAL_BYTES);
            assert!(matches!(
                old.outcome_source_require_production(),
                Err(PermissionError::UnsupportedProfile)
            ));
        }
    }
}

const _: () = {
    assert!(USE_OUTCOME_FUNCTIONAL_BYTES != ACCOUNT_BYTES);
    assert!(USE_OUTCOME_FUNCTIONAL_BYTES != USE_DISPATCH_ACCOUNT_BYTES);
    assert!(USE_OUTCOME_FUNCTIONAL_BYTES != USE_RENEWAL_SCAFFOLD_BYTES);
    assert!(USE_OUTCOME_FUNCTIONAL_BYTES != USE_RENEWAL_PRODUCTION_BYTES);
    assert!(USE_OUTCOME_FUNCTIONAL_BYTES != USE_CALLBACK_ACCOUNT_BYTES);
    assert!(USE_OUTCOME_FUNCTIONAL_BYTES != USE_OUTCOME_SCAFFOLD_BYTES);
};

// Private cumulative allowance for failure-owned withdrawal and explicit storage retry.
// Existing per-record/source/resource limits and old account purposes remain unchanged.
const USE_WITHHELD_FUNCTIONAL_BYTES: usize = 32 * 1024 * 1024;
const USE_WITHHELD_FUNCTIONAL_NODES: usize = 262_144;
impl PermissionEvidenceBudget {
    pub(super) fn fresh_use_withheld_functional() -> Self {
        Self {
            bytes_limit: USE_WITHHELD_FUNCTIONAL_BYTES,
            ..Self::new()
        }
    }
    fn withheld_functional_reserve(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        if self.bytes_limit != USE_WITHHELD_FUNCTIONAL_BYTES {
            return Err(PermissionError::UnsupportedProfile);
        }
        if self.exhausted {
            return Err(PermissionError::LimitExceeded);
        }
        match (
            self.usage.encoded_bytes.checked_add(bytes),
            self.usage.structural_nodes.checked_add(nodes),
        ) {
            (Some(bytes), Some(nodes))
                if bytes <= USE_WITHHELD_FUNCTIONAL_BYTES
                    && nodes <= USE_WITHHELD_FUNCTIONAL_NODES =>
            {
                self.usage.encoded_bytes = bytes;
                self.usage.structural_nodes = nodes;
                Ok(())
            }
            _ => {
                self.exhausted = true;
                Err(PermissionError::LimitExceeded)
            }
        }
    }
}

// Dedicated deletion account; immutable purpose and normal historical identifiers.
impl PermissionEvidenceBudget {
    pub(super) fn for_settled_thread_deletion() -> Self {
        Self {
            // Preserve the existing pure-codec charging-path identity. Only the
            // sealed deletion purpose selects the effective 64MiB byte ceiling.
            bytes_limit: 16 * 1024 * 1024,
            settled_thread_deletion: true,
            original_always_profile: OriginalAlwaysProfile::NormalOriginalAlways,
            ..Self::new()
        }
    }
    pub(super) fn settled_deletion_journal_preflight(
        &mut self,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        preflight_profile(raw, RawProfile::Legacy, self)
    }
}
