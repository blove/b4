//! Opaque ownership declarations for the separately qualified runtime claim path.
//! Construction and paid runtime helpers remain pending; these types grant no authority.
#![allow(dead_code)] // Exact declarations await their independently reviewed consumers.

use super::runtime_use::{
    PermissionRuntimeEvidenceKind, PermissionRuntimeUseEvidence, PermissionRuntimeUseSession,
    PermissionRuntimeUseWitness,
};
use super::{
    PermissionAcceptance, PermissionCatalog, PermissionDecision, PermissionError,
    PermissionEvidenceBudget, ToolBindingRegistration,
};
use crate::approval::{self, ApprovalAcceptance, ApprovalResource, ApprovalView, GrantRecord};
use crate::{EffectInvocation, InvocationContext, JournalEntry, StoredExecution, ThreadRecord};
use b4_contract::{CommitPlan, Definition};
use std::{mem::size_of, sync::Arc};

// A private strong allocation identity. It is never a quote, receipt or authority.
pub(super) struct PermissionRuntimeOrigin;
/// Trusted private selection of finite input bounds; it confers no authority.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PermissionRuntimeInputProfile {
    Strict,
    NormalNewUse,
    NormalOriginalAlways,
    NormalReadyCancellation,
}
impl PermissionRuntimeInputProfile {
    fn bounded(
        self,
        q: PermissionRuntimeMeasure,
        strict: (usize, usize, usize),
        normal: (usize, usize, usize),
    ) -> bool {
        let (bytes, nodes, depth) = match self {
            Self::Strict => strict,
            Self::NormalNewUse | Self::NormalOriginalAlways | Self::NormalReadyCancellation => {
                normal
            }
        };
        runtime_bounded(q, bytes, nodes, depth)
    }
    // Descriptive IDs and their enclosing TaskSpec grow together only in the
    // two entered normal lanes; cardinality and depth keep their old bounds.
    pub(super) fn task_spec_bytes(self) -> usize {
        match self {
            Self::Strict => 88,
            Self::NormalNewUse | Self::NormalOriginalAlways | Self::NormalReadyCancellation => {
                1_024
            }
        }
    }
    fn task_spec(self, q: PermissionRuntimeMeasure) -> bool {
        runtime_bounded(q, self.task_spec_bytes(), 9, 1)
    }
    fn new_use_bytes(self, strict: usize, normal: usize) -> usize {
        if matches!(self, Self::NormalNewUse | Self::NormalOriginalAlways) {
            normal
        } else {
            strict
        }
    }
    fn task_id_bytes(self) -> usize {
        self.new_use_bytes(7, 256)
    }
    fn effect_id_bytes(self) -> usize {
        self.new_use_bytes(14, 256)
    }
    fn definition(self, q: PermissionRuntimeMeasure) -> bool {
        runtime_bounded(q, self.new_use_bytes(181, 2_048), 19, 4)
    }
    pub(super) fn ready_snapshot(self, q: PermissionRuntimeMeasure) -> bool {
        if self == Self::NormalReadyCancellation {
            runtime_bounded(q, 4_096, 96, 64)
        } else {
            self.bounded(q, (960, 96, 127), (65_536, 16_384, 64))
        }
    }
    pub(super) fn dispatch_snapshot(self, q: PermissionRuntimeMeasure) -> bool {
        if self == Self::NormalReadyCancellation {
            runtime_bounded(q, 4_096, 100, 64)
        } else {
            self.bounded(q, (2_048, 100, 127), (65_536, 16_384, 64))
        }
    }
    pub(super) fn input(self, q: PermissionRuntimeMeasure) -> bool {
        if self == Self::NormalReadyCancellation {
            runtime_bounded(q, 31, 5, 127)
        } else {
            self.bounded(q, (31, 5, 127), (65_536, 16_384, 64))
        }
    }
    pub(super) fn thread(self, q: PermissionRuntimeMeasure) -> bool {
        self.bounded(q, (306, 21, 2), (1_024, 21, 2))
    }
    pub(super) fn journal(self, q: PermissionRuntimeMeasure) -> bool {
        self.bounded(q, (160, 11, 1), (1_024, 11, 1))
    }
    // Historical limits belong only to the explicitly entered cancellation lane.
    fn cancellation_bytes(self, strict: usize, normal: usize) -> usize {
        if matches!(
            self,
            Self::NormalReadyCancellation | Self::NormalOriginalAlways
        ) {
            normal
        } else {
            strict
        }
    }
    fn cancellation_history(
        self,
        q: PermissionRuntimeMeasure,
        strict: (usize, usize, usize),
        normal_bytes: usize,
    ) -> bool {
        runtime_bounded(
            q,
            self.cancellation_bytes(strict.0, normal_bytes),
            strict.1,
            strict.2,
        )
    }
    // Structured human history is qualified only for genuine normal originals.
    // Other purposes retain their existing cancellation-history geometry.
    fn human_history_acceptance(self, q: PermissionRuntimeMeasure) -> bool {
        if self == Self::NormalOriginalAlways {
            runtime_bounded(q, 16_384, 768, 64)
        } else {
            self.cancellation_history(q, (1_766, 154, 64), 8_192)
        }
    }
    fn human_history_before(self, q: PermissionRuntimeMeasure) -> bool {
        if self == Self::NormalOriginalAlways {
            runtime_bounded(q, 8_192, 512, 64)
        } else {
            self.cancellation_history(q, (848, 91, 64), 4_096)
        }
    }
    pub(super) fn historical_journal(self, q: PermissionRuntimeMeasure) -> bool {
        if matches!(self, Self::NormalNewUse | Self::NormalOriginalAlways) {
            runtime_bounded(q, 1_024, 11, 1)
        } else {
            self.cancellation_history(q, (160, 11, 1), 1_024)
        }
    }
    pub(super) fn cancelled_snapshot(self, q: PermissionRuntimeMeasure) -> bool {
        if self == Self::NormalReadyCancellation {
            runtime_bounded(q, 4_096, 100, 64)
        } else {
            runtime_bounded(q, 2_048, 100, 127)
        }
    }
    pub(super) fn cancellation_journal(self, q: PermissionRuntimeMeasure) -> bool {
        self.cancellation_history(q, (256, 11, 1), 1_024)
    }
    fn execution_id_bytes(self) -> usize {
        match self {
            Self::Strict => 20,
            Self::NormalNewUse | Self::NormalOriginalAlways | Self::NormalReadyCancellation => {
                b4_contract::MAX_ID_BYTES
            }
        }
    }
    fn turn_id_bytes(self) -> usize {
        match self {
            Self::Strict => 13,
            Self::NormalNewUse | Self::NormalOriginalAlways | Self::NormalReadyCancellation => {
                b4_contract::MAX_ID_BYTES
            }
        }
    }
    fn turn_history_bytes(self) -> usize {
        match self {
            Self::Strict => 26,
            Self::NormalNewUse | Self::NormalOriginalAlways | Self::NormalReadyCancellation => 512,
        }
    }
}
pub(super) type PermissionRuntimeOriginOwner = Arc<PermissionRuntimeOrigin>;
// Retain the actual checked canonical measurement type; no numeric constructor.
type PermissionRuntimeMeasure = super::validation::CanonicalMeasure;
struct PermissionRuntimeResourceFacts {
    actual: [PermissionRuntimeMeasure; 6],
    raw_and_scalar: [usize; 8],
}
struct PermissionRuntimeWitnessFacts {
    actual: [PermissionRuntimeMeasure; 9],
    admitted_counts_and_flags: [usize; 2],
}
struct PermissionImmutableDispatchWitness {
    acceptance: PermissionAcceptance,
    decision: PermissionDecision,
    grant: GrantRecord,
    park: JournalEntry,
    resume: JournalEntry,
    raw: [Vec<u8>; 5],
    raw_geometry: [runtime_compat::RuntimeHumanRawGeometry; 3],
    facts: PermissionRuntimeWitnessFacts,
    origin: Arc<PermissionRuntimeOrigin>,
}
struct PermissionRuntimeObservation {
    resource: ApprovalResource,
    facts: PermissionRuntimeResourceFacts,
    origin: Arc<PermissionRuntimeOrigin>,
}

// Public captures expose no variants or construction route.
pub struct PermissionRuntimeCapture {
    inner: PermissionRuntimeCaptureKind,
}

// These declarations preserve the stable outer C1 and real short Session loan.
// No factory, getter, phase mutation, SQL admission or service body is present.
pub(super) enum PermissionRuntimeOperationKind {
    Dispatch,
    CompleteDeferred,
    ReadyCancellation,
    UseCancellation,
}
pub(super) enum PermissionRuntimePhase {
    ObserveR1Started,
    AdmissionInProgress,
    ObserveR1Committed,
    ReadyAuthorityCommitted,
    UnclaimedGated,
    ObserveR2Committed,
    Prepared,
    ClaimedCommitted,
    ObserveR3Committed,
    ClaimedAuthorityCommitted,
    ClaimedGated,
    ObserveR4Committed,
    PermitIssued,
    ReadyCancellationR1Started,
    ReadyCancellationR1Committed,
    ReadyCancellationR1Authorized,
    ReadyCancellationC1Committed,
    ReadyCancellationReadyVerified,
    ReadyCancellationR2Committed,
    ReadyCancellationPrepared,
    ReadyCancellationCommitted,
}
pub(super) enum PermissionRuntimeReadRole {
    ObserveR1Preclaim,
    ReadyAuthorityC1,
    ObserveR2Refence,
    ObserveR3Claimed,
    ClaimedAuthorityC2,
    ObserveR4Refence,
}
pub(super) struct PermissionRuntimeState {
    pub(super) origin: Arc<PermissionRuntimeOrigin>,
    pub(super) input_profile: PermissionRuntimeInputProfile,
    pub(super) kind: PermissionRuntimeOperationKind,
    pub(super) phase: PermissionRuntimePhase,
}
struct PermissionRuntimeCatalogFacts<'catalog> {
    actual: [PermissionRuntimeMeasure; 4],
    catalog: &'catalog PermissionCatalog,
    definition: &'catalog Definition,
    registration: &'catalog ToolBindingRegistration,
    origin: Arc<PermissionRuntimeOrigin>,
}
struct PermissionLegacyOnceWitnessFacts {
    actual: [PermissionRuntimeMeasure; 5],
    admitted_counts_and_flags: [usize; 2],
}
struct PermissionLegacyOnceWitness {
    grants: Vec<GrantRecord>,
    acceptances: Vec<ApprovalAcceptance>,
    journals: Vec<JournalEntry>,
    raw: [Vec<u8>; 4],
    facts: PermissionLegacyOnceWitnessFacts,
    origin: Arc<PermissionRuntimeOrigin>,
}
#[allow(clippy::large_enum_variant)] // Inline capture owners retain the separately paid bounded allocation schedule.
enum PermissionReadyDispatchCapture {
    NewUse {
        observation: PermissionRuntimeObservation,
        witness: Arc<PermissionRuntimeUseWitness>,
        origin: Arc<PermissionRuntimeOrigin>,
    },
    NewHuman {
        observation: PermissionRuntimeObservation,
        witness: Arc<PermissionImmutableDispatchWitness>,
        origin: Arc<PermissionRuntimeOrigin>,
    },
    LegacyOnce {
        view: ApprovalView,
        resource_facts: PermissionRuntimeResourceFacts,
        legacy_facts: PermissionLegacyOnceWitnessFacts,
        journals: Vec<JournalEntry>,
        raw: [Vec<u8>; 4],
        origin: Arc<PermissionRuntimeOrigin>,
    },
}
#[allow(clippy::large_enum_variant)] // Inline legacy witness retains the separately paid bounded ownership schedule.
enum PermissionClaimWitness {
    NewUse(Arc<PermissionRuntimeUseWitness>),
    NewHuman(Arc<PermissionImmutableDispatchWitness>),
    LegacyOnce(PermissionLegacyOnceWitness),
}
// A later C2 match owns only actual paid match facts, never a fabricated A/D witness.
struct PermissionRuntimeLegacyClaimedMatch {
    facts: PermissionLegacyOnceWitnessFacts,
    current_checked: [usize; 8],
    origin: Arc<PermissionRuntimeOrigin>,
}
enum PermissionRuntimeClaimedWitness {
    NewHuman(Arc<PermissionImmutableDispatchWitness>),
    LegacyOnce(PermissionRuntimeLegacyClaimedMatch),
}
enum PermissionRuntimeCaptureKind {
    Observed(PermissionRuntimeObservation),
    Ready(PermissionReadyDispatchCapture),
    Claimed {
        observation: PermissionRuntimeObservation,
        witness: PermissionRuntimeClaimedWitness,
        dispatch_journal: JournalEntry,
        dispatch_measure: PermissionRuntimeMeasure,
        origin: Arc<PermissionRuntimeOrigin>,
    },
}
struct PermissionRuntimePostReadCertificate {
    encoded_bytes: usize,
    structural_nodes: usize,
    normal_sql_slots: usize,
    capacity_sql_slots: usize,
}
struct PermissionRuntimePostFacts {
    actual: PermissionRuntimeResourceFacts,
    fresh_read: PermissionRuntimePostReadCertificate,
}
struct PermissionRuntimeCandidatePostFacts {
    actual: [PermissionRuntimeMeasure; 3],
}
pub struct PermissionPreparedRuntimeMutation<'capture, 'catalog> {
    capture: &'capture PermissionReadyDispatchCapture,
    requested_execution: &'capture b4_contract::ExecutionId,
    requested_context: &'capture approval::ApprovalContext,
    fence: approval::ApprovalFence,
    requested_task: b4_contract::TaskId,
    requested_attempt: b4_contract::AttemptId,
    facts: b4_contract::TransitionFacts,
    candidate_plan: CommitPlan,
    candidate_post_facts: PermissionRuntimeCandidatePostFacts,
    invocation: EffectInvocation,
    context: InvocationContext,
    catalog: PermissionRuntimeCatalogFacts<'catalog>,
    origin: Arc<PermissionRuntimeOrigin>,
}
struct PermissionPreparedRuntimeBorrowParts<'short, 'capture, 'catalog> {
    capture: &'capture PermissionReadyDispatchCapture,
    candidate_plan: &'short CommitPlan,
    requested_task: &'short b4_contract::TaskId,
    requested_attempt: &'short b4_contract::AttemptId,
    invocation: &'short EffectInvocation,
    context: &'short InvocationContext,
    catalog: &'short PermissionRuntimeCatalogFacts<'catalog>,
    post_facts: &'short PermissionRuntimeCandidatePostFacts,
    origin: &'short Arc<PermissionRuntimeOrigin>,
}
struct PermissionPreparedDispatchOwnedParts<'catalog> {
    invocation: EffectInvocation,
    context: InvocationContext,
    catalog: PermissionRuntimeCatalogFacts<'catalog>,
    origin: Arc<PermissionRuntimeOrigin>,
}
struct PermissionRuntimeNewHeader {
    primary: String,
    digest: Option<[u8; 64]>,
    previous: Option<u64>,
    committed: Option<u64>,
    bytes: usize,
}
struct PermissionRuntimeGrantHeader {
    interrupt: String,
    parking: u64,
    consumed: bool,
    bytes: usize,
}
struct PermissionRuntimeJournalHeader {
    sequence: i64,
    revision: u64,
    owner: u64,
    status: b4_contract::ExecutionStatus,
    time: u64,
    bytes: usize,
}
struct PermissionRuntimeFreshMatchStorage {
    headers: [Option<PermissionRuntimeNewHeader>; 2],
    raw: [Vec<u8>; 5],
    journal_headers: Vec<PermissionRuntimeJournalHeader>,
    range_counts_and_flags: [usize; 8],
}
struct PermissionRuntimeClaimLink<'input> {
    resource: &'input ApprovalResource,
    invocation: &'input EffectInvocation,
    context: &'input InvocationContext,
    witness: &'input PermissionClaimWitness,
    catalog: &'input PermissionCatalog,
    origin: &'input Arc<PermissionRuntimeOrigin>,
}
struct PermissionRuntimeFirstResourceRequest<'input> {
    requested_execution: &'input b4_contract::ExecutionId,
    requested_context: &'input approval::ApprovalContext,
}
enum PermissionRuntimeMatchedReadRole {
    ReadyAuthorityC1,
    ObserveR2Refence,
    ObserveR3Claimed,
    ClaimedAuthorityC2,
    ObserveR4Refence,
}
struct PermissionRuntimeMatchedReadRequest<'input> {
    role: PermissionRuntimeMatchedReadRole,
    requested_execution: &'input b4_contract::ExecutionId,
    context: &'input InvocationContext,
    definition: &'input Definition,
    expected_resource: &'input ApprovalResource,
    expected_facts: &'input PermissionRuntimeResourceFacts,
    task_id: Option<&'input b4_contract::TaskId>,
    claimed: Option<PermissionRuntimeClaimLink<'input>>,
}
enum PermissionRuntimeReadMode<'input> {
    FirstResource(PermissionRuntimeFirstResourceRequest<'input>),
    Matched(PermissionRuntimeMatchedReadRequest<'input>),
}
pub struct PermissionRuntimeSelector<'input> {
    origin: &'input PermissionRuntimeOriginOwner,
    mode: PermissionRuntimeReadMode<'input>,
}
pub struct PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    admission: super::validation::RuntimeDispatchAdmission<'budget>,
    selector: &'selector PermissionRuntimeSelector<'input>,
    origin: Arc<PermissionRuntimeOrigin>,
    role: PermissionRuntimeReadRole,
    metadata_checked: bool,
    stage: usize,
    execution: Option<(
        StoredExecution,
        [Option<PermissionRuntimeMeasure>; 5],
        [usize; 4],
    )>,
    thread: Option<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>,
    headers: [Option<PermissionRuntimeNewHeader>; 2],
    counts: [usize; 5],
    grant_header: Option<PermissionRuntimeGrantHeader>,
    acceptance: Option<(PermissionAcceptance, [PermissionRuntimeMeasure; 3])>,
    decision: Option<(PermissionDecision, [PermissionRuntimeMeasure; 2])>,
    grant: Option<(GrantRecord, [PermissionRuntimeMeasure; 2])>,
    human_geometry: [Option<runtime_compat::RuntimeHumanRawGeometry>; 3],
    old_acceptances: Vec<(ApprovalAcceptance, PermissionRuntimeMeasure)>,
    journals: Vec<(JournalEntry, PermissionRuntimeMeasure)>,
    journal_header: Option<PermissionRuntimeJournalHeader>,
    raw: [Vec<u8>; 5],
    dispatch_raw: Vec<u8>,
    resource_facts: Option<PermissionRuntimeResourceFacts>,
    witness_facts: Option<PermissionRuntimeWitnessFacts>,
    legacy_facts: Option<PermissionLegacyOnceWitnessFacts>,
    dispatch_measure: Option<PermissionRuntimeMeasure>,
    complete_flags: [bool; 8],
    completed_capture: Result<PermissionRuntimeCapture, PermissionError>,
}
pub struct PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    admission: super::validation::RuntimeDispatchAdmission<'budget>,
    prepared: &'prepared PermissionPreparedRuntimeMutation<'capture, 'catalog>,
    origin: Arc<PermissionRuntimeOrigin>,
    role: PermissionRuntimeReadRole,
    stage: usize,
    metadata_checked: bool,
    current_owner: u64,
    execution: Option<(
        StoredExecution,
        [Option<PermissionRuntimeMeasure>; 5],
        [usize; 4],
    )>,
    thread: Option<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>,
    match_storage: Option<PermissionRuntimeFreshMatchStorage>,
    checked_resource_facts: Option<PermissionRuntimeResourceFacts>,
    complete_flags: [bool; 8],
    use_matched: bool,
}
struct PermissionRuntimeBeforeScalars {
    execution_revision: u64,
    resource_revision: u64,
    owner_generation: u64,
    next_execution_revision: u64,
    next_resource_revision: u64,
    updated_at_ms: u64,
}
pub struct PermissionRuntimeWork<'work, 'budget, 'prepared, 'capture, 'catalog>
where
    'budget: 'work,
    'prepared: 'work,
    'capture: 'prepared,
    'catalog: 'prepared,
{
    session: &'work mut PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>,
    before_execution: StoredExecution,
    post_thread: ThreadRecord,
    authoritative_plan: CommitPlan,
    post_facts: PermissionRuntimeResourceFacts,
    snapshot_payload: String,
    journal_payload: String,
    journal: JournalEntry,
    before_scalars: PermissionRuntimeBeforeScalars,
    sql_stage: usize,
    fresh_match_storage: PermissionRuntimeFreshMatchStorage,
    origin: Arc<PermissionRuntimeOrigin>,
    post_read_certificate: PermissionRuntimePostReadCertificate,
}
pub struct PermissionRuntimeBindings<'binding> {
    execution_id: &'binding str,
    thread_id: &'binding str,
    generation: &'binding str,
    before_revision: u64,
    before_resource_revision: u64,
    owner_generation: u64,
    next_revision: u64,
    next_resource_revision: u64,
    updated_at_ms: u64,
    snapshot: &'binding str,
    journal: &'binding str,
}
struct PermissionRuntimeDispatchCommitted;
pub struct PermissionRuntimeCommit {
    resource: ApprovalResource,
    facts: PermissionRuntimeResourceFacts,
    origin: Arc<PermissionRuntimeOrigin>,
    committed: PermissionRuntimeDispatchCommitted,
}
pub struct PermissionDispatchClaim<'catalog> {
    resource: ApprovalResource,
    facts: PermissionRuntimeResourceFacts,
    invocation: EffectInvocation,
    context: InvocationContext,
    catalog: PermissionRuntimeCatalogFacts<'catalog>,
    witness: PermissionClaimWitness,
    origin: Arc<PermissionRuntimeOrigin>,
}
struct PermissionRuntimeRenewalProof {
    current_checked: [usize; 8],
    origin: Arc<PermissionRuntimeOrigin>,
}
pub struct PermissionDispatchPermit<'claim, 'catalog>
where
    'catalog: 'claim,
{
    claim: &'claim PermissionDispatchClaim<'catalog>,
    catalog: &'catalog PermissionCatalog,
    selected_definition: &'catalog Definition,
    renewal_proof: PermissionRuntimeRenewalProof,
    origin: Arc<PermissionRuntimeOrigin>,
}
// Unconditional finite envelopes; actual compilation and site proofs are pending.
const _: () = assert!(size_of::<PermissionRuntimeState>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeOperationKind>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimePhase>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeReadRole>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeCatalogFacts<'static>>() <= 256);
const _: () = assert!(size_of::<PermissionLegacyOnceWitnessFacts>() <= 256);
const _: () = assert!(size_of::<PermissionLegacyOnceWitness>() <= 1_024);
const _: () = assert!(size_of::<PermissionReadyDispatchCapture>() <= 2_048);
const _: () =
    assert!(size_of::<Result<PermissionReadyDispatchCapture, PermissionError>>() <= 2_048);
const _: () = assert!(size_of::<PermissionClaimWitness>() <= 1_024);
const _: () = assert!(size_of::<PermissionRuntimeCaptureKind>() <= 2_048);
const _: () = assert!(size_of::<Result<PermissionRuntimeCaptureKind, PermissionError>>() <= 2_048);
const _: () = assert!(size_of::<PermissionRuntimePostReadCertificate>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimePostFacts>() <= 512);
const _: () = assert!(size_of::<PermissionPreparedRuntimeMutation<'static, 'static>>() <= 4_096);
const _: () = assert!(
    size_of::<Result<PermissionPreparedRuntimeMutation<'static, 'static>, PermissionError>>()
        <= 4_096
);
const _: () =
    assert!(size_of::<PermissionPreparedRuntimeBorrowParts<'static, 'static, 'static>>() <= 256);
const _: () = assert!(size_of::<PermissionPreparedDispatchOwnedParts<'static>>() <= 1_024);
const _: () = assert!(size_of::<PermissionRuntimeNewHeader>() <= 256);
const _: () = assert!(size_of::<Option<PermissionRuntimeNewHeader>>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeGrantHeader>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeJournalHeader>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeFreshMatchStorage>() <= 1_024);
const _: () = assert!(size_of::<PermissionRuntimeClaimLink<'static>>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeSelector<'static>>() <= 256);
const _: () =
    assert!(size_of::<PermissionRuntimeReadSession<'static, 'static, 'static>>() <= 8_192);
const _: () = assert!(
    size_of::<Result<PermissionRuntimeReadSession<'static, 'static, 'static>, PermissionError>>()
        <= 8_192
);
// Tighter bounds proposed after the actual pinned layout diagnostic.
// Each actual Read Session, Result and Flow measured 5104 bytes.
const _: () =
    assert!(size_of::<PermissionRuntimeReadSession<'static, 'static, 'static>>() <= 5_120);
const _: () = assert!(
    size_of::<Result<PermissionRuntimeReadSession<'static, 'static, 'static>, PermissionError>>()
        <= 5_120
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadSession<'static, 'static, 'static>,
        >,
    >() <= 5_120
);

const _: () = assert!(
    size_of::<PermissionRuntimeWriteSession<'static, 'static, 'static, 'static>>() <= 4_096
);
const _: () = assert!(
    size_of::<
        Result<PermissionRuntimeWriteSession<'static, 'static, 'static, 'static>, PermissionError>,
    >() <= 4_096
);
const _: () = assert!(size_of::<PermissionRuntimeBeforeScalars>() <= 128);
const _: () = assert!(
    size_of::<PermissionRuntimeWork<'static, 'static, 'static, 'static, 'static>>() <= 4_096
);
const _: () = assert!(
    size_of::<
        Result<PermissionRuntimeWork<'static, 'static, 'static, 'static, 'static>, PermissionError>,
    >() <= 4_096
);
const _: () = assert!(size_of::<PermissionRuntimeBindings<'static>>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeCommit>() <= 2_048);
const _: () = assert!(size_of::<Result<PermissionRuntimeCommit, PermissionError>>() <= 2_048);
const _: () = assert!(size_of::<PermissionDispatchClaim<'static>>() <= 4_096);
const _: () =
    assert!(size_of::<Result<PermissionDispatchClaim<'static>, PermissionError>>() <= 4_096);
const _: () = assert!(size_of::<PermissionRuntimeRenewalProof>() <= 128);
const _: () = assert!(size_of::<PermissionDispatchPermit<'static, 'static>>() <= 256);
const _: () = assert!(
    size_of::<Result<PermissionDispatchPermit<'static, 'static>, PermissionError>>() <= 512
);

const _: () = assert!(size_of::<PermissionRuntimeOrigin>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeMeasure>() <= 24);
const _: () = assert!(size_of::<PermissionRuntimeResourceFacts>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeWitnessFacts>() <= 256);
const _: () = assert!(size_of::<PermissionImmutableDispatchWitness>() <= 4_096);
const _: () = assert!(size_of::<PermissionRuntimeObservation>() <= 2_048);
const _: () = assert!(size_of::<Result<PermissionRuntimeObservation, PermissionError>>() <= 2_048);
const _: () = assert!(size_of::<PermissionRuntimeCapture>() <= 2_048);
const _: () = assert!(size_of::<Result<PermissionRuntimeCapture, PermissionError>>() <= 2_048);

// New bootstrap/closed-C2/presence envelopes require their own actual width gate.
const _: () = assert!(size_of::<PermissionRuntimeOriginOwner>() <= 128);
const _: () = assert!(size_of::<Result<PermissionRuntimeOriginOwner, PermissionError>>() <= 256);
const _: () = assert!(size_of::<Option<PermissionRuntimeState>>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeFirstResourceRequest<'static>>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeMatchedReadRole>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeMatchedReadRequest<'static>>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeReadMode<'static>>() <= 256);
const _: () =
    assert!(size_of::<Result<PermissionRuntimeSelector<'static>, PermissionError>>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeLegacyClaimedMatch>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeClaimedWitness>() <= 256);
const _: () = assert!(size_of::<Option<PermissionRuntimeResourceFacts>>() <= 256);
const _: () = assert!(size_of::<Option<PermissionRuntimeWitnessFacts>>() <= 256);
const _: () = assert!(size_of::<Option<PermissionLegacyOnceWitnessFacts>>() <= 256);
const _: () = assert!(size_of::<Option<PermissionRuntimeMeasure>>() <= 128);
const _: () = assert!(size_of::<PermissionEvidenceBudget>() <= 256);

// Closed backend controls carry data only; the Session admits each actual action.
#[derive(Clone, Copy)]
pub enum PermissionRuntimeJournalPoint {
    Park,
    Resume,
    Dispatch,
}
#[derive(Clone, Copy)]
pub enum PermissionRuntimeReadSql {
    NewHeaders(super::PermissionNewGroup),
    AcceptancePayload,
    DecisionPayload,
    GrantHeaders,
    GrantPayload,
    OldAcceptanceHeaders,
    OldAcceptancePayload,
    JournalHeader(PermissionRuntimeJournalPoint),
    JournalPayload(PermissionRuntimeJournalPoint),
    CommitRead,
}
#[derive(Clone, Copy)]
pub enum PermissionRuntimeWriteSql {
    BeginImmediate,
    SnapshotCas,
    ThreadCas,
    InsertDispatchJournal,
    Commit,
}
// These added controls and the actual bindings Result need Root's actual width gate.
const _: () = assert!(size_of::<PermissionRuntimeJournalPoint>() <= 16);
const _: () = assert!(size_of::<Result<PermissionRuntimeJournalPoint, PermissionError>>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeReadSql>() <= 16);
const _: () = assert!(size_of::<Result<PermissionRuntimeReadSql, PermissionError>>() <= 256);
const _: () = assert!(size_of::<PermissionRuntimeWriteSql>() <= 16);
const _: () = assert!(size_of::<Result<PermissionRuntimeWriteSql, PermissionError>>() <= 256);
const _: () =
    assert!(size_of::<Result<PermissionRuntimeBindings<'static>, PermissionError>>() <= 256);

// Whole admission and temporary actual-fact carriers require a new width gate.
const _: () = assert!(size_of::<super::validation::RuntimeDispatchAdmission<'static>>() <= 32);
const _: () = assert!(
    size_of::<Result<super::validation::RuntimeDispatchAdmission<'static>, PermissionError>>()
        <= 128
);
const _: () = assert!(
    size_of::<
        Option<(
            StoredExecution,
            [Option<PermissionRuntimeMeasure>; 5],
            [usize; 4]
        )>,
    >() <= 2_048
);
const _: () =
    assert!(size_of::<Option<(PermissionAcceptance, [PermissionRuntimeMeasure; 3])>>() <= 2_048);
const _: () =
    assert!(size_of::<Option<(PermissionDecision, [PermissionRuntimeMeasure; 2])>>() <= 1_024);
const _: () = assert!(size_of::<Option<(GrantRecord, [PermissionRuntimeMeasure; 2])>>() <= 1_024);
const _: () = assert!(size_of::<(ApprovalAcceptance, PermissionRuntimeMeasure)>() <= 1_024);
const _: () = assert!(size_of::<(JournalEntry, PermissionRuntimeMeasure)>() <= 256);

// Pure facts retain only measurements of the actual returned Snapshot.
const _: () = assert!(size_of::<PermissionRuntimeCandidatePostFacts>() <= 128);

// Retain only actual guarded physical digest/key facts, never duplicate query IDs.
const _: () = assert!(size_of::<Option<[u8; 64]>>() <= 80);
const _: () = assert!(size_of::<PermissionRuntimeGrantHeader>() <= 128);

// Borrowed/scalar observations of stable actual Work bindings only.
impl<'binding> PermissionRuntimeBindings<'binding> {
    pub fn execution_id(&self) -> &str {
        self.execution_id
    }
    pub fn thread_id(&self) -> &str {
        self.thread_id
    }
    pub fn generation(&self) -> &str {
        self.generation
    }
    pub fn before_revision(&self) -> u64 {
        self.before_revision
    }
    pub fn before_resource_revision(&self) -> u64 {
        self.before_resource_revision
    }
    pub fn owner_generation(&self) -> u64 {
        self.owner_generation
    }
    pub fn next_revision(&self) -> u64 {
        self.next_revision
    }
    pub fn next_resource_revision(&self) -> u64 {
        self.next_resource_revision
    }
    pub fn updated_at_ms(&self) -> u64 {
        self.updated_at_ms
    }
    pub fn snapshot(&self) -> &str {
        self.snapshot
    }
    pub fn journal(&self) -> &str {
        self.journal
    }
}

// Same literal bounded-Once predicate as ApprovalService; unused self omitted.
use crate::approval::{valid_binding, validate_acceptance_record, ApprovalContext, ApprovalError};
use b4_contract::{ApprovalDecision, TaskId};
fn accepted_once(
    view: &ApprovalView,
    context: &ApprovalContext,
    task_id: &TaskId,
) -> Result<(), ApprovalError> {
    let task = view
        .execution
        .snapshot
        .tasks
        .iter()
        .find(|task| &task.spec.id == task_id)
        .ok_or(b4_kernel::KernelError::UnknownTask)?;
    if !task.spec.requires_approval {
        return Ok(());
    }
    if !task.approved {
        return Err(ApprovalError::InvalidGrant);
    }
    let interrupt = task.interrupt.as_ref().ok_or(ApprovalError::InvalidGrant)?;
    let grant = view
        .grants
        .iter()
        .find(|grant| grant.task_id == *task_id && grant.interrupt_id == interrupt.id)
        .ok_or(ApprovalError::GrantUnavailable)?;
    valid_binding(view, grant, task, &context.policy_revision)?;
    if !grant.consumed {
        return Err(ApprovalError::InvalidGrant);
    }
    let current = &view.execution.snapshot;
    let matches = view.acceptances.iter().any(|acceptance| {
        validate_acceptance_record(acceptance).is_ok()
            && acceptance.execution_id == current.execution_id
            && acceptance.thread_id == view.thread.id
            && acceptance.thread_generation == view.thread.generation
            && acceptance.context == *context
            && acceptance.definition_revision == current.definition.revision
            && acceptance.previous_revision >= grant.task_parking_revision
            && acceptance.previous_revision < current.revision
            && current.turn_history.contains(&acceptance.turn_id)
            && acceptance.decisions.iter().any(|entry| {
                entry.task_id == *task_id
                    && entry.interrupt_id == grant.interrupt_id
                    && entry.decision == ApprovalDecision::Once
                    && entry.proposal_digest == grant.proposal_digest
                    && entry.task_parking_revision == grant.task_parking_revision
            })
    });
    if !matches {
        return Err(ApprovalError::InvalidGrant);
    }
    Ok(())
}
// Caller prepays G_READY_HISTORY core and the separately named origin comparison.
// These are borrowed historical relations; the current catalog gate is separate.
fn runtime_bounded(q: PermissionRuntimeMeasure, bytes: usize, nodes: usize, depth: usize) -> bool {
    q.encoded_bytes() <= bytes && q.structural_nodes() <= nodes && q.maximum_depth() <= depth
}
struct RuntimeHistoryWork {
    string_pairs: usize,
}
impl RuntimeHistoryWork {
    fn same(&mut self, left: &str, right: &str) -> Result<bool, PermissionError> {
        if self.string_pairs == 64 || left.len() > 256 || right.len() > 256 {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.string_pairs += 1;
        Ok(left == right)
    }
    fn origin(
        &mut self,
        left: &crate::ThreadOrigin,
        right: &crate::ThreadOrigin,
    ) -> Result<bool, PermissionError> {
        Ok(self.same(&left.application, &right.application)?
            && self.same(&left.tenant, &right.tenant)?
            && self.same(&left.principal, &right.principal)?)
    }
}
fn runtime_ready_history(
    observation: &PermissionRuntimeObservation,
    witness: &PermissionImmutableDispatchWitness,
    profile: PermissionRuntimeInputProfile,
) -> Result<(), PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    let q = &witness.facts.actual;
    if !(if matches!(
        profile,
        PermissionRuntimeInputProfile::NormalReadyCancellation
            | PermissionRuntimeInputProfile::NormalOriginalAlways
    ) {
        profile.ready_snapshot(observation.facts.actual[0])
    } else {
        runtime_bounded(observation.facts.actual[0], 960, 96, 127)
    }) || !profile.human_history_acceptance(q[0])
        || !profile.human_history_before(q[1])
        || !profile.cancellation_history(q[2], (670, 55, 64), 2_070)
        || !profile.cancellation_history(q[3], (998, 67, 64), 4_096)
        || !profile.cancellation_history(q[4], (670, 55, 64), 2_070)
        || !profile.cancellation_history(q[5], (962, 83, 65), 4_096)
        || !runtime_bounded(q[6], 128, 16, 64)
        || !profile.cancellation_history(q[7], (160, 11, 1), 1_024)
        || !profile.cancellation_history(q[8], (160, 11, 1), 1_024)
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    if !Arc::ptr_eq(&observation.origin, &witness.origin) {
        return Err(PermissionError::CorruptRecord);
    }
    let a = &witness.acceptance;
    let d = &witness.decision;
    let g = &witness.grant;
    let before = a.before_snapshot();
    let next = &observation.resource.execution.snapshot;
    if before.tasks.len() != 1
        || next.tasks.len() != 1
        || a.decisions().len() != 1
        || before.definition.batches.len() != 1
        || before.definition.batches[0].len() != 1
        || before.turn_history.len() > 2
        || !before.attempt_history.is_empty()
        || before.status != E::WaitingForApproval
        || before.failure.is_some()
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let task = &before.tasks[0];
    let entry = &a.decisions()[0];
    let pending = task
        .interrupt
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let binding = entry.binding().ok_or(PermissionError::CorruptRecord)?;
    let pid = entry
        .permission_id()
        .ok_or(PermissionError::CorruptRecord)?;
    let origin = observation
        .resource
        .thread
        .origin
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let active = observation
        .resource
        .thread
        .active_execution_id
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let mut work = RuntimeHistoryWork { string_pairs: 0 };
    if entry.decision() != ApprovalDecision::Always
        || task.status != T::WaitingForApproval
        || !task.spec.requires_approval
        || task.approved
        || task.attempt_id.is_some()
        || task.result.is_some()
        || task.failure.is_some()
        || !g.consumed
        || !work.same(
            a.thread_id().as_str(),
            observation.resource.thread.id.as_str(),
        )?
        || !work.same(
            a.thread_generation().as_str(),
            observation.resource.thread.generation.as_str(),
        )?
        || !work.origin(a.origin(), origin)?
        || !work.same(a.application().application(), origin.application.as_str())?
        || !work.same(
            observation.resource.execution.thread_id.as_str(),
            observation.resource.thread.id.as_str(),
        )?
        || !work.same(active.as_str(), before.execution_id.as_str())?
        || !work.same(
            d.creator_execution_id().as_str(),
            before.execution_id.as_str(),
        )?
        || !work.same(d.creator_turn_id().as_str(), a.turn_id().as_str())?
        || !work.same(d.creator_task_id().as_str(), entry.task_id().as_str())?
        || !work.same(
            d.creator_interrupt_id().as_str(),
            entry.interrupt_id().as_str(),
        )?
        || !work.same(d.permission_id().as_str(), pid.as_str())?
    {
        return Err(PermissionError::CorruptRecord);
    }
    if !work.same(g.thread_id.as_str(), a.thread_id().as_str())?
        || !work.same(g.thread_generation.as_str(), a.thread_generation().as_str())?
        || !work.origin(&g.origin, a.origin())?
        || !work.same(g.execution_id.as_str(), before.execution_id.as_str())?
        || !work.same(g.route.as_str(), before.definition.route.as_str())?
        || !work.same(
            g.definition_revision.as_str(),
            before.definition.revision.as_str(),
        )?
        || !work.same(g.task_id.as_str(), task.spec.id.as_str())?
        || !work.same(g.effect_id.as_str(), task.spec.effect_id.as_str())?
        || !work.same(g.interrupt_id.as_str(), pending.id.as_str())?
        || !work.same(g.policy_revision.as_str(), a.policy_revision())?
        || !work.same(g.proposal_digest.as_str(), entry.proposal_digest())?
        || !work.same(
            entry.grant_ref().execution_id().as_str(),
            g.execution_id.as_str(),
        )?
        || !work.same(
            entry.grant_ref().interrupt_id().as_str(),
            g.interrupt_id.as_str(),
        )?
        || !work.same(entry.task_id().as_str(), g.task_id.as_str())?
        || !work.same(entry.interrupt_id().as_str(), g.interrupt_id.as_str())?
        || !work.same(binding.effect_id().as_str(), task.spec.effect_id.as_str())?
        || g.prompt != pending.prompt
        || entry.task_parking_revision() != g.task_parking_revision
        || g.task_parking_revision != before.revision
        || g.issued_at_ms != before.updated_at_ms
        || g.issued_at_ms > a.facts().now_ms
        || a.facts().now_ms >= g.expires_at_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    let scope = d.scope();
    if scope.thread_id() != a.thread_id()
        || scope.thread_generation() != a.thread_generation()
        || scope.origin() != a.origin()
        || scope.application() != a.application()
        || scope.route() != &before.definition.route
        || scope.definition() != &before.definition
        || scope.effect_id() != binding.effect_id()
        || scope.tool_id() != binding.tool_id()
        || scope.tool_revision() != binding.tool_revision()
        || scope.policy_revision() != a.policy_revision()
        || scope.constraint_revision() != binding.constraint_revision()
        || !runtime_same_resume(before, a, next, &mut work)?
        || !runtime_same_journal(&witness.park, before, &mut work)?
        || !runtime_same_journal(&witness.resume, next, &mut work)?
    {
        return Err(PermissionError::CorruptRecord);
    }
    // The caller has completed the identity-only A/D/G ranges, checked their
    // exact singleton counts, all physical headers and each actual payload.
    // The selected creator/reference collections therefore name these actual
    // records. This borrowed relation adds no separate collection authority.
    Ok(())
}
fn runtime_same_resume(
    before: &b4_contract::Snapshot,
    acceptance: &PermissionAcceptance,
    next: &b4_contract::Snapshot,
    work: &mut RuntimeHistoryWork,
) -> Result<bool, PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    let task = &before.tasks[0];
    let next_task = &next.tasks[0];
    let revision = before
        .revision
        .checked_add(1)
        .ok_or(PermissionError::CorruptRecord)?;
    let history = before.turn_history.len();
    if next.schema_version != before.schema_version
        || next.revision != revision
        || acceptance.committed_revision() != revision
        || !work.same(next.execution_id.as_str(), before.execution_id.as_str())?
        || !work.same(next.turn_id.as_str(), acceptance.turn_id().as_str())?
        || next.owner_generation != before.owner_generation
        || next.definition != before.definition
        || next.status != E::Running
        || next.batch_index != before.batch_index
        || next.values != before.values
        || next.attempt_history != before.attempt_history
        || next.attempts_used != before.attempts_used
        || next.updated_at_ms != acceptance.facts().now_ms
        || next.output != before.output
        || next.failure != before.failure
        || next.turn_history.len() != history + 1
        || next.turn_history[..history] != before.turn_history[..]
        || next.turn_history.last() != Some(acceptance.turn_id())
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
    Ok(true)
}
fn runtime_same_journal(
    journal: &crate::JournalEntry,
    snapshot: &b4_contract::Snapshot,
    work: &mut RuntimeHistoryWork,
) -> Result<bool, PermissionError> {
    Ok(work.same(
        journal.execution_id.as_str(),
        snapshot.execution_id.as_str(),
    )? && journal.revision == snapshot.revision
        && journal.owner_generation == snapshot.owner_generation
        && journal.status == snapshot.status
        && journal.updated_at_ms == snapshot.updated_at_ms)
}
use super::validation::{self, runtime_compat};
use super::{
    PermissionExecutionCells, PermissionResourceKey, PermissionResourceSql, PermissionThreadCells,
};

fn runtime_add(left: usize, right: usize) -> Result<usize, PermissionError> {
    left.checked_add(right)
        .ok_or(PermissionError::LimitExceeded)
}
fn runtime_mul(left: usize, right: usize) -> Result<usize, PermissionError> {
    left.checked_mul(right)
        .ok_or(PermissionError::LimitExceeded)
}
fn runtime_owned(
    raw: &str,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    // The literal legacy resource-string quote pays copy/UTF8/allocation controls.
    budget.runtime_ready_cancellation_string_failure_cleanup(raw.len())?;
    budget.runtime_reserve(runtime_add(runtime_mul(7, raw.len())?, 128)?, 1)?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(raw.len())
        .map_err(|_| PermissionError::LimitExceeded)?;
    owned.push_str(raw);
    Ok(owned)
}
fn runtime_bytes(
    raw: &[u8],
    budget: &mut PermissionEvidenceBudget,
) -> Result<Vec<u8>, PermissionError> {
    budget.runtime_reserve(runtime_add(runtime_mul(6, raw.len())?, 128)?, 1)?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(raw.len())
        .map_err(|_| PermissionError::LimitExceeded)?;
    owned.extend_from_slice(raw);
    Ok(owned)
}
fn runtime_u64(raw: &[u8]) -> Result<u64, PermissionError> {
    let raw: [u8; 8] = raw.try_into().map_err(|_| PermissionError::CorruptRecord)?;
    Ok(u64::from_be_bytes(raw))
}
fn runtime_opaque(raw: &[u8]) -> Result<&str, PermissionError> {
    if raw.is_empty() || raw.len() > 256 {
        return Err(PermissionError::CorruptRecord);
    }
    let label = std::str::from_utf8(raw).map_err(|_| PermissionError::CorruptRecord)?;
    if label.chars().any(char::is_control) {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(label)
}
fn runtime_generation(raw: &[u8]) -> Result<&str, PermissionError> {
    if raw.len() != 64
        || !raw
            .iter()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(v))
    {
        return Err(PermissionError::CorruptRecord);
    }
    std::str::from_utf8(raw).map_err(|_| PermissionError::CorruptRecord)
}
fn runtime_record_identity(raw: &[u8], prefix: &[u8]) -> bool {
    raw.len() == 69
        && raw.starts_with(prefix)
        && raw[5..]
            .iter()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(v))
}
// Normal payloads buy their actual variable owner/comparison work before
// kernel clones and handoffs. Strict's adopted fixed purchases are unchanged.
fn runtime_normal_input_owners(
    profile: PermissionRuntimeInputProfile,
    actual: PermissionRuntimeMeasure,
    copies: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    if profile != PermissionRuntimeInputProfile::Strict {
        budget.runtime_reserve(
            runtime_mul(
                copies,
                runtime_add(
                    runtime_mul(128, actual.structural_nodes())?,
                    runtime_mul(6, actual.encoded_bytes())?,
                )?,
            )?,
            runtime_mul(copies, actual.structural_nodes())?,
        )?;
    }
    Ok(())
}

fn runtime_normal_cancellation_owners(
    profile: PermissionRuntimeInputProfile,
    actual: PermissionRuntimeMeasure,
    copies: usize,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    if matches!(
        profile,
        PermissionRuntimeInputProfile::NormalReadyCancellation
            | PermissionRuntimeInputProfile::NormalOriginalAlways
    ) {
        runtime_normal_input_owners(profile, actual, copies, budget)?;
    }
    Ok(())
}

fn runtime_first_resource_facts(
    execution: &StoredExecution,
    actual: &[Option<PermissionRuntimeMeasure>; 5],
    execution_cells: [usize; 4],
    thread: &ThreadRecord,
    thread_measure: PermissionRuntimeMeasure,
    thread_cells: [usize; 4],
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionRuntimeResourceFacts, PermissionError> {
    // This is reached only AFTER real Thread and the old complete linkage check.
    // The actual helper retained these outputs only after the literal strict
    // Snapshot family had completed both Spec operands and both Input operands.
    let [root, task_spec, definition_spec, task_input, current_input] = *actual;
    let actual = [
        root.ok_or(PermissionError::UnsupportedProfile)?,
        task_spec.ok_or(PermissionError::UnsupportedProfile)?,
        definition_spec.ok_or(PermissionError::UnsupportedProfile)?,
        task_input.ok_or(PermissionError::UnsupportedProfile)?,
        current_input.ok_or(PermissionError::UnsupportedProfile)?,
        thread_measure,
    ];
    let snapshot = &execution.snapshot;
    let profile = budget.runtime_input_profile();
    if let Some(expected) = budget.ready_driver_fence {
        if profile.ready_snapshot(actual[0])
            && profile.task_spec(actual[1])
            && profile.task_spec(actual[2])
            && profile.input(actual[3])
            && profile.input(actual[4])
            && profile.thread(actual[5])
            && snapshot.status == b4_contract::ExecutionStatus::Cancelled
            && snapshot.owner_generation == expected.execution.owner_generation
            && snapshot.revision
                == expected
                    .execution
                    .revision
                    .checked_add(1)
                    .ok_or(PermissionError::LimitExceeded)?
            && thread.resource_revision
                == expected
                    .resource_revision
                    .checked_add(1)
                    .ok_or(PermissionError::LimitExceeded)?
            && snapshot.tasks.len() == 1
            && snapshot.tasks[0].status == b4_contract::TaskStatus::Cancelled
            && snapshot.tasks[0].attempt_id.is_none()
            && snapshot.attempts_used == 0
            && snapshot.attempt_history.is_empty()
            && snapshot.output.is_none()
            && snapshot.failure.is_none()
            && snapshot.tasks[0].result.is_none()
            && snapshot.tasks[0].failure.is_none()
        {
            budget.ready_preclaim_fence_refused = true;
            return Err(crate::StoreError::RevisionConflict.into());
        }
    }
    if !profile.ready_snapshot(actual[0])
        || !profile.task_spec(actual[1])
        || !profile.task_spec(actual[2])
        || !profile.input(actual[3])
        || !profile.input(actual[4])
        || !profile.thread(actual[5])
        || snapshot.definition.batches.len() != 1
        || snapshot.definition.batches[0].len() != 1
        || snapshot.tasks.len() != 1
        || snapshot.status != b4_contract::ExecutionStatus::Running
        || snapshot.batch_index != 0
        || snapshot.turn_history.len() > 2
        || !snapshot.attempt_history.is_empty()
        || snapshot.failure.is_some()
        || snapshot.output.is_some()
        || !thread.metadata.is_empty()
        || thread.origin.is_none()
        || thread.active_execution_id.is_none()
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let task = &snapshot.tasks[0];
    if task.status != b4_contract::TaskStatus::Ready
        || !task.spec.requires_approval
        || !task.approved
        || task.interrupt.is_none()
        || task.attempt_id.is_some()
        || task.result.is_some()
        || task.failure.is_some()
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    if budget.runtime_ready_cancellation_has_fixed_purchase() {
        let nodes = runtime_add(actual[0].structural_nodes(), actual[5].structural_nodes())?;
        let bytes = runtime_add(actual[0].encoded_bytes(), actual[5].encoded_bytes())?;
        let stored_id = runtime_add(128, runtime_mul(6, execution.thread_id.as_str().len())?)?;
        budget.runtime_reserve(
            runtime_add(
                runtime_add(runtime_mul(128, nodes)?, runtime_mul(6, bytes)?)?,
                stored_id,
            )?,
            runtime_add(nodes, 1)?,
        )?;
    }
    Ok(PermissionRuntimeResourceFacts {
        actual,
        raw_and_scalar: [
            execution_cells[0],
            execution_cells[1],
            execution_cells[2],
            execution_cells[3],
            thread_cells[0],
            thread_cells[1],
            thread_cells[2],
            thread_cells[3],
        ],
    })
}

impl PermissionRuntimeSelector<'_> {
    fn read_role(&self) -> Result<PermissionRuntimeReadRole, PermissionError> {
        match &self.mode {
            PermissionRuntimeReadMode::FirstResource(_) => {
                Ok(PermissionRuntimeReadRole::ObserveR1Preclaim)
            }
            PermissionRuntimeReadMode::Matched(request) => match request.role {
                PermissionRuntimeMatchedReadRole::ReadyAuthorityC1 => {
                    Ok(PermissionRuntimeReadRole::ReadyAuthorityC1)
                }
                PermissionRuntimeMatchedReadRole::ObserveR2Refence => {
                    Ok(PermissionRuntimeReadRole::ObserveR2Refence)
                }
                _ => Err(PermissionError::UnsupportedProfile),
            },
        }
    }
}
// Original closed-role512/8 and named shared-default/member2688/60 partitions.
// These literal sums are evaluated only at compile time.
const RUNTIME_READ_CLOSED_ROLE_AND_GEOMETRY_BYTES: usize = 512 + 2_688;
const RUNTIME_READ_CLOSED_ROLE_AND_GEOMETRY_NODES: usize = 8 + 60;

// Original Human W base, then retained geometry, I1 adapters, I1 helper controls,
// the pure certificate calculation and actual expected-resource references.
const RUNTIME_HUMAN_CLASSIFIED_WRITE_BYTES: usize =
    107_176 + 24_576 + 73_728 + 147_456 + 65_536 + 512;
const RUNTIME_HUMAN_CLASSIFIED_WRITE_NODES: usize = 682 + 1_024 + 4_096 + 6_400 + 8_192 + 32;

impl PermissionEvidenceBudget {
    pub fn runtime_read<'budget, 'selector, 'input>(
        &'budget mut self,
        selector: &'selector PermissionRuntimeSelector<'input>,
    ) -> Result<PermissionRuntimeReadSession<'budget, 'selector, 'input>, PermissionError>
    where
        'input: 'selector,
    {
        self.require_dispatch_resource_account()?;
        self.runtime_reserve(0, 0)?;
        // O: seven full Session owner/carrier sites and six actual Capture sites.
        // Helper-local real q/resource bundles are paid by their own helper sites.
        // Separate 128 bounded wrapper/control slots, each <=256B/16 logical nodes.
        // Paid before the first resource wrapper; helper, O and SQL fees stay separate.
        self.runtime_reserve(32_768, 2_048)?;
        self.runtime_reserve(69_632, 1_088)?;
        // Keep the original closed-role/default payer visible. The separately
        // named shared geometry-default partition precedes all new initializers.
        // Empty field retirement and the common grant/admission branches are included.
        self.runtime_reserve(
            RUNTIME_READ_CLOSED_ROLE_AND_GEOMETRY_BYTES,
            RUNTIME_READ_CLOSED_ROLE_AND_GEOMETRY_NODES,
        )?;
        let role = selector.read_role()?;
        self.runtime_reserve(64, 1)?;
        let origin = Arc::clone(selector.origin);
        let admission = self.admit_runtime_dispatch_read(selector.origin, &role)?;
        Ok(PermissionRuntimeReadSession {
            admission,
            selector,
            origin,
            role,
            metadata_checked: false,
            stage: 0,
            execution: None,
            thread: None,
            headers: [None, None],
            counts: [0; 5],
            grant_header: None,
            acceptance: None,
            decision: None,
            grant: None,
            human_geometry: [None, None, None],
            old_acceptances: Vec::new(),
            journals: Vec::new(),
            journal_header: None,
            raw: std::array::from_fn(|_| Vec::new()),
            dispatch_raw: Vec::new(),
            resource_facts: None,
            witness_facts: None,
            legacy_facts: None,
            dispatch_measure: None,
            complete_flags: [false; 8],
            completed_capture: Err(PermissionError::CorruptRecord),
        })
    }
}

// The actual immediate hook closure is consumed once; it may capture borrowed
// cells and the exclusive session loan but creates no extra data owner.
fn runtime_checked_hook<F, T, const F_BYTES: usize>(check: F) -> Result<T, PermissionError>
where
    F: FnOnce() -> Result<T, PermissionError>,
{
    const { assert!(std::mem::size_of::<F>() <= 512) };
    const { assert!(std::mem::size_of::<F>() <= F_BYTES) };
    check()
}

macro_rules! runtime_resource_methods {
    ($facts:ident) => {
        pub fn before_resource_sql(
            &mut self,
            statement: PermissionResourceSql,
            key: Option<PermissionResourceKey<'_>>,
        ) -> Result<(), PermissionError> {
            let result = runtime_checked_hook_mut::<_, _, 128>(|| {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                let expected = match statement {
                    PermissionResourceSql::BeginDeferred => 0,
                    PermissionResourceSql::Metadata => 1,
                    PermissionResourceSql::ExecutionLengths => 2,
                    PermissionResourceSql::ExecutionPayload => 3,
                    PermissionResourceSql::ThreadLengths => 4,
                    PermissionResourceSql::ThreadPayload => 5,
                    PermissionResourceSql::CommitRead => 7,
                };
                if matches!(
                    statement,
                    PermissionResourceSql::BeginDeferred | PermissionResourceSql::CommitRead
                ) {
                    self.check_read_transaction(statement)?;
                }
                let budget = self.admission.budget_mut();
                budget.runtime_check_ready_cancellation()?;
                if self.stage != expected {
                    return Err(PermissionError::CorruptRecord);
                }
                validation::runtime_phase::runtime_resource_before_sql(budget, statement, key)?;
                self.stage = if matches!(statement, PermissionResourceSql::CommitRead) {
                    90
                } else {
                    expected + 1
                };
                Ok(())
            });
            self.admission.budget_mut().runtime_finish(result)
        }
        pub fn check_metadata(
            &mut self,
            version: i64,
            checksum: &[u8],
            owner: &[u8],
            expected_owner: u64,
            schema: &[u8],
        ) -> Result<(), PermissionError> {
            let result = runtime_checked_hook_mut::<_, _, 128>(|| {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                if self.stage != 2 || self.metadata_checked {
                    return Err(PermissionError::CorruptRecord);
                }
                validation::runtime_phase::runtime_resource_check_metadata(
                    self.admission.budget_mut(),
                    version,
                    checksum,
                    owner,
                    expected_owner,
                    schema,
                )?;
                self.metadata_checked = true;
                Ok(())
            });
            self.admission.budget_mut().runtime_finish(result)
        }
        pub fn admit_execution_lengths(
            &mut self,
            thread: usize,
            snapshot: usize,
        ) -> Result<(), PermissionError> {
            let result = runtime_checked_hook_mut::<_, _, 64>(|| {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                if self.stage != 3 || !self.metadata_checked {
                    return Err(PermissionError::CorruptRecord);
                }
                validation::runtime_phase::runtime_admit_execution_lengths(
                    self.admission.budget_mut(),
                    thread,
                    snapshot,
                )
            });
            self.admission.budget_mut().runtime_finish(result)
        }
        pub fn capture_execution(
            &mut self,
            cells: PermissionExecutionCells<'_>,
        ) -> Result<(), PermissionError> {
            let result = runtime_checked_hook::<_, _, 256>(|| {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                if self.stage != 4 || !self.metadata_checked || self.execution.is_some() {
                    return Err(PermissionError::CorruptRecord);
                }
                let scalars = [
                    cells.thread_id.len(),
                    cells.snapshot.len(),
                    cells.revision.len(),
                    cells.owner_generation.len(),
                ];
                let (execution, facts) =
                    runtime_compat::capture_resource_execution(cells, self.admission.budget_mut())?;
                self.execution = Some((execution, facts.into_actual(), scalars));
                Ok(())
            });
            self.admission.budget_mut().runtime_finish(result)
        }
        pub fn thread_query_id(&mut self) -> Result<b4_contract::ThreadId, PermissionError> {
            let result = runtime_checked_hook_mut::<_, _, 64>(|| {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                if self.stage != 4 {
                    return Err(PermissionError::CorruptRecord);
                }
                let execution = self
                    .execution
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?;
                if self
                    .admission
                    .budget_mut()
                    .runtime_ready_cancellation_has_fixed_purchase()
                {
                    self.admission.budget_mut().runtime_reserve(
                        runtime_add(128, runtime_mul(6, execution.0.thread_id.as_str().len())?)?,
                        1,
                    )?;
                }
                let owned =
                    runtime_owned(execution.0.thread_id.as_str(), self.admission.budget_mut())?;
                b4_contract::ThreadId::new(owned).map_err(|_| PermissionError::CorruptRecord)
            });
            self.admission.budget_mut().runtime_finish(result)
        }
        pub fn admit_thread_lengths(
            &mut self,
            id: usize,
            active: usize,
            generation: usize,
            metadata: usize,
            origin: usize,
        ) -> Result<(), PermissionError> {
            let result = runtime_checked_hook_mut::<_, _, 64>(|| {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                if self.stage != 5 {
                    return Err(PermissionError::CorruptRecord);
                }
                validation::runtime_phase::runtime_admit_thread_lengths(
                    self.admission.budget_mut(),
                    id,
                    active,
                    generation,
                    metadata,
                    origin,
                )
            });
            self.admission.budget_mut().runtime_finish(result)
        }
        pub fn capture_thread(
            &mut self,
            cells: PermissionThreadCells<'_>,
        ) -> Result<(), PermissionError> {
            let result = runtime_checked_hook::<_, _, 256>(|| {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                if self.stage != 6 || self.thread.is_some() {
                    return Err(PermissionError::CorruptRecord);
                }
                let scalars = [
                    cells.id.as_str().len(),
                    cells.metadata.len(),
                    cells.origin.map_or(0, <[u8]>::len),
                    cells.active_execution_id.map_or(0, <[u8]>::len),
                ];
                let (thread, measured) =
                    runtime_compat::capture_resource_thread(cells, self.admission.budget_mut())?;
                let requested_execution = self.execution_id();
                let execution = self
                    .execution
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?;
                validation::runtime_phase::runtime_resource_check_linkage(
                    self.admission.budget_mut(),
                    requested_execution,
                    &thread,
                    &execution.0,
                )?;
                self.admission
                    .budget_mut()
                    .runtime_prepaid_ready_cancellation_fixed(512, 32)?; // Fixed qualification metadata is prepaid for cancellation.
                let facts = runtime_first_resource_facts(
                    &execution.0,
                    &execution.1,
                    execution.2,
                    &thread,
                    measured,
                    scalars,
                    self.admission.budget_mut(),
                )?;
                self.thread = Some((thread, measured, scalars));
                self.$facts = Some(facts);
                self.stage = 7;
                self.check_expected_resource()?;
                self.build_completed_resource()?;
                Ok(())
            });
            self.admission.budget_mut().runtime_finish(result)
        }
        pub fn thread_id(&self) -> Result<&b4_contract::ThreadId, PermissionError> {
            self.thread
                .as_ref()
                .map(|t| &t.0.id)
                .ok_or(PermissionError::CorruptRecord)
        }
    };
}
impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    pub fn execution_id(&self) -> &'input b4_contract::ExecutionId {
        match &self.selector.mode {
            PermissionRuntimeReadMode::FirstResource(request) => request.requested_execution,
            PermissionRuntimeReadMode::Matched(request) => request.requested_execution,
        }
    }
    pub fn is_sensitive(&self) -> bool {
        matches!(self.role, PermissionRuntimeReadRole::ReadyAuthorityC1)
    }
    runtime_resource_methods!(resource_facts);
}
impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    pub fn execution_id(&self) -> &'capture b4_contract::ExecutionId {
        self.prepared.requested_execution
    }
    runtime_resource_methods!(checked_resource_facts);
}

fn runtime_resource_equal_parts(
    actual_execution: &StoredExecution,
    actual_thread: &ThreadRecord,
    actual_facts: &PermissionRuntimeResourceFacts,
    expected_execution: &StoredExecution,
    expected_thread: &ThreadRecord,
    expected_facts: &PermissionRuntimeResourceFacts,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let bytes = runtime_add(
        runtime_add(
            actual_facts.actual[0].encoded_bytes(),
            actual_facts.actual[5].encoded_bytes(),
        )?,
        runtime_add(
            expected_facts.actual[0].encoded_bytes(),
            expected_facts.actual[5].encoded_bytes(),
        )?,
    )?;
    let nodes = runtime_add(
        runtime_add(
            actual_facts.actual[0].structural_nodes(),
            actual_facts.actual[5].structural_nodes(),
        )?,
        runtime_add(
            expected_facts.actual[0].structural_nodes(),
            expected_facts.actual[5].structural_nodes(),
        )?,
    )?;
    budget.runtime_reserve(
        runtime_add(
            bytes,
            runtime_add(
                actual_execution.thread_id.as_str().len(),
                expected_execution.thread_id.as_str().len(),
            )?,
        )?,
        runtime_add(nodes, 2)?,
    )?;
    if actual_execution != expected_execution || actual_thread != expected_thread {
        return Err(crate::StoreError::RevisionConflict.into());
    }
    Ok(())
}
impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    fn check_expected_resource(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let request = match &self.selector.mode {
                PermissionRuntimeReadMode::FirstResource(_) => return Ok(()),
                PermissionRuntimeReadMode::Matched(request) => request,
            };
            let actual_execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let actual_thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let actual_facts = self
                .resource_facts
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            runtime_resource_equal_parts(
                actual_execution,
                actual_thread,
                actual_facts,
                &request.expected_resource.execution,
                &request.expected_resource.thread,
                request.expected_facts,
                self.admission.budget_mut(),
            )
        });
        self.admission.budget_mut().runtime_finish(result)
    }
}
impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    fn check_expected_resource(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let (expected_execution, expected_thread, expected_facts) = match self.prepared.capture
            {
                PermissionReadyDispatchCapture::LegacyOnce {
                    view,
                    resource_facts,
                    ..
                } => (&view.execution, &view.thread, resource_facts),
                PermissionReadyDispatchCapture::NewHuman { observation, .. }
                | PermissionReadyDispatchCapture::NewUse { observation, .. } => {
                    // Classified C1 W prepays references; complete BOTH Eq remains below.
                    (
                        &observation.resource.execution,
                        &observation.resource.thread,
                        &observation.facts,
                    )
                }
            };
            let actual_execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let actual_thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let actual_facts = self
                .checked_resource_facts
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            runtime_resource_equal_parts(
                actual_execution,
                actual_thread,
                actual_facts,
                expected_execution,
                expected_thread,
                expected_facts,
                self.admission.budget_mut(),
            )
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
}

impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    fn build_completed_resource(&mut self) -> Result<(), PermissionError> {
        if self.is_sensitive() {
            Ok(())
        } else {
            self.build_observed_capture()
        }
    }
}
impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    fn build_completed_resource(&mut self) -> Result<(), PermissionError> {
        Ok(())
    }
}

// Dedicated resource wrapper controls: full actual carriers, no helper/O/SQL fee reuse.
const _: () = assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 256);
const _: () = assert!(std::mem::size_of::<Result<usize, PermissionError>>() <= 256);
const _: () = assert!(std::mem::size_of::<Result<b4_contract::ThreadId, PermissionError>>() <= 256);
const _: () = assert!(std::mem::size_of::<Result<&StoredExecution, PermissionError>>() <= 256);
const _: () = assert!(std::mem::size_of::<Result<&ThreadRecord, PermissionError>>() <= 256);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionRuntimeResourceFacts, PermissionError>>() <= 256);
const _: () = assert!(std::mem::size_of::<[Option<PermissionRuntimeMeasure>; 5]>() <= 256);
const _: () = assert!(std::mem::size_of::<[PermissionRuntimeMeasure; 6]>() <= 256);
const _: () = assert!(std::mem::size_of::<[usize; 8]>() <= 256);
const _: () = assert!(std::mem::size_of::<PermissionExecutionCells<'static>>() <= 256);
const _: () = assert!(std::mem::size_of::<PermissionThreadCells<'static>>() <= 256);
const _: () = assert!(std::mem::size_of::<PermissionResourceSql>() <= 256);
const _: () = assert!(std::mem::size_of::<Option<PermissionResourceKey<'static>>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<(
        &StoredExecution,
        &ThreadRecord,
        &PermissionRuntimeResourceFacts
    )>() <= 256
);
use super::{
    PermissionAcceptanceHeaderCells, PermissionGrantHeaderCells, PermissionNewGroup,
    PermissionNewHeaderCells, PermissionProjectionJournalHeaderCells,
};

fn runtime_group(group: PermissionNewGroup) -> usize {
    match group {
        PermissionNewGroup::Acceptances => 0,
        PermissionNewGroup::Decisions => 1,
        PermissionNewGroup::Uses => 2,
    }
}
// Keep the two independently priced entry schedules explicit.
#[allow(clippy::if_same_then_else)]
fn runtime_prepaid_owned(
    raw: &str,
    budget: &mut PermissionEvidenceBudget,
) -> Result<String, PermissionError> {
    if budget.runtime_ready_cancellation_has_fixed_purchase() {
        budget.runtime_reserve(runtime_add(runtime_mul(13, raw.len())?, 256)?, 2)?;
    } else {
        // Ordinary headers own a distinct observed copy and unique cleanup ticket.
        budget.runtime_reserve(runtime_add(runtime_mul(13, raw.len())?, 256)?, 2)?;
    }
    let mut owned = String::new();
    owned
        .try_reserve_exact(raw.len())
        .map_err(runtime_checked_value(|_| PermissionError::LimitExceeded))?;
    owned.push_str(raw);
    Ok(owned)
}
fn runtime_prepaid_bytes(raw: &[u8]) -> Result<Vec<u8>, PermissionError> {
    // W prepays this actual bounded raw owner before its first allocation.
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(raw.len())
        .map_err(|_| PermissionError::LimitExceeded)?;
    owned.extend_from_slice(raw);
    Ok(owned)
}
impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    pub fn begin_sensitive(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 7 || !self.is_sensitive() || self.resource_facts.is_none() {
                return Err(PermissionError::CorruptRecord);
            }
            // J_FRESH: before C1's first Park action; later C2 fresh J is capacity.
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(65_536, 512)?;
            // Closed outer/nested enums and payload cursor controls are paid
            // before the backend constructs its first sensitive statement.
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(640, 128)?;
            self.stage = 40;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            let code = self.stage / 4;
            let attempts = self.stage % 4;
            if matches!(code, 11 | 12 | 13 | 30 | 40 | 50 | 60) {
                // Includes final absent and second physical row, before Rows.next.
                self.admission
                    .budget_mut()
                    .runtime_prepaid_ready_cancellation_fixed(4_096, 32)?;
                if attempts == 3 {
                    return Err(PermissionError::CorruptRecord);
                }
                self.stage += 1;
            } else if matches!(code, 20 | 21 | 31 | 41 | 51 | 61) {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                // Both first/absence cursor controls belong to THIS statement's
                // already prepaid F scratch; final caller site proof is required.
                if attempts == 2 {
                    return Err(PermissionError::CorruptRecord);
                }
                self.stage += 1;
                if attempts == 1 && code == 31 {
                    self.stage = 30 * 4;
                }
                if attempts == 1 && code == 41 {
                    self.stage = 40 * 4;
                }
            } else {
                return Err(PermissionError::CorruptRecord);
            }
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_new_header(
        &mut self,
        cells: PermissionNewHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 256>(|| {
            let index = runtime_group(cells.group);
            if self.stage / 4 != 11 + index || self.complete_flags[index] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            if cells.record_version != 1
                || cells.record_bytes == 0
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            let mut previous = None;
            let mut committed = None;
            let mut digest = None;
            match cells.group {
                PermissionNewGroup::Acceptances => {
                    if runtime_opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != execution.snapshot.execution_id.as_str()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.digest.is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    runtime_opaque(cells.primary)?;
                    let p = runtime_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    let c = runtime_u64(
                        cells
                            .committed_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    if p.checked_add(1) != Some(c) || c > execution.snapshot.revision {
                        return Err(PermissionError::CorruptRecord);
                    }
                    previous = Some(p);
                    committed = Some(c);
                }
                PermissionNewGroup::Decisions => {
                    if cells.execution_id.is_some()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.previous_revision.is_some()
                        || cells.committed_revision.is_some()
                        || !runtime_record_identity(cells.primary, b"b4pd_")
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    let raw = cells.digest.ok_or(PermissionError::CorruptRecord)?;
                    runtime_generation(raw)?;
                    digest = Some(raw.try_into().map_err(|_| PermissionError::CorruptRecord)?);
                }
                PermissionNewGroup::Uses => {
                    if runtime_opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != execution.snapshot.execution_id.as_str()
                        || cells.digest.is_some()
                        || cells.committed_revision.is_some()
                        || !runtime_record_identity(
                            cells.secondary.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pu_",
                        )
                        || !runtime_record_identity(
                            cells.permission_id.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pd_",
                        )
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    runtime_opaque(cells.primary)?;
                    runtime_opaque(cells.turn_id.ok_or(PermissionError::CorruptRecord)?)?;
                    if runtime_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )? > execution.snapshot.revision
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
            }
            self.counts[index] = runtime_add(self.counts[index], 1)?;
            if self.counts[index] > 2 {
                return Err(PermissionError::UnsupportedProfile);
            }
            if index < 2 && self.counts[index] == 1 {
                self.headers[index] = Some(PermissionRuntimeNewHeader {
                    primary: runtime_prepaid_owned(
                        runtime_opaque(cells.primary)?,
                        self.admission.budget_mut(),
                    )?,
                    digest,
                    previous,
                    committed,
                    bytes: cells.record_bytes,
                });
            }
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_new_range(&mut self, group: PermissionNewGroup) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let index = runtime_group(group);
            if self.stage / 4 != 11 + index || self.complete_flags[index] {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[index] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn is_new_human(&self) -> Result<bool, PermissionError> {
        if !self.complete_flags[..3].iter().all(|v| *v) {
            return Err(PermissionError::CorruptRecord);
        }
        match [self.counts[0], self.counts[1], self.counts[2]] {
            [1, 1, 0] => Ok(true),
            [0, 0, 0] => Ok(false),
            _ => Err(PermissionError::UnsupportedProfile),
        }
    }
    pub fn acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 20 {
            return Err(PermissionError::CorruptRecord);
        }
        let header = self.headers[0]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.primary, header.bytes))
    }
    pub fn decision_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 21 {
            return Err(PermissionError::CorruptRecord);
        }
        let header = self.headers[1]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.primary, header.bytes))
    }
    pub fn capture_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            if self.stage / 4 != 20 || !self.is_new_human()? || self.acceptance.is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            let header = self.headers[0]
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            let profile = self.admission.budget_mut().runtime_input_profile();
            let acceptance_bytes = if profile == PermissionRuntimeInputProfile::NormalOriginalAlways
            {
                16_384
            } else {
                profile.cancellation_bytes(1_766, 8_192)
            };
            if raw.len() > acceptance_bytes {
                return Err(PermissionError::UnsupportedProfile);
            }
            // Classified C1 W prepays new geometry payload and member interfaces.
            // Original codec record/q carrier and traversal charges remain separate.
            let (a, q, geometry) = runtime_compat::capture_human_acceptance_with_geometry(
                raw,
                self.admission.budget_mut(),
            )?;
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            if a.turn_id().as_str() != header.primary
                || Some(a.before_snapshot().revision) != header.previous
                || Some(a.committed_revision()) != header.committed
                || a.thread_id() != &thread.id
                || a.thread_generation() != &thread.generation
                || a.before_snapshot().execution_id != execution.snapshot.execution_id
                || !profile.human_history_acceptance(q[0])
                || !profile.human_history_before(q[1])
                || !profile.cancellation_history(q[2], (670, 55, 64), 2_070)
            {
                return Err(PermissionError::CorruptRecord);
            }
            runtime_normal_cancellation_owners(profile, q[0], 2, self.admission.budget_mut())?;
            self.raw[0] = runtime_prepaid_bytes(raw)?;
            let [geometry_slot, _, _] = &mut self.human_geometry;
            *geometry_slot = Some(geometry);
            self.acceptance = Some((a, q));
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn capture_decision_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            if self.stage / 4 != 21
                || !self.is_new_human()?
                || self.acceptance.is_none()
                || self.decision.is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
            let header = self.headers[1]
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            let profile = self.admission.budget_mut().runtime_input_profile();
            if raw.len() > profile.cancellation_bytes(998, 4_096) {
                return Err(PermissionError::UnsupportedProfile);
            }
            // Classified C1 W prepays new geometry interfaces.
            let (d, q, geometry) = runtime_compat::capture_human_decision_with_geometry(
                raw,
                self.admission.budget_mut(),
            )?;
            if d.permission_id().as_str() != header.primary
                || d.scope_digest().as_bytes()
                    != header
                        .digest
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                || !profile.cancellation_history(q[0], (998, 67, 64), 4_096)
                || !profile.cancellation_history(q[1], (670, 55, 64), 2_070)
            {
                return Err(PermissionError::CorruptRecord);
            }
            runtime_normal_cancellation_owners(profile, q[0], 2, self.admission.budget_mut())?;
            self.raw[1] = runtime_prepaid_bytes(raw)?;
            let [_, geometry_slot, _] = &mut self.human_geometry;
            *geometry_slot = Some(geometry);
            self.decision = Some((d, q));
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_grant_header(
        &mut self,
        cells: PermissionGrantHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 256>(|| {
            if self.stage / 4 != 30 || self.complete_flags[3] || self.grant_header.is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let task = &execution.snapshot.tasks[0];
            let pending = task
                .interrupt
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            let interrupt = runtime_opaque(cells.interrupt_id)?;
            let parking = runtime_u64(cells.parking_revision)?;
            if runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.task_id)? != task.spec.id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
                || interrupt != pending.id.as_str()
                || parking.checked_add(1) != Some(execution.snapshot.revision)
                || !matches!(cells.consumed, 0 | 1)
                || cells.record_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            self.counts[3] = runtime_add(self.counts[3], 1)?;
            if self.counts[3] != 1 {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.grant_header = Some(PermissionRuntimeGrantHeader {
                interrupt: runtime_prepaid_owned(interrupt, self.admission.budget_mut())?,
                parking,
                consumed: cells.consumed == 1,
                bytes: cells.record_bytes,
            });
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn grant_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 31 {
            return Err(PermissionError::CorruptRecord);
        }
        let header = self
            .grant_header
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.interrupt, header.bytes))
    }
    pub fn capture_grant_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            if self.stage / 4 != 31 || self.grant.is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            let header = self
                .grant_header
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            let human = self.is_new_human()?;
            let profile = self.admission.budget_mut().runtime_input_profile();
            let grant_bytes = if human {
                profile.cancellation_bytes(962, 4_096)
            } else {
                961
            };
            if raw.len() > grant_bytes {
                return Err(PermissionError::UnsupportedProfile);
            }
            // The earlier shared role partition prepays this common branch.
            if human {
                let (g, q, geometry) = runtime_compat::capture_human_grant_with_geometry(
                    raw,
                    self.admission.budget_mut(),
                )?;
                let execution = &self
                    .execution
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?
                    .0;
                let thread = &self
                    .thread
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?
                    .0;
                let task = &execution.snapshot.tasks[0];
                if g.execution_id != execution.snapshot.execution_id
                    || g.thread_id != thread.id
                    || g.thread_generation != thread.generation
                    || g.task_id != task.spec.id
                    || g.interrupt_id.as_str() != header.interrupt
                    || g.task_parking_revision != header.parking
                    || g.consumed != header.consumed
                    || !runtime_bounded(q[0], grant_bytes, 83, 65)
                    || !runtime_bounded(q[1], 128, 16, 64)
                {
                    return Err(PermissionError::CorruptRecord);
                }
                runtime_normal_cancellation_owners(profile, q[0], 2, self.admission.budget_mut())?;
                self.raw[2] = runtime_prepaid_bytes(raw)?;
                let [_, _, geometry_slot] = &mut self.human_geometry;
                *geometry_slot = Some(geometry);
                self.grant = Some((g, q));
                return Ok(());
            }
            let (g, q) = runtime_compat::capture_grant(raw, self.admission.budget_mut())?;
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let task = &execution.snapshot.tasks[0];
            if g.execution_id != execution.snapshot.execution_id
                || g.thread_id != thread.id
                || g.thread_generation != thread.generation
                || g.task_id != task.spec.id
                || g.interrupt_id.as_str() != header.interrupt
                || g.task_parking_revision != header.parking
                || g.consumed != header.consumed
                || !runtime_bounded(q[0], grant_bytes, 83, 65)
                || !runtime_bounded(q[1], 128, 16, 64)
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.raw[2] = runtime_prepaid_bytes(raw)?;
            self.grant = Some((g, q));
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_grant_range(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage / 4 != 30
                || self.complete_flags[3]
                || self.counts[3] != 1
                || self.grant_header.is_some()
                || self.grant.is_none()
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.complete_flags[3] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_old_acceptance_header(
        &mut self,
        cells: PermissionAcceptanceHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            if self.stage / 4 != 40 || !self.complete_flags[3] || self.complete_flags[4] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let turn = runtime_opaque(cells.turn_id)?;
            let previous = runtime_u64(cells.previous_revision)?;
            let accepted = runtime_u64(cells.accepted_at_ms)?;
            if runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
                || runtime_opaque(cells.definition_revision)?
                    != execution.snapshot.definition.revision.as_str()
                || turn != execution.snapshot.turn_id.as_str()
                || previous.checked_add(1) != Some(execution.snapshot.revision)
                || accepted != execution.snapshot.updated_at_ms
                || cells.record_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            self.counts[4] = runtime_add(self.counts[4], 1)?;
            if self.counts[4] != 1 || self.is_new_human()? {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.headers[0] = Some(PermissionRuntimeNewHeader {
                primary: runtime_prepaid_owned(turn, self.admission.budget_mut())?,
                digest: None,
                previous: Some(previous),
                committed: Some(accepted),
                bytes: cells.record_bytes,
            });
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn old_acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 41 {
            return Err(PermissionError::CorruptRecord);
        }
        let header = self.headers[0]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.primary, header.bytes))
    }
    pub fn capture_old_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            if self.stage / 4 != 41 || self.is_new_human()? || !self.old_acceptances.is_empty() {
                return Err(PermissionError::CorruptRecord);
            }
            let header = self.headers[0]
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            // Independent Once quote/profile, never an A/D geometry transplant.
            if raw.len() > 584 {
                return Err(PermissionError::UnsupportedProfile);
            }
            let (a, q) = runtime_compat::capture_old_acceptance(raw, self.admission.budget_mut())?;
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            if a.execution_id != execution.snapshot.execution_id
                || a.thread_id != thread.id
                || a.thread_generation != thread.generation
                || a.turn_id.as_str() != header.primary
                || Some(a.previous_revision) != header.previous
                || Some(a.accepted_at_ms) != header.committed
                || !runtime_bounded(q, 584, 40, 65)
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.raw[0] = runtime_prepaid_bytes(raw)?;
            self.old_acceptances
                .try_reserve_exact(1)
                .map_err(|_| PermissionError::LimitExceeded)?;
            self.old_acceptances.push((a, q));
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_old_acceptance_range(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let expected = usize::from(!self.is_new_human()?);
            if self.stage / 4 != 40
                || self.complete_flags[4]
                || self.counts[4] != expected
                || self.old_acceptances.len() != expected
                || self.headers[0].is_some()
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.complete_flags[4] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn journal_revision(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<u64, PermissionError> {
        if !self.complete_flags[4] {
            return Err(PermissionError::CorruptRecord);
        }
        let revision = self
            .execution
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .0
            .snapshot
            .revision;
        match point {
            PermissionRuntimeJournalPoint::Park => revision
                .checked_sub(1)
                .ok_or(PermissionError::CorruptRecord),
            PermissionRuntimeJournalPoint::Resume => Ok(revision),
            PermissionRuntimeJournalPoint::Dispatch => Err(PermissionError::UnsupportedProfile),
        }
    }
    pub fn admit_journal_header(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        cells: PermissionProjectionJournalHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 256>(|| {
            let (code, flag, status) = match point {
                PermissionRuntimeJournalPoint::Park => {
                    (50, 5, b4_contract::ExecutionStatus::WaitingForApproval)
                }
                PermissionRuntimeJournalPoint::Resume => {
                    (60, 6, b4_contract::ExecutionStatus::Running)
                }
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if self.stage / 4 != code || self.complete_flags[flag] {
                return Err(PermissionError::CorruptRecord);
            }
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(320, 8)?;
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let current = self.journal_header.as_ref();
            if (flag == 5 && current.is_some()) || (flag == 6 && !self.complete_flags[5]) {
                return Err(PermissionError::CorruptRecord);
            }
            let revision = runtime_u64(cells.revision)?;
            let owner = runtime_u64(cells.owner)?;
            let time = runtime_u64(cells.time)?;
            let status_raw: &[u8] = if flag == 5 {
                b"\"waiting_for_approval\""
            } else {
                b"\"running\""
            };
            if cells.sequence <= 0
                || cells.kind != b"commit"
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || revision != self.journal_revision(point)?
                || owner != execution.snapshot.owner_generation
                || cells.status != status_raw
                || cells.payload_bytes == 0
                || (flag == 6
                    && (time != execution.snapshot.updated_at_ms
                        || current.is_none_or(|old| cells.sequence <= old.sequence)))
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.payload_bytes
                > self
                    .admission
                    .budget_mut()
                    .runtime_input_profile()
                    .cancellation_bytes(160, 1_024)
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            // A duplicate target header is Corrupt; the first Resume may replace
            // the retained, already closed Park header only once.
            if flag == 6 && current.is_some_and(|old| old.revision == revision) {
                return Err(PermissionError::CorruptRecord);
            }
            self.journal_header = Some(PermissionRuntimeJournalHeader {
                sequence: cells.sequence,
                revision,
                owner,
                status,
                time,
                bytes: cells.payload_bytes,
            });
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn journal_query_key(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(i64, usize), PermissionError> {
        let expected = match point {
            PermissionRuntimeJournalPoint::Park => 51,
            PermissionRuntimeJournalPoint::Resume => 61,
            PermissionRuntimeJournalPoint::Dispatch => {
                return Err(PermissionError::UnsupportedProfile)
            }
        };
        if self.stage / 4 != expected {
            return Err(PermissionError::CorruptRecord);
        }
        let header = self
            .journal_header
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((header.sequence, header.bytes))
    }
    pub fn capture_journal_payload(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 128>(|| {
            let (code, ordinal, slot) = match point {
                PermissionRuntimeJournalPoint::Park => (51, 0, 3),
                PermissionRuntimeJournalPoint::Resume => (61, 1, 4),
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if self.stage / 4 != code || self.journals.len() != ordinal {
                return Err(PermissionError::CorruptRecord);
            }
            let header = self
                .journal_header
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            let (j, q) = runtime_compat::capture_journal(raw, self.admission.budget_mut())?;
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            if j.execution_id != execution.snapshot.execution_id
                || j.revision != header.revision
                || j.owner_generation != header.owner
                || j.status != header.status
                || j.updated_at_ms != header.time
                || !self
                    .admission
                    .budget_mut()
                    .runtime_input_profile()
                    .cancellation_history(q, (160, 11, 1), 1_024)
            {
                return Err(PermissionError::CorruptRecord);
            }
            runtime_normal_cancellation_owners(
                self.admission.budget_mut().runtime_input_profile(),
                q,
                2,
                self.admission.budget_mut(),
            )?;
            self.raw[slot] = runtime_prepaid_bytes(raw)?;
            self.journals
                .try_reserve_exact(1)
                .map_err(|_| PermissionError::LimitExceeded)?;
            self.journals.push((j, q));
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_journal_range(
        &mut self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let (code, flag, size) = match point {
                PermissionRuntimeJournalPoint::Park => (51, 5, 1),
                PermissionRuntimeJournalPoint::Resume => (61, 6, 2),
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if self.stage / 4 != code
                || self.complete_flags[flag]
                || self.journals.len() != size
                || self.journal_header.is_none()
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[flag] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_evidence(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if !self.complete_flags[..7].iter().all(|v| *v)
                || self.complete_flags[7]
                || self.journals.len() != 2
                || self.grant.is_none()
                || self.resource_facts.is_none()
            {
                return Err(PermissionError::CorruptRecord);
            }
            if self.is_new_human()? {
                if self.acceptance.is_none()
                    || self.decision.is_none()
                    || !self.old_acceptances.is_empty()
                {
                    return Err(PermissionError::CorruptRecord);
                }
            } else if self.old_acceptances.len() != 1
                || self.acceptance.is_some()
                || self.decision.is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[7] = true;
            self.build_ready_capture()?;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
}

impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    pub fn before_sql(
        &mut self,
        statement: PermissionRuntimeReadSql,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(0, 0)?;
            if !self.is_sensitive() {
                return Err(PermissionError::CorruptRecord);
            }
            let code = self.stage / 4;
            let next =
                match statement {
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Acceptances)
                        if code == 10 && !self.complete_flags[0] =>
                    {
                        11
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Decisions)
                        if code == 11 && self.complete_flags[0] && !self.complete_flags[1] =>
                    {
                        12
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Uses)
                        if code == 12 && self.complete_flags[1] && !self.complete_flags[2] =>
                    {
                        13
                    }
                    PermissionRuntimeReadSql::AcceptancePayload
                        if code == 13 && self.is_new_human()? =>
                    {
                        // W is paid BEFORE the first actual retained Human raw/owner.
                        self.admission
                            .budget_mut()
                            .runtime_prepaid_ready_cancellation_fixed(
                                RUNTIME_HUMAN_CLASSIFIED_WRITE_BYTES,
                                RUNTIME_HUMAN_CLASSIFIED_WRITE_NODES,
                            )?;
                        // Separate bounded temporary-journal Vec allocations/moves/controls.
                        self.admission
                            .budget_mut()
                            .runtime_prepaid_ready_cancellation_fixed(4_096, 128)?;
                        20
                    }
                    PermissionRuntimeReadSql::DecisionPayload
                        if code == 20 && self.acceptance.is_some() && self.stage % 4 == 2 =>
                    {
                        21
                    }
                    PermissionRuntimeReadSql::GrantHeaders
                        if self.complete_flags[2]
                            && !self.complete_flags[3]
                            && ((code == 13 && !self.is_new_human()?)
                                || (code == 21
                                    && self.decision.is_some()
                                    && self.stage % 4 == 2)) =>
                    {
                        30
                    }
                    PermissionRuntimeReadSql::GrantPayload
                        if code == 30 && self.grant_header.is_some() && self.grant.is_none() =>
                    {
                        if !self.is_new_human()? {
                            // Independent actual LegacyOnce W, never Human A/D basis.
                            self.admission
                                .budget_mut()
                                .runtime_prepaid_ready_cancellation_fixed(57_196, 483)?;
                            // Separate temporary and final Legacy collection allocations/moves/controls.
                            self.admission
                                .budget_mut()
                                .runtime_prepaid_ready_cancellation_fixed(16_384, 512)?;
                        }
                        31
                    }
                    PermissionRuntimeReadSql::OldAcceptanceHeaders
                        if code == 30 && self.complete_flags[3] && !self.complete_flags[4] =>
                    {
                        40
                    }
                    PermissionRuntimeReadSql::OldAcceptancePayload
                        if code == 40 && self.headers[0].is_some() && !self.is_new_human()? =>
                    {
                        41
                    }
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 40 && self.complete_flags[4] && !self.complete_flags[5] => 50,
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 50 && self.journal_header.is_some() => 51,
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 51 && self.complete_flags[5] && !self.complete_flags[6] => 60,
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 60 && self.journal_header.is_some() => 61,
                    PermissionRuntimeReadSql::CommitRead
                        if self.complete_flags[7] && code == 61 =>
                    {
                        self.require_completed_capture()?;
                        90
                    }
                    _ => return Err(PermissionError::CorruptRecord),
                };
            // Getter receiver loan closes before the mutable Admission loan.
            let execution_id = self.execution_id();
            let thread = if let Some((thread, _, _)) = &self.thread {
                &thread.id
            } else {
                match &self.completed_capture {
                    Ok(PermissionRuntimeCapture {
                        inner:
                            PermissionRuntimeCaptureKind::Ready(
                                PermissionReadyDispatchCapture::NewHuman { observation, .. }
                                | PermissionReadyDispatchCapture::NewUse { observation, .. },
                            ),
                    }) => &observation.resource.thread.id,
                    Ok(PermissionRuntimeCapture {
                        inner:
                            PermissionRuntimeCaptureKind::Ready(
                                PermissionReadyDispatchCapture::LegacyOnce { view, .. },
                            ),
                    }) => &view.thread.id,
                    _ => return Err(PermissionError::CorruptRecord),
                }
            };
            let turn = match statement {
                PermissionRuntimeReadSql::AcceptancePayload
                | PermissionRuntimeReadSql::OldAcceptancePayload => Some(
                    self.headers[0]
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .primary
                        .as_str(),
                ),
                _ => None,
            };
            let pid = if matches!(statement, PermissionRuntimeReadSql::DecisionPayload) {
                Some(
                    self.headers[1]
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .primary
                        .as_str(),
                )
            } else {
                None
            };
            let interrupt = if matches!(statement, PermissionRuntimeReadSql::GrantPayload) {
                Some(
                    &self
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .tasks[0]
                        .interrupt
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .id,
                )
            } else {
                None
            };
            let journal = match statement {
                PermissionRuntimeReadSql::JournalHeader(point) => {
                    let revision = self
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .revision;
                    let revision = match point {
                        PermissionRuntimeJournalPoint::Park => revision
                            .checked_sub(1)
                            .ok_or(PermissionError::CorruptRecord)?,
                        PermissionRuntimeJournalPoint::Resume => revision,
                        PermissionRuntimeJournalPoint::Dispatch => {
                            return Err(PermissionError::UnsupportedProfile)
                        }
                    };
                    Some((0, revision)) // Closed Header variant uses only the actual revision.
                }
                PermissionRuntimeReadSql::JournalPayload(_) => {
                    let header = self
                        .journal_header
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?;
                    Some((
                        u64::try_from(header.sequence)
                            .map_err(|_| PermissionError::CorruptRecord)?,
                        header.revision,
                    ))
                }
                _ => None,
            };
            validation::runtime_phase::runtime_read_before_sql(
                self.admission.budget_mut(),
                &statement,
                execution_id,
                thread,
                turn,
                pid,
                interrupt,
                journal,
            )?;
            if let Some(interrupt) = interrupt {
                validation::runtime_phase::runtime_admit_reference_grant(
                    self.admission.budget_mut(),
                    execution_id,
                    interrupt,
                )?;
            }
            self.stage = if next == 90 { 90 } else { next * 4 };
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
}

// Every actual owning capture and all of its required checks are finished
// before CommitRead SQL admission. Post-COMMIT transports only this result.
impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    fn build_observed_capture(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.is_sensitive() || self.stage != 7 || self.completed_capture.is_ok() {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = self
                .execution
                .take()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = self.thread.take().ok_or(PermissionError::CorruptRecord)?.0;
            let facts = self
                .resource_facts
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(64, 1)?;
            let origin = Arc::clone(&self.origin);
            let observation = PermissionRuntimeObservation {
                resource: ApprovalResource { thread, execution },
                facts,
                origin,
            };
            self.completed_capture = Ok(PermissionRuntimeCapture {
                inner: PermissionRuntimeCaptureKind::Observed(observation),
            });
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    fn build_ready_capture(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if !self.is_sensitive() || !self.complete_flags[7] || self.completed_capture.is_ok() {
                return Err(PermissionError::CorruptRecord);
            }
            let human = self.is_new_human()?;
            let execution = self
                .execution
                .take()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = self.thread.take().ok_or(PermissionError::CorruptRecord)?.0;
            let facts = self
                .resource_facts
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            let (grant, grant_q) = self.grant.take().ok_or(PermissionError::CorruptRecord)?;
            if self.journals.len() != 2 {
                return Err(PermissionError::CorruptRecord);
            }
            let (resume, resume_q) = self.journals.pop().ok_or(PermissionError::CorruptRecord)?;
            let (park, park_q) = self.journals.pop().ok_or(PermissionError::CorruptRecord)?;
            let ready = if human {
                // Classified C1 W prepays geometry take/field/array endpoints.
                let [a_geometry_slot, d_geometry_slot, g_geometry_slot] = &mut self.human_geometry;
                let a_geometry = match a_geometry_slot.take() {
                    Some(actual) => actual,
                    None => return Err(PermissionError::CorruptRecord),
                };
                let d_geometry = match d_geometry_slot.take() {
                    Some(actual) => actual,
                    None => return Err(PermissionError::CorruptRecord),
                };
                let g_geometry = match g_geometry_slot.take() {
                    Some(actual) => actual,
                    None => return Err(PermissionError::CorruptRecord),
                };
                let (acceptance, aq) = self
                    .acceptance
                    .take()
                    .ok_or(PermissionError::CorruptRecord)?;
                let (decision, dq) = self.decision.take().ok_or(PermissionError::CorruptRecord)?;
                if !self.old_acceptances.is_empty() {
                    return Err(PermissionError::CorruptRecord);
                }
                let actual = [
                    aq[0], aq[1], aq[2], dq[0], dq[1], grant_q[0], grant_q[1], park_q, resume_q,
                ];
                self.admission
                    .budget_mut()
                    .runtime_prepaid_ready_cancellation_fixed(128, 2)?;
                let observation_origin = Arc::clone(&self.origin);
                let witness_origin = Arc::clone(&self.origin);
                let observation = PermissionRuntimeObservation {
                    resource: ApprovalResource { thread, execution },
                    facts,
                    origin: observation_origin,
                };
                let witness = PermissionImmutableDispatchWitness {
                    acceptance,
                    decision,
                    grant,
                    park,
                    resume,
                    raw: std::mem::take(&mut self.raw),
                    raw_geometry: [a_geometry, d_geometry, g_geometry],
                    facts: PermissionRuntimeWitnessFacts {
                        actual,
                        admitted_counts_and_flags: [1, 0],
                    },
                    origin: witness_origin,
                };
                // Initial compatibility pays a DISTINCT precommit graph core.
                // The later service Scope lane retains its original full debit;
                // no cross-phase saving or whole-seven-resource fit is claimed.
                self.admission.budget_mut().runtime_reserve(65_536, 2_048)?;
                self.admission.budget_mut().runtime_reserve(64, 1)?;
                if self.admission.budget_mut().runtime_input_profile()
                    == PermissionRuntimeInputProfile::NormalReadyCancellation
                {
                    for actual in witness.facts.actual {
                        runtime_normal_input_owners(
                            PermissionRuntimeInputProfile::NormalReadyCancellation,
                            actual,
                            2,
                            self.admission.budget_mut(),
                        )?;
                    }
                }
                runtime_ready_history(
                    &observation,
                    &witness,
                    self.admission.budget_mut().runtime_input_profile(),
                )?;
                let witness = Arc::new(witness);
                self.admission.budget_mut().runtime_reserve(64, 1)?;
                let origin = Arc::clone(&self.origin);
                PermissionReadyDispatchCapture::NewHuman {
                    observation,
                    witness,
                    origin,
                }
            } else {
                if self.acceptance.is_some()
                    || self.decision.is_some()
                    || self.old_acceptances.len() != 1
                {
                    return Err(PermissionError::CorruptRecord);
                }
                let (acceptance, acceptance_q) = self
                    .old_acceptances
                    .pop()
                    .ok_or(PermissionError::CorruptRecord)?;
                let mut grants = Vec::new();
                let mut acceptances = Vec::new();
                let mut journals = Vec::new();
                // The separate Legacy collection envelope was paid at W entrance,
                // before temporary and final Vec slots, moves and growth controls. No W/O spare is used.
                grants
                    .try_reserve_exact(1)
                    .map_err(|_| PermissionError::LimitExceeded)?;
                acceptances
                    .try_reserve_exact(1)
                    .map_err(|_| PermissionError::LimitExceeded)?;
                journals
                    .try_reserve_exact(2)
                    .map_err(|_| PermissionError::LimitExceeded)?;
                grants.push(grant);
                acceptances.push(acceptance);
                journals.push(park);
                journals.push(resume);
                let [old_a, empty_d, g, j2, j3] = std::mem::take(&mut self.raw);
                if !empty_d.is_empty() {
                    return Err(PermissionError::CorruptRecord);
                }
                self.admission.budget_mut().runtime_reserve(64, 1)?;
                let origin = Arc::clone(&self.origin);
                PermissionReadyDispatchCapture::LegacyOnce {
                    view: ApprovalView {
                        thread,
                        execution,
                        grants,
                        acceptances,
                    },
                    resource_facts: facts,
                    legacy_facts: PermissionLegacyOnceWitnessFacts {
                        actual: [grant_q[0], grant_q[1], acceptance_q, park_q, resume_q],
                        admitted_counts_and_flags: [1, 2],
                    },
                    journals,
                    raw: [old_a, g, j2, j3],
                    origin,
                }
            };
            self.completed_capture = Ok(PermissionRuntimeCapture {
                inner: PermissionRuntimeCaptureKind::Ready(ready),
            });
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    fn require_completed_capture(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission.budget_mut().runtime_reserve(256, 4)?;
            if self.completed_capture.is_err() {
                return Err(PermissionError::CorruptRecord);
            }
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    fn check_read_transaction(
        &mut self,
        statement: PermissionResourceSql,
    ) -> Result<(), PermissionError> {
        if matches!(statement, PermissionResourceSql::CommitRead) {
            if self.is_sensitive() {
                return Err(PermissionError::CorruptRecord);
            }
            self.require_completed_capture()?;
        }
        Ok(())
    }
    pub fn finish_after_commit(self) -> Result<PermissionRuntimeCapture, PermissionError> {
        // No fallible work or new result construction after actual COMMIT.
        let Self {
            admission,
            completed_capture,
            ..
        } = self;
        admission.finish();
        completed_capture
    }
}
impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    fn check_read_transaction(&mut self, _: PermissionResourceSql) -> Result<(), PermissionError> {
        Err(PermissionError::CorruptRecord)
    }
}

// Exact logical slot/carrier widths for separately paid capture collections.
const _: () = assert!(size_of::<GrantRecord>() <= 1_024);
const _: () = assert!(size_of::<ApprovalAcceptance>() <= 1_024);
const _: () = assert!(size_of::<JournalEntry>() <= 256);
const _: () = assert!(size_of::<Vec<GrantRecord>>() <= 32);
const _: () = assert!(size_of::<Vec<ApprovalAcceptance>>() <= 32);
const _: () = assert!(size_of::<Vec<JournalEntry>>() <= 32);
const _: () = assert!(size_of::<std::collections::TryReserveError>() <= 128);
const _: () = assert!(size_of::<Result<(), std::collections::TryReserveError>>() <= 128);
const _: () = assert!(size_of::<Result<(), PermissionError>>() <= 256);

use super::PermissionStore;
use crate::approval::{validate_approval_context, ApprovalAuthority, ApprovalOperation};
use b4_contract::{AttemptId, ExecutionId, KernelCommand, TransitionFacts};

fn runtime_ready_parts(
    capture: &PermissionReadyDispatchCapture,
) -> (
    &ThreadRecord,
    &StoredExecution,
    &PermissionRuntimeResourceFacts,
) {
    match capture {
        PermissionReadyDispatchCapture::NewHuman { observation, .. }
        | PermissionReadyDispatchCapture::NewUse { observation, .. } => (
            &observation.resource.thread,
            &observation.resource.execution,
            &observation.facts,
        ),
        PermissionReadyDispatchCapture::LegacyOnce {
            view,
            resource_facts,
            ..
        } => (&view.thread, &view.execution, resource_facts),
    }
}
fn runtime_capture_origin(
    capture: &PermissionReadyDispatchCapture,
) -> &PermissionRuntimeOriginOwner {
    match capture {
        PermissionReadyDispatchCapture::NewHuman { origin, .. }
        | PermissionReadyDispatchCapture::NewUse { origin, .. }
        | PermissionReadyDispatchCapture::LegacyOnce { origin, .. } => origin,
    }
}
fn runtime_observed(
    capture: PermissionRuntimeCapture,
) -> Result<PermissionRuntimeObservation, PermissionError> {
    match capture.inner {
        PermissionRuntimeCaptureKind::Observed(observation) => Ok(observation),
        _ => Err(PermissionError::CorruptRecord),
    }
}
fn runtime_ready(
    capture: PermissionRuntimeCapture,
) -> Result<PermissionReadyDispatchCapture, PermissionError> {
    match capture.inner {
        PermissionRuntimeCaptureKind::Ready(ready) => Ok(ready),
        _ => Err(PermissionError::CorruptRecord),
    }
}
fn runtime_fence_parts<S: PermissionStore + ?Sized>(
    store: &S,
    thread: &ThreadRecord,
    execution: &StoredExecution,
    id: &ExecutionId,
    fence: approval::ApprovalFence,
) -> Result<(), PermissionError> {
    let current = &execution.snapshot;
    if &current.execution_id != id
        || thread.active_execution_id.as_ref() != Some(id)
        || execution.thread_id != thread.id
    {
        return Err(crate::StoreError::ExecutionNotActive.into());
    }
    if fence.execution.owner_generation != current.owner_generation
        || current.owner_generation != store.owner_generation()
    {
        return Err(crate::StoreError::OwnerConflict.into());
    }
    if fence.execution.revision != current.revision
        || fence.resource_revision != thread.resource_revision
    {
        return Err(crate::StoreError::RevisionConflict.into());
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)] // Current caller, both fences and operation facts are explicit.
pub fn prepare_permission_dispatch_claim<'catalog, S, A>(
    store: &S,
    authority: &A,
    catalog: &'catalog PermissionCatalog,
    execution_id: &ExecutionId,
    fence: approval::ApprovalFence,
    context: &ApprovalContext,
    task_id: TaskId,
    attempt_id: AttemptId,
    facts: TransitionFacts,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionDispatchClaim<'catalog>, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    prepare_permission_dispatch_claim_with_profile(
        store,
        authority,
        catalog,
        execution_id,
        fence,
        context,
        task_id,
        attempt_id,
        facts,
        budget,
        PermissionRuntimeInputProfile::Strict,
    )
}

#[allow(clippy::too_many_arguments)] // Current caller, both fences and operation facts are explicit.
pub(super) fn prepare_permission_normal_use_dispatch_claim<'catalog, S, A>(
    store: &S,
    authority: &A,
    catalog: &'catalog PermissionCatalog,
    execution_id: &ExecutionId,
    fence: approval::ApprovalFence,
    context: &ApprovalContext,
    task_id: TaskId,
    attempt_id: AttemptId,
    facts: TransitionFacts,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionDispatchClaim<'catalog>, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    prepare_permission_dispatch_claim_with_profile(
        store,
        authority,
        catalog,
        execution_id,
        fence,
        context,
        task_id,
        attempt_id,
        facts,
        budget,
        PermissionRuntimeInputProfile::NormalNewUse,
    )
}

#[allow(clippy::too_many_arguments)] // Current caller, both fences and operation facts are explicit.
pub(super) fn prepare_permission_normal_original_dispatch_claim<'catalog, S, A>(
    store: &S,
    authority: &A,
    catalog: &'catalog PermissionCatalog,
    execution_id: &ExecutionId,
    fence: approval::ApprovalFence,
    context: &ApprovalContext,
    task_id: TaskId,
    attempt_id: AttemptId,
    facts: TransitionFacts,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionDispatchClaim<'catalog>, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    prepare_permission_dispatch_claim_with_profile(
        store,
        authority,
        catalog,
        execution_id,
        fence,
        context,
        task_id,
        attempt_id,
        facts,
        budget,
        PermissionRuntimeInputProfile::NormalOriginalAlways,
    )
}

#[allow(clippy::too_many_arguments)] // Current caller, both fences and operation facts are explicit.
fn prepare_permission_dispatch_claim_with_profile<'catalog, S, A>(
    store: &S,
    authority: &A,
    catalog: &'catalog PermissionCatalog,
    execution_id: &ExecutionId,
    fence: approval::ApprovalFence,
    context: &ApprovalContext,
    task_id: TaskId,
    attempt_id: AttemptId,
    facts: TransitionFacts,
    budget: &mut PermissionEvidenceBudget,
    profile: PermissionRuntimeInputProfile,
) -> Result<PermissionDispatchClaim<'catalog>, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    let prepare = runtime_checked_hook::<_, _, 512>(|| {
        budget.runtime_reserve(0, 0)?;
        // The fresh-account guard MUST precede every P/F/ownership debit.
        let origin = budget.begin_runtime_dispatch(execution_id, context, profile)?;
        // Exact P residual: both actual5-frame pair helpers debit their own
        // 9,896/152 later; remaining P controls/context/lookups are prepaid now.
        budget.runtime_reserve(40_854, 360)?;
        // Exact F_POSTCOMMIT scope before ANY successful normal owner escapes:
        // Resource/Plan/Invocation/Context disposal, Commit and Claim moves.
        budget.runtime_reserve(309_022, 2_235)?;
        // All32 eventual strong-origin retirements are capacity prepaid here.
        budget.runtime_reserve(2_048, 32)?;
        if !store.approval_durable() {
            return Err(ApprovalError::DurabilityRequired.into());
        }
        let context_bytes = runtime_add(
            runtime_add(context.actor.application.len(), context.actor.tenant.len())?,
            runtime_add(context.actor.principal.len(), context.policy_revision.len())?,
        )?;
        // Only the fresh Dispatch origin/kind's private profile selects this lane.
        let normal_new_use = matches!(
            budget.runtime_input_profile(),
            PermissionRuntimeInputProfile::NormalNewUse
                | PermissionRuntimeInputProfile::NormalOriginalAlways
        );
        if context_bytes > if normal_new_use { 1_024 } else { 33 }
            || execution_id.as_str().len() > profile.execution_id_bytes()
            || task_id.as_str().len() > profile.task_id_bytes()
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if normal_new_use {
            // Pay actual four-label validation and origin comparisons before
            // scanning, capturing or constructing any enlarged context owner.
            budget.runtime_reserve(runtime_add(runtime_mul(13, context_bytes)?, 1_024)?, 8)?;
        }
        validate_approval_context(context)?;
        // New private selector/carrier controls precede actual construction.
        // These are explicit additive256/4 sites, not donated decoder scratch.
        budget.runtime_reserve(256, 4)?;
        let r1_selector = PermissionRuntimeSelector {
            origin: &origin,
            mode: PermissionRuntimeReadMode::FirstResource(PermissionRuntimeFirstResourceRequest {
                requested_execution: execution_id,
                requested_context: context,
            }),
        };

        let r1 = runtime_observed(store.capture_permission_runtime(&r1_selector, budget)?)?;
        #[allow(clippy::drop_non_drop)]
        // Intentionally consume the selector and end its input loans.
        drop(r1_selector);
        budget.runtime_reserve(64, 1)?;
        if !Arc::ptr_eq(&origin, &r1.origin) {
            return Err(PermissionError::CorruptRecord);
        }
        runtime_fence_parts(
            store,
            &r1.resource.thread,
            &r1.resource.execution,
            execution_id,
            fence,
        )
        .map_err(|error| {
            if matches!(
                error,
                PermissionError::Store(crate::StoreError::RevisionConflict)
            ) {
                budget.ready_preclaim_fence_refused = true;
            }
            error
        })?;
        if r1.resource.thread.origin.as_ref() != Some(&context.actor) {
            return Err(ApprovalError::Unauthorized.into());
        }

        authority
            .authorize(context, ApprovalOperation::Dispatch, &r1.resource)
            .map_err(|_| ApprovalError::Unauthorized)?;

        let native = catalog
            .current_definition(&r1.resource.execution.snapshot.definition.route)
            .ok_or(PermissionError::InvalidCatalog)?;
        let definition_q = runtime_compat::measure_definition_pair(
            &r1.resource.execution.snapshot.definition,
            native,
            budget,
        )?;
        if !profile.definition(definition_q[0])
            || !profile.definition(definition_q[1])
            || native != &r1.resource.execution.snapshot.definition
        {
            return Err(PermissionError::InvalidCatalog);
        }
        let selected_task = &r1.resource.execution.snapshot.tasks[0];
        if normal_new_use {
            // Pay the actual requested/current task comparison before equality;
            // full snapshot owner quotes below cover the enclosing TaskSpecs.
            budget.runtime_reserve(
                runtime_add(selected_task.spec.id.as_str().len(), task_id.as_str().len())?,
                2,
            )?;
        }
        if selected_task.spec.id != task_id {
            return Err(b4_kernel::KernelError::UnknownTask.into());
        }
        let identity = catalog.application_identity();
        let application_bytes = runtime_add(identity.application.len(), identity.revision.len())?;
        if application_bytes > if normal_new_use { 512 } else { 27 } {
            return Err(PermissionError::InvalidCatalog);
        }
        if normal_new_use {
            // The full native identity comparison precedes InvocationContext's
            // separate owner invoice, so pay this actual pair prospectively.
            budget.runtime_reserve(runtime_add(runtime_mul(13, application_bytes)?, 512)?, 4)?;
        }
        if identity.application != context.actor.application {
            return Err(PermissionError::InvalidCatalog);
        }
        // P's actual eight borrowed fields are checked BEFORE any clone.
        let resource_origin = r1
            .resource
            .thread
            .origin
            .as_ref()
            .ok_or(ApprovalError::Unauthorized)?;
        let context_total = r1.resource.thread.id.as_str().len()
            + r1.resource.thread.generation.as_str().len()
            + resource_origin.application.len()
            + resource_origin.tenant.len()
            + resource_origin.principal.len()
            + identity.revision.len()
            + native.revision.as_str().len()
            + context.policy_revision.len();
        if context_total
            > if matches!(
                profile,
                PermissionRuntimeInputProfile::NormalNewUse
                    | PermissionRuntimeInputProfile::NormalOriginalAlways
            ) {
                2_048
            } else {
                139
            }
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if matches!(
            profile,
            PermissionRuntimeInputProfile::NormalNewUse
                | PermissionRuntimeInputProfile::NormalOriginalAlways
        ) {
            // Actual eight string owners, comparisons and retirement, before cloning.
            budget.runtime_reserve(runtime_add(runtime_mul(13, context_total)?, 8 * 256)?, 16)?;
        }
        // One actual owned InvocationContext. P pays all eight string clones
        // before this construction; no ApprovalContext owner is fabricated.
        let invocation_context = InvocationContext {
            thread_id: r1.resource.thread.id.clone(),
            thread_generation: r1.resource.thread.generation.clone(),
            origin: resource_origin.clone(),
            application_revision: identity.revision.clone(),
            execution_revision: native.revision.clone(),
            policy_revision: context.policy_revision.clone(),
        };
        budget.runtime_reserve(256, 4)?;
        let c1_selector = PermissionRuntimeSelector {
            origin: &origin,
            mode: PermissionRuntimeReadMode::Matched(PermissionRuntimeMatchedReadRequest {
                role: PermissionRuntimeMatchedReadRole::ReadyAuthorityC1,
                requested_execution: execution_id,
                context: &invocation_context,
                definition: native,
                expected_resource: &r1.resource,
                expected_facts: &r1.facts,
                task_id: Some(&task_id),
                claimed: None,
            }),
        };

        let captured = runtime_ready(store.capture_permission_runtime(&c1_selector, budget)?)?;
        if profile == PermissionRuntimeInputProfile::NormalNewUse
            && !matches!(captured, PermissionReadyDispatchCapture::NewUse { .. })
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if profile == PermissionRuntimeInputProfile::NormalOriginalAlways
            && !matches!(captured, PermissionReadyDispatchCapture::NewHuman { .. })
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        #[allow(clippy::drop_non_drop)]
        // Intentionally consume the selector and end its input loans.
        drop(c1_selector);
        budget.runtime_reserve(64, 1)?;
        if !Arc::ptr_eq(&origin, runtime_capture_origin(&captured)) {
            return Err(PermissionError::CorruptRecord);
        }
        let (thread, execution, _) = runtime_ready_parts(&captured);
        runtime_fence_parts(store, thread, execution, execution_id, fence)?;
        let current_task = &execution.snapshot.tasks[0];
        // These independently used KD string/prefix bounds are qualified on
        // actual borrowed C1 fields before invocation clone or Kernel work.
        budget.runtime_reserve(512, 16)?;
        let turn_history_bytes: usize = execution
            .snapshot
            .turn_history
            .iter()
            .map(|turn| turn.as_str().len())
            .sum();
        if execution.snapshot.turn_id.as_str().len() > profile.turn_id_bytes()
            || current_task.spec.effect_id.as_str().len() > profile.effect_id_bytes()
            || turn_history_bytes > profile.turn_history_bytes()
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if let PermissionReadyDispatchCapture::LegacyOnce { view, .. } = &captured {
            // Actual old bounded predicate, literal semantics, original caller.
            // Third renewal call remains future; each actual call pays separately.
            budget.runtime_reserve(23_621, 671)?;
            accepted_once(view, context, &task_id)?;
        }

        let (definition, registration, actual) = catalog.runtime_unclaimed_capture(
            &invocation_context,
            &execution.snapshot.definition,
            current_task,
            budget,
        )?;
        if let PermissionReadyDispatchCapture::NewHuman {
            observation,
            witness,
            ..
        } = &captured
        {
            let lane = runtime_compat::begin_scope_history(budget)?;
            let current = runtime_compat::RuntimeCurrentScope {
                thread_id: &observation.resource.thread.id,
                thread_generation: &observation.resource.thread.generation,
                origin: observation
                    .resource
                    .thread
                    .origin
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?,
                application: catalog.application_identity(),
                route: &definition.route,
                definition,
                effect_id: &registration.effect_id,
                tool_id: &registration.tool_id,
                tool_revision: &registration.tool_revision,
                policy_revision: &context.policy_revision,
                constraint_revision: &registration.constraint_revision,
            };
            let (scope, same) = lane.measure_current_scope_and_equal(
                current,
                witness.decision.scope(),
                witness.facts.actual[4],
            )?;
            if !same || !runtime_bounded(scope, profile.cancellation_bytes(670, 2_070), 55, 6) {
                return Err(PermissionError::ConstraintDenied);
            }
        }
        if let PermissionReadyDispatchCapture::NewUse {
            observation,
            witness,
            ..
        } = &captured
        {
            budget.runtime_reserve(128 + 2 * 4096, 1)?;
            if !witness.same_origin(&origin)
                || !witness.validate_source(
                    &observation.resource,
                    &invocation_context,
                    registration,
                    catalog.application_identity(),
                    budget,
                )?
            {
                return Err(PermissionError::ConstraintDenied);
            }
        }
        budget.mark_runtime_dispatch_unclaimed(&origin)?;
        let (expected_resource, expected_facts) = match &captured {
            PermissionReadyDispatchCapture::NewHuman { observation, .. }
            | PermissionReadyDispatchCapture::NewUse { observation, .. } => {
                (&observation.resource, &observation.facts)
            }
            PermissionReadyDispatchCapture::LegacyOnce { .. } => (&r1.resource, &r1.facts),
        };
        // Legacy R2 fences the retained R1 after paid complete C1-R1 equality;
        // it does not pretend ApprovalView has an ApprovalResource field.
        budget.runtime_reserve(256, 4)?;
        let r2_selector = PermissionRuntimeSelector {
            origin: &origin,
            mode: PermissionRuntimeReadMode::Matched(PermissionRuntimeMatchedReadRequest {
                role: PermissionRuntimeMatchedReadRole::ObserveR2Refence,
                requested_execution: execution_id,
                context: &invocation_context,
                definition,
                expected_resource,
                expected_facts,
                task_id: Some(&task_id),
                claimed: None,
            }),
        };

        let r2 = runtime_observed(store.capture_permission_runtime(&r2_selector, budget)?)?;
        #[allow(clippy::drop_non_drop)]
        // Intentionally consume the selector and end its input loans.
        drop(r2_selector);
        budget.runtime_reserve(64, 1)?;
        if !Arc::ptr_eq(&origin, &r2.origin) {
            return Err(PermissionError::CorruptRecord);
        }
        runtime_fence_parts(
            store,
            &r2.resource.thread,
            &r2.resource.execution,
            execution_id,
            fence,
        )?;
        // KD_PURE residual precedes every real strict/clone/command/history/
        // invocation operation. Actual postroot/spec/input helper owns84353/114.
        budget.runtime_reserve(160_040, 948)?;
        // Separate actual TransitionFacts value/ref/result transports; no KD spare.
        budget.runtime_reserve(512, 16)?;
        runtime_normal_input_owners(profile, r2.facts.actual[0], 8, budget)?;
        if normal_new_use {
            // The command and invocation each clone the requested task ID.
            // Buy their actual copy and retirement before either owner exists.
            budget.runtime_reserve(
                runtime_mul(
                    2,
                    runtime_add(runtime_mul(13, task_id.as_str().len())?, 256)?,
                )?,
                4,
            )?;
        }

        let candidate_plan = b4_kernel::transition(
            &execution.snapshot,
            KernelCommand::Dispatch {
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
            },
            facts.clone(),
        )?;
        let actual_post =
            runtime_compat::capture_dispatch_snapshot(&candidate_plan.snapshot, budget)?;
        runtime_normal_input_owners(profile, actual_post[0], 2, budget)?;
        let invocation = crate::PreparedEffect::invocation_for(
            &candidate_plan.snapshot,
            task_id.clone(),
            attempt_id.clone(),
        )?;
        budget.runtime_reserve(28_672, 448)?; // Seven actual Prepared owner/Result sites.
        budget.runtime_reserve(128, 2)?;
        let catalog_origin = Arc::clone(&origin);
        let prepared_origin = Arc::clone(&origin);
        let prepared = PermissionPreparedRuntimeMutation {
            capture: &captured,
            requested_execution: execution_id,
            requested_context: context,
            fence,
            requested_task: task_id,
            requested_attempt: attempt_id,
            facts,
            candidate_plan,
            candidate_post_facts: PermissionRuntimeCandidatePostFacts {
                actual: actual_post,
            },
            invocation,
            context: invocation_context,
            catalog: PermissionRuntimeCatalogFacts {
                actual,
                catalog,
                definition,
                registration,
                origin: catalog_origin,
            },
            origin: prepared_origin,
        };
        budget.mark_runtime_dispatch_prepared(&origin)?;

        let commit = store.commit_permission_runtime(&prepared, budget)?;
        // The port has consumed Work and its Session and ended all backend
        // Budget/Prepared loans before returning this owned nongeneric Commit.
        let PermissionPreparedRuntimeMutation {
            invocation,
            context,
            catalog,
            ..
        } = prepared;
        let witness = match captured {
            PermissionReadyDispatchCapture::NewUse { witness, .. } => {
                PermissionClaimWitness::NewUse(witness)
            }
            PermissionReadyDispatchCapture::NewHuman { witness, .. } => {
                PermissionClaimWitness::NewHuman(witness)
            }
            PermissionReadyDispatchCapture::LegacyOnce {
                view,
                legacy_facts,
                journals,
                raw,
                origin,
                ..
            } => PermissionClaimWitness::LegacyOnce(PermissionLegacyOnceWitness {
                grants: view.grants,
                acceptances: view.acceptances,
                journals,
                raw,
                facts: legacy_facts,
                origin,
            }),
        };
        // Already paid infallible owners only after durable success.
        Ok(PermissionDispatchClaim {
            resource: commit.resource,
            facts: commit.facts,
            invocation,
            context,
            catalog,
            witness,
            origin: commit.origin,
        })
    });
    budget.runtime_finish(prepare)
}

impl PermissionEvidenceBudget {
    pub fn runtime_write<'budget, 'prepared, 'capture, 'catalog>(
        &'budget mut self,
        prepared: &'prepared PermissionPreparedRuntimeMutation<'capture, 'catalog>,
    ) -> Result<
        PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>,
        PermissionError,
    >
    where
        'capture: 'prepared,
        'catalog: 'prepared,
    {
        if self.observe_use_dispatch_account()?
            && !matches!(
                prepared.capture,
                PermissionReadyDispatchCapture::NewUse { .. }
            )
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.runtime_reserve(0, 0)?;
        // The earlier shared Read role partition prepays added common branch controls.
        self.runtime_reserve(128, 2)?;
        if !Arc::ptr_eq(&prepared.origin, runtime_capture_origin(prepared.capture))
            || !Arc::ptr_eq(&prepared.origin, &prepared.catalog.origin)
        {
            return Err(PermissionError::CorruptRecord);
        }
        if matches!(
            prepared.capture,
            PermissionReadyDispatchCapture::NewHuman { .. }
        ) {
            // Retain the actual prepared-origin preflight before the new loan.
            self.check_runtime_dispatch_prepared(&prepared.origin)?;
            // Classified C1 W prepays added I1 adapters, helpers and retained members.
            // Identical primary copy/allocation work retains each physical row's H scope.
        }
        // Separate 128 bounded wrapper/control slots, each <=256B/16 logical nodes.
        // Paid before the first resource wrapper; helper, O and SQL fees stay separate.
        self.runtime_reserve(32_768, 2_048)?;
        self.runtime_reserve(69_632, 1_088)?;
        self.runtime_reserve(1_024, 128)?; // Caller-made closed SQL controls, before construction.
        self.runtime_reserve(64, 1)?;
        let origin = Arc::clone(&prepared.origin);
        let admission = if matches!(
            prepared.capture,
            PermissionReadyDispatchCapture::NewHuman { .. }
        ) {
            self.admit_runtime_dispatch_human_first_claim(&prepared.origin)?
        } else if matches!(
            prepared.capture,
            PermissionReadyDispatchCapture::NewUse { .. }
        ) {
            self.admit_runtime_dispatch_use(&prepared.origin)?
        } else {
            self.admit_runtime_dispatch_once(&prepared.origin)?
        };
        Ok(PermissionRuntimeWriteSession {
            admission,
            prepared,
            origin,
            role: PermissionRuntimeReadRole::ReadyAuthorityC1,
            stage: 0,
            metadata_checked: false,
            current_owner: prepared.fence.execution.owner_generation,
            execution: None,
            thread: None,
            match_storage: None,
            checked_resource_facts: None,
            complete_flags: [false; 8],
            use_matched: false,
        })
    }
}
impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    pub fn before_begin_immediate(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if self.stage != 0 {
                return Err(PermissionError::CorruptRecord);
            }
            let execution_id = self.execution_id();
            let (thread, _, _) = runtime_ready_parts(self.prepared.capture);
            let statement = PermissionRuntimeWriteSql::BeginImmediate;
            validation::runtime_phase::runtime_write_before_sql(
                self.admission.budget_mut(),
                &statement,
                execution_id,
                &thread.id,
                self.prepared.fence.execution.revision,
            )?;
            self.stage = 1;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn begin_sensitive(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if self.stage != 7
                || self.checked_resource_facts.is_none()
                || self.match_storage.is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
            // Fresh scalar storage only. No decoded record, placeholder q,
            // forged ReadSession or second account admission is constructed.
            self.admission.budget_mut().runtime_reserve(1_536, 24)?;
            let mut headers = Vec::new();
            headers
                .try_reserve_exact(2)
                .map_err(|_| PermissionError::LimitExceeded)?;
            self.match_storage = Some(PermissionRuntimeFreshMatchStorage {
                headers: [None, None],
                raw: std::array::from_fn(|_| Vec::new()),
                journal_headers: headers,
                range_counts_and_flags: [0; 8],
            });
            self.stage = 40;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            let code = self.stage / 4;
            let attempts = self.stage % 4;
            if matches!(code, 11 | 12 | 13 | 30 | 40 | 50 | 60) {
                self.admission.budget_mut().runtime_reserve(4_096, 32)?;
                if attempts == 3 {
                    return Err(PermissionError::CorruptRecord);
                }
                self.stage += 1;
            } else if matches!(code, 20 | 21 | 31 | 41 | 51 | 61) {
                self.admission.budget_mut().runtime_reserve(0, 0)?;
                if attempts == 2 {
                    return Err(PermissionError::CorruptRecord);
                }
                self.stage += 1;
                if attempts == 1 && code == 31 {
                    self.stage = 30 * 4;
                }
                if attempts == 1 && code == 41 {
                    self.stage = 40 * 4;
                }
            } else {
                return Err(PermissionError::CorruptRecord);
            }
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn before_sql(
        &mut self,
        statement: PermissionRuntimeReadSql,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let code = self.stage / 4;
            let storage = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            let next =
                match statement {
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Acceptances)
                        if code == 10 && !self.complete_flags[0] =>
                    {
                        11
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Decisions)
                        if code == 11 && self.complete_flags[0] && !self.complete_flags[1] =>
                    {
                        12
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Uses)
                        if code == 12 && self.complete_flags[1] && !self.complete_flags[2] =>
                    {
                        13
                    }
                    PermissionRuntimeReadSql::AcceptancePayload
                        if code == 13
                            && self.is_new_human()?
                            && storage.headers[0].is_some()
                            && storage.range_counts_and_flags[7] & 16 == 0 =>
                    {
                        20
                    }
                    PermissionRuntimeReadSql::DecisionPayload
                        if code == 20
                            && storage.headers[1].is_some()
                            && storage.range_counts_and_flags[7] & 16 != 0
                            && storage.range_counts_and_flags[7] & 32 == 0 =>
                    {
                        21
                    }
                    PermissionRuntimeReadSql::GrantHeaders
                        if code == 21
                            && storage.range_counts_and_flags[7] & 48 == 48
                            && storage.headers.iter().all(Option::is_none) =>
                    {
                        30
                    }
                    PermissionRuntimeReadSql::GrantHeaders
                        if code == 13
                            && self.complete_flags[2]
                            && !self.complete_flags[3]
                            && !self.is_new_human()? =>
                    {
                        30
                    }
                    PermissionRuntimeReadSql::GrantPayload
                        if code == 30
                            && storage.headers[1].is_some()
                            && storage.range_counts_and_flags[7] & 1 == 0 =>
                    {
                        31
                    }
                    PermissionRuntimeReadSql::OldAcceptanceHeaders
                        if code == 30 && self.complete_flags[3] && !self.complete_flags[4] =>
                    {
                        40
                    }
                    PermissionRuntimeReadSql::OldAcceptancePayload
                        if code == 40
                            && storage.headers[0].is_some()
                            && storage.range_counts_and_flags[7] & 2 == 0 =>
                    {
                        41
                    }
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 40 && self.complete_flags[4] && !self.complete_flags[5] => 50,
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 50 && storage.journal_headers.len() == 1 => 51,
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 51 && self.complete_flags[5] && !self.complete_flags[6] => 60,
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 60 && storage.journal_headers.len() == 2 => 61,
                    _ => return Err(PermissionError::CorruptRecord),
                };
            let execution_id = self.execution_id();
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0
                .id;
            let storage = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            let turn = if matches!(
                statement,
                PermissionRuntimeReadSql::OldAcceptancePayload
                    | PermissionRuntimeReadSql::AcceptancePayload
            ) {
                Some(
                    storage.headers[0]
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .primary
                        .as_str(),
                )
            } else {
                None
            };
            let interrupt = if matches!(statement, PermissionRuntimeReadSql::GrantPayload) {
                Some(
                    &self
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .tasks[0]
                        .interrupt
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .id,
                )
            } else {
                None
            };
            let journal = match statement {
                PermissionRuntimeReadSql::JournalHeader(point) => {
                    let revision = self
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .revision;
                    Some((
                        0,
                        match point {
                            PermissionRuntimeJournalPoint::Park => revision
                                .checked_sub(1)
                                .ok_or(PermissionError::CorruptRecord)?,
                            PermissionRuntimeJournalPoint::Resume => revision,
                            PermissionRuntimeJournalPoint::Dispatch => {
                                return Err(PermissionError::UnsupportedProfile)
                            }
                        },
                    ))
                }
                PermissionRuntimeReadSql::JournalPayload(_) => {
                    let h = storage
                        .journal_headers
                        .last()
                        .ok_or(PermissionError::CorruptRecord)?;
                    Some((
                        u64::try_from(h.sequence).map_err(|_| PermissionError::CorruptRecord)?,
                        h.revision,
                    ))
                }
                _ => None,
            };
            validation::runtime_phase::runtime_read_before_sql(
                self.admission.budget_mut(),
                &statement,
                execution_id,
                thread,
                turn,
                if matches!(statement, PermissionRuntimeReadSql::DecisionPayload) {
                    Some(
                        storage.headers[1]
                            .as_ref()
                            .ok_or(PermissionError::CorruptRecord)?
                            .primary
                            .as_str(),
                    )
                } else {
                    None
                },
                interrupt,
                journal,
            )?;
            if let Some(interrupt) = interrupt {
                validation::runtime_phase::runtime_admit_reference_grant(
                    self.admission.budget_mut(),
                    execution_id,
                    interrupt,
                )?;
            }
            self.stage = next * 4;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_new_header(
        &mut self,
        cells: PermissionNewHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let index = runtime_group(cells.group);
            if self.stage / 4 != 11 + index || self.complete_flags[index] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            if cells.record_version != 1
                || cells.record_bytes == 0
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            match cells.group {
                PermissionNewGroup::Acceptances => {
                    if runtime_opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != execution.snapshot.execution_id.as_str()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.digest.is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    runtime_opaque(cells.primary)?;
                    let p = runtime_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    let c = runtime_u64(
                        cells
                            .committed_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    if p.checked_add(1) != Some(c) || c > execution.snapshot.revision {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
                PermissionNewGroup::Decisions => {
                    if cells.execution_id.is_some()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.previous_revision.is_some()
                        || cells.committed_revision.is_some()
                        || !runtime_record_identity(cells.primary, b"b4pd_")
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    let raw = cells.digest.ok_or(PermissionError::CorruptRecord)?;
                    runtime_generation(raw)?;
                }
                PermissionNewGroup::Uses => {
                    if runtime_opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != execution.snapshot.execution_id.as_str()
                        || cells.digest.is_some()
                        || cells.committed_revision.is_some()
                        || !runtime_record_identity(
                            cells.secondary.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pu_",
                        )
                        || !runtime_record_identity(
                            cells.permission_id.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pd_",
                        )
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    runtime_opaque(cells.primary)?;
                    runtime_opaque(cells.turn_id.ok_or(PermissionError::CorruptRecord)?)?;
                    if runtime_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )? > execution.snapshot.revision
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
            }
            let storage = self
                .match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?;
            storage.range_counts_and_flags[index] =
                runtime_add(storage.range_counts_and_flags[index], 1)?;
            if storage.range_counts_and_flags[index] > 2 {
                return Err(PermissionError::UnsupportedProfile);
            }
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) && index < 2
                && storage.range_counts_and_flags[index] == 1
            {
                let digest = cells
                    .digest
                    .map(|raw| {
                        <[u8; 64]>::try_from(raw).map_err(|_| PermissionError::CorruptRecord)
                    })
                    .transpose()?;
                storage.headers[index] = Some(PermissionRuntimeNewHeader {
                    primary: runtime_prepaid_owned(
                        runtime_opaque(cells.primary)?,
                        self.admission.budget_mut(),
                    )?,
                    digest,
                    previous: cells.previous_revision.map(runtime_u64).transpose()?,
                    committed: cells.committed_revision.map(runtime_u64).transpose()?,
                    bytes: cells.record_bytes,
                });
            }
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_new_range(&mut self, group: PermissionNewGroup) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let index = runtime_group(group);
            if self.stage / 4 != 11 + index || self.complete_flags[index] {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[index] = true;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn is_new_human(&self) -> Result<bool, PermissionError> {
        if !self.complete_flags[..3].iter().all(|v| *v) {
            return Err(PermissionError::CorruptRecord);
        }
        let c = &self
            .match_storage
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .range_counts_and_flags;
        if matches!(
            self.prepared.capture,
            PermissionReadyDispatchCapture::NewHuman { .. }
        ) {
            if c[..3] != [1, 1, 0] {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            return Ok(true);
        }
        if c[..3] != [0, 0, 0] {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        Ok(false)
    }
    pub fn acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.human_payload_key(0, 20)
    }
    pub fn decision_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.human_payload_key(1, 21)
    }
    fn human_payload_key(
        &self,
        slot: usize,
        code: usize,
    ) -> Result<(&str, usize), PermissionError> {
        if !matches!(
            self.prepared.capture,
            PermissionReadyDispatchCapture::NewHuman { .. }
        ) || self.stage / 4 != code
        {
            return Err(PermissionError::CorruptRecord);
        }
        let header = self
            .match_storage
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .headers[slot]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.primary, header.bytes))
    }
    pub fn capture_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.capture_human_payload(0, 20, 16, raw)
    }
    pub fn capture_decision_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.capture_human_payload(1, 21, 32, raw)
    }
    fn capture_human_payload(
        &mut self,
        slot: usize,
        code: usize,
        bit: usize,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        let prepared = self.prepared;
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let witness = match prepared.capture {
                PermissionReadyDispatchCapture::NewHuman { witness, .. } => witness.as_ref(),
                _ => return Err(PermissionError::CorruptRecord),
            };
            if self.stage / 4 != code || self.stage % 4 != 1 {
                return Err(PermissionError::CorruptRecord);
            }
            let header = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .headers[slot]
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if header.bytes != raw.len() {
                return Err(PermissionError::CorruptRecord);
            }
            let expected_primary = if slot == 0 {
                witness.acceptance.turn_id().as_str()
            } else {
                witness.decision.permission_id().as_str()
            };
            let digest_bytes = if slot == 0 { 0 } else { 128 };
            self.admission.budget_mut().runtime_reserve(
                runtime_add(
                    runtime_add(header.primary.len(), expected_primary.len())?,
                    digest_bytes,
                )?,
                if slot == 0 { 2 } else { 4 },
            )?;
            let header_matches = if slot == 0 {
                header.primary == expected_primary
                    && header.digest.is_none()
                    && header.previous == Some(witness.acceptance.before_snapshot().revision)
                    && header.committed == Some(witness.acceptance.committed_revision())
            } else {
                header.primary == expected_primary
                    && header.previous.is_none()
                    && header.committed.is_none()
                    && header.digest.as_ref().is_some_and(|actual| {
                        actual.as_slice() == witness.decision.scope_digest().as_bytes()
                    })
            };
            if !header_matches || header.bytes != witness.raw[slot].len() {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            self.admission
                .budget_mut()
                .runtime_reserve(runtime_add(witness.raw[slot].len(), raw.len())?, 2)?;
            if witness.raw[slot].as_slice() != raw {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let storage = self
                .match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?;
            storage.headers[slot] = None;
            storage.range_counts_and_flags[7] |= bit;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_grant_header(
        &mut self,
        cells: PermissionGrantHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if self.stage / 4 != 30 || self.complete_flags[3] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let task = &execution.snapshot.tasks[0];
            let pending = task
                .interrupt
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            let parking = runtime_u64(cells.parking_revision)?;
            let consumed = match cells.consumed {
                0 => false,
                1 => true,
                _ => return Err(PermissionError::CorruptRecord),
            };
            if runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.task_id)? != task.spec.id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
                || runtime_opaque(cells.interrupt_id)? != pending.id.as_str()
                || parking.checked_add(1) != Some(execution.snapshot.revision)
                || cells.record_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            let grant = if let PermissionReadyDispatchCapture::NewHuman { witness, .. } =
                self.prepared.capture
            {
                &witness.grant
            } else {
                let (view, _) = self.legacy_view()?;
                &view.grants[0]
            };
            if consumed != grant.consumed || parking != grant.task_parking_revision {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            let storage = self
                .match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?;
            storage.range_counts_and_flags[3] = runtime_add(storage.range_counts_and_flags[3], 1)?;
            if storage.range_counts_and_flags[3] != 1 || storage.headers[1].is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            // Closed Legacy-only physical G header projection uses the existing
            // generic private scalar header slots; no A/D record is fabricated.
            storage.headers[1] = Some(PermissionRuntimeNewHeader {
                primary: runtime_prepaid_owned(
                    runtime_opaque(cells.interrupt_id)?,
                    self.admission.budget_mut(),
                )?,
                digest: None,
                previous: Some(parking),
                committed: Some(u64::from(consumed)),
                bytes: cells.record_bytes,
            });
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn grant_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 31 {
            return Err(PermissionError::CorruptRecord);
        }
        let header = self
            .match_storage
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .headers[1]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.primary, header.bytes))
    }
    fn match_legacy_raw(&mut self, slot: usize, raw: &[u8]) -> Result<(), PermissionError> {
        if let PermissionReadyDispatchCapture::NewHuman { witness, .. } = self.prepared.capture {
            let mapped = match slot {
                1 => 2,
                2 => 3,
                3 => 4,
                _ => return Err(PermissionError::CorruptRecord),
            };
            let expected = &witness.raw[mapped];
            self.admission
                .budget_mut()
                .runtime_reserve(runtime_add(expected.len(), raw.len())?, 2)?;
            if expected.as_slice() != raw {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            return Ok(());
        }
        let expected = match self.prepared.capture {
            PermissionReadyDispatchCapture::LegacyOnce { raw, .. } => &raw[slot],
            _ => return Err(PermissionError::CorruptRecord),
        };
        self.admission
            .budget_mut()
            .runtime_reserve(runtime_add(expected.len(), raw.len())?, 2)?;
        if expected.as_slice() != raw {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        Ok(())
    }
    pub fn capture_grant_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if self.stage / 4 != 31 {
                return Err(PermissionError::CorruptRecord);
            }
            let header = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .headers[1]
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            self.match_legacy_raw(1, raw)?;
            let storage = self
                .match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?;
            storage.headers[1] = None;
            storage.range_counts_and_flags[7] |= 1;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_grant_range(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let s = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if self.stage / 4 != 30
                || self.complete_flags[3]
                || s.range_counts_and_flags[3] != 1
                || s.headers[1].is_some()
                || s.range_counts_and_flags[7] & 1 == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[3] = true;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_old_acceptance_header(
        &mut self,
        cells: PermissionAcceptanceHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if self.stage / 4 != 40 || self.complete_flags[4] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let turn = runtime_opaque(cells.turn_id)?;
            let previous = runtime_u64(cells.previous_revision)?;
            let accepted = runtime_u64(cells.accepted_at_ms)?;
            if runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
                || runtime_opaque(cells.definition_revision)?
                    != execution.snapshot.definition.revision.as_str()
                || turn != execution.snapshot.turn_id.as_str()
                || previous.checked_add(1) != Some(execution.snapshot.revision)
                || accepted != execution.snapshot.updated_at_ms
                || cells.record_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let storage = self
                .match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?;
            storage.range_counts_and_flags[4] = runtime_add(storage.range_counts_and_flags[4], 1)?;
            if storage.range_counts_and_flags[4] != 1 || storage.headers[0].is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            storage.headers[0] = Some(PermissionRuntimeNewHeader {
                primary: runtime_prepaid_owned(turn, self.admission.budget_mut())?,
                digest: None,
                previous: Some(previous),
                committed: Some(accepted),
                bytes: cells.record_bytes,
            });
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn old_acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 41 {
            return Err(PermissionError::CorruptRecord);
        }
        let h = self
            .match_storage
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .headers[0]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&h.primary, h.bytes))
    }
    pub fn capture_old_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if self.stage / 4 != 41 {
                return Err(PermissionError::CorruptRecord);
            }
            let h = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .headers[0]
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if h.bytes != raw.len() {
                return Err(PermissionError::CorruptRecord);
            }
            self.match_legacy_raw(0, raw)?;
            let s = self
                .match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?;
            s.headers[0] = None;
            s.range_counts_and_flags[7] |= 2;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_old_acceptance_range(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                let storage = self
                    .match_storage
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?;
                if self.stage / 4 != 40
                    || self.complete_flags[4]
                    || storage.range_counts_and_flags[4] != 0
                    || storage.headers[0].is_some()
                    || storage.range_counts_and_flags[7] & 2 != 0
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.complete_flags[4] = true;
                return Ok(());
            }
            let s = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if self.stage / 4 != 40
                || self.complete_flags[4]
                || s.range_counts_and_flags[4] != 1
                || s.headers[0].is_some()
                || s.range_counts_and_flags[7] & 2 == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[4] = true;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn journal_revision(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<u64, PermissionError> {
        if !self.complete_flags[4] {
            return Err(PermissionError::CorruptRecord);
        }
        let revision = self
            .execution
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .0
            .snapshot
            .revision;
        match point {
            PermissionRuntimeJournalPoint::Park => revision
                .checked_sub(1)
                .ok_or(PermissionError::CorruptRecord),
            PermissionRuntimeJournalPoint::Resume => Ok(revision),
            PermissionRuntimeJournalPoint::Dispatch => Err(PermissionError::UnsupportedProfile),
        }
    }
    pub fn admit_journal_header(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        cells: PermissionProjectionJournalHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            self.admission.budget_mut().runtime_reserve(320, 8)?;
            let (code, flag, status, raw_status): (_, _, _, &[u8]) = match point {
                PermissionRuntimeJournalPoint::Park => (
                    50,
                    5,
                    b4_contract::ExecutionStatus::WaitingForApproval,
                    b"\"waiting_for_approval\"",
                ),
                PermissionRuntimeJournalPoint::Resume => {
                    (60, 6, b4_contract::ExecutionStatus::Running, b"\"running\"")
                }
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if self.stage / 4 != code || self.complete_flags[flag] {
                return Err(PermissionError::CorruptRecord);
            }
            let expected_revision = self.journal_revision(point)?;
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let revision = runtime_u64(cells.revision)?;
            let owner = runtime_u64(cells.owner)?;
            let time = runtime_u64(cells.time)?;
            if cells.kind != b"commit"
                || cells.sequence <= 0
                || cells.status != raw_status
                || runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || revision != expected_revision
                || owner != execution.snapshot.owner_generation
                || cells.payload_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            let expected = match self.prepared.capture {
                PermissionReadyDispatchCapture::NewHuman { witness, .. } => {
                    if flag == 5 {
                        &witness.park
                    } else {
                        &witness.resume
                    }
                }
                PermissionReadyDispatchCapture::LegacyOnce { journals, .. } => &journals[flag - 5],
                PermissionReadyDispatchCapture::NewUse { .. } => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if expected.revision != revision
                || expected.owner_generation != owner
                || expected.status != status
                || expected.updated_at_ms != time
            {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let s = self
                .match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?;
            if s.journal_headers.len() != flag - 5
                || (flag == 6 && s.journal_headers[0].sequence >= cells.sequence)
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.payload_bytes > 160 {
                return Err(PermissionError::UnsupportedProfile);
            }
            s.journal_headers.push(PermissionRuntimeJournalHeader {
                sequence: cells.sequence,
                revision,
                owner,
                status,
                time,
                bytes: cells.payload_bytes,
            });
            s.range_counts_and_flags[flag] = 1;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn journal_query_key(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(i64, usize), PermissionError> {
        let code = match point {
            PermissionRuntimeJournalPoint::Park => 51,
            PermissionRuntimeJournalPoint::Resume => 61,
            PermissionRuntimeJournalPoint::Dispatch => {
                return Err(PermissionError::UnsupportedProfile)
            }
        };
        if self.stage / 4 != code {
            return Err(PermissionError::CorruptRecord);
        }
        let h = self
            .match_storage
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .journal_headers
            .last()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((h.sequence, h.bytes))
    }
    pub fn capture_journal_payload(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let (code, slot, bit) = match point {
                PermissionRuntimeJournalPoint::Park => (51, 2, 4),
                PermissionRuntimeJournalPoint::Resume => (61, 3, 8),
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if self.stage / 4 != code {
                return Err(PermissionError::CorruptRecord);
            }
            let h = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .journal_headers
                .last()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != h.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            self.match_legacy_raw(slot, raw)?;
            self.match_storage
                .as_mut()
                .ok_or(PermissionError::CorruptRecord)?
                .range_counts_and_flags[7] |= bit;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_journal_range(
        &mut self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            let (code, flag, bit) = match point {
                PermissionRuntimeJournalPoint::Park => (51, 5, 4),
                PermissionRuntimeJournalPoint::Resume => (61, 6, 8),
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            let s = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if self.stage / 4 != code
                || self.complete_flags[flag]
                || s.range_counts_and_flags[flag] != 1
                || s.range_counts_and_flags[7] & bit == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[flag] = true;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_evidence(&mut self) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                let storage = self
                    .match_storage
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?;
                if !self.complete_flags[..7].iter().all(|v| *v)
                    || self.complete_flags[7]
                    || storage.range_counts_and_flags[..3] != [1, 1, 0]
                    || storage.range_counts_and_flags[3..7] != [1, 0, 1, 1]
                    || storage.range_counts_and_flags[7] != 61
                    || storage.headers.iter().any(Option::is_some)
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.complete_flags[7] = true;
                return Ok(());
            }
            let s = self
                .match_storage
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if !self.complete_flags[..7].iter().all(|v| *v)
                || self.complete_flags[7]
                || s.range_counts_and_flags[..3] != [0, 0, 0]
                || s.range_counts_and_flags[3..7] != [1, 1, 1, 1]
                || s.range_counts_and_flags[7] != 15
                || s.headers.iter().any(Option::is_some)
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[7] = true;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    fn legacy_view(
        &self,
    ) -> Result<(&ApprovalView, &PermissionLegacyOnceWitnessFacts), PermissionError> {
        match self.prepared.capture {
            PermissionReadyDispatchCapture::LegacyOnce {
                view, legacy_facts, ..
            } if view.grants.len() == 1 && view.acceptances.len() == 1 => Ok((view, legacy_facts)),
            _ => Err(PermissionError::CorruptRecord),
        }
    }
    pub fn finish_after_commit(self) {
        let Self { admission, .. } = self;
        admission.finish();
    }
}

// Exact first Once Dispatch relation over actual current owners. Historical
// eligibility and current Unclaimed approval were established separately.
fn runtime_once_post_graph(
    before: &StoredExecution,
    after: &b4_contract::Snapshot,
    thread: &ThreadRecord,
    prepared: &PermissionPreparedRuntimeMutation<'_, '_>,
    journal: &JournalEntry,
) -> Result<(), PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    let current = &before.snapshot;
    if current.tasks.len() != 1
        || after.tasks.len() != 1
        || current.turn_history.len() > 2
        || !current.attempt_history.is_empty()
        || after.attempt_history.len() != 1
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let task = &current.tasks[0];
    let next = &after.tasks[0];
    if after.schema_version != current.schema_version
        || after.execution_id != current.execution_id
        || after.turn_id != current.turn_id
        || after.owner_generation != current.owner_generation
        || after.definition != current.definition
        || after.status != E::Running
        || current.status != E::Running
        || after.revision
            != current
                .revision
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?
        || after.batch_index != current.batch_index
        || after.values != current.values
        || after.turn_history != current.turn_history
        || after.attempt_history[0] != prepared.requested_attempt
        || after.attempts_used
            != current
                .attempts_used
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?
        || after.updated_at_ms != prepared.facts.now_ms
        || after.output != current.output
        || after.failure != current.failure
        || task.status != T::Ready
        || next.status != T::InFlight
        || !task.approved
        || !next.approved
        || next.spec != task.spec
        || next.input != task.input
        || next.interrupt != task.interrupt
        || task.attempt_id.is_some()
        || next.attempt_id.as_ref() != Some(&prepared.requested_attempt)
        || next.result != task.result
        || next.failure != task.failure
        || task.spec.id != prepared.requested_task
        || prepared.invocation.execution_id != after.execution_id
        || prepared.invocation.turn_id != after.turn_id
        || prepared.invocation.owner_generation != after.owner_generation
        || prepared.invocation.task_id != next.spec.id
        || prepared.invocation.effect_id != next.spec.effect_id
        || prepared.invocation.attempt_id != prepared.requested_attempt
        || prepared.invocation.input != next.input
        || thread.id != before.thread_id
        || thread.active_execution_id.as_ref() != Some(&after.execution_id)
        || prepared.context.thread_id != thread.id
        || prepared.context.thread_generation != thread.generation
        || thread.origin.as_ref() != Some(&prepared.context.origin)
        || journal.execution_id != after.execution_id
        || journal.revision != after.revision
        || journal.owner_generation != after.owner_generation
        || journal.status != after.status
        || journal.updated_at_ms != after.updated_at_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(())
}

impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    pub fn prepare_work<'work>(
        &'work mut self,
    ) -> Result<PermissionRuntimeWork<'work, 'budget, 'prepared, 'capture, 'catalog>, PermissionError>
    where
        'budget: 'work,
        'prepared: 'work,
    {
        // All fallible owner extraction and every actual post owner exist
        // before the Work takes its real short Session loan.
        let prepared = self.prepared;
        let result = runtime_checked_hook::<_, _, 512>(|| {
            self.admission.budget_mut().runtime_reserve(0, 0)?;
            if !self.complete_flags[7] || self.stage / 4 != 61 || self.stage % 4 != 2 {
                return Err(PermissionError::CorruptRecord);
            }
            if matches!(
                prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                self.admission.budget_mut().runtime_reserve(1_024, 64)?;
                let (observation, witness, origin) = match prepared.capture {
                    PermissionReadyDispatchCapture::NewHuman {
                        observation,
                        witness,
                        origin,
                    } => (observation, witness, origin),
                    _ => return Err(PermissionError::CorruptRecord),
                };
                // Genuine C1 graph/gate inputs are immutable; complete fresh I1
                // resource/header/raw matches preserve them, with no callback.
                if !Arc::ptr_eq(&prepared.origin, origin)
                    || !Arc::ptr_eq(&prepared.origin, &observation.origin)
                    || !Arc::ptr_eq(&prepared.origin, &witness.origin)
                    || witness.facts.admitted_counts_and_flags != [1, 0]
                    || !witness.grant.consumed
                {
                    return Err(PermissionError::CorruptRecord);
                }
            } else if let PermissionReadyDispatchCapture::NewUse {
                witness, origin, ..
            } = prepared.capture
            {
                self.admission
                    .budget_mut()
                    .runtime_reserve(128 + 2 * 4096, 1)?;
                if !self.use_matched
                    || !Arc::ptr_eq(&prepared.origin, origin)
                    || !witness.same_origin(&prepared.origin)
                {
                    return Err(PermissionError::CorruptRecord);
                }
            } else {
                self.admission.budget_mut().runtime_reserve(23_621, 671)?;
                let (view, _) = match prepared.capture {
                    PermissionReadyDispatchCapture::LegacyOnce {
                        view, legacy_facts, ..
                    } => (view, legacy_facts),
                    _ => return Err(PermissionError::CorruptRecord),
                };
                accepted_once(view, prepared.requested_context, &prepared.requested_task)?;
                // A separately paid full historical J2 tuple guard closes the
                // grant-issuance/Park time link, independent of current-to-Dispatch.
                self.admission.budget_mut().runtime_reserve(1_024, 64)?;
                let historical = match prepared.capture {
                    PermissionReadyDispatchCapture::LegacyOnce { journals, .. } => journals,
                    _ => return Err(PermissionError::CorruptRecord),
                };
                if historical.len() != 2 || view.grants.len() != 1 {
                    return Err(PermissionError::CorruptRecord);
                }
                let park = &historical[0];
                let resume = &historical[1];
                let grant = &view.grants[0];
                let current = &view.execution.snapshot;
                if park.execution_id != current.execution_id
                    || park.revision != grant.task_parking_revision
                    || park.owner_generation != current.owner_generation
                    || park.status != b4_contract::ExecutionStatus::WaitingForApproval
                    || park.updated_at_ms != grant.issued_at_ms
                    || resume.execution_id != current.execution_id
                    || resume.revision != current.revision
                    || resume.owner_generation != current.owner_generation
                    || resume.status != b4_contract::ExecutionStatus::Running
                    || resume.updated_at_ms != current.updated_at_ms
                    || park.revision.checked_add(1) != Some(resume.revision)
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            // Exact KD_I1 residual excludes the independently paid actual
            // returned Snapshot root/spec/input3pass helper84353/114.
            self.admission
                .budget_mut()
                .runtime_reserve(171_206, 2_074)?;
            let execution = self
                .execution
                .take()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let (mut thread, _, _) = self.thread.take().ok_or(PermissionError::CorruptRecord)?;
            let before_facts = self
                .checked_resource_facts
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            let match_storage = self
                .match_storage
                .take()
                .ok_or(PermissionError::CorruptRecord)?;
            // Separate actual TransitionFacts value/ref/result transports; no KD spare.
            self.admission.budget_mut().runtime_reserve(512, 16)?;
            runtime_normal_input_owners(
                self.admission.budget_mut().runtime_input_profile(),
                before_facts.actual[0],
                8,
                self.admission.budget_mut(),
            )?;
            let authoritative_plan = b4_kernel::transition(
                &execution.snapshot,
                KernelCommand::Dispatch {
                    task_id: prepared.requested_task.clone(),
                    attempt_id: prepared.requested_attempt.clone(),
                },
                prepared.facts.clone(),
            )?;
            let after = runtime_compat::capture_dispatch_snapshot(
                &authoritative_plan.snapshot,
                self.admission.budget_mut(),
            )?;
            // Variable full-plan comparisons and remaining owned handoffs are
            // prepaid here, before comparison and any durable write.
            runtime_normal_input_owners(
                self.admission.budget_mut().runtime_input_profile(),
                after[0],
                8,
                self.admission.budget_mut(),
            )?;
            if !crate::validate_commit(
                &execution.snapshot,
                prepared.fence.execution,
                &authoritative_plan,
                self.current_owner,
            )? {
                return Err(PermissionError::CorruptRecord);
            }
            // ONE whole equality. KD includes BOTH complete actual plans,
            // expected_revision and the full actual dispatchable collections.
            if authoritative_plan != prepared.candidate_plan {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            // E's residual precedes the actual journal owner/format/conversions.
            // Snapshot emission and full postThread5 independently debit their
            // actual passes; no old Thread updated_at_ms field is invented.
            self.admission.budget_mut().runtime_reserve(5_324, 209)?;
            let next_resource_revision = thread
                .resource_revision
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?;
            let before_scalars = PermissionRuntimeBeforeScalars {
                execution_revision: execution.snapshot.revision,
                resource_revision: thread.resource_revision,
                owner_generation: execution.snapshot.owner_generation,
                next_execution_revision: authoritative_plan.snapshot.revision,
                next_resource_revision,
                updated_at_ms: authoritative_plan.snapshot.updated_at_ms,
            };
            thread.resource_revision = next_resource_revision;
            let post_thread =
                runtime_compat::measure_post_thread(&thread, self.admission.budget_mut())?;
            if !self
                .admission
                .budget_mut()
                .runtime_input_profile()
                .thread(post_thread)
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            // Real returned Snapshot after strict checked complete BOTH Spec
            // and Input pairs; canonical recipes are identical. These are
            // copies of actual measured immutable after facts, not Ready facts.
            let post_facts = PermissionRuntimeResourceFacts {
                actual: [
                    after[0],
                    after[1],
                    after[1],
                    after[2],
                    after[2],
                    post_thread,
                ],
                raw_and_scalar: [
                    execution.thread_id.as_str().len(),
                    after[0].encoded_bytes(),
                    8,
                    8,
                    thread.id.as_str().len(),
                    before_facts.raw_and_scalar[5],
                    before_facts.raw_and_scalar[6],
                    before_facts.raw_and_scalar[7],
                ],
            };
            let snapshot_bytes = runtime_compat::emit_dispatch_snapshot(
                &authoritative_plan.snapshot,
                after[0],
                self.admission.budget_mut(),
            )?;
            let snapshot_payload =
                String::from_utf8(snapshot_bytes).map_err(|_| PermissionError::CorruptRecord)?;
            let journal = JournalEntry::from(&authoritative_plan.snapshot);
            // Exact proposed actual generated-J bridge; no produced-byte
            // decoder or numeric CanonicalMeasure constructor is called here.
            let (journal_bytes, journal_measure) =
                runtime_compat::emit_generated_journal(&journal, self.admission.budget_mut())?;
            if !self
                .admission
                .budget_mut()
                .runtime_input_profile()
                .journal(journal_measure)
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            let journal_payload =
                String::from_utf8(journal_bytes).map_err(|_| PermissionError::CorruptRecord)?;
            self.admission.budget_mut().runtime_reserve(65_536, 2_048)?;
            runtime_once_post_graph(
                &execution,
                &authoritative_plan.snapshot,
                &thread,
                prepared,
                &journal,
            )?;
            // Original Once captures contain ONLY real G/oldA/Park/Resume;
            // the branch-specific before-write certificate is an exact
            // separately reviewed calculation, never Human A/D evidence.
            self.admission.budget_mut().runtime_reserve(32_768, 512)?;
            let post_read_certificate = if self.admission.budget_mut().runtime_input_profile()
                == PermissionRuntimeInputProfile::NormalOriginalAlways
            {
                PermissionRuntimePostReadCertificate {
                    encoded_bytes: 0,
                    structural_nodes: 0,
                    normal_sql_slots: 0,
                    capacity_sql_slots: 0,
                }
            } else if matches!(
                prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                runtime_human_current_post_read_certificate(
                    &authoritative_plan.snapshot,
                    &thread,
                    &post_facts,
                    prepared.capture,
                    &journal,
                    journal_measure,
                )?
            } else if matches!(
                prepared.capture,
                PermissionReadyDispatchCapture::NewUse { .. }
            ) {
                // No renewal profile is implemented for this first-claim slice.
                // Retain an explicit zero future-read certificate; every actual
                // current/source read is paid independently before construction.
                PermissionRuntimePostReadCertificate {
                    encoded_bytes: 0,
                    structural_nodes: 0,
                    normal_sql_slots: 0,
                    capacity_sql_slots: 0,
                }
            } else {
                runtime_once_post_read_certificate(
                    &authoritative_plan.snapshot,
                    &thread,
                    &post_facts,
                    prepared.capture,
                    &journal,
                    journal_measure,
                )?
            };
            self.admission.budget_mut().runtime_reserve(24_576, 384)?; // Six actual owning Work/Result sites.
            self.admission.budget_mut().runtime_reserve(24_576, 384)?; // Six separate actual eleven-field tuple/Result sites.
            self.admission.budget_mut().runtime_reserve(64, 1)?;
            let origin = Arc::clone(&self.origin);
            Ok((
                execution,
                thread,
                authoritative_plan,
                post_facts,
                snapshot_payload,
                journal_payload,
                journal,
                before_scalars,
                match_storage,
                origin,
                post_read_certificate,
            ))
        });
        let (
            before_execution,
            post_thread,
            authoritative_plan,
            post_facts,
            snapshot_payload,
            journal_payload,
            journal,
            before_scalars,
            fresh_match_storage,
            origin,
            post_read_certificate,
        ) = self.admission.budget_mut().runtime_finish(result)?;
        Ok(PermissionRuntimeWork {
            session: self,
            before_execution,
            post_thread,
            authoritative_plan,
            post_facts,
            snapshot_payload,
            journal_payload,
            journal,
            before_scalars,
            sql_stage: 0,
            fresh_match_storage,
            origin,
            post_read_certificate,
        })
    }
}
impl<'work, 'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWork<'work, 'budget, 'prepared, 'capture, 'catalog>
where
    'budget: 'work,
    'prepared: 'work,
    'capture: 'prepared,
    'catalog: 'prepared,
{
    pub fn before_write_sql(
        &mut self,
        statement: PermissionRuntimeWriteSql,
    ) -> Result<PermissionRuntimeBindings<'_>, PermissionError> {
        let result = (|| {
            self.session.admission.budget_mut().runtime_reserve(0, 0)?;
            let expected = match statement {
                PermissionRuntimeWriteSql::SnapshotCas => 0,
                PermissionRuntimeWriteSql::ThreadCas => 2,
                PermissionRuntimeWriteSql::InsertDispatchJournal => 4,
                PermissionRuntimeWriteSql::Commit => 6,
                PermissionRuntimeWriteSql::BeginImmediate => {
                    return Err(PermissionError::CorruptRecord)
                }
            };
            if self.sql_stage != expected {
                return Err(PermissionError::CorruptRecord);
            }
            validation::runtime_phase::runtime_write_before_sql(
                self.session.admission.budget_mut(),
                &statement,
                &self.authoritative_plan.snapshot.execution_id,
                &self.post_thread.id,
                self.before_scalars.next_execution_revision,
            )?;
            self.sql_stage = expected + 1;
            Ok(())
        })();
        self.session.admission.budget_mut().runtime_finish(result)?;
        // Fixed borrowed bindings and Result carriers are prepaid in THIS
        // admitted statement's disjoint Rust scratch before construction.
        Ok(PermissionRuntimeBindings {
            execution_id: self.authoritative_plan.snapshot.execution_id.as_str(),
            thread_id: self.post_thread.id.as_str(),
            generation: self.post_thread.generation.as_str(),
            before_revision: self.before_scalars.execution_revision,
            before_resource_revision: self.before_scalars.resource_revision,
            owner_generation: self.before_scalars.owner_generation,
            next_revision: self.before_scalars.next_execution_revision,
            next_resource_revision: self.before_scalars.next_resource_revision,
            updated_at_ms: self.before_scalars.updated_at_ms,
            snapshot: &self.snapshot_payload,
            journal: &self.journal_payload,
        })
    }
    pub fn check_affected_rows(
        &mut self,
        statement: PermissionRuntimeWriteSql,
        rows: usize,
    ) -> Result<(), PermissionError> {
        let result = (|| {
            self.session.admission.budget_mut().runtime_reserve(0, 0)?;
            let expected = match statement {
                PermissionRuntimeWriteSql::SnapshotCas => 1,
                PermissionRuntimeWriteSql::ThreadCas => 3,
                PermissionRuntimeWriteSql::InsertDispatchJournal => 5,
                _ => return Err(PermissionError::CorruptRecord),
            };
            if self.sql_stage != expected {
                return Err(PermissionError::CorruptRecord);
            }
            if rows != 1 {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            self.sql_stage = expected + 1;
            Ok(())
        })();
        self.session.admission.budget_mut().runtime_finish(result)
    }
    pub fn finish_after_commit(self) -> PermissionRuntimeCommit {
        // Everything required was checked before the actual successful COMMIT.
        // Consume Work first to end the real Work-to-Session loan. No new
        // checked extraction, Arc clone, allocation or validator follows it.
        let Self {
            before_execution,
            post_thread,
            authoritative_plan,
            post_facts,
            origin,
            ..
        } = self;
        PermissionRuntimeCommit {
            resource: ApprovalResource {
                thread: post_thread,
                execution: StoredExecution {
                    thread_id: before_execution.thread_id,
                    snapshot: authoritative_plan.snapshot,
                },
            },
            facts: post_facts,
            origin,
            committed: PermissionRuntimeDispatchCommitted,
        }
    }
}

// This REAL tuple/Result is independently bounded; Work's size alone is not a proof.
const _: () = assert!(
    size_of::<
        Result<
            (
                StoredExecution,
                ThreadRecord,
                CommitPlan,
                PermissionRuntimeResourceFacts,
                String,
                String,
                JournalEntry,
                PermissionRuntimeBeforeScalars,
                PermissionRuntimeFreshMatchStorage,
                Arc<PermissionRuntimeOrigin>,
                PermissionRuntimePostReadCertificate
            ),
            PermissionError,
        >,
    >() <= 4_096
);

// Data-only virtual upper for the unchanged public LegacyOnce DispatchSome
// empty-new-header route. C32768/512 is paid by Work before this calculation.
fn runtime_once_post_read_certificate(
    snapshot: &b4_contract::Snapshot,
    thread: &ThreadRecord,
    facts: &PermissionRuntimeResourceFacts,
    capture: &PermissionReadyDispatchCapture,
    journal: &JournalEntry,
    journal_measure: PermissionRuntimeMeasure,
) -> Result<PermissionRuntimePostReadCertificate, PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    let (view, legacy, journals, raw) = match capture {
        PermissionReadyDispatchCapture::LegacyOnce {
            view,
            legacy_facts,
            journals,
            raw,
            ..
        } => (view, legacy_facts, journals, raw),
        _ => return Err(PermissionError::CorruptRecord),
    };
    // Actual six measurements belong to the authoritative immutable post;
    // paired facts are copied only after the independently paid strict pairs.
    if !runtime_bounded(facts.actual[0], 2_048, 100, 127)
        || !runtime_bounded(facts.actual[1], 88, 9, 127)
        || !runtime_bounded(facts.actual[2], 88, 9, 127)
        || !runtime_bounded(facts.actual[3], 31, 5, 127)
        || !runtime_bounded(facts.actual[4], 31, 5, 127)
        || !runtime_bounded(facts.actual[5], 306, 21, 127)
        || !runtime_bounded(legacy.actual[0], 961, 83, 64)
        || !runtime_bounded(legacy.actual[1], 128, 16, 64)
        || !runtime_bounded(legacy.actual[2], 584, 40, 64)
        || !runtime_bounded(legacy.actual[3], 160, 11, 1)
        || !runtime_bounded(legacy.actual[4], 160, 11, 1)
        || !runtime_bounded(journal_measure, 160, 11, 1)
        || raw[0].len() > 584
        || raw[1].len() > 961
        || raw[2].len() > 160
        || raw[3].len() > 160
        || legacy.admitted_counts_and_flags != [1, 2]
        || view.grants.len() != 1
        || view.acceptances.len() != 1
        || journals.len() != 2
        || snapshot.definition.batches.len() != 1
        || snapshot.definition.batches[0].len() != 1
        || snapshot.tasks.len() != 1
        || snapshot.batch_index != 0
        || snapshot.turn_history.len() != 2
        || snapshot.attempt_history.len() != 1
        || snapshot.status != E::Running
        || snapshot.output.is_some()
        || snapshot.failure.is_some()
        || !thread.metadata.is_empty()
        || thread.origin.is_none()
        || thread.active_execution_id.as_ref() != Some(&snapshot.execution_id)
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let task = &snapshot.tasks[0];
    let interrupt = task
        .interrupt
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let attempt = task
        .attempt_id
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let acceptance = &view.acceptances[0];
    let origin = thread
        .origin
        .as_ref()
        .ok_or(PermissionError::CorruptRecord)?;
    let labels = runtime_add(
        runtime_add(origin.application.len(), origin.tenant.len())?,
        runtime_add(
            origin.principal.len(),
            acceptance.context.policy_revision.len(),
        )?,
    )?;
    let turn_bytes = snapshot
        .turn_history
        .iter()
        .try_fold(0usize, |sum, turn| runtime_add(sum, turn.as_str().len()))?;
    if task.status != T::InFlight || !task.approved || !task.spec.requires_approval
        || task.result.is_some() || task.failure.is_some()
        || snapshot.attempt_history[0] != *attempt || attempt.as_str().len() > 256
        || task.spec != snapshot.definition.batches[0][0] || task.input != snapshot.values
        || snapshot.execution_id.as_str().len() > 20 || thread.id.as_str().len() > 17
        || thread.generation.as_str().len() != 64 || task.spec.id.as_str().len() > 7
        || task.spec.effect_id.as_str().len() > 14 || interrupt.id.as_str().len() > 17
        || snapshot.turn_id.as_str().len() > 13 || turn_bytes > 26
        || acceptance.turn_id.as_str().len() > 13
        || snapshot.definition.revision.as_str().len() > 12 || snapshot.definition.route.as_str().len() > 23
        || labels > 33 || !view.grants[0].consumed || acceptance.decisions.len() != 1
        || acceptance.decisions[0].decision != ApprovalDecision::Once
        // Physical raw lengths are retained from actual I1 cells. Typed empty
        // metadata plus length2 is the exact original {}, not reserialized data.
        || facts.raw_and_scalar[0] > 17 || facts.raw_and_scalar[1] > 2_048
        || facts.raw_and_scalar[2] != 8 || facts.raw_and_scalar[3] != 8
        || facts.raw_and_scalar[4] > 17 || facts.raw_and_scalar[5] != 2
        || facts.raw_and_scalar[6] == 0 || facts.raw_and_scalar[6] > 306
        || facts.raw_and_scalar[7] == 0 || facts.raw_and_scalar[7] > 20
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    if journal.execution_id != snapshot.execution_id
        || journal.revision != snapshot.revision
        || journal.owner_generation != snapshot.owner_generation
        || journal.status != snapshot.status
        || journal.updated_at_ms != snapshot.updated_at_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    // Complete fresh old public passes, all allocating old128 resource/raw
    // workspaces and strict support, both Eq operands, real G/oldA decoders,
    // physical rows, shared keys, cursor/endrange, and Finished/View carriers.
    // No A/D or journal SQL materialization is synthesized for the empty route.
    let source_terms: [(usize, usize); 9] = [
        (3_836_886, 1_518), // Two complete public Resource materializations.
        (76_614, 330),      // Public service request/context/catalog/Definition/task.
        (184_351, 958),     // Whole combined Resource/Definition match.
        (871_740, 548),     // One consumed G complete old decoder/physical capture.
        (400_285, 310),     // One old acceptance complete decoder/physical capture.
        (14_976, 117),      // Seven cursor attempts and five endrange debits.
        (235_520, 23),      // Twenty-three statement capacities at old10240/1.
        (10_593, 164),      // Seventeen actual key attempts/ten retained keys.
        (4_096, 64),        // Old Finished and View factory.
    ];
    let mut encoded_bytes = 0usize;
    let mut structural_nodes = 0usize;
    for (bytes, nodes) in source_terms {
        encoded_bytes = runtime_add(encoded_bytes, bytes)?;
        structural_nodes = runtime_add(structural_nodes, nodes)?;
    }
    let normal_sql_slots = 21usize;
    let capacity_sql_slots = 23usize;
    let retained_keys = 10usize;
    if encoded_bytes > 8_388_608
        || structural_nodes > 65_536
        || normal_sql_slots > capacity_sql_slots
        || capacity_sql_slots > 128
        || retained_keys > 1_024
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    Ok(PermissionRuntimePostReadCertificate {
        encoded_bytes,
        structural_nodes,
        normal_sql_slots,
        capacity_sql_slots,
    })
}
const _: () = assert!(size_of::<[(usize, usize); 9]>() <= 256);
const _: () =
    assert!(size_of::<Result<PermissionRuntimePostReadCertificate, PermissionError>>() <= 256);

// Full actual non-Copy incoming clock-facts value, not a measure or receipt.
const _: () = assert!(size_of::<b4_contract::TransitionFacts>() <= 16);

// Canonical physical journal-status comparison transport bounds.
const _: () = assert!(size_of::<&mut PermissionEvidenceBudget>() <= 16);
const _: () = assert!(size_of::<&mut validation::RuntimeDispatchAdmission<'static>>() <= 16);

// Data-only upper for the CURRENT nonempty public Dispatch reader prefix.
// Its historical Resume header deliberately refuses the new InFlight revision;
// no future J3 reader, successful availability or authority is represented here.
// Visible virtual-reader rows are evaluated only at compile time.
const RUNTIME_HUMAN_READER_SOURCE_TERMS: [(usize, usize); 15] = [
    (3_836_886, 1_518), // Two complete old allocating public Resource passes.
    (76_614, 330),      // Actual same-catalog/context request and current Definition.
    (184_351, 958),     // Complete combined Resource/Definition BOTH match.
    (801_424, 5_132),   // Fifteen AD passes + separate root/Eq and intrinsic bounds.
    (124_912, 343),     // G65+prompt64 walks, root, shape, index and actual Vec.
    (10_916, 39),       // Physical G header/query strings/raw-length admission.
    (266_240, 26),      // Twenty-six old statement capacities at10240/1.
    (22_528, 176),      // Eleven actual header cursor attempts.
    (8_192, 64),        // Two old new-header admissions.
    (16_384, 128),      // Two projection header owners before retaining prefixes.
    (768, 6),           // Six completed ranges.
    (65_536, 512),      // Literal AD lifecycle before first payload.
    (20_157, 305),      // Closed22 attempted/15 retained keys and3 prefix strings.
    (65_536, 512),      // Literal journal lifecycle BEFORE first Park header.
    (16_384, 256),      // First projection branch's preconstruction controls.
];
const RUNTIME_HUMAN_READER_SOURCE_BYTES: usize = RUNTIME_HUMAN_READER_SOURCE_TERMS[0].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[1].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[2].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[3].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[4].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[5].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[6].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[7].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[8].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[9].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[10].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[11].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[12].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[13].0
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[14].0;
const _: () = assert!(RUNTIME_HUMAN_READER_SOURCE_BYTES == 5_516_828);
const RUNTIME_HUMAN_READER_SOURCE_NODES: usize = RUNTIME_HUMAN_READER_SOURCE_TERMS[0].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[1].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[2].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[3].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[4].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[5].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[6].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[7].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[8].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[9].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[10].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[11].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[12].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[13].1
    + RUNTIME_HUMAN_READER_SOURCE_TERMS[14].1;
const _: () = assert!(RUNTIME_HUMAN_READER_SOURCE_NODES == 10_305);
// Every admitted nonnegative intermediate is at most this proven upper.
const _: usize = 198_943_418;

fn runtime_human_current_post_read_certificate(
    snapshot: &b4_contract::Snapshot,
    thread: &ThreadRecord,
    facts: &PermissionRuntimeResourceFacts,
    capture: &PermissionReadyDispatchCapture,
    journal: &JournalEntry,
    journal_measure: PermissionRuntimeMeasure,
) -> Result<PermissionRuntimePostReadCertificate, PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    let (observation, witness, origin) = match capture {
        PermissionReadyDispatchCapture::NewHuman {
            observation,
            witness,
            origin,
        } => (observation, witness.as_ref(), origin),
        _ => return Err(PermissionError::CorruptRecord),
    };
    let q = &witness.facts.actual;
    if !Arc::ptr_eq(origin, &witness.origin)
        || !Arc::ptr_eq(origin, &observation.origin)
        || witness.facts.admitted_counts_and_flags != [1, 0]
    {
        return Err(PermissionError::CorruptRecord);
    }
    if !runtime_bounded(facts.actual[0], 2_048, 100, 127)
        || !runtime_bounded(facts.actual[1], 88, 9, 127)
        || !runtime_bounded(facts.actual[2], 88, 9, 127)
        || !runtime_bounded(facts.actual[3], 31, 5, 127)
        || !runtime_bounded(facts.actual[4], 31, 5, 127)
        || !runtime_bounded(facts.actual[5], 306, 21, 2)
        || !runtime_bounded(q[0], 1_766, 154, 64)
        || !runtime_bounded(q[1], 848, 91, 64)
        || !runtime_bounded(q[2], 670, 55, 64)
        || !runtime_bounded(q[3], 998, 67, 64)
        || !runtime_bounded(q[4], 670, 55, 64)
        || !runtime_bounded(q[5], 962, 83, 65)
        || !runtime_bounded(q[6], 128, 16, 64)
        || !runtime_bounded(q[7], 160, 11, 1)
        || !runtime_bounded(q[8], 160, 11, 1)
        || !runtime_bounded(journal_measure, 160, 11, 1)
        || witness.raw[0].len() > 1_766 || witness.raw[1].len() > 998
        || witness.raw[2].len() > 962 || witness.raw[3].len() > 160
        || witness.raw[4].len() > 160
        || snapshot.definition.batches.len() != 1 || snapshot.definition.batches[0].len() != 1
        || snapshot.tasks.len() != 1 || snapshot.batch_index != 0
        || snapshot.turn_history.len() != 2 || snapshot.attempt_history.len() != 1
        || snapshot.status != E::Running || snapshot.output.is_some() || snapshot.failure.is_some()
        || !thread.metadata.is_empty() || thread.origin.is_none()
        || thread.active_execution_id.as_ref() != Some(&snapshot.execution_id)
        // AcceptanceWire inserts defaults only in this ONE actual decoded TaskRecord.
        || witness.acceptance.before_snapshot().tasks.len() != 1
        || runtime_compat::human_acceptance_decisions_len(&witness.acceptance) != 1
        || witness.acceptance.decisions()[0].decision() != ApprovalDecision::Always
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let task = &snapshot.tasks[0];
    let interrupt = match task.interrupt.as_ref() {
        Some(actual) => actual,
        None => return Err(PermissionError::CorruptRecord),
    };
    let attempt = match task.attempt_id.as_ref() {
        Some(actual) => actual,
        None => return Err(PermissionError::CorruptRecord),
    };
    let resource_origin = match thread.origin.as_ref() {
        Some(actual) => actual,
        None => return Err(PermissionError::CorruptRecord),
    };
    // Reached actual root measures bound all four strings before this sum.
    let labels = resource_origin.application.len()
        + resource_origin.tenant.len()
        + resource_origin.principal.len()
        + runtime_compat::human_acceptance_policy_revision_len(&witness.acceptance);
    // The earlier immutable profile guard established exactly two entries.
    let turn_bytes = snapshot.turn_history[0].byte_len() + snapshot.turn_history[1].byte_len();
    if task.status != T::InFlight
        || !task.approved
        || !task.spec.requires_approval
        || task.result.is_some()
        || task.failure.is_some()
        || snapshot.attempt_history[0] != *attempt
        || attempt.byte_len() > 256
        || task.spec != snapshot.definition.batches[0][0]
        || task.input != snapshot.values
        || snapshot.execution_id.byte_len() > 20
        || thread.id.byte_len() > 17
        || thread.generation.byte_len() != 64
        || task.spec.id.byte_len() > 7
        || task.spec.effect_id.byte_len() > 14
        || interrupt.id.byte_len() > 17
        || snapshot.turn_id.byte_len() > 13
        || turn_bytes > 26
        || witness.acceptance.turn_id().byte_len() > 13
        || snapshot.definition.revision.byte_len() > 12
        || snapshot.definition.route.byte_len() > 23
        || labels > 33
        || !witness.grant.consumed
        || facts.raw_and_scalar[0] > 17
        || facts.raw_and_scalar[1] > 2_048
        || facts.raw_and_scalar[2] != 8
        || facts.raw_and_scalar[3] != 8
        || facts.raw_and_scalar[4] > 17
        || facts.raw_and_scalar[5] != 2
        || facts.raw_and_scalar[6] == 0
        || facts.raw_and_scalar[6] > 306
        || facts.raw_and_scalar[7] == 0
        || facts.raw_and_scalar[7] > 20
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    if journal.execution_id != snapshot.execution_id
        || journal.revision != snapshot.revision
        || journal.owner_generation != snapshot.owner_generation
        || journal.status != snapshot.status
        || journal.updated_at_ms != snapshot.updated_at_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    // The classified C1 W entrance prepays this finite new calculation.
    // Exact normal/first-error allowance remains an independent Source gate.
    let a_shape = &witness.raw_geometry[0];
    let d_shape = &witness.raw_geometry[1];
    let g_shape = &witness.raw_geometry[2];
    // Each opaque value moved with its real successful C1 codec output. I1 has
    // already matched those same raw bytes and actual resource/header owners.
    // Actual default insertions are guarded before the plain nonnegative formula.
    if !a_shape.is_acceptance()
        || !d_shape.is_decision()
        || !g_shape.is_grant()
        || a_shape.raw_len() != witness.raw[0].len()
        || d_shape.raw_len() != witness.raw[1].len()
        || g_shape.raw_len() != witness.raw[2].len()
        || a_shape.raw_len() == 0
        || a_shape.raw_len() > 1_766
        || d_shape.raw_len() == 0
        || d_shape.raw_len() > 998
        || g_shape.raw_len() == 0
        || g_shape.raw_len() > 962
        || a_shape.raw_nodes() > a_shape.raw_len()
        || d_shape.raw_nodes() > d_shape.raw_len()
        || g_shape.raw_nodes() > g_shape.raw_len()
        || a_shape.containers() > a_shape.raw_len()
        || d_shape.containers() > d_shape.raw_len()
        || g_shape.containers() > g_shape.raw_len()
        || a_shape.keys() > a_shape.raw_len()
        || d_shape.keys() > d_shape.raw_len()
        || g_shape.keys() > g_shape.raw_len()
        || a_shape.numbers() > a_shape.raw_nodes()
        || d_shape.numbers() > d_shape.raw_nodes()
        || g_shape.numbers() > g_shape.raw_nodes()
        || a_shape.preflight_depth_limit() != 64
        || d_shape.preflight_depth_limit() != 64
        || g_shape.preflight_depth_limit() != 65
        || a_shape.inserted_bytes() > 64
        || a_shape.inserted_nodes() > 8
        || d_shape.inserted_bytes() != 0
        || d_shape.inserted_nodes() != 0
        || g_shape.inserted_bytes() != 0
        || g_shape.inserted_nodes() != 0
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let a_raw = runtime_human_raw_preflight_upper(a_shape);
    let d_raw = runtime_human_raw_preflight_upper(d_shape);
    let g_raw = runtime_human_raw_preflight_upper(g_shape);
    // These fresh conservative typed/hash terms do NOT subtract a generic F
    // from a historical fixture total. Fifteen real AD passes are bounded
    // independently, and G's literal walk/root/shape/index/Vec sites are separate.
    let encoded_bytes = a_raw.0 + d_raw.0 + g_raw.0 + RUNTIME_HUMAN_READER_SOURCE_BYTES;
    let structural_nodes = a_raw.1 + d_raw.1 + g_raw.1 + RUNTIME_HUMAN_READER_SOURCE_NODES;
    let normal_sql_slots = 24usize;
    let capacity_sql_slots = 26usize;
    if encoded_bytes > 8_388_608
        || structural_nodes > 65_536
        || normal_sql_slots > capacity_sql_slots
        || capacity_sql_slots > 128
        || 15usize > 1_024
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    Ok(PermissionRuntimePostReadCertificate {
        encoded_bytes,
        structural_nodes,
        normal_sql_slots,
        capacity_sql_slots,
    })
}
const _: () = assert!(size_of::<[(usize, usize); 15]>() <= 256);
const _: () = assert!(size_of::<[usize; 13]>() <= 256);
const _: () = assert!(size_of::<(usize, usize)>() <= 128);
const _: () = assert!(size_of::<Result<(usize, usize), PermissionError>>() <= 256);

// Human certificate geometry is observed from its real immutable C1 bytes.
// These private counters carry no permission or canonical-measure authority.
struct RuntimeHumanRawShape {
    raw_len: usize,
    nodes: usize,
    containers: usize,
    keys: usize,
    encoded_key_bytes: usize,
    max_object_keys: usize,
    numbers: usize,
    maximum_depth: usize,
}
struct RuntimeHumanRawFrame {
    object: bool,
    expecting_key: bool,
    keys: usize,
}
enum RuntimeHumanRawKind {
    Acceptance,
    Decision,
    Grant,
}
struct RuntimeHumanRawWorkspace {
    frames: Vec<RuntimeHumanRawFrame>,
}
impl RuntimeHumanRawWorkspace {
    fn observe(
        &mut self,
        raw: &[u8],
        kind: RuntimeHumanRawKind,
    ) -> Result<RuntimeHumanRawShape, PermissionError> {
        let (maximum_bytes, maximum_depth) = match kind {
            RuntimeHumanRawKind::Acceptance => (1_766, 64),
            RuntimeHumanRawKind::Decision => (998, 64),
            RuntimeHumanRawKind::Grant => (962, 65),
        };
        if raw.is_empty() || raw.len() > maximum_bytes || !self.frames.is_empty() {
            return Err(PermissionError::UnsupportedProfile);
        }
        let mut shape = RuntimeHumanRawShape {
            raw_len: raw.len(),
            nodes: 0,
            containers: 0,
            keys: 0,
            encoded_key_bytes: 0,
            max_object_keys: 0,
            numbers: 0,
            maximum_depth: 0,
        };
        let mut position = 0usize;
        // The real codec already proved this immutable byte slice's grammar,
        // UTF8, escapes and duplicate-key rules. This separate paid pass only
        // observes lexical shape; it cannot confer that original acceptance.
        while position < raw.len() {
            match raw[position] {
                b' ' | b'\n' | b'\r' | b'\t' | b':' => position += 1,
                b',' => {
                    let frame = match self.frames.last_mut() {
                        Some(actual) => actual,
                        None => return Err(PermissionError::CorruptRecord),
                    };
                    if frame.object {
                        frame.expecting_key = true;
                    }
                    position += 1;
                }
                b'{' | b'[' => {
                    if self.frames.len() == maximum_depth {
                        return Err(PermissionError::UnsupportedProfile);
                    }
                    shape.nodes += 1;
                    shape.containers += 1;
                    let object = raw[position] == b'{';
                    self.frames.push(RuntimeHumanRawFrame {
                        object,
                        expecting_key: object,
                        keys: 0,
                    });
                    shape.maximum_depth = shape.maximum_depth.max(self.frames.len());
                    position += 1;
                }
                b'}' | b']' => {
                    let frame = match self.frames.pop() {
                        Some(actual) => actual,
                        None => return Err(PermissionError::CorruptRecord),
                    };
                    if frame.object != (raw[position] == b'}') {
                        return Err(PermissionError::CorruptRecord);
                    }
                    position += 1;
                }
                b'"' => {
                    let begin = position;
                    position += 1;
                    loop {
                        let byte = match raw.get(position) {
                            Some(actual) => *actual,
                            None => return Err(PermissionError::CorruptRecord),
                        };
                        match byte {
                            b'\\' => {
                                if position + 1 >= raw.len() {
                                    return Err(PermissionError::CorruptRecord);
                                }
                                position += 2;
                            }
                            b'"' => {
                                position += 1;
                                break;
                            }
                            _ => position += 1,
                        }
                    }
                    shape.nodes += 1;
                    if let Some(frame) = self.frames.last_mut() {
                        if frame.object && frame.expecting_key {
                            frame.keys += 1;
                            frame.expecting_key = false;
                            shape.keys += 1;
                            shape.encoded_key_bytes += position - begin;
                            shape.max_object_keys = shape.max_object_keys.max(frame.keys);
                        }
                    }
                }
                b't' | b'f' | b'n' => {
                    let token: &[u8] = match raw[position] {
                        b't' => b"true",
                        b'f' => b"false",
                        _ => b"null",
                    };
                    if raw.get(position..position + token.len()) != Some(token) {
                        return Err(PermissionError::CorruptRecord);
                    }
                    shape.nodes += 1;
                    position += token.len();
                }
                b'-' | b'0'..=b'9' => {
                    shape.nodes += 1;
                    shape.numbers += 1;
                    position += 1;
                    while position < raw.len()
                        && matches!(
                            raw[position],
                            b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
                        )
                    {
                        position += 1;
                    }
                }
                _ => return Err(PermissionError::CorruptRecord),
            }
        }
        if !self.frames.is_empty() || shape.nodes == 0 {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(shape)
    }
}
fn runtime_human_raw_preflight_upper(
    shape: &runtime_compat::RuntimeHumanRawGeometry,
) -> (usize, usize) {
    let raw_len = shape.raw_len();
    let nodes = shape.raw_nodes();
    let containers = shape.containers();
    let keys = shape.keys();
    let numbers = shape.numbers();
    let inserted_bytes = shape.inserted_bytes();
    let inserted_nodes = shape.inserted_nodes();
    let preflight_depth_limit = shape.preflight_depth_limit();
    // These are safe upper bounds, not observed key-span or object maxima.
    // Accepted quoted key spans are disjoint raw substrings; each object's
    // key count is bounded by the actual total from that same successful codec.
    let encoded_key_byte_upper = raw_len;
    let object_key_count_upper = keys;
    // Prior guards establish counters <= L <=1766, selected depth64/64/65,
    // and actual I/U<=64/8. Original equality-before-push depth checks remain
    // in the copied codec. Every nonnegative intermediate fits 198_943_418.
    let bytes = (preflight_depth_limit + 1) * 512
        + 4_608
        + 4 * raw_len
        + 1_536 * containers
        + 2_960 * keys
        + 39 * encoded_key_byte_upper
        + 2 * object_key_count_upper * encoded_key_byte_upper
        + 16 * object_key_count_upper * keys
        + 32 * numbers
        + 128 * nodes
        + inserted_bytes
        + 128 * inserted_nodes;
    (bytes, 2 * nodes + inserted_nodes)
}

const _: () = assert!(size_of::<RuntimeHumanRawShape>() <= 128);
const _: () = assert!(size_of::<Result<RuntimeHumanRawShape, PermissionError>>() <= 256);
const _: () = assert!(size_of::<RuntimeHumanRawWorkspace>() <= 256);
const _: () = assert!(size_of::<RuntimeHumanRawKind>() <= 1);
const _: () = assert!(size_of::<RuntimeHumanRawFrame>() <= 128);
const _: () = assert!(
    66 * size_of::<RuntimeHumanRawFrame>()
        + size_of::<RuntimeHumanRawWorkspace>()
        + size_of::<RuntimeHumanRawShape>()
        + size_of::<Result<RuntimeHumanRawShape, PermissionError>>()
        + size_of::<Option<RuntimeHumanRawFrame>>()
        + 16 * size_of::<usize>()
        <= 33_792
);

// Actual simultaneous observer/caller owners; these widths are not fee savings.
const _: () = assert!(size_of::<usize>() <= 16);
const _: () = assert!(
    size_of::<(
        &'static mut RuntimeHumanRawWorkspace,
        &'static [u8],
        RuntimeHumanRawKind
    )>() <= 256
);
const _: () = assert!(
    66 * size_of::<RuntimeHumanRawFrame>()
        + size_of::<RuntimeHumanRawWorkspace>()
        + 5 * size_of::<RuntimeHumanRawShape>()
        + 2 * size_of::<Result<RuntimeHumanRawShape, PermissionError>>()
        + size_of::<Option<RuntimeHumanRawFrame>>()
        + 3 * size_of::<(usize, usize)>()
        + 16 * size_of::<usize>()
        <= 33_792
);
const _: () = assert!(
    3 * size_of::<RuntimeHumanRawShape>()
        + 3 * size_of::<(usize, usize)>()
        + size_of::<[(usize, usize); 15]>()
        + size_of::<[usize; 13]>()
        + size_of::<PermissionRuntimePostReadCertificate>()
        + size_of::<Result<PermissionRuntimePostReadCertificate, PermissionError>>()
        + size_of::<RuntimeHumanRawWorkspace>()
        + size_of::<Result<RuntimeHumanRawShape, PermissionError>>()
        + size_of::<Option<RuntimeHumanRawFrame>>()
        + 16 * size_of::<usize>()
        <= 2_048
);

// Per-byte groups use this actual compact Frame, not the weaker128 ceiling.
const _: () = assert!(size_of::<RuntimeHumanRawFrame>() <= 16);
const _: () = assert!(size_of::<Option<RuntimeHumanRawFrame>>() <= 32);
const _: () = assert!(
    6 * size_of::<usize>()
        + 2 * size_of::<RuntimeHumanRawFrame>()
        + size_of::<Option<RuntimeHumanRawFrame>>()
        + 2 * size_of::<&'static mut RuntimeHumanRawFrame>()
        <= 128
);

// Experimental actual-type width checks only. No fee savings or behavior credit.
const _: () = assert!(size_of::<PermissionError>() <= 128);
const _: () = assert!(size_of::<Result<std::convert::Infallible, PermissionError>>() <= 128);
const _: () = assert!(size_of::<usize>() <= 8);
const _: () = assert!(size_of::<&'static mut PermissionEvidenceBudget>() <= 8);
const _: () = assert!(size_of::<&'static [u8]>() <= 16);
const _: () = assert!(
    size_of::<(
        &'static StoredExecution,
        &'static ThreadRecord,
        &'static PermissionRuntimeResourceFacts
    )>() <= 48
);
const _: () = assert!(
    size_of::<(
        StoredExecution,
        ThreadRecord,
        CommitPlan,
        PermissionRuntimeResourceFacts,
        String,
        String,
        JournalEntry,
        PermissionRuntimeBeforeScalars,
        PermissionRuntimeFreshMatchStorage,
        Arc<PermissionRuntimeOrigin>,
        PermissionRuntimePostReadCertificate
    )>() <= 3_072
);
const _: () = assert!(
    size_of::<
        Result<
            (
                StoredExecution,
                ThreadRecord,
                CommitPlan,
                PermissionRuntimeResourceFacts,
                String,
                String,
                JournalEntry,
                PermissionRuntimeBeforeScalars,
                PermissionRuntimeFreshMatchStorage,
                Arc<PermissionRuntimeOrigin>,
                PermissionRuntimePostReadCertificate
            ),
            PermissionError,
        >,
    >() <= 3_072
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (
                StoredExecution,
                ThreadRecord,
                CommitPlan,
                PermissionRuntimeResourceFacts,
                String,
                String,
                JournalEntry,
                PermissionRuntimeBeforeScalars,
                PermissionRuntimeFreshMatchStorage,
                Arc<PermissionRuntimeOrigin>,
                PermissionRuntimePostReadCertificate
            ),
        >,
    >() <= 3_072
);
const _: () = assert!(
    size_of::<PermissionRuntimeWork<'static, 'static, 'static, 'static, 'static>>() <= 3_072
);
const _: () = assert!(
    size_of::<
        Result<PermissionRuntimeWork<'static, 'static, 'static, 'static, 'static>, PermissionError>,
    >() <= 3_072
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeWork<'static, 'static, 'static, 'static, 'static>,
        >,
    >() <= 3_072
);
const _: () = assert!(
    size_of::<PermissionRuntimeWriteSession<'static, 'static, 'static, 'static>>() <= 4_096
);
const _: () = assert!(
    size_of::<
        Result<PermissionRuntimeWriteSession<'static, 'static, 'static, 'static>, PermissionError>,
    >() <= 4_096
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeWriteSession<'static, 'static, 'static, 'static>,
        >,
    >() <= 4_096
);
const _: () = assert!(size_of::<PermissionRuntimeCommit>() <= 1_536);
const _: () = assert!(size_of::<Result<PermissionRuntimeCommit, PermissionError>>() <= 1_536);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeCommit,
        >,
    >() <= 1_536
);
const _: () = assert!(size_of::<PermissionDispatchClaim<'static>>() <= 2_048);
const _: () =
    assert!(size_of::<Result<PermissionDispatchClaim<'static>, PermissionError>>() <= 2_048);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionDispatchClaim<'static>,
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ()>>()
        <= 128
);
const _: () = assert!(size_of::<RuntimeHumanRawShape>() <= 64);
const _: () = assert!(size_of::<RuntimeHumanRawWorkspace>() <= 24);
const _: () = assert!(size_of::<Result<RuntimeHumanRawShape, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            RuntimeHumanRawShape,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<(usize, usize)>() <= 16);
const _: () = assert!(
    66 * size_of::<RuntimeHumanRawFrame>()
        + size_of::<RuntimeHumanRawWorkspace>()
        + 5 * size_of::<RuntimeHumanRawShape>()
        + 2 * size_of::<Result<RuntimeHumanRawShape, PermissionError>>()
        + size_of::<Option<RuntimeHumanRawFrame>>()
        + 3 * size_of::<(usize, usize)>()
        + 16 * size_of::<usize>()
        <= 2_048
);

// Exact Option/argument carriers used by the Human normal and ingress recipes.
const _: () = assert!(size_of::<Option<RuntimeHumanRawFrame>>() <= 32);
const _: () = assert!(size_of::<Option<&'static mut RuntimeHumanRawFrame>>() <= 8);
const _: () = assert!(size_of::<Option<&'static u8>>() <= 8);
const _: () = assert!(size_of::<Option<&'static [u8]>>() <= 16);
const _: () = assert!(size_of::<Option<usize>>() <= 16);
const _: () = assert!(size_of::<Result<(), PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<(
        &'static StoredExecution,
        &'static ThreadRecord,
        &'static PermissionRuntimeResourceFacts
    )>() <= 24
);
const _: () = assert!(size_of::<(&'static mut PermissionEvidenceBudget, usize, usize)>() <= 24);

// Actual originating Kernel result and residual Flow are full Plan carriers.
// These assertions are independent of the smaller active Error variant.
const _: () = assert!(size_of::<CommitPlan>() <= 3_072);
const _: () = assert!(size_of::<Result<CommitPlan, b4_kernel::KernelError>>() <= 3_072);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, b4_kernel::KernelError>, CommitPlan>,
    >() <= 3_072
);
const _: () = assert!(size_of::<b4_kernel::KernelError>() <= 128);
const _: () = assert!(size_of::<Result<std::convert::Infallible, b4_kernel::KernelError>>() <= 128);

// One batch of concrete small carriers used by the Human event inventory.
// These exact full Results/Flows are independent of unit/error-only assertions.
const _: () = assert!(size_of::<&'static PermissionRuntimeFreshMatchStorage>() <= 8);
const _: () = assert!(size_of::<Option<&'static PermissionRuntimeFreshMatchStorage>>() <= 8);
const _: () = assert!(
    size_of::<Result<&'static PermissionRuntimeFreshMatchStorage, PermissionError>>() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionRuntimeFreshMatchStorage,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<&'static mut PermissionRuntimeFreshMatchStorage>() <= 8);
const _: () = assert!(size_of::<Option<&'static mut PermissionRuntimeFreshMatchStorage>>() <= 8);
const _: () = assert!(
    size_of::<Result<&'static mut PermissionRuntimeFreshMatchStorage, PermissionError>>() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static mut PermissionRuntimeFreshMatchStorage,
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<&'static (
        StoredExecution,
        [Option<PermissionRuntimeMeasure>; 5],
        [usize; 4]
    )>() <= 8
);
const _: () = assert!(
    size_of::<
        Option<&'static (
            StoredExecution,
            [Option<PermissionRuntimeMeasure>; 5],
            [usize; 4]
        )>,
    >() <= 8
);
const _: () = assert!(
    size_of::<
        Result<
            &'static (
                StoredExecution,
                [Option<PermissionRuntimeMeasure>; 5],
                [usize; 4]
            ),
            PermissionError,
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static (
                StoredExecution,
                [Option<PermissionRuntimeMeasure>; 5],
                [usize; 4]
            ),
        >,
    >() <= 128
);
const _: () =
    assert!(size_of::<&'static (ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>() <= 8);
const _: () = assert!(
    size_of::<Option<&'static (ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>>() <= 8
);
const _: () = assert!(
    size_of::<Result<&'static (ThreadRecord, PermissionRuntimeMeasure, [usize; 4]), PermissionError>>(
    ) <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static (ThreadRecord, PermissionRuntimeMeasure, [usize; 4]),
        >,
    >() <= 128
);
const _: () = assert!(size_of::<&'static PermissionRuntimeNewHeader>() <= 8);
const _: () = assert!(size_of::<Option<&'static PermissionRuntimeNewHeader>>() <= 8);
const _: () =
    assert!(size_of::<Result<&'static PermissionRuntimeNewHeader, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionRuntimeNewHeader,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<&'static PermissionRuntimeJournalHeader>() <= 8);
const _: () = assert!(size_of::<Option<&'static PermissionRuntimeJournalHeader>>() <= 8);
const _: () =
    assert!(size_of::<Result<&'static PermissionRuntimeJournalHeader, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionRuntimeJournalHeader,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<&'static b4_contract::PendingApproval>() <= 8);
const _: () = assert!(size_of::<Option<&'static b4_contract::PendingApproval>>() <= 8);
const _: () =
    assert!(size_of::<Result<&'static b4_contract::PendingApproval, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static b4_contract::PendingApproval,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<&'static b4_contract::ThreadId>() <= 8);
const _: () = assert!(size_of::<Option<&'static b4_contract::ThreadId>>() <= 8);
const _: () = assert!(size_of::<Result<&'static b4_contract::ThreadId, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static b4_contract::ThreadId,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<&'static [u8]>() <= 16);
const _: () = assert!(size_of::<Option<&'static [u8]>>() <= 16);
const _: () = assert!(size_of::<Result<&'static [u8], PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &'static [u8]>,
    >() <= 128
);
const _: () = assert!(size_of::<&'static str>() <= 16);
const _: () = assert!(size_of::<Option<&'static str>>() <= 16);
const _: () = assert!(size_of::<Result<&'static str, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &'static str>>(
    ) <= 128
);
const _: () = assert!(size_of::<u64>() <= 8);
const _: () = assert!(size_of::<Option<u64>>() <= 16);
const _: () = assert!(size_of::<Result<u64, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, u64>>()
        <= 128
);
const _: () = assert!(size_of::<usize>() <= 8);
const _: () = assert!(size_of::<Option<usize>>() <= 16);
const _: () = assert!(size_of::<Result<usize, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, usize>>()
        <= 128
);
const _: () = assert!(size_of::<bool>() <= 1);
const _: () = assert!(size_of::<Option<bool>>() <= 1);
const _: () = assert!(size_of::<Result<bool, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, bool>>()
        <= 128
);
const _: () = assert!(size_of::<(&'static str, usize)>() <= 24);
const _: () = assert!(size_of::<Result<(&'static str, usize), PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (&'static str, usize),
        >,
    >() <= 128
);
const _: () = assert!(size_of::<[u8; 64]>() <= 64);
const _: () = assert!(size_of::<Option<[u8; 64]>>() <= 72);
const _: () = assert!(size_of::<Result<[u8; 64], PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, [u8; 64]>>()
        <= 128
);
const _: () = assert!(size_of::<Option<Result<[u8; 64], PermissionError>>>() <= 128);
const _: () = assert!(size_of::<Result<Option<[u8; 64]>, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, Option<[u8; 64]>>,
    >() <= 128
);
const _: () = assert!(size_of::<Option<Result<u64, PermissionError>>>() <= 128);
const _: () = assert!(size_of::<Result<Option<u64>, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, Option<u64>>>(
    ) <= 128
);
const _: () = assert!(size_of::<String>() <= 32);
const _: () = assert!(size_of::<Option<String>>() <= 32);
const _: () = assert!(size_of::<Result<String, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, String>>()
        <= 128
);
const _: () = assert!(size_of::<std::array::TryFromSliceError>() <= 128);
const _: () = assert!(size_of::<Result<[u8; 64], std::array::TryFromSliceError>>() <= 128);
const _: () = assert!(size_of::<std::str::Utf8Error>() <= 128);
const _: () = assert!(size_of::<Result<&'static str, std::str::Utf8Error>>() <= 128);
const _: () = assert!(size_of::<std::collections::TryReserveError>() <= 128);
const _: () = assert!(size_of::<Result<(), std::collections::TryReserveError>>() <= 128);
const _: () = assert!(size_of::<Option<&'static b4_contract::AttemptId>>() <= 8);
const _: () = assert!(size_of::<Option<&'static crate::store::ThreadOrigin>>() <= 8);

// Retained-geometry caller layouts are mandatory experimental Source gates.
// These bounds do not price logical events, deep cleanup or failed ingress.
const _: () = assert!(size_of::<runtime_compat::RuntimeHumanRawGeometry>() <= 96);
const _: () = assert!(size_of::<Option<runtime_compat::RuntimeHumanRawGeometry>>() <= 128);
const _: () = assert!(size_of::<[runtime_compat::RuntimeHumanRawGeometry; 3]>() <= 288);
const _: () = assert!(size_of::<[Option<runtime_compat::RuntimeHumanRawGeometry>; 3]>() <= 384);
const _: () = assert!(size_of::<&'static runtime_compat::RuntimeHumanRawGeometry>() <= 8);
const _: () = assert!(size_of::<PermissionImmutableDispatchWitness>() <= 4_096);
const _: () =
    assert!(size_of::<PermissionRuntimeReadSession<'static, 'static, 'static>>() <= 8_192);
const _: () = assert!(size_of::<b4_contract::ExecutionStatus>() <= 8);
const _: () = assert!(size_of::<b4_contract::TaskStatus>() <= 8);
const _: () = assert!(size_of::<b4_contract::ApprovalDecision>() <= 8);
const _: () = assert!(size_of::<PermissionRuntimePostReadCertificate>() <= 32);
const _: () =
    assert!(size_of::<Result<PermissionRuntimePostReadCertificate, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimePostReadCertificate,
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<(
        &'static PermissionRuntimeObservation,
        &'static PermissionImmutableDispatchWitness,
        &'static Arc<PermissionRuntimeOrigin>
    )>() <= 24
);

// Exact private capture/Observed/Ready Try owners; these checks do not price events.
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeCapture,
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeObservation,
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionReadyDispatchCapture,
        >,
    >() <= 2_048
);

// Exact captured argument types in the pinned enabled-UB interface recipe.
const _: () = assert!(size_of::<(*const u8,)>() <= 128);
const _: () = assert!(size_of::<(*const (), usize)>() <= 128);

// Cancellation reuses this storage loan only after its distinct admission checks.
// The selected Read factory passes only its actual uncaptured empty-Vec generator.
fn runtime_ready_empty_raw_five<F>(empty: F) -> [Vec<u8>; 5]
where
    F: FnMut(usize) -> Vec<u8>,
{
    const { assert!(size_of::<F>() == 0) };
    std::array::from_fn(empty)
}

impl PermissionEvidenceBudget {
    pub fn runtime_ready_cancellation_read<'budget, 'selector, 'input>(
        &'budget mut self,
        selector: &'selector PermissionRuntimeSelector<'input>,
    ) -> Result<PermissionRuntimeReadSession<'budget, 'selector, 'input>, PermissionError>
    where
        'input: 'selector,
    {
        self.require_standard_account()?;
        self.runtime_check_ready_cancellation()?;
        // O: seven full Session owner/carrier sites and six actual Capture sites.
        // Helper-local real q/resource bundles are paid by their own helper sites.
        // Separate 128 bounded wrapper/control slots, each <=256B/16 logical nodes.
        // Paid before the first resource wrapper; helper, O and SQL fees stay separate.
        self.runtime_prepaid_ready_cancellation_fixed(32_768, 2_048)?;
        self.runtime_prepaid_ready_cancellation_fixed(69_632, 1_088)?;
        // Keep the original closed-role/default payer visible. The separately
        // named shared geometry-default partition precedes all new initializers.
        // Empty field retirement and the common grant/admission branches are included.
        self.runtime_prepaid_ready_cancellation_fixed(
            RUNTIME_READ_CLOSED_ROLE_AND_GEOMETRY_BYTES,
            RUNTIME_READ_CLOSED_ROLE_AND_GEOMETRY_NODES,
        )?;
        let role = selector.read_role()?;
        self.runtime_prepaid_ready_cancellation_fixed(64, 1)?;
        let origin = Arc::clone(selector.origin);
        let admission = self.admit_runtime_ready_cancellation_read(selector.origin, &role)?;
        Ok(PermissionRuntimeReadSession {
            admission,
            selector,
            origin,
            role,
            metadata_checked: false,
            stage: 0,
            execution: None,
            thread: None,
            headers: [None, None],
            counts: [0; 5],
            grant_header: None,
            acceptance: None,
            decision: None,
            grant: None,
            human_geometry: [None, None, None],
            old_acceptances: Vec::new(),
            journals: Vec::new(),
            journal_header: None,
            raw: runtime_ready_empty_raw_five(|_| Vec::new()),
            dispatch_raw: Vec::new(),
            resource_facts: None,
            witness_facts: None,
            legacy_facts: None,
            dispatch_measure: None,
            complete_flags: [false; 8],
            completed_capture: Err(PermissionError::CorruptRecord),
        })
    }
}

// C0-C6 are prospective cancellation-only fixed partitions. These amounts are
// unaccepted until the independent weighted normal/first-error invoice closes.
const READY_CANCEL_C0_BYTES: usize = 557_056;
const READY_CANCEL_C0_NODES: usize = 4_096;
const READY_CANCEL_C2_BYTES: usize = 65_536;
const READY_CANCEL_C2_NODES: usize = 2_048;
const READY_CANCEL_C3_BYTES: usize = 51_550;
const READY_CANCEL_C3_NODES: usize = 557;
const READY_CANCEL_C4_BYTES: usize = 65_536;
const READY_CANCEL_C4_NODES: usize = 2_048;
const READY_CANCEL_C5_BYTES: usize = 80_456;
const READY_CANCEL_C5_NODES: usize = 2_240;
const READY_CANCEL_C6_BYTES: usize = 79_704;
const READY_CANCEL_C6_NODES: usize = 3_067;

/// Sealed actual cancellation proposal; no constructor or permission view authority.
pub struct PermissionPreparedReadyCancellation<'capture, 'catalog> {
    capture: &'capture PermissionReadyDispatchCapture,
    requested_context: &'capture ApprovalContext,
    target: &'capture crate::RunCancellationTarget,
    fence: approval::ApprovalFence,
    facts: TransitionFacts,
    candidate_plan: CommitPlan,
    candidate_post_facts: PermissionRuntimeCandidatePostFacts,
    catalog: PermissionRuntimeCatalogFacts<'catalog>,
    origin: Arc<PermissionRuntimeOrigin>,
}

fn runtime_ready_cancellation_target(
    thread: &ThreadRecord,
    execution: &StoredExecution,
    target: &crate::RunCancellationTarget,
) -> Result<(), PermissionError> {
    let snapshot = &execution.snapshot;
    if thread.id != target.thread_id
        || thread.generation != target.thread_generation
        || thread.resource_revision != target.resource_revision
        || execution.thread_id != target.thread_id
        || snapshot.execution_id != target.execution_id
        || thread.active_execution_id.as_ref() != Some(&target.execution_id)
        || snapshot.revision != target.execution_fence.revision
        || snapshot.owner_generation != target.execution_fence.owner_generation
        || snapshot.turn_id != target.turn_id
    {
        return Err(crate::StoreError::RevisionConflict.into());
    }
    Ok(())
}

pub fn cancel_permission_ready<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    request: &super::PermissionReadyCancellationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<StoredExecution, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: super::PermissionCancellationAuthority + ?Sized,
{
    cancel_permission_ready_profile(
        store,
        authority,
        catalog,
        request,
        budget,
        PermissionRuntimeInputProfile::Strict,
        |_, execution, _| Ok(execution),
    )
}

/// Cancel an original Always Ready task using finite normal identifier bounds.
pub fn cancel_permission_ready_with_normal_inputs<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    request: &super::PermissionReadyCancellationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<StoredExecution, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: super::PermissionCancellationAuthority + ?Sized,
{
    cancel_permission_ready_profile(
        store,
        authority,
        catalog,
        request,
        budget,
        PermissionRuntimeInputProfile::NormalReadyCancellation,
        |_, execution, _| Ok(execution),
    )
}

pub fn cancel_permission_ready_with_normal_inputs_confirmed<
    S: PermissionStore + ?Sized,
    A: super::PermissionCancellationAuthority + ?Sized,
>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    request: &super::PermissionReadyCancellationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<super::PermissionReadyCancellationConfirmation, PermissionError> {
    cancel_permission_ready_profile(
        store,
        authority,
        catalog,
        request,
        budget,
        PermissionRuntimeInputProfile::NormalReadyCancellation,
        |before, execution, budget| {
            super::PermissionReadyCancellationConfirmation::from_commit(
                super::ready_confirmation::ReadyOrigin::OriginalAlways,
                before,
                execution,
                &request.facts,
                budget,
            )
        },
    )
}

fn cancel_permission_ready_profile<S, A, T>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    request: &super::PermissionReadyCancellationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
    profile: PermissionRuntimeInputProfile,
    project: impl FnOnce(
        &ApprovalResource,
        StoredExecution,
        &mut PermissionEvidenceBudget,
    ) -> Result<T, PermissionError>,
) -> Result<T, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: super::PermissionCancellationAuthority + ?Sized,
{
    const {
        assert!(size_of::<&S>() <= 16);
    }
    const {
        assert!(size_of::<&A>() <= 16);
    }
    let result = runtime_checked_ready_cancellation_entry::<_, _, 128>(|| {
        budget.runtime_check_ready_cancellation()?;
        let origin = budget.begin_runtime_ready_cancellation(profile)?;
        budget.runtime_prepaid_ready_cancellation_fixed(
            READY_CANCEL_C0_BYTES,
            READY_CANCEL_C0_NODES,
        )?;
        let target = request.target;
        let context = request.context;
        if profile == PermissionRuntimeInputProfile::NormalReadyCancellation
            && (target.execution_id.as_str().len() > profile.execution_id_bytes()
                || target.thread_id.as_str().len() > b4_contract::MAX_ID_BYTES
                || target.turn_id.as_str().len() > profile.turn_id_bytes())
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if !store.approval_durable() {
            return Err(ApprovalError::DurabilityRequired.into());
        }
        validate_approval_context(context)?;
        let fence = approval::ApprovalFence {
            execution: target.execution_fence,
            resource_revision: target.resource_revision,
        };
        let r1_selector = PermissionRuntimeSelector {
            origin: &origin,
            mode: PermissionRuntimeReadMode::FirstResource(PermissionRuntimeFirstResourceRequest {
                requested_execution: &target.execution_id,
                requested_context: context,
            }),
        };
        let r1 =
            runtime_observed(store.capture_permission_ready_cancellation(&r1_selector, budget)?)?;
        #[allow(clippy::drop_non_drop)]
        drop(r1_selector);
        budget.runtime_prepaid_ready_cancellation_fixed(
            READY_CANCEL_C2_BYTES,
            READY_CANCEL_C2_NODES,
        )?;
        if !Arc::ptr_eq(&origin, &r1.origin) {
            return Err(PermissionError::CorruptRecord);
        }
        runtime_fence_parts(
            store,
            &r1.resource.thread,
            &r1.resource.execution,
            &target.execution_id,
            fence,
        )?;
        runtime_ready_cancellation_target(&r1.resource.thread, &r1.resource.execution, target)?;
        if r1.resource.thread.origin.as_ref() != Some(&context.actor) {
            return Err(ApprovalError::Unauthorized.into());
        }
        let task = &r1.resource.execution.snapshot.tasks[0];
        let (definition, registration, actual) = catalog
            .runtime_ready_cancellation_catalog_capture(
                &r1.resource.execution.snapshot.definition,
                task,
                budget,
            )?;
        let identity = catalog.application_identity();
        if identity.application != context.actor.application {
            return Err(PermissionError::InvalidCatalog);
        }
        authority.authorize_cancel(context, &r1.resource, target)?;
        budget.mark_runtime_ready_cancellation_authorized(&origin)?;
        budget.runtime_reserve(READY_CANCEL_C3_BYTES, READY_CANCEL_C3_NODES)?;
        let resource_origin = r1
            .resource
            .thread
            .origin
            .as_ref()
            .ok_or(ApprovalError::Unauthorized)?;
        runtime_normal_cancellation_owners(profile, r1.facts.actual[5], 2, budget)?;
        let invocation_context = InvocationContext {
            thread_id: r1.resource.thread.id.clone(),
            thread_generation: r1.resource.thread.generation.clone(),
            origin: resource_origin.clone(),
            application_revision: identity.revision.clone(),
            execution_revision: definition.revision.clone(),
            policy_revision: context.policy_revision.clone(),
        };
        let c1_selector = PermissionRuntimeSelector {
            origin: &origin,
            mode: PermissionRuntimeReadMode::Matched(PermissionRuntimeMatchedReadRequest {
                role: PermissionRuntimeMatchedReadRole::ReadyAuthorityC1,
                requested_execution: &target.execution_id,
                context: &invocation_context,
                definition,
                expected_resource: &r1.resource,
                expected_facts: &r1.facts,
                task_id: Some(&task.spec.id),
                claimed: None,
            }),
        };
        let captured =
            runtime_ready(store.capture_permission_ready_cancellation(&c1_selector, budget)?)?;
        #[allow(clippy::drop_non_drop)]
        drop(c1_selector);
        let (observation, witness) = match &captured {
            PermissionReadyDispatchCapture::NewHuman {
                observation,
                witness,
                origin: captured_origin,
            } => {
                if !Arc::ptr_eq(&origin, captured_origin) || !Arc::ptr_eq(&origin, &witness.origin)
                {
                    return Err(PermissionError::CorruptRecord);
                }
                (observation, witness)
            }
            PermissionReadyDispatchCapture::LegacyOnce { .. }
            | PermissionReadyDispatchCapture::NewUse { .. } => {
                return Err(PermissionError::UnsupportedProfile)
            }
        };
        runtime_fence_parts(
            store,
            &observation.resource.thread,
            &observation.resource.execution,
            &target.execution_id,
            fence,
        )?;
        runtime_ready_cancellation_target(
            &observation.resource.thread,
            &observation.resource.execution,
            target,
        )?;
        if profile == PermissionRuntimeInputProfile::NormalReadyCancellation {
            let snapshot = &observation.resource.execution.snapshot;
            let history_bytes = snapshot
                .turn_history
                .iter()
                .map(|turn| turn.as_str().len())
                .sum::<usize>();
            if snapshot.turn_id.as_str().len() > profile.turn_id_bytes()
                || history_bytes > profile.turn_history_bytes()
            {
                return Err(PermissionError::UnsupportedProfile);
            }
        }
        let lane = runtime_compat::begin_scope_history(budget)?;
        let current = runtime_compat::RuntimeCurrentScope {
            thread_id: &observation.resource.thread.id,
            thread_generation: &observation.resource.thread.generation,
            origin: observation
                .resource
                .thread
                .origin
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?,
            application: identity,
            route: &definition.route,
            definition,
            effect_id: &registration.effect_id,
            tool_id: &registration.tool_id,
            tool_revision: &registration.tool_revision,
            policy_revision: &context.policy_revision,
            constraint_revision: &registration.constraint_revision,
        };
        let (scope, same) = lane.measure_current_scope_and_equal(
            current,
            witness.decision.scope(),
            witness.facts.actual[4],
        )?;
        if !same || !profile.cancellation_history(scope, (670, 55, 6), 2_070) {
            return Err(PermissionError::InvalidCatalog);
        }
        budget.mark_runtime_ready_cancellation_verified(&origin)?;
        budget.runtime_prepaid_ready_cancellation_fixed(
            READY_CANCEL_C4_BYTES,
            READY_CANCEL_C4_NODES,
        )?;
        let r2_selector = PermissionRuntimeSelector {
            origin: &origin,
            mode: PermissionRuntimeReadMode::Matched(PermissionRuntimeMatchedReadRequest {
                role: PermissionRuntimeMatchedReadRole::ObserveR2Refence,
                requested_execution: &target.execution_id,
                context: &invocation_context,
                definition,
                expected_resource: &observation.resource,
                expected_facts: &observation.facts,
                task_id: Some(&task.spec.id),
                claimed: None,
            }),
        };
        let r2 =
            runtime_observed(store.capture_permission_ready_cancellation(&r2_selector, budget)?)?;
        #[allow(clippy::drop_non_drop)]
        drop(r2_selector);
        if !Arc::ptr_eq(&origin, &r2.origin) {
            return Err(PermissionError::CorruptRecord);
        }
        runtime_fence_parts(
            store,
            &r2.resource.thread,
            &r2.resource.execution,
            &target.execution_id,
            fence,
        )?;
        runtime_ready_cancellation_target(&r2.resource.thread, &r2.resource.execution, target)?;
        if r2.resource.thread.origin.as_ref() != Some(&context.actor) {
            return Err(ApprovalError::Unauthorized.into());
        }
        authority.authorize_cancel(context, &r2.resource, target)?;
        budget.runtime_reserve(READY_CANCEL_C5_BYTES, READY_CANCEL_C5_NODES)?;
        runtime_normal_input_owners(profile, observation.facts.actual[0], 8, budget)?;
        let candidate_plan = b4_kernel::transition(
            &observation.resource.execution.snapshot,
            KernelCommand::Cancel {
                turn_id: observation.resource.execution.snapshot.turn_id.clone(),
                owner_generation: observation.resource.execution.snapshot.owner_generation,
            },
            request.facts.clone(),
        )?;
        if !crate::validate_commit(
            &observation.resource.execution.snapshot,
            fence.execution,
            &candidate_plan,
            store.owner_generation(),
        )? {
            return Err(PermissionError::CorruptRecord);
        }
        let post = runtime_compat::capture_dispatch_snapshot(&candidate_plan.snapshot, budget)?;
        runtime_normal_input_owners(profile, post[0], 8, budget)?;
        budget.runtime_reserve(READY_CANCEL_C6_BYTES, READY_CANCEL_C6_NODES)?;
        let prepared = PermissionPreparedReadyCancellation {
            capture: &captured,
            requested_context: context,
            target,
            fence,
            facts: request.facts.clone(),
            candidate_plan,
            candidate_post_facts: PermissionRuntimeCandidatePostFacts { actual: post },
            catalog: PermissionRuntimeCatalogFacts {
                actual,
                catalog,
                definition,
                registration,
                origin: Arc::clone(&origin),
            },
            origin: Arc::clone(&origin),
        };
        budget.mark_runtime_ready_cancellation_prepared(&origin)?;
        let execution = store.commit_permission_ready_cancellation(&prepared, budget)?;
        project(&observation.resource, execution, budget)
    });
    budget.runtime_finish(result)
}

// The default port checks a genuine private seal, then preserves its own refusal.
// No writer is admitted and no cancellation phase succeeds on this boundary.
pub(super) fn refuse_permission_ready_cancellation(
    prepared: &PermissionPreparedReadyCancellation<'_, '_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<StoredExecution, PermissionError> {
    let result = (|| {
        budget.check_runtime_ready_cancellation_prepared(&prepared.origin)?;
        if !Arc::ptr_eq(&prepared.origin, &prepared.catalog.origin)
            || !Arc::ptr_eq(&prepared.origin, runtime_capture_origin(prepared.capture))
        {
            return Err(PermissionError::CorruptRecord);
        }
        match prepared.capture {
            PermissionReadyDispatchCapture::NewHuman { observation, .. } => {
                runtime_ready_cancellation_target(
                    &observation.resource.thread,
                    &observation.resource.execution,
                    prepared.target,
                )?;
                if observation.resource.thread.origin.as_ref()
                    != Some(&prepared.requested_context.actor)
                    || prepared.fence.execution != prepared.target.execution_fence
                    || prepared.fence.resource_revision != prepared.target.resource_revision
                    || prepared.candidate_plan.expected_revision
                        != prepared.fence.execution.revision
                    || prepared.candidate_plan.snapshot.execution_id != prepared.target.execution_id
                    || prepared.candidate_plan.snapshot.owner_generation
                        != prepared.fence.execution.owner_generation
                    || prepared.candidate_plan.snapshot.updated_at_ms != prepared.facts.now_ms
                    || !budget
                        .runtime_input_profile()
                        .cancelled_snapshot(prepared.candidate_post_facts.actual[0])
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            PermissionReadyDispatchCapture::LegacyOnce { .. }
            | PermissionReadyDispatchCapture::NewUse { .. } => {
                return Err(PermissionError::UnsupportedProfile)
            }
        }
        Err(PermissionError::UnsupportedProfile)
    })();
    budget.runtime_finish(result)
}

// Unconditional concrete layout obligations; actual compiler success is Root-only.
const _: () = assert!(size_of::<PermissionPreparedReadyCancellation<'static, 'static>>() <= 3_072);
const _: () = assert!(
    size_of::<Result<PermissionPreparedReadyCancellation<'static, 'static>, PermissionError>>()
        <= 3_072
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionPreparedReadyCancellation<'static, 'static>,
        >,
    >() <= 3_072
);
const _: () = assert!(size_of::<super::PermissionReadyCancellationRequest<'static>>() <= 32);
const _: () = assert!(size_of::<PermissionRuntimeOperationKind>() <= 16);
const _: () = assert!(size_of::<PermissionRuntimePhase>() <= 16);
const _: () = assert!(size_of::<PermissionRuntimeState>() <= 64);
const _: () = assert!(size_of::<Result<StoredExecution, PermissionError>>() <= 2_048);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, StoredExecution>,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<(
        &ApprovalContext,
        &ApprovalResource,
        &crate::RunCancellationTarget
    )>() <= 24
);
const _: () = assert!(size_of::<Result<bool, crate::StoreError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, crate::StoreError>, bool>>()
        <= 128
);

// Exact pure cancellation q/catalog carriers; no Result-to-Flow layout inference.
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            [PermissionRuntimeMeasure; 3],
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            [PermissionRuntimeMeasure; 4],
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<(
        &'static Definition,
        &'static ToolBindingRegistration,
        [PermissionRuntimeMeasure; 4]
    )>() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (
                &'static Definition,
                &'static ToolBindingRegistration,
                [PermissionRuntimeMeasure; 4]
            ),
        >,
    >() <= 256
);

// Exact cancellation-only task iterator interfaces; declarations do not price work.
const _: () = assert!(size_of::<std::slice::IterMut<'static, b4_contract::TaskRecord>>() <= 32);
const _: () = assert!(size_of::<std::slice::Iter<'static, b4_contract::TaskRecord>>() <= 32);
const _: () = assert!(size_of::<Option<&'static mut b4_contract::TaskRecord>>() <= 8);

// Exact const-evaluation and callback argument tuples used by cancellation task traversals.
const _: () = assert!(size_of::<(*const (), usize, usize)>() <= 128);
const _: () = assert!(size_of::<(&'static b4_contract::TaskRecord,)>() <= 128);

// Exact borrowed State eager extraction carriers; declarations do not price helpers.
const _: () = assert!(size_of::<Result<&'static PermissionRuntimeState, PermissionError>>() <= 128);
const _: () =
    assert!(size_of::<Result<&'static mut PermissionRuntimeState, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionRuntimeState,
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static mut PermissionRuntimeState,
        >,
    >() <= 128
);

// Dedicated cancellation route proposals: every amount awaits the exact
// independent normal/one-origin invoice before adoption or execution.
const READY_CANCEL_ADAPTER_INGRESS_BYTES: usize = 32_768;
const READY_CANCEL_ADAPTER_INGRESS_NODES: usize = 512;
const READY_CANCEL_C1_RETENTION_BYTES: usize = 107_176;
const READY_CANCEL_C1_RETENTION_NODES: usize = 682;
const READY_CANCEL_C1_GEOMETRY_BYTES: usize = 24_576;
const READY_CANCEL_C1_GEOMETRY_NODES: usize = 1_024;
const READY_CANCEL_C1_FORWARD_BYTES: usize = 16_384;
const READY_CANCEL_C1_FORWARD_NODES: usize = 256;
const READY_CANCEL_I1_ENTRY_BYTES: usize = 98_304;
const READY_CANCEL_I1_ENTRY_NODES: usize = 1_024;
const READY_CANCEL_I1_MATCH_BYTES: usize = 131_072;
const READY_CANCEL_I1_MATCH_NODES: usize = 2_048;
const READY_CANCEL_I1_KERNEL_BYTES: usize = 229_376;
const READY_CANCEL_I1_KERNEL_NODES: usize = 8_192;
const READY_CANCEL_I1_OUTPUT_BYTES: usize = 65_536;
const READY_CANCEL_I1_OUTPUT_NODES: usize = 512;
const READY_CANCEL_I1_SQL_BYTES: usize = 32_768;
const READY_CANCEL_I1_SQL_NODES: usize = 512;
const READY_CANCEL_C1_ENVELOPE_BYTES: usize = READY_CANCEL_C1_RETENTION_BYTES
    + READY_CANCEL_C1_GEOMETRY_BYTES
    + READY_CANCEL_C1_FORWARD_BYTES
    + READY_CANCEL_I1_ENTRY_BYTES
    + READY_CANCEL_I1_MATCH_BYTES
    + READY_CANCEL_I1_KERNEL_BYTES
    + READY_CANCEL_I1_OUTPUT_BYTES
    + READY_CANCEL_I1_SQL_BYTES;
const READY_CANCEL_C1_ENVELOPE_NODES: usize = READY_CANCEL_C1_RETENTION_NODES
    + READY_CANCEL_C1_GEOMETRY_NODES
    + READY_CANCEL_C1_FORWARD_NODES
    + READY_CANCEL_I1_ENTRY_NODES
    + READY_CANCEL_I1_MATCH_NODES
    + READY_CANCEL_I1_KERNEL_NODES
    + READY_CANCEL_I1_OUTPUT_NODES
    + READY_CANCEL_I1_SQL_NODES;

/// Closed scoped receiver for genuine cancellation C1 sensitivity.
pub struct PermissionReadyCancellationSensitiveRead<'read, 'budget, 'selector, 'input> {
    session: &'read mut PermissionRuntimeReadSession<'budget, 'selector, 'input>,
}
impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    pub fn ready_cancellation_sensitive<'read>(
        &'read mut self,
    ) -> Result<
        PermissionReadyCancellationSensitiveRead<'read, 'budget, 'selector, 'input>,
        PermissionError,
    > {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if !self.admission.is_ready_cancellation_c1() {
                return Err(PermissionError::UnsupportedProfile);
            }
            if self.stage != 7 || !self.is_sensitive() || self.resource_facts.is_none() {
                return Err(PermissionError::CorruptRecord);
            }
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)?;
        Ok(PermissionReadyCancellationSensitiveRead { session: self })
    }
}
impl<'read, 'budget, 'selector, 'input>
    PermissionReadyCancellationSensitiveRead<'read, 'budget, 'selector, 'input>
{
    pub fn before_sql(
        &mut self,
        statement: PermissionRuntimeReadSql,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_hook_mut::<_, _, 64>(|| {
            self.session
                .admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if !self.session.is_sensitive() {
                return Err(PermissionError::CorruptRecord);
            }
            let code = self.session.stage / 4;
            let next =
                match statement {
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Acceptances)
                        if code == 10 && !self.session.complete_flags[0] =>
                    {
                        11
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Decisions)
                        if code == 11
                            && self.session.complete_flags[0]
                            && !self.session.complete_flags[1] =>
                    {
                        12
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Uses)
                        if code == 12
                            && self.session.complete_flags[1]
                            && !self.session.complete_flags[2] =>
                    {
                        13
                    }
                    PermissionRuntimeReadSql::AcceptancePayload
                        if code == 13 && self.session.is_new_human()? =>
                    {
                        // W is paid BEFORE the first actual retained Human raw/owner.
                        self.session
                            .admission
                            .budget_mut()
                            .runtime_reserve(66_004, 326)?;
                        // Separate bounded temporary-journal Vec allocations/moves/controls.
                        self.session
                            .admission
                            .budget_mut()
                            .runtime_prepaid_ready_cancellation_fixed(4_096, 128)?;
                        20
                    }
                    PermissionRuntimeReadSql::DecisionPayload
                        if code == 20
                            && self.session.acceptance.is_some()
                            && self.session.stage % 4 == 2 =>
                    {
                        21
                    }
                    PermissionRuntimeReadSql::GrantHeaders
                        if self.session.complete_flags[2]
                            && !self.session.complete_flags[3]
                            && ((code == 13 && !self.session.is_new_human()?)
                                || (code == 21
                                    && self.session.decision.is_some()
                                    && self.session.stage % 4 == 2)) =>
                    {
                        30
                    }
                    PermissionRuntimeReadSql::GrantPayload
                        if code == 30
                            && self.session.grant_header.is_some()
                            && self.session.grant.is_none() =>
                    {
                        if !self.session.is_new_human()? {
                            // Independent actual LegacyOnce W, never Human A/D basis.
                            self.session
                                .admission
                                .budget_mut()
                                .runtime_prepaid_ready_cancellation_fixed(57_196, 483)?;
                            // Separate temporary and final Legacy collection allocations/moves/controls.
                            self.session
                                .admission
                                .budget_mut()
                                .runtime_prepaid_ready_cancellation_fixed(16_384, 512)?;
                        }
                        31
                    }
                    PermissionRuntimeReadSql::OldAcceptanceHeaders
                        if code == 30
                            && self.session.complete_flags[3]
                            && !self.session.complete_flags[4] =>
                    {
                        40
                    }
                    PermissionRuntimeReadSql::OldAcceptancePayload
                        if code == 40
                            && self.session.headers[0].is_some()
                            && !self.session.is_new_human()? =>
                    {
                        41
                    }
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 40
                        && self.session.complete_flags[4]
                        && !self.session.complete_flags[5] =>
                    {
                        50
                    }
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 50 && self.session.journal_header.is_some() => 51,
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 51
                        && self.session.complete_flags[5]
                        && !self.session.complete_flags[6] =>
                    {
                        60
                    }
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 60 && self.session.journal_header.is_some() => 61,
                    PermissionRuntimeReadSql::CommitRead
                        if self.session.complete_flags[7] && code == 61 =>
                    {
                        self.session.require_completed_capture()?;
                        90
                    }
                    _ => return Err(PermissionError::CorruptRecord),
                };
            // Getter receiver loan closes before the mutable Admission loan.
            let execution_id = self.session.execution_id();
            let thread = if let Some((thread, _, _)) = &self.session.thread {
                &thread.id
            } else {
                match &self.session.completed_capture {
                    Ok(PermissionRuntimeCapture {
                        inner:
                            PermissionRuntimeCaptureKind::Ready(
                                PermissionReadyDispatchCapture::NewHuman { observation, .. },
                            ),
                    }) => &observation.resource.thread.id,
                    Ok(PermissionRuntimeCapture {
                        inner:
                            PermissionRuntimeCaptureKind::Ready(
                                PermissionReadyDispatchCapture::LegacyOnce { view, .. },
                            ),
                    }) => &view.thread.id,
                    _ => return Err(PermissionError::CorruptRecord),
                }
            };
            let turn = match statement {
                PermissionRuntimeReadSql::AcceptancePayload
                | PermissionRuntimeReadSql::OldAcceptancePayload => Some(
                    self.session.headers[0]
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .primary
                        .as_str(),
                ),
                _ => None,
            };
            let pid = if matches!(statement, PermissionRuntimeReadSql::DecisionPayload) {
                Some(
                    self.session.headers[1]
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .primary
                        .as_str(),
                )
            } else {
                None
            };
            let interrupt = if matches!(statement, PermissionRuntimeReadSql::GrantPayload) {
                Some(
                    &self
                        .session
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .tasks[0]
                        .interrupt
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .id,
                )
            } else {
                None
            };
            let journal = match statement {
                PermissionRuntimeReadSql::JournalHeader(point) => {
                    let revision = self
                        .session
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .revision;
                    let revision = match point {
                        PermissionRuntimeJournalPoint::Park => revision
                            .checked_sub(1)
                            .ok_or(PermissionError::CorruptRecord)?,
                        PermissionRuntimeJournalPoint::Resume => revision,
                        PermissionRuntimeJournalPoint::Dispatch => {
                            return Err(PermissionError::UnsupportedProfile)
                        }
                    };
                    Some((0, revision)) // Closed Header variant uses only the actual revision.
                }
                PermissionRuntimeReadSql::JournalPayload(_) => {
                    let header = self
                        .session
                        .journal_header
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?;
                    Some((
                        u64::try_from(header.sequence).map_err(
                            runtime_checked_read_sequence_mapper(|_| {
                                PermissionError::CorruptRecord
                            }),
                        )?,
                        header.revision,
                    ))
                }
                _ => None,
            };
            validation::runtime_phase::runtime_read_before_sql(
                self.session.admission.budget_mut(),
                &statement,
                execution_id,
                thread,
                turn,
                pid,
                interrupt,
                journal,
            )?;
            if let Some(interrupt) = interrupt {
                validation::runtime_phase::runtime_admit_reference_grant(
                    self.session.admission.budget_mut(),
                    execution_id,
                    interrupt,
                )?;
            }
            self.session.stage = if next == 90 { 90 } else { next * 4 };
            Ok(())
        });
        self.session.admission.budget_mut().runtime_finish(result)
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        self.session.before_row()
    }
    pub fn thread_id(&self) -> Result<&b4_contract::ThreadId, PermissionError> {
        self.session.thread_id()
    }
    pub fn admit_new_header(
        &mut self,
        cells: PermissionNewHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.session.admit_new_header(cells)
    }
    pub fn end_new_range(&mut self, group: PermissionNewGroup) -> Result<(), PermissionError> {
        self.session.end_new_range(group)
    }
    pub fn is_new_human(&self) -> Result<bool, PermissionError> {
        self.session.is_new_human()
    }
    pub fn acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.session.acceptance_query_key()
    }
    pub fn decision_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.session.decision_query_key()
    }
    pub fn capture_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.session.capture_acceptance_payload(raw)
    }
    pub fn capture_decision_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.session.capture_decision_payload(raw)
    }
    pub fn admit_grant_header(
        &mut self,
        cells: PermissionGrantHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.session.admit_grant_header(cells)
    }
    pub fn grant_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.session.grant_query_key()
    }
    pub fn capture_grant_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.session.capture_grant_payload(raw)
    }
    pub fn end_grant_range(&mut self) -> Result<(), PermissionError> {
        self.session.end_grant_range()
    }
    pub fn admit_old_acceptance_header(
        &mut self,
        cells: PermissionAcceptanceHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.session.admit_old_acceptance_header(cells)
    }
    pub fn old_acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.session.old_acceptance_query_key()
    }
    pub fn capture_old_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.session.capture_old_acceptance_payload(raw)
    }
    pub fn end_old_acceptance_range(&mut self) -> Result<(), PermissionError> {
        self.session.end_old_acceptance_range()
    }
    pub fn journal_revision(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<u64, PermissionError> {
        self.session.journal_revision(point)
    }
    pub fn admit_journal_header(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        cells: PermissionProjectionJournalHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.session.admit_journal_header(point, cells)
    }
    pub fn journal_query_key(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(i64, usize), PermissionError> {
        self.session.journal_query_key(point)
    }
    pub fn capture_journal_payload(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        self.session.capture_journal_payload(point, raw)
    }
    pub fn end_journal_range(
        &mut self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(), PermissionError> {
        self.session.end_journal_range(point)
    }
    pub fn end_evidence(&mut self) -> Result<(), PermissionError> {
        self.session.end_evidence()
    }
    pub fn begin_sensitive(&mut self) -> Result<(), PermissionError> {
        self.session.begin_sensitive()
    }
}

/// Closed terminal write controls for the actual cancellation domain.
#[derive(Clone, Copy)]
pub enum PermissionReadyCancellationWriteSql {
    BeginImmediate,
    SnapshotCas,
    ThreadCas,
    InsertCancelJournal,
    Commit,
}
pub struct PermissionRuntimeReadyCancellationWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    admission: validation::RuntimeDispatchAdmission<'budget>,
    prepared: &'prepared PermissionPreparedReadyCancellation<'capture, 'catalog>,
    origin: Arc<PermissionRuntimeOrigin>,
    stage: usize,
    metadata_checked: bool,
    current_owner: u64,
    execution: Option<(
        StoredExecution,
        [Option<PermissionRuntimeMeasure>; 5],
        [usize; 4],
    )>,
    thread: Option<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>,
    match_storage: Option<PermissionRuntimeFreshMatchStorage>,
    checked_resource_facts: Option<PermissionRuntimeResourceFacts>,
    complete_flags: [bool; 8],
}
pub struct PermissionRuntimeReadyCancellationWork<'work, 'budget, 'prepared, 'capture, 'catalog>
where
    'budget: 'work,
    'prepared: 'work,
    'capture: 'prepared,
    'catalog: 'prepared,
{
    session: &'work mut PermissionRuntimeReadyCancellationWriteSession<
        'budget,
        'prepared,
        'capture,
        'catalog,
    >,
    output: StoredExecution,
    post_thread: ThreadRecord,
    snapshot_payload: String,
    journal_payload: String,
    before_scalars: PermissionRuntimeBeforeScalars,
    sql_stage: usize,
    binding_execution_id: &'capture str,
    binding_thread_id: &'capture str,
    binding_generation: &'capture str,
}

fn runtime_check_ready_cancellation_writer_prepared(
    prepared: &PermissionPreparedReadyCancellation<'_, '_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let result = runtime_checked_entered_iife(|| {
        budget.check_runtime_ready_cancellation_prepared(&prepared.origin)?;
        if !Arc::ptr_eq(&prepared.origin, &prepared.catalog.origin)
            || !Arc::ptr_eq(&prepared.origin, runtime_capture_origin(prepared.capture))
        {
            return Err(PermissionError::CorruptRecord);
        }
        match prepared.capture {
            PermissionReadyDispatchCapture::NewHuman { observation, .. } => {
                runtime_ready_cancellation_target(
                    &observation.resource.thread,
                    &observation.resource.execution,
                    prepared.target,
                )?;
                if observation.resource.thread.origin.as_ref()
                    != Some(&prepared.requested_context.actor)
                    || prepared.fence.execution != prepared.target.execution_fence
                    || prepared.fence.resource_revision != prepared.target.resource_revision
                    || prepared.candidate_plan.expected_revision
                        != prepared.fence.execution.revision
                    || prepared.candidate_plan.snapshot.execution_id != prepared.target.execution_id
                    || prepared.candidate_plan.snapshot.owner_generation
                        != prepared.fence.execution.owner_generation
                    || prepared.candidate_plan.snapshot.updated_at_ms != prepared.facts.now_ms
                    || !budget
                        .runtime_input_profile()
                        .cancelled_snapshot(prepared.candidate_post_facts.actual[0])
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            PermissionReadyDispatchCapture::LegacyOnce { .. }
            | PermissionReadyDispatchCapture::NewUse { .. } => {
                return Err(PermissionError::UnsupportedProfile)
            }
        }
        Ok(())
    });
    budget.runtime_finish(result)
}

// Only the selected Ready cancellation common and Work hooks use this interface.
// Qualify each actual closure environment before its consuming FnOnce call.
fn runtime_checked_ready_cancellation_hook<F, U>(callback: F) -> Result<U, PermissionError>
where
    F: FnOnce() -> Result<U, PermissionError>,
{
    const { assert!(std::mem::size_of::<F>() <= 512) };
    callback()
}

impl PermissionEvidenceBudget {
    pub fn runtime_ready_cancellation_write<'budget, 'prepared, 'capture, 'catalog>(
        &'budget mut self,
        prepared: &'prepared PermissionPreparedReadyCancellation<'capture, 'catalog>,
    ) -> Result<
        PermissionRuntimeReadyCancellationWriteSession<'budget, 'prepared, 'capture, 'catalog>,
        PermissionError,
    >
    where
        'capture: 'prepared,
        'catalog: 'prepared,
    {
        self.require_standard_account()?;
        // C1's dedicated envelope precedes this real backend entry and pays
        // its new full-owner interfaces and every denied future hook.
        self.runtime_check_ready_cancellation()?;
        runtime_check_ready_cancellation_writer_prepared(prepared, self)?;
        self.runtime_prepaid_ready_cancellation_fixed(64, 1)?;
        let origin = Arc::clone(&prepared.origin);
        let admission = self.admit_runtime_ready_cancellation_write()?;
        Ok(PermissionRuntimeReadyCancellationWriteSession {
            admission,
            prepared,
            origin,
            stage: 0,
            metadata_checked: false,
            current_owner: prepared.fence.execution.owner_generation,
            execution: None,
            thread: None,
            match_storage: None,
            checked_resource_facts: None,
            complete_flags: [false; 8],
        })
    }
}

impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeReadyCancellationWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    pub fn execution_id(&self) -> &'capture b4_contract::ExecutionId {
        &self.prepared.target.execution_id
    }
    pub fn before_resource_sql(
        &mut self,
        statement: PermissionResourceSql,
        key: Option<PermissionResourceKey<'_>>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let expected = match statement {
                PermissionResourceSql::BeginDeferred => 0,
                PermissionResourceSql::Metadata => 1,
                PermissionResourceSql::ExecutionLengths => 2,
                PermissionResourceSql::ExecutionPayload => 3,
                PermissionResourceSql::ThreadLengths => 4,
                PermissionResourceSql::ThreadPayload => 5,
                PermissionResourceSql::CommitRead => 7,
            };
            if matches!(
                statement,
                PermissionResourceSql::BeginDeferred | PermissionResourceSql::CommitRead
            ) {
                self.check_read_transaction(statement)?;
            }
            let budget = self.admission.budget_mut();
            budget.runtime_check_ready_cancellation()?;
            if self.stage != expected {
                return Err(PermissionError::CorruptRecord);
            }
            validation::runtime_phase::runtime_resource_before_sql(budget, statement, key)?;
            self.stage = if matches!(statement, PermissionResourceSql::CommitRead) {
                90
            } else {
                expected + 1
            };
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn check_metadata(
        &mut self,
        version: i64,
        checksum: &[u8],
        owner: &[u8],
        expected_owner: u64,
        schema: &[u8],
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 2 || self.metadata_checked {
                return Err(PermissionError::CorruptRecord);
            }
            validation::runtime_phase::runtime_resource_check_metadata(
                self.admission.budget_mut(),
                version,
                checksum,
                owner,
                expected_owner,
                schema,
            )?;
            // The supplied owner is the actual backend metadata owner already
            // decoded and validated by the original helper, not a cached fence.
            if expected_owner != self.prepared.fence.execution.owner_generation {
                return Err(crate::StoreError::OwnerConflict.into());
            }
            self.current_owner = expected_owner;
            self.metadata_checked = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_execution_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 3 || !self.metadata_checked {
                return Err(PermissionError::CorruptRecord);
            }
            validation::runtime_phase::runtime_admit_execution_lengths(
                self.admission.budget_mut(),
                thread,
                snapshot,
            )
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn capture_execution(
        &mut self,
        cells: PermissionExecutionCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 4 || !self.metadata_checked || self.execution.is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            let scalars = [
                cells.thread_id.len(),
                cells.snapshot.len(),
                cells.revision.len(),
                cells.owner_generation.len(),
            ];
            let (execution, facts) =
                runtime_compat::capture_resource_execution(cells, self.admission.budget_mut())?;
            self.execution = Some((execution, facts.into_actual(), scalars));
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn thread_query_id(&mut self) -> Result<b4_contract::ThreadId, PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 4 {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if self
                .admission
                .budget_mut()
                .runtime_ready_cancellation_has_fixed_purchase()
            {
                self.admission.budget_mut().runtime_reserve(
                    runtime_add(128, runtime_mul(6, execution.0.thread_id.as_str().len())?)?,
                    1,
                )?;
            }
            let owned = runtime_owned(execution.0.thread_id.as_str(), self.admission.budget_mut())?;
            b4_contract::ThreadId::new(owned).map_err(|_| PermissionError::CorruptRecord)
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_thread_lengths(
        &mut self,
        id: usize,
        active: usize,
        generation: usize,
        metadata: usize,
        origin: usize,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 5 {
                return Err(PermissionError::CorruptRecord);
            }
            validation::runtime_phase::runtime_admit_thread_lengths(
                self.admission.budget_mut(),
                id,
                active,
                generation,
                metadata,
                origin,
            )
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn capture_thread(
        &mut self,
        cells: PermissionThreadCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 6 || self.thread.is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            let scalars = [
                cells.id.as_str().len(),
                cells.metadata.len(),
                cells.origin.map_or(0, <[u8]>::len),
                cells.active_execution_id.map_or(0, <[u8]>::len),
            ];
            let (thread, measured) =
                runtime_compat::capture_resource_thread(cells, self.admission.budget_mut())?;
            let requested_execution = self.execution_id();
            let execution = self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            validation::runtime_phase::runtime_resource_check_linkage(
                self.admission.budget_mut(),
                requested_execution,
                &thread,
                &execution.0,
            )?;
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(512, 32)?; // Fixed qualification metadata is prepaid for cancellation.
            let facts = runtime_first_resource_facts(
                &execution.0,
                &execution.1,
                execution.2,
                &thread,
                measured,
                scalars,
                self.admission.budget_mut(),
            )?;
            self.thread = Some((thread, measured, scalars));
            self.checked_resource_facts = Some(facts);
            self.stage = 7;
            self.check_expected_resource()?;
            self.build_completed_resource()?;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn thread_id(&self) -> Result<&b4_contract::ThreadId, PermissionError> {
        self.thread
            .as_ref()
            .map(|t| &t.0.id)
            .ok_or(PermissionError::CorruptRecord)
    }
    fn check_read_transaction(&mut self, _: PermissionResourceSql) -> Result<(), PermissionError> {
        Err(PermissionError::CorruptRecord)
    }
    fn build_completed_resource(&mut self) -> Result<(), PermissionError> {
        Ok(())
    }
    fn check_expected_resource(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let (expected_execution, expected_thread, expected_facts) = match self.prepared.capture
            {
                PermissionReadyDispatchCapture::LegacyOnce {
                    view,
                    resource_facts,
                    ..
                } => (&view.execution, &view.thread, resource_facts),
                PermissionReadyDispatchCapture::NewHuman { observation, .. } => {
                    // Classified C1 W prepays references; complete BOTH Eq remains below.
                    (
                        &observation.resource.execution,
                        &observation.resource.thread,
                        &observation.facts,
                    )
                }
                PermissionReadyDispatchCapture::NewUse { .. } => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            let actual_execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let actual_thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let actual_facts = self
                .checked_resource_facts
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            runtime_resource_equal_parts(
                actual_execution,
                actual_thread,
                actual_facts,
                expected_execution,
                expected_thread,
                expected_facts,
                self.admission.budget_mut(),
            )?;
            runtime_ready_cancellation_target(
                actual_thread,
                actual_execution,
                self.prepared.target,
            )?;
            if actual_thread.origin.as_ref() != Some(&self.prepared.requested_context.actor) {
                return Err(ApprovalError::Unauthorized.into());
            }
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn before_begin_immediate(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 0 {
                return Err(PermissionError::CorruptRecord);
            }
            let execution_id = self.execution_id();
            let (thread, _, _) = runtime_ready_parts(self.prepared.capture);
            let statement = PermissionReadyCancellationWriteSql::BeginImmediate;
            validation::runtime_phase::runtime_ready_cancellation_write_before_sql(
                self.admission.budget_mut(),
                &statement,
                execution_id,
                &thread.id,
                self.prepared.fence.execution.revision,
            )?;
            self.stage = 1;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn begin_sensitive(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage != 7
                || self.checked_resource_facts.is_none()
                || self.match_storage.is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
            // Fresh scalar storage only. No decoded record, placeholder q,
            // forged ReadSession or second account admission is constructed.
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(1_536, 24)?;
            self.admission.budget_mut().runtime_reserve(256, 1)?;
            let mut headers = Vec::new();
            headers
                .try_reserve_exact(2)
                .map_err(|_| PermissionError::LimitExceeded)?;
            self.match_storage = Some(PermissionRuntimeFreshMatchStorage {
                headers: [None, None],
                raw: std::array::from_fn(|_| Vec::new()),
                journal_headers: headers,
                range_counts_and_flags: [0; 8],
            });
            self.stage = 40;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            let code = self.stage / 4;
            let attempts = self.stage % 4;
            if matches!(code, 11 | 12 | 13 | 30 | 40 | 50 | 60) {
                self.admission
                    .budget_mut()
                    .runtime_prepaid_ready_cancellation_fixed(4_096, 32)?;
                if attempts == 3 {
                    return Err(PermissionError::CorruptRecord);
                }
                self.stage += 1;
            } else if matches!(code, 20 | 21 | 31 | 41 | 51 | 61) {
                self.admission
                    .budget_mut()
                    .runtime_check_ready_cancellation()?;
                if attempts == 2 {
                    return Err(PermissionError::CorruptRecord);
                }
                self.stage += 1;
                if attempts == 1 && code == 31 {
                    self.stage = 30 * 4;
                }
                if attempts == 1 && code == 41 {
                    self.stage = 40 * 4;
                }
            } else {
                return Err(PermissionError::CorruptRecord);
            }
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn before_sql(
        &mut self,
        statement: PermissionRuntimeReadSql,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let code = self.stage / 4;
            let Some(storage) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            let next =
                match statement {
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Acceptances)
                        if code == 10 && !self.complete_flags[0] =>
                    {
                        11
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Decisions)
                        if code == 11 && self.complete_flags[0] && !self.complete_flags[1] =>
                    {
                        12
                    }
                    PermissionRuntimeReadSql::NewHeaders(PermissionNewGroup::Uses)
                        if code == 12 && self.complete_flags[1] && !self.complete_flags[2] =>
                    {
                        13
                    }
                    PermissionRuntimeReadSql::AcceptancePayload
                        if code == 13
                            && self.is_new_human()?
                            && storage.headers[0].is_some()
                            && storage.range_counts_and_flags[7] & 16 == 0 =>
                    {
                        20
                    }
                    PermissionRuntimeReadSql::DecisionPayload
                        if code == 20
                            && storage.headers[1].is_some()
                            && storage.range_counts_and_flags[7] & 16 != 0
                            && storage.range_counts_and_flags[7] & 32 == 0 =>
                    {
                        21
                    }
                    PermissionRuntimeReadSql::GrantHeaders
                        if code == 21
                            && storage.range_counts_and_flags[7] & 48 == 48
                            && storage.headers.iter().all(Option::is_none) =>
                    {
                        30
                    }
                    PermissionRuntimeReadSql::GrantHeaders
                        if code == 13
                            && self.complete_flags[2]
                            && !self.complete_flags[3]
                            && !self.is_new_human()? =>
                    {
                        30
                    }
                    PermissionRuntimeReadSql::GrantPayload
                        if code == 30
                            && storage.headers[1].is_some()
                            && storage.range_counts_and_flags[7] & 1 == 0 =>
                    {
                        31
                    }
                    PermissionRuntimeReadSql::OldAcceptanceHeaders
                        if code == 30 && self.complete_flags[3] && !self.complete_flags[4] =>
                    {
                        40
                    }
                    PermissionRuntimeReadSql::OldAcceptancePayload
                        if code == 40
                            && storage.headers[0].is_some()
                            && storage.range_counts_and_flags[7] & 2 == 0 =>
                    {
                        41
                    }
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 40 && self.complete_flags[4] && !self.complete_flags[5] => 50,
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Park,
                    ) if code == 50 && storage.journal_headers.len() == 1 => 51,
                    PermissionRuntimeReadSql::JournalHeader(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 51 && self.complete_flags[5] && !self.complete_flags[6] => 60,
                    PermissionRuntimeReadSql::JournalPayload(
                        PermissionRuntimeJournalPoint::Resume,
                    ) if code == 60 && storage.journal_headers.len() == 2 => 61,
                    _ => return Err(PermissionError::CorruptRecord),
                };
            let execution_id = self.execution_id();
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0
                .id;
            let Some(storage) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            let turn = if matches!(
                statement,
                PermissionRuntimeReadSql::OldAcceptancePayload
                    | PermissionRuntimeReadSql::AcceptancePayload
            ) {
                Some(
                    storage.headers[0]
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .primary
                        .as_str(),
                )
            } else {
                None
            };
            let interrupt = if matches!(statement, PermissionRuntimeReadSql::GrantPayload) {
                Some(
                    &self
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .tasks[0]
                        .interrupt
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .id,
                )
            } else {
                None
            };
            let journal = match statement {
                PermissionRuntimeReadSql::JournalHeader(point) => {
                    let revision = self
                        .execution
                        .as_ref()
                        .ok_or(PermissionError::CorruptRecord)?
                        .0
                        .snapshot
                        .revision;
                    Some((
                        0,
                        match point {
                            PermissionRuntimeJournalPoint::Park => revision
                                .checked_sub(1)
                                .ok_or(PermissionError::CorruptRecord)?,
                            PermissionRuntimeJournalPoint::Resume => revision,
                            PermissionRuntimeJournalPoint::Dispatch => {
                                return Err(PermissionError::UnsupportedProfile)
                            }
                        },
                    ))
                }
                PermissionRuntimeReadSql::JournalPayload(_) => {
                    let h = storage
                        .journal_headers
                        .last()
                        .ok_or(PermissionError::CorruptRecord)?;
                    Some((
                        u64::try_from(h.sequence).map_err(|_| PermissionError::CorruptRecord)?,
                        h.revision,
                    ))
                }
                _ => None,
            };
            validation::runtime_phase::runtime_read_before_sql(
                self.admission.budget_mut(),
                &statement,
                execution_id,
                thread,
                turn,
                if matches!(statement, PermissionRuntimeReadSql::DecisionPayload) {
                    Some(
                        storage.headers[1]
                            .as_ref()
                            .ok_or(PermissionError::CorruptRecord)?
                            .primary
                            .as_str(),
                    )
                } else {
                    None
                },
                interrupt,
                journal,
            )?;
            if let Some(interrupt) = interrupt {
                validation::runtime_phase::runtime_admit_reference_grant(
                    self.admission.budget_mut(),
                    execution_id,
                    interrupt,
                )?;
            }
            self.stage = next * 4;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_new_header(
        &mut self,
        cells: PermissionNewHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let index = runtime_group(cells.group);
            if self.stage / 4 != 11 + index || self.complete_flags[index] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            if cells.record_version != 1
                || cells.record_bytes == 0
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            match cells.group {
                PermissionNewGroup::Acceptances => {
                    if runtime_opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != execution.snapshot.execution_id.as_str()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.digest.is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    runtime_opaque(cells.primary)?;
                    let p = runtime_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    let c = runtime_u64(
                        cells
                            .committed_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )?;
                    if p.checked_add(1) != Some(c) || c > execution.snapshot.revision {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
                PermissionNewGroup::Decisions => {
                    if cells.execution_id.is_some()
                        || cells.secondary.is_some()
                        || cells.permission_id.is_some()
                        || cells.turn_id.is_some()
                        || cells.previous_revision.is_some()
                        || cells.committed_revision.is_some()
                        || !runtime_record_identity(cells.primary, b"b4pd_")
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    let raw = cells.digest.ok_or(PermissionError::CorruptRecord)?;
                    runtime_generation(raw)?;
                }
                PermissionNewGroup::Uses => {
                    if runtime_opaque(cells.execution_id.ok_or(PermissionError::CorruptRecord)?)?
                        != execution.snapshot.execution_id.as_str()
                        || cells.digest.is_some()
                        || cells.committed_revision.is_some()
                        || !runtime_record_identity(
                            cells.secondary.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pu_",
                        )
                        || !runtime_record_identity(
                            cells.permission_id.ok_or(PermissionError::CorruptRecord)?,
                            b"b4pd_",
                        )
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    runtime_opaque(cells.primary)?;
                    runtime_opaque(cells.turn_id.ok_or(PermissionError::CorruptRecord)?)?;
                    if runtime_u64(
                        cells
                            .previous_revision
                            .ok_or(PermissionError::CorruptRecord)?,
                    )? > execution.snapshot.revision
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
            }
            let Some(storage) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            storage.range_counts_and_flags[index] =
                runtime_add(storage.range_counts_and_flags[index], 1)?;
            if storage.range_counts_and_flags[index] > 2 {
                return Err(PermissionError::UnsupportedProfile);
            }
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) && index < 2
                && storage.range_counts_and_flags[index] == 1
            {
                let digest = cells
                    .digest
                    .map(|raw| {
                        <[u8; 64]>::try_from(raw).map_err(|_| PermissionError::CorruptRecord)
                    })
                    .transpose()?;
                storage.headers[index] = Some(PermissionRuntimeNewHeader {
                    primary: runtime_prepaid_owned(
                        runtime_opaque(cells.primary)?,
                        self.admission.budget_mut(),
                    )?,
                    digest,
                    previous: cells.previous_revision.map(runtime_u64).transpose()?,
                    committed: cells.committed_revision.map(runtime_u64).transpose()?,
                    bytes: cells.record_bytes,
                });
            }
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_new_range(&mut self, group: PermissionNewGroup) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let index = runtime_group(group);
            if self.stage / 4 != 11 + index || self.complete_flags[index] {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[index] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn is_new_human(&self) -> Result<bool, PermissionError> {
        if !self.complete_flags[..3].iter().all(|v| *v) {
            return Err(PermissionError::CorruptRecord);
        }
        let Some(storage) = &self.match_storage else {
            return Err(PermissionError::CorruptRecord);
        };
        let c = &storage.range_counts_and_flags;
        if matches!(
            self.prepared.capture,
            PermissionReadyDispatchCapture::NewHuman { .. }
        ) {
            if c[..3] != [1, 1, 0] {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            return Ok(true);
        }
        if c[..3] != [0, 0, 0] {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        Ok(false)
    }
    pub fn acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.human_payload_key(0, 20)
    }
    pub fn decision_query_key(&self) -> Result<(&str, usize), PermissionError> {
        self.human_payload_key(1, 21)
    }
    fn human_payload_key(
        &self,
        slot: usize,
        code: usize,
    ) -> Result<(&str, usize), PermissionError> {
        if !matches!(
            self.prepared.capture,
            PermissionReadyDispatchCapture::NewHuman { .. }
        ) || self.stage / 4 != code
        {
            return Err(PermissionError::CorruptRecord);
        }
        let Some(storage) = &self.match_storage else {
            return Err(PermissionError::CorruptRecord);
        };
        let header = storage.headers[slot]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.primary, header.bytes))
    }
    pub fn capture_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.capture_human_payload(0, 20, 16, raw)
    }
    pub fn capture_decision_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        self.capture_human_payload(1, 21, 32, raw)
    }
    fn capture_human_payload(
        &mut self,
        slot: usize,
        code: usize,
        bit: usize,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        let prepared = self.prepared;
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let witness = match prepared.capture {
                PermissionReadyDispatchCapture::NewHuman { witness, .. } => witness.as_ref(),
                _ => return Err(PermissionError::CorruptRecord),
            };
            if self.stage / 4 != code || self.stage % 4 != 1 {
                return Err(PermissionError::CorruptRecord);
            }
            let Some(storage) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            let header = storage.headers[slot]
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if header.bytes != raw.len() {
                return Err(PermissionError::CorruptRecord);
            }
            let expected_primary = if slot == 0 {
                witness.acceptance.turn_id().as_str()
            } else {
                witness.decision.permission_id().as_str()
            };
            let digest_bytes = if slot == 0 { 0 } else { 128 };
            self.admission.budget_mut().runtime_reserve(
                runtime_add(
                    runtime_add(header.primary.len(), expected_primary.len())?,
                    digest_bytes,
                )?,
                if slot == 0 { 2 } else { 4 },
            )?;
            let header_matches = if slot == 0 {
                header.primary == expected_primary
                    && header.digest.is_none()
                    && header.previous == Some(witness.acceptance.before_snapshot().revision)
                    && header.committed == Some(witness.acceptance.committed_revision())
            } else {
                header.primary == expected_primary
                    && header.previous.is_none()
                    && header.committed.is_none()
                    && header.digest.as_ref().is_some_and(|actual| {
                        actual.as_slice() == witness.decision.scope_digest().as_bytes()
                    })
            };
            if !header_matches || header.bytes != witness.raw[slot].len() {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            self.admission
                .budget_mut()
                .runtime_reserve(runtime_add(witness.raw[slot].len(), raw.len())?, 2)?;
            if witness.raw[slot].as_slice() != raw {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let Some(storage) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            storage.headers[slot] = None;
            storage.range_counts_and_flags[7] |= bit;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_grant_header(
        &mut self,
        cells: PermissionGrantHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage / 4 != 30 || self.complete_flags[3] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let task = &execution.snapshot.tasks[0];
            let pending = task
                .interrupt
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            let parking = runtime_u64(cells.parking_revision)?;
            let consumed = match cells.consumed {
                0 => false,
                1 => true,
                _ => return Err(PermissionError::CorruptRecord),
            };
            if runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.task_id)? != task.spec.id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
                || runtime_opaque(cells.interrupt_id)? != pending.id.as_str()
                || parking.checked_add(1) != Some(execution.snapshot.revision)
                || cells.record_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            let grant = if let PermissionReadyDispatchCapture::NewHuman { witness, .. } =
                self.prepared.capture
            {
                &witness.grant
            } else {
                let (view, _) = self.legacy_view()?;
                &view.grants[0]
            };
            if consumed != grant.consumed || parking != grant.task_parking_revision {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            let Some(storage) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            storage.range_counts_and_flags[3] = runtime_add(storage.range_counts_and_flags[3], 1)?;
            if storage.range_counts_and_flags[3] != 1 || storage.headers[1].is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            // Closed Legacy-only physical G header projection uses the existing
            // generic private scalar header slots; no A/D record is fabricated.
            storage.headers[1] = Some(PermissionRuntimeNewHeader {
                primary: runtime_prepaid_owned(
                    runtime_opaque(cells.interrupt_id)?,
                    self.admission.budget_mut(),
                )?,
                digest: None,
                previous: Some(parking),
                committed: Some(u64::from(consumed)),
                bytes: cells.record_bytes,
            });
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn grant_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 31 {
            return Err(PermissionError::CorruptRecord);
        }
        let Some(storage) = &self.match_storage else {
            return Err(PermissionError::CorruptRecord);
        };
        let header = storage.headers[1]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&header.primary, header.bytes))
    }
    fn match_legacy_raw(&mut self, slot: usize, raw: &[u8]) -> Result<(), PermissionError> {
        if let PermissionReadyDispatchCapture::NewHuman { witness, .. } = self.prepared.capture {
            let mapped = match slot {
                1 => 2,
                2 => 3,
                3 => 4,
                _ => return Err(PermissionError::CorruptRecord),
            };
            let expected = &witness.raw[mapped];
            self.admission
                .budget_mut()
                .runtime_reserve(runtime_add(expected.len(), raw.len())?, 2)?;
            if expected.as_slice() != raw {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            return Ok(());
        }
        let expected = match self.prepared.capture {
            PermissionReadyDispatchCapture::LegacyOnce { raw, .. } => &raw[slot],
            _ => return Err(PermissionError::CorruptRecord),
        };
        self.admission
            .budget_mut()
            .runtime_reserve(runtime_add(expected.len(), raw.len())?, 2)?;
        if expected.as_slice() != raw {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        Ok(())
    }
    pub fn capture_grant_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage / 4 != 31 {
                return Err(PermissionError::CorruptRecord);
            }
            let Some(storage) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            let header = storage.headers[1]
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != header.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            self.match_legacy_raw(1, raw)?;
            let Some(storage) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            storage.headers[1] = None;
            storage.range_counts_and_flags[7] |= 1;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_grant_range(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let Some(s) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            if self.stage / 4 != 30
                || self.complete_flags[3]
                || s.range_counts_and_flags[3] != 1
                || s.headers[1].is_some()
                || s.range_counts_and_flags[7] & 1 == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[3] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn admit_old_acceptance_header(
        &mut self,
        cells: PermissionAcceptanceHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage / 4 != 40 || self.complete_flags[4] {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let turn = runtime_opaque(cells.turn_id)?;
            let previous = runtime_u64(cells.previous_revision)?;
            let accepted = runtime_u64(cells.accepted_at_ms)?;
            if runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || runtime_generation(cells.generation)? != thread.generation.as_str()
                || runtime_opaque(cells.definition_revision)?
                    != execution.snapshot.definition.revision.as_str()
                || turn != execution.snapshot.turn_id.as_str()
                || previous.checked_add(1) != Some(execution.snapshot.revision)
                || accepted != execution.snapshot.updated_at_ms
                || cells.record_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.record_bytes > 65_536 {
                return self.admission.budget_mut().runtime_limit();
            }
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let Some(storage) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            storage.range_counts_and_flags[4] = runtime_add(storage.range_counts_and_flags[4], 1)?;
            if storage.range_counts_and_flags[4] != 1 || storage.headers[0].is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            storage.headers[0] = Some(PermissionRuntimeNewHeader {
                primary: runtime_prepaid_owned(turn, self.admission.budget_mut())?,
                digest: None,
                previous: Some(previous),
                committed: Some(accepted),
                bytes: cells.record_bytes,
            });
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn old_acceptance_query_key(&self) -> Result<(&str, usize), PermissionError> {
        if self.stage / 4 != 41 {
            return Err(PermissionError::CorruptRecord);
        }
        let Some(storage) = &self.match_storage else {
            return Err(PermissionError::CorruptRecord);
        };
        let h = storage.headers[0]
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((&h.primary, h.bytes))
    }
    pub fn capture_old_acceptance_payload(&mut self, raw: &[u8]) -> Result<(), PermissionError> {
        let result = (|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if self.stage / 4 != 41 {
                return Err(PermissionError::CorruptRecord);
            }
            let Some(storage) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            let h = storage.headers[0]
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            if h.bytes != raw.len() {
                return Err(PermissionError::CorruptRecord);
            }
            self.match_legacy_raw(0, raw)?;
            let Some(s) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            s.headers[0] = None;
            s.range_counts_and_flags[7] |= 2;
            Ok(())
        })();
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_old_acceptance_range(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                let Some(storage) = &self.match_storage else {
                    return Err(PermissionError::CorruptRecord);
                };
                if self.stage / 4 != 40
                    || self.complete_flags[4]
                    || storage.range_counts_and_flags[4] != 0
                    || storage.headers[0].is_some()
                    || storage.range_counts_and_flags[7] & 2 != 0
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.complete_flags[4] = true;
                return Ok(());
            }
            let Some(s) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            if self.stage / 4 != 40
                || self.complete_flags[4]
                || s.range_counts_and_flags[4] != 1
                || s.headers[0].is_some()
                || s.range_counts_and_flags[7] & 2 == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[4] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn journal_revision(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<u64, PermissionError> {
        if !self.complete_flags[4] {
            return Err(PermissionError::CorruptRecord);
        }
        let revision = self
            .execution
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .0
            .snapshot
            .revision;
        match point {
            PermissionRuntimeJournalPoint::Park => revision
                .checked_sub(1)
                .ok_or(PermissionError::CorruptRecord),
            PermissionRuntimeJournalPoint::Resume => Ok(revision),
            PermissionRuntimeJournalPoint::Dispatch => Err(PermissionError::UnsupportedProfile),
        }
    }
    pub fn admit_journal_header(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        cells: PermissionProjectionJournalHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            self.admission
                .budget_mut()
                .runtime_prepaid_ready_cancellation_fixed(320, 8)?;
            let (code, flag, status, raw_status): (_, _, _, &[u8]) = match point {
                PermissionRuntimeJournalPoint::Park => (
                    50,
                    5,
                    b4_contract::ExecutionStatus::WaitingForApproval,
                    b"\"waiting_for_approval\"",
                ),
                PermissionRuntimeJournalPoint::Resume => {
                    (60, 6, b4_contract::ExecutionStatus::Running, b"\"running\"")
                }
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if self.stage / 4 != code || self.complete_flags[flag] {
                return Err(PermissionError::CorruptRecord);
            }
            let expected_revision = self.journal_revision(point)?;
            let execution = &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let thread = &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0;
            let revision = runtime_u64(cells.revision)?;
            let owner = runtime_u64(cells.owner)?;
            let time = runtime_u64(cells.time)?;
            if cells.kind != b"commit"
                || cells.sequence <= 0
                || cells.status != raw_status
                || runtime_opaque(cells.execution_id)? != execution.snapshot.execution_id.as_str()
                || runtime_opaque(cells.thread_id)? != thread.id.as_str()
                || revision != expected_revision
                || owner != execution.snapshot.owner_generation
                || cells.payload_bytes == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            let expected = match self.prepared.capture {
                PermissionReadyDispatchCapture::NewHuman { witness, .. } => {
                    if flag == 5 {
                        &witness.park
                    } else {
                        &witness.resume
                    }
                }
                PermissionReadyDispatchCapture::LegacyOnce { journals, .. } => &journals[flag - 5],
                PermissionReadyDispatchCapture::NewUse { .. } => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if expected.revision != revision
                || expected.owner_generation != owner
                || expected.status != status
                || expected.updated_at_ms != time
            {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let Some(s) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            if s.journal_headers.len() != flag - 5
                || (flag == 6 && s.journal_headers[0].sequence >= cells.sequence)
            {
                return Err(PermissionError::CorruptRecord);
            }
            if cells.payload_bytes
                > self
                    .admission
                    .budget_mut()
                    .runtime_input_profile()
                    .cancellation_bytes(160, 1_024)
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            s.journal_headers.push(PermissionRuntimeJournalHeader {
                sequence: cells.sequence,
                revision,
                owner,
                status,
                time,
                bytes: cells.payload_bytes,
            });
            s.range_counts_and_flags[flag] = 1;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn journal_query_key(
        &self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(i64, usize), PermissionError> {
        let code = match point {
            PermissionRuntimeJournalPoint::Park => 51,
            PermissionRuntimeJournalPoint::Resume => 61,
            PermissionRuntimeJournalPoint::Dispatch => {
                return Err(PermissionError::UnsupportedProfile)
            }
        };
        if self.stage / 4 != code {
            return Err(PermissionError::CorruptRecord);
        }
        let Some(storage) = &self.match_storage else {
            return Err(PermissionError::CorruptRecord);
        };
        let h = storage
            .journal_headers
            .last()
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((h.sequence, h.bytes))
    }
    pub fn capture_journal_payload(
        &mut self,
        point: PermissionRuntimeJournalPoint,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let (code, slot, bit) = match point {
                PermissionRuntimeJournalPoint::Park => (51, 2, 4),
                PermissionRuntimeJournalPoint::Resume => (61, 3, 8),
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            if self.stage / 4 != code {
                return Err(PermissionError::CorruptRecord);
            }
            let Some(storage) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            let h = storage
                .journal_headers
                .last()
                .ok_or(PermissionError::CorruptRecord)?;
            if raw.len() != h.bytes {
                return Err(PermissionError::CorruptRecord);
            }
            self.match_legacy_raw(slot, raw)?;
            let Some(storage) = &mut self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            storage.range_counts_and_flags[7] |= bit;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_journal_range(
        &mut self,
        point: PermissionRuntimeJournalPoint,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let (code, flag, bit) = match point {
                PermissionRuntimeJournalPoint::Park => (51, 5, 4),
                PermissionRuntimeJournalPoint::Resume => (61, 6, 8),
                PermissionRuntimeJournalPoint::Dispatch => {
                    return Err(PermissionError::UnsupportedProfile)
                }
            };
            let Some(s) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            if self.stage / 4 != code
                || self.complete_flags[flag]
                || s.range_counts_and_flags[flag] != 1
                || s.range_counts_and_flags[7] & bit == 0
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[flag] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    pub fn end_evidence(&mut self) -> Result<(), PermissionError> {
        let result = runtime_checked_entered_iife(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if matches!(
                self.prepared.capture,
                PermissionReadyDispatchCapture::NewHuman { .. }
            ) {
                let Some(storage) = &self.match_storage else {
                    return Err(PermissionError::CorruptRecord);
                };
                if !self.complete_flags[..7].iter().all(|v| *v)
                    || self.complete_flags[7]
                    || storage.range_counts_and_flags[..3] != [1, 1, 0]
                    || storage.range_counts_and_flags[3..7] != [1, 0, 1, 1]
                    || storage.range_counts_and_flags[7] != 61
                    || storage.headers.iter().any(Option::is_some)
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.complete_flags[7] = true;
                return Ok(());
            }
            let Some(s) = &self.match_storage else {
                return Err(PermissionError::CorruptRecord);
            };
            if !self.complete_flags[..7].iter().all(|v| *v)
                || self.complete_flags[7]
                || s.range_counts_and_flags[..3] != [0, 0, 0]
                || s.range_counts_and_flags[3..7] != [1, 1, 1, 1]
                || s.range_counts_and_flags[7] != 15
                || s.headers.iter().any(Option::is_some)
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.complete_flags[7] = true;
            Ok(())
        });
        self.admission.budget_mut().runtime_finish(result)
    }
    fn legacy_view(
        &self,
    ) -> Result<(&ApprovalView, &PermissionLegacyOnceWitnessFacts), PermissionError> {
        match self.prepared.capture {
            PermissionReadyDispatchCapture::LegacyOnce {
                view, legacy_facts, ..
            } if view.grants.len() == 1 && view.acceptances.len() == 1 => Ok((view, legacy_facts)),
            _ => Err(PermissionError::CorruptRecord),
        }
    }
    pub fn finish_after_commit(self) {
        let Self { admission, .. } = self;
        admission.finish();
    }
    pub fn prepare_work<'work>(
        &'work mut self,
    ) -> Result<
        PermissionRuntimeReadyCancellationWork<'work, 'budget, 'prepared, 'capture, 'catalog>,
        PermissionError,
    >
    where
        'budget: 'work,
        'prepared: 'work,
    {
        let prepared = self.prepared;
        let result = runtime_checked_output_hook_mut::<_, _, 128>(|| {
            self.admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            if !self.complete_flags[7] || self.stage / 4 != 61 || self.stage % 4 != 2 {
                return Err(PermissionError::CorruptRecord);
            }
            let (observation, witness, origin) = match prepared.capture {
                PermissionReadyDispatchCapture::NewHuman {
                    observation,
                    witness,
                    origin,
                } => (observation, witness, origin),
                _ => return Err(PermissionError::UnsupportedProfile),
            };
            if !Arc::ptr_eq(&prepared.origin, origin)
                || !Arc::ptr_eq(&prepared.origin, &observation.origin)
                || !Arc::ptr_eq(&prepared.origin, &witness.origin)
                || !Arc::ptr_eq(&self.origin, &prepared.origin)
                || witness.facts.admitted_counts_and_flags != [1, 0]
                || !witness.grant.consumed
            {
                return Err(PermissionError::CorruptRecord);
            }
            let execution = match self.execution.take() {
                Some((actual, _, _)) => actual,
                None => return Err(PermissionError::CorruptRecord),
            };
            let mut thread = match self.thread.take() {
                Some((actual, _, _)) => actual,
                None => return Err(PermissionError::CorruptRecord),
            };
            let before_facts = match self.checked_resource_facts.take() {
                Some(actual) => actual,
                None => return Err(PermissionError::CorruptRecord),
            };
            let match_storage = match self.match_storage.take() {
                Some(actual) => actual,
                None => return Err(PermissionError::CorruptRecord),
            };
            // This is a second actual Cancel operation, not a replayed result.
            // Its full plan and one-task clone/mutation/validator interfaces are
            // a distinct prospective part of the earlier C1 envelope.
            self.admission.budget_mut().runtime_reserve(80_456, 2_240)?;
            let profile = self.admission.budget_mut().runtime_input_profile();
            runtime_normal_input_owners(
                profile,
                before_facts.actual[0],
                8,
                self.admission.budget_mut(),
            )?;
            runtime_normal_input_owners(
                profile,
                prepared.candidate_post_facts.actual[0],
                8,
                self.admission.budget_mut(),
            )?;
            let authoritative_plan = b4_kernel::transition(
                &execution.snapshot,
                KernelCommand::Cancel {
                    turn_id: execution.snapshot.turn_id.clone(),
                    owner_generation: execution.snapshot.owner_generation,
                },
                prepared.facts.clone(),
            )?;
            if !crate::validate_commit(
                &execution.snapshot,
                prepared.fence.execution,
                &authoritative_plan,
                self.current_owner,
            )? {
                return Err(PermissionError::CorruptRecord);
            }
            // ONE equality covers both full actual plans and dispatchable Vecs.
            if authoritative_plan != prepared.candidate_plan {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            let after = runtime_compat::capture_dispatch_snapshot(
                &authoritative_plan.snapshot,
                self.admission.budget_mut(),
            )?;
            let [after_root, after_spec, after_input] = &after;
            let [prepared_root, prepared_spec, prepared_input] =
                &prepared.candidate_post_facts.actual;
            let [_, before_spec, _, before_input, _, _] = &before_facts.actual;
            if !runtime_ready_cancellation_measure_same(*after_root, *prepared_root)
                || !runtime_ready_cancellation_measure_same(*after_spec, *prepared_spec)
                || !runtime_ready_cancellation_measure_same(*after_input, *prepared_input)
                || !runtime_ready_cancellation_measure_same(*after_spec, *before_spec)
                || !runtime_ready_cancellation_measure_same(*after_input, *before_input)
            {
                return Err(PermissionError::CorruptRecord);
            }
            let next_resource_revision = thread
                .resource_revision
                .checked_add(1)
                .ok_or(PermissionError::CorruptRecord)?;
            let before_scalars = PermissionRuntimeBeforeScalars {
                execution_revision: execution.snapshot.revision,
                resource_revision: thread.resource_revision,
                owner_generation: self.current_owner,
                next_execution_revision: authoritative_plan.snapshot.revision,
                next_resource_revision,
                updated_at_ms: authoritative_plan.snapshot.updated_at_ms,
            };
            thread.resource_revision = next_resource_revision;
            let post_thread =
                runtime_compat::measure_post_thread(&thread, self.admission.budget_mut())?;
            if !profile.thread(post_thread) {
                return Err(PermissionError::UnsupportedProfile);
            }
            let journal = JournalEntry::from(&authoritative_plan.snapshot);
            runtime_ready_cancellation_post_graph(
                &execution,
                &authoritative_plan.snapshot,
                &thread,
                prepared,
                &journal,
            )?;
            let snapshot_payload = runtime_compat::emit_ready_cancellation_snapshot_string(
                &authoritative_plan.snapshot,
                *after_root,
                self.admission.budget_mut(),
            )?;
            let (journal_payload, journal_measure) =
                runtime_compat::emit_ready_cancellation_journal_string(
                    &journal,
                    self.admission.budget_mut(),
                )?;
            let [_, _, _, _, _, _, _, _, resume_measure] = &witness.facts.actual;
            if !profile.cancellation_history(*resume_measure, (160, 11, 1), 1_024)
                || !profile.cancellation_journal(journal_measure)
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            // All output objects exist before any write or Work loan.
            let CommitPlan { snapshot, .. } = authoritative_plan;
            let StoredExecution { thread_id, .. } = execution;
            let output = StoredExecution {
                thread_id,
                snapshot,
            };
            drop(journal);
            drop(match_storage);
            Ok((
                output,
                thread,
                snapshot_payload,
                journal_payload,
                before_scalars,
            ))
        });
        let (output, post_thread, snapshot_payload, journal_payload, before_scalars) =
            self.admission.budget_mut().runtime_finish(result)?;
        let target: &'capture crate::RunCancellationTarget = prepared.target;
        let binding_execution_id: &'capture str = target.execution_id.as_str();
        let binding_thread_id: &'capture str = target.thread_id.as_str();
        let binding_generation: &'capture str = target.thread_generation.as_str();
        Ok(PermissionRuntimeReadyCancellationWork {
            session: self,
            output,
            post_thread,
            snapshot_payload,
            journal_payload,
            before_scalars,
            sql_stage: 0,
            binding_execution_id,
            binding_thread_id,
            binding_generation,
        })
    }
}

fn runtime_ready_cancellation_measure_same(
    a: PermissionRuntimeMeasure,
    b: PermissionRuntimeMeasure,
) -> bool {
    a.encoded_bytes() == b.encoded_bytes()
        && a.structural_nodes() == b.structural_nodes()
        && a.maximum_depth() == b.maximum_depth()
}
fn runtime_ready_cancellation_post_graph(
    before: &StoredExecution,
    after: &b4_contract::Snapshot,
    thread: &ThreadRecord,
    prepared: &PermissionPreparedReadyCancellation<'_, '_>,
    journal: &JournalEntry,
) -> Result<(), PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    let current = &before.snapshot;
    if current.tasks.len() != 1 || after.tasks.len() != 1 || current.turn_history.len() > 2 {
        return Err(PermissionError::UnsupportedProfile);
    }
    let [task] = current.tasks.as_slice() else {
        return Err(PermissionError::UnsupportedProfile);
    };
    let [next] = after.tasks.as_slice() else {
        return Err(PermissionError::UnsupportedProfile);
    };
    if current.status != E::Running
        || task.status != T::Ready
        || !task.approved
        || task.attempt_id.is_some()
        || !current.attempt_history.is_empty()
        || after.schema_version != current.schema_version
        || after.execution_id != current.execution_id
        || after.turn_id != current.turn_id
        || after.turn_history != current.turn_history
        || after.attempt_history != current.attempt_history
        || after.owner_generation != current.owner_generation
        || after.definition != current.definition
        || after.status != E::Cancelled
        || current.revision.checked_add(1) != Some(after.revision)
        || after.batch_index != current.batch_index
        || after.values != current.values
        || after.attempts_used != current.attempts_used
        || after.updated_at_ms != prepared.facts.now_ms
        || after.updated_at_ms < current.updated_at_ms
        || after.output != current.output
        || after.failure != current.failure
        || next.status != T::Cancelled
        || next.spec != task.spec
        || next.input != task.input
        || next.attempt_id != task.attempt_id
        || next.result != task.result
        || next.failure != task.failure
        || next.interrupt != task.interrupt
        || next.approved != task.approved
        || journal.execution_id != after.execution_id
        || journal.revision != after.revision
        || journal.owner_generation != after.owner_generation
        || journal.status != after.status
        || journal.updated_at_ms != after.updated_at_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    let original_thread = match prepared.capture {
        PermissionReadyDispatchCapture::NewHuman { observation, .. } => {
            &observation.resource.thread
        }
        _ => return Err(PermissionError::UnsupportedProfile),
    };
    if thread.id != original_thread.id
        || thread.generation != original_thread.generation
        || thread.metadata != original_thread.metadata
        || thread.origin != original_thread.origin
        || thread.active_execution_id != original_thread.active_execution_id
        || original_thread.resource_revision.checked_add(1) != Some(thread.resource_revision)
        || before.thread_id != original_thread.id
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(())
}

impl<'work, 'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeReadyCancellationWork<'work, 'budget, 'prepared, 'capture, 'catalog>
where
    'budget: 'work,
    'prepared: 'work,
    'capture: 'prepared,
    'catalog: 'prepared,
{
    pub fn before_write_sql(
        &mut self,
        statement: PermissionReadyCancellationWriteSql,
    ) -> Result<PermissionRuntimeBindings<'_>, PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.session
                .admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let expected = match statement {
                PermissionReadyCancellationWriteSql::SnapshotCas => 0,
                PermissionReadyCancellationWriteSql::ThreadCas => 2,
                PermissionReadyCancellationWriteSql::InsertCancelJournal => 4,
                PermissionReadyCancellationWriteSql::Commit => 6,
                PermissionReadyCancellationWriteSql::BeginImmediate => {
                    return Err(PermissionError::CorruptRecord)
                }
            };
            if self.sql_stage != expected {
                return Err(PermissionError::CorruptRecord);
            }
            validation::runtime_phase::runtime_ready_cancellation_write_before_sql(
                self.session.admission.budget_mut(),
                &statement,
                &self.output.snapshot.execution_id,
                &self.post_thread.id,
                self.before_scalars.next_execution_revision,
            )?;
            self.sql_stage = expected + 1;
            Ok(())
        });
        self.session.admission.budget_mut().runtime_finish(result)?;
        // Fixed borrowed bindings and Result carriers are prepaid in THIS
        // admitted statement's disjoint Rust scratch before construction.
        Ok(PermissionRuntimeBindings {
            execution_id: self.binding_execution_id,
            thread_id: self.binding_thread_id,
            generation: self.binding_generation,
            before_revision: self.before_scalars.execution_revision,
            before_resource_revision: self.before_scalars.resource_revision,
            owner_generation: self.before_scalars.owner_generation,
            next_revision: self.before_scalars.next_execution_revision,
            next_resource_revision: self.before_scalars.next_resource_revision,
            updated_at_ms: self.before_scalars.updated_at_ms,
            snapshot: match statement {
                PermissionReadyCancellationWriteSql::SnapshotCas => &self.snapshot_payload,
                _ => "",
            },
            journal: match statement {
                PermissionReadyCancellationWriteSql::InsertCancelJournal => &self.journal_payload,
                _ => "",
            },
        })
    }
    pub fn check_affected_rows(
        &mut self,
        statement: PermissionReadyCancellationWriteSql,
        rows: usize,
    ) -> Result<(), PermissionError> {
        let result = runtime_checked_ready_cancellation_hook(|| {
            self.session
                .admission
                .budget_mut()
                .runtime_check_ready_cancellation()?;
            let expected = match statement {
                PermissionReadyCancellationWriteSql::SnapshotCas => 1,
                PermissionReadyCancellationWriteSql::ThreadCas => 3,
                PermissionReadyCancellationWriteSql::InsertCancelJournal => 5,
                _ => return Err(PermissionError::CorruptRecord),
            };
            if self.sql_stage != expected {
                return Err(PermissionError::CorruptRecord);
            }
            if rows != 1 {
                return Err(crate::StoreError::RevisionConflict.into());
            }
            self.sql_stage = expected + 1;
            Ok(())
        });
        self.session.admission.budget_mut().runtime_finish(result)
    }
    pub fn finish_after_commit(self) -> StoredExecution {
        // The real output was built before Work and all three writes. This
        // consume ends its Session loan; no validator or allocation follows.
        let Self { output, .. } = self;
        output
    }
}

// Unconditional exact new type obligations; compiler evidence remains Root-only.
const _: () = assert!(size_of::<PermissionReadyCancellationWriteSql>() <= 16);
const _: () = assert!(
    size_of::<PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>>()
        <= 2_048
);
const _: () = assert!(
    size_of::<
        Result<
            PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>,
            PermissionError,
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>,
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>>(
    ) <= 1_024
);
const _: () = assert!(
    size_of::<
        Result<
            PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>,
            PermissionError,
        >,
    >() <= 1_024
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>,
        >,
    >() <= 1_024
);
const _: () = assert!(
    size_of::<(
        StoredExecution,
        ThreadRecord,
        String,
        String,
        PermissionRuntimeBeforeScalars
    )>() <= 1_024
);
const _: () = assert!(
    size_of::<
        Result<
            (
                StoredExecution,
                ThreadRecord,
                String,
                String,
                PermissionRuntimeBeforeScalars
            ),
            PermissionError,
        >,
    >() <= 1_024
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (
                StoredExecution,
                ThreadRecord,
                String,
                String,
                PermissionRuntimeBeforeScalars
            ),
        >,
    >() <= 1_024
);
const _: () = assert!(
    size_of::<
        &'static mut PermissionRuntimeReadyCancellationWriteSession<
            'static,
            'static,
            'static,
            'static,
        >,
    >() <= 8
);
const _: () = assert!(
    size_of::<
        &'static mut PermissionRuntimeReadyCancellationWork<
            'static,
            'static,
            'static,
            'static,
            'static,
        >,
    >() <= 8
);
const _: () =
    assert!(size_of::<&'static PermissionPreparedReadyCancellation<'static, 'static>>() <= 8);
const _: () = assert!(size_of::<StoredExecution>() <= 2_048);
const _: () = assert!(size_of::<ThreadRecord>() <= 512);
const _: () = assert!(size_of::<Option<PermissionRuntimeFreshMatchStorage>>() <= 1_024);
const _: () = assert!(
    size_of::<
        Option<(
            StoredExecution,
            [Option<PermissionRuntimeMeasure>; 5],
            [usize; 4]
        )>,
    >() <= 2_048
);
const _: () =
    assert!(size_of::<Option<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>>() <= 1_024);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            validation::RuntimeDispatchAdmission<'static>,
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeBindings<'static>,
        >,
    >() <= 256
);
const _: () = assert!(size_of::<Result<b4_contract::ThreadId, PermissionError>>() <= 256);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            b4_contract::ThreadId,
        >,
    >() <= 256
);
const _: () = assert!(
    size_of::<PermissionReadyCancellationSensitiveRead<'static, 'static, 'static, 'static>>() <= 8
);
const _: () = assert!(
    size_of::<
        Result<
            PermissionReadyCancellationSensitiveRead<'static, 'static, 'static, 'static>,
            PermissionError,
        >,
    >() <= 128
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionReadyCancellationSensitiveRead<'static, 'static, 'static, 'static>,
        >,
    >() <= 128
);
const _: () =
    assert!(size_of::<&'static mut PermissionRuntimeReadSession<'static, 'static, 'static>>() <= 8);
const _: () = assert!(
    size_of::<&'static PermissionReadyCancellationSensitiveRead<'static, 'static, 'static, 'static>>(
    ) <= 8
);
const _: () = assert!(
    size_of::<
        &'static mut PermissionReadyCancellationSensitiveRead<'static, 'static, 'static, 'static>,
    >() <= 8
);
const _: () = assert!(size_of::<PermissionNewHeaderCells<'static>>() <= 256);
const _: () = assert!(size_of::<PermissionGrantHeaderCells<'static>>() <= 128);
const _: () = assert!(size_of::<PermissionAcceptanceHeaderCells<'static>>() <= 128);
const _: () = assert!(size_of::<PermissionProjectionJournalHeaderCells<'static>>() <= 128);
const _: () = assert!(size_of::<PermissionNewGroup>() <= 16);
const _: () = assert!(size_of::<(i64, usize)>() <= 16);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, (i64, usize)>>(
    ) <= 128
);

// Exact shared references used by the dedicated cancellation borrowed patterns.
const _: () = assert!(size_of::<&'static [b4_contract::TaskRecord]>() <= 16);
const _: () = assert!(size_of::<&'static b4_contract::TaskRecord>() <= 8);
const _: () = assert!(size_of::<&'static PermissionRuntimeMeasure>() <= 8);
const _: () = assert!(size_of::<&'static [PermissionRuntimeMeasure; 3]>() <= 8);
const _: () = assert!(size_of::<&'static [PermissionRuntimeMeasure; 6]>() <= 8);
const _: () = assert!(size_of::<&'static [PermissionRuntimeMeasure; 9]>() <= 8);

// Exact bare payload tuples consumed by the four manual cancellation takes.
const _: () = assert!(
    size_of::<(
        StoredExecution,
        [Option<PermissionRuntimeMeasure>; 5],
        [usize; 4]
    )>() <= 2_048
);
const _: () = assert!(size_of::<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>() <= 1_024);
const _: () = assert!(size_of::<serde_json::Error>() <= 128);
const _: () = assert!(size_of::<Result<String, serde_json::Error>>() <= 128);
const _: () = assert!(size_of::<Result<String, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, String>>()
        <= 128
);
const _: () = assert!(size_of::<(String, PermissionRuntimeMeasure)>() <= 128);
const _: () =
    assert!(size_of::<Result<(String, PermissionRuntimeMeasure), PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (String, PermissionRuntimeMeasure),
        >,
    >() <= 128
);
const _: () = assert!(size_of::<std::sync::Arc<PermissionRuntimeOrigin>>() <= 128);
const _: () = assert!(size_of::<std::sync::atomic::Ordering>() <= 16);

// Actual private G helper/result/view storage bounds; these buy no work or data.
const _: () = assert!(size_of::<RuntimeHistoryWork>() <= 8);
const _: () =
    assert!(size_of::<Result<&'static super::PermissionBindingIdentity, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static super::PermissionBindingIdentity,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<Result<&'static super::PermissionId, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static super::PermissionId,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<Result<&'static crate::ThreadOrigin, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static crate::ThreadOrigin,
        >,
    >() <= 128
);
const _: () =
    assert!(size_of::<Result<&'static b4_contract::ExecutionId, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static b4_contract::ExecutionId,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<Option<&'static super::PermissionBindingIdentity>>() <= 8);
const _: () = assert!(size_of::<Option<&'static super::PermissionId>>() <= 8);
const _: () = assert!(size_of::<Option<&'static b4_contract::ExecutionId>>() <= 8);
const _: () = assert!(size_of::<Option<&'static b4_contract::TurnId>>() <= 8);
const _: () = assert!(size_of::<&'static [b4_contract::TurnId]>() <= 16);
const _: () = assert!(size_of::<std::ops::RangeTo<usize>>() <= 8);
const _: () = assert!(size_of::<std::ops::RangeFull>() == 0);

// Genuine current Read factory metadata owners and borrowed State views.
const _: () = assert!(size_of::<PermissionRuntimeOriginOwner>() <= 8);
const _: () = assert!(size_of::<Option<&'static PermissionRuntimeState>>() <= 8);
const _: () = assert!(size_of::<Option<&'static mut PermissionRuntimeState>>() <= 8);
const _: () = assert!(size_of::<[Vec<u8>; 5]>() <= 128);

// Genuine complete Read wrapper owner/Result/Flow bounds; no work is bought here.
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (StoredExecution, runtime_compat::RuntimeSnapshotFacts),
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (ThreadRecord, PermissionRuntimeMeasure),
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeResourceFacts,
        >,
    >() <= 256
);
const _: () = assert!(
    size_of::<
        Result<
            (
                StoredExecution,
                [Option<PermissionRuntimeMeasure>; 5],
                [usize; 4]
            ),
            PermissionError,
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (
                StoredExecution,
                [Option<PermissionRuntimeMeasure>; 5],
                [usize; 4]
            ),
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<Result<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4]), PermissionError>>()
        <= 1_024
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (ThreadRecord, PermissionRuntimeMeasure, [usize; 4]),
        >,
    >() <= 1_024
);
const _: () = assert!(size_of::<Result<PermissionRuntimeNewHeader, PermissionError>>() <= 256);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeNewHeader,
        >,
    >() <= 256
);
const _: () = assert!(size_of::<Result<PermissionRuntimeGrantHeader, PermissionError>>() <= 256);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeGrantHeader,
        >,
    >() <= 256
);
const _: () = assert!(
    size_of::<Result<(PermissionAcceptance, [PermissionRuntimeMeasure; 3]), PermissionError>>()
        <= 2_048
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (PermissionAcceptance, [PermissionRuntimeMeasure; 3]),
        >,
    >() <= 2_048
);
const _: () = assert!(
    size_of::<Result<(PermissionDecision, [PermissionRuntimeMeasure; 2]), PermissionError>>()
        <= 1_024
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (PermissionDecision, [PermissionRuntimeMeasure; 2]),
        >,
    >() <= 1_024
);
const _: () = assert!(
    size_of::<Result<(GrantRecord, [PermissionRuntimeMeasure; 2]), PermissionError>>() <= 1_024
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (GrantRecord, [PermissionRuntimeMeasure; 2]),
        >,
    >() <= 1_024
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (JournalEntry, PermissionRuntimeMeasure),
        >,
    >() <= 256
);
const _: () = assert!(size_of::<Option<PermissionRuntimeGrantHeader>>() <= 256);
const _: () = assert!(size_of::<Option<PermissionRuntimeJournalHeader>>() <= 256);
const _: () = assert!(size_of::<std::sync::Arc<PermissionImmutableDispatchWitness>>() <= 8);

const _: () = assert!(size_of::<b4_contract::ThreadId>() <= 256);

// Separate original-account entry purchase before cancellation initialization.
// Its compiler-qualified callback carriers have no data-owner or native credit.

pub(super) fn runtime_checked_ready_cancellation_entry<F, T, const F_BYTES: usize>(
    call: F,
) -> Result<T, PermissionError>
where
    F: FnOnce() -> Result<T, PermissionError>,
{
    const {
        assert!(size_of::<F>() <= F_BYTES);
    }
    call()
}

// Selected borrowed Read callbacks invoke FnMut directly; the owned Cells callbacks
// retain the existing FnOnce helper. Every actual callback keeps its whole-F bound.
fn runtime_checked_hook_mut<F, T, const F_BYTES: usize>(mut check: F) -> Result<T, PermissionError>
where
    F: FnMut() -> Result<T, PermissionError>,
{
    const {
        assert!(size_of::<F>() <= F_BYTES);
        assert!(size_of::<&mut F>() <= 8);
        assert!(size_of::<()>() == 0);
    };
    check()
}

// Genuine standard-library owners used by the selected Read Journal paths.
const _: () = assert!(size_of::<Option<(JournalEntry, PermissionRuntimeMeasure)>>() <= 256);
const _: () = assert!(size_of::<Result<u64, std::num::TryFromIntError>>() <= 128);

// Separate actual Read question carriers; borrowed payloads are not enum proxies.
const _: () =
    assert!(size_of::<Result<&'static PermissionRuntimeResourceFacts, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionRuntimeResourceFacts,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<Result<&'static [u8; 64], PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &'static [u8; 64]>,
    >() <= 128
);
const _: () =
    assert!(size_of::<Result<&'static PermissionRuntimeGrantHeader, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionRuntimeGrantHeader,
        >,
    >() <= 128
);
const _: () = assert!(size_of::<Result<Vec<u8>, PermissionError>>() <= 128);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, Vec<u8>>>()
        <= 128
);

fn runtime_checked_read_sequence_mapper<F>(mapper: F) -> F
where
    F: FnOnce(std::num::TryFromIntError) -> PermissionError,
{
    const {
        assert!(size_of::<F>() <= 64);
    }
    mapper
}

// Genuine Flow carrier of the five selected FirstFacts Option::ok_or question sites.
// The exact aliased Result carrier is already asserted in validation.rs.
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeMeasure,
        >,
    >() <= 128
);

// Additional whole-type bounds from the pinned compiler layout audit.
const _: () = assert!(std::mem::size_of::<PermissionRuntimeCaptureKind>() <= 1056);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionRuntimeCaptureKind, PermissionError>>() <= 1056);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeNewHeader>() <= 144);
const _: () = assert!(std::mem::size_of::<Option<PermissionRuntimeNewHeader>>() <= 144);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeJournalHeader>() <= 48);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeFreshMatchStorage>() <= 480);
const _: () =
    assert!(std::mem::size_of::<PermissionRuntimeReadSession<'static, 'static, 'static>>() <= 5120);
const _: () = assert!(
    std::mem::size_of::<
        Result<PermissionRuntimeReadSession<'static, 'static, 'static>, PermissionError>,
    >() <= 5120
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadSession<'static, 'static, 'static>,
        >,
    >() <= 5120
);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeBeforeScalars>() <= 48);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeBindings<'static>>() <= 128);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeResourceFacts>() <= 208);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeWitnessFacts>() <= 240);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeObservation>() <= 768);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionRuntimeObservation, PermissionError>>() <= 768);
const _: () = assert!(std::mem::size_of::<PermissionRuntimeCapture>() <= 1056);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionRuntimeCapture, PermissionError>>() <= 1056);
const _: () = assert!(std::mem::size_of::<Option<PermissionRuntimeResourceFacts>>() <= 224);
const _: () = assert!(std::mem::size_of::<Option<PermissionRuntimeWitnessFacts>>() <= 240);
const _: () = assert!(
    std::mem::size_of::<Result<PermissionRuntimeBindings<'static>, PermissionError>>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        Option<(
            StoredExecution,
            [Option<PermissionRuntimeMeasure>; 5],
            [usize; 4]
        )>,
    >() <= 544
);
const _: () = assert!(
    std::mem::size_of::<Option<(PermissionAcceptance, [PermissionRuntimeMeasure; 3])>>() <= 672
);
const _: () = assert!(
    std::mem::size_of::<Option<(PermissionDecision, [PermissionRuntimeMeasure; 2])>>() <= 608
);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionRuntimeResourceFacts, PermissionError>>() <= 224);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            (
                StoredExecution,
                ThreadRecord,
                CommitPlan,
                PermissionRuntimeResourceFacts,
                String,
                String,
                JournalEntry,
                PermissionRuntimeBeforeScalars,
                PermissionRuntimeFreshMatchStorage,
                Arc<PermissionRuntimeOrigin>,
                PermissionRuntimePostReadCertificate
            ),
            PermissionError,
        >,
    >() <= 1792
);
const _: () = assert!(
    std::mem::size_of::<(
        StoredExecution,
        ThreadRecord,
        CommitPlan,
        PermissionRuntimeResourceFacts,
        String,
        String,
        JournalEntry,
        PermissionRuntimeBeforeScalars,
        PermissionRuntimeFreshMatchStorage,
        Arc<PermissionRuntimeOrigin>,
        PermissionRuntimePostReadCertificate
    )>() <= 1792
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (
                StoredExecution,
                ThreadRecord,
                CommitPlan,
                PermissionRuntimeResourceFacts,
                String,
                String,
                JournalEntry,
                PermissionRuntimeBeforeScalars,
                PermissionRuntimeFreshMatchStorage,
                Arc<PermissionRuntimeOrigin>,
                PermissionRuntimePostReadCertificate
            ),
        >,
    >() <= 1792
);
const _: () = assert!(std::mem::size_of::<CommitPlan>() <= 384);
const _: () = assert!(std::mem::size_of::<Result<CommitPlan, b4_kernel::KernelError>>() <= 384);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, b4_kernel::KernelError>, CommitPlan>,
    >() <= 384
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeCapture,
        >,
    >() <= 1056
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeObservation,
        >,
    >() <= 768
);
const _: () =
    assert!(std::mem::size_of::<PermissionPreparedReadyCancellation<'static, 'static>>() <= 640);
const _: () = assert!(
    std::mem::size_of::<
        Result<PermissionPreparedReadyCancellation<'static, 'static>, PermissionError>,
    >() <= 640
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionPreparedReadyCancellation<'static, 'static>,
        >,
    >() <= 640
);
const _: () = assert!(std::mem::size_of::<Result<StoredExecution, PermissionError>>() <= 352);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, StoredExecution>,
    >() <= 352
);
const _: () = assert!(
    std::mem::size_of::<
        PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>,
    >() <= 1568
);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>,
            PermissionError,
        >,
    >() <= 1568
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>,
        >,
    >() <= 1568
);
const _: () = assert!(
    std::mem::size_of::<
        PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>,
    >() <= 704
);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>,
            PermissionError,
        >,
    >() <= 704
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>,
        >,
    >() <= 704
);
const _: () = assert!(
    std::mem::size_of::<(
        StoredExecution,
        ThreadRecord,
        String,
        String,
        PermissionRuntimeBeforeScalars
    )>() <= 640
);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            (
                StoredExecution,
                ThreadRecord,
                String,
                String,
                PermissionRuntimeBeforeScalars
            ),
            PermissionError,
        >,
    >() <= 640
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (
                StoredExecution,
                ThreadRecord,
                String,
                String,
                PermissionRuntimeBeforeScalars
            ),
        >,
    >() <= 640
);
const _: () = assert!(std::mem::size_of::<StoredExecution>() <= 352);
const _: () = assert!(std::mem::size_of::<ThreadRecord>() <= 192);
const _: () = assert!(std::mem::size_of::<Option<PermissionRuntimeFreshMatchStorage>>() <= 480);
const _: () = assert!(
    std::mem::size_of::<Option<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>>() <= 240
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeBindings<'static>,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<PermissionGrantHeaderCells<'static>>() <= 112);
const _: () = assert!(std::mem::size_of::<PermissionAcceptanceHeaderCells<'static>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<(
        StoredExecution,
        [Option<PermissionRuntimeMeasure>; 5],
        [usize; 4]
    )>() <= 544
);
const _: () =
    assert!(std::mem::size_of::<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4])>() <= 240);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (StoredExecution, runtime_compat::RuntimeSnapshotFacts),
        >,
    >() <= 512
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (ThreadRecord, PermissionRuntimeMeasure),
        >,
    >() <= 208
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeResourceFacts,
        >,
    >() <= 224
);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            (
                StoredExecution,
                [Option<PermissionRuntimeMeasure>; 5],
                [usize; 4]
            ),
            PermissionError,
        >,
    >() <= 544
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (
                StoredExecution,
                [Option<PermissionRuntimeMeasure>; 5],
                [usize; 4]
            ),
        >,
    >() <= 544
);
const _: () = assert!(
    std::mem::size_of::<
        Result<(ThreadRecord, PermissionRuntimeMeasure, [usize; 4]), PermissionError>,
    >() <= 240
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (ThreadRecord, PermissionRuntimeMeasure, [usize; 4]),
        >,
    >() <= 240
);
const _: () =
    assert!(std::mem::size_of::<Result<PermissionRuntimeNewHeader, PermissionError>>() <= 144);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeNewHeader,
        >,
    >() <= 144
);
const _: () = assert!(
    std::mem::size_of::<
        Result<(PermissionAcceptance, [PermissionRuntimeMeasure; 3]), PermissionError>,
    >() <= 672
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (PermissionAcceptance, [PermissionRuntimeMeasure; 3]),
        >,
    >() <= 672
);
const _: () = assert!(
    std::mem::size_of::<Result<(PermissionDecision, [PermissionRuntimeMeasure; 2]), PermissionError>>(
    ) <= 608
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (PermissionDecision, [PermissionRuntimeMeasure; 2]),
        >,
    >() <= 608
);
const _: () = assert!(std::mem::size_of::<Option<PermissionRuntimeJournalHeader>>() <= 48);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>,
            PermissionError,
        >,
    >() <= 1568
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadyCancellationWriteSession<'static, 'static, 'static, 'static>,
        >,
    >() <= 1568
);
const _: () = assert!(
    std::mem::size_of::<
        Result<
            PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>,
            PermissionError,
        >,
    >() <= 704
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeReadyCancellationWork<'static, 'static, 'static, 'static, 'static>,
        >,
    >() <= 704
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeObservation,
        >,
    >() <= 768
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeCapture,
        >,
    >() <= 1056
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, StoredExecution>,
    >() <= 352
);
const _: () = assert!(std::mem::size_of::<Result<ThreadRecord, PermissionError>>() <= 192);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ThreadRecord>,
    >() <= 192
);
const _: () = assert!(std::mem::size_of::<Result<CommitPlan, PermissionError>>() <= 384);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, CommitPlan>,
    >() <= 384
);

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

// Supplemental exact reached array and Kernel whole-type bounds.
const _: () = assert!(std::mem::size_of::<[usize; 4]>() <= 128);
const _: () = assert!(std::mem::size_of::<[PermissionRuntimeMeasure; 9]>() <= 256);
const _: () = assert!(std::mem::size_of::<[usize; 2]>() <= 16);
const _: () = assert!(std::mem::size_of::<b4_contract::Snapshot>() <= 2048);
const _: () = assert!(std::mem::size_of::<b4_contract::KernelCommand>() <= 512);
const _: () = assert!(std::mem::size_of::<b4_contract::TurnId>() <= 256);
const _: () = assert!(std::mem::size_of::<Vec<b4_contract::TaskId>>() <= 32);
const _: () = assert!(std::mem::size_of::<Vec<&'static b4_contract::TaskSpec>>() <= 32);
const _: () =
    assert!(std::mem::size_of::<std::collections::BTreeSet<&'static b4_contract::TurnId>>() <= 32);
const _: () = assert!(
    std::mem::size_of::<std::collections::BTreeSet<&'static b4_contract::AttemptId>>() <= 32
);
const _: () =
    assert!(std::mem::size_of::<std::collections::BTreeSet<&'static b4_contract::TaskId>>() <= 32);
const _: () = assert!(
    std::mem::size_of::<std::collections::BTreeSet<&'static b4_contract::EffectId>>() <= 32
);
const _: () = assert!(
    std::mem::size_of::<std::collections::BTreeSet<&'static b4_contract::InterruptId>>() <= 32
);
const _: () = assert!(std::mem::size_of::<Result<u64, b4_kernel::KernelError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, b4_kernel::KernelError>, u64>,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<b4_contract::SnapshotVersionError>() <= 128);
const _: () = assert!(std::mem::size_of::<Result<(), b4_contract::SnapshotVersionError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, b4_contract::SnapshotVersionError>,
            (),
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<Result<(), crate::StoreError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, crate::StoreError>, ()>,
    >() <= 128
);

// Inert whole named bounds for the private mandatory-domain compiler prospect.
// N01: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<crate::approval::ApprovalFence>() <= 64);
    assert!(std::mem::align_of::<crate::approval::ApprovalFence>() <= 16);
};
// N02: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<crate::store::ExecutionFence>() <= 32);
    assert!(std::mem::align_of::<crate::store::ExecutionFence>() <= 16);
};
// N03: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<crate::InvocationContext>() <= 512);
    assert!(std::mem::align_of::<crate::InvocationContext>() <= 16);
};
// N04: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<crate::ThreadOrigin>() <= 128);
    assert!(std::mem::align_of::<crate::ThreadOrigin>() <= 16);
};
// N05: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<crate::approval::ApprovalError>() <= 128);
    assert!(std::mem::align_of::<crate::approval::ApprovalError>() <= 16);
};
// N06: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<Result<std::convert::Infallible, crate::approval::ApprovalError>>()
            <= 128
    );
    assert!(
        std::mem::align_of::<Result<std::convert::Infallible, crate::approval::ApprovalError>>()
            <= 16
    );
};
// N07: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(std::mem::size_of::<Result<(), crate::approval::ApprovalError>>() <= 128);
    assert!(std::mem::align_of::<Result<(), crate::approval::ApprovalError>>() <= 16);
};
// N08: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, crate::approval::ApprovalError>,
                (),
            >,
        >() <= 128
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, crate::approval::ApprovalError>,
                (),
            >,
        >() <= 16
    );
};
// N09: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionRuntimeCaptureKind,
            >,
        >() <= 2048
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionRuntimeCaptureKind,
            >,
        >() <= 16
    );
};
// N10: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionReadyDispatchCapture,
            >,
        >() <= 2048
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionReadyDispatchCapture,
            >,
        >() <= 16
    );
};
// N11: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<Result<&'static crate::ThreadOrigin, crate::approval::ApprovalError>>()
            <= 128
    );
    assert!(
        std::mem::align_of::<Result<&'static crate::ThreadOrigin, crate::approval::ApprovalError>>(
        ) <= 16
    );
};
// N12: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, crate::approval::ApprovalError>,
                &'static crate::ThreadOrigin,
            >,
        >() <= 128
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, crate::approval::ApprovalError>,
                &'static crate::ThreadOrigin,
            >,
        >() <= 16
    );
};
// N13: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<(
            &'static b4_contract::Definition,
            &'static super::ToolBindingRegistration,
            [PermissionRuntimeMeasure; 4]
        )>() <= 256
    );
    assert!(
        std::mem::align_of::<(
            &'static b4_contract::Definition,
            &'static super::ToolBindingRegistration,
            [PermissionRuntimeMeasure; 4]
        )>() <= 16
    );
};
// N14: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            Result<
                (
                    &'static b4_contract::Definition,
                    &'static super::ToolBindingRegistration,
                    [PermissionRuntimeMeasure; 4]
                ),
                PermissionError,
            >,
        >() <= 256
    );
    assert!(
        std::mem::align_of::<
            Result<
                (
                    &'static b4_contract::Definition,
                    &'static super::ToolBindingRegistration,
                    [PermissionRuntimeMeasure; 4]
                ),
                PermissionError,
            >,
        >() <= 16
    );
};
// N15: complete named type; static lifetime is a layout proposition only.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (
                    &'static b4_contract::Definition,
                    &'static super::ToolBindingRegistration,
                    [PermissionRuntimeMeasure; 4]
                ),
            >,
        >() <= 256
    );
    assert!(
        std::mem::align_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (
                    &'static b4_contract::Definition,
                    &'static super::ToolBindingRegistration,
                    [PermissionRuntimeMeasure; 4]
                ),
            >,
        >() <= 16
    );
};

// Bound the actual five mandatory cancellation callbacks before their single call.
pub(super) fn runtime_checked_mandatory_iife<F, R, const LIMIT: usize>(mut call: F) -> R
where
    F: FnMut() -> R,
{
    const { assert!(std::mem::size_of::<F>() <= LIMIT) };
    call()
}

// Preserve each actual immediate Unit hook with a genuine callback bound.
fn runtime_checked_entered_iife<F, T>(mut callback: F) -> Result<T, PermissionError>
where
    F: FnMut() -> Result<T, PermissionError>,
{
    const { assert!(std::mem::size_of::<F>() <= 512) };
    callback()
}

impl<'budget, 'selector, 'input> PermissionRuntimeReadSession<'budget, 'selector, 'input> {
    pub fn before_code_only_scope(&mut self) -> Result<(), PermissionError> {
        // New scope-only M128 metadata plus its named unique mode-guard and
        // original connection/capture loan ends. Callback work stays exterior.
        self.admission.budget_mut().runtime_reserve(384 + 88, 1 + 7)
    }
    pub fn before_evidence_kind(&mut self) -> Result<(), PermissionError> {
        // New classifier M512 metadata and its original receiver loan end.
        // A separately trusted bounded prefix covers a refused first purchase.
        self.admission
            .budget_mut()
            .runtime_reserve(1_152 + 16, 1 + 1)
    }
    pub fn evidence_kind(&self) -> Result<PermissionRuntimeEvidenceKind, PermissionError> {
        if !self.complete_flags[..3]
            .iter()
            .all(runtime_checked_value(|v: &bool| *v))
        {
            return Err(PermissionError::CorruptRecord);
        }
        let kind = match [self.counts[0], self.counts[1], self.counts[2]] {
            [1, 1, 0] => PermissionRuntimeEvidenceKind::NewHuman,
            [0, 0, 0] => PermissionRuntimeEvidenceKind::LegacyOnce,
            [0, 1, 1] => PermissionRuntimeEvidenceKind::NewUse,
            _ => return Err(PermissionError::UnsupportedProfile),
        };
        self.admission
            .check_use_dispatch_evidence(matches!(kind, PermissionRuntimeEvidenceKind::NewUse))?;
        Ok(kind)
    }
    pub fn use_session(&mut self) -> Result<PermissionRuntimeUseSession<'_>, PermissionError> {
        if self.evidence_kind()? != PermissionRuntimeEvidenceKind::NewUse || !self.is_sensitive() {
            return Err(PermissionError::UnsupportedProfile);
        }
        PermissionRuntimeUseSession::new(
            self.admission.budget_mut(),
            &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0,
            &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0,
            &self.origin,
            None,
        )
    }
    pub fn accept_use(
        &mut self,
        evidence: PermissionRuntimeUseEvidence,
    ) -> Result<(), PermissionError> {
        self.admission
            .budget_mut()
            .runtime_reserve(128 + 2 * 8192, 1)?;
        if self.evidence_kind()? != PermissionRuntimeEvidenceKind::NewUse
            || self.completed_capture.is_ok()
        {
            return Err(PermissionError::CorruptRecord);
        }
        let witness = evidence.into_capture(&self.origin)?;
        // Requested Arc storage and unique owner/end precede its construction.
        self.admission.budget_mut().runtime_reserve(8192 + 128, 1)?;
        let witness = Arc::new(witness);
        self.admission.budget_mut().runtime_reserve(128, 2)?;
        let observation = PermissionRuntimeObservation {
            resource: ApprovalResource {
                execution: self
                    .execution
                    .take()
                    .ok_or(PermissionError::CorruptRecord)?
                    .0,
                thread: self.thread.take().ok_or(PermissionError::CorruptRecord)?.0,
            },
            facts: self
                .resource_facts
                .take()
                .ok_or(PermissionError::CorruptRecord)?,
            origin: Arc::clone(&self.origin),
        };
        self.completed_capture = Ok(PermissionRuntimeCapture {
            inner: PermissionRuntimeCaptureKind::Ready(PermissionReadyDispatchCapture::NewUse {
                observation,
                witness,
                origin: Arc::clone(&self.origin),
            }),
        });
        self.complete_flags[7] = true;
        self.stage = 61 * 4 + 2;
        Ok(())
    }
}
impl<'budget, 'prepared, 'capture, 'catalog>
    PermissionRuntimeWriteSession<'budget, 'prepared, 'capture, 'catalog>
where
    'capture: 'prepared,
    'catalog: 'prepared,
{
    pub fn before_code_only_scope(&mut self) -> Result<(), PermissionError> {
        // New scope-only M2048 metadata and the same named loan/end envelope.
        self.admission
            .budget_mut()
            .runtime_reserve(4_224 + 88, 1 + 7)
    }
    pub fn before_evidence_kind(&mut self) -> Result<(), PermissionError> {
        // New classifier M512 metadata and its original receiver loan end.
        // A separately trusted bounded prefix covers a refused first purchase.
        self.admission
            .budget_mut()
            .runtime_reserve(1_152 + 16, 1 + 1)
    }
    pub fn evidence_kind(&self) -> Result<PermissionRuntimeEvidenceKind, PermissionError> {
        let storage = self
            .match_storage
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        if !self.complete_flags[..3]
            .iter()
            .all(runtime_checked_value(|v: &bool| *v))
        {
            return Err(PermissionError::CorruptRecord);
        }
        let kind = match [
            storage.range_counts_and_flags[0],
            storage.range_counts_and_flags[1],
            storage.range_counts_and_flags[2],
        ] {
            [1, 1, 0] => PermissionRuntimeEvidenceKind::NewHuman,
            [0, 0, 0] => PermissionRuntimeEvidenceKind::LegacyOnce,
            [0, 1, 1] => PermissionRuntimeEvidenceKind::NewUse,
            _ => return Err(PermissionError::UnsupportedProfile),
        };
        self.admission
            .check_use_dispatch_evidence(matches!(kind, PermissionRuntimeEvidenceKind::NewUse))?;
        Ok(kind)
    }
    pub fn use_session(&mut self) -> Result<PermissionRuntimeUseSession<'_>, PermissionError> {
        if self.evidence_kind()? != PermissionRuntimeEvidenceKind::NewUse || self.use_matched {
            return Err(PermissionError::UnsupportedProfile);
        }
        let witness = match self.prepared.capture {
            PermissionReadyDispatchCapture::NewUse { witness, .. } => witness.as_ref(),
            _ => return Err(PermissionError::CorruptRecord),
        };
        PermissionRuntimeUseSession::new(
            self.admission.budget_mut(),
            &self
                .execution
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0,
            &self
                .thread
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?
                .0,
            &self.origin,
            Some(witness),
        )
    }
    pub fn accept_use(
        &mut self,
        evidence: PermissionRuntimeUseEvidence,
    ) -> Result<(), PermissionError> {
        self.admission
            .budget_mut()
            .runtime_reserve(128 + 2 * 8192, 1)?;
        if self.evidence_kind()? != PermissionRuntimeEvidenceKind::NewUse || self.use_matched {
            return Err(PermissionError::CorruptRecord);
        }
        evidence.require_match(&self.origin)?;
        self.use_matched = true;
        self.complete_flags[7] = true;
        self.stage = 61 * 4 + 2;
        Ok(())
    }
}
fn runtime_checked_value<F>(value: F) -> F {
    const {
        assert!(size_of::<F>() <= 128);
    }
    value
}
const _: () = assert!(size_of::<PermissionRuntimeWriteSession<'_, '_, '_, '_>>() <= 4096);

const _: () = assert!(size_of::<Result<PermissionRuntimeEvidenceKind, PermissionError>>() <= 512);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeEvidenceKind,
        >,
    >() <= 512
);

// Narrow sibling loan of an actual sealed NewUse Claim. No layout/variant or
// existing callable changes; the caller's fresh renewal Budget pays before entry.
pub(super) struct PermissionUseClaimLoan<'claim> {
    pub(super) resource: &'claim ApprovalResource,
    pub(super) invocation: &'claim EffectInvocation,
    pub(super) historical_context: &'claim InvocationContext,
    pub(super) witness: &'claim PermissionRuntimeUseWitness,
    pub(super) origin: &'claim Arc<PermissionRuntimeOrigin>,
}
impl PermissionDispatchClaim<'_> {
    pub(super) fn renewal_use_loan<'claim>(
        &'claim self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionUseClaimLoan<'claim>, PermissionError> {
        budget.runtime_reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        match &self.witness {
            PermissionClaimWitness::NewUse(witness) if witness.same_origin(&self.origin) => {
                Ok(PermissionUseClaimLoan {
                    resource: &self.resource,
                    invocation: &self.invocation,
                    historical_context: &self.context,
                    witness,
                    origin: &self.origin,
                })
            }
            _ => Err(PermissionError::UnsupportedProfile),
        }
    }
}
const _: () = {
    assert!(size_of::<PermissionUseClaimLoan<'static>>() <= 128);
    assert!(size_of::<Result<PermissionUseClaimLoan<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseClaimLoan<'static>,
            >,
        >() <= 128
    );
};

const _: () = assert!(
    size_of::<(
        &PermissionDispatchClaim<'static>,
        &mut PermissionEvidenceBudget
    )>() <= 128
);

// Callback-only sibling; no resource observation, copy, renewal or raw extraction.
pub(super) struct PermissionCallbackClaimLoan<'claim> {
    pub(super) invocation: &'claim EffectInvocation,
    pub(super) origin: &'claim Arc<PermissionRuntimeOrigin>,
}
impl PermissionDispatchClaim<'_> {
    pub(super) fn callback_use_loan<'claim>(
        &'claim self,
        budget: &mut super::runtime_use_callback::CallbackBudget,
    ) -> Result<PermissionCallbackClaimLoan<'claim>, PermissionError> {
        budget.purchase(
            super::runtime_use_callback::CALLBACK_LOAN_FIXED,
            super::runtime_use_callback::CALLBACK_LOAN_NODES,
        )?;
        match &self.witness {
            PermissionClaimWitness::NewUse(witness) if witness.same_origin(&self.origin) => {
                Ok(PermissionCallbackClaimLoan {
                    invocation: &self.invocation,
                    origin: &self.origin,
                })
            }
            _ => Err(PermissionError::UnsupportedProfile),
        }
    }
}
const _: () = {
    assert!(size_of::<PermissionCallbackClaimLoan<'static>>() <= 128);
    assert!(size_of::<Result<PermissionCallbackClaimLoan<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<(
            &PermissionDispatchClaim<'static>,
            &mut super::runtime_use_callback::CallbackBudget
        )>() <= 128
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionCallbackClaimLoan<'static>,
            >,
        >() <= 128
    );
};

// Outcome-only paid sibling: borrow the actual retained NewUse identity.
pub(super) struct PermissionOutcomeClaimLoan<'claim> {
    pub(super) resource: &'claim ApprovalResource,
    pub(super) invocation: &'claim EffectInvocation,
    pub(super) witness: &'claim PermissionRuntimeUseWitness,
    pub(super) origin: &'claim Arc<PermissionRuntimeOrigin>,
}
impl PermissionDispatchClaim<'_> {
    pub(super) fn outcome_use_loan<'claim>(
        &'claim self,
        account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<PermissionOutcomeClaimLoan<'claim>, PermissionError> {
        account.purchase(
            super::runtime_use_outcome::LOAN_BYTES,
            super::runtime_use_outcome::LOAN_NODES,
        )?;
        match &self.witness {
            PermissionClaimWitness::NewUse(witness) if witness.same_origin(&self.origin) => {
                Ok(PermissionOutcomeClaimLoan {
                    resource: &self.resource,
                    invocation: &self.invocation,
                    witness,
                    origin: &self.origin,
                })
            }
            _ => Err(PermissionError::UnsupportedProfile),
        }
    }
}
const _: () = {
    assert!(size_of::<PermissionOutcomeClaimLoan<'_>>() <= 128);
    assert!(size_of::<Result<PermissionOutcomeClaimLoan<'_>, PermissionError>>() <= 128);
    assert!(
        size_of::<(
            &PermissionDispatchClaim<'_>,
            &mut super::PermissionUseOutcomeAccount
        )>() <= 128
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionOutcomeClaimLoan<'_>,
            >,
        >() <= 128
    );
    assert!(size_of::<(&PermissionRuntimeUseWitness, &Arc<PermissionRuntimeOrigin>)>() <= 128);
};

// Scheduling observation only: the original opaque Claim remains authoritative.
impl PermissionDispatchClaim<'_> {
    pub(super) fn driver_prepared(
        &self,
        account: &mut super::driver_adapter_account::DriverAdapterAccount,
    ) -> Result<crate::PreparedEffect, PermissionError> {
        account.snapshot(&self.resource.execution)?;
        account.before_copy(&self.resource.execution)?;
        account.json_map(&self.invocation.input)?;
        account.before_copy(&(
            &self.invocation.execution_id,
            &self.invocation.turn_id,
            self.invocation.owner_generation,
            &self.invocation.task_id,
            &self.invocation.effect_id,
            &self.invocation.attempt_id,
            &self.invocation.input,
        ))?;
        Ok(crate::PreparedEffect {
            execution: self.resource.execution.clone(),
            invocation: self.invocation.clone(),
        })
    }
}

#[path = "original_always.rs"]
mod original_always;
pub(super) use original_always::OriginalAlwaysWork;
pub use original_always::{
    PermissionOriginalAlwaysCommit, PermissionOriginalAlwaysObservation,
    PermissionOriginalAlwaysOutcomeRequest, PermissionOriginalAlwaysRead,
    PermissionOriginalAlwaysSelector,
};

impl PermissionDispatchClaim<'_> {
    pub(super) fn withheld_use_loan<'a>(
        &'a self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionUseClaimLoan<'a>, PermissionError> {
        budget.runtime_reserve(512, 8)?;
        match &self.witness {
            PermissionClaimWitness::NewUse(witness) if witness.same_origin(&self.origin) => {
                Ok(PermissionUseClaimLoan {
                    resource: &self.resource,
                    invocation: &self.invocation,
                    historical_context: &self.context,
                    witness,
                    origin: &self.origin,
                })
            }
            _ => Err(PermissionError::UnsupportedProfile),
        }
    }
}

#[path = "original_always_withheld_cancellation.rs"]
mod original_always_withheld_cancellation;
pub use original_always_withheld_cancellation::{
    PermissionOriginalAlwaysWithheldCancellationCommit,
    PermissionOriginalAlwaysWithheldCancellationRequest,
    PermissionPreparedOriginalAlwaysWithheldCancellation,
};
