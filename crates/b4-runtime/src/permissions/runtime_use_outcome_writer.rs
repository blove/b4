//! Opaque outcome writer stages with trusted postcommit acknowledgement.

use super::runtime_mutation::PermissionRuntimeOrigin;
use super::runtime_use::PermissionRuntimeUseWitness;
use super::runtime_use_outcome::{
    OutcomeOrigin, PermissionUseOutcomeAccount, PermissionUseOutcomeRequest,
};
use super::runtime_use_outcome_source::{
    PermissionUseOutcomePrepared, PermissionUseOutcomeSource, PermissionUseOutcomeSourceSelector,
};
use super::{validation, PermissionError, PermissionEvidenceBudget, PermissionResourceRead};
use crate::approval::ApprovalResource;
use crate::{EffectInvocation, EffectOutcome, InvocationContext, JournalEntry};
use b4_contract::{CommitPlan, ExecutionId, KernelCommand, Snapshot, TransitionFacts};
use std::{mem::size_of, sync::Arc};

// New source-only fee proposals; no old cohort or budget donation is implied.
const WRITER_CALL_FIXED: usize = 4 * 4096 + 19 * (4096 + 2 * 4096) + 16 * 4096;
const WRITER_CALL_NODES: usize = 4 + 19 * 3 + 16;
struct OutcomeWriterOrigin;
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WriterStage {
    BeforeImmediate,
    ImmediateRequested,
    ImmediateAcknowledged,
    AwaitComparison,
    Compared,
    Encoded,
    ExecutionIssued,
    ExecutionParameters,
    ExecutionUpdated,
    ThreadIssued,
    ThreadParameters,
    ThreadUpdated,
    JournalIssued,
    JournalParameters,
    JournalInserted,
    Refused,
}
struct WriterProtocol {
    stage: WriterStage,
}
impl WriterProtocol {
    fn new() -> Self {
        Self {
            stage: WriterStage::BeforeImmediate,
        }
    }
    fn enter(&mut self, expected: WriterStage, next: WriterStage) -> Result<(), PermissionError> {
        let old = self.stage;
        self.stage = WriterStage::Refused;
        if old != expected {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.stage = next;
        Ok(())
    }
    fn acknowledge(
        &mut self,
        expected: WriterStage,
        next: WriterStage,
        rows: usize,
    ) -> Result<(), PermissionError> {
        let old = self.stage;
        self.stage = WriterStage::Refused;
        if old != expected || rows != 1 {
            return Err(PermissionError::CorruptRecord);
        }
        self.stage = next;
        Ok(())
    }
}
fn checked_iife<F, R>(value: F) -> R
where
    F: FnOnce() -> R,
{
    const {
        assert!(size_of::<F>() <= 4096);
    }
    value()
}

/// Borrowed genuine request/preparation; no public raw-plan constructor/getter.
pub struct PermissionUseOutcomeWriteSelector<'request, 'input> {
    request: &'request PermissionUseOutcomeRequest<'input>,
    prepared: &'request PermissionUseOutcomePrepared,
}
impl<'input> PermissionUseOutcomeRequest<'input> {
    pub fn prepare_write_selector<'request>(
        &'request self,
        prepared: &'request PermissionUseOutcomePrepared,
        account: &mut PermissionUseOutcomeAccount,
    ) -> Result<PermissionUseOutcomeWriteSelector<'request, 'input>, PermissionError> {
        account.source_selector_inception()?;
        let loan = self.source_loan();
        account.source_admission(loan.origin, loan.producer, loan.attempt)?;
        account.source_purchase(WRITER_CALL_FIXED, WRITER_CALL_NODES)?;
        let source = PermissionUseOutcomeSourceSelector::from_request(self);
        prepared.require_full_plan(&source, loan.current, account)?;
        Ok(PermissionUseOutcomeWriteSelector {
            request: self,
            prepared,
        })
    }
}
/// Owns only stage/serialized outputs; actual Receipt stays owned by Work.
pub struct PermissionUseOutcomeWriteSession<'account, 'request, 'input> {
    account: &'account mut PermissionUseOutcomeAccount,
    selector: &'account PermissionUseOutcomeWriteSelector<'request, 'input>,
    protocol: WriterProtocol,
    immediate: PermissionUseOutcomeImmediateSourceSelector<'input>,
    verified: Option<OutcomeVerified>,
    snapshot: Option<PermissionUseOutcomeEncodedSnapshot>,
    journal: Option<PermissionUseOutcomeEncodedJournal>,
}
struct OutcomeVerified {
    resource: ApprovalResource,
    plan: CommitPlan,
}
impl<'request, 'input> PermissionUseOutcomeWriteSelector<'request, 'input> {
    pub fn begin<'account>(
        &'account self,
        account: &'account mut PermissionUseOutcomeAccount,
    ) -> Result<PermissionUseOutcomeWriteSession<'account, 'request, 'input>, PermissionError> {
        let loan = self.request.source_loan();
        // Consume this live begin attempt before ANY fallible inception purchase.
        // This infallible genuine loan and first-refusal exterior remain priced obligations.
        account.begin_writer_attempt(loan.origin, loan.producer, loan.attempt)?;
        account.source_port_inception()?;
        account.source_purchase(8 * 4096 + WRITER_CALL_FIXED, WRITER_CALL_NODES + 8)?;
        let (_, resource, producer, origin, attempt) = self.prepared.writer_loan(account)?;
        if attempt == 0
            || attempt != loan.attempt
            || resource != loan.current
            || !Arc::ptr_eq(origin, loan.origin)
            || !Arc::ptr_eq(producer, loan.producer)
        {
            return Err(PermissionError::CorruptRecord);
        }
        let writer = Arc::new(OutcomeWriterOrigin);
        Ok(PermissionUseOutcomeWriteSession {
            account,
            selector: self,
            protocol: WriterProtocol::new(),
            immediate: PermissionUseOutcomeImmediateSourceSelector {
                resource: loan.resource,
                current: loan.current,
                controlled: loan.controlled,
                witness: loan.witness,
                producer: loan.producer,
                operation: loan.origin,
                context: loan.context,
                invocation: loan.invocation,
                actual: loan.actual,
                facts: loan.facts,
                attempt: loan.attempt,
                writer,
                round: 1,
            },
            verified: None,
            snapshot: None,
            journal: None,
        })
    }
}
impl PermissionUseOutcomeWriteSession<'_, '_, '_> {
    // Consume the eligible stage BEFORE the first entered fallible purchase.
    fn take_stage(&mut self, expected: WriterStage) -> Result<(), PermissionError> {
        let old = self.protocol.stage;
        self.protocol.stage = WriterStage::Refused;
        self.account
            .source_purchase(WRITER_CALL_FIXED, WRITER_CALL_NODES)?;
        self.protocol.stage = old;
        self.protocol.enter(expected, WriterStage::Refused)
    }
    pub fn before_begin_immediate(&mut self) -> Result<(), PermissionError> {
        self.take_stage(WriterStage::BeforeImmediate)?;
        // Dedicated native lock/transaction/error inception proposal.
        self.account
            .source_purchase(8 * 512 + 19 * (512 + 2 * 128) + 16 * 128, 4 + 19 * 3 + 16)?;
        self.protocol.stage = WriterStage::ImmediateRequested;
        Ok(())
    }
    /// Trusted typed backend acknowledgement after obtaining actual Immediate.
    pub fn acknowledge_immediate_owner(&mut self, owner: u64) -> Result<(), PermissionError> {
        self.take_stage(WriterStage::ImmediateRequested)?;
        if owner != self.immediate.resource.execution.snapshot.owner_generation {
            return Err(PermissionError::CorruptRecord);
        }
        self.protocol.stage = WriterStage::ImmediateAcknowledged;
        Ok(())
    }
    pub fn begin_source_read(
        &mut self,
    ) -> Result<PermissionUseOutcomeImmediateSourceSession<'_, '_>, PermissionError> {
        self.take_stage(WriterStage::ImmediateAcknowledged)?;
        self.account.source_admission(
            self.immediate.operation,
            self.immediate.producer,
            self.immediate.attempt,
        )?;
        self.account.source_purchase(
            SOURCE_STATE_FIXED + WRITER_CALL_FIXED,
            128 + WRITER_CALL_NODES,
        )?;
        if !self.immediate.witness.same_origin(self.immediate.producer) {
            return Err(PermissionError::CorruptRecord);
        }
        // While a reader lives, the parent writer remains Refused. Only genuine
        // successful reader finish advances AwaitComparison; drop/errors cannot.
        let mut state = ImmediateSourceState::new();
        state.ranges = ImmediateRanges::for_witness(self.immediate.witness)?;
        Ok(PermissionUseOutcomeImmediateSourceSession {
            account: &mut *self.account,
            selector: &self.immediate,
            protocol: &mut self.protocol,
            state: Box::new(state),
            headers_ended: [false; 12],
            payloads_ended: [false; 12],
            poisoned: false,
        })
    }
    pub fn compare_source(
        &mut self,
        capture: PermissionUseOutcomeImmediateSourceCapture,
    ) -> Result<(), PermissionError> {
        self.take_stage(WriterStage::AwaitComparison)?;
        if capture.round == 0
            || capture.round != self.immediate.round
            || capture.attempt == 0
            || capture.attempt != self.immediate.attempt
            || !Arc::ptr_eq(&capture.writer, &self.immediate.writer)
            || !Arc::ptr_eq(&capture.origin, self.immediate.operation)
            || !Arc::ptr_eq(&capture.producer, self.immediate.producer)
        {
            return Err(PermissionError::CorruptRecord);
        }
        let source = PermissionUseOutcomeSourceSelector::from_request(self.selector.request);
        self.selector
            .prepared
            .require_full_plan(&source, &capture.resource, self.account)?;
        let (expected, resource, _, _, _) = self.selector.prepared.writer_loan(self.account)?;
        validation::use_outcome_quote_resource_pair(
            resource,
            &capture.resource,
            self.account.source_budget()?,
        )?;
        validation::use_outcome_quote_plan_pair(
            expected,
            &capture.plan,
            self.account.source_budget()?,
        )?;
        if resource != &capture.resource || expected != &capture.plan {
            return Err(PermissionError::CorruptRecord);
        }
        self.verified = Some(OutcomeVerified {
            resource: capture.resource,
            plan: capture.plan,
        });
        self.protocol.stage = WriterStage::Compared;
        Ok(())
    }
    pub fn encode(&mut self) -> Result<(), PermissionError> {
        self.take_stage(WriterStage::Compared)?;
        let verified = self
            .verified
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let next = verified
            .resource
            .thread
            .resource_revision
            .checked_add(1)
            .ok_or(PermissionError::LimitExceeded)?;
        let snapshot = validation::use_outcome_encode_snapshot(
            &verified.plan.snapshot,
            self.account.source_budget()?,
        )?;
        let typed = validation::use_outcome_journal(
            &verified.plan.snapshot,
            self.account.source_budget()?,
        )?;
        let journal =
            validation::use_outcome_encode_journal(&typed, self.account.source_budget()?)?;
        self.account
            .source_purchase(8 * 4096 + 8 * WRITER_CALL_FIXED, 8 + 8 * WRITER_CALL_NODES)?;
        self.snapshot = Some(PermissionUseOutcomeEncodedSnapshot {
            bytes: snapshot,
            writer: Arc::clone(&self.immediate.writer),
        });
        self.journal = Some(PermissionUseOutcomeEncodedJournal {
            bytes: journal,
            typed,
            writer: Arc::clone(&self.immediate.writer),
            next_resource_revision: next,
        });
        self.protocol.stage = WriterStage::Encoded;
        Ok(())
    }
    pub fn execution_cas_loan(
        &mut self,
    ) -> Result<PermissionUseOutcomeExecutionCasLoan<'_>, PermissionError> {
        self.take_stage(WriterStage::Encoded)?;
        let verified = self
            .verified
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let encoded = self
            .snapshot
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        if !Arc::ptr_eq(&encoded.writer, &self.immediate.writer) {
            return Err(PermissionError::CorruptRecord);
        }
        self.protocol.stage = WriterStage::ExecutionIssued;
        Ok(PermissionUseOutcomeExecutionCasLoan {
            account: &mut *self.account,
            protocol: &mut self.protocol,
            execution: verified.resource.execution.snapshot.execution_id.as_str(),
            payload: &encoded.bytes,
            expected: verified.resource.execution.snapshot.revision.to_be_bytes(),
            owner: verified
                .resource
                .execution
                .snapshot
                .owner_generation
                .to_be_bytes(),
            next: verified.plan.snapshot.revision.to_be_bytes(),
        })
    }
    pub fn record_execution_rows(&mut self, rows: usize) -> Result<(), PermissionError> {
        self.record_rows(
            WriterStage::ExecutionParameters,
            WriterStage::ExecutionUpdated,
            rows,
        )
    }
    pub fn thread_cas_loan(
        &mut self,
    ) -> Result<PermissionUseOutcomeThreadCasLoan<'_>, PermissionError> {
        self.take_stage(WriterStage::ExecutionUpdated)?;
        let verified = self
            .verified
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let journal = self
            .journal
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        if !Arc::ptr_eq(&journal.writer, &self.immediate.writer) {
            return Err(PermissionError::CorruptRecord);
        }
        self.protocol.stage = WriterStage::ThreadIssued;
        Ok(PermissionUseOutcomeThreadCasLoan {
            account: &mut *self.account,
            protocol: &mut self.protocol,
            thread: verified.resource.thread.id.as_str(),
            generation: verified.resource.thread.generation.as_str(),
            active: verified.resource.execution.snapshot.execution_id.as_str(),
            expected: verified.resource.thread.resource_revision.to_be_bytes(),
            next: journal.next_resource_revision.to_be_bytes(),
        })
    }
    pub fn record_thread_rows(&mut self, rows: usize) -> Result<(), PermissionError> {
        self.record_rows(
            WriterStage::ThreadParameters,
            WriterStage::ThreadUpdated,
            rows,
        )
    }
    pub fn journal_insert_loan(
        &mut self,
    ) -> Result<PermissionUseOutcomeJournalInsertLoan<'_>, PermissionError> {
        self.take_stage(WriterStage::ThreadUpdated)?;
        let verified = self
            .verified
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        let journal = self
            .journal
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        if !Arc::ptr_eq(&journal.writer, &self.immediate.writer) {
            return Err(PermissionError::CorruptRecord);
        }
        self.protocol.stage = WriterStage::JournalIssued;
        Ok(PermissionUseOutcomeJournalInsertLoan {
            account: &mut *self.account,
            protocol: &mut self.protocol,
            thread: verified.resource.thread.id.as_str(),
            execution: journal.typed.execution_id.as_str(),
            payload: &journal.bytes,
            revision: journal.typed.revision.to_be_bytes(),
            owner: journal.typed.owner_generation.to_be_bytes(),
            time: journal.typed.updated_at_ms.to_be_bytes(),
            status: validation::ready_use_status(journal.typed.status),
        })
    }
    pub fn record_journal_rows(&mut self, rows: usize) -> Result<(), PermissionError> {
        self.record_rows(
            WriterStage::JournalParameters,
            WriterStage::JournalInserted,
            rows,
        )
    }
    fn record_rows(
        &mut self,
        expected: WriterStage,
        next: WriterStage,
        rows: usize,
    ) -> Result<(), PermissionError> {
        let old = self.protocol.stage;
        self.protocol.stage = WriterStage::Refused;
        self.account
            .source_purchase(WRITER_CALL_FIXED, WRITER_CALL_NODES)?;
        self.protocol.stage = old;
        self.protocol.acknowledge(expected, next, rows)
    }
    /// Prepare all fallible work before asking the backend to commit SQL.
    pub fn before_commit(mut self) -> Result<PermissionUseOutcomePendingCommit, PermissionError> {
        self.take_stage(WriterStage::JournalInserted)?;
        self.account
            .source_purchase(8 * 4096 + WRITER_CALL_FIXED, 8 + WRITER_CALL_NODES)?;
        let verified = self.verified.take().ok_or(PermissionError::CorruptRecord)?;
        Ok(PermissionUseOutcomePendingCommit {
            plan: verified.plan,
            resource: verified.resource,
            producer: Arc::clone(self.immediate.producer),
            origin: Arc::clone(self.immediate.operation),
            attempt: self.immediate.attempt,
            now_ms: self.immediate.facts.now_ms,
        })
    }
}
/// Prepared private result; reaching this stage is not evidence of SQL commit.
/// The trusted Store adapter must first obtain the real database commit acknowledgement.
pub struct PermissionUseOutcomePendingCommit {
    plan: CommitPlan,
    resource: ApprovalResource,
    producer: Arc<PermissionRuntimeOrigin>,
    origin: Arc<OutcomeOrigin>,
    attempt: u64,
    now_ms: u64,
}
impl PermissionUseOutcomePendingCommit {
    /// Trusted Store boundary: call only immediately after successful SQL commit.
    pub fn acknowledge_database_commit(self) -> super::PermissionUseOutcomeCommit {
        super::PermissionUseOutcomeCommit {
            plan: self.plan,
            resource: self.resource,
            producer: self.producer,
            origin: self.origin,
            attempt: self.attempt,
            now_ms: self.now_ms,
        }
    }
}
const _: () = {
    assert!(size_of::<PermissionUseOutcomePendingCommit>() <= 8192);
    assert!(size_of::<Result<PermissionUseOutcomePendingCommit, PermissionError>>() <= 8192);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseOutcomePendingCommit,
            >,
        >() <= 8192
    );
};
/// Serialized owners retained by the write session; no public byte factory.
pub struct PermissionUseOutcomeEncodedSnapshot {
    bytes: String,
    writer: Arc<OutcomeWriterOrigin>,
}
pub struct PermissionUseOutcomeEncodedJournal {
    bytes: String,
    typed: JournalEntry,
    writer: Arc<OutcomeWriterOrigin>,
    next_resource_revision: u64,
}

pub struct PermissionUseOutcomeExecutionCasLoan<'a> {
    account: &'a mut PermissionUseOutcomeAccount,
    protocol: &'a mut WriterProtocol,
    execution: &'a str,
    payload: &'a str,
    expected: [u8; 8],
    owner: [u8; 8],
    next: [u8; 8],
}
pub struct PermissionUseOutcomeThreadCasLoan<'a> {
    account: &'a mut PermissionUseOutcomeAccount,
    protocol: &'a mut WriterProtocol,
    thread: &'a str,
    generation: &'a str,
    active: &'a str,
    expected: [u8; 8],
    next: [u8; 8],
}
pub struct PermissionUseOutcomeJournalInsertLoan<'a> {
    account: &'a mut PermissionUseOutcomeAccount,
    protocol: &'a mut WriterProtocol,
    thread: &'a str,
    execution: &'a str,
    payload: &'a str,
    revision: [u8; 8],
    owner: [u8; 8],
    time: [u8; 8],
    status: &'static str,
}
pub type PermissionUseOutcomeExecutionCasParameters<'a> =
    (&'a str, &'a str, &'a [u8], &'a [u8], &'a [u8]);
pub type PermissionUseOutcomeThreadCasParameters<'a> =
    (&'a str, &'a str, &'a str, &'a [u8], &'a [u8]);
pub type PermissionUseOutcomeJournalInsertParameters<'a> = (
    &'a str,
    &'a str,
    &'a [u8],
    &'a [u8],
    &'a str,
    &'a [u8],
    &'a str,
);

impl PermissionUseOutcomeExecutionCasLoan<'_> {
    pub fn parameters(
        &mut self,
    ) -> Result<PermissionUseOutcomeExecutionCasParameters<'_>, PermissionError> {
        let old = self.protocol.stage;
        self.protocol.stage = WriterStage::Refused;
        self.account
            .source_purchase(WRITER_CALL_FIXED, WRITER_CALL_NODES)?;
        if old != WriterStage::ExecutionIssued {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.protocol.stage = WriterStage::ExecutionParameters;
        Ok((
            self.execution,
            self.payload,
            &self.expected,
            &self.owner,
            &self.next,
        ))
    }
}
impl PermissionUseOutcomeThreadCasLoan<'_> {
    pub fn parameters(
        &mut self,
    ) -> Result<PermissionUseOutcomeThreadCasParameters<'_>, PermissionError> {
        let old = self.protocol.stage;
        self.protocol.stage = WriterStage::Refused;
        self.account
            .source_purchase(WRITER_CALL_FIXED, WRITER_CALL_NODES)?;
        if old != WriterStage::ThreadIssued {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.protocol.stage = WriterStage::ThreadParameters;
        Ok((
            self.thread,
            self.generation,
            self.active,
            &self.expected,
            &self.next,
        ))
    }
}
impl PermissionUseOutcomeJournalInsertLoan<'_> {
    pub fn parameters(
        &mut self,
    ) -> Result<PermissionUseOutcomeJournalInsertParameters<'_>, PermissionError> {
        let old = self.protocol.stage;
        self.protocol.stage = WriterStage::Refused;
        self.account
            .source_purchase(WRITER_CALL_FIXED, WRITER_CALL_NODES)?;
        if old != WriterStage::JournalIssued {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.protocol.stage = WriterStage::JournalParameters;
        Ok((
            self.thread,
            self.execution,
            &self.revision,
            &self.owner,
            self.status,
            &self.time,
            self.payload,
        ))
    }
}
// Source controls, row adapters, and whole helper/carrier cohorts are fresh.
// These are proposals only; no renewal or callback debit funds this graph.
pub(super) const LEAF_FIXED: usize = 4 * 128 + 4 * 128;
pub(super) const SOURCE_CALL_FIXED: usize = 4 * 4096 + 19 * (4096 + 2 * 4096) + 16 * 4096; // Provisional Immediate wrapper/current helpers.
pub(super) const SDK_ACTION_FIXED: usize = 128 * 128
    + 7 * 512
    + 19 * (3 * 512 + 6 * 64)
    + (16 * 512 + 4 * 64)
    + 14 * (19 * 64 + 8 * 64 + 32 * 32)
    + 4 * (19 * (128 + 2 * 64) + 16 * 128);
pub(super) const ADAPTER_ACTION_FIXED: usize =
    14 * (4 * 19 * 128 + 3 * 16 * 128) + 8 * 512 + 16 * 128;
pub(super) const OBSERVATION_BOUND: usize = 2048;
pub(super) const CODE_ONLY_FIXED: usize = 5 * OBSERVATION_BOUND
    + 6 * 128
    + 3 * 128
    + 8 * 128
    + 256
    + 128
    + 4 * 32
    + 8 * (SDK_ACTION_FIXED + ADAPTER_ACTION_FIXED);
const SOURCE_STATE_FIXED: usize = 12 * 4096 + 32 * 128; // New retained full Resource owner.

fn outcome_checked_value<F>(value: F) -> F {
    const {
        assert!(size_of::<F>() <= 128);
    }
    value
}

struct PermissionUseOutcomeImmediateSourceSelector<'input> {
    resource: &'input ApprovalResource,
    current: &'input ApprovalResource,
    controlled: bool,
    witness: &'input PermissionRuntimeUseWitness,
    producer: &'input Arc<PermissionRuntimeOrigin>,
    operation: &'input Arc<OutcomeOrigin>,
    context: &'input InvocationContext,
    invocation: &'input EffectInvocation,
    actual: &'input EffectOutcome,
    facts: &'input TransitionFacts,
    attempt: u64,
    writer: Arc<OutcomeWriterOrigin>,
    round: u64,
}
/// Constructible only by a genuine writer session after Immediate acknowledgement.
pub struct PermissionUseOutcomeImmediateSourceSession<'account, 'input> {
    account: &'account mut PermissionUseOutcomeAccount,
    selector: &'account PermissionUseOutcomeImmediateSourceSelector<'input>,
    protocol: &'account mut WriterProtocol,
    state: Box<ImmediateSourceState>,
    headers_ended: [bool; 12],
    payloads_ended: [bool; 12],
    poisoned: bool,
}
/// Not convertible from Deferred Prepared. It owns full current Resource/plan.
pub struct PermissionUseOutcomeImmediateSourceCapture {
    resource: ApprovalResource,
    plan: CommitPlan,
    producer: Arc<PermissionRuntimeOrigin>,
    origin: Arc<OutcomeOrigin>,
    attempt: u64,
    writer: Arc<OutcomeWriterOrigin>,
    round: u64,
}
struct ImmediateSourceState {
    resource: Option<ApprovalResource>,
    ranges: ImmediateRanges,
    rows: [usize; 12],
    payloads: [usize; 12],
    stage: Option<(PermissionUseOutcomeSource, Option<usize>)>,
    resource_matched: bool,
    payload_capture_attempted: bool,
    payload_captured: bool,
    dispatch_fields: Vec<Vec<u8>>,
    dispatch_sequence: i64,
    dispatch_bytes: usize,
    dispatch: Option<crate::JournalEntry>,
    cancellation: Option<crate::JournalEntry>,
    cancel_sequence: i64,
    cancel_bytes: usize,
    creator_lengths: Option<(usize, usize)>,
}
impl ImmediateSourceState {
    fn new() -> Self {
        Self {
            ranges: ImmediateRanges::new(),
            resource: None,
            rows: [0; 12],
            payloads: [0; 12],
            stage: None,
            resource_matched: false,
            payload_capture_attempted: false,
            payload_captured: false,
            dispatch_fields: Vec::new(),
            dispatch_sequence: 0,
            dispatch_bytes: 0,
            dispatch: None,
            cancellation: None,
            cancel_sequence: 0,
            cancel_bytes: 0,
            creator_lengths: None,
        }
    }
}
impl<'account, 'input> PermissionUseOutcomeImmediateSourceSession<'account, 'input> {
    /// True only for the exact current cancellation projection in this source.
    pub fn has_cancellation_journal(&self) -> bool {
        self.selector.controlled && self.selector.current != self.selector.resource
    }
    pub fn expected_count(&self, source: PermissionUseOutcomeSource) -> usize {
        if source == PermissionUseOutcomeSource::OriginalJournal {
            self.selector.witness.original_journal_count()
        } else if source == PermissionUseOutcomeSource::CurrentJournal {
            if self.selector.current == self.selector.resource {
                4
            } else {
                5
            }
        } else {
            source.count()
        }
    }

    fn control(&mut self) -> Result<(), PermissionError> {
        self.account.source_purchase(LEAF_FIXED, 8)?;
        self.account.source_purchase(SOURCE_CALL_FIXED, 64)
    }
    pub fn before_code_only_scope(&mut self) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            // C2's Resource8 + twelve complete header ranges + fourteen payload
            // points are independent query incarnations. Six is the largest
            // reached header/terminal-row count (originalJ5 + sentinel).
            self.account.source_purchase(CODE_ONLY_FIXED, 40)?;
            // Fresh owned Resource -> session match Args, moves, vacant cells and
            // retirement controls are distinct from the existing reader shim.
            self.account.source_purchase(16 * OBSERVATION_BOUND, 64)?;
            self.account
                .source_purchase(26 * (SDK_ACTION_FIXED + ADAPTER_ACTION_FIXED), 104)
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn current_execution_id(&mut self) -> Result<&'input ExecutionId, PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            Ok(&self.selector.resource.execution.snapshot.execution_id)
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn resource_read(&mut self) -> Result<PermissionResourceRead<'_>, PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = self.account.source_resource_read();
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }

    pub fn match_resource(&mut self, resource: ApprovalResource) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            validation::use_outcome_quote_resource_pair(
                self.selector.resource,
                &resource,
                self.account.source_budget()?,
            )?;
            super::use_cancellation_relation::require_current(
                self.selector.resource,
                &resource,
                self.selector.controlled,
            )?;
            if resource != *self.selector.current {
                return Err(PermissionError::CorruptRecord);
            }
            if self.state.resource.is_some() {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.ranges.current_journal_count = if resource == *self.selector.resource {
                4
            } else {
                5
            };
            self.state.resource = Some(resource);
            self.state.resource_matched = true;
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn header_key(
        &mut self,
        source: PermissionUseOutcomeSource,
    ) -> Result<&'input str, PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            let retained = self
                .selector
                .witness
                .outcome_source_loan(self.account.source_budget()?)?;
            Ok(match source {
                PermissionUseOutcomeSource::ThreadDecision => {
                    self.selector.resource.thread.id.as_str()
                }
                PermissionUseOutcomeSource::GlobalUseIdentity => {
                    retained.use_record.use_id().as_str()
                }
                PermissionUseOutcomeSource::OriginalAcceptance
                | PermissionUseOutcomeSource::OriginalGrant
                | PermissionUseOutcomeSource::OriginalLegacyAcceptance
                | PermissionUseOutcomeSource::OriginalJournal
                | PermissionUseOutcomeSource::Creator => {
                    retained.creator.snapshot.execution_id.as_str()
                }
                _ => self
                    .selector
                    .resource
                    .execution
                    .snapshot
                    .execution_id
                    .as_str(),
            })
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn before_headers(
        &mut self,
        source: PermissionUseOutcomeSource,
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if !self.state.resource_matched
                || self.state.stage.is_some()
                || self.headers_ended[source.index()]
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.ranges.begin_headers(source)?;
            self.state.stage = Some((source, None));
            self.account.source_purchase(10_240, 1)
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            // Native row advance/cell adapters get a new independent current ticket.
            self.account
                .source_purchase(SDK_ACTION_FIXED + ADAPTER_ACTION_FIXED, 4)
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn admit_header(
        &mut self,
        source: PermissionUseOutcomeSource,
        cells: super::PermissionRuntimeUseHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if self.state.stage != Some((source, None)) {
                return Err(PermissionError::CorruptRecord);
            }
            let index = self.state.rows[source.index()];
            if cells.fields.len() > 8 {
                return Err(PermissionError::CorruptRecord);
            }
            // At most eight length callbacks and eight checked accumulator Try
            // incarnations; reserve independently before either traversal.
            self.account
                .source_purchase(16 * SOURCE_CALL_FIXED, 16 * 64)?;
            if cells
                .fields
                .iter()
                .any(outcome_checked_value(|v: &&[u8]| v.len() > 256))
            {
                return Err(PermissionError::CorruptRecord);
            }
            let total = cells.fields.iter().try_fold(
                0usize,
                outcome_checked_value(|n: usize, v: &&[u8]| {
                    n.checked_add(v.len()).ok_or(PermissionError::LimitExceeded)
                }),
            )?;
            self.account
                .source_purchase(2 * total + 512, cells.fields.len())?;
            if self.expected_count(source) == 0 {
                // The adapter already checks full bounded native types. Validate
                // every reached identifier/blob/version/length before refusing.
                let identifiers = match source {
                    PermissionUseOutcomeSource::CurrentAcceptance => 3,
                    PermissionUseOutcomeSource::CurrentGrant => 4,
                    _ => 2,
                };
                let expected_fields = match source {
                    PermissionUseOutcomeSource::CurrentAcceptance => 6,
                    PermissionUseOutcomeSource::CurrentGrant => 6,
                    _ => 2,
                };
                if cells.fields.len() != expected_fields {
                    return Err(PermissionError::CorruptRecord);
                }
                for field in &cells.fields[..identifiers] {
                    // One native UTF-8 Try plus bounded per-byte character calls.
                    self.account.source_purchase(
                        SOURCE_CALL_FIXED + field.len() * (4 * 128),
                        64 + field.len(),
                    )?;
                    let text = std::str::from_utf8(field)
                        .map_err(outcome_checked_value(|_| PermissionError::CorruptRecord))?;
                    if text.is_empty()
                        || text.len() > 256
                        || text.chars().any(outcome_checked_value(char::is_control))
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
                let execution = match source {
                    PermissionUseOutcomeSource::OriginalLegacyAcceptance => {
                        let retained = self
                            .selector
                            .witness
                            .outcome_source_loan(self.account.source_budget()?)?;
                        retained.creator.snapshot.execution_id.as_str()
                    }
                    _ => self
                        .selector
                        .resource
                        .execution
                        .snapshot
                        .execution_id
                        .as_str(),
                };
                if cells.fields.first().copied() != Some(execution.as_bytes()) {
                    return Err(PermissionError::CorruptRecord);
                }
                if matches!(
                    source,
                    PermissionUseOutcomeSource::CurrentAcceptance
                        | PermissionUseOutcomeSource::CurrentGrant
                ) {
                    if cells.record_bytes == 0 || cells.record_bytes > 65_536 {
                        return Err(PermissionError::CorruptRecord);
                    }
                    let generation = cells
                        .fields
                        .get(if source == PermissionUseOutcomeSource::CurrentGrant {
                            4
                        } else {
                            3
                        })
                        .ok_or(PermissionError::CorruptRecord)?;
                    self.account.source_purchase(64 * 4 * 128, 64)?;
                    if generation.len() != 64
                        || !generation.iter().all(outcome_checked_value(|b: &u8| {
                            b.is_ascii_digit() || (b'a'..=b'f').contains(b)
                        }))
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    let thread_index = if source == PermissionUseOutcomeSource::CurrentGrant {
                        3
                    } else {
                        2
                    };
                    if cells.fields[thread_index]
                        != self.selector.resource.thread.id.as_str().as_bytes()
                        || *generation
                            != self.selector.resource.thread.generation.as_str().as_bytes()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                    let blob_start = if source == PermissionUseOutcomeSource::CurrentGrant {
                        5
                    } else {
                        4
                    };
                    for field in &cells.fields[blob_start..] {
                        if field.len() != 8 {
                            return Err(PermissionError::CorruptRecord);
                        }
                    }
                    self.account.source_purchase(3 * 19 * 128 + 16 * 128, 64)?;
                    if source == PermissionUseOutcomeSource::CurrentAcceptance {
                        let previous = outcome_be_u64(cells.fields[4])?;
                        let committed = outcome_be_u64(cells.fields[5])?;
                        if previous.checked_add(1) != Some(committed) {
                            return Err(PermissionError::CorruptRecord);
                        }
                    } else if outcome_be_u64(cells.fields[5])? == 0 {
                        return Err(PermissionError::CorruptRecord);
                    }
                    if source == PermissionUseOutcomeSource::CurrentAcceptance && cells.scalar != 1
                        || source == PermissionUseOutcomeSource::CurrentGrant
                            && !matches!(cells.scalar, 0 | 1)
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
                return Err(PermissionError::UnsupportedProfile);
            }
            if index >= self.expected_count(source) {
                return Err(PermissionError::CorruptRecord);
            }
            if source == PermissionUseOutcomeSource::CurrentJournal && index >= 3 {
                let snap = if index == 3 {
                    &self.selector.resource.execution.snapshot
                } else {
                    &self.selector.current.execution.snapshot
                };
                let retained = self
                    .selector
                    .witness
                    .outcome_source_loan(self.account.source_budget()?)?;
                let (_, last, _, _) =
                    retained.outcome_window(source, 2, self.account.source_budget()?)?;
                if cells.fields.len() != 7
                    || cells.scalar <= last
                    || (index == 4 && cells.scalar <= self.state.dispatch_sequence)
                    || cells.fields[0] != b"commit"
                    || cells.fields[1] != self.selector.resource.thread.id.as_str().as_bytes()
                    || cells.fields[2] != snap.execution_id.as_str().as_bytes()
                    || cells.fields[3] != snap.revision.to_be_bytes()
                    || cells.fields[4] != snap.owner_generation.to_be_bytes()
                    || cells.fields[5] != validation::ready_use_status(snap.status).as_bytes()
                    || cells.fields[6] != snap.updated_at_ms.to_be_bytes()
                    || cells.record_bytes == 0
                    || cells.record_bytes > 65_536
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.account.source_purchase(
                    13 * total + 256 * cells.fields.len() + 512,
                    2 * cells.fields.len() + 2,
                )?;
                // The outer allocator and each inner allocator have distinct native,
                // mapped, Try, Args and closure tickets, before their first call.
                self.account.source_purchase(SOURCE_CALL_FIXED, 64)?;
                let mut fields = Vec::new();
                fields
                    .try_reserve_exact(cells.fields.len())
                    .map_err(outcome_checked_value(|_| PermissionError::LimitExceeded))?;
                for field in cells.fields {
                    self.account.source_purchase(SOURCE_CALL_FIXED, 64)?;
                    let mut value = Vec::new();
                    value
                        .try_reserve_exact(field.len())
                        .map_err(outcome_checked_value(|_| PermissionError::LimitExceeded))?;
                    value.extend_from_slice(field);
                    fields.push(value);
                }
                if index == 3 {
                    self.state.dispatch_fields = fields;
                    self.state.dispatch_sequence = cells.scalar;
                    self.state.dispatch_bytes = cells.record_bytes;
                } else {
                    self.state.cancel_sequence = cells.scalar;
                    self.state.cancel_bytes = cells.record_bytes;
                }
            } else {
                let retained = self
                    .selector
                    .witness
                    .outcome_source_loan(self.account.source_budget()?)?;
                let (fields, scalar, bytes, _) =
                    retained.outcome_window(source, index, self.account.source_budget()?)?;
                self.account.source_purchase(8 * 4 * 128, 8)?;
                if fields.len() != cells.fields.len()
                    || scalar != cells.scalar
                    || bytes != cells.record_bytes
                    || fields.iter().zip(cells.fields).any(outcome_checked_value(
                        |(a, b): (&Vec<u8>, &&[u8])| a.as_slice() != *b,
                    ))
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            self.state.rows[source.index()] += 1;
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn end_headers(
        &mut self,
        source: PermissionUseOutcomeSource,
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if self.state.stage != Some((source, None))
                || self.state.rows[source.index()] != self.expected_count(source)
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state
                .ranges
                .close_headers(source, self.state.rows[source.index()])?;
            self.headers_ended[source.index()] = true;
            self.state.stage = None;
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn admit_creator_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if self.state.stage != Some((PermissionUseOutcomeSource::Creator, None))
                || self.state.creator_lengths.is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
            let retained = self
                .selector
                .witness
                .outcome_source_loan(self.account.source_budget()?)?;
            let (fields, _, bytes, _) = retained.outcome_window(
                PermissionUseOutcomeSource::Creator,
                0,
                self.account.source_budget()?,
            )?;
            if thread > 256
                || snapshot > 65_536
                || fields.first().map(outcome_checked_value(Vec::len)) != Some(thread)
                || bytes != snapshot
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.account.source_purchase(thread + snapshot + 512, 8)?;
            self.state.creator_lengths = Some((thread, snapshot));
            self.state.rows[PermissionUseOutcomeSource::Creator.index()] = 1;
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn creator_lengths(&mut self) -> Result<(usize, usize), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            self.state
                .creator_lengths
                .ok_or(PermissionError::CorruptRecord)
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn before_payload(
        &mut self,
        source: PermissionUseOutcomeSource,
        index: usize,
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if !self
                .headers_ended
                .iter()
                .all(outcome_checked_value(|v: &bool| *v))
                || self.state.stage.is_some()
                || index >= self.expected_count(source)
                || self.state.payloads[source.index()] != index
                || self.state.payload_capture_attempted
                || self.state.payload_captured
                || source == PermissionUseOutcomeSource::GlobalUseIdentity
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.stage = Some((source, Some(index)));
            self.account.source_purchase(10_240, 1)
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn payload_key(
        &mut self,
        source: PermissionUseOutcomeSource,
        index: usize,
    ) -> Result<(&'input str, &'input str, i64, u64, usize), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            let retained = self
                .selector
                .witness
                .outcome_source_loan(self.account.source_budget()?)?;
            let current = self
                .selector
                .resource
                .execution
                .snapshot
                .execution_id
                .as_str();
            if source == PermissionUseOutcomeSource::CurrentJournal && index >= 3 {
                return Ok((
                    current,
                    "",
                    if index == 3 {
                        self.state.dispatch_sequence
                    } else {
                        self.state.cancel_sequence
                    },
                    if index == 3 {
                        self.selector.resource.execution.snapshot.revision
                    } else {
                        self.selector.current.execution.snapshot.revision
                    },
                    if index == 3 {
                        self.state.dispatch_bytes
                    } else {
                        self.state.cancel_bytes
                    },
                ));
            }
            let (fields, scalar, bytes, _) =
                retained.outcome_window(source, index, self.account.source_budget()?)?;
            // Each reached native conversion owns a separate nominal/Try ticket.
            // Dispatch index3 returned above has no conversions.
            let native_conversions = match source {
                PermissionUseOutcomeSource::CurrentUse => 2,
                PermissionUseOutcomeSource::ThreadDecision
                | PermissionUseOutcomeSource::OriginalAcceptance
                | PermissionUseOutcomeSource::OriginalGrant => 1,
                PermissionUseOutcomeSource::OriginalJournal
                | PermissionUseOutcomeSource::CurrentJournal => 2,
                _ => return Err(PermissionError::CorruptRecord),
            };
            self.account.source_purchase(
                native_conversions * SOURCE_CALL_FIXED,
                native_conversions * 64,
            )?;
            let text = outcome_checked_value(|v: &'input [u8]| {
                std::str::from_utf8(v)
                    .map_err(outcome_checked_value(|_| PermissionError::CorruptRecord))
            });
            Ok(match source {
                PermissionUseOutcomeSource::CurrentUse => {
                    (text(&fields[1])?, text(&fields[2])?, 0, 0, bytes)
                }
                PermissionUseOutcomeSource::ThreadDecision => (text(&fields[0])?, "", 0, 0, bytes),
                PermissionUseOutcomeSource::OriginalAcceptance => (
                    retained.creator.snapshot.execution_id.as_str(),
                    text(&fields[1])?,
                    0,
                    0,
                    bytes,
                ),
                PermissionUseOutcomeSource::OriginalGrant => (
                    retained.creator.snapshot.execution_id.as_str(),
                    text(&fields[1])?,
                    0,
                    0,
                    bytes,
                ),
                PermissionUseOutcomeSource::OriginalJournal
                | PermissionUseOutcomeSource::CurrentJournal => {
                    let rev = u64::from_be_bytes(
                        fields[3]
                            .as_slice()
                            .try_into()
                            .map_err(outcome_checked_value(|_| PermissionError::CorruptRecord))?,
                    );
                    (text(&fields[2])?, "", scalar, rev, bytes)
                }
                _ => return Err(PermissionError::CorruptRecord),
            })
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn capture_payload(
        &mut self,
        source: PermissionUseOutcomeSource,
        index: usize,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if self.state.stage != Some((source, Some(index)))
                || self.state.payload_capture_attempted
                || self.state.payload_captured
                || !matches!(
                    source,
                    PermissionUseOutcomeSource::CurrentUse
                        | PermissionUseOutcomeSource::ThreadDecision
                        | PermissionUseOutcomeSource::OriginalAcceptance
                        | PermissionUseOutcomeSource::OriginalGrant
                        | PermissionUseOutcomeSource::OriginalJournal
                        | PermissionUseOutcomeSource::CurrentJournal
                )
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.payload_capture_attempted = true;
            if raw.len() > 65_536 {
                return Err(PermissionError::LimitExceeded);
            }
            self.account.source_purchase(2 * raw.len() + 256, 2)?;
            if source == PermissionUseOutcomeSource::CurrentJournal && index >= 3 {
                if raw.len()
                    != if index == 3 {
                        self.state.dispatch_bytes
                    } else {
                        self.state.cancel_bytes
                    }
                {
                    return Err(PermissionError::CorruptRecord);
                }
                // The retained library envelope is independently applied to this
                // new Journal decoder incarnation. Its new accepted/partial typed
                // owner endpoints have an additional explicit prepaid bound.
                self.account.source_purchase(40_960, 8)?;
                self.account
                    .source_purchase(128 * 11 + 6 * 1024 + 7 * 160 + 128 * 11, 22)?;
                if self.selector.witness.input_profile()
                    == super::runtime_mutation::PermissionRuntimeInputProfile::NormalNewUse
                {
                    // Pay actual enlarged journal owner endpoints before decoding.
                    self.account.source_purchase(7 * raw.len(), 11)?;
                }
                let (journal, _) = validation::use_outcome_capture_journal(
                    raw,
                    self.selector.witness.input_profile(),
                    self.account.source_budget()?,
                )?;
                let s = if index == 3 {
                    &self.selector.resource.execution.snapshot
                } else {
                    &self.selector.current.execution.snapshot
                };
                if journal.execution_id != s.execution_id
                    || journal.revision != s.revision
                    || journal.owner_generation != s.owner_generation
                    || journal.status != s.status
                    || journal.updated_at_ms != s.updated_at_ms
                {
                    return Err(PermissionError::CorruptRecord);
                }
                if index == 3 {
                    self.state.dispatch = Some(journal);
                } else {
                    self.state.cancellation = Some(journal);
                }
            } else {
                let retained = self
                    .selector
                    .witness
                    .outcome_source_loan(self.account.source_budget()?)?;
                let (_, _, _, old) =
                    retained.outcome_window(source, index, self.account.source_budget()?)?;
                self.account.source_purchase(raw.len() + old.len(), 2)?;
                if raw != old {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            self.state.payload_captured = true;
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn capture_creator_raw(
        &mut self,
        thread: &[u8],
        revision: &[u8],
        owner: &[u8],
        snapshot: &[u8],
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if self.state.stage != Some((PermissionUseOutcomeSource::Creator, Some(0)))
                || self.state.payload_capture_attempted
                || self.state.payload_captured
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.payload_capture_attempted = true;
            let retained = self
                .selector
                .witness
                .outcome_source_loan(self.account.source_budget()?)?;
            let (fields, _, bytes, old) = retained.outcome_window(
                PermissionUseOutcomeSource::Creator,
                0,
                self.account.source_budget()?,
            )?;
            if thread.len() > 256
                || revision.len() != 8
                || owner.len() != 8
                || snapshot.len() > 65_536
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.account.source_purchase(
                2 * (thread.len() + revision.len() + owner.len() + snapshot.len()) + 512,
                4,
            )?;
            if fields.len() != 3
                || fields[0] != thread
                || fields[1] != revision
                || fields[2] != owner
                || bytes != snapshot.len()
                || old != snapshot
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.payload_captured = true;
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn end_payload(
        &mut self,
        source: PermissionUseOutcomeSource,
        index: usize,
    ) -> Result<(), PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;
        let result = checked_iife(|| {
            self.control()?;
            if self.state.stage != Some((source, Some(index)))
                || !self.state.payload_capture_attempted
                || !self.state.payload_captured
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.payload_capture_attempted = false;
            self.state.payload_captured = false;
            self.state.payloads[source.index()] += 1;
            self.state.stage = None;
            if self.state.payloads[source.index()] == self.expected_count(source) {
                self.payloads_ended[source.index()] = true;
            }
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    pub fn finish(mut self) -> Result<PermissionUseOutcomeImmediateSourceCapture, PermissionError> {
        if self.poisoned {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.poisoned = true;

        self.control()?;
        for source in [
            PermissionUseOutcomeSource::CurrentAcceptance,
            PermissionUseOutcomeSource::CurrentLegacyAcceptance,
            PermissionUseOutcomeSource::CurrentGrant,
            PermissionUseOutcomeSource::OriginalLegacyAcceptance,
            PermissionUseOutcomeSource::GlobalUseIdentity,
        ] {
            self.payloads_ended[source.index()] = self.headers_ended[source.index()];
        }
        if !self
            .headers_ended
            .iter()
            .all(outcome_checked_value(|v: &bool| *v))
            || !self
                .payloads_ended
                .iter()
                .all(outcome_checked_value(|v: &bool| *v))
            || !self.state.resource_matched
            || !self.state.ranges.headers_complete()
            || self.state.dispatch.is_none()
            || (self.selector.current != self.selector.resource
                && self.state.cancellation.is_none())
            || self.state.stage.is_some()
            || self.state.payload_capture_attempted
            || self.state.payload_captured
        {
            return Err(PermissionError::CorruptRecord);
        }
        if !self.selector.witness.same_origin(self.selector.producer) {
            return Err(PermissionError::CorruptRecord);
        }
        validate_claimed_projection(
            self.selector.witness,
            self.selector.resource,
            self.selector.invocation,
            self.account.source_budget()?,
        )?;
        validation::use_outcome_quote_context(
            self.selector.resource,
            self.selector.context,
            self.account.source_budget()?,
        )?;
        let plan = reconstruct_plan(self.selector, self.account)?;
        self.account
            .source_purchase(8 * 4096 + 8 * SOURCE_CALL_FIXED, 128)?;
        let resource = self
            .state
            .resource
            .take()
            .ok_or(PermissionError::CorruptRecord)?;
        self.protocol.stage = WriterStage::AwaitComparison;
        Ok(PermissionUseOutcomeImmediateSourceCapture {
            writer: Arc::clone(&self.selector.writer),
            round: self.selector.round,
            resource,
            plan,
            producer: Arc::clone(self.selector.producer),
            origin: Arc::clone(self.selector.operation),
            attempt: self.selector.attempt,
        })
    }
}
const _: () = {
    assert!(size_of::<ImmediateSourceState>() <= 4096);
    assert!(size_of::<Box<ImmediateSourceState>>() <= 128);
    assert!(size_of::<Result<(&str, &str, i64, u64, usize), PermissionError>>() <= 128);
    assert!(size_of::<Option<crate::JournalEntry>>() <= 256);
};
fn validate_claimed_projection(
    witness: &PermissionRuntimeUseWitness,
    current: &ApprovalResource,
    invocation: &crate::EffectInvocation,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    budget.outcome_source_reserve(LEAF_FIXED, 8)?;
    budget.outcome_source_reserve(SOURCE_CALL_FIXED, 64)?;
    let retained = witness.outcome_source_loan(budget)?;
    let u = retained.use_record;
    let d = retained.decision;
    let before = u.before_snapshot();
    let now = &current.execution.snapshot;
    validation::use_outcome_quote_claimed_projection(before, now, budget)?;
    if before.tasks.len() != 1
        || now.tasks.len() != 1
        || retained.current_journal_prefix.len() != 3
        || before.definition.batches.len() != 1
        || before.definition.batches[0].len() != 1
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let a = &before.tasks[0];
    let b = &now.tasks[0];
    if before.revision != 1
        || u.committed_revision() != 2
        || now.revision != 3
        || before.status != E::Running
        || now.status != before.status
        || before.attempts_used != 0
        || !before.attempt_history.is_empty()
        || now.attempts_used != 1
        || (now.attempt_history.len() != 1
            || now.attempt_history.first() != Some(&invocation.attempt_id))
        || now.schema_version != before.schema_version
        || now.execution_id != before.execution_id
        || now.turn_id != before.turn_id
        || now.owner_generation != before.owner_generation
        || now.definition != before.definition
        || now.batch_index != before.batch_index
        || now.values != before.values
        || now.turn_history != before.turn_history
        || now.output != before.output
        || now.failure != before.failure
        || now.updated_at_ms < u.facts().now_ms
        || before.output.is_some()
        || before.failure.is_some()
        || a.spec != b.spec
        || a.input != b.input
        || a.status != T::Ready
        || a.approved
        || a.interrupt.is_some()
        || a.attempt_id.is_some()
        || a.result.is_some()
        || a.failure.is_some()
        || b.status != T::InFlight
        || !b.approved
        || b.interrupt.as_ref() != Some(u.marker())
        || b.attempt_id.as_ref() != Some(&invocation.attempt_id)
        || b.result != a.result
        || b.failure != a.failure
        || !b.spec.requires_approval
        || invocation.execution_id != now.execution_id
        || invocation.turn_id != now.turn_id
        || invocation.owner_generation != now.owner_generation
        || invocation.task_id != b.spec.id
        || invocation.effect_id != b.spec.effect_id
        || invocation.input != b.input
        || u.record_version() != 1
        || u.permission_version() != d.record_version()
        || u.permission_id() != d.permission_id()
        || u.thread_id() != &current.thread.id
        || u.thread_generation() != &current.thread.generation
        || u.execution_id() != &now.execution_id
        || u.task_id() != &b.spec.id
        || u.application() != d.scope().application()
        || u.tool_id() != d.scope().tool_id()
        || u.tool_revision() != d.scope().tool_revision()
        || u.policy_revision() != d.scope().policy_revision()
        || u.constraint_revision() != d.scope().constraint_revision()
    {
        return Err(PermissionError::CorruptRecord);
    }
    budget.outcome_source_reserve(3 * 16 * 128, 48)?;
    let statuses = [E::Accepted, E::Running, E::Running];
    let mut last = 0;
    for (i, j) in retained.current_journal_prefix.iter().enumerate() {
        if j.execution_id != now.execution_id
            || j.revision != i as u64
            || j.owner_generation != now.owner_generation
            || j.status != statuses[i]
            || j.updated_at_ms < last
        {
            return Err(PermissionError::CorruptRecord);
        }
        last = j.updated_at_ms;
    }
    if retained.current_journal_prefix[1].updated_at_ms != before.updated_at_ms
        || retained.current_journal_prefix[2].updated_at_ms != u.facts().now_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    // Genuine retained grant was validated at production/renewal. Complete raw
    // equality is required above; completion does not renew its authorization.
    Ok(())
}

// New actual outcome and projection interfaces; every full Args/Result/Flow is
// bounded independently, including the owned Resource consumption boundary.
fn outcome_be_u64(bytes: &[u8]) -> Result<u64, PermissionError> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(
        outcome_checked_value(|_| PermissionError::CorruptRecord),
    )?))
}
fn reconstruct_plan(
    selector: &PermissionUseOutcomeImmediateSourceSelector<'_>,
    account: &mut PermissionUseOutcomeAccount,
) -> Result<CommitPlan, PermissionError> {
    super::use_cancellation_relation::require_current(
        selector.resource,
        selector.current,
        selector.controlled,
    )?;
    validation::use_outcome_quote_kernel(
        &selector.current.execution.snapshot,
        selector.invocation,
        selector.actual,
        selector.facts,
        account.source_budget()?,
    )?;
    let command = match selector.actual {
        EffectOutcome::Succeeded(result) => KernelCommand::Complete {
            task_id: selector.invocation.task_id.clone(),
            attempt_id: selector.invocation.attempt_id.clone(),
            result: result.clone(),
        },
        EffectOutcome::Failed(failure) => KernelCommand::FailAttempt {
            task_id: selector.invocation.task_id.clone(),
            attempt_id: selector.invocation.attempt_id.clone(),
            failure: failure.clone(),
        },
        EffectOutcome::Cancelled
            if selector.controlled && selector.current != selector.resource =>
        {
            KernelCommand::CancelAttempt {
                task_id: selector.invocation.task_id.clone(),
                attempt_id: selector.invocation.attempt_id.clone(),
            }
        }
        EffectOutcome::Cancelled => return Err(PermissionError::UnsupportedProfile),
    };
    b4_kernel::transition(
        &selector.current.execution.snapshot,
        command,
        selector.facts.clone(),
    )
    .map_err(outcome_checked_value(|_| PermissionError::CorruptRecord))
}
struct ImmediateRanges {
    ended: [bool; 12],
    active: Option<PermissionUseOutcomeSource>,
    original_journal_count: usize,
    current_journal_count: usize,
}
impl ImmediateRanges {
    fn new() -> Self {
        Self {
            ended: [false; 12],
            active: None,
            original_journal_count: 5,
            current_journal_count: 4,
        }
    }
    // Production selects its exact history only from the retained sealed source.
    // The standalone pure default remains the original five-row control.
    fn for_witness(witness: &PermissionRuntimeUseWitness) -> Result<Self, PermissionError> {
        let count = witness.original_journal_count();
        if !matches!(count, 5..=7) {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(Self {
            original_journal_count: count,
            ..Self::new()
        })
    }
    fn headers_complete(&self) -> bool {
        self.ended.iter().all(outcome_checked_value(|v: &bool| *v))
    }
    fn begin_headers(&mut self, source: PermissionUseOutcomeSource) -> Result<(), PermissionError> {
        if self.active.is_some() || self.ended[source.index()] {
            return Err(PermissionError::CorruptRecord);
        }
        self.active = Some(source);
        Ok(())
    }
    fn close_headers(
        &mut self,
        source: PermissionUseOutcomeSource,
        count: usize,
    ) -> Result<(), PermissionError> {
        let expected = if source == PermissionUseOutcomeSource::OriginalJournal {
            self.original_journal_count
        } else if source == PermissionUseOutcomeSource::CurrentJournal {
            self.current_journal_count
        } else {
            source.count()
        };
        if self.active != Some(source) || count != expected {
            return Err(PermissionError::CorruptRecord);
        }
        self.ended[source.index()] = true;
        self.active = None;
        Ok(())
    }
}

// Distinct provisional full callable/interface cohorts before serde operations.
pub(super) const SERIALIZE_CALL_FIXED: usize = 4 * 4096 + 19 * (4096 + 2 * 4096) + 16 * 4096;
pub(super) const SERIALIZE_CALL_NODES: usize = 4 + 19 * 3 + 16;
pub(super) fn serializer_checked_value<F>(value: F) -> F {
    const {
        assert!(size_of::<F>() <= 128);
    }
    value
}

impl PermissionUseOutcomeImmediateSourceSession<'_, '_> {
    /// On any native transaction/read refusal, typed backend latches this session
    /// before returning the original error. A failed fee also leaves it refused.
    pub fn refuse_backend_error(&mut self) -> Result<(), PermissionError> {
        self.poisoned = true;
        self.protocol.stage = WriterStage::Refused;
        self.account
            .source_purchase(SOURCE_CALL_FIXED, WRITER_CALL_NODES)
    }
}

// Concrete whole carrier bounds for this typed outcome path.
const _: () = {
    use std::{convert::Infallible, ops::ControlFlow};
    assert!(size_of::<WriterStage>() <= 128);
    assert!(size_of::<WriterProtocol>() <= 128);
    assert!(size_of::<OutcomeWriterOrigin>() <= 128);
    assert!(size_of::<Arc<OutcomeWriterOrigin>>() <= 128);
    assert!(size_of::<OutcomeVerified>() <= 4096);
    assert!(size_of::<PermissionUseOutcomeWriteSelector<'static, 'static>>() <= 128);
    assert!(size_of::<PermissionUseOutcomeWriteSession<'static, 'static, 'static>>() <= 4096);
    assert!(
        size_of::<
            Result<PermissionUseOutcomeWriteSession<'static, 'static, 'static>, PermissionError>,
        >() <= 4096
    );
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                PermissionUseOutcomeWriteSession<'static, 'static, 'static>,
            >,
        >() <= 4096
    );
    assert!(size_of::<PermissionUseOutcomeImmediateSourceSelector<'static>>() <= 128);
    assert!(size_of::<PermissionUseOutcomeImmediateSourceSession<'static, 'static>>() <= 128);
    assert!(
        size_of::<
            Result<PermissionUseOutcomeImmediateSourceSession<'static, 'static>, PermissionError>,
        >() <= 128
    );
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                PermissionUseOutcomeImmediateSourceSession<'static, 'static>,
            >,
        >() <= 128
    );
    assert!(size_of::<PermissionUseOutcomeImmediateSourceCapture>() <= 4096);
    assert!(
        size_of::<Result<PermissionUseOutcomeImmediateSourceCapture, PermissionError>>() <= 4096
    );
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                PermissionUseOutcomeImmediateSourceCapture,
            >,
        >() <= 4096
    );
    assert!(size_of::<PermissionUseOutcomeEncodedSnapshot>() <= 128);
    assert!(size_of::<PermissionUseOutcomeEncodedJournal>() <= 512);
    assert!(size_of::<PermissionUseOutcomeExecutionCasLoan<'static>>() <= 128);
    assert!(size_of::<PermissionUseOutcomeThreadCasLoan<'static>>() <= 128);
    assert!(size_of::<PermissionUseOutcomeJournalInsertLoan<'static>>() <= 128);
    assert!(
        size_of::<Result<PermissionUseOutcomeExecutionCasLoan<'static>, PermissionError>>() <= 128
    );
    assert!(
        size_of::<Result<PermissionUseOutcomeThreadCasLoan<'static>, PermissionError>>() <= 128
    );
    assert!(
        size_of::<Result<PermissionUseOutcomeJournalInsertLoan<'static>, PermissionError>>() <= 128
    );
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                PermissionUseOutcomeExecutionCasLoan<'static>,
            >,
        >() <= 128
    );
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                PermissionUseOutcomeThreadCasLoan<'static>,
            >,
        >() <= 128
    );
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                PermissionUseOutcomeJournalInsertLoan<'static>,
            >,
        >() <= 128
    );
    assert!(
        size_of::<(
            &PermissionUseOutcomeRequest<'static>,
            &PermissionUseOutcomePrepared,
            &mut PermissionUseOutcomeAccount
        )>() <= 128
    );
    assert!(
        size_of::<(
            &PermissionUseOutcomeWriteSelector<'static, 'static>,
            &mut PermissionUseOutcomeAccount
        )>() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeWriteSession<'static, 'static, 'static>,
            PermissionUseOutcomeImmediateSourceCapture
        )>() <= 4096
    );
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeWriteSession<'static, 'static, 'static>,
            usize
        )>() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeImmediateSourceSession<'static, 'static>,
            ApprovalResource
        )>() <= 4096
    );
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeImmediateSourceSession<'static, 'static>,
            PermissionUseOutcomeSource,
            super::PermissionRuntimeUseHeaderCells<'static>
        )>() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeImmediateSourceSession<'static, 'static>,
            PermissionUseOutcomeSource,
            usize,
            &[u8]
        )>() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeImmediateSourceSession<'static, 'static>,
            &[u8],
            &[u8],
            &[u8],
            &[u8]
        )>() <= 128
    );
    assert!(size_of::<(&str, &str, &[u8], &[u8], &[u8])>() <= 128);
    assert!(size_of::<(&str, &str, &str, &[u8], &[u8])>() <= 128);
    assert!(size_of::<(&str, &str, &[u8], &[u8], &str, &[u8], &str)>() <= 128);
    assert!(size_of::<Result<(&str, &str, &[u8], &[u8], &[u8]), PermissionError>>() <= 128);
    assert!(size_of::<Result<(&str, &str, &str, &[u8], &[u8]), PermissionError>>() <= 128);
    assert!(
        size_of::<Result<(&str, &str, &[u8], &[u8], &str, &[u8], &str), PermissionError>>() <= 128
    );
    assert!(
        size_of::<
            ControlFlow<Result<Infallible, PermissionError>, (&str, &str, &[u8], &[u8], &[u8])>,
        >() <= 128
    );
    assert!(
        size_of::<ControlFlow<Result<Infallible, PermissionError>, (&str, &str, &str, &[u8], &[u8])>>(
        ) <= 128
    );
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                (&str, &str, &[u8], &[u8], &str, &[u8], &str),
            >,
        >() <= 128
    );
    assert!(size_of::<(&Snapshot, &mut PermissionEvidenceBudget)>() <= 128);
    assert!(size_of::<(&JournalEntry, &mut PermissionEvidenceBudget)>() <= 128);
    assert!(size_of::<Result<String, PermissionError>>() <= 128);
    assert!(size_of::<ControlFlow<Result<Infallible, PermissionError>, String>>() <= 128);
};

#[cfg(test)]
mod writer_controls {
    use super::*;
    #[test]
    fn failed_acknowledgement_is_terminal() {
        let mut state = WriterProtocol::new();
        assert!(state
            .enter(
                WriterStage::BeforeImmediate,
                WriterStage::ImmediateRequested
            )
            .is_ok());
        assert!(state
            .acknowledge(
                WriterStage::ExecutionIssued,
                WriterStage::ExecutionUpdated,
                0
            )
            .is_err());
        assert_eq!(state.stage, WriterStage::Refused);
        assert!(state
            .enter(
                WriterStage::BeforeImmediate,
                WriterStage::ImmediateRequested
            )
            .is_err());
    }
    #[test]
    fn missing_or_extra_cas_rows_never_advance() {
        for rows in [0usize, 2usize, usize::MAX] {
            let mut state = WriterProtocol {
                stage: WriterStage::ThreadIssued,
            };
            assert!(state
                .acknowledge(WriterStage::ThreadIssued, WriterStage::ThreadUpdated, rows)
                .is_err());
            assert_eq!(state.stage, WriterStage::Refused);
        }
    }
    #[test]
    fn exact_rows_cannot_acknowledge_twice() {
        let mut state = WriterProtocol {
            stage: WriterStage::ExecutionIssued,
        };
        assert!(state
            .acknowledge(
                WriterStage::ExecutionIssued,
                WriterStage::ExecutionUpdated,
                1
            )
            .is_ok());
        assert!(state
            .acknowledge(
                WriterStage::ExecutionIssued,
                WriterStage::ExecutionUpdated,
                1
            )
            .is_err());
        assert_eq!(state.stage, WriterStage::Refused);
    }
}
