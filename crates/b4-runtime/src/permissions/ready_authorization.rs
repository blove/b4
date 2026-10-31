//! Bounded typed Ready authorization with a narrow atomic permission-use writer.
//!
//! The caller owns the captured command and setup objects. This entry borrows
//! them, validates actual current state, and reaches a separately opt-in atomic port.

use super::{
    validation, PermissionCallPhase, PermissionCatalog, PermissionError, PermissionEvidenceBudget,
    PermissionReadPurpose, PermissionStore,
};
use crate::approval::{
    ApprovalAuthority, ApprovalContext, ApprovalFence, ApprovalOperation, ApprovalResource,
};
use crate::{InvocationContext, StoredExecution};
use b4_contract::{ExecutionStatus, TaskStatus, TransitionFacts};
use b4_kernel::{KernelError, PermissionAuthorizationCommand};

/// Caller-owned captured bindings and clock facts; construction grants no authority.
pub struct PermissionReadyAuthorizationRequest<'a> {
    pub context: &'a ApprovalContext,
    pub command: &'a PermissionAuthorizationCommand,
    pub facts: TransitionFacts,
}

/// Synchronous borrowed validated intent; it is neither a match nor a permit.
pub struct PermissionReadyAuthorizationIntent<'a> {
    resource: &'a ApprovalResource,
    invocation: &'a InvocationContext,
    _context: &'a ApprovalContext,
    command: &'a PermissionAuthorizationCommand,
    catalog: &'a PermissionCatalog,
    facts: &'a TransitionFacts,
    fence: ApprovalFence,
    registration: &'a super::ToolBindingRegistration,
}

/// Validate an actual required, unapproved Ready task and enter the typed port.
///
/// Supplied command capture/scans/copies belong to the caller's setup account.
/// `Some` means the typed store committed one exact new Use, Snapshot, Thread
/// revision and journal atomically; `None` means a bounded verified miss.
/// The general durable-permissions capability remains independent and false.
/// The installed narrow writer requires ready_authorization_atomic_durable.
///
/// The host owns the implementations of `ApprovalAuthority` and custom store
/// ports, including their invoked work and owned error values. Their returned
/// errors are forwarded unchanged; this account does not bound arbitrary
/// callback allocations or recursive errors from another implementation.
pub fn try_authorize_permission_ready<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    fence: ApprovalFence,
    request: &PermissionReadyAuthorizationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<Option<StoredExecution>, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    const {
        assert!(std::mem::size_of::<&S>() <= 16);
        assert!(std::mem::size_of::<&A>() <= 16);
    }
    budget.begin_ready_authorization()?;
    let result = authorize_ready_inner(store, authority, catalog, fence, request, budget);
    budget.finish_ready_authorization(result)
}

fn authorize_ready_inner<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    fence: ApprovalFence,
    request: &PermissionReadyAuthorizationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<Option<StoredExecution>, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    if !store.approval_durable() {
        return Err(crate::approval::ApprovalError::DurabilityRequired.into());
    }
    if !store.ready_authorization_atomic_durable() {
        return Err(PermissionError::UnsupportedProfile);
    }
    let command = request.command;
    validation::check_permission_read_request(
        request.context,
        command.execution_id(),
        Some(&command.task_spec().id),
        budget,
    )?;
    let resource = store.permission_ready_authorization_resource(command.execution_id(), budget)?;
    validation::quote_ready_authorization_context_owner(
        &resource,
        request.context,
        catalog,
        budget,
    )?;
    let invocation = validation::prepare_permission_read_context(
        &resource,
        fence,
        request.context,
        catalog,
        PermissionReadPurpose::Dispatch,
        Some(&command.task_spec().id),
        budget,
    )?;
    match authority.authorize(request.context, ApprovalOperation::Dispatch, &resource) {
        Ok(()) => {}
        Err(error) => return Err(PermissionError::Approval(error)),
    }
    let current = &resource.execution.snapshot;
    validation::quote_ready_authorization_search(current, command, budget)?;
    let mut selected = None;
    for task in &current.tasks {
        if task.spec.id == command.task_spec().id {
            selected = Some(task);
            break;
        }
    }
    let task = match selected {
        Some(task) => task,
        None => return Err(PermissionError::Kernel(KernelError::UnknownTask)),
    };
    validation::quote_ready_authorization_bindings(current, task, command, budget)?;
    if &current.execution_id != command.execution_id()
        || current.revision != command.expected_revision()
        || current.owner_generation != command.owner_generation()
        || &current.turn_id != command.turn_id()
        || &current.definition != command.definition()
        || current.batch_index != command.batch_index()
        || &task.spec != command.task_spec()
        || &task.input != command.input()
    {
        return Err(PermissionError::Kernel(KernelError::InvalidTransition(
            "permission_authorization_stale",
        )));
    }
    if request.facts.now_ms < current.updated_at_ms {
        return Err(PermissionError::Kernel(KernelError::ClockMovedBackwards));
    }
    let in_batch = match current.definition.batches.get(current.batch_index) {
        Some(batch) => batch.contains(&task.spec),
        None => false,
    };
    if current.status != ExecutionStatus::Running
        || current.failure.is_some()
        || !in_batch
        || task.input != current.values
        || !task.spec.requires_approval
        || task.status != TaskStatus::Ready
        || task.approved
        || task.attempt_id.is_some()
        || task.result.is_some()
        || task.failure.is_some()
        || task.interrupt.is_some()
    {
        return Err(PermissionError::Kernel(KernelError::InvalidTransition(
            "permission_task_not_unapproved_ready",
        )));
    }
    for candidate in &current.tasks {
        if let Some(pending) = &candidate.interrupt {
            if pending.id.as_str() == command.use_id().as_str() {
                return Err(PermissionError::Kernel(KernelError::IdCollision));
            }
        }
    }
    catalog.validate_call_budgeted(
        &invocation,
        &current.definition,
        task,
        PermissionCallPhase::Unclaimed,
        budget,
    )?;
    let registration =
        catalog.ready_registration(&current.definition, &task.spec.effect_id, budget)?;
    let intent = PermissionReadyAuthorizationIntent {
        resource: &resource,
        invocation: &invocation,
        _context: request.context,
        command,
        catalog,
        facts: &request.facts,
        fence,
        registration,
    };
    store.try_commit_permission_ready_authorization(&intent, budget)
}

// Whole actual named carriers. These are prospective until the final compiler gate.
const _: () = assert!(std::mem::size_of::<PermissionReadyAuthorizationRequest<'static>>() <= 128);
const _: () = assert!(std::mem::size_of::<PermissionReadyAuthorizationIntent<'static>>() <= 128);
const _: () = assert!(std::mem::size_of::<ApprovalResource>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<ApprovalResource, PermissionError>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ApprovalResource>,
    >() <= 4096
);
const _: () = assert!(std::mem::size_of::<InvocationContext>() <= 512);
const _: () = assert!(std::mem::size_of::<Result<InvocationContext, PermissionError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, InvocationContext>,
    >() <= 512
);
const _: () =
    assert!(std::mem::size_of::<Result<Option<StoredExecution>, PermissionError>>() <= 512);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            Option<StoredExecution>,
        >,
    >() <= 512
);
const _: () = assert!(std::mem::size_of::<Result<(), crate::approval::ApprovalError>>() <= 4096);

/// Which exact resource/source observation is being performed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PermissionReadyAuthorizationReadPhase {
    Deferred,
    Immediate,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PermissionReadyAuthorizationSource {
    Decision,
    Acceptance,
    Grant,
    Creator,
    Journal,
    LegacyAcceptance,
    CurrentUse,
    GlobalUseId,
}
impl PermissionReadyAuthorizationSource {
    fn index(self) -> usize {
        self as usize
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PermissionReadyAuthorizationWriteSql {
    InsertUse,
    SnapshotCas,
    ThreadCas,
    InsertJournal,
}

struct ReadySourceHeader {
    fields: Vec<Vec<u8>>,
    scalar: i64,
    bytes: usize,
}
struct ReadySourceWitness {
    header: ReadySourceHeader,
    payload: Vec<u8>,
}

/// A store can feed observations, but cannot construct a matched use or work.
pub struct PermissionReadyAuthorizationWriterSession<'intent, 'budget> {
    intent: &'intent PermissionReadyAuthorizationIntent<'intent>,
    budget: &'budget mut PermissionEvidenceBudget,
    phase: PermissionReadyAuthorizationReadPhase,
    rows: [usize; 8],
    ended: [bool; 8],
    witnesses: [Vec<ReadySourceWitness>; 5],
    decision: Option<super::PermissionDecision>,
    acceptance: Option<super::PermissionAcceptance>,
    grant: Option<crate::approval::GrantRecord>,
    creator: Option<StoredExecution>,
    journals: Vec<crate::JournalEntry>,
    creator_lengths: Option<(usize, usize)>,
    current_snapshot_witness: Vec<u8>,
    current_metadata_witness: Vec<u8>,
    current_origin_witness: Option<Vec<u8>>,
    current_checked: bool,
    matching: bool,
    deferred_finished: bool,
}

impl<'intent> PermissionReadyAuthorizationIntent<'intent> {
    pub fn begin_writer<'borrow, 'budget>(
        &'borrow self,
        budget: &'budget mut PermissionEvidenceBudget,
    ) -> Result<PermissionReadyAuthorizationWriterSession<'borrow, 'budget>, PermissionError>
    where
        'intent: 'borrow,
    {
        budget.begin_ready_authorization_writer()?;
        let (current_snapshot_witness, current_metadata_witness, current_origin_witness) =
            validation::ready_use_current_witnesses(self.resource, budget)?;
        Ok(PermissionReadyAuthorizationWriterSession {
            intent: self,
            budget,
            phase: PermissionReadyAuthorizationReadPhase::Deferred,
            rows: [0; 8],
            ended: [false; 8],
            witnesses: std::array::from_fn(checked_writer_value(|_| Vec::new())),
            decision: None,
            acceptance: None,
            grant: None,
            creator: None,
            journals: Vec::new(),
            creator_lengths: None,
            current_snapshot_witness,
            current_metadata_witness,
            current_origin_witness,
            current_checked: false,
            matching: false,
            deferred_finished: false,
        })
    }
}

impl<'intent, 'budget> PermissionReadyAuthorizationWriterSession<'intent, 'budget> {
    pub fn thread_id(&self) -> &'intent b4_contract::ThreadId {
        &self.intent.resource.thread.id
    }
    pub fn current_execution_id(&self) -> &'intent b4_contract::ExecutionId {
        self.intent.command.execution_id()
    }
    pub fn use_id(&self) -> &'intent str {
        self.intent.command.use_id().as_str()
    }
    pub fn decision_permission_id(&self) -> Result<&str, PermissionError> {
        let header = &self
            .pending(PermissionReadyAuthorizationSource::Decision)?
            .header;
        std::str::from_utf8(&header.fields[0])
            .map_err(checked_writer_value(|_| PermissionError::CorruptRecord))
    }
    pub fn begin_read(
        &mut self,
        phase: PermissionReadyAuthorizationReadPhase,
    ) -> Result<(), PermissionError> {
        self.budget.reserve(0, 0)?;
        if phase != self.phase
            || phase != PermissionReadyAuthorizationReadPhase::Deferred
            || self.current_checked
        {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(())
    }
    pub fn creator_execution_id(&self) -> Result<&b4_contract::ExecutionId, PermissionError> {
        Ok(self
            .decision
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .creator_execution_id())
    }
    pub fn creator_turn_id(&self) -> Result<&b4_contract::TurnId, PermissionError> {
        Ok(self
            .decision
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .creator_turn_id())
    }
    pub fn creator_interrupt_id(&self) -> Result<&b4_contract::InterruptId, PermissionError> {
        Ok(self
            .decision
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?
            .creator_interrupt_id())
    }
    pub fn resource_read(
        &mut self,
    ) -> Result<PermissionReadyAuthorizationCurrentRead<'_, 'intent>, PermissionError> {
        let evidence = self.budget.resource_read()?;
        Ok(PermissionReadyAuthorizationCurrentRead {
            evidence,
            resource: self.intent.resource,
            snapshot: &self.current_snapshot_witness,
            metadata: &self.current_metadata_witness,
            origin: self.current_origin_witness.as_deref(),
            metadata_checked: false,
            execution_checked: false,
            thread_checked: false,
            linkage_checked: false,
        })
    }
    pub fn check_current(
        &mut self,
        current: PermissionReadyAuthorizationCurrentObservation<'intent>,
    ) -> Result<(), PermissionError> {
        self.budget.reserve(256, 4)?;
        if !std::ptr::eq(current.resource, self.intent.resource)
            || current.resource.execution.snapshot.revision != self.intent.fence.execution.revision
            || current.resource.execution.snapshot.owner_generation
                != self.intent.fence.execution.owner_generation
            || current.resource.thread.resource_revision != self.intent.fence.resource_revision
            || current.resource.thread.active_execution_id.as_ref()
                != Some(self.intent.command.execution_id())
        {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        self.current_checked = true;
        Ok(())
    }
    pub fn before_source_sql(
        &mut self,
        source: PermissionReadyAuthorizationSource,
    ) -> Result<(), PermissionError> {
        self.budget.reserve(10_240, 1)?;
        if !self.current_checked || self.ended[source.index()] {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(())
    }
    pub fn before_row(
        &mut self,
        source: PermissionReadyAuthorizationSource,
    ) -> Result<(), PermissionError> {
        self.budget.reserve(1_024, 4)?;
        if self.ended[source.index()] {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(())
    }
    pub fn reject_present(
        &mut self,
        source: PermissionReadyAuthorizationSource,
    ) -> Result<(), PermissionError> {
        self.budget.reserve(256, 2)?;
        if !matches!(
            source,
            PermissionReadyAuthorizationSource::LegacyAcceptance
                | PermissionReadyAuthorizationSource::CurrentUse
                | PermissionReadyAuthorizationSource::GlobalUseId
        ) {
            return Err(PermissionError::CorruptRecord);
        }
        Err(PermissionError::UnsupportedProfile)
    }
    // Both source phases select the exact history length from the decoded
    // creator; Immediate also compares every retained header and payload.
    pub fn creator_journal_count(&self) -> Result<usize, PermissionError> {
        let creator = self
            .creator
            .as_ref()
            .ok_or(PermissionError::CorruptRecord)?;
        match (creator.snapshot.status, creator.snapshot.revision) {
            (ExecutionStatus::Cancelled, 4) => Ok(5),
            (ExecutionStatus::Completed | ExecutionStatus::Failed, 5) => Ok(6),
            (ExecutionStatus::Cancelled | ExecutionStatus::Failed, 6) => Ok(7),
            _ => Err(PermissionError::UnsupportedProfile),
        }
    }
    fn admit_header(
        &mut self,
        source: PermissionReadyAuthorizationSource,
        fields: &[&[u8]],
        scalar: i64,
        bytes: usize,
    ) -> Result<(), PermissionError> {
        let i = source.index();
        let limit = if source == PermissionReadyAuthorizationSource::Journal {
            self.creator_journal_count()?
        } else {
            1
        };
        self.budget.reserve(1_024, 8)?;
        if i >= 5
            || self.ended[i]
            || self.rows[i] == limit
            || bytes == 0
            || bytes
                > if source == PermissionReadyAuthorizationSource::Journal {
                    1_024
                } else {
                    65_536
                }
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let mut total = 0usize;
        for field in fields {
            total = checked_add(total, field.len())?;
        }
        self.budget
            .reserve(checked_add(checked_mul(total, 2)?, 512)?, fields.len())?;
        if self.phase == PermissionReadyAuthorizationReadPhase::Immediate {
            let previous = self.witnesses[i]
                .get(self.rows[i])
                .ok_or(PermissionError::CorruptRecord)?;
            if previous.header.scalar != scalar
                || previous.header.bytes != bytes
                || previous.header.fields.len() != fields.len()
            {
                return Err(PermissionError::CorruptRecord);
            }
            for (old, fresh) in previous.header.fields.iter().zip(fields) {
                if old.as_slice() != *fresh {
                    return Err(PermissionError::CorruptRecord);
                }
            }
        } else {
            self.budget.reserve(
                checked_add(checked_mul(total, 13)?, checked_mul(fields.len(), 256)?)?,
                checked_mul(fields.len(), 2)?,
            )?;
            self.budget.reserve(512, 2)?;
            let mut owned = Vec::new();
            owned
                .try_reserve_exact(fields.len())
                .map_err(checked_writer_value(|_| PermissionError::LimitExceeded))?;
            for field in fields {
                owned.push(owned_bytes(field)?);
            }
            self.witnesses[i]
                .try_reserve_exact(1)
                .map_err(checked_writer_value(|_| PermissionError::LimitExceeded))?;
            self.witnesses[i].push(ReadySourceWitness {
                header: ReadySourceHeader {
                    fields: owned,
                    scalar,
                    bytes,
                },
                payload: Vec::new(),
            });
        }
        self.rows[i] += 1;
        Ok(())
    }
    pub fn admit_new_header(
        &mut self,
        cells: super::PermissionNewHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        use super::PermissionNewGroup;
        match cells.group {
            PermissionNewGroup::Decisions => self.admit_header(
                PermissionReadyAuthorizationSource::Decision,
                &[
                    cells.primary,
                    cells.thread_id,
                    cells.generation,
                    cells.digest.ok_or(PermissionError::CorruptRecord)?,
                ],
                cells.record_version,
                cells.record_bytes,
            ),
            PermissionNewGroup::Acceptances => self.admit_header(
                PermissionReadyAuthorizationSource::Acceptance,
                &[
                    cells.execution_id.ok_or(PermissionError::CorruptRecord)?,
                    cells.primary,
                    cells.thread_id,
                    cells.generation,
                    cells
                        .previous_revision
                        .ok_or(PermissionError::CorruptRecord)?,
                    cells
                        .committed_revision
                        .ok_or(PermissionError::CorruptRecord)?,
                ],
                cells.record_version,
                cells.record_bytes,
            ),
            PermissionNewGroup::Uses => Err(PermissionError::UnsupportedProfile),
        }
    }
    pub fn admit_grant_header(
        &mut self,
        cells: super::PermissionGrantHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.admit_header(
            PermissionReadyAuthorizationSource::Grant,
            &[
                cells.execution_id,
                cells.interrupt_id,
                cells.task_id,
                cells.thread_id,
                cells.generation,
                cells.parking_revision,
            ],
            cells.consumed,
            cells.record_bytes,
        )
    }
    pub fn admit_journal_header(
        &mut self,
        cells: super::PermissionProjectionJournalHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.budget.reserve(16, 2)?;
        let seen = self.rows[PermissionReadyAuthorizationSource::Journal.index()];
        if cells.sequence <= 0
            || (seen > 0
                && cells.sequence
                    <= self.witnesses[PermissionReadyAuthorizationSource::Journal.index()]
                        [seen - 1]
                        .header
                        .scalar)
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.admit_header(
            PermissionReadyAuthorizationSource::Journal,
            &[
                cells.kind,
                cells.thread_id,
                cells.execution_id,
                cells.revision,
                cells.owner,
                cells.status,
                cells.time,
            ],
            cells.sequence,
            cells.payload_bytes,
        )
    }
    fn pending(
        &self,
        source: PermissionReadyAuthorizationSource,
    ) -> Result<&ReadySourceWitness, PermissionError> {
        let i = source.index();
        let row = self.rows[i]
            .checked_sub(1)
            .ok_or(PermissionError::CorruptRecord)?;
        self.witnesses[i]
            .get(row)
            .ok_or(PermissionError::CorruptRecord)
    }
    pub fn payload_bytes(
        &self,
        source: PermissionReadyAuthorizationSource,
    ) -> Result<usize, PermissionError> {
        Ok(self.pending(source)?.header.bytes)
    }
    pub fn journal_sequence(&self) -> Result<i64, PermissionError> {
        Ok(self
            .pending(PermissionReadyAuthorizationSource::Journal)?
            .header
            .scalar)
    }
    pub fn journal_revision(&self) -> Result<u64, PermissionError> {
        be_u64(
            &self
                .pending(PermissionReadyAuthorizationSource::Journal)?
                .header
                .fields[3],
        )
    }
    pub fn capture_payload(
        &mut self,
        source: PermissionReadyAuthorizationSource,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        let i = source.index();
        let row = self.rows[i]
            .checked_sub(1)
            .ok_or(PermissionError::CorruptRecord)?;
        if raw.len() != self.pending(source)?.header.bytes {
            return Err(PermissionError::CorruptRecord);
        }
        self.budget
            .reserve(checked_add(checked_mul(raw.len(), 2)?, 256)?, 2)?;
        if self.phase == PermissionReadyAuthorizationReadPhase::Immediate {
            if self.pending(source)?.payload.as_slice() != raw {
                return Err(PermissionError::CorruptRecord);
            }
            return Ok(());
        }
        self.budget
            .reserve(checked_add(checked_mul(raw.len(), 13)?, 256)?, 2)?;
        self.witnesses[i][row].payload = owned_bytes(raw)?;
        match source {
            PermissionReadyAuthorizationSource::Decision => {
                let value = super::PermissionDecision::decode_record(raw, self.budget)?;
                let h = &self.witnesses[i][row].header;
                if h.scalar != i64::from(value.record_version())
                    || h.fields[0] != value.permission_id().as_str().as_bytes()
                    || h.fields[1] != value.scope().thread_id().as_str().as_bytes()
                    || h.fields[2] != value.scope().thread_generation().as_str().as_bytes()
                    || h.fields[3] != value.scope_digest().as_bytes()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.decision = Some(value);
            }
            PermissionReadyAuthorizationSource::Acceptance => {
                let value = super::PermissionAcceptance::decode_record(raw, self.budget)?;
                let h = &self.witnesses[i][row].header;
                if h.scalar != i64::from(value.record_version())
                    || h.fields[0] != value.before_snapshot().execution_id.as_str().as_bytes()
                    || h.fields[1] != value.turn_id().as_str().as_bytes()
                    || h.fields[2] != value.thread_id().as_str().as_bytes()
                    || h.fields[3] != value.thread_generation().as_str().as_bytes()
                    || be_u64(&h.fields[4])? != value.before_snapshot().revision
                    || be_u64(&h.fields[5])? != value.committed_revision()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.acceptance = Some(value);
            }
            PermissionReadyAuthorizationSource::Grant => {
                let (value, _) = validation::runtime_compat::capture_grant(raw, self.budget)?;
                let h = &self.witnesses[i][row].header;
                if h.scalar != 1
                    || !value.consumed
                    || h.fields[0] != value.execution_id.as_str().as_bytes()
                    || h.fields[1] != value.interrupt_id.as_str().as_bytes()
                    || h.fields[2] != value.task_id.as_str().as_bytes()
                    || h.fields[3] != value.thread_id.as_str().as_bytes()
                    || h.fields[4] != value.thread_generation.as_str().as_bytes()
                    || be_u64(&h.fields[5])? != value.task_parking_revision
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.grant = Some(value);
            }
            PermissionReadyAuthorizationSource::Journal => {
                let (value, _) = validation::runtime_compat::capture_journal(raw, self.budget)?;
                let h = &self.witnesses[i][row].header;
                let status = validation::ready_use_status(value.status);
                if h.scalar <= 0
                    || h.fields[1] != self.intent.resource.thread.id.as_str().as_bytes()
                    || h.fields[2] != value.execution_id.as_str().as_bytes()
                    || be_u64(&h.fields[3])? != value.revision
                    || be_u64(&h.fields[4])? != value.owner_generation
                    || h.fields[5] != status.as_bytes()
                    || be_u64(&h.fields[6])? != value.updated_at_ms
                    || h.fields[0].as_slice()
                        != if row == 0 {
                            b"admit".as_slice()
                        } else {
                            b"commit".as_slice()
                        }
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.budget.reserve(512, 2)?;
                self.journals
                    .try_reserve_exact(1)
                    .map_err(checked_writer_value(|_| PermissionError::LimitExceeded))?;
                self.journals.push(value);
            }
            _ => return Err(PermissionError::CorruptRecord),
        }
        Ok(())
    }
    pub fn end_source(
        &mut self,
        source: PermissionReadyAuthorizationSource,
    ) -> Result<(), PermissionError> {
        let i = source.index();
        self.budget.reserve(256, 2)?;
        if self.ended[i] {
            return Err(PermissionError::CorruptRecord);
        }
        if self.phase == PermissionReadyAuthorizationReadPhase::Immediate
            && i < 5
            && self.rows[i] != self.witnesses[i].len()
        {
            return Err(PermissionError::CorruptRecord);
        }
        if source == PermissionReadyAuthorizationSource::Journal
            && self.rows[i] != self.creator_journal_count()?
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if matches!(
            source,
            PermissionReadyAuthorizationSource::Acceptance
                | PermissionReadyAuthorizationSource::Grant
                | PermissionReadyAuthorizationSource::Creator
        ) && self.rows[i] != 1
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.ended[i] = true;
        Ok(())
    }
    pub fn admit_creator_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        self.budget
            .reserve(checked_add(checked_add(thread, snapshot)?, 512)?, 8)?;
        if thread > 256 || snapshot > 1_048_576 || snapshot == 0 {
            return Err(PermissionError::LimitExceeded);
        }
        self.creator_lengths = Some((thread, snapshot));
        Ok(())
    }
    pub fn creator_lengths(&self) -> Result<(usize, usize), PermissionError> {
        self.creator_lengths.ok_or(PermissionError::CorruptRecord)
    }
    pub fn capture_creator_raw(
        &mut self,
        thread_id: &[u8],
        revision: &[u8],
        owner_generation: &[u8],
        snapshot_raw: &[u8],
    ) -> Result<(), PermissionError> {
        let (thread, snapshot) = self.creator_lengths()?;
        if thread_id.len() != thread || snapshot_raw.len() != snapshot {
            return Err(PermissionError::CorruptRecord);
        }
        self.admit_header(
            PermissionReadyAuthorizationSource::Creator,
            &[thread_id, revision, owner_generation],
            0,
            snapshot_raw.len(),
        )?;
        let i = PermissionReadyAuthorizationSource::Creator.index();
        self.budget
            .reserve(checked_add(checked_mul(snapshot_raw.len(), 2)?, 256)?, 2)?;
        if self.phase == PermissionReadyAuthorizationReadPhase::Immediate {
            if self.witnesses[i][0].payload.as_slice() != snapshot_raw {
                return Err(PermissionError::CorruptRecord);
            }
        } else {
            self.budget
                .reserve(checked_add(checked_mul(snapshot_raw.len(), 13)?, 256)?, 2)?;
            self.witnesses[i][0].payload = owned_bytes(snapshot_raw)?;
            // Disjoint fields permit the original verified D identity to remain
            // borrowed while its separate account performs the real decode.
            let decision = self
                .decision
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            self.creator = Some(validation::ready_use_decode_creator(
                super::PermissionExecutionCells {
                    execution_id: decision.creator_execution_id(),
                    thread_id,
                    revision,
                    owner_generation,
                    snapshot: snapshot_raw,
                },
                self.budget,
            )?);
        }
        Ok(())
    }
    pub fn finish_deferred(&mut self) -> Result<bool, PermissionError> {
        self.budget.reserve(0, 0)?;
        if self.phase != PermissionReadyAuthorizationReadPhase::Deferred
            || !self.current_checked
            || !self.ended[0]
            || !self.ended[PermissionReadyAuthorizationSource::CurrentUse.index()]
            || !self.ended[PermissionReadyAuthorizationSource::GlobalUseId.index()]
            || self.deferred_finished
        {
            return Err(PermissionError::CorruptRecord);
        }
        if self.decision.is_none() {
            self.matching = false;
        } else {
            if self
                .ended
                .iter()
                .any(checked_writer_value(|done: &bool| !done))
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.matching = validation::ready_use_validate_source(
                self.decision
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?,
                self.acceptance
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?,
                self.grant.as_ref().ok_or(PermissionError::CorruptRecord)?,
                self.creator
                    .as_ref()
                    .ok_or(PermissionError::CorruptRecord)?,
                &self.journals,
                self.intent.resource,
                self.intent.invocation,
                self.intent.registration,
                self.intent.catalog.application_identity(),
                self.budget,
            )?;
        }
        self.deferred_finished = true;
        Ok(self.matching)
    }
    pub fn begin_immediate(&mut self) -> Result<(), PermissionError> {
        self.budget.reserve(20_480, 2)?;
        if !self.deferred_finished || self.phase != PermissionReadyAuthorizationReadPhase::Deferred
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.phase = PermissionReadyAuthorizationReadPhase::Immediate;
        self.rows = [0; 8];
        self.ended = [false; 8];
        self.current_checked = false;
        Ok(())
    }
    fn check_immediate(&mut self) -> Result<(), PermissionError> {
        self.budget.reserve(0, 0)?;
        if self.phase != PermissionReadyAuthorizationReadPhase::Immediate
            || !self.current_checked
            || !self.ended[0]
            || !self.ended[PermissionReadyAuthorizationSource::CurrentUse.index()]
            || !self.ended[PermissionReadyAuthorizationSource::GlobalUseId.index()]
        {
            return Err(PermissionError::CorruptRecord);
        }
        if self.decision.is_some()
            && self
                .ended
                .iter()
                .any(checked_writer_value(|done: &bool| !done))
        {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(())
    }
    pub fn finish_miss(&mut self) -> Result<Option<StoredExecution>, PermissionError> {
        self.check_immediate()?;
        if self.matching {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(None)
    }
    pub fn prepare_work<'work>(
        &'work mut self,
    ) -> Result<PermissionReadyAuthorizationWork<'work, 'intent, 'budget>, PermissionError> {
        self.check_immediate()?;
        if !self.matching {
            return Err(PermissionError::CorruptRecord);
        }
        let before = &self.intent.resource.execution.snapshot;
        validation::ready_use_quote_plan(before, self.budget)?;
        let plan =
            b4_kernel::authorize_ready(before, self.intent.command, self.intent.facts.clone())?;
        if !plan.dispatchable.is_empty()
            || plan.expected_revision != before.revision
            || plan.snapshot.revision
                != before
                    .revision
                    .checked_add(1)
                    .ok_or(PermissionError::CorruptRecord)?
            || plan.snapshot.execution_id != before.execution_id
            || plan.snapshot.owner_generation != before.owner_generation
        {
            return Err(PermissionError::CorruptRecord);
        }
        let permission_use = validation::ready_use_factory(
            self.intent.resource,
            self.intent.command,
            self.intent.facts,
            self.decision
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?,
            self.intent.registration,
            self.intent.catalog.application_identity(),
            &plan,
            self.budget,
        )?;
        let use_bytes = String::from_utf8(permission_use.encode_record(self.budget)?)
            .map_err(checked_writer_value(|_| PermissionError::CorruptRecord))?;
        let snapshot_bytes = String::from_utf8(validation::ready_use_encode_snapshot(
            &plan.snapshot,
            self.budget,
        )?)
        .map_err(checked_writer_value(|_| PermissionError::CorruptRecord))?;
        let journal = validation::ready_use_journal(&plan.snapshot, self.budget)?;
        let journal_bytes =
            String::from_utf8(validation::ready_use_encode_journal(&journal, self.budget)?)
                .map_err(checked_writer_value(|_| PermissionError::CorruptRecord))?;
        let next_resource_revision = self
            .intent
            .resource
            .thread
            .resource_revision
            .checked_add(1)
            .ok_or(PermissionError::CorruptRecord)?;
        self.budget.reserve(
            checked_add(
                checked_mul(13, self.intent.resource.thread.id.as_str().len())?,
                256,
            )?,
            2,
        )?;
        let output = StoredExecution {
            thread_id: self.intent.resource.thread.id.clone(),
            snapshot: plan.snapshot,
        };
        Ok(PermissionReadyAuthorizationWork {
            session: self,
            output,
            permission_use,
            use_bytes,
            snapshot_bytes,
            journal_bytes,
            journal,
            next_resource_revision,
            stage: 0,
            commit_admitted: false,
        })
    }
}

/// Only an admitted matched session can produce the four-write work object.
pub struct PermissionReadyAuthorizationWork<'work, 'intent, 'budget> {
    session: &'work mut PermissionReadyAuthorizationWriterSession<'intent, 'budget>,
    output: StoredExecution,
    permission_use: super::PermissionUse,
    use_bytes: String,
    snapshot_bytes: String,
    journal_bytes: String,
    journal: crate::JournalEntry,
    next_resource_revision: u64,
    stage: usize,
    commit_admitted: bool,
}
pub struct PermissionReadyAuthorizationSqlBindings<'a> {
    execution_id: &'a str,
    thread_id: &'a str,
    generation: &'a str,
    task_id: &'a str,
    use_id: &'a str,
    permission_id: &'a str,
    turn_id: &'a str,
    use_record: &'a str,
    snapshot: &'a str,
    journal: &'a str,
    before_revision: u64,
    before_resource_revision: u64,
    owner_generation: u64,
    next_revision: u64,
    next_resource_revision: u64,
    updated_at_ms: u64,
}
impl PermissionReadyAuthorizationSqlBindings<'_> {
    pub fn execution_id(&self) -> &str {
        self.execution_id
    }
    pub fn thread_id(&self) -> &str {
        self.thread_id
    }
    pub fn generation(&self) -> &str {
        self.generation
    }
    pub fn task_id(&self) -> &str {
        self.task_id
    }
    pub fn use_id(&self) -> &str {
        self.use_id
    }
    pub fn permission_id(&self) -> &str {
        self.permission_id
    }
    pub fn turn_id(&self) -> &str {
        self.turn_id
    }
    pub fn use_record(&self) -> &str {
        self.use_record
    }
    pub fn snapshot(&self) -> &str {
        self.snapshot
    }
    pub fn journal(&self) -> &str {
        self.journal
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
}
impl<'work, 'intent, 'budget> PermissionReadyAuthorizationWork<'work, 'intent, 'budget> {
    pub fn before_write_sql(
        &mut self,
        stage: PermissionReadyAuthorizationWriteSql,
    ) -> Result<PermissionReadyAuthorizationSqlBindings<'_>, PermissionError> {
        self.session.budget.reserve(10_240, 1)?;
        if stage as usize != self.stage || self.commit_admitted {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionReadyAuthorizationSqlBindings {
            execution_id: self.execution_id(),
            thread_id: self.thread_id(),
            generation: self.generation(),
            task_id: self.task_id(),
            use_id: self.use_id(),
            permission_id: self.permission_id(),
            turn_id: self.turn_id(),
            use_record: self.use_record(),
            snapshot: self.snapshot(),
            journal: self.journal(),
            before_revision: self.before_revision(),
            before_resource_revision: self.before_resource_revision(),
            owner_generation: self.owner_generation(),
            next_revision: self.next_revision(),
            next_resource_revision: self.next_resource_revision(),
            updated_at_ms: self.updated_at_ms(),
        })
    }
    pub fn check_affected_rows(
        &mut self,
        stage: PermissionReadyAuthorizationWriteSql,
        rows: usize,
    ) -> Result<(), PermissionError> {
        self.affected(stage, rows)
    }
    pub fn affected(
        &mut self,
        stage: PermissionReadyAuthorizationWriteSql,
        rows: usize,
    ) -> Result<(), PermissionError> {
        self.session.budget.reserve(256, 2)?;
        if stage as usize != self.stage || rows != 1 {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        self.stage += 1;
        Ok(())
    }
    pub fn before_commit(&mut self) -> Result<(), PermissionError> {
        self.session.budget.reserve(10_240, 1)?;
        if self.stage != 4 || self.commit_admitted {
            return Err(PermissionError::CorruptRecord);
        }
        self.commit_admitted = true;
        Ok(())
    }
    pub fn finish_committed(self) -> Result<StoredExecution, PermissionError> {
        if !self.commit_admitted || self.stage != 4 {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(self.output)
    }
    pub fn execution_id(&self) -> &str {
        self.output.snapshot.execution_id.as_str()
    }
    pub fn thread_id(&self) -> &str {
        self.output.thread_id.as_str()
    }
    pub fn generation(&self) -> &str {
        self.session.intent.resource.thread.generation.as_str()
    }
    pub fn task_id(&self) -> &str {
        self.permission_use.task_id().as_str()
    }
    pub fn use_id(&self) -> &str {
        self.permission_use.use_id().as_str()
    }
    pub fn permission_id(&self) -> &str {
        self.permission_use.permission_id().as_str()
    }
    pub fn turn_id(&self) -> &str {
        self.output.snapshot.turn_id.as_str()
    }
    pub fn before_revision(&self) -> u64 {
        self.session.intent.resource.execution.snapshot.revision
    }
    pub fn before_resource_revision(&self) -> u64 {
        self.session.intent.resource.thread.resource_revision
    }
    pub fn owner_generation(&self) -> u64 {
        self.output.snapshot.owner_generation
    }
    pub fn next_revision(&self) -> u64 {
        self.output.snapshot.revision
    }
    pub fn next_resource_revision(&self) -> u64 {
        self.next_resource_revision
    }
    pub fn updated_at_ms(&self) -> u64 {
        self.journal.updated_at_ms
    }
    pub fn use_record(&self) -> &str {
        &self.use_bytes
    }
    pub fn snapshot(&self) -> &str {
        &self.snapshot_bytes
    }
    pub fn journal(&self) -> &str {
        &self.journal_bytes
    }
}
fn checked_writer_value<F>(value: F) -> F {
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}
fn checked_add(a: usize, b: usize) -> Result<usize, PermissionError> {
    a.checked_add(b).ok_or(PermissionError::LimitExceeded)
}
fn checked_mul(a: usize, b: usize) -> Result<usize, PermissionError> {
    a.checked_mul(b).ok_or(PermissionError::LimitExceeded)
}
fn be_u64(raw: &[u8]) -> Result<u64, PermissionError> {
    let bytes: [u8; 8] = raw
        .try_into()
        .map_err(checked_writer_value(|_| PermissionError::CorruptRecord))?;
    Ok(u64::from_be_bytes(bytes))
}
fn owned_bytes(raw: &[u8]) -> Result<Vec<u8>, PermissionError> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(raw.len())
        .map_err(checked_writer_value(|_| PermissionError::LimitExceeded))?;
    owned.extend_from_slice(raw);
    Ok(owned)
}

/// Fresh current observations retain only original borrowed typed owners.
/// Exact compact spelling is part of this deliberately bounded writer profile.
pub struct PermissionReadyAuthorizationCurrentRead<'read, 'intent> {
    evidence: super::PermissionResourceRead<'read>,
    resource: &'intent ApprovalResource,
    snapshot: &'read [u8],
    metadata: &'read [u8],
    origin: Option<&'read [u8]>,
    metadata_checked: bool,
    execution_checked: bool,
    thread_checked: bool,
    linkage_checked: bool,
}
pub struct PermissionReadyAuthorizationCurrentObservation<'intent> {
    resource: &'intent ApprovalResource,
}
impl<'read, 'intent> PermissionReadyAuthorizationCurrentRead<'read, 'intent> {
    pub fn before_sql(
        &mut self,
        stage: super::PermissionResourceSql,
        key: Option<super::PermissionResourceKey<'_>>,
    ) -> Result<(), PermissionError> {
        self.evidence.before_sql(stage, key)
    }
    pub fn check_metadata(
        &mut self,
        version: i64,
        checksum: &[u8],
        owner: &[u8],
        expected_owner: u64,
        expected_schema: &[u8],
    ) -> Result<(), PermissionError> {
        self.evidence
            .check_metadata(version, checksum, owner, expected_owner, expected_schema)?;
        if expected_owner != self.resource.execution.snapshot.owner_generation {
            return Err(crate::StoreError::OwnerConflict.into());
        }
        self.metadata_checked = true;
        Ok(())
    }
    pub fn admit_execution_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        self.evidence.admit_execution_lengths(thread, snapshot)?;
        if thread != self.resource.execution.thread_id.as_str().len()
            || snapshot != self.snapshot.len()
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        Ok(())
    }
    pub fn decode_execution(
        &mut self,
        cells: super::PermissionExecutionCells<'_>,
    ) -> Result<&'intent StoredExecution, PermissionError> {
        self.evidence.ready_use_quote_raw_compare(
            checked_add(cells.thread_id.len(), cells.snapshot.len())?,
            8,
        )?;
        let expected = &self.resource.execution;
        if !self.metadata_checked
            || cells.execution_id != &expected.snapshot.execution_id
            || cells.thread_id != expected.thread_id.as_str().as_bytes()
            || be_u64(cells.revision)? != expected.snapshot.revision
            || be_u64(cells.owner_generation)? != expected.snapshot.owner_generation
        {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        if cells.snapshot != self.snapshot {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.execution_checked = true;
        Ok(expected)
    }
    pub fn admit_thread_lengths(
        &mut self,
        id: usize,
        active: usize,
        generation: usize,
        metadata: usize,
        origin: usize,
    ) -> Result<(), PermissionError> {
        self.evidence
            .admit_thread_lengths(id, active, generation, metadata, origin)?;
        let expected = &self.resource.thread;
        let active_length = match &expected.active_execution_id {
            Some(id) => id.as_str().len(),
            None => 0,
        };
        let origin_length = match self.origin {
            Some(raw) => raw.len(),
            None => 0,
        };
        if id != expected.id.as_str().len()
            || active != active_length
            || generation != expected.generation.as_str().len()
            || metadata != self.metadata.len()
            || origin != origin_length
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        Ok(())
    }
    // Preserve direct borrowed control without introducing another callback value.
    #[allow(clippy::manual_map)]
    pub fn decode_thread(
        &mut self,
        cells: super::PermissionThreadCells<'_>,
    ) -> Result<&'intent crate::ThreadRecord, PermissionError> {
        let mut bytes = checked_add(cells.metadata.len(), cells.generation.len())?;
        if let Some(raw) = cells.origin {
            bytes = checked_add(bytes, raw.len())?;
        }
        if let Some(raw) = cells.active_execution_id {
            bytes = checked_add(bytes, raw.len())?;
        }
        self.evidence.ready_use_quote_raw_compare(bytes, 12)?;
        let expected = &self.resource.thread;
        let active = match &expected.active_execution_id {
            Some(id) => Some(id.as_str().as_bytes()),
            None => None,
        };
        if !self.execution_checked
            || cells.id != &expected.id
            || cells.active_execution_id != active
            || be_u64(cells.created_at_ms)? != expected.created_at_ms
            || be_u64(cells.resource_revision)? != expected.resource_revision
            || cells.generation != expected.generation.as_str().as_bytes()
        {
            return Err(crate::StoreError::RevisionConflict.into());
        }
        if cells.metadata != self.metadata || cells.origin != self.origin {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.thread_checked = true;
        Ok(expected)
    }
    pub fn check_linkage(
        &mut self,
        id: &b4_contract::ExecutionId,
        thread: &crate::ThreadRecord,
        execution: &StoredExecution,
    ) -> Result<(), PermissionError> {
        self.evidence.check_linkage(id, thread, execution)?;
        if !self.metadata_checked
            || !self.execution_checked
            || !self.thread_checked
            || !std::ptr::eq(thread, &self.resource.thread)
            || !std::ptr::eq(execution, &self.resource.execution)
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.linkage_checked = true;
        Ok(())
    }
    pub fn finish(
        self,
    ) -> Result<PermissionReadyAuthorizationCurrentObservation<'intent>, PermissionError> {
        if !self.metadata_checked
            || !self.execution_checked
            || !self.thread_checked
            || !self.linkage_checked
        {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionReadyAuthorizationCurrentObservation {
            resource: self.resource,
        })
    }
}

// Exact new named writer carriers; fulfillment remains compiler prospective.
const _: () = assert!(
    std::mem::size_of::<PermissionReadyAuthorizationWriterSession<'static, 'static>>() <= 4096
);
const _: () = assert!(
    std::mem::size_of::<
        Result<PermissionReadyAuthorizationWriterSession<'static, 'static>, PermissionError>,
    >() <= 4096
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionReadyAuthorizationWriterSession<'static, 'static>,
        >,
    >() <= 4096
);
const _: () = assert!(
    std::mem::size_of::<PermissionReadyAuthorizationWork<'static, 'static, 'static>>() <= 4096
);
const _: () = assert!(
    std::mem::size_of::<
        Result<PermissionReadyAuthorizationWork<'static, 'static, 'static>, PermissionError>,
    >() <= 4096
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionReadyAuthorizationWork<'static, 'static, 'static>,
        >,
    >() <= 4096
);
const _: () =
    assert!(std::mem::size_of::<PermissionReadyAuthorizationSqlBindings<'static>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<Result<PermissionReadyAuthorizationSqlBindings<'static>, PermissionError>>(
    ) <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionReadyAuthorizationSqlBindings<'static>,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<ReadySourceHeader>() <= 64);
const _: () = assert!(std::mem::size_of::<ReadySourceWitness>() <= 128);
const _: () = assert!(std::mem::size_of::<Result<ReadySourceWitness, PermissionError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            ReadySourceWitness,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<[Vec<ReadySourceWitness>; 5]>() <= 160);
const _: () =
    assert!(std::mem::size_of::<Result<[Vec<ReadySourceWitness>; 5], PermissionError>>() <= 160);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            [Vec<ReadySourceWitness>; 5],
        >,
    >() <= 160
);
const _: () = assert!(std::mem::size_of::<[usize; 8]>() <= 64);
const _: () = assert!(std::mem::size_of::<[bool; 8]>() <= 8);
const _: () = assert!(std::mem::size_of::<Vec<ReadySourceWitness>>() <= 32);
const _: () = assert!(std::mem::size_of::<Vec<Vec<u8>>>() <= 32);
const _: () = assert!(std::mem::size_of::<Vec<crate::JournalEntry>>() <= 32);
const _: () = assert!(
    std::mem::size_of::<PermissionReadyAuthorizationCurrentRead<'static, 'static>>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        Result<PermissionReadyAuthorizationCurrentRead<'static, 'static>, PermissionError>,
    >() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionReadyAuthorizationCurrentRead<'static, 'static>,
        >,
    >() <= 128
);
const _: () =
    assert!(std::mem::size_of::<PermissionReadyAuthorizationCurrentObservation<'static>>() <= 16);
const _: () = assert!(std::mem::size_of::<PermissionReadyAuthorizationReadPhase>() <= 8);
const _: () = assert!(std::mem::size_of::<PermissionReadyAuthorizationSource>() <= 8);
const _: () = assert!(std::mem::size_of::<PermissionReadyAuthorizationWriteSql>() <= 8);

// Exact borrowed getter/flow and whole header-argument carriers.
const _: () = {
    assert!(std::mem::size_of::<Result<&'static ReadySourceWitness, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                &'static ReadySourceWitness,
            >,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<Result<&'static b4_contract::ExecutionId, PermissionError>>() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                &'static b4_contract::ExecutionId,
            >,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<&'static b4_contract::TurnId, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                &'static b4_contract::TurnId,
            >,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<Result<&'static b4_contract::InterruptId, PermissionError>>() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                &'static b4_contract::InterruptId,
            >,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<&'static str, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &'static str>,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<(usize, usize), PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (usize, usize),
            >,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<u64, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, u64>,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<usize, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, usize>,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<bool, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, bool>,
        >() <= 128
    );
    assert!(std::mem::size_of::<[&'static [u8]; 3]>() <= 512);
    assert!(std::mem::size_of::<[&'static [u8]; 4]>() <= 512);
    assert!(std::mem::size_of::<[&'static [u8]; 6]>() <= 512);
    assert!(std::mem::size_of::<[&'static [u8]; 7]>() <= 512);
    assert!(std::mem::size_of::<&'static [&'static [u8]]>() <= 16);
    assert!(std::mem::size_of::<Result<&'static str, std::str::Utf8Error>>() <= 128);
};

// The actual UTF-8 mapper argument has its own whole nominal premise.
const _: () = assert!(std::mem::size_of::<std::str::Utf8Error>() <= 128);

// Whole individual borrowed byte-slice view; no array/proxy inference.
const _: () = assert!(std::mem::size_of::<&'static [u8]>() <= 16);
