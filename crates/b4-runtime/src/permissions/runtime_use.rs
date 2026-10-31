//! Bounded retained PermissionUse provenance for the first typed Dispatch claim.
//! Backend cells are observations. Only a completed, privately sealed source set
//! can be accepted by its originating Runtime session.
use super::runtime_mutation::{PermissionRuntimeInputProfile, PermissionRuntimeOrigin};
use super::{
    validation, PermissionAcceptance, PermissionDecision, PermissionError,
    PermissionEvidenceBudget, PermissionUse,
};
use crate::{
    approval::{ApprovalResource, GrantRecord},
    JournalEntry, StoredExecution, ThreadRecord,
};
use b4_contract::{ExecutionId, ExecutionStatus, Snapshot, TaskStatus, ThreadId};
use std::{mem::size_of, sync::Arc};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PermissionRuntimeEvidenceKind {
    NewHuman,
    LegacyOnce,
    NewUse,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PermissionRuntimeUseSource {
    CurrentUse,
    ThreadDecision,
    CurrentLegacyAcceptance,
    CurrentGrant,
    GlobalUseIdentity,
    OriginalAcceptance,
    OriginalGrant,
    OriginalCreator,
    OriginalLegacyAcceptance,
    OriginalJournal,
    CurrentJournal,
}
impl PermissionRuntimeUseSource {
    fn index(self) -> usize {
        self as usize
    }
    fn count(self) -> usize {
        match self {
            Self::CurrentLegacyAcceptance | Self::CurrentGrant | Self::OriginalLegacyAcceptance => {
                0
            }
            Self::OriginalJournal => 5,
            Self::CurrentJournal => 3,
            _ => 1,
        }
    }
}
#[derive(Clone, Copy)]
pub enum PermissionRuntimeUseReadSql {
    Headers(PermissionRuntimeUseSource),
    Payload(PermissionRuntimeUseSource, usize),
    CreatorLengths,
    CreatorPayload,
}
pub struct PermissionRuntimeUseHeaderCells<'a> {
    pub fields: &'a [&'a [u8]],
    pub scalar: i64,
    pub record_bytes: usize,
}
struct UseHeader {
    fields: Vec<Vec<u8>>,
    scalar: i64,
    bytes: usize,
}
struct UseRawWitness {
    header: UseHeader,
    payload: Vec<u8>,
}
pub(super) struct PermissionRuntimeUseWitness {
    raw: [Vec<UseRawWitness>; 11],
    use_record: PermissionUse,
    decision: PermissionDecision,
    acceptance: PermissionAcceptance,
    grant: GrantRecord,
    creator: StoredExecution,
    original_journals: Vec<JournalEntry>,
    // Keep validated typed journals alive through the witness's paid retirement.
    #[allow(dead_code)]
    current_journals: Vec<JournalEntry>,
    origin: Arc<PermissionRuntimeOrigin>,
    // Selected at genuine Dispatch inception and retained across all account loans.
    input_profile: PermissionRuntimeInputProfile,
    // Immutable geometry provenance from exact normal Dispatch capture only.
    normal_dispatch_origin: bool,
}
pub struct PermissionRuntimeUseEvidence {
    inner: UseEvidence,
}
// Inline owned sources preserve the paid bounded allocation and retirement schedule.
#[allow(clippy::large_enum_variant)]
enum UseEvidence {
    Captured(PermissionRuntimeUseWitness),
    Matched(Arc<PermissionRuntimeOrigin>),
}
/// A short exclusive account loan; no public constructor or authority fields.
pub struct PermissionRuntimeUseSession<'a> {
    budget: &'a mut PermissionEvidenceBudget,
    execution: &'a StoredExecution,
    thread: &'a ThreadRecord,
    origin: &'a Arc<PermissionRuntimeOrigin>,
    expected: Option<&'a PermissionRuntimeUseWitness>,
    raw: [Vec<UseRawWitness>; 11],
    rows: [usize; 11],
    ended: [bool; 11],
    payloads: [usize; 11],
    stage: Option<PermissionRuntimeUseReadSql>,
    row_attempts: usize,
    entered_cancellation: bool,
    creator_lengths: Option<(usize, usize)>,
    use_record: Option<PermissionUse>,
    decision: Option<PermissionDecision>,
    acceptance: Option<PermissionAcceptance>,
    grant: Option<GrantRecord>,
    creator: Option<StoredExecution>,
    original_journals: Vec<JournalEntry>,
    current_journals: Vec<JournalEntry>,
}
fn checked<F>(value: F) -> F {
    const {
        assert!(size_of::<F>() <= 128);
    }
    value
}
fn add(a: usize, b: usize) -> Result<usize, PermissionError> {
    a.checked_add(b).ok_or(PermissionError::LimitExceeded)
}
fn mul(a: usize, b: usize) -> Result<usize, PermissionError> {
    a.checked_mul(b).ok_or(PermissionError::LimitExceeded)
}
fn utf8(raw: &[u8]) -> Result<&str, PermissionError> {
    std::str::from_utf8(raw).map_err(checked(|_: std::str::Utf8Error| {
        PermissionError::CorruptRecord
    }))
}
fn opaque(raw: &[u8]) -> Result<&str, PermissionError> {
    let s = utf8(raw)?;
    if s.is_empty() || s.len() > 256 || s.chars().any(checked(char::is_control)) {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(s)
}
fn generation(raw: &[u8]) -> Result<&str, PermissionError> {
    let s = utf8(raw)?;
    if s.len() != 64
        || !s.bytes().all(checked(|b: u8| {
            b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
        }))
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(s)
}
fn be(raw: &[u8]) -> Result<u64, PermissionError> {
    Ok(u64::from_be_bytes(raw.try_into().map_err(checked(
        |_: std::array::TryFromSliceError| PermissionError::CorruptRecord,
    ))?))
}
fn owned(raw: &[u8]) -> Result<Vec<u8>, PermissionError> {
    let mut value = Vec::new();
    value.try_reserve_exact(raw.len()).map_err(checked(
        |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
    ))?;
    value.extend_from_slice(raw);
    Ok(value)
}
impl<'a> PermissionRuntimeUseSession<'a> {
    pub(super) fn new(
        budget: &'a mut PermissionEvidenceBudget,
        execution: &'a StoredExecution,
        thread: &'a ThreadRecord,
        origin: &'a Arc<PermissionRuntimeOrigin>,
        expected: Option<&'a PermissionRuntimeUseWitness>,
    ) -> Result<Self, PermissionError> {
        // A distinct source-entry invoice precedes every new source owner. Its
        // finite first-refusal prefix is separately required by the Source model.
        budget.runtime_begin_use_source()?;
        if execution.snapshot.owner_generation == 0
            || thread.active_execution_id.as_ref() != Some(&execution.snapshot.execution_id)
        {
            return Err(PermissionError::CorruptRecord);
        }
        if let Some(old) = expected {
            if !Arc::ptr_eq(origin, &old.origin)
                || old.input_profile != budget.runtime_input_profile()
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
        Ok(Self {
            budget,
            execution,
            thread,
            origin,
            expected,
            raw: std::array::from_fn(checked(|_| Vec::new())),
            rows: [0; 11],
            ended: [false; 11],
            payloads: [0; 11],
            stage: None,
            row_attempts: 0,
            entered_cancellation: false,
            creator_lengths: None,
            use_record: None,
            decision: None,
            acceptance: None,
            grant: None,
            creator: None,
            original_journals: Vec::new(),
            current_journals: Vec::new(),
        })
    }
    pub(super) fn for_entered_cancellation(
        budget: &'a mut PermissionEvidenceBudget,
        execution: &'a StoredExecution,
        thread: &'a ThreadRecord,
        origin: &'a Arc<PermissionRuntimeOrigin>,
        expected: Option<&'a PermissionRuntimeUseWitness>,
    ) -> Result<Self, PermissionError> {
        let mut session = Self::new(budget, execution, thread, origin, expected)?;
        session.entered_cancellation = true;
        Ok(session)
    }
    /// Selects the bounded fourth current journal only in the management read.
    pub fn entered_cancellation_read(&self) -> bool {
        self.entered_cancellation
    }
    fn control(&mut self) -> Result<(), PermissionError> {
        // New complete M512 helper metadata root; data and original loans are exterior.
        self.budget.runtime_reserve(128 + 2 * 512, 1)
    }
    fn use_value(&self) -> Result<&PermissionUse, PermissionError> {
        match self.expected {
            Some(v) => Ok(&v.use_record),
            None => self
                .use_record
                .as_ref()
                .ok_or(PermissionError::CorruptRecord),
        }
    }
    fn decision_value(&self) -> Result<&PermissionDecision, PermissionError> {
        match self.expected {
            Some(v) => Ok(&v.decision),
            None => self.decision.as_ref().ok_or(PermissionError::CorruptRecord),
        }
    }
    fn header(
        &self,
        source: PermissionRuntimeUseSource,
        index: usize,
    ) -> Result<&UseRawWitness, PermissionError> {
        let raw = match self.expected {
            Some(v) => &v.raw,
            None => &self.raw,
        };
        raw[source.index()]
            .get(index)
            .ok_or(PermissionError::CorruptRecord)
    }
    pub fn current_execution_id(&self) -> &ExecutionId {
        &self.execution.snapshot.execution_id
    }
    pub fn thread_id(&self) -> &ThreadId {
        &self.thread.id
    }
    pub fn creator_execution_id(&self) -> Result<&ExecutionId, PermissionError> {
        Ok(self.decision_value()?.creator_execution_id())
    }
    pub fn selected_use_key(&self) -> Result<(&str, &str, usize), PermissionError> {
        let h = &self
            .header(PermissionRuntimeUseSource::CurrentUse, 0)?
            .header;
        Ok((utf8(&h.fields[1])?, utf8(&h.fields[2])?, h.bytes))
    }
    pub fn decision_key(&self) -> Result<(&str, usize), PermissionError> {
        let h = &self
            .header(PermissionRuntimeUseSource::ThreadDecision, 0)?
            .header;
        Ok((utf8(&h.fields[0])?, h.bytes))
    }
    pub fn acceptance_key(&self) -> Result<(&str, usize), PermissionError> {
        let h = &self
            .header(PermissionRuntimeUseSource::OriginalAcceptance, 0)?
            .header;
        Ok((utf8(&h.fields[1])?, h.bytes))
    }
    pub fn grant_key(&self) -> Result<(&str, usize), PermissionError> {
        let h = &self
            .header(PermissionRuntimeUseSource::OriginalGrant, 0)?
            .header;
        Ok((utf8(&h.fields[1])?, h.bytes))
    }
    pub fn journal_key(
        &self,
        source: PermissionRuntimeUseSource,
        index: usize,
    ) -> Result<(i64, u64, usize), PermissionError> {
        if !matches!(
            source,
            PermissionRuntimeUseSource::OriginalJournal
                | PermissionRuntimeUseSource::CurrentJournal
        ) {
            return Err(PermissionError::CorruptRecord);
        }
        let h = &self.header(source, index)?.header;
        Ok((h.scalar, be(&h.fields[3])?, h.bytes))
    }
    pub fn before_sql(&mut self, sql: PermissionRuntimeUseReadSql) -> Result<(), PermissionError> {
        self.control()?;
        // Actual action capacity is purchased before its Statement construction.
        self.budget.runtime_reserve(10_240, 1)?;
        if self.stage.is_some() {
            return Err(PermissionError::CorruptRecord);
        }
        match sql {
            PermissionRuntimeUseReadSql::Headers(s)
                if s != PermissionRuntimeUseSource::OriginalCreator && !self.ended[s.index()] => {}
            PermissionRuntimeUseReadSql::Payload(s, i)
                if self.ended[s.index()]
                    && i == self.payloads[s.index()]
                    && i < self.rows[s.index()] => {}
            PermissionRuntimeUseReadSql::CreatorLengths if self.creator_lengths.is_none() => {}
            PermissionRuntimeUseReadSql::CreatorPayload
                if self.ended[PermissionRuntimeUseSource::OriginalCreator.index()]
                    && self.payloads[PermissionRuntimeUseSource::OriginalCreator.index()] == 0 => {}
            _ => return Err(PermissionError::CorruptRecord),
        }
        self.stage = Some(sql);
        self.row_attempts = 0;
        Ok(())
    }
    pub fn expected_count(&self, source: PermissionRuntimeUseSource) -> usize {
        if source == PermissionRuntimeUseSource::OriginalJournal {
            self.expected
                .map(|w| w.original_journals.len())
                .or_else(|| {
                    self.creator
                        .as_ref()
                        .map(|c| match (c.snapshot.status, c.snapshot.revision) {
                            (ExecutionStatus::Cancelled | ExecutionStatus::Failed, 6) => 7,
                            _ => (c.snapshot.revision.saturating_add(1) as usize).min(6),
                        })
                })
                .unwrap_or(5)
                .min(7)
        } else if source == PermissionRuntimeUseSource::CurrentJournal && self.entered_cancellation
        {
            4
        } else {
            source.count()
        }
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        self.control()?;
        self.budget.runtime_reserve(1024, 4)?;
        let limit = match self.stage {
            Some(PermissionRuntimeUseReadSql::Headers(s)) => self.expected_count(s) + 1,
            Some(_) => 2,
            None => return Err(PermissionError::CorruptRecord),
        };
        if self.row_attempts == limit {
            return Err(PermissionError::CorruptRecord);
        }
        self.row_attempts += 1;
        Ok(())
    }
    pub fn reject_present(
        &mut self,
        source: PermissionRuntimeUseSource,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if self.expected_count(source) != 0 {
            return Err(PermissionError::CorruptRecord);
        }
        Err(PermissionError::UnsupportedProfile)
    }
    fn validate_fields(
        &self,
        s: PermissionRuntimeUseSource,
        c: &PermissionRuntimeUseHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        use PermissionRuntimeUseSource as S;
        let f = c.fields;
        let expected = match s {
            S::CurrentUse => 8,
            S::ThreadDecision => 4,
            S::GlobalUseIdentity => 3,
            S::CurrentLegacyAcceptance | S::OriginalLegacyAcceptance => 2,
            S::OriginalAcceptance | S::OriginalGrant | S::CurrentGrant => 6,
            S::OriginalJournal | S::CurrentJournal => 7,
            S::OriginalCreator => 3,
        };
        if f.len() != expected {
            return Err(PermissionError::CorruptRecord);
        }
        match s {
            S::CurrentUse => {
                if c.scalar != 1
                    || opaque(f[0])? != self.current_execution_id().as_str()
                    || opaque(f[3])? != self.thread.id.as_str()
                    || generation(f[4])? != self.thread.generation.as_str()
                    || be(f[7])?
                        != if self.entered_cancellation {
                            2
                        } else {
                            self.execution.snapshot.revision
                        }
                {
                    return Err(PermissionError::CorruptRecord);
                }
                for i in [1, 2, 5, 6] {
                    opaque(f[i])?;
                }
            }
            S::ThreadDecision => {
                if c.scalar != 1
                    || opaque(f[1])? != self.thread.id.as_str()
                    || generation(f[2])? != self.thread.generation.as_str()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                opaque(f[0])?;
                generation(f[3])?;
                if opaque(f[0])? != self.use_value()?.permission_id().as_str() {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            S::GlobalUseIdentity => {
                let u = self.use_value()?;
                if c.scalar != 0
                    || c.record_bytes != 0
                    || opaque(f[0])? != u.execution_id().as_str()
                    || opaque(f[1])? != u.task_id().as_str()
                    || opaque(f[2])? != u.use_id().as_str()
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            S::CurrentLegacyAcceptance | S::OriginalLegacyAcceptance => {
                let id = if s == S::OriginalLegacyAcceptance {
                    self.creator_execution_id()?
                } else {
                    self.current_execution_id()
                };
                if c.scalar != 0 || c.record_bytes != 0 || opaque(f[0])? != id.as_str() {
                    return Err(PermissionError::CorruptRecord);
                }
                opaque(f[1])?;
            }
            S::OriginalAcceptance => {
                let d = self.decision_value()?;
                if c.scalar != 1
                    || opaque(f[0])? != d.creator_execution_id().as_str()
                    || opaque(f[1])? != d.creator_turn_id().as_str()
                    || opaque(f[2])? != self.thread.id.as_str()
                    || generation(f[3])? != self.thread.generation.as_str()
                    || be(f[4])?.checked_add(1) != Some(be(f[5])?)
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            S::OriginalGrant | S::CurrentGrant => {
                let id = if s == S::OriginalGrant {
                    self.creator_execution_id()?
                } else {
                    self.current_execution_id()
                };
                if !(0..=1).contains(&c.scalar)
                    || opaque(f[0])? != id.as_str()
                    || opaque(f[3])? != self.thread.id.as_str()
                    || generation(f[4])? != self.thread.generation.as_str()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                opaque(f[1])?;
                opaque(f[2])?;
                be(f[5])?;
                if s == S::OriginalGrant
                    && (c.scalar != 1
                        || opaque(f[1])? != self.decision_value()?.creator_interrupt_id().as_str()
                        || opaque(f[2])? != self.decision_value()?.creator_task_id().as_str())
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            S::OriginalJournal | S::CurrentJournal => {
                let id = if s == S::OriginalJournal {
                    self.creator_execution_id()?
                } else {
                    self.current_execution_id()
                };
                if c.scalar <= 0
                    || opaque(f[1])? != self.thread.id.as_str()
                    || opaque(f[2])? != id.as_str()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                if !matches!(utf8(f[0])?, "admit" | "commit") {
                    return Err(PermissionError::CorruptRecord);
                }
                be(f[3])?;
                be(f[4])?;
                be(f[6])?;
                opaque(f[5])?;
            }
            S::OriginalCreator => {
                opaque(f[0])?;
                be(f[1])?;
                be(f[2])?;
            }
        }
        if self.budget.runtime_input_profile() == PermissionRuntimeInputProfile::NormalNewUse {
            let limit = match s {
                S::OriginalAcceptance if self.budget.runtime_is_normal_new_use_dispatch() => 16_384,
                S::OriginalCreator if self.budget.runtime_is_normal_new_use_dispatch() => 8_192,
                S::OriginalAcceptance => 8_192,
                S::ThreadDecision | S::OriginalGrant | S::OriginalCreator => 4_096,
                S::OriginalJournal | S::CurrentJournal => 1_024,
                S::CurrentUse => 16_384,
                _ => 65_536,
            };
            if c.record_bytes > limit {
                return Err(PermissionError::UnsupportedProfile);
            }
        }
        if self.expected_count(s) != 0
            && s != S::GlobalUseIdentity
            && (c.record_bytes == 0
                || c.record_bytes
                    > if matches!(s, S::OriginalJournal | S::CurrentJournal) {
                        1024
                    } else {
                        65536
                    })
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        Ok(())
    }
    fn retain_header(
        &mut self,
        s: PermissionRuntimeUseSource,
        c: PermissionRuntimeUseHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        let mut total = 0;
        for field in c.fields {
            total = add(total, field.len())?;
        }
        self.budget
            .runtime_reserve(add(mul(total, 2)?, 512)?, c.fields.len())?;
        let index = self.rows[s.index()];
        if self.expected.is_some() {
            let old = &self.header(s, index)?.header;
            if old.scalar != c.scalar
                || old.bytes != c.record_bytes
                || old.fields.len() != c.fields.len()
            {
                return Err(PermissionError::CorruptRecord);
            }
            for (old, new) in old.fields.iter().zip(c.fields) {
                if old.as_slice() != *new {
                    return Err(PermissionError::CorruptRecord);
                }
            }
        } else {
            self.budget.runtime_reserve(
                add(mul(total, 13)?, mul(c.fields.len(), 256)?)?,
                mul(c.fields.len(), 2)?,
            )?;
            self.budget.runtime_reserve(512, 2)?;
            let mut fields = Vec::new();
            fields.try_reserve_exact(c.fields.len()).map_err(checked(
                |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
            ))?;
            for field in c.fields {
                fields.push(owned(field)?);
            }
            self.raw[s.index()].try_reserve_exact(1).map_err(checked(
                |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
            ))?;
            self.raw[s.index()].push(UseRawWitness {
                header: UseHeader {
                    fields,
                    scalar: c.scalar,
                    bytes: c.record_bytes,
                },
                payload: Vec::new(),
            });
        }
        self.rows[s.index()] += 1;
        Ok(())
    }
    // Preserve the separate journal-order probe and reviewed control branch boundaries.
    #[allow(clippy::collapsible_if)]
    pub fn admit_header(
        &mut self,
        s: PermissionRuntimeUseSource,
        c: PermissionRuntimeUseHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if !matches!(self.stage,Some(PermissionRuntimeUseReadSql::Headers(v)) if v==s)
            || self.ended[s.index()]
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.validate_fields(s, &c)?;
        if self.expected_count(s) == 0 {
            return Err(PermissionError::UnsupportedProfile);
        }
        if self.rows[s.index()] == self.expected_count(s) {
            return Err(PermissionError::UnsupportedProfile);
        }
        if matches!(
            s,
            PermissionRuntimeUseSource::OriginalJournal
                | PermissionRuntimeUseSource::CurrentJournal
        ) && self.rows[s.index()] > 0
        {
            if self.header(s, self.rows[s.index()] - 1)?.header.scalar >= c.scalar {
                return Err(PermissionError::CorruptRecord);
            }
        }
        self.retain_header(s, c)
    }
    pub fn end_headers(&mut self, s: PermissionRuntimeUseSource) -> Result<(), PermissionError> {
        self.control()?;
        let stage_ok = if s == PermissionRuntimeUseSource::OriginalCreator {
            matches!(
                self.stage,
                Some(PermissionRuntimeUseReadSql::CreatorLengths)
            )
        } else {
            matches!(self.stage,Some(PermissionRuntimeUseReadSql::Headers(v)) if v==s)
        };
        if !stage_ok || self.rows[s.index()] != self.expected_count(s) || self.ended[s.index()] {
            return Err(PermissionError::CorruptRecord);
        }
        self.ended[s.index()] = true;
        self.stage = None;
        Ok(())
    }
    pub fn header_count(&self, s: PermissionRuntimeUseSource) -> Result<usize, PermissionError> {
        if !self.ended[s.index()] {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(self.rows[s.index()])
    }
    pub fn capture_payload(
        &mut self,
        s: PermissionRuntimeUseSource,
        index: usize,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        self.control()?;
        if !matches!(self.stage,Some(PermissionRuntimeUseReadSql::Payload(v,i)) if v==s && i==index)
            || raw.len() != self.header(s, index)?.header.bytes
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.budget
            .runtime_reserve(add(mul(raw.len(), 2)?, 256)?, 2)?;
        if self.expected.is_some() {
            if self.header(s, index)?.payload.as_slice() != raw {
                return Err(PermissionError::CorruptRecord);
            }
            return Ok(());
        }
        self.budget
            .runtime_reserve(add(mul(raw.len(), 13)?, 256)?, 2)?;
        self.raw[s.index()][index].payload = owned(raw)?;
        // The new codec metadata region is separate from each observed deep-owner ticket.
        self.budget.runtime_reserve(128 + 2 * 4096, 1)?;
        use PermissionRuntimeUseSource as S;
        match s {
            S::CurrentUse => {
                let v = PermissionUse::decode_record(raw, self.budget)?;
                let h = &self.raw[s.index()][index].header;
                if h.fields[0] != v.execution_id().as_str().as_bytes()
                    || h.fields[1] != v.task_id().as_str().as_bytes()
                    || h.fields[2] != v.use_id().as_str().as_bytes()
                    || h.fields[3] != v.thread_id().as_str().as_bytes()
                    || h.fields[4] != v.thread_generation().as_str().as_bytes()
                    || h.fields[5] != v.permission_id().as_str().as_bytes()
                    || h.fields[6] != v.before_snapshot().turn_id.as_str().as_bytes()
                    || be(&h.fields[7])? != v.committed_revision()
                    || h.scalar != i64::from(v.record_version())
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.use_record = Some(v);
            }
            S::ThreadDecision => {
                let v = PermissionDecision::decode_record(raw, self.budget)?;
                let h = &self.raw[s.index()][index].header;
                if h.scalar != i64::from(v.record_version())
                    || h.fields[0] != v.permission_id().as_str().as_bytes()
                    || h.fields[1] != v.scope().thread_id().as_str().as_bytes()
                    || h.fields[2] != v.scope().thread_generation().as_str().as_bytes()
                    || h.fields[3] != v.scope_digest().as_bytes()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.decision = Some(v);
            }
            S::OriginalAcceptance => {
                let v = PermissionAcceptance::decode_record(raw, self.budget)?;
                let h = &self.raw[s.index()][index].header;
                if h.scalar != i64::from(v.record_version())
                    || h.fields[0] != v.before_snapshot().execution_id.as_str().as_bytes()
                    || h.fields[1] != v.turn_id().as_str().as_bytes()
                    || h.fields[2] != v.thread_id().as_str().as_bytes()
                    || h.fields[3] != v.thread_generation().as_str().as_bytes()
                    || be(&h.fields[4])? != v.before_snapshot().revision
                    || be(&h.fields[5])? != v.committed_revision()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.acceptance = Some(v);
            }
            S::OriginalGrant => {
                let (v, _) = validation::runtime_compat::capture_grant(raw, self.budget)?;
                let h = &self.raw[s.index()][index].header;
                if !v.consumed
                    || h.scalar != 1
                    || h.fields[0] != v.execution_id.as_str().as_bytes()
                    || h.fields[1] != v.interrupt_id.as_str().as_bytes()
                    || h.fields[2] != v.task_id.as_str().as_bytes()
                    || h.fields[3] != v.thread_id.as_str().as_bytes()
                    || h.fields[4] != v.thread_generation.as_str().as_bytes()
                    || be(&h.fields[5])? != v.task_parking_revision
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.grant = Some(v);
            }
            S::OriginalJournal | S::CurrentJournal => {
                // Explicit bounded library lifecycle: partial checked-ID/custom
                // diagnostic owners, serde scratch and accepted Journal data.
                self.budget.runtime_reserve(40_960, 8)?;
                let (v, _) = validation::runtime_compat::capture_journal(raw, self.budget)?;
                let h = &self.raw[s.index()][index].header;
                if h.fields[2] != v.execution_id.as_str().as_bytes()
                    || be(&h.fields[3])? != v.revision
                    || be(&h.fields[4])? != v.owner_generation
                    || h.fields[5] != validation::ready_use_status(v.status).as_bytes()
                    || be(&h.fields[6])? != v.updated_at_ms
                    || h.fields[0].as_slice()
                        != if index == 0 {
                            b"admit".as_slice()
                        } else {
                            b"commit".as_slice()
                        }
                {
                    return Err(PermissionError::CorruptRecord);
                }
                self.budget.runtime_reserve(512, 2)?;
                let target = if s == S::OriginalJournal {
                    &mut self.original_journals
                } else {
                    &mut self.current_journals
                };
                target.try_reserve_exact(1).map_err(checked(
                    |_: std::collections::TryReserveError| PermissionError::LimitExceeded,
                ))?;
                target.push(v);
            }
            _ => return Err(PermissionError::CorruptRecord),
        }
        Ok(())
    }
    pub fn end_payload(
        &mut self,
        s: PermissionRuntimeUseSource,
        index: usize,
    ) -> Result<(), PermissionError> {
        self.control()?;
        let ok = if s == PermissionRuntimeUseSource::OriginalCreator {
            matches!(
                self.stage,
                Some(PermissionRuntimeUseReadSql::CreatorPayload)
            )
        } else {
            matches!(self.stage,Some(PermissionRuntimeUseReadSql::Payload(v,i)) if v==s && i==index)
        };
        if !ok || self.payloads[s.index()] != index {
            return Err(PermissionError::CorruptRecord);
        }
        if self.expected.is_none() && self.raw[s.index()][index].payload.is_empty() {
            return Err(PermissionError::CorruptRecord);
        }
        self.payloads[s.index()] += 1;
        self.stage = None;
        Ok(())
    }
    pub fn admit_creator_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        self.control()?;
        self.budget
            .runtime_reserve(add(add(thread, snapshot)?, 512)?, 8)?;
        if !matches!(
            self.stage,
            Some(PermissionRuntimeUseReadSql::CreatorLengths)
        ) || self.creator_lengths.is_some()
            || thread == 0
            || thread > 256
            || snapshot == 0
            || snapshot > 65536
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        if self.budget.runtime_input_profile() == PermissionRuntimeInputProfile::NormalNewUse
            && snapshot
                > if self.budget.runtime_is_normal_new_use_dispatch() {
                    8_192
                } else {
                    4_096
                }
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.creator_lengths = Some((thread, snapshot));
        self.rows[PermissionRuntimeUseSource::OriginalCreator.index()] = 1;
        Ok(())
    }
    pub fn creator_lengths(&self) -> Result<(usize, usize), PermissionError> {
        self.creator_lengths.ok_or(PermissionError::CorruptRecord)
    }
    pub fn capture_creator_raw(
        &mut self,
        thread: &[u8],
        revision: &[u8],
        owner: &[u8],
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        self.control()?;
        let (tl, sl) = self.creator_lengths()?;
        if !matches!(
            self.stage,
            Some(PermissionRuntimeUseReadSql::CreatorPayload)
        ) || tl != thread.len()
            || sl != raw.len()
        {
            return Err(PermissionError::CorruptRecord);
        }
        let s = PermissionRuntimeUseSource::OriginalCreator;
        let c = PermissionRuntimeUseHeaderCells {
            fields: &[thread, revision, owner],
            scalar: 0,
            record_bytes: sl,
        };
        self.validate_fields(s, &c)?;
        // Header is observed in the payload query only, after the lengths range completed.
        self.rows[s.index()] = 0;
        self.retain_header(s, c)?;
        self.budget
            .runtime_reserve(add(mul(raw.len(), 2)?, 256)?, 2)?;
        if self.expected.is_some() {
            if self.header(s, 0)?.payload.as_slice() != raw {
                return Err(PermissionError::CorruptRecord);
            }
        } else {
            self.budget
                .runtime_reserve(add(mul(raw.len(), 13)?, 256)?, 2)?;
            self.raw[s.index()][0].payload = owned(raw)?;
            self.budget.runtime_reserve(128 + 2 * 4096, 1)?;
            let d = self
                .decision
                .as_ref()
                .ok_or(PermissionError::CorruptRecord)?;
            self.creator = Some(validation::ready_use_decode_creator(
                super::PermissionExecutionCells {
                    execution_id: d.creator_execution_id(),
                    thread_id: thread,
                    revision,
                    owner_generation: owner,
                    snapshot: raw,
                },
                self.budget,
            )?);
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<PermissionRuntimeUseEvidence, PermissionError> {
        self.budget.runtime_reserve(128 + 2 * 8192, 1)?;
        for i in 0..11 {
            let source = match i {
                0 => PermissionRuntimeUseSource::CurrentUse,
                1 => PermissionRuntimeUseSource::ThreadDecision,
                2 => PermissionRuntimeUseSource::CurrentLegacyAcceptance,
                3 => PermissionRuntimeUseSource::CurrentGrant,
                4 => PermissionRuntimeUseSource::GlobalUseIdentity,
                5 => PermissionRuntimeUseSource::OriginalAcceptance,
                6 => PermissionRuntimeUseSource::OriginalGrant,
                7 => PermissionRuntimeUseSource::OriginalCreator,
                8 => PermissionRuntimeUseSource::OriginalLegacyAcceptance,
                9 => PermissionRuntimeUseSource::OriginalJournal,
                _ => PermissionRuntimeUseSource::CurrentJournal,
            };
            let payloads = if matches!(
                source,
                PermissionRuntimeUseSource::GlobalUseIdentity
                    | PermissionRuntimeUseSource::CurrentLegacyAcceptance
                    | PermissionRuntimeUseSource::CurrentGrant
                    | PermissionRuntimeUseSource::OriginalLegacyAcceptance
            ) {
                0
            } else {
                self.expected_count(source)
            };
            if !self.ended[i]
                || self.rows[i] != self.expected_count(source)
                || self.payloads[i] != payloads
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
        if self.stage.is_some() {
            return Err(PermissionError::CorruptRecord);
        }
        if self.expected.is_some() {
            self.budget.runtime_reserve(64, 1)?;
            return Ok(PermissionRuntimeUseEvidence {
                inner: UseEvidence::Matched(Arc::clone(self.origin)),
            });
        }
        let u = self
            .use_record
            .take()
            .ok_or(PermissionError::CorruptRecord)?;
        let d = self.decision.take().ok_or(PermissionError::CorruptRecord)?;
        if self.entered_cancellation {
            validate_entered_projection(
                &u,
                &d,
                self.execution,
                self.thread,
                &self.current_journals,
                self.budget,
            )?;
        } else {
            validate_projection(
                &u,
                &d,
                self.execution,
                self.thread,
                &self.current_journals,
                self.budget,
            )?;
        }
        self.budget.runtime_reserve(64, 1)?;
        Ok(PermissionRuntimeUseEvidence {
            inner: UseEvidence::Captured(PermissionRuntimeUseWitness {
                raw: self.raw,
                use_record: u,
                decision: d,
                acceptance: self
                    .acceptance
                    .take()
                    .ok_or(PermissionError::CorruptRecord)?,
                grant: self.grant.take().ok_or(PermissionError::CorruptRecord)?,
                creator: self.creator.take().ok_or(PermissionError::CorruptRecord)?,
                original_journals: self.original_journals,
                current_journals: self.current_journals,
                origin: Arc::clone(self.origin),
                input_profile: self.budget.runtime_input_profile(),
                normal_dispatch_origin: self.budget.runtime_is_normal_new_use_dispatch(),
            }),
        })
    }
}
fn validate_entered_projection(
    u: &PermissionUse,
    d: &PermissionDecision,
    execution: &StoredExecution,
    thread: &ThreadRecord,
    journals: &[JournalEntry],
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let current = &execution.snapshot;
    if current.revision != 3
        || current.status != ExecutionStatus::Running
        || current.tasks.len() != 1
        || journals.len() != 4
        || current.tasks[0].status != TaskStatus::InFlight
        || current.tasks[0].attempt_id.is_none()
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    // Reconstruct the actual authorized Ready projection from immutable Use.
    // This is validation only, never a dispatch claim or callback permit.
    budget.runtime_reserve(131_072, 2048)?;
    let mut ready = u.before_snapshot().clone();
    if ready.tasks.len() != 1 {
        return Err(PermissionError::CorruptRecord);
    }
    ready.revision = u.committed_revision();
    ready.updated_at_ms = u.facts().now_ms;
    ready.tasks[0].approved = true;
    ready.tasks[0].interrupt = Some(u.marker().clone());
    let ready_execution = StoredExecution {
        thread_id: execution.thread_id.clone(),
        snapshot: ready,
    };
    validate_projection(u, d, &ready_execution, thread, &journals[..3], budget)?;
    let plan = b4_kernel::transition(
        &ready_execution.snapshot,
        b4_contract::KernelCommand::Dispatch {
            task_id: current.tasks[0].spec.id.clone(),
            attempt_id: current.tasks[0]
                .attempt_id
                .clone()
                .ok_or(PermissionError::CorruptRecord)?,
        },
        b4_contract::TransitionFacts {
            now_ms: current.updated_at_ms,
        },
    )?;
    if plan.snapshot != *current
        || journals[3] != JournalEntry::from(current)
        || current.updated_at_ms < ready_execution.snapshot.updated_at_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(())
}

fn validate_projection(
    u: &PermissionUse,
    d: &PermissionDecision,
    execution: &StoredExecution,
    thread: &ThreadRecord,
    journals: &[JournalEntry],
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    let before = u.before_snapshot();
    let current = &execution.snapshot;
    budget.runtime_reserve(32_768, 128)?;
    validation::runtime_use_quote_projection(before, current, budget)?;
    if before.tasks.len() != 1
        || current.tasks.len() != 1
        || journals.len() != 3
        || before.revision != 1
        || u.committed_revision() != 2
        || current.revision != 2
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let a = &before.tasks[0];
    let b = &current.tasks[0];
    if u.record_version() != 1
        || u.permission_version() != d.record_version()
        || u.permission_id() != d.permission_id()
        || u.thread_id() != &thread.id
        || u.thread_generation() != &thread.generation
        || u.execution_id() != &current.execution_id
        || u.task_id() != &a.spec.id
        || current.schema_version != before.schema_version
        || current.execution_id != before.execution_id
        || current.turn_id != before.turn_id
        || current.owner_generation != before.owner_generation
        || current.definition != before.definition
        || current.status != before.status
        || current.status != ExecutionStatus::Running
        || current.batch_index != before.batch_index
        || current.values != before.values
        || current.turn_history != before.turn_history
        || current.attempt_history != before.attempt_history
        || current.attempts_used != before.attempts_used
        || current.updated_at_ms != u.facts().now_ms
        || current.output != before.output
        || current.failure != before.failure
        || b.spec != a.spec
        || b.status != a.status
        || b.status != TaskStatus::Ready
        || b.input != a.input
        || b.attempt_id != a.attempt_id
        || b.result != a.result
        || b.failure != a.failure
        || a.approved
        || !b.approved
        || a.interrupt.is_some()
        || b.interrupt.as_ref() != Some(u.marker())
        || u.application() != d.scope().application()
        || u.tool_id() != d.scope().tool_id()
        || u.tool_revision() != d.scope().tool_revision()
        || u.policy_revision() != d.scope().policy_revision()
        || u.constraint_revision() != d.scope().constraint_revision()
    {
        return Err(PermissionError::CorruptRecord);
    }
    let statuses = [
        ExecutionStatus::Accepted,
        ExecutionStatus::Running,
        ExecutionStatus::Running,
    ];
    let mut last = 0;
    for (i, j) in journals.iter().enumerate() {
        if j.execution_id != current.execution_id
            || j.revision != i as u64
            || j.owner_generation != current.owner_generation
            || j.status != statuses[i]
            || j.updated_at_ms < last
        {
            return Err(PermissionError::CorruptRecord);
        }
        last = j.updated_at_ms;
    }
    if journals[1].updated_at_ms != before.updated_at_ms
        || journals[2].updated_at_ms != current.updated_at_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(())
}
impl PermissionRuntimeUseEvidence {
    pub(super) fn into_capture(
        self,
        origin: &Arc<PermissionRuntimeOrigin>,
    ) -> Result<PermissionRuntimeUseWitness, PermissionError> {
        match self.inner {
            UseEvidence::Captured(w) if Arc::ptr_eq(origin, &w.origin) => Ok(w),
            _ => Err(PermissionError::CorruptRecord),
        }
    }
    pub(super) fn require_match(
        self,
        origin: &Arc<PermissionRuntimeOrigin>,
    ) -> Result<(), PermissionError> {
        match self.inner {
            UseEvidence::Matched(o) if Arc::ptr_eq(origin, &o) => Ok(()),
            _ => Err(PermissionError::CorruptRecord),
        }
    }
}
impl PermissionRuntimeUseWitness {
    pub(super) fn validate_ready_resource(
        &self,
        resource: &crate::approval::ApprovalResource,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        validate_projection(
            &self.use_record,
            &self.decision,
            &resource.execution,
            &resource.thread,
            &self.current_journals,
            budget,
        )
    }
    pub(super) fn validate_entered_resource(
        &self,
        resource: &crate::approval::ApprovalResource,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        validate_entered_projection(
            &self.use_record,
            &self.decision,
            &resource.execution,
            &resource.thread,
            &self.current_journals,
            budget,
        )
    }
    pub(super) fn input_profile(&self) -> PermissionRuntimeInputProfile {
        self.input_profile
    }
    pub(super) fn validate_source(
        &self,
        current: &ApprovalResource,
        invocation: &crate::InvocationContext,
        registration: &super::ToolBindingRegistration,
        application: &crate::ApplicationIdentity,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<bool, PermissionError> {
        validation::runtime_use_inherited_geometry(
            self.input_profile,
            self.normal_dispatch_origin,
            &self.decision,
            &self.acceptance,
            &self.grant,
            &self.creator,
            &self.original_journals,
            budget,
        )?;
        validation::ready_use_validate_source(
            &self.decision,
            &self.acceptance,
            &self.grant,
            &self.creator,
            &self.original_journals,
            current,
            invocation,
            registration,
            application,
            budget,
        )
    }
    pub(super) fn same_origin(&self, origin: &Arc<PermissionRuntimeOrigin>) -> bool {
        Arc::ptr_eq(&self.origin, origin)
    }
}

// Exact nominal prospects; no field-sum or anonymous capture-width inference.
const _: () = assert!(size_of::<PermissionRuntimeUseSession<'_>>() <= 8192);
const _: () = assert!(size_of::<PermissionRuntimeUseWitness>() <= 8192);
const _: () = assert!(size_of::<PermissionRuntimeUseEvidence>() <= 8192);
const _: () =
    assert!(size_of::<Result<PermissionRuntimeUseSession<'_>, PermissionError>>() <= 8192);
const _: () = assert!(size_of::<Result<PermissionRuntimeUseEvidence, PermissionError>>() <= 8192);
const _: () = assert!(size_of::<Result<PermissionRuntimeUseWitness, PermissionError>>() <= 8192);
const _: () = assert!(size_of::<UseHeader>() <= 128);
const _: () = assert!(size_of::<UseRawWitness>() <= 128);
const _: () = assert!(size_of::<[Vec<UseRawWitness>; 11]>() <= 512);
const _: () = assert!(size_of::<Result<(), PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<&UseRawWitness, PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<&PermissionUse, PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<&PermissionDecision, PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<&ExecutionId, PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<(&str, &str, usize), PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<(&str, usize), PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<(i64, u64, usize), PermissionError>>() <= 512);
const _: () = assert!(size_of::<Result<(usize, usize), PermissionError>>() <= 512);
const _: () = assert!(size_of::<PermissionRuntimeUseHeaderCells<'_>>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeUseReadSql>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeEvidenceKind>() <= 8);
const _: () = assert!(size_of::<Snapshot>() <= 4096);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeUseSession<'static>,
        >,
    >() <= 8192
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeUseEvidence,
        >,
    >() <= 8192
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionRuntimeUseWitness,
        >,
    >() <= 8192
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, PermissionUse>,
    >() <= 4096
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionDecision,
        >,
    >() <= 4096
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            PermissionAcceptance,
        >,
    >() <= 4096
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, StoredExecution>,
    >() <= 4096
);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, GrantRecord>>(
    ) <= 4096
);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ()>>()
        <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static UseRawWitness,
        >,
    >() <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionUse,
        >,
    >() <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static PermissionDecision,
        >,
    >() <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            &'static ExecutionId,
        >,
    >() <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (&'static str, &'static str, usize),
        >,
    >() <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            (&'static str, usize),
        >,
    >() <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, (i64, u64, usize)>,
    >() <= 512
);
const _: () = assert!(
    size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, (usize, usize)>,
    >() <= 512
);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, usize>>()
        <= 512
);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, u64>>()
        <= 512
);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &'static str>>(
    ) <= 512
);
const _: () = assert!(size_of::<PermissionError>() <= 512);
const _: () = assert!(size_of::<std::str::Utf8Error>() <= 128);
const _: () = assert!(size_of::<std::array::TryFromSliceError>() <= 128);
const _: () = assert!(size_of::<std::collections::TryReserveError>() <= 128);
const _: () = assert!(size_of::<Vec<UseRawWitness>>() <= 32);
const _: () = assert!(size_of::<Vec<Vec<u8>>>() <= 32);
const _: () = assert!(size_of::<Vec<JournalEntry>>() <= 32);
const _: () = assert!(size_of::<&'static [u8]>() <= 16);

const _: () = assert!(size_of::<UseRawWitness>() <= 64);
const _: () = assert!(size_of::<Arc<PermissionRuntimeUseWitness>>() <= 8);

const _: () = assert!(size_of::<crate::StoreError>() <= 512);
const _: () = assert!(size_of::<b4_kernel::KernelError>() <= 512);
const _: () = assert!(size_of::<serde_json::Error>() <= 128);

// Collected complete source-control and materialization carrier prospects.
const _: () = assert!(size_of::<UseEvidence>() <= 8192);
const _: () = assert!(size_of::<Option<PermissionUse>>() <= 4096);
const _: () = assert!(size_of::<Option<PermissionDecision>>() <= 4096);
const _: () = assert!(size_of::<Option<PermissionAcceptance>>() <= 4096);
const _: () = assert!(size_of::<Option<GrantRecord>>() <= 4096);
const _: () = assert!(size_of::<Option<StoredExecution>>() <= 4096);
const _: () = assert!(size_of::<Result<[u8; 8], std::array::TryFromSliceError>>() <= 128);
const _: () = assert!(size_of::<Result<[u8; 8], PermissionError>>() <= 512);
const _: () = assert!(
    size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, [u8; 8]>>()
        <= 512
);
const _: () = assert!(size_of::<[ExecutionStatus; 3]>() <= 128);
const _: () = assert!(size_of::<PermissionRuntimeUseSource>() <= 128);
const _: () = assert!(size_of::<Option<PermissionRuntimeUseReadSql>>() <= 128);

// The renewal leaf borrows existing owners and all eleven old role positions.
// CurrentJ4 only compares its first three raw/typed windows with CurrentJ3 here;
// validating every new row and the won Dispatch remains C2's future obligation.
#[allow(dead_code)]
pub(super) struct PermissionUseRetainedLoan<'source> {
    pub(super) use_record: &'source PermissionUse,
    pub(super) decision: &'source PermissionDecision,
    pub(super) acceptance: &'source PermissionAcceptance,
    pub(super) grant: &'source GrantRecord,
    pub(super) creator: &'source StoredExecution,
    pub(super) original_journals: &'source [JournalEntry],
    pub(super) current_journal_prefix: &'source [JournalEntry],
    witness: &'source PermissionRuntimeUseWitness,
}
impl PermissionRuntimeUseWitness {
    pub(super) fn renewal_source_loan<'source>(
        &'source self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionUseRetainedLoan<'source>, PermissionError> {
        budget.runtime_reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        Ok(PermissionUseRetainedLoan {
            use_record: &self.use_record,
            decision: &self.decision,
            acceptance: &self.acceptance,
            grant: &self.grant,
            creator: &self.creator,
            original_journals: &self.original_journals,
            current_journal_prefix: &self.current_journals,
            witness: self,
        })
    }
}
#[allow(dead_code)]
impl PermissionUseRetainedLoan<'_> {
    // Keep the independently bounded role, header and payload operands explicit.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn raw_window_matches(
        &self,
        role: super::runtime_use_renewal::PermissionUseRenewalSource,
        index: usize,
        fields: &[Vec<u8>],
        scalar: i64,
        bytes: usize,
        payload: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<bool, PermissionError> {
        budget.runtime_reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        let retained = role.retained().ok_or(PermissionError::UnsupportedProfile)?;
        if index
            >= if retained == PermissionRuntimeUseSource::OriginalJournal {
                self.witness.original_journals.len()
            } else {
                role.retained_prefix()
            }
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let window = self.witness.raw[retained.index()]
            .get(index)
            .ok_or(PermissionError::CorruptRecord)?;
        if fields.len() != window.header.fields.len() {
            return Ok(false);
        }
        budget.runtime_reserve(
            0,
            fields
                .len()
                .checked_add(window.header.fields.len())
                .ok_or(PermissionError::LimitExceeded)?,
        )?;
        let mut total = payload
            .len()
            .checked_add(window.payload.len())
            .ok_or(PermissionError::LimitExceeded)?;
        for field in fields {
            total = total
                .checked_add(field.len())
                .ok_or(PermissionError::LimitExceeded)?;
        }
        for field in &window.header.fields {
            total = total
                .checked_add(field.len())
                .ok_or(PermissionError::LimitExceeded)?;
        }
        budget.runtime_reserve(
            total,
            fields
                .len()
                .checked_add(window.header.fields.len())
                .ok_or(PermissionError::LimitExceeded)?,
        )?;
        Ok(window.header.fields == fields
            && window.header.scalar == scalar
            && window.header.bytes == bytes
            && window.payload == payload)
    }
}
const _: () = {
    assert!(size_of::<PermissionUseRetainedLoan<'static>>() <= 128);
    assert!(size_of::<Result<PermissionUseRetainedLoan<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRetainedLoan<'static>,
            >,
        >() <= 128
    );
};

const _: () = {
    assert!(size_of::<Option<&UseRawWitness>>() <= 128);
    assert!(size_of::<(&PermissionRuntimeUseWitness, &mut PermissionEvidenceBudget)>() <= 128);
    assert!(
        size_of::<(
            &PermissionUseRetainedLoan<'static>,
            super::runtime_use_renewal::PermissionUseRenewalSource,
            usize,
            &[Vec<u8>],
            i64,
            usize,
            &[u8],
            &mut PermissionEvidenceBudget
        )>() <= 128
    );
};

const _: () = {
    assert!(size_of::<Result<&UseRawWitness, PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                &UseRawWitness,
            >,
        >() <= 128
    );
};

// New paid borrowed windows: the fresh C2 adapter never clones old source data.
impl<'source> PermissionUseRetainedLoan<'source> {
    #[allow(clippy::type_complexity)] // Borrowed header columns and raw payload stay together.
    pub(super) fn renewal_window(
        &self,
        role: super::runtime_use_renewal::PermissionUseRenewalSource,
        index: usize,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(&'source [Vec<u8>], i64, usize, &'source [u8]), PermissionError> {
        budget.runtime_reserve(super::runtime_use_renewal::LEAF_FIXED, 8)?;
        budget.runtime_reserve(super::runtime_use_renewal::SOURCE_CALL_FIXED, 64)?;
        let retained = role.retained().ok_or(PermissionError::UnsupportedProfile)?;
        if index
            >= if retained == PermissionRuntimeUseSource::OriginalJournal {
                self.witness.original_journals.len()
            } else {
                role.retained_prefix()
            }
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let raw = self.witness.raw[retained.index()]
            .get(index)
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((
            &raw.header.fields,
            raw.header.scalar,
            raw.header.bytes,
            &raw.payload,
        ))
    }
}
const _: () = {
    assert!(size_of::<Result<(&[Vec<u8>], i64, usize, &[u8]), PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (&[Vec<u8>], i64, usize, &[u8]),
            >,
        >() <= 128
    );
};

// Outcome-purpose borrowed accessors: never lend a renewal source seal.
type OutcomeRetainedWindow<'source> = (&'source [Vec<u8>], i64, usize, &'source [u8]);

impl PermissionRuntimeUseWitness {
    pub(super) fn outcome_source_loan<'source>(
        &'source self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionUseRetainedLoan<'source>, PermissionError> {
        budget.outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
        Ok(PermissionUseRetainedLoan {
            use_record: &self.use_record,
            decision: &self.decision,
            acceptance: &self.acceptance,
            grant: &self.grant,
            creator: &self.creator,
            original_journals: &self.original_journals,
            current_journal_prefix: &self.current_journals,
            witness: self,
        })
    }
}
impl<'source> PermissionUseRetainedLoan<'source> {
    pub(super) fn outcome_window(
        &self,
        role: super::runtime_use_outcome_source::PermissionUseOutcomeSource,
        index: usize,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<OutcomeRetainedWindow<'source>, PermissionError> {
        budget.outcome_source_reserve(super::runtime_use_outcome_source::LEAF_FIXED, 8)?;
        budget.outcome_source_reserve(super::runtime_use_outcome_source::SOURCE_CALL_FIXED, 64)?;
        let retained = role.retained().ok_or(PermissionError::UnsupportedProfile)?;
        if index
            >= if retained == PermissionRuntimeUseSource::OriginalJournal {
                self.witness.original_journals.len()
            } else {
                role.retained_prefix()
            }
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let raw = self.witness.raw[retained.index()]
            .get(index)
            .ok_or(PermissionError::CorruptRecord)?;
        Ok((
            &raw.header.fields,
            raw.header.scalar,
            raw.header.bytes,
            &raw.payload,
        ))
    }
}

impl PermissionRuntimeUseWitness {
    pub(super) fn original_journal_count(&self) -> usize {
        self.original_journals.len()
    }
}

impl PermissionRuntimeUseWitness {
    pub(super) fn matches_withheld_ready(
        &self,
        ready: &Self,
        producer: &Arc<PermissionRuntimeOrigin>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        budget.runtime_reserve(262_144, 4096)?;
        if !ready.same_origin(producer)
            || self.current_journals.len() != 4
            || ready.current_journals.len() != 3
            || self.current_journals[..3] != ready.current_journals[..]
        {
            return Err(PermissionError::CorruptRecord);
        }
        for (role, (actual, retained)) in self.raw.iter().zip(ready.raw.iter()).enumerate() {
            let prefix = role == PermissionRuntimeUseSource::CurrentJournal.index();
            if actual.len() != retained.len() + usize::from(prefix) {
                return Err(PermissionError::CorruptRecord);
            }
            for (a, b) in actual.iter().zip(retained.iter()) {
                let bytes = a
                    .payload
                    .len()
                    .checked_add(b.payload.len())
                    .and_then(|n| n.checked_add(a.header.bytes))
                    .and_then(|n| n.checked_add(b.header.bytes))
                    .ok_or(PermissionError::LimitExceeded)?;
                budget.runtime_reserve(bytes, 8)?;
                if a.header.fields != b.header.fields
                    || a.header.scalar != b.header.scalar
                    || a.header.bytes != b.header.bytes
                    || a.payload != b.payload
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
        }
        Ok(())
    }
}
