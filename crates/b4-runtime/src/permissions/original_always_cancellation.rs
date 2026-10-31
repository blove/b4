//! Dedicated management source for a genuinely entered original Always callback.
use super::*;
use crate::approval::{validate_approval_context, ApprovalContext, ApprovalResource, GrantRecord};
use crate::{InvocationContext, JournalEntry, RunCancellationTarget, StoreError, StoredExecution};
use b4_contract::{
    CommitPlan, ExecutionId, ExecutionStatus, KernelCommand, TaskStatus, TransitionFacts,
};

pub struct PermissionOriginalAlwaysCancellationRequest<'a> {
    pub context: &'a ApprovalContext,
    pub target: &'a RunCancellationTarget,
    pub facts: TransitionFacts,
}
/// Selection is sealed to one request and, on immediate reread, its exact source.
pub struct PermissionOriginalAlwaysCancellationSelector<'a> {
    request: &'a PermissionOriginalAlwaysCancellationRequest<'a>,
    expected: Option<&'a PermissionOriginalAlwaysCancellationCapture>,
}
pub struct PermissionOriginalAlwaysCancellationCapture {
    resource: ApprovalResource,
    acceptance: PermissionAcceptance,
    decision: PermissionDecision,
    grant: GrantRecord,
    records: [Vec<u8>; 3],
    journals: Vec<JournalEntry>,
    journal_raw: Vec<Vec<u8>>,
    matched: bool,
}
pub struct PermissionOriginalAlwaysCancellationRead<'a> {
    selector: &'a PermissionOriginalAlwaysCancellationSelector<'a>,
    resource: ApprovalResource,
    records: [Option<Vec<u8>>; 3],
    absent: bool,
    journals: Vec<JournalEntry>,
    journal_raw: Vec<Vec<u8>>,
}
pub struct PermissionPreparedOriginalAlwaysCancellation<'a> {
    request: &'a PermissionOriginalAlwaysCancellationRequest<'a>,
    capture: PermissionOriginalAlwaysCancellationCapture,
    plan: CommitPlan,
}
pub struct PermissionOriginalAlwaysCancellationCommit {
    execution: StoredExecution,
}
fn target_matches(
    resource: &ApprovalResource,
    target: &RunCancellationTarget,
) -> Result<(), PermissionError> {
    let t = &resource.thread;
    let e = &resource.execution;
    let snapshot = &e.snapshot;
    if t.id != target.thread_id
        || t.generation != target.thread_generation
        || t.resource_revision != target.resource_revision
        || e.thread_id != target.thread_id
        || t.active_execution_id.as_ref() != Some(&target.execution_id)
        || snapshot.execution_id != target.execution_id
        || snapshot.turn_id != target.turn_id
        || crate::ExecutionFence::from(snapshot) != target.execution_fence
    {
        return Err(StoreError::RevisionConflict.into());
    }
    let s = &resource.execution.snapshot;
    if s.revision != 4
        || s.status != ExecutionStatus::Running
        || s.tasks.len() != 1
        || s.tasks[0].status != TaskStatus::InFlight
        || s.tasks[0].attempt_id.is_none()
        || s.definition.batches.len() != 1
        || s.definition.batches[0].len() != 1
        || s.tasks[0].spec != s.definition.batches[0][0]
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    Ok(())
}
impl PermissionOriginalAlwaysCancellationSelector<'_> {
    /// Charge the bounded physical resource read before decoding/copying it.
    pub fn before_resource(
        &self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        budget.runtime_reserve(262_144, 4096)
    }
    pub fn execution_id(&self) -> &ExecutionId {
        &self.request.target.execution_id
    }
    pub fn thread_id(&self) -> &b4_contract::ThreadId {
        &self.request.target.thread_id
    }
    pub fn begin<'a>(
        &'a self,
        resource: ApprovalResource,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionOriginalAlwaysCancellationRead<'a>, PermissionError> {
        budget.runtime_reserve(262_144, 4096)?;
        target_matches(&resource, self.request.target)?;
        if resource.thread.origin.as_ref() != Some(&self.request.context.actor) {
            return Err(crate::approval::ApprovalError::Unauthorized.into());
        }
        if self.expected.is_some_and(|old| old.resource != resource) {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionOriginalAlwaysCancellationRead {
            selector: self,
            resource,
            records: [None, None, None],
            absent: false,
            journals: Vec::new(),
            journal_raw: Vec::new(),
        })
    }
}
impl PermissionOriginalAlwaysCancellationRead<'_> {
    /// Physical indices and immutable bytes are checked together, before retention.
    pub fn record(
        &mut self,
        source: usize,
        fields: &[&[u8]],
        scalar: i64,
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        if source > 2 || self.records[source].is_some() || raw.len() > [8192, 4096, 4096][source] {
            return Err(PermissionError::CorruptRecord);
        }
        budget.runtime_reserve(
            raw.len()
                .checked_mul(16)
                .ok_or(PermissionError::LimitExceeded)?,
            256,
        )?;
        if let Some(old) = self.selector.expected {
            if old.records[source] != raw {
                return Err(PermissionError::CorruptRecord);
            }
        }
        let valid = match source {
            0 => {
                let a = PermissionAcceptance::decode_record(raw, budget)?;
                let previous = a.before_snapshot().revision.to_be_bytes();
                let committed = a.committed_revision().to_be_bytes();
                scalar == i64::from(a.record_version())
                    && fields
                        == [
                            a.before_snapshot().execution_id.as_str().as_bytes(),
                            a.turn_id().as_str().as_bytes(),
                            a.thread_id().as_str().as_bytes(),
                            a.thread_generation().as_str().as_bytes(),
                            &previous,
                            &committed,
                        ]
            }
            1 => {
                let d = PermissionDecision::decode_record(raw, budget)?;
                scalar == i64::from(d.record_version())
                    && fields
                        == [
                            d.permission_id().as_str().as_bytes(),
                            d.scope().thread_id().as_str().as_bytes(),
                            d.scope().thread_generation().as_str().as_bytes(),
                            d.scope_digest().as_bytes(),
                        ]
            }
            _ => {
                let (g, _) = super::validation::runtime_compat::capture_grant(raw, budget)?;
                let parking = g.task_parking_revision.to_be_bytes();
                scalar == 1
                    && g.consumed
                    && fields
                        == [
                            g.execution_id.as_str().as_bytes(),
                            g.interrupt_id.as_str().as_bytes(),
                            g.task_id.as_str().as_bytes(),
                            g.thread_id.as_str().as_bytes(),
                            g.thread_generation.as_str().as_bytes(),
                            &parking,
                        ]
            }
        };
        if !valid {
            return Err(PermissionError::CorruptRecord);
        }
        self.records[source] = Some(raw.to_vec());
        Ok(())
    }
    pub fn absence(&mut self, uses: bool, legacy: bool) -> Result<(), PermissionError> {
        if uses || legacy || self.absent {
            return Err(PermissionError::CorruptRecord);
        }
        self.absent = true;
        Ok(())
    }
    pub fn journal(
        &mut self,
        entry: JournalEntry,
        raw: &[u8],
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        budget.runtime_reserve(40_960, 128)?;
        if self.journals.len() >= 5 {
            return Err(PermissionError::CorruptRecord);
        }
        if raw.len() > 1024 {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.journal_raw.push(raw.to_vec());
        self.journals.push(entry);
        Ok(())
    }
    pub fn finish(
        self,
        owner: u64,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionOriginalAlwaysCancellationCapture, PermissionError> {
        if !self.absent || self.journals.len() != 5 {
            return Err(PermissionError::CorruptRecord);
        }
        if self.resource.execution.snapshot.owner_generation != owner {
            return Err(StoreError::OwnerConflict.into());
        }
        let [a, d, g] = self.records;
        let records = [
            a.ok_or(PermissionError::CorruptRecord)?,
            d.ok_or(PermissionError::CorruptRecord)?,
            g.ok_or(PermissionError::CorruptRecord)?,
        ];
        let acceptance = PermissionAcceptance::decode_record(&records[0], budget)?;
        let decision = PermissionDecision::decode_record(&records[1], budget)?;
        let (grant, _) = super::validation::runtime_compat::capture_grant(&records[2], budget)?;
        if let Some(old) = self.selector.expected {
            if old.journals != self.journals
                || old.journal_raw != self.journal_raw
                || old.resource != self.resource
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
        Ok(PermissionOriginalAlwaysCancellationCapture {
            resource: self.resource,
            acceptance,
            decision,
            grant,
            records,
            journals: self.journals,
            journal_raw: self.journal_raw,
            matched: self.selector.expected.is_some(),
        })
    }
}
impl PermissionPreparedOriginalAlwaysCancellation<'_> {
    pub fn selector(&self) -> PermissionOriginalAlwaysCancellationSelector<'_> {
        PermissionOriginalAlwaysCancellationSelector {
            request: self.request,
            expected: Some(&self.capture),
        }
    }
    pub fn resource(&self) -> &ApprovalResource {
        &self.capture.resource
    }
    pub fn plan(&self) -> &CommitPlan {
        &self.plan
    }
    pub fn acknowledge(
        &self,
        capture: PermissionOriginalAlwaysCancellationCapture,
    ) -> Result<PermissionOriginalAlwaysCancellationCommit, PermissionError> {
        if !capture.matched
            || capture.resource != self.capture.resource
            || capture.records != self.capture.records
            || capture.journals != self.capture.journals
            || capture.journal_raw != self.capture.journal_raw
        {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionOriginalAlwaysCancellationCommit {
            execution: StoredExecution {
                thread_id: capture.resource.thread.id,
                snapshot: self.plan.snapshot.clone(),
            },
        })
    }
}
pub fn request_permission_original_always_cancellation<S, A>(
    store: &S,
    authority: &A,
    catalog: &PermissionCatalog,
    request: &PermissionOriginalAlwaysCancellationRequest<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<StoredExecution, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: PermissionCancellationAuthority + ?Sized,
{
    if !store.permission_original_always_cancellation_atomic_durable() {
        return Err(PermissionError::UnsupportedProfile);
    }
    validate_approval_context(request.context)?;
    budget.runtime_reserve(1024, 32)?;
    let selector = PermissionOriginalAlwaysCancellationSelector {
        request,
        expected: None,
    };
    let capture = store.capture_permission_original_always_cancellation(&selector, budget)?;
    if capture.matched {
        return Err(PermissionError::CorruptRecord);
    }
    target_matches(&capture.resource, request.target)?;
    if capture.resource.execution.snapshot.owner_generation != store.owner_generation() {
        return Err(StoreError::OwnerConflict.into());
    }
    let resource = &capture.resource;
    let s = &resource.execution.snapshot;
    let registration =
        catalog.ready_registration(&s.definition, &s.tasks[0].spec.effect_id, budget)?;
    let context = InvocationContext {
        thread_id: resource.thread.id.clone(),
        thread_generation: resource.thread.generation.clone(),
        origin: request.context.actor.clone(),
        application_revision: catalog.application_identity().revision.clone(),
        execution_revision: s.definition.revision.clone(),
        policy_revision: request.context.policy_revision.clone(),
    };
    if !super::validation::original_running_validate_source(
        &capture.decision,
        &capture.acceptance,
        &capture.grant,
        &resource.execution,
        &capture.journals,
        resource,
        &context,
        registration,
        catalog.application_identity(),
        budget,
    )? {
        return Err(PermissionError::ConstraintDenied);
    }
    authority.authorize_cancel(request.context, resource, request.target)?;
    budget.runtime_reserve(262_144, 4096)?;
    let plan = b4_kernel::transition(
        s,
        KernelCommand::Cancel {
            turn_id: s.turn_id.clone(),
            owner_generation: s.owner_generation,
        },
        request.facts.clone(),
    )?;
    if !super::original_always_cancellation_relation::snapshot_matches(s, &plan.snapshot) {
        return Err(PermissionError::CorruptRecord);
    }
    let prepared = PermissionPreparedOriginalAlwaysCancellation {
        request,
        capture,
        plan,
    };
    let committed = store.commit_permission_original_always_cancellation(&prepared, budget)?;
    if committed.execution.thread_id != prepared.capture.resource.execution.thread_id
        || committed.execution.snapshot != prepared.plan.snapshot
    {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(committed.execution)
}
