//! Explicit management of physically quiescent, settled permission history.
use super::*;
use crate::{
    approval::{ApprovalContext, ApprovalError},
    JournalEntry, StoredExecution, ThreadDeletionTarget, ThreadView,
};
use b4_contract::ExecutionId;
use std::sync::Arc;

/// Dedicated management policy, called after capture and outside Store locks.
/// The embedding must hold per-thread exclusion and ensure every Driver/native
/// invocation for this thread has returned, retaining exclusion through result.
/// A durable terminal status alone does not prove physical callback quiescence.
pub trait PermissionThreadDeletionAuthority: Send + Sync {
    fn authorize_delete(
        &self,
        context: &ApprovalContext,
        resource: &PermissionThreadDeletionResource,
        target: &ThreadDeletionTarget,
    ) -> Result<(), PermissionError>;
}
impl<A: PermissionThreadDeletionAuthority + ?Sized> PermissionThreadDeletionAuthority for Arc<A> {
    fn authorize_delete(
        &self,
        context: &ApprovalContext,
        resource: &PermissionThreadDeletionResource,
        target: &ThreadDeletionTarget,
    ) -> Result<(), PermissionError> {
        (**self).authorize_delete(context, resource, target)
    }
}

/// Immutable historical resource plus a separate current writer epoch.
pub struct PermissionThreadDeletionResource {
    view: ThreadView,
    owner: u64,
}
impl PermissionThreadDeletionResource {
    pub fn view(&self) -> &ThreadView {
        &self.view
    }
    pub fn store_owner_generation(&self) -> u64 {
        self.owner
    }
}

/// One sealed monotonic account. Backend ports can debit work, never reset or
/// borrow its inner codec budget. Row maxima do not guarantee joint admission.
pub struct PermissionThreadDeletionAccount {
    codec: PermissionEvidenceBudget,
}
impl PermissionThreadDeletionAccount {
    fn new() -> Self {
        Self {
            codec: PermissionEvidenceBudget::for_settled_thread_deletion(),
        }
    }
    pub fn charge(&mut self, bytes: usize, nodes: usize) -> Result<(), PermissionError> {
        self.codec.reserve(bytes, nodes)
    }
    pub fn execution(
        &mut self,
        cells: PermissionExecutionCells<'_>,
    ) -> Result<StoredExecution, PermissionError> {
        validation::resource_execution(cells, &mut self.codec)
    }
    pub fn thread(
        &mut self,
        cells: PermissionThreadCells<'_>,
    ) -> Result<crate::ThreadRecord, PermissionError> {
        validation::resource_thread(cells, &mut self.codec)
    }
    pub fn grant(&mut self, raw: &[u8]) -> Result<crate::approval::GrantRecord, PermissionError> {
        validation::decode_legacy_grant(raw, &mut self.codec)
    }
    pub fn ordinary_acceptance(
        &mut self,
        raw: &[u8],
    ) -> Result<crate::approval::ApprovalAcceptance, PermissionError> {
        validation::decode_legacy_acceptance(raw, &mut self.codec)
    }
    pub fn acceptance(&mut self, raw: &[u8]) -> Result<PermissionAcceptance, PermissionError> {
        PermissionAcceptance::decode_record(raw, &mut self.codec)
    }
    pub fn decision(&mut self, raw: &[u8]) -> Result<PermissionDecision, PermissionError> {
        PermissionDecision::decode_record(raw, &mut self.codec)
    }
    pub fn permission_use(&mut self, raw: &[u8]) -> Result<PermissionUse, PermissionError> {
        PermissionUse::decode_record(raw, &mut self.codec)
    }
    pub fn journal(&mut self, raw: &[u8]) -> Result<JournalEntry, PermissionError> {
        self.codec.settled_deletion_journal_preflight(raw)?;
        serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)
    }
    /// Purchase and append one exact physical cell with an unambiguous tag/length.
    pub fn append_cell(
        &mut self,
        source: &mut Vec<u8>,
        tag: u8,
        bytes: &[u8],
    ) -> Result<(), PermissionError> {
        let size = bytes
            .len()
            .checked_add(9)
            .ok_or(PermissionError::LimitExceeded)?;
        self.charge(
            size.checked_mul(3).ok_or(PermissionError::LimitExceeded)?,
            1,
        )?;
        source
            .try_reserve(size)
            .map_err(|_| PermissionError::LimitExceeded)?;
        source.push(tag);
        source.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        source.extend_from_slice(bytes);
        Ok(())
    }
}

/// Backend capture port. Construction checks exact target and terminal eligibility;
/// the backend additionally validates every bounded physical ownership relation.
pub struct PermissionThreadDeletionCapture {
    resource: PermissionThreadDeletionResource,
    executions: Vec<StoredExecution>,
    source: Vec<u8>,
    counts: [usize; 8],
}
impl PermissionThreadDeletionCapture {
    pub fn from_validated_source(
        view: ThreadView,
        executions: Vec<StoredExecution>,
        owner: u64,
        source: Vec<u8>,
        counts: [usize; 8],
        target: &ThreadDeletionTarget,
        account: &mut PermissionThreadDeletionAccount,
    ) -> Result<Self, PermissionError> {
        account.charge(
            source
                .len()
                .checked_mul(2)
                .ok_or(PermissionError::LimitExceeded)?,
            32,
        )?;
        if owner == 0
            || executions.len() > 16
            || counts[0] != 1
            || counts[1] != executions.len()
            || counts[2] > 512
            || counts[3..].iter().any(|n| *n > 64)
        {
            return Err(PermissionError::LimitExceeded);
        }
        if view.execution.is_none() {
            return Err(crate::StoreError::ThreadBusy.into());
        }
        crate::validate_thread_deletion(&view, target, &executions)?;
        if executions.iter().any(|execution| {
            !matches!(
                execution.snapshot.status,
                b4_contract::ExecutionStatus::Completed
                    | b4_contract::ExecutionStatus::Failed
                    | b4_contract::ExecutionStatus::Cancelled
            )
        }) {
            return Err(crate::StoreError::ThreadBusy.into());
        }
        Ok(Self {
            resource: PermissionThreadDeletionResource { view, owner },
            executions,
            source,
            counts,
        })
    }
    pub fn resource(&self) -> &PermissionThreadDeletionResource {
        &self.resource
    }
    pub fn counts(&self) -> &[usize; 8] {
        &self.counts
    }
    pub fn matches(
        &self,
        other: &Self,
        account: &mut PermissionThreadDeletionAccount,
    ) -> Result<bool, PermissionError> {
        account.charge(
            self.source
                .len()
                .checked_add(other.source.len())
                .ok_or(PermissionError::LimitExceeded)?,
            32,
        )?;
        Ok(self.resource.owner == other.resource.owner
            && self.source == other.source
            && self.counts == other.counts)
    }
    /// Backend acknowledgment of actual counts, to be returned only after commit.
    pub fn acknowledge_removed(
        &self,
        counts: [usize; 8],
    ) -> Result<PermissionThreadDeletionCommit, PermissionError> {
        if counts != self.counts {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionThreadDeletionCommit {
            removed: counts.iter().sum(),
        })
    }
}
pub struct PermissionThreadDeletionCommit {
    removed: usize,
}

/// Actual committed deletion; no public constructor, wire decoder or replay role.
#[derive(Debug)]
pub struct PermissionThreadDeletionConfirmation {
    target: ThreadDeletionTarget,
    owner: u64,
    ids: Vec<ExecutionId>,
    removed: usize,
}
impl PermissionThreadDeletionConfirmation {
    pub fn target(&self) -> &ThreadDeletionTarget {
        &self.target
    }
    pub fn store_owner_generation(&self) -> u64 {
        self.owner
    }
    pub fn removed_execution_ids(&self) -> &[ExecutionId] {
        &self.ids
    }
    pub fn removed_rows(&self) -> usize {
        self.removed
    }
}

/// Delete only complete settled history after current dedicated authorization.
/// Caller/authority must hold per-thread exclusion and real native/Driver
/// quiescence through return. In particular, wait for synchronous Drivers to
/// return before calling this entry, even if cancellation is already durable.
/// Errors, including uncertain commit errors, grant no confirmation or retry.
pub fn delete_settled_permission_thread<S, A>(
    store: Arc<S>,
    authority: Arc<A>,
    context: &ApprovalContext,
    target: &ThreadDeletionTarget,
) -> Result<PermissionThreadDeletionConfirmation, PermissionError>
where
    S: PermissionStore + 'static,
    A: PermissionThreadDeletionAuthority + ?Sized + 'static,
{
    if !store.permission_settled_thread_deletion_atomic_durable() {
        return Err(PermissionError::UnsupportedProfile);
    }
    crate::approval::validate_approval_context(context)?;
    let mut account = PermissionThreadDeletionAccount::new();
    account.charge(8192, 16)?;
    let capture = store.capture_permission_settled_thread_deletion(target, &mut account)?;
    if capture.resource.view.thread.origin.as_ref() != Some(&context.actor) {
        return Err(ApprovalError::Unauthorized.into());
    }
    authority.authorize_delete(context, capture.resource(), target)?;
    account.charge(8192, 16)?;
    let captured_target = target.clone();
    let ids = capture
        .executions
        .iter()
        .map(|e| e.snapshot.execution_id.clone())
        .collect();
    let commit = store.commit_permission_settled_thread_deletion(&capture, target, &mut account)?;
    // All fallible account work and result allocation precede the durable writer.
    Ok(PermissionThreadDeletionConfirmation {
        target: captured_target,
        owner: capture.resource.owner,
        ids,
        removed: commit.removed,
    })
}
