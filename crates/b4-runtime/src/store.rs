use crate::confinement::{
    RecoveryConfinementBackendError, RecoveryConfinementOutcome, RecoveryConfinementReason,
    RecoveryConfinementTarget,
};
use crate::discovery::{DiscoveryError, UnfinishedDiscoveryCursor, UnfinishedDiscoveryPage};
use b4_contract::{
    CommitPlan, ExecutionId, ExecutionStatus, Snapshot, ThreadGeneration, ThreadId,
    TransitionFacts, TurnId,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

/// A finite backend classification for capability discovery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StoreKind {
    #[default]
    Unspecified,
    Memory,
    Sqlite,
}

/// Static implementation declarations, independent of current readiness.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreCapabilities {
    pub kind: StoreKind,
    pub durable_runtime: bool,
    pub fenced_cancellation: bool,
    pub fenced_deletion: bool,
}

/// Immutable server-owned context. User metadata never supplies this identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadOrigin {
    pub application: String,
    pub tenant: String,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadSeed {
    pub id: ThreadId,
    pub created_at_ms: u64,
    pub metadata: Map<String, Value>,
    pub origin: Option<ThreadOrigin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadRecord {
    pub id: ThreadId,
    pub generation: ThreadGeneration,
    pub active_execution_id: Option<ExecutionId>,
    pub created_at_ms: u64,
    pub metadata: Map<String, Value>,
    pub origin: Option<ThreadOrigin>,
    pub resource_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadView {
    pub thread: ThreadRecord,
    pub execution: Option<StoredExecution>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredExecution {
    pub thread_id: ThreadId,
    pub snapshot: Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionFence {
    pub revision: u64,
    pub owner_generation: u64,
}

impl From<&Snapshot> for ExecutionFence {
    fn from(snapshot: &Snapshot) -> Self {
        Self {
            revision: snapshot.revision,
            owner_generation: snapshot.owner_generation,
        }
    }
}

/// An exact server-captured thread incarnation and its current execution fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadDeletionTarget {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub resource_revision: u64,
    pub active_execution_id: Option<ExecutionId>,
    pub execution_fence: Option<ExecutionFence>,
}

impl ThreadDeletionTarget {
    /// Capture identity before policy renewal. Busy snapshots can be captured;
    /// deletion safety is checked again inside the backend transaction.
    pub fn from_view(view: &ThreadView) -> Result<Self, StoreError> {
        match (&view.thread.active_execution_id, &view.execution) {
            (None, None) => {}
            (Some(id), Some(execution))
                if execution.thread_id == view.thread.id
                    && &execution.snapshot.execution_id == id => {}
            _ => {
                return Err(StoreError::Corrupt(
                    "thread_deletion_active_relationship".into(),
                ))
            }
        }
        Ok(Self {
            thread_id: view.thread.id.clone(),
            thread_generation: view.thread.generation.clone(),
            resource_revision: view.thread.resource_revision,
            active_execution_id: view.thread.active_execution_id.clone(),
            execution_fence: view
                .execution
                .as_ref()
                .map(|execution| (&execution.snapshot).into()),
        })
    }
}

/// Immutable run identity captured before authorization is renewed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCancellationTarget {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub resource_revision: u64,
    pub execution_id: ExecutionId,
    pub execution_fence: ExecutionFence,
    pub turn_id: TurnId,
}

impl RunCancellationTarget {
    pub fn from_view(view: &ThreadView) -> Result<Self, StoreError> {
        match (&view.thread.active_execution_id, &view.execution) {
            (None, None) => Err(StoreError::NoRunInFlight),
            (Some(id), Some(execution))
                if execution.thread_id == view.thread.id
                    && &execution.snapshot.execution_id == id =>
            {
                Ok(Self {
                    thread_id: view.thread.id.clone(),
                    thread_generation: view.thread.generation.clone(),
                    resource_revision: view.thread.resource_revision,
                    execution_id: id.clone(),
                    execution_fence: (&execution.snapshot).into(),
                    turn_id: execution.snapshot.turn_id.clone(),
                })
            }
            _ => Err(StoreError::Corrupt(
                "run_cancellation_active_relationship".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalEntry {
    pub execution_id: ExecutionId,
    pub revision: u64,
    pub owner_generation: u64,
    pub status: ExecutionStatus,
    pub updated_at_ms: u64,
}

impl From<&Snapshot> for JournalEntry {
    fn from(s: &Snapshot) -> Self {
        Self {
            execution_id: s.execution_id.clone(),
            revision: s.revision,
            owner_generation: s.owner_generation,
            status: s.status,
            updated_at_ms: s.updated_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    ThreadExists,
    ThreadNotFound,
    ExecutionExists,
    ExecutionNotFound,
    ExecutionNotActive,
    ThreadBusy,
    NoRunInFlight,
    RevisionConflict,
    OwnerConflict,
    InvalidThread(String),
    InvalidSnapshot(String),
    InvalidCommit(String),
    Unavailable(String),
    Corrupt(String),
    AlreadyOwned,
    UnsupportedSchema,
    UnsupportedOperation,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StoreError {}

/// Implementations atomically compare the fence, replace the snapshot and append
/// its journal record. All inputs are trusted internal runtime data, not AP DTOs.
pub trait Store: Send + Sync {
    /// Custom stores advertise no capabilities unless explicitly declared.
    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities::default()
    }
    fn owner_generation(&self) -> u64;
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.create_thread_with(ThreadSeed {
            id,
            created_at_ms: 0,
            metadata: Map::new(),
            origin: None,
        })
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError>;
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError>;
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError>;
    fn delete_thread_fenced(&self, _target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        Err(StoreError::UnsupportedOperation)
    }
    fn cancel_run_fenced(
        &self,
        _target: &RunCancellationTarget,
        _facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        Err(StoreError::UnsupportedOperation)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError>;
    fn admit(&self, thread_id: &ThreadId, initial: Snapshot)
        -> Result<StoredExecution, StoreError>;
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError>;
    fn readiness(&self) -> Result<(), StoreError>;
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError>;
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError>;

    /// Read bounded recorded observations for a trusted embedding.
    ///
    /// A page neither authorizes recovery nor certifies readiness. Custom stores
    /// retain explicit refusal unless they implement the discovery contract.
    fn discover_unfinished(
        &self,
        _cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        Err(DiscoveryError::UnsupportedOperation)
    }

    /// Capture one bounded, fully audited ordinary view for this exact target.
    /// This observation does not authorize confinement or recovery.
    fn confinement_view_fenced(
        &self,
        _target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        Err(RecoveryConfinementBackendError::UnsupportedOperation)
    }

    /// Re-audit the exact original view and reconstruct a no-dispatch block
    /// atomically. A returned receipt does not establish physical retirement.
    fn confine_recovery_fenced(
        &self,
        _original: &ThreadView,
        _target: &RecoveryConfinementTarget,
        _reason: RecoveryConfinementReason,
        _fresh_turn: &TurnId,
        _facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        Err(RecoveryConfinementBackendError::UnsupportedOperation)
    }
}

impl<S: Store + ?Sized> Store for Arc<S> {
    fn capabilities(&self) -> StoreCapabilities {
        (**self).capabilities()
    }
    fn owner_generation(&self) -> u64 {
        (**self).owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        (**self).create_thread(id)
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        (**self).create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        (**self).thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        (**self).thread_view(id)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        (**self).delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        (**self).cancel_run_fenced(target, facts)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        (**self).execution(id)
    }
    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        (**self).admit(thread_id, initial)
    }
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        (**self).admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )
    }
    fn readiness(&self) -> Result<(), StoreError> {
        (**self).readiness()
    }
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        (**self).commit(execution_id, fence, plan)
    }
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        (**self).journal(execution_id)
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        (**self).discover_unfinished(cursor)
    }

    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        (**self).confinement_view_fenced(target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        (**self).confine_recovery_fenced(original, target, reason, fresh_turn, facts)
    }
}

impl<S: Store + ?Sized> Store for &S {
    fn capabilities(&self) -> StoreCapabilities {
        (**self).capabilities()
    }
    fn owner_generation(&self) -> u64 {
        (**self).owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        (**self).create_thread(id)
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        (**self).create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        (**self).thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        (**self).thread_view(id)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        (**self).delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        (**self).cancel_run_fenced(target, facts)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        (**self).execution(id)
    }
    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        (**self).admit(thread_id, initial)
    }
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        (**self).admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )
    }
    fn readiness(&self) -> Result<(), StoreError> {
        (**self).readiness()
    }
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        (**self).commit(execution_id, fence, plan)
    }
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        (**self).journal(execution_id)
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        (**self).discover_unfinished(cursor)
    }

    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        (**self).confinement_view_fenced(target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        (**self).confine_recovery_fenced(original, target, reason, fresh_turn, facts)
    }
}

pub fn validate_thread_seed(seed: &ThreadSeed) -> Result<(), StoreError> {
    if let Some(origin) = &seed.origin {
        for component in [&origin.application, &origin.tenant, &origin.principal] {
            if component.is_empty()
                || component.len() > 256
                || component.chars().any(char::is_control)
            {
                return Err(StoreError::InvalidThread("invalid_origin_component".into()));
            }
        }
    }
    Ok(())
}

/// Validate the exact captured target and every execution owned by this thread.
/// The backend must supply the complete set under the deletion transaction and
/// separately check the ownership of its journal and approval indexes.
pub fn validate_thread_deletion(
    view: &ThreadView,
    target: &ThreadDeletionTarget,
    executions: &[StoredExecution],
) -> Result<(), StoreError> {
    if ThreadDeletionTarget::from_view(view)? != *target {
        return Err(StoreError::RevisionConflict);
    }
    let mut ids = BTreeSet::new();
    let mut active_found = view.execution.is_none();
    for execution in executions {
        if execution.thread_id != view.thread.id || !ids.insert(&execution.snapshot.execution_id) {
            return Err(StoreError::Corrupt(
                "thread_deletion_execution_ownership".into(),
            ));
        }
        b4_kernel::validate_snapshot(&execution.snapshot)
            .map_err(|_| StoreError::Corrupt("thread_deletion_snapshot_invalid".into()))?;
        if view.thread.active_execution_id.as_ref() == Some(&execution.snapshot.execution_id) {
            if view.execution.as_ref() != Some(execution) {
                return Err(StoreError::Corrupt(
                    "thread_deletion_active_snapshot".into(),
                ));
            }
            active_found = true;
        }
    }
    if !active_found {
        return Err(StoreError::Corrupt("thread_deletion_active_missing".into()));
    }
    for execution in executions {
        if releases_slot(execution.snapshot.status) {
            continue;
        }
        let active =
            view.thread.active_execution_id.as_ref() == Some(&execution.snapshot.execution_id);
        if !active {
            return Err(StoreError::Corrupt(
                "thread_deletion_history_unsettled".into(),
            ));
        }
        if execution.snapshot.status != ExecutionStatus::WaitingForApproval {
            return Err(StoreError::ThreadBusy);
        }
    }
    Ok(())
}

/// Recompute cancellation from the exact current view under the backend lock or
/// transaction. Approval indexes must be checked before applying the plan.
pub fn validate_run_cancellation(
    view: &ThreadView,
    target: &RunCancellationTarget,
    owner_generation: u64,
    facts: TransitionFacts,
) -> Result<CommitPlan, StoreError> {
    let current = match RunCancellationTarget::from_view(view) {
        Err(StoreError::NoRunInFlight) => return Err(StoreError::RevisionConflict),
        result => result?,
    };
    if current != *target {
        return Err(StoreError::RevisionConflict);
    }
    let snapshot = &view
        .execution
        .as_ref()
        .expect("coherent active view")
        .snapshot;
    if owner_generation == 0 || snapshot.owner_generation != owner_generation {
        return Err(StoreError::OwnerConflict);
    }
    b4_kernel::validate_snapshot(snapshot)
        .map_err(|_| StoreError::Corrupt("run_cancellation_snapshot_invalid".into()))?;
    if !matches!(
        snapshot.status,
        ExecutionStatus::Accepted | ExecutionStatus::Running
    ) || snapshot.failure.is_some()
    {
        return Err(StoreError::NoRunInFlight);
    }
    b4_kernel::transition(
        snapshot,
        b4_contract::KernelCommand::Cancel {
            turn_id: target.turn_id.clone(),
            owner_generation,
        },
        facts,
    )
    .map_err(|error| match error {
        b4_kernel::KernelError::RevisionOverflow => {
            StoreError::Unavailable("run_cancellation_revision_exhausted".into())
        }
        _ => StoreError::InvalidCommit("run_cancellation_transition_invalid".into()),
    })
}

pub fn next_resource_revision(revision: u64) -> Result<u64, StoreError> {
    revision
        .checked_add(1)
        .ok_or_else(|| StoreError::Unavailable("resource revision exhausted".into()))
}

fn checked_cancellation_validator_mapper<F>(mapper: F) -> F {
    const { assert!(std::mem::size_of::<F>() <= 4_096) };
    mapper
}

fn validate_snapshot(snapshot: &Snapshot) -> Result<(), StoreError> {
    b4_kernel::validate_snapshot(snapshot).map_err(checked_cancellation_validator_mapper(
        |error: b4_kernel::KernelError| match error {
            b4_kernel::KernelError::InvalidSnapshot(message) => {
                StoreError::InvalidSnapshot(message)
            }
            error => StoreError::InvalidSnapshot(error.to_string()),
        },
    ))
}

/// Admission always starts with a portable Accepted snapshot under this owner.
pub fn validate_initial(snapshot: &Snapshot, owner: u64) -> Result<(), StoreError> {
    validate_snapshot(snapshot)?;
    if snapshot.status != ExecutionStatus::Accepted {
        return Err(StoreError::InvalidSnapshot(
            "initial_status_must_be_accepted".into(),
        ));
    }
    if snapshot.revision != 0 {
        return Err(StoreError::InvalidSnapshot(
            "initial_revision_must_be_zero".into(),
        ));
    }
    if owner == 0 || snapshot.owner_generation != owner {
        return Err(StoreError::OwnerConflict);
    }
    Ok(())
}

/// Return whether the plan changes persisted state. Recovery may compare an old
/// owner fence, but the replacement must belong to the store's current owner.
pub fn validate_commit(
    current: &Snapshot,
    fence: ExecutionFence,
    plan: &CommitPlan,
    owner: u64,
) -> Result<bool, StoreError> {
    validate_snapshot(current)?;
    validate_snapshot(&plan.snapshot)?;
    if fence.revision != current.revision || plan.expected_revision != current.revision {
        return Err(StoreError::RevisionConflict);
    }
    if fence.owner_generation != current.owner_generation
        || owner == 0
        || plan.snapshot.owner_generation != owner
        || plan.snapshot.owner_generation < current.owner_generation
    {
        return Err(StoreError::OwnerConflict);
    }
    let proposed = &plan.snapshot;
    if proposed.execution_id != current.execution_id {
        return Err(StoreError::InvalidCommit(
            "execution_identity_changed".into(),
        ));
    }
    if proposed.definition != current.definition {
        return Err(StoreError::InvalidCommit("definition_changed".into()));
    }
    if proposed.updated_at_ms < current.updated_at_ms {
        return Err(StoreError::InvalidCommit("clock_moved_backwards".into()));
    }
    if !proposed.turn_history.starts_with(&current.turn_history) {
        return Err(StoreError::InvalidCommit("turn_history_changed".into()));
    }
    if !proposed
        .attempt_history
        .starts_with(&current.attempt_history)
    {
        return Err(StoreError::InvalidCommit("attempt_history_changed".into()));
    }
    if proposed == current {
        return Ok(false);
    }
    if current.revision.checked_add(1) != Some(proposed.revision) {
        return Err(StoreError::InvalidCommit(
            "revision_must_increment_once".into(),
        ));
    }
    Ok(true)
}
pub fn releases_slot(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
    )
}

/// Generate an immutable incarnation from 256 bits of operating-system entropy.
pub fn generate_thread_generation() -> Result<ThreadGeneration, StoreError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| StoreError::Unavailable("random_source_unavailable".into()))?;
    ThreadGeneration::new(crate::approval::hex_bytes(&bytes))
        .map_err(|_| StoreError::Unavailable("invalid_random_generation".into()))
}
