//! Fixed-policy helpers for Store implementors; none authorizes recovery.

use super::shape::JsonShapeGuard;
use super::{
    DiscoveryError, DiscoveryOwnerRelation, UnfinishedDiscoveryCursor, UnfinishedExecutionRef,
    DISCOVERY_TOTAL_SNAPSHOT_BYTES,
};
use b4_contract::{
    ExecutionId, ExecutionStatus, Snapshot, TaskStatus, ThreadGeneration, ThreadId,
    SNAPSHOT_VERSION,
};
use serde::Deserialize;
use std::sync::Arc;

/// A unique nonclone marker that owns no backend or execution resource.
#[derive(Default)]
pub struct DiscoveryOrigin {
    marker: Arc<()>,
}

impl DiscoveryOrigin {
    /// Check source and owned generation before borrowing the cursor's last ID.
    pub fn checked_after<'cursor>(
        &self,
        current_owner: u64,
        cursor: Option<&'cursor UnfinishedDiscoveryCursor>,
    ) -> Result<Option<&'cursor ExecutionId>, DiscoveryError> {
        if current_owner == 0 {
            return Err(DiscoveryError::OwnerConflict);
        }
        match cursor {
            Some(cursor)
                if cursor.owner_generation != current_owner
                    || !Arc::ptr_eq(&self.marker, &cursor.marker) =>
            {
                Err(DiscoveryError::CursorMismatch)
            }
            Some(cursor) => Ok(Some(&cursor.after)),
            None => Ok(None),
        }
    }

    /// Capture only the process-local marker, owned generation and bounded ID.
    pub fn cursor(&self, current_owner: u64, last_id: &ExecutionId) -> UnfinishedDiscoveryCursor {
        UnfinishedDiscoveryCursor {
            marker: Arc::clone(&self.marker),
            owner_generation: current_owner,
            after: last_id.clone(),
        }
    }
}

/// Decode within fixed byte/shape limits before kernel and source validation.
pub fn decode_snapshot(payload: &[u8]) -> Result<Snapshot, DiscoveryError> {
    let mut guard = JsonShapeGuard::new();
    guard.consume(payload)?;
    guard.finish()?;
    // Derived map decoding rejects a missing/duplicate/non-u64 version; unknown
    // values are completely traversed without materializing JSON child trees.
    #[derive(Deserialize)]
    struct VersionProbe {
        schema_version: u64,
    }
    let probe: VersionProbe =
        serde_json::from_slice(payload).map_err(|_| DiscoveryError::CorruptRecord)?;
    if probe.schema_version != u64::from(SNAPSHOT_VERSION) {
        return Err(DiscoveryError::UnsupportedSnapshot);
    }
    serde_json::from_slice(payload).map_err(|_| DiscoveryError::CorruptRecord)
}

/// Borrowed indexed identity, independent of decoded snapshot contents.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotIndex<'a> {
    pub execution_id: &'a ExecutionId,
    pub thread_id: &'a ThreadId,
    pub revision: u64,
    pub owner_generation: u64,
}

/// Borrowed thread identity and source-checked pointer relationships.
#[derive(Debug, Clone, Copy)]
pub struct ThreadIndex<'a> {
    pub id: &'a ThreadId,
    pub generation: &'a ThreadGeneration,
    pub active_execution_id: Option<&'a ExecutionId>,
    pub resource_revision: u64,
    pub active_relationship_valid: bool,
    pub foreign_active_pointer: bool,
}

/// Account compact serialized bytes without cloning the borrowed snapshot.
pub fn measure_snapshot(snapshot: &Snapshot) -> Result<usize, DiscoveryError> {
    let mut guard = JsonShapeGuard::new();
    if serde_json::to_writer(&mut guard, snapshot).is_err() {
        return Err(if guard.own_limit_refusal() {
            DiscoveryError::LimitExceeded
        } else {
            DiscoveryError::CorruptRecord
        });
    }
    Ok(guard.finish()?.bytes)
}

/// Validate before filtering terminal history; this never authorizes recovery.
///
/// The backend must first bound this snapshot: Memory uses `measure_snapshot`
/// on its borrowed source, while SQLite uses `decode_snapshot` on persisted raw
/// bytes. Re-encoding decoded SQLite floats could expand their spelling and
/// incorrectly impose an additional canonical byte cap. No payload clone or
/// serialization occurs here before the unchanged kernel validation.
pub fn project_snapshot(
    snapshot: &Snapshot,
    index: SnapshotIndex<'_>,
    thread: ThreadIndex<'_>,
    current_owner: u64,
) -> Result<Option<UnfinishedExecutionRef>, DiscoveryError> {
    if current_owner == 0 {
        return Err(DiscoveryError::OwnerConflict);
    }
    snapshot
        .validate_version()
        .map_err(|_| DiscoveryError::UnsupportedSnapshot)?;
    b4_kernel::validate_snapshot(snapshot).map_err(|_| DiscoveryError::CorruptRecord)?;
    if snapshot.execution_id != *index.execution_id
        || snapshot.revision != index.revision
        || snapshot.owner_generation != index.owner_generation
        || thread.id != index.thread_id
        || !thread.active_relationship_valid
        || thread.foreign_active_pointer
    {
        return Err(DiscoveryError::CorruptRecord);
    }
    if snapshot.owner_generation > current_owner {
        return Err(DiscoveryError::OwnerConflict);
    }
    let has_in_flight_task = snapshot
        .tasks
        .iter()
        .any(|task| task.status == TaskStatus::InFlight);
    let has_outcome_unknown_task = snapshot
        .tasks
        .iter()
        .any(|task| task.status == TaskStatus::OutcomeUnknown);
    let selected = !matches!(
        snapshot.status,
        ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
    ) || has_in_flight_task
        || has_outcome_unknown_task;
    if !selected {
        return Ok(None);
    }
    if thread.active_execution_id != Some(index.execution_id) {
        return Err(DiscoveryError::CorruptRecord);
    }
    Ok(Some(UnfinishedExecutionRef {
        thread_id: thread.id.clone(),
        thread_generation: thread.generation.clone(),
        execution_id: snapshot.execution_id.clone(),
        execution_fence: snapshot.into(),
        resource_revision: thread.resource_revision,
        route: snapshot.definition.route.clone(),
        definition_revision: snapshot.definition.revision.clone(),
        status: snapshot.status,
        owner_relation: if snapshot.owner_generation == current_owner {
            DiscoveryOwnerRelation::Current
        } else {
            DiscoveryOwnerRelation::Prior
        },
        has_in_flight_task,
        has_outcome_unknown_task,
    }))
}

/// Fixed aggregate accounting for at most32 selected rows of at most1MiB each.
#[derive(Default)]
pub struct PageBudget {
    pub(super) total: usize,
}

impl PageBudget {
    pub fn add_snapshot_bytes(&mut self, bytes: usize) -> Result<(), DiscoveryError> {
        self.add_with_limit(bytes, DISCOVERY_TOTAL_SNAPSHOT_BYTES)
    }

    pub(super) fn add_with_limit(
        &mut self,
        bytes: usize,
        limit: usize,
    ) -> Result<(), DiscoveryError> {
        let total = self
            .total
            .checked_add(bytes)
            .filter(|total| *total <= limit)
            .ok_or(DiscoveryError::LimitExceeded)?;
        self.total = total;
        Ok(())
    }
}
