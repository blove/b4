//! Borrowed MemoryStore discovery under its existing coherent source lock.

use super::backend::{
    measure_snapshot, project_snapshot, DiscoveryOrigin, PageBudget, SnapshotIndex, ThreadIndex,
};
use super::{DiscoveryError, UnfinishedDiscoveryPage, DISCOVERY_SCANNED_ROWS};
use crate::{StoredExecution, ThreadRecord};
use b4_contract::{ExecutionId, ThreadId};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};

pub(crate) fn discover(
    origin: &DiscoveryOrigin,
    current_owner: u64,
    threads: &BTreeMap<ThreadId, ThreadRecord>,
    executions: &BTreeMap<ExecutionId, StoredExecution>,
    after: Option<&ExecutionId>,
) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
    let bounds = (after.map_or(Unbounded, Excluded), Unbounded);
    let mut rows = executions.range(bounds);
    let mut records = Vec::with_capacity(DISCOVERY_SCANNED_ROWS);
    let mut budget = PageBudget::default();
    let mut scanned_rows = 0;
    let mut last_id = None;
    for (id, stored) in rows.by_ref().take(DISCOVERY_SCANNED_ROWS) {
        let snapshot = &stored.snapshot;
        budget.add_snapshot_bytes(measure_snapshot(snapshot)?)?;
        let thread = threads
            .get(&stored.thread_id)
            .ok_or(DiscoveryError::CorruptRecord)?;
        let active_relationship_valid = thread.active_execution_id.as_ref().is_none_or(|active| {
            executions
                .get(active)
                .is_some_and(|execution| execution.thread_id == thread.id)
        });
        let foreign_active_pointer = threads.iter().any(|(key, other)| {
            key != &stored.thread_id && other.active_execution_id.as_ref() == Some(id)
        });
        if let Some(record) = project_snapshot(
            snapshot,
            SnapshotIndex {
                execution_id: id,
                thread_id: &stored.thread_id,
                revision: snapshot.revision,
                owner_generation: snapshot.owner_generation,
            },
            ThreadIndex {
                id: &thread.id,
                generation: &thread.generation,
                active_execution_id: thread.active_execution_id.as_ref(),
                resource_revision: thread.resource_revision,
                active_relationship_valid,
                foreign_active_pointer,
            },
            current_owner,
        )? {
            records.push(record);
        }
        scanned_rows += 1;
        last_id = Some(id);
    }
    // The extra entry is identity-only: never touch its StoredExecution or
    // serialize/validate that snapshot. Typed keys are already byte-bounded.
    let next_cursor = match (rows.next().map(|(id, _)| id), last_id) {
        (Some(_), Some(last)) => Some(origin.cursor(current_owner, last)),
        _ => None,
    };
    Ok(UnfinishedDiscoveryPage {
        owner_generation: current_owner,
        scanned_rows,
        records,
        next_cursor,
    })
}
