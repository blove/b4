//! Atomic management cancellation of a genuine NewUse Dispatch3.
use super::*;
use b4_runtime::permissions::{
    PermissionPreparedUseWithheldCancellation, PermissionUseWithheldCancellationCapture,
    PermissionUseWithheldCancellationCommit, PermissionUseWithheldCancellationSelector,
};
use rusqlite::{Connection, Transaction};

fn storage(_: rusqlite::Error) -> PermissionError {
    StoreError::Unavailable("permission_use_withheld_cancellation_storage".into()).into()
}
fn one(rows: usize) -> Result<(), PermissionError> {
    if rows != 1 {
        return Err(StoreError::RevisionConflict.into());
    }
    Ok(())
}
fn capture_in(
    store: &SqliteStore,
    tx: &Transaction<'_>,
    selector: &PermissionUseWithheldCancellationSelector<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionUseWithheldCancellationCapture, PermissionError> {
    let resource = {
        let id = selector.execution_id();
        let mut evidence = budget.resource_read()?;
        let (execution, thread) = capture_resource_in_transaction!(
            tx,
            evidence,
            id,
            store.owner_generation,
            before_sql,
            decode_execution,
            decode_thread,
            execution,
            thread,
            &execution.thread_id,
            (execution, thread)
        );
        evidence.check_linkage(id, &thread, &execution)?;
        ApprovalResource { thread, execution }
    };
    // NewUse never acquires authority from a second original acceptance.
    let accepted: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM permission_acceptances WHERE execution_id=?1)",
            [selector.execution_id().as_str()],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if accepted {
        return Err(PermissionError::UnsupportedProfile);
    }
    let session = selector.begin_read(&resource, budget)?;
    let evidence = use_dispatch::read(tx, session)?;
    selector.finish_read(resource, evidence, store.owner_generation)
}
pub(super) fn capture(
    store: &SqliteStore,
    selector: &PermissionUseWithheldCancellationSelector<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionUseWithheldCancellationCapture, PermissionError> {
    let mut connection = store.connection()?;
    connection.with_code_only_errors(|connection: &Connection| {
        let tx = Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
            .map_err(storage)?;
        let capture = capture_in(store, &tx, selector, budget)?;
        tx.commit().map_err(storage)?;
        Ok(capture)
    })
}
pub(super) fn commit(
    store: &SqliteStore,
    prepared: &PermissionPreparedUseWithheldCancellation<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionUseWithheldCancellationCommit, PermissionError> {
    let mut connection = store.connection()?;
    connection.with_code_only_errors(|connection: &Connection| {
        let tx = Transaction::new_unchecked(connection, TransactionBehavior::Immediate).map_err(storage)?;
        let selector = prepared.selector();
        let capture = capture_in(store, &tx, &selector, budget)?;
        let resource = prepared.resource();
        let current = &resource.execution.snapshot;
        let next = &prepared.plan().snapshot;
        let thread = &resource.thread;
        if !b4_runtime::validate_commit(current, current.into(), prepared.plan(), store.owner_generation)? {
            return Err(PermissionError::CorruptRecord);
        }
        if !b4_runtime::validate_commit(next, next.into(), prepared.terminal_plan(), store.owner_generation)? {
            return Err(PermissionError::CorruptRecord);
        }
        let mut revision = thread.resource_revision;
        for (before, after) in [(current, next), (next, &prepared.terminal_plan().snapshot)] {
            let previous_revision = revision;
            revision = revision.checked_add(1).ok_or(PermissionError::LimitExceeded)?;
            let snapshot = serde_json::to_string(after).map_err(|_|PermissionError::CorruptRecord)?;
            if snapshot.len() > 65_536 { return Err(PermissionError::UnsupportedProfile); }
            one(tx.execute(
                "UPDATE executions SET snapshot=?2,revision=?3 WHERE id=?1 AND revision=?4 AND owner_epoch=?5",
                params![before.execution_id.as_str(), snapshot, after.revision.to_be_bytes().as_slice(),
                    before.revision.to_be_bytes().as_slice(), store.owner_generation.to_be_bytes().as_slice()],
            ).map_err(storage)?)?;
            one(tx.execute(
                "UPDATE threads SET resource_revision=?2 WHERE id=?1 AND resource_revision=?3 AND generation=?4 AND active_execution_id=?5",
                params![thread.id.as_str(), revision.to_be_bytes().as_slice(), previous_revision.to_be_bytes().as_slice(),
                    thread.generation.as_str(), before.execution_id.as_str()],
            ).map_err(storage)?)?;
            crate::append_journal(&tx, "commit", &thread.id, after)?;
        }
        let commit = prepared.acknowledge(capture)?;
        tx.commit().map_err(storage)?;
        Ok(commit)
    })
}
