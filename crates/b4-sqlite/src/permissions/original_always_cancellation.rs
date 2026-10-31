//! Atomic management request against the actual original human acceptance.
use super::*;
use b4_runtime::permissions::{
    PermissionOriginalAlwaysCancellationCapture, PermissionOriginalAlwaysCancellationCommit,
    PermissionOriginalAlwaysCancellationSelector, PermissionPreparedOriginalAlwaysCancellation,
};
use b4_runtime::JournalEntry;
use rusqlite::{Connection, Transaction};
fn storage(_: rusqlite::Error) -> PermissionError {
    StoreError::Unavailable("permission_original_always_cancellation_storage".into()).into()
}
fn one(rows: usize) -> Result<(), PermissionError> {
    if rows == 1 {
        Ok(())
    } else {
        Err(StoreError::RevisionConflict.into())
    }
}
fn capture_in(
    store: &SqliteStore,
    tx: &Transaction<'_>,
    selector: &PermissionOriginalAlwaysCancellationSelector<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionOriginalAlwaysCancellationCapture, PermissionError> {
    selector.before_resource(budget)?;
    let id = selector.execution_id().as_str();
    let thread = selector.thread_id().as_str();
    // Bound allocations before the ordinary strict resource decoders run.
    let valid:bool=tx.query_row("SELECT typeof(snapshot)='text' AND length(CAST(snapshot AS BLOB)) BETWEEN 1 AND 65536 AND length(CAST(thread_id AS BLOB)) BETWEEN 1 AND 256 FROM executions WHERE id=?1",[id],|r|r.get(0)).optional().map_err(storage)?.ok_or(StoreError::ExecutionNotFound)?;
    if !valid {
        return Err(PermissionError::UnsupportedProfile);
    }
    let valid:bool=tx.query_row("SELECT typeof(metadata)='text' AND length(CAST(metadata AS BLOB)) BETWEEN 1 AND 1024 AND typeof(origin)='text' AND length(CAST(origin AS BLOB)) BETWEEN 1 AND 1024 AND active_execution_id=?2 AND length(CAST(generation AS BLOB))=64 FROM threads WHERE id=?1",params![thread,id],|r|r.get(0)).optional().map_err(storage)?.ok_or(StoreError::ThreadNotFound)?;
    if !valid {
        return Err(PermissionError::UnsupportedProfile);
    }
    let execution =
        crate::read_execution(tx, selector.execution_id())?.ok_or(StoreError::ExecutionNotFound)?;
    let thread_record =
        crate::read_thread(tx, selector.thread_id())?.ok_or(StoreError::ThreadNotFound)?;
    let owner = tx
        .query_row(
            "SELECT owner_epoch FROM metadata WHERE id=1",
            [],
            |r| match r.get_ref(0)? {
                ValueRef::Blob(v) if v.len() == 8 => Ok(u64::from_be_bytes(
                    v.try_into().expect("checked owner width"),
                )),
                _ => Err(rusqlite::Error::InvalidQuery),
            },
        )
        .map_err(storage)?;
    if owner != store.owner_generation {
        return Err(StoreError::OwnerConflict.into());
    }
    let mut read = selector.begin(
        ApprovalResource {
            thread: thread_record,
            execution,
        },
        budget,
    )?;
    for (source,sql,key,field_count) in [
        (0,"SELECT execution_id,turn_id,thread_id,thread_generation,previous_revision,committed_revision,record_version,CASE WHEN typeof(record)='text' AND length(CAST(record AS BLOB)) BETWEEN 1 AND 8192 THEN record END FROM permission_acceptances WHERE execution_id=?1 ORDER BY previous_revision,turn_id LIMIT 2",id,6),
        (1,"SELECT permission_id,thread_id,thread_generation,scope_digest,record_version,CASE WHEN typeof(record)='text' AND length(CAST(record AS BLOB)) BETWEEN 1 AND 4096 THEN record END FROM permission_decisions WHERE thread_id=?1 ORDER BY permission_id LIMIT 2",thread,4),
        (2,"SELECT execution_id,interrupt_id,task_id,thread_id,thread_generation,task_parking_revision,consumed,CASE WHEN typeof(record)='text' AND length(CAST(record AS BLOB)) BETWEEN 1 AND 4096 THEN record END FROM approval_grants WHERE execution_id=?1 ORDER BY interrupt_id LIMIT 2",id,6),
    ] {
        let mut stmt=tx.prepare(sql).map_err(storage)?; let mut rows=stmt.query([key]).map_err(storage)?;
        let row=rows.next().map_err(storage)?.ok_or(PermissionError::CorruptRecord)?;
        let mut fields:Vec<&[u8]>=Vec::with_capacity(field_count);
        for index in 0..field_count { let bytes=match row.get_ref(index).map_err(storage)? {ValueRef::Text(v)|ValueRef::Blob(v) if v.len()<=256=>v,_=>return Err(PermissionError::CorruptRecord)};fields.push(bytes); }
        let scalar=row.get(field_count).map_err(storage)?;
        let raw=match row.get_ref(field_count+1).map_err(storage)? {ValueRef::Text(v)=>v,_=>return Err(PermissionError::CorruptRecord)};
        read.record(source,&fields,scalar,raw,budget)?;
        if rows.next().map_err(storage)?.is_some() {return Err(PermissionError::UnsupportedProfile)}
    }
    let uses: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM permission_uses WHERE execution_id=?1)",
            [id],
            |r| r.get(0),
        )
        .map_err(storage)?;
    let legacy: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM approval_acceptances WHERE execution_id=?1)",
            [id],
            |r| r.get(0),
        )
        .map_err(storage)?;
    read.absence(uses, legacy)?;
    let mut stmt=tx.prepare("SELECT kind,thread_id,execution_id,revision,owner_epoch,status,updated_at_ms,CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB)) BETWEEN 1 AND 1024 THEN payload END FROM journal WHERE execution_id=?1 ORDER BY sequence LIMIT 6").map_err(storage)?;
    let mut rows = stmt.query([id]).map_err(storage)?;
    let mut index = 0;
    while let Some(row) = rows.next().map_err(storage)? {
        if index >= 5 {
            return Err(PermissionError::CorruptRecord);
        }
        let raw = match row.get_ref(7).map_err(storage)? {
            ValueRef::Text(v) => v,
            _ => return Err(PermissionError::CorruptRecord),
        };
        let entry: JournalEntry =
            serde_json::from_slice(raw).map_err(|_| PermissionError::CorruptRecord)?;
        let expected_kind = if index == 0 {
            b"admit".as_slice()
        } else {
            b"commit".as_slice()
        };
        let status =
            serde_json::to_string(&entry.status).map_err(|_| PermissionError::CorruptRecord)?;
        for (column, expected) in [
            (0, expected_kind),
            (1, thread.as_bytes()),
            (2, id.as_bytes()),
            (5, status.as_bytes()),
        ] {
            if !matches!(row.get_ref(column).map_err(storage)?,ValueRef::Text(v) if v==expected) {
                return Err(PermissionError::CorruptRecord);
            }
        }
        for (column, expected) in [
            (3, entry.revision),
            (4, entry.owner_generation),
            (6, entry.updated_at_ms),
        ] {
            if !matches!(row.get_ref(column).map_err(storage)?,ValueRef::Blob(v) if v==expected.to_be_bytes())
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
        read.journal(entry, raw, budget)?;
        index += 1;
    }
    read.finish(store.owner_generation, budget)
}
pub(super) fn capture(
    store: &SqliteStore,
    selector: &PermissionOriginalAlwaysCancellationSelector<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionOriginalAlwaysCancellationCapture, PermissionError> {
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
    prepared: &PermissionPreparedOriginalAlwaysCancellation<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionOriginalAlwaysCancellationCommit, PermissionError> {
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
        let revision = thread.resource_revision.checked_add(1).ok_or(PermissionError::LimitExceeded)?;
        let snapshot = serde_json::to_string(next).map_err(|_|PermissionError::CorruptRecord)?;
        if snapshot.len() > 65_536 { return Err(PermissionError::UnsupportedProfile); }
        one(tx.execute(
            "UPDATE executions SET snapshot=?2,revision=?3 WHERE id=?1 AND revision=?4 AND owner_epoch=?5",
            params![current.execution_id.as_str(), snapshot, next.revision.to_be_bytes().as_slice(),
                current.revision.to_be_bytes().as_slice(), store.owner_generation.to_be_bytes().as_slice()],
        ).map_err(storage)?)?;
        one(tx.execute(
            "UPDATE threads SET resource_revision=?2 WHERE id=?1 AND resource_revision=?3 AND generation=?4 AND active_execution_id=?5",
            params![thread.id.as_str(), revision.to_be_bytes().as_slice(), thread.resource_revision.to_be_bytes().as_slice(),
                thread.generation.as_str(), current.execution_id.as_str()],
        ).map_err(storage)?)?;
        crate::append_journal(&tx, "commit", &thread.id, next)?;
        let commit = prepared.acknowledge(capture)?;
        tx.commit().map_err(storage)?;
        Ok(commit)
    })
}
