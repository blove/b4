//! Fixed, bounded read-only discovery on the already owned connection.

use b4_contract::{ExecutionId, ThreadGeneration, ThreadId, MAX_ID_BYTES};
use b4_runtime::discovery::backend::{
    decode_snapshot, project_snapshot, PageBudget, SnapshotIndex, ThreadIndex,
};
use b4_runtime::{
    DiscoveryError, UnfinishedDiscoveryCursor, UnfinishedDiscoveryPage, UnfinishedExecutionRef,
    DISCOVERY_SCANNED_ROWS, DISCOVERY_SNAPSHOT_BYTES,
};
use rusqlite::{params, types::ValueRef, Connection, ErrorCode, Row};

const METADATA: &str = "SELECT id, schema_version, typeof(migration_checksum), length(CAST(migration_checksum AS BLOB)), CASE WHEN typeof(migration_checksum) = 'text' AND length(CAST(migration_checksum AS BLOB)) = 64 THEN migration_checksum END, typeof(owner_epoch), length(owner_epoch), CASE WHEN typeof(owner_epoch) = 'blob' AND length(owner_epoch) = 8 THEN owner_epoch END FROM metadata LIMIT 2";
const FIRST_IDENTITIES: &str = "SELECT typeof(id), length(CAST(id AS BLOB)), CASE WHEN typeof(id) = 'text' AND length(CAST(id AS BLOB)) <= 256 THEN id END FROM executions ORDER BY id COLLATE BINARY LIMIT 33";
const AFTER_IDENTITIES: &str = "SELECT typeof(id), length(CAST(id AS BLOB)), CASE WHEN typeof(id) = 'text' AND length(CAST(id AS BLOB)) <= 256 THEN id END FROM executions WHERE id COLLATE BINARY > ?1 COLLATE BINARY ORDER BY id COLLATE BINARY LIMIT 33";
const EXECUTION: &str = "SELECT typeof(thread_id), length(CAST(thread_id AS BLOB)), CASE WHEN typeof(thread_id) = 'text' AND length(CAST(thread_id AS BLOB)) <= 256 THEN thread_id END, typeof(revision), length(revision), CASE WHEN typeof(revision) = 'blob' AND length(revision) = 8 THEN revision END, typeof(owner_epoch), length(owner_epoch), CASE WHEN typeof(owner_epoch) = 'blob' AND length(owner_epoch) = 8 THEN owner_epoch END, typeof(snapshot), length(CAST(snapshot AS BLOB)), CASE WHEN typeof(snapshot) = 'text' AND length(CAST(snapshot AS BLOB)) <= 1048576 THEN snapshot END FROM executions WHERE id = ?1";
const THREAD: &str = "SELECT typeof(id), length(CAST(id AS BLOB)), CASE WHEN typeof(id) = 'text' AND length(CAST(id AS BLOB)) <= 256 THEN id END, typeof(generation), length(CAST(generation AS BLOB)), CASE WHEN typeof(generation) = 'text' AND length(CAST(generation AS BLOB)) = 64 THEN generation END, typeof(resource_revision), length(resource_revision), CASE WHEN typeof(resource_revision) = 'blob' AND length(resource_revision) = 8 THEN resource_revision END, typeof(active_execution_id), length(CAST(active_execution_id AS BLOB)), CASE WHEN typeof(active_execution_id) = 'text' AND length(CAST(active_execution_id AS BLOB)) <= 256 THEN active_execution_id END FROM threads WHERE id = ?1";
const ACTIVE_THREAD: &str = "SELECT typeof(thread_id), length(CAST(thread_id AS BLOB)), CASE WHEN typeof(thread_id) = 'text' AND length(CAST(thread_id AS BLOB)) <= 256 THEN thread_id END FROM executions WHERE id = ?1";
const FOREIGN_POINTER: &str =
    "SELECT EXISTS(SELECT 1 FROM threads WHERE active_execution_id = ?1 AND id != ?2)";

pub(super) fn read(
    store: &crate::SqliteStore,
    cursor: Option<&UnfinishedDiscoveryCursor>,
) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
    let after = store
        .discovery_origin
        .checked_after(store.owner_generation, cursor)?;
    let mut connection = store
        .connection
        .lock()
        .map_err(|_| DiscoveryError::Unavailable)?;
    let transaction = connection.transaction().map_err(sql_error)?;
    verify_metadata(&transaction, store.owner_generation)?;
    let identities = identities(&transaction, after)?;
    let scanned_rows = identities.len().min(DISCOVERY_SCANNED_ROWS);
    let mut records = Vec::with_capacity(scanned_rows);
    let mut budget = PageBudget::default();
    for id in identities.iter().take(DISCOVERY_SCANNED_ROWS) {
        if let Some(record) = read_execution(&transaction, id, store.owner_generation, &mut budget)?
        {
            records.push(record);
        }
    }
    let next_cursor = if identities.len() > DISCOVERY_SCANNED_ROWS {
        let last = identities
            .get(DISCOVERY_SCANNED_ROWS - 1)
            .ok_or(DiscoveryError::CorruptRecord)?;
        Some(store.discovery_origin.cursor(store.owner_generation, last))
    } else {
        None
    };
    // No statement, row or decoded Snapshot survives its helper scope.
    transaction.commit().map_err(sql_error)?;
    Ok(UnfinishedDiscoveryPage {
        owner_generation: store.owner_generation,
        scanned_rows,
        records,
        next_cursor,
    })
}

fn verify_metadata(connection: &Connection, owner: u64) -> Result<(), DiscoveryError> {
    let mut statement = connection.prepare(METADATA).map_err(sql_error)?;
    let mut rows = statement.query([]).map_err(sql_error)?;
    let row = rows
        .next()
        .map_err(sql_error)?
        .ok_or(DiscoveryError::UnsupportedSchema)?;
    if row.get_ref(0).map_err(sql_error)? != ValueRef::Integer(1)
        || row.get_ref(1).map_err(sql_error)? != ValueRef::Integer(crate::SCHEMA_VERSION)
    {
        return Err(DiscoveryError::UnsupportedSchema);
    }
    let stored_checksum = text(row, 2, 64, DiscoveryError::UnsupportedSchema)
        .map_err(|_| DiscoveryError::UnsupportedSchema)?;
    if stored_checksum != crate::checksum(crate::SCHEMA) {
        return Err(DiscoveryError::UnsupportedSchema);
    }
    let stored_owner = unsigned(row, 5)?;
    if stored_owner == 0 || stored_owner != owner {
        return Err(DiscoveryError::OwnerConflict);
    }
    if rows.next().map_err(sql_error)?.is_some() {
        return Err(DiscoveryError::UnsupportedSchema);
    }
    Ok(())
}

fn identities(
    connection: &Connection,
    after: Option<&ExecutionId>,
) -> Result<Vec<ExecutionId>, DiscoveryError> {
    let mut statement = connection
        .prepare(if after.is_some() {
            AFTER_IDENTITIES
        } else {
            FIRST_IDENTITIES
        })
        .map_err(sql_error)?;
    let mut rows = if let Some(id) = after {
        statement.query([id.as_str()])
    } else {
        statement.query([])
    }
    .map_err(sql_error)?;
    let mut identities = Vec::with_capacity(DISCOVERY_SCANNED_ROWS + 1);
    while let Some(row) = rows.next().map_err(sql_error)? {
        if identities.len() > DISCOVERY_SCANNED_ROWS {
            return Err(DiscoveryError::LimitExceeded);
        }
        let id = identity(row, 0)?;
        identities
            .push(ExecutionId::new(id.to_owned()).map_err(|_| DiscoveryError::CorruptRecord)?);
    }
    Ok(identities)
}

fn read_execution(
    connection: &Connection,
    id: &ExecutionId,
    owner: u64,
    budget: &mut PageBudget,
) -> Result<Option<UnfinishedExecutionRef>, DiscoveryError> {
    let mut statement = connection.prepare(EXECUTION).map_err(sql_error)?;
    let mut rows = statement.query([id.as_str()]).map_err(sql_error)?;
    let row = rows
        .next()
        .map_err(sql_error)?
        .ok_or(DiscoveryError::CorruptRecord)?;
    let thread_id =
        ThreadId::new(identity(row, 0)?.to_owned()).map_err(|_| DiscoveryError::CorruptRecord)?;
    let revision = unsigned(row, 3)?;
    let recorded_owner = unsigned(row, 6)?;
    let payload = bytes(
        row,
        9,
        DISCOVERY_SNAPSHOT_BYTES,
        DiscoveryError::LimitExceeded,
    )?;
    budget.add_snapshot_bytes(payload.len())?;
    let snapshot = decode_snapshot(payload)?;
    let thread = read_thread(connection, &thread_id)?;
    let active_relationship_valid = match &thread.active {
        Some(active) => active_belongs_to(connection, active, &thread.id)?,
        None => true,
    };
    let foreign_active_pointer = connection
        .query_row(
            FOREIGN_POINTER,
            params![id.as_str(), thread_id.as_str()],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sql_error)?;
    let record = project_snapshot(
        &snapshot,
        SnapshotIndex {
            execution_id: id,
            thread_id: &thread_id,
            revision,
            owner_generation: recorded_owner,
        },
        ThreadIndex {
            id: &thread.id,
            generation: &thread.generation,
            active_execution_id: thread.active.as_ref(),
            resource_revision: thread.resource_revision,
            active_relationship_valid,
            foreign_active_pointer,
        },
        owner,
    )?;
    // This row's complete authority payload dies before the next full row.
    drop(snapshot);
    if rows.next().map_err(sql_error)?.is_some() {
        return Err(DiscoveryError::CorruptRecord);
    }
    Ok(record)
}

struct ThreadFields {
    id: ThreadId,
    generation: ThreadGeneration,
    active: Option<ExecutionId>,
    resource_revision: u64,
}

fn read_thread(connection: &Connection, id: &ThreadId) -> Result<ThreadFields, DiscoveryError> {
    let mut statement = connection.prepare(THREAD).map_err(sql_error)?;
    let mut rows = statement.query([id.as_str()]).map_err(sql_error)?;
    let row = rows
        .next()
        .map_err(sql_error)?
        .ok_or(DiscoveryError::CorruptRecord)?;
    let id =
        ThreadId::new(identity(row, 0)?.to_owned()).map_err(|_| DiscoveryError::CorruptRecord)?;
    let generation =
        ThreadGeneration::new(text(row, 3, 64, DiscoveryError::CorruptRecord)?.to_owned())
            .map_err(|_| DiscoveryError::CorruptRecord)?;
    let resource_revision = unsigned(row, 6)?;
    let active = if row.get_ref(9).map_err(sql_error)? == ValueRef::Text(b"null") {
        if row.get_ref(10).map_err(sql_error)? != ValueRef::Null
            || row.get_ref(11).map_err(sql_error)? != ValueRef::Null
        {
            return Err(DiscoveryError::CorruptRecord);
        }
        None
    } else {
        Some(
            ExecutionId::new(identity(row, 9)?.to_owned())
                .map_err(|_| DiscoveryError::CorruptRecord)?,
        )
    };
    if rows.next().map_err(sql_error)?.is_some() {
        return Err(DiscoveryError::CorruptRecord);
    }
    Ok(ThreadFields {
        id,
        generation,
        active,
        resource_revision,
    })
}

fn active_belongs_to(
    connection: &Connection,
    active: &ExecutionId,
    thread: &ThreadId,
) -> Result<bool, DiscoveryError> {
    let mut statement = connection.prepare(ACTIVE_THREAD).map_err(sql_error)?;
    let mut rows = statement.query([active.as_str()]).map_err(sql_error)?;
    let Some(row) = rows.next().map_err(sql_error)? else {
        return Ok(false);
    };
    let matches = identity(row, 0)? == thread.as_str();
    if rows.next().map_err(sql_error)?.is_some() {
        return Err(DiscoveryError::CorruptRecord);
    }
    Ok(matches)
}

fn identity<'row>(row: &'row Row<'_>, offset: usize) -> Result<&'row str, DiscoveryError> {
    let id = text(row, offset, MAX_ID_BYTES, DiscoveryError::CorruptRecord)?;
    if id.is_empty() || id.chars().any(char::is_control) {
        return Err(DiscoveryError::CorruptRecord);
    }
    Ok(id)
}

fn text<'row>(
    row: &'row Row<'_>,
    offset: usize,
    maximum: usize,
    oversized: DiscoveryError,
) -> Result<&'row str, DiscoveryError> {
    std::str::from_utf8(bytes(row, offset, maximum, oversized)?)
        .map_err(|_| DiscoveryError::CorruptRecord)
}

fn bytes<'row>(
    row: &'row Row<'_>,
    offset: usize,
    maximum: usize,
    oversized: DiscoveryError,
) -> Result<&'row [u8], DiscoveryError> {
    if row.get_ref(offset).map_err(sql_error)? != ValueRef::Text(b"text") {
        return Err(DiscoveryError::CorruptRecord);
    }
    let length = length(row, offset + 1)?;
    if length > maximum {
        return Err(oversized);
    }
    match row.get_ref(offset + 2).map_err(sql_error)? {
        ValueRef::Text(bytes) if bytes.len() == length => Ok(bytes),
        _ => Err(DiscoveryError::CorruptRecord),
    }
}

fn unsigned(row: &Row<'_>, offset: usize) -> Result<u64, DiscoveryError> {
    if row.get_ref(offset).map_err(sql_error)? != ValueRef::Text(b"blob")
        || length(row, offset + 1)? != 8
    {
        return Err(DiscoveryError::CorruptRecord);
    }
    let ValueRef::Blob(bytes) = row.get_ref(offset + 2).map_err(sql_error)? else {
        return Err(DiscoveryError::CorruptRecord);
    };
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| DiscoveryError::CorruptRecord)?;
    Ok(u64::from_be_bytes(bytes))
}

fn length(row: &Row<'_>, index: usize) -> Result<usize, DiscoveryError> {
    match row.get_ref(index).map_err(sql_error)? {
        ValueRef::Integer(length) => {
            usize::try_from(length).map_err(|_| DiscoveryError::CorruptRecord)
        }
        _ => Err(DiscoveryError::CorruptRecord),
    }
}

fn sql_error(error: rusqlite::Error) -> DiscoveryError {
    match error.sqlite_error_code() {
        Some(ErrorCode::TooBig) => DiscoveryError::LimitExceeded,
        Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => DiscoveryError::CorruptRecord,
        Some(ErrorCode::Unknown | ErrorCode::SchemaChanged) => DiscoveryError::UnsupportedSchema,
        _ => DiscoveryError::Unavailable,
    }
}

#[cfg(test)]
mod tests;
