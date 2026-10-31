use super::SqliteStore;
use b4_contract::*;
use b4_runtime::approval::{ApprovalAcceptance, GrantRecord};
use b4_runtime::confinement::{audit, backend, shape};
use b4_runtime::{
    JournalEntry, RecoveryConfinementBackendError as Error, RecoveryConfinementOutcome,
    RecoveryConfinementReason, RecoveryConfinementTarget, StoredExecution, ThreadOrigin,
    ThreadRecord, ThreadView,
};
use rusqlite::{params, Connection, TransactionBehavior};
mod queries;
use queries::{self as q, Kind};

pub(super) fn capture(
    store: &SqliteStore,
    target: &RecoveryConfinementTarget,
) -> Result<ThreadView, Error> {
    let mut connection = store.connection().map_err(|_| Error::Unavailable)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(q::sql_error)?;
    let view = read(&transaction, target, store.owner_generation)?;
    transaction.commit().map_err(q::sql_error)?;
    Ok(view)
}

pub(super) fn mutate(
    store: &SqliteStore,
    original: &ThreadView,
    target: &RecoveryConfinementTarget,
    reason: RecoveryConfinementReason,
    fresh: &TurnId,
    facts: TransitionFacts,
) -> Result<RecoveryConfinementOutcome, Error> {
    let mut connection = store.connection().map_err(|_| Error::Unavailable)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(q::sql_error)?;
    let actual = read(&transaction, target, store.owner_generation)?;
    if &actual != original {
        return Err(Error::TargetChanged);
    }
    let current = &actual
        .execution
        .as_ref()
        .ok_or(Error::CorruptRecord)?
        .snapshot;
    if current.status == ExecutionStatus::RecoveryBlocked {
        transaction.commit().map_err(q::sql_error)?;
        return Ok(RecoveryConfinementOutcome::AlreadyBlocked);
    }
    let (snapshot, receipt) = backend::prepare_change(
        &actual,
        target,
        reason,
        fresh,
        store.owner_generation,
        facts,
    )?;
    let snapshot_bytes = shape::encode(&snapshot, backend::SNAPSHOT_BYTES)?;
    let journal_bytes = shape::encode(&JournalEntry::from(&snapshot), backend::JOURNAL_BYTES)?;
    let status_bytes = shape::encode(&snapshot.status, 32)?;
    let snapshot_text = std::str::from_utf8(&snapshot_bytes).map_err(|_| Error::Unavailable)?;
    let journal_text = std::str::from_utf8(&journal_bytes).map_err(|_| Error::Unavailable)?;
    let status_text = std::str::from_utf8(&status_bytes).map_err(|_| Error::Unavailable)?;
    // Every input and replacement guard precedes the first update. Both exact
    // fences are checked again by SQL under this same Immediate transaction.
    let changed = transaction.execute(
        "UPDATE executions SET revision=?1, owner_epoch=?2, snapshot=?3 WHERE id=?4 COLLATE BINARY AND revision=?5 AND owner_epoch=?6",
        params![snapshot.revision.to_be_bytes().as_slice(), snapshot.owner_generation.to_be_bytes().as_slice(), snapshot_text,
            target.execution_id.as_str(), target.execution_fence.revision.to_be_bytes().as_slice(), target.execution_fence.owner_generation.to_be_bytes().as_slice()],
    ).map_err(q::sql_error)?;
    if changed != 1 {
        return Err(Error::TargetChanged);
    }
    let changed = transaction.execute(
        "UPDATE threads SET resource_revision=?1 WHERE id=?2 COLLATE BINARY AND generation=?3 COLLATE BINARY AND resource_revision=?4 AND active_execution_id=?5 COLLATE BINARY",
        params![receipt.new_resource_revision.to_be_bytes().as_slice(), target.thread_id.as_str(), target.thread_generation.as_str(),
            target.resource_revision.to_be_bytes().as_slice(), target.execution_id.as_str()],
    ).map_err(q::sql_error)?;
    if changed != 1 {
        return Err(Error::TargetChanged);
    }
    let inserted = transaction.execute(
        "INSERT INTO journal(kind,thread_id,execution_id,revision,owner_epoch,status,updated_at_ms,payload) VALUES('commit',?1,?2,?3,?4,?5,?6,?7)",
        params![target.thread_id.as_str(), target.execution_id.as_str(), snapshot.revision.to_be_bytes().as_slice(),
            snapshot.owner_generation.to_be_bytes().as_slice(), status_text, snapshot.updated_at_ms.to_be_bytes().as_slice(), journal_text],
    ).map_err(q::sql_error)?;
    if inserted != 1 {
        return Err(Error::Unavailable);
    }
    transaction.commit().map_err(q::sql_error)?;
    Ok(RecoveryConfinementOutcome::Confined(receipt))
}

fn read(
    connection: &Connection,
    target: &RecoveryConfinementTarget,
    owner: u64,
) -> Result<ThreadView, Error> {
    let mut budget = backend::AuditBudget::default();
    metadata(connection, owner, &mut budget)?;
    let thread = thread(connection, target, &mut budget)?;
    let execution = execution(connection, &thread, target, &mut budget)?;
    // Keep the strict current-resource checks ahead of the partial authority
    // refusal; the complete ordinary audit below still runs for legacy rows.
    backend::validate_origin(thread.origin.as_ref())?;
    execution
        .snapshot
        .validate_version()
        .map_err(|_| Error::UnsupportedSnapshot)?;
    b4_kernel::validate_snapshot(&execution.snapshot).map_err(|_| Error::CorruptRecord)?;
    if target.execution_fence != (&execution.snapshot).into()
        || target.turn_id != execution.snapshot.turn_id
    {
        return Err(Error::TargetChanged);
    }
    crate::reject_legacy_permission_target(connection, &execution, &thread).map_err(|error| {
        match error {
            b4_runtime::StoreError::Corrupt(_) => Error::CorruptRecord,
            b4_runtime::StoreError::UnsupportedOperation => Error::UnsupportedAuthorityProfile,
            b4_runtime::StoreError::Unavailable(_) => Error::Unavailable,
            _ => Error::Unavailable,
        }
    })?;
    let grants = grants(connection, &thread, &execution, &mut budget)?;
    let acceptances = acceptances(connection, &thread, &execution, &mut budget)?;
    let creation = creation(connection, &thread, owner, &mut budget)?;
    let journal = journal(
        connection,
        &thread,
        &execution,
        &grants,
        &acceptances,
        creation,
        &mut budget,
    )?;
    let foreign: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM threads WHERE active_execution_id=?1 COLLATE BINARY AND id!=?2 COLLATE BINARY)",
        params![target.execution_id.as_str(),target.thread_id.as_str()], |row| row.get(0),
    ).map_err(q::sql_error)?;
    audit::resource(
        &thread,
        &execution,
        &grants,
        &acceptances,
        &journal,
        target,
        owner,
        foreign,
    )?;
    Ok(ThreadView {
        thread,
        execution: Some(execution),
    })
}

fn metadata(
    connection: &Connection,
    owner: u64,
    budget: &mut backend::AuditBudget,
) -> Result<(), Error> {
    let sql = q::select(
        "metadata",
        &[
            ("id", Kind::Integer),
            ("schema_version", Kind::Integer),
            ("migration_checksum", Kind::Text(64)),
            ("owner_epoch", Kind::Blob8),
        ],
        "LIMIT 2",
    );
    let mut statement = connection.prepare(&sql).map_err(q::sql_error)?;
    let mut rows = statement.query([]).map_err(q::sql_error)?;
    let row = rows
        .next()
        .map_err(q::sql_error)?
        .ok_or(Error::CorruptRecord)?;
    if q::integer(row, 0)? != 1 || q::integer(row, 1)? != crate::SCHEMA_VERSION {
        return Err(Error::CorruptRecord);
    }
    let checksum = q::indexed_text(row, 2, 64, budget)?;
    if checksum.len() != 64 || checksum != crate::checksum(crate::SCHEMA) {
        return Err(Error::CorruptRecord);
    }
    let recorded = q::unsigned(row, 3, budget)?;
    if owner == 0 || recorded != owner {
        return Err(Error::Unavailable);
    }
    if rows.next().map_err(q::sql_error)?.is_some() {
        return Err(Error::CorruptRecord);
    }
    Ok(())
}

fn thread(
    connection: &Connection,
    target: &RecoveryConfinementTarget,
    budget: &mut backend::AuditBudget,
) -> Result<ThreadRecord, Error> {
    let fields = [
        ("id", Kind::Text(256)),
        ("generation", Kind::Text(64)),
        ("active_execution_id", Kind::Text(256)),
        ("created_at_ms", Kind::Blob8),
        ("resource_revision", Kind::Blob8),
        ("metadata", Kind::Text(backend::THREAD_BYTES)),
        ("origin", Kind::Text(backend::THREAD_BYTES)),
    ];
    let sql = q::select("threads", &fields, "WHERE id=?1 COLLATE BINARY LIMIT 2");
    let mut statement = connection.prepare(&sql).map_err(q::sql_error)?;
    let mut rows = statement
        .query([target.thread_id.as_str()])
        .map_err(q::sql_error)?;
    let row = rows.next().map_err(q::sql_error)?.ok_or(Error::NotFound)?;
    let total = [
        q::text_len(row, 0, 256)?,
        q::text_len(row, 1, 64)?,
        q::optional_len(row, 2, 256)?,
        16,
        q::text_len(row, 5, backend::THREAD_BYTES)?,
        q::optional_len(row, 6, backend::THREAD_BYTES)?,
    ]
    .into_iter()
    .try_fold(0usize, |sum, n| {
        sum.checked_add(n).ok_or(Error::LimitExceeded)
    })?;
    if total > backend::THREAD_BYTES {
        return Err(Error::LimitExceeded);
    }
    budget.check_bytes(total)?;
    let id =
        ThreadId::new(q::indexed_text(row, 0, 256, budget)?).map_err(|_| Error::CorruptRecord)?;
    let generation = ThreadGeneration::new(q::indexed_text(row, 1, 64, budget)?)
        .map_err(|_| Error::CorruptRecord)?;
    let active = q::optional_text(row, 2, 256)?;
    budget.add_bytes(active.map_or(0, str::len))?;
    let active_execution_id = active
        .map(ExecutionId::new)
        .transpose()
        .map_err(|_| Error::CorruptRecord)?;
    let created_at_ms = q::unsigned(row, 3, budget)?;
    let resource_revision = q::unsigned(row, 4, budget)?;
    let metadata = backend::decode_metadata(q::payload(row, 5, backend::THREAD_BYTES, budget)?)?;
    let origin = if q::optional_text(row, 6, backend::THREAD_BYTES)?.is_some() {
        Some(backend::decode_record::<ThreadOrigin>(
            q::payload(row, 6, backend::THREAD_BYTES, budget)?,
            backend::THREAD_BYTES,
        )?)
    } else {
        None
    };
    if id != target.thread_id {
        return Err(Error::CorruptRecord);
    }
    if generation != target.thread_generation || resource_revision != target.resource_revision {
        return Err(Error::TargetChanged);
    }
    match &active_execution_id {
        None => return Err(Error::NotActive),
        Some(id) if id != &target.execution_id => return Err(Error::TargetChanged),
        _ => {}
    }
    if rows.next().map_err(q::sql_error)?.is_some() {
        return Err(Error::CorruptRecord);
    }
    Ok(ThreadRecord {
        id,
        generation,
        active_execution_id,
        created_at_ms,
        metadata,
        origin,
        resource_revision,
    })
}

fn execution(
    connection: &Connection,
    thread: &ThreadRecord,
    target: &RecoveryConfinementTarget,
    budget: &mut backend::AuditBudget,
) -> Result<StoredExecution, Error> {
    let sql = q::select(
        "executions",
        &[
            ("id", Kind::Text(256)),
            ("thread_id", Kind::Text(256)),
            ("revision", Kind::Blob8),
            ("owner_epoch", Kind::Blob8),
            ("snapshot", Kind::Text(backend::SNAPSHOT_BYTES)),
        ],
        "WHERE id=?1 COLLATE BINARY LIMIT 2",
    );
    let mut statement = connection.prepare(&sql).map_err(q::sql_error)?;
    let mut rows = statement
        .query([target.execution_id.as_str()])
        .map_err(q::sql_error)?;
    let row = rows
        .next()
        .map_err(q::sql_error)?
        .ok_or(Error::CorruptRecord)?;
    let id = q::indexed_text(row, 0, 256, budget)?;
    let thread_id =
        ThreadId::new(q::indexed_text(row, 1, 256, budget)?).map_err(|_| Error::CorruptRecord)?;
    let revision = q::unsigned(row, 2, budget)?;
    let recorded_owner = q::unsigned(row, 3, budget)?;
    let snapshot = backend::decode_snapshot(q::payload(row, 4, backend::SNAPSHOT_BYTES, budget)?)?;
    if snapshot.execution_id.as_str() != id
        || id != target.execution_id.as_str()
        || thread_id != thread.id
        || snapshot.revision != revision
        || snapshot.owner_generation != recorded_owner
    {
        return Err(Error::CorruptRecord);
    }
    if rows.next().map_err(q::sql_error)?.is_some() {
        return Err(Error::CorruptRecord);
    }
    Ok(StoredExecution {
        thread_id,
        snapshot,
    })
}

fn grants(
    connection: &Connection,
    thread: &ThreadRecord,
    execution: &StoredExecution,
    budget: &mut backend::AuditBudget,
) -> Result<Vec<GrantRecord>, Error> {
    count(
        connection,
        "approval_grants",
        &execution.snapshot.execution_id,
        backend::AUTHORITY_ROWS,
    )?;
    let sql = q::select(
        "approval_grants",
        &[
            ("execution_id", Kind::Text(256)),
            ("interrupt_id", Kind::Text(256)),
            ("task_id", Kind::Text(256)),
            ("thread_id", Kind::Text(256)),
            ("thread_generation", Kind::Text(64)),
            ("task_parking_revision", Kind::Blob8),
            ("consumed", Kind::Integer),
            ("record", Kind::Text(backend::AUTHORITY_BYTES)),
        ],
        "WHERE execution_id=?1 COLLATE BINARY ORDER BY interrupt_id COLLATE BINARY LIMIT 64",
    );
    let mut statement = connection.prepare(&sql).map_err(q::sql_error)?;
    let mut rows = statement
        .query([execution.snapshot.execution_id.as_str()])
        .map_err(q::sql_error)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(q::sql_error)? {
        if records.len() == backend::AUTHORITY_ROWS {
            return Err(Error::LimitExceeded);
        }
        let id = q::indexed_text(row, 0, 256, budget)?;
        let interrupt = q::indexed_text(row, 1, 256, budget)?;
        let task = q::indexed_text(row, 2, 256, budget)?;
        let thread_id = q::indexed_text(row, 3, 256, budget)?;
        let generation = q::indexed_text(row, 4, 64, budget)?;
        let parking = q::unsigned(row, 5, budget)?;
        let consumed = q::integer(row, 6)?;
        let record: GrantRecord = backend::decode_record(
            q::payload(row, 7, backend::AUTHORITY_BYTES, budget)?,
            backend::AUTHORITY_BYTES,
        )?;
        if record.execution_id.as_str() != id
            || record.execution_id != execution.snapshot.execution_id
            || record.interrupt_id.as_str() != interrupt
            || record.task_id.as_str() != task
            || record.thread_id.as_str() != thread_id
            || record.thread_id != thread.id
            || record.thread_generation.as_str() != generation
            || record.thread_generation != thread.generation
            || record.task_parking_revision != parking
            || i64::from(record.consumed) != consumed
        {
            return Err(Error::CorruptRecord);
        }
        records.try_reserve(1).map_err(|_| Error::Unavailable)?;
        records.push(record);
    }
    Ok(records)
}

fn acceptances(
    connection: &Connection,
    thread: &ThreadRecord,
    execution: &StoredExecution,
    budget: &mut backend::AuditBudget,
) -> Result<Vec<ApprovalAcceptance>, Error> {
    count(
        connection,
        "approval_acceptances",
        &execution.snapshot.execution_id,
        backend::AUTHORITY_ROWS,
    )?;
    let sql = q::select("approval_acceptances", &[("execution_id",Kind::Text(256)),("turn_id",Kind::Text(256)),
        ("thread_id",Kind::Text(256)),("thread_generation",Kind::Text(64)),("previous_revision",Kind::Blob8),
        ("definition_revision",Kind::Text(256)),("accepted_at_ms",Kind::Blob8),("record",Kind::Text(backend::AUTHORITY_BYTES))],
        "WHERE execution_id=?1 COLLATE BINARY ORDER BY previous_revision,turn_id COLLATE BINARY LIMIT 64");
    let mut statement = connection.prepare(&sql).map_err(q::sql_error)?;
    let mut rows = statement
        .query([execution.snapshot.execution_id.as_str()])
        .map_err(q::sql_error)?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(q::sql_error)? {
        if records.len() == backend::AUTHORITY_ROWS {
            return Err(Error::LimitExceeded);
        }
        let id = q::indexed_text(row, 0, 256, budget)?;
        let turn = q::indexed_text(row, 1, 256, budget)?;
        let thread_id = q::indexed_text(row, 2, 256, budget)?;
        let generation = q::indexed_text(row, 3, 64, budget)?;
        let previous = q::unsigned(row, 4, budget)?;
        let revision = q::indexed_text(row, 5, 256, budget)?;
        let accepted = q::unsigned(row, 6, budget)?;
        let record: ApprovalAcceptance = backend::decode_record(
            q::payload(row, 7, backend::AUTHORITY_BYTES, budget)?,
            backend::AUTHORITY_BYTES,
        )?;
        if record.execution_id.as_str() != id
            || record.execution_id != execution.snapshot.execution_id
            || record.turn_id.as_str() != turn
            || record.thread_id.as_str() != thread_id
            || record.thread_id != thread.id
            || record.thread_generation.as_str() != generation
            || record.thread_generation != thread.generation
            || record.previous_revision != previous
            || record.definition_revision.as_str() != revision
            || record.accepted_at_ms != accepted
        {
            return Err(Error::CorruptRecord);
        }
        records.try_reserve(1).map_err(|_| Error::Unavailable)?;
        records.push(record);
    }
    Ok(records)
}

fn creation(
    connection: &Connection,
    thread: &ThreadRecord,
    owner: u64,
    budget: &mut backend::AuditBudget,
) -> Result<(i64, u64), Error> {
    let sql = q::select(
        "journal",
        &[
            ("sequence", Kind::Integer),
            ("kind", Kind::Text(6)),
            ("thread_id", Kind::Text(256)),
            ("execution_id", Kind::Null),
            ("revision", Kind::Null),
            ("owner_epoch", Kind::Blob8),
            ("status", Kind::Null),
            ("updated_at_ms", Kind::Blob8),
            ("payload", Kind::Null),
        ],
        "WHERE thread_id=?1 COLLATE BINARY AND kind='thread' LIMIT 2",
    );
    let mut statement = connection.prepare(&sql).map_err(q::sql_error)?;
    let mut rows = statement
        .query([thread.id.as_str()])
        .map_err(q::sql_error)?;
    let row = rows
        .next()
        .map_err(q::sql_error)?
        .ok_or(Error::CorruptRecord)?;
    let sequence = q::integer(row, 0)?;
    let kind = q::indexed_text(row, 1, 6, budget)?;
    let thread_id = q::indexed_text(row, 2, 256, budget)?;
    for i in [3, 4, 6, 8] {
        q::null(row, i)?;
    }
    let recorded_owner = q::unsigned(row, 5, budget)?;
    let updated = q::unsigned(row, 7, budget)?;
    if sequence <= 0
        || kind != "thread"
        || thread_id != thread.id.as_str()
        || recorded_owner == 0
        || recorded_owner > owner
        || updated != thread.created_at_ms
    {
        return Err(Error::CorruptRecord);
    }
    if rows.next().map_err(q::sql_error)?.is_some() {
        return Err(Error::CorruptRecord);
    }
    Ok((sequence, recorded_owner))
}

fn journal(
    connection: &Connection,
    thread: &ThreadRecord,
    execution: &StoredExecution,
    grants: &[GrantRecord],
    acceptances: &[ApprovalAcceptance],
    creation: (i64, u64),
    budget: &mut backend::AuditBudget,
) -> Result<backend::JournalEvidence, Error> {
    let maximum = backend::JOURNAL_ROWS
        - usize::from(execution.snapshot.status != ExecutionStatus::RecoveryBlocked);
    count(
        connection,
        "journal",
        &execution.snapshot.execution_id,
        maximum,
    )?;
    let sql = q::select(
        "journal",
        &[
            ("sequence", Kind::Integer),
            ("kind", Kind::Text(6)),
            ("thread_id", Kind::Text(256)),
            ("execution_id", Kind::Text(256)),
            ("revision", Kind::Blob8),
            ("owner_epoch", Kind::Blob8),
            ("status", Kind::Text(32)),
            ("updated_at_ms", Kind::Blob8),
            ("payload", Kind::Text(backend::JOURNAL_BYTES)),
        ],
        "WHERE execution_id=?1 COLLATE BINARY ORDER BY sequence LIMIT 4096",
    );
    let mut statement = connection.prepare(&sql).map_err(q::sql_error)?;
    let mut rows = statement
        .query([execution.snapshot.execution_id.as_str()])
        .map_err(q::sql_error)?;
    let mut chain = backend::JournalAudit::new(&execution.snapshot, grants, acceptances)?;
    let mut previous_sequence = creation.0;
    let mut count = 0usize;
    while let Some(row) = rows.next().map_err(q::sql_error)? {
        if count == maximum {
            return Err(Error::LimitExceeded);
        }
        let sequence = q::integer(row, 0)?;
        let kind = q::indexed_text(row, 1, 6, budget)?;
        let thread_id = q::indexed_text(row, 2, 256, budget)?;
        let id = q::indexed_text(row, 3, 256, budget)?;
        let revision = q::unsigned(row, 4, budget)?;
        let recorded_owner = q::unsigned(row, 5, budget)?;
        let status: ExecutionStatus = backend::decode_record(q::payload(row, 6, 32, budget)?, 32)?;
        let updated = q::unsigned(row, 7, budget)?;
        let entry: JournalEntry = backend::decode_record(
            q::payload(row, 8, backend::JOURNAL_BYTES, budget)?,
            backend::JOURNAL_BYTES,
        )?;
        if sequence <= previous_sequence
            || kind != if count == 0 { "admit" } else { "commit" }
            || thread_id != thread.id.as_str()
            || id != execution.snapshot.execution_id.as_str()
            || entry.execution_id.as_str() != id
            || entry.revision != revision
            || entry.owner_generation != recorded_owner
            || entry.status != status
            || entry.updated_at_ms != updated
        {
            return Err(Error::CorruptRecord);
        }
        previous_sequence = sequence;
        count += 1;
        chain.observe(entry)?;
    }
    let evidence = chain.finish()?;
    if creation.1 > evidence.first_owner() {
        return Err(Error::CorruptRecord);
    }
    Ok(evidence)
}

/// This fixed scalar lookahead never selects a payload from the overflow row.
/// Table names and maxima are internal constants, never caller input.
fn count(
    connection: &Connection,
    table: &str,
    execution: &ExecutionId,
    maximum: usize,
) -> Result<(), Error> {
    let sql = format!("SELECT COUNT(*) FROM (SELECT 1 FROM {table} WHERE execution_id=?1 COLLATE BINARY LIMIT {})", maximum + 1);
    let count: i64 = connection
        .query_row(&sql, [execution.as_str()], |row| row.get(0))
        .map_err(q::sql_error)?;
    if usize::try_from(count).map_err(|_| Error::CorruptRecord)? > maximum {
        return Err(Error::LimitExceeded);
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod rollback_tests;
