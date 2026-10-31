//! The narrow typed Ready Use writer. Historical authority is validated by Runtime.
use super::permission_native_write_invalid as native;
use super::*;
use b4_runtime::permissions::{
    PermissionReadyAuthorizationIntent, PermissionReadyAuthorizationReadPhase,
    PermissionReadyAuthorizationSource as Source, PermissionReadyAuthorizationWriteSql as Write,
    PermissionReadyAuthorizationWriterSession,
};

const DECISION_HEADERS: &str = "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_decisions WHERE thread_id = ?1 ORDER BY permission_id LIMIT 2";
const ACCEPTANCE_HEADERS: &str = "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_acceptances WHERE execution_id = ?1 ORDER BY previous_revision, turn_id LIMIT 2";
const GRANT_HEADERS: &str = "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, typeof(record), length(CAST(record AS BLOB)) FROM approval_grants WHERE execution_id = ?1 ORDER BY interrupt_id LIMIT 2";
const JOURNAL_HEADERS: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 7";
const ORIGINAL_CANCELLED_JOURNAL_HEADERS: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 8";
const LEGACY_ACCEPTANCE: &str = "SELECT execution_id, turn_id FROM approval_acceptances WHERE execution_id = ?1 ORDER BY turn_id LIMIT 1";
const CURRENT_USE: &str = "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_uses WHERE execution_id = ?1 ORDER BY task_id LIMIT 2";
const GLOBAL_USE_ID: &str =
    "SELECT execution_id, task_id, use_id FROM permission_uses WHERE use_id = ?1 LIMIT 1";
const INSERT_USE: &str = "INSERT INTO permission_uses (execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9)";

// Each borrowed row is consumed synchronously. The Runtime session alone can
// retain paid owned witnesses, classify a genuine miss or create matched Work.
fn read_sources(
    transaction: &rusqlite::Transaction<'_>,
    session: &mut PermissionReadyAuthorizationWriterSession<'_, '_>,
) -> Result<(), PermissionError> {
    session.before_source_sql(Source::Decision)?;
    let mut found = false;
    {
        let mut statement = transaction
            .prepare(DECISION_HEADERS)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = statement
            .query((session.thread_id().as_str(),))
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        loop {
            session.before_row(Source::Decision)?;
            let Some(row) = rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            else {
                break;
            };
            session.admit_new_header(PermissionNewHeaderCells {
                group: PermissionNewGroup::Decisions,
                execution_id: None,
                primary: bounded_text(row, 0, 256)?,
                thread_id: bounded_text(row, 1, 256)?,
                generation: text(row, 2, 64)?,
                secondary: None,
                permission_id: None,
                turn_id: None,
                digest: Some(text(row, 3, 64)?),
                previous_revision: None,
                committed_revision: None,
                record_version: integer(row, 4)?,
                record_bytes: record_length(row, 5, 6)?,
            })?;
            found = true;
            session.before_source_sql(Source::Decision)?;
            let bytes = session.payload_bytes(Source::Decision)?;
            let mut payload = transaction
                .prepare(POPULATED_DECISION_PAYLOAD)
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            let mut payload_rows = {
                let permission = session.decision_permission_id()?;
                payload
                    .query((permission, bytes as i64))
                    .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            };
            session.before_row(Source::Decision)?;
            let row = payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .ok_or(PermissionError::CorruptRecord)?;
            session.capture_payload(Source::Decision, text(row, 0, bytes)?)?;
            session.before_row(Source::Decision)?;
            if payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
    }
    session.end_source(Source::Decision)?;
    session.before_source_sql(Source::CurrentUse)?;
    {
        let mut statement = transaction
            .prepare(CURRENT_USE)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = statement
            .query((session.current_execution_id().as_str(),))
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        session.before_row(Source::CurrentUse)?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            session.reject_present(Source::CurrentUse)?;
        }
    }
    session.end_source(Source::CurrentUse)?;
    session.before_source_sql(Source::GlobalUseId)?;
    {
        let mut statement = transaction
            .prepare(GLOBAL_USE_ID)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = statement
            .query((session.use_id(),))
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        session.before_row(Source::GlobalUseId)?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            session.reject_present(Source::GlobalUseId)?;
        }
    }
    session.end_source(Source::GlobalUseId)?;
    if !found {
        return Ok(());
    }

    session.before_source_sql(Source::Acceptance)?;
    {
        let mut statement = transaction
            .prepare(ACCEPTANCE_HEADERS)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = {
            let id = session.creator_execution_id()?;
            statement
                .query((id.as_str(),))
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        loop {
            session.before_row(Source::Acceptance)?;
            let Some(row) = rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            else {
                break;
            };
            session.admit_new_header(PermissionNewHeaderCells {
                group: PermissionNewGroup::Acceptances,
                execution_id: Some(bounded_text(row, 0, 256)?),
                primary: bounded_text(row, 1, 256)?,
                thread_id: bounded_text(row, 2, 256)?,
                generation: text(row, 3, 64)?,
                secondary: None,
                permission_id: None,
                turn_id: None,
                digest: None,
                previous_revision: Some(blob(row, 4)?),
                committed_revision: Some(blob(row, 5)?),
                record_version: integer(row, 6)?,
                record_bytes: record_length(row, 7, 8)?,
            })?;
            session.before_source_sql(Source::Acceptance)?;
            let bytes = session.payload_bytes(Source::Acceptance)?;
            let mut payload = transaction
                .prepare(POPULATED_ACCEPTANCE_PAYLOAD)
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            let mut payload_rows = {
                let id = session.creator_execution_id()?;
                let turn = session.creator_turn_id()?;
                payload
                    .query((id.as_str(), turn.as_str(), bytes as i64))
                    .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            };
            session.before_row(Source::Acceptance)?;
            let row = payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .ok_or(PermissionError::CorruptRecord)?;
            session.capture_payload(Source::Acceptance, text(row, 0, bytes)?)?;
            session.before_row(Source::Acceptance)?;
            if payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
    }
    session.end_source(Source::Acceptance)?;

    session.before_source_sql(Source::Grant)?;
    {
        let mut statement = transaction
            .prepare(GRANT_HEADERS)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = {
            let id = session.creator_execution_id()?;
            statement
                .query((id.as_str(),))
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        loop {
            session.before_row(Source::Grant)?;
            let Some(row) = rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            else {
                break;
            };
            session.admit_grant_header(PermissionGrantHeaderCells {
                execution_id: bounded_text(row, 0, 256)?,
                interrupt_id: bounded_text(row, 1, 256)?,
                task_id: bounded_text(row, 2, 256)?,
                thread_id: bounded_text(row, 3, 256)?,
                generation: text(row, 4, 64)?,
                parking_revision: blob(row, 5)?,
                consumed: integer(row, 6)?,
                record_bytes: record_length(row, 7, 8)?,
            })?;
            session.before_source_sql(Source::Grant)?;
            let bytes = session.payload_bytes(Source::Grant)?;
            let mut payload = transaction
                .prepare(super::GRANT_PAYLOAD)
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            let mut payload_rows = {
                let id = session.creator_execution_id()?;
                let interrupt = session.creator_interrupt_id()?;
                payload
                    .query((id.as_str(), interrupt.as_str(), bytes as i64))
                    .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            };
            session.before_row(Source::Grant)?;
            let row = payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .ok_or(PermissionError::CorruptRecord)?;
            session.capture_payload(Source::Grant, text(row, 0, bytes)?)?;
            session.before_row(Source::Grant)?;
            if payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
    }
    session.end_source(Source::Grant)?;

    session.before_source_sql(Source::Creator)?;
    {
        let mut statement = transaction
            .prepare(EXECUTION_LENGTHS)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = {
            let id = session.creator_execution_id()?;
            statement
                .query((id.as_str(),))
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        session.before_row(Source::Creator)?;
        let row = rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .ok_or(PermissionError::CorruptRecord)?;
        let sizes = execution_lengths(row)?;
        session.admit_creator_lengths(sizes.thread, sizes.snapshot)?;
        session.before_row(Source::Creator)?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.before_source_sql(Source::Creator)?;
    {
        let mut statement = transaction
            .prepare(EXECUTION_PAYLOAD)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let (thread, snapshot) = session.creator_lengths()?;
        let mut rows = {
            let id = session.creator_execution_id()?;
            statement
                .query((id.as_str(), thread as i64, snapshot as i64))
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        session.before_row(Source::Creator)?;
        let row = rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .ok_or(PermissionError::CorruptRecord)?;
        session.capture_creator_raw(
            text(row, 0, thread)?,
            blob(row, 1)?,
            blob(row, 2)?,
            text(row, 3, snapshot)?,
        )?;
        session.before_row(Source::Creator)?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.end_source(Source::Creator)?;

    session.before_source_sql(Source::Journal)?;
    {
        let mut statement = transaction
            .prepare(if session.creator_journal_count()? == 7 {
                ORIGINAL_CANCELLED_JOURNAL_HEADERS
            } else {
                JOURNAL_HEADERS
            })
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = {
            let id = session.creator_execution_id()?;
            statement
                .query((id.as_str(),))
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        loop {
            session.before_row(Source::Journal)?;
            let Some(row) = rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            else {
                break;
            };
            session.admit_journal_header(PermissionProjectionJournalHeaderCells {
                sequence: integer(row, 0)?,
                kind: bounded_text(row, 1, 32)?,
                thread_id: bounded_text(row, 2, 256)?,
                execution_id: bounded_text(row, 3, 256)?,
                revision: blob(row, 4)?,
                owner: blob(row, 5)?,
                status: bounded_text(row, 6, 32)?,
                time: blob(row, 7)?,
                payload_bytes: record_length(row, 8, 9)?,
            })?;
            session.before_source_sql(Source::Journal)?;
            let bytes = session.payload_bytes(Source::Journal)?;
            let sequence = session.journal_sequence()?;
            let revision_bytes = session.journal_revision()?.to_be_bytes();
            let mut payload = transaction
                .prepare(POPULATED_JOURNAL_PAYLOAD)
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            let mut payload_rows = {
                let id = session.creator_execution_id()?;
                payload
                    .query((
                        sequence,
                        id.as_str(),
                        revision_bytes.as_slice(),
                        bytes as i64,
                    ))
                    .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            };
            session.before_row(Source::Journal)?;
            let row = payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .ok_or(PermissionError::CorruptRecord)?;
            session.capture_payload(Source::Journal, text(row, 0, bytes)?)?;
            session.before_row(Source::Journal)?;
            if payload_rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
                .is_some()
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
    }
    session.end_source(Source::Journal)?;

    session.before_source_sql(Source::LegacyAcceptance)?;
    {
        let mut statement = transaction
            .prepare(LEGACY_ACCEPTANCE)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = {
            let id = session.creator_execution_id()?;
            statement
                .query((id.as_str(),))
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        session.before_row(Source::LegacyAcceptance)?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            session.reject_present(Source::LegacyAcceptance)?;
        }
    }
    session.end_source(Source::LegacyAcceptance)?;
    Ok(())
}

// Keep both linkage checks' explicit temporary borrows and compiler coercions.
#[allow(clippy::needless_borrow)]
pub(super) fn commit(
    store: &SqliteStore,
    intent: &PermissionReadyAuthorizationIntent<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<Option<b4_runtime::StoredExecution>, PermissionError> {
    let mut session = intent.begin_writer(budget)?;
    let mut connection = store.connection()?;
    let source_operation = |connection: &rusqlite::Connection| -> Result<bool, PermissionError> {
        session.begin_read(PermissionReadyAuthorizationReadPhase::Deferred)?;
        let id = session.current_execution_id();
        let transaction;
        let resource;
        {
            let mut evidence = session.resource_read()?;
            evidence.before_sql(PermissionResourceSql::BeginDeferred, None)?;
            transaction =
                rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
                    .map_err(
                        permission_native_read_transaction_unavailable
                            as fn(rusqlite::Error) -> StoreError,
                    )?;
            let (execution, thread) = capture_resource_in_transaction!(
                transaction,
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
            evidence.before_sql(PermissionResourceSql::CommitRead, None)?;
            resource = evidence.finish()?;
        }
        session.check_current(resource)?;
        read_sources(&transaction, &mut session)?;
        let matched = session.finish_deferred()?;
        transaction.commit().map_err(
            permission_native_read_commit_unavailable as fn(rusqlite::Error) -> StoreError,
        )?;
        Ok(matched)
    };
    assert_operation_size(&source_operation);
    let matched = connection.with_code_only_errors(source_operation)?;

    let write_operation = |connection: &rusqlite::Connection| -> Result<Option<b4_runtime::StoredExecution>, PermissionError> {
        session.begin_immediate()?;
        let id = session.current_execution_id();
        let transaction;
        let resource;
        {
            let mut evidence = session.resource_read()?;
            evidence.before_sql(PermissionResourceSql::BeginDeferred, None)?;
            transaction = rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
                .map_err(permission_native_write_transaction_unavailable as fn(rusqlite::Error) -> StoreError)?;
            let (execution, thread) = capture_resource_in_transaction!(
                transaction, evidence, id, store.owner_generation, before_sql,
                decode_execution, decode_thread, execution, thread, &execution.thread_id,
                (execution, thread)
            );
            evidence.check_linkage(id, &thread, &execution)?;
            evidence.before_sql(PermissionResourceSql::CommitRead, None)?;
            resource = evidence.finish()?;
        }
        session.check_current(resource)?;
        // Full ranges, not merely chosen point keys: a new second candidate
        // cannot be hidden by unchanged selected payload bytes.
        read_sources(&transaction, &mut session)?;
        if !matched {
            let result = session.finish_miss()?;
            transaction.commit().map_err(permission_native_write_commit_unavailable as fn(rusqlite::Error) -> StoreError)?;
            return Ok(result);
        }
        let mut work = session.prepare_work()?;
        let rows = {
            let bound = work.before_write_sql(Write::InsertUse)?;
            let revision_bytes = bound.next_revision().to_be_bytes();
            let parameters = (bound.execution_id(), bound.task_id(), bound.use_id(), bound.thread_id(), bound.generation(), bound.permission_id(), bound.turn_id(), revision_bytes.as_slice(), bound.use_record());
            let mut statement = transaction.prepare(INSERT_USE).map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            statement.execute(parameters).map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        work.check_affected_rows(Write::InsertUse, rows)?;
        let rows = {
            let bound = work.before_write_sql(Write::SnapshotCas)?;
            let next_revision = bound.next_revision().to_be_bytes();
            let owner = bound.owner_generation().to_be_bytes();
            let before_revision = bound.before_revision().to_be_bytes();
            let parameters = (next_revision.as_slice(), owner.as_slice(), bound.snapshot(), bound.execution_id(), bound.thread_id(), before_revision.as_slice(), owner.as_slice());
            let mut statement = transaction.prepare(WRITE_SNAPSHOT_CAS).map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            statement.execute(parameters).map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        work.check_affected_rows(Write::SnapshotCas, rows)?;
        let rows = {
            let bound = work.before_write_sql(Write::ThreadCas)?;
            let next_revision = bound.next_resource_revision().to_be_bytes();
            let before_revision = bound.before_resource_revision().to_be_bytes();
            let parameters = (next_revision.as_slice(), bound.thread_id(), bound.generation(), before_revision.as_slice(), bound.execution_id());
            let mut statement = transaction.prepare(WRITE_THREAD_CAS).map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            statement.execute(parameters).map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        work.check_affected_rows(Write::ThreadCas, rows)?;
        let rows = {
            let bound = work.before_write_sql(Write::InsertJournal)?;
            let revision = bound.next_revision().to_be_bytes();
            let owner = bound.owner_generation().to_be_bytes();
            let time = bound.updated_at_ms().to_be_bytes();
            let parameters = (bound.thread_id(), bound.execution_id(), revision.as_slice(), owner.as_slice(), time.as_slice(), bound.journal());
            let mut statement = transaction.prepare(WRITE_RESUME_JOURNAL).map_err(native as fn(rusqlite::Error) -> PermissionError)?;
            statement.execute(parameters).map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        work.check_affected_rows(Write::InsertJournal, rows)?;
        work.before_commit()?;
        transaction.commit().map_err(permission_native_write_commit_unavailable as fn(rusqlite::Error) -> StoreError)?;
        let stored = work.finish_committed()?;
        Ok(Some(stored))
    };
    assert_operation_size(&write_operation);
    connection.with_code_only_errors(write_operation)
}

fn assert_operation_size<F>(_: &F) {
    const {
        assert!(std::mem::size_of::<F>() <= 512);
    }
}

const _: () = {
    assert!(DECISION_HEADERS.len() <= 1536);
    assert!(ACCEPTANCE_HEADERS.len() <= 1536);
    assert!(GRANT_HEADERS.len() <= 1536);
    assert!(JOURNAL_HEADERS.len() <= 1536);
    assert!(ORIGINAL_CANCELLED_JOURNAL_HEADERS.len() <= 1536);
    assert!(LEGACY_ACCEPTANCE.len() <= 1536);
    assert!(CURRENT_USE.len() <= 1536);
    assert!(GLOBAL_USE_ID.len() <= 1536);
    assert!(INSERT_USE.len() <= 1536);
    assert!(std::mem::size_of::<fn(rusqlite::Error) -> PermissionError>() <= 8);
    assert!(std::mem::size_of::<(&str, &str, &str, &str, &str, &str, &str, &[u8], &str)>() <= 160);
    assert!(
        std::mem::size_of::<
            b4_runtime::permissions::PermissionReadyAuthorizationCurrentRead<'static, 'static>,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<
            b4_runtime::permissions::PermissionReadyAuthorizationCurrentObservation<'static>,
        >() <= 16
    );
    assert!(
        std::mem::size_of::<
            Result<
                b4_runtime::permissions::PermissionReadyAuthorizationCurrentObservation<'static>,
                PermissionError,
            >,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                b4_runtime::permissions::PermissionReadyAuthorizationCurrentObservation<'static>,
            >,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<Result<&'static b4_runtime::StoredExecution, PermissionError>>() <= 128
    );
    assert!(
        std::mem::size_of::<Result<&'static b4_runtime::ThreadRecord, PermissionError>>() <= 128
    );
    assert!(std::mem::size_of::<Result<bool, PermissionError>>() <= 128);
    assert!(std::mem::size_of::<PermissionNewHeaderCells<'static>>() <= 512);
    assert!(std::mem::size_of::<PermissionGrantHeaderCells<'static>>() <= 512);
    assert!(std::mem::size_of::<PermissionProjectionJournalHeaderCells<'static>>() <= 512);
    assert!(
        std::mem::size_of::<
            b4_runtime::permissions::PermissionReadyAuthorizationSqlBindings<'static>,
        >() <= 512
    );
    assert!(
        std::mem::size_of::<
            Result<
                b4_runtime::permissions::PermissionReadyAuthorizationSqlBindings<'static>,
                PermissionError,
            >,
        >() <= 512
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                b4_runtime::permissions::PermissionReadyAuthorizationSqlBindings<'static>,
            >,
        >() <= 512
    );
    assert!(std::mem::size_of::<[u8; 8]>() <= 8);
};
