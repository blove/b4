//! Bounded runtime claim compatibility: real Once dispatch and an unsupported Human writer.
use super::{
    blob, bounded_text, cell, execution_lengths, integer, invalid, kind, length, optional_text,
    record_length, text, thread_lengths, ACCEPTANCE_HEADERS, ACCEPTANCE_PAYLOAD, EXECUTION_LENGTHS,
    EXECUTION_PAYLOAD, GRANT_HEADERS, GRANT_PAYLOAD, METADATA, NEW_ACCEPTANCES, NEW_DECISIONS,
    NEW_USES, POPULATED_ACCEPTANCE_PAYLOAD, POPULATED_DECISION_PAYLOAD, POPULATED_JOURNAL_HEADERS,
    POPULATED_JOURNAL_PAYLOAD, THREAD_LENGTHS, THREAD_PAYLOAD, WRITE_RESUME_JOURNAL,
    WRITE_SNAPSHOT_CAS, WRITE_THREAD_CAS,
};
use crate::SqliteStore;
use b4_runtime::permissions::{
    PermissionAcceptanceHeaderCells, PermissionError, PermissionEvidenceBudget,
    PermissionExecutionCells, PermissionGrantHeaderCells, PermissionNewGroup,
    PermissionNewHeaderCells, PermissionPreparedRuntimeMutation,
    PermissionProjectionJournalHeaderCells, PermissionResourceKey, PermissionResourceSql,
    PermissionRuntimeCapture, PermissionRuntimeCommit, PermissionRuntimeEvidenceKind,
    PermissionRuntimeJournalPoint, PermissionRuntimeReadSql, PermissionRuntimeSelector,
    PermissionRuntimeWriteSql, PermissionThreadCells,
};
use b4_runtime::permissions::{
    PermissionPreparedReadyCancellation, PermissionReadyCancellationWriteSql,
};
use b4_runtime::StoreError;
use rusqlite::{params, types::ValueRef, OptionalExtension, TransactionBehavior};

// One bounded recipe runs under either actual Session's unique Budget admission.
// SQL predicates select the target identity alone; all contradictory cells reach Runtime.
macro_rules! capture_sensitive_in_transaction {
    ($transaction:ident, $evidence:ident, $id:ident) => {
        capture_sensitive_in_transaction!($transaction, $evidence, $id, false)
    };
    ($transaction:ident, $evidence:ident, $id:ident, $use_dispatch:expr) => {{
        for (group, sql) in [
            (PermissionNewGroup::Acceptances, NEW_ACCEPTANCES),
            (PermissionNewGroup::Decisions, NEW_DECISIONS),
            (PermissionNewGroup::Uses, NEW_USES),
        ] {
            $evidence.before_sql(PermissionRuntimeReadSql::NewHeaders(group))?;
            let mut statement = $transaction.prepare(sql).map_err(
                super::permission_native_view_invalid as fn(rusqlite::Error) -> PermissionError,
            )?;
            let mut rows = {
                let key = if group == PermissionNewGroup::Decisions {
                    $evidence.thread_id()?.as_str()
                } else {
                    $id.as_str()
                };
                statement.query((key,)).map_err(
                    super::permission_native_view_invalid as fn(rusqlite::Error) -> PermissionError,
                )?
            };
            loop {
                $evidence.before_row()?;
                let Some(row) = rows.next().map_err(
                    super::permission_native_view_invalid as fn(rusqlite::Error) -> PermissionError,
                )?
                else {
                    break;
                };
                let cells = match group {
                    PermissionNewGroup::Acceptances => PermissionNewHeaderCells {
                        group,
                        execution_id: Some(bounded_text(row, 0, 256)?),
                        primary: bounded_text(row, 1, 256)?,
                        thread_id: bounded_text(row, 2, 256)?,
                        generation: text(row, 3, 64)?,
                        previous_revision: Some(blob(row, 4)?),
                        committed_revision: Some(blob(row, 5)?),
                        record_version: integer(row, 6)?,
                        record_bytes: record_length(row, 7, 8)?,
                        secondary: None,
                        permission_id: None,
                        turn_id: None,
                        digest: None,
                    },
                    PermissionNewGroup::Decisions => PermissionNewHeaderCells {
                        group,
                        execution_id: None,
                        primary: bounded_text(row, 0, 256)?,
                        thread_id: bounded_text(row, 1, 256)?,
                        generation: text(row, 2, 64)?,
                        digest: Some(text(row, 3, 64)?),
                        record_version: integer(row, 4)?,
                        record_bytes: record_length(row, 5, 6)?,
                        secondary: None,
                        permission_id: None,
                        turn_id: None,
                        previous_revision: None,
                        committed_revision: None,
                    },
                    PermissionNewGroup::Uses => PermissionNewHeaderCells {
                        group,
                        execution_id: Some(bounded_text(row, 0, 256)?),
                        primary: bounded_text(row, 1, 256)?,
                        secondary: Some(bounded_text(row, 2, 256)?),
                        thread_id: bounded_text(row, 3, 256)?,
                        generation: text(row, 4, 64)?,
                        permission_id: Some(bounded_text(row, 5, 256)?),
                        turn_id: Some(bounded_text(row, 6, 256)?),
                        previous_revision: Some(blob(row, 7)?),
                        committed_revision: None,
                        digest: None,
                        record_version: integer(row, 8)?,
                        record_bytes: record_length(row, 9, 10)?,
                    },
                };
                $evidence.admit_new_header(cells)?;
            }
            $evidence.end_new_range(group)?;
        }
        // Only ordinary dispatch Read/Write supplies the opaque Use adapter.
        // Cancellation's three-argument invocation retains its original path.
        if !$use_dispatch {
            if $evidence.is_new_human()? {
                $evidence.before_sql(PermissionRuntimeReadSql::AcceptancePayload)?;
                {
                    let mut statement =
                        $transaction.prepare(POPULATED_ACCEPTANCE_PAYLOAD).map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?;
                    let (mut rows, bytes) = {
                        let (turn, bytes) = $evidence.acceptance_query_key()?;
                        (
                            statement
                                .query(($id.as_str(), turn, bytes as i64))
                                .map_err(
                                    super::permission_native_view_invalid
                                        as fn(rusqlite::Error) -> PermissionError,
                                )?,
                            bytes,
                        )
                    };
                    $evidence.before_row()?;
                    let row = rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .ok_or(PermissionError::CorruptRecord)?;
                    $evidence.capture_acceptance_payload(text(row, 0, bytes)?)?;
                    $evidence.before_row()?;
                    if rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
                $evidence.before_sql(PermissionRuntimeReadSql::DecisionPayload)?;
                {
                    let mut statement = $transaction.prepare(POPULATED_DECISION_PAYLOAD).map_err(
                        super::permission_native_view_invalid
                            as fn(rusqlite::Error) -> PermissionError,
                    )?;
                    let (mut rows, bytes) = {
                        let (permission, bytes) = $evidence.decision_query_key()?;
                        (
                            statement.query((permission, bytes as i64)).map_err(
                                super::permission_native_view_invalid
                                    as fn(rusqlite::Error) -> PermissionError,
                            )?,
                            bytes,
                        )
                    };
                    $evidence.before_row()?;
                    let row = rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .ok_or(PermissionError::CorruptRecord)?;
                    $evidence.capture_decision_payload(text(row, 0, bytes)?)?;
                    $evidence.before_row()?;
                    if rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
            }
            $evidence.before_sql(PermissionRuntimeReadSql::GrantHeaders)?;
            {
                let mut statement = $transaction.prepare(GRANT_HEADERS).map_err(
                    super::permission_native_view_invalid as fn(rusqlite::Error) -> PermissionError,
                )?;
                let mut rows = statement.query(($id.as_str(),)).map_err(
                    super::permission_native_view_invalid as fn(rusqlite::Error) -> PermissionError,
                )?;
                loop {
                    $evidence.before_row()?;
                    let Some(row) = rows.next().map_err(
                        super::permission_native_view_invalid
                            as fn(rusqlite::Error) -> PermissionError,
                    )?
                    else {
                        break;
                    };
                    $evidence.admit_grant_header(PermissionGrantHeaderCells {
                        execution_id: bounded_text(row, 0, 256)?,
                        interrupt_id: bounded_text(row, 1, 256)?,
                        task_id: bounded_text(row, 2, 256)?,
                        thread_id: bounded_text(row, 3, 256)?,
                        generation: text(row, 4, 64)?,
                        parking_revision: blob(row, 5)?,
                        consumed: integer(row, 6)?,
                        record_bytes: record_length(row, 7, 8)?,
                    })?;
                    $evidence.before_sql(PermissionRuntimeReadSql::GrantPayload)?;
                    let mut payload_statement = $transaction.prepare(GRANT_PAYLOAD).map_err(
                        super::permission_native_view_invalid
                            as fn(rusqlite::Error) -> PermissionError,
                    )?;
                    let (mut payload_rows, bytes) = {
                        let (interrupt, bytes) = $evidence.grant_query_key()?;
                        (
                            payload_statement
                                .query(($id.as_str(), interrupt, bytes as i64))
                                .map_err(
                                    super::permission_native_view_invalid
                                        as fn(rusqlite::Error) -> PermissionError,
                                )?,
                            bytes,
                        )
                    };
                    $evidence.before_row()?;
                    let payload_row = payload_rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .ok_or(PermissionError::CorruptRecord)?;
                    $evidence.capture_grant_payload(text(payload_row, 0, bytes)?)?;
                    $evidence.before_row()?;
                    if payload_rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
            }
            $evidence.end_grant_range()?;
            $evidence.before_sql(PermissionRuntimeReadSql::OldAcceptanceHeaders)?;
            {
                let mut statement = $transaction.prepare(ACCEPTANCE_HEADERS).map_err(
                    super::permission_native_view_invalid as fn(rusqlite::Error) -> PermissionError,
                )?;
                let mut rows = statement.query(($id.as_str(),)).map_err(
                    super::permission_native_view_invalid as fn(rusqlite::Error) -> PermissionError,
                )?;
                loop {
                    $evidence.before_row()?;
                    let Some(row) = rows.next().map_err(
                        super::permission_native_view_invalid
                            as fn(rusqlite::Error) -> PermissionError,
                    )?
                    else {
                        break;
                    };
                    $evidence.admit_old_acceptance_header(PermissionAcceptanceHeaderCells {
                        execution_id: bounded_text(row, 0, 256)?,
                        turn_id: bounded_text(row, 1, 256)?,
                        thread_id: bounded_text(row, 2, 256)?,
                        generation: text(row, 3, 64)?,
                        previous_revision: blob(row, 4)?,
                        definition_revision: bounded_text(row, 5, 256)?,
                        accepted_at_ms: blob(row, 6)?,
                        record_bytes: record_length(row, 7, 8)?,
                    })?;
                    $evidence.before_sql(PermissionRuntimeReadSql::OldAcceptancePayload)?;
                    let mut payload_statement = $transaction.prepare(ACCEPTANCE_PAYLOAD).map_err(
                        super::permission_native_view_invalid
                            as fn(rusqlite::Error) -> PermissionError,
                    )?;
                    let (mut payload_rows, bytes) = {
                        let (turn, bytes) = $evidence.old_acceptance_query_key()?;
                        (
                            payload_statement
                                .query(($id.as_str(), turn, bytes as i64))
                                .map_err(
                                    super::permission_native_view_invalid
                                        as fn(rusqlite::Error) -> PermissionError,
                                )?,
                            bytes,
                        )
                    };
                    $evidence.before_row()?;
                    let payload_row = payload_rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .ok_or(PermissionError::CorruptRecord)?;
                    $evidence.capture_old_acceptance_payload(text(payload_row, 0, bytes)?)?;
                    $evidence.before_row()?;
                    if payload_rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
            }
            $evidence.end_old_acceptance_range()?;
            for point in [
                PermissionRuntimeJournalPoint::Park,
                PermissionRuntimeJournalPoint::Resume,
            ] {
                let revision = $evidence.journal_revision(point)?;
                $evidence.before_sql(PermissionRuntimeReadSql::JournalHeader(point))?;
                {
                    let mut statement = $transaction.prepare(POPULATED_JOURNAL_HEADERS).map_err(
                        super::permission_native_view_invalid
                            as fn(rusqlite::Error) -> PermissionError,
                    )?;
                    let mut rows = statement
                        .query(($id.as_str(), revision.to_be_bytes().as_slice()))
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?;
                    loop {
                        $evidence.before_row()?;
                        let Some(row) = rows.next().map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        else {
                            break;
                        };
                        $evidence.admit_journal_header(
                            point,
                            PermissionProjectionJournalHeaderCells {
                                sequence: integer(row, 0)?,
                                kind: bounded_text(row, 1, 8)?,
                                thread_id: bounded_text(row, 2, 256)?,
                                execution_id: bounded_text(row, 3, 256)?,
                                revision: blob(row, 4)?,
                                owner: blob(row, 5)?,
                                status: bounded_text(row, 6, 64)?,
                                time: blob(row, 7)?,
                                payload_bytes: record_length(row, 8, 9)?,
                            },
                        )?;
                    }
                }
                $evidence.before_sql(PermissionRuntimeReadSql::JournalPayload(point))?;
                {
                    let mut statement = $transaction.prepare(POPULATED_JOURNAL_PAYLOAD).map_err(
                        super::permission_native_view_invalid
                            as fn(rusqlite::Error) -> PermissionError,
                    )?;
                    let (mut rows, bytes) = {
                        let (sequence, bytes) = $evidence.journal_query_key(point)?;
                        (
                            statement
                                .query((
                                    sequence,
                                    $id.as_str(),
                                    revision.to_be_bytes().as_slice(),
                                    bytes as i64,
                                ))
                                .map_err(
                                    super::permission_native_view_invalid
                                        as fn(rusqlite::Error) -> PermissionError,
                                )?,
                            bytes,
                        )
                    };
                    $evidence.before_row()?;
                    let row = rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .ok_or(PermissionError::CorruptRecord)?;
                    $evidence.capture_journal_payload(point, text(row, 0, bytes)?)?;
                    $evidence.before_row()?;
                    if rows
                        .next()
                        .map_err(
                            super::permission_native_view_invalid
                                as fn(rusqlite::Error) -> PermissionError,
                        )?
                        .is_some()
                    {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
                $evidence.end_journal_range(point)?;
            }
            $evidence.end_evidence()?;
        }
    }};
}

pub(super) fn capture(
    store: &SqliteStore,
    selector: &PermissionRuntimeSelector<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionRuntimeCapture, PermissionError> {
    let mut evidence = budget.runtime_read(selector)?;
    let id = evidence.execution_id();
    evidence.before_resource_sql(PermissionResourceSql::BeginDeferred, None)?;
    evidence.before_code_only_scope()?;
    let mut connection = store.connection()?;
    let operation = |connection: &rusqlite::Connection| -> Result<(), PermissionError> {
        let transaction =
            rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
                .map_err(runtime_dispatch_checked_error_adapter(|_| {
                    crate::unavailable("permission_runtime_transaction_unavailable")
                }))?;
        capture_resource_in_transaction!(
            transaction,
            evidence,
            id,
            store.owner_generation,
            before_resource_sql,
            capture_execution,
            capture_thread,
            (),
            (),
            evidence.thread_query_id()?,
            ()
        );
        if evidence.is_sensitive() {
            evidence.begin_sensitive()?;
            capture_sensitive_in_transaction!(transaction, evidence, id, {
                evidence.before_evidence_kind()?;
                match evidence.evidence_kind()? {
                    PermissionRuntimeEvidenceKind::NewUse => {
                        let selected =
                            super::use_dispatch::read(&transaction, evidence.use_session()?)?;
                        evidence.accept_use(selected)?;
                        true
                    }
                    PermissionRuntimeEvidenceKind::NewHuman
                    | PermissionRuntimeEvidenceKind::LegacyOnce => false,
                }
            });
            evidence.before_sql(PermissionRuntimeReadSql::CommitRead)?;
        } else {
            evidence.before_resource_sql(PermissionResourceSql::CommitRead, None)?;
        }
        transaction
            .commit()
            .map_err(runtime_dispatch_checked_error_adapter(|_| {
                crate::unavailable("permission_runtime_commit_unavailable")
            }))?;
        Ok(())
    };
    assert_runtime_dispatch_operation_size(&operation);
    connection.with_code_only_errors(operation)?;
    evidence.finish_after_commit()
}

pub(super) fn capture_ready_cancellation(
    store: &SqliteStore,
    selector: &PermissionRuntimeSelector<'_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionRuntimeCapture, PermissionError> {
    let mut evidence = budget.runtime_ready_cancellation_read(selector)?;
    let id = evidence.execution_id();
    evidence.before_resource_sql(PermissionResourceSql::BeginDeferred, None)?;
    let mut connection = store.connection()?;
    let operation = |connection: &rusqlite::Connection| -> Result<(), PermissionError> {
        let transaction =
            rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
                .map_err(
                    super::permission_native_read_transaction_unavailable
                        as fn(rusqlite::Error) -> StoreError,
                )?;
        capture_resource_in_transaction!(
            transaction,
            evidence,
            id,
            store.owner_generation,
            before_resource_sql,
            capture_execution,
            capture_thread,
            (),
            (),
            evidence.thread_query_id()?,
            ()
        );
        if evidence.is_sensitive() {
            // This unique adapter loan ends before the real read COMMIT.
            let mut sensitive = evidence.ready_cancellation_sensitive()?;
            sensitive.begin_sensitive()?;
            capture_sensitive_in_transaction!(transaction, sensitive, id);
            sensitive.before_sql(PermissionRuntimeReadSql::CommitRead)?;
        } else {
            evidence.before_resource_sql(PermissionResourceSql::CommitRead, None)?;
        }
        transaction.commit().map_err(
            super::permission_native_read_commit_unavailable as fn(rusqlite::Error) -> StoreError,
        )?;
        Ok(())
    };
    assert_ready_cancellation_operation_size(&operation);
    connection.with_code_only_errors(operation)?;
    evidence.finish_after_commit()
}

pub(super) fn commit(
    store: &SqliteStore,
    prepared: &PermissionPreparedRuntimeMutation<'_, '_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<PermissionRuntimeCommit, PermissionError> {
    // Runtime validates genuine Prepared before its fixed Human refusal, then
    // admits the actual Once I1 loan before any connection or Immediate begins.
    let mut evidence = budget.runtime_write(prepared)?;
    let id = evidence.execution_id();
    evidence.before_begin_immediate()?;
    evidence.before_code_only_scope()?;
    let mut connection = store.connection()?;
    let operation =
        |connection: &rusqlite::Connection| -> Result<PermissionRuntimeCommit, PermissionError> {
            let transaction =
                rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
                    .map_err(runtime_dispatch_checked_error_adapter(|_| {
                        crate::unavailable("permission_runtime_write_transaction_unavailable")
                    }))?;
            capture_resource_in_transaction!(
                transaction,
                evidence,
                id,
                store.owner_generation,
                before_resource_sql,
                capture_execution,
                capture_thread,
                (),
                (),
                evidence.thread_query_id()?,
                ()
            );
            evidence.begin_sensitive()?;
            capture_sensitive_in_transaction!(transaction, evidence, id, {
                evidence.before_evidence_kind()?;
                match evidence.evidence_kind()? {
                    PermissionRuntimeEvidenceKind::NewUse => {
                        let selected =
                            super::use_dispatch::read(&transaction, evidence.use_session()?)?;
                        evidence.accept_use(selected)?;
                        true
                    }
                    PermissionRuntimeEvidenceKind::NewHuman
                    | PermissionRuntimeEvidenceKind::LegacyOnce => false,
                }
            });
            let mut work = evidence.prepare_work()?;
            let rows = {
                let bound = work.before_write_sql(PermissionRuntimeWriteSql::SnapshotCas)?;
                transaction
                    .execute(
                        WRITE_SNAPSHOT_CAS,
                        params![
                            bound.next_revision().to_be_bytes().as_slice(),
                            bound.owner_generation().to_be_bytes().as_slice(),
                            bound.snapshot(),
                            bound.execution_id(),
                            bound.thread_id(),
                            bound.before_revision().to_be_bytes().as_slice(),
                            bound.owner_generation().to_be_bytes().as_slice()
                        ],
                    )
                    .map_err(runtime_dispatch_checked_error_adapter(|_| {
                        super::write_invalid()
                    }))?
            };
            work.check_affected_rows(PermissionRuntimeWriteSql::SnapshotCas, rows)?;
            let rows = {
                let bound = work.before_write_sql(PermissionRuntimeWriteSql::ThreadCas)?;
                transaction
                    .execute(
                        WRITE_THREAD_CAS,
                        params![
                            bound.next_resource_revision().to_be_bytes().as_slice(),
                            bound.thread_id(),
                            bound.generation(),
                            bound.before_resource_revision().to_be_bytes().as_slice(),
                            bound.execution_id()
                        ],
                    )
                    .map_err(runtime_dispatch_checked_error_adapter(|_| {
                        super::write_invalid()
                    }))?
            };
            work.check_affected_rows(PermissionRuntimeWriteSql::ThreadCas, rows)?;
            let rows = {
                let bound =
                    work.before_write_sql(PermissionRuntimeWriteSql::InsertDispatchJournal)?;
                transaction
                    .execute(
                        WRITE_RESUME_JOURNAL,
                        params![
                            bound.thread_id(),
                            bound.execution_id(),
                            bound.next_revision().to_be_bytes().as_slice(),
                            bound.owner_generation().to_be_bytes().as_slice(),
                            bound.updated_at_ms().to_be_bytes().as_slice(),
                            bound.journal()
                        ],
                    )
                    .map_err(runtime_dispatch_checked_error_adapter(|_| {
                        super::write_invalid()
                    }))?
            };
            work.check_affected_rows(PermissionRuntimeWriteSql::InsertDispatchJournal, rows)?;
            {
                let _ = work.before_write_sql(PermissionRuntimeWriteSql::Commit)?;
            }
            transaction
                .commit()
                .map_err(runtime_dispatch_checked_error_adapter(|_| {
                    crate::unavailable("permission_runtime_write_commit_unavailable")
                }))?;
            let commit = work.finish_after_commit();
            Ok(commit)
        };
    assert_runtime_dispatch_operation_size(&operation);
    let commit = connection.with_code_only_errors(operation)?;
    evidence.finish_after_commit();
    Ok(commit)
}

const WRITE_CANCEL_JOURNAL: &str = "INSERT INTO journal (kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload) VALUES ('commit', ?1, ?2, ?3, ?4, '\"cancelled\"', ?5, ?6)";

fn assert_ready_cancellation_operation_size<F: Sized>(_: &F) {
    const {
        assert!(std::mem::size_of::<F>() <= 64);
    }
}

pub(super) fn commit_ready_cancellation(
    store: &SqliteStore,
    prepared: &PermissionPreparedReadyCancellation<'_, '_>,
    budget: &mut PermissionEvidenceBudget,
) -> Result<b4_runtime::StoredExecution, PermissionError> {
    // Admit the genuine cancellation Prepared before any connection or Immediate.
    let mut evidence = budget.runtime_ready_cancellation_write(prepared)?;
    let id = evidence.execution_id();
    evidence.before_begin_immediate()?;
    let mut connection = store.connection()?;
    let operation = |connection: &rusqlite::Connection|
        -> Result<b4_runtime::StoredExecution, PermissionError> {
        let transaction = rusqlite::Transaction::new_unchecked(
            connection,
            TransactionBehavior::Immediate,
        )
            .map_err(super::permission_native_write_transaction_unavailable as fn(rusqlite::Error) -> StoreError)?;
        capture_resource_in_transaction!(
            transaction,
            evidence,
            id,
            store.owner_generation,
            before_resource_sql,
            capture_execution,
            capture_thread,
            (),
            (),
            evidence.thread_query_id()?,
            ()
        );
        evidence.begin_sensitive()?;
        capture_sensitive_in_transaction!(transaction, evidence, id);
        let mut work = evidence.prepare_work()?;
        let rows = {
            let bound = work.before_write_sql(PermissionReadyCancellationWriteSql::SnapshotCas)?;
            let parameter_0_bytes = bound.next_revision().to_be_bytes();
            let parameter_0 = parameter_0_bytes.as_slice();
            let parameter_1_bytes = bound.owner_generation().to_be_bytes();
            let parameter_1 = parameter_1_bytes.as_slice();
            let parameter_2 = bound.snapshot();
            let parameter_3 = bound.execution_id();
            let parameter_4 = bound.thread_id();
            let parameter_5_bytes = bound.before_revision().to_be_bytes();
            let parameter_5 = parameter_5_bytes.as_slice();
            let parameter_6_bytes = bound.owner_generation().to_be_bytes();
            let parameter_6 = parameter_6_bytes.as_slice();
            let parameters = (
                parameter_0,
                parameter_1,
                parameter_2,
                parameter_3,
                parameter_4,
                parameter_5,
                parameter_6,
            );
            let native_result = match transaction.prepare(WRITE_SNAPSHOT_CAS) {
                Ok(mut statement) => statement.execute(parameters),
                Err(error) => Err(error),
            };
            native_result.map_err(super::permission_native_write_invalid as fn(rusqlite::Error) -> PermissionError)?
        };
        work.check_affected_rows(PermissionReadyCancellationWriteSql::SnapshotCas, rows)?;
        let rows = {
            let bound = work.before_write_sql(PermissionReadyCancellationWriteSql::ThreadCas)?;
            let parameter_0_bytes = bound.next_resource_revision().to_be_bytes();
            let parameter_0 = parameter_0_bytes.as_slice();
            let parameter_1 = bound.thread_id();
            let parameter_2 = bound.generation();
            let parameter_3_bytes = bound.before_resource_revision().to_be_bytes();
            let parameter_3 = parameter_3_bytes.as_slice();
            let parameter_4 = bound.execution_id();
            let parameters = (
                parameter_0,
                parameter_1,
                parameter_2,
                parameter_3,
                parameter_4,
            );
            let native_result = match transaction.prepare(WRITE_THREAD_CAS) {
                Ok(mut statement) => statement.execute(parameters),
                Err(error) => Err(error),
            };
            native_result.map_err(super::permission_native_write_invalid as fn(rusqlite::Error) -> PermissionError)?
        };
        work.check_affected_rows(PermissionReadyCancellationWriteSql::ThreadCas, rows)?;
        let rows = {
            let bound =
                work.before_write_sql(PermissionReadyCancellationWriteSql::InsertCancelJournal)?;
            let parameter_0 = bound.thread_id();
            let parameter_1 = bound.execution_id();
            let parameter_2_bytes = bound.next_revision().to_be_bytes();
            let parameter_2 = parameter_2_bytes.as_slice();
            let parameter_3_bytes = bound.owner_generation().to_be_bytes();
            let parameter_3 = parameter_3_bytes.as_slice();
            let parameter_4_bytes = bound.updated_at_ms().to_be_bytes();
            let parameter_4 = parameter_4_bytes.as_slice();
            let parameter_5 = bound.journal();
            let parameters = (
                parameter_0,
                parameter_1,
                parameter_2,
                parameter_3,
                parameter_4,
                parameter_5,
            );
            let native_result = match transaction.prepare(WRITE_CANCEL_JOURNAL) {
                Ok(mut statement) => statement.execute(parameters),
                Err(error) => Err(error),
            };
            native_result.map_err(super::permission_native_write_invalid as fn(rusqlite::Error) -> PermissionError)?
        };
        work.check_affected_rows(
            PermissionReadyCancellationWriteSql::InsertCancelJournal,
            rows,
        )?;
        {
            let _ = work.before_write_sql(PermissionReadyCancellationWriteSql::Commit)?;
        }
        transaction
            .commit()
            .map_err(super::permission_native_write_commit_unavailable as fn(rusqlite::Error) -> StoreError)?;
        let output = work.finish_after_commit();
        Ok(output)
    };
    assert_ready_cancellation_operation_size(&operation);
    let output = connection.with_code_only_errors(operation)?;
    evidence.finish_after_commit();
    Ok(output)
}

const _: () = assert!(std::mem::size_of::<rusqlite::Error>() <= 128);
const _: () = assert!(std::mem::size_of::<rusqlite::types::ValueRef<'static>>() <= 128);
const _: () = assert!(std::mem::size_of::<rusqlite::types::ToSqlOutput<'static>>() <= 128);
const _: () =
    assert!(std::mem::size_of::<rusqlite::Result<rusqlite::types::ToSqlOutput<'static>>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, rusqlite::Error>,
            rusqlite::types::ToSqlOutput<'static>,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<rusqlite::Transaction<'static>>() <= 128);
const _: () =
    assert!(std::mem::size_of::<rusqlite::Result<rusqlite::Transaction<'static>>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<Result<rusqlite::Transaction<'static>, b4_runtime::StoreError>>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, b4_runtime::StoreError>,
            rusqlite::Transaction<'static>,
        >,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<rusqlite::Result<usize>>() <= 128);
const _: () = assert!(std::mem::size_of::<Result<usize, PermissionError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, usize>,
    >() <= 128
);
const _: () = assert!(std::mem::size_of::<rusqlite::Result<()>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, rusqlite::Error>, ()>>(
    ) <= 128
);
const _: () =
    assert!(std::mem::size_of::<std::sync::MutexGuard<'static, rusqlite::Connection>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        Result<std::sync::MutexGuard<'static, rusqlite::Connection>, b4_runtime::StoreError>,
    >() <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, b4_runtime::StoreError>,
            std::sync::MutexGuard<'static, rusqlite::Connection>,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<b4_runtime::StoreError>() <= 128);
const _: () = assert!(std::mem::size_of::<Result<(), b4_runtime::StoreError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, b4_runtime::StoreError>, ()>,
    >() <= 128
);
const _: () =
    assert!(std::mem::size_of::<std::slice::Iter<'static, &'static dyn rusqlite::ToSql>>() <= 32);
const _: () = assert!(std::mem::size_of::<Option<&'static &'static dyn rusqlite::ToSql>>() <= 8);
const _: () = assert!(
    std::mem::size_of::<std::sync::LockResult<std::sync::MutexGuard<'static, rusqlite::Connection>>>(
    ) <= 256
);
const _: () = assert!(
    std::mem::size_of::<std::sync::PoisonError<std::sync::MutexGuard<'static, rusqlite::Connection>>>(
    ) <= 256
);

// Prospective tuple Params carriers: each assertion requires fresh actual compiler evidence.
const _: () = assert!(
    std::mem::size_of::<(
        &'static [u8],
        &'static [u8],
        &'static str,
        &'static str,
        &'static str,
        &'static [u8],
        &'static [u8],
    )>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<(
        &'static [u8],
        &'static str,
        &'static str,
        &'static [u8],
        &'static str,
    )>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<(
        &'static str,
        &'static str,
        &'static [u8],
        &'static [u8],
        &'static [u8],
        &'static str,
    )>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, rusqlite::Error>, usize>,
    >() <= 128
);

// Prospective native READ tuple and cell carriers; actual compilation remains required.
const _: () = assert!(std::mem::size_of::<(&'static str,)>() <= 32);
const _: () = assert!(std::mem::size_of::<(&'static str, i64)>() <= 64);
const _: () = assert!(std::mem::size_of::<(&'static str, &'static [u8])>() <= 64);
const _: () = assert!(std::mem::size_of::<(&'static str, i64, i64)>() <= 64);
const _: () = assert!(std::mem::size_of::<(&'static str, &'static str, i64)>() <= 64);
const _: () = assert!(std::mem::size_of::<(&'static str, Option<i64>, i64, Option<i64>)>() <= 128);
const _: () = assert!(std::mem::size_of::<(i64, &'static str, &'static [u8], i64)>() <= 128);
const _: () =
    assert!(std::mem::size_of::<rusqlite::Result<rusqlite::types::ValueRef<'static>>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<Result<rusqlite::types::ValueRef<'static>, PermissionError>>() <= 128
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            rusqlite::types::ValueRef<'static>,
        >,
    >() <= 128
);

// Genuine native query/result carriers used by the selected cancellation paths.
const _: () = assert!(std::mem::size_of::<rusqlite::Statement<'static>>() <= 256);
const _: () = assert!(std::mem::size_of::<rusqlite::Result<rusqlite::Statement<'static>>>() <= 256);
const _: () =
    assert!(std::mem::size_of::<Result<rusqlite::Statement<'static>, PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, rusqlite::Error>,
            rusqlite::Statement<'static>,
        >,
    >() <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            rusqlite::Statement<'static>,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<rusqlite::Rows<'static>>() <= 256);
const _: () = assert!(std::mem::size_of::<rusqlite::Result<rusqlite::Rows<'static>>>() <= 256);
const _: () =
    assert!(std::mem::size_of::<Result<rusqlite::Rows<'static>, PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, rusqlite::Error>,
            rusqlite::Rows<'static>,
        >,
    >() <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            rusqlite::Rows<'static>,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<rusqlite::Row<'static>>() <= 256);
const _: () = assert!(std::mem::size_of::<Option<rusqlite::Row<'static>>>() <= 256);
const _: () = assert!(std::mem::size_of::<Option<&'static rusqlite::Row<'static>>>() <= 8);
const _: () = assert!(
    std::mem::size_of::<rusqlite::Result<Option<&'static rusqlite::Row<'static>>>>() <= 256
);
const _: () = assert!(
    std::mem::size_of::<Result<Option<&'static rusqlite::Row<'static>>, PermissionError>>() <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, rusqlite::Error>,
            Option<&'static rusqlite::Row<'static>>,
        >,
    >() <= 256
);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            Option<&'static rusqlite::Row<'static>>,
        >,
    >() <= 256
);
const _: () =
    assert!(std::mem::size_of::<rusqlite::Result<&'static rusqlite::Row<'static>>>() <= 256);
const _: () = assert!(std::mem::size_of::<rusqlite::Result<bool>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, rusqlite::Error>, bool>,
    >() <= 256
);
const _: () =
    assert!(std::mem::size_of::<rusqlite::Result<(rusqlite::Statement<'static>, usize)>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, rusqlite::Error>,
            (rusqlite::Statement<'static>, usize),
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<Option<&'static rusqlite::Statement<'static>>>() <= 8);
const _: () = assert!(std::mem::size_of::<Result<&'static [u8], PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &'static [u8]>,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<Result<Option<&'static [u8]>, PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            Option<&'static [u8]>,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<Result<Option<usize>, PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, Option<usize>>,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<Result<i64, PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, i64>,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<Result<usize, std::num::TryFromIntError>>() <= 128);
const _: () = assert!(std::mem::size_of::<super::ExecutionLengths>() <= 128);
const _: () =
    assert!(std::mem::size_of::<Result<super::ExecutionLengths, PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            super::ExecutionLengths,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<super::ThreadLengths>() <= 128);
const _: () = assert!(std::mem::size_of::<Result<super::ThreadLengths, PermissionError>>() <= 256);
const _: () = assert!(
    std::mem::size_of::<
        std::ops::ControlFlow<
            Result<std::convert::Infallible, PermissionError>,
            super::ThreadLengths,
        >,
    >() <= 256
);
const _: () = assert!(std::mem::size_of::<rusqlite::types::Value>() <= 128);

// Genuine scalar Params member carried by selected ThreadPayload tuples.
const _: () = assert!(std::mem::size_of::<Option<i64>>() <= 16);

// Genuine reached native and StoreError residual carriers.
const _: () =
    assert!(std::mem::size_of::<Result<std::convert::Infallible, rusqlite::Error>>() <= 128);
const _: () =
    assert!(std::mem::size_of::<Result<std::convert::Infallible, b4_runtime::StoreError>>() <= 128);

const _: () = assert!(std::mem::size_of::<(b4_runtime::permissions::PermissionError,)>() <= 128);
const _: () = assert!(std::mem::size_of::<(std::num::TryFromIntError,)>() <= 128);

// Actual closed dispatch-operation environments and whole returned carriers.
fn assert_runtime_dispatch_operation_size<F: Sized>(_: &F) {
    const {
        assert!(std::mem::size_of::<F>() <= 64);
    }
}

const _: () = {
    assert!(std::mem::size_of::<(&'static rusqlite::Connection,)>() <= 8);
    assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ()>,
        >() <= 128
    );
    assert!(std::mem::size_of::<Result<PermissionRuntimeCommit, PermissionError>>() <= 1536);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionRuntimeCommit,
            >,
        >() <= 1536
    );
};

// Probe each distinct ordinary error adapter value before its map_err call.
fn runtime_dispatch_checked_error_adapter<F>(adapter: F) -> F {
    const { assert!(std::mem::size_of::<F>() <= 64) };
    adapter
}
