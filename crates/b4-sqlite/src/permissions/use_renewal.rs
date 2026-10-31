//! Dedicated readonly R3/R4 substrate. Combined C2 remains precisely unsupported.
use super::*;
use b4_runtime::permissions::{
    PermissionUseRenewalAccount, PermissionUseRenewalObservation,
    PermissionUseRenewalResourceSelector, PermissionUseRenewalSourceCapture,
    PermissionUseRenewalSourceSelector,
};

fn checked<F>(value: F) -> F {
    const {
        assert!(std::mem::size_of::<F>() <= 128);
    }
    value
}
pub(super) fn capture_resource(
    store: &SqliteStore,
    selector: &PermissionUseRenewalResourceSelector<'_>,
    account: &mut PermissionUseRenewalAccount,
) -> Result<PermissionUseRenewalObservation, PermissionError> {
    let mut session = selector.begin(account)?;
    session.before_code_only_scope()?;
    let mut connection = store.connection()?;
    let operation =
        |connection: &rusqlite::Connection| -> Result<ApprovalResource, PermissionError> {
            let id = session.execution_id()?;
            let mut evidence = session.resource_read()?;
            evidence.before_sql(PermissionResourceSql::BeginDeferred, None)?;
            let transaction =
                rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
                    .map_err(checked(
                        super::permission_native_read_transaction_unavailable
                            as fn(rusqlite::Error) -> StoreError,
                    ))?;
            let (execution, thread) = capture_resource_in_transaction!(
                transaction,
                evidence,
                id,
                store.owner_generation,
                before_sql,
                decode_use_renewal_execution,
                decode_use_renewal_thread,
                execution,
                thread,
                &execution.thread_id,
                (execution, thread)
            );
            evidence.check_linkage(id, &thread, &execution)?;
            evidence.before_sql(PermissionResourceSql::CommitRead, None)?;
            transaction.commit().map_err(checked(
                super::permission_native_read_commit_unavailable
                    as fn(rusqlite::Error) -> StoreError,
            ))?;
            Ok(ApprovalResource { thread, execution })
        };
    const {
        assert!(std::mem::size_of::<Result<ApprovalResource, PermissionError>>() <= 2048);
    }
    let resource = connection.with_code_only_errors(checked(operation))?;
    session.finish(resource)
}
pub(super) fn capture_source(
    store: &SqliteStore,
    selector: &PermissionUseRenewalSourceSelector<'_>,
    account: &mut PermissionUseRenewalAccount,
) -> Result<PermissionUseRenewalSourceCapture, PermissionError> {
    let mut session = selector.begin(account)?;
    session.before_code_only_scope()?;
    let mut connection = store.connection()?;
    let operation=|connection:&rusqlite::Connection|->Result<PermissionUseRenewalSourceCapture,PermissionError>{
        let transaction={let mut evidence=session.resource_read()?; evidence.before_sql(PermissionResourceSql::BeginDeferred,None)?;
            rusqlite::Transaction::new_unchecked(connection,TransactionBehavior::Deferred).map_err(checked(super::permission_native_read_transaction_unavailable as fn(rusqlite::Error)->StoreError))?};
        let resource={let id=session.current_execution_id()?;let mut evidence=session.resource_read()?;
            let (execution,thread)=capture_resource_in_transaction!(transaction,evidence,id,store.owner_generation,before_sql,
                decode_use_renewal_execution,decode_use_renewal_thread,execution,thread,&execution.thread_id,(execution,thread));
            evidence.check_linkage(id,&thread,&execution)?;ApprovalResource{thread,execution}};
        session.match_resource(resource)?;
        read_combined(&transaction,&mut session)?;
        {let mut evidence=session.resource_read()?;evidence.before_sql(PermissionResourceSql::CommitRead,None)?;}
        transaction.commit().map_err(checked(super::permission_native_read_commit_unavailable as fn(rusqlite::Error)->StoreError))?;
        session.finish()
    };
    connection.with_code_only_errors(checked(operation))
}

const _: () = {
    assert!(std::mem::size_of::<std::sync::MutexGuard<'static, rusqlite::Connection>>() <= 128);
    assert!(std::mem::size_of::<rusqlite::Connection>() <= 256);
    assert!(
        std::mem::size_of::<Result<std::sync::MutexGuard<'static, rusqlite::Connection>, StoreError>>(
        ) <= 128
    );
    assert!(std::mem::size_of::<Result<rusqlite::Statement<'static>, rusqlite::Error>>() <= 512);
    assert!(std::mem::size_of::<Result<rusqlite::Rows<'static>, rusqlite::Error>>() <= 128);
    assert!(std::mem::size_of::<rusqlite::Transaction<'static>>() <= 128);
    assert!(std::mem::size_of::<rusqlite::Statement<'static>>() <= 512);
    assert!(std::mem::size_of::<rusqlite::Row<'static>>() <= 128);
    assert!(std::mem::size_of::<rusqlite::Rows<'static>>() <= 128);
    assert!(std::mem::size_of::<rusqlite::Error>() <= 128);
    assert!(std::mem::size_of::<rusqlite::types::ValueRef<'static>>() <= 128);
    assert!(std::mem::size_of::<Result<ApprovalResource, PermissionError>>() <= 2048);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                ApprovalResource,
            >,
        >() <= 2048
    );
    assert!(
        std::mem::size_of::<Result<PermissionUseRenewalObservation, PermissionError>>() <= 2048
    );
};

const _: () = {
    assert!(
        std::mem::size_of::<(
            &SqliteStore,
            &PermissionUseRenewalResourceSelector<'static>,
            &mut PermissionUseRenewalAccount
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &SqliteStore,
            &PermissionUseRenewalSourceSelector<'static>,
            &mut PermissionUseRenewalAccount
        )>() <= 128
    );
};

// Genuine native and mapped SDK Results and their Try residuals.
const _: () = {
    assert!(std::mem::size_of::<Result<rusqlite::Transaction<'static>, rusqlite::Error>>() <= 128);
    assert!(std::mem::size_of::<Result<rusqlite::Transaction<'static>, StoreError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, StoreError>,
                rusqlite::Transaction<'static>,
            >,
        >() <= 128
    );
    assert!(std::mem::size_of::<rusqlite::types::ToSqlOutput<'static>>() <= 128);
    assert!(
        std::mem::size_of::<Result<rusqlite::types::ToSqlOutput<'static>, rusqlite::Error>>()
            <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, rusqlite::Error>,
                rusqlite::types::ToSqlOutput<'static>,
            >,
        >() <= 128
    );
    assert!(std::mem::size_of::<(&str, i64, i64)>() <= 128);
    assert!(std::mem::size_of::<(&str, Option<i64>, i64, Option<i64>)>() <= 128);
};

// B4 cell adapters: exact Args and conversion/metadata Result carriers.
const _: () = {
    assert!(std::mem::size_of::<(&rusqlite::Row<'static>, usize, &[u8])>() <= 128);
    assert!(std::mem::size_of::<(&rusqlite::Row<'static>, usize, usize)>() <= 128);
    assert!(std::mem::size_of::<(&rusqlite::Row<'static>, usize, Option<usize>)>() <= 128);
    assert!(std::mem::size_of::<Result<usize, std::num::TryFromIntError>>() <= 128);
    assert!(std::mem::size_of::<Result<(), StoreError>>() <= 128);
    assert!(
        std::mem::size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, StoreError>, ()>>(
        ) <= 128
    );
};

const _: () = {
    assert!(std::mem::size_of::<&rusqlite::Row<'static>>() <= 128);
    assert!(std::mem::size_of::<(&rusqlite::Row<'static>,)>() <= 128);
    assert!(std::mem::size_of::<(&rusqlite::Row<'static>, usize)>() <= 128);
};

use super::permission_native_view_invalid as native;
use b4_runtime::permissions::{
    PermissionRuntimeUseHeaderCells, PermissionUseRenewalSource as Source,
    PermissionUseRenewalSourceSession,
};
use rusqlite::Transaction;
// Identity-only predicates preserve contradictory stored indices for validation.
// Singleton ranges include their second-row sentinel before any payload read.
const USE_HEADERS: &str = "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_uses WHERE execution_id = ?1 ORDER BY task_id LIMIT 2";
const DECISION_HEADERS: &str = "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_decisions WHERE thread_id = ?1 ORDER BY permission_id LIMIT 2";
const ACCEPTANCE_HEADERS: &str = "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_acceptances WHERE execution_id = ?1 ORDER BY previous_revision, turn_id LIMIT 2";
const GRANT_HEADERS: &str = "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, typeof(record), length(CAST(record AS BLOB)) FROM approval_grants WHERE execution_id = ?1 ORDER BY interrupt_id LIMIT 2";
const LEGACY_ACCEPTANCE: &str = "SELECT execution_id, turn_id FROM approval_acceptances WHERE execution_id = ?1 ORDER BY turn_id LIMIT 2";
const GLOBAL_USE: &str =
    "SELECT execution_id, task_id, use_id FROM permission_uses WHERE use_id = ?1 LIMIT 2";
const ORIGINAL_JOURNAL: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 7";
const ORIGINAL_CANCELLED_JOURNAL: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 8";
const CURRENT_JOURNAL: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 5";
const USE_PAYLOAD: &str = "SELECT CASE WHEN typeof(record) = 'text' AND length(CAST(record AS BLOB)) = ?4 AND ?4 BETWEEN 1 AND 65536 THEN record END FROM permission_uses WHERE execution_id = ?1 AND task_id = ?2 AND use_id = ?3";

fn read_headers(
    transaction: &Transaction<'_>,
    session: &mut PermissionUseRenewalSourceSession<'_, '_>,
    source: Source,
) -> Result<(), PermissionError> {
    let sql = match source {
        Source::CurrentUse => USE_HEADERS,
        Source::ThreadDecision => DECISION_HEADERS,
        Source::CurrentLegacyAcceptance | Source::OriginalLegacyAcceptance => LEGACY_ACCEPTANCE,
        Source::CurrentGrant | Source::OriginalGrant => GRANT_HEADERS,
        Source::GlobalUseIdentity => GLOBAL_USE,
        Source::OriginalAcceptance | Source::CurrentAcceptance => ACCEPTANCE_HEADERS,
        Source::OriginalJournal if session.expected_count(source) == 7 => {
            ORIGINAL_CANCELLED_JOURNAL
        }
        Source::OriginalJournal => ORIGINAL_JOURNAL,
        Source::CurrentJournal => CURRENT_JOURNAL,
        Source::Creator => return Err(PermissionError::CorruptRecord),
    };
    session.before_headers(source)?;
    {
        let mut statement = transaction
            .prepare(sql)
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?;
        let mut rows = {
            let key = session.header_key(source)?;
            statement
                .query((key,))
                .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
        };
        loop {
            session.before_row()?;
            let Some(row) = rows
                .next()
                .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
            else {
                break;
            };
            match source {
                Source::CurrentLegacyAcceptance | Source::OriginalLegacyAcceptance => {
                    session.admit_header(
                        source,
                        PermissionRuntimeUseHeaderCells {
                            fields: &[bounded_text(row, 0, 256)?, bounded_text(row, 1, 256)?],
                            scalar: 0,
                            record_bytes: 0,
                        },
                    )?;
                    return Err(PermissionError::UnsupportedProfile);
                }
                Source::CurrentUse => session.admit_header(
                    source,
                    PermissionRuntimeUseHeaderCells {
                        fields: &[
                            bounded_text(row, 0, 256)?,
                            bounded_text(row, 1, 256)?,
                            bounded_text(row, 2, 256)?,
                            bounded_text(row, 3, 256)?,
                            text(row, 4, 64)?,
                            bounded_text(row, 5, 256)?,
                            bounded_text(row, 6, 256)?,
                            blob(row, 7)?,
                        ],
                        scalar: integer(row, 8)?,
                        record_bytes: record_length(row, 9, 10)?,
                    },
                )?,
                Source::ThreadDecision => session.admit_header(
                    source,
                    PermissionRuntimeUseHeaderCells {
                        fields: &[
                            bounded_text(row, 0, 256)?,
                            bounded_text(row, 1, 256)?,
                            text(row, 2, 64)?,
                            text(row, 3, 64)?,
                        ],
                        scalar: integer(row, 4)?,
                        record_bytes: record_length(row, 5, 6)?,
                    },
                )?,
                Source::GlobalUseIdentity => session.admit_header(
                    source,
                    PermissionRuntimeUseHeaderCells {
                        fields: &[
                            bounded_text(row, 0, 256)?,
                            bounded_text(row, 1, 256)?,
                            bounded_text(row, 2, 256)?,
                        ],
                        scalar: 0,
                        record_bytes: 0,
                    },
                )?,
                Source::OriginalAcceptance | Source::CurrentAcceptance => session.admit_header(
                    source,
                    PermissionRuntimeUseHeaderCells {
                        fields: &[
                            bounded_text(row, 0, 256)?,
                            bounded_text(row, 1, 256)?,
                            bounded_text(row, 2, 256)?,
                            text(row, 3, 64)?,
                            blob(row, 4)?,
                            blob(row, 5)?,
                        ],
                        scalar: integer(row, 6)?,
                        record_bytes: record_length(row, 7, 8)?,
                    },
                )?,
                Source::OriginalGrant | Source::CurrentGrant => {
                    session.admit_header(
                        source,
                        PermissionRuntimeUseHeaderCells {
                            fields: &[
                                bounded_text(row, 0, 256)?,
                                bounded_text(row, 1, 256)?,
                                bounded_text(row, 2, 256)?,
                                bounded_text(row, 3, 256)?,
                                text(row, 4, 64)?,
                                blob(row, 5)?,
                            ],
                            scalar: integer(row, 6)?,
                            record_bytes: record_length(row, 7, 8)?,
                        },
                    )?;
                }
                Source::OriginalJournal | Source::CurrentJournal => session.admit_header(
                    source,
                    PermissionRuntimeUseHeaderCells {
                        fields: &[
                            bounded_text(row, 1, 32)?,
                            bounded_text(row, 2, 256)?,
                            bounded_text(row, 3, 256)?,
                            blob(row, 4)?,
                            blob(row, 5)?,
                            bounded_text(row, 6, 64)?,
                            blob(row, 7)?,
                        ],
                        scalar: integer(row, 0)?,
                        record_bytes: record_length(row, 8, 9)?,
                    },
                )?,
                Source::Creator => return Err(PermissionError::CorruptRecord),
            }
        }
    }
    session.end_headers(source)?;
    Ok(())
}

fn read_payload(
    transaction: &Transaction<'_>,
    session: &mut PermissionUseRenewalSourceSession<'_, '_>,
    source: Source,
    index: usize,
) -> Result<(), PermissionError> {
    let sql = match source {
        Source::CurrentUse => USE_PAYLOAD,
        Source::ThreadDecision => POPULATED_DECISION_PAYLOAD,
        Source::OriginalAcceptance => POPULATED_ACCEPTANCE_PAYLOAD,
        Source::OriginalGrant => GRANT_PAYLOAD,
        Source::OriginalJournal | Source::CurrentJournal => POPULATED_JOURNAL_PAYLOAD,
        _ => return Err(PermissionError::CorruptRecord),
    };
    session.before_payload(source, index)?;
    {
        let mut statement = transaction
            .prepare(sql)
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?;
        let (first, second, sequence, revision, bytes) = session.payload_key(source, index)?;
        let revision_bytes = revision.to_be_bytes();
        let rows = match source {
            Source::CurrentUse => statement.query((
                session.current_execution_id()?.as_str(),
                first,
                second,
                bytes as i64,
            )),
            Source::ThreadDecision => statement.query((first, bytes as i64)),
            Source::OriginalAcceptance | Source::OriginalGrant => {
                statement.query((first, second, bytes as i64))
            }
            Source::OriginalJournal | Source::CurrentJournal => {
                statement.query((sequence, first, revision_bytes.as_slice(), bytes as i64))
            }
            _ => return Err(PermissionError::CorruptRecord),
        };
        let mut rows = rows.map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?;
        session.before_row()?;
        let row = rows
            .next()
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
            .ok_or(PermissionError::CorruptRecord)?;
        session.capture_payload(source, index, text(row, 0, bytes)?)?;
        session.before_row()?;
        if rows
            .next()
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.end_payload(source, index)
}

fn read_creator_headers(
    transaction: &Transaction<'_>,
    session: &mut PermissionUseRenewalSourceSession<'_, '_>,
) -> Result<(), PermissionError> {
    session.before_headers(Source::Creator)?;
    {
        let mut statement = transaction
            .prepare(EXECUTION_LENGTHS)
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?;
        let id = session.header_key(Source::Creator)?;
        let mut rows = statement
            .query((id,))
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?;
        session.before_row()?;
        let row = rows
            .next()
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
            .ok_or(PermissionError::CorruptRecord)?;
        let lengths = execution_lengths(row)?;
        session.admit_creator_lengths(lengths.thread, lengths.snapshot)?;
        session.before_row()?;
        if rows
            .next()
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.end_headers(Source::Creator)
}
fn read_creator_payload(
    transaction: &Transaction<'_>,
    session: &mut PermissionUseRenewalSourceSession<'_, '_>,
) -> Result<(), PermissionError> {
    session.before_payload(Source::Creator, 0)?;
    {
        let mut statement = transaction
            .prepare(EXECUTION_PAYLOAD)
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?;
        let id = session.header_key(Source::Creator)?;
        let (thread, snapshot) = session.creator_lengths()?;
        let mut rows = statement
            .query((id, thread as i64, snapshot as i64))
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?;
        session.before_row()?;
        let row = rows
            .next()
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
            .ok_or(PermissionError::CorruptRecord)?;
        session.capture_creator_raw(
            text(row, 0, thread)?,
            blob(row, 1)?,
            blob(row, 2)?,
            text(row, 3, snapshot)?,
        )?;
        session.before_row()?;
        if rows
            .next()
            .map_err(checked(native as fn(rusqlite::Error) -> PermissionError))?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.end_payload(Source::Creator, 0)
}
fn read_combined(
    transaction: &Transaction<'_>,
    session: &mut PermissionUseRenewalSourceSession<'_, '_>,
) -> Result<(), PermissionError> {
    for source in [
        Source::CurrentAcceptance,
        Source::CurrentLegacyAcceptance,
        Source::CurrentGrant,
        Source::OriginalLegacyAcceptance,
        Source::CurrentUse,
        Source::ThreadDecision,
        Source::GlobalUseIdentity,
        Source::OriginalAcceptance,
        Source::OriginalGrant,
        Source::OriginalJournal,
        Source::CurrentJournal,
    ] {
        read_headers(transaction, session, source)?;
    }
    read_creator_headers(transaction, session)?;
    for source in [
        Source::CurrentUse,
        Source::ThreadDecision,
        Source::OriginalAcceptance,
        Source::OriginalGrant,
    ] {
        read_payload(transaction, session, source, 0)?;
    }
    read_creator_payload(transaction, session)?;
    for source in [Source::OriginalJournal, Source::CurrentJournal] {
        for index in 0..session.expected_count(source) {
            read_payload(transaction, session, source, index)?;
        }
    }
    Ok(())
}

const _: () = {
    assert!(USE_HEADERS.len() <= 1536);
    assert!(DECISION_HEADERS.len() <= 1536);
    assert!(ACCEPTANCE_HEADERS.len() <= 1536);
    assert!(GRANT_HEADERS.len() <= 1536);
    assert!(LEGACY_ACCEPTANCE.len() <= 1536);
    assert!(GLOBAL_USE.len() <= 1536);
    assert!(ORIGINAL_JOURNAL.len() <= 1536);
    assert!(ORIGINAL_CANCELLED_JOURNAL.len() <= 1536);
    assert!(CURRENT_JOURNAL.len() <= 1536);
    assert!(USE_PAYLOAD.len() <= 1536);
    assert!(std::mem::size_of::<[&[u8]; 8]>() <= 128);
    assert!(std::mem::size_of::<[&[u8]; 7]>() <= 128);
    assert!(std::mem::size_of::<[&[u8]; 6]>() <= 128);
    assert!(std::mem::size_of::<[&[u8]; 4]>() <= 128);
    assert!(std::mem::size_of::<[&[u8]; 3]>() <= 128);
    assert!(std::mem::size_of::<(&str, &str, &str, i64)>() <= 128);
    assert!(std::mem::size_of::<(&str, &str, i64)>() <= 128);
    assert!(std::mem::size_of::<(i64, &str, &[u8], i64)>() <= 128);
    assert!(
        std::mem::size_of::<(
            &Transaction<'static>,
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            Source
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<(
            &Transaction<'static>,
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            Source,
            usize
        )>() <= 128
    );
    assert!(
        std::mem::size_of::<Result<PermissionUseRenewalSourceCapture, PermissionError>>() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRenewalSourceCapture,
            >,
        >() <= 128
    );
};

// Fresh actual SDK adapters and whole caller Args/Results/Flows for this C2.
const _: () = {
    use std::{convert::Infallible, mem::size_of, ops::ControlFlow};
    assert!(size_of::<(&str,)>() <= 128);
    assert!(size_of::<(&str, i64)>() <= 128);
    assert!(size_of::<(&str, &str)>() <= 128);
    assert!(size_of::<[&[u8]; 2]>() <= 128);
    assert!(size_of::<Result<(&str, &str, i64, u64, usize), PermissionError>>() <= 128);
    assert!(
        size_of::<ControlFlow<Result<Infallible, PermissionError>, (&str, &str, i64, u64, usize)>>(
        ) <= 128
    );
    assert!(size_of::<Result<Option<&rusqlite::Row<'static>>, rusqlite::Error>>() <= 128);
    assert!(size_of::<Result<Option<&rusqlite::Row<'static>>, PermissionError>>() <= 128);
    assert!(
        size_of::<ControlFlow<Result<Infallible, rusqlite::Error>, Option<&rusqlite::Row<'static>>>>(
        ) <= 128
    );
    assert!(
        size_of::<ControlFlow<Result<Infallible, PermissionError>, Option<&rusqlite::Row<'static>>>>(
        ) <= 128
    );
    assert!(size_of::<Result<&rusqlite::Row<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<ControlFlow<Result<Infallible, PermissionError>, &rusqlite::Row<'static>>>()
            <= 128
    );
    assert!(size_of::<Result<rusqlite::Statement<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<ControlFlow<Result<Infallible, PermissionError>, rusqlite::Statement<'static>>>()
            <= 128
    );
    assert!(size_of::<Result<rusqlite::Rows<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<ControlFlow<Result<Infallible, PermissionError>, rusqlite::Rows<'static>>>()
            <= 128
    );
    assert!(
        size_of::<(
            &Transaction<'static>,
            &mut PermissionUseRenewalSourceSession<'static, 'static>
        )>() <= 128
    );
    assert!(
        size_of::<(
            &Transaction<'static>,
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            Source,
            usize
        )>() <= 128
    );
    assert!(size_of::<[Source; 11]>() <= 128);
    assert!(size_of::<[Source; 4]>() <= 128);
    assert!(size_of::<[(Source, usize); 2]>() <= 128);
};
