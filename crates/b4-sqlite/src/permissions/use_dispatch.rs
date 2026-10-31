//! Complete bounded Use provenance, shared by Deferred capture and Immediate match.
use super::permission_native_view_invalid as native;
use super::{
    blob, bounded_text, execution_lengths, integer, record_length, text, EXECUTION_LENGTHS,
    EXECUTION_PAYLOAD, GRANT_PAYLOAD, POPULATED_ACCEPTANCE_PAYLOAD, POPULATED_DECISION_PAYLOAD,
    POPULATED_JOURNAL_PAYLOAD,
};
use b4_runtime::permissions::{
    PermissionError, PermissionRuntimeUseEvidence, PermissionRuntimeUseHeaderCells,
    PermissionRuntimeUseReadSql as ReadSql, PermissionRuntimeUseSession,
    PermissionRuntimeUseSource as Source,
};
use rusqlite::Transaction;

// Identity-only predicates preserve contradictory stored indices for validation.
// Singleton ranges include their second-row sentinel before any payload read.
const USE_HEADERS: &str = "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_uses WHERE execution_id = ?1 ORDER BY task_id LIMIT 2";
const DECISION_HEADERS: &str = "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_decisions WHERE thread_id = ?1 ORDER BY permission_id LIMIT 2";
const ACCEPTANCE_HEADERS: &str = "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_acceptances WHERE execution_id = ?1 ORDER BY previous_revision, turn_id LIMIT 2";
const GRANT_HEADERS: &str = "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, typeof(record), length(CAST(record AS BLOB)) FROM approval_grants WHERE execution_id = ?1 ORDER BY interrupt_id LIMIT 2";
const LEGACY_ACCEPTANCE: &str = "SELECT execution_id, turn_id FROM approval_acceptances WHERE execution_id = ?1 ORDER BY turn_id LIMIT 1";
const GLOBAL_USE: &str =
    "SELECT execution_id, task_id, use_id FROM permission_uses WHERE use_id = ?1 LIMIT 2";
const ORIGINAL_JOURNAL: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 7";
const ORIGINAL_CANCELLED_JOURNAL: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 8";
const CURRENT_JOURNAL: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 4";
const USE_PAYLOAD: &str = "SELECT CASE WHEN typeof(record) = 'text' AND length(CAST(record AS BLOB)) = ?4 AND ?4 BETWEEN 1 AND 65536 THEN record END FROM permission_uses WHERE execution_id = ?1 AND task_id = ?2 AND use_id = ?3";

fn read_headers(
    transaction: &Transaction<'_>,
    session: &mut PermissionRuntimeUseSession<'_>,
    source: Source,
) -> Result<(), PermissionError> {
    let sql = match source {
        Source::CurrentUse => USE_HEADERS,
        Source::ThreadDecision => DECISION_HEADERS,
        Source::CurrentLegacyAcceptance | Source::OriginalLegacyAcceptance => LEGACY_ACCEPTANCE,
        Source::CurrentGrant | Source::OriginalGrant => GRANT_HEADERS,
        Source::GlobalUseIdentity => GLOBAL_USE,
        Source::OriginalAcceptance => ACCEPTANCE_HEADERS,
        Source::OriginalJournal if session.expected_count(source) == 7 => ORIGINAL_CANCELLED_JOURNAL,
        Source::OriginalJournal => ORIGINAL_JOURNAL,
        Source::CurrentJournal if session.entered_cancellation_read() =>
            "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 ORDER BY sequence LIMIT 5",
        Source::CurrentJournal => CURRENT_JOURNAL,
        Source::OriginalCreator => return Err(PermissionError::CorruptRecord),
    };
    session.before_sql(ReadSql::Headers(source))?;
    {
        let mut statement = transaction
            .prepare(sql)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let mut rows = {
            let key = match source {
                Source::ThreadDecision => session.thread_id().as_str(),
                Source::GlobalUseIdentity => session.selected_use_key()?.1,
                Source::OriginalAcceptance
                | Source::OriginalGrant
                | Source::OriginalLegacyAcceptance
                | Source::OriginalJournal => session.creator_execution_id()?.as_str(),
                _ => session.current_execution_id().as_str(),
            };
            statement
                .query((key,))
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
        };
        loop {
            session.before_row()?;
            let Some(row) = rows
                .next()
                .map_err(native as fn(rusqlite::Error) -> PermissionError)?
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
                    session.reject_present(source)?;
                    return Err(PermissionError::CorruptRecord);
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
                Source::OriginalAcceptance => session.admit_header(
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
                    if source == Source::CurrentGrant {
                        session.reject_present(source)?;
                        return Err(PermissionError::CorruptRecord);
                    }
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
                Source::OriginalCreator => return Err(PermissionError::CorruptRecord),
            }
        }
    }
    session.end_headers(source)?;
    let expected = match source {
        Source::CurrentLegacyAcceptance
        | Source::CurrentGrant
        | Source::OriginalLegacyAcceptance => 0,
        Source::OriginalJournal => session.expected_count(source),
        Source::CurrentJournal if session.entered_cancellation_read() => {
            session.expected_count(source)
        }
        Source::CurrentJournal => 3,
        _ => 1,
    };
    if session.header_count(source)? != expected {
        return Err(PermissionError::CorruptRecord);
    }
    Ok(())
}

fn read_payload(
    transaction: &Transaction<'_>,
    session: &mut PermissionRuntimeUseSession<'_>,
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
    session.before_sql(ReadSql::Payload(source, index))?;
    {
        let mut statement = transaction
            .prepare(sql)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let (rows, bytes) = {
            match source {
                Source::CurrentUse => {
                    let (task, use_id, bytes) = session.selected_use_key()?;
                    (
                        statement.query((
                            session.current_execution_id().as_str(),
                            task,
                            use_id,
                            bytes as i64,
                        )),
                        bytes,
                    )
                }
                Source::ThreadDecision => {
                    let (permission, bytes) = session.decision_key()?;
                    (statement.query((permission, bytes as i64)), bytes)
                }
                Source::OriginalAcceptance => {
                    let (turn, bytes) = session.acceptance_key()?;
                    (
                        statement.query((
                            session.creator_execution_id()?.as_str(),
                            turn,
                            bytes as i64,
                        )),
                        bytes,
                    )
                }
                Source::OriginalGrant => {
                    let (interrupt, bytes) = session.grant_key()?;
                    (
                        statement.query((
                            session.creator_execution_id()?.as_str(),
                            interrupt,
                            bytes as i64,
                        )),
                        bytes,
                    )
                }
                Source::OriginalJournal | Source::CurrentJournal => {
                    let (sequence, revision, bytes) = session.journal_key(source, index)?;
                    let execution = if source == Source::OriginalJournal {
                        session.creator_execution_id()?.as_str()
                    } else {
                        session.current_execution_id().as_str()
                    };
                    (
                        statement.query((
                            sequence,
                            execution,
                            revision.to_be_bytes().as_slice(),
                            bytes as i64,
                        )),
                        bytes,
                    )
                }
                _ => return Err(PermissionError::CorruptRecord),
            }
        };
        let mut rows = rows.map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        session.before_row()?;
        let row = rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .ok_or(PermissionError::CorruptRecord)?;
        session.capture_payload(source, index, text(row, 0, bytes)?)?;
        session.before_row()?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.end_payload(source, index)
}

fn read_creator(
    transaction: &Transaction<'_>,
    session: &mut PermissionRuntimeUseSession<'_>,
) -> Result<(), PermissionError> {
    session.before_sql(ReadSql::CreatorLengths)?;
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
        session.before_row()?;
        let row = rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .ok_or(PermissionError::CorruptRecord)?;
        let lengths = execution_lengths(row)?;
        session.admit_creator_lengths(lengths.thread, lengths.snapshot)?;
        session.before_row()?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.end_headers(Source::OriginalCreator)?;
    session.before_sql(ReadSql::CreatorPayload)?;
    {
        let mut statement = transaction
            .prepare(EXECUTION_PAYLOAD)
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?;
        let (mut rows, thread_bytes, snapshot_bytes) = {
            let (thread, snapshot) = session.creator_lengths()?;
            let id = session.creator_execution_id()?;
            (
                statement
                    .query((id.as_str(), thread as i64, snapshot as i64))
                    .map_err(native as fn(rusqlite::Error) -> PermissionError)?,
                thread,
                snapshot,
            )
        };
        session.before_row()?;
        let row = rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .ok_or(PermissionError::CorruptRecord)?;
        session.capture_creator_raw(
            text(row, 0, thread_bytes)?,
            blob(row, 1)?,
            blob(row, 2)?,
            text(row, 3, snapshot_bytes)?,
        )?;
        session.before_row()?;
        if rows
            .next()
            .map_err(native as fn(rusqlite::Error) -> PermissionError)?
            .is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
    }
    session.end_payload(Source::OriginalCreator, 0)
}

pub(super) fn read(
    transaction: &Transaction<'_>,
    mut session: PermissionRuntimeUseSession<'_>,
) -> Result<PermissionRuntimeUseEvidence, PermissionError> {
    // U and D payloads provide the borrowed original keys for dependent ranges.
    read_headers(transaction, &mut session, Source::CurrentUse)?;
    read_payload(transaction, &mut session, Source::CurrentUse, 0)?;
    read_headers(transaction, &mut session, Source::ThreadDecision)?;
    read_payload(transaction, &mut session, Source::ThreadDecision, 0)?;
    for source in [
        Source::CurrentLegacyAcceptance,
        Source::CurrentGrant,
        Source::OriginalLegacyAcceptance,
        Source::GlobalUseIdentity,
    ] {
        read_headers(transaction, &mut session, source)?;
    }
    read_headers(transaction, &mut session, Source::OriginalAcceptance)?;
    read_payload(transaction, &mut session, Source::OriginalAcceptance, 0)?;
    read_headers(transaction, &mut session, Source::OriginalGrant)?;
    read_payload(transaction, &mut session, Source::OriginalGrant, 0)?;
    read_creator(transaction, &mut session)?;
    for source in [Source::OriginalJournal, Source::CurrentJournal] {
        read_headers(transaction, &mut session, source)?;
        for index in 0..session.expected_count(source) {
            read_payload(transaction, &mut session, source, index)?;
        }
    }
    session.finish()
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
    assert!(std::mem::size_of::<(&'static str, &'static str, &'static str, i64)>() <= 64);
    assert!(std::mem::size_of::<[&'static [u8]; 8]>() <= 128);
    assert!(std::mem::size_of::<[&'static [u8]; 7]>() <= 128);
    assert!(std::mem::size_of::<[&'static [u8]; 6]>() <= 128);
    assert!(std::mem::size_of::<[&'static [u8]; 4]>() <= 128);
    assert!(std::mem::size_of::<[&'static [u8]; 3]>() <= 128);
    assert!(std::mem::size_of::<[&'static [u8]; 2]>() <= 128);
};
