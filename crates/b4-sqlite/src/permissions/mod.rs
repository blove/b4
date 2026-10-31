//! Bounded permission projections and the narrow atomic Ready Use writer.
use crate::SqliteStore;
use b4_contract::ExecutionId;
use b4_runtime::permissions::{
    PermissionOriginalAlwaysCancellationCapture, PermissionOriginalAlwaysCancellationCommit,
    PermissionOriginalAlwaysCancellationSelector, PermissionOriginalAlwaysSelector,
    PermissionPreparedOriginalAlwaysCancellation,
};
use b4_runtime::{
    approval::{ApprovalFence, ApprovalResource},
    permissions::{
        PermissionAcceptanceHeaderCells, PermissionError, PermissionEvidenceBudget,
        PermissionExecutionCells, PermissionGrantHeaderCells, PermissionJournalPoint,
        PermissionLegacyViewRead, PermissionNewGroup, PermissionNewHeaderCells,
        PermissionPopulatedSql, PermissionPopulatedViewRead, PermissionProjectionBranch,
        PermissionProjectionJournalHeaderCells, PermissionProjectionRead, PermissionReadPurpose,
        PermissionResourceKey, PermissionResourceSql, PermissionStore, PermissionThreadCells,
        PermissionView, PermissionViewSelector, PermissionViewSql,
    },
    StoreError,
};
use rusqlite::{params, types::ValueRef, OptionalExtension, Row, TransactionBehavior};

const METADATA: &str = "SELECT schema_version, typeof(migration_checksum), length(CAST(migration_checksum AS BLOB)), CASE WHEN typeof(migration_checksum) = 'text' AND length(CAST(migration_checksum AS BLOB)) = 64 THEN migration_checksum END, typeof(owner_epoch), length(owner_epoch), CASE WHEN typeof(owner_epoch) = 'blob' AND length(owner_epoch) = 8 THEN owner_epoch END FROM metadata WHERE id = 1";
const EXECUTION_LENGTHS: &str = "SELECT typeof(id), length(CAST(id AS BLOB)), typeof(thread_id), length(CAST(thread_id AS BLOB)), typeof(revision), length(revision), typeof(owner_epoch), length(owner_epoch), typeof(snapshot), length(CAST(snapshot AS BLOB)) FROM executions WHERE id = ?1";
const EXECUTION_PAYLOAD: &str = "SELECT CASE WHEN typeof(thread_id) = 'text' AND length(CAST(thread_id AS BLOB)) = ?2 AND ?2 <= 256 THEN thread_id END, CASE WHEN typeof(revision) = 'blob' AND length(revision) = 8 THEN revision END, CASE WHEN typeof(owner_epoch) = 'blob' AND length(owner_epoch) = 8 THEN owner_epoch END, CASE WHEN typeof(snapshot) = 'text' AND length(CAST(snapshot AS BLOB)) = ?3 AND ?3 <= 1048576 THEN snapshot END FROM executions WHERE id = ?1";
const THREAD_LENGTHS: &str = "SELECT typeof(id), length(CAST(id AS BLOB)), typeof(active_execution_id), length(CAST(active_execution_id AS BLOB)), typeof(created_at_ms), length(created_at_ms), typeof(metadata), length(CAST(metadata AS BLOB)), typeof(origin), length(CAST(origin AS BLOB)), typeof(resource_revision), length(resource_revision), typeof(generation), length(CAST(generation AS BLOB)) FROM threads WHERE id = ?1";
const THREAD_PAYLOAD: &str = "SELECT CASE WHEN active_execution_id IS NULL THEN NULL WHEN typeof(active_execution_id) = 'text' AND length(CAST(active_execution_id AS BLOB)) = ?2 AND ?2 <= 256 THEN active_execution_id END, CASE WHEN typeof(created_at_ms) = 'blob' AND length(created_at_ms) = 8 THEN created_at_ms END, CASE WHEN typeof(metadata) = 'text' AND length(CAST(metadata AS BLOB)) = ?3 AND ?3 <= 262144 THEN metadata END, CASE WHEN origin IS NULL THEN NULL WHEN typeof(origin) = 'text' AND length(CAST(origin AS BLOB)) = ?4 AND ?4 <= 262144 THEN origin END, CASE WHEN typeof(resource_revision) = 'blob' AND length(resource_revision) = 8 THEN resource_revision END, CASE WHEN typeof(generation) = 'text' AND length(CAST(generation AS BLOB)) = 64 THEN generation END FROM threads WHERE id = ?1";

struct ExecutionLengths {
    thread: usize,
    snapshot: usize,
}
struct ThreadLengths {
    id: usize,
    active: Option<usize>,
    metadata: usize,
    origin: Option<usize>,
}

pub(super) fn assert_permission_sql_callback_size<F: Sized>(_: &F) {
    const {
        assert!(std::mem::size_of::<F>() <= 64);
    }
}

// Qualify the exact Option.map value without invoking or replacing it.
fn ready_authorization_checked_sql_metadata_value<F>(value: F) -> F {
    assert_permission_sql_callback_size(&value);
    value
}

fn permission_native_resource_invalid(_: rusqlite::Error) -> PermissionError {
    invalid()
}
fn permission_integer_length_invalid(_: std::num::TryFromIntError) -> PermissionError {
    invalid()
}
fn permission_native_schema_unsupported(_: rusqlite::Error) -> StoreError {
    StoreError::UnsupportedSchema
}
fn permission_cell_schema_unsupported(_: PermissionError) -> StoreError {
    StoreError::UnsupportedSchema
}
fn permission_native_view_invalid(_: rusqlite::Error) -> PermissionError {
    view_invalid()
}
fn permission_native_write_invalid(_: rusqlite::Error) -> PermissionError {
    write_invalid()
}
fn permission_native_read_transaction_unavailable(_: rusqlite::Error) -> StoreError {
    crate::unavailable("permission_runtime_transaction_unavailable")
}
fn permission_native_read_commit_unavailable(_: rusqlite::Error) -> StoreError {
    crate::unavailable("permission_runtime_commit_unavailable")
}
fn permission_native_write_transaction_unavailable(_: rusqlite::Error) -> StoreError {
    crate::unavailable("permission_runtime_write_transaction_unavailable")
}
fn permission_native_write_commit_unavailable(_: rusqlite::Error) -> StoreError {
    crate::unavailable("permission_runtime_write_commit_unavailable")
}

const _: () = assert!(std::mem::size_of::<fn(rusqlite::Error) -> PermissionError>() <= 8);
const _: () = assert!(std::mem::size_of::<fn(rusqlite::Error) -> StoreError>() <= 8);
const _: () = assert!(std::mem::size_of::<fn(PermissionError) -> StoreError>() <= 8);
const _: () = assert!(std::mem::size_of::<fn(std::num::TryFromIntError) -> PermissionError>() <= 8);

fn invalid() -> PermissionError {
    StoreError::Corrupt("permission_resource_cells_invalid".into()).into()
}
fn cell<'a>(row: &'a Row<'_>, index: usize) -> Result<ValueRef<'a>, PermissionError> {
    row.get_ref(index)
        .map_err(permission_native_resource_invalid as fn(rusqlite::Error) -> PermissionError)
}
fn kind(row: &Row<'_>, index: usize, expected: &[u8]) -> Result<(), PermissionError> {
    if !matches!(cell(row, index)?, ValueRef::Text(value) if value == expected) {
        return Err(invalid());
    }
    Ok(())
}
fn length(row: &Row<'_>, index: usize) -> Result<usize, PermissionError> {
    match cell(row, index)? {
        ValueRef::Integer(value) => usize::try_from(value).map_err(
            permission_integer_length_invalid as fn(std::num::TryFromIntError) -> PermissionError,
        ),
        _ => Err(invalid()),
    }
}
fn text<'a>(row: &'a Row<'_>, index: usize, expected: usize) -> Result<&'a [u8], PermissionError> {
    match cell(row, index)? {
        ValueRef::Text(value) if value.len() == expected => Ok(value),
        _ => Err(invalid()),
    }
}
fn blob<'a>(row: &'a Row<'_>, index: usize) -> Result<&'a [u8], PermissionError> {
    match cell(row, index)? {
        ValueRef::Blob(value) if value.len() == 8 => Ok(value),
        _ => Err(invalid()),
    }
}
fn optional_length(
    row: &Row<'_>,
    kind_index: usize,
    length_index: usize,
) -> Result<Option<usize>, PermissionError> {
    match cell(row, kind_index)? {
        ValueRef::Text(b"null") if matches!(cell(row, length_index)?, ValueRef::Null) => Ok(None),
        ValueRef::Text(b"text") => {
            let result = length(row, length_index);
            let mapper = Some;
            assert_permission_sql_callback_size(&mapper);
            result.map(mapper)
        }
        _ => Err(invalid()),
    }
}
fn optional_text<'a>(
    row: &'a Row<'_>,
    index: usize,
    expected: Option<usize>,
) -> Result<Option<&'a [u8]>, PermissionError> {
    match expected {
        Some(length) => {
            let result = text(row, index, length);
            let mapper = Some;
            assert_permission_sql_callback_size(&mapper);
            result.map(mapper)
        }
        None if matches!(cell(row, index)?, ValueRef::Null) => Ok(None),
        None => Err(invalid()),
    }
}
fn execution_lengths(row: &Row<'_>) -> Result<ExecutionLengths, PermissionError> {
    kind(row, 0, b"text")?;
    if length(row, 1)? > 256 {
        return Err(invalid());
    }
    kind(row, 2, b"text")?;
    kind(row, 4, b"blob")?;
    kind(row, 6, b"blob")?;
    kind(row, 8, b"text")?;
    if length(row, 5)? != 8 || length(row, 7)? != 8 {
        return Err(invalid());
    }
    Ok(ExecutionLengths {
        thread: length(row, 3)?,
        snapshot: length(row, 9)?,
    })
}
fn thread_lengths(row: &Row<'_>) -> Result<ThreadLengths, PermissionError> {
    kind(row, 0, b"text")?;
    kind(row, 4, b"blob")?;
    kind(row, 6, b"text")?;
    kind(row, 10, b"blob")?;
    kind(row, 12, b"text")?;
    if length(row, 5)? != 8 || length(row, 11)? != 8 || length(row, 13)? != 64 {
        return Err(invalid());
    }
    Ok(ThreadLengths {
        id: length(row, 1)?,
        active: optional_length(row, 2, 3)?,
        metadata: length(row, 7)?,
        origin: optional_length(row, 8, 9)?,
    })
}

// One fixed recipe, expanded into the two explicitly admitted phase owners.
// Ordinary resource retains its original borrow, fees and seven/eight SQL schedule.
macro_rules! capture_resource_in_transaction {
    ($transaction:ident, $evidence:ident, $id:ident, $owner:expr,
     $sql_method:ident, $execution_method:ident, $thread_method:ident,
     $execution:pat, $thread:pat, $query_id:expr, $output:expr) => {{
        $evidence.$sql_method(
            PermissionResourceSql::Metadata,
            Some(PermissionResourceKey::Metadata),
        )?;
        $transaction
            .query_row(METADATA, [], {
                let metadata_callback = |row: &rusqlite::Row<'_>| {
                    Ok({
                        let mut callback = || {
                            let version = match cell(row, 0)? {
                                ValueRef::Integer(version) => version,
                                _ => return Err(StoreError::UnsupportedSchema.into()),
                            };
                            kind(row, 1, b"text").map_err(
                                crate::permissions::permission_cell_schema_unsupported
                                    as fn(PermissionError) -> StoreError,
                            )?;
                            kind(row, 4, b"blob").map_err(
                                crate::permissions::permission_cell_schema_unsupported
                                    as fn(PermissionError) -> StoreError,
                            )?;
                            if length(row, 2)? != 64 {
                                return Err(StoreError::UnsupportedSchema.into());
                            }
                            if length(row, 5)? != 8 {
                                return Err(invalid());
                            }
                            $evidence.check_metadata(
                                version,
                                text(row, 3, 64)?,
                                blob(row, 6)?,
                                $owner,
                                crate::SCHEMA.as_bytes(),
                            )
                        };
                        crate::permissions::assert_permission_sql_callback_size(&callback);
                        callback()
                    })
                };
                crate::permissions::assert_permission_resource_query_callback_size(
                    &metadata_callback,
                );
                metadata_callback
            })
            .map_err(
                crate::permissions::permission_native_schema_unsupported
                    as fn(rusqlite::Error) -> StoreError,
            )??;
        $evidence.$sql_method(
            PermissionResourceSql::ExecutionLengths,
            Some(PermissionResourceKey::Execution($id)),
        )?;
        let execution_length = $transaction
            .query_row(EXECUTION_LENGTHS, ($id.as_str(),), {
                let execution_lengths_callback =
                    |row: &rusqlite::Row<'_>| Ok(execution_lengths(row));
                crate::permissions::assert_permission_resource_query_callback_size(
                    &execution_lengths_callback,
                );
                execution_lengths_callback
            })
            .optional()
            .map_err(
                crate::permissions::permission_native_resource_invalid
                    as fn(rusqlite::Error) -> PermissionError,
            )?
            .ok_or(StoreError::ExecutionNotFound)??;
        $evidence.admit_execution_lengths(execution_length.thread, execution_length.snapshot)?;
        $evidence.$sql_method(
            PermissionResourceSql::ExecutionPayload,
            Some(PermissionResourceKey::Execution($id)),
        )?;
        let $execution = $transaction
            .query_row(
                EXECUTION_PAYLOAD,
                (
                    $id.as_str(),
                    execution_length.thread as i64,
                    execution_length.snapshot as i64,
                ),
                {
                    let execution_payload_callback = |row: &rusqlite::Row<'_>| {
                        Ok({
                            let mut callback = || {
                                $evidence.$execution_method(PermissionExecutionCells {
                                    execution_id: $id,
                                    thread_id: text(row, 0, execution_length.thread)?,
                                    revision: blob(row, 1)?,
                                    owner_generation: blob(row, 2)?,
                                    snapshot: text(row, 3, execution_length.snapshot)?,
                                })
                            };
                            crate::permissions::assert_permission_sql_callback_size(&callback);
                            callback()
                        })
                    };
                    crate::permissions::assert_permission_resource_query_callback_size(
                        &execution_payload_callback,
                    );
                    execution_payload_callback
                },
            )
            .optional()
            .map_err(
                crate::permissions::permission_native_resource_invalid
                    as fn(rusqlite::Error) -> PermissionError,
            )?
            .ok_or(StoreError::ExecutionNotFound)??;
        let query_id = $query_id;
        $evidence.$sql_method(
            PermissionResourceSql::ThreadLengths,
            Some(PermissionResourceKey::Thread(&query_id)),
        )?;
        let thread_length = $transaction
            .query_row(THREAD_LENGTHS, (query_id.as_str(),), {
                let thread_lengths_callback = |row: &rusqlite::Row<'_>| Ok(thread_lengths(row));
                crate::permissions::assert_permission_resource_query_callback_size(
                    &thread_lengths_callback,
                );
                thread_lengths_callback
            })
            .optional()
            .map_err(
                crate::permissions::permission_native_resource_invalid
                    as fn(rusqlite::Error) -> PermissionError,
            )?
            .ok_or(StoreError::ThreadNotFound)??;
        $evidence.admit_thread_lengths(
            thread_length.id,
            thread_length.active.unwrap_or(0),
            64,
            thread_length.metadata,
            thread_length.origin.unwrap_or(0),
        )?;
        $evidence.$sql_method(
            PermissionResourceSql::ThreadPayload,
            Some(PermissionResourceKey::Thread(&query_id)),
        )?;
        let $thread = $transaction
            .query_row(
                THREAD_PAYLOAD,
                (
                    query_id.as_str(),
                    thread_length.active.map(
                        crate::permissions::ready_authorization_checked_sql_metadata_value(
                            |length: usize| length as i64,
                        ),
                    ),
                    thread_length.metadata as i64,
                    thread_length.origin.map(
                        crate::permissions::ready_authorization_checked_sql_metadata_value(
                            |length: usize| length as i64,
                        ),
                    ),
                ),
                {
                    let thread_payload_callback = |row: &rusqlite::Row<'_>| {
                        Ok({
                            let mut callback = || {
                                $evidence.$thread_method(PermissionThreadCells {
                                    id: &query_id,
                                    active_execution_id: optional_text(
                                        row,
                                        0,
                                        thread_length.active,
                                    )?,
                                    created_at_ms: blob(row, 1)?,
                                    metadata: text(row, 2, thread_length.metadata)?,
                                    origin: optional_text(row, 3, thread_length.origin)?,
                                    resource_revision: blob(row, 4)?,
                                    generation: text(row, 5, 64)?,
                                })
                            };
                            crate::permissions::assert_permission_sql_callback_size(&callback);
                            callback()
                        })
                    };
                    crate::permissions::assert_permission_resource_query_callback_size(
                        &thread_payload_callback,
                    );
                    thread_payload_callback
                },
            )
            .optional()
            .map_err(
                crate::permissions::permission_native_resource_invalid
                    as fn(rusqlite::Error) -> PermissionError,
            )?
            .ok_or(StoreError::ThreadNotFound)??;
        $output
    }};
}

impl PermissionStore for SqliteStore {
    fn permission_settled_thread_deletion_atomic_durable(&self) -> bool {
        true
    }
    fn capture_permission_settled_thread_deletion(
        &self,
        target: &b4_runtime::ThreadDeletionTarget,
        account: &mut b4_runtime::permissions::PermissionThreadDeletionAccount,
    ) -> Result<b4_runtime::permissions::PermissionThreadDeletionCapture, PermissionError> {
        thread_deletion::capture(self, target, account)
    }
    fn commit_permission_settled_thread_deletion(
        &self,
        capture: &b4_runtime::permissions::PermissionThreadDeletionCapture,
        target: &b4_runtime::ThreadDeletionTarget,
        account: &mut b4_runtime::permissions::PermissionThreadDeletionAccount,
    ) -> Result<b4_runtime::permissions::PermissionThreadDeletionCommit, PermissionError> {
        thread_deletion::commit(self, capture, target, account)
    }

    fn capture_permission_original_always_current(
        &self,
        selector: &PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<ApprovalResource, PermissionError> {
        original_always::current(self, selector)
    }
    fn capture_permission_original_always(
        &self,
        selector: &b4_runtime::permissions::PermissionOriginalAlwaysSelector<'_>,
    ) -> Result<b4_runtime::permissions::PermissionOriginalAlwaysObservation, PermissionError> {
        original_always::capture(self, selector)
    }
    fn commit_permission_original_always_outcome(
        &self,
        request: &b4_runtime::permissions::PermissionOriginalAlwaysOutcomeRequest<'_>,
    ) -> Result<b4_runtime::permissions::PermissionOriginalAlwaysCommit, PermissionError> {
        original_always::commit(self, request)
    }

    fn commit_permission_use_outcome(
        &self,
        request: &b4_runtime::permissions::PermissionUseOutcomeRequest<'_>,
        account: &mut b4_runtime::permissions::PermissionUseOutcomeAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseOutcomeCommit, PermissionError> {
        use_outcome::commit(self, request, account)
    }

    fn capture_permission_use_outcome_current(
        &self,
        request: &b4_runtime::permissions::PermissionUseOutcomeRequest<'_>,
        account: &mut b4_runtime::permissions::PermissionUseOutcomeAccount,
    ) -> Result<ApprovalResource, PermissionError> {
        let id = request.current_execution_id();
        let mut evidence = request.current_resource_read(account)?;
        let mut connection = self.connection()?;
        evidence.before_sql(PermissionResourceSql::BeginDeferred, None)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|_| crate::unavailable("permission_use_current_read"))?;
        let (execution, thread) = capture_resource_in_transaction!(
            transaction,
            evidence,
            id,
            self.owner_generation,
            before_sql,
            decode_use_outcome_execution,
            decode_use_outcome_thread,
            execution,
            thread,
            &execution.thread_id,
            (execution, thread)
        );
        evidence.check_linkage(id, &thread, &execution)?;
        evidence.before_sql(PermissionResourceSql::CommitRead, None)?;
        transaction
            .commit()
            .map_err(|_| crate::unavailable("permission_use_current_read"))?;
        Ok(ApprovalResource { thread, execution })
    }

    fn capture_permission_use_outcome_source(
        &self,
        selector: &b4_runtime::permissions::PermissionUseOutcomeSourceSelector<'_>,
        account: &mut b4_runtime::permissions::PermissionUseOutcomeAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseOutcomePrepared, PermissionError> {
        use_outcome::capture_source(self, selector, account)
    }

    fn permission_original_ready_cancellation_atomic_durable(&self) -> bool {
        true
    }
    fn permission_original_always_cancellation_atomic_durable(&self) -> bool {
        true
    }
    fn capture_permission_original_always_cancellation(
        &self,
        selector: &PermissionOriginalAlwaysCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionOriginalAlwaysCancellationCapture, PermissionError> {
        original_always_cancellation::capture(self, selector, budget)
    }
    fn commit_permission_original_always_cancellation(
        &self,
        prepared: &PermissionPreparedOriginalAlwaysCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionOriginalAlwaysCancellationCommit, PermissionError> {
        original_always_cancellation::commit(self, prepared, budget)
    }
    fn permission_original_always_withheld_cancellation_atomic_durable(&self) -> bool {
        true
    }
    fn commit_permission_original_always_withheld_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedOriginalAlwaysWithheldCancellation<
            '_,
        >,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<
        b4_runtime::permissions::PermissionOriginalAlwaysWithheldCancellationCommit,
        PermissionError,
    > {
        original_always_withheld_cancellation::commit(self, prepared, budget)
    }
    fn permission_use_withheld_cancellation_atomic_durable(&self) -> bool {
        true
    }
    fn capture_permission_use_withheld_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionUseWithheldCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseWithheldCancellationCapture, PermissionError>
    {
        use_withheld_cancellation::capture(self, selector, budget)
    }
    fn commit_permission_use_withheld_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedUseWithheldCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseWithheldCancellationCommit, PermissionError>
    {
        use_withheld_cancellation::commit(self, prepared, budget)
    }

    fn permission_use_cancellation_atomic_durable(&self) -> bool {
        true
    }
    fn capture_permission_use_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionUseCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseCancellationCapture, PermissionError> {
        use_cancellation::capture(self, selector, budget)
    }
    fn commit_permission_use_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedUseCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseCancellationCommit, PermissionError> {
        use_cancellation::commit(self, prepared, budget)
    }

    fn permission_use_ready_cancellation_atomic_durable(&self) -> bool {
        true
    }
    fn capture_permission_use_ready_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionUseReadyCancellationSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseReadyCancellationCapture, PermissionError>
    {
        use_ready_cancellation::capture(self, selector, budget)
    }
    fn commit_permission_use_ready_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedUseReadyCancellation<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionUseReadyCancellationCommit, PermissionError>
    {
        use_ready_cancellation::commit(self, prepared, budget)
    }

    fn original_always_atomic_durable(&self) -> bool {
        true
    }

    fn ready_authorization_atomic_durable(&self) -> bool {
        true
    }

    fn try_commit_permission_ready_authorization(
        &self,
        intent: &b4_runtime::permissions::PermissionReadyAuthorizationIntent<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Option<b4_runtime::StoredExecution>, PermissionError> {
        ready_authorization::commit(self, intent, budget)
    }

    fn capture_permission_use_renewal_resource(
        &self,
        selector: &b4_runtime::permissions::PermissionUseRenewalResourceSelector<'_>,
        account: &mut b4_runtime::permissions::PermissionUseRenewalAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseRenewalObservation, PermissionError> {
        use_renewal::capture_resource(self, selector, account)
    }
    fn capture_permission_use_renewal_source(
        &self,
        selector: &b4_runtime::permissions::PermissionUseRenewalSourceSelector<'_>,
        account: &mut b4_runtime::permissions::PermissionUseRenewalAccount,
    ) -> Result<b4_runtime::permissions::PermissionUseRenewalSourceCapture, PermissionError> {
        use_renewal::capture_source(self, selector, account)
    }

    fn capture_permission_runtime(
        &self,
        selector: &b4_runtime::permissions::PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCapture, PermissionError> {
        runtime::capture(self, selector, budget)
    }

    fn capture_permission_ready_cancellation(
        &self,
        selector: &b4_runtime::permissions::PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCapture, PermissionError> {
        runtime::capture_ready_cancellation(self, selector, budget)
    }

    fn commit_permission_runtime(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedRuntimeMutation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::permissions::PermissionRuntimeCommit, PermissionError> {
        runtime::commit(self, prepared, budget)
    }

    fn commit_permission_ready_cancellation(
        &self,
        prepared: &b4_runtime::permissions::PermissionPreparedReadyCancellation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::StoredExecution, PermissionError> {
        runtime::commit_ready_cancellation(self, prepared, budget)
    }

    fn permission_ready_authorization_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        read_ready_authorization_resource(self, id, budget)
    }

    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        let mut evidence = budget.resource_read()?; // Sticky refusal precedes connection and SQL.
        evidence.before_sql(PermissionResourceSql::BeginDeferred, None)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|_| crate::unavailable("permission_resource_transaction_unavailable"))?;
        let (execution, thread) = capture_resource_in_transaction!(
            transaction,
            evidence,
            id,
            self.owner_generation,
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
        transaction
            .commit()
            .map_err(|_| crate::unavailable("permission_resource_commit_unavailable"))?;
        Ok(ApprovalResource { thread, execution })
    }

    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        if selector.purpose() == PermissionReadPurpose::Dispatch
            && selector.task_id().is_some()
            && selector.expected_view().is_none()
        {
            let mut evidence = budget.combined_read(id, fence, selector)?;
            evidence.before_resource_sql(PermissionResourceSql::BeginDeferred, None)?;
            let mut connection = self.connection()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Deferred)
                .map_err(|_| crate::unavailable("permission_view_transaction_unavailable"))?;
            capture_resource_in_transaction!(
                transaction,
                evidence,
                id,
                self.owner_generation,
                before_resource_sql,
                capture_execution,
                capture_thread,
                (),
                (),
                evidence.thread_query_id()?,
                ()
            );
            let mut evidence = evidence.into_projection()?;
            read_projection_new_headers(
                &transaction,
                &mut evidence,
                id,
                &selector.context().thread_id,
            )?;
            if evidence.headers_empty()? {
                {
                    let legacy = evidence.empty_legacy_mut()?;
                    read_old_grants(&transaction, legacy, id)?;
                    read_old_acceptances(&transaction, legacy, id)?;
                    legacy.before_sql(PermissionViewSql::Resource(
                        PermissionResourceSql::CommitRead,
                    ))?;
                }
                transaction
                    .commit()
                    .map_err(|_| crate::unavailable("permission_view_commit_unavailable"))?;
                return evidence.finish_empty_after_commit();
            }
            return match evidence.finish_headers()? {
                PermissionProjectionBranch::Legacy(_) => Err(PermissionError::CorruptRecord),
                PermissionProjectionBranch::First(mut evidence) => {
                    read_populated_acceptance_payload(&transaction, &mut evidence)?;
                    read_populated_decision_payload(&transaction, &mut evidence)?;
                    read_populated_grants(&transaction, &mut evidence)?;
                    read_populated_old_acceptance_headers(&transaction, &mut evidence)?;
                    read_populated_journal(
                        &transaction,
                        &mut evidence,
                        PermissionJournalPoint::Park,
                    )?;
                    read_populated_journal(
                        &transaction,
                        &mut evidence,
                        PermissionJournalPoint::Resume,
                    )?;
                    if evidence.has_first_dispatch_journal()? {
                        read_populated_journal(
                            &transaction,
                            &mut evidence,
                            PermissionJournalPoint::Dispatch,
                        )?;
                    }
                    evidence.end_evidence()?;
                    evidence.before_sql(PermissionPopulatedSql::CommitRead)?;
                    transaction
                        .commit()
                        .map_err(|_| crate::unavailable("permission_view_commit_unavailable"))?;
                    evidence.finish_after_commit()
                }
            };
        }
        let mut evidence = budget.combined_read(id, fence, selector)?;
        evidence.before_resource_sql(PermissionResourceSql::BeginDeferred, None)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|_| crate::unavailable("permission_view_transaction_unavailable"))?;
        capture_resource_in_transaction!(
            transaction,
            evidence,
            id,
            self.owner_generation,
            before_resource_sql,
            capture_execution,
            capture_thread,
            (),
            (),
            evidence.thread_query_id()?,
            ()
        );
        let mut evidence = evidence.into_legacy()?;
        // Full resource/policy expectation is matched before any of these tables.
        read_new_headers(
            &transaction,
            &mut evidence,
            id,
            &selector.context().thread_id,
        )?;
        read_old_grants(&transaction, &mut evidence, id)?;
        read_old_acceptances(&transaction, &mut evidence, id)?;
        evidence.before_sql(PermissionViewSql::Resource(
            PermissionResourceSql::CommitRead,
        ))?;
        transaction
            .commit()
            .map_err(|_| crate::unavailable("permission_view_commit_unavailable"))?;
        evidence.finish_after_commit()
    }
    fn commit_permission_acceptance(
        &self,
        prepared: b4_runtime::permissions::PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<b4_runtime::StoredExecution, PermissionError> {
        let mut evidence = budget.acceptance_write(&prepared)?;
        let id = evidence.execution_id();
        let _ = evidence.before_write_sql(PermissionAcceptanceSql::BeginImmediate, None)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| crate::unavailable("permission_acceptance_transaction_unavailable"))?;
        capture_resource_in_transaction!(
            transaction,
            evidence,
            id,
            self.owner_generation,
            before_resource_sql,
            capture_execution,
            capture_thread,
            (),
            (),
            evidence.thread_query_id()?,
            ()
        );
        // The complete actual I1 resource is matched before sensitive rows.
        evidence.begin_sensitive()?;
        read_write_new_headers(&transaction, &mut evidence)?;
        read_write_grants(&transaction, &mut evidence)?;
        read_write_acceptances(&transaction, &mut evidence)?;
        read_write_park_journal(&transaction, &mut evidence)?;
        // Work owns all generated keys and payloads. Runtime pays full graph,
        // renewed eligibility and the prospective reader certificate here.
        let work = evidence.prepare_work()?;
        let bindings = write_bindings(
            &mut evidence,
            PermissionAcceptanceSql::GlobalRequestedPermission,
            &work,
        )?;
        let present = transaction
            .query_row(WRITE_PERMISSION_POINT, [bindings.permission_id], |row| {
                row.get::<_, i64>(0)
            })
            .optional()
            .map_err(|_| write_invalid())?
            .is_some();
        evidence.check_permission_absent(present)?;
        write_acceptance_changes(&transaction, &mut evidence, &work)?;
        let _ = write_bindings(&mut evidence, PermissionAcceptanceSql::Commit, &work)?;
        transaction
            .commit()
            .map_err(|_| crate::unavailable("permission_acceptance_commit_unavailable"))?;
        evidence.finish_after_commit(work)
    }
}

// Closed SQL bodies stay within the per-attempt prepaid2048-byte syntax/bind allowance.
const _: () = assert!(METADATA.len() <= 1536);
const _: () = assert!(EXECUTION_LENGTHS.len() <= 1536);
const _: () = assert!(EXECUTION_PAYLOAD.len() <= 1536);
const _: () = assert!(THREAD_LENGTHS.len() <= 1536);
const _: () = assert!(THREAD_PAYLOAD.len() <= 1536);

// Headers select the target identity alone. Contradictory thread/incarnation
// indices are validated, never filtered into an apparent absence.
const NEW_ACCEPTANCES: &str = "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_acceptances WHERE execution_id = ?1 ORDER BY previous_revision, turn_id LIMIT 65";
const NEW_DECISIONS: &str = "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_decisions WHERE thread_id = ?1 ORDER BY permission_id LIMIT 65";
const NEW_USES: &str = "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_uses WHERE execution_id = ?1 ORDER BY task_id LIMIT 65";
const GRANT_HEADERS: &str = "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, typeof(record), length(CAST(record AS BLOB)) FROM approval_grants WHERE execution_id = ?1 ORDER BY interrupt_id LIMIT 65";
const ACCEPTANCE_HEADERS: &str = "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, typeof(record), length(CAST(record AS BLOB)) FROM approval_acceptances WHERE execution_id = ?1 ORDER BY previous_revision, turn_id LIMIT 65";
const GRANT_PAYLOAD: &str = "SELECT CASE WHEN typeof(record) = 'text' AND length(CAST(record AS BLOB)) = ?3 AND ?3 BETWEEN 1 AND 65536 THEN record END FROM approval_grants WHERE execution_id = ?1 AND interrupt_id = ?2";
const ACCEPTANCE_PAYLOAD: &str = "SELECT CASE WHEN typeof(record) = 'text' AND length(CAST(record AS BLOB)) = ?3 AND ?3 BETWEEN 1 AND 65536 THEN record END FROM approval_acceptances WHERE execution_id = ?1 AND turn_id = ?2";

fn view_invalid() -> PermissionError {
    StoreError::Corrupt("permission_view_cells_invalid".into()).into()
}
fn bounded_text<'a>(
    row: &'a Row<'_>,
    index: usize,
    cap: usize,
) -> Result<&'a [u8], PermissionError> {
    match cell(row, index)? {
        ValueRef::Text(raw) if !raw.is_empty() && raw.len() <= cap => Ok(raw),
        _ => Err(view_invalid()),
    }
}
fn integer(row: &Row<'_>, index: usize) -> Result<i64, PermissionError> {
    match cell(row, index)? {
        ValueRef::Integer(n) => Ok(n),
        _ => Err(view_invalid()),
    }
}
fn record_length(
    row: &Row<'_>,
    kind_index: usize,
    length_index: usize,
) -> Result<usize, PermissionError> {
    kind(row, kind_index, b"text")?;
    length(row, length_index)
}
fn read_new_headers(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionLegacyViewRead<'_, '_, '_>,
    id: &ExecutionId,
    thread: &b4_contract::ThreadId,
) -> Result<(), PermissionError> {
    for (group, sql, key) in [
        (
            PermissionNewGroup::Acceptances,
            NEW_ACCEPTANCES,
            id.as_str(),
        ),
        (
            PermissionNewGroup::Decisions,
            NEW_DECISIONS,
            thread.as_str(),
        ),
        (PermissionNewGroup::Uses, NEW_USES, id.as_str()),
    ] {
        evidence.before_sql(PermissionViewSql::NewHeaders(group))?;
        let mut statement = transaction.prepare(sql).map_err(|_| view_invalid())?;
        let mut rows = statement.query([key]).map_err(|_| view_invalid())?;
        loop {
            evidence.before_row()?;
            let Some(row) = rows.next().map_err(|_| view_invalid())? else {
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
            evidence.admit_new_header(cells)?;
        }
        evidence.end_new_range(group)?;
    }
    Ok(())
}
fn read_old_grants(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionLegacyViewRead<'_, '_, '_>,
    id: &ExecutionId,
) -> Result<(), PermissionError> {
    evidence.before_sql(PermissionViewSql::GrantHeaders)?;
    let mut statement = transaction
        .prepare(GRANT_HEADERS)
        .map_err(|_| view_invalid())?;
    let mut rows = statement.query([id.as_str()]).map_err(|_| view_invalid())?;
    loop {
        evidence.before_row()?;
        let Some(row) = rows.next().map_err(|_| view_invalid())? else {
            break;
        };
        let bytes = record_length(row, 7, 8)?;
        let interrupt = evidence.admit_grant_header(PermissionGrantHeaderCells {
            execution_id: bounded_text(row, 0, 256)?,
            interrupt_id: bounded_text(row, 1, 256)?,
            task_id: bounded_text(row, 2, 256)?,
            thread_id: bounded_text(row, 3, 256)?,
            generation: text(row, 4, 64)?,
            parking_revision: blob(row, 5)?,
            consumed: integer(row, 6)?,
            record_bytes: bytes,
        })?;
        evidence.before_sql(PermissionViewSql::GrantPayload)?;
        transaction
            .query_row(
                GRANT_PAYLOAD,
                params![id.as_str(), interrupt.as_str(), bytes as i64],
                |row| Ok(text(row, 0, bytes).and_then(|raw| evidence.decode_grant_payload(raw))),
            )
            .optional()
            .map_err(|_| view_invalid())?
            .ok_or_else(view_invalid)??;
    }
    evidence.end_grant_range()
}
fn read_old_acceptances(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionLegacyViewRead<'_, '_, '_>,
    id: &ExecutionId,
) -> Result<(), PermissionError> {
    evidence.before_sql(PermissionViewSql::AcceptanceHeaders)?;
    let mut statement = transaction
        .prepare(ACCEPTANCE_HEADERS)
        .map_err(|_| view_invalid())?;
    let mut rows = statement.query([id.as_str()]).map_err(|_| view_invalid())?;
    loop {
        evidence.before_row()?;
        let Some(row) = rows.next().map_err(|_| view_invalid())? else {
            break;
        };
        let bytes = record_length(row, 7, 8)?;
        let turn = evidence.admit_acceptance_header(PermissionAcceptanceHeaderCells {
            execution_id: bounded_text(row, 0, 256)?,
            turn_id: bounded_text(row, 1, 256)?,
            thread_id: bounded_text(row, 2, 256)?,
            generation: text(row, 3, 64)?,
            previous_revision: blob(row, 4)?,
            definition_revision: bounded_text(row, 5, 256)?,
            accepted_at_ms: blob(row, 6)?,
            record_bytes: bytes,
        })?;
        evidence.before_sql(PermissionViewSql::AcceptancePayload)?;
        transaction
            .query_row(
                ACCEPTANCE_PAYLOAD,
                params![id.as_str(), turn.as_str(), bytes as i64],
                |row| {
                    Ok(text(row, 0, bytes).and_then(|raw| evidence.decode_acceptance_payload(raw)))
                },
            )
            .optional()
            .map_err(|_| view_invalid())?
            .ok_or_else(view_invalid)??;
    }
    evidence.end_acceptance_range()
}
const _: () = assert!(NEW_ACCEPTANCES.len() <= 1536);
const _: () = assert!(NEW_DECISIONS.len() <= 1536);
const _: () = assert!(NEW_USES.len() <= 1536);
const _: () = assert!(GRANT_HEADERS.len() <= 1536);
const _: () = assert!(ACCEPTANCE_HEADERS.len() <= 1536);
const _: () = assert!(GRANT_PAYLOAD.len() <= 1536);
const _: () = assert!(ACCEPTANCE_PAYLOAD.len() <= 1536);

use b4_runtime::permissions::{
    PermissionAcceptanceBindings, PermissionAcceptanceSql, PermissionAcceptanceWork,
    PermissionAcceptanceWrite, PermissionParkJournalHeaderCells,
};

// First fresh-Park acceptance transaction; generated bind keys come from sealed work.
const WRITE_PARK_HEADERS: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 AND revision = ?2 ORDER BY sequence LIMIT 2";
const WRITE_PARK_PAYLOAD: &str = "SELECT CASE WHEN typeof(payload) = 'text' AND length(CAST(payload AS BLOB)) = ?4 AND ?4 BETWEEN 1 AND 65536 THEN payload END FROM journal WHERE sequence = ?1 AND execution_id = ?2 AND revision = ?3";
const WRITE_PERMISSION_POINT: &str =
    "SELECT 1 FROM permission_decisions WHERE permission_id = ?1 LIMIT 1";
const WRITE_SNAPSHOT_CAS: &str = "UPDATE executions SET revision = ?1, owner_epoch = ?2, snapshot = ?3 WHERE id = ?4 AND thread_id = ?5 AND revision = ?6 AND owner_epoch = ?7";
const WRITE_THREAD_CAS: &str = "UPDATE threads SET resource_revision = ?1 WHERE id = ?2 AND generation = ?3 AND resource_revision = ?4 AND active_execution_id = ?5";
const WRITE_CONSUME_GRANT: &str = "UPDATE approval_grants SET consumed = 1, record = ?1 WHERE execution_id = ?2 AND interrupt_id = ?3 AND thread_id = ?4 AND thread_generation = ?5 AND consumed = 0";
const WRITE_ACCEPTANCE: &str = "INSERT INTO permission_acceptances (execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)";
const WRITE_DECISION: &str = "INSERT INTO permission_decisions (permission_id, thread_id, thread_generation, scope_digest, record_version, record) VALUES (?1, ?2, ?3, ?4, 1, ?5)";
const WRITE_RESUME_JOURNAL: &str = "INSERT INTO journal (kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload) VALUES ('commit', ?1, ?2, ?3, ?4, '\"running\"', ?5, ?6)";

const _: () = assert!(WRITE_PARK_HEADERS.len() <= 1536);
const _: () = assert!(WRITE_PARK_PAYLOAD.len() <= 1536);
const _: () = assert!(WRITE_PERMISSION_POINT.len() <= 1536);
const _: () = assert!(WRITE_SNAPSHOT_CAS.len() <= 1536);
const _: () = assert!(WRITE_THREAD_CAS.len() <= 1536);
const _: () = assert!(WRITE_CONSUME_GRANT.len() <= 1536);
const _: () = assert!(WRITE_ACCEPTANCE.len() <= 1536);
const _: () = assert!(WRITE_DECISION.len() <= 1536);
const _: () = assert!(WRITE_RESUME_JOURNAL.len() <= 1536);

fn read_write_new_headers(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionAcceptanceWrite<'_, '_>,
) -> Result<(), PermissionError> {
    let id = evidence.execution_id();
    let thread = evidence.thread_id();
    for (group, sql, key) in [
        (
            PermissionNewGroup::Acceptances,
            NEW_ACCEPTANCES,
            id.as_str(),
        ),
        (
            PermissionNewGroup::Decisions,
            NEW_DECISIONS,
            thread.as_str(),
        ),
        (PermissionNewGroup::Uses, NEW_USES, id.as_str()),
    ] {
        evidence.before_view_sql(PermissionViewSql::NewHeaders(group))?;
        let mut statement = transaction.prepare(sql).map_err(|_| view_invalid())?;
        let mut rows = statement.query([key]).map_err(|_| view_invalid())?;
        loop {
            evidence.before_row()?;
            let Some(row) = rows.next().map_err(|_| view_invalid())? else {
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
            evidence.admit_new_header(cells)?;
        }
        evidence.end_new_range(group)?;
    }
    Ok(())
}
fn read_write_grants(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionAcceptanceWrite<'_, '_>,
) -> Result<(), PermissionError> {
    let id = evidence.execution_id();
    evidence.before_view_sql(PermissionViewSql::GrantHeaders)?;
    let mut statement = transaction
        .prepare(GRANT_HEADERS)
        .map_err(|_| view_invalid())?;
    let mut rows = statement.query([id.as_str()]).map_err(|_| view_invalid())?;
    loop {
        evidence.before_row()?;
        let Some(row) = rows.next().map_err(|_| view_invalid())? else {
            break;
        };
        let bytes = record_length(row, 7, 8)?;
        let interrupt = evidence.admit_grant_header(PermissionGrantHeaderCells {
            execution_id: bounded_text(row, 0, 256)?,
            interrupt_id: bounded_text(row, 1, 256)?,
            task_id: bounded_text(row, 2, 256)?,
            thread_id: bounded_text(row, 3, 256)?,
            generation: text(row, 4, 64)?,
            parking_revision: blob(row, 5)?,
            consumed: integer(row, 6)?,
            record_bytes: bytes,
        })?;
        evidence.before_view_sql(PermissionViewSql::GrantPayload)?;
        transaction
            .query_row(
                GRANT_PAYLOAD,
                params![id.as_str(), interrupt.as_str(), bytes as i64],
                |row| Ok(text(row, 0, bytes).and_then(|raw| evidence.decode_grant_payload(raw))),
            )
            .optional()
            .map_err(|_| view_invalid())?
            .ok_or_else(view_invalid)??;
    }
    evidence.end_grant_range()
}
fn read_write_acceptances(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionAcceptanceWrite<'_, '_>,
) -> Result<(), PermissionError> {
    let id = evidence.execution_id();
    evidence.before_view_sql(PermissionViewSql::AcceptanceHeaders)?;
    let mut statement = transaction
        .prepare(ACCEPTANCE_HEADERS)
        .map_err(|_| view_invalid())?;
    let mut rows = statement.query([id.as_str()]).map_err(|_| view_invalid())?;
    loop {
        evidence.before_row()?;
        let Some(row) = rows.next().map_err(|_| view_invalid())? else {
            break;
        };
        let bytes = record_length(row, 7, 8)?;
        let turn = evidence.admit_acceptance_header(PermissionAcceptanceHeaderCells {
            execution_id: bounded_text(row, 0, 256)?,
            turn_id: bounded_text(row, 1, 256)?,
            thread_id: bounded_text(row, 2, 256)?,
            generation: text(row, 3, 64)?,
            previous_revision: blob(row, 4)?,
            definition_revision: bounded_text(row, 5, 256)?,
            accepted_at_ms: blob(row, 6)?,
            record_bytes: bytes,
        })?;
        evidence.before_view_sql(PermissionViewSql::AcceptancePayload)?;
        transaction
            .query_row(
                ACCEPTANCE_PAYLOAD,
                params![id.as_str(), turn.as_str(), bytes as i64],
                |row| {
                    Ok(text(row, 0, bytes).and_then(|raw| evidence.decode_acceptance_payload(raw)))
                },
            )
            .optional()
            .map_err(|_| view_invalid())?
            .ok_or_else(view_invalid)??;
    }
    evidence.end_acceptance_range()
}

fn read_write_park_journal(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionAcceptanceWrite<'_, '_>,
) -> Result<(), PermissionError> {
    let id = evidence.execution_id();
    let revision = evidence.parking_revision();
    let _ = evidence.before_write_sql(PermissionAcceptanceSql::ParkJournalHeader, None)?;
    let mut selected = None;
    {
        let mut statement = transaction
            .prepare(WRITE_PARK_HEADERS)
            .map_err(|_| write_invalid())?;
        let mut rows = statement
            .query(params![id.as_str(), revision.to_be_bytes().as_slice()])
            .map_err(|_| write_invalid())?;
        loop {
            evidence.before_row()?;
            let Some(row) = rows.next().map_err(|_| write_invalid())? else {
                break;
            };
            selected = Some(evidence.admit_park_journal_header(
                PermissionParkJournalHeaderCells {
                    sequence: integer(row, 0)?,
                    kind: bounded_text(row, 1, 8)?,
                    thread_id: bounded_text(row, 2, 256)?,
                    execution_id: bounded_text(row, 3, 256)?,
                    revision: blob(row, 4)?,
                    owner_generation: blob(row, 5)?,
                    status: bounded_text(row, 6, 64)?,
                    updated_at_ms: blob(row, 7)?,
                    payload_bytes: record_length(row, 8, 9)?,
                },
            )?);
        }
    }
    evidence.end_park_journal_range()?;
    let (sequence, bytes) = selected.ok_or_else(write_invalid)?;
    let _ = evidence.before_write_sql(PermissionAcceptanceSql::ParkJournalPayload, None)?;
    transaction.query_row(
        WRITE_PARK_PAYLOAD,
        params![sequence, id.as_str(), revision.to_be_bytes().as_slice(), bytes as i64],
        |row| Ok(text(row, 0, bytes).and_then(|raw| evidence.capture_park_journal_payload(raw))),
    ).optional().map_err(|_| write_invalid())?.ok_or_else(write_invalid)??;
    Ok(())
}

fn write_invalid() -> PermissionError {
    StoreError::Corrupt("permission_acceptance_cells_invalid".into()).into()
}

fn write_bindings<'key, 'budget, 'outer>(
    evidence: &mut PermissionAcceptanceWrite<'budget, 'outer>,
    statement: PermissionAcceptanceSql,
    work: &'key PermissionAcceptanceWork<'budget, 'outer>,
) -> Result<PermissionAcceptanceBindings<'key>, PermissionError> {
    evidence
        .before_write_sql(statement, Some(work))?
        .ok_or(PermissionError::CorruptRecord)
}

fn exactly_one(changed: usize) -> Result<(), PermissionError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StoreError::RevisionConflict.into())
    }
}

fn write_acceptance_changes<'budget, 'outer>(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionAcceptanceWrite<'budget, 'outer>,
    work: &PermissionAcceptanceWork<'budget, 'outer>,
) -> Result<(), PermissionError> {
    let bound = write_bindings(evidence, PermissionAcceptanceSql::SnapshotCas, work)?;
    exactly_one(
        transaction
            .execute(
                WRITE_SNAPSHOT_CAS,
                params![
                    bound.next_revision.to_be_bytes().as_slice(),
                    bound.owner_generation.to_be_bytes().as_slice(),
                    bound.snapshot,
                    bound.execution_id,
                    bound.thread_id,
                    bound.before_revision.to_be_bytes().as_slice(),
                    bound.owner_generation.to_be_bytes().as_slice()
                ],
            )
            .map_err(|_| write_invalid())?,
    )?;
    let bound = write_bindings(evidence, PermissionAcceptanceSql::ThreadCas, work)?;
    exactly_one(
        transaction
            .execute(
                WRITE_THREAD_CAS,
                params![
                    bound.next_resource_revision.to_be_bytes().as_slice(),
                    bound.thread_id,
                    bound.generation,
                    bound.before_resource_revision.to_be_bytes().as_slice(),
                    bound.execution_id
                ],
            )
            .map_err(|_| write_invalid())?,
    )?;
    let bound = write_bindings(evidence, PermissionAcceptanceSql::ConsumeGrant, work)?;
    exactly_one(
        transaction
            .execute(
                WRITE_CONSUME_GRANT,
                params![
                    bound.consumed_grant,
                    bound.execution_id,
                    bound.interrupt_id,
                    bound.thread_id,
                    bound.generation
                ],
            )
            .map_err(|_| write_invalid())?,
    )?;
    let bound = write_bindings(evidence, PermissionAcceptanceSql::InsertAcceptance, work)?;
    exactly_one(
        transaction
            .execute(
                WRITE_ACCEPTANCE,
                params![
                    bound.execution_id,
                    bound.turn_id,
                    bound.thread_id,
                    bound.generation,
                    bound.before_revision.to_be_bytes().as_slice(),
                    bound.next_revision.to_be_bytes().as_slice(),
                    bound.acceptance
                ],
            )
            .map_err(|_| write_invalid())?,
    )?;
    let bound = write_bindings(evidence, PermissionAcceptanceSql::InsertDecision, work)?;
    exactly_one(
        transaction
            .execute(
                WRITE_DECISION,
                params![
                    bound.permission_id,
                    bound.thread_id,
                    bound.generation,
                    bound.scope_digest,
                    bound.decision
                ],
            )
            .map_err(|_| write_invalid())?,
    )?;
    let bound = write_bindings(evidence, PermissionAcceptanceSql::InsertResumeJournal, work)?;
    exactly_one(
        transaction
            .execute(
                WRITE_RESUME_JOURNAL,
                params![
                    bound.thread_id,
                    bound.execution_id,
                    bound.next_revision.to_be_bytes().as_slice(),
                    bound.owner_generation.to_be_bytes().as_slice(),
                    bound.updated_at_ms.to_be_bytes().as_slice(),
                    bound.resume_journal
                ],
            )
            .map_err(|_| write_invalid())?,
    )?;
    Ok(())
}

fn read_projection_new_headers(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionProjectionRead<'_, '_, '_>,
    id: &ExecutionId,
    thread: &b4_contract::ThreadId,
) -> Result<(), PermissionError> {
    for (group, sql, key) in [
        (
            PermissionNewGroup::Acceptances,
            NEW_ACCEPTANCES,
            id.as_str(),
        ),
        (
            PermissionNewGroup::Decisions,
            NEW_DECISIONS,
            thread.as_str(),
        ),
        (PermissionNewGroup::Uses, NEW_USES, id.as_str()),
    ] {
        evidence.before_sql(PermissionViewSql::NewHeaders(group))?;
        let mut statement = transaction.prepare(sql).map_err(|_| view_invalid())?;
        let mut rows = statement.query([key]).map_err(|_| view_invalid())?;
        loop {
            evidence.before_row()?;
            let Some(row) = rows.next().map_err(|_| view_invalid())? else {
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
            evidence.admit_new_header(cells)?;
        }
        evidence.end_new_range(group)?;
    }
    Ok(())
}

// Populated historical reads use the same captured resource and one Deferred loan.
const POPULATED_ACCEPTANCE_PAYLOAD: &str = "SELECT CASE WHEN typeof(record) = 'text' AND length(CAST(record AS BLOB)) = ?3 AND ?3 BETWEEN 1 AND 65536 THEN record END FROM permission_acceptances WHERE execution_id = ?1 AND turn_id = ?2";
const POPULATED_DECISION_PAYLOAD: &str = "SELECT CASE WHEN typeof(record) = 'text' AND length(CAST(record AS BLOB)) = ?2 AND ?2 BETWEEN 1 AND 65536 THEN record END FROM permission_decisions WHERE permission_id = ?1";
const POPULATED_JOURNAL_HEADERS: &str = "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, typeof(payload), length(CAST(payload AS BLOB)) FROM journal WHERE execution_id = ?1 AND revision = ?2 ORDER BY sequence LIMIT 2";
const POPULATED_JOURNAL_PAYLOAD: &str = "SELECT CASE WHEN typeof(payload) = 'text' AND length(CAST(payload AS BLOB)) = ?4 AND ?4 BETWEEN 1 AND 1024 THEN payload END FROM journal WHERE sequence = ?1 AND execution_id = ?2 AND revision = ?3";

const _: () = assert!(POPULATED_ACCEPTANCE_PAYLOAD.len() <= 1536);
const _: () = assert!(POPULATED_DECISION_PAYLOAD.len() <= 1536);
const _: () = assert!(POPULATED_JOURNAL_HEADERS.len() <= 1536);
const _: () = assert!(POPULATED_JOURNAL_PAYLOAD.len() <= 1536);

fn read_populated_acceptance_payload(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionPopulatedViewRead<'_, '_, '_>,
) -> Result<(), PermissionError> {
    let bytes = evidence.acceptance_bytes();
    evidence.before_sql(PermissionPopulatedSql::AcceptancePayload)?;
    let mut statement = transaction
        .prepare(POPULATED_ACCEPTANCE_PAYLOAD)
        .map_err(|_| view_invalid())?;
    let mut rows = statement
        .query(params![
            evidence.execution_id().as_str(),
            evidence.first_acceptance_turn(),
            bytes as i64
        ])
        .map_err(|_| view_invalid())?;
    let row = rows
        .next()
        .map_err(|_| view_invalid())?
        .ok_or(PermissionError::CorruptRecord)?;
    evidence.capture_acceptance_payload(text(row, 0, bytes)?)
}

fn read_populated_decision_payload(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionPopulatedViewRead<'_, '_, '_>,
) -> Result<(), PermissionError> {
    let bytes = evidence.decision_bytes();
    evidence.before_sql(PermissionPopulatedSql::DecisionPayload)?;
    let mut statement = transaction
        .prepare(POPULATED_DECISION_PAYLOAD)
        .map_err(|_| view_invalid())?;
    let mut rows = statement
        .query(params![evidence.first_permission_id(), bytes as i64])
        .map_err(|_| view_invalid())?;
    let row = rows
        .next()
        .map_err(|_| view_invalid())?
        .ok_or(PermissionError::CorruptRecord)?;
    evidence.capture_decision_payload(text(row, 0, bytes)?)
}

fn read_populated_grants(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionPopulatedViewRead<'_, '_, '_>,
) -> Result<(), PermissionError> {
    evidence.before_sql(PermissionPopulatedSql::GrantHeaders)?;
    let mut statement = transaction
        .prepare(GRANT_HEADERS)
        .map_err(|_| view_invalid())?;
    let mut rows = statement
        .query([evidence.execution_id().as_str()])
        .map_err(|_| view_invalid())?;
    loop {
        evidence.before_row()?;
        let Some(row) = rows.next().map_err(|_| view_invalid())? else {
            break;
        };
        let bytes = record_length(row, 7, 8)?;
        let interrupt = evidence.admit_grant_header(PermissionGrantHeaderCells {
            execution_id: bounded_text(row, 0, 256)?,
            interrupt_id: bounded_text(row, 1, 256)?,
            task_id: bounded_text(row, 2, 256)?,
            thread_id: bounded_text(row, 3, 256)?,
            generation: text(row, 4, 64)?,
            parking_revision: blob(row, 5)?,
            consumed: integer(row, 6)?,
            record_bytes: bytes,
        })?;
        evidence.before_sql(PermissionPopulatedSql::GrantPayload)?;
        let mut payload_statement = transaction
            .prepare(GRANT_PAYLOAD)
            .map_err(|_| view_invalid())?;
        let mut payload_rows = payload_statement
            .query(params![
                evidence.execution_id().as_str(),
                interrupt.as_str(),
                bytes as i64
            ])
            .map_err(|_| view_invalid())?;
        let payload_row = payload_rows
            .next()
            .map_err(|_| view_invalid())?
            .ok_or(PermissionError::CorruptRecord)?;
        evidence.capture_grant_payload(text(payload_row, 0, bytes)?)?;
    }
    evidence.end_grant_range()
}

fn read_populated_old_acceptance_headers(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionPopulatedViewRead<'_, '_, '_>,
) -> Result<(), PermissionError> {
    evidence.before_sql(PermissionPopulatedSql::OldAcceptanceHeaders)?;
    let mut statement = transaction
        .prepare(ACCEPTANCE_HEADERS)
        .map_err(|_| view_invalid())?;
    let mut rows = statement
        .query([evidence.execution_id().as_str()])
        .map_err(|_| view_invalid())?;
    loop {
        evidence.before_row()?;
        let Some(row) = rows.next().map_err(|_| view_invalid())? else {
            break;
        };
        let bytes = record_length(row, 7, 8)?;
        evidence.admit_old_acceptance_header(PermissionAcceptanceHeaderCells {
            execution_id: bounded_text(row, 0, 256)?,
            turn_id: bounded_text(row, 1, 256)?,
            thread_id: bounded_text(row, 2, 256)?,
            generation: text(row, 3, 64)?,
            previous_revision: blob(row, 4)?,
            definition_revision: bounded_text(row, 5, 256)?,
            accepted_at_ms: blob(row, 6)?,
            record_bytes: bytes,
        })?;
    }
    evidence.end_old_acceptance_range()
}

fn read_populated_journal(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &mut PermissionPopulatedViewRead<'_, '_, '_>,
    point: PermissionJournalPoint,
) -> Result<(), PermissionError> {
    let revision = evidence.journal_revision(point)?;
    evidence.before_sql(PermissionPopulatedSql::JournalHeader(point))?;
    let mut selected = None;
    {
        let mut statement = transaction
            .prepare(POPULATED_JOURNAL_HEADERS)
            .map_err(|_| view_invalid())?;
        let mut rows = statement
            .query(params![
                evidence.execution_id().as_str(),
                revision.to_be_bytes().as_slice()
            ])
            .map_err(|_| view_invalid())?;
        loop {
            evidence.before_row()?;
            let Some(row) = rows.next().map_err(|_| view_invalid())? else {
                break;
            };
            selected = Some(evidence.admit_journal_header(
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
            )?);
        }
    }
    let (sequence, bytes) = selected.ok_or(PermissionError::CorruptRecord)?;
    evidence.before_sql(PermissionPopulatedSql::JournalPayload(point))?;
    let mut statement = transaction
        .prepare(POPULATED_JOURNAL_PAYLOAD)
        .map_err(|_| view_invalid())?;
    let mut rows = statement
        .query(params![
            sequence,
            evidence.execution_id().as_str(),
            revision.to_be_bytes().as_slice(),
            bytes as i64
        ])
        .map_err(|_| view_invalid())?;
    let row = rows
        .next()
        .map_err(|_| view_invalid())?
        .ok_or(PermissionError::CorruptRecord)?;
    evidence.capture_journal_payload(point, text(row, 0, bytes)?)?;
    evidence.end_journal_range(point)
}

// Compile actual Resource callback and nested returned carriers at every shared invocation.
fn assert_permission_resource_query_callback_size<F, U>(_: &F)
where
    F: for<'row, 'stmt> FnOnce(
        &'row rusqlite::Row<'stmt>,
    ) -> rusqlite::Result<Result<U, PermissionError>>,
{
    const {
        assert!(std::mem::size_of::<F>() <= 64);
        assert!(std::mem::size_of::<U>() <= 512);
        assert!(std::mem::size_of::<Result<U, PermissionError>>() <= 512);
        assert!(std::mem::size_of::<Option<Result<U, PermissionError>>>() <= 512);
        assert!(std::mem::size_of::<rusqlite::Result<Result<U, PermissionError>>>() <= 512);
        assert!(std::mem::size_of::<rusqlite::Result<Option<Result<U, PermissionError>>>>() <= 512);
        assert!(
            std::mem::size_of::<Result<Option<Result<U, PermissionError>>, PermissionError>>()
                <= 512
        );
        assert!(std::mem::size_of::<Result<Result<U, PermissionError>, StoreError>>() <= 512);
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    Option<Result<U, PermissionError>>,
                >,
            >() <= 512
        );
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, StoreError>,
                    Result<U, PermissionError>,
                >,
            >() <= 512
        );
        assert!(
            std::mem::size_of::<
                std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, U>,
            >() <= 512
        );
    }
}

// Declared after the protected resource macro so the child uses its literal recipe.
mod ready_authorization;
mod runtime;
mod use_dispatch;

const _: () = assert!(std::mem::size_of::<(usize,)>() <= 8);
const _: () = assert!(std::mem::size_of::<(&'static [u8],)>() <= 16);

// B4_SDK_LAYOUT_055_PERMISSIONERROR: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = b4_runtime::permissions::PermissionError;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_056_STOREERROR: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = b4_runtime::StoreError;
    assert!(std::mem::size_of::<T>() <= 32);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_057_EXECUTIONLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = ExecutionLengths;
    assert!(std::mem::size_of::<T>() <= 16);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_058_THREADLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = ThreadLengths;
    assert!(std::mem::size_of::<T>() <= 48);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_059_BINDINGS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = b4_runtime::permissions::PermissionRuntimeBindings<'static>;
    assert!(std::mem::size_of::<T>() <= 128);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_060_NATIVEFNPOINTER: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = fn(rusqlite::Error) -> b4_runtime::permissions::PermissionError;
    assert!(std::mem::size_of::<T>() <= 8);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_061_NATIVESTOREFNPOINTER: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = fn(rusqlite::Error) -> b4_runtime::StoreError;
    assert!(std::mem::size_of::<T>() <= 8);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_062_PERMISSIONSTOREFNPOINTER: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = fn(b4_runtime::permissions::PermissionError) -> b4_runtime::StoreError;
    assert!(std::mem::size_of::<T>() <= 8);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_063_INTFNPOINTER: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = fn(std::num::TryFromIntError) -> b4_runtime::permissions::PermissionError;
    assert!(std::mem::size_of::<T>() <= 8);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_064_PERMISSIONARGS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (b4_runtime::permissions::PermissionError,);
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_065_INTEGERARGS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (std::num::TryFromIntError,);
    assert!(std::mem::size_of::<T>() <= 8);
    assert!(std::mem::align_of::<T>() == 1);
};

// B4_SDK_LAYOUT_066_PERMISSIONRESULTUNIT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<(), b4_runtime::permissions::PermissionError>;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_067_PERMISSIONFLOWUNIT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        (),
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_068_PERMISSIONRESULTUSIZE: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<usize, b4_runtime::permissions::PermissionError>;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_069_PERMISSIONFLOWUSIZE: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        usize,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_070_PERMISSIONRESULTOPTIONUSIZE: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<Option<usize>, b4_runtime::permissions::PermissionError>;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_071_PERMISSIONFLOWOPTIONUSIZE: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        Option<usize>,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_072_PERMISSIONRESULTBYTES: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<&'static [u8], b4_runtime::permissions::PermissionError>;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_073_PERMISSIONFLOWBYTES: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        &'static [u8],
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_074_PERMISSIONRESULTOPTIONBYTES: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<Option<&'static [u8]>, b4_runtime::permissions::PermissionError>;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_075_PERMISSIONFLOWOPTIONBYTES: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        Option<&'static [u8]>,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_076_PERMISSIONRESULTEXECUTIONLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<ExecutionLengths, b4_runtime::permissions::PermissionError>;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_077_PERMISSIONFLOWEXECUTIONLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        ExecutionLengths,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_078_PERMISSIONRESULTTHREADLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<ThreadLengths, b4_runtime::permissions::PermissionError>;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_079_PERMISSIONFLOWTHREADLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        ThreadLengths,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_080_PERMISSIONRESULTBINDINGS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        b4_runtime::permissions::PermissionRuntimeBindings<'static>,
        b4_runtime::permissions::PermissionError,
    >;
    assert!(std::mem::size_of::<T>() <= 128);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_081_PERMISSIONFLOWBINDINGS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        b4_runtime::permissions::PermissionRuntimeBindings<'static>,
    >;
    assert!(std::mem::size_of::<T>() <= 128);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_082_PERMISSIONRESULTVALUEREF: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        rusqlite::types::ValueRef<'static>,
        b4_runtime::permissions::PermissionError,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_083_PERMISSIONFLOWVALUEREF: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::ops::ControlFlow<
        std::result::Result<std::convert::Infallible, b4_runtime::permissions::PermissionError>,
        rusqlite::types::ValueRef<'static>,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_084_NESTEDNATIVERESULTUNIT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        std::result::Result<(), b4_runtime::permissions::PermissionError>,
        rusqlite::Error,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_085_NESTEDNATIVEOPTIONALUNIT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        Option<std::result::Result<(), b4_runtime::permissions::PermissionError>>,
        rusqlite::Error,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_086_NESTEDPERMISSIONOPTIONALUNIT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        Option<std::result::Result<(), b4_runtime::permissions::PermissionError>>,
        b4_runtime::permissions::PermissionError,
    >;
    assert!(std::mem::size_of::<T>() <= 80);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_087_NESTEDSTORERESULTUNIT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        std::result::Result<(), b4_runtime::permissions::PermissionError>,
        b4_runtime::StoreError,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_088_NESTEDNATIVERESULTEXECUTIONLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        std::result::Result<ExecutionLengths, b4_runtime::permissions::PermissionError>,
        rusqlite::Error,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_089_NESTEDNATIVEOPTIONALEXECUTIONLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        Option<std::result::Result<ExecutionLengths, b4_runtime::permissions::PermissionError>>,
        rusqlite::Error,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_090_NESTEDPERMISSIONOPTIONALEXECUTIONLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        Option<std::result::Result<ExecutionLengths, b4_runtime::permissions::PermissionError>>,
        b4_runtime::permissions::PermissionError,
    >;
    assert!(std::mem::size_of::<T>() <= 80);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_091_NESTEDSTORERESULTEXECUTIONLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        std::result::Result<ExecutionLengths, b4_runtime::permissions::PermissionError>,
        b4_runtime::StoreError,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_092_NESTEDNATIVERESULTTHREADLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        std::result::Result<ThreadLengths, b4_runtime::permissions::PermissionError>,
        rusqlite::Error,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_093_NESTEDNATIVEOPTIONALTHREADLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        Option<std::result::Result<ThreadLengths, b4_runtime::permissions::PermissionError>>,
        rusqlite::Error,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_094_NESTEDPERMISSIONOPTIONALTHREADLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        Option<std::result::Result<ThreadLengths, b4_runtime::permissions::PermissionError>>,
        b4_runtime::permissions::PermissionError,
    >;
    assert!(std::mem::size_of::<T>() <= 80);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_095_NESTEDSTORERESULTTHREADLENGTHS: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = std::result::Result<
        std::result::Result<ThreadLengths, b4_runtime::permissions::PermissionError>,
        b4_runtime::StoreError,
    >;
    assert!(std::mem::size_of::<T>() <= 72);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_096_PARAMSREAD1: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (&'static str,);
    assert!(std::mem::size_of::<T>() <= 16);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_097_PARAMSREAD2INT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (&'static str, i64);
    assert!(std::mem::size_of::<T>() <= 24);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_098_PARAMSREAD2BYTES: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (&'static str, &'static [u8]);
    assert!(std::mem::size_of::<T>() <= 32);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_099_PARAMSREAD3INT: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (&'static str, i64, i64);
    assert!(std::mem::size_of::<T>() <= 32);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_100_PARAMSREAD3KEY: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (&'static str, &'static str, i64);
    assert!(std::mem::size_of::<T>() <= 40);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_101_PARAMSREAD4OPTIONAL: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (&'static str, Option<i64>, i64, Option<i64>);
    assert!(std::mem::size_of::<T>() <= 56);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_102_PARAMSREAD4JOURNAL: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (i64, &'static str, &'static [u8], i64);
    assert!(std::mem::size_of::<T>() <= 48);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_103_PARAMSWRITE7: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (
        &'static [u8],
        &'static [u8],
        &'static str,
        &'static str,
        &'static str,
        &'static [u8],
        &'static [u8],
    );
    assert!(std::mem::size_of::<T>() <= 112);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_104_PARAMSWRITE5: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (
        &'static [u8],
        &'static str,
        &'static str,
        &'static [u8],
        &'static str,
    );
    assert!(std::mem::size_of::<T>() <= 80);
    assert!(std::mem::align_of::<T>() == 8);
};

// B4_SDK_LAYOUT_105_PARAMSWRITE6: genuine whole nominal upper and exact observed alignment.
const _: () = {
    type T = (
        &'static str,
        &'static str,
        &'static [u8],
        &'static [u8],
        &'static [u8],
        &'static str,
    );
    assert!(std::mem::size_of::<T>() <= 96);
    assert!(std::mem::align_of::<T>() == 8);
};

// This distinct read has no permission-use writer and never changes old readers.
fn read_ready_authorization_resource(
    store: &SqliteStore,
    id: &ExecutionId,
    budget: &mut PermissionEvidenceBudget,
) -> Result<ApprovalResource, PermissionError> {
    let mut evidence = budget.ready_authorization_resource_read()?;
    evidence.before_sql(PermissionResourceSql::BeginDeferred, None)?;
    let mut connection = store.connection()?;
    let owner_generation = store.owner_generation;
    let operation =
        |connection: &rusqlite::Connection| -> Result<ApprovalResource, PermissionError> {
            let transaction =
                rusqlite::Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
                    .map_err(
                        permission_native_read_transaction_unavailable
                            as fn(rusqlite::Error) -> StoreError,
                    )?;
            let (execution, thread) = capture_resource_in_transaction!(
                transaction,
                evidence,
                id,
                owner_generation,
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
            transaction.commit().map_err(
                permission_native_read_commit_unavailable as fn(rusqlite::Error) -> StoreError,
            )?;
            Ok(ApprovalResource { thread, execution })
        };
    assert_ready_authorization_resource_operation_size(&operation);
    // The owned resource may escape; all query/txn locals retire inside this
    // synchronous scope before SDK restoration and the external mutex-loan end.
    connection.with_code_only_errors(operation)
}

fn assert_ready_authorization_resource_operation_size<F>(_: &F) {
    const {
        assert!(std::mem::size_of::<F>() <= 512);
    }
}

const _: () = assert!(std::mem::size_of::<(&rusqlite::Connection,)>() <= 8);
const _: () = assert!(std::mem::size_of::<ApprovalResource>() <= 4096);
const _: () = assert!(std::mem::size_of::<Result<ApprovalResource, PermissionError>>() <= 4096);
const _: () = assert!(
    std::mem::size_of::<Result<std::sync::MutexGuard<'static, rusqlite::Connection>, StoreError>>()
        <= 256
);

mod use_renewal;

mod use_cancellation;
mod use_outcome;
mod use_ready_cancellation;

mod use_outcome_immediate;

mod original_always;

mod original_always_cancellation;

mod use_withheld_cancellation;

mod original_always_withheld_cancellation;

mod thread_deletion;
