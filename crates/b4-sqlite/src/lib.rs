//! SQLite implementation of the native transactional store contract.

mod backup;
pub use backup::BackupError;
mod confinement;
mod discovery;
mod inspection;
mod permissions;
pub use inspection::{inspect_stopped, InspectionError, InspectionOptions, InspectionReport};

use b4_contract::{CommitPlan, ExecutionId, Snapshot, ThreadGeneration, ThreadId, TransitionFacts};
use b4_runtime::approval::{
    reject_unrecorded_decisions, validate_acceptance_record, validate_approval_change,
    validate_grant_record, ApprovalAcceptance, ApprovalError, ApprovalFence, ApprovalMutation,
    ApprovalStore, ApprovalView, GrantRecord,
};
use b4_runtime::{
    generate_thread_generation, next_resource_revision, releases_slot, validate_commit,
    validate_initial, validate_run_cancellation, validate_thread_deletion, validate_thread_seed,
    ExecutionFence, JournalEntry, RunCancellationTarget, Store, StoreCapabilities, StoreError,
    StoreKind, StoredExecution, ThreadDeletionTarget, ThreadOrigin, ThreadRecord, ThreadSeed,
    ThreadView,
};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

const SCHEMA: &str = include_str!("schema.sql");
const SCHEMA_V1: &str = include_str!("schema-v1.sql");
const SCHEMA_V2: &str = include_str!("schema-v2.sql");
const SCHEMA_V3: &str = include_str!("schema-v3.sql");
const MIGRATION_V2: &str = include_str!("migration-v2.sql");
const MIGRATION_V3: &str = include_str!("migration-v3.sql");
const MIGRATION_V4: &str = include_str!("migration-v4.sql");
const SCHEMA_VERSION: i64 = 4;
// The existing six-table stopped-reader and backup formats remain schema3.
const LEGACY_SCHEMA_VERSION: i64 = 3;

struct OwnerLock(File);

impl Drop for OwnerLock {
    fn drop(&mut self) {
        // Closing our descriptor alone can leave flock held by an unrelated
        // child between fork and exec. Release this file description explicitly.
        let _ = FileExt::unlock(&self.0);
    }
}

/// A single synchronous connection protected by an exclusive data-directory lock.
/// The connection field is declared before the lock so it is dropped first.
pub struct SqliteStore {
    connection: Mutex<Connection>,
    _owner_lock: OwnerLock,
    owner_generation: u64,
    discovery_origin: b4_runtime::discovery::backend::DiscoveryOrigin,
    #[cfg(unix)]
    data_dir: PathBuf,
}

impl SqliteStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir).map_err(unavailable)?;
        #[cfg(unix)]
        let data_dir = std::fs::canonicalize(data_dir).map_err(unavailable)?;
        // Never truncate or unlink the lock: competing owners must lock one inode.
        let owner_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(data_dir.join("owner.lock"))
            .map_err(unavailable)?;
        owner_lock.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                StoreError::AlreadyOwned
            } else {
                unavailable(error)
            }
        })?;
        // Local drop order also closes the connection before releasing ownership
        // when configuring or initializing the database fails.
        let owner_lock = OwnerLock(owner_lock);

        let mut connection =
            Connection::open(data_dir.join("runtime.sqlite3")).map_err(unavailable)?;
        configure(&connection)?;
        let owner_generation = initialize(&mut connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            _owner_lock: owner_lock,
            owner_generation,
            discovery_origin: b4_runtime::discovery::backend::DiscoveryOrigin::default(),
            #[cfg(unix)]
            data_dir,
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(
            permission_connection_poisoned
                as fn(std::sync::PoisonError<std::sync::MutexGuard<'_, Connection>>) -> StoreError,
        )
    }

    fn admit_inner(
        &self,
        thread_id: &ThreadId,
        expected_resource: Option<(&ThreadGeneration, u64)>,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        validate_initial(&initial, self.owner_generation)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let thread = read_thread(&transaction, thread_id)?.ok_or_else(|| {
            if expected_resource.is_some() {
                StoreError::RevisionConflict
            } else {
                StoreError::ThreadNotFound
            }
        })?;
        if expected_resource.is_some_and(|(generation, revision)| {
            generation != &thread.generation || revision != thread.resource_revision
        }) {
            return Err(StoreError::RevisionConflict);
        }
        if read_execution(&transaction, &initial.execution_id)?.is_some() {
            return Err(StoreError::ExecutionExists);
        }
        let resource_revision = next_resource_revision(thread.resource_revision)?;
        if let Some(active) = thread.active_execution_id {
            let active = read_execution(&transaction, &active)?
                .ok_or_else(|| corrupt("missing active execution"))?;
            if !releases_slot(active.snapshot.status) {
                return Err(StoreError::ThreadBusy);
            }
        }
        transaction.execute(
            "INSERT INTO executions (id, thread_id, revision, owner_epoch, snapshot) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![initial.execution_id.as_str(), thread_id.as_str(), initial.revision.to_be_bytes().as_slice(),
                initial.owner_generation.to_be_bytes().as_slice(), serde_json::to_string(&initial).map_err(unavailable)?],
        ).map_err(unavailable)?;
        let changed = transaction
            .execute(
                "UPDATE threads SET active_execution_id = ?1, resource_revision = ?2 WHERE id = ?3 AND resource_revision = ?4 AND generation = ?5",
                params![initial.execution_id.as_str(), resource_revision.to_be_bytes().as_slice(), thread_id.as_str(), thread.resource_revision.to_be_bytes().as_slice(), thread.generation.as_str()],
            )
            .map_err(unavailable)?;
        if changed != 1 {
            return Err(corrupt("admission lost thread slot"));
        }
        append_journal(&transaction, "admit", thread_id, &initial)?;
        transaction.commit().map_err(unavailable)?;
        Ok(StoredExecution {
            thread_id: thread_id.clone(),
            snapshot: initial,
        })
    }
}

impl Store for SqliteStore {
    fn confinement_view_fenced(
        &self,
        target: &b4_runtime::RecoveryConfinementTarget,
    ) -> Result<ThreadView, b4_runtime::RecoveryConfinementBackendError> {
        confinement::capture(self, target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &b4_runtime::RecoveryConfinementTarget,
        reason: b4_runtime::RecoveryConfinementReason,
        fresh_turn: &b4_contract::TurnId,
        facts: TransitionFacts,
    ) -> Result<b4_runtime::RecoveryConfinementOutcome, b4_runtime::RecoveryConfinementBackendError>
    {
        confinement::mutate(self, original, target, reason, fresh_turn, facts)
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            kind: StoreKind::Sqlite,
            durable_runtime: true,
            fenced_cancellation: true,
            fenced_deletion: true,
        }
    }

    fn owner_generation(&self) -> u64 {
        self.owner_generation
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&b4_runtime::UnfinishedDiscoveryCursor>,
    ) -> Result<b4_runtime::UnfinishedDiscoveryPage, b4_runtime::DiscoveryError> {
        discovery::read(self, cursor)
    }

    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        validate_thread_seed(&seed)?;
        let generation = generate_thread_generation()?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        if read_thread(&transaction, &seed.id)?.is_some() {
            return Err(StoreError::ThreadExists);
        }
        let origin = seed
            .origin
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(unavailable)?;
        transaction.execute(
            "INSERT INTO threads (id, created_at_ms, metadata, origin, resource_revision, generation) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![seed.id.as_str(), seed.created_at_ms.to_be_bytes().as_slice(), serde_json::to_string(&seed.metadata).map_err(unavailable)?, origin, 0_u64.to_be_bytes().as_slice(), generation.as_str()],
        ).map_err(unavailable)?;
        transaction.execute(
            "INSERT INTO journal (kind, thread_id, owner_epoch, updated_at_ms) VALUES ('thread', ?1, ?2, ?3)",
            params![seed.id.as_str(), self.owner_generation.to_be_bytes().as_slice(), seed.created_at_ms.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
        transaction.commit().map_err(unavailable)?;
        Ok(ThreadRecord {
            id: seed.id,
            generation,
            active_execution_id: None,
            created_at_ms: seed.created_at_ms,
            metadata: seed.metadata,
            origin: seed.origin,
            resource_revision: 0,
        })
    }

    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        let connection = self.connection()?;
        read_thread(&connection, id)
    }

    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(unavailable)?;
        let Some(thread) = read_thread(&transaction, id)? else {
            return Ok(None);
        };
        let execution = thread
            .active_execution_id
            .as_ref()
            .map(|id| {
                read_execution(&transaction, id)?.ok_or_else(|| corrupt("missing active execution"))
            })
            .transpose()?;
        transaction.commit().map_err(unavailable)?;
        Ok(Some(ThreadView { thread, execution }))
    }

    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        let connection = self.connection()?;
        read_execution(&connection, id)
    }

    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let thread =
            read_thread(&transaction, &target.thread_id)?.ok_or(StoreError::RevisionConflict)?;
        let executions = read_deletion_executions(&transaction, &thread.id)?;
        let execution = thread
            .active_execution_id
            .as_ref()
            .map(|id| {
                executions
                    .iter()
                    .find(|execution| &execution.snapshot.execution_id == id)
                    .cloned()
                    .ok_or_else(|| corrupt("thread_deletion_active_missing"))
            })
            .transpose()?;
        let view = ThreadView { thread, execution };
        validate_thread_deletion(&view, target, &executions)?;
        validate_deletion_records(&transaction, &view.thread, &executions)?;

        // Clear the thread-to-execution pointer before deleting execution children
        // and the execution-to-thread rows. Every relation was checked above.
        let changed = transaction.execute(
            "UPDATE threads SET active_execution_id = NULL WHERE id = ?1 AND generation = ?2 AND resource_revision = ?3",
            params![target.thread_id.as_str(), target.thread_generation.as_str(), target.resource_revision.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        for table in [
            "approval_grants",
            "approval_acceptances",
            "journal",
            "executions",
        ] {
            transaction
                .execute(
                    &format!("DELETE FROM {table} WHERE thread_id = ?1"),
                    [target.thread_id.as_str()],
                )
                .map_err(unavailable)?;
        }
        let changed = transaction
            .execute(
                "DELETE FROM threads WHERE id = ?1 AND generation = ?2 AND resource_revision = ?3",
                params![
                    target.thread_id.as_str(),
                    target.thread_generation.as_str(),
                    target.resource_revision.to_be_bytes().as_slice()
                ],
            )
            .map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        transaction.commit().map_err(unavailable)
    }

    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.admit_inner(thread_id, None, initial)
    }

    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let thread =
            read_thread(&transaction, &target.thread_id)?.ok_or(StoreError::RevisionConflict)?;
        let execution = thread
            .active_execution_id
            .as_ref()
            .map(|id| {
                read_execution(&transaction, id)?
                    .ok_or_else(|| corrupt("run_cancellation_active_missing"))
            })
            .transpose()?;
        let view = ThreadView { thread, execution };
        let plan = validate_run_cancellation(&view, target, self.owner_generation, facts)?;
        let current = view.execution.as_ref().ok_or(StoreError::NoRunInFlight)?;
        validate_commit(
            &current.snapshot,
            target.execution_fence,
            &plan,
            self.owner_generation,
        )?;
        reject_legacy_permission_target(&transaction, current, &view.thread)?;
        let executions = read_deletion_executions(&transaction, &view.thread.id)?;
        validate_deletion_records(&transaction, &view.thread, &executions)?;
        let resource_revision = next_resource_revision(view.thread.resource_revision)?;
        let snapshot = plan.snapshot;
        let changed = transaction.execute(
            "UPDATE executions SET revision = ?1, owner_epoch = ?2, snapshot = ?3 WHERE id = ?4 AND thread_id = ?5 AND revision = ?6 AND owner_epoch = ?7",
            params![snapshot.revision.to_be_bytes().as_slice(), snapshot.owner_generation.to_be_bytes().as_slice(), serde_json::to_string(&snapshot).map_err(unavailable)?, target.execution_id.as_str(), target.thread_id.as_str(), target.execution_fence.revision.to_be_bytes().as_slice(), target.execution_fence.owner_generation.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        let changed = transaction.execute(
            "UPDATE threads SET resource_revision = ?1 WHERE id = ?2 AND generation = ?3 AND resource_revision = ?4 AND active_execution_id = ?5",
            params![resource_revision.to_be_bytes().as_slice(), target.thread_id.as_str(), target.thread_generation.as_str(), target.resource_revision.to_be_bytes().as_slice(), target.execution_id.as_str()],
        ).map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        // Cancellation voids only outstanding grants; consumed Once authority
        // and its acceptance audit remain unchanged and are never refunded.
        transaction.execute(
            "DELETE FROM approval_grants WHERE execution_id = ?1 AND thread_id = ?2 AND thread_generation = ?3 AND consumed = 0",
            params![target.execution_id.as_str(), target.thread_id.as_str(), target.thread_generation.as_str()],
        ).map_err(unavailable)?;
        append_journal(&transaction, "commit", &target.thread_id, &snapshot)?;
        transaction.commit().map_err(unavailable)?;
        Ok(StoredExecution {
            thread_id: target.thread_id.clone(),
            snapshot,
        })
    }

    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.admit_inner(
            thread_id,
            Some((expected_generation, expected_resource_revision)),
            initial,
        )
    }

    fn readiness(&self) -> Result<(), StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(unavailable)?;
        verify_schema(&transaction, SCHEMA_VERSION)?;
        let (version, stored_checksum, owner) = read_metadata(&transaction)?;
        if version != SCHEMA_VERSION || stored_checksum != checksum(SCHEMA) {
            return Err(StoreError::UnsupportedSchema);
        }
        if decode_u64(&owner)? != self.owner_generation {
            return Err(StoreError::OwnerConflict);
        }
        transaction.commit().map_err(unavailable)?;
        Ok(())
    }

    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let current =
            read_execution(&transaction, execution_id)?.ok_or(StoreError::ExecutionNotFound)?;
        let thread = read_thread(&transaction, &current.thread_id)?
            .ok_or_else(|| corrupt("execution thread missing"))?;
        if thread.active_execution_id.as_ref() != Some(execution_id) {
            return Err(StoreError::ExecutionNotActive);
        }
        let changed = validate_commit(&current.snapshot, fence, &plan, self.owner_generation)?;
        reject_legacy_permission_target(&transaction, &current, &thread)?;
        let grants = read_grants(&transaction, execution_id, &thread)?;
        reject_unrecorded_decisions(&current.snapshot, &plan.snapshot, &grants)?;
        if !changed {
            return Ok(current);
        }
        let resource_revision = next_resource_revision(thread.resource_revision)?;
        let snapshot = plan.snapshot;
        let changed = transaction.execute(
            "UPDATE executions SET revision = ?1, owner_epoch = ?2, snapshot = ?3 WHERE id = ?4 AND revision = ?5 AND owner_epoch = ?6",
            params![snapshot.revision.to_be_bytes().as_slice(), snapshot.owner_generation.to_be_bytes().as_slice(),
                serde_json::to_string(&snapshot).map_err(unavailable)?, execution_id.as_str(),
                fence.revision.to_be_bytes().as_slice(), fence.owner_generation.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        let changed = transaction.execute(
            "UPDATE threads SET resource_revision = ?1 WHERE id = ?2 AND resource_revision = ?3",
            params![resource_revision.to_be_bytes().as_slice(), current.thread_id.as_str(), thread.resource_revision.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict);
        }
        append_journal(&transaction, "commit", &current.thread_id, &snapshot)?;
        transaction.commit().map_err(unavailable)?;
        Ok(StoredExecution {
            thread_id: current.thread_id,
            snapshot,
        })
    }

    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal WHERE execution_id = ?1 ORDER BY sequence"
        ).map_err(unavailable)?;
        let mut rows = statement
            .query([execution_id.as_str()])
            .map_err(unavailable)?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next().map_err(unavailable)? {
            let id: String = row.get(0).map_err(corrupt)?;
            let revision: Vec<u8> = row.get(1).map_err(corrupt)?;
            let owner: Vec<u8> = row.get(2).map_err(corrupt)?;
            let status: String = row.get(3).map_err(corrupt)?;
            let updated_at: Vec<u8> = row.get(4).map_err(corrupt)?;
            let payload: String = row.get(5).map_err(corrupt)?;
            let entry: JournalEntry = serde_json::from_str(&payload).map_err(corrupt)?;
            if entry.execution_id.as_str() != id
                || &entry.execution_id != execution_id
                || entry.revision != decode_u64(&revision)?
                || entry.owner_generation != decode_u64(&owner)?
                || entry.updated_at_ms != decode_u64(&updated_at)?
                || serde_json::to_string(&entry.status).map_err(corrupt)? != status
            {
                return Err(corrupt("journal index and payload disagree"));
            }
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl ApprovalStore for SqliteStore {
    fn approval_durable(&self) -> bool {
        true
    }

    fn approval_view(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Option<ApprovalView>, ApprovalError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(unavailable)?;
        let view = read_approval_view(&transaction, execution_id)?;
        transaction.commit().map_err(unavailable)?;
        Ok(view)
    }

    fn commit_approval(
        &self,
        execution_id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let view =
            read_approval_view(&transaction, execution_id)?.ok_or(StoreError::ExecutionNotFound)?;
        let changes =
            validate_approval_change(&view, fence, &plan, &mutation, self.owner_generation)?;
        let resource_revision = next_resource_revision(view.thread.resource_revision)?;
        let snapshot = plan.snapshot;
        let changed = transaction.execute(
            "UPDATE executions SET revision = ?1, owner_epoch = ?2, snapshot = ?3 WHERE id = ?4 AND revision = ?5 AND owner_epoch = ?6",
            params![snapshot.revision.to_be_bytes().as_slice(), snapshot.owner_generation.to_be_bytes().as_slice(),
                serde_json::to_string(&snapshot).map_err(unavailable)?, execution_id.as_str(),
                fence.execution.revision.to_be_bytes().as_slice(), fence.execution.owner_generation.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict.into());
        }
        let changed = transaction.execute(
            "UPDATE threads SET resource_revision = ?1 WHERE id = ?2 AND resource_revision = ?3",
            params![resource_revision.to_be_bytes().as_slice(), view.thread.id.as_str(), fence.resource_revision.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
        if changed != 1 {
            return Err(StoreError::RevisionConflict.into());
        }
        for grant in changes.grants {
            write_grant(&transaction, &grant)?;
        }
        if let Some(acceptance) = changes.acceptance {
            write_acceptance(&transaction, &acceptance)?;
        }
        append_journal(&transaction, "commit", &view.thread.id, &snapshot)?;
        transaction.commit().map_err(unavailable)?;
        Ok(StoredExecution {
            thread_id: view.thread.id,
            snapshot,
        })
    }
}

fn read_deletion_executions(
    connection: &Connection,
    thread_id: &ThreadId,
) -> Result<Vec<StoredExecution>, StoreError> {
    let mut statement = connection
        .prepare("SELECT id FROM executions WHERE thread_id = ?1 ORDER BY id")
        .map_err(|_| corrupt("thread_deletion_execution_index"))?;
    let ids = statement
        .query_map([thread_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(|_| corrupt("thread_deletion_execution_index"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| corrupt("thread_deletion_execution_index"))?;
    ids.into_iter()
        .map(|id| {
            let id =
                ExecutionId::new(id).map_err(|_| corrupt("thread_deletion_execution_identity"))?;
            read_execution(connection, &id)?
                .ok_or_else(|| corrupt("thread_deletion_execution_missing"))
        })
        .collect()
}

/// Both indexes matter: a foreign execution can have a dependent row that claims
/// this thread, and an owned execution can have a row that claims another thread.
/// Reject either relationship before cleanup can touch any row.
fn validate_deletion_records(
    connection: &Connection,
    thread: &ThreadRecord,
    executions: &[StoredExecution],
) -> Result<(), StoreError> {
    let foreign_pointer: bool = connection.query_row(
        "SELECT EXISTS (SELECT 1 FROM threads t JOIN executions e ON e.id = t.active_execution_id WHERE e.thread_id = ?1 AND t.id != ?1)",
        [thread.id.as_str()], |row| row.get(0),
    ).map_err(|_| corrupt("thread_deletion_active_relationship"))?;
    if foreign_pointer {
        return Err(corrupt("thread_deletion_active_relationship"));
    }
    for table in ["journal", "approval_grants", "approval_acceptances"] {
        let missing_execution = if table == "journal" {
            "(d.execution_id IS NOT NULL AND (e.thread_id IS NULL OR e.thread_id != ?1))"
        } else {
            "(e.thread_id IS NULL OR e.thread_id != ?1)"
        };
        let mismatch: bool = connection.query_row(
            &format!("SELECT EXISTS (SELECT 1 FROM {table} d LEFT JOIN executions e ON e.id = d.execution_id WHERE (d.thread_id = ?1 OR e.thread_id = ?1) AND (d.thread_id != ?1 OR {missing_execution}))"),
            [thread.id.as_str()], |row| row.get(0),
        ).map_err(|_| corrupt("thread_deletion_dependent_relationship"))?;
        if mismatch {
            return Err(corrupt("thread_deletion_dependent_relationship"));
        }
    }
    validate_deletion_journal(connection, thread, executions)?;
    for execution in executions {
        let id = &execution.snapshot.execution_id;
        let grants = read_grants(connection, id, thread)?;
        let acceptances = read_acceptances(connection, id, thread)?;
        if grants.iter().any(|grant| {
            thread.origin.as_ref() != Some(&grant.origin)
                || grant.route != execution.snapshot.definition.route
                || grant.definition_revision != execution.snapshot.definition.revision
                || grant.task_parking_revision > execution.snapshot.revision
                || !execution.snapshot.tasks.iter().any(|task| {
                    task.spec.id == grant.task_id && task.spec.effect_id == grant.effect_id
                })
        }) || acceptances.iter().any(|acceptance| {
            thread.origin.as_ref() != Some(&acceptance.context.actor)
                || acceptance.definition_revision != execution.snapshot.definition.revision
                || acceptance.previous_revision >= execution.snapshot.revision
                || !execution
                    .snapshot
                    .turn_history
                    .contains(&acceptance.turn_id)
                || acceptance.decisions.iter().any(|decision| {
                    !execution
                        .snapshot
                        .tasks
                        .iter()
                        .any(|task| task.spec.id == decision.task_id)
                })
        }) {
            return Err(corrupt("thread_deletion_authority_identity"));
        }
    }
    Ok(())
}

fn validate_deletion_journal(
    connection: &Connection,
    thread: &ThreadRecord,
    executions: &[StoredExecution],
) -> Result<(), StoreError> {
    let invalid = || corrupt("thread_deletion_journal_record");
    let mut statement = connection.prepare(
        "SELECT kind, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal WHERE thread_id = ?1 ORDER BY sequence",
    ).map_err(|_| invalid())?;
    let mut rows = statement
        .query([thread.id.as_str()])
        .map_err(|_| invalid())?;
    let mut creation_count = 0;
    let mut entries = std::collections::BTreeMap::<ExecutionId, Vec<JournalEntry>>::new();
    while let Some(row) = rows.next().map_err(|_| invalid())? {
        let kind: String = row.get(0).map_err(|_| invalid())?;
        let execution_id: Option<String> = row.get(1).map_err(|_| invalid())?;
        let revision: Option<Vec<u8>> = row.get(2).map_err(|_| invalid())?;
        let owner: Vec<u8> = row.get(3).map_err(|_| invalid())?;
        let status: Option<String> = row.get(4).map_err(|_| invalid())?;
        let updated: Vec<u8> = row.get(5).map_err(|_| invalid())?;
        let payload: Option<String> = row.get(6).map_err(|_| invalid())?;
        let owner = decode_u64(&owner).map_err(|_| invalid())?;
        let updated = decode_u64(&updated).map_err(|_| invalid())?;
        if owner == 0 {
            return Err(invalid());
        }
        if kind == "thread" {
            if execution_id.is_some()
                || revision.is_some()
                || status.is_some()
                || payload.is_some()
                || updated != thread.created_at_ms
            {
                return Err(invalid());
            }
            creation_count += 1;
            continue;
        }
        if !matches!(kind.as_str(), "admit" | "commit") {
            return Err(invalid());
        }
        let id = ExecutionId::new(execution_id.ok_or_else(invalid)?).map_err(|_| invalid())?;
        let execution = executions
            .iter()
            .find(|execution| execution.snapshot.execution_id == id)
            .ok_or_else(invalid)?;
        let revision = decode_u64(&revision.ok_or_else(invalid)?).map_err(|_| invalid())?;
        let entry: JournalEntry =
            serde_json::from_str(&payload.ok_or_else(invalid)?).map_err(|_| invalid())?;
        if entry.execution_id != id
            || entry.revision != revision
            || entry.owner_generation != owner
            || entry.updated_at_ms != updated
            || Some(serde_json::to_string(&entry.status).map_err(|_| invalid())?) != status
            || revision > execution.snapshot.revision
            || owner > execution.snapshot.owner_generation
            || updated > execution.snapshot.updated_at_ms
            || (kind == "admit" && revision != 0)
            || (kind == "commit" && revision == 0)
        {
            return Err(invalid());
        }
        entries.entry(id).or_default().push(entry);
    }
    if creation_count != 1 || entries.len() != executions.len() {
        return Err(invalid());
    }
    for execution in executions {
        let journal = entries
            .get(&execution.snapshot.execution_id)
            .ok_or_else(invalid)?;
        if journal.first().is_none_or(|entry| entry.revision != 0)
            || journal.last() != Some(&JournalEntry::from(&execution.snapshot))
            || journal.windows(2).any(|pair| {
                pair[0].revision.checked_add(1) != Some(pair[1].revision)
                    || pair[0].owner_generation > pair[1].owner_generation
                    || pair[0].updated_at_ms > pair[1].updated_at_ms
            })
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn read_approval_view(
    connection: &Connection,
    execution_id: &ExecutionId,
) -> Result<Option<ApprovalView>, StoreError> {
    let Some(execution) = read_execution(connection, execution_id)? else {
        return Ok(None);
    };
    let thread = read_thread(connection, &execution.thread_id)?
        .ok_or_else(|| corrupt("execution thread missing"))?;
    reject_legacy_permission_target(connection, &execution, &thread)?;
    let grants = read_grants(connection, execution_id, &thread)?;
    let acceptances = read_acceptances(connection, execution_id, &thread)?;
    Ok(Some(ApprovalView {
        thread,
        execution,
        grants,
        acceptances,
    }))
}

fn read_grants(
    connection: &Connection,
    execution_id: &ExecutionId,
    thread: &ThreadRecord,
) -> Result<Vec<GrantRecord>, StoreError> {
    let invalid = || corrupt("invalid_approval_grant");
    let mut statement = connection.prepare(
        "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants WHERE execution_id = ?1 ORDER BY interrupt_id"
    ).map_err(|_| invalid())?;
    let mut rows = statement
        .query([execution_id.as_str()])
        .map_err(|_| invalid())?;
    let mut grants = Vec::new();
    while let Some(row) = rows.next().map_err(|_| invalid())? {
        let execution: String = row.get(0).map_err(|_| invalid())?;
        let interrupt: String = row.get(1).map_err(|_| invalid())?;
        let task: String = row.get(2).map_err(|_| invalid())?;
        let thread_id: String = row.get(3).map_err(|_| invalid())?;
        let generation: String = row.get(4).map_err(|_| invalid())?;
        let parking_revision: Vec<u8> = row.get(5).map_err(|_| invalid())?;
        let consumed: i64 = row.get(6).map_err(|_| invalid())?;
        let payload: String = row.get(7).map_err(|_| invalid())?;
        let grant: GrantRecord = serde_json::from_str(&payload).map_err(|_| invalid())?;
        validate_grant_record(&grant).map_err(|_| invalid())?;
        if grant.execution_id.as_str() != execution
            || &grant.execution_id != execution_id
            || grant.interrupt_id.as_str() != interrupt
            || grant.task_id.as_str() != task
            || grant.thread_id.as_str() != thread_id
            || grant.thread_id != thread.id
            || grant.thread_generation.as_str() != generation
            || grant.thread_generation != thread.generation
            || grant.task_parking_revision
                != decode_u64(&parking_revision).map_err(|_| invalid())?
            || i64::from(grant.consumed) != consumed
        {
            return Err(invalid());
        }
        grants.push(grant);
    }
    Ok(grants)
}

fn read_acceptances(
    connection: &Connection,
    execution_id: &ExecutionId,
    thread: &ThreadRecord,
) -> Result<Vec<ApprovalAcceptance>, StoreError> {
    let invalid = || corrupt("invalid_approval_acceptance");
    let mut statement = connection.prepare(
        "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances WHERE execution_id = ?1 ORDER BY previous_revision, turn_id"
    ).map_err(|_| invalid())?;
    let mut rows = statement
        .query([execution_id.as_str()])
        .map_err(|_| invalid())?;
    let mut acceptances = Vec::new();
    while let Some(row) = rows.next().map_err(|_| invalid())? {
        let execution: String = row.get(0).map_err(|_| invalid())?;
        let turn: String = row.get(1).map_err(|_| invalid())?;
        let thread_id: String = row.get(2).map_err(|_| invalid())?;
        let generation: String = row.get(3).map_err(|_| invalid())?;
        let revision: Vec<u8> = row.get(4).map_err(|_| invalid())?;
        let definition_revision: String = row.get(5).map_err(|_| invalid())?;
        let accepted_at: Vec<u8> = row.get(6).map_err(|_| invalid())?;
        let payload: String = row.get(7).map_err(|_| invalid())?;
        let acceptance: ApprovalAcceptance =
            serde_json::from_str(&payload).map_err(|_| invalid())?;
        validate_acceptance_record(&acceptance).map_err(|_| invalid())?;
        if acceptance.execution_id.as_str() != execution
            || &acceptance.execution_id != execution_id
            || acceptance.turn_id.as_str() != turn
            || acceptance.thread_id.as_str() != thread_id
            || acceptance.thread_id != thread.id
            || acceptance.thread_generation.as_str() != generation
            || acceptance.thread_generation != thread.generation
            || acceptance.previous_revision != decode_u64(&revision).map_err(|_| invalid())?
            || acceptance.definition_revision.as_str() != definition_revision
            || acceptance.accepted_at_ms != decode_u64(&accepted_at).map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        acceptances.push(acceptance);
    }
    Ok(acceptances)
}

fn write_grant(connection: &Connection, grant: &GrantRecord) -> Result<(), StoreError> {
    let record =
        serde_json::to_string(grant).map_err(|_| unavailable("approval grant encoding failed"))?;
    connection.execute(
        "INSERT INTO approval_grants (execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT (execution_id, interrupt_id) DO UPDATE SET task_id = excluded.task_id, thread_id = excluded.thread_id, thread_generation = excluded.thread_generation, task_parking_revision = excluded.task_parking_revision, consumed = excluded.consumed, record = excluded.record",
        params![grant.execution_id.as_str(), grant.interrupt_id.as_str(), grant.task_id.as_str(), grant.thread_id.as_str(), grant.thread_generation.as_str(), grant.task_parking_revision.to_be_bytes().as_slice(), i64::from(grant.consumed), record],
    ).map_err(|_| unavailable("approval grant write failed"))?;
    Ok(())
}

fn write_acceptance(
    connection: &Connection,
    acceptance: &ApprovalAcceptance,
) -> Result<(), StoreError> {
    let record = serde_json::to_string(acceptance)
        .map_err(|_| unavailable("approval acceptance encoding failed"))?;
    connection.execute(
        "INSERT INTO approval_acceptances (execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![acceptance.execution_id.as_str(), acceptance.turn_id.as_str(), acceptance.thread_id.as_str(), acceptance.thread_generation.as_str(), acceptance.previous_revision.to_be_bytes().as_slice(), acceptance.definition_revision.as_str(), acceptance.accepted_at_ms.to_be_bytes().as_slice(), record],
    ).map_err(|_| unavailable("approval acceptance write failed"))?;
    Ok(())
}

fn unavailable(error: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable(error.to_string())
}
fn corrupt(error: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt(error.to_string())
}

fn decode_u64(bytes: &[u8]) -> Result<u64, StoreError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| corrupt("invalid unsigned integer encoding"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn configure(connection: &Connection) -> Result<(), StoreError> {
    connection
        .busy_timeout(Duration::from_millis(5000))
        .map_err(unavailable)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(unavailable)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(unavailable)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(unavailable)?;
    let journal: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(unavailable)?;
    let sync: i64 = connection
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .map_err(unavailable)?;
    let foreign_keys: i64 = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get(0))
        .map_err(unavailable)?;
    let timeout: i64 = connection
        .pragma_query_value(None, "busy_timeout", |row| row.get(0))
        .map_err(unavailable)?;
    if !journal.eq_ignore_ascii_case("wal") || sync != 2 || foreign_keys != 1 || timeout != 5000 {
        return Err(unavailable(
            "required SQLite durability settings unavailable",
        ));
    }
    Ok(())
}

fn initialize(connection: &mut Connection) -> Result<u64, StoreError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(unavailable)?;
    let table_count: i64 = transaction
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
            [],
            |row| row.get(0),
        )
        .map_err(unavailable)?;
    let current_checksum = checksum(SCHEMA);
    if table_count == 0 {
        transaction.execute_batch(SCHEMA).map_err(unavailable)?;
        transaction.execute(
            "INSERT INTO metadata (id, schema_version, migration_checksum, owner_epoch) VALUES (1, ?1, ?2, ?3)",
            params![SCHEMA_VERSION, current_checksum, 0_u64.to_be_bytes().as_slice()],
        ).map_err(unavailable)?;
    }
    let (version, stored_checksum, owner) = read_metadata(&transaction)?;
    let schema = match version {
        1 => SCHEMA_V1,
        2 => SCHEMA_V2,
        3 => SCHEMA_V3,
        4 => SCHEMA,
        _ => return Err(StoreError::UnsupportedSchema),
    };
    verify_schema(&transaction, version)?;
    if stored_checksum != checksum(schema) {
        return Err(StoreError::UnsupportedSchema);
    }
    let mut stage = version;
    if stage == 1 {
        transaction
            .execute_batch(MIGRATION_V2)
            .map_err(unavailable)?;
        stage = 2;
    }
    if stage == 2 {
        transaction
            .execute_batch(MIGRATION_V3)
            .map_err(unavailable)?;
        let thread_ids: Vec<String> = {
            let mut statement = transaction
                .prepare("SELECT id FROM threads ORDER BY id")
                .map_err(unavailable)?;
            let ids = statement
                .query_map([], |row| row.get(0))
                .map_err(unavailable)?
                .collect::<Result<_, _>>()
                .map_err(unavailable)?;
            ids
        };
        for id in thread_ids {
            let generation = generate_thread_generation()?;
            transaction
                .execute(
                    "UPDATE threads SET generation = ?1 WHERE id = ?2",
                    params![generation.as_str(), id],
                )
                .map_err(unavailable)?;
        }
        stage = 3;
    }
    if stage == 3 {
        transaction
            .execute_batch(MIGRATION_V4)
            .map_err(unavailable)?;
        stage = 4;
    }
    if stage != SCHEMA_VERSION {
        return Err(StoreError::UnsupportedSchema);
    }
    if version < SCHEMA_VERSION {
        transaction
            .execute(
                "UPDATE metadata SET schema_version = ?1, migration_checksum = ?2 WHERE id = 1",
                params![SCHEMA_VERSION, current_checksum],
            )
            .map_err(unavailable)?;
        verify_schema(&transaction, SCHEMA_VERSION)?;
    }
    let owner_generation = decode_u64(&owner)?
        .checked_add(1)
        .ok_or_else(|| unavailable("owner generation exhausted"))?;
    let changed = transaction
        .execute(
            "UPDATE metadata SET owner_epoch = ?1 WHERE id = 1 AND owner_epoch = ?2",
            params![owner_generation.to_be_bytes().as_slice(), owner],
        )
        .map_err(unavailable)?;
    if changed != 1 {
        return Err(corrupt("owner epoch compare-and-swap failed"));
    }
    transaction.commit().map_err(unavailable)?;
    Ok(owner_generation)
}

fn checksum(schema: &str) -> String {
    format!("{:x}", Sha256::digest(schema.as_bytes()))
}

fn read_metadata(connection: &Connection) -> Result<(i64, String, Vec<u8>), StoreError> {
    connection
        .query_row(
            "SELECT schema_version, migration_checksum, owner_epoch FROM metadata WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|_| StoreError::UnsupportedSchema)
}

type SchemaObject = (String, String, String, Option<String>);

fn schema_objects(connection: &Connection) -> Result<Vec<SchemaObject>, StoreError> {
    let mut statement = connection
        .prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name")
        .map_err(|_| StoreError::UnsupportedSchema)?;
    let objects = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .map_err(|_| StoreError::UnsupportedSchema)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| StoreError::UnsupportedSchema)?;
    Ok(objects)
}

fn verify_schema(connection: &Connection, version: i64) -> Result<(), StoreError> {
    let expected = Connection::open_in_memory().map_err(unavailable)?;
    expected.execute_batch(SCHEMA_V1).map_err(unavailable)?;
    match version {
        1 => {}
        2 => expected.execute_batch(MIGRATION_V2).map_err(unavailable)?,
        3 => {
            expected.execute_batch(MIGRATION_V2).map_err(unavailable)?;
            expected.execute_batch(MIGRATION_V3).map_err(unavailable)?;
        }
        4 => {
            expected.execute_batch(MIGRATION_V2).map_err(unavailable)?;
            expected.execute_batch(MIGRATION_V3).map_err(unavailable)?;
            expected.execute_batch(MIGRATION_V4).map_err(unavailable)?;
        }
        _ => return Err(StoreError::UnsupportedSchema),
    }
    if schema_objects(connection)? != schema_objects(&expected)? {
        return Err(StoreError::UnsupportedSchema);
    }
    Ok(())
}

fn read_thread(connection: &Connection, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
    let row = connection
        .query_row(
            "SELECT active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads WHERE id = ?1",
            [id.as_str()],
            |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?, row.get::<_, Vec<u8>>(4)?, row.get::<_, String>(5)?)),
        )
        .optional()
        .map_err(corrupt)?;
    row.map(
        |(active, created_at, metadata, origin, revision, generation)| {
            let active_execution_id = active.map(ExecutionId::new).transpose().map_err(corrupt)?;
            if let Some(active_id) = &active_execution_id {
                let execution = read_execution(connection, active_id)?
                    .ok_or_else(|| corrupt("missing active execution"))?;
                if &execution.thread_id != id {
                    return Err(corrupt("active execution belongs to another thread"));
                }
            }
            let seed = ThreadSeed {
                id: id.clone(),
                created_at_ms: decode_u64(&created_at)?,
                metadata: serde_json::from_str(&metadata).map_err(corrupt)?,
                origin: origin
                    .map(|origin| serde_json::from_str::<ThreadOrigin>(&origin))
                    .transpose()
                    .map_err(corrupt)?,
            };
            validate_thread_seed(&seed).map_err(corrupt)?;
            Ok(ThreadRecord {
                id: id.clone(),
                generation: ThreadGeneration::new(generation).map_err(corrupt)?,
                active_execution_id,
                created_at_ms: seed.created_at_ms,
                metadata: seed.metadata,
                origin: seed.origin,
                resource_revision: decode_u64(&revision)?,
            })
        },
    )
    .transpose()
}

fn read_execution(
    connection: &Connection,
    id: &ExecutionId,
) -> Result<Option<StoredExecution>, StoreError> {
    let row = connection
        .query_row(
            "SELECT thread_id, revision, owner_epoch, snapshot FROM executions WHERE id = ?1",
            [id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(corrupt)?;
    row.map(|(thread_id, revision, owner, payload)| {
        let thread_id = ThreadId::new(thread_id).map_err(corrupt)?;
        let thread_exists: bool = connection
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM threads WHERE id = ?1)",
                [thread_id.as_str()],
                |row| row.get(0),
            )
            .map_err(corrupt)?;
        if !thread_exists {
            return Err(corrupt("execution thread missing"));
        }
        let snapshot: Snapshot = serde_json::from_str(&payload).map_err(corrupt)?;
        b4_kernel::validate_snapshot(&snapshot).map_err(corrupt)?;
        if &snapshot.execution_id != id
            || snapshot.revision != decode_u64(&revision)?
            || snapshot.owner_generation != decode_u64(&owner)?
        {
            return Err(corrupt("execution index and payload disagree"));
        }
        Ok(StoredExecution {
            thread_id,
            snapshot,
        })
    })
    .transpose()
}

fn append_journal(
    connection: &Connection,
    kind: &str,
    thread_id: &ThreadId,
    snapshot: &Snapshot,
) -> Result<(), StoreError> {
    let entry = JournalEntry::from(snapshot);
    connection.execute(
        "INSERT INTO journal (kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![kind, thread_id.as_str(), snapshot.execution_id.as_str(), snapshot.revision.to_be_bytes().as_slice(),
            snapshot.owner_generation.to_be_bytes().as_slice(), serde_json::to_string(&snapshot.status).map_err(unavailable)?,
            snapshot.updated_at_ms.to_be_bytes().as_slice(), serde_json::to_string(&entry).map_err(unavailable)?],
    ).map_err(unavailable)?;
    Ok(())
}

// The old approval path cannot interpret persistent permission provenance.
// Inspect every bounded scalar range before refusing a target A/U profile;
// unrelated thread-wide decisions alone do not make an execution a target.
const LEGACY_PERMISSION_ACCEPTANCES: &str = "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_acceptances WHERE execution_id = ?1 ORDER BY previous_revision, turn_id LIMIT 65";
const LEGACY_PERMISSION_DECISIONS: &str = "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_decisions WHERE thread_id = ?1 ORDER BY permission_id LIMIT 65";
const LEGACY_PERMISSION_USES: &str = "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, typeof(record), length(CAST(record AS BLOB)) FROM permission_uses WHERE execution_id = ?1 ORDER BY task_id LIMIT 65";

fn legacy_permission_invalid() -> StoreError {
    corrupt("legacy_permission_header_invalid")
}
fn legacy_permission_unavailable() -> StoreError {
    unavailable("legacy_permission_header_unavailable")
}
fn legacy_permission_cell<'a>(
    row: &'a rusqlite::Row<'_>,
    index: usize,
) -> Result<rusqlite::types::ValueRef<'a>, StoreError> {
    row.get_ref(index).map_err(|_| legacy_permission_invalid())
}
fn legacy_permission_text<'a>(
    row: &'a rusqlite::Row<'_>,
    index: usize,
    maximum: usize,
) -> Result<&'a str, StoreError> {
    let rusqlite::types::ValueRef::Text(raw) = legacy_permission_cell(row, index)? else {
        return Err(legacy_permission_invalid());
    };
    if raw.is_empty() || raw.len() > maximum {
        return Err(legacy_permission_invalid());
    }
    let text = std::str::from_utf8(raw).map_err(|_| legacy_permission_invalid())?;
    if text.chars().any(char::is_control) {
        return Err(legacy_permission_invalid());
    }
    Ok(text)
}
fn legacy_permission_hex<'a>(
    row: &'a rusqlite::Row<'_>,
    index: usize,
) -> Result<&'a str, StoreError> {
    let text = legacy_permission_text(row, index, 64)?;
    if text.len() != 64
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(legacy_permission_invalid());
    }
    Ok(text)
}
fn legacy_permission_identity(
    row: &rusqlite::Row<'_>,
    index: usize,
    prefix: &str,
) -> Result<(), StoreError> {
    let text = legacy_permission_text(row, index, 69)?;
    if text.len() != 69
        || !text.starts_with(prefix)
        || !text.as_bytes()[5..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(legacy_permission_invalid());
    }
    Ok(())
}
fn legacy_permission_u64(row: &rusqlite::Row<'_>, index: usize) -> Result<u64, StoreError> {
    let rusqlite::types::ValueRef::Blob(raw) = legacy_permission_cell(row, index)? else {
        return Err(legacy_permission_invalid());
    };
    let bytes: [u8; 8] = raw.try_into().map_err(|_| legacy_permission_invalid())?;
    Ok(u64::from_be_bytes(bytes))
}
fn legacy_permission_record_header(
    row: &rusqlite::Row<'_>,
    version: usize,
    maximum: usize,
) -> Result<(), StoreError> {
    if !matches!(
        legacy_permission_cell(row, version)?,
        rusqlite::types::ValueRef::Integer(1)
    ) || !matches!(
        legacy_permission_cell(row, version + 1)?,
        rusqlite::types::ValueRef::Text(b"text")
    ) {
        return Err(legacy_permission_invalid());
    }
    let rusqlite::types::ValueRef::Integer(raw_length) = legacy_permission_cell(row, version + 2)?
    else {
        return Err(legacy_permission_invalid());
    };
    let length = usize::try_from(raw_length).map_err(|_| legacy_permission_invalid())?;
    if length == 0 {
        return Err(legacy_permission_invalid());
    }
    if length > maximum {
        return Err(StoreError::UnsupportedOperation);
    }
    Ok(())
}
fn reject_legacy_permission_target(
    connection: &Connection,
    execution: &StoredExecution,
    thread: &ThreadRecord,
) -> Result<(), StoreError> {
    let id = execution.snapshot.execution_id.as_str();
    let mut target = false;
    for (group, sql, key) in [
        (0_u8, LEGACY_PERMISSION_ACCEPTANCES, id),
        (1, LEGACY_PERMISSION_DECISIONS, thread.id.as_str()),
        (2, LEGACY_PERMISSION_USES, id),
    ] {
        let mut statement = connection
            .prepare(sql)
            .map_err(|_| legacy_permission_unavailable())?;
        let mut rows = statement
            .query([key])
            .map_err(|_| legacy_permission_unavailable())?;
        let mut count = 0;
        while let Some(row) = rows.next().map_err(|_| legacy_permission_unavailable())? {
            if count == 64 {
                return Err(StoreError::UnsupportedOperation);
            }
            count += 1;
            let (thread_column, generation_column, version_column, maximum) = match group {
                0 => (2, 3, 6, 65_536),
                1 => (1, 2, 4, 65_536),
                _ => (3, 4, 8, 16_384),
            };
            if legacy_permission_text(row, thread_column, 256)? != thread.id.as_str()
                || legacy_permission_hex(row, generation_column)? != thread.generation.as_str()
            {
                return Err(legacy_permission_invalid());
            }
            legacy_permission_record_header(row, version_column, maximum)?;
            match group {
                0 => {
                    if legacy_permission_text(row, 0, 256)? != id {
                        return Err(legacy_permission_invalid());
                    }
                    legacy_permission_text(row, 1, 256)?;
                    let previous = legacy_permission_u64(row, 4)?;
                    let committed = legacy_permission_u64(row, 5)?;
                    if previous.checked_add(1) != Some(committed)
                        || committed > execution.snapshot.revision
                    {
                        return Err(legacy_permission_invalid());
                    }
                    target = true;
                }
                1 => {
                    legacy_permission_identity(row, 0, "b4pd_")?;
                    legacy_permission_hex(row, 3)?;
                }
                _ => {
                    if legacy_permission_text(row, 0, 256)? != id {
                        return Err(legacy_permission_invalid());
                    }
                    legacy_permission_text(row, 1, 256)?;
                    legacy_permission_identity(row, 2, "b4pu_")?;
                    legacy_permission_identity(row, 5, "b4pd_")?;
                    legacy_permission_text(row, 6, 256)?;
                    if legacy_permission_u64(row, 7)? > execution.snapshot.revision {
                        return Err(legacy_permission_invalid());
                    }
                    target = true;
                }
            }
        }
    }
    if target {
        return Err(StoreError::UnsupportedOperation);
    }
    Ok(())
}

const _: () = assert!(LEGACY_PERMISSION_ACCEPTANCES.len() <= 1536);
const _: () = assert!(LEGACY_PERMISSION_DECISIONS.len() <= 1536);
const _: () = assert!(LEGACY_PERMISSION_USES.len() <= 1536);
const _: () = assert!(
    std::mem::size_of::<
        fn(
            std::sync::PoisonError<std::sync::MutexGuard<'static, rusqlite::Connection>>,
        ) -> b4_runtime::StoreError,
    >() <= 8
);
const _: () = assert!(
    std::mem::size_of::<(
        std::sync::PoisonError<std::sync::MutexGuard<'static, rusqlite::Connection>>,
    )>() <= 256
);

fn permission_connection_poisoned(
    _: std::sync::PoisonError<std::sync::MutexGuard<'_, Connection>>,
) -> StoreError {
    StoreError::Unavailable("connection mutex poisoned".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn closed_store_releases_lock_even_with_an_inherited_file_description() {
        let directory =
            std::env::temp_dir().join(format!("b4-sqlite-inherited-lock-{}", std::process::id()));
        let store = SqliteStore::open(&directory).unwrap();
        // A duplicate shares the open file description just as an unrelated
        // child inherits it between fork and exec, before CLOEXEC takes effect.
        let inherited = store._owner_lock.0.try_clone().unwrap();
        assert!(matches!(
            SqliteStore::open(&directory),
            Err(StoreError::AlreadyOwned)
        ));
        drop(store);
        let successor = SqliteStore::open(&directory).unwrap();
        assert_eq!(successor.owner_generation(), 2);
        drop(inherited);
        assert!(matches!(
            SqliteStore::open(&directory),
            Err(StoreError::AlreadyOwned)
        ));
        drop(successor);
        let next = SqliteStore::open(&directory).unwrap();
        assert_eq!(next.owner_generation(), 3);
        drop(next);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn live_connection_has_required_durability_settings() {
        let directory =
            std::env::temp_dir().join(format!("b4-sqlite-pragmas-{}", std::process::id()));
        let store = SqliteStore::open(&directory).unwrap();
        {
            let connection = store.connection().unwrap();
            let journal: String = connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .unwrap();
            let sync: i64 = connection
                .pragma_query_value(None, "synchronous", |row| row.get(0))
                .unwrap();
            let foreign_keys: i64 = connection
                .pragma_query_value(None, "foreign_keys", |row| row.get(0))
                .unwrap();
            let timeout: i64 = connection
                .pragma_query_value(None, "busy_timeout", |row| row.get(0))
                .unwrap();
            assert_eq!(journal, "wal");
            assert_eq!(sync, 2);
            assert_eq!(foreign_keys, 1);
            assert_eq!(timeout, 5000);
        }
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
