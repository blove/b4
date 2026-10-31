use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::{params, types::Value as SqlValue, Connection};
use serde_json::{json, Map, Value};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::{
    collections::BTreeMap,
    fs::{DirBuilder, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
pub const SNAPSHOT_BYTES: usize = 1_048_576;
pub const THREAD_BYTES: usize = 262_144;
pub type RawTables = BTreeMap<&'static str, Vec<Vec<SqlValue>>>;
pub struct Directory(PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-backend-confinement-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = DirBuilder::new();
        #[cfg(unix)]
        options.mode(0o700);
        options.create(&path).unwrap();
        for name in ["owner.lock", "runtime.sqlite3"] {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            drop(options.open(path.join(name)).unwrap());
        }
        #[cfg(unix)]
        {
            assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o700);
            for name in ["owner.lock", "runtime.sqlite3"] {
                assert_eq!(
                    path.join(name).metadata().unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }
        Self(path)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
    pub fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
    pub fn rows(&self) -> RawTables {
        let connection = self.connection();
        [
            "metadata",
            "threads",
            "executions",
            "journal",
            "approval_grants",
            "approval_acceptances",
        ]
        .into_iter()
        .map(|name| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {name} ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            let values = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| row.get(index))
                        .collect::<rusqlite::Result<Vec<SqlValue>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (name, values)
        })
        .collect()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn thread_id() -> ThreadId {
    ThreadId::new("backend-confinement-thread").unwrap()
}
pub fn execution_id() -> ExecutionId {
    ExecutionId::new("backend-confinement-execution").unwrap()
}
pub fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "backend-fixture".into(),
            tenant: "backend-tenant".into(),
            principal: "backend-actor".into(),
        },
        policy_revision: "recorded-policy-v1".into(),
    }
}
pub fn spec(id: &str, approval: bool, recovery: RecoveryPolicy) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        effect_id: EffectId::new(format!("backend-effect-{id}")).unwrap(),
        recovery,
        requires_approval: approval,
    }
}
pub fn seed(
    directory: &Directory,
    batches: Vec<Vec<TaskSpec>>,
    values: Map<String, Value>,
    metadata: Map<String, Value>,
) -> Arc<SqliteStore> {
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    store
        .create_thread_with(ThreadSeed {
            id: thread_id(),
            created_at_ms: 100,
            metadata,
            origin: Some(context().actor),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        execution_id(),
        TurnId::new("backend-original-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/backend-confinement#workflow").unwrap(),
            revision: ExecutionRevision::new("original-v1").unwrap(),
            batches,
            max_attempts: 32,
        },
        values,
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    assert!(initial.tasks.is_empty());
    store.admit(&thread_id(), initial).unwrap();
    store
}
pub fn ordinary(store: &SqliteStore) -> ThreadView {
    store.thread_view(&thread_id()).unwrap().unwrap()
}
pub fn authority(store: &SqliteStore) -> ApprovalView {
    store.approval_view(&execution_id()).unwrap().unwrap()
}
pub fn commit(store: &SqliteStore, command: KernelCommand) {
    let original = ordinary(store).execution.unwrap().snapshot;
    let plan = b4_kernel::transition(
        &original,
        command,
        TransitionFacts {
            now_ms: original.updated_at_ms + 1,
        },
    )
    .unwrap();
    store
        .commit(&execution_id(), (&original).into(), plan)
        .unwrap();
}
pub fn dispatch(store: &SqliteStore, id: &str) {
    commit(
        store,
        KernelCommand::Dispatch {
            task_id: TaskId::new(id).unwrap(),
            attempt_id: AttemptId::new(format!("backend-attempt-{id}")).unwrap(),
        },
    );
}
pub fn complete(store: &SqliteStore, id: &str) {
    commit(
        store,
        KernelCommand::Complete {
            task_id: TaskId::new(id).unwrap(),
            attempt_id: AttemptId::new(format!("backend-attempt-{id}")).unwrap(),
            result: TaskResult {
                output: json!({"original_result": id}),
                writes: vec![StateWrite::Append {
                    field: "history".into(),
                    items: vec![json!(id)],
                }],
            },
        },
    );
}
pub struct Allow;
impl ApprovalAuthority for Allow {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
pub fn approval_fence(view: &ThreadView) -> ApprovalFence {
    ApprovalFence {
        execution: (&view.execution.as_ref().unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}
pub fn park(store: &Arc<SqliteStore>, id: &str) {
    let view = ordinary(store);
    let service = ApprovalService::new(store.clone(), Allow).unwrap();
    service
        .park(
            &context(),
            &execution_id(),
            approval_fence(&view),
            TaskId::new(id).unwrap(),
            InterruptId::new(format!("backend-interrupt-{id}")).unwrap(),
            json!({"original_prompt": id}),
            1_000,
            TransitionFacts {
                now_ms: view.execution.as_ref().unwrap().snapshot.updated_at_ms + 1,
            },
        )
        .unwrap();
    drop(service);
}
pub fn accept(store: &Arc<SqliteStore>, entries: &[(&str, ApprovalDecision)]) {
    let view = ordinary(store);
    let records = authority(store);
    let request = ApprovalAcceptanceRequest {
        context: context(),
        route: records.execution.snapshot.definition.route.clone(),
        definition_revision: records.execution.snapshot.definition.revision.clone(),
        turn_id: TurnId::new("backend-approved-turn").unwrap(),
        now_ms: records.execution.snapshot.updated_at_ms + 1,
        decisions: entries
            .iter()
            .map(|(id, decision)| {
                let grant = records
                    .grants
                    .iter()
                    .find(|grant| grant.task_id.as_str() == *id)
                    .unwrap();
                GrantDecision {
                    interrupt_id: grant.interrupt_id.clone(),
                    decision: *decision,
                    cancelled: false,
                    token: Some(grant.token.clone()),
                }
            })
            .collect(),
    };
    let service = ApprovalService::new(store.clone(), Allow).unwrap();
    service
        .accept(&execution_id(), approval_fence(&view), request)
        .unwrap();
    drop(service);
}
pub fn advance(directory: &Directory, store: Arc<SqliteStore>) -> Arc<SqliteStore> {
    let owner = store.owner_generation();
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    let reopened = Arc::new(SqliteStore::open(directory.path()).unwrap());
    assert_eq!(reopened.owner_generation(), owner + 1);
    reopened.readiness().unwrap();
    reopened
}
pub fn raw_reopen_expected(before: &RawTables, owner: u64) -> RawTables {
    let mut expected = before.clone();
    expected.get_mut("metadata").unwrap()[0][3] = blob(owner);
    expected
}
pub fn retire(
    directory: &Directory,
    store: Arc<SqliteStore>,
    saved: &ThreadView,
    rows: &RawTables,
) {
    let owner = store.owner_generation();
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    let reopened = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(reopened.owner_generation(), owner + 1);
    reopened.readiness().unwrap();
    assert_eq!(ordinary(&reopened), *saved);
    assert_eq!(directory.rows(), raw_reopen_expected(rows, owner + 1));
    drop(reopened);
}
pub fn blob(value: u64) -> SqlValue {
    SqlValue::Blob(value.to_be_bytes().to_vec())
}
pub fn failure_text(reason: RecoveryConfinementReason) -> &'static str {
    match reason {
        RecoveryConfinementReason::ExactDefinitionUnavailable => "recovery_definition_unavailable",
        RecoveryConfinementReason::RequiredAuthorityUnavailable => "recovery_authority_unavailable",
        RecoveryConfinementReason::DispatchNotAuthorized => "recovery_dispatch_not_authorized",
    }
}
pub fn expected(
    original: &ThreadView,
    owner: u64,
    reason: RecoveryConfinementReason,
    turn: &TurnId,
) -> ThreadView {
    let mut result = original.clone();
    result.thread.resource_revision += 1;
    let snapshot = &mut result.execution.as_mut().unwrap().snapshot;
    snapshot.status = ExecutionStatus::RecoveryBlocked;
    snapshot.failure = Some(failure_text(reason).into());
    snapshot.owner_generation = owner;
    snapshot.revision += 1;
    snapshot.turn_id = turn.clone();
    snapshot.turn_history.push(turn.clone());
    snapshot.updated_at_ms = 5_000;
    result
}
pub fn expected_rows(before: &RawTables, view: &ThreadView) -> RawTables {
    let mut rows = before.clone();
    rows.get_mut("threads").unwrap()[0][5] = blob(view.thread.resource_revision);
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    let execution = &mut rows.get_mut("executions").unwrap()[0];
    execution[2] = blob(snapshot.revision);
    execution[3] = blob(snapshot.owner_generation);
    execution[4] = SqlValue::Text(serde_json::to_string(snapshot).unwrap());
    let journal = rows.get_mut("journal").unwrap();
    let SqlValue::Integer(sequence) = journal.last().unwrap()[0] else {
        unreachable!()
    };
    let entry = JournalEntry::from(snapshot);
    journal.push(vec![
        SqlValue::Integer(sequence + 1),
        SqlValue::Text("commit".into()),
        SqlValue::Text(view.thread.id.as_str().into()),
        SqlValue::Text(snapshot.execution_id.as_str().into()),
        blob(snapshot.revision),
        blob(snapshot.owner_generation),
        SqlValue::Text(serde_json::to_string(&snapshot.status).unwrap()),
        blob(snapshot.updated_at_ms),
        SqlValue::Text(serde_json::to_string(&entry).unwrap()),
    ]);
    rows
}

pub fn assert_confined(
    directory: &Directory,
    store: Arc<SqliteStore>,
    reason: RecoveryConfinementReason,
) {
    let original = ordinary(&store);
    let original_authority = authority(&store);
    let journal_before = store.journal(&execution_id()).unwrap();
    let before = directory.rows();
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let owner = store.owner_generation();
    let turn = TurnId::new("backend-confinement-turn").unwrap();
    let captured = store.confinement_view_fenced(&target);
    assert_eq!(directory.rows(), before);
    let observed = store.confine_recovery_fenced(
        &original,
        &target,
        reason,
        &turn,
        TransitionFacts { now_ms: 5_000 },
    );
    let after = ordinary(&store);
    let authority_after = authority(&store);
    let journal_after = store.journal(&execution_id()).unwrap();
    let rows_after = directory.rows();
    let reopened = advance(directory, store);
    assert_eq!(ordinary(&reopened), after);
    assert_eq!(authority(&reopened), authority_after);
    assert_eq!(reopened.journal(&execution_id()).unwrap(), journal_after);
    assert_eq!(
        directory.rows(),
        raw_reopen_expected(&rows_after, owner + 1)
    );
    drop(reopened);
    assert_eq!(
        captured.expect("bounded capture must return actual prior view"),
        original
    );
    let RecoveryConfinementOutcome::Confined(receipt) =
        observed.expect("known prior work must durably confine")
    else {
        panic!("original eligible state cannot be AlreadyBlocked");
    };
    let expected = expected(&original, owner, reason, &turn);
    assert_eq!(after, expected);
    let mut expected_authority = original_authority;
    expected_authority.thread = expected.thread.clone();
    expected_authority.execution = expected.execution.as_ref().unwrap().clone();
    assert_eq!(authority_after, expected_authority);
    let mut expected_journal = journal_before;
    expected_journal.push(JournalEntry::from(
        &expected.execution.as_ref().unwrap().snapshot,
    ));
    assert_eq!(journal_after, expected_journal);
    assert_eq!(rows_after, expected_rows(&before, &expected));
    assert_eq!(receipt.thread_id, target.thread_id);
    assert_eq!(receipt.thread_generation, target.thread_generation);
    assert_eq!(receipt.execution_id, target.execution_id);
    assert_eq!(receipt.original_fence, target.execution_fence);
    assert_eq!(
        receipt.new_fence,
        ExecutionFence::from(&expected.execution.as_ref().unwrap().snapshot)
    );
    assert_eq!(receipt.original_resource_revision, target.resource_revision);
    assert_eq!(receipt.new_resource_revision, target.resource_revision + 1);
    assert_eq!(receipt.original_turn_id, target.turn_id);
    assert_eq!(receipt.fresh_turn_id, turn);
    assert_eq!(
        receipt.route,
        expected
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .definition
            .route
    );
    assert_eq!(
        receipt.definition_revision,
        expected
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .definition
            .revision
    );
    assert_eq!(receipt.reason, reason);
}

pub fn assert_refused(
    directory: &Directory,
    store: Arc<SqliteStore>,
    refusal: RecoveryConfinementBackendError,
) {
    let original = ordinary(&store);
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = directory.rows();
    let journal = store.journal(&execution_id()).unwrap();
    let capture = store.confinement_view_fenced(&target);
    let mutation = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &TurnId::new("backend-refused-turn").unwrap(),
        TransitionFacts { now_ms: 5_000 },
    );
    assert_eq!(ordinary(&store), original);
    assert_eq!(store.journal(&execution_id()).unwrap(), journal);
    assert_eq!(directory.rows(), before);
    retire(directory, store, &original, &before);
    assert_eq!(capture.unwrap_err(), refusal);
    assert_eq!(mutation.unwrap_err(), refusal);
}

pub fn write_snapshot_raw(directory: &Directory, raw: &str) {
    let decoded: Snapshot = serde_json::from_str(raw).unwrap();
    b4_kernel::validate_snapshot(&decoded).unwrap();
    assert_eq!(decoded.execution_id, execution_id());
    directory
        .connection()
        .execute(
            "UPDATE executions SET snapshot=?1 WHERE id=?2",
            params![raw, execution_id().as_str()],
        )
        .unwrap();
}
pub fn snapshot_raw(directory: &Directory) -> String {
    directory
        .connection()
        .query_row(
            "SELECT snapshot FROM executions WHERE id=?1",
            [execution_id().as_str()],
            |row| row.get(0),
        )
        .unwrap()
}
pub fn shape_nodes(snapshot: &Snapshot) -> usize {
    let document = serde_json::to_value(snapshot).unwrap();
    let mut count = 0;
    let mut work = vec![&document];
    while let Some(value) = work.pop() {
        count += 1;
        match value {
            Value::Object(object) => {
                count += object.len();
                work.extend(object.values());
            }
            Value::Array(array) => work.extend(array),
            _ => {}
        }
    }
    count
}

// Additional helpers only. Original frozen support prefix remains exact.
pub const BOUNDARY_AUTHORITY_BYTES: usize = 131_072;
pub const BOUNDARY_AGGREGATE_BYTES: usize = 8_388_608;
pub const BOUNDARY_AGGREGATE_NODES: usize = 65_536;

/// One real single-pending-task acceptance, with a fresh ordinary turn and the
/// actual original grant token. Deny settles this batch without an attempt.
pub fn boundary_accept_deny(store: &Arc<SqliteStore>, id: &str, turn: &str) {
    let original = ordinary(store);
    let records = authority(store);
    let grant = records
        .grants
        .iter()
        .find(|grant| grant.task_id.as_str() == id && !grant.consumed)
        .unwrap();
    let service = ApprovalService::new(store.clone(), Allow).unwrap();
    service
        .accept(
            &execution_id(),
            approval_fence(&original),
            ApprovalAcceptanceRequest {
                context: context(),
                route: records.execution.snapshot.definition.route.clone(),
                definition_revision: records.execution.snapshot.definition.revision.clone(),
                turn_id: TurnId::new(turn).unwrap(),
                now_ms: records.execution.snapshot.updated_at_ms + 1,
                decisions: vec![GrantDecision {
                    interrupt_id: grant.interrupt_id.clone(),
                    decision: ApprovalDecision::Deny,
                    cancelled: false,
                    token: Some(grant.token.clone()),
                }],
            },
        )
        .unwrap();
    drop(service);
}

/// Grow a source-valid journal using actual owner succession, never a repeated
/// Cancel in CancelRequested, invented grant/acceptance, or duplicate no-op.
/// This is deliberately expensive: each added entry uses a normal real writer
/// close/reopen and exact-definition Recover with a unique new turn.
pub fn boundary_journal_history(
    directory: &Directory,
    entries: usize,
    metadata: Map<String, Value>,
) -> Arc<SqliteStore> {
    assert!((2..=4_096).contains(&entries));
    let mut store = seed(
        directory,
        vec![vec![spec("boundary-ready", false, RecoveryPolicy::Pure)]],
        Map::new(),
        metadata,
    );
    commit(&store, KernelCommand::Start);
    for revision in 2..entries {
        store = advance(directory, store);
        let original = ordinary(&store);
        let snapshot = &original.execution.as_ref().unwrap().snapshot;
        assert_eq!(snapshot.status, ExecutionStatus::Running);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Ready);
        assert_eq!(snapshot.attempts_used, 0);
        assert!(snapshot.owner_generation < store.owner_generation());
        commit(
            &store,
            KernelCommand::Recover {
                available_revision: snapshot.definition.revision.clone(),
                turn_id: TurnId::new(format!("boundary-journal-turn-{revision:04}")).unwrap(),
                owner_generation: store.owner_generation(),
            },
        );
    }
    let original = ordinary(&store);
    let snapshot = &original.execution.as_ref().unwrap().snapshot;
    let journal = store.journal(&execution_id()).unwrap();
    assert_eq!(journal.len(), entries);
    assert_eq!(snapshot.revision as usize, entries - 1);
    assert_eq!(snapshot.turn_history.len(), entries - 1);
    assert!(snapshot.updated_at_ms < 5_000);
    assert_eq!(snapshot.attempts_used, 0);
    assert!(snapshot.attempt_history.is_empty());
    assert!(journal
        .windows(2)
        .all(|pair| pair[0].revision + 1 == pair[1].revision
            && pair[0].owner_generation <= pair[1].owner_generation
            && pair[0].updated_at_ms <= pair[1].updated_at_ms));
    assert_eq!(journal.first().unwrap().revision, 0);
    assert_eq!(journal.last().unwrap(), &JournalEntry::from(snapshot));
    b4_kernel::validate_snapshot(snapshot).unwrap();
    store
}

pub fn boundary_assert_already_blocked(directory: &Directory, store: Arc<SqliteStore>) {
    let original = ordinary(&store);
    let records = authority(&store);
    let journal = store.journal(&execution_id()).unwrap();
    let before = directory.rows();
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let captured = store.confinement_view_fenced(&target);
    let observed = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::DispatchNotAuthorized,
        &TurnId::new("boundary-already-blocked-observation").unwrap(),
        TransitionFacts { now_ms: 5_000 },
    );
    assert_eq!(ordinary(&store), original);
    assert_eq!(authority(&store), records);
    assert_eq!(store.journal(&execution_id()).unwrap(), journal);
    assert_eq!(directory.rows(), before);
    retire(directory, store, &original, &before);
    assert_eq!(captured.unwrap(), original);
    assert_eq!(
        observed.unwrap(),
        RecoveryConfinementOutcome::AlreadyBlocked
    );
}

fn boundary_padded_json(raw: &str, bytes: usize) -> String {
    assert!(raw.len() <= bytes);
    let original: Value = serde_json::from_str(raw).unwrap();
    let mut padded = raw.to_owned();
    padded.push_str(&" ".repeat(bytes - raw.len()));
    assert_eq!(padded.len(), bytes);
    assert_eq!(serde_json::from_str::<Value>(&padded).unwrap(), original);
    padded
}

pub fn boundary_pad_grant(directory: &Directory, interrupt: &InterruptId, bytes: usize) {
    let connection = directory.connection();
    let raw: String = connection
        .query_row(
            "SELECT record FROM approval_grants WHERE execution_id=?1 AND interrupt_id=?2",
            params![execution_id().as_str(), interrupt.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let original: GrantRecord = serde_json::from_str(&raw).unwrap();
    let padded = boundary_padded_json(&raw, bytes);
    assert_eq!(
        serde_json::from_str::<GrantRecord>(&padded).unwrap(),
        original
    );
    assert_eq!(
        connection
            .execute(
                "UPDATE approval_grants SET record=?1 WHERE execution_id=?2 AND interrupt_id=?3",
                params![padded, execution_id().as_str(), interrupt.as_str()]
            )
            .unwrap(),
        1
    );
}

pub fn boundary_pad_acceptance(directory: &Directory, turn: &TurnId, bytes: usize) {
    let connection = directory.connection();
    let raw: String = connection
        .query_row(
            "SELECT record FROM approval_acceptances WHERE execution_id=?1 AND turn_id=?2",
            params![execution_id().as_str(), turn.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let original: ApprovalAcceptance = serde_json::from_str(&raw).unwrap();
    let padded = boundary_padded_json(&raw, bytes);
    assert_eq!(
        serde_json::from_str::<ApprovalAcceptance>(&padded).unwrap(),
        original
    );
    assert_eq!(
        connection
            .execute(
                "UPDATE approval_acceptances SET record=?1 WHERE execution_id=?2 AND turn_id=?3",
                params![padded, execution_id().as_str(), turn.as_str()]
            )
            .unwrap(),
        1
    );
}

pub fn boundary_pad_admit_journal(directory: &Directory, bytes: usize) {
    let connection = directory.connection();
    let raw: String = connection
        .query_row(
            "SELECT payload FROM journal WHERE execution_id=?1 AND kind='admit'",
            [execution_id().as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let original: JournalEntry = serde_json::from_str(&raw).unwrap();
    let padded = boundary_padded_json(&raw, bytes);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(&padded).unwrap(),
        original
    );
    assert_eq!(
        connection
            .execute(
                "UPDATE journal SET payload=?1 WHERE execution_id=?2 AND kind='admit'",
                params![padded, execution_id().as_str()]
            )
            .unwrap(),
        1
    );
}

fn boundary_text(value: &SqlValue) -> Option<&str> {
    match value {
        SqlValue::Text(text) => Some(text),
        SqlValue::Null => None,
        _ => panic!("boundary fixture expected TEXT or NULL"),
    }
}

pub fn boundary_raw_text_bytes(rows: &RawTables, table: &str, column: usize) -> usize {
    rows[table]
        .iter()
        .map(|row| boundary_text(&row[column]).map_or(0, str::len))
        .sum()
}

/// A generous test-only upper bound over all six tables of these single-resource
/// fixtures. It counts every INTEGER/REAL as8 bytes, even SQL bookkeeping fields.
/// This is not a production heap, database size, or exact ledger claim.
pub fn boundary_all_table_byte_upper_bound(rows: &RawTables) -> usize {
    rows.values()
        .flatten()
        .flatten()
        .map(|value| match value {
            SqlValue::Text(text) => text.len(),
            SqlValue::Blob(blob) => blob.len(),
            SqlValue::Integer(_) | SqlValue::Real(_) => 8,
            SqlValue::Null => 0,
        })
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .unwrap()
}

pub fn boundary_json_nodes(value: &Value) -> usize {
    let mut work = vec![value];
    let mut count = 0;
    while let Some(value) = work.pop() {
        count += 1;
        match value {
            Value::Object(object) => {
                count += object.len();
                work.extend(object.values());
            }
            Value::Array(array) => work.extend(array),
            _ => {}
        }
    }
    count
}

/// Mandatory selected JSON payload nodes plus a conservative interval for the
/// JSON-encoded status index strings. The two node fixtures use margins that
/// cross the aggregate limit regardless of whether those fixed typed status
/// indexes are charged as an additional JSON record. No exact65536 edge claim.
pub fn boundary_json_node_interval(rows: &RawTables) -> (usize, usize) {
    let mut nodes = 0_usize;
    for (table, column) in [
        ("threads", 3),
        ("threads", 4),
        ("executions", 4),
        ("journal", 8),
        ("approval_grants", 7),
        ("approval_acceptances", 7),
    ] {
        for row in &rows[table] {
            if let Some(raw) = boundary_text(&row[column]) {
                let value: Value = serde_json::from_str(raw).unwrap();
                nodes = nodes.checked_add(boundary_json_nodes(&value)).unwrap();
            }
        }
    }
    let statuses = rows["journal"]
        .iter()
        .filter(|row| boundary_text(&row[6]).is_some())
        .count();
    (nodes, nodes.checked_add(statuses).unwrap())
}

pub fn boundary_assert_individual_payload_caps(rows: &RawTables) {
    assert_eq!(rows["threads"].len(), 1);
    assert_eq!(rows["executions"].len(), 1);
    let thread = &rows["threads"][0];
    let components = [0, 1, 3, 4, 6]
        .into_iter()
        .map(|column| boundary_text(&thread[column]).map_or(0, str::len))
        .sum::<usize>()
        + 16;
    assert!(components <= THREAD_BYTES);
    for (table, column, cap) in [
        ("threads", 3, THREAD_BYTES),
        ("threads", 4, THREAD_BYTES),
        ("executions", 4, SNAPSHOT_BYTES),
        ("journal", 8, 4_096),
        ("approval_grants", 7, BOUNDARY_AUTHORITY_BYTES),
        ("approval_acceptances", 7, BOUNDARY_AUTHORITY_BYTES),
    ] {
        for row in &rows[table] {
            if let Some(raw) = boundary_text(&row[column]) {
                assert!(raw.len() <= cap);
                let value: Value = serde_json::from_str(raw).unwrap();
                assert!(boundary_json_nodes(&value) <= 16_384);
            }
        }
    }
    assert!(rows["approval_grants"].len() <= 64);
    assert!(rows["approval_acceptances"].len() <= 64);
}
