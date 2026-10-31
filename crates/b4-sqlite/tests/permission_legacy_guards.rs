#![cfg(unix)]

use b4_contract::*;
use b4_kernel::{new_execution, transition, validate_snapshot};
use b4_runtime::approval::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use rusqlite::types::{Value, ValueRef};
use serde_json::{json, Map};
use std::collections::BTreeMap;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const HEADER_TURN: &str = "legacy-negative-header-turn";
const HEADER_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const HEADER_PERMISSION: &str =
    "b4pd_2222222222222222222222222222222222222222222222222222222222222222";
const HEADER_USE: &str = "b4pu_1111111111111111111111111111111111111111111111111111111111111111";

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-permission-legacy-guards-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct AllowAuthority;
impl ApprovalAuthority for AllowAuthority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
struct CountNativeCalls(Arc<AtomicUsize>);
impl NativeEffect for CountNativeCalls {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"fixture": true}),
            writes: vec![],
        }))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    AcceptanceGeneration,
    DecisionGeneration,
    UseGeneration,
    TargetAcceptance,
    Once,
}
impl Case {
    fn has_acceptance(self) -> bool {
        self != Self::Once
    }
    fn has_decision(self) -> bool {
        matches!(self, Self::DecisionGeneration | Self::UseGeneration)
    }
    fn has_use(self) -> bool {
        self == Self::UseGeneration
    }
}

#[derive(PartialEq)]
struct Ordinary {
    thread: ThreadView,
    execution: StoredExecution,
    journal: Vec<JournalEntry>,
}
fn ordinary(store: &SqliteStore, thread: &ThreadId, execution: &ExecutionId) -> Ordinary {
    Ordinary {
        thread: store.thread_view(thread).unwrap().unwrap(),
        execution: store.execution(execution).unwrap().unwrap(),
        journal: store.journal(execution).unwrap(),
    }
}
fn fence(store: &dyn ApprovalStore, thread: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}

// Complete typed cells, including payload spelling and every scalar index.
// The dedicated tiny fixture owns one row in each group except journal(4/5).
#[derive(Clone, PartialEq)]
struct RawNine {
    tables: [Vec<Vec<Value>>; 9],
}
fn raw_cells(
    connection: &rusqlite::Connection,
    sql: &str,
    columns: usize,
    maximum: usize,
) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let mut cursor = statement.query([]).unwrap();
    let mut rows = Vec::new();
    while let Some(row) = cursor.next().unwrap() {
        assert!(
            rows.len() < maximum,
            "the complete tiny capture detects unexpected extra rows"
        );
        let mut values = Vec::new();
        for column in 0..columns {
            let borrowed = row.get_ref(column).unwrap();
            match borrowed {
                ValueRef::Text(bytes) => assert!(
                    bytes.len() <= 16_384 && std::str::from_utf8(bytes).is_ok(),
                    "raw capture retains only bounded fixture UTF-8 text"
                ),
                ValueRef::Blob(bytes) => assert!(
                    bytes.len() == 8,
                    "raw integer cells retain exact BLOB8 width"
                ),
                ValueRef::Null | ValueRef::Integer(_) => {}
                ValueRef::Real(_) => {
                    panic!("the fixed nine-table fixture has no real-valued SQL cell")
                }
            }
            values.push(row.get::<_, Value>(column).unwrap());
        }
        rows.push(values);
    }
    rows
}
fn raw_nine(connection: &rusqlite::Connection) -> RawNine {
    RawNine { tables: [
        raw_cells(connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        raw_cells(connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        raw_cells(connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 2", 5, 1),
        raw_cells(connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 6", 9, 5),
        raw_cells(connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        raw_cells(connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] }
}
fn raw_observer(directory: &Directory) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    let enabled: i64 = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get(0))
        .unwrap();
    assert!(
        enabled == 1,
        "the raw negative fixture retains actual foreign key checks"
    );
    connection
}
fn close_observer(connection: rusqlite::Connection) {
    assert!(
        connection.close().is_ok(),
        "the owned raw observer closes normally"
    );
}

// '{}' is deliberately a NONAUTHORITY payload. These rows qualify bounded
// index classification, never a genuine accepted Always or semantic graph.
fn install_owned_headers(
    connection: &mut rusqlite::Connection,
    before: &ApprovalView,
    case: Case,
    other_generation: &str,
) {
    assert!(
        case != Case::Once,
        "the old Once control has no fabricated new rows"
    );
    assert!(
        before.execution.snapshot.revision == 2 && before.execution.snapshot.tasks.len() == 1,
        "header subjects retain the real single Park"
    );
    let actual = before.thread.generation.as_str();
    assert!(
        actual != other_generation,
        "the selected generation fault is provably distinct"
    );
    let transaction = connection.transaction().unwrap();
    let generation = if case == Case::AcceptanceGeneration {
        other_generation
    } else {
        actual
    };
    let count = transaction.execute(
        "INSERT INTO permission_acceptances (execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, '{}')",
        rusqlite::params![before.execution.snapshot.execution_id.as_str(), HEADER_TURN, before.thread.id.as_str(), generation, 1_u64.to_be_bytes().as_slice(), 2_u64.to_be_bytes().as_slice()],
    ).unwrap();
    assert!(count == 1, "the raw fixture owns exactly one A header");
    if case.has_decision() {
        let generation = if case == Case::DecisionGeneration {
            other_generation
        } else {
            actual
        };
        let count = transaction.execute(
            "INSERT INTO permission_decisions (permission_id, thread_id, thread_generation, scope_digest, record_version, record) VALUES (?1, ?2, ?3, ?4, 1, '{}')",
            rusqlite::params![HEADER_PERMISSION, before.thread.id.as_str(), generation, HEADER_DIGEST],
        ).unwrap();
        assert!(count == 1, "the raw fixture owns exactly one D header");
    }
    if case.has_use() {
        let count = transaction.execute(
            "INSERT INTO permission_uses (execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, '{}')",
            rusqlite::params![before.execution.snapshot.execution_id.as_str(), before.execution.snapshot.tasks[0].spec.id.as_str(), HEADER_USE, before.thread.id.as_str(), other_generation, HEADER_PERMISSION, HEADER_TURN, 2_u64.to_be_bytes().as_slice()],
        ).unwrap();
        assert!(
            count == 1,
            "the raw fixture owns exactly one FK-linked U header"
        );
    }
    transaction.commit().unwrap();
}
fn repair_owned_headers(connection: &mut rusqlite::Connection, before: &ApprovalView, case: Case) {
    let transaction = connection.transaction().unwrap();
    let u = transaction
        .execute(
            "DELETE FROM permission_uses WHERE execution_id = ?1 AND task_id = ?2 AND use_id = ?3",
            rusqlite::params![
                before.execution.snapshot.execution_id.as_str(),
                before.execution.snapshot.tasks[0].spec.id.as_str(),
                HEADER_USE
            ],
        )
        .unwrap();
    let d = transaction
        .execute(
            "DELETE FROM permission_decisions WHERE permission_id = ?1 AND thread_id = ?2",
            rusqlite::params![HEADER_PERMISSION, before.thread.id.as_str()],
        )
        .unwrap();
    let a = transaction
        .execute(
            "DELETE FROM permission_acceptances WHERE execution_id = ?1 AND turn_id = ?2",
            rusqlite::params![before.execution.snapshot.execution_id.as_str(), HEADER_TURN],
        )
        .unwrap();
    assert!(
        u == usize::from(case.has_use())
            && d == usize::from(case.has_decision())
            && a == usize::from(case.has_acceptance()),
        "owned repair removes only exact U then D then A rows"
    );
    transaction.commit().unwrap();
}
fn expected_subject(original: &RawNine, before: &ApprovalView, case: Case, other: &str) -> RawNine {
    let mut expected = original.clone();
    let text = |s: &str| Value::Text(s.to_owned());
    let execution = before.execution.snapshot.execution_id.as_str();
    let thread = before.thread.id.as_str();
    let actual = before.thread.generation.as_str();
    if case.has_acceptance() {
        let generation = if case == Case::AcceptanceGeneration {
            other
        } else {
            actual
        };
        expected.tables[6].push(vec![
            text(execution),
            text(HEADER_TURN),
            text(thread),
            text(generation),
            Value::Blob(1_u64.to_be_bytes().to_vec()),
            Value::Blob(2_u64.to_be_bytes().to_vec()),
            Value::Integer(1),
            text("{}"),
        ]);
    }
    if case.has_decision() {
        let generation = if case == Case::DecisionGeneration {
            other
        } else {
            actual
        };
        expected.tables[7].push(vec![
            text(HEADER_PERMISSION),
            text(thread),
            text(generation),
            text(HEADER_DIGEST),
            Value::Integer(1),
            text("{}"),
        ]);
    }
    if case.has_use() {
        expected.tables[8].push(vec![
            text(execution),
            text(before.execution.snapshot.tasks[0].spec.id.as_str()),
            text(HEADER_USE),
            text(thread),
            text(other),
            text(HEADER_PERMISSION),
            text(HEADER_TURN),
            Value::Blob(2_u64.to_be_bytes().to_vec()),
            Value::Integer(1),
            text("{}"),
        ]);
    }
    expected
}

struct Captured {
    case: Case,
    result: Result<Option<ApprovalView>, ApprovalError>,
    before: ApprovalView,
    before_ordinary: Ordinary,
    after_ordinary: Ordinary,
    repaired: ApprovalView,
    repaired_ordinary: Ordinary,
    next: ApprovalView,
    next_ordinary: Ordinary,
    raw_original: RawNine,
    raw_subject: RawNine,
    raw_after: RawNine,
    raw_restored: RawNine,
    raw_next: RawNine,
    other_generation: String,
    original_owner: u64,
    next_owner: u64,
    native_calls: usize,
    directory_removed: bool,
}
fn capture_after_normal_retirement(case: Case) -> Captured {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("legacy-guard-thread").unwrap();
    let execution_id = ExecutionId::new("legacy-guard-execution").unwrap();
    let definition = Definition {
        route: RouteKey::new("/legacy-guard#agent").unwrap(),
        revision: ExecutionRevision::new("legacy-guard-definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("first").unwrap(),
            effect_id: EffectId::new("first-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 4,
    };
    let origin = ThreadOrigin {
        application: "legacy-guard-app".into(),
        tenant: "fixture-tenant".into(),
        principal: "fixture-principal".into(),
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: origin.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountNativeCalls(native_calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    assert!(
        registered == definition,
        "the selected native Definition is exact"
    );
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert!(
        original_owner == 1 && store.approval_durable(),
        "the fixture owns a fresh genuine durable writer"
    );
    let thread = store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(origin.clone()),
        })
        .unwrap();
    let context = ApprovalContext {
        actor: origin,
        policy_revision: "policy-1".into(),
    };
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("legacy-guard-original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("payload".into(), json!({"fixture": true, "nullable": null}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread_id, initial).unwrap();
    let started = store
        .commit(
            &execution_id,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    assert!(
        started.snapshot.revision == 1
            && started.snapshot.tasks.len() == 1
            && started.snapshot.tasks[0].status == TaskStatus::Ready
            && !started.snapshot.tasks[0].approved,
        "actual Start reaches genuine unapproved Ready"
    );
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let parked = service
        .park(
            &context,
            &execution_id,
            fence(store.as_ref(), &thread_id),
            started.snapshot.tasks[0].spec.id.clone(),
            InterruptId::new("first-interrupt").unwrap(),
            json!({"question":"First fixture?"}),
            1000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let parked_view = store.approval_view(&execution_id).unwrap().unwrap();
    assert!(
        parked == parked_view.execution
            && parked.snapshot.revision == 2
            && parked.snapshot.status == ExecutionStatus::WaitingForApproval
            && parked_view.grants.len() == 1
            && parked_view.acceptances.is_empty()
            && pending.len() == 1,
        "actual Park issues one real old grant without an acceptance"
    );
    let grant = &parked_view.grants[0];
    let pending_entry = &pending[0];
    let parked_task = &parked.snapshot.tasks[0];
    assert!(
        grant.token == pending_entry.token
            && grant.verifier == token_verifier(&pending_entry.token)
            && grant.resume_key == pending_entry.resume_key
            && !grant.consumed
            && grant.prompt == pending_entry.prompt
            && grant.origin == context.actor
            && grant.execution_id == execution_id
            && grant.thread_id == thread_id
            && grant.thread_generation == thread.generation
            && grant.task_parking_revision == 2
            && grant.route == definition.route
            && grant.definition_revision == definition.revision
            && grant.task_id == parked_task.spec.id
            && grant.task_id == pending_entry.task_id
            && grant.effect_id == parked_task.spec.effect_id
            && grant.interrupt_id == pending_entry.interrupt_id
            && parked_task.interrupt.as_ref().unwrap().id == grant.interrupt_id
            && grant.policy_revision == context.policy_revision
            && grant.proposal_digest == proposal_digest(parked_task, &grant.prompt).unwrap()
            && grant.issued_at_ms == 102
            && grant.expires_at_ms == 1102
            && grant.expires_at_ms == pending_entry.expires_at_ms,
        "the actual token and complete binding come from the real old Park"
    );
    if case == Case::Once {
        let request = ApprovalAcceptanceRequest {
            context: context.clone(),
            route: definition.route.clone(),
            definition_revision: definition.revision.clone(),
            turn_id: TurnId::new("legacy-guard-once-turn").unwrap(),
            now_ms: 104,
            decisions: vec![GrantDecision {
                interrupt_id: pending_entry.interrupt_id.clone(),
                decision: ApprovalDecision::Once,
                cancelled: false,
                token: Some(pending_entry.token.clone()),
            }],
        };
        let accepted = service
            .accept(&execution_id, fence(store.as_ref(), &thread_id), request)
            .unwrap();
        assert!(
            accepted.snapshot.revision == 3
                && accepted.snapshot.tasks[0].approved
                && accepted.snapshot.tasks[0].status == TaskStatus::Ready
                && accepted.snapshot.tasks[0].interrupt.as_ref().unwrap().id
                    == pending_entry.interrupt_id,
            "real Once keeps the original interrupt in approved Ready"
        );
    }
    let before = store.approval_view(&execution_id).unwrap().unwrap();
    validate_snapshot(&before.execution.snapshot).unwrap();
    let before_ordinary = ordinary(&store, &thread_id, &execution_id);
    let other_generation = if before.thread.generation.as_str()
        == "0000000000000000000000000000000000000000000000000000000000000000"
    {
        "1111111111111111111111111111111111111111111111111111111111111111".to_owned()
    } else {
        "0000000000000000000000000000000000000000000000000000000000000000".to_owned()
    };
    let mut observer = raw_observer(&directory);
    let raw_original = raw_nine(&observer);
    assert!(
        raw_original.tables[6].is_empty()
            && raw_original.tables[7].is_empty()
            && raw_original.tables[8].is_empty(),
        "the real old setup has no new authority rows"
    );
    if case != Case::Once {
        install_owned_headers(&mut observer, &before, case, &other_generation);
    }
    let raw_subject = raw_nine(&observer);
    let result = store.approval_view(&execution_id);
    let raw_after = raw_nine(&observer);
    // A successful guard must refuse while rows remain. Ordinary AFTER never
    // re-enters the guarded sensitive accessor until exact owned repair.
    let after_ordinary = ordinary(&store, &thread_id, &execution_id);
    if case != Case::Once {
        repair_owned_headers(&mut observer, &before, case);
    }
    let raw_restored = raw_nine(&observer);
    let repaired = store.approval_view(&execution_id).unwrap().unwrap();
    let repaired_ordinary = ordinary(&store, &thread_id, &execution_id);
    close_observer(observer);
    store.readiness().unwrap();
    drop(parked_view);
    drop(pending);
    drop(parked);
    drop(started);
    drop(admitted);
    drop(thread);
    drop(service);
    drop(authority);
    drop(application);
    assert!(
        Arc::strong_count(&store) == 1,
        "all original durable-store aliases retire normally"
    );
    drop(store);
    let next_store = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next_store.owner_generation();
    let next_ordinary = ordinary(&next_store, &thread_id, &execution_id);
    let next = next_store.approval_view(&execution_id).unwrap().unwrap();
    let next_observer = raw_observer(&directory);
    let raw_next = raw_nine(&next_observer);
    close_observer(next_observer);
    next_store.readiness().unwrap();
    drop(next_store);
    let native_call_count = native_calls.load(Ordering::SeqCst);
    drop(native_calls);
    let directory_path = directory.0.clone();
    drop(directory);
    let directory_removed = !directory_path.exists();
    assert!(
        next_owner == original_owner.checked_add(1).unwrap()
            && native_call_count == 0
            && directory_removed,
        "the genuine next writer and all fixture owners retire normally before any feature oracle"
    );
    Captured {
        case,
        result,
        before,
        before_ordinary,
        after_ordinary,
        repaired,
        repaired_ordinary,
        next,
        next_ordinary,
        raw_original,
        raw_subject,
        raw_after,
        raw_restored,
        raw_next,
        other_generation,
        original_owner,
        next_owner,
        native_calls: native_call_count,
        directory_removed,
    }
}
fn assert_genuine_setup(captured: &Captured) {
    let before = &captured.before;
    assert!(
        validate_snapshot(&before.execution.snapshot).is_ok()
            && before.execution.snapshot.definition.batches.len() == 1
            && before.execution.snapshot.tasks.len() == 1
            && before.thread.origin.is_some()
            && before.grants.len() == 1,
        "the owned actual setup remains strict and native-bound after retirement"
    );
    assert!(
        captured.raw_subject
            == expected_subject(
                &captured.raw_original,
                before,
                captured.case,
                &captured.other_generation
            ),
        "the complete nine-group subject differs only by exact owned negative headers"
    );
    let expected_journal_rows = if captured.case == Case::Once { 5 } else { 4 };
    assert!(
        captured.raw_original.tables[0].len() == 1
            && captured.raw_original.tables[1].len() == 1
            && captured.raw_original.tables[2].len() == 1
            && captured.raw_original.tables[3].len() == expected_journal_rows
            && captured.raw_original.tables[4].len() == 1
            && captured.raw_original.tables[5].len() == usize::from(captured.case == Case::Once),
        "all old table groups have the exact tiny genuine record counts"
    );
}
fn assert_restored_after_retirement(captured: &Captured) {
    assert!(
        captured.before_ordinary == captured.after_ordinary
            && captured.after_ordinary == captured.repaired_ordinary
            && captured.repaired_ordinary == captured.next_ordinary,
        "the full actual ordinary state is unchanged through guarded read, repair and next writer"
    );
    assert!(
        captured.before == captured.repaired && captured.repaired == captured.next,
        "the sensitive old view is read again only after exact repair and remains whole equal"
    );
    assert!(
        captured.raw_subject == captured.raw_after
            && captured.raw_original == captured.raw_restored,
        "the legacy read changes no cell and exact owned repair restores all nine groups"
    );
    assert!(
        captured.next_owner == captured.original_owner.checked_add(1).unwrap()
            && captured.next.execution.snapshot.owner_generation == captured.original_owner,
        "genuine reopening advances durable ownership without adopting the execution owner"
    );
    let mut expected_next = captured.raw_original.clone();
    expected_next.tables[0][0][3] = Value::Blob(captured.next_owner.to_be_bytes().to_vec());
    assert!(
        captured.raw_next == expected_next,
        "all nine next-writer groups preserve every cell except the exact metadata owner increment"
    );
    assert!(
        captured.native_calls == 0 && captured.directory_removed,
        "all original and next owners plus directory retire before the oracle without a callback"
    );
}
fn assert_old_once(captured: &Captured) {
    assert!(
        captured.case == Case::Once
            && captured
                .result
                .as_ref()
                .is_ok_and(|view| view.as_ref() == Some(&captured.before)),
        "the independently invoked old Once neighbor remains a complete successful sensitive read"
    );
    assert!(
        captured.before.grants[0].consumed
            && captured.before.acceptances.len() == 1
            && captured.before.acceptances[0].decisions.len() == 1
            && captured.before.acceptances[0].decisions[0].decision == ApprovalDecision::Once
            && captured.before.execution.snapshot.tasks[0].approved
            && captured.before.execution.snapshot.tasks[0].status == TaskStatus::Ready
            && captured.before.execution.snapshot.tasks[0]
                .interrupt
                .is_some(),
        "the compatibility neighbor has a truthful real Once and original interrupt"
    );
    assert_restored_after_retirement(captured);
}
fn classify(result: &Result<Option<ApprovalView>, ApprovalError>) -> &'static str {
    match result {
        Ok(Some(_)) => "legacy_view_present",
        Ok(None) => "legacy_view_absent",
        Err(ApprovalError::Store(StoreError::Corrupt(_))) => "store_corrupt",
        Err(ApprovalError::Store(StoreError::UnsupportedOperation)) => {
            "store_unsupported_operation"
        }
        Err(_) => "other_refusal",
    }
}
fn run_parent(case: Case) {
    let neighbor = capture_after_normal_retirement(Case::Once);
    let subject = capture_after_normal_retirement(case);
    assert_genuine_setup(&neighbor);
    assert_old_once(&neighbor);
    assert_genuine_setup(&subject);
    // No healthy Unsupported assertion precedes a later-family fault oracle.
    match case {
        Case::AcceptanceGeneration | Case::DecisionGeneration | Case::UseGeneration => assert!(matches!(&subject.result, Err(ApprovalError::Store(StoreError::Corrupt(_)))), "the legacy accessor must classify the selected new-header contradiction after retirement; class={}", classify(&subject.result)),
        Case::TargetAcceptance => assert!(matches!(&subject.result, Err(ApprovalError::Store(StoreError::UnsupportedOperation))), "the legacy accessor must conservatively refuse the target nonauthority header profile after retirement; class={}", classify(&subject.result)),
        Case::Once => assert!(subject.result.as_ref().is_ok_and(|view| view.as_ref() == Some(&subject.before)), "the real old Once parent remains available after retirement; class={}", classify(&subject.result)),
    }
    assert_restored_after_retirement(&subject);
    if case == Case::Once {
        assert_old_once(&subject);
    }
}

#[test]
fn legacy_approval_view_rejects_target_acceptance_generation_header() {
    run_parent(Case::AcceptanceGeneration);
}
#[test]
fn legacy_approval_view_rejects_later_decision_generation_after_target_acceptance_header() {
    run_parent(Case::DecisionGeneration);
}
#[test]
fn legacy_approval_view_rejects_later_use_generation_after_target_acceptance_and_decision_headers()
{
    run_parent(Case::UseGeneration);
}
#[test]
fn legacy_approval_view_refuses_target_nonauthority_acceptance_header_profile() {
    run_parent(Case::TargetAcceptance);
}
#[test]
fn legacy_approval_view_keeps_real_old_once_after_normal_retirement() {
    run_parent(Case::Once);
}

// Each negative probe owns only one raw nonauthority A header. The successful
// neighbor for that same port is independently produced by real old Once.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BypassPort {
    Commit,
    Cancel,
    ConfinementCapture,
    ConfinementMutation,
}
impl BypassPort {
    fn prior_owner(self) -> bool {
        matches!(self, Self::ConfinementCapture | Self::ConfinementMutation)
    }
}

// The same finite nine-table projection permits the one real extra commit.
// The protected old helper retains its original five-journal-row bound.
fn bypass_raw_nine(connection: &rusqlite::Connection) -> RawNine {
    RawNine { tables: [
        raw_cells(connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        raw_cells(connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        raw_cells(connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 2", 5, 1),
        raw_cells(connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 7", 9, 6),
        raw_cells(connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        raw_cells(connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] }
}

fn bypass_owned_header(before: &ApprovalView) -> Vec<Value> {
    let snapshot = &before.execution.snapshot;
    let text = |value: &str| Value::Text(value.to_owned());
    vec![
        text(snapshot.execution_id.as_str()),
        text(snapshot.turn_id.as_str()),
        text(before.thread.id.as_str()),
        text(before.thread.generation.as_str()),
        Value::Blob(
            snapshot
                .revision
                .checked_sub(1)
                .unwrap()
                .to_be_bytes()
                .to_vec(),
        ),
        Value::Blob(snapshot.revision.to_be_bytes().to_vec()),
        Value::Integer(1),
        text("{}"),
    ]
}
fn bypass_install_header(connection: &mut rusqlite::Connection, before: &ApprovalView) {
    let snapshot = &before.execution.snapshot;
    let previous = snapshot.revision.checked_sub(1).unwrap().to_be_bytes();
    let committed = snapshot.revision.to_be_bytes();
    let transaction = connection.transaction().unwrap();
    let count = transaction.execute(
        "INSERT INTO permission_acceptances (execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, '{}')",
        rusqlite::params![snapshot.execution_id.as_str(), snapshot.turn_id.as_str(), before.thread.id.as_str(), before.thread.generation.as_str(), previous.as_slice(), committed.as_slice()],
    ).unwrap();
    assert!(
        count == 1,
        "the negative fixture owns one exact schema-legal A header"
    );
    transaction.commit().unwrap();
}
fn bypass_repair_header(
    connection: &mut rusqlite::Connection,
    owned: &[Value],
    actual_after: &RawNine,
) -> RawNine {
    let mut expected = actual_after.clone();
    // No U or D was seeded or owned. Their repair stages have no writes.
    // A may have disappeared during the subject; that is retained evidence.
    if let Some(row) = actual_after.tables[6].first() {
        assert!(
            row == owned,
            "conditional repair proves all eight owned A cells before deleting"
        );
        let transaction = connection.transaction().unwrap();
        let count = transaction.execute(
            "DELETE FROM permission_acceptances WHERE execution_id = ?1 AND turn_id = ?2 AND thread_id = ?3 AND thread_generation = ?4 AND previous_revision = ?5 AND committed_revision = ?6 AND record_version = ?7 AND record = ?8",
            rusqlite::params![&owned[0], &owned[1], &owned[2], &owned[3], &owned[4], &owned[5], &owned[6], &owned[7]],
        ).unwrap();
        assert!(count == 1, "repair deletes only the exact owned A row");
        transaction.commit().unwrap();
        expected.tables[6].clear();
    }
    expected
}

struct BypassPrepared {
    plan: Option<CommitPlan>,
    cancellation: Option<RunCancellationTarget>,
    confinement: Option<RecoveryConfinementTarget>,
    fresh_turn: TurnId,
}
struct BypassCaptured<R> {
    port: BypassPort,
    once: bool,
    result: R,
    request: Option<ApprovalAcceptanceRequest>,
    producer: Ordinary,
    producer_approval: ApprovalView,
    before: ApprovalView,
    before_ordinary: Ordinary,
    after_ordinary: Ordinary,
    repaired: ApprovalView,
    repaired_ordinary: Ordinary,
    next: ApprovalView,
    next_ordinary: Ordinary,
    raw_original: RawNine,
    raw_subject: RawNine,
    raw_after: RawNine,
    raw_restored: RawNine,
    raw_next: RawNine,
    prepared: BypassPrepared,
    producer_owner: u64,
    operation_owner: u64,
    next_owner: u64,
    native_calls: usize,
    directory_removed: bool,
}

fn capture_bypass_after_normal_retirement<R>(
    port: BypassPort,
    once: bool,
    operation: impl FnOnce(&SqliteStore, &Ordinary, &BypassPrepared) -> R,
) -> BypassCaptured<R> {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("legacy-bypass-thread").unwrap();
    let execution_id = ExecutionId::new("legacy-bypass-execution").unwrap();
    let definition = Definition {
        route: RouteKey::new("/legacy-bypass#agent").unwrap(),
        revision: ExecutionRevision::new("legacy-bypass-definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("first").unwrap(),
            effect_id: EffectId::new("first-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 4,
    };
    let origin = ThreadOrigin {
        application: "legacy-bypass-app".into(),
        tenant: "fixture-tenant".into(),
        principal: "fixture-principal".into(),
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: origin.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountNativeCalls(native_calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    assert!(
        registered == definition,
        "actual native registration supplies the full one-task Definition"
    );
    let mut store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let producer_owner = store.owner_generation();
    assert!(
        producer_owner == 1 && store.approval_durable(),
        "the native fixture owns a genuine durable original writer"
    );
    let thread = store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(origin.clone()),
        })
        .unwrap();
    let context = ApprovalContext {
        actor: origin,
        policy_revision: "policy-1".into(),
    };
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("legacy-bypass-original-turn").unwrap(),
        producer_owner,
        registered,
        Map::from_iter([("payload".into(), json!({"fixture": true, "nullable": null}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread_id, initial).unwrap();
    let started = store
        .commit(
            &execution_id,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    assert!(
        validate_snapshot(&started.snapshot).is_ok()
            && started.snapshot.status == ExecutionStatus::Running
            && started.snapshot.revision == 1
            && started.snapshot.tasks.len() == 1
            && started.snapshot.tasks[0].status == TaskStatus::Ready
            && !started.snapshot.tasks[0].approved
            && started.snapshot.tasks[0].interrupt.is_none(),
        "actual Start is the lawful unapproved current-owner Running producer"
    );
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let mut request = None;
    if once || port.prior_owner() {
        let parked = service
            .park(
                &context,
                &execution_id,
                fence(store.as_ref(), &thread_id),
                started.snapshot.tasks[0].spec.id.clone(),
                InterruptId::new("first-interrupt").unwrap(),
                json!({"question": "First fixture?"}),
                1000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
            .unwrap();
        let parked_view = store.approval_view(&execution_id).unwrap().unwrap();
        assert!(
            pending.len() == 1
                && parked == parked_view.execution
                && parked_view.grants.len() == 1
                && parked_view.acceptances.is_empty()
                && parked.snapshot.revision == 2
                && parked.snapshot.status == ExecutionStatus::WaitingForApproval,
            "actual Park supplies one real old grant and no acceptance"
        );
        let grant = &parked_view.grants[0];
        let entry = &pending[0];
        let task = &parked.snapshot.tasks[0];
        assert!(
            grant.token == entry.token
                && grant.verifier == token_verifier(&entry.token)
                && grant.resume_key == entry.resume_key
                && !grant.consumed
                && grant.prompt == entry.prompt
                && grant.origin == context.actor
                && grant.policy_revision == context.policy_revision
                && grant.execution_id == execution_id
                && grant.thread_id == thread_id
                && grant.thread_generation == thread.generation
                && grant.route == definition.route
                && grant.definition_revision == definition.revision
                && grant.task_id == task.spec.id
                && grant.task_id == entry.task_id
                && grant.effect_id == task.spec.effect_id
                && grant.interrupt_id == entry.interrupt_id
                && task.interrupt.as_ref().unwrap().id == grant.interrupt_id
                && grant.proposal_digest == proposal_digest(task, &grant.prompt).unwrap()
                && grant.task_parking_revision == 2
                && grant.issued_at_ms == 102
                && grant.expires_at_ms == 1102
                && grant.expires_at_ms == entry.expires_at_ms,
            "all token, prompt, policy and proposal bindings come from the real Park"
        );
        if once {
            let actual_request = ApprovalAcceptanceRequest {
                context: context.clone(),
                route: definition.route.clone(),
                definition_revision: definition.revision.clone(),
                turn_id: TurnId::new("legacy-bypass-once-turn").unwrap(),
                now_ms: 104,
                decisions: vec![GrantDecision {
                    interrupt_id: entry.interrupt_id.clone(),
                    decision: ApprovalDecision::Once,
                    cancelled: false,
                    token: Some(entry.token.clone()),
                }],
            };
            let accepted = service
                .accept(
                    &execution_id,
                    fence(store.as_ref(), &thread_id),
                    actual_request.clone(),
                )
                .unwrap();
            assert!(
                validate_snapshot(&accepted.snapshot).is_ok()
                    && accepted.snapshot.revision == 3
                    && accepted.snapshot.status == ExecutionStatus::Running
                    && accepted.snapshot.tasks[0].approved
                    && accepted.snapshot.tasks[0].status == TaskStatus::Ready
                    && accepted.snapshot.tasks[0].interrupt.as_ref().unwrap().id
                        == entry.interrupt_id,
                "genuine Once consumes the real grant and retains its original interrupt"
            );
            request = Some(actual_request);
            drop(accepted);
        }
        drop(parked_view);
        drop(pending);
        drop(parked);
    }
    let producer = ordinary(&store, &thread_id, &execution_id);
    let producer_approval = store.approval_view(&execution_id).unwrap().unwrap();
    store.readiness().unwrap();
    drop(started);
    drop(admitted);
    drop(thread);
    drop(service);
    drop(authority);
    drop(application);
    if port.prior_owner() {
        assert!(
            Arc::strong_count(&store) == 1,
            "all owner-one aliases retire before the real prior-owner probe"
        );
        drop(store);
        store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert!(
            store.owner_generation() == producer_owner.checked_add(1).unwrap()
                && ordinary(&store, &thread_id, &execution_id) == producer
                && store.approval_view(&execution_id).unwrap().as_ref() == Some(&producer_approval),
            "the real second writer preserves the full prior-owner producer"
        );
    }
    let operation_owner = store.owner_generation();
    let before = store.approval_view(&execution_id).unwrap().unwrap();
    let before_ordinary = ordinary(&store, &thread_id, &execution_id);
    validate_snapshot(&before.execution.snapshot).unwrap();
    let fresh_turn = TurnId::new("legacy-bypass-confinement-turn").unwrap();
    assert!(
        !before.execution.snapshot.turn_history.contains(&fresh_turn),
        "the fixed confinement turn is genuinely fresh"
    );
    let (plan, cancellation, confinement) = if port.prior_owner() {
        let target = RecoveryConfinementTarget::from_view(&before_ordinary.thread).unwrap();
        let plan = if port == BypassPort::ConfinementMutation {
            Some(
                b4_kernel::propose_recovery_confinement(
                    &before.execution.snapshot,
                    RecoveryConfinementReason::DispatchNotAuthorized,
                    fresh_turn.clone(),
                    operation_owner,
                    facts(105),
                )
                .unwrap(),
            )
        } else {
            None
        };
        (plan, None, Some(target))
    } else {
        let target = RunCancellationTarget::from_view(&before_ordinary.thread).unwrap();
        let plan = transition(
            &before.execution.snapshot,
            KernelCommand::Cancel {
                turn_id: before.execution.snapshot.turn_id.clone(),
                owner_generation: operation_owner,
            },
            facts(105),
        )
        .unwrap();
        assert!(
            plan == validate_run_cancellation(
                &before_ordinary.thread,
                &target,
                operation_owner,
                facts(105)
            )
            .unwrap(),
            "the same current-owner plan passes the real public cancellation validator"
        );
        (Some(plan), Some(target), None)
    };
    if let Some(plan) = &plan {
        assert!(
            validate_snapshot(&plan.snapshot).is_ok()
                && plan.dispatchable.is_empty()
                && plan.expected_revision == before.execution.snapshot.revision,
            "the retained actual public proposal is strict and has no dispatch"
        );
    }
    let prepared = BypassPrepared {
        plan,
        cancellation,
        confinement,
        fresh_turn,
    };
    let mut observer = raw_observer(&directory);
    let raw_original = bypass_raw_nine(&observer);
    assert!(
        raw_original.tables[6].is_empty()
            && raw_original.tables[7].is_empty()
            && raw_original.tables[8].is_empty(),
        "the genuine producer seeds no new authority rows"
    );
    let owned_header = bypass_owned_header(&before);
    if !once {
        bypass_install_header(&mut observer, &before);
    }
    let raw_subject = bypass_raw_nine(&observer);
    let result = operation(store.as_ref(), &before_ordinary, &prepared);
    let raw_after = bypass_raw_nine(&observer);
    let after_ordinary = ordinary(&store, &thread_id, &execution_id);
    let expected_repaired = if once {
        raw_after.clone()
    } else {
        bypass_repair_header(&mut observer, &owned_header, &raw_after)
    };
    let raw_restored = bypass_raw_nine(&observer);
    assert!(
        raw_restored == expected_repaired,
        "owned repair preserves the complete actually captured AFTER except its exact raw row"
    );
    let repaired = store.approval_view(&execution_id).unwrap().unwrap();
    let repaired_ordinary = ordinary(&store, &thread_id, &execution_id);
    assert!(
        after_ordinary == repaired_ordinary,
        "raw repair does not rewrite the actual business state"
    );
    close_observer(observer);
    store.readiness().unwrap();
    assert!(
        Arc::strong_count(&store) == 1,
        "all operation-owner aliases retire normally before the next writer"
    );
    drop(store);
    let next_store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let next_owner = next_store.owner_generation();
    let next_ordinary = ordinary(&next_store, &thread_id, &execution_id);
    let next = next_store.approval_view(&execution_id).unwrap().unwrap();
    let next_observer = raw_observer(&directory);
    let raw_next = bypass_raw_nine(&next_observer);
    close_observer(next_observer);
    let mut expected_next = raw_restored.clone();
    expected_next.tables[0][0][3] = Value::Blob(next_owner.to_be_bytes().to_vec());
    assert!(
        next_owner == operation_owner.checked_add(1).unwrap()
            && next_ordinary == repaired_ordinary
            && next == repaired
            && raw_next == expected_next,
        "real N+1 ownership preserves all ACTUALLY CAPTURED AFTER rows and views"
    );
    next_store.readiness().unwrap();
    assert!(
        Arc::strong_count(&next_store) == 1,
        "the sole next-writer alias retires normally"
    );
    drop(next_store);
    let native_call_count = native_calls.load(Ordering::SeqCst);
    drop(native_calls);
    let directory_path = directory.0.clone();
    drop(directory);
    let directory_removed = !directory_path.exists();
    assert!(
        native_call_count == 0 && directory_removed,
        "all real writers and Directory retire before any feature oracle"
    );
    BypassCaptured {
        port,
        once,
        result,
        request,
        producer,
        producer_approval,
        before,
        before_ordinary,
        after_ordinary,
        repaired,
        repaired_ordinary,
        next,
        next_ordinary,
        raw_original,
        raw_subject,
        raw_after,
        raw_restored,
        raw_next,
        prepared,
        producer_owner,
        operation_owner,
        next_owner,
        native_calls: native_call_count,
        directory_removed,
    }
}

fn assert_bypass_genuine_setup<R>(captured: &BypassCaptured<R>) {
    let snapshot = &captured.before.execution.snapshot;
    assert!(
        validate_snapshot(snapshot).is_ok()
            && snapshot.definition.batches.len() == 1
            && snapshot.tasks.len() == 1
            && snapshot.tasks[0].spec.recovery == RecoveryPolicy::Pure
            && snapshot.tasks[0].spec.requires_approval
            && snapshot.owner_generation == captured.producer_owner
            && captured.producer_owner == 1
            && captured.producer == captured.before_ordinary
            && captured.producer_approval == captured.before,
        "the fully retained native one-task producer is lawful and unchanged before the probe"
    );
    let revision = if captured.once {
        3
    } else if captured.port.prior_owner() {
        2
    } else {
        1
    };
    let status = if !captured.once && captured.port.prior_owner() {
        ExecutionStatus::WaitingForApproval
    } else {
        ExecutionStatus::Running
    };
    assert!(
        snapshot.revision == revision
            && snapshot.status == status
            && captured.operation_owner
                == captured.producer_owner + u64::from(captured.port.prior_owner())
            && captured.raw_original.tables[0].len() == 1
            && captured.raw_original.tables[1].len() == 1
            && captured.raw_original.tables[2].len() == 1
            && captured.raw_original.tables[3].len() == revision as usize + 2
            && captured.raw_original.tables[4].len()
                == usize::from(captured.once || captured.port.prior_owner())
            && captured.raw_original.tables[5].len() == usize::from(captured.once),
        "current-owner Running and prior-owner Waiting use their actual distinct public producers"
    );
    let mut expected_subject = captured.raw_original.clone();
    if !captured.once {
        expected_subject.tables[6].push(bypass_owned_header(&captured.before));
    }
    assert!(
        captured.raw_subject == expected_subject,
        "the full nine-group input differs only by the exact owned raw negative A header"
    );
    if captured.port.prior_owner() {
        let target = captured.prepared.confinement.as_ref().unwrap();
        assert!(
            target
                == &RecoveryConfinementTarget::from_view(&captured.before_ordinary.thread).unwrap()
                && target.execution_fence == snapshot.into()
                && target.turn_id == snapshot.turn_id
                && captured.operation_owner > snapshot.owner_generation,
            "confinement has exact original target fences and a genuinely later durable owner"
        );
    } else {
        assert!(
            captured.prepared.cancellation.as_ref().unwrap()
                == &RunCancellationTarget::from_view(&captured.before_ordinary.thread).unwrap()
                && captured.operation_owner == snapshot.owner_generation,
            "current-owner ports have an exact lawful cancellation target"
        );
    }
}
fn assert_bypass_old_once<R>(captured: &BypassCaptured<R>) {
    let request = captured.request.as_ref().unwrap();
    let before = &captured.before;
    assert!(captured.once && before.grants.len() == 1 && before.grants[0].consumed
        && before.acceptances.len() == 1 && before.acceptances[0].context == request.context
        && before.acceptances[0].turn_id == request.turn_id && before.acceptances[0].accepted_at_ms == request.now_ms
        && before.acceptances[0].decisions.len() == 1 && before.acceptances[0].decisions[0].decision == ApprovalDecision::Once
        && request.decisions.len() == 1 && request.decisions[0].token.as_ref() == Some(&before.grants[0].token)
        && request.decisions[0].interrupt_id == before.grants[0].interrupt_id
        && before.execution.snapshot.tasks[0].approved && before.execution.snapshot.tasks[0].status == TaskStatus::Ready
        && before.execution.snapshot.tasks[0].interrupt.as_ref().unwrap().id == before.grants[0].interrupt_id,
        "the same-port neighbor has a genuine consumed old Once and complete owned acceptance request");
    assert!(captured.repaired.grants == before.grants && captured.repaired.acceptances == before.acceptances
        && captured.raw_restored.tables[4] == captured.raw_original.tables[4]
        && captured.raw_restored.tables[5] == captured.raw_original.tables[5]
        && captured.raw_subject == captured.raw_original && captured.raw_after == captured.raw_restored,
        "the healthy same-port operation retains old consumed authority without new permission rows");
}
fn assert_bypass_closed_after<R>(captured: &BypassCaptured<R>) {
    let mut expected_next = captured.raw_restored.clone();
    expected_next.tables[0][0][3] = Value::Blob(captured.next_owner.to_be_bytes().to_vec());
    assert!(captured.after_ordinary == captured.repaired_ordinary && captured.repaired_ordinary == captured.next_ordinary
        && captured.repaired == captured.next && captured.raw_next == expected_next
        && captured.next_owner == captured.operation_owner.checked_add(1).unwrap()
        && captured.native_calls == 0 && captured.directory_removed,
        "the original and genuine successor owners preserve complete actual AFTER and retire normally");
}
fn assert_bypass_subject_unchanged<R>(captured: &BypassCaptured<R>) {
    assert!(
        captured.before_ordinary == captured.after_ordinary
            && captured.before == captured.repaired
            && captured.raw_subject == captured.raw_after
            && captured.raw_original == captured.raw_restored
            && captured.repaired.execution.snapshot.owner_generation
                == captured.before.execution.snapshot.owner_generation,
        "after the refusal primary, the complete subject state and owned header are unchanged"
    );
    assert_bypass_closed_after(captured);
}
fn bypass_store_class<R>(result: &Result<R, StoreError>) -> &'static str {
    match result {
        Ok(_) => "store_ok",
        Err(StoreError::UnsupportedOperation) => "store_unsupported_operation",
        Err(StoreError::Corrupt(_)) => "store_corrupt",
        Err(StoreError::OwnerConflict) => "store_owner_conflict",
        Err(StoreError::RevisionConflict) => "store_revision_conflict",
        Err(_) => "other_store_refusal",
    }
}
fn bypass_confinement_class<R>(
    result: &Result<R, RecoveryConfinementBackendError>,
) -> &'static str {
    match result {
        Ok(_) => "confinement_ok",
        Err(RecoveryConfinementBackendError::UnsupportedAuthorityProfile) => {
            "confinement_unsupported_authority_profile"
        }
        Err(RecoveryConfinementBackendError::TargetChanged) => "confinement_target_changed",
        Err(RecoveryConfinementBackendError::OwnerConflict) => "confinement_owner_conflict",
        Err(RecoveryConfinementBackendError::CorruptRecord) => "confinement_corrupt_record",
        Err(RecoveryConfinementBackendError::LimitExceeded) => "confinement_limit_exceeded",
        Err(_) => "other_confinement_refusal",
    }
}
fn bypass_store_operation(
    port: BypassPort,
    store: &SqliteStore,
    before: &Ordinary,
    prepared: &BypassPrepared,
) -> Result<StoredExecution, StoreError> {
    match port {
        BypassPort::Commit => store.commit(
            &before.execution.snapshot.execution_id,
            (&before.execution.snapshot).into(),
            prepared.plan.as_ref().unwrap().clone(),
        ),
        BypassPort::Cancel => {
            store.cancel_run_fenced(prepared.cancellation.as_ref().unwrap(), facts(105))
        }
        _ => unreachable!("the store-only parent selects one of its two fixed ports"),
    }
}
fn run_bypass_store_parent(port: BypassPort) {
    let neighbor = capture_bypass_after_normal_retirement(port, true, |store, before, prepared| {
        bypass_store_operation(port, store, before, prepared)
    });
    let subject = capture_bypass_after_normal_retirement(port, false, |store, before, prepared| {
        bypass_store_operation(port, store, before, prepared)
    });
    assert_bypass_genuine_setup(&neighbor);
    assert_bypass_old_once(&neighbor);
    assert!(
        neighbor
            .result
            .as_ref()
            .is_ok_and(|actual| actual == &neighbor.repaired.execution
                && actual.snapshot == neighbor.prepared.plan.as_ref().unwrap().snapshot
                && actual.snapshot.status == ExecutionStatus::Cancelled
                && neighbor.after_ordinary.journal.len()
                    == neighbor.before_ordinary.journal.len() + 1),
        "the separately genuine old Once succeeds through this same mutation port; class={}",
        bypass_store_class(&neighbor.result)
    );
    assert_bypass_closed_after(&neighbor);
    assert_bypass_genuine_setup(&subject);
    assert!(matches!(&subject.result, Err(StoreError::UnsupportedOperation)),
        "the legacy mutation port must refuse the owned target A header after complete normal retirement; class={}", bypass_store_class(&subject.result));
    assert_bypass_subject_unchanged(&subject);
}

#[test]
fn legacy_direct_commit_refuses_target_permission_acceptance_before_write() {
    run_bypass_store_parent(BypassPort::Commit);
}
#[test]
fn legacy_run_cancellation_refuses_target_permission_acceptance_before_write() {
    run_bypass_store_parent(BypassPort::Cancel);
}
#[test]
fn legacy_confinement_capture_refuses_target_permission_acceptance_after_prior_owner_retirement() {
    let capture = |store: &SqliteStore, _: &Ordinary, prepared: &BypassPrepared| {
        store.confinement_view_fenced(prepared.confinement.as_ref().unwrap())
    };
    let neighbor =
        capture_bypass_after_normal_retirement(BypassPort::ConfinementCapture, true, capture);
    let subject =
        capture_bypass_after_normal_retirement(BypassPort::ConfinementCapture, false, capture);
    assert_bypass_genuine_setup(&neighbor);
    assert_bypass_old_once(&neighbor);
    assert!(
        neighbor
            .result
            .as_ref()
            .is_ok_and(|actual| actual == &neighbor.before_ordinary.thread)
            && neighbor.before_ordinary == neighbor.after_ordinary
            && neighbor.before == neighbor.repaired,
        "the independent prior-owner old Once succeeds through the same full capture; class={}",
        bypass_confinement_class(&neighbor.result)
    );
    assert_bypass_closed_after(&neighbor);
    assert_bypass_genuine_setup(&subject);
    assert!(matches!(&subject.result, Err(RecoveryConfinementBackendError::UnsupportedAuthorityProfile)),
        "the legacy confinement capture must refuse target new authority after real owner-three retirement; class={}", bypass_confinement_class(&subject.result));
    assert_bypass_subject_unchanged(&subject);
}
#[test]
fn legacy_confinement_mutation_refuses_target_permission_acceptance_before_write() {
    let mutate = |store: &SqliteStore, before: &Ordinary, prepared: &BypassPrepared| {
        store.confine_recovery_fenced(
            &before.thread,
            prepared.confinement.as_ref().unwrap(),
            RecoveryConfinementReason::DispatchNotAuthorized,
            &prepared.fresh_turn,
            facts(105),
        )
    };
    let neighbor =
        capture_bypass_after_normal_retirement(BypassPort::ConfinementMutation, true, mutate);
    let subject =
        capture_bypass_after_normal_retirement(BypassPort::ConfinementMutation, false, mutate);
    assert_bypass_genuine_setup(&neighbor);
    assert_bypass_old_once(&neighbor);
    assert!(
        neighbor.result.as_ref().is_ok_and(|actual| match actual {
            RecoveryConfinementOutcome::Confined(receipt) =>
                receipt.thread_id == neighbor.before.thread.id
                    && receipt.thread_generation == neighbor.before.thread.generation
                    && receipt.execution_id == neighbor.before.execution.snapshot.execution_id
                    && receipt.original_fence == (&neighbor.before.execution.snapshot).into()
                    && receipt.new_fence == (&neighbor.repaired.execution.snapshot).into()
                    && receipt.original_resource_revision
                        == neighbor.before.thread.resource_revision
                    && receipt.new_resource_revision == neighbor.repaired.thread.resource_revision
                    && receipt.original_turn_id == neighbor.before.execution.snapshot.turn_id
                    && receipt.fresh_turn_id == neighbor.prepared.fresh_turn
                    && receipt.route == neighbor.before.execution.snapshot.definition.route
                    && receipt.definition_revision
                        == neighbor.before.execution.snapshot.definition.revision
                    && receipt.reason == RecoveryConfinementReason::DispatchNotAuthorized,
            RecoveryConfinementOutcome::AlreadyBlocked => false,
        }) && neighbor.repaired.execution.snapshot
            == neighbor.prepared.plan.as_ref().unwrap().snapshot
            && neighbor.repaired.execution.snapshot.status == ExecutionStatus::RecoveryBlocked
            && neighbor.repaired.execution.snapshot.failure.as_deref()
                == Some("recovery_dispatch_not_authorized")
            && neighbor.repaired.execution.snapshot.owner_generation == neighbor.operation_owner
            && neighbor.after_ordinary.journal.len() == neighbor.before_ordinary.journal.len() + 1,
        "the independent old Once reaches lawful same-port no-dispatch confinement; class={}",
        bypass_confinement_class(&neighbor.result)
    );
    assert_bypass_closed_after(&neighbor);
    assert_bypass_genuine_setup(&subject);
    assert!(matches!(&subject.result, Err(RecoveryConfinementBackendError::UnsupportedAuthorityProfile)),
        "the legacy confinement mutation must refuse the owned target A header before writing; class={}", bypass_confinement_class(&subject.result));
    assert_bypass_subject_unchanged(&subject);
}
