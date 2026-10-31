//! Explicit ignored infrastructure for a genuine old-writer schema3 archive.
//! This producer qualifies no permission, migration, or feature acceptance case.
//! The caller owns the fresh private root and retains the archive after return.

#![cfg(unix)]

use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::{types::Value as SqlValue, Connection, OpenFlags};
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder},
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

// Frozen current schema3 SQL checksum, not a future include_str of schema.sql.
const SCHEMA3_CHECKSUM: &str = "98dd75f10f1560859f13742a0059f4e0c4f9f8fdcf7a701c731e0a49837353c6";
const TABLE_QUERIES: [(&str, &str); 6] = [
    ("metadata", "SELECT * FROM metadata ORDER BY id"),
    ("threads", "SELECT * FROM threads ORDER BY id"),
    ("executions", "SELECT * FROM executions ORDER BY id"),
    ("journal", "SELECT * FROM journal ORDER BY sequence"),
    (
        "approval_grants",
        "SELECT * FROM approval_grants ORDER BY execution_id, interrupt_id",
    ),
    (
        "approval_acceptances",
        "SELECT * FROM approval_acceptances ORDER BY execution_id, turn_id",
    ),
];
type LogicalCells = Vec<(String, Vec<Vec<SqlValue>>)>;
type Service = ApprovalService<Arc<SqliteStore>, Arc<ExactAuthority>>;

// Errors and boolean assertions never print private paths or sensitive values.
fn must<T, E>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|_| panic!("schema3 producer operation failed"))
}
fn present<T>(value: Option<T>) -> T {
    value.unwrap_or_else(|| panic!("schema3 producer expected stored value missing"))
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn readonly(path: &Path) -> Connection {
    must(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    ))
}

fn schema3_guard(path: &Path, owner: u64) {
    let db = readonly(path);
    let metadata: (i64, String, Vec<u8>) = must(db.query_row(
        "SELECT schema_version, migration_checksum, owner_epoch FROM metadata WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ));
    let count: i64 = must(db.query_row("SELECT count(*) FROM metadata", [], |row| row.get(0)));
    assert!(
        count == 1
            && metadata.0 == 3
            && metadata.1 == SCHEMA3_CHECKSUM
            && metadata.2 == owner.to_be_bytes(),
        "producer requires the exact prior schema3 metadata and owner"
    );
    let mut statement = must(db.prepare(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
    ));
    let rows = must(statement.query_map([], |row| row.get::<_, String>(0)));
    let tables = must(rows.collect::<Result<Vec<_>, _>>());
    assert!(
        tables
            == [
                "approval_acceptances",
                "approval_grants",
                "executions",
                "journal",
                "metadata",
                "threads"
            ],
        "producer requires exactly the six prior schema3 tables"
    );
}

// Preserve SQLite cell types and exact stored JSON/BLOB bytes without decoding.
fn logical_cells(path: &Path) -> LogicalCells {
    let db = readonly(path);
    let mut tables = Vec::new();
    for (table, query) in TABLE_QUERIES {
        let mut statement = must(db.prepare(query));
        let columns = statement.column_count();
        let rows = must(statement.query_map([], |row| {
            (0..columns)
                .map(|column| row.get(column))
                .collect::<Result<Vec<SqlValue>, _>>()
        }));
        tables.push((table.to_owned(), must(rows.collect::<Result<Vec<_>, _>>())));
    }
    tables
}

struct SeedCase {
    label: &'static str,
    thread_id: ThreadId,
    execution_id: ExecutionId,
    original_turn: TurnId,
    interrupt_id: InterruptId,
    definition: Definition,
    now_ms: u64,
}
impl SeedCase {
    fn new(label: &'static str, now_ms: u64) -> Self {
        Self {
            label,
            thread_id: must(ThreadId::new(format!("schema3-{label}-thread"))),
            execution_id: must(ExecutionId::new(format!("schema3-{label}-execution"))),
            original_turn: must(TurnId::new(format!("schema3-{label}-original-turn"))),
            interrupt_id: must(InterruptId::new(format!("schema3-{label}-interrupt"))),
            definition: Definition {
                route: must(RouteKey::new(format!("/schema3-{label}#agent"))),
                revision: must(ExecutionRevision::new("schema3-definition-1")),
                batches: vec![vec![TaskSpec {
                    id: must(TaskId::new(format!("schema3-{label}-task"))),
                    effect_id: must(EffectId::new(format!("schema3-{label}-effect"))),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: true,
                }]],
                max_attempts: 3,
            },
            now_ms,
        }
    }
}

struct ExactAuthority {
    context: ApprovalContext,
    definitions: Vec<Definition>,
}
impl ApprovalAuthority for ExactAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if context == &self.context
            && resource.thread.origin.as_ref() == Some(&context.actor)
            && self
                .definitions
                .iter()
                .any(|definition| definition == &resource.execution.snapshot.definition)
        {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
struct CountCalls(Arc<AtomicUsize>);
impl NativeEffect for CountCalls {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"unexpected_dispatch": true}),
            writes: vec![],
        }))
    }
}

fn fence(store: &SqliteStore, thread: &ThreadId) -> ApprovalFence {
    let view = present(must(store.thread_view(thread)));
    ApprovalFence {
        execution: (&present(view.execution).snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}

struct Parked {
    stored: StoredExecution,
    pending: Vec<SensitivePendingApproval>,
}
fn seed_and_park(
    store: &SqliteStore,
    application: &NativeApplication,
    service: &Service,
    context: &ApprovalContext,
    case: &SeedCase,
) -> Parked {
    let registered = present(application.definition(&case.definition.route)).clone();
    assert!(
        registered == case.definition,
        "seed must use the real exact current registration"
    );
    must(store.create_thread_with(ThreadSeed {
        id: case.thread_id.clone(),
        created_at_ms: case.now_ms,
        metadata: Map::from_iter([("fixture".into(), json!("schema3"))]),
        origin: Some(context.actor.clone()),
    }));
    let initial = must(new_execution(
        case.execution_id.clone(),
        case.original_turn.clone(),
        store.owner_generation(),
        registered,
        Map::from_iter([(
            "input".into(),
            json!({
                "label": case.label, "nullable": null, "ordered": [3, 1, 2],
            }),
        )]),
        facts(case.now_ms),
    ));
    let admitted = must(store.admit(&case.thread_id, initial));
    let running = must(store.commit(
        &case.execution_id,
        (&admitted.snapshot).into(),
        must(transition(
            &admitted.snapshot,
            KernelCommand::Start,
            facts(case.now_ms + 1),
        )),
    ));
    assert!(
        running.snapshot.status == ExecutionStatus::Running,
        "real Start must precede park"
    );
    let task = &case.definition.batches[0][0];
    let prompt =
        json!({"fixture": case.label, "question": "Retain prior approval?", "nullable": null});
    let parked = must(service.park(
        context,
        &case.execution_id,
        fence(store, &case.thread_id),
        task.id.clone(),
        case.interrupt_id.clone(),
        prompt.clone(),
        1_000,
        facts(case.now_ms + 2),
    ));
    let pending = must(service.pending(context, &case.execution_id, fence(store, &case.thread_id)));
    let view = present(must(store.approval_view(&case.execution_id)));
    assert!(
        parked.snapshot.status == ExecutionStatus::WaitingForApproval
            && view.execution == parked
            && pending.len() == 1
            && view.grants.len() == 1
            && view.acceptances.is_empty(),
        "real stable park must contain one untouched grant and no acceptance"
    );
    let grant = &view.grants[0];
    assert!(
        grant.thread_id == case.thread_id
            && grant.thread_generation == view.thread.generation
            && grant.origin == context.actor
            && grant.policy_revision == context.policy_revision
            && grant.execution_id == case.execution_id
            && grant.route == case.definition.route
            && grant.definition_revision == case.definition.revision
            && grant.task_id == task.id
            && grant.effect_id == task.effect_id
            && grant.interrupt_id == case.interrupt_id
            && grant.task_parking_revision == parked.snapshot.revision
            && grant.prompt == prompt,
        "real generated grant must retain the complete original binding"
    );
    assert!(
        grant.token == pending[0].token
            && grant.verifier == token_verifier(&pending[0].token)
            && grant.resume_key == pending[0].resume_key
            && grant.proposal_digest == must(proposal_digest(&parked.snapshot.tasks[0], &prompt))
            && pending[0].task_id == task.id
            && pending[0].interrupt_id == case.interrupt_id
            && pending[0].prompt == prompt
            && pending[0].expires_at_ms == grant.expires_at_ms,
        "real pending capability and proposal must match without disclosing sensitive values"
    );
    assert!(
        !grant.consumed
            && grant.issued_at_ms == case.now_ms + 2
            && grant.expires_at_ms == case.now_ms + 1_002
            && grant.issued_at_ms <= case.now_ms + 3
            && case.now_ms + 3 < grant.expires_at_ms,
        "real generated grant must be unconsumed and legal at the future acceptance time"
    );
    Parked {
        stored: parked,
        pending,
    }
}

fn caller_root() -> PathBuf {
    let root = PathBuf::from(present(std::env::var_os("B4_SCHEMA3_FIXTURE_ROOT")));
    let metadata = must(fs::symlink_metadata(&root));
    assert!(
        root.is_absolute()
            && metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode() & 0o7777 == 0o700
            && must(fs::canonicalize(&root)) == root,
        "caller must reserve a canonical absolute private directory700"
    );
    assert!(
        must(fs::read_dir(&root)).next().is_none(),
        "caller private root must be fresh and empty"
    );
    root
}

#[test]
#[ignore = "private schema3 producer infrastructure; explicit source-bound invocation only"]
fn produce_genuine_schema3_fixture_archive() {
    let root = caller_root();
    let source = root.join("source");
    must(DirBuilder::new().mode(0o700).create(&source));
    must(fs::set_permissions(
        &source,
        fs::Permissions::from_mode(0o700),
    ));
    assert!(
        must(fs::symlink_metadata(&source)).permissions().mode() & 0o7777 == 0o700,
        "source directory must remain private700"
    );
    let source_db = source.join("runtime.sqlite3");
    let archive = root.join("schema3-fixture.sqlite3");
    assert!(!archive.exists(), "archive destination must be absent");
    let store = Arc::new(must(SqliteStore::open(&source)));
    let original_owner = store.owner_generation();
    // Refuse a future writer before any seeding or archive publication.
    schema3_guard(&source_db, original_owner);
    let pending_case = SeedCase::new("pending", 100);
    let once_case = SeedCase::new("once", 200);
    let calls = Arc::new(AtomicUsize::new(0));
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "schema3-fixture-app".into(),
            tenant: "fixture-tenant".into(),
            principal: "fixture-principal".into(),
        },
        policy_revision: "schema3-policy-1".into(),
    };
    let application = Arc::new(must(NativeApplication::new(
        ApplicationIdentity {
            application: context.actor.application.clone(),
            revision: "schema3-application-1".into(),
        },
        [&pending_case, &once_case]
            .into_iter()
            .map(|case| RouteRegistration {
                definition: case.definition.clone(),
                effects: BTreeMap::from([(
                    case.definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            })
            .collect(),
    )));
    let authority = Arc::new(ExactAuthority {
        context: context.clone(),
        definitions: vec![
            pending_case.definition.clone(),
            once_case.definition.clone(),
        ],
    });
    let service = must(ApprovalService::new(store.clone(), authority.clone()));
    let pending = seed_and_park(
        store.as_ref(),
        &application,
        &service,
        &context,
        &pending_case,
    );
    let once = seed_and_park(store.as_ref(), &application, &service, &context, &once_case);
    let approved_turn = must(TurnId::new("schema3-once-approved-turn"));
    let accepted = must(service.accept(
        &once_case.execution_id,
        fence(store.as_ref(), &once_case.thread_id),
        ApprovalAcceptanceRequest {
            context: context.clone(),
            route: once_case.definition.route.clone(),
            definition_revision: once_case.definition.revision.clone(),
            turn_id: approved_turn.clone(),
            now_ms: 203,
            decisions: vec![GrantDecision {
                interrupt_id: once_case.interrupt_id.clone(),
                decision: ApprovalDecision::Once,
                cancelled: false,
                token: Some(once.pending[0].token.clone()),
            }],
        },
    ));
    assert!(
        accepted.snapshot.status == ExecutionStatus::Running
            && accepted.snapshot.turn_id == approved_turn
            && accepted.snapshot.owner_generation == original_owner
            && accepted.snapshot.tasks.len() == 1
            && accepted.snapshot.tasks[0].status == TaskStatus::Ready
            && accepted.snapshot.tasks[0].approved
            && accepted.snapshot.tasks[0].attempt_id.is_none()
            && accepted.snapshot.attempts_used == 0
            && accepted.snapshot.attempt_history.is_empty(),
        "real Once continuation must be approved Ready with no dispatched attempt"
    );
    let pending_view = present(must(store.approval_view(&pending_case.execution_id)));
    let accepted_view = present(must(store.approval_view(&once_case.execution_id)));
    assert!(
        pending_view.execution == pending.stored
            && pending_view.grants.len() == 1
            && !pending_view.grants[0].consumed
            && pending_view.acceptances.is_empty(),
        "the pending seed must retain its original unconsumed grant"
    );
    assert!(
        accepted_view.execution == accepted
            && accepted_view.grants.len() == 1
            && accepted_view.grants[0].consumed
            && accepted_view.acceptances.len() == 1
            && accepted_view.acceptances[0].context == context
            && accepted_view.acceptances[0].thread_id == once_case.thread_id
            && accepted_view.acceptances[0].thread_generation == accepted_view.thread.generation
            && accepted_view.acceptances[0].execution_id == once_case.execution_id
            && accepted_view.acceptances[0].turn_id == approved_turn
            && accepted_view.acceptances[0].previous_revision == once.stored.snapshot.revision
            && accepted_view.acceptances[0].accepted_at_ms == 203
            && accepted_view.acceptances[0].definition_revision == once_case.definition.revision
            && accepted_view.acceptances[0].decisions.len() == 1
            && accepted_view.acceptances[0].decisions[0].decision == ApprovalDecision::Once
            && accepted_view.acceptances[0].decisions[0].task_id
                == once_case.definition.batches[0][0].id
            && accepted_view.acceptances[0].decisions[0].interrupt_id == once_case.interrupt_id
            && accepted_view.acceptances[0].decisions[0].task_parking_revision
                == once.stored.snapshot.revision
            && accepted_view.acceptances[0].decisions[0].proposal_digest
                == accepted_view.grants[0].proposal_digest,
        "real Once acceptance must retain consumed grant and original acceptance provenance"
    );
    assert!(
        pending_view.thread.generation != accepted_view.thread.generation,
        "real thread generations must be distinct"
    );
    let pending_thread = present(must(store.thread_view(&pending_case.thread_id)));
    let once_thread = present(must(store.thread_view(&once_case.thread_id)));
    schema3_guard(&source_db, original_owner);
    assert!(
        calls.load(Ordering::SeqCst) == 0,
        "producer must never invoke business callbacks"
    );
    drop(service);
    assert!(
        Arc::strong_count(&store) == 1,
        "all old store aliases must retire before export"
    );
    let mut owned =
        Arc::try_unwrap(store).unwrap_or_else(|_| panic!("source store was not uniquely owned"));
    let cut = logical_cells(&source_db);
    assert!(
        cut.len() == 6
            && cut[0].0 == "metadata"
            && cut[0].1.len() == 1
            && cut[0].1[0].len() == 4
            && cut[1].1.len() == 2
            && cut[2].1.len() == 2
            && cut[3].1.len() == 9
            && cut[4].1.len() == 2
            && cut[5].1.len() == 1,
        "canonical schema3 cut must retain the exact finite public-API seed inventory"
    );
    must(owned.export_backup(&archive));
    let archive_metadata = must(fs::symlink_metadata(&archive));
    assert!(
        archive_metadata.is_file()
            && !archive_metadata.file_type().is_symlink()
            && archive_metadata.permissions().mode() & 0o7777 == 0o600
            && archive_metadata.len() > 0,
        "standalone archive must be a private regular file600"
    );
    schema3_guard(&archive, original_owner);
    assert!(
        logical_cells(&source_db) == cut,
        "export must retain the exact six-table source cut"
    );
    assert!(
        logical_cells(&archive) == cut,
        "archive must retain every exact ordered JSON and BLOB cell"
    );
    // No archive qualification can depend on an old writer remaining open.
    drop(owned);
    drop(application);
    drop(authority);
    assert!(
        calls.load(Ordering::SeqCst) == 0,
        "normal retirement must not dispatch callbacks"
    );
    let next = must(SqliteStore::open(&source));
    let next_owner = next.owner_generation();
    assert!(
        next_owner == present(original_owner.checked_add(1)),
        "genuine next source writer must advance owner once"
    );
    assert!(
        present(must(next.thread_view(&pending_case.thread_id))) == pending_thread
            && present(must(next.thread_view(&once_case.thread_id))) == once_thread,
        "ordinary next-writer projections must preserve both prior thread generations and continuations"
    );
    drop(next);
    let mut expected_next_cut = cut.clone();
    expected_next_cut[0].1[0][3] = SqlValue::Blob(next_owner.to_be_bytes().to_vec());
    assert!(
        logical_cells(&source_db) == expected_next_cut,
        "next source open may change only the current owner metadata cell"
    );
    schema3_guard(&archive, original_owner);
    assert!(
        logical_cells(&archive) == cut,
        "retained archive owner and six-table cut must remain original"
    );
    assert!(
        calls.load(Ordering::SeqCst) == 0,
        "producer must finish with zero business callback invocations"
    );
    // The caller's root and standalone archive survive this infrastructure helper.
    must(fs::remove_dir_all(&source));
}
