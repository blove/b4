use b4_contract::*;
use b4_kernel::new_execution;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use fs2::FileExt;
use rusqlite::{types::Value as SqlValue, Connection, OpenFlags};
use serde_json::{json, Map};
use std::{
    fs::{DirBuilder, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

// All assertions involving sensitive rows use static messages. These fixtures
// qualify synchronous trusted SQLite final cuts, not HTTP shutdown accounting.
pub fn must<T, E>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|_| panic!("Controlled backup fixture operation failed"))
}

pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let generation = must(generate_thread_generation());
        let path = std::env::temp_dir().join(format!("b4-backup-restore-{}", generation.as_str()));
        must(DirBuilder::new().mode(0o700).create(&path));
        Self(path)
    }
    pub fn child(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        must(DirBuilder::new().mode(0o700).create(&path));
        path
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, PartialEq)]
pub struct LogicalRows(pub Vec<(String, Vec<Vec<SqlValue>>)>);

pub fn rows(path: &Path) -> LogicalRows {
    let db = must(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    ));
    let mut tables = Vec::new();
    for (table, order) in [
        ("metadata", "id"),
        ("threads", "id"),
        ("executions", "id"),
        ("journal", "sequence"),
        ("approval_grants", "execution_id, interrupt_id"),
        ("approval_acceptances", "execution_id, turn_id"),
    ] {
        let mut statement = must(db.prepare(&format!("SELECT * FROM {table} ORDER BY {order}")));
        let columns = statement.column_count();
        let entries = must(statement.query_map([], |row| {
            (0..columns)
                .map(|column| row.get(column))
                .collect::<Result<Vec<SqlValue>, _>>()
        }));
        tables.push((table.to_owned(), must(entries.collect())));
    }
    LogicalRows(tables)
}

pub fn authority_rows(all: &LogicalRows) -> LogicalRows {
    LogicalRows(
        all.0
            .iter()
            .filter(|(table, _)| table.starts_with("approval_"))
            .cloned()
            .collect(),
    )
}

pub fn assert_journal_extension(before: &LogicalRows, after: &LogicalRows, added: usize) {
    let journal = |all: &LogicalRows| {
        all.0
            .iter()
            .find(|(table, _)| table == "journal")
            .unwrap()
            .1
            .clone()
    };
    let original = journal(before);
    let current = journal(after);
    assert_eq!(
        current.len(),
        original.len() + added,
        "Only expected transitions may append journal records"
    );
    assert!(
        current[..original.len()] == original,
        "Every original ordered journal record must remain exact"
    );
}

pub fn owner_locked(directory: &Path) -> bool {
    let file = must(
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join("owner.lock")),
    );
    match file.try_lock_exclusive() {
        Ok(()) => {
            must(FileExt::unlock(&file));
            false
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => true,
        Err(_) => panic!("Owner-lock probe failed"),
    }
}

// Installation is test-only: fresh private directory, exclusive private file,
// streamed copy, and synchronization. It is not a production restore command.
pub fn install(archive: &Path, directory: &Path) {
    let mut input = must(File::open(archive));
    let mut output = must(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join("runtime.sqlite3")),
    );
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = must(input.read(&mut buffer));
        if count == 0 {
            break;
        }
        must(output.write_all(&buffer[..count]));
    }
    must(output.sync_all());
    must(File::open(directory).and_then(|parent| parent.sync_all()));
}

pub fn thread() -> ThreadId {
    must(ThreadId::new("backup-thread"))
}
pub fn execution() -> ExecutionId {
    must(ExecutionId::new("backup-execution"))
}
pub fn task(name: &str) -> TaskId {
    must(TaskId::new(name))
}
pub fn attempt(name: &str) -> AttemptId {
    must(AttemptId::new(name))
}
pub fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
pub fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "backup-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    }
}
pub fn definition(recovery: RecoveryPolicy) -> Definition {
    Definition {
        route: must(RouteKey::new("/backup#workflow")),
        revision: must(ExecutionRevision::new("definition-1")),
        batches: vec![vec![
            TaskSpec {
                id: task("sibling"),
                effect_id: must(EffectId::new("sibling-effect")),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            },
            TaskSpec {
                id: task("gated"),
                effect_id: must(EffectId::new("gated-effect")),
                recovery,
                requires_approval: true,
            },
        ]],
        max_attempts: 4,
    }
}

pub struct ExactAuthority(pub Definition);
impl ApprovalAuthority for ExactAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if resource.thread.origin.as_ref() == Some(&context.actor)
            && resource.execution.snapshot.definition == self.0
        {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
pub type Service = ApprovalService<Arc<SqliteStore>, ExactAuthority>;
pub fn service(store: Arc<SqliteStore>, available: Definition) -> Service {
    must(ApprovalService::new(store, ExactAuthority(available)))
}
pub fn view(store: &dyn Store) -> ThreadView {
    must(store.thread_view(&thread())).unwrap_or_else(|| panic!("Fixture thread disappeared"))
}
pub fn fence(view: &ThreadView) -> ApprovalFence {
    ApprovalFence {
        execution: (&view.execution.as_ref().unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}

#[derive(Default)]
pub struct Calls {
    pub sibling: AtomicUsize,
    pub gated: AtomicUsize,
}
pub struct Fixture {
    pub store: Arc<SqliteStore>,
    pub calls: Arc<Calls>,
    pub definition: Definition,
}
pub fn parked(directory: &Path, recovery: RecoveryPolicy) -> Fixture {
    let store = Arc::new(must(SqliteStore::open(directory)));
    let definition = definition(recovery);
    must(store.create_thread_with(ThreadSeed {
        id: thread(),
        created_at_ms: 100,
        metadata: Map::from_iter([("fixture".into(), json!("backup"))]),
        origin: Some(context().actor),
    }));
    let runtime = Runtime::new(store.clone());
    let admitted = must(runtime.admit(
        &thread(),
        must(new_execution(
            execution(),
            must(TurnId::new("initial-turn")),
            store.owner_generation(),
            definition.clone(),
            Map::from_iter([("input".into(), json!({"fixture":"preserved"}))]),
            facts(100),
        )),
    ));
    must(runtime.apply(
        &execution(),
        (&admitted.snapshot).into(),
        KernelCommand::Start,
        facts(101),
    ));
    let calls = Arc::new(Calls::default());
    let approvals = service(store.clone(), definition.clone());
    must(approvals.dispatch_outcome(
        &context(),
        &execution(),
        fence(&view(store.as_ref())),
        task("sibling"),
        attempt("sibling-attempt"),
        facts(102),
        || facts(103),
        |_| {
            calls.sibling.fetch_add(1, Ordering::SeqCst);
            Ok(EffectOutcome::Succeeded(TaskResult {
                output: json!({"sibling":true}),
                writes: vec![StateWrite::Append {
                    field: "joined".into(),
                    items: vec![json!("sibling")],
                }],
            }))
        },
    ));
    let parked = must(approvals.park(
        &context(),
        &execution(),
        fence(&view(store.as_ref())),
        task("gated"),
        must(InterruptId::new("original-interrupt")),
        json!({"question":"Authorize fixture?", "arguments":{"nullable":null}}),
        100,
        facts(104),
    ));
    assert_eq!(parked.snapshot.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(calls.sibling.load(Ordering::SeqCst), 1);
    assert_eq!(parked.snapshot.attempts_used, 1);
    drop(approvals);
    drop(runtime);
    Fixture {
        store,
        calls,
        definition,
    }
}

pub fn pending(service: &Service) -> Vec<SensitivePendingApproval> {
    must(service.pending(
        &context(),
        &execution(),
        fence(&view(service.store().as_ref())),
    ))
}
pub fn acceptance(
    view: &ThreadView,
    pending: &[SensitivePendingApproval],
    now_ms: u64,
) -> ApprovalAcceptanceRequest {
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    ApprovalAcceptanceRequest {
        context: context(),
        route: snapshot.definition.route.clone(),
        definition_revision: snapshot.definition.revision.clone(),
        turn_id: must(TurnId::new("approved-turn")),
        now_ms,
        decisions: pending
            .iter()
            .map(|entry| GrantDecision {
                interrupt_id: entry.interrupt_id.clone(),
                decision: ApprovalDecision::Once,
                cancelled: false,
                token: Some(entry.token.clone()),
            })
            .collect(),
    }
}
pub fn accept(service: &Service, now_ms: u64) -> StoredExecution {
    let captured = view(service.store().as_ref());
    must(service.accept(
        &execution(),
        fence(&captured),
        acceptance(&captured, &pending(service), now_ms),
    ))
}
pub fn recovery(
    view: &ThreadView,
    available_definition: Definition,
    now_ms: u64,
) -> ApprovalRecoveryRequest {
    ApprovalRecoveryRequest {
        context: context(),
        thread_id: view.thread.id.clone(),
        thread_generation: view.thread.generation.clone(),
        execution_id: view
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .execution_id
            .clone(),
        available_definition,
        turn_id: must(TurnId::new("recovered-turn")),
        facts: facts(now_ms),
    }
}
pub fn recover(service: &Service, now_ms: u64) -> StoredExecution {
    let captured = view(service.store().as_ref());
    must(
        service.recover(
            &execution(),
            fence(&captured),
            recovery(
                &captured,
                captured
                    .execution
                    .as_ref()
                    .unwrap()
                    .snapshot
                    .definition
                    .clone(),
                now_ms,
            ),
        ),
    )
}
pub fn complete(service: &Service, calls: &Calls, now_ms: u64) -> StoredExecution {
    must(service.dispatch_outcome(
        &context(),
        &execution(),
        fence(&view(service.store().as_ref())),
        task("gated"),
        attempt("gated-attempt"),
        facts(now_ms),
        || facts(now_ms + 1),
        |invocation| {
            calls.gated.fetch_add(1, Ordering::SeqCst);
            assert_eq!(invocation.task_id, task("gated"));
            Ok(EffectOutcome::Succeeded(TaskResult {
                output: json!({"gated":true}),
                writes: vec![StateWrite::Append {
                    field: "joined".into(),
                    items: vec![json!("gated")],
                }],
            }))
        },
    ))
}

pub fn export_and_retire(
    fixture: Fixture,
    source: &Path,
    archive: &Path,
) -> (LogicalRows, Arc<Calls>, Definition) {
    let Fixture {
        store,
        calls,
        definition,
    } = fixture;
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "All synchronous runtime/service references must retire before export"
    );
    let mut store = Arc::try_unwrap(store)
        .unwrap_or_else(|_| panic!("Store ownership was not unique at the final cut"));
    let before = rows(&source.join("runtime.sqlite3"));
    assert!(
        owner_locked(source),
        "Source owner lock must be held before export"
    );
    store
        .export_backup(archive)
        .unwrap_or_else(|error| panic!("Final-cut export did not publish: {error}"));
    assert!(
        rows(&source.join("runtime.sqlite3")) == before,
        "Export changed source logical records"
    );
    assert!(
        rows(archive) == before,
        "Archive omitted or changed logical cut records"
    );
    assert!(owner_locked(source), "Export must retain source owner lock");
    drop(store);
    assert!(
        !owner_locked(source),
        "Source destructor must release actual owner lock"
    );
    // The retired source is never reopened. Restore operates in a different,
    // fresh directory only after all original connections and writers closed.
    (before, calls, definition)
}

pub fn open_restored(archive: &Path, restored: &Path, cut: &LogicalRows) -> Arc<SqliteStore> {
    install(archive, restored);
    assert!(
        rows(&restored.join("runtime.sqlite3")) == *cut,
        "Test installation changed cut records"
    );
    let store = Arc::new(must(SqliteStore::open(restored)));
    assert_eq!(store.owner_generation(), 2);
    assert!(
        matches!(SqliteStore::open(restored), Err(StoreError::AlreadyOwned)),
        "Restored directory must independently retain owner lock"
    );
    let mut expected = cut.clone();
    let metadata = &mut expected
        .0
        .iter_mut()
        .find(|(table, _)| table == "metadata")
        .unwrap()
        .1;
    assert_eq!(metadata.len(), 1);
    metadata[0][3] = SqlValue::Blob(2_u64.to_be_bytes().to_vec());
    assert!(
        rows(&restored.join("runtime.sqlite3")) == expected,
        "Opening may advance only the copied Store owner epoch"
    );
    store
}
