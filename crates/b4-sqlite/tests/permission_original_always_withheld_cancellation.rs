//! Failure-owned withdrawal of genuine original human Always Work after explicit entry refusal.
//! No permission records, callback receipts or terminal outcomes are synthesized.
use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::{types::Value as Cell, Connection};
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};

const APP: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";
const TASK: &str = "publish-monthly-report";
const EFFECT: &str = "reports.publish-monthly";
static NEXT: AtomicU64 = AtomicU64::new(1);
fn id() -> String {
    format!(
        "b4ca1100-0000-4000-8000-{:012x}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: APP.into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        },
        policy_revision: "policy-1".into(),
    }
}
fn result() -> TaskResult {
    TaskResult {
        output: json!({"published": true, "month": "2026-09"}),
        writes: vec![],
    }
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-permission-original-always-withheld-cancellation-{}-{}",
            std::process::id(),
            id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn connection(path: &Path) -> Connection {
    Connection::open(path.join("runtime.sqlite3")).unwrap()
}

fn integrity(path: &Path) {
    let db = connection(path);
    let result: String = db
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(result, "ok");
    db.close().unwrap();
}

// All observations are capped with a sentinel. Original terminal6 plus fresh
// NewUse terminal4 needs thirteen journal rows, including thread creation.
fn raw(path: &Path) -> Vec<Vec<Vec<Cell>>> {
    let db = connection(path);
    [
        ("metadata", "id", 1),
        ("threads", "id", 1),
        ("executions", "id", 2),
        ("journal", "sequence", 13),
        ("approval_grants", "execution_id, interrupt_id", 1),
        ("approval_acceptances", "execution_id, turn_id", 1),
        ("permission_acceptances", "execution_id, turn_id", 1),
        ("permission_decisions", "permission_id", 1),
        ("permission_uses", "execution_id, task_id", 1),
    ]
    .map(|(table, order, cap)| {
        let mut stmt = db
            .prepare(&format!(
                "SELECT * FROM {table} ORDER BY {order} LIMIT {}",
                cap + 1
            ))
            .unwrap();
        let cols = stmt.column_count();
        let rows = stmt
            .query_map([], |r| {
                (0..cols)
                    .map(|i| r.get(i))
                    .collect::<Result<Vec<Cell>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            rows.len() <= cap,
            "bounded {table} observer reached sentinel"
        );
        rows
    })
    .into_iter()
    .collect()
}
#[derive(Clone, Debug, PartialEq)]
struct Capture {
    view: ThreadView,
    executions: BTreeMap<String, StoredExecution>,
    journals: BTreeMap<String, Vec<JournalEntry>>,
    raw: Vec<Vec<Vec<Cell>>>,
}
fn capture(store: &SqliteStore, path: &Path, thread: &ThreadId) -> Capture {
    let raw = raw(path);
    let ids: Vec<_> = raw[2]
        .iter()
        .map(|r| match &r[0] {
            Cell::Text(v) => ExecutionId::new(v.clone()).unwrap(),
            _ => panic!("execution ID"),
        })
        .collect();
    Capture {
        view: store.thread_view(thread).unwrap().unwrap(),
        executions: ids
            .iter()
            .map(|id| (id.to_string(), store.execution(id).unwrap().unwrap()))
            .collect(),
        journals: ids
            .iter()
            .map(|id| (id.to_string(), store.journal(id).unwrap()))
            .collect(),
        raw,
    }
}
fn expected(before: &StoredExecution, command: KernelCommand, now: u64) -> StoredExecution {
    StoredExecution {
        thread_id: before.thread_id.clone(),
        snapshot: transition(&before.snapshot, command, facts(now))
            .unwrap()
            .snapshot,
    }
}
// Independent complete nine-table projection. IDs locate rows; UUID ordering is irrelevant.
fn project_commit(before: &Capture, expected: &StoredExecution, canonical: bool) -> Capture {
    let mut projected = before.clone();
    projected.view.thread.resource_revision += 1;
    projected.view.execution = Some(expected.clone());
    let execution = expected.snapshot.execution_id.to_string();
    projected
        .executions
        .insert(execution.clone(), expected.clone());
    let entry = JournalEntry::from(&expected.snapshot);
    projected
        .journals
        .get_mut(&execution)
        .unwrap()
        .push(entry.clone());
    projected.raw[1][0][5] = Cell::Blob(
        projected
            .view
            .thread
            .resource_revision
            .to_be_bytes()
            .to_vec(),
    );
    let row = projected.raw[2]
        .iter_mut()
        .find(|r| r[0] == Cell::Text(execution.clone()))
        .unwrap();
    row[2] = Cell::Blob(expected.snapshot.revision.to_be_bytes().to_vec());
    row[3] = Cell::Blob(expected.snapshot.owner_generation.to_be_bytes().to_vec());
    row[4] = Cell::Text(if canonical {
        serde_json::to_string(&serde_json::to_value(&expected.snapshot).unwrap()).unwrap()
    } else {
        serde_json::to_string(&expected.snapshot).unwrap()
    });
    let seq = match projected.raw[3].last().unwrap()[0] {
        Cell::Integer(v) => v + 1,
        _ => panic!("sequence"),
    };
    projected.raw[3].push(vec![
        Cell::Integer(seq),
        Cell::Text("commit".into()),
        Cell::Text(expected.thread_id.to_string()),
        Cell::Text(execution),
        Cell::Blob(entry.revision.to_be_bytes().to_vec()),
        Cell::Blob(entry.owner_generation.to_be_bytes().to_vec()),
        Cell::Text(serde_json::to_string(&entry.status).unwrap()),
        Cell::Blob(entry.updated_at_ms.to_be_bytes().to_vec()),
        Cell::Text(serde_json::to_string(&entry).unwrap()),
    ]);
    projected
}
fn assert_commit(before: &Capture, after: &Capture, expected: &StoredExecution, canonical: bool) {
    assert_eq!(
        &project_commit(before, expected, canonical),
        after,
        "complete typed/kernel/raw commit projection"
    );
}

struct Authority {
    store: Weak<SqliteStore>,
    definition: Definition,
}
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        actual: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        let store = self.store.upgrade().ok_or(ApprovalError::Unauthorized)?;
        let current = store
            .thread_view(&resource.thread.id)?
            .ok_or(ApprovalError::Unauthorized)?;
        if actual == &context()
            && resource.thread.origin.as_ref() == Some(&context().actor)
            && resource.execution.snapshot.definition == self.definition
            && current.thread == resource.thread
            && current.execution.as_ref() == Some(&resource.execution)
        {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
impl PermissionCancellationAuthority for Authority {
    fn authorize_cancel(
        &self,
        actual: &ApprovalContext,
        resource: &ApprovalResource,
        target: &RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        self.authorize(actual, ApprovalOperation::Dispatch, resource)
            .map_err(PermissionError::Approval)?;
        let view = ThreadView {
            thread: resource.thread.clone(),
            execution: Some(resource.execution.clone()),
        };
        if target == &RunCancellationTarget::from_view(&view)? {
            Ok(())
        } else {
            Err(PermissionError::Store(StoreError::RevisionConflict))
        }
    }
}
#[derive(Default)]
struct Constraint {
    revoked: AtomicBool,
    calls: AtomicUsize,
}
impl ToolConstraint for Constraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.revoked.load(Ordering::SeqCst) {
            Err(ToolConstraintError::Denied)
        } else {
            Ok(())
        }
    }
}
fn catalog(definition: &Definition, effect: Arc<dyn NativeEffect>) -> PermissionCatalog {
    catalog_with_constraint(definition, effect, Arc::new(Constraint::default()))
}
fn catalog_with_constraint(
    definition: &Definition,
    effect: Arc<dyn NativeEffect>,
    constraint: Arc<Constraint>,
) -> PermissionCatalog {
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: APP.into(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    EffectId::new(EFFECT).unwrap(),
                    EffectBinding {
                        callback: effect,
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    PermissionCatalog::new(
        application,
        vec![ToolBindingRegistration {
            definition: definition.clone(),
            effect_id: EffectId::new(EFFECT).unwrap(),
            tool_id: StableToolId::new("release-tool").unwrap(),
            tool_revision: StableToolRevision::new("tool-1").unwrap(),
            constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
            constraint,
        }],
    )
    .unwrap()
}
struct Direct(Arc<AtomicUsize>);
impl NativeEffect for Direct {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(result()))
    }
}
fn fence(store: &SqliteStore, thread: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}
fn ready_use(
    store: &SqliteStore,
    authority: &Authority,
    catalog: &PermissionCatalog,
    thread: &ThreadId,
    definition: &Definition,
    now: u64,
) -> StoredExecution {
    let execution = ExecutionId::new(id()).unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new(id()).unwrap(),
        store.owner_generation(),
        definition.clone(),
        Map::from_iter([(
            "input".into(),
            json!({"month":"2026-09", "labels":["monthly","review"], "dry_run":false}),
        )]),
        facts(now),
    )
    .unwrap();
    let admitted = store.admit(thread, initial).unwrap();
    let started = store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(now + 1)).unwrap(),
        )
        .unwrap();
    let use_id = PermissionUseId::new(format!(
        "b4pu_{:064x}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
    .unwrap();
    let command = b4_kernel::PermissionAuthorizationCommand::capture(
        &started.snapshot,
        TaskId::new(TASK).unwrap(),
        use_id,
    )
    .unwrap();
    let expected = b4_kernel::authorize_ready(&started.snapshot, &command, facts(now + 2))
        .unwrap()
        .snapshot;
    let ready = try_authorize_permission_ready(
        store,
        authority,
        catalog,
        fence(store, thread),
        &PermissionReadyAuthorizationRequest {
            context: &context(),
            command: &command,
            facts: facts(now + 2),
        },
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap()
    .expect("genuine saved permission matches");
    assert_eq!(ready.snapshot, expected);
    ready
}
struct Hooks {
    attempt: AttemptId,
    dispatch_ms: u64,
    completion_ms: u64,
}
impl PermissionUseDriverHooks for Hooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        facts(if current.snapshot.revision == 2 {
            self.dispatch_ms
        } else {
            self.completion_ms
        })
    }
    fn attempt_id(&self) -> AttemptId {
        self.attempt.clone()
    }
}
struct Entry {
    state: Capture,
    invocation: EffectInvocation,
    context: InvocationContext,
}
fn check_entry(entry: &Entry, expected: &Capture, revision: u64) {
    assert_eq!(&entry.state, expected);
    let claimed = expected.view.execution.as_ref().unwrap();
    assert_eq!(claimed.snapshot.revision, revision);
    assert_eq!(claimed.snapshot.status, ExecutionStatus::Running);
    assert_eq!(claimed.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        entry.invocation,
        EffectInvocation {
            execution_id: claimed.snapshot.execution_id.clone(),
            turn_id: claimed.snapshot.turn_id.clone(),
            owner_generation: claimed.snapshot.owner_generation,
            task_id: claimed.snapshot.tasks[0].spec.id.clone(),
            effect_id: claimed.snapshot.tasks[0].spec.effect_id.clone(),
            attempt_id: claimed.snapshot.tasks[0].attempt_id.clone().unwrap(),
            input: claimed.snapshot.tasks[0].input.clone(),
        }
    );
    assert_eq!(
        entry.context,
        InvocationContext {
            thread_id: expected.view.thread.id.clone(),
            thread_generation: expected.view.thread.generation.clone(),
            origin: context().actor,
            application_revision: "application-1".into(),
            execution_revision: claimed.snapshot.definition.revision.clone(),
            policy_revision: context().policy_revision,
        }
    );
}
// Real entered-original uncertainty and reopened NewUse success both record
// observations for assertions outside the native callback's panic boundary.
struct Controlled {
    store: Weak<SqliteStore>,
    path: PathBuf,
    thread: ThreadId,
    calls: AtomicUsize,
    direct_calls: AtomicUsize,
    entry: Mutex<Option<Entry>>,
    succeed: bool,
}
impl NativeEffect for Controlled {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.direct_calls.fetch_add(1, Ordering::SeqCst);
        Err(EffectUncertain)
    }
    fn invoke_controlled(
        &self,
        invocation: &EffectInvocation,
        ctx: &InvocationContext,
        _: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let store = self.store.upgrade().ok_or(EffectUncertain)?;
        let state = capture(&store, &self.path, &self.thread);
        *self.entry.lock().map_err(|_| EffectUncertain)? = Some(Entry {
            state,
            invocation: invocation.clone(),
            context: ctx.clone(),
        });
        if self.succeed {
            Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
                result(),
            )))
        } else {
            Err(EffectUncertain)
        }
    }
}
struct Admission {
    allow: bool,
    panic: bool,
    entries: AtomicUsize,
    attempt: AttemptId,
}
impl Admission {
    fn refused() -> Self {
        Self {
            allow: false,
            panic: false,
            entries: AtomicUsize::new(0),
            attempt: AttemptId::new(id()).unwrap(),
        }
    }
}
impl PermissionUseDriverHooks for Admission {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        facts(if current.snapshot.revision == 3 {
            108
        } else {
            110
        })
    }
    fn attempt_id(&self) -> AttemptId {
        self.attempt.clone()
    }
    fn enter_callback(&self) -> bool {
        self.entries.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "owned hook-panic control");
        self.allow
    }
}
struct Fixture {
    directory: Directory,
    store: Arc<SqliteStore>,
    authority: Arc<Authority>,
    catalog: PermissionCatalog,
    callback: Arc<Controlled>,
    constraint: Arc<Constraint>,
    thread: ThreadId,
    ready: StoredExecution,
}
impl Fixture {
    fn new() -> Self {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert!(store
            .as_ref()
            .permission_original_always_withheld_cancellation_atomic_durable());
        assert!(store.permission_original_always_withheld_cancellation_atomic_durable());
        let thread = ThreadId::new(id()).unwrap();
        let definition = Definition {
            route: RouteKey::new("/reports#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new(TASK).unwrap(),
                effect_id: EffectId::new(EFFECT).unwrap(),
                requires_approval: true,
                recovery: RecoveryPolicy::Pure,
            }]],
            max_attempts: 1,
        };
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(context().actor),
            })
            .unwrap();
        let authority = Arc::new(Authority {
            store: Arc::downgrade(&store),
            definition: definition.clone(),
        });
        let seed_calls = Arc::new(AtomicUsize::new(0));
        let seed_catalog = Arc::new(catalog(&definition, Arc::new(Direct(seed_calls.clone()))));
        let original_id = ExecutionId::new(id()).unwrap();
        let initial = new_execution(
            original_id.clone(),
            TurnId::new(id()).unwrap(),
            store.owner_generation(),
            definition.clone(),
            Map::from_iter([("input".into(), json!({"month":"2026-09"}))]),
            facts(100),
        )
        .unwrap();
        let admitted = store.admit(&thread, initial).unwrap();
        store
            .commit(
                &original_id,
                (&admitted.snapshot).into(),
                transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
            )
            .unwrap();
        let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
        service
            .park(
                &context(),
                &original_id,
                fence(&store, &thread),
                TaskId::new(TASK).unwrap(),
                InterruptId::new("publish-approval").unwrap(),
                json!({"question":"Publish monthly report?"}),
                60_000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(&context(), &original_id, fence(&store, &thread))
            .unwrap();
        let grant = &pending[0];
        accept_permission_always_with_normal_inputs(
            store.clone(),
            authority.clone(),
            seed_catalog.clone(),
            &original_id,
            fence(&store, &thread),
            ApprovalAcceptanceRequest {
                context: context(),
                route: definition.route.clone(),
                definition_revision: definition.revision.clone(),
                turn_id: TurnId::new(id()).unwrap(),
                now_ms: 103,
                decisions: vec![GrantDecision {
                    interrupt_id: grant.interrupt_id.clone(),
                    decision: ApprovalDecision::Always,
                    cancelled: false,
                    token: Some(grant.token.clone()),
                }],
            },
        )
        .unwrap();
        let ready = store.execution(&original_id).unwrap().unwrap();
        assert_eq!(ready.snapshot.revision, 3);
        assert_eq!(ready.snapshot.status, ExecutionStatus::Running);
        assert_eq!(ready.snapshot.tasks[0].status, TaskStatus::Ready);
        let genuine = capture(&store, &directory.0, &thread);
        assert_eq!(genuine.raw[2].len(), 1);
        assert_eq!(genuine.raw[3].len(), 5);
        assert_eq!(genuine.raw[4].len(), 1, "real consumed human grant");
        assert!(genuine.raw[5].is_empty(), "no legacy acceptance");
        assert_eq!(genuine.raw[6].len(), 1, "real Always acceptance");
        assert_eq!(genuine.raw[7].len(), 1, "saved permission decision");
        assert!(genuine.raw[8].is_empty(), "original has no Use authority");
        assert_eq!(seed_calls.load(Ordering::SeqCst), 0);
        drop(pending);
        drop(service);
        drop(seed_catalog);
        drop(seed_calls);
        let callback = Arc::new(Controlled {
            store: Arc::downgrade(&store),
            path: directory.0.clone(),
            thread: thread.clone(),
            calls: AtomicUsize::new(0),
            direct_calls: AtomicUsize::new(0),
            entry: Mutex::new(None),
            succeed: false,
        });
        let constraint = Arc::new(Constraint::default());
        let catalog = catalog_with_constraint(&definition, callback.clone(), constraint.clone());
        Self {
            directory,
            store,
            authority,
            catalog,
            callback,
            constraint,
            thread,
            ready,
        }
    }
    fn capture(&self) -> Capture {
        capture(&self.store, &self.directory.0, &self.thread)
    }
    fn bindings<'a>(
        &'a self,
        ctx: &'a ApprovalContext,
        capacity: &'a TaskCapacity,
        hooks: &'a Admission,
    ) -> PermissionOriginalAlwaysDriverBindings<'a, SqliteStore, Authority, Admission> {
        PermissionOriginalAlwaysDriverBindings {
            store: self.store.as_ref(),
            authority: self.authority.as_ref(),
            producer_catalog: &self.catalog,
            current_catalog: &self.catalog,
            context: ctx,
            hooks,
            capacity,
        }
    }
    fn retire(self, after: &Capture, callback_calls: usize, capacity: TaskCapacity, reuse: bool) {
        assert_eq!(self.callback.calls.load(Ordering::SeqCst), callback_calls);
        assert_eq!(self.callback.direct_calls.load(Ordering::SeqCst), 0);
        integrity(&self.directory.0);
        let Self {
            directory,
            store,
            authority,
            catalog: retired_catalog,
            callback,
            constraint,
            thread,
            ready: _,
        } = self;
        let definition = authority.definition.clone();
        let owner = store.owner_generation();
        drop(authority);
        drop(retired_catalog);
        drop(callback);
        drop(constraint);
        assert_eq!(Arc::strong_count(&store), 1);
        store.readiness().unwrap();
        drop(store);
        let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let actual = capture(&reopened, &directory.0, &thread);
        assert_eq!(actual.view, after.view);
        assert_eq!(actual.executions, after.executions);
        assert_eq!(actual.journals, after.journals);
        assert_eq!(actual.raw[1..], after.raw[1..]);
        assert_eq!(actual.raw[0][0][..3], after.raw[0][0][..3]);
        assert_eq!(reopened.owner_generation(), owner + 1);
        assert_eq!(
            actual.raw[0][0][3],
            Cell::Blob((owner + 1).to_be_bytes().to_vec())
        );
        if reuse {
            let authority = Authority {
                store: Arc::downgrade(&reopened),
                definition: definition.clone(),
            };
            let callback = Arc::new(Controlled {
                store: Arc::downgrade(&reopened),
                path: directory.0.clone(),
                thread: thread.clone(),
                calls: AtomicUsize::new(0),
                direct_calls: AtomicUsize::new(0),
                entry: Mutex::new(None),
                succeed: true,
            });
            let catalog = catalog(&definition, callback.clone());
            let ready = ready_use(&reopened, &authority, &catalog, &thread, &definition, 120);
            let hooks = Hooks {
                attempt: AttemptId::new(id()).unwrap(),
                dispatch_ms: 123,
                completion_ms: 125,
            };
            let ctx = context();
            let fresh_before = capture(&reopened, &directory.0, &thread);
            let dispatch = expected(
                &ready,
                KernelCommand::Dispatch {
                    task_id: TaskId::new(TASK).unwrap(),
                    attempt_id: hooks.attempt.clone(),
                },
                123,
            );
            let completion = expected(
                &dispatch,
                KernelCommand::Complete {
                    task_id: TaskId::new(TASK).unwrap(),
                    attempt_id: hooks.attempt.clone(),
                    result: result(),
                },
                125,
            );
            let settled = drive_permission_use_cancellable_execution(
                PermissionUseDriverBindings {
                    store: reopened.as_ref(),
                    authority: &authority,
                    producer_catalog: &catalog,
                    current_catalog: &catalog,
                    context: &ctx,
                    hooks: &hooks,
                    capacity: &capacity,
                },
                ready,
                &NativeCancellationSignal::new().observer(),
            )
            .unwrap();
            assert_eq!(settled, completion);
            assert_eq!(settled.snapshot.revision, 4);
            assert_eq!(settled.snapshot.status, ExecutionStatus::Completed);
            assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
            assert_eq!(callback.direct_calls.load(Ordering::SeqCst), 0);
            let expected_entry = project_commit(&fresh_before, &dispatch, true);
            check_entry(
                callback.entry.lock().unwrap().as_ref().unwrap(),
                &expected_entry,
                3,
            );
            let later = capture(&reopened, &directory.0, &thread);
            let dispatched = project_commit(&fresh_before, &dispatch, true);
            assert_commit(&dispatched, &later, &completion, false);
            assert_eq!(later.raw[3].len(), after.raw[3].len() + 5);
            assert_eq!(later.raw[8].len(), after.raw[8].len() + 1);
            assert_eq!(later.raw[3].len(), 13);
            assert!(after.raw[8].is_empty(), "original creator has no Use");
            assert_eq!(
                later.raw[8].len(),
                1,
                "only actual reopened NewUse has a Use"
            );
            assert_eq!(later.raw[4..8], after.raw[4..8]);
            for (id, execution) in &after.executions {
                assert_eq!(later.executions.get(id), Some(execution));
                assert_eq!(later.journals.get(id), after.journals.get(id));
            }
        }
        integrity(&directory.0);
        reopened.readiness().unwrap();
        assert_eq!(Arc::strong_count(&reopened), 1);
        drop(reopened);
        let path = directory.0.clone();
        drop(directory);
        assert!(!path.exists());
    }
}

fn withdraw(
    failure: &mut PermissionOriginalAlwaysDriverFailure<'_, SqliteStore, Authority>,
    authority: &dyn PermissionCancellationAuthority,
    catalog: &PermissionCatalog,
    ctx: &ApprovalContext,
    target: &RunCancellationTarget,
) -> Result<StoredExecution, PermissionError> {
    failure.cancel_withheld(
        authority,
        catalog,
        &PermissionOriginalAlwaysWithheldCancellationRequest {
            context: ctx,
            target,
            facts: facts(111),
        },
    )
}
fn check_dispatch(fixture: &Fixture, before: &Capture, hooks: &Admission) -> Capture {
    assert_eq!(fixture.ready.snapshot.revision, 3);
    assert_eq!(fixture.ready.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(
        before.journals[fixture.ready.snapshot.execution_id.as_str()].len(),
        4
    );
    let dispatch = expected(
        &fixture.ready,
        KernelCommand::Dispatch {
            task_id: TaskId::new(TASK).unwrap(),
            attempt_id: hooks.attempt.clone(),
        },
        108,
    );
    let actual = fixture.capture();
    assert_commit(before, &actual, &dispatch, true);
    assert_eq!(dispatch.snapshot.revision, 4);
    assert_eq!(dispatch.snapshot.status, ExecutionStatus::Running);
    assert_eq!(dispatch.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(hooks.entries.load(Ordering::SeqCst), 1);
    assert!(actual.raw[8].is_empty(), "original Dispatch has no Use");
    actual
}
fn check_uninvoked(
    fixture: &Fixture,
    failure: &PermissionOriginalAlwaysDriverFailure<'_, SqliteStore, Authority>,
    hooks: &Admission,
) {
    assert!(!hooks.allow, "actual entry hook explicitly returned false");
    assert!(!hooks.panic);
    assert_eq!(hooks.entries.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.callback.calls.load(Ordering::SeqCst), 0);
    assert!(fixture.callback.entry.lock().unwrap().is_none());
    assert_eq!(fixture.callback.direct_calls.load(Ordering::SeqCst), 0);
    assert_eq!(failure.callback_status(), None);
    assert!(failure.settled_outcome().is_none());
    assert_eq!(failure.usage().completion_entries(), 0);
    assert_eq!(failure.usage().candidate_entries(), 0);
    assert_eq!(failure.usage().committed_completions(), 0);
}
fn healthy_withdrawal(
    fixture: &Fixture,
    failure: &mut PermissionOriginalAlwaysDriverFailure<'_, SqliteStore, Authority>,
    hooks: &Admission,
    before: &Capture,
    target: &RunCancellationTarget,
) -> Capture {
    let dispatch = before.view.execution.as_ref().unwrap();
    // Both plans are independent kernel transitions from the actual Dispatch4.
    let requested = expected(
        dispatch,
        KernelCommand::Cancel {
            turn_id: dispatch.snapshot.turn_id.clone(),
            owner_generation: fixture.store.owner_generation(),
        },
        111,
    );
    let terminal = expected(
        &requested,
        KernelCommand::CancelAttempt {
            task_id: TaskId::new(TASK).unwrap(),
            attempt_id: hooks.attempt.clone(),
        },
        111,
    );
    assert_eq!(requested.snapshot.revision, 5);
    assert_eq!(requested.snapshot.status, ExecutionStatus::CancelRequested);
    assert_eq!(terminal.snapshot.revision, 6);
    assert_eq!(terminal.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(
        withdraw(
            failure,
            fixture.authority.as_ref(),
            &fixture.catalog,
            &context(),
            target
        )
        .expect("confirmed atomic withdrawal of the same actual retained Work"),
        terminal
    );
    let after = fixture.capture();
    let intermediate = project_commit(before, &requested, false);
    assert_commit(&intermediate, &after, &terminal, false);
    assert_eq!(
        after.view.thread.resource_revision,
        before.view.thread.resource_revision + 2
    );
    assert_eq!(after.raw[3].len(), before.raw[3].len() + 2);
    assert_eq!(
        after.journals[dispatch.snapshot.execution_id.as_str()].len(),
        7
    );
    assert_eq!(
        after.raw[3].len(),
        8,
        "thread creation plus exact seven-entry creator history"
    );
    assert!(
        after.raw[8].is_empty(),
        "original cancellation never creates Use authority"
    );
    assert_eq!(
        after.raw[4..],
        before.raw[4..],
        "all saved approval/permission authority is immutable"
    );
    check_uninvoked(fixture, failure, hooks);
    after
}

#[test]
fn actual_refused_original_work_withdraws_atomically_to_six_and_fresh_reuse_completes_after_reopen()
{
    let fixture = Fixture::new();
    let before = fixture.capture();
    let capacity = TaskCapacity::new(1).unwrap();
    let after = {
        let ctx = context();
        let hooks = Admission::refused();
        let signal = NativeCancellationSignal::new();
        let mut failure = drive_permission_original_always_cancellable_execution(
            fixture.bindings(&ctx, &capacity, &hooks),
            fixture.ready.clone(),
            &signal.observer(),
        )
        .expect_err("actual drained Driver refuses local entry");
        let dispatch = check_dispatch(&fixture, &before, &hooks);
        check_uninvoked(&fixture, &failure, &hooks);
        assert!(matches!(
            failure.retry_settlement(&facts(110)),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert_eq!(fixture.capture(), dispatch);
        let target = RunCancellationTarget::from_view(&dispatch.view).unwrap();
        let after = healthy_withdrawal(&fixture, &mut failure, &hooks, &dispatch, &target);
        assert!(withdraw(
            &mut failure,
            fixture.authority.as_ref(),
            &fixture.catalog,
            &ctx,
            &target
        )
        .is_err());
        assert_eq!(
            fixture.capture(),
            after,
            "confirmed retirement cannot append a second cancellation"
        );
        check_uninvoked(&fixture, &failure, &hooks);
        assert!(matches!(
            failure.retry_settlement(&facts(112)),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert_eq!(fixture.capture(), after);
        drop(failure);
        after
    };
    fixture.retire(&after, 0, capacity, true);
}

#[test]
fn retained_work_survives_management_refusals_and_constraint_revocation_without_gate_reentry() {
    let fixture = Fixture::new();
    let before = fixture.capture();
    let capacity = TaskCapacity::new(1).unwrap();
    let after = {
        let ctx = context();
        let hooks = Admission::refused();
        let signal = NativeCancellationSignal::new();
        let mut failure = drive_permission_original_always_cancellable_execution(
            fixture.bindings(&ctx, &capacity, &hooks),
            fixture.ready.clone(),
            &signal.observer(),
        )
        .unwrap_err();
        let dispatch = check_dispatch(&fixture, &before, &hooks);
        let target = RunCancellationTarget::from_view(&dispatch.view).unwrap();
        let mut denied = ctx.clone();
        denied.actor.principal = "another-owner".into();
        assert!(withdraw(
            &mut failure,
            fixture.authority.as_ref(),
            &fixture.catalog,
            &denied,
            &target
        )
        .is_err());
        assert_eq!(fixture.capture(), dispatch);
        check_uninvoked(&fixture, &failure, &hooks);

        let mut changed = fixture.authority.definition.clone();
        changed.revision = ExecutionRevision::new("definition-2").unwrap();
        let other_calls = Arc::new(AtomicUsize::new(0));
        let mismatched = catalog(&changed, Arc::new(Direct(other_calls.clone())));
        assert!(withdraw(
            &mut failure,
            fixture.authority.as_ref(),
            &mismatched,
            &ctx,
            &target
        )
        .is_err());
        assert_eq!(other_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.capture(), dispatch);
        check_uninvoked(&fixture, &failure, &hooks);
        drop(mismatched);

        let mut stale = target.clone();
        stale.resource_revision -= 1;
        assert!(withdraw(
            &mut failure,
            fixture.authority.as_ref(),
            &fixture.catalog,
            &ctx,
            &stale
        )
        .is_err());
        assert_eq!(fixture.capture(), dispatch);
        check_uninvoked(&fixture, &failure, &hooks);

        let gate_calls = fixture.constraint.calls.load(Ordering::SeqCst);
        assert!(
            gate_calls > 0,
            "actual driver checked the initially allowed effect gate"
        );
        fixture.constraint.revoked.store(true, Ordering::SeqCst);
        let invocation_context = InvocationContext {
            thread_id: fixture.thread.clone(),
            thread_generation: dispatch.view.thread.generation.clone(),
            origin: ctx.actor.clone(),
            application_revision: "application-1".into(),
            execution_revision: fixture.ready.snapshot.definition.revision.clone(),
            policy_revision: ctx.policy_revision.clone(),
        };
        // The genuine preclaim Ready3 task is a denial probe, never a new claim.
        assert!(matches!(
            fixture.catalog.validate_call(
                &invocation_context,
                &fixture.ready.snapshot.definition,
                &fixture.ready.snapshot.tasks[0],
                PermissionCallPhase::Unclaimed,
            ),
            Err(PermissionError::ConstraintDenied)
        ));
        assert_eq!(
            fixture.constraint.calls.load(Ordering::SeqCst),
            gate_calls + 1
        );
        assert_eq!(fixture.capture(), dispatch);
        let after = healthy_withdrawal(&fixture, &mut failure, &hooks, &dispatch, &target);
        assert_eq!(
            fixture.constraint.calls.load(Ordering::SeqCst),
            gate_calls + 1,
            "management withdrawal does not rerun effect policy"
        );
        drop(failure);
        after
    };
    fixture.retire(&after, 0, capacity, true);
}

#[test]
fn exact_terminal_journal_fault_rolls_back_nine_tables_and_same_work_explicit_retry_succeeds() {
    let fixture = Fixture::new();
    let before = fixture.capture();
    let capacity = TaskCapacity::new(1).unwrap();
    let after = {
        let ctx = context();
        let hooks = Admission::refused();
        let signal = NativeCancellationSignal::new();
        let mut failure = drive_permission_original_always_cancellable_execution(
            fixture.bindings(&ctx, &capacity, &hooks),
            fixture.ready.clone(),
            &signal.observer(),
        )
        .unwrap_err();
        let dispatch = check_dispatch(&fixture, &before, &hooks);
        let target = RunCancellationTarget::from_view(&dispatch.view).unwrap();
        let next_resource = dispatch.view.thread.resource_revision + 2;
        let ddl = format!(
            "CREATE TRIGGER withheld_cancel_journal_guard BEFORE INSERT ON journal
             WHEN NEW.kind = 'commit'
              AND NEW.execution_id = '{execution}' AND NEW.thread_id = '{thread}' AND NEW.revision = x'0000000000000006'
              AND EXISTS (SELECT 1 FROM executions WHERE id = '{execution}' AND revision = x'0000000000000006')
              AND EXISTS (SELECT 1 FROM threads WHERE id = '{thread}' AND active_execution_id = '{execution}' AND resource_revision = x'{next_resource:016x}')
             BEGIN SELECT RAISE(ABORT, 'withheld_cancel_post_terminal_journal_fault'); END",
            execution = fixture.ready.snapshot.execution_id, thread = fixture.thread,
        );
        connection(&fixture.directory.0)
            .execute_batch(&ddl)
            .unwrap();
        let count: i64 = connection(&fixture.directory.0).query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='withheld_cancel_journal_guard'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 1);
        let actual_ddl: String = connection(&fixture.directory.0).query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='withheld_cancel_journal_guard'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(
            actual_ddl, ddl,
            "literal exact post-terminal guard installed"
        );
        let error = withdraw(
            &mut failure,
            fixture.authority.as_ref(),
            &fixture.catalog,
            &ctx,
            &target,
        )
        .unwrap_err();
        assert!(
            matches!(&error, PermissionError::Store(_)),
            "actual late storage abort: {error:?}"
        );
        // The exact guard, its presence, complete rollback and subsequent
        // same-owner healthy control, together with reviewed writer ordering,
        // identify this bounded late fault. A generic Store error alone cannot.
        // SQLite deliberately omits dynamic trigger messages from public errors.
        assert_eq!(
            fixture.capture(),
            dispatch,
            "full Running4 rollback across all nine tables"
        );
        check_uninvoked(&fixture, &failure, &hooks);
        connection(&fixture.directory.0)
            .execute_batch("DROP TRIGGER withheld_cancel_journal_guard;")
            .unwrap();
        let count: i64 = connection(&fixture.directory.0).query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='withheld_cancel_journal_guard'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 0);
        let after = healthy_withdrawal(&fixture, &mut failure, &hooks, &dispatch, &target);
        drop(failure);
        after
    };
    fixture.retire(&after, 0, capacity, true);
}

#[test]
fn actual_entered_uncertainty_and_hook_panic_cannot_claim_withheld_authority() {
    // Distinct genuine owners exercise actual receipt and GatePending phases.
    for hook_panic in [false, true] {
        let fixture = Fixture::new();
        let before = fixture.capture();
        let capacity = TaskCapacity::new(1).unwrap();
        let after = {
            let ctx = context();
            let mut hooks = Admission::refused();
            hooks.allow = true;
            hooks.panic = hook_panic;
            let signal = NativeCancellationSignal::new();
            let mut failure = drive_permission_original_always_cancellable_execution(
                fixture.bindings(&ctx, &capacity, &hooks),
                fixture.ready.clone(),
                &signal.observer(),
            )
            .expect_err("actual callback uncertainty or admission panic");
            let dispatch = check_dispatch(&fixture, &before, &hooks);
            let status = if hook_panic {
                None
            } else {
                Some(PermissionUseCallbackStatus::Uncertain)
            };
            assert_eq!(failure.callback_status(), status);
            assert!(failure.settled_outcome().is_none());
            assert_eq!(
                fixture.callback.calls.load(Ordering::SeqCst),
                usize::from(!hook_panic)
            );
            assert_eq!(fixture.callback.direct_calls.load(Ordering::SeqCst), 0);
            if hook_panic {
                assert!(fixture.callback.entry.lock().unwrap().is_none());
            } else {
                check_entry(
                    fixture.callback.entry.lock().unwrap().as_ref().unwrap(),
                    &dispatch,
                    4,
                );
            }
            let target = RunCancellationTarget::from_view(&dispatch.view).unwrap();
            assert!(withdraw(
                &mut failure,
                fixture.authority.as_ref(),
                &fixture.catalog,
                &ctx,
                &target
            )
            .is_err());
            assert_eq!(
                fixture.capture(),
                dispatch,
                "ineligible ownership adds no write"
            );
            assert_eq!(failure.callback_status(), status);
            assert_eq!(hooks.entries.load(Ordering::SeqCst), 1);
            assert_eq!(
                fixture.callback.calls.load(Ordering::SeqCst),
                usize::from(!hook_panic)
            );
            drop(failure);
            dispatch
        };
        fixture.retire(&after, usize::from(!hook_panic), capacity, false);
    }
}
