//! Expectations for cancellation of a genuine NewUse while still Ready2.
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
        mpsc::{self, Receiver, SyncSender},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

const WAIT: Duration = Duration::from_secs(10);
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
            "b4-permission-use-ready-cancellation-{}-{}",
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

// All observations are capped with a sentinel. Three genuine executions need
// seventeen journal rows, including the thread creation record.
fn raw(path: &Path) -> Vec<Vec<Vec<Cell>>> {
    let db = connection(path);
    [
        ("metadata", "id", 1),
        ("threads", "id", 1),
        ("executions", "id", 3),
        ("journal", "sequence", 17),
        ("approval_grants", "execution_id, interrupt_id", 1),
        ("approval_acceptances", "execution_id, turn_id", 1),
        ("permission_acceptances", "execution_id, turn_id", 1),
        ("permission_decisions", "permission_id", 1),
        ("permission_uses", "execution_id, task_id", 2),
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
struct Entered {
    state: Capture,
    invocation: EffectInvocation,
    context: InvocationContext,
}
struct Controlled {
    store: Weak<SqliteStore>,
    path: PathBuf,
    thread: ThreadId,
    entered: SyncSender<Entered>,
    release: Mutex<Receiver<()>>,
    calls: AtomicUsize,
    direct_calls: AtomicUsize,
}
impl NativeEffect for Controlled {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.direct_calls.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(result()))
    }
    fn invoke_controlled(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
        _: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        // Only the receiver wait holds this mutex; no application/store call does.
        let store = self.store.upgrade().ok_or(EffectUncertain)?;
        let state = capture(&store, &self.path, &self.thread);
        drop(store);
        self.entered
            .send(Entered {
                state,
                invocation: invocation.clone(),
                context: context.clone(),
            })
            .map_err(|_| EffectUncertain)?;
        self.release
            .lock()
            .map_err(|_| EffectUncertain)?
            .recv_timeout(WAIT)
            .map_err(|_| EffectUncertain)?;
        Ok(ControlledNativeOutcome::Cancelled)
    }
}
struct Fixture {
    directory: Directory,
    store: Arc<SqliteStore>,
    authority: Arc<Authority>,
    catalog: PermissionCatalog,
    callback: Arc<Controlled>,
    constraint: Arc<Constraint>,
    entered: Receiver<Entered>,
    release: SyncSender<()>,
    thread: ThreadId,
    original: StoredExecution,
    ready: StoredExecution,
    hooks: Hooks,
}
impl Fixture {
    fn new() -> Self {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert!(store
            .as_ref()
            .permission_use_ready_cancellation_atomic_durable());
        assert!(store.permission_use_ready_cancellation_atomic_durable());
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
        let original_ready = store.thread_view(&thread).unwrap().unwrap();
        assert_eq!(
            original_ready.execution.as_ref().unwrap().snapshot.revision,
            3
        );
        assert_eq!(
            original_ready.execution.as_ref().unwrap().snapshot.tasks[0].status,
            TaskStatus::Ready
        );
        let target = RunCancellationTarget::from_view(&original_ready).unwrap();
        let original = cancel_permission_ready_with_normal_inputs(
            store.as_ref(),
            authority.as_ref(),
            seed_catalog.as_ref(),
            &PermissionReadyCancellationRequest {
                context: &context(),
                target: &target,
                facts: facts(104),
            },
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(original.snapshot.revision, 4);
        assert_eq!(original.snapshot.status, ExecutionStatus::Cancelled);
        assert_eq!(seed_calls.load(Ordering::SeqCst), 0);
        drop(pending);
        drop(service);
        drop(seed_catalog);
        drop(seed_calls);
        let (entered_tx, entered) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::sync_channel(1);
        let callback = Arc::new(Controlled {
            store: Arc::downgrade(&store),
            path: directory.0.clone(),
            thread: thread.clone(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
            calls: AtomicUsize::new(0),
            direct_calls: AtomicUsize::new(0),
        });
        let constraint = Arc::new(Constraint::default());
        let catalog = catalog_with_constraint(&definition, callback.clone(), constraint.clone());
        let ready = ready_use(&store, &authority, &catalog, &thread, &definition, 105);
        Self {
            directory,
            store,
            authority,
            catalog,
            callback,
            constraint,
            entered,
            release,
            thread,
            original,
            ready,
            hooks: Hooks {
                attempt: AttemptId::new(id()).unwrap(),
                dispatch_ms: 108,
                completion_ms: 110,
            },
        }
    }
    fn capture(&self) -> Capture {
        capture(&self.store, &self.directory.0, &self.thread)
    }
    fn bindings<'a>(
        &'a self,
        ctx: &'a ApprovalContext,
        capacity: &'a TaskCapacity,
    ) -> PermissionUseDriverBindings<'a, SqliteStore, Authority, Hooks> {
        PermissionUseDriverBindings {
            store: self.store.as_ref(),
            authority: self.authority.as_ref(),
            producer_catalog: &self.catalog,
            current_catalog: &self.catalog,
            context: ctx,
            hooks: &self.hooks,
            capacity,
        }
    }
    fn request(
        &self,
        ctx: &ApprovalContext,
        target: &RunCancellationTarget,
    ) -> Result<StoredExecution, PermissionError> {
        request_permission_use_cancellation(
            self.store.as_ref(),
            self.authority.as_ref(),
            &self.catalog,
            &PermissionUseCancellationRequest {
                context: ctx,
                target,
                facts: facts(109),
            },
            &mut PermissionEvidenceBudget::new(),
        )
    }
    fn check_entry(&self, before: &Capture, entered: &Entered) {
        let claimed = expected(
            &self.ready,
            KernelCommand::Dispatch {
                task_id: TaskId::new(TASK).unwrap(),
                attempt_id: self.hooks.attempt.clone(),
            },
            108,
        );
        assert_commit(before, &entered.state, &claimed, true);
        assert_eq!(
            entered.invocation,
            EffectInvocation {
                execution_id: claimed.snapshot.execution_id.clone(),
                turn_id: claimed.snapshot.turn_id.clone(),
                owner_generation: claimed.snapshot.owner_generation,
                task_id: TaskId::new(TASK).unwrap(),
                effect_id: EffectId::new(EFFECT).unwrap(),
                attempt_id: self.hooks.attempt.clone(),
                input: claimed.snapshot.tasks[0].input.clone()
            }
        );
        assert_eq!(
            entered.context,
            InvocationContext {
                thread_id: self.thread.clone(),
                thread_generation: before.view.thread.generation.clone(),
                origin: context().actor,
                application_revision: "application-1".into(),
                execution_revision: self.ready.snapshot.definition.revision.clone(),
                policy_revision: context().policy_revision
            }
        );
        assert_eq!(self.callback.calls.load(Ordering::SeqCst), 1);
        assert_eq!(self.callback.direct_calls.load(Ordering::SeqCst), 0);
    }
    fn terminal(&self, current: &StoredExecution, now: u64) -> StoredExecution {
        let task_id = TaskId::new(TASK).unwrap();
        let attempt_id = self.hooks.attempt.clone();
        expected(
            current,
            KernelCommand::CancelAttempt {
                task_id,
                attempt_id,
            },
            now,
        )
    }
    fn retire(self, after: &Capture, callback_calls: usize) {
        assert_eq!(
            self.store
                .execution(&self.original.snapshot.execution_id)
                .unwrap()
                .unwrap(),
            self.original
        );
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
            entered,
            release,
            thread,
            original: _,
            ready: _,
            hooks: _,
        } = self;
        let definition = authority.definition.clone();
        let owner = store.owner_generation();
        drop(authority);
        drop(retired_catalog);
        drop(callback);
        drop(constraint);
        drop(entered);
        drop(release);
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
        {
            let authority = Authority {
                store: Arc::downgrade(&reopened),
                definition: definition.clone(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let catalog = catalog(&definition, Arc::new(Direct(calls.clone())));
            let ready = ready_use(&reopened, &authority, &catalog, &thread, &definition, 120);
            let hooks = Hooks {
                attempt: AttemptId::new(id()).unwrap(),
                dispatch_ms: 123,
                completion_ms: 125,
            };
            let capacity = TaskCapacity::new(1).unwrap();
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
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let later = capture(&reopened, &directory.0, &thread);
            let dispatched = project_commit(&fresh_before, &dispatch, true);
            assert_commit(&dispatched, &later, &completion, false);
            assert_eq!(later.raw[3].len(), after.raw[3].len() + 5);
            assert_eq!(later.raw[8].len(), after.raw[8].len() + 1);
            for row in &after.raw[8] {
                assert!(later.raw[8].contains(row), "immutable previous Use");
            }
            assert_eq!(later.raw[4..8], after.raw[4..8]);
            for (id, execution) in &after.executions {
                assert_eq!(later.executions.get(id), Some(execution));
                assert_eq!(later.journals.get(id), after.journals.get(id));
            }
        }
        integrity(&directory.0);
        reopened.readiness().unwrap();
        drop(reopened);
        let path = directory.0.clone();
        drop(directory);
        assert!(!path.exists());
    }
}

fn cancel_ready(
    fixture: &Fixture,
    authority: &dyn PermissionCancellationAuthority,
    catalog: &PermissionCatalog,
    ctx: &ApprovalContext,
    target: &RunCancellationTarget,
) -> Result<StoredExecution, PermissionError> {
    cancel_permission_use_ready(
        fixture.store.as_ref(),
        authority,
        catalog,
        &PermissionUseReadyCancellationRequest {
            context: ctx,
            target,
            facts: facts(108),
        },
        &mut PermissionEvidenceBudget::new(),
    )
}
fn check_ready(fixture: &Fixture, state: &Capture) {
    assert_eq!(state.view.execution.as_ref(), Some(&fixture.ready));
    assert_eq!(fixture.ready.snapshot.revision, 2);
    assert_eq!(fixture.ready.snapshot.status, ExecutionStatus::Running);
    assert_eq!(fixture.ready.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(fixture.ready.snapshot.attempt_history.is_empty());
    assert_eq!(fixture.ready.snapshot.attempts_used, 0);
    assert!(fixture.ready.snapshot.tasks[0].attempt_id.is_none());
    assert_eq!(
        state.journals[fixture.ready.snapshot.execution_id.as_str()].len(),
        3
    );
    assert_eq!(state.raw[8].len(), 1, "actual immutable Use exists");
    assert_eq!(fixture.callback.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.callback.direct_calls.load(Ordering::SeqCst), 0);
}
fn healthy_cancel(fixture: &Fixture, before: &Capture, target: &RunCancellationTarget) -> Capture {
    let cancelled = cancel_ready(
        fixture,
        fixture.authority.as_ref(),
        &fixture.catalog,
        &context(),
        target,
    )
    .expect("confirmed genuine Ready cancellation");
    let expected = expected(
        &fixture.ready,
        KernelCommand::Cancel {
            turn_id: fixture.ready.snapshot.turn_id.clone(),
            owner_generation: fixture.store.owner_generation(),
        },
        108,
    );
    assert_eq!(cancelled, expected);
    assert_eq!(cancelled.snapshot.revision, 3);
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert!(cancelled.snapshot.attempt_history.is_empty());
    assert_eq!(cancelled.snapshot.attempts_used, 0);
    assert!(cancelled.snapshot.tasks[0].attempt_id.is_none());
    let after = fixture.capture();
    assert_commit(before, &after, &expected, false);
    assert_eq!(fixture.callback.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.callback.direct_calls.load(Ordering::SeqCst), 0);
    after
}

#[test]
fn ready_cancel_three_without_entry_preserves_authority_for_fresh_complete_four_after_reopen() {
    let fixture = Fixture::new();
    let before = fixture.capture();
    check_ready(&fixture, &before);
    let target = RunCancellationTarget::from_view(&before.view).unwrap();
    let after = healthy_cancel(&fixture, &before, &target);
    assert!(cancel_ready(
        &fixture,
        fixture.authority.as_ref(),
        &fixture.catalog,
        &context(),
        &target,
    )
    .is_err());
    assert_eq!(
        fixture.capture(),
        after,
        "repeat cannot append cancellation"
    );
    fixture.retire(&after, 0);
}

#[test]
fn actor_catalog_and_stale_target_refuse_without_mutation_before_healthy_control() {
    let fixture = Fixture::new();
    let before = fixture.capture();
    check_ready(&fixture, &before);
    let target = RunCancellationTarget::from_view(&before.view).unwrap();
    let mut denied = context();
    denied.actor.principal = "another-owner".into();
    assert!(cancel_ready(
        &fixture,
        fixture.authority.as_ref(),
        &fixture.catalog,
        &denied,
        &target,
    )
    .is_err());
    assert_eq!(fixture.capture(), before);

    let mut changed = fixture.authority.definition.clone();
    changed.revision = ExecutionRevision::new("definition-2").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mismatched = catalog(&changed, Arc::new(Direct(calls.clone())));
    assert!(cancel_ready(
        &fixture,
        fixture.authority.as_ref(),
        &mismatched,
        &context(),
        &target,
    )
    .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.capture(), before);
    drop(mismatched);

    let mut stale = target.clone();
    stale.resource_revision -= 1;
    assert!(cancel_ready(
        &fixture,
        fixture.authority.as_ref(),
        &fixture.catalog,
        &context(),
        &stale,
    )
    .is_err());
    assert_eq!(fixture.capture(), before);

    // Revocation changes only effect execution policy in the SAME catalog.
    // A real Unclaimed probe denies this actual Ready task without a claim.
    let gate_calls = fixture.constraint.calls.load(Ordering::SeqCst);
    fixture.constraint.revoked.store(true, Ordering::SeqCst);
    let invocation_context = InvocationContext {
        thread_id: fixture.thread.clone(),
        thread_generation: before.view.thread.generation.clone(),
        origin: context().actor,
        application_revision: "application-1".into(),
        execution_revision: fixture.ready.snapshot.definition.revision.clone(),
        policy_revision: context().policy_revision,
    };
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
    assert_eq!(fixture.capture(), before);
    check_ready(&fixture, &before);
    let after = healthy_cancel(&fixture, &before, &target);
    assert_eq!(
        fixture.constraint.calls.load(Ordering::SeqCst),
        gate_calls + 1,
        "dedicated management cancellation does not rerun effect execution policy"
    );
    fixture.retire(&after, 0);
}

#[test]
fn exact_post_update_journal_abort_restores_ready_two_before_same_target_healthy_cancel() {
    let fixture = Fixture::new();
    let before = fixture.capture();
    check_ready(&fixture, &before);
    let target = RunCancellationTarget::from_view(&before.view).unwrap();
    let next_resource = before.view.thread.resource_revision + 1;
    // The exact journal3 guard fires only after BOTH execution and thread CAS
    // updates are visible in this transaction. IDs are local validated UUIDs.
    let ddl = format!(
        "CREATE TRIGGER ready_cancel_journal_guard BEFORE INSERT ON journal
         WHEN NEW.kind = 'commit'
          AND NEW.execution_id = '{execution}' AND NEW.revision = x'0000000000000003'
          AND EXISTS (SELECT 1 FROM executions WHERE id = '{execution}' AND revision = x'0000000000000003')
          AND EXISTS (SELECT 1 FROM threads WHERE id = '{thread}' AND active_execution_id = '{execution}' AND resource_revision = x'{next_resource:016x}')
         BEGIN SELECT RAISE(ABORT, 'ready_cancel_post_update_journal_fault'); END;",
        execution = fixture.ready.snapshot.execution_id,
        thread = fixture.thread,
    );
    connection(&fixture.directory.0)
        .execute_batch(&ddl)
        .unwrap();
    let trigger_count: i64 = connection(&fixture.directory.0).query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='ready_cancel_journal_guard'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(trigger_count, 1);
    assert!(cancel_ready(
        &fixture,
        fixture.authority.as_ref(),
        &fixture.catalog,
        &context(),
        &target,
    )
    .is_err());
    assert_eq!(
        fixture.capture(),
        before,
        "late journal abort rolls back all nine tables"
    );
    check_ready(&fixture, &fixture.capture());
    connection(&fixture.directory.0)
        .execute_batch("DROP TRIGGER ready_cancel_journal_guard;")
        .unwrap();
    let after = healthy_cancel(&fixture, &before, &target);
    fixture.retire(&after, 0);
}

struct PausedManagement {
    base: Arc<Authority>,
    expected_ready: Capture,
    path: PathBuf,
    authorized: SyncSender<Capture>,
    resume: Mutex<Receiver<()>>,
}
impl PermissionCancellationAuthority for PausedManagement {
    fn authorize_cancel(
        &self,
        ctx: &ApprovalContext,
        resource: &ApprovalResource,
        target: &RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        self.base.authorize_cancel(ctx, resource, target)?;
        let store = self.base.store.upgrade().expect("owned contender store");
        let state = capture(&store, &self.path, &resource.thread.id);
        assert_eq!(
            state, self.expected_ready,
            "management decision sees coherent genuine Ready2"
        );
        assert_eq!(resource.thread, state.view.thread);
        assert_eq!(Some(&resource.execution), state.view.execution.as_ref());
        drop(store);
        self.authorized
            .send(state)
            .expect("owned authorization receiver");
        // No Store guard is held while paused. A real driver must be able to
        // capture its own source and commit Dispatch before this resumes.
        self.resume
            .lock()
            .unwrap()
            .recv_timeout(WAIT)
            .expect("finite owner resumes cancellation after actual callback entry");
        Ok(())
    }
}

#[test]
fn actual_dispatch_after_management_authorization_wins_before_immediate_reread() {
    let fixture = Fixture::new();
    let before = fixture.capture();
    check_ready(&fixture, &before);
    let target = RunCancellationTarget::from_view(&before.view).unwrap();
    let (authorized_tx, authorized_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let management = PausedManagement {
        base: fixture.authority.clone(),
        expected_ready: before.clone(),
        path: fixture.directory.0.clone(),
        authorized: authorized_tx,
        resume: Mutex::new(resume_rx),
    };
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    let capacity = TaskCapacity::new(1).unwrap();
    let ctx = context();
    let (actual, after_request) = std::thread::scope(|scope| {
        // Capture only the thread-safe bindings, not Fixture's owned receivers.
        let store_ref = fixture.store.as_ref();
        let catalog_ref = &fixture.catalog;
        let management_ref = &management;
        let target_ref = &target;
        let ctx_ref = &ctx;
        let cancellation = scope.spawn(move || {
            cancel_permission_use_ready(
                store_ref,
                management_ref,
                catalog_ref,
                &PermissionUseReadyCancellationRequest {
                    context: ctx_ref,
                    target: target_ref,
                    facts: facts(108),
                },
                &mut PermissionEvidenceBudget::new(),
            )
        });
        assert_eq!(
            authorized_rx
                .recv_timeout(WAIT)
                .expect("source captured and management authorized"),
            before
        );
        let bindings = fixture.bindings(&ctx, &capacity);
        let ready = fixture.ready.clone();
        let observer_ref = &observer;
        let driver = scope.spawn(move || {
            drive_permission_use_cancellable_execution(bindings, ready, observer_ref)
        });
        let entered = fixture
            .entered
            .recv_timeout(WAIT)
            .expect("genuine competing callback entered");
        fixture.check_entry(&before, &entered);
        resume_tx.send(()).unwrap();
        assert!(cancellation
            .join()
            .expect("owned Ready cancellation thread")
            .is_err());
        assert_eq!(
            fixture.capture(),
            entered.state,
            "Immediate reread refusal adds no cancellation mutation"
        );

        // Retire the actual entered callback through the separately qualified
        // entered management path, with the unpaused base authority.
        let entered_target = RunCancellationTarget::from_view(&entered.state.view).unwrap();
        let requested = fixture.request(&ctx, &entered_target).unwrap();
        let expected_request = expected(
            entered.state.view.execution.as_ref().unwrap(),
            KernelCommand::Cancel {
                turn_id: fixture.ready.snapshot.turn_id.clone(),
                owner_generation: fixture.store.owner_generation(),
            },
            109,
        );
        assert_eq!(requested, expected_request);
        assert_eq!(requested.snapshot.revision, 4);
        assert_eq!(requested.snapshot.status, ExecutionStatus::CancelRequested);
        let after_request = fixture.capture();
        assert_commit(&entered.state, &after_request, &expected_request, false);
        signal.commit_cancellation();
        fixture.release.send(()).unwrap();
        (
            driver.join().expect("owned finite contender driver"),
            after_request,
        )
    });
    let expected_terminal = fixture.terminal(after_request.view.execution.as_ref().unwrap(), 110);
    assert_eq!(actual.as_ref().unwrap(), &expected_terminal);
    assert_eq!(expected_terminal.snapshot.revision, 5);
    assert_eq!(
        expected_terminal.snapshot.status,
        ExecutionStatus::Cancelled
    );
    let after = fixture.capture();
    assert_commit(&after_request, &after, &expected_terminal, false);
    drop(actual);
    drop(management);
    drop(observer);
    drop(signal);
    fixture.retire(&after, 1);
}
