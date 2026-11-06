//! Expectations for cancellation after a genuine NewUse controlled callback enters.
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
            "b4-permission-use-cancellation-{}-{}",
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
fn assert_commit(before: &Capture, after: &Capture, expected: &StoredExecution, canonical: bool) {
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
    assert_eq!(
        &projected, after,
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
#[derive(Clone, Copy)]
enum Outcome {
    Ack,
    Success,
    Failed,
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
    outcome: Outcome,
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
        Ok(match self.outcome {
            Outcome::Ack => ControlledNativeOutcome::Cancelled,
            Outcome::Success => {
                ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(result()))
            }
            Outcome::Failed => ControlledNativeOutcome::Settled(NativeOutcome::Failed(
                "publisher rejected report".into(),
            )),
        })
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
    fn new(outcome: Outcome) -> Self {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        assert!(store.as_ref().permission_use_cancellation_atomic_durable());
        assert!(store.permission_use_cancellation_atomic_durable());
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
        let target =
            RunCancellationTarget::from_view(&store.thread_view(&thread).unwrap().unwrap())
                .unwrap();
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
            outcome,
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
    fn terminal(&self, current: &StoredExecution, outcome: Outcome, now: u64) -> StoredExecution {
        let task_id = TaskId::new(TASK).unwrap();
        let attempt_id = self.hooks.attempt.clone();
        expected(
            current,
            match outcome {
                Outcome::Ack => KernelCommand::CancelAttempt {
                    task_id,
                    attempt_id,
                },
                Outcome::Success => KernelCommand::Complete {
                    task_id,
                    attempt_id,
                    result: result(),
                },
                Outcome::Failed => KernelCommand::FailAttempt {
                    task_id,
                    attempt_id,
                    failure: "publisher rejected report".into(),
                },
            },
            now,
        )
    }
    fn retire(self, after: &Capture, fresh_reuse: bool) {
        assert_eq!(
            self.store
                .execution(&self.original.snapshot.execution_id)
                .unwrap()
                .unwrap(),
            self.original
        );
        assert_eq!(self.callback.calls.load(Ordering::SeqCst), 1);
        assert_eq!(self.callback.direct_calls.load(Ordering::SeqCst), 0);
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
        if fresh_reuse {
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
            assert_eq!(settled.snapshot.revision, 4);
            assert_eq!(settled.snapshot.status, ExecutionStatus::Completed);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let later = capture(&reopened, &directory.0, &thread);
            assert_eq!(later.raw[3].len(), 17);
            assert_eq!(later.raw[4..8], after.raw[4..8]);
            for (id, execution) in &after.executions {
                assert_eq!(later.executions.get(id), Some(execution));
                assert_eq!(later.journals.get(id), after.journals.get(id));
            }
        }
        reopened.readiness().unwrap();
        drop(reopened);
        let path = directory.0.clone();
        drop(directory);
        assert!(!path.exists());
    }
}

fn guard(path: &Path, execution: &ExecutionId, revision: u64) {
    // SQLite trigger DDL cannot use parameters. IDs are validated local UUIDs;
    // the exact revision guard makes a later healthy same-target attempt diagnostic.
    connection(path).execute_batch(&format!("CREATE TRIGGER permission_use_cancel_guard BEFORE INSERT ON journal WHEN NEW.kind = 'commit' AND NEW.execution_id = '{}' AND NEW.revision = x'{revision:016x}' BEGIN SELECT RAISE(ABORT, 'permission_use_cancel_journal_fault'); END;", execution)).unwrap();
    let count: i64 = connection(path).query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='permission_use_cancel_guard'", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);
}
fn remove_guard(path: &Path) {
    connection(path)
        .execute_batch("DROP TRIGGER permission_use_cancel_guard;")
        .unwrap();
}

fn settled_case(
    outcome: Outcome,
    cancel: bool,
    refusal_controls: bool,
    request_guard: bool,
    terminal_guard: bool,
) {
    let fixture = Fixture::new(outcome);
    let before = fixture.capture();
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    let ctx = context();
    let capacity = TaskCapacity::new(1).unwrap();
    let mut requested = None;
    let mut actual = std::thread::scope(|scope| {
        let bindings = fixture.bindings(&ctx, &capacity);
        let ready = fixture.ready.clone();
        let observer_ref = &observer;
        let run = scope.spawn(move || {
            drive_permission_use_cancellable_execution(bindings, ready, observer_ref)
        });
        let entered = fixture
            .entered
            .recv_timeout(WAIT)
            .expect("actual controlled callback entry");
        fixture.check_entry(&before, &entered);
        let target = RunCancellationTarget::from_view(&entered.state.view).unwrap();
        if refusal_controls {
            let mut denied = ctx.clone();
            denied.actor.principal = "another-owner".into();
            assert!(fixture.request(&denied, &target).is_err());
            assert_eq!(fixture.capture(), entered.state);
            let stale = RunCancellationTarget::from_view(&before.view).unwrap();
            assert!(fixture.request(&ctx, &stale).is_err());
            assert_eq!(fixture.capture(), entered.state);
        }
        if request_guard {
            guard(
                &fixture.directory.0,
                &fixture.ready.snapshot.execution_id,
                4,
            );
            assert!(fixture.request(&ctx, &target).is_err());
            assert_eq!(
                fixture.capture(),
                entered.state,
                "late request journal abort rolls back every table"
            );
            remove_guard(&fixture.directory.0);
        }
        if cancel {
            let stored = fixture
                .request(&ctx, &target)
                .expect("actual durable management result");
            let expected = expected(
                entered.state.view.execution.as_ref().unwrap(),
                KernelCommand::Cancel {
                    turn_id: fixture.ready.snapshot.turn_id.clone(),
                    owner_generation: fixture.store.owner_generation(),
                },
                109,
            );
            assert_eq!(stored, expected);
            assert_eq!(stored.snapshot.revision, 4);
            assert_eq!(stored.snapshot.status, ExecutionStatus::CancelRequested);
            let after_request = fixture.capture();
            assert_commit(&entered.state, &after_request, &expected, false);
            if terminal_guard {
                guard(
                    &fixture.directory.0,
                    &fixture.ready.snapshot.execution_id,
                    5,
                );
            }
            requested = Some(after_request);
            signal.commit_cancellation();
        } else {
            requested = Some(entered.state);
        }
        fixture.release.send(()).unwrap();
        run.join().expect("owned finite Driver thread")
    });
    let current = requested.unwrap();
    let expected = fixture.terminal(
        current.view.execution.as_ref().unwrap(),
        outcome,
        if terminal_guard { 111 } else { 110 },
    );
    if terminal_guard {
        let rollback = fixture.capture();
        assert_eq!(rollback, current);
        let failure = actual
            .as_mut()
            .expect_err("actual terminal write fails under exact guard");
        assert_eq!(
            failure.callback_status(),
            Some(PermissionUseCallbackStatus::Settled)
        );
        let owned = match outcome {
            Outcome::Ack => EffectOutcome::Cancelled,
            Outcome::Success => EffectOutcome::Succeeded(result()),
            Outcome::Failed => EffectOutcome::Failed("publisher rejected report".into()),
        };
        assert_eq!(failure.settled_outcome(), Some(&owned));
        assert_eq!(failure.usage().committed_completions(), 0);
        remove_guard(&fixture.directory.0);
        assert_eq!(failure.retry_settlement(&facts(111)).unwrap(), expected);
        assert!(failure.retry_settlement(&facts(112)).is_err());
        assert!(failure.callback_status().is_none());
        assert!(failure.settled_outcome().is_none());
    } else {
        assert_eq!(actual.as_ref().unwrap(), &expected);
    }
    let after = fixture.capture();
    assert_commit(&current, &after, &expected, false);
    assert_eq!(expected.snapshot.revision, if cancel { 5 } else { 4 });
    if matches!(outcome, Outcome::Success) && cancel {
        assert_eq!(expected.snapshot.status, ExecutionStatus::Cancelled);
        assert_eq!(expected.snapshot.tasks[0].status, TaskStatus::Succeeded);
    }
    if matches!(outcome, Outcome::Failed) {
        assert_eq!(expected.snapshot.status, ExecutionStatus::Failed);
    }
    drop(actual);
    drop(observer);
    drop(signal);
    fixture.retire(&after, cancel);
}
#[test]
fn durable_request_then_real_ack_cancels_and_preserves_permission_after_reopen() {
    settled_case(Outcome::Ack, true, false, false, false);
}
#[test]
fn actual_success_after_request_preserves_kernel_result_and_known_failure_semantics() {
    settled_case(Outcome::Success, true, false, false, false);
    settled_case(Outcome::Failed, true, false, false, false);
}
#[test]
fn controlled_success_without_request_uses_complete_four() {
    settled_case(Outcome::Success, false, false, false, false);
}
#[test]
fn current_denial_and_stale_target_leave_live_dispatch_unchanged() {
    settled_case(Outcome::Ack, true, true, false, false);
}
#[test]
fn exact_request_journal_abort_rolls_back_before_same_target_healthy_request() {
    settled_case(Outcome::Ack, true, false, true, false);
}
#[test]
fn terminal_journal_abort_retains_real_ack_for_storage_only_retry_once() {
    settled_case(Outcome::Ack, true, false, false, true);
}
#[test]
fn signal_only_ack_cannot_create_stored_cancellation_authority() {
    let fixture = Fixture::new(Outcome::Ack);
    let before = fixture.capture();
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    let ctx = context();
    let capacity = TaskCapacity::new(1).unwrap();
    let mut entered_state = None;
    let mut actual = std::thread::scope(|scope| {
        let bindings = fixture.bindings(&ctx, &capacity);
        let ready = fixture.ready.clone();
        let observer_ref = &observer;
        let run = scope.spawn(move || {
            drive_permission_use_cancellable_execution(bindings, ready, observer_ref)
        });
        let entered = fixture
            .entered
            .recv_timeout(WAIT)
            .expect("actual callback entry");
        fixture.check_entry(&before, &entered);
        entered_state = Some(entered.state);
        signal.commit_cancellation();
        fixture.release.send(()).unwrap();
        run.join().expect("owned finite Driver thread")
    });
    let failure = actual
        .as_mut()
        .expect_err("local intent does not authorize a terminal snapshot");
    assert!(
        failure.callback_status().is_some(),
        "actual receipt remains observable until retirement"
    );
    assert!(failure.retry_settlement(&facts(111)).is_err());
    let after = fixture.capture();
    assert_eq!(after, entered_state.unwrap());
    assert_eq!(after.view.execution.as_ref().unwrap().snapshot.revision, 3);
    assert_eq!(
        after.view.execution.as_ref().unwrap().snapshot.status,
        ExecutionStatus::Running
    );
    drop(actual);
    drop(observer);
    drop(signal);
    fixture.retire(&after, false);
}

#[test]
fn revoked_effect_gate_after_entry_does_not_veto_authorized_management_cancellation() {
    let fixture = Fixture::new(Outcome::Ack);
    let before = fixture.capture();
    let gate_calls_before_driver = fixture.constraint.calls.load(Ordering::SeqCst);
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    let ctx = context();
    let capacity = TaskCapacity::new(1).unwrap();
    let mut requested = None;
    let actual = std::thread::scope(|scope| {
        let bindings = fixture.bindings(&ctx, &capacity);
        let ready = fixture.ready.clone();
        let observer_ref = &observer;
        let run = scope.spawn(move || {
            drive_permission_use_cancellable_execution(bindings, ready, observer_ref)
        });
        let entered = fixture
            .entered
            .recv_timeout(WAIT)
            .expect("actual controlled callback entered before constraint revocation");
        fixture.check_entry(&before, &entered);
        assert!(!fixture.constraint.revoked.load(Ordering::SeqCst));
        let gate_calls_at_entry = fixture.constraint.calls.load(Ordering::SeqCst);
        assert!(
            gate_calls_at_entry > gate_calls_before_driver,
            "actual driver evaluated the allowed effect gate before callback entry"
        );

        // Keep the exact catalog/Definition/registration and immutable permission
        // source. Only effect execution policy changes after actual native entry.
        fixture.constraint.revoked.store(true, Ordering::SeqCst);
        let current = entered.state.view.execution.as_ref().unwrap();
        assert!(matches!(
            fixture.catalog.validate_call(
                &entered.context,
                &current.snapshot.definition,
                &current.snapshot.tasks[0],
                PermissionCallPhase::Claimed(&entered.invocation),
            ),
            Err(PermissionError::ConstraintDenied)
        ));
        let gate_calls_after_probe = fixture.constraint.calls.load(Ordering::SeqCst);
        assert_eq!(gate_calls_after_probe, gate_calls_at_entry + 1);
        assert_eq!(fixture.capture(), entered.state);
        assert_eq!(fixture.callback.calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.callback.direct_calls.load(Ordering::SeqCst), 0);

        let target = RunCancellationTarget::from_view(&entered.state.view).unwrap();
        let stored = fixture
            .request(&ctx, &target)
            .expect("dedicated management authority permits stopping the entered effect");
        let expected = expected(
            current,
            KernelCommand::Cancel {
                turn_id: fixture.ready.snapshot.turn_id.clone(),
                owner_generation: fixture.store.owner_generation(),
            },
            109,
        );
        assert_eq!(stored, expected);
        assert_eq!(stored.snapshot.revision, 4);
        assert_eq!(stored.snapshot.status, ExecutionStatus::CancelRequested);
        assert_eq!(
            fixture.constraint.calls.load(Ordering::SeqCst),
            gate_calls_after_probe,
            "management cancellation must not rerun the effect execution gate"
        );
        let after_request = fixture.capture();
        assert_commit(&entered.state, &after_request, &expected, false);
        requested = Some(after_request);
        signal.commit_cancellation();
        fixture.release.send(()).unwrap();
        run.join().expect("owned finite Driver thread")
    });
    let current = requested.unwrap();
    let expected = fixture.terminal(current.view.execution.as_ref().unwrap(), Outcome::Ack, 110);
    assert_eq!(actual.as_ref().unwrap(), &expected);
    assert_eq!(expected.snapshot.revision, 5);
    assert_eq!(expected.snapshot.status, ExecutionStatus::Cancelled);
    assert!(fixture.constraint.revoked.load(Ordering::SeqCst));
    let after = fixture.capture();
    assert_commit(&current, &after, &expected, false);
    drop(actual);
    drop(observer);
    drop(signal);
    fixture.retire(&after, true);
}
