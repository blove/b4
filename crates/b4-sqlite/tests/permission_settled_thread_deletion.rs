//! Real SQLite business expectations for explicit deletion of settled permission history.
//! Positive history is produced only by approval/permission services and actual Drivers.
use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::{types::Value as Cell, Connection};
use serde_json::{json, Map};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};

const APP: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";
const TASK: &str = "publish-monthly-report";
const EFFECT: &str = "reports.publish-monthly";
static NEXT: AtomicU64 = AtomicU64::new(1);
fn id() -> String {
    format!(
        "b4de1100-0000-4000-8000-{:012x}",
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
            tenant: "reporting".into(),
            principal: "monthly-editor".into(),
        },
        policy_revision: "report-policy-2026-10".into(),
    }
}
fn result() -> TaskResult {
    TaskResult {
        output: json!({"published":true,"month":"2026-09","labels":["monthly","review"]}),
        writes: vec![],
    }
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-settled-deletion-{}-{}",
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

// A bounded, independent nine-table observer. Equality assertions below deliberately
// use booleans so failures never dump sensitive grant records or callback inputs.
#[derive(Clone, PartialEq)]
struct Rows(Vec<Vec<Vec<Cell>>>);
const TABLES: [(&str, &str, usize); 9] = [
    ("metadata", "id", 1),
    ("threads", "id", 4),
    ("executions", "id", 40),
    ("journal", "sequence", 512),
    ("approval_grants", "execution_id, interrupt_id", 64),
    ("approval_acceptances", "execution_id, turn_id", 64),
    ("permission_acceptances", "execution_id, turn_id", 64),
    ("permission_decisions", "permission_id", 64),
    ("permission_uses", "execution_id, task_id", 64),
];
fn rows(path: &Path) -> Rows {
    let db = connection(path);
    Rows(
        TABLES
            .iter()
            .map(|(table, order, cap)| {
                let mut stmt = db
                    .prepare(&format!(
                        "SELECT * FROM {table} ORDER BY {order} LIMIT {}",
                        cap + 1
                    ))
                    .unwrap();
                let cols = stmt.column_count();
                let found = stmt
                    .query_map([], |r| {
                        (0..cols)
                            .map(|i| r.get(i))
                            .collect::<Result<Vec<Cell>, _>>()
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert!(
                    found.len() <= *cap,
                    "bounded {table} observer reached sentinel"
                );
                found
            })
            .collect(),
    )
}
fn text(cell: &Cell) -> &str {
    match cell {
        Cell::Text(v) => v,
        _ => panic!("expected text cell"),
    }
}
fn owned(row: &[Cell], table: usize, thread: &ThreadId) -> bool {
    let column = match table {
        1 => 0,
        2 => 1,
        3 => 2,
        4 => 3,
        5 | 6 => 2,
        7 => 1,
        8 => 3,
        _ => return false,
    };
    text(&row[column]) == thread.as_str()
}
fn without(before: &Rows, thread: &ThreadId) -> Rows {
    Rows(
        before
            .0
            .iter()
            .enumerate()
            .map(|(table, data)| {
                data.iter()
                    .filter(|row| !owned(row, table, thread))
                    .cloned()
                    .collect()
            })
            .collect(),
    )
}
fn target(store: &SqliteStore, thread: &ThreadId) -> ThreadDeletionTarget {
    ThreadDeletionTarget::from_view(&store.thread_view(thread).unwrap().unwrap()).unwrap()
}
fn fence(store: &SqliteStore, thread: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}

struct Authority;
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        ctx: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if ctx == &context() && resource.thread.origin.as_ref() == Some(&ctx.actor) {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
impl PermissionCancellationAuthority for Authority {
    fn authorize_cancel(
        &self,
        ctx: &ApprovalContext,
        resource: &ApprovalResource,
        selected: &RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        self.authorize(ctx, ApprovalOperation::Dispatch, resource)
            .map_err(PermissionError::Approval)?;
        let view = ThreadView {
            thread: resource.thread.clone(),
            execution: Some(resource.execution.clone()),
        };
        if &RunCancellationTarget::from_view(&view)? == selected {
            Ok(())
        } else {
            Err(PermissionError::Store(StoreError::RevisionConflict))
        }
    }
}
struct DeleteAuthority {
    denied: bool,
    expected_owner: Option<u64>,
    calls: AtomicUsize,
}
impl DeleteAuthority {
    fn allowed() -> Arc<Self> {
        Arc::new(Self {
            denied: false,
            expected_owner: None,
            calls: AtomicUsize::new(0),
        })
    }
}
impl PermissionThreadDeletionAuthority for DeleteAuthority {
    fn authorize_delete(
        &self,
        ctx: &ApprovalContext,
        resource: &PermissionThreadDeletionResource,
        selected: &ThreadDeletionTarget,
    ) -> Result<(), PermissionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            &ThreadDeletionTarget::from_view(resource.view()).unwrap(),
            selected
        );
        assert!(resource.store_owner_generation() > 0);
        if let Some(owner) = self.expected_owner {
            assert_eq!(resource.store_owner_generation(), owner);
        }
        if !self.denied
            && ctx == &context()
            && resource.view().thread.origin.as_ref() == Some(&ctx.actor)
        {
            Ok(())
        } else {
            Err(PermissionError::Approval(ApprovalError::Unauthorized))
        }
    }
}
struct Constraint;
impl ToolConstraint for Constraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
#[derive(Clone, Copy)]
enum Outcome {
    Success,
    Failed,
    CancelAck,
    CancelLateSuccess,
}
struct Callback {
    store: Weak<SqliteStore>,
    catalog: Mutex<Weak<PermissionCatalog>>,
    outcome: Mutex<Outcome>,
    original: Mutex<bool>,
    signal: Arc<NativeCancellationSignal>,
    calls: AtomicUsize,
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        panic!("permission Driver must enter the controlled native callback")
    }
    fn invoke_controlled(
        &self,
        invocation: &EffectInvocation,
        invocation_context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(invocation_context.origin, context().actor);
        assert!(invocation.input == Map::from_iter([("input".into(), json!({"month":"2026-09"}))]));
        let outcome = *self.outcome.lock().unwrap();
        if matches!(outcome, Outcome::CancelAck | Outcome::CancelLateSuccess) {
            // The actual callback has entered. A dedicated management request
            // records cancellation before this real native outcome is returned.
            let store = self.store.upgrade().unwrap();
            let catalog = self.catalog.lock().unwrap().upgrade().unwrap();
            let view = store
                .thread_view(&invocation_context.thread_id)
                .unwrap()
                .unwrap();
            assert_eq!(
                view.execution.as_ref().unwrap().snapshot.tasks[0].status,
                TaskStatus::InFlight
            );
            let selected = RunCancellationTarget::from_view(&view).unwrap();
            let requested = if *self.original.lock().unwrap() {
                request_permission_original_always_cancellation(
                    store.as_ref(),
                    &Authority,
                    &catalog,
                    &PermissionOriginalAlwaysCancellationRequest {
                        context: &context(),
                        target: &selected,
                        facts: facts(109),
                    },
                    &mut PermissionEvidenceBudget::new(),
                )
            } else {
                request_permission_use_cancellation(
                    store.as_ref(),
                    &Authority,
                    &catalog,
                    &PermissionUseCancellationRequest {
                        context: &context(),
                        target: &selected,
                        facts: facts(109),
                    },
                    &mut PermissionEvidenceBudget::new(),
                )
            }
            .unwrap();
            assert_eq!(requested.snapshot.status, ExecutionStatus::CancelRequested);
            self.signal.commit_cancellation();
            assert!(observer.cancellation_requested());
        }
        Ok(match outcome {
            Outcome::CancelAck => ControlledNativeOutcome::Cancelled,
            Outcome::Success | Outcome::CancelLateSuccess => {
                ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(result()))
            }
            Outcome::Failed => ControlledNativeOutcome::Settled(NativeOutcome::Failed(
                "publisher rejected report".into(),
            )),
        })
    }
}
struct Hooks(AttemptId);
impl PermissionUseDriverHooks for Hooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        facts(if current.snapshot.tasks[0].status == TaskStatus::Ready {
            108
        } else {
            110
        })
    }
    fn attempt_id(&self) -> AttemptId {
        self.0.clone()
    }
}
struct History {
    thread: ThreadId,
    definition: Definition,
    catalog: Arc<PermissionCatalog>,
    callback: Arc<Callback>,
    ids: Vec<ExecutionId>,
}
impl History {
    fn new(store: &Arc<SqliteStore>) -> Self {
        let thread = ThreadId::new(id()).unwrap();
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(context().actor),
            })
            .unwrap();
        let definition = Definition {
            route: RouteKey::new("/reports#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new(TASK).unwrap(),
                effect_id: EffectId::new(EFFECT).unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            }]],
            max_attempts: 1,
        };
        let callback = Arc::new(Callback {
            store: Arc::downgrade(store),
            catalog: Mutex::new(Weak::new()),
            outcome: Mutex::new(Outcome::Success),
            original: Mutex::new(true),
            signal: Arc::new(NativeCancellationSignal::new()),
            calls: AtomicUsize::new(0),
        });
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
                            callback: callback.clone(),
                            output_kind: OutputKind::Value,
                        },
                    )]),
                    input_validator: None,
                }],
            )
            .unwrap(),
        );
        let catalog = Arc::new(
            PermissionCatalog::new(
                application,
                vec![ToolBindingRegistration {
                    definition: definition.clone(),
                    effect_id: EffectId::new(EFFECT).unwrap(),
                    tool_id: StableToolId::new("report-publisher").unwrap(),
                    tool_revision: StableToolRevision::new("tool-1").unwrap(),
                    constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                    constraint: Arc::new(Constraint),
                }],
            )
            .unwrap(),
        );
        *callback.catalog.lock().unwrap() = Arc::downgrade(&catalog);
        Self {
            thread,
            definition,
            catalog,
            callback,
            ids: vec![],
        }
    }
    fn start(&mut self, store: &SqliteStore) -> StoredExecution {
        let execution = ExecutionId::new(id()).unwrap();
        let initial = new_execution(
            execution.clone(),
            TurnId::new(id()).unwrap(),
            store.owner_generation(),
            self.definition.clone(),
            Map::from_iter([("input".into(), json!({"month":"2026-09"}))]),
            facts(100),
        )
        .unwrap();
        let admitted = store.admit(&self.thread, initial).unwrap();
        let started = store
            .commit(
                &execution,
                (&admitted.snapshot).into(),
                transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
            )
            .unwrap();
        self.ids.push(execution);
        started
    }
    fn original_ready(&mut self, store: &Arc<SqliteStore>) -> StoredExecution {
        let started = self.start(store);
        let execution = &started.snapshot.execution_id;
        let service = ApprovalService::new(store.clone(), Arc::new(Authority)).unwrap();
        service
            .park(
                &context(),
                execution,
                fence(store, &self.thread),
                TaskId::new(TASK).unwrap(),
                InterruptId::new("publish-approval").unwrap(),
                json!({"question":"Publish monthly report?","month":"2026-09"}),
                60_000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(&context(), execution, fence(store, &self.thread))
            .unwrap();
        assert_eq!(pending.len(), 1);
        accept_permission_always_with_normal_inputs(
            store.clone(),
            Arc::new(Authority),
            self.catalog.clone(),
            execution,
            fence(store, &self.thread),
            ApprovalAcceptanceRequest {
                context: context(),
                route: self.definition.route.clone(),
                definition_revision: self.definition.revision.clone(),
                turn_id: TurnId::new(id()).unwrap(),
                now_ms: 103,
                decisions: vec![GrantDecision {
                    interrupt_id: pending[0].interrupt_id.clone(),
                    decision: ApprovalDecision::Always,
                    cancelled: false,
                    token: Some(pending[0].token.clone()),
                }],
            },
        )
        .unwrap();
        store.execution(execution).unwrap().unwrap()
    }
    fn saved_ready(&mut self, store: &SqliteStore) -> StoredExecution {
        let started = self.start(store);
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
        try_authorize_permission_ready(
            store,
            &Authority,
            &self.catalog,
            fence(store, &self.thread),
            &PermissionReadyAuthorizationRequest {
                context: &context(),
                command: &command,
                facts: facts(102),
            },
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap()
        .expect("genuine saved permission")
    }
    fn ordinary_once(&mut self, store: &Arc<SqliteStore>) {
        let started = self.start(store);
        let execution = &started.snapshot.execution_id;
        let service = ApprovalService::new(store.clone(), Arc::new(Authority)).unwrap();
        service
            .park(
                &context(),
                execution,
                fence(store, &self.thread),
                TaskId::new(TASK).unwrap(),
                InterruptId::new("publish-once").unwrap(),
                json!({"question":"Publish this one monthly report?"}),
                60_000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(&context(), execution, fence(store, &self.thread))
            .unwrap();
        assert_eq!(pending.len(), 1);
        service
            .accept(
                execution,
                fence(store, &self.thread),
                ApprovalAcceptanceRequest {
                    context: context(),
                    route: self.definition.route.clone(),
                    definition_revision: self.definition.revision.clone(),
                    turn_id: TurnId::new(id()).unwrap(),
                    now_ms: 103,
                    decisions: vec![GrantDecision {
                        interrupt_id: pending[0].interrupt_id.clone(),
                        decision: ApprovalDecision::Once,
                        cancelled: false,
                        token: Some(pending[0].token.clone()),
                    }],
                },
            )
            .unwrap();
        let invocation_context = InvocationContext {
            thread_id: self.thread.clone(),
            thread_generation: store.thread(&self.thread).unwrap().unwrap().generation,
            origin: context().actor,
            application_revision: "application-1".into(),
            execution_revision: self.definition.revision.clone(),
            policy_revision: context().policy_revision,
        };
        let observer = self.callback.signal.observer();
        let settled = service
            .dispatch_outcome(
                &context(),
                execution,
                fence(store, &self.thread),
                TaskId::new(TASK).unwrap(),
                AttemptId::new(id()).unwrap(),
                facts(108),
                || facts(110),
                |invocation| {
                    // The ordinary accepted Once invokes the same registered native
                    // callback synchronously through the real application registry.
                    match self
                        .catalog
                        .application()
                        .invoke_controlled(
                            &self.definition,
                            invocation,
                            &invocation_context,
                            &observer,
                        )
                        .map_err(|_| "ordinary native callback failed".to_string())?
                    {
                        ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(result)) => {
                            Ok(EffectOutcome::Succeeded(result))
                        }
                        _ => Err("ordinary Once expected actual native success".into()),
                    }
                },
            )
            .unwrap();
        assert_eq!(settled.snapshot.status, ExecutionStatus::Completed);
        assert!(settled.snapshot.tasks[0].result.as_ref() == Some(&result()));
    }
    fn drive(
        &self,
        store: &SqliteStore,
        ready: StoredExecution,
        original: bool,
        outcome: Outcome,
    ) -> StoredExecution {
        *self.callback.original.lock().unwrap() = original;
        *self.callback.outcome.lock().unwrap() = outcome;
        let ctx = context();
        let capacity = TaskCapacity::new(1).unwrap();
        let hooks = Hooks(AttemptId::new(id()).unwrap());
        // Each cancelled history below has one actual cancellation; normal
        // subsequent calls use fresh histories rather than reusing a fired signal.
        let observer = self.callback.signal.observer();
        let actual = if original {
            drive_permission_original_always_cancellable_execution(
                PermissionOriginalAlwaysDriverBindings {
                    store,
                    authority: &Authority,
                    producer_catalog: &self.catalog,
                    current_catalog: &self.catalog,
                    context: &ctx,
                    hooks: &hooks,
                    capacity: &capacity,
                },
                ready,
                &observer,
            )
            .unwrap()
        } else {
            drive_permission_use_cancellable_execution(
                PermissionUseDriverBindings {
                    store,
                    authority: &Authority,
                    producer_catalog: &self.catalog,
                    current_catalog: &self.catalog,
                    context: &ctx,
                    hooks: &hooks,
                    capacity: &capacity,
                },
                ready,
                &observer,
            )
            .unwrap()
        };
        let expected = match outcome {
            Outcome::Success => ExecutionStatus::Completed,
            Outcome::Failed => ExecutionStatus::Failed,
            _ => ExecutionStatus::Cancelled,
        };
        assert_eq!(actual.snapshot.status, expected);
        if matches!(outcome, Outcome::Success | Outcome::CancelLateSuccess) {
            assert_eq!(actual.snapshot.tasks[0].status, TaskStatus::Succeeded);
            assert_eq!(actual.snapshot.tasks[0].result.as_ref(), Some(&result()));
        }
        actual
    }
    fn complete(&mut self, store: &Arc<SqliteStore>, total: usize) {
        assert!((1..=17).contains(&total));
        let calls_before = self.callback.calls.load(Ordering::SeqCst);
        let original = self.original_ready(store);
        self.drive(store, original, true, Outcome::Success);
        for _ in 1..total {
            let ready = self.saved_ready(store);
            self.drive(store, ready, false, Outcome::Success);
        }
        assert_eq!(
            self.callback.calls.load(Ordering::SeqCst),
            calls_before + total
        );
    }
}

fn delete_and_check(store: &Arc<SqliteStore>, path: &Path, thread: &ThreadId, ids: &[ExecutionId]) {
    let selected = target(store, thread);
    let before = rows(path);
    let expected = without(&before, thread);
    let expected_count = before.0.iter().map(Vec::len).sum::<usize>()
        - expected.0.iter().map(Vec::len).sum::<usize>();
    let authority = Arc::new(DeleteAuthority {
        denied: false,
        expected_owner: Some(store.owner_generation()),
        calls: AtomicUsize::new(0),
    });
    let confirmation =
        delete_settled_permission_thread(store.clone(), authority.clone(), &context(), &selected)
            .unwrap();
    assert!(authority.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(confirmation.target(), &selected);
    assert_eq!(
        confirmation.store_owner_generation(),
        store.owner_generation()
    );
    assert_eq!(confirmation.removed_rows(), expected_count);
    let actual_ids = confirmation
        .removed_execution_ids()
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_ids, ids.iter().cloned().collect::<BTreeSet<_>>());
    assert_eq!(confirmation.removed_execution_ids().len(), ids.len());
    assert!(
        rows(path) == expected,
        "exact eight-table deletion, foreign bytes and metadata preserved"
    );
    assert!(store.thread_view(thread).unwrap().is_none());
    for execution in ids {
        assert!(store.execution(execution).unwrap().is_none());
        assert!(store.journal(execution).unwrap().is_empty());
    }
    store.readiness().unwrap();
}
fn refuse<A: PermissionThreadDeletionAuthority + 'static>(
    store: &Arc<SqliteStore>,
    path: &Path,
    ctx: &ApprovalContext,
    selected: &ThreadDeletionTarget,
    authority: Arc<A>,
) {
    let before = rows(path);
    assert!(delete_settled_permission_thread(store.clone(), authority, ctx, selected).is_err());
    assert!(
        rows(path) == before,
        "refusal must preserve every durable table"
    );
}

#[test]
fn completed_always_and_multiple_saved_uses_delete_exactly_after_normal_owner_reopen() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert!(store
        .as_ref()
        .permission_settled_thread_deletion_atomic_durable());
    assert!(
        store.permission_settled_thread_deletion_atomic_durable(),
        "Arc forwards narrow support"
    );
    let mut history = History::new(&store);
    history.ordinary_once(&store);
    history.complete(&store, 3);
    let mut foreign = History::new(&store);
    foreign.complete(&store, 2);
    let before_reopen = rows(&directory.0);
    assert_eq!(before_reopen.0[4].len(), 3);
    assert_eq!(
        before_reopen.0[5].len(),
        1,
        "genuine ordinary Once acceptance"
    );
    assert_eq!(before_reopen.0[6].len(), 2);
    assert_eq!(before_reopen.0[7].len(), 2);
    assert_eq!(before_reopen.0[8].len(), 3);
    let owned_counts: Vec<_> = before_reopen
        .0
        .iter()
        .enumerate()
        .skip(1)
        .map(|(table, data)| {
            data.iter()
                .filter(|row| owned(row, table, &history.thread))
                .count()
        })
        .collect();
    assert_eq!(
        owned_counts,
        vec![1, 4, 23, 2, 1, 1, 1, 2],
        "all eight owned tables contain genuine history; total 35 rows"
    );
    let thread = history.thread.clone();
    let ids = history.ids.clone();
    let foreign_thread = foreign.thread.clone();
    let old_owner = store.owner_generation();
    drop(history);
    drop(foreign);
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(reopened.owner_generation(), old_owner + 1);
    assert!(rows(&directory.0).0[1..] == before_reopen.0[1..]);
    let selected = target(&reopened, &thread);
    assert_eq!(
        selected.execution_fence.as_ref().unwrap().owner_generation,
        old_owner
    );
    let before_generic = rows(&directory.0);
    assert!(
        reopened.delete_thread_fenced(&selected).is_err(),
        "generic deletion retains permission guard"
    );
    assert!(rows(&directory.0) == before_generic);
    delete_and_check(&reopened, &directory.0, &thread, &ids);
    assert!(reopened.thread_view(&foreign_thread).unwrap().is_some());
    let after = rows(&directory.0);
    drop(reopened);
    let final_store = SqliteStore::open(&directory.0).unwrap();
    assert!(rows(&directory.0).0[1..] == after.0[1..]);
    assert!(final_store.thread_view(&thread).unwrap().is_none());
    final_store.readiness().unwrap();
    drop(final_store);
}

#[test]
fn real_cancelled_original_and_saved_late_outcome_remain_recorded_until_deleted() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let mut original = History::new(&store);
    let ready = original.original_ready(&store);
    let ack = original.drive(&store, ready, true, Outcome::CancelAck);
    assert_eq!(ack.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert!(
        store
            .execution(&ack.snapshot.execution_id)
            .unwrap()
            .as_ref()
            == Some(&ack)
    );
    delete_and_check(&store, &directory.0, &original.thread, &original.ids);
    let mut saved = History::new(&store);
    saved.complete(&store, 1);
    let ready = saved.saved_ready(&store);
    let late = saved.drive(&store, ready, false, Outcome::CancelLateSuccess);
    assert_eq!(late.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert!(
        store
            .execution(&late.snapshot.execution_id)
            .unwrap()
            .as_ref()
            == Some(&late)
    );
    assert_eq!(
        rows(&directory.0).0[8].len(),
        1,
        "actual saved Use remains until deletion"
    );
    delete_and_check(&store, &directory.0, &saved.thread, &saved.ids);
    let mut failed = History::new(&store);
    failed.complete(&store, 1);
    let ready = failed.saved_ready(&store);
    failed.drive(&store, ready, false, Outcome::Failed);
    delete_and_check(&store, &directory.0, &failed.thread, &failed.ids);
    drop(original);
    drop(saved);
    drop(failed);
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert!(rows(&directory.0).0[1..].iter().all(Vec::is_empty));
    reopened.readiness().unwrap();
    drop(reopened);
}

// An embedding with ordinary durable SQLite support but no opt-in to the new
// deletion profile. In particular, do not override the new declaration here.
struct DefaultDeletionStore(Arc<SqliteStore>);
impl Store for DefaultDeletionStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.0.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.0.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.0.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.0.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.0.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.0.execution(id)
    }
    fn admit(&self, id: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.0.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.0.admit_fenced(id, generation, revision, initial)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.0.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.0.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.0.journal(id)
    }
}
impl ApprovalStore for DefaultDeletionStore {
    fn approval_durable(&self) -> bool {
        self.0.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.0.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.0.commit_approval(id, fence, plan, mutation)
    }
}
impl PermissionStore for DefaultDeletionStore {
    fn durable_permissions(&self) -> bool {
        self.0.durable_permissions()
    }
}
struct OriginPermissive;
impl PermissionThreadDeletionAuthority for OriginPermissive {
    fn authorize_delete(
        &self,
        _: &ApprovalContext,
        _: &PermissionThreadDeletionResource,
        _: &ThreadDeletionTarget,
    ) -> Result<(), PermissionError> {
        Ok(())
    }
}

#[test]
fn current_authority_exact_target_settlement_bounds_and_foreign_links_are_required() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let mut history = History::new(&store);
    history.complete(&store, 2);
    let selected = target(&store, &history.thread);
    let denied = Arc::new(DeleteAuthority {
        denied: true,
        expected_owner: None,
        calls: AtomicUsize::new(0),
    });
    refuse(&store, &directory.0, &context(), &selected, denied.clone());
    assert!(
        denied.calls.load(Ordering::SeqCst) > 0,
        "current dedicated authority was consulted"
    );

    let mut wrong_origin = context();
    wrong_origin.actor.principal = "different-editor".into();
    let before = rows(&directory.0);
    assert!(delete_settled_permission_thread(
        store.clone(),
        Arc::new(OriginPermissive),
        &wrong_origin,
        &selected
    )
    .is_err());
    assert!(
        rows(&directory.0) == before,
        "origin confinement holds even with permissive embedding authority"
    );
    let mut stale = selected.clone();
    stale.resource_revision += 1;
    refuse(
        &store,
        &directory.0,
        &context(),
        &stale,
        Arc::new(OriginPermissive),
    );
    let mut wrong_historical_owner = selected.clone();
    wrong_historical_owner
        .execution_fence
        .as_mut()
        .unwrap()
        .owner_generation += 1;
    refuse(
        &store,
        &directory.0,
        &context(),
        &wrong_historical_owner,
        Arc::new(OriginPermissive),
    );

    let unsupported = Arc::new(DefaultDeletionStore(store.clone()));
    assert!(!unsupported
        .as_ref()
        .permission_settled_thread_deletion_atomic_durable());
    assert!(!unsupported.permission_settled_thread_deletion_atomic_durable());
    let before = rows(&directory.0);
    assert!(delete_settled_permission_thread(
        unsupported.clone(),
        DeleteAuthority::allowed(),
        &context(),
        &selected
    )
    .is_err());
    assert!(
        rows(&directory.0) == before,
        "default-false profile never mutates ordinary durable SQLite"
    );
    drop(unsupported);

    let mut foreign = History::new(&store);
    foreign.complete(&store, 2);
    let db = connection(&directory.0);
    let (foreign_permission, original_record): (String, String) = db
        .query_row(
            "SELECT permission_id, record FROM permission_decisions WHERE thread_id=?1",
            [foreign.thread.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let owned_permission: String = db
        .query_row(
            "SELECT permission_id FROM permission_decisions WHERE thread_id=?1",
            [history.thread.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    // Adversarial setup on fixture-owned data only: a foreign decision's
    // creator is payload-only, without a SQL creator column or foreign key.
    let mut forged: serde_json::Value = serde_json::from_str(&original_record).unwrap();
    forged["creator_execution_id"] = json!(history.ids[0].as_str());
    db.execute(
        "UPDATE permission_decisions SET record=?1 WHERE permission_id=?2",
        rusqlite::params![serde_json::to_string(&forged).unwrap(), foreign_permission],
    )
    .unwrap();
    refuse(
        &store,
        &directory.0,
        &context(),
        &selected,
        DeleteAuthority::allowed(),
    );
    // A structurally unknown creator cannot be treated as proof of disjointness.
    forged["creator_execution_id"] =
        json!({"unrecognized_creator_encoding":history.ids[0].as_str()});
    db.execute(
        "UPDATE permission_decisions SET record=?1 WHERE permission_id=?2",
        rusqlite::params![serde_json::to_string(&forged).unwrap(), foreign_permission],
    )
    .unwrap();
    refuse(
        &store,
        &directory.0,
        &context(),
        &selected,
        DeleteAuthority::allowed(),
    );
    db.execute(
        "UPDATE permission_decisions SET record=?1 WHERE permission_id=?2",
        rusqlite::params![original_record, foreign_permission],
    )
    .unwrap();
    // Adversarial indexed reverse link: retain a genuine foreign Use row and
    // corrupt only its indexed permission reference to the owned decision.
    let foreign_execution = foreign.ids[1].as_str();
    db.execute(
        "UPDATE permission_uses SET permission_id=?1 WHERE execution_id=?2",
        rusqlite::params![owned_permission, foreign_execution],
    )
    .unwrap();
    refuse(
        &store,
        &directory.0,
        &context(),
        &selected,
        DeleteAuthority::allowed(),
    );
    db.execute(
        "UPDATE permission_uses SET permission_id=?1 WHERE execution_id=?2",
        rusqlite::params![foreign_permission, foreign_execution],
    )
    .unwrap();
    db.close().unwrap();
    store.readiness().unwrap();
    // Healthy control after restoration proves unrelated genuine foreign
    // decisions/Uses do not prevent deletion.
    delete_and_check(&store, &directory.0, &history.thread, &history.ids);
    delete_and_check(&store, &directory.0, &foreign.thread, &foreign.ids);

    let mut live = History::new(&store);
    let ready = live.original_ready(&store);
    assert_eq!(ready.snapshot.tasks[0].status, TaskStatus::Ready);
    refuse(
        &store,
        &directory.0,
        &context(),
        &target(&store, &live.thread),
        DeleteAuthority::allowed(),
    );
    live.drive(&store, ready, true, Outcome::Success);
    delete_and_check(&store, &directory.0, &live.thread, &live.ids);

    // The matched mixed-history row boundary uses genuine Once executions,
    // followed by an actual Always and saved Use: sixteen succeeds, seventeen refuses.
    let mut boundary = History::new(&store);
    for _ in 0..14 {
        boundary.ordinary_once(&store);
    }
    boundary.complete(&store, 2);
    assert_eq!(rows(&directory.0).0[2].len(), 16);
    delete_and_check(&store, &directory.0, &boundary.thread, &boundary.ids);
    let mut overflow = History::new(&store);
    for _ in 0..15 {
        overflow.ordinary_once(&store);
    }
    overflow.complete(&store, 2);
    assert_eq!(rows(&directory.0).0[2].len(), 17);
    refuse(
        &store,
        &directory.0,
        &context(),
        &target(&store, &overflow.thread),
        DeleteAuthority::allowed(),
    );
    store.readiness().unwrap();
    drop(history);
    drop(foreign);
    drop(live);
    drop(boundary);
    drop(overflow);
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    reopened.readiness().unwrap();
    assert_eq!(rows(&directory.0).0[2].len(), 17);
    drop(reopened);
}

#[test]
fn exact_late_thread_delete_abort_rolls_back_all_nine_tables_then_healthy_delete_succeeds() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let mut history = History::new(&store);
    history.complete(&store, 3);
    let selected = target(&store, &history.thread);
    let before = rows(&directory.0);
    assert_eq!(before.0[1].len(), 1);
    assert_eq!(before.0[2].len(), 3);
    assert_eq!(before.0[4].len(), 1);
    assert_eq!(before.0[6].len(), 1);
    assert_eq!(before.0[7].len(), 1);
    assert_eq!(before.0[8].len(), 2);
    // Install after normal open/schema validation. The trigger can raise this
    // exact fault only after all actual dependent DELETEs have run. DDL uses
    // exclusively this fixture's generated UUID, never application data.
    let ddl = format!(
        "CREATE TRIGGER settled_deletion_late_guard BEFORE DELETE ON threads
        WHEN OLD.id = '{}' AND OLD.active_execution_id IS NULL
        AND NOT EXISTS (SELECT 1 FROM executions WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM journal WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM approval_grants WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM approval_acceptances WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM permission_acceptances WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM permission_decisions WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM permission_uses WHERE thread_id = OLD.id)
        BEGIN SELECT RAISE(ABORT, 'settled_deletion_after_dependents'); END",
        history.thread,
    );
    let db = connection(&directory.0);
    db.execute_batch(&ddl).unwrap();
    let actual: String = db.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='settled_deletion_late_guard'", [], |r| r.get(0)).unwrap();
    assert_eq!(actual, ddl);
    let failure = match delete_settled_permission_thread(
        store.clone(),
        DeleteAuthority::allowed(),
        &context(),
        &selected,
    ) {
        Ok(_) => panic!("exact late DELETE must abort"),
        Err(error) => error,
    };
    // SQLite keeps code-only errors. The writer adds this fixed, non-sensitive
    // phase code only when executing the final owned-thread DELETE fails.
    assert!(
        matches!(failure, PermissionError::Store(StoreError::Unavailable(ref message)) if message == "permission_settled_thread_deletion_final_thread_delete_failed"),
        "failure must reach the guarded DELETE, not a schema scan"
    );
    assert!(
        rows(&directory.0) == before,
        "late DELETE failure rolls back metadata and all eight owned tables"
    );
    let still_present: String = db.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='settled_deletion_late_guard'", [], |r| r.get(0)).unwrap();
    assert_eq!(still_present, ddl);
    db.execute_batch("DROP TRIGGER settled_deletion_late_guard")
        .unwrap();
    let count: i64 = db.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='settled_deletion_late_guard'", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0);
    db.close().unwrap();
    assert_eq!(target(&store, &history.thread), selected);
    delete_and_check(&store, &directory.0, &history.thread, &history.ids);
    drop(history);
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    reopened.readiness().unwrap();
    assert!(rows(&directory.0).0[1..].iter().all(Vec::is_empty));
    drop(reopened);
}
