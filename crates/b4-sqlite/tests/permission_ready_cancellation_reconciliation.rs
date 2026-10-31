//! Focused provenance controls for real returned no-Work Driver failures.
//! The opaque confirmation is always returned by the actual typed SQLite writer.
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
    time::Instant,
};

// Share ports, not another integration test suite. All defaults delegate actual SQLite.
#[path = "../../b4-http/tests/permission_entered_support/store.rs"]
mod ports;
use ports::Boundary;
mod cooperative {
    use super::*;
    pub struct Gate {
        deadline: Instant,
        released: Mutex<bool>,
        changed: std::sync::Condvar,
    }
    impl Gate {
        pub fn hold(&self) -> bool {
            let mut released = self.released.lock().unwrap();
            while !*released {
                let remaining = self.deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return false;
                }
                released = self.changed.wait_timeout(released, remaining).unwrap().0;
            }
            Instant::now() < self.deadline
        }
    }
}
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
            "b4-ready-reconciliation-{}-{}",
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

struct Fixture {
    inner: Arc<SqliteStore>,
    store: Arc<Boundary>,
    authority: Arc<Authority>,
    catalog: Arc<PermissionCatalog>,
    calls: Arc<AtomicUsize>,
    ready: StoredExecution,
    hooks: Hooks,
    // Directory retires after both actual Store strong owners.
    directory: Directory,
}
impl Fixture {
    fn new(new_use: bool) -> Self {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
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
        let thread = ThreadId::new(id()).unwrap();
        inner
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(context().actor),
            })
            .unwrap();
        let authority = Arc::new(Authority {
            store: Arc::downgrade(&inner),
            definition: definition.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let catalog = Arc::new(catalog(&definition, Arc::new(Direct(calls.clone()))));
        let execution = ExecutionId::new(id()).unwrap();
        let initial = new_execution(
            execution.clone(),
            TurnId::new(id()).unwrap(),
            inner.owner_generation(),
            definition.clone(),
            Map::from_iter([("input".into(), json!({"month":"2026-09"}))]),
            facts(100),
        )
        .unwrap();
        let admitted = inner.admit(&thread, initial).unwrap();
        inner
            .commit(
                &execution,
                (&admitted.snapshot).into(),
                transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
            )
            .unwrap();
        let service = ApprovalService::new(inner.clone(), authority.clone()).unwrap();
        service
            .park(
                &context(),
                &execution,
                fence(&inner, &thread),
                TaskId::new(TASK).unwrap(),
                InterruptId::new("approval").unwrap(),
                json!({"question":"Publish?"}),
                60_000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(&context(), &execution, fence(&inner, &thread))
            .unwrap();
        let grant = &pending[0];
        accept_permission_always_with_normal_inputs(
            inner.clone(),
            authority.clone(),
            catalog.clone(),
            &execution,
            fence(&inner, &thread),
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
        drop(pending);
        drop(service);
        let original = inner.execution(&execution).unwrap().unwrap();
        assert_eq!(original.snapshot.revision, 3);
        assert_eq!(original.snapshot.tasks[0].status, TaskStatus::Ready);
        let ready = if new_use {
            // A real original typed cancellation preserves the saved permission.
            let target =
                RunCancellationTarget::from_view(&inner.thread_view(&thread).unwrap().unwrap())
                    .unwrap();
            cancel_permission_ready_with_normal_inputs(
                inner.as_ref(),
                authority.as_ref(),
                &catalog,
                &PermissionReadyCancellationRequest {
                    context: &context(),
                    target: &target,
                    facts: facts(104),
                },
                &mut PermissionEvidenceBudget::new(),
            )
            .unwrap();
            ready_use(&inner, &authority, &catalog, &thread, &definition, 105)
        } else {
            original
        };
        assert_eq!(ready.snapshot.revision, if new_use { 2 } else { 3 });
        assert_eq!(ready.snapshot.tasks[0].status, TaskStatus::Ready);
        let store = Arc::new(Boundary::new(inner.clone()));
        Self {
            directory,
            inner,
            store,
            authority,
            catalog,
            calls,
            ready,
            hooks: Hooks {
                attempt: AttemptId::new(id()).unwrap(),
                dispatch_ms: 110,
                completion_ms: 112,
            },
        }
    }
    fn bindings<'a, H: PermissionUseDriverHooks>(
        &'a self,
        ctx: &'a ApprovalContext,
        capacity: &'a TaskCapacity,
        hooks: &'a H,
    ) -> PermissionUseDriverBindings<'a, Boundary, Authority, H> {
        PermissionUseDriverBindings {
            store: self.store.as_ref(),
            authority: self.authority.as_ref(),
            producer_catalog: &self.catalog,
            current_catalog: &self.catalog,
            context: ctx,
            hooks,
            capacity,
        }
    }
    fn confirm(&self, new_use: bool) -> PermissionReadyCancellationConfirmation {
        let target = RunCancellationTarget::from_view(
            &self
                .inner
                .thread_view(&self.ready.thread_id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let mut budget = PermissionEvidenceBudget::for_ready_cancellation();
        if new_use {
            cancel_permission_use_ready_confirmed(
                self.store.as_ref(),
                self.authority.as_ref(),
                &self.catalog,
                &PermissionUseReadyCancellationRequest {
                    context: &context(),
                    target: &target,
                    facts: facts(110),
                },
                &mut budget,
            )
            .unwrap()
        } else {
            cancel_permission_ready_with_normal_inputs_confirmed(
                self.store.as_ref(),
                self.authority.as_ref(),
                &self.catalog,
                &PermissionReadyCancellationRequest {
                    context: &context(),
                    target: &target,
                    facts: facts(110),
                },
                &mut budget,
            )
            .unwrap()
        }
    }
    fn untouched(&self) {
        assert_eq!(self.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            self.inner
                .execution(&self.ready.snapshot.execution_id)
                .unwrap()
                .unwrap(),
            self.ready
        );
    }
}
// Bounded independent durable observer, including every authority and journal row.
fn raw(path: &Path) -> Vec<Vec<Vec<Cell>>> {
    let db = Connection::open(path.join("runtime.sqlite3")).unwrap();
    let result = [
        ("metadata", "id"),
        ("threads", "id"),
        ("executions", "id"),
        ("journal", "sequence"),
        ("approval_grants", "execution_id,interrupt_id"),
        ("approval_acceptances", "execution_id,turn_id"),
        ("permission_acceptances", "execution_id,turn_id"),
        ("permission_decisions", "permission_id"),
        ("permission_uses", "execution_id,task_id"),
    ]
    .into_iter()
    .map(|(table, order)| {
        let mut statement = db
            .prepare(&format!("SELECT * FROM {table} ORDER BY {order} LIMIT 65"))
            .unwrap();
        let width = statement.column_count();
        let rows = statement
            .query_map([], |r| {
                (0..width)
                    .map(|i| r.get(i))
                    .collect::<Result<Vec<Cell>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(rows.len() <= 64, "bounded {table} observer");
        rows
    })
    .collect();
    db.close().unwrap();
    result
}
fn indices(path: &Path) -> Vec<(String, String, Option<String>)> {
    let db = Connection::open(path.join("runtime.sqlite3")).unwrap();
    let result = {
        let mut statement = db.prepare("SELECT name,tbl_name,sql FROM sqlite_schema WHERE type='index' ORDER BY name LIMIT 65").unwrap();
        let rows = statement
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(rows.len() <= 64);
        rows
    };
    db.close().unwrap();
    result
}
fn no_work<S: PermissionStore, A: ApprovalAuthority>(
    failure: &PermissionUseDriverFailure<'_, S, A>,
) {
    assert_eq!(failure.callback_status(), None);
    assert!(failure.settled_outcome().is_none());
    assert_eq!(failure.usage().completion_entries(), 0);
    assert_eq!(failure.usage().candidate_entries(), 0);
    assert_eq!(failure.usage().committed_completions(), 0);
}

#[test]
fn actual_stale_initial_failures_reconcile_only_after_real_confirmed_original_and_new_use_writes() {
    for new_use in [false, true] {
        let fixture = Fixture::new(new_use);
        let capacity = TaskCapacity::new(1).unwrap();
        let ctx = context();
        let signal = NativeCancellationSignal::new();
        let confirmation = fixture.confirm(new_use);
        assert_eq!(
            confirmation.execution().snapshot.revision,
            if new_use { 3 } else { 4 }
        );
        assert_eq!(
            confirmation.execution().snapshot.status,
            ExecutionStatus::Cancelled
        );
        let committed = raw(&fixture.directory.0);
        // The real producer snapshot is now stale; no failure evidence is constructed.
        let reconciled = if new_use {
            let failure = drive_permission_use_cancellable_execution(
                fixture.bindings(&ctx, &capacity, &fixture.hooks),
                fixture.ready.clone(),
                &signal.observer(),
            )
            .unwrap_err();
            no_work(&failure);
            failure.reconcile_ready_cancellation(&confirmation).unwrap()
        } else {
            let failure = drive_permission_original_always_cancellable_execution(
                fixture.bindings(&ctx, &capacity, &fixture.hooks),
                fixture.ready.clone(),
                &signal.observer(),
            )
            .unwrap_err();
            assert_eq!(failure.callback_status(), None);
            assert!(failure.settled_outcome().is_none());
            assert_eq!(failure.usage().committed_completions(), 0);
            failure.reconcile_ready_cancellation(&confirmation).unwrap()
        };
        assert_eq!(&reconciled, confirmation.execution());
        assert_eq!(
            raw(&fixture.directory.0),
            committed,
            "reconciliation cannot write or replay"
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        assert!(!signal.observer().cancellation_requested());
    }
}
struct CancelOnFacts<'a> {
    fixture: &'a Fixture,
    confirmation: Mutex<Option<PermissionReadyCancellationConfirmation>>,
    once: AtomicBool,
}
impl PermissionUseDriverHooks for CancelOnFacts<'_> {
    fn facts(&self, _: &StoredExecution) -> TransitionFacts {
        // This hook executes inside the real prepare_dispatch path, after the
        // initial coherent check and before its typed Ready capture/fence check.
        if !self.once.swap(true, Ordering::SeqCst) {
            *self.confirmation.lock().unwrap() = Some(self.fixture.confirm(true));
        }
        facts(110)
    }
    fn attempt_id(&self) -> AttemptId {
        self.fixture.hooks.attempt.clone()
    }
}
#[test]
fn actual_preclaim_preparation_fence_refusal_reconciles_after_confirmed_ready_writer() {
    let fixture = Fixture::new(true);
    let capacity = TaskCapacity::new(1).unwrap();
    let ctx = context();
    let hooks = CancelOnFacts {
        fixture: &fixture,
        confirmation: Mutex::new(None),
        once: AtomicBool::new(false),
    };
    let signal = NativeCancellationSignal::new();
    let failure = drive_permission_use_cancellable_execution(
        fixture.bindings(&ctx, &capacity, &hooks),
        fixture.ready.clone(),
        &signal.observer(),
    )
    .unwrap_err();
    assert!(hooks.once.load(Ordering::SeqCst));
    no_work(&failure);
    let confirmation = hooks.confirmation.lock().unwrap().take().unwrap();
    let committed = raw(&fixture.directory.0);
    fixture.store.thread_view_reads.store(0, Ordering::SeqCst);
    assert_eq!(
        failure.reconcile_ready_cancellation(&confirmation).unwrap(),
        *confirmation.execution()
    );
    assert!(
        fixture.store.thread_view_reads.load(Ordering::SeqCst) > 0,
        "own retained Store coherent reread"
    );
    assert_eq!(raw(&fixture.directory.0), committed);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert!(!signal.observer().cancellation_requested());
}
#[test]
fn unrelated_actual_actor_failure_and_mismatched_initial_confirmation_retain_failure_without_writes(
) {
    let fixture = Fixture::new(true);
    let capacity = TaskCapacity::new(1).unwrap();
    let mut wrong_actor = context();
    wrong_actor.actor.principal = "unrelated-owner".into();
    let signal = NativeCancellationSignal::new();
    // Today this actual actor refusal is broad UnsupportedProfile, just like
    // initial staleness. The new seam must discriminate private provenance.
    let failure = drive_permission_use_cancellable_execution(
        fixture.bindings(&wrong_actor, &capacity, &fixture.hooks),
        fixture.ready.clone(),
        &signal.observer(),
    )
    .unwrap_err();
    no_work(&failure);
    let usage = failure.usage();
    let confirmation = fixture.confirm(true);
    let committed = raw(&fixture.directory.0);
    let mut retained = failure
        .reconcile_ready_cancellation(&confirmation)
        .unwrap_err();
    no_work(&retained);
    assert_eq!(
        retained.usage().committed_completions(),
        usage.committed_completions()
    );
    assert!(matches!(
        retained.retry_settlement(&facts(111)),
        Err(PermissionError::UnsupportedProfile)
    ));
    assert_eq!(raw(&fixture.directory.0), committed);
    drop(retained);

    // A legitimate stale failure cannot consume a different actual run's proof.
    let other = Fixture::new(true);
    let other_confirmation = other.confirm(true);
    let ctx = context();
    let failure = drive_permission_use_cancellable_execution(
        fixture.bindings(&ctx, &capacity, &fixture.hooks),
        fixture.ready.clone(),
        &signal.observer(),
    )
    .unwrap_err();
    let retained = failure
        .reconcile_ready_cancellation(&other_confirmation)
        .unwrap_err();
    no_work(&retained);
    assert_eq!(raw(&fixture.directory.0), committed);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert!(!signal.observer().cancellation_requested());
    drop(retained);

    // Fixture-only adversarial mutation AFTER the genuine same-run confirmation:
    // change only its coherent current thread resource revision. The execution
    // remains the genuine Cancelled terminal, so snapshot equality alone is
    // insufficient authority for reconciling the newly returned stale failure.
    let current = fixture
        .inner
        .thread_view(&fixture.ready.thread_id)
        .unwrap()
        .unwrap();
    let changed_revision = current.thread.resource_revision + 1;
    let db = Connection::open(fixture.directory.0.join("runtime.sqlite3")).unwrap();
    assert_eq!(
        db.execute(
            "UPDATE threads SET resource_revision=?1 WHERE id=?2",
            rusqlite::params![
                changed_revision.to_be_bytes().as_slice(),
                fixture.ready.thread_id.as_str()
            ]
        )
        .unwrap(),
        1
    );
    db.close().unwrap();
    let changed = fixture
        .inner
        .thread_view(&fixture.ready.thread_id)
        .unwrap()
        .unwrap();
    assert_eq!(changed.thread.resource_revision, changed_revision);
    assert_eq!(changed.execution.as_ref(), Some(confirmation.execution()));
    let changed_bytes = raw(&fixture.directory.0);
    let failure = drive_permission_use_cancellable_execution(
        fixture.bindings(&ctx, &capacity, &fixture.hooks),
        fixture.ready.clone(),
        &signal.observer(),
    )
    .unwrap_err();
    no_work(&failure);
    let retained = failure
        .reconcile_ready_cancellation(&confirmation)
        .unwrap_err();
    no_work(&retained);
    assert_eq!(
        raw(&fixture.directory.0),
        changed_bytes,
        "changed full current resource refuses without writes"
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert!(!signal.observer().cancellation_requested());
}

#[test]
fn genuine_other_store_same_identity_confirmation_cannot_bless_actual_own_store_still_ready() {
    let fixture = Fixture::new(true);
    let source_before = raw(&fixture.directory.0);
    let source_indices = indices(&fixture.directory.0);
    let copied = Directory::new();
    let destination = copied.0.join("runtime.sqlite3");
    assert!(!destination.exists());
    let db = Connection::open(fixture.directory.0.join("runtime.sqlite3")).unwrap();
    db.execute("VACUUM INTO ?1", [destination.to_str().unwrap()])
        .unwrap();
    db.close().unwrap();
    let actual_copy = raw(&copied.0);
    assert_eq!(indices(&copied.0), source_indices);
    assert_eq!(
        actual_copy, source_before,
        "VACUUM INTO includes committed live WAL evidence"
    );
    // Adversarial fixture setup ONLY: the subsequent real open increments this
    // copied counter to A's owner. No execution/journal/authority bytes change.
    // This is not a backup restore or unresolved-Ready recovery workflow.
    let db = Connection::open(&destination).unwrap();
    db.execute(
        "UPDATE metadata SET owner_epoch=?1 WHERE id=1",
        [(fixture.inner.owner_generation() - 1)
            .to_be_bytes()
            .as_slice()],
    )
    .unwrap();
    db.close().unwrap();
    let other_inner = Arc::new(SqliteStore::open(&copied.0).unwrap());
    assert_eq!(
        other_inner.owner_generation(),
        fixture.inner.owner_generation()
    );
    assert_eq!(
        raw(&copied.0),
        source_before,
        "every row and index identity survives actual copy open"
    );
    let other_authority = Authority {
        store: Arc::downgrade(&other_inner),
        definition: fixture.ready.snapshot.definition.clone(),
    };
    let target = RunCancellationTarget::from_view(
        &other_inner
            .thread_view(&fixture.ready.thread_id)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let confirmation = cancel_permission_use_ready_confirmed(
        other_inner.as_ref(),
        &other_authority,
        &fixture.catalog,
        &PermissionUseReadyCancellationRequest {
            context: &context(),
            target: &target,
            facts: facts(110),
        },
        &mut PermissionEvidenceBudget::for_ready_cancellation(),
    )
    .unwrap();
    assert_eq!(
        confirmation.execution().snapshot.execution_id,
        fixture.ready.snapshot.execution_id
    );
    assert_eq!(
        confirmation.execution().snapshot.owner_generation,
        fixture.ready.snapshot.owner_generation
    );
    assert_eq!(
        confirmation.execution().snapshot.status,
        ExecutionStatus::Cancelled
    );
    assert_eq!(
        raw(&fixture.directory.0),
        source_before,
        "copy write cannot mutate A"
    );
    fixture.untouched();
    let capacity = TaskCapacity::new(1).unwrap();
    let ctx = context();
    let signal = NativeCancellationSignal::new();
    // Bounded injected RevisionConflict from the preclaim Store port, after
    // delegating the actual SQLite capture. It is an actual returned Driver
    // preparation failure, not a fabricated failure or concurrent SQLite write.
    fixture
        .store
        .refuse_ready_capture
        .store(true, Ordering::SeqCst);
    let failure = drive_permission_use_cancellable_execution(
        fixture.bindings(&ctx, &capacity, &fixture.hooks),
        fixture.ready.clone(),
        &signal.observer(),
    )
    .unwrap_err();
    no_work(&failure);
    assert!(!fixture.store.refuse_ready_capture.load(Ordering::SeqCst));
    fixture.store.thread_view_reads.store(0, Ordering::SeqCst);
    let retained = failure
        .reconcile_ready_cancellation(&confirmation)
        .unwrap_err();
    no_work(&retained);
    assert!(
        fixture.store.thread_view_reads.load(Ordering::SeqCst) > 0,
        "must consult A through retained Store"
    );
    fixture.untouched();
    assert_eq!(raw(&fixture.directory.0), source_before);
    assert_eq!(indices(&fixture.directory.0), source_indices);
    assert_eq!(indices(&copied.0), source_indices);
    assert!(!signal.observer().cancellation_requested());
    let other_after = raw(&copied.0);
    drop(retained);
    drop(other_inner);
    let reopened = SqliteStore::open(&copied.0).unwrap();
    assert_eq!(
        reopened
            .execution(&fixture.ready.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        *confirmation.execution()
    );
    let mut expected = other_after;
    expected[0][0][3] = Cell::Blob(reopened.owner_generation().to_be_bytes().to_vec());
    assert_eq!(raw(&copied.0), expected);
    reopened.readiness().unwrap();
    drop(reopened);
}
