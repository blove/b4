use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    fs::{DirBuilder, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub const WAIT: Duration = Duration::from_secs(15);
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

pub struct Directory(PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-startup-preparation-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(&path).unwrap();
        for name in ["owner.lock", "runtime.sqlite3"] {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            drop(options.open(path.join(name)).unwrap());
        }
        #[cfg(unix)]
        {
            assert_eq!(
                std::fs::symlink_metadata(&path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            for name in ["owner.lock", "runtime.sqlite3"] {
                let metadata = std::fs::symlink_metadata(path.join(name)).unwrap();
                assert!(metadata.is_file());
                assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            }
        }
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn connection(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
    pub fn rows(&self) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
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
        .map(|table| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (table.into(), rows)
        })
        .collect()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn principal() -> Principal {
    Principal {
        application: "startup-fixture-application".into(),
        tenant: "startup-fixture-tenant".into(),
        principal: "startup-fixture-principal".into(),
    }
}

pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers
            .get("authorization")
            .is_some_and(|value| value == "Bearer startup-fixture")
        {
            Ok(principal())
        } else {
            Err(AuthenticationError)
        }
    }
}

pub struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, actor: &Principal, _: &ResourceContext) -> bool {
        actor == &principal()
    }
}

pub fn definition() -> Definition {
    Definition {
        route: RouteKey::new("/startup-fixture#workflow").unwrap(),
        revision: ExecutionRevision::new("startup-fixture-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("finite-task").unwrap(),
            effect_id: EffectId::new("finite-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]],
        max_attempts: 3,
    }
}

pub struct FiniteEffect(pub Arc<AtomicUsize>);
impl NativeEffect for FiniteEffect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("startup-finite-value"),
            writes: vec![],
        }))
    }
}

pub fn application(calls: Arc<AtomicUsize>) -> Arc<NativeApplication> {
    Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: principal().application,
                revision: "startup-native-fixture-1".into(),
            },
            vec![RouteRegistration {
                definition: definition(),
                effects: BTreeMap::from([(
                    EffectId::new("finite-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(FiniteEffect(calls)),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    )
}

pub fn config(application: Arc<NativeApplication>) -> NativeHostConfig {
    NativeHostConfig::new(
        application,
        ApprovalHostConfig::new("startup-fixture-policy-1".into(), BTreeMap::new()),
    )
}

pub fn seed_accepted(store: &SqliteStore) -> ThreadView {
    let actor = principal();
    let thread_id = ThreadId::new("startup-fixture-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 1,
            metadata: Map::from_iter([("fixture".into(), json!("startup"))]),
            origin: Some(ThreadOrigin {
                application: actor.application,
                tenant: actor.tenant,
                principal: actor.principal,
            }),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("startup-fixture-execution").unwrap(),
        TurnId::new("startup-fixture-original-turn").unwrap(),
        store.owner_generation(),
        definition(),
        Map::new(),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    store.admit(&thread_id, initial).unwrap();
    let view = store.thread_view(&thread_id).unwrap().unwrap();
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Accepted);
    assert_eq!(snapshot.owner_generation, store.owner_generation());
    assert_eq!(snapshot.definition, definition());
    assert!(snapshot.tasks.is_empty());
    let authority = store
        .approval_view(&snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(authority.grants.is_empty());
    assert!(authority.acceptances.is_empty());
    view
}

pub async fn quiesce(handle: &ShutdownHandle) {
    quiesce_with_fault(handle, false).await;
}

pub async fn quiesce_with_fault(handle: &ShutdownHandle, faulted: bool) {
    handle.begin_shutdown();
    let outcome = tokio::time::timeout(WAIT, handle.wait_quiescent(WAIT))
        .await
        .expect("shutdown observation remained finite")
        .unwrap();
    assert_eq!(outcome, ShutdownOutcome::Quiescent { faulted });
}

pub fn origin() -> ThreadOrigin {
    let actor = principal();
    ThreadOrigin {
        application: actor.application,
        tenant: actor.tenant,
        principal: actor.principal,
    }
}
pub fn approval_context() -> ApprovalContext {
    ApprovalContext {
        actor: origin(),
        policy_revision: "startup-fixture-policy-1".into(),
    }
}
pub struct Authority;
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if context == &approval_context() && resource.thread.origin.as_ref() == Some(&origin()) {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}

#[derive(Clone, Copy)]
pub enum SeedMode {
    Accepted,
    Running,
    Parked,
    Approved,
    Unsafe,
    Reconcilable,
    AttemptLimit,
    Cancelling,
    FailureDrain,
    Terminal,
}
pub fn mode_definition(mode: SeedMode) -> Definition {
    let mut result = definition();
    result.batches[0][0].recovery = match mode {
        SeedMode::Unsafe => RecoveryPolicy::Unsafe,
        SeedMode::Reconcilable => RecoveryPolicy::Reconcilable,
        _ => RecoveryPolicy::Pure,
    };
    result.batches[0][0].requires_approval = matches!(mode, SeedMode::Parked | SeedMode::Approved);
    if matches!(mode, SeedMode::AttemptLimit) {
        result.max_attempts = 1;
    }
    if matches!(mode, SeedMode::FailureDrain) {
        let mut sibling = result.batches[0][0].clone();
        sibling.id = TaskId::new("sibling-task").unwrap();
        sibling.effect_id = EffectId::new("sibling-effect").unwrap();
        result.batches[0].push(sibling);
    }
    result
}
pub fn apply(
    store: &impl Store,
    current: &StoredExecution,
    command: KernelCommand,
) -> StoredExecution {
    Runtime::new(store)
        .apply(
            &current.snapshot.execution_id,
            ExecutionFence::from(&current.snapshot),
            command,
            TransitionFacts {
                now_ms: current.snapshot.updated_at_ms + 1,
            },
        )
        .unwrap()
}
pub fn seed_named(
    store: Arc<SqliteStore>,
    name: &str,
    mode: SeedMode,
    with_origin: bool,
) -> ApprovalView {
    let thread_id = ThreadId::new(format!("startup-thread-{name}")).unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 1,
            metadata: Map::from_iter([("private-fixture".into(), json!(name))]),
            origin: with_origin.then(origin),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new(format!("startup-execution-{name}")).unwrap(),
        TurnId::new(format!("startup-original-turn-{name}")).unwrap(),
        store.owner_generation(),
        mode_definition(mode),
        Map::from_iter([("retained".into(), json!(name))]),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    let mut current = store.admit(&thread_id, initial).unwrap();
    if !matches!(mode, SeedMode::Accepted) {
        current = apply(store.as_ref(), &current, KernelCommand::Start);
    }
    if matches!(mode, SeedMode::Parked | SeedMode::Approved) {
        let service = ApprovalService::new(store.clone(), Authority).unwrap();
        current = service
            .park(
                &approval_context(),
                &current.snapshot.execution_id,
                approval_fence(store.as_ref(), &current),
                TaskId::new("finite-task").unwrap(),
                InterruptId::new(format!("original-private-interrupt-{name}")).unwrap(),
                json!({"question":"private startup approval"}),
                1,
                TransitionFacts { now_ms: 4 },
            )
            .unwrap();
        if matches!(mode, SeedMode::Approved) {
            let pending = service
                .pending(
                    &approval_context(),
                    &current.snapshot.execution_id,
                    approval_fence(store.as_ref(), &current),
                )
                .unwrap();
            current = service
                .accept(
                    &current.snapshot.execution_id,
                    approval_fence(store.as_ref(), &current),
                    ApprovalAcceptanceRequest {
                        context: approval_context(),
                        route: current.snapshot.definition.route.clone(),
                        definition_revision: current.snapshot.definition.revision.clone(),
                        turn_id: TurnId::new(format!("accepted-once-turn-{name}")).unwrap(),
                        now_ms: 4,
                        decisions: vec![GrantDecision {
                            interrupt_id: pending[0].interrupt_id.clone(),
                            decision: ApprovalDecision::Once,
                            cancelled: false,
                            token: Some(pending[0].token.clone()),
                        }],
                    },
                )
                .unwrap();
        }
    }
    if matches!(
        mode,
        SeedMode::Unsafe
            | SeedMode::Reconcilable
            | SeedMode::AttemptLimit
            | SeedMode::Cancelling
            | SeedMode::FailureDrain
            | SeedMode::Terminal
    ) {
        current = apply(
            store.as_ref(),
            &current,
            KernelCommand::Dispatch {
                task_id: TaskId::new("finite-task").unwrap(),
                attempt_id: AttemptId::new(format!("original-attempt-{name}")).unwrap(),
            },
        );
    }
    if matches!(mode, SeedMode::Cancelling) {
        current = store
            .cancel_run_fenced(
                &RunCancellationTarget::from_view(&store.thread_view(&thread_id).unwrap().unwrap())
                    .unwrap(),
                TransitionFacts { now_ms: 5 },
            )
            .unwrap();
        assert_eq!(current.snapshot.status, ExecutionStatus::CancelRequested);
    }
    if matches!(mode, SeedMode::FailureDrain) {
        current = apply(
            store.as_ref(),
            &current,
            KernelCommand::Dispatch {
                task_id: TaskId::new("sibling-task").unwrap(),
                attempt_id: AttemptId::new(format!("original-sibling-attempt-{name}")).unwrap(),
            },
        );
        current = apply(
            store.as_ref(),
            &current,
            KernelCommand::FailAttempt {
                task_id: TaskId::new("finite-task").unwrap(),
                attempt_id: AttemptId::new(format!("original-attempt-{name}")).unwrap(),
                failure: "fixture-known-failure".into(),
            },
        );
        assert_eq!(current.snapshot.status, ExecutionStatus::Running);
    }
    if matches!(mode, SeedMode::Terminal) {
        current = apply(
            store.as_ref(),
            &current,
            KernelCommand::Complete {
                task_id: TaskId::new("finite-task").unwrap(),
                attempt_id: AttemptId::new(format!("original-attempt-{name}")).unwrap(),
                result: TaskResult {
                    output: json!("old-terminal-value"),
                    writes: vec![],
                },
            },
        );
        assert_eq!(current.snapshot.status, ExecutionStatus::Completed);
    }
    store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap()
}
pub fn approval_fence(store: &impl Store, current: &StoredExecution) -> ApprovalFence {
    ApprovalFence {
        execution: ExecutionFence::from(&current.snapshot),
        resource_revision: store
            .thread(&current.thread_id)
            .unwrap()
            .unwrap()
            .resource_revision,
    }
}
pub fn reopen(store: Arc<SqliteStore>, directory: &Directory) -> Arc<SqliteStore> {
    let owner = store.owner_generation();
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    let reopened = Arc::new(SqliteStore::open(directory.path()).unwrap());
    assert_eq!(reopened.owner_generation(), owner + 1);
    reopened
}
pub fn target(store: &impl Store, view: &ApprovalView) -> RecoveryTarget {
    RecoveryTarget::from_view(&store.thread_view(&view.thread.id).unwrap().unwrap()).unwrap()
}
pub fn application_for(
    definition: Definition,
    callback: Arc<dyn NativeEffect>,
) -> Arc<NativeApplication> {
    let effects = definition
        .batches
        .iter()
        .flatten()
        .map(|task| {
            (
                task.effect_id.clone(),
                EffectBinding {
                    callback: callback.clone(),
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: principal().application,
                revision: "startup-native-fixture-1".into(),
            },
            vec![RouteRegistration {
                definition,
                effects,
                input_validator: None,
            }],
        )
        .unwrap(),
    )
}
pub fn config_for(application: Arc<NativeApplication>) -> NativeHostConfig {
    let prompts = application
        .definitions()
        .filter_map(|d| {
            let prompts: BTreeMap<_, _> = d
                .batches
                .iter()
                .flatten()
                .filter(|task| task.requires_approval)
                .map(|task| {
                    (
                        task.id.clone(),
                        json!({"question":"private startup approval"}),
                    )
                })
                .collect();
            (!prompts.is_empty()).then(|| (d.route.clone(), prompts))
        })
        .collect();
    NativeHostConfig::new(
        application,
        ApprovalHostConfig::new("startup-fixture-policy-1".into(), prompts),
    )
}

pub struct Callback {
    pub gate: Option<Arc<super::Gate>>,
    pub calls: AtomicUsize,
    pub returned: AtomicBool,
    pub contexts: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
}
impl Callback {
    pub fn new(gate: Option<Arc<super::Gate>>) -> Arc<Self> {
        Arc::new(Self {
            gate,
            calls: AtomicUsize::new(0),
            returned: AtomicBool::new(false),
            contexts: Mutex::new(Vec::new()),
        })
    }
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.contexts
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        if self.gate.as_ref().is_some_and(|g| !g.hold()) {
            return Err(EffectUncertain);
        }
        let result = Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("startup-finite-value"),
            writes: vec![StateWrite::Append {
                field: "actual-calls".into(),
                items: vec![json!("once")],
            }],
        }));
        self.returned.store(true, Ordering::Release);
        result
    }
}

pub struct CountingAuth(pub AtomicUsize);
impl Authenticator for CountingAuth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Auth.authenticate(headers)
    }
}
pub struct CountingPolicy {
    pub calls: AtomicUsize,
    pub allowed: AtomicBool,
    pub recover_gate: Option<Arc<super::Gate>>,
}
impl CountingPolicy {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            allowed: AtomicBool::new(true),
            recover_gate: None,
        })
    }
}
impl ResourcePolicy for CountingPolicy {
    fn authorize(&self, actor: &Principal, context: &ResourceContext) -> bool {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if matches!(context.operation, Operation::Recover)
            && self.recover_gate.as_ref().is_some_and(|g| !g.hold())
        {
            return false;
        }
        self.allowed.load(Ordering::Acquire) && actor == &principal()
    }
}

pub struct Resolver {
    pub calls: AtomicUsize,
    pub gate: Option<Arc<super::Gate>>,
    pub result: Result<Principal, StartupResolutionError>,
    pub drop_gate: Option<Arc<super::Gate>>,
    pub captured_store: Option<Arc<SqliteStore>>,
    pub panic_payload: bool,
}
impl Resolver {
    pub fn new(gate: Option<Arc<super::Gate>>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            gate,
            result: Ok(principal()),
            drop_gate: None,
            captured_store: None,
            panic_payload: false,
        })
    }
}
impl StartupPrincipalResolver for Resolver {
    fn resolve(&self, stored: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        assert_eq!(
            stored,
            &origin(),
            "trusted fixed lookup only accepts configured immutable origin"
        );
        if self.gate.as_ref().is_some_and(|g| !g.hold()) {
            return Err(StartupResolutionError::Unavailable);
        }
        if self.panic_payload {
            std::panic::panic_any(HeldPayload {
                gate: self.drop_gate.as_ref().unwrap().clone(),
                store: self.captured_store.as_ref().unwrap().clone(),
            });
        }
        self.result.clone()
    }
}
impl Drop for Resolver {
    fn drop(&mut self) {
        if !self.panic_payload {
            if let Some(gate) = &self.drop_gate {
                assert!(
                    gate.hold(),
                    "resolver destructor gate expired or cleanup released"
                );
            }
        }
    }
}
struct HeldPayload {
    gate: Arc<super::Gate>,
    store: Arc<SqliteStore>,
}
impl Drop for HeldPayload {
    fn drop(&mut self) {
        let _ = &self.store;
        assert!(
            self.gate.hold(),
            "opaque payload destructor gate expired or cleanup released"
        );
    }
}

pub async fn finished(handle: &StartupPreparationHandle) -> StartupPreparationOutcome {
    match tokio::time::timeout(WAIT, handle.wait(WAIT))
        .await
        .expect("preparation observer remained finite")
        .unwrap()
    {
        StartupWaitOutcome::Finished(outcome) => outcome,
        StartupWaitOutcome::Pending(status) => panic!("preparation still pending: {status:?}"),
    }
}
pub async fn completed(store: &SqliteStore, id: &ExecutionId) {
    tokio::time::timeout(WAIT, async {
        loop {
            if store.execution(id).unwrap().unwrap().snapshot.status == ExecutionStatus::Completed {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual finite callback committed completion");
}
pub fn assert_locked(directory: &Directory) {
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
}
pub fn retire_source(
    store: Arc<SqliteStore>,
    application: Arc<NativeApplication>,
    directory: &Directory,
) {
    let owner = store.owner_generation();
    let weak_store = Arc::downgrade(&store);
    let weak_application = Arc::downgrade(&application);
    drop(application);
    drop(store);
    assert!(weak_application.upgrade().is_none());
    assert!(weak_store.upgrade().is_none());
    let successor = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(successor.owner_generation(), owner + 1);
    drop(successor);
}

pub fn assert_recovered_park(
    before: &ApprovalView,
    after: &ApprovalView,
    adoption: &RecoveryAdoption,
) {
    let mut expected = before.clone();
    expected.thread.resource_revision += 1;
    expected.execution.snapshot.owner_generation = adoption.execution_fence.owner_generation;
    expected.execution.snapshot.revision += 1;
    expected.execution.snapshot.turn_id = adoption.turn_id.clone();
    expected
        .execution
        .snapshot
        .turn_history
        .push(adoption.turn_id.clone());
    // Wall clock is a trusted transition fact; all other fields are derived from BEFORE.
    expected.execution.snapshot.updated_at_ms = after.execution.snapshot.updated_at_ms;
    assert!(expected.execution.snapshot.updated_at_ms >= before.execution.snapshot.updated_at_ms);
    assert_eq!(&expected, after);
}
