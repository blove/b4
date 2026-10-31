use super::*;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    response::Response,
};
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Weak,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

pub type RawRows = Vec<(String, Vec<Vec<rusqlite::types::Value>>)>;

pub fn original_origin() -> ThreadOrigin {
    let p = principal();
    ThreadOrigin {
        application: p.application,
        tenant: p.tenant,
        principal: p.principal,
    }
}

pub fn seed_named(
    store: &SqliteStore,
    name: &str,
    selected: Definition,
    in_flight: bool,
) -> (ThreadView, ApprovalView) {
    let id = ThreadId::new(format!("history-{name}")).unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::from_iter([("fixture".into(), json!(name))]),
            origin: Some(original_origin()),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new(format!("history-execution-{name}")).unwrap(),
        TurnId::new(format!("history-turn-{name}")).unwrap(),
        store.owner_generation(),
        selected.clone(),
        Map::from_iter([
            ("input".into(), json!({"original":name})),
            ("retained".into(), json!([false, null, 0])),
        ]),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    let mut execution = store.admit(&id, initial).unwrap();
    if in_flight {
        for command in [
            KernelCommand::Start,
            KernelCommand::Dispatch {
                task_id: selected.batches[0][0].id.clone(),
                attempt_id: AttemptId::new(format!("history-attempt-{name}")).unwrap(),
            },
        ] {
            let plan =
                b4_kernel::transition(&execution.snapshot, command, TransitionFacts { now_ms: 3 })
                    .unwrap();
            execution = store
                .commit(
                    &execution.snapshot.execution_id,
                    ExecutionFence::from(&execution.snapshot),
                    plan,
                )
                .unwrap();
        }
        assert_eq!(execution.snapshot.status, ExecutionStatus::Running);
        assert_eq!(execution.snapshot.tasks[0].status, TaskStatus::InFlight);
    }
    let view = store.thread_view(&id).unwrap().unwrap();
    let approval = store
        .approval_view(&execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(approval.execution.snapshot.definition, selected);
    assert_eq!(
        approval.execution.snapshot.owner_generation,
        store.owner_generation()
    );
    assert!(approval.grants.is_empty() && approval.acceptances.is_empty());
    (view, approval)
}

pub struct Prior {
    pub directory: Directory,
    pub store: Arc<SqliteStore>,
    pub owner: u64,
    pub view: ThreadView,
    pub approval: ApprovalView,
    pub rows: RawRows,
}
impl Prior {
    pub fn new(name: &str, selected: Definition, in_flight: bool) -> Self {
        let directory = Directory::new();
        let first = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let owner = first.owner_generation();
        let (view, approval) = seed_named(first.as_ref(), name, selected, in_flight);
        let weak = Arc::downgrade(&first);
        drop(first);
        assert!(weak.upgrade().is_none());
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        assert_eq!(store.owner_generation(), owner + 1);
        assert_eq!(store.thread_view(&view.thread.id).unwrap().unwrap(), view);
        assert_eq!(
            store
                .approval_view(&approval.execution.snapshot.execution_id)
                .unwrap()
                .unwrap(),
            approval
        );
        store.readiness().unwrap();
        let rows = directory.rows();
        let owner = store.owner_generation();
        Self {
            directory,
            store,
            owner,
            view,
            approval,
            rows,
        }
    }
    pub fn target(&self) -> RecoveryTarget {
        RecoveryTarget::from_view(&self.view).unwrap()
    }
    pub fn current(&self) -> ThreadView {
        self.store
            .thread_view(&self.view.thread.id)
            .unwrap()
            .unwrap()
    }
    pub fn authority(&self) -> ApprovalView {
        self.store
            .approval_view(&self.approval.execution.snapshot.execution_id)
            .unwrap()
            .unwrap()
    }
    pub fn unchanged(&self) {
        assert_eq!(self.current(), self.view);
        assert_eq!(self.authority(), self.approval);
        assert_eq!(self.directory.rows(), self.rows);
    }
}

pub struct RecordingEffect {
    pub calls: Calls,
    pub version: &'static str,
    pub gate: Option<Arc<Gate>>,
    pub void: bool,
    pub _capture: Option<Arc<SqliteStore>>,
}
impl NativeEffect for RecordingEffect {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        if self.gate.as_ref().is_some_and(|g| !g.hold()) {
            return Err(EffectUncertain);
        }
        let mut result = result(self.version);
        if self.void {
            result.output = Value::Null;
        }
        Ok(NativeOutcome::Succeeded(result))
    }
}
pub struct RecordingValidator {
    pub calls: Arc<AtomicUsize>,
    pub reject: bool,
    pub _capture: Option<Arc<SqliteStore>>,
}
impl InputValidator for RecordingValidator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject {
            Err(InvalidApplicationInput)
        } else {
            Ok(())
        }
    }
}

pub fn bound(
    selected: Definition,
    version: &'static str,
    calls: Calls,
    gate: Option<Arc<Gate>>,
    void: bool,
    capture: Option<Arc<SqliteStore>>,
    validator: Option<Arc<dyn InputValidator>>,
) -> RouteRegistration {
    RouteRegistration {
        definition: selected,
        effects: BTreeMap::from([(
            EffectId::new("shared-effect").unwrap(),
            EffectBinding {
                callback: Arc::new(RecordingEffect {
                    calls,
                    version,
                    gate,
                    void,
                    _capture: capture,
                }),
                output_kind: if void {
                    OutputKind::Void
                } else {
                    OutputKind::Value
                },
            },
        )]),
        input_validator: validator,
    }
}

pub fn assembly(
    current: RouteRegistration,
    history: Vec<RouteRegistration>,
) -> Arc<NativeApplication> {
    Arc::new(
        NativeApplication::new_with_history(
            ApplicationIdentity {
                application: principal().application,
                revision: "assembly-v2".into(),
            },
            vec![current],
            history,
        )
        .unwrap(),
    )
}

pub fn configuration(application: Arc<NativeApplication>, ttl_ms: u64) -> NativeHostConfig {
    let prompts = application
        .definitions()
        .filter_map(|d| {
            let tasks: BTreeMap<_, _> = d
                .batches
                .iter()
                .flatten()
                .filter(|task| task.requires_approval)
                .map(|task| {
                    (
                        task.id.clone(),
                        json!({"question":"allow current-v2 fixture?"}),
                    )
                })
                .collect();
            (!tasks.is_empty()).then(|| (d.route.clone(), tasks))
        })
        .collect();
    let mut approval = ApprovalHostConfig::new("history-current-policy".into(), prompts);
    approval.ttl_ms = ttl_ms;
    assert_eq!(
        approval.service_config,
        ApprovalServiceConfig::RequiredDurable
    );
    NativeHostConfig::new(application, approval)
}

pub fn host(
    store: Arc<SqliteStore>,
    application: Arc<NativeApplication>,
    policy: Arc<dyn ResourcePolicy>,
) -> HttpHost<SqliteStore> {
    HttpHost::new_with_application(
        store,
        Arc::new(Auth),
        policy,
        configuration(application, 300_000),
    )
    .unwrap()
}

pub async fn request(
    host: &HttpHost<SqliteStore>,
    method: &str,
    path: &str,
    payload: Option<Value>,
    authenticated: bool,
) -> Response {
    let mut request = Request::builder().method(method).uri(path);
    if authenticated {
        request = request.header("authorization", "Bearer native-history-fixture");
    }
    let bytes = payload.map(|p| p.to_string()).unwrap_or_default();
    let request = request
        .header("content-type", "application/json")
        .body(Body::from(bytes))
        .unwrap();
    tokio::time::timeout(WAIT, host.router().oneshot(request))
        .await
        .expect("actual router request remained finite")
        .unwrap()
}

pub async fn wire(response: Response) -> (StatusCode, String) {
    let status = response.status();
    let bytes = tokio::time::timeout(WAIT, to_bytes(response.into_body(), 65_536))
        .await
        .expect("actual finite response body remained bounded")
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}
pub async fn json_body(response: Response) -> (StatusCode, Value) {
    let (status, body) = wire(response).await;
    (status, serde_json::from_str(&body).unwrap())
}
pub async fn first_frame(body: &mut axum::body::BodyDataStream) -> String {
    tokio::time::timeout(WAIT, async {
        let mut bytes = Vec::new();
        loop {
            let mut pinned = std::pin::Pin::new(&mut *body);
            let data =
                std::future::poll_fn(|cx| futures_core::Stream::poll_next(pinned.as_mut(), cx))
                    .await
                    .unwrap()
                    .unwrap();
            {
                bytes.extend_from_slice(&data);
                assert!(bytes.len() <= 65_536);
                if bytes.windows(2).any(|x| x == b"\n\n") {
                    return String::from_utf8(bytes).unwrap();
                }
            }
        }
    })
    .await
    .expect("live attachment first real frame remained finite")
}

pub async fn completed(store: &SqliteStore, id: &ExecutionId) -> StoredExecution {
    tokio::time::timeout(WAIT, async {
        loop {
            let view = store.execution(id).unwrap().unwrap();
            if view.snapshot.status == ExecutionStatus::Completed {
                return view;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual finite callback committed completion")
}

pub async fn retire(
    host: HttpHost<SqliteStore>,
    application: Arc<NativeApplication>,
    store: Arc<SqliteStore>,
    directory: &Directory,
) -> ShutdownHandle {
    let shutdown = host.shutdown_handle();
    let owner = store.owner_generation();
    let weak_store = Arc::downgrade(&store);
    let weak_app = Arc::downgrade(&application);
    quiesce(&shutdown).await;
    drop(host);
    drop(application);
    drop(store);
    assert!(weak_store.upgrade().is_none());
    assert!(weak_app.upgrade().is_none());
    let reopened = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(reopened.owner_generation(), owner + 1);
    reopened.readiness().unwrap();
    drop(reopened);
    shutdown
}

pub fn calls() -> Calls {
    Arc::new(Mutex::new(Vec::new()))
}
pub fn assert_calls(
    calls: &Calls,
    version: &str,
    owner: u64,
    definition: &Definition,
) -> EffectInvocation {
    let all = calls.lock().unwrap();
    assert_eq!(all.len(), 1);
    let (call, context) = &all[0];
    assert_eq!(call.owner_generation, owner);
    assert_eq!(call.task_id, definition.batches[0][0].id);
    assert_eq!(call.effect_id, definition.batches[0][0].effect_id);
    assert_eq!(context.application_revision, "assembly-v2");
    assert_eq!(
        context.execution_revision,
        ExecutionRevision::new(version).unwrap()
    );
    assert_eq!(context.origin, original_origin());
    assert_eq!(context.policy_revision, "history-current-policy");
    call.clone()
}

pub struct FixedResolver {
    pub origin: ThreadOrigin,
    pub actor: Principal,
}
impl FixedResolver {
    pub fn trusted() -> Arc<Self> {
        Arc::new(Self {
            origin: original_origin(),
            actor: principal(),
        })
    }
}
impl StartupPrincipalResolver for FixedResolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        // Match a separately installed finite actor table; never derive actor
        // identity from thread metadata or copy arbitrary stored strings.
        if origin == &self.origin {
            Ok(self.actor.clone())
        } else {
            Err(StartupResolutionError::Unauthorized)
        }
    }
}

pub struct PolicyProbe {
    pub contexts: Arc<Mutex<Vec<ResourceContext>>>,
    pub allowed: Arc<AtomicBool>,
    pub gate: Option<Arc<Gate>>,
    pub operation: Operation,
    pub adopted_source: Option<Arc<SqliteStore>>,
}
impl ResourcePolicy for PolicyProbe {
    fn authorize(&self, actor: &Principal, context: &ResourceContext) -> bool {
        self.contexts.lock().unwrap().push(context.clone());
        let reached = context.operation == self.operation
            && self.adopted_source.as_ref().is_none_or(|store| {
                context
                    .thread_id
                    .as_ref()
                    .and_then(|id| store.thread_view(id).unwrap())
                    .and_then(|v| v.execution)
                    .is_some_and(|e| e.snapshot.owner_generation == store.owner_generation())
            });
        if reached && self.gate.as_ref().is_some_and(|gate| !gate.hold()) {
            return false;
        }
        actor == &principal() && self.allowed.load(Ordering::SeqCst)
    }
}

pub fn policy_probe(
    operation: Operation,
    gate: Option<Arc<Gate>>,
    adopted_source: Option<Arc<SqliteStore>>,
) -> Arc<PolicyProbe> {
    Arc::new(PolicyProbe {
        contexts: Arc::new(Mutex::new(Vec::new())),
        allowed: Arc::new(AtomicBool::new(true)),
        gate,
        operation,
        adopted_source,
    })
}
pub fn assert_resume_v1(contexts: &Arc<Mutex<Vec<ResourceContext>>>) {
    let all = contexts.lock().unwrap();
    let resume = all
        .iter()
        .find(|c| c.operation == Operation::Resume)
        .expect("actual Resume policy entry");
    assert_eq!(resume.origin.as_ref(), Some(&original_origin()));
    for selected in [&resume.stored_route, &resume.requested_route] {
        let selected = selected.as_ref().unwrap();
        assert_eq!(selected.route, definition("v1").route);
        assert_eq!(selected.revision, ExecutionRevision::new("v1").unwrap());
    }
}
pub async fn expire_at(expires_at_ms: u64) {
    tokio::time::timeout(WAIT, async {
        while SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            < u128::from(expires_at_ms)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual persisted grant expiration remained finite");
}
pub fn weak_absent(store: &Weak<SqliteStore>, application: &Weak<NativeApplication>) {
    assert!(store.upgrade().is_none());
    assert!(application.upgrade().is_none());
}

pub async fn idle(host: &HttpHost<SqliteStore>) {
    let shutdown = host.shutdown_handle();
    tokio::time::timeout(WAIT, async {
        while shutdown.status().active_owners != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordinary producer really retired before admitting a successor");
}
pub struct ExactCurrentInput {
    pub calls: Arc<AtomicUsize>,
}
impl InputValidator for ExactCurrentInput {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if input == &json!({"current":true}) {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}
pub async fn preparation_complete(handle: &StartupPreparationHandle) -> StartupProgress {
    let wait = tokio::time::timeout(WAIT, handle.wait(WAIT))
        .await
        .expect("startup observation remained finite")
        .unwrap();
    match wait {
        StartupWaitOutcome::Finished(StartupPreparationOutcome::Complete { progress }) => progress,
        other => panic!("expected actual completed startup observation, got {other:?}"),
    }
}
