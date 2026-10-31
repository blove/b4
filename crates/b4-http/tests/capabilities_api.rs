use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        let application = match headers.get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer owner") => "app",
            Some("Bearer foreign") => "foreign",
            _ => return Err(AuthenticationError),
        };
        Ok(Principal {
            application: application.into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
    }
}
#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    wake: Condvar,
}
impl Gate {
    fn hold(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.wake.notify_all();
        while !state.1 {
            let (next, timeout) = self
                .wake
                .wait_timeout(state, Duration::from_secs(8))
                .unwrap();
            state = next;
            assert!(!timeout.timed_out(), "capability_policy_gate_timeout");
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.state.lock().unwrap().0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.wake.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[derive(Default)]
struct Policy {
    calls: Mutex<Vec<ResourceContext>>,
    deny: AtomicBool,
    capability_gate: Option<Arc<Gate>>,
    capability_panic: AtomicBool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        self.calls.lock().unwrap().push(context.clone());
        if context.operation == Operation::ReadCapabilities {
            if let Some(gate) = &self.capability_gate {
                gate.hold();
            }
            assert!(
                !self.capability_panic.load(Ordering::Acquire),
                "capability_policy_fixture_panic"
            );
        }
        !self.deny.load(Ordering::Acquire)
    }
}
#[derive(Default)]
struct ObservedStore {
    inner: MemoryStore,
    readiness: AtomicUsize,
    records: AtomicUsize,
    mutations: AtomicUsize,
    sensitive: AtomicUsize,
    declarations: AtomicUsize,
    unavailable: AtomicBool,
    readiness_gate: Mutex<Option<Arc<Gate>>>,
    declaration_gate: Mutex<Option<Arc<Gate>>>,
}
impl ObservedStore {
    fn reset(&self) {
        for count in [
            &self.readiness,
            &self.records,
            &self.mutations,
            &self.sensitive,
            &self.declarations,
        ] {
            count.store(0, Ordering::Release);
        }
    }
    fn no_records_or_mutations(&self) {
        assert_eq!(self.records.load(Ordering::Acquire), 0);
        assert_eq!(self.mutations.load(Ordering::Acquire), 0);
        assert_eq!(self.sensitive.load(Ordering::Acquire), 0);
    }
}
impl Store for ObservedStore {
    // Intentionally retain the conservative custom declaration despite real ports.
    fn capabilities(&self) -> StoreCapabilities {
        self.declarations.fetch_add(1, Ordering::Relaxed);
        let gate = self.declaration_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.hold();
        }
        StoreCapabilities::default()
    }
    fn owner_generation(&self) -> u64 {
        self.records.fetch_add(1, Ordering::Relaxed);
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.records.fetch_add(1, Ordering::Relaxed);
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.records.fetch_add(1, Ordering::Relaxed);
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.records.fetch_add(1, Ordering::Relaxed);
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.inner.admit_fenced(id, generation, revision, snapshot)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.records.fetch_add(1, Ordering::Relaxed);
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.readiness.fetch_add(1, Ordering::Relaxed);
        let gate = self.readiness_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.hold();
        }
        if self.unavailable.load(Ordering::Acquire) {
            Err(StoreError::Unavailable("private_database_path".into()))
        } else {
            self.inner.readiness()
        }
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.inner.cancel_run_fenced(target, facts)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_thread_fenced(target)
    }
}
impl ApprovalStore for ObservedStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.sensitive.fetch_add(1, Ordering::Relaxed);
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
fn definition() -> Definition {
    Definition {
        route: RouteKey::new("/private-inventory#workflow").unwrap(),
        revision: ExecutionRevision::new("private-program-revision").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("private-task").unwrap(),
            effect_id: EffectId::new("private-effect").unwrap(),
            requires_approval: false,
            recovery: RecoveryPolicy::Pure,
        }]],
        max_attempts: 1,
    }
}
fn script_config() -> HostConfig {
    HostConfig::new(vec![ScriptRoute {
        definition: definition(),
        effects: BTreeMap::from([(EffectId::new("private-effect").unwrap(), ScriptEffect::Void)]),
    }])
}
struct Callback {
    calls: Arc<AtomicUsize>,
    uncertain: bool,
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.uncertain {
            Err(EffectUncertain)
        } else {
            Ok(NativeOutcome::Succeeded(TaskResult {
                output: Value::Null,
                writes: vec![],
            }))
        }
    }
}
struct Validator(Arc<AtomicUsize>);
impl InputValidator for Validator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
fn native_config(
    calls: Arc<AtomicUsize>,
    validators: Arc<AtomicUsize>,
    uncertain: bool,
    mode: ApprovalServiceConfig,
) -> NativeHostConfig {
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "app".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                definition: definition(),
                effects: BTreeMap::from([(
                    EffectId::new("private-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(Callback { calls, uncertain }),
                        output_kind: OutputKind::Void,
                    },
                )]),
                input_validator: Some(Arc::new(Validator(validators))),
            }],
        )
        .unwrap(),
    );
    let mut approval = ApprovalHostConfig::new("private-policy-revision".into(), BTreeMap::new());
    approval.service_config = mode;
    approval.ttl_ms = 1234;
    NativeHostConfig::new(application, approval)
}
async fn request<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    payload: Option<Value>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(authorization) = authorization {
        builder = builder.header("authorization", authorization);
    }
    host.router()
        .oneshot(
            builder
                .body(payload.map_or_else(Body::empty, |v| Body::from(v.to_string())))
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn descriptor<S: Store + 'static>(host: &HttpHost<S>) -> Value {
    let response = request(host, "GET", "/b4/capabilities", Some("Bearer owner"), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["content-type"], "application/json");
    let body = to_bytes(response.into_body(), 65_536).await.unwrap();
    serde_json::from_slice::<CapabilityDescriptorV2>(&body)
        .unwrap()
        .validate()
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    for private in [
        "private-inventory",
        "private-program-revision",
        "private-task",
        "private-effect",
        "private-policy-revision",
        "private_database_path",
        "Bearer owner",
    ] {
        assert!(
            !std::str::from_utf8(&body).unwrap().contains(private),
            "private_capability_content"
        );
    }
    value
}
fn route_free(context: &ResourceContext) {
    assert_eq!(context.operation, Operation::ReadCapabilities);
    assert!(context.thread_id.is_none());
    assert!(context.origin.is_none());
    assert!(context.stored_route.is_none());
    assert!(context.requested_route.is_none());
}

#[tokio::test]
async fn scripted_memory_descriptor_is_honest_and_uses_fixed_response_bound() {
    let store = Arc::new(MemoryStore::new());
    let policy = Arc::new(Policy::default());
    let mut config = script_config();
    config.max_active_runs = 3;
    config.max_concurrent_tasks = 5;
    config.max_tasks_per_execution = 4;
    config.viewer_queue_frames = 7;
    config.heartbeat_ms = 17;
    config.max_input_bytes = 123;
    config.max_output_bytes = 1;
    config.max_attached_viewers = 5;
    config.turn_buffer_bytes = 12345;
    config.turn_buffer_frames = 333;
    config.viewer_queue_bytes = 23456;
    config.attachment_snapshot_bytes = 34567;
    let host = HttpHost::new(store, Arc::new(Auth), policy.clone(), config).unwrap();
    let value = descriptor(&host).await;
    assert_eq!(value["descriptor_version"], 2);
    assert_eq!(value["target_profile"], "b4-ap-core/0");
    assert_eq!(value["profile_status"], "native_subset");
    assert_eq!(value["core_profile_frozen"], false);
    assert_eq!(value["runtime_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(value["application"], Value::Null);
    assert_eq!(value["execution"]["native_callbacks"], false);
    assert_eq!(value["execution"]["approval_service_mode"], "disabled");
    assert_eq!(
        value["store"],
        json!({"kind":"memory","durable_runtime":false,"durable_approvals":null})
    );
    assert_eq!(value["features"]["live_attach"], true);
    assert_eq!(value["features"]["supervised_admission_drain"], true);
    assert_eq!(value["features"]["required_once_deny"], false);
    assert_eq!(value["features"]["trusted_known_id_recovery"], false);
    assert_eq!(value["features"]["fenced_cancellation"], true);
    assert_eq!(value["features"]["fenced_deletion"], true);
    for flag in [
        "automatic_recovery_scan",
        "always_permissions",
        "durable_event_replay",
        "native_live_chunks",
        "built_in_model_providers",
        "memory_candidates",
        "managed_workspaces",
        "ag_ui",
        "dynamic_graphs",
        "graceful_shutdown",
        "turn_deadline",
        "native_callback_sandbox",
    ] {
        assert_eq!(value["features"][flag], false);
    }
    assert_eq!(
        value["readiness"],
        json!({"response":"status_only","aggregation":"store_readiness_and_no_owned_worker_fault_and_serving_admission","checks":["store_readiness","owned_worker_fault","serving_admission"]})
    );
    for (key, expected) in [
        ("body_bytes", 65536),
        ("json_depth", 64),
        ("active_runs", 3),
        ("concurrent_tasks", 5),
        ("concurrent_tasks_per_execution", 4),
        ("viewer_queue_frames", 7),
        ("heartbeat_ms", 17),
        ("input_bytes", 123),
        ("output_bytes", 1),
        ("max_attached_viewers", 5),
        ("turn_buffer_bytes", 12345),
        ("turn_buffer_frames", 333),
        ("viewer_queue_bytes", 23456),
        ("attachment_snapshot_bytes", 34567),
        ("program_routes", 64),
        ("program_tasks", 64),
        ("program_attempts", 64),
        ("completion_attempts", 8),
        ("task_failure_bytes", 1024),
        ("script_chunks", 64),
        ("script_delay_ms", 5000),
        ("approval_bytes", 65536),
    ] {
        assert_eq!(value["limits"][key], expected);
    }
    assert_eq!(value["limits"]["approval_ttl_ms"], Value::Null);
    for key in [
        "callback_timeout_ms",
        "validator_timeout_ms",
        "turn_deadline_ms",
        "max_threads",
        "max_history_bytes",
        "parked_retention_ms",
        "connection_limit",
        "whole_process_memory_bytes",
    ] {
        assert_eq!(value["controls"][key], Value::Null);
    }
    assert_eq!(
        value["controls"]["shutdown_default_wait_ms"],
        DEFAULT_SHUTDOWN_WAIT.as_millis() as u64
    );
    assert_eq!(
        value["controls"]["shutdown_max_wait_ms"],
        MAX_SHUTDOWN_WAIT.as_millis() as u64
    );
    let calls = policy.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    route_free(&calls[0]);
}

#[tokio::test]
async fn native_volatile_memory_reports_identity_and_installed_bridge_without_callbacks() {
    let store = Arc::new(ObservedStore::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let validators = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        native_config(
            calls.clone(),
            validators.clone(),
            false,
            ApprovalServiceConfig::VolatileFixture,
        ),
    )
    .unwrap();
    store.reset();
    let value = descriptor(&host).await;
    assert_eq!(
        value["application"],
        json!({"id":"app","revision":"app-v1"})
    );
    assert_eq!(value["execution"]["native_callbacks"], true);
    assert_eq!(
        value["execution"]["approval_service_mode"],
        "volatile_fixture"
    );
    assert_eq!(value["features"]["required_once_deny"], true);
    assert_eq!(value["limits"]["approval_ttl_ms"], 1234);
    assert_eq!(
        value["store"],
        json!({"kind":"unspecified","durable_runtime":false,"durable_approvals":false})
    );
    assert_eq!(value["features"]["fenced_cancellation"], false);
    assert_eq!(value["features"]["fenced_deletion"], false);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(validators.load(Ordering::Acquire), 0);
    store.no_records_or_mutations();
    assert_eq!(store.readiness.load(Ordering::Acquire), 1);
}

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-capability-api-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[tokio::test]
async fn physical_sqlite_approvals_are_unknown_without_port_and_independent_of_mode() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let plain = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        script_config(),
    )
    .unwrap();
    assert_eq!(
        descriptor(&plain).await["store"],
        json!({"kind":"sqlite","durable_runtime":true,"durable_approvals":null})
    );
    for (mode, label) in [
        (ApprovalServiceConfig::RequiredDurable, "durable_required"),
        (ApprovalServiceConfig::VolatileFixture, "volatile_fixture"),
    ] {
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy::default()),
            native_config(
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                false,
                mode,
            ),
        )
        .unwrap();
        let value = descriptor(&host).await;
        assert_eq!(
            value["store"],
            json!({"kind":"sqlite","durable_runtime":true,"durable_approvals":true})
        );
        assert_eq!(value["execution"]["approval_service_mode"], label);
        assert_eq!(value["features"]["required_once_deny"], true);
        let mut approval =
            ApprovalHostConfig::new("private-policy-revision".into(), BTreeMap::new());
        approval.service_config = mode;
        let legacy = HttpHost::new_with_approvals(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy::default()),
            script_config(),
            approval,
        )
        .unwrap();
        let legacy_value = descriptor(&legacy).await;
        assert_eq!(legacy_value["application"], Value::Null);
        assert_eq!(legacy_value["execution"]["native_callbacks"], false);
        assert_eq!(legacy_value["execution"]["approval_service_mode"], label);
        assert_eq!(legacy_value["store"]["durable_approvals"], true);
        assert_eq!(legacy_value["features"]["required_once_deny"], true);
        assert_eq!(legacy_value["features"]["trusted_known_id_recovery"], true);
    }
}

#[tokio::test]
async fn custom_backend_defaults_are_unadvertised_even_with_real_optional_ports() {
    let store = Arc::new(ObservedStore::default());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        script_config(),
    )
    .unwrap();
    store.reset();
    let value = descriptor(&host).await;
    assert_eq!(
        value["store"],
        json!({"kind":"unspecified","durable_runtime":false,"durable_approvals":null})
    );
    assert_eq!(value["features"]["fenced_cancellation"], false);
    assert_eq!(value["features"]["fenced_deletion"], false);
    store.no_records_or_mutations();
}

#[tokio::test]
async fn authentication_scope_and_policy_precede_private_backend_diagnostics() {
    let store = Arc::new(ObservedStore::default());
    let policy = Arc::new(Policy::default());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        native_config(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            false,
            ApprovalServiceConfig::VolatileFixture,
        ),
    )
    .unwrap();
    store.reset();
    store.unavailable.store(true, Ordering::Release);
    for (token, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some("Bearer foreign"), StatusCode::NOT_FOUND),
    ] {
        let response = request(&host, "GET", "/b4/capabilities", token, None).await;
        assert_eq!(response.status(), status);
    }
    assert!(policy.calls.lock().unwrap().is_empty());
    assert_eq!(store.readiness.load(Ordering::Acquire), 0);
    policy.deny.store(true, Ordering::Release);
    let response = request(&host, "GET", "/b4/capabilities", Some("Bearer owner"), None).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(store.readiness.load(Ordering::Acquire), 0);
    store.no_records_or_mutations();
    let calls = policy.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    route_free(&calls[0]);
}

#[tokio::test]
async fn unavailable_store_returns_static_503_after_authorization() {
    let store = Arc::new(ObservedStore::default());
    let policy = Arc::new(Policy::default());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        script_config(),
    )
    .unwrap();
    store.reset();
    store.unavailable.store(true, Ordering::Release);
    let response = request(&host, "GET", "/b4/capabilities", Some("Bearer owner"), None).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), 65_536).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["details"]["code"],
        "host_unready"
    );
    assert!(!std::str::from_utf8(&body)
        .unwrap()
        .contains("private_database_path"));
    assert_eq!(store.readiness.load(Ordering::Acquire), 1);
    store.no_records_or_mutations();
    route_free(&policy.calls.lock().unwrap()[0]);
}

#[tokio::test]
async fn held_discovery_policy_rechecks_real_owned_worker_fault_before_metadata() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let policy = Arc::new(Policy {
        capability_gate: Some(gate.clone()),
        ..Policy::default()
    });
    let store = Arc::new(ObservedStore::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        policy,
        native_config(
            calls.clone(),
            Arc::new(AtomicUsize::new(0)),
            true,
            ApprovalServiceConfig::VolatileFixture,
        ),
    )
    .unwrap();
    let pending_host = host.clone();
    let pending = tokio::spawn(async move {
        request(
            &pending_host,
            "GET",
            "/b4/capabilities",
            Some("Bearer owner"),
            None,
        )
        .await
    });
    gate.entered().await;
    let fault = request(
        &host,
        "POST",
        "/threads/fault/runs/wait",
        Some("Bearer owner"),
        Some(json!({"route":"/private-inventory#workflow","input":null})),
    )
    .await;
    assert_eq!(fault.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    store.reset();
    gate.release();
    let response = pending.await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), 65_536).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["details"]["code"],
        "host_unready"
    );
    store.no_records_or_mutations();
}

async fn static_error(response: axum::response::Response, code: &str) {
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), 65_536).await.unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["error"]["details"]["code"], code);
    assert!(value.get("descriptor_version").is_none());
    assert!(!std::str::from_utf8(&body).unwrap().contains("private_"));
}

fn capability_request<S: Store + 'static>(
    host: &HttpHost<S>,
) -> tokio::task::JoinHandle<axum::response::Response> {
    let owned = host.clone();
    tokio::spawn(async move {
        request(
            &owned,
            "GET",
            "/b4/capabilities",
            Some("Bearer owner"),
            None,
        )
        .await
    })
}

#[tokio::test]
async fn closed_capabilities_refuse_before_policy_and_store_without_latching_fault() {
    let store = Arc::new(ObservedStore::default());
    let policy = Arc::new(Policy::default());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        script_config(),
    )
    .unwrap();
    store.reset();
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    for token in [None, Some("Bearer foreign"), Some("Bearer owner")] {
        static_error(
            request(&host, "GET", "/b4/capabilities", token, None).await,
            "host_draining",
        )
        .await;
    }
    assert!(policy.calls.lock().unwrap().is_empty());
    assert_eq!(store.readiness.load(Ordering::Acquire), 0);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    store.no_records_or_mutations();
    assert_eq!(
        shutdown.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
}

#[tokio::test]
async fn held_capability_policy_loses_admission_cut_before_readiness_or_metadata() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let policy = Arc::new(Policy {
        capability_gate: Some(gate.clone()),
        ..Policy::default()
    });
    let store = Arc::new(ObservedStore::default());
    let host = HttpHost::new(store.clone(), Arc::new(Auth), policy, script_config()).unwrap();
    store.reset();
    let pending = capability_request(&host);
    gate.entered().await;
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().active_owners, 1);
    assert_eq!(
        shutdown.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    static_error(pending.await.unwrap(), "host_draining").await;
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(store.readiness.load(Ordering::Acquire), 0);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    store.no_records_or_mutations();
}

#[tokio::test]
async fn abandoned_capability_policy_keeps_owner_until_real_return_after_cut() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let policy = Arc::new(Policy {
        capability_gate: Some(gate.clone()),
        ..Policy::default()
    });
    let store = Arc::new(ObservedStore::default());
    let host = HttpHost::new(store.clone(), Arc::new(Auth), policy, script_config()).unwrap();
    store.reset();
    let pending = capability_request(&host);
    gate.entered().await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().active_owners, 1);
    assert_eq!(
        shutdown.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(store.readiness.load(Ordering::Acquire), 0);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    store.no_records_or_mutations();
}

#[tokio::test]
async fn abandoned_capability_readiness_error_faults_inside_owned_job() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ObservedStore::default());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        script_config(),
    )
    .unwrap();
    store.reset();
    *store.readiness_gate.lock().unwrap() = Some(gate.clone());
    let pending = capability_request(&host);
    gate.entered().await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().active_owners, 1);
    assert!(!shutdown.status().faulted);
    store.unavailable.store(true, Ordering::Release);
    gate.release();
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: true }
    );
    assert_eq!(store.readiness.load(Ordering::Acquire), 1);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    store.no_records_or_mutations();
}

#[tokio::test]
async fn abandoned_capability_policy_panic_faults_before_owner_retirement() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let policy = Arc::new(Policy {
        capability_gate: Some(gate.clone()),
        capability_panic: AtomicBool::new(true),
        ..Policy::default()
    });
    let store = Arc::new(ObservedStore::default());
    let host = HttpHost::new(store.clone(), Arc::new(Auth), policy, script_config()).unwrap();
    store.reset();
    let pending = capability_request(&host);
    gate.entered().await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().active_owners, 1);
    gate.release();
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: true }
    );
    assert_eq!(store.readiness.load(Ordering::Acquire), 0);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    store.no_records_or_mutations();
}

#[tokio::test]
async fn capability_metadata_response_rechecks_cut_after_backend_declaration() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ObservedStore::default());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        script_config(),
    )
    .unwrap();
    store.reset();
    *store.declaration_gate.lock().unwrap() = Some(gate.clone());
    let pending = capability_request(&host);
    gate.entered().await;
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().active_owners, 1);
    gate.release();
    static_error(pending.await.unwrap(), "host_draining").await;
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(store.readiness.load(Ordering::Acquire), 1);
    assert_eq!(store.declarations.load(Ordering::Acquire), 1);
    store.no_records_or_mutations();
}
