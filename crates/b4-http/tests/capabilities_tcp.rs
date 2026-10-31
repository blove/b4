use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

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
            assert!(!timeout.timed_out(), "capability_tcp_policy_timeout");
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
    calls: AtomicUsize,
    deny: AtomicBool,
    gate: Option<Arc<Gate>>,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::ReadCapabilities {
            assert!(
                context.thread_id.is_none()
                    && context.origin.is_none()
                    && context.stored_route.is_none()
                    && context.requested_route.is_none()
            );
            self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some(gate) = &self.gate {
                gate.hold();
            }
        }
        !self.deny.load(Ordering::Acquire)
    }
}
#[derive(Default)]
struct Probe {
    inner: MemoryStore,
    health: AtomicUsize,
    declarations: AtomicUsize,
    records: AtomicUsize,
    sensitive: AtomicUsize,
    writes: AtomicUsize,
    unavailable: AtomicBool,
}
impl Probe {
    fn reset(&self) {
        for count in [
            &self.health,
            &self.declarations,
            &self.records,
            &self.sensitive,
            &self.writes,
        ] {
            count.store(0, Ordering::Release);
        }
    }
    fn no_private_reads_or_writes(&self) {
        assert_eq!(self.records.load(Ordering::Acquire), 0);
        assert_eq!(self.sensitive.load(Ordering::Acquire), 0);
        assert_eq!(self.writes.load(Ordering::Acquire), 0);
    }
}
impl Store for Probe {
    fn capabilities(&self) -> StoreCapabilities {
        self.declarations.fetch_add(1, Ordering::Relaxed);
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.records.fetch_add(1, Ordering::Relaxed);
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
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
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.admit_fenced(id, generation, revision, snapshot)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.records.fetch_add(1, Ordering::Relaxed);
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.health.fetch_add(1, Ordering::Relaxed);
        if self.unavailable.load(Ordering::Acquire) {
            Err(StoreError::Unavailable("private_sql_location".into()))
        } else {
            self.inner.readiness()
        }
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.cancel_run_fenced(target, facts)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_thread_fenced(target)
    }
}
impl ApprovalStore for Probe {
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
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.commit_approval(id, fence, plan, mutation)
    }
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
fn native_config(calls: Arc<AtomicUsize>, uncertain: bool) -> NativeHostConfig {
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "app".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                definition: Definition {
                    route: RouteKey::new("/secret-native#workflow").unwrap(),
                    revision: ExecutionRevision::new("secret-definition").unwrap(),
                    batches: vec![vec![TaskSpec {
                        id: TaskId::new("secret-task").unwrap(),
                        effect_id: EffectId::new("secret-effect").unwrap(),
                        requires_approval: false,
                        recovery: RecoveryPolicy::Pure,
                    }]],
                    max_attempts: 1,
                },
                effects: BTreeMap::from([(
                    EffectId::new("secret-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(Callback { calls, uncertain }),
                        output_kind: OutputKind::Void,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let mut approval = ApprovalHostConfig::new("secret-policy".into(), BTreeMap::new());
    approval.service_config = ApprovalServiceConfig::VolatileFixture;
    let mut config = NativeHostConfig::new(application, approval);
    config.limits.max_output_bytes = 64;
    config
}
struct Server {
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve<S: Store + 'static>(host: &HttpHost<S>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("capability_transport_requires_loopback");
    let address = listener.local_addr().unwrap();
    let router = host.router();
    Server {
        address,
        task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    }
}
struct Response {
    status: u16,
    headers: String,
    body: Vec<u8>,
}
async fn exchange(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> Response {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let authorization = token
        .map(|token| format!("Authorization: {token}\r\n"))
        .unwrap_or_default();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n{authorization}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    let mut bytes = vec![];
    tokio::time::timeout(Duration::from_secs(8), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(
        bytes.len() <= 65_536 + 8192,
        "unbounded_capability_http_response"
    );
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(&bytes[..split])
        .unwrap()
        .to_ascii_lowercase();
    assert!(
        !headers.contains("transfer-encoding: chunked"),
        "bounded_json_response_expected"
    );
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = bytes[split + 4..].to_vec();
    assert!(body.len() <= 65_536);
    assert!(headers.contains("cache-control: no-store\r\n"));
    assert!(headers.contains("content-type: application/json\r\n"));
    Response {
        status,
        headers,
        body,
    }
}
fn private_content_absent(body: &[u8]) {
    let text = std::str::from_utf8(body).unwrap();
    for secret in [
        "secret-native",
        "secret-definition",
        "secret-task",
        "secret-effect",
        "secret-policy",
        "private_sql_location",
        "Bearer owner",
    ] {
        assert!(
            !text.contains(secret),
            "private_capability_transport_content"
        );
    }
}

#[tokio::test]
async fn actual_tcp_native_descriptor_is_bounded_private_and_callback_free() {
    let store = Arc::new(Probe::default());
    let policy = Arc::new(Policy::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        native_config(calls.clone(), false),
    )
    .unwrap();
    store.reset();
    let server = serve(&host).await;
    let response = exchange(
        server.address,
        "GET",
        "/b4/capabilities",
        Some("Bearer owner"),
        "",
    )
    .await;
    assert_eq!(response.status, 200);
    private_content_absent(&response.body);
    assert!(response.body.len() > 64);
    assert!(response
        .headers
        .contains(&format!("content-length: {}", response.body.len())));
    let value: Value = serde_json::from_slice(&response.body).unwrap();
    serde_json::from_slice::<CapabilityDescriptorV2>(&response.body)
        .unwrap()
        .validate()
        .unwrap();
    assert_eq!(
        value["application"],
        json!({"id":"app","revision":"app-v1"})
    );
    assert_eq!(
        value["store"],
        json!({"kind":"memory","durable_runtime":false,"durable_approvals":false})
    );
    assert_eq!(value["limits"]["output_bytes"], 64);
    assert_eq!(value["features"]["required_once_deny"], true);
    assert_eq!(value["features"]["native_callback_sandbox"], false);
    assert_eq!(value["descriptor_version"], 2);
    assert_eq!(value["features"]["supervised_admission_drain"], true);
    assert_eq!(value["features"]["graceful_shutdown"], false);
    assert_eq!(
        value["readiness"]["checks"],
        json!(["store_readiness", "owned_worker_fault", "serving_admission"])
    );
    assert_eq!(
        value["controls"]["shutdown_default_wait_ms"],
        DEFAULT_SHUTDOWN_WAIT.as_millis() as u64
    );
    assert_eq!(
        value["controls"]["shutdown_max_wait_ms"],
        MAX_SHUTDOWN_WAIT.as_millis() as u64
    );
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(policy.calls.load(Ordering::Acquire), 1);
    assert_eq!(store.health.load(Ordering::Acquire), 1);
    assert_eq!(store.declarations.load(Ordering::Acquire), 1);
    store.no_private_reads_or_writes();
}

#[tokio::test]
async fn actual_tcp_auth_scope_and_denial_hide_unavailable_store() {
    let store = Arc::new(Probe::default());
    let policy = Arc::new(Policy::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        native_config(calls.clone(), false),
    )
    .unwrap();
    store.reset();
    store.unavailable.store(true, Ordering::Release);
    let server = serve(&host).await;
    for (token, status) in [(None, 401), (Some("Bearer foreign"), 404)] {
        let response = exchange(server.address, "GET", "/b4/capabilities", token, "").await;
        assert_eq!(response.status, status);
        private_content_absent(&response.body);
    }
    assert_eq!(policy.calls.load(Ordering::Acquire), 0);
    assert_eq!(store.health.load(Ordering::Acquire), 0);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    policy.deny.store(true, Ordering::Release);
    let denied = exchange(
        server.address,
        "GET",
        "/b4/capabilities",
        Some("Bearer owner"),
        "",
    )
    .await;
    assert_eq!(denied.status, 403);
    private_content_absent(&denied.body);
    assert_eq!(store.health.load(Ordering::Acquire), 0);
    policy.deny.store(false, Ordering::Release);
    let unavailable = exchange(
        server.address,
        "GET",
        "/b4/capabilities",
        Some("Bearer owner"),
        "",
    )
    .await;
    assert_eq!(unavailable.status, 503);
    private_content_absent(&unavailable.body);
    assert_eq!(store.health.load(Ordering::Acquire), 1);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    store.no_private_reads_or_writes();
}

#[tokio::test]
async fn actual_tcp_held_policy_cannot_publish_metadata_after_owned_worker_fault() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let policy = Arc::new(Policy {
        gate: Some(gate.clone()),
        ..Policy::default()
    });
    let store = Arc::new(Probe::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = native_config(calls.clone(), true);
    config.limits.max_output_bytes = 65_536;
    let host =
        HttpHost::new_with_application(store.clone(), Arc::new(Auth), policy, config).unwrap();
    let server = serve(&host).await;
    let address = server.address;
    let pending = tokio::spawn(async move {
        exchange(address, "GET", "/b4/capabilities", Some("Bearer owner"), "").await
    });
    gate.entered().await;
    let fault = exchange(
        server.address,
        "POST",
        "/threads/fault/runs/wait",
        Some("Bearer owner"),
        r#"{"route":"/secret-native#workflow","input":null}"#,
    )
    .await;
    assert_eq!(fault.status, 503);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    store.reset();
    gate.release();
    let response = pending.await.unwrap();
    assert_eq!(response.status, 503);
    private_content_absent(&response.body);
    let value: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(value["error"]["details"]["code"], "host_unready");
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    store.no_private_reads_or_writes();
}

#[tokio::test]
async fn actual_tcp_closed_capabilities_are_static_without_policy_or_store_work() {
    let store = Arc::new(Probe::default());
    let policy = Arc::new(Policy::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        native_config(calls.clone(), false),
    )
    .unwrap();
    store.reset();
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    let server = serve(&host).await;
    for token in [None, Some("Bearer foreign"), Some("Bearer owner")] {
        let response = exchange(server.address, "GET", "/b4/capabilities", token, "").await;
        assert_eq!(response.status, 503);
        private_content_absent(&response.body);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"]["details"]["code"], "host_draining");
        assert!(body.get("descriptor_version").is_none());
    }
    assert_eq!(policy.calls.load(Ordering::Acquire), 0);
    assert_eq!(store.health.load(Ordering::Acquire), 0);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    store.no_private_reads_or_writes();
    assert_eq!(
        shutdown.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
}

#[tokio::test]
async fn actual_tcp_disconnected_capability_policy_remains_owned_and_loses_cut() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let policy = Arc::new(Policy {
        gate: Some(gate.clone()),
        ..Policy::default()
    });
    let store = Arc::new(Probe::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        policy,
        native_config(calls.clone(), false),
    )
    .unwrap();
    store.reset();
    let server = serve(&host).await;
    let address = server.address;
    let client = tokio::spawn(async move {
        exchange(address, "GET", "/b4/capabilities", Some("Bearer owner"), "").await
    });
    gate.entered().await;
    client.abort();
    assert!(client.await.err().unwrap().is_cancelled());
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
    assert_eq!(store.health.load(Ordering::Acquire), 0);
    assert_eq!(store.declarations.load(Ordering::Acquire), 0);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    store.no_private_reads_or_writes();
}
