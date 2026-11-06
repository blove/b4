use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Map, Value};
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
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some("Bearer owner")
        {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
    }
}
fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: "app".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
struct Owner;
impl ResourcePolicy for Owner {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        context.origin.as_ref().is_none_or(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}
struct Gate {
    operation: Operation,
    block_check: usize,
    checks: AtomicUsize,
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
}
impl Gate {
    fn new(operation: Operation, block_check: usize) -> Self {
        Self {
            operation,
            block_check,
            checks: AtomicUsize::new(0),
            entered: AtomicBool::new(false),
            released: Mutex::new(false),
            wake: Condvar::new(),
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
    fn hold(&self) {
        self.entered.store(true, Ordering::Release);
        let guard = self.released.lock().unwrap();
        let (released, _) = self
            .wake
            .wait_timeout_while(guard, Duration::from_secs(5), |released| !*released)
            .unwrap();
        assert!(*released, "test gate was not released");
    }
}

struct ClaimedGateStore {
    inner: MemoryStore,
    gate: Gate,
    completion_gate: bool,
    fail_cancelled_completion: bool,
    completion_load_gate: bool,
    execution_reads: AtomicUsize,
}
impl Store for ClaimedGateStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        if self.completion_load_gate && self.execution_reads.fetch_add(1, Ordering::AcqRel) == 2 {
            self.gate.hold();
        }
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, initial)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        if self.fail_cancelled_completion
            && plan.snapshot.status == ExecutionStatus::Cancelled
            && plan
                .snapshot
                .tasks
                .iter()
                .any(|task| task.status == TaskStatus::Succeeded)
        {
            return Err(StoreError::RevisionConflict);
        }
        let dispatch = !self.completion_gate
            && plan.snapshot.status == ExecutionStatus::Running
            && plan
                .snapshot
                .tasks
                .iter()
                .any(|t| t.status == TaskStatus::InFlight && t.result.is_none());
        if self.completion_gate
            && plan.snapshot.status == ExecutionStatus::Completed
            && self.gate.checks.fetch_add(1, Ordering::AcqRel) == 0
        {
            self.gate.hold();
        }
        let stored = self.inner.commit(id, fence, plan)?;
        if dispatch && self.gate.checks.fetch_add(1, Ordering::AcqRel) == 0 {
            self.gate.hold();
        }
        Ok(stored)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_cancel_between_dispatch_commit_and_callback_entry_acknowledges_attempt() {
    let store = Arc::new(ClaimedGateStore {
        inner: MemoryStore::new(),
        gate: Gate::new(Operation::RunWait, 0),
        completion_gate: false,
        fail_cancelled_completion: false,
        completion_load_gate: false,
        execution_reads: AtomicUsize::new(0),
    });
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| store.gate.entered.load(Ordering::Acquire)).await;
    let before = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(before.snapshot.attempts_used, 1);
    assert_eq!(before.snapshot.tasks[0].status, TaskStatus::InFlight);
    let cancel = exchange(address, "POST", &format!("/threads/{id}/cancel"), "").await;
    assert_eq!(cancel.0, 200);
    assert_eq!(read_response(producer).await.0, 409);
    assert_eq!(
        exchange(address, "DELETE", &format!("/threads/{id}"), "")
            .await
            .0,
        409
    );
    store.gate.release();
    wait_until(|| {
        store
            .execution(&before.snapshot.execution_id)
            .unwrap()
            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Cancelled)
    })
    .await;
    let settled = store
        .execution(&before.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(settled.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert!(settled.snapshot.tasks[0].result.is_none());
    assert!(settled.snapshot.tasks[0].failure.is_none());
    assert_eq!(settled.snapshot.attempts_used, 1);
    assert_eq!(
        settled.snapshot.attempt_history,
        before.snapshot.attempt_history
    );
    assert_eq!(settled.snapshot.values, before.snapshot.values);
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_cancel_during_outcome_commit_retries_only_retained_result() {
    let store = Arc::new(ClaimedGateStore {
        inner: MemoryStore::new(),
        gate: Gate::new(Operation::RunWait, 0),
        completion_gate: true,
        fail_cancelled_completion: false,
        completion_load_gate: false,
        execution_reads: AtomicUsize::new(0),
    });
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| store.gate.entered.load(Ordering::Acquire)).await;
    let before = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(before.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        exchange(address, "POST", &format!("/threads/{id}/cancel"), "")
            .await
            .0,
        200
    );
    assert_eq!(read_response(producer).await.0, 409);
    store.gate.release();
    wait_until(|| {
        store
            .execution(&before.snapshot.execution_id)
            .unwrap()
            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Cancelled)
    })
    .await;
    let settled = store
        .execution(&before.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(settled.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        settled.snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("settled")
    );
    assert_eq!(settled.snapshot.attempts_used, 1);
    assert_eq!(
        settled.snapshot.attempt_history,
        before.snapshot.attempt_history
    );
    assert_eq!(settled.snapshot.values, before.snapshot.values);
    assert!(settled.snapshot.output.is_none());
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_completion_retry_exhaustion_faults_after_cancelled_terminal() {
    let store = Arc::new(ClaimedGateStore {
        inner: MemoryStore::new(),
        gate: Gate::new(Operation::RunWait, 0),
        completion_gate: true,
        fail_cancelled_completion: true,
        completion_load_gate: false,
        execution_reads: AtomicUsize::new(0),
    });
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| store.gate.entered.load(Ordering::Acquire)).await;
    let before = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(
        exchange(address, "POST", &format!("/threads/{id}/cancel"), "")
            .await
            .0,
        200
    );
    let terminal = read_response(producer).await;
    assert_eq!(terminal.0, 409);
    assert_eq!(
        serde_json::from_str::<Value>(&terminal.1).unwrap()["error"]["details"]["code"],
        "run_cancelled"
    );
    store.gate.release();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ready = exchange(address, "GET", "/readyz", "").await;
            if ready.0 == 503 {
                break;
            }
            assert_eq!(ready.0, 200);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let unresolved = store
        .execution(&before.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(unresolved.snapshot.status, ExecutionStatus::CancelRequested);
    assert_eq!(unresolved.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(unresolved.snapshot.tasks[0].result.is_none());
    assert_eq!(
        unresolved.snapshot.attempt_history,
        before.snapshot.attempt_history
    );
    assert_eq!(unresolved.snapshot.values, before.snapshot.values);
    assert_eq!(
        exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            r#"{"route":"/cancel#workflow"}"#
        )
        .await
        .0,
        503
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_cancel_between_completion_facts_and_reload_retains_known_outcome() {
    let store = Arc::new(ClaimedGateStore {
        inner: MemoryStore::new(),
        gate: Gate::new(Operation::RunWait, 0),
        completion_gate: false,
        fail_cancelled_completion: false,
        completion_load_gate: true,
        execution_reads: AtomicUsize::new(0),
    });
    // Keep the Dispatch return ungated: this test targets the fresh completion
    // reload after the callback and its completion time have already been read.
    store.gate.checks.store(1, Ordering::Release);
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| store.gate.entered.load(Ordering::Acquire)).await;
    let before = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        exchange(address, "POST", &format!("/threads/{id}/cancel"), "")
            .await
            .0,
        200
    );
    assert_eq!(read_response(producer).await.0, 409);
    store.gate.release();
    wait_until(|| {
        store
            .thread_view(&thread_id)
            .unwrap()
            .and_then(|view| view.execution)
            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Cancelled)
    })
    .await;
    let settled = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(settled.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        settled.snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("settled")
    );
    assert_eq!(
        settled.snapshot.attempt_history,
        before.snapshot.attempt_history
    );
    assert_eq!(settled.snapshot.values, before.snapshot.values);
    assert!(settled.snapshot.output.is_none());
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}
impl ResourcePolicy for Gate {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        if context.operation == self.operation
            && self.checks.fetch_add(1, Ordering::AcqRel) == self.block_check
        {
            self.entered.store(true, Ordering::Release);
            let guard = self.released.lock().unwrap();
            let (released, _) = self
                .wake
                .wait_timeout_while(guard, Duration::from_secs(5), |released| !*released)
                .unwrap();
            if !*released {
                return false;
            }
        }
        Owner.authorize(principal, context)
    }
}
struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("b4-cancel-tcp-{}", uuid::Uuid::new_v4())))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn route(required: bool, delay_ms: u64) -> ScriptRoute {
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/cancel#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            max_attempts: 1,
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: EffectId::new("effect").unwrap(),
                requires_approval: required,
                recovery: RecoveryPolicy::Pure,
            }]],
        },
        effects: BTreeMap::from([(
            EffectId::new("effect").unwrap(),
            ScriptEffect::Delayed {
                duration_ms: delay_ms,
                result: TaskResult {
                    output: json!("settled"),
                    writes: vec![StateWrite::Append {
                        field: "calls".into(),
                        items: vec![json!("settled")],
                    }],
                },
            },
        )]),
    }
}
fn config(scripted: ScriptRoute) -> HostConfig {
    let mut config = HostConfig::new(vec![scripted]);
    config.max_active_runs = 1;
    config
}
fn approval_config() -> ApprovalHostConfig {
    ApprovalHostConfig::new(
        "policy-v1".into(),
        BTreeMap::from([(
            RouteKey::new("/cancel#workflow").unwrap(),
            BTreeMap::from([(TaskId::new("task").unwrap(), json!({"question":"approve?"}))]),
        )]),
    )
}
async fn serve<S: Store + 'static>(
    host: &HttpHost<S>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("actual TCP cancellation tests require loopback bind permission");
    let address = listener.local_addr().unwrap();
    let router = host.router();
    (
        address,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}
async fn open(address: std::net::SocketAddr, method: &str, path: &str, body: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer owner\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    stream
}
async fn exchange(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) -> (u16, String, String) {
    let mut stream = open(address, method, path, body).await;
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let raw = String::from_utf8(bytes).unwrap();
    let (headers, body) = raw.split_once("\r\n\r\n").unwrap();
    (
        headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers.into(),
        body.into(),
    )
}
async fn create(address: std::net::SocketAddr) -> String {
    let result = exchange(address, "POST", "/threads", "{}").await;
    assert_eq!(result.0, 200);
    serde_json::from_str::<Value>(&result.2).unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .into()
}
async fn wait_until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn read_response(mut stream: TcpStream) -> (u16, String) {
    let mut bytes = vec![];
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let raw = String::from_utf8(bytes).unwrap();
    let (headers, body) = raw.split_once("\r\n\r\n").unwrap();
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut rest = body.as_bytes();
        let mut payload = Vec::new();
        loop {
            let end = rest.windows(2).position(|w| w == b"\r\n").unwrap();
            let size = usize::from_str_radix(
                std::str::from_utf8(&rest[..end])
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )
            .unwrap();
            rest = &rest[end + 2..];
            if size == 0 {
                break;
            }
            payload.extend_from_slice(&rest[..size]);
            assert_eq!(&rest[size..size + 2], b"\r\n");
            rest = &rest[size + 2..];
        }
        String::from_utf8(payload).unwrap()
    } else {
        body.into()
    };
    (
        headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body,
    )
}

fn reference_status(id: &str, status: u16) {
    let capture: Value = serde_json::from_str(include_str!("../../../conformance/reference/recordings/2026-10-01-lifecycle/lifecycle-observations.json")).unwrap();
    let observation = capture["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == id)
        .unwrap();
    assert_eq!(
        u64::from(status),
        observation["response"]["status"].as_u64().unwrap()
    );
}

fn reference_cancel_receipt(id: &str, actual: &Value, thread: &str) {
    let capture:Value=serde_json::from_str(include_str!("../../../conformance/reference/recordings/2026-10-01-lifecycle/lifecycle-observations.json")).unwrap();
    let observation = capture["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == id)
        .unwrap();
    let mut reference: Value =
        serde_json::from_str(observation["response"]["body_utf8"].as_str().unwrap()).unwrap();
    assert_eq!(actual["thread_id"], thread);
    assert!(reference["thread_id"].is_string());
    let mut native = actual.clone();
    reference["thread_id"] = json!("<thread>");
    native["thread_id"] = json!("<thread>");
    assert_eq!(
        native, reference,
        "selected cancellation body after thread-ID normalization"
    );
}

fn reference_cancelled_done(actual: &Value) {
    let capture:Value=serde_json::from_str(include_str!("../../../conformance/reference/recordings/2026-10-01-lifecycle/lifecycle-observations.json")).unwrap();
    let observation = capture["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == "stream-cancel.producer")
        .unwrap();
    let frames = observation["response"]["sse_frames"].as_array().unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0]["event"], "done");
    assert_eq!(*actual, frames[0]["data"]);
}

async fn stream_headers(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut part = [0u8; 1024];
            let read = stream.read(&mut part).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&part[..read]);
        }
    })
    .await
    .unwrap();
    let status = std::str::from_utf8(&bytes)
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_cancel_held_in_initial_policy_cannot_retarget_successor() {
    let store = Arc::new(MemoryStore::new());
    let gate = Arc::new(Gate::new(Operation::CancelRun, 0));
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        gate.clone(),
        config(route(false, 600)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let first = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| {
        store
            .thread_view(&thread_id)
            .unwrap()
            .and_then(|v| v.execution)
            .is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.status == TaskStatus::InFlight)
            })
    })
    .await;
    let first_id = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
        .execution_id;
    let held_cancel = open(address, "POST", &format!("/threads/{id}/cancel"), "").await;
    wait_until(|| gate.entered.load(Ordering::Acquire)).await;
    assert_eq!(read_response(first).await.0, 200);
    // Wait for actual claim release through the public admission boundary.
    let mut successor = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let mut next = open(
                address,
                "POST",
                &format!("/threads/{id}/runs/stream"),
                r#"{"route":"/cancel#workflow"}"#,
            )
            .await;
            let (status, prefix) = stream_headers(&mut next).await;
            if status == 200 {
                break (next, prefix);
            }
            assert!(status == 409 || status == 503);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let execution = store
                .thread_view(&thread_id)
                .unwrap()
                .unwrap()
                .execution
                .unwrap();
            if execution.snapshot.execution_id != first_id
                && execution.snapshot.status == ExecutionStatus::Running
            {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let second = second.unwrap();
    assert_ne!(second.snapshot.execution_id, first_id);
    assert_eq!(second.snapshot.status, ExecutionStatus::Running);
    gate.release();
    let stale = read_response(held_cancel).await;
    assert_eq!(stale.0, 409);
    reference_status("late-cancel.held-cancel", stale.0);
    assert_eq!(
        serde_json::from_str::<Value>(&stale.1).unwrap()["error"]["details"]["code"],
        "no_run_in_flight"
    );
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), successor.0.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    successor.1.extend(tail);
    let raw = String::from_utf8(successor.1).unwrap();
    assert!(raw.contains("event: done\n"));
    assert!(raw.contains("settled"));
    assert!(!raw.contains("cancelled"));
    assert_eq!(
        store
            .execution(&second.snapshot.execution_id)
            .unwrap()
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::Completed
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_abandoned_cancellation_keeps_owned_store_until_commit_and_drain() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak = Arc::downgrade(&store);
    let gate = Arc::new(Gate::new(Operation::CancelRun, 1));
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        gate.clone(),
        config(route(false, 3000)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| {
        store
            .thread_view(&thread_id)
            .unwrap()
            .and_then(|v| v.execution)
            .is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.status == TaskStatus::InFlight)
            })
    })
    .await;
    let cancel = open(address, "POST", &format!("/threads/{id}/cancel"), "").await;
    wait_until(|| gate.entered.load(Ordering::Acquire)).await;
    let execution_id = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
        .execution_id;
    drop(cancel);
    drop(producer);
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    gate.release();
    wait_until(|| weak.upgrade().is_none()).await;
    // Weak disappearance precedes field destruction on the blocking worker;
    // qualify actual lock release rather than assuming the destructor finished.
    let reopened = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory.0) {
                Ok(store) => break store,
                Err(StoreError::AlreadyOwned) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("unexpected reopen error: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.owner_generation(), 2);
    let settled = reopened.execution(&execution_id).unwrap().unwrap();
    assert_eq!(settled.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(settled.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert!(!settled.snapshot.values.contains_key("calls"));
    assert!(settled.snapshot.output.is_none());
    assert!(reopened
        .journal(&execution_id)
        .unwrap()
        .iter()
        .any(|e| e.status == ExecutionStatus::CancelRequested));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_cancel_finishes_wait_before_effect_quiescence_and_keeps_capacity() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(false, 3000)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| {
        store
            .thread_view(&thread_id)
            .unwrap()
            .and_then(|v| v.execution)
            .is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.status == TaskStatus::InFlight)
            })
    })
    .await;
    // The finite delay keeps the already entered invocation live. Bound all
    // prompt observations well inside its declared duration; durable state
    // below proves that cancellation has not falsely claimed quiescence.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cancelled = tokio::time::timeout(
        Duration::from_millis(800),
        exchange(address, "POST", &format!("/threads/{id}/cancel"), ""),
    )
    .await
    .unwrap();
    assert_eq!(cancelled.0, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&cancelled.2).unwrap(),
        json!({"status":"interrupted","thread_id":id})
    );
    reference_status("wait-cancel.cancel", cancelled.0);
    reference_cancel_receipt(
        "wait-cancel.cancel",
        &serde_json::from_str::<Value>(&cancelled.2).unwrap(),
        &id,
    );
    let done = tokio::time::timeout(Duration::from_millis(800), read_response(producer))
        .await
        .unwrap();
    assert_eq!(done.0, 409);
    assert_eq!(
        serde_json::from_str::<Value>(&done.1).unwrap()["error"]["details"]["code"],
        "run_cancelled"
    );
    reference_status("wait-cancel.producer", done.0);
    let draining = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(draining.snapshot.status, ExecutionStatus::CancelRequested);
    assert!(draining.snapshot.output.is_none());
    assert!(draining.snapshot.tasks[0].result.is_none());
    let deleted = exchange(address, "DELETE", &format!("/threads/{id}"), "").await;
    assert_eq!(deleted.0, 409);
    reference_status("wait-cancel.delete-before-quiescence", deleted.0);
    assert_eq!(
        serde_json::from_str::<Value>(&deleted.2).unwrap()["error"]["details"]["code"],
        "run_in_flight"
    );
    let another = exchange(
        address,
        "POST",
        "/threads/other/runs/wait",
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    assert_eq!(another.0, 503);
    assert_eq!(
        serde_json::from_str::<Value>(&another.2).unwrap()["error"]["details"]["code"],
        "capacity_exhausted"
    );
    assert!(store
        .thread(&ThreadId::new("other").unwrap())
        .unwrap()
        .is_none());
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    wait_until(|| {
        store
            .execution(&draining.snapshot.execution_id)
            .unwrap()
            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Cancelled)
    })
    .await;
    let settled = store
        .execution(&draining.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(settled.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert!(settled.snapshot.tasks[0].result.is_some());
    assert!(settled.snapshot.output.is_none());
    assert!(!settled.snapshot.values.contains_key("calls"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    wait_until(|| {
        // The terminal commit precedes final claim release by a small owned
        // worker window. This readiness predicate leaves that fact explicit.
        store
            .execution(&draining.snapshot.execution_id)
            .unwrap()
            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Cancelled)
    })
    .await;
    let mut deleted = exchange(address, "DELETE", &format!("/threads/{id}"), "").await;
    for _ in 0..20 {
        if deleted.0 != 409 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        deleted = exchange(address, "DELETE", &format!("/threads/{id}"), "").await;
    }
    assert_eq!(deleted.0, 204);
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    let reopened = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
    assert_eq!(reopened.owner_generation(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_cancel_stream_has_one_cancelled_done_and_no_late_output() {
    let store = Arc::new(MemoryStore::new());
    let mut scripted = route(false, 2000);
    scripted.definition.max_attempts = 2;
    scripted.definition.batches.push(vec![TaskSpec {
        id: TaskId::new("later-task").unwrap(),
        effect_id: EffectId::new("later-effect").unwrap(),
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    }]);
    scripted.effects.insert(
        EffectId::new("later-effect").unwrap(),
        ScriptEffect::Chunks {
            chunks: vec![json!({"later":true})],
            result: TaskResult {
                output: json!("later"),
                writes: vec![StateWrite::Append {
                    field: "calls".into(),
                    items: vec![json!("later")],
                }],
            },
        },
    );
    let mut configuration = config(scripted);
    configuration.max_active_runs = 2;
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        configuration,
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    wait_until(|| {
        store
            .thread_view(&thread_id)
            .unwrap()
            .and_then(|v| v.execution)
            .is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.status == TaskStatus::InFlight)
            })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cancelled = tokio::time::timeout(
        Duration::from_millis(800),
        exchange(address, "POST", &format!("/threads/{id}/cancel"), ""),
    )
    .await
    .unwrap();
    assert_eq!(cancelled.0, 200);
    reference_status("stream-cancel.cancel", cancelled.0);
    reference_cancel_receipt(
        "stream-cancel.cancel",
        &serde_json::from_str::<Value>(&cancelled.2).unwrap(),
        &id,
    );
    let (status, events) =
        tokio::time::timeout(Duration::from_millis(800), read_response(producer))
            .await
            .unwrap();
    assert_eq!(status, 200);
    reference_status("stream-cancel.producer", status);
    let terminals: Vec<_> = events
        .split("\n\n")
        .filter(|b| b.starts_with("event: done\n"))
        .collect();
    assert_eq!(terminals.len(), 1, "{events}");
    let payload = terminals[0]
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(payload).unwrap(),
        json!({"output":{"cancelled":true}})
    );
    reference_cancelled_done(&serde_json::from_str::<Value>(payload).unwrap());
    assert!(!events.contains("settled"));
    assert!(!events.contains("later"));
    assert!(!events.contains("event: chunk"));
    let draining = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(draining.snapshot.status, ExecutionStatus::CancelRequested);
    let same_run = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/cancel#workflow"}"#,
    )
    .await;
    assert_eq!(same_run.0, 409);
    reference_status("wait-cancel.run-before-quiescence", same_run.0);
    let deletion = exchange(address, "DELETE", &format!("/threads/{id}"), "").await;
    assert_eq!(deletion.0, 409);
    reference_status("stream-cancel.delete-before-quiescence", deletion.0);
    wait_until(|| {
        store
            .execution(&draining.snapshot.execution_id)
            .unwrap()
            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Cancelled)
    })
    .await;
    let settled = store
        .execution(&draining.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(settled.snapshot.attempts_used, 1);
    assert_eq!(settled.snapshot.tasks.len(), 1);
    assert!(!settled.snapshot.values.contains_key("calls"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_stable_approval_park_is_not_an_active_cancellation_target() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(true, 0)),
        approval_config(),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    assert_eq!(
        exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            r#"{"route":"/cancel#workflow"}"#
        )
        .await
        .0,
        200
    );
    let thread_id = ThreadId::new(id.as_str()).unwrap();
    let before = store.thread_view(&thread_id).unwrap().unwrap();
    let current = before.execution.as_ref().unwrap();
    assert_eq!(current.snapshot.status, ExecutionStatus::WaitingForApproval);
    let authority = store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let cancel = exchange(address, "POST", &format!("/threads/{id}/cancel"), "").await;
    assert_eq!(cancel.0, 409);
    reference_status("parked.cancel", cancel.0);
    assert_eq!(
        serde_json::from_str::<Value>(&cancel.2).unwrap()["error"]["details"]["code"],
        "no_run_in_flight"
    );
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), before);
    assert_eq!(
        store
            .approval_view(&current.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        authority
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_prior_owner_inflight_without_local_worker_is_not_cancelled() {
    let directory = Directory::new();
    let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
    let id = ThreadId::new("prior-owner").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(origin()),
        })
        .unwrap();
    let runtime = Runtime::new(&store);
    let accepted = runtime
        .admit(
            &id,
            b4_kernel::new_execution(
                ExecutionId::new("prior-execution").unwrap(),
                TurnId::new("prior-turn").unwrap(),
                1,
                route(false, 0).definition,
                Map::new(),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let running = runtime
        .apply(
            &accepted.snapshot.execution_id,
            ExecutionFence::from(&accepted.snapshot),
            KernelCommand::Start,
            TransitionFacts { now_ms: 2 },
        )
        .unwrap();
    let claimed = runtime
        .apply(
            &running.snapshot.execution_id,
            ExecutionFence::from(&running.snapshot),
            KernelCommand::Dispatch {
                task_id: TaskId::new("task").unwrap(),
                attempt_id: AttemptId::new("prior-attempt").unwrap(),
            },
            TransitionFacts { now_ms: 3 },
        )
        .unwrap();
    let before = store.thread_view(&id).unwrap().unwrap();
    drop(store);
    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = HttpHost::new(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let cancel = exchange(address, "POST", "/threads/prior-owner/cancel", "").await;
    assert_eq!(cancel.0, 409);
    assert_eq!(
        serde_json::from_str::<Value>(&cancel.2).unwrap()["error"]["details"]["code"],
        "no_run_in_flight"
    );
    assert_eq!(reopened.thread_view(&id).unwrap().unwrap(), before);
    assert_eq!(
        reopened
            .execution(&claimed.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        claimed
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}
