use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
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
        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer owner") {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
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
            assert!(!timeout.timed_out(), "parallel TCP gate was not released");
        }
    }
    async fn entered(&self) {
        until(|| self.state.lock().unwrap().0).await;
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
struct GatedStore {
    inner: MemoryStore,
    first_result: Arc<Gate>,
    second_result: Arc<Gate>,
    parked: Arc<Gate>,
    hold_results: bool,
    hold_park: bool,
    first_seen: AtomicBool,
    second_seen: AtomicBool,
}
impl GatedStore {
    fn new(hold_results: bool, hold_park: bool) -> Self {
        Self {
            inner: MemoryStore::new(),
            first_result: Arc::new(Gate::default()),
            second_result: Arc::new(Gate::default()),
            parked: Arc::new(Gate::default()),
            hold_results,
            hold_park,
            first_seen: AtomicBool::new(false),
            second_seen: AtomicBool::new(false),
        }
    }
    fn snapshot(&self, id: &str) -> Snapshot {
        self.inner
            .thread_view(&ThreadId::new(id).unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
    }
}
impl Store for GatedStore {
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
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, snapshot)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        if self.hold_results {
            if plan
                .snapshot
                .tasks
                .first()
                .is_some_and(|task| task.result.is_some())
                && !self.first_seen.swap(true, Ordering::AcqRel)
            {
                self.first_result.hold();
            }
            if plan
                .snapshot
                .tasks
                .get(1)
                .is_some_and(|task| task.result.is_some())
                && !self.second_seen.swap(true, Ordering::AcqRel)
            {
                self.second_result.hold();
            }
        }
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
}
impl ApprovalStore for GatedStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        let park = matches!(mutation, ApprovalMutation::Issue(_));
        let committed = self.inner.commit_approval(id, fence, plan, mutation)?;
        if park && self.hold_park {
            self.parked.hold();
        }
        Ok(committed)
    }
}
fn route(required: bool) -> ScriptRoute {
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/parallel#workflow").unwrap(),
            revision: ExecutionRevision::new("parallel-v1").unwrap(),
            batches: vec![vec![
                TaskSpec {
                    id: TaskId::new("first").unwrap(),
                    effect_id: EffectId::new("first").unwrap(),
                    requires_approval: false,
                    recovery: RecoveryPolicy::Pure,
                },
                TaskSpec {
                    id: TaskId::new("second").unwrap(),
                    effect_id: EffectId::new("second").unwrap(),
                    requires_approval: required,
                    recovery: RecoveryPolicy::Pure,
                },
            ]],
            max_attempts: 2,
        },
        effects: ["first", "second"]
            .into_iter()
            .map(|id| {
                (
                    EffectId::new(id).unwrap(),
                    ScriptEffect::Delayed {
                        duration_ms: if id == "first" { 300 } else { 900 },
                        result: TaskResult {
                            output: json!(id),
                            writes: vec![StateWrite::Append {
                                field: "calls".into(),
                                items: vec![json!(id)],
                            }],
                        },
                    },
                )
            })
            .collect(),
    }
}
fn fast_route() -> ScriptRoute {
    let mut script = route(false);
    script.definition.route = RouteKey::new("/fast#workflow").unwrap();
    script.definition.batches[0].truncate(1);
    script.definition.max_attempts = 1;
    script.effects = BTreeMap::from([(
        EffectId::new("first").unwrap(),
        ScriptEffect::Result(TaskResult {
            output: json!("available"),
            writes: vec![],
        }),
    )]);
    script
}
fn config(required: bool, capacity: usize) -> HostConfig {
    let mut config = HostConfig::new(vec![route(required), fast_route()]);
    config.max_active_runs = capacity;
    config.heartbeat_ms = 10;
    config
}
async fn serve<S: Store + 'static>(
    host: &HttpHost<S>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("parallel transport tests require loopback bind permission");
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
struct Response {
    status: u16,
    body: String,
}
fn decode(bytes: &[u8]) -> Response {
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(&bytes[..split]).unwrap();
    let mut body = bytes[split + 4..].to_vec();
    if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut remaining = body.as_slice();
        let mut decoded = vec![];
        loop {
            let end = remaining
                .windows(2)
                .position(|window| window == b"\r\n")
                .unwrap();
            let size = usize::from_str_radix(
                std::str::from_utf8(&remaining[..end])
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )
            .unwrap();
            remaining = &remaining[end + 2..];
            if size == 0 {
                break;
            }
            decoded.extend_from_slice(&remaining[..size]);
            assert_eq!(&remaining[size..size + 2], b"\r\n");
            remaining = &remaining[size + 2..];
        }
        body = decoded;
    }
    Response {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body: String::from_utf8(body).unwrap(),
    }
}
async fn finish(mut stream: TcpStream, mut prefix: Vec<u8>) -> Response {
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut prefix))
        .await
        .unwrap()
        .unwrap();
    decode(&prefix)
}
async fn exchange(address: std::net::SocketAddr, method: &str, path: &str, body: &str) -> Response {
    finish(open(address, method, path, body).await, vec![]).await
}
async fn prefix(stream: &mut TcpStream, marker: &[u8]) -> Vec<u8> {
    let mut bytes = vec![];
    tokio::time::timeout(Duration::from_secs(3), async {
        while !bytes.windows(marker.len()).any(|window| window == marker) {
            let mut part = [0; 1024];
            let read = stream.read(&mut part).await.unwrap();
            assert!(read > 0, "stream ended before expected prefix");
            bytes.extend_from_slice(&part[..read]);
        }
    })
    .await
    .unwrap();
    bytes
}
async fn create(address: std::net::SocketAddr) -> String {
    let response = exchange(address, "POST", "/threads", "{}").await;
    assert_eq!(response.status, 200);
    serde_json::from_str::<Value>(&response.body).unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .into()
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
fn frames(body: &str) -> Vec<(String, Value)> {
    body.replace("\r\n", "\n")
        .split("\n\n")
        .filter_map(|block| {
            let event = block
                .lines()
                .find_map(|line| line.strip_prefix("event: "))?;
            let payload = block
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .collect::<Vec<_>>()
                .join("\n");
            Some((event.into(), serde_json::from_str(&payload).unwrap()))
        })
        .collect()
}
async fn delete_when_released(address: std::net::SocketAddr, id: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = exchange(address, "DELETE", &format!("/threads/{id}"), "").await;
            if response.status == 204 {
                break;
            }
            assert_eq!(response.status, 409);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_parallel_cancel_keeps_both_attempts_owned_and_attachment_has_one_terminal() {
    let store = Arc::new(GatedStore::new(true, false));
    let _release_first = Release(store.first_result.clone());
    let _release_second = Release(store.second_result.clone());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(false, 2),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        r#"{"route":"/parallel#workflow","input":"original"}"#,
    )
    .await;
    store.first_result.entered().await;
    let before = store.snapshot(&id);
    assert_eq!(before.attempts_used, 2);
    assert!(before
        .tasks
        .iter()
        .all(|task| task.status == TaskStatus::InFlight));
    let mut attached = open(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    let attached_prefix = prefix(&mut attached, b"event: state").await;
    assert_eq!(
        exchange(address, "POST", &format!("/threads/{id}/cancel"), "")
            .await
            .status,
        200
    );
    let primary = tokio::time::timeout(Duration::from_millis(500), finish(producer, vec![]))
        .await
        .unwrap();
    let attachment = tokio::time::timeout(
        Duration::from_millis(500),
        finish(attached, attached_prefix),
    )
    .await
    .unwrap();
    assert_eq!(primary.status, 200);
    assert_eq!(attachment.status, 200);
    let cancelled = ("done".into(), json!({"output":{"cancelled":true}}));
    assert_eq!(frames(&primary.body), vec![cancelled.clone()]);
    let attached_frames = frames(&attachment.body);
    assert_eq!(attached_frames[0].0, "state");
    assert_eq!(attached_frames[0].1["live"], true);
    assert_eq!(
        attached_frames
            .iter()
            .filter(|frame| frame.0 == "done")
            .count(),
        1
    );
    assert_eq!(attached_frames.last().unwrap(), &cancelled);
    assert_eq!(
        exchange(address, "DELETE", &format!("/threads/{id}"), "")
            .await
            .status,
        409
    );
    assert_eq!(
        exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            r#"{"route":"/parallel#workflow"}"#
        )
        .await
        .status,
        409
    );
    store.first_result.release();
    store.second_result.entered().await;
    let half_drained = store.snapshot(&id);
    assert_eq!(half_drained.status, ExecutionStatus::CancelRequested);
    assert_eq!(half_drained.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(half_drained.tasks[1].status, TaskStatus::InFlight);
    assert!(!half_drained.values.contains_key("calls"));
    assert_eq!(
        exchange(address, "DELETE", &format!("/threads/{id}"), "")
            .await
            .status,
        409
    );
    store.second_result.release();
    until(|| store.snapshot(&id).status == ExecutionStatus::Cancelled).await;
    let settled = store.snapshot(&id);
    assert_eq!(settled.attempts_used, 2);
    assert_eq!(settled.attempt_history, before.attempt_history);
    assert!(settled
        .tasks
        .iter()
        .all(|task| task.status == TaskStatus::Succeeded));
    assert_eq!(settled.tasks[0].result.as_ref().unwrap().output, "first");
    assert_eq!(settled.tasks[1].result.as_ref().unwrap().output, "second");
    assert!(!settled.values.contains_key("calls"));
    assert_eq!(settled.values["input"], "original");
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    delete_when_released(address, &id).await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_abandoned_parallel_viewer_retains_run_capacity_through_both_results() {
    let store = Arc::new(GatedStore::new(true, false));
    let _release_first = Release(store.first_result.clone());
    let _release_second = Release(store.second_result.clone());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(false, 1),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let other = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        r#"{"route":"/parallel#workflow"}"#,
    )
    .await;
    store.first_result.entered().await;
    let before = store.snapshot(&id);
    assert_eq!(before.attempts_used, 2);
    assert!(before
        .tasks
        .iter()
        .all(|task| task.status == TaskStatus::InFlight));
    drop(producer);
    assert_eq!(
        exchange(
            address,
            "POST",
            &format!("/threads/{other}/runs/wait"),
            r#"{"route":"/fast#workflow"}"#
        )
        .await
        .status,
        503
    );
    assert!(store
        .inner
        .thread_view(&ThreadId::new(&other).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .is_none());
    assert_eq!(
        exchange(address, "DELETE", &format!("/threads/{id}"), "")
            .await
            .status,
        409
    );
    store.first_result.release();
    store.second_result.entered().await;
    let half_drained = store.snapshot(&id);
    assert_eq!(half_drained.status, ExecutionStatus::Running);
    assert_eq!(half_drained.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(half_drained.tasks[1].status, TaskStatus::InFlight);
    assert_eq!(
        exchange(
            address,
            "POST",
            &format!("/threads/{other}/runs/wait"),
            r#"{"route":"/fast#workflow"}"#
        )
        .await
        .status,
        503
    );
    store.second_result.release();
    until(|| store.snapshot(&id).status == ExecutionStatus::Completed).await;
    let completed = store.snapshot(&id);
    assert_eq!(completed.attempts_used, 2);
    assert_eq!(completed.values["calls"], json!(["first", "second"]));
    delete_when_released(address, &id).await;
    let available = exchange(
        address,
        "POST",
        &format!("/threads/{other}/runs/wait"),
        r#"{"route":"/fast#workflow"}"#,
    )
    .await;
    assert_eq!(available.status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&available.body).unwrap(),
        "available"
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_parallel_park_and_once_resume_preserve_the_sibling_attempt() {
    let store = Arc::new(GatedStore::new(false, true));
    let _release = Release(store.parked.clone());
    let mut approvals = ApprovalHostConfig::new(
        "policy-v1".into(),
        BTreeMap::from([(
            RouteKey::new("/parallel#workflow").unwrap(),
            BTreeMap::from([(
                TaskId::new("second").unwrap(),
                json!({"question":"Approve second?"}),
            )]),
        )]),
    );
    approvals.service_config = ApprovalServiceConfig::VolatileFixture;
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(true, 2),
        approvals,
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        r#"{"route":"/parallel#workflow","input":"batch"}"#,
    )
    .await;
    store.parked.entered().await;
    let parking = store.snapshot(&id);
    assert_eq!(parking.status, ExecutionStatus::Running);
    assert_eq!(parking.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(parking.tasks[1].status, TaskStatus::WaitingForApproval);
    assert_eq!(
        exchange(
            address,
            "GET",
            &format!("/threads/{id}/pending_interrupts"),
            ""
        )
        .await
        .status,
        409
    );
    store.parked.release();
    let parked_stream = finish(producer, vec![]).await;
    assert_eq!(parked_stream.status, 200);
    assert_eq!(
        frames(&parked_stream.body)
            .iter()
            .filter(|frame| frame.0 == "interrupt")
            .count(),
        1
    );
    assert_eq!(
        frames(&parked_stream.body).last().unwrap(),
        &("done".into(), json!({"output":null}))
    );
    let parked = store.snapshot(&id);
    assert_eq!(parked.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(parked.attempts_used, 1);
    assert_eq!(parked.tasks[0].status, TaskStatus::Succeeded);
    let pending = exchange(
        address,
        "GET",
        &format!("/threads/{id}/pending_interrupts"),
        "",
    )
    .await;
    assert_eq!(pending.status, 200);
    let pending: Value = serde_json::from_str(&pending.body).unwrap();
    let interrupt = &pending["interrupts"][0];
    let decision = json!({"route":"/parallel#workflow","resume":[{
        "status":"resolved", "interruptId":interrupt["interruptId"],
        "payload":"once", "grant":interrupt["grant"],
    }]})
    .to_string();
    let resumed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response =
                exchange(address, "POST", &format!("/threads/{id}/resume"), &decision).await;
            if response.status == 200 {
                break response;
            }
            assert_eq!(response.status, 409);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        frames(&resumed.body),
        vec![("done".into(), json!({"output":"second"}))]
    );
    let completed = store.snapshot(&id);
    assert_eq!(completed.status, ExecutionStatus::Completed);
    assert_eq!(completed.attempts_used, 2);
    assert_eq!(completed.tasks[0], parked.tasks[0]);
    assert_eq!(completed.attempt_history[0], parked.attempt_history[0]);
    assert_eq!(completed.values["calls"], json!(["first", "second"]));
    assert!(completed
        .tasks
        .iter()
        .all(|task| task.input["input"] == "batch"));
    let authority = store
        .approval_view(&completed.execution_id)
        .unwrap()
        .unwrap();
    assert!(authority.grants[0].consumed);
    assert_eq!(authority.acceptances.len(), 1);
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
}
