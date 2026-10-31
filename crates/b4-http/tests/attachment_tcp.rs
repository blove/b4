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
        let principal = match headers.get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer owner") => "owner",
            Some("Bearer foreign") => "foreign",
            _ => return Err(AuthenticationError),
        };
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: principal.into(),
        })
    }
}
struct Allow;
impl ResourcePolicy for Allow {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        true
    }
}
fn route(required: bool, delay: u64) -> ScriptRoute {
    let task = TaskSpec {
        id: TaskId::new("task").unwrap(),
        effect_id: EffectId::new("effect").unwrap(),
        requires_approval: required,
        recovery: RecoveryPolicy::Pure,
    };
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/attach#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![task]],
            max_attempts: 1,
        },
        effects: BTreeMap::from([(
            EffectId::new("effect").unwrap(),
            ScriptEffect::Delayed {
                duration_ms: delay,
                result: TaskResult {
                    output: json!("actual-output"),
                    writes: vec![StateWrite::Append {
                        field: "calls".into(),
                        items: vec![json!("actual-output")],
                    }],
                },
            },
        )]),
    }
}
fn config(scripted: ScriptRoute) -> HostConfig {
    let mut c = HostConfig::new(vec![scripted]);
    c.heartbeat_ms = 10;
    c
}
async fn serve<S: Store + 'static>(
    host: &HttpHost<S>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("attachment transport tests require loopback bind permission");
    let address = listener.local_addr().unwrap();
    let router = host.router();
    (
        address,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}
async fn open_as(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    actor: &str,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {actor}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    stream
}
async fn open(address: std::net::SocketAddr, method: &str, path: &str, body: &str) -> TcpStream {
    open_as(address, method, path, body, "owner").await
}
struct Response {
    status: u16,
    headers: String,
    body: String,
}
fn decode(bytes: Vec<u8>) -> Response {
    let split = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let headers = String::from_utf8(bytes[..split].to_vec()).unwrap();
    let raw = &bytes[split + 4..];
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut rest = raw;
        let mut payload = vec![];
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
        String::from_utf8(raw.to_vec()).unwrap()
    };
    Response {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers,
        body,
    }
}
async fn finish(mut stream: TcpStream, mut prefix: Vec<u8>) -> Response {
    tokio::time::timeout(Duration::from_secs(6), stream.read_to_end(&mut prefix))
        .await
        .unwrap()
        .unwrap();
    decode(prefix)
}
async fn exchange(address: std::net::SocketAddr, method: &str, path: &str, body: &str) -> Response {
    finish(open(address, method, path, body).await, vec![]).await
}
async fn prefix(stream: &mut TcpStream, marker: &[u8]) -> Vec<u8> {
    let mut bytes = vec![];
    tokio::time::timeout(Duration::from_secs(3), async {
        while !bytes.windows(marker.len()).any(|w| w == marker) {
            let mut part = [0; 1024];
            let count = stream.read(&mut part).await.unwrap();
            assert!(count > 0, "response ended before expected prefix");
            bytes.extend_from_slice(&part[..count]);
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
fn frames(body: &str) -> Vec<(String, Value)> {
    body.replace("\r\n", "\n")
        .split("\n\n")
        .filter_map(|block| {
            let name = block
                .lines()
                .find_map(|line| line.strip_prefix("event: "))?;
            let payload = block
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .collect::<Vec<_>>()
                .join("\n");
            Some((name.into(), serde_json::from_str(&payload).unwrap()))
        })
        .collect()
}
async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
}
fn inflight<S: Store>(store: &S, id: &str) -> bool {
    store
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .and_then(|v| v.execution)
        .is_some_and(|e| {
            e.snapshot
                .tasks
                .iter()
                .any(|t| t.status == TaskStatus::InFlight)
        })
}
fn selected_reference(id: &str, response: &Response, inactive_state: bool, terminal: bool) {
    let capture: Value = serde_json::from_str(include_str!("../../../conformance/reference/recordings/2026-10-01-lifecycle/lifecycle-observations.json")).unwrap();
    let reference = &capture["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == id)
        .unwrap()["response"];
    assert_eq!(
        u64::from(response.status),
        reference["status"].as_u64().unwrap(),
        "selected captured status for {id}"
    );
    if inactive_state || terminal {
        let actual = frames(&response.body);
        let recorded = reference["sse_frames"].as_array().unwrap();
        if inactive_state {
            for field in [
                "anchor",
                "input",
                "live",
                "resume",
                "run_started_at",
                "status",
                "turn",
            ] {
                assert_eq!(
                    actual[0].1[field], recorded[0]["data"][field],
                    "selected inactive field {field} for {id}"
                );
            }
        }
        if terminal {
            assert_eq!(actual.last().unwrap().0, "done");
            assert_eq!(
                actual.last().unwrap().1,
                recorded.last().unwrap()["data"],
                "exact selected done for {id}"
            );
        }
    }
}

fn inactive(response: &Response) -> Value {
    assert_eq!(response.status, 200);
    assert!(response.headers.to_ascii_lowercase().contains("no-store"));
    let events = frames(&response.body);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, "state");
    assert_eq!(events[0].1["live"], false);
    assert_eq!(events[0].1["turn"], Value::Null);
    assert_eq!(events[1], ("done".into(), json!({"output":null})));
    let normalized = response.body.replace("\r\n", "\n");
    let blocks: Vec<_> = normalized
        .split("\n\n")
        .filter(|block| {
            !block.is_empty()
                && !block
                    .lines()
                    .all(|line| line.is_empty() || line.starts_with(':'))
        })
        .collect();
    assert_eq!(blocks.len(), 3);
    assert!(
        blocks[1] == "retry: 2000",
        "Inactive retry block must follow state and precede done"
    );
    assert!(
        ![blocks[0], blocks[2]]
            .iter()
            .any(|block| block.lines().any(|line| line.starts_with("retry:"))),
        "Inactive retry must occupy a separate block"
    );
    events[0].1.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_attachment_idle_missing_and_foreign_are_protected() {
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new(
        store,
        Arc::new(Auth),
        Arc::new(Allow),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let missing = exchange(address, "GET", "/threads/missing/runs/stream", "").await;
    assert_eq!(missing.status, 404);
    selected_reference("missing.attach", &missing, false, false);
    let id = create(address).await;
    let path = format!("/threads/{id}/runs/stream");
    let idle = exchange(address, "GET", &path, "").await;
    assert_eq!(idle.status, 409);
    selected_reference("idle.attach", &idle, false, false);
    assert_eq!(
        serde_json::from_str::<Value>(&idle.body).unwrap()["error"]["details"]["code"],
        "thread_route_unknown"
    );
    let foreign = finish(open_as(address, "GET", &path, "", "foreign").await, vec![]).await;
    assert_eq!(foreign.status, 404);
    assert!(!foreign.body.contains("thread_route_unknown"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_inactive_attachment_reads_durable_values_and_null_terminal() {
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let run = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/attach#workflow","input":{"snow":"雪"}}"#,
    )
    .await;
    assert_eq!(run.status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&run.body).unwrap(),
        "actual-output"
    );
    let captured = store
        .thread_view(&ThreadId::new(id.as_str()).unwrap())
        .unwrap()
        .unwrap();
    let response = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    let state = inactive(&response);
    selected_reference("completed.attach", &response, true, true);
    assert_eq!(state["status"], "idle");
    assert_eq!(state["input"], Value::Null);
    assert_eq!(state["run_started_at"], Value::Null);
    assert_eq!(state["values"]["calls"], json!(["actual-output"]));
    assert_eq!(state["values"]["input"]["snow"], "雪");
    assert_eq!(
        captured,
        store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .unwrap()
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_live_attachment_finishes_cancelled_before_worker_quiescence() {
    let store = Arc::new(MemoryStore::new());
    let mut cfg = config(route(false, 2500));
    cfg.max_active_runs = 1;
    let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Allow), cfg).unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/attach#workflow","input":"live"}"#,
    )
    .await;
    until(|| inflight(&*store, &id)).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut attached = open(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    let start = prefix(&mut attached, b"event: state\n").await;
    assert_eq!(
        exchange(address, "POST", &format!("/threads/{id}/cancel"), "")
            .await
            .status,
        200
    );
    let response = tokio::time::timeout(Duration::from_millis(800), finish(attached, start))
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    selected_reference("stream-cancel.attach", &response, false, true);
    let events = frames(&response.body);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].1["live"], true);
    assert_eq!(
        events[1],
        ("done".into(), json!({"output":{"cancelled":true}}))
    );
    assert_eq!(finish(producer, vec![]).await.status, 409);
    let snapshot = store
        .thread_view(&ThreadId::new(id.as_str()).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::CancelRequested);
    assert!(snapshot.tasks[0].result.is_none());
    let after = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    assert_eq!(inactive(&after)["status"], "busy");
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
            "/threads/other/runs/wait",
            r#"{"route":"/attach#workflow"}"#
        )
        .await
        .status,
        503
    );
    until(|| {
        store
            .execution(&snapshot.execution_id)
            .unwrap()
            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Cancelled)
    })
    .await;
    let settled = store.execution(&snapshot.execution_id).unwrap().unwrap();
    assert_eq!(settled.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert!(!settled.snapshot.values.contains_key("calls"));
    assert!(settled.snapshot.output.is_none());
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
    let _ = server.await;
}

fn chunked_route(delay: u64) -> ScriptRoute {
    let mut r = route(false, delay);
    r.definition.max_attempts = 3;
    let early = TaskSpec {
        id: TaskId::new("early").unwrap(),
        effect_id: EffectId::new("early").unwrap(),
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    };
    let tail = TaskSpec {
        id: TaskId::new("tail").unwrap(),
        effect_id: EffectId::new("tail").unwrap(),
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    };
    r.definition.batches.insert(0, vec![early]);
    r.definition.batches.push(vec![tail]);
    for (name, chunk) in [
        ("early", json!({"early":"雪"})),
        ("tail", json!({"tail":"é"})),
    ] {
        r.effects.insert(
            EffectId::new(name).unwrap(),
            ScriptEffect::Chunks {
                chunks: vec![chunk],
                result: TaskResult {
                    output: json!(name),
                    writes: vec![],
                },
            },
        );
    }
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_attachment_cut_contains_earlier_chunks_and_only_later_tail() {
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(chunked_route(500)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/attach#workflow","input":[1,null,"雪"]}"#,
    )
    .await;
    until(|| {
        store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .and_then(|v| v.execution)
            .is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.spec.id.as_str() == "task" && t.status == TaskStatus::InFlight)
            })
    })
    .await;
    let response = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    assert_eq!(response.status, 200);
    let events = frames(&response.body);
    selected_reference("live.attach", &response, false, false);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].0, "state");
    assert_eq!(events[0].1["live"], true);
    assert_eq!(events[0].1["input"], json!([1, null, "雪"]));
    assert_eq!(
        events[0].1["turn"],
        json!([{"event":"chunk","data":{"early":"雪"}}])
    );
    assert_eq!(events[1], ("chunk".into(), json!({"tail":"é"})));
    assert_eq!(events[2], ("done".into(), json!({"output":"tail"})));
    assert_eq!(finish(producer, vec![]).await.status, 200);
    assert!(!response.body.contains("\nid:"));
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_attachment_capacity_and_disconnect_affect_only_viewers() {
    let store = Arc::new(MemoryStore::new());
    let mut cfg = config(route(false, 1800));
    cfg.max_attached_viewers = 1;
    let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Allow), cfg).unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/attach#workflow"}"#,
    )
    .await;
    until(|| inflight(&*store, &id)).await;
    let path = format!("/threads/{id}/runs/stream");
    let mut first = open(address, "GET", &path, "").await;
    let _ = prefix(&mut first, b"event: state\n").await;
    let full = exchange(address, "GET", &path, "").await;
    assert_eq!(full.status, 200);
    assert_eq!(
        frames(&full.body),
        vec![("detached".into(), json!({"reason":"capacity"}))]
    );
    assert_eq!(
        exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            r#"{"route":"/attach#workflow"}"#
        )
        .await
        .status,
        409
    );
    assert!(inflight(&*store, &id));
    drop(first);
    let mut replacement = tokio::time::timeout(Duration::from_millis(800), async {
        loop {
            let mut next = open(address, "GET", &path, "").await;
            let bytes = prefix(&mut next, b"event: ").await;
            if bytes
                .windows(b"event: state\n".len())
                .any(|w| w == b"event: state\n")
            {
                break (next, bytes);
            }
            assert!(bytes
                .windows(b"event: detached\n".len())
                .any(|w| w == b"event: detached\n"));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(inflight(&*store, &id));
    assert_eq!(
        exchange(address, "POST", &format!("/threads/{id}/cancel"), "")
            .await
            .status,
        200
    );
    let response = finish(replacement.0, std::mem::take(&mut replacement.1)).await;
    assert_eq!(
        frames(&response.body).last().unwrap(),
        &("done".into(), json!({"output":{"cancelled":true}}))
    );
    assert_eq!(finish(producer, vec![]).await.status, 409);
    until(|| !inflight(&*store, &id)).await;
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
    let _ = server.await;
}

struct PolicyGate {
    target: usize,
    calls: AtomicUsize,
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
}
impl PolicyGate {
    fn new(target: usize) -> Self {
        Self {
            target,
            calls: AtomicUsize::new(0),
            entered: AtomicBool::new(false),
            released: Mutex::new(false),
            wake: Condvar::new(),
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
impl ResourcePolicy for PolicyGate {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::Attach
            && self.calls.fetch_add(1, Ordering::AcqRel) == self.target
        {
            self.entered.store(true, Ordering::Release);
            let lock = self.released.lock().unwrap();
            let (released, _) = self
                .wake
                .wait_timeout_while(lock, Duration::from_secs(5), |done| !*done)
                .unwrap();
            assert!(*released, "attachment policy gate not released");
        }
        true
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_attachment_policy_wait_cannot_retarget_successor() {
    for target in [0, 1] {
        let store = Arc::new(MemoryStore::new());
        let gate = Arc::new(PolicyGate::new(target));
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth),
            gate.clone(),
            config(route(false, 500)),
        )
        .unwrap();
        let (address, server) = serve(&host).await;
        let id = create(address).await;
        let path = format!("/threads/{id}");
        let producer = open(
            address,
            "POST",
            &format!("{path}/runs/wait"),
            r#"{"route":"/attach#workflow"}"#,
        )
        .await;
        until(|| inflight(&*store, &id)).await;
        let held = open(address, "GET", &format!("{path}/runs/stream"), "").await;
        until(|| gate.entered.load(Ordering::Acquire)).await;
        assert_eq!(finish(producer, vec![]).await.status, 200);
        let first = store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .execution_id;
        let mut successor = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut next = open(
                    address,
                    "POST",
                    &format!("{path}/runs/stream"),
                    r#"{"route":"/attach#workflow"}"#,
                )
                .await;
                let bytes = prefix(&mut next, b"\r\n\r\n").await;
                if String::from_utf8_lossy(&bytes).starts_with("HTTP/1.1 200") {
                    break (next, bytes);
                }
                let rejected = finish(next, bytes).await;
                assert_eq!(rejected.status, 409);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let current = store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap();
        assert_ne!(current.snapshot.execution_id, first);
        gate.release();
        let stale = finish(held, vec![]).await;
        assert_eq!(stale.status, 409);
        assert_eq!(
            serde_json::from_str::<Value>(&stale.body).unwrap()["error"]["details"]["code"],
            "resource_changed"
        );
        assert!(!stale.body.contains("event: state"));
        let response = finish(successor.0, std::mem::take(&mut successor.1)).await;
        assert_eq!(response.status, 200);
        assert_eq!(
            frames(&response.body).last().unwrap(),
            &("done".into(), json!({"output":"actual-output"}))
        );
        assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
        server.abort();
        let _ = server.await;
    }
}

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("b4-attachment-tcp-{}", uuid::Uuid::new_v4())))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn approval_config() -> ApprovalHostConfig {
    ApprovalHostConfig::new(
        "policy-v1".into(),
        BTreeMap::from([(
            RouteKey::new("/attach#workflow").unwrap(),
            BTreeMap::from([(TaskId::new("task").unwrap(), json!({"question":"approve?"}))]),
        )]),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_sqlite_parked_attachment_after_reopen_preserves_authority() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(route(true, 0)),
        approval_config(),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let response = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/attach#workflow","input":"parked"}"#,
    )
    .await;
    assert_eq!(response.status, 200);
    let captured = store
        .thread_view(&ThreadId::new(id.as_str()).unwrap())
        .unwrap()
        .unwrap();
    let execution_id = captured
        .execution
        .as_ref()
        .unwrap()
        .snapshot
        .execution_id
        .clone();
    let sensitive = store.approval_view(&execution_id).unwrap().unwrap();
    let before_response = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    selected_reference("parked.attach", &before_response, true, true);
    let before = inactive(&before_response);
    assert_eq!(before["status"], "interrupted");
    assert_eq!(before["interrupts"].as_array().unwrap().len(), 1);
    assert_eq!(
        store.approval_view(&execution_id).unwrap().unwrap(),
        sensitive
    );
    let foreign = finish(
        open_as(
            address,
            "GET",
            &format!("/threads/{id}/runs/stream"),
            "",
            "foreign",
        )
        .await,
        vec![],
    )
    .await;
    selected_reference("parked.foreign-attach", &foreign, false, false);
    assert_eq!(foreign.status, 404);
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    let reopened = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory.0) {
                Ok(store) => break Arc::new(store),
                Err(StoreError::AlreadyOwned) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("unexpected reopen error: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.owner_generation(), 2);
    let host = HttpHost::new_with_approvals(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(route(true, 0)),
        approval_config(),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let after_response = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    selected_reference("parked.attach-after-reopen", &after_response, true, true);
    let after = inactive(&after_response);
    assert!(
        after["interrupts"] == before["interrupts"],
        "Parked grant projection changed after reopen"
    );
    assert_eq!(after["values"], before["values"]);
    assert_eq!(after["status"], "interrupted");
    assert_eq!(
        reopened
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .unwrap(),
        captured
    );
    assert_eq!(
        reopened.approval_view(&execution_id).unwrap().unwrap(),
        sensitive
    );
    assert_eq!(reopened.owner_generation(), 2);
    assert_eq!(
        exchange(
            address,
            "GET",
            &format!("/threads/{id}/pending_interrupts"),
            ""
        )
        .await
        .status,
        501
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_sqlite_completed_attachment_after_reopen_has_null_done() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    assert_eq!(
        exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            r#"{"route":"/attach#workflow"}"#
        )
        .await
        .status,
        200
    );
    let captured = store
        .thread_view(&ThreadId::new(id.as_str()).unwrap())
        .unwrap()
        .unwrap();
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    let reopened = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory.0) {
                Ok(store) => break Arc::new(store),
                Err(StoreError::AlreadyOwned) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("unexpected reopen error: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    let host = HttpHost::new(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let response = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    selected_reference("completed.attach-after-reopen", &response, true, true);
    let state = inactive(&response);
    assert_eq!(state["values"]["calls"], json!(["actual-output"]));
    assert_eq!(state["interrupts"], json!([]));
    assert_eq!(
        reopened
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .unwrap(),
        captured
    );
    assert_eq!(reopened.owner_generation(), 2);
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_digest_utf8_overflow_marks_history_unavailable_and_preserves_tail() {
    let entry = json!({"event":"chunk","data":{"early":"雪"}});
    let bytes = serde_json::to_vec(&entry).unwrap().len();
    assert!(entry.to_string().chars().count() < bytes);
    let store = Arc::new(MemoryStore::new());
    let mut cfg = config(chunked_route(600));
    cfg.turn_buffer_bytes = bytes - 1;
    let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Allow), cfg).unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/attach#workflow"}"#,
    )
    .await;
    until(|| {
        store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .and_then(|v| v.execution)
            .is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.spec.id.as_str() == "task" && t.status == TaskStatus::InFlight)
            })
    })
    .await;
    let response = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    let events = frames(&response.body);
    assert_eq!(response.status, 200);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].1["turn"], Value::Null);
    assert_eq!(events[0].1["turn_truncated"], true);
    assert_eq!(events[1], ("chunk".into(), json!({"tail":"é"})));
    assert_eq!(events[2], ("done".into(), json!({"output":"tail"})));
    assert_eq!(finish(producer, vec![]).await.status, 200);
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_viewer_byte_overflow_detaches_only_attachment_and_settles_producer() {
    let store = Arc::new(MemoryStore::new());
    let mut cfg = config(chunked_route(600));
    cfg.viewer_queue_bytes = 1;
    cfg.max_attached_viewers = 1;
    let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Allow), cfg).unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let producer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/attach#workflow"}"#,
    )
    .await;
    until(|| {
        store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .and_then(|v| v.execution)
            .is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|t| t.spec.id.as_str() == "task" && t.status == TaskStatus::InFlight)
            })
    })
    .await;
    let response = exchange(address, "GET", &format!("/threads/{id}/runs/stream"), "").await;
    assert_eq!(response.status, 200);
    let events = frames(&response.body);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, "state");
    assert_eq!(events[1], ("detached".into(), json!({"reason":"overflow"})));
    assert_eq!(finish(producer, vec![]).await.status, 200);
    let stored = store
        .thread_view(&ThreadId::new(id.as_str()).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(stored.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(
        stored
            .snapshot
            .tasks
            .last()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .output,
        json!("tail")
    );
    assert_eq!(
        stored.snapshot.output.as_ref(),
        Some(&json!(stored.snapshot.values))
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_inactive_attachment_policy_wait_refuses_deleted_recreated_generation() {
    for target in [0, 1] {
        let store = Arc::new(MemoryStore::new());
        let gate = Arc::new(PolicyGate::new(target));
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth),
            gate.clone(),
            config(route(false, 0)),
        )
        .unwrap();
        let (address, server) = serve(&host).await;
        let id = create(address).await;
        let path = format!("/threads/{id}");
        assert_eq!(
            exchange(
                address,
                "POST",
                &format!("{path}/runs/wait"),
                r#"{"route":"/attach#workflow","input":"original"}"#
            )
            .await
            .status,
            200
        );
        let before = store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .unwrap();
        let held = open(address, "GET", &format!("{path}/runs/stream"), "").await;
        until(|| gate.entered.load(Ordering::Acquire)).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let deletion = exchange(address, "DELETE", &path, "").await;
                if deletion.status == 204 {
                    break;
                }
                assert_eq!(deletion.status, 409);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            exchange(
                address,
                "POST",
                &format!("{path}/runs/wait"),
                r#"{"route":"/attach#workflow","input":"replacement"}"#
            )
            .await
            .status,
            200
        );
        let replacement = store
            .thread_view(&ThreadId::new(id.as_str()).unwrap())
            .unwrap()
            .unwrap();
        assert_ne!(before.thread.generation, replacement.thread.generation);
        assert_eq!(
            replacement.execution.as_ref().unwrap().snapshot.values["input"],
            "replacement"
        );
        gate.release();
        let stale = finish(held, vec![]).await;
        assert_eq!(stale.status, 409);
        assert_eq!(
            serde_json::from_str::<Value>(&stale.body).unwrap()["error"]["details"]["code"],
            "resource_changed"
        );
        assert!(!stale.body.contains("event: state"));
        assert_eq!(
            store
                .thread_view(&ThreadId::new(id.as_str()).unwrap())
                .unwrap()
                .unwrap(),
            replacement
        );
        assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_preheader_disconnect_during_either_policy_wait_releases_viewer_slot() {
    for target in [0, 1] {
        let store = Arc::new(MemoryStore::new());
        let gate = Arc::new(PolicyGate::new(target));
        let mut cfg = config(route(false, 1800));
        cfg.max_attached_viewers = 1;
        let host = HttpHost::new(store.clone(), Arc::new(Auth), gate.clone(), cfg).unwrap();
        let (address, server) = serve(&host).await;
        let id = create(address).await;
        let path = format!("/threads/{id}");
        let producer = open(
            address,
            "POST",
            &format!("{path}/runs/wait"),
            r#"{"route":"/attach#workflow"}"#,
        )
        .await;
        until(|| inflight(&*store, &id)).await;
        let abandoned = open(address, "GET", &format!("{path}/runs/stream"), "").await;
        until(|| gate.entered.load(Ordering::Acquire)).await;
        drop(abandoned);
        let replacement = tokio::time::timeout(Duration::from_millis(800), async {
            loop {
                let mut next = open(address, "GET", &format!("{path}/runs/stream"), "").await;
                let bytes = prefix(&mut next, b"event: ").await;
                if bytes
                    .windows(b"event: state\n".len())
                    .any(|w| w == b"event: state\n")
                {
                    break (next, bytes);
                }
                let detached = finish(next, bytes).await;
                assert_eq!(detached.status, 200);
                assert_eq!(
                    frames(&detached.body),
                    vec![("detached".into(), json!({"reason":"capacity"}))]
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        // Capacity is available while the abandoned worker's policy is STILL
        // blocked; eventual policy cleanup cannot account for this reuse.
        assert!(!*gate.released.lock().unwrap());
        assert!(inflight(&*store, &id));
        gate.release();
        let response = finish(replacement.0, replacement.1).await;
        assert_eq!(response.status, 200);
        assert_eq!(
            frames(&response.body).last().unwrap(),
            &("done".into(), json!({"output":"actual-output"}))
        );
        assert_eq!(finish(producer, vec![]).await.status, 200);
        assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_tcp_recovered_publisher_marks_unavailable_prior_history_and_settles_once() {
    let directory = Directory::new();
    let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
    let thread = ThreadId::new("recovered").unwrap();
    let mut scripted = route(false, 700);
    scripted.definition.max_attempts = 2;
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: serde_json::Map::new(),
            origin: Some(ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    let accepted = store
        .admit(
            &thread,
            b4_kernel::new_execution(
                ExecutionId::new("prior-execution").unwrap(),
                TurnId::new("prior-turn").unwrap(),
                store.owner_generation(),
                scripted.definition.clone(),
                serde_json::Map::new(),
                TransitionFacts { now_ms: 2 },
            )
            .unwrap(),
        )
        .unwrap();
    let runtime = Runtime::new(&store);
    let running = runtime
        .apply(
            &accepted.snapshot.execution_id,
            (&accepted.snapshot).into(),
            KernelCommand::Start,
            TransitionFacts { now_ms: 3 },
        )
        .unwrap();
    runtime
        .apply(
            &running.snapshot.execution_id,
            (&running.snapshot).into(),
            KernelCommand::Dispatch {
                task_id: TaskId::new("task").unwrap(),
                attempt_id: AttemptId::new("prior-attempt").unwrap(),
            },
            TransitionFacts { now_ms: 4 },
        )
        .unwrap();
    drop(store);
    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let captured = reopened.thread_view(&thread).unwrap().unwrap();
    let target = RecoveryTarget::from_view(&captured).unwrap();
    let host = HttpHost::new_with_approvals(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(scripted),
        ApprovalHostConfig::new("policy-v1".into(), BTreeMap::new()),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let orphan = exchange(address, "GET", "/threads/recovered/runs/stream", "").await;
    assert_eq!(orphan.status, 409);
    assert_eq!(
        serde_json::from_str::<Value>(&orphan.body).unwrap()["error"]["details"]["code"],
        "recovery_required"
    );
    assert_eq!(reopened.thread_view(&thread).unwrap().unwrap(), captured);
    let primary = host
        .recover_execution(
            Principal {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            },
            target,
        )
        .await;
    assert_eq!(primary.status(), 200);
    let response = exchange(address, "GET", "/threads/recovered/runs/stream", "").await;
    assert_eq!(response.status, 200);
    let events = frames(&response.body);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, "state");
    assert_eq!(events[0].1["live"], true);
    assert_eq!(events[0].1["turn"], Value::Null);
    assert_eq!(events[0].1["turn_truncated"], true);
    assert_eq!(
        events[1],
        ("done".into(), json!({"output":"actual-output"}))
    );
    let primary_body = axum::body::to_bytes(primary.into_body(), 8192)
        .await
        .unwrap();
    assert_eq!(
        frames(std::str::from_utf8(&primary_body).unwrap()),
        vec![("done".into(), json!({"output":"actual-output"}))]
    );
    let durable = reopened
        .thread_view(&thread)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(durable.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(durable.snapshot.owner_generation, 2);
    assert_eq!(durable.snapshot.attempt_history.len(), 2);
    assert_eq!(
        durable.snapshot.attempt_history[0],
        AttemptId::new("prior-attempt").unwrap()
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.status, 200);
    server.abort();
    let _ = server.await;
}
