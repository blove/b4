use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
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
            Some("Bearer other") => "other",
            _ => return Err(AuthenticationError),
        };
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: principal.into(),
        })
    }
}
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        c.origin.as_ref().is_none_or(|o| {
            o.application == p.application && o.tenant == p.tenant && o.principal == p.principal
        })
    }
}
fn route(effect: ScriptEffect) -> ScriptRoute {
    let id = EffectId::new("effect").unwrap();
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/test#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: id.clone(),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            }]],
            max_attempts: 1,
        },
        effects: BTreeMap::from([(id, effect)]),
    }
}
async fn serve<S: Store + 'static>(
    host: &HttpHost<S>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("actual TCP tests require loopback bind permission");
    let address = listener.local_addr().unwrap();
    let router = host.router();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (address, server)
}
async fn open(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let auth = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n{auth}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    stream
}
async fn exchange(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (u16, String, String) {
    let mut stream = open(address, method, path, token, body).await;
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let raw = String::from_utf8(bytes).unwrap();
    let (headers, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    // Native JSON has Content-Length. SSE is chunked; assertions use the raw
    // independently read framing and do not invoke the native SSE encoder.
    (status, headers.into(), body.into())
}
async fn create(address: std::net::SocketAddr) -> String {
    let (status, _, body) = exchange(
        address,
        "POST",
        "/threads",
        Some("owner"),
        r#"{"metadata":{"note":"secret"}}"#,
    )
    .await;
    assert_eq!(status, 200);
    serde_json::from_str::<Value>(&body).unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .into()
}
async fn wait_status<S: Store + 'static>(host: &HttpHost<S>, id: &str, status: ExecutionStatus) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let view = host
                .store()
                .thread_view(&ThreadId::new(id).unwrap())
                .unwrap()
                .unwrap();
            if view.execution.is_some_and(|e| e.snapshot.status == status) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_tcp_auth_policy_projection_wait_and_stream() {
    let host = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Policy),
        HostConfig::new(vec![route(ScriptEffect::Input)]),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    assert_eq!(
        exchange(address, "POST", "/threads", None, "{}").await.0,
        401
    );
    let id = create(address).await;
    let denied = exchange(address, "GET", &format!("/threads/{id}"), Some("other"), "").await;
    assert_eq!(denied.0, 404);
    assert!(!denied.2.contains("secret"));
    assert_eq!(
        exchange(
            address,
            "GET",
            &format!("/threads/{id}/state"),
            Some("owner"),
            ""
        )
        .await
        .0,
        404
    );
    let wait = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        Some("owner"),
        r#"{"route":"/test#workflow","input":false}"#,
    )
    .await;
    assert_eq!(wait.0, 200);
    assert_eq!(wait.2, "false");
    let stream = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        Some("owner"),
        r#"{"route":"/test#workflow","input":0}"#,
    )
    .await;
    assert_eq!(stream.0, 200);
    assert!(stream.1.contains("text/event-stream"));
    assert!(stream.1.contains("no-store, no-transform"));
    assert_eq!(stream.2.matches("event: done").count(), 1);
    assert!(stream.2.contains("data: {\"output\":0}\n\n"));
    let state = exchange(
        address,
        "GET",
        &format!("/threads/{id}/state"),
        Some("owner"),
        "",
    )
    .await;
    let state: Value = serde_json::from_str(&state.2).unwrap();
    assert_eq!(state["config"]["configurable"]["thread_id"], id);
    assert!(state.get("schema_version").is_none());
    server.abort();
}

#[tokio::test]
async fn actual_tcp_viewer_disconnect_does_not_cancel_or_free_same_thread_slot() {
    for endpoint in ["wait", "stream"] {
        let host = HttpHost::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(Policy),
            HostConfig::new(vec![route(ScriptEffect::Delayed {
                duration_ms: 300,
                result: TaskResult {
                    output: json!("done"),
                    writes: vec![],
                },
            })]),
        )
        .unwrap();
        let (address, server) = serve(&host).await;
        let id = create(address).await;
        let viewer = open(
            address,
            "POST",
            &format!("/threads/{id}/runs/{endpoint}"),
            Some("owner"),
            r#"{"route":"/test#workflow"}"#,
        )
        .await;
        wait_status(&host, &id, ExecutionStatus::Running).await;
        drop(viewer);
        assert_eq!(
            exchange(
                address,
                "POST",
                &format!("/threads/{id}/runs/wait"),
                Some("owner"),
                r#"{"route":"/test#workflow"}"#
            )
            .await
            .0,
            409
        );
        wait_status(&host, &id, ExecutionStatus::Completed).await;
        let view = host
            .store()
            .thread_view(&ThreadId::new(id).unwrap())
            .unwrap()
            .unwrap();
        let s = view.execution.unwrap().snapshot;
        assert_eq!(s.attempts_used, 1);
        assert_eq!(s.tasks[0].result.as_ref().unwrap().output, json!("done"));
        server.abort();
    }
}

struct FaultStore {
    inner: MemoryStore,
    fail: AtomicBool,
    outcome_commit_attempted: AtomicBool,
}
impl Store for FaultStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
    }
    fn create_thread_with(&self, s: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(s)
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
    fn admit(&self, id: &ThreadId, s: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, s)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        r: u64,
        s: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, r, s)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        f: ExecutionFence,
        p: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let has_outcome = p
            .snapshot
            .tasks
            .iter()
            .any(|t| matches!(t.status, TaskStatus::Succeeded | TaskStatus::Failed));
        if has_outcome {
            self.outcome_commit_attempted.store(true, Ordering::Release);
        }
        if self.fail.load(Ordering::Acquire) && has_outcome {
            Err(StoreError::Unavailable("do not expose this secret".into()))
        } else {
            self.inner.commit(id, f, p)
        }
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}
#[tokio::test]
async fn actual_tcp_failed_result_commit_marks_host_unready_and_retains_in_flight() {
    let store = Arc::new(FaultStore {
        inner: MemoryStore::new(),
        fail: AtomicBool::new(false),
        outcome_commit_attempted: AtomicBool::new(false),
    });
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        HostConfig::new(vec![route(ScriptEffect::Input)]),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    store.fail.store(true, Ordering::Release);
    let wait = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        Some("owner"),
        r#"{"route":"/test#workflow","input":"value"}"#,
    )
    .await;
    assert_eq!(wait.0, 503);
    assert!(!wait.2.contains("secret"));
    assert_eq!(exchange(address, "GET", "/readyz", None, "").await.0, 503);
    assert_eq!(exchange(address, "GET", "/healthz", None, "").await.0, 200);
    let view = store
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap();
    let s = view.execution.unwrap().snapshot;
    assert_eq!(s.status, ExecutionStatus::Running);
    assert_eq!(s.tasks[0].status, TaskStatus::InFlight);
    assert!(s.tasks[0].result.is_none());
    server.abort();
}

#[tokio::test]
async fn actual_tcp_expected_failure_is_durable_healthy_idle_and_allows_successor() {
    let directory = std::env::temp_dir().join(format!("b4-http-failure-{}", uuid::Uuid::new_v4()));
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory).unwrap());
    let mut success = route(ScriptEffect::Input);
    success.definition.route = RouteKey::new("/success#workflow").unwrap();
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        HostConfig::new(vec![
            route(ScriptEffect::Failure("fixture execution failure".into())),
            success,
        ]),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.clone()).unwrap();
    let mut failures = Vec::new();
    for endpoint in ["wait", "stream"] {
        let (status, headers, body) = exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/{endpoint}"),
            Some("owner"),
            r#"{"route":"/test#workflow"}"#,
        )
        .await;
        if endpoint == "wait" {
            assert_eq!(status, 500);
            assert_eq!(
                serde_json::from_str::<Value>(&body).unwrap(),
                json!({"error":{"kind":"execution_error","message":"fixture execution failure"}})
            );
        } else {
            assert_eq!(status, 200);
            assert!(headers.contains("text/event-stream"));
            assert_eq!(body.matches("event: done\n").count(), 1);
            assert!(
                body.contains("data: {\"output\":{\"error\":\"fixture execution failure\"}}\n\n")
            );
        }
        let s = store
            .thread_view(&thread_id)
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot;
        assert_eq!(s.status, ExecutionStatus::Failed);
        assert_eq!(s.failure.as_deref(), Some("fixture execution failure"));
        assert!(s.output.is_none());
        assert_eq!(s.tasks[0].status, TaskStatus::Failed);
        assert_eq!(s.tasks[0].failure, s.failure);
        failures.push(s.execution_id);
        assert_eq!(exchange(address, "GET", "/readyz", None, "").await.0, 200);
        let thread = exchange(address, "GET", &format!("/threads/{id}"), Some("owner"), "").await;
        assert_eq!(
            serde_json::from_str::<Value>(&thread.2).unwrap()["status"],
            "idle"
        );
        let successor = exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            Some("owner"),
            r#"{"route":"/success#workflow","input":false}"#,
        )
        .await;
        assert_eq!(successor.0, 200);
        assert_eq!(successor.2, "false");
    }
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    let reopened = b4_sqlite::SqliteStore::open(&directory).unwrap();
    for id in failures {
        let s = reopened.execution(&id).unwrap().unwrap().snapshot;
        assert_eq!(s.status, ExecutionStatus::Failed);
        assert_eq!(s.failure.as_deref(), Some("fixture execution failure"));
        assert!(s.output.is_none());
    }
    assert_eq!(
        reopened
            .thread_view(&thread_id)
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::Completed
    );
    reopened.readiness().unwrap();
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn actual_tcp_failed_failure_commit_never_claims_known_failure_or_exposes_store_secret() {
    for endpoint in ["wait", "stream"] {
        let store = Arc::new(FaultStore {
            inner: MemoryStore::new(),
            fail: AtomicBool::new(false),
            outcome_commit_attempted: AtomicBool::new(false),
        });
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            HostConfig::new(vec![route(ScriptEffect::Failure(
                "expected application failure".into(),
            ))]),
        )
        .unwrap();
        let (address, server) = serve(&host).await;
        let id = create(address).await;
        store.fail.store(true, Ordering::Release);
        let (status, _, body) = exchange(
            address,
            "POST",
            &format!("/threads/{id}/runs/{endpoint}"),
            Some("owner"),
            r#"{"route":"/test#workflow"}"#,
        )
        .await;
        if endpoint == "wait" {
            assert_eq!(status, 503);
        } else {
            assert_eq!(status, 200);
            assert!(!body.contains("event: done"));
        }
        assert!(!body.contains("expected application failure"));
        assert!(!body.contains("secret"));
        assert!(
            store.outcome_commit_attempted.load(Ordering::Acquire),
            "the explicit outcome must reach the failing commit before the host faults"
        );
        assert_eq!(exchange(address, "GET", "/readyz", None, "").await.0, 503);
        assert_eq!(exchange(address, "GET", "/healthz", None, "").await.0, 200);
        let protected =
            exchange(address, "GET", &format!("/threads/{id}"), Some("owner"), "").await;
        assert_eq!(protected.0, 503);
        assert!(!protected.2.contains("secret"));
        let s = store
            .thread_view(&ThreadId::new(id).unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot;
        assert_eq!(s.status, ExecutionStatus::Running);
        assert!(s.failure.is_none());
        assert!(s.output.is_none());
        assert_eq!(s.tasks[0].status, TaskStatus::InFlight);
        assert!(s.tasks[0].failure.is_none());
        assert!(s.tasks[0].result.is_none());
        server.abort();
    }
}

#[tokio::test]
async fn actual_tcp_heartbeat_precedes_one_committed_done() {
    let mut config = HostConfig::new(vec![route(ScriptEffect::Delayed {
        duration_ms: 150,
        result: TaskResult {
            output: json!(true),
            writes: vec![],
        },
    })]);
    config.heartbeat_ms = 20;
    let host = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Policy),
        config,
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let (status, _, body) = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        Some("owner"),
        r#"{"route":"/test#workflow"}"#,
    )
    .await;
    assert_eq!(status, 200);
    let heartbeat = body
        .find(": ping\n\n")
        .expect("quiet real TCP stream emits heartbeat");
    let done = body
        .find("event: done\n")
        .expect("settled stream emits terminal done");
    assert!(heartbeat < done);
    assert_eq!(body.matches("event: done\n").count(), 1);
    assert!(body.contains("data: {\"output\":true}\n\n"));
    let s = host
        .store()
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(s.status, ExecutionStatus::Completed);
    assert_eq!(s.attempts_used, 1);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_disconnected_worker_retains_sqlite_owner_until_quiescence() {
    let directory =
        std::env::temp_dir().join(format!("b4-http-owned-worker-{}", uuid::Uuid::new_v4()));
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory).unwrap());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        HostConfig::new(vec![route(ScriptEffect::Delayed {
            duration_ms: 500,
            result: TaskResult {
                output: json!("durable result"),
                writes: vec![],
            },
        })]),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let thread_id = ThreadId::new(id.clone()).unwrap();
    let creation = store.thread_view(&thread_id).unwrap().unwrap().thread;
    let viewer = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        Some("owner"),
        r#"{"route":"/test#workflow"}"#,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let view = store.thread_view(&thread_id).unwrap().unwrap();
            if view.execution.is_some_and(|e| {
                e.snapshot
                    .tasks
                    .iter()
                    .any(|task| task.status == TaskStatus::InFlight)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(viewer);
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    assert!(
        matches!(
            b4_sqlite::SqliteStore::open(&directory),
            Err(StoreError::AlreadyOwned)
        ),
        "detached live worker retains the SQLite owner lock"
    );
    let reopened = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory) {
                Ok(store) => break store,
                Err(StoreError::AlreadyOwned) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("owner reopen failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    let view = reopened.thread_view(&thread_id).unwrap().unwrap();
    assert_eq!(view.thread.created_at_ms, creation.created_at_ms);
    assert_eq!(view.thread.metadata, creation.metadata);
    assert_eq!(view.thread.origin, creation.origin);
    let snapshot = view.execution.unwrap().snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Completed);
    assert_eq!(snapshot.attempts_used, 1);
    assert_eq!(snapshot.attempt_history.len(), 1);
    assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("durable result")
    );
    reopened.readiness().unwrap();
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}
