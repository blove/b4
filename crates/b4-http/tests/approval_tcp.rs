use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
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
struct Policy {
    pending: AtomicBool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        (c.operation != Operation::ReadPending || self.pending.load(Ordering::Acquire))
            && c.origin.as_ref().is_none_or(|o| {
                o.application == p.application && o.tenant == p.tenant && o.principal == p.principal
            })
    }
}
fn route(delay_ms: u64, sibling: bool) -> ScriptRoute {
    let required = TaskSpec {
        id: TaskId::new("approval").unwrap(),
        effect_id: EffectId::new("approved_effect").unwrap(),
        requires_approval: true,
        recovery: RecoveryPolicy::Pure,
    };
    let mut tasks = vec![];
    let mut effects = BTreeMap::new();
    if sibling {
        tasks.push(TaskSpec {
            id: TaskId::new("sibling").unwrap(),
            effect_id: EffectId::new("sibling_effect").unwrap(),
            requires_approval: false,
            recovery: RecoveryPolicy::Pure,
        });
        effects.insert(
            EffectId::new("sibling_effect").unwrap(),
            ScriptEffect::Result(TaskResult {
                output: json!("sibling"),
                writes: vec![StateWrite::Append {
                    field: "history".into(),
                    items: vec![json!("sibling")],
                }],
            }),
        );
    }
    tasks.push(required);
    effects.insert(
        EffectId::new("approved_effect").unwrap(),
        ScriptEffect::Delayed {
            duration_ms: delay_ms,
            result: TaskResult {
                output: json!("approved"),
                writes: vec![StateWrite::Append {
                    field: "history".into(),
                    items: vec![json!("approved")],
                }],
            },
        },
    );
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/approve#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            max_attempts: tasks.len() as u32,
            batches: vec![tasks],
        },
        effects,
    }
}
fn approvals(mode: ApprovalServiceConfig) -> ApprovalHostConfig {
    ApprovalHostConfig {
        policy_revision: "policy-v1".into(),
        ttl_ms: 30_000,
        prompts: BTreeMap::from([(
            RouteKey::new("/approve#workflow").unwrap(),
            BTreeMap::from([(
                TaskId::new("approval").unwrap(),
                json!({"question":"Approve this local fixture?"}),
            )]),
        )]),
        service_config: mode,
    }
}
fn memory_host(
    delay_ms: u64,
    sibling: bool,
    capacity: usize,
    pending: bool,
) -> HttpHost<MemoryStore> {
    let mut config = HostConfig::new(vec![route(delay_ms, sibling)]);
    config.max_active_runs = capacity;
    HttpHost::new_with_approvals(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Policy {
            pending: AtomicBool::new(pending),
        }),
        config,
        approvals(ApprovalServiceConfig::VolatileFixture),
    )
    .unwrap()
}
async fn serve<S: Store + 'static>(
    host: &HttpHost<S>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("actual TCP approval tests require loopback bind permission");
    let address = listener.local_addr().unwrap();
    let router = host.router();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (address, server)
}
async fn open(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    owner: &str,
    body: &str,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {owner}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    stream
}
async fn exchange(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    owner: &str,
    body: &str,
) -> (u16, String, String) {
    let mut stream = open(address, method, path, owner, body).await;
    let mut bytes = vec![];
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
    let (status, _, body) = exchange(address, "POST", "/threads", "owner", "{}").await;
    assert_eq!(status, 200);
    serde_json::from_str::<Value>(&body).unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .into()
}
async fn park(address: std::net::SocketAddr, id: &str, endpoint: &str) -> (u16, String, String) {
    exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/{endpoint}"),
        "owner",
        r#"{"route":"/approve#workflow","input":{"request":"fixture"}}"#,
    )
    .await
}
async fn pending(address: std::net::SocketAddr, id: &str) -> Value {
    let (status, headers, body) = exchange(
        address,
        "GET",
        &format!("/threads/{id}/pending_interrupts"),
        "owner",
        "",
    )
    .await;
    assert_eq!(status, 200);
    assert!(headers.contains("no-store"));
    serde_json::from_str::<Value>(&body).unwrap()["interrupts"][0].clone()
}
fn decision(interrupt: &Value, payload: &str) -> String {
    json!({"route":"/approve#workflow","resume":[{
        "status":"resolved", "interruptId":interrupt["interruptId"],
        "payload":payload, "grant":interrupt["grant"],
    }]})
    .to_string()
}
async fn wait_status<S: Store + 'static>(
    host: &HttpHost<S>,
    id: &str,
    status: ExecutionStatus,
) -> Snapshot {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let view = host
                .store()
                .thread_view(&ThreadId::new(id).unwrap())
                .unwrap()
                .unwrap();
            if let Some(execution) = view.execution {
                if execution.snapshot.status == status {
                    break execution.snapshot;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn actual_tcp_wait_park_once_preserves_committed_sibling_without_replay() {
    let host = memory_host(0, true, 2, true);
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let parked = park(address, &id, "wait").await;
    assert_eq!(parked.0, 200);
    assert_eq!(parked.2, "null");
    assert!(!parked.2.contains("b4ag_"));
    let before = wait_status(&host, &id, ExecutionStatus::WaitingForApproval).await;
    assert_eq!(before.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(before.attempts_used, 1);
    let interrupt = pending(address, &id).await;
    assert_eq!(
        interrupt["value"],
        json!({"question":"Approve this local fixture?"})
    );
    assert!(interrupt["grant"].as_str().unwrap().starts_with("b4ag_"));
    let state = exchange(address, "GET", &format!("/threads/{id}/state"), "owner", "").await;
    assert_eq!(state.0, 200);
    assert!(!state.2.contains("b4ag_"));
    let resumed = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&interrupt, "once"),
    )
    .await;
    assert_eq!(resumed.0, 200);
    assert_eq!(resumed.2.matches("event: done").count(), 1);
    assert!(resumed.2.contains("data: {\"output\":\"approved\"}"));
    assert!(!resumed.2.contains("b4ag_"));
    let after = wait_status(&host, &id, ExecutionStatus::Completed).await;
    assert_eq!(after.attempts_used, 2);
    assert_eq!(after.tasks[0].attempt_id, before.tasks[0].attempt_id);
    assert_eq!(after.values["history"], json!(["sibling", "approved"]));
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

#[tokio::test]
async fn actual_tcp_stream_park_emits_authorized_interrupt_then_one_null_done() {
    let host = memory_host(0, false, 1, true);
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let stream = park(address, &id, "stream").await;
    assert_eq!(stream.0, 200);
    assert!(stream.1.contains("text/event-stream"));
    assert!(stream.1.contains("no-store"));
    assert_eq!(stream.2.matches("event: interrupt").count(), 1);
    assert_eq!(stream.2.matches("event: done").count(), 1);
    assert!(stream.2.find("event: interrupt").unwrap() < stream.2.find("event: done").unwrap());
    assert!(stream.2.contains("data: {\"output\":null}"));
    let interrupt = pending(address, &id).await;
    assert!(stream.2.contains(interrupt["grant"].as_str().unwrap()));
    assert_eq!(interrupt.as_object().unwrap().len(), 4);
    assert!(interrupt["resumeKey"].is_string());
    server.abort();
}

#[tokio::test]
async fn actual_tcp_wait_park_does_not_require_sensitive_pending_read_permission() {
    let host = memory_host(0, false, 1, false);
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let wait = park(address, &id, "wait").await;
    assert_eq!(wait.0, 200);
    assert_eq!(wait.2, "null");
    assert_eq!(
        exchange(
            address,
            "GET",
            &format!("/threads/{id}/pending_interrupts"),
            "owner",
            ""
        )
        .await
        .0,
        404
    );
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

#[tokio::test]
async fn actual_tcp_denial_and_cancelled_decision_complete_without_callback() {
    for cancelled in [false, true] {
        let host = memory_host(0, false, 1, true);
        let (address, server) = serve(&host).await;
        let id = create(address).await;
        assert_eq!(park(address, &id, "wait").await.0, 200);
        let interrupt = pending(address, &id).await;
        let body = if cancelled {
            json!({"route":"/approve#workflow","resume":[{"status":"cancelled","interruptId":interrupt["interruptId"],"grant":interrupt["grant"]}]}).to_string()
        } else {
            decision(&interrupt, "deny")
        };
        let resumed = exchange(
            address,
            "POST",
            &format!("/threads/{id}/resume"),
            "owner",
            &body,
        )
        .await;
        assert_eq!(resumed.0, 200);
        assert!(resumed.2.contains("data: {\"output\":{\"denied\":true}}"));
        assert_eq!(resumed.2.matches("event: done").count(), 1);
        assert!(!resumed.2.contains("b4ag_"));
        let snapshot = wait_status(&host, &id, ExecutionStatus::Completed).await;
        assert_eq!(snapshot.attempts_used, 0);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Denied);
        assert!(snapshot.tasks[0].result.is_none());
        assert_eq!(
            exchange(address, "GET", "/readyz", "owner", "").await.0,
            200
        );
        server.abort();
    }
}

#[tokio::test]
async fn actual_tcp_resume_disconnect_retains_worker_thread_claim_and_capacity() {
    let host = memory_host(300, false, 1, true);
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let other_id = create(address).await;
    assert_eq!(park(address, &id, "wait").await.0, 200);
    let interrupt = pending(address, &id).await;
    let viewer = open(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&interrupt, "once"),
    )
    .await;
    let running = wait_status(&host, &id, ExecutionStatus::Running).await;
    assert_eq!(running.tasks[0].status, TaskStatus::InFlight);
    drop(viewer);
    let duplicate = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&interrupt, "once"),
    )
    .await;
    assert_ne!(duplicate.0, 200);
    assert!(!duplicate.2.contains("b4ag_"));
    let capacity = park(address, &other_id, "wait").await;
    assert_eq!(capacity.0, 503);
    assert!(capacity.2.contains("capacity_exhausted"));
    let completed = wait_status(&host, &id, ExecutionStatus::Completed).await;
    assert_eq!(completed.attempts_used, 1);
    assert_eq!(park(address, &other_id, "wait").await.0, 200);
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

#[tokio::test]
async fn actual_tcp_concurrent_resume_has_one_winner_and_one_committed_attempt() {
    let host = memory_host(150, false, 4, true);
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    assert_eq!(park(address, &id, "wait").await.0, 200);
    let interrupt = pending(address, &id).await;
    let path = format!("/threads/{id}/resume");
    let body = decision(&interrupt, "once");
    let (a, b) = tokio::join!(
        exchange(address, "POST", &path, "owner", &body),
        exchange(address, "POST", &path, "owner", &body)
    );
    assert_eq!([a.0, b.0].into_iter().filter(|s| *s == 200).count(), 1);
    let rejected = if a.0 == 200 { b } else { a };
    assert_eq!(rejected.0, 409);
    assert!(!rejected.2.contains("b4ag_"));
    let completed = wait_status(&host, &id, ExecutionStatus::Completed).await;
    assert_eq!(completed.attempts_used, 1);
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

#[tokio::test]
async fn actual_tcp_capacity_rejection_preserves_pending_grant_for_retry() {
    let host = memory_host(250, false, 1, true);
    let (address, server) = serve(&host).await;
    let first = create(address).await;
    let second = create(address).await;
    assert_eq!(park(address, &first, "wait").await.0, 200);
    assert_eq!(park(address, &second, "wait").await.0, 200);
    let first_interrupt = pending(address, &first).await;
    let second_interrupt = pending(address, &second).await;
    let first_viewer = open(
        address,
        "POST",
        &format!("/threads/{first}/resume"),
        "owner",
        &decision(&first_interrupt, "once"),
    )
    .await;
    wait_status(&host, &first, ExecutionStatus::Running).await;
    let before = host
        .store()
        .thread_view(&ThreadId::new(&second).unwrap())
        .unwrap()
        .unwrap();
    let rejected = exchange(
        address,
        "POST",
        &format!("/threads/{second}/resume"),
        "owner",
        &decision(&second_interrupt, "once"),
    )
    .await;
    assert_eq!(rejected.0, 503);
    assert!(rejected.2.contains("capacity_exhausted"));
    assert_eq!(
        host.store()
            .thread_view(&ThreadId::new(&second).unwrap())
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(pending(address, &second).await, second_interrupt);
    drop(first_viewer);
    wait_status(&host, &first, ExecutionStatus::Completed).await;
    let accepted = exchange(
        address,
        "POST",
        &format!("/threads/{second}/resume"),
        "owner",
        &decision(&second_interrupt, "once"),
    )
    .await;
    assert_eq!(accepted.0, 200);
    assert_eq!(
        wait_status(&host, &second, ExecutionStatus::Completed)
            .await
            .attempts_used,
        1
    );
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

#[tokio::test]
async fn actual_tcp_postadmission_pending_policy_denial_closes_without_done_and_stays_healthy() {
    let host = memory_host(0, false, 1, false);
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let rejected = park(address, &id, "stream").await;
    assert_eq!(rejected.0, 200);
    assert!(!rejected.2.contains("event: interrupt"));
    assert!(!rejected.2.contains("event: done"));
    assert!(!rejected.2.contains("b4ag_"));
    let snapshot = wait_status(&host, &id, ExecutionStatus::WaitingForApproval).await;
    assert_eq!(snapshot.attempts_used, 0);
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    let next = create(address).await;
    assert_eq!(park(address, &next, "wait").await.0, 200);
    server.abort();
}

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("b4-http-approval-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn sqlite_host(store: Arc<b4_sqlite::SqliteStore>) -> HttpHost<b4_sqlite::SqliteStore> {
    HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        Arc::new(Policy {
            pending: AtomicBool::new(true),
        }),
        HostConfig::new(vec![route(0, true)]),
        approvals(ApprovalServiceConfig::RequiredDurable),
    )
    .unwrap()
}

#[tokio::test]
async fn actual_tcp_sqlite_restored_park_requires_explicit_owner_recovery_then_resumes() {
    let directory = Directory::new();
    let host = sqlite_host(Arc::new(
        b4_sqlite::SqliteStore::open(&directory.0).unwrap(),
    ));
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    assert_eq!(park(address, &id, "wait").await.0, 200);
    let original_interrupt = pending(address, &id).await;
    let original = wait_status(&host, &id, ExecutionStatus::WaitingForApproval).await;
    server.abort();
    let _ = server.await;
    drop(host);

    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = sqlite_host(store.clone());
    let (address, server) = serve(&host).await;
    let closed = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&original_interrupt, "once"),
    )
    .await;
    assert_eq!(closed.0, 409);
    assert_eq!(
        serde_json::from_str::<Value>(&closed.2).unwrap()["error"]["message"],
        "recovery_required"
    );
    assert!(!closed.2.contains("b4ag_"));
    let restored = store.execution(&original.execution_id).unwrap().unwrap();
    assert_eq!(restored.snapshot, original);
    let recovered = Runtime::new(store.clone())
        .recover(
            &original.execution_id,
            ExecutionFence::from(&restored.snapshot),
            ExecutionRevision::new("v1").unwrap(),
            TurnId::new("recovery-turn").unwrap(),
            TransitionFacts {
                now_ms: restored.snapshot.updated_at_ms + 1,
            },
        )
        .unwrap();
    assert_eq!(
        recovered.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(pending(address, &id).await, original_interrupt);
    let resumed = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&original_interrupt, "once"),
    )
    .await;
    assert_eq!(resumed.0, 200);
    assert!(resumed.2.contains("data: {\"output\":\"approved\"}"));
    let completed = wait_status(&host, &id, ExecutionStatus::Completed).await;
    assert_eq!(completed.attempts_used, 2);
    assert_eq!(completed.tasks[0].attempt_id, original.tasks[0].attempt_id);
    assert_eq!(completed.values["history"], json!(["sibling", "approved"]));
    assert_eq!(
        store
            .approval_view(&original.execution_id)
            .unwrap()
            .unwrap()
            .acceptances
            .len(),
        1
    );
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

#[tokio::test]
async fn actual_tcp_resume_advances_to_new_required_batch_and_discloses_only_its_grant() {
    let mut scripted = route(0, false);
    let mut second = scripted.definition.batches[0][0].clone();
    second.id = TaskId::new("approval-2").unwrap();
    second.effect_id = EffectId::new("approved_effect_2").unwrap();
    scripted.effects.insert(
        second.effect_id.clone(),
        scripted.effects.values().next().unwrap().clone(),
    );
    scripted.definition.batches.push(vec![second]);
    scripted.definition.max_attempts = 2;
    let mut approval = approvals(ApprovalServiceConfig::VolatileFixture);
    approval.prompts.values_mut().next().unwrap().insert(
        TaskId::new("approval-2").unwrap(),
        json!({"question":"Approve the second batch?"}),
    );
    let host = HttpHost::new_with_approvals(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Policy {
            pending: AtomicBool::new(true),
        }),
        HostConfig::new(vec![scripted]),
        approval,
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    assert_eq!(park(address, &id, "wait").await.0, 200);
    let first = pending(address, &id).await;
    let resumed = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&first, "once"),
    )
    .await;
    assert_eq!(resumed.0, 200);
    assert_eq!(resumed.2.matches("event: interrupt").count(), 1);
    assert_eq!(resumed.2.matches("event: done").count(), 1);
    assert!(resumed.2.contains("data: {\"output\":null}"));
    assert!(!resumed.2.contains(first["grant"].as_str().unwrap()));
    let second = pending(address, &id).await;
    assert_ne!(first["interruptId"], second["interruptId"]);
    assert_eq!(
        second["value"],
        json!({"question":"Approve the second batch?"})
    );
    assert!(resumed.2.contains(second["grant"].as_str().unwrap()));
    let final_response = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&second, "once"),
    )
    .await;
    assert_eq!(final_response.0, 200);
    assert!(final_response.2.contains("data: {\"output\":\"approved\"}"));
    assert!(!final_response.2.contains("b4ag_"));
    let completed = wait_status(&host, &id, ExecutionStatus::Completed).await;
    assert_eq!(completed.attempts_used, 2);
    assert_eq!(completed.values["history"], json!(["approved", "approved"]));
    assert_eq!(
        host.store()
            .approval_view(&completed.execution_id)
            .unwrap()
            .unwrap()
            .acceptances
            .len(),
        2
    );
    server.abort();
}

struct RevokeAfterClaim {
    store: Arc<MemoryStore>,
}
impl ResourcePolicy for RevokeAfterClaim {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::Resume
            && context.thread_id.as_ref().is_some_and(|id| {
                self.store
                    .thread_view(id)
                    .unwrap()
                    .and_then(|view| view.execution)
                    .is_some_and(|execution| {
                        execution
                            .snapshot
                            .tasks
                            .iter()
                            .any(|task| task.status == TaskStatus::InFlight)
                    })
            })
        {
            return false;
        }
        context
            .origin
            .as_ref()
            .is_none_or(|origin| origin.principal == principal.principal)
    }
}
#[tokio::test]
async fn actual_tcp_postclaim_policy_revocation_keeps_inflight_without_callback_or_done() {
    let store = Arc::new(MemoryStore::new());
    let policy = Arc::new(RevokeAfterClaim {
        store: store.clone(),
    });
    let host = HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        policy.clone(),
        HostConfig::new(vec![route(0, false)]),
        approvals(ApprovalServiceConfig::VolatileFixture),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    assert_eq!(park(address, &id, "wait").await.0, 200);
    let interrupt = pending(address, &id).await;
    let resumed = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&interrupt, "once"),
    )
    .await;
    assert_eq!(resumed.0, 200);
    assert!(!resumed.2.contains("event: done"));
    assert!(!resumed.2.contains("b4ag_"));
    let running = wait_status(&host, &id, ExecutionStatus::Running).await;
    assert_eq!(running.tasks[0].status, TaskStatus::InFlight);
    assert!(running.tasks[0].result.is_none());
    assert_eq!(running.attempts_used, 1);
    let view = host
        .store()
        .approval_view(&running.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(view.acceptances.len(), 1);
    assert!(view.grants[0].consumed);
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

struct RevokeAfterApprovedTask {
    store: Arc<MemoryStore>,
}
impl ResourcePolicy for RevokeAfterApprovedTask {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::Resume
            && context.thread_id.as_ref().is_some_and(|id| {
                self.store
                    .thread_view(id)
                    .unwrap()
                    .and_then(|view| view.execution)
                    .is_some_and(|execution| {
                        execution.snapshot.values.get("history") == Some(&json!(["approved"]))
                    })
            })
        {
            return false;
        }
        context
            .origin
            .as_ref()
            .is_none_or(|origin| origin.principal == principal.principal)
    }
}

#[tokio::test]
async fn actual_tcp_ordinary_step_after_once_renews_policy_before_any_attempt_or_callback() {
    let store = Arc::new(MemoryStore::new());
    let mut scripted = route(0, false);
    scripted.definition.batches.push(vec![TaskSpec {
        id: TaskId::new("ordinary-after").unwrap(),
        effect_id: EffectId::new("ordinary-after-effect").unwrap(),
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    }]);
    scripted.definition.max_attempts = 2;
    scripted.effects.insert(
        EffectId::new("ordinary-after-effect").unwrap(),
        ScriptEffect::Result(TaskResult {
            output: json!("revoked_callback_ran"),
            writes: vec![StateWrite::Replace {
                field: "revoked_write".into(),
                value: json!(true),
            }],
        }),
    );
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(RevokeAfterApprovedTask { store }),
        HostConfig::new(vec![scripted]),
        approvals(ApprovalServiceConfig::VolatileFixture),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    assert_eq!(park(address, &id, "wait").await.0, 200);
    let interrupt = pending(address, &id).await;
    let resumed = exchange(
        address,
        "POST",
        &format!("/threads/{id}/resume"),
        "owner",
        &decision(&interrupt, "once"),
    )
    .await;
    assert_eq!(resumed.0, 200);
    assert!(!resumed.2.contains("event: done"));
    assert!(!resumed.2.contains("revoked_callback_ran"));
    assert!(!resumed.2.contains("b4ag_"));
    let current = host
        .store()
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(current.snapshot.status, ExecutionStatus::Running);
    assert_eq!(current.snapshot.attempts_used, 1);
    assert_eq!(current.snapshot.values["history"], json!(["approved"]));
    assert_eq!(current.snapshot.tasks.len(), 2);
    assert_eq!(current.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(current.snapshot.tasks[1].status, TaskStatus::Ready);
    assert!(current.snapshot.tasks[1].attempt_id.is_none());
    assert!(current.snapshot.tasks[1].result.is_none());
    assert!(!current.snapshot.values.contains_key("revoked_write"));
    let approvals = host
        .store()
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(approvals.acceptances.len(), 1);
    assert!(approvals.grants[0].consumed);
    assert_eq!(
        exchange(address, "GET", "/readyz", "owner", "").await.0,
        200
    );
    server.abort();
}

struct RevokeAfterRunAuthorization {
    revoked: AtomicBool,
}
impl ResourcePolicy for RevokeAfterRunAuthorization {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::RunWait {
            // The first check grants the request while revoking later checks,
            // modelling a policy change before its owned worker can admit work.
            !self.revoked.swap(true, Ordering::AcqRel)
        } else {
            true
        }
    }
}

#[tokio::test]
async fn actual_tcp_run_policy_revocation_before_worker_admission_changes_no_resource() {
    for existing in [false, true] {
        let host = HttpHost::new_with_approvals(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(RevokeAfterRunAuthorization {
                revoked: AtomicBool::new(false),
            }),
            HostConfig::new(vec![route(0, false)]),
            approvals(ApprovalServiceConfig::VolatileFixture),
        )
        .unwrap();
        let (address, server) = serve(&host).await;
        let id = if existing {
            create(address).await
        } else {
            "not-created".into()
        };
        let thread_id = ThreadId::new(&id).unwrap();
        let before = host.store().thread_view(&thread_id).unwrap();
        let rejected = park(address, &id, "wait").await;
        assert_eq!(rejected.0, 404);
        assert!(rejected.2.contains("resource_unavailable"));
        assert!(!rejected.2.contains("b4ag_"));
        assert_eq!(host.store().thread_view(&thread_id).unwrap(), before);
        assert_eq!(
            exchange(address, "GET", "/readyz", "owner", "").await.0,
            200
        );
        server.abort();
    }
}
