use axum::{http::HeaderMap, routing::post};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn principal() -> Principal {
    Principal {
        application: "app".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        },
        policy_revision: "policy-v1".into(),
    }
}
struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(principal())
    }
}
struct Policy {
    allow: AtomicBool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        self.allow.load(Ordering::Acquire)
    }
}
struct Authority;
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("b4-recover-tcp-{}", uuid::Uuid::new_v4())))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn route(required: bool, delay_ms: u64, max_attempts: u32) -> ScriptRoute {
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/recover#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            max_attempts,
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
                    output: json!("finished"),
                    writes: vec![StateWrite::Append {
                        field: "calls".into(),
                        items: vec![json!("finished")],
                    }],
                },
            },
        )]),
    }
}
fn host(
    store: Arc<b4_sqlite::SqliteStore>,
    scripted: ScriptRoute,
    policy: Arc<dyn ResourcePolicy>,
    capacity: usize,
) -> HttpHost<b4_sqlite::SqliteStore> {
    let required = scripted
        .definition
        .batches
        .iter()
        .flatten()
        .any(|task| task.requires_approval);
    let approval = ApprovalHostConfig::new(
        "policy-v1".into(),
        if required {
            BTreeMap::from([(
                scripted.definition.route.clone(),
                BTreeMap::from([(TaskId::new("task").unwrap(), json!({"question":"approve?"}))]),
            )])
        } else {
            BTreeMap::new()
        },
    );
    let mut config = HostConfig::new(vec![scripted]);
    config.max_active_runs = capacity;
    HttpHost::new_with_approvals(store, Arc::new(Auth), policy, config, approval).unwrap()
}
fn seed(store: &b4_sqlite::SqliteStore, scripted: &ScriptRoute, id: &str) -> StoredExecution {
    let id = ThreadId::new(id).unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    Runtime::new(store)
        .admit(
            &id,
            b4_kernel::new_execution(
                ExecutionId::new(format!("exec-{id}")).unwrap(),
                TurnId::new(format!("turn-{id}")).unwrap(),
                store.owner_generation(),
                scripted.definition.clone(),
                Map::new(),
                TransitionFacts { now_ms: 2 },
            )
            .unwrap(),
        )
        .unwrap()
}
fn apply(
    store: &b4_sqlite::SqliteStore,
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
fn target(store: &b4_sqlite::SqliteStore, id: &str) -> RecoveryTarget {
    RecoveryTarget::from_view(
        &store
            .thread_view(&ThreadId::new(id).unwrap())
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
fn fence(store: &b4_sqlite::SqliteStore, current: &StoredExecution) -> ApprovalFence {
    ApprovalFence {
        execution: ExecutionFence::from(&current.snapshot),
        resource_revision: store
            .thread(&current.thread_id)
            .unwrap()
            .unwrap()
            .resource_revision,
    }
}
async fn serve(
    host: &HttpHost<b4_sqlite::SqliteStore>,
    target: RecoveryTarget,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("actual TCP recovery tests require loopback bind permission");
    let address = listener.local_addr().unwrap();
    // Only this fixture exposes the trusted call over a local TCP route. The
    // production host router has no recovery endpoint or request DTO.
    let recovery = host.clone();
    let router = host.router().route(
        "/fixture/recover",
        post(move || {
            let recovery = recovery.clone();
            let target = target.clone();
            async move { recovery.recover_execution(principal(), target).await }
        }),
    );
    (
        address,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}
async fn open(address: std::net::SocketAddr, method: &str, path: &str, body: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    stream
}
async fn exchange(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) -> (u16, String) {
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
        body.to_owned(),
    )
}
async fn wait_status(
    host: &HttpHost<b4_sqlite::SqliteStore>,
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
async fn actual_tcp_trusted_recovery_continues_expired_committed_once_without_another_grant() {
    let directory = Directory::new();
    let scripted = route(true, 0, 2);
    let saved_target;
    let saved_authority;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        let initial = seed(&store, &scripted, "once");
        let running = apply(&store, &initial, KernelCommand::Start);
        let service = ApprovalService::new(store.clone(), Authority).unwrap();
        let parked = service
            .park(
                &context(),
                &running.snapshot.execution_id,
                fence(&store, &running),
                TaskId::new("task").unwrap(),
                InterruptId::new("original-interrupt").unwrap(),
                json!({"question":"approve?"}),
                1,
                TransitionFacts { now_ms: 4 },
            )
            .unwrap();
        let pending = service
            .pending(
                &context(),
                &parked.snapshot.execution_id,
                fence(&store, &parked),
            )
            .unwrap();
        service
            .accept(
                &parked.snapshot.execution_id,
                fence(&store, &parked),
                ApprovalAcceptanceRequest {
                    context: context(),
                    route: scripted.definition.route.clone(),
                    definition_revision: scripted.definition.revision.clone(),
                    turn_id: TurnId::new("accepted-before-restart").unwrap(),
                    now_ms: 4,
                    decisions: vec![GrantDecision {
                        interrupt_id: pending[0].interrupt_id.clone(),
                        token: Some(pending[0].token.clone()),
                        decision: ApprovalDecision::Once,
                        cancelled: false,
                    }],
                },
            )
            .unwrap();
        saved_target = target(&store, "once");
        saved_authority = store
            .approval_view(&saved_target.execution_id)
            .unwrap()
            .unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target.clone()).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 200);
    assert_eq!(result.1.matches("event: done").count(), 1);
    assert!(result.1.contains("data: {\"output\":\"finished\"}"));
    assert!(!result.1.contains("b4ag_"));
    let complete = wait_status(&host, "once", ExecutionStatus::Completed).await;
    assert_eq!(complete.values["calls"], json!(["finished"]));
    assert_eq!(complete.attempts_used, 1);
    assert_eq!(complete.definition.max_attempts, 2);
    let authority = store
        .approval_view(&complete.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(authority.grants, saved_authority.grants);
    assert_eq!(authority.acceptances, saved_authority.acceptances);
    let duplicate = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(duplicate.0, 409);
    assert!(!duplicate.1.contains("event: done"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_trusted_recovery_adopts_accepted_and_starts_it_once() {
    let directory = Directory::new();
    let scripted = route(false, 0, 1);
    let saved_target;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        seed(&store, &scripted, "accepted");
        saved_target = target(&store, "accepted");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store,
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 200);
    assert_eq!(result.1.matches("event: done").count(), 1);
    let complete = wait_status(&host, "accepted", ExecutionStatus::Completed).await;
    assert_eq!(complete.attempts_used, 1);
    assert_eq!(complete.values["calls"], json!(["finished"]));
    assert_eq!(complete.turn_history.len(), 2);
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_trusted_park_recovery_preserves_grant_identity_without_callback() {
    let directory = Directory::new();
    let scripted = route(true, 0, 1);
    let saved_target;
    let saved_authority;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        let initial = seed(&store, &scripted, "parked");
        let running = apply(&store, &initial, KernelCommand::Start);
        let service = ApprovalService::new(store.clone(), Authority).unwrap();
        service
            .park(
                &context(),
                &running.snapshot.execution_id,
                fence(&store, &running),
                TaskId::new("task").unwrap(),
                InterruptId::new("stable-interrupt").unwrap(),
                json!({"question":"approve?"}),
                30_000,
                TransitionFacts { now_ms: 4 },
            )
            .unwrap();
        saved_target = target(&store, "parked");
        saved_authority = store
            .approval_view(&saved_target.execution_id)
            .unwrap()
            .unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 200);
    assert_eq!(result.1.matches("event: interrupt").count(), 1);
    assert_eq!(result.1.matches("event: done").count(), 1);
    assert!(result.1.contains("stable-interrupt"));
    assert!(result.1.contains(saved_authority.grants[0].token.as_str()));
    assert!(result.1.contains("data: {\"output\":null}"));
    let parked = wait_status(&host, "parked", ExecutionStatus::WaitingForApproval).await;
    assert_eq!(parked.attempts_used, 0);
    assert!(!parked.values.contains_key("calls"));
    let authority = store.approval_view(&parked.execution_id).unwrap().unwrap();
    assert_eq!(authority.grants, saved_authority.grants);
    assert_eq!(authority.acceptances, saved_authority.acceptances);
    let pending = exchange(address, "GET", "/threads/parked/pending_interrupts", "").await;
    assert_eq!(pending.0, 200);
    assert!(pending.1.contains(saved_authority.grants[0].token.as_str()));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_recovery_settles_cancel_requested_without_callback() {
    let directory = Directory::new();
    let scripted = route(false, 0, 2);
    let saved_target;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        let initial = seed(&store, &scripted, "cancelled");
        let running = apply(&store, &initial, KernelCommand::Start);
        let claimed = apply(
            &store,
            &running,
            KernelCommand::Dispatch {
                task_id: TaskId::new("task").unwrap(),
                attempt_id: AttemptId::new("old-attempt").unwrap(),
            },
        );
        apply(
            &store,
            &claimed,
            KernelCommand::Cancel {
                turn_id: claimed.snapshot.turn_id.clone(),
                owner_generation: claimed.snapshot.owner_generation,
            },
        );
        saved_target = target(&store, "cancelled");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store,
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 200);
    assert_eq!(result.1.matches("event: done").count(), 1);
    assert!(result.1.contains("data: {\"output\":{\"cancelled\":true}}"));
    let settled = wait_status(&host, "cancelled", ExecutionStatus::Cancelled).await;
    assert_eq!(settled.attempts_used, 1);
    assert!(!settled.values.contains_key("calls"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_recovery_budget_exhaustion_adopts_without_callback_and_stays_healthy() {
    let directory = Directory::new();
    let scripted = route(false, 0, 1);
    let saved_target;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        let initial = seed(&store, &scripted, "exhausted");
        let running = apply(&store, &initial, KernelCommand::Start);
        apply(
            &store,
            &running,
            KernelCommand::Dispatch {
                task_id: TaskId::new("task").unwrap(),
                attempt_id: AttemptId::new("old-attempt").unwrap(),
            },
        );
        saved_target = target(&store, "exhausted");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target.clone()).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 409);
    assert!(result.1.contains("recovery_attempt_limit"));
    assert!(!result.1.contains("event: done"));
    let current = store
        .execution(&saved_target.execution_id)
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(current.owner_generation, store.owner_generation());
    assert_eq!(current.attempts_used, 1);
    assert_eq!(
        current.attempt_history,
        vec![AttemptId::new("old-attempt").unwrap()]
    );
    assert_eq!(current.tasks[0].status, TaskStatus::Ready);
    assert!(!current.values.contains_key("calls"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_disconnected_recovery_retains_capacity_claim_and_store_owner() {
    let directory = Directory::new();
    let scripted = route(false, 500, 1);
    let first_target;
    let other_target;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        seed(&store, &scripted, "first");
        seed(&store, &scripted, "other");
        first_target = target(&store, "first");
        other_target = target(&store, "other");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, first_target.clone()).await;
    let mut stream = open(address, "POST", "/fixture/recover", "").await;
    let mut header = [0_u8; 1024];
    assert!(stream.read(&mut header).await.unwrap() > 0);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let current = store
                .execution(&first_target.execution_id)
                .unwrap()
                .unwrap();
            if current
                .snapshot
                .tasks
                .first()
                .is_some_and(|task| task.status == TaskStatus::InFlight)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let before = store.thread_view(&other_target.thread_id).unwrap().unwrap();
    let rejected = host.recover_execution(principal(), other_target).await;
    assert_eq!(
        rejected.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap().unwrap(),
        before
    );
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    let complete = wait_status(&host, "first", ExecutionStatus::Completed).await;
    assert_eq!(complete.values["calls"], json!(["finished"]));
    assert_eq!(complete.attempts_used, 1);
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_recovery_adopts_existing_blocked_disposition_without_successful_stream() {
    let directory = Directory::new();
    let scripted = route(false, 0, 1);
    let saved_target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        seed(&store, &scripted, "blocked");
    }
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        let current = store
            .execution(&ExecutionId::new("exec-blocked").unwrap())
            .unwrap()
            .unwrap();
        Runtime::new(&store)
            .recover(
                &current.snapshot.execution_id,
                ExecutionFence::from(&current.snapshot),
                ExecutionRevision::new("unavailable-revision").unwrap(),
                TurnId::new("previous-recovery").unwrap(),
                TransitionFacts { now_ms: 3 },
            )
            .unwrap();
        saved_target = target(&store, "blocked");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target.clone()).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 409);
    assert!(result.1.contains("recovery_blocked"));
    assert!(!result.1.contains("event: done"));
    let current = store
        .execution(&saved_target.execution_id)
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(current.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(current.owner_generation, store.owner_generation());
    assert_eq!(current.attempts_used, 0);
    assert!(!current.values.contains_key("calls"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

struct RevokeBeforeWorker {
    calls: AtomicUsize,
}
impl ResourcePolicy for RevokeBeforeWorker {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        context.operation != Operation::Recover || self.calls.fetch_add(1, Ordering::AcqRel) == 0
    }
}
#[tokio::test]
async fn actual_tcp_recovery_policy_revocation_before_owned_commit_changes_no_resource() {
    let directory = Directory::new();
    let scripted = route(false, 0, 1);
    let saved_target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        seed(&store, &scripted, "revoked");
        saved_target = target(&store, "revoked");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let before = store.thread_view(&saved_target.thread_id).unwrap().unwrap();
    let host = host(
        store.clone(),
        scripted,
        Arc::new(RevokeBeforeWorker {
            calls: AtomicUsize::new(0),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 404);
    assert!(result.1.contains("resource_unavailable"));
    assert!(!result.1.contains("event: done"));
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap().unwrap(),
        before
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

struct RevokeBeforeCallback {
    store: Arc<b4_sqlite::SqliteStore>,
}
impl ResourcePolicy for RevokeBeforeCallback {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        !(context.operation == Operation::Recover
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
            }))
    }
}
#[tokio::test]
async fn actual_tcp_recovered_dispatch_renews_policy_after_claim_without_callback_or_done() {
    let directory = Directory::new();
    let scripted = route(false, 0, 1);
    let saved_target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        seed(&store, &scripted, "dispatch-revoked");
        saved_target = target(&store, "dispatch-revoked");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(RevokeBeforeCallback {
            store: store.clone(),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target.clone()).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 200);
    assert!(!result.1.contains("event: done"));
    assert!(!result.1.contains("b4ag_"));
    let current = store
        .execution(&saved_target.execution_id)
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(current.status, ExecutionStatus::Running);
    assert_eq!(current.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(current.attempts_used, 1);
    assert!(!current.values.contains_key("calls"));
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

struct DenyPending;
impl ResourcePolicy for DenyPending {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        context.operation != Operation::ReadPending
    }
}
#[tokio::test]
async fn actual_tcp_recovered_park_disclosure_renews_pending_policy_and_closes_without_done() {
    let directory = Directory::new();
    let scripted = route(true, 0, 1);
    let saved_target;
    let original_grants;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        let initial = seed(&store, &scripted, "pending-revoked");
        let running = apply(&store, &initial, KernelCommand::Start);
        ApprovalService::new(store.clone(), Authority)
            .unwrap()
            .park(
                &context(),
                &running.snapshot.execution_id,
                fence(&store, &running),
                TaskId::new("task").unwrap(),
                InterruptId::new("protected-interrupt").unwrap(),
                json!({"question":"approve?"}),
                30_000,
                TransitionFacts { now_ms: 4 },
            )
            .unwrap();
        saved_target = target(&store, "pending-revoked");
        original_grants = store
            .approval_view(&saved_target.execution_id)
            .unwrap()
            .unwrap()
            .grants;
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(store.clone(), scripted, Arc::new(DenyPending), 1);
    let (address, server) = serve(&host, saved_target.clone()).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 200);
    assert!(!result.1.contains("event: interrupt"));
    assert!(!result.1.contains("event: done"));
    assert!(!result.1.contains("b4ag_"));
    assert!(!result.1.contains("protected-interrupt"));
    let current = store
        .execution(&saved_target.execution_id)
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(current.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(current.owner_generation, store.owner_generation());
    assert_eq!(
        store
            .approval_view(&saved_target.execution_id)
            .unwrap()
            .unwrap()
            .grants,
        original_grants
    );
    assert_eq!(
        exchange(
            address,
            "GET",
            "/threads/pending-revoked/pending_interrupts",
            ""
        )
        .await
        .0,
        404
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_recovery_retains_original_budget_when_later_batch_cannot_dispatch() {
    let directory = Directory::new();
    let mut scripted = route(false, 0, 2);
    let mut later = scripted.definition.batches[0][0].clone();
    later.id = TaskId::new("later").unwrap();
    later.effect_id = EffectId::new("later-effect").unwrap();
    scripted.definition.batches.push(vec![later]);
    scripted.effects.insert(
        EffectId::new("later-effect").unwrap(),
        ScriptEffect::Result(TaskResult {
            output: json!("later-callback"),
            writes: vec![StateWrite::Replace {
                field: "later-ran".into(),
                value: json!(true),
            }],
        }),
    );
    let saved_target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        let initial = seed(&store, &scripted, "later-budget");
        let running = apply(&store, &initial, KernelCommand::Start);
        apply(
            &store,
            &running,
            KernelCommand::Dispatch {
                task_id: TaskId::new("task").unwrap(),
                attempt_id: AttemptId::new("old-attempt").unwrap(),
            },
        );
        saved_target = target(&store, "later-budget");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target.clone()).await;
    let result = exchange(address, "POST", "/fixture/recover", "").await;
    assert_eq!(result.0, 200);
    assert!(!result.1.contains("event: done"));
    assert!(!result.1.contains("later-callback"));
    let current = store
        .execution(&saved_target.execution_id)
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(current.status, ExecutionStatus::Running);
    assert_eq!(current.attempts_used, 2);
    assert_eq!(current.definition.max_attempts, 2);
    assert_eq!(
        current.attempt_history[0],
        AttemptId::new("old-attempt").unwrap()
    );
    assert_eq!(current.values["calls"], json!(["finished"]));
    assert!(!current.values.contains_key("later-ran"));
    assert_eq!(current.tasks[1].status, TaskStatus::Ready);
    assert!(current.tasks[1].attempt_id.is_none());
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_abandoned_recovery_response_keeps_owner_until_effect_quiescence() {
    let directory = Directory::new();
    let scripted = route(false, 500, 1);
    let saved_target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        seed(&store, &scripted, "detached-owner");
        saved_target = target(&store, "detached-owner");
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = host(
        store.clone(),
        scripted,
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        1,
    );
    let (address, server) = serve(&host, saved_target.clone()).await;
    let mut stream = open(address, "POST", "/fixture/recover", "").await;
    let mut header = [0_u8; 1024];
    assert!(stream.read(&mut header).await.unwrap() > 0);
    drop(stream);
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    let reopened = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory.0) {
                Ok(store) => break store,
                Err(StoreError::AlreadyOwned) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("unexpected reopen failure: {error}"),
            }
        }
    })
    .await
    .unwrap();
    let complete = reopened
        .execution(&saved_target.execution_id)
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(complete.status, ExecutionStatus::Completed);
    assert_eq!(complete.attempts_used, 1);
    assert_eq!(complete.values["calls"], json!(["finished"]));
}
