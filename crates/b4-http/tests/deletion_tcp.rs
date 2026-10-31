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
        Self(std::env::temp_dir().join(format!("b4-deletion-tcp-{}", uuid::Uuid::new_v4())))
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
            route: RouteKey::new("/delete#workflow").unwrap(),
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
            RouteKey::new("/delete#workflow").unwrap(),
            BTreeMap::from([(TaskId::new("task").unwrap(), json!({"question":"approve?"}))]),
        )]),
    )
}
async fn serve<S: Store + 'static>(
    host: &HttpHost<S>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("actual TCP deletion tests require loopback bind permission");
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
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
fn empty_deleted(result: &(u16, String, String)) {
    assert_eq!(result.0, 204);
    assert!(result.1.to_lowercase().contains("cache-control: no-store"));
    assert!(result.2.is_empty());
}
fn selected_reference_status(id: &str, actual: u16) {
    // This compares only the selected response status. Native scripted effects,
    // timing, authority and response bytes have separate qualification above.
    let recording: Value = serde_json::from_str(include_str!("../../../conformance/reference/recordings/2026-10-01-lifecycle/lifecycle-observations.json")).unwrap();
    assert_eq!(recording["evidence_classification"], "runtime_observation");
    let observation = recording["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == id)
        .unwrap();
    assert_eq!(
        u64::from(actual),
        observation["response"]["status"].as_u64().unwrap(),
        "Selected status comparison for {id}"
    );
}
fn no_authority(directory: &Directory, execution_id: &ExecutionId) {
    let connection = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    for table in [
        "executions",
        "approval_grants",
        "approval_acceptances",
        "journal",
    ] {
        let field = if table == "executions" {
            "id"
        } else {
            "execution_id"
        };
        let count: i64 = connection
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE {field} = ?1"),
                [execution_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 0,
            "Deleted execution must leave no associated {table} rows"
        );
    }
}

#[tokio::test]
async fn actual_tcp_disconnected_producer_fences_deletion_until_quiescence_without_blocking_unrelated_delete(
) {
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(route(false, 750)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let producer = create(address).await;
    let unrelated = create(address).await;
    let mut stream = open(
        address,
        "POST",
        &format!("/threads/{producer}/runs/stream"),
        r#"{"route":"/delete#workflow","input":{}}"#,
    )
    .await;
    let mut headers = [0_u8; 1024];
    assert!(stream.read(&mut headers).await.unwrap() > 0);
    drop(stream);
    let id = ThreadId::new(&producer).unwrap();
    wait_until(|| {
        store
            .thread_view(&id)
            .unwrap()
            .unwrap()
            .execution
            .is_some_and(|execution| {
                execution
                    .snapshot
                    .tasks
                    .iter()
                    .any(|task| task.status == TaskStatus::InFlight)
            })
    })
    .await;
    let before = store.thread_view(&id).unwrap().unwrap();
    let busy = exchange(address, "DELETE", &format!("/threads/{producer}"), "").await;
    assert_eq!(busy.0, 409);
    selected_reference_status("live.delete-active", busy.0);
    assert!(busy.2.contains("run_in_flight"));
    assert_eq!(
        store
            .thread_view(&id)
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .execution_id,
        before.execution.unwrap().snapshot.execution_id,
        "Refused deletion must leave the producer execution present while its own work may settle"
    );
    empty_deleted(&exchange(address, "DELETE", &format!("/threads/{unrelated}"), "").await);
    assert!(store
        .thread(&ThreadId::new(&unrelated).unwrap())
        .unwrap()
        .is_none());
    wait_until(|| {
        store
            .thread_view(&id)
            .unwrap()
            .unwrap()
            .execution
            .is_some_and(|execution| execution.snapshot.status == ExecutionStatus::Completed)
    })
    .await;
    let completed = store.thread_view(&id).unwrap().unwrap().execution.unwrap();
    assert_eq!(completed.snapshot.values["calls"], json!(["settled"]));
    assert_eq!(completed.snapshot.attempts_used, 1);
    // Snapshot settlement can precede the worker's final guard release. Retry
    // only that honest run_in_flight window rather than assume it already ended.
    let deleted = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let result = exchange(address, "DELETE", &format!("/threads/{producer}"), "").await;
            if result.0 == 204 {
                break result;
            }
            assert_eq!(result.0, 409);
            assert!(result.2.contains("run_in_flight"));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    empty_deleted(&deleted);
    selected_reference_status("live.delete-completed", deleted.0);
    assert!(store.thread(&id).unwrap().is_none());
    assert!(store
        .execution(&completed.snapshot.execution_id)
        .unwrap()
        .is_none());
    assert_eq!(
        exchange(address, "GET", &format!("/threads/{producer}/state"), "")
            .await
            .0,
        404
    );
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_committed_park_remains_claimed_until_disclosure_worker_releases() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let gate = Arc::new(Gate::new(Operation::ReadPending, 0));
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        gate.clone(),
        config(route(true, 0)),
        approval_config(),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let stream = open(
        address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        r#"{"route":"/delete#workflow","input":{}}"#,
    )
    .await;
    wait_until(|| gate.entered.load(Ordering::Acquire)).await;
    drop(stream);
    let thread_id = ThreadId::new(&id).unwrap();
    let parked = store
        .thread_view(&thread_id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(parked.snapshot.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(parked.snapshot.attempts_used, 0);
    let authority = store
        .approval_view(&parked.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(authority.grants.len(), 1);
    assert!(authority.acceptances.is_empty());
    let busy = exchange(address, "DELETE", &format!("/threads/{id}"), "").await;
    assert_eq!(busy.0, 409);
    assert!(busy.2.contains("run_in_flight"));
    assert_eq!(
        store
            .approval_view(&parked.snapshot.execution_id)
            .unwrap()
            .unwrap()
            .grants,
        authority.grants
    );
    gate.release();
    let deleted = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let result = exchange(address, "DELETE", &format!("/threads/{id}"), "").await;
            if result.0 == 204 {
                break result;
            }
            assert_eq!(result.0, 409);
            assert!(result.2.contains("run_in_flight"));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    empty_deleted(&deleted);
    selected_reference_status("parked.delete", deleted.0);
    assert!(store.thread(&thread_id).unwrap().is_none());
    assert!(store
        .approval_view(&parked.snapshot.execution_id)
        .unwrap()
        .is_none());
    no_authority(&directory, &parked.snapshot.execution_id);
    let gone = exchange(address, "GET", &format!("/threads/{id}/state"), "").await;
    assert_eq!(gone.0, 404);
    selected_reference_status("parked.deleted-state", gone.0);
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
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
fn approval_context() -> ApprovalContext {
    ApprovalContext {
        actor: origin(),
        policy_revision: "policy-v1".into(),
    }
}
fn seed(store: &b4_sqlite::SqliteStore, scripted: &ScriptRoute, id: &str) -> StoredExecution {
    let id = ThreadId::new(id).unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(origin()),
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
#[tokio::test]
async fn actual_tcp_prior_owner_stable_park_and_settled_once_delete_without_adoption() {
    let directory = Directory::new();
    let scripted = route(true, 0);
    let execution_ids;
    {
        let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        let service = ApprovalService::new(store.clone(), Authority).unwrap();
        let runtime = Runtime::new(store.clone());
        let mut ids = Vec::new();
        for (id, complete) in [("prior-parked", false), ("prior-complete", true)] {
            let accepted = seed(&store, &scripted, id);
            let running = runtime
                .apply(
                    &accepted.snapshot.execution_id,
                    ExecutionFence::from(&accepted.snapshot),
                    KernelCommand::Start,
                    TransitionFacts { now_ms: 3 },
                )
                .unwrap();
            let parked = service
                .park(
                    &approval_context(),
                    &running.snapshot.execution_id,
                    fence(&store, &running),
                    TaskId::new("task").unwrap(),
                    InterruptId::new(format!("interrupt-{id}")).unwrap(),
                    json!({"question":"approve?"}),
                    30_000,
                    TransitionFacts { now_ms: 4 },
                )
                .unwrap();
            if complete {
                let pending = service
                    .pending(
                        &approval_context(),
                        &parked.snapshot.execution_id,
                        fence(&store, &parked),
                    )
                    .unwrap();
                let resumed = service
                    .accept(
                        &parked.snapshot.execution_id,
                        fence(&store, &parked),
                        ApprovalAcceptanceRequest {
                            context: approval_context(),
                            route: scripted.definition.route.clone(),
                            definition_revision: scripted.definition.revision.clone(),
                            turn_id: TurnId::new("approved-turn").unwrap(),
                            now_ms: 5,
                            decisions: vec![GrantDecision {
                                interrupt_id: pending[0].interrupt_id.clone(),
                                token: Some(pending[0].token.clone()),
                                decision: ApprovalDecision::Once,
                                cancelled: false,
                            }],
                        },
                    )
                    .unwrap();
                service
                    .dispatch_outcome(
                        &approval_context(),
                        &resumed.snapshot.execution_id,
                        fence(&store, &resumed),
                        TaskId::new("task").unwrap(),
                        AttemptId::new("approved-attempt").unwrap(),
                        TransitionFacts { now_ms: 6 },
                        || TransitionFacts { now_ms: 7 },
                        |_| {
                            Ok(EffectOutcome::Succeeded(TaskResult {
                                output: json!("settled"),
                                writes: vec![],
                            }))
                        },
                    )
                    .unwrap();
                assert_eq!(
                    store
                        .approval_view(&resumed.snapshot.execution_id)
                        .unwrap()
                        .unwrap()
                        .acceptances
                        .len(),
                    1
                );
            }
            ids.push(parked.snapshot.execution_id);
        }
        execution_ids = ids;
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(scripted),
        approval_config(),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    for (id, execution_id) in ["prior-parked", "prior-complete"]
        .into_iter()
        .zip(execution_ids)
    {
        let original = store.execution(&execution_id).unwrap().unwrap();
        assert!(original.snapshot.owner_generation < store.owner_generation());
        let old_journal = store.journal(&execution_id).unwrap();
        empty_deleted(&exchange(address, "DELETE", &format!("/threads/{id}"), "").await);
        assert!(store.thread(&ThreadId::new(id).unwrap()).unwrap().is_none());
        assert!(store.execution(&execution_id).unwrap().is_none());
        assert!(store.journal(&execution_id).unwrap().is_empty());
        assert!(!old_journal.is_empty());
        no_authority(&directory, &execution_id);
    }
    assert_eq!(exchange(address, "GET", "/readyz", "").await.0, 200);
    server.abort();
}

#[tokio::test]
async fn actual_tcp_abandoned_delete_keeps_exclusion_and_store_owner_until_commit_quiesces() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let gate = Arc::new(Gate::new(Operation::DeleteThread, 1));
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        gate.clone(),
        config(route(false, 0)),
    )
    .unwrap();
    let (address, server) = serve(&host).await;
    let id = create(address).await;
    let delete = open(address, "DELETE", &format!("/threads/{id}"), "").await;
    wait_until(|| gate.entered.load(Ordering::Acquire)).await;
    let thread_id = ThreadId::new(&id).unwrap();
    let before = store.thread_view(&thread_id).unwrap().unwrap();
    let refused = exchange(
        address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        r#"{"route":"/delete#workflow","input":{}}"#,
    )
    .await;
    assert_eq!(refused.0, 409);
    assert!(refused.2.contains("thread_busy"));
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), before);
    let unrelated = create(address).await;
    let completed = exchange(
        address,
        "POST",
        &format!("/threads/{unrelated}/runs/wait"),
        r#"{"route":"/delete#workflow","input":{}}"#,
    )
    .await;
    assert_eq!(completed.0, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&completed.2).unwrap(),
        json!("settled")
    );
    drop(delete);
    server.abort();
    let _ = server.await;
    drop(host);
    drop(store);
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    gate.release();
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
    assert!(reopened.thread(&thread_id).unwrap().is_none());
    assert_eq!(
        reopened
            .thread_view(&ThreadId::new(&unrelated).unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::Completed
    );
}
