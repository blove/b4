use axum::{
    body::to_bytes,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
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
    sync::oneshot,
};

pub const WAIT: Duration = Duration::from_secs(15);

pub fn principal() -> Principal {
    Principal {
        application: "discovery-application".into(),
        tenant: "discovery-tenant".into(),
        principal: "discovery-principal".into(),
    }
}
pub fn context() -> ApprovalContext {
    let actor = principal();
    ApprovalContext {
        actor: ThreadOrigin {
            application: actor.application,
            tenant: actor.tenant,
            principal: actor.principal,
        },
        policy_revision: "discovery-policy-1".into(),
    }
}
pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers
            .get("authorization")
            .is_some_and(|header| header == "Bearer discovery-fixture")
        {
            Ok(principal())
        } else {
            Err(AuthenticationError)
        }
    }
}
pub struct Policy(pub AtomicBool);
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
pub struct Authority;
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

#[derive(Clone, Copy)]
pub enum Phase {
    Accepted,
    Parked,
    Ready,
    Unsafe,
}

pub fn route(phase: Phase) -> ScriptRoute {
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/discovery-recovery#workflow").unwrap(),
            revision: ExecutionRevision::new("discovery-recovery-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: EffectId::new("effect").unwrap(),
                recovery: if matches!(phase, Phase::Unsafe) {
                    RecoveryPolicy::Unsafe
                } else {
                    RecoveryPolicy::Pure
                },
                requires_approval: matches!(phase, Phase::Parked | Phase::Ready),
            }]],
            max_attempts: 3,
        },
        effects: BTreeMap::from([(
            EffectId::new("effect").unwrap(),
            ScriptEffect::Result(TaskResult {
                output: json!("finite-fixture-result"),
                writes: vec![StateWrite::Append {
                    field: "calls".into(),
                    items: vec![json!("once")],
                }],
            }),
        )]),
    }
}

pub fn apply(
    store: &impl Store,
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
pub fn fence(store: &impl Store, current: &StoredExecution) -> ApprovalFence {
    ApprovalFence {
        execution: ExecutionFence::from(&current.snapshot),
        resource_revision: store
            .thread(&current.thread_id)
            .unwrap()
            .unwrap()
            .resource_revision,
    }
}

pub fn seed(store: Arc<SqliteStore>, scripted: &ScriptRoute, phase: Phase) -> ApprovalView {
    let id = ThreadId::new("discovered-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::from_iter([("private".into(), json!({"fixture": true}))]),
            origin: Some(context().actor),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("discovered-execution").unwrap(),
        TurnId::new("initial-turn").unwrap(),
        store.owner_generation(),
        scripted.definition.clone(),
        Map::new(),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    let mut current = store.admit(&id, initial).unwrap();
    if !matches!(phase, Phase::Accepted) {
        current = apply(store.as_ref(), &current, KernelCommand::Start);
    }
    if matches!(phase, Phase::Parked | Phase::Ready) {
        let service = ApprovalService::new(store.clone(), Authority).unwrap();
        current = service
            .park(
                &context(),
                &current.snapshot.execution_id,
                fence(store.as_ref(), &current),
                TaskId::new("task").unwrap(),
                InterruptId::new("original-private-interrupt").unwrap(),
                json!({"question": "approve finite fixture?"}),
                1,
                TransitionFacts { now_ms: 4 },
            )
            .unwrap();
        if matches!(phase, Phase::Ready) {
            let pending = service
                .pending(
                    &context(),
                    &current.snapshot.execution_id,
                    fence(store.as_ref(), &current),
                )
                .unwrap();
            current = service
                .accept(
                    &current.snapshot.execution_id,
                    fence(store.as_ref(), &current),
                    ApprovalAcceptanceRequest {
                        context: context(),
                        route: scripted.definition.route.clone(),
                        definition_revision: scripted.definition.revision.clone(),
                        turn_id: TurnId::new("accepted-turn").unwrap(),
                        now_ms: 4,
                        decisions: vec![GrantDecision {
                            interrupt_id: pending[0].interrupt_id.clone(),
                            decision: ApprovalDecision::Once,
                            cancelled: false,
                            token: Some(pending[0].token.clone()),
                        }],
                    },
                )
                .unwrap();
        }
    }
    if matches!(phase, Phase::Unsafe) {
        current = apply(
            store.as_ref(),
            &current,
            KernelCommand::Dispatch {
                task_id: TaskId::new("task").unwrap(),
                attempt_id: AttemptId::new("original-lost-attempt").unwrap(),
            },
        );
    }
    store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap()
}

pub fn build_host(
    store: Arc<SqliteStore>,
    scripted: ScriptRoute,
    policy: Arc<Policy>,
) -> HttpHost<SqliteStore> {
    let prompts = if scripted.definition.batches[0][0].requires_approval {
        BTreeMap::from([(
            scripted.definition.route.clone(),
            BTreeMap::from([(
                TaskId::new("task").unwrap(),
                json!({"question": "approve finite fixture?"}),
            )]),
        )])
    } else {
        BTreeMap::new()
    };
    HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        policy,
        HostConfig::new(vec![scripted]),
        ApprovalHostConfig::new("discovery-policy-1".into(), prompts),
    )
    .unwrap()
}

struct UnexpectedNativeCall(Arc<AtomicUsize>);
impl NativeEffect for UnexpectedNativeCall {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("finite-native-fixture-result"),
            writes: vec![],
        }))
    }
}

pub fn native_host(
    store: Arc<SqliteStore>,
    scripted: ScriptRoute,
) -> (HttpHost<SqliteStore>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let application = NativeApplication::new(
        ApplicationIdentity {
            application: principal().application,
            revision: "discovery-native-1".into(),
        },
        vec![RouteRegistration {
            definition: scripted.definition,
            effects: BTreeMap::from([(
                EffectId::new("effect").unwrap(),
                EffectBinding {
                    callback: Arc::new(UnexpectedNativeCall(calls.clone())),
                    output_kind: OutputKind::Value,
                },
            )]),
            input_validator: None,
        }],
    )
    .unwrap();
    let host = HttpHost::new_with_application(
        store,
        Arc::new(Auth),
        Arc::new(Policy(AtomicBool::new(true))),
        NativeHostConfig::new(
            Arc::new(application),
            ApprovalHostConfig::new("discovery-policy-1".into(), BTreeMap::new()),
        ),
    )
    .unwrap();
    (host, calls)
}

pub fn discovered_target(
    store: &SqliteStore,
    prior: bool,
) -> (UnfinishedDiscoveryPage, RecoveryTarget) {
    let page = store.discover_unfinished(None).unwrap();
    assert_eq!(page.scanned_rows, 1);
    assert_eq!(page.records.len(), 1);
    assert!(page.next_cursor.is_none());
    let record = &page.records[0];
    assert_eq!(
        record.owner_relation,
        if prior {
            DiscoveryOwnerRelation::Prior
        } else {
            DiscoveryOwnerRelation::Current
        }
    );
    // Discovery is used only to identify an ordinary resource. Authority and
    // full immutable definition remain the separately trusted host's work.
    let view = store.thread_view(&record.thread_id).unwrap().unwrap();
    let target = RecoveryTarget::from_view(&view).unwrap();
    assert_eq!(target.thread_generation, record.thread_generation);
    assert_eq!(target.execution_id, record.execution_id);
    assert_eq!(target.fence.execution, record.execution_fence);
    assert_eq!(target.fence.resource_revision, record.resource_revision);
    assert_eq!(
        view.execution.as_ref().unwrap().snapshot.definition.route,
        record.route
    );
    assert_eq!(
        view.execution.unwrap().snapshot.definition.revision,
        record.definition_revision
    );
    (page, target)
}

pub async fn body(response: Response) -> (StatusCode, String) {
    let status = response.status();
    let raw = tokio::time::timeout(WAIT, to_bytes(response.into_body(), 262_144))
        .await
        .unwrap()
        .unwrap();
    (status, String::from_utf8(raw.to_vec()).unwrap())
}

pub async fn recover(
    host: &HttpHost<SqliteStore>,
    actor: Principal,
    target: RecoveryTarget,
) -> (StatusCode, String) {
    body(
        tokio::time::timeout(WAIT, host.recover_execution(actor, target))
            .await
            .unwrap(),
    )
    .await
}

pub struct Server {
    pub address: std::net::SocketAddr,
    stop: oneshot::Sender<()>,
    join: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn start(host: &HttpHost<SqliteStore>) -> Self {
        let listener = tokio::time::timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
            .await
            .unwrap()
            .unwrap();
        let address = listener.local_addr().unwrap();
        let router = host.router();
        let (stop, receiver) = oneshot::channel();
        let join = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = receiver.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            stop,
            join,
        }
    }
    pub async fn get(&self, path: &str, authenticated: bool) -> (u16, String) {
        tokio::time::timeout(WAIT, async {
            let mut socket = TcpStream::connect(self.address).await.unwrap();
            let authorization = if authenticated { "Authorization: Bearer discovery-fixture\r\n" } else { "" };
            socket.write_all(format!("GET {path} HTTP/1.1\r\nHost: {}\r\n{authorization}Connection: close\r\n\r\n", self.address).as_bytes()).await.unwrap();
            let mut raw = Vec::new();
            // A fixed response cap precedes allocation growth.
            socket.take(262_145).read_to_end(&mut raw).await.unwrap();
            assert!(raw.len() <= 262_144);
            let raw = String::from_utf8(raw).unwrap();
            let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
            (status, raw)
        }).await.unwrap()
    }
    pub async fn finish(self) {
        self.stop.send(()).unwrap();
        tokio::time::timeout(WAIT, self.join)
            .await
            .unwrap()
            .unwrap();
    }
}

pub async fn retire(
    host: HttpHost<SqliteStore>,
    server: Option<Server>,
    store: Arc<SqliteStore>,
    path: &std::path::Path,
) {
    let weak = Arc::downgrade(&store);
    let owner = store.owner_generation();
    assert!(matches!(
        SqliteStore::open(path),
        Err(StoreError::AlreadyOwned)
    ));
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    assert_eq!(
        tokio::time::timeout(WAIT, shutdown.wait_quiescent(WAIT))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    if let Some(server) = server {
        server.finish().await;
    }
    drop(host);
    drop(store);
    assert!(weak.upgrade().is_none());
    let successor = SqliteStore::open(path).unwrap();
    assert_eq!(successor.owner_generation(), owner + 1);
    drop(successor);
}
