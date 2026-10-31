use axum::{http::HeaderMap, routing::post};
use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[path = "shutdown_support/store.rs"]
mod observation;
#[path = "native_support/mod.rs"]
mod wire;
use observation::ObservedStore;
use wire::{
    cancelled, config, definition, exchange, finish, frames, open, prefix, principal, snapshot,
    Directory, Gate, Release,
};

type Sqlite = b4_sqlite::SqliteStore;
type Observed = ObservedStore<Sqlite>;
const RUN: &str = r#"{"route":"/native#workflow","input":{"caller":"original"}}"#;

/// Abort the fixture listener on every failure path. Tests explicitly stop and
/// await it before disposing external host/router references on their success path.
struct Server {
    address: SocketAddr,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Server {
    async fn start<S: Store + 'static>(
        host: &HttpHost<S>,
        recovery: Option<RecoveryTarget>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("shutdown TCP qualification requires loopback bind permission");
        let address = listener.local_addr().unwrap();
        let mut router = host.router();
        if let Some(target) = recovery {
            // Test-only adapter for the exact trusted embedding operation. No
            // production shutdown or recovery HTTP endpoint is introduced.
            let host = host.clone();
            router = router.route(
                "/fixture/recover",
                post(move || {
                    let host = host.clone();
                    let target = target.clone();
                    async move { host.recover_execution(principal(), target).await }
                }),
            );
        }
        Self {
            address,
            task: Some(tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            })),
        }
    }
    async fn stop(mut self) {
        let task = self.task.take().unwrap();
        task.abort();
        let _ = task.await;
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct Auth {
    calls: Arc<AtomicUsize>,
    gate: Option<Arc<Gate>>,
}
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.hold();
        }
        wire::Auth.authenticate(headers)
    }
}
struct Policy {
    calls: Arc<AtomicUsize>,
    gate: Option<(Operation, Arc<Gate>)>,
    held: AtomicBool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, p: &Principal, context: &ResourceContext) -> bool {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((operation, gate)) = &self.gate {
            if context.operation == *operation && !self.held.swap(true, Ordering::SeqCst) {
                gate.hold();
            }
        }
        wire::Owner.authorize(p, context)
    }
}
struct Validator {
    gate: Arc<Gate>,
    calls: Arc<AtomicUsize>,
}
impl InputValidator for Validator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.gate.hold();
        Ok(())
    }
}
#[derive(Clone, Copy)]
enum Outcome {
    Succeeded,
    Failed,
    Uncertain,
}
struct Effect {
    gate: Option<Arc<Gate>>,
    calls: Arc<Vec<AtomicUsize>>,
    outcome: Outcome,
}
impl NativeEffect for Effect {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        assert_eq!(context.origin.application, "native-test");
        let first = invocation.task_id.as_str() == "first";
        self.calls[usize::from(!first)].fetch_add(1, Ordering::SeqCst);
        if first {
            if let Some(gate) = &self.gate {
                gate.hold();
            }
        }
        match self.outcome {
            Outcome::Succeeded => Ok(NativeOutcome::Succeeded(TaskResult {
                output: json!({"actual":invocation.task_id.as_str()}),
                writes: vec![StateWrite::Append {
                    field: "joined".into(),
                    items: vec![json!(invocation.task_id.as_str())],
                }],
            })),
            Outcome::Failed => Ok(NativeOutcome::Failed("actual late native failure".into())),
            Outcome::Uncertain => Err(EffectUncertain),
        }
    }
}
fn effect_calls() -> Arc<Vec<AtomicUsize>> {
    Arc::new(vec![AtomicUsize::new(0), AtomicUsize::new(0)])
}
fn store(directory: &Directory) -> Arc<Observed> {
    Arc::new(ObservedStore::new(Sqlite::open(&directory.0).unwrap()))
}
fn route(recovery: RecoveryPolicy) -> Definition {
    definition(
        "/native#workflow",
        "native-v1",
        &[("first", false, recovery)],
    )
}
fn auth(calls: Arc<AtomicUsize>, gate: Option<Arc<Gate>>) -> Arc<dyn Authenticator> {
    Arc::new(Auth { calls, gate })
}
fn policy(
    calls: Arc<AtomicUsize>,
    gate: Option<(Operation, Arc<Gate>)>,
) -> Arc<dyn ResourcePolicy> {
    Arc::new(Policy {
        calls,
        gate,
        held: AtomicBool::new(false),
    })
}
fn host(
    store: Arc<Observed>,
    program: Definition,
    callback: Arc<dyn NativeEffect>,
    validator: Option<Arc<dyn InputValidator>>,
    authenticator: Arc<dyn Authenticator>,
    resource_policy: Arc<dyn ResourcePolicy>,
) -> HttpHost<Observed> {
    let app = wire::application(program, callback, validator);
    HttpHost::new_with_application(store, authenticator, resource_policy, config(app, true, 1))
        .unwrap()
}
fn ordinary_host(
    store: Arc<Observed>,
    gate: Option<Arc<Gate>>,
    outcome: Outcome,
) -> (HttpHost<Observed>, Arc<Vec<AtomicUsize>>) {
    let calls = effect_calls();
    let host = host(
        store,
        route(if matches!(outcome, Outcome::Uncertain) {
            RecoveryPolicy::Unsafe
        } else {
            RecoveryPolicy::Pure
        }),
        Arc::new(Effect {
            gate,
            calls: calls.clone(),
            outcome,
        }),
        None,
        auth(Arc::new(AtomicUsize::new(0)), None),
        policy(Arc::new(AtomicUsize::new(0)), None),
    );
    (host, calls)
}
fn locked(directory: &Directory) {
    assert!(
        matches!(Sqlite::open(&directory.0), Err(StoreError::AlreadyOwned)),
        "a surviving registered job must retain the actual SQLite directory owner"
    );
}
async fn reopen(directory: &Directory) -> Sqlite {
    // Group retirement is not an instantaneous Arc destructor/lock-release
    // guarantee. Prove the physical close; retry only the known destruction gap.
    let reopened = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Sqlite::open(&directory.0) {
                Ok(store) => break store,
                Err(StoreError::AlreadyOwned) => tokio::task::yield_now().await,
                Err(_) => panic!("SQLite did not reopen after physical job return"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.owner_generation(), 2);
    reopened
}
async fn incomplete(handle: &ShutdownHandle, faulted: bool) {
    assert_eq!(
        handle
            .wait_quiescent(Duration::from_millis(15))
            .await
            .unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted
        }
    );
    assert_eq!(handle.status().phase, ShutdownPhase::Draining);
}
async fn quiescent(handle: &ShutdownHandle, faulted: bool) {
    assert_eq!(
        handle.wait_quiescent(Duration::from_secs(5)).await.unwrap(),
        ShutdownOutcome::Quiescent { faulted }
    );
    assert_eq!(handle.status().active_owners, 0);
    assert_eq!(handle.status().phase, ShutdownPhase::Draining);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_closed_operations_refuse_before_body_auth_policy_and_store_and_probes_stay_live() {
    let directory = Directory::new();
    let store = store(&directory);
    let auth_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let host = host(
        store.clone(),
        route(RecoveryPolicy::Pure),
        Arc::new(Effect {
            gate: None,
            calls: effect_calls(),
            outcome: Outcome::Succeeded,
        }),
        None,
        auth(auth_calls.clone(), None),
        policy(policy_calls.clone(), None),
    );
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let baseline = store.counters();
    assert_eq!(handle.begin_shutdown().phase, ShutdownPhase::Draining);
    for (method, path) in [
        ("POST", "/threads"),
        ("GET", "/threads/missing"),
        ("GET", "/threads/missing/state"),
        ("GET", "/threads/missing/pending_interrupts"),
        ("POST", "/threads/missing/runs/wait"),
        ("POST", "/threads/missing/runs/stream"),
        ("POST", "/threads/missing/resume"),
        ("POST", "/threads/missing/cancel"),
        ("DELETE", "/threads/missing"),
        ("GET", "/threads/missing/runs/stream"),
        ("GET", "/b4/capabilities"),
    ] {
        let response = exchange(server.address, method, path, "malformed request body").await;
        assert_eq!(response.status, 503);
        assert_eq!(response.json()["error"]["details"]["code"], "host_draining");
    }
    let mut partial = tokio::net::TcpStream::connect(server.address)
        .await
        .unwrap();
    // Omit auth and all declared body bytes. A response proves neither auth nor
    // body collection is a prerequisite for this closed operational refusal.
    partial.write_all(format!("POST /threads HTTP/1.1\r\nHost: {}\r\nContent-Length: 65536\r\nConnection: close\r\n\r\n", server.address).as_bytes()).await.unwrap();
    let response = finish(partial, vec![]).await;
    assert_eq!(response.status, 503);
    assert_eq!(response.json()["error"]["details"]["code"], "host_draining");
    assert_eq!(
        exchange(server.address, "GET", "/healthz", "").await.status,
        200
    );
    assert_eq!(
        exchange(server.address, "GET", "/readyz", "").await.status,
        503
    );
    assert_eq!(auth_calls.load(Ordering::SeqCst), 0);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.counters(), baseline);
    quiescent(&handle, false).await;
    server.stop().await;
    drop(host);
    drop(store);
    drop(reopen(&directory).await);
    assert_eq!(handle.status().active_owners, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_abandoned_authenticator_retains_sqlite_until_actual_return_and_loses_mutation_lease() {
    let directory = Directory::new();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = store(&directory);
    let auth_calls = Arc::new(AtomicUsize::new(0));
    let calls = effect_calls();
    let host = host(
        store.clone(),
        route(RecoveryPolicy::Pure),
        Arc::new(Effect {
            gate: None,
            calls: calls.clone(),
            outcome: Outcome::Succeeded,
        }),
        None,
        auth(auth_calls.clone(), Some(gate.clone())),
        policy(Arc::new(AtomicUsize::new(0)), None),
    );
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let client = open(server.address, "POST", "/threads/auth-held/runs/wait", RUN).await;
    gate.entered().await;
    drop(client);
    incomplete(&handle, false).await;
    server.stop().await;
    drop(host);
    drop(store);
    locked(&directory);
    assert_eq!(handle.status().active_owners, 1);
    gate.release();
    quiescent(&handle, false).await;
    let reopened = reopen(&directory).await;
    assert!(reopened
        .thread(&ThreadId::new("auth-held").unwrap())
        .unwrap()
        .is_none());
    assert_eq!(auth_calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls[0].load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_abandoned_policy_without_run_permit_keeps_owner_and_cannot_create_after_cut() {
    let directory = Directory::new();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = store(&directory);
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let (callback, calls) = (Outcome::Succeeded, effect_calls());
    let host = host(
        store.clone(),
        route(RecoveryPolicy::Pure),
        Arc::new(Effect {
            gate: None,
            calls: calls.clone(),
            outcome: callback,
        }),
        None,
        auth(Arc::new(AtomicUsize::new(0)), None),
        policy(
            policy_calls.clone(),
            Some((Operation::CreateThread, gate.clone())),
        ),
    );
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let creations = store.creates.clone();
    let client = open(server.address, "POST", "/threads", "{}").await;
    gate.entered().await;
    drop(client);
    incomplete(&handle, false).await;
    assert_eq!(store.creates.load(Ordering::SeqCst), 0);
    // An independently cancelled wait cannot release the request's real job.
    let wait = tokio::spawn(handle.wait_quiescent(Duration::from_secs(5)));
    wait.abort();
    let _ = wait.await;
    assert_eq!(handle.status().active_owners, 1);
    server.stop().await;
    drop(host);
    drop(store);
    locked(&directory);
    gate.release();
    quiescent(&handle, false).await;
    let reopened = reopen(&directory).await;
    assert_eq!(creations.load(Ordering::SeqCst), 0);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls[0].load(Ordering::SeqCst), 0);
    assert_eq!(reopened.owner_generation(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_abandoned_readonly_store_fault_is_counted_without_run_permit_and_latched_before_retirement(
) {
    let directory = Directory::new();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = store(&directory);
    store
        .inner
        .create_thread_with(ThreadSeed {
            id: ThreadId::new("read-held").unwrap(),
            created_at_ms: 1,
            metadata: serde_json::Map::new(),
            origin: Some(ThreadOrigin {
                application: "native-test".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    let (host, calls) = ordinary_host(store.clone(), None, Outcome::Succeeded);
    let held = gate.clone();
    *store.view_hook.lock().unwrap() = Some(Arc::new(move |_| {
        held.hold();
        Err(StoreError::Unavailable(
            "private fixture store diagnostic".into(),
        ))
    }));
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let client = open(server.address, "GET", "/threads/read-held/state", "").await;
    gate.entered().await;
    drop(client);
    incomplete(&handle, false).await;
    server.stop().await;
    drop(host);
    drop(store);
    locked(&directory);
    gate.release();
    quiescent(&handle, true).await;
    let reopened = reopen(&directory).await;
    assert!(reopened
        .thread_view(&ThreadId::new("read-held").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .is_none());
    assert_eq!(calls[0].load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_validator_disconnect_and_multiple_cancelled_waits_preserve_owner_without_implicit_admission(
) {
    let directory = Directory::new();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = store(&directory);
    let validation_calls = Arc::new(AtomicUsize::new(0));
    let calls = effect_calls();
    let host = host(
        store.clone(),
        route(RecoveryPolicy::Pure),
        Arc::new(Effect {
            gate: None,
            calls: calls.clone(),
            outcome: Outcome::Succeeded,
        }),
        Some(Arc::new(Validator {
            gate: gate.clone(),
            calls: validation_calls.clone(),
        })),
        auth(Arc::new(AtomicUsize::new(0)), None),
        policy(Arc::new(AtomicUsize::new(0)), None),
    );
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let client = open(
        server.address,
        "POST",
        "/threads/validator-held/runs/wait",
        RUN,
    )
    .await;
    gate.entered().await;
    drop(client);
    incomplete(&handle, false).await;
    let first = tokio::spawn(handle.wait_quiescent(Duration::from_secs(5)));
    let second = tokio::spawn(handle.wait_quiescent(Duration::from_secs(5)));
    let cancelled_wait = tokio::spawn(handle.wait_quiescent(Duration::from_secs(5)));
    cancelled_wait.abort();
    let _ = cancelled_wait.await;
    assert_eq!(handle.status().active_owners, 1);
    server.stop().await;
    drop(host);
    drop(store);
    locked(&directory);
    gate.release();
    let expected = Ok(ShutdownOutcome::Quiescent { faulted: false });
    assert_eq!(first.await.unwrap(), expected);
    assert_eq!(second.await.unwrap(), expected);
    quiescent(&handle, false).await;
    let reopened = reopen(&directory).await;
    assert!(reopened
        .thread(&ThreadId::new("validator-held").unwrap())
        .unwrap()
        .is_none());
    assert_eq!(validation_calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls[0].load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_admitted_turn_keeps_dispatching_later_batch_after_timeout_disconnect_and_cut() {
    let directory = Directory::new();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = store(&directory);
    let calls = effect_calls();
    let mut program = route(RecoveryPolicy::Pure);
    program.batches.push(vec![TaskSpec {
        id: TaskId::new("second").unwrap(),
        effect_id: EffectId::new("second").unwrap(),
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    }]);
    let host = host(
        store.clone(),
        program,
        Arc::new(Effect {
            gate: Some(gate.clone()),
            calls: calls.clone(),
            outcome: Outcome::Succeeded,
        }),
        None,
        auth(Arc::new(AtomicUsize::new(0)), None),
        policy(Arc::new(AtomicUsize::new(0)), None),
    );
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let mut client = open(server.address, "POST", "/threads/admitted/runs/stream", RUN).await;
    let _ = prefix(&mut client, b"\r\n\r\n").await;
    gate.entered().await;
    assert_eq!(
        snapshot(store.as_ref(), "admitted").tasks[0].status,
        TaskStatus::InFlight
    );
    drop(client);
    incomplete(&handle, false).await;
    assert_eq!(calls[1].load(Ordering::SeqCst), 0);
    server.stop().await;
    drop(host);
    drop(store);
    locked(&directory);
    gate.release();
    quiescent(&handle, false).await;
    let reopened = reopen(&directory).await;
    let saved = snapshot(&reopened, "admitted");
    assert_eq!(saved.status, ExecutionStatus::Completed);
    assert!(saved
        .tasks
        .iter()
        .all(|task| task.status == TaskStatus::Succeeded));
    assert_eq!(saved.values["joined"], json!(["first", "second"]));
    assert_eq!(
        saved.output,
        Some(json!({"input":{"caller":"original"},"joined":["first","second"]}))
    );
    for (task, expected) in saved.tasks.iter().zip(["first", "second"]) {
        assert_eq!(
            task.result.as_ref().unwrap().output,
            json!({"actual":expected})
        );
    }
    assert_eq!(saved.attempts_used, 2);
    assert!(calls.iter().all(|call| call.load(Ordering::SeqCst) == 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_cancelled_viewer_done_is_not_quiescence_and_actual_late_failure_stays_durable() {
    let directory = Directory::new();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = store(&directory);
    let (host, calls) = ordinary_host(store.clone(), Some(gate.clone()), Outcome::Failed);
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let mut primary = open(
        server.address,
        "POST",
        "/threads/cancelled/runs/stream",
        RUN,
    )
    .await;
    let primary_prefix = prefix(&mut primary, b"\r\n\r\n").await;
    gate.entered().await;
    let mut attached = open(server.address, "GET", "/threads/cancelled/runs/stream", "").await;
    let attached_prefix = prefix(&mut attached, b"event: state").await;
    assert_eq!(
        exchange(server.address, "POST", "/threads/cancelled/cancel", "")
            .await
            .status,
        200
    );
    cancelled(&finish(primary, primary_prefix).await);
    cancelled(&finish(attached, attached_prefix).await);
    let saved = snapshot(store.as_ref(), "cancelled");
    assert_eq!(saved.status, ExecutionStatus::CancelRequested);
    assert_eq!(saved.tasks[0].status, TaskStatus::InFlight);
    incomplete(&handle, false).await;
    server.stop().await;
    drop(host);
    drop(store);
    locked(&directory);
    gate.release();
    quiescent(&handle, false).await;
    let reopened = reopen(&directory).await;
    let saved = snapshot(&reopened, "cancelled");
    assert_eq!(saved.status, ExecutionStatus::Failed);
    assert_eq!(saved.tasks[0].status, TaskStatus::Failed);
    assert_eq!(saved.failure.as_deref(), Some("actual late native failure"));
    assert_eq!(saved.attempts_used, 1);
    assert!(!saved.values.contains_key("joined"));
    assert_eq!(calls[0].load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_faulted_quiescence_preserves_unsafe_uncertainty_and_exact_known_recovery_never_replays(
) {
    let directory = Directory::new();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = store(&directory);
    let (host, calls) = ordinary_host(store.clone(), Some(gate.clone()), Outcome::Uncertain);
    let server = Server::start(&host, None).await;
    let handle = host.shutdown_handle();
    let mut client = open(server.address, "POST", "/threads/unsafe/runs/stream", RUN).await;
    let start = prefix(&mut client, b"\r\n\r\n").await;
    gate.entered().await;
    let before = snapshot(store.as_ref(), "unsafe");
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    incomplete(&handle, false).await;
    gate.release();
    let response = finish(client, start).await;
    assert!(!frames(&response.body)
        .iter()
        .any(|(event, _)| event == "done"));
    quiescent(&handle, true).await;
    let unknown = snapshot(store.as_ref(), "unsafe");
    assert_eq!(unknown.tasks[0].status, TaskStatus::InFlight);
    assert!(unknown.tasks[0].result.is_none());
    assert_eq!(unknown.attempts_used, 1);
    server.stop().await;
    drop(host);
    drop(store);
    let reopened = Arc::new(reopen(&directory).await);
    let view = reopened
        .thread_view(&ThreadId::new("unsafe").unwrap())
        .unwrap()
        .unwrap();
    let target = RecoveryTarget::from_view(&view).unwrap();
    let callback = Arc::new(Effect {
        gate: None,
        calls: calls.clone(),
        outcome: Outcome::Succeeded,
    });
    let app = wire::application(route(RecoveryPolicy::Unsafe), callback, None);
    let replacement = HttpHost::new_with_application(
        reopened.clone(),
        Arc::new(wire::Auth),
        Arc::new(wire::Owner),
        config(app, true, 1),
    )
    .unwrap();
    let replacement_server = Server::start(&replacement, Some(target)).await;
    assert_eq!(
        exchange(replacement_server.address, "POST", "/fixture/recover", "")
            .await
            .status,
        409
    );
    let blocked = snapshot(reopened.as_ref(), "unsafe");
    assert_eq!(blocked.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(blocked.tasks[0].status, TaskStatus::OutcomeUnknown);
    assert_eq!(blocked.attempts_used, 1);
    assert_eq!(blocked.attempt_history, before.attempt_history);
    assert_eq!(calls[0].load(Ordering::SeqCst), 1);
    assert_eq!(
        exchange(replacement_server.address, "GET", "/readyz", "")
            .await
            .status,
        200
    );
    assert_eq!(handle.status().phase, ShutdownPhase::Draining);
    assert!(handle.status().faulted);
    replacement_server.stop().await;
}
