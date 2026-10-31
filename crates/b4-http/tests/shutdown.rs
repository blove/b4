use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
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
use tower::ServiceExt;

#[derive(Default)]
struct Gate {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn hold(&self) {
        self.entered.notify_one();
        let mut release = self.released.lock().unwrap();
        while !*release {
            release = self.changed.wait(release).unwrap();
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Auth {
    count: Arc<AtomicUsize>,
    gate: Option<Arc<Gate>>,
    panic: bool,
}
impl Authenticator for Auth {
    fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if let Some(g) = &self.gate {
            g.hold();
        }
        assert!(!self.panic, "private_auth_diagnostic");
        Ok(principal())
    }
}
fn principal() -> Principal {
    Principal {
        application: "app".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
struct Policy {
    count: Arc<AtomicUsize>,
    gate: Option<Arc<Gate>>,
    operation: Operation,
    hold_call: usize,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, c: &ResourceContext) -> bool {
        let call = self.count.fetch_add(1, Ordering::SeqCst);
        if c.operation == self.operation && call == self.hold_call {
            if let Some(g) = &self.gate {
                g.hold();
            }
        }
        true
    }
}
struct ProbeStore {
    inner: MemoryStore,
    reads: AtomicUsize,
    probes: AtomicUsize,
    ready_gate: Mutex<Option<Arc<Gate>>>,
    ready_failure: AtomicBool,
    create_hook: Mutex<Option<Arc<StoreHook>>>,
    view_hook: Mutex<Option<Arc<ViewHook>>>,
    approval_hook: Mutex<Option<Arc<ApprovalHook>>>,
}
type StoreHook = dyn Fn() -> Result<(), StoreError> + Send + Sync;
type ViewHook = dyn Fn(usize) -> Result<(), StoreError> + Send + Sync;
type ApprovalHook = dyn Fn() -> Result<(), ApprovalError> + Send + Sync;
impl ProbeStore {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            reads: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
            ready_gate: Mutex::new(None),
            ready_failure: AtomicBool::new(false),
            create_hook: Mutex::new(None),
            view_hook: Mutex::new(None),
            approval_hook: Mutex::new(None),
        }
    }
}
impl Store for ProbeStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, s: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        let hook = self.create_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook()?;
        }
        self.inner.create_thread_with(s)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        let call = self.reads.fetch_add(1, Ordering::SeqCst);
        let hook = self.view_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(call)?;
        }
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
        g: &ThreadGeneration,
        r: u64,
        s: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, g, r, s)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        f: ExecutionFence,
        p: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(id, f, p)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        let gate = self.ready_gate.lock().unwrap().clone();
        if let Some(g) = gate {
            g.hold();
        }
        if self.ready_failure.load(Ordering::SeqCst) {
            Err(StoreError::Unavailable("private_ready_diagnostic".into()))
        } else {
            Ok(())
        }
    }
    fn delete_thread_fenced(&self, t: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(t)
    }
    fn cancel_run_fenced(
        &self,
        t: &RunCancellationTarget,
        f: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(t, f)
    }
}
impl ApprovalStore for ProbeStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        let hook = self.approval_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook()?;
        }
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        f: ApprovalFence,
        p: CommitPlan,
        m: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, f, p, m)
    }
}
fn definition() -> Definition {
    Definition {
        route: RouteKey::new("/work#workflow").unwrap(),
        revision: ExecutionRevision::new("work-v1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("work").unwrap(),
            effect_id: EffectId::new("work").unwrap(),
            requires_approval: false,
            recovery: RecoveryPolicy::Pure,
        }]],
        max_attempts: 1,
    }
}
fn script() -> HostConfig {
    HostConfig::new(vec![ScriptRoute {
        definition: definition(),
        effects: BTreeMap::from([(EffectId::new("work").unwrap(), ScriptEffect::Input)]),
    }])
}
fn auth(count: Arc<AtomicUsize>) -> Arc<Auth> {
    Arc::new(Auth {
        count,
        gate: None,
        panic: false,
    })
}
fn policy(count: Arc<AtomicUsize>) -> Arc<Policy> {
    Arc::new(Policy {
        count,
        gate: None,
        operation: Operation::Unsupported,
        hold_call: usize::MAX,
    })
}
fn request(method: &str, path: &str, payload: Option<Value>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(
            payload.map(|v| v.to_string()).unwrap_or_default(),
        ))
        .unwrap()
}
async fn json_body(r: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(r.into_body(), 1_000_000).await.unwrap()).unwrap()
}
async fn quiescent(h: &ShutdownHandle, faulted: bool) {
    assert_eq!(
        h.wait_quiescent(Duration::from_secs(5)).await.unwrap(),
        ShutdownOutcome::Quiescent { faulted }
    );
}

#[tokio::test]
async fn closed_protected_requests_refuse_before_auth_body_policy_or_store() {
    let store = Arc::new(ProbeStore::new());
    let a = Arc::new(AtomicUsize::new(0));
    let p = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new(store.clone(), auth(a.clone()), policy(p.clone()), script()).unwrap();
    let probes = store.probes.load(Ordering::SeqCst);
    let h = host.shutdown_handle();
    h.begin_shutdown();
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
        let response = host
            .router()
            .oneshot(request(method, path, Some(json!({"invalid":"body"}))))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(
            json_body(response).await["error"]["details"]["code"],
            "host_draining"
        );
    }
    assert_eq!(a.load(Ordering::SeqCst), 0);
    assert_eq!(p.load(Ordering::SeqCst), 0);
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    assert_eq!(store.probes.load(Ordering::SeqCst), probes);
    assert_eq!(
        host.router()
            .oneshot(request("GET", "/healthz", None))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        host.router()
            .oneshot(request("GET", "/readyz", None))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(store.probes.load(Ordering::SeqCst), probes);
    quiescent(&h, false).await;
}
#[tokio::test]
async fn abandoned_auth_job_stays_owned_and_records_its_panic_before_retirement() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let host = HttpHost::new(
        Arc::new(ProbeStore::new()),
        Arc::new(Auth {
            count: Arc::new(AtomicUsize::new(0)),
            gate: Some(gate.clone()),
            panic: true,
        }),
        policy(Arc::new(AtomicUsize::new(0))),
        script(),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let request = tokio::spawn(
        host.router()
            .oneshot(request("GET", "/threads/missing", None)),
    );
    gate.entered().await;
    request.abort();
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    quiescent(&h, true).await;
}
#[tokio::test]
async fn held_resource_preflight_loses_cut_without_implicit_creation() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: Arc::new(AtomicUsize::new(0)),
            gate: Some(gate.clone()),
            operation: Operation::RunWait,
            hold_call: 0,
        }),
        script(),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/new/runs/wait",
        Some(json!({"route":"/work#workflow"})),
    )));
    gate.entered().await;
    h.begin_shutdown();
    gate.release();
    let response = job.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "host_draining"
    );
    quiescent(&h, false).await;
    assert!(store
        .inner
        .thread_view(&ThreadId::new("new").unwrap())
        .unwrap()
        .is_none());
}
struct Callback {
    gate: Option<Arc<Gate>>,
    calls: Arc<AtomicUsize>,
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(g) = &self.gate {
            g.hold();
        }
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("actual"),
            writes: vec![],
        }))
    }
}
struct Validator(Arc<Gate>);
impl InputValidator for Validator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.hold();
        Ok(())
    }
}
fn native_config(
    callback: Arc<dyn NativeEffect>,
    validator: Option<Arc<dyn InputValidator>>,
) -> NativeHostConfig {
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "app".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                definition: definition(),
                effects: BTreeMap::from([(
                    EffectId::new("work").unwrap(),
                    EffectBinding {
                        callback,
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: validator,
            }],
        )
        .unwrap(),
    );
    let mut approval = ApprovalHostConfig::new("policy-v1".into(), BTreeMap::new());
    approval.service_config = ApprovalServiceConfig::VolatileFixture;
    NativeHostConfig::new(application, approval)
}
#[tokio::test]
async fn abandoned_validator_retains_owner_but_cannot_create_after_cut() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        native_config(
            Arc::new(Callback {
                gate: None,
                calls: calls.clone(),
            }),
            Some(Arc::new(Validator(gate.clone()))),
        ),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/validated/runs/wait",
        Some(json!({"route":"/work#workflow"})),
    )));
    gate.entered().await;
    job.abort();
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    quiescent(&h, false).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(store
        .inner
        .thread_view(&ThreadId::new("validated").unwrap())
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn admitted_callback_finishes_with_actual_result_after_cut_and_abandonment() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        native_config(
            Arc::new(Callback {
                gate: Some(gate.clone()),
                calls: calls.clone(),
            }),
            None,
        ),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/admitted/runs/wait",
        Some(json!({"route":"/work#workflow"})),
    )));
    gate.entered().await;
    job.abort();
    assert_eq!(
        h.wait_quiescent(Duration::from_millis(5)).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    quiescent(&h, false).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let view = store
        .inner
        .thread_view(&ThreadId::new("admitted").unwrap())
        .unwrap()
        .unwrap();
    let snapshot = view.execution.unwrap().snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Completed);
    assert_eq!(
        snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("actual")
    );
}
#[tokio::test]
async fn abandoned_readiness_error_is_owned_and_faulted_and_postcut_probe_is_skipped() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        script(),
    )
    .unwrap();
    *store.ready_gate.lock().unwrap() = Some(gate.clone());
    store.ready_failure.store(true, Ordering::SeqCst);
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request("GET", "/readyz", None)));
    gate.entered().await;
    job.abort();
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    let probes = store.probes.load(Ordering::SeqCst);
    assert_eq!(
        host.router()
            .oneshot(request("GET", "/readyz", None))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(store.probes.load(Ordering::SeqCst), probes);
    gate.release();
    quiescent(&h, true).await;
}

#[tokio::test]
async fn abandoned_panic_payload_is_destroyed_before_final_owner_retirement() {
    struct Payload {
        handle: ShutdownHandle,
        observed: Arc<AtomicBool>,
        dropped: Arc<tokio::sync::Notify>,
        _store: Arc<ProbeStore>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.observed
                .store(self.handle.status().active_owners > 0, Ordering::SeqCst);
            self.dropped.notify_one();
        }
    }
    struct PayloadAuth {
        gate: Arc<Gate>,
        handle: Mutex<Option<ShutdownHandle>>,
        observed: Arc<AtomicBool>,
        dropped: Arc<tokio::sync::Notify>,
        store: std::sync::Weak<ProbeStore>,
    }
    impl Authenticator for PayloadAuth {
        fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
            self.gate.hold();
            std::panic::panic_any(Payload {
                handle: self.handle.lock().unwrap().clone().unwrap(),
                observed: self.observed.clone(),
                dropped: self.dropped.clone(),
                _store: self.store.upgrade().unwrap(),
            });
        }
    }
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let observed = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(tokio::sync::Notify::new());
    let auth = Arc::new(PayloadAuth {
        gate: gate.clone(),
        handle: Mutex::new(None),
        observed: observed.clone(),
        dropped: dropped.clone(),
        store: Arc::downgrade(&store),
    });
    let host = HttpHost::new(
        store,
        auth.clone(),
        policy(Arc::new(AtomicUsize::new(0))),
        script(),
    )
    .unwrap();
    let h = host.shutdown_handle();
    *auth.handle.lock().unwrap() = Some(h.clone());
    let job = tokio::spawn(
        host.router()
            .oneshot(request("GET", "/threads/missing", None)),
    );
    gate.entered().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    drop(host);
    h.begin_shutdown();
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    assert!(
        observed.load(Ordering::SeqCst),
        "opaque panic payload escaped counted ownership"
    );
    quiescent(&h, true).await;
}

#[tokio::test]
async fn explicit_create_policy_loses_cut_before_store_mutation() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let creates = Arc::new(AtomicUsize::new(0));
    let counter = creates.clone();
    *store.create_hook.lock().unwrap() = Some(Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    let host = HttpHost::new(
        store,
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: Arc::new(AtomicUsize::new(0)),
            gate: Some(gate.clone()),
            operation: Operation::CreateThread,
            hold_call: 0,
        }),
        script(),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(
        host.router()
            .oneshot(request("POST", "/threads", Some(json!({})))),
    );
    gate.entered().await;
    h.begin_shutdown();
    gate.release();
    let response = job.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "host_draining"
    );
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    quiescent(&h, false).await;
}
#[tokio::test]
async fn leased_abandoned_create_retains_store_and_latches_late_failure() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        script(),
    )
    .unwrap();
    let held = gate.clone();
    *store.create_hook.lock().unwrap() = Some(Arc::new(move || {
        held.hold();
        Err(StoreError::Unavailable("private_create_failure".into()))
    }));
    let h = host.shutdown_handle();
    let job = tokio::spawn(
        host.router()
            .oneshot(request("POST", "/threads", Some(json!({})))),
    );
    gate.entered().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    quiescent(&h, true).await;
}
fn seed_idle(store: &ProbeStore, id: &str) -> ThreadView {
    let id = ThreadId::new(id).unwrap();
    store
        .inner
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Default::default(),
            origin: Some(ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    store.inner.thread_view(&id).unwrap().unwrap()
}
#[tokio::test]
async fn delete_renewed_policy_loses_cut_without_removing_original_thread() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let original = seed_idle(&store, "delete");
    let host = HttpHost::new(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: Arc::new(AtomicUsize::new(0)),
            gate: Some(gate.clone()),
            operation: Operation::DeleteThread,
            hold_call: 1,
        }),
        script(),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(
        host.router()
            .oneshot(request("DELETE", "/threads/delete", None)),
    );
    gate.entered().await;
    h.begin_shutdown();
    gate.release();
    let response = job.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "host_draining"
    );
    quiescent(&h, false).await;
    assert_eq!(
        store
            .inner
            .thread_view(&original.thread.id)
            .unwrap()
            .unwrap(),
        original
    );
}
#[tokio::test]
async fn abandoned_initial_authorization_and_attachment_read_failures_stay_counted() {
    for path in ["/threads/read", "/threads/read/runs/stream"] {
        let gate = Arc::new(Gate::default());
        let _release = Release(gate.clone());
        let store = Arc::new(ProbeStore::new());
        let host = HttpHost::new(
            store.clone(),
            auth(Arc::new(AtomicUsize::new(0))),
            policy(Arc::new(AtomicUsize::new(0))),
            script(),
        )
        .unwrap();
        let held = gate.clone();
        *store.view_hook.lock().unwrap() = Some(Arc::new(move |_| {
            held.hold();
            Err(StoreError::Corrupt("private_view_failure".into()))
        }));
        let h = host.shutdown_handle();
        let job = tokio::spawn(host.router().oneshot(request("GET", path, None)));
        gate.entered().await;
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        assert_eq!(
            h.wait_quiescent(Duration::ZERO).await.unwrap(),
            ShutdownOutcome::Incomplete {
                active_owners: 1,
                faulted: false
            }
        );
        gate.release();
        quiescent(&h, true).await;
    }
}

#[tokio::test]
async fn cancel_renewal_losing_cut_never_freezes_or_cancels_entered_callback() {
    let callback_gate = Arc::new(Gate::default());
    let _callback_release = Release(callback_gate.clone());
    let cancel_gate = Arc::new(Gate::default());
    let _cancel_release = Release(cancel_gate.clone());
    let store = Arc::new(ProbeStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_application(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: count.clone(),
            gate: Some(cancel_gate.clone()),
            operation: Operation::CancelRun,
            hold_call: 1,
        }),
        native_config(
            Arc::new(Callback {
                gate: Some(callback_gate.clone()),
                calls: calls.clone(),
            }),
            None,
        ),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let run = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/cancel/runs/wait",
        Some(json!({"route":"/work#workflow"})),
    )));
    callback_gate.entered().await;
    count.store(0, Ordering::SeqCst);
    let cancel = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/cancel/cancel",
        None,
    )));
    cancel_gate.entered().await;
    h.begin_shutdown();
    cancel_gate.release();
    let response = cancel.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "host_draining"
    );
    assert_eq!(h.status().active_owners, 1);
    callback_gate.release();
    let response = run.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await, "actual");
    quiescent(&h, false).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .inner
            .thread_view(&ThreadId::new("cancel").unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::Completed
    );
}

fn approval_route() -> ScriptRoute {
    let mut route = script().routes.remove(0);
    route.definition.batches[0][0].requires_approval = true;
    route
}
fn approvals() -> ApprovalHostConfig {
    let mut config = ApprovalHostConfig::new(
        "policy-v1".into(),
        BTreeMap::from([(
            definition().route,
            BTreeMap::from([(TaskId::new("work").unwrap(), json!({"question":"allow?"}))]),
        )]),
    );
    config.service_config = ApprovalServiceConfig::VolatileFixture;
    config
}
async fn settle_park(host: &HttpHost<ProbeStore>, store: &ProbeStore) -> ApprovalView {
    let response = host
        .router()
        .oneshot(request(
            "POST",
            "/threads/park/runs/wait",
            Some(json!({"route":"/work#workflow"})),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await, Value::Null);
    tokio::time::timeout(Duration::from_secs(5), async {
        while host.shutdown_handle().status().active_owners > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let view = store
        .inner
        .thread_view(&ThreadId::new("park").unwrap())
        .unwrap()
        .unwrap();
    store
        .inner
        .approval_view(&view.execution.unwrap().snapshot.execution_id)
        .unwrap()
        .unwrap()
}
fn resume_payload(grant: &GrantRecord) -> Value {
    json!({"route":"/work#workflow","resume":[{"status":"resolved","interruptId":grant.interrupt_id,"payload":"once","grant":grant.token.as_str()}]})
}
#[tokio::test]
async fn resume_initial_policy_loses_cut_without_consuming_once() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_approvals(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: count.clone(),
            gate: Some(gate.clone()),
            operation: Operation::Resume,
            hold_call: 0,
        }),
        HostConfig::new(vec![approval_route()]),
        approvals(),
    )
    .unwrap();
    let before = settle_park(&host, &store).await;
    count.store(0, Ordering::SeqCst);
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/park/resume",
        Some(resume_payload(&before.grants[0])),
    )));
    gate.entered().await;
    h.begin_shutdown();
    gate.release();
    let response = job.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "host_draining"
    );
    quiescent(&h, false).await;
    assert_eq!(
        store
            .inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        before
    );
}
#[tokio::test]
async fn resume_winning_lease_finishes_once_across_internal_service_policy_wait() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new_with_approvals(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: count.clone(),
            gate: Some(gate.clone()),
            operation: Operation::Resume,
            hold_call: 1,
        }),
        HostConfig::new(vec![approval_route()]),
        approvals(),
    )
    .unwrap();
    let before = settle_park(&host, &store).await;
    count.store(0, Ordering::SeqCst);
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/park/resume",
        Some(resume_payload(&before.grants[0])),
    )));
    gate.entered().await;
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    let response = job.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    assert!(String::from_utf8(bytes.to_vec())
        .unwrap()
        .contains("event: done"));
    quiescent(&h, false).await;
    let after = store
        .inner
        .approval_view(&before.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(after.grants[0].consumed);
    assert_eq!(after.execution.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(after.execution.snapshot.attempts_used, 1);
}
#[tokio::test]
async fn abandoned_pending_projection_failure_faults_before_owned_job_retires() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new_with_approvals(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        HostConfig::new(vec![approval_route()]),
        approvals(),
    )
    .unwrap();
    let before = settle_park(&host, &store).await;
    let held = gate.clone();
    *store.approval_hook.lock().unwrap() = Some(Arc::new(move || {
        held.hold();
        Err(ApprovalError::Store(StoreError::Unavailable(
            "private_pending_failure".into(),
        )))
    }));
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "GET",
        "/threads/park/pending_interrupts",
        None,
    )));
    gate.entered().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    quiescent(&h, true).await;
    assert_eq!(
        store
            .inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn successful_held_readiness_rechecks_another_job_fault_at_response_cut() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth {
            count: Arc::new(AtomicUsize::new(0)),
            gate: None,
            panic: true,
        }),
        policy(Arc::new(AtomicUsize::new(0))),
        script(),
    )
    .unwrap();
    *store.ready_gate.lock().unwrap() = Some(gate.clone());
    let h = host.shutdown_handle();
    let ready = tokio::spawn(host.router().oneshot(request("GET", "/readyz", None)));
    gate.entered().await;
    assert_eq!(
        host.router()
            .oneshot(request("GET", "/threads/fault", None))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(h.status().faulted);
    assert_eq!(h.status().phase, ShutdownPhase::Serving);
    gate.release();
    assert_eq!(
        ready.await.unwrap().unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    quiescent(&h, true).await;
}

#[tokio::test]
async fn request_body_wait_is_counted_and_abandonment_retires_without_store_work() {
    struct HeldBody {
        entered: Arc<tokio::sync::Notify>,
        receiver: tokio::sync::oneshot::Receiver<axum::body::Bytes>,
    }
    impl futures_core::Stream for HeldBody {
        type Item = Result<axum::body::Bytes, std::convert::Infallible>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.entered.notify_one();
            match std::future::Future::poll(std::pin::Pin::new(&mut self.receiver), cx) {
                std::task::Poll::Ready(Ok(bytes)) => std::task::Poll::Ready(Some(Ok(bytes))),
                std::task::Poll::Ready(Err(_)) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }
    }
    let store = Arc::new(ProbeStore::new());
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(policy_calls.clone()),
        script(),
    )
    .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let (_send, receiver) = tokio::sync::oneshot::channel();
    let request = Request::builder()
        .method("POST")
        .uri("/threads/body/runs/wait")
        .body(Body::from_stream(HeldBody {
            entered: entered.clone(),
            receiver,
        }))
        .unwrap();
    let job = tokio::spawn(host.router().oneshot(request));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let h = host.shutdown_handle();
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    quiescent(&h, false).await;
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recovery_final_host_preflight_loses_cut_without_adopting_prior_owner() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let dir = Directory(
        std::env::temp_dir().join(format!("b4-shutdown-recover-{}", uuid::Uuid::new_v4())),
    );
    let target;
    {
        let old = b4_sqlite::SqliteStore::open(&dir.0).unwrap();
        let id = ThreadId::new("recover").unwrap();
        old.create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Default::default(),
            origin: Some(ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
        let snapshot = b4_kernel::new_execution(
            ExecutionId::new("recover-execution").unwrap(),
            TurnId::new("recover-turn").unwrap(),
            old.owner_generation(),
            definition(),
            Default::default(),
            TransitionFacts { now_ms: 2 },
        )
        .unwrap();
        Runtime::new(&old).admit(&id, snapshot).unwrap();
        target = RecoveryTarget::from_view(&old.thread_view(&id).unwrap().unwrap()).unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&dir.0).unwrap());
    let before = store.thread_view(&target.thread_id).unwrap().unwrap();
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let host = HttpHost::new_with_approvals(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: Arc::new(AtomicUsize::new(0)),
            gate: Some(gate.clone()),
            operation: Operation::Recover,
            hold_call: 1,
        }),
        script(),
        ApprovalHostConfig::new("policy-v1".into(), BTreeMap::new()),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let caller = host.clone();
    let job = tokio::spawn(async move { caller.recover_execution(principal(), target).await });
    gate.entered().await;
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    let response = job.await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "host_draining"
    );
    quiescent(&h, false).await;
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap().unwrap(),
        before
    );
}

#[tokio::test]
async fn abandoned_recovery_never_returns_registry_capture_past_owner_retirement() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct CallbackCapture {
        handle: Arc<Mutex<Option<ShutdownHandle>>>,
        observed: Arc<AtomicBool>,
        dropped: Arc<tokio::sync::Notify>,
        _store: Arc<b4_sqlite::SqliteStore>,
    }
    impl Drop for CallbackCapture {
        fn drop(&mut self) {
            self.observed.store(
                self.handle
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .status()
                    .active_owners
                    > 0,
                Ordering::SeqCst,
            );
            self.dropped.notify_one();
        }
    }
    impl NativeEffect for CallbackCapture {
        fn invoke(
            &self,
            _: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            panic!("callback_must_not_enter")
        }
    }
    let dir = Directory(std::env::temp_dir().join(format!(
        "b4-shutdown-registry-drop-{}",
        uuid::Uuid::new_v4()
    )));
    let target;
    {
        let old = b4_sqlite::SqliteStore::open(&dir.0).unwrap();
        let id = ThreadId::new("recover-capture").unwrap();
        old.create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Default::default(),
            origin: Some(ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
        let initial = b4_kernel::new_execution(
            ExecutionId::new("capture-execution").unwrap(),
            TurnId::new("capture-turn").unwrap(),
            old.owner_generation(),
            definition(),
            Default::default(),
            TransitionFacts { now_ms: 2 },
        )
        .unwrap();
        Runtime::new(&old).admit(&id, initial).unwrap();
        target = RecoveryTarget::from_view(&old.thread_view(&id).unwrap().unwrap()).unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&dir.0).unwrap());
    let callback_handle = Arc::new(Mutex::new(None));
    let observed = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(tokio::sync::Notify::new());
    let callback = Arc::new(CallbackCapture {
        handle: callback_handle.clone(),
        observed: observed.clone(),
        dropped: dropped.clone(),
        _store: store.clone(),
    });
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let host = HttpHost::new_with_application(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        Arc::new(Policy {
            count: Arc::new(AtomicUsize::new(0)),
            gate: Some(gate.clone()),
            operation: Operation::Recover,
            hold_call: 0,
        }),
        native_config(callback, None),
    )
    .unwrap();
    let h = host.shutdown_handle();
    *callback_handle.lock().unwrap() = Some(h.clone());
    let caller = host.clone();
    let job = tokio::spawn(async move { caller.recover_execution(principal(), target).await });
    gate.entered().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    drop(host);
    drop(store);
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    assert!(
        observed.load(Ordering::SeqCst),
        "returned registry capture escaped counted ownership"
    );
    quiescent(&h, false).await;
    let reopened = b4_sqlite::SqliteStore::open(&dir.0).unwrap();
    assert_eq!(reopened.owner_generation(), 3);
}

#[tokio::test]
async fn already_admitted_ready_sibling_dispatches_after_cut() {
    struct FirstHeld {
        gate: Arc<Gate>,
        calls: Arc<Mutex<Vec<TaskId>>>,
    }
    impl NativeEffect for FirstHeld {
        fn invoke(
            &self,
            call: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            let first = {
                let mut calls = self.calls.lock().unwrap();
                calls.push(call.task_id.clone());
                calls.len() == 1
            };
            if first {
                self.gate.hold();
            }
            Ok(NativeOutcome::Succeeded(TaskResult {
                output: json!(call.task_id),
                writes: vec![],
            }))
        }
    }
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(ProbeStore::new());
    let mut definition = definition();
    let mut sibling = definition.batches[0][0].clone();
    sibling.id = TaskId::new("sibling").unwrap();
    sibling.effect_id = EffectId::new("sibling").unwrap();
    definition.batches[0].push(sibling);
    definition.max_attempts = 2;
    let callback = Arc::new(FirstHeld {
        gate: gate.clone(),
        calls: calls.clone(),
    });
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "app".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                definition,
                effects: BTreeMap::from([
                    (
                        EffectId::new("work").unwrap(),
                        EffectBinding {
                            callback: callback.clone(),
                            output_kind: OutputKind::Value,
                        },
                    ),
                    (
                        EffectId::new("sibling").unwrap(),
                        EffectBinding {
                            callback,
                            output_kind: OutputKind::Value,
                        },
                    ),
                ]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let mut approval = ApprovalHostConfig::new("policy-v1".into(), BTreeMap::new());
    approval.service_config = ApprovalServiceConfig::VolatileFixture;
    let mut config = NativeHostConfig::new(application, approval);
    config.limits.max_tasks_per_execution = 1;
    config.limits.max_concurrent_tasks = 1;
    let host = HttpHost::new_with_application(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        config,
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/siblings/runs/wait",
        Some(json!({"route":"/work#workflow"})),
    )));
    gate.entered().await;
    let held = store
        .inner
        .thread_view(&ThreadId::new("siblings").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(held.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(held.tasks[1].status, TaskStatus::Ready);
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    let response = job.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await, "sibling");
    quiescent(&h, false).await;
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            TaskId::new("work").unwrap(),
            TaskId::new("sibling").unwrap()
        ]
    );
    let snapshot = store
        .inner
        .thread_view(&ThreadId::new("siblings").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Completed);
    assert_eq!(snapshot.attempts_used, 2);
}
#[tokio::test]
async fn prompt_cancelled_wait_is_not_physical_quiescence() {
    struct LateSuccess {
        gate: Arc<Gate>,
        calls: Arc<AtomicUsize>,
    }
    impl NativeEffect for LateSuccess {
        fn invoke(
            &self,
            _: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.gate.hold();
            Ok(NativeOutcome::Succeeded(TaskResult {
                output: json!("actual"),
                writes: vec![StateWrite::Replace {
                    field: "late".into(),
                    value: json!(true),
                }],
            }))
        }
    }
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        native_config(
            Arc::new(LateSuccess {
                gate: gate.clone(),
                calls: calls.clone(),
            }),
            None,
        ),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/drain-cancel/runs/wait",
        Some(json!({"route":"/work#workflow"})),
    )));
    gate.entered().await;
    let cancel = host
        .router()
        .oneshot(request("POST", "/threads/drain-cancel/cancel", None))
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::OK);
    let wait = job.await.unwrap().unwrap();
    assert_eq!(wait.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(wait).await["error"]["details"]["code"],
        "run_cancelled"
    );
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    let held = store
        .inner
        .thread_view(&ThreadId::new("drain-cancel").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(held.status, ExecutionStatus::CancelRequested);
    assert_eq!(held.tasks[0].status, TaskStatus::InFlight);
    gate.release();
    quiescent(&h, false).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let snapshot = store
        .inner
        .thread_view(&ThreadId::new("drain-cancel").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert!(!snapshot.values.contains_key("late"));
    assert_eq!(snapshot.tasks[0].result.as_ref().unwrap().writes.len(), 1);
    assert_eq!(
        snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("actual")
    );
}
#[tokio::test]
async fn uncertain_callback_is_faulted_physical_quiescence_with_inflight_evidence() {
    struct Uncertain(Arc<Gate>);
    impl NativeEffect for Uncertain {
        fn invoke(
            &self,
            _: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            self.0.hold();
            Err(EffectUncertain)
        }
    }
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(ProbeStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(Arc::new(AtomicUsize::new(0))),
        native_config(Arc::new(Uncertain(gate.clone())), None),
    )
    .unwrap();
    let h = host.shutdown_handle();
    let job = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/unknown/runs/wait",
        Some(json!({"route":"/work#workflow"})),
    )));
    gate.entered().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(
        h.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    quiescent(&h, true).await;
    let snapshot = store
        .inner
        .thread_view(&ThreadId::new("unknown").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Running);
    assert_eq!(snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(snapshot.tasks[0].result.is_none());
    assert_eq!(snapshot.attempts_used, 1);
}
#[tokio::test]
async fn closed_embedding_recovery_refuses_before_store_or_policy() {
    let store = Arc::new(ProbeStore::new());
    let view = seed_idle(&store, "embedding");
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new("embedding-execution").unwrap(),
        TurnId::new("embedding-turn").unwrap(),
        store.owner_generation(),
        definition(),
        Default::default(),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    Runtime::new(&store.inner)
        .admit(&view.thread.id, snapshot)
        .unwrap();
    let target =
        RecoveryTarget::from_view(&store.inner.thread_view(&view.thread.id).unwrap().unwrap())
            .unwrap();
    let policies = Arc::new(AtomicUsize::new(0));
    let host = HttpHost::new(
        store.clone(),
        auth(Arc::new(AtomicUsize::new(0))),
        policy(policies.clone()),
        script(),
    )
    .unwrap();
    let h = host.shutdown_handle();
    h.begin_shutdown();
    let response = host.recover_execution(principal(), target).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "host_draining"
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    assert_eq!(policies.load(Ordering::SeqCst), 0);
    quiescent(&h, false).await;
}

#[tokio::test]
async fn replacement_panic_payload_drop_keeps_store_owned_until_it_returns() {
    struct ReplacementPayload {
        _store: Arc<ProbeStore>,
        gate: Arc<Gate>,
    }
    impl Drop for ReplacementPayload {
        fn drop(&mut self) {
            self.gate.hold();
        }
    }
    struct FirstPayload {
        store: Option<Arc<ProbeStore>>,
        gate: Arc<Gate>,
    }
    impl Drop for FirstPayload {
        fn drop(&mut self) {
            std::panic::panic_any(ReplacementPayload {
                _store: self.store.take().unwrap(),
                gate: self.gate.clone(),
            });
        }
    }
    struct ReplacingAuth {
        store: Arc<ProbeStore>,
        auth_gate: Arc<Gate>,
        drop_gate: Arc<Gate>,
    }
    impl Authenticator for ReplacingAuth {
        fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
            self.auth_gate.hold();
            std::panic::panic_any(FirstPayload {
                store: Some(self.store.clone()),
                gate: self.drop_gate.clone(),
            });
        }
    }
    let auth_gate = Arc::new(Gate::default());
    let drop_gate = Arc::new(Gate::default());
    let _auth_release = Release(auth_gate.clone());
    let _drop_release = Release(drop_gate.clone());
    let store = Arc::new(ProbeStore::new());
    let weak = Arc::downgrade(&store);
    let host = HttpHost::new(
        store.clone(),
        Arc::new(ReplacingAuth {
            store: store.clone(),
            auth_gate: auth_gate.clone(),
            drop_gate: drop_gate.clone(),
        }),
        policy(Arc::new(AtomicUsize::new(0))),
        script(),
    )
    .unwrap();
    let handle = host.shutdown_handle();
    let job = tokio::spawn(
        host.router()
            .oneshot(request("GET", "/threads/replacement", None)),
    );
    auth_gate.entered().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(handle.begin_shutdown().active_owners, 1);
    drop(host);
    drop(store);
    auth_gate.release();
    drop_gate.entered().await;
    assert!(weak.upgrade().is_some());
    assert_eq!(
        handle.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: true,
        }
    );
    drop_gate.release();
    quiescent(&handle, true).await;
    assert!(weak.upgrade().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn carrier_destructor_panic_disposal_stays_owned_in_abandoned_jobs() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct Payload {
        _store: Arc<b4_sqlite::SqliteStore>,
        gate: Arc<Gate>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.gate.hold();
        }
    }
    struct DestructingAuth {
        store: Arc<b4_sqlite::SqliteStore>,
        gate: Arc<Gate>,
        payload: Arc<Gate>,
    }
    impl Authenticator for DestructingAuth {
        fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
            self.gate.hold();
            Ok(principal())
        }
    }
    impl Drop for DestructingAuth {
        fn drop(&mut self) {
            std::panic::panic_any(Payload {
                _store: self.store.clone(),
                gate: self.payload.clone(),
            });
        }
    }
    let directory = Directory(std::env::temp_dir().join(format!(
        "b4-shutdown-carrier-destructor-{}",
        uuid::Uuid::new_v4()
    )));
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let entered = Arc::new(Gate::default());
    let payload = Arc::new(Gate::default());
    let _entered_release = Release(entered.clone());
    let _payload_release = Release(payload.clone());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(DestructingAuth {
            store: store.clone(),
            gate: entered.clone(),
            payload: payload.clone(),
        }),
        policy(Arc::new(AtomicUsize::new(0))),
        script(),
    )
    .unwrap();
    let handle = host.shutdown_handle();
    let job = tokio::spawn(
        host.router()
            .oneshot(request("GET", "/threads/missing", None)),
    );
    entered.entered().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(handle.begin_shutdown().active_owners, 1);
    drop(host);
    drop(store);
    entered.release();
    payload.entered().await;
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    assert_eq!(
        handle.wait_quiescent(Duration::ZERO).await.unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: true
        }
    );
    payload.release();
    quiescent(&handle, true).await;
    let reopened = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
    assert_eq!(reopened.owner_generation(), 2);
}
