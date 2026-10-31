use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .ok_or(AuthenticationError)?
                .into(),
        })
    }
}
struct Allow;
impl ResourcePolicy for Allow {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        true
    }
}
fn route() -> ScriptRoute {
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/cancel#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: EffectId::new("effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }]],
            max_attempts: 2,
        },
        effects: BTreeMap::from([(EffectId::new("effect").unwrap(), ScriptEffect::Input)]),
    }
}
fn host<S: Store + 'static>(store: Arc<S>, policy: Arc<dyn ResourcePolicy>) -> HttpHost<S> {
    HttpHost::new(
        store,
        Arc::new(Auth),
        policy,
        HostConfig::new(vec![route()]),
    )
    .unwrap()
}
async fn call<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    actor: &str,
    payload: Option<Value>,
) -> axum::response::Response {
    host.router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", actor)
                .body(payload.map_or_else(Body::empty, |v| Body::from(v.to_string())))
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 200_000).await.unwrap()).unwrap()
}
fn seed<S: Store>(store: &S, name: &str, principal: Option<&str>) -> ThreadId {
    let id = ThreadId::new(name).unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: principal.map(|principal| ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: principal.into(),
            }),
        })
        .unwrap();
    id
}
#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, Duration::from_secs(5))
                .unwrap();
            state = next;
            assert!(!timeout.timed_out(), "cancellation gate not released");
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.state.lock().unwrap().0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}
struct GatedStore {
    inner: MemoryStore,
    gate: Arc<Gate>,
    boundary: &'static str,
    commits: AtomicUsize,
}
impl Store for GatedStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
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
        let first = self.commits.fetch_add(1, Ordering::SeqCst) == 0;
        if self.boundary == "start" && first {
            self.gate.block();
        }
        let dispatched = p
            .snapshot
            .tasks
            .iter()
            .any(|t| t.status == TaskStatus::InFlight);
        let result = self.inner.commit(id, f, p);
        if matches!(self.boundary, "dispatch" | "cancel-panic" | "cancel-fault") && dispatched {
            self.gate.block();
        }
        result
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
}
struct SupportedStore {
    inner: GatedStore,
    retries: AtomicUsize,
    capture_gate: Arc<Gate>,
    capture_hold: AtomicBool,
    execution_loads: AtomicUsize,
    producer_thread: Mutex<Option<std::thread::ThreadId>>,
}
impl Store for SupportedStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, s: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(s)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        let view = self.inner.thread_view(id);
        let is_producer =
            *self.producer_thread.lock().unwrap() == Some(std::thread::current().id());
        if !is_producer && self.capture_hold.swap(false, Ordering::SeqCst) {
            self.capture_gate.block();
        }
        view
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        if matches!(
            self.inner.boundary,
            "completion-clock" | "clock-without-control"
        ) && self.execution_loads.fetch_add(1, Ordering::SeqCst) == 2
        {
            self.inner.gate.block();
        }
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
        *self.producer_thread.lock().unwrap() = Some(std::thread::current().id());
        self.inner.admit_fenced(id, g, r, s)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        f: ExecutionFence,
        p: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let completion = p.snapshot.tasks.iter().any(|task| {
            task.result.is_some()
                || task.failure.is_some()
                || task.status == TaskStatus::Cancelled && task.attempt_id.is_some()
        });
        if completion {
            match self.inner.boundary {
                "completion-held" if self.retries.fetch_add(1, Ordering::SeqCst) == 0 => {
                    self.inner.gate.block()
                }
                "completion-retry" if self.retries.fetch_add(1, Ordering::SeqCst) == 0 => {
                    return Err(StoreError::RevisionConflict)
                }
                "completion-exhaust" => {
                    self.retries.fetch_add(1, Ordering::SeqCst);
                    return Err(StoreError::RevisionConflict);
                }
                "completion-fault" | "fault-during-cancel" => {
                    return Err(StoreError::Unavailable("private completion failure".into()))
                }
                "completion-panic" => panic!("private completion panic"),
                _ => {}
            }
        }
        self.inner.commit(id, f, p)
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
        if self.inner.boundary == "fault-during-cancel" {
            self.inner.gate.block();
        }
        match self.inner.boundary {
            "cancel-panic" => panic!("private cancellation panic"),
            "cancel-fault" => Err(StoreError::Unavailable(
                "private cancellation failure".into(),
            )),
            _ => self.inner.inner.cancel_run_fenced(target, facts),
        }
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.inner.delete_thread_fenced(target)
    }
}
fn supported(boundary: &'static str) -> (Arc<SupportedStore>, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    (
        Arc::new(SupportedStore {
            inner: GatedStore {
                inner: MemoryStore::new(),
                gate: gate.clone(),
                boundary,
                commits: AtomicUsize::new(0),
            },
            retries: AtomicUsize::new(0),
            capture_gate: Arc::new(Gate::default()),
            capture_hold: AtomicBool::new(false),
            execution_loads: AtomicUsize::new(0),
            producer_thread: Mutex::new(None),
        }),
        gate,
    )
}
impl ApprovalStore for SupportedStore {
    fn approval_durable(&self) -> bool {
        self.inner.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.inner.commit_approval(id, fence, plan, mutation)
    }
}
fn required_host<S: ApprovalStore + 'static>(
    store: Arc<S>,
    policy: Arc<dyn ResourcePolicy>,
) -> HttpHost<S> {
    let mut required = route();
    required.definition.batches[0][0].requires_approval = true;
    let mut config = ApprovalHostConfig::new(
        "policy-v1".into(),
        BTreeMap::from([(
            required.definition.route.clone(),
            BTreeMap::from([(TaskId::new("task").unwrap(), json!({"question":"allow?"}))]),
        )]),
    );
    config.service_config = ApprovalServiceConfig::VolatileFixture;
    HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        policy,
        HostConfig::new(vec![required]),
        config,
    )
    .unwrap()
}
async fn settled<S: Store>(store: &S, id: &str, status: ExecutionStatus) -> StoredExecution {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(execution) = store
                .thread_view(&ThreadId::new(id).unwrap())
                .unwrap()
                .and_then(|v| v.execution)
            {
                if execution.snapshot.status == status {
                    return execution;
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn accepted_start_gap_and_claimed_attempt_before_callback_cancel_without_results() {
    for boundary in ["start", "dispatch"] {
        let (store, gate) = supported(boundary);
        let host = host(store.clone(), Arc::new(Allow));
        let producer = {
            let h = host.clone();
            tokio::spawn(async move {
                call(
                    &h,
                    "POST",
                    "/threads/gated/runs/wait",
                    "owner",
                    Some(json!({"route":"/cancel#workflow","input":"must-not-return"})),
                )
                .await
            })
        };
        gate.entered().await;
        let cancel = call(&host, "POST", "/threads/gated/cancel", "owner", None).await;
        let cancel_status = cancel.status();
        let terminal = tokio::time::timeout(Duration::from_millis(500), producer).await;
        let deletion = call(&host, "DELETE", "/threads/gated", "owner", None)
            .await
            .status();
        let repeated = call(&host, "POST", "/threads/gated/cancel", "owner", None)
            .await
            .status();
        gate.release();
        assert_eq!(cancel_status, StatusCode::OK);
        assert_eq!(
            body(cancel).await,
            json!({"status":"interrupted","thread_id":"gated"})
        );
        let terminal = terminal.unwrap().unwrap();
        assert_eq!(terminal.status(), StatusCode::CONFLICT);
        assert_eq!(
            body(terminal).await["error"]["details"]["code"],
            "run_cancelled"
        );
        assert_eq!(deletion, StatusCode::CONFLICT);
        assert_eq!(repeated, StatusCode::CONFLICT);
        let current = settled(&*store, "gated", ExecutionStatus::Cancelled).await;
        assert_eq!(
            current.snapshot.attempts_used,
            if boundary == "start" { 0 } else { 1 }
        );
        assert!(current
            .snapshot
            .tasks
            .iter()
            .all(|task| task.status == TaskStatus::Cancelled
                && task.result.is_none()
                && task.failure.is_none()));
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::OK
        );
    }
}

struct RenewalPolicy {
    calls: AtomicUsize,
    gate: Arc<Gate>,
}
impl ResourcePolicy for RenewalPolicy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::CancelRun {
            assert!(context.requested_route.is_none());
            if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                self.gate.block();
                return false;
            }
        }
        true
    }
}
#[tokio::test]
async fn fresh_cancellation_policy_denial_preserves_the_claimed_execution() {
    let (store, effect_gate) = supported("dispatch");
    let policy_gate = Arc::new(Gate::default());
    let policy = Arc::new(RenewalPolicy {
        calls: AtomicUsize::new(0),
        gate: policy_gate.clone(),
    });
    let host = host(store.clone(), policy.clone());
    let producer = {
        let h = host.clone();
        tokio::spawn(async move {
            call(
                &h,
                "POST",
                "/threads/renew/runs/wait",
                "owner",
                Some(json!({"route":"/cancel#workflow"})),
            )
            .await
        })
    };
    effect_gate.entered().await;
    let before = store.thread_view(&ThreadId::new("renew").unwrap()).unwrap();
    let cancel = {
        let h = host.clone();
        tokio::spawn(async move { call(&h, "POST", "/threads/renew/cancel", "owner", None).await })
    };
    policy_gate.entered().await;
    policy_gate.release();
    let response = cancel.await.unwrap();
    let status = response.status();
    let after = store.thread_view(&ThreadId::new("renew").unwrap()).unwrap();
    effect_gate.release();
    assert_eq!(producer.await.unwrap().status(), StatusCode::OK);
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(before, after);
    assert_eq!(policy.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
}
#[tokio::test]
async fn producer_fault_during_pending_cancellation_never_restores_a_successful_gate() {
    let (store, cancel_gate) = supported("fault-during-cancel");
    let mut delayed = route();
    delayed.effects.insert(
        EffectId::new("effect").unwrap(),
        ScriptEffect::Delayed {
            duration_ms: 300,
            result: TaskResult {
                output: json!("private success"),
                writes: vec![],
            },
        },
    );
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![delayed]),
    )
    .unwrap();
    let producer = {
        let h = host.clone();
        tokio::spawn(async move {
            call(
                &h,
                "POST",
                "/threads/fault/runs/wait",
                "owner",
                Some(json!({"route":"/cancel#workflow"})),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if store
                .thread_view(&ThreadId::new("fault").unwrap())
                .unwrap()
                .is_some_and(|v| {
                    v.execution.is_some_and(|e| {
                        e.snapshot
                            .tasks
                            .iter()
                            .any(|t| t.status == TaskStatus::InFlight)
                    })
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let cancel = {
        let h = host.clone();
        tokio::spawn(async move { call(&h, "POST", "/threads/fault/cancel", "owner", None).await })
    };
    cancel_gate.entered().await;
    let terminal = tokio::time::timeout(Duration::from_secs(2), producer)
        .await
        .unwrap()
        .unwrap();
    cancel_gate.release();
    let response = cancel.await.unwrap();
    assert_eq!(terminal.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}
#[tokio::test]
async fn cancellation_infrastructure_failure_and_panic_block_a_never_entered_callback() {
    for boundary in ["cancel-panic", "cancel-fault"] {
        let (store, gate) = supported(boundary);
        let host = host(store.clone(), Arc::new(Allow));
        let producer = {
            let h = host.clone();
            tokio::spawn(async move {
                call(
                    &h,
                    "POST",
                    "/threads/fault/runs/wait",
                    "owner",
                    Some(json!({"route":"/cancel#workflow"})),
                )
                .await
            })
        };
        gate.entered().await;
        let response = call(&host, "POST", "/threads/fault/cancel", "owner", None).await;
        gate.release();
        let terminal = producer.await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(terminal.status(), StatusCode::SERVICE_UNAVAILABLE);
        let view = store
            .thread_view(&ThreadId::new("fault").unwrap())
            .unwrap()
            .unwrap();
        let task = &view.execution.unwrap().snapshot.tasks[0];
        assert_eq!(task.status, TaskStatus::InFlight);
        assert!(task.result.is_none());
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
struct ParkPolicy {
    calls: AtomicUsize,
    gate: Arc<Gate>,
}
impl ResourcePolicy for ParkPolicy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::RunWait && self.calls.fetch_add(1, Ordering::SeqCst) == 2
        {
            self.gate.block();
        }
        true
    }
}
#[tokio::test]
async fn cancellation_wins_required_task_park_authorization_without_minting_authority() {
    let store = Arc::new(MemoryStore::new());
    let gate = Arc::new(Gate::default());
    let host = required_host(
        store.clone(),
        Arc::new(ParkPolicy {
            calls: AtomicUsize::new(0),
            gate: gate.clone(),
        }),
    );
    let producer = {
        let h = host.clone();
        tokio::spawn(async move {
            call(
                &h,
                "POST",
                "/threads/park-race/runs/wait",
                "owner",
                Some(json!({"route":"/cancel#workflow"})),
            )
            .await
        })
    };
    gate.entered().await;
    let cancel = call(&host, "POST", "/threads/park-race/cancel", "owner", None).await;
    let status = cancel.status();
    let terminal = tokio::time::timeout(Duration::from_millis(500), producer).await;
    gate.release();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(terminal.unwrap().unwrap().status(), StatusCode::CONFLICT);
    let current = settled(&*store, "park-race", ExecutionStatus::Cancelled).await;
    let authority = store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(authority.grants.is_empty() && authority.acceptances.is_empty());
    assert_eq!(current.snapshot.attempts_used, 0);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
}
#[tokio::test]
async fn cancellation_after_once_dispatch_claim_acknowledges_own_attempt_and_keeps_receipt() {
    let (store, gate) = supported("dispatch");
    let host = required_host(store.clone(), Arc::new(Allow));
    let parked = call(
        &host,
        "POST",
        "/threads/once/runs/wait",
        "owner",
        Some(json!({"route":"/cancel#workflow"})),
    )
    .await;
    assert_eq!(parked.status(), StatusCode::OK);
    let parked = settled(&*store, "once", ExecutionStatus::WaitingForApproval).await;
    let authority = store
        .approval_view(&parked.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let grant = &authority.grants[0];
    let request = json!({"route":"/cancel#workflow","resume":[{"status":"resolved","interruptId":grant.interrupt_id,"payload":"once","grant":grant.token.as_str()}]});
    let producer = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = call(
                &host,
                "POST",
                "/threads/once/resume",
                "owner",
                Some(request.clone()),
            )
            .await;
            if response.status() == StatusCode::OK {
                break response;
            }
            assert_eq!(
                body(response).await["error"]["details"]["code"],
                "thread_busy"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    gate.entered().await;
    let consumed = store
        .approval_view(&parked.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(consumed.grants[0].consumed);
    assert_eq!(consumed.acceptances.len(), 1);
    let cancel = call(&host, "POST", "/threads/once/cancel", "owner", None).await;
    let status = cancel.status();
    let bytes = tokio::time::timeout(
        Duration::from_millis(500),
        to_bytes(producer.into_body(), 200_000),
    )
    .await;
    gate.release();
    assert_eq!(status, StatusCode::OK);
    let stream = String::from_utf8(bytes.unwrap().unwrap().to_vec()).unwrap();
    assert_eq!(stream.matches("event: done").count(), 1);
    assert!(stream.contains("\"cancelled\":true"));
    assert!(!stream.contains(grant.token.as_str()));
    let current = settled(&*store, "once", ExecutionStatus::Cancelled).await;
    let after = store
        .approval_view(&current.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(after.grants == consumed.grants && after.acceptances == consumed.acceptances);
    assert_eq!(current.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert!(current.snapshot.tasks[0].result.is_none());
    assert_eq!(current.snapshot.attempts_used, 1);
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn initial_view_and_local_control_capture_cannot_mix_two_successive_turns() {
    let (store, _) = supported("capture-race");
    let mut script = route();
    script.effects.insert(
        EffectId::new("effect").unwrap(),
        ScriptEffect::Delayed {
            duration_ms: 300,
            result: TaskResult {
                output: json!("completed"),
                writes: vec![],
            },
        },
    );
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![script]),
    )
    .unwrap();
    let first = call(
        &host,
        "POST",
        "/threads/capture/runs/stream",
        "owner",
        Some(json!({"route":"/cancel#workflow"})),
    )
    .await;
    store.capture_hold.store(true, Ordering::SeqCst);
    let cancel = {
        let h = host.clone();
        tokio::spawn(
            async move { call(&h, "POST", "/threads/capture/cancel", "owner", None).await },
        )
    };
    store.capture_gate.entered().await;
    let first_bytes = to_bytes(first.into_body(), 200_000).await.unwrap();
    assert!(String::from_utf8(first_bytes.to_vec())
        .unwrap()
        .contains("completed"));
    let successor = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = call(
                &host,
                "POST",
                "/threads/capture/runs/stream",
                "owner",
                Some(json!({"route":"/cancel#workflow"})),
            )
            .await;
            if response.status() == StatusCode::OK {
                break response;
            }
            assert_eq!(response.status(), StatusCode::CONFLICT);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let successor_id = store
        .thread_view(&ThreadId::new("capture").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
        .execution_id;
    store.capture_gate.release();
    let cancelled = cancel.await.unwrap();
    let status = cancelled.status();
    let bytes = to_bytes(successor.into_body(), 200_000).await.unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body(cancelled).await["error"]["details"]["code"],
        "no_run_in_flight"
    );
    assert!(String::from_utf8(bytes.to_vec())
        .unwrap()
        .contains("completed"));
    assert_eq!(
        store
            .execution(&successor_id)
            .unwrap()
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::Completed
    );
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn late_failure_and_chunked_success_remain_durable_without_a_second_wire_terminal() {
    for failure in [false, true] {
        let (store, gate) = supported("completion-held");
        let mut script = route();
        script.effects.insert(
            EffectId::new("effect").unwrap(),
            if failure {
                ScriptEffect::Failure("application failed".into())
            } else {
                ScriptEffect::Chunks {
                    chunks: vec![json!("late chunk")],
                    result: TaskResult {
                        output: json!("late result"),
                        writes: vec![StateWrite::Replace {
                            field: "must-not-reduce".into(),
                            value: json!(true),
                        }],
                    },
                }
            },
        );
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Allow),
            HostConfig::new(vec![script]),
        )
        .unwrap();
        let producer = call(
            &host,
            "POST",
            "/threads/late/runs/stream",
            "owner",
            Some(json!({"route":"/cancel#workflow"})),
        )
        .await;
        gate.entered().await;
        let cancel = call(&host, "POST", "/threads/late/cancel", "owner", None).await;
        let status = cancel.status();
        let terminal = tokio::time::timeout(
            Duration::from_millis(500),
            to_bytes(producer.into_body(), 200_000),
        )
        .await;
        gate.release();
        assert_eq!(status, StatusCode::OK);
        let stream = String::from_utf8(terminal.unwrap().unwrap().to_vec()).unwrap();
        assert_eq!(stream.matches("event: done").count(), 1);
        assert!(stream.contains("\"cancelled\":true"));
        assert!(
            !stream.contains("late chunk")
                && !stream.contains("late result")
                && !stream.contains("application failed")
        );
        let current = settled(
            &*store,
            "late",
            if failure {
                ExecutionStatus::Failed
            } else {
                ExecutionStatus::Cancelled
            },
        )
        .await;
        assert_eq!(current.snapshot.attempts_used, 1);
        assert!(!current.snapshot.values.contains_key("must-not-reduce"));
        if failure {
            assert_eq!(
                current.snapshot.tasks[0].failure.as_deref(),
                Some("application failed")
            );
        } else {
            assert_eq!(
                current.snapshot.tasks[0].result.as_ref().unwrap().output,
                json!("late result")
            );
        }
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn cancellation_between_completion_clock_capture_and_execution_load_retries_known_outcome() {
    let (store, gate) = supported("completion-clock");
    let host = host(store.clone(), Arc::new(Allow));
    let producer = {
        let h = host.clone();
        tokio::spawn(async move {
            call(
                &h,
                "POST",
                "/threads/clock/runs/wait",
                "owner",
                Some(json!({"route":"/cancel#workflow","input":"retained result"})),
            )
            .await
        })
    };
    gate.entered().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cancel = call(&host, "POST", "/threads/clock/cancel", "owner", None).await;
    let terminal = tokio::time::timeout(Duration::from_millis(500), producer).await;
    gate.release();
    assert_eq!(cancel.status(), StatusCode::OK);
    assert_eq!(terminal.unwrap().unwrap().status(), StatusCode::CONFLICT);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let current = store
                .thread_view(&ThreadId::new("clock").unwrap())
                .unwrap()
                .unwrap()
                .execution
                .unwrap();
            if current.snapshot.status == ExecutionStatus::Cancelled
                || call(&host, "GET", "/readyz", "owner", None).await.status()
                    == StatusCode::SERVICE_UNAVAILABLE
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let current = store
        .thread_view(&ThreadId::new("clock").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
    assert_eq!(current.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(current.snapshot.attempts_used, 1);
    assert_eq!(
        current.snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("retained result")
    );
}

#[tokio::test]
async fn retained_completion_retries_an_external_clock_conflict_without_local_control() {
    let (store, gate) = supported("clock-without-control");
    let host = host(store.clone(), Arc::new(Allow));
    let producer = {
        let h = host.clone();
        tokio::spawn(async move {
            call(
                &h,
                "POST",
                "/threads/external-clock/runs/wait",
                "owner",
                Some(json!({"route":"/cancel#workflow"})),
            )
            .await
        })
    };
    gate.entered().await;
    let current = store
        .thread_view(&ThreadId::new("external-clock").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    Runtime::new(&*store)
        .apply(
            &current.snapshot.execution_id,
            ExecutionFence::from(&current.snapshot),
            KernelCommand::Cancel {
                turn_id: current.snapshot.turn_id.clone(),
                owner_generation: current.snapshot.owner_generation,
            },
            TransitionFacts {
                now_ms: current.snapshot.updated_at_ms + 10_000,
            },
        )
        .unwrap();
    gate.release();
    let terminal = producer.await.unwrap();
    assert_eq!(terminal.status(), StatusCode::OK);
    assert_eq!(body(terminal).await, json!({"cancelled":true}));
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
    let current = store
        .thread_view(&ThreadId::new("external-clock").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(current.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(current.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(current.snapshot.attempts_used, 1);
    assert!(current.snapshot.tasks[0].result.is_some());
}

#[tokio::test]
async fn missing_idle_completed_and_unowned_work_are_healthy_non_cancellable() {
    let store = Arc::new(MemoryStore::new());
    seed(&*store, "idle", Some("owner"));
    let id = seed(&*store, "unowned", Some("owner"));
    store
        .admit(
            &id,
            b4_kernel::new_execution(
                ExecutionId::new("unowned-exec").unwrap(),
                TurnId::new("unowned-turn").unwrap(),
                1,
                route().definition,
                Map::new(),
                TransitionFacts { now_ms: 2 },
            )
            .unwrap(),
        )
        .unwrap();
    let host = host(store.clone(), Arc::new(Allow));
    assert_eq!(
        call(
            &host,
            "POST",
            "/threads/completed/runs/wait",
            "owner",
            Some(json!({"route":"/cancel#workflow"}))
        )
        .await
        .status(),
        StatusCode::OK
    );
    for name in ["idle", "completed", "unowned"] {
        let before = store.thread_view(&ThreadId::new(name).unwrap()).unwrap();
        let response = call(
            &host,
            "POST",
            &format!("/threads/{name}/cancel"),
            "owner",
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            body(response).await["error"]["details"]["code"],
            "no_run_in_flight"
        );
        assert_eq!(
            store.thread_view(&ThreadId::new(name).unwrap()).unwrap(),
            before
        );
    }
    assert_eq!(
        call(&host, "POST", "/threads/missing/cancel", "owner", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn cancellation_cannot_cross_principal_or_legacy_origin_even_with_permissive_policy() {
    let store = Arc::new(MemoryStore::new());
    seed(&*store, "foreign", Some("other"));
    seed(&*store, "legacy", None);
    let host = host(store.clone(), Arc::new(Allow));
    for name in ["foreign", "legacy"] {
        let before = store.thread_view(&ThreadId::new(name).unwrap()).unwrap();
        assert_eq!(
            call(
                &host,
                "POST",
                &format!("/threads/{name}/cancel"),
                "owner",
                None
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            store.thread_view(&ThreadId::new(name).unwrap()).unwrap(),
            before
        );
    }
}

#[tokio::test]
async fn default_custom_store_cancellation_is_healthy_unsupported_for_a_live_owned_turn() {
    let gate = Arc::new(Gate::default());
    let store = Arc::new(GatedStore {
        inner: MemoryStore::new(),
        gate: gate.clone(),
        boundary: "dispatch",
        commits: AtomicUsize::new(0),
    });
    let host = host(store.clone(), Arc::new(Allow));
    let producer = {
        let h = host.clone();
        tokio::spawn(async move {
            call(
                &h,
                "POST",
                "/threads/custom/runs/wait",
                "owner",
                Some(json!({"route":"/cancel#workflow"})),
            )
            .await
        })
    };
    gate.entered().await;
    let response = call(&host, "POST", "/threads/custom/cancel", "owner", None).await;
    let status = response.status();
    gate.release();
    assert_eq!(producer.await.unwrap().status(), StatusCode::OK);
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        body(response).await["error"]["details"]["code"],
        "unsupported_operation"
    );
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
}
