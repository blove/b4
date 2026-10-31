use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
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
    fn authenticate(&self, h: &HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: h
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
            route: RouteKey::new("/attach#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: EffectId::new("effect").unwrap(),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            }]],
            max_attempts: 2,
        },
        effects: BTreeMap::from([(EffectId::new("effect").unwrap(), ScriptEffect::Input)]),
    }
}
fn host(store: Arc<MemoryStore>) -> HttpHost<MemoryStore> {
    HttpHost::new(
        store,
        Arc::new(Auth),
        Arc::new(Allow),
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
async fn text(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 3_000_000)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}
fn events(source: &str) -> Vec<(String, Value)> {
    source
        .split("\n\n")
        .filter_map(|block| {
            let event = block.lines().find_map(|l| l.strip_prefix("event: "))?;
            let data = block.lines().find_map(|l| l.strip_prefix("data: "))?;
            Some((event.into(), serde_json::from_str(data).unwrap()))
        })
        .collect()
}
fn seed(store: &MemoryStore, id: &str, owner: Option<&str>) {
    store
        .create_thread_with(ThreadSeed {
            id: ThreadId::new(id).unwrap(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: owner.map(|principal| ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: principal.into(),
            }),
        })
        .unwrap();
}
#[tokio::test]
async fn inactive_completed_attachment_has_native_state_retry_and_null_terminal() {
    let store = Arc::new(MemoryStore::new());
    let host = host(store.clone());
    assert_eq!(
        call(
            &host,
            "POST",
            "/threads/completed/runs/wait",
            "owner",
            Some(json!({"route":"/attach#workflow","input":"saved"}))
        )
        .await
        .status(),
        StatusCode::OK
    );
    let response = call(
        &host,
        "GET",
        "/threads/completed/runs/stream",
        "owner",
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["cache-control"],
        "no-store, no-transform"
    );
    let wire = text(response).await;
    let frames = events(&wire);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].0, "state");
    assert_eq!(frames[0].1["live"], false);
    assert_eq!(frames[0].1["values"]["input"], "saved");
    assert_eq!(frames[0].1["turn"], Value::Null);
    assert_eq!(frames[1], ("done".into(), json!({"output":null})));
    assert!(wire.contains("retry: 2000"));
}
#[tokio::test]
async fn idle_missing_and_foreign_attachment_fail_without_disclosing_a_turn() {
    let store = Arc::new(MemoryStore::new());
    seed(&store, "idle", Some("owner"));
    seed(&store, "foreign", Some("other"));
    seed(&store, "legacy", None);
    let host = host(store);
    let response = call(&host, "GET", "/threads/idle/runs/stream", "owner", None).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(text(response).await.contains("thread_route_unknown"));
    for name in ["missing", "foreign", "legacy"] {
        assert_eq!(
            call(
                &host,
                "GET",
                &format!("/threads/{name}/runs/stream"),
                "owner",
                None
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(&host, "GET", "/readyz", "owner", None).await.status(),
        StatusCode::OK
    );
}

fn delayed_config() -> HostConfig {
    let mut script = route();
    script.effects.insert(
        EffectId::new("effect").unwrap(),
        ScriptEffect::Delayed {
            duration_ms: 500,
            result: TaskResult {
                output: json!("finished"),
                writes: vec![],
            },
        },
    );
    let mut config = HostConfig::new(vec![script]);
    config.max_attached_viewers = 1;
    config
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
            assert!(!timeout.timed_out(), "attachment policy gate not released");
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.state.lock().unwrap().0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
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
struct HeldPolicy {
    calls: AtomicUsize,
    hold_on: usize,
    gate: Arc<Gate>,
}
impl ResourcePolicy for HeldPolicy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::Attach
            && self.calls.fetch_add(1, Ordering::SeqCst) == self.hold_on
        {
            self.gate.block();
        }
        true
    }
}

#[tokio::test]
async fn abandoned_initial_and_renewed_policy_wait_release_the_reserved_viewer_immediately() {
    for hold_on in [0, 1] {
        let gate = Arc::new(Gate::default());
        let store = Arc::new(MemoryStore::new());
        let host = HttpHost::new(
            store,
            Arc::new(Auth),
            Arc::new(HeldPolicy {
                calls: AtomicUsize::new(0),
                hold_on,
                gate: gate.clone(),
            }),
            delayed_config(),
        )
        .unwrap();
        let producer = call(
            &host,
            "POST",
            "/threads/abandon/runs/stream",
            "owner",
            Some(json!({"route":"/attach#workflow"})),
        )
        .await;
        let abandoned = {
            let host = host.clone();
            tokio::spawn(async move {
                call(&host, "GET", "/threads/abandon/runs/stream", "owner", None).await
            })
        };
        gate.entered().await;
        abandoned.abort();
        assert!(abandoned.await.unwrap_err().is_cancelled());
        let replacement = call(&host, "GET", "/threads/abandon/runs/stream", "owner", None).await;
        let status = replacement.status();
        drop(replacement);
        let another = call(&host, "GET", "/threads/abandon/runs/stream", "owner", None).await;
        let another_status = another.status();
        drop(another);
        gate.release();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(another_status, StatusCode::OK);
        assert!(text(producer).await.contains("finished"));
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::OK
        );
    }
}

struct RenewDenial {
    calls: AtomicUsize,
    deny: AtomicBool,
}
impl ResourcePolicy for RenewDenial {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        context.operation != Operation::Attach
            || self.calls.fetch_add(1, Ordering::SeqCst).is_multiple_of(2)
            || !self.deny.load(Ordering::SeqCst)
    }
}
#[tokio::test]
async fn viewer_capacity_and_snapshot_limits_stay_private_until_renewed_authority() {
    for snapshot_limit in [false, true] {
        let policy = Arc::new(RenewDenial {
            calls: AtomicUsize::new(0),
            deny: AtomicBool::new(false),
        });
        let store = Arc::new(MemoryStore::new());
        let mut config = delayed_config();
        if snapshot_limit {
            config.attachment_snapshot_bytes = 1;
        }
        let host = HttpHost::new(store, Arc::new(Auth), policy.clone(), config).unwrap();
        let producer = call(
            &host,
            "POST",
            "/threads/private/runs/stream",
            "owner",
            Some(json!({"route":"/attach#workflow"})),
        )
        .await;
        let original = if snapshot_limit {
            None
        } else {
            Some(call(&host, "GET", "/threads/private/runs/stream", "owner", None).await)
        };
        policy.deny.store(true, Ordering::SeqCst);
        let denied = call(&host, "GET", "/threads/private/runs/stream", "owner", None).await;
        assert_eq!(denied.status(), StatusCode::NOT_FOUND);
        let wire = text(denied).await;
        assert!(!wire.contains("capacity") && !wire.contains("snapshot_limit"));
        drop(original);
        policy.deny.store(false, Ordering::SeqCst);
        let allowed = call(&host, "GET", "/threads/private/runs/stream", "owner", None).await;
        if snapshot_limit {
            assert_eq!(allowed.status(), StatusCode::CONFLICT);
            assert!(text(allowed).await.contains("attachment_snapshot_limit"));
        } else {
            assert_eq!(allowed.status(), StatusCode::OK);
            drop(allowed);
        }
        assert!(text(producer).await.contains("finished"));
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn live_attachment_has_immutable_native_cut_and_nonlive_reconnect_has_null_done() {
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        delayed_config(),
    )
    .unwrap();
    let producer = call(
        &host,
        "POST",
        "/threads/live/runs/stream",
        "owner",
        Some(json!({"route":"/attach#workflow","input":"original"})),
    )
    .await;
    let attached = call(&host, "GET", "/threads/live/runs/stream", "owner", None).await;
    assert_eq!(attached.status(), StatusCode::OK);
    let frames = events(&text(attached).await);
    assert_eq!(frames[0].0, "state");
    assert_eq!(frames[0].1["live"], true);
    assert_eq!(frames[0].1["input"], "original");
    assert_eq!(frames[0].1["values"]["input"], "original");
    assert_eq!(frames[0].1["turn"], json!([]));
    assert!(frames[0].1["anchor"].is_string());
    assert_eq!(
        frames.last().unwrap(),
        &("done".into(), json!({"output":"finished"}))
    );
    assert!(text(producer).await.contains("finished"));
    let inactive = call(&host, "GET", "/threads/live/runs/stream", "owner", None).await;
    assert_eq!(
        events(&text(inactive).await).last().unwrap(),
        &("done".into(), json!({"output":null}))
    );
}

#[tokio::test]
async fn unregistered_definition_and_orphan_execution_are_healthy_protected_refusals() {
    for mismatch in [false, true] {
        let store = Arc::new(MemoryStore::new());
        seed(&store, "orphan", Some("owner"));
        let mut definition = route().definition;
        if mismatch {
            definition.max_attempts += 1;
        }
        store
            .admit(
                &ThreadId::new("orphan").unwrap(),
                b4_kernel::new_execution(
                    ExecutionId::new("orphan-exec").unwrap(),
                    TurnId::new("orphan-turn").unwrap(),
                    1,
                    definition,
                    Map::new(),
                    TransitionFacts { now_ms: 1 },
                )
                .unwrap(),
            )
            .unwrap();
        let before = store
            .thread_view(&ThreadId::new("orphan").unwrap())
            .unwrap();
        let host = host(store.clone());
        let response = call(&host, "GET", "/threads/orphan/runs/stream", "owner", None).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(text(response).await.contains(if mismatch {
            "thread_route_unknown"
        } else {
            "attachment_unavailable"
        }));
        assert_eq!(
            store
                .thread_view(&ThreadId::new("orphan").unwrap())
                .unwrap(),
            before
        );
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::OK
        );
    }
}

#[test]
fn every_new_publication_limit_has_a_finite_configured_range() {
    for field in [
        "viewers",
        "digest_bytes",
        "digest_frames",
        "queue_bytes",
        "state_bytes",
    ] {
        for zero in [true, false] {
            let mut config = HostConfig::new(vec![route()]);
            let value = if zero { 0 } else { usize::MAX };
            match field {
                "viewers" => config.max_attached_viewers = value,
                "digest_bytes" => config.turn_buffer_bytes = value,
                "digest_frames" => config.turn_buffer_frames = value,
                "queue_bytes" => config.viewer_queue_bytes = value,
                _ => config.attachment_snapshot_bytes = value,
            };
            assert!(HttpHost::new(
                Arc::new(MemoryStore::new()),
                Arc::new(Auth),
                Arc::new(Allow),
                config
            )
            .is_err());
        }
    }
}

#[tokio::test]
async fn original_attachment_identity_survives_neither_initial_nor_renewed_policy_successor_races()
{
    for hold_on in [0, 1] {
        let store = Arc::new(MemoryStore::new());
        let gate = Arc::new(Gate::default());
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth),
            Arc::new(HeldPolicy {
                calls: AtomicUsize::new(0),
                hold_on,
                gate: gate.clone(),
            }),
            delayed_config(),
        )
        .unwrap();
        let first = call(
            &host,
            "POST",
            "/threads/race/runs/stream",
            "owner",
            Some(json!({"route":"/attach#workflow"})),
        )
        .await;
        let attached = {
            let host = host.clone();
            tokio::spawn(async move {
                call(&host, "GET", "/threads/race/runs/stream", "owner", None).await
            })
        };
        gate.entered().await;
        assert!(text(first).await.contains("finished"));
        let successor = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let response = call(
                    &host,
                    "POST",
                    "/threads/race/runs/stream",
                    "owner",
                    Some(json!({"route":"/attach#workflow"})),
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
        gate.release();
        let response = attached.await.unwrap();
        let status = response.status();
        let wire = text(response).await;
        assert!(text(successor).await.contains("finished"));
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "held policy {hold_on} must not expose a retired cut"
        );
        assert!(wire.contains("resource_changed"));
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::OK
        );
    }
}

struct FaultedPublicationStore {
    inner: MemoryStore,
    dispatch: Arc<Gate>,
    capture: Arc<Gate>,
    pause_capture: AtomicBool,
}
impl Store for FaultedPublicationStore {
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
        let view = self.inner.thread_view(id);
        if self.pause_capture.swap(false, Ordering::SeqCst) {
            self.capture.block();
        }
        view
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
        let dispatch = plan
            .snapshot
            .tasks
            .iter()
            .any(|task| task.status == TaskStatus::InFlight);
        let result = self.inner.commit(id, fence, plan);
        if dispatch && result.is_ok() {
            self.dispatch.block();
        }
        result
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn cancel_run_fenced(
        &self,
        _: &RunCancellationTarget,
        _: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        Err(StoreError::Unavailable("cancel persistence fault".into()))
    }
}
struct ReleaseGate(Arc<Gate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn preterminal_fault_during_initial_capture_never_becomes_an_inactive_success() {
    for actor in ["owner", "other"] {
        let store = Arc::new(FaultedPublicationStore {
            inner: MemoryStore::new(),
            dispatch: Arc::new(Gate::default()),
            capture: Arc::new(Gate::default()),
            pause_capture: AtomicBool::new(false),
        });
        let _dispatch_cleanup = ReleaseGate(store.dispatch.clone());
        let _capture_cleanup = ReleaseGate(store.capture.clone());
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Allow),
            HostConfig::new(vec![route()]),
        )
        .unwrap();
        let producer = call(
            &host,
            "POST",
            "/threads/fault/runs/stream",
            "owner",
            Some(json!({"route":"/attach#workflow"})),
        )
        .await;
        store.dispatch.entered().await;
        store.pause_capture.store(true, Ordering::SeqCst);
        let attachment = {
            let host = host.clone();
            tokio::spawn(async move {
                call(&host, "GET", "/threads/fault/runs/stream", actor, None).await
            })
        };
        store.capture.entered().await;
        assert_eq!(
            call(&host, "POST", "/threads/fault/cancel", "owner", None)
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        store.capture.release();
        let response = attachment.await.unwrap();
        let status = response.status();
        let wire = text(response).await;
        store.dispatch.release();
        assert!(!text(producer).await.contains("event: done"));
        assert_eq!(
            status,
            if actor == "owner" {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::NOT_FOUND
            }
        );
        assert!(!wire.contains("event: done"));
        assert_eq!(
            call(&host, "GET", "/readyz", "owner", None).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let execution = store
            .inner
            .thread_view(&ThreadId::new("fault").unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap();
        assert_eq!(execution.snapshot.status, ExecutionStatus::Running);
        assert_eq!(execution.snapshot.tasks[0].status, TaskStatus::InFlight);
        assert!(execution.snapshot.tasks[0].result.is_none());
    }
}
