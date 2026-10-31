use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        let name = headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .ok_or(AuthenticationError)?;
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: name.into(),
        })
    }
}
struct Policy {
    allow: bool,
    contexts: Mutex<Vec<ResourceContext>>,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        self.contexts.lock().unwrap().push(context.clone());
        self.allow
    }
}
fn route() -> ScriptRoute {
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/delete#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: EffectId::new("effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }]],
            max_attempts: 2,
        },
        effects: BTreeMap::from([(EffectId::new("effect").unwrap(), ScriptEffect::Void)]),
    }
}
fn seed<S: Store>(store: &S, id: &str, origin: Option<&str>) -> ThreadId {
    let id = ThreadId::new(id).unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: origin.map(|name| ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: name.into(),
            }),
        })
        .unwrap();
    id
}
fn host(store: Arc<MemoryStore>, policy: Arc<dyn ResourcePolicy>) -> HttpHost<MemoryStore> {
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
    actor: Option<&str>,
    payload: Option<serde_json::Value>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(actor) = actor {
        builder = builder.header("authorization", actor);
    }
    host.router()
        .oneshot(
            builder
                .body(payload.map_or_else(Body::empty, |v| Body::from(v.to_string())))
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn text(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 200_000)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}
fn reference_status(case: &str) -> StatusCode {
    let recording: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/reference/recordings/2026-10-01-lifecycle/lifecycle-observations.json"
    ))
    .unwrap();
    let observation = recording["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == case)
        .unwrap();
    StatusCode::from_u16(
        observation["response"]["status"]
            .as_u64()
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn authorized_idle_and_initially_missing_delete_have_empty_no_store_204() {
    let store = Arc::new(MemoryStore::new());
    let id = seed(&*store, "idle", Some("owner"));
    let policy = Arc::new(Policy {
        allow: true,
        contexts: Mutex::new(vec![]),
    });
    let host = host(store.clone(), policy.clone());
    for (path, case) in [
        ("/threads/idle", "idle.delete"),
        ("/threads/missing", "missing.delete"),
    ] {
        let response = call(&host, "DELETE", path, Some("owner"), None).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.status(), reference_status(case));
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(text(response).await.is_empty());
    }
    assert!(store.thread(&id).unwrap().is_none());
    let contexts = policy.contexts.lock().unwrap();
    let missing = contexts
        .iter()
        .find(|c| {
            c.thread_id
                .as_ref()
                .is_some_and(|id| id.as_str() == "missing")
        })
        .unwrap();
    assert!(
        missing.origin.is_none()
            && missing.stored_route.is_none()
            && missing.requested_route.is_none()
    );
}

#[tokio::test]
async fn deletion_requires_authentication_and_hides_denied_existing_and_missing_resources() {
    let store = Arc::new(MemoryStore::new());
    let id = seed(&*store, "protected", Some("owner"));
    let host = host(
        store.clone(),
        Arc::new(Policy {
            allow: false,
            contexts: Mutex::new(vec![]),
        }),
    );
    assert_eq!(
        call(&host, "DELETE", "/threads/protected", None, None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    for path in ["/threads/protected", "/threads/missing"] {
        let response = call(&host, "DELETE", path, Some("owner"), None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(text(response).await.contains("resource_unavailable"));
    }
    assert!(store.thread(&id).unwrap().is_some());
}

#[tokio::test]
async fn permissive_policy_cannot_delete_foreign_origin_or_legacy_thread() {
    let store = Arc::new(MemoryStore::new());
    let foreign = seed(&*store, "foreign", Some("other"));
    let legacy = seed(&*store, "legacy", None);
    let mut ids = vec![foreign, legacy];
    for (id, application, tenant) in [
        ("foreign-app", "other-app", "tenant"),
        ("foreign-tenant", "app", "other-tenant"),
    ] {
        let id = ThreadId::new(id).unwrap();
        store
            .create_thread_with(ThreadSeed {
                id: id.clone(),
                created_at_ms: 1,
                metadata: Map::new(),
                origin: Some(ThreadOrigin {
                    application: application.into(),
                    tenant: tenant.into(),
                    principal: "owner".into(),
                }),
            })
            .unwrap();
        ids.push(id);
    }
    let host = host(
        store.clone(),
        Arc::new(Policy {
            allow: true,
            contexts: Mutex::new(vec![]),
        }),
    );
    for id in &ids {
        let response = call(
            &host,
            "DELETE",
            &format!("/threads/{id}"),
            Some("owner"),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(store.thread(id).unwrap().is_some());
    }
}

#[tokio::test]
async fn durable_unstarted_work_refuses_deletion_without_faulting_readiness() {
    let store = Arc::new(MemoryStore::new());
    let id = seed(&*store, "accepted", Some("owner"));
    let initial = b4_kernel::new_execution(
        ExecutionId::new("execution").unwrap(),
        TurnId::new("turn").unwrap(),
        1,
        route().definition,
        Map::new(),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    store.admit(&id, initial).unwrap();
    let before = store.thread_view(&id).unwrap();
    let host = host(
        store.clone(),
        Arc::new(Policy {
            allow: true,
            contexts: Mutex::new(vec![]),
        }),
    );
    let response = call(&host, "DELETE", "/threads/accepted", Some("owner"), None).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(text(response).await.contains("run_in_flight"));
    assert_eq!(store.thread_view(&id).unwrap(), before);
    assert_eq!(
        call(&host, "GET", "/readyz", None, None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn completed_run_deletion_removes_native_state_and_execution() {
    let store = Arc::new(MemoryStore::new());
    let host = host(
        store.clone(),
        Arc::new(Policy {
            allow: true,
            contexts: Mutex::new(vec![]),
        }),
    );
    let response = call(
        &host,
        "POST",
        "/threads/completed/runs/wait",
        Some("owner"),
        Some(json!({"route":"/delete#workflow"})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let id = ThreadId::new("completed").unwrap();
    let execution_id = store
        .thread_view(&id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
        .execution_id;
    let response = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = call(&host, "DELETE", "/threads/completed", Some("owner"), None).await;
            if response.status() != StatusCode::CONFLICT {
                break response;
            }
            assert!(text(response).await.contains("run_in_flight"));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.status(), reference_status("completed.delete"));
    assert!(store.execution(&execution_id).unwrap().is_none());
    assert!(store.journal(&execution_id).unwrap().is_empty());
    assert_eq!(
        call(
            &host,
            "GET",
            "/threads/completed/state",
            Some("owner"),
            None
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

struct ChangingPolicy {
    calls: AtomicUsize,
    store: Arc<MemoryStore>,
    change: &'static str,
}

struct AdmissionAbaPolicy {
    store: Arc<MemoryStore>,
    calls: AtomicUsize,
    replace_on: usize,
}
impl ResourcePolicy for AdmissionAbaPolicy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::RunWait
            && self.calls.fetch_add(1, Ordering::AcqRel) == self.replace_on
        {
            let id = context.thread_id.as_ref().unwrap();
            let view = self.store.thread_view(id).unwrap().unwrap();
            self.store
                .delete_thread_fenced(&ThreadDeletionTarget::from_view(&view).unwrap())
                .unwrap();
            self.store
                .create_thread_with(ThreadSeed {
                    id: id.clone(),
                    created_at_ms: 1,
                    metadata: Map::new(),
                    origin: Some(ThreadOrigin {
                        application: "app".into(),
                        tenant: "tenant".into(),
                        principal: "other".into(),
                    }),
                })
                .unwrap();
        }
        true
    }
}

#[tokio::test]
async fn run_authorized_before_delete_and_recreate_cannot_enter_foreign_successor() {
    let store = Arc::new(MemoryStore::new());
    let id = seed(&*store, "aba", Some("owner"));
    let original_generation = store.thread(&id).unwrap().unwrap().generation;
    let host = host(
        store.clone(),
        Arc::new(AdmissionAbaPolicy {
            store: store.clone(),
            calls: AtomicUsize::new(0),
            replace_on: 0,
        }),
    );
    let response = call(
        &host,
        "POST",
        "/threads/aba/runs/wait",
        Some("owner"),
        Some(json!({"route":"/delete#workflow"})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let replacement = store.thread_view(&id).unwrap().unwrap();
    assert_ne!(replacement.thread.generation, original_generation);
    assert_eq!(replacement.thread.origin.unwrap().principal, "other");
    assert!(replacement.execution.is_none());
    assert_eq!(
        call(&host, "GET", "/readyz", None, None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn recreation_during_owned_admission_policy_cannot_bypass_atomic_generation_fence() {
    let store = Arc::new(MemoryStore::new());
    let id = seed(&*store, "aba-commit", Some("owner"));
    let original_generation = store.thread(&id).unwrap().unwrap().generation;
    let host = host(
        store.clone(),
        Arc::new(AdmissionAbaPolicy {
            store: store.clone(),
            calls: AtomicUsize::new(0),
            replace_on: 1,
        }),
    );
    let response = call(
        &host,
        "POST",
        "/threads/aba-commit/runs/wait",
        Some("owner"),
        Some(json!({"route":"/delete#workflow"})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(text(response).await.contains("resource_changed"));
    let replacement = store.thread_view(&id).unwrap().unwrap();
    assert_ne!(replacement.thread.generation, original_generation);
    assert_eq!(replacement.thread.origin.unwrap().principal, "other");
    assert!(replacement.execution.is_none());
    assert_eq!(
        call(&host, "GET", "/readyz", None, None).await.status(),
        StatusCode::OK
    );
}
impl ResourcePolicy for ChangingPolicy {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation != Operation::DeleteThread {
            return true;
        }
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        if self.change == "deny" {
            return call == 0;
        }
        if call == 0 {
            let id = context.thread_id.as_ref().unwrap();
            if self.change == "remove" {
                let view = self.store.thread_view(id).unwrap().unwrap();
                self.store
                    .delete_thread_fenced(&ThreadDeletionTarget::from_view(&view).unwrap())
                    .unwrap();
            } else {
                let initial = b4_kernel::new_execution(
                    ExecutionId::new("successor").unwrap(),
                    TurnId::new("successor-turn").unwrap(),
                    1,
                    route().definition,
                    Map::new(),
                    TransitionFacts { now_ms: 2 },
                )
                .unwrap();
                self.store.admit(id, initial).unwrap();
            }
        }
        true
    }
}

#[tokio::test]
async fn renewed_delete_policy_refusal_changes_no_resource_and_stays_healthy() {
    let store = Arc::new(MemoryStore::new());
    let id = seed(&*store, "renew", Some("owner"));
    let before = store.thread_view(&id).unwrap();
    let policy = Arc::new(ChangingPolicy {
        calls: AtomicUsize::new(0),
        store: store.clone(),
        change: "deny",
    });
    let host = host(store.clone(), policy.clone());
    assert_eq!(
        call(&host, "DELETE", "/threads/renew", Some("owner"), None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(policy.calls.load(Ordering::Acquire), 2);
    assert_eq!(store.thread_view(&id).unwrap(), before);
    assert_eq!(
        call(&host, "GET", "/readyz", None, None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn captured_deletion_never_retargets_a_new_execution_or_disappearance() {
    for change in ["successor", "remove"] {
        let store = Arc::new(MemoryStore::new());
        let id = seed(&*store, "raced", Some("owner"));
        let host = host(
            store.clone(),
            Arc::new(ChangingPolicy {
                calls: AtomicUsize::new(0),
                store: store.clone(),
                change,
            }),
        );
        let response = call(&host, "DELETE", "/threads/raced", Some("owner"), None).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(text(response).await.contains("resource_changed"));
        if change == "successor" {
            assert_eq!(
                store
                    .thread_view(&id)
                    .unwrap()
                    .unwrap()
                    .execution
                    .unwrap()
                    .snapshot
                    .execution_id
                    .as_str(),
                "successor"
            );
        } else {
            assert!(store.thread(&id).unwrap().is_none());
        }
        assert_eq!(
            call(&host, "GET", "/readyz", None, None).await.status(),
            StatusCode::OK
        );
    }
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
            let waited = self
                .changed
                .wait_timeout(state, Duration::from_secs(5))
                .unwrap();
            state = waited.0;
            assert!(
                !waited.1.timed_out(),
                "controlled deletion gate was not released"
            );
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
struct LegacyStore {
    inner: MemoryStore,
}
struct FaultStore {
    inner: MemoryStore,
    gate: Arc<Gate>,
    panic: bool,
}
macro_rules! delegate_store {
    ($name:ty, $($extra:tt)*) => {
        impl Store for $name {
            fn owner_generation(&self) -> u64 { self.inner.owner_generation() }
            fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> { self.inner.create_thread_with(seed) }
            fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> { self.inner.thread(id) }
            fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> { self.inner.thread_view(id) }
            fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> { self.inner.execution(id) }
            fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> { self.inner.admit(id, snapshot) }
            fn admit_fenced(&self, id: &ThreadId, generation: &ThreadGeneration, rev: u64, snapshot: Snapshot) -> Result<StoredExecution, StoreError> { self.inner.admit_fenced(id, generation, rev, snapshot) }
            fn readiness(&self) -> Result<(), StoreError> { self.inner.readiness() }
            fn commit(&self, id: &ExecutionId, fence: ExecutionFence, plan: CommitPlan) -> Result<StoredExecution, StoreError> { self.inner.commit(id, fence, plan) }
            fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> { self.inner.journal(id) }
            $($extra)*
        }
    };
}
delegate_store!(LegacyStore,);
delegate_store!(
    FaultStore,
    fn delete_thread_fenced(&self, _: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.gate.block();
        if self.panic {
            panic!("controlled deletion panic");
        }
        Err(StoreError::Unavailable(
            "sensitive infrastructure diagnostic".into(),
        ))
    }
);

#[tokio::test]
async fn legacy_store_default_delete_is_healthy_unsupported() {
    let store = Arc::new(LegacyStore {
        inner: MemoryStore::new(),
    });
    let id = seed(&*store, "legacy-store", Some("owner"));
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy {
            allow: true,
            contexts: Mutex::new(vec![]),
        }),
        HostConfig::new(vec![route()]),
    )
    .unwrap();
    let response = call(
        &host,
        "DELETE",
        "/threads/legacy-store",
        Some("owner"),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert!(text(response).await.contains("unsupported_operation"));
    assert!(store.thread(&id).unwrap().is_some());
    assert_eq!(
        call(&host, "GET", "/readyz", None, None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn abandoned_deletion_worker_marks_fault_on_failure_and_panic() {
    for panic in [false, true] {
        let gate = Arc::new(Gate::default());
        let store = Arc::new(FaultStore {
            inner: MemoryStore::new(),
            gate: gate.clone(),
            panic,
        });
        let id = seed(&*store, "fault", Some("owner"));
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy {
                allow: true,
                contexts: Mutex::new(vec![]),
            }),
            HostConfig::new(vec![route()]),
        )
        .unwrap();
        let request_host = host.clone();
        let task = tokio::spawn(async move {
            call(
                &request_host,
                "DELETE",
                "/threads/fault",
                Some("owner"),
                None,
            )
            .await
        });
        gate.entered().await;
        task.abort();
        let _ = task.await;
        let response = call(
            &host,
            "POST",
            "/threads/fault/runs/wait",
            Some("owner"),
            Some(json!({"route":"/delete#workflow"})),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        gate.release();
        tokio::time::timeout(Duration::from_secs(3), async {
            while call(&host, "GET", "/readyz", None, None).await.status()
                != StatusCode::SERVICE_UNAVAILABLE
            {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        let response = call(&host, "GET", "/threads/fault", Some("owner"), None).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = text(response).await;
        assert!(body.contains("host_unready") && !body.contains("sensitive"));
        assert!(store.thread(&id).unwrap().is_some());
    }
}
