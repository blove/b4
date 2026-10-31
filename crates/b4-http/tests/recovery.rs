use axum::{body::to_bytes, http::StatusCode};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, _: &axum::http::HeaderMap) -> Result<Principal, AuthenticationError> {
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
    allow: AtomicBool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        self.allow.load(Ordering::Acquire)
    }
}
fn route(required: bool) -> ScriptRoute {
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/recover#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            max_attempts: 2,
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: EffectId::new("effect").unwrap(),
                requires_approval: required,
                recovery: RecoveryPolicy::Pure,
            }]],
        },
        effects: BTreeMap::from([(
            EffectId::new("effect").unwrap(),
            ScriptEffect::Result(TaskResult {
                output: json!("finished"),
                writes: vec![],
            }),
        )]),
    }
}
fn approval_config(required: bool, durable: bool) -> ApprovalHostConfig {
    let mut config = ApprovalHostConfig::new(
        "policy-v1".into(),
        if required {
            BTreeMap::from([(
                RouteKey::new("/recover#workflow").unwrap(),
                BTreeMap::from([(TaskId::new("task").unwrap(), json!({"question":"approve?"}))]),
            )])
        } else {
            BTreeMap::new()
        },
    );
    if !durable {
        config.service_config = ApprovalServiceConfig::VolatileFixture;
    }
    config
}
fn seed<S: Store>(store: &S, scripted: &ScriptRoute) -> ThreadView {
    let id = ThreadId::new("thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("execution").unwrap(),
        TurnId::new("initial").unwrap(),
        store.owner_generation(),
        scripted.definition.clone(),
        Map::new(),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    Runtime::new(store).admit(&id, initial).unwrap();
    store.thread_view(&id).unwrap().unwrap()
}
async fn body(response: axum::response::Response) -> (StatusCode, String) {
    let status = response.status();
    let raw = to_bytes(response.into_body(), 200_000).await.unwrap();
    (status, String::from_utf8(raw.to_vec()).unwrap())
}
async fn ready<S: Store + 'static>(host: &HttpHost<S>) -> StatusCode {
    use tower::ServiceExt;
    host.router()
        .oneshot(
            axum::http::Request::builder()
                .uri("/readyz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}
struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("b4-http-recovery-{}", uuid::Uuid::new_v4())))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn trusted_recovery_target_captures_complete_coherent_identity_and_fence() {
    let store = MemoryStore::new();
    let id = ThreadId::new("empty").unwrap();
    store.create_thread(id.clone()).unwrap();
    assert!(RecoveryTarget::from_view(&store.thread_view(&id).unwrap().unwrap()).is_none());
    let view = seed(&store, &route(false));
    let target = RecoveryTarget::from_view(&view).unwrap();
    assert_eq!(target.thread_id, view.thread.id);
    assert_eq!(target.thread_generation, view.thread.generation);
    assert_eq!(
        target.execution_id,
        view.execution.as_ref().unwrap().snapshot.execution_id
    );
    assert_eq!(
        target.fence.execution,
        ExecutionFence::from(&view.execution.unwrap().snapshot)
    );
    assert_eq!(
        target.fence.resource_revision,
        view.thread.resource_revision
    );
}

#[tokio::test]
async fn ordinary_host_trusted_recovery_is_unsupported_and_has_no_public_route() {
    use tower::ServiceExt;
    let store = Arc::new(MemoryStore::new());
    let scripted = route(false);
    let before = seed(&*store, &scripted);
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        HostConfig::new(vec![scripted]),
    )
    .unwrap();
    let rejected = body(
        host.recover_execution(principal(), RecoveryTarget::from_view(&before).unwrap())
            .await,
    )
    .await;
    assert_eq!(rejected.0, StatusCode::NOT_IMPLEMENTED);
    assert!(rejected.1.contains("recovery_unsupported"));
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap().unwrap(),
        before
    );
    let response = host
        .router()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/threads/thread/recover")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(ready(&host).await, StatusCode::OK);
}

#[tokio::test]
async fn trusted_recovery_renews_origin_and_policy_before_protected_diagnostics() {
    let directory = Directory::new();
    let scripted = route(false);
    let target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        target = RecoveryTarget::from_view(&seed(&store, &scripted)).unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let before = store.thread_view(&target.thread_id).unwrap().unwrap();
    let policy = Arc::new(Policy {
        allow: AtomicBool::new(false),
    });
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        HostConfig::new(vec![scripted]),
        approval_config(false, true),
    )
    .unwrap();
    let mut malformed = target.clone();
    malformed.execution_id = ExecutionId::new("secret-wrong-execution").unwrap();
    malformed.fence.resource_revision += 1;
    for actor in [
        principal(),
        Principal {
            principal: "foreign".into(),
            ..principal()
        },
    ] {
        let rejected = body(host.recover_execution(actor, malformed.clone()).await).await;
        assert_eq!(rejected.0, StatusCode::NOT_FOUND);
        assert!(rejected.1.contains("resource_unavailable"));
        assert!(!rejected.1.contains("secret-wrong-execution"));
        assert_eq!(
            store.thread_view(&target.thread_id).unwrap().unwrap(),
            before
        );
    }
    policy.allow.store(true, Ordering::Release);
    let rejected = body(
        host.recover_execution(
            Principal {
                principal: "foreign".into(),
                ..principal()
            },
            target.clone(),
        )
        .await,
    )
    .await;
    assert_eq!(rejected.0, StatusCode::NOT_FOUND);
    assert_eq!(
        store.thread_view(&target.thread_id).unwrap().unwrap(),
        before
    );
    assert_eq!(ready(&host).await, StatusCode::OK);
}

#[tokio::test]
async fn trusted_recovery_rejects_current_owner_and_every_stale_target_without_mutation() {
    let store = Arc::new(MemoryStore::new());
    let scripted = route(false);
    let before = seed(&*store, &scripted);
    let target = RecoveryTarget::from_view(&before).unwrap();
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        HostConfig::new(vec![scripted]),
        approval_config(false, false),
    )
    .unwrap();
    let mut targets = vec![target.clone()];
    let mut stale = target.clone();
    stale.fence.resource_revision += 1;
    targets.push(stale);
    let mut stale = target.clone();
    stale.fence.execution.revision += 1;
    targets.push(stale);
    let mut stale = target.clone();
    stale.fence.execution.owner_generation += 1;
    targets.push(stale);
    let mut stale = target.clone();
    stale.thread_generation = ThreadGeneration::new("0".repeat(64)).unwrap();
    targets.push(stale);
    let mut stale = target;
    stale.execution_id = ExecutionId::new("wrong").unwrap();
    targets.push(stale);
    for target in targets {
        let rejected = body(host.recover_execution(principal(), target).await).await;
        assert_eq!(rejected.0, StatusCode::CONFLICT);
        assert!(!rejected.1.contains("b4ag_"));
        assert_eq!(
            store.thread_view(&before.thread.id).unwrap().unwrap(),
            before
        );
    }
    assert_eq!(ready(&host).await, StatusCode::OK);
}

#[tokio::test]
async fn trusted_recovery_rejects_changed_full_definition_before_adoption() {
    let directory = Directory::new();
    let scripted = route(false);
    let target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        target = RecoveryTarget::from_view(&seed(&store, &scripted)).unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let before = store.thread_view(&target.thread_id).unwrap().unwrap();
    let mut different = scripted;
    different.definition.max_attempts += 1;
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        HostConfig::new(vec![different]),
        approval_config(false, true),
    )
    .unwrap();
    let rejected = body(host.recover_execution(principal(), target).await).await;
    assert_eq!(rejected.0, StatusCode::CONFLICT);
    assert!(rejected.1.contains("recovery_definition_unavailable"));
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap().unwrap(),
        before
    );
    assert_eq!(ready(&host).await, StatusCode::OK);
}

struct AdmissionGate {
    checks: AtomicUsize,
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
}
impl ResourcePolicy for AdmissionGate {
    fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::Recover
            && self.checks.fetch_add(1, Ordering::AcqRel) == 1
        {
            self.entered.store(true, Ordering::Release);
            let guard = self.released.lock().unwrap();
            let (guard, _) = self
                .wake
                .wait_timeout_while(guard, Duration::from_secs(5), |released| !*released)
                .unwrap();
            return *guard;
        }
        true
    }
}
#[tokio::test]
async fn abandoned_trusted_recovery_future_keeps_thread_claim_before_commit() {
    let directory = Directory::new();
    let scripted = route(false);
    let target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        target = RecoveryTarget::from_view(&seed(&store, &scripted)).unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let before = store.thread_view(&target.thread_id).unwrap().unwrap();
    let policy = Arc::new(AdmissionGate {
        checks: AtomicUsize::new(0),
        entered: AtomicBool::new(false),
        released: Mutex::new(false),
        wake: Condvar::new(),
    });
    let mut config = HostConfig::new(vec![scripted]);
    config.max_active_runs = 2;
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        config,
        approval_config(false, true),
    )
    .unwrap();
    let pending_host = host.clone();
    let pending_target = target.clone();
    let caller = tokio::spawn(async move {
        pending_host
            .recover_execution(principal(), pending_target)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !policy.entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    let rejected = body(host.recover_execution(principal(), target).await).await;
    assert_eq!(rejected.0, StatusCode::CONFLICT);
    assert!(rejected.1.contains("thread_busy"));
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap().unwrap(),
        before
    );
    *policy.released.lock().unwrap() = true;
    policy.wake.notify_all();
    let complete = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let current = store
                .thread_view(&before.thread.id)
                .unwrap()
                .unwrap()
                .execution
                .unwrap();
            if current.snapshot.status == ExecutionStatus::Completed {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(complete.snapshot.attempts_used, 1);
    assert_eq!(
        complete.snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("finished")
    );
    assert_eq!(ready(&host).await, StatusCode::OK);
}

#[tokio::test]
async fn failed_recovery_commit_rolls_back_and_marks_host_unready_with_static_error() {
    let directory = Directory::new();
    let scripted = route(false);
    let target;
    {
        let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
        target = RecoveryTarget::from_view(&seed(&store, &scripted)).unwrap();
    }
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let before = store.thread_view(&target.thread_id).unwrap().unwrap();
    let journal = store.journal(&target.execution_id).unwrap();
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy {
            allow: AtomicBool::new(true),
        }),
        HostConfig::new(vec![scripted]),
        approval_config(false, true),
    )
    .unwrap();
    let connection = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_recovery_journal BEFORE INSERT ON journal BEGIN SELECT RAISE(ABORT, 'private-recovery-storage-diagnostic'); END;").unwrap();
    let rejected = body(host.recover_execution(principal(), target.clone()).await).await;
    assert_eq!(rejected.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(rejected.1.contains("host_unready"));
    assert!(!rejected.1.contains("private-recovery-storage-diagnostic"));
    assert!(!rejected.1.contains("event: done"));
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap().unwrap(),
        before
    );
    assert_eq!(store.journal(&target.execution_id).unwrap(), journal);
    assert_eq!(ready(&host).await, StatusCode::SERVICE_UNAVAILABLE);
}
