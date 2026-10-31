#[allow(dead_code)]
mod startup_preparation_support;

use b4_http::*;
use b4_runtime::approval::ApprovalStore;
use b4_runtime::{Store, StoreError};
use b4_sqlite::SqliteStore;
use startup_preparation_support::{server::Server, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparing_host_closes_real_tcp_readiness_and_ordinary_admission() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let owner = store.owner_generation();
    let calls = Arc::new(AtomicUsize::new(0));
    let application = application(calls.clone());
    let application_weak = Arc::downgrade(&application);
    let host = HttpHost::new_with_application_preparing(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config(application.clone()),
    )
    .unwrap();
    let status = host.preparation_status();
    assert_eq!(status.phase, StartupPreparationPhase::NotStarted);
    assert_eq!(status.progress, StartupProgress::default());
    let server = Server::start(host.router()).await;
    let health = server.get("/healthz").await;
    let ready = server.get("/readyz").await;
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));

    let store_weak = Arc::downgrade(&store);
    let shutdown = host.shutdown_handle();
    quiesce(&shutdown).await;
    server.finish().await;
    drop(host);
    drop(application);
    drop(store);
    assert!(application_weak.upgrade().is_none());
    assert!(store_weak.upgrade().is_none());
    let reopened = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(reopened.owner_generation(), owner + 1);
    drop(reopened);
    assert_eq!(calls.load(Ordering::Acquire), 0);

    // Both real socket responses survive normal server/Store retirement.
    assert_eq!(health.0, 200);
    assert_eq!(
        ready.0, 503,
        "Preparing must close readiness before preparation begins"
    );
}

async fn assert_closed(
    server: &Server,
    auth: &CountingAuth,
    policy: &CountingPolicy,
    source: &BoundaryStore<SqliteStore>,
) {
    let before = (
        auth.0.load(Ordering::Acquire),
        policy.calls.load(Ordering::Acquire),
        source.probes.load(Ordering::Acquire),
    );
    for (method, path) in [
        ("POST", "/threads"),
        ("GET", "/threads/known"),
        ("DELETE", "/threads/known"),
        ("GET", "/threads/known/state"),
        ("GET", "/threads/known/pending_interrupts"),
        ("POST", "/threads/known/runs/wait"),
        ("POST", "/threads/known/runs/stream"),
        ("POST", "/threads/known/resume"),
        ("POST", "/threads/known/cancel"),
        ("GET", "/threads/known/runs/stream"),
        ("GET", "/b4/capabilities"),
        ("GET", "/unknown"),
        ("PUT", "/threads/known"),
        ("HEAD", "/healthz"),
        ("POST", "/healthz"),
        ("HEAD", "/readyz"),
        ("OPTIONS", "/unknown"),
        ("DELETE", "/threads/%ZZ"),
    ] {
        for authorization in [None, Some("Bearer startup-fixture")] {
            // Body-declared ordinary paths never receive the promised bytes.
            // A reply still must complete before auth/body/Store work can begin.
            let response = server.request(method, path, authorization, &[], 100).await;
            assert_eq!(response.0, 503, "{method} {path}");
            if method != "HEAD" {
                assert!(response.1.contains("host_draining"), "{method} {path}");
            }
        }
    }
    assert_eq!(server.get("/healthz").await.0, 200);
    assert_eq!(server.get("/readyz").await.0, 503);
    assert_eq!(
        (
            auth.0.load(Ordering::Acquire),
            policy.calls.load(Ordering::Acquire),
            source.probes.load(Ordering::Acquire)
        ),
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_preparing_and_incomplete_reject_all_ordinary_paths_before_auth_body_or_store() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "tcp-barrier", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let source = Arc::new(BoundaryStore::new(inner.clone()));
    let auth = Arc::new(CountingAuth(AtomicUsize::new(0)));
    let policy = CountingPolicy::new();
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        source.clone(),
        auth.clone(),
        policy.clone(),
        config_for(application.clone()),
    )
    .unwrap();
    let server = Server::start(host.router()).await;
    assert_closed(&server, auth.as_ref(), policy.as_ref(), source.as_ref()).await;
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let resolver = Arc::new(Resolver {
        calls: AtomicUsize::new(0),
        gate: Some(gate.clone()),
        result: Err(StartupResolutionError::Unauthorized),
        drop_gate: None,
        captured_store: None,
        panic_payload: false,
    });
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    gate.entered().await;
    assert_eq!(
        inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        before
    );
    assert_closed(&server, auth.as_ref(), policy.as_ref(), source.as_ref()).await;
    gate.release();
    assert!(matches!(
        finished(&handle).await,
        StartupPreparationOutcome::Incomplete {
            reason: StartupIncompleteReason::ResolutionRefused,
            ..
        }
    ));
    assert_closed(&server, auth.as_ref(), policy.as_ref(), source.as_ref()).await;
    assert_eq!(
        inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    quiesce(&host.shutdown_handle()).await;
    server.finish().await;
    drop(host);
    drop(source);
    drop(auth);
    drop(policy);
    drop(resolver);
    retire_source(inner, application, &directory);
    gate.positive();
    assert_eq!(handle.status().phase, StartupPreparationPhase::Incomplete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_completed_preparation_opens_real_authenticated_operations_and_resource_free_handles() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let source = Arc::new(BoundaryStore::new(inner.clone()));
    let auth = Arc::new(CountingAuth(AtomicUsize::new(0)));
    let policy = CountingPolicy::new();
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        source.clone(),
        auth.clone(),
        policy.clone(),
        config_for(application.clone()),
    )
    .unwrap();
    let server = Server::start(host.router()).await;
    assert_eq!(server.get("/healthz").await.0, 200);
    assert_eq!(server.get("/readyz").await.0, 503);
    let handle = host
        .begin_preparation(
            Resolver::new(None),
            StartupPreparationLimits {
                max_pages: 1,
                max_recoveries: 1,
            },
        )
        .unwrap();
    assert!(matches!(
        finished(&handle).await,
        StartupPreparationOutcome::Complete {
            progress: StartupProgress {
                pages: 1,
                scanned_rows: 0,
                candidates: 0,
                admitted: 0,
                ..
            }
        }
    ));
    let ready = server.get("/readyz").await;
    assert_eq!(ready.0, 200);
    assert_eq!(auth.0.load(Ordering::Acquire), 0);
    let capabilities = server
        .request(
            "GET",
            "/b4/capabilities",
            Some("Bearer startup-fixture"),
            &[],
            0,
        )
        .await;
    assert_eq!(capabilities.0, 200);
    let created = server
        .request("POST", "/threads", Some("Bearer startup-fixture"), b"{}", 2)
        .await;
    assert_eq!(created.0, 200);
    assert!(auth.0.load(Ordering::Acquire) >= 2);
    assert!(policy.calls.load(Ordering::Acquire) > 0);
    assert!(source.probes.load(Ordering::Acquire) > 0);
    let body = created.1.split_once("\r\n\r\n").unwrap().1;
    let projection: serde_json::Value = serde_json::from_str(body).unwrap();
    let id = b4_contract::ThreadId::new(projection["thread_id"].as_str().unwrap()).unwrap();
    assert_eq!(inner.thread(&id).unwrap().unwrap().origin, Some(origin()));
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    let serving = host.serving_host().unwrap();
    let shutdown = host.shutdown_handle();
    quiesce(&shutdown).await;
    server.finish().await;
    drop(serving);
    drop(host);
    drop(source);
    drop(auth);
    drop(policy);
    retire_source(inner, application, &directory);
    assert_eq!(handle.status().phase, StartupPreparationPhase::Complete);
    assert_eq!(shutdown.status().active_owners, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_startup_shutdown_race_never_reopens_readiness_or_admits_held_target() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    seed_named(inner.clone(), "tcp-race", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let rows = directory.rows();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let resolver = Resolver::new(Some(gate.clone()));
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let server = Server::start(host.router()).await;
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    gate.entered().await;
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    assert_eq!(server.get("/healthz").await.0, 200);
    assert_eq!(server.get("/readyz").await.0, 503);
    assert_locked(&directory);
    gate.release();
    assert!(matches!(
        finished(&handle).await,
        StartupPreparationOutcome::Incomplete {
            reason: StartupIncompleteReason::Shutdown,
            ..
        }
    ));
    assert_eq!(server.get("/readyz").await.0, 503);
    assert_eq!(directory.rows(), rows);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    quiesce(&shutdown).await;
    server.finish().await;
    drop(host);
    drop(resolver);
    retire_source(inner, application, &directory);
    gate.positive();
    assert_eq!(shutdown.status().phase, ShutdownPhase::Draining);
}
