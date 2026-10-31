#[allow(dead_code)]
mod native_history_support;
use b4_contract::*;
use b4_http::*;
use b4_runtime::approval::ApprovalStore;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use native_history_support::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_tcp_new_work_selects_current_v2_after_real_v1_startup() {
    let fixture = Prior::new("tcp-startup", definition("v1"), true);
    let old = calls();
    let current = calls();
    let validations = Arc::new(AtomicUsize::new(0));
    let app = assembly(
        bound(
            definition("v2"),
            "v2",
            current.clone(),
            None,
            false,
            None,
            Some(Arc::new(ExactCurrentInput {
                calls: validations.clone(),
            })),
        ),
        vec![registration("v1", "v1", old.clone())],
    );
    let weak_store = Arc::downgrade(&fixture.store);
    let weak_app = Arc::downgrade(&app);
    let preparing = HttpHost::new_with_application_preparing(
        fixture.store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        configuration(app.clone(), 300_000),
    )
    .unwrap();
    let server = Server::start(preparing.router()).await;
    let handle = preparing
        .begin_preparation(
            FixedResolver::trusted(),
            StartupPreparationLimits::default(),
        )
        .unwrap();
    let progress = preparation_complete(&handle).await;
    let v1 = completed(
        fixture.store.as_ref(),
        &fixture.approval.execution.snapshot.execution_id,
    )
    .await;
    let refused = server
        .request(
            "POST",
            "/threads/tcp-unauthenticated/runs/wait",
            Some(serde_json::json!({"route":"/native-history#workflow","input":{"current":true}})),
            false,
        )
        .await;
    let admitted = server
        .request(
            "POST",
            "/threads/tcp-current/runs/wait",
            Some(serde_json::json!({"route":"/native-history#workflow","input":{"current":true}})),
            true,
        )
        .await;
    let invalid = server
        .request(
            "POST",
            "/threads/tcp-invalid/runs/wait",
            Some(serde_json::json!({"route":"/native-history#workflow","input":{"current":false}})),
            true,
        )
        .await;
    let current_view = fixture
        .store
        .thread_view(&ThreadId::new("tcp-current").unwrap())
        .unwrap()
        .unwrap();
    let no_auth = fixture
        .store
        .thread(&ThreadId::new("tcp-unauthenticated").unwrap())
        .unwrap();
    let no_invalid = fixture
        .store
        .thread(&ThreadId::new("tcp-invalid").unwrap())
        .unwrap();
    let final_history = fixture.current();
    let final_authority = fixture.authority();
    let shutdown = preparing.shutdown_handle();
    quiesce(&shutdown).await;
    server.finish().await;
    drop(preparing);
    drop(app);
    drop(fixture.store);
    weak_absent(&weak_store, &weak_app);
    let next = SqliteStore::open(fixture.directory.path()).unwrap();
    assert_eq!(next.owner_generation(), fixture.owner + 1);
    next.readiness().unwrap();
    assert_eq!(
        next.thread_view(&fixture.view.thread.id).unwrap().unwrap(),
        final_history
    );
    assert_eq!(
        next.approval_view(&fixture.approval.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        final_authority
    );
    drop(next);
    assert_eq!(progress.admitted, 1);
    assert_eq!(v1.snapshot.definition, definition("v1"));
    assert_eq!(v1.snapshot.tasks[0].result, Some(result("v1")));
    assert_eq!(v1.snapshot.attempts_used, 2);
    assert_eq!(refused.0, 401);
    assert!(refused.1.contains("authentication_required"));
    assert!(no_auth.is_none());
    assert_eq!(admitted.0, 200);
    assert!(admitted.1.contains("\"selected_version\":\"v2\""));
    assert_eq!(invalid.0, 400);
    assert!(invalid.1.contains("invalid_application_input"));
    assert!(no_invalid.is_none());
    assert_eq!(
        current_view.execution.as_ref().unwrap().snapshot.definition,
        definition("v2")
    );
    assert_eq!(
        current_view.execution.as_ref().unwrap().snapshot.tasks[0].result,
        Some(result("v2"))
    );
    assert_calls(&old, "v1", fixture.owner, &definition("v1"));
    assert_calls(&current, "v2", fixture.owner, &definition("v2"));
    assert_eq!(validations.load(Ordering::SeqCst), 2);
    assert_eq!(shutdown.status().active_owners, 0);
    assert!(!shutdown.status().faulted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_native_wrong_resume_route_preserves_current_requested_provenance() {
    for missing in [false, true] {
        let fixture = Prior::new(
            if missing {
                "tcp-route-missing"
            } else {
                "tcp-route-current"
            },
            definition("v1"),
            false,
        );
        let old = calls();
        let current = calls();
        let mut current_definition = definition("v2");
        current_definition.route = RouteKey::new("/other-current#workflow").unwrap();
        let app = assembly(
            bound(
                current_definition.clone(),
                "v2",
                current.clone(),
                None,
                false,
                None,
                None,
            ),
            vec![registration("v1", "v1", old.clone())],
        );
        let policy = policy_probe(Operation::Resume, None, None);
        let contexts = policy.contexts.clone();
        // Deliberately deny the wrong-route operation after ordinary provenance
        // capture; this keeps the static private refusal independent of later
        // owner/state/request-route precedence.
        policy.allowed.store(false, Ordering::SeqCst);
        let host = host(fixture.store.clone(), app.clone(), policy.clone());
        let server = Server::start(host.router()).await;
        let route = if missing {
            "/absent-request#workflow"
        } else {
            "/other-current#workflow"
        };
        let response = server
            .request(
                "POST",
                &format!("/threads/{}/resume", fixture.view.thread.id),
                Some(serde_json::json!({"route":route,"resume":[{"status":"cancelled","interruptId":"unsubmitted-history-probe"}]})),
                true,
            )
            .await;
        let after = fixture.current();
        let authority = fixture.authority();
        let rows = fixture.directory.rows();
        let shutdown = host.shutdown_handle();
        quiesce(&shutdown).await;
        server.finish().await;
        drop(policy);
        let observation = retire(host, app, fixture.store, &fixture.directory).await;
        let all = contexts.lock().unwrap();
        let first = all
            .iter()
            .find(|c| c.operation == Operation::Resume)
            .unwrap();
        assert_eq!(
            first.stored_route.as_ref().unwrap().revision,
            ExecutionRevision::new("v1").unwrap()
        );
        if missing {
            assert!(first.requested_route.is_none());
        } else {
            let requested = first.requested_route.as_ref().unwrap();
            assert_eq!(requested.route, current_definition.route);
            assert_eq!(requested.revision, ExecutionRevision::new("v2").unwrap());
        }
        assert_eq!(response.0, 404);
        assert!(response.1.contains("resource_unavailable"));
        assert_eq!(after, fixture.view);
        assert_eq!(authority, fixture.approval);
        assert_eq!(rows, fixture.rows);
        assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
        assert!(!observation.status().faulted);
    }
}
