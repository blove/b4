#[allow(dead_code)]
mod native_history_support;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use native_history_support::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_adopts_accepted_and_pure_history_without_claiming_callback_settlement() {
    for in_flight in [false, true] {
        let fixture = Prior::new(
            if in_flight {
                "startup-pure"
            } else {
                "startup-accepted"
            },
            definition("v1"),
            in_flight,
        );
        let old = calls();
        let current = calls();
        let old_validations = Arc::new(AtomicUsize::new(0));
        let current_validations = Arc::new(AtomicUsize::new(0));
        let gate = Gate::new();
        let _cleanup = GateCleanup(gate.clone());
        let old_registration = bound(
            definition("v1"),
            "v1",
            old.clone(),
            Some(gate.clone()),
            false,
            None,
            Some(Arc::new(RecordingValidator {
                calls: old_validations.clone(),
                reject: true,
                _capture: None,
            })),
        );
        let current_registration = bound(
            definition("v2"),
            "v2",
            current.clone(),
            None,
            false,
            None,
            Some(Arc::new(ExactCurrentInput {
                calls: current_validations.clone(),
            })),
        );
        let app = assembly(current_registration, vec![old_registration]);
        let weak_store = Arc::downgrade(&fixture.store);
        let weak_app = Arc::downgrade(&app);
        let preparing = HttpHost::new_with_application_preparing(
            fixture.store.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            configuration(app.clone(), 300_000),
        )
        .unwrap();
        let handle = preparing
            .begin_preparation(
                FixedResolver::trusted(),
                StartupPreparationLimits::default(),
            )
            .unwrap();
        let progress = preparation_complete(&handle).await;
        gate.entered().await;
        let held = fixture.authority();
        assert_eq!(held.execution.snapshot.status, ExecutionStatus::Running);
        assert_eq!(
            held.execution.snapshot.tasks[0].status,
            TaskStatus::InFlight
        );
        assert!(matches!(
            SqliteStore::open(fixture.directory.path()),
            Err(StoreError::AlreadyOwned)
        ));
        let serving = preparing
            .serving_host()
            .expect("observed preparation activated ordinary admission");
        let ready = json_body(request(&serving, "GET", "/readyz", None, false).await).await;
        let new=json_body(request(&serving,"POST","/threads/history-current-new/runs/wait",Some(serde_json::json!({"route":"/native-history#workflow","input":{"current":true}})),true).await).await;
        let new_view = fixture
            .store
            .thread_view(&ThreadId::new("history-current-new").unwrap())
            .unwrap()
            .unwrap();
        let rejected=json_body(request(&serving,"POST","/threads/history-current-invalid/runs/wait",Some(serde_json::json!({"route":"/native-history#workflow","input":{"current":false}})),true).await).await;
        let invalid_thread = fixture
            .store
            .thread(&ThreadId::new("history-current-invalid").unwrap())
            .unwrap();
        gate.release();
        let settled = completed(
            fixture.store.as_ref(),
            &fixture.approval.execution.snapshot.execution_id,
        )
        .await;
        let final_view = fixture.current();
        let final_authority = fixture.authority();
        let shutdown = preparing.shutdown_handle();
        quiesce(&shutdown).await;
        drop(serving);
        drop(preparing);
        drop(app);
        drop(fixture.store);
        gate.positive();
        weak_absent(&weak_store, &weak_app);
        let next = SqliteStore::open(fixture.directory.path()).unwrap();
        assert_eq!(next.owner_generation(), fixture.owner + 1);
        next.readiness().unwrap();
        assert_eq!(
            next.thread_view(&fixture.view.thread.id).unwrap().unwrap(),
            final_view
        );
        assert_eq!(
            next.approval_view(&fixture.approval.execution.snapshot.execution_id)
                .unwrap()
                .unwrap(),
            final_authority
        );
        drop(next);
        assert_eq!(
            progress,
            StartupProgress {
                pages: 1,
                scanned_rows: 1,
                candidates: 1,
                admitted: 1,
                runnable: 1,
                parked: 0,
                blocked: 0,
                terminal: 0
            }
        );
        assert_eq!(handle.status().phase, StartupPreparationPhase::Complete);
        assert_eq!(
            ready,
            (
                axum::http::StatusCode::OK,
                serde_json::json!({"status":"ready"})
            )
        );
        assert_eq!(
            new,
            (
                axum::http::StatusCode::OK,
                serde_json::json!({"selected_version":"v2"})
            )
        );
        assert_eq!(rejected.0, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(rejected.1["error"]["message"], "invalid_application_input");
        assert!(invalid_thread.is_none());
        assert_eq!(old_validations.load(Ordering::SeqCst), 0);
        assert_eq!(current_validations.load(Ordering::SeqCst), 2);
        assert_eq!(
            new_view.execution.as_ref().unwrap().snapshot.definition,
            definition("v2")
        );
        assert_eq!(
            new_view.execution.as_ref().unwrap().snapshot.tasks[0].result,
            Some(result("v2"))
        );
        assert_eq!(
            settled.snapshot.definition,
            fixture.approval.execution.snapshot.definition
        );
        assert_eq!(settled.snapshot.tasks[0].result, Some(result("v1")));
        let mut expected = fixture.approval.execution.snapshot.values.clone();
        expected.insert("actual_selected_version".into(), serde_json::json!("v1"));
        assert_eq!(settled.snapshot.values, expected);
        assert_eq!(
            settled.snapshot.attempts_used,
            if in_flight { 2 } else { 1 }
        );
        let old_call = assert_calls(&old, "v1", fixture.owner, &definition("v1"));
        assert_eq!(old_call.execution_id, settled.snapshot.execution_id);
        assert_eq!(old_call.turn_id, settled.snapshot.turn_id);
        assert_calls(&current, "v2", fixture.owner, &definition("v2"));
        assert!(final_authority.grants.is_empty() && final_authority.acceptances.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_missing_and_altered_history_remain_incomplete_with_unchanged_authority() {
    for changed in [false, true] {
        let fixture = Prior::new(
            if changed {
                "startup-changed"
            } else {
                "startup-missing"
            },
            definition("v1"),
            false,
        );
        let old = calls();
        let current = calls();
        let mut altered = definition("v1");
        altered.batches[0][0].recovery = RecoveryPolicy::Idempotent;
        let app = assembly(
            registration("v2", "v2", current.clone()),
            if changed {
                vec![bound(altered, "v1", old.clone(), None, false, None, None)]
            } else {
                Vec::new()
            },
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
        let handle = preparing
            .begin_preparation(
                FixedResolver::trusted(),
                StartupPreparationLimits::default(),
            )
            .unwrap();
        let observed = tokio::time::timeout(WAIT, handle.wait(WAIT))
            .await
            .unwrap()
            .unwrap();
        let no_serving = preparing.serving_host().is_none();
        let after = fixture.current();
        let authority = fixture.authority();
        let rows = fixture.directory.rows();
        let shutdown = preparing.shutdown_handle();
        quiesce(&shutdown).await;
        drop(preparing);
        drop(app);
        drop(fixture.store);
        weak_absent(&weak_store, &weak_app);
        let next = SqliteStore::open(fixture.directory.path()).unwrap();
        assert_eq!(next.owner_generation(), fixture.owner + 1);
        next.readiness().unwrap();
        drop(next);
        assert_eq!(
            observed,
            StartupWaitOutcome::Finished(StartupPreparationOutcome::Incomplete {
                progress: StartupProgress {
                    pages: 1,
                    scanned_rows: 1,
                    candidates: 1,
                    admitted: 0,
                    runnable: 0,
                    parked: 0,
                    blocked: 0,
                    terminal: 0
                },
                reason: StartupIncompleteReason::RecoveryRefused
            })
        );
        assert!(no_serving);
        assert_eq!(after, fixture.view);
        assert_eq!(authority, fixture.approval);
        assert_eq!(rows, fixture.rows);
        assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
        assert!(!shutdown.status().faulted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_drains_a_historical_only_removed_route() {
    let mut removed = definition("v1");
    removed.route = RouteKey::new("/historical-only#workflow").unwrap();
    let fixture = Prior::new("startup-removed", removed.clone(), false);
    let old = calls();
    let current = calls();
    let app = assembly(
        registration("v2", "v2", current.clone()),
        vec![bound(
            removed.clone(),
            "v1",
            old.clone(),
            None,
            false,
            None,
            None,
        )],
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
    let handle = preparing
        .begin_preparation(
            FixedResolver::trusted(),
            StartupPreparationLimits::default(),
        )
        .unwrap();
    let progress = preparation_complete(&handle).await;
    let settled = completed(
        fixture.store.as_ref(),
        &fixture.approval.execution.snapshot.execution_id,
    )
    .await;
    let serving = preparing.serving_host().unwrap();
    let not_new = wire(
        request(
            &serving,
            "POST",
            "/threads/removed-startup-new/runs/wait",
            Some(serde_json::json!({"route":removed.route,"input":null})),
            true,
        )
        .await,
    )
    .await;
    let absent = fixture
        .store
        .thread(&ThreadId::new("removed-startup-new").unwrap())
        .unwrap();
    let shutdown = preparing.shutdown_handle();
    quiesce(&shutdown).await;
    drop(serving);
    drop(preparing);
    drop(app);
    drop(fixture.store);
    weak_absent(&weak_store, &weak_app);
    let next = SqliteStore::open(fixture.directory.path()).unwrap();
    assert_eq!(next.owner_generation(), fixture.owner + 1);
    next.readiness().unwrap();
    drop(next);
    assert_eq!(progress.admitted, 1);
    assert_eq!(progress.runnable, 1);
    assert_eq!(settled.snapshot.definition, removed);
    assert_eq!(settled.snapshot.tasks[0].result, Some(result("v1")));
    assert_eq!(not_new.0, axum::http::StatusCode::NOT_FOUND);
    assert!(absent.is_none());
    assert_calls(&old, "v1", fixture.owner, &removed);
    assert!(current.lock().unwrap().is_empty());
    assert!(!shutdown.status().faulted);
}
