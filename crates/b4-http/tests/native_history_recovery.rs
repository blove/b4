mod native_history_support;

use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use native_history_support::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_typed_recovery_adopts_prior_owner_recorded_definition() {
    let directory = Directory::new();
    let prior = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let first_owner = prior.owner_generation();
    let (original_view, original_approval) = seed_accepted(prior.as_ref());
    let prior_weak = Arc::downgrade(&prior);
    drop(prior);
    assert!(prior_weak.upgrade().is_none());

    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let owner = store.owner_generation();
    assert_eq!(owner, first_owner + 1);
    store.readiness().unwrap();
    let view = store
        .thread_view(&original_view.thread.id)
        .unwrap()
        .unwrap();
    assert_eq!(view, original_view);
    assert_eq!(
        store
            .approval_view(&original_approval.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        original_approval
    );
    let raw_before = directory.rows();
    let target = RecoveryTarget::from_view(&view).unwrap();
    let old_calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let current_calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let application = Arc::new(
        NativeApplication::new_with_history(
            ApplicationIdentity {
                application: principal().application,
                revision: "assembly-v2".into(),
            },
            vec![registration("v2", "v2", current_calls.clone())],
            vec![registration("v1", "v1", old_calls.clone())],
        )
        .unwrap(),
    );
    let application_weak = Arc::downgrade(&application);
    let configuration = NativeHostConfig::new(
        application.clone(),
        ApprovalHostConfig::new("history-current-policy".into(), BTreeMap::new()),
    );
    assert_eq!(
        configuration.approval.service_config,
        ApprovalServiceConfig::RequiredDurable
    );
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        configuration,
    )
    .unwrap();
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    let observed = tokio::time::timeout(WAIT, host.admit_recovery(principal(), target))
        .await
        .expect("history recovery admission remained finite");

    // GREEN must let the actual finite producer finish; the staged healthy
    // definition refusal has no producer. No abort/forced cleanup is used.
    if observed.is_ok() {
        tokio::time::timeout(WAIT, async {
            loop {
                let snapshot = store
                    .execution(&original_approval.execution.snapshot.execution_id)
                    .unwrap()
                    .unwrap()
                    .snapshot;
                if snapshot.status == ExecutionStatus::Completed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original selected finite callback completed");
    }
    let after_view = store
        .thread_view(&original_view.thread.id)
        .unwrap()
        .unwrap();
    let after_approval = store
        .approval_view(&original_approval.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let raw_after = directory.rows();
    let store_weak = Arc::downgrade(&store);
    let shutdown = host.shutdown_handle();
    quiesce(&shutdown).await;
    drop(host);
    drop(application);
    drop(store);
    assert!(application_weak.upgrade().is_none());
    assert!(store_weak.upgrade().is_none());
    let successor = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(successor.owner_generation(), owner + 1);
    successor.readiness().unwrap();
    assert_eq!(
        successor
            .thread_view(&original_view.thread.id)
            .unwrap()
            .unwrap(),
        after_view
    );
    drop(successor);

    // Refusal evidence and feature expectations follow real normal ownership
    // retirement. Pure views, observation and shutdown handle remain alive.
    if let Err(error) = &observed {
        assert_eq!(error.reason(), RecoveryRefusal::DefinitionUnavailable);
        assert!(error.adoption().is_none());
        assert_eq!(after_view, original_view);
        assert_eq!(after_approval, original_approval);
        assert_eq!(raw_after, raw_before);
        assert!(old_calls.lock().unwrap().is_empty());
        assert!(current_calls.lock().unwrap().is_empty());
    }
    eprintln!("legal prior-owner source reopened; healthy Quiescent; both Weak owners absent; actual next writer opened before history expectation");
    let admission =
        observed.expect("retained v1 typed recovery must adopt its exact recorded Definition");
    assert_eq!(admission.disposition, RecoveryDisposition::Runnable);
    assert_eq!(admission.adoption.thread_id, original_view.thread.id);
    assert_eq!(
        admission.adoption.thread_generation,
        original_view.thread.generation
    );
    assert_eq!(
        admission.adoption.execution_id,
        original_approval.execution.snapshot.execution_id
    );
    assert_eq!(admission.adoption.execution_fence.owner_generation, owner);
    assert_eq!(
        admission.adoption.execution_fence.revision,
        original_approval.execution.snapshot.revision + 1
    );
    let completed = &after_approval.execution.snapshot;
    assert_eq!(completed.definition, definition("v1"));
    assert_eq!(completed.status, ExecutionStatus::Completed);
    assert_eq!(completed.attempts_used, 1);
    assert_eq!(completed.tasks[0].result, Some(result("v1")));
    assert_eq!(
        completed.values["actual_selected_version"],
        serde_json::json!("v1")
    );
    let calls = old_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1.application_revision, "assembly-v2");
    assert_eq!(
        calls[0].1.execution_revision,
        ExecutionRevision::new("v1").unwrap()
    );
    assert_eq!(calls[0].0.owner_generation, owner);
    assert!(current_calls.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_legacy_void_output_and_attachment_replay_keep_distinct_shapes() {
    let fixture = Prior::new("void", definition("v1"), true);
    let old = calls();
    let current = calls();
    let gate = Gate::new();
    let _cleanup = GateCleanup(gate.clone());
    let app = assembly(
        registration("v2", "v2", current.clone()),
        vec![bound(
            definition("v1"),
            "v1",
            old.clone(),
            Some(gate.clone()),
            true,
            None,
            None,
        )],
    );
    let host = host(fixture.store.clone(), app.clone(), Arc::new(Policy));
    let response =
        tokio::time::timeout(WAIT, host.recover_execution(principal(), fixture.target()))
            .await
            .unwrap();
    gate.entered().await;
    let held = fixture.authority();
    assert_eq!(held.execution.snapshot.owner_generation, fixture.owner);
    assert_eq!(held.execution.snapshot.definition, definition("v1"));
    assert_eq!(
        held.execution.snapshot.tasks[0].status,
        TaskStatus::InFlight
    );
    let attachment = request(
        &host,
        "GET",
        &format!("/threads/{}/runs/stream", fixture.view.thread.id),
        None,
        true,
    )
    .await;
    let attachment_status = attachment.status();
    let mut viewer = attachment.into_body().into_data_stream();
    let prefix = first_frame(&mut viewer).await;
    drop(viewer);
    assert!(matches!(
        SqliteStore::open(fixture.directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    gate.release();
    let terminal = wire(response).await;
    let final_execution = completed(
        fixture.store.as_ref(),
        &fixture.approval.execution.snapshot.execution_id,
    )
    .await;
    idle(&host).await;
    let replay = wire(
        request(
            &host,
            "GET",
            &format!("/threads/{}/runs/stream", fixture.view.thread.id),
            None,
            true,
        )
        .await,
    )
    .await;
    let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
    gate.positive();
    assert_eq!(attachment_status, axum::http::StatusCode::OK);
    assert!(prefix.contains("event: state") && prefix.contains("\"live\":true"));
    assert_eq!(terminal.0, axum::http::StatusCode::OK);
    assert!(terminal.1.contains("event: done\ndata: {}"));
    assert_eq!(replay.0, axum::http::StatusCode::OK);
    assert!(replay.1.contains("event: state") && replay.1.contains("\"live\":false"));
    assert!(replay.1.contains("event: done\ndata: {\"output\":null}"));
    let mut expected = result("v1");
    expected.output = serde_json::Value::Null;
    assert_eq!(
        final_execution.snapshot.definition,
        fixture.approval.execution.snapshot.definition
    );
    assert_eq!(final_execution.snapshot.tasks[0].result, Some(expected));
    assert_eq!(final_execution.snapshot.attempts_used, 2);
    let mut expected_values = fixture.approval.execution.snapshot.values.clone();
    expected_values.insert("actual_selected_version".into(), serde_json::json!("v1"));
    assert_eq!(final_execution.snapshot.values, expected_values);
    let call = assert_calls(&old, "v1", fixture.owner, &definition("v1"));
    assert_ne!(
        call.attempt_id,
        fixture.approval.execution.snapshot.tasks[0]
            .attempt_id
            .clone()
            .unwrap()
    );
    assert_eq!(call.execution_id, final_execution.snapshot.execution_id);
    assert_eq!(call.turn_id, final_execution.snapshot.turn_id);
    assert_eq!(
        call.input["input"],
        fixture.approval.execution.snapshot.values["input"]
    );
    assert!(current.lock().unwrap().is_empty());
    assert_eq!(shutdown.status().active_owners, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_historical_legacy_response_keeps_the_real_callback_owned() {
    let fixture = Prior::new("detach", definition("v1"), false);
    let old = calls();
    let current = calls();
    let gate = Gate::new();
    let _cleanup = GateCleanup(gate.clone());
    let app = assembly(
        registration("v2", "v2", current.clone()),
        vec![bound(
            definition("v1"),
            "v1",
            old.clone(),
            Some(gate.clone()),
            false,
            None,
            None,
        )],
    );
    let host = host(fixture.store.clone(), app.clone(), Arc::new(Policy));
    let response =
        tokio::time::timeout(WAIT, host.recover_execution(principal(), fixture.target()))
            .await
            .unwrap();
    gate.entered().await;
    let status = response.status();
    drop(response);
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    let cut = shutdown
        .wait_quiescent(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert!(matches!(cut,ShutdownOutcome::Incomplete { active_owners,.. } if active_owners>0));
    assert!(matches!(
        SqliteStore::open(fixture.directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    gate.release();
    let final_execution = completed(
        fixture.store.as_ref(),
        &fixture.approval.execution.snapshot.execution_id,
    )
    .await;
    let observation = retire(host, app, fixture.store, &fixture.directory).await;
    gate.positive();
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(final_execution.snapshot.tasks[0].result, Some(result("v1")));
    assert_eq!(
        final_execution.snapshot.values["actual_selected_version"],
        serde_json::json!("v1")
    );
    assert_calls(&old, "v1", fixture.owner, &definition("v1"));
    assert!(current.lock().unwrap().is_empty());
    assert_eq!(observation.status().active_owners, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_historical_route_drains_without_becoming_a_new_route() {
    let mut removed = definition("v1");
    removed.route = RouteKey::new("/removed-history#workflow").unwrap();
    let fixture = Prior::new("removed", removed.clone(), false);
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
    assert!(app.definition(&removed.route).is_none());
    let host = host(fixture.store.clone(), app.clone(), Arc::new(Policy));
    let admission = tokio::time::timeout(WAIT, host.admit_recovery(principal(), fixture.target()))
        .await
        .unwrap();
    let final_execution = completed(
        fixture.store.as_ref(),
        &fixture.approval.execution.snapshot.execution_id,
    )
    .await;
    let new = wire(
        request(
            &host,
            "POST",
            "/threads/removed-new/runs/wait",
            Some(serde_json::json!({"route":removed.route,"input":null})),
            true,
        )
        .await,
    )
    .await;
    idle(&host).await;
    let replay = wire(
        request(
            &host,
            "GET",
            &format!("/threads/{}/runs/stream", fixture.view.thread.id),
            None,
            true,
        )
        .await,
    )
    .await;
    let no_new_thread = fixture
        .store
        .thread(&ThreadId::new("removed-new").unwrap())
        .unwrap();
    let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
    assert_eq!(
        admission.unwrap().disposition,
        RecoveryDisposition::Runnable
    );
    assert_eq!(final_execution.snapshot.definition, removed);
    assert_eq!(final_execution.snapshot.tasks[0].result, Some(result("v1")));
    assert_eq!(new.0, axum::http::StatusCode::NOT_FOUND);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&new.1).unwrap()["error"]["message"],
        "resource_unavailable"
    );
    assert!(no_new_thread.is_none());
    assert_eq!(replay.0, axum::http::StatusCode::OK);
    assert!(replay.1.contains("event: done\ndata: {\"output\":null}"));
    assert_calls(&old, "v1", fixture.owner, &removed);
    assert!(current.lock().unwrap().is_empty());
    assert_eq!(shutdown.status().active_owners, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_or_altered_history_refuses_without_mutating_any_authority() {
    for changed in [false, true] {
        let fixture = Prior::new(
            if changed { "changed" } else { "absent" },
            definition("v1"),
            false,
        );
        let old = calls();
        let current = calls();
        let mut altered = definition("v1");
        altered.max_attempts += 1;
        let history = if changed {
            vec![bound(altered, "v1", old.clone(), None, false, None, None)]
        } else {
            Vec::new()
        };
        let app = assembly(registration("v2", "v2", current.clone()), history);
        let host = host(fixture.store.clone(), app.clone(), Arc::new(Policy));
        let typed = tokio::time::timeout(WAIT, host.admit_recovery(principal(), fixture.target()))
            .await
            .unwrap();
        let legacy = wire(
            tokio::time::timeout(WAIT, host.recover_execution(principal(), fixture.target()))
                .await
                .unwrap(),
        )
        .await;
        let after = fixture.current();
        let authority = fixture.authority();
        let rows = fixture.directory.rows();
        let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
        let error = typed.unwrap_err();
        assert_eq!(error.reason(), RecoveryRefusal::DefinitionUnavailable);
        assert!(error.adoption().is_none());
        assert_eq!(legacy.0, axum::http::StatusCode::CONFLICT);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&legacy.1).unwrap()["error"]["message"],
            "recovery_definition_unavailable"
        );
        assert_eq!(after, fixture.view);
        assert_eq!(authority, fixture.approval);
        assert_eq!(rows, fixture.rows);
        assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
        assert!(!shutdown.status().faulted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_recovery_preserves_actor_policy_and_every_captured_fence() {
    for variant in 0..7 {
        let fixture = Prior::new(&format!("fence-{variant}"), definition("v1"), false);
        let old = calls();
        let current = calls();
        let app = assembly(
            registration("v2", "v2", current.clone()),
            vec![registration("v1", "v1", old.clone())],
        );
        let policy = policy_probe(Operation::Recover, None, None);
        if variant == 1 {
            policy
                .allowed
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        let host = host(fixture.store.clone(), app.clone(), policy.clone());
        let mut actor = principal();
        let mut target = fixture.target();
        match variant {
            0 => actor.principal = "different-trusted-actor".into(),
            1 => {}
            2 => target.fence.resource_revision += 1,
            3 => target.fence.execution.revision += 1,
            4 => target.execution_id = ExecutionId::new("different-execution").unwrap(),
            5 => target.fence.execution.owner_generation += 1,
            _ => {
                let original = target.thread_generation.as_str();
                let mut bytes = original.as_bytes().to_vec();
                bytes[0] = if bytes[0] == b'0' { b'1' } else { b'0' };
                target.thread_generation =
                    ThreadGeneration::new(String::from_utf8(bytes).unwrap()).unwrap();
            }
        }
        let outcome = tokio::time::timeout(WAIT, host.admit_recovery(actor, target))
            .await
            .unwrap();
        let after = fixture.current();
        let authority = fixture.authority();
        let rows = fixture.directory.rows();
        drop(policy);
        let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
        let error = outcome.unwrap_err();
        assert_eq!(
            error.reason(),
            if variant <= 1 {
                RecoveryRefusal::ResourceUnavailable
            } else {
                RecoveryRefusal::ResourceChanged
            }
        );
        assert!(error.adoption().is_none());
        assert_eq!(after, fixture.view);
        assert_eq!(authority, fixture.approval);
        assert_eq!(rows, fixture.rows);
        assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
        assert!(!shutdown.status().faulted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_owner_and_real_successor_cannot_reuse_the_original_history_target() {
    let fixture = Prior::new("successor", definition("v1"), false);
    let old = calls();
    let current = calls();
    let app = assembly(
        registration("v2", "v2", current.clone()),
        vec![registration("v1", "v1", old.clone())],
    );
    let host = host(fixture.store.clone(), app.clone(), Arc::new(Policy));
    let first = tokio::time::timeout(WAIT, host.admit_recovery(principal(), fixture.target()))
        .await
        .unwrap()
        .unwrap();
    completed(fixture.store.as_ref(), &first.adoption.execution_id).await;
    idle(&host).await;
    let successor = b4_kernel::new_execution(
        ExecutionId::new("actual-successor").unwrap(),
        TurnId::new("actual-successor-turn").unwrap(),
        fixture.owner,
        definition("v2"),
        serde_json::Map::new(),
        TransitionFacts { now_ms: 10 },
    )
    .unwrap();
    fixture
        .store
        .admit(&fixture.view.thread.id, successor)
        .unwrap();
    let before = fixture.current();
    let new_target = RecoveryTarget::from_view(&before).unwrap();
    let rows = fixture.directory.rows();
    let stale = tokio::time::timeout(WAIT, host.admit_recovery(principal(), fixture.target()))
        .await
        .unwrap();
    let local = tokio::time::timeout(WAIT, host.admit_recovery(principal(), new_target))
        .await
        .unwrap();
    let after = fixture.current();
    let after_rows = fixture.directory.rows();
    let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
    assert_eq!(
        stale.unwrap_err().reason(),
        RecoveryRefusal::ResourceChanged
    );
    assert_eq!(
        local.unwrap_err().reason(),
        RecoveryRefusal::RecoveryNotNeeded
    );
    assert_eq!(after, before);
    assert_eq!(after_rows, rows);
    assert_calls(&old, "v1", fixture.owner, &definition("v1"));
    assert!(current.lock().unwrap().is_empty());
    assert!(!shutdown.status().faulted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_policy_revocation_after_actual_adoption_keeps_confirmed_evidence() {
    let fixture = Prior::new("renewal", definition("v1"), false);
    let old = calls();
    let current = calls();
    let app = assembly(
        registration("v2", "v2", current.clone()),
        vec![registration("v1", "v1", old.clone())],
    );
    let gate = Gate::new();
    let _cleanup = GateCleanup(gate.clone());
    let policy = policy_probe(
        Operation::Recover,
        Some(gate.clone()),
        Some(fixture.store.clone()),
    );
    let host = host(fixture.store.clone(), app.clone(), policy.clone());
    let owned = host.clone();
    let target = fixture.target();
    let pending = tokio::spawn(async move { owned.admit_recovery(principal(), target).await });
    gate.entered().await;
    let adopted = fixture.current();
    let authority = fixture.authority();
    let adopted_rows = fixture.directory.rows();
    assert_eq!(
        adopted
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .owner_generation,
        fixture.owner
    );
    assert_eq!(
        adopted.execution.as_ref().unwrap().snapshot.definition,
        definition("v1")
    );
    policy
        .allowed
        .store(false, std::sync::atomic::Ordering::SeqCst);
    gate.release();
    let observed = tokio::time::timeout(WAIT, pending).await.unwrap().unwrap();
    let after = fixture.current();
    let after_authority = fixture.authority();
    let after_rows = fixture.directory.rows();
    drop(policy);
    let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
    gate.positive();
    // The genuine admission milestone precedes later dispatch authority.
    // Revocation here stops invocation without rolling back confirmed adoption.
    let confirmed_admission = observed.unwrap();
    assert_eq!(
        confirmed_admission.disposition,
        RecoveryDisposition::Runnable
    );
    let receipt = confirmed_admission.adoption;
    assert_eq!(
        receipt.execution_id,
        fixture.approval.execution.snapshot.execution_id
    );
    assert_eq!(receipt.execution_fence.owner_generation, fixture.owner);
    assert_eq!(
        receipt.execution_fence.revision,
        fixture.approval.execution.snapshot.revision + 1
    );
    assert_eq!(receipt.thread_id, fixture.view.thread.id);
    assert_eq!(receipt.thread_generation, fixture.view.thread.generation);
    assert_eq!(
        receipt.turn_id,
        adopted.execution.as_ref().unwrap().snapshot.turn_id
    );
    assert_eq!(after, adopted);
    assert_eq!(after_authority, authority);
    assert_eq!(after_rows, adopted_rows);
    assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
    assert!(!shutdown.status().faulted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_native_resume_policy_uses_recorded_support_even_when_history_is_missing() {
    for supported in [false, true] {
        let fixture = Prior::new(
            if supported {
                "resume-supported"
            } else {
                "resume-missing"
            },
            definition("v1"),
            false,
        );
        let old = calls();
        let current = calls();
        let app = assembly(
            registration("v2", "v2", current.clone()),
            if supported {
                vec![registration("v1", "v1", old.clone())]
            } else {
                Vec::new()
            },
        );
        let gate = Gate::new();
        let _cleanup = GateCleanup(gate.clone());
        let policy = policy_probe(Operation::Resume, Some(gate.clone()), None);
        let contexts = policy.contexts.clone();
        let host = host(fixture.store.clone(), app.clone(), policy.clone());
        let owned = host.clone();
        let path = format!("/threads/{}/resume", fixture.view.thread.id);
        let pending = tokio::spawn(async move {
            request(
                &owned,
                "POST",
                &path,
                Some(serde_json::json!({"route":"/native-history#workflow","resume":[{"status":"cancelled","interruptId":"unsubmitted-history-probe"}]})),
                true,
            )
            .await
        });
        gate.entered().await;
        assert_resume_v1(&contexts);
        gate.release();
        let response = wire(tokio::time::timeout(WAIT, pending).await.unwrap().unwrap()).await;
        let after = fixture.current();
        let authority = fixture.authority();
        let rows = fixture.directory.rows();
        drop(policy);
        let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
        gate.positive();
        assert_eq!(response.0, axum::http::StatusCode::CONFLICT);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response.1).unwrap()["error"]["message"],
            if supported {
                "recovery_required"
            } else {
                "route_mismatch"
            }
        );
        assert_eq!(after, fixture.view);
        assert_eq!(authority, fixture.approval);
        assert_eq!(rows, fixture.rows);
        assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
        assert!(!shutdown.status().faulted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_historical_resume_policy_retires_all_transitive_registration_captures() {
    let fixture = Prior::new("resume-abandoned", definition("v1"), false);
    let old = calls();
    let current = calls();
    let validations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let validator = Arc::new(RecordingValidator {
        calls: validations.clone(),
        reject: false,
        _capture: Some(fixture.store.clone()),
    });
    let app = assembly(
        registration("v2", "v2", current.clone()),
        vec![bound(
            definition("v1"),
            "v1",
            old.clone(),
            None,
            false,
            Some(fixture.store.clone()),
            Some(validator),
        )],
    );
    let weak_store = Arc::downgrade(&fixture.store);
    let weak_app = Arc::downgrade(&app);
    let gate = Gate::new();
    let _cleanup = GateCleanup(gate.clone());
    let policy = policy_probe(Operation::Resume, Some(gate.clone()), None);
    let contexts = policy.contexts.clone();
    let host = host(fixture.store.clone(), app.clone(), policy.clone());
    let owned = host.clone();
    let path = format!("/threads/{}/resume", fixture.view.thread.id);
    let pending = tokio::spawn(async move {
        request(
            &owned,
            "POST",
            &path,
            Some(serde_json::json!({"route":"/native-history#workflow","resume":[{"status":"cancelled","interruptId":"unsubmitted-history-probe"}]})),
            true,
        )
        .await
    });
    gate.entered().await;
    assert_resume_v1(&contexts);
    pending.abort();
    assert!(tokio::time::timeout(WAIT, pending)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    let after = fixture.current();
    let authority = fixture.authority();
    let rows = fixture.directory.rows();
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    drop(host);
    drop(app);
    drop(policy);
    drop(fixture.store);
    let held = shutdown
        .wait_quiescent(std::time::Duration::ZERO)
        .await
        .unwrap();
    assert!(
        matches!(held,ShutdownOutcome::Incomplete{active_owners,faulted:false} if active_owners>0)
    );
    assert!(weak_store.upgrade().is_some() && weak_app.upgrade().is_some());
    assert!(matches!(
        SqliteStore::open(fixture.directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    gate.release();
    quiesce(&shutdown).await;
    gate.positive();
    weak_absent(&weak_store, &weak_app);
    let next = SqliteStore::open(fixture.directory.path()).unwrap();
    assert_eq!(next.owner_generation(), fixture.owner + 1);
    next.readiness().unwrap();
    let reopened = next.thread_view(&fixture.view.thread.id).unwrap().unwrap();
    let reopened_authority = next
        .approval_view(&fixture.approval.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    drop(next);
    assert_eq!(after, fixture.view);
    assert_eq!(authority, fixture.approval);
    assert_eq!(rows, fixture.rows);
    assert_eq!(reopened, fixture.view);
    assert_eq!(reopened_authority, fixture.approval);
    assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
    assert_eq!(validations.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_scripted_resume_preserves_initial_current_requested_provenance() {
    let fixture = Prior::new("scripted-provenance", definition("v1"), false);
    let gate = Gate::new();
    let _cleanup = GateCleanup(gate.clone());
    let policy = policy_probe(Operation::Resume, Some(gate.clone()), None);
    let contexts = policy.contexts.clone();
    let config = HostConfig::new(vec![ScriptRoute {
        definition: definition("v2"),
        effects: BTreeMap::from([(EffectId::new("shared-effect").unwrap(), ScriptEffect::Input)]),
    }]);
    let host = HttpHost::new_with_approvals(
        fixture.store.clone(),
        Arc::new(Auth),
        policy.clone(),
        config,
        ApprovalHostConfig::new("history-current-policy".into(), BTreeMap::new()),
    )
    .unwrap();
    let owned = host.clone();
    let path = format!("/threads/{}/resume", fixture.view.thread.id);
    let pending = tokio::spawn(async move {
        request(
            &owned,
            "POST",
            &path,
            Some(serde_json::json!({"route":"/native-history#workflow","resume":[{"status":"cancelled","interruptId":"unsubmitted-history-probe"}]})),
            true,
        )
        .await
    });
    gate.entered().await;
    {
        let all = contexts.lock().unwrap();
        let first = all
            .iter()
            .find(|c| c.operation == Operation::Resume)
            .unwrap();
        assert_eq!(
            first.stored_route.as_ref().unwrap().revision,
            ExecutionRevision::new("v1").unwrap()
        );
        assert_eq!(
            first.requested_route.as_ref().unwrap().revision,
            ExecutionRevision::new("v2").unwrap()
        );
    }
    gate.release();
    let response = wire(tokio::time::timeout(WAIT, pending).await.unwrap().unwrap()).await;
    let after = fixture.current();
    let authority = fixture.authority();
    let rows = fixture.directory.rows();
    let shutdown = host.shutdown_handle();
    let weak = Arc::downgrade(&fixture.store);
    quiesce(&shutdown).await;
    drop(host);
    drop(policy);
    drop(fixture.store);
    assert!(weak.upgrade().is_none());
    let next = SqliteStore::open(fixture.directory.path()).unwrap();
    assert_eq!(next.owner_generation(), fixture.owner + 1);
    next.readiness().unwrap();
    drop(next);
    gate.positive();
    assert_eq!(response.0, axum::http::StatusCode::CONFLICT);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&response.1).unwrap()["error"]["message"],
        "route_mismatch"
    );
    assert_eq!(after, fixture.view);
    assert_eq!(authority, fixture.approval);
    assert_eq!(rows, fixture.rows);
    assert!(!shutdown.status().faulted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_once_and_deny_keep_real_grants_and_never_borrow_history_prompts() {
    for decision in ["once", "deny"] {
        let fixture = Prior::new(decision, definition("v1"), false);
        let old = calls();
        let current = calls();
        let validations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut selected = definition("v2");
        selected.batches[0][0].requires_approval = true;
        let app = assembly(
            bound(
                selected.clone(),
                "v2",
                current.clone(),
                None,
                false,
                None,
                Some(Arc::new(RecordingValidator {
                    calls: validations.clone(),
                    reject: false,
                    _capture: None,
                })),
            ),
            vec![registration("v1", "v1", old.clone())],
        );
        let host = host(fixture.store.clone(), app.clone(), Arc::new(Policy));
        let thread = ThreadId::new(format!("history-current-{decision}")).unwrap();
        let path = format!("/threads/{thread}");
        let parked = json_body(
            request(
                &host,
                "POST",
                &format!("{path}/runs/wait"),
                Some(serde_json::json!({"route":selected.route,"input":[false,null,0]})),
                true,
            )
            .await,
        )
        .await;
        let before = fixture.store.thread_view(&thread).unwrap().unwrap();
        let original = fixture.authority();
        let pending = json_body(
            request(
                &host,
                "GET",
                &format!("{path}/pending_interrupts"),
                None,
                true,
            )
            .await,
        )
        .await;
        let entry = &pending.1["interrupts"][0];
        let before_authority = fixture
            .store
            .approval_view(&before.execution.as_ref().unwrap().snapshot.execution_id)
            .unwrap()
            .unwrap();
        let ordinary =
            json_body(request(&host, "GET", &format!("{path}/state"), None, true).await).await;
        let payload = serde_json::json!({"route":selected.route,"resume":[{"status":"resolved","interruptId":entry["interruptId"],"payload":decision,"grant":entry["grant"]}]});
        let resumed = wire(
            request(
                &host,
                "POST",
                &format!("{path}/resume"),
                Some(payload.clone()),
                true,
            )
            .await,
        )
        .await;
        let after = fixture.store.thread_view(&thread).unwrap().unwrap();
        let authority = fixture
            .store
            .approval_view(&after.execution.as_ref().unwrap().snapshot.execution_id)
            .unwrap()
            .unwrap();
        let repeated = wire(
            request(
                &host,
                "POST",
                &format!("{path}/resume"),
                Some(payload),
                true,
            )
            .await,
        )
        .await;
        let original_after = fixture.authority();
        let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
        assert_eq!(parked.0, axum::http::StatusCode::OK);
        assert_eq!(parked.1, serde_json::Value::Null);
        assert_eq!(
            before.execution.as_ref().unwrap().snapshot.status,
            ExecutionStatus::WaitingForApproval
        );
        assert_eq!(pending.0, axum::http::StatusCode::OK);
        assert_eq!(pending.1["interrupts"].as_array().unwrap().len(), 1);
        assert_eq!(
            entry["value"],
            serde_json::json!({"question":"allow current-v2 fixture?"})
        );
        assert_eq!(resumed.0, axum::http::StatusCode::OK);
        assert_eq!(
            after.execution.as_ref().unwrap().snapshot.execution_id,
            before.execution.as_ref().unwrap().snapshot.execution_id
        );
        assert_ne!(
            after.execution.as_ref().unwrap().snapshot.turn_id,
            before.execution.as_ref().unwrap().snapshot.turn_id
        );
        assert_eq!(
            after.execution.as_ref().unwrap().snapshot.definition,
            selected
        );
        assert_eq!(authority.grants.len(), 1);
        let mut original_grant = before_authority.grants[0].clone();
        assert_eq!(
            entry["grant"].as_str().unwrap(),
            original_grant.token.as_str()
        );
        assert_eq!(
            entry["resumeKey"].as_str().unwrap(),
            original_grant.resume_key
        );
        assert_eq!(
            original_grant.definition_revision,
            ExecutionRevision::new("v2").unwrap()
        );
        assert_eq!(original_grant.origin, original_origin());
        assert_eq!(original_grant.policy_revision, "history-current-policy");
        original_grant.consumed = true;
        assert_eq!(authority.grants[0], original_grant);
        assert!(authority.grants[0].consumed);
        assert!(!ordinary
            .1
            .to_string()
            .contains(entry["grant"].as_str().unwrap()));
        assert!(!ordinary
            .1
            .to_string()
            .contains(entry["resumeKey"].as_str().unwrap()));
        assert_eq!(authority.acceptances.len(), 1);
        assert_eq!(
            authority.acceptances[0].decisions[0].decision,
            if decision == "once" {
                ApprovalDecision::Once
            } else {
                ApprovalDecision::Deny
            }
        );
        assert_eq!(validations.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(old.lock().unwrap().is_empty());
        if decision == "once" {
            assert_eq!(
                after.execution.as_ref().unwrap().snapshot.tasks[0].result,
                Some(result("v2"))
            );
            assert_calls(&current, "v2", fixture.owner, &selected);
        } else {
            assert!(current.lock().unwrap().is_empty());
            assert_eq!(
                after.execution.as_ref().unwrap().snapshot.tasks[0].status,
                TaskStatus::Denied
            );
        }
        assert_eq!(repeated.0, axum::http::StatusCode::CONFLICT);
        assert_eq!(original_after, original);
        assert_eq!(original, fixture.approval);
        assert!(!shutdown.status().faulted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_current_grant_refuses_without_changing_approval_or_history() {
    let fixture = Prior::new("ttl", definition("v1"), false);
    let old = calls();
    let current = calls();
    let mut selected = definition("v2");
    selected.batches[0][0].requires_approval = true;
    let app = assembly(
        bound(
            selected.clone(),
            "v2",
            current.clone(),
            None,
            false,
            None,
            None,
        ),
        vec![registration("v1", "v1", old.clone())],
    );
    let host = HttpHost::new_with_application(
        fixture.store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        configuration(app.clone(), 1_000),
    )
    .unwrap();
    let parked = request(
        &host,
        "POST",
        "/threads/history-expiring/runs/wait",
        Some(serde_json::json!({"route":selected.route,"input":null})),
        true,
    )
    .await;
    let _ = json_body(parked).await;
    let thread = ThreadId::new("history-expiring").unwrap();
    let before = fixture.store.thread_view(&thread).unwrap().unwrap();
    let execution_id = before
        .execution
        .as_ref()
        .unwrap()
        .snapshot
        .execution_id
        .clone();
    let authority = fixture.store.approval_view(&execution_id).unwrap().unwrap();
    let rows = fixture.directory.rows();
    let grant = &authority.grants[0];
    expire_at(grant.expires_at_ms).await;
    let payload = serde_json::json!({"route":selected.route,"resume":[{"status":"resolved","interruptId":grant.interrupt_id,"payload":"once","grant":grant.token.as_str()}]});
    let rejected = wire(
        request(
            &host,
            "POST",
            "/threads/history-expiring/resume",
            Some(payload),
            true,
        )
        .await,
    )
    .await;
    let after = fixture.store.thread_view(&thread).unwrap().unwrap();
    let after_authority = fixture.store.approval_view(&execution_id).unwrap().unwrap();
    let after_rows = fixture.directory.rows();
    let original = fixture.authority();
    let shutdown = retire(host, app, fixture.store, &fixture.directory).await;
    assert_eq!(rejected.0, axum::http::StatusCode::CONFLICT);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rejected.1).unwrap()["error"]["message"],
        "grant_expired"
    );
    assert_eq!(after, before);
    assert_eq!(after_authority, authority);
    assert_eq!(after_rows, rows);
    assert_eq!(original, fixture.approval);
    assert!(old.lock().unwrap().is_empty() && current.lock().unwrap().is_empty());
    assert!(!shutdown.status().faulted);
}
