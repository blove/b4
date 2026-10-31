#[allow(dead_code)]
mod startup_preparation_support;

use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use startup_preparation_support::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

async fn retire<S: ApprovalStore + 'static>(
    host: PreparingHttpHost<S>,
    source: Arc<S>,
    inner: Arc<SqliteStore>,
    application: Arc<NativeApplication>,
    directory: &Directory,
    faulted: bool,
) {
    quiesce_with_fault(&host.shutdown_handle(), faulted).await;
    drop(host);
    drop(source);
    retire_source(inner, application, directory);
}
fn progress(outcome: StartupPreparationOutcome) -> StartupProgress {
    match outcome {
        StartupPreparationOutcome::Complete { progress } => progress,
        _ => panic!("expected actual finite preparation Complete: {outcome:?}"),
    }
}
fn incomplete(
    outcome: StartupPreparationOutcome,
    expected: StartupIncompleteReason,
) -> StartupProgress {
    match outcome {
        StartupPreparationOutcome::Incomplete { progress, reason } => {
            assert_eq!(reason, expected);
            progress
        }
        _ => panic!("expected nonserving Incomplete: {outcome:?}"),
    }
}
fn adopted(before: &ApprovalView, turn: TurnId, owner: u64, now: u64) -> ApprovalView {
    let mut expected = before.clone();
    expected.thread.resource_revision += 1;
    let snapshot = &mut expected.execution.snapshot;
    snapshot.revision += 1;
    snapshot.owner_generation = owner;
    snapshot.turn_id = turn.clone();
    snapshot.turn_history.push(turn);
    snapshot.updated_at_ms = now;
    expected
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_automatically_adopts_exact_work_and_completes_before_callback_retirement() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "automatic", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let before_rows = directory.rows();
    let owner = inner.owner_generation();
    let resolve_gate = Gate::new();
    let before_gate = Gate::new();
    let return_gate = Gate::new();
    let callback_gate = Gate::new();
    let _cleanup = [
        Cleanup(resolve_gate.clone()),
        Cleanup(before_gate.clone()),
        Cleanup(return_gate.clone()),
        Cleanup(callback_gate.clone()),
    ];
    let mut wrapper = BoundaryStore::new(inner.clone());
    wrapper.before_recovery = Some(before_gate.clone());
    wrapper.after_commit = Some(return_gate.clone());
    let source = Arc::new(wrapper);
    let callback = Callback::new(Some(callback_gate.clone()));
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        source.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let resolver = Resolver::new(Some(resolve_gate.clone()));
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    resolve_gate.entered().await;
    assert_eq!(directory.rows(), before_rows);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    assert!(host.serving_host().is_none());
    assert_eq!(
        host.begin_preparation(Resolver::new(None), StartupPreparationLimits::default())
            .unwrap_err(),
        StartupStartError::AlreadyStarted
    );
    assert!(matches!(
        handle.wait(std::time::Duration::ZERO).await.unwrap(),
        StartupWaitOutcome::Pending(_)
    ));
    assert_eq!(
        handle
            .wait(MAX_SHUTDOWN_WAIT + std::time::Duration::from_nanos(1))
            .await
            .unwrap_err(),
        StartupWaitError::InvalidTimeout
    );
    drop(handle.wait(WAIT));
    assert_locked(&directory);
    resolve_gate.release();
    before_gate.entered().await;
    assert_eq!(directory.rows(), before_rows);
    assert_eq!(handle.status().progress.admitted, 0);
    let arguments = source.recovery_arguments.lock().unwrap().clone();
    assert_eq!(arguments.len(), 1);
    let (id, fence, plan, mutation) = &arguments[0];
    let request = match mutation {
        ApprovalMutation::Recover(request) => request,
        _ => panic!("fixture observed wrong real mutation"),
    };
    assert_eq!(id, &before.execution.snapshot.execution_id);
    assert_eq!(
        fence.execution,
        ExecutionFence::from(&before.execution.snapshot)
    );
    assert_eq!(fence.resource_revision, before.thread.resource_revision);
    assert_eq!(request.thread_id, before.thread.id);
    assert_eq!(request.thread_generation, before.thread.generation);
    assert_eq!(request.execution_id, before.execution.snapshot.execution_id);
    assert_eq!(
        request.available_definition,
        before.execution.snapshot.definition
    );
    assert_eq!(request.context, approval_context());
    assert!(!before
        .execution
        .snapshot
        .turn_history
        .contains(&request.turn_id));
    let expected_adopted = adopted(
        &before,
        request.turn_id.clone(),
        owner,
        request.facts.now_ms,
    );
    assert_eq!(plan.snapshot, expected_adopted.execution.snapshot);
    assert_eq!(plan.expected_revision, before.execution.snapshot.revision);
    assert!(plan.dispatchable.is_empty());
    before_gate.release();
    return_gate.entered().await;
    assert_eq!(inner.approval_view(id).unwrap().unwrap(), expected_adopted);
    assert_eq!(handle.status().progress.admitted, 0);
    assert!(host.serving_host().is_none());
    assert_locked(&directory);
    return_gate.release();
    callback_gate.entered().await;
    let outcome = finished(&handle).await;
    assert_eq!(
        progress(outcome),
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
    assert!(host.serving_host().is_some());
    let held = inner.approval_view(id).unwrap().unwrap();
    let (invocation, context) = callback.contexts.lock().unwrap()[0].clone();
    assert_eq!(context.thread_id, before.thread.id);
    assert_eq!(context.thread_generation, before.thread.generation);
    assert_eq!(context.origin, origin());
    assert_eq!(
        context.execution_revision,
        before.execution.snapshot.definition.revision
    );
    assert_eq!(context.application_revision, "startup-native-fixture-1");
    assert_eq!(context.policy_revision, "startup-fixture-policy-1");
    let mut expected_held = expected_adopted.clone();
    expected_held.thread.resource_revision += 2;
    let snapshot = &mut expected_held.execution.snapshot;
    snapshot.revision += 2;
    snapshot.status = ExecutionStatus::Running;
    snapshot.updated_at_ms = held.execution.snapshot.updated_at_ms;
    snapshot.attempts_used = 1;
    snapshot.attempt_history.push(invocation.attempt_id.clone());
    snapshot.tasks = vec![TaskRecord {
        spec: before.execution.snapshot.definition.batches[0][0].clone(),
        status: TaskStatus::InFlight,
        input: before.execution.snapshot.values.clone(),
        attempt_id: Some(invocation.attempt_id.clone()),
        result: None,
        failure: None,
        interrupt: None,
        approved: false,
    }];
    assert_eq!(held, expected_held);
    assert_eq!(invocation.execution_id, id.clone());
    assert_eq!(invocation.turn_id, request.turn_id);
    assert_eq!(invocation.owner_generation, owner);
    assert_eq!(invocation.task_id, TaskId::new("finite-task").unwrap());
    assert_eq!(
        invocation.effect_id,
        EffectId::new("finite-effect").unwrap()
    );
    assert_eq!(invocation.input, before.execution.snapshot.values);
    assert!(!callback.returned.load(Ordering::Acquire));
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    assert_locked(&directory);
    assert!(
        matches!(host.shutdown_handle().wait_quiescent(std::time::Duration::ZERO).await.unwrap(),ShutdownOutcome::Incomplete{active_owners,..} if active_owners>0)
    );
    callback_gate.release();
    completed(inner.as_ref(), id).await;
    let final_view = inner.approval_view(id).unwrap().unwrap();
    let mut expected_final = expected_held;
    expected_final.thread.resource_revision += 1;
    let snapshot = &mut expected_final.execution.snapshot;
    snapshot.revision += 1;
    snapshot.updated_at_ms = final_view.execution.snapshot.updated_at_ms;
    snapshot.status = ExecutionStatus::Completed;
    snapshot.batch_index = 1;
    snapshot.tasks[0].status = TaskStatus::Succeeded;
    snapshot.tasks[0].result = Some(TaskResult {
        output: serde_json::json!("startup-finite-value"),
        writes: vec![StateWrite::Append {
            field: "actual-calls".into(),
            items: vec![serde_json::json!("once")],
        }],
    });
    snapshot
        .values
        .insert("actual-calls".into(), serde_json::json!(["once"]));
    snapshot.output = Some(serde_json::Value::Object(snapshot.values.clone()));
    assert_eq!(final_view, expected_final);
    assert!(callback.returned.load(Ordering::Acquire));
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    assert_eq!(resolver.calls.load(Ordering::Acquire), 1);
    drop(resolver);
    let shutdown = host.shutdown_handle();
    retire(host, source, inner, application, &directory, false).await;
    for gate in [resolve_gate, before_gate, return_gate, callback_gate] {
        gate.positive();
    }
    assert_eq!(handle.status().phase, StartupPreparationPhase::Complete);
    assert_eq!(shutdown.status().active_owners, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_preserves_stable_park_and_consumed_once_after_ttl() {
    for mode in [SeedMode::Parked, SeedMode::Approved] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(inner.clone(), "approval", mode, true);
        let inner = reopen(inner, &directory);
        let callback = Callback::new(None);
        let application = application_for(mode_definition(mode), callback.clone());
        let host = HttpHost::new_with_application_preparing(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let handle = host
            .begin_preparation(Resolver::new(None), StartupPreparationLimits::default())
            .unwrap();
        let p = progress(finished(&handle).await);
        assert_eq!(p.admitted, 1);
        assert_eq!(p.candidates, 1);
        if matches!(mode, SeedMode::Parked) {
            assert_eq!(p.parked, 1);
            assert_eq!(callback.calls.load(Ordering::Acquire), 0);
            let after = inner
                .approval_view(&before.execution.snapshot.execution_id)
                .unwrap()
                .unwrap();
            let adoption = RecoveryAdoption {
                thread_id: before.thread.id.clone(),
                thread_generation: before.thread.generation.clone(),
                execution_id: before.execution.snapshot.execution_id.clone(),
                turn_id: after.execution.snapshot.turn_id.clone(),
                execution_fence: ExecutionFence::from(&after.execution.snapshot),
            };
            assert_recovered_park(&before, &after, &adoption);
        } else {
            assert_eq!(p.runnable, 1);
            completed(inner.as_ref(), &before.execution.snapshot.execution_id).await;
            let after = inner
                .approval_view(&before.execution.snapshot.execution_id)
                .unwrap()
                .unwrap();
            assert_eq!(after.grants, before.grants);
            assert_eq!(after.acceptances, before.acceptances);
            assert!(after.execution.snapshot.updated_at_ms > before.grants[0].expires_at_ms);
            assert_eq!(callback.calls.load(Ordering::Acquire), 1);
            assert!(callback.returned.load(Ordering::Acquire));
        }
        retire(host, inner.clone(), inner, application, &directory, false).await;
        assert_eq!(handle.status().phase, StartupPreparationPhase::Complete);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_classifies_real_unsafe_block_but_attempt_limit_stays_closed() {
    for mode in [SeedMode::Unsafe, SeedMode::AttemptLimit] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(inner.clone(), "classification", mode, true);
        let inner = reopen(inner, &directory);
        let callback = Callback::new(None);
        let application = application_for(mode_definition(mode), callback.clone());
        let host = HttpHost::new_with_application_preparing(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let handle = host
            .begin_preparation(Resolver::new(None), StartupPreparationLimits::default())
            .unwrap();
        let outcome = finished(&handle).await;
        if matches!(mode, SeedMode::Unsafe) {
            assert_eq!(progress(outcome).blocked, 1);
            assert!(host.serving_host().is_some());
            assert_eq!(
                inner
                    .execution(&before.execution.snapshot.execution_id)
                    .unwrap()
                    .unwrap()
                    .snapshot
                    .status,
                ExecutionStatus::RecoveryBlocked
            );
        } else {
            let p = incomplete(outcome, StartupIncompleteReason::AttemptLimit);
            assert_eq!(p.admitted, 1);
            assert_eq!(p.runnable, 0);
            assert!(host.serving_host().is_none());
        }
        let after = inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            after.execution.snapshot.attempts_used,
            before.execution.snapshot.attempts_used
        );
        assert_eq!(
            after.execution.snapshot.attempt_history,
            before.execution.snapshot.attempt_history
        );
        assert_eq!(
            after.execution.snapshot.definition,
            before.execution.snapshot.definition
        );
        assert_eq!(after.grants, before.grants);
        assert_eq!(after.acceptances, before.acceptances);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        retire(host, inner.clone(), inner, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_resolver_failures_foreign_actor_and_originless_rows_remain_unchanged() {
    for case in ["unavailable", "unauthorized", "foreign", "originless"] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let _before = seed_named(
            inner.clone(),
            "resolution",
            SeedMode::Accepted,
            case != "originless",
        );
        let inner = reopen(inner, &directory);
        let rows = directory.rows();
        let callback = Callback::new(None);
        let application = application_for(definition(), callback.clone());
        let host = HttpHost::new_with_application_preparing(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let mut actor = principal();
        actor.tenant = "foreign".into();
        let resolver = Arc::new(Resolver {
            calls: AtomicUsize::new(0),
            gate: None,
            result: match case {
                "unavailable" => Err(StartupResolutionError::Unavailable),
                "unauthorized" => Err(StartupResolutionError::Unauthorized),
                "foreign" => Ok(actor),
                _ => Ok(principal()),
            },
            drop_gate: None,
            captured_store: None,
            panic_payload: false,
        });
        let handle = host
            .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
            .unwrap();
        let p = incomplete(
            finished(&handle).await,
            StartupIncompleteReason::ResolutionRefused,
        );
        assert_eq!(p.admitted, 0);
        assert_eq!(directory.rows(), rows);
        assert!(host.serving_host().is_none());
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        assert_eq!(
            resolver.calls.load(Ordering::Acquire),
            usize::from(case != "originless")
        );
        drop(resolver);
        retire(host, inner.clone(), inner, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_renews_policy_and_exact_whole_definition_without_fabricating_block() {
    for case in ["policy", "definition", "route"] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed_named(inner.clone(), "renew", SeedMode::Accepted, true);
        let inner = reopen(inner, &directory);
        let rows = directory.rows();
        let callback = Callback::new(None);
        let mut registered = definition();
        if case == "definition" {
            registered.max_attempts += 1;
        }
        if case == "route" {
            registered.route = RouteKey::new("/other#workflow").unwrap();
        }
        let application = application_for(registered, callback.clone());
        let policy = CountingPolicy::new();
        let gate = Gate::new();
        let _cleanup = Cleanup(gate.clone());
        let resolver = Resolver::new(Some(gate.clone()));
        let host = HttpHost::new_with_application_preparing(
            inner.clone(),
            Arc::new(Auth),
            policy.clone(),
            config_for(application.clone()),
        )
        .unwrap();
        let handle = host
            .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
            .unwrap();
        gate.entered().await;
        assert_eq!(directory.rows(), rows);
        if case == "policy" {
            policy.allowed.store(false, Ordering::Release);
        }
        gate.release();
        let p = incomplete(
            finished(&handle).await,
            StartupIncompleteReason::RecoveryRefused,
        );
        assert_eq!(p.admitted, 0);
        assert_eq!(directory.rows(), rows);
        assert!(host.serving_host().is_none());
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        drop(resolver);
        drop(policy);
        retire(host, inner.clone(), inner, application, &directory, false).await;
        gate.positive();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_stale_view_after_held_resolver_fails_closed_without_rescan() {
    // Deliberate test-only violation of the documented direct-alias covenant.
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "stale", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let resolver = Resolver::new(Some(gate.clone()));
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    gate.entered().await;
    let changed = ApprovalService::new(inner.clone(), Authority)
        .unwrap()
        .recover(
            &before.execution.snapshot.execution_id,
            approval_fence(inner.as_ref(), &before.execution),
            ApprovalRecoveryRequest {
                context: approval_context(),
                thread_id: before.thread.id.clone(),
                thread_generation: before.thread.generation.clone(),
                execution_id: before.execution.snapshot.execution_id.clone(),
                available_definition: definition(),
                turn_id: TurnId::new("external-covenant-violation-turn").unwrap(),
                facts: TransitionFacts { now_ms: 10 },
            },
        )
        .unwrap();
    let rows = directory.rows();
    gate.release();
    let p = incomplete(
        finished(&handle).await,
        StartupIncompleteReason::RecoveryRefused,
    );
    assert_eq!(p.admitted, 0);
    assert_eq!(directory.rows(), rows);
    assert_eq!(
        inner
            .execution(&changed.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        changed
    );
    assert_eq!(resolver.calls.load(Ordering::Acquire), 1);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    drop(resolver);
    retire(host, inner.clone(), inner, application, &directory, false).await;
    gate.positive();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_current_owner_and_discovery_refusal_never_open_serving() {
    for refusal in [false, true] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed_named(inner.clone(), "current", SeedMode::Accepted, true);
        let rows = directory.rows();
        let mut wrapper = BoundaryStore::new(inner.clone());
        wrapper.discover_refused = refusal;
        let source = Arc::new(wrapper);
        let callback = Callback::new(None);
        let application = application_for(definition(), callback.clone());
        let resolver = Resolver::new(None);
        let host = HttpHost::new_with_application_preparing(
            source.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let handle = host
            .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
            .unwrap();
        let p = incomplete(
            finished(&handle).await,
            if refusal {
                StartupIncompleteReason::DiscoveryRefused
            } else {
                StartupIncompleteReason::CurrentOwnerExecution
            },
        );
        assert_eq!(p.admitted, 0);
        assert_eq!(resolver.calls.load(Ordering::Acquire), 0);
        assert_eq!(source.recoveries.load(Ordering::Acquire), 0);
        assert_eq!(directory.rows(), rows);
        assert!(host.serving_host().is_none());
        drop(resolver);
        retire(host, source, inner, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_terminal_prefix_pages_and_exact_caps_are_counted_honestly() {
    for (pages, expected_complete) in [(1, false), (2, true)] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        for number in 0..32 {
            seed_named(
                inner.clone(),
                &format!("{number:03}"),
                SeedMode::Terminal,
                true,
            );
        }
        let candidate = seed_named(inner.clone(), "032", SeedMode::Accepted, true);
        let inner = reopen(inner, &directory);
        let callback = Callback::new(None);
        let application = application_for(definition(), callback.clone());
        let host = HttpHost::new_with_application_preparing(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let resolver = Resolver::new(None);
        let handle = host
            .begin_preparation(
                resolver.clone(),
                StartupPreparationLimits {
                    max_pages: pages,
                    max_recoveries: 1,
                },
            )
            .unwrap();
        let outcome = finished(&handle).await;
        if expected_complete {
            let p = progress(outcome);
            assert_eq!(p.pages, 2);
            assert_eq!(p.scanned_rows, 33);
            assert_eq!(p.candidates, 1);
            assert_eq!(p.admitted, 1);
            completed(inner.as_ref(), &candidate.execution.snapshot.execution_id).await;
            assert_eq!(callback.calls.load(Ordering::Acquire), 1);
        } else {
            let p = incomplete(outcome, StartupIncompleteReason::LimitExceeded);
            assert_eq!(p.pages, 1);
            assert_eq!(p.scanned_rows, 32);
            assert_eq!(p.candidates, 0);
            assert_eq!(p.admitted, 0);
            assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        }
        assert_eq!(
            resolver.calls.load(Ordering::Acquire),
            usize::from(expected_complete)
        );
        drop(resolver);
        retire(host, inner.clone(), inner, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_candidate_cap_does_not_touch_the_next_park() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let first = seed_named(inner.clone(), "000", SeedMode::Parked, true);
    let second = seed_named(inner.clone(), "001", SeedMode::Parked, true);
    let inner = reopen(inner, &directory);
    let callback = Callback::new(None);
    let application = application_for(mode_definition(SeedMode::Parked), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let handle = host
        .begin_preparation(
            Resolver::new(None),
            StartupPreparationLimits {
                max_pages: 1,
                max_recoveries: 1,
            },
        )
        .unwrap();
    let p = incomplete(
        finished(&handle).await,
        StartupIncompleteReason::LimitExceeded,
    );
    assert_eq!(p.candidates, 1);
    assert_eq!(p.admitted, 1);
    assert_eq!(p.parked, 1);
    assert_eq!(
        inner
            .approval_view(&second.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        second
    );
    assert_eq!(
        inner
            .approval_view(&first.execution.snapshot.execution_id)
            .unwrap()
            .unwrap()
            .execution
            .snapshot
            .owner_generation,
        inner.owner_generation()
    );
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    assert!(host.serving_host().is_none());
    retire(host, inner.clone(), inner, application, &directory, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_nonwaiting_capacity_refusal_keeps_earlier_callback_owned() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let first = seed_named(inner.clone(), "000", SeedMode::Accepted, true);
    let second = seed_named(inner.clone(), "001", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let callback = Callback::new(Some(gate.clone()));
    let application = application_for(definition(), callback.clone());
    let mut cfg = config_for(application.clone());
    cfg.limits.max_active_runs = 1;
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        cfg,
    )
    .unwrap();
    let handle = host
        .begin_preparation(Resolver::new(None), StartupPreparationLimits::default())
        .unwrap();
    gate.entered().await;
    let p = incomplete(
        finished(&handle).await,
        StartupIncompleteReason::RecoveryRefused,
    );
    assert_eq!(p.admitted, 1);
    assert_eq!(p.runnable, 1);
    assert_eq!(p.candidates, 2);
    assert_eq!(
        inner
            .approval_view(&second.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        second
    );
    assert!(!callback.returned.load(Ordering::Acquire));
    assert!(host.serving_host().is_none());
    assert_locked(&directory);
    gate.release();
    completed(inner.as_ref(), &first.execution.snapshot.execution_id).await;
    assert!(callback.returned.load(Ordering::Acquire));
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    retire(host, inner.clone(), inner, application, &directory, false).await;
    gate.positive();
    assert_eq!(handle.status().phase, StartupPreparationPhase::Incomplete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_shutdown_cut_blocks_held_resolver_without_adoption_or_activation() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    seed_named(inner.clone(), "shutdown", SeedMode::Accepted, true);
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
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    gate.entered().await;
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    assert!(
        matches!(shutdown.wait_quiescent(std::time::Duration::ZERO).await.unwrap(),ShutdownOutcome::Incomplete{active_owners,..} if active_owners>0)
    );
    assert_locked(&directory);
    gate.release();
    let p = incomplete(finished(&handle).await, StartupIncompleteReason::Shutdown);
    assert_eq!(p.admitted, 0);
    assert_eq!(directory.rows(), rows);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    assert!(host.serving_host().is_none());
    drop(resolver);
    retire(host, inner.clone(), inner, application, &directory, false).await;
    gate.positive();
    assert_eq!(shutdown.status().phase, ShutdownPhase::Draining);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_already_won_recovery_lease_can_finish_after_shutdown_cut() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "lease", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let mut wrapper = BoundaryStore::new(inner.clone());
    wrapper.before_recovery = Some(gate.clone());
    let source = Arc::new(wrapper);
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        source.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let handle = host
        .begin_preparation(Resolver::new(None), StartupPreparationLimits::default())
        .unwrap();
    gate.entered().await;
    host.shutdown_handle().begin_shutdown();
    assert_locked(&directory);
    gate.release();
    let p = incomplete(finished(&handle).await, StartupIncompleteReason::Shutdown);
    assert_eq!(p.admitted, 1);
    completed(inner.as_ref(), &before.execution.snapshot.execution_id).await;
    assert_eq!(source.recoveries.load(Ordering::Acquire), 1);
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    assert!(callback.returned.load(Ordering::Acquire));
    assert!(host.serving_host().is_none());
    retire(host, source, inner, application, &directory, false).await;
    gate.positive();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_handle_and_waiter_drop_do_not_abort_the_owned_resolver() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "observer", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
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
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    gate.entered().await;
    let retained = handle.clone();
    assert!(matches!(
        handle.wait(std::time::Duration::ZERO).await.unwrap(),
        StartupWaitOutcome::Pending(_)
    ));
    let waiter = tokio::spawn(handle.wait(WAIT));
    waiter.abort();
    assert!(tokio::time::timeout(WAIT, waiter)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    drop(handle);
    drop(resolver);
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert_locked(&directory);
    gate.release();
    assert_eq!(progress(finished(&retained).await).admitted, 1);
    completed(inner.as_ref(), &before.execution.snapshot.execution_id).await;
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    retire(host, inner.clone(), inner, application, &directory, false).await;
    gate.positive();
    assert_eq!(retained.status().phase, StartupPreparationPhase::Complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_normal_resolver_capture_cleanup_keeps_owners_and_serving_closed() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    seed_named(inner.clone(), "capture", SeedMode::Parked, true);
    let inner = reopen(inner, &directory);
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let resolver = Arc::new(Resolver {
        calls: AtomicUsize::new(0),
        gate: None,
        result: Ok(principal()),
        drop_gate: Some(gate.clone()),
        captured_store: Some(inner.clone()),
        panic_payload: false,
    });
    let callback = Callback::new(None);
    let application = application_for(mode_definition(SeedMode::Parked), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    drop(resolver);
    gate.entered().await;
    assert!(host.serving_host().is_none());
    assert_eq!(handle.status().phase, StartupPreparationPhase::Preparing);
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert_locked(&directory);
    gate.release();
    assert_eq!(progress(finished(&handle).await).parked, 1);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire(host, inner.clone(), inner, application, &directory, false).await;
    gate.positive();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_opaque_resolver_panic_payload_is_disposed_while_store_remains_owned() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    seed_named(inner.clone(), "payload", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let rows = directory.rows();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let resolver = Arc::new(Resolver {
        calls: AtomicUsize::new(0),
        gate: None,
        result: Ok(principal()),
        drop_gate: Some(gate.clone()),
        captured_store: Some(inner.clone()),
        panic_payload: true,
    });
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    drop(resolver);
    gate.entered().await;
    assert!(host.serving_host().is_none());
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert_locked(&directory);
    gate.release();
    let p = incomplete(finished(&handle).await, StartupIncompleteReason::Faulted);
    assert_eq!(p.admitted, 0);
    assert_eq!(directory.rows(), rows);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire(host, inner.clone(), inner, application, &directory, true).await;
    gate.positive();
    assert_eq!(handle.status().phase, StartupPreparationPhase::Incomplete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_ambiguous_commit_is_faulted_unconfirmed_and_never_retried() {
    for behavior in [
        RecoveryBehavior::CommitThenError,
        RecoveryBehavior::CommitThenPanic,
        RecoveryBehavior::FailViewAfterReceipt,
    ] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(inner.clone(), "ambiguous", SeedMode::Accepted, true);
        let inner = reopen(inner, &directory);
        let mut wrapper = BoundaryStore::new(inner.clone());
        wrapper.behavior = behavior;
        let source = Arc::new(wrapper);
        let callback = Callback::new(None);
        let application = application_for(definition(), callback.clone());
        let host = HttpHost::new_with_application_preparing(
            source.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let handle = host
            .begin_preparation(Resolver::new(None), StartupPreparationLimits::default())
            .unwrap();
        let p = incomplete(finished(&handle).await, StartupIncompleteReason::Faulted);
        assert_eq!(
            p.admitted,
            usize::from(matches!(behavior, RecoveryBehavior::FailViewAfterReceipt))
        );
        assert_eq!(p.runnable, 0);
        assert_eq!(source.recoveries.load(Ordering::Acquire), 1);
        let after = inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            after.execution.snapshot.owner_generation,
            inner.owner_generation()
        );
        assert_eq!(
            after.execution.snapshot.revision,
            before.execution.snapshot.revision + 1
        );
        assert_eq!(
            after.thread.resource_revision,
            before.thread.resource_revision + 1
        );
        assert_eq!(after.grants, before.grants);
        assert_eq!(after.acceptances, before.acceptances);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        assert!(host.serving_host().is_none());
        retire(host, source, inner, application, &directory, true).await;
    }
}

#[test]
fn preparing_construction_requires_actual_durability_and_required_approval_mode() {
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let memory = Arc::new(MemoryStore::new());
    assert!(HttpHost::new_with_application_preparing(
        memory,
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone())
    )
    .is_err());
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let mut cfg = config_for(application.clone());
    cfg.approval.service_config = ApprovalServiceConfig::VolatileFixture;
    assert!(HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        cfg
    )
    .is_err());
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire_source(inner, application, &directory);
}

#[test]
fn preparation_without_existing_executor_starts_no_job_and_changes_no_source() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let rows = directory.rows();
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    assert_eq!(
        host.begin_preparation(Resolver::new(None), StartupPreparationLimits::default())
            .unwrap_err(),
        StartupStartError::ExecutorUnavailable
    );
    assert_eq!(
        host.preparation_status().phase,
        StartupPreparationPhase::NotStarted
    );
    assert_eq!(directory.rows(), rows);
    assert_eq!(host.shutdown_handle().status().active_owners, 0);
    assert!(host.serving_host().is_none());
    drop(host);
    retire_source(inner, application, &directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_invalid_limits_leave_not_started_and_empty_valid_source_can_complete() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let rows = directory.rows();
    for limits in [
        StartupPreparationLimits {
            max_pages: 0,
            max_recoveries: 1,
        },
        StartupPreparationLimits {
            max_pages: 65,
            max_recoveries: 1,
        },
        StartupPreparationLimits {
            max_pages: 1,
            max_recoveries: 0,
        },
        StartupPreparationLimits {
            max_pages: 1,
            max_recoveries: 257,
        },
    ] {
        assert_eq!(
            host.begin_preparation(Resolver::new(None), limits)
                .unwrap_err(),
            StartupStartError::InvalidLimits
        );
        assert_eq!(
            host.preparation_status().phase,
            StartupPreparationPhase::NotStarted
        );
        assert_eq!(host.shutdown_handle().status().active_owners, 0);
        assert_eq!(directory.rows(), rows);
    }
    let handle = host
        .begin_preparation(
            Resolver::new(None),
            StartupPreparationLimits {
                max_pages: 1,
                max_recoveries: 1,
            },
        )
        .unwrap();
    assert_eq!(
        progress(finished(&handle).await),
        StartupProgress {
            pages: 1,
            ..StartupProgress::default()
        }
    );
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire(host, inner.clone(), inner, application, &directory, false).await;
    assert_eq!(handle.status().phase, StartupPreparationPhase::Complete);
}

struct UnreadBody {
    polls: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl futures_core::Stream for UnreadBody {
    type Item = Result<axum::body::Bytes, std::io::Error>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::AcqRel);
        std::task::Poll::Pending
    }
}
impl Drop for UnreadBody {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_router_rejects_before_polling_the_actual_supplied_request_body() {
    use tower::ServiceExt;
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let source = Arc::new(BoundaryStore::new(inner.clone()));
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let auth = Arc::new(CountingAuth(AtomicUsize::new(0)));
    let policy = CountingPolicy::new();
    let host = HttpHost::new_with_application_preparing(
        source.clone(),
        auth.clone(),
        policy.clone(),
        config_for(application.clone()),
    )
    .unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let probes = source.probes.load(Ordering::Acquire);
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/threads")
        .header("authorization", "Bearer startup-fixture")
        .body(axum::body::Body::from_stream(UnreadBody {
            polls: polls.clone(),
            dropped: dropped.clone(),
        }))
        .unwrap();
    let response = tokio::time::timeout(WAIT, host.router().oneshot(request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    drop(response);
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(auth.0.load(Ordering::Acquire), 0);
    assert_eq!(policy.calls.load(Ordering::Acquire), 0);
    assert_eq!(source.probes.load(Ordering::Acquire), probes);
    retire(host, source, inner, application, &directory, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_maximum_candidate_cap_accepts_exact_256_and_refuses_the_257th() {
    for count in [256, 257] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let mut final_original = None;
        for number in 0..count {
            final_original = Some(seed_named(
                inner.clone(),
                &format!("{number:03}"),
                SeedMode::Parked,
                true,
            ));
        }
        let inner = reopen(inner, &directory);
        let callback = Callback::new(None);
        let application = application_for(mode_definition(SeedMode::Parked), callback.clone());
        let resolver = Resolver::new(None);
        let mut cfg = config_for(application.clone());
        cfg.limits.max_active_runs = 256;
        let host = HttpHost::new_with_application_preparing(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            cfg,
        )
        .unwrap();
        let handle = host
            .begin_preparation(
                resolver.clone(),
                StartupPreparationLimits {
                    max_pages: 9,
                    max_recoveries: 256,
                },
            )
            .unwrap();
        let outcome = finished(&handle).await;
        let p = if count == 256 {
            progress(outcome)
        } else {
            incomplete(outcome, StartupIncompleteReason::LimitExceeded)
        };
        assert_eq!(p.pages, if count == 256 { 8 } else { 9 });
        assert_eq!(p.scanned_rows, count);
        assert_eq!(p.candidates, 256);
        assert_eq!(p.admitted, 256);
        assert_eq!(p.parked, 256);
        assert_eq!(p.runnable, 0);
        assert_eq!(resolver.calls.load(Ordering::Acquire), 256);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        if count == 257 {
            let original = final_original.unwrap();
            assert_eq!(
                inner
                    .approval_view(&original.execution.snapshot.execution_id)
                    .unwrap()
                    .unwrap(),
                original
            );
            assert!(host.serving_host().is_none());
        }
        drop(resolver);
        retire(host, inner.clone(), inner, application, &directory, false).await;
    }
}

struct ReplacingResolver {
    store: Arc<SqliteStore>,
    first: Arc<Gate>,
    replacement: Arc<Gate>,
}
impl StartupPrincipalResolver for ReplacingResolver {
    fn resolve(&self, stored: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        assert_eq!(stored, &origin());
        std::panic::panic_any(FirstPayload {
            store: self.store.clone(),
            first: self.first.clone(),
            replacement: self.replacement.clone(),
        });
    }
}
struct FirstPayload {
    store: Arc<SqliteStore>,
    first: Arc<Gate>,
    replacement: Arc<Gate>,
}
impl Drop for FirstPayload {
    fn drop(&mut self) {
        assert!(
            self.first.hold(),
            "first payload gate expired or cleanup released"
        );
        // A destructor panic is caught while disposing an already-caught
        // payload, not deliberately raised during active stack unwinding.
        std::panic::panic_any(ReplacementPayload {
            store: self.store.clone(),
            gate: self.replacement.clone(),
        });
    }
}
struct ReplacementPayload {
    store: Arc<SqliteStore>,
    gate: Arc<Gate>,
}
impl Drop for ReplacementPayload {
    fn drop(&mut self) {
        let _ = &self.store;
        assert!(
            self.gate.hold(),
            "replacement payload gate expired or cleanup released"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_replacement_panic_payload_cleanup_remains_counted_until_actual_return() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    seed_named(inner.clone(), "replacement", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let rows = directory.rows();
    let first = Gate::new();
    let replacement = Gate::new();
    let _cleanup = [Cleanup(first.clone()), Cleanup(replacement.clone())];
    let resolver = Arc::new(ReplacingResolver {
        store: inner.clone(),
        first: first.clone(),
        replacement: replacement.clone(),
    });
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application_preparing(
        inner.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let handle = host
        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
        .unwrap();
    drop(resolver);
    first.entered().await;
    assert!(host.shutdown_handle().status().faulted);
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert!(host.serving_host().is_none());
    assert_locked(&directory);
    first.release();
    replacement.entered().await;
    assert!(host.shutdown_handle().status().faulted);
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert!(host.serving_host().is_none());
    assert_locked(&directory);
    replacement.release();
    let p = incomplete(finished(&handle).await, StartupIncompleteReason::Faulted);
    assert_eq!(p.admitted, 0);
    assert_eq!(directory.rows(), rows);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire(host, inner.clone(), inner, application, &directory, true).await;
    first.positive();
    replacement.positive();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_actual_cancelled_and_failed_adoptions_increment_terminal_only() {
    for (mode, status) in [
        (SeedMode::Cancelling, ExecutionStatus::Cancelled),
        (SeedMode::FailureDrain, ExecutionStatus::Failed),
    ] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(inner.clone(), "terminal-count", mode, true);
        let inner = reopen(inner, &directory);
        let owner = inner.owner_generation();
        let callback = Callback::new(None);
        let application = application_for(mode_definition(mode), callback.clone());
        let host = HttpHost::new_with_application_preparing(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let resolver = Resolver::new(None);
        let handle = host
            .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
            .unwrap();
        assert_eq!(
            progress(finished(&handle).await),
            StartupProgress {
                pages: 1,
                scanned_rows: 1,
                candidates: 1,
                admitted: 1,
                terminal: 1,
                ..StartupProgress::default()
            }
        );
        assert!(host.serving_host().is_some());
        let after = inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert!(!before
            .execution
            .snapshot
            .turn_history
            .contains(&after.execution.snapshot.turn_id));
        assert!(after.execution.snapshot.updated_at_ms >= before.execution.snapshot.updated_at_ms);
        let mut expected = adopted(
            &before,
            after.execution.snapshot.turn_id.clone(),
            owner,
            after.execution.snapshot.updated_at_ms,
        );
        expected.execution.snapshot.status = status;
        for task in &mut expected.execution.snapshot.tasks {
            if task.status == TaskStatus::InFlight {
                task.status = TaskStatus::Cancelled;
            }
        }
        // The existing Recover transition settles these legal prior in-flight
        // cases without reducing values, refunding attempts or dispatching.
        assert_eq!(after, expected);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        assert_eq!(resolver.calls.load(Ordering::Acquire), 1);
        drop(resolver);
        retire(host, inner.clone(), inner, application, &directory, false).await;
        assert_eq!(handle.status().phase, StartupPreparationPhase::Complete);
    }
}
