#[allow(dead_code)]
mod startup_preparation_support;

use b4_contract::*;
use b4_http::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use startup_preparation_support::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_recovery_adopts_prior_owner_without_response() {
    let directory = Directory::new();
    let original = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let first_owner = original.owner_generation();
    let initial = seed_accepted(original.as_ref());
    let original_weak = Arc::downgrade(&original);
    drop(original);
    assert!(original_weak.upgrade().is_none());

    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let owner = store.owner_generation();
    assert_eq!(owner, first_owner + 1);
    let view = store.thread_view(&initial.thread.id).unwrap().unwrap();
    assert_eq!(view, initial);
    let target = RecoveryTarget::from_view(&view).unwrap();
    assert_eq!(target.thread_id, view.thread.id);
    assert_eq!(target.thread_generation, view.thread.generation);
    assert_eq!(
        target.fence.execution,
        ExecutionFence::from(&view.execution.as_ref().unwrap().snapshot)
    );
    assert_eq!(
        target.fence.resource_revision,
        view.thread.resource_revision
    );
    let expected_execution = target.execution_id.clone();
    let expected_revision = target.fence.execution.revision + 1;
    let calls = Arc::new(AtomicUsize::new(0));
    let application = application(calls.clone());
    let application_weak = Arc::downgrade(&application);
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config(application.clone()),
    )
    .unwrap();
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    let observed = tokio::time::timeout(WAIT, host.admit_recovery(principal(), target))
        .await
        .expect("typed recovery admission remained finite");

    // A later GREEN run must finish the real finite callback before shutdown;
    // the staged refusal has no producer to retire. Neither branch aborts work.
    if observed.is_ok() {
        tokio::time::timeout(WAIT, async {
            loop {
                let current = store.execution(&expected_execution).unwrap().unwrap();
                if current.snapshot.status == ExecutionStatus::Completed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("finite native execution actually completed");
    }
    let store_weak = Arc::downgrade(&store);
    let shutdown = host.shutdown_handle();
    quiesce(&shutdown).await;
    drop(host);
    drop(application);
    drop(store);
    assert!(application_weak.upgrade().is_none());
    assert!(store_weak.upgrade().is_none());
    let reopened = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(reopened.owner_generation(), owner + 1);
    drop(reopened);

    // Feature expectations come after positive physical ownership retirement.
    let admission =
        observed.expect("expected typed recovery adoption, not the staged Unsupported refusal");
    assert_eq!(admission.disposition, RecoveryDisposition::Runnable);
    assert_eq!(admission.adoption.thread_id, initial.thread.id);
    assert_eq!(
        admission.adoption.thread_generation,
        initial.thread.generation
    );
    assert_eq!(admission.adoption.execution_id, expected_execution);
    assert_eq!(admission.adoption.execution_fence.owner_generation, owner);
    assert_eq!(
        admission.adoption.execution_fence.revision,
        expected_revision
    );
    assert!(!initial
        .execution
        .unwrap()
        .snapshot
        .turn_history
        .contains(&admission.adoption.turn_id));
    assert_eq!(calls.load(Ordering::Acquire), 1);
}

async fn admit<S: Store + 'static>(
    host: &HttpHost<S>,
    target: RecoveryTarget,
) -> Result<RecoveryAdmission, RecoveryAdmissionError> {
    tokio::time::timeout(WAIT, host.admit_recovery(principal(), target))
        .await
        .expect("typed admission remained finite")
}
async fn retire<S: ApprovalStore + 'static>(
    host: HttpHost<S>,
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
fn rejection(error: RecoveryAdmissionError, reason: RecoveryRefusal) {
    assert_eq!(error.reason(), reason);
    assert!(error.adoption().is_none());
    assert_eq!(format!("{error:?}"), "RecoveryAdmissionError");
    assert!(std::error::Error::source(&error).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_recovery_preserves_exact_park_and_original_grant_without_callback() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(store.clone(), "park", SeedMode::Parked, true);
    let store = reopen(store, &directory);
    let callback = Callback::new(None);
    let application = application_for(mode_definition(SeedMode::Parked), callback.clone());
    let source = Arc::new(BoundaryStore::new(store.clone()));
    let host = HttpHost::new_with_application(
        source.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let admission = admit(&host, target(store.as_ref(), &before)).await.unwrap();
    assert_eq!(admission.disposition, RecoveryDisposition::Parked);
    let after = store
        .approval_view(&before.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_recovered_park(&before, &after, &admission.adoption);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    assert_eq!(
        source.approval_reads.load(Ordering::Acquire),
        1,
        "detached park must not read grants again for presentation"
    );
    assert_locked(&directory);
    retire(host, source, store, application, &directory, false).await;
    // The sensitive bounded DTO survives the actual physical writer successor.
    assert_eq!(
        admission.adoption.execution_id,
        before.execution.snapshot.execution_id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_accepted_once_dispatches_after_original_ttl_without_remint_or_refund() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(store.clone(), "approved", SeedMode::Approved, true);
    assert!(before.grants[0].consumed);
    assert_eq!(before.grants[0].expires_at_ms, 5);
    assert_eq!(before.acceptances[0].accepted_at_ms, 4);
    let store = reopen(store, &directory);
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let callback = Callback::new(Some(gate.clone()));
    let application = application_for(mode_definition(SeedMode::Approved), callback.clone());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let admission = admit(&host, target(store.as_ref(), &before)).await.unwrap();
    assert_eq!(admission.disposition, RecoveryDisposition::Runnable);
    gate.entered().await;
    let held = store
        .approval_view(&before.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(held.grants, before.grants);
    assert_eq!(held.acceptances, before.acceptances);
    assert_eq!(held.execution.snapshot.status, ExecutionStatus::Running);
    assert_eq!(
        held.execution.snapshot.tasks[0].status,
        TaskStatus::InFlight
    );
    assert!(held.execution.snapshot.updated_at_ms > before.grants[0].expires_at_ms);
    assert_eq!(held.execution.snapshot.attempts_used, 1);
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    assert_locked(&directory);
    gate.release();
    completed(store.as_ref(), &before.execution.snapshot.execution_id).await;
    let final_view = store
        .approval_view(&before.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(final_view.grants, before.grants);
    assert_eq!(final_view.acceptances, before.acceptances);
    assert_eq!(
        final_view.execution.snapshot.values["retained"],
        before.execution.snapshot.values["retained"]
    );
    assert_eq!(
        final_view.execution.snapshot.values["actual-calls"],
        serde_json::json!(["once"])
    );
    assert_eq!(
        final_view.execution.snapshot.tasks[0]
            .result
            .as_ref()
            .unwrap()
            .output,
        serde_json::json!("startup-finite-value")
    );
    assert!(callback.returned.load(Ordering::Acquire));
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    gate.positive();
    retire(host, store.clone(), store, application, &directory, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_unsafe_and_reconcilable_adopt_real_block_without_callback() {
    for mode in [SeedMode::Unsafe, SeedMode::Reconcilable] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(store.clone(), "unsafe", mode, true);
        let store = reopen(store, &directory);
        let callback = Callback::new(None);
        let application = application_for(mode_definition(mode), callback.clone());
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let admission = admit(&host, target(store.as_ref(), &before)).await.unwrap();
        assert_eq!(admission.disposition, RecoveryDisposition::Blocked);
        let after = store
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        let mut expected = before.clone();
        expected.thread.resource_revision += 1;
        expected.execution.snapshot.revision += 1;
        expected.execution.snapshot.owner_generation = store.owner_generation();
        expected.execution.snapshot.turn_id = admission.adoption.turn_id.clone();
        expected
            .execution
            .snapshot
            .turn_history
            .push(admission.adoption.turn_id.clone());
        expected.execution.snapshot.updated_at_ms = after.execution.snapshot.updated_at_ms;
        expected.execution.snapshot.status = ExecutionStatus::RecoveryBlocked;
        expected.execution.snapshot.failure = Some("effect_outcome_unknown".into());
        expected.execution.snapshot.tasks[0].status = TaskStatus::OutcomeUnknown;
        assert_eq!(after, expected);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        assert_locked(&directory);
        retire(host, store.clone(), store, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_attempt_limit_adopts_without_resetting_attempt_budget() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(store.clone(), "budget", SeedMode::AttemptLimit, true);
    let store = reopen(store, &directory);
    let callback = Callback::new(None);
    let application = application_for(mode_definition(SeedMode::AttemptLimit), callback.clone());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let admission = admit(&host, target(store.as_ref(), &before)).await.unwrap();
    assert_eq!(admission.disposition, RecoveryDisposition::AttemptLimit);
    let after = store
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
    assert_eq!(after.execution.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(after.execution.snapshot.status, ExecutionStatus::Running);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire(host, store.clone(), store, application, &directory, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_actual_cancellation_and_failure_drain_become_terminal_dispositions() {
    for (mode, disposition, status) in [
        (
            SeedMode::Cancelling,
            RecoveryDisposition::Cancelled,
            ExecutionStatus::Cancelled,
        ),
        (
            SeedMode::FailureDrain,
            RecoveryDisposition::Failed,
            ExecutionStatus::Failed,
        ),
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(store.clone(), "terminal", mode, true);
        let store = reopen(store, &directory);
        let callback = Callback::new(None);
        let application = application_for(mode_definition(mode), callback.clone());
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let admission = admit(&host, target(store.as_ref(), &before)).await.unwrap();
        assert_eq!(admission.disposition, disposition);
        let after = store
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(after.execution.snapshot.status, status);
        assert_eq!(
            after.execution.snapshot.attempt_history,
            before.execution.snapshot.attempt_history
        );
        assert_eq!(
            after.execution.snapshot.attempts_used,
            before.execution.snapshot.attempts_used
        );
        assert_eq!(after.grants, before.grants);
        assert_eq!(after.acceptances, before.acceptances);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        retire(host, store.clone(), store, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_authority_definition_and_every_captured_fence_refuse_before_adoption() {
    for case in [
        "foreign",
        "policy",
        "definition",
        "route",
        "generation",
        "resource",
        "revision",
        "owner",
        "execution",
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(store.clone(), "refusal", SeedMode::Accepted, true);
        let store = reopen(store, &directory);
        let rows = directory.rows();
        let callback = Callback::new(None);
        let mut registered = definition();
        if case == "definition" {
            registered.max_attempts += 1;
        }
        if case == "route" {
            registered.route = RouteKey::new("/foreign#workflow").unwrap();
        }
        let application = application_for(registered, callback.clone());
        let policy = CountingPolicy::new();
        if case == "policy" {
            policy.allowed.store(false, Ordering::Release);
        }
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            policy,
            config_for(application.clone()),
        )
        .unwrap();
        let mut target = target(store.as_ref(), &before);
        let mut actor = principal();
        match case {
            "foreign" => actor.tenant = "foreign-tenant".into(),
            "generation" => {
                target.thread_generation = ThreadGeneration::new("0".repeat(64)).unwrap()
            }
            "resource" => target.fence.resource_revision += 1,
            "revision" => target.fence.execution.revision += 1,
            "owner" => target.fence.execution.owner_generation += 1,
            "execution" => target.execution_id = ExecutionId::new("foreign-execution").unwrap(),
            _ => {}
        }
        let error = tokio::time::timeout(WAIT, host.admit_recovery(actor, target))
            .await
            .unwrap()
            .unwrap_err();
        rejection(
            error,
            match case {
                "foreign" | "policy" => RecoveryRefusal::ResourceUnavailable,
                "definition" | "route" => RecoveryRefusal::DefinitionUnavailable,
                _ => RecoveryRefusal::ResourceChanged,
            },
        );
        assert_eq!(directory.rows(), rows);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        retire(host, store.clone(), store, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_current_owner_and_terminal_targets_are_not_recovered() {
    for mode in [SeedMode::Accepted, SeedMode::Terminal] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(store.clone(), "current", mode, true);
        let store = if matches!(mode, SeedMode::Terminal) {
            reopen(store, &directory)
        } else {
            store
        };
        let rows = directory.rows();
        let callback = Callback::new(None);
        let application = application_for(mode_definition(mode), callback.clone());
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        rejection(
            admit(&host, target(store.as_ref(), &before))
                .await
                .unwrap_err(),
            RecoveryRefusal::RecoveryNotNeeded,
        );
        assert_eq!(directory.rows(), rows);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        retire(host, store.clone(), store, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_real_journal_trigger_failure_proves_full_rollback_and_static_fault() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(store.clone(), "rollback", SeedMode::Accepted, true);
    let store = reopen(store, &directory);
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    directory.connection().execute_batch("CREATE TRIGGER reject_fixture_recovery BEFORE INSERT ON journal BEGIN SELECT RAISE(ABORT,'private-sql-trigger-text'); END;").unwrap();
    let rows = directory.rows();
    let error = admit(&host, target(store.as_ref(), &before))
        .await
        .unwrap_err();
    rejection(error, RecoveryRefusal::Unavailable);
    assert_eq!(directory.rows(), rows);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    // Restore the original schema after the independently observed rollback;
    // the temporary fault trigger itself would prevent a real writer reopen.
    directory
        .connection()
        .execute_batch("DROP TRIGGER reject_fixture_recovery;")
        .unwrap();
    retire(host, store.clone(), store, application, &directory, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_commit_then_error_or_panic_has_no_confirmed_receipt_but_actual_durable_mutation() {
    for behavior in [
        RecoveryBehavior::CommitThenError,
        RecoveryBehavior::CommitThenPanic,
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
        let host = HttpHost::new_with_application(
            source.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let error = admit(&host, target(inner.as_ref(), &before))
            .await
            .unwrap_err();
        rejection(error, RecoveryRefusal::Unavailable);
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
        assert_ne!(
            after.execution.snapshot.turn_id,
            before.execution.snapshot.turn_id
        );
        assert_eq!(
            after.thread.resource_revision,
            before.thread.resource_revision + 1
        );
        assert_eq!(after.grants, before.grants);
        assert_eq!(after.acceptances, before.acceptances);
        assert_eq!(source.recoveries.load(Ordering::Acquire), 1);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        retire(host, source, inner, application, &directory, true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_failure_after_returned_adoption_keeps_confirmed_receipt() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "receipt", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let mut wrapper = BoundaryStore::new(inner.clone());
    wrapper.behavior = RecoveryBehavior::FailViewAfterReceipt;
    let source = Arc::new(wrapper);
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application(
        source.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let error = admit(&host, target(inner.as_ref(), &before))
        .await
        .unwrap_err();
    assert_eq!(error.reason(), RecoveryRefusal::Unavailable);
    let adoption = error.adoption().unwrap();
    let actual = inner
        .execution(&before.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        adoption.execution_fence,
        ExecutionFence::from(&actual.snapshot)
    );
    assert_eq!(adoption.turn_id, actual.snapshot.turn_id);
    assert_eq!(adoption.thread_generation, before.thread.generation);
    assert_eq!(source.recoveries.load(Ordering::Acquire), 1);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire(host, source, inner, application, &directory, true).await;
    assert_eq!(
        error.adoption().unwrap().execution_id,
        before.execution.snapshot.execution_id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_caller_detach_preserves_preflight_owner_until_actual_return() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(store.clone(), "detach", SeedMode::Accepted, true);
    let store = reopen(store, &directory);
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let policy = Arc::new(CountingPolicy {
        calls: AtomicUsize::new(0),
        allowed: std::sync::atomic::AtomicBool::new(true),
        recover_gate: Some(gate.clone()),
    });
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        policy.clone(),
        config_for(application.clone()),
    )
    .unwrap();
    let caller_host = host.clone();
    let original_target = target(store.as_ref(), &before);
    let caller = tokio::spawn(async move {
        caller_host
            .admit_recovery(principal(), original_target)
            .await
    });
    gate.entered().await;
    caller.abort();
    assert!(tokio::time::timeout(WAIT, caller)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert_locked(&directory);
    gate.release();
    quiesce(&host.shutdown_handle()).await;
    assert_eq!(
        store
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    gate.positive();
    drop(policy);
    retire(host, store.clone(), store, application, &directory, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_caller_detach_after_real_commit_keeps_owned_driver_running() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "committed-detach", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let mut wrapper = BoundaryStore::new(inner.clone());
    wrapper.after_commit = Some(gate.clone());
    let source = Arc::new(wrapper);
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application(
        source.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let caller_host = host.clone();
    let original_target = target(inner.as_ref(), &before);
    let caller = tokio::spawn(async move {
        caller_host
            .admit_recovery(principal(), original_target)
            .await
    });
    gate.entered().await;
    let committed = inner
        .execution(&before.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        committed.snapshot.owner_generation,
        inner.owner_generation()
    );
    assert_eq!(
        committed.snapshot.revision,
        before.execution.snapshot.revision + 1
    );
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    caller.abort();
    assert!(tokio::time::timeout(WAIT, caller)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert_locked(&directory);
    gate.release();
    completed(inner.as_ref(), &before.execution.snapshot.execution_id).await;
    assert_eq!(source.recoveries.load(Ordering::Acquire), 1);
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    assert!(callback.returned.load(Ordering::Acquire));
    gate.positive();
    retire(host, source, inner, application, &directory, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_unpolled_call_starts_no_owner_preflight_or_store_work() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "unpolled", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let source = Arc::new(BoundaryStore::new(inner.clone()));
    let callback = Callback::new(None);
    let application = application_for(definition(), callback.clone());
    let host = HttpHost::new_with_application(
        source.clone(),
        Arc::new(Auth),
        Arc::new(Policy),
        config_for(application.clone()),
    )
    .unwrap();
    let t = target(inner.as_ref(), &before);
    let probes = source.probes.load(Ordering::Acquire);
    let rows = directory.rows();
    drop(host.admit_recovery(principal(), t));
    assert_eq!(source.probes.load(Ordering::Acquire), probes);
    assert_eq!(host.shutdown_handle().status().active_owners, 0);
    assert_eq!(directory.rows(), rows);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    retire(host, source, inner, application, &directory, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_duplicate_prior_target_retains_real_same_thread_claim_before_commit() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = seed_named(inner.clone(), "claim", SeedMode::Accepted, true);
    let inner = reopen(inner, &directory);
    let rows = directory.rows();
    let gate = Gate::new();
    let callback_gate = Gate::new();
    let _cleanup = [Cleanup(gate.clone()), Cleanup(callback_gate.clone())];
    let mut wrapper = BoundaryStore::new(inner.clone());
    wrapper.before_recovery = Some(gate.clone());
    let source = Arc::new(wrapper);
    let callback = Callback::new(Some(callback_gate.clone()));
    let application = application_for(definition(), callback.clone());
    let mut cfg = config_for(application.clone());
    cfg.limits.max_active_runs = 2;
    let host =
        HttpHost::new_with_application(source.clone(), Arc::new(Auth), Arc::new(Policy), cfg)
            .unwrap();
    let first_host = host.clone();
    let t = target(inner.as_ref(), &before);
    let second_target = t.clone();
    let first = tokio::spawn(async move { first_host.admit_recovery(principal(), t).await });
    gate.entered().await;
    rejection(
        admit(&host, second_target).await.unwrap_err(),
        RecoveryRefusal::ThreadBusy,
    );
    assert_eq!(directory.rows(), rows);
    assert_eq!(source.recoveries.load(Ordering::Acquire), 1);
    assert_eq!(callback.calls.load(Ordering::Acquire), 0);
    assert_locked(&directory);
    gate.release();
    let result = tokio::time::timeout(WAIT, first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.disposition, RecoveryDisposition::Runnable);
    callback_gate.entered().await;
    assert_locked(&directory);
    callback_gate.release();
    completed(inner.as_ref(), &before.execution.snapshot.execution_id).await;
    assert!(callback.returned.load(Ordering::Acquire));
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    retire(host, source, inner, application, &directory, false).await;
    gate.positive();
    callback_gate.positive();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_response_adapter_preserves_real_block_and_attempt_limit_conflicts() {
    for (mode, code) in [
        (SeedMode::Unsafe, "recovery_blocked"),
        (SeedMode::AttemptLimit, "recovery_attempt_limit"),
    ] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(inner.clone(), "adapter", mode, true);
        let inner = reopen(inner, &directory);
        let callback = Callback::new(None);
        let application = application_for(mode_definition(mode), callback.clone());
        let host = HttpHost::new_with_application(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let response = tokio::time::timeout(
            WAIT,
            host.recover_execution(principal(), target(inner.as_ref(), &before)),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
        let bytes = tokio::time::timeout(WAIT, axum::body::to_bytes(response.into_body(), 65_536))
            .await
            .unwrap()
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["details"]["code"], code);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        retire(host, inner.clone(), inner, application, &directory, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_post_receipt_thread_and_active_relationship_mismatch_faults_without_dispatch() {
    for behavior in [
        RecoveryBehavior::ForeignThreadAfterReceipt,
        RecoveryBehavior::MissingActiveAfterReceipt,
    ] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(inner.clone(), "relationship", SeedMode::Accepted, true);
        let inner = reopen(inner, &directory);
        let mut wrapper = BoundaryStore::new(inner.clone());
        wrapper.behavior = behavior;
        let source = Arc::new(wrapper);
        let callback = Callback::new(None);
        let application = application_for(definition(), callback.clone());
        let host = HttpHost::new_with_application(
            source.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let error = admit(&host, target(inner.as_ref(), &before))
            .await
            .unwrap_err();
        assert_eq!(error.reason(), RecoveryRefusal::Unavailable);
        let actual = inner
            .execution(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            error.adoption().unwrap().execution_fence,
            ExecutionFence::from(&actual.snapshot)
        );
        assert_eq!(error.adoption().unwrap().turn_id, actual.snapshot.turn_id);
        assert_eq!(source.recoveries.load(Ordering::Acquire), 1);
        assert_eq!(callback.calls.load(Ordering::Acquire), 0);
        retire(host, source, inner, application, &directory, true).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_runnable_response_and_body_drop_keep_actual_callback_owned() {
    for drop_body_only in [false, true] {
        let directory = Directory::new();
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let before = seed_named(inner.clone(), "legacy-drop", SeedMode::Accepted, true);
        let inner = reopen(inner, &directory);
        let gate = Gate::new();
        let _cleanup = Cleanup(gate.clone());
        let callback = Callback::new(Some(gate.clone()));
        let application = application_for(definition(), callback.clone());
        let host = HttpHost::new_with_application(
            inner.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            config_for(application.clone()),
        )
        .unwrap();
        let response = tokio::time::timeout(
            WAIT,
            host.recover_execution(principal(), target(inner.as_ref(), &before)),
        )
        .await
        .expect("legacy response admission remained finite");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert!(response.headers()[axum::http::header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream"));
        gate.entered().await;
        let held = inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(held.execution.snapshot.status, ExecutionStatus::Running);
        assert_eq!(
            held.execution.snapshot.owner_generation,
            inner.owner_generation()
        );
        assert_eq!(
            held.execution.snapshot.definition,
            before.execution.snapshot.definition
        );
        assert_eq!(
            held.execution.snapshot.values,
            before.execution.snapshot.values
        );
        assert_eq!(
            held.execution.snapshot.tasks[0].status,
            TaskStatus::InFlight
        );
        assert_eq!(held.execution.snapshot.attempts_used, 1);
        assert_eq!(held.grants, before.grants);
        assert_eq!(held.acceptances, before.acceptances);
        if drop_body_only {
            drop(response.into_body());
        } else {
            drop(response);
        }
        assert_eq!(
            inner
                .approval_view(&before.execution.snapshot.execution_id)
                .unwrap()
                .unwrap(),
            held
        );
        assert_eq!(callback.calls.load(Ordering::Acquire), 1);
        assert!(!callback.returned.load(Ordering::Acquire));
        let shutdown = host.shutdown_handle();
        assert!(
            matches!(shutdown.wait_quiescent(std::time::Duration::ZERO).await.unwrap(), ShutdownOutcome::Incomplete { active_owners, .. } if active_owners > 0)
        );
        assert_locked(&directory);
        gate.release();
        completed(inner.as_ref(), &before.execution.snapshot.execution_id).await;
        assert_eq!(callback.calls.load(Ordering::Acquire), 1);
        assert!(callback.returned.load(Ordering::Acquire));
        let after = inner
            .approval_view(&before.execution.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(after.grants, before.grants);
        assert_eq!(after.acceptances, before.acceptances);
        let expected_result = TaskResult {
            output: serde_json::json!("startup-finite-value"),
            writes: vec![StateWrite::Append {
                field: "actual-calls".into(),
                items: vec![serde_json::json!("once")],
            }],
        };
        assert_eq!(
            after.execution.snapshot.tasks[0].status,
            TaskStatus::Succeeded
        );
        assert_eq!(
            after.execution.snapshot.tasks[0].result,
            Some(expected_result)
        );
        assert!(after.execution.snapshot.tasks[0].failure.is_none());
        let mut expected_values = before.execution.snapshot.values.clone();
        expected_values.insert("actual-calls".into(), serde_json::json!(["once"]));
        assert_eq!(after.execution.snapshot.values, expected_values);
        assert_eq!(
            after.execution.snapshot.output,
            Some(serde_json::Value::Object(expected_values))
        );
        retire(host, inner.clone(), inner, application, &directory, false).await;
        gate.positive();
        assert_eq!(shutdown.status().active_owners, 0);
    }
}
