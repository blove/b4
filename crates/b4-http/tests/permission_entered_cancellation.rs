//! Opt-in entered permission cancellation, observed over real loopback HTTP and SQLite.
#[path = "native_cooperative_support/mod.rs"]
mod cooperative;
#[path = "permission_entered_support/mod.rs"]
mod support;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{permissions::*, *};
use b4_sqlite::SqliteStore;
use cooperative::tcp::{finish, Server};
use cooperative::{snapshot, until, Callback, Cleanup, Directory, Gate, Return, Series};
use serde_json::Value;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use support::{store::Boundary, *};
fn read_capture(store: &SqliteStore, path: &std::path::Path, id: &str) -> Capture {
    let thread = ThreadId::new(id).unwrap();
    let current = store
        .thread_view(&thread)
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    capture(store, path, &thread, &current.execution_id)
}
async fn entered(callback: &Callback) {
    until(|| callback.gate.facts().entered).await;
    assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
    assert!(!callback.notified());
}
async fn settled(store: &SqliteStore, id: &str) {
    until(|| snapshot(store, id).tasks[0].status != TaskStatus::InFlight).await;
}
fn provenance(c: &Capture, original: bool) {
    assert_eq!(c.raw.0[6].len(), 1);
    assert_eq!(c.raw.0[7].len(), 1);
    let bytes = |cell: &rusqlite::types::Value| match cell {
        rusqlite::types::Value::Text(s) => s.as_bytes().to_vec(),
        _ => panic!("encoded record"),
    };
    let acceptance = PermissionAcceptance::decode_record(
        &bytes(&c.raw.0[6][0][7]),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let decision = PermissionDecision::decode_record(
        &bytes(&c.raw.0[7][0][5]),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    assert_eq!(
        acceptance.decisions()[0].decision(),
        ApprovalDecision::Always
    );
    assert_eq!(
        acceptance.decisions()[0].permission_id(),
        Some(decision.permission_id())
    );
    assert_eq!(c.raw.0[8].len(), usize::from(!original));
    if !original {
        let use_record = PermissionUse::decode_record(
            &bytes(&c.raw.0[8][0][9]),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(use_record.permission_id(), decision.permission_id());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_and_saved_new_use_ack_keep_ownership_and_survive_reopen_for_later_reuse() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let first = Callback::new(Return::Acknowledge);
    let second = Callback::new(Return::Acknowledge);
    let third = Callback::new(Return::Succeed);
    let _a = Cleanup(first.gate.clone());
    let _b = Cleanup(second.gate.clone());
    let _c = Cleanup(third.gate.clone());
    let catalog = catalog(
        Series::new(vec![first.clone(), second.clone(), third.clone()]),
        Arc::new(Constraint::default()),
    );
    let host = support::host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let metadata = request(&server, "GET", "/b4/capabilities", "").await;
    assert_eq!(metadata.status, 200);
    let descriptor: Value = serde_json::from_str(&metadata.body).unwrap();
    assert_eq!(descriptor["descriptor_version"], 5);
    assert_eq!(
        descriptor["features"]["durable_permission_entered_cancellation"],
        true
    );
    assert_eq!(descriptor["features"]["fenced_cancellation"], false);
    for (index, callback) in [first.clone(), second.clone()].into_iter().enumerate() {
        let stream = if index == 0 {
            original(&server, "ack").await
        } else {
            reuse(&server, "ack").await
        };
        entered(&callback).await;
        let before = snapshot(store.as_ref(), "ack");
        provenance(&read_capture(&store, &directory.0, "ack"), index == 0);
        let response = cancel(&server, "ack").await;
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_str::<Value>(&response.body).unwrap()["status"],
            "interrupted"
        );
        terminal(&finish(stream, Vec::new()).await);
        assert!(callback.notified());
        let held = snapshot(store.as_ref(), "ack");
        assert_eq!(held.status, ExecutionStatus::CancelRequested);
        assert_eq!(held.revision, if index == 0 { 5 } else { 4 });
        assert_eq!(held.tasks[0].status, TaskStatus::InFlight);
        assert_eq!(held.attempt_history, before.attempt_history);
        excluded(&server, &host, &directory.0, "ack").await;
        callback.gate.release();
        settled(&store, "ack").await;
        let after = snapshot(store.as_ref(), "ack");
        assert_eq!(after.status, ExecutionStatus::Cancelled);
        assert_eq!(after.tasks[0].status, TaskStatus::Cancelled);
        assert!(after.tasks[0].result.is_none() && after.tasks[0].failure.is_none());
        cooperative::acknowledged(&callback);
        until(|| host.shutdown_handle().status().active_owners == 0).await;
    }
    retire(host, server, false).await;
    let before = read_capture(&store, &directory.0, "ack");
    let owner = store.owner_generation();
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert!(same_after_reopen(
        &before,
        &read_capture(&store, &directory.0, "ack"),
        owner + 1
    ));
    let host = support::host(store.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let stream = reuse(&server, "ack").await;
    entered(&third).await;
    third.gate.release();
    assert_eq!(finish(stream, Vec::new()).await.status, 200);
    settled(&store, "ack").await;
    assert_eq!(
        snapshot(store.as_ref(), "ack").tasks[0].status,
        TaskStatus::Succeeded
    );
    assert_eq!(raw(&directory.0).0[8].len(), 2);
    retire(host, server, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmed_request_preserves_actual_late_success_and_failure() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let success = Callback::new(Return::Succeed);
    let failure = Callback::new(Return::Fail);
    let _a = Cleanup(success.gate.clone());
    let _b = Cleanup(failure.gate.clone());
    let catalog = catalog(
        Series::new(vec![success.clone(), failure.clone()]),
        Arc::new(Constraint::default()),
    );
    let host = support::host(store.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    for (index, callback) in [success.clone(), failure.clone()].into_iter().enumerate() {
        let stream = if index == 0 {
            original(&server, "late").await
        } else {
            reuse(&server, "late").await
        };
        entered(&callback).await;
        let before = snapshot(store.as_ref(), "late");
        assert_eq!(cancel(&server, "late").await.status, 200);
        assert!(callback.notified());
        terminal(&finish(stream, Vec::new()).await);
        callback.gate.release();
        settled(&store, "late").await;
        let after = snapshot(store.as_ref(), "late");
        assert_eq!(
            after.status,
            if index == 0 {
                ExecutionStatus::Cancelled
            } else {
                ExecutionStatus::Failed
            }
        );
        assert_eq!(after.attempt_history, before.attempt_history);
        if index == 0 {
            assert_eq!(after.tasks[0].status, TaskStatus::Succeeded);
            let NativeOutcome::Succeeded(result) = cooperative::genuine_success() else {
                unreachable!()
            };
            assert_eq!(after.tasks[0].result, Some(result));
            assert_eq!(after.values, before.values);
        } else {
            assert_eq!(after.tasks[0].status, TaskStatus::Failed);
            assert_eq!(
                after.tasks[0].failure.as_deref(),
                Some("known-native-failure")
            );
            assert!(after.tasks[0].result.is_none());
        }
        until(|| host.shutdown_handle().status().active_owners == 0).await;
    }
    retire(host, server, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_cancel_policy_and_origin_refuse_without_writes_but_constraint_cannot_veto_stop() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Acknowledge);
    let _a = Cleanup(callback.gate.clone());
    let policy = Arc::new(Policy::default());
    let constraint = Arc::new(Constraint::default());
    let host = support::host(
        store.clone(),
        catalog(callback.clone(), constraint.clone()),
        policy.clone(),
    );
    let server = Server::start(&host).await;
    let stream = original(&server, "privacy").await;
    entered(&callback).await;
    let before = read_capture(&store, &directory.0, "privacy");
    policy.deny.store(true, Ordering::Release);
    let denied = cancel(&server, "privacy").await;
    assert!([403, 404].contains(&denied.status));
    assert_eq!(read_capture(&store, &directory.0, "privacy"), before);
    assert!(!callback.notified());
    policy.deny.store(false, Ordering::Release);
    let foreign = finish(
        open(
            server.address,
            "POST",
            "/threads/privacy/cancel",
            "",
            "foreign",
        )
        .await,
        Vec::new(),
    )
    .await;
    assert!([403, 404].contains(&foreign.status));
    assert_eq!(read_capture(&store, &directory.0, "privacy"), before);
    assert!(!callback.notified());
    constraint.0.store(true, Ordering::Release);
    assert_eq!(cancel(&server, "privacy").await.status, 200);
    terminal(&finish(stream, Vec::new()).await);
    assert!(callback.notified());
    callback.gate.release();
    settled(&store, "privacy").await;
    cooperative::acknowledged(&callback);
    retire(host, server, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_dispatch_without_actual_entry_refuses_then_same_run_enters_and_cancels() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let boundary_gate = Gate::new();
    let _boundary = Cleanup(boundary_gate.clone());
    let mut boundary = Boundary::new(inner.clone());
    boundary.before_entry = Some(boundary_gate.clone());
    let store = Arc::new(boundary);
    let first = Callback::new(Return::Succeed);
    let callback = Callback::new(Return::Acknowledge);
    let _a = Cleanup(first.gate.clone());
    let _b = Cleanup(callback.gate.clone());
    let catalog = catalog(
        Series::new(vec![first.clone(), callback.clone()]),
        Arc::new(Constraint::default()),
    );
    let host = support::host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let stream = original(&server, "unentered").await;
    entered(&first).await;
    first.gate.release();
    assert_eq!(finish(stream, Vec::new()).await.status, 200);
    settled(&inner, "unentered").await;
    until(|| host.shutdown_handle().status().active_owners == 0).await;
    let stream = reuse(&server, "unentered").await;
    until(|| boundary_gate.facts().entered).await;
    let before = read_capture(&inner, &directory.0, "unentered");
    assert_eq!(
        before.view.execution.as_ref().unwrap().snapshot.tasks[0].status,
        TaskStatus::InFlight
    );
    assert_eq!(before.view.execution.as_ref().unwrap().snapshot.revision, 3);
    assert!(!callback.gate.facts().entered);
    assert_eq!(callback.controlled.load(Ordering::Acquire), 0);
    let response = cancel(&server, "unentered").await;
    assert_eq!(response.status, 409);
    assert_eq!(
        serde_json::from_str::<Value>(&response.body).unwrap()["error"]["details"]["code"],
        "run_not_cancellable"
    );
    assert_eq!(read_capture(&inner, &directory.0, "unentered"), before);
    boundary_gate.release();
    entered(&callback).await;
    assert_eq!(
        snapshot(inner.as_ref(), "unentered").execution_id,
        before
            .view
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .execution_id
    );
    assert_eq!(cancel(&server, "unentered").await.status, 200);
    terminal(&finish(stream, Vec::new()).await);
    callback.gate.release();
    settled(&inner, "unentered").await;
    retire(host, server, false).await;
    // Both old permission constructors retain their advertised version and unsupported cancellation.
    for uses in [false, true] {
        let legacy = Callback::new(Return::Succeed);
        let _legacy = Cleanup(legacy.gate.clone());
        let catalog = support::catalog(legacy.clone(), Arc::new(Constraint::default()));
        let config = config(catalog.clone());
        let policy = Arc::new(Policy::default());
        let old = if uses {
            HttpHost::new_with_permission_uses(
                store.clone(),
                Arc::new(Auth),
                policy,
                PermissionUseHostConfig::new(catalog.clone(), config.approval),
            )
            .unwrap()
        } else {
            HttpHost::new_with_permissions(store.clone(), Arc::new(Auth), policy, config).unwrap()
        };
        let server = Server::start(&old).await;
        let metadata = request(&server, "GET", "/b4/capabilities", "").await;
        let metadata: Value = serde_json::from_str(&metadata.body).unwrap();
        assert_eq!(metadata["descriptor_version"], if uses { 3 } else { 4 });
        assert!(metadata["features"]
            .get("durable_permission_entered_cancellation")
            .is_none());
        let id = if uses { "old-use" } else { "old-creation" };
        let primary = if uses {
            reuse(&server, "unentered").await
        } else {
            original(&server, id).await
        };
        let id = if uses { "unentered" } else { id };
        until(|| legacy.gate.facts().entered).await;
        assert_eq!(legacy.controlled.load(Ordering::Acquire), 0);
        assert_eq!(legacy.legacy.load(Ordering::Acquire), 1);
        let before = raw(&directory.0);
        let refusal = cancel(&server, id).await;
        assert_eq!(refusal.status, 501);
        assert_eq!(
            serde_json::from_str::<Value>(&refusal.body).unwrap()["error"]["details"]["code"],
            "permission_operation_unsupported"
        );
        assert_eq!(raw(&directory.0), before);
        assert!(!legacy.notified());
        legacy.gate.release();
        assert_eq!(finish(primary, Vec::new()).await.status, 200);
        settled(&inner, id).await;
        retire(old, server, false).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_paused_old_cancel_never_retargets_a_live_successor() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let old = Callback::new(Return::Succeed);
    let successor = Callback::new(Return::Succeed);
    let _a = Cleanup(old.gate.clone());
    let _b = Cleanup(successor.gate.clone());
    let pause = Gate::new();
    let _pause = Cleanup(pause.clone());
    let policy = Arc::new(Policy::default());
    let host = support::host(
        store.clone(),
        catalog(
            Series::new(vec![old.clone(), successor.clone()]),
            Arc::new(Constraint::default()),
        ),
        policy.clone(),
    );
    let server = Server::start(&host).await;
    let primary = original(&server, "successor").await;
    entered(&old).await;
    let original_id = snapshot(store.as_ref(), "successor").execution_id;
    *policy.pause.lock().unwrap() = Some(pause.clone());
    let cancellation = open(
        server.address,
        "POST",
        "/threads/successor/cancel",
        "",
        "owner",
    )
    .await;
    until(|| pause.facts().entered).await;
    old.gate.release();
    assert_eq!(finish(primary, Vec::new()).await.status, 200);
    settled(&store, "successor").await;
    // Only the paused management operation remains after the old run retires.
    until(|| host.shutdown_handle().status().active_owners == 1).await;
    let primary = reuse(&server, "successor").await;
    entered(&successor).await;
    let before = read_capture(&store, &directory.0, "successor");
    assert_ne!(
        before
            .view
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .execution_id,
        original_id
    );
    pause.release();
    let response = finish(cancellation, Vec::new()).await;
    assert_eq!(response.status, 409);
    assert_eq!(read_capture(&store, &directory.0, "successor"), before);
    assert!(!old.notified() && !successor.notified());
    successor.gate.release();
    assert_eq!(finish(primary, Vec::new()).await.status, 200);
    settled(&store, "successor").await;
    retire(host, server, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_late_request_journal_abort_rolls_back_without_signal_then_fresh_owner_healthy_control(
) {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Succeed);
    let _a = Cleanup(callback.gate.clone());
    let catalog = catalog(callback.clone(), Arc::new(Constraint::default()));
    let host = support::host(store.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let primary = original(&server, "fault").await;
    entered(&callback).await;
    let before = read_capture(&store, &directory.0, "fault");
    let current = before.view.execution.as_ref().unwrap();
    let revision = current.snapshot.revision + 1;
    let resource = before.view.thread.resource_revision + 1;
    let execution = &current.snapshot.execution_id;
    let sql=format!("CREATE TRIGGER entered_cancel_guard BEFORE INSERT ON journal WHEN NEW.kind = 'commit' AND NEW.execution_id = '{execution}' AND NEW.thread_id = 'fault' AND NEW.revision = x'{revision:016x}' AND EXISTS (SELECT 1 FROM executions WHERE id = NEW.execution_id AND revision = NEW.revision) AND EXISTS (SELECT 1 FROM threads WHERE id = NEW.thread_id AND active_execution_id = NEW.execution_id AND resource_revision = x'{resource:016x}') BEGIN SELECT RAISE(ABORT, 'entered_late_request_journal_fault'); END");
    let db = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    db.execute_batch(&sql).unwrap();
    let ddl: String = db
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='entered_cancel_guard'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ddl, sql);
    let count:i64=db.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='entered_cancel_guard'",[],|r|r.get(0)).unwrap();
    assert_eq!(count, 1);
    let response = cancel(&server, "fault").await;
    assert_eq!(response.status, 503);
    assert_eq!(read_capture(&store, &directory.0, "fault"), before);
    assert!(!callback.notified());
    assert!(!callback.returned.load(Ordering::Acquire));
    db.execute_batch("DROP TRIGGER entered_cancel_guard")
        .unwrap();
    let count:i64=db.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='entered_cancel_guard'",[],|r|r.get(0)).unwrap();
    assert_eq!(count, 0);
    db.close().unwrap();
    assert_eq!(read_capture(&store, &directory.0, "fault"), before);
    callback.gate.release();
    let _response = finish(primary, Vec::new()).await;
    assert!(host.shutdown_handle().status().faulted);
    retire(host, server, true).await;
    assert!(!callback.notified());
    let owner = store.owner_generation();
    drop(store);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), owner + 1);
    let healthy = Callback::new(Return::Acknowledge);
    let _b = Cleanup(healthy.gate.clone());
    let host = support::host(
        store.clone(),
        support::catalog(healthy.clone(), Arc::new(Constraint::default())),
        Arc::new(Policy::default()),
    );
    let server = Server::start(&host).await;
    let primary = original(&server, "healthy").await;
    entered(&healthy).await;
    assert_eq!(cancel(&server, "healthy").await.status, 200);
    terminal(&finish(primary, Vec::new()).await);
    healthy.gate.release();
    settled(&store, "healthy").await;
    cooperative::acknowledged(&healthy);
    retire(host, server, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_admitted_cancel_retains_callback_and_sqlite_until_commit_and_quiescence() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let owner = inner.owner_generation();
    let committed = Gate::new();
    let _boundary = Cleanup(committed.clone());
    let mut boundary = Boundary::new(inner.clone());
    boundary.after_cancel = Some(committed.clone());
    let store = Arc::new(boundary);
    let callback = Callback::new(Return::Acknowledge);
    let gate = callback.gate.clone();
    let _callback = Cleanup(gate.clone());
    let constraint = Arc::new(Constraint::default());
    let weak_constraint = Arc::downgrade(&constraint);
    let catalog = catalog(callback.clone(), constraint.clone());
    let weak_store = Arc::downgrade(&store);
    let weak_inner = Arc::downgrade(&inner);
    let weak_callback = Arc::downgrade(&callback);
    let host = support::host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    let primary = original(&server, "abandon").await;
    entered(&callback).await;
    let cancellation = open(
        server.address,
        "POST",
        "/threads/abandon/cancel",
        "",
        "owner",
    )
    .await;
    until(|| committed.facts().entered).await;
    let held = read_capture(&inner, &directory.0, "abandon");
    assert_eq!(
        held.view.execution.as_ref().unwrap().snapshot.status,
        ExecutionStatus::CancelRequested
    );
    assert!(!callback.notified());
    drop(cancellation);
    excluded(&server, &host, &directory.0, "abandon").await;
    committed.release();
    until(|| callback.notified()).await;
    terminal(&finish(primary, Vec::new()).await);
    server.stop().await;
    drop(host);
    drop(catalog);
    drop(constraint);
    drop(callback);
    drop(store);
    drop(inner);
    assert!(
        weak_store.upgrade().is_some()
            && weak_inner.upgrade().is_some()
            && weak_callback.upgrade().is_some()
            && weak_constraint.upgrade().is_some()
    );
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    gate.release();
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert!(
        weak_store.upgrade().is_none()
            && weak_inner.upgrade().is_none()
            && weak_callback.upgrade().is_none()
            && weak_constraint.upgrade().is_none()
    );
    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(reopened.owner_generation(), owner + 1);
    let after = snapshot(&reopened, "abandon");
    assert_eq!(after.status, ExecutionStatus::Cancelled);
    assert_eq!(after.tasks[0].status, TaskStatus::Cancelled);
    assert!(after.tasks[0].result.is_none());
    assert_eq!(
        after.attempt_history,
        held.view
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .attempt_history
    );
    reopened.readiness().unwrap();
}
