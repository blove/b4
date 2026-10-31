//! V6 registered Ready cancellation through real human approval, loopback HTTP and SQLite.
#[path = "native_cooperative_support/mod.rs"]
mod cooperative;
#[path = "permission_entered_support/mod.rs"]
mod support;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{permissions::*, *};
use b4_sqlite::SqliteStore;
use cooperative::tcp::{finish, Server};
use cooperative::{snapshot, until, Callback, Cleanup, Directory, Gate, Return};
use serde_json::Value;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use support::{store::Boundary, *};

fn ready_host<S: PermissionStore + 'static>(
    store: Arc<S>,
    catalog: Arc<PermissionCatalog>,
    policy: Arc<Policy>,
) -> HttpHost<S> {
    HttpHost::new_with_permission_ready_cancellation(store, Arc::new(Auth), policy, config(catalog))
        .unwrap()
}
fn observe(store: &SqliteStore, path: &std::path::Path, id: &str) -> Capture {
    let thread = ThreadId::new(id).unwrap();
    let current = store
        .thread_view(&thread)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    capture(store, path, &thread, &current.snapshot.execution_id)
}
fn uninvoked(callback: &Callback) {
    assert_eq!(callback.controlled.load(Ordering::Acquire), 0);
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
    assert!(
        callback.observer().is_none(),
        "no real receipt or signal recipient"
    );
    assert!(!callback.notified());
    assert!(!callback.returned.load(Ordering::Acquire));
    assert!(!callback.gate.facts().entered);
}
fn ready(before: &Capture, original: bool) {
    let current = &before.view.execution.as_ref().unwrap().snapshot;
    assert_eq!(current.revision, if original { 3 } else { 2 });
    assert_eq!(current.status, ExecutionStatus::Running);
    assert_eq!(current.tasks[0].status, TaskStatus::Ready);
    assert!(current.tasks[0].attempt_id.is_none());
    assert_eq!(before.raw.0[6].len(), 1, "genuine Always acceptance");
    assert_eq!(before.raw.0[7].len(), 1, "saved permission decision");
    assert_eq!(before.raw.0[8].len(), usize::from(!original));
}
fn exact_cancel(before: &Capture, after: &Capture) {
    let prior = before.view.execution.as_ref().unwrap();
    let actual = after.view.execution.as_ref().unwrap();
    let expected = b4_kernel::transition(
        &prior.snapshot,
        KernelCommand::Cancel {
            turn_id: prior.snapshot.turn_id.clone(),
            owner_generation: prior.snapshot.owner_generation,
        },
        TransitionFacts {
            now_ms: actual.snapshot.updated_at_ms,
        },
    )
    .unwrap()
    .snapshot;
    assert_eq!(actual.snapshot, expected);
    assert_eq!(actual.snapshot.revision, prior.snapshot.revision + 1);
    assert_eq!(actual.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(actual.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert_eq!(
        actual.snapshot.attempt_history,
        prior.snapshot.attempt_history
    );
    assert!(
        actual.snapshot.tasks[0].result.is_none() && actual.snapshot.tasks[0].failure.is_none()
    );
    let mut thread = before.view.thread.clone();
    thread.resource_revision += 1;
    assert_eq!(after.view.thread, thread);
    let mut journal = before.current_journal.clone();
    journal.push(JournalEntry::from(&actual.snapshot));
    assert_eq!(after.current_journal, journal);
    assert_eq!(after.original, *actual);
    assert_eq!(after.raw.0[0], before.raw.0[0]);
    assert_eq!(
        after.raw.0[4..],
        before.raw.0[4..],
        "immutable grant/acceptance/permission/Use evidence"
    );
    assert_eq!(&after.raw.0[3][..before.raw.0[3].len()], &before.raw.0[3]);
    assert_eq!(after.raw.0[3].len(), before.raw.0[3].len() + 1);
    assert_eq!(after.raw.0[2].len(), before.raw.0[2].len());
    for row in &before.raw.0[2] {
        if row[0] != rusqlite::types::Value::Text(actual.snapshot.execution_id.to_string()) {
            assert!(after.raw.0[2].contains(row), "old execution unchanged");
        }
    }
}
fn arm(store: &Boundary) -> (Arc<Gate>, Cleanup) {
    let gate = Gate::new();
    *store.before_ready.lock().unwrap() = Some(gate.clone());
    (gate.clone(), Cleanup(gate))
}
async fn entered(callback: &Callback) {
    until(|| callback.gate.facts().entered).await;
    assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
}
async fn drained<S: Store + 'static>(host: &HttpHost<S>) {
    until(|| host.shutdown_handle().status().active_owners == 0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_and_saved_new_use_ready_terminal_hold_owners_then_reopen_and_reuse() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let store = Arc::new(Boundary::new(inner.clone()));
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let catalog = catalog(callback.clone(), Arc::new(Constraint::default()));
    let host = ready_host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    for original_run in [true, false] {
        let (gate, _gate) = arm(&store);
        let primary = if original_run {
            original(&server, "ready").await
        } else {
            reuse(&server, "ready").await
        };
        until(|| gate.facts().entered).await;
        let before = observe(&inner, &directory.0, "ready");
        ready(&before, original_run);
        uninvoked(&callback);
        assert_eq!(cancel(&server, "ready").await.status, 200);
        terminal(&finish(primary, Vec::new()).await);
        exact_cancel(&before, &observe(&inner, &directory.0, "ready"));
        uninvoked(&callback);
        excluded(&server, &host, &directory.0, "ready").await;
        gate.release();
        drained(&host).await;
        assert!(!gate.facts().expired && gate.facts().explicit);
        uninvoked(&callback);
    }
    retire(host, server, false).await;
    let before = observe(&inner, &directory.0, "ready");
    let owner = inner.owner_generation();
    drop(store);
    assert_eq!(Arc::strong_count(&inner), 1);
    drop(inner);
    let reopened = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert!(same_after_reopen(
        &before,
        &observe(&reopened, &directory.0, "ready"),
        owner + 1
    ));
    let host = ready_host(reopened.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let primary = reuse(&server, "ready").await;
    entered(&callback).await;
    callback.gate.release();
    assert_eq!(finish(primary, Vec::new()).await.status, 200);
    drained(&host).await;
    assert_eq!(
        snapshot(&reopened, "ready").status,
        ExecutionStatus::Completed
    );
    assert_eq!(raw(&directory.0).0[8].len(), 2);
    retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_wins_ready_writer_refusal_releases_pending_then_same_run_enters_and_cancels() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let writer = Gate::new();
    let pre_entry = Gate::new();
    let _writer = Cleanup(writer.clone());
    let _entry = Cleanup(pre_entry.clone());
    let mut boundary = Boundary::new(inner.clone());
    boundary.before_ready_cancel = Some(writer.clone());
    boundary.before_entry = Some(pre_entry.clone());
    let store = Arc::new(boundary);
    let callback = Callback::new(Return::Acknowledge);
    let _callback = Cleanup(callback.gate.clone());
    let host = ready_host(
        store.clone(),
        catalog(callback.clone(), Arc::new(Constraint::default())),
        Arc::new(Policy::default()),
    );
    let server = Server::start(&host).await;
    // Seed Always by a real original Ready cancellation. The one-shot writer
    // barrier is explicitly released here and is reused only as an observed seam.
    writer.release();
    let (seed_gate, _seed) = arm(&store);
    let seed = original(&server, "race").await;
    until(|| seed_gate.facts().entered).await;
    assert_eq!(cancel(&server, "race").await.status, 200);
    terminal(&finish(seed, Vec::new()).await);
    seed_gate.release();
    drained(&host).await;
    retire(host, server, false).await;
    drop(store);

    let writer = Gate::new();
    let _writer = Cleanup(writer.clone());
    let mut boundary = Boundary::new(inner.clone());
    boundary.before_ready_cancel = Some(writer.clone());
    boundary.before_entry = Some(pre_entry.clone());
    let store = Arc::new(boundary);
    let host = ready_host(
        store.clone(),
        catalog(callback.clone(), Arc::new(Constraint::default())),
        Arc::new(Policy::default()),
    );
    let server = Server::start(&host).await;
    let (gate, _gate) = arm(&store);
    let primary = reuse(&server, "race").await;
    until(|| gate.facts().entered).await;
    let cancellation = open(server.address, "POST", "/threads/race/cancel", "", "owner").await;
    until(|| writer.facts().entered).await;
    ready(&observe(&inner, &directory.0, "race"), false);
    gate.release();
    until(|| pre_entry.facts().entered).await;
    let dispatched = observe(&inner, &directory.0, "race");
    assert_eq!(
        dispatched
            .view
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .revision,
        3
    );
    assert_eq!(
        dispatched.view.execution.as_ref().unwrap().snapshot.tasks[0].status,
        TaskStatus::InFlight
    );
    uninvoked(&callback);
    writer.release();
    let refused = finish(cancellation, Vec::new()).await;
    assert_eq!(refused.status, 409);
    assert_eq!(
        serde_json::from_str::<Value>(&refused.body).unwrap()["error"]["details"]["code"],
        "run_not_cancellable"
    );
    assert_eq!(observe(&inner, &directory.0, "race"), dispatched);
    uninvoked(&callback);
    pre_entry.release();
    entered(&callback).await;
    assert_eq!(
        snapshot(&inner, "race").execution_id,
        dispatched
            .view
            .execution
            .as_ref()
            .unwrap()
            .snapshot
            .execution_id
    );
    assert_eq!(cancel(&server, "race").await.status, 200);
    assert!(callback.notified());
    terminal(&finish(primary, Vec::new()).await);
    callback.gate.release();
    drained(&host).await;
    cooperative::acknowledged(&callback);
    assert_eq!(snapshot(&inner, "race").status, ExecutionStatus::Cancelled);
    retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_origin_capability_metadata_and_v5_ready_refusal_preserve_profile_boundaries() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert!(inner.permission_original_ready_cancellation_atomic_durable());
    assert!(inner.permission_use_ready_cancellation_atomic_durable());
    let mut missing = Boundary::new(inner.clone());
    missing.disable_original_ready = true;
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let constraint = Arc::new(Constraint::default());
    let catalog = catalog(callback.clone(), constraint.clone());
    assert!(HttpHost::new_with_permission_ready_cancellation(
        Arc::new(missing),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        config(catalog.clone())
    )
    .is_err());
    let store = Arc::new(Boundary::new(inner.clone()));
    let policy = Arc::new(Policy::default());
    let host = ready_host(store.clone(), catalog.clone(), policy.clone());
    let server = Server::start(&host).await;
    let metadata = request(&server, "GET", "/b4/capabilities", "").await;
    assert_eq!(metadata.status, 200);
    let descriptor: Value = serde_json::from_str(&metadata.body).unwrap();
    assert_eq!(descriptor["descriptor_version"], 6);
    assert_eq!(
        descriptor["features"]["durable_permission_ready_cancellation"],
        true
    );
    assert_eq!(
        descriptor["features"]["durable_permission_entered_cancellation"],
        true
    );
    for flag in [
        "fenced_cancellation",
        "fenced_deletion",
        "automatic_recovery_scan",
        "always_permissions",
        "durable_event_replay",
    ] {
        assert_eq!(descriptor["features"][flag], false, "{flag}");
    }
    let (gate, _gate) = arm(&store);
    let primary = original(&server, "policy").await;
    until(|| gate.facts().entered).await;
    let before = observe(&inner, &directory.0, "policy");
    policy.deny.store(true, Ordering::Release);
    assert!([403, 404].contains(&cancel(&server, "policy").await.status));
    assert_eq!(observe(&inner, &directory.0, "policy"), before);
    policy.deny.store(false, Ordering::Release);
    let foreign = finish(
        open(
            server.address,
            "POST",
            "/threads/policy/cancel",
            "",
            "foreign",
        )
        .await,
        Vec::new(),
    )
    .await;
    assert!([403, 404].contains(&foreign.status));
    assert_eq!(observe(&inner, &directory.0, "policy"), before);
    uninvoked(&callback);
    constraint.0.store(true, Ordering::Release);
    assert_eq!(cancel(&server, "policy").await.status, 200);
    terminal(&finish(primary, Vec::new()).await);
    exact_cancel(&before, &observe(&inner, &directory.0, "policy"));
    gate.release();
    drained(&host).await;
    uninvoked(&callback);
    retire(host, server, false).await;
    constraint.0.store(false, Ordering::Release);

    let host = support::host(store.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let metadata = request(&server, "GET", "/b4/capabilities", "").await;
    let descriptor: Value = serde_json::from_str(&metadata.body).unwrap();
    assert_eq!(descriptor["descriptor_version"], 5);
    assert!(descriptor["features"]
        .get("durable_permission_ready_cancellation")
        .is_none());
    let (gate, _gate) = arm(&store);
    let primary = original(&server, "v5").await;
    until(|| gate.facts().entered).await;
    let before = observe(&inner, &directory.0, "v5");
    assert_eq!(cancel(&server, "v5").await.status, 409);
    assert_eq!(observe(&inner, &directory.0, "v5"), before);
    uninvoked(&callback);
    gate.release();
    entered(&callback).await;
    callback.gate.release();
    assert_eq!(finish(primary, Vec::new()).await.status, 200);
    drained(&host).await;
    retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_late_ready_journal_fault_rolls_back_then_owned_fault_drains_and_fresh_owner_control()
{
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let store = Arc::new(Boundary::new(inner.clone()));
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let catalog = catalog(callback.clone(), Arc::new(Constraint::default()));
    let host = ready_host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let (gate, _gate) = arm(&store);
    let primary = original(&server, "fault").await;
    until(|| gate.facts().entered).await;
    let before = observe(&inner, &directory.0, "fault");
    ready(&before, true);
    let current = before.view.execution.as_ref().unwrap();
    let execution = &current.snapshot.execution_id;
    let revision = current.snapshot.revision + 1;
    let resource = before.view.thread.resource_revision + 1;
    let ddl = format!(
        "CREATE TRIGGER ready_cancel_guard BEFORE INSERT ON journal WHEN NEW.kind = 'commit' AND NEW.execution_id = '{execution}' AND NEW.thread_id = 'fault' AND NEW.revision = x'{revision:016x}' AND EXISTS (SELECT 1 FROM executions WHERE id = NEW.execution_id AND revision = NEW.revision) AND EXISTS (SELECT 1 FROM threads WHERE id = NEW.thread_id AND active_execution_id = NEW.execution_id AND resource_revision = x'{resource:016x}') BEGIN SELECT RAISE(ABORT, 'ready_post_update_journal_fault'); END"
    );
    let db = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    db.execute_batch(&ddl).unwrap();
    let actual: String = db
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE name='ready_cancel_guard' AND type='trigger'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(actual, ddl);
    let presence: i64 = db
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name='ready_cancel_guard' AND type='trigger'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(presence, 1);
    assert_eq!(cancel(&server, "fault").await.status, 503);
    assert_eq!(
        observe(&inner, &directory.0, "fault"),
        before,
        "complete physical rollback"
    );
    uninvoked(&callback);
    assert!(host.shutdown_handle().status().active_owners > 0);
    assert!(host.shutdown_handle().status().faulted);
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    assert_eq!(
        request(&server, "POST", "/threads/fault/runs/wait", RUN)
            .await
            .status,
        503
    );
    db.execute_batch("DROP TRIGGER ready_cancel_guard").unwrap();
    let presence: i64 = db
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name='ready_cancel_guard' AND type='trigger'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(presence, 0);
    db.close().unwrap();
    gate.release();
    let response = finish(primary, Vec::new()).await;
    assert!(
        !response.body.contains(r#""output":{"cancelled":true}"#),
        "failed writer cannot publish success"
    );
    drained(&host).await;
    uninvoked(&callback);
    retire(host, server, true).await;
    drop(store);
    drop(inner);
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    inner.readiness().unwrap();
    let store = Arc::new(Boundary::new(inner.clone()));
    let host = ready_host(store.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let (gate, _gate) = arm(&store);
    let primary = original(&server, "fresh-control").await;
    until(|| gate.facts().entered).await;
    let before = observe(&inner, &directory.0, "fresh-control");
    assert_eq!(cancel(&server, "fresh-control").await.status, 200);
    terminal(&finish(primary, Vec::new()).await);
    exact_cancel(&before, &observe(&inner, &directory.0, "fresh-control"));
    gate.release();
    drained(&host).await;
    uninvoked(&callback);
    retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_ready_management_keeps_assembly_store_claim_and_capacity_through_commit_and_drain(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let committed = Gate::new();
    let _commit = Cleanup(committed.clone());
    let mut boundary = Boundary::new(inner.clone());
    boundary.after_ready_cancel = Some(committed.clone());
    let store = Arc::new(boundary);
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let constraint = Arc::new(Constraint::default());
    let catalog = catalog(callback.clone(), constraint.clone());
    let weak_store = Arc::downgrade(&store);
    let weak_inner = Arc::downgrade(&inner);
    let weak_callback = Arc::downgrade(&callback);
    let weak_constraint = Arc::downgrade(&constraint);
    let owner = inner.owner_generation();
    let host = ready_host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    let (gate, _gate) = arm(&store);
    let primary = original(&server, "abandon").await;
    until(|| gate.facts().entered).await;
    let before = observe(&inner, &directory.0, "abandon");
    let cancellation = open(
        server.address,
        "POST",
        "/threads/abandon/cancel",
        "",
        "owner",
    )
    .await;
    until(|| committed.facts().entered).await;
    let held = observe(&inner, &directory.0, "abandon");
    exact_cancel(&before, &held);
    uninvoked(&callback);
    drop(cancellation);
    excluded(&server, &host, &directory.0, "abandon").await;
    committed.release();
    terminal(&finish(primary, Vec::new()).await);
    // Driver remains at a finite real Store boundary after the management
    // response has gone away. Its actual owners must still exclude reuse/open.
    excluded(&server, &host, &directory.0, "abandon").await;
    uninvoked(&callback);
    server.stop().await;
    drop(host);
    drop(catalog);
    drop(constraint);
    drop(callback);
    drop(store);
    drop(inner);
    assert!(weak_store.upgrade().is_some() && weak_inner.upgrade().is_some());
    assert!(weak_callback.upgrade().is_some() && weak_constraint.upgrade().is_some());
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
    assert!(weak_store.upgrade().is_none() && weak_inner.upgrade().is_none());
    assert!(weak_callback.upgrade().is_none() && weak_constraint.upgrade().is_none());
    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert!(same_after_reopen(
        &held,
        &observe(&reopened, &directory.0, "abandon"),
        owner + 1
    ));
    reopened.readiness().unwrap();
}
