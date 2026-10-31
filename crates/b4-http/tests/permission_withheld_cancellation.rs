//! V7 expectations: real borrowed Driver failure, real typed SQLite withdrawal.
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
use support::{
    store::{Boundary, WithheldRead},
    *,
};

fn withheld_host<S: PermissionStore + 'static>(
    store: Arc<S>,
    catalog: Arc<PermissionCatalog>,
    policy: Arc<Policy>,
) -> HttpHost<S> {
    HttpHost::new_with_permission_withheld_cancellation(
        store,
        Arc::new(Auth),
        policy,
        config(catalog),
    )
    .unwrap()
}
fn observe(store: &SqliteStore, path: &std::path::Path, id: &str) -> Capture {
    let thread = ThreadId::new(id).unwrap();
    let execution = store
        .thread_view(&thread)
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
        .execution_id;
    capture(store, path, &thread, &execution)
}
fn uninvoked(callback: &Callback) {
    assert_eq!(callback.controlled.load(Ordering::Acquire), 0);
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
    assert!(callback.observer().is_none());
    assert!(!callback.notified() && !callback.returned.load(Ordering::Acquire));
    assert!(!callback.gate.facts().entered);
}
async fn drained<S: Store + 'static>(host: &HttpHost<S>) {
    until(|| host.shutdown_handle().status().active_owners == 0).await;
}
async fn entered(callback: &Callback) {
    until(|| callback.gate.facts().entered).await;
    assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
    assert!(callback.observer().is_some());
}
fn dispatched(before: &Capture, original: bool) {
    let s = &before.view.execution.as_ref().unwrap().snapshot;
    assert_eq!(s.revision, if original { 4 } else { 3 });
    assert_eq!(s.status, ExecutionStatus::Running);
    assert_eq!(s.tasks[0].status, TaskStatus::InFlight);
    assert!(s.tasks[0].attempt_id.is_some());
    let execution = rusqlite::types::Value::Text(s.execution_id.to_string());
    let acceptance_rows: Vec<_> = before.raw.0[6]
        .iter()
        .filter(|r| r[0] == execution)
        .collect();
    let use_rows: Vec<_> = before.raw.0[8]
        .iter()
        .filter(|r| r[0] == execution)
        .collect();
    assert_eq!(acceptance_rows.len(), usize::from(original));
    assert_eq!(use_rows.len(), usize::from(!original));
    let bytes = |cell: &rusqlite::types::Value| match cell {
        rusqlite::types::Value::Text(s) => s.as_bytes().to_vec(),
        _ => panic!("encoded permission evidence"),
    };
    let acceptances: Vec<_> = before.raw.0[6]
        .iter()
        .map(|r| {
            PermissionAcceptance::decode_record(&bytes(&r[7]), &mut PermissionEvidenceBudget::new())
                .unwrap()
        })
        .collect();
    let decisions: Vec<_> = before.raw.0[7]
        .iter()
        .map(|r| {
            PermissionDecision::decode_record(&bytes(&r[5]), &mut PermissionEvidenceBudget::new())
                .unwrap()
        })
        .collect();
    if original {
        let acceptance = PermissionAcceptance::decode_record(
            &bytes(&acceptance_rows[0][7]),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(
            acceptance.decisions()[0].decision(),
            ApprovalDecision::Always
        );
        assert!(decisions
            .iter()
            .any(|d| Some(d.permission_id()) == acceptance.decisions()[0].permission_id()));
    } else {
        let used = PermissionUse::decode_record(
            &bytes(&use_rows[0][9]),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert!(decisions
            .iter()
            .any(|d| d.permission_id() == used.permission_id()));
        assert!(acceptances
            .iter()
            .any(|a| a.decisions()[0].decision() == ApprovalDecision::Always
                && a.decisions()[0].permission_id() == Some(used.permission_id())));
    }
}
fn exact_withdrawal(before: &Capture, after: &Capture) {
    let prior = &before.view.execution.as_ref().unwrap().snapshot;
    let actual = &after.view.execution.as_ref().unwrap().snapshot;
    let facts = TransitionFacts {
        now_ms: actual.updated_at_ms,
    };
    let requested = b4_kernel::transition(
        prior,
        KernelCommand::Cancel {
            turn_id: prior.turn_id.clone(),
            owner_generation: prior.owner_generation,
        },
        facts.clone(),
    )
    .unwrap()
    .snapshot;
    assert_eq!(requested.status, ExecutionStatus::CancelRequested);
    let terminal = b4_kernel::transition(
        &requested,
        KernelCommand::CancelAttempt {
            task_id: prior.tasks[0].spec.id.clone(),
            attempt_id: prior.tasks[0].attempt_id.clone().unwrap(),
        },
        facts,
    )
    .unwrap()
    .snapshot;
    assert_eq!(actual, &terminal);
    assert_eq!(actual.revision, prior.revision + 2);
    assert_eq!(actual.status, ExecutionStatus::Cancelled);
    assert_eq!(actual.tasks[0].status, TaskStatus::Cancelled);
    assert!(actual.tasks[0].result.is_none() && actual.tasks[0].failure.is_none());
    let mut journal = before.current_journal.clone();
    journal.extend([
        JournalEntry::from(&requested),
        JournalEntry::from(&terminal),
    ]);
    assert_eq!(after.current_journal, journal);
    assert_eq!(after.original_journal, journal);
    assert_eq!(after.original.snapshot, terminal);
    let mut thread = before.view.thread.clone();
    thread.resource_revision += 2;
    assert_eq!(after.view.thread, thread);
    assert_eq!(after.raw.0[0], before.raw.0[0]);
    assert_eq!(
        after.raw.0[4..],
        before.raw.0[4..],
        "all authority, acceptance and Use evidence immutable"
    );
    assert_eq!(&after.raw.0[3][..before.raw.0[3].len()], &before.raw.0[3]);
    assert_eq!(after.raw.0[3].len(), before.raw.0[3].len() + 2);
    assert_eq!(after.raw.0[2].len(), before.raw.0[2].len());
    for row in &before.raw.0[2] {
        if row[0] != rusqlite::types::Value::Text(actual.execution_id.to_string()) {
            assert!(after.raw.0[2].contains(row), "prior execution unchanged");
        }
    }
}
/// All barriers belong to actual Store operations, never to a fabricated Work.
struct Seams {
    renewal: Arc<Gate>,
    preflight: Arc<Gate>,
    writer: Arc<Gate>,
    committed: Arc<Gate>,
    _cleanup: Vec<Cleanup>,
}
impl Seams {
    fn install(inner: Arc<SqliteStore>, original: bool) -> (Arc<Boundary>, Self) {
        let renewal = Gate::new();
        let preflight = Gate::new();
        let writer = Gate::new();
        let committed = Gate::new();
        let mut boundary = Boundary::new(inner);
        if original {
            *boundary.original_withheld_renewal.lock().unwrap() = Some((2, renewal.clone()));
        } else {
            boundary.before_entry = Some(renewal.clone());
        }
        boundary.before_withheld = Some(writer.clone());
        boundary.after_withheld = Some(committed.clone());
        let cleanup = [&renewal, &preflight, &writer, &committed]
            .into_iter()
            .map(|g| Cleanup(g.clone()))
            .collect();
        (
            Arc::new(boundary),
            Self {
                renewal,
                preflight,
                writer,
                committed,
                _cleanup: cleanup,
            },
        )
    }
    async fn paused(
        &self,
        store: &Boundary,
        path: &std::path::Path,
        id: &str,
        original: bool,
    ) -> Capture {
        until(|| self.renewal.facts().entered).await;
        let before = observe(&store.inner, path, id);
        dispatched(&before, original);
        before
    }
    fn arm_preflight(&self, store: &Boundary, before: &Capture) {
        // Existing transport authorization read, manager read, then the required
        // coherent read AFTER the atomic Reserved decision. No other request is
        // issued while this one-shot same-thread/execution selector is armed.
        *store.withheld_preflight.lock().unwrap() = Some(WithheldRead {
            thread: before.view.thread.id.clone(),
            execution: before
                .view
                .execution
                .as_ref()
                .unwrap()
                .snapshot
                .execution_id
                .clone(),
            remaining: 3,
            gate: self.preflight.clone(),
        });
    }
    async fn selected(&self, store: &Boundary, before: &Capture, path: &std::path::Path, id: &str) {
        until(|| self.preflight.facts().entered).await;
        self.renewal.release();
        assert_eq!(
            store.withheld_writes.load(Ordering::Acquire),
            0,
            "Reserved cannot write before coherent preflight finishes"
        );
        assert_eq!(observe(&store.inner, path, id), *before);
        self.preflight.release();
        until(|| self.writer.facts().entered).await;
        assert_eq!(
            store.withheld_writes.load(Ordering::Acquire),
            1,
            "one actual typed withdrawal, no retry"
        );
        assert_eq!(observe(&store.inner, path, id), *before);
    }
    fn positive(&self) {
        for gate in [
            &self.renewal,
            &self.preflight,
            &self.writer,
            &self.committed,
        ] {
            cooperative::assert_positive(gate);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_and_saved_use_withdraw_real_work_atomically_hold_owners_reopen_and_reuse() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let catalog = catalog(callback.clone(), Arc::new(Constraint::default()));
    for original_run in [true, false] {
        let (store, seams) = Seams::install(inner.clone(), original_run);
        let host = withheld_host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
        let server = Server::start(&host).await;
        let primary = if original_run {
            original(&server, "withdraw").await
        } else {
            reuse(&server, "withdraw").await
        };
        let before = seams
            .paused(&store, &directory.0, "withdraw", original_run)
            .await;
        uninvoked(&callback);
        seams.arm_preflight(&store, &before);
        let cancellation = open(
            server.address,
            "POST",
            "/threads/withdraw/cancel",
            "",
            "owner",
        )
        .await;
        seams
            .selected(&store, &before, &directory.0, "withdraw")
            .await;
        uninvoked(&callback);
        excluded(&server, &host, &directory.0, "withdraw").await;
        seams.writer.release();
        until(|| seams.committed.facts().entered).await;
        exact_withdrawal(&before, &observe(&inner, &directory.0, "withdraw"));
        uninvoked(&callback);
        excluded(&server, &host, &directory.0, "withdraw").await;
        seams.committed.release();
        assert_eq!(finish(cancellation, Vec::new()).await.status, 200);
        terminal(&finish(primary, Vec::new()).await);
        drained(&host).await;
        seams.positive();
        uninvoked(&callback);
        assert_eq!(store.withheld_writes.load(Ordering::Acquire), 1);
        retire(host, server, false).await;
    }
    let before = observe(&inner, &directory.0, "withdraw");
    let owner = inner.owner_generation();
    drop(inner);
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert!(same_after_reopen(
        &before,
        &observe(&inner, &directory.0, "withdraw"),
        owner + 1
    ));
    let host = withheld_host(inner.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let primary = reuse(&server, "withdraw").await;
    entered(&callback).await;
    callback.gate.release();
    assert_eq!(finish(primary, Vec::new()).await.status, 200);
    drained(&host).await;
    assert_eq!(
        snapshot(&*inner, "withdraw").status,
        ExecutionStatus::Completed
    );
    assert_eq!(raw(&directory.0).0[8].len(), 2);
    retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_entry_wins_and_entered_cancellation_preserves_real_ack_success_and_failure() {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let callbacks = [
        Callback::new(Return::Acknowledge),
        Callback::new(Return::Succeed),
        Callback::new(Return::Fail),
    ];
    let _cleanup: Vec<_> = callbacks.iter().map(|c| Cleanup(c.gate.clone())).collect();
    let store = Arc::new(Boundary::new(inner.clone()));
    let host = withheld_host(
        store.clone(),
        catalog(
            Series::new(callbacks.to_vec()),
            Arc::new(Constraint::default()),
        ),
        Arc::new(Policy::default()),
    );
    let server = Server::start(&host).await;
    for (index, callback) in callbacks.iter().enumerate() {
        let primary = if index == 0 {
            original(&server, "entered").await
        } else {
            reuse(&server, "entered").await
        };
        entered(callback).await;
        let before = snapshot(&*inner, "entered");
        assert_eq!(cancel(&server, "entered").await.status, 200);
        assert!(callback.notified());
        assert_eq!(
            snapshot(&*inner, "entered").status,
            ExecutionStatus::CancelRequested
        );
        terminal(&finish(primary, Vec::new()).await);
        excluded(&server, &host, &directory.0, "entered").await;
        callback.gate.release();
        drained(&host).await;
        let after = snapshot(&*inner, "entered");
        assert_eq!(after.revision, before.revision + 2);
        match index {
            0 => {
                assert_eq!(after.status, ExecutionStatus::Cancelled);
                cooperative::acknowledged(callback);
            }
            1 => {
                assert_eq!(after.status, ExecutionStatus::Cancelled);
                assert_eq!(after.tasks[0].status, TaskStatus::Succeeded);
                let NativeOutcome::Succeeded(result) = cooperative::genuine_success() else {
                    unreachable!()
                };
                assert_eq!(after.tasks[0].result, Some(result));
                assert_eq!(after.values, before.values);
            }
            _ => {
                assert_eq!(after.status, ExecutionStatus::Failed);
                assert_eq!(after.tasks[0].status, TaskStatus::Failed);
                assert_eq!(
                    after.tasks[0].failure.as_deref(),
                    Some("known-native-failure")
                );
                assert!(after.tasks[0].result.is_none());
            }
        }
        assert!(callback.returned.load(Ordering::Acquire));
        assert_eq!(store.withheld_writes.load(Ordering::Acquire), 0);
    }
    retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_authority_privacy_capabilities_and_profile_boundaries_with_revoked_constraint_stop(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert!(inner.permission_original_always_withheld_cancellation_atomic_durable());
    assert!(inner.permission_use_withheld_cancellation_atomic_durable());
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let constraint = Arc::new(Constraint::default());
    let catalog = catalog(callback.clone(), constraint.clone());
    let mut missing_original = Boundary::new(inner.clone());
    missing_original.disable_original_withheld = true;
    assert!(HttpHost::new_with_permission_withheld_cancellation(
        Arc::new(missing_original),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        config(catalog.clone())
    )
    .is_err());
    let mut missing_use = Boundary::new(inner.clone());
    missing_use.disable_use_withheld = true;
    assert!(HttpHost::new_with_permission_withheld_cancellation(
        Arc::new(missing_use),
        Arc::new(Auth),
        Arc::new(Policy::default()),
        config(catalog.clone())
    )
    .is_err());
    let (store, seams) = Seams::install(inner.clone(), true);
    let policy = Arc::new(Policy::default());
    let host = withheld_host(store.clone(), catalog.clone(), policy.clone());
    let server = Server::start(&host).await;
    let metadata = request(&server, "GET", "/b4/capabilities", "").await;
    assert_eq!(metadata.status, 200);
    let descriptor: CapabilityDescriptorV7 = serde_json::from_str(&metadata.body).unwrap();
    descriptor.validate().unwrap();
    let descriptor: Value = serde_json::from_str(&metadata.body).unwrap();
    assert_eq!(descriptor["descriptor_version"], 7);
    for flag in [
        "durable_permission_withheld_cancellation",
        "durable_permission_ready_cancellation",
        "durable_permission_entered_cancellation",
    ] {
        assert_eq!(descriptor["features"][flag], true);
    }
    for flag in [
        "fenced_cancellation",
        "fenced_deletion",
        "automatic_recovery_scan",
        "always_permissions",
        "durable_event_replay",
    ] {
        assert_eq!(descriptor["features"][flag], false);
    }
    let primary = original(&server, "privacy").await;
    let before = seams.paused(&store, &directory.0, "privacy", true).await;
    policy.deny.store(true, Ordering::Release);
    assert!([403, 404].contains(&cancel(&server, "privacy").await.status));
    assert_eq!(observe(&inner, &directory.0, "privacy"), before);
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
    assert!(!foreign.body.contains("permission_id"));
    assert_eq!(observe(&inner, &directory.0, "privacy"), before);
    uninvoked(&callback);
    assert_eq!(store.withheld_writes.load(Ordering::Acquire), 0);
    seams.arm_preflight(&store, &before);
    let cancellation = open(
        server.address,
        "POST",
        "/threads/privacy/cancel",
        "",
        "owner",
    )
    .await;
    seams
        .selected(&store, &before, &directory.0, "privacy")
        .await;
    // The actual failure's typed writer has now been reached. Revoking the
    // shared effect constraint here is strictly AFTER renewed Work exists.
    constraint.0.store(true, Ordering::Release);
    seams.writer.release();
    until(|| seams.committed.facts().entered).await;
    exact_withdrawal(&before, &observe(&inner, &directory.0, "privacy"));
    seams.committed.release();
    assert_eq!(finish(cancellation, Vec::new()).await.status, 200);
    terminal(&finish(primary, Vec::new()).await);
    drained(&host).await;
    seams.positive();
    uninvoked(&callback);
    retire(host, server, false).await;
    drop(store);
    constraint.0.store(false, Ordering::Release);
    for version in [5, 6] {
        let callback = Callback::new(Return::Succeed);
        let _callback = Cleanup(callback.gate.clone());
        let catalog = support::catalog(callback.clone(), constraint.clone());
        let gate = Gate::new();
        let _gate = Cleanup(gate.clone());
        let boundary = Boundary::new(inner.clone());
        *boundary.original_withheld_renewal.lock().unwrap() = Some((2, gate.clone()));
        let store = Arc::new(boundary);
        let host = if version == 5 {
            support::host(store.clone(), catalog.clone(), Arc::new(Policy::default()))
        } else {
            HttpHost::new_with_permission_ready_cancellation(
                store.clone(),
                Arc::new(Auth),
                Arc::new(Policy::default()),
                config(catalog.clone()),
            )
            .unwrap()
        };
        let server = Server::start(&host).await;
        let metadata = request(&server, "GET", "/b4/capabilities", "").await;
        let descriptor: Value = serde_json::from_str(&metadata.body).unwrap();
        assert_eq!(descriptor["descriptor_version"], version);
        assert!(descriptor["features"]
            .get("durable_permission_withheld_cancellation")
            .is_none());
        let id = format!("legacy-{version}");
        let primary = original(&server, &id).await;
        until(|| gate.facts().entered).await;
        let before = observe(&inner, &directory.0, &id);
        assert_eq!(cancel(&server, &id).await.status, 409);
        assert_eq!(observe(&inner, &directory.0, &id), before);
        assert_eq!(store.withheld_writes.load(Ordering::Acquire), 0);
        gate.release();
        entered(&callback).await;
        callback.gate.release();
        assert_eq!(finish(primary, Vec::new()).await.status, 200);
        drained(&host).await;
        assert_eq!(snapshot(&*inner, &id).status, ExecutionStatus::Completed);
        cooperative::assert_positive(&gate);
        retire(host, server, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_second_terminal_journal_fault_physically_rolls_back_faults_and_drains_then_fresh_control(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let catalog = catalog(callback.clone(), Arc::new(Constraint::default()));
    let (store, seams) = Seams::install(inner.clone(), true);
    let host = withheld_host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let primary = original(&server, "fault").await;
    let before = seams.paused(&store, &directory.0, "fault", true).await;
    let execution = &before
        .view
        .execution
        .as_ref()
        .unwrap()
        .snapshot
        .execution_id;
    let revision = before.view.execution.as_ref().unwrap().snapshot.revision + 2;
    let resource = before.view.thread.resource_revision + 2;
    let ddl = format!("CREATE TRIGGER withheld_terminal_guard BEFORE INSERT ON journal WHEN NEW.kind = 'commit' AND NEW.execution_id = '{execution}' AND NEW.thread_id = 'fault' AND NEW.revision = x'{revision:016x}' AND EXISTS (SELECT 1 FROM executions WHERE id = NEW.execution_id AND revision = NEW.revision) AND EXISTS (SELECT 1 FROM threads WHERE id = NEW.thread_id AND active_execution_id = NEW.execution_id AND resource_revision = x'{resource:016x}') BEGIN SELECT RAISE(ABORT, 'withheld_second_post_update_journal_fault'); END");
    let db = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    db.execute_batch(&ddl).unwrap();
    let actual: String = db
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE name='withheld_terminal_guard' AND type='trigger'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(actual, ddl);
    let presence: i64 = db.query_row("SELECT count(*) FROM sqlite_schema WHERE name='withheld_terminal_guard' AND type='trigger'", [], |r| r.get(0)).unwrap();
    assert_eq!(presence, 1);
    seams.arm_preflight(&store, &before);
    let cancellation = open(server.address, "POST", "/threads/fault/cancel", "", "owner").await;
    seams.selected(&store, &before, &directory.0, "fault").await;
    excluded(&server, &host, &directory.0, "fault").await;
    seams.writer.release();
    assert_eq!(finish(cancellation, Vec::new()).await.status, 503);
    assert_eq!(
        observe(&inner, &directory.0, "fault"),
        before,
        "all nine physical tables and coherent state rolled back"
    );
    assert_eq!(
        store.withheld_writes.load(Ordering::Acquire),
        1,
        "no resend after writer error"
    );
    assert!(!seams.committed.facts().entered);
    uninvoked(&callback);
    assert!(host.shutdown_handle().status().faulted);
    let response = finish(primary, Vec::new()).await;
    assert!(!response.body.contains(r#""output":{"cancelled":true}"#));
    drained(&host).await;
    db.execute_batch("DROP TRIGGER withheld_terminal_guard")
        .unwrap();
    let presence: i64 = db.query_row("SELECT count(*) FROM sqlite_schema WHERE name='withheld_terminal_guard' AND type='trigger'", [], |r| r.get(0)).unwrap();
    assert_eq!(presence, 0);
    db.close().unwrap();
    for gate in [&seams.renewal, &seams.preflight, &seams.writer] {
        cooperative::assert_positive(gate);
    }
    retire(host, server, true).await;
    drop(store);
    drop(inner);
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    inner.readiness().unwrap();
    // Fresh thread under the new owner, without retrying or recovering the old run.
    let (store, seams) = Seams::install(inner.clone(), true);
    let host = withheld_host(store.clone(), catalog, Arc::new(Policy::default()));
    let server = Server::start(&host).await;
    let primary = original(&server, "fresh-control").await;
    let before = seams
        .paused(&store, &directory.0, "fresh-control", true)
        .await;
    seams.arm_preflight(&store, &before);
    let cancellation = open(
        server.address,
        "POST",
        "/threads/fresh-control/cancel",
        "",
        "owner",
    )
    .await;
    seams
        .selected(&store, &before, &directory.0, "fresh-control")
        .await;
    seams.writer.release();
    until(|| seams.committed.facts().entered).await;
    exact_withdrawal(&before, &observe(&inner, &directory.0, "fresh-control"));
    seams.committed.release();
    assert_eq!(finish(cancellation, Vec::new()).await.status, 200);
    terminal(&finish(primary, Vec::new()).await);
    drained(&host).await;
    seams.positive();
    uninvoked(&callback);
    retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_admitted_withdrawal_keeps_assembly_claim_capacity_and_store_through_commit_and_drain(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Succeed);
    let _callback = Cleanup(callback.gate.clone());
    let constraint = Arc::new(Constraint::default());
    let catalog = catalog(callback.clone(), constraint.clone());
    let (store, seams) = Seams::install(inner.clone(), true);
    let weak_store = Arc::downgrade(&store);
    let weak_inner = Arc::downgrade(&inner);
    let weak_callback = Arc::downgrade(&callback);
    let weak_constraint = Arc::downgrade(&constraint);
    let owner = inner.owner_generation();
    let host = withheld_host(store.clone(), catalog.clone(), Arc::new(Policy::default()));
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    let primary = original(&server, "abandon").await;
    let before = seams.paused(&store, &directory.0, "abandon", true).await;
    seams.arm_preflight(&store, &before);
    let cancellation = open(
        server.address,
        "POST",
        "/threads/abandon/cancel",
        "",
        "owner",
    )
    .await;
    seams
        .selected(&store, &before, &directory.0, "abandon")
        .await;
    // The owned command has reached its actual typed writer. Both clients now
    // disappear before commit, so neither future can drive the operation onward.
    drop(cancellation);
    drop(primary);
    excluded(&server, &host, &directory.0, "abandon").await;
    uninvoked(&callback);
    seams.writer.release();
    until(|| seams.committed.facts().entered).await;
    let held = observe(&inner, &directory.0, "abandon");
    exact_withdrawal(&before, &held);
    excluded(&server, &host, &directory.0, "abandon").await;
    uninvoked(&callback);
    assert_eq!(store.withheld_writes.load(Ordering::Acquire), 1);
    server.stop().await;
    drop(host);
    drop(catalog);
    drop(callback);
    drop(constraint);
    drop(store);
    drop(inner);
    assert!(weak_store.upgrade().is_some() && weak_inner.upgrade().is_some());
    assert!(weak_callback.upgrade().is_some() && weak_constraint.upgrade().is_some());
    assert!(shutdown.status().active_owners > 0);
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    seams.committed.release();
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    seams.positive();
    assert!(weak_store.upgrade().is_none() && weak_inner.upgrade().is_none());
    assert!(weak_callback.upgrade().is_none() && weak_constraint.upgrade().is_none());
    let reopened = SqliteStore::open(&directory.0).unwrap();
    reopened.readiness().unwrap();
    assert!(same_after_reopen(
        &held,
        &observe(&reopened, &directory.0, "abandon"),
        owner + 1
    ));
}
