//! Actual owned workers and durable refusal boundaries of the narrow reuse lane.
mod permission_use_support;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_kernel::{authorize_ready, new_execution, transition, PermissionAuthorizationCommand};
use b4_runtime::{permissions::*, *};
use permission_use_support::*;
use serde_json::{json, Map};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{oneshot, Notify},
};
use tower::ServiceExt;

fn principal() -> Principal {
    Principal {
        application: APP.into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
struct RecoverPolicy {
    deny: AtomicBool,
    calls: AtomicUsize,
}
impl ResourcePolicy for RecoverPolicy {
    fn authorize(&self, actual: &Principal, resource: &ResourceContext) -> bool {
        if resource.operation == Operation::Recover {
            self.calls.fetch_add(1, Ordering::SeqCst);
            return !self.deny.load(Ordering::SeqCst)
                && actual == &principal()
                && resource.origin.as_ref() == Some(&origin());
        }
        actual == &principal() && resource.origin.as_ref() == Some(&origin())
    }
}
struct Resolver(Arc<AtomicUsize>);
impl StartupPrincipalResolver for Resolver {
    fn resolve(&self, actual: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        assert_eq!(actual, &origin());
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(principal())
    }
}
struct Operator {
    original: ThreadView,
    calls: Arc<AtomicUsize>,
}
impl RecoveryConfinementAuthority for Operator {
    fn authorize(
        &self,
        actual: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        assert_eq!(actual, &self.original);
        assert_eq!(reason, RecoveryConfinementReason::DispatchNotAuthorized);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Count(Arc<AtomicUsize>);
impl NativeEffect for Count {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(result()))
    }
}
#[tokio::test]
async fn permission_use_prior_owner_recovery_and_authorized_confinement_are_unsupported() {
    let callbacks = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::new(Arc::new(Count(callbacks.clone())), false);
    let before = fixture.capture();
    let current = &before.view.execution.as_ref().unwrap().snapshot;
    assert_eq!(current.status, ExecutionStatus::Accepted);
    assert_eq!(current.revision, 0);
    assert_eq!(current.definition, fixture.definition);
    assert_eq!(
        current.owner_generation + 1,
        fixture.store.owner_generation()
    );
    let policy = Arc::new(RecoverPolicy {
        deny: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    let host = fixture.host(policy.clone());
    let target = RecoveryTarget::from_view(&before.view).unwrap();
    let admission = host
        .admit_recovery(principal(), target.clone())
        .await
        .unwrap_err();
    let response = host.recover_execution(principal(), target).await;
    let response_status = response.status();
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let recover_unchanged = before == fixture.capture();
    let recover_policy_calls = policy.calls.load(Ordering::SeqCst);
    policy.deny.store(true, Ordering::SeqCst);
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let operator_calls = Arc::new(AtomicUsize::new(0));
    let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver(resolver_calls.clone()));
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(Operator {
        original: before.view.clone(),
        calls: operator_calls.clone(),
    });
    let refusal = host
        .confine_prior_owner(
            RecoveryConfinementTarget::from_view(&before.view).unwrap(),
            &resolver,
            &authority,
        )
        .await
        .unwrap_err();
    let unchanged = before == fixture.capture();
    let policy_calls = policy.calls.load(Ordering::SeqCst);
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    let drained = shutdown.wait_quiescent(WAIT).await.unwrap();
    drop(host);
    drop(shutdown);
    drop(authority);
    drop(resolver);
    fixture.retire();
    assert_eq!(admission.reason(), RecoveryRefusal::Unsupported);
    assert!(admission.adoption().is_none());
    assert_eq!(response_status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["message"],
        "recovery_unsupported"
    );
    assert_eq!(recover_policy_calls, 2);
    assert_eq!(
        refusal.reason(),
        RecoveryConfinementRefusal::UnsupportedHost
    );
    assert!(refusal.confirmed_receipt().is_none());
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 1);
    assert_eq!(operator_calls.load(Ordering::SeqCst), 1);
    assert_eq!(policy_calls, 3);
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    assert!(recover_unchanged && unchanged);
    assert_eq!(drained, ShutdownOutcome::Quiescent { faulted: false });
}

#[derive(Default)]
struct Gate {
    entered: Notify,
    released: Mutex<bool>,
    changed: Condvar,
    calls: AtomicUsize,
}
impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
impl NativeEffect for Gate {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let (released, _) = self
            .changed
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(20),
                |released| !*released,
            )
            .unwrap();
        assert!(*released, "bounded callback gate expired");
        Ok(NativeOutcome::Succeeded(result()))
    }
}
async fn listener(
    host: &HttpHost<b4_sqlite::SqliteStore>,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = host.router();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    (address, stop, task)
}
async fn stop_listener(stop: oneshot::Sender<()>, task: tokio::task::JoinHandle<()>) {
    let _ = stop.send(());
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
}
async fn streaming_request(address: std::net::SocketAddr, thread: &ThreadId) -> TcpStream {
    let body = json!({"route":ROUTE,"input":{"month":"2026-09"}}).to_string();
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("POST /threads/{thread}/runs/stream HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer owner\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    stream
}

// Replay actual authorization/Dispatch (and optional Complete) independently.
// The Use remains an authorization witness even after callback settlement fails.
fn check_reuse(fixture: &Fixture, before: &Capture, after: &Capture, completed: bool) {
    let current = &after.view.execution.as_ref().unwrap().snapshot;
    let journal = &after.current_journal;
    assert_eq!(journal.len(), if completed { 5 } else { 4 });
    assert_eq!(after.raw.0[8].len(), 1);
    let usage = PermissionUse::decode_record(
        text(&after.raw.0[8][0][9]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let decision = PermissionDecision::decode_record(
        text(&after.raw.0[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let initial = new_execution(
        current.execution_id.clone(),
        current.turn_id.clone(),
        fixture.store.owner_generation(),
        fixture.definition.clone(),
        Map::from_iter([("input".into(), json!({"month":"2026-09"}))]),
        facts(journal[0].updated_at_ms),
    )
    .unwrap();
    let start = transition(
        &initial,
        KernelCommand::Start,
        facts(journal[1].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let task = TaskId::new(TASK).unwrap();
    let command =
        PermissionAuthorizationCommand::capture(&start, task.clone(), usage.use_id().clone())
            .unwrap();
    let authorized = authorize_ready(&start, &command, facts(journal[2].updated_at_ms))
        .unwrap()
        .snapshot;
    assert_eq!(usage.before_snapshot(), &start);
    assert_eq!(usage.facts(), &facts(journal[2].updated_at_ms));
    assert_eq!(usage.permission_id(), decision.permission_id());
    assert_eq!(usage.thread_id(), &fixture.thread);
    assert_eq!(usage.thread_generation(), &after.view.thread.generation);
    assert_eq!(usage.execution_id(), &current.execution_id);
    assert_eq!(usage.task_id(), &task);
    assert_eq!(usage.committed_revision(), 2);
    assert_eq!(current.attempt_history.len(), 1);
    let dispatched = transition(
        &authorized,
        KernelCommand::Dispatch {
            task_id: task.clone(),
            attempt_id: current.attempt_history[0].clone(),
        },
        facts(journal[3].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let mut expected = vec![
        JournalEntry::from(&initial),
        JournalEntry::from(&start),
        JournalEntry::from(&authorized),
        JournalEntry::from(&dispatched),
    ];
    let final_snapshot = if completed {
        let complete = transition(
            &dispatched,
            KernelCommand::Complete {
                task_id: task,
                attempt_id: current.attempt_history[0].clone(),
                result: result(),
            },
            facts(journal[4].updated_at_ms),
        )
        .unwrap()
        .snapshot;
        expected.push(JournalEntry::from(&complete));
        complete
    } else {
        dispatched
    };
    assert_eq!(current, &final_snapshot);
    assert_eq!(*journal, expected);
    assert_eq!(current.revision, if completed { 4 } else { 3 });
    assert_eq!(
        current.status,
        if completed {
            ExecutionStatus::Completed
        } else {
            ExecutionStatus::Running
        }
    );
    assert_eq!(
        current.tasks[0].status,
        if completed {
            TaskStatus::Succeeded
        } else {
            TaskStatus::InFlight
        }
    );
    assert_eq!(after.original, before.original);
    assert_eq!(after.original_journal, before.original_journal);
    assert_eq!(after.raw.0[4..8], before.raw.0[4..8]);
    assert_eq!(after.raw.0[3][..before.raw.0[3].len()], before.raw.0[3]);
}

#[tokio::test]
async fn permission_use_detached_tcp_viewer_keeps_worker_owned_until_complete() {
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    let fixture = Fixture::new(gate.clone(), true);
    let before = fixture.capture();
    let host = fixture.host(Arc::new(Policy));
    let (address, stop, task) = listener(&host).await;
    let viewer = streaming_request(address, &fixture.thread).await;
    tokio::time::timeout(WAIT, gate.entered.notified())
        .await
        .unwrap();
    let during = fixture.capture();
    drop(viewer);
    // Capacity belongs to the actual worker even after its viewer disappears.
    let busy = exchange(
        address,
        &format!("/threads/{}/runs/wait", fixture.thread),
        &json!({"route":ROUTE,"input":{}}).to_string(),
    )
    .await;
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    let held = shutdown
        .wait_quiescent(Duration::from_millis(25))
        .await
        .unwrap();
    let store_retained = Arc::strong_count(&fixture.store) > 1;
    let still_dispatch = during == fixture.capture();
    drop(release);
    let drained = shutdown.wait_quiescent(WAIT).await.unwrap();
    stop_listener(stop, task).await;
    let after = fixture.capture();
    check_reuse(&fixture, &before, &during, false);
    check_reuse(&fixture, &before, &after, true);
    // Callback completion cannot rewrite the authorization-only Use witness.
    assert_eq!(during.raw.0[8], after.raw.0[8]);
    drop(host);
    drop(shutdown);
    fixture.retire();
    assert_eq!(Arc::strong_count(&gate), 1);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
    assert!(
        matches!(held, ShutdownOutcome::Incomplete { active_owners, faulted: false } if active_owners > 0)
    );
    assert!(store_retained && still_dispatch);
    assert_eq!(busy.0, 503);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&busy.1).unwrap()["error"]["message"],
        "capacity_exhausted"
    );
    assert_eq!(drained, ShutdownOutcome::Quiescent { faulted: false });
}

#[derive(Default)]
struct AbortComplete {
    path: Mutex<Option<PathBuf>>,
    captured: Mutex<Option<(Raw, String)>>,
    calls: AtomicUsize,
}
impl NativeEffect for AbortComplete {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let path = self.path.lock().unwrap().clone().unwrap();
        let before = raw(&path);
        let execution = invocation.execution_id.as_str().replace('\'', "''");
        let completed = serde_json::to_string(&ExecutionStatus::Completed).unwrap();
        let sql = format!("CREATE TRIGGER lifecycle_abort_complete BEFORE INSERT ON journal WHEN NEW.kind='commit' AND NEW.execution_id='{execution}' AND NEW.revision=X'0000000000000004' AND NEW.status='{completed}' AND EXISTS (SELECT 1 FROM executions WHERE id=NEW.execution_id AND revision=NEW.revision) BEGIN SELECT RAISE(ABORT, 'lifecycle_exact_complete_abort'); END");
        let connection = rusqlite::Connection::open_with_flags(
            path.join("runtime.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )
        .unwrap();
        connection.execute_batch(&sql).unwrap();
        let count: i64 = connection.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='lifecycle_abort_complete' AND sql=?1", [&sql], |row| row.get(0)).unwrap();
        let actual: String = connection.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='lifecycle_abort_complete'", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 1);
        assert_eq!(actual, sql);
        connection.close().unwrap();
        *self.captured.lock().unwrap() = Some((before, sql));
        Ok(NativeOutcome::Succeeded(result()))
    }
}
#[tokio::test]
async fn permission_use_complete_journal_abort_rolls_back_without_callback_replay() {
    let callback = Arc::new(AbortComplete::default());
    let fixture = Fixture::new(callback.clone(), true);
    *callback.path.lock().unwrap() = Some(fixture.path.clone());
    let before = fixture.capture();
    let host = fixture.host(Arc::new(Policy));
    let (address, stop, task) = listener(&host).await;
    let response = exchange(
        address,
        &format!("/threads/{}/runs/wait", fixture.thread),
        &json!({"route":ROUTE,"input":{"month":"2026-09"}}).to_string(),
    )
    .await;
    let ready = host
        .router()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let readiness = ready.status();
    let ready_body = to_bytes(ready.into_body(), 4096).await.unwrap();
    let shutdown = host.shutdown_handle();
    let faulted = shutdown.status().faulted;
    shutdown.begin_shutdown();
    let drained = shutdown.wait_quiescent(WAIT).await.unwrap();
    stop_listener(stop, task).await;
    let after = fixture.capture();
    let (callback_raw, sql) = callback.captured.lock().unwrap().take().unwrap();
    // A callback succeeded, but the actual transaction's Complete insert aborted:
    // no execution, journal, thread-revision, or authority cell may survive it.
    assert_eq!(after.raw, callback_raw);
    check_reuse(&fixture, &before, &after, false);
    assert!(after.view.execution.as_ref().unwrap().snapshot.tasks[0]
        .result
        .is_none());
    let connection = rusqlite::Connection::open_with_flags(
        fixture.path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .unwrap();
    let installed: i64 = connection.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='lifecycle_abort_complete' AND sql=?1", [&sql], |row| row.get(0)).unwrap();
    connection
        .execute_batch("DROP TRIGGER lifecycle_abort_complete")
        .unwrap();
    connection.close().unwrap();
    drop(host);
    drop(shutdown);
    fixture.retire();
    assert_eq!(installed, 1);
    assert_eq!(Arc::strong_count(&callback), 1);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_ne!(response.0, 200);
    assert!(!response.1.contains("\"published\":true"));
    assert_eq!(readiness, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ready_body).unwrap(),
        json!({"status":"unready"})
    );
    assert!(faulted);
    assert_eq!(drained, ShutdownOutcome::Quiescent { faulted: true });
}
