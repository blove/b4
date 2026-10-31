use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

#[path = "native_support/store.rs"]
mod interception;
#[path = "native_support/mod.rs"]
mod support;
use interception::InterceptStore;
use support::*;

struct Validator {
    gate: Arc<Gate>,
    calls: Arc<AtomicUsize>,
}
impl InputValidator for Validator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.gate.hold();
        Ok(())
    }
}
struct Callback {
    gates: Vec<Arc<Gate>>,
    calls: Arc<Vec<AtomicUsize>>,
    outcomes: Vec<Result<NativeOutcome, EffectUncertain>>,
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        let index = if call.task_id.as_str() == "first" {
            0
        } else {
            1
        };
        assert_eq!(context.origin.application, "native-test");
        self.calls[index].fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.gates.get(index) {
            gate.hold();
        }
        self.outcomes[index].clone()
    }
}
fn calls(count: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect())
}
fn success(name: &str) -> Result<NativeOutcome, EffectUncertain> {
    Ok(NativeOutcome::Succeeded(TaskResult {
        output: json!(name),
        writes: vec![StateWrite::Append {
            field: "joined".into(),
            items: vec![json!(name)],
        }],
    }))
}
fn route(count: usize) -> Definition {
    let all = [
        ("first", false, RecoveryPolicy::Pure),
        ("second", false, RecoveryPolicy::Pure),
    ];
    definition("/native#workflow", "native-v1", &all[..count])
}
const RUN: &str = r#"{"route":"/native#workflow","input":{"caller":"original"}}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_abandoned_preheader_validator_retains_registry_store_capacity_and_claim() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let validation_calls = Arc::new(AtomicUsize::new(0));
    let effect_calls = calls(1);
    let app = application(
        route(1),
        Arc::new(Callback {
            gates: vec![],
            calls: effect_calls.clone(),
            outcomes: vec![success("first")],
        }),
        Some(Arc::new(Validator {
            gate: gate.clone(),
            calls: validation_calls.clone(),
        })),
    );
    let weak_app = Arc::downgrade(&app);
    let store = Arc::new(MemoryStore::new());
    let weak_store = Arc::downgrade(&store);
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), false, 2),
    )
    .unwrap();
    let server = Server::start(&host, None).await;
    let first = open(server.address, "POST", "/threads/held/runs/wait", RUN).await;
    gate.entered().await;
    drop(first);
    assert!(store
        .thread_view(&ThreadId::new("held").unwrap())
        .unwrap()
        .is_none());
    assert_eq!(
        exchange(server.address, "POST", "/threads/held/runs/wait", RUN)
            .await
            .status,
        409
    );
    let second = open(server.address, "POST", "/threads/second/runs/wait", RUN).await;
    until(|| validation_calls.load(Ordering::SeqCst) == 2).await;
    drop(second);
    assert_eq!(
        exchange(server.address, "POST", "/threads/third/runs/wait", RUN)
            .await
            .status,
        503
    );
    assert!(store
        .thread_view(&ThreadId::new("second").unwrap())
        .unwrap()
        .is_none());
    assert_eq!(effect_calls[0].load(Ordering::SeqCst), 0);
    server.stop().await;
    drop(host);
    drop(store);
    drop(app);
    assert!(weak_store.upgrade().is_some());
    assert!(weak_app.upgrade().is_some());
    gate.release();
    until(|| weak_store.upgrade().is_none() && weak_app.upgrade().is_none()).await;
    assert_eq!(effect_calls[0].load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_primary_and_attached_disconnect_leave_native_callback_owned_until_real_success() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let effect_calls = calls(1);
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let app = application(
        route(1),
        Arc::new(Callback {
            gates: vec![gate.clone()],
            calls: effect_calls.clone(),
            outcomes: vec![success("first")],
        }),
        None,
    );
    let weak_app = Arc::downgrade(&app);
    let weak_store = Arc::downgrade(&store);
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), true, 1),
    )
    .unwrap();
    let server = Server::start(&host, None).await;
    let mut primary = open(
        server.address,
        "POST",
        "/threads/disconnected/runs/stream",
        RUN,
    )
    .await;
    let _ = prefix(&mut primary, b"\r\n\r\n").await;
    gate.entered().await;
    let mut attached = open(
        server.address,
        "GET",
        "/threads/disconnected/runs/stream",
        "",
    )
    .await;
    let _ = prefix(&mut attached, b"event: state").await;
    drop(primary);
    drop(attached);
    assert_eq!(
        exchange(server.address, "DELETE", "/threads/disconnected", "")
            .await
            .status,
        409
    );
    assert_eq!(
        exchange(server.address, "POST", "/threads/other/runs/wait", RUN)
            .await
            .status,
        503
    );
    assert_eq!(
        snapshot(store.as_ref(), "disconnected").tasks[0].status,
        TaskStatus::InFlight
    );
    server.stop().await;
    drop(host);
    drop(store);
    drop(app);
    assert!(weak_store.upgrade().is_some());
    assert!(weak_app.upgrade().is_some());
    gate.release();
    until(|| weak_store.upgrade().is_none() && weak_app.upgrade().is_none()).await;
    // Weak zero precedes the last Arc destructor finishing. Prove the actual
    // SQLite close by acquiring its directory lock, without assuming that gap.
    let reopened = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory.0) {
                Ok(store) => break store,
                Err(StoreError::AlreadyOwned) => tokio::task::yield_now().await,
                Err(_) => panic!("native store did not reopen after physical close"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.owner_generation(), 2);
    let final_snapshot = snapshot(&reopened, "disconnected");
    assert_eq!(final_snapshot.status, ExecutionStatus::Completed);
    assert_eq!(final_snapshot.values["joined"], json!(["first"]));
    assert_eq!(effect_calls[0].load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_cancel_fans_out_before_late_native_failure_and_preserves_actual_failed_record() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let effect_calls = calls(1);
    let store = Arc::new(MemoryStore::new());
    let app = application(
        route(1),
        Arc::new(Callback {
            gates: vec![gate.clone()],
            calls: effect_calls.clone(),
            outcomes: vec![Ok(NativeOutcome::Failed("native public failure".into()))],
        }),
        None,
    );
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app, false, 1),
    )
    .unwrap();
    let server = Server::start(&host, None).await;
    let mut primary = open(
        server.address,
        "POST",
        "/threads/late-failure/runs/stream",
        RUN,
    )
    .await;
    let primary_prefix = prefix(&mut primary, b"\r\n\r\n").await;
    gate.entered().await;
    let mut attached = open(
        server.address,
        "GET",
        "/threads/late-failure/runs/stream",
        "",
    )
    .await;
    let attached_prefix = prefix(&mut attached, b"event: state").await;
    assert_eq!(
        exchange(server.address, "POST", "/threads/late-failure/cancel", "")
            .await
            .status,
        200
    );
    cancelled(&finish(primary, primary_prefix).await);
    cancelled(&finish(attached, attached_prefix).await);
    assert_eq!(
        exchange(server.address, "POST", "/threads/other/runs/wait", RUN)
            .await
            .status,
        503
    );
    assert_eq!(
        exchange(server.address, "DELETE", "/threads/late-failure", "")
            .await
            .status,
        409
    );
    let before = snapshot(store.as_ref(), "late-failure");
    assert_eq!(before.status, ExecutionStatus::CancelRequested);
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    gate.release();
    until(|| snapshot(store.as_ref(), "late-failure").status == ExecutionStatus::Failed).await;
    let settled = snapshot(store.as_ref(), "late-failure");
    assert_eq!(settled.tasks[0].status, TaskStatus::Failed);
    assert_eq!(
        settled.tasks[0].failure.as_deref(),
        Some("native public failure")
    );
    assert_eq!(settled.failure.as_deref(), Some("native public failure"));
    assert_eq!(effect_calls[0].load(Ordering::SeqCst), 1);
    assert_eq!(
        exchange(server.address, "GET", "/readyz", "").await.status,
        200
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_cached_cancel_survives_native_uncertainty_while_entered_sibling_keeps_store_owned() {
    let unknown = Arc::new(Gate::default());
    let sibling = Arc::new(Gate::default());
    let _a = Release(unknown.clone());
    let _b = Release(sibling.clone());
    let effect_calls = calls(2);
    let store = Arc::new(MemoryStore::new());
    let weak = Arc::downgrade(&store);
    let app = application(
        route(2),
        Arc::new(Callback {
            gates: vec![unknown.clone(), sibling.clone()],
            calls: effect_calls.clone(),
            outcomes: vec![Err(EffectUncertain), success("second")],
        }),
        None,
    );
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app, false, 1),
    )
    .unwrap();
    let server = Server::start(&host, None).await;
    let mut primary = open(
        server.address,
        "POST",
        "/threads/uncertain/runs/stream",
        RUN,
    )
    .await;
    let p = prefix(&mut primary, b"\r\n\r\n").await;
    unknown.entered().await;
    sibling.entered().await;
    let mut attached = open(server.address, "GET", "/threads/uncertain/runs/stream", "").await;
    let a = prefix(&mut attached, b"event: state").await;
    assert_eq!(
        exchange(server.address, "POST", "/threads/uncertain/cancel", "")
            .await
            .status,
        200
    );
    unknown.release();
    until(|| effect_calls[0].load(Ordering::SeqCst) == 1).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if exchange(server.address, "GET", "/readyz", "").await.status == 503 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancelled(&finish(primary, p).await);
    cancelled(&finish(attached, a).await);
    let before = snapshot(store.as_ref(), "uncertain");
    assert_eq!(before.status, ExecutionStatus::CancelRequested);
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(before.tasks[1].status, TaskStatus::InFlight);
    server.stop().await;
    drop(host);
    drop(store);
    assert!(weak.upgrade().is_some());
    // Keep a temporary read handle until the actual sibling outcome is durable.
    let retained = weak.upgrade().unwrap();
    sibling.release();
    until(|| snapshot(retained.as_ref(), "uncertain").tasks[1].status == TaskStatus::Succeeded)
        .await;
    let settled = snapshot(retained.as_ref(), "uncertain");
    assert_eq!(settled.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(settled.tasks[1].result.as_ref().unwrap().output, "second");
    assert!(!settled.values.contains_key("joined"));
    assert_eq!(settled.attempts_used, 2);
    drop(retained);
    until(|| weak.upgrade().is_none()).await;
    assert!(effect_calls.iter().all(|c| c.load(Ordering::SeqCst) == 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_retains_exact_native_success_through_eight_completion_attempts_without_callback_replay(
) {
    let ready = Arc::new(Gate::default());
    let held = Arc::new(Gate::default());
    let _a = Release(ready.clone());
    let _b = Release(held.clone());
    let effect_calls = calls(2);
    let proposals = Arc::new(Mutex::new(Vec::<TaskResult>::new()));
    let captured = proposals.clone();
    let inner = Arc::new(MemoryStore::new());
    let store = Arc::new(InterceptStore {
        inner: inner.clone(),
        before_commit: Some(Arc::new(move |plan| {
            if let Some(result) = plan
                .snapshot
                .tasks
                .iter()
                .find(|t| t.spec.id.as_str() == "first" && t.status == TaskStatus::Succeeded)
                .and_then(|t| t.result.clone())
            {
                captured.lock().unwrap().push(result);
                return Err(StoreError::RevisionConflict);
            }
            Ok(())
        })),
        after_approval: None,
    });
    let app = application(
        route(2),
        Arc::new(Callback {
            gates: vec![ready.clone(), held.clone()],
            calls: effect_calls.clone(),
            outcomes: vec![success("first"), success("second")],
        }),
        None,
    );
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app, false, 1),
    )
    .unwrap();
    let server = Server::start(&host, None).await;
    let mut primary = open(server.address, "POST", "/threads/retained/runs/stream", RUN).await;
    let p = prefix(&mut primary, b"\r\n\r\n").await;
    ready.entered().await;
    held.entered().await;
    ready.release();
    until(|| proposals.lock().unwrap().len() == 8).await;
    assert_eq!(
        exchange(server.address, "GET", "/readyz", "").await.status,
        503
    );
    let response = finish(primary, p).await;
    assert!(!frames(&response.body)
        .iter()
        .any(|(event, _)| event == "done"));
    let before = snapshot(inner.as_ref(), "retained");
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(before.tasks[1].status, TaskStatus::InFlight);
    {
        let values = proposals.lock().unwrap();
        assert_eq!(values.len(), 8);
        assert!(values.iter().all(|v| v == &values[0]));
        assert_eq!(values[0].output, "first");
    }
    held.release();
    until(|| snapshot(inner.as_ref(), "retained").tasks[1].status == TaskStatus::Succeeded).await;
    let after = snapshot(inner.as_ref(), "retained");
    assert_eq!(after.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(after.tasks[1].result.as_ref().unwrap().output, "second");
    assert_eq!(after.attempts_used, 2);
    assert!(!after.values.contains_key("joined"));
    assert!(effect_calls.iter().all(|c| c.load(Ordering::SeqCst) == 1));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t001_create_read_thread_round_trips_metadata_and_timestamps_after_reopen() {
    const DEADLINE: Duration = Duration::from_secs(10);
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let effect_calls = calls(1);
    let make_app = || {
        application(
            route(1),
            Arc::new(Callback {
                gates: vec![],
                calls: effect_calls.clone(),
                outcomes: vec![success("first")],
            }),
            None,
        )
    };
    // The creation journal has no execution ID, so inspect it independently
    // through a short-lived read-only connection rather than Store::journal.
    let read_journal = || {
        let connection = rusqlite::Connection::open_with_flags(
            directory_path.join("runtime.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        connection.busy_timeout(Duration::from_secs(5)).unwrap();
        let execution_count: i64 = connection
            .query_row("SELECT count(*) FROM executions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            execution_count, 0,
            "create/read must never admit an execution"
        );
        let mut statement = connection
            .prepare("SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence")
            .unwrap();
        let rows = statement
            .query_map([], |row| {
                (0..9)
                    .map(|column| row.get::<_, rusqlite::types::Value>(column))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows
    };
    let app = make_app();
    let weak_app = Arc::downgrade(&app);
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak_store = Arc::downgrade(&store);
    let owner_generation = store.owner_generation();
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), true, 1),
    )
    .unwrap();
    let server = Server::start(&host, None).await;
    let metadata = json!({
        "label": "初めてのスレッド 🧵",
        "nested": {"nullable": null, "items": [true, false, 7, {"name": "café"}]},
        "tenant": "ordinary-client-data"
    });
    let request = json!({"metadata": metadata.clone()}).to_string();
    let before_post = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let created = tokio::time::timeout(
        DEADLINE,
        exchange(server.address, "POST", "/threads", &request),
    )
    .await
    .expect("create request exceeded its complete TCP deadline");
    let after_post = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    assert_eq!(created.status, 200);
    let body = created.json();
    assert_eq!(body.as_object().unwrap().len(), 5);
    let id = body["thread_id"].as_str().unwrap();
    uuid::Uuid::parse_str(id).expect("server must assign a valid UUID");
    let thread_id = ThreadId::new(id).unwrap();
    let view = store.thread_view(&thread_id).unwrap().unwrap();
    assert!(view.execution.is_none());
    let record = view.thread.clone();
    assert_eq!(record.id, thread_id);
    assert!(record.active_execution_id.is_none());
    assert_eq!(record.metadata, metadata.as_object().unwrap().clone());
    assert_eq!(
        record.origin,
        Some(ThreadOrigin {
            application: "native-test".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
    );
    assert_ne!(record.generation.as_str(), "0".repeat(64));
    assert_eq!(record.resource_revision, 0);
    assert!((before_post..=after_post).contains(&u128::from(record.created_at_ms)));
    // Convert the actual stored epoch using only the existing formatting
    // feature. RFC3339 may trim trailing fractional zeros.
    let expected_timestamp = time::OffsetDateTime::from_unix_timestamp(
        i64::try_from(record.created_at_ms / 1_000).unwrap(),
    )
    .unwrap()
    .replace_nanosecond(u32::try_from(record.created_at_ms % 1_000).unwrap() * 1_000_000)
    .unwrap()
    .format(&time::format_description::well_known::Rfc3339)
    .unwrap();
    assert_eq!(
        body,
        json!({
            "thread_id": id,
            "created_at": expected_timestamp,
            "updated_at": expected_timestamp,
            "metadata": metadata,
            "status": "idle"
        })
    );
    let path = format!("/threads/{id}");
    for _ in 0..2 {
        let read = tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &path, ""))
            .await
            .expect("read request exceeded its complete TCP deadline");
        assert_eq!(read.status, 200);
        assert_eq!(read.body, created.body);
    }
    let journal = read_journal();
    use rusqlite::types::Value as SqlValue;
    assert_eq!(
        journal,
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("thread".into()),
            SqlValue::Text(id.into()),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Blob(owner_generation.to_be_bytes().to_vec()),
            SqlValue::Null,
            SqlValue::Blob(record.created_at_ms.to_be_bytes().to_vec()),
            SqlValue::Null,
        ]]
    );
    assert_eq!(effect_calls[0].load(Ordering::SeqCst), 0);
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), view);
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .expect("host quiescence exceeded deadline")
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_journal(), journal);
    // stop joins the aborted listener; the host outcome above proves quiescence.
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    drop(shutdown);
    drop(host);
    drop(store);
    drop(app);
    tokio::time::timeout(DEADLINE, async {
        while weak_store.upgrade().is_some() || weak_app.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired host retained store or application ownership");
    // A successful real owner acquisition is the positive release proof; weak
    // zero alone can precede completion of the last store destructor.
    let reopened = tokio::time::timeout(DEADLINE, async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory.0) {
                Ok(store) => break Arc::new(store),
                Err(StoreError::AlreadyOwned) => tokio::task::yield_now().await,
                Err(error) => panic!("durable store reopen failed: {error:?}"),
            }
        }
    })
    .await
    .expect("durable owner lock was not released");
    assert_eq!(reopened.owner_generation(), owner_generation + 1);
    assert_eq!(reopened.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_journal(), journal);
    let reopened_weak = Arc::downgrade(&reopened);
    let app = make_app();
    let reopened_app_weak = Arc::downgrade(&app);
    let host = HttpHost::new_with_application(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), true, 1),
    )
    .unwrap();
    let server = Server::start(&host, None).await;
    let read = tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &path, ""))
        .await
        .expect("reopened read exceeded its complete TCP deadline");
    assert_eq!(read.status, 200);
    assert_eq!(read.body, created.body);
    assert_eq!(reopened.thread(&thread_id).unwrap().unwrap(), record);
    assert_eq!(reopened.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_journal(), journal);
    assert_eq!(effect_calls[0].load(Ordering::SeqCst), 0);
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    drop(shutdown);
    drop(host);
    drop(app);
    tokio::time::timeout(DEADLINE, async {
        while Arc::strong_count(&reopened) != 1 || reopened_app_weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reopened host retained store or application ownership");
    // Drop the sole store synchronously, including its owner-lock destructor,
    // before asking the Directory fixture to remove the durable files.
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("store alias survived cleanup")));
    assert!(reopened_weak.upgrade().is_none());
    assert!(reopened_app_weak.upgrade().is_none());
    drop(directory);
    assert!(!directory_path.exists(), "native directory must be removed");
}
