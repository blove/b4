use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use rusqlite::types::Value as SqlValue;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

mod native_support;
use native_support::*;

struct CountedEffect(Arc<AtomicUsize>);

impl NativeEffect for CountedEffect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("unexpected execution"),
            writes: vec![],
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t002_empty_thread_state_stays_unavailable_after_reopen() {
    const DEADLINE: Duration = Duration::from_secs(10);
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let effect_calls = Arc::new(AtomicUsize::new(0));
    let make_app = || {
        application(
            definition(
                "/empty-state#workflow",
                "empty-state-v1",
                &[("unused", false, RecoveryPolicy::Pure)],
            ),
            Arc::new(CountedEffect(effect_calls.clone())),
            None,
        )
    };
    // Include the raw thread row and every journal column, independently of
    // the runtime projection. Each read-only connection closes on return.
    let read_durable = || {
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
            "state reads must not admit an execution"
        );
        let read_rows = |sql: &str, columns: usize| {
            let mut statement = connection.prepare(sql).unwrap();
            let rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get::<_, SqlValue>(column))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            rows
        };
        (
            read_rows(
                "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id",
                7,
            ),
            read_rows(
                "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence",
                9,
            ),
        )
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
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .expect("TCP listener start exceeded deadline");
    let metadata = json!({
        "label": "empty checkpoint 🧵",
        "nested": {"source": "T002", "items": [null, true, 23, {"label": "café"}]},
        "state": {"ordinary_metadata": true}
    });
    let request = json!({"metadata": metadata.clone()}).to_string();
    let created = tokio::time::timeout(
        DEADLINE,
        exchange(server.address, "POST", "/threads", &request),
    )
    .await
    .expect("thread creation exceeded its complete TCP deadline");
    assert_eq!(created.status, 200);
    let body = created.json();
    let id = body["thread_id"].as_str().unwrap();
    uuid::Uuid::parse_str(id).expect("server must assign a valid UUID");
    let thread_id = ThreadId::new(id).unwrap();
    let view = store.thread_view(&thread_id).unwrap().unwrap();
    assert!(view.execution.is_none());
    let record = view.thread.clone();
    assert_eq!(record.id, thread_id);
    assert!(record.active_execution_id.is_none());
    assert_eq!(record.metadata, metadata.as_object().unwrap().clone());
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
    let durable = read_durable();
    assert_eq!(durable.0.len(), 1);
    assert_eq!(durable.0[0][0], SqlValue::Text(id.into()));
    assert_eq!(durable.0[0][1], SqlValue::Null);
    assert_eq!(
        durable.1,
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
    let thread_path = format!("/threads/{id}");
    let state_path = format!("{thread_path}/state");
    let expected_error = json!({
        "error": {
            "kind": "request_error",
            "message": "checkpoint_unavailable",
            "details": {"code": "checkpoint_unavailable"}
        }
    });
    for _ in 0..2 {
        let state =
            tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &state_path, ""))
                .await
                .expect("state read exceeded its complete TCP deadline");
        assert_eq!(state.status, 404);
        assert_eq!(state.json(), expected_error);
        let read =
            tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &thread_path, ""))
                .await
                .expect("thread read exceeded its complete TCP deadline");
        assert_eq!(read.status, 200);
        assert_eq!(read.body, created.body);
        assert_eq!(store.thread(&thread_id).unwrap().unwrap(), record);
        assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), view);
        assert_eq!(read_durable(), durable);
        assert_eq!(effect_calls.load(Ordering::SeqCst), 0);
    }
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .expect("first host quiescence exceeded deadline")
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_durable(), durable);
    // stop aborts and joins the listener; host quiescence is proved above.
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
    // Reacquiring the real directory owner lock proves physical release;
    // weak-reference expiry can precede the final store destructor returning.
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
    assert_eq!(read_durable(), durable);
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
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .expect("reopened TCP listener start exceeded deadline");
    for _ in 0..2 {
        let state =
            tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &state_path, ""))
                .await
                .expect("reopened state read exceeded its complete TCP deadline");
        assert_eq!(state.status, 404);
        assert_eq!(state.json(), expected_error);
        let read =
            tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &thread_path, ""))
                .await
                .expect("reopened thread read exceeded its complete TCP deadline");
        assert_eq!(read.status, 200);
        assert_eq!(read.body, created.body);
        assert_eq!(reopened.thread(&thread_id).unwrap().unwrap(), record);
        assert_eq!(reopened.thread_view(&thread_id).unwrap().unwrap(), view);
        assert_eq!(read_durable(), durable);
        assert_eq!(effect_calls.load(Ordering::SeqCst), 0);
    }
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .expect("reopened host quiescence exceeded deadline")
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(reopened.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_durable(), durable);
    assert_eq!(effect_calls.load(Ordering::SeqCst), 0);
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
    // Synchronously destroy the sole store, including its owner lock, before
    // the directory fixture removes the durable files.
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("store alias survived cleanup")));
    assert!(reopened_weak.upgrade().is_none());
    assert!(reopened_app_weak.upgrade().is_none());
    drop(directory);
    assert!(!directory_path.exists(), "native directory must be removed");
}
