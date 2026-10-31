use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

mod native_support;
use native_support::*;

struct CountedValidator(Arc<AtomicUsize>);
impl InputValidator for CountedValidator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

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
async fn release_t005_malformed_json_uses_ap_error_without_execution() {
    const DEADLINE: Duration = Duration::from_secs(10);
    const ID: &str = "release-t005-client-selected";
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let validations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let make_app = || {
        application(
            definition(
                "/known#workflow",
                "known-v1",
                &[("unused", false, RecoveryPolicy::Pure)],
            ),
            Arc::new(CountedEffect(calls.clone())),
            Some(Arc::new(CountedValidator(validations.clone()))),
        )
    };
    let thread_id = ThreadId::new(ID).unwrap();
    let run_path = format!("/threads/{ID}/runs/wait");
    // Each observation opens and closes an independent read-only connection.
    // Global row counts also detect accidental writes outside the target ID.
    let assert_absent = |store: &b4_sqlite::SqliteStore| {
        assert_eq!(validations.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(store.thread_view(&thread_id).unwrap().is_none());
        let connection = rusqlite::Connection::open_with_flags(
            directory_path.join("runtime.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        connection.busy_timeout(Duration::from_secs(5)).unwrap();
        let threads: i64 = connection
            .query_row("SELECT count(*) FROM threads", [], |row| row.get(0))
            .unwrap();
        let executions: i64 = connection
            .query_row("SELECT count(*) FROM executions", [], |row| row.get(0))
            .unwrap();
        let journal: i64 = connection
            .query_row("SELECT count(*) FROM journal", [], |row| row.get(0))
            .unwrap();
        assert_eq!((threads, executions, journal), (0, 0, 0));
    };
    let mut next_store = Some(Arc::new(
        b4_sqlite::SqliteStore::open(&directory.0).unwrap(),
    ));
    let owner_generation = next_store.as_ref().unwrap().owner_generation();
    for host_round in 0..2 {
        let store = next_store.take().unwrap();
        let weak_store = Arc::downgrade(&store);
        let app = make_app();
        let weak_app = Arc::downgrade(&app);
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
        assert_absent(&store);
        // The HTTP framing is valid; only the literal JSON body is incomplete.
        let response =
            tokio::time::timeout(DEADLINE, exchange(server.address, "POST", &run_path, "{"))
                .await
                .expect("rejected run exceeded its complete TCP deadline");
        assert_eq!(response.status, 400, "host {host_round}");
        assert_eq!(
            response.json(),
            json!({"error":{"kind":"request_error","message":"invalid_json","details":{"code":"invalid_json"}}}),
            "host {host_round}"
        );
        assert_absent(&store);
        let shutdown = host.shutdown_handle();
        assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
        assert_eq!(
            tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
                .await
                .expect("host quiescence exceeded deadline")
                .unwrap(),
            ShutdownOutcome::Quiescent { faulted: false }
        );
        assert_absent(&store);
        // The listener is aborted and joined only after healthy host quiescence.
        tokio::time::timeout(DEADLINE, server.stop())
            .await
            .expect("TCP listener stop exceeded deadline");
        drop(shutdown);
        drop(host);
        drop(app);
        tokio::time::timeout(DEADLINE, async {
            while Arc::strong_count(&store) != 1 || weak_app.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retired host retained store or application ownership");
        assert_absent(&store);
        // Synchronously destroy the sole store, including its physical owner
        // lock, before reopening or removing the directory.
        drop(Arc::try_unwrap(store).unwrap_or_else(|_| panic!("store alias survived cleanup")));
        assert!(weak_store.upgrade().is_none());
        assert!(weak_app.upgrade().is_none());
        if host_round == 0 {
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
            assert_absent(&reopened);
            next_store = Some(reopened);
        }
    }
    assert!(next_store.is_none());
    assert_eq!(validations.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(directory);
    assert!(!directory_path.exists(), "native directory must be removed");
}
