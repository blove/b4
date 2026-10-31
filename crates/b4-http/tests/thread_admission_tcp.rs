use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

mod native_support;
use native_support::*;

struct AdmissionPolicy(Arc<AtomicBool>);
impl ResourcePolicy for AdmissionPolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        self.0.load(Ordering::SeqCst) && Owner.authorize(principal, context)
    }
}
struct CountedValidator(Arc<AtomicUsize>);
impl InputValidator for CountedValidator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            input,
            &json!({"question":"admit café 🧵", "items":[null, true, 17]})
        );
        Ok(())
    }
}
struct CountedEffect {
    calls: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>,
}
impl NativeEffect for CountedEffect {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed
            .lock()
            .unwrap()
            .push((invocation.clone(), context.clone()));
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"answer":"authorized native result", "accepted":true}),
            writes: vec![StateWrite::Replace {
                field: "admitted".into(),
                value: json!(true),
            }],
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t003_implicit_thread_creation_requires_authorization() {
    const DEADLINE: Duration = Duration::from_secs(10);
    const ID: &str = "release-t003-client-selected";
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let allowed = Arc::new(AtomicBool::new(false));
    let validations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let make_app = || {
        application(
            definition(
                "/admission#workflow",
                "admission-v1",
                &[("admit", false, RecoveryPolicy::Pure)],
            ),
            Arc::new(CountedEffect {
                calls: calls.clone(),
                observed: observed.clone(),
            }),
            Some(Arc::new(CountedValidator(validations.clone()))),
        )
    };
    // Capture every column from the owned durable rows, including the creation
    // journal row that Store::journal cannot select by execution ID.
    // All statements/connections are short-lived and read-only.
    let read_durable = || {
        let connection = rusqlite::Connection::open_with_flags(
            directory_path.join("runtime.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        connection.busy_timeout(Duration::from_secs(5)).unwrap();
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
            read_rows("SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id", 7),
            read_rows("SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id", 5),
            read_rows("SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence", 9),
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
        Arc::new(AdmissionPolicy(allowed.clone())),
        config(app.clone(), true, 1),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .expect("TCP listener start exceeded deadline");
    let thread_id = ThreadId::new(ID).unwrap();
    let thread_path = format!("/threads/{ID}");
    let state_path = format!("{thread_path}/state");
    let run_path = format!("{thread_path}/runs/wait");
    let input = json!({"question":"admit café 🧵", "items":[null, true, 17]});
    let request = json!({"route":"/admission#workflow", "input":input.clone()}).to_string();
    assert!(request.len() < 1024);
    assert!(store.thread_view(&thread_id).unwrap().is_none());
    assert_eq!(read_durable(), (vec![], vec![], vec![]));
    let denied = tokio::time::timeout(
        DEADLINE,
        exchange(server.address, "POST", &run_path, &request),
    )
    .await
    .expect("denied run exceeded its complete TCP deadline");
    assert_eq!(denied.status, 404);
    assert_eq!(
        denied.json(),
        json!({"error":{"kind":"request_error", "message":"resource_unavailable", "details":{"code":"resource_unavailable"}}})
    );
    assert_eq!(validations.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(observed.lock().unwrap().is_empty());
    assert!(store.thread_view(&thread_id).unwrap().is_none());
    assert_eq!(read_durable(), (vec![], vec![], vec![]));

    allowed.store(true, Ordering::SeqCst);
    let accepted = tokio::time::timeout(
        DEADLINE,
        exchange(server.address, "POST", &run_path, &request),
    )
    .await
    .expect("authorized run exceeded its complete TCP deadline");
    assert_eq!(accepted.status, 200);
    let expected_output = json!({"answer":"authorized native result", "accepted":true});
    assert_eq!(accepted.json(), expected_output);
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let view: ThreadView = store.thread_view(&thread_id).unwrap().unwrap();
    assert_eq!(view.thread.id, thread_id);
    assert!(view.thread.metadata.is_empty());
    assert_ne!(view.thread.generation.as_str(), "0".repeat(64));
    assert!(view.thread.resource_revision > 0);
    let expected_origin = ThreadOrigin {
        application: "native-test".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    };
    assert_eq!(view.thread.origin, Some(expected_origin.clone()));
    let execution = view.execution.as_ref().unwrap();
    let snapshot = &execution.snapshot;
    assert_eq!(execution.thread_id, thread_id);
    assert_eq!(
        view.thread.active_execution_id.as_ref(),
        Some(&snapshot.execution_id)
    );
    assert_eq!(snapshot.status, ExecutionStatus::Completed);
    assert_eq!(snapshot.revision, 3);
    assert_eq!(snapshot.owner_generation, owner_generation);
    assert_eq!(
        snapshot.definition,
        definition(
            "/admission#workflow",
            "admission-v1",
            &[("admit", false, RecoveryPolicy::Pure)]
        )
    );
    assert_eq!(
        snapshot.values,
        json!({"input":input, "admitted":true})
            .as_object()
            .unwrap()
            .clone()
    );
    let expected_aggregate_output = json!({"input":input, "admitted":true});
    assert_eq!(snapshot.output.as_ref(), Some(&expected_aggregate_output));
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        snapshot.tasks[0].result.as_ref().unwrap().output,
        expected_output
    );
    assert_eq!(snapshot.attempts_used, 1);
    {
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        let (call, context) = &observed[0];
        assert_eq!(call.execution_id, snapshot.execution_id);
        assert_eq!(call.owner_generation, owner_generation);
        assert_eq!(call.turn_id, snapshot.turn_id);
        assert_eq!(call.task_id.as_str(), "admit");
        assert_eq!(call.effect_id.as_str(), "admit");
        assert_eq!(
            call.input,
            json!({"input":input}).as_object().unwrap().clone()
        );
        assert_eq!(
            snapshot.tasks[0].attempt_id.as_ref(),
            Some(&call.attempt_id)
        );
        assert_eq!(
            context,
            &InvocationContext {
                thread_id: thread_id.clone(),
                thread_generation: view.thread.generation.clone(),
                origin: expected_origin.clone(),
                application_revision: "application-v1".into(),
                execution_revision: ExecutionRevision::new("admission-v1").unwrap(),
                policy_revision: "policy-v1".into(),
            }
        );
    }
    let durable = read_durable();
    assert_eq!(
        durable.0,
        vec![vec![
            SqlValue::Text(ID.into()),
            SqlValue::Text(snapshot.execution_id.as_str().into()),
            SqlValue::Blob(view.thread.created_at_ms.to_be_bytes().to_vec()),
            SqlValue::Text("{}".into()),
            SqlValue::Text(serde_json::to_string(&expected_origin).unwrap()),
            SqlValue::Blob(view.thread.resource_revision.to_be_bytes().to_vec()),
            SqlValue::Text(view.thread.generation.as_str().into()),
        ]]
    );
    assert_eq!(durable.1.len(), 1);
    assert_eq!(
        durable.1[0][0],
        SqlValue::Text(snapshot.execution_id.as_str().into())
    );
    assert_eq!(durable.1[0][1], SqlValue::Text(ID.into()));
    assert_eq!(
        durable.1[0][2],
        SqlValue::Blob(snapshot.revision.to_be_bytes().to_vec())
    );
    assert_eq!(
        durable.1[0][3],
        SqlValue::Blob(owner_generation.to_be_bytes().to_vec())
    );
    let SqlValue::Text(raw_snapshot) = &durable.1[0][4] else {
        panic!("snapshot must be text")
    };
    assert_eq!(
        serde_json::from_str::<Snapshot>(raw_snapshot).unwrap(),
        *snapshot
    );
    assert_eq!(
        durable.2[0],
        vec![
            SqlValue::Integer(1),
            SqlValue::Text("thread".into()),
            SqlValue::Text(ID.into()),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Blob(owner_generation.to_be_bytes().to_vec()),
            SqlValue::Null,
            SqlValue::Blob(view.thread.created_at_ms.to_be_bytes().to_vec()),
            SqlValue::Null,
        ]
    );
    // One Pure task persists four execution rows: accepted admission, start,
    // dispatch, and completed result. Together with creation, this is five rows.
    assert_eq!(durable.2.len(), 5);
    for (index, row) in durable.2.iter().enumerate().skip(1) {
        let revision = u64::try_from(index - 1).unwrap();
        assert_eq!(row[0], SqlValue::Integer(i64::try_from(index + 1).unwrap()));
        assert_eq!(
            row[1],
            SqlValue::Text(if index == 1 { "admit" } else { "commit" }.into())
        );
        assert_eq!(row[2], SqlValue::Text(ID.into()));
        assert_eq!(
            row[3],
            SqlValue::Text(snapshot.execution_id.as_str().into())
        );
        assert_eq!(row[4], SqlValue::Blob(revision.to_be_bytes().to_vec()));
        assert_eq!(
            row[5],
            SqlValue::Blob(owner_generation.to_be_bytes().to_vec())
        );
        let SqlValue::Text(payload) = &row[8] else {
            panic!("journal payload must be text")
        };
        let entry: JournalEntry = serde_json::from_str(payload).unwrap();
        assert_eq!(entry.execution_id, snapshot.execution_id);
        assert_eq!(entry.revision, revision);
        assert_eq!(entry.owner_generation, owner_generation);
        assert_eq!(
            row[6],
            SqlValue::Text(serde_json::to_string(&entry.status).unwrap())
        );
        assert_eq!(
            row[7],
            SqlValue::Blob(entry.updated_at_ms.to_be_bytes().to_vec())
        );
        if index == 1 {
            assert_eq!(entry.status, ExecutionStatus::Accepted);
        } else if index + 1 == durable.2.len() {
            assert_eq!(entry.status, ExecutionStatus::Completed);
            assert_eq!(entry.revision, snapshot.revision);
            assert_eq!(entry.updated_at_ms, snapshot.updated_at_ms);
        } else {
            assert_eq!(entry.status, ExecutionStatus::Running);
        }
    }
    let read_thread =
        tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &thread_path, ""))
            .await
            .unwrap();
    assert_eq!(read_thread.status, 200);
    assert_eq!(read_thread.json()["thread_id"], ID);
    assert_eq!(read_thread.json()["metadata"], json!({}));
    assert_eq!(read_thread.json()["status"], "idle");
    let read_state =
        tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &state_path, ""))
            .await
            .unwrap();
    assert_eq!(read_state.status, 200);
    let state = read_state.json();
    assert_eq!(state["config"]["configurable"]["thread_id"], ID);
    assert_eq!(
        state["config"]["configurable"]["checkpoint_id"],
        snapshot.execution_id.as_str()
    );
    assert_eq!(state["metadata"]["route"], "/admission#workflow");
    assert_eq!(state["metadata"]["execution_revision"], "admission-v1");
    assert_eq!(state["values"], json!({"input":input, "admitted":true}));
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_durable(), durable);
    // stop aborts and joins the listener after healthy host quiescence.
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
        Arc::new(AdmissionPolicy(allowed.clone())),
        config(app.clone(), true, 1),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .unwrap();
    let thread = tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &thread_path, ""))
        .await
        .unwrap();
    assert_eq!(thread.status, 200);
    assert_eq!(thread.body, read_thread.body);
    let state = tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &state_path, ""))
        .await
        .unwrap();
    assert_eq!(state.status, 200);
    assert_eq!(state.body, read_state.body);
    assert_eq!(reopened.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_durable(), durable);
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.lock().unwrap().len(), 1);
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(reopened.thread_view(&thread_id).unwrap().unwrap(), view);
    assert_eq!(read_durable(), durable);
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
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("store alias survived cleanup")));
    assert!(reopened_weak.upgrade().is_none());
    assert!(reopened_app_weak.upgrade().is_none());
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(directory);
    assert!(!directory_path.exists(), "native directory must be removed");
}
