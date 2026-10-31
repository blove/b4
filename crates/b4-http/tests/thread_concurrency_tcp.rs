use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

mod native_support;
use native_support::*;

struct CountedValidator(Arc<AtomicUsize>);
impl InputValidator for CountedValidator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            input,
            &json!({"question":"overlap café 🧵", "items":[null, true, 17]})
        );
        Ok(())
    }
}
struct CountedEffect {
    gate: Arc<Gate>,
    returned: Arc<AtomicUsize>,
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
        self.gate.hold();
        self.returned.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"answer":"single native winner", "accepted":true}),
            writes: vec![],
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t007_same_thread_overlap_has_one_winner() {
    const DEADLINE: Duration = Duration::from_secs(10);
    const ID: &str = "release-t007-same-thread";
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let gate = Arc::new(Gate::default());
    // Install panic-path release before starting any callback work.
    let _release = Release(gate.clone());
    let returned = Arc::new(AtomicUsize::new(0));
    let validations = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    // The same immutable one-Pure-task application serves both host lifetimes.
    let app = application(
        definition(
            "/thread-overlap#workflow",
            "thread-overlap-v1",
            &[("admit", false, RecoveryPolicy::Pure)],
        ),
        Arc::new(CountedEffect {
            gate: gate.clone(),
            returned: returned.clone(),
            calls: calls.clone(),
            observed: observed.clone(),
        }),
        Some(Arc::new(CountedValidator(validations.clone()))),
    );
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
    let weak_app = Arc::downgrade(&app);
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak_store = Arc::downgrade(&store);
    let owner_generation = store.owner_generation();
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), true, 2),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .expect("TCP listener start exceeded deadline");
    let thread_id = ThreadId::new(ID).unwrap();
    let thread_path = format!("/threads/{ID}");
    let state_path = format!("{thread_path}/state");
    let run_path = format!("{thread_path}/runs/wait");
    let input = json!({"question":"overlap café 🧵", "items":[null, true, 17]});
    let request = json!({"route":"/thread-overlap#workflow", "input":input.clone()}).to_string();
    assert!(request.len() < 1024);
    assert!(store.thread_view(&thread_id).unwrap().is_none());
    assert_eq!(read_durable(), (vec![], vec![], vec![]));
    // Keep the actual winner socket connected while its native callback is held.
    let winner = tokio::time::timeout(DEADLINE, open(server.address, "POST", &run_path, &request))
        .await
        .expect("winner open exceeded its complete TCP deadline");
    tokio::time::timeout(DEADLINE, gate.entered())
        .await
        .expect("winner callback did not enter before deadline");
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(returned.load(Ordering::SeqCst), 0);
    let held: ThreadView = store.thread_view(&thread_id).unwrap().unwrap();
    let held_snapshot = &held.execution.as_ref().unwrap().snapshot;
    assert_eq!(held_snapshot.status, ExecutionStatus::Running);
    assert_eq!(held_snapshot.revision, 2);
    assert_eq!(held_snapshot.tasks.len(), 1);
    assert_eq!(held_snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(held_snapshot.tasks[0].result.is_none());
    assert_eq!(held_snapshot.attempts_used, 1);
    assert_eq!(held_snapshot.attempt_history.len(), 1);
    assert_eq!(held_snapshot.output, None);
    assert_eq!(
        held_snapshot.values,
        json!({"input":input}).as_object().unwrap().clone()
    );
    assert_eq!(held_snapshot.tasks[0].input, held_snapshot.values);
    assert_provenance(&held, &observed.lock().unwrap(), owner_generation, &input);
    let held_durable = read_durable();
    assert_records(
        &held,
        &held_durable,
        owner_generation,
        &[
            ExecutionStatus::Accepted,
            ExecutionStatus::Running,
            ExecutionStatus::Running,
        ],
    );

    // Claim acquisition precedes validation: the loser must never validate.
    // Observe its full response positively before releasing the winning callback.
    let loser = tokio::time::timeout(
        DEADLINE,
        exchange(server.address, "POST", &run_path, &request),
    )
    .await
    .expect("overlapping loser exceeded its complete TCP deadline");
    assert_eq!(loser.status, 409);
    assert_eq!(
        loser.json(),
        json!({"error":{"kind":"request_error","message":"thread_busy","details":{"code":"thread_busy"}}})
    );
    assert_eq!(returned.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(observed.lock().unwrap().len(), 1);
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), held);
    assert_eq!(read_durable(), held_durable);

    gate.release();
    let accepted = tokio::time::timeout(DEADLINE, finish(winner, vec![]))
        .await
        .expect("winner finish exceeded its complete TCP deadline");
    assert_eq!(accepted.status, 200);
    let expected_output = json!({"answer":"single native winner", "accepted":true});
    assert_eq!(accepted.json(), expected_output);
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let view: ThreadView = store.thread_view(&thread_id).unwrap().unwrap();
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Completed);
    assert_eq!(snapshot.revision, 3);
    assert_eq!(
        snapshot.values,
        json!({"input":input}).as_object().unwrap().clone()
    );
    let expected_aggregate_output = json!({"input":input});
    assert_eq!(snapshot.output.as_ref(), Some(&expected_aggregate_output));
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        snapshot.tasks[0].result.as_ref().unwrap().output,
        expected_output
    );
    assert_eq!(snapshot.attempts_used, 1);
    assert!(snapshot.tasks[0].result.as_ref().unwrap().writes.is_empty());
    assert_eq!(snapshot.attempt_history.len(), 1);
    assert_eq!(snapshot.attempt_history, held_snapshot.attempt_history);
    assert_eq!(snapshot.execution_id, held_snapshot.execution_id);
    assert_eq!(snapshot.turn_id, held_snapshot.turn_id);
    assert_eq!(
        snapshot.tasks[0].input,
        json!({"input":input}).as_object().unwrap().clone()
    );
    assert_provenance(&view, &observed.lock().unwrap(), owner_generation, &input);
    let durable = read_durable();
    assert_records(
        &view,
        &durable,
        owner_generation,
        &[
            ExecutionStatus::Accepted,
            ExecutionStatus::Running,
            ExecutionStatus::Running,
            ExecutionStatus::Completed,
        ],
    );
    assert_eq!(
        &durable.2[..held_durable.2.len()],
        held_durable.2.as_slice()
    );
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
    assert_eq!(state["metadata"]["route"], "/thread-overlap#workflow");
    assert_eq!(state["metadata"]["execution_revision"], "thread-overlap-v1");
    assert_eq!(state["values"], json!({"input":input}));
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
    tokio::time::timeout(DEADLINE, async {
        while Arc::strong_count(&store) != 1 || Arc::strong_count(&app) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired host retained store or application ownership");
    drop(Arc::try_unwrap(store).unwrap_or_else(|_| panic!("store alias survived cleanup")));
    assert!(weak_store.upgrade().is_none());
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
    let host = HttpHost::new_with_application(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), true, 2),
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
    assert_eq!(returned.load(Ordering::SeqCst), 1);
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
    tokio::time::timeout(DEADLINE, async {
        while Arc::strong_count(&reopened) != 1 || Arc::strong_count(&app) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reopened host retained store or application ownership");
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("store alias survived cleanup")));
    assert!(reopened_weak.upgrade().is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    assert_eq!(observed.lock().unwrap().len(), 1);
    drop(directory);
    assert!(!directory_path.exists(), "native directory must be removed");
}

type DurableRows = (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>);

fn assert_records(
    view: &ThreadView,
    durable: &DurableRows,
    owner_generation: u64,
    statuses: &[ExecutionStatus],
) {
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    let expected_origin = ThreadOrigin {
        application: "native-test".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    };
    assert_eq!(
        durable.0,
        vec![vec![
            SqlValue::Text(view.thread.id.as_str().into()),
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
    assert_eq!(
        durable.1[0][1],
        SqlValue::Text(view.thread.id.as_str().into())
    );
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
            SqlValue::Text(view.thread.id.as_str().into()),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Blob(owner_generation.to_be_bytes().to_vec()),
            SqlValue::Null,
            SqlValue::Blob(view.thread.created_at_ms.to_be_bytes().to_vec()),
            SqlValue::Null,
        ]
    );
    // One creation row plus the independently expected execution journal chain.
    assert_eq!(durable.2.len(), statuses.len() + 1);
    for (index, row) in durable.2.iter().enumerate().skip(1) {
        let revision = u64::try_from(index - 1).unwrap();
        assert_eq!(row[0], SqlValue::Integer(i64::try_from(index + 1).unwrap()));
        assert_eq!(
            row[1],
            SqlValue::Text(if index == 1 { "admit" } else { "commit" }.into())
        );
        assert_eq!(row[2], SqlValue::Text(view.thread.id.as_str().into()));
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
        assert_eq!(entry.status, statuses[index - 1]);
        if index + 1 == durable.2.len() {
            assert_eq!(entry.revision, snapshot.revision);
            assert_eq!(entry.updated_at_ms, snapshot.updated_at_ms);
        }
    }
}

fn assert_provenance(
    view: &ThreadView,
    observed: &[(EffectInvocation, InvocationContext)],
    owner_generation: u64,
    input: &Value,
) {
    assert_eq!(view.thread.id.as_str(), "release-t007-same-thread");
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
    assert_eq!(execution.thread_id, view.thread.id);
    assert_eq!(
        view.thread.active_execution_id.as_ref(),
        Some(&snapshot.execution_id)
    );
    assert_eq!(snapshot.owner_generation, owner_generation);
    assert_eq!(
        snapshot.definition,
        definition(
            "/thread-overlap#workflow",
            "thread-overlap-v1",
            &[("admit", false, RecoveryPolicy::Pure)]
        )
    );
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].spec, snapshot.definition.batches[0][0]);
    assert_eq!(observed.len(), 1);
    let (call, context) = &observed[0];
    assert_eq!(call.execution_id, snapshot.execution_id);
    assert_eq!(call.owner_generation, owner_generation);
    assert_eq!(call.turn_id, snapshot.turn_id);
    assert_eq!(call.task_id.as_str(), "admit");
    assert_eq!(call.effect_id.as_str(), "admit");
    assert!(call.input.contains_key("input"));
    assert_eq!(call.input.len(), 1);
    assert_eq!(call.input.get("input"), Some(input));
    assert_eq!(snapshot.tasks[0].input, call.input);
    assert_eq!(
        snapshot.tasks[0].attempt_id.as_ref(),
        Some(&call.attempt_id)
    );
    assert_eq!(snapshot.attempt_history, vec![call.attempt_id.clone()]);
    assert_eq!(
        context,
        &InvocationContext {
            thread_id: view.thread.id.clone(),
            thread_generation: view.thread.generation.clone(),
            origin: expected_origin,
            application_revision: "application-v1".into(),
            execution_revision: ExecutionRevision::new("thread-overlap-v1").unwrap(),
            policy_revision: "policy-v1".into(),
        }
    );
}
