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

struct CapturedValidator {
    calls: Arc<AtomicUsize>,
    inputs: Arc<Mutex<Vec<Value>>>,
}
impl InputValidator for CapturedValidator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inputs.lock().unwrap().push(input.clone());
        Ok(())
    }
}

struct CapturedEffect {
    calls: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>,
}
impl NativeEffect for CapturedEffect {
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
        assert!(invocation.input.contains_key("input"));
        assert_eq!(invocation.input.len(), 1);
        let raw_input = invocation.input.get("input").unwrap();
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"received": raw_input}),
            writes: vec![],
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t006_omitted_null_and_false_inputs_round_trip() {
    const DEADLINE: Duration = Duration::from_secs(10);
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let validations = Arc::new(AtomicUsize::new(0));
    let validator_inputs = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let expected_definition = definition(
        "/input-shapes#workflow",
        "input-shapes-v1",
        &[("receive", false, RecoveryPolicy::Pure)],
    );
    // Both hosts use this same immutable application; reopen performs only reads.
    let app = application(
        expected_definition.clone(),
        Arc::new(CapturedEffect {
            calls: calls.clone(),
            observed: observed.clone(),
        }),
        Some(Arc::new(CapturedValidator {
            calls: validations.clone(),
            inputs: validator_inputs.clone(),
        })),
    );
    let weak_app = Arc::downgrade(&app);
    // Request bodies and expected raw inputs/results are declared independently.
    // The first body truly has no input member.
    let cases = [
        (
            "release-t006-omitted",
            json!({"route":"/input-shapes#workflow"}),
            json!({}),
            json!({"received":{}}),
        ),
        (
            "release-t006-null",
            json!({"route":"/input-shapes#workflow", "input":null}),
            Value::Null,
            json!({"received":null}),
        ),
        (
            "release-t006-false",
            json!({"route":"/input-shapes#workflow", "input":false}),
            json!(false),
            json!({"received":false}),
        ),
    ];
    assert!(!cases[0].1.as_object().unwrap().contains_key("input"));
    let expected_origin = ThreadOrigin {
        application: "native-test".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    };
    // Every column is captured using a short-lived independent read-only
    // connection, including thread creation journal records.
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
    let mut next_store = Some(Arc::new(
        b4_sqlite::SqliteStore::open(&directory.0).unwrap(),
    ));
    let owner_generation = next_store.as_ref().unwrap().owner_generation();
    let mut views = Vec::new();
    let mut responses = Vec::new();
    let mut captured_durable = None;
    for host_round in 0..2 {
        let store = next_store.take().unwrap();
        let weak_store = Arc::downgrade(&store);
        assert_eq!(store.owner_generation(), owner_generation + host_round);
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
        if host_round == 0 {
            assert_eq!(read_durable(), (vec![], vec![], vec![]));
            for (index, (id, body, expected_input, expected_result)) in cases.iter().enumerate() {
                let thread_id = ThreadId::new(*id).unwrap();
                assert!(store.thread_view(&thread_id).unwrap().is_none());
                let request = body.to_string();
                let run_path = format!("/threads/{id}/runs/wait");
                let response = tokio::time::timeout(
                    DEADLINE,
                    exchange(server.address, "POST", &run_path, &request),
                )
                .await
                .expect("run exceeded its complete TCP deadline");
                assert_eq!(response.status, 200);
                assert_eq!(response.json(), *expected_result);
                assert_eq!(validations.load(Ordering::SeqCst), index + 1);
                assert_eq!(calls.load(Ordering::SeqCst), index + 1);
                assert_eq!(
                    *validator_inputs.lock().unwrap(),
                    cases[..=index]
                        .iter()
                        .map(|case| case.2.clone())
                        .collect::<Vec<_>>()
                );
                let view = store.thread_view(&thread_id).unwrap().unwrap();
                assert_eq!(view.thread.id, thread_id);
                assert!(view.thread.metadata.is_empty());
                assert_eq!(view.thread.origin, Some(expected_origin.clone()));
                assert!(view.thread.resource_revision > 0);
                assert_ne!(view.thread.generation.as_str(), "0".repeat(64));
                let execution = view.execution.as_ref().unwrap();
                let snapshot = &execution.snapshot;
                assert_eq!(execution.thread_id, thread_id);
                assert_eq!(
                    view.thread.active_execution_id.as_ref(),
                    Some(&snapshot.execution_id)
                );
                assert!(views.iter().all(|previous: &ThreadView| {
                    previous.execution.as_ref().unwrap().snapshot.execution_id
                        != snapshot.execution_id
                }));
                assert_eq!(snapshot.status, ExecutionStatus::Completed);
                assert_eq!(snapshot.revision, 3);
                assert_eq!(snapshot.owner_generation, owner_generation);
                assert_eq!(snapshot.definition, expected_definition);
                assert!(snapshot.values.contains_key("input"));
                assert_eq!(snapshot.values.get("input"), Some(expected_input));
                assert_eq!(snapshot.values.len(), 1);
                let expected_aggregate = json!({"input":expected_input});
                assert_eq!(snapshot.output.as_ref(), Some(&expected_aggregate));
                assert_eq!(snapshot.tasks.len(), 1);
                assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
                assert_eq!(
                    snapshot.tasks[0].result.as_ref().unwrap().output,
                    *expected_result
                );
                assert!(snapshot.tasks[0].result.as_ref().unwrap().writes.is_empty());
                assert_eq!(snapshot.attempts_used, 1);
                {
                    let observed = observed.lock().unwrap();
                    assert_eq!(observed.len(), index + 1);
                    let (invocation, context) = &observed[index];
                    assert!(invocation.input.contains_key("input"));
                    assert_eq!(invocation.input.get("input"), Some(expected_input));
                    assert_eq!(invocation.input.len(), 1);
                    assert_eq!(invocation.execution_id, snapshot.execution_id);
                    assert_eq!(invocation.owner_generation, owner_generation);
                    assert_eq!(invocation.turn_id, snapshot.turn_id);
                    assert_eq!(invocation.task_id.as_str(), "receive");
                    assert_eq!(invocation.effect_id.as_str(), "receive");
                    assert_eq!(
                        snapshot.tasks[0].attempt_id.as_ref(),
                        Some(&invocation.attempt_id)
                    );
                    assert_eq!(
                        context,
                        &InvocationContext {
                            thread_id: thread_id.clone(),
                            thread_generation: view.thread.generation.clone(),
                            origin: expected_origin.clone(),
                            application_revision: "application-v1".into(),
                            execution_revision: ExecutionRevision::new("input-shapes-v1").unwrap(),
                            policy_revision: "policy-v1".into(),
                        }
                    );
                }
                views.push(view);
            }
            let durable = read_durable();
            assert_eq!(durable.0.len(), 3);
            assert_eq!(durable.1.len(), 3);
            // One Pure task has admission/start/dispatch/result execution
            // journal records, plus the separate thread creation record.
            assert_eq!(durable.2.len(), 15);
            for (index, (id, _, expected_input, _)) in cases.iter().enumerate() {
                let view = &views[index];
                let snapshot = &view.execution.as_ref().unwrap().snapshot;
                let thread_rows: Vec<_> = durable
                    .0
                    .iter()
                    .filter(|row| row[0] == SqlValue::Text((*id).into()))
                    .collect();
                assert_eq!(
                    thread_rows,
                    vec![&vec![
                        SqlValue::Text((*id).into()),
                        SqlValue::Text(snapshot.execution_id.as_str().into()),
                        SqlValue::Blob(view.thread.created_at_ms.to_be_bytes().to_vec()),
                        SqlValue::Text("{}".into()),
                        SqlValue::Text(serde_json::to_string(&expected_origin).unwrap()),
                        SqlValue::Blob(view.thread.resource_revision.to_be_bytes().to_vec()),
                        SqlValue::Text(view.thread.generation.as_str().into()),
                    ]]
                );
                let execution_rows: Vec<_> = durable
                    .1
                    .iter()
                    .filter(|row| row[1] == SqlValue::Text((*id).into()))
                    .collect();
                assert_eq!(execution_rows.len(), 1);
                let row = execution_rows[0];
                assert_eq!(
                    row[0],
                    SqlValue::Text(snapshot.execution_id.as_str().into())
                );
                assert_eq!(
                    row[2],
                    SqlValue::Blob(snapshot.revision.to_be_bytes().to_vec())
                );
                assert_eq!(
                    row[3],
                    SqlValue::Blob(owner_generation.to_be_bytes().to_vec())
                );
                let SqlValue::Text(raw_snapshot) = &row[4] else {
                    panic!("snapshot must be text")
                };
                let saved: Snapshot = serde_json::from_str(raw_snapshot).unwrap();
                assert_eq!(&saved, snapshot);
                assert!(saved.values.contains_key("input"));
                assert_eq!(saved.values.get("input"), Some(expected_input));
                let journal = &durable.2[index * 5..(index + 1) * 5];
                assert_eq!(
                    journal[0],
                    vec![
                        SqlValue::Integer(i64::try_from(index * 5 + 1).unwrap()),
                        SqlValue::Text("thread".into()),
                        SqlValue::Text((*id).into()),
                        SqlValue::Null,
                        SqlValue::Null,
                        SqlValue::Blob(owner_generation.to_be_bytes().to_vec()),
                        SqlValue::Null,
                        SqlValue::Blob(view.thread.created_at_ms.to_be_bytes().to_vec()),
                        SqlValue::Null,
                    ]
                );
                for (offset, row) in journal.iter().enumerate().skip(1) {
                    let revision = u64::try_from(offset - 1).unwrap();
                    assert_eq!(
                        row[0],
                        SqlValue::Integer(i64::try_from(index * 5 + offset + 1).unwrap())
                    );
                    assert_eq!(
                        row[1],
                        SqlValue::Text(if offset == 1 { "admit" } else { "commit" }.into())
                    );
                    assert_eq!(row[2], SqlValue::Text((*id).into()));
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
                    assert_eq!(
                        entry.status,
                        match offset {
                            1 => ExecutionStatus::Accepted,
                            4 => ExecutionStatus::Completed,
                            _ => ExecutionStatus::Running,
                        }
                    );
                    if offset == 4 {
                        assert_eq!(entry.updated_at_ms, snapshot.updated_at_ms);
                    }
                }
            }
            captured_durable = Some(durable);
        }
        for (index, (id, _, expected_input, _)) in cases.iter().enumerate() {
            let thread_id = ThreadId::new(*id).unwrap();
            assert_eq!(
                store.thread_view(&thread_id).unwrap().as_ref(),
                Some(&views[index])
            );
            let thread_path = format!("/threads/{id}");
            let state_path = format!("{thread_path}/state");
            let thread =
                tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &thread_path, ""))
                    .await
                    .expect("thread read exceeded its complete TCP deadline");
            assert_eq!(thread.status, 200);
            let thread_json = thread.json();
            assert_eq!(thread_json.get("thread_id"), Some(&json!(id)));
            assert_eq!(thread_json.get("metadata"), Some(&json!({})));
            assert_eq!(thread_json.get("status"), Some(&json!("idle")));
            let state =
                tokio::time::timeout(DEADLINE, exchange(server.address, "GET", &state_path, ""))
                    .await
                    .expect("state read exceeded its complete TCP deadline");
            assert_eq!(state.status, 200);
            let state_json = state.json();
            let values = state_json.get("values").unwrap().as_object().unwrap();
            assert!(values.contains_key("input"));
            assert_eq!(values.get("input"), Some(expected_input));
            assert_eq!(values.len(), 1);
            let configurable = state_json
                .get("config")
                .unwrap()
                .get("configurable")
                .unwrap();
            assert_eq!(configurable.get("thread_id"), Some(&json!(id)));
            assert_eq!(
                configurable.get("checkpoint_id"),
                Some(&json!(views[index]
                    .execution
                    .as_ref()
                    .unwrap()
                    .snapshot
                    .execution_id
                    .as_str()))
            );
            let metadata = state_json.get("metadata").unwrap();
            assert_eq!(
                metadata.get("route"),
                Some(&json!("/input-shapes#workflow"))
            );
            assert_eq!(
                metadata.get("execution_revision"),
                Some(&json!("input-shapes-v1"))
            );
            if host_round == 0 {
                responses.push((thread.body, state.body));
            } else {
                assert_eq!(thread.body, responses[index].0);
                assert_eq!(state.body, responses[index].1);
            }
        }
        assert_eq!(read_durable(), *captured_durable.as_ref().unwrap());
        assert_eq!(validations.load(Ordering::SeqCst), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            *validator_inputs.lock().unwrap(),
            vec![json!({}), Value::Null, json!(false)]
        );
        assert_eq!(observed.lock().unwrap().len(), 3);
        let shutdown = host.shutdown_handle();
        assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
        assert_eq!(
            tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
                .await
                .expect("host quiescence exceeded deadline")
                .unwrap(),
            ShutdownOutcome::Quiescent { faulted: false }
        );
        assert_eq!(read_durable(), *captured_durable.as_ref().unwrap());
        // Fixture listener abort/join follows positively observed healthy quiescence.
        tokio::time::timeout(DEADLINE, server.stop())
            .await
            .expect("TCP listener stop exceeded deadline");
        drop(shutdown);
        drop(host);
        tokio::time::timeout(DEADLINE, async {
            while Arc::strong_count(&store) != 1 || Arc::strong_count(&app) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retired host retained store or application ownership");
        assert_eq!(read_durable(), *captured_durable.as_ref().unwrap());
        // Synchronously destroy the sole owner before reopen or directory removal.
        drop(Arc::try_unwrap(store).unwrap_or_else(|_| panic!("store alias survived cleanup")));
        assert!(weak_store.upgrade().is_none());
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
            assert_eq!(read_durable(), *captured_durable.as_ref().unwrap());
            next_store = Some(reopened);
        }
    }
    assert!(next_store.is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    assert_eq!(validations.load(Ordering::SeqCst), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(observed.lock().unwrap().len(), 3);
    drop(directory);
    assert!(!directory_path.exists(), "native directory must be removed");
}
