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

const DEADLINE: Duration = Duration::from_secs(10);
const ROUTE: &str = "/different-threads#workflow";
const REVISION: &str = "different-threads-v1";

struct Target {
    id: &'static str,
    input: Value,
    output: Value,
    gate: Arc<Gate>,
    validations: AtomicUsize,
    raw_inputs: Mutex<Vec<Value>>,
    entered: AtomicUsize,
    returned: AtomicUsize,
    observed: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
}
impl Target {
    fn new(id: &'static str, input: Value, output: Value) -> Self {
        Self {
            id,
            input,
            output,
            gate: Arc::new(Gate::default()),
            validations: AtomicUsize::new(0),
            raw_inputs: Mutex::new(vec![]),
            entered: AtomicUsize::new(0),
            returned: AtomicUsize::new(0),
            observed: Mutex::new(vec![]),
        }
    }
    fn thread_id(&self) -> ThreadId {
        ThreadId::new(self.id).unwrap()
    }
    fn path(&self) -> String {
        format!("/threads/{}", self.id)
    }
    fn request(&self) -> String {
        json!({"route":ROUTE,"input":self.input}).to_string()
    }
    fn assert_counts(&self, returned: usize) {
        assert_eq!(self.validations.load(Ordering::SeqCst), 1);
        assert_eq!(*self.raw_inputs.lock().unwrap(), vec![self.input.clone()]);
        assert_eq!(self.entered.load(Ordering::SeqCst), 1);
        assert_eq!(self.returned.load(Ordering::SeqCst), returned);
        assert_eq!(self.observed.lock().unwrap().len(), 1);
    }
}
struct CountedValidator(Arc<[Target; 2]>);
impl InputValidator for CountedValidator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        let target = self
            .0
            .iter()
            .find(|target| &target.input == input)
            .expect("validator received an unexpected raw input");
        target.raw_inputs.lock().unwrap().push(input.clone());
        assert_eq!(target.validations.fetch_add(1, Ordering::SeqCst), 0);
        Ok(())
    }
}
struct ConcurrentEffect(Arc<[Target; 2]>);
impl NativeEffect for ConcurrentEffect {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        // Trusted context selects the independent gate, never callback arrival order.
        let target = self
            .0
            .iter()
            .find(|target| target.id == context.thread_id.as_str())
            .expect("callback received an unexpected trusted thread ID");
        assert_eq!(target.validations.load(Ordering::SeqCst), 1);
        assert_eq!(
            *target.raw_inputs.lock().unwrap(),
            vec![target.input.clone()]
        );
        assert_eq!(
            invocation.input,
            json!({"input":target.input}).as_object().unwrap().clone()
        );
        target
            .observed
            .lock()
            .unwrap()
            .push((invocation.clone(), context.clone()));
        assert_eq!(target.entered.fetch_add(1, Ordering::SeqCst), 0);
        target.gate.hold();
        assert_eq!(target.returned.fetch_add(1, Ordering::SeqCst), 0);
        // Callback results are literal independently of the test's expected outputs.
        let output = match context.thread_id.as_str() {
            "release-t008-thread-a" => json!({"answer":"independent A", "accepted":true}),
            "release-t008-thread-b" => json!({"answer":"independent B", "count":23}),
            _ => unreachable!("trusted thread ID was already selected"),
        };
        Ok(NativeOutcome::Succeeded(TaskResult {
            output,
            writes: vec![],
        }))
    }
}
fn host_config(app: Arc<NativeApplication>) -> NativeHostConfig {
    let mut configured = config(app, true, 2);
    configured.limits.max_active_runs = 2;
    configured.limits.max_concurrent_tasks = 2;
    configured.limits.max_tasks_per_execution = 1;
    configured
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t008_different_threads_execute_concurrently() {
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let targets = Arc::new([
        Target::new(
            "release-t008-thread-a",
            json!({"question":"A café 🧵", "items":[null,true,17]}),
            json!({"answer":"independent A", "accepted":true}),
        ),
        Target::new(
            "release-t008-thread-b",
            json!({"question":"B 東京", "items":[false,23,"separate"]}),
            json!({"answer":"independent B", "count":23}),
        ),
    ]);
    // Both panic-path release guards exist before any request can start work.
    let _release_a = Release(targets[0].gate.clone());
    let _release_b = Release(targets[1].gate.clone());
    let app = application(
        definition(ROUTE, REVISION, &[("answer", false, RecoveryPolicy::Pure)]),
        Arc::new(ConcurrentEffect(targets.clone())),
        Some(Arc::new(CountedValidator(targets.clone()))),
    );
    // Independent full-column read-only observations; no retained SQL aliases.
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
        host_config(app.clone()),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .expect("listener start deadline");
    assert_ne!(targets[0].id, targets[1].id);
    assert_ne!(targets[0].input, targets[1].input);
    assert_ne!(targets[0].output, targets[1].output);
    for target in targets.iter() {
        assert!(target.request().len() < 1024);
        assert!(store.thread_view(&target.thread_id()).unwrap().is_none());
    }
    assert_eq!(read_durable(), (vec![], vec![], vec![]));
    let socket_a = tokio::time::timeout(
        DEADLINE,
        open(
            server.address,
            "POST",
            &format!("{}/runs/wait", targets[0].path()),
            &targets[0].request(),
        ),
    )
    .await
    .expect("request A open deadline");
    tokio::time::timeout(DEADLINE, targets[0].gate.entered())
        .await
        .expect("callback A entry deadline");
    targets[0].assert_counts(0);
    assert_eq!(targets[1].validations.load(Ordering::SeqCst), 0);
    assert_eq!(targets[1].entered.load(Ordering::SeqCst), 0);
    assert_eq!(targets[1].returned.load(Ordering::SeqCst), 0);
    // Retain the real A socket and its held callback while opening B.
    let socket_b = tokio::time::timeout(
        DEADLINE,
        open(
            server.address,
            "POST",
            &format!("{}/runs/wait", targets[1].path()),
            &targets[1].request(),
        ),
    )
    .await
    .expect("request B open deadline");
    tokio::time::timeout(DEADLINE, targets[1].gate.entered())
        .await
        .expect("callback B entry deadline");
    for target in targets.iter() {
        target.assert_counts(0);
    }
    assert_eq!(
        targets
            .iter()
            .map(|t| t.validations.load(Ordering::SeqCst))
            .sum::<usize>(),
        2
    );
    assert_eq!(
        targets
            .iter()
            .map(|t| t.entered.load(Ordering::SeqCst))
            .sum::<usize>(),
        2
    );
    // Both actual callback bodies entered, and neither returned: decisive overlap.
    let held: Vec<ThreadView> = targets
        .iter()
        .map(|target| store.thread_view(&target.thread_id()).unwrap().unwrap())
        .collect();
    let held_journals: Vec<Vec<JournalEntry>> = held
        .iter()
        .map(|view| {
            store
                .journal(&view.execution.as_ref().unwrap().snapshot.execution_id)
                .unwrap()
        })
        .collect();
    let held_durable = read_durable();
    assert_totals(&held_durable, 8);
    for (index, target) in targets.iter().enumerate() {
        let view = &held[index];
        let snapshot = &view.execution.as_ref().unwrap().snapshot;
        assert_eq!(snapshot.status, ExecutionStatus::Running);
        assert_eq!(snapshot.revision, 2);
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::InFlight);
        assert!(snapshot.tasks[0].result.is_none());
        assert_eq!(snapshot.attempts_used, 1);
        assert_eq!(snapshot.attempt_history.len(), 1);
        assert_eq!(snapshot.output, None);
        assert_eq!(
            snapshot.values,
            json!({"input":target.input}).as_object().unwrap().clone()
        );
        assert_eq!(snapshot.tasks[0].input, snapshot.values);
        assert_provenance(view, target, owner_generation);
        assert_records(
            view,
            &partition(&held_durable, target.id),
            owner_generation,
            &[
                ExecutionStatus::Accepted,
                ExecutionStatus::Running,
                ExecutionStatus::Running,
            ],
            &held_journals[index],
        );
    }
    let first = &held[0].execution.as_ref().unwrap().snapshot;
    let second = &held[1].execution.as_ref().unwrap().snapshot;
    assert_ne!(held[0].thread.generation, held[1].thread.generation);
    assert_ne!(first.execution_id, second.execution_id);
    assert_ne!(first.turn_id, second.turn_id);
    assert_ne!(first.attempt_history[0], second.attempt_history[0]);
    // Recheck the stable dual-held point immediately before either release.
    for (index, target) in targets.iter().enumerate() {
        target.assert_counts(0);
        assert_eq!(
            store.thread_view(&target.thread_id()).unwrap().unwrap(),
            held[index]
        );
    }
    assert_eq!(read_durable(), held_durable);
    targets[0].gate.release();
    targets[1].gate.release();
    let (response_a, response_b) = tokio::join!(
        tokio::time::timeout(DEADLINE, finish(socket_a, vec![])),
        tokio::time::timeout(DEADLINE, finish(socket_b, vec![])),
    );
    let responses = [
        response_a.expect("request A finish deadline"),
        response_b.expect("request B finish deadline"),
    ];
    let views: Vec<ThreadView> = targets
        .iter()
        .map(|target| store.thread_view(&target.thread_id()).unwrap().unwrap())
        .collect();
    let journals: Vec<Vec<JournalEntry>> = views
        .iter()
        .map(|view| {
            store
                .journal(&view.execution.as_ref().unwrap().snapshot.execution_id)
                .unwrap()
        })
        .collect();
    let durable = read_durable();
    assert_totals(&durable, 10);
    assert_eq!(
        &durable.2[..held_durable.2.len()],
        held_durable.2.as_slice()
    );
    for (index, target) in targets.iter().enumerate() {
        target.assert_counts(1);
        assert_eq!(responses[index].status, 200);
        assert_eq!(responses[index].json(), target.output);
        let view = &views[index];
        let snapshot = &view.execution.as_ref().unwrap().snapshot;
        let held_snapshot = &held[index].execution.as_ref().unwrap().snapshot;
        assert_eq!(snapshot.status, ExecutionStatus::Completed);
        assert_eq!(snapshot.revision, 3);
        assert_eq!(
            snapshot.values,
            json!({"input":target.input}).as_object().unwrap().clone()
        );
        assert_eq!(snapshot.output, Some(json!({"input":target.input})));
        assert_ne!(snapshot.output.as_ref(), Some(&target.output));
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
        let result = snapshot.tasks[0].result.as_ref().unwrap();
        assert_eq!(result.output, target.output);
        assert!(result.writes.is_empty());
        assert_eq!(snapshot.attempts_used, 1);
        assert_eq!(snapshot.attempt_history.len(), 1);
        assert_eq!(snapshot.attempt_history, held_snapshot.attempt_history);
        assert_eq!(snapshot.execution_id, held_snapshot.execution_id);
        assert_eq!(snapshot.turn_id, held_snapshot.turn_id);
        assert_eq!(snapshot.tasks[0].input, held_snapshot.tasks[0].input);
        assert_provenance(view, target, owner_generation);
        assert_records(
            view,
            &partition(&durable, target.id),
            owner_generation,
            &[
                ExecutionStatus::Accepted,
                ExecutionStatus::Running,
                ExecutionStatus::Running,
                ExecutionStatus::Completed,
            ],
            &journals[index],
        );
        assert_eq!(
            &journals[index][..held_journals[index].len()],
            held_journals[index].as_slice()
        );
    }
    let mut http_views = vec![];
    for (index, target) in targets.iter().enumerate() {
        let thread = tokio::time::timeout(
            DEADLINE,
            exchange(server.address, "GET", &target.path(), ""),
        )
        .await
        .unwrap();
        let state = tokio::time::timeout(
            DEADLINE,
            exchange(
                server.address,
                "GET",
                &format!("{}/state", target.path()),
                "",
            ),
        )
        .await
        .unwrap();
        assert_http_views(&thread, &state, target, &views[index]);
        http_views.push((thread.body, state.body));
    }
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    for (index, target) in targets.iter().enumerate() {
        assert_eq!(
            store.thread_view(&target.thread_id()).unwrap().unwrap(),
            views[index]
        );
        assert_eq!(
            store
                .journal(
                    &views[index]
                        .execution
                        .as_ref()
                        .unwrap()
                        .snapshot
                        .execution_id
                )
                .unwrap(),
            journals[index]
        );
        target.assert_counts(1);
    }
    assert_eq!(read_durable(), durable);
    // The unchanged listener fixture aborts/joins only after healthy quiescence.
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    drop(shutdown);
    drop(host);
    tokio::time::timeout(DEADLINE, async {
        while Arc::strong_count(&store) != 1 || Arc::strong_count(&app) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired host retained store or app aliases");
    // Synchronously destroy the sole Store before reacquiring its owner lock.
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
    let reopened_weak = Arc::downgrade(&reopened);
    for (index, target) in targets.iter().enumerate() {
        assert_eq!(
            reopened.thread_view(&target.thread_id()).unwrap().unwrap(),
            views[index]
        );
        assert_eq!(
            reopened
                .journal(
                    &views[index]
                        .execution
                        .as_ref()
                        .unwrap()
                        .snapshot
                        .execution_id
                )
                .unwrap(),
            journals[index]
        );
    }
    assert_eq!(read_durable(), durable);
    let host = HttpHost::new_with_application(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        host_config(app.clone()),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .unwrap();
    for (index, target) in targets.iter().enumerate() {
        let thread = tokio::time::timeout(
            DEADLINE,
            exchange(server.address, "GET", &target.path(), ""),
        )
        .await
        .unwrap();
        let state = tokio::time::timeout(
            DEADLINE,
            exchange(
                server.address,
                "GET",
                &format!("{}/state", target.path()),
                "",
            ),
        )
        .await
        .unwrap();
        assert_http_views(&thread, &state, target, &views[index]);
        assert_eq!(thread.body, http_views[index].0);
        assert_eq!(state.body, http_views[index].1);
        assert_eq!(
            reopened.thread_view(&target.thread_id()).unwrap().unwrap(),
            views[index]
        );
        assert_eq!(
            reopened
                .journal(
                    &views[index]
                        .execution
                        .as_ref()
                        .unwrap()
                        .snapshot
                        .execution_id
                )
                .unwrap(),
            journals[index]
        );
        target.assert_counts(1);
    }
    assert_eq!(read_durable(), durable);
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    for (index, target) in targets.iter().enumerate() {
        assert_eq!(
            reopened.thread_view(&target.thread_id()).unwrap().unwrap(),
            views[index]
        );
        assert_eq!(
            reopened
                .journal(
                    &views[index]
                        .execution
                        .as_ref()
                        .unwrap()
                        .snapshot
                        .execution_id
                )
                .unwrap(),
            journals[index]
        );
        target.assert_counts(1);
    }
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
    .expect("reopened host retained store or app aliases");
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("store alias survived cleanup")));
    assert!(reopened_weak.upgrade().is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    for target in targets.iter() {
        target.assert_counts(1);
    }
    drop(directory);
    assert!(!directory_path.exists(), "native directory must be removed");
}

fn assert_http_views(thread: &Response, state: &Response, target: &Target, view: &ThreadView) {
    assert_eq!(thread.status, 200);
    let thread_json = thread.json();
    assert_eq!(thread_json["thread_id"], target.id);
    assert_eq!(thread_json["metadata"], json!({}));
    assert_eq!(thread_json["status"], "idle");
    assert_eq!(state.status, 200);
    let state_json = state.json();
    assert_eq!(state_json["config"]["configurable"]["thread_id"], target.id);
    assert_eq!(
        state_json["config"]["configurable"]["checkpoint_id"],
        view.execution
            .as_ref()
            .unwrap()
            .snapshot
            .execution_id
            .as_str()
    );
    assert_eq!(state_json["metadata"]["route"], ROUTE);
    assert_eq!(state_json["metadata"]["execution_revision"], REVISION);
    assert_eq!(state_json["values"], json!({"input":target.input}));
}

type DurableRows = (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>);
fn partition(durable: &DurableRows, id: &str) -> DurableRows {
    let selected = SqlValue::Text(id.into());
    (
        durable
            .0
            .iter()
            .filter(|row| row[0] == selected)
            .cloned()
            .collect(),
        durable
            .1
            .iter()
            .filter(|row| row[1] == selected)
            .cloned()
            .collect(),
        durable
            .2
            .iter()
            .filter(|row| row[2] == selected)
            .cloned()
            .collect(),
    )
}
fn assert_totals(durable: &DurableRows, journal_rows: usize) {
    assert_eq!(durable.0.len(), 2);
    assert_eq!(durable.1.len(), 2);
    assert_eq!(durable.2.len(), journal_rows);
    // Only the database's own sequence is ordered; no A/B commit order is imposed.
    for (index, row) in durable.2.iter().enumerate() {
        assert_eq!(row[0], SqlValue::Integer(i64::try_from(index + 1).unwrap()));
    }
}

fn assert_records(
    view: &ThreadView,
    durable: &DurableRows,
    owner_generation: u64,
    statuses: &[ExecutionStatus],
    typed_journal: &[JournalEntry],
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
            durable.2[0][0].clone(),
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
    assert_eq!(typed_journal.len(), statuses.len());
    // One creation row plus the source-backed single-Pure-task execution chain.
    assert_eq!(durable.2.len(), statuses.len() + 1);
    for (index, row) in durable.2.iter().enumerate().skip(1) {
        let revision = u64::try_from(index - 1).unwrap();
        let (SqlValue::Integer(previous), SqlValue::Integer(sequence)) =
            (&durable.2[index - 1][0], &row[0])
        else {
            panic!("journal sequence must be an integer")
        };
        assert!(sequence > previous);
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
        assert_eq!(entry, typed_journal[index - 1]);
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

fn assert_provenance(view: &ThreadView, target: &Target, owner_generation: u64) {
    assert_eq!(view.thread.id.as_str(), target.id);
    let observed = target.observed.lock().unwrap();
    let input = &target.input;
    assert!(view.thread.metadata.is_empty());
    assert_ne!(view.thread.generation.as_str(), "0".repeat(64));

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
    assert_eq!(view.thread.resource_revision, snapshot.revision + 1);
    assert_eq!(snapshot.turn_history, vec![snapshot.turn_id.clone()]);
    assert!(snapshot.failure.is_none());
    assert_eq!(
        snapshot.definition,
        definition(ROUTE, REVISION, &[("answer", false, RecoveryPolicy::Pure)])
    );
    assert_eq!(snapshot.tasks.len(), 1);
    assert_eq!(snapshot.tasks[0].spec, snapshot.definition.batches[0][0]);
    assert!(snapshot.tasks[0].failure.is_none());
    assert_eq!(observed.len(), 1);
    let (call, context) = &observed[0];
    assert_eq!(call.execution_id, snapshot.execution_id);
    assert_eq!(call.owner_generation, owner_generation);
    assert_eq!(call.turn_id, snapshot.turn_id);
    assert_eq!(call.task_id.as_str(), "answer");
    assert_eq!(call.effect_id.as_str(), "answer");
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
            execution_revision: ExecutionRevision::new(REVISION).unwrap(),
            policy_revision: "policy-v1".into(),
        }
    );
}
