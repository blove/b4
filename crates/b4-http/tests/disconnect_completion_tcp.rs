//! T012: closing the sole stream/wait client after callback entry preserves work.
use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};

mod native_support;
use native_support::{
    application, config, definition, open, until, Auth, Directory, Owner, Response,
};

const DEADLINE: Duration = Duration::from_secs(10);
const HOLD_LIMIT: Duration = Duration::from_secs(30);
const ROUTE: &str = "/disconnect-completion#workflow";
const REVISION: &str = "disconnect-completion-v1";

#[derive(Default)]
struct Gate {
    state: Mutex<(bool, Option<Instant>)>,
    changed: Condvar,
}
impl Gate {
    fn hold(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(state.1.is_none(), "gate may be entered only once");
        let began = Instant::now();
        state.1 = Some(began);
        let deadline = began + HOLD_LIMIT;
        while !state.0 {
            let left = deadline
                .checked_duration_since(Instant::now())
                .expect("callback's finite gate deadline expired");
            let (next, _) = self.changed.wait_timeout(state, left).unwrap();
            state = next;
        }
    }
    fn is_held(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.1.is_some() && !state.0
    }
    fn assert_held_before_deadline(&self) {
        let state = self.state.lock().unwrap();
        assert!(!state.0);
        assert!(state.1.expect("callback actually entered gate").elapsed() < HOLD_LIMIT);
    }
    fn release(&self) {
        self.state.lock().unwrap().0 = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct Target {
    id: &'static str,
    streaming: bool,
    input: Value,
    gate: Arc<Gate>,
    validations: AtomicUsize,
    validated: Mutex<Vec<Value>>,
    streaming_entries: AtomicUsize,
    controlled_entries: AtomicUsize,
    direct_entries: AtomicUsize,
    entered: AtomicUsize,
    returned: AtomicUsize,
    calls: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
    observer: Mutex<Option<NativeCancellationObserver>>,
    clear_checks: Mutex<Vec<&'static str>>,
}
impl Target {
    fn new(id: &'static str, streaming: bool, input: Value) -> Self {
        Self {
            id,
            streaming,
            input,
            gate: Arc::new(Gate::default()),
            validations: AtomicUsize::new(0),
            validated: Mutex::new(vec![]),
            streaming_entries: AtomicUsize::new(0),
            controlled_entries: AtomicUsize::new(0),
            direct_entries: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            returned: AtomicUsize::new(0),
            calls: Mutex::new(vec![]),
            observer: Mutex::new(None),
            clear_checks: Mutex::new(vec![]),
        }
    }
    fn thread_id(&self) -> ThreadId {
        ThreadId::new(self.id).unwrap()
    }
    fn path(&self) -> String {
        format!("/threads/{}", self.id)
    }
    fn assert_counts(&self, admitted: usize, returned: usize) {
        assert_eq!(self.validations.load(Ordering::SeqCst), admitted);
        assert_eq!(
            *self.validated.lock().unwrap(),
            if admitted == 0 {
                vec![]
            } else {
                vec![self.input.clone()]
            }
        );
        assert_eq!(
            self.streaming_entries.load(Ordering::SeqCst),
            if self.streaming { admitted } else { 0 }
        );
        assert_eq!(
            self.controlled_entries.load(Ordering::SeqCst),
            if self.streaming { 0 } else { admitted }
        );
        assert_eq!(self.direct_entries.load(Ordering::SeqCst), 0);
        assert_eq!(self.entered.load(Ordering::SeqCst), admitted);
        assert_eq!(self.returned.load(Ordering::SeqCst), returned);
        assert_eq!(self.calls.lock().unwrap().len(), admitted);
        assert_eq!(self.observer.lock().unwrap().is_some(), admitted == 1);
        if admitted == 1 {
            self.assert_clear();
        }
        if admitted == 1 && returned == 0 {
            self.gate.assert_held_before_deadline();
        }
        let expected = if admitted == 0 {
            vec![]
        } else if returned == 0 {
            vec!["before_hold"]
        } else {
            vec!["before_hold", "before_return"]
        };
        assert_eq!(*self.clear_checks.lock().unwrap(), expected);
    }
    fn assert_clear(&self) {
        assert!(!self
            .observer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .cancellation_requested());
    }
    // Acceptance literals are independent of the callback's fixed return below.
    fn expected_result(&self) -> TaskResult {
        if self.streaming {
            TaskResult {
                output: json!({"answer":"stream survived","items":[true,17,null]}),
                writes: vec![
                    StateWrite::Replace {
                        field: "aggregate".into(),
                        value: json!({"saved":"stream state","n":101}),
                    },
                    StateWrite::Append {
                        field: "trail".into(),
                        items: vec![json!("stream committed"), json!(17)],
                    },
                ],
            }
        } else {
            TaskResult {
                output: json!({"answer":"wait survived","items":[false,29,"東京"]}),
                writes: vec![
                    StateWrite::Replace {
                        field: "aggregate".into(),
                        value: json!({"saved":"wait state","n":202}),
                    },
                    StateWrite::Append {
                        field: "trail".into(),
                        items: vec![json!("wait committed"), json!(29)],
                    },
                ],
            }
        }
    }
    fn expected_values(&self) -> Value {
        if self.streaming {
            json!({"input":{"question":"stream café 🧵","seed":11},"aggregate":{"saved":"stream state","n":101},"trail":["stream committed",17]})
        } else {
            json!({"input":{"question":"wait 東京","seed":23},"aggregate":{"saved":"wait state","n":202},"trail":["wait committed",29]})
        }
    }
}
struct Validator(Arc<[Target; 2]>);
impl InputValidator for Validator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        let target = self
            .0
            .iter()
            .find(|t| t.input == *input)
            .expect("unexpected raw input");
        target.validated.lock().unwrap().push(input.clone());
        assert_eq!(target.validations.fetch_add(1, Ordering::SeqCst), 0);
        Ok(())
    }
}
struct Callback(Arc<[Target; 2]>);
impl Callback {
    fn target(&self, context: &InvocationContext) -> &Target {
        self.0
            .iter()
            .find(|t| t.id == context.thread_id.as_str())
            .expect("unexpected trusted ID")
    }
    fn complete(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        let target = self.target(context);
        assert_eq!(target.validations.load(Ordering::SeqCst), 1);
        assert_eq!(
            *target.validated.lock().unwrap(),
            vec![target.input.clone()]
        );
        assert_eq!(
            call.input,
            json!({"input":target.input}).as_object().unwrap().clone()
        );
        assert!(!observer.cancellation_requested());
        target
            .calls
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        *target.observer.lock().unwrap() = Some(observer.clone());
        target.clear_checks.lock().unwrap().push("before_hold");
        assert_eq!(target.entered.fetch_add(1, Ordering::SeqCst), 0);
        target.gate.hold();
        assert!(!observer.cancellation_requested());
        target.clear_checks.lock().unwrap().push("before_return");
        // Fixed producer literals: never call expected_result/expected_values here.
        let result = match context.thread_id.as_str() {
            "release-t012-stream" => TaskResult {
                output: json!({"answer":"stream survived","items":[true,17,null]}),
                writes: vec![
                    StateWrite::Replace {
                        field: "aggregate".into(),
                        value: json!({"saved":"stream state","n":101}),
                    },
                    StateWrite::Append {
                        field: "trail".into(),
                        items: vec![json!("stream committed"), json!(17)],
                    },
                ],
            },
            "release-t012-wait" => TaskResult {
                output: json!({"answer":"wait survived","items":[false,29,"東京"]}),
                writes: vec![
                    StateWrite::Replace {
                        field: "aggregate".into(),
                        value: json!({"saved":"wait state","n":202}),
                    },
                    StateWrite::Append {
                        field: "trail".into(),
                        items: vec![json!("wait committed"), json!(29)],
                    },
                ],
            },
            _ => unreachable!(),
        };
        assert_eq!(target.returned.fetch_add(1, Ordering::SeqCst), 0);
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
            result,
        )))
    }
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.target(context)
            .direct_entries
            .fetch_add(1, Ordering::SeqCst);
        panic!("unexpected plain callback dispatch")
    }
    fn invoke_streaming(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
        _: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        let target = self.target(context);
        assert!(target.streaming);
        assert_eq!(target.streaming_entries.fetch_add(1, Ordering::SeqCst), 0);
        self.complete(call, context, observer)
    }
    fn invoke_controlled(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        let target = self.target(context);
        assert!(!target.streaming);
        assert_eq!(target.controlled_entries.fetch_add(1, Ordering::SeqCst), 0);
        self.complete(call, context, observer)
    }
}
fn host_config(app: Arc<NativeApplication>) -> NativeHostConfig {
    let mut c = config(app, true, 1);
    c.limits.max_concurrent_tasks = 1;
    c.limits.max_tasks_per_execution = 1;
    c
}
// Local fixture: panic drops abort the listener; healthy retirement precedes stop.
struct LocalListener {
    address: SocketAddr,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl LocalListener {
    async fn start<S: Store + 'static>(host: &HttpHost<S>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = host.router();
        Self {
            address,
            task: Some(tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap()
            })),
        }
    }
    async fn stop(mut self) {
        let task = self.task.take().unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
}
impl Drop for LocalListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: "native-test".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
fn expected_definition() -> Definition {
    definition(ROUTE, REVISION, &[("answer", false, RecoveryPolicy::Pure)])
}
fn healthy(status: ShutdownStatus) {
    assert_eq!(status.phase, ShutdownPhase::Serving);
    assert!(!status.faulted);
}
async fn retired(shutdown: &ShutdownHandle) {
    until(|| {
        let s = shutdown.status();
        healthy(s);
        s.active_owners == 0
    })
    .await;
}

// Each durable read uses a fresh read-only connection, closed before returning.
type Rows = (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>);
fn rows(path: &Path) -> Rows {
    let db = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    let read = |sql: &str, width: usize| {
        let mut q = db.prepare(sql).unwrap();
        let result = q
            .query_map([], |row| {
                (0..width)
                    .map(|i| row.get::<_, SqlValue>(i))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        result
    };
    (
        read("SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id", 7),
        read("SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id", 5),
        read("SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence", 9),
    )
}
fn text(s: impl Into<String>) -> SqlValue {
    SqlValue::Text(s.into())
}
fn blob(n: u64) -> SqlValue {
    SqlValue::Blob(n.to_be_bytes().to_vec())
}
fn partition(raw: &Rows, id: &str) -> Rows {
    let id = text(id);
    (
        raw.0.iter().filter(|r| r[0] == id).cloned().collect(),
        raw.1.iter().filter(|r| r[1] == id).cloned().collect(),
        raw.2.iter().filter(|r| r[2] == id).cloned().collect(),
    )
}
fn totals(raw: &Rows, threads: usize, journal: usize) {
    assert_eq!(
        (raw.0.len(), raw.1.len(), raw.2.len()),
        (threads, threads, journal)
    );
    for (i, row) in raw.2.iter().enumerate() {
        assert_eq!(row[0], SqlValue::Integer((i + 1).try_into().unwrap()));
    }
}

// Rebuild the entire typed view, including every task/attempt/provenance field.
// Only server-generated IDs and observed timestamps are captured, never outputs.
fn assert_view(
    target: &Target,
    view: &ThreadView,
    generation: u64,
    completed: bool,
    lower: u64,
    upper: u64,
) {
    target.assert_counts(1, usize::from(completed));
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    assert!((lower..=upper).contains(&view.thread.created_at_ms));
    assert!((view.thread.created_at_ms..=upper).contains(&snapshot.updated_at_ms));
    assert_eq!(view.thread.generation.as_str().len(), 64);
    assert_ne!(view.thread.generation.as_str(), "0".repeat(64));
    assert!(view
        .thread
        .generation
        .as_str()
        .bytes()
        .all(|b| b.is_ascii_hexdigit()));
    let calls = target.calls.lock().unwrap();
    let (call, context) = &calls[0];
    let expected_input = json!({"input":target.input}).as_object().unwrap().clone();
    assert_eq!(
        call,
        &EffectInvocation {
            execution_id: snapshot.execution_id.clone(),
            turn_id: snapshot.turn_id.clone(),
            owner_generation: generation,
            task_id: TaskId::new("answer").unwrap(),
            effect_id: EffectId::new("answer").unwrap(),
            attempt_id: call.attempt_id.clone(),
            input: expected_input.clone(),
        }
    );
    assert_eq!(
        context,
        &InvocationContext {
            thread_id: target.thread_id(),
            thread_generation: view.thread.generation.clone(),
            origin: origin(),
            application_revision: "application-v1".into(),
            execution_revision: ExecutionRevision::new(REVISION).unwrap(),
            policy_revision: "policy-v1".into(),
        }
    );
    let definition = expected_definition();
    let values = if completed {
        target.expected_values().as_object().unwrap().clone()
    } else {
        expected_input.clone()
    };
    assert_eq!(
        view,
        &ThreadView {
            thread: ThreadRecord {
                id: target.thread_id(),
                generation: context.thread_generation.clone(),
                active_execution_id: Some(call.execution_id.clone()),
                created_at_ms: view.thread.created_at_ms,
                metadata: Default::default(),
                origin: Some(origin()),
                resource_revision: if completed { 4 } else { 3 },
            },
            execution: Some(StoredExecution {
                thread_id: target.thread_id(),
                snapshot: Snapshot {
                    schema_version: 1,
                    revision: if completed { 3 } else { 2 },
                    execution_id: call.execution_id.clone(),
                    turn_id: call.turn_id.clone(),
                    turn_history: vec![call.turn_id.clone()],
                    attempt_history: vec![call.attempt_id.clone()],
                    owner_generation: generation,
                    definition: definition.clone(),
                    status: if completed {
                        ExecutionStatus::Completed
                    } else {
                        ExecutionStatus::Running
                    },
                    batch_index: usize::from(completed),
                    values,
                    tasks: vec![TaskRecord {
                        spec: definition.batches[0][0].clone(),
                        status: if completed {
                            TaskStatus::Succeeded
                        } else {
                            TaskStatus::InFlight
                        },
                        input: expected_input,
                        attempt_id: Some(call.attempt_id.clone()),
                        result: completed.then(|| target.expected_result()),
                        failure: None,
                        interrupt: None,
                        approved: false,
                    }],
                    attempts_used: 1,
                    updated_at_ms: snapshot.updated_at_ms,
                    output: completed.then(|| target.expected_values()),
                    failure: None,
                }
            }),
        }
    );
    if completed {
        assert_ne!(snapshot.output, Some(target.expected_result().output));
    }
}
#[allow(clippy::too_many_arguments)] // Keep independent full-column oracle inputs explicit.
fn assert_raw(
    target: &Target,
    index: usize,
    view: &ThreadView,
    journal: &[JournalEntry],
    raw: &Rows,
    generation: u64,
    completed: bool,
    upper: u64,
) {
    let s = &view.execution.as_ref().unwrap().snapshot;
    let revision = if completed { 3 } else { 2 };
    assert_eq!(
        raw.0,
        vec![vec![
            text(target.id),
            text(s.execution_id.as_str()),
            blob(view.thread.created_at_ms),
            text("{}"),
            text(serde_json::to_string(&origin()).unwrap()),
            blob(revision + 1),
            text(view.thread.generation.as_str())
        ]]
    );
    assert_eq!(raw.1.len(), 1);
    assert_eq!(raw.1[0].len(), 5);
    assert_eq!(
        &raw.1[0][..4],
        &[
            text(s.execution_id.as_str()),
            text(target.id),
            blob(revision),
            blob(generation)
        ]
    );
    let SqlValue::Text(encoded) = &raw.1[0][4] else {
        panic!("snapshot text")
    };
    assert_eq!(serde_json::from_str::<Snapshot>(encoded).unwrap(), *s);
    // Kernel Start, Dispatch, Complete yield Running1, Running2, Completed3.
    // SQLite admission contributes Accepted0 plus a separate thread creation row.
    let statuses = [
        ExecutionStatus::Accepted,
        ExecutionStatus::Running,
        ExecutionStatus::Running,
        ExecutionStatus::Completed,
    ];
    let count = if completed { 4 } else { 3 };
    assert_eq!(journal.len(), count);
    assert_eq!(raw.2.len(), count + 1);
    let first = i64::try_from(index * 5 + 1).unwrap();
    assert_eq!(
        raw.2[0],
        vec![
            SqlValue::Integer(first),
            text("thread"),
            text(target.id),
            SqlValue::Null,
            SqlValue::Null,
            blob(generation),
            SqlValue::Null,
            blob(view.thread.created_at_ms),
            SqlValue::Null
        ]
    );
    let mut previous_time = view.thread.created_at_ms;
    for i in 0..count {
        let expected = JournalEntry {
            execution_id: s.execution_id.clone(),
            revision: i.try_into().unwrap(),
            owner_generation: generation,
            status: statuses[i],
            updated_at_ms: journal[i].updated_at_ms,
        };
        assert_eq!(journal[i], expected);
        assert!((previous_time..=upper).contains(&expected.updated_at_ms));
        previous_time = expected.updated_at_ms;
        let row = &raw.2[i + 1];
        assert_eq!(row.len(), 9);
        assert_eq!(
            &row[..8],
            &[
                SqlValue::Integer(first + i64::try_from(i + 1).unwrap()),
                text(if i == 0 { "admit" } else { "commit" }),
                text(target.id),
                text(s.execution_id.as_str()),
                blob(i.try_into().unwrap()),
                blob(generation),
                text(serde_json::to_string(&statuses[i]).unwrap()),
                blob(expected.updated_at_ms)
            ]
        );
        let SqlValue::Text(payload) = &row[8] else {
            panic!("journal text")
        };
        assert_eq!(
            serde_json::from_str::<JournalEntry>(payload).unwrap(),
            expected
        );
    }
    assert_eq!(previous_time, s.updated_at_ms);
}
fn timestamp(ms: u64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}
// GET responses are finite content-length JSON. Preserve header-adjacent bytes.
async fn get(address: SocketAddr, path: &str) -> Response {
    tokio::time::timeout(DEADLINE, async {
        let socket = open(address, "GET", path, "").await;
        let mut bytes = vec![];
        socket.take(65_537).read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.len() <= 65_536, "GET byte cap");
        let split = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let header = std::str::from_utf8(&bytes[..split]).unwrap();
        let mut lines = header.split("\r\n");
        assert_eq!(lines.next(), Some("HTTP/1.1 200 OK"));
        let mut length = None;
        for line in lines {
            let (name, value) = line.split_once(':').unwrap();
            assert!(!name.eq_ignore_ascii_case("transfer-encoding"));
            if name.eq_ignore_ascii_case("content-length") {
                assert!(length
                    .replace(value.trim().parse::<usize>().unwrap())
                    .is_none());
            }
        }
        let body = &bytes[split + 4..];
        assert_eq!(Some(body.len()), length);
        Response {
            status: 200,
            body: String::from_utf8(body.to_vec()).unwrap(),
        }
    })
    .await
    .expect("bounded GET deadline")
}
async fn assert_gets(address: SocketAddr, target: &Target, view: &ThreadView) -> (String, String) {
    let thread = get(address, &target.path()).await;
    let state = get(address, &format!("{}/state", target.path())).await;
    let s = &view.execution.as_ref().unwrap().snapshot;
    assert_eq!(thread.status, 200);
    assert_eq!(
        thread.json(),
        json!({"thread_id":target.id,"created_at":timestamp(view.thread.created_at_ms),"updated_at":timestamp(s.updated_at_ms),"metadata":{},"status":"idle"})
    );
    assert_eq!(state.status, 200);
    assert_eq!(
        state.json(),
        json!({"config":{"configurable":{"thread_id":target.id,"checkpoint_id":s.execution_id,"checkpoint_ns":""}},"created_at":timestamp(s.updated_at_ms),"metadata":{"source":"native_scripted","revision":3,"route":ROUTE,"execution_revision":REVISION},"next":[],"parent_config":null,"values":target.expected_values()})
    );
    (thread.body, state.body)
}
fn assert_unchanged<S: Store>(
    store: &S,
    targets: &[Target; 2],
    views: &[ThreadView],
    journals: &[Vec<JournalEntry>],
) {
    for (i, view) in views.iter().enumerate() {
        targets[i].assert_counts(1, 1);
        assert_eq!(
            store.thread_view(&targets[i].thread_id()).unwrap().as_ref(),
            Some(view)
        );
        let e = view.execution.as_ref().unwrap();
        assert_eq!(
            store.execution(&e.snapshot.execution_id).unwrap().as_ref(),
            Some(e)
        );
        assert_eq!(
            store.journal(&e.snapshot.execution_id).unwrap(),
            journals[i]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t012_disconnected_stream_and_wait_preserve_admitted_work() {
    let directory = Directory::new();
    let path = directory.0.clone();
    let targets = Arc::new([
        Target::new(
            "release-t012-stream",
            true,
            json!({"question":"stream café 🧵","seed":11}),
        ),
        Target::new(
            "release-t012-wait",
            false,
            json!({"question":"wait 東京","seed":23}),
        ),
    ]);
    // Installed before any admission; callback waits have their own finite limit.
    let _release_stream = Release(targets[0].gate.clone());
    let _release_wait = Release(targets[1].gate.clone());
    let app = application(
        expected_definition(),
        Arc::new(Callback(targets.clone())),
        Some(Arc::new(Validator(targets.clone()))),
    );
    let weak_app = Arc::downgrade(&app);
    let store = Arc::new(b4_sqlite::SqliteStore::open(&path).unwrap());
    let weak_store = Arc::downgrade(&store);
    let generation = store.owner_generation();
    let host = HttpHost::new_with_streaming_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        host_config(app.clone()),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, LocalListener::start(&host))
        .await
        .unwrap();
    let address = server.address;
    let shutdown = host.shutdown_handle();
    assert_ne!(targets[0].id, targets[1].id);
    assert_ne!(targets[0].input, targets[1].input);
    for target in targets.iter() {
        target.assert_counts(0, 0);
        assert!(store.thread_view(&target.thread_id()).unwrap().is_none());
    }
    assert_eq!(rows(&path), (vec![], vec![], vec![]));
    let mut views: Vec<ThreadView> = vec![];
    let mut journals: Vec<Vec<JournalEntry>> = vec![];
    let mut prior_raw = rows(&path);
    let mut http = vec![];
    for (index, target) in targets.iter().enumerate() {
        // Capacity one is actually retired before each sequential admission.
        retired(&shutdown).await;
        assert_eq!(shutdown.status().active_owners, 0);
        let lower = now_ms();
        tokio::time::timeout(DEADLINE, async {
            let body = json!({"route":ROUTE,"input":target.input}).to_string();
            assert!(body.len() < 1024);
            let endpoint = if target.streaming { "stream" } else { "wait" };
            // open owns the whole sole socket. In particular, wait does not read
            // response headers, which are selected only after completion.
            let client = open(
                address,
                "POST",
                &format!("{}/runs/{endpoint}", target.path()),
                &body,
            )
            .await;
            until(|| target.entered.load(Ordering::SeqCst) == 1 && target.gate.is_held()).await;
            target.assert_counts(1, 0);
            let status = shutdown.status();
            healthy(status);
            assert!(status.active_owners > 0);
            let held = store.thread_view(&target.thread_id()).unwrap().unwrap();
            let execution = held.execution.as_ref().unwrap();
            assert_eq!(
                store
                    .execution(&execution.snapshot.execution_id)
                    .unwrap()
                    .as_ref(),
                Some(execution)
            );
            let held_journal = store.journal(&execution.snapshot.execution_id).unwrap();
            let held_raw = rows(&path);
            totals(&held_raw, index + 1, index * 5 + 4);
            let upper = now_ms();
            assert_view(target, &held, generation, false, lower, upper);
            assert_raw(
                target,
                index,
                &held,
                &held_journal,
                &partition(&held_raw, target.id),
                generation,
                false,
                upper,
            );
            assert_unchanged(store.as_ref(), &targets, &views, &journals);
            for (i, previous) in views.iter().enumerate() {
                assert_eq!(
                    partition(&held_raw, targets[i].id),
                    partition(&prior_raw, targets[i].id)
                );
                let p = &previous.execution.as_ref().unwrap().snapshot;
                assert_ne!(held.thread.generation, previous.thread.generation);
                assert_ne!(execution.snapshot.execution_id, p.execution_id);
                assert_ne!(execution.snapshot.turn_id, p.turn_id);
                assert_ne!(execution.snapshot.attempt_history, p.attempt_history);
            }
            // Concrete local close only: no quiet interval or claimed server FIN
            // observation. No socket clone, split half, viewer/result is retained.
            drop(client);
            target.assert_clear();
            target.assert_counts(1, 0);
            assert_eq!(
                store.thread_view(&target.thread_id()).unwrap().as_ref(),
                Some(&held)
            );
            assert_eq!(
                store.journal(&execution.snapshot.execution_id).unwrap(),
                held_journal
            );
            assert_eq!(rows(&path), held_raw);
            let status = shutdown.status();
            healthy(status);
            assert!(
                status.active_owners > 0,
                "held admitted work remains owned after local close"
            );
            assert!(weak_store.upgrade().is_some()); // Supporting, not ownership proof.
            target.assert_clear();
            target.gate.assert_held_before_deadline();
            target.gate.release();
            until(|| {
                let current = store.thread_view(&target.thread_id()).unwrap().unwrap();
                let s = &current.execution.as_ref().unwrap().snapshot;
                s.status == ExecutionStatus::Completed
                    && s.tasks.len() == 1
                    && s.tasks[0].status == TaskStatus::Succeeded
                    && s.tasks[0].result.is_some()
            })
            .await;
            retired(&shutdown).await;
            target.assert_counts(1, 1);
            let final_view = store.thread_view(&target.thread_id()).unwrap().unwrap();
            let s = &final_view.execution.as_ref().unwrap().snapshot;
            let final_journal = store.journal(&s.execution_id).unwrap();
            let raw = rows(&path);
            totals(&raw, index + 1, (index + 1) * 5);
            let upper = now_ms();
            assert_view(target, &final_view, generation, true, lower, upper);
            assert_raw(
                target,
                index,
                &final_view,
                &final_journal,
                &partition(&raw, target.id),
                generation,
                true,
                upper,
            );
            assert_eq!(
                &final_journal[..held_journal.len()],
                held_journal.as_slice()
            );
            assert_eq!(&raw.2[..held_raw.2.len()], held_raw.2.as_slice());
            assert_eq!(final_view.thread.generation, held.thread.generation);
            assert_eq!(s.execution_id, execution.snapshot.execution_id);
            assert_eq!(s.turn_id, execution.snapshot.turn_id);
            assert_eq!(s.attempt_history, execution.snapshot.attempt_history);
            assert_eq!(s.tasks[0].input, execution.snapshot.tasks[0].input);
            assert_unchanged(store.as_ref(), &targets, &views, &journals);
            for (i, _) in views.iter().enumerate() {
                assert_eq!(
                    partition(&raw, targets[i].id),
                    partition(&prior_raw, targets[i].id)
                );
            }
            views.push(final_view);
            journals.push(final_journal);
            prior_raw = raw;
        })
        .await
        .expect("admission, local close, release and durable settlement deadline");
        for pending in &targets[index + 1..] {
            pending.assert_counts(0, 0);
        }
        http.push(assert_gets(address, target, &views[index]).await);
    }
    let durable = rows(&path);
    totals(&durable, 2, 10);
    assert_eq!(durable, prior_raw);
    assert_unchanged(store.as_ref(), &targets, &views, &journals);
    retired(&shutdown).await;
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_unchanged(store.as_ref(), &targets, &views, &journals);
    assert_eq!(rows(&path), durable);
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&store) == 1 && Arc::strong_count(&app) == 1).await;
    // Physically destroy the sole store after quiescence, then reacquire its lock.
    drop(Arc::try_unwrap(store).unwrap_or_else(|_| panic!("original Store aliases survived")));
    assert!(weak_store.upgrade().is_none());
    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&path).unwrap());
    let weak_reopened = Arc::downgrade(&reopened);
    assert_eq!(reopened.owner_generation(), generation + 1);
    assert_unchanged(reopened.as_ref(), &targets, &views, &journals);
    assert_eq!(rows(&path), durable);
    let host = HttpHost::new_with_streaming_application(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        host_config(app.clone()),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, LocalListener::start(&host))
        .await
        .unwrap();
    let address = server.address;
    let shutdown = host.shutdown_handle();
    // Fresh host serves GETs only: no admission, callback, validation or replay.
    for (index, target) in targets.iter().enumerate() {
        assert_eq!(
            assert_gets(address, target, &views[index]).await,
            http[index]
        );
        assert_unchanged(reopened.as_ref(), &targets, &views, &journals);
        assert_eq!(rows(&path), durable);
        assert_view(
            target,
            &views[index],
            generation,
            true,
            views[index].thread.created_at_ms,
            now_ms(),
        );
        assert_raw(
            target,
            index,
            &views[index],
            &journals[index],
            &partition(&durable, target.id),
            generation,
            true,
            now_ms(),
        );
    }
    retired(&shutdown).await;
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_unchanged(reopened.as_ref(), &targets, &views, &journals);
    assert_eq!(rows(&path), durable);
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&reopened) == 1 && Arc::strong_count(&app) == 1).await;
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("fresh Store aliases survived")));
    assert!(weak_reopened.upgrade().is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    for target in targets.iter() {
        target.assert_counts(1, 1);
    }
    drop(directory);
    assert!(!path.exists(), "fixture directory must be removed");
}
