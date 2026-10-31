//! T013: live and settled native attachments start with state and execute no work.
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
const ROUTE: &str = "/attachment-state#workflow";
const REVISION: &str = "attachment-state-v1";

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
    fn new() -> Self {
        Self {
            input: json!({"question":"attachment café 東京 🧵","seed":41,"flags":[true,null]}),
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
        ThreadId::new("release-t013-attachment-state").unwrap()
    }
    fn path(&self) -> String {
        format!("/threads/{}", self.thread_id().as_str())
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
        assert_eq!(self.streaming_entries.load(Ordering::SeqCst), 0);
        assert_eq!(self.direct_entries.load(Ordering::SeqCst), 0);
        assert_eq!(self.controlled_entries.load(Ordering::SeqCst), admitted);
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
        assert_eq!(
            *self.clear_checks.lock().unwrap(),
            if admitted == 0 {
                vec![]
            } else if returned == 0 {
                vec!["before_hold"]
            } else {
                vec!["before_hold", "before_return"]
            }
        );
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
    // Independent oracle literals; the producer does not use these functions.
    fn expected_result(&self) -> TaskResult {
        TaskResult {
            output: json!({"answer":"one native answer","items":[false,53,"雪"]}),
            writes: vec![
                StateWrite::Replace {
                    field: "aggregate".into(),
                    value: json!({"saved":"durable attachment state","n":307}),
                },
                StateWrite::Append {
                    field: "trail".into(),
                    items: vec![json!("one commit"), json!(53)],
                },
            ],
        }
    }
    fn expected_values(&self) -> Value {
        json!({"input":{"question":"attachment café 東京 🧵","seed":41,"flags":[true,null]},"aggregate":{"saved":"durable attachment state","n":307},"trail":["one commit",53]})
    }
}
struct Validator(Arc<Target>);
impl InputValidator for Validator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.validated.lock().unwrap().push(input.clone());
        assert_eq!(self.0.validations.fetch_add(1, Ordering::SeqCst), 0);
        Ok(())
    }
}
struct Callback(Arc<Target>);
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.direct_entries.fetch_add(1, Ordering::SeqCst);
        panic!("unexpected plain callback entry")
    }
    fn invoke_streaming(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
        _: &NativeCancellationObserver,
        _: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.0.streaming_entries.fetch_add(1, Ordering::SeqCst);
        panic!("wait/attachment must not select streaming callback")
    }
    fn invoke_controlled(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        let target = &self.0;
        assert_eq!(target.controlled_entries.fetch_add(1, Ordering::SeqCst), 0);
        assert_eq!(target.validations.load(Ordering::SeqCst), 1);
        assert_eq!(
            *target.validated.lock().unwrap(),
            vec![json!({"question":"attachment café 東京 🧵","seed":41,"flags":[true,null]})]
        );
        target
            .calls
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        assert!(!observer.cancellation_requested());
        *target.observer.lock().unwrap() = Some(observer.clone());
        target.clear_checks.lock().unwrap().push("before_hold");
        assert_eq!(target.entered.fetch_add(1, Ordering::SeqCst), 0);
        target.gate.hold();
        assert!(!observer.cancellation_requested());
        target.clear_checks.lock().unwrap().push("before_return");
        let result = TaskResult {
            output: json!({"answer":"one native answer","items":[false,53,"雪"]}),
            writes: vec![
                StateWrite::Replace {
                    field: "aggregate".into(),
                    value: json!({"saved":"durable attachment state","n":307}),
                },
                StateWrite::Append {
                    field: "trail".into(),
                    items: vec![json!("one commit"), json!(53)],
                },
            ],
        };
        assert_eq!(target.returned.fetch_add(1, Ordering::SeqCst), 0);
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
            result,
        )))
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
fn assert_raw(
    target: &Target,
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
            text(target.thread_id().as_str()),
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
            text(target.thread_id().as_str()),
            blob(revision),
            blob(generation)
        ]
    );
    let SqlValue::Text(encoded) = &raw.1[0][4] else {
        panic!("snapshot text")
    };
    let UniqueJson(_) = serde_json::from_str(encoded).unwrap();
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
    let first = 1;
    assert_eq!(
        raw.2[0],
        vec![
            SqlValue::Integer(first),
            text("thread"),
            text(target.thread_id().as_str()),
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
                text(target.thread_id().as_str()),
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
        let UniqueJson(_) = serde_json::from_str(payload).unwrap();
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
// Wait and GET responses are finite content-length JSON. Preserve all body bytes.
async fn finite_json(socket: TcpStream) -> Response {
    tokio::time::timeout(DEADLINE, async {
        let mut bytes = vec![];
        socket.take(65_537).read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.len() <= 65_536, "JSON response byte cap");
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
    .expect("bounded JSON response deadline")
}
async fn get(address: SocketAddr, path: &str) -> Response {
    tokio::time::timeout(DEADLINE, async {
        finite_json(open(address, "GET", path, "").await).await
    })
    .await
    .expect("GET connect/write/body deadline")
}
async fn assert_gets(address: SocketAddr, target: &Target, view: &ThreadView) -> (String, String) {
    let thread = get(address, &target.path()).await;
    let state = get(address, &format!("{}/state", target.path())).await;
    let s = &view.execution.as_ref().unwrap().snapshot;
    assert_eq!(thread.status, 200);
    let UniqueJson(thread_json) = serde_json::from_str(&thread.body).unwrap();
    let UniqueJson(state_json) = serde_json::from_str(&state.body).unwrap();
    assert_eq!(
        thread_json,
        json!({"thread_id":target.thread_id().as_str(),"created_at":timestamp(view.thread.created_at_ms),"updated_at":timestamp(s.updated_at_ms),"metadata":{},"status":"idle"})
    );
    assert_eq!(state.status, 200);
    assert_eq!(
        state_json,
        json!({"config":{"configurable":{"thread_id":target.thread_id().as_str(),"checkpoint_id":s.execution_id,"checkpoint_ns":""}},"created_at":timestamp(s.updated_at_ms),"metadata":{"source":"native_scripted","revision":3,"route":ROUTE,"execution_revision":REVISION},"next":[],"parent_config":null,"values":target.expected_values()})
    );
    (thread.body, state.body)
}
// serde_json::Value alone accepts repeated object keys. Reject them recursively,
// as well as concatenated JSON envelopes (from_str requires end of input).
struct UniqueJson(Value);
impl<'de> serde::Deserialize<'de> for UniqueJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJson;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON with unique object keys")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::Bool(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::from(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::from(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueJson(Value::Number(n)))
                    .ok_or_else(|| E::custom("nonfinite JSON number"))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::String(v.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::String(v)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::Null))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = vec![];
                while let Some(UniqueJson(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(UniqueJson(Value::Array(values)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate JSON object key"));
                    }
                    let UniqueJson(value) = map.next_value()?;
                    values.insert(key, value);
                }
                Ok(UniqueJson(Value::Object(values)))
            }
        }
        d.deserialize_any(Visitor)
    }
}

const CAP: usize = 65_536;
#[derive(Debug, PartialEq)]
enum Frame {
    Application(String, Value),
    Retry(u64),
}
// Consume every field and frame. Only the selected state/done vocabulary is legal;
// an exact retry control is retained in order rather than filtered away.
fn frames(body: &str) -> Vec<Frame> {
    assert!(!body.contains('\r'));
    assert!(body.ends_with("\n\n"));
    let mut result = vec![];
    let mut terminal = false;
    for block in body[..body.len() - 2].split("\n\n") {
        assert!(!block.is_empty());
        if block == ": ping" || block == ":ping" {
            continue;
        }
        assert!(!terminal, "application/control data after terminal");
        if block == "retry: 2000" {
            result.push(Frame::Retry(2000));
            continue;
        }
        let mut event = None;
        let mut data = None;
        for line in block.split('\n') {
            if let Some(v) = line.strip_prefix("event: ") {
                assert!(event.replace(v).is_none(), "duplicate event");
            } else if let Some(v) = line.strip_prefix("data: ") {
                assert!(data.replace(v).is_none(), "duplicate data");
            } else {
                panic!("unknown/malformed AP field: {line:?}");
            }
        }
        let event = event.expect("missing event");
        assert!(
            matches!(event, "state" | "done"),
            "unexpected application event"
        );
        let UniqueJson(value) = serde_json::from_str(data.expect("missing data")).unwrap();
        terminal = event == "done";
        result.push(Frame::Application(event.into(), value));
    }
    result
}
// Incremental HTTP decoder: an incomplete header, chunk size, chunk body or
// terminator simply needs more bytes. The original prefix is never discarded.
fn decode_sse(bytes: &[u8], eof: bool) -> Option<Vec<u8>> {
    assert!(bytes.len() <= CAP);
    let Some(split) = bytes.windows(4).position(|p| p == b"\r\n\r\n") else {
        assert!(!eof, "missing headers");
        return None;
    };
    let headers = std::str::from_utf8(&bytes[..split]).unwrap();
    let mut lines = headers.split("\r\n");
    assert_eq!(lines.next(), Some("HTTP/1.1 200 OK"));
    let mut chunked = false;
    let mut sse = false;
    for line in lines {
        let (name, value) = line.split_once(':').unwrap();
        assert!(!name.eq_ignore_ascii_case("content-length"));
        if name.eq_ignore_ascii_case("transfer-encoding") {
            assert!(!chunked);
            assert_eq!(value.trim(), "chunked");
            chunked = true;
        }
        if name.eq_ignore_ascii_case("content-type") {
            assert!(!sse);
            assert_eq!(value.trim(), "text/event-stream");
            sse = true;
        }
    }
    assert!(chunked && sse);
    let mut rest = &bytes[split + 4..];
    let mut decoded = vec![];
    loop {
        let Some(end) = rest.windows(2).position(|p| p == b"\r\n") else {
            assert!(!eof, "missing chunk size");
            return Some(decoded);
        };
        let digits = std::str::from_utf8(&rest[..end]).unwrap();
        assert!(!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_hexdigit()));
        let size = usize::from_str_radix(digits, 16).unwrap();
        assert!(size <= CAP);
        rest = &rest[end + 2..];
        if size == 0 {
            if rest.len() < 2 {
                assert!(!eof, "missing final CRLF");
                return Some(decoded);
            }
            assert_eq!(
                rest, b"\r\n",
                "missing terminator, trailers or trailing bytes"
            );
            return Some(decoded);
        }
        if rest.len() < size + 2 {
            // Available body bytes may already complete the first SSE event.
            decoded.extend_from_slice(&rest[..rest.len().min(size)]);
            assert!(!eof, "truncated chunk");
            return Some(decoded);
        }
        decoded.extend_from_slice(&rest[..size]);
        assert_eq!(&rest[size..size + 2], b"\r\n");
        rest = &rest[size + 2..];
    }
}
async fn first_state(socket: &mut TcpStream) -> (Vec<u8>, Value) {
    tokio::time::timeout(DEADLINE, async {
        let mut bytes = vec![];
        loop {
            if let Some(body) = decode_sse(&bytes, false) {
                // Search decoded bytes, independent of HTTP/TCP chunk boundaries.
                if let Some(end) = body.windows(2).position(|p| p == b"\n\n") {
                    let prefix = std::str::from_utf8(&body[..end + 2]).unwrap();
                    let parsed = frames(prefix);
                    if let Some(first) = parsed.first() {
                        let Frame::Application(name, value) = first else {
                            panic!("control before state")
                        };
                        assert_eq!(name, "state");
                        assert_eq!(parsed.len(), 1);
                        return (bytes, value.clone());
                    }
                    // Legal ping comments may precede the first application frame.
                    let mut offset = end + 2;
                    while let Some(end) = body[offset..].windows(2).position(|p| p == b"\n\n") {
                        let end = offset + end + 2;
                        let parsed = frames(std::str::from_utf8(&body[..end]).unwrap());
                        if let Some(first) = parsed.first() {
                            let Frame::Application(name, value) = first else {
                                panic!("control before state")
                            };
                            assert_eq!(name, "state");
                            assert_eq!(parsed.len(), 1);
                            return (bytes, value.clone());
                        }
                        offset = end;
                    }
                }
            }
            let mut part = [0; 1024];
            let n = socket.read(&mut part).await.unwrap();
            assert!(n > 0, "EOF before full state");
            bytes.extend_from_slice(&part[..n]);
            assert!(bytes.len() <= CAP);
        }
    })
    .await
    .expect("complete first-state deadline")
}
async fn complete_sse(mut socket: TcpStream, mut bytes: Vec<u8>) -> Vec<Frame> {
    tokio::time::timeout(DEADLINE, async {
        assert!(bytes.len() <= CAP);
        (&mut socket)
            .take(u64::try_from(CAP + 1 - bytes.len()).unwrap())
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert!(bytes.len() <= CAP);
        let body = decode_sse(&bytes, true).unwrap();
        frames(std::str::from_utf8(&body).unwrap())
    })
    .await
    .expect("complete SSE EOF deadline")
}
// Parsing support is intentionally local: time has formatting but no parsing
// feature. Validate the host's canonical UTC RFC3339 spelling by reconstruction.
fn validated_started_at(value: &Value, lower: u64, upper: u64) -> String {
    let s = value.as_str().expect("host clock string");
    assert!(s.is_ascii());
    assert!(s.len() >= 20 && s.ends_with('Z'));
    assert_eq!(&s[4..5], "-");
    assert_eq!(&s[7..8], "-");
    assert_eq!(&s[10..11], "T");
    assert_eq!(&s[13..14], ":");
    assert_eq!(&s[16..17], ":");
    let number = |a, b| {
        let p = &s[a..b];
        assert!(p.bytes().all(|b| b.is_ascii_digit()));
        p.parse::<u32>().unwrap()
    };
    let date = time::Date::from_calendar_date(
        number(0, 4).try_into().unwrap(),
        time::Month::try_from(u8::try_from(number(5, 7)).unwrap()).unwrap(),
        number(8, 10).try_into().unwrap(),
    )
    .unwrap();
    let nanos = if s.len() == 20 {
        0
    } else {
        assert_eq!(&s[19..20], ".");
        let fraction = &s[20..s.len() - 1];
        assert!(
            !fraction.is_empty()
                && fraction.len() <= 9
                && fraction.bytes().all(|b| b.is_ascii_digit())
        );
        fraction.parse::<u32>().unwrap() * 10_u32.pow(u32::try_from(9 - fraction.len()).unwrap())
    };
    let clock = time::Time::from_hms_nano(
        number(11, 13).try_into().unwrap(),
        number(14, 16).try_into().unwrap(),
        number(17, 19).try_into().unwrap(),
        nanos,
    )
    .unwrap();
    let parsed = time::PrimitiveDateTime::new(date, clock).assume_utc();
    assert_eq!(
        parsed
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap(),
        s
    );
    let millis = u64::try_from(parsed.unix_timestamp_nanos() / 1_000_000).unwrap();
    assert!((lower..=upper).contains(&millis));
    s.to_owned()
}
fn idle(target: &Target) -> Vec<Frame> {
    vec![
        Frame::Application(
            "state".into(),
            json!({"anchor":null,"input":null,"interrupts":[],"live":false,"resume":false,"run_started_at":null,"status":"idle","turn":null,"values":target.expected_values()}),
        ),
        Frame::Retry(2000),
        Frame::Application("done".into(), json!({"output":null})),
    ]
}
fn unchanged<S: Store>(
    store: &S,
    target: &Target,
    view: &ThreadView,
    journal: &[JournalEntry],
    raw: &Rows,
    path: &Path,
    returned: usize,
) {
    target.assert_counts(1, returned);
    assert_eq!(
        store.thread_view(&target.thread_id()).unwrap().as_ref(),
        Some(view)
    );
    let execution = view.execution.as_ref().unwrap();
    assert_eq!(
        store
            .execution(&execution.snapshot.execution_id)
            .unwrap()
            .as_ref(),
        Some(execution)
    );
    assert_eq!(
        store.journal(&execution.snapshot.execution_id).unwrap(),
        journal
    );
    assert_eq!(&rows(path), raw);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t013_live_and_settled_attach_start_with_state_without_execution() {
    let directory = Directory::new();
    let path = directory.0.clone();
    let target = Arc::new(Target::new());
    // Both panic guards exist before admission. The callback also has its own
    // finite gate deadline; listener abort/join below is only fixture cleanup.
    let _release = Release(target.gate.clone());
    let app = application(
        expected_definition(),
        Arc::new(Callback(target.clone())),
        Some(Arc::new(Validator(target.clone()))),
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
    retired(&shutdown).await;
    target.assert_counts(0, 0);
    assert!(store.thread_view(&target.thread_id()).unwrap().is_none());
    assert_eq!(rows(&path), (vec![], vec![], vec![]));
    let lower = now_ms();
    let (held, held_journal, held_raw, first, primary, attached, prefix) = tokio::time::timeout(DEADLINE, async {
        let body = json!({"route":ROUTE,"input":{"question":"attachment café 東京 🧵","seed":41,"flags":[true,null]}}).to_string();
        // Own the primary socket without reading headers, which wait only
        // selects after the callback has settled.
        let primary = open(address, "POST", &format!("{}/runs/wait", target.path()), &body).await;
        until(|| target.entered.load(Ordering::SeqCst) == 1 && target.gate.is_held()).await;
        target.assert_counts(1, 0);
        healthy(shutdown.status());
        assert!(shutdown.status().active_owners > 0);
        let held = store.thread_view(&target.thread_id()).unwrap().unwrap();
        let execution = held.execution.as_ref().unwrap();
        assert_eq!(store.execution(&execution.snapshot.execution_id).unwrap().as_ref(), Some(execution));
        let held_journal = store.journal(&execution.snapshot.execution_id).unwrap();
        let held_raw = rows(&path);
        let upper = now_ms();
        assert_view(&target, &held, generation, false, lower, upper);
        assert_raw(&target, &held, &held_journal, &held_raw, generation, false, upper);
        assert_eq!((held_raw.0.len(), held_raw.1.len(), held_raw.2.len()), (1, 1, 4));
        // This is a second real owned connection, while the actual callback is
        // held; a complete first application frame is received before release.
        let mut attached = open(address, "GET", &format!("{}/runs/stream", target.path()), "").await;
        let (prefix, first) = first_state(&mut attached).await;
        let started = validated_started_at(&first["run_started_at"], lower, now_ms());
        assert_eq!(first, json!({"anchor":execution.snapshot.execution_id,"input":{"question":"attachment café 東京 🧵","seed":41,"flags":[true,null]},"interrupts":[],"live":true,"resume":false,"run_started_at":started,"status":"busy","turn":[],"values":{"input":{"question":"attachment café 東京 🧵","seed":41,"flags":[true,null]}}}));
        // Positive full equality during continued hold proves selected read-only
        // behavior, without using any quiet-time or missing-event inference.
        target.gate.assert_held_before_deadline();
        unchanged(store.as_ref(), &target, &held, &held_journal, &held_raw, &path, 0);
        healthy(shutdown.status());
        assert!(shutdown.status().active_owners > 0);
        target.assert_clear();
        (held, held_journal, held_raw, first, primary, attached, prefix)
    }).await.expect("admit/held-state/read-only observation deadline");
    target.gate.assert_held_before_deadline();
    target.gate.release();
    // Collect both complete responses with their original bytes and strict EOF
    // framing. No producer chunks or fabricated task/result events are allowed.
    let (wait, live) = tokio::join!(finite_json(primary), complete_sse(attached, prefix));
    assert_eq!(wait.status, 200);
    let UniqueJson(wait_output) = serde_json::from_str(&wait.body).unwrap();
    assert_eq!(
        wait_output,
        json!({"answer":"one native answer","items":[false,53,"雪"]})
    );
    assert_eq!(
        live,
        vec![
            Frame::Application("state".into(), first),
            Frame::Application(
                "done".into(),
                json!({"output":{"answer":"one native answer","items":[false,53,"雪"]}})
            )
        ]
    );
    until(|| {
        let view = store.thread_view(&target.thread_id()).unwrap().unwrap();
        let s = &view.execution.as_ref().unwrap().snapshot;
        s.status == ExecutionStatus::Completed
            && s.tasks.len() == 1
            && s.tasks[0].status == TaskStatus::Succeeded
            && s.tasks[0].result.as_ref() == Some(&target.expected_result())
    })
    .await;
    retired(&shutdown).await;
    target.assert_counts(1, 1);
    let settled = store.thread_view(&target.thread_id()).unwrap().unwrap();
    let s = &settled.execution.as_ref().unwrap().snapshot;
    let journal = store.journal(&s.execution_id).unwrap();
    let durable = rows(&path);
    let upper = now_ms();
    assert_view(&target, &settled, generation, true, lower, upper);
    assert_raw(
        &target, &settled, &journal, &durable, generation, true, upper,
    );
    assert_eq!(
        (durable.0.len(), durable.1.len(), durable.2.len()),
        (1, 1, 5)
    );
    assert_eq!(&journal[..held_journal.len()], held_journal.as_slice());
    assert_eq!(&durable.2[..held_raw.2.len()], held_raw.2.as_slice());
    let h = &held.execution.as_ref().unwrap().snapshot;
    assert_eq!(settled.thread.generation, held.thread.generation);
    assert_eq!(s.execution_id, h.execution_id);
    assert_eq!(s.turn_id, h.turn_id);
    assert_eq!(s.attempt_history, h.attempt_history);
    assert_eq!(s.tasks[0].input, h.tasks[0].input);
    unchanged(
        store.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
    let settled_frames = tokio::time::timeout(DEADLINE, async {
        complete_sse(
            open(
                address,
                "GET",
                &format!("{}/runs/stream", target.path()),
                "",
            )
            .await,
            vec![],
        )
        .await
    })
    .await
    .expect("settled attach connect/write/body deadline");
    assert_eq!(settled_frames, idle(&target));
    unchanged(
        store.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
    retired(&shutdown).await;
    let http = assert_gets(address, &target, &settled).await;
    unchanged(
        store.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    unchanged(
        store.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&store) == 1 && Arc::strong_count(&app) == 1).await;
    // Destroy the actual sole SQLite owner synchronously before reopen.
    drop(Arc::try_unwrap(store).unwrap_or_else(|_| panic!("original store aliases survived")));
    assert!(weak_store.upgrade().is_none());
    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&path).unwrap());
    let weak_reopened = Arc::downgrade(&reopened);
    assert_eq!(reopened.owner_generation(), generation + 1);
    unchanged(
        reopened.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
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
    // Fresh host uses GET only. Durable provenance/owner epoch and histories
    // remain the original values; opening an owner does not replay execution.
    assert_eq!(assert_gets(address, &target, &settled).await, http);
    unchanged(
        reopened.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
    assert_view(&target, &settled, generation, true, lower, now_ms());
    assert_raw(
        &target,
        &settled,
        &journal,
        &durable,
        generation,
        true,
        now_ms(),
    );
    let reopened_frames = tokio::time::timeout(DEADLINE, async {
        complete_sse(
            open(
                address,
                "GET",
                &format!("{}/runs/stream", target.path()),
                "",
            )
            .await,
            vec![],
        )
        .await
    })
    .await
    .expect("fresh-host settled attach deadline");
    assert_eq!(reopened_frames, idle(&target));
    unchanged(
        reopened.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
    retired(&shutdown).await;
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    unchanged(
        reopened.as_ref(),
        &target,
        &settled,
        &journal,
        &durable,
        &path,
        1,
    );
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&reopened) == 1 && Arc::strong_count(&app) == 1).await;
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("fresh store aliases survived")));
    assert!(weak_reopened.upgrade().is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    target.assert_counts(1, 1);
    drop(directory);
    assert!(!path.exists(), "fixture directory removed");
}
