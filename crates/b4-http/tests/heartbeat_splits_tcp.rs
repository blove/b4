//! T017: one native TCP stream keeps its heartbeat and Unicode events at every byte cut.
use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
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
const MAX_RAW: usize = 65_536;
const MAX_HEADER: usize = 8_192;
const MAX_BODY: usize = 32_768;
const MAX_FRAME: usize = 8_192;
const ROUTE: &str = "/heartbeat-splits#workflow";
const REVISION: &str = "heartbeat-splits-v1";
const THREAD: &str = "release-t017-heartbeat-splits";

#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool, InstantState)>,
    changed: Condvar,
}
#[derive(Default)]
struct InstantState(Option<Instant>);
impl Gate {
    fn hold(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(!state.0 && !state.1 && state.2 .0.is_none());
        state.0 = true;
        state.2 .0 = Some(Instant::now());
        self.changed.notify_all();
        let deadline = Instant::now() + HOLD_LIMIT;
        while !state.1 {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("finite callback gate expired");
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
    fn held(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.0 && !state.1 && state.2 .0.unwrap().elapsed() < HOLD_LIMIT
    }
    fn release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.1 = true;
        drop(state);
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
    gates: [Arc<Gate>; 2],
    input: Value,
    validated: Mutex<Vec<Value>>,
    calls: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
    entries: AtomicUsize,
    returns: AtomicUsize,
    emissions: Mutex<Vec<NativeChunkResult>>,
}
impl Target {
    fn new() -> Self {
        Self {
            gates: std::array::from_fn(|_| Arc::new(Gate::default())),
            input: json!({"question":"heartbeat café 東京 🧵","seed":71}),
            validated: Mutex::new(vec![]),
            calls: Mutex::new(vec![]),
            entries: AtomicUsize::new(0),
            returns: AtomicUsize::new(0),
            emissions: Mutex::new(vec![]),
        }
    }
    fn id(&self) -> ThreadId {
        ThreadId::new(THREAD).unwrap()
    }
    fn path(&self) -> String {
        format!("/threads/{THREAD}")
    }
    fn route(&self) -> String {
        format!("/threads/{THREAD}/runs/stream")
    }
    fn expected_result(&self) -> TaskResult {
        TaskResult {
            output: json!({"answer":"settled 雪","items":[true,31,null]}),
            writes: vec![StateWrite::Replace {
                field: "aggregate".into(),
                value: json!({"saved":"heartbeat durable","n":71}),
            }],
        }
    }
    fn expected_values(&self) -> Value {
        json!({"input":{"question":"heartbeat café 東京 🧵","seed":71},
            "aggregate":{"saved":"heartbeat durable","n":71}})
    }
    fn assert_counts(&self, returned: usize) {
        assert_eq!(*self.validated.lock().unwrap(), vec![self.input.clone()]);
        assert_eq!(self.entries.load(Ordering::SeqCst), 1);
        assert_eq!(self.returns.load(Ordering::SeqCst), returned);
        assert_eq!(self.calls.lock().unwrap().len(), 1);
        assert_eq!(
            *self.emissions.lock().unwrap(),
            if returned == 0 {
                vec![]
            } else {
                vec![NativeChunkResult::Accepted]
            }
        );
    }
}
struct Validator(Arc<Target>);
impl InputValidator for Validator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.validated.lock().unwrap().push(input.clone());
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
        panic!("ordinary callback entry is forbidden")
    }
    fn invoke_controlled(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
        _: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        panic!("controlled callback entry is forbidden")
    }
    fn invoke_streaming(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
        sink: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        let target = &self.0;
        assert_eq!(target.entries.fetch_add(1, Ordering::SeqCst), 0);
        assert_eq!(
            *target.validated.lock().unwrap(),
            vec![json!({"question":"heartbeat café 東京 🧵","seed":71})]
        );
        assert!(!observer.cancellation_requested());
        target
            .calls
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        target.gates[0].hold();
        assert!(!observer.cancellation_requested());
        target.emissions.lock().unwrap().push(
            sink.emit(&json!({"piece":"雪 café 東京 🧵 \"quoted\" \\ path\nnext","index":71})),
        );
        target.gates[1].hold();
        assert!(!observer.cancellation_requested());
        let result = TaskResult {
            output: json!({"answer":"settled 雪","items":[true,31,null]}),
            writes: vec![StateWrite::Replace {
                field: "aggregate".into(),
                value: json!({"saved":"heartbeat durable","n":71}),
            }],
        };
        assert_eq!(target.returns.fetch_add(1, Ordering::SeqCst), 0);
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
            result,
        )))
    }
}

fn host_config(app: Arc<NativeApplication>) -> NativeHostConfig {
    let mut c = config(app, true, 1);
    c.limits.heartbeat_ms = 20;
    c.limits.max_concurrent_tasks = 1;
    c.limits.max_tasks_per_execution = 1;
    c.limits.max_input_bytes = 65_536;
    c.limits.max_output_bytes = 65_536;
    c.limits.turn_buffer_frames = 64;
    c.limits.turn_buffer_bytes = 262_144;
    c.limits.viewer_queue_frames = 64;
    c.limits.viewer_queue_bytes = 262_144;
    c.limits.attachment_snapshot_bytes = 262_144;
    c.limits.max_attached_viewers = 4;
    c
}
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
        assert!(tokio::time::timeout(DEADLINE, task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
    }
}
impl Drop for LocalListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

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
        q.query_map([], |row| {
            (0..width)
                .map(|i| row.get::<_, SqlValue>(i))
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    (
        read("SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id", 7),
        read("SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id", 5),
        read("SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence", 9),
    )
}

// serde_json::Value accepts duplicate keys, so the independent wire oracle does not.
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
                    .ok_or_else(|| E::custom("nonfinite number"))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueJson(Value::String(v.into())))
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
                        return Err(serde::de::Error::custom("duplicate key"));
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

#[derive(Clone, Debug, PartialEq)]
enum Frame {
    Ping,
    App(String, Value),
}

enum Stage {
    Headers,
    Size(Vec<u8>),
    SizeLf(usize),
    Body(usize),
    BodyCr,
    BodyLf,
    FinalCr,
    FinalLf,
    Done,
}
struct Decoder {
    stage: Stage,
    headers: Vec<u8>,
    line: Vec<u8>,
    event: Option<String>,
    data: Option<Value>,
    comment_open: bool,
    frames: Vec<Frame>,
    raw_count: usize,
    body_count: usize,
    terminal: bool,
    saw_raw_utf8: bool,
}
impl Decoder {
    fn new() -> Self {
        Self {
            stage: Stage::Headers,
            headers: vec![],
            line: vec![],
            event: None,
            data: None,
            comment_open: false,
            frames: vec![],
            raw_count: 0,
            body_count: 0,
            terminal: false,
            saw_raw_utf8: false,
        }
    }
    fn feed(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.byte(byte);
        }
    }
    fn byte(&mut self, byte: u8) {
        self.raw_count += 1;
        assert!(self.raw_count <= MAX_RAW, "raw response cap");
        let stage = std::mem::replace(&mut self.stage, Stage::Done);
        self.stage = match stage {
            Stage::Headers => {
                self.headers.push(byte);
                assert!(self.headers.len() <= MAX_HEADER);
                if self.headers.ends_with(b"\r\n\r\n") {
                    self.check_headers();
                    Stage::Size(vec![])
                } else {
                    Stage::Headers
                }
            }
            Stage::Size(mut digits) => {
                if byte == b'\r' {
                    assert!(!digits.is_empty() && digits.len() <= 6);
                    assert!(digits.iter().all(u8::is_ascii_hexdigit));
                    let size =
                        usize::from_str_radix(std::str::from_utf8(&digits).unwrap(), 16).unwrap();
                    assert!(size <= MAX_BODY);
                    Stage::SizeLf(size)
                } else {
                    assert!(
                        byte.is_ascii_hexdigit(),
                        "malformed chunk length or extension"
                    );
                    digits.push(byte);
                    assert!(digits.len() <= 6);
                    Stage::Size(digits)
                }
            }
            Stage::SizeLf(size) => {
                assert_eq!(byte, b'\n', "chunk size CRLF");
                if size == 0 {
                    Stage::FinalCr
                } else {
                    Stage::Body(size)
                }
            }
            Stage::Body(remaining) => {
                self.body_count += 1;
                assert!(self.body_count <= MAX_BODY);
                self.sse_byte(byte);
                if remaining == 1 {
                    Stage::BodyCr
                } else {
                    Stage::Body(remaining - 1)
                }
            }
            Stage::BodyCr => {
                assert_eq!(byte, b'\r', "chunk data CR");
                Stage::BodyLf
            }
            Stage::BodyLf => {
                assert_eq!(byte, b'\n', "chunk data LF");
                Stage::Size(vec![])
            }
            Stage::FinalCr => {
                assert_eq!(byte, b'\r', "trailers forbidden");
                Stage::FinalLf
            }
            Stage::FinalLf => {
                assert_eq!(byte, b'\n', "final chunk LF");
                Stage::Done
            }
            Stage::Done => panic!("bytes after final HTTP chunk"),
        };
    }
    fn check_headers(&self) {
        let raw = std::str::from_utf8(&self.headers).unwrap();
        assert!(raw.ends_with("\r\n\r\n"));
        let mut lines = raw[..raw.len() - 4].split("\r\n");
        assert_eq!(lines.next(), Some("HTTP/1.1 200 OK"));
        let mut seen = BTreeSet::new();
        for line in lines {
            let (name, value) = line.split_once(':').expect("HTTP header field");
            assert!(
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            );
            let lower = name.to_ascii_lowercase();
            assert!(seen.insert(lower.clone()), "duplicate HTTP header");
            let value = value.trim();
            match lower.as_str() {
                "content-type" => assert_eq!(value, "text/event-stream"),
                "transfer-encoding" => assert_eq!(value, "chunked"),
                "cache-control" => assert_eq!(value, "no-store, no-transform"),
                "x-accel-buffering" => assert_eq!(value, "no"),
                // The test request asks for Connection: close so the final
                // chunk is followed by a bounded, observable TCP EOF.
                "connection" => assert_eq!(value, "close"),
                "content-length" => panic!("SSE must be chunked"),
                _ => {
                    assert!(!value.contains('\r') && !value.contains('\n'));
                }
            }
        }
        for required in [
            "content-type",
            "transfer-encoding",
            "cache-control",
            "x-accel-buffering",
            "connection",
        ] {
            assert!(
                seen.contains(required),
                "missing required SSE header: {required}"
            );
        }
    }
    fn sse_byte(&mut self, byte: u8) {
        assert!(!self.terminal, "SSE bytes after terminal");
        assert_ne!(byte, b'\r', "SSE CR prohibited");
        if byte == b'\n' {
            if self.line.is_empty() {
                if self.comment_open {
                    assert!(self.event.is_none() && self.data.is_none());
                    self.comment_open = false;
                    self.frames.push(Frame::Ping);
                } else if self.event.is_some() || self.data.is_some() {
                    let event = self.event.take().expect("data without event");
                    let data = self.data.take().expect("event without data");
                    assert!(matches!(
                        event.as_str(),
                        "tool_call" | "chunk" | "tool_result" | "done"
                    ));
                    self.terminal = event == "done";
                    self.frames.push(Frame::App(event, data));
                } else {
                    panic!("empty SSE block");
                }
            } else {
                let line = std::str::from_utf8(&self.line).expect("complete UTF-8 SSE line");
                assert!(!self.comment_open, "comment must end with blank line");
                if line == ": ping" {
                    assert!(
                        self.event.is_none() && self.data.is_none(),
                        "comment inside application frame"
                    );
                    self.comment_open = true;
                } else if let Some(value) = line.strip_prefix("event: ") {
                    assert!(!value.is_empty());
                    assert!(
                        self.event.replace(value.to_owned()).is_none(),
                        "duplicate event field"
                    );
                } else if let Some(value) = line.strip_prefix("data: ") {
                    assert!(self.data.is_none(), "duplicate data field");
                    assert!(value.len() <= MAX_FRAME);
                    assert!(self.event.is_some(), "data before event");
                    if self.event.as_deref() == Some("chunk") {
                        assert!(
                            value
                                .as_bytes()
                                .windows("雪".len())
                                .any(|w| w == "雪".as_bytes()),
                            "selected UTF-8 must be raw in chunk JSON data"
                        );
                        assert!(
                            value.contains("\\nnext"),
                            "embedded newline must remain escaped"
                        );
                        assert!(
                            value.contains("\\\"quoted\\\""),
                            "quotes must remain escaped"
                        );
                        self.saw_raw_utf8 = true;
                    }
                    let UniqueJson(json) =
                        serde_json::from_str(value).expect("unique complete JSON data");
                    self.data = Some(json);
                } else {
                    panic!("unknown or malformed SSE field: {line:?}");
                }
                self.line.clear();
            }
        } else {
            self.line.push(byte);
            assert!(self.line.len() <= MAX_FRAME);
        }
        assert!(self.frames.len() <= 16, "frame cap");
    }
    fn finish(&self) {
        assert!(matches!(self.stage, Stage::Done), "truncated HTTP response");
        assert!(
            self.line.is_empty()
                && self.event.is_none()
                && self.data.is_none()
                && !self.comment_open,
            "incomplete SSE frame"
        );
        assert!(self.terminal, "EOF without done");
        assert!(
            self.saw_raw_utf8,
            "selected raw UTF-8 missing from data field"
        );
    }
}

fn expected_frames(attempt: &str) -> Vec<Frame> {
    vec![
        Frame::App(
            "tool_call".into(),
            json!({"id":attempt,"name":"answer","input":{"input":{"question":"heartbeat café 東京 🧵","seed":71}}}),
        ),
        Frame::Ping,
        Frame::App(
            "chunk".into(),
            json!({"piece":"雪 café 東京 🧵 \"quoted\" \\ path\nnext","index":71}),
        ),
        Frame::App(
            "tool_result".into(),
            json!({"id":attempt,"name":"answer","output":{"answer":"settled 雪","items":[true,31,null]}}),
        ),
        Frame::App(
            "done".into(),
            json!({"output":{"answer":"settled 雪","items":[true,31,null]}}),
        ),
    ]
}

fn assert_frames(frames: &[Frame], attempt: &str) {
    let expected = expected_frames(attempt);
    assert!(frames.len() >= expected.len());
    assert_eq!(frames.first(), expected.first());
    assert_eq!(frames.last(), expected.last());
    let application: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f, Frame::App(..)))
        .collect();
    let expected_application: Vec<_> = expected
        .iter()
        .filter(|f| matches!(f, Frame::App(..)))
        .collect();
    assert_eq!(
        application, expected_application,
        "exact four application frames"
    );
    assert!(
        frames[1..frames.len() - 2].contains(&Frame::Ping),
        "preterminal heartbeat"
    );
    let chunk_position = frames.iter().position(|f| *f == expected[2]).unwrap();
    assert!(
        frames[1..chunk_position].contains(&Frame::Ping),
        "heartbeat before Unicode chunk"
    );
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
fn text(s: impl Into<String>) -> SqlValue {
    SqlValue::Text(s.into())
}
fn blob(n: u64) -> SqlValue {
    SqlValue::Blob(n.to_be_bytes().to_vec())
}
fn integer(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}
fn timestamp(ms: u64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

fn assert_view(
    target: &Target,
    view: &ThreadView,
    generation: u64,
    completed: bool,
    lower: u64,
    upper: u64,
) {
    target.assert_counts(usize::from(completed));
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
            thread_id: target.id(),
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
                id: target.id(),
                generation: context.thread_generation.clone(),
                active_execution_id: Some(call.execution_id.clone()),
                created_at_ms: view.thread.created_at_ms,
                metadata: Default::default(),
                origin: Some(origin()),
                resource_revision: if completed { 4 } else { 3 },
            },
            execution: Some(StoredExecution {
                thread_id: target.id(),
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
// These explicit oracle inputs keep captured clocks and durable observations
// separate from the producer and expected values.
#[allow(clippy::too_many_arguments)]
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
            text(target.id().as_str()),
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
            text(target.id().as_str()),
            blob(revision),
            blob(generation)
        ]
    );
    let SqlValue::Text(encoded) = &raw.1[0][4] else {
        panic!("snapshot text")
    };
    let UniqueJson(raw_snapshot) = serde_json::from_str(encoded).unwrap();
    assert_eq!(raw_snapshot, serde_json::to_value(s).unwrap());
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
            integer(first),
            text("thread"),
            text(target.id().as_str()),
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
                integer(first + i64::try_from(i + 1).unwrap()),
                text(if i == 0 { "admit" } else { "commit" }),
                text(target.id().as_str()),
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
        let UniqueJson(raw_entry) = serde_json::from_str(payload).unwrap();
        assert_eq!(raw_entry, serde_json::to_value(&expected).unwrap());
        assert_eq!(
            serde_json::from_str::<JournalEntry>(payload).unwrap(),
            expected
        );
    }
    assert_eq!(previous_time, s.updated_at_ms);
}

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
        json!({"thread_id":target.id().as_str(),"created_at":timestamp(view.thread.created_at_ms),"updated_at":timestamp(s.updated_at_ms),"metadata":{},"status":"idle"})
    );
    assert_eq!(state.status, 200);
    assert_eq!(
        state_json,
        json!({"config":{"configurable":{"thread_id":target.id().as_str(),"checkpoint_id":s.execution_id,"checkpoint_ns":""}},"created_at":timestamp(s.updated_at_ms),"metadata":{"source":"native_scripted","revision":3,"route":ROUTE,"execution_revision":REVISION},"next":[],"parent_config":null,"values":target.expected_values()})
    );
    (thread.body, state.body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t017_heartbeat_and_split_utf8_tcp_preserve_one_terminal() {
    let directory = Directory::new();
    let path = directory.0.clone();
    let target = Arc::new(Target::new());
    // Created before admission: a failed socket/parser/store assertion cannot strand the worker.
    let _release0 = Release(target.gates[0].clone());
    let _release1 = Release(target.gates[1].clone());
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
    until(|| shutdown.status().active_owners == 0).await;
    assert!(store.thread_view(&target.id()).unwrap().is_none());
    assert_eq!(rows(&path), (vec![], vec![], vec![]));

    let lower = now_ms();
    let request =
        json!({"route":ROUTE,"input":{"question":"heartbeat café 東京 🧵","seed":71}}).to_string();
    let mut socket =
        tokio::time::timeout(DEADLINE, open(address, "POST", &target.route(), &request))
            .await
            .unwrap();
    until(|| target.entries.load(Ordering::SeqCst) == 1 && target.gates[0].held()).await;
    target.assert_counts(0);
    assert!(target.gates[0].held());
    assert_eq!(shutdown.status().phase, ShutdownPhase::Serving);
    assert!(shutdown.status().active_owners > 0);
    let held = store.thread_view(&target.id()).unwrap().unwrap();
    let held_snapshot = &held.execution.as_ref().unwrap().snapshot;
    assert_eq!(held_snapshot.status, ExecutionStatus::Running);
    assert_eq!(held_snapshot.tasks.len(), 1);
    assert_eq!(held_snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(held_snapshot.tasks[0].result.is_none());
    let attempt = held_snapshot.tasks[0]
        .attempt_id
        .as_ref()
        .unwrap()
        .as_str()
        .to_owned();
    assert_eq!(
        target.calls.lock().unwrap()[0].0.attempt_id.as_str(),
        attempt
    );
    let held_journal = store.journal(&held_snapshot.execution_id).unwrap();
    let held_raw = rows(&path);
    assert_view(&target, &held, generation, false, lower, now_ms());
    assert_raw(
        &target,
        &held,
        &held_journal,
        &held_raw,
        generation,
        false,
        now_ms(),
    );
    assert_eq!(
        (held_raw.0.len(), held_raw.1.len(), held_raw.2.len()),
        (1, 1, 4)
    );

    // Feed the actual loopback response incrementally until its complete trusted
    // call and first exact quiet comment are observed while the callback stays held.
    let mut raw = Vec::new();
    let mut live = Decoder::new();
    tokio::time::timeout(DEADLINE, async {
        loop {
            if live.frames.contains(&Frame::Ping) {
                break;
            }
            let mut part = [0u8; 1024];
            let count = socket.read(&mut part).await.unwrap();
            assert!(count > 0, "EOF before real heartbeat");
            raw.extend_from_slice(&part[..count]);
            live.feed(&part[..count]);
        }
    })
    .await
    .expect("finite real heartbeat deadline");
    assert!(target.gates[0].held());
    assert_eq!(target.returns.load(Ordering::SeqCst), 0);
    assert_eq!(
        *target.emissions.lock().unwrap(),
        Vec::<NativeChunkResult>::new()
    );
    assert!(live.frames.len() >= 2);
    assert_eq!(live.frames[0], expected_frames(&attempt)[0]);
    assert!(live.frames[1..].iter().all(|f| *f == Frame::Ping));
    assert!(live.frames.contains(&Frame::Ping));
    assert_eq!(
        store.thread_view(&target.id()).unwrap().as_ref(),
        Some(&held)
    );
    assert_eq!(
        store.journal(&held_snapshot.execution_id).unwrap(),
        held_journal
    );
    assert_eq!(rows(&path), held_raw);

    target.gates[0].release();
    until(|| {
        target.gates[1].held()
            && *target.emissions.lock().unwrap() == vec![NativeChunkResult::Accepted]
    })
    .await;
    assert_eq!(target.returns.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.thread_view(&target.id()).unwrap().as_ref(),
        Some(&held)
    );
    assert_eq!(
        store.journal(&held_snapshot.execution_id).unwrap(),
        held_journal
    );
    assert_eq!(rows(&path), held_raw);
    target.gates[1].release();
    until(|| target.returns.load(Ordering::SeqCst) == 1).await;
    until(|| {
        let view = store.thread_view(&target.id()).unwrap().unwrap();
        let snapshot = &view.execution.as_ref().unwrap().snapshot;
        snapshot.status == ExecutionStatus::Completed
            && snapshot.tasks.len() == 1
            && snapshot.tasks[0].status == TaskStatus::Succeeded
            && snapshot.tasks[0].result.as_ref() == Some(&target.expected_result())
    })
    .await;
    // The durable result is read before the wire terminal is consumed.
    let settled = store.thread_view(&target.id()).unwrap().unwrap();
    let snapshot = &settled.execution.as_ref().unwrap().snapshot;
    assert_eq!(
        snapshot.tasks[0].attempt_id.as_ref().unwrap().as_str(),
        attempt
    );
    assert_eq!(
        snapshot.tasks[0].result.as_ref(),
        Some(&target.expected_result())
    );
    assert_eq!(snapshot.output.as_ref(), Some(&target.expected_values()));
    let journal = store.journal(&snapshot.execution_id).unwrap();
    let durable = rows(&path);
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
    assert_eq!(
        (durable.0.len(), durable.1.len(), durable.2.len()),
        (1, 1, 5)
    );

    tokio::time::timeout(DEADLINE, async {
        let mut part = [0u8; 1024];
        loop {
            let count = socket.read(&mut part).await.unwrap();
            if count == 0 {
                break;
            }
            raw.extend_from_slice(&part[..count]);
            live.feed(&part[..count]);
        }
    })
    .await
    .expect("finite terminal and HTTP EOF deadline");
    live.finish();
    assert_frames(&live.frames, &attempt);
    assert_eq!(live.raw_count, raw.len());
    assert!(raw.len() <= MAX_RAW);
    assert!(raw.windows(b"data: {".len()).any(|w| w == b"data: {"));
    assert!(
        raw.windows(b"\\nnext".len()).any(|w| w == b"\\nnext"),
        "newline stays escaped in JSON data"
    );
    assert!(
        raw.windows(b"\\\"quoted\\\"".len())
            .any(|w| w == b"\\\"quoted\\\""),
        "quotes stay escaped in data"
    );
    assert!(
        raw.windows(b"\\\\ path".len()).any(|w| w == b"\\\\ path"),
        "backslash stays escaped in data"
    );

    // Explicitly witness boundaries that the exhaustive 0..=N replay includes.
    let mut selected_cuts = BTreeSet::new();
    for scalar in ["雪", "é", "東", "京", "🧵"] {
        let mut count = 0;
        for offset in 0..raw.len().saturating_sub(scalar.len()) + 1 {
            if &raw[offset..offset + scalar.len()] == scalar.as_bytes() {
                count += 1;
                for cut in offset + 1..offset + scalar.len() {
                    selected_cuts.insert(cut);
                }
            }
        }
        assert!(
            count > 0,
            "selected multibyte scalar absent from raw transcript"
        );
    }
    let mut crlf_cuts = 0;
    for (offset, bytes) in raw.windows(2).enumerate() {
        if bytes == b"\r\n" {
            selected_cuts.insert(offset + 1);
            crlf_cuts += 1;
        }
    }
    assert!(crlf_cuts > 3, "HTTP and chunk CR/LF boundaries present");
    for label in [b"event:".as_slice(), b"data:".as_slice()] {
        let mut count = 0;
        for (offset, bytes) in raw.windows(label.len()).enumerate() {
            if bytes == label {
                count += 1;
                for cut in offset + 1..offset + label.len() {
                    selected_cuts.insert(cut);
                }
            }
        }
        assert!(count >= 4, "all SSE labels present");
    }
    let header_end = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let size_end = raw[header_end..]
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap()
        + header_end;
    assert!(size_end > header_end && raw[header_end..size_end].iter().all(u8::is_ascii_hexdigit));
    for cut in header_end + 1..=size_end + 1 {
        selected_cuts.insert(cut);
    }
    assert!(!selected_cuts.is_empty());
    assert!(selected_cuts.iter().all(|cut| *cut <= raw.len()));
    for cut in 0..=raw.len() {
        let mut replay = Decoder::new();
        replay.feed(&raw[..cut]);
        replay.feed(&raw[cut..]);
        replay.finish();
        assert_frames(&replay.frames, &attempt);
        assert_eq!(replay.frames, live.frames, "two-piece cut {cut}");
    }
    let mut bytewise = Decoder::new();
    for byte in &raw {
        bytewise.feed(std::slice::from_ref(byte));
    }
    bytewise.finish();
    assert_frames(&bytewise.frames, &attempt);
    assert_eq!(bytewise.frames, live.frames);
    assert_eq!(
        store.thread_view(&target.id()).unwrap().as_ref(),
        Some(&settled)
    );
    assert_eq!(store.journal(&snapshot.execution_id).unwrap(), journal);
    assert_eq!(rows(&path), durable);

    let http = assert_gets(address, &target, &settled).await;
    until(|| shutdown.status().active_owners == 0).await;
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&store) == 1 && Arc::strong_count(&app) == 1).await;
    drop(Arc::try_unwrap(store).unwrap_or_else(|_| panic!("original store aliases survived")));
    assert!(weak_store.upgrade().is_none());

    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&path).unwrap());
    let weak_reopened = Arc::downgrade(&reopened);
    assert_eq!(reopened.owner_generation(), generation + 1);
    assert_eq!(
        reopened.thread_view(&target.id()).unwrap().as_ref(),
        Some(&settled)
    );
    assert_eq!(reopened.journal(&snapshot.execution_id).unwrap(), journal);
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
    assert_eq!(assert_gets(address, &target, &settled).await, http);
    assert_eq!(
        reopened.thread_view(&target.id()).unwrap().as_ref(),
        Some(&settled)
    );
    assert_eq!(reopened.journal(&snapshot.execution_id).unwrap(), journal);
    assert_eq!(rows(&path), durable);
    target.assert_counts(1);
    until(|| shutdown.status().active_owners == 0).await;
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&reopened) == 1 && Arc::strong_count(&app) == 1).await;
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("reopened store aliases survived")));
    assert!(weak_reopened.upgrade().is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    drop(directory);
    assert!(!path.exists());
}
