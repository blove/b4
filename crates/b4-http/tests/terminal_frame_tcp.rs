//! T011: four fixed normal native completions, each with exactly one final done.
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
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};

mod native_support;
use native_support::{config, definition, exchange, open, until, Auth, Directory, Owner, Response};

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
enum Kind {
    Structured,
    Null,
    Void,
    Failed,
}
struct Target {
    kind: Kind,
    id: &'static str,
    route: &'static str,
    revision: &'static str,
    input: Value,
    validations: AtomicUsize,
    validated: Mutex<Vec<Value>>,
    entered: AtomicUsize,
    returned: AtomicUsize,
    calls: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
}
impl Target {
    fn new(
        kind: Kind,
        id: &'static str,
        route: &'static str,
        revision: &'static str,
        input: Value,
    ) -> Self {
        Self {
            kind,
            id,
            route,
            revision,
            input,
            validations: AtomicUsize::new(0),
            validated: Mutex::new(vec![]),
            entered: AtomicUsize::new(0),
            returned: AtomicUsize::new(0),
            calls: Mutex::new(vec![]),
        }
    }
    fn output_kind(&self) -> OutputKind {
        if matches!(self.kind, Kind::Void) {
            OutputKind::Void
        } else {
            OutputKind::Value
        }
    }
    fn thread_id(&self) -> ThreadId {
        ThreadId::new(self.id).unwrap()
    }
    fn path(&self) -> String {
        format!("/threads/{}", self.id)
    }
    fn assert_counts(&self, count: usize) {
        let expected = if count == 0 {
            vec![]
        } else {
            vec![self.input.clone()]
        };
        assert_eq!(self.validations.load(Ordering::SeqCst), count);
        assert_eq!(*self.validated.lock().unwrap(), expected);
        assert_eq!(self.entered.load(Ordering::SeqCst), count);
        assert_eq!(self.returned.load(Ordering::SeqCst), count);
        assert_eq!(self.calls.lock().unwrap().len(), count);
    }
    // Independent acceptance literals, never obtained from a callback or HTTP body.
    fn expected_result(&self) -> Option<TaskResult> {
        let (output, value) = match self.kind {
            Kind::Structured => (
                json!({"answer":"terminal value","items":[true,31,null]}),
                json!({"saved":"structured state","n":101}),
            ),
            Kind::Null => (Value::Null, json!({"saved":"null state","n":202})),
            Kind::Void => (Value::Null, json!({"saved":"void state","n":303})),
            Kind::Failed => return None,
        };
        Some(TaskResult {
            output,
            writes: vec![StateWrite::Replace {
                field: "aggregate".into(),
                value,
            }],
        })
    }
    fn expected_values(&self) -> Value {
        match self.kind {
            Kind::Structured => {
                json!({"input":{"question":"structured request","seed":13},"aggregate":{"saved":"structured state","n":101}})
            }
            Kind::Null => {
                json!({"input":{"question":"null request","seed":27},"aggregate":{"saved":"null state","n":202}})
            }
            Kind::Void => {
                json!({"input":{"question":"void request","seed":39},"aggregate":{"saved":"void state","n":303}})
            }
            Kind::Failed => json!({"input":{"question":"failure request","seed":51}}),
        }
    }
    fn expected_done(&self) -> Value {
        match self.kind {
            Kind::Structured => {
                json!({"output":{"answer":"terminal value","items":[true,31,null]}})
            }
            Kind::Null => json!({"output":null}),
            Kind::Void => json!({}),
            Kind::Failed => json!({"output":{"error":"known terminal failure"}}),
        }
    }
    fn terminal_status(&self) -> ExecutionStatus {
        if matches!(self.kind, Kind::Failed) {
            ExecutionStatus::Failed
        } else {
            ExecutionStatus::Completed
        }
    }
}
struct Validator(Arc<Target>);
impl InputValidator for Validator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.validated.lock().unwrap().push(input.clone());
        self.0.validations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Callback(Arc<Target>);
impl NativeEffect for Callback {
    // The existing invoke entry exercises both default streaming delegations.
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        let target = &self.0;
        target.entered.fetch_add(1, Ordering::SeqCst);
        target
            .calls
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        // These outcome literals are deliberately separate from the oracle above.
        let outcome = match target.kind {
            Kind::Structured => NativeOutcome::Succeeded(TaskResult {
                output: json!({"answer":"terminal value","items":[true,31,null]}),
                writes: vec![StateWrite::Replace {
                    field: "aggregate".into(),
                    value: json!({"saved":"structured state","n":101}),
                }],
            }),
            Kind::Null => NativeOutcome::Succeeded(TaskResult {
                output: Value::Null,
                writes: vec![StateWrite::Replace {
                    field: "aggregate".into(),
                    value: json!({"saved":"null state","n":202}),
                }],
            }),
            Kind::Void => NativeOutcome::Succeeded(TaskResult {
                output: Value::Null,
                writes: vec![StateWrite::Replace {
                    field: "aggregate".into(),
                    value: json!({"saved":"void state","n":303}),
                }],
            }),
            Kind::Failed => NativeOutcome::Failed("known terminal failure".into()),
        };
        target.returned.fetch_add(1, Ordering::SeqCst);
        Ok(outcome)
    }
}
fn host_config(app: Arc<NativeApplication>) -> NativeHostConfig {
    let mut c = config(app, true, 1);
    c.limits.max_tasks_per_execution = 1;
    c
}

// The usual fixture has no Drop guard. This listener aborts on panic, and its
// successful path explicitly aborts and joins only after owner quiescence.
struct Listener {
    address: SocketAddr,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Listener {
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
impl Drop for Listener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Decode the entire finite HTTP response, preserving bytes read with the headers.
/// A cap and one total deadline cover EOF, regardless of TCP packet boundaries.
async fn complete(mut socket: TcpStream, mut bytes: Vec<u8>) -> Response {
    const CAP: usize = 65_536;
    assert!(bytes.len() <= CAP);
    let remaining = u64::try_from(CAP + 1 - bytes.len()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        (&mut socket).take(remaining).read_to_end(&mut bytes),
    )
    .await
    .expect("complete SSE response deadline")
    .unwrap();
    assert!(
        bytes.len() <= CAP,
        "fixed response exceeded its total byte bound"
    );
    let split = bytes
        .windows(4)
        .position(|p| p == b"\r\n\r\n")
        .expect("HTTP header terminator");
    let headers = std::str::from_utf8(&bytes[..split]).unwrap();
    let mut lines = headers.split("\r\n");
    assert_eq!(lines.next(), Some("HTTP/1.1 200 OK"));
    let mut chunked = false;
    let mut sse = false;
    for line in lines {
        let (name, value) = line.split_once(':').expect("HTTP field");
        if name.eq_ignore_ascii_case("transfer-encoding") {
            assert!(!chunked, "duplicate transfer encoding");
            assert_eq!(value.trim(), "chunked");
            chunked = true;
        }
        if name.eq_ignore_ascii_case("content-type") {
            assert!(!sse, "duplicate content type");
            assert_eq!(value.trim(), "text/event-stream");
            sse = true;
        }
    }
    assert!(chunked && sse);
    let mut rest = &bytes[split + 4..];
    let mut decoded = Vec::new();
    loop {
        let end = rest
            .windows(2)
            .position(|p| p == b"\r\n")
            .expect("chunk length terminator");
        let digits = std::str::from_utf8(&rest[..end]).unwrap();
        assert!(!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_hexdigit()));
        let size = usize::from_str_radix(digits, 16).unwrap();
        rest = &rest[end + 2..];
        if size == 0 {
            assert_eq!(rest, b"\r\n", "no missing terminator or unparsed trailers");
            break;
        }
        assert!(rest.len() >= size + 2, "truncated HTTP chunk");
        decoded.extend_from_slice(&rest[..size]);
        assert_eq!(&rest[size..size + 2], b"\r\n");
        rest = &rest[size + 2..];
    }
    Response {
        status: 200,
        body: String::from_utf8(decoded).unwrap(),
    }
}

/// No filter_map: every frame is consumed or explicitly recognized as a comment.
/// AP application frames have exactly one event field and one JSON data field.
fn parse_ap(body: &str) -> Vec<(String, Value)> {
    assert!(
        !body.contains('\r'),
        "unexpected line-ending bytes in AP projection"
    );
    assert!(body.ends_with("\n\n"), "unterminated SSE frame");
    let mut events = vec![];
    for block in body[..body.len() - 2].split("\n\n") {
        assert!(!block.is_empty(), "empty application frame");
        if block == ": ping" || block == ":ping" {
            continue;
        }
        let mut event = None;
        let mut data = None;
        for line in block.split('\n') {
            if let Some(value) = line.strip_prefix("event: ") {
                assert!(event.replace(value).is_none(), "duplicate event field");
            } else if let Some(value) = line.strip_prefix("data: ") {
                assert!(
                    data.replace(value).is_none(),
                    "extra JSON envelope or duplicate data field"
                );
            } else {
                panic!("unknown or malformed AP SSE field: {line:?}");
            }
        }
        let event = event.expect("data without event");
        assert!(
            matches!(event, "tool_call" | "tool_result" | "done"),
            "unknown application event"
        );
        let UniqueJson(value) = serde_json::from_str(data.expect("event without data"))
            .expect("one exact JSON payload with unique keys");
        events.push((event.to_owned(), value));
    }
    events
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

type Rows = (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>);
fn rows(path: &Path) -> Rows {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    connection.busy_timeout(Duration::from_secs(5)).unwrap();
    let read = |sql, width| {
        let mut query = connection.prepare(sql).unwrap();
        let result = query
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
    let selected = text(id);
    (
        raw.0.iter().filter(|r| r[0] == selected).cloned().collect(),
        raw.1.iter().filter(|r| r[1] == selected).cloned().collect(),
        raw.2.iter().filter(|r| r[2] == selected).cloned().collect(),
    )
}
fn assert_durable(
    target: &Target,
    index: usize,
    view: &ThreadView,
    journal: &[JournalEntry],
    raw: &Rows,
    generation: u64,
) {
    target.assert_counts(1);
    let execution = view.execution.as_ref().unwrap();
    let snapshot = &execution.snapshot;
    let origin = ThreadOrigin {
        application: "native-test".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    };
    assert_eq!(view.thread.id, target.thread_id());
    assert_eq!(execution.thread_id, target.thread_id());
    assert_eq!(view.thread.origin, Some(origin.clone()));
    assert!(view.thread.metadata.is_empty());
    assert_eq!(view.thread.resource_revision, 4);
    assert_eq!(
        view.thread.active_execution_id,
        Some(snapshot.execution_id.clone())
    );
    assert!(!view.thread.generation.as_str().is_empty());
    assert!(view.thread.created_at_ms > 0);
    assert_eq!(snapshot.tasks.len(), 1);
    let attempt = snapshot.tasks[0]
        .attempt_id
        .clone()
        .expect("one admitted attempt");
    let failed = matches!(target.kind, Kind::Failed);
    let failure = failed.then(|| "known terminal failure".to_owned());
    let expected_definition = definition(
        target.route,
        target.revision,
        &[("answer", false, RecoveryPolicy::Pure)],
    );
    let expected_input = json!({"input":target.input}).as_object().unwrap().clone();
    let expected_result = target.expected_result();
    let expected_values = target.expected_values();
    assert_eq!(
        snapshot,
        &Snapshot {
            schema_version: 1,
            revision: 3,
            execution_id: snapshot.execution_id.clone(),
            turn_id: snapshot.turn_id.clone(),
            turn_history: vec![snapshot.turn_id.clone()],
            attempt_history: vec![attempt.clone()],
            owner_generation: generation,
            definition: expected_definition.clone(),
            status: target.terminal_status(),
            batch_index: if failed { 0 } else { 1 },
            values: expected_values.as_object().unwrap().clone(),
            tasks: vec![TaskRecord {
                spec: expected_definition.batches[0][0].clone(),
                status: if failed {
                    TaskStatus::Failed
                } else {
                    TaskStatus::Succeeded
                },
                input: expected_input.clone(),
                attempt_id: Some(attempt.clone()),
                result: expected_result.clone(),
                failure: failure.clone(),
                interrupt: None,
                approved: false,
            }],
            attempts_used: 1,
            updated_at_ms: snapshot.updated_at_ms,
            output: if failed {
                None
            } else {
                Some(expected_values.clone())
            },
            failure,
        }
    );
    if let Some(result) = &expected_result {
        assert_ne!(snapshot.output.as_ref(), Some(&result.output));
    }
    let calls = target.calls.lock().unwrap();
    assert_eq!(
        calls[0],
        (
            EffectInvocation {
                execution_id: snapshot.execution_id.clone(),
                turn_id: snapshot.turn_id.clone(),
                owner_generation: generation,
                task_id: TaskId::new("answer").unwrap(),
                effect_id: EffectId::new("answer").unwrap(),
                attempt_id: attempt,
                input: expected_input,
            },
            InvocationContext {
                thread_id: target.thread_id(),
                thread_generation: view.thread.generation.clone(),
                origin: origin.clone(),
                application_revision: "application-v1".into(),
                execution_revision: ExecutionRevision::new(target.revision).unwrap(),
                policy_revision: "policy-v1".into(),
            },
        )
    );
    assert_eq!(
        raw.0,
        vec![vec![
            text(target.id),
            text(snapshot.execution_id.as_str()),
            blob(view.thread.created_at_ms),
            text("{}"),
            text(serde_json::to_string(&origin).unwrap()),
            blob(4),
            text(view.thread.generation.as_str())
        ]]
    );
    assert_eq!(raw.1.len(), 1);
    assert_eq!(raw.1[0].len(), 5);
    assert_eq!(
        &raw.1[0][..4],
        &[
            text(snapshot.execution_id.as_str()),
            text(target.id),
            blob(3),
            blob(generation)
        ]
    );
    let SqlValue::Text(encoded) = &raw.1[0][4] else {
        panic!("raw snapshot text")
    };
    assert_eq!(
        serde_json::from_str::<Snapshot>(encoded).unwrap(),
        *snapshot
    );
    assert_eq!(raw.2.len(), 5);
    let first_sequence = i64::try_from(index * 5 + 1).unwrap();
    assert_eq!(
        raw.2[0],
        vec![
            SqlValue::Integer(first_sequence),
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
    assert_eq!(journal.len(), 4);
    for (j, status) in [
        ExecutionStatus::Accepted,
        ExecutionStatus::Running,
        ExecutionStatus::Running,
        target.terminal_status(),
    ]
    .into_iter()
    .enumerate()
    {
        let expected = JournalEntry {
            execution_id: snapshot.execution_id.clone(),
            revision: u64::try_from(j).unwrap(),
            owner_generation: generation,
            status,
            updated_at_ms: journal[j].updated_at_ms,
        };
        assert_eq!(journal[j], expected);
        assert!(expected.updated_at_ms >= view.thread.created_at_ms);
        if j > 0 {
            assert!(expected.updated_at_ms >= journal[j - 1].updated_at_ms);
        }
        let row = &raw.2[j + 1];
        assert_eq!(row.len(), 9);
        assert_eq!(
            &row[..8],
            &[
                SqlValue::Integer(first_sequence + i64::try_from(j).unwrap() + 1),
                text(if j == 0 { "admit" } else { "commit" }),
                text(target.id),
                text(snapshot.execution_id.as_str()),
                blob(u64::try_from(j).unwrap()),
                blob(generation),
                text(serde_json::to_string(&status).unwrap()),
                blob(expected.updated_at_ms),
            ]
        );
        let SqlValue::Text(payload) = &row[8] else {
            panic!("raw journal JSON")
        };
        assert_eq!(
            serde_json::from_str::<JournalEntry>(payload).unwrap(),
            expected
        );
    }
    assert_eq!(journal[3].updated_at_ms, snapshot.updated_at_ms);
}
fn assert_gets(thread: &Response, state: &Response, target: &Target, view: &ThreadView) {
    assert_eq!(thread.status, 200);
    let t = thread.json();
    assert_eq!(t["thread_id"], target.id);
    assert_eq!(t["metadata"], json!({}));
    assert_eq!(t["status"], "idle");
    assert_eq!(state.status, 200);
    let s = state.json();
    assert_eq!(s["config"]["configurable"]["thread_id"], target.id);
    assert_eq!(
        s["config"]["configurable"]["checkpoint_id"],
        view.execution
            .as_ref()
            .unwrap()
            .snapshot
            .execution_id
            .as_str()
    );
    assert_eq!(s["metadata"]["route"], target.route);
    assert_eq!(s["metadata"]["execution_revision"], target.revision);
    assert_eq!(s["values"], target.expected_values());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t011_normal_native_streams_end_with_one_done() {
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let targets = [
        Arc::new(Target::new(
            Kind::Structured,
            "release-t011-structured",
            "/terminal-structured#workflow",
            "terminal-structured-v1",
            json!({"question":"structured request","seed":13}),
        )),
        Arc::new(Target::new(
            Kind::Null,
            "release-t011-null",
            "/terminal-null#workflow",
            "terminal-null-v1",
            json!({"question":"null request","seed":27}),
        )),
        Arc::new(Target::new(
            Kind::Void,
            "release-t011-void",
            "/terminal-void#workflow",
            "terminal-void-v1",
            json!({"question":"void request","seed":39}),
        )),
        Arc::new(Target::new(
            Kind::Failed,
            "release-t011-failed",
            "/terminal-failed#workflow",
            "terminal-failed-v1",
            json!({"question":"failure request","seed":51}),
        )),
    ];
    let registrations = targets
        .iter()
        .map(|target| RouteRegistration {
            definition: definition(
                target.route,
                target.revision,
                &[("answer", false, RecoveryPolicy::Pure)],
            ),
            effects: [(
                EffectId::new("answer").unwrap(),
                EffectBinding {
                    callback: Arc::new(Callback(target.clone())),
                    output_kind: target.output_kind(),
                },
            )]
            .into_iter()
            .collect(),
            input_validator: Some(Arc::new(Validator(target.clone()))),
        })
        .collect();
    let app = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "native-test".into(),
                revision: "application-v1".into(),
            },
            registrations,
        )
        .unwrap(),
    );
    let weak_app = Arc::downgrade(&app);
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak_store = Arc::downgrade(&store);
    let generation = store.owner_generation();
    assert_eq!(rows(&directory.0), (vec![], vec![], vec![]));
    let host = HttpHost::new_with_streaming_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        host_config(app.clone()),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Listener::start(&host))
        .await
        .unwrap();
    let address = server.address;
    let shutdown = host.shutdown_handle();
    let mut views: Vec<ThreadView> = vec![];
    let mut journals = vec![];
    for (index, target) in targets.iter().enumerate() {
        for pending in &targets[index..] {
            pending.assert_counts(0);
        }
        assert!(store.thread_view(&target.thread_id()).unwrap().is_none());
        // One whole-request deadline includes connect, authenticated write, headers,
        // every chunk, and EOF. complete retains all header-adjacent body bytes.
        let response = tokio::time::timeout(DEADLINE, async {
            let body = json!({"route":target.route,"input":target.input}).to_string();
            let socket = open(
                address,
                "POST",
                &format!("{}/runs/stream", target.path()),
                &body,
            )
            .await;
            complete(socket, vec![]).await
        })
        .await
        .expect("whole stream request deadline");
        // EOF is not evidence of physical worker retirement. Observe it positively
        // while the host remains healthy and Serving, before the next admission.
        until(|| {
            let status = shutdown.status();
            assert_eq!(status.phase, ShutdownPhase::Serving);
            assert!(!status.faulted);
            status.active_owners == 0
        })
        .await;
        target.assert_counts(1);
        let view = store.thread_view(&target.thread_id()).unwrap().unwrap();
        let execution = view.execution.as_ref().unwrap();
        assert_eq!(
            store
                .execution(&execution.snapshot.execution_id)
                .unwrap()
                .as_ref(),
            Some(execution)
        );
        let journal = store.journal(&execution.snapshot.execution_id).unwrap();
        let raw = rows(&directory.0);
        assert_eq!(raw.0.len(), index + 1);
        assert_eq!(raw.1.len(), index + 1);
        assert_eq!(raw.2.len(), (index + 1) * 5);
        assert_durable(
            target,
            index,
            &view,
            &journal,
            &partition(&raw, target.id),
            generation,
        );
        let attempt = execution.snapshot.tasks[0]
            .attempt_id
            .as_ref()
            .unwrap()
            .as_str();
        let mut expected = vec![(
            "tool_call".to_owned(),
            json!({"id":attempt,"name":"answer","input":{"input":target.input}}),
        )];
        if let Some(result) = target.expected_result() {
            expected.push((
                "tool_result".into(),
                json!({"id":attempt,"name":"answer","output":result.output}),
            ));
        }
        expected.push(("done".into(), target.expected_done()));
        let events = parse_ap(&response.body);
        assert_eq!(events.iter().filter(|(name, _)| name == "done").count(), 1);
        assert_eq!(events.last().unwrap().0, "done");
        assert_eq!(events, expected);
        for previous in &views {
            let prior = &previous.execution.as_ref().unwrap().snapshot;
            assert_ne!(view.thread.generation, previous.thread.generation);
            assert_ne!(execution.snapshot.execution_id, prior.execution_id);
            assert_ne!(execution.snapshot.turn_id, prior.turn_id);
            assert_ne!(execution.snapshot.attempt_history, prior.attempt_history);
            assert!(view.thread.created_at_ms >= prior.updated_at_ms);
        }
        views.push(view);
        journals.push(journal);
    }
    let durable = rows(&directory.0);
    assert_eq!(
        (durable.0.len(), durable.1.len(), durable.2.len()),
        (4, 4, 20)
    );
    for (i, row) in durable.2.iter().enumerate() {
        assert_eq!(row[0], SqlValue::Integer(i64::try_from(i + 1).unwrap()));
    }
    let mut http_views = vec![];
    for (index, target) in targets.iter().enumerate() {
        let thread = tokio::time::timeout(DEADLINE, exchange(address, "GET", &target.path(), ""))
            .await
            .unwrap();
        let state = tokio::time::timeout(
            DEADLINE,
            exchange(address, "GET", &format!("{}/state", target.path()), ""),
        )
        .await
        .unwrap();
        assert_gets(&thread, &state, target, &views[index]);
        http_views.push((thread.body, state.body));
    }
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
        assert_durable(
            target,
            index,
            &views[index],
            &journals[index],
            &partition(&durable, target.id),
            generation,
        );
    }
    assert_eq!(rows(&directory.0), durable);
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&store) == 1 && Arc::strong_count(&app) == 1).await;
    drop(Arc::try_unwrap(store).unwrap_or_else(|_| panic!("original SQLite owner alias")));
    assert!(weak_store.upgrade().is_none());
    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak_reopened = Arc::downgrade(&reopened);
    assert_eq!(reopened.owner_generation(), generation + 1);
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
    assert_eq!(rows(&directory.0), durable);
    let host = HttpHost::new_with_streaming_application(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        host_config(app.clone()),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Listener::start(&host))
        .await
        .unwrap();
    let address = server.address;
    // A fresh host performs GETs only; the original transient stream is not replayed.
    for (index, target) in targets.iter().enumerate() {
        let thread = tokio::time::timeout(DEADLINE, exchange(address, "GET", &target.path(), ""))
            .await
            .unwrap();
        let state = tokio::time::timeout(
            DEADLINE,
            exchange(address, "GET", &format!("{}/state", target.path()), ""),
        )
        .await
        .unwrap();
        assert_gets(&thread, &state, target, &views[index]);
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
    assert_eq!(rows(&directory.0), durable);
    let shutdown = host.shutdown_handle();
    assert_eq!(
        shutdown.status(),
        ShutdownStatus {
            phase: ShutdownPhase::Serving,
            active_owners: 0,
            faulted: false
        }
    );
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
        assert_durable(
            target,
            index,
            &views[index],
            &journals[index],
            &partition(&durable, target.id),
            generation,
        );
    }
    assert_eq!(rows(&directory.0), durable);
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&reopened) == 1 && Arc::strong_count(&app) == 1).await;
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("fresh SQLite owner alias")));
    assert!(weak_reopened.upgrade().is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    for target in &targets {
        target.assert_counts(1);
    }
    drop(directory);
    assert!(
        !directory_path.exists(),
        "fixture directory must be removed"
    );
}
