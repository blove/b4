//! T010: direct native payloads, witnessed after a real SQLite completion commit.
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{io::AsyncReadExt, net::TcpStream};

mod native_support;
use native_support::{
    application, config, definition, exchange, open, prefix, until, Auth, Directory, Owner,
    Response, Server,
};

const DEADLINE: Duration = Duration::from_secs(10);
const THREAD: &str = "release-t010-direct";
const ROUTE: &str = "/direct-stream#workflow";
const REVISION: &str = "direct-stream-v1";

#[derive(Default)]
struct Observed {
    validated: Mutex<Vec<Value>>,
    entered: AtomicUsize,
    returned: AtomicUsize,
    calls: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
    emissions: Mutex<Vec<NativeChunkResult>>,
}
struct Validator(Arc<Observed>);
impl InputValidator for Validator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.validated.lock().unwrap().push(input.clone());
        Ok(())
    }
}
struct Callback(Arc<Observed>);
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        panic!("the primary opted-in stream must enter the streaming callback")
    }
    fn invoke_streaming(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        _: &NativeCancellationObserver,
        sink: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.0.entered.fetch_add(1, Ordering::SeqCst);
        self.0
            .calls
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        // Callback literals are deliberately separate from the acceptance oracle.
        self.0
            .emissions
            .lock()
            .unwrap()
            .push(sink.emit(&json!({"piece":"雪 \"quoted\"\nnext", "index":7})));
        let result = TaskResult {
            output: json!({"answer":"native final", "details":[true,23,null]}),
            writes: vec![StateWrite::Replace {
                field: "aggregate".into(),
                value: json!({"saved":"state only", "count":91}),
            }],
        };
        self.0.returned.fetch_add(1, Ordering::SeqCst);
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
            result,
        )))
    }
}

// Positive commit witness: hold only AFTER SQLite has returned committed success.
#[derive(Default)]
struct BoundaryFacts {
    entered: bool,
    released: bool,
    cleanup: bool,
    expired: bool,
}
#[derive(Default)]
struct Boundary {
    facts: Mutex<BoundaryFacts>,
    changed: Condvar,
    returned: AtomicUsize,
}
impl Boundary {
    fn hold(&self) {
        let end = Instant::now() + Duration::from_secs(20);
        let mut facts = self.facts.lock().unwrap();
        facts.entered = true;
        self.changed.notify_all();
        while !facts.released && !facts.cleanup {
            let remaining = end.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                facts.expired = true;
                break;
            }
            facts = self.changed.wait_timeout(facts, remaining).unwrap().0;
        }
        let explicitly_released = facts.released && !facts.cleanup && !facts.expired;
        drop(facts);
        assert!(
            explicitly_released,
            "completion witness was not explicitly released before its deadline"
        );
    }
    fn release(&self) {
        self.facts.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}
struct ReleaseOnDrop(Arc<Boundary>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0
            .facts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cleanup = true;
        self.0.changed.notify_all();
    }
}
struct CompletionStore {
    inner: Arc<b4_sqlite::SqliteStore>,
    boundary: Arc<Boundary>,
}
impl Store for CompletionStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, snapshot)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let committed = self.inner.commit(id, fence, plan)?;
        if committed
            .snapshot
            .tasks
            .iter()
            .any(|task| task.status == TaskStatus::Succeeded)
        {
            assert_eq!(committed.snapshot.status, ExecutionStatus::Completed);
            self.boundary.hold();
            self.boundary.returned.fetch_add(1, Ordering::SeqCst);
        }
        Ok(committed)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
}
impl ApprovalStore for CompletionStore {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, fence, plan, mutation)
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
        let mut event = None;
        let mut data = None;
        let mut comments = 0;
        for line in block.split('\n') {
            if line.starts_with(':') {
                comments += 1;
                continue;
            }
            if let Some(value) = line.strip_prefix("event: ") {
                assert!(event.replace(value).is_none(), "duplicate event field");
            } else if let Some(value) = line.strip_prefix("data: ") {
                assert!(
                    data.replace(value).is_none(),
                    "extra data envelope or duplicate data field"
                );
            } else {
                panic!("unknown or malformed AP SSE field: {line:?}");
            }
        }
        if event.is_none() && data.is_none() {
            assert!(
                comments > 0,
                "only explicit legal heartbeat comments may be skipped"
            );
            continue;
        }
        let event = event.expect("data without event");
        assert!(
            matches!(event, "tool_call" | "chunk" | "tool_result" | "done"),
            "unknown application event"
        );
        let value: Value =
            serde_json::from_str(data.expect("event without data")).expect("exact JSON payload");
        events.push((event.to_owned(), value));
    }
    events
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
fn assert_observed(observed: &Observed) {
    assert_eq!(
        *observed.validated.lock().unwrap(),
        vec![json!({"question":"stream original", "seed":37})]
    );
    assert_eq!(observed.entered.load(Ordering::SeqCst), 1);
    assert_eq!(observed.returned.load(Ordering::SeqCst), 1);
    assert_eq!(observed.calls.lock().unwrap().len(), 1);
    assert_eq!(
        *observed.emissions.lock().unwrap(),
        vec![NativeChunkResult::Accepted]
    );
}
fn assert_durable(
    view: &ThreadView,
    journal: &[JournalEntry],
    raw: &Rows,
    generation: u64,
    observed: &Observed,
) {
    assert_observed(observed);
    let execution = view.execution.as_ref().unwrap();
    let s = &execution.snapshot;
    let origin = ThreadOrigin {
        application: "native-test".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    };
    assert_eq!(view.thread.id.as_str(), THREAD);
    assert_eq!(execution.thread_id.as_str(), THREAD);
    assert_eq!(view.thread.origin, Some(origin.clone()));
    assert!(view.thread.metadata.is_empty());
    assert_eq!(view.thread.resource_revision, 4);
    assert_eq!(
        view.thread.active_execution_id.as_ref(),
        Some(&s.execution_id)
    );
    assert!(!view.thread.generation.as_str().is_empty());
    assert_eq!(s.schema_version, 1);
    assert_eq!(s.owner_generation, generation);
    assert_eq!(s.revision, 3);
    assert_eq!(s.status, ExecutionStatus::Completed);
    assert_eq!(s.batch_index, 1);
    assert_eq!(
        s.definition,
        definition(ROUTE, REVISION, &[("compose", false, RecoveryPolicy::Pure)])
    );
    assert_eq!(s.turn_history, vec![s.turn_id.clone()]);
    assert!(s.failure.is_none());
    assert_eq!(s.attempts_used, 1);
    assert_eq!(s.attempt_history.len(), 1);
    assert_eq!(s.tasks.len(), 1);
    let task = &s.tasks[0];
    assert_eq!(task.spec.id.as_str(), "compose");
    assert_eq!(task.spec.effect_id.as_str(), "compose");
    assert_eq!(task.spec.recovery, RecoveryPolicy::Pure);
    assert!(!task.spec.requires_approval);
    assert_eq!(task.status, TaskStatus::Succeeded);
    assert!(!task.approved);
    assert!(task.failure.is_none() && task.interrupt.is_none());
    assert_eq!(
        task.input,
        json!({"input":{"question":"stream original", "seed":37}})
            .as_object()
            .unwrap()
            .clone()
    );
    assert_eq!(
        task.result,
        Some(TaskResult {
            output: json!({"answer":"native final", "details":[true,23,null]}),
            writes: vec![StateWrite::Replace {
                field: "aggregate".into(),
                value: json!({"saved":"state only", "count":91})
            }],
        })
    );
    let aggregate = json!({"input":{"question":"stream original", "seed":37},"aggregate":{"saved":"state only", "count":91}});
    assert_eq!(&s.values, aggregate.as_object().unwrap());
    assert_eq!(s.output, Some(aggregate));
    assert_ne!(
        s.output.as_ref(),
        Some(&task.result.as_ref().unwrap().output)
    );
    let calls = observed.calls.lock().unwrap();
    let (call, context) = &calls[0];
    assert_eq!(
        call,
        &EffectInvocation {
            execution_id: s.execution_id.clone(),
            turn_id: s.turn_id.clone(),
            owner_generation: generation,
            task_id: TaskId::new("compose").unwrap(),
            effect_id: EffectId::new("compose").unwrap(),
            attempt_id: task.attempt_id.clone().unwrap(),
            input: json!({"input":{"question":"stream original", "seed":37}})
                .as_object()
                .unwrap()
                .clone(),
        }
    );
    assert_eq!(s.attempt_history, vec![call.attempt_id.clone()]);
    assert_eq!(
        context,
        &InvocationContext {
            thread_id: ThreadId::new(THREAD).unwrap(),
            thread_generation: view.thread.generation.clone(),
            origin: origin.clone(),
            application_revision: "application-v1".into(),
            execution_revision: ExecutionRevision::new(REVISION).unwrap(),
            policy_revision: "policy-v1".into(),
        }
    );
    assert_eq!(
        raw.0,
        vec![vec![
            text(THREAD),
            text(s.execution_id.as_str()),
            blob(view.thread.created_at_ms),
            text("{}"),
            text(serde_json::to_string(&origin).unwrap()),
            blob(4),
            text(view.thread.generation.as_str())
        ]]
    );
    assert_eq!(raw.1.len(), 1);
    assert_eq!(
        &raw.1[0][..4],
        &[
            text(s.execution_id.as_str()),
            text(THREAD),
            blob(3),
            blob(generation)
        ]
    );
    let SqlValue::Text(snapshot) = &raw.1[0][4] else {
        panic!("snapshot text required")
    };
    assert_eq!(serde_json::from_str::<Snapshot>(snapshot).unwrap(), *s);
    assert_eq!(raw.1[0].len(), 5);
    assert_eq!(raw.2.len(), 5);
    assert_eq!(
        raw.2[0],
        vec![
            SqlValue::Integer(1),
            text("thread"),
            text(THREAD),
            SqlValue::Null,
            SqlValue::Null,
            blob(generation),
            SqlValue::Null,
            blob(view.thread.created_at_ms),
            SqlValue::Null
        ]
    );
    assert_eq!(journal.len(), 4);
    for (index, (sequence, kind, revision, status)) in [
        (2, "admit", 0, ExecutionStatus::Accepted),
        (3, "commit", 1, ExecutionStatus::Running),
        (4, "commit", 2, ExecutionStatus::Running),
        (5, "commit", 3, ExecutionStatus::Completed),
    ]
    .into_iter()
    .enumerate()
    {
        let expected = JournalEntry {
            execution_id: s.execution_id.clone(),
            revision,
            owner_generation: generation,
            status,
            updated_at_ms: journal[index].updated_at_ms,
        };
        assert_eq!(journal[index], expected);
        assert!(expected.updated_at_ms >= view.thread.created_at_ms);
        if index > 0 {
            assert!(expected.updated_at_ms >= journal[index - 1].updated_at_ms);
        }
        let row = &raw.2[index + 1];
        assert_eq!(
            &row[..8],
            &[
                SqlValue::Integer(sequence),
                text(kind),
                text(THREAD),
                text(s.execution_id.as_str()),
                blob(revision),
                blob(generation),
                text(serde_json::to_string(&status).unwrap()),
                blob(expected.updated_at_ms)
            ]
        );
        assert_eq!(row.len(), 9);
        let SqlValue::Text(payload) = &row[8] else {
            panic!("journal JSON required")
        };
        assert_eq!(
            serde_json::from_str::<JournalEntry>(payload).unwrap(),
            expected
        );
    }
    assert_eq!(journal[3].updated_at_ms, s.updated_at_ms);
}

fn assert_gets(thread: &Response, state: &Response, view: &ThreadView) {
    assert_eq!(thread.status, 200);
    let t = thread.json();
    assert_eq!(t["thread_id"], THREAD);
    assert_eq!(t["metadata"], json!({}));
    assert_eq!(t["status"], "idle");
    assert_eq!(state.status, 200);
    let s = state.json();
    assert_eq!(s["config"]["configurable"]["thread_id"], THREAD);
    assert_eq!(
        s["config"]["configurable"]["checkpoint_id"],
        view.execution
            .as_ref()
            .unwrap()
            .snapshot
            .execution_id
            .as_str()
    );
    assert_eq!(s["metadata"]["route"], ROUTE);
    assert_eq!(s["metadata"]["execution_revision"], REVISION);
    assert_eq!(
        s["values"],
        json!({"input":{"question":"stream original", "seed":37},"aggregate":{"saved":"state only", "count":91}})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_t010_native_stream_payloads_are_direct_after_commit() {
    let directory = Directory::new();
    let directory_path = directory.0.clone();
    let observed = Arc::new(Observed::default());
    let boundary = Arc::new(Boundary::default());
    let _release_on_panic = ReleaseOnDrop(boundary.clone());
    let app = application(
        definition(ROUTE, REVISION, &[("compose", false, RecoveryPolicy::Pure)]),
        Arc::new(Callback(observed.clone())),
        Some(Arc::new(Validator(observed.clone()))),
    );
    let weak_app = Arc::downgrade(&app);
    let sqlite = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak_sqlite = Arc::downgrade(&sqlite);
    let generation = sqlite.owner_generation();
    let store = Arc::new(CompletionStore {
        inner: sqlite,
        boundary: boundary.clone(),
    });
    let weak_store = Arc::downgrade(&store);
    assert_eq!(rows(&directory.0), (vec![], vec![], vec![]));
    let host = HttpHost::new_with_streaming_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), true, 1),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .unwrap();
    let address = server.address;
    let mut socket = tokio::time::timeout(DEADLINE, open(address,"POST","/threads/release-t010-direct/runs/stream",r#"{"route":"/direct-stream#workflow","input":{"question":"stream original","seed":37}}"#)).await.unwrap();
    // Header admission is positive evidence only. Keep every possible body byte.
    let initial = prefix(&mut socket, b"\r\n\r\n").await;
    until(|| boundary.facts.lock().unwrap().entered).await;
    assert_eq!(boundary.returned.load(Ordering::SeqCst), 0);
    assert!(!boundary.facts.lock().unwrap().released);
    let id = ThreadId::new(THREAD).unwrap();
    let view = store.thread_view(&id).unwrap().unwrap();
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
    assert_durable(&view, &journal, &raw, generation, &observed);
    assert!(
        weak_sqlite.upgrade().is_some()
            && weak_store.upgrade().is_some()
            && weak_app.upgrade().is_some()
    );
    // No timing/no-bytes claim: the positive boundary proves SQLite completion
    // while the driver still has not received its StoredExecution return value.
    boundary.release();
    let response = complete(socket, initial).await;
    assert_eq!(boundary.returned.load(Ordering::SeqCst), 1);
    let attempt = execution.snapshot.tasks[0]
        .attempt_id
        .as_ref()
        .unwrap()
        .as_str();
    assert_eq!(
        parse_ap(&response.body),
        vec![
            (
                "tool_call".into(),
                json!({"id":attempt,"name":"compose","input":{"input":{"question":"stream original","seed":37}}})
            ),
            (
                "chunk".into(),
                json!({"piece":"雪 \"quoted\"\nnext","index":7})
            ),
            (
                "tool_result".into(),
                json!({"id":attempt,"name":"compose","output":{"answer":"native final","details":[true,23,null]}})
            ),
            (
                "done".into(),
                json!({"output":{"answer":"native final","details":[true,23,null]}})
            ),
        ]
    );
    assert_observed(&observed);
    assert_eq!(store.thread_view(&id).unwrap().unwrap(), view);
    assert_eq!(rows(&directory.0), raw);
    let before_thread = tokio::time::timeout(
        DEADLINE,
        exchange(address, "GET", "/threads/release-t010-direct", ""),
    )
    .await
    .unwrap();
    let before_state = tokio::time::timeout(
        DEADLINE,
        exchange(address, "GET", "/threads/release-t010-direct/state", ""),
    )
    .await
    .unwrap();
    assert_gets(&before_thread, &before_state, &view);
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_durable(
        &store.thread_view(&id).unwrap().unwrap(),
        &store.journal(&execution.snapshot.execution_id).unwrap(),
        &rows(&directory.0),
        generation,
        &observed,
    );
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&store) == 1 && Arc::strong_count(&app) == 1).await;
    let retired = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("retired store alias"));
    let CompletionStore {
        inner,
        boundary: retired_boundary,
    } = retired;
    drop(retired_boundary);
    drop(Arc::try_unwrap(inner).unwrap_or_else(|_| panic!("SQLite owner alias")));
    assert!(weak_store.upgrade().is_none() && weak_sqlite.upgrade().is_none());
    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak_reopened = Arc::downgrade(&reopened);
    assert_eq!(reopened.owner_generation(), generation + 1);
    assert_eq!(reopened.thread_view(&id).unwrap().unwrap(), view);
    assert_eq!(
        reopened.journal(&execution.snapshot.execution_id).unwrap(),
        journal
    );
    assert_eq!(rows(&directory.0), raw);
    let host = HttpHost::new_with_streaming_application(
        reopened.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        config(app.clone(), true, 1),
    )
    .unwrap();
    let server = tokio::time::timeout(DEADLINE, Server::start(&host, None))
        .await
        .unwrap();
    let address = server.address;
    // Fresh host, GET only: no validator or callback replay, attempt or journal mutation.
    let thread = tokio::time::timeout(
        DEADLINE,
        exchange(address, "GET", "/threads/release-t010-direct", ""),
    )
    .await
    .unwrap();
    let state = tokio::time::timeout(
        DEADLINE,
        exchange(address, "GET", "/threads/release-t010-direct/state", ""),
    )
    .await
    .unwrap();
    assert_gets(&thread, &state, &view);
    assert_eq!(thread.body, before_thread.body);
    assert_eq!(state.body, before_state.body);
    assert_eq!(reopened.thread_view(&id).unwrap().unwrap(), view);
    assert_eq!(
        reopened.journal(&execution.snapshot.execution_id).unwrap(),
        journal
    );
    assert_eq!(rows(&directory.0), raw);
    assert_observed(&observed);
    let shutdown = host.shutdown_handle();
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    assert_eq!(
        tokio::time::timeout(DEADLINE, shutdown.wait_quiescent(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert_eq!(reopened.thread_view(&id).unwrap().unwrap(), view);
    assert_eq!(
        reopened.journal(&execution.snapshot.execution_id).unwrap(),
        journal
    );
    assert_eq!(rows(&directory.0), raw);
    tokio::time::timeout(DEADLINE, server.stop()).await.unwrap();
    assert!(tokio::time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .is_err());
    drop(shutdown);
    drop(host);
    until(|| Arc::strong_count(&reopened) == 1 && Arc::strong_count(&app) == 1).await;
    drop(Arc::try_unwrap(reopened).unwrap_or_else(|_| panic!("fresh SQLite alias")));
    assert!(weak_reopened.upgrade().is_none());
    drop(app);
    assert!(weak_app.upgrade().is_none());
    assert_observed(&observed);
    assert_eq!(boundary.returned.load(Ordering::SeqCst), 1);
    let facts = boundary.facts.lock().unwrap();
    assert!(facts.entered && facts.released && !facts.expired && !facts.cleanup);
    drop(facts);
    drop(directory);
    assert!(!directory_path.exists());
}
