//! T-018: a grant is committed before its first real TCP disclosure.
#![cfg(unix)]

use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

const CHILD_MODE: &str = "B4_T018_CHILD_MODE";
const CHILD_DIRECTORY: &str = "B4_T018_DIRECTORY";
const CHILD_NONCE: &str = "B4_T018_NONCE";
const ROUTE: &str = "/release-t018#workflow";
const REVISION: &str = "release-t018-v1";
const TASK: &str = "approval";
const PROMPT: &str = "T018 private prompt: approve local action?";
const MAX_WIRE: usize = 2 * 1024 * 1024;
const MAX_FRAMES: usize = 1024;
const MAX_FRAME: usize = 64 * 1024;
const IO_DEADLINE: Duration = Duration::from_secs(20);

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer owner") {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "t018-app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
    }
}
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        context.origin.as_ref().is_none_or(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}
struct ForbiddenEffect;
impl NativeEffect for ForbiddenEffect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        panic!("approval effect entered before a valid resume")
    }
    fn invoke_streaming(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
        _: &NativeCancellationObserver,
        _: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        panic!("streaming approval effect entered before a valid resume")
    }
}
fn application() -> Arc<NativeApplication> {
    let definition = Definition {
        route: RouteKey::new(ROUTE).unwrap(),
        revision: ExecutionRevision::new(REVISION).unwrap(),
        max_attempts: 3,
        batches: vec![vec![TaskSpec {
            id: TaskId::new(TASK).unwrap(),
            effect_id: EffectId::new("approved-effect").unwrap(),
            requires_approval: true,
            recovery: RecoveryPolicy::Pure,
        }]],
    };
    Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "t018-app".into(),
                revision: "t018-app-v1".into(),
            },
            vec![RouteRegistration {
                definition,
                effects: BTreeMap::from([(
                    EffectId::new("approved-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(ForbiddenEffect),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    )
}
fn config() -> NativeHostConfig {
    let approval = ApprovalHostConfig {
        policy_revision: "t018-policy-v1".into(),
        ttl_ms: 120_000,
        prompts: BTreeMap::from([(
            RouteKey::new(ROUTE).unwrap(),
            BTreeMap::from([(TaskId::new(TASK).unwrap(), json!(PROMPT))]),
        )]),
        service_config: ApprovalServiceConfig::RequiredDurable,
    };
    let mut config = NativeHostConfig::new(application(), approval);
    config.limits.max_active_runs = 4;
    config.limits.heartbeat_ms = 100;
    config
}
fn child(mode: &str, directory: &Path, nonce: &str) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = Arc::new(SqliteStore::open(directory).unwrap());
        let host = HttpHost::new_with_application(store, Arc::new(Auth), Arc::new(Policy), config()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        assert!(address.ip().is_loopback());
        let connection = Connection::open(directory.join("runtime.sqlite3")).unwrap();
        match mode {
            "grant_abort" => connection.execute_batch("CREATE TRIGGER t018_grant_abort BEFORE INSERT ON approval_grants BEGIN SELECT RAISE(ABORT, 'private-t018-grant-failure'); END;").unwrap(),
            "journal_abort" => connection.execute_batch("CREATE TRIGGER t018_journal_abort BEFORE INSERT ON journal WHEN NEW.status = '\"waiting_for_approval\"' BEGIN SELECT RAISE(ABORT, 'private-t018-journal-failure'); END;").unwrap(),
            "healthy" => {},
            _ => panic!("unknown child mode"),
        }
        drop(connection);
        let ready = directory.join("ready");
        let temporary = directory.join("ready.tmp");
        let mut file = File::create(&temporary).unwrap();
        file.write_all(
            &serde_json::to_vec(&json!({
                "nonce": nonce,
                "pid": std::process::id(),
                "port": address.port(),
            }))
            .unwrap(),
        )
        .unwrap();
        file.sync_all().unwrap();
        drop(file);
        fs::rename(temporary, ready).unwrap();
        axum::serve(listener, host.router()).await.unwrap();
    });
}
struct Fixture {
    directory: PathBuf,
    process: Option<Child>,
    address: SocketAddr,
    started: Instant,
}
impl Fixture {
    fn start(mode: &str) -> Self {
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        assert_eq!(nonce.len(), 32);
        let directory = std::env::temp_dir().join(format!("b4-t018-{nonce}"));
        fs::create_dir(&directory).unwrap();
        let log = File::create(directory.join("child.log")).unwrap();
        let process = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("release_t018_approval_park_is_durable_before_tcp_disclosure")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_MODE, mode)
            .env(CHILD_DIRECTORY, &directory)
            .env(CHILD_NONCE, &nonce)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let pid = process.id();
        let started = Instant::now();
        let mut fixture = Self {
            directory,
            process: Some(process),
            address: "127.0.0.1:1".parse().unwrap(),
            started,
        };
        let ready = fixture.directory.join("ready");
        loop {
            assert!(
                started.elapsed() <= Duration::from_secs(10),
                "child readiness exceeded 10 seconds"
            );
            assert!(
                fixture
                    .process
                    .as_mut()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "child exited before readiness"
            );
            if let Ok(value) = fs::read_to_string(&ready) {
                let fields: Value = serde_json::from_str(&value).expect("readiness must be JSON");
                assert_eq!(fields.as_object().unwrap().len(), 3);
                assert_eq!(fields["nonce"].as_str(), Some(nonce.as_str()));
                assert_eq!(fields["pid"].as_u64(), Some(u64::from(pid)));
                let port = u16::try_from(fields["port"].as_u64().unwrap()).unwrap();
                assert_ne!(port, 0);
                fixture.address = format!("127.0.0.1:{port}").parse().unwrap();
                return fixture;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn enforce_lifetime(&self) {
        assert!(
            self.started.elapsed() <= Duration::from_secs(120),
            "child lifetime exceeded 120 seconds"
        );
    }
    fn kill_reap(&mut self) {
        if let Some(process) = self.process.as_mut() {
            #[cfg(unix)]
            {
                let mut signal = Command::new("kill")
                    .arg("-TERM")
                    .arg(process.id().to_string())
                    .spawn()
                    .unwrap();
                let signal_start = Instant::now();
                let status = loop {
                    if let Some(status) = signal.try_wait().unwrap() {
                        break status;
                    }
                    if signal_start.elapsed() >= Duration::from_secs(5) {
                        let _ = signal.kill();
                        let _ = signal.wait();
                        panic!("TERM delivery exceeded 5 seconds");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                };
                assert!(status.success(), "TERM delivery failed");
                let start = Instant::now();
                while process.try_wait().unwrap().is_none()
                    && start.elapsed() < Duration::from_secs(5)
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                if process.try_wait().unwrap().is_none() {
                    process.kill().unwrap();
                }
                let start = Instant::now();
                while process.try_wait().unwrap().is_none()
                    && start.elapsed() < Duration::from_secs(5)
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                assert!(
                    process.try_wait().unwrap().is_some(),
                    "KILL/reap exceeded 5 seconds"
                );
            }
        }
        self.process.take();
    }
    fn hard_kill_reap(&mut self) {
        if let Some(process) = self.process.as_mut() {
            process.kill().unwrap();
            let start = Instant::now();
            while process.try_wait().unwrap().is_none() && start.elapsed() < Duration::from_secs(5)
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                process.try_wait().unwrap().is_some(),
                "hard kill/reap exceeded 5 seconds"
            );
        }
        self.process.take();
    }
    fn diagnostics(&self) -> Vec<u8> {
        let log = self.directory.join("child.log");
        let mut bytes = Vec::new();
        File::open(log)
            .unwrap()
            .take((MAX_WIRE + 1) as u64)
            .read_to_end(&mut bytes)
            .unwrap();
        assert!(bytes.len() <= MAX_WIRE, "child diagnostics exceeded cap");
        bytes
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // This fallback also runs while an assertion is unwinding. The directory
        // remains intact until a successful reap; an unreapable child is fatal.
        if let Some(mut process) = self.process.take() {
            let mut reaped = matches!(process.try_wait(), Ok(Some(_)));
            if !reaped {
                if let Ok(mut signal) = Command::new("kill")
                    .arg("-TERM")
                    .arg(process.id().to_string())
                    .spawn()
                {
                    let signal_start = Instant::now();
                    loop {
                        match signal.try_wait() {
                            Ok(Some(_)) => break,
                            Ok(None) if signal_start.elapsed() < Duration::from_secs(5) => {
                                std::thread::sleep(Duration::from_millis(10));
                            }
                            _ => {
                                let _ = signal.kill();
                                let _ = signal.try_wait();
                                break;
                            }
                        }
                    }
                }
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(5) {
                    match process.try_wait() {
                        Ok(Some(_)) => {
                            reaped = true;
                            break;
                        }
                        Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                        Err(_) => break,
                    }
                }
            }
            if !reaped {
                let _ = process.kill();
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(5) {
                    match process.try_wait() {
                        Ok(Some(_)) => {
                            reaped = true;
                            break;
                        }
                        Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                        Err(_) => break,
                    }
                }
            }
            if !reaped {
                eprintln!(
                    "T-018 child could not be reaped; preserving {}",
                    self.directory.display()
                );
                std::process::abort();
            }
        }
        let _ = fs::remove_dir_all(&self.directory);
    }
}

// The parser works on an accumulated byte buffer. It does not equate reads with
// HTTP chunks, SSE frames, or JSON values.
struct Wire {
    stream: TcpStream,
    raw: Vec<u8>,
    decoded: Vec<u8>,
    cursor: usize,
    status: u16,
    headers: BTreeMap<String, String>,
    chunked: bool,
    complete: bool,
    eof: bool,
    chunks: usize,
    deadline: Instant,
}
fn delimiter(bytes: &[u8], delimiter: &[u8]) -> Option<usize> {
    bytes
        .windows(delimiter.len())
        .position(|window| window == delimiter)
}
impl Wire {
    fn open(address: SocketAddr, method: &str, path: &str, body: &str) -> Self {
        let deadline = Instant::now() + IO_DEADLINE;
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write!(stream, "{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer owner\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        let mut wire = Self {
            stream,
            raw: vec![],
            decoded: vec![],
            cursor: 0,
            status: 0,
            headers: BTreeMap::new(),
            chunked: false,
            complete: false,
            eof: false,
            chunks: 0,
            deadline,
        };
        while delimiter(&wire.raw, b"\r\n\r\n").is_none() {
            wire.read_more();
        }
        let end = delimiter(&wire.raw, b"\r\n\r\n").unwrap();
        let header = std::str::from_utf8(&wire.raw[..end]).unwrap();
        let mut lines = header.split("\r\n");
        let status_line = lines.next().unwrap();
        assert!(status_line.starts_with("HTTP/1.1 ") || status_line.starts_with("HTTP/1.0 "));
        wire.status = status_line
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        for line in lines {
            let (key, value) = line.split_once(':').expect("malformed HTTP header");
            assert!(!key.is_empty());
            assert!(wire
                .headers
                .insert(key.to_ascii_lowercase(), value.trim().to_owned())
                .is_none());
        }
        wire.chunked = wire
            .headers
            .get("transfer-encoding")
            .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
        wire.cursor = end + 4;
        wire.pump();
        wire
    }
    fn read_more(&mut self) {
        assert!(
            Instant::now() < self.deadline,
            "HTTP/read/EOF exceeded 20 seconds"
        );
        let mut buffer = [0u8; 8192];
        match self.stream.read(&mut buffer) {
            Ok(0) => {
                assert!(!self.chunked || self.complete, "truncated chunked body");
                self.complete = true;
                self.eof = true;
            }
            Ok(count) => {
                assert!(
                    self.raw.len() + count <= MAX_WIRE,
                    "wire response exceeded 2 MiB"
                );
                self.raw.extend_from_slice(&buffer[..count]);
                self.pump();
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) => {}
            Err(error) => panic!("HTTP read failed: {error}"),
        }
    }
    fn pump(&mut self) {
        if self.status == 0 {
            return;
        }
        if !self.chunked {
            self.decoded.extend_from_slice(&self.raw[self.cursor..]);
            self.cursor = self.raw.len();
            return;
        }
        while !self.complete {
            let Some(line_end_relative) = delimiter(&self.raw[self.cursor..], b"\r\n") else {
                return;
            };
            let line_end = self.cursor + line_end_relative;
            let size_line = std::str::from_utf8(&self.raw[self.cursor..line_end]).unwrap();
            let hex = size_line.split(';').next().unwrap();
            let size = usize::from_str_radix(hex, 16).expect("invalid chunk size");
            assert!(size <= MAX_WIRE && self.decoded.len() + size <= MAX_WIRE);
            let body_start = line_end + 2;
            if size == 0 {
                if self.raw.len() < body_start + 2 {
                    return;
                }
                assert_eq!(
                    &self.raw[body_start..body_start + 2],
                    b"\r\n",
                    "unexpected chunk trailers"
                );
                self.cursor = body_start + 2;
                self.complete = true;
                return;
            }
            if self.raw.len() < body_start + size + 2 {
                return;
            }
            assert_eq!(&self.raw[body_start + size..body_start + size + 2], b"\r\n");
            self.chunks += 1;
            assert!(self.chunks <= MAX_FRAMES, "chunk count exceeded 1024");
            self.decoded
                .extend_from_slice(&self.raw[body_start..body_start + size]);
            self.cursor = body_start + size + 2;
        }
    }
    fn finish(&mut self) {
        while !self.complete || !self.eof {
            self.read_more();
        }
        if let Some(length) = self.headers.get("content-length") {
            assert_eq!(self.decoded.len(), length.parse::<usize>().unwrap());
        }
        assert_eq!(self.cursor, self.raw.len(), "unparsed HTTP bytes");
    }
    fn json(&mut self) -> Value {
        self.finish();
        assert!(self.decoded.len() <= MAX_WIRE);
        let value: Value = serde_json::from_slice(&self.decoded).unwrap();
        assert_json_depth(&value, 0);
        value
    }
}
fn assert_json_depth(value: &Value, depth: usize) {
    assert!(depth <= 64, "JSON depth exceeded 64");
    match value {
        Value::Array(items) => {
            for item in items {
                assert_json_depth(item, depth + 1);
            }
        }
        Value::Object(fields) => {
            for item in fields.values() {
                assert_json_depth(item, depth + 1);
            }
        }
        _ => {}
    }
}
fn frames(bytes: &[u8], allow_incomplete: bool) -> (Vec<(String, Value)>, usize) {
    let mut offset = 0;
    let mut events = vec![];
    let mut frame_count = 0;
    while let Some(end) = delimiter(&bytes[offset..], b"\n\n") {
        frame_count += 1;
        assert!(frame_count <= MAX_FRAMES, "SSE frame count exceeded 1024");
        let frame = &bytes[offset..offset + end];
        assert!(frame.len() <= MAX_FRAME);
        offset += end + 2;
        if frame.is_empty() {
            continue;
        }
        let text = std::str::from_utf8(frame).unwrap();
        let mut event = None;
        let mut data = None;
        for line in text.lines() {
            let line = line.trim_end_matches('\r');
            if line.starts_with(':') {
                continue;
            } else if let Some(value) = line.strip_prefix("event: ") {
                assert!(event.replace(value).is_none(), "duplicate SSE event");
            } else if let Some(value) = line.strip_prefix("data: ") {
                assert!(data.replace(value).is_none(), "duplicate SSE data");
            } else {
                panic!("malformed SSE frame");
            }
        }
        if event.is_none() && data.is_none() {
            continue;
        }
        let value: Value = serde_json::from_str(data.expect("missing SSE data")).unwrap();
        assert_json_depth(&value, 0);
        events.push((event.expect("missing SSE event").to_owned(), value));
        assert!(events.len() <= MAX_FRAMES);
    }
    if !allow_incomplete {
        assert_eq!(offset, bytes.len(), "incomplete SSE frame");
    }
    assert!(bytes.len() - offset <= MAX_FRAME);
    (events, offset)
}
fn readonly(directory: &Path) -> Connection {
    let connection = Connection::open_with_flags(
        directory.join("runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    connection.busy_timeout(Duration::from_millis(100)).unwrap();
    connection
}
fn count(connection: &Connection, table: &str) -> i64 {
    assert!(["approval_grants", "journal"].contains(&table));
    connection
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}
fn number(bytes: &[u8]) -> u64 {
    assert_eq!(bytes.len(), 8);
    u64::from_be_bytes(bytes.try_into().unwrap())
}
type RawJournalRow = (String, String, Vec<u8>, Vec<u8>, String, String);
type RawGrantRow = (String, String, String, String, Vec<u8>, i64, String);
#[derive(Clone, Debug, PartialEq)]
struct Evidence {
    thread_generation: String,
    resource_revision: u64,
    resource_revision_raw: Vec<u8>,
    execution_id: String,
    snapshot: Snapshot,
    snapshot_raw: String,
    grant: GrantRecord,
    grant_raw: String,
    journal: Vec<RawJournalRow>,
}
fn evidence(connection: &Connection, thread: &str) -> Option<Evidence> {
    let row = connection.query_row(
        "SELECT t.generation, t.resource_revision, e.id, e.snapshot FROM threads t JOIN executions e ON e.id=t.active_execution_id WHERE t.id=?1",
        [thread], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?)),
    ).ok()?;
    let snapshot: Snapshot = serde_json::from_str(&row.3).unwrap();
    let grants: Vec<RawGrantRow> = connection.prepare(
        "SELECT execution_id, interrupt_id, task_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants"
    ).unwrap().query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?))).unwrap().collect::<Result<_, _>>().unwrap();
    if grants.len() != 1 {
        return None;
    }
    let grant = &grants[0];
    assert_eq!(grant.0, row.2);
    assert_eq!(grant.2, TASK);
    assert_eq!(grant.3, row.0);
    assert_eq!(grant.5, 0);
    let record: GrantRecord = serde_json::from_str(&grant.6).unwrap();
    assert_eq!(record.execution_id.as_str(), row.2);
    assert_eq!(record.interrupt_id.as_str(), grant.1);
    assert_eq!(record.task_parking_revision, number(&grant.4));
    let journal: Vec<RawJournalRow> = connection.prepare("SELECT kind, execution_id, revision, owner_epoch, status, payload FROM journal WHERE execution_id=?1 ORDER BY sequence").unwrap()
        .query_map([row.2.as_str()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))).unwrap()
        .collect::<Result<_, _>>().unwrap();
    Some(Evidence {
        thread_generation: row.0,
        resource_revision: number(&row.1),
        resource_revision_raw: row.1,
        execution_id: row.2,
        snapshot,
        snapshot_raw: row.3,
        grant: record,
        grant_raw: grant.6.clone(),
        journal,
    })
}
fn poll_park(directory: &Path, thread: &str) -> Evidence {
    let start = Instant::now();
    for _ in 0..500 {
        assert!(
            start.elapsed() <= Duration::from_secs(10),
            "SQLite Park poll exceeded 10 seconds"
        );
        if let Some(value) = evidence(&readonly(directory), thread) {
            if value.snapshot.status == ExecutionStatus::WaitingForApproval {
                return value;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("committed park was not observed")
}
fn no_sensitive(bytes: &[u8]) {
    assert!(
        !bytes
            .windows(PROMPT.len())
            .any(|part| part == PROMPT.as_bytes()),
        "prompt disclosed"
    );
    assert!(
        !bytes.windows(5).any(|part| part == b"b4ag_"),
        "grant disclosed"
    );
}
fn request(address: SocketAddr, method: &str, path: &str, body: &str) -> Wire {
    Wire::open(address, method, path, body)
}
fn scenario(mode: &str) {
    let mut fixture = Fixture::start(mode);
    fixture.enforce_lifetime();
    let mut created = request(
        fixture.address,
        "POST",
        "/threads",
        &json!({"metadata": {"case":"t018"}}).to_string(),
    );
    assert_eq!(created.status, 200);
    let thread = created.json()["thread_id"].as_str().unwrap().to_owned();
    uuid::Uuid::parse_str(&thread).unwrap();
    let path = format!("/threads/{thread}/runs/stream");
    let body = json!({"route": ROUTE, "input": {"request":"t018-frozen-input"}}).to_string();
    let mut stream = request(fixture.address, "POST", &path, &body);
    assert_eq!(stream.status, 200, "run must be admitted as SSE");
    assert!(stream
        .headers
        .get("content-type")
        .is_some_and(|value| value.starts_with("text/event-stream")));
    if mode != "healthy" {
        stream.finish();
        let (events, _) = frames(&stream.decoded, false);
        assert!(!events
            .iter()
            .any(|(name, _)| name == "interrupt" || name == "done"));
        no_sensitive(&stream.raw);
        no_sensitive(&fixture.diagnostics());
        let connection = readonly(&fixture.directory);
        let (execution_id, running_revision, _) = poll_running_or_journal(&connection, &thread);
        assert_eq!(count(&connection, "approval_grants"), 0);
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM journal WHERE status='\"waiting_for_approval\"'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        let raw_snapshot: String = connection
            .query_row(
                "SELECT snapshot FROM executions WHERE id=?1",
                [execution_id],
                |r| r.get(0),
            )
            .unwrap();
        let snapshot: Snapshot = serde_json::from_str(&raw_snapshot).unwrap();
        assert_eq!(snapshot.status, ExecutionStatus::Running);
        assert_eq!(snapshot.revision, running_revision);
        assert!(snapshot
            .tasks
            .iter()
            .all(|task| task.status != TaskStatus::WaitingForApproval));
        assert_eq!(snapshot.definition.route.as_str(), ROUTE);
        assert_eq!(snapshot.definition.revision.as_str(), REVISION);
        assert_eq!(
            snapshot.values.get("input"),
            Some(&json!({"request":"t018-frozen-input"}))
        );
        let mut pending = request(
            fixture.address,
            "GET",
            &format!("/threads/{thread}/pending_interrupts"),
            "",
        );
        assert_eq!(pending.status, 503);
        let error = pending.json();
        assert_eq!(error["error"]["details"]["code"], "host_unready");
        no_sensitive(&pending.raw);
        no_sensitive(&fixture.diagnostics());
        fixture.enforce_lifetime();
        fixture.kill_reap();
        return;
    }
    let mut interrupt = None;
    let mut parsed = 0;
    while interrupt.is_none() {
        let (events, end) = frames(&stream.decoded, true);
        assert!(events.len() <= MAX_FRAMES);
        for (name, value) in events.into_iter().skip(parsed) {
            if name == "interrupt" {
                assert!(interrupt.replace(value).is_none());
            } else if name == "done" {
                assert!(interrupt.is_some(), "terminal preceded interrupt");
            }
        }
        parsed = frames(&stream.decoded[..end], false).0.len();
        if interrupt.is_none() {
            assert!(!stream.complete, "stream closed before interrupt");
            stream.read_more();
        }
    }
    let interrupt = interrupt.unwrap();
    assert_eq!(interrupt["value"], PROMPT);
    assert_eq!(interrupt.as_object().unwrap().len(), 4);
    let interrupt_id = interrupt["interruptId"].as_str().unwrap();
    let token = interrupt["grant"].as_str().unwrap();
    let resume_key = interrupt["resumeKey"].as_str().unwrap();
    assert!(token.starts_with("b4ag_") && token.len() > 20);
    assert!(!resume_key.is_empty());
    let before = poll_park(&fixture.directory, &thread);
    let (running_execution, running_revision, _) =
        poll_running_or_journal(&readonly(&fixture.directory), &thread);
    assert_eq!(running_execution, before.execution_id);
    assert_eq!(before.snapshot.revision, running_revision + 1);
    assert_eq!(before.snapshot.definition.route.as_str(), ROUTE);
    assert_eq!(before.snapshot.definition.revision.as_str(), REVISION);
    assert_eq!(
        before.snapshot.values.get("input"),
        Some(&json!({"request":"t018-frozen-input"}))
    );
    assert_eq!(before.grant.prompt, json!(PROMPT));
    assert_eq!(before.grant.token.as_str(), token);
    assert_eq!(before.grant.resume_key, resume_key);
    assert_eq!(before.grant.interrupt_id.as_str(), interrupt_id);
    assert_eq!(before.grant.task_id.as_str(), TASK);
    assert_eq!(before.grant.route.as_str(), ROUTE);
    assert_eq!(before.grant.definition_revision.as_str(), REVISION);
    assert_eq!(before.grant.policy_revision, "t018-policy-v1");
    assert_eq!(before.grant.thread_id.as_str(), thread);
    assert_eq!(
        before.grant.thread_generation.as_str(),
        before.thread_generation
    );
    assert_eq!(before.grant.task_parking_revision, before.snapshot.revision);
    assert_eq!(
        before.resource_revision, 3,
        "admit, Start and Park each increment resource revision"
    );
    assert_eq!(before.journal.last().unwrap().0, "commit");
    assert_eq!(before.journal.last().unwrap().4, "\"waiting_for_approval\"");
    assert_eq!(
        number(&before.journal.last().unwrap().2),
        before.snapshot.revision
    );
    let journal_entry: JournalEntry =
        serde_json::from_str(&before.journal.last().unwrap().5).unwrap();
    assert_eq!(journal_entry.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(journal_entry.revision, before.snapshot.revision);
    let mut pending = request(
        fixture.address,
        "GET",
        &format!("/threads/{thread}/pending_interrupts"),
        "",
    );
    assert_eq!(pending.status, 200);
    assert_eq!(
        pending.headers.get("cache-control").map(String::as_str),
        Some("no-store")
    );
    assert_eq!(pending.json()["interrupts"], json!([interrupt]));
    let mut state = request(
        fixture.address,
        "GET",
        &format!("/threads/{thread}/state"),
        "",
    );
    assert_eq!(state.status, 200);
    state.finish();
    no_sensitive(&state.raw);
    fixture.enforce_lifetime();
    fixture.hard_kill_reap();
    no_sensitive(&fixture.diagnostics());
    let reopened = SqliteStore::open(&fixture.directory).unwrap();
    assert_eq!(
        reopened.owner_generation(),
        before.snapshot.owner_generation + 1
    );
    let after = evidence(&readonly(&fixture.directory), &thread).unwrap();
    assert_eq!(
        after, before,
        "typed and raw committed park evidence changed after hard process exit"
    );
    let typed = reopened
        .thread_view(&ThreadId::new(thread).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(typed.execution.unwrap().snapshot, before.snapshot);
    assert_eq!(
        reopened
            .approval_view(&ExecutionId::new(before.execution_id.clone()).unwrap())
            .unwrap()
            .unwrap()
            .grants,
        vec![before.grant]
    );
}
fn poll_running_or_journal(connection: &Connection, thread: &str) -> (String, u64, u64) {
    let rows: Vec<(String, Vec<u8>, String)> = connection.prepare("SELECT execution_id, revision, status FROM journal WHERE thread_id=?1 AND kind IN ('admit','commit') ORDER BY sequence").unwrap()
        .query_map([thread], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().collect::<Result<_, _>>().unwrap();
    assert!(
        rows.iter().any(|row| row.2 == "\"accepted\""),
        "missing committed admission"
    );
    let running = rows
        .iter()
        .find(|row| row.2 == "\"running\"")
        .expect("missing committed Running journal");
    assert_eq!(rows.iter().filter(|row| row.2 == "\"running\"").count(), 1);
    (running.0.clone(), number(&running.1), rows.len() as u64)
}
#[test]
fn release_t018_approval_park_is_durable_before_tcp_disclosure() {
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        let directory = PathBuf::from(std::env::var(CHILD_DIRECTORY).unwrap());
        let nonce = std::env::var(CHILD_NONCE).unwrap();
        child(&mode, &directory, &nonce);
        return;
    }
    for mode in ["grant_abort", "journal_abort", "healthy"] {
        scenario(mode);
    }
}
