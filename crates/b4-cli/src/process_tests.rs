//! Private selflaunch qualification; none of these controls enters b4run.
use super::*;
use b4_contract::*;
use b4_http::{
    ApprovalHostConfig, HostLimits, Operation, Principal, ResourceContext, ResourcePolicy,
};
use b4_runtime::{
    ApplicationIdentity, EffectBinding, EffectInvocation, EffectUncertain, InvocationContext,
    NativeApplication, NativeEffect, NativeOutcome, OutputKind, RouteRegistration, Store,
    StoreError,
};
use b4_sqlite::SqliteStore;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const TOKEN: &str = "fixture-owner";
const CHILD_DIR: &str = "B4_PRIVATE_RUNNER_DIRECTORY";
const CHILD_SCENARIO: &str = "B4_PRIVATE_RUNNER_SCENARIO";
const HELPER: &str = "process::tests::runner_child_fixture";
const RUN: &str = r#"{"route":"/native#workflow","input":null}"#;

#[derive(Clone)]
pub(super) struct Observer {
    pub directory: PathBuf,
    pub scenario: String,
}
impl Observer {
    pub fn marker(&self, name: &str) {
        marker(&self.directory, name);
    }
    pub fn listening(&self, address: SocketAddr) {
        publish(&self.directory, "listening", &address.to_string());
    }
    pub fn incomplete(&self) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("incomplete"))
            .unwrap();
        writeln!(file, "interval").unwrap();
    }
    pub async fn probe(self, abort: tokio::task::AbortHandle) {
        let mut interval = tokio::time::interval(Duration::from_millis(3));
        let mut aborted = false;
        loop {
            interval.tick().await;
            for index in 1..=2 {
                if self.directory.join(format!("ping-{index}")).exists() {
                    self.marker(&format!("pong-{index}"));
                }
            }
            if self.scenario == "lost" && !aborted && self.directory.join("abort-server").exists() {
                aborted = true;
                abort.abort();
            }
        }
    }
}

pub(super) struct ObservedListener {
    inner: tokio::net::TcpListener,
    observer: Option<Observer>,
}
impl ObservedListener {
    pub(super) fn new(inner: tokio::net::TcpListener, observer: Option<Observer>) -> Self {
        Self { inner, observer }
    }
}
impl axum::serve::Listener for ObservedListener {
    type Io = ObservedIo;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (socket, address) = axum::serve::Listener::accept(&mut self.inner).await;
        let observer = self
            .observer
            .clone()
            .filter(|o| matches!(o.scenario.as_str(), "transport" | "lost" | "lost-panic"));
        if observer.is_some() {
            socket2::SockRef::from(&socket)
                .set_send_buffer_size(1024)
                .expect("private accepted socket buffer configuration failed");
        }
        (
            ObservedIo {
                socket,
                observer,
                bytes: 0,
                marked: false,
            },
            address,
        )
    }
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}
pub(super) struct ObservedIo {
    socket: tokio::net::TcpStream,
    observer: Option<Observer>,
    bytes: usize,
    marked: bool,
}
impl ObservedIo {
    fn observed(&mut self, result: &std::task::Poll<std::io::Result<usize>>) {
        match result {
            std::task::Poll::Ready(Ok(n)) => self.bytes += n,
            // Unread data can fit the kernel queue despite small buffer settings.
            // Record an actual pending socket write after positive progress;
            // every I/O result and wakeup still comes from the real TcpStream.
            std::task::Poll::Pending if self.bytes > 0 && !self.marked => {
                if let Some(observer) = self.observer.as_ref() {
                    observer.marker("write-pending");
                    self.marked = true;
                }
            }
            _ => {}
        }
    }
}
impl tokio::io::AsyncRead for ObservedIo {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for ObservedIo {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let result = std::pin::Pin::new(&mut self.socket).poll_write(cx, buf);
        self.observed(&result);
        result
    }
    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let result = std::pin::Pin::new(&mut self.socket).poll_write_vectored(cx, bufs);
        self.observed(&result);
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

struct OpaqueServerPanic(PathBuf);
impl Drop for OpaqueServerPanic {
    fn drop(&mut self) {
        marker(&self.0, "opaque-payload-dropped");
        panic!("private panic payload destructor");
    }
}

/// Panic the actual outer Axum task only after the parent has observed durable
/// callback completion on its unread real SSE connection. Connection tasks are
/// still Axum's detached transport tasks, not a fabricated completion future.
pub(super) async fn serve_with_panic_probe(
    serving: impl std::future::Future<Output = std::io::Result<()>>,
    observer: Option<Observer>,
) -> std::io::Result<()> {
    let Some(observer) = observer.filter(|observer| observer.scenario == "lost-panic") else {
        return serving.await;
    };
    tokio::pin!(serving);
    let panic_probe = async move {
        loop {
            if observer.directory.join("panic-server").exists() {
                observer.marker("outer-panic-issued");
                std::panic::panic_any(OpaqueServerPanic(observer.directory));
            }
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
    };
    tokio::select! {
        result = &mut serving => result,
        () = panic_probe => unreachable!("private server panic probe returned"),
    }
}
struct HeldPolicy {
    observer: Observer,
    inner: Arc<dyn ResourcePolicy>,
}
impl ResourcePolicy for HeldPolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::ReadCapabilities {
            hold(&self.observer.directory, "policy-held");
        }
        self.inner.authorize(principal, context)
    }
}
pub(super) fn policy(
    observer: Option<&Observer>,
    inner: Arc<dyn ResourcePolicy>,
) -> Arc<dyn ResourcePolicy> {
    match observer.filter(|o| o.scenario == "policy") {
        Some(observer) => Arc::new(HeldPolicy {
            observer: observer.clone(),
            inner,
        }),
        None => inner,
    }
}
fn marker(directory: &Path, name: &str) {
    File::create(directory.join(name))
        .unwrap()
        .sync_all()
        .unwrap();
}
fn publish(directory: &Path, name: &str, content: &str) {
    let pending = directory.join(format!("{name}.pending"));
    let mut file = File::create(&pending).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.sync_all().unwrap();
    std::fs::rename(pending, directory.join(name)).unwrap();
}
fn hold(directory: &Path, name: &str) {
    marker(directory, name);
    let deadline = Instant::now() + Duration::from_secs(20);
    while !directory.join("release").exists() {
        assert!(
            Instant::now() < deadline,
            "private callback release was not received"
        );
        std::thread::sleep(Duration::from_millis(3));
    }
}
struct Callback {
    observer: Observer,
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.observer.directory.join("calls"))
            .unwrap();
        writeln!(file, "invoke").unwrap();
        file.sync_all().unwrap();
        match self.observer.scenario.as_str() {
            "held" | "closed-output" => hold(&self.observer.directory, "callback-held"),
            "fault" => {
                marker(&self.observer.directory, "callback-fault");
                panic!("private opaque panic payload");
            }
            _ => {}
        }
        let item = if matches!(
            self.observer.scenario.as_str(),
            "transport" | "lost" | "lost-panic"
        ) {
            Value::String("x".repeat(60_000))
        } else {
            json!("settled")
        };
        self.observer.marker("callback-returning");
        if matches!(
            self.observer.scenario.as_str(),
            "transport" | "lost" | "lost-panic"
        ) {
            return Ok(NativeOutcome::Succeeded(TaskResult {
                output: item,
                writes: vec![],
            }));
        }
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("settled"),
            writes: vec![StateWrite::Append {
                field: "items".into(),
                items: vec![item],
            }],
        }))
    }
}
fn assembly(observer: &Observer) -> AppAssembly {
    if observer.scenario == "park" {
        return crate::demo::assemble().unwrap();
    }
    let task = TaskSpec {
        id: TaskId::new("work").unwrap(),
        effect_id: EffectId::new("work").unwrap(),
        requires_approval: false,
        recovery: if observer.scenario == "fault" {
            RecoveryPolicy::Unsafe
        } else {
            RecoveryPolicy::Pure
        },
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "native-demo".into(),
                revision: "fixture-v1".into(),
            },
            vec![RouteRegistration {
                definition: Definition {
                    route: RouteKey::new("/native#workflow").unwrap(),
                    revision: ExecutionRevision::new("fixture-v1").unwrap(),
                    max_attempts: 1,
                    batches: vec![vec![task.clone()]],
                },
                effects: BTreeMap::from([(
                    task.effect_id,
                    EffectBinding {
                        callback: Arc::new(Callback {
                            observer: observer.clone(),
                        }),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    AppAssembly {
        application,
        approval: ApprovalHostConfig::new("fixture-policy-v1".into(), BTreeMap::new()),
        limits: HostLimits::default(),
    }
}

/// A regular test returns immediately outside a child role; no ignored fixture
/// or environment control is compiled into the shipped executable.
#[test]
fn runner_child_fixture() {
    let Some(directory) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    crate::install_panic_hook();
    let observer = Observer {
        directory: PathBuf::from(directory),
        scenario: std::env::var(CHILD_SCENARIO).unwrap(),
    };
    let data = observer.directory.join("data");
    let command = crate::config::parse(
        [OsString::from("serve"), OsString::from("--demo")],
        |key| match key {
            "B4RUN_TOKEN" => Some(TOKEN.into()),
            "B4RUN_PORT" => Some("0".into()),
            "B4RUN_DATA_DIR" => Some(data.as_os_str().to_owned()),
            "B4RUN_SHUTDOWN_WAIT_MS" => Some("40".into()),
            _ => None,
        },
    );
    let result = match command {
        Ok(crate::config::Command::Serve(config)) => {
            let app = assembly(&observer);
            observer.marker("runner-invoked");
            serve_observed(config, app, observer.clone())
        }
        _ => Err(LauncherError::InvalidConfiguration),
    };
    if let Err(error) = result {
        publish(&observer.directory, "runner-error", error.code());
    }
    publish(
        &observer.directory,
        "runner-return",
        &result.err().map_or(0, LauncherError::exit_code).to_string(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while !observer.directory.join("child-exit").exists() {
        assert!(
            Instant::now() < deadline,
            "private child exit acknowledgement missing"
        );
        std::thread::sleep(Duration::from_millis(3));
    }
    // The parent has already acquired SQLite while this child was alive. Avoid
    // libtest attempting a summary write to deliberately closed fixture pipes.
    std::process::exit(0);
}

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "b4-process-{}-{unique}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn data(&self) -> PathBuf {
        self.0.join("data")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct OwnedChild {
    child: Child,
    directory: PathBuf,
}
impl OwnedChild {
    fn start(directory: &Directory, scenario: &str) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", HELPER, "--nocapture", "--test-threads=1"])
            .env(CHILD_DIR, &directory.0)
            .env(CHILD_SCENARIO, scenario)
            .stdin(Stdio::null())
            .stdout(if scenario == "closed-output" {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stderr(if scenario == "closed-output" {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .spawn()
            .unwrap();
        Self {
            child,
            directory: directory.0.clone(),
        }
    }
    fn alive(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "private runner child exited prematurely (opaque payload dropped: {})",
            self.directory.join("opaque-payload-dropped").exists()
        );
    }
    fn close_diagnostics(&mut self) {
        drop(self.child.stdout.take());
        drop(self.child.stderr.take());
    }
    fn reached(&mut self, name: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.directory.join(name).exists() {
            self.alive();
            assert!(
                !self.directory.join("runner-return").exists(),
                "runner returned before the required process witness ({})",
                std::fs::read_to_string(self.directory.join("runner-error"))
                    .unwrap_or_else(|_| "clean".into())
            );
            assert!(
                Instant::now() < deadline,
                "private process checkpoint missing"
            );
            std::thread::sleep(Duration::from_millis(3));
        }
    }
    fn address(&mut self) -> SocketAddr {
        self.reached("listening");
        std::fs::read_to_string(self.directory.join("listening"))
            .unwrap()
            .parse()
            .unwrap()
    }
    fn signal(&mut self, signal: &str) {
        self.alive();
        assert!(Command::new("/bin/kill")
            .args([signal, &self.child.id().to_string()])
            .status()
            .unwrap()
            .success());
    }
    fn incomplete_count(&self) -> usize {
        std::fs::read_to_string(self.directory.join("incomplete")).map_or(0, |s| s.lines().count())
    }
    fn intervals(&mut self, minimum: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.incomplete_count() < minimum {
            self.alive();
            assert!(
                !self.directory.join("runner-return").exists(),
                "incomplete ownership returned"
            );
            assert!(
                Instant::now() < deadline,
                "required incomplete intervals missing"
            );
            std::thread::sleep(Duration::from_millis(3));
        }
    }
    fn ping(&mut self, index: usize) {
        marker(&self.directory, &format!("ping-{index}"));
        self.reached(&format!("pong-{index}"));
    }
    fn return_code(&mut self) -> u8 {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.directory.join("runner-return").exists() {
            self.alive();
            assert!(
                Instant::now() < deadline,
                "runner did not return after positive retirement witnesses"
            );
            std::thread::sleep(Duration::from_millis(3));
        }
        self.alive();
        std::fs::read_to_string(self.directory.join("runner-return"))
            .unwrap()
            .parse()
            .unwrap()
    }
    fn finish(&mut self) {
        marker(&self.directory, "child-exit");
        assert!(self.child.wait().unwrap().success());
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.try_wait().is_ok_and(|s| s.is_none()) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}
fn open(address: SocketAddr, method: &str, path: &str, body: &str) -> TcpStream {
    let mut socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write_request(&mut socket, address, method, path, body);
    socket
}
fn write_request(
    socket: &mut TcpStream,
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) {
    write!(socket, "{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
}
fn finish_response(mut socket: TcpStream) -> Vec<u8> {
    let mut bytes = vec![];
    socket
        .read_to_end(&mut bytes)
        .expect("private response did not finish");
    bytes
}
fn exchange(address: SocketAddr, method: &str, path: &str, body: &str) -> Vec<u8> {
    finish_response(open(address, method, path, body))
}
fn locked(directory: &Directory) {
    assert!(
        matches!(
            SqliteStore::open(directory.data()),
            Err(StoreError::AlreadyOwned)
        ),
        "runner must retain actual SQLite ownership"
    );
}
fn snapshot(directory: &Directory) -> Snapshot {
    let bytes: String = Connection::open(directory.data().join("runtime.sqlite3"))
        .unwrap()
        .query_row(
            "SELECT snapshot FROM executions WHERE thread_id='held'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    serde_json::from_str(&bytes).unwrap()
}
fn settled(child: &mut OwnedChild, directory: &Directory) {
    child.reached("callback-returning");
    let deadline = Instant::now() + Duration::from_secs(5);
    while snapshot(directory).tasks[0].status != TaskStatus::Succeeded {
        child.alive();
        assert!(
            Instant::now() < deadline,
            "actual callback durable settlement missing"
        );
        std::thread::sleep(Duration::from_millis(3));
    }
}
fn clean_return(child: &mut OwnedChild, directory: &Directory, code: u8) -> SqliteStore {
    assert_eq!(child.return_code(), code);
    let store = SqliteStore::open(directory.data())
        .expect("SQLite must reopen while the runner child is still alive");
    child.alive();
    store
}

fn held_shutdown(signal: &str, scenario: &str) {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, scenario);
    child.reached("runner-invoked");
    let address = child.address();
    let request = open(address, "POST", "/threads/held/runs/wait", RUN);
    child.reached("callback-held");
    if scenario == "closed-output" {
        child.close_diagnostics();
    }
    child.signal(signal);
    child.intervals(3);
    locked(&directory);
    child.ping(1);
    child.signal("-INT");
    child.signal("-TERM");
    child.intervals(5);
    locked(&directory);
    child.ping(2);
    marker(&directory.0, "release");
    let response = finish_response(request);
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "held work must settle normally"
    );
    let store = clean_return(&mut child, &directory, 0);
    let view = store
        .thread_view(&ThreadId::new("held").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        view.execution.unwrap().snapshot.values["items"],
        json!(["settled"])
    );
    child.finish();
}
#[test]
fn sigint_retains_held_work_executor_and_lock_until_real_return() {
    held_shutdown("-INT", "held");
}
#[test]
fn sigterm_retains_held_work_executor_and_lock_until_real_return() {
    held_shutdown("-TERM", "held");
}

#[test]
fn closed_diagnostic_pipes_cannot_bypass_owned_process_supervision() {
    held_shutdown("-TERM", "closed-output");
}

#[test]
fn faulted_quiescence_preserves_unsafe_inflight_without_replay() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "fault");
    let address = child.address();
    let response = exchange(address, "POST", "/threads/held/runs/wait", RUN);
    assert!(response.starts_with(b"HTTP/1.1 503"));
    child.reached("callback-fault");
    let before = snapshot(&directory);
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    child.signal("-TERM");
    let _store = clean_return(&mut child, &directory, 1);
    assert_eq!(snapshot(&directory), before);
    assert_eq!(
        std::fs::read_to_string(directory.0.join("calls"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    child.finish();
}

fn unread_stream(address: SocketAddr) -> TcpStream {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    socket.set_recv_buffer_size(1024).unwrap();
    eprintln!(
        "unread_stream recv_buffer={}",
        socket.recv_buffer_size().unwrap()
    );
    socket.connect(&address.into()).unwrap();
    let mut stream: TcpStream = socket.into();
    // A single bounded response can fit Linux's kernel queue. Pipeline a finite
    // set of real native SSE requests and await observed write backpressure.
    // Shutdown can cut admission before all 32 requests are admitted.
    for _ in 0..32 {
        write!(stream, "POST /threads/held/runs/stream HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{RUN}", RUN.len()).unwrap();
    }
    stream
}
#[test]
fn quiescent_unread_sse_retains_transport_and_rate_limits_waits_until_peer_close() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "transport");
    let address = child.address();
    let peer = unread_stream(address);
    child.reached("write-pending");
    settled(&mut child, &directory);
    child.signal("-INT");
    child.reached("quiescent");
    child.reached("pending-server-at-quiescence");
    child.intervals(3);
    assert!(!directory.0.join("server-success").exists());
    assert!(!directory.0.join("server-failure").exists());
    locked(&directory);
    child.ping(1);
    let before = child.incomplete_count();
    let started = Instant::now();
    std::thread::sleep(Duration::from_millis(240));
    child.alive();
    let count = child.incomplete_count() - before;
    assert!(
        count >= 1 && count <= started.elapsed().as_millis() as usize / 40 + 3,
        "transport-only waits must use a bounded independent timer (observed {count})"
    );
    assert!(
        !directory.0.join("runner-return").exists(),
        "server_success={} server_failure={} pending_server={} write_pending={}",
        directory.0.join("server-success").exists(),
        directory.0.join("server-failure").exists(),
        directory.0.join("pending-server-at-quiescence").exists(),
        directory.0.join("write-pending").exists(),
    );
    drop(peer);
    let _store = clean_return(&mut child, &directory, 0);
    child.finish();
}
#[test]
fn lost_actual_outer_server_witness_retains_executor_and_owner_after_quiescence() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "lost");
    let address = child.address();
    let peer = unread_stream(address);
    child.reached("write-pending");
    settled(&mut child, &directory);
    marker(&directory.0, "abort-server");
    child.reached("server-failure");
    child.reached("quiescent");
    child.intervals(3);
    locked(&directory);
    child.ping(1);
    drop(peer);
    child.signal("-TERM");
    child.intervals(5);
    child.ping(2);
    locked(&directory);
    assert!(
        !directory.0.join("runner-return").exists(),
        "lost transport witness must retain indefinitely"
    );
    // Drop kills and reaps the fixture; this is cleanup, never retirement proof.
}

#[test]
fn opaque_outer_server_panic_is_retained_without_payload_drop_or_owner_release() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "lost-panic");
    let address = child.address();
    let peer = unread_stream(address);
    child.reached("write-pending");
    settled(&mut child, &directory);
    marker(&directory.0, "panic-server");
    child.reached("outer-panic-issued");
    child.reached("server-failure");
    child.reached("quiescent");
    child.intervals(3);
    assert!(
        !directory.0.join("opaque-payload-dropped").exists(),
        "the failed outer task's opaque payload must remain owned without disposal"
    );
    child.ping(1);
    locked(&directory);
    drop(peer);
    child.signal("-TERM");
    child.intervals(5);
    child.ping(2);
    locked(&directory);
    assert!(!directory.0.join("opaque-payload-dropped").exists());
    assert!(!directory.0.join("runner-return").exists());
    // SIGKILL on Drop is test cleanup only, not transport retirement evidence.
}

#[test]
fn held_policy_without_run_permit_keeps_executor_and_owner_until_return() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "policy");
    let address = child.address();
    let request = open(address, "GET", "/b4/capabilities", "");
    child.reached("policy-held");
    child.signal("-TERM");
    child.intervals(3);
    child.ping(1);
    locked(&directory);
    marker(&directory.0, "release");
    let response = finish_response(request);
    assert!(
        response.starts_with(b"HTTP/1.1 503"),
        "held metadata must observe the admission cut after policy returns"
    );
    let _store = clean_return(&mut child, &directory, 0);
    child.finish();
}

fn authority_records(directory: &Directory) -> (Vec<String>, Vec<String>) {
    let connection = Connection::open(directory.data().join("runtime.sqlite3")).unwrap();
    let records = |sql| {
        connection
            .prepare(sql)
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };
    (
        records("SELECT record FROM approval_grants ORDER BY record"),
        records("SELECT record FROM approval_acceptances ORDER BY record"),
    )
}
#[test]
fn clean_shutdown_preserves_parked_prompt_token_expiry_and_acceptance_records() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "park");
    let address = child.address();
    let response = exchange(
        address,
        "POST",
        "/threads/held/runs/wait",
        r#"{"route":"/approve#workflow","input":null}"#,
    );
    assert!(response.starts_with(b"HTTP/1.1 200"));
    let before = authority_records(&directory);
    assert_eq!(before.0.len(), 1);
    assert!(before.1.is_empty());
    let before_snapshot = snapshot(&directory);
    locked(&directory);
    child.signal("-INT");
    let _store = clean_return(&mut child, &directory, 0);
    assert!(
        authority_records(&directory) == before,
        "exact parked authority must survive clean shutdown"
    );
    assert!(
        snapshot(&directory) == before_snapshot,
        "parked execution must not be rewritten during drain"
    );
    child.finish();
}
