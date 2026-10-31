use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rusqlite::{types::Value as SqlValue, Connection, OpenFlags};
use serde_json::Value;

static NEXT_SANDBOX: AtomicU64 = AtomicU64::new(0);

pub const TOKEN: &str = "launcher-qualification-secret";

pub struct Sandbox {
    pub path: PathBuf,
}

impl Sandbox {
    pub fn new() -> Self {
        let id = NEXT_SANDBOX.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("b4-cli-qualification-{}-{id}", std::process::id()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .expect("an isolated temporary parent must be created");
        Self { path }
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub fn command(data: &Path) -> Command {
    // This selects the executable only in the qualification parent; the child
    // environment is cleared and production has no test configuration hook.
    let binary = std::env::var_os("B4RUN_QUALIFICATION_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_b4run").into());
    let mut command = Command::new(binary);
    command.env_clear();
    command.env("B4RUN_TOKEN", TOKEN);
    command.env("B4RUN_PORT", "0");
    command.env("B4RUN_DATA_DIR", data);
    command.env("B4RUN_SHUTDOWN_WAIT_MS", "20");
    command
}

pub struct Captured {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub struct Process {
    child: Option<Child>,
    stdout: PathBuf,
    stderr: PathBuf,
}

fn private_file(path: &Path) -> File {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .expect("private process output must be captured")
}

impl Process {
    pub fn spawn(command: &mut Command, sandbox: &Sandbox) -> Self {
        let id = NEXT_SANDBOX.fetch_add(1, Ordering::Relaxed);
        let stdout = sandbox.path.join(format!("stdout-{id}"));
        let stderr = sandbox.path.join(format!("stderr-{id}"));
        command.stdout(Stdio::from(private_file(&stdout)));
        command.stderr(Stdio::from(private_file(&stderr)));
        command.stdin(Stdio::null());
        let child = command.spawn().expect("the compiled launcher must execute");
        Self {
            child: Some(child),
            stdout,
            stderr,
        }
    }

    pub fn wait(&mut self) -> Captured {
        let deadline = Instant::now() + Duration::from_secs(12);
        let status = loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("child must remain owned")
                .try_wait()
                .expect("child status must be observable")
            {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "launcher exit must be positively observed within test bound"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        self.child.take();
        Captured {
            status,
            stdout: self.stdout(),
            stderr: self.stderr(),
        }
    }

    pub fn stdout(&self) -> Vec<u8> {
        std::fs::read(&self.stdout).expect("captured public stdout must be readable")
    }

    pub fn stderr(&self) -> Vec<u8> {
        std::fs::read(&self.stderr).expect("captured public stderr must be readable")
    }

    pub fn signal(&mut self, signal: &str) {
        let id = self
            .child
            .as_ref()
            .expect("child must remain owned")
            .id()
            .to_string();
        let status = Command::new("/bin/kill")
            .args([signal, &id])
            .status()
            .expect("the operating system signal utility must execute");
        assert!(status.success(), "actual OS signal delivery must succeed");
    }

    pub fn listening(&mut self) -> SocketAddr {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            let stdout = self.stdout();
            let text = std::str::from_utf8(&stdout).expect("launcher output must be public UTF8");
            for line in text.lines() {
                if let Some(address) = line.strip_prefix("b4run_listening ") {
                    return address
                        .strip_prefix("http://")
                        .expect("public listener must use the fixed local HTTP scheme")
                        .parse()
                        .expect("public listener address must be valid");
                }
            }
            assert!(
                self.child
                    .as_mut()
                    .expect("child must remain owned")
                    .try_wait()
                    .expect("child status must be observable")
                    .is_none(),
                "launcher must stay alive until listener announcement"
            );
            assert!(
                Instant::now() < deadline,
                "launcher must announce actual loopback readiness"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // SIGKILL is failure cleanup only. No test infers a shutdown witness
            // or lock release from this path.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub fn run(command: &mut Command, sandbox: &Sandbox) -> Captured {
    Process::spawn(command, sandbox).wait()
}

pub fn serve(data: &Path, sandbox: &Sandbox) -> (Process, SocketAddr) {
    let mut process = Process::spawn(command(data).args(["serve", "--demo"]), sandbox);
    let address = process.listening();
    assert!(
        address.ip().is_loopback() && address.is_ipv4(),
        "serve must bind IPv4 loopback"
    );
    assert!(
        address.port() != 0,
        "serve must announce its actual assigned port"
    );
    (process, address)
}

pub fn command_under_restrictive_umask(data: &Path) -> Command {
    let launcher = command(data);
    let mut shell = Command::new("/bin/sh");
    shell.args([
        "-c",
        "umask 0777; exec \"$1\" serve --demo",
        "b4-qualification",
    ]);
    shell.arg(launcher.get_program());
    shell.env_clear();
    for (key, value) in launcher.get_envs() {
        if let Some(value) = value {
            shell.env(key, value);
        }
    }
    shell
}

pub fn clean_stop(process: &mut Process, signal: &str) -> Captured {
    process.signal(signal);
    let output = process.wait();
    assert!(
        output.status.success(),
        "signal shutdown must positively report clean process exit"
    );
    assert!(
        output
            .stdout
            .windows(b"b4run_draining\n".len())
            .any(|w| w == b"b4run_draining\n"),
        "signal shutdown must announce admission drain"
    );
    assert!(
        output.stdout.ends_with(b"b4run_stopped\n"),
        "clean exit must follow normal owned runtime destruction"
    );
    assert!(
        output.stderr.is_empty(),
        "clean exit must have no private or error diagnostic"
    );
    let text = std::str::from_utf8(&output.stdout)
        .expect("all clean process diagnostics must be fixed public UTF8");
    for line in text.lines() {
        let public_listener = line
            .strip_prefix("b4run_listening http://")
            .and_then(|address| address.parse::<SocketAddr>().ok())
            .is_some_and(|address| {
                address.is_ipv4() && address.ip().is_loopback() && address.port() != 0
            });
        assert!(public_listener || ["b4run_draining", "b4run_drain_incomplete", "b4run_stopped"].contains(&line), "clean process diagnostics may contain only public address and exact fixed lifecycle codes");
    }
    output
}

pub struct Response {
    pub status: u16,
    pub headers: String,
    pub body: Vec<u8>,
}

impl Response {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("bounded public response must be JSON")
    }

    pub fn frames(&self) -> Vec<(String, Value)> {
        std::str::from_utf8(&self.body)
            .expect("finite SSE output must be public UTF8")
            .replace("\r\n", "\n")
            .split("\n\n")
            .filter_map(|block| {
                let name = block
                    .lines()
                    .find_map(|line| line.strip_prefix("event: "))?;
                let payload = block
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .collect::<Vec<_>>()
                    .join("\n");
                Some((
                    name.to_owned(),
                    serde_json::from_str(&payload)
                        .expect("finite SSE frame must contain valid JSON"),
                ))
            })
            .collect()
    }
}

fn decode_chunks(mut rest: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    loop {
        let end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("actual chunk must contain size boundary");
        let size_text =
            std::str::from_utf8(&rest[..end]).expect("actual chunk size must be public ASCII");
        let size = usize::from_str_radix(
            size_text
                .split(';')
                .next()
                .expect("actual chunk must have size"),
            16,
        )
        .expect("actual chunk size must be valid hex");
        rest = &rest[end + 2..];
        if size == 0 {
            break;
        }
        assert!(
            size <= rest.len().saturating_sub(2),
            "actual chunk payload must be complete"
        );
        assert!(
            &rest[size..size + 2] == b"\r\n",
            "actual chunk must have final boundary"
        );
        payload.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
    payload
}

pub fn raw_exchange(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &str,
    body: &str,
) -> Response {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
        .expect("actual loopback listener must accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .expect("test reads must have a bound");
    stream
        .set_write_timeout(Some(Duration::from_secs(8)))
        .expect("test writes must have a bound");
    stream.write_all(format!("{method} {path} HTTP/1.1\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes())
        .expect("actual HTTP request must be written");
    let mut bytes = Vec::new();
    stream
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .expect("actual HTTP response must finish");
    assert!(
        bytes.len() <= 1_048_576,
        "public response must stay bounded"
    );
    let split = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("HTTP response must include headers");
    let response_headers = std::str::from_utf8(&bytes[..split])
        .expect("HTTP headers must be public UTF8")
        .to_ascii_lowercase();
    let status = response_headers
        .split_whitespace()
        .nth(1)
        .expect("HTTP response must have status")
        .parse()
        .expect("HTTP status must be numeric");
    let body = if response_headers.contains("transfer-encoding: chunked") {
        decode_chunks(&bytes[split + 4..])
    } else {
        bytes[split + 4..].to_vec()
    };
    Response {
        status,
        headers: response_headers,
        body,
    }
}

pub fn exchange(address: SocketAddr, method: &str, path: &str, body: &str) -> Response {
    raw_exchange(
        address,
        method,
        path,
        &format!("Host: {address}\r\nAuthorization: Bearer {TOKEN}\r\n"),
        body,
    )
}

pub fn private_modes(data: &Path) {
    let directory = std::fs::symlink_metadata(data).expect("private data leaf must exist");
    assert!(
        directory.is_dir() && directory.permissions().mode() & 0o7777 == 0o700,
        "data directory must be an actual private 0700 directory"
    );
    for name in [
        "runtime.sqlite3",
        "owner.lock",
        "runtime.sqlite3-wal",
        "runtime.sqlite3-shm",
        "runtime.sqlite3-journal",
    ] {
        let path = data.join(name);
        if path.exists() {
            let metadata =
                std::fs::symlink_metadata(path).expect("storage metadata must be readable");
            assert!(
                metadata.is_file() && metadata.permissions().mode() & 0o7777 == 0o600,
                "database, owner file and SQLite sidecars must be actual private 0600 files"
            );
        }
    }
    assert!(
        data.join("runtime.sqlite3").is_file() && data.join("owner.lock").is_file(),
        "both private storage files must exist"
    );
}

pub fn sql_rows(data: &Path, table: &str) -> Vec<Vec<SqlValue>> {
    assert!(
        [
            "metadata",
            "threads",
            "executions",
            "journal",
            "approval_grants",
            "approval_acceptances"
        ]
        .contains(&table),
        "only fixed qualification tables may be read"
    );
    let connection = Connection::open_with_flags(
        data.join("runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("actual SQLite runtime state must be readable without owner acquisition");
    let mut statement = connection
        .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
        .expect("fixed runtime table must be queryable");
    let count = statement.column_count();
    statement
        .query_map([], |row| (0..count).map(|column| row.get(column)).collect())
        .expect("raw SQLite records must be readable")
        .collect::<Result<_, _>>()
        .expect("raw SQLite records must decode")
}

pub fn all_rows(data: &Path) -> Vec<Vec<Vec<SqlValue>>> {
    [
        "metadata",
        "threads",
        "executions",
        "journal",
        "approval_grants",
        "approval_acceptances",
    ]
    .iter()
    .map(|table| sql_rows(data, table))
    .collect()
}

pub fn file_bytes(data: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(data)
        .expect("actual storage directory must be readable")
        .map(|entry| {
            let entry = entry.expect("actual storage entry must be readable");
            let name = entry
                .file_name()
                .into_string()
                .expect("launcher storage names must be UTF8");
            let bytes = std::fs::read(entry.path()).expect("actual storage bytes must be readable");
            (name, bytes)
        })
        .collect()
}

pub fn snapshot(data: &Path, thread: &str) -> Value {
    let connection = Connection::open_with_flags(
        data.join("runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("actual SQLite execution must be readable");
    let text: String = connection
        .query_row(
            "SELECT snapshot FROM executions WHERE thread_id = ?1",
            [thread],
            |row| row.get(0),
        )
        .expect("actual admitted execution must be stored");
    serde_json::from_str(&text).expect("actual snapshot must be valid JSON")
}
