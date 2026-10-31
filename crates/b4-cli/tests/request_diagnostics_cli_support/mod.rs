use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
pub const TOKEN: &str = "request-diagnostics-process-secret";

pub struct Sandbox {
    pub path: PathBuf,
}
impl Sandbox {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-request-diagnostics-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .expect("isolated diagnostics fixture parent must be created");
        Self { path }
    }
}
impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn capture(path: &PathBuf) -> File {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .expect("private child capture must be created")
}

pub struct Process {
    child: Option<Child>,
    stdout: PathBuf,
    stderr: PathBuf,
    pub address: SocketAddr,
}
impl Process {
    pub fn start(sandbox: &Sandbox) -> Self {
        let number = NEXT.fetch_add(1, Ordering::Relaxed);
        let stdout = sandbox.path.join(format!("stdout-{number}"));
        let stderr = sandbox.path.join(format!("stderr-{number}"));
        let binary = std::env::var_os("B4RUN_QUALIFICATION_BINARY")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_b4run").into());
        let child = Command::new(binary)
            .args(["serve", "--demo"])
            .env_clear()
            .env("B4RUN_TOKEN", TOKEN)
            .env("B4RUN_PORT", "0")
            .env("B4RUN_DATA_DIR", sandbox.path.join("state"))
            .env("B4RUN_SHUTDOWN_WAIT_MS", "2000")
            .stdin(Stdio::null())
            .stdout(Stdio::from(capture(&stdout)))
            .stderr(Stdio::from(capture(&stderr)))
            .spawn()
            .expect("compiled native executable must start");
        let mut process = Self {
            child: Some(child),
            stdout,
            stderr,
            address: "127.0.0.1:0".parse().unwrap(),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let bytes = std::fs::read(&process.stdout).unwrap();
            let text = std::str::from_utf8(&bytes).expect("launcher output must be public UTF8");
            if let Some(address) = text.lines().find_map(|line| {
                line.strip_prefix("b4run_listening http://")
                    .and_then(|value| value.parse::<SocketAddr>().ok())
            }) {
                assert!(address.ip().is_loopback() && address.port() != 0);
                process.address = address;
                return process;
            }
            assert!(
                process
                    .child
                    .as_mut()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "native process must remain alive until listener announcement"
            );
            assert!(
                Instant::now() < deadline,
                "native listener start must be bounded"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn stop(mut self) {
        let pid = self.child.as_ref().unwrap().id().to_string();
        assert!(
            Command::new("/bin/kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success(),
            "actual native process shutdown signal must be delivered"
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "normal native exit must be observed"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        self.child.take();
        assert!(
            status.success(),
            "native diagnostics qualification must retire cleanly"
        );
        let stdout = std::fs::read(&self.stdout).unwrap();
        assert!(stdout.ends_with(b"b4run_stopped\n"));
        assert!(std::fs::read(&self.stderr).unwrap().is_empty());
        for line in std::str::from_utf8(&stdout).unwrap().lines() {
            assert!(
                line.starts_with("b4run_listening http://127.0.0.1:")
                    || ["b4run_draining", "b4run_drain_incomplete", "b4run_stopped"]
                        .contains(&line),
                "native output may contain only fixed lifecycle codes and public listener"
            );
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Failure cleanup is never counted as positive shutdown evidence.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
impl Response {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("actual response must contain bounded JSON")
    }
    pub fn request_id(&self) -> String {
        let value = self.headers.get("x-b4-request-id");
        assert!(
            value.is_some(),
            "actual native response must carry the server request reference"
        );
        let value = value.unwrap();
        let bytes = value.as_bytes();
        assert!(
            bytes.len() == 53
                && &bytes[..4] == b"b4r_"
                && bytes[36] == b'_'
                && bytes[4..36]
                    .iter()
                    .chain(bytes[37..].iter())
                    .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(byte) })
                && &bytes[37..] != b"0000000000000000",
            "native reference must have the fixed server namespace and nonzero sequence"
        );
        value.to_owned()
    }
}

fn decode_chunks(mut input: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    loop {
        let end = input.windows(2).position(|bytes| bytes == b"\r\n").unwrap();
        let size = usize::from_str_radix(
            std::str::from_utf8(&input[..end])
                .unwrap()
                .split(';')
                .next()
                .unwrap(),
            16,
        )
        .unwrap();
        input = &input[end + 2..];
        if size == 0 {
            return output;
        }
        assert!(
            size <= input.len().saturating_sub(2),
            "actual HTTP chunk must be complete"
        );
        output.extend_from_slice(&input[..size]);
        assert!(&input[size..size + 2] == b"\r\n");
        input = &input[size + 2..];
    }
}

pub fn exchange(
    address: SocketAddr,
    method: &str,
    path: &str,
    authenticated: bool,
    body: &str,
) -> Response {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    let authorization = if authenticated {
        format!("Authorization: Bearer {TOKEN}\r\n")
    } else {
        String::new()
    };
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n{authorization}x-b4-request-id: caller-private-reference\r\nCookie: private-cookie\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).unwrap();
    let mut bytes = Vec::new();
    stream.take(1_048_577).read_to_end(&mut bytes).unwrap();
    assert!(
        bytes.len() <= 1_048_576,
        "finite native response must remain bounded"
    );
    let split = bytes
        .windows(4)
        .position(|value| value == b"\r\n\r\n")
        .unwrap();
    let head = std::str::from_utf8(&bytes[..split]).unwrap();
    let status = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = BTreeMap::new();
    for line in head.lines().skip(1) {
        let (key, value) = line.split_once(':').unwrap();
        let key = key.to_ascii_lowercase();
        assert!(
            headers.insert(key, value.trim().to_owned()).is_none(),
            "actual response must not duplicate reference headers"
        );
    }
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunks(&bytes[split + 4..])
    } else {
        bytes[split + 4..].to_vec()
    };
    Response {
        status,
        headers,
        body,
    }
}
