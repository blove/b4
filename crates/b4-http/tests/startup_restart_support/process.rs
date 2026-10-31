use super::{
    evidence::{encode, verify_directory, Writer, LOG_BYTES},
    model::*,
    CHILD_LANE, CHILD_ROLE, CHILD_ROOT, CHILD_SELECTOR, WAIT,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs,
    io::{Read, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream as StdStream,
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command as ChildCommand, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};
const REQUEST_BYTES: usize = 1024;
const REPLY_BYTES: usize = 2048;

pub struct Endpoint {
    path: PathBuf,
    device: u64,
    inode: u64,
    listener: UnixListener,
}
impl Endpoint {
    pub fn bind(root: &Path) -> Result<Self> {
        verify_directory(root)?;
        let path = root.join("control.sock");
        if path.as_os_str().as_bytes().len() >= 104 {
            return Err(Failure::Bound);
        }
        if path.exists() || fs::symlink_metadata(&path).is_ok() {
            return Err(Failure::Setup);
        }
        let listener = UnixListener::bind(&path).map_err(|_| Failure::Io)?;
        // This is our create-new socket only; foreign Store artifacts are never chmodded.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|_| Failure::Permission)?;
        let m = fs::symlink_metadata(&path).map_err(|_| Failure::Io)?;
        if !m.file_type().is_socket() || m.permissions().mode() & 0o777 != 0o600 {
            return Err(Failure::Permission);
        }
        Ok(Self {
            path,
            device: m.dev(),
            inode: m.ino(),
            listener,
        })
    }
    pub async fn accept(
        self,
        child: &OwnedChild,
        lane: Lane,
        role: Role,
        writer: Writer,
    ) -> Result<Control> {
        let (stream, _) = tokio::time::timeout(WAIT, self.listener.accept())
            .await
            .map_err(|_| Failure::Timeout)?
            .map_err(|_| Failure::Io)?;
        let mut control = Control {
            stream,
            ordinal: 0,
            writer,
            prefix: role.text(),
            hello: None,
        };
        let hello: Hello = read_frame(&mut control.stream, REPLY_BYTES).await?;
        if hello.version != 1
            || hello.ordinal != 0
            || hello.pid != child.pid()
            || hello.lane != lane
            || hello.role != role
            || hello.owner == 0
            || hello.port == 0
        {
            return Err(Failure::Protocol);
        }
        control
            .writer
            .persist(&format!("{}-hello.json", role.text()), &hello, REPLY_BYTES)?;
        control.hello = Some(hello);
        // The connection stays alive; removing this exact socket frees its name
        // for the next owned child after signal/reap and parent Store retirement.
        self.remove_owned()?;
        Ok(control)
    }
    fn remove_owned(&self) -> Result<()> {
        let m = fs::symlink_metadata(&self.path).map_err(|_| Failure::Io)?;
        if m.dev() != self.device || m.ino() != self.inode || !m.file_type().is_socket() {
            return Err(Failure::Permission);
        }
        fs::remove_file(&self.path).map_err(|_| Failure::Io)
    }
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        if let Ok(m) = fs::symlink_metadata(&self.path) {
            if m.dev() == self.device && m.ino() == self.inode && m.file_type().is_socket() {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}
pub struct Control {
    stream: UnixStream,
    ordinal: u8,
    writer: Writer,
    prefix: &'static str,
    pub hello: Option<Hello>,
}
impl Control {
    pub fn hello(&self) -> Result<&Hello> {
        self.hello.as_ref().ok_or(Failure::Protocol)
    }
    pub async fn command(&mut self, command: Command) -> Result<Reply> {
        tokio::time::timeout(WAIT, async {
            self.ordinal = self
                .ordinal
                .checked_add(1)
                .filter(|n| *n <= 16)
                .ok_or(Failure::Bound)?;
            let request = Request {
                version: 1,
                ordinal: self.ordinal,
                command,
            };
            self.writer.persist(
                &format!("{}-control-{:02}-request.json", self.prefix, self.ordinal),
                &request,
                REQUEST_BYTES,
            )?;
            write_frame(&mut self.stream, &request, REQUEST_BYTES).await?;
            let reply: Reply = read_frame(&mut self.stream, REPLY_BYTES).await?;
            if reply.version != 1 || reply.ordinal != self.ordinal {
                return Err(Failure::Protocol);
            }
            self.writer.persist(
                &format!("{}-control-{:02}-reply.json", self.prefix, self.ordinal),
                &reply,
                REPLY_BYTES,
            )?;
            Ok(reply)
        })
        .await
        .map_err(|_| Failure::Timeout)?
    }
}
async fn read_frame<T: DeserializeOwned>(stream: &mut UnixStream, cap: usize) -> Result<T> {
    tokio::time::timeout(WAIT, async {
        let mut bytes = Vec::new();
        loop {
            let b = stream.read_u8().await.map_err(|_| Failure::Io)?;
            if b == b'\n' {
                break;
            }
            if bytes.len() == cap || b == b'\r' {
                return Err(Failure::Bound);
            }
            bytes.push(b);
        }
        serde_json::from_slice(&bytes).map_err(|_| Failure::Protocol)
    })
    .await
    .map_err(|_| Failure::Timeout)?
}
async fn write_frame<T: Serialize>(stream: &mut UnixStream, value: &T, cap: usize) -> Result<()> {
    let bytes = encode(value, cap)?;
    tokio::time::timeout(WAIT, async {
        stream.write_all(&bytes).await.map_err(|_| Failure::Io)?;
        stream.write_all(b"\n").await.map_err(|_| Failure::Io)
    })
    .await
    .map_err(|_| Failure::Timeout)?
}

pub struct OwnedChild {
    child: Child,
    reaped: bool,
    logs: Vec<JoinHandle<Result<()>>>,
    loss: Arc<AtomicBool>,
}
impl OwnedChild {
    pub fn start(root: &Path, lane: Lane, role: Role, writer: Writer) -> Result<Self> {
        let exe = std::env::current_exe().map_err(|_| Failure::Setup)?;
        // No foreign PID/process group and no Node/client/build/provider launch.
        let child = ChildCommand::new(exe)
            .args(["--exact", CHILD_SELECTOR, "--ignored", "--test-threads=1"])
            .env(CHILD_ROOT, root)
            .env(CHILD_LANE, lane.text())
            .env(CHILD_ROLE, role.text())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| Failure::Child)?;
        let loss = Arc::new(AtomicBool::new(false));
        let mut owned = Self {
            child,
            reaped: false,
            logs: Vec::new(),
            loss,
        };
        let stdout = owned.child.stdout.take().ok_or(Failure::Child)?;
        let stderr = owned.child.stderr.take().ok_or(Failure::Child)?;
        owned.logs.push(drain(
            stdout,
            writer.clone(),
            format!("{}-stdout.log", role.text()),
            owned.loss.clone(),
        )?);
        owned.logs.push(drain(
            stderr,
            writer,
            format!("{}-stderr.log", role.text()),
            owned.loss.clone(),
        )?);
        Ok(owned)
    }
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
    async fn wait_exit(&mut self) -> Result<std::process::ExitStatus> {
        tokio::time::timeout(WAIT, async {
            loop {
                if let Some(status) = self.child.try_wait().map_err(|_| Failure::Child)? {
                    self.reaped = true;
                    return Ok(status);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .map_err(|_| Failure::Timeout)?
    }
    async fn join_logs(&mut self) -> Result<()> {
        tokio::time::timeout(WAIT, async {
            while self.logs.iter().any(|t| !t.is_finished()) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .map_err(|_| Failure::Timeout)?;
        for task in self.logs.drain(..) {
            task.join().map_err(|_| Failure::Cleanup)??;
        }
        if self.loss.load(Ordering::Acquire) {
            return Err(Failure::Bound);
        }
        Ok(())
    }
    pub async fn kill_and_reap(&mut self) -> Result<()> {
        self.child.kill().map_err(|_| Failure::Child)?;
        let status = self.wait_exit().await?;
        if status.signal() != Some(9) {
            return Err(Failure::Child);
        }
        self.join_logs().await
    }
    pub async fn normal_exit_and_reap(&mut self) -> Result<()> {
        let status = self.wait_exit().await?;
        self.join_logs().await?;
        if !status.success() || status.signal().is_some() {
            return Err(Failure::Child);
        }
        Ok(())
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Negative cleanup only. It cannot supply positive normal/quiescent proof.
        if !self.reaped {
            let _ = self.child.kill();
            let deadline = Instant::now() + WAIT;
            while Instant::now() < deadline {
                if self.child.try_wait().is_ok_and(|s| s.is_some()) {
                    self.reaped = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        let deadline = Instant::now() + WAIT;
        while self.logs.iter().any(|t| !t.is_finished()) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        for task in self.logs.drain(..) {
            if task.is_finished() {
                let _ = task.join();
            } else {
                self.loss.store(true, Ordering::Release);
            }
        }
    }
}
fn drain<R: Read + Send + 'static>(
    mut input: R,
    writer: Writer,
    name: String,
    loss: Arc<AtomicBool>,
) -> Result<JoinHandle<Result<()>>> {
    // Preopen before spawning; output size is checked independently of role quota.
    let mut file = writer.open_log(&name)?;
    Ok(std::thread::spawn(move || {
        let mut persisted = 0usize;
        let mut bytes = [0u8; 4096];
        let mut failed = false;
        loop {
            let n = match input.read(&mut bytes) {
                Ok(n) => n,
                Err(_) => {
                    failed = true;
                    break;
                }
            };
            if n == 0 {
                break;
            }
            if failed {
                continue;
            }
            if persisted.checked_add(n).is_none_or(|s| s > LOG_BYTES) {
                failed = true;
                loss.store(true, Ordering::Release);
                writer.mark_loss();
                continue;
            }
            if writer.log_bytes(&mut file, &bytes[..n]).is_err() {
                failed = true;
                loss.store(true, Ordering::Release);
                writer.mark_loss();
                continue;
            }
            persisted += n;
        }
        file.sync_all().map_err(|_| Failure::Io)?;
        if failed {
            loss.store(true, Ordering::Release);
            writer.mark_loss();
            return Err(Failure::Bound);
        }
        Ok(())
    }))
}

/// Pure command/reply worker: no Store, host, native application or resolver.
pub struct Pending {
    pub request: Request,
    pub reply: mpsc::Sender<Result<Reply>>,
}
pub struct Reader {
    pub receiver: tokio::sync::mpsc::Receiver<Pending>,
    task: JoinHandle<Result<()>>,
}
impl Reader {
    pub async fn connect(root: &Path, hello: &Hello) -> Result<Self> {
        let stream = tokio::time::timeout(WAIT, UnixStream::connect(root.join("control.sock")))
            .await
            .map_err(|_| Failure::Timeout)?
            .map_err(|_| Failure::Io)?;
        let mut stream = stream.into_std().map_err(|_| Failure::Io)?;
        stream.set_nonblocking(false).map_err(|_| Failure::Io)?;
        stream
            .set_read_timeout(Some(WAIT))
            .map_err(|_| Failure::Io)?;
        stream
            .set_write_timeout(Some(WAIT))
            .map_err(|_| Failure::Io)?;
        std_write(&mut stream, hello, REPLY_BYTES)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let task = std::thread::spawn(move || {
            for expected in 1u8..=16 {
                let request: Request = std_read(&mut stream, REQUEST_BYTES)?;
                if request.version != 1 || request.ordinal != expected {
                    return Err(Failure::Protocol);
                }
                let stop = matches!(request.command, Command::Stop {});
                let (tx, rx) = mpsc::channel();
                sender
                    .blocking_send(Pending { request, reply: tx })
                    .map_err(|_| Failure::Protocol)?;
                let reply = rx.recv_timeout(WAIT).map_err(|_| Failure::Timeout)??;
                std_write(&mut stream, &reply, REPLY_BYTES)?;
                if stop {
                    return Ok(());
                }
            }
            Err(Failure::Bound)
        });
        Ok(Self { receiver, task })
    }
    pub async fn next(&mut self) -> Result<Pending> {
        tokio::time::timeout(WAIT, self.receiver.recv())
            .await
            .map_err(|_| Failure::Timeout)?
            .ok_or(Failure::Protocol)
    }
    pub async fn finish(self) -> Result<()> {
        tokio::time::timeout(WAIT, async {
            while !self.task.is_finished() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .map_err(|_| Failure::Timeout)?;
        self.task.join().map_err(|_| Failure::Cleanup)?
    }
}
fn std_read<T: DeserializeOwned>(stream: &mut StdStream, cap: usize) -> Result<T> {
    let deadline = Instant::now() + WAIT;
    let mut bytes = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Failure::Timeout);
        }
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|_| Failure::Io)?;
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).map_err(|_| Failure::Io)?;
        if b[0] == b'\n' {
            break;
        }
        if bytes.len() == cap || b[0] == b'\r' {
            return Err(Failure::Bound);
        }
        bytes.push(b[0]);
    }
    serde_json::from_slice(&bytes).map_err(|_| Failure::Protocol)
}
fn std_write<T: Serialize>(stream: &mut StdStream, value: &T, cap: usize) -> Result<()> {
    let mut bytes = encode(value, cap)?;
    bytes.push(b'\n');
    let deadline = Instant::now() + WAIT;
    let mut at = 0;
    while at < bytes.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Failure::Timeout);
        }
        stream
            .set_write_timeout(Some(remaining))
            .map_err(|_| Failure::Io)?;
        let n = stream.write(&bytes[at..]).map_err(|_| Failure::Io)?;
        if n == 0 {
            return Err(Failure::Io);
        }
        at += n;
    }
    Ok(())
}
