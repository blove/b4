//! Finite legal seed and real selflaunch helpers; no test declarations.
use super::*;
use b4_runtime::{
    approval::{
        ApprovalAcceptanceRequest, ApprovalAuthority, ApprovalContext, ApprovalError,
        ApprovalFence, ApprovalOperation, ApprovalResource, ApprovalService, ApprovalStore,
        ApprovalView, GrantDecision,
    },
    EffectBinding, EffectInvocation, EffectUncertain, InvocationContext, JournalEntry,
    NativeEffect, NativeOutcome, OutputKind, RouteRegistration, Store, StoreError, ThreadOrigin,
};
use rusqlite::{types::Value as SqlValue, Connection, OpenFlags};
use serde_json::json;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

pub(super) const CHILD_DIRECTORY: &str = "B4_PRIVATE_STARTUP_DIRECTORY";
pub(super) const CHILD_SCENARIO: &str = "B4_PRIVATE_STARTUP_SCENARIO";
const CHILD_TEST: &str = "process::startup::tests::startup_child_fixture";
const WAIT: Duration = Duration::from_secs(12);
const TOKEN: &str = "private-startup-owner";
const THREAD: &str = "private-startup-seed";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

pub(super) struct Directory {
    pub path: PathBuf,
}
impl Directory {
    pub fn new() -> Self {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "b4-cli-private-startup-{}-{id}",
            std::process::id()
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self { path }
    }
    pub fn data(&self) -> PathBuf {
        self.path.join("data")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn private_file(path: &Path) -> File {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap()
}
pub(super) fn write_marker(directory: &Path, name: &str, text: &str) {
    let mut file = private_file(&directory.join(name));
    file.write_all(text.as_bytes()).unwrap();
}
pub(super) fn mark(directory: &Path, name: &str) {
    write_marker(directory, name, "observed");
}
pub(super) fn release(directory: &Path, gate: &str) {
    mark(directory, &format!("{gate}-release"));
}
pub(super) fn hold(directory: &Path, gate: &str) {
    mark(directory, &format!("{gate}-entered"));
    let deadline = Instant::now() + Duration::from_secs(45);
    while !directory.join(format!("{gate}-release")).exists() {
        if Instant::now() >= deadline {
            mark(directory, &format!("{gate}-expired"));
            panic!("private trusted work did not receive explicit release");
        }
        std::thread::sleep(Duration::from_millis(3));
    }
    mark(directory, &format!("{gate}-returned"));
}
pub(super) fn assert_absent(directory: &Directory, name: &str) {
    assert!(
        !directory.path.join(name).exists(),
        "unexpected marker {name}"
    );
}
pub(super) fn config(data: &Path) -> Config {
    let command =
        crate::config::parse(
            ["serve", "--demo", "--recover"].map(Into::into),
            |key| match key {
                "B4RUN_TOKEN" => Some(TOKEN.into()),
                "B4RUN_PORT" => Some("0".into()),
                "B4RUN_DATA_DIR" => Some(data.as_os_str().to_owned()),
                "B4RUN_SHUTDOWN_WAIT_MS" => Some("20".into()),
                _ => None,
            },
        )
        .unwrap();
    let crate::config::Command::ServeRecover(config) = command else {
        panic!("explicit parser selection required");
    };
    config
}

pub(super) struct OwnedChild {
    child: Option<Child>,
    directory: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
}
pub(super) struct Output {
    pub stderr: Vec<u8>,
    pub result: String,
}
impl OwnedChild {
    pub fn spawn(directory: &Directory, scenario: &str) -> Self {
        let stdout = directory.path.join("stdout");
        let stderr = directory.path.join("stderr");
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                CHILD_TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env_clear()
            .env(CHILD_DIRECTORY, &directory.path)
            .env(CHILD_SCENARIO, scenario)
            .stdin(Stdio::null())
            .stdout(Stdio::from(private_file(&stdout)))
            .stderr(Stdio::from(private_file(&stderr)))
            .spawn()
            .unwrap();
        Self {
            child: Some(child),
            directory: directory.path.clone(),
            stdout,
            stderr,
        }
    }
    pub fn assert_running(&mut self) {
        assert!(self.child.as_mut().unwrap().try_wait().unwrap().is_none());
    }
    pub fn marker(&mut self, name: &str) {
        let deadline = Instant::now() + WAIT;
        while !self.directory.join(name).exists() {
            if self.child.as_mut().unwrap().try_wait().unwrap().is_some() {
                assert!(
                    self.directory.join(name).exists(),
                    "child exited without actual marker {name}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "missing actual child marker {name}"
            );
            std::thread::sleep(Duration::from_millis(3));
        }
    }
    pub fn marker_after_exit(&self, name: &str) {
        assert!(
            self.directory.join(name).exists(),
            "missing normal retirement marker {name}"
        );
    }
    pub fn listening(&mut self) -> SocketAddr {
        self.marker("listening");
        let address: SocketAddr = std::fs::read_to_string(self.directory.join("listening"))
            .unwrap()
            .parse()
            .unwrap();
        assert!(address.is_ipv4() && address.ip().is_loopback() && address.port() != 0);
        address
    }
    pub fn ping(&mut self, id: u8) {
        mark(&self.directory, &format!("ping-{id}"));
        self.marker(&format!("pong-{id}"));
    }
    pub fn signal(&mut self, signal: &str) {
        assert!(Command::new("/bin/kill")
            .args([signal, &self.child.as_ref().unwrap().id().to_string()])
            .status()
            .unwrap()
            .success());
    }
    pub fn normal_exit(&mut self) -> Output {
        let deadline = Instant::now() + WAIT;
        let status = loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "normal child retirement did not occur"
            );
            std::thread::sleep(Duration::from_millis(3));
        };
        self.child.take();
        assert!(status.success());
        let stdout = bounded_file(&self.stdout);
        let stderr = bounded_file(&self.stderr);
        assert!(stdout
            .windows(b"b4run_draining\n".len())
            .any(|row| row == b"b4run_draining\n"));
        assert!(stdout
            .windows(b"b4run_stopped\n".len())
            .any(|row| row == b"b4run_stopped\n"));
        let result = std::fs::read_to_string(self.directory.join("runner-result")).unwrap();
        assert!(self.directory.join("runner-returned").exists());
        for gate in ["recover", "callback"] {
            assert!(!self.directory.join(format!("{gate}-expired")).exists());
        }
        Output { stderr, result }
    }
    pub fn kill_cleanup(&mut self) {
        let mut child = self.child.take().unwrap();
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn bounded_file(path: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    File::open(path)
        .unwrap()
        .take(65_537)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 65_536);
    bytes
}
pub(super) fn request(address: SocketAddr, method: &str, path: &str, body: &str) -> u16 {
    let mut stream = TcpStream::connect_timeout(&address, WAIT).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    write!(stream, "{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    let mut bytes = Vec::new();
    stream.take(65_537).read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() <= 65_536);
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}
pub(super) fn assert_owned(data: &Path) {
    assert!(matches!(
        SqliteStore::open(data),
        Err(StoreError::AlreadyOwned)
    ));
}
pub(super) fn current_owner(data: &Path) -> u64 {
    let connection = Connection::open_with_flags(
        data.join("runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let bytes: Vec<u8> = connection
        .query_row("SELECT owner_epoch FROM metadata WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    u64::from_be_bytes(bytes.try_into().unwrap())
}
pub(super) fn captured_snapshot(data: &Path, id: &ExecutionId) -> Snapshot {
    let connection = Connection::open_with_flags(
        data.join("runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let text: String = connection
        .query_row(
            "SELECT snapshot FROM executions WHERE id = ?1",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&text).unwrap()
}
pub(super) fn captured_authority(data: &Path) -> Vec<Vec<Vec<SqlValue>>> {
    let connection = Connection::open_with_flags(
        data.join("runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    ["approval_grants", "approval_acceptances"]
        .map(|table| {
            let mut query = connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY execution_id, 2"))
                .unwrap();
            let width = query.column_count();
            let mapped = query
                .query_map([], |row| (0..width).map(|column| row.get(column)).collect())
                .unwrap();
            mapped.collect::<Result<Vec<Vec<SqlValue>>, _>>().unwrap()
        })
        .to_vec()
}

struct FixedAccept;
impl ApprovalAuthority for FixedAccept {
    fn authorize(
        &self,
        context: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if operation != ApprovalOperation::Accept
            || context.policy_revision != "native-demo-policy-v1"
            || context.actor != trusted_origin()
            || resource.thread.origin.as_ref() != Some(&trusted_origin())
        {
            return Err(ApprovalError::Unauthorized);
        }
        Ok(())
    }
}
fn trusted_origin() -> ThreadOrigin {
    ThreadOrigin {
        application: "native-demo".into(),
        tenant: "local".into(),
        principal: "owner".into(),
    }
}
pub(super) struct Seeded {
    pub owner: u64,
    pub view: ApprovalView,
    pub authority_rows: Vec<Vec<Vec<SqlValue>>>,
}
pub(super) fn seed(data: &Path, accepted_once: bool) -> Seeded {
    std::fs::DirBuilder::new().mode(0o700).create(data).unwrap();
    drop(private_file(&data.join("runtime.sqlite3")));
    drop(private_file(&data.join("owner.lock")));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let store = Arc::new(SqliteStore::open(data).unwrap());
    let owner = store.owner_generation();
    let assembly = crate::demo::assemble().unwrap();
    let auth: Arc<dyn Authenticator> =
        Arc::new(super::super::super::BearerAuth(format!("Bearer {TOKEN}")));
    let policy: Arc<dyn ResourcePolicy> = Arc::new(super::super::super::OwnerPolicy);
    let weak_store = Arc::downgrade(&store);
    let weak_app = Arc::downgrade(&assembly.application);
    let weak_auth = Arc::downgrade(&auth);
    let weak_policy = Arc::downgrade(&policy);
    let mut config = NativeHostConfig::new(assembly.application, assembly.approval);
    config.limits = assembly.limits;
    let host = HttpHost::new_with_application(store.clone(), auth, policy, config).unwrap();
    let seeded = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let router = host.router();
        let shutdown = host.shutdown_handle();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stop_rx.await;
                })
                .await
        });
        assert_eq!(
            request(
                address,
                "POST",
                &format!("/threads/{THREAD}/runs/wait"),
                &json!({"route":"/approve#workflow","input":{"original":"seed-input"}}).to_string()
            ),
            200
        );
        let ordinary = store
            .thread_view(&ThreadId::new(THREAD).unwrap())
            .unwrap()
            .unwrap();
        let snapshot = ordinary.execution.unwrap().snapshot;
        let parked = store
            .approval_view(&snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.status, ExecutionStatus::WaitingForApproval);
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
        assert_eq!(snapshot.tasks[1].status, TaskStatus::WaitingForApproval);
        assert_eq!(snapshot.owner_generation, owner);
        assert_eq!(parked.thread.origin.as_ref(), Some(&trusted_origin()));
        assert_eq!(parked.grants.len(), 1);
        assert!(!parked.grants[0].consumed && parked.acceptances.is_empty());
        if accepted_once {
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap();
            assert!(now_ms >= snapshot.updated_at_ms && now_ms < parked.grants[0].expires_at_ms);
            let turn_id = TurnId::new("private-trusted-once-turn").unwrap();
            assert!(!snapshot.turn_history.contains(&turn_id));
            let service = ApprovalService::new(store.clone(), FixedAccept).unwrap();
            let accepted = service
                .accept(
                    &snapshot.execution_id,
                    ApprovalFence {
                        execution: (&snapshot).into(),
                        resource_revision: parked.thread.resource_revision,
                    },
                    ApprovalAcceptanceRequest {
                        context: ApprovalContext {
                            actor: trusted_origin(),
                            policy_revision: "native-demo-policy-v1".into(),
                        },
                        route: snapshot.definition.route.clone(),
                        definition_revision: snapshot.definition.revision.clone(),
                        turn_id,
                        now_ms,
                        decisions: vec![GrantDecision {
                            interrupt_id: parked.grants[0].interrupt_id.clone(),
                            decision: ApprovalDecision::Once,
                            cancelled: false,
                            token: Some(parked.grants[0].token.clone()),
                        }],
                    },
                )
                .unwrap();
            assert_eq!(accepted.snapshot.status, ExecutionStatus::Running);
            let mut expected_task = snapshot.tasks[1].clone();
            expected_task.status = TaskStatus::Ready;
            expected_task.approved = true;
            assert_eq!(accepted.snapshot.tasks[1], expected_task);
            assert_eq!(accepted.snapshot.definition, snapshot.definition);
            assert_eq!(accepted.snapshot.tasks[0], snapshot.tasks[0]);
            drop(service);
        }
        let view = store
            .approval_view(&snapshot.execution_id)
            .unwrap()
            .unwrap();
        let journal: Vec<JournalEntry> = store.journal(&snapshot.execution_id).unwrap();
        if accepted_once {
            let mut expected_grants = parked.grants.clone();
            expected_grants[0].consumed = true;
            assert_eq!(view.grants, expected_grants);
            assert_eq!(view.acceptances.len(), 1);
            assert_eq!(
                view.acceptances[0].decisions[0].decision,
                ApprovalDecision::Once
            );
            assert_eq!(view.acceptances[0].context.actor, trusted_origin());
        }
        let authority_rows = captured_authority(data);
        shutdown.begin_shutdown();
        let _ = stop_tx.send(());
        assert_eq!(
            tokio::time::timeout(WAIT, shutdown.wait_quiescent(WAIT))
                .await
                .unwrap()
                .unwrap(),
            ShutdownOutcome::Quiescent { faulted: false }
        );
        tokio::time::timeout(WAIT, server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(host);
        drop(store);
        (
            Seeded {
                owner,
                view,
                authority_rows,
            },
            journal,
        )
    });
    drop(runtime);
    assert!(weak_store.upgrade().is_none());
    assert!(weak_app.upgrade().is_none());
    assert!(weak_auth.upgrade().is_none());
    assert!(weak_policy.upgrade().is_none());
    assert_eq!(
        std::fs::metadata(data).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["runtime.sqlite3", "owner.lock"] {
        assert_eq!(
            std::fs::metadata(data.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let successor = SqliteStore::open(data).unwrap();
    assert_eq!(successor.owner_generation(), owner + 1);
    assert_eq!(
        successor
            .approval_view(&seeded.0.view.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        seeded.0.view
    );
    assert_eq!(
        successor
            .journal(&seeded.0.view.execution.snapshot.execution_id)
            .unwrap(),
        seeded.1
    );
    drop(successor);
    seeded.0
}

struct HeldRelease(PathBuf);
impl NativeEffect for HeldRelease {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        assert_eq!(context.origin, trusted_origin());
        let task = call.task_id.as_str();
        let (output, item) = match task {
            "prepare" => (json!({"prepared":true}), "prepared"),
            "release" => {
                hold(&self.0, "callback");
                (json!({"released":true}), "released")
            }
            "first" | "second" => (
                json!({"task":task,"input":call.input.get("input").cloned().unwrap_or(serde_json::Value::Null)}),
                task,
            ),
            _ => return Err(EffectUncertain),
        };
        Ok(NativeOutcome::Succeeded(TaskResult {
            output,
            writes: vec![StateWrite::Append {
                field: "items".into(),
                items: vec![json!(item)],
            }],
        }))
    }
}
pub(super) fn held_application(directory: &Path) -> crate::demo::AppAssembly {
    let mut assembly = crate::demo::assemble().unwrap();
    let registrations = assembly
        .application
        .definitions()
        .map(|definition| RouteRegistration {
            definition: definition.clone(),
            effects: definition
                .batches
                .iter()
                .flatten()
                .map(|task| {
                    (
                        task.effect_id.clone(),
                        EffectBinding {
                            callback: Arc::new(HeldRelease(directory.to_owned()))
                                as Arc<dyn NativeEffect>,
                            output_kind: OutputKind::Value,
                        },
                    )
                })
                .collect(),
            input_validator: None,
        })
        .collect();
    assembly.application = Arc::new(
        NativeApplication::new(assembly.application.identity().clone(), registrations).unwrap(),
    );
    assembly
}
