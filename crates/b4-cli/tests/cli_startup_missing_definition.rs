#![cfg(unix)]

// This protected support module contains no genuine test declarations.
#[allow(dead_code)]
#[path = "cli_support/mod.rs"]
mod cli_support;

use b4_contract::{
    Definition, EffectId, ExecutionRevision, ExecutionStatus, RecoveryPolicy, RouteKey, Snapshot,
    StateWrite, TaskId, TaskResult, TaskSpec, TaskStatus,
};
use b4_http::{
    ApprovalHostConfig, AuthenticationError, Authenticator, HttpHost, NativeHostConfig, Principal,
    ResourceContext, ResourcePolicy, ShutdownOutcome,
};
use b4_runtime::{
    approval::{ApprovalStore, ApprovalView},
    ApplicationIdentity, EffectBinding, EffectInvocation, EffectUncertain, InvocationContext,
    JournalEntry, NativeApplication, NativeEffect, NativeOutcome, OutputKind, RouteRegistration,
    Store,
};
use b4_sqlite::SqliteStore;
use cli_support::{all_rows, command, exchange, private_modes, Sandbox, TOKEN};
use rusqlite::{types::Value as SqlValue, Connection, OpenFlags};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(12);
const HISTORICAL_ROUTE: &str = "/retired#workflow";
const HISTORICAL_REVISION: &str = "retired-approval-v1";

struct ExactDemoAuth;
impl Authenticator for ExactDemoAuth {
    fn authenticate(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Principal, AuthenticationError> {
        if headers.get_all("authorization").iter().count() != 1
            || headers.get("authorization").and_then(|v| v.to_str().ok())
                != Some(format!("Bearer {TOKEN}").as_str())
        {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "native-demo".into(),
            tenant: "local".into(),
            principal: "owner".into(),
        })
    }
}

struct ExactOwnerPolicy;
impl ResourcePolicy for ExactOwnerPolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        context.origin.as_ref().is_none_or(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}

struct FinitePrepare(Arc<AtomicUsize>);
impl NativeEffect for FinitePrepare {
    fn invoke(
        &self,
        call: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let item = match call.task_id.as_str() {
            "prepare" => "prepared-historical",
            "release" => "released-historical",
            _ => return Err(EffectUncertain),
        };
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"task":call.task_id.as_str()}),
            writes: vec![StateWrite::Append {
                field: "items".into(),
                items: vec![json!(item)],
            }],
        }))
    }
}

fn historical_application(calls: Arc<AtomicUsize>) -> Arc<NativeApplication> {
    let tasks = ["prepare", "release"]
        .into_iter()
        .map(|id| TaskSpec {
            id: TaskId::new(id).unwrap(),
            effect_id: EffectId::new(id).unwrap(),
            requires_approval: id == "release",
            recovery: RecoveryPolicy::Pure,
        })
        .collect::<Vec<_>>();
    let effects = tasks
        .iter()
        .map(|task| {
            (
                task.effect_id.clone(),
                EffectBinding {
                    callback: Arc::new(FinitePrepare(calls.clone())) as Arc<dyn NativeEffect>,
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "native-demo".into(),
                revision: "retired-application-v1".into(),
            },
            vec![RouteRegistration {
                definition: Definition {
                    route: RouteKey::new(HISTORICAL_ROUTE).unwrap(),
                    revision: ExecutionRevision::new(HISTORICAL_REVISION).unwrap(),
                    max_attempts: 2,
                    batches: vec![tasks],
                },
                effects,
                input_validator: None,
            }],
        )
        .unwrap(),
    )
}

struct Seeded {
    owner: u64,
    view: ApprovalView,
    journal: Vec<JournalEntry>,
}

fn seed_historical_park(data: &Path) -> Seeded {
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
    let store_weak = Arc::downgrade(&store);
    let calls = Arc::new(AtomicUsize::new(0));
    let application = historical_application(calls.clone());
    let application_weak = Arc::downgrade(&application);
    let auth: Arc<dyn Authenticator> = Arc::new(ExactDemoAuth);
    let auth_weak = Arc::downgrade(&auth);
    let policy: Arc<dyn ResourcePolicy> = Arc::new(ExactOwnerPolicy);
    let policy_weak = Arc::downgrade(&policy);
    let approval = ApprovalHostConfig::new(
        "native-demo-policy-v1".into(),
        BTreeMap::from([(
            RouteKey::new(HISTORICAL_ROUTE).unwrap(),
            BTreeMap::from([(
                TaskId::new("release").unwrap(),
                json!({"kind":"approval","message":"Approve the historical release?"}),
            )]),
        )]),
    );
    let host = HttpHost::new_with_application(
        store.clone(),
        auth.clone(),
        policy.clone(),
        NativeHostConfig::new(application.clone(), approval),
    )
    .unwrap();
    drop(auth);
    drop(policy);
    drop(application);
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
        let response = exchange(
            address,
            "POST",
            "/threads/historical-cli-park/runs/wait",
            &json!({"route":HISTORICAL_ROUTE,"input":{"original":"historical-input"}}).to_string(),
        );
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"null");
        let pending = exchange(
            address,
            "GET",
            "/threads/historical-cli-park/pending_interrupts",
            "",
        );
        assert_eq!(pending.status, 200);
        assert!(pending.headers.contains("cache-control: no-store"));
        let pending = pending.json();
        let interrupts = pending["interrupts"].as_array().unwrap();
        assert_eq!(interrupts.len(), 1);
        assert!(interrupts[0]["grant"]
            .as_str()
            .unwrap()
            .starts_with("b4ag_"));
        let seed_snapshot: Snapshot =
            serde_json::from_value(cli_support::snapshot(data, "historical-cli-park")).unwrap();
        let view = store
            .approval_view(&seed_snapshot.execution_id)
            .unwrap()
            .unwrap();
        let journal = store.journal(&seed_snapshot.execution_id).unwrap();
        assert_eq!(view.execution.snapshot, seed_snapshot);
        assert_eq!(seed_snapshot.owner_generation, owner);
        assert_eq!(seed_snapshot.status, ExecutionStatus::WaitingForApproval);
        assert_eq!(seed_snapshot.definition.route.as_str(), HISTORICAL_ROUTE);
        assert_eq!(
            seed_snapshot.definition.revision.as_str(),
            HISTORICAL_REVISION
        );
        assert_eq!(seed_snapshot.tasks.len(), 2);
        assert_eq!(seed_snapshot.tasks[0].status, TaskStatus::Succeeded);
        assert_eq!(
            seed_snapshot.tasks[1].status,
            TaskStatus::WaitingForApproval
        );
        let origin = view.thread.origin.as_ref().unwrap();
        assert_eq!(origin.application, "native-demo");
        assert_eq!(origin.tenant, "local");
        assert_eq!(origin.principal, "owner");
        assert_eq!(view.grants.len(), 1);
        assert!(!view.grants[0].consumed && view.acceptances.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        shutdown.begin_shutdown();
        let _ = stop_tx.send(());
        let quiescent = tokio::time::timeout(WAIT, shutdown.wait_quiescent(WAIT))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(quiescent, ShutdownOutcome::Quiescent { faulted: false });
        tokio::time::timeout(WAIT, server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(host);
        drop(store);
        Seeded {
            owner,
            view,
            journal,
        }
    });
    drop(runtime);
    assert!(store_weak.upgrade().is_none());
    assert!(application_weak.upgrade().is_none());
    assert!(auth_weak.upgrade().is_none());
    assert!(policy_weak.upgrade().is_none());
    private_modes(data);
    let successor =
        SqliteStore::open(data).expect("normal seed retirement must permit a genuine next writer");
    assert_eq!(successor.owner_generation(), seeded.owner + 1);
    assert_eq!(
        successor
            .approval_view(&seeded.view.execution.snapshot.execution_id)
            .unwrap()
            .unwrap(),
        seeded.view
    );
    assert_eq!(
        successor
            .journal(&seeded.view.execution.snapshot.execution_id)
            .unwrap(),
        seeded.journal
    );
    drop(successor);
    seeded
}

fn current_owner(data: &Path) -> u64 {
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

struct OwnedCli {
    child: Option<Child>,
    stdout: PathBuf,
    stderr: PathBuf,
}

struct ExitObservation {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    signaled_after_ready: bool,
}

fn private_file(path: &Path) -> File {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap()
}

fn private_output(path: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    File::open(path)
        .unwrap()
        .take(65_537)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 65_536);
    bytes
}

impl OwnedCli {
    fn spawn(data: &Path, sandbox: &Sandbox) -> Self {
        let stdout = sandbox.path.join("missing-definition-stdout");
        let stderr = sandbox.path.join("missing-definition-stderr");
        let mut command = command(data);
        command.args(["serve", "--demo", "--recover"]);
        command.stdout(Stdio::from(private_file(&stdout)));
        command.stderr(Stdio::from(private_file(&stderr)));
        command.stdin(Stdio::null());
        let child = command.spawn().unwrap();
        Self {
            child: Some(child),
            stdout,
            stderr,
        }
    }
    fn stdout(&self) -> Vec<u8> {
        private_output(&self.stdout)
    }
    fn actual_exit(&mut self) -> Option<ExitStatus> {
        self.child.as_mut().unwrap().try_wait().unwrap()
    }
    fn finish(&mut self, status: ExitStatus, signaled_after_ready: bool) -> ExitObservation {
        self.child.take();
        let stdout = self.stdout();
        let stderr = private_output(&self.stderr);
        ExitObservation {
            status,
            stdout,
            stderr,
            signaled_after_ready,
        }
    }
    fn terminate(&mut self) {
        let pid = self.child.as_ref().unwrap().id().to_string();
        assert!(Command::new("/bin/kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success());
    }
    fn wait_actual_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.actual_exit() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "normal CLI exit must be positively observed"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for OwnedCli {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Failure cleanup never supplies a normal-return or lock-release witness.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn bounded_ready_status(address: SocketAddr) -> Option<u16> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    stream.write_all(format!("GET /readyz HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n").as_bytes()).ok()?;
    let mut bytes = Vec::new();
    stream.take(65_537).read_to_end(&mut bytes).ok()?;
    assert!(bytes.len() <= 65_536);
    std::str::from_utf8(&bytes)
        .ok()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn exit_or_normal_ready_shutdown(process: &mut OwnedCli) -> ExitObservation {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(status) = process.actual_exit() {
            return process.finish(status, false);
        }
        let stdout = process.stdout();
        let text = std::str::from_utf8(&stdout).unwrap();
        let address = text.lines().find_map(|line| {
            line.strip_prefix("b4run_listening http://")
                .and_then(|address| address.parse::<SocketAddr>().ok())
        });
        if let Some(address) = address {
            assert!(address.is_ipv4() && address.ip().is_loopback() && address.port() != 0);
            match bounded_ready_status(address) {
                Some(200) => {
                    // Ordinary scaffold readiness is reachable: close it normally.
                    process.terminate();
                    let status = process.wait_actual_exit();
                    return process.finish(status, true);
                }
                Some(status) => assert_eq!(status, 503),
                None => { /* A fast normal failure can close transport between observations. */ }
            }
        }
        assert!(
            Instant::now() < deadline,
            "finite preparation must exit or become actually ready"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn explicit_cli_missing_historical_definition_refuses_after_normal_retirement() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-missing-state");
    let seeded = seed_historical_park(&data);
    let before_rows = all_rows(&data);
    assert_eq!(before_rows.len(), 6);
    assert_eq!(before_rows[0].len(), 1);
    assert_eq!(before_rows[0][0].len(), 4);
    assert_eq!(current_owner(&data), seeded.owner + 1);
    let mut process = OwnedCli::spawn(&data, &sandbox);
    let output = exit_or_normal_ready_shutdown(&mut process);
    assert!(
        output.stdout.ends_with(b"b4run_stopped\n"),
        "actual normal runtime destruction must precede process exit"
    );
    let text = std::str::from_utf8(&output.stdout).unwrap();
    assert!(text.lines().any(|line| line == "b4run_draining"));
    for line in text.lines() {
        let listener = line
            .strip_prefix("b4run_listening http://")
            .and_then(|address| address.parse::<SocketAddr>().ok())
            .is_some_and(|address| {
                address.is_ipv4() && address.ip().is_loopback() && address.port() != 0
            });
        assert!(
            listener
                || [
                    "b4run_preparing",
                    "b4run_ready",
                    "b4run_draining",
                    "b4run_drain_incomplete",
                    "b4run_stopped"
                ]
                .contains(&line)
        );
    }
    let restart_owner = current_owner(&data);
    assert_eq!(restart_owner, seeded.owner + 2);
    let next = SqliteStore::open(&data)
        .expect("actual normal CLI retirement must permit a genuine next writer");
    let next_owner = next.owner_generation();
    assert_eq!(next_owner, restart_owner + 1);
    let after = next
        .approval_view(&seeded.view.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let journal = next
        .journal(&seeded.view.execution.snapshot.execution_id)
        .unwrap();
    drop(next);
    assert_eq!(
        after, seeded.view,
        "incompatible original work and approval authority must remain exact"
    );
    assert_eq!(journal, seeded.journal);
    let mut expected_rows = before_rows;
    expected_rows[0][0][3] = SqlValue::Blob(next_owner.to_be_bytes().to_vec());
    assert_eq!(
        all_rows(&data),
        expected_rows,
        "all six real tables may change only by genuine metadata owner advancement"
    );
    private_modes(&data);

    // The intended RED is reached only after normal seed/CLI retirement,
    // all seed Weak absence and both genuine subsequent writer closures.
    assert_eq!(
        (output.status.code(), output.stderr.as_slice()),
        (Some(1), b"b4run_preparation_failed\n".as_slice()),
        "missing original definition must yield only the fixed preparation failure"
    );
    // Later branch checks have zero initial RED credit until the tuple passes.
    assert!(
        !output.signaled_after_ready,
        "incompatible startup must fail before ordinary readiness"
    );
    assert!(!text.lines().any(|line| line == "b4run_ready"));
}
