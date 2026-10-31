use super::{
    application,
    capture::{CaptureListener, CaptureSummary, Record},
    control,
    files::{self, Evidence},
    gate::{Cleanup, Gates},
    Error, Result,
};
use b4_contract::ThreadId;
use b4_http::{ShutdownOutcome, ShutdownPhase};
use b4_runtime::{approval::ApprovalStore, Store, StoreError};
use b4_sqlite::SqliteStore;
use serde_json::json;
use std::{
    io::Read,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::oneshot};
pub struct Args {
    pub node: PathBuf,
    pub runner: PathBuf,
    pub bundle_dir: PathBuf,
    pub output_dir: PathBuf,
}
const FINAL_WAIT: Duration = Duration::from_secs(10);
const RUN_WAIT: Duration = Duration::from_secs(120);
struct OwnedChild {
    child: Option<Child>,
    control: Option<JoinHandle<Result<control::Proof>>>,
    stderr: Option<JoinHandle<Result<Vec<u8>>>>,
    gates: Gates,
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.gates.cleanup();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let until = Instant::now() + FINAL_WAIT;
            while Instant::now() < until {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => break,
                    Ok(None) => thread::sleep(Duration::from_millis(2)),
                }
            }
        }
        let until = Instant::now() + FINAL_WAIT;
        while Instant::now() < until
            && (self.control.as_ref().is_some_and(|t| !t.is_finished())
                || self.stderr.as_ref().is_some_and(|t| !t.is_finished()))
        {
            thread::sleep(Duration::from_millis(2));
        }
        if self.control.as_ref().is_some_and(JoinHandle::is_finished) {
            let _ = self.control.take().unwrap().join();
        }
        if self.stderr.as_ref().is_some_and(JoinHandle::is_finished) {
            let _ = self.stderr.take().unwrap().join();
        }
    }
}
struct Server {
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}
impl Server {
    async fn stop(&mut self) -> Result<()> {
        self.stop
            .take()
            .ok_or(Error("server_stop_missing"))?
            .send(())
            .map_err(|_| Error("server_stop_failed"))?;
        let task = self.task.as_mut().ok_or(Error("server_join_missing"))?;
        let result = tokio::time::timeout(FINAL_WAIT, task)
            .await
            .map_err(|_| Error("server_join_expired"))?
            .map_err(|_| Error("server_join_failed"))?;
        self.task.take();
        result
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
async fn join<T>(task: &mut Option<JoinHandle<Result<T>>>) -> Result<T> {
    let until = Instant::now() + FINAL_WAIT;
    while !task
        .as_ref()
        .ok_or(Error("thread_join_missing"))?
        .is_finished()
    {
        if Instant::now() >= until {
            return Err(Error("thread_join_expired"));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    task.take()
        .unwrap()
        .join()
        .map_err(|_| Error("thread_panicked"))?
}
fn check_input(args: &Args) -> Result<()> {
    for p in [&args.node, &args.runner, &args.bundle_dir, &args.output_dir] {
        if !p.is_absolute() {
            return Err(Error("fixture_absolute_arguments_required"));
        }
    }
    for p in [&args.node, &args.runner] {
        if !p.is_file() {
            return Err(Error("fixture_input_missing"));
        }
    }
    if !args.bundle_dir.is_dir() {
        return Err(Error("fixture_input_missing"));
    }
    Ok(())
}
async fn absent(mut predicate: impl FnMut() -> bool) -> Result<()> {
    let until = Instant::now() + FINAL_WAIT;
    while !predicate() {
        if Instant::now() >= until {
            return Err(Error("resource_retirement_expired"));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(())
}
fn persist_captures(
    evidence: &Evidence,
    summary: &CaptureSummary,
    records: &[Record],
) -> Result<()> {
    let mut entries = Vec::with_capacity(records.len());
    let mut sorted: Vec<_> = records.iter().collect();
    sorted.sort_by_key(|r| r.ordinal);
    for record in sorted {
        let read = format!("conn-{:04}.read.bin", record.ordinal);
        let write = format!("conn-{:04}.write.bin", record.ordinal);
        evidence.write(&read, record.read_bytes())?;
        evidence.write(&write, record.write_bytes())?;
        entries.push(json!({"ordinal":record.ordinal,"read_file":read,"write_file":write,"read_bytes":record.read_bytes().len(),"write_bytes":record.write_bytes().len(),"read_eof":record.read_eof,"write_shutdown":record.write_shutdown}));
    }
    evidence.json("capture-manifest.json",&json!({"version":1,"accepted":summary.accepted,"retired":summary.retired,"sticky_loss":summary.sticky_loss,"connections":entries}))
}
pub async fn run(args: Args) -> Result<()> {
    run_with_wait(args, RUN_WAIT).await
}
async fn run_with_wait(args: Args, wait: Duration) -> Result<()> {
    check_input(&args)?;
    let run_deadline = Instant::now() + wait;
    files::private_dir(&args.output_dir)?;
    files::private_dir(&args.output_dir.join("evidence"))?;
    let native = args.output_dir.join("evidence/native");
    let client = args.output_dir.join("evidence/client");
    files::private_dir(&native)?;
    files::private_dir(&client)?;
    let evidence = Evidence::new(native.clone());
    let state = args.output_dir.join("state");
    files::prepare_store(&state)?;
    let store = Arc::new(SqliteStore::open(&state).map_err(|_| Error("fixture_store_open"))?);
    files::verify_store(&state)?;
    let prior_owner_generation = store.owner_generation();
    let weak_store = Arc::downgrade(&store);
    let mut random = [0u8; 32];
    getrandom::fill(&mut random).map_err(|_| Error("fixture_entropy_failed"))?;
    let token: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    let gates = Gates::new();
    let _cleanup = Cleanup(gates.clone());
    let (host, weak_application) = application::host(store.clone(), &token, &gates)?;
    let shutdown = host.shutdown_handle();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| Error("fixture_bind_failed"))?;
    let address = listener
        .local_addr()
        .map_err(|_| Error("fixture_address_failed"))?;
    let (listener, collector) = CaptureListener::new(listener);
    let (stop, stopped) = oneshot::channel();
    let router = host.router();
    let mut server = Server {
        stop: Some(stop),
        task: Some(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .map_err(|_| Error("server_failed"))
        })),
    };
    let child = Command::new(&args.node)
        .arg(&args.runner)
        .arg(&args.bundle_dir)
        .arg(&client)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| Error("child_start_failed"))?;
    let mut owned = OwnedChild {
        child: Some(child),
        control: None,
        stderr: None,
        gates: gates.clone(),
    };
    let reader = owned
        .child
        .as_mut()
        .unwrap()
        .stdout
        .take()
        .ok_or(Error("child_stdout_missing"))?;
    let writer = owned
        .child
        .as_mut()
        .unwrap()
        .stdin
        .take()
        .ok_or(Error("child_stdin_missing"))?;
    let mut stderr = owned
        .child
        .as_mut()
        .unwrap()
        .stderr
        .take()
        .ok_or(Error("child_stderr_missing"))?;
    let failed = Arc::new(AtomicBool::new(false));
    let control_failed = failed.clone();
    let stderr_failed = failed.clone();
    let observer = store.clone();
    let control_gates = gates.clone();
    let control_evidence = evidence.clone();
    owned.control = Some(
        thread::Builder::new()
            .name("ap-client-control".into())
            .spawn(move || {
                let result = control::serve(
                    reader,
                    writer,
                    observer,
                    control_gates,
                    control_evidence,
                    format!("http://{address}"),
                    token,
                );
                if result.is_err() {
                    control_failed.store(true, Ordering::Release);
                }
                result
            })
            .map_err(|_| Error("control_thread_start"))?,
    );
    owned.stderr = Some(
        thread::Builder::new()
            .name("ap-client-stderr".into())
            .spawn(move || {
                let result = (|| {
                    let mut bytes = Vec::new();
                    let mut buffer = [0; 4096];
                    loop {
                        let n = match stderr.read(&mut buffer) {
                            Ok(n) => n,
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            Err(_) => return Err(Error("stderr_read_failed")),
                        };
                        if n == 0 {
                            return Ok(bytes);
                        }
                        if n > 1048576usize.saturating_sub(bytes.len()) {
                            return Err(Error("stderr_limit_exceeded"));
                        }
                        bytes.extend_from_slice(&buffer[..n]);
                    }
                })();
                if result.is_err() {
                    stderr_failed.store(true, Ordering::Release);
                }
                result
            })
            .map_err(|_| Error("stderr_thread_start"))?,
    );
    let status = loop {
        if failed.load(Ordering::Acquire) {
            return Err(Error("child_control_failed"));
        }
        if let Some(status) = owned
            .child
            .as_mut()
            .unwrap()
            .try_wait()
            .map_err(|_| Error("child_wait_failed"))?
        {
            break status;
        }
        if Instant::now() >= run_deadline {
            return Err(Error("child_watchdog_expired"));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    };
    if status.code() != Some(0) {
        return Err(Error("child_exit_failed"));
    }
    owned.child.take();
    let control = join(&mut owned.control).await?;
    let stderr = join(&mut owned.stderr).await?;
    if !gates.all_positive() {
        return Err(Error("gate_retirement_failed"));
    }
    if shutdown.status().phase != ShutdownPhase::Serving {
        return Err(Error("premature_admission_cut"));
    }
    evidence.write("stderr.bin", &stderr)?;
    let client_used = files::verify_client(&client)?;
    files::verify_dir(&args.output_dir)?;
    files::verify_dir(&args.output_dir.join("evidence"))?;
    files::verify_dir(&native)?;
    shutdown.begin_shutdown();
    server.stop().await?;
    let outcome = shutdown
        .wait_quiescent(FINAL_WAIT)
        .await
        .map_err(|_| Error("shutdown_wait_failed"))?;
    if outcome != (ShutdownOutcome::Quiescent { faulted: false }) {
        return Err(Error("shutdown_not_quiescent"));
    }
    let (summary, records) = collector.finish().await?;
    drop(host);
    drop(store);
    absent(|| weak_store.upgrade().is_none() && weak_application.upgrade().is_none()).await?;
    let until = Instant::now() + FINAL_WAIT;
    let writer = loop {
        match SqliteStore::open(&state) {
            Ok(writer) => break writer,
            Err(StoreError::AlreadyOwned) if Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(1)).await
            }
            Err(_) => return Err(Error("writer_reopen_failed")),
        }
    };
    if writer.owner_generation()
        != prior_owner_generation
            .checked_add(1)
            .ok_or(Error("owner_generation_exhausted"))?
        || writer.readiness().is_err()
    {
        return Err(Error("writer_witness_failed"));
    }
    files::verify_store(&state)?;
    for (index, id) in control.observed_threads.iter().enumerate() {
        let id = ThreadId::new(id).map_err(|_| Error("control_invalid_thread"))?;
        let view = writer
            .thread_view(&id)
            .map_err(|_| Error("writer_view_failed"))?;
        evidence.json(&format!("reopened-{:04}-thread.json", index + 1), &view)?;
        let pending = match view.as_ref().and_then(|v| v.execution.as_ref()) {
            Some(e) => writer
                .approval_view(&e.snapshot.execution_id)
                .map_err(|_| Error("writer_view_failed"))?,
            None => None,
        };
        let value=pending.as_ref().map(|p|json!({"thread":p.thread,"execution":p.execution,"grants":p.grants,"acceptances":p.acceptances}));
        evidence.json(&format!("reopened-{:04}-approval.json", index + 1), &value)?;
    }
    persist_captures(&evidence, &summary, &records)?;
    let gate_status = |gate: &super::gate::Gate| gate.latest();
    evidence.json("native-proof.json",&json!({"version":1,"child":{"exit_code":0,"normal_reap":true},"control":control,"control_joined":true,"stderr":{"pipe_eof":true,"joined":true,"bytes":stderr.len()},"server":{"normal_join":true},"shutdown":{"outcome":"quiescent","faulted":false,"active_owners":shutdown.status().active_owners},"capture":summary,"weak_store_absent":weak_store.upgrade().is_none(),"weak_application_absent":weak_application.upgrade().is_none(),"writer":{"reopened":true,"prior_owner_generation":prior_owner_generation,"owner_generation":writer.owner_generation(),"ready":true},"gates":{"controller_dispatch":gate_status(&gates.controller),"worker_held":gate_status(&gates.worker)},"mode_checks":{"private_root":true,"store_at_open":true,"store_at_reopen":true},"evidence":{"native_limit_bytes":files::NATIVE_LIMIT,"client_limit_bytes":files::CLIENT_LIMIT,"client_used_bytes":client_used}}))?;
    let native_used = evidence.finish()?;
    if native_used
        .checked_add(client_used)
        .is_none_or(|total| total > 32 * 1024 * 1024)
    {
        return Err(Error("evidence_total_exceeded"));
    }
    drop(writer);
    files::verify_store(&state)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_parent_completes_positive_child_and_resource_retirement() {
        let base = std::env::temp_dir().join(format!("b4-ap-life-{}", uuid::Uuid::new_v4()));
        files::private_dir(&base).unwrap();
        let bundle = base.join("bundle");
        files::private_dir(&bundle).unwrap();
        let runner = base.join("unit-runner.sh");
        let observations = b"{\"version\":1,\"groups\":[]}\n";
        let mut used = observations.len();
        let ledger = loop {
            let bytes = files::encode(
                &json!({"version":1,"limit_bytes":files::CLIENT_LIMIT,"used_bytes":used}),
                8192,
            )
            .unwrap();
            let next = observations.len() + bytes.len();
            if next == used {
                break bytes;
            }
            used = next;
        };
        let script=format!("umask 077\nprintf '%s\\n' '{{\"version\":1,\"ordinal\":1,\"case\":\"setup\",\"command\":{{\"ready\":{{}}}}}}'\nIFS= read -r reply || exit 2\nprintf '%s\\n' '{}' > \"$2/client-observations.json\"\nprintf '%s\\n' '{}' > \"$2/quota-ledger.json\"\nprintf '%s\\n' '{{\"version\":1,\"ordinal\":2,\"case\":\"retirement\",\"command\":{{\"final_stop\":{{}}}}}}'\nIFS= read -r reply || exit 3\nexit 0\n",std::str::from_utf8(observations).unwrap().trim_end(),std::str::from_utf8(&ledger).unwrap().trim_end());
        std::fs::write(&runner, script).unwrap();
        let output = base.join("output");
        let result = run(Args {
            node: PathBuf::from("/bin/sh"),
            runner,
            bundle_dir: bundle,
            output_dir: output.clone(),
        })
        .await;
        assert!(
            result.is_ok(),
            "synthetic child/native actual resource lifecycle failed: {result:?}"
        );
        let proof: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.join("evidence/native/native-proof.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(proof["writer"]["owner_generation"], 2);
        assert_eq!(proof["child"]["exit_code"], 0);
        assert_eq!(proof["shutdown"]["faulted"], false);
        std::fs::remove_dir_all(base).unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn child_watchdog_forced_cleanup_never_emits_positive_retirement() {
        let base = std::env::temp_dir().join(format!("b4-ap-watchdog-{}", uuid::Uuid::new_v4()));
        files::private_dir(&base).unwrap();
        let bundle = base.join("bundle");
        files::private_dir(&bundle).unwrap();
        let runner = base.join("unit-stall.sh");
        std::fs::write(&runner, "IFS= read -r never\n").unwrap();
        let output = base.join("output");
        let result = run_with_wait(
            Args {
                node: PathBuf::from("/bin/sh"),
                runner,
                bundle_dir: bundle,
                output_dir: output.clone(),
            },
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(result.unwrap_err().0, "child_watchdog_expired");
        assert!(!output.join("evidence/native/native-proof.json").exists());
        assert!(!output.join("evidence/native/quota-ledger.json").exists());
        let deadline = Instant::now() + FINAL_WAIT;
        let writer = loop {
            match SqliteStore::open(output.join("state")) {
                Ok(writer) => break writer,
                Err(StoreError::AlreadyOwned) if Instant::now() < deadline => {
                    tokio::task::yield_now().await
                }
                Err(_) => panic!("failed fixture cleanup must release its actual writer"),
            }
        };
        assert_eq!(writer.owner_generation(), 2);
        assert!(writer.readiness().is_ok());
        drop(writer);
        std::fs::remove_dir_all(base).unwrap();
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod native_tests;
