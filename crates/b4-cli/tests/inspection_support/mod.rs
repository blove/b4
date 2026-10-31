use b4_sqlite::SqliteStore;
use serde_json::{json, Value};
use std::fs::{DirBuilder, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

mod fixtures;
mod preservation;
mod reports;
pub use fixtures::*;
pub use preservation::*;
pub use reports::*;

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct Sandbox(pub PathBuf);

impl Sandbox {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-inspection-process-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }

    pub fn clean_store(&self, name: &str) -> PathBuf {
        let data = self.0.join(name);
        DirBuilder::new().mode(0o700).create(&data).unwrap();
        drop(SqliteStore::open(&data).expect("real fixture Store must open and retire"));
        for name in ["runtime.sqlite3", "owner.lock"] {
            std::fs::set_permissions(
                data.join(name),
                std::os::unix::fs::PermissionsExt::from_mode(0o600),
            )
            .unwrap();
        }
        assert_eq!(
            std::fs::read_dir(&data).unwrap().count(),
            2,
            "normally retired Store fixture must have no sidecars"
        );
        data
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn command(data: &Path) -> Command {
    let binary = std::env::var_os("B4RUN_QUALIFICATION_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_b4run").into());
    let mut command = Command::new(binary);
    command.env_clear().env("B4RUN_DATA_DIR", data);
    command
}

pub struct Captured {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub struct ChildGuard(pub Option<Child>);

impl ChildGuard {
    pub fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            if let Some(status) = self.0.as_mut().unwrap().try_wait().unwrap() {
                self.0.take();
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "actual inspection subprocess must exit within qualification bound"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            // Forced termination is failure cleanup, never a retirement witness.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub fn private_file(sandbox: &Sandbox, label: &str) -> (PathBuf, File) {
    let path = sandbox
        .0
        .join(format!("{label}-{}", NEXT.fetch_add(1, Ordering::Relaxed)));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    (path, file)
}

pub fn run(command: &mut Command, sandbox: &Sandbox) -> Captured {
    let (stdout, stdout_file) = private_file(sandbox, "stdout");
    let (stderr, stderr_file) = private_file(sandbox, "stderr");
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    let mut child = ChildGuard(Some(command.spawn().unwrap()));
    Captured {
        status: child.wait(),
        stdout: std::fs::read(stdout).unwrap(),
        stderr: std::fs::read(stderr).unwrap(),
    }
}

pub fn empty_report() -> Value {
    json!({
        "inspection_report_version": 2,
        "mode": "stopped_readonly",
        "redacted": true,
        "reader": {
            "package_version": "0.1.0",
            "sqlite_library_version": "3.53.2",
            "snapshot_version": 1,
            "store_schema_version": 4,
            "target_profile": "b4-ap-core/0",
            "profile_status": "native_subset",
            "capability_descriptor_versions": [1, 2, 3],
            "core_profile_frozen": false
        },
        "validation_scope": "schema_structure_execution_snapshots_and_bounded_permission_records",
        "table_rows": {
            "metadata": 1, "threads": 0, "executions": 0, "journal": 0,
            "approval_grants": 0, "approval_acceptances": 0,
            "permission_acceptances": 0, "permission_decisions": 0, "permission_uses": 0
        },
        "permission_authority": {"human_always_decisions": 0, "token_free_uses": 0},
        "executions_by_status": {
            "accepted": 0, "running": 0, "waiting_for_approval": 0,
            "completed": 0, "failed": 0, "cancel_requested": 0,
            "cancelled": 0, "recovery_blocked": 0
        },
        "initialized_tasks_by_status": {
            "ready": 0, "waiting_for_approval": 0, "in_flight": 0,
            "succeeded": 0, "failed": 0, "denied": 0, "cancelled": 0,
            "outcome_unknown": 0
        },
        "declared_definition_tasks_by_recovery_policy": {
            "pure": 0, "idempotent": 0, "reconcilable": 0, "unsafe": 0
        },
        "recorded_uncertain_tasks_by_policy": {
            "pure": 0, "idempotent": 0, "reconcilable": 0, "unsafe": 0
        },
        "blocked": null
    })
}

pub fn successful_report(output: &Captured, expected: &Value) {
    assert!(
        output.status.code() == Some(0),
        "clean stopped Store inspection must emit the expected report with exit 0"
    );
    assert!(
        output.stderr.is_empty(),
        "successful report has no diagnostic"
    );
    assert!(
        output.stdout.ends_with(b"\n") && output.stdout.len() <= 65_537,
        "successful report must be bounded and newline terminated"
    );
    let value: Value = serde_json::from_slice(&output.stdout)
        .expect("successful inspection output must be one complete JSON object");
    assert!(
        value == *expected,
        "report must match exact fixed public contract"
    );
    assert_report_schema(&value);
}
