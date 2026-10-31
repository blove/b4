#![cfg(unix)]

use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::Connection;
use serde_json::{json, Map};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const CHILD_DIRECTORY: &str = "B4_DRIVER_CRASH_DIRECTORY";
const CHILD_SCENARIO: &str = "B4_DRIVER_CRASH_SCENARIO";
const APPROVAL_TTL: u64 = 100_000;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "b4-driver-crash-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
    fn records(&self) -> (Vec<String>, Vec<String>) {
        let connection = self.connection();
        let rows = |table| {
            connection
                .prepare(&format!("SELECT record FROM {table} ORDER BY record"))
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<String>, _>>()
                .unwrap()
        };
        (rows("approval_grants"), rows("approval_acceptances"))
    }
    fn stored(&self) -> StoredExecution {
        let snapshot: String = self
            .connection()
            .query_row(
                "SELECT snapshot FROM executions WHERE id=?1",
                [execution().as_str()],
                |row| row.get(0),
            )
            .unwrap();
        StoredExecution {
            thread_id: thread(),
            snapshot: serde_json::from_str(&snapshot).unwrap(),
        }
    }
    fn ledger(&self) -> Vec<String> {
        let bytes = match std::fs::read_to_string(self.0.join("effects.ledger")) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => panic!("Cannot read effect ledger: {error}"),
        };
        bytes.lines().map(str::to_owned).collect()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct OwnedChild(Child);
impl OwnedChild {
    fn start(directory: &Directory, scenario: &str) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "driver_crash_child_helper",
                    "--ignored",
                    "--test-threads=1",
                ])
                .env(CHILD_DIRECTORY, &directory.0)
                .env(CHILD_SCENARIO, scenario)
                // All synchronization is through synced files; child diagnostics
                // cannot dump generated approval tokens into the parent test output.
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
    fn wait_for(&mut self, directory: &Directory, marker: &str) {
        let deadline = Instant::now() + Duration::from_secs(8);
        while !directory.0.join(marker).is_file() {
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "Crash fixture exited before its synchronized boundary"
            );
            assert!(
                !directory.0.join("fault").is_file(),
                "Crash fixture faulted before its synchronized boundary"
            );
            assert!(
                Instant::now() < deadline,
                "Crash fixture did not reach its synchronized boundary"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn kill_and_wait(&mut self) {
        self.0.kill().unwrap();
        let status = self.0.wait().unwrap();
        assert_eq!(
            status.signal(),
            Some(9),
            "Fixture must stop by SIGKILL, without Rust cleanup"
        );
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn marker(directory: &Path, name: &str) {
    let file = File::create(directory.join(name)).unwrap();
    file.sync_all().unwrap();
}
fn wait_marker(directory: &Path, name: &str) {
    while !directory.join(name).is_file() {
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn hold_for_kill() -> ! {
    loop {
        std::thread::park();
    }
}
fn thread() -> ThreadId {
    ThreadId::new("crash-thread").unwrap()
}
fn execution() -> ExecutionId {
    ExecutionId::new("crash-execution").unwrap()
}
fn task(name: &str) -> TaskId {
    TaskId::new(name).unwrap()
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    }
}
fn definition(scenario: &str) -> Definition {
    let spec = |name: &str, required, recovery| TaskSpec {
        id: task(name),
        effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
        requires_approval: required,
        recovery,
    };
    let batch = match scenario {
        "sibling_park" => vec![
            spec("sibling", false, RecoveryPolicy::Pure),
            spec("approval", true, RecoveryPolicy::Pure),
        ],
        "accepted" => vec![spec("approval", true, RecoveryPolicy::Pure)],
        "unsafe" => vec![spec("unsafe", false, RecoveryPolicy::Unsafe)],
        _ => panic!("Unknown crash scenario"),
    };
    Definition {
        route: RouteKey::new("/driver-crash#workflow").unwrap(),
        revision: ExecutionRevision::new("fixture-v1").unwrap(),
        batches: vec![batch],
        max_attempts: 4,
    }
}
struct Allow;
impl ApprovalAuthority for Allow {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if resource.thread.origin.as_ref() == Some(&context().actor) {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
type Service = ApprovalService<Arc<SqliteStore>, Allow>;
fn fence(view: &ThreadView) -> ApprovalFence {
    ApprovalFence {
        execution: (&view.execution.as_ref().unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}
fn view(store: &SqliteStore) -> ThreadView {
    store.thread_view(&thread()).unwrap().unwrap()
}
fn service(store: Arc<SqliteStore>) -> Service {
    ApprovalService::new(store, Allow).unwrap()
}
fn accept(service: &Service, now_ms: u64) -> StoredExecution {
    let captured = view(service.store());
    let pending = service
        .pending(&context(), &execution(), fence(&captured))
        .unwrap();
    assert_eq!(pending.len(), 1);
    service
        .accept(
            &execution(),
            fence(&captured),
            ApprovalAcceptanceRequest {
                context: context(),
                route: captured
                    .execution
                    .as_ref()
                    .unwrap()
                    .snapshot
                    .definition
                    .route
                    .clone(),
                definition_revision: captured
                    .execution
                    .as_ref()
                    .unwrap()
                    .snapshot
                    .definition
                    .revision
                    .clone(),
                turn_id: TurnId::new(format!(
                    "accepted-owner-{}",
                    service.store().owner_generation()
                ))
                .unwrap(),
                now_ms,
                decisions: vec![GrantDecision {
                    interrupt_id: pending[0].interrupt_id.clone(),
                    decision: ApprovalDecision::Once,
                    cancelled: false,
                    token: Some(pending[0].token.clone()),
                }],
            },
        )
        .unwrap()
}
fn recover(service: &Service, scenario: &str, now_ms: u64) -> StoredExecution {
    let captured = view(service.store());
    let current = captured.execution.as_ref().unwrap();
    let request = ApprovalRecoveryRequest {
        context: context(),
        thread_id: thread(),
        thread_generation: captured.thread.generation.clone(),
        execution_id: execution(),
        available_definition: definition(scenario),
        turn_id: TurnId::new(format!(
            "recovered-owner-{}",
            service.store().owner_generation()
        ))
        .unwrap(),
        facts: TransitionFacts { now_ms },
    };
    let mut stale = request.clone();
    stale.thread_generation =
        ThreadGeneration::new(if captured.thread.generation.as_str().starts_with('0') {
            "1".repeat(64)
        } else {
            "0".repeat(64)
        })
        .unwrap();
    assert_eq!(
        service.recover(&execution(), fence(&captured), stale),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    );
    assert_eq!(
        service.store().execution(&execution()).unwrap().as_ref(),
        Some(current)
    );
    service
        .recover(&execution(), fence(&captured), request)
        .unwrap()
}

struct Host {
    store: Arc<SqliteStore>,
    directory: PathBuf,
    checkpoint: &'static str,
    clock: AtomicU64,
    attempts: AtomicUsize,
    faulted: AtomicBool,
}
impl Host {
    fn new(
        store: Arc<SqliteStore>,
        directory: &Path,
        checkpoint: &'static str,
        now_ms: u64,
    ) -> Self {
        Self {
            store,
            directory: directory.into(),
            checkpoint,
            clock: AtomicU64::new(now_ms),
            attempts: AtomicUsize::new(0),
            faulted: AtomicBool::new(false),
        }
    }
    fn stable_checkpoint(&self, stored: &StoredExecution) {
        if self.checkpoint == "sibling_park"
            && stored.snapshot.status == ExecutionStatus::WaitingForApproval
        {
            assert_eq!(stored.snapshot.tasks[0].status, TaskStatus::Succeeded);
            marker(&self.directory, "stable-park");
            hold_for_kill();
        }
    }
}
impl DriverHost for Host {
    type Error = ApprovalError;
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        TransitionFacts {
            now_ms: self
                .clock
                .fetch_add(1, Ordering::AcqRel)
                .max(current.snapshot.updated_at_ms + 1),
        }
    }
    fn attempt_id(&self) -> AttemptId {
        AttemptId::new(format!(
            "attempt-owner-{}-{}",
            self.store.owner_generation(),
            self.attempts.fetch_add(1, Ordering::AcqRel)
        ))
        .unwrap()
    }
    fn prepare_dispatch(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<ApprovalError>> {
        let captured = view(&self.store);
        let prepared = service(self.store.clone()).prepare_dispatch(
            &context(),
            &execution(),
            fence(&captured),
            task_id,
            attempt_id,
            self.facts(current),
        )?;
        if self.checkpoint == "accepted" {
            assert!(prepared.execution.snapshot.tasks[0].approved);
            assert_eq!(
                prepared.execution.snapshot.tasks[0].status,
                TaskStatus::InFlight
            );
            marker(&self.directory, "accepted-before-entry");
            hold_for_kill();
        }
        Ok(prepared)
    }
    fn park(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
    ) -> Result<StoredExecution, ApprovalError> {
        if self.checkpoint == "sibling_park" {
            wait_marker(&self.directory, "sibling-entered");
        }
        let captured = view(&self.store);
        let parked = service(self.store.clone()).park(
            &context(),
            &execution(),
            fence(&captured),
            task_id,
            InterruptId::new("approval-interrupt").unwrap(),
            json!({"question":"Approve local fixture?"}),
            APPROVAL_TTL,
            self.facts(current),
        )?;
        if self.checkpoint == "sibling_park" {
            assert_eq!(parked.snapshot.tasks[0].status, TaskStatus::InFlight);
            marker(&self.directory, "partial-park");
        }
        self.stable_checkpoint(&parked);
        Ok(parked)
    }
    fn invoke(&self, invocation: &EffectInvocation) -> Result<EffectOutcome, String> {
        assert_eq!(invocation.input["input"], json!({"fixture":"original"}));
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("effects.ledger"))
            .unwrap();
        writeln!(
            file,
            "{}|{}|{}",
            invocation.task_id.as_str(),
            invocation.attempt_id.as_str(),
            invocation.owner_generation
        )
        .unwrap();
        file.sync_all().unwrap();
        if self.checkpoint == "sibling_park" {
            marker(&self.directory, "sibling-entered");
            wait_marker(&self.directory, "release-sibling");
        }
        if self.checkpoint == "unsafe" {
            marker(&self.directory, "unsafe-before-result");
            hold_for_kill();
        }
        Ok(EffectOutcome::Succeeded(TaskResult {
            output: json!(invocation.task_id.as_str()),
            writes: vec![StateWrite::Append {
                field: "items".into(),
                items: vec![json!(invocation.task_id.as_str())],
            }],
        }))
    }
    fn after_commit(
        &self,
        _: &EffectInvocation,
        _: &EffectOutcome,
        current: &StoredExecution,
    ) -> Result<(), ApprovalError> {
        self.stable_checkpoint(current);
        Ok(())
    }
    fn on_fault(&self) {
        self.faulted.store(true, Ordering::Release);
        marker(&self.directory, "fault");
    }
}
fn drive(
    store: Arc<SqliteStore>,
    current: StoredExecution,
    directory: &Path,
    checkpoint: &'static str,
    now_ms: u64,
) -> Result<StoredExecution, DriverFailure<ApprovalError>> {
    let host = Host::new(store.clone(), directory, checkpoint, now_ms);
    drive_execution(
        &Runtime::new(store),
        current,
        &host,
        &TaskCapacity::new(2).unwrap(),
        DriverConfig::default(),
    )
}
fn assert_locked(directory: &Directory) {
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
}

#[test]
fn sigkill_after_parallel_sibling_and_park_preserves_grant_and_never_replays_sibling() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "sibling_park");
    child.wait_for(&directory, "partial-park");
    assert_locked(&directory);
    let partial = directory.stored();
    assert_eq!(partial.snapshot.status, ExecutionStatus::Running);
    assert_eq!(partial.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        partial.snapshot.tasks[1].status,
        TaskStatus::WaitingForApproval
    );
    assert_eq!(directory.ledger().len(), 1);
    marker(&directory.0, "release-sibling");
    child.wait_for(&directory, "stable-park");
    let before = directory.stored();
    assert_eq!(before.snapshot.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(before.snapshot.attempts_used, 1);
    assert_eq!(before.snapshot.values["items"], json!([]));
    let authority = directory.records();
    assert_eq!(authority.0.len(), 1);
    assert!(authority.1.is_empty());
    child.kill_and_wait();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    assert_eq!(store.execution(&execution()).unwrap(), Some(before.clone()));
    let service = service(store.clone());
    let captured = view(&store);
    let original_records = store.approval_view(&execution()).unwrap().unwrap();
    let pending = service.attachment_pending(&context(), &captured).unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        directory.records() == authority,
        "Read-only pending changed authority rows"
    );
    let recovered = recover(&service, "sibling_park", 500);
    assert_eq!(
        recovered.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(recovered.snapshot.tasks[0], before.snapshot.tasks[0]);
    assert!(
        directory.records() == authority,
        "Recovery changed grant or audit records"
    );
    assert!(store.approval_view(&execution()).unwrap().unwrap().grants == original_records.grants);
    let accepted = accept(&service, 600);
    let accepted_records = directory.records();
    let completed = drive(store.clone(), accepted, &directory.0, "none", 700).unwrap();
    assert_eq!(completed.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(completed.snapshot.attempts_used, 2);
    assert_eq!(completed.snapshot.tasks[0], before.snapshot.tasks[0]);
    assert_eq!(
        completed.snapshot.values["items"],
        json!(["sibling", "approval"])
    );
    assert!(
        directory.records() == accepted_records,
        "Continuation changed accepted authority"
    );
    let ledger = directory.ledger();
    assert_eq!(ledger.len(), 2);
    assert_eq!(
        ledger
            .iter()
            .filter(|line| line.starts_with("sibling|"))
            .count(),
        1
    );
    assert_eq!(
        ledger
            .iter()
            .filter(|line| line.starts_with("approval|"))
            .count(),
        1
    );
    store.readiness().unwrap();
}

#[test]
fn sigkill_after_once_before_callback_entry_keeps_receipt_budget_and_expired_continuation() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "accepted");
    child.wait_for(&directory, "accepted-before-entry");
    assert_locked(&directory);
    let before = directory.stored();
    assert_eq!(before.snapshot.status, ExecutionStatus::Running);
    assert_eq!(before.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(before.snapshot.tasks[0].approved);
    assert_eq!(before.snapshot.attempts_used, 1);
    assert!(
        !directory.0.join("effects.ledger").exists(),
        "A callback entered before the accepted checkpoint"
    );
    assert!(directory.ledger().is_empty());
    let authority = directory.records();
    assert_eq!(authority.0.len(), 1);
    assert_eq!(authority.1.len(), 1);
    let grant: GrantRecord = serde_json::from_str(&authority.0[0]).unwrap();
    assert!(grant.consumed);
    assert!(grant.expires_at_ms < 2_000_000);
    child.kill_and_wait();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    assert_eq!(store.execution(&execution()).unwrap(), Some(before.clone()));
    let recovered = recover(&service(store.clone()), "accepted", 2_000_000);
    assert_eq!(recovered.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(recovered.snapshot.attempts_used, 1);
    assert_eq!(
        recovered.snapshot.attempt_history,
        before.snapshot.attempt_history
    );
    assert!(
        directory.records() == authority,
        "Recovery changed consumed Once or audit"
    );
    let completed = drive(store.clone(), recovered, &directory.0, "none", 2_000_010).unwrap();
    assert_eq!(completed.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(completed.snapshot.attempts_used, 2);
    assert_eq!(
        completed.snapshot.attempt_history[0],
        before.snapshot.attempt_history[0]
    );
    assert_eq!(directory.ledger().len(), 1);
    assert!(directory.ledger()[0].starts_with("approval|attempt-owner-2-"));
    assert!(
        directory.records() == authority,
        "Expired accepted continuation reissued or rewrote authority"
    );
    store.readiness().unwrap();
}

#[test]
fn sigkill_after_unsafe_effect_before_result_blocks_recovery_without_replay() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "unsafe");
    child.wait_for(&directory, "unsafe-before-result");
    assert_locked(&directory);
    let before = directory.stored();
    assert_eq!(before.snapshot.status, ExecutionStatus::Running);
    assert_eq!(before.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(before.snapshot.tasks[0].result.is_none());
    assert_eq!(before.snapshot.attempts_used, 1);
    let ledger = directory.ledger();
    assert_eq!(ledger.len(), 1);
    assert!(ledger[0].starts_with("unsafe|attempt-owner-1-"));
    child.kill_and_wait();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    assert_eq!(store.execution(&execution()).unwrap(), Some(before));
    let blocked = recover(&service(store.clone()), "unsafe", 500);
    assert_eq!(blocked.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(blocked.snapshot.tasks[0].status, TaskStatus::OutcomeUnknown);
    assert_eq!(blocked.snapshot.attempts_used, 1);
    let attempted = drive(store.clone(), blocked.clone(), &directory.0, "none", 600);
    match attempted {
        Err(failure) => {
            assert!(matches!(
                failure.errors.as_slice(),
                [DriverError::RecoveryBlocked]
            ));
            assert!(failure.retained_outcomes.is_empty());
            assert!(failure.unsettled_invocations.is_empty());
        }
        Ok(_) => panic!("RecoveryBlocked must refuse execution"),
    }
    assert_eq!(store.execution(&execution()).unwrap(), Some(blocked));
    assert!(
        directory.ledger() == ledger,
        "Unsafe recovery replayed the durable effect"
    );
    store.readiness().unwrap();
}

#[test]
#[ignore = "Separate-process crash helper, invoked by the three parent tests"]
fn driver_crash_child_helper() {
    let directory =
        PathBuf::from(std::env::var_os(CHILD_DIRECTORY).expect("Crash fixture directory"));
    let scenario = std::env::var(CHILD_SCENARIO).expect("Crash fixture scenario");
    let checkpoint = match scenario.as_str() {
        "sibling_park" => "sibling_park",
        "accepted" => "accepted",
        "unsafe" => "unsafe",
        _ => panic!("Unknown crash fixture scenario"),
    };
    let store = Arc::new(SqliteStore::open(&directory).unwrap());
    store
        .create_thread_with(ThreadSeed {
            id: thread(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let admitted = store
        .admit(
            &thread(),
            b4_kernel::new_execution(
                execution(),
                TurnId::new("original-turn").unwrap(),
                store.owner_generation(),
                definition(checkpoint),
                Map::from_iter([
                    ("input".into(), json!({"fixture":"original"})),
                    ("items".into(), json!([])),
                ]),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let first = drive(store.clone(), admitted, &directory, checkpoint, 2);
    assert!(
        first.is_ok(),
        "Crash fixture driver failed before its checkpoint"
    );
    if checkpoint == "accepted" {
        assert_eq!(
            first.unwrap().snapshot.status,
            ExecutionStatus::WaitingForApproval
        );
        let accepted = accept(&service(store.clone()), 500);
        let continued = drive(store, accepted, &directory, checkpoint, 501);
        assert!(
            continued.is_ok(),
            "Accepted crash fixture driver failed before its checkpoint"
        );
    }
    panic!("Crash fixture returned without reaching its checkpoint");
}
