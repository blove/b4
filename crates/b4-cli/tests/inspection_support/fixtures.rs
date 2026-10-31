use super::Sandbox;
use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::{Runtime, Store, StoredExecution, ThreadOrigin, ThreadSeed};
use b4_sqlite::SqliteStore;
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type FixtureRuntime = Runtime<Arc<SqliteStore>>;

pub fn private_modes(data: &Path) {
    std::fs::set_permissions(data, std::fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["owner.lock", "runtime.sqlite3"] {
        std::fs::set_permissions(data.join(name), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert_eq!(std::fs::read_dir(data).unwrap().count(), 2);
}

fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: "private-application-secret".into(),
        tenant: "private-tenant-secret".into(),
        principal: "private-principal-secret".into(),
    }
}

fn context() -> ApprovalContext {
    ApprovalContext {
        actor: origin(),
        policy_revision: "private-policy-revision-secret".into(),
    }
}

fn task(number: usize, recovery: RecoveryPolicy, requires_approval: bool) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(format!("private-task-{number}-secret")).unwrap(),
        effect_id: EffectId::new(format!("private-effect-{number}-secret")).unwrap(),
        recovery,
        requires_approval,
    }
}

fn admitted(
    runtime: &FixtureRuntime,
    number: usize,
    batches: Vec<Vec<TaskSpec>>,
) -> StoredExecution {
    let thread = ThreadId::new(format!("private-thread-{number:03}-secret")).unwrap();
    runtime
        .store()
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 101,
            metadata: Map::from_iter([(
                "private-metadata-secret".into(),
                json!("private-thread-value-secret"),
            )]),
            origin: Some(origin()),
        })
        .expect("real thread fixture must be accepted");
    let turn = TurnId::new(format!("private-turn-{number:03}-secret")).unwrap();
    runtime
        .admit(
            &thread,
            Snapshot {
                schema_version: SNAPSHOT_VERSION,
                revision: 0,
                execution_id: ExecutionId::new(format!("private-execution-{number:03}-secret"))
                    .unwrap(),
                turn_id: turn.clone(),
                turn_history: vec![turn],
                attempt_history: vec![],
                owner_generation: runtime.store().owner_generation(),
                definition: Definition {
                    route: RouteKey::new("/private-route-secret#workflow").unwrap(),
                    revision: ExecutionRevision::new("private-definition-revision-secret").unwrap(),
                    batches,
                    max_attempts: 32,
                },
                status: ExecutionStatus::Accepted,
                batch_index: 0,
                values: Map::from_iter([(
                    "private-input-key-secret".into(),
                    json!("private-input-value-secret"),
                )]),
                tasks: vec![],
                attempts_used: 0,
                updated_at_ms: 102,
                output: None,
                failure: None,
            },
        )
        .expect("real Accepted snapshot fixture must be admitted")
}

fn apply(runtime: &FixtureRuntime, execution: &mut StoredExecution, command: KernelCommand) {
    *execution = runtime
        .apply(
            &execution.snapshot.execution_id,
            (&execution.snapshot).into(),
            command,
            TransitionFacts {
                now_ms: execution.snapshot.updated_at_ms + 1,
            },
        )
        .expect("real fixture transition must satisfy Store and kernel validation");
}

fn dispatch(runtime: &FixtureRuntime, execution: &mut StoredExecution) -> AttemptId {
    let attempt = AttemptId::new(format!(
        "private-attempt-{}-secret",
        execution.snapshot.execution_id.as_str()
    ))
    .unwrap();
    apply(
        runtime,
        execution,
        KernelCommand::Dispatch {
            task_id: execution.snapshot.tasks[0].spec.id.clone(),
            attempt_id: attempt.clone(),
        },
    );
    attempt
}

struct FixtureAuthority;

impl ApprovalAuthority for FixtureAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if resource.thread.origin.as_ref() == Some(&context.actor) {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}

fn approval_fence(runtime: &FixtureRuntime, execution: &StoredExecution) -> ApprovalFence {
    ApprovalFence {
        execution: (&execution.snapshot).into(),
        resource_revision: runtime
            .store()
            .thread(&execution.thread_id)
            .unwrap()
            .unwrap()
            .resource_revision,
    }
}

fn park(runtime: &FixtureRuntime, execution: &mut StoredExecution) {
    let service = ApprovalService::new(runtime.store().clone(), FixtureAuthority).unwrap();
    *execution = service
        .park(
            &context(),
            &execution.snapshot.execution_id,
            approval_fence(runtime, execution),
            execution.snapshot.tasks[0].spec.id.clone(),
            InterruptId::new(format!(
                "private-interrupt-{}-secret",
                execution.snapshot.execution_id.as_str()
            ))
            .unwrap(),
            json!({"private-prompt-key-secret": "private-prompt-value-secret"}),
            10_000,
            TransitionFacts {
                now_ms: execution.snapshot.updated_at_ms + 1,
            },
        )
        .expect("real durable grant must be issued");
}

fn deny(runtime: &FixtureRuntime, execution: &mut StoredExecution) {
    let service = ApprovalService::new(runtime.store().clone(), FixtureAuthority).unwrap();
    let fence = approval_fence(runtime, execution);
    let pending = service
        .pending(&context(), &execution.snapshot.execution_id, fence)
        .unwrap();
    *execution = service
        .accept(
            &execution.snapshot.execution_id,
            fence,
            ApprovalAcceptanceRequest {
                context: context(),
                route: execution.snapshot.definition.route.clone(),
                definition_revision: execution.snapshot.definition.revision.clone(),
                turn_id: TurnId::new("private-acceptance-turn-secret").unwrap(),
                now_ms: execution.snapshot.updated_at_ms + 1,
                decisions: pending
                    .into_iter()
                    .map(|item| GrantDecision {
                        interrupt_id: item.interrupt_id,
                        decision: ApprovalDecision::Deny,
                        cancelled: false,
                        token: Some(item.token),
                    })
                    .collect(),
            },
        )
        .expect("real durable denial acceptance must be recorded");
}

pub struct RichFixture {
    pub data: PathBuf,
    pub secrets: BTreeSet<String>,
}

fn collect_secrets(value: &Value, secrets: &mut BTreeSet<String>) {
    match value {
        Value::String(text) if text.len() >= 8 => {
            secrets.insert(text.clone());
        }
        Value::Array(items) => items.iter().for_each(|item| collect_secrets(item, secrets)),
        Value::Object(properties) => {
            for (key, item) in properties {
                if key.starts_with("private-") {
                    secrets.insert(key.clone());
                }
                collect_secrets(item, secrets);
            }
        }
        _ => {}
    }
}

pub fn rich_fixture(sandbox: &Sandbox) -> RichFixture {
    let data = sandbox.0.join("private-rich-state");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&data)
        .unwrap();
    let runtime = Runtime::new(Arc::new(SqliteStore::open(&data).unwrap()));
    let mut executions = Vec::new();
    executions.push(admitted(
        &runtime,
        1,
        vec![
            vec![task(1, RecoveryPolicy::Pure, false)],
            vec![task(11, RecoveryPolicy::Unsafe, false)],
        ],
    ));
    let mut running = admitted(
        &runtime,
        2,
        vec![vec![task(2, RecoveryPolicy::Pure, false)]],
    );
    apply(&runtime, &mut running, KernelCommand::Start);
    executions.push(running);
    let mut waiting = admitted(
        &runtime,
        3,
        vec![vec![task(3, RecoveryPolicy::Idempotent, true)]],
    );
    apply(&runtime, &mut waiting, KernelCommand::Start);
    park(&runtime, &mut waiting);
    executions.push(waiting);
    let mut completed = admitted(
        &runtime,
        4,
        vec![vec![task(4, RecoveryPolicy::Reconcilable, false)]],
    );
    apply(&runtime, &mut completed, KernelCommand::Start);
    let attempt = dispatch(&runtime, &mut completed);
    let id = completed.snapshot.tasks[0].spec.id.clone();
    apply(
        &runtime,
        &mut completed,
        KernelCommand::Complete {
            task_id: id,
            attempt_id: attempt,
            result: TaskResult {
                output: json!("private-output-secret"),
                writes: vec![StateWrite::Append {
                    field: "private-write-key-secret".into(),
                    items: vec![json!("private-write-value-secret")],
                }],
            },
        },
    );
    executions.push(completed);
    let mut failed = admitted(
        &runtime,
        5,
        vec![vec![task(5, RecoveryPolicy::Unsafe, false)]],
    );
    apply(&runtime, &mut failed, KernelCommand::Start);
    let attempt = dispatch(&runtime, &mut failed);
    let id = failed.snapshot.tasks[0].spec.id.clone();
    apply(
        &runtime,
        &mut failed,
        KernelCommand::FailAttempt {
            task_id: id,
            attempt_id: attempt,
            failure: "private-failure-secret".into(),
        },
    );
    executions.push(failed);
    let mut cancelling = admitted(
        &runtime,
        6,
        vec![vec![task(6, RecoveryPolicy::Pure, false)]],
    );
    apply(&runtime, &mut cancelling, KernelCommand::Start);
    dispatch(&runtime, &mut cancelling);
    let cancel = KernelCommand::Cancel {
        turn_id: cancelling.snapshot.turn_id.clone(),
        owner_generation: cancelling.snapshot.owner_generation,
    };
    apply(&runtime, &mut cancelling, cancel);
    executions.push(cancelling);
    let mut cancelled = admitted(
        &runtime,
        7,
        vec![vec![task(7, RecoveryPolicy::Idempotent, false)]],
    );
    let cancel = KernelCommand::Cancel {
        turn_id: cancelled.snapshot.turn_id.clone(),
        owner_generation: cancelled.snapshot.owner_generation,
    };
    apply(&runtime, &mut cancelled, cancel);
    executions.push(cancelled);
    let mut unknown = admitted(
        &runtime,
        8,
        vec![vec![task(8, RecoveryPolicy::Unsafe, false)]],
    );
    apply(&runtime, &mut unknown, KernelCommand::Start);
    dispatch(&runtime, &mut unknown);
    let mut denied = admitted(
        &runtime,
        9,
        vec![vec![task(9, RecoveryPolicy::Reconcilable, true)]],
    );
    apply(&runtime, &mut denied, KernelCommand::Start);
    park(&runtime, &mut denied);
    deny(&runtime, &mut denied);
    executions.push(denied);
    drop(runtime);
    let runtime = Runtime::new(Arc::new(SqliteStore::open(&data).unwrap()));
    unknown = runtime
        .recover(
            &unknown.snapshot.execution_id,
            (&unknown.snapshot).into(),
            unknown.snapshot.definition.revision.clone(),
            TurnId::new("private-recovery-turn-secret").unwrap(),
            TransitionFacts {
                now_ms: unknown.snapshot.updated_at_ms + 1,
            },
        )
        .expect("real unsafe recovery must record uncertainty");
    executions.push(unknown);
    for (number, expected) in [
        (1, ExecutionStatus::Accepted),
        (2, ExecutionStatus::Running),
        (3, ExecutionStatus::WaitingForApproval),
        (4, ExecutionStatus::Completed),
        (5, ExecutionStatus::Failed),
        (6, ExecutionStatus::CancelRequested),
        (7, ExecutionStatus::Cancelled),
        (8, ExecutionStatus::RecoveryBlocked),
        (9, ExecutionStatus::Completed),
    ] {
        let execution = executions
            .iter()
            .find(|execution| {
                execution.snapshot.execution_id.as_str()
                    == format!("private-execution-{number:03}-secret")
            })
            .unwrap();
        assert!(
            execution.snapshot.status == expected,
            "rich fixture must persist its independently specified execution state"
        );
    }
    let mut secrets = BTreeSet::new();
    for execution in &executions {
        collect_secrets(&serde_json::to_value(execution).unwrap(), &mut secrets);
        if let Some(view) = runtime
            .store()
            .approval_view(&execution.snapshot.execution_id)
            .unwrap()
        {
            collect_secrets(&serde_json::to_value(view.thread).unwrap(), &mut secrets);
            collect_secrets(&serde_json::to_value(view.grants).unwrap(), &mut secrets);
            collect_secrets(
                &serde_json::to_value(view.acceptances).unwrap(),
                &mut secrets,
            );
        }
    }
    // Safe wire enum names are intentional report contents, not sensitive values.
    for wire in [
        "accepted",
        "running",
        "waiting_for_approval",
        "completed",
        "failed",
        "cancel_requested",
        "cancelled",
        "recovery_blocked",
        "ready",
        "in_flight",
        "succeeded",
        "denied",
        "outcome_unknown",
        "pure",
        "idempotent",
        "reconcilable",
        "unsafe",
    ] {
        secrets.remove(wire);
    }
    drop(runtime);
    private_modes(&data);
    let journal_rows: i64 = super::raw_connection(&data)
        .query_row("SELECT COUNT(*) FROM journal", [], |row| row.get(0))
        .unwrap();
    assert!(
        journal_rows == 37,
        "real rich fixture must record the hand-counted 37 durable journal rows"
    );
    assert!(
        secrets.iter().any(|secret| secret.starts_with("b4ag_")),
        "real fixture must contain issued sensitive approval authority"
    );
    RichFixture { data, secrets }
}

pub fn accepted_fixture(sandbox: &Sandbox, count: usize) -> PathBuf {
    let data = sandbox.0.join(format!("private-accepted-{count}"));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&data)
        .unwrap();
    let runtime = Runtime::new(Arc::new(SqliteStore::open(&data).unwrap()));
    for number in 0..count {
        admitted(
            &runtime,
            number,
            vec![vec![task(number, RecoveryPolicy::Pure, false)]],
        );
    }
    drop(runtime);
    private_modes(&data);
    data
}

pub fn terminal_history_fixture(sandbox: &Sandbox) -> PathBuf {
    let data = sandbox.0.join("private-terminal-history");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&data)
        .unwrap();
    let runtime = Runtime::new(Arc::new(SqliteStore::open(&data).unwrap()));
    let mut completed = admitted(&runtime, 1, vec![]);
    apply(&runtime, &mut completed, KernelCommand::Start);
    assert!(
        completed.snapshot.status == ExecutionStatus::Completed,
        "first real history execution must settle terminal"
    );
    let mut initial = completed.snapshot.clone();
    initial.execution_id = ExecutionId::new("private-history-second-execution-secret").unwrap();
    initial.turn_id = TurnId::new("private-history-second-turn-secret").unwrap();
    initial.turn_history = vec![initial.turn_id.clone()];
    initial.revision = 0;
    initial.status = ExecutionStatus::Accepted;
    initial.output = None;
    initial.definition.batches = vec![vec![task(2, RecoveryPolicy::Pure, false)]];
    runtime
        .admit(&completed.thread_id, initial)
        .expect("a real subsequent admission must retain prior terminal history");
    assert!(
        runtime
            .store()
            .thread(&completed.thread_id)
            .unwrap()
            .unwrap()
            .active_execution_id
            .as_ref()
            != Some(&completed.snapshot.execution_id),
        "terminal history fixture must no longer be the active execution"
    );
    drop(runtime);
    private_modes(&data);
    data
}

pub fn failure_drain_fixture(sandbox: &Sandbox) -> PathBuf {
    let data = sandbox.0.join("private-failure-drain");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&data)
        .unwrap();
    let runtime = Runtime::new(Arc::new(SqliteStore::open(&data).unwrap()));
    let mut execution = admitted(
        &runtime,
        1,
        vec![vec![
            task(1, RecoveryPolicy::Unsafe, false),
            task(2, RecoveryPolicy::Reconcilable, false),
        ]],
    );
    apply(&runtime, &mut execution, KernelCommand::Start);
    let first = dispatch(&runtime, &mut execution);
    let second_task = execution.snapshot.tasks[1].spec.id.clone();
    apply(
        &runtime,
        &mut execution,
        KernelCommand::Dispatch {
            task_id: second_task,
            attempt_id: AttemptId::new("private-drain-second-attempt-secret").unwrap(),
        },
    );
    let first_task = execution.snapshot.tasks[0].spec.id.clone();
    apply(
        &runtime,
        &mut execution,
        KernelCommand::FailAttempt {
            task_id: first_task,
            attempt_id: first,
            failure: "private-drain-failure-secret".into(),
        },
    );
    assert!(
        execution.snapshot.status == ExecutionStatus::Running
            && execution.snapshot.tasks[1].status == TaskStatus::InFlight,
        "real application failure drain must retain a Running uncertain sibling"
    );
    drop(runtime);
    private_modes(&data);
    data
}
