use b4_contract::*;
use b4_kernel::new_execution;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-runtime-integration-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn id() -> ExecutionId {
    ExecutionId::new("execution").unwrap()
}
fn task() -> TaskId {
    TaskId::new("task").unwrap()
}
fn attempt(id: &str) -> AttemptId {
    AttemptId::new(id).unwrap()
}
fn facts() -> TransitionFacts {
    TransitionFacts { now_ms: 100 }
}
fn result() -> TaskResult {
    TaskResult {
        output: json!("ok"),
        writes: vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!("ok")],
        }],
    }
}
fn setup(
    dir: &Directory,
    policy: RecoveryPolicy,
    approval: bool,
) -> (Runtime<SqliteStore>, StoredExecution) {
    let rt = Runtime::new(SqliteStore::open(&dir.0).unwrap());
    let thread = ThreadId::new("thread").unwrap();
    rt.store().create_thread(thread.clone()).unwrap();
    let initial = new_execution(
        id(),
        TurnId::new("turn-1").unwrap(),
        rt.store().owner_generation(),
        Definition {
            route: RouteKey::new("/test#agent").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task(),
                effect_id: EffectId::new("effect").unwrap(),
                requires_approval: approval,
                recovery: policy,
            }]],
            max_attempts: 3,
        },
        Map::new(),
        facts(),
    )
    .unwrap();
    let accepted = rt.admit(&thread, initial).unwrap();
    let running = rt
        .apply(
            &id(),
            ExecutionFence::from(&accepted.snapshot),
            KernelCommand::Start,
            facts(),
        )
        .unwrap();
    (rt, running)
}

#[test]
fn reopened_unsafe_attempt_blocks_and_rejects_old_owner_completion() {
    let dir = Directory::new();
    let (rt, running) = setup(&dir, RecoveryPolicy::Unsafe, false);
    let error = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&running.snapshot),
            task(),
            attempt("attempt-1"),
            facts(),
            facts,
            |_| Err("response lost after effect".into()),
        )
        .unwrap_err();
    assert!(matches!(error, RuntimeError::EffectFailed(_)));
    let uncertain = rt.store().execution(&id()).unwrap().unwrap();
    drop(rt);
    let rt = Runtime::new(SqliteStore::open(&dir.0).unwrap());
    assert_eq!(rt.store().owner_generation(), 2);
    assert_eq!(
        rt.complete(&id(), 1, task(), attempt("attempt-1"), &result(), facts())
            .unwrap_err(),
        RuntimeError::Store(StoreError::OwnerConflict)
    );
    let recovered = rt
        .recover(
            &id(),
            ExecutionFence::from(&uncertain.snapshot),
            ExecutionRevision::new("v1").unwrap(),
            TurnId::new("turn-2").unwrap(),
            facts(),
        )
        .unwrap();
    assert_eq!(recovered.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(
        recovered.snapshot.tasks[0].status,
        TaskStatus::OutcomeUnknown
    );
    assert_eq!(recovered.snapshot.owner_generation, 2);
    assert_eq!(
        rt.complete(&id(), 1, task(), attempt("attempt-1"), &result(), facts())
            .unwrap_err(),
        RuntimeError::Store(StoreError::OwnerConflict)
    );
    assert_eq!(rt.store().execution(&id()).unwrap().unwrap(), recovered);
}

#[test]
fn reopened_pure_attempt_reuses_effect_identity_with_a_fresh_attempt() {
    let dir = Directory::new();
    let (rt, running) = setup(&dir, RecoveryPolicy::Pure, false);
    let in_flight = rt
        .apply(
            &id(),
            ExecutionFence::from(&running.snapshot),
            KernelCommand::Dispatch {
                task_id: task(),
                attempt_id: attempt("attempt-1"),
            },
            facts(),
        )
        .unwrap();
    drop(rt);
    let rt = Runtime::new(SqliteStore::open(&dir.0).unwrap());
    let recovered = rt
        .recover(
            &id(),
            ExecutionFence::from(&in_flight.snapshot),
            ExecutionRevision::new("v1").unwrap(),
            TurnId::new("turn-2").unwrap(),
            facts(),
        )
        .unwrap();
    assert_eq!(recovered.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(recovered.snapshot.attempts_used, 1);
    let done = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&recovered.snapshot),
            task(),
            attempt("attempt-2"),
            facts(),
            facts,
            |call| {
                assert_eq!(call.effect_id.as_str(), "effect");
                assert_eq!(call.attempt_id.as_str(), "attempt-2");
                assert_eq!(call.owner_generation, 2);
                Ok(result())
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(done.snapshot.attempts_used, 2);
    drop(rt);
    let reopened = SqliteStore::open(&dir.0).unwrap();
    assert_eq!(reopened.execution(&id()).unwrap().unwrap(), done);
}

#[test]
fn parked_prompt_survives_reopen_and_authorized_resume() {
    let dir = Directory::new();
    let (rt, running) = setup(&dir, RecoveryPolicy::Unsafe, true);
    let parked = rt
        .apply(
            &id(),
            ExecutionFence::from(&running.snapshot),
            KernelCommand::Park {
                task_id: task(),
                interrupt_id: InterruptId::new("approval").unwrap(),
                prompt: json!({"tool":"fixture"}),
            },
            facts(),
        )
        .unwrap();
    drop(rt);
    let rt = Runtime::new(SqliteStore::open(&dir.0).unwrap());
    let recovered = rt
        .recover(
            &id(),
            ExecutionFence::from(&parked.snapshot),
            ExecutionRevision::new("v1").unwrap(),
            TurnId::new("turn-2").unwrap(),
            facts(),
        )
        .unwrap();
    assert_eq!(
        recovered.snapshot.tasks[0]
            .interrupt
            .as_ref()
            .unwrap()
            .prompt,
        json!({"tool":"fixture"})
    );
    let resumed = rt
        .apply(
            &id(),
            ExecutionFence::from(&recovered.snapshot),
            KernelCommand::Resume {
                route: recovered.snapshot.definition.route.clone(),
                turn_id: TurnId::new("turn-3").unwrap(),
                decisions: vec![Decision {
                    interrupt_id: InterruptId::new("approval").unwrap(),
                    decision: ApprovalDecision::Once,
                }],
            },
            facts(),
        )
        .unwrap();
    let done = rt
        .dispatch(
            &id(),
            ExecutionFence::from(&resumed.snapshot),
            task(),
            attempt("attempt-1"),
            facts(),
            facts,
            |_| Ok(result()),
        )
        .unwrap();
    assert_eq!(done.snapshot.output, Some(json!({"items":["ok"]})));
}

#[test]
fn unavailable_definition_blocks_recovery_without_dispatch() {
    let dir = Directory::new();
    let (rt, running) = setup(&dir, RecoveryPolicy::Pure, false);
    drop(rt);
    let rt = Runtime::new(SqliteStore::open(&dir.0).unwrap());
    let blocked = rt
        .recover(
            &id(),
            ExecutionFence::from(&running.snapshot),
            ExecutionRevision::new("missing").unwrap(),
            TurnId::new("turn-2").unwrap(),
            facts(),
        )
        .unwrap();
    assert_eq!(blocked.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert!(rt
        .dispatch(
            &id(),
            ExecutionFence::from(&blocked.snapshot),
            task(),
            attempt("attempt-1"),
            facts(),
            facts,
            |_| panic!("blocked definition invoked")
        )
        .is_err());
}
