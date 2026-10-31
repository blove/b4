use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::approval::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use rusqlite::{params, Connection};
use serde_json::{json, Map};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-approvals-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn execution() -> ExecutionId {
    ExecutionId::new("execution").unwrap()
}
fn thread() -> ThreadId {
    ThreadId::new("thread").unwrap()
}
fn task() -> TaskId {
    TaskId::new("task").unwrap()
}
fn interrupt() -> InterruptId {
    InterruptId::new("interrupt").unwrap()
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "principal".into(),
        },
        policy_revision: "policy-1".into(),
    }
}
fn running(directory: &Directory) -> (SqliteStore, StoredExecution) {
    let store = SqliteStore::open(&directory.0).unwrap();
    let running = start(&store);
    (store, running)
}
fn start(store: &dyn ApprovalStore) -> StoredExecution {
    start_named(store, &["task"])
}
fn start_named(store: &dyn ApprovalStore, names: &[&str]) -> StoredExecution {
    store
        .create_thread_with(ThreadSeed {
            id: thread(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let initial = new_execution(
        execution(),
        TurnId::new("turn-1").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/approvals#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![names
                .iter()
                .map(|name| TaskSpec {
                    id: TaskId::new(*name).unwrap(),
                    effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: true,
                })
                .collect()],
            max_attempts: 3,
        },
        json!({"input":{"a":1}}).as_object().unwrap().clone(),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    let admitted = store.admit(&thread(), initial).unwrap();
    let plan = transition(
        &admitted.snapshot,
        KernelCommand::Start,
        TransitionFacts { now_ms: 101 },
    )
    .unwrap();
    store
        .commit(&execution(), (&admitted.snapshot).into(), plan)
        .unwrap()
}

fn fence(store: &dyn ApprovalStore, snapshot: &Snapshot) -> ApprovalFence {
    ApprovalFence {
        execution: snapshot.into(),
        resource_revision: store.thread(&thread()).unwrap().unwrap().resource_revision,
    }
}
fn issue_plan(store: &dyn ApprovalStore, current: &StoredExecution) -> (CommitPlan, GrantRecord) {
    issue_task_plan(store, current, task(), interrupt())
}
fn issue_task_plan(
    store: &dyn ApprovalStore,
    current: &StoredExecution,
    task_id: TaskId,
    interrupt_id: InterruptId,
) -> (CommitPlan, GrantRecord) {
    let prompt = json!({"tool":"fixture","arguments":{"a":1,"nullable":null}});
    let plan = transition(
        &current.snapshot,
        KernelCommand::Park {
            task_id: task_id.clone(),
            interrupt_id: interrupt_id.clone(),
            prompt: prompt.clone(),
        },
        TransitionFacts { now_ms: 102 },
    )
    .unwrap();
    let thread = store.thread(&thread()).unwrap().unwrap();
    let token = ApprovalToken::new(format!("b4ag_{}", "1".repeat(64))).unwrap();
    let parked_task = plan
        .snapshot
        .tasks
        .iter()
        .find(|task| task.spec.id == task_id)
        .unwrap();
    let grant = GrantRecord {
        thread_id: thread.id,
        thread_generation: thread.generation,
        origin: context().actor,
        execution_id: execution(),
        route: plan.snapshot.definition.route.clone(),
        definition_revision: plan.snapshot.definition.revision.clone(),
        task_id,
        effect_id: parked_task.spec.effect_id.clone(),
        interrupt_id,
        resume_key: "2".repeat(64),
        task_parking_revision: plan.snapshot.revision,
        policy_revision: context().policy_revision,
        proposal_digest: proposal_digest(parked_task, &prompt).unwrap(),
        prompt,
        verifier: token_verifier(&token),
        token,
        issued_at_ms: 102,
        expires_at_ms: 1_102,
        consumed: false,
    };
    (plan, grant)
}
fn issue(store: &dyn ApprovalStore, current: &StoredExecution) -> StoredExecution {
    let (plan, grant) = issue_plan(store, current);
    store
        .commit_approval(
            &execution(),
            fence(store, &current.snapshot),
            plan,
            ApprovalMutation::Issue(grant),
        )
        .unwrap()
}
fn accept_plan(
    store: &dyn ApprovalStore,
    parked: &StoredExecution,
) -> (CommitPlan, ApprovalAcceptanceRequest) {
    let grant = store
        .approval_view(&execution())
        .unwrap()
        .unwrap()
        .grants
        .remove(0);
    let request = ApprovalAcceptanceRequest {
        context: context(),
        route: parked.snapshot.definition.route.clone(),
        definition_revision: parked.snapshot.definition.revision.clone(),
        turn_id: TurnId::new("turn-approved").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt(),
            decision: ApprovalDecision::Once,
            cancelled: false,
            token: Some(grant.token),
        }],
    };
    let plan = transition(
        &parked.snapshot,
        KernelCommand::Resume {
            route: request.route.clone(),
            turn_id: request.turn_id.clone(),
            decisions: vec![Decision {
                interrupt_id: interrupt(),
                decision: ApprovalDecision::Once,
            }],
        },
        TransitionFacts {
            now_ms: request.now_ms,
        },
    )
    .unwrap();
    (plan, request)
}

fn with_backends(test: impl Fn(&dyn ApprovalStore)) {
    test(&MemoryStore::new());
    let directory = Directory::new();
    test(&SqliteStore::open(&directory.0).unwrap());
}

#[test]
fn both_backends_qualify_fenced_exact_set_atomic_acceptance() {
    with_backends(|store| {
        let running = start(store);
        let parked = issue(store, &running);
        let before = store.approval_view(&execution()).unwrap();
        let parked_fence = fence(store, &parked.snapshot);
        let (plan, request) = accept_plan(store, &parked);
        let mut missing = request.clone();
        missing.decisions.clear();
        assert_eq!(
            store.commit_approval(
                &execution(),
                parked_fence,
                plan.clone(),
                ApprovalMutation::Accept(missing)
            ),
            Err(ApprovalError::InterruptSetMismatch)
        );
        let mut invalid = request.clone();
        invalid.decisions[0].token =
            Some(ApprovalToken::new(format!("b4ag_{}", "3".repeat(64))).unwrap());
        assert_eq!(
            store.commit_approval(
                &execution(),
                parked_fence,
                plan.clone(),
                ApprovalMutation::Accept(invalid)
            ),
            Err(ApprovalError::InvalidGrant)
        );
        let mut expired = request.clone();
        expired.now_ms = 1_102;
        assert_eq!(
            store.commit_approval(
                &execution(),
                parked_fence,
                plan.clone(),
                ApprovalMutation::Accept(expired)
            ),
            Err(ApprovalError::Expired)
        );
        let stale = ApprovalFence {
            resource_revision: parked_fence.resource_revision - 1,
            ..parked_fence
        };
        assert_eq!(
            store.commit_approval(
                &execution(),
                stale,
                plan.clone(),
                ApprovalMutation::Accept(request.clone())
            ),
            Err(ApprovalError::Store(StoreError::RevisionConflict))
        );
        let mut wrong_proposal = plan.clone();
        wrong_proposal.dispatchable.clear();
        assert_eq!(
            store.commit_approval(
                &execution(),
                parked_fence,
                wrong_proposal,
                ApprovalMutation::Accept(request.clone())
            ),
            Err(ApprovalError::InvalidProposal)
        );
        assert_eq!(store.approval_view(&execution()).unwrap(), before);
        let resumed = store
            .commit_approval(
                &execution(),
                parked_fence,
                plan.clone(),
                ApprovalMutation::Accept(request.clone()),
            )
            .unwrap();
        let view = store.approval_view(&execution()).unwrap().unwrap();
        assert_eq!(view.execution, resumed);
        assert_eq!(view.thread.resource_revision, 4);
        assert!(view.grants.iter().all(|grant| grant.consumed));
        assert_eq!(view.acceptances.len(), 1);
        assert_eq!(
            store.commit_approval(
                &execution(),
                parked_fence,
                plan,
                ApprovalMutation::Accept(request)
            ),
            Err(ApprovalError::Store(StoreError::RevisionConflict))
        );
        assert_eq!(store.approval_view(&execution()).unwrap(), Some(view));
    });
}

#[test]
fn both_backends_allow_only_one_concurrent_acceptance() {
    with_backends(|store| {
        let running = start(store);
        let parked = issue(store, &running);
        let captured = fence(store, &parked.snapshot);
        let (plan, request) = accept_plan(store, &parked);
        let barrier = std::sync::Barrier::new(2);
        let outcomes = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..2)
                .map(|_| {
                    let barrier = &barrier;
                    let plan = plan.clone();
                    let request = request.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        store.commit_approval(
                            &execution(),
                            captured,
                            plan,
                            ApprovalMutation::Accept(request),
                        )
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(outcomes.iter().filter(|result|**result == Err(ApprovalError::Store(StoreError::RevisionConflict))).count(), 1);
        let view = store.approval_view(&execution()).unwrap().unwrap();
        assert_eq!(view.acceptances.len(), 1);
        assert_eq!(view.thread.resource_revision, 4);
        assert!(view.grants[0].consumed);
    });
}

fn parked_pair(store: &dyn ApprovalStore) -> StoredExecution {
    let mut current = start_named(store, &["a", "b"]);
    for name in ["a", "b"] {
        let (plan, grant) = issue_task_plan(
            store,
            &current,
            TaskId::new(name).unwrap(),
            InterruptId::new(format!("interrupt-{name}")).unwrap(),
        );
        current = store
            .commit_approval(
                &execution(),
                fence(store, &current.snapshot),
                plan,
                ApprovalMutation::Issue(grant),
            )
            .unwrap();
    }
    current
}
fn pair_accept_plan(
    store: &dyn ApprovalStore,
    parked: &StoredExecution,
) -> (CommitPlan, ApprovalAcceptanceRequest) {
    let grants = store.approval_view(&execution()).unwrap().unwrap().grants;
    let decisions = grants
        .into_iter()
        .map(|grant| GrantDecision {
            interrupt_id: grant.interrupt_id,
            decision: if grant.task_id.as_str() == "a" {
                ApprovalDecision::Once
            } else {
                ApprovalDecision::Deny
            },
            cancelled: false,
            token: Some(grant.token),
        })
        .collect::<Vec<_>>();
    let request = ApprovalAcceptanceRequest {
        context: context(),
        route: parked.snapshot.definition.route.clone(),
        definition_revision: parked.snapshot.definition.revision.clone(),
        turn_id: TurnId::new("turn-approved").unwrap(),
        now_ms: 103,
        decisions,
    };
    let plan = transition(
        &parked.snapshot,
        KernelCommand::Resume {
            route: request.route.clone(),
            turn_id: request.turn_id.clone(),
            decisions: request
                .decisions
                .iter()
                .map(|entry| Decision {
                    interrupt_id: entry.interrupt_id.clone(),
                    decision: entry.decision,
                })
                .collect(),
        },
        TransitionFacts {
            now_ms: request.now_ms,
        },
    )
    .unwrap();
    (plan, request)
}

#[test]
fn both_backends_validate_every_grant_before_consuming_the_complete_set() {
    with_backends(|store| {
        let parked = parked_pair(store);
        let before = store.approval_view(&execution()).unwrap();
        let captured = fence(store, &parked.snapshot);
        let (plan, request) = pair_accept_plan(store, &parked);
        let mut invalid_second = request.clone();
        invalid_second.decisions[1].token = None;
        assert_eq!(
            store.commit_approval(
                &execution(),
                captured,
                plan.clone(),
                ApprovalMutation::Accept(invalid_second)
            ),
            Err(ApprovalError::MissingToken)
        );
        assert_eq!(store.approval_view(&execution()).unwrap(), before);
        let resumed = store
            .commit_approval(
                &execution(),
                captured,
                plan,
                ApprovalMutation::Accept(request),
            )
            .unwrap();
        assert_eq!(resumed.snapshot.tasks[0].status, TaskStatus::Ready);
        assert!(resumed.snapshot.tasks[0].approved);
        assert_eq!(resumed.snapshot.tasks[1].status, TaskStatus::Denied);
        assert!(!resumed.snapshot.tasks[1].approved);
        let view = store.approval_view(&execution()).unwrap().unwrap();
        assert_eq!(view.grants.len(), 2);
        assert!(view.grants.iter().all(|grant| grant.consumed));
        assert_eq!(view.acceptances.len(), 1);
        assert_eq!(view.acceptances[0].decisions.len(), 2);
    });
}

#[test]
fn second_grant_update_failure_rolls_back_the_already_updated_first_grant() {
    let directory = Directory::new();
    let store = SqliteStore::open(&directory.0).unwrap();
    let parked = parked_pair(&store);
    let before = store.approval_view(&execution()).unwrap();
    let journal = store.journal(&execution()).unwrap();
    directory.connection().execute_batch("CREATE TRIGGER abort_second_grant BEFORE UPDATE ON approval_grants WHEN NEW.interrupt_id = 'interrupt-b' BEGIN SELECT RAISE(ABORT, 'injected'); END").unwrap();
    let (plan, request) = pair_accept_plan(&store, &parked);
    assert!(matches!(
        store.commit_approval(
            &execution(),
            fence(&store, &parked.snapshot),
            plan,
            ApprovalMutation::Accept(request)
        ),
        Err(ApprovalError::Store(StoreError::Unavailable(_)))
    ));
    assert_eq!(store.approval_view(&execution()).unwrap(), before);
    assert_eq!(store.journal(&execution()).unwrap(), journal);
}

#[test]
fn both_backends_preserve_trusted_cancellation_after_partial_grant_park() {
    with_backends(|store| {
        let running = start_named(store, &["a", "b"]);
        let (plan, grant) = issue_task_plan(
            store,
            &running,
            TaskId::new("a").unwrap(),
            InterruptId::new("interrupt-a").unwrap(),
        );
        let parked = store
            .commit_approval(
                &execution(),
                fence(store, &running.snapshot),
                plan,
                ApprovalMutation::Issue(grant),
            )
            .unwrap();
        let grants = store.approval_view(&execution()).unwrap().unwrap().grants;
        let cancellation = transition(
            &parked.snapshot,
            KernelCommand::Cancel {
                turn_id: parked.snapshot.turn_id.clone(),
                owner_generation: store.owner_generation(),
            },
            TransitionFacts { now_ms: 105 },
        )
        .unwrap();
        let cancelled = store
            .commit(&execution(), (&parked.snapshot).into(), cancellation)
            .unwrap();
        assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
        let view = store.approval_view(&execution()).unwrap().unwrap();
        assert_eq!(view.grants, grants);
        assert!(view.acceptances.is_empty());
    });
}

#[test]
fn issue_commits_grant_snapshot_resource_revision_and_journal_atomically() {
    let directory = Directory::new();
    let (store, running) = running(&directory);
    let parked = issue(&store, &running);
    let view = store.approval_view(&execution()).unwrap().unwrap();
    assert_eq!(view.execution, parked);
    assert_eq!(view.thread.resource_revision, 3);
    assert_eq!(view.grants.len(), 1);
    assert!(!view.grants[0].consumed);
    assert!(view.acceptances.is_empty());
    assert_eq!(store.journal(&execution()).unwrap().len(), 3);
    let token = view.grants[0].token.as_str();
    assert!(!serde_json::to_string(&parked.snapshot)
        .unwrap()
        .contains(token));
    assert!(
        !serde_json::to_string(&store.journal(&execution()).unwrap())
            .unwrap()
            .contains(token)
    );
}

#[test]
fn failed_grant_or_journal_insert_rolls_back_the_entire_park() {
    for table in ["approval_grants", "journal"] {
        let directory = Directory::new();
        let (store, running) = running(&directory);
        let before = store.approval_view(&execution()).unwrap();
        let journal = store.journal(&execution()).unwrap();
        directory.connection().execute_batch(&format!("CREATE TRIGGER abort_park BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END")).unwrap();
        let (plan, grant) = issue_plan(&store, &running);
        assert!(matches!(
            store.commit_approval(
                &execution(),
                fence(&store, &running.snapshot),
                plan,
                ApprovalMutation::Issue(grant)
            ),
            Err(ApprovalError::Store(StoreError::Unavailable(_)))
        ));
        assert_eq!(store.approval_view(&execution()).unwrap(), before);
        assert_eq!(store.journal(&execution()).unwrap(), journal);
        directory
            .connection()
            .execute_batch("DROP TRIGGER abort_park")
            .unwrap();
        assert_eq!(
            issue(&store, &running).snapshot.status,
            ExecutionStatus::WaitingForApproval
        );
    }
}

#[test]
fn failed_grant_update_acceptance_or_journal_insert_spends_nothing() {
    for (event, table) in [
        ("UPDATE", "approval_grants"),
        ("INSERT", "approval_acceptances"),
        ("INSERT", "journal"),
    ] {
        let directory = Directory::new();
        let (store, running) = running(&directory);
        let parked = issue(&store, &running);
        let before = store.approval_view(&execution()).unwrap();
        let journal = store.journal(&execution()).unwrap();
        directory.connection().execute_batch(&format!("CREATE TRIGGER abort_accept BEFORE {event} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END")).unwrap();
        let (plan, request) = accept_plan(&store, &parked);
        assert!(matches!(
            store.commit_approval(
                &execution(),
                fence(&store, &parked.snapshot),
                plan,
                ApprovalMutation::Accept(request)
            ),
            Err(ApprovalError::Store(StoreError::Unavailable(_)))
        ));
        assert_eq!(store.approval_view(&execution()).unwrap(), before);
        assert_eq!(store.journal(&execution()).unwrap(), journal);
        directory
            .connection()
            .execute_batch("DROP TRIGGER abort_accept")
            .unwrap();
        let (plan, request) = accept_plan(&store, &parked);
        let resumed = store
            .commit_approval(
                &execution(),
                fence(&store, &parked.snapshot),
                plan,
                ApprovalMutation::Accept(request),
            )
            .unwrap();
        assert!(resumed.snapshot.tasks[0].approved);
        let view = store.approval_view(&execution()).unwrap().unwrap();
        assert!(view.grants[0].consumed);
        assert_eq!(view.acceptances.len(), 1);
        assert_eq!(view.thread.resource_revision, 4);
        assert!(!serde_json::to_string(&view.acceptances)
            .unwrap()
            .contains(view.grants[0].token.as_str()));
    }
}

#[test]
fn ordinary_resume_cannot_bypass_a_required_grant_transaction() {
    let directory = Directory::new();
    let (store, running) = running(&directory);
    let parked = issue(&store, &running);
    let before = store.approval_view(&execution()).unwrap();
    let (plan, _) = accept_plan(&store, &parked);
    assert_eq!(
        store.commit(&execution(), (&parked.snapshot).into(), plan),
        Err(StoreError::InvalidCommit(
            "required_approval_transaction".into()
        ))
    );
    assert_eq!(store.approval_view(&execution()).unwrap(), before);
}

#[test]
fn forged_verifier_is_corrupt_without_sensitive_error_text() {
    let directory = Directory::new();
    let (store, running) = running(&directory);
    issue(&store, &running);
    let view = store.approval_view(&execution()).unwrap().unwrap();
    let mut grant = view.grants[0].clone();
    grant.verifier = [0; 32];
    directory
        .connection()
        .execute(
            "UPDATE approval_grants SET record = ?1",
            [serde_json::to_string(&grant).unwrap()],
        )
        .unwrap();
    assert_eq!(
        store.approval_view(&execution()),
        Err(ApprovalError::Store(StoreError::Corrupt(
            "invalid_approval_grant".into()
        )))
    );
}

#[test]
fn an_empty_recorded_acceptance_is_corrupt() {
    let directory = Directory::new();
    let (store, running) = running(&directory);
    let parked = issue(&store, &running);
    let (plan, request) = accept_plan(&store, &parked);
    store
        .commit_approval(
            &execution(),
            fence(&store, &parked.snapshot),
            plan,
            ApprovalMutation::Accept(request),
        )
        .unwrap();
    let mut acceptance = store
        .approval_view(&execution())
        .unwrap()
        .unwrap()
        .acceptances
        .remove(0);
    acceptance.decisions.clear();
    directory
        .connection()
        .execute(
            "UPDATE approval_acceptances SET record = ?1",
            [serde_json::to_string(&acceptance).unwrap()],
        )
        .unwrap();
    assert_eq!(
        store.approval_view(&execution()),
        Err(ApprovalError::Store(StoreError::Corrupt(
            "invalid_approval_acceptance".into()
        )))
    );
}

struct Allow;
impl ApprovalAuthority for Allow {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}

fn recovered(
    service: &ApprovalService<SqliteStore, Allow>,
    snapshot: &Snapshot,
    turn: &str,
    now_ms: u64,
) -> StoredExecution {
    Runtime::new(service.store())
        .recover(
            &execution(),
            snapshot.into(),
            snapshot.definition.revision.clone(),
            TurnId::new(turn).unwrap(),
            TransitionFacts { now_ms },
        )
        .unwrap()
}

#[test]
fn reopened_pending_retains_exact_prompt_token_interrupt_and_resume_key() {
    let directory = Directory::new();
    let (store, running) = running(&directory);
    let service = ApprovalService::new(store, Allow).unwrap();
    let parked = service
        .park(
            &context(),
            &execution(),
            fence(service.store(), &running.snapshot),
            task(),
            interrupt(),
            json!({"tool":"fixture","arguments":{"x":null}}),
            1_000,
            TransitionFacts { now_ms: 102 },
        )
        .unwrap();
    let captured = fence(service.store(), &parked.snapshot);
    let pending = service.pending(&context(), &execution(), captured).unwrap();
    assert_eq!(
        service.pending(&context(), &execution(), captured).unwrap(),
        pending
    );
    let generation = service
        .store()
        .thread(&thread())
        .unwrap()
        .unwrap()
        .generation;
    drop(service);
    let service = ApprovalService::new(SqliteStore::open(&directory.0).unwrap(), Allow).unwrap();
    assert_eq!(
        service.pending(&context(), &execution(), captured),
        Err(ApprovalError::Store(StoreError::OwnerConflict))
    );
    let current = recovered(&service, &parked.snapshot, "turn-recovered", 104);
    let current_fence = fence(service.store(), &current.snapshot);
    assert_eq!(
        service
            .pending(&context(), &execution(), current_fence)
            .unwrap(),
        pending
    );
    let view = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    assert_eq!(view.thread.generation, generation);
    assert_eq!(
        view.grants[0].task_parking_revision,
        parked.snapshot.revision
    );
    assert_eq!(current.snapshot.revision, parked.snapshot.revision + 1);
}

#[test]
fn accepted_before_restart_is_explicitly_recovered_and_dispatched_without_reapproval() {
    let directory = Directory::new();
    let (store, running) = running(&directory);
    let service = ApprovalService::new(store, Allow).unwrap();
    let parked = service
        .park(
            &context(),
            &execution(),
            fence(service.store(), &running.snapshot),
            task(),
            interrupt(),
            json!({"tool":"fixture"}),
            1_000,
            TransitionFacts { now_ms: 102 },
        )
        .unwrap();
    let pending = service
        .pending(
            &context(),
            &execution(),
            fence(service.store(), &parked.snapshot),
        )
        .unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context(),
        route: parked.snapshot.definition.route.clone(),
        definition_revision: parked.snapshot.definition.revision.clone(),
        turn_id: TurnId::new("turn-approved").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt(),
            decision: ApprovalDecision::Once,
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };
    let accepted = service
        .accept(
            &execution(),
            fence(service.store(), &parked.snapshot),
            request,
        )
        .unwrap();
    let accepted_view = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    let old_fence = fence(service.store(), &accepted.snapshot);
    assert!(accepted.snapshot.tasks[0].approved);
    drop(service);
    let service = ApprovalService::new(SqliteStore::open(&directory.0).unwrap(), Allow).unwrap();
    assert_eq!(
        service.store().approval_view(&execution()).unwrap(),
        Some(accepted_view.clone())
    );
    assert_eq!(
        service.dispatch_outcome(
            &context(),
            &execution(),
            old_fence,
            task(),
            AttemptId::new("stale-attempt").unwrap(),
            TransitionFacts { now_ms: 2_000 },
            || TransitionFacts { now_ms: 2_001 },
            |_| panic!("old owner invoked effect")
        ),
        Err(ApprovalError::Store(StoreError::OwnerConflict))
    );
    let recovered = recovered(&service, &accepted.snapshot, "turn-recovered", 2_000);
    let result = service
        .dispatch_outcome(
            &context(),
            &execution(),
            fence(service.store(), &recovered.snapshot),
            task(),
            AttemptId::new("fresh-attempt").unwrap(),
            TransitionFacts { now_ms: 2_001 },
            || TransitionFacts { now_ms: 2_002 },
            |call| {
                assert_eq!(call.owner_generation, 2);
                assert_eq!(call.effect_id.as_str(), "effect-task");
                assert_eq!(call.input, running.snapshot.tasks[0].input);
                Ok(EffectOutcome::Succeeded(TaskResult {
                    output: json!("completed"),
                    writes: vec![StateWrite::Replace {
                        field: "result".into(),
                        value: json!("completed"),
                    }],
                }))
            },
        )
        .unwrap();
    assert_eq!(result.snapshot.status, ExecutionStatus::Completed);
    let finished_view = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    assert_eq!(finished_view.grants, accepted_view.grants);
    assert_eq!(finished_view.acceptances, accepted_view.acceptances);
    drop(service);
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(store.execution(&execution()).unwrap(), Some(result));
}

#[test]
fn grant_and_acceptance_indexes_must_match_strict_records() {
    for mutation in [
        "UPDATE approval_grants SET task_id = 'other'",
        "UPDATE approval_grants SET interrupt_id = 'other'",
        "UPDATE approval_grants SET thread_generation = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
        "UPDATE approval_grants SET task_parking_revision = x'0000000000000063'",
        "UPDATE approval_grants SET consumed = 0",
        "UPDATE approval_acceptances SET turn_id = 'other'",
        "UPDATE approval_acceptances SET previous_revision = x'0000000000000063'",
        "UPDATE approval_acceptances SET definition_revision = 'other'",
        "UPDATE approval_acceptances SET accepted_at_ms = x'0000000000000063'",
    ] {
        let directory = Directory::new();
        let (store, running) = running(&directory);
        let parked = issue(&store, &running);
        let (plan, request) = accept_plan(&store, &parked);
        store.commit_approval(&execution(), fence(&store, &parked.snapshot), plan, ApprovalMutation::Accept(request)).unwrap();
        directory.connection().execute_batch(mutation).unwrap();
        let error = store.approval_view(&execution()).unwrap_err();
        let expected = if mutation.contains("UPDATE approval_grants") { "invalid_approval_grant" } else { "invalid_approval_acceptance" };
        assert_eq!(error, ApprovalError::Store(StoreError::Corrupt(expected.into())), "{mutation}");
    }
}

#[test]
fn excessive_prompt_depth_is_rejected_before_any_approval_write_in_both_backends() {
    with_backends(|store| {
        let running = start(store);
        let before = store.approval_view(&execution()).unwrap();
        let journal = store.journal(&execution()).unwrap();
        let (_, mut grant) = issue_plan(store, &running);
        let mut prompt = serde_json::Value::Null;
        for _ in 0..130 {
            prompt = json!([prompt]);
        }
        grant.prompt = prompt.clone();
        let canonical = json!({"version":1,"task":running.snapshot.tasks[0].spec,"input":running.snapshot.tasks[0].input,"prompt":prompt});
        grant.proposal_digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&canonical).unwrap())
        );
        // Use the kernel proposal directly so the transactional validator remains
        // the final guard for a trusted caller that bypassed service validation.
        let plan = transition(
            &running.snapshot,
            KernelCommand::Park {
                task_id: task(),
                interrupt_id: interrupt(),
                prompt,
            },
            TransitionFacts { now_ms: 102 },
        )
        .unwrap();
        assert_eq!(
            store.commit_approval(
                &execution(),
                fence(store, &running.snapshot),
                plan,
                ApprovalMutation::Issue(grant)
            ),
            Err(ApprovalError::InvalidProposal)
        );
        assert_eq!(store.approval_view(&execution()).unwrap(), before);
        assert_eq!(store.journal(&execution()).unwrap(), journal);
    });
}

#[test]
fn permitted_prompt_depth_boundary_remains_readable_after_sqlite_reopen() {
    let directory = Directory::new();
    let (store, running) = running(&directory);
    let service = ApprovalService::new(store, Allow).unwrap();
    let mut prompt = serde_json::Value::Null;
    for _ in 0..64 {
        prompt = json!([prompt]);
    }
    let parked = service
        .park(
            &context(),
            &execution(),
            fence(service.store(), &running.snapshot),
            task(),
            interrupt(),
            prompt.clone(),
            1_000,
            TransitionFacts { now_ms: 102 },
        )
        .unwrap();
    let pending = service
        .pending(
            &context(),
            &execution(),
            fence(service.store(), &parked.snapshot),
        )
        .unwrap();
    assert_eq!(pending[0].prompt, prompt);
    drop(service);
    let service = ApprovalService::new(SqliteStore::open(&directory.0).unwrap(), Allow).unwrap();
    assert_eq!(
        service.store().execution(&execution()).unwrap(),
        Some(parked.clone())
    );
    let current = recovered(&service, &parked.snapshot, "turn-recovered", 104);
    assert_eq!(
        service
            .pending(
                &context(),
                &execution(),
                fence(service.store(), &current.snapshot)
            )
            .unwrap(),
        pending
    );
}

#[test]
fn malformed_sensitive_record_errors_are_static_and_redacted() {
    for payload in ["{", "{}", "{\"b4ag_sensitive_token_field\":true}"] {
        let directory = Directory::new();
        let (store, _) = running(&directory);
        let generation = store.thread(&thread()).unwrap().unwrap().generation;
        directory.connection().execute(
            "INSERT INTO approval_grants (execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record) VALUES ('execution', 'interrupt', 'task', 'thread', ?1, ?2, 0, ?3)",
            params![generation.as_str(), 2_u64.to_be_bytes().as_slice(), payload],
        ).unwrap();
        assert_eq!(
            store.approval_view(&execution()),
            Err(ApprovalError::Store(StoreError::Corrupt(
                "invalid_approval_grant".into()
            )))
        );
        // Ordinary projections remain available for the authority precheck.
        assert!(store.thread_view(&thread()).unwrap().is_some());
    }
}

#[test]
fn malformed_acceptance_errors_are_static_and_redacted() {
    let directory = Directory::new();
    let (store, _) = running(&directory);
    let generation = store.thread(&thread()).unwrap().unwrap().generation;
    directory.connection().execute(
        "INSERT INTO approval_acceptances (execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record) VALUES ('execution', 'turn-2', 'thread', ?1, ?2, 'definition-1', ?3, ?4)",
        params![generation.as_str(), 2_u64.to_be_bytes().as_slice(), 102_u64.to_be_bytes().as_slice(), "{\"b4ag_sensitive_unknown_field\":true}"],
    ).unwrap();
    assert_eq!(
        store.approval_view(&execution()),
        Err(ApprovalError::Store(StoreError::Corrupt(
            "invalid_approval_acceptance".into()
        )))
    );
}
