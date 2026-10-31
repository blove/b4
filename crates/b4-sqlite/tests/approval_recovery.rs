use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::approval::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use rusqlite::Connection;
use serde_json::{json, Map};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-approval-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
    fn authority_records(&self) -> (Vec<String>, Vec<String>) {
        let connection = self.connection();
        let records = |table| {
            connection
                .prepare(&format!("SELECT record FROM {table} ORDER BY record"))
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<String>, _>>()
                .unwrap()
        };
        (records("approval_grants"), records("approval_acceptances"))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
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
type Service = ApprovalService<SqliteStore, Allow>;

fn thread() -> ThreadId {
    ThreadId::new("thread").unwrap()
}
fn execution() -> ExecutionId {
    ExecutionId::new("execution").unwrap()
}
fn task() -> TaskId {
    TaskId::new("release").unwrap()
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
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn fence(store: &dyn ApprovalStore) -> ApprovalFence {
    let view = store.thread_view(&thread()).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}
fn started(directory: &Directory, recovery: RecoveryPolicy) -> (Service, StoredExecution) {
    let store = SqliteStore::open(&directory.0).unwrap();
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
        TurnId::new("original-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/approval-recovery#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task(),
                effect_id: EffectId::new("release-effect").unwrap(),
                recovery,
                requires_approval: true,
            }]],
            max_attempts: 3,
        },
        Map::from_iter([("input".into(), json!({"release":"fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread(), initial).unwrap();
    let running = store
        .commit(
            &execution(),
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    (ApprovalService::new(store, Allow).unwrap(), running)
}
fn park(service: &Service) -> StoredExecution {
    service
        .park(
            &context(),
            &execution(),
            fence(service.store()),
            task(),
            InterruptId::new("release-interrupt").unwrap(),
            json!({"question":"Release fixture?", "arguments":{"nullable":null}}),
            100,
            facts(102),
        )
        .unwrap()
}
fn accept(service: &Service) -> StoredExecution {
    let parked = park(service);
    let pending = service
        .pending(&context(), &execution(), fence(service.store()))
        .unwrap();
    service
        .accept(
            &execution(),
            fence(service.store()),
            ApprovalAcceptanceRequest {
                context: context(),
                route: parked.snapshot.definition.route,
                definition_revision: parked.snapshot.definition.revision,
                turn_id: TurnId::new("approved-turn").unwrap(),
                now_ms: 103,
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
fn recovery_request(store: &dyn ApprovalStore, turn: &str) -> ApprovalRecoveryRequest {
    let view = store.thread_view(&thread()).unwrap().unwrap();
    let current = view.execution.unwrap();
    ApprovalRecoveryRequest {
        context: context(),
        thread_id: view.thread.id,
        thread_generation: view.thread.generation,
        execution_id: current.snapshot.execution_id,
        available_definition: current.snapshot.definition,
        turn_id: TurnId::new(turn).unwrap(),
        facts: facts(2_000),
    }
}
fn reopen(directory: &Directory, service: Service) -> Service {
    drop(service);
    ApprovalService::new(SqliteStore::open(&directory.0).unwrap(), Allow).unwrap()
}
fn recover(service: &Service, turn: &str) -> StoredExecution {
    service
        .recover(
            &execution(),
            fence(service.store()),
            recovery_request(service.store(), turn),
        )
        .unwrap()
}
fn recovery_plan(store: &dyn ApprovalStore, request: &ApprovalRecoveryRequest) -> CommitPlan {
    transition(
        &store.execution(&execution()).unwrap().unwrap().snapshot,
        KernelCommand::Recover {
            available_revision: request.available_definition.revision.clone(),
            turn_id: request.turn_id.clone(),
            owner_generation: store.owner_generation(),
        },
        request.facts.clone(),
    )
    .unwrap()
}

#[test]
fn fenced_recovery_keeps_exact_parked_grant_and_pending_identity_after_reopen() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    let parked = park(&service);
    let pending = service
        .pending(&context(), &execution(), fence(service.store()))
        .unwrap();
    let before = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    let bytes = directory.authority_records();
    let service = reopen(&directory, service);
    assert_eq!(service.store().owner_generation(), 2);
    assert_eq!(
        service.store().approval_view(&execution()).unwrap(),
        Some(before.clone())
    );
    let recovered = recover(&service, "recovered-turn");
    assert_eq!(
        recovered.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(recovered.snapshot.owner_generation, 2);
    assert_eq!(recovered.snapshot.revision, parked.snapshot.revision + 1);
    let after = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    assert_eq!(after.thread.generation, before.thread.generation);
    assert_eq!(
        after.thread.resource_revision,
        before.thread.resource_revision + 1
    );
    assert_eq!(after.grants, before.grants);
    assert_eq!(after.acceptances, before.acceptances);
    assert_eq!(directory.authority_records(), bytes);
    assert_eq!(
        service
            .pending(&context(), &execution(), fence(service.store()))
            .unwrap(),
        pending
    );
    assert_eq!(service.store().journal(&execution()).unwrap().len(), 4);
}

#[test]
fn accepted_once_recovers_and_dispatches_once_after_original_expiry_without_new_audit() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    let accepted = accept(&service);
    let before = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    let bytes = directory.authority_records();
    assert!(before.grants[0].consumed);
    assert_eq!(before.acceptances.len(), 1);
    assert!(before.grants[0].expires_at_ms < 2_000);
    let service = reopen(&directory, service);
    let recovered = recover(&service, "recovered-turn");
    assert_eq!(recovered.snapshot.status, ExecutionStatus::Running);
    assert_eq!(recovered.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(
        recovered.snapshot.attempt_history,
        accepted.snapshot.attempt_history
    );
    let calls = AtomicUsize::new(0);
    let finished = service
        .dispatch_outcome(
            &context(),
            &execution(),
            fence(service.store()),
            task(),
            AttemptId::new("first-effect-attempt").unwrap(),
            facts(2_001),
            || facts(2_002),
            |invocation| {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(invocation.owner_generation, 2);
                assert_eq!(invocation.turn_id, recovered.snapshot.turn_id);
                assert_eq!(invocation.effect_id.as_str(), "release-effect");
                Ok(EffectOutcome::Succeeded(TaskResult {
                    output: json!({"released":true}),
                    writes: vec![StateWrite::Append {
                        field: "releases".into(),
                        items: vec![json!("fixture")],
                    }],
                }))
            },
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(finished.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(finished.snapshot.values["releases"], json!(["fixture"]));
    let after = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    assert_eq!(after.grants, before.grants);
    assert_eq!(after.acceptances, before.acceptances);
    assert_eq!(directory.authority_records(), bytes);
    drop(service);
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(store.owner_generation(), 3);
    assert_eq!(store.execution(&execution()).unwrap(), Some(finished));
    assert_eq!(directory.authority_records(), bytes);
}

#[test]
fn same_owner_and_duplicate_recovery_refuse_without_any_mutation() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    park(&service);
    let before = service.store().approval_view(&execution()).unwrap();
    let journal = service.store().journal(&execution()).unwrap();
    assert_eq!(
        service.recover(
            &execution(),
            fence(service.store()),
            recovery_request(service.store(), "same-owner-turn")
        ),
        Err(ApprovalError::RecoveryNotNeeded)
    );
    assert_eq!(service.store().approval_view(&execution()).unwrap(), before);
    assert_eq!(service.store().journal(&execution()).unwrap(), journal);
    let service = reopen(&directory, service);
    recover(&service, "recovered-turn");
    let before = service.store().approval_view(&execution()).unwrap();
    let journal = service.store().journal(&execution()).unwrap();
    let bytes = directory.authority_records();
    assert_eq!(
        service.recover(
            &execution(),
            fence(service.store()),
            recovery_request(service.store(), "duplicate-turn")
        ),
        Err(ApprovalError::RecoveryNotNeeded)
    );
    assert_eq!(service.store().approval_view(&execution()).unwrap(), before);
    assert_eq!(service.store().journal(&execution()).unwrap(), journal);
    assert_eq!(directory.authority_records(), bytes);
}

#[test]
fn recovery_transaction_rechecks_complete_target_and_execution_resource_fences() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    park(&service);
    let service = reopen(&directory, service);
    let request = recovery_request(service.store(), "recovered-turn");
    let captured = fence(service.store());
    let plan = recovery_plan(service.store(), &request);
    let before = service.store().approval_view(&execution()).unwrap();
    let journal = service.store().journal(&execution()).unwrap();
    let bytes = directory.authority_records();
    let mut wrong_thread = request.clone();
    wrong_thread.thread_id = ThreadId::new("other-thread").unwrap();
    let mut wrong_generation = request.clone();
    wrong_generation.thread_generation = ThreadGeneration::new("a".repeat(64)).unwrap();
    let mut wrong_execution = request.clone();
    wrong_execution.execution_id = ExecutionId::new("other-execution").unwrap();
    for (target, expected) in [
        (
            wrong_thread,
            ApprovalError::Store(StoreError::ExecutionNotActive),
        ),
        (
            wrong_generation,
            ApprovalError::Store(StoreError::RevisionConflict),
        ),
        (
            wrong_execution,
            ApprovalError::Store(StoreError::ExecutionNotActive),
        ),
    ] {
        assert_eq!(
            service.store().commit_approval(
                &execution(),
                captured,
                plan.clone(),
                ApprovalMutation::Recover(target)
            ),
            Err(expected)
        );
        assert_eq!(service.store().approval_view(&execution()).unwrap(), before);
    }
    for (changed_fence, expected) in [
        (
            ApprovalFence {
                execution: ExecutionFence {
                    owner_generation: 0,
                    ..captured.execution
                },
                ..captured
            },
            StoreError::OwnerConflict,
        ),
        (
            ApprovalFence {
                execution: ExecutionFence {
                    owner_generation: 2,
                    ..captured.execution
                },
                ..captured
            },
            StoreError::OwnerConflict,
        ),
        (
            ApprovalFence {
                execution: ExecutionFence {
                    revision: captured.execution.revision - 1,
                    ..captured.execution
                },
                ..captured
            },
            StoreError::RevisionConflict,
        ),
        (
            ApprovalFence {
                resource_revision: captured.resource_revision - 1,
                ..captured
            },
            StoreError::RevisionConflict,
        ),
    ] {
        assert_eq!(
            service.store().commit_approval(
                &execution(),
                changed_fence,
                plan.clone(),
                ApprovalMutation::Recover(request.clone())
            ),
            Err(ApprovalError::Store(expected))
        );
        assert_eq!(service.store().approval_view(&execution()).unwrap(), before);
    }
    assert_eq!(service.store().journal(&execution()).unwrap(), journal);
    assert_eq!(directory.authority_records(), bytes);
}

#[test]
fn changed_resource_alone_rejects_recovery_even_when_execution_fence_is_unchanged() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    park(&service);
    let service = reopen(&directory, service);
    let request = recovery_request(service.store(), "recovered-turn");
    let captured = fence(service.store());
    let execution_before = service.store().execution(&execution()).unwrap();
    let journal = service.store().journal(&execution()).unwrap();
    let bytes = directory.authority_records();
    // Resource revision is an independent CAS field. Its BLOB stays well formed,
    // and the execution record, authority records and journal are untouched.
    directory
        .connection()
        .execute(
            "UPDATE threads SET resource_revision = ?1 WHERE id = 'thread'",
            [(captured.resource_revision + 1).to_be_bytes().as_slice()],
        )
        .unwrap();
    let changed = service.store().approval_view(&execution()).unwrap();
    assert_eq!(
        service.store().execution(&execution()).unwrap(),
        execution_before
    );
    assert_eq!(
        service.recover(&execution(), captured, request),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    );
    assert_eq!(
        service.store().approval_view(&execution()).unwrap(),
        changed
    );
    assert_eq!(service.store().journal(&execution()).unwrap(), journal);
    assert_eq!(directory.authority_records(), bytes);
}

#[test]
fn recovery_rejects_every_full_definition_difference_and_forged_resume_plan() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    park(&service);
    let service = reopen(&directory, service);
    let request = recovery_request(service.store(), "recovered-turn");
    let captured = fence(service.store());
    let plan = recovery_plan(service.store(), &request);
    let before = service.store().approval_view(&execution()).unwrap();
    let journal = service.store().journal(&execution()).unwrap();
    let bytes = directory.authority_records();
    let original = &request.available_definition;
    let mut route = original.clone();
    route.route = RouteKey::new("/other#agent").unwrap();
    let mut revision = original.clone();
    revision.revision = ExecutionRevision::new("definition-2").unwrap();
    let mut effect = original.clone();
    effect.batches[0][0].effect_id = EffectId::new("other-effect").unwrap();
    let mut policy = original.clone();
    policy.batches[0][0].recovery = RecoveryPolicy::Unsafe;
    let mut approval = original.clone();
    approval.batches[0][0].requires_approval = false;
    let mut budget = original.clone();
    budget.max_attempts += 1;
    for changed in [route, revision, effect, policy, approval, budget] {
        let changed_request = ApprovalRecoveryRequest {
            available_definition: changed,
            ..request.clone()
        };
        assert_eq!(
            service.recover(&execution(), captured, changed_request.clone()),
            Err(ApprovalError::RecoveryDefinitionMismatch)
        );
        assert_eq!(
            service.store().commit_approval(
                &execution(),
                captured,
                plan.clone(),
                ApprovalMutation::Recover(changed_request)
            ),
            Err(ApprovalError::RecoveryDefinitionMismatch)
        );
    }
    let mut forged_resume = transition(
        &before.as_ref().unwrap().execution.snapshot,
        KernelCommand::Resume {
            route: original.route.clone(),
            turn_id: request.turn_id.clone(),
            decisions: vec![Decision {
                interrupt_id: InterruptId::new("release-interrupt").unwrap(),
                decision: ApprovalDecision::Once,
            }],
        },
        request.facts.clone(),
    )
    .unwrap();
    forged_resume.snapshot.owner_generation = service.store().owner_generation();
    assert_eq!(
        service.store().commit_approval(
            &execution(),
            captured,
            forged_resume,
            ApprovalMutation::Recover(request.clone())
        ),
        Err(ApprovalError::InvalidProposal)
    );
    let mut forged_time = plan;
    forged_time.snapshot.updated_at_ms += 1;
    assert_eq!(
        service.store().commit_approval(
            &execution(),
            captured,
            forged_time,
            ApprovalMutation::Recover(request)
        ),
        Err(ApprovalError::InvalidProposal)
    );
    assert_eq!(service.store().approval_view(&execution()).unwrap(), before);
    assert_eq!(service.store().journal(&execution()).unwrap(), journal);
    assert_eq!(directory.authority_records(), bytes);
}

#[test]
fn journal_failure_rolls_back_all_recovery_changes_and_reopen_keeps_original_authority() {
    for accepted in [false, true] {
        let directory = Directory::new();
        let (service, _) = started(&directory, RecoveryPolicy::Pure);
        if accepted {
            accept(&service);
        } else {
            park(&service);
        }
        let service = reopen(&directory, service);
        let before = service.store().approval_view(&execution()).unwrap();
        let journal = service.store().journal(&execution()).unwrap();
        let bytes = directory.authority_records();
        directory.connection().execute_batch(
            "CREATE TRIGGER abort_recovery BEFORE INSERT ON journal BEGIN SELECT RAISE(ABORT, 'injected'); END"
        ).unwrap();
        assert!(matches!(
            service.recover(
                &execution(),
                fence(service.store()),
                recovery_request(service.store(), "recovered-turn")
            ),
            Err(ApprovalError::Store(StoreError::Unavailable(_)))
        ));
        assert_eq!(service.store().owner_generation(), 2);
        assert_eq!(service.store().approval_view(&execution()).unwrap(), before);
        assert_eq!(service.store().journal(&execution()).unwrap(), journal);
        assert_eq!(directory.authority_records(), bytes);
        // Reopen validates the exact schema; remove the controlled failure
        // trigger after proving that the failed transaction rolled back.
        directory
            .connection()
            .execute_batch("DROP TRIGGER abort_recovery")
            .unwrap();
        drop(service);
        let store = SqliteStore::open(&directory.0).unwrap();
        assert_eq!(store.owner_generation(), 3);
        assert_eq!(store.approval_view(&execution()).unwrap(), before);
        assert_eq!(store.journal(&execution()).unwrap(), journal);
        assert_eq!(directory.authority_records(), bytes);
        let service = ApprovalService::new(store, Allow).unwrap();
        let recovered = recover(&service, "recovered-after-failure");
        assert_eq!(recovered.snapshot.owner_generation, 3);
        assert_eq!(directory.authority_records(), bytes);
    }
}

fn claim_attempt(service: &Service) -> StoredExecution {
    let current = service.store().execution(&execution()).unwrap().unwrap();
    service
        .store()
        .commit(
            &execution(),
            (&current.snapshot).into(),
            transition(
                &current.snapshot,
                KernelCommand::Dispatch {
                    task_id: task(),
                    attempt_id: AttemptId::new("lost-attempt").unwrap(),
                },
                facts(104),
            )
            .unwrap(),
        )
        .unwrap()
}

#[test]
fn pure_inflight_recovery_preserves_attempt_budget_and_allows_one_safe_retry() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    accept(&service);
    let inflight = claim_attempt(&service);
    let before = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    let bytes = directory.authority_records();
    let service = reopen(&directory, service);
    let recovered = recover(&service, "recovered-turn");
    assert_eq!(recovered.snapshot.status, ExecutionStatus::Running);
    assert_eq!(recovered.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(recovered.snapshot.definition.max_attempts, 3);
    assert_eq!(
        recovered.snapshot.attempt_history,
        inflight.snapshot.attempt_history
    );
    assert_eq!(
        recovered.snapshot.tasks[0].attempt_id,
        inflight.snapshot.tasks[0].attempt_id
    );
    let calls = AtomicUsize::new(0);
    let finished = service
        .dispatch_outcome(
            &context(),
            &execution(),
            fence(service.store()),
            task(),
            AttemptId::new("retry-attempt").unwrap(),
            facts(2_001),
            || facts(2_002),
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(EffectOutcome::Succeeded(TaskResult {
                    output: json!("retried"),
                    writes: vec![],
                }))
            },
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(finished.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(finished.snapshot.attempt_history.len(), 2);
    assert_eq!(
        finished.snapshot.attempt_history,
        vec![
            AttemptId::new("lost-attempt").unwrap(),
            AttemptId::new("retry-attempt").unwrap()
        ]
    );
    let after = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    assert_eq!(after.grants, before.grants);
    assert_eq!(after.acceptances, before.acceptances);
    assert_eq!(directory.authority_records(), bytes);
}

#[test]
fn unsafe_inflight_recovery_blocks_and_remains_blocked_across_another_owner() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Unsafe);
    accept(&service);
    let inflight = claim_attempt(&service);
    let bytes = directory.authority_records();
    let service = reopen(&directory, service);
    let blocked = recover(&service, "recovered-turn");
    assert_eq!(blocked.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(blocked.snapshot.tasks[0].status, TaskStatus::OutcomeUnknown);
    assert_eq!(
        blocked.snapshot.attempt_history,
        inflight.snapshot.attempt_history
    );
    assert_eq!(
        blocked.snapshot.failure.as_deref(),
        Some("effect_outcome_unknown")
    );
    assert_eq!(directory.authority_records(), bytes);
    let service = reopen(&directory, service);
    let again = recover(&service, "next-owner-turn");
    assert_eq!(again.snapshot.owner_generation, 3);
    assert_eq!(again.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(again.snapshot.tasks, blocked.snapshot.tasks);
    assert_eq!(again.snapshot.failure, blocked.snapshot.failure);
    assert_eq!(directory.authority_records(), bytes);
    assert!(matches!(
        service.dispatch_outcome(
            &context(),
            &execution(),
            fence(service.store()),
            task(),
            AttemptId::new("forbidden-attempt").unwrap(),
            facts(2_001),
            || facts(2_002),
            |_| panic!("unknown unsafe effect must not run")
        ),
        Err(ApprovalError::Kernel(_))
    ));
}

#[test]
fn cancel_requested_pure_inflight_recovery_settles_cancelled_without_another_attempt() {
    let directory = Directory::new();
    let (service, _) = started(&directory, RecoveryPolicy::Pure);
    accept(&service);
    let inflight = claim_attempt(&service);
    let cancel = transition(
        &inflight.snapshot,
        KernelCommand::Cancel {
            turn_id: inflight.snapshot.turn_id.clone(),
            owner_generation: service.store().owner_generation(),
        },
        facts(105),
    )
    .unwrap();
    let cancelling = service
        .store()
        .commit(&execution(), (&inflight.snapshot).into(), cancel)
        .unwrap();
    assert_eq!(cancelling.snapshot.status, ExecutionStatus::CancelRequested);
    let before = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    let bytes = directory.authority_records();
    let service = reopen(&directory, service);
    let cancelled = recover(&service, "recovered-turn");
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(cancelled.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert_eq!(
        cancelled.snapshot.attempt_history,
        cancelling.snapshot.attempt_history
    );
    let after = service
        .store()
        .approval_view(&execution())
        .unwrap()
        .unwrap();
    assert_eq!(after.thread.active_execution_id, Some(execution()));
    assert_eq!(after.grants, before.grants);
    assert_eq!(after.acceptances, before.acceptances);
    assert_eq!(directory.authority_records(), bytes);
    drop(service);
    let store = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(store.execution(&execution()).unwrap(), Some(cancelled));
    assert_eq!(
        store
            .thread_view(&thread())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::Cancelled
    );
}
