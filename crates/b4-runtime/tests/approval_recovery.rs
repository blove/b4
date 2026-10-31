use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

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
fn id() -> ExecutionId {
    ExecutionId::new("recover-execution").unwrap()
}
fn task() -> TaskId {
    TaskId::new("work").unwrap()
}
fn fixture(store: &MemoryStore, recovery: RecoveryPolicy, approval: bool) -> ApprovalView {
    let thread = ThreadId::new("recover-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    store
        .admit(
            &thread,
            b4_kernel::new_execution(
                id(),
                TurnId::new("turn-1").unwrap(),
                store.owner_generation(),
                Definition {
                    route: RouteKey::new("/recover#agent").unwrap(),
                    revision: ExecutionRevision::new("v1").unwrap(),
                    batches: vec![vec![TaskSpec {
                        id: task(),
                        effect_id: EffectId::new("effect").unwrap(),
                        recovery,
                        requires_approval: approval,
                    }]],
                    max_attempts: 3,
                },
                Map::new(),
                facts(1),
            )
            .unwrap(),
        )
        .unwrap();
    store.approval_view(&id()).unwrap().unwrap()
}
fn fence(view: &ApprovalView) -> ApprovalFence {
    ApprovalFence {
        execution: (&view.execution.snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}
fn request(view: &ApprovalView) -> ApprovalRecoveryRequest {
    ApprovalRecoveryRequest {
        context: context(),
        thread_id: view.thread.id.clone(),
        thread_generation: view.thread.generation.clone(),
        execution_id: id(),
        available_definition: view.execution.snapshot.definition.clone(),
        turn_id: TurnId::new("turn-2").unwrap(),
        facts: facts(10),
    }
}
fn recovery_plan(view: &ApprovalView, request: &ApprovalRecoveryRequest, owner: u64) -> CommitPlan {
    b4_kernel::transition(
        &view.execution.snapshot,
        KernelCommand::Recover {
            available_revision: request.available_definition.revision.clone(),
            turn_id: request.turn_id.clone(),
            owner_generation: owner,
        },
        request.facts.clone(),
    )
    .unwrap()
}
fn apply(store: &MemoryStore, command: KernelCommand) -> ApprovalView {
    let current = store.execution(&id()).unwrap().unwrap();
    let plan = b4_kernel::transition(
        &current.snapshot,
        command,
        facts(current.snapshot.updated_at_ms + 1),
    )
    .unwrap();
    store
        .commit(&id(), (&current.snapshot).into(), plan)
        .unwrap();
    store.approval_view(&id()).unwrap().unwrap()
}
struct Allow;
impl ApprovalAuthority for Allow {
    fn authorize(
        &self,
        _: &ApprovalContext,
        operation: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        assert_eq!(operation, ApprovalOperation::Recover);
        Ok(())
    }
}

#[test]
fn current_owner_recovery_is_rejected_without_mutation() {
    let store = Arc::new(MemoryStore::new());
    let before = fixture(&store, RecoveryPolicy::Pure, false);
    let journal = store.journal(&id()).unwrap();
    let service =
        ApprovalService::with_config(store.clone(), Allow, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    assert_eq!(
        service.recover(&id(), fence(&before), request(&before)),
        Err(ApprovalError::RecoveryNotNeeded)
    );
    assert_eq!(store.approval_view(&id()).unwrap(), Some(before));
    assert_eq!(store.journal(&id()).unwrap(), journal);
    store.readiness().unwrap();
}

#[test]
fn shared_recovery_validator_requires_exact_identity_fences_and_definition() {
    let store = MemoryStore::new();
    let view = fixture(&store, RecoveryPolicy::Pure, false);
    let request = request(&view);
    let owner = store.owner_generation() + 1;
    let plan = b4_kernel::transition(
        &view.execution.snapshot,
        KernelCommand::Recover {
            available_revision: request.available_definition.revision.clone(),
            turn_id: request.turn_id.clone(),
            owner_generation: owner,
        },
        request.facts.clone(),
    )
    .unwrap();
    let changes = validate_approval_change(
        &view,
        fence(&view),
        &plan,
        &ApprovalMutation::Recover(request.clone()),
        owner,
    )
    .unwrap();
    assert!(changes.grants.is_empty());
    assert!(changes.acceptance.is_none());
    let mut changed = request.clone();
    changed.thread_generation = ThreadGeneration::new("0".repeat(64)).unwrap();
    assert_eq!(
        validate_approval_change(
            &view,
            fence(&view),
            &plan,
            &ApprovalMutation::Recover(changed),
            owner,
        ),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    );
    let mut changed = request;
    changed.available_definition.max_attempts += 1;
    assert_eq!(
        validate_approval_change(
            &view,
            fence(&view),
            &plan,
            &ApprovalMutation::Recover(changed),
            owner,
        ),
        Err(ApprovalError::RecoveryDefinitionMismatch)
    );
}

struct ReadProbe {
    inner: Arc<MemoryStore>,
    sensitive_reads: AtomicUsize,
    direct_execution_reads: AtomicUsize,
}
impl Store for ReadProbe {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.direct_execution_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.execution(id)
    }
    fn admit(&self, thread: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(thread, initial)
    }
    fn admit_fenced(
        &self,
        thread: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner
            .admit_fenced(thread, generation, revision, initial)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}
impl ApprovalStore for ReadProbe {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, _: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.sensitive_reads.fetch_add(1, Ordering::SeqCst);
        Err(StoreError::Corrupt("protected_decoder_failure".into()).into())
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
struct Deny(Arc<AtomicUsize>);
impl ApprovalAuthority for Deny {
    fn authorize(
        &self,
        _: &ApprovalContext,
        operation: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        assert_eq!(operation, ApprovalOperation::Recover);
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(StoreError::Unavailable("protected_policy_failure".into()).into())
    }
}

#[test]
fn denied_recovery_authorizes_thread_before_target_diagnostics_or_sensitive_reads() {
    let memory = Arc::new(MemoryStore::new());
    let view = fixture(&memory, RecoveryPolicy::Pure, false);
    let calls = Arc::new(AtomicUsize::new(0));
    let service = ApprovalService::with_config(
        ReadProbe {
            inner: memory.clone(),
            sensitive_reads: AtomicUsize::new(0),
            direct_execution_reads: AtomicUsize::new(0),
        },
        Deny(calls.clone()),
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap();
    let mut wrong = request(&view);
    wrong.execution_id = ExecutionId::new("missing-execution").unwrap();
    wrong.thread_generation = ThreadGeneration::new("0".repeat(64)).unwrap();
    wrong.available_definition.max_attempts += 1;
    let mut stale = fence(&view);
    stale.resource_revision += 1;
    assert_eq!(
        service.recover(&wrong.execution_id.clone(), stale, wrong),
        Err(ApprovalError::Unauthorized)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(service.store().sensitive_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        service
            .store()
            .direct_execution_reads
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(memory.approval_view(&id()).unwrap(), Some(view));
}

#[test]
fn foreign_origin_and_absent_thread_reject_without_authority_or_sensitive_reads() {
    let memory = Arc::new(MemoryStore::new());
    let view = fixture(&memory, RecoveryPolicy::Pure, false);
    let calls = Arc::new(AtomicUsize::new(0));
    let service = ApprovalService::with_config(
        ReadProbe {
            inner: memory,
            sensitive_reads: AtomicUsize::new(0),
            direct_execution_reads: AtomicUsize::new(0),
        },
        Deny(calls.clone()),
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap();
    let mut foreign = request(&view);
    foreign.context.actor.principal = "mallory".into();
    assert_eq!(
        service.recover(&id(), fence(&view), foreign),
        Err(ApprovalError::Unauthorized)
    );
    let mut absent = request(&view);
    absent.thread_id = ThreadId::new("missing-thread").unwrap();
    assert_eq!(
        service.recover(&id(), fence(&view), absent),
        Err(ApprovalError::Unauthorized)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(service.store().sensitive_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        service
            .store()
            .direct_execution_reads
            .load(Ordering::SeqCst),
        0
    );
}

#[test]
fn shared_recovery_validator_rejects_resource_only_races_and_wrong_targets() {
    let store = MemoryStore::new();
    let view = fixture(&store, RecoveryPolicy::Pure, false);
    let request = request(&view);
    let owner = store.owner_generation() + 1;
    let plan = recovery_plan(&view, &request, owner);
    let mut resource_changed = view.clone();
    resource_changed.thread.resource_revision += 1;
    assert_eq!(
        validate_approval_change(
            &resource_changed,
            fence(&view),
            &plan,
            &ApprovalMutation::Recover(request.clone()),
            owner
        ),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    );
    for field in ["thread", "execution"] {
        let mut changed = request.clone();
        match field {
            "thread" => changed.thread_id = ThreadId::new("wrong-thread").unwrap(),
            _ => changed.execution_id = ExecutionId::new("wrong-execution").unwrap(),
        }
        assert_eq!(
            validate_approval_change(
                &view,
                fence(&view),
                &plan,
                &ApprovalMutation::Recover(changed),
                owner
            ),
            Err(ApprovalError::Store(StoreError::ExecutionNotActive))
        );
    }
    let mut stale_owner = fence(&view);
    stale_owner.execution.owner_generation += 1;
    assert_eq!(
        validate_approval_change(
            &view,
            stale_owner,
            &plan,
            &ApprovalMutation::Recover(request.clone()),
            owner
        ),
        Err(ApprovalError::Store(StoreError::OwnerConflict))
    );
    let mut stale_revision = fence(&view);
    stale_revision.execution.revision += 1;
    assert_eq!(
        validate_approval_change(
            &view,
            stale_revision,
            &plan,
            &ApprovalMutation::Recover(request),
            owner
        ),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    );
}

#[test]
fn shared_recovery_validator_rejects_forged_non_recovery_and_dispatch_plans() {
    let store = MemoryStore::new();
    let view = fixture(&store, RecoveryPolicy::Pure, false);
    let request = request(&view);
    let owner = store.owner_generation() + 1;
    let mut start =
        b4_kernel::transition(&view.execution.snapshot, KernelCommand::Start, facts(10)).unwrap();
    start.snapshot.owner_generation = owner;
    assert_eq!(
        validate_approval_change(
            &view,
            fence(&view),
            &start,
            &ApprovalMutation::Recover(request.clone()),
            owner
        ),
        Err(ApprovalError::InvalidProposal)
    );
    let mut plan = recovery_plan(&view, &request, owner);
    plan.dispatchable.push(task());
    assert_eq!(
        validate_approval_change(
            &view,
            fence(&view),
            &plan,
            &ApprovalMutation::Recover(request),
            owner
        ),
        Err(ApprovalError::InvalidProposal)
    );
}

#[test]
fn pure_recovery_retains_attempt_budget_and_history() {
    let store = MemoryStore::new();
    fixture(&store, RecoveryPolicy::Pure, false);
    apply(&store, KernelCommand::Start);
    let view = apply(
        &store,
        KernelCommand::Dispatch {
            task_id: task(),
            attempt_id: AttemptId::new("attempt-1").unwrap(),
        },
    );
    let request = request(&view);
    let owner = store.owner_generation() + 1;
    let plan = recovery_plan(&view, &request, owner);
    validate_approval_change(
        &view,
        fence(&view),
        &plan,
        &ApprovalMutation::Recover(request),
        owner,
    )
    .unwrap();
    assert_eq!(plan.snapshot.status, ExecutionStatus::Running);
    assert_eq!(plan.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(
        plan.snapshot.attempts_used,
        view.execution.snapshot.attempts_used
    );
    assert_eq!(
        plan.snapshot.attempt_history,
        view.execution.snapshot.attempt_history
    );
    assert_eq!(plan.snapshot.definition, view.execution.snapshot.definition);
}

#[test]
fn unsafe_recovery_adopts_conservatively_without_approval_changes() {
    let store = MemoryStore::new();
    fixture(&store, RecoveryPolicy::Unsafe, false);
    apply(&store, KernelCommand::Start);
    let view = apply(
        &store,
        KernelCommand::Dispatch {
            task_id: task(),
            attempt_id: AttemptId::new("attempt-1").unwrap(),
        },
    );
    let request = request(&view);
    let owner = store.owner_generation() + 1;
    let plan = recovery_plan(&view, &request, owner);
    let changes = validate_approval_change(
        &view,
        fence(&view),
        &plan,
        &ApprovalMutation::Recover(request),
        owner,
    )
    .unwrap();
    assert_eq!(plan.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(plan.snapshot.tasks[0].status, TaskStatus::OutcomeUnknown);
    assert!(plan.dispatchable.is_empty());
    assert!(changes.grants.is_empty());
    assert!(changes.acceptance.is_none());
}

#[test]
fn cancellation_recovery_settles_without_reissuing_approval() {
    let store = MemoryStore::new();
    fixture(&store, RecoveryPolicy::Pure, false);
    apply(&store, KernelCommand::Start);
    apply(
        &store,
        KernelCommand::Dispatch {
            task_id: task(),
            attempt_id: AttemptId::new("attempt-1").unwrap(),
        },
    );
    let view = apply(
        &store,
        KernelCommand::Cancel {
            turn_id: TurnId::new("turn-1").unwrap(),
            owner_generation: store.owner_generation(),
        },
    );
    let request = request(&view);
    let owner = store.owner_generation() + 1;
    let plan = recovery_plan(&view, &request, owner);
    validate_approval_change(
        &view,
        fence(&view),
        &plan,
        &ApprovalMutation::Recover(request),
        owner,
    )
    .unwrap();
    assert_eq!(plan.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(plan.snapshot.tasks[0].status, TaskStatus::Cancelled);
    assert!(plan.dispatchable.is_empty());
}

#[test]
fn recovery_preserves_committed_grants_and_acceptance_without_expiry_checks() {
    let store = Arc::new(MemoryStore::new());
    fixture(&store, RecoveryPolicy::Pure, true);
    apply(&store, KernelCommand::Start);
    struct All;
    impl ApprovalAuthority for All {
        fn authorize(
            &self,
            _: &ApprovalContext,
            _: ApprovalOperation,
            _: &ApprovalResource,
        ) -> Result<(), ApprovalError> {
            Ok(())
        }
    }
    let service =
        ApprovalService::with_config(store.clone(), All, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    let before = store.approval_view(&id()).unwrap().unwrap();
    service
        .park(
            &context(),
            &id(),
            fence(&before),
            task(),
            InterruptId::new("interrupt").unwrap(),
            json!({"message":"Proceed?"}),
            5,
            facts(3),
        )
        .unwrap();
    let parked = store.approval_view(&id()).unwrap().unwrap();
    let pending = service.pending(&context(), &id(), fence(&parked)).unwrap();
    service
        .accept(
            &id(),
            fence(&parked),
            ApprovalAcceptanceRequest {
                context: context(),
                route: parked.execution.snapshot.definition.route.clone(),
                definition_revision: parked.execution.snapshot.definition.revision.clone(),
                turn_id: TurnId::new("accepted-turn").unwrap(),
                now_ms: 4,
                decisions: vec![GrantDecision {
                    interrupt_id: pending[0].interrupt_id.clone(),
                    decision: ApprovalDecision::Once,
                    cancelled: false,
                    token: Some(pending[0].token.clone()),
                }],
            },
        )
        .unwrap();
    let view = store.approval_view(&id()).unwrap().unwrap();
    let request = request(&view);
    assert!(request.facts.now_ms > view.grants[0].expires_at_ms);
    let owner = store.owner_generation() + 1;
    let plan = recovery_plan(&view, &request, owner);
    let changes = validate_approval_change(
        &view,
        fence(&view),
        &plan,
        &ApprovalMutation::Recover(request),
        owner,
    )
    .unwrap();
    assert!(changes.grants.is_empty());
    assert!(changes.acceptance.is_none());
    assert_eq!(plan.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(plan.snapshot.tasks[0].approved);
    assert_eq!(store.approval_view(&id()).unwrap(), Some(view));
}
