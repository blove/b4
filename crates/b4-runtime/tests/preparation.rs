use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};

fn id() -> ExecutionId {
    ExecutionId::new("execution").unwrap()
}
fn task(name: &str) -> TaskId {
    TaskId::new(name).unwrap()
}
fn attempt(name: &str) -> AttemptId {
    AttemptId::new(name).unwrap()
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
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
fn spec(name: &str, approval: bool) -> TaskSpec {
    TaskSpec {
        id: task(name),
        effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
        recovery: RecoveryPolicy::Pure,
        requires_approval: approval,
    }
}

struct ProbeStore {
    inner: MemoryStore,
    owner: AtomicU64,
    sensitive_reads: AtomicUsize,
    fail_dispatch: AtomicBool,
    erase_acceptance: AtomicBool,
}
impl ProbeStore {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            owner: AtomicU64::new(1),
            sensitive_reads: AtomicUsize::new(0),
            fail_dispatch: AtomicBool::new(false),
            erase_acceptance: AtomicBool::new(false),
        }
    }
}
impl Store for ProbeStore {
    fn owner_generation(&self) -> u64 {
        self.owner.load(Ordering::SeqCst)
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
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, initial)
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
        if self.fail_dispatch.load(Ordering::SeqCst)
            && plan
                .snapshot
                .tasks
                .iter()
                .any(|task| task.status == TaskStatus::InFlight)
        {
            return Err(StoreError::Unavailable("dispatch_commit_failed".into()));
        }
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}
impl ApprovalStore for ProbeStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.sensitive_reads.fetch_add(1, Ordering::SeqCst);
        let mut view = self.inner.approval_view(id)?;
        if self.erase_acceptance.load(Ordering::SeqCst) {
            if let Some(view) = &mut view {
                view.acceptances.clear();
            }
        }
        Ok(view)
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

#[derive(Clone, Copy)]
enum PolicyAction {
    Allow,
    DenyBefore,
    DenyRenewal,
    CancelRenewal,
    CompleteSiblingRenewal,
    RecreateBefore,
    RecreateRenewal,
}
struct Authority {
    store: Arc<ProbeStore>,
    dispatch_calls: AtomicUsize,
    action: PolicyAction,
}
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if operation != ApprovalOperation::Dispatch {
            return Ok(());
        }
        let call = self.dispatch_calls.fetch_add(1, Ordering::SeqCst);
        match (self.action, call) {
            (PolicyAction::DenyBefore, 0) | (PolicyAction::DenyRenewal, 1) => {
                Err(ApprovalError::Unauthorized)
            }
            (PolicyAction::CancelRenewal, 1) => {
                let view = ThreadView {
                    thread: resource.thread.clone(),
                    execution: Some(resource.execution.clone()),
                };
                self.store.cancel_run_fenced(
                    &RunCancellationTarget::from_view(&view).unwrap(),
                    facts(6),
                )?;
                Ok(())
            }
            (PolicyAction::CompleteSiblingRenewal, 1) => {
                Runtime::new(&self.store).complete_outcome(
                    &id(),
                    resource.execution.snapshot.owner_generation,
                    task("sibling"),
                    attempt("sibling-attempt"),
                    &EffectOutcome::Succeeded(TaskResult {
                        output: json!("sibling-result"),
                        writes: vec![StateWrite::Append {
                            field: "items".into(),
                            items: vec![json!("sibling")],
                        }],
                    }),
                    facts(6),
                )?;
                Ok(())
            }
            (PolicyAction::RecreateBefore, 0) | (PolicyAction::RecreateRenewal, 1) => {
                let captured = ThreadView {
                    thread: resource.thread.clone(),
                    execution: Some(resource.execution.clone()),
                };
                let cancelled = self.store.inner.cancel_run_fenced(
                    &RunCancellationTarget::from_view(&captured).unwrap(),
                    facts(6),
                )?;
                if cancelled.snapshot.status == ExecutionStatus::CancelRequested {
                    Runtime::new(&self.store).complete_outcome(
                        &id(),
                        cancelled.snapshot.owner_generation,
                        task("a"),
                        attempt("attempt-a"),
                        &EffectOutcome::Cancelled,
                        facts(7),
                    )?;
                }
                let terminal = self.store.thread_view(&resource.thread.id)?.unwrap();
                self.store
                    .inner
                    .delete_thread_fenced(&ThreadDeletionTarget::from_view(&terminal).unwrap())?;
                let mut replacement_actor = context().actor;
                replacement_actor.principal = "bob".into();
                let replacement =
                    started_with_actor(&self.store, vec![spec("a", false)], replacement_actor);
                if call == 1 {
                    Runtime::new(&self.store).apply(
                        &id(),
                        (&replacement.snapshot).into(),
                        KernelCommand::Dispatch {
                            task_id: task("a"),
                            attempt_id: attempt("attempt-a"),
                        },
                        facts(5),
                    )?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
fn started(store: &ProbeStore, specs: Vec<TaskSpec>) -> StoredExecution {
    started_with_actor(store, specs, context().actor)
}
fn started_with_actor(
    store: &ProbeStore,
    specs: Vec<TaskSpec>,
    actor: ThreadOrigin,
) -> StoredExecution {
    let thread = ThreadId::new("thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(actor),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        id(),
        TurnId::new("turn-1").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/approval#agent").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![specs],
            max_attempts: 8,
        },
        Map::from_iter([("input".into(), json!({"nested":[null,2],"ok":true}))]),
        facts(1),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    Runtime::new(store)
        .apply(
            &id(),
            (&admitted.snapshot).into(),
            KernelCommand::Start,
            facts(2),
        )
        .unwrap()
}
fn service(
    store: Arc<ProbeStore>,
    action: PolicyAction,
) -> ApprovalService<Arc<ProbeStore>, Authority> {
    ApprovalService::with_config(
        store.clone(),
        Authority {
            store,
            dispatch_calls: AtomicUsize::new(0),
            action,
        },
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap()
}
fn fence(store: &ProbeStore) -> ApprovalFence {
    let execution = store.execution(&id()).unwrap().unwrap();
    let thread = store.thread(&execution.thread_id).unwrap().unwrap();
    ApprovalFence {
        execution: (&execution.snapshot).into(),
        resource_revision: thread.resource_revision,
    }
}
fn approved(store: &Arc<ProbeStore>) -> StoredExecution {
    let service = service(store.clone(), PolicyAction::Allow);
    service
        .park(
            &context(),
            &id(),
            fence(store),
            task("a"),
            InterruptId::new("interrupt-a").unwrap(),
            json!("Proceed?"),
            100,
            facts(3),
        )
        .unwrap();
    let pending = service.pending(&context(), &id(), fence(store)).unwrap();
    service
        .accept(
            &id(),
            fence(store),
            ApprovalAcceptanceRequest {
                context: context(),
                route: RouteKey::new("/approval#agent").unwrap(),
                definition_revision: ExecutionRevision::new("v1").unwrap(),
                turn_id: TurnId::new("turn-2").unwrap(),
                now_ms: 4,
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
fn prepared_claim(prepared: &PreparedEffect, expected: &StoredExecution) {
    let current = &prepared.execution.snapshot;
    assert_eq!(current.revision, expected.snapshot.revision + 1);
    assert_eq!(current.attempts_used, expected.snapshot.attempts_used + 1);
    let claimed = current
        .tasks
        .iter()
        .find(|entry| entry.spec.id == task("a"))
        .unwrap();
    assert_eq!(claimed.status, TaskStatus::InFlight);
    assert_eq!(claimed.attempt_id, Some(attempt("attempt-a")));
    assert_eq!(prepared.invocation.execution_id, id());
    assert_eq!(prepared.invocation.turn_id, current.turn_id);
    assert_eq!(
        prepared.invocation.owner_generation,
        current.owner_generation
    );
    assert_eq!(prepared.invocation.task_id, task("a"));
    assert_eq!(prepared.invocation.effect_id.as_str(), "effect-a");
    assert_eq!(prepared.invocation.attempt_id, attempt("attempt-a"));
    assert_eq!(prepared.invocation.input, claimed.input);
    assert_eq!(claimed.input, expected.snapshot.tasks[0].input);
}

#[test]
fn native_preparation_durably_claims_exact_input_without_an_effect_callback() {
    let store = ProbeStore::new();
    let running = started(&store, vec![spec("a", false)]);
    let runtime = Runtime::new(&store);
    let prepared = runtime
        .prepare_dispatch(
            &id(),
            (&running.snapshot).into(),
            task("a"),
            attempt("attempt-a"),
            facts(3),
        )
        .unwrap();
    prepared_claim(&prepared, &running);
    assert_eq!(
        store.execution(&id()).unwrap(),
        Some(prepared.execution.clone())
    );
    assert_eq!(store.journal(&id()).unwrap().len(), 3);
    assert!(prepared.execution.snapshot.tasks[0].result.is_none());
    let done = runtime
        .complete_outcome(
            &id(),
            prepared.invocation.owner_generation,
            task("a"),
            attempt("attempt-a"),
            &EffectOutcome::Succeeded(TaskResult {
                output: json!("actual-result"),
                writes: vec![],
            }),
            facts(4),
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
}

#[test]
fn native_preclaim_refusals_have_no_claim_evidence_or_budget_spend() {
    let store = ProbeStore::new();
    let running = started(&store, vec![spec("a", false)]);
    let runtime = Runtime::new(&store);
    let mut stale = ExecutionFence::from(&running.snapshot);
    stale.revision -= 1;
    let failure = runtime
        .prepare_dispatch(&id(), stale, task("a"), attempt("attempt-a"), facts(3))
        .unwrap_err();
    assert_eq!(
        failure.cause,
        RuntimeError::Store(StoreError::RevisionConflict)
    );
    assert!(failure.claimed.is_none());
    store.owner.store(2, Ordering::SeqCst);
    let failure = runtime
        .prepare_dispatch(
            &id(),
            (&running.snapshot).into(),
            task("a"),
            attempt("attempt-a"),
            facts(3),
        )
        .unwrap_err();
    assert_eq!(
        failure.cause,
        RuntimeError::Store(StoreError::OwnerConflict)
    );
    assert!(failure.claimed.is_none());
    assert_eq!(store.execution(&id()).unwrap(), Some(running));
    assert_eq!(store.journal(&id()).unwrap().len(), 2);
}

#[test]
fn failed_dispatch_transaction_never_claims_an_attempt() {
    let store = ProbeStore::new();
    let running = started(&store, vec![spec("a", false)]);
    store.fail_dispatch.store(true, Ordering::SeqCst);
    let failure = Runtime::new(&store)
        .prepare_dispatch(
            &id(),
            (&running.snapshot).into(),
            task("a"),
            attempt("attempt-a"),
            facts(3),
        )
        .unwrap_err();
    assert_eq!(
        failure.cause,
        RuntimeError::Store(StoreError::Unavailable("dispatch_commit_failed".into()))
    );
    assert!(failure.claimed.is_none());
    assert_eq!(store.execution(&id()).unwrap(), Some(running));
}

#[test]
fn required_preparation_preserves_consumed_once_after_token_expiry() {
    let store = Arc::new(ProbeStore::new());
    started(&store, vec![spec("a", true)]);
    let accepted = approved(&store);
    let before = store.inner.approval_view(&id()).unwrap().unwrap();
    let prepared = service(store.clone(), PolicyAction::Allow)
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(104),
        )
        .unwrap();
    prepared_claim(&prepared, &accepted);
    let after = store.inner.approval_view(&id()).unwrap().unwrap();
    assert_eq!(before.grants, after.grants);
    assert_eq!(before.acceptances, after.acceptances);
    assert!(after.grants[0].consumed);
    assert!(prepared.execution.snapshot.tasks[0].result.is_none());
    let token = after.grants[0].token.as_str();
    assert!(!format!("{prepared:?}").contains(token));
}

#[test]
fn denied_initial_authority_precedes_sensitive_decode_and_has_no_claim() {
    let store = Arc::new(ProbeStore::new());
    let running = started(&store, vec![spec("a", false)]);
    let service = service(store.clone(), PolicyAction::DenyBefore);
    let failure = service
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(3),
        )
        .unwrap_err();
    assert_eq!(failure.cause, ApprovalError::Unauthorized);
    assert!(failure.claimed.is_none());
    assert_eq!(store.sensitive_reads.load(Ordering::SeqCst), 0);
    assert_eq!(store.execution(&id()).unwrap(), Some(running));
}

#[test]
fn changed_once_policy_and_missing_acceptance_refuse_before_claim() {
    let store = Arc::new(ProbeStore::new());
    started(&store, vec![spec("a", true)]);
    let accepted = approved(&store);
    let service = service(store.clone(), PolicyAction::Allow);
    let mut changed = context();
    changed.policy_revision = "policy-2".into();
    let failure = service
        .prepare_dispatch(
            &changed,
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(5),
        )
        .unwrap_err();
    assert_eq!(failure.cause, ApprovalError::InvalidGrant);
    assert!(failure.claimed.is_none());
    store.erase_acceptance.store(true, Ordering::SeqCst);
    let failure = service
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(5),
        )
        .unwrap_err();
    assert_eq!(failure.cause, ApprovalError::InvalidGrant);
    assert!(failure.claimed.is_none());
    assert_eq!(store.execution(&id()).unwrap(), Some(accepted));
}

#[test]
fn renewed_policy_refusal_returns_exact_never_entered_claim() {
    let store = Arc::new(ProbeStore::new());
    let running = started(&store, vec![spec("a", false)]);
    let failure = service(store.clone(), PolicyAction::DenyRenewal)
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(5),
        )
        .unwrap_err();
    assert_eq!(failure.cause, ApprovalError::Unauthorized);
    let claimed = failure.claimed.unwrap();
    prepared_claim(&claimed, &running);
    assert_eq!(store.execution(&id()).unwrap(), Some(claimed.execution));
    assert_eq!(store.sensitive_reads.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_during_renewal_retains_original_claim_for_exact_acknowledgement() {
    let store = Arc::new(ProbeStore::new());
    let running = started(&store, vec![spec("a", false)]);
    let failure = service(store.clone(), PolicyAction::CancelRenewal)
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(5),
        )
        .unwrap_err();
    assert_eq!(
        failure.cause,
        ApprovalError::Store(StoreError::RevisionConflict)
    );
    let claimed = failure.claimed.unwrap();
    prepared_claim(&claimed, &running);
    let current = store.execution(&id()).unwrap().unwrap();
    assert_eq!(current.snapshot.status, ExecutionStatus::CancelRequested);
    assert_eq!(
        current.snapshot.tasks[0].attempt_id,
        Some(claimed.invocation.attempt_id.clone())
    );
    let cancelled = Runtime::new(&store)
        .complete_outcome(
            &id(),
            claimed.invocation.owner_generation,
            claimed.invocation.task_id,
            claimed.invocation.attempt_id,
            &EffectOutcome::Cancelled,
            facts(7),
        )
        .unwrap();
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(cancelled.snapshot.attempts_used, 1);
    assert!(cancelled.snapshot.tasks[0].result.is_none());
}

#[test]
fn sibling_commit_during_renewal_is_a_strict_claimed_view_refusal() {
    let store = Arc::new(ProbeStore::new());
    let running = started(&store, vec![spec("a", false), spec("sibling", false)]);
    let sibling = Runtime::new(&store)
        .prepare_dispatch(
            &id(),
            (&running.snapshot).into(),
            task("sibling"),
            attempt("sibling-attempt"),
            facts(3),
        )
        .unwrap();
    let failure = service(store.clone(), PolicyAction::CompleteSiblingRenewal)
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(5),
        )
        .unwrap_err();
    assert_eq!(
        failure.cause,
        ApprovalError::Store(StoreError::RevisionConflict)
    );
    let claimed = failure.claimed.unwrap();
    prepared_claim(&claimed, &sibling.execution);
    let current = store.execution(&id()).unwrap().unwrap();
    assert_eq!(current.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(current.snapshot.tasks[1].status, TaskStatus::Succeeded);
    assert_eq!(
        current.snapshot.tasks[1].result.as_ref().unwrap().output,
        json!("sibling-result")
    );
    assert!(!current.snapshot.values.contains_key("items"));
}

#[test]
fn initial_policy_recreation_is_rejected_before_sensitive_successor_decode() {
    let store = Arc::new(ProbeStore::new());
    let running = started(&store, vec![spec("a", false)]);
    let original_thread = store.thread(&running.thread_id).unwrap().unwrap();
    let failure = service(store.clone(), PolicyAction::RecreateBefore)
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(5),
        )
        .unwrap_err();
    assert_eq!(
        failure.cause,
        ApprovalError::Store(StoreError::RevisionConflict)
    );
    assert!(failure.claimed.is_none());
    assert_eq!(store.sensitive_reads.load(Ordering::SeqCst), 0);
    let successor_thread = store.thread(&running.thread_id).unwrap().unwrap();
    assert_ne!(successor_thread.generation, original_thread.generation);
    assert_eq!(successor_thread.origin.unwrap().principal, "bob");
    assert_eq!(store.execution(&id()).unwrap(), Some(running));
}

#[test]
fn renewed_policy_recreation_preserves_claim_without_sensitive_successor_decode() {
    let store = Arc::new(ProbeStore::new());
    let running = started(&store, vec![spec("a", false)]);
    let original_thread = store.thread(&running.thread_id).unwrap().unwrap();
    let failure = service(store.clone(), PolicyAction::RecreateRenewal)
        .prepare_dispatch(
            &context(),
            &id(),
            fence(&store),
            task("a"),
            attempt("attempt-a"),
            facts(5),
        )
        .unwrap_err();
    assert_eq!(
        failure.cause,
        ApprovalError::Store(StoreError::RevisionConflict)
    );
    let claimed = failure.claimed.unwrap();
    prepared_claim(&claimed, &running);
    assert_eq!(store.sensitive_reads.load(Ordering::SeqCst), 1);
    let successor_thread = store.thread(&running.thread_id).unwrap().unwrap();
    assert_ne!(successor_thread.generation, original_thread.generation);
    assert_eq!(successor_thread.origin.unwrap().principal, "bob");
    // The new incarnation deliberately recreates all legacy execution fence
    // fields; the token-free ThreadRecord generation is the decisive check.
    assert_eq!(store.execution(&id()).unwrap(), Some(claimed.execution));
}
