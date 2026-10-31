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
struct Authority(AtomicUsize);
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn execution_id() -> ExecutionId {
    ExecutionId::new("execution").unwrap()
}
fn task(name: &str) -> TaskId {
    TaskId::new(name).unwrap()
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn started(store: &MemoryStore, names: &[&str]) -> StoredExecution {
    started_tasks(
        store,
        names
            .iter()
            .map(|name| TaskSpec {
                id: task(name),
                effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            })
            .collect(),
    )
}
fn started_tasks(store: &MemoryStore, tasks: Vec<TaskSpec>) -> StoredExecution {
    let thread = ThreadId::new("thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        execution_id(),
        TurnId::new("turn-1").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/approval#agent").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![tasks],
            max_attempts: 8,
        },
        Map::from_iter([("input".into(), json!({"z":[null,2],"a":true}))]),
        facts(1),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution_id(),
            ExecutionFence::from(&admitted.snapshot),
            b4_kernel::transition(&admitted.snapshot, KernelCommand::Start, facts(2)).unwrap(),
        )
        .unwrap()
}
fn fence<S: ApprovalStore>(store: &S) -> ApprovalFence {
    let view = store.approval_view(&execution_id()).unwrap().unwrap();
    ApprovalFence {
        execution: ExecutionFence::from(&view.execution.snapshot),
        resource_revision: view.thread.resource_revision,
    }
}
fn service(store: Arc<MemoryStore>) -> ApprovalService<Arc<MemoryStore>, Authority> {
    ApprovalService::with_config(
        store,
        Authority(AtomicUsize::new(0)),
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap()
}
fn park(
    service: &ApprovalService<Arc<MemoryStore>, Authority>,
    name: &str,
    now: u64,
) -> StoredExecution {
    service
        .park(
            &context(),
            &execution_id(),
            fence(service.store()),
            task(name),
            InterruptId::new(format!("interrupt-{name}")).unwrap(),
            json!({"question":"Proceed?", "nested":{"z":null,"a":[2,1]}}),
            100,
            facts(now),
        )
        .unwrap()
}
fn request(
    pending: &[SensitivePendingApproval],
    decision: ApprovalDecision,
    now_ms: u64,
) -> ApprovalAcceptanceRequest {
    ApprovalAcceptanceRequest {
        context: context(),
        route: RouteKey::new("/approval#agent").unwrap(),
        definition_revision: ExecutionRevision::new("v1").unwrap(),
        turn_id: TurnId::new("turn-2").unwrap(),
        now_ms,
        decisions: pending
            .iter()
            .map(|entry| GrantDecision {
                interrupt_id: entry.interrupt_id.clone(),
                decision,
                cancelled: false,
                token: Some(entry.token.clone()),
            })
            .collect(),
    }
}

#[test]
fn memory_generations_are_server_generated_and_stable() {
    let store = MemoryStore::new();
    let first = store.create_thread(ThreadId::new("a").unwrap()).unwrap();
    let second = store.create_thread(ThreadId::new("b").unwrap()).unwrap();
    assert_ne!(first.generation, second.generation);
    assert_eq!(first.generation.as_str().len(), 64);
    assert_eq!(
        store.thread(&first.id).unwrap().unwrap().generation,
        first.generation
    );
    assert!(serde_json::from_value::<ThreadSeed>(json!({"id":"seed","created_at_ms":0,"metadata":{},"origin":null,"generation":first.generation})).is_err());
}
#[test]
fn required_service_rejects_volatile_storage() {
    assert!(matches!(
        ApprovalService::new(MemoryStore::new(), Authority(AtomicUsize::new(0))),
        Err(ApprovalError::DurabilityRequired)
    ));
}
#[test]
fn exact_set_acceptance_is_atomic_and_secrets_never_enter_snapshot_or_journal() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a", "b"]);
    let service = service(store.clone());
    park(&service, "a", 3);
    assert!(matches!(
        service.pending(&context(), &execution_id(), fence(&store)),
        Err(ApprovalError::NotStablePark)
    ));
    let parked = park(&service, "b", 4);
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending[0].token.as_str().starts_with("b4ag_"));
    assert_eq!(pending[0].token.as_str().len(), 69);
    let secret = pending[0].token.as_str();
    assert!(!format!("{pending:?}").contains(secret));
    assert!(!format!("{:?}", store.approval_view(&execution_id()).unwrap()).contains(secret));
    assert!(!serde_json::to_string(&parked.snapshot)
        .unwrap()
        .contains(secret));
    assert!(
        !serde_json::to_string(&store.journal(&execution_id()).unwrap())
            .unwrap()
            .contains(secret)
    );
    let mut incomplete = request(&pending, ApprovalDecision::Once, 5);
    incomplete.decisions.pop();
    let old_fence = fence(&store);
    assert_eq!(
        service.accept(&execution_id(), old_fence, incomplete),
        Err(ApprovalError::InterruptSetMismatch)
    );
    assert_eq!(store.execution(&execution_id()).unwrap().unwrap(), parked);
    assert!(store
        .approval_view(&execution_id())
        .unwrap()
        .unwrap()
        .grants
        .iter()
        .all(|grant| !grant.consumed));
    let resumed = service
        .accept(
            &execution_id(),
            old_fence,
            request(&pending, ApprovalDecision::Once, 5),
        )
        .unwrap();
    assert!(resumed
        .snapshot
        .tasks
        .iter()
        .all(|task| task.approved && task.status == TaskStatus::Ready));
    let view = store.approval_view(&execution_id()).unwrap().unwrap();
    assert!(view.grants.iter().all(|grant| grant.consumed));
    assert_eq!(view.acceptances.len(), 1);
    assert!(!serde_json::to_string(&view.acceptances)
        .unwrap()
        .contains(secret));
    assert!(service
        .accept(
            &execution_id(),
            old_fence,
            request(&pending, ApprovalDecision::Once, 5)
        )
        .is_err());
}
#[test]
fn authority_precedes_grant_errors_and_wrong_origin_cannot_read() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a"]);
    let service = service(store.clone());
    park(&service, "a", 3);
    let mut unauthorized = context();
    unauthorized.actor.principal = "bob".into();
    assert_eq!(
        service.pending(&unauthorized, &execution_id(), fence(&store)),
        Err(ApprovalError::Unauthorized)
    );
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    let mut invalid = request(&pending, ApprovalDecision::Always, 4);
    invalid.context = unauthorized;
    invalid.decisions.clear();
    assert_eq!(
        service.accept(&execution_id(), fence(&store), invalid),
        Err(ApprovalError::Unauthorized)
    );
}
#[test]
fn unsupported_expired_and_invalid_tokens_spend_nothing() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a"]);
    let service = service(store.clone());
    let parked = park(&service, "a", 3);
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    assert_eq!(
        service.accept(
            &execution_id(),
            fence(&store),
            request(&pending, ApprovalDecision::Always, 4)
        ),
        Err(ApprovalError::UnsupportedAlways)
    );
    let mut missing = request(&pending, ApprovalDecision::Once, 4);
    missing.decisions[0].token = None;
    assert_eq!(
        service.accept(&execution_id(), fence(&store), missing),
        Err(ApprovalError::MissingToken)
    );
    let mut invalid = request(&pending, ApprovalDecision::Once, 4);
    invalid.decisions[0].token =
        Some(ApprovalToken::new(format!("b4ag_{}", "0".repeat(64))).unwrap());
    assert_eq!(
        service.accept(&execution_id(), fence(&store), invalid),
        Err(ApprovalError::InvalidGrant)
    );
    assert_eq!(
        service.accept(
            &execution_id(),
            fence(&store),
            request(&pending, ApprovalDecision::Once, 103)
        ),
        Err(ApprovalError::Expired)
    );
    assert_eq!(store.execution(&execution_id()).unwrap().unwrap(), parked);
    assert!(
        !store
            .approval_view(&execution_id())
            .unwrap()
            .unwrap()
            .grants[0]
            .consumed
    );
}
#[test]
fn legacy_resume_cannot_bypass_required_grant_and_denial_creates_no_dispatch() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a"]);
    let service = service(store.clone());
    let parked = park(&service, "a", 3);
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    let plan = b4_kernel::transition(
        &parked.snapshot,
        KernelCommand::Resume {
            route: parked.snapshot.definition.route.clone(),
            decisions: vec![Decision {
                interrupt_id: pending[0].interrupt_id.clone(),
                decision: ApprovalDecision::Once,
            }],
            turn_id: TurnId::new("trusted-bypass").unwrap(),
        },
        facts(4),
    )
    .unwrap();
    assert!(store
        .commit(
            &execution_id(),
            ExecutionFence::from(&parked.snapshot),
            plan
        )
        .is_err());
    let mut denied = request(&pending, ApprovalDecision::Once, 4);
    denied.decisions[0].cancelled = true;
    let result = service
        .accept(&execution_id(), fence(&store), denied)
        .unwrap();
    assert_eq!(result.snapshot.tasks[0].status, TaskStatus::Denied);
    assert!(!result.snapshot.tasks[0].approved);
}
#[test]
fn dispatch_rechecks_policy_before_effect() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a"]);
    let service = service(store.clone());
    park(&service, "a", 3);
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    service
        .accept(
            &execution_id(),
            fence(&store),
            request(&pending, ApprovalDecision::Once, 4),
        )
        .unwrap();
    let calls = AtomicUsize::new(0);
    let mut changed = context();
    changed.policy_revision = "policy-2".into();
    assert!(service
        .dispatch_outcome(
            &changed,
            &execution_id(),
            fence(&store),
            task("a"),
            AttemptId::new("attempt").unwrap(),
            facts(5),
            || facts(6),
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(EffectOutcome::Succeeded(TaskResult {
                    output: json!(true),
                    writes: vec![],
                }))
            }
        )
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let done = service
        .dispatch_outcome(
            &context(),
            &execution_id(),
            fence(&store),
            task("a"),
            AttemptId::new("attempt").unwrap(),
            facts(104),
            || facts(105),
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(EffectOutcome::Succeeded(TaskResult {
                    output: json!(true),
                    writes: vec![],
                }))
            },
        )
        .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn changed_approved_proposal_cannot_dispatch() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a"]);
    let service = service(store.clone());
    park(&service, "a", 3);
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    let accepted = service
        .accept(
            &execution_id(),
            fence(&store),
            request(&pending, ApprovalDecision::Once, 4),
        )
        .unwrap();
    let mut changed = accepted.snapshot.clone();
    changed.revision += 1;
    changed.updated_at_ms = 5;
    changed.tasks[0].input.insert("changed".into(), json!(true));
    changed.values.insert("changed".into(), json!(true));
    store
        .commit(
            &execution_id(),
            ExecutionFence::from(&accepted.snapshot),
            CommitPlan {
                expected_revision: accepted.snapshot.revision,
                snapshot: changed,
                dispatchable: vec![],
            },
        )
        .unwrap();
    let result = service.dispatch_outcome(
        &context(),
        &execution_id(),
        fence(&store),
        task("a"),
        AttemptId::new("attempt").unwrap(),
        facts(6),
        || facts(7),
        |_| panic!("changed proposal invoked effect"),
    );
    assert_eq!(result, Err(ApprovalError::InvalidGrant));
    assert_eq!(
        store
            .execution(&execution_id())
            .unwrap()
            .unwrap()
            .snapshot
            .tasks[0]
            .status,
        TaskStatus::Ready
    );
}

#[test]
fn canonical_proposals_preserve_arrays_nulls_and_enforce_bounds_before_park() {
    let store = Arc::new(MemoryStore::new());
    let running = started(&store, &["a"]);
    let task = &running.snapshot.tasks[0];
    let first = json!({"z":{"c":null,"b":[1,2]},"a":true});
    let reordered = json!({"a":true,"z":{"b":[1,2],"c":null}});
    assert_eq!(
        canonical_proposal(task, &first).unwrap(),
        canonical_proposal(task, &reordered).unwrap()
    );
    assert_ne!(
        proposal_digest(task, &first).unwrap(),
        proposal_digest(task, &json!({"a":true,"z":{"b":[2,1],"c":null}})).unwrap()
    );
    assert_ne!(
        proposal_digest(task, &first).unwrap(),
        proposal_digest(task, &json!({"a":true,"z":{"b":[1,2]}})).unwrap()
    );
    let service = service(store.clone());
    for ttl in [0, MAX_APPROVAL_TTL_MS + 1] {
        assert_eq!(
            service.park(
                &context(),
                &execution_id(),
                fence(&store),
                task.spec.id.clone(),
                InterruptId::new("bounded").unwrap(),
                json!("prompt"),
                ttl,
                facts(3)
            ),
            Err(ApprovalError::InvalidTtl)
        );
    }
    assert_eq!(
        service.park(
            &context(),
            &execution_id(),
            fence(&store),
            task.spec.id.clone(),
            InterruptId::new("bounded").unwrap(),
            json!("prompt"),
            1,
            facts(u64::MAX)
        ),
        Err(ApprovalError::InvalidTtl)
    );
    assert_eq!(
        service.park(
            &context(),
            &execution_id(),
            fence(&store),
            task.spec.id.clone(),
            InterruptId::new("bounded").unwrap(),
            json!("x".repeat(MAX_APPROVAL_BYTES)),
            1,
            facts(3)
        ),
        Err(ApprovalError::ProposalTooLarge)
    );
    let mut large_task = task.clone();
    large_task
        .input
        .insert("large".into(), json!("x".repeat(MAX_APPROVAL_BYTES)));
    assert_eq!(
        canonical_proposal(&large_task, &json!(null)),
        Err(ApprovalError::ProposalTooLarge)
    );
    assert_eq!(store.execution(&execution_id()).unwrap().unwrap(), running);
    assert!(store
        .approval_view(&execution_id())
        .unwrap()
        .unwrap()
        .grants
        .is_empty());
    for invalid in ["", "b4ag_x", &format!("b4ag_{}", "A".repeat(64))] {
        assert!(ApprovalToken::new(invalid).is_err());
    }
}

#[test]
fn original_task_park_revision_survives_sibling_completion() {
    let store = Arc::new(MemoryStore::new());
    let running = started_tasks(
        &store,
        vec![
            TaskSpec {
                id: task("a"),
                effect_id: EffectId::new("effect-a").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
            TaskSpec {
                id: task("sibling"),
                effect_id: EffectId::new("effect-sibling").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            },
        ],
    );
    let runtime = Runtime::new(store.clone());
    let dispatched = runtime
        .apply(
            &execution_id(),
            ExecutionFence::from(&running.snapshot),
            KernelCommand::Dispatch {
                task_id: task("sibling"),
                attempt_id: AttemptId::new("sibling-attempt").unwrap(),
            },
            facts(3),
        )
        .unwrap();
    let service = service(store.clone());
    park(&service, "a", 4);
    let grant = store
        .approval_view(&execution_id())
        .unwrap()
        .unwrap()
        .grants
        .remove(0);
    let completed = runtime
        .complete_outcome(
            &execution_id(),
            dispatched.snapshot.owner_generation,
            task("sibling"),
            AttemptId::new("sibling-attempt").unwrap(),
            &EffectOutcome::Succeeded(TaskResult {
                output: json!("sibling"),
                writes: vec![],
            }),
            facts(5),
        )
        .unwrap();
    assert_eq!(
        completed.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert!(completed.snapshot.revision > grant.task_parking_revision);
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    assert_eq!(pending[0].token, grant.token);
    let accepted = service
        .accept(
            &execution_id(),
            fence(&store),
            request(&pending, ApprovalDecision::Once, 6),
        )
        .unwrap();
    assert!(accepted.snapshot.tasks[0].approved);
    assert_eq!(accepted.snapshot.tasks[1].status, TaskStatus::Succeeded);
}

#[test]
fn concurrent_complete_set_acceptance_has_one_winner() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a"]);
    let service = service(store.clone());
    park(&service, "a", 3);
    let pending = service
        .pending(&context(), &execution_id(), fence(&store))
        .unwrap();
    let captured = fence(&store);
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    service.accept(
                        &execution_id(),
                        captured,
                        request(&pending, ApprovalDecision::Once, 4),
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        store
            .approval_view(&execution_id())
            .unwrap()
            .unwrap()
            .acceptances
            .len(),
        1
    );
    assert_eq!(store.journal(&execution_id()).unwrap().len(), 4);
}

struct ReadProbeStore {
    inner: Arc<MemoryStore>,
    reads: AtomicUsize,
}

impl Store for ReadProbeStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
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
        self.inner.execution(id)
    }
    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit(thread_id, initial)
    }
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(execution_id, fence, plan)
    }
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(execution_id)
    }
}

impl ApprovalStore for ReadProbeStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, _: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Err(StoreError::Corrupt("sensitive_decode_failed".into()).into())
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
struct RejectAuthority;
impl ApprovalAuthority for RejectAuthority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Err(StoreError::Unavailable("sensitive_authority_marker".into()).into())
    }
}
#[test]
fn denied_authority_never_reads_sensitive_rows() {
    let memory = Arc::new(MemoryStore::new());
    started(&memory, &["a"]);
    let captured = fence(&memory);
    let probe = ReadProbeStore {
        inner: memory,
        reads: AtomicUsize::new(0),
    };
    let service = ApprovalService::with_config(
        probe,
        RejectAuthority,
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap();
    assert_eq!(
        service.pending(&context(), &execution_id(), captured),
        Err(ApprovalError::Unauthorized)
    );
    assert_eq!(service.store().reads.load(Ordering::SeqCst), 0);
}

fn nested_arrays(depth: usize) -> serde_json::Value {
    (0..depth).fold(json!(null), |value, _| {
        serde_json::Value::Array(vec![value])
    })
}

#[test]
fn approval_depth_limits_reject_before_any_park_mutation() {
    let store = Arc::new(MemoryStore::new());
    let running = started(&store, &["a"]);
    let service = service(store.clone());
    let before = store.approval_view(&execution_id()).unwrap();
    let journal = store.journal(&execution_id()).unwrap();
    for depth in [MAX_APPROVAL_JSON_DEPTH + 1, 130] {
        assert_eq!(
            service.park(
                &context(),
                &execution_id(),
                fence(&store),
                task("a"),
                InterruptId::new("deep").unwrap(),
                nested_arrays(depth),
                100,
                facts(3)
            ),
            Err(ApprovalError::InvalidProposal)
        );
        assert_eq!(store.approval_view(&execution_id()).unwrap(), before);
        assert_eq!(store.journal(&execution_id()).unwrap(), journal);
    }
    let mut deep_input = running.snapshot.tasks[0].clone();
    deep_input
        .input
        .insert("deep".into(), nested_arrays(MAX_APPROVAL_JSON_DEPTH));
    assert_eq!(
        canonical_proposal(&deep_input, &json!(null)),
        Err(ApprovalError::InvalidProposal)
    );
    deep_input
        .input
        .insert("deep".into(), nested_arrays(MAX_APPROVAL_JSON_DEPTH - 1));
    assert!(canonical_proposal(&deep_input, &nested_arrays(MAX_APPROVAL_JSON_DEPTH)).is_ok());
    let saved = service
        .park(
            &context(),
            &execution_id(),
            fence(&store),
            task("a"),
            InterruptId::new("deep").unwrap(),
            nested_arrays(MAX_APPROVAL_JSON_DEPTH),
            100,
            facts(3),
        )
        .unwrap();
    let encoded = serde_json::to_vec(&saved.snapshot).unwrap();
    assert_eq!(
        serde_json::from_slice::<Snapshot>(&encoded).unwrap(),
        saved.snapshot
    );
}

#[test]
fn persisted_grant_validation_rejects_deep_prompt() {
    let store = Arc::new(MemoryStore::new());
    started(&store, &["a"]);
    let service = service(store.clone());
    park(&service, "a", 3);
    let mut grant = store
        .approval_view(&execution_id())
        .unwrap()
        .unwrap()
        .grants
        .remove(0);
    grant.prompt = nested_arrays(MAX_APPROVAL_JSON_DEPTH + 1);
    assert_eq!(
        validate_grant_record(&grant),
        Err(ApprovalError::InvalidProposal)
    );
}
