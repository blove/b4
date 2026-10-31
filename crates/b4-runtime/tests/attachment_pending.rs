#[path = "attachment_pending/support.rs"]
mod support;
use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use support::*;

struct ReadStore {
    inner: Arc<MemoryStore>,
    owner: u64,
    reads: AtomicUsize,
    mutate: fn(&mut ApprovalView),
    snapshot_owner: Option<u64>,
}
impl Store for ReadStore {
    fn owner_generation(&self) -> u64 {
        self.owner
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        let mut view = self.inner.thread_view(id)?;
        if let (Some(view), Some(owner)) = (&mut view, self.snapshot_owner) {
            if let Some(execution) = &mut view.execution {
                execution.snapshot.owner_generation = owner;
            }
        }
        Ok(view)
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
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}
impl ApprovalStore for ReadStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut result = self.inner.approval_view(id)?;
        if let Some(view) = &mut result {
            (self.mutate)(view);
        }
        Ok(result)
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
struct Authority {
    allow: bool,
    calls: AtomicUsize,
}
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.allow {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
fn service(
    inner: Arc<MemoryStore>,
    owner: u64,
    allow: bool,
    mutate: fn(&mut ApprovalView),
) -> ApprovalService<ReadStore, Arc<Authority>> {
    ApprovalService::with_config(
        ReadStore {
            inner,
            owner,
            reads: AtomicUsize::new(0),
            mutate,
            snapshot_owner: None,
        },
        Arc::new(Authority {
            allow,
            calls: AtomicUsize::new(0),
        }),
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap()
}

#[test]
fn older_owner_stable_park_disclosure_preserves_expired_tokens_and_all_history() {
    let store = Arc::new(MemoryStore::new());
    let (captured, pending, before) = fixture(store.clone());
    let journal = store.journal(&execution()).unwrap();
    let reader = service(store.clone(), 2, true, |_| {});
    let disclosed = reader.attachment_pending(&context(), &captured).unwrap();
    assert!(disclosed == pending);
    assert_eq!(disclosed[0].expires_at_ms, 8);
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
    assert_eq!(store.journal(&execution()).unwrap(), journal);
    assert_eq!(
        reader.pending(&context(), &execution(), fence(&captured)),
        Err(ApprovalError::Store(StoreError::OwnerConflict))
    );
    assert_eq!(
        reader.accept(
            &execution(),
            fence(&captured),
            acceptance(&captured, &pending, 100, "later-turn")
        ),
        Err(ApprovalError::Store(StoreError::OwnerConflict))
    );
}

#[test]
fn same_owner_disclosure_does_not_refresh_expiry_or_make_expired_acceptance_valid() {
    let store = Arc::new(MemoryStore::new());
    let (captured, pending, before) = fixture(store.clone());
    let reader = service(store.clone(), 1, true, |_| {});
    assert!(reader.attachment_pending(&context(), &captured).unwrap() == pending);
    assert_eq!(
        reader.accept(
            &execution(),
            fence(&captured),
            acceptance(&captured, &pending, 100, "later-turn")
        ),
        Err(ApprovalError::Expired)
    );
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}

#[test]
fn complete_captured_identity_is_compared_before_sensitive_rows() {
    let store = Arc::new(MemoryStore::new());
    let (captured, _, _) = fixture(store.clone());
    let reader = service(store, 1, true, |_| {});
    let mut variants = vec![];
    let mut v = captured.clone();
    v.thread.generation = ThreadGeneration::new("f".repeat(64)).unwrap();
    variants.push(v);
    let mut v = captured.clone();
    v.thread.resource_revision += 1;
    variants.push(v);
    let mut v = captured.clone();
    v.thread.active_execution_id = Some(ExecutionId::new("different").unwrap());
    variants.push(v);
    let mut v = captured.clone();
    v.thread.origin.as_mut().unwrap().principal = "bob".into();
    variants.push(v);
    let mut v = captured.clone();
    v.thread
        .metadata
        .insert("changed".into(), serde_json::json!(true));
    variants.push(v);
    let mut v = captured.clone();
    v.execution.as_mut().unwrap().thread_id = ThreadId::new("different").unwrap();
    variants.push(v);
    let mut v = captured.clone();
    v.execution.as_mut().unwrap().snapshot.execution_id = ExecutionId::new("different").unwrap();
    variants.push(v);
    let mut v = captured.clone();
    v.execution.as_mut().unwrap().snapshot.revision += 1;
    variants.push(v);
    let mut v = captured.clone();
    v.execution.as_mut().unwrap().snapshot.owner_generation += 1;
    variants.push(v);
    let mut v = captured.clone();
    v.execution.as_mut().unwrap().snapshot.turn_id = TurnId::new("different").unwrap();
    variants.push(v);
    let mut v = captured.clone();
    v.execution.as_mut().unwrap().snapshot.definition.revision =
        ExecutionRevision::new("different").unwrap();
    variants.push(v);
    let mut v = captured.clone();
    v.execution
        .as_mut()
        .unwrap()
        .snapshot
        .values
        .insert("changed".into(), serde_json::json!(true));
    variants.push(v);
    for variant in variants {
        assert_eq!(
            reader.attachment_pending(&context(), &variant),
            Err(ApprovalError::Store(StoreError::RevisionConflict))
        );
    }
    assert_eq!(reader.store().reads.load(Ordering::SeqCst), 0);
}

#[test]
fn denial_and_wrong_actor_precede_corrupt_sensitive_disclosure() {
    let store = Arc::new(MemoryStore::new());
    let (captured, _, before) = fixture(store.clone());
    let reader = service(store.clone(), 2, false, |view| {
        view.grants[0].verifier = [0; 32]
    });
    assert_eq!(
        reader.attachment_pending(&context(), &captured),
        Err(ApprovalError::Unauthorized)
    );
    assert_eq!(reader.store().reads.load(Ordering::SeqCst), 0);
    let reader = service(store.clone(), 2, true, |view| {
        view.grants[0].verifier = [0; 32]
    });
    let mut wrong = context();
    wrong.actor.principal = "bob".into();
    assert_eq!(
        reader.attachment_pending(&wrong, &captured),
        Err(ApprovalError::Unauthorized)
    );
    assert_eq!(reader.store().reads.load(Ordering::SeqCst), 0);
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}

#[test]
fn every_stored_grant_and_audit_is_validated_as_infrastructure_corruption() {
    let store = Arc::new(MemoryStore::new());
    let (captured, _, before) = fixture(store.clone());
    let corruptions: Vec<fn(&mut ApprovalView)> = vec![
        |v| v.grants[0].verifier = [0; 32],
        |v| v.grants[0].origin.principal = "bob".into(),
        |v| v.grants[0].proposal_digest = "f".repeat(64),
        |v| v.grants[0].effect_id = EffectId::new("different").unwrap(),
        |v| v.grants[0].prompt = serde_json::json!("different"),
        |v| v.grants[0].task_parking_revision += 1,
        |v| v.grants[0].consumed = false,
        |v| v.grants.push(v.grants[1].clone()),
        |v| v.grants[1].verifier = [0; 32],
        |v| v.grants[1].consumed = true,
        |v| {
            v.grants.remove(1);
        },
        |v| v.acceptances.clear(),
        |v| v.acceptances[0].context.actor.principal = "bob".into(),
        |v| v.acceptances[0].execution_id = ExecutionId::new("different").unwrap(),
        |v| v.acceptances[0].thread_generation = ThreadGeneration::new("f".repeat(64)).unwrap(),
        |v| v.acceptances[0].definition_revision = ExecutionRevision::new("different").unwrap(),
        |v| v.acceptances[0].previous_revision = v.execution.snapshot.revision,
        |v| v.acceptances[0].turn_id = TurnId::new("different").unwrap(),
        |v| v.acceptances[0].decisions[0].proposal_digest = "f".repeat(64),
        |v| v.acceptances[0].decisions[0].task_parking_revision += 1,
        |v| v.acceptances.push(v.acceptances[0].clone()),
    ];
    for mutate in corruptions {
        let reader = service(store.clone(), 1, true, mutate);
        assert!(matches!(
            reader.attachment_pending(&context(), &captured),
            Err(ApprovalError::Store(StoreError::Corrupt(_)))
        ));
    }
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}

#[test]
fn historical_acceptance_at_grant_expiry_is_stored_corruption() {
    let store = Arc::new(MemoryStore::new());
    let (captured, _, before) = fixture(store.clone());
    let reader = service(store.clone(), 1, true, |view| {
        view.grants[0].expires_at_ms = view.acceptances[0].accepted_at_ms;
    });
    assert!(matches!(
        reader.attachment_pending(&context(), &captured),
        Err(ApprovalError::Store(StoreError::Corrupt(_)))
    ));
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}

#[test]
fn missing_consumed_grant_and_audit_cannot_erase_an_approved_tasks_authority_history() {
    for decision in [ApprovalDecision::Once, ApprovalDecision::Deny] {
        let store = Arc::new(MemoryStore::new());
        let (captured, pending, before) = fixture_with_history(store.clone(), "policy-1", decision);
        let valid_reader = service(store.clone(), 2, true, |_| {});
        assert!(
            valid_reader
                .attachment_pending(&context(), &captured)
                .unwrap()
                == pending
        );
        let reader = service(store.clone(), 1, true, |view| {
            view.grants.retain(|grant| !grant.consumed);
            view.acceptances.clear();
        });
        assert!(matches!(
            reader.attachment_pending(&context(), &captured),
            Err(ApprovalError::Store(StoreError::Corrupt(_)))
        ));
        assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
    }
}

#[test]
fn identity_change_during_authority_is_rejected_before_sensitive_rows() {
    struct Recreate(Arc<MemoryStore>);
    impl ApprovalAuthority for Recreate {
        fn authorize(
            &self,
            _: &ApprovalContext,
            operation: ApprovalOperation,
            _: &ApprovalResource,
        ) -> Result<(), ApprovalError> {
            assert_eq!(operation, ApprovalOperation::AttachmentPending);
            let current = self.0.thread_view(&thread()).unwrap().unwrap();
            self.0
                .delete_thread_fenced(&ThreadDeletionTarget::from_view(&current).unwrap())
                .unwrap();
            fixture(self.0.clone());
            Ok(())
        }
    }
    let store = Arc::new(MemoryStore::new());
    let (captured, _, _) = fixture(store.clone());
    let reader = ApprovalService::with_config(
        ReadStore {
            inner: store.clone(),
            owner: 1,
            reads: AtomicUsize::new(0),
            mutate: |_| {},
            snapshot_owner: None,
        },
        Recreate(store.clone()),
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap();
    assert_eq!(
        reader.attachment_pending(&context(), &captured),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    );
    assert_eq!(reader.store().reads.load(Ordering::SeqCst), 0);
    let successor = store.thread_view(&thread()).unwrap().unwrap();
    assert_ne!(successor.thread.generation, captured.thread.generation);
    assert_eq!(
        successor.execution.as_ref().unwrap().snapshot,
        captured.execution.as_ref().unwrap().snapshot
    );
}

#[test]
fn genuine_pending_policy_mismatch_is_a_healthy_refusal() {
    let store = Arc::new(MemoryStore::new());
    let (captured, _, before) = fixture(store.clone());
    let reader = service(store.clone(), 1, true, |_| {});
    let mut changed = context();
    changed.policy_revision = "policy-2".into();
    assert_eq!(
        reader.attachment_pending(&changed, &captured),
        Err(ApprovalError::InvalidGrant)
    );
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}

#[test]
fn missing_later_pending_authority_precedes_an_earlier_policy_mismatch() {
    let store = Arc::new(MemoryStore::new());
    let captured = parallel_fixture(store.clone());
    let before = store.approval_view(&execution()).unwrap().unwrap();
    let reader = service(store.clone(), 1, true, |view| {
        view.grants
            .retain(|grant| grant.task_id.as_str() != "second");
    });
    let mut current_policy = context();
    current_policy.policy_revision = "policy-2".into();
    assert!(matches!(
        reader.attachment_pending(&current_policy, &captured),
        Err(ApprovalError::Store(StoreError::Corrupt(_)))
    ));
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}

#[test]
fn consumed_history_retains_its_original_policy_while_current_pending_uses_fresh_policy() {
    let store = Arc::new(MemoryStore::new());
    let (captured, pending, before) = fixture_with_pending_policy(store.clone(), "policy-2");
    assert_eq!(before.grants[0].policy_revision, "policy-1");
    assert_eq!(before.acceptances[0].context.policy_revision, "policy-1");
    let mut current = context();
    current.policy_revision = "policy-2".into();
    let reader = service(store.clone(), 2, true, |_| {});
    assert!(reader.attachment_pending(&current, &captured).unwrap() == pending);
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}

#[test]
fn sensitive_view_change_after_authorization_is_fenced() {
    let store = Arc::new(MemoryStore::new());
    let (captured, _, _) = fixture(store.clone());
    for mutate in [
        (|v: &mut ApprovalView| v.thread.resource_revision += 1) as fn(&mut ApprovalView),
        |v: &mut ApprovalView| v.execution.snapshot.turn_id = TurnId::new("changed").unwrap(),
    ] {
        let reader = service(store.clone(), 1, true, mutate);
        assert_eq!(
            reader.attachment_pending(&context(), &captured),
            Err(ApprovalError::Store(StoreError::RevisionConflict))
        );
    }
}

#[test]
fn dedicated_read_operation_and_owner_bounds_precede_sensitive_read() {
    struct AttachmentOnly;
    impl ApprovalAuthority for AttachmentOnly {
        fn authorize(
            &self,
            _: &ApprovalContext,
            operation: ApprovalOperation,
            _: &ApprovalResource,
        ) -> Result<(), ApprovalError> {
            assert_eq!(operation, ApprovalOperation::AttachmentPending);
            Ok(())
        }
    }
    let store = Arc::new(MemoryStore::new());
    let (_, _, _) = fixture(store.clone());
    for (owner, snapshot_owner, expected) in [
        (2, None, None),
        (0, None, Some(StoreError::OwnerConflict)),
        (1, Some(2), Some(StoreError::OwnerConflict)),
        (
            1,
            Some(0),
            Some(StoreError::Corrupt(
                "attachment_approval_snapshot_invalid".into(),
            )),
        ),
    ] {
        let reader = ApprovalService::with_config(
            ReadStore {
                inner: store.clone(),
                owner,
                reads: AtomicUsize::new(0),
                mutate: |_| {},
                snapshot_owner,
            },
            AttachmentOnly,
            ApprovalServiceConfig::VolatileFixture,
        )
        .unwrap();
        let captured = reader.store().thread_view(&thread()).unwrap().unwrap();
        if let Some(error) = expected {
            assert_eq!(
                reader.attachment_pending(&context(), &captured),
                Err(ApprovalError::Store(error))
            );
            assert_eq!(reader.store().reads.load(Ordering::SeqCst), 0);
        } else {
            assert!(reader.attachment_pending(&context(), &captured).is_ok());
            assert_eq!(reader.store().reads.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn older_owner_unfinished_work_is_not_disclosed_as_a_stable_park() {
    let store = Arc::new(MemoryStore::new());
    let (captured, pending, _) = fixture(store.clone());
    let ordinary =
        ApprovalService::with_config(store.clone(), Allow, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    ordinary
        .accept(
            &execution(),
            fence(&captured),
            acceptance(&captured, &pending, 7, "second-approved-turn"),
        )
        .unwrap();
    let running = store.thread_view(&thread()).unwrap().unwrap();
    let reader = service(store.clone(), 2, true, |_| {});
    assert_eq!(
        reader.attachment_pending(&context(), &running),
        Err(ApprovalError::NotStablePark)
    );
    assert_eq!(reader.store().reads.load(Ordering::SeqCst), 0);
    let before = store.approval_view(&execution()).unwrap().unwrap();
    assert_eq!(
        reader.attachment_pending(&context(), &captured),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    );
    assert!(store.approval_view(&execution()).unwrap().unwrap() == before);
}
