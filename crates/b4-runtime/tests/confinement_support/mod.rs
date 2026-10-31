use b4_contract::*;
use b4_runtime::{approval::*, *};
use serde_json::{Map, Value};
use std::sync::Arc;

pub fn thread_id() -> ThreadId {
    ThreadId::new("public-memory-confinement-thread").unwrap()
}
pub fn execution_id() -> ExecutionId {
    ExecutionId::new("public-memory-confinement-execution").unwrap()
}
pub fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "memory-confinement-app".into(),
            tenant: "memory-confinement-tenant".into(),
            principal: "memory-confinement-actor".into(),
        },
        policy_revision: "original-memory-policy".into(),
    }
}
pub fn seed(values: Map<String, Value>, approval: bool) -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::new());
    assert_eq!(store.owner_generation(), 1);
    store
        .create_thread_with(ThreadSeed {
            id: thread_id(),
            created_at_ms: 100,
            metadata: Map::from_iter([("original_metadata".into(), Value::Bool(true))]),
            origin: Some(context().actor),
        })
        .unwrap();
    let snapshot = b4_kernel::new_execution(
        execution_id(),
        TurnId::new("original-memory-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/memory-confinement#workflow").unwrap(),
            revision: ExecutionRevision::new("original-memory-v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("memory-task").unwrap(),
                effect_id: EffectId::new("memory-effect").unwrap(),
                requires_approval: approval,
                recovery: RecoveryPolicy::Unsafe,
            }]],
            max_attempts: 3,
        },
        values,
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    store.admit(&thread_id(), snapshot).unwrap();
    store
}
pub fn view(store: &MemoryStore) -> ThreadView {
    store.thread_view(&thread_id()).unwrap().unwrap()
}
pub fn authority(store: &MemoryStore) -> ApprovalView {
    store.approval_view(&execution_id()).unwrap().unwrap()
}
pub fn commit(store: &MemoryStore, command: KernelCommand) {
    let original = view(store).execution.unwrap().snapshot;
    let plan = b4_kernel::transition(
        &original,
        command,
        TransitionFacts {
            now_ms: original.updated_at_ms + 1,
        },
    )
    .unwrap();
    store
        .commit(&execution_id(), (&original).into(), plan)
        .unwrap();
}
pub struct Allow;
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
pub fn assert_refused(store: Arc<MemoryStore>, refusal: RecoveryConfinementBackendError) {
    let original = view(&store);
    let original_authority = authority(&store);
    let original_journal = store.journal(&execution_id()).unwrap();
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let capture = store.confinement_view_fenced(&target);
    let mutation = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
        &TurnId::new("public-memory-fresh-turn").unwrap(),
        TransitionFacts { now_ms: 5_000 },
    );
    assert_eq!(store.owner_generation(), 1);
    assert_eq!(view(&store), original);
    assert_eq!(authority(&store), original_authority);
    assert_eq!(store.journal(&execution_id()).unwrap(), original_journal);
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    assert_eq!(capture.unwrap_err(), refusal);
    assert_eq!(mutation.unwrap_err(), refusal);
}
pub fn nodes(snapshot: &Snapshot) -> usize {
    let document = serde_json::to_value(snapshot).unwrap();
    let mut work = vec![&document];
    let mut count = 0;
    while let Some(value) = work.pop() {
        count += 1;
        match value {
            Value::Object(object) => {
                count += object.len();
                work.extend(object.values());
            }
            Value::Array(array) => work.extend(array),
            _ => {}
        }
    }
    count
}

/// A custom old Store deliberately implements no new confinement port.
pub struct OldStore(pub Arc<MemoryStore>);
impl Store for OldStore {
    fn owner_generation(&self) -> u64 {
        self.0.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.0.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.0.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.0.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.0.execution(id)
    }
    fn admit(&self, id: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.0.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.0.admit_fenced(id, generation, revision, initial)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.0.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.0.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.0.journal(id)
    }
}
