use b4_contract::*;
use b4_runtime::{approval::ApprovalStore, *};
use serde_json::Map;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

// Metadata must not call any persistence or health operation on a custom store.
macro_rules! forbid_store_operations {
    () => {
        fn owner_generation(&self) -> u64 {
            panic!("unexpected_store_probe")
        }
        fn create_thread_with(&self, _: ThreadSeed) -> Result<ThreadRecord, StoreError> {
            panic!("unexpected_store_probe")
        }
        fn thread(&self, _: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
            panic!("unexpected_store_probe")
        }
        fn thread_view(&self, _: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
            panic!("unexpected_store_probe")
        }
        fn execution(&self, _: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
            panic!("unexpected_store_probe")
        }
        fn admit(&self, _: &ThreadId, _: Snapshot) -> Result<StoredExecution, StoreError> {
            panic!("unexpected_store_probe")
        }
        fn admit_fenced(
            &self,
            _: &ThreadId,
            _: &ThreadGeneration,
            _: u64,
            _: Snapshot,
        ) -> Result<StoredExecution, StoreError> {
            panic!("unexpected_store_probe")
        }
        fn readiness(&self) -> Result<(), StoreError> {
            panic!("unexpected_store_probe")
        }
        fn commit(
            &self,
            _: &ExecutionId,
            _: ExecutionFence,
            _: CommitPlan,
        ) -> Result<StoredExecution, StoreError> {
            panic!("unexpected_store_probe")
        }
        fn journal(&self, _: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
            panic!("unexpected_store_probe")
        }
    };
}

struct DefaultOnlyStore;
impl Store for DefaultOnlyStore {
    forbid_store_operations!();
}

struct DeclaredStore(AtomicUsize);
const CUSTOM_DECLARATION: StoreCapabilities = StoreCapabilities {
    kind: StoreKind::Unspecified,
    durable_runtime: true,
    fenced_cancellation: false,
    fenced_deletion: true,
};
impl Store for DeclaredStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.0.fetch_add(1, Ordering::Relaxed);
        CUSTOM_DECLARATION
    }
    forbid_store_operations!();
}

#[test]
fn custom_store_defaults_are_conservative_without_probing() {
    let expected = StoreCapabilities {
        kind: StoreKind::Unspecified,
        durable_runtime: false,
        fenced_cancellation: false,
        fenced_deletion: false,
    };
    assert_eq!(StoreKind::default(), StoreKind::Unspecified);
    assert_eq!(StoreCapabilities::default(), expected);
    assert_eq!(DefaultOnlyStore.capabilities(), expected);
}

#[test]
fn arc_and_reference_wrappers_forward_exact_declarations() {
    let concrete = Arc::new(DeclaredStore(AtomicUsize::new(0)));
    assert_eq!(Store::capabilities(concrete.as_ref()), CUSTOM_DECLARATION);
    assert_eq!(Store::capabilities(&concrete), CUSTOM_DECLARATION);
    let reference = concrete.as_ref();
    assert_eq!(Store::capabilities(&reference), CUSTOM_DECLARATION);
    let dynamic: Arc<dyn Store> = concrete.clone();
    assert_eq!(Store::capabilities(&dynamic), CUSTOM_DECLARATION);
    let dynamic_reference: &dyn Store = concrete.as_ref();
    assert_eq!(Store::capabilities(&dynamic_reference), CUSTOM_DECLARATION);
    let nested = Arc::new(reference);
    assert_eq!(Store::capabilities(&nested), CUSTOM_DECLARATION);
    assert_eq!(concrete.0.load(Ordering::Relaxed), 6);
}

fn accepted(store: &MemoryStore) -> StoredExecution {
    let thread = ThreadId::new("capability-thread").unwrap();
    store.create_thread(thread.clone()).unwrap();
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new("capability-execution").unwrap(),
        TurnId::new("capability-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/capabilities#workflow").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("pure-task").unwrap(),
                effect_id: EffectId::new("pure-effect").unwrap(),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            }]],
            max_attempts: 1,
        },
        Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    store.admit(&thread, snapshot).unwrap()
}

#[test]
fn unadvertised_optional_operations_keep_their_unsupported_defaults() {
    let memory = MemoryStore::new();
    let current = accepted(&memory);
    let view = memory.thread_view(&current.thread_id).unwrap().unwrap();
    let cancel = RunCancellationTarget::from_view(&view).unwrap();
    let delete = ThreadDeletionTarget::from_view(&view).unwrap();
    assert_eq!(
        DefaultOnlyStore.cancel_run_fenced(&cancel, TransitionFacts { now_ms: 2 }),
        Err(StoreError::UnsupportedOperation)
    );
    assert_eq!(
        DefaultOnlyStore.delete_thread_fenced(&delete),
        Err(StoreError::UnsupportedOperation)
    );
}

#[test]
fn memory_declaration_preserves_records_and_matches_actual_ports() {
    let store = MemoryStore::new();
    let current = accepted(&store);
    let before = store.thread_view(&current.thread_id).unwrap().unwrap();
    let journal = store.journal(&current.snapshot.execution_id).unwrap();
    let owner = store.owner_generation();
    let expected = StoreCapabilities {
        kind: StoreKind::Memory,
        durable_runtime: false,
        fenced_cancellation: true,
        fenced_deletion: true,
    };
    for _ in 0..3 {
        assert_eq!(store.capabilities(), expected);
    }
    assert!(!store.approval_durable());
    assert_eq!(store.owner_generation(), owner);
    assert_eq!(
        store.thread_view(&current.thread_id).unwrap(),
        Some(before.clone())
    );
    assert_eq!(
        store.journal(&current.snapshot.execution_id).unwrap(),
        journal
    );

    let target = RunCancellationTarget::from_view(&before).unwrap();
    let cancelled = store
        .cancel_run_fenced(&target, TransitionFacts { now_ms: 2 })
        .unwrap();
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert!(cancelled.snapshot.attempt_history.is_empty());
    assert!(cancelled.snapshot.output.is_none());
    let view = store.thread_view(&current.thread_id).unwrap().unwrap();
    store
        .delete_thread_fenced(&ThreadDeletionTarget::from_view(&view).unwrap())
        .unwrap();
    assert!(store.thread(&current.thread_id).unwrap().is_none());
    assert!(store
        .execution(&current.snapshot.execution_id)
        .unwrap()
        .is_none());
    assert!(store
        .journal(&current.snapshot.execution_id)
        .unwrap()
        .is_empty());
    assert_eq!(store.capabilities(), expected);
}
