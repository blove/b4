use b4_contract::*;
use b4_runtime::*;
use std::sync::Arc;

struct DefaultOnly;
impl Store for DefaultOnly {
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
}

pub fn defaults() {
    let concrete = Arc::new(DefaultOnly);
    let dynamic: Arc<dyn Store> = concrete.clone();
    let reference: &dyn Store = concrete.as_ref();
    for result in [
        concrete.discover_unfinished(None),
        dynamic.discover_unfinished(None),
        Store::discover_unfinished(&reference, None),
    ] {
        assert_eq!(result.unwrap_err(), DiscoveryError::UnsupportedOperation);
    }
}
