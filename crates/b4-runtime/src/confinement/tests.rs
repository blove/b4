use super::*;
use crate::{MemoryStore, Store, ThreadOrigin, ThreadSeed, ThreadView};
use b4_contract::*;
use serde_json::{Map, Value};

fn seeded(values: Map<String, Value>, metadata: Map<String, Value>) -> (MemoryStore, ThreadView) {
    let store = MemoryStore::new();
    let thread_id = ThreadId::new("private-memory-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata,
            origin: Some(ThreadOrigin {
                application: "private-app".into(),
                tenant: "private-tenant".into(),
                principal: "private-principal".into(),
            }),
        })
        .unwrap();
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new("private-memory-execution").unwrap(),
        TurnId::new("private-original-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/private-confinement#workflow").unwrap(),
            revision: ExecutionRevision::new("private-definition").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("private-task").unwrap(),
                effect_id: EffectId::new("private-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }]],
            max_attempts: 3,
        },
        values,
        TransitionFacts { now_ms: 100 },
    )
    .unwrap();
    store.admit(&thread_id, snapshot).unwrap();
    let original = store.thread_view(&thread_id).unwrap().unwrap();
    (store, original)
}

fn unchanged(store: &MemoryStore, original: &ThreadView) {
    assert!(store.thread_view(&original.thread.id).unwrap().as_ref() == Some(original));
    assert_eq!(store.owner_generation(), 1);
    assert_eq!(
        store
            .journal(&original.execution.as_ref().unwrap().snapshot.execution_id)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn memory_current_owner_capture_is_static_refusal_and_preserves_the_full_view() {
    let (store, original) = seeded(Map::new(), Map::new());
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let observed = store.confinement_view_fenced(&target);
    unchanged(&store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::OwnerConflict
    );
}

#[test]
fn memory_current_owner_mutation_is_static_refusal_and_preserves_the_full_view() {
    let (store, original) = seeded(Map::new(), Map::new());
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let observed = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &TurnId::new("private-fresh-turn").unwrap(),
        TransitionFacts { now_ms: 250 },
    );
    unchanged(&store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::OwnerConflict
    );
}

#[test]
fn memory_absent_target_is_static_not_found_without_creating_a_resource() {
    let (store, original) = seeded(Map::new(), Map::new());
    let mut target = RecoveryConfinementTarget::from_view(&original).unwrap();
    target.thread_id = ThreadId::new("private-absent-thread").unwrap();
    let observed = store.confinement_view_fenced(&target);
    unchanged(&store, &original);
    assert!(store.thread(&target.thread_id).unwrap().is_none());
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::NotFound
    );
}

#[test]
fn memory_valid_snapshot_over_one_mib_refuses_before_owner_classification() {
    let values = Map::from_iter([("padding".into(), Value::String("x".repeat(1_048_576)))]);
    let (store, original) = seeded(values, Map::new());
    let snapshot = &original.execution.as_ref().unwrap().snapshot;
    b4_kernel::validate_snapshot(snapshot).unwrap();
    assert!(serde_json::to_vec(snapshot).unwrap().len() > 1_048_576);
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let observed = store.confinement_view_fenced(&target);
    unchanged(&store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::LimitExceeded
    );
}

#[test]
fn memory_valid_thread_components_over_256_kib_refuse_without_mutation() {
    let metadata = Map::from_iter([("padding".into(), Value::String("x".repeat(262_144)))]);
    let (store, original) = seeded(Map::new(), metadata);
    assert!(serde_json::to_vec(&original.thread.metadata).unwrap().len() > 262_144);
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let observed = store.confinement_view_fenced(&target);
    unchanged(&store, &original);
    assert_eq!(
        observed.unwrap_err(),
        RecoveryConfinementBackendError::LimitExceeded
    );
}
