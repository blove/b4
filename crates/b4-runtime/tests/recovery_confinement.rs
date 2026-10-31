mod confinement_support;
use b4_contract::*;
use b4_runtime::{approval::*, *};
use confinement_support::*;
use serde_json::{json, Map, Value};
use std::sync::Arc;

#[test]
fn memory_actual_volatile_grant_profile_remains_current_owner_and_exactly_unchanged() {
    let store = seed(
        Map::from_iter([("original".into(), json!({"keep": true}))]),
        true,
    );
    commit(&store, KernelCommand::Start);
    let original = view(&store);
    let service =
        ApprovalService::with_config(store.clone(), Allow, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    service
        .park(
            &context(),
            &execution_id(),
            ApprovalFence {
                execution: (&original.execution.as_ref().unwrap().snapshot).into(),
                resource_revision: original.thread.resource_revision,
            },
            TaskId::new("memory-task").unwrap(),
            InterruptId::new("actual-memory-interrupt").unwrap(),
            json!({"original_prompt": true}),
            1_000,
            TransitionFacts { now_ms: 102 },
        )
        .unwrap();
    drop(service);
    let records = authority(&store);
    assert_eq!(
        records.execution.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(records.grants.len(), 1);
    assert!(records.acceptances.is_empty());
    assert!(!records.grants[0].consumed);
    assert!(records.grants[0].expires_at_ms < 5_000);
    assert_refused(store, RecoveryConfinementBackendError::OwnerConflict);
}

#[test]
fn memory_legacy_direct_approval_refuses_the_unsupported_profile_before_owner_classification() {
    for decision in [
        None,
        Some(ApprovalDecision::Once),
        Some(ApprovalDecision::Always),
    ] {
        let store = seed(Map::new(), true);
        commit(&store, KernelCommand::Start);
        commit(
            &store,
            KernelCommand::Park {
                task_id: TaskId::new("memory-task").unwrap(),
                interrupt_id: InterruptId::new("legacy-memory-interrupt").unwrap(),
                prompt: json!({"legacy": true}),
            },
        );
        if let Some(decision) = decision {
            commit(
                &store,
                KernelCommand::Resume {
                    route: view(&store)
                        .execution
                        .as_ref()
                        .unwrap()
                        .snapshot
                        .definition
                        .route
                        .clone(),
                    decisions: vec![Decision {
                        interrupt_id: InterruptId::new("legacy-memory-interrupt").unwrap(),
                        decision,
                    }],
                    turn_id: TurnId::new("legacy-memory-resume").unwrap(),
                },
            );
        }
        let records = authority(&store);
        assert!(records.grants.is_empty());
        assert!(records.acceptances.is_empty());
        b4_kernel::validate_snapshot(&records.execution.snapshot).unwrap();
        assert_refused(
            store,
            RecoveryConfinementBackendError::UnsupportedAuthorityProfile,
        );
    }
}

#[test]
fn memory_depth_and_node_limits_precede_current_owner_without_rewriting_valid_values() {
    for depth in [62, 63] {
        let nested = (0..depth).fold(Value::Null, |value, _| Value::Array(vec![value]));
        let store = seed(Map::from_iter([("nested".into(), nested)]), false);
        b4_kernel::validate_snapshot(&view(&store).execution.as_ref().unwrap().snapshot).unwrap();
        let refusal = if depth == 62 {
            RecoveryConfinementBackendError::OwnerConflict
        } else {
            RecoveryConfinementBackendError::LimitExceeded
        };
        assert_refused(store, refusal);
    }
    let measuring = seed(
        Map::from_iter([("wide".into(), Value::Array(vec![]))]),
        false,
    );
    let base = nodes(&view(&measuring).execution.as_ref().unwrap().snapshot);
    let weak = Arc::downgrade(&measuring);
    drop(measuring);
    assert!(weak.upgrade().is_none());
    for extra in [0, 1] {
        let store = seed(
            Map::from_iter([(
                "wide".into(),
                Value::Array(vec![Value::Null; 16_384 - base + extra]),
            )]),
            false,
        );
        let original = view(&store);
        assert_eq!(
            nodes(&original.execution.as_ref().unwrap().snapshot),
            16_384 + extra
        );
        b4_kernel::validate_snapshot(&original.execution.as_ref().unwrap().snapshot).unwrap();
        assert_refused(
            store,
            if extra == 0 {
                RecoveryConfinementBackendError::OwnerConflict
            } else {
                RecoveryConfinementBackendError::LimitExceeded
            },
        );
    }
}

#[test]
fn memory_target_guards_and_arc_reference_forwarding_preserve_custom_default_refusal() {
    let store = seed(Map::new(), false);
    let original = view(&store);
    let before_authority = authority(&store);
    let journal = store.journal(&execution_id()).unwrap();
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let turn = TurnId::new("public-memory-forwarding-turn").unwrap();
    let capture_arc = <Arc<MemoryStore> as Store>::confinement_view_fenced(&store, &target);
    let capture_reference =
        <&MemoryStore as Store>::confinement_view_fenced(&store.as_ref(), &target);
    let mutation_arc = <Arc<MemoryStore> as Store>::confine_recovery_fenced(
        &store,
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &turn,
        TransitionFacts { now_ms: 5_000 },
    );
    let mutation_reference = <&MemoryStore as Store>::confine_recovery_fenced(
        &store.as_ref(),
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &turn,
        TransitionFacts { now_ms: 5_000 },
    );
    let mut stale_targets = Vec::new();
    let mut stale = target.clone();
    stale.resource_revision += 1;
    stale_targets.push(stale);
    let mut stale = target.clone();
    stale.execution_fence.revision += 1;
    stale_targets.push(stale);
    let mut stale = target.clone();
    stale.turn_id = TurnId::new("different-memory-turn").unwrap();
    stale_targets.push(stale);
    let mut stale = target.clone();
    stale.thread_generation = ThreadGeneration::new("f".repeat(64)).unwrap();
    assert_ne!(stale.thread_generation, target.thread_generation);
    stale_targets.push(stale);
    let stale_results: Vec<_> = stale_targets
        .iter()
        .map(|stale| {
            (
                store.confinement_view_fenced(stale),
                store.confine_recovery_fenced(
                    &original,
                    stale,
                    RecoveryConfinementReason::ExactDefinitionUnavailable,
                    &turn,
                    TransitionFacts { now_ms: 5_000 },
                ),
            )
        })
        .collect();
    let old = Arc::new(OldStore(store.clone()));
    let custom_capture = <Arc<OldStore> as Store>::confinement_view_fenced(&old, &target);
    let custom_mutation = <&OldStore as Store>::confine_recovery_fenced(
        &old.as_ref(),
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &turn,
        TransitionFacts { now_ms: 5_000 },
    );
    drop(old);
    assert_eq!(view(&store), original);
    assert_eq!(authority(&store), before_authority);
    assert_eq!(store.journal(&execution_id()).unwrap(), journal);
    assert_eq!(store.owner_generation(), 1);
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    for result in [capture_arc, capture_reference] {
        assert_eq!(
            result.unwrap_err(),
            RecoveryConfinementBackendError::OwnerConflict
        );
    }
    for result in [mutation_arc, mutation_reference] {
        assert_eq!(
            result.unwrap_err(),
            RecoveryConfinementBackendError::OwnerConflict
        );
    }
    for (capture, mutation) in stale_results {
        assert_eq!(
            capture.unwrap_err(),
            RecoveryConfinementBackendError::TargetChanged
        );
        assert_eq!(
            mutation.unwrap_err(),
            RecoveryConfinementBackendError::TargetChanged
        );
    }
    assert_eq!(
        custom_capture.unwrap_err(),
        RecoveryConfinementBackendError::UnsupportedOperation
    );
    assert_eq!(
        custom_mutation.unwrap_err(),
        RecoveryConfinementBackendError::UnsupportedOperation
    );
}
