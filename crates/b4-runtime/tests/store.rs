mod support;

use b4_contract::*;
use b4_runtime::{validate_commit, validate_initial, ExecutionFence, MemoryStore, StoreError};
use serde_json::json;
use support::{initial, noop, plan, task};

#[test]
fn memory_backend_satisfies_common_contract() {
    support::backend_contract(&MemoryStore::new());
}

#[test]
fn default_memory_backend_satisfies_common_contract() {
    support::backend_contract(&MemoryStore::default());
}

#[test]
fn admission_requires_valid_accepted_revision_zero_and_current_owner() {
    let snapshot = initial("initial-validator", 1, vec![]);
    assert_eq!(validate_initial(&snapshot, 1), Ok(()));
    assert_eq!(
        validate_initial(&snapshot, 2),
        Err(StoreError::OwnerConflict)
    );
    assert_eq!(
        validate_initial(&snapshot, 0),
        Err(StoreError::OwnerConflict)
    );
    let mut revision = snapshot.clone();
    revision.revision = 1;
    assert!(matches!(
        validate_initial(&revision, 1),
        Err(StoreError::InvalidSnapshot(_))
    ));
    let started = plan(&snapshot, KernelCommand::Start).snapshot;
    assert!(matches!(
        validate_initial(&started, 1),
        Err(StoreError::InvalidSnapshot(_))
    ));
    let mut malformed = snapshot;
    malformed.owner_generation = 0;
    assert_eq!(
        validate_initial(&malformed, 1),
        Err(StoreError::InvalidSnapshot(
            "invalid_owner_or_budget".into()
        ))
    );
}

#[test]
fn commit_validates_both_snapshots_and_only_identical_noop_retains_revision() {
    let snapshot = initial("commit-validator", 1, vec![]);
    let fence = ExecutionFence::from(&snapshot);
    assert_eq!(
        validate_commit(&snapshot, fence, &noop(&snapshot), 1),
        Ok(false)
    );
    assert_eq!(
        validate_commit(&snapshot, fence, &plan(&snapshot, KernelCommand::Start), 1),
        Ok(true)
    );
    let mut timestamp_only = noop(&snapshot);
    timestamp_only.snapshot.updated_at_ms += 1;
    assert!(matches!(
        validate_commit(&snapshot, fence, &timestamp_only, 1),
        Err(StoreError::InvalidCommit(_))
    ));
    let mut malformed = snapshot.clone();
    malformed.turn_history.clear();
    assert!(matches!(
        validate_commit(&malformed, fence, &noop(&snapshot), 1),
        Err(StoreError::InvalidSnapshot(_))
    ));
    let mut proposed = noop(&snapshot);
    proposed.snapshot.turn_history.clear();
    assert!(matches!(
        validate_commit(&snapshot, fence, &proposed, 1),
        Err(StoreError::InvalidSnapshot(_))
    ));
}

#[test]
fn commit_preserves_definition_identity_time_and_history_prefixes() {
    let snapshot = initial("immutable-validator", 1, vec![]);
    let fence = ExecutionFence::from(&snapshot);
    let mut changed = plan(&snapshot, KernelCommand::Start);
    changed.snapshot.execution_id = ExecutionId::new("different-execution").unwrap();
    assert_eq!(
        validate_commit(&snapshot, fence, &changed, 1),
        Err(StoreError::InvalidCommit(
            "execution_identity_changed".into()
        ))
    );
    changed = plan(&snapshot, KernelCommand::Start);
    changed.snapshot.definition.route = RouteKey::new("/different#agent").unwrap();
    assert_eq!(
        validate_commit(&snapshot, fence, &changed, 1),
        Err(StoreError::InvalidCommit("definition_changed".into()))
    );
    changed = plan(&snapshot, KernelCommand::Start);
    changed.snapshot.updated_at_ms = snapshot.updated_at_ms - 1;
    assert_eq!(
        validate_commit(&snapshot, fence, &changed, 1),
        Err(StoreError::InvalidCommit("clock_moved_backwards".into()))
    );
    changed = plan(&snapshot, KernelCommand::Start);
    changed.snapshot.turn_id = TurnId::new("replacement-turn").unwrap();
    changed.snapshot.turn_history = vec![changed.snapshot.turn_id.clone()];
    assert_eq!(
        validate_commit(&snapshot, fence, &changed, 1),
        Err(StoreError::InvalidCommit("turn_history_changed".into()))
    );

    let running = plan(
        &initial("attempt-validator", 1, vec![vec![task("a", false)]]),
        KernelCommand::Start,
    )
    .snapshot;
    let inflight = plan(
        &running,
        KernelCommand::Dispatch {
            task_id: TaskId::new("a").unwrap(),
            attempt_id: AttemptId::new("first-attempt").unwrap(),
        },
    )
    .snapshot;
    let mut recovery = plan(
        &inflight,
        KernelCommand::Recover {
            available_revision: inflight.definition.revision.clone(),
            turn_id: TurnId::new("recovery-turn").unwrap(),
            owner_generation: 2,
        },
    );
    assert_eq!(
        validate_commit(&inflight, ExecutionFence::from(&inflight), &recovery, 2),
        Ok(true)
    );
    recovery.snapshot.attempt_history[0] = AttemptId::new("forged-attempt").unwrap();
    recovery.snapshot.tasks[0].attempt_id = Some(recovery.snapshot.attempt_history[0].clone());
    assert_eq!(
        validate_commit(&inflight, ExecutionFence::from(&inflight), &recovery, 2),
        Err(StoreError::InvalidCommit("attempt_history_changed".into()))
    );
}

#[test]
fn commit_fences_transfer_to_current_owner_and_never_regress() {
    let snapshot = initial("owner-validator", 1, vec![]);
    let fence = ExecutionFence::from(&snapshot);
    let recovery = plan(
        &snapshot,
        KernelCommand::Recover {
            available_revision: snapshot.definition.revision.clone(),
            turn_id: TurnId::new("recovered-turn").unwrap(),
            owner_generation: 2,
        },
    );
    assert_eq!(validate_commit(&snapshot, fence, &recovery, 2), Ok(true));
    assert_eq!(
        validate_commit(&snapshot, fence, &recovery, 1),
        Err(StoreError::OwnerConflict)
    );
    assert_eq!(
        validate_commit(
            &snapshot,
            ExecutionFence {
                revision: 0,
                owner_generation: 2
            },
            &recovery,
            2
        ),
        Err(StoreError::OwnerConflict)
    );
    assert_eq!(
        validate_commit(
            &snapshot,
            ExecutionFence {
                revision: 1,
                owner_generation: 1
            },
            &recovery,
            2
        ),
        Err(StoreError::RevisionConflict)
    );
    let mut wrong_expected = recovery.clone();
    wrong_expected.expected_revision += 1;
    assert_eq!(
        validate_commit(&snapshot, fence, &wrong_expected, 2),
        Err(StoreError::RevisionConflict)
    );
    assert_eq!(
        validate_commit(&snapshot, fence, &noop(&snapshot), 2),
        Err(StoreError::OwnerConflict)
    );
    let newer = initial("newer-owner", 2, vec![]);
    let mut regression = plan(&newer, KernelCommand::Start);
    regression.snapshot.owner_generation = 1;
    assert_eq!(
        validate_commit(&newer, ExecutionFence::from(&newer), &regression, 1),
        Err(StoreError::OwnerConflict)
    );
}

#[test]
fn commit_requires_exactly_one_revision_increment_without_overflow() {
    let snapshot = initial("revision-validator", 1, vec![]);
    let fence = ExecutionFence::from(&snapshot);
    let mut jumped = plan(&snapshot, KernelCommand::Start);
    jumped.snapshot.revision += 1;
    assert_eq!(
        validate_commit(&snapshot, fence, &jumped, 1),
        Err(StoreError::InvalidCommit(
            "revision_must_increment_once".into()
        ))
    );
    let mut maximum = snapshot;
    maximum.revision = u64::MAX;
    assert_eq!(
        validate_commit(&maximum, ExecutionFence::from(&maximum), &noop(&maximum), 1),
        Ok(false)
    );
    let mut overflow = noop(&maximum);
    overflow.snapshot.revision = 0;
    overflow.snapshot.values.insert("new".into(), json!(true));
    assert_eq!(
        validate_commit(&maximum, ExecutionFence::from(&maximum), &overflow, 1),
        Err(StoreError::InvalidCommit(
            "revision_must_increment_once".into()
        ))
    );
}
