#![allow(dead_code)]

pub mod failures;

use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{ExecutionFence, JournalEntry, Store, StoreError};
use serde_json::{json, Map};
use std::sync::Barrier;

pub fn initial(id: &str, owner: u64, batches: Vec<Vec<TaskSpec>>) -> Snapshot {
    new_execution(
        ExecutionId::new(id).unwrap(),
        TurnId::new(format!("{id}-turn")).unwrap(),
        owner,
        Definition {
            route: RouteKey::new("/store-test#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches,
            max_attempts: 8,
        },
        Map::new(),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap()
}

pub fn task(id: &str, approval: bool) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        effect_id: EffectId::new(format!("effect-{id}")).unwrap(),
        recovery: RecoveryPolicy::Pure,
        requires_approval: approval,
    }
}

pub fn plan(snapshot: &Snapshot, command: KernelCommand) -> CommitPlan {
    transition(
        snapshot,
        command,
        TransitionFacts {
            now_ms: snapshot.updated_at_ms + 1,
        },
    )
    .unwrap()
}

pub fn noop(snapshot: &Snapshot) -> CommitPlan {
    CommitPlan {
        expected_revision: snapshot.revision,
        snapshot: snapshot.clone(),
        dispatchable: vec![],
    }
}

fn commit<S: Store>(store: &S, snapshot: &Snapshot, command: KernelCommand) -> Snapshot {
    store
        .commit(
            &snapshot.execution_id,
            ExecutionFence::from(snapshot),
            plan(snapshot, command),
        )
        .unwrap()
        .snapshot
}

/// Behavioral contract shared by every store implementation. Each call expects
/// a fresh store; all fixtures use its current owner generation.
pub fn backend_contract<S: Store>(store: &S) {
    thread_and_admission_contract(store);
    commit_and_journal_contract(store);
    terminal_history_contract(store);
    busy_status_contract(store);
    concurrent_admission_contract(store);
}

fn thread_and_admission_contract<S: Store>(store: &S) {
    let thread_id = ThreadId::new("admission-thread").unwrap();
    let execution = initial("admission-execution", store.owner_generation(), vec![]);
    assert_ne!(store.owner_generation(), 0);
    assert_eq!(store.thread(&thread_id).unwrap(), None);
    assert_eq!(store.execution(&execution.execution_id).unwrap(), None);
    assert!(store.journal(&execution.execution_id).unwrap().is_empty());
    assert_eq!(
        store.admit(&thread_id, execution.clone()),
        Err(StoreError::ThreadNotFound)
    );
    assert_eq!(
        store
            .create_thread(thread_id.clone())
            .unwrap()
            .active_execution_id,
        None
    );
    assert_eq!(
        store.create_thread(thread_id.clone()),
        Err(StoreError::ThreadExists)
    );

    let mut invalid = execution.clone();
    invalid.turn_history.clear();
    assert!(matches!(
        store.admit(&thread_id, invalid),
        Err(StoreError::InvalidSnapshot(_))
    ));
    let mut stale_owner = execution.clone();
    stale_owner.owner_generation += 1;
    assert_eq!(
        store.admit(&thread_id, stale_owner),
        Err(StoreError::OwnerConflict)
    );
    assert_eq!(
        store
            .thread(&thread_id)
            .unwrap()
            .unwrap()
            .active_execution_id,
        None
    );
    assert!(store.journal(&execution.execution_id).unwrap().is_empty());

    let admitted = store.admit(&thread_id, execution.clone()).unwrap();
    assert_eq!(admitted.thread_id, thread_id);
    assert_eq!(admitted.snapshot, execution);
    assert_eq!(
        store.execution(&execution.execution_id).unwrap(),
        Some(admitted)
    );
    assert_eq!(
        store
            .thread(&thread_id)
            .unwrap()
            .unwrap()
            .active_execution_id,
        Some(execution.execution_id.clone())
    );
    assert_eq!(
        store.journal(&execution.execution_id).unwrap(),
        vec![JournalEntry::from(&execution)]
    );
    assert_eq!(
        store.admit(
            &thread_id,
            initial("busy-admission", store.owner_generation(), vec![])
        ),
        Err(StoreError::ThreadBusy)
    );
    let second_thread = ThreadId::new("global-id-thread").unwrap();
    store.create_thread(second_thread.clone()).unwrap();
    assert_eq!(
        store.admit(&second_thread, execution),
        Err(StoreError::ExecutionExists)
    );
    assert_eq!(
        store
            .thread(&second_thread)
            .unwrap()
            .unwrap()
            .active_execution_id,
        None
    );
}

fn commit_and_journal_contract<S: Store>(store: &S) {
    let thread_id = ThreadId::new("commit-thread").unwrap();
    store.create_thread(thread_id.clone()).unwrap();
    let initial = initial("commit-execution", store.owner_generation(), vec![]);
    store.admit(&thread_id, initial.clone()).unwrap();
    let expected = plan(&initial, KernelCommand::Start);

    let stale_revision = ExecutionFence {
        revision: initial.revision + 1,
        owner_generation: initial.owner_generation,
    };
    assert_eq!(
        store.commit(&initial.execution_id, stale_revision, expected.clone()),
        Err(StoreError::RevisionConflict)
    );
    let stale_owner = ExecutionFence {
        revision: initial.revision,
        owner_generation: initial.owner_generation + 1,
    };
    assert_eq!(
        store.commit(&initial.execution_id, stale_owner, expected.clone()),
        Err(StoreError::OwnerConflict)
    );
    let mut wrong_plan_revision = expected.clone();
    wrong_plan_revision.expected_revision += 1;
    assert_eq!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            wrong_plan_revision
        ),
        Err(StoreError::RevisionConflict)
    );
    let mut wrong_owner = expected.clone();
    wrong_owner.snapshot.owner_generation += 1;
    assert_eq!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            wrong_owner
        ),
        Err(StoreError::OwnerConflict)
    );
    let mut wrong_id = expected.clone();
    wrong_id.snapshot.execution_id = ExecutionId::new("changed-execution").unwrap();
    assert!(matches!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            wrong_id
        ),
        Err(StoreError::InvalidCommit(_))
    ));
    let mut wrong_definition = expected.clone();
    wrong_definition.snapshot.definition.revision =
        ExecutionRevision::new("changed-definition").unwrap();
    assert!(matches!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            wrong_definition
        ),
        Err(StoreError::InvalidCommit(_))
    ));
    let mut malformed = expected.clone();
    malformed.snapshot.output = Some(json!({"forged": true}));
    assert!(matches!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            malformed
        ),
        Err(StoreError::InvalidSnapshot(_))
    ));
    let mut changed_same_revision = noop(&initial);
    changed_same_revision.snapshot.updated_at_ms += 1;
    assert!(matches!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            changed_same_revision
        ),
        Err(StoreError::InvalidCommit(_))
    ));
    let mut jumped_revision = expected.clone();
    jumped_revision.snapshot.revision += 1;
    assert!(matches!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            jumped_revision
        ),
        Err(StoreError::InvalidCommit(_))
    ));
    assert_eq!(
        store
            .execution(&initial.execution_id)
            .unwrap()
            .unwrap()
            .snapshot,
        initial
    );
    assert_eq!(
        store.journal(&initial.execution_id).unwrap(),
        vec![JournalEntry::from(&initial)]
    );

    let changed = store
        .commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            expected.clone(),
        )
        .unwrap();
    assert_eq!(changed.snapshot, expected.snapshot);
    assert_eq!(
        store.commit(
            &initial.execution_id,
            ExecutionFence::from(&initial),
            expected
        ),
        Err(StoreError::RevisionConflict)
    );
    let duplicate = store
        .commit(
            &initial.execution_id,
            ExecutionFence::from(&changed.snapshot),
            noop(&changed.snapshot),
        )
        .unwrap();
    assert_eq!(duplicate, changed);
    assert_eq!(
        store.journal(&initial.execution_id).unwrap(),
        vec![
            JournalEntry::from(&initial),
            JournalEntry::from(&changed.snapshot)
        ]
    );
    assert_eq!(
        store.commit(
            &ExecutionId::new("unknown-execution").unwrap(),
            ExecutionFence::from(&initial),
            noop(&initial)
        ),
        Err(StoreError::ExecutionNotFound)
    );
}

fn terminal_history_contract<S: Store>(store: &S) {
    for status in [
        ExecutionStatus::Completed,
        ExecutionStatus::Failed,
        ExecutionStatus::Cancelled,
    ] {
        let name = format!("terminal-{status:?}");
        let thread_id = ThreadId::new(format!("{name}-thread")).unwrap();
        store.create_thread(thread_id.clone()).unwrap();
        let batches = match status {
            ExecutionStatus::Completed => vec![],
            ExecutionStatus::Failed => vec![vec![task("a", false), task("b", false)]],
            _ => vec![vec![task("a", false)]],
        };
        let accepted = initial(&name, store.owner_generation(), batches);
        store.admit(&thread_id, accepted.clone()).unwrap();
        let mut current = commit(store, &accepted, KernelCommand::Start);
        if status == ExecutionStatus::Cancelled {
            current = commit(
                store,
                &current,
                KernelCommand::Cancel {
                    turn_id: current.turn_id.clone(),
                    owner_generation: current.owner_generation,
                },
            );
        } else if status == ExecutionStatus::Failed {
            for id in ["a", "b"] {
                current = commit(
                    store,
                    &current,
                    KernelCommand::Dispatch {
                        task_id: TaskId::new(id).unwrap(),
                        attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
                    },
                );
            }
            for id in ["a", "b"] {
                current = commit(
                    store,
                    &current,
                    KernelCommand::Complete {
                        task_id: TaskId::new(id).unwrap(),
                        attempt_id: AttemptId::new(format!("attempt-{id}")).unwrap(),
                        result: TaskResult {
                            output: json!(id),
                            writes: vec![StateWrite::Replace {
                                field: "conflict".into(),
                                value: json!(id),
                            }],
                        },
                    },
                );
            }
        }
        assert_eq!(current.status, status);
        let journal = store.journal(&current.execution_id).unwrap();
        assert_eq!(journal.first().unwrap().revision, 0);
        assert_eq!(journal.last().unwrap(), &JournalEntry::from(&current));
        for entries in journal.windows(2) {
            assert_eq!(entries[1].revision, entries[0].revision + 1);
            assert!(entries[1].updated_at_ms >= entries[0].updated_at_ms);
        }
        let successor = initial(
            &format!("{name}-successor"),
            store.owner_generation(),
            vec![],
        );
        store.admit(&thread_id, successor.clone()).unwrap();
        assert_eq!(
            store
                .thread(&thread_id)
                .unwrap()
                .unwrap()
                .active_execution_id,
            Some(successor.execution_id)
        );
        assert_eq!(
            store
                .execution(&current.execution_id)
                .unwrap()
                .unwrap()
                .snapshot,
            current
        );
        assert_eq!(
            store.commit(
                &current.execution_id,
                ExecutionFence::from(&current),
                noop(&current)
            ),
            Err(StoreError::ExecutionNotActive)
        );
        assert_eq!(store.journal(&current.execution_id).unwrap(), journal);
        let another_thread = ThreadId::new(format!("{name}-another-thread")).unwrap();
        store.create_thread(another_thread.clone()).unwrap();
        assert_eq!(
            store.admit(&another_thread, accepted),
            Err(StoreError::ExecutionExists)
        );
    }
}

fn busy_status_contract<S: Store>(store: &S) {
    for status in [
        ExecutionStatus::Running,
        ExecutionStatus::WaitingForApproval,
        ExecutionStatus::CancelRequested,
        ExecutionStatus::RecoveryBlocked,
    ] {
        let name = format!("busy-{status:?}");
        let thread_id = ThreadId::new(format!("{name}-thread")).unwrap();
        store.create_thread(thread_id.clone()).unwrap();
        let accepted = initial(
            &name,
            store.owner_generation(),
            vec![vec![task(
                "a",
                status == ExecutionStatus::WaitingForApproval,
            )]],
        );
        store.admit(&thread_id, accepted.clone()).unwrap();
        let mut current = commit(store, &accepted, KernelCommand::Start);
        match status {
            ExecutionStatus::WaitingForApproval => {
                current = commit(
                    store,
                    &current,
                    KernelCommand::Park {
                        task_id: TaskId::new("a").unwrap(),
                        interrupt_id: InterruptId::new("approval-a").unwrap(),
                        prompt: json!("approve"),
                    },
                )
            }
            ExecutionStatus::CancelRequested => {
                current = commit(
                    store,
                    &current,
                    KernelCommand::Dispatch {
                        task_id: TaskId::new("a").unwrap(),
                        attempt_id: AttemptId::new("attempt-a").unwrap(),
                    },
                );
                current = commit(
                    store,
                    &current,
                    KernelCommand::Cancel {
                        turn_id: current.turn_id.clone(),
                        owner_generation: current.owner_generation,
                    },
                );
            }
            ExecutionStatus::RecoveryBlocked => {
                let mut blocked = noop(&current);
                blocked.snapshot.revision += 1;
                blocked.snapshot.status = ExecutionStatus::RecoveryBlocked;
                blocked.snapshot.failure = Some("execution_revision_unavailable".into());
                current = store
                    .commit(
                        &current.execution_id,
                        ExecutionFence::from(&current),
                        blocked,
                    )
                    .unwrap()
                    .snapshot;
            }
            _ => {}
        }
        assert_eq!(current.status, status);
        let replacement = initial(
            &format!("{name}-replacement"),
            store.owner_generation(),
            vec![],
        );
        assert_eq!(
            store.admit(&thread_id, replacement.clone()),
            Err(StoreError::ThreadBusy)
        );
        assert!(store
            .execution(&replacement.execution_id)
            .unwrap()
            .is_none());
        assert!(store.journal(&replacement.execution_id).unwrap().is_empty());
        assert_eq!(
            store
                .thread(&thread_id)
                .unwrap()
                .unwrap()
                .active_execution_id,
            Some(current.execution_id)
        );
    }
}

fn concurrent_admission_contract<S: Store>(store: &S) {
    let thread_id = ThreadId::new("concurrent-thread").unwrap();
    store.create_thread(thread_id.clone()).unwrap();
    let barrier = Barrier::new(8);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|index| {
                let thread_id = &thread_id;
                let barrier = &barrier;
                scope.spawn(move || {
                    let candidate = initial(
                        &format!("concurrent-execution-{index}"),
                        store.owner_generation(),
                        vec![],
                    );
                    barrier.wait();
                    (
                        candidate.execution_id.clone(),
                        store.admit(thread_id, candidate),
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        results.iter().filter(|(_, result)| result.is_ok()).count(),
        1
    );
    for (id, result) in results {
        if let Ok(admitted) = result {
            assert_eq!(
                store
                    .thread(&thread_id)
                    .unwrap()
                    .unwrap()
                    .active_execution_id,
                Some(id.clone())
            );
            assert_eq!(
                store.journal(&id).unwrap(),
                vec![JournalEntry::from(&admitted.snapshot)]
            );
        } else {
            assert_eq!(result, Err(StoreError::ThreadBusy));
            assert!(store.execution(&id).unwrap().is_none());
            assert!(store.journal(&id).unwrap().is_empty());
        }
    }
}
