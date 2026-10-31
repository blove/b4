use super::*;
use crate::discovery::DiscoveryError;
use serde_json::{Map, Value};
use std::sync::Arc;

fn admitted(store: &MemoryStore, ordinal: usize) -> StoredExecution {
    let thread = ThreadId::new(format!("private-thread-{ordinal:04}")).unwrap();
    store.create_thread(thread.clone()).unwrap();
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new(format!("private-execution-{ordinal:04}")).unwrap(),
        TurnId::new(format!("private-turn-{ordinal:04}")).unwrap(),
        1,
        Definition {
            route: RouteKey::new("/private#workflow").unwrap(),
            revision: ExecutionRevision::new("private-v1").unwrap(),
            batches: vec![],
            max_attempts: 8,
        },
        Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    store.admit(&thread, snapshot).unwrap()
}

#[test]
fn discovery_refuses_corrupt_selected_source_without_mutation() {
    for case in 0..9 {
        let store = MemoryStore::new();
        let stored = admitted(&store, 1);
        let id = stored.snapshot.execution_id;
        let foreign = ThreadId::new("foreign").unwrap();
        store.create_thread(foreign.clone()).unwrap();
        let mut state = store.lock().unwrap();
        match case {
            0 => {
                state.executions.get_mut(&id).unwrap().snapshot.execution_id =
                    ExecutionId::new("payload-mismatch").unwrap()
            }
            1 => {
                state.executions.get_mut(&id).unwrap().thread_id =
                    ThreadId::new("missing-thread").unwrap()
            }
            2 => state.threads.get_mut(&stored.thread_id).unwrap().id = foreign.clone(),
            3 => {
                state
                    .threads
                    .get_mut(&stored.thread_id)
                    .unwrap()
                    .active_execution_id = None
            }
            4 => state.threads.get_mut(&foreign).unwrap().active_execution_id = Some(id.clone()),
            5 => {
                state
                    .threads
                    .get_mut(&stored.thread_id)
                    .unwrap()
                    .active_execution_id = Some(ExecutionId::new("missing-active").unwrap())
            }
            6 => {
                state
                    .executions
                    .get_mut(&id)
                    .unwrap()
                    .snapshot
                    .owner_generation = 0
            }
            7 => {
                state
                    .executions
                    .get_mut(&id)
                    .unwrap()
                    .snapshot
                    .schema_version = 2
            }
            _ => {
                state
                    .executions
                    .get_mut(&id)
                    .unwrap()
                    .snapshot
                    .owner_generation = 2
            }
        }
        let before = state.clone();
        drop(state);
        let expected = match case {
            7 => DiscoveryError::UnsupportedSnapshot,
            8 => DiscoveryError::OwnerConflict,
            _ => DiscoveryError::CorruptRecord,
        };
        assert!(matches!(store.discover_unfinished(None), Err(error) if error == expected));
        assert_eq!(*store.lock().unwrap(), before);
    }
}

#[test]
fn discovery_validates_terminal_history_and_foreign_pointer() {
    let store = MemoryStore::new();
    let initial = admitted(&store, 1);
    let terminal = store
        .commit(
            &initial.snapshot.execution_id,
            (&initial.snapshot).into(),
            b4_kernel::transition(
                &initial.snapshot,
                KernelCommand::Start,
                TransitionFacts { now_ms: 2 },
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(terminal.snapshot.status, ExecutionStatus::Completed);
    assert!(store.discover_unfinished(None).unwrap().records.is_empty());
    let foreign = ThreadId::new("foreign").unwrap();
    store.create_thread(foreign.clone()).unwrap();
    store
        .lock()
        .unwrap()
        .threads
        .get_mut(&foreign)
        .unwrap()
        .active_execution_id = Some(terminal.snapshot.execution_id);
    let before = store.lock().unwrap().clone();
    assert!(matches!(
        store.discover_unfinished(None),
        Err(DiscoveryError::CorruptRecord)
    ));
    assert_eq!(*store.lock().unwrap(), before);
}

#[test]
fn discovery_refuses_unfinished_history_behind_a_terminal_active_pointer() {
    let store = MemoryStore::new();
    let initial = admitted(&store, 1);
    store
        .commit(
            &initial.snapshot.execution_id,
            (&initial.snapshot).into(),
            b4_kernel::transition(
                &initial.snapshot,
                KernelCommand::Start,
                TransitionFacts { now_ms: 2 },
            )
            .unwrap(),
        )
        .unwrap();
    let mut hidden = initial;
    hidden.snapshot.execution_id = ExecutionId::new("aaa-hidden-unfinished").unwrap();
    store
        .lock()
        .unwrap()
        .executions
        .insert(hidden.snapshot.execution_id.clone(), hidden);
    let before = store.lock().unwrap().clone();
    assert!(matches!(
        store.discover_unfinished(None),
        Err(DiscoveryError::CorruptRecord)
    ));
    assert_eq!(*store.lock().unwrap(), before);
}

#[test]
fn identity_only_lookahead_does_not_validate_or_measure_its_payload() {
    let store = MemoryStore::new();
    for ordinal in 1..=33 {
        admitted(&store, ordinal);
    }
    let last = ExecutionId::new("private-execution-0033").unwrap();
    store
        .lock()
        .unwrap()
        .executions
        .get_mut(&last)
        .unwrap()
        .snapshot
        .schema_version = 2;
    let before = store.lock().unwrap().clone();
    let page = store.discover_unfinished(None).unwrap();
    assert_eq!(page.scanned_rows, 32);
    assert_eq!(page.records.len(), 32);
    assert!(page.next_cursor.is_some());
    assert!(matches!(
        store.discover_unfinished(page.next_cursor.as_ref()),
        Err(DiscoveryError::UnsupportedSnapshot)
    ));
    assert_eq!(*store.lock().unwrap(), before);
}

#[test]
fn discovery_limits_borrowed_values_before_recursive_kernel_work() {
    let store = MemoryStore::new();
    let stored = admitted(&store, 1);
    let mut value = Value::Null;
    for _ in 0..65 {
        value = Value::Array(vec![value]);
    }
    store
        .lock()
        .unwrap()
        .executions
        .get_mut(&stored.snapshot.execution_id)
        .unwrap()
        .snapshot
        .values
        .insert("deep".into(), value);
    let before = store.lock().unwrap().clone();
    b4_kernel::validate_snapshot(&before.executions[&stored.snapshot.execution_id].snapshot)
        .unwrap();
    assert!(matches!(
        store.discover_unfinished(None),
        Err(DiscoveryError::LimitExceeded)
    ));
    assert_eq!(*store.lock().unwrap(), before);
}

#[test]
fn discovery_poison_is_static_unavailable() {
    let store = Arc::new(MemoryStore::new());
    let other = Arc::clone(&store);
    let result = std::thread::spawn(move || {
        let _guard = other.state.lock().unwrap();
        panic!("private intentional poison");
    })
    .join();
    assert!(result.is_err());
    assert!(matches!(
        store.discover_unfinished(None),
        Err(DiscoveryError::Unavailable)
    ));
}
