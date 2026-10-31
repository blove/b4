use b4_contract::*;
use b4_runtime::*;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::time::Duration;
#[path = "driver/support.rs"]
mod support;
use support::*;

#[test]
fn callback_overlap_and_reverse_completion_join_in_definition_order() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "overlap", &[("a", false), ("b", false)]);
    let gate = Arc::new((Mutex::new((0usize, false)), Condvar::new()));
    let invoke_gate = gate.clone();
    let mut host = host(store.clone(), move |call| {
        assert_eq!(call.input["input"], json!({"seed":0}));
        let (lock, wake) = &*invoke_gate;
        let mut state = lock.lock().unwrap();
        state.0 += 1;
        wake.notify_all();
        if call.task_id.as_str() == "a" {
            let (state, timeout) = wake
                .wait_timeout_while(state, Duration::from_secs(2), |s| !s.1)
                .unwrap();
            if timeout.timed_out() || state.0 != 2 {
                return Err("second callback did not overlap".into());
            }
        }
        Ok(result(call))
    });
    let commit_gate = gate.clone();
    host.commit = Box::new(move |call| {
        if call.task_id.as_str() == "b" {
            let (lock, wake) = &*commit_gate;
            lock.lock().unwrap().1 = true;
            wake.notify_all();
        }
    });
    let done = drive_execution(
        &Runtime::new(store.clone()),
        initial,
        &host,
        &TaskCapacity::new(2).unwrap(),
        DriverConfig::default(),
    )
    .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(done.snapshot.values["items"], json!(["a", "b"]));
    assert_eq!(done.snapshot.attempts_used, 2);
    assert_eq!(host.fault.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_driver_limits_refuse_before_start_or_callback() {
    for config in [
        DriverConfig {
            max_parallel_tasks: 0,
            ..DriverConfig::default()
        },
        DriverConfig {
            max_parallel_tasks: 65,
            ..DriverConfig::default()
        },
        DriverConfig {
            completion_attempts: 0,
            ..DriverConfig::default()
        },
        DriverConfig {
            completion_attempts: 65,
            ..DriverConfig::default()
        },
        DriverConfig {
            max_outcome_bytes: 0,
            ..DriverConfig::default()
        },
        DriverConfig {
            max_outcome_bytes: 65_537,
            ..DriverConfig::default()
        },
    ] {
        let store = Arc::new(MemoryStore::new());
        let initial = accepted(&store, "invalid", &[("a", false)]);
        let before = store.journal(&initial.snapshot.execution_id).unwrap();
        let host = host(store.clone(), |_| panic!("invalid config invoked callback"));
        let err = drive_execution(
            &Runtime::new(store.clone()),
            initial.clone(),
            &host,
            &TaskCapacity::new(2).unwrap(),
            config,
        )
        .unwrap_err();
        assert!(matches!(err.errors[0], DriverError::Config(_)));
        assert_eq!(
            store.execution(&initial.snapshot.execution_id).unwrap(),
            Some(initial)
        );
        assert_eq!(
            store
                .journal(&ExecutionId::new("invalid").unwrap())
                .unwrap(),
            before
        );
        assert_eq!(host.fault.load(Ordering::SeqCst), 0);
    }
    assert!(TaskCapacity::new(0).is_err());
    assert!(TaskCapacity::new(257).is_err());
}

#[test]
fn callback_capacity_one_processes_returned_permit_before_waiting() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "one", &[("a", false), ("b", false), ("c", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    let done = drive_execution(
        &Runtime::new(store),
        initial,
        &host,
        &TaskCapacity::new(1).unwrap(),
        DriverConfig::default(),
    )
    .unwrap();
    assert_eq!(done.snapshot.values["items"], json!(["a", "b", "c"]));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[test]
fn required_park_bypasses_an_earlier_ordinary_task_waiting_for_capacity() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(
        &store,
        "park-before-capacity",
        &[("a", false), ("b", false), ("c", true)],
    );
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = gate.clone();
    let entered = Arc::new(AtomicUsize::new(0));
    let callbacks = entered.clone();
    let host = host(store.clone(), move |call| {
        callbacks.fetch_add(1, Ordering::SeqCst);
        if call.task_id.as_str() == "a" {
            let (lock, wake) = &*callback_gate;
            let state = lock.lock().unwrap();
            let (_released, timeout) = wake
                .wait_timeout_while(state, Duration::from_secs(5), |released| !*released)
                .unwrap();
            assert!(!timeout.timed_out(), "callback cleanup timed out");
        }
        Ok(result(call))
    });
    let capacity = TaskCapacity::new(1).unwrap();
    std::thread::scope(|scope| {
        let driven = scope.spawn(|| {
            drive_execution(
                &Runtime::new(store.clone()),
                initial.clone(),
                &host,
                &capacity,
                DriverConfig {
                    max_parallel_tasks: 1,
                    ..DriverConfig::default()
                },
            )
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut parked = false;
        while std::time::Instant::now() < deadline {
            let current = store
                .execution(&initial.snapshot.execution_id)
                .unwrap()
                .unwrap();
            parked = current.snapshot.tasks.len() == 3
                && current.snapshot.tasks[0].status == TaskStatus::InFlight
                && current.snapshot.tasks[1].status == TaskStatus::Ready
                && current.snapshot.tasks[2].status == TaskStatus::WaitingForApproval;
            if parked {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let observed = entered.load(Ordering::SeqCst);
        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        let done = driven.join().unwrap().unwrap();
        assert_eq!(done.snapshot.status, ExecutionStatus::WaitingForApproval);
        assert!(
            parked,
            "required sibling never parked while callback capacity was held"
        );
        assert_eq!(observed, 1);
        assert_eq!(done.snapshot.values["input"], json!({"seed":0}));
        assert!(done.snapshot.values.get("items").is_none());
        assert_eq!(done.snapshot.tasks[0].status, TaskStatus::Succeeded);
        assert_eq!(done.snapshot.tasks[1].status, TaskStatus::Succeeded);
    });
}

#[test]
fn fabricated_preparation_cannot_invoke_before_a_durable_dispatch_claim() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "fabricated", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    host.prepare = Some(Box::new(|current, task_id, attempt_id| {
        let plan = b4_kernel::transition(
            &current.snapshot,
            KernelCommand::Dispatch {
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
            },
            TransitionFacts {
                now_ms: current.snapshot.updated_at_ms + 1,
            },
        )
        .unwrap();
        let task = plan
            .snapshot
            .tasks
            .iter()
            .find(|t| t.spec.id == task_id)
            .unwrap();
        Ok(PreparedEffect {
            invocation: EffectInvocation {
                execution_id: plan.snapshot.execution_id.clone(),
                turn_id: plan.snapshot.turn_id.clone(),
                owner_generation: plan.snapshot.owner_generation,
                task_id,
                attempt_id,
                effect_id: task.spec.effect_id.clone(),
                input: task.input.clone(),
            },
            execution: StoredExecution {
                thread_id: current.thread_id.clone(),
                snapshot: plan.snapshot,
            },
        })
    }));
    let failed = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(2).unwrap(),
        DriverConfig::default(),
    )
    .unwrap_err();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "callback ran before its Dispatch commit"
    );
    assert!(matches!(
        failed.errors[0],
        DriverError::InvalidPreparedEffect
    ));
    assert!(failed.unsettled_invocations.is_empty());
    let durable = store
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.snapshot.attempts_used, 0);
    assert_eq!(durable.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(host.fault.load(Ordering::SeqCst) > 0);
}

#[test]
fn preparation_panic_preserves_only_its_real_durable_never_entered_attempt() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "prepare-panic", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    let claimed_store = store.clone();
    host.prepare = Some(Box::new(move |current, task_id, attempt_id| {
        Runtime::new(claimed_store.clone())
            .prepare_dispatch(
                &current.snapshot.execution_id,
                ExecutionFence::from(&current.snapshot),
                task_id,
                attempt_id,
                TransitionFacts {
                    now_ms: current.snapshot.updated_at_ms + 1,
                },
            )
            .unwrap();
        panic!("static renewed policy panic");
    }));
    let failure = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(2).unwrap(),
        DriverConfig::default(),
    )
    .unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(failure.errors[0], DriverError::CoordinatorPanic));
    assert!(failure.retained_outcomes.is_empty());
    assert_eq!(
        failure.unsettled_invocations.len(),
        1,
        "durable claim evidence lost after policy panic"
    );
    let durable = store
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        failure.unsettled_invocations[0].attempt_id,
        *durable.snapshot.tasks[0].attempt_id.as_ref().unwrap()
    );
    assert_eq!(
        failure.unsettled_invocations[0].input,
        durable.snapshot.tasks[0].input
    );
    assert!(host.fault.load(Ordering::SeqCst) > 0);
}

#[test]
fn fabricated_preparation_refusal_cannot_report_an_uncommitted_attempt() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "fabricated", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    host.prepare = Some(Box::new(|current, task_id, attempt_id| {
        let plan = b4_kernel::transition(
            &current.snapshot,
            KernelCommand::Dispatch {
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
            },
            TransitionFacts {
                now_ms: current.snapshot.updated_at_ms + 1,
            },
        )
        .unwrap();
        let task = plan
            .snapshot
            .tasks
            .iter()
            .find(|t| t.spec.id == task_id)
            .unwrap();
        let uncommitted = PreparedEffect {
            invocation: EffectInvocation {
                execution_id: plan.snapshot.execution_id.clone(),
                turn_id: plan.snapshot.turn_id.clone(),
                owner_generation: plan.snapshot.owner_generation,
                task_id,
                attempt_id,
                effect_id: task.spec.effect_id.clone(),
                input: task.input.clone(),
            },
            execution: StoredExecution {
                thread_id: current.thread_id.clone(),
                snapshot: plan.snapshot,
            },
        };
        Err(PreparationError {
            cause: RuntimeError::EffectFailed("refused".into()),
            claimed: Some(Box::new(uncommitted)),
        })
    }));
    let failed = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(2).unwrap(),
        DriverConfig::default(),
    )
    .unwrap_err();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "callback ran before its Dispatch commit"
    );
    assert!(matches!(
        failed.errors[0],
        DriverError::InvalidPreparedEffect
    ));
    assert!(failed.unsettled_invocations.is_empty());
    let durable = store
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.snapshot.attempts_used, 0);
    assert_eq!(durable.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(host.fault.load(Ordering::SeqCst) > 0);
}
