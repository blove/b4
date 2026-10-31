use b4_contract::*;
use b4_runtime::*;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
#[path = "driver/fault_store.rs"]
mod fault_store;
#[path = "driver/support.rs"]
mod support;
use fault_store::*;
use support::*;

#[derive(Default)]
struct Signals {
    bits: Mutex<u32>,
    wake: Condvar,
}
impl Signals {
    fn set(&self, bits: u32) {
        *self.bits.lock().unwrap() |= bits;
        self.wake.notify_all();
    }
    fn wait(&self, bits: u32) -> bool {
        let value = self.bits.lock().unwrap();
        let (value, _) = self
            .wake
            .wait_timeout_while(value, Duration::from_secs(10), |value| {
                *value & bits != bits
            })
            .unwrap();
        *value & bits == bits
    }
}
fn observed(check: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}
fn config() -> DriverConfig {
    DriverConfig::default()
}
fn assert_actual_retained(failure: &DriverFailure<RuntimeError>, expected: &EffectOutcome) {
    assert_eq!(failure.retained_outcomes.len(), 1);
    assert_eq!(&failure.retained_outcomes[0].outcome, expected);
    assert_eq!(failure.unsettled_invocations.len(), 1);
    assert_eq!(
        failure.retained_outcomes[0].invocation,
        failure.unsettled_invocations[0]
    );
}

fn unknown_sibling(panic: bool) {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "unknown-sibling", &[("a", false), ("b", false)]);
    let signals = Arc::new(Signals::default());
    let callback_signals = signals.clone();
    let host = host(store.clone(), move |call| {
        if call.task_id.as_str() == "a" {
            callback_signals.set(1);
            if !callback_signals.wait(2) {
                return Err("sibling never entered".into());
            }
            if panic {
                panic!("static callback panic fixture");
            }
            Err("static unknown effect outcome".into())
        } else {
            callback_signals.set(2);
            if !callback_signals.wait(4) {
                return Err("sibling cleanup timed out".into());
            }
            Ok(result(call))
        }
    });
    let capacity = TaskCapacity::new(2).unwrap();
    std::thread::scope(|scope| {
        let driven = scope.spawn(|| {
            drive_execution(
                &Runtime::new(store.clone()),
                initial.clone(),
                &host,
                &capacity,
                config(),
            )
        });
        let both_entered = signals.wait(3);
        let fault_before_join = observed(|| host.fault.load(Ordering::SeqCst) > 0);
        let before_release = store
            .execution(&initial.snapshot.execution_id)
            .unwrap()
            .unwrap();
        signals.set(4);
        let failure = driven.join().unwrap().unwrap_err();
        assert!(both_entered);
        assert!(
            fault_before_join,
            "uncertainty was reported only after sibling quiescence"
        );
        assert_eq!(
            before_release.snapshot.tasks[1].status,
            TaskStatus::InFlight
        );
        assert!(failure.errors.iter().any(|error| if panic {
            matches!(error, DriverError::CallbackPanic)
        } else {
            matches!(error, DriverError::CallbackUncertain)
        }));
        let after = store
            .execution(&initial.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(after.snapshot.tasks[0].status, TaskStatus::InFlight);
        assert_eq!(after.snapshot.tasks[1].status, TaskStatus::Succeeded);
        assert_eq!(
            after.snapshot.tasks[1].result.as_ref().unwrap().output,
            json!("b")
        );
        assert!(!after.snapshot.values.contains_key("items"));
        assert_eq!(failure.unsettled_invocations.len(), 1);
        assert_eq!(failure.unsettled_invocations[0].task_id.as_str(), "a");
        assert!(failure.retained_outcomes.is_empty());
    });
}
#[test]
fn callback_unknown_faults_before_uncooperative_sibling_joins_and_commits_real_sibling() {
    unknown_sibling(false);
}
#[test]
fn callback_panic_faults_before_uncooperative_sibling_joins_and_commits_real_sibling() {
    unknown_sibling(true);
}

#[test]
fn completion_clock_panic_keeps_queued_actual_outcome_and_exact_unsettled_attempt() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "clock-panic", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    host.clock = Some(Box::new(|current| {
        if current
            .snapshot
            .tasks
            .iter()
            .any(|task| task.status == TaskStatus::InFlight)
        {
            panic!("static completion clock panic fixture");
        }
        TransitionFacts {
            now_ms: current.snapshot.updated_at_ms + 1,
        }
    }));
    let failure = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(1).unwrap(),
        config(),
    )
    .unwrap_err();
    let actual = result(&failure.unsettled_invocations[0]);
    assert_actual_retained(&failure, &actual);
    assert!(failure
        .errors
        .iter()
        .any(|error| matches!(error, DriverError::CoordinatorPanic)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(host.fault.load(Ordering::SeqCst) > 0);
    assert_eq!(
        store
            .execution(&initial.snapshot.execution_id)
            .unwrap()
            .unwrap()
            .snapshot
            .tasks[0]
            .status,
        TaskStatus::InFlight
    );
}

#[test]
fn postcommit_projection_panic_never_reprojects_or_retains_an_already_durable_result() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "projection-panic", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let projections = Arc::new(AtomicUsize::new(0));
    let projected = projections.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    host.commit = Box::new(move |_| {
        projected.fetch_add(1, Ordering::SeqCst);
        panic!("static projection panic after durable completion");
    });
    let failure = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(1).unwrap(),
        config(),
    )
    .unwrap_err();
    assert!(failure
        .errors
        .iter()
        .any(|error| matches!(error, DriverError::CoordinatorPanic)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(projections.load(Ordering::SeqCst), 1);
    assert!(
        failure.retained_outcomes.is_empty(),
        "durably committed result was returned as uncommitted evidence"
    );
    assert!(failure.unsettled_invocations.is_empty());
    let current = store
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(current.snapshot.values["items"], json!(["a"]));
    assert_eq!(current.snapshot.attempts_used, 1);
    assert!(host.fault.load(Ordering::SeqCst) > 0);
}

#[test]
fn preparation_panic_drains_and_commits_an_already_produced_sibling_outcome() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "prepare-panic", &[("a", false), ("b", false)]);
    let signals = Arc::new(Signals::default());
    let callback_signals = signals.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        let actual = result(call);
        callback_signals.set(1);
        Ok(actual)
    });
    let prepare_store = store.clone();
    let prepare_signals = signals.clone();
    host.prepare = Some(Box::new(move |current, task, attempt| {
        if task.as_str() == "b" {
            assert!(prepare_signals.wait(1));
            panic!("static preparation panic fixture");
        }
        Runtime::new(&prepare_store).prepare_dispatch(
            &current.snapshot.execution_id,
            (&current.snapshot).into(),
            task,
            attempt,
            TransitionFacts {
                now_ms: current.snapshot.updated_at_ms + 1,
            },
        )
    }));
    let failure = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(2).unwrap(),
        config(),
    )
    .unwrap_err();
    assert!(failure
        .errors
        .iter()
        .any(|error| matches!(error, DriverError::CoordinatorPanic)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(host.fault.load(Ordering::SeqCst) > 0);
    let current = store
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(current.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        current.snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("a")
    );
    assert_eq!(current.snapshot.tasks[1].status, TaskStatus::Ready);
    assert_eq!(current.snapshot.attempts_used, 1);
    assert!(failure.retained_outcomes.is_empty());
    assert!(failure.unsettled_invocations.is_empty());
}

#[test]
fn postclaim_preparation_panic_returns_exact_durable_never_entered_attempt_without_an_outcome() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "postclaim-panic", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    let prepare_store = store.clone();
    host.prepare = Some(Box::new(move |current, task, attempt| {
        Runtime::new(&prepare_store)
            .prepare_dispatch(
                &current.snapshot.execution_id,
                (&current.snapshot).into(),
                task,
                attempt,
                TransitionFacts {
                    now_ms: current.snapshot.updated_at_ms + 1,
                },
            )
            .unwrap();
        panic!("static renewed policy panic after durable claim");
    }));
    let failure = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(1).unwrap(),
        config(),
    )
    .unwrap_err();
    assert!(failure
        .errors
        .iter()
        .any(|error| matches!(error, DriverError::CoordinatorPanic)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(host.fault.load(Ordering::SeqCst) > 0);
    assert!(failure.retained_outcomes.is_empty());
    let current = store
        .execution(&initial.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let task = &current.snapshot.tasks[0];
    assert_eq!(task.status, TaskStatus::InFlight);
    assert_eq!(current.snapshot.attempts_used, 1);
    assert!(task.result.is_none() && task.failure.is_none());
    assert_eq!(
        failure.unsettled_invocations.len(),
        1,
        "durably claimed attempt disappeared from panic evidence"
    );
    assert_eq!(
        failure.unsettled_invocations[0],
        EffectInvocation {
            execution_id: current.snapshot.execution_id.clone(),
            turn_id: current.snapshot.turn_id.clone(),
            owner_generation: current.snapshot.owner_generation,
            task_id: task.spec.id.clone(),
            effect_id: task.spec.effect_id.clone(),
            attempt_id: task.attempt_id.clone().unwrap(),
            input: task.input.clone(),
        }
    );
}

#[test]
fn healthy_first_refusal_is_escalated_when_entered_sibling_later_becomes_unknown() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(
        &store,
        "healthy-then-unknown",
        &[("a", false), ("b", false)],
    );
    let signals = Arc::new(Signals::default());
    let callback_signals = signals.clone();
    let mut host = host(store.clone(), move |_| {
        callback_signals.set(1);
        if !callback_signals.wait(4) {
            return Err("cleanup timed out".into());
        }
        Err("static later uncertainty".into())
    });
    host.healthy_refusals = true;
    let prepare_store = store.clone();
    let prepare_signals = signals.clone();
    host.prepare = Some(Box::new(move |current, task, attempt| {
        if task.as_str() == "b" {
            assert!(prepare_signals.wait(1));
            prepare_signals.set(2);
            return Err(PreparationError {
                cause: RuntimeError::EffectFailed("healthy refusal".into()),
                claimed: None,
            });
        }
        Runtime::new(&prepare_store).prepare_dispatch(
            &current.snapshot.execution_id,
            (&current.snapshot).into(),
            task,
            attempt,
            TransitionFacts {
                now_ms: current.snapshot.updated_at_ms + 1,
            },
        )
    }));
    let capacity = TaskCapacity::new(2).unwrap();
    std::thread::scope(|scope| {
        let driven = scope.spawn(|| {
            drive_execution(
                &Runtime::new(store.clone()),
                initial,
                &host,
                &capacity,
                config(),
            )
        });
        let refused = signals.wait(2);
        let initial_faults = host.fault.load(Ordering::SeqCst);
        signals.set(4);
        let failure = driven.join().unwrap().unwrap_err();
        assert!(refused);
        assert_eq!(initial_faults, 0);
        assert!(
            host.fault.load(Ordering::SeqCst) > 0,
            "later uncertainty stayed healthy after first refusal"
        );
        assert!(matches!(failure.errors.first(), Some(DriverError::Host(_))));
        assert!(failure
            .errors
            .iter()
            .any(|error| matches!(error, DriverError::CallbackUncertain)));
        assert_eq!(failure.unsettled_invocations.len(), 1);
    });
}

#[test]
fn ordinary_completion_cas_retry_commits_the_actual_result_without_callback_replay() {
    let store = Arc::new(FaultStore::new(Fault::CasOnce));
    let initial = accepted(&store, "cas-retry", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    let done = drive_execution(
        &Runtime::new(store.clone()),
        initial,
        &host,
        &TaskCapacity::new(1).unwrap(),
        config(),
    )
    .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(done.snapshot.values["items"], json!(["a"]));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.completion_commits.load(Ordering::SeqCst), 2);
    assert_eq!(done.snapshot.attempts_used, 1);
    assert_eq!(host.fault.load(Ordering::SeqCst), 0);
}

#[test]
fn actual_concurrent_cancellation_cas_retries_retained_completion_without_local_cancel_proof() {
    let store = Arc::new(FaultStore::new(Fault::ConcurrentCancel));
    let initial = accepted(&store, "actual-cancel-cas", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    assert!(!host.cancellation_committed());
    let done = drive_execution(
        &Runtime::new(store.clone()),
        initial,
        &host,
        &TaskCapacity::new(1).unwrap(),
        config(),
    )
    .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(done.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        done.snapshot.tasks[0].result.as_ref().unwrap().output,
        json!("a")
    );
    assert!(!done.snapshot.values.contains_key("items"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.completion_commits.load(Ordering::SeqCst), 2);
    assert_eq!(done.snapshot.attempts_used, 1);
    assert_eq!(host.fault.load(Ordering::SeqCst), 0);
}

#[test]
fn completion_clock_backwards_retry_refreshes_facts_without_callback_replay() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "clock-retry", &[("a", false)]);
    let clocks = Arc::new(AtomicUsize::new(0));
    let clock_count = clocks.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut host = host(store.clone(), move |call| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    host.clock = Some(Box::new(move |current| {
        let backward = current
            .snapshot
            .tasks
            .iter()
            .any(|task| task.status == TaskStatus::InFlight)
            && clock_count.fetch_add(1, Ordering::SeqCst) == 0;
        TransitionFacts {
            now_ms: if backward {
                current.snapshot.updated_at_ms - 1
            } else {
                current.snapshot.updated_at_ms + 1
            },
        }
    }));
    let done = drive_execution(
        &Runtime::new(store),
        initial,
        &host,
        &TaskCapacity::new(1).unwrap(),
        config(),
    )
    .unwrap();
    assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(clocks.load(Ordering::SeqCst), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(done.snapshot.attempts_used, 1);
    assert_eq!(host.fault.load(Ordering::SeqCst), 0);
}

#[test]
fn completion_exhaustion_unavailability_and_corruption_retain_bounded_actual_outcome() {
    for (fault, commits) in [
        (Fault::CasAlways, 3),
        (Fault::Unavailable, 1),
        (Fault::Corrupt, 1),
    ] {
        let store = Arc::new(FaultStore::new(fault));
        let initial = accepted(&store, "retention", &[("a", false)]);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let host = host(store.clone(), move |call| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(result(call))
        });
        let failure = drive_execution(
            &Runtime::new(store.clone()),
            initial.clone(),
            &host,
            &TaskCapacity::new(1).unwrap(),
            DriverConfig {
                completion_attempts: 3,
                ..config()
            },
        )
        .unwrap_err();
        let actual = result(&failure.unsettled_invocations[0]);
        assert_actual_retained(&failure, &actual);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.completion_commits.load(Ordering::SeqCst), commits);
        assert!(host.fault.load(Ordering::SeqCst) > 0);
        assert_eq!(
            store
                .inner
                .execution(&initial.snapshot.execution_id)
                .unwrap()
                .unwrap()
                .snapshot
                .tasks[0]
                .status,
            TaskStatus::InFlight
        );
    }
}

#[test]
fn generation_turn_owner_task_and_attempt_changes_refuse_completion_retry() {
    for fault in [
        Fault::Generation,
        Fault::Turn,
        Fault::Owner,
        Fault::Task,
        Fault::Attempt,
    ] {
        let store = Arc::new(FaultStore::new(fault));
        let initial = accepted(&store, "changed-retry", &[("a", false)]);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let host = host(store.clone(), move |call| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(result(call))
        });
        let failure = drive_execution(
            &Runtime::new(store.clone()),
            initial.clone(),
            &host,
            &TaskCapacity::new(1).unwrap(),
            config(),
        )
        .unwrap_err();
        let actual = result(&failure.unsettled_invocations[0]);
        assert_actual_retained(&failure, &actual);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.completion_commits.load(Ordering::SeqCst), 1);
        assert!(host.fault.load(Ordering::SeqCst) > 0);
        assert_eq!(
            store
                .inner
                .execution(&initial.snapshot.execution_id)
                .unwrap()
                .unwrap()
                .snapshot
                .attempts_used,
            1
        );
    }
}

#[test]
fn independent_drivers_share_one_permit_before_dispatch_and_through_postcommit_handling() {
    let first = Arc::new(MemoryStore::new());
    let second = Arc::new(FaultStore::new(Fault::None));
    let initial_first = accepted(&first, "capacity-first", &[("a", false)]);
    let initial_second = accepted(&second, "capacity-second", &[("b", false)]);
    let signals = Arc::new(Signals::default());
    let callback_signals = signals.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let first_calls = calls.clone();
    let mut first_host = host(first.clone(), move |call| {
        first_calls.fetch_add(1, Ordering::SeqCst);
        callback_signals.set(1);
        if !callback_signals.wait(2) {
            return Err("callback cleanup timed out".into());
        }
        Ok(result(call))
    });
    let commit_signals = signals.clone();
    first_host.commit = Box::new(move |_| {
        commit_signals.set(4);
        assert!(commit_signals.wait(8));
    });
    let second_calls = calls.clone();
    let second_host = host(second.clone(), move |call| {
        second_calls.fetch_add(1, Ordering::SeqCst);
        Ok(result(call))
    });
    let capacity = TaskCapacity::new(1).unwrap();
    std::thread::scope(|scope| {
        let a = scope.spawn(|| {
            drive_execution(
                &Runtime::new(first.clone()),
                initial_first,
                &first_host,
                &capacity,
                config(),
            )
        });
        let first_entered = signals.wait(1);
        let b = scope.spawn(|| {
            drive_execution(
                &Runtime::new(second.clone()),
                initial_second.clone(),
                &second_host,
                &capacity,
                config(),
            )
        });
        // Two Ready reloads prove the second coordinator has passed at least
        // one capacity attempt while the first worker still owns the permit.
        let waiting = observed(|| second.ready_views.load(Ordering::SeqCst) >= 2);
        let before = second
            .execution(&initial_second.snapshot.execution_id)
            .unwrap()
            .unwrap();
        let calls_before = calls.load(Ordering::SeqCst);
        signals.set(2);
        let projection_entered = signals.wait(4);
        second.ready_views.store(0, Ordering::SeqCst);
        let still_waiting = observed(|| second.ready_views.load(Ordering::SeqCst) >= 2);
        let during_projection = second
            .execution(&initial_second.snapshot.execution_id)
            .unwrap()
            .unwrap();
        let calls_during_projection = calls.load(Ordering::SeqCst);
        signals.set(8);
        let done_a = a.join().unwrap().unwrap();
        let done_b = b.join().unwrap().unwrap();
        assert!(first_entered && waiting && projection_entered);
        assert_eq!(before.snapshot.attempts_used, 0);
        assert_eq!(before.snapshot.tasks[0].status, TaskStatus::Ready);
        assert_eq!(
            during_projection.snapshot.attempts_used, 0,
            "second driver dispatched while the first completion hook still owned its permit"
        );
        assert_eq!(calls_before, 1);
        assert_eq!(calls_during_projection, 1);
        assert!(still_waiting);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(done_a.snapshot.status, ExecutionStatus::Completed);
        assert_eq!(done_b.snapshot.status, ExecutionStatus::Completed);
    });
}

fn outcome_with(output: Value) -> EffectOutcome {
    EffectOutcome::Succeeded(TaskResult {
        output,
        writes: vec![],
    })
}
fn nested(depth: usize) -> Value {
    (0..depth).fold(Value::Null, |value, _| Value::Array(vec![value]))
}
fn limited(outcome: EffectOutcome, maximum: usize, should_complete: bool) {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "limits", &[("a", false)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let actual = outcome.clone();
    let host = host(store.clone(), move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(actual.clone())
    });
    let result = drive_execution(
        &Runtime::new(store.clone()),
        initial.clone(),
        &host,
        &TaskCapacity::new(1).unwrap(),
        DriverConfig {
            max_outcome_bytes: maximum,
            ..config()
        },
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    if should_complete {
        let done = result.unwrap();
        match outcome {
            EffectOutcome::Succeeded(actual) => {
                assert_eq!(done.snapshot.status, ExecutionStatus::Completed);
                assert_eq!(done.snapshot.tasks[0].result, Some(actual));
            }
            EffectOutcome::Failed(actual) => {
                assert_eq!(done.snapshot.status, ExecutionStatus::Failed);
                assert_eq!(done.snapshot.tasks[0].failure, Some(actual));
            }
            EffectOutcome::Cancelled => unreachable!(),
        }
        assert_eq!(host.fault.load(Ordering::SeqCst), 0);
    } else {
        let failure = result.unwrap_err();
        assert!(failure
            .errors
            .iter()
            .any(|error| matches!(error, DriverError::OutcomeLimit)));
        assert!(failure.retained_outcomes.is_empty());
        assert_eq!(failure.unsettled_invocations.len(), 1);
        assert!(host.fault.load(Ordering::SeqCst) > 0);
        let current = store
            .execution(&initial.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(current.snapshot.tasks[0].status, TaskStatus::InFlight);
        assert!(current.snapshot.tasks[0].result.is_none());
        assert!(current.snapshot.tasks[0].failure.is_none());
    }
}
#[test]
fn utf8_result_limit_counts_serialized_bytes_and_preserves_exact_boundary() {
    let outcome = outcome_with(json!("é🦀"));
    let EffectOutcome::Succeeded(result) = &outcome else {
        unreachable!()
    };
    let bytes = serde_json::to_vec(result).unwrap().len();
    assert!(bytes > serde_json::to_string(result).unwrap().chars().count());
    limited(outcome.clone(), bytes, true);
    limited(outcome, bytes - 1, false);
}
#[test]
fn depth_64_commits_but_depth_65_leaves_actual_attempt_uncertain() {
    limited(outcome_with(nested(64)), 65_536, true);
    limited(outcome_with(nested(65)), 65_536, false);
}
#[test]
fn write_payload_depth_limits_cover_replace_and_each_append_item() {
    for append in [false, true] {
        for depth in [64, 65] {
            let write = if append {
                StateWrite::Append {
                    field: "items".into(),
                    items: vec![nested(depth)],
                }
            } else {
                StateWrite::Replace {
                    field: "items".into(),
                    value: nested(depth),
                }
            };
            limited(
                EffectOutcome::Succeeded(TaskResult {
                    output: Value::Null,
                    writes: vec![write],
                }),
                65_536,
                depth == 64,
            );
        }
    }
}
#[test]
fn oversized_result_and_failure_do_not_become_false_application_completion() {
    limited(outcome_with(json!("x".repeat(65_536))), 65_536, false);
    limited(
        EffectOutcome::Failed("x".repeat(MAX_TASK_FAILURE_BYTES)),
        65_536,
        true,
    );
    limited(
        EffectOutcome::Failed("x".repeat(MAX_TASK_FAILURE_BYTES + 1)),
        65_536,
        false,
    );
    limited(
        EffectOutcome::Failed("é".repeat(MAX_TASK_FAILURE_BYTES / 2 + 1)),
        65_536,
        false,
    );
}
