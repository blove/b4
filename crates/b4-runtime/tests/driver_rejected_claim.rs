use b4_contract::*;
use b4_runtime::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
#[path = "driver/support.rs"]
mod support;
use support::*;

#[derive(Default)]
struct CallbackGate {
    state: Mutex<(bool, bool)>,
    wake: Condvar,
}
impl CallbackGate {
    fn entered(&self) -> bool {
        let state = self.state.lock().unwrap();
        let (state, _) = self
            .wake
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.0)
            .unwrap();
        state.0
    }
    fn enter_and_wait(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.wake.notify_all();
        let (state, _) = self
            .wake
            .wait_timeout_while(state, Duration::from_secs(10), |state| !state.1)
            .unwrap();
        state.1
    }
    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.wake.notify_all();
    }
}

#[test]
fn rejected_preparation_claim_cannot_cancel_an_already_entered_sibling() {
    let store = Arc::new(MemoryStore::new());
    let initial = accepted(&store, "rejected-sibling", &[("a", false), ("b", false)]);
    let gate = Arc::new(CallbackGate::default());
    let callback_gate = gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let mut host = host(store.clone(), move |call| {
        callback_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(call.task_id.as_str(), "a");
        if !callback_gate.enter_and_wait() {
            return Err("callback cleanup timed out".into());
        }
        Ok(result(call))
    });
    // The host supplies positive cancellation proof. That proof must never let
    // mismatched preparation evidence acknowledge an already-entered sibling.
    host.cancel.store(true, Ordering::SeqCst);
    let claimed = Arc::new(Mutex::new(None::<PreparedEffect>));
    let prepare_claim = claimed.clone();
    let prepare_store = store.clone();
    let prepare_gate = gate.clone();
    host.prepare = Some(Box::new(move |current, task_id, attempt_id| {
        if task_id.as_str() == "a" {
            let prepared = Runtime::new(&prepare_store).prepare_dispatch(
                &current.snapshot.execution_id,
                (&current.snapshot).into(),
                task_id,
                attempt_id,
                TransitionFacts {
                    now_ms: current.snapshot.updated_at_ms + 1,
                },
            )?;
            *prepare_claim.lock().unwrap() = Some(prepared.clone());
            return Ok(prepared);
        }
        assert!(prepare_gate.entered());
        let view = prepare_store
            .thread_view(&current.thread_id)
            .unwrap()
            .unwrap();
        prepare_store
            .cancel_run_fenced(
                &RunCancellationTarget::from_view(&view).unwrap(),
                TransitionFacts {
                    now_ms: view.execution.as_ref().unwrap().snapshot.updated_at_ms + 1,
                },
            )
            .unwrap();
        // This deliberately faulty port returns a's real claim while preparing
        // b. The coordinator must reject it before any CancelAttempt repair.
        Err(PreparationError {
            cause: RuntimeError::Store(StoreError::RevisionConflict),
            claimed: prepare_claim.lock().unwrap().clone().map(Box::new),
        })
    }));
    let capacity = TaskCapacity::new(2).unwrap();
    std::thread::scope(|scope| {
        let driven = scope.spawn(|| {
            drive_execution(
                &Runtime::new(store.clone()),
                initial.clone(),
                &host,
                &capacity,
                DriverConfig::default(),
            )
        });
        let callback_entered = gate.entered();
        let deadline = Instant::now() + Duration::from_secs(5);
        while host.fault.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let fault_before_quiescence = host.fault.load(Ordering::SeqCst) > 0;
        // Release before asserting or joining, including when observation failed.
        gate.release();
        let failure = driven.join().unwrap().unwrap_err();
        assert!(callback_entered && fault_before_quiescence);
        assert!(failure
            .errors
            .iter()
            .any(|error| matches!(error, DriverError::InvalidPreparedEffect)));
        assert!(failure.errors.iter().any(|error| matches!(
            error,
            DriverError::Host(RuntimeError::Store(StoreError::RevisionConflict))
        )));
        let current = store
            .execution(&initial.snapshot.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            current.snapshot.tasks[0].status,
            TaskStatus::Succeeded,
            "rejected claim evidence cancelled an entered callback before its actual result"
        );
        assert_eq!(
            current.snapshot.tasks[0].result.as_ref().unwrap().output,
            json!("a")
        );
        assert_eq!(current.snapshot.tasks[1].status, TaskStatus::Cancelled);
        assert_eq!(current.snapshot.status, ExecutionStatus::Cancelled);
        assert_eq!(current.snapshot.attempts_used, 1);
        assert_eq!(current.snapshot.attempt_history.len(), 1);
        assert!(!current.snapshot.values.contains_key("items"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(failure.retained_outcomes.is_empty());
        assert!(failure.unsettled_invocations.is_empty());
    });
}
