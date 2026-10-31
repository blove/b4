use super::startup_test_support::*;
use super::*;
use crate::startup::{
    CompletionCandidate, StartupIncompleteReason, StartupPreparationPhase, StartupProgress,
};
use b4_sqlite::SqliteStore;
use std::time::Duration;

struct HeldPayload {
    _store: Arc<SqliteStore>,
    gate: Arc<Gate>,
}
impl Drop for HeldPayload {
    fn drop(&mut self) {
        self.gate.hold();
    }
}
struct ReplacementPayload {
    store: Arc<SqliteStore>,
    first: Arc<Gate>,
    second: Arc<Gate>,
}
impl Drop for ReplacementPayload {
    fn drop(&mut self) {
        self.first.hold();
        std::panic::panic_any(HeldPayload {
            _store: self.store.clone(),
            gate: self.second.clone(),
        });
    }
}
struct PanicFuture {
    payload: Option<Box<dyn Any + Send>>,
}
impl Future for PanicFuture {
    type Output = CompletionCandidate;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<CompletionCandidate> {
        resume_unwind(self.payload.take().unwrap())
    }
}
struct ReadyDropPanicFuture {
    payload: Option<Box<dyn Any + Send>>,
}
impl Future for ReadyDropPanicFuture {
    type Output = CompletionCandidate;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<CompletionCandidate> {
        Poll::Ready(CompletionCandidate {
            progress: StartupProgress::default(),
            reason: None,
        })
    }
}
impl Drop for ReadyDropPanicFuture {
    fn drop(&mut self) {
        resume_unwind(self.payload.take().unwrap());
    }
}
struct CaptureFuture {
    _store: Arc<SqliteStore>,
    gate: Arc<Gate>,
    ready: bool,
    polled: Arc<tokio::sync::Notify>,
}
impl Future for CaptureFuture {
    type Output = CompletionCandidate;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<CompletionCandidate> {
        self.polled.notify_one();
        if self.ready {
            Poll::Ready(CompletionCandidate {
                progress: StartupProgress::default(),
                reason: None,
            })
        } else {
            Poll::Pending
        }
    }
}
impl Drop for CaptureFuture {
    fn drop(&mut self) {
        self.gate.hold();
    }
}
async fn consume(task: tokio::task::JoinHandle<()>) {
    match task.await {
        Ok(()) => (),
        Err(error) if error.is_panic() => {
            let payload = error.into_panic();
            // Failure cleanup only: the deliberate old carrier can leave its
            // opaque payload in JoinError. This is never positive ownership.
            tokio::task::spawn_blocking(move || dispose_panic(payload))
                .await
                .unwrap();
        }
        Err(error) => assert!(error.is_cancelled()),
    }
}
fn failure_observed(f: &Fixture) -> bool {
    let status = f.supervisor.preparation_status();
    let held = f.held_resources();
    f.supervisor.status().faulted
        && f.supervisor.status().active_owners > 0
        && status.phase == StartupPreparationPhase::Incomplete
        && status.incomplete_reason == Some(StartupIncompleteReason::Faulted)
        && held
}
async fn joined(task: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_polled_opaque_payload_remains_counted_until_real_disposal() {
    let mut f = Fixture::new();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let future = PanicFuture {
        payload: Some(Box::new(HeldPayload {
            _store: f.store.upgrade().unwrap(),
            gate: gate.clone(),
        })),
    };
    let task = tokio::spawn(StartupFuture::new(
        future,
        f.carrier.take().unwrap(),
        f.capability.clone(),
    ));
    let watcher = tokio::spawn(consume(task));
    gate.entered().await;
    let observed = failure_observed(&f);
    gate.release();
    joined(watcher).await;
    let retired = f.retired().await;
    assert!(gate.positive());
    assert!(
        observed,
        "opaque payload escaped its faulted counted carrier"
    );
    assert_eq!(retired, crate::ShutdownOutcome::Quiescent { faulted: true });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_ready_capture_cleanup_precedes_serving_activation() {
    let mut f = Fixture::new();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let future = CaptureFuture {
        _store: f.store.upgrade().unwrap(),
        gate: gate.clone(),
        ready: true,
        polled: Arc::new(tokio::sync::Notify::new()),
    };
    let watcher = tokio::spawn(consume(tokio::spawn(StartupFuture::new(
        future,
        f.carrier.take().unwrap(),
        f.capability.clone(),
    ))));
    gate.entered().await;
    let before = f.supervisor.preparation_status();
    let held = f.held_resources();
    let observed = before.phase == StartupPreparationPhase::Preparing
        && !f.supervisor.is_serving()
        && f.supervisor.status().active_owners > 0
        && held;
    gate.release();
    joined(watcher).await;
    let after = f.supervisor.preparation_status();
    let retired = f.retired().await;
    assert!(gate.positive());
    assert!(
        observed,
        "serving activated before actual future capture cleanup"
    );
    assert_eq!(after.phase, StartupPreparationPhase::Complete);
    assert_eq!(
        retired,
        crate::ShutdownOutcome::Quiescent { faulted: false }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_cancelled_pending_capture_faults_before_counted_cleanup() {
    let mut f = Fixture::new();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let polled = Arc::new(tokio::sync::Notify::new());
    let future = CaptureFuture {
        _store: f.store.upgrade().unwrap(),
        gate: gate.clone(),
        ready: false,
        polled: polled.clone(),
    };
    let task = tokio::spawn(StartupFuture::new(
        future,
        f.carrier.take().unwrap(),
        f.capability.clone(),
    ));
    let abort = task.abort_handle();
    let watcher = tokio::spawn(consume(task));
    tokio::time::timeout(Duration::from_secs(5), polled.notified())
        .await
        .unwrap();
    abort.abort();
    gate.entered().await;
    let observed = failure_observed(&f);
    gate.release();
    joined(watcher).await;
    let retired = f.retired().await;
    assert!(gate.positive());
    assert!(
        observed,
        "cancelled future cleanup lacked faulted counted ownership"
    );
    assert_eq!(retired, crate::ShutdownOutcome::Quiescent { faulted: true });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_replacement_panic_payload_disposal_keeps_same_owner_group() {
    let mut f = Fixture::new();
    let first = Gate::new();
    let second = Gate::new();
    let _first_cleanup = Cleanup(first.clone());
    let _second_cleanup = Cleanup(second.clone());
    let future = PanicFuture {
        payload: Some(Box::new(ReplacementPayload {
            store: f.store.upgrade().unwrap(),
            first: first.clone(),
            second: second.clone(),
        })),
    };
    let watcher = tokio::spawn(consume(tokio::spawn(StartupFuture::new(
        future,
        f.carrier.take().unwrap(),
        f.capability.clone(),
    ))));
    first.entered().await;
    let first_owned = failure_observed(&f);
    first.release();
    second.entered().await;
    let second_owned = failure_observed(&f);
    second.release();
    joined(watcher).await;
    let retired = f.retired().await;
    assert!(first.positive() && second.positive());
    assert!(
        first_owned && second_owned,
        "replacement opaque payload escaped its original owner group"
    );
    assert_eq!(retired, crate::ShutdownOutcome::Quiescent { faulted: true });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_ready_future_drop_panic_is_faulted_and_counted_before_payload_cleanup() {
    let mut f = Fixture::new();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let future = ReadyDropPanicFuture {
        payload: Some(Box::new(HeldPayload {
            _store: f.store.upgrade().unwrap(),
            gate: gate.clone(),
        })),
    };
    let watcher = tokio::spawn(consume(tokio::spawn(StartupFuture::new(
        future,
        f.carrier.take().unwrap(),
        f.capability.clone(),
    ))));
    gate.entered().await;
    let observed = failure_observed(&f) && f.supervisor.status().active_owners == 1;
    gate.release();
    joined(watcher).await;
    let retired = f.retired().await;
    assert!(gate.positive());
    assert!(
        observed,
        "Ready future Drop panic escaped its single counted group"
    );
    assert_eq!(retired, crate::ShutdownOutcome::Quiescent { faulted: true });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_last_carrier_shared_drop_panic_prevents_activation_until_owned_cleanup() {
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let mut f = Fixture::with_carrier_panic(Some(gate.clone()));
    let future = async {
        CompletionCandidate {
            progress: StartupProgress::default(),
            reason: None,
        }
    };
    let watcher = tokio::spawn(consume(tokio::spawn(StartupFuture::new(
        future,
        f.carrier.take().unwrap(),
        f.capability.clone(),
    ))));
    gate.entered().await;
    let held = f.held_resources();
    let status = f.supervisor.status();
    let observed =
        status.faulted && status.active_owners == 1 && !f.supervisor.is_serving() && held;
    gate.release();
    joined(watcher).await;
    let finished = f.supervisor.preparation_status();
    let retired = f.retired().await;
    assert!(gate.positive());
    assert!(
        observed,
        "carrier Shared panic lost its anchor or opened serving"
    );
    assert_eq!(finished.phase, StartupPreparationPhase::Incomplete);
    assert_eq!(
        finished.incomplete_reason,
        Some(StartupIncompleteReason::Faulted)
    );
    assert_eq!(retired, crate::ShutdownOutcome::Quiescent { faulted: true });
}
