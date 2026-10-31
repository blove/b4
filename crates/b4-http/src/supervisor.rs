//! Admission closure and physical quiescence of registered framework jobs.
use crate::startup::{
    CompletionCandidate, StartupIncompleteReason, StartupPreparationPhase,
    StartupPreparationStatus, StartupProgress, StartupStartError,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};
use tokio::sync::Notify;

pub const DEFAULT_SHUTDOWN_WAIT: Duration = Duration::from_secs(30);
pub const MAX_SHUTDOWN_WAIT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownPhase {
    Serving,
    Draining,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownStatus {
    pub phase: ShutdownPhase,
    pub active_owners: usize,
    pub faulted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    Quiescent { faulted: bool },
    Incomplete { active_owners: usize, faulted: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownWaitError {
    InvalidTimeout,
}
impl std::fmt::Display for ShutdownWaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid_shutdown_timeout")
    }
}
impl std::error::Error for ShutdownWaitError {}

struct Gate {
    phase: ShutdownPhase,
    active_owners: usize,
    preparation: Option<Preparation>,
}
enum Preparation {
    NotStarted,
    Running {
        attempt: Arc<()>,
        progress: StartupProgress,
    },
    Finished(StartupPreparationStatus),
}
struct SupervisorState {
    gate: Mutex<Gate>,
    fault: Arc<AtomicBool>,
    changed: Notify,
}

#[derive(Clone)]
pub struct ShutdownHandle {
    state: Arc<SupervisorState>,
}

struct OwnerGroup {
    state: Arc<SupervisorState>,
    role: OwnerRole,
}
enum OwnerRole {
    Ordinary,
    Startup(Arc<()>),
}
#[derive(Clone)]
pub(crate) struct OwnerToken {
    group: Arc<OwnerGroup>,
}
pub(crate) struct MutationLease {
    _owner: OwnerToken,
}

#[derive(Clone)]
pub(crate) struct StartupCapability {
    state: Arc<SupervisorState>,
    attempt: Arc<()>,
}

impl SupervisorState {
    fn lock(&self) -> MutexGuard<'_, Gate> {
        self.gate.lock().unwrap_or_else(|poisoned| {
            self.fault.store(true, Ordering::Release);
            poisoned.into_inner()
        })
    }
    fn snapshot(&self, gate: &Gate) -> ShutdownStatus {
        ShutdownStatus {
            phase: gate.phase,
            active_owners: gate.active_owners,
            faulted: self.fault.load(Ordering::Acquire),
        }
    }
    fn preparation(gate: &Gate) -> StartupPreparationStatus {
        match &gate.preparation {
            Some(Preparation::Running { progress, .. }) => StartupPreparationStatus {
                phase: StartupPreparationPhase::Preparing,
                progress: *progress,
                incomplete_reason: None,
            },
            Some(Preparation::Finished(status)) => *status,
            _ => StartupPreparationStatus {
                phase: StartupPreparationPhase::NotStarted,
                progress: StartupProgress::default(),
                incomplete_reason: None,
            },
        }
    }
    fn ordinary_open(gate: &Gate) -> bool {
        gate.phase == ShutdownPhase::Serving
            && matches!(
                &gate.preparation,
                None | Some(Preparation::Finished(StartupPreparationStatus {
                    phase: StartupPreparationPhase::Complete,
                    ..
                }))
            )
    }
}
impl std::fmt::Debug for ShutdownHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ShutdownHandle")
            .field(&self.status())
            .finish()
    }
}
impl ShutdownHandle {
    pub(crate) fn new(fault: Arc<AtomicBool>) -> Self {
        Self {
            state: Arc::new(SupervisorState {
                gate: Mutex::new(Gate {
                    phase: ShutdownPhase::Serving,
                    active_owners: 0,
                    preparation: None,
                }),
                fault,
                changed: Notify::new(),
            }),
        }
    }
    pub(crate) fn new_preparing(fault: Arc<AtomicBool>) -> Self {
        let handle = Self::new(fault);
        handle.state.lock().preparation = Some(Preparation::NotStarted);
        handle
    }
    pub(crate) fn preparation_status(&self) -> StartupPreparationStatus {
        SupervisorState::preparation(&self.state.lock())
    }
    pub(crate) fn wait_preparation(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<crate::StartupWaitOutcome, crate::StartupWaitError>> + Send + 'static
    {
        let owned = self.clone();
        async move {
            if timeout > MAX_SHUTDOWN_WAIT {
                return Err(crate::StartupWaitError::InvalidTimeout);
            }
            let current = owned.preparation_status();
            if let Some(outcome) = Self::preparation_outcome(current) {
                return Ok(crate::StartupWaitOutcome::Finished(outcome));
            }
            if timeout.is_zero() {
                return Ok(crate::StartupWaitOutcome::Pending(current));
            }
            let wait = async {
                loop {
                    let notified = owned.state.changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let current = owned.preparation_status();
                    if let Some(outcome) = Self::preparation_outcome(current) {
                        return outcome;
                    }
                    notified.await;
                }
            };
            Ok(match tokio::time::timeout(timeout, wait).await {
                Ok(outcome) => crate::StartupWaitOutcome::Finished(outcome),
                Err(_) => {
                    let status = owned.preparation_status();
                    match Self::preparation_outcome(status) {
                        Some(outcome) => crate::StartupWaitOutcome::Finished(outcome),
                        None => crate::StartupWaitOutcome::Pending(status),
                    }
                }
            })
        }
    }
    fn preparation_outcome(
        status: StartupPreparationStatus,
    ) -> Option<crate::StartupPreparationOutcome> {
        match status.phase {
            StartupPreparationPhase::Complete => Some(crate::StartupPreparationOutcome::Complete {
                progress: status.progress,
            }),
            StartupPreparationPhase::Incomplete => {
                Some(crate::StartupPreparationOutcome::Incomplete {
                    progress: status.progress,
                    reason: status.incomplete_reason.expect("startup_final_reason"),
                })
            }
            StartupPreparationPhase::NotStarted | StartupPreparationPhase::Preparing => None,
        }
    }
    pub(crate) fn begin_startup(
        &self,
    ) -> Result<(StartupCapability, OwnerToken), StartupStartError> {
        let mut gate = self.state.lock();
        if gate.phase != ShutdownPhase::Serving {
            return Err(StartupStartError::Shutdown);
        }
        if self.state.fault.load(Ordering::Acquire) {
            return Err(StartupStartError::Faulted);
        }
        if !matches!(gate.preparation, Some(Preparation::NotStarted)) {
            return Err(StartupStartError::AlreadyStarted);
        }
        let attempt = Arc::new(());
        let owner = self
            .register_locked(&mut gate, OwnerRole::Startup(attempt.clone()))
            .ok_or(StartupStartError::Faulted)?;
        gate.preparation = Some(Preparation::Running {
            attempt: attempt.clone(),
            progress: StartupProgress::default(),
        });
        Ok((
            StartupCapability {
                state: self.state.clone(),
                attempt,
            },
            owner,
        ))
    }
    pub(crate) fn register_startup_owner(
        &self,
        capability: &StartupCapability,
    ) -> Option<OwnerToken> {
        let mut gate = self.state.lock();
        if gate.phase != ShutdownPhase::Serving
            || self.state.fault.load(Ordering::Acquire)
            || !self.matches_startup(&gate, capability)
        {
            return None;
        }
        self.register_locked(&mut gate, OwnerRole::Startup(capability.attempt.clone()))
    }
    fn matches_startup(&self, gate: &Gate, capability: &StartupCapability) -> bool {
        Arc::ptr_eq(&self.state, &capability.state)
            && matches!(&gate.preparation,
            Some(Preparation::Running { attempt, .. }) if Arc::ptr_eq(attempt, &capability.attempt))
    }
    pub(crate) fn update_startup_progress(
        &self,
        capability: &StartupCapability,
        progress: StartupProgress,
    ) {
        let mut gate = self.state.lock();
        if self.matches_startup(&gate, capability) {
            if let Some(Preparation::Running {
                progress: current, ..
            }) = &mut gate.preparation
            {
                *current = progress;
            }
        }
    }
    pub(crate) fn finish_startup(
        &self,
        capability: &StartupCapability,
        candidate: CompletionCandidate,
    ) -> bool {
        let mut gate = self.state.lock();
        if !self.matches_startup(&gate, capability) {
            return false;
        }
        let reason = if self.state.fault.load(Ordering::Acquire) {
            Some(StartupIncompleteReason::Faulted)
        } else if gate.phase != ShutdownPhase::Serving {
            Some(StartupIncompleteReason::Shutdown)
        } else {
            candidate.reason
        };
        let complete = reason.is_none();
        gate.preparation = Some(Preparation::Finished(StartupPreparationStatus {
            phase: if complete {
                StartupPreparationPhase::Complete
            } else {
                StartupPreparationPhase::Incomplete
            },
            progress: candidate.progress,
            incomplete_reason: reason,
        }));
        drop(gate);
        self.state.changed.notify_waiters();
        complete
    }
    /// Observe registered owner groups. Serving with zero owners is only idle;
    /// the monotonic admission cut is necessary for a stable quiescence proof.
    pub fn status(&self) -> ShutdownStatus {
        self.state.snapshot(&self.state.lock())
    }
    /// Close admission synchronously. Existing registered jobs keep ownership;
    /// only operations whose separate mutation lease won the cut may mutate.
    pub fn begin_shutdown(&self) -> ShutdownStatus {
        let mut gate = self.state.lock();
        gate.phase = ShutdownPhase::Draining;
        if matches!(gate.preparation, Some(Preparation::NotStarted)) {
            gate.preparation = Some(Preparation::Finished(StartupPreparationStatus {
                phase: StartupPreparationPhase::Incomplete,
                progress: StartupProgress::default(),
                incomplete_reason: Some(StartupIncompleteReason::Shutdown),
            }));
        }
        self.state.snapshot(&gate)
    }
    /// A valid call closes admission before returning the owned future, even
    /// when it is never polled. Invalid durations do not change admission. Timer
    /// construction occurs on polling, so creating this future needs no executor.
    /// Dropping or timing out the future never releases a surviving owner.
    pub fn wait_quiescent(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<ShutdownOutcome, ShutdownWaitError>> + Send + 'static {
        let valid = timeout <= MAX_SHUTDOWN_WAIT;
        if valid {
            self.begin_shutdown();
        }
        let owned = self.clone();
        async move {
            if !valid {
                return Err(ShutdownWaitError::InvalidTimeout);
            }
            let current = owned.status();
            if current.active_owners == 0 || timeout.is_zero() {
                return Ok(Self::outcome(current));
            }
            let wait = async {
                loop {
                    // Register before the observation: the last owner can drop
                    // between the count check and awaiting this notification.
                    let notified = owned.state.changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let current = owned.status();
                    if current.active_owners == 0 {
                        return current;
                    }
                    notified.await;
                }
            };
            let status = match tokio::time::timeout(timeout, wait).await {
                Ok(status) => status,
                Err(_) => owned.status(),
            };
            Ok(Self::outcome(status))
        }
    }
    fn outcome(status: ShutdownStatus) -> ShutdownOutcome {
        if status.active_owners == 0 {
            ShutdownOutcome::Quiescent {
                faulted: status.faulted,
            }
        } else {
            ShutdownOutcome::Incomplete {
                active_owners: status.active_owners,
                faulted: status.faulted,
            }
        }
    }
    pub(crate) fn is_serving(&self) -> bool {
        SupervisorState::ordinary_open(&self.state.lock())
    }
    pub(crate) fn register_owner(&self) -> Option<OwnerToken> {
        let mut gate = self.state.lock();
        if !SupervisorState::ordinary_open(&gate) {
            return None;
        }
        self.register_locked(&mut gate, OwnerRole::Ordinary)
    }
    fn register_locked(&self, gate: &mut Gate, role: OwnerRole) -> Option<OwnerToken> {
        let Some(count) = gate.active_owners.checked_add(1) else {
            self.state.fault.store(true, Ordering::Release);
            return None;
        };
        gate.active_owners = count;
        Some(OwnerToken {
            group: Arc::new(OwnerGroup {
                state: self.state.clone(),
                role,
            }),
        })
    }
}

#[cfg(test)]
#[path = "startup/gate_tests.rs"]
mod startup_gate_tests;
impl OwnerToken {
    /// Lifetime registration alone does not authorize a mutation after the cut.
    /// A winning lease retains its group through service-internal policy retries.
    pub(crate) fn try_admit(&self) -> Option<MutationLease> {
        let gate = self.group.state.lock();
        let open = match &self.group.role {
            OwnerRole::Ordinary => SupervisorState::ordinary_open(&gate),
            OwnerRole::Startup(attempt) => {
                gate.phase == ShutdownPhase::Serving
                    && !self.group.state.fault.load(Ordering::Acquire)
                    && matches!(&gate.preparation, Some(Preparation::Running { attempt: current, .. })
                    if Arc::ptr_eq(attempt, current))
            }
        };
        open.then(|| MutationLease {
            _owner: self.clone(),
        })
    }
}
impl Drop for OwnerGroup {
    fn drop(&mut self) {
        let mut gate = self.state.lock();
        match gate.active_owners.checked_sub(1) {
            Some(count) => gate.active_owners = count,
            None => {
                self.state.fault.store(true, Ordering::Release);
            }
        }
        let quiescent = gate.active_owners == 0;
        drop(gate);
        if quiescent {
            self.state.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn handle() -> ShutdownHandle {
        ShutdownHandle::new(Arc::new(AtomicBool::new(false)))
    }

    #[test]
    fn shutdown_cut_is_idempotent_and_refuses_owner_registration() {
        let h = handle();
        assert!(h.is_serving());
        assert_eq!(h.begin_shutdown().phase, ShutdownPhase::Draining);
        assert_eq!(h.begin_shutdown(), h.status());
        assert!(h.register_owner().is_none());
    }
    #[test]
    fn cloned_owner_group_retires_only_after_its_last_clone() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        let second = owner.clone();
        assert_eq!(h.status().active_owners, 1);
        drop(owner);
        assert_eq!(h.status().active_owners, 1);
        drop(second);
        assert_eq!(h.status().active_owners, 0);
    }
    #[test]
    fn earlier_request_cannot_obtain_mutation_lease_after_cut() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        h.begin_shutdown();
        assert!(owner.try_admit().is_none());
        assert_eq!(h.status().active_owners, 1);
    }
    #[test]
    fn pre_cut_mutation_lease_retains_group_through_draining() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        let lease = owner.try_admit().unwrap();
        drop(owner);
        h.begin_shutdown();
        assert_eq!(h.status().active_owners, 1);
        drop(lease);
        assert_eq!(h.status().active_owners, 0);
    }
    #[test]
    fn constructing_unpolled_wait_closes_admission_outside_tokio() {
        let h = handle();
        let future = h.wait_quiescent(DEFAULT_SHUTDOWN_WAIT);
        fn send_static<T: Send + 'static>(_: &T) {}
        send_static(&future);
        assert_eq!(h.status().phase, ShutdownPhase::Draining);
        assert!(h.register_owner().is_none());
        drop(future);
        assert_eq!(h.status().phase, ShutdownPhase::Draining);
    }
    #[test]
    fn invalid_wait_creation_does_not_change_serving_or_owners() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        let invalid = h.wait_quiescent(MAX_SHUTDOWN_WAIT + Duration::from_nanos(1));
        assert_eq!(h.status().phase, ShutdownPhase::Serving);
        assert_eq!(h.status().active_owners, 1);
        drop(invalid);
        drop(owner);
    }
    #[test]
    fn maximum_wait_creation_is_valid_without_creating_timer() {
        let h = handle();
        let future = h.wait_quiescent(MAX_SHUTDOWN_WAIT);
        assert_eq!(h.status().phase, ShutdownPhase::Draining);
        drop(future);
    }
    #[tokio::test]
    async fn invalid_timeout_result_is_static_and_has_no_admission_cut() {
        let h = handle();
        assert_eq!(
            h.wait_quiescent(MAX_SHUTDOWN_WAIT + Duration::from_nanos(1))
                .await,
            Err(ShutdownWaitError::InvalidTimeout)
        );
        assert!(h.is_serving());
    }
    #[tokio::test]
    async fn zero_wait_is_incomplete_until_final_owner_returns() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        assert_eq!(
            h.wait_quiescent(Duration::ZERO).await,
            Ok(ShutdownOutcome::Incomplete {
                active_owners: 1,
                faulted: false
            })
        );
        drop(owner);
        assert_eq!(
            h.wait_quiescent(Duration::ZERO).await,
            Ok(ShutdownOutcome::Quiescent { faulted: false })
        );
    }
    #[tokio::test]
    async fn timed_out_wait_preserves_real_owner_and_closed_admission() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        assert_eq!(
            h.wait_quiescent(Duration::from_millis(10)).await,
            Ok(ShutdownOutcome::Incomplete {
                active_owners: 1,
                faulted: false
            })
        );
        assert!(owner.try_admit().is_none());
        assert_eq!(h.status().active_owners, 1);
        drop(owner);
        assert_eq!(
            h.wait_quiescent(Duration::ZERO).await,
            Ok(ShutdownOutcome::Quiescent { faulted: false })
        );
    }
    #[tokio::test]
    async fn dropped_wait_does_not_release_owners_or_reopen_admission() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        let future = h.wait_quiescent(DEFAULT_SHUTDOWN_WAIT);
        drop(future);
        assert_eq!(h.status().phase, ShutdownPhase::Draining);
        assert_eq!(h.status().active_owners, 1);
        drop(owner);
    }
    #[tokio::test]
    async fn faulted_physical_quiescence_is_reported_separately() {
        let fault = Arc::new(AtomicBool::new(false));
        let h = ShutdownHandle::new(fault.clone());
        let owner = h.register_owner().unwrap();
        fault.store(true, Ordering::Release);
        assert_eq!(
            h.wait_quiescent(Duration::ZERO).await,
            Ok(ShutdownOutcome::Incomplete {
                active_owners: 1,
                faulted: true
            })
        );
        drop(owner);
        assert_eq!(
            h.wait_quiescent(Duration::ZERO).await,
            Ok(ShutdownOutcome::Quiescent { faulted: true })
        );
    }
    #[tokio::test]
    async fn multiple_waiters_observe_last_owner_without_lost_notification() {
        let h = handle();
        let owner = h.register_owner().unwrap();
        let final_owner = owner.clone();
        let first = tokio::spawn(h.wait_quiescent(Duration::from_secs(1)));
        let second = tokio::spawn(h.wait_quiescent(Duration::from_secs(1)));
        tokio::task::yield_now().await;
        drop(owner);
        assert_eq!(h.status().active_owners, 1);
        drop(final_owner);
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            (first.await.unwrap(), second.await.unwrap())
        })
        .await
        .unwrap();
        let expected = Ok(ShutdownOutcome::Quiescent { faulted: false });
        assert_eq!(result, (expected, expected));
    }
    #[test]
    fn cut_and_mutation_lease_race_has_one_linearized_admission() {
        for _ in 0..32 {
            let h = handle();
            let owner = h.register_owner().unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let other = barrier.clone();
            let shutdown = h.clone();
            let (lease, closed) = std::thread::scope(|scope| {
                let close = scope.spawn(move || {
                    other.wait();
                    shutdown.begin_shutdown()
                });
                barrier.wait();
                let lease = owner.try_admit();
                (lease, close.join().unwrap())
            });
            assert_eq!(closed.phase, ShutdownPhase::Draining);
            assert!(owner.try_admit().is_none());
            assert!(h.register_owner().is_none());
            drop(owner);
            assert_eq!(h.status().active_owners, usize::from(lease.is_some()));
            drop(lease);
            assert_eq!(h.status().active_owners, 0);
        }
    }
}
