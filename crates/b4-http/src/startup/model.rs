use std::{future::Future, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupResolutionError {
    Unavailable,
    Unauthorized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupPreparationLimits {
    pub max_pages: usize,
    pub max_recoveries: usize,
}
impl Default for StartupPreparationLimits {
    fn default() -> Self {
        Self {
            max_pages: 32,
            max_recoveries: 64,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StartupProgress {
    pub pages: usize,
    pub scanned_rows: usize,
    pub candidates: usize,
    pub admitted: usize,
    pub runnable: usize,
    pub parked: usize,
    pub blocked: usize,
    pub terminal: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupPreparationPhase {
    NotStarted,
    Preparing,
    Complete,
    Incomplete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupIncompleteReason {
    Shutdown,
    LimitExceeded,
    DiscoveryRefused,
    CurrentOwnerExecution,
    ResolutionRefused,
    RecoveryRefused,
    AttemptLimit,
    Faulted,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupPreparationStatus {
    pub phase: StartupPreparationPhase,
    pub progress: StartupProgress,
    pub incomplete_reason: Option<StartupIncompleteReason>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupPreparationOutcome {
    Complete {
        progress: StartupProgress,
    },
    Incomplete {
        progress: StartupProgress,
        reason: StartupIncompleteReason,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupWaitOutcome {
    Finished(StartupPreparationOutcome),
    Pending(StartupPreparationStatus),
}

/// Only this value-only result may leave the private coordinator future.
#[derive(Clone, Copy)]
pub(crate) struct CompletionCandidate {
    pub progress: StartupProgress,
    pub reason: Option<StartupIncompleteReason>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupStartError {
    InvalidLimits,
    ExecutorUnavailable,
    AlreadyStarted,
    Shutdown,
    Faulted,
}
impl std::fmt::Display for StartupStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "invalid_startup_preparation_limits",
            Self::ExecutorUnavailable => "startup_executor_unavailable",
            Self::AlreadyStarted => "startup_preparation_already_started",
            Self::Shutdown => "startup_host_draining",
            Self::Faulted => "startup_preparation_unavailable",
        })
    }
}
impl std::error::Error for StartupStartError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupWaitError {
    InvalidTimeout,
}
impl std::fmt::Display for StartupWaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid_startup_preparation_timeout")
    }
}
impl std::error::Error for StartupWaitError {}

/// A resource-free observation value. Dropping a wait or observer is never
/// callback cancellation, activation evidence or physical-retirement evidence.
#[derive(Debug, Clone)]
pub struct StartupPreparationHandle {
    supervisor: crate::ShutdownHandle,
}
impl StartupPreparationHandle {
    pub(crate) fn new(supervisor: crate::ShutdownHandle) -> Self {
        Self { supervisor }
    }
    pub fn status(&self) -> StartupPreparationStatus {
        self.supervisor.preparation_status()
    }
    pub fn wait(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<StartupWaitOutcome, StartupWaitError>> + Send + 'static {
        self.supervisor.wait_preparation(timeout)
    }
}
