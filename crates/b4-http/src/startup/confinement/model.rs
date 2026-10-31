use crate::{
    RecoveryConfinementError, RecoveryConfinementOutcome, StartupPreparationHandle,
    StartupPreparationOutcome, StartupPreparationPhase, StartupPreparationStatus, StartupWaitError,
    StartupWaitOutcome,
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Monotonic lower bounds for separately confirmed confinement evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StartupConfinementProgress {
    pub confirmed_blocks: usize,
    pub already_blocked: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupConfinementPreparationStatus {
    pub preparation: StartupPreparationStatus,
    pub confinement: StartupConfinementProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupConfinementPreparationOutcome {
    pub preparation: StartupPreparationOutcome,
    pub confinement: StartupConfinementProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupConfinementWaitOutcome {
    Finished(StartupConfinementPreparationOutcome),
    Pending(StartupConfinementPreparationStatus),
}

/// Only the two bounded pure integers are held by this private mutex. The
/// writer publishes only verified returned confinement evidence.
#[derive(Debug, Default)]
pub(super) struct Counters(Mutex<StartupConfinementProgress>);
impl Counters {
    pub(super) fn record(
        &self,
        result: &Result<RecoveryConfinementOutcome, RecoveryConfinementError>,
        candidates: usize,
        maximum: usize,
    ) -> Result<(), ()> {
        let mut current = self.0.lock().map_err(|_| ())?;
        let (confirmed, already) = match result {
            Ok(RecoveryConfinementOutcome::Confined(_)) => (1, 0),
            Ok(RecoveryConfinementOutcome::AlreadyBlocked) => (0, 1),
            Err(error) if error.confirmed_receipt().is_some() => (1, 0),
            Err(_) => (0, 0),
        };
        let confirmed_blocks = current.confirmed_blocks.checked_add(confirmed).ok_or(())?;
        let already_blocked = current.already_blocked.checked_add(already).ok_or(())?;
        let sum = confirmed_blocks.checked_add(already_blocked).ok_or(())?;
        if sum > candidates || sum > maximum.min(256) {
            return Err(());
        }
        *current = StartupConfinementProgress {
            confirmed_blocks,
            already_blocked,
        };
        Ok(())
    }
    fn read(&self) -> StartupConfinementProgress {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Resource-free observation. No Store, application, callback, capability,
/// claim, permit or owner token can be retained by this handle or its waits.
#[derive(Debug, Clone)]
pub struct StartupConfinementPreparationHandle {
    preparation: StartupPreparationHandle,
    confinement: Arc<Counters>,
}
impl StartupConfinementPreparationHandle {
    pub(super) fn new(preparation: StartupPreparationHandle, confinement: Arc<Counters>) -> Self {
        Self {
            preparation,
            confinement,
        }
    }
    fn sample(
        preparation: &StartupPreparationHandle,
        confinement: &Counters,
    ) -> StartupConfinementPreparationStatus {
        // Sample extra progress first so it cannot get ahead of the sampled
        // candidate count. A finished supervisor snapshot requires the stable
        // final tuple, published before that finalization.
        let mut counters = confinement.read();
        let preparation = preparation.status();
        if matches!(
            preparation.phase,
            StartupPreparationPhase::Complete | StartupPreparationPhase::Incomplete
        ) {
            counters = confinement.read();
        }
        StartupConfinementPreparationStatus {
            preparation,
            confinement: counters,
        }
    }
    fn finished(
        status: StartupConfinementPreparationStatus,
    ) -> Option<StartupConfinementPreparationOutcome> {
        let preparation = match status.preparation.phase {
            StartupPreparationPhase::Complete => StartupPreparationOutcome::Complete {
                progress: status.preparation.progress,
            },
            StartupPreparationPhase::Incomplete => StartupPreparationOutcome::Incomplete {
                progress: status.preparation.progress,
                reason: status
                    .preparation
                    .incomplete_reason
                    .expect("startup_final_reason"),
            },
            StartupPreparationPhase::NotStarted | StartupPreparationPhase::Preparing => {
                return None
            }
        };
        Some(StartupConfinementPreparationOutcome {
            preparation,
            confinement: status.confinement,
        })
    }
    pub fn status(&self) -> StartupConfinementPreparationStatus {
        Self::sample(&self.preparation, &self.confinement)
    }
    pub fn wait(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<StartupConfinementWaitOutcome, StartupWaitError>> + Send + 'static
    {
        let preparation = self.preparation.clone();
        let waiting = preparation.wait(timeout);
        let confinement = self.confinement.clone();
        async move {
            Ok(match waiting.await? {
                StartupWaitOutcome::Finished(preparation) => {
                    // The old wait's final snapshot precedes this final tuple.
                    StartupConfinementWaitOutcome::Finished(StartupConfinementPreparationOutcome {
                        preparation,
                        confinement: confinement.read(),
                    })
                }
                StartupWaitOutcome::Pending(_) => {
                    // The old Pending snapshot may already be stale. Obtain a
                    // fresh ordered pair and retain Finished if it won the race.
                    let current = Self::sample(&preparation, &confinement);
                    match Self::finished(current) {
                        Some(outcome) => StartupConfinementWaitOutcome::Finished(outcome),
                        None => StartupConfinementWaitOutcome::Pending(current),
                    }
                }
            })
        }
    }
}
