//! Internal startup-role adapter. Ordinary Preparing admission stays closed.

use super::{
    operation::{self, StartupSelection},
    RecoveryConfinementAuthority, RecoveryConfinementError, RecoveryConfinementOutcome,
    RecoveryConfinementRefusal, RecoveryConfinementTarget,
};
use crate::{
    claim::{ClaimError, ThreadClaim},
    lifecycle::OwnedShared,
    supervisor::StartupCapability,
    Principal, StartupPrincipalResolver,
};
use b4_runtime::{approval::ApprovalStore, ThreadView};
use std::sync::{atomic::Ordering, Arc};

type PureResult = Result<RecoveryConfinementOutcome, RecoveryConfinementError>;

pub(crate) struct StartupCapture {
    pub(crate) target: RecoveryConfinementTarget,
    pub(crate) original: Arc<ThreadView>,
    pub(crate) principal: Principal,
}

#[allow(clippy::result_large_err)] // Keep the accepted pure confinement error/receipt unboxed.
pub(crate) async fn confine<S: ApprovalStore + 'static>(
    coordinator: &OwnedShared<S>,
    capability: &StartupCapability,
    capture: StartupCapture,
    resolver: &Arc<dyn StartupPrincipalResolver>,
    authority: &Arc<dyn RecoveryConfinementAuthority>,
) -> PureResult {
    tokio::runtime::Handle::try_current().map_err(|_| {
        RecoveryConfinementError::new(RecoveryConfinementRefusal::ExecutorUnavailable, None)
    })?;
    let refuse = |reason| {
        RecoveryConfinementError::new(
            if coordinator.fault.load(Ordering::Acquire) {
                RecoveryConfinementRefusal::Unavailable
            } else {
                reason
            },
            None,
        )
    };
    if coordinator.fault.load(Ordering::Acquire) {
        return Err(refuse(RecoveryConfinementRefusal::Unavailable));
    }
    let carrier = OwnedShared::register_startup(coordinator.shared_arc(), capability)
        .ok_or_else(|| refuse(RecoveryConfinementRefusal::AdmissionClosed))?;
    let permit = carrier
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| refuse(RecoveryConfinementRefusal::CapacityExhausted))?;
    let claim = ThreadClaim::acquire(carrier.claims.clone(), capture.target.thread_id.clone())
        .map_err(|error| match error {
            ClaimError::Busy => refuse(RecoveryConfinementRefusal::ThreadBusy),
            ClaimError::Poisoned => {
                carrier.fault.store(true, Ordering::Release);
                RecoveryConfinementError::new(RecoveryConfinementRefusal::Unavailable, None)
            }
        })?;
    let supervisor = carrier.supervisor.clone();
    // All counted registration and nonwaiting guards precede these clones.
    let task = operation::spawn_startup_core(
        carrier,
        capture.target,
        StartupSelection {
            original: capture.original,
            principal: capture.principal,
        },
        resolver.clone(),
        authority.clone(),
        permit,
        claim,
    )?;
    // Joining includes real Job cleanup and removal of its pure view alias.
    operation::finish_join(&supervisor, task.await)
}
