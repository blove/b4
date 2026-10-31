//! Explicit known-ID prior-owner confinement for trusted embedding applications.

mod model;
pub use b4_runtime::{
    RecoveryConfinementBackendError, RecoveryConfinementOutcome, RecoveryConfinementReason,
    RecoveryConfinementReceipt, RecoveryConfinementTarget,
};
pub use model::{
    RecoveryConfinementAuthority, RecoveryConfinementAuthorityRefusal, RecoveryConfinementError,
    RecoveryConfinementRefusal,
};

use crate::{
    claim::{ClaimError, ThreadClaim},
    lifecycle::OwnedShared,
    HttpHost, StartupPrincipalResolver,
};
use b4_runtime::Store;
use std::{
    future::Future,
    sync::{atomic::Ordering, Arc},
};

impl<S: Store + 'static> HttpHost<S> {
    /// Request confinement of one exact target with separate trusted authority.
    /// The borrowed handles remain the caller's external ownership. A returned
    /// receipt will certify a durable mutation, not readiness or retirement.
    #[allow(clippy::manual_async_fn)] // Keep the explicit borrowed + Send future contract.
    pub fn confine_prior_owner<'a>(
        &'a self,
        target: RecoveryConfinementTarget,
        resolver: &'a Arc<dyn StartupPrincipalResolver>,
        authority: &'a Arc<dyn RecoveryConfinementAuthority>,
    ) -> impl Future<Output = Result<RecoveryConfinementOutcome, RecoveryConfinementError>> + Send + 'a
    {
        async move {
            // An unpolled future holds only pure target and borrowed aliases.
            tokio::runtime::Handle::try_current().map_err(|_| {
                RecoveryConfinementError::new(RecoveryConfinementRefusal::ExecutorUnavailable, None)
            })?;
            if self.shared.fault.load(Ordering::Acquire) {
                return Err(RecoveryConfinementError::new(
                    RecoveryConfinementRefusal::Unavailable,
                    None,
                ));
            }
            let refuse = |reason| {
                RecoveryConfinementError::new(
                    if self.shared.fault.load(Ordering::Acquire) {
                        RecoveryConfinementRefusal::Unavailable
                    } else {
                        reason
                    },
                    None,
                )
            };
            let carrier = OwnedShared::register(self.shared.clone())
                .ok_or_else(|| refuse(RecoveryConfinementRefusal::AdmissionClosed))?;
            let permit = carrier
                .capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| refuse(RecoveryConfinementRefusal::CapacityExhausted))?;
            let claim = ThreadClaim::acquire(carrier.claims.clone(), target.thread_id.clone())
                .map_err(|error| match error {
                    ClaimError::Busy => refuse(RecoveryConfinementRefusal::ThreadBusy),
                    ClaimError::Poisoned => {
                        carrier.fault.store(true, Ordering::Release);
                        RecoveryConfinementError::new(RecoveryConfinementRefusal::Unavailable, None)
                    }
                })?;
            let supervisor = carrier.supervisor.clone();
            // No suspension or trusted-handle clone precedes owner, permit and
            // same-thread claim. The Job owns every framework clone from here.
            let task = operation::spawn_core(
                carrier,
                target,
                resolver.clone(),
                authority.clone(),
                permit,
                claim,
            )?;
            operation::finish_join(&supervisor, task.await)
        }
    }
}
mod operation;
pub(crate) mod startup;
#[cfg(test)]
#[allow(clippy::duplicate_mod)] // Preserve protected helpers and their private visibility.
mod test_support;
#[cfg(test)]
mod tests;
