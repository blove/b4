//! Borrowed opt-in preparation with separately confirmed confinement progress.

use super::{
    PreparingHttpHost, StartupPreparationHandle, StartupPreparationLimits,
    StartupPrincipalResolver, StartupStartError,
};
use crate::{lifecycle::OwnedShared, RecoveryConfinementAuthority};
use b4_runtime::approval::ApprovalStore;
use std::{future::Future, sync::Arc};

mod coordinator;
mod model;
mod ownership;
pub use model::{
    StartupConfinementPreparationHandle, StartupConfinementPreparationOutcome,
    StartupConfinementPreparationStatus, StartupConfinementProgress, StartupConfinementWaitOutcome,
};

impl<S: ApprovalStore + 'static> PreparingHttpHost<S> {
    /// Start the separate opt-in mode. Unpolled futures retain only borrowed
    /// handles; dropping an observer never cancels the supervised coordinator.
    #[allow(clippy::manual_async_fn)] // Keep the explicit borrowed + Send future contract.
    pub fn begin_preparation_with_confinement<'a>(
        &'a self,
        resolver: &'a Arc<dyn StartupPrincipalResolver>,
        authority: &'a Arc<dyn RecoveryConfinementAuthority>,
        limits: StartupPreparationLimits,
    ) -> impl Future<Output = Result<StartupConfinementPreparationHandle, StartupStartError>> + Send + 'a
    {
        async move {
            if !(1..=64).contains(&limits.max_pages) || !(1..=256).contains(&limits.max_recoveries)
            {
                return Err(StartupStartError::InvalidLimits);
            }
            let executor = tokio::runtime::Handle::try_current()
                .map_err(|_| StartupStartError::ExecutorUnavailable)?;
            let supervisor = self.host.shared.supervisor.clone();
            let (capability, owner) = supervisor.begin_startup()?;
            let carrier = OwnedShared::from_registered(self.host.shared.clone(), owner);
            // Registered ownership precedes both trusted-handle clones. The
            // guard is captured even when the supervised inner is never polled.
            let arguments = ownership::Arguments::new(&carrier, resolver, authority);
            let counters = Arc::new(model::Counters::default());
            let inner = ownership::prepare(
                carrier.clone(),
                arguments,
                limits,
                capability.clone(),
                counters.clone(),
            );
            let owned = crate::lifecycle::StartupFuture::new(inner, carrier, capability);
            drop(executor.spawn(owned));
            Ok(StartupConfinementPreparationHandle::new(
                StartupPreparationHandle::new(supervisor),
                counters,
            ))
        }
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
