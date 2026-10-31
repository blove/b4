use crate::{HttpHost, Principal, ShutdownHandle};
use axum::Router;
use b4_runtime::{approval::ApprovalStore, ThreadOrigin};
use std::sync::Arc;

mod confinement;
pub(crate) mod coordinator;
mod model;
pub use confinement::{
    StartupConfinementPreparationHandle, StartupConfinementPreparationOutcome,
    StartupConfinementPreparationStatus, StartupConfinementProgress, StartupConfinementWaitOutcome,
};
pub use model::*;

/// A trusted resolver of immutable origin into current server-owned authority.
/// Stored labels alone do not authenticate a principal.
pub trait StartupPrincipalResolver: Send + Sync {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError>;
}

/// An opt-in native host awaiting bounded startup preparation. It exposes no
/// store accessor and no ordinary host until actual preparation completes.
pub struct PreparingHttpHost<S> {
    host: HttpHost<S>,
}
impl<S: ApprovalStore + 'static> PreparingHttpHost<S> {
    pub(crate) fn new(host: HttpHost<S>) -> Self {
        Self { host }
    }
    pub fn router(&self) -> Router {
        self.host.router()
    }
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.host.shutdown_handle()
    }
    pub fn preparation_status(&self) -> StartupPreparationStatus {
        self.host.shared.supervisor.preparation_status()
    }
    /// Synchronously count one attempt before returning an observation handle.
    /// Dropping the caller or handle never aborts the owned coordinator.
    pub fn begin_preparation(
        &self,
        resolver: Arc<dyn StartupPrincipalResolver>,
        limits: StartupPreparationLimits,
    ) -> Result<StartupPreparationHandle, StartupStartError> {
        if !(1..=64).contains(&limits.max_pages) || !(1..=256).contains(&limits.max_recoveries) {
            return Err(StartupStartError::InvalidLimits);
        }
        let executor = tokio::runtime::Handle::try_current()
            .map_err(|_| StartupStartError::ExecutorUnavailable)?;
        let supervisor = self.host.shared.supervisor.clone();
        let (capability, owner) = supervisor.begin_startup()?;
        let carrier =
            crate::lifecycle::OwnedShared::from_registered(self.host.shared.clone(), owner);
        let inner = coordinator::prepare(carrier.clone(), resolver, limits, capability.clone());
        let owned = crate::lifecycle::StartupFuture::new(inner, carrier, capability);
        // Tokio sees only unit; the wrapper consumes every arbitrary inner
        // capture/payload while its same-group ownership anchor survives.
        drop(executor.spawn(owned));
        Ok(StartupPreparationHandle::new(supervisor))
    }
    pub fn serving_host(&self) -> Option<HttpHost<S>> {
        (self.preparation_status().phase == StartupPreparationPhase::Complete
            && self.host.shared.supervisor.is_serving())
        .then(|| self.host.clone())
    }
}
