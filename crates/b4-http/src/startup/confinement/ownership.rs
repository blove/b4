//! Independently fenced disposal of the two trusted coordinator arguments.

use crate::{
    lifecycle::{dispose_panic, OwnedShared},
    startup::{CompletionCandidate, StartupPreparationLimits},
    supervisor::StartupCapability,
    RecoveryConfinementAuthority, StartupPrincipalResolver,
};
use b4_runtime::approval::ApprovalStore;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// Constructed only after registered ownership. The outer StartupFuture keeps
/// its carrier and independent same-group anchor alive through this guard,
/// including never-polled future Drop and first/replacement opaque payloads.
pub(super) struct Arguments {
    resolver: Option<Arc<dyn StartupPrincipalResolver>>,
    authority: Option<Arc<dyn RecoveryConfinementAuthority>>,
    fault: Arc<AtomicBool>,
}
impl Arguments {
    pub(super) fn new<S>(
        carrier: &OwnedShared<S>,
        resolver: &Arc<dyn StartupPrincipalResolver>,
        authority: &Arc<dyn RecoveryConfinementAuthority>,
    ) -> Self {
        Self {
            fault: carrier.fault.clone(),
            resolver: Some(resolver.clone()),
            authority: Some(authority.clone()),
        }
    }
    pub(super) fn resolver(&self) -> &Arc<dyn StartupPrincipalResolver> {
        self.resolver
            .as_ref()
            .expect("startup_confinement_resolver_retired")
    }
    pub(super) fn authority(&self) -> &Arc<dyn RecoveryConfinementAuthority> {
        self.authority
            .as_ref()
            .expect("startup_confinement_authority_retired")
    }
    fn dispose<T>(&self, value: T) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            self.fault.store(true, Ordering::Release);
            dispose_panic(payload);
        }
    }
    fn retire(&mut self) {
        // Never unwind one custom destructor through the other in a tuple.
        let resolver = self.resolver.take();
        self.dispose(resolver);
        let authority = self.authority.take();
        self.dispose(authority);
    }
}
impl Drop for Arguments {
    fn drop(&mut self) {
        self.retire();
    }
}

pub(super) async fn prepare<S: ApprovalStore + 'static>(
    shared: OwnedShared<S>,
    mut arguments: Arguments,
    limits: StartupPreparationLimits,
    capability: StartupCapability,
    counters: Arc<super::model::Counters>,
) -> CompletionCandidate {
    // The guard outlives every joined Job and final original-view cleanup.
    // Returned progress is published before either trusted capture retires.
    let candidate =
        super::coordinator::prepare(shared, &arguments, limits, capability, counters).await;
    arguments.retire();
    candidate
}
