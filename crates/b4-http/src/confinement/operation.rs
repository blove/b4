//! Counted known-ID confinement with pure confirmed result handling.
//! Framework argument clones begin only after registered ownership and guards.

#![allow(clippy::result_large_err)] // The accepted pure receipt/error API stays unboxed.

use super::{
    RecoveryConfinementAuthority, RecoveryConfinementBackendError, RecoveryConfinementError,
    RecoveryConfinementOutcome, RecoveryConfinementReason, RecoveryConfinementReceipt,
    RecoveryConfinementRefusal, RecoveryConfinementTarget,
};
use crate::{
    claim::ThreadClaim,
    lifecycle::{dispose_panic, OwnedShared},
    supervisor::{MutationLease, OwnerToken},
    Operation, Principal, ResourceContext, RouteProvenance, ShutdownHandle,
    StartupPrincipalResolver,
};
use b4_contract::{ExecutionStatus, TransitionFacts, TurnId};
use b4_runtime::{Store, ThreadView};
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{sync::OwnedSemaphorePermit, task::JoinHandle};

type PureResult = Result<RecoveryConfinementOutcome, RecoveryConfinementError>;

/// Pure selection retained only until this counted Job has actually returned.
/// The coordinator retains the other view alias; no full bootstrap clone occurs.
pub(super) struct StartupSelection {
    pub(super) original: Arc<ThreadView>,
    pub(super) principal: Principal,
}

/// Optional ownership permits idempotent normal and never-started cleanup.
/// The same-group anchor stays through each resource-bearing destructor and
/// any first/replacement panic payload; guards stay through final Shared Drop.
pub(super) struct Job<S> {
    carrier: Option<OwnedShared<S>>,
    anchor: Option<OwnerToken>,
    resolver: Option<Arc<dyn StartupPrincipalResolver>>,
    authority: Option<Arc<dyn RecoveryConfinementAuthority>>,
    lease: Option<MutationLease>,
    claim: Option<ThreadClaim>,
    permit: Option<OwnedSemaphorePermit>,
    target: RecoveryConfinementTarget,
    fault: Arc<AtomicBool>,
    confirmed_receipt: Option<RecoveryConfinementReceipt>,
    startup: Option<StartupSelection>,
    logic_finished: bool,
}
impl<S> Job<S> {
    pub(super) fn new(
        carrier: OwnedShared<S>,
        target: RecoveryConfinementTarget,
        resolver: Arc<dyn StartupPrincipalResolver>,
        authority: Arc<dyn RecoveryConfinementAuthority>,
        permit: OwnedSemaphorePermit,
        claim: ThreadClaim,
    ) -> Self
    where
        S: Send + Sync + 'static,
    {
        Self {
            anchor: Some(carrier.anchor()),
            fault: carrier.fault.clone(),
            carrier: Some(carrier),
            resolver: Some(resolver),
            authority: Some(authority),
            lease: None,
            claim: Some(claim),
            permit: Some(permit),
            target,
            confirmed_receipt: None,
            startup: None,
            logic_finished: false,
        }
    }
    fn mark_fault(&self) {
        self.fault.store(true, Ordering::Release);
    }
    fn dispose<T>(&self, value: T) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            self.mark_fault();
            dispose_panic(payload);
        }
    }
    fn retire_resources(&mut self) {
        // Each Arc is taken and dropped under a separate catch. A destructor
        // never unwinds through another custom destructor in a tuple.
        let resolver = self.resolver.take();
        self.dispose(resolver);
        let authority = self.authority.take();
        self.dispose(authority);
        // Existing OwnedShared catches last Shared Drop and opaque payloads.
        // Claim, permit, lease and this independent anchor are still present.
        let carrier = self.carrier.take();
        self.dispose(carrier);
        let claim = self.claim.take();
        self.dispose(claim);
        let permit = self.permit.take();
        self.dispose(permit);
        let lease = self.lease.take();
        self.dispose(lease);
    }
    fn retire(&mut self) {
        self.retire_resources();
        // Remaining data is pure. OwnerToken destruction is the protected
        // nonapplication supervisor primitive, after all opaque disposal.
        drop(self.anchor.take());
    }
    fn fault_result(&self) -> PureResult {
        Err(RecoveryConfinementError::new(
            RecoveryConfinementRefusal::Unavailable,
            self.confirmed_receipt.clone(),
        ))
    }
}
impl<S: Store + 'static> Job<S> {
    fn backend_error(&self, error: RecoveryConfinementBackendError) -> RecoveryConfinementError {
        if matches!(
            error,
            RecoveryConfinementBackendError::CorruptRecord
                | RecoveryConfinementBackendError::Unavailable
        ) {
            self.mark_fault();
            RecoveryConfinementError::new(RecoveryConfinementRefusal::Unavailable, None)
        } else {
            RecoveryConfinementError::new(RecoveryConfinementRefusal::Backend(error), None)
        }
    }
    fn refusal(reason: RecoveryConfinementRefusal) -> RecoveryConfinementError {
        RecoveryConfinementError::new(reason, None)
    }
    fn validate_capture(
        &self,
        view: &ThreadView,
        current_owner: u64,
    ) -> Result<(), RecoveryConfinementError> {
        let captured = RecoveryConfinementTarget::from_view(view)
            .map_err(|error| self.backend_error(error))?;
        if captured != self.target {
            return Err(self.backend_error(RecoveryConfinementBackendError::TargetChanged));
        }
        if current_owner == 0 {
            return Err(self.backend_error(RecoveryConfinementBackendError::Unavailable));
        }
        if self.target.execution_fence.owner_generation == 0 {
            return Err(self.backend_error(RecoveryConfinementBackendError::CorruptRecord));
        }
        if self.target.execution_fence.owner_generation >= current_owner {
            return Err(self.backend_error(RecoveryConfinementBackendError::OwnerConflict));
        }
        let carrier = self.carrier.as_ref().expect("confinement_carrier_retired");
        let application = carrier
            .application
            .as_ref()
            .ok_or_else(|| Self::refusal(RecoveryConfinementRefusal::UnsupportedHost))?;
        let origin = view
            .thread
            .origin
            .as_ref()
            .ok_or_else(|| Self::refusal(RecoveryConfinementRefusal::ResourceUnavailable))?;
        if origin.application != application.identity().application
            || [&origin.application, &origin.tenant, &origin.principal]
                .into_iter()
                .any(|label| {
                    label.is_empty() || label.len() > 256 || label.chars().any(char::is_control)
                })
        {
            return Err(Self::refusal(
                RecoveryConfinementRefusal::ResourceUnavailable,
            ));
        }
        let status = view
            .execution
            .as_ref()
            .expect("validated_active_execution")
            .snapshot
            .status;
        if matches!(
            status,
            ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
        ) {
            return Err(self.backend_error(RecoveryConfinementBackendError::Terminal));
        }
        Ok(())
    }
    fn observe(&self, original: &ThreadView) -> Option<RecoveryConfinementReason> {
        let carrier = self.carrier.as_ref().expect("confinement_carrier_retired");
        let application = carrier.application.as_ref().expect("validated_native_host");
        let selected = &original
            .execution
            .as_ref()
            .expect("validated_active_execution")
            .snapshot
            .definition;
        if application.definition_exact(selected).is_none() {
            return Some(RecoveryConfinementReason::ExactDefinitionUnavailable);
        }
        let origin = original
            .thread
            .origin
            .as_ref()
            .expect("validated_original_origin");
        let principal = match self
            .resolver
            .as_ref()
            .expect("confinement_resolver_retired")
            .resolve(origin)
        {
            Ok(principal)
                if crate::transport::valid_principal(&principal)
                    && principal.application == origin.application
                    && principal.tenant == origin.tenant
                    && principal.principal == origin.principal =>
            {
                principal
            }
            _ => return Some(RecoveryConfinementReason::RequiredAuthorityUnavailable),
        };
        let provenance = || RouteProvenance {
            route: selected.route.clone(),
            revision: selected.revision.clone(),
        };
        let context = ResourceContext {
            operation: Operation::Recover,
            thread_id: Some(original.thread.id.clone()),
            origin: Some(origin.clone()),
            stored_route: Some(provenance()),
            requested_route: Some(provenance()),
        };
        if carrier.policy.authorize(&principal, &context) {
            None
        } else {
            Some(RecoveryConfinementReason::DispatchNotAuthorized)
        }
    }
    fn observe_selected(
        &self,
        original: &ThreadView,
    ) -> Result<Option<RecoveryConfinementReason>, RecoveryConfinementError> {
        let Some(selection) = &self.startup else {
            return Ok(self.observe(original));
        };
        let carrier = self.carrier.as_ref().expect("confinement_carrier_retired");
        let application = carrier.application.as_ref().expect("validated_native_host");
        let origin = original
            .thread
            .origin
            .as_ref()
            .expect("validated_original_origin");
        // Fresh resolution precedes BOTH missing full Definition and policy.
        // This mode never turns missing/different dispatch authority into a
        // RequiredAuthorityUnavailable permission to confine.
        let principal = self
            .resolver
            .as_ref()
            .expect("confinement_resolver_retired")
            .resolve(origin)
            .map_err(|_| Self::refusal(RecoveryConfinementRefusal::ResourceUnavailable))?;
        if !crate::transport::valid_principal(&principal)
            || !crate::application::scope_allows(carrier, &principal)
            || principal.application != origin.application
            || principal.tenant != origin.tenant
            || principal.principal != origin.principal
            || principal != selection.principal
        {
            return Err(Self::refusal(
                RecoveryConfinementRefusal::ResourceUnavailable,
            ));
        }
        let selected = &original
            .execution
            .as_ref()
            .expect("validated_active_execution")
            .snapshot
            .definition;
        if application.definition_exact(selected).is_none() {
            return Ok(Some(RecoveryConfinementReason::ExactDefinitionUnavailable));
        }
        let provenance = || RouteProvenance {
            route: selected.route.clone(),
            revision: selected.revision.clone(),
        };
        let context = ResourceContext {
            operation: Operation::Recover,
            thread_id: Some(original.thread.id.clone()),
            origin: Some(origin.clone()),
            stored_route: Some(provenance()),
            requested_route: Some(provenance()),
        };
        Ok((!carrier.policy.authorize(&principal, &context))
            .then_some(RecoveryConfinementReason::DispatchNotAuthorized))
    }
    fn validate_startup_original(
        &self,
        captured: &ThreadView,
    ) -> Result<(), RecoveryConfinementError> {
        let Some(selection) = &self.startup else {
            return Ok(());
        };
        if selection.original.as_ref() != captured {
            return Err(Self::refusal(
                RecoveryConfinementRefusal::ObservationChanged,
            ));
        }
        let origin = captured
            .thread
            .origin
            .as_ref()
            .expect("validated_original_origin");
        let carrier = self.carrier.as_ref().expect("confinement_carrier_retired");
        if !crate::transport::valid_principal(&selection.principal)
            || !crate::application::scope_allows(carrier, &selection.principal)
            || selection.principal.application != origin.application
            || selection.principal.tenant != origin.tenant
            || selection.principal.principal != origin.principal
        {
            return Err(Self::refusal(
                RecoveryConfinementRefusal::ResourceUnavailable,
            ));
        }
        Ok(())
    }
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementError> {
        self.authority
            .as_ref()
            .expect("confinement_authority_retired")
            .authorize(original, reason)
            .map_err(|error| {
                Self::refusal(match error {
                    super::RecoveryConfinementAuthorityRefusal::Denied => {
                        RecoveryConfinementRefusal::AuthorityDenied
                    }
                    super::RecoveryConfinementAuthorityRefusal::Unavailable => {
                        RecoveryConfinementRefusal::AuthorityUnavailable
                    }
                })
            })
    }
    fn core(&mut self) -> PureResult {
        if self.fault.load(Ordering::Acquire) {
            return self.fault_result();
        }
        if self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .application
            .is_none()
        {
            return Err(Self::refusal(RecoveryConfinementRefusal::UnsupportedHost));
        }
        let current_owner = self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .store
            .owner_generation();
        let original = self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .store
            .confinement_view_fenced(&self.target)
            .map_err(|error| self.backend_error(error))?;
        self.validate_capture(&original, current_owner)?;
        self.validate_startup_original(&original)?;
        if original
            .execution
            .as_ref()
            .expect("validated_active_execution")
            .snapshot
            .status
            == ExecutionStatus::RecoveryBlocked
        {
            // The bounded backend capture has audited this exact prior target.
            // Idempotence does not renew authority, adopt or overwrite a reason.
            return Ok(RecoveryConfinementOutcome::AlreadyBlocked);
        }
        let reason = self
            .observe_selected(&original)?
            .ok_or_else(|| Self::refusal(RecoveryConfinementRefusal::NotApplicable))?;
        self.authorize(&original, reason)?;
        if self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .permission_uses
            .is_some()
        {
            return Err(Self::refusal(RecoveryConfinementRefusal::UnsupportedHost));
        }

        if self.fault.load(Ordering::Acquire) {
            return self.fault_result();
        }
        self.lease = Some(
            self.carrier
                .as_ref()
                .expect("confinement_carrier_retired")
                .try_admit()
                .ok_or_else(|| Self::refusal(RecoveryConfinementRefusal::AdmissionClosed))?,
        );
        let renewed = self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .store
            .confinement_view_fenced(&self.target)
            .map_err(|error| self.backend_error(error))?;
        if renewed != original {
            return Err(Self::refusal(
                RecoveryConfinementRefusal::ObservationChanged,
            ));
        }
        let renewed_owner = self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .store
            .owner_generation();
        if renewed_owner != current_owner {
            return Err(self.backend_error(RecoveryConfinementBackendError::Unavailable));
        }
        self.validate_capture(&renewed, current_owner)?;
        self.validate_startup_original(&renewed)?;
        if self.observe_selected(&original)? != Some(reason) {
            return Err(Self::refusal(
                RecoveryConfinementRefusal::ObservationChanged,
            ));
        }
        self.authorize(&original, reason)?;
        if self.fault.load(Ordering::Acquire) {
            return self.fault_result();
        }
        if self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .store
            .owner_generation()
            != current_owner
        {
            return Err(self.backend_error(RecoveryConfinementBackendError::Unavailable));
        }
        let fresh_turn = TurnId::new(uuid::Uuid::new_v4().to_string())
            .map_err(|_| self.backend_error(RecoveryConfinementBackendError::Unavailable))?;
        let outcome = self
            .carrier
            .as_ref()
            .expect("confinement_carrier_retired")
            .store
            .confine_recovery_fenced(
                &original,
                &self.target,
                reason,
                &fresh_turn,
                TransitionFacts {
                    now_ms: crate::transport::now_ms(),
                },
            )
            .map_err(|error| self.backend_error(error))?;
        match outcome {
            RecoveryConfinementOutcome::Confined(receipt)
                if valid_receipt(
                    &receipt,
                    &original,
                    &self.target,
                    current_owner,
                    reason,
                    &fresh_turn,
                ) =>
            {
                // Save only verified returned durable evidence OUTSIDE the
                // logic catch, before any later cloned-handle/Shared teardown.
                self.confirmed_receipt = Some(receipt.clone());
                Ok(RecoveryConfinementOutcome::Confined(receipt))
            }
            _ => Err(self.backend_error(RecoveryConfinementBackendError::CorruptRecord)),
        }
    }
    fn run(mut self) -> PureResult {
        let result = match catch_unwind(AssertUnwindSafe(|| {
            let result = self.core();
            self.logic_finished = true;
            self.retire_resources();
            result
        })) {
            Ok(result) => result,
            Err(payload) => {
                self.mark_fault();
                // The independent anchor is outside the complete logic and
                // resource-teardown catch, so opaque infrastructure payloads
                // cannot escape counted ownership even on unexpected unwind.
                dispose_panic(payload);
                self.fault_result()
            }
        };
        self.logic_finished = true;
        // Idempotent cleanup of any options left by an unexpected unwind.
        self.retire();
        if self.fault.load(Ordering::Acquire) {
            self.fault_result()
        } else {
            result
        }
    }
}
impl<S> Drop for Job<S> {
    fn drop(&mut self) {
        if !self.logic_finished {
            // Direct never-started disposal or unexpected unwind is a fault,
            // observed before potentially held user-capture/payload cleanup.
            self.mark_fault();
        }
        self.retire();
    }
}

pub(super) fn spawn_core<S: Store + 'static>(
    carrier: OwnedShared<S>,
    target: RecoveryConfinementTarget,
    resolver: Arc<dyn StartupPrincipalResolver>,
    authority: Arc<dyn RecoveryConfinementAuthority>,
    permit: OwnedSemaphorePermit,
    claim: ThreadClaim,
) -> Result<JoinHandle<PureResult>, RecoveryConfinementError> {
    let job = Job::new(carrier, target, resolver, authority, permit, claim);
    submit(job)
}

pub(super) fn spawn_startup_core<S: Store + 'static>(
    carrier: OwnedShared<S>,
    target: RecoveryConfinementTarget,
    selection: StartupSelection,
    resolver: Arc<dyn StartupPrincipalResolver>,
    authority: Arc<dyn RecoveryConfinementAuthority>,
    permit: OwnedSemaphorePermit,
    claim: ThreadClaim,
) -> Result<JoinHandle<PureResult>, RecoveryConfinementError> {
    let mut job = Job::new(carrier, target, resolver, authority, permit, claim);
    job.startup = Some(selection);
    submit(job)
}

fn submit<S: Store + 'static>(
    job: Job<S>,
) -> Result<JoinHandle<PureResult>, RecoveryConfinementError> {
    let outside_anchor = job.anchor.clone();
    let fault = job.fault.clone();
    let result = catch_unwind(AssertUnwindSafe(|| {
        tokio::task::spawn_blocking(move || job.run())
    }));
    let result = match result {
        Ok(handle) => Ok(handle),
        Err(payload) => {
            fault.store(true, Ordering::Release);
            // Job RAII cleanup may already have run during submission unwind;
            // this outside same-group anchor still counts the caught payload.
            dispose_panic(payload);
            Err(RecoveryConfinementError::new(
                RecoveryConfinementRefusal::Unavailable,
                None,
            ))
        }
    };
    drop(outside_anchor);
    result
}

pub(super) fn finish_join(
    supervisor: &ShutdownHandle,
    joined: Result<PureResult, tokio::task::JoinError>,
) -> PureResult {
    let result = match joined {
        Ok(result) => result,
        Err(_) => {
            // All application/Store-bearing panics are consumed in the Job.
            // A cancelled queued Job's Drop has already marked its fault.
            return Err(RecoveryConfinementError::new(
                RecoveryConfinementRefusal::Unavailable,
                None,
            ));
        }
    };
    if supervisor.status().faulted {
        let receipt = match &result {
            Ok(RecoveryConfinementOutcome::Confined(receipt)) => Some(receipt.clone()),
            Ok(RecoveryConfinementOutcome::AlreadyBlocked) => None,
            Err(error) => error.confirmed_receipt().cloned(),
        };
        Err(RecoveryConfinementError::new(
            RecoveryConfinementRefusal::Unavailable,
            receipt,
        ))
    } else {
        result
    }
}

/// Validate every pure field without cloning or treating a partial match as
/// commit evidence. The captured current owner may be more than oldowner+1.
fn valid_receipt(
    receipt: &RecoveryConfinementReceipt,
    original: &ThreadView,
    target: &RecoveryConfinementTarget,
    current_owner: u64,
    reason: RecoveryConfinementReason,
    fresh_turn: &TurnId,
) -> bool {
    let Some(execution) = &original.execution else {
        return false;
    };
    let Some(next_execution_revision) = target.execution_fence.revision.checked_add(1) else {
        return false;
    };
    let Some(next_resource_revision) = target.resource_revision.checked_add(1) else {
        return false;
    };
    receipt.thread_id == target.thread_id
        && receipt.thread_generation == target.thread_generation
        && receipt.execution_id == target.execution_id
        && receipt.original_fence == target.execution_fence
        && receipt.new_fence.revision == next_execution_revision
        && receipt.new_fence.owner_generation == current_owner
        && current_owner > target.execution_fence.owner_generation
        && receipt.original_resource_revision == target.resource_revision
        && receipt.new_resource_revision == next_resource_revision
        && receipt.original_turn_id == target.turn_id
        && receipt.fresh_turn_id == *fresh_turn
        && !execution.snapshot.turn_history.contains(fresh_turn)
        && receipt.route == execution.snapshot.definition.route
        && receipt.definition_revision == execution.snapshot.definition.revision
        && receipt.reason == reason
}
