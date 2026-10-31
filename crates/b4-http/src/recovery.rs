use crate::{host::Shared, transport::*, *};
use axum::{http::StatusCode, response::Response};
use b4_contract::*;
use b4_runtime::{approval::*, ExecutionFence, Store, StoreError, ThreadOrigin, ThreadView};
use std::sync::atomic::Ordering;

mod admission;
pub(crate) mod mapping;
pub use admission::{
    RecoveryAdmission, RecoveryAdmissionError, RecoveryAdoption, RecoveryDisposition,
    RecoveryRefusal,
};
#[cfg(test)]
mod tests;

/// A trusted embedding application's exact known execution target. Construct
/// from a coherent ordinary view and retain all fields until recovery launches.
/// This is neither an Agent Protocol request nor permission to bypass policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryTarget {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub fence: ApprovalFence,
}
impl RecoveryTarget {
    pub fn from_view(view: &ThreadView) -> Option<Self> {
        let execution = view.execution.as_ref()?;
        if view.thread.active_execution_id.as_ref() != Some(&execution.snapshot.execution_id) {
            return None;
        }
        Some(Self {
            thread_id: view.thread.id.clone(),
            thread_generation: view.thread.generation.clone(),
            execution_id: execution.snapshot.execution_id.clone(),
            fence: ApprovalFence {
                execution: ExecutionFence::from(&execution.snapshot),
                resource_revision: view.thread.resource_revision,
            },
        })
    }
}

impl<S: Store + 'static> HttpHost<S> {
    /// Admit one exact known prior-owner target without retaining a response
    /// viewer. A receipt is an admission milestone, not callback retirement.
    pub async fn admit_recovery(
        &self,
        principal: Principal,
        target: RecoveryTarget,
    ) -> Result<RecoveryAdmission, RecoveryAdmissionError> {
        let shared = crate::lifecycle::OwnedShared::register(self.shared.clone())
            .ok_or_else(|| RecoveryAdmissionError::new(RecoveryRefusal::AdmissionClosed, None))?;
        recover_owned(shared, principal, target, None, false)
            .await
            .map(|result| result.admission)
    }

    /// Recover and supervise one known prior-owner execution. The embedding
    /// application supplies verified server-owned identity. No router endpoint
    /// exposes this method. Capacity and same-thread claims are retained by the
    /// owned worker through quiescence even if this caller or response detaches.
    pub async fn recover_execution(
        &self,
        principal: Principal,
        target: RecoveryTarget,
    ) -> Response {
        let Some(shared) = crate::lifecycle::OwnedShared::register(self.shared.clone()) else {
            return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
        };
        match recover_owned(shared, principal, target, None, true).await {
            Ok(result) => match result.admission.disposition {
                RecoveryDisposition::Blocked => error(StatusCode::CONFLICT, "recovery_blocked"),
                RecoveryDisposition::AttemptLimit => {
                    error(StatusCode::CONFLICT, "recovery_attempt_limit")
                }
                _ => result.presentation.expect("recovery_response_presentation"),
            },
            Err(reason) => mapping::response(reason),
        }
    }
}

pub(crate) async fn recover_owned<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    target: RecoveryTarget,
    captured: Option<ThreadView>,
    presentation: bool,
) -> Result<crate::worker::RecoveryLaunch, RecoveryAdmissionError> {
    if !valid_principal(&principal) || !crate::application::scope_allows(&shared, &principal) {
        return Err(RecoveryAdmissionError::new(
            RecoveryRefusal::ResourceUnavailable,
            None,
        ));
    }
    if shared.approval.is_none() {
        return Err(RecoveryAdmissionError::new(
            RecoveryRefusal::Unsupported,
            None,
        ));
    }
    if shared.fault.load(Ordering::Acquire) {
        return Err(RecoveryAdmissionError::new(
            RecoveryRefusal::Unavailable,
            None,
        ));
    }
    let owned = shared.clone();
    let captured_target = target.clone();
    let captured_principal = principal.clone();
    let authorized = owned
        .spawn(move |s| {
            preflight(s, &captured_principal, &captured_target, captured.as_ref())
                .map(|(view, _route)| view)
                .map_err(|error| mapping::approval(s, error, None))
        })
        .await;
    let view = match authorized {
        Ok(result) => result?,
        Err(error) => {
            shared.fault.store(true, Ordering::Release);
            if error.is_panic() {
                crate::lifecycle::dispose_panic(error.into_panic());
            }
            return Err(RecoveryAdmissionError::new(
                RecoveryRefusal::Unavailable,
                None,
            ));
        }
    };
    if shared.permission_uses.is_some() {
        return Err(RecoveryAdmissionError::new(
            RecoveryRefusal::Unsupported,
            None,
        ));
    }
    let route = view
        .execution
        .as_ref()
        .and_then(|execution| {
            crate::route::recorded(
                shared.application.as_ref(),
                &shared.routes,
                &execution.snapshot.definition,
            )
        })
        .ok_or_else(|| RecoveryAdmissionError::new(RecoveryRefusal::DefinitionUnavailable, None))?;
    crate::worker::launch_recovery(shared, principal, target, view, route, presentation).await
}

pub(crate) fn preflight<S: Store>(
    shared: &Shared<S>,
    principal: &Principal,
    target: &RecoveryTarget,
    captured: Option<&ThreadView>,
) -> Result<(ThreadView, crate::route::RegisteredRoute), ApprovalError> {
    if !valid_principal(principal) || !crate::application::scope_allows(shared, principal) {
        return Err(ApprovalError::Unauthorized);
    }
    let view = shared
        .store
        .thread_view(&target.thread_id)?
        .ok_or(ApprovalError::Unauthorized)?;
    let origin = ThreadOrigin {
        application: principal.application.clone(),
        tenant: principal.tenant.clone(),
        principal: principal.principal.clone(),
    };
    let provenance = view.execution.as_ref().map(|execution| RouteProvenance {
        route: execution.snapshot.definition.route.clone(),
        revision: execution.snapshot.definition.revision.clone(),
    });
    let context = ResourceContext {
        operation: Operation::Recover,
        thread_id: Some(view.thread.id.clone()),
        origin: view.thread.origin.clone(),
        stored_route: provenance.clone(),
        requested_route: provenance,
    };
    // Resolve and authorize the actual ordinary resource before disclosing any
    // target, registration, owner or terminal-state discrepancy.
    if !valid_principal(principal)
        || view.thread.origin.as_ref() != Some(&origin)
        || !shared.policy.authorize(principal, &context)
    {
        return Err(ApprovalError::Unauthorized);
    }
    let current = view
        .execution
        .as_ref()
        .ok_or(ApprovalError::RecoveryNotNeeded)?;
    let route = crate::route::recorded(
        shared.application.as_ref(),
        &shared.routes,
        &current.snapshot.definition,
    )
    .ok_or(ApprovalError::RecoveryDefinitionMismatch)?;
    if captured.is_some_and(|captured| *captured != view)
        || target.thread_generation != view.thread.generation
        || target.execution_id != current.snapshot.execution_id
        || target.fence.execution != ExecutionFence::from(&current.snapshot)
        || target.fence.resource_revision != view.thread.resource_revision
    {
        return Err(StoreError::RevisionConflict.into());
    }
    if view.thread.active_execution_id.as_ref() != Some(&target.execution_id)
        || b4_runtime::releases_slot(current.snapshot.status)
        || current.snapshot.owner_generation == shared.store.owner_generation()
    {
        return Err(ApprovalError::RecoveryNotNeeded);
    }
    if current.snapshot.owner_generation > shared.store.owner_generation() {
        return Err(StoreError::OwnerConflict.into());
    }
    Ok((view, route))
}
