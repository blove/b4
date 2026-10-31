use crate::{
    claim::{ClaimError, ThreadClaim},
    transport::{error, store_error, unavailable},
    *,
};
use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use b4_runtime::{
    approval::ApprovalError, permissions::PermissionError, Store, StoreError, ThreadDeletionTarget,
    ThreadView,
};
use std::panic::{catch_unwind, AssertUnwindSafe};

fn origin_matches(principal: &Principal, view: &ThreadView) -> bool {
    view.thread.origin.as_ref().is_some_and(|origin| {
        origin.application == principal.application
            && origin.tenant == principal.tenant
            && origin.principal == principal.principal
    })
}
pub(crate) fn empty() -> Response {
    (
        StatusCode::NO_CONTENT,
        [(header::CACHE_CONTROL, "no-store")],
    )
        .into_response()
}

pub(crate) async fn delete<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    captured: ThreadView,
) -> Response {
    let claim = match ThreadClaim::acquire(shared.claims.clone(), captured.thread.id.clone()) {
        Ok(claim) => claim,
        Err(ClaimError::Busy) => return error(StatusCode::CONFLICT, "run_in_flight"),
        Err(ClaimError::Poisoned) => return unavailable(&shared),
    };
    let owned = shared.clone();
    // There is no await between acquiring the exclusion claim and transferring
    // it to an owned worker. Dropping the caller cannot cancel deletion or hide
    // its infrastructure fault from readiness.
    match owned
        .spawn(move |owned| {
            let _claim = claim;
            match catch_unwind(AssertUnwindSafe(|| {
                let current = match owned.store.thread_view(&captured.thread.id) {
                    Ok(current) => current,
                    Err(error) => return store_error(owned, error),
                };
                let context = ResourceContext {
                    operation: Operation::DeleteThread,
                    thread_id: Some(captured.thread.id.clone()),
                    origin: current.as_ref().and_then(|view| view.thread.origin.clone()),
                    stored_route: current
                        .as_ref()
                        .and_then(|view| view.execution.as_ref())
                        .map(|execution| RouteProvenance {
                            route: execution.snapshot.definition.route.clone(),
                            revision: execution.snapshot.definition.revision.clone(),
                        }),
                    requested_route: None,
                };
                if !owned.policy.authorize(&principal, &context)
                    || current
                        .as_ref()
                        .is_some_and(|view| !origin_matches(&principal, view))
                {
                    return error(StatusCode::NOT_FOUND, "resource_unavailable");
                }
                if current.as_ref() != Some(&captured) {
                    return error(StatusCode::CONFLICT, "resource_changed");
                }
                let target = match ThreadDeletionTarget::from_view(&captured) {
                    Ok(target) => target,
                    Err(error) => return store_error(owned, error),
                };
                if owned.permission_deletion.is_some() {
                    let Some(execution) = captured.execution.as_ref() else {
                        return error(
                            StatusCode::NOT_IMPLEMENTED,
                            "permission_operation_unsupported",
                        );
                    };
                    if !b4_runtime::releases_slot(execution.snapshot.status) {
                        return error(StatusCode::CONFLICT, "run_in_flight");
                    }
                }
                let Some(_lease) = owned.try_admit() else {
                    return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
                };
                if let Some(bridge) = &owned.permission_deletion {
                    return match bridge.delete(&principal, &target) {
                        Ok(confirmation) if confirmation.target() == &target => empty(),
                        Ok(_) => unavailable(owned),
                        Err(error) => permission_error(owned, error),
                    };
                }
                match owned.store.delete_thread_fenced(&target) {
                    Ok(()) => empty(),
                    Err(StoreError::ThreadBusy) => error(StatusCode::CONFLICT, "run_in_flight"),
                    Err(StoreError::ThreadNotFound | StoreError::RevisionConflict) => {
                        error(StatusCode::CONFLICT, "resource_changed")
                    }
                    Err(StoreError::UnsupportedOperation) => {
                        error(StatusCode::NOT_IMPLEMENTED, "unsupported_operation")
                    }
                    Err(error) => store_error(owned, error),
                }
            })) {
                Ok(response) => response,
                Err(_) => unavailable(owned),
            }
        })
        .await
    {
        Ok(response) => response,
        Err(_) => unavailable(&shared),
    }
}

// Expected management refusals keep readiness healthy; all other errors are
// infrastructure failures and expose only the static unavailable response.
fn permission_error<S: Store>(shared: &crate::host::Shared<S>, cause: PermissionError) -> Response {
    match cause {
        PermissionError::Approval(ApprovalError::Unauthorized | ApprovalError::RouteMismatch) => {
            error(StatusCode::NOT_FOUND, "resource_unavailable")
        }
        PermissionError::Store(StoreError::ThreadNotFound | StoreError::RevisionConflict)
        | PermissionError::Approval(ApprovalError::Store(
            StoreError::ThreadNotFound | StoreError::RevisionConflict,
        )) => error(StatusCode::CONFLICT, "resource_changed"),
        PermissionError::Store(StoreError::ThreadBusy)
        | PermissionError::Approval(ApprovalError::Store(StoreError::ThreadBusy)) => {
            error(StatusCode::CONFLICT, "run_in_flight")
        }
        PermissionError::LimitExceeded => error(StatusCode::CONFLICT, "permission_limit_exceeded"),
        PermissionError::UnsupportedProfile => error(
            StatusCode::NOT_IMPLEMENTED,
            "permission_operation_unsupported",
        ),
        _ => unavailable(shared),
    }
}
