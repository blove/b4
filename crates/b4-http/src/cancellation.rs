use crate::{
    claim::CapturedRun,
    transport::{error, json_response, now_ms, unavailable},
    *,
};
use axum::{http::StatusCode, response::Response};
use b4_contract::{ExecutionStatus, ThreadId, TransitionFacts};
use b4_runtime::{
    approval::ApprovalError, permissions::PermissionError, RunCancellationTarget, Store,
    StoreError, ThreadView,
};
use serde_json::json;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn origin_matches(principal: &Principal, view: &ThreadView) -> bool {
    view.thread.origin.as_ref().is_some_and(|origin| {
        origin.application == principal.application
            && origin.tenant == principal.tenant
            && origin.principal == principal.principal
    })
}
fn no_run() -> Response {
    error(StatusCode::CONFLICT, "no_run_in_flight")
}
pub(crate) async fn cancel<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    id: ThreadId,
    captured: Option<CapturedRun>,
) -> Response {
    let owned = shared.clone();
    match owned
        .spawn(move |owned| {
            let response = catch_unwind(AssertUnwindSafe(|| {
                let mut lease = None;
                let mut pending: Option<crate::control::CancellationPending> = None;
                for _ in 0..8 {
                    let _ = &lease;
                    let current = match owned.store.thread_view(&id) {
                        Ok(view) => view,
                        Err(_) => return unavailable(owned),
                    };
                    let context = ResourceContext {
                        operation: Operation::CancelRun,
                        thread_id: Some(id.clone()),
                        origin: current.as_ref().and_then(|v| v.thread.origin.clone()),
                        stored_route: current.as_ref().and_then(|v| v.execution.as_ref()).map(
                            |e| RouteProvenance {
                                route: e.snapshot.definition.route.clone(),
                                revision: e.snapshot.definition.revision.clone(),
                            },
                        ),
                        requested_route: None,
                    };
                    if !owned.policy.authorize(&principal, &context)
                        || current
                            .as_ref()
                            .is_some_and(|v| !origin_matches(&principal, v))
                    {
                        if let Some(pending) = pending.take() {
                            pending.reject();
                        }
                        return error(StatusCode::NOT_FOUND, "resource_unavailable");
                    }
                    let Some(view) = current else {
                        if let Some(pending) = pending.take() {
                            pending.reject();
                        }
                        return no_run();
                    };
                    let Some(run) = &captured else {
                        return no_run();
                    };
                    let held = match owned.claims.lock() {
                        Ok(claims) => claims.contains(&id, run),
                        Err(_) => return unavailable(owned),
                    };
                    if !held
                        || !run.control.identity.matches(&view)
                        || view.execution.as_ref().is_none_or(|execution| {
                            !matches!(
                                execution.snapshot.status,
                                ExecutionStatus::Accepted
                                    | ExecutionStatus::Running
                                    | ExecutionStatus::CancelRequested
                            )
                        })
                    {
                        if let Some(pending) = pending.take() {
                            pending.reject();
                        }
                        return no_run();
                    }
                    // A durable Dispatch alone is not callback entry. Resolve the
                    // private proof before Pending can block the driver's entry gate.
                    let permission_kind = if let Some(bridge) = &owned.permission_cancellation {
                        match bridge.classify(&run.control, &view) {
                            Some(kind) => Some(kind),
                            None => {
                                if let Some(pending) = pending.take() {
                                    pending.reject();
                                }
                                return error(StatusCode::CONFLICT, "run_not_cancellable");
                            }
                        }
                    } else {
                        None
                    };
                    if let Some(crate::permission_cancellation::CancellationKind::Withheld(kind)) =
                        permission_kind
                    {
                        // This branch owns one command and cannot reach the legacy
                        // RevisionConflict retry loop below.
                        if lease.is_none() {
                            lease = owned.try_admit();
                            if lease.is_none() {
                                return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
                            }
                        }
                        let pending = match run.control.reserve_permission_withheld() {
                            Ok(Some(pending)) => pending,
                            Ok(None) => return error(StatusCode::CONFLICT, "run_not_cancellable"),
                            Err(_) => return unavailable(owned),
                        };
                        return cancel_permission_withheld(
                            owned, &principal, &id, run, &view, kind, pending,
                        );
                    }
                    if pending.is_none() {
                        if lease.is_none() {
                            lease = owned.try_admit();
                            if lease.is_none() {
                                return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
                            }
                        }
                        pending = match run.control.begin_cancel() {
                            Ok(Some(pending)) => Some(pending),
                            Ok(None) => return no_run(),
                            Err(_) => return unavailable(owned),
                        };
                    }
                    let target = match RunCancellationTarget::from_view(&view) {
                        Ok(target) => target,
                        Err(_) => return unavailable(owned),
                    };
                    let facts = TransitionFacts {
                        now_ms: now_ms()
                            .max(view.execution.as_ref().unwrap().snapshot.updated_at_ms),
                    };
                    let result = if let Some(kind) = permission_kind {
                        owned
                            .permission_cancellation
                            .as_ref()
                            .unwrap()
                            .cancel(&principal, kind, &target, facts)
                    } else {
                        owned
                            .store
                            .cancel_run_fenced(&target, facts)
                            .map(crate::permission_cancellation::CancellationCommit::Entered)
                            .map_err(PermissionError::Store)
                    };
                    match result {
                        Ok(result) => {
                            use crate::permission_cancellation::{
                                CancellationCommit, CancellationKind, OriginKind,
                            };
                            let committed = match &result {
                                CancellationCommit::Ready(confirmation) => confirmation.execution(),
                                CancellationCommit::Entered(execution) => execution,
                            };
                            if let Some(CancellationKind::Ready(kind)) = permission_kind {
                                let expected_revision = match kind {
                                    OriginKind::OriginalAlways => 4,
                                    OriginKind::NewUse => 3,
                                };
                                if !run.control.identity.matches_execution(committed)
                                    || committed.snapshot.status != ExecutionStatus::Cancelled
                                    || committed.snapshot.revision != expected_revision
                                    || committed.snapshot.revision
                                        != target.execution_fence.revision + 1
                                    || committed.snapshot.tasks.len() != 1
                                    || committed.snapshot.tasks[0].attempt_id.is_some()
                                    || committed.snapshot.attempts_used != 0
                                    || !committed.snapshot.attempt_history.is_empty()
                                    || !matches!(result, CancellationCommit::Ready(_))
                                {
                                    return unavailable(owned);
                                }
                            }
                            if let Some(CancellationKind::Entered(kind)) = permission_kind {
                                let expected_revision = match kind {
                                    crate::permission_cancellation::OriginKind::OriginalAlways => 5,
                                    crate::permission_cancellation::OriginKind::NewUse => 4,
                                };
                                if !run.control.identity.matches_execution(committed)
                                    || committed.snapshot.status != ExecutionStatus::CancelRequested
                                    || committed.snapshot.revision != expected_revision
                                    || committed.snapshot.revision
                                        != target.execution_fence.revision + 1
                                {
                                    return unavailable(owned);
                                }
                            }
                            let pending = pending.take().unwrap();
                            let published = match result {
                                CancellationCommit::Ready(confirmation) => {
                                    pending.commit_ready(confirmation)
                                }
                                CancellationCommit::Entered(_) => pending.commit(),
                            };
                            if !published {
                                return unavailable(owned);
                            }
                            return json_response(
                                StatusCode::OK,
                                json!({"status":"interrupted","thread_id":id}),
                            );
                        }
                        Err(PermissionError::Store(StoreError::RevisionConflict)) => {
                            if matches!(
                                permission_kind,
                                Some(crate::permission_cancellation::CancellationKind::Ready(_))
                            ) {
                                // Release Pending before reclassification: Dispatch may be
                                // waiting at this same run's entry gate. The next bounded
                                // iteration can select real entered/Ready evidence or refuse.
                                pending.take().unwrap().reject();
                            }
                            continue;
                        }
                        Err(PermissionError::Store(
                            StoreError::NoRunInFlight
                            | StoreError::ExecutionNotActive
                            | StoreError::ExecutionNotFound
                            | StoreError::ThreadNotFound
                            | StoreError::OwnerConflict,
                        )) => {
                            pending.take().unwrap().reject();
                            return no_run();
                        }
                        Err(PermissionError::Approval(
                            ApprovalError::Unauthorized | ApprovalError::RouteMismatch,
                        )) => {
                            pending.take().unwrap().reject();
                            return error(StatusCode::NOT_FOUND, "resource_unavailable");
                        }
                        Err(PermissionError::UnsupportedProfile) => {
                            pending.take().unwrap().reject();
                            return error(StatusCode::CONFLICT, "run_not_cancellable");
                        }
                        Err(PermissionError::Store(StoreError::UnsupportedOperation)) => {
                            pending.take().unwrap().reject();
                            return error(StatusCode::NOT_IMPLEMENTED, "unsupported_operation");
                        }
                        Err(_) => return unavailable(owned),
                    }
                }
                if let Some(pending) = pending.take() {
                    pending.reject();
                }
                error(StatusCode::CONFLICT, "resource_changed")
            }));
            match response {
                Ok(response) => response,
                Err(_) => {
                    if let Some(run) = &captured {
                        run.control.fault();
                    }
                    unavailable(owned)
                }
            }
        })
        .await
    {
        Ok(response) => response,
        Err(_) => unavailable(&shared),
    }
}

/// One owned management operation. Pending's drop faults a Reserved/Ready
/// handoff on every early return or unwind, and wakes the drained worker.
fn cancel_permission_withheld<S: Store + 'static>(
    owned: &crate::lifecycle::OwnedShared<S>,
    principal: &Principal,
    id: &ThreadId,
    run: &CapturedRun,
    selected: &ThreadView,
    kind: crate::permission_cancellation::OriginKind,
    pending: crate::control::CancellationPending,
) -> Response {
    use crate::permission_cancellation::OriginKind;
    // The third current-view read in this operation is deliberately AFTER
    // reservation. No registry/control lock is held across Store or policy.
    let current = match owned.store.thread_view(id) {
        Ok(Some(view)) => view,
        _ => return unavailable(owned),
    };
    let held = match owned.claims.lock() {
        Ok(claims) => claims.contains(id, run),
        Err(_) => return unavailable(owned),
    };
    let target = match RunCancellationTarget::from_view(selected) {
        Ok(target) => target,
        Err(_) => return unavailable(owned),
    };
    if !held || current != *selected || !run.control.identity.matches(&current) {
        return error(StatusCode::CONFLICT, "resource_changed");
    }
    if !origin_matches(principal, &current) {
        return error(StatusCode::NOT_FOUND, "resource_unavailable");
    }
    let Some(bridge) = &owned.permission_cancellation else {
        return unavailable(owned);
    };
    if let Err(reason) = bridge.authorize_withheld(principal, &current, &target) {
        return match reason {
            PermissionError::Approval(
                ApprovalError::Unauthorized | ApprovalError::RouteMismatch,
            ) => error(StatusCode::NOT_FOUND, "resource_unavailable"),
            _ => unavailable(owned),
        };
    }
    let facts = TransitionFacts {
        now_ms: now_ms().max(current.execution.as_ref().unwrap().snapshot.updated_at_ms),
    };
    if !run
        .control
        .permission_withheld_ready(crate::control::WithheldCommand {
            principal: principal.clone(),
            kind,
            target: target.clone(),
            facts: facts.clone(),
        })
    {
        return unavailable(owned);
    }
    let result = match run.control.permission_withheld_result() {
        Some(result) => result,
        None => return unavailable(owned),
    };
    let committed = match result {
        Ok(committed) => committed,
        Err(PermissionError::Approval(
            ApprovalError::Unauthorized | ApprovalError::RouteMismatch,
        )) => return error(StatusCode::NOT_FOUND, "resource_unavailable"),
        Err(_) => return unavailable(owned),
    };
    // The real failure-owned typed operation proves both transitions and their
    // atomic journal writes. Bind that result to this captured same-run command;
    // dispatched Work necessarily retains its actual attempt and history.
    let before = &selected.execution.as_ref().unwrap().snapshot;
    let after = &committed.snapshot;
    if !run.control.identity.matches_execution(&committed)
        || after.status != ExecutionStatus::Cancelled
        || after.revision
            != match kind {
                OriginKind::OriginalAlways => 6,
                OriginKind::NewUse => 5,
            }
        || after.revision != target.execution_fence.revision + 2
        || after.tasks.len() != 1
        || before.tasks.len() != 1
        || after.tasks[0].status != b4_contract::TaskStatus::Cancelled
        || after.tasks[0].attempt_id != before.tasks[0].attempt_id
        || after.tasks[0].spec != before.tasks[0].spec
        || after.schema_version != before.schema_version
        || after.turn_history != before.turn_history
        || after.definition != before.definition
        || after.batch_index != before.batch_index
        || after.values != before.values
        || after.updated_at_ms != facts.now_ms
        || after.attempts_used != before.attempts_used
        || after.attempt_history != before.attempt_history
        || after.output.is_some()
        || after.failure.is_some()
        || after.tasks[0].result.is_some()
        || after.tasks[0].failure.is_some()
    {
        return unavailable(owned);
    }
    if !pending.commit_withheld() {
        return unavailable(owned);
    }
    json_response(
        StatusCode::OK,
        json!({"status":"interrupted","thread_id":id}),
    )
}
