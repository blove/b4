use crate::{
    claim::CapturedRun,
    control::{RunIdentity, Subscription},
    host::Shared,
    transport::{approval_error, error, store_error, unavailable},
    viewer::{self, Frame, ViewerStream},
    *,
};
use axum::{
    http::StatusCode,
    response::{sse::Event, Response},
};
use b4_contract::*;
use b4_runtime::{Store, StoreError, ThreadView};
use serde_json::json;
use std::time::Duration;

fn origin_matches(principal: &Principal, view: &ThreadView) -> bool {
    view.thread.origin.as_ref().is_some_and(|origin| {
        origin.application == principal.application
            && origin.tenant == principal.tenant
            && origin.principal == principal.principal
    })
}
fn context(view: Option<&ThreadView>, id: &ThreadId) -> ResourceContext {
    ResourceContext {
        operation: Operation::Attach,
        thread_id: Some(id.clone()),
        origin: view.and_then(|v| v.thread.origin.clone()),
        stored_route: view
            .and_then(|v| v.execution.as_ref())
            .map(|e| RouteProvenance {
                route: e.snapshot.definition.route.clone(),
                revision: e.snapshot.definition.revision.clone(),
            }),
        requested_route: None,
    }
}
fn status(view: &ThreadView) -> &'static str {
    match view.execution.as_ref().map(|e| e.snapshot.status) {
        Some(ExecutionStatus::WaitingForApproval) => "interrupted",
        Some(
            ExecutionStatus::Accepted
            | ExecutionStatus::Running
            | ExecutionStatus::CancelRequested
            | ExecutionStatus::RecoveryBlocked,
        ) => "busy",
        _ => "idle",
    }
}
fn registered<S>(shared: &Shared<S>, view: &ThreadView) -> bool {
    view.execution.as_ref().is_some_and(|execution| {
        crate::route::recorded(
            shared.application.as_ref(),
            &shared.routes,
            &execution.snapshot.definition,
        )
        .is_some()
    })
}
fn identity(view: &ThreadView) -> Option<RunIdentity> {
    let execution = view.execution.as_ref()?;
    Some(RunIdentity {
        thread_id: view.thread.id.clone(),
        thread_generation: view.thread.generation.clone(),
        execution_id: execution.snapshot.execution_id.clone(),
        turn_id: execution.snapshot.turn_id.clone(),
        owner_generation: execution.snapshot.owner_generation,
    })
}

pub(crate) async fn attach<S: Store + 'static>(
    shared: crate::lifecycle::OwnedShared<S>,
    principal: Principal,
    id: ThreadId,
) -> Response {
    let s = shared.clone();
    let thread = id.clone();
    let captured = match s
        .spawn(move |s| {
            let result = s.store.thread_view(&thread);
            if let Err(error) = &result {
                crate::lifecycle::record_store_failure(s, error);
            }
            result
        })
        .await
    {
        Ok(Ok(view)) => view,
        Ok(Err(error)) => return store_error(&shared, error),
        Err(_) => return unavailable(&shared),
    };
    // Keep the subscription in THIS async future. A policy worker receives no
    // guard, so abandoning either wait immediately releases the viewer slot.
    let run: Option<CapturedRun> = match shared.claims.lock() {
        Ok(claims) => claims.capture(&id).filter(|run| {
            captured
                .as_ref()
                .is_some_and(|view| run.control.identity.matches(view))
        }),
        Err(_) => return unavailable(&shared),
    };
    let mut subscription = run
        .as_ref()
        .map(|run| run.control.subscribe(status(captured.as_ref().unwrap())));
    let live = matches!(
        subscription,
        Some(Subscription::Live { .. } | Subscription::Capacity)
    );
    let local = run.is_some();
    let initial = captured.clone();
    let s = shared.clone();
    let p = principal.clone();
    let thread = id.clone();
    let allowed = match s
        .spawn(move |s| {
            s.policy.authorize(&p, &context(initial.as_ref(), &thread))
                && initial.as_ref().is_none_or(|view| origin_matches(&p, view))
        })
        .await
    {
        Ok(allowed) => allowed,
        Err(_) => return unavailable(&shared),
    };
    if !allowed {
        return error(StatusCode::NOT_FOUND, "resource_unavailable");
    }
    let Some(captured) = captured else {
        return error(StatusCode::NOT_FOUND, "resource_unavailable");
    };
    // Renew authority before every protected diagnostic, including viewer
    // capacity and output limits. Never reselect a publisher after this wait.
    let original = captured.clone();
    let expected = identity(&captured);
    let s = shared.clone();
    let p = principal.clone();
    let thread = id.clone();
    let renewed = s
        .spawn(move |s| {
            let current = s.store.thread_view(&thread).inspect_err(|error| {
                crate::lifecycle::record_store_failure(s, error);
            })?;
            if !s.policy.authorize(&p, &context(current.as_ref(), &thread))
                || current
                    .as_ref()
                    .is_some_and(|view| !origin_matches(&p, view))
            {
                return Ok::<_, StoreError>(Err("resource_unavailable"));
            }
            // Policy can wait while the original publisher retires. Renew its
            // resource view after that wait without selecting another publisher.
            let current = s.store.thread_view(&thread).inspect_err(|error| {
                crate::lifecycle::record_store_failure(s, error);
            })?;
            if current
                .as_ref()
                .is_some_and(|view| !origin_matches(&p, view))
            {
                return Ok(Err("resource_unavailable"));
            }
            let Some(current) = current else {
                return Ok(Err("resource_changed"));
            };
            if current.thread.origin != original.thread.origin
                || (live
                    && expected
                        .as_ref()
                        .is_none_or(|identity| !identity.matches(&current)))
                || (!live && current != original)
                || current.execution.as_ref().map(|e| &e.snapshot.definition)
                    != original.execution.as_ref().map(|e| &e.snapshot.definition)
            {
                return Ok(Err("resource_changed"));
            }
            if !registered(s, &current) {
                return Ok(Err("thread_route_unknown"));
            }
            let execution = current.execution.as_ref().unwrap();
            if execution.snapshot.status == ExecutionStatus::RecoveryBlocked {
                return Ok(Err("recovery_required"));
            }
            if matches!(
                execution.snapshot.status,
                ExecutionStatus::Accepted
                    | ExecutionStatus::Running
                    | ExecutionStatus::CancelRequested
            ) && !local
            {
                return Ok(Err(
                    if execution.snapshot.owner_generation != s.store.owner_generation() {
                        "recovery_required"
                    } else {
                        "attachment_unavailable"
                    },
                ));
            }
            Ok(Ok(current))
        })
        .await;
    let current = match renewed {
        Ok(Ok(Ok(current))) => current,
        Ok(Ok(Err(code))) => {
            return error(
                if code == "resource_unavailable" {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::CONFLICT
                },
                code,
            )
        }
        Ok(Err(error)) => return store_error(&shared, error),
        Err(_) => return unavailable(&shared),
    };
    match subscription.take() {
        Some(Subscription::Faulted) => unavailable(&shared),
        Some(Subscription::Unavailable) => error(StatusCode::CONFLICT, "attachment_unavailable"),
        Some(Subscription::Capacity) => viewer::response(
            ViewerStream::finite(vec![Event::default()
                .event("detached")
                .data("{\"reason\":\"capacity\"}")]),
            shared.config.heartbeat_ms,
        ),
        Some(Subscription::Live { state, mut stream }) => {
            if Frame::new("state", &state).bytes > shared.config.attachment_snapshot_bytes {
                return error(StatusCode::CONFLICT, "attachment_snapshot_limit");
            }
            stream.prefix(vec![Event::default()
                .event("state")
                .data(state.to_string())]);
            viewer::response(stream, shared.config.heartbeat_ms)
        }
        _ => {
            let interrupts = if current
                .execution
                .as_ref()
                .is_some_and(|e| e.snapshot.status == ExecutionStatus::WaitingForApproval)
            {
                let Some(bridge) = shared.approval.clone() else {
                    return error(StatusCode::NOT_IMPLEMENTED, "unsupported_execution_state");
                };
                let p = principal.clone();
                let view = current.clone();
                match shared
                    .clone()
                    .spawn(move |s| {
                        let result = bridge.attachment_pending(&p, &view);
                        if let Err(error) = &result {
                            crate::lifecycle::record_approval_failure(s, error);
                        }
                        result
                    })
                    .await
                {
                    Ok(Ok(interrupts)) => interrupts,
                    Ok(Err(error)) => return approval_error(&shared, error),
                    Err(_) => return unavailable(&shared),
                }
            } else {
                vec![]
            };
            let state = json!({"anchor":null,"input":null,"interrupts":interrupts,"live":false,"resume":false,"run_started_at":null,"status":status(&current),"turn":null,"values":current.execution.as_ref().map(|e|&e.snapshot.values)});
            if Frame::new("state", &state).bytes > shared.config.attachment_snapshot_bytes {
                return error(StatusCode::CONFLICT, "attachment_snapshot_limit");
            }
            viewer::response(
                ViewerStream::finite(vec![
                    Event::default().event("state").data(state.to_string()),
                    Event::default().retry(Duration::from_millis(2000)),
                    Event::default().event("done").data("{\"output\":null}"),
                ]),
                shared.config.heartbeat_ms,
            )
        }
    }
}
