use super::{
    gate::{GateId, Gates},
    Error, Result,
};
use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{
    ApplicationIdentity, EffectBinding, EffectInvocation, EffectUncertain, InputValidator,
    InvalidApplicationInput, InvocationContext, NativeApplication, NativeEffect, NativeOutcome,
    OutputKind, RouteRegistration,
};
use b4_sqlite::SqliteStore;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
pub const WORKER_VALUE: &str = "/fixture/worker-value#workflow";
pub const WORKER_VOID: &str = "/fixture/worker-void#workflow";
pub const WORKER_FAILURE: &str = "/fixture/worker-failure#workflow";
pub const WORKER_HELD: &str = "/fixture/worker-held#workflow";
pub const WORKER_PARK: &str = "/fixture/worker-park#workflow";
const FACTORY: [&str; 10] = [
    "/work-orders/create#workflow",
    "/work-orders/dispatch#workflow",
    "/work-orders/approve#workflow",
    "/work-orders/retry#workflow",
    "/work-orders/deny#workflow",
    "/work-orders/intake#workflow",
    "/work-orders/approve-intake#workflow",
    "/work-orders/reject-intake#workflow",
    "/work-orders/cancel#workflow",
    "/reconcile#workflow",
];
struct Auth(String);
impl Authenticator for Auth {
    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> std::result::Result<Principal, AuthenticationError> {
        if headers.get_all("authorization").iter().count() != 1
            || headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(self.0.as_str())
        {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "ap-client-fixture".into(),
            tenant: "local".into(),
            principal: "owner".into(),
        })
    }
}
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        if matches!(
            c.operation,
            Operation::ReadThread
                | Operation::ReadState
                | Operation::ReadPending
                | Operation::CancelRun
                | Operation::Attach
                | Operation::DeleteThread
        ) && c
            .thread_id
            .as_ref()
            .is_some_and(|id| id.as_str().starts_with("fixture-concealed-"))
        {
            return false;
        }
        c.origin.as_ref().is_none_or(|o| {
            o.application == p.application && o.tenant == p.tenant && o.principal == p.principal
        })
    }
}
struct ObjectInput;
impl InputValidator for ObjectInput {
    fn validate(&self, input: &Value) -> std::result::Result<(), InvalidApplicationInput> {
        if input.is_object() || input.is_null() {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}
#[derive(Clone, Copy)]
enum Kind {
    Factory,
    Dispatch,
    Value,
    Void,
    Failure,
    Held,
    Park,
}
struct Effect {
    kind: Kind,
    gates: Gates,
}
impl NativeEffect for Effect {
    fn invoke(
        &self,
        call: &EffectInvocation,
        _: &InvocationContext,
    ) -> std::result::Result<NativeOutcome, EffectUncertain> {
        let _returned = match self.kind {
            Kind::Dispatch => self
                .gates
                .get(GateId::ControllerDispatch)
                .enter()
                .map_err(|_| EffectUncertain)?,
            Kind::Held => self
                .gates
                .get(GateId::WorkerHeld)
                .enter()
                .map_err(|_| EffectUncertain)?,
            _ => None,
        };
        if matches!(self.kind, Kind::Failure)
            || matches!(self.kind, Kind::Dispatch)
                && call
                    .input
                    .get("input")
                    .and_then(|input| input.get("operationKey"))
                    .and_then(Value::as_str)
                    == Some("fixture-known-failure")
        {
            return Ok(NativeOutcome::Failed("fixture-known-failure".into()));
        }
        let output = match self.kind {
            Kind::Void => Value::Null,
            Kind::Factory | Kind::Dispatch => json!({"ok":true,"message":"fixture-value"}),
            _ => json!({"fixture":"value"}),
        };
        Ok(NativeOutcome::Succeeded(TaskResult {
            output,
            writes: vec![],
        }))
    }
}
fn registration(route: &str, kind: Kind, gates: &Gates) -> Result<RouteRegistration> {
    let id = TaskId::new("fixture-task").map_err(|_| Error("fixture_registration_invalid"))?;
    let effect =
        EffectId::new("fixture-effect").map_err(|_| Error("fixture_registration_invalid"))?;
    let required = matches!(kind, Kind::Park);
    Ok(RouteRegistration {
        definition: Definition {
            route: RouteKey::new(route).map_err(|_| Error("fixture_registration_invalid"))?,
            revision: ExecutionRevision::new("fixture-v1")
                .map_err(|_| Error("fixture_registration_invalid"))?,
            max_attempts: 1,
            batches: vec![vec![TaskSpec {
                id,
                effect_id: effect.clone(),
                requires_approval: required,
                recovery: RecoveryPolicy::Pure,
            }]],
        },
        effects: BTreeMap::from([(
            effect,
            EffectBinding {
                callback: Arc::new(Effect {
                    kind,
                    gates: gates.clone(),
                }),
                output_kind: if matches!(kind, Kind::Void) {
                    OutputKind::Void
                } else {
                    OutputKind::Value
                },
            },
        )]),
        input_validator: Some(Arc::new(ObjectInput)),
    })
}
pub fn host(
    store: Arc<SqliteStore>,
    token: &str,
    gates: &Gates,
) -> Result<(HttpHost<SqliteStore>, std::sync::Weak<NativeApplication>)> {
    let mut routes = Vec::new();
    for route in FACTORY {
        let kind = if route == "/work-orders/cancel#workflow" {
            Kind::Void
        } else if route == "/work-orders/dispatch#workflow" {
            Kind::Dispatch
        } else {
            Kind::Factory
        };
        routes.push(registration(route, kind, gates)?);
    }
    for (route, kind) in [
        (WORKER_VALUE, Kind::Value),
        (WORKER_VOID, Kind::Void),
        (WORKER_FAILURE, Kind::Failure),
        (WORKER_HELD, Kind::Held),
        (WORKER_PARK, Kind::Park),
    ] {
        routes.push(registration(route, kind, gates)?);
    }
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "ap-client-fixture".into(),
                revision: "fixture-application-v1".into(),
            },
            routes,
        )
        .map_err(|_| Error("fixture_application_invalid"))?,
    );
    let weak = Arc::downgrade(&application);
    let approval = ApprovalHostConfig::new(
        "fixture-policy-v1".into(),
        BTreeMap::from([(
            RouteKey::new(WORKER_PARK).unwrap(),
            BTreeMap::from([(
                TaskId::new("fixture-task").unwrap(),
                json!({"question":"fixture-approval"}),
            )]),
        )]),
    );
    let mut config = NativeHostConfig::new(application, approval);
    config.limits.max_active_runs = 4;
    config.limits.max_concurrent_tasks = 4;
    config.limits.max_input_bytes = 65536;
    config.limits.max_output_bytes = 65536;
    let host = HttpHost::new_with_application(
        store,
        Arc::new(Auth(format!("Bearer {token}"))),
        Arc::new(Policy),
        config,
    )
    .map_err(|_| Error("fixture_host_invalid"))?;
    Ok((host, weak))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_has_true_void_single_held_task_and_finite_nested_dispatch_table() {
        let gates = Gates::new();
        let cancel = registration(FACTORY[8], Kind::Void, &gates).unwrap();
        assert_eq!(
            cancel.effects.values().next().unwrap().output_kind,
            OutputKind::Void
        );
        let held = registration(WORKER_HELD, Kind::Held, &gates).unwrap();
        assert_eq!(held.definition.batches.iter().flatten().count(), 1);
        assert!(!held.definition.batches[0][0].requires_approval);
        let park = registration(WORKER_PARK, Kind::Park, &gates).unwrap();
        assert!(park.definition.batches[0][0].requires_approval);
    }
}
