use b4_contract::{
    Definition, EffectId, ExecutionRevision, RecoveryPolicy, RouteKey, TaskId, TaskSpec,
};
use b4_http::{
    AuthenticationError, Authenticator, HostConfig, HttpHost, Principal, ResourceContext,
    ResourcePolicy, ScriptEffect, ScriptRoute,
};
use b4_runtime::{MemoryStore, Store};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

pub mod batch;
pub mod native;
pub mod tcp;

pub const ROUTE: &str = "/diagnostics#workflow";
pub const AUTHORIZATION: &str = "Bearer diagnostics-owner";

pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Principal, AuthenticationError> {
        let (application, principal) = match headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
        {
            Some(AUTHORIZATION) => ("diagnostics-application", "diagnostics-principal"),
            Some("Bearer diagnostics-other") => {
                ("diagnostics-application", "private-other-principal")
            }
            Some("Bearer diagnostics-other-application") => {
                ("private-other-application", "diagnostics-principal")
            }
            _ => return Err(AuthenticationError),
        };
        Ok(Principal {
            application: application.into(),
            tenant: "diagnostics-tenant".into(),
            principal: principal.into(),
        })
    }
}

pub struct OwnerPolicy;
impl ResourcePolicy for OwnerPolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        context.origin.as_ref().is_none_or(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}

pub fn script_route(effect_value: ScriptEffect, requires_approval: bool) -> ScriptRoute {
    let effect = EffectId::new("diagnostics-effect").unwrap();
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new(ROUTE).unwrap(),
            revision: ExecutionRevision::new("diagnostics-revision").unwrap(),
            max_attempts: 1,
            batches: vec![vec![TaskSpec {
                id: TaskId::new("diagnostics-task").unwrap(),
                effect_id: effect.clone(),
                requires_approval,
                recovery: RecoveryPolicy::Pure,
            }]],
        },
        effects: BTreeMap::from([(effect, effect_value)]),
    }
}

pub fn scripted_host<S: Store + 'static>(store: Arc<S>, config: HostConfig) -> HttpHost<S> {
    HttpHost::new(store, Arc::new(Auth), Arc::new(OwnerPolicy), config).unwrap()
}

pub fn memory_host() -> HttpHost<MemoryStore> {
    scripted_host(
        Arc::new(MemoryStore::new()),
        HostConfig::new(vec![script_route(ScriptEffect::Input, false)]),
    )
}

pub fn run_input(input: Value) -> String {
    json!({"route":ROUTE,"input":input}).to_string()
}

pub fn request(
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: &str,
) -> axum::http::Request<axum::body::Body> {
    let mut request = axum::http::Request::builder()
        .method(method)
        .uri(path)
        .header("x-b4-request-id", "private-caller-correlation")
        .header("cookie", "private-request-cookie");
    if let Some(authorization) = authorization {
        request = request.header("authorization", authorization);
    }
    request
        .body(axum::body::Body::from(body.to_owned()))
        .unwrap()
}

pub async fn response<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: &str,
) -> axum::response::Response {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        host.router()
            .oneshot(request(method, path, authorization, body)),
    )
    .await
    .expect("actual fallback must resolve within fixture bound")
    .unwrap()
}

pub async fn body(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), 262_144)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).expect("actual finite fallback response must contain JSON")
}

pub fn error_body(code: &str) -> Value {
    json!({"error":{"kind":"request_error","message":code,"details":{"code":code}}})
}
