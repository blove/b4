//! Token-protected local approval fixture; no provider or external effect.
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use b4_contract::*;
use b4_http::*;
use b4_sqlite::SqliteStore;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

struct TokenAuth(String);
impl Authenticator for TokenAuth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get_all("authorization").iter().count() != 1
            || headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some(format!("Bearer {}", self.0).as_str())
        {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "approval-demo".into(),
            tenant: "local".into(),
            principal: "owner".into(),
        })
    }
}

struct OwnerPolicy;
impl ResourcePolicy for OwnerPolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        context.origin.as_ref().is_none_or(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}

async fn browser_guard(
    State(host): State<Arc<String>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.headers().contains_key("origin")
        || request.headers().get_all("host").iter().count() != 1
        || request
            .headers()
            .get("host")
            .and_then(|value| value.to_str().ok())
            != Some(host.as_str())
    {
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            r#"{"error":{"kind":"request_error","message":"request_origin_denied","details":{}}}"#,
        )
            .into_response();
    }
    next.run(request).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("B4RUN_TOKEN").map_err(|_| "B4RUN_TOKEN is required")?;
    if token.trim().is_empty() {
        return Err("B4RUN_TOKEN must not be empty".into());
    }
    let port = std::env::var("B4RUN_PORT")
        .unwrap_or_else(|_| "3001".into())
        .parse::<u16>()?;
    let data = std::env::var("B4RUN_DATA_DIR").unwrap_or_else(|_| ".b4-http-data".into());
    let route_key = RouteKey::new("/approve#workflow")?;
    let prepare = EffectId::new("prepare")?;
    let release = EffectId::new("release")?;
    let approval_task = TaskId::new("release")?;
    let route = ScriptRoute {
        definition: Definition {
            route: route_key.clone(),
            revision: ExecutionRevision::new("approval-fixture-v1")?,
            batches: vec![vec![
                TaskSpec {
                    id: TaskId::new("prepare")?,
                    effect_id: prepare.clone(),
                    requires_approval: false,
                    recovery: RecoveryPolicy::Pure,
                },
                TaskSpec {
                    id: approval_task.clone(),
                    effect_id: release.clone(),
                    requires_approval: true,
                    recovery: RecoveryPolicy::Pure,
                },
            ]],
            max_attempts: 2,
        },
        effects: BTreeMap::from([
            (
                prepare,
                ScriptEffect::Result(TaskResult {
                    output: json!({"prepared":true}),
                    writes: vec![StateWrite::Append {
                        field: "items".into(),
                        items: vec![json!("prepared")],
                    }],
                }),
            ),
            (
                release,
                ScriptEffect::Result(TaskResult {
                    output: json!({"released":true}),
                    writes: vec![StateWrite::Append {
                        field: "items".into(),
                        items: vec![json!("released")],
                    }],
                }),
            ),
        ]),
    };
    let approval = ApprovalHostConfig::new(
        "approval-demo-policy-v1".into(),
        BTreeMap::from([(
            route_key,
            BTreeMap::from([(
                approval_task,
                json!({"kind":"approval","message":"Approve the local release fixture?"}),
            )]),
        )]),
    );
    let host = HttpHost::new_with_approvals(
        Arc::new(SqliteStore::open(data)?),
        Arc::new(TokenAuth(token)),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![route]),
        approval,
    )?;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let router = host.router().layer(middleware::from_fn_with_state(
        Arc::new(address.to_string()),
        browser_guard,
    ));
    println!("Approval HTTP fixture listening at http://{address}");
    axum::serve(listener, router).await?;
    Ok(())
}
