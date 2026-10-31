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
use std::{collections::BTreeMap, sync::Arc};

struct TokenAuth {
    token: String,
}
impl Authenticator for TokenAuth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get_all("authorization").iter().count() != 1 {
            return Err(AuthenticationError);
        }
        let value = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or(AuthenticationError)?;
        if value.strip_prefix("Bearer ") != Some(self.token.as_str()) {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "scripted-demo".into(),
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
    let valid = !request.headers().contains_key("origin")
        && request.headers().get_all("host").iter().count() == 1
        && request.headers().get("host").and_then(|h| h.to_str().ok()) == Some(host.as_str());
    if !valid {
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            r#"{"error":{"kind":"request_error","message":"request_origin_denied","details":{}}}"#,
        )
            .into_response();
    }
    next.run(request).await
}
fn route(name: &str, effect: ScriptEffect) -> ScriptRoute {
    let effect_id = EffectId::new(name).unwrap();
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new(format!("/{name}#workflow")).unwrap(),
            revision: ExecutionRevision::new("scripted-v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new(format!("{name}-task")).unwrap(),
                effect_id: effect_id.clone(),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            }]],
            max_attempts: 1,
        },
        effects: BTreeMap::from([(effect_id, effect)]),
    }
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
    let routes = vec![
        route("probe", ScriptEffect::ProbeInput),
        route("void", ScriptEffect::Void),
        route(
            "delay",
            ScriptEffect::Delayed {
                duration_ms: 2_000,
                result: TaskResult {
                    output: serde_json::json!({"finished":true}),
                    writes: vec![StateWrite::Append {
                        field: "calls".into(),
                        items: vec![serde_json::json!("finished")],
                    }],
                },
            },
        ),
        route(
            "fail",
            ScriptEffect::Failure("fixture execution failure".into()),
        ),
        route(
            "false",
            ScriptEffect::Result(TaskResult {
                output: serde_json::json!(false),
                writes: vec![],
            }),
        ),
        route(
            "zero",
            ScriptEffect::Result(TaskResult {
                output: serde_json::json!(0),
                writes: vec![],
            }),
        ),
        route(
            "empty",
            ScriptEffect::Result(TaskResult {
                output: serde_json::json!(""),
                writes: vec![],
            }),
        ),
    ];
    let host = HttpHost::new(
        Arc::new(SqliteStore::open(data)?),
        Arc::new(TokenAuth { token }),
        Arc::new(OwnerPolicy),
        HostConfig::new(routes),
    )?;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let router = host.router().layer(middleware::from_fn_with_state(
        Arc::new(address.to_string()),
        browser_guard,
    ));
    println!("Scripted HTTP subset listening at http://{address}");
    axum::serve(listener, router).await?;
    Ok(())
}
