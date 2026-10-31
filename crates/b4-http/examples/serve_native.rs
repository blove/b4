//! Immutable native SQLite application; no provider or external effect.
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{
    ApplicationIdentity, EffectBinding, EffectInvocation, EffectUncertain, InputValidator,
    InvalidApplicationInput, InvocationContext, NativeApplication, NativeEffect, NativeOutcome,
    OutputKind, RouteRegistration,
};
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
            application: "native-demo".into(),
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

struct ObjectInput;
impl InputValidator for ObjectInput {
    fn validate(&self, input: &serde_json::Value) -> Result<(), InvalidApplicationInput> {
        if input.is_null() || input.is_object() {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}
struct LocalEffect;
impl NativeEffect for LocalEffect {
    fn invoke(
        &self,
        call: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        let task = call.task_id.as_str();
        let (output, item) = match task {
            "first" | "second" => {
                // Finite native callbacks can overlap. State still joins in the
                // immutable definition order, regardless of completion order.
                std::thread::sleep(std::time::Duration::from_millis(if task == "first" {
                    120
                } else {
                    60
                }));
                (
                    json!({"task":task,"input":call.input.get("input").cloned().unwrap_or(serde_json::Value::Null)}),
                    task,
                )
            }
            "prepare" => (json!({"prepared":true}), "prepared"),
            "release" => (json!({"released":true}), "released"),
            _ => return Err(EffectUncertain),
        };
        Ok(NativeOutcome::Succeeded(TaskResult {
            output,
            writes: vec![StateWrite::Append {
                field: "items".into(),
                items: vec![json!(item)],
            }],
        }))
    }
}
fn registration(
    route: &str,
    revision: &str,
    tasks: &[(&str, bool)],
) -> Result<RouteRegistration, Box<dyn std::error::Error>> {
    let specs = tasks
        .iter()
        .map(|(id, required)| {
            Ok(TaskSpec {
                id: TaskId::new(*id)?,
                effect_id: EffectId::new(*id)?,
                requires_approval: *required,
                recovery: RecoveryPolicy::Pure,
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let effects = specs
        .iter()
        .map(|task| {
            (
                task.effect_id.clone(),
                EffectBinding {
                    callback: Arc::new(LocalEffect) as Arc<dyn NativeEffect>,
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    Ok(RouteRegistration {
        definition: Definition {
            route: RouteKey::new(route)?,
            revision: ExecutionRevision::new(revision)?,
            max_attempts: specs.len() as u32,
            batches: vec![specs],
        },
        effects,
        input_validator: Some(Arc::new(ObjectInput)),
    })
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
    let data = std::env::var("B4RUN_DATA_DIR").unwrap_or_else(|_| ".b4-native-data".into());
    let application = Arc::new(NativeApplication::new(
        ApplicationIdentity {
            application: "native-demo".into(),
            revision: "native-demo-v1".into(),
        },
        vec![
            registration(
                "/native#workflow",
                "native-parallel-v1",
                &[("first", false), ("second", false)],
            )?,
            registration(
                "/approve#workflow",
                "native-approval-v1",
                &[("prepare", false), ("release", true)],
            )?,
        ],
    )?);
    let approval = ApprovalHostConfig::new(
        "native-demo-policy-v1".into(),
        BTreeMap::from([(
            RouteKey::new("/approve#workflow")?,
            BTreeMap::from([(
                TaskId::new("release")?,
                json!({"kind":"approval","message":"Approve the local native release?"}),
            )]),
        )]),
    );
    let host = HttpHost::new_with_application(
        Arc::new(SqliteStore::open(data)?),
        Arc::new(TokenAuth(token)),
        Arc::new(OwnerPolicy),
        NativeHostConfig::new(application, approval),
    )?;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let router = host.router().layer(middleware::from_fn_with_state(
        Arc::new(address.to_string()),
        browser_guard,
    ));
    println!("Native HTTP application listening at http://{address}");
    axum::serve(listener, router).await?;
    Ok(())
}
