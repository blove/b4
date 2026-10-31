#![allow(dead_code)]

mod callback;
mod gate;
pub mod policy;
pub mod store;
pub mod tcp;
pub use callback::*;
pub use gate::*;

use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tower::ServiceExt;

pub const RUN: &str = r#"{"route":"/cooperative#workflow","input":{"original":true}}"#;
pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get("authorization").and_then(|v| v.to_str().ok())
            != Some("Bearer cooperative-owner")
        {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "cooperative-test".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
    }
}
pub struct Owner;
impl ResourcePolicy for Owner {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        c.origin.as_ref().is_none_or(|o| {
            o.application == p.application && o.tenant == p.tenant && o.principal == p.principal
        })
    }
}
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
        let path = std::env::temp_dir().join(format!("b4-cooperative-{}", uuid::Uuid::new_v4()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        for file in ["owner.lock", "runtime.sqlite3"] {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path.join(file))
                .unwrap();
        }
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn definition() -> Definition {
    Definition {
        route: RouteKey::new("/cooperative#workflow").unwrap(),
        revision: ExecutionRevision::new("cooperative-v1").unwrap(),
        max_attempts: 4,
        batches: vec![vec![TaskSpec {
            id: TaskId::new("task").unwrap(),
            effect_id: EffectId::new("effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]],
    }
}
pub fn application(
    definition: Definition,
    callbacks: Vec<Arc<dyn NativeEffect>>,
) -> Arc<NativeApplication> {
    let tasks: Vec<_> = definition.batches.iter().flatten().collect();
    assert_eq!(tasks.len(), callbacks.len());
    Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "cooperative-test".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                effects: tasks
                    .iter()
                    .zip(callbacks)
                    .map(|(t, c)| {
                        (
                            t.effect_id.clone(),
                            EffectBinding {
                                callback: c,
                                output_kind: OutputKind::Value,
                            },
                        )
                    })
                    .collect(),
                definition,
                input_validator: None,
            }],
        )
        .unwrap(),
    )
}
pub fn config(app: Arc<NativeApplication>, durable: bool, capacity: usize) -> NativeHostConfig {
    let prompts = app
        .definitions()
        .map(|d| {
            (
                d.route.clone(),
                d.batches
                    .iter()
                    .flatten()
                    .filter(|t| t.requires_approval)
                    .map(|t| (t.id.clone(), json!({"question":"cooperative approval"})))
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .filter(|(_, tasks)| !tasks.is_empty())
        .collect();
    let mut approval = ApprovalHostConfig::new("policy-v1".into(), prompts);
    if !durable {
        approval.service_config = ApprovalServiceConfig::VolatileFixture;
    }
    let mut c = NativeHostConfig::new(app, approval);
    c.limits.max_active_runs = capacity;
    c.limits.max_tasks_per_execution = 2;
    c.limits.heartbeat_ms = 10;
    c
}
pub fn host<S: ApprovalStore + 'static>(store: Arc<S>, app: Arc<NativeApplication>) -> HttpHost<S> {
    let durable = store.approval_durable();
    HttpHost::new_with_application(
        store,
        Arc::new(Auth),
        Arc::new(Owner),
        config(app, durable, 1),
    )
    .unwrap()
}
pub async fn call<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    body: &str,
) -> axum::response::Response {
    tokio::time::timeout(
        Duration::from_secs(15),
        host.router().oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", "Bearer cooperative-owner")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        ),
    )
    .await
    .expect("finite cooperative API request")
    .unwrap()
}
pub async fn json_body(response: axum::response::Response) -> Value {
    let bytes = tokio::time::timeout(
        Duration::from_secs(10),
        to_bytes(response.into_body(), 100_000),
    )
    .await
    .unwrap()
    .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
pub async fn joined(
    handle: tokio::task::JoinHandle<axum::response::Response>,
) -> axum::response::Response {
    tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("finite owned request join")
        .unwrap()
}
pub fn assert_positive(gate: &Gate) {
    let facts = gate.facts();
    assert!(facts.explicit && !facts.expired && !facts.cleanup);
}
pub fn acknowledged(callback: &Callback) {
    assert!(callback
        .observed_acceptance
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(callback.returned.load(std::sync::atomic::Ordering::Acquire));
    assert_positive(&callback.gate);
}
pub fn snapshot<S: Store>(store: &S, id: &str) -> Snapshot {
    store
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
}
pub async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("cooperative fixture observable boundary");
}
pub async fn finish_host<S: Store + 'static>(host: &HttpHost<S>, faulted: bool) {
    assert_eq!(
        host.shutdown_handle()
            .wait_quiescent(Duration::from_secs(10))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted }
    );
}
pub async fn assert_cancel<S: Store + 'static>(host: &HttpHost<S>, id: &str) {
    let response = call(host, "POST", &format!("/threads/{id}/cancel"), "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await,
        json!({"status":"interrupted","thread_id":id})
    );
}
