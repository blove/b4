//! Selected comparison against an independently executed pinned TypeScript
//! workflow corpus. Authentication is added; request bodies and paths are kept.
use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{MemoryStore, Store};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers
            .get("authorization")
            .is_some_and(|h| h == "Bearer fixture")
        {
            Ok(Principal {
                application: "fixture".into(),
                tenant: "local".into(),
                principal: "owner".into(),
            })
        } else {
            Err(AuthenticationError)
        }
    }
}
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        c.origin.as_ref().is_none_or(|o| {
            o.application == p.application && o.tenant == p.tenant && o.principal == p.principal
        })
    }
}
fn route(name: &str, effect: ScriptEffect) -> ScriptRoute {
    let effect_id = EffectId::new("effect").unwrap();
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new(format!("/{name}#workflow")).unwrap(),
            revision: ExecutionRevision::new("fixture-v1").unwrap(),
            max_attempts: 1,
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: effect_id.clone(),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            }]],
        },
        effects: BTreeMap::from([(effect_id, effect)]),
    }
}
fn frames(text: &str) -> Vec<Value> {
    text.split("\n\n")
        .filter_map(|frame| {
            let event = frame.lines().find_map(|l| l.strip_prefix("event: "))?;
            let data = frame
                .lines()
                .find_map(|l| l.strip_prefix("data: "))
                .expect("JSON SSE data");
            Some(json!({"event":event, "data":serde_json::from_str::<Value>(data).unwrap()}))
        })
        .collect()
}

#[tokio::test]
async fn selected_successful_workflow_terminals_match_pinned_reference() {
    let captured: Value = serde_json::from_str(include_str!(
        "../../../conformance/reference/recordings/2026-09-30-workflows/workflow-observations.json"
    ))
    .unwrap();
    assert_eq!(
        captured["reference"]["commit"],
        "8acac534ba6eb960b34de217b0da45b8405023af"
    );
    let host = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Policy),
        HostConfig::new(vec![
            route("probe", ScriptEffect::ProbeInput),
            route("void", ScriptEffect::Void),
            route(
                "zero",
                ScriptEffect::Result(TaskResult {
                    output: json!(0),
                    writes: vec![],
                }),
            ),
            route(
                "false",
                ScriptEffect::Result(TaskResult {
                    output: json!(false),
                    writes: vec![],
                }),
            ),
            route(
                "empty",
                ScriptEffect::Result(TaskResult {
                    output: json!(""),
                    writes: vec![],
                }),
            ),
        ]),
    )
    .unwrap();
    let mut compared = 0;
    for case in captured["observations"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        if !(id.starts_with("wait-input-")
            || id.starts_with("stream-input-")
            || [
                "wait-void",
                "stream-void",
                "wait-zero",
                "stream-zero",
                "wait-false",
                "stream-false",
                "wait-empty",
                "stream-empty",
            ]
            .contains(&id))
        {
            continue;
        }
        let request = &case["request"];
        let mut builder = Request::builder()
            .method(request["method"].as_str().unwrap())
            .uri(request["path"].as_str().unwrap())
            .header("authorization", "Bearer fixture");
        for h in request["headers"].as_array().unwrap() {
            builder = builder.header(h[0].as_str().unwrap(), h[1].as_str().unwrap());
        }
        let response = host
            .router()
            .oneshot(
                builder
                    .body(Body::from(
                        request["body_utf8"].as_str().unwrap().to_owned(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            case["response"]["status"].as_u64().unwrap() as u16,
            "{id}"
        );
        let reference_content_type = case["response"]["headers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| h[0] == "content-type")
            .unwrap()[1]
            .as_str()
            .unwrap();
        // Axum may add charset to SSE. Media type equality is the selected claim.
        assert_eq!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap(),
            reference_content_type.split(';').next().unwrap(),
            "{id}"
        );
        let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
        if id.starts_with("stream-") {
            let expected: Vec<_> = case["response"]["sse_frames"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| json!({"event":f["event"], "data":f["data"]}))
                .collect();
            assert_eq!(
                frames(std::str::from_utf8(&bytes).unwrap()),
                expected,
                "{id}"
            );
        } else {
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap(),
                serde_json::from_str::<Value>(case["response"]["body_utf8"].as_str().unwrap())
                    .unwrap(),
                "{id}"
            );
        }
        let thread_id = request["path"].as_str().unwrap().split('/').nth(2).unwrap();
        let view = host
            .store()
            .thread_view(&ThreadId::new(thread_id).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            view.execution.unwrap().snapshot.status,
            ExecutionStatus::Completed,
            "{id}"
        );
        compared += 1;
    }
    assert_eq!(compared, 22, "scope must not silently shrink");
}

#[tokio::test]
async fn selected_failure_workflow_terminals_match_pinned_reference() {
    let captured: Value = serde_json::from_str(include_str!(
        "../../../conformance/reference/recordings/2026-09-30-workflows/workflow-observations.json"
    ))
    .unwrap();
    assert_eq!(
        captured["reference"]["commit"],
        "8acac534ba6eb960b34de217b0da45b8405023af"
    );
    let host = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Policy),
        HostConfig::new(vec![route(
            "fail",
            ScriptEffect::Failure("fixture execution failure".into()),
        )]),
    )
    .unwrap();
    let mut compared = 0;
    for case in captured["observations"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        if !["wait-fail", "stream-fail"].contains(&id) {
            continue;
        }
        let request = &case["request"];
        let mut builder = Request::builder()
            .method(request["method"].as_str().unwrap())
            .uri(request["path"].as_str().unwrap())
            .header("authorization", "Bearer fixture");
        for h in request["headers"].as_array().unwrap() {
            builder = builder.header(h[0].as_str().unwrap(), h[1].as_str().unwrap());
        }
        let response = host
            .router()
            .oneshot(
                builder
                    .body(Body::from(
                        request["body_utf8"].as_str().unwrap().to_owned(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            u64::from(response.status().as_u16()),
            case["response"]["status"].as_u64().unwrap(),
            "{id}"
        );
        let reference_media_type = case["response"]["headers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| h[0] == "content-type")
            .unwrap()[1]
            .as_str()
            .unwrap();
        assert_eq!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap(),
            reference_media_type.split(';').next().unwrap(),
            "{id}"
        );
        let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
        if id == "stream-fail" {
            let expected: Vec<_> = case["response"]["sse_frames"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| json!({"event":f["event"], "data":f["data"]}))
                .collect();
            assert_eq!(
                frames(std::str::from_utf8(&bytes).unwrap()),
                expected,
                "{id}"
            );
        } else {
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap(),
                serde_json::from_str::<Value>(case["response"]["body_utf8"].as_str().unwrap())
                    .unwrap(),
                "{id}"
            );
        }
        let thread_id = request["path"].as_str().unwrap().split('/').nth(2).unwrap();
        let view = host
            .store()
            .thread_view(&ThreadId::new(thread_id).unwrap())
            .unwrap()
            .unwrap();
        let snapshot = view.execution.unwrap().snapshot;
        assert_eq!(snapshot.status, ExecutionStatus::Failed);
        assert_eq!(
            snapshot.failure.as_deref(),
            Some("fixture execution failure")
        );
        assert!(snapshot.output.is_none());
        assert_eq!(snapshot.tasks[0].status, TaskStatus::Failed);
        assert!(snapshot.tasks[0].result.is_none());
        let ready = host
            .router()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready.status(), 200);
        compared += 1;
    }
    assert_eq!(
        compared, 2,
        "failure comparison scope must not silently shrink"
    );
}
