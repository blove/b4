//! Selected required-grant comparisons against independently captured production
//! TypeScript behavior. These exercise the native scripted HTTP host, not its
//! agent/model adapter. Generated grants and interrupts are rebound by prompt;
//! raw recordings stay unchanged. Status/code comparisons do not imply matching
//! agent output, denial wrappers, token encoding or whole-protocol conformance.
use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, ExecutionFence, MemoryStore, Runtime, Store};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

const ROUTE: &str = "/park#agent";
const THREAD: &str = "grant-reference";

fn capture() -> Value {
    let capture: Value = serde_json::from_str(include_str!(
        "../../../conformance/reference/recordings/2026-10-01-grants/grant-observations.json"
    ))
    .unwrap();
    assert_eq!(
        capture["reference"]["commit"],
        "8acac534ba6eb960b34de217b0da45b8405023af"
    );
    assert_eq!(capture["observations"].as_array().unwrap().len(), 39);
    capture
}

fn observation<'a>(capture: &'a Value, id: &str) -> &'a Value {
    capture["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == id)
        .unwrap_or_else(|| panic!("selected reference observation missing: {id}"))
}

fn reference_body(case: &Value) -> Value {
    serde_json::from_str(case["response"]["body_utf8"].as_str().unwrap()).unwrap()
}

fn reference_entries(capture: &Value, id: &str) -> Vec<Value> {
    reference_body(observation(capture, id))["interrupts"]
        .as_array()
        .unwrap()
        .clone()
}

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers
            .get("authorization")
            .is_some_and(|header| header == "Bearer fixture")
        {
            Ok(Principal {
                application: "grant-reference".into(),
                tenant: "local".into(),
                principal: "owner".into(),
            })
        } else {
            Err(AuthenticationError)
        }
    }
}

struct Owner;
impl ResourcePolicy for Owner {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        context.origin.as_ref().is_none_or(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}

fn prompt(env: &str) -> Value {
    json!({
        "type":"permission-request", "kind":"tool",
        "detail":{
            "toolName":"deployProd", "argsPreview":json!({"env":env}).to_string(),
            "suggestedPattern":"deployProd"
        }
    })
}

fn host<S: ApprovalStore + 'static>(store: Arc<S>, pair: bool) -> HttpHost<S> {
    let environments = if pair {
        vec!["pair-a", "pair-b"]
    } else {
        vec!["staging"]
    };
    let mut tasks = vec![];
    let mut effects = BTreeMap::new();
    let mut prompts = BTreeMap::new();
    for environment in environments {
        let task = TaskId::new(environment).unwrap();
        let effect = EffectId::new(environment).unwrap();
        tasks.push(TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            requires_approval: true,
            recovery: RecoveryPolicy::Pure,
        });
        prompts.insert(task, prompt(environment));
        effects.insert(
            effect,
            ScriptEffect::Result(TaskResult {
                output: json!(format!("deployed to {environment}")),
                writes: vec![StateWrite::Append {
                    field: "ledger".into(),
                    items: vec![json!(environment)],
                }],
            }),
        );
    }
    let route = RouteKey::new(ROUTE).unwrap();
    let scripted = ScriptRoute {
        definition: Definition {
            route: route.clone(),
            revision: ExecutionRevision::new("grant-fixture-v1").unwrap(),
            max_attempts: tasks.len() as u32,
            batches: vec![tasks],
        },
        effects,
    };
    let mut approval =
        ApprovalHostConfig::new("grant-policy-v1".into(), BTreeMap::from([(route, prompts)]));
    if !store.approval_durable() {
        approval.service_config = ApprovalServiceConfig::VolatileFixture;
    }
    HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        Arc::new(Owner),
        HostConfig::new(vec![scripted]),
        approval,
    )
    .unwrap()
}

struct Reply {
    status: u16,
    body: String,
}
impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }
}

async fn request<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    suffix: &str,
    body: &str,
) -> Reply {
    let response = host
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(format!("/threads/{THREAD}{suffix}"))
                .header("authorization", "Bearer fixture")
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = to_bytes(response.into_body(), 200_000).await.unwrap();
    Reply {
        status,
        body: String::from_utf8(bytes.to_vec()).unwrap(),
    }
}

async fn resume_after_claim_release<S: Store + 'static>(host: &HttpHost<S>, body: &str) -> Reply {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let reply = request(host, "POST", "/resume", body).await;
        if reply.status != 409 || reply.json()["error"]["details"]["code"] != "thread_busy" {
            return reply;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "owned claim did not release"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

async fn park<S: Store + 'static>(host: &HttpHost<S>) -> Vec<Value> {
    let parked = request(
        host,
        "POST",
        "/runs/wait",
        &json!({"route":ROUTE,"input":{"messages":[]}}).to_string(),
    )
    .await;
    assert_eq!(parked.status, 200);
    assert_eq!(parked.body, "null");
    pending(host).await.json()["interrupts"]
        .as_array()
        .unwrap()
        .clone()
}

async fn pending<S: Store + 'static>(host: &HttpHost<S>) -> Reply {
    let reply = request(host, "GET", "/pending_interrupts", "").await;
    assert_eq!(reply.status, 200);
    reply
}

fn snapshot<S: Store + 'static>(host: &HttpHost<S>) -> Snapshot {
    host.store()
        .thread_view(&ThreadId::new(THREAD).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
}

// Bind the recorded request's actual opaque fields to the corresponding native
// park. Wrong grants/IDs and decision payloads are retained exactly as recorded.
fn rebind(case: &Value, source: &[Value], native: &[Value]) -> String {
    let mut body: Value =
        serde_json::from_str(case["request"]["body_utf8"].as_str().unwrap()).unwrap();
    for entry in body["resume"].as_array_mut().unwrap() {
        let Some(original) = source
            .iter()
            .find(|pending| pending["interruptId"] == entry["interruptId"])
        else {
            continue;
        };
        let replacement = native
            .iter()
            .find(|pending| pending["value"]["detail"] == original["value"]["detail"])
            .expect("each captured proposal has one native scripted counterpart");
        entry["interruptId"] = replacement["interruptId"].clone();
        if entry.get("grant") == original.get("grant") {
            entry["grant"] = replacement["grant"].clone();
        }
    }
    body.to_string()
}

fn same_status(case: &Value, native: &Reply) {
    assert_eq!(
        u64::from(native.status),
        case["response"]["status"].as_u64().unwrap(),
        "selected status: {}",
        case["id"].as_str().unwrap()
    );
}

fn same_refusal(case: &Value, native: &Reply) {
    same_status(case, native);
    assert_eq!(
        native.json()["error"]["details"]["code"],
        reference_body(case)["error"]["details"]["code"],
        "selected refusal code: {}",
        case["id"].as_str().unwrap()
    );
    assert!(
        !native.body.contains("b4ag_"),
        "refusal must not echo a grant"
    );
}

fn assert_replay(case: &Value, native: &Reply) {
    same_status(case, native);
    // Both refuse settled replay. Native recovery/park vocabulary differs;
    // retain that distinction rather than normalizing the error codes.
    assert_eq!(native.status, 409);
    assert_eq!(
        reference_body(case)["error"]["details"]["code"],
        "stale_interrupt"
    );
    assert_eq!(native.json()["error"]["details"]["code"], "resume_required");
    assert!(!native.body.contains("b4ag_"));
}

fn one_terminal(case: &Value, native: &Reply) {
    same_status(case, native);
    assert_eq!(native.body.matches("event: done").count(), 1);
    assert_eq!(
        case["response"]["sse_frames"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|frame| frame["event"] == "done")
            .count(),
        1
    );
    assert!(!native.body.contains("b4ag_"));
    let source_done = case["response"]["sse_frames"]
        .as_array()
        .unwrap()
        .iter()
        .find(|frame| frame["event"] == "done")
        .unwrap();
    let native_done: Value = native
        .body
        .split("\n\n")
        .find(|frame| frame.lines().any(|line| line == "event: done"))
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).unwrap())
        .unwrap();
    // The real agent returns a messages object; the scripted native host returns
    // the effect result or its denial wrapper. Only terminal count/status match.
    assert!(source_done["data"]["output"]["messages"].is_array());
    assert!(native_done["output"].get("messages").is_none());
}

#[tokio::test]
async fn selected_grant_refusals_match_reference_without_consuming_any_decision() {
    let capture = capture();
    let mut compared = 0;
    for (pair, pending_id, cases) in [
        (
            false,
            "once.pending",
            vec![
                "once.resume-missing",
                "once.resume-malformed",
                "once.resume-wrong",
                "once.resume-wrong-id",
            ],
        ),
        (
            true,
            "pair.pending",
            vec!["pair.resume-incomplete", "pair.resume-one-invalid"],
        ),
    ] {
        let host = host(Arc::new(MemoryStore::new()), pair);
        let native = park(&host).await;
        let source = reference_entries(&capture, pending_id);
        let before = snapshot(&host);
        for id in cases {
            let case = observation(&capture, id);
            let refused = resume_after_claim_release(&host, &rebind(case, &source, &native)).await;
            same_refusal(case, &refused);
            assert_eq!(snapshot(&host), before, "refusal may not change task state");
            let grants = host
                .store()
                .approval_view(&before.execution_id)
                .unwrap()
                .unwrap();
            assert!(grants.grants.iter().all(|grant| !grant.consumed));
            assert!(grants.acceptances.is_empty());
            compared += 1;
        }
        if pair {
            let accepted = observation(&capture, "pair.resume-accepted");
            let reply =
                resume_after_claim_release(&host, &rebind(accepted, &source, &native)).await;
            one_terminal(accepted, &reply);
            let completed = snapshot(&host);
            assert_eq!(completed.status, ExecutionStatus::Completed);
            let source_calls = accepted["ledger"].as_array().unwrap().len()
                - observation(&capture, pending_id)["ledger"]
                    .as_array()
                    .unwrap()
                    .len();
            assert_eq!(source_calls, 2);
            assert_eq!(completed.attempts_used as usize, source_calls);
            assert_eq!(completed.values["ledger"], json!(["pair-a", "pair-b"]));
            let replay = observation(&capture, "pair.resume-replay");
            let refused =
                resume_after_claim_release(&host, &rebind(replay, &source, &native)).await;
            assert_replay(replay, &refused);
            assert_eq!(snapshot(&host), completed);
            compared += 2;
        }
    }
    assert_eq!(
        compared, 8,
        "six refusals, one pair acceptance and one replay"
    );
}

#[tokio::test]
async fn selected_once_deny_and_cancelled_match_execution_counts_and_replay_status() {
    let capture = capture();
    let mut compared = 0;
    for (prefix, accepted_id, expected_calls) in [
        ("once", "once.resume-accepted", 1),
        ("deny", "deny.resume", 0),
        ("cancelled", "cancelled.resume", 0),
    ] {
        let host = host(Arc::new(MemoryStore::new()), false);
        let native = park(&host).await;
        let pending_id = format!("{prefix}.pending");
        let source = reference_entries(&capture, &pending_id);
        let accepted = observation(&capture, accepted_id);
        let reply = resume_after_claim_release(&host, &rebind(accepted, &source, &native)).await;
        one_terminal(accepted, &reply);
        let source_calls = accepted["ledger"].as_array().unwrap().len()
            - observation(&capture, &pending_id)["ledger"]
                .as_array()
                .unwrap()
                .len();
        assert_eq!(source_calls, expected_calls);
        let completed = snapshot(&host);
        assert_eq!(completed.status, ExecutionStatus::Completed);
        assert_eq!(completed.attempts_used as usize, source_calls);
        if expected_calls == 1 {
            assert_eq!(completed.values["ledger"], json!(["staging"]));
            assert_eq!(completed.tasks[0].status, TaskStatus::Succeeded);
        } else {
            assert_eq!(completed.tasks[0].status, TaskStatus::Denied);
            assert!(completed.tasks[0].result.is_none());
            assert!(completed.values.get("ledger").is_none());
        }
        let replay = observation(&capture, &format!("{prefix}.resume-replay"));
        let refused = resume_after_claim_release(&host, &rebind(replay, &source, &native)).await;
        assert_replay(replay, &refused);
        assert_eq!(snapshot(&host), completed);
        compared += 2;
    }
    assert_eq!(compared, 6, "three decisions and their settled replays");
}

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("b4-grant-reference-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn repeated_pending_and_graceful_reopen_preserve_each_runtime_opaque_identity() {
    let capture = capture();
    for (before, after) in [
        ("once.pending", "once.pending-again"),
        ("once.pending-before-restart", "once.pending-after-restart"),
    ] {
        assert!(
            observation(&capture, before)["response"]["body_utf8"]
                == observation(&capture, after)["response"]["body_utf8"],
            "reference preserves raw pending identity across {before}/{after}"
        );
    }
    let directory = Directory::new();
    let initial = host(
        Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap()),
        false,
    );
    park(&initial).await;
    let first = pending(&initial).await;
    let again = pending(&initial).await;
    assert!(
        first.body == again.body,
        "native repeated pending is byte-identical"
    );
    let parked = snapshot(&initial);
    drop(initial);

    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let restored = store.execution(&parked.execution_id).unwrap().unwrap();
    // Native reopen requires explicit recovery of a known ID, unlike the source
    // handler's automatic restoration. Compare identity after that trusted step.
    Runtime::new(store.clone())
        .recover(
            &parked.execution_id,
            ExecutionFence::from(&restored.snapshot),
            parked.definition.revision.clone(),
            TurnId::new("grant-reference-recovery").unwrap(),
            TransitionFacts {
                now_ms: restored.snapshot.updated_at_ms + 1,
            },
        )
        .unwrap();
    let reopened = host(store, false);
    let after = pending(&reopened).await;
    assert!(
        first.body == after.body,
        "native reopen preserves grant/prompt/resume identity"
    );
    assert_eq!(snapshot(&reopened).attempts_used, 0);
}

#[tokio::test]
async fn route_token_encoding_and_prompt_wrappers_remain_explicit_reference_differences() {
    let capture = capture();
    let host = host(Arc::new(MemoryStore::new()), false);
    let native = park(&host).await;
    let source = reference_entries(&capture, "once.pending");
    let source_grant = source[0]["grant"].as_str().unwrap();
    let native_grant = native[0]["grant"].as_str().unwrap();
    assert!(source_grant.starts_with("b4ag_") && source_grant.len() == 48);
    assert!(native_grant.starts_with("b4ag_") && native_grant.len() == 69);
    assert!(source_grant[5..]
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'));
    assert!(native_grant[5..]
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
    assert!(
        source[0]["value"]["grant"] == source[0]["grant"],
        "source prompt embeds its opaque grant"
    );
    assert!(native[0]["value"].get("grant").is_none());
    assert!(native[0]["value"].get("interruptId").is_none());
    for field in ["type", "kind", "detail"] {
        assert_eq!(native[0]["value"][field], source[0]["value"][field]);
    }

    let route_source = reference_entries(&capture, "route.pending");
    let case = observation(&capture, "route.resume-supplied-route-differs");
    assert_eq!(case["response"]["status"], 200);
    let before = snapshot(&host);
    let refused = resume_after_claim_release(&host, &rebind(case, &route_source, &native)).await;
    assert_eq!(refused.status, 409);
    assert_eq!(refused.json()["error"]["details"]["code"], "route_mismatch");
    assert_eq!(snapshot(&host), before);
    assert!(host
        .store()
        .approval_view(&before.execution_id)
        .unwrap()
        .unwrap()
        .grants
        .iter()
        .all(|grant| !grant.consumed));
}
