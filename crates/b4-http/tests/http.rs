use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{MemoryStore, Store};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Principal, AuthenticationError> {
        match headers.get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer owner") => Ok(Principal {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
            Some("Bearer other") => Ok(Principal {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "other".into(),
            }),
            _ => Err(AuthenticationError),
        }
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
fn route(effect: ScriptEffect) -> ScriptRoute {
    let id = EffectId::new("echo").unwrap();
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/echo#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            max_attempts: 1,
            batches: vec![vec![TaskSpec {
                id: TaskId::new("echo-task").unwrap(),
                effect_id: id.clone(),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            }]],
        },
        effects: BTreeMap::from([(id, effect)]),
    }
}
fn host(effect: ScriptEffect) -> HttpHost<MemoryStore> {
    HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![route(effect)]),
    )
    .unwrap()
}
async fn request<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = host
        .router()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn create(host: &HttpHost<MemoryStore>) -> String {
    let (status, body) = request(
        host,
        "POST",
        "/threads",
        Some("owner"),
        r#"{"metadata":{"tenant":"forged","note":"hello"}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    body["thread_id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn authenticated_origin_controls_reads_and_user_metadata_does_not() {
    let h = host(ScriptEffect::Input);
    assert_eq!(
        request(&h, "POST", "/threads", None, "{}").await.0,
        StatusCode::UNAUTHORIZED
    );
    let id = create(&h).await;
    let first = request(&h, "GET", &format!("/threads/{id}"), Some("owner"), "").await;
    let second = request(&h, "GET", &format!("/threads/{id}"), Some("owner"), "").await;
    assert_eq!(first, second);
    assert_eq!(first.1["metadata"]["note"], "hello");
    let denied = request(&h, "GET", &format!("/threads/{id}"), Some("other"), "").await;
    assert_eq!(denied.0, StatusCode::NOT_FOUND);
    assert!(!denied.1.to_string().contains("hello"));
    let origin = h
        .store()
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap()
        .thread
        .origin
        .unwrap();
    assert_eq!(origin.tenant, "tenant");
}

#[tokio::test]
async fn fresh_thread_has_no_fabricated_checkpoint_and_legacy_origin_is_denied() {
    let h = host(ScriptEffect::Input);
    let id = create(&h).await;
    assert_eq!(
        request(
            &h,
            "GET",
            &format!("/threads/{id}/state"),
            Some("owner"),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    h.store()
        .create_thread(ThreadId::new("legacy").unwrap())
        .unwrap();
    assert_eq!(
        request(&h, "GET", "/threads/legacy", Some("owner"), "")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn unsupported_thread_operations_cannot_probe_unauthorized_existence() {
    let h = host(ScriptEffect::Input);
    let id = create(&h).await;
    for suffix in ["/runs/cancel", "/runs/attach", "/runs/resume"] {
        let denied = request(
            &h,
            "DELETE",
            &format!("/threads/{id}{suffix}"),
            Some("other"),
            "",
        )
        .await;
        let missing = request(
            &h,
            "DELETE",
            &format!("/threads/absent{suffix}"),
            Some("other"),
            "",
        )
        .await;
        assert_eq!(denied, missing);
        assert_eq!(denied.0, StatusCode::NOT_FOUND);
    }
    assert_eq!(
        request(
            &h,
            "DELETE",
            &format!("/threads/{id}/runs/attach"),
            Some("owner"),
            ""
        )
        .await
        .0,
        StatusCode::NOT_IMPLEMENTED
    );
}

#[tokio::test]
async fn strict_parser_returns_ap_errors_without_content_type_requirement() {
    let h = host(ScriptEffect::Input);
    let id = create(&h).await;
    for body in [
        "",
        "not json",
        r#"{"route":"/echo#workflow","unknown":1}"#,
        r#"{"route":"/echo#workflow","input":{"x":1,"x":2}}"#,
    ] {
        let (status, body) = request(
            &h,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            Some("owner"),
            body,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["kind"], "request_error");
        assert!(body["error"]["details"]["code"].is_string());
        assert!(
            body["error"].get("code").is_none(),
            "protocol diagnostics must not invent registry codes"
        );
    }
    for (input, expected) in [
        ("", json!({})),
        (",\"input\":null", Value::Null),
        (",\"input\":false", json!(false)),
        (",\"input\":[1]", json!([1])),
    ] {
        let (status, output) = request(
            &h,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            Some("owner"),
            &format!("{{\"route\":\"/echo#workflow\"{input}}}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(output, expected);
    }
}

#[tokio::test]
async fn stream_has_direct_data_one_committed_terminal_and_pending_is_no_store() {
    let h = host(ScriptEffect::Result(TaskResult {
        output: json!(false),
        writes: vec![],
    }));
    let id = create(&h).await;
    let response = h
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/threads/{id}/runs/stream"))
                .header("authorization", "Bearer owner")
                .body(Body::from(r#"{"route":"/echo#workflow"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
    let body = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(body.matches("event: done").count(), 1);
    assert!(body.contains("data: {\"output\":false}"));
    assert!(!body.contains("\"type\""));
    let response = h
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/threads/{id}/pending_interrupts"))
                .header("authorization", "Bearer owner")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap(),
        json!({"interrupts":[]})
    );
    let state = request(
        &h,
        "GET",
        &format!("/threads/{id}/state"),
        Some("owner"),
        "",
    )
    .await
    .1;
    assert!(state.get("schema_version").is_none());
    assert_eq!(state["config"]["configurable"]["thread_id"], id);
}

#[tokio::test]
async fn void_and_falsy_are_settled_without_losing_values() {
    for (effect, expected_status, expected) in [
        (ScriptEffect::Void, StatusCode::OK, Value::Null),
        (
            ScriptEffect::Result(TaskResult {
                output: json!(0),
                writes: vec![],
            }),
            StatusCode::OK,
            json!(0),
        ),
    ] {
        let h = host(effect);
        let id = create(&h).await;
        let (status, body) = request(
            &h,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            Some("owner"),
            r#"{"route":"/echo#workflow"}"#,
        )
        .await;
        assert_eq!((status, body), (expected_status, expected));
        let view = h
            .store()
            .thread_view(&ThreadId::new(id).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            view.execution.unwrap().snapshot.status,
            ExecutionStatus::Completed
        );
        assert_eq!(
            request(&h, "GET", "/readyz", None, "").await.0,
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn expected_failure_is_committed_idle_healthy_and_allows_successor() {
    let mut success = route(ScriptEffect::Input);
    success.definition.route = RouteKey::new("/success#workflow").unwrap();
    let h = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![
            route(ScriptEffect::Failure("fixture execution failure".into())),
            success,
        ]),
    )
    .unwrap();
    let id = create(&h).await;
    let (status, body) = request(
        &h,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        Some("owner"),
        r#"{"route":"/echo#workflow"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        body,
        json!({"error":{"kind":"execution_error","message":"fixture execution failure"}})
    );
    let snapshot = h
        .store()
        .thread_view(&ThreadId::new(id.clone()).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Failed);
    assert_eq!(
        snapshot.failure.as_deref(),
        Some("fixture execution failure")
    );
    assert!(snapshot.output.is_none());
    assert_eq!(snapshot.tasks[0].status, TaskStatus::Failed);
    assert_eq!(snapshot.tasks[0].failure, snapshot.failure);
    assert!(snapshot.tasks[0].result.is_none());
    assert_eq!(
        request(&h, "GET", &format!("/threads/{id}"), Some("owner"), "")
            .await
            .1["status"],
        "idle"
    );
    assert_eq!(
        request(&h, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
    let successor = request(
        &h,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        Some("owner"),
        r#"{"route":"/success#workflow","input":false}"#,
    )
    .await;
    assert_eq!(successor, (StatusCode::OK, json!(false)));
}

#[tokio::test]
async fn failure_stops_remaining_tasks_and_batches_without_reducing_saved_results() {
    let mut scripted = route(ScriptEffect::Failure("expected failure".into()));
    let first = EffectId::new("first").unwrap();
    let unrun = EffectId::new("unrun").unwrap();
    let later = EffectId::new("later").unwrap();
    let spec = |name: &str, effect_id: EffectId| TaskSpec {
        id: TaskId::new(name).unwrap(),
        effect_id,
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    };
    scripted.definition.batches[0].insert(0, spec("first-task", first.clone()));
    scripted.definition.batches[0].push(spec("unrun-task", unrun.clone()));
    scripted
        .definition
        .batches
        .push(vec![spec("later-task", later.clone())]);
    scripted.definition.max_attempts = 4;
    scripted.effects.insert(
        first,
        ScriptEffect::Result(TaskResult {
            output: json!("retained"),
            writes: vec![StateWrite::Replace {
                field: "should-not-apply".into(),
                value: json!(true),
            }],
        }),
    );
    scripted.effects.insert(
        unrun,
        ScriptEffect::Result(TaskResult {
            output: json!("not-called"),
            writes: vec![],
        }),
    );
    scripted.effects.insert(
        later,
        ScriptEffect::Result(TaskResult {
            output: json!("not-called-later"),
            writes: vec![],
        }),
    );
    let mut config = HostConfig::new(vec![scripted]);
    config.max_tasks_per_execution = 1;
    let h = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        config,
    )
    .unwrap();
    let id = create(&h).await;
    assert_eq!(
        request(
            &h,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            Some("owner"),
            r#"{"route":"/echo#workflow"}"#
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let s = h
        .store()
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(s.status, ExecutionStatus::Failed);
    assert_eq!(s.attempts_used, 2);
    assert_eq!(s.batch_index, 0);
    assert_eq!(s.tasks.len(), 3);
    assert_eq!(
        s.tasks[0].result.as_ref().unwrap().output,
        json!("retained")
    );
    assert_eq!(s.tasks[1].status, TaskStatus::Failed);
    assert_eq!(s.tasks[2].status, TaskStatus::Cancelled);
    assert!(s.tasks[2].attempt_id.is_none());
    assert!(!s.values.contains_key("should-not-apply"));
    assert!(s.output.is_none());
}

#[test]
fn failure_fixture_is_bounded_by_utf8_bytes_and_both_encoded_envelopes() {
    let new_host = |reason: String, max_output_bytes| {
        let mut config = HostConfig::new(vec![route(ScriptEffect::Failure(reason))]);
        config.max_output_bytes = max_output_bytes;
        HttpHost::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(OwnerPolicy),
            config,
        )
    };
    assert!(new_host(String::new(), 65_536).is_err());
    assert!(new_host("é".repeat(512), 65_536).is_ok());
    assert!(new_host("é".repeat(513), 65_536).is_err());
    let reason = "a\n\"\\".repeat(20);
    let wait = json!({"error":{"kind":"execution_error","message":reason}});
    let done = json!({"output":{"error":reason}});
    let bound = serde_json::to_vec(&wait)
        .unwrap()
        .len()
        .max(serde_json::to_vec(&done).unwrap().len());
    assert!(new_host(reason.clone(), bound).is_ok());
    assert!(new_host(reason, bound - 1).is_err());
}

#[tokio::test]
async fn committed_reducer_failure_uses_saved_public_reason_and_stays_ready() {
    let h = host(ScriptEffect::Result(TaskResult {
        output: json!("retained"),
        writes: vec![StateWrite::Append {
            field: "input".into(),
            items: vec![json!(true)],
        }],
    }));
    let id = create(&h).await;
    let terminal = request(
        &h,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        Some("owner"),
        r#"{"route":"/echo#workflow"}"#,
    )
    .await;
    assert_eq!(
        terminal,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":{"kind":"execution_error","message":"append_target_not_array"}})
        )
    );
    let s = h
        .store()
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(s.status, ExecutionStatus::Failed);
    assert_eq!(s.failure.as_deref(), Some("append_target_not_array"));
    assert!(s.output.is_none());
    assert_eq!(s.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        s.tasks[0].result.as_ref().unwrap().output,
        json!("retained")
    );
    assert!(s.tasks[0].failure.is_none());
    assert_eq!(
        request(&h, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn slow_viewer_overflow_finishes_persistence_and_detaches_without_terminal_claim() {
    let effect = ScriptEffect::Chunks {
        chunks: (0..64).map(|i| json!(i)).collect(),
        result: TaskResult {
            output: json!("settled"),
            writes: vec![],
        },
    };
    let mut config = HostConfig::new(vec![route(effect)]);
    config.viewer_queue_frames = 1;
    let h = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        config,
    )
    .unwrap();
    let id = create(&h).await;
    let response = h
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/threads/{id}/runs/stream"))
                .header("authorization", "Bearer owner")
                .body(Body::from(r#"{"route":"/echo#workflow"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let view = h
        .store()
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        view.execution.unwrap().snapshot.status,
        ExecutionStatus::Completed
    );
    let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
    let body = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!(body, "event: detached\ndata: {\"reason\":\"overflow\"}\n\n");
    assert!(!body.contains("event: chunk"));
    assert!(!body.contains("event: done"));
    assert_eq!(
        request(&h, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn finite_capacity_rejects_before_implicit_thread_creation() {
    let effect = ScriptEffect::Delayed {
        duration_ms: 150,
        result: TaskResult {
            output: json!("done"),
            writes: vec![],
        },
    };
    let mut config = HostConfig::new(vec![route(effect)]);
    config.max_active_runs = 1;
    let h = HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        config,
    )
    .unwrap();
    let id = create(&h).await;
    let response = h
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/threads/{id}/runs/stream"))
                .header("authorization", "Bearer owner")
                .body(Body::from(r#"{"route":"/echo#workflow"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, _) = request(
        &h,
        "POST",
        "/threads/not-created/runs/wait",
        Some("owner"),
        r#"{"route":"/echo#workflow"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(h
        .store()
        .thread_view(&ThreadId::new("not-created").unwrap())
        .unwrap()
        .is_none());
    drop(response);
    tokio::time::sleep(std::time::Duration::from_millis(180)).await;
    assert_eq!(
        request(&h, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

#[test]
fn configuration_rejects_approval_missing_fixture_and_unbounded_limits() {
    let mut r = route(ScriptEffect::Input);
    r.definition.batches[0][0].requires_approval = true;
    assert!(HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![r])
    )
    .is_err());
    let mut r = route(ScriptEffect::Input);
    r.effects.clear();
    assert!(HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![r])
    )
    .is_err());
}

#[test]
fn configuration_requires_enough_attempts_and_finite_positive_limits() {
    let mut r = route(ScriptEffect::Input);
    let effect = EffectId::new("second").unwrap();
    r.definition.batches[0].push(TaskSpec {
        id: TaskId::new("second").unwrap(),
        effect_id: effect.clone(),
        requires_approval: false,
        recovery: RecoveryPolicy::Pure,
    });
    r.effects.insert(effect, ScriptEffect::Input);
    assert!(HttpHost::new(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![r])
    )
    .is_err());
    {
        let mut config = HostConfig::new(vec![route(ScriptEffect::Input)]);
        config.max_active_runs = 0;
        assert!(HttpHost::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(OwnerPolicy),
            config
        )
        .is_err());
    }
    {
        let mut config = HostConfig::new(vec![route(ScriptEffect::Input)]);
        config.viewer_queue_frames = usize::MAX;
        assert!(HttpHost::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(OwnerPolicy),
            config
        )
        .is_err());
    }
}

#[tokio::test]
async fn approval_bearing_persisted_threads_do_not_report_an_empty_pending_list() {
    let h = host(ScriptEffect::Input);
    let id = create(&h).await;
    let rt = b4_runtime::Runtime::new(h.store().clone());
    let mut definition = route(ScriptEffect::Input).definition;
    definition.batches[0][0].requires_approval = true;
    let initial = b4_kernel::new_execution(
        ExecutionId::new("parked-execution").unwrap(),
        TurnId::new("parked-turn").unwrap(),
        rt.store().owner_generation(),
        definition,
        serde_json::Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    let accepted = rt
        .admit(&ThreadId::new(id.clone()).unwrap(), initial)
        .unwrap();
    let running = rt
        .apply(
            &accepted.snapshot.execution_id,
            b4_runtime::ExecutionFence::from(&accepted.snapshot),
            KernelCommand::Start,
            TransitionFacts { now_ms: 1 },
        )
        .unwrap();
    rt.apply(
        &running.snapshot.execution_id,
        b4_runtime::ExecutionFence::from(&running.snapshot),
        KernelCommand::Park {
            task_id: TaskId::new("echo-task").unwrap(),
            interrupt_id: InterruptId::new("sensitive-interrupt").unwrap(),
            prompt: json!({"secret":"do-not-disclose"}),
        },
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    for suffix in ["state", "pending_interrupts"] {
        let (status, body) = request(
            &h,
            "GET",
            &format!("/threads/{id}/{suffix}"),
            Some("owner"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(!body.to_string().contains("sensitive-interrupt"));
        assert!(!body.to_string().contains("do-not-disclose"));
    }
    assert_eq!(
        request(
            &h,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            Some("owner"),
            r#"{"route":"/echo#workflow"}"#
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn unavailable_revision_remains_blocked_and_pending_fails_closed() {
    let directory = std::env::temp_dir().join(format!("b4-http-blocked-{}", uuid::Uuid::new_v4()));
    let id = ThreadId::new("blocked-thread").unwrap();
    let store = b4_sqlite::SqliteStore::open(&directory).unwrap();
    store
        .create_thread_with(b4_runtime::ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: serde_json::Map::new(),
            origin: Some(b4_runtime::ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("blocked-execution").unwrap(),
        TurnId::new("initial-turn").unwrap(),
        store.owner_generation(),
        route(ScriptEffect::Input).definition,
        serde_json::Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    let accepted = store.admit(&id, initial).unwrap();
    let started = store
        .commit(
            &accepted.snapshot.execution_id,
            b4_runtime::ExecutionFence::from(&accepted.snapshot),
            b4_kernel::transition(
                &accepted.snapshot,
                KernelCommand::Start,
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let accepted = store
        .commit(
            &started.snapshot.execution_id,
            b4_runtime::ExecutionFence::from(&started.snapshot),
            b4_kernel::transition(
                &started.snapshot,
                KernelCommand::Dispatch {
                    task_id: TaskId::new("echo-task").unwrap(),
                    attempt_id: AttemptId::new("unfinished-attempt").unwrap(),
                },
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    drop(store);
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory).unwrap());
    let rt = b4_runtime::Runtime::new(store.clone());
    let old_work = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![route(ScriptEffect::Input)]),
    )
    .unwrap();
    for suffix in ["state", "pending_interrupts"] {
        assert_eq!(
            request(
                &old_work,
                "GET",
                &format!("/threads/{id}/{suffix}"),
                Some("owner"),
                ""
            )
            .await
            .0,
            StatusCode::NOT_IMPLEMENTED,
            "unsettled prior-owner work cannot project as current work"
        );
    }
    drop(old_work);
    rt.recover(
        &accepted.snapshot.execution_id,
        b4_runtime::ExecutionFence::from(&accepted.snapshot),
        ExecutionRevision::new("unavailable-v2").unwrap(),
        TurnId::new("new-turn").unwrap(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    let h = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![route(ScriptEffect::Input)]),
    )
    .unwrap();
    for suffix in ["state", "pending_interrupts"] {
        assert_eq!(
            request(
                &h,
                "GET",
                &format!("/threads/{id}/{suffix}"),
                Some("owner"),
                ""
            )
            .await
            .0,
            StatusCode::NOT_IMPLEMENTED
        );
    }
    assert_eq!(
        request(
            &h,
            "POST",
            &format!("/threads/{id}/runs/wait"),
            Some("owner"),
            r#"{"route":"/echo#workflow"}"#
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        store
            .thread_view(&id)
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .status,
        ExecutionStatus::RecoveryBlocked
    );
    drop(h);
    drop(rt);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}
