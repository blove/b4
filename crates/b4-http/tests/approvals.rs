use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{
    approval::*, ExecutionFence, MemoryStore, Runtime, Store, ThreadOrigin, ThreadSeed,
};
use serde_json::{json, Map, Value};
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Principal, AuthenticationError> {
        let (principal, tenant) = match headers.get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer owner") => ("owner", "tenant"),
            Some("Bearer other") => ("other", "tenant"),
            Some("Bearer other-tenant") => ("owner", "other-tenant"),
            _ => return Err(AuthenticationError),
        };
        Ok(Principal {
            application: "app".into(),
            tenant: tenant.into(),
            principal: principal.into(),
        })
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
fn route() -> ScriptRoute {
    let effect = EffectId::new("effect").unwrap();
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/approve#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            max_attempts: 1,
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: effect.clone(),
                requires_approval: true,
                recovery: RecoveryPolicy::Pure,
            }]],
        },
        effects: BTreeMap::from([(effect, ScriptEffect::Input)]),
    }
}
fn approval_config() -> ApprovalHostConfig {
    let r = route();
    let mut config = ApprovalHostConfig::new(
        "policy-v1".into(),
        BTreeMap::from([(
            r.definition.route,
            BTreeMap::from([(TaskId::new("task").unwrap(), json!({"question":"allow?"}))]),
        )]),
    );
    config.service_config = ApprovalServiceConfig::VolatileFixture;
    config
}
fn make_host(
    store: Arc<MemoryStore>,
    config: ApprovalHostConfig,
) -> Result<HttpHost<MemoryStore>, ConfigError> {
    HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        Arc::new(Owner),
        HostConfig::new(vec![route()]),
        config,
    )
}
async fn request<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = host
        .router()
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 200_000).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap(), headers)
}

async fn request_after_claim_release<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let reply = request(host, method, path, token, body).await;
        if reply.0 != StatusCode::CONFLICT || reply.1["error"]["message"] != "thread_busy" {
            return reply;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "owned claim did not release"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}
struct Authority;
impl ApprovalAuthority for Authority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        },
        policy_revision: "policy-v1".into(),
    }
}
fn park<S: ApprovalStore>(store: &Arc<S>) -> ExecutionId {
    park_with_ttl(store, 60_000, 0)
}
fn park_with_ttl<S: ApprovalStore>(store: &Arc<S>, ttl_ms: u64, age_ms: u64) -> ExecutionId {
    let thread = ThreadId::new("thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let runtime = Runtime::new(store.clone());
    let id = ExecutionId::new("execution").unwrap();
    let accepted = runtime
        .admit(
            &thread,
            b4_kernel::new_execution(
                id.clone(),
                TurnId::new("turn").unwrap(),
                store.owner_generation(),
                route().definition,
                Map::from_iter([("input".into(), json!({"safe":true}))]),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let running = runtime
        .apply(
            &id,
            ExecutionFence::from(&accepted.snapshot),
            KernelCommand::Start,
            TransitionFacts { now_ms: 2 },
        )
        .unwrap();
    let view = store.thread_view(&thread).unwrap().unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    ApprovalService::with_config(
        store.clone(),
        Authority,
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap()
    .park(
        &context(),
        &id,
        ApprovalFence {
            execution: ExecutionFence::from(&running.snapshot),
            resource_revision: view.thread.resource_revision,
        },
        TaskId::new("task").unwrap(),
        InterruptId::new("interrupt").unwrap(),
        json!({"question":"allow?"}),
        ttl_ms,
        TransitionFacts {
            now_ms: now - age_ms,
        },
    )
    .unwrap();
    id
}

#[test]
fn explicit_constructor_requires_durable_storage_by_default() {
    let mut config = approval_config();
    config.service_config = ApprovalServiceConfig::RequiredDurable;
    assert_eq!(
        make_host(Arc::new(MemoryStore::new()), config)
            .err()
            .unwrap()
            .0,
        "approval_durability_required"
    );
    assert!(make_host(Arc::new(MemoryStore::new()), approval_config()).is_ok());
    assert_eq!(
        HttpHost::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(Owner),
            HostConfig::new(vec![route()])
        )
        .err()
        .unwrap()
        .0,
        "approval_unsupported"
    );
}

#[test]
fn constructor_rejects_prompt_coverage_revision_ttl_depth_and_aggregate_limits() {
    for case in 0..6 {
        let mut config = approval_config();
        match case {
            0 => config.prompts.clear(),
            1 => config.policy_revision.clear(),
            2 => config.ttl_ms = 0,
            3 => config.ttl_ms = MAX_APPROVAL_TTL_MS + 1,
            4 => {
                let mut value = Value::Null;
                for _ in 0..65 {
                    value = json!([value]);
                }
                config
                    .prompts
                    .values_mut()
                    .next()
                    .unwrap()
                    .insert(TaskId::new("task").unwrap(), value);
            }
            _ => {
                config
                    .prompts
                    .values_mut()
                    .next()
                    .unwrap()
                    .insert(TaskId::new("extra").unwrap(), Value::Null);
            }
        }
        assert!(
            make_host(Arc::new(MemoryStore::new()), config).is_err(),
            "case {case}"
        );
    }
    let mut host_config = HostConfig::new(vec![route()]);
    host_config.max_output_bytes = 100;
    assert!(HttpHost::new_with_approvals(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Owner),
        host_config,
        approval_config()
    )
    .is_err());
}

#[tokio::test]
async fn authorized_pending_exposes_grant_only_in_no_store_dedicated_projection() {
    let store = Arc::new(MemoryStore::new());
    park(&store);
    let host = make_host(store, approval_config()).unwrap();
    let pending = request(
        &host,
        "GET",
        "/threads/thread/pending_interrupts",
        Some("owner"),
        "",
    )
    .await;
    assert_eq!(pending.0, StatusCode::OK);
    assert_eq!(pending.2["cache-control"], "no-store");
    assert_eq!(pending.1["interrupts"][0]["interruptId"], "interrupt");
    assert_eq!(
        pending.1["interrupts"][0]["value"],
        json!({"question":"allow?"})
    );
    assert!(pending.1["interrupts"][0]["grant"]
        .as_str()
        .unwrap()
        .starts_with("b4ag_"));
    assert!(pending.1["interrupts"][0]["resumeKey"].is_string());
    let state = request(&host, "GET", "/threads/thread/state", Some("owner"), "").await;
    assert_eq!(state.0, StatusCode::OK);
    assert!(!state.1.to_string().contains("b4ag_"));
    assert!(!state.1.to_string().contains("allow?"));
    assert_eq!(
        request(
            &host,
            "GET",
            "/threads/thread/pending_interrupts",
            Some("other"),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn resume_strict_parse_policy_and_native_validation_precedence() {
    let store = Arc::new(MemoryStore::new());
    let id = park(&store);
    let host = make_host(store.clone(), approval_config()).unwrap();
    let before = store.execution(&id).unwrap().unwrap();
    for body in [
        "",
        r#"{"route":"/approve#workflow","resume":[]}"#,
        r#"{"route":"/approve#workflow","resume":[],"extra":true}"#,
        r#"{"route":"/approve#workflow","resume":[{"status":"cancelled","interruptId":"interrupt","grant":null}]}"#,
    ] {
        assert_eq!(
            request(&host, "POST", "/threads/thread/resume", Some("owner"), body)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let body = r#"{"route":"/wrong#workflow","resume":[{"status":"resolved","interruptId":"wrong","payload":"always","grant":"opaque-secret"}]}"#;
    let denied = request(&host, "POST", "/threads/thread/resume", Some("other"), body).await;
    assert_eq!(denied.0, StatusCode::NOT_FOUND);
    assert_eq!(
        request(&host, "POST", "/threads/thread/resume", Some("owner"), body)
            .await
            .1["error"]["message"],
        "route_mismatch"
    );
    let body = body.replace("/wrong#workflow", "/approve#workflow");
    assert_eq!(
        request(
            &host,
            "POST",
            "/threads/thread/resume",
            Some("owner"),
            &body
        )
        .await
        .1["error"]["message"],
        "interrupt_set_mismatch"
    );
    let body = body.replace("\"wrong\"", "\"interrupt\"");
    assert_eq!(
        request(
            &host,
            "POST",
            "/threads/thread/resume",
            Some("owner"),
            &body
        )
        .await
        .1["error"]["message"],
        "always_unsupported"
    );
    let body = body.replace("\"always\"", "\"once\"");
    let invalid = request(
        &host,
        "POST",
        "/threads/thread/resume",
        Some("owner"),
        &body,
    )
    .await;
    assert_eq!(invalid.0, StatusCode::FORBIDDEN);
    assert_eq!(invalid.1["error"]["message"], "grant_invalid");
    assert!(!invalid.1.to_string().contains("opaque-secret"));
    assert_eq!(store.execution(&id).unwrap().unwrap(), before);
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn combined_input_and_prompt_limit_rejects_before_creating_a_thread() {
    let store = Arc::new(MemoryStore::new());
    let mut config = approval_config();
    config
        .prompts
        .values_mut()
        .next()
        .unwrap()
        .insert(TaskId::new("task").unwrap(), json!("p".repeat(40_000)));
    let host = make_host(store.clone(), config).unwrap();
    let body = json!({"route":"/approve#workflow","input":"i".repeat(40_000)}).to_string();
    let response = request(
        &host,
        "POST",
        "/threads/not-created/runs/wait",
        Some("owner"),
        &body,
    )
    .await;
    assert_eq!(response.0, StatusCode::BAD_REQUEST);
    assert_eq!(response.1["error"]["message"], "approval_proposal_limit");
    assert!(store
        .thread(&ThreadId::new("not-created").unwrap())
        .unwrap()
        .is_none());
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

fn resume_body(grant: Option<&str>) -> String {
    let mut entry = json!({"status":"resolved","interruptId":"interrupt","payload":"once"});
    if let Some(grant) = grant {
        entry["grant"] = json!(grant);
    }
    json!({"route":"/approve#workflow","resume":[entry]}).to_string()
}

#[tokio::test]
async fn grant_failure_statuses_are_distinct_without_consumption_or_sensitive_diagnostics() {
    for expired in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let id = if expired {
            park_with_ttl(&store, 1, 1000)
        } else {
            park(&store)
        };
        let host = make_host(store.clone(), approval_config()).unwrap();
        let before = store.execution(&id).unwrap().unwrap();
        let pending = request(
            &host,
            "GET",
            "/threads/thread/pending_interrupts",
            Some("owner"),
            "",
        )
        .await;
        let grant = pending.1["interrupts"][0]["grant"].as_str().unwrap();
        let wrong = format!("b4ag_{}", "0".repeat(64));
        let mut bodies = vec![
            (resume_body(None), StatusCode::BAD_REQUEST, "grant_required"),
            (
                resume_body(Some(&wrong)),
                StatusCode::FORBIDDEN,
                "grant_invalid",
            ),
        ];
        if expired {
            bodies.push((
                resume_body(Some(grant)),
                StatusCode::CONFLICT,
                "grant_expired",
            ));
        }
        for (body, status, code) in bodies {
            let rejected = request_after_claim_release(
                &host,
                "POST",
                "/threads/thread/resume",
                Some("owner"),
                &body,
            )
            .await;
            assert_eq!(rejected.0, status);
            assert_eq!(rejected.1["error"]["message"], code);
            assert!(!rejected.1.to_string().contains(grant));
            assert_eq!(store.execution(&id).unwrap().unwrap(), before);
            assert!(!store.approval_view(&id).unwrap().unwrap().grants[0].consumed);
        }
        assert_eq!(
            request(&host, "GET", "/readyz", None, "").await.0,
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn changed_current_definition_rejects_before_grant_validation_or_disclosure() {
    let store = Arc::new(MemoryStore::new());
    let id = park(&store);
    let mut route = route();
    route.definition.revision = ExecutionRevision::new("v2").unwrap();
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        HostConfig::new(vec![route]),
        approval_config(),
    )
    .unwrap();
    let pending = request(
        &host,
        "GET",
        "/threads/thread/pending_interrupts",
        Some("owner"),
        "",
    )
    .await;
    assert_eq!(pending.0, StatusCode::NOT_FOUND);
    assert!(!pending.1.to_string().contains("allow?"));
    let rejected = request(
        &host,
        "POST",
        "/threads/thread/resume",
        Some("owner"),
        &resume_body(Some("opaque-secret")),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::CONFLICT);
    assert_eq!(rejected.1["error"]["message"], "route_mismatch");
    assert!(!store.approval_view(&id).unwrap().unwrap().grants[0].consumed);
}

struct DenyRenewal(std::sync::atomic::AtomicUsize);
impl ResourcePolicy for DenyRenewal {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0
    }
}
#[tokio::test]
async fn sensitive_pending_renews_policy_before_disclosing_a_grant() {
    let store = Arc::new(MemoryStore::new());
    park(&store);
    let host = HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        Arc::new(DenyRenewal(std::sync::atomic::AtomicUsize::new(0))),
        HostConfig::new(vec![route()]),
        approval_config(),
    )
    .unwrap();
    let denied = request(
        &host,
        "GET",
        "/threads/thread/pending_interrupts",
        Some("owner"),
        "",
    )
    .await;
    assert_eq!(denied.0, StatusCode::NOT_FOUND);
    assert!(!denied.1.to_string().contains("b4ag_"));
    assert!(!denied.1.to_string().contains("allow?"));
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("b4-http-approval-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[tokio::test]
async fn sqlite_resume_commit_failure_rolls_back_grant_snapshot_journal_and_audit() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let id = park(&store);
    let mut config = approval_config();
    config.service_config = ApprovalServiceConfig::RequiredDurable;
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        HostConfig::new(vec![route()]),
        config,
    )
    .unwrap();
    let pending = request(
        &host,
        "GET",
        "/threads/thread/pending_interrupts",
        Some("owner"),
        "",
    )
    .await;
    let grant = pending.1["interrupts"][0]["grant"].as_str().unwrap();
    let before = store.approval_view(&id).unwrap().unwrap();
    let journal = store.journal(&id).unwrap();
    directory.connection().execute_batch("CREATE TRIGGER fail_accept BEFORE INSERT ON approval_acceptances BEGIN SELECT RAISE(ABORT, 'private-storage-diagnostic'); END;").unwrap();
    let failed = request(
        &host,
        "POST",
        "/threads/thread/resume",
        Some("owner"),
        &resume_body(Some(grant)),
    )
    .await;
    assert_eq!(failed.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(failed.1["error"]["message"], "host_unready");
    assert!(!failed.1.to_string().contains("private-storage-diagnostic"));
    assert!(!failed.1.to_string().contains(grant));
    assert_eq!(store.approval_view(&id).unwrap().unwrap(), before);
    assert_eq!(store.journal(&id).unwrap(), journal);
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn partial_park_refuses_pending_disclosure_until_every_ready_sibling_settles() {
    let store = Arc::new(MemoryStore::new());
    let thread = ThreadId::new("partial").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let mut route = route();
    let mut sibling = route.definition.batches[0][0].clone();
    sibling.id = TaskId::new("sibling").unwrap();
    sibling.effect_id = EffectId::new("sibling-effect").unwrap();
    sibling.requires_approval = false;
    route
        .effects
        .insert(sibling.effect_id.clone(), ScriptEffect::Input);
    route.definition.batches[0].push(sibling);
    route.definition.max_attempts = 2;
    let runtime = Runtime::new(store.clone());
    let id = ExecutionId::new("partial-execution").unwrap();
    let accepted = runtime
        .admit(
            &thread,
            b4_kernel::new_execution(
                id.clone(),
                TurnId::new("partial-turn").unwrap(),
                store.owner_generation(),
                route.definition.clone(),
                Map::new(),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let running = runtime
        .apply(
            &id,
            ExecutionFence::from(&accepted.snapshot),
            KernelCommand::Start,
            TransitionFacts { now_ms: 2 },
        )
        .unwrap();
    let resource = store.thread_view(&thread).unwrap().unwrap();
    let parked = ApprovalService::with_config(
        store.clone(),
        Authority,
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap()
    .park(
        &context(),
        &id,
        ApprovalFence {
            execution: ExecutionFence::from(&running.snapshot),
            resource_revision: resource.thread.resource_revision,
        },
        TaskId::new("task").unwrap(),
        InterruptId::new("partial-interrupt").unwrap(),
        json!("partial-private-prompt"),
        60_000,
        TransitionFacts { now_ms: 3 },
    )
    .unwrap();
    assert_eq!(parked.snapshot.status, ExecutionStatus::Running);
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![route.clone()]),
        approval_config(),
    )
    .unwrap();
    let pending = request(
        &host,
        "GET",
        "/threads/partial/pending_interrupts",
        Some("owner"),
        "",
    )
    .await;
    assert_eq!(pending.0, StatusCode::CONFLICT);
    assert_eq!(pending.1["error"]["message"], "resume_in_progress");
    assert!(!pending.1.to_string().contains("partial-private-prompt"));
    assert!(!pending.1.to_string().contains("b4ag_"));
    assert_eq!(
        request(
            &host,
            "GET",
            "/threads/partial/pending_interrupts",
            Some("other"),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
    let revoked = HttpHost::new_with_approvals(
        store,
        Arc::new(Auth),
        Arc::new(DenyRenewal(std::sync::atomic::AtomicUsize::new(0))),
        HostConfig::new(vec![route]),
        approval_config(),
    )
    .unwrap();
    assert_eq!(
        request(
            &revoked,
            "GET",
            "/threads/partial/pending_interrupts",
            Some("owner"),
            ""
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn whole_set_token_checks_follow_definition_order_after_caller_permutation() {
    let store = Arc::new(MemoryStore::new());
    let thread = ThreadId::new("ordered").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let mut route = route();
    let mut second = route.definition.batches[0][0].clone();
    second.id = TaskId::new("second").unwrap();
    second.effect_id = EffectId::new("second-effect").unwrap();
    route
        .effects
        .insert(second.effect_id.clone(), ScriptEffect::Input);
    route.definition.batches[0].push(second.clone());
    route.definition.max_attempts = 2;
    let runtime = Runtime::new(store.clone());
    let id = ExecutionId::new("ordered-execution").unwrap();
    let initial = runtime
        .admit(
            &thread,
            b4_kernel::new_execution(
                id.clone(),
                TurnId::new("ordered-turn").unwrap(),
                store.owner_generation(),
                route.definition.clone(),
                Map::new(),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let mut current = runtime
        .apply(
            &id,
            ExecutionFence::from(&initial.snapshot),
            KernelCommand::Start,
            TransitionFacts { now_ms: 2 },
        )
        .unwrap();
    let service = ApprovalService::with_config(
        store.clone(),
        Authority,
        ApprovalServiceConfig::VolatileFixture,
    )
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for (task_id, interrupt) in [
        (TaskId::new("task").unwrap(), "interrupt"),
        (second.id.clone(), "second-interrupt"),
    ] {
        let view = store.thread_view(&thread).unwrap().unwrap();
        current = service
            .park(
                &context(),
                &id,
                ApprovalFence {
                    execution: ExecutionFence::from(&current.snapshot),
                    resource_revision: view.thread.resource_revision,
                },
                task_id,
                InterruptId::new(interrupt).unwrap(),
                Value::Null,
                60_000,
                TransitionFacts { now_ms: now },
            )
            .unwrap();
    }
    assert_eq!(current.snapshot.status, ExecutionStatus::WaitingForApproval);
    let mut config = approval_config();
    config
        .prompts
        .values_mut()
        .next()
        .unwrap()
        .insert(second.id, Value::Null);
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Owner),
        HostConfig::new(vec![route]),
        config,
    )
    .unwrap();
    for reverse in [true, false] {
        let mut entries = vec![
            json!({"status":"resolved","interruptId":"interrupt","payload":"once"}),
            json!({"status":"resolved","interruptId":"second-interrupt","payload":"once","grant":"malformed-private-grant"}),
        ];
        if reverse {
            entries.reverse();
        }
        let rejected = request(
            &host,
            "POST",
            "/threads/ordered/resume",
            Some("owner"),
            &json!({"route":"/approve#workflow","resume":entries}).to_string(),
        )
        .await;
        assert_eq!(rejected.0, StatusCode::BAD_REQUEST);
        assert_eq!(rejected.1["error"]["message"], "grant_required");
        assert_eq!(store.execution(&id).unwrap().unwrap(), current);
        assert!(store
            .approval_view(&id)
            .unwrap()
            .unwrap()
            .grants
            .iter()
            .all(|grant| !grant.consumed));
    }
}

#[tokio::test]
async fn resume_renewal_denial_precedes_invalid_native_token_and_spends_nothing() {
    let store = Arc::new(MemoryStore::new());
    let id = park(&store);
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(DenyRenewal(std::sync::atomic::AtomicUsize::new(0))),
        HostConfig::new(vec![route()]),
        approval_config(),
    )
    .unwrap();
    let denied = request(
        &host,
        "POST",
        "/threads/thread/resume",
        Some("owner"),
        &resume_body(Some("malformed-sensitive-grant")),
    )
    .await;
    assert_eq!(denied.0, StatusCode::NOT_FOUND);
    assert_eq!(denied.1["error"]["message"], "resource_unavailable");
    assert!(!store.approval_view(&id).unwrap().unwrap().grants[0].consumed);
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

struct Allow;
impl ResourcePolicy for Allow {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        true
    }
}
#[tokio::test]
async fn permissive_policy_cannot_disclose_grants_or_resume_for_a_nonorigin_actor() {
    let store = Arc::new(MemoryStore::new());
    let id = park(&store);
    let before = store.approval_view(&id).unwrap().unwrap();
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![route()]),
        approval_config(),
    )
    .unwrap();
    for actor in ["other", "other-tenant"] {
        for body in [resume_body(Some("malformed-private-token")), r#"{"route":"/wrong#workflow","resume":[{"status":"resolved","interruptId":"wrong","payload":"always"}]}"#.into()] {
            let rejected = request(&host, "POST", "/threads/thread/resume", Some(actor), &body).await;
            assert_eq!(rejected.0, StatusCode::NOT_FOUND);
            assert_eq!(rejected.1["error"]["message"], "resource_unavailable");
            assert!(!rejected.1.to_string().contains("malformed-private-token"));
        }
        let rejected = request(
            &host,
            "GET",
            "/threads/thread/pending_interrupts",
            Some(actor),
            "",
        )
        .await;
        assert_eq!(rejected.0, StatusCode::NOT_FOUND);
        assert!(!rejected.1.to_string().contains("allow?"));
        assert!(!rejected.1.to_string().contains("b4ag_"));
    }
    assert_eq!(store.approval_view(&id).unwrap().unwrap(), before);
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn permissive_policy_cannot_admit_approval_work_on_another_actors_idle_thread() {
    let store = Arc::new(MemoryStore::new());
    let thread = ThreadId::new("idle-owner").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let before = store.thread_view(&thread).unwrap().unwrap();
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![route()]),
        approval_config(),
    )
    .unwrap();
    for (actor, endpoint) in [("other", "wait"), ("other-tenant", "stream")] {
        let rejected = request(
            &host,
            "POST",
            &format!("/threads/idle-owner/runs/{endpoint}"),
            Some(actor),
            r#"{"route":"/approve#workflow"}"#,
        )
        .await;
        assert_eq!(rejected.0, StatusCode::NOT_FOUND);
        assert_eq!(rejected.1["error"]["message"], "resource_unavailable");
        assert_eq!(store.thread_view(&thread).unwrap().unwrap(), before);
    }
    assert_eq!(
        request(&host, "GET", "/readyz", None, "").await.0,
        StatusCode::OK
    );
}
