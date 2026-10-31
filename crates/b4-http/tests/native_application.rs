use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(Principal {
            application: headers
                .get("application")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("app")
                .into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
    }
}
struct Allow;
impl ResourcePolicy for Allow {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        true
    }
}
struct Echo {
    contexts: Arc<Mutex<Vec<InvocationContext>>>,
}
impl NativeEffect for Echo {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.contexts.lock().unwrap().push(context.clone());
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: call.input["input"].clone(),
            writes: vec![StateWrite::Replace {
                field: "native".into(),
                value: json!(true),
            }],
        }))
    }
}
struct Reject;
impl InputValidator for Reject {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        Err(InvalidApplicationInput)
    }
}
fn definition() -> Definition {
    Definition {
        route: RouteKey::new("/echo#workflow").unwrap(),
        revision: ExecutionRevision::new("echo-v1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("echo").unwrap(),
            effect_id: EffectId::new("echo").unwrap(),
            requires_approval: false,
            recovery: RecoveryPolicy::Pure,
        }]],
        max_attempts: 1,
    }
}
fn application(
    callback: Arc<dyn NativeEffect>,
    validator: Option<Arc<dyn InputValidator>>,
    output_kind: OutputKind,
) -> Arc<NativeApplication> {
    Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "app".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                definition: definition(),
                effects: BTreeMap::from([(
                    EffectId::new("echo").unwrap(),
                    EffectBinding {
                        callback,
                        output_kind,
                    },
                )]),
                input_validator: validator,
            }],
        )
        .unwrap(),
    )
}
fn config(application: Arc<NativeApplication>) -> NativeHostConfig {
    let mut approval = ApprovalHostConfig::new("policy-v1".into(), BTreeMap::new());
    approval.service_config = ApprovalServiceConfig::VolatileFixture;
    NativeHostConfig::new(application, approval)
}
async fn request<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    payload: Option<Value>,
    application: &str,
) -> axum::response::Response {
    host.router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("application", application)
                .header("content-type", "application/json")
                .body(Body::from(
                    payload.map(|v| v.to_string()).unwrap_or_default(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await.unwrap()).unwrap()
}

#[tokio::test]
async fn native_dispatch_uses_registered_callback_and_server_context() {
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            None,
            OutputKind::Value,
        )),
    )
    .unwrap();
    let response = request(&host, "POST", "/threads/native/runs/wait", Some(json!({"route":"/echo#workflow","input":{"metadata":{"application":"forged"},"answer":42}})), "app").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body(response).await,
        json!({"metadata":{"application":"forged"},"answer":42})
    );
    let view = store
        .thread_view(&ThreadId::new("native").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        view.execution.as_ref().unwrap().snapshot.values["native"],
        true
    );
    let contexts = contexts.lock().unwrap();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].thread_generation, view.thread.generation);
    assert_eq!(contexts[0].origin, view.thread.origin.unwrap());
    assert_eq!(contexts[0].application_revision, "app-v1");
    assert_eq!(contexts[0].execution_revision, definition().revision);
    assert_eq!(contexts[0].policy_revision, "policy-v1");
}

#[tokio::test]
async fn native_invalid_input_has_no_implicit_thread_or_callback_and_stays_ready() {
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            Some(Arc::new(Reject)),
            OutputKind::Value,
        )),
    )
    .unwrap();
    let response = request(
        &host,
        "POST",
        "/threads/rejected/runs/wait",
        Some(json!({"route":"/echo#workflow","input":null})),
        "app",
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body(response).await["error"]["details"]["code"],
        "invalid_application_input"
    );
    assert!(store
        .thread_view(&ThreadId::new("rejected").unwrap())
        .unwrap()
        .is_none());
    assert!(contexts.lock().unwrap().is_empty());
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn native_missing_route_and_private_denial_have_same_envelope() {
    struct Deny;
    impl ResourcePolicy for Deny {
        fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
            false
        }
    }
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let app = application(Arc::new(Echo { contexts }), None, OutputKind::Value);
    let public = HttpHost::new_with_application(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Allow),
        config(app.clone()),
    )
    .unwrap();
    let private = HttpHost::new_with_application(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Deny),
        config(app),
    )
    .unwrap();
    let missing = request(
        &public,
        "POST",
        "/threads/new/runs/wait",
        Some(json!({"route":"/missing#workflow","input":null})),
        "app",
    )
    .await;
    let denied = request(
        &private,
        "POST",
        "/threads/new/runs/wait",
        Some(json!({"route":"/echo#workflow","input":null})),
        "app",
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(denied.status(), StatusCode::NOT_FOUND);
    assert_eq!(body(missing).await, body(denied).await);
}

#[path = "native_application/support.rs"]
mod support;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use support::*;

struct BlockingValidator {
    gate: Arc<Gate>,
    calls: Arc<AtomicUsize>,
}
impl InputValidator for BlockingValidator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.gate.block();
        Ok(())
    }
}
struct SwitchedPolicy(Arc<AtomicBool>);
impl ResourcePolicy for SwitchedPolicy {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        !self.0.load(Ordering::SeqCst)
    }
}
struct Fixed(NativeOutcome);
impl NativeEffect for Fixed {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        Ok(self.0.clone())
    }
}
async fn wire(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_validator_retains_store_capacity_and_thread_claim_before_any_mutation() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let mut config = config(application(
        Arc::new(Echo { contexts }),
        Some(Arc::new(BlockingValidator {
            gate: gate.clone(),
            calls: calls.clone(),
        })),
        OutputKind::Value,
    ));
    config.limits.max_active_runs = 2;
    let host =
        HttpHost::new_with_application(store.clone(), Arc::new(Auth), Arc::new(Allow), config)
            .unwrap();
    let worker_host = host.clone();
    let pending = tokio::spawn(async move {
        request(
            &worker_host,
            "POST",
            "/threads/held/runs/wait",
            Some(json!({"route":"/echo#workflow","input":null})),
            "app",
        )
        .await
    });
    gate.entered().await;
    assert!(store
        .thread_view(&ThreadId::new("held").unwrap())
        .unwrap()
        .is_none());
    pending.abort();
    assert_eq!(
        request(
            &host,
            "POST",
            "/threads/held/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app"
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    let second_host = host.clone();
    let second = tokio::spawn(async move {
        request(
            &second_host,
            "POST",
            "/threads/also-held/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app",
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while calls.load(Ordering::SeqCst) != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    second.abort();
    assert_eq!(
        request(
            &host,
            "POST",
            "/threads/over-capacity/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app"
        )
        .await
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(store
        .thread_view(&ThreadId::new("also-held").unwrap())
        .unwrap()
        .is_none());
    let retained = Arc::downgrade(&store);
    drop(host);
    drop(store);
    assert!(retained.upgrade().is_some());
    gate.release();
    tokio::time::timeout(Duration::from_secs(3), async {
        while retained.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validator_return_renews_policy_and_does_not_create_a_thread_after_revocation() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let denied = Arc::new(AtomicBool::new(false));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(SwitchedPolicy(denied.clone())),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            Some(Arc::new(BlockingValidator {
                gate: gate.clone(),
                calls: Arc::new(AtomicUsize::new(0)),
            })),
            OutputKind::Value,
        )),
    )
    .unwrap();
    let worker_host = host.clone();
    let pending = tokio::spawn(async move {
        request(
            &worker_host,
            "POST",
            "/threads/revoked/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app",
        )
        .await
    });
    gate.entered().await;
    denied.store(true, Ordering::SeqCst);
    gate.release();
    assert_eq!(pending.await.unwrap().status(), StatusCode::NOT_FOUND);
    assert!(store
        .thread_view(&ThreadId::new("revoked").unwrap())
        .unwrap()
        .is_none());
    assert!(contexts.lock().unwrap().is_empty());
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validator_return_cannot_admit_into_a_recreated_thread_generation() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let id = ThreadId::new("aba").unwrap();
    let seed = ThreadSeed {
        id: id.clone(),
        created_at_ms: 1,
        metadata: Default::default(),
        origin: Some(ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        }),
    };
    store.create_thread_with(seed.clone()).unwrap();
    let original = store.thread_view(&id).unwrap().unwrap();
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            Some(Arc::new(BlockingValidator {
                gate: gate.clone(),
                calls: Arc::new(AtomicUsize::new(0)),
            })),
            OutputKind::Value,
        )),
    )
    .unwrap();
    let worker_host = host.clone();
    let pending = tokio::spawn(async move {
        request(
            &worker_host,
            "POST",
            "/threads/aba/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app",
        )
        .await
    });
    gate.entered().await;
    store
        .delete_thread_fenced(&ThreadDeletionTarget::from_view(&original).unwrap())
        .unwrap();
    let replacement = store.create_thread_with(seed).unwrap();
    assert_ne!(replacement.generation, original.thread.generation);
    gate.release();
    assert_eq!(pending.await.unwrap().status(), StatusCode::CONFLICT);
    assert!(store.thread_view(&id).unwrap().unwrap().execution.is_none());
    assert!(contexts.lock().unwrap().is_empty());
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn wrong_application_never_reads_store_or_calls_validator_even_for_known_recovery() {
    let calls = Arc::new(AtomicUsize::new(0));
    struct CountValidator(Arc<AtomicUsize>);
    impl InputValidator for CountValidator {
        fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(CountStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            Some(Arc::new(CountValidator(calls.clone()))),
            OutputKind::Value,
        )),
    )
    .unwrap();
    assert_eq!(
        request(
            &host,
            "POST",
            "/threads/known/runs/wait",
            Some(json!({"route":"/echo#workflow","input":null})),
            "app"
        )
        .await
        .status(),
        StatusCode::OK
    );
    let target = RecoveryTarget::from_view(
        &store
            .inner
            .thread_view(&ThreadId::new("known").unwrap())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    contexts.lock().unwrap().clear();
    calls.store(0, Ordering::SeqCst);
    store.reset();
    for (method, path, payload) in [
        ("GET", "/threads/known", None),
        ("GET", "/threads/known/state", None),
        ("GET", "/threads/known/pending_interrupts", None),
        ("GET", "/threads/known/runs/stream", None),
        ("POST", "/threads", Some(json!({}))),
        ("POST", "/threads/known/cancel", None),
        ("DELETE", "/threads/known", None),
        (
            "POST",
            "/threads/new/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
        ),
        (
            "POST",
            "/threads/known/resume",
            Some(json!({"route":"/echo#workflow","resume":[]})),
        ),
    ] {
        assert_eq!(
            request(&host, method, path, payload, "foreign")
                .await
                .status(),
            StatusCode::NOT_FOUND,
            "{method} {path}"
        );
    }
    assert_eq!(
        host.recover_execution(
            Principal {
                application: "foreign".into(),
                tenant: "tenant".into(),
                principal: "owner".into()
            },
            target
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(store.counts(), (0, 0, 0));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(contexts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn value_null_and_falsy_preserve_output_while_void_has_empty_primary_done_and_inactive_null_done(
) {
    for (index, value) in [Value::Null, json!(false), json!(0), json!("")]
        .into_iter()
        .enumerate()
    {
        let host = HttpHost::new_with_application(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(Allow),
            config(application(
                Arc::new(Fixed(NativeOutcome::Succeeded(TaskResult {
                    output: value.clone(),
                    writes: vec![],
                }))),
                None,
                OutputKind::Value,
            )),
        )
        .unwrap();
        let response = request(
            &host,
            "POST",
            &format!("/threads/value-{index}/runs/stream"),
            Some(json!({"route":"/echo#workflow"})),
            "app",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(wire(response)
            .await
            .contains(&format!("data: {{\"output\":{value}}}")));
    }
    let host = HttpHost::new_with_application(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(Allow),
        config(application(
            Arc::new(Fixed(NativeOutcome::Succeeded(TaskResult {
                output: Value::Null,
                writes: vec![],
            }))),
            None,
            OutputKind::Void,
        )),
    )
    .unwrap();
    let primary = request(
        &host,
        "POST",
        "/threads/void/runs/stream",
        Some(json!({"route":"/echo#workflow"})),
        "app",
    )
    .await;
    assert!(wire(primary).await.contains("event: done\ndata: {}"));
    let attached = request(&host, "GET", "/threads/void/runs/stream", None, "app").await;
    assert_eq!(attached.status(), StatusCode::OK);
    assert!(wire(attached)
        .await
        .contains("event: done\ndata: {\"output\":null}"));
}

#[tokio::test]
async fn known_failure_remains_durable_even_for_void_but_uncertainty_and_invalid_void_fault() {
    struct Uncertain;
    impl NativeEffect for Uncertain {
        fn invoke(
            &self,
            _: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            Err(EffectUncertain)
        }
    }
    for case in 0..3 {
        let callback: Arc<dyn NativeEffect> = match case {
            0 => Arc::new(Fixed(NativeOutcome::Failed("public_failure".into()))),
            1 => Arc::new(Uncertain),
            _ => Arc::new(Fixed(NativeOutcome::Succeeded(TaskResult {
                output: json!("non_null"),
                writes: vec![],
            }))),
        };
        let store = Arc::new(MemoryStore::new());
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Allow),
            config(application(callback, None, OutputKind::Void)),
        )
        .unwrap();
        let response = request(
            &host,
            "POST",
            "/threads/outcome/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app",
        )
        .await;
        let snapshot = store
            .thread_view(&ThreadId::new("outcome").unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot;
        if case == 0 {
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(body(response).await["error"]["message"], "public_failure");
            assert_eq!(snapshot.status, ExecutionStatus::Failed);
            assert_eq!(
                request(&host, "GET", "/readyz", None, "app").await.status(),
                StatusCode::OK
            );
        } else {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body(response).await["error"]["message"], "host_unready");
            assert_eq!(snapshot.tasks[0].status, TaskStatus::InFlight);
            assert!(snapshot.tasks[0].result.is_none());
            assert_eq!(
                request(&host, "GET", "/readyz", None, "app").await.status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
    }
}

#[tokio::test]
async fn validator_panic_faults_without_any_thread_or_callback() {
    struct Panic;
    impl InputValidator for Panic {
        fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
            panic!("private_validator_detail")
        }
    }
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            Some(Arc::new(Panic)),
            OutputKind::Value,
        )),
    )
    .unwrap();
    let response = request(
        &host,
        "POST",
        "/threads/panic/runs/wait",
        Some(json!({"route":"/echo#workflow"})),
        "app",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body(response).await["error"]["message"], "host_unready");
    assert!(store
        .thread_view(&ThreadId::new("panic").unwrap())
        .unwrap()
        .is_none());
    assert!(contexts.lock().unwrap().is_empty());
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn empty_prompt_native_bridge_renews_ordinary_effect_authority_before_entry() {
    struct DenyAdmitted;
    impl ResourcePolicy for DenyAdmitted {
        fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
            context.stored_route.is_none()
        }
    }
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(DenyAdmitted),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            None,
            OutputKind::Value,
        )),
    )
    .unwrap();
    let response = request(
        &host,
        "POST",
        "/threads/renewed/runs/wait",
        Some(json!({"route":"/echo#workflow"})),
        "app",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(contexts.lock().unwrap().is_empty());
    let snapshot = store
        .thread_view(&ThreadId::new("renewed").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.attempts_used, 0);
    assert_eq!(snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::OK
    );
}

#[test]
fn native_constructor_reuses_finite_limits_and_requires_explicit_approval_durability() {
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let app = application(
        Arc::new(Echo {
            contexts: contexts.clone(),
        }),
        None,
        OutputKind::Value,
    );
    let durable = NativeHostConfig::new(
        app.clone(),
        ApprovalHostConfig::new("policy".into(), BTreeMap::new()),
    );
    assert_eq!(
        HttpHost::new_with_application(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(Allow),
            durable
        )
        .err()
        .unwrap()
        .0,
        "approval_durability_required"
    );
    let modifications: [fn(&mut HostLimits); 12] = [
        |l| l.max_active_runs = 0,
        |l| l.max_concurrent_tasks = 257,
        |l| l.max_tasks_per_execution = 65,
        |l| l.viewer_queue_frames = 0,
        |l| l.heartbeat_ms = 0,
        |l| l.max_input_bytes = 65_537,
        |l| l.max_output_bytes = 0,
        |l| l.max_attached_viewers = 65,
        |l| l.turn_buffer_bytes = 0,
        |l| l.turn_buffer_frames = 4097,
        |l| l.viewer_queue_bytes = 8_388_609,
        |l| l.attachment_snapshot_bytes = 0,
    ];
    for modify in modifications {
        let mut selected = config(app.clone());
        modify(&mut selected.limits);
        assert_eq!(
            HttpHost::new_with_application(
                Arc::new(MemoryStore::new()),
                Arc::new(Auth),
                Arc::new(Allow),
                selected
            )
            .err()
            .unwrap()
            .0,
            "invalid_limits"
        );
    }
    assert!(contexts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn native_required_once_consumes_same_grant_and_resume_never_revalidates_input() {
    struct CountValidator(Arc<AtomicUsize>);
    impl InputValidator for CountValidator {
        fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let mut selected = definition();
    selected.batches[0][0].requires_approval = true;
    let app = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "app".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                definition: selected.clone(),
                effects: BTreeMap::from([(
                    EffectId::new("echo").unwrap(),
                    EffectBinding {
                        callback: Arc::new(Echo {
                            contexts: contexts.clone(),
                        }),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: Some(Arc::new(CountValidator(calls.clone()))),
            }],
        )
        .unwrap(),
    );
    let mut selected_config = config(app);
    selected_config.approval.prompts.insert(
        selected.route.clone(),
        BTreeMap::from([(
            TaskId::new("echo").unwrap(),
            json!({"question":"allow native?"}),
        )]),
    );
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        selected_config,
    )
    .unwrap();
    let parked = request(
        &host,
        "POST",
        "/threads/approved/runs/wait",
        Some(json!({"route":"/echo#workflow","input":[1,false,null]})),
        "app",
    )
    .await;
    assert_eq!(parked.status(), StatusCode::OK);
    assert_eq!(body(parked).await, Value::Null);
    assert!(contexts.lock().unwrap().is_empty());
    let before = store
        .thread_view(&ThreadId::new("approved").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    let pending = body(
        request(
            &host,
            "GET",
            "/threads/approved/pending_interrupts",
            None,
            "app",
        )
        .await,
    )
    .await;
    let entry = &pending["interrupts"][0];
    let payload = json!({"route":"/echo#workflow","resume":[{"status":"resolved","interruptId":entry["interruptId"],"payload":"once","grant":entry["grant"]}]});
    let resumed = request(
        &host,
        "POST",
        "/threads/approved/resume",
        Some(payload.clone()),
        "app",
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    assert!(wire(resumed)
        .await
        .contains("data: {\"output\":[1,false,null]}"));
    let after = store
        .thread_view(&ThreadId::new("approved").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(after.snapshot.execution_id, before.snapshot.execution_id);
    assert_ne!(after.snapshot.turn_id, before.snapshot.turn_id);
    assert_eq!(
        after.snapshot.tasks[0].input["input"],
        json!([1, false, null])
    );
    assert_eq!(contexts.lock().unwrap().len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let authority = store
        .approval_view(&after.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(authority.grants.iter().all(|grant| grant.consumed));
    assert_eq!(
        request(
            &host,
            "POST",
            "/threads/approved/resume",
            Some(payload),
            "app"
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(contexts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn changed_registered_definition_cannot_resume_old_grant_or_disclose_pending() {
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let mut selected = definition();
    selected.batches[0][0].requires_approval = true;
    let make_app = |definition| {
        Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: "app".into(),
                    revision: "app-v1".into(),
                },
                vec![RouteRegistration {
                    definition,
                    effects: BTreeMap::from([(
                        EffectId::new("echo").unwrap(),
                        EffectBinding {
                            callback: Arc::new(Echo {
                                contexts: contexts.clone(),
                            }),
                            output_kind: OutputKind::Value,
                        },
                    )]),
                    input_validator: None,
                }],
            )
            .unwrap(),
        )
    };
    let make_config = |application| {
        let mut conf = config(application);
        conf.approval.prompts.insert(
            selected.route.clone(),
            BTreeMap::from([(TaskId::new("echo").unwrap(), json!("allow?"))]),
        );
        conf
    };
    let store = Arc::new(MemoryStore::new());
    let original = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        make_config(make_app(selected.clone())),
    )
    .unwrap();
    assert_eq!(
        request(
            &original,
            "POST",
            "/threads/revision/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app"
        )
        .await
        .status(),
        StatusCode::OK
    );
    let pending = body(
        request(
            &original,
            "GET",
            "/threads/revision/pending_interrupts",
            None,
            "app",
        )
        .await,
    )
    .await;
    let entry = &pending["interrupts"][0];
    let payload = json!({"route":"/echo#workflow","resume":[{"status":"resolved","interruptId":entry["interruptId"],"payload":"once","grant":entry["grant"]}]});
    let id = ThreadId::new("revision").unwrap();
    let before = store.thread_view(&id).unwrap().unwrap();
    let authority = store
        .approval_view(&before.execution.as_ref().unwrap().snapshot.execution_id)
        .unwrap();
    let mut changed = selected.clone();
    changed.revision = ExecutionRevision::new("echo-v2").unwrap();
    let newer = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        make_config(make_app(changed)),
    )
    .unwrap();
    for path in [
        "/threads/revision/pending_interrupts",
        "/threads/revision/runs/stream",
    ] {
        let response = request(&newer, "GET", path, None, "app").await;
        assert!(response.status().is_client_error());
        let public = body(response).await.to_string();
        assert!(!public.contains(entry["grant"].as_str().unwrap()));
    }
    assert!(request(
        &newer,
        "POST",
        "/threads/revision/resume",
        Some(payload),
        "app"
    )
    .await
    .status()
    .is_client_error());
    assert_eq!(store.thread_view(&id).unwrap().unwrap(), before);
    assert_eq!(
        store
            .approval_view(&before.execution.unwrap().snapshot.execution_id)
            .unwrap(),
        authority
    );
    assert!(contexts.lock().unwrap().is_empty());
    assert_eq!(
        request(&newer, "GET", "/readyz", None, "app")
            .await
            .status(),
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_native_siblings_physically_overlap_and_join_in_definition_order() {
    struct Overlap {
        state: Mutex<Vec<TaskId>>,
        changed: std::sync::Condvar,
    }
    impl NativeEffect for Overlap {
        fn invoke(
            &self,
            call: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            let mut state = self.state.lock().unwrap();
            state.push(call.task_id.clone());
            self.changed.notify_all();
            while state.len() < 2 {
                let (next, timeout) = self
                    .changed
                    .wait_timeout(state, Duration::from_secs(3))
                    .unwrap();
                state = next;
                assert!(
                    !timeout.timed_out(),
                    "second physical native callback never entered"
                );
            }
            Ok(NativeOutcome::Succeeded(TaskResult {
                output: json!(call.task_id),
                writes: vec![StateWrite::Append {
                    field: "items".into(),
                    items: vec![json!(call.task_id)],
                }],
            }))
        }
    }
    let callback = Arc::new(Overlap {
        state: Mutex::new(Vec::new()),
        changed: std::sync::Condvar::new(),
    });
    let mut selected = definition();
    selected.batches = vec![vec!["first", "second"]
        .into_iter()
        .map(|id| TaskSpec {
            id: TaskId::new(id).unwrap(),
            effect_id: EffectId::new(id).unwrap(),
            requires_approval: false,
            recovery: RecoveryPolicy::Pure,
        })
        .collect()];
    selected.max_attempts = 2;
    let app = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "app".into(),
                revision: "app-v1".into(),
            },
            vec![RouteRegistration {
                definition: selected,
                effects: ["first", "second"]
                    .into_iter()
                    .map(|id| {
                        (
                            EffectId::new(id).unwrap(),
                            EffectBinding {
                                callback: callback.clone(),
                                output_kind: OutputKind::Value,
                            },
                        )
                    })
                    .collect(),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let store = Arc::new(MemoryStore::new());
    let host =
        HttpHost::new_with_application(store.clone(), Arc::new(Auth), Arc::new(Allow), config(app))
            .unwrap();
    let response = request(
        &host,
        "POST",
        "/threads/parallel/runs/wait",
        Some(json!({"route":"/echo#workflow"})),
        "app",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body(response).await, "second");
    assert_eq!(callback.state.lock().unwrap().len(), 2);
    let snapshot = store
        .thread_view(&ThreadId::new("parallel").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.values["items"], json!(["first", "second"]));
    assert_eq!(snapshot.attempts_used, 2);
}

struct BlockingNative {
    gate: Arc<Gate>,
    calls: Arc<AtomicUsize>,
}
impl NativeEffect for BlockingNative {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.gate.block();
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("late_actual_success"),
            writes: vec![StateWrite::Replace {
                field: "late".into(),
                value: json!(true),
            }],
        }))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_cancel_publishes_prompt_terminal_but_retains_entered_callback_and_actual_result() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(MemoryStore::new());
    let mut selected = config(application(
        Arc::new(BlockingNative {
            gate: gate.clone(),
            calls: calls.clone(),
        }),
        None,
        OutputKind::Value,
    ));
    selected.limits.max_active_runs = 1;
    let host =
        HttpHost::new_with_application(store.clone(), Arc::new(Auth), Arc::new(Allow), selected)
            .unwrap();
    let worker_host = host.clone();
    let waiting = tokio::spawn(async move {
        request(
            &worker_host,
            "POST",
            "/threads/draining/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app",
        )
        .await
    });
    gate.entered().await;
    let attached = request(&host, "GET", "/threads/draining/runs/stream", None, "app").await;
    assert_eq!(
        request(&host, "POST", "/threads/draining/cancel", None, "app")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let terminal = wire(attached).await;
    assert_eq!(terminal.matches("event: done").count(), 1);
    assert!(terminal.contains("data: {\"output\":{\"cancelled\":true}}"));
    assert_eq!(
        request(&host, "DELETE", "/threads/draining", None, "app")
            .await
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(
            &host,
            "POST",
            "/threads/other/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app"
        )
        .await
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let id = ThreadId::new("draining").unwrap();
    let before = store
        .thread_view(&id)
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(before.status, ExecutionStatus::CancelRequested);
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    gate.release();
    let settled = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = store
                .thread_view(&id)
                .unwrap()
                .unwrap()
                .execution
                .unwrap()
                .snapshot;
            if snapshot.status == ExecutionStatus::Cancelled {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(settled.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        settled.tasks[0].result.as_ref().unwrap().output,
        "late_actual_success"
    );
    assert!(!settled.values.contains_key("late"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_during_native_postclaim_authority_never_enters_callback() {
    struct GateAuthority {
        gate: Arc<Gate>,
        calls: AtomicUsize,
    }
    impl ResourcePolicy for GateAuthority {
        fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
            if context.operation == Operation::RunWait
                && context.stored_route.is_some()
                && self.calls.fetch_add(1, Ordering::SeqCst) == 1
            {
                self.gate.block();
            }
            true
        }
    }
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(GateAuthority {
            gate: gate.clone(),
            calls: AtomicUsize::new(0),
        }),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            None,
            OutputKind::Value,
        )),
    )
    .unwrap();
    let worker_host = host.clone();
    let waiting = tokio::spawn(async move {
        request(
            &worker_host,
            "POST",
            "/threads/no-entry/runs/wait",
            Some(json!({"route":"/echo#workflow"})),
            "app",
        )
        .await
    });
    gate.entered().await;
    let id = ThreadId::new("no-entry").unwrap();
    assert_eq!(
        store
            .thread_view(&id)
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .tasks[0]
            .status,
        TaskStatus::InFlight
    );
    assert_eq!(
        request(&host, "POST", "/threads/no-entry/cancel", None, "app")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    gate.release();
    let settled = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = store
                .thread_view(&id)
                .unwrap()
                .unwrap()
                .execution
                .unwrap()
                .snapshot;
            if snapshot.status == ExecutionStatus::Cancelled {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(settled.tasks[0].status, TaskStatus::Cancelled);
    assert!(settled.tasks[0].result.is_none());
    assert_eq!(settled.attempts_used, 1);
    assert!(contexts.lock().unwrap().is_empty());
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn denied_native_route_never_enters_input_validator_or_callback() {
    struct Deny;
    impl ResourcePolicy for Deny {
        fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
            false
        }
    }
    struct CountValidator(Arc<AtomicUsize>);
    impl InputValidator for CountValidator {
        fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let validations = Arc::new(AtomicUsize::new(0));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(CountStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Deny),
        config(application(
            Arc::new(Echo {
                contexts: contexts.clone(),
            }),
            Some(Arc::new(CountValidator(validations.clone()))),
            OutputKind::Value,
        )),
    )
    .unwrap();
    store.reset();
    let response = request(
        &host,
        "POST",
        "/threads/denied/runs/wait",
        Some(json!({"route":"/echo#workflow"})),
        "app",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(validations.load(Ordering::SeqCst), 0);
    assert!(contexts.lock().unwrap().is_empty());
    let (_, sensitive, mutations) = store.counts();
    assert_eq!((sensitive, mutations), (0, 0));
    assert!(store
        .inner
        .thread_view(&ThreadId::new("denied").unwrap())
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn native_callback_panic_is_static_fault_and_retains_uncertain_attempt() {
    struct Panic;
    impl NativeEffect for Panic {
        fn invoke(
            &self,
            _: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            panic!("private_native_callback_detail")
        }
    }
    let store = Arc::new(MemoryStore::new());
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        config(application(Arc::new(Panic), None, OutputKind::Value)),
    )
    .unwrap();
    let response = request(
        &host,
        "POST",
        "/threads/callback-panic/runs/wait",
        Some(json!({"route":"/echo#workflow"})),
        "app",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body(response).await["error"]["message"], "host_unready");
    let snapshot = store
        .thread_view(&ThreadId::new("callback-panic").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot;
    assert_eq!(snapshot.tasks[0].status, TaskStatus::InFlight);
    assert!(snapshot.tasks[0].result.is_none());
    assert!(snapshot.failure.is_none());
    assert_eq!(
        request(&host, "GET", "/readyz", None, "app").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_authority_wait_reloads_exact_generation_before_validator_and_admission() {
    struct CountValidator(Arc<AtomicUsize>);
    impl InputValidator for CountValidator {
        fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct PolicyGate {
        calls: AtomicUsize,
        selected: usize,
        gate: Arc<Gate>,
    }
    impl ResourcePolicy for PolicyGate {
        fn authorize(&self, _: &Principal, context: &ResourceContext) -> bool {
            if context.operation == Operation::RunWait
                && self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.selected
            {
                self.gate.block();
            }
            true
        }
    }
    // Both producer policy waits load a view before calling application policy.
    // The post-policy reload must see a replacement incarnation in either case.
    for selected in [2, 3] {
        let gate = Arc::new(Gate::default());
        let _release = Release(gate.clone());
        let validations = Arc::new(AtomicUsize::new(0));
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(MemoryStore::new());
        let id = ThreadId::new("policy-aba").unwrap();
        let seed = ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Default::default(),
            origin: Some(ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        };
        store.create_thread_with(seed.clone()).unwrap();
        let before = store.thread_view(&id).unwrap().unwrap();
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(PolicyGate {
                calls: AtomicUsize::new(0),
                selected,
                gate: gate.clone(),
            }),
            config(application(
                Arc::new(Echo {
                    contexts: contexts.clone(),
                }),
                Some(Arc::new(CountValidator(validations.clone()))),
                OutputKind::Value,
            )),
        )
        .unwrap();
        let worker_host = host.clone();
        let waiting = tokio::spawn(async move {
            request(
                &worker_host,
                "POST",
                "/threads/policy-aba/runs/wait",
                Some(json!({"route":"/echo#workflow"})),
                "app",
            )
            .await
        });
        gate.entered().await;
        store
            .delete_thread_fenced(&ThreadDeletionTarget::from_view(&before).unwrap())
            .unwrap();
        let replacement = store.create_thread_with(seed).unwrap();
        gate.release();
        assert_eq!(waiting.await.unwrap().status(), StatusCode::CONFLICT);
        assert_eq!(
            validations.load(Ordering::SeqCst),
            usize::from(selected == 3)
        );
        assert!(contexts.lock().unwrap().is_empty());
        let after = store.thread_view(&id).unwrap().unwrap();
        assert_eq!(after.thread.generation, replacement.generation);
        assert!(after.execution.is_none());
        assert_eq!(
            request(&host, "GET", "/readyz", None, "app").await.status(),
            StatusCode::OK
        );
    }
}
