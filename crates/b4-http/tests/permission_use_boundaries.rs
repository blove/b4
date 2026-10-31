//! Focused public HTTP boundaries for the deliberately narrow reuse host.
//! The independent success/reuse oracle remains in permission_uses.rs.
use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::types::Value as Cell;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tower::ServiceExt;

const APP: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";
const ROUTE: &str = "/reports#agent";
const TASK: &str = "publish-monthly-report";
const EFFECT: &str = "reports.publish-monthly";
const WAIT: Duration = Duration::from_secs(5);
fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: APP.into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        let principal = match headers.get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer owner") => "owner",
            Some("Bearer other") => "other",
            _ => return Err(AuthenticationError),
        };
        Ok(Principal {
            application: APP.into(),
            tenant: "tenant".into(),
            principal: principal.into(),
        })
    }
}
struct Policy {
    deny_resource: bool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, principal: &Principal, resource: &ResourceContext) -> bool {
        if resource.operation == Operation::ReadCapabilities {
            return principal.principal == "owner";
        }
        // Permit both identities here: the HTTP origin boundary must independently
        // reject a valid authenticated identity that does not own the resource.
        !self.deny_resource && resource.origin.as_ref() == Some(&origin())
    }
}
struct Constraint;
impl ToolConstraint for Constraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
struct Publish(Arc<AtomicUsize>);
impl NativeEffect for Publish {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"published":true}),
            writes: vec![],
        }))
    }
}
fn definition() -> Definition {
    Definition {
        route: RouteKey::new(ROUTE).unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new(TASK).unwrap(),
            effect_id: EffectId::new(EFFECT).unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 1,
    }
}
fn config(selected: Definition, calls: Arc<AtomicUsize>) -> PermissionUseHostConfig {
    let effects = selected
        .batches
        .iter()
        .flatten()
        .map(|task| {
            (
                task.effect_id.clone(),
                EffectBinding {
                    callback: Arc::new(Publish(calls.clone())),
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    let registrations = selected
        .batches
        .iter()
        .flatten()
        .map(|task| ToolBindingRegistration {
            definition: selected.clone(),
            effect_id: task.effect_id.clone(),
            tool_id: StableToolId::new("release-tool").unwrap(),
            tool_revision: StableToolRevision::new("tool-1").unwrap(),
            constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
            constraint: Arc::new(Constraint),
        })
        .collect();
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: APP.into(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: selected.clone(),
                effects,
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let catalog = Arc::new(PermissionCatalog::new(application, registrations).unwrap());
    PermissionUseHostConfig::new(
        catalog,
        ApprovalHostConfig::new(
            "policy-1".into(),
            BTreeMap::from([(
                selected.route,
                BTreeMap::from([(TaskId::new(TASK).unwrap(), json!({"question":"Publish?"}))]),
            )]),
        ),
    )
}

// Literal SQL columns cover all nine runtime/authority tables. This intentionally
// does not invoke the host's projection or a PermissionStore serialization helper.
#[derive(Debug, PartialEq)]
struct Raw([Vec<Vec<Cell>>; 9]);
fn raw(path: &Path) -> Raw {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let queries = [
        "SELECT id,schema_version,migration_checksum,owner_epoch FROM metadata ORDER BY id",
        "SELECT id,active_execution_id,created_at_ms,metadata,origin,resource_revision,generation FROM threads ORDER BY id",
        "SELECT id,thread_id,revision,owner_epoch,snapshot FROM executions ORDER BY id",
        "SELECT sequence,kind,thread_id,execution_id,revision,owner_epoch,status,updated_at_ms,payload FROM journal ORDER BY sequence",
        "SELECT execution_id,interrupt_id,task_id,thread_id,thread_generation,task_parking_revision,consumed,record FROM approval_grants ORDER BY execution_id,interrupt_id",
        "SELECT execution_id,turn_id,thread_id,thread_generation,previous_revision,definition_revision,accepted_at_ms,record FROM approval_acceptances ORDER BY execution_id,turn_id",
        "SELECT execution_id,turn_id,thread_id,thread_generation,previous_revision,committed_revision,record_version,record FROM permission_acceptances ORDER BY execution_id,turn_id",
        "SELECT permission_id,thread_id,thread_generation,scope_digest,record_version,record FROM permission_decisions ORDER BY permission_id",
        "SELECT execution_id,task_id,use_id,thread_id,thread_generation,permission_id,turn_id,authorization_revision,record_version,record FROM permission_uses ORDER BY execution_id,task_id",
    ];
    let tables = queries.map(|sql| {
        let mut statement = connection.prepare(sql).unwrap();
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|i| row.get(i))
                    .collect::<Result<Vec<Cell>, _>>()
            })
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    });
    connection.close().unwrap();
    Raw(tables)
}
struct Fixture {
    path: PathBuf,
    store: Arc<SqliteStore>,
    host: HttpHost<SqliteStore>,
    calls: Arc<AtomicUsize>,
    thread: ThreadId,
}
impl Fixture {
    fn new(deny_resource: bool) -> Self {
        let path =
            std::env::temp_dir().join(format!("b4-http-use-boundary-{}", uuid::Uuid::new_v4()));
        let store = Arc::new(SqliteStore::open(&path).unwrap());
        let thread = ThreadId::new(uuid::Uuid::new_v4().to_string()).unwrap();
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(origin()),
            })
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let host = HttpHost::new_with_permission_uses(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy { deny_resource }),
            config(definition(), calls.clone()),
        )
        .unwrap();
        Self {
            path,
            store,
            host,
            calls,
            thread,
        }
    }
    async fn request(
        &self,
        method: &str,
        suffix: &str,
        token: Option<&str>,
        value: Option<Value>,
    ) -> (StatusCode, String) {
        let path = if suffix.starts_with("/b4/") {
            suffix.to_owned()
        } else {
            format!("/threads/{}{suffix}", self.thread)
        };
        let mut request = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let body = value.map(|v| v.to_string()).unwrap_or_default();
        let response = tokio::time::timeout(
            WAIT,
            self.host
                .router()
                .oneshot(request.body(Body::from(body)).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
        let status = response.status();
        let bytes = tokio::time::timeout(WAIT, to_bytes(response.into_body(), 200_000))
            .await
            .unwrap()
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }
    async fn retire(self) {
        let before = raw(&self.path);
        let view = self.store.thread_view(&self.thread).unwrap();
        let owner = self.store.owner_generation();
        let shutdown = self.host.shutdown_handle();
        shutdown.begin_shutdown();
        let drained = shutdown.wait_quiescent(WAIT).await.unwrap();
        drop(self.host);
        drop(shutdown);
        let sole = Arc::strong_count(&self.store) == 1 && Arc::strong_count(&self.calls) == 1;
        let healthy = self.store.readiness().is_ok();
        drop(self.store);
        let reopened = SqliteStore::open(&self.path).unwrap();
        let mut expected = before.0;
        expected[0][0][3] = Cell::Blob((owner + 1).to_be_bytes().to_vec());
        let persisted = raw(&self.path).0 == expected
            && reopened.thread_view(&self.thread).unwrap() == view
            && reopened.owner_generation() == owner + 1
            && reopened.readiness().is_ok();
        drop(reopened);
        std::fs::remove_dir_all(&self.path).unwrap();
        assert!(sole && healthy && persisted && !self.path.exists());
        assert_eq!(drained, ShutdownOutcome::Quiescent { faulted: false });
    }
}
fn json_body(response: &(StatusCode, String)) -> Value {
    serde_json::from_str(&response.1).unwrap()
}

#[tokio::test]
async fn permission_use_constructor_rejects_wider_profiles_before_mutation() {
    let fixture = Fixture::new(false);
    let before = raw(&fixture.path);
    let mut errors = Vec::new();
    for case in 0..7 {
        let mut selected = definition();
        match case {
            3 => selected.batches[0][0].requires_approval = false,
            4 => selected.batches[0][0].recovery = RecoveryPolicy::Idempotent,
            5 => {
                let mut second = selected.batches[0][0].clone();
                second.id = TaskId::new("second").unwrap();
                second.effect_id = EffectId::new("reports.second").unwrap();
                selected.batches[0].push(second);
                selected.max_attempts = 2;
            }
            _ => (),
        }
        let mut configuration = config(selected, fixture.calls.clone());
        match case {
            0 => configuration.limits.max_active_runs = 2,
            1 => configuration.limits.max_concurrent_tasks = 2,
            2 => configuration.limits.max_tasks_per_execution = 2,
            6 => configuration.limits.max_output_bytes = 32_768,
            _ => (),
        }
        errors.push(
            HttpHost::new_with_permission_uses(
                fixture.store.clone(),
                Arc::new(Auth),
                Arc::new(Policy {
                    deny_resource: false,
                }),
                configuration,
            )
            .err()
            .unwrap()
            .0,
        );
    }
    let unchanged = raw(&fixture.path) == before;
    let callbacks = fixture.calls.load(Ordering::SeqCst);
    fixture.retire().await;
    assert_eq!(&errors[..6], &["permission_use_profile"; 6]);
    assert_eq!(errors[6], "permission_use_output_limit");
    assert!(unchanged && callbacks == 0);
}

#[tokio::test]
async fn permission_use_capabilities_endpoint_requires_auth_and_exposes_valid_v3() {
    let fixture = Fixture::new(false);
    let before = raw(&fixture.path);
    let anonymous = fixture.request("GET", "/b4/capabilities", None, None).await;
    let denied = fixture
        .request("GET", "/b4/capabilities", Some("other"), None)
        .await;
    let response = fixture
        .request("GET", "/b4/capabilities", Some("owner"), None)
        .await;
    let descriptor: CapabilityDescriptorV3 = serde_json::from_str(&response.1).unwrap();
    let valid = descriptor.validate().is_ok();
    let features = descriptor.features;
    let unchanged = raw(&fixture.path) == before && fixture.calls.load(Ordering::SeqCst) == 0;
    fixture.retire().await;
    assert_eq!(anonymous.0, StatusCode::UNAUTHORIZED);
    assert_eq!(denied.0, StatusCode::FORBIDDEN);
    assert_eq!(response.0, StatusCode::OK);
    assert!(valid && features.durable_permission_reuse && features.required_once_deny);
    assert!(
        !features.trusted_known_id_recovery
            && !features.fenced_cancellation
            && !features.fenced_deletion
            && !features.always_permissions
    );
    assert!(unchanged);
}

#[tokio::test]
async fn permission_use_wrong_origin_and_resource_policy_refuse_without_mutation() {
    for deny_resource in [false, true] {
        let fixture = Fixture::new(deny_resource);
        let before = raw(&fixture.path);
        let response = fixture
            .request(
                "POST",
                "/runs/wait",
                Some(if deny_resource { "owner" } else { "other" }),
                Some(json!({"route":ROUTE,"input":{}})),
            )
            .await;
        let unchanged = before == raw(&fixture.path) && fixture.calls.load(Ordering::SeqCst) == 0;
        fixture.retire().await;
        assert_eq!(response.0, StatusCode::NOT_FOUND);
        assert_eq!(
            json_body(&response)["error"]["message"],
            "resource_unavailable"
        );
        assert!(unchanged);
    }
}

#[tokio::test]
async fn permission_use_input_byte_depth_and_node_limits_reject_before_admit() {
    let fixture = Fixture::new(false);
    let before = raw(&fixture.path);
    let mut deep = Value::Null;
    for _ in 0..17 {
        deep = json!([deep]);
    }
    let inputs = [json!("x".repeat(4097)), deep, json!(vec![Value::Null; 256])];
    let mut responses = Vec::new();
    for input in inputs {
        let body = json!({"route":ROUTE,"input":input});
        assert!(body.to_string().len() < 65_536);
        responses.push(
            fixture
                .request("POST", "/runs/wait", Some("owner"), Some(body))
                .await,
        );
    }
    let unchanged = raw(&fixture.path) == before && fixture.calls.load(Ordering::SeqCst) == 0;
    fixture.retire().await;
    for response in responses {
        assert_eq!(response.0, StatusCode::BAD_REQUEST);
        assert_eq!(json_body(&response)["error"]["message"], "input_limit");
    }
    assert!(unchanged);
}

#[tokio::test]
async fn permission_use_missing_permission_once_resumes_real_park() {
    missing_permission("once").await;
}
#[tokio::test]
async fn permission_use_missing_permission_deny_resumes_real_park() {
    missing_permission("deny").await;
}

async fn missing_permission(decision: &str) {
    let fixture = Fixture::new(false);
    let initial = raw(&fixture.path);
    let parked_response = fixture
        .request(
            "POST",
            "/runs/wait",
            Some("owner"),
            Some(json!({"route":ROUTE,"input":{"month":"2026-09"}})),
        )
        .await;
    let parked = fixture
        .store
        .thread_view(&fixture.thread)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    let parked_journal = fixture
        .store
        .journal(&parked.snapshot.execution_id)
        .unwrap();
    let parked_raw = raw(&fixture.path);
    let parked_calls = fixture.calls.load(Ordering::SeqCst);
    let pending_response = fixture
        .request("GET", "/pending_interrupts", Some("owner"), None)
        .await;
    let pending = json_body(&pending_response);
    let entry = &pending["interrupts"][0];
    let actual_token = entry["grant"].as_str().unwrap().to_owned();
    let resumed = fixture.request("POST", "/resume", Some("owner"), Some(json!({"route":ROUTE,"resume":[{"status":"resolved","interruptId":entry["interruptId"],"payload":decision,"grant":actual_token}]}))).await;
    let after = fixture
        .store
        .thread_view(&fixture.thread)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    let journal = fixture.store.journal(&after.snapshot.execution_id).unwrap();
    let authority = fixture
        .store
        .approval_view(&after.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let after_raw = raw(&fixture.path);
    let callbacks = fixture.calls.load(Ordering::SeqCst);
    let no_permission = [6, 7, 8].into_iter().all(|index| {
        initial.0[index].is_empty()
            && parked_raw.0[index].is_empty()
            && after_raw.0[index].is_empty()
    });
    // Project the resumed transition with the kernel, using only real framework
    // IDs and durable timestamps. HTTP output alone cannot establish completion.
    let accepted = b4_kernel::transition(
        &parked.snapshot,
        KernelCommand::Resume {
            route: definition().route,
            decisions: vec![Decision {
                interrupt_id: parked.snapshot.tasks[0]
                    .interrupt
                    .as_ref()
                    .unwrap()
                    .id
                    .clone(),
                decision: if decision == "once" {
                    ApprovalDecision::Once
                } else {
                    ApprovalDecision::Deny
                },
            }],
            turn_id: after.snapshot.turn_id.clone(),
        },
        TransitionFacts {
            now_ms: journal[3].updated_at_ms,
        },
    )
    .unwrap()
    .snapshot;
    let mut expected_journal = parked_journal.clone();
    expected_journal.push(JournalEntry::from(&accepted));
    let expected = if decision == "once" {
        let dispatched = b4_kernel::transition(
            &accepted,
            KernelCommand::Dispatch {
                task_id: TaskId::new(TASK).unwrap(),
                attempt_id: after.snapshot.attempt_history[0].clone(),
            },
            TransitionFacts {
                now_ms: journal[4].updated_at_ms,
            },
        )
        .unwrap()
        .snapshot;
        expected_journal.push(JournalEntry::from(&dispatched));
        let completed = b4_kernel::transition(
            &dispatched,
            KernelCommand::Complete {
                task_id: TaskId::new(TASK).unwrap(),
                attempt_id: after.snapshot.attempt_history[0].clone(),
                result: TaskResult {
                    output: json!({"published":true}),
                    writes: vec![],
                },
            },
            TransitionFacts {
                now_ms: journal[5].updated_at_ms,
            },
        )
        .unwrap()
        .snapshot;
        expected_journal.push(JournalEntry::from(&completed));
        completed
    } else {
        accepted
    };
    fixture.retire().await;
    assert_eq!(parked_response.0, StatusCode::OK);
    assert_eq!(parked.snapshot.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(parked.snapshot.revision, 2);
    assert_eq!(
        parked.snapshot.tasks[0].status,
        TaskStatus::WaitingForApproval
    );
    assert_eq!(parked_journal.len(), 3);
    assert_eq!(parked_calls, 0);
    assert_eq!(parked_raw.0[4].len(), 1);
    assert_eq!(pending_response.0, StatusCode::OK);
    assert_eq!(pending["interrupts"].as_array().unwrap().len(), 1);
    assert!(actual_token.starts_with("b4ag_"));
    assert_eq!(resumed.0, StatusCode::OK);
    assert_eq!(after.snapshot.execution_id, parked.snapshot.execution_id);
    assert_ne!(after.snapshot.turn_id, parked.snapshot.turn_id);
    assert_eq!(&journal[..3], &parked_journal);
    assert_eq!(authority.grants.len(), 1);
    assert!(authority.grants[0].consumed);
    assert_eq!(after_raw.0[5].len(), 1);
    assert_eq!(after.snapshot, expected);
    assert_eq!(journal, expected_journal);
    assert!(
        no_permission,
        "Once/Deny must never create permission acceptance, decision, or Use evidence"
    );
    if decision == "once" {
        assert_eq!(callbacks, 1);
        assert_eq!(after.snapshot.status, ExecutionStatus::Completed);
        assert_eq!(after.snapshot.tasks[0].status, TaskStatus::Succeeded);
        assert_eq!(after.snapshot.revision, 5);
        assert_eq!(journal.len(), 6);
        assert_eq!(after.snapshot.attempts_used, 1);
        assert!(resumed.1.contains("\"published\":true"));
    } else {
        assert_eq!(callbacks, 0);
        assert_eq!(after.snapshot.status, ExecutionStatus::Completed);
        assert_eq!(after.snapshot.tasks[0].status, TaskStatus::Denied);
        assert_eq!(after.snapshot.revision, 3);
        assert_eq!(journal.len(), 4);
        assert_eq!(after.snapshot.attempts_used, 0);
        assert!(!resumed.1.contains("\"published\":true"));
    }
}

#[tokio::test]
async fn permission_use_cancel_and_delete_real_park_are_unsupported_without_mutation() {
    let fixture = Fixture::new(false);
    let parked = fixture
        .request(
            "POST",
            "/runs/wait",
            Some("owner"),
            Some(json!({"route":ROUTE,"input":{}})),
        )
        .await;
    let actual = fixture
        .store
        .thread_view(&fixture.thread)
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    let before = raw(&fixture.path);
    let cancel = fixture
        .request("POST", "/cancel", Some("owner"), None)
        .await;
    let delete = fixture.request("DELETE", "", Some("owner"), None).await;
    let unchanged = before == raw(&fixture.path) && fixture.calls.load(Ordering::SeqCst) == 0;
    fixture.retire().await;
    assert_eq!(parked.0, StatusCode::OK);
    assert_eq!(actual.snapshot.status, ExecutionStatus::WaitingForApproval);
    for response in [cancel, delete] {
        assert_eq!(response.0, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            json_body(&response)["error"]["message"],
            "permission_operation_unsupported"
        );
    }
    assert!(unchanged);
}
