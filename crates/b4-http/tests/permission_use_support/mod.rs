//! Shared fixture for focused permission-use lifecycle tests.
use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::types::Value as Cell;
use serde_json::{json, Map};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub(super) const APP: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";
pub(super) const ROUTE: &str = "/reports#agent";
pub(super) const TASK: &str = "publish-monthly-report";
pub(super) const EFFECT: &str = "reports.publish-monthly";
pub(super) const WAIT: Duration = Duration::from_secs(5);
pub(super) fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
pub(super) fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: APP.into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
pub(super) fn context() -> ApprovalContext {
    ApprovalContext {
        actor: origin(),
        policy_revision: "policy-1".into(),
    }
}
pub(super) fn result() -> TaskResult {
    TaskResult {
        output: json!({"published": true}),
        writes: vec![],
    }
}

pub(super) struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer owner") {
            return Err(AuthenticationError);
        }
        let actor = origin();
        Ok(Principal {
            application: actor.application,
            tenant: actor.tenant,
            principal: actor.principal,
        })
    }
}
pub(super) struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, principal: &Principal, resource: &ResourceContext) -> bool {
        principal.application == APP
            && principal.tenant == "tenant"
            && principal.principal == "owner"
            && resource.origin.as_ref() == Some(&origin())
            && resource.thread_id.is_some()
            && [&resource.stored_route, &resource.requested_route]
                .into_iter()
                .all(|route| {
                    route.as_ref().is_none_or(|r| {
                        r.route.as_str() == ROUTE && r.revision.as_str() == "definition-1"
                    })
                })
    }
}
pub(super) struct SetupAuthority;
impl ApprovalAuthority for SetupAuthority {
    fn authorize(
        &self,
        actual: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if actual == &context()
            && resource.thread.origin.as_ref() == Some(&origin())
            && resource.execution.snapshot.definition.route.as_str() == ROUTE
        {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
impl PermissionCancellationAuthority for SetupAuthority {
    fn authorize_cancel(
        &self,
        actual: &ApprovalContext,
        resource: &ApprovalResource,
        target: &RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        self.authorize(actual, ApprovalOperation::Dispatch, resource)
            .map_err(PermissionError::Approval)?;
        if target.execution_fence == (&resource.execution.snapshot).into() {
            Ok(())
        } else {
            Err(PermissionError::Store(StoreError::RevisionConflict))
        }
    }
}
pub(super) struct CatalogAllowConstraint;
impl ToolConstraint for CatalogAllowConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
pub(super) fn fence(store: &SqliteStore, thread: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}

// Read-only independent cells: every durable table, including old execution and journal rows.
#[derive(Debug, PartialEq)]
pub(super) struct Raw(pub(super) [Vec<Vec<Cell>>; 9]);
pub(super) fn raw(path: &Path) -> Raw {
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
pub(super) fn text(cell: &Cell) -> &str {
    match cell {
        Cell::Text(value) => value,
        _ => panic!("expected text"),
    }
}
#[derive(Debug, PartialEq)]
pub(super) struct Capture {
    pub(super) view: ThreadView,
    pub(super) original: StoredExecution,
    pub(super) original_journal: Vec<JournalEntry>,
    pub(super) current_journal: Vec<JournalEntry>,
    pub(super) raw: Raw,
}
pub(super) fn capture(
    store: &SqliteStore,
    path: &Path,
    thread: &ThreadId,
    original: &ExecutionId,
) -> Capture {
    let view = store.thread_view(thread).unwrap().unwrap();
    let current_journal = store
        .journal(&view.execution.as_ref().unwrap().snapshot.execution_id)
        .unwrap();
    Capture {
        view,
        original: store.execution(original).unwrap().unwrap(),
        original_journal: store.journal(original).unwrap(),
        current_journal,
        raw: raw(path),
    }
}
pub(super) fn same_after_reopen(before: &Capture, after: &Capture, owner: u64) -> bool {
    let mut expected = before.raw.0.clone();
    expected[0][0][3] = Cell::Blob(owner.to_be_bytes().to_vec());
    before.view == after.view
        && before.original == after.original
        && before.original_journal == after.original_journal
        && before.current_journal == after.current_journal
        && expected == after.raw.0
}
pub(super) async fn exchange(
    address: std::net::SocketAddr,
    path: &str,
    body: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("POST {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer owner\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(WAIT, stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let raw = String::from_utf8(bytes).unwrap();
    let (headers, body) = raw.split_once("\r\n\r\n").unwrap();
    (
        headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body.into(),
    )
}

pub(super) struct Fixture {
    pub path: std::path::PathBuf,
    pub thread: ThreadId,
    pub original: ExecutionId,
    pub definition: Definition,
    pub store: Arc<SqliteStore>,
    pub catalog: Arc<PermissionCatalog>,
}
impl Fixture {
    // Provision through the same real park -> Always -> Ready cancel -> reopen
    // public ports as the independent success test, without manufacturing records.
    pub fn new(callback: Arc<dyn NativeEffect>, provision_permission: bool) -> Self {
        let path =
            std::env::temp_dir().join(format!("b4-http-use-lifecycle-{}", uuid::Uuid::new_v4()));
        let thread = ThreadId::new(uuid::Uuid::new_v4().to_string()).unwrap();
        let original = ExecutionId::new(uuid::Uuid::new_v4().to_string()).unwrap();
        let task = TaskId::new(TASK).unwrap();
        let effect = EffectId::new(EFFECT).unwrap();
        let definition = Definition {
            route: RouteKey::new(ROUTE).unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task.clone(),
                effect_id: effect.clone(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            }]],
            max_attempts: 1,
        };
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: APP.into(),
                    revision: "application-1".into(),
                },
                vec![RouteRegistration {
                    definition: definition.clone(),
                    effects: BTreeMap::from([(
                        effect.clone(),
                        EffectBinding {
                            callback,
                            output_kind: OutputKind::Value,
                        },
                    )]),
                    input_validator: None,
                }],
            )
            .unwrap(),
        );
        let catalog = Arc::new(
            PermissionCatalog::new(
                application,
                vec![ToolBindingRegistration {
                    definition: definition.clone(),
                    effect_id: effect,
                    tool_id: StableToolId::new("release-tool").unwrap(),
                    tool_revision: StableToolRevision::new("tool-1").unwrap(),
                    constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                    constraint: Arc::new(CatalogAllowConstraint),
                }],
            )
            .unwrap(),
        );
        let store = Arc::new(SqliteStore::open(&path).unwrap());
        let owner = store.owner_generation();
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(origin()),
            })
            .unwrap();
        let initial = new_execution(
            original.clone(),
            TurnId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            owner,
            definition.clone(),
            Map::from_iter([("input".into(), json!({"month":"2026-09"}))]),
            facts(100),
        )
        .unwrap();
        let admitted = store.admit(&thread, initial).unwrap();
        if provision_permission {
            store
                .commit(
                    &original,
                    (&admitted.snapshot).into(),
                    transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
                )
                .unwrap();
            let service = ApprovalService::new(store.clone(), Arc::new(SetupAuthority)).unwrap();
            service
                .park(
                    &context(),
                    &original,
                    fence(&store, &thread),
                    task,
                    InterruptId::new("publish-approval").unwrap(),
                    json!({"question":"Publish?"}),
                    1000,
                    facts(102),
                )
                .unwrap();
            let pending = service
                .pending(&context(), &original, fence(&store, &thread))
                .unwrap();
            drop(service);
            let approved_turn = TurnId::new(uuid::Uuid::new_v4().to_string()).unwrap();
            let ready = accept_permission_always_with_normal_inputs(
                store.clone(),
                Arc::new(SetupAuthority),
                catalog.clone(),
                &original,
                fence(&store, &thread),
                ApprovalAcceptanceRequest {
                    context: context(),
                    route: definition.route.clone(),
                    definition_revision: definition.revision.clone(),
                    turn_id: approved_turn.clone(),
                    now_ms: 103,
                    decisions: vec![GrantDecision {
                        interrupt_id: pending[0].interrupt_id.clone(),
                        decision: ApprovalDecision::Always,
                        cancelled: false,
                        token: Some(pending[0].token.clone()),
                    }],
                },
            )
            .unwrap();
            let target =
                RunCancellationTarget::from_view(&store.thread_view(&thread).unwrap().unwrap())
                    .unwrap();
            let cancelled = cancel_permission_ready_with_normal_inputs(
                store.as_ref(),
                &SetupAuthority,
                catalog.as_ref(),
                &PermissionReadyCancellationRequest {
                    context: &context(),
                    target: &target,
                    facts: facts(104),
                },
                &mut PermissionEvidenceBudget::new(),
            )
            .unwrap();
            assert_eq!(ready.snapshot.revision, 3);
            assert_eq!(
                cancelled.snapshot,
                transition(
                    &ready.snapshot,
                    KernelCommand::Cancel {
                        turn_id: approved_turn,
                        owner_generation: owner
                    },
                    facts(104)
                )
                .unwrap()
                .snapshot
            );
        }
        let provisioned = capture(&store, &path, &thread, &original);
        if provision_permission {
            assert!(produced_permission_matches(&provisioned, &definition));
            assert!(provisioned.raw.0[8].is_empty());
        } else {
            assert_eq!(
                provisioned.original.snapshot.status,
                ExecutionStatus::Accepted
            );
            assert_eq!(provisioned.original.snapshot.revision, 0);
        }
        assert_eq!(Arc::strong_count(&store), 1);
        drop(store);
        let store = Arc::new(SqliteStore::open(&path).unwrap());
        assert_eq!(store.owner_generation(), owner + 1);
        assert!(same_after_reopen(
            &provisioned,
            &capture(&store, &path, &thread, &original),
            owner + 1
        ));
        Self {
            path,
            thread,
            original,
            definition,
            store,
            catalog,
        }
    }
    pub fn host(&self, policy: Arc<dyn ResourcePolicy>) -> HttpHost<SqliteStore> {
        HttpHost::new_with_permission_uses(
            self.store.clone(),
            Arc::new(Auth),
            policy,
            PermissionUseHostConfig::new(
                self.catalog.clone(),
                ApprovalHostConfig::new(
                    "policy-1".into(),
                    BTreeMap::from([(
                        self.definition.route.clone(),
                        BTreeMap::from([(
                            TaskId::new(TASK).unwrap(),
                            json!({"question":"Publish?"}),
                        )]),
                    )]),
                ),
            ),
        )
        .unwrap()
    }
    pub fn capture(&self) -> Capture {
        capture(&self.store, &self.path, &self.thread, &self.original)
    }
    pub fn retire(self) {
        let before = self.capture();
        let owner = self.store.owner_generation();
        let catalog = Arc::downgrade(&self.catalog);
        drop(self.catalog);
        assert!(catalog.upgrade().is_none());
        assert_eq!(Arc::strong_count(&self.store), 1);
        assert!(self.store.readiness().is_ok());
        drop(self.store);
        let reopened = SqliteStore::open(&self.path).unwrap();
        assert_eq!(reopened.owner_generation(), owner + 1);
        assert!(same_after_reopen(
            &before,
            &capture(&reopened, &self.path, &self.thread, &self.original),
            owner + 1
        ));
        assert!(reopened.readiness().is_ok());
        drop(reopened);
        std::fs::remove_dir_all(&self.path).unwrap();
        assert!(!self.path.exists());
    }
}

fn produced_permission_matches(capture: &Capture, definition: &Definition) -> bool {
    let check = || -> Option<bool> {
        if capture.raw.0[4].len() != 1
            || !capture.raw.0[5].is_empty()
            || capture.raw.0[6].len() != 1
            || capture.raw.0[7].len() != 1
        {
            return Some(false);
        }
        let grant: GrantRecord = serde_json::from_str(text(&capture.raw.0[4][0][7])).ok()?;
        let acceptance = PermissionAcceptance::decode_record(
            text(&capture.raw.0[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .ok()?;
        let decision = PermissionDecision::decode_record(
            text(&capture.raw.0[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .ok()?;
        let accepted = acceptance.decisions().first()?;
        Some(
            grant.consumed
                && grant.execution_id == capture.original.snapshot.execution_id
                && grant.task_id.as_str() == TASK
                && grant.effect_id.as_str() == EFFECT
                && grant.task_parking_revision == 2
                && acceptance.before_snapshot().revision == 2
                && acceptance.committed_revision() == 3
                && acceptance.origin() == &origin()
                && acceptance.application().application() == APP
                && acceptance.application().revision() == "application-1"
                && acceptance.policy_revision() == "policy-1"
                && accepted.decision() == ApprovalDecision::Always
                && accepted.permission_id() == Some(decision.permission_id())
                && decision.creator_execution_id() == &grant.execution_id
                && decision.creator_task_id() == &grant.task_id
                && decision.creator_interrupt_id() == &grant.interrupt_id
                && decision.scope().definition() == definition
                && decision.scope().origin() == &origin()
                && decision.scope().thread_id() == &capture.view.thread.id
                && decision.scope().thread_generation() == &capture.view.thread.generation
                && decision.scope().policy_revision() == "policy-1",
        )
    };
    check() == Some(true)
}
