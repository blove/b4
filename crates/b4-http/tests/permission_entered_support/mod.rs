//! Real human HTTP Always provisioning and finite loopback observations.
pub mod store;
use crate::cooperative::{
    self,
    tcp::{Response, Server},
    Gate,
};
use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{permissions::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::types::Value as Cell;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
pub const APP: &str = "cd9427d0-2fb8-47cc-9ace-f5651168af32";
pub const ROUTE: &str = "/reports#agent";
pub const TASK: &str = "publish-monthly-report";
pub const EFFECT: &str = "reports.publish-monthly";
pub const RUN: &str = r#"{"route":"/reports#agent","input":{"month":"2026-09"}}"#;
pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, h: &HeaderMap) -> Result<Principal, AuthenticationError> {
        let principal = match h.get("authorization").and_then(|v| v.to_str().ok()) {
            Some("Bearer owner") => "owner",
            Some("Bearer foreign") => "foreign",
            _ => return Err(AuthenticationError),
        };
        Ok(Principal {
            application: APP.into(),
            tenant: "tenant".into(),
            principal: principal.into(),
        })
    }
}
#[derive(Default)]
pub struct Policy {
    pub deny: AtomicBool,
    pub pause: Mutex<Option<Arc<Gate>>>,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, p: &Principal, r: &ResourceContext) -> bool {
        if r.operation == Operation::CancelRun {
            let gate = self.pause.lock().unwrap().take();
            if let Some(g) = gate {
                if !g.hold() {
                    return false;
                }
            }
            if self.deny.load(Ordering::Acquire) {
                return false;
            }
        }
        p.principal == "owner"
            && p.application == APP
            && p.tenant == "tenant"
            && (r.operation == Operation::ReadCapabilities
                || r.origin.as_ref().is_none_or(|o| {
                    o.application == p.application
                        && o.tenant == p.tenant
                        && o.principal == p.principal
                }))
    }
}
#[derive(Default)]
pub struct Constraint(pub AtomicBool);
impl ToolConstraint for Constraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        if self.0.load(Ordering::Acquire) {
            Err(ToolConstraintError::Denied)
        } else {
            Ok(())
        }
    }
}
pub fn catalog(
    callback: Arc<dyn NativeEffect>,
    constraint: Arc<Constraint>,
) -> Arc<PermissionCatalog> {
    let effect = EffectId::new(EFFECT).unwrap();
    let definition = Definition {
        route: RouteKey::new(ROUTE).unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new(TASK).unwrap(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 1,
    };
    let app = Arc::new(
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
    Arc::new(
        PermissionCatalog::new(
            app,
            vec![ToolBindingRegistration {
                definition,
                effect_id: effect,
                tool_id: StableToolId::new("report-publisher").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint,
            }],
        )
        .unwrap(),
    )
}
pub fn config(catalog: Arc<PermissionCatalog>) -> PermissionHostConfig {
    PermissionHostConfig::new(
        catalog,
        ApprovalHostConfig::new(
            "policy-1".into(),
            BTreeMap::from([(
                RouteKey::new(ROUTE).unwrap(),
                BTreeMap::from([(TaskId::new(TASK).unwrap(), json!({"question":"Publish?"}))]),
            )]),
        ),
    )
}
pub fn host<S: PermissionStore + 'static>(
    store: Arc<S>,
    catalog: Arc<PermissionCatalog>,
    policy: Arc<Policy>,
) -> HttpHost<S> {
    HttpHost::new_with_permission_cancellation(store, Arc::new(Auth), policy, config(catalog))
        .unwrap()
}
pub async fn open(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    who: &str,
) -> TcpStream {
    tokio::time::timeout(Duration::from_secs(5),async {
  let mut stream=TcpStream::connect(address).await.unwrap();
  stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {who}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
  stream
 }).await.unwrap()
}
pub async fn request(server: &Server, method: &str, path: &str, body: &str) -> Response {
    cooperative::tcp::finish(
        open(server.address, method, path, body, "owner").await,
        Vec::new(),
    )
    .await
}
pub async fn original(server: &Server, id: &str) -> TcpStream {
    let parked = request(server, "POST", &format!("/threads/{id}/runs/wait"), RUN).await;
    assert_eq!(parked.status, 200);
    let pending = request(
        server,
        "GET",
        &format!("/threads/{id}/pending_interrupts"),
        "",
    )
    .await;
    assert_eq!(pending.status, 200);
    let value: Value = serde_json::from_str(&pending.body).unwrap();
    let rows = value["interrupts"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let i = &rows[0];
    assert!(i["grant"].as_str().unwrap().starts_with("b4ag_"));
    let body=json!({"route":ROUTE,"resume":[{"status":"resolved","interruptId":i["interruptId"],"payload":"always","grant":i["grant"]}]}).to_string();
    open(
        server.address,
        "POST",
        &format!("/threads/{id}/resume"),
        &body,
        "owner",
    )
    .await
}
pub async fn reuse(server: &Server, id: &str) -> TcpStream {
    open(
        server.address,
        "POST",
        &format!("/threads/{id}/runs/stream"),
        RUN,
        "owner",
    )
    .await
}
pub async fn cancel(server: &Server, id: &str) -> Response {
    request(server, "POST", &format!("/threads/{id}/cancel"), "").await
}
pub fn terminal(response: &Response) {
    assert_eq!(response.status, 200);
    let done: Vec<_> = response
        .body
        .split("\n\n")
        .filter(|f| f.lines().any(|l| l == "event: done"))
        .collect();
    assert_eq!(done.len(), 1);
    assert!(done[0].contains(r#""output":{"cancelled":true}"#));
    assert!(!response.body.contains("event: error") && !response.body.contains("event: chunk"));
    assert!(
        !response.body.contains("actual-write") && !response.body.contains("known-native-failure")
    );
}
pub async fn excluded<S: Store + 'static>(
    server: &Server,
    host: &HttpHost<S>,
    path: &Path,
    id: &str,
) {
    let response = request(server, "POST", &format!("/threads/{id}/runs/wait"), RUN).await;
    assert_eq!(response.status, 503);
    assert_eq!(
        serde_json::from_str::<Value>(&response.body).unwrap()["error"]["details"]["code"],
        "capacity_exhausted"
    );
    assert!(matches!(
        SqliteStore::open(path),
        Err(StoreError::AlreadyOwned)
    ));
    assert!(host.shutdown_handle().status().active_owners > 0);
}
pub async fn retire<S: Store + 'static>(host: HttpHost<S>, server: Server, faulted: bool) {
    server.stop().await;
    assert_eq!(
        host.shutdown_handle()
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted }
    );
    drop(host);
}
// Read-only independent cells: every durable table, including old execution and journal rows.
#[derive(Debug, PartialEq)]
pub struct Raw(pub [Vec<Vec<Cell>>; 9]);
pub fn raw(path: &Path) -> Raw {
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
        let mut statement = connection.prepare(&format!("{sql} LIMIT 65")).unwrap();
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|i| row.get(i))
                    .collect::<Result<Vec<Cell>, _>>()
            })
            .unwrap();
        {
            let captured = rows.collect::<Result<Vec<_>, _>>().unwrap();
            assert!(
                captured.len() <= 64,
                "raw evidence exceeded bounded observer"
            );
            captured
        }
    });
    connection.close().unwrap();
    Raw(tables)
}
#[derive(Debug, PartialEq)]
pub struct Capture {
    pub view: ThreadView,
    pub original: StoredExecution,
    pub original_journal: Vec<JournalEntry>,
    pub current_journal: Vec<JournalEntry>,
    pub raw: Raw,
}
pub fn capture(
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
pub fn same_after_reopen(before: &Capture, after: &Capture, owner: u64) -> bool {
    let mut expected = before.raw.0.clone();
    expected[0][0][3] = Cell::Blob(owner.to_be_bytes().to_vec());
    before.view == after.view
        && before.original == after.original
        && before.original_journal == after.original_journal
        && before.current_journal == after.current_journal
        && expected == after.raw.0
}
