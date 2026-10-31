//! Focused HTTP deletion helpers; genuine permission history uses the public HTTP APIs.
pub mod store;
use crate::{
    cooperative::{
        self,
        tcp::{finish, Response, Server},
        Gate,
    },
    support,
};
use b4_http::*;
use b4_runtime::{permissions::*, *};
use rusqlite::{types::Value as Cell, Connection};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
pub struct DeletePolicy {
    pub denied: AtomicBool,
    pub pause: Mutex<Option<Arc<Gate>>>,
}
impl ResourcePolicy for DeletePolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::DeleteThread {
            let gate = self.pause.lock().unwrap().take();
            if gate.is_some_and(|g| !g.hold()) || self.denied.load(Ordering::Acquire) {
                return false;
            }
            assert!(context.requested_route.is_none());
        }
        principal.principal == "owner"
            && principal.application == support::APP
            && principal.tenant == "tenant"
            && context.origin.as_ref().is_none_or(|o| {
                o.application == principal.application
                    && o.tenant == principal.tenant
                    && o.principal == principal.principal
            })
    }
}
pub fn deletion_host<S: PermissionStore + 'static>(
    store: Arc<S>,
    catalog: Arc<PermissionCatalog>,
    policy: Arc<DeletePolicy>,
) -> HttpHost<S> {
    HttpHost::new_with_permission_settled_thread_deletion(
        store,
        Arc::new(support::Auth),
        policy,
        support::config(catalog),
    )
    .unwrap()
}
pub async fn drained<S: Store + 'static>(host: &HttpHost<S>) {
    cooperative::until(|| host.shutdown_handle().status().active_owners == 0).await;
}
pub async fn delete(server: &Server, id: &str) -> Response {
    support::request(server, "DELETE", &format!("/threads/{id}"), "").await
}
pub fn code(response: &Response, status: u16, code: &str) {
    assert_eq!(response.status, status);
    let value: Value = serde_json::from_str(&response.body).unwrap();
    assert_eq!(value["error"]["details"]["code"], code);
    assert!(
        !response.body.contains("permission_id")
            && !response.body.contains("settled_deletion_after_dependents")
    );
}
pub async fn healthy(server: &Server) {
    assert_eq!(
        support::request(server, "GET", "/readyz", "").await.status,
        200
    );
}
pub async fn ordinary_once(server: &Server, id: &str) {
    let parked = support::request(
        server,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        support::RUN,
    )
    .await;
    assert_eq!(parked.status, 200);
    let pending = support::request(
        server,
        "GET",
        &format!("/threads/{id}/pending_interrupts"),
        "",
    )
    .await;
    assert_eq!(pending.status, 200);
    let value: Value = serde_json::from_str(&pending.body).unwrap();
    let interrupts = value["interrupts"].as_array().unwrap();
    assert_eq!(interrupts.len(), 1);
    let item = &interrupts[0];
    let body = json!({"route":support::ROUTE,"resume":[{"status":"resolved","interruptId":item["interruptId"],"payload":"once","grant":item["grant"]}]}).to_string();
    let response = support::request(server, "POST", &format!("/threads/{id}/resume"), &body).await;
    assert_eq!(response.status, 200);
    assert!(!response.body.contains("event: error"));
}
pub async fn complete(server: &Server, id: &str, original: bool) {
    let stream = if original {
        support::original(server, id).await
    } else {
        support::reuse(server, id).await
    };
    let response = finish(stream, Vec::new()).await;
    assert_eq!(response.status, 200);
    assert!(!response.body.contains("event: error"));
}
pub async fn history<S: Store + 'static>(
    server: &Server,
    host: &HttpHost<S>,
    id: &str,
    once: usize,
    uses: usize,
) {
    for _ in 0..once {
        ordinary_once(server, id).await;
        drained(host).await;
    }
    complete(server, id, true).await;
    drained(host).await;
    for _ in 0..uses {
        complete(server, id, false).await;
        drained(host).await;
    }
}
#[derive(Clone, PartialEq)]
pub struct Rows(pub Vec<Vec<Vec<Cell>>>);
pub const TABLES: [(&str, &str, usize); 9] = [
    ("metadata", "id", 1),
    ("threads", "id", 8),
    ("executions", "id", 40),
    ("journal", "sequence", 512),
    ("approval_grants", "execution_id,interrupt_id", 64),
    ("approval_acceptances", "execution_id,turn_id", 64),
    ("permission_acceptances", "execution_id,turn_id", 64),
    ("permission_decisions", "permission_id", 64),
    ("permission_uses", "execution_id,task_id", 64),
];
pub fn db(path: &Path) -> Connection {
    Connection::open(path.join("runtime.sqlite3")).unwrap()
}
pub fn rows(path: &Path) -> Rows {
    let db = Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    Rows(
        TABLES
            .iter()
            .map(|(table, order, cap)| {
                let mut stmt = db
                    .prepare(&format!(
                        "SELECT * FROM {table} ORDER BY {order} LIMIT {}",
                        cap + 1
                    ))
                    .unwrap();
                let columns = stmt.column_count();
                let found = stmt
                    .query_map([], |row| {
                        (0..columns)
                            .map(|i| row.get(i))
                            .collect::<Result<Vec<Cell>, _>>()
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert!(found.len() <= *cap, "bounded physical observer sentinel");
                found
            })
            .collect(),
    )
}
pub fn without(before: &Rows, id: &str) -> Rows {
    Rows(
        before
            .0
            .iter()
            .enumerate()
            .map(|(table, data)| {
                let column = match table {
                    1 => Some(0),
                    2 => Some(1),
                    3 => Some(2),
                    4 => Some(3),
                    5 | 6 => Some(2),
                    7 => Some(1),
                    8 => Some(3),
                    _ => None,
                };
                data.iter()
                    .filter(|row| !column.is_some_and(|c| matches!(&row[c],Cell::Text(s) if s==id)))
                    .cloned()
                    .collect()
            })
            .collect(),
    )
}
pub fn unchanged(path: &Path, before: &Rows) {
    assert!(rows(path) == *before, "all nine tables unchanged");
}
pub fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: support::APP.into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
