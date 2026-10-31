//! Fresh HTTP Always creation, durable original completion, and reopened reuse.
use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_kernel::{authorize_ready, new_execution, transition, PermissionAuthorizationCommand};
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::types::Value as Cell;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
const APP: &str = "cd9427d0-2fb8-47cc-9ace-f5651168af32";
const ROUTE: &str = "/reports#agent";
const TASK: &str = "publish-monthly-report";
const EFFECT: &str = "reports.publish-monthly";
const WAIT: Duration = Duration::from_secs(5);
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: APP.into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
fn result() -> TaskResult {
    TaskResult {
        output: json!({"published":true}),
        writes: vec![],
    }
}
fn input() -> Map<String, Value> {
    Map::from_iter([("input".into(), json!({"month":"2026-09"}))])
}
struct Auth;
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
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, principal: &Principal, resource: &ResourceContext) -> bool {
        principal.application == APP
            && principal.tenant == "tenant"
            && principal.principal == "owner"
            && (resource.operation == Operation::ReadCapabilities
                || (resource.origin.as_ref() == Some(&origin()) && resource.thread_id.is_some()))
            && [&resource.stored_route, &resource.requested_route]
                .into_iter()
                .all(|route| {
                    route.as_ref().is_none_or(|r| {
                        r.route.as_str() == ROUTE && r.revision.as_str() == "definition-1"
                    })
                })
    }
}
// Read-only independent cells: every durable table, including old execution and journal rows.
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
        let mut statement = connection.prepare(&format!("{sql} LIMIT 13")).unwrap();
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
                captured.len() <= 12,
                "raw evidence exceeded bounded observer"
            );
            captured
        }
    });
    connection.close().unwrap();
    Raw(tables)
}
fn text(cell: &Cell) -> &str {
    match cell {
        Cell::Text(value) => value,
        _ => panic!("expected text"),
    }
}
#[derive(Debug, PartialEq)]
struct Capture {
    view: ThreadView,
    original: StoredExecution,
    original_journal: Vec<JournalEntry>,
    current_journal: Vec<JournalEntry>,
    raw: Raw,
}
fn capture(store: &SqliteStore, path: &Path, thread: &ThreadId, original: &ExecutionId) -> Capture {
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
fn same_after_reopen(before: &Capture, after: &Capture, owner: u64) -> bool {
    let mut expected = before.raw.0.clone();
    expected[0][0][3] = Cell::Blob(owner.to_be_bytes().to_vec());
    before.view == after.view
        && before.original == after.original
        && before.original_journal == after.original_journal
        && before.current_journal == after.current_journal
        && expected == after.raw.0
}

struct Allow;
impl ToolConstraint for Allow {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
#[derive(Clone)]
struct Call {
    invocation: EffectInvocation,
    context: InvocationContext,
    dispatched: StoredExecution,
}
type Calls = Arc<Mutex<Vec<Call>>>;
struct Publish {
    store: Weak<SqliteStore>,
    calls: Calls,
}
impl NativeEffect for Publish {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        let dispatched = self
            .store
            .upgrade()
            .unwrap()
            .execution(&invocation.execution_id)
            .unwrap()
            .unwrap();
        self.calls.lock().unwrap().push(Call {
            invocation: invocation.clone(),
            context: context.clone(),
            dispatched,
        });
        Ok(NativeOutcome::Succeeded(result()))
    }
}
fn make_catalog(store: &Arc<SqliteStore>, calls: Calls) -> Arc<PermissionCatalog> {
    catalog_with_callback(Arc::new(Publish {
        store: Arc::downgrade(store),
        calls,
    }))
}
fn catalog_with_callback(callback: Arc<dyn NativeEffect>) -> Arc<PermissionCatalog> {
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
    Arc::new(
        PermissionCatalog::new(
            application,
            vec![ToolBindingRegistration {
                definition,
                effect_id: effect,
                tool_id: StableToolId::new("report-publisher").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: Arc::new(Allow),
            }],
        )
        .unwrap(),
    )
}
fn host(store: Arc<SqliteStore>, catalog: Arc<PermissionCatalog>) -> HttpHost<SqliteStore> {
    HttpHost::new_with_permissions(
        store,
        Arc::new(Auth),
        Arc::new(Policy),
        PermissionHostConfig::new(
            catalog,
            ApprovalHostConfig::new(
                "policy-1".into(),
                BTreeMap::from([(
                    RouteKey::new(ROUTE).unwrap(),
                    BTreeMap::from([(
                        TaskId::new(TASK).unwrap(),
                        json!({"question":"Publish monthly report?"}),
                    )]),
                )]),
            ),
        ),
    )
    .unwrap()
}
struct Server {
    host: HttpHost<SqliteStore>,
    address: std::net::SocketAddr,
    stop: oneshot::Sender<()>,
    listener: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(host: HttpHost<SqliteStore>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = host.router();
        let (stop, stopped) = oneshot::channel();
        let listener = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            host,
            address,
            stop,
            listener,
        }
    }
    async fn retire(self) -> bool {
        self.retire_with_fault(false).await
    }
    async fn retire_with_fault(self, faulted: bool) -> bool {
        let shutdown = self.host.shutdown_handle();
        shutdown.begin_shutdown();
        let quiet = shutdown.wait_quiescent(WAIT).await.unwrap();
        let sent = self.stop.send(()).is_ok();
        let mut listener = self.listener;
        let joined = match tokio::time::timeout(WAIT, &mut listener).await {
            Ok(result) => result.is_ok(),
            Err(_) => {
                // Hard abort belongs only to failed retirement; never report it as quiet.
                listener.abort();
                let _ = listener.await;
                false
            }
        };
        drop(shutdown);
        drop(self.host);
        if quiet != (ShutdownOutcome::Quiescent { faulted }) || !sent || !joined {
            eprintln!(
                "retire quiet={quiet:?} sent={sent} joined={joined} expected_faulted={faulted}"
            );
        }
        quiet == ShutdownOutcome::Quiescent { faulted } && sent && joined
    }
    async fn request(&self, method: &str, path: &str, body: Value) -> (u16, String) {
        self.request_as(method, path, body, "Bearer owner").await
    }
    async fn request_as(
        &self,
        method: &str,
        path: &str,
        body: Value,
        authorization: &str,
    ) -> (u16, String) {
        let body = if method == "GET" {
            String::new()
        } else {
            body.to_string()
        };
        let mut stream = TcpStream::connect(self.address).await.unwrap();
        stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: {authorization}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", self.address, body.len()).as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(WAIT, stream.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let response = String::from_utf8(bytes).unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        // JSON responses have a fixed Content-Length. Resume is SSE/chunked;
        // its transport status is observed here, durable completion below.
        (
            headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
            body.into(),
        )
    }
}
fn callback_matches(call: &Call, snapshot: &Snapshot, thread: &ThreadRecord) -> bool {
    callback_matches_input(call, snapshot, thread, &input())
}
fn callback_matches_input(
    call: &Call,
    snapshot: &Snapshot,
    thread: &ThreadRecord,
    expected_input: &Map<String, Value>,
) -> bool {
    call.invocation
        == EffectInvocation {
            execution_id: snapshot.execution_id.clone(),
            turn_id: snapshot.turn_id.clone(),
            owner_generation: snapshot.owner_generation,
            task_id: TaskId::new(TASK).unwrap(),
            effect_id: EffectId::new(EFFECT).unwrap(),
            attempt_id: snapshot.attempt_history[0].clone(),
            input: expected_input.clone(),
        }
        && call.context
            == InvocationContext {
                thread_id: thread.id.clone(),
                thread_generation: thread.generation.clone(),
                origin: origin(),
                application_revision: "application-1".into(),
                execution_revision: snapshot.definition.revision.clone(),
                policy_revision: "policy-1".into(),
            }
}
// Failure diagnostics identify locations only, never grant/token bodies.
fn raw_matches(label: &str, actual: &Raw, expected: &Raw) -> bool {
    let tables = [
        "metadata",
        "threads",
        "executions",
        "journal",
        "approval_grants",
        "approval_acceptances",
        "permission_acceptances",
        "permission_decisions",
        "permission_uses",
    ];
    for (table, (actual_rows, expected_rows)) in actual.0.iter().zip(&expected.0).enumerate() {
        if actual_rows.len() != expected_rows.len() {
            eprintln!(
                "{label}: table={} actual_rows={} expected_rows={}",
                tables[table],
                actual_rows.len(),
                expected_rows.len()
            );
            return false;
        }
        for (row, (actual_cells, expected_cells)) in
            actual_rows.iter().zip(expected_rows).enumerate()
        {
            if let Some(column) = actual_cells
                .iter()
                .zip(expected_cells)
                .position(|(actual, expected)| actual != expected)
            {
                eprintln!("{label}: table={} row={row} column={column}", tables[table]);
                return false;
            }
        }
    }
    actual == expected
}
// Project SQL column semantics independently from the actual post-flow rows.
// Canonical authority bodies enter only after their typed provenance checks.
fn projected_raw(before: &Raw, thread: &ThreadRecord, snapshots: &[&Snapshot]) -> Raw {
    let mut expected = before.0.clone();
    let complete = snapshots.last().unwrap();
    expected[1][0][1] = Cell::Text(complete.execution_id.to_string());
    expected[1][0][5] = blob(thread.resource_revision + snapshots.len() as u64);
    expected[2].push(vec![
        Cell::Text(complete.execution_id.to_string()),
        Cell::Text(thread.id.to_string()),
        blob(complete.revision),
        blob(complete.owner_generation),
        json_cell(complete),
    ]);
    expected[2].sort_by(|left, right| text(&left[0]).cmp(text(&right[0])));
    let mut sequence = match expected[3].last().unwrap()[0] {
        Cell::Integer(value) => value,
        _ => panic!("journal sequence must be an integer"),
    };
    for (index, snapshot) in snapshots.iter().enumerate() {
        sequence += 1;
        let entry = JournalEntry::from(*snapshot);
        expected[3].push(vec![
            Cell::Integer(sequence),
            Cell::Text(if index == 0 { "admit" } else { "commit" }.into()),
            Cell::Text(thread.id.to_string()),
            Cell::Text(snapshot.execution_id.to_string()),
            blob(entry.revision),
            blob(snapshot.owner_generation),
            json_cell(&entry.status),
            blob(entry.updated_at_ms),
            json_cell(&entry),
        ]);
    }
    Raw(expected)
}
fn blob(value: u64) -> Cell {
    Cell::Blob(value.to_be_bytes().to_vec())
}
fn json_cell(value: &impl serde::Serialize) -> Cell {
    Cell::Text(serde_json::to_string(value).unwrap())
}
fn encoded_cell(bytes: Vec<u8>) -> Cell {
    Cell::Text(String::from_utf8(bytes).unwrap())
}
fn original_authority_rows(
    expected: &mut Raw,
    parked_grant: &GrantRecord,
    acceptance: &PermissionAcceptance,
    decision: &PermissionDecision,
) {
    let mut grant = parked_grant.clone();
    grant.consumed = true;
    expected.0[4].push(vec![
        Cell::Text(grant.execution_id.to_string()),
        Cell::Text(grant.interrupt_id.to_string()),
        Cell::Text(grant.task_id.to_string()),
        Cell::Text(grant.thread_id.to_string()),
        Cell::Text(grant.thread_generation.to_string()),
        blob(grant.task_parking_revision),
        Cell::Integer(1),
        // The consumed overlay uses the canonical alphabetical JSON writer.
        json_cell(&serde_json::to_value(&grant).unwrap()),
    ]);
    expected.0[6].push(vec![
        Cell::Text(grant.execution_id.to_string()),
        Cell::Text(acceptance.turn_id().to_string()),
        Cell::Text(grant.thread_id.to_string()),
        Cell::Text(grant.thread_generation.to_string()),
        blob(2),
        blob(3),
        Cell::Integer(1),
        encoded_cell(
            acceptance
                .encode_record(&mut PermissionEvidenceBudget::new())
                .unwrap(),
        ),
    ]);
    expected.0[7].push(vec![
        Cell::Text(decision.permission_id().as_str().into()),
        Cell::Text(grant.thread_id.to_string()),
        Cell::Text(grant.thread_generation.to_string()),
        Cell::Text(decision.scope_digest().into()),
        Cell::Integer(1),
        encoded_cell(
            decision
                .encode_record(&mut PermissionEvidenceBudget::new())
                .unwrap(),
        ),
    ]);
}
fn reuse_authority_row(
    expected: &mut Raw,
    thread: &ThreadRecord,
    record: &PermissionUse,
    complete: &Snapshot,
) {
    expected.0[8].push(vec![
        Cell::Text(complete.execution_id.to_string()),
        Cell::Text(TASK.into()),
        Cell::Text(record.use_id().as_str().into()),
        Cell::Text(thread.id.to_string()),
        Cell::Text(thread.generation.to_string()),
        Cell::Text(record.permission_id().as_str().into()),
        Cell::Text(complete.turn_id.to_string()),
        blob(2),
        Cell::Integer(1),
        encoded_cell(
            record
                .encode_record(&mut PermissionEvidenceBudget::new())
                .unwrap(),
        ),
    ]);
}
struct OriginalExpectation<'a> {
    thread: &'a ThreadRecord,
    input: &'a Map<String, Value>,
}
fn original_kernel(
    before: &Raw,
    expected: OriginalExpectation<'_>,
    parked_grant: &GrantRecord,
    capture: &Capture,
    acceptance: &PermissionAcceptance,
    decision: &PermissionDecision,
    call: &Call,
) -> bool {
    let thread = expected.thread;
    let current = &capture.original.snapshot;
    let journal = &capture.original_journal;
    if journal.len() != 6 || current.attempt_history.len() != 1 {
        return false;
    }
    let waiting = acceptance.before_snapshot();
    let task = TaskId::new(TASK).unwrap();
    let pending = waiting.tasks[0].interrupt.as_ref().unwrap();
    let initial = new_execution(
        current.execution_id.clone(),
        waiting.turn_id.clone(),
        current.owner_generation,
        current.definition.clone(),
        expected.input.clone(),
        facts(journal[0].updated_at_ms),
    )
    .unwrap();
    let start = transition(
        &initial,
        KernelCommand::Start,
        facts(journal[1].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let park = transition(
        &start,
        KernelCommand::Park {
            task_id: task.clone(),
            interrupt_id: pending.id.clone(),
            prompt: json!({"question":"Publish monthly report?"}),
        },
        facts(journal[2].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let ready = transition(
        &park,
        KernelCommand::Resume {
            route: current.definition.route.clone(),
            decisions: vec![Decision {
                interrupt_id: pending.id.clone(),
                decision: ApprovalDecision::Always,
            }],
            turn_id: acceptance.turn_id().clone(),
        },
        acceptance.facts().clone(),
    )
    .unwrap()
    .snapshot;
    let dispatched = transition(
        &ready,
        KernelCommand::Dispatch {
            task_id: task.clone(),
            attempt_id: current.attempt_history[0].clone(),
        },
        facts(journal[4].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let complete = transition(
        &dispatched,
        KernelCommand::Complete {
            task_id: task,
            attempt_id: current.attempt_history[0].clone(),
            result: result(),
        },
        facts(journal[5].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let snapshots = [&initial, &start, &park, &ready, &dispatched, &complete];
    let mut expected = projected_raw(before, thread, &snapshots);
    original_authority_rows(&mut expected, parked_grant, acceptance, decision);
    let raw_valid = raw_matches("original raw", &capture.raw, &expected);
    raw_valid
        && blob(current.owner_generation) == before.0[0][0][3]
        && park == *waiting
        && acceptance.committed_revision() == 3
        && ready.revision == 3
        && call.dispatched.snapshot == dispatched
        && complete == *current
        && current.revision == 5
        && current.status == ExecutionStatus::Completed
        && current.tasks[0].status == TaskStatus::Succeeded
        && current.attempts_used == 1
        && [&initial, &start, &park, &ready, &dispatched, &complete]
            .map(JournalEntry::from)
            .as_slice()
            == journal
}
fn reused_kernel(
    before: &Capture,
    capture: &Capture,
    record: &PermissionUse,
    call: &Call,
    expected_input: &Map<String, Value>,
) -> bool {
    let current = &capture.view.execution.as_ref().unwrap().snapshot;
    let journal = &capture.current_journal;
    if journal.len() != 5 || current.attempt_history.len() != 1 {
        return false;
    }
    let task = TaskId::new(TASK).unwrap();
    let initial = new_execution(
        current.execution_id.clone(),
        current.turn_id.clone(),
        current.owner_generation,
        current.definition.clone(),
        expected_input.clone(),
        facts(journal[0].updated_at_ms),
    )
    .unwrap();
    let start = transition(
        &initial,
        KernelCommand::Start,
        facts(journal[1].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let command =
        PermissionAuthorizationCommand::capture(&start, task.clone(), record.use_id().clone())
            .unwrap();
    let ready = authorize_ready(&start, &command, facts(journal[2].updated_at_ms))
        .unwrap()
        .snapshot;
    let dispatched = transition(
        &ready,
        KernelCommand::Dispatch {
            task_id: task.clone(),
            attempt_id: current.attempt_history[0].clone(),
        },
        facts(journal[3].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let complete = transition(
        &dispatched,
        KernelCommand::Complete {
            task_id: task,
            attempt_id: current.attempt_history[0].clone(),
            result: result(),
        },
        facts(journal[4].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let snapshots = [&initial, &start, &ready, &dispatched, &complete];
    let mut expected = projected_raw(&before.raw, &before.view.thread, &snapshots);
    reuse_authority_row(&mut expected, &before.view.thread, record, &complete);
    raw_matches("reuse raw", &capture.raw, &expected)
        && record.before_snapshot() == &start
        && record.facts() == &facts(journal[2].updated_at_ms)
        && record.committed_revision() == 2
        && call.dispatched.snapshot == dispatched
        && complete == *current
        && current.revision == 4
        && current.status == ExecutionStatus::Completed
        && current.tasks[0].status == TaskStatus::Succeeded
        && current.attempts_used == 1
        && [&initial, &start, &ready, &dispatched, &complete]
            .map(JournalEntry::from)
            .as_slice()
            == journal
}

// HTTP counts values; canonical snapshot accounting also counts every object key.
fn input_geometry(value: &Value) -> (usize, usize, usize, usize) {
    fn walk(value: &Value, depth: usize) -> (usize, usize, usize) {
        let mut values = 1;
        let mut canonical = 1;
        let mut deepest = depth;
        let children: Vec<_> = match value {
            Value::Array(children) => children.iter().collect(),
            Value::Object(children) => {
                canonical += children.len();
                children.values().collect()
            }
            _ => Vec::new(),
        };
        for child in children {
            let (child_values, child_canonical, child_depth) = walk(child, depth + 1);
            values += child_values;
            canonical += child_canonical;
            deepest = deepest.max(child_depth);
        }
        (values, canonical, deepest)
    }
    let (values, canonical, depth) = walk(value, 0);
    (
        serde_json::to_vec(value).unwrap().len(),
        values,
        canonical,
        depth,
    )
}

#[tokio::test]
async fn http_always_creates_completes_and_reuses_after_reopen() {
    always_completes_and_reuses_after_reopen(input(), input()).await;
}

#[tokio::test]
async fn http_structured_original_completes_and_reuses_after_reopen() {
    let original = json!({
        "report": {
            "title": "月次 Café 🚀",
            "sections": [
                {"name": "north", "values": [1, 2, null],
                 "options": {"enabled": true, "limit": 3, "cursor": null}},
                {"name": "南", "values": [3, 4, 5],
                 "options": {"enabled": false, "limit": null, "cursor": "次"}}
            ]
        },
        "options": {"notify": ["ops", "分析"], "dryRun": false, "note": null}
    });
    let reused = json!({"report": {"month": "十一月", "publish": true},
                        "options": {"notify": ["分析", null], "cursor": null}});
    let (input_bytes, input_values, _, input_depth) = input_geometry(&original);
    assert_eq!((input_bytes, input_values, input_depth), (294, 30, 5));
    let (reuse_bytes, reuse_values, _, reuse_depth) = input_geometry(&reused);
    assert!(reuse_bytes <= 4096 && reuse_values <= 256 && reuse_depth <= 16);
    let (parked_bytes, _, parked_nodes, _) = always_completes_and_reuses_after_reopen(
        Map::from_iter([("input".into(), original)]),
        Map::from_iter([("input".into(), reused)]),
    )
    .await;
    println!(
        "http-structured-original: input_bytes={input_bytes} input_values={input_values} input_depth={input_depth} reuse_bytes={reuse_bytes} reuse_values={reuse_values} reuse_depth={reuse_depth} parked_bytes={parked_bytes} parked_canonical_nodes={parked_nodes}"
    );
    assert!(parked_bytes <= 4096 && parked_nodes > 91 && parked_nodes <= 512);
}

#[tokio::test]
async fn http_text_original_completes_and_reuses_after_reopen() {
    let notes = "a".repeat(2048);
    let reuse_notes = "b".repeat(2048);
    assert!(notes.is_ascii() && reuse_notes.is_ascii());
    assert_eq!(notes.len(), 2048);
    assert_eq!(reuse_notes.len(), 2048);
    assert_ne!(notes, reuse_notes);
    let original = json!({"notes": notes});
    let reused = json!({"notes": reuse_notes});
    let (input_bytes, input_values, _, input_depth) = input_geometry(&original);
    let (reuse_bytes, reuse_values, _, reuse_depth) = input_geometry(&reused);
    assert!(input_bytes <= 4096 && input_values <= 256 && input_depth <= 16);
    assert!(reuse_bytes <= 4096 && reuse_values <= 256 && reuse_depth <= 16);
    let (parked_bytes, _, parked_nodes, _) = always_completes_and_reuses_after_reopen(
        Map::from_iter([("input".into(), original)]),
        Map::from_iter([("input".into(), reused)]),
    )
    .await;
    println!(
        "http-text-original: input_bytes={input_bytes} input_values={input_values} input_depth={input_depth} reuse_bytes={reuse_bytes} reuse_values={reuse_values} reuse_depth={reuse_depth} parked_bytes={parked_bytes} parked_canonical_nodes={parked_nodes}"
    );
    assert!(parked_bytes > 4096 && parked_bytes <= 8192 && parked_nodes <= 512);
}

async fn always_completes_and_reuses_after_reopen(
    original_input: Map<String, Value>,
    reused_input: Map<String, Value>,
) -> (usize, usize, usize, usize) {
    let directory =
        std::env::temp_dir().join(format!("b4-http-original-always-{}", uuid::Uuid::new_v4()));
    let thread = ThreadId::new(uuid::Uuid::new_v4().to_string()).unwrap();
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(SqliteStore::open(&directory).unwrap());
    let owner1 = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(origin()),
        })
        .unwrap();
    let empty = raw(&directory);
    let before_thread = store.thread(&thread).unwrap().unwrap();
    let fresh = empty.0[2..].iter().enumerate().all(|(index, rows)| {
        if index == 1 {
            rows.len() == 1
        } else {
            rows.is_empty()
        }
    });
    let catalog = make_catalog(&store, calls.clone());
    let catalog_weak = Arc::downgrade(&catalog);
    let server = Server::start(host(store.clone(), catalog.clone())).await;
    let metadata = server.request("GET", "/b4/capabilities", Value::Null).await;
    let published: Result<CapabilityDescriptorV4, _> = serde_json::from_str(&metadata.1);
    let descriptor_valid = metadata.0 == 200
        && published.is_ok_and(|descriptor| {
            descriptor.validate().is_ok()
                && descriptor.descriptor_version == 4
                && descriptor.features.durable_permission_creation
                && descriptor.features.durable_permission_reuse
                && !descriptor.features.always_permissions
                && !descriptor.features.trusted_known_id_recovery
                && !descriptor.features.fenced_cancellation
                && !descriptor.features.fenced_deletion
                && descriptor
                    .application
                    .as_ref()
                    .is_some_and(|app| app.id == APP && app.revision == "application-1")
                && descriptor.limits.active_runs == 1
                && descriptor.limits.concurrent_tasks == 1
                && descriptor.limits.concurrent_tasks_per_execution == 1
                && descriptor.limits.output_bytes == 65_536
        });
    let first = server
        .request(
            "POST",
            &format!("/threads/{thread}/runs/wait"),
            json!({"route":ROUTE,"input":original_input["input"]}),
        )
        .await;
    let waiting = store.thread_view(&thread).unwrap().unwrap();
    let original = waiting
        .execution
        .as_ref()
        .unwrap()
        .snapshot
        .execution_id
        .clone();
    let parked = capture(&store, &directory, &thread, &original);
    let parked_geometry = input_geometry(&serde_json::to_value(&parked.original.snapshot).unwrap());
    let first_waited = first.0 == 200
        && parked.original.snapshot.values == original_input
        && parked.original.snapshot.tasks[0].input == original_input
        && parked.original.snapshot.revision == 2
        && parked.original.snapshot.status == ExecutionStatus::WaitingForApproval
        && parked
            .original_journal
            .iter()
            .map(|entry| entry.revision)
            .eq(0..=2)
        && parked.raw.0[4].len() == 1
        && parked.raw.0[5..].iter().all(Vec::is_empty)
        && calls.lock().unwrap().is_empty();
    let pending = server
        .request(
            "GET",
            &format!("/threads/{thread}/pending_interrupts"),
            Value::Null,
        )
        .await;
    let pending_json: Value = serde_json::from_str(&pending.1).unwrap();
    let interrupts = pending_json["interrupts"].as_array().unwrap();
    let interrupt = &interrupts[0];
    let pending_matches = pending.0 == 200
        && interrupts.len() == 1
        && interrupt["interruptId"]
            == json!(
                parked.original.snapshot.tasks[0]
                    .interrupt
                    .as_ref()
                    .unwrap()
                    .id
            )
        && interrupt["value"] == json!({"question":"Publish monthly report?"})
        && interrupt["grant"]
            .as_str()
            .is_some_and(|grant| grant.starts_with("b4ag_"))
        && interrupt["resumeKey"].is_string();
    let resume = server.request("POST", &format!("/threads/{thread}/resume"), json!({"route":ROUTE,"resume":[{"status":"resolved","interruptId":interrupt["interruptId"],"payload":"always","grant":interrupt["grant"]}]})).await;
    let first_drained = server.retire().await;
    let completed = capture(&store, &directory, &thread, &original);
    let original_calls = calls.lock().unwrap().clone();
    let parked_grant: GrantRecord = serde_json::from_str(text(&parked.raw.0[4][0][7])).unwrap();
    let original_shape = completed.raw.0[4].len() == 1
        && completed.raw.0[5].is_empty()
        && completed.raw.0[6].len() == 1
        && completed.raw.0[7].len() == 1
        && completed.raw.0[8].is_empty();
    // Decode the physical durable records, never replace them with manufactured witnesses.
    let grant: GrantRecord = serde_json::from_str(text(&completed.raw.0[4][0][7])).unwrap();
    let acceptance = PermissionAcceptance::decode_record(
        text(&completed.raw.0[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let decision = PermissionDecision::decode_record(
        text(&completed.raw.0[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let accepted = &acceptance.decisions()[0];
    let provenance = !parked_grant.consumed
        && parked_grant.token.as_str() == interrupt["grant"].as_str().unwrap()
        && grant.consumed
        && grant.execution_id == original
        && grant.task_id.as_str() == TASK
        && grant.effect_id.as_str() == EFFECT
        && grant.task_parking_revision == 2
        && acceptance.before_snapshot() == &parked.original.snapshot
        && acceptance.before_snapshot().values == original_input
        && acceptance.before_snapshot().tasks[0].input == original_input
        && completed.original.snapshot.values == original_input
        && completed.original.snapshot.tasks[0].input == original_input
        && acceptance.thread_id() == &thread
        && acceptance.thread_generation() == &completed.view.thread.generation
        && acceptance.origin() == &origin()
        && acceptance.application().application() == APP
        && acceptance.application().revision() == "application-1"
        && acceptance.policy_revision() == "policy-1"
        && accepted.decision() == ApprovalDecision::Always
        && accepted.permission_id() == Some(decision.permission_id())
        && decision.creator_execution_id() == &original
        && decision.creator_turn_id() == acceptance.turn_id()
        && decision.creator_task_id() == &grant.task_id
        && decision.creator_interrupt_id() == &grant.interrupt_id
        && decision.scope().thread_id() == &thread
        && decision.scope().thread_generation() == &completed.view.thread.generation
        && decision.scope().origin() == &origin()
        && decision.scope().definition() == &completed.original.snapshot.definition
        && decision.scope().policy_revision() == "policy-1"
        && decision.scope().tool_id().as_str() == "report-publisher"
        && completed.raw.0[6][0][0] == Cell::Text(original.to_string())
        && completed.raw.0[6][0][4] == Cell::Blob(2_u64.to_be_bytes().to_vec())
        && completed.raw.0[6][0][5] == Cell::Blob(3_u64.to_be_bytes().to_vec())
        && completed.raw.0[7][0][0] == Cell::Text(decision.permission_id().as_str().into());
    let original_valid = original_calls.len() == 1
        && original_calls.first().is_some_and(|call| {
            original_kernel(
                &empty,
                OriginalExpectation {
                    thread: &before_thread,
                    input: &original_input,
                },
                &parked_grant,
                &completed,
                &acceptance,
                &decision,
                call,
            ) && callback_matches_input(
                call,
                &completed.original.snapshot,
                &completed.view.thread,
                &original_input,
            )
        });
    drop(catalog);
    let released1 = catalog_weak.upgrade().is_none()
        && Arc::strong_count(&store) == 1
        && store.readiness().is_ok();
    drop(store);
    let store = Arc::new(SqliteStore::open(&directory).unwrap());
    let owner2 = store.owner_generation();
    let reopened = capture(&store, &directory, &thread, &original);
    let persisted1 = owner2 == owner1 + 1 && same_after_reopen(&completed, &reopened, owner2);
    let catalog = make_catalog(&store, calls.clone());
    let catalog_weak = Arc::downgrade(&catalog);
    let server = Server::start(host(store.clone(), catalog.clone())).await;
    let reused_response = server
        .request(
            "POST",
            &format!("/threads/{thread}/runs/wait"),
            json!({"route":ROUTE,"input":reused_input["input"]}),
        )
        .await;
    let pending_after = server
        .request(
            "GET",
            &format!("/threads/{thread}/pending_interrupts"),
            Value::Null,
        )
        .await;
    let second_drained = server.retire().await;
    let reused = capture(&store, &directory, &thread, &original);
    let all_calls = calls.lock().unwrap().clone();
    let record = PermissionUse::decode_record(
        text(&reused.raw.0[8][0][9]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let current = &reused.view.execution.as_ref().unwrap().snapshot;
    let reuse_provenance = reused.raw.0[8].len() == 1
        && record.before_snapshot().values == reused_input
        && record.before_snapshot().tasks[0].input == reused_input
        && current.values == reused_input
        && current.tasks[0].input == reused_input
        && record.permission_id() == decision.permission_id()
        && record.execution_id() == &current.execution_id
        && record.execution_id() != &original
        && record.thread_id() == &thread
        && record.thread_generation() == &reused.view.thread.generation
        && record.task_id().as_str() == TASK
        && record.policy_revision() == "policy-1"
        && record.application().application() == APP
        && record.application().revision() == "application-1"
        && record.tool_id() == decision.scope().tool_id()
        && record.tool_revision() == decision.scope().tool_revision()
        && record.constraint_revision() == decision.scope().constraint_revision()
        && current.owner_generation == owner2;
    let reuse_valid = all_calls.len() == 2
        && all_calls.get(1).is_some_and(|call| {
            reused_kernel(&reopened, &reused, &record, call, &reused_input)
                && callback_matches_input(call, current, &reused.view.thread, &reused_input)
        });
    let immutable = reused.original == completed.original
        && reused.original_journal == completed.original_journal
        && reused.raw.0[4..8] == completed.raw.0[4..8]
        && reused.raw.0[3][..completed.raw.0[3].len()] == completed.raw.0[3];
    let physical_history = reused.raw.0[2].len() == 2 && reused.raw.0[3].len() == 12;
    let no_replacement = pending_after.0 == 200
        && serde_json::from_str::<Value>(&pending_after.1).unwrap()["interrupts"] == json!([]);
    let response_valid = reused_response.0 == 200
        && serde_json::from_str::<Value>(&reused_response.1).ok() == Some(result().output);
    drop(catalog);
    let released2 = catalog_weak.upgrade().is_none()
        && Arc::strong_count(&store) == 1
        && store.readiness().is_ok();
    drop(store);
    let store = SqliteStore::open(&directory).unwrap();
    let owner3 = store.owner_generation();
    let persisted2 = owner3 == owner2 + 1
        && same_after_reopen(
            &reused,
            &capture(&store, &directory, &thread, &original),
            owner3,
        )
        && store.readiness().is_ok();
    drop(store);
    drop(calls);
    std::fs::remove_dir_all(&directory).unwrap();
    assert!(!directory.exists());
    assert!(fresh && descriptor_valid && first_waited && pending_matches);
    assert_eq!(resume.0, 200, "Always HTTP resume: {}", resume.1);
    assert_eq!(resume.1.matches("event: done").count(), 1);
    assert!(
        first_drained && second_drained && released1 && released2,
        "first_drained={first_drained} second_drained={second_drained} released1={released1} released2={released2}"
    );
    assert!(original_shape, "original authority table shape");
    assert!(provenance, "original authority provenance");
    assert!(original_valid, "original kernel/raw/callback oracle");
    assert!(persisted1, "original evidence survives owner2 reopen");
    assert!(
        response_valid
            && no_replacement
            && reuse_provenance
            && reuse_valid
            && immutable
            && physical_history
            && persisted2
    );
    parked_geometry
}

#[path = "permission_approval_boundaries/mod.rs"]
mod boundaries;
