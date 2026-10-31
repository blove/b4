//! Actual HTTP producers, full durable refusal/rollback boundaries, and normal retirement.
use super::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct CurrentPolicy(AtomicBool);
impl ResourcePolicy for CurrentPolicy {
    fn authorize(&self, principal: &Principal, resource: &ResourceContext) -> bool {
        !self.0.load(Ordering::SeqCst) && Policy.authorize(principal, resource)
    }
}
fn approval() -> ApprovalHostConfig {
    ApprovalHostConfig::new(
        "policy-1".into(),
        BTreeMap::from([(
            RouteKey::new(ROUTE).unwrap(),
            BTreeMap::from([(
                TaskId::new(TASK).unwrap(),
                json!({"question":"Publish monthly report?"}),
            )]),
        )]),
    )
}
struct Fixture {
    directory: PathBuf,
    thread: ThreadId,
    store: Arc<SqliteStore>,
    catalog: Arc<PermissionCatalog>,
    calls: Calls,
    policy: Arc<CurrentPolicy>,
}
impl Fixture {
    fn new(terminal: Option<Arc<Mutex<Option<Capture>>>>) -> Self {
        let directory =
            std::env::temp_dir().join(format!("b4-http-always-boundary-{}", uuid::Uuid::new_v4()));
        let thread = ThreadId::new(uuid::Uuid::new_v4().to_string()).unwrap();
        let store = Arc::new(SqliteStore::open(&directory).unwrap());
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(origin()),
            })
            .unwrap();
        let calls: Calls = Arc::new(Mutex::new(Vec::new()));
        let publish = Publish {
            store: Arc::downgrade(&store),
            calls: calls.clone(),
        };
        let callback: Arc<dyn NativeEffect> = match terminal {
            None => Arc::new(publish),
            Some(evidence) => Arc::new(AbortTerminal {
                publish,
                directory: directory.clone(),
                evidence,
            }),
        };
        Self {
            directory,
            thread,
            store,
            catalog: catalog_with_callback(callback),
            calls,
            policy: Arc::new(CurrentPolicy::default()),
        }
    }
    async fn server(&self, original: bool) -> Server {
        let host = if original {
            HttpHost::new_with_permissions(
                self.store.clone(),
                Arc::new(Auth),
                self.policy.clone(),
                PermissionHostConfig::new(self.catalog.clone(), approval()),
            )
        } else {
            HttpHost::new_with_permission_uses(
                self.store.clone(),
                Arc::new(Auth),
                self.policy.clone(),
                PermissionUseHostConfig::new(self.catalog.clone(), approval()),
            )
        }
        .unwrap();
        Server::start(host).await
    }
    async fn park(&self, server: &Server) -> (Capture, Value) {
        let first = server
            .request(
                "POST",
                &format!("/threads/{}/runs/wait", self.thread),
                json!({"route":ROUTE,"input":{"month":"2026-09"}}),
            )
            .await;
        let view = self.store.thread_view(&self.thread).unwrap().unwrap();
        let execution = &view.execution.as_ref().unwrap().snapshot.execution_id;
        let parked = capture(&self.store, &self.directory, &self.thread, execution);
        let pending = server
            .request(
                "GET",
                &format!("/threads/{}/pending_interrupts", self.thread),
                Value::Null,
            )
            .await;
        let pending_json: Value = serde_json::from_str(&pending.1).unwrap();
        let interrupts = pending_json["interrupts"].as_array().unwrap();
        assert!(first.0 == 200 && pending.0 == 200 && interrupts.len() == 1);
        let interrupt = &interrupts[0];
        let grant: GrantRecord = serde_json::from_str(text(&parked.raw.0[4][0][7])).unwrap();
        assert!(
            parked.original.snapshot.revision == 2
                && parked.original.snapshot.status == ExecutionStatus::WaitingForApproval
        );
        assert!(parked.original_journal.iter().map(|j| j.revision).eq(0..=2));
        assert!(parked.raw.0[4].len() == 1 && parked.raw.0[5..].iter().all(Vec::is_empty));
        assert!(
            !grant.consumed
                && grant.execution_id == *execution
                && grant.token.as_str() == interrupt["grant"].as_str().unwrap()
        );
        assert!(self.calls.lock().unwrap().is_empty());
        (
            parked,
            json!({"route":ROUTE,"resume":[{"status":"resolved","interruptId":interrupt["interruptId"],"payload":"always","grant":interrupt["grant"]}]}),
        )
    }
    async fn resume(&self, server: &Server, body: Value) -> (u16, String) {
        server
            .request("POST", &format!("/threads/{}/resume", self.thread), body)
            .await
    }
    fn observe(&self, parked: &Capture) -> Capture {
        capture(
            &self.store,
            &self.directory,
            &self.thread,
            &parked.original.snapshot.execution_id,
        )
    }
    // Called only after normal listener and framework retirement, including faulted hosts.
    fn close_reopen(self, expected: Capture) -> bool {
        let owner = self.store.owner_generation();
        let weak_catalog = Arc::downgrade(&self.catalog);
        drop(self.catalog);
        let released = weak_catalog.upgrade().is_none() && Arc::strong_count(&self.store) == 1;
        let healthy = self.store.readiness().is_ok();
        drop(self.store);
        let reopened = SqliteStore::open(&self.directory).unwrap();
        let after = capture(
            &reopened,
            &self.directory,
            &self.thread,
            &expected.original.snapshot.execution_id,
        );
        let persisted = reopened.owner_generation() == owner + 1
            && same_after_reopen(&expected, &after, owner + 1)
            && reopened.readiness().is_ok();
        drop(reopened);
        drop(self.calls);
        std::fs::remove_dir_all(&self.directory).unwrap();
        released && healthy && persisted && !self.directory.exists()
    }
}
fn is_error(response: &(u16, String), status: u16, code: &str) -> bool {
    response.0 == status
        && serde_json::from_str::<Value>(&response.1)
            .is_ok_and(|v| v["error"]["details"]["code"] == code)
}
fn invalid_token(body: &Value) -> Value {
    let mut body = body.clone();
    // Corrupt one authenticated token byte while retaining its normal syntax and size.
    let token = body["resume"][0]["grant"].as_str().unwrap();
    let mut token = token.as_bytes().to_vec();
    let last = token.last_mut().unwrap();
    *last = if *last == b'a' { b'b' } else { b'a' };
    body["resume"][0]["grant"] = Value::String(String::from_utf8(token).unwrap());
    body
}

#[tokio::test]
async fn http_always_authentication_current_policy_and_token_refuse_without_any_write() {
    let fixture = Fixture::new(None);
    let server = fixture.server(true).await;
    let (parked, body) = fixture.park(&server).await;
    let path = format!("/threads/{}/resume", fixture.thread);
    let anonymous = server
        .request_as("POST", &path, body.clone(), "Bearer invalid")
        .await;
    let auth_unchanged = fixture.observe(&parked) == parked;
    fixture.policy.0.store(true, Ordering::SeqCst);
    let forbidden = fixture.resume(&server, body.clone()).await;
    let policy_unchanged = fixture.observe(&parked) == parked;
    fixture.policy.0.store(false, Ordering::SeqCst);
    let invalid = fixture.resume(&server, invalid_token(&body)).await;
    let token_unchanged = fixture.observe(&parked) == parked;
    let ready = server.request("GET", "/readyz", Value::Null).await;
    let no_calls = fixture.calls.lock().unwrap().is_empty();
    let drained = server.retire().await;
    let persisted = fixture.close_reopen(parked);
    assert!(is_error(&anonymous, 401, "authentication_required"));
    assert!(is_error(&forbidden, 404, "resource_unavailable"));
    assert!(is_error(&invalid, 403, "grant_invalid"));
    assert!(
        auth_unchanged
            && policy_unchanged
            && token_unchanged
            && no_calls
            && ready.0 == 200
            && drained
            && persisted
    );
}

#[tokio::test]
async fn http_legacy_permission_use_builder_still_refuses_actual_always_token() {
    let fixture = Fixture::new(None);
    let server = fixture.server(false).await;
    let (parked, body) = fixture.park(&server).await;
    let refused = fixture.resume(&server, body).await;
    let unchanged = fixture.observe(&parked) == parked && fixture.calls.lock().unwrap().is_empty();
    let drained = server.retire().await;
    let persisted = fixture.close_reopen(parked);
    assert!(is_error(&refused, 400, "always_unsupported") && unchanged && drained && persisted);
}

// Injection changes schema only; all authority and execution rows come from real producers.
fn install_abort(path: &Path, before: &Capture, revision: u64) {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .unwrap();
    let execution = before
        .original
        .snapshot
        .execution_id
        .as_str()
        .replace('\'', "''");
    let next_resource = before.view.thread.resource_revision + 1;
    let sql = format!("CREATE TRIGGER original_http_late_journal BEFORE INSERT ON journal
        WHEN NEW.kind='commit' AND NEW.execution_id='{execution}' AND NEW.revision=X'{revision:016x}'
        AND EXISTS (SELECT 1 FROM executions WHERE id=NEW.execution_id AND revision=NEW.revision)
        AND EXISTS (SELECT 1 FROM threads WHERE id=NEW.thread_id AND active_execution_id=NEW.execution_id AND resource_revision=X'{next_resource:016x}')
        AND EXISTS (SELECT 1 FROM approval_grants WHERE execution_id=NEW.execution_id AND consumed=1)
        AND EXISTS (SELECT 1 FROM permission_acceptances WHERE execution_id=NEW.execution_id AND committed_revision=X'0000000000000003')
        AND EXISTS (SELECT 1 FROM permission_decisions WHERE thread_id=NEW.thread_id)
        AND NOT EXISTS (SELECT 1 FROM permission_uses WHERE execution_id=NEW.execution_id)
        BEGIN SELECT RAISE(ABORT, 'original_http_exact_late_journal_abort'); END");
    connection.execute_batch(&sql).unwrap();
    let actual: String = connection.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='original_http_late_journal'", [], |r| r.get(0)).unwrap();
    assert!(actual == sql, "exact journal fault guard installed");
    connection.close().unwrap();
}
fn remove_abort(path: &Path) {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .unwrap();
    connection
        .execute_batch("DROP TRIGGER original_http_late_journal")
        .unwrap();
    let count: i64 = connection.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='original_http_late_journal'", [], |r| r.get(0)).unwrap();
    assert!(count == 0);
    connection.close().unwrap();
}

#[tokio::test]
async fn http_always_late_acceptance_journal_abort_preserves_waiting_and_real_token() {
    let fixture = Fixture::new(None);
    let server = fixture.server(true).await;
    let (parked, body) = fixture.park(&server).await;
    install_abort(&fixture.directory, &parked, 3);
    // These distinct refusal codes establish that ordinary request validation
    // still reaches its intended stage with the exact journal guard installed.
    let wrong = fixture.resume(&server, invalid_token(&body)).await;
    let wrong_unchanged = fixture.observe(&parked) == parked;
    let mut missing = body.clone();
    missing["resume"][0]
        .as_object_mut()
        .unwrap()
        .remove("grant");
    let missing = fixture.resume(&server, missing).await;
    let missing_unchanged = fixture.observe(&parked) == parked;
    let aborted = fixture.resume(&server, body.clone()).await;
    let rolled_back =
        fixture.observe(&parked) == parked && fixture.calls.lock().unwrap().is_empty();
    let unready = server.request("GET", "/readyz", Value::Null).await;
    let drained_fault = server.retire_with_fault(true).await;
    remove_abort(&fixture.directory);
    let removed_unchanged = fixture.observe(&parked) == parked;
    // Rebuild the faulted host under the same store owner. Successful acceptance
    // of the very same HTTP-issued token distinguishes the injected writer fault
    // from a grant, policy, schema-readiness, or request-shape refusal.
    let server = fixture.server(true).await;
    let resumed = fixture.resume(&server, body).await;
    let drained = server.retire().await;
    let completed = fixture.observe(&parked);
    let calls = fixture.calls.lock().unwrap().clone();
    let completed_valid = completed.original.snapshot.status == ExecutionStatus::Completed
        && completed.original.snapshot.revision == 5
        && calls.len() == 1
        && completed.raw.0[6].len() == 1
        && completed.raw.0[7].len() == 1
        && completed.raw.0[8].is_empty();
    let persisted = fixture.close_reopen(completed);
    assert!(is_error(&wrong, 403, "grant_invalid") && is_error(&missing, 400, "grant_required"));
    assert!(wrong_unchanged && missing_unchanged && rolled_back && removed_unchanged);
    assert!(is_error(&aborted, 503, "host_unready") && unready.0 == 503 && drained_fault);
    assert!(
        resumed.0 == 200
            && resumed.1.matches("event: done").count() == 1
            && completed_valid
            && drained
            && persisted
    );
}

struct AbortTerminal {
    publish: Publish,
    directory: PathBuf,
    evidence: Arc<Mutex<Option<Capture>>>,
}
impl NativeEffect for AbortTerminal {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        let outcome = self.publish.invoke(invocation, context)?;
        let store = self.publish.store.upgrade().unwrap();
        let dispatched = capture(
            &store,
            &self.directory,
            &context.thread_id,
            &invocation.execution_id,
        );
        assert!(
            dispatched.original.snapshot.revision == 4
                && dispatched.original.snapshot.status == ExecutionStatus::Running
        );
        install_abort(&self.directory, &dispatched, 5);
        *self.evidence.lock().unwrap() = Some(dispatched);
        // The real callback success crosses the outcome boundary; no fabricated
        // receipt, direct Complete/Fail commit, or fallback writer is used.
        Ok(outcome)
    }
}

#[tokio::test]
async fn http_always_callback_terminal_journal_abort_keeps_actual_dispatch_without_replay() {
    let evidence = Arc::new(Mutex::new(None));
    let fixture = Fixture::new(Some(evidence.clone()));
    let server = fixture.server(true).await;
    let (parked, body) = fixture.park(&server).await;
    let response = fixture.resume(&server, body.clone()).await;
    let unready = server.request("GET", "/readyz", Value::Null).await;
    let replay = fixture.resume(&server, body).await;
    let drained = server.retire_with_fault(true).await;
    let dispatched = evidence.lock().unwrap().take().unwrap();
    let after = fixture.observe(&parked);
    let calls = fixture.calls.lock().unwrap().clone();
    let rolled_back = dispatched == after;
    remove_abort(&fixture.directory);
    let removal_unchanged = fixture.observe(&parked) == dispatched;
    let journal = &dispatched.original_journal;
    let acceptance = PermissionAcceptance::decode_record(
        text(&dispatched.raw.0[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let ready = transition(
        &parked.original.snapshot,
        KernelCommand::Resume {
            route: RouteKey::new(ROUTE).unwrap(),
            decisions: vec![Decision {
                interrupt_id: parked.original.snapshot.tasks[0]
                    .interrupt
                    .as_ref()
                    .unwrap()
                    .id
                    .clone(),
                decision: ApprovalDecision::Always,
            }],
            turn_id: acceptance.turn_id().clone(),
        },
        acceptance.facts().clone(),
    )
    .unwrap()
    .snapshot;
    let actual = &dispatched.original.snapshot;
    let dispatch = transition(
        &ready,
        KernelCommand::Dispatch {
            task_id: TaskId::new(TASK).unwrap(),
            attempt_id: actual.attempt_history[0].clone(),
        },
        facts(journal[4].updated_at_ms),
    )
    .unwrap()
    .snapshot;
    let provenance = acceptance.before_snapshot() == &parked.original.snapshot
        && acceptance.committed_revision() == 3
        && actual == &dispatch
        && journal.len() == 5
        && journal.iter().map(|j| j.revision).eq(0..=4)
        && journal[3] == JournalEntry::from(&ready)
        && journal[4] == JournalEntry::from(&dispatch);
    let grant: GrantRecord = serde_json::from_str(text(&dispatched.raw.0[4][0][7])).unwrap();
    let shape = grant.consumed
        && dispatched.raw.0[5].is_empty()
        && dispatched.raw.0[6].len() == 1
        && dispatched.raw.0[7].len() == 1
        && dispatched.raw.0[8].is_empty();
    let callback_valid = calls.len() == 1
        && calls.first().is_some_and(|call| {
            call.dispatched == dispatched.original
                && callback_matches(call, actual, &dispatched.view.thread)
        });
    let persisted = fixture.close_reopen(dispatched);
    // Infrastructure faults close the SSE transport without a terminal frame.
    assert!(response.0 == 200 && !response.1.contains("event: done"));
    assert!(unready.0 == 503 && is_error(&replay, 503, "host_unready"));
    assert!(
        drained
            && rolled_back
            && removal_unchanged
            && provenance
            && shape
            && callback_valid
            && persisted
    );
}
