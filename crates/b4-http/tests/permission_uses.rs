//! Real HTTP reuse of authority produced by the public Always path.
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
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

const APP: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";
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
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: origin(),
        policy_revision: "policy-1".into(),
    }
}
fn result() -> TaskResult {
    TaskResult {
        output: json!({"published": true}),
        writes: vec![],
    }
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
struct SetupAuthority;
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
struct CatalogAllowConstraint;
impl ToolConstraint for CatalogAllowConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
type Calls = Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>;
struct Publish(Calls);
impl NativeEffect for Publish {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0
            .lock()
            .unwrap()
            .push((invocation.clone(), context.clone()));
        Ok(NativeOutcome::Succeeded(result()))
    }
}
fn fence(store: &SqliteStore, thread: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
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
async fn exchange(address: std::net::SocketAddr, path: &str, body: &str) -> (u16, String) {
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

#[tokio::test]
async fn http_permission_reuse_persists_success() {
    let directory =
        std::env::temp_dir().join(format!("b4-http-permission-use-{}", uuid::Uuid::new_v4()));
    let thread = ThreadId::new("f8e8bb7b-f8ac-4f29-a38f-3e0dfdb69b84").unwrap();
    let original_id = ExecutionId::new("653c8f87-d983-4262-ad6e-17a08938a45f").unwrap();
    let task = TaskId::new(TASK).unwrap();
    let effect = EffectId::new(EFFECT).unwrap();
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
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
                        callback: Arc::new(Publish(calls.clone())),
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
            application.clone(),
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
    let initial = new_execution(
        original_id.clone(),
        TurnId::new("446916ea-643b-4d47-8aee-cc0e995dc769").unwrap(),
        owner1,
        application.definition(&definition.route).unwrap().clone(),
        Map::from_iter([("input".into(), json!({"month": "2026-09"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &original_id,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let service = ApprovalService::new(store.clone(), Arc::new(SetupAuthority)).unwrap();
    let prompt = json!({"question": "Publish monthly report?"});
    service
        .park(
            &context(),
            &original_id,
            fence(&store, &thread),
            task.clone(),
            InterruptId::new("publish-approval").unwrap(),
            prompt.clone(),
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context(), &original_id, fence(&store, &thread))
        .unwrap();
    drop(service);
    let waiting = capture(&store, &directory, &thread, &original_id);
    let approved_turn = TurnId::new("a7ae0611-7e54-4ccb-ab14-2d095f33908b").unwrap();
    let ready = accept_permission_always_with_normal_inputs(
        store.clone(),
        Arc::new(SetupAuthority),
        catalog.clone(),
        &original_id,
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
    );
    drop(pending);
    let ready = match ready {
        Ok(ready) => ready,
        Err(error) => {
            let code = error.code();
            retire_failed_producer(
                store,
                catalog,
                application,
                calls,
                &directory,
                &thread,
                &original_id,
            );
            panic!("genuine Always setup failed: {code}");
        }
    };
    let ready_capture = capture(&store, &directory, &thread, &original_id);
    let target = RunCancellationTarget::from_view(&ready_capture.view).unwrap();
    let expected_cancel = transition(
        &ready.snapshot,
        KernelCommand::Cancel {
            turn_id: approved_turn,
            owner_generation: owner1,
        },
        facts(104),
    )
    .unwrap()
    .snapshot;
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
    );
    let cancelled = match cancelled {
        Ok(cancelled) => cancelled,
        Err(error) => {
            let code = error.code();
            retire_failed_producer(
                store,
                catalog,
                application,
                calls,
                &directory,
                &thread,
                &original_id,
            );
            panic!("genuine Ready cancellation setup failed: {code}");
        }
    };
    let provisioned = capture(&store, &directory, &thread, &original_id);
    let setup_valid = produced_permission_matches(&provisioned, &definition)
        && waiting.original.snapshot.revision == 2
        && waiting.original.snapshot.status == ExecutionStatus::WaitingForApproval
        && ready.snapshot.revision == 3
        && ready.snapshot.tasks[0].approved
        && ready.snapshot.tasks[0].status == TaskStatus::Ready
        && cancelled.snapshot == expected_cancel
        && cancelled.snapshot.revision == 4
        && cancelled.snapshot.status == ExecutionStatus::Cancelled
        && cancelled.snapshot.attempts_used == 0
        && calls.lock().unwrap().is_empty()
        && provisioned.raw.0[4..8] == ready_capture.raw.0[4..8]
        && provisioned.raw.0[8].is_empty();
    let owner1_sole = Arc::strong_count(&store) == 1 && store.readiness().is_ok();
    drop(store);
    let store = Arc::new(SqliteStore::open(&directory).unwrap());
    let owner2 = store.owner_generation();
    let reopened = capture(&store, &directory, &thread, &original_id);
    let provisioning_persisted =
        owner2 == owner1 + 1 && same_after_reopen(&provisioned, &reopened, owner2);
    let expected_context = InvocationContext {
        thread_id: thread.clone(),
        thread_generation: reopened.view.thread.generation.clone(),
        origin: origin(),
        application_revision: "application-1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: "policy-1".into(),
    };
    let config = PermissionUseHostConfig::new(
        catalog.clone(),
        ApprovalHostConfig::new(
            "policy-1".into(),
            BTreeMap::from([(
                definition.route.clone(),
                BTreeMap::from([(task.clone(), prompt)]),
            )]),
        ),
    );
    let host =
        HttpHost::new_with_permission_uses(store.clone(), Arc::new(Auth), Arc::new(Policy), config)
            .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = host.router();
    let (stop, stopped) = oneshot::channel();
    let listener_task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let response = exchange(
        address,
        &format!("/threads/{thread}/runs/wait"),
        &json!({"route": ROUTE, "input": {"month": "2026-09"}}).to_string(),
    )
    .await;
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    let drained = shutdown.wait_quiescent(WAIT).await.unwrap();
    let _ = stop.send(());
    tokio::time::timeout(WAIT, listener_task)
        .await
        .unwrap()
        .unwrap();
    let after = capture(&store, &directory, &thread, &original_id);
    let observed_calls = calls.lock().unwrap().clone();
    // Every behavioral observation is captured before ownership retirement and assertions.
    let current = after.view.execution.as_ref().unwrap();
    let callback_matches = observed_calls.len() == 1
        && observed_calls
            .first()
            .zip(current.snapshot.attempt_history.first())
            .is_some_and(|((invocation, actual_context), attempt)| {
                actual_context == &expected_context
                    && invocation
                        == &EffectInvocation {
                            execution_id: current.snapshot.execution_id.clone(),
                            turn_id: current.snapshot.turn_id.clone(),
                            owner_generation: owner2,
                            task_id: task.clone(),
                            effect_id: EffectId::new(EFFECT).unwrap(),
                            attempt_id: attempt.clone(),
                            input: Map::from_iter([("input".into(), json!({"month": "2026-09"}))]),
                        }
            });
    let immutable_original = after.original == provisioned.original
        && after.original_journal == provisioned.original_journal
        && after.raw.0[4..8] == provisioned.raw.0[4..8]
        && after.raw.0[3][..provisioned.raw.0[3].len()] == provisioned.raw.0[3];
    let real_use_complete = reuse_matches(
        &reopened,
        &after,
        &definition,
        &expected_context,
        &task,
        owner2,
    );
    let response_success = response.0 == 200
        && serde_json::from_str::<Value>(&response.1).ok() == Some(result().output);
    drop(host);
    drop(shutdown);
    let catalog_weak = Arc::downgrade(&catalog);
    let application_weak = Arc::downgrade(&application);
    drop(catalog);
    drop(application);
    let aliases_retired = catalog_weak.upgrade().is_none()
        && application_weak.upgrade().is_none()
        && Arc::strong_count(&calls) == 1
        && Arc::strong_count(&store) == 1;
    let healthy = store.readiness().is_ok();
    drop(store);
    let store = SqliteStore::open(&directory).unwrap();
    let final_capture = capture(&store, &directory, &thread, &original_id);
    let persisted = store.owner_generation() == owner2 + 1
        && same_after_reopen(&after, &final_capture, owner2 + 1)
        && store.readiness().is_ok();
    drop(store);
    std::fs::remove_dir_all(&directory).unwrap();
    let retired = owner1_sole
        && aliases_retired
        && healthy
        && persisted
        && !directory.exists()
        && drained == ShutdownOutcome::Quiescent { faulted: false };
    eprintln!("http-permission-reuse: setup={setup_valid} provisioning_persisted={provisioning_persisted} status={} callbacks={} callback_matches={callback_matches} original_unchanged={immutable_original} use_complete={real_use_complete} retired={retired}", response.0, observed_calls.len());
    assert!(
        setup_valid && provisioning_persisted && retired,
        "genuine producer and actual-state close/reopen/removal must be healthy"
    );
    assert!(response_success && callback_matches && immutable_original && real_use_complete, "HTTP must reuse the persisted permission, invoke once, and persist exact Complete4; response={response:?}");
}

// Independently replay Start, authorization, Dispatch and Complete using only
// the durable use witness and observed framework-generated IDs/timestamps.
fn reuse_matches(
    before: &Capture,
    after: &Capture,
    definition: &Definition,
    context: &InvocationContext,
    task: &TaskId,
    owner: u64,
) -> bool {
    let check = || -> Option<bool> {
        let current = &after.view.execution.as_ref()?.snapshot;
        if after.raw.0[8].len() != 1
            || after.current_journal.len() != 5
            || current.attempt_history.len() != 1
        {
            return Some(false);
        }
        let record = PermissionUse::decode_record(
            text(&after.raw.0[8][0][9]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .ok()?;
        let decision = PermissionDecision::decode_record(
            text(&after.raw.0[7].first()?[5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .ok()?;
        let journal = &after.current_journal;
        let initial = new_execution(
            current.execution_id.clone(),
            current.turn_id.clone(),
            owner,
            definition.clone(),
            Map::from_iter([("input".into(), json!({"month": "2026-09"}))]),
            facts(journal[0].updated_at_ms),
        )
        .ok()?;
        let start = transition(
            &initial,
            KernelCommand::Start,
            facts(journal[1].updated_at_ms),
        )
        .ok()?
        .snapshot;
        let command =
            PermissionAuthorizationCommand::capture(&start, task.clone(), record.use_id().clone())
                .ok()?;
        let authorized = authorize_ready(&start, &command, facts(journal[2].updated_at_ms))
            .ok()?
            .snapshot;
        let dispatched = transition(
            &authorized,
            KernelCommand::Dispatch {
                task_id: task.clone(),
                attempt_id: current.attempt_history[0].clone(),
            },
            facts(journal[3].updated_at_ms),
        )
        .ok()?
        .snapshot;
        let complete = transition(
            &dispatched,
            KernelCommand::Complete {
                task_id: task.clone(),
                attempt_id: current.attempt_history[0].clone(),
                result: result(),
            },
            facts(journal[4].updated_at_ms),
        )
        .ok()?
        .snapshot;
        let snapshots = [&initial, &start, &authorized, &dispatched, &complete];
        let expected_journal = snapshots.map(JournalEntry::from).to_vec();
        let typed_match = record.before_snapshot() == &start
            && record.facts() == &facts(journal[2].updated_at_ms)
            && record.committed_revision() == 2
            && record.execution_id() == &current.execution_id
            && record.task_id() == task
            && record.thread_id() == &context.thread_id
            && record.thread_generation() == &context.thread_generation
            && record.permission_id() == decision.permission_id()
            && record.application().application() == APP
            && record.application().revision() == "application-1"
            && record.policy_revision() == context.policy_revision
            && record.tool_id().as_str() == "release-tool"
            && record.tool_revision().as_str() == "tool-1"
            && record.constraint_revision().as_str() == "constraint-1"
            && complete == *current
            && complete.revision == 4
            && complete.status == ExecutionStatus::Completed
            && complete.tasks[0].status == TaskStatus::Succeeded
            && complete.attempts_used == 1
            && expected_journal == *journal
            && record.record_version() == 1
            && record.permission_version() == 1
            && authorized.tasks[0].interrupt.as_ref() == Some(record.marker())
            && current.owner_generation == owner
            && current.execution_id != before.original.snapshot.execution_id;
        if !typed_match {
            return Some(false);
        }

        // Build every expected cell from the pre-HTTP state, independently
        // projected snapshots and literal SQL column semantics. The canonical
        // Use body is accepted only after the full typed relationships above.
        let mut expected = before.raw.0.clone();
        expected[1][0][1] = Cell::Text(complete.execution_id.to_string());
        expected[1][0][5] = Cell::Blob(
            (before.view.thread.resource_revision + 5)
                .to_be_bytes()
                .to_vec(),
        );
        expected[2].push(vec![
            Cell::Text(complete.execution_id.to_string()),
            Cell::Text(context.thread_id.to_string()),
            Cell::Blob(4_u64.to_be_bytes().to_vec()),
            Cell::Blob(owner.to_be_bytes().to_vec()),
            Cell::Text(serde_json::to_string(&complete).ok()?),
        ]);
        expected[2].sort_by(|left, right| text(&left[0]).cmp(text(&right[0])));
        let mut sequence = match expected[3].last()?.first()? {
            Cell::Integer(value) => *value,
            _ => return None,
        };
        for (index, entry) in expected_journal.iter().enumerate() {
            sequence += 1;
            expected[3].push(vec![
                Cell::Integer(sequence),
                Cell::Text(if index == 0 { "admit" } else { "commit" }.into()),
                Cell::Text(context.thread_id.to_string()),
                Cell::Text(complete.execution_id.to_string()),
                Cell::Blob(entry.revision.to_be_bytes().to_vec()),
                Cell::Blob(owner.to_be_bytes().to_vec()),
                Cell::Text(serde_json::to_string(&entry.status).ok()?),
                Cell::Blob(entry.updated_at_ms.to_be_bytes().to_vec()),
                Cell::Text(serde_json::to_string(entry).ok()?),
            ]);
        }
        let canonical_use = String::from_utf8(
            record
                .encode_record(&mut PermissionEvidenceBudget::new())
                .ok()?,
        )
        .ok()?;
        expected[8].push(vec![
            Cell::Text(complete.execution_id.to_string()),
            Cell::Text(TASK.into()),
            Cell::Text(record.use_id().as_str().into()),
            Cell::Text(context.thread_id.to_string()),
            Cell::Text(context.thread_generation.to_string()),
            Cell::Text(decision.permission_id().as_str().into()),
            Cell::Text(complete.turn_id.to_string()),
            Cell::Blob(2_u64.to_be_bytes().to_vec()),
            Cell::Integer(1),
            Cell::Text(canonical_use),
        ]);
        let mut expected_thread = before.view.thread.clone();
        expected_thread.active_execution_id = Some(complete.execution_id.clone());
        expected_thread.resource_revision += 5;
        Some(
            after.raw.0 == expected
                && after.view
                    == ThreadView {
                        thread: expected_thread,
                        execution: Some(StoredExecution {
                            thread_id: context.thread_id.clone(),
                            snapshot: complete,
                        }),
                    },
        )
    };
    check() == Some(true)
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

// Producer errors retire their actual state before identifying a setup failure.
fn retire_failed_producer(
    store: Arc<SqliteStore>,
    catalog: Arc<PermissionCatalog>,
    application: Arc<NativeApplication>,
    calls: Calls,
    path: &Path,
    thread: &ThreadId,
    original: &ExecutionId,
) {
    let actual = capture(&store, path, thread, original);
    let owner = store.owner_generation();
    let catalog_weak = Arc::downgrade(&catalog);
    let application_weak = Arc::downgrade(&application);
    drop(catalog);
    drop(application);
    let sole = Arc::strong_count(&store) == 1
        && Arc::strong_count(&calls) == 1
        && catalog_weak.upgrade().is_none()
        && application_weak.upgrade().is_none();
    drop(store);
    let reopened = SqliteStore::open(path).unwrap();
    let persisted = same_after_reopen(
        &actual,
        &capture(&reopened, path, thread, original),
        owner + 1,
    ) && reopened.owner_generation() == owner + 1
        && reopened.readiness().is_ok();
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
    assert!(
        sole && persisted && !path.exists(),
        "failed producer must retire its actual persisted state"
    );
}
