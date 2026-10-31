use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::approval::*;
use b4_runtime::permissions::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::collections::BTreeMap;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-permission-first-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct AllowAuthority;
impl ApprovalAuthority for AllowAuthority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
struct AllowConstraint;
impl ToolConstraint for AllowConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
struct CountCalls(Arc<AtomicUsize>);
impl NativeEffect for CountCalls {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"released": true}),
            writes: vec![],
        }))
    }
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
fn fence(store: &dyn ApprovalStore, thread: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}

#[test]
fn human_always_continuation_is_persisted_after_normal_close_and_next_writer() {
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let approved_turn = TurnId::new("approved-turn").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                effect_id: effect.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    assert_eq!(registered, definition);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    let running = store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    assert_eq!(running.snapshot.status, ExecutionStatus::Running);
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    let parked = service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt.clone(),
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    assert_eq!(parked.snapshot.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(pending.len(), 1);
    assert_eq!(before.grants.len(), 1);
    assert!(before.acceptances.is_empty());
    let grant = &before.grants[0];
    assert_eq!(grant.thread_id, thread);
    assert_eq!(grant.thread_generation, before.thread.generation);
    assert_eq!(grant.origin, context.actor);
    assert_eq!(grant.policy_revision, context.policy_revision);
    assert_eq!(grant.execution_id, execution);
    assert_eq!(grant.route, definition.route);
    assert_eq!(grant.definition_revision, definition.revision);
    assert_eq!(grant.task_id, task);
    assert_eq!(grant.effect_id, effect);
    assert_eq!(grant.interrupt_id, interrupt);
    assert_eq!(grant.task_parking_revision, parked.snapshot.revision);
    assert_eq!(grant.prompt, prompt);
    assert_eq!(grant.token, pending[0].token);
    assert!(
        grant.verifier == token_verifier(&pending[0].token),
        "real grant verifier must match pending token"
    );
    assert_eq!(
        grant.proposal_digest,
        proposal_digest(&parked.snapshot.tasks[0], &prompt).unwrap()
    );
    assert_eq!(grant.issued_at_ms, 102);
    assert_eq!(grant.expires_at_ms, 1_102);
    assert!(grant.issued_at_ms <= 103 && 103 < grant.expires_at_ms);
    assert!(!grant.consumed);
    assert_eq!(pending[0].task_id, task);
    assert_eq!(pending[0].interrupt_id, interrupt);
    assert!(
        pending[0].resume_key == grant.resume_key,
        "real pending resume key must match grant"
    );
    assert_eq!(pending[0].prompt, prompt);
    assert_eq!(pending[0].expires_at_ms, grant.expires_at_ms);
    let result = accept_permission_human(
        store.clone(),
        authority.clone(),
        catalog.clone(),
        &execution,
        fence(store.as_ref(), &thread),
        ApprovalAcceptanceRequest {
            context,
            route: definition.route,
            definition_revision: definition.revision,
            turn_id: approved_turn.clone(),
            now_ms: 103,
            decisions: vec![GrantDecision {
                interrupt_id: interrupt,
                decision: ApprovalDecision::Always,
                cancelled: false,
                token: Some(pending[0].token.clone()),
            }],
        },
    );
    drop(service);
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "all old store aliases must be retired"
    );
    drop(store);
    drop(catalog);
    drop(application);
    drop(authority);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let persisted = next.execution(&execution).unwrap().unwrap();
    drop(next);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let original_refusal = matches!(
        &result,
        Err(PermissionError::Approval(ApprovalError::UnsupportedAlways))
    );
    assert!(
        result.is_ok(),
        "persistent Always acceptance must succeed; original UnsupportedAlways={original_refusal}"
    );
    let accepted = result.unwrap();
    assert_eq!(accepted.snapshot.status, ExecutionStatus::Running);
    assert_eq!(accepted.snapshot.turn_id, approved_turn);
    assert_eq!(accepted.snapshot.owner_generation, original_owner);
    assert_eq!(accepted.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(accepted.snapshot.tasks[0].approved);
    assert!(accepted.snapshot.tasks[0].attempt_id.is_none());
    assert_eq!(accepted.snapshot.attempts_used, 0);
    assert!(accepted.snapshot.attempt_history.is_empty());
    assert_eq!(persisted, accepted);
}

// These fixed observations come from real produced rows, never authority seeds.
#[derive(PartialEq)]
struct ProducedRawNine {
    tables: [Vec<Vec<rusqlite::types::Value>>; 9],
}
fn produced_raw_cells(
    connection: &rusqlite::Connection,
    sql: &str,
    columns: usize,
    maximum: usize,
) -> Vec<Vec<rusqlite::types::Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let mut cursor = statement.query([]).unwrap();
    let mut rows = Vec::new();
    while let Some(row) = cursor.next().unwrap() {
        assert!(
            rows.len() < maximum,
            "fixed produced fixture has bounded rows"
        );
        let mut cells = Vec::new();
        for column in 0..columns {
            match row.get_ref(column).unwrap() {
                rusqlite::types::ValueRef::Text(bytes) => assert!(
                    bytes.len() <= 16_384 && std::str::from_utf8(bytes).is_ok(),
                    "fixed produced text is bounded UTF-8 before ownership"
                ),
                rusqlite::types::ValueRef::Blob(bytes) => {
                    assert!(bytes.len() == 8, "fixed produced scalar is BLOB8")
                }
                rusqlite::types::ValueRef::Null | rusqlite::types::ValueRef::Integer(_) => {}
                rusqlite::types::ValueRef::Real(_) => {
                    panic!("fixed produced fixture has no REAL cell")
                }
            }
            cells.push(row.get::<_, rusqlite::types::Value>(column).unwrap());
        }
        rows.push(cells);
    }
    rows
}
fn produced_raw_nine(path: &std::path::Path) -> ProducedRawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let raw = ProducedRawNine { tables: [
        produced_raw_cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        produced_raw_cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        produced_raw_cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 2", 5, 1),
        produced_raw_cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 6", 9, 5),
        produced_raw_cells(&connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        produced_raw_cells(&connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] };
    assert!(
        connection.close().is_ok(),
        "read-only produced observer closes normally"
    );
    raw
}
fn produced_text(value: &rusqlite::types::Value) -> &str {
    match value {
        rusqlite::types::Value::Text(text) => text,
        _ => panic!("produced row retains TEXT"),
    }
}
fn produced_u64(value: &rusqlite::types::Value) -> u64 {
    match value {
        rusqlite::types::Value::Blob(bytes) => {
            u64::from_be_bytes(bytes.as_slice().try_into().unwrap())
        }
        _ => panic!("produced row retains BLOB8"),
    }
}
fn produced_usage(budget: &PermissionEvidenceBudget) -> (usize, usize) {
    let usage = budget.usage();
    (usage.encoded_bytes(), usage.structural_nodes())
}
struct ProducedViewCapture {
    producer: Result<StoredExecution, PermissionError>,
    reader: Result<PermissionView, PermissionError>,
    resource_control: Result<ApprovalResource, PermissionError>,
    gate_control: Result<(), PermissionError>,
    before: ApprovalView,
    pending: Vec<SensitivePendingApproval>,
    request: ApprovalAcceptanceRequest,
    definition: Definition,
    read_before: ThreadView,
    read_after: ThreadView,
    execution_before: StoredExecution,
    execution_after: StoredExecution,
    journal_before: Vec<JournalEntry>,
    journal_after: Vec<JournalEntry>,
    raw_before: ProducedRawNine,
    raw_after: ProducedRawNine,
    reader_usage: ((usize, usize), (usize, usize)),
    resource_usage: ((usize, usize), (usize, usize)),
    original_owner: u64,
    read_owner: u64,
    next_owner: u64,
    read_fence: ApprovalFence,
    passive_reopen: bool,
    registered_matches: bool,
}
fn capture_produced_view_after_normal_retirement(passive_reopen: bool) -> ProducedViewCapture {
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let registered_matches = registered == definition;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: ApprovalDecision::Always,
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };
    let producer = accept_permission_human(
        store.clone(),
        authority.clone(),
        catalog.clone(),
        &execution,
        fence(store.as_ref(), &thread),
        request.clone(),
    );
    let original_after = store.thread_view(&thread).unwrap().unwrap();
    let original_journal_after = store.journal(&execution).unwrap();
    let original_raw_after = produced_raw_nine(&directory.0);
    drop(service);
    let store = if passive_reopen {
        store.readiness().unwrap();
        assert!(
            Arc::strong_count(&store) == 1,
            "all original producer store aliases retire"
        );
        drop(store);
        let next = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let reopened = next.thread_view(&thread).unwrap().unwrap();
        let reopened_journal = next.journal(&execution).unwrap();
        let reopened_raw = produced_raw_nine(&directory.0);
        assert!(
            reopened == original_after && reopened_journal == original_journal_after,
            "real next writer retains ACTUAL producer AFTER"
        );
        assert!(
            reopened_raw.tables[1..] == original_raw_after.tables[1..],
            "real next writer preserves ACTUAL produced rows"
        );
        assert!(
            next.owner_generation() == original_owner.checked_add(1).unwrap(),
            "real next writer advances owner"
        );
        next
    } else {
        store
    };
    let read_owner = store.owner_generation();
    let read_before = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = produced_raw_nine(&directory.0);
    // Passive reading uses the actual recorded execution owner, not the live store owner.
    let read_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: read_before.thread.resource_revision,
    };
    let invocation = InvocationContext {
        thread_id: read_before.thread.id.clone(),
        thread_generation: read_before.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: application.identity().revision.clone(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let gate_control = catalog.validate_call(
        &invocation,
        &definition,
        &execution_before.snapshot.tasks[0],
        PermissionCallPhase::Unclaimed,
    );
    let (resource_control, resource_usage) = {
        let mut budget = PermissionEvidenceBudget::new();
        let initial_usage = produced_usage(&budget);
        let result = store.permission_resource(&execution, &mut budget);
        (result, (initial_usage, produced_usage(&budget)))
    };
    let (reader, reader_usage) = {
        let mut budget = PermissionEvidenceBudget::new();
        let initial_usage = produced_usage(&budget);
        let result = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            read_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut budget,
        );
        (result, (initial_usage, produced_usage(&budget)))
    };
    let read_after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = produced_raw_nine(&directory.0);
    store.readiness().unwrap();
    assert!(
        Arc::strong_count(&store) == 1,
        "all reader store aliases retire normally"
    );
    drop(store);
    let next_owner = if passive_reopen {
        read_owner
    } else {
        let next = SqliteStore::open(&directory.0).unwrap();
        let owner = next.owner_generation();
        let next_view = next.thread_view(&thread).unwrap().unwrap();
        let next_execution = next.execution(&execution).unwrap().unwrap();
        let next_journal = next.journal(&execution).unwrap();
        let next_raw = produced_raw_nine(&directory.0);
        next.readiness().unwrap();
        drop(next);
        assert!(
            next_view == read_after
                && next_execution == execution_after
                && next_journal == journal_after,
            "real next writer retains ACTUAL reader AFTER"
        );
        assert!(
            next_raw.tables[1..] == raw_after.tables[1..],
            "real next writer preserves ACTUAL reader rows"
        );
        assert!(
            owner == original_owner.checked_add(1).unwrap(),
            "real next writer advances owner"
        );
        owner
    };
    drop(catalog);
    drop(application);
    drop(authority);
    assert!(
        calls.load(Ordering::SeqCst) == 0,
        "producer and passive reads never dispatch native effects"
    );
    drop(calls);
    drop(original_after);
    drop(original_journal_after);
    drop(original_raw_after);
    drop(directory);
    ProducedViewCapture {
        producer,
        reader,
        resource_control,
        gate_control,
        before,
        pending,
        request,
        definition,
        read_before,
        read_after,
        execution_before,
        execution_after,
        journal_before,
        journal_after,
        raw_before,
        raw_after,
        reader_usage,
        resource_usage,
        original_owner,
        read_owner,
        next_owner,
        read_fence,
        passive_reopen,
        registered_matches,
    }
}
fn assert_produced_reader_setup(
    capture: &ProducedViewCapture,
) -> (
    PermissionAcceptance,
    PermissionDecision,
    GrantRecord,
    Vec<JournalEntry>,
) {
    assert!(
        capture.registered_matches,
        "same real native Definition is registered"
    );
    let producer_code = capture
        .producer
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.producer.is_ok(),
        "qualified genuine Always producer must succeed; code={producer_code}"
    );
    let accepted = capture.producer.as_ref().unwrap();
    assert_eq!(
        capture.read_before.execution.as_ref(),
        Some(&capture.execution_before)
    );
    assert_eq!(accepted, &capture.execution_before);
    b4_kernel::validate_snapshot(&capture.before.execution.snapshot).unwrap();
    b4_kernel::validate_snapshot(&accepted.snapshot).unwrap();
    assert_eq!(
        capture.before.execution.snapshot.definition,
        capture.definition
    );
    assert_eq!(
        capture.before.execution.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(capture.before.execution.snapshot.revision, 2);
    assert_eq!(capture.before.thread.resource_revision, 3);
    assert_eq!(capture.before.grants.len(), 1);
    assert!(capture.before.acceptances.is_empty());
    assert_eq!(capture.pending.len(), 1);
    let prior_grant = &capture.before.grants[0];
    let pending = &capture.pending[0];
    assert_eq!(prior_grant.thread_id, capture.before.thread.id);
    assert_eq!(
        prior_grant.thread_generation,
        capture.before.thread.generation
    );
    assert_eq!(
        prior_grant.execution_id,
        capture.before.execution.snapshot.execution_id
    );
    assert_eq!(prior_grant.route, capture.definition.route);
    assert_eq!(prior_grant.definition_revision, capture.definition.revision);
    assert_eq!(prior_grant.task_id, capture.definition.batches[0][0].id);
    assert_eq!(
        prior_grant.effect_id,
        capture.definition.batches[0][0].effect_id
    );
    assert_eq!(prior_grant.task_parking_revision, 2);
    assert_eq!(
        capture.request.decisions[0].interrupt_id,
        prior_grant.interrupt_id
    );
    assert_eq!(prior_grant.token, pending.token);
    assert_eq!(prior_grant.verifier, token_verifier(&pending.token));
    assert_eq!(
        prior_grant.proposal_digest,
        proposal_digest(
            &capture.before.execution.snapshot.tasks[0],
            &prior_grant.prompt
        )
        .unwrap()
    );
    assert_eq!(
        prior_grant.prompt,
        json!({"question": "Release fixture?", "arguments": {"nullable": null}})
    );
    assert_eq!(pending.task_id, capture.definition.batches[0][0].id);
    assert_eq!(pending.interrupt_id, prior_grant.interrupt_id);
    assert_eq!(pending.resume_key, prior_grant.resume_key);
    assert_eq!(pending.prompt, prior_grant.prompt);
    assert_eq!(pending.expires_at_ms, prior_grant.expires_at_ms);
    assert!(
        prior_grant.issued_at_ms == 102
            && prior_grant.expires_at_ms == 1_102
            && !prior_grant.consumed
    );
    assert_eq!(capture.request.context.actor, prior_grant.origin);
    assert_eq!(
        capture.request.context.policy_revision,
        prior_grant.policy_revision
    );
    assert_eq!(capture.request.route, capture.definition.route);
    assert_eq!(
        capture.request.definition_revision,
        capture.definition.revision
    );
    assert_eq!(capture.request.now_ms, 103);
    assert_eq!(capture.request.decisions.len(), 1);
    assert_eq!(
        capture.request.decisions[0].token.as_ref(),
        Some(&pending.token)
    );
    assert_eq!(
        capture.request.decisions[0].decision,
        ApprovalDecision::Always
    );
    assert!(!capture.request.decisions[0].cancelled);
    assert_eq!(accepted.snapshot.status, ExecutionStatus::Running);
    assert_eq!(accepted.snapshot.turn_id, capture.request.turn_id);
    assert_eq!(accepted.snapshot.revision, 3);
    assert_eq!(accepted.snapshot.updated_at_ms, 103);
    assert_eq!(accepted.snapshot.owner_generation, capture.original_owner);
    assert_eq!(accepted.snapshot.tasks.len(), 1);
    assert_eq!(
        accepted.snapshot.tasks[0].spec,
        capture.definition.batches[0][0]
    );
    assert_eq!(accepted.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(accepted.snapshot.tasks[0].approved);
    assert_eq!(
        accepted.snapshot.tasks[0].interrupt,
        capture.before.execution.snapshot.tasks[0].interrupt
    );
    assert!(
        accepted.snapshot.tasks[0].attempt_id.is_none()
            && accepted.snapshot.tasks[0].result.is_none()
            && accepted.snapshot.tasks[0].failure.is_none()
    );
    assert!(accepted.snapshot.attempts_used == 0 && accepted.snapshot.attempt_history.is_empty());
    assert_eq!(capture.read_before.thread.resource_revision, 4);
    assert_eq!(capture.read_fence.execution, (&accepted.snapshot).into());
    assert_eq!(
        capture.read_fence.resource_revision,
        capture.read_before.thread.resource_revision
    );
    assert_eq!(
        capture.next_owner,
        capture.original_owner.checked_add(1).unwrap()
    );
    assert_eq!(
        capture.read_owner,
        if capture.passive_reopen {
            capture.next_owner
        } else {
            capture.original_owner
        }
    );
    let resource_code = capture
        .resource_control
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.resource_control.is_ok(),
        "same recorded-owner resource control succeeds; code={resource_code}"
    );
    let resource = capture.resource_control.as_ref().unwrap();
    assert_eq!(resource.thread, capture.read_before.thread);
    assert_eq!(resource.execution, capture.execution_before);
    assert!(
        capture.gate_control.is_ok(),
        "same native catalog and genuine Ready call pass pure allowing gate"
    );
    let pair = capture.resource_usage;
    assert!(
        pair.1 .0 > pair.0 .0
            && pair.1 .1 > pair.0 .1
            && pair.1 .0 <= 8_388_608
            && pair.1 .1 <= 65_536,
        "actual control account stays finite and monotone"
    );
    let raw = &capture.raw_before.tables;
    assert_eq!(raw[4].len(), 1);
    assert!(raw[5].is_empty() && raw[8].is_empty());
    assert_eq!(raw[6].len(), 1);
    assert_eq!(raw[7].len(), 1);
    let grant: GrantRecord = serde_json::from_str(produced_text(&raw[4][0][7])).unwrap();
    let mut expected_consumed = prior_grant.clone();
    expected_consumed.consumed = true;
    assert_eq!(grant, expected_consumed);
    assert_eq!(raw[4][0][6], rusqlite::types::Value::Integer(1));
    let acceptance = {
        let mut budget = PermissionEvidenceBudget::new();
        PermissionAcceptance::decode_record(produced_text(&raw[6][0][7]).as_bytes(), &mut budget)
            .unwrap()
    };
    let decision = {
        let mut budget = PermissionEvidenceBudget::new();
        PermissionDecision::decode_record(produced_text(&raw[7][0][5]).as_bytes(), &mut budget)
            .unwrap()
    };
    assert_eq!(
        acceptance.before_snapshot(),
        &capture.before.execution.snapshot
    );
    assert_eq!(acceptance.thread_id(), &capture.before.thread.id);
    assert_eq!(
        acceptance.thread_generation(),
        &capture.before.thread.generation
    );
    assert_eq!(acceptance.origin(), &capture.request.context.actor);
    assert_eq!(acceptance.turn_id(), &capture.request.turn_id);
    assert_eq!(acceptance.facts(), &facts(103));
    assert_eq!(acceptance.committed_revision(), 3);
    assert_eq!(acceptance.decisions().len(), 1);
    let accepted_decision = &acceptance.decisions()[0];
    assert_eq!(accepted_decision.decision(), ApprovalDecision::Always);
    assert_eq!(accepted_decision.task_id(), &pending.task_id);
    assert_eq!(accepted_decision.interrupt_id(), &pending.interrupt_id);
    assert_eq!(
        accepted_decision.grant_ref().execution_id(),
        &grant.execution_id
    );
    assert_eq!(
        accepted_decision.grant_ref().interrupt_id(),
        &grant.interrupt_id
    );
    assert_eq!(accepted_decision.proposal_digest(), grant.proposal_digest);
    assert_eq!(
        accepted_decision.task_parking_revision(),
        grant.task_parking_revision
    );
    let binding = accepted_decision.binding().unwrap();
    assert_eq!(binding.effect_id(), &grant.effect_id);
    assert_eq!(binding.tool_id().as_str(), "release-tool");
    assert_eq!(binding.tool_revision().as_str(), "tool-1");
    assert_eq!(binding.constraint_revision().as_str(), "constraint-1");
    assert_eq!(
        accepted_decision.permission_id(),
        Some(decision.permission_id())
    );
    assert_eq!(decision.creator_execution_id(), &grant.execution_id);
    assert_eq!(decision.creator_turn_id(), &capture.request.turn_id);
    assert_eq!(decision.creator_task_id(), &grant.task_id);
    assert_eq!(decision.creator_interrupt_id(), &grant.interrupt_id);
    assert_eq!(acceptance.application().application(), "permission-app");
    assert_eq!(acceptance.application().revision(), "application-1");
    assert_eq!(
        acceptance.policy_revision(),
        capture.request.context.policy_revision
    );
    assert_eq!(decision.scope().application(), acceptance.application());
    assert_eq!(decision.scope().route(), &capture.definition.route);
    assert_eq!(decision.scope().tool_id(), binding.tool_id());
    assert_eq!(decision.scope().tool_revision(), binding.tool_revision());
    assert_eq!(
        decision.scope().constraint_revision(),
        binding.constraint_revision()
    );
    assert_eq!(decision.scope().definition(), &capture.definition);
    assert_eq!(decision.scope().origin(), &grant.origin);
    assert_eq!(decision.scope().thread_id(), &capture.before.thread.id);
    assert_eq!(
        decision.scope().thread_generation(),
        &capture.before.thread.generation
    );
    assert_eq!(decision.scope().effect_id(), &grant.effect_id);
    assert_eq!(decision.scope().policy_revision(), grant.policy_revision);
    assert_eq!(produced_text(&raw[4][0][0]), grant.execution_id.as_str());
    assert_eq!(produced_text(&raw[4][0][1]), grant.interrupt_id.as_str());
    assert_eq!(produced_text(&raw[4][0][2]), grant.task_id.as_str());
    assert_eq!(produced_text(&raw[4][0][3]), grant.thread_id.as_str());
    assert_eq!(
        produced_text(&raw[4][0][4]),
        grant.thread_generation.as_str()
    );
    assert_eq!(produced_u64(&raw[4][0][5]), grant.task_parking_revision);
    assert_eq!(produced_text(&raw[6][0][0]), grant.execution_id.as_str());
    assert_eq!(produced_text(&raw[6][0][1]), acceptance.turn_id().as_str());
    assert_eq!(
        produced_text(&raw[6][0][2]),
        acceptance.thread_id().as_str()
    );
    assert_eq!(
        produced_text(&raw[6][0][3]),
        acceptance.thread_generation().as_str()
    );
    assert_eq!(
        raw[6][0][6],
        rusqlite::types::Value::Integer(i64::from(acceptance.record_version()))
    );
    assert_eq!(
        produced_u64(&raw[6][0][4]),
        acceptance.before_snapshot().revision
    );
    assert_eq!(produced_u64(&raw[6][0][5]), acceptance.committed_revision());
    assert_eq!(
        produced_text(&raw[7][0][0]),
        decision.permission_id().as_str()
    );
    assert_eq!(
        produced_text(&raw[7][0][1]),
        decision.scope().thread_id().as_str()
    );
    assert_eq!(
        produced_text(&raw[7][0][2]),
        decision.scope().thread_generation().as_str()
    );
    assert_eq!(
        raw[7][0][4],
        rusqlite::types::Value::Integer(i64::from(decision.record_version()))
    );
    assert_eq!(produced_text(&raw[7][0][3]), decision.scope_digest());
    let expected_journal = vec![
        JournalEntry::from(&capture.before.execution.snapshot),
        JournalEntry::from(&accepted.snapshot),
    ];
    assert_eq!(capture.journal_before.len(), 4);
    assert_eq!(&capture.journal_before[2..], expected_journal.as_slice());
    assert_eq!(raw[3].len(), 5);
    assert_eq!(raw[3][3..].len(), expected_journal.len());
    // The complete five-column journal objects are the real mirrored persisted entries.
    for (entry, row) in expected_journal.iter().zip(&raw[3][3..]) {
        assert_eq!(produced_text(&row[1]), "commit");
        assert_eq!(produced_text(&row[2]), capture.before.thread.id.as_str());
        assert_eq!(produced_text(&row[3]), entry.execution_id.as_str());
        assert_eq!(produced_u64(&row[4]), entry.revision);
        assert_eq!(produced_u64(&row[5]), entry.owner_generation);
        assert_eq!(
            produced_text(&row[6]),
            serde_json::to_string(&entry.status).unwrap()
        );
        assert_eq!(produced_u64(&row[7]), entry.updated_at_ms);
        assert_eq!(
            serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
            *entry
        );
    }
    (acceptance, decision, grant, expected_journal)
}
fn assert_complete_produced_view(
    capture: ProducedViewCapture,
    acceptance: PermissionAcceptance,
    decision: PermissionDecision,
    grant: GrantRecord,
    expected_journal: Vec<JournalEntry>,
) {
    let view = capture.reader.unwrap();
    assert_eq!(view.thread(), &capture.read_before.thread);
    assert_eq!(view.execution(), &capture.execution_before);
    assert_eq!(&view.resource().thread, view.thread());
    assert_eq!(&view.resource().execution, view.execution());
    assert_eq!(view.grants(), std::slice::from_ref(&grant));
    assert!(view.acceptances().is_empty());
    assert_eq!(
        view.permission_acceptances(),
        std::slice::from_ref(&acceptance)
    );
    assert_eq!(view.permission_decisions(), std::slice::from_ref(&decision));
    assert!(view.permission_uses().is_empty());
    assert_eq!(
        view.creator_acceptances(),
        std::slice::from_ref(&acceptance)
    );
    assert_eq!(view.referenced_grants(), std::slice::from_ref(&grant));
    assert!(view.referenced_acceptances().is_empty());
    assert_eq!(view.journal_entries(), expected_journal.as_slice());
    assert_eq!(view.receipt().purpose(), PermissionReadPurpose::Dispatch);
    let usage = view.receipt().usage_at_capture();
    let receipt_usage = (usage.encoded_bytes(), usage.structural_nodes());
    assert!(
        receipt_usage.0 > capture.reader_usage.0 .0
            && receipt_usage.1 > capture.reader_usage.0 .1
            && receipt_usage.0 <= capture.reader_usage.1 .0
            && receipt_usage.1 <= capture.reader_usage.1 .1,
        "actual receipt stays within the same reader account"
    );
    assert!(
        capture.reader_usage.1 .0 > capture.reader_usage.0 .0
            && capture.reader_usage.1 .1 > capture.reader_usage.0 .1
            && capture.reader_usage.1 .0 <= 8_388_608
            && capture.reader_usage.1 .1 <= 65_536,
        "actual reader account stays finite and monotone"
    );
    assert_eq!(capture.read_after, capture.read_before);
    assert_eq!(capture.execution_after, capture.execution_before);
    assert_eq!(capture.journal_after, capture.journal_before);
    assert!(
        capture.raw_after == capture.raw_before,
        "populated read preserves all nine actual tables including its live metadata owner"
    );
}
#[test]
fn produced_always_permission_view_is_available_on_original_writer() {
    let capture = capture_produced_view_after_normal_retirement(false);
    let (acceptance, decision, grant, journal) = assert_produced_reader_setup(&capture);
    let code = capture
        .reader
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.reader.is_ok(),
        "produced permission view must be available on original writer; code={code}"
    );
    assert_complete_produced_view(capture, acceptance, decision, grant, journal);
}
#[test]
fn produced_always_permission_view_is_available_after_passive_reopen() {
    let capture = capture_produced_view_after_normal_retirement(true);
    let (acceptance, decision, grant, journal) = assert_produced_reader_setup(&capture);
    let code = capture
        .reader
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.reader.is_ok(),
        "produced permission view must be available after passive reopen; code={code}"
    );
    assert_complete_produced_view(capture, acceptance, decision, grant, journal);
}

struct CreatorTurnObservation {
    thread: ThreadView,
    execution: StoredExecution,
    journal: Vec<JournalEntry>,
    raw: ProducedRawNine,
}
fn observe_creator_turn_state(
    store: &SqliteStore,
    path: &std::path::Path,
    thread: &ThreadId,
    execution: &ExecutionId,
) -> CreatorTurnObservation {
    CreatorTurnObservation {
        thread: store.thread_view(thread).unwrap().unwrap(),
        execution: store.execution(execution).unwrap().unwrap(),
        journal: store.journal(execution).unwrap(),
        raw: produced_raw_nine(path),
    }
}
fn update_owned_creator_turn_record(
    path: &std::path::Path,
    permission_id: &str,
    expected_record: &str,
    replacement_record: &str,
) -> usize {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .unwrap();
    let rows = connection
        .execute(
            "UPDATE permission_decisions SET record = ?1 WHERE permission_id = ?2 AND record = ?3",
            rusqlite::params![replacement_record, permission_id, expected_record],
        )
        .unwrap();
    assert!(
        connection.close().is_ok(),
        "owned physical mutation observer closes normally"
    );
    rows
}
struct CreatorTurnCorruptionCapture {
    healthy: ProducedViewCapture,
    original_decision: Result<PermissionDecision, PermissionError>,
    original_canonical: Result<Vec<u8>, PermissionError>,
    original_codec_usage: ((usize, usize), (usize, usize)),
    original_row: Vec<rusqlite::types::Value>,
    mutant_row: Vec<rusqlite::types::Value>,
    original_record: String,
    mutant_record: String,
    creator_value_start: usize,
    mutation_rows: usize,
    mutant_decision: Result<PermissionDecision, PermissionError>,
    mutant_codec_usage: ((usize, usize), (usize, usize)),
    subject: Result<PermissionView, PermissionError>,
    subject_usage: ((usize, usize), (usize, usize)),
    corrupt_before: CreatorTurnObservation,
    subject_after: CreatorTurnObservation,
    repair_rows: Option<usize>,
    restored_final: CreatorTurnObservation,
    next_loaded: CreatorTurnObservation,
    original_aliases: usize,
    catalog_aliases: usize,
    application_aliases: usize,
    counter_aliases: usize,
    native_calls: usize,
    directory_retired: bool,
}
fn capture_creator_turn_corruption_after_normal_retirement() -> CreatorTurnCorruptionCapture {
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let registered_matches = registered == definition;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: ApprovalDecision::Always,
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };
    let producer = accept_permission_human(
        store.clone(),
        authority.clone(),
        catalog.clone(),
        &execution,
        fence(store.as_ref(), &thread),
        request.clone(),
    );
    // The original producer helper disposes its Directory; this copy retains the
    // same genuine sequence in the live scope until the owned fault is repaired.
    drop(service);
    let read_owner = store.owner_generation();
    let read_before = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = produced_raw_nine(&directory.0);
    // Passive reading uses the actual recorded execution owner, not the live store owner.
    let read_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: read_before.thread.resource_revision,
    };
    let invocation = InvocationContext {
        thread_id: read_before.thread.id.clone(),
        thread_generation: read_before.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: application.identity().revision.clone(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let gate_control = catalog.validate_call(
        &invocation,
        &definition,
        &execution_before.snapshot.tasks[0],
        PermissionCallPhase::Unclaimed,
    );
    let (resource_control, resource_usage) = {
        let mut budget = PermissionEvidenceBudget::new();
        let initial_usage = produced_usage(&budget);
        let result = store.permission_resource(&execution, &mut budget);
        (result, (initial_usage, produced_usage(&budget)))
    };
    let (reader, reader_usage) = {
        let mut budget = PermissionEvidenceBudget::new();
        let initial_usage = produced_usage(&budget);
        let result = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            read_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut budget,
        );
        (result, (initial_usage, produced_usage(&budget)))
    };
    let read_after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = produced_raw_nine(&directory.0);
    let original_row = raw_after.tables[7][0].clone();
    let original_record = produced_text(&original_row[5]).to_owned();
    let permission_id = produced_text(&original_row[0]).to_owned();
    let (original_decision, original_canonical, original_codec_usage) = {
        let mut budget = PermissionEvidenceBudget::new();
        let initial_usage = produced_usage(&budget);
        let decoded = PermissionDecision::decode_record(original_record.as_bytes(), &mut budget);
        // A physical fault is owned only after this actual bounded source control.
        let canonical = decoded
            .as_ref()
            .expect("actual produced Decision passes its bounded public codec")
            .encode_record(&mut budget);
        (decoded, canonical, (initial_usage, produced_usage(&budget)))
    };
    let actual_original = original_decision.as_ref().unwrap();
    assert!(
        actual_original.creator_turn_id() == &request.turn_id
            && actual_original.creator_turn_id().as_str() == "approved-turn",
        "physical fault starts from the actual approved creator turn"
    );
    assert!(
        original_canonical.as_ref().unwrap().as_slice() == original_record.as_bytes(),
        "actual produced Decision is already canonical"
    );
    let parsed: serde_json::Value = serde_json::from_str(&original_record).unwrap();
    assert_eq!(
        parsed
            .get("creator_turn_id")
            .and_then(serde_json::Value::as_str),
        Some(actual_original.creator_turn_id().as_str())
    );
    let wrong_turn = TurnId::new("creator-wrong").unwrap();
    let original_turn = actual_original.creator_turn_id().as_str();
    assert!(original_turn.len() == 13 && wrong_turn.as_str().len() == 13);
    let prefix = "\"creator_turn_id\":\"";
    let scalar = format!("{prefix}{original_turn}\"");
    assert!(
        original_record.match_indices(&scalar).count() == 1,
        "canonical root creator scalar has exactly one owned span"
    );
    let creator_value_start = original_record.find(&scalar).unwrap() + prefix.len();
    let mut mutant_record = original_record.clone();
    mutant_record.replace_range(
        creator_value_start..creator_value_start + original_turn.len(),
        wrong_turn.as_str(),
    );
    let mut mutant_row = original_row.clone();
    mutant_row[5] = rusqlite::types::Value::Text(mutant_record.clone());
    let mutation_rows = update_owned_creator_turn_record(
        &directory.0,
        &permission_id,
        &original_record,
        &mutant_record,
    );
    let corrupt_before =
        observe_creator_turn_state(store.as_ref(), &directory.0, &thread, &execution);
    let (mutant_decision, mutant_codec_usage) = {
        let mut budget = PermissionEvidenceBudget::new();
        let initial_usage = produced_usage(&budget);
        // Decode the actual persisted mutant, never the proposed replacement DTO.
        let result = PermissionDecision::decode_record(
            produced_text(&corrupt_before.raw.tables[7][0][5]).as_bytes(),
            &mut budget,
        );
        (result, (initial_usage, produced_usage(&budget)))
    };
    let (subject, subject_usage) = {
        let mut budget = PermissionEvidenceBudget::new();
        let initial_usage = produced_usage(&budget);
        let result = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            read_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut budget,
        );
        (result, (initial_usage, produced_usage(&budget)))
    };
    // This is unmasked actual subject AFTER, retained before any owned repair.
    let subject_after =
        observe_creator_turn_state(store.as_ref(), &directory.0, &thread, &execution);
    let repair_rows = if subject_after.raw.tables[7].as_slice() == std::slice::from_ref(&mutant_row)
    {
        Some(update_owned_creator_turn_record(
            &directory.0,
            &permission_id,
            &mutant_record,
            &original_record,
        ))
    } else {
        // Missing or unexpected rows are not overwritten by test cleanup.
        None
    };
    let restored_final =
        observe_creator_turn_state(store.as_ref(), &directory.0, &thread, &execution);
    store.readiness().unwrap();
    let original_aliases = Arc::strong_count(&store);
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_loaded = observe_creator_turn_state(&next, &directory.0, &thread, &execution);
    next.readiness().unwrap();
    drop(next);
    let catalog_aliases = Arc::strong_count(&catalog);
    drop(catalog);
    let application_aliases = Arc::strong_count(&application);
    drop(application);
    drop(authority);
    let counter_aliases = Arc::strong_count(&calls);
    let native_calls = calls.load(Ordering::SeqCst);
    drop(calls);
    drop(invocation);
    drop(parsed);
    drop(wrong_turn);
    drop(permission_id);
    let directory_path = directory.0.clone();
    drop(directory);
    let directory_retired = !directory_path.exists();
    drop(directory_path);
    let healthy = ProducedViewCapture {
        producer,
        reader,
        resource_control,
        gate_control,
        before,
        pending,
        request,
        definition,
        read_before,
        read_after,
        execution_before,
        execution_after,
        journal_before,
        journal_after,
        raw_before,
        raw_after,
        reader_usage,
        resource_usage,
        original_owner,
        read_owner,
        next_owner,
        read_fence,
        passive_reopen: false,
        registered_matches,
    };
    CreatorTurnCorruptionCapture {
        healthy,
        original_decision,
        original_canonical,
        original_codec_usage,
        original_row,
        mutant_row,
        original_record,
        mutant_record,
        creator_value_start,
        mutation_rows,
        mutant_decision,
        mutant_codec_usage,
        subject,
        subject_usage,
        corrupt_before,
        subject_after,
        repair_rows,
        restored_final,
        next_loaded,
        original_aliases,
        catalog_aliases,
        application_aliases,
        counter_aliases,
        native_calls,
        directory_retired,
    }
}
#[test]
fn populated_permission_view_refuses_codec_valid_wrong_creator_turn() {
    let capture = capture_creator_turn_corruption_after_normal_retirement();
    let (acceptance, decision, grant, journal) = assert_produced_reader_setup(&capture.healthy);
    let original_owner = capture.healthy.original_owner;
    let next_owner = capture.healthy.next_owner;
    assert!(
        capture.directory_retired,
        "scoped fixture Directory is disposed"
    );
    assert!(
        capture.original_aliases == 1
            && capture.catalog_aliases == 1
            && capture.application_aliases == 1
            && capture.counter_aliases == 1
            && capture.native_calls == 0,
        "all original store and native aliases retire normally without invocation"
    );
    let original = capture.original_decision.as_ref().unwrap();
    assert!(
        original == &decision,
        "original codec control retains the real Decision"
    );
    assert!(
        capture.original_canonical.as_ref().unwrap().as_slice()
            == capture.original_record.as_bytes(),
        "original public canonical control matches the actual owned payload"
    );
    assert_eq!(original.creator_turn_id(), acceptance.turn_id());
    assert_eq!(original.creator_turn_id().as_str(), "approved-turn");
    assert_eq!(
        capture.mutation_rows, 1,
        "one actual D row receives the owned fault"
    );
    assert_eq!(capture.original_row.len(), 6);
    assert_eq!(capture.mutant_row.len(), 6);
    assert!(
        capture.original_row.as_slice() == capture.healthy.raw_after.tables[7][0].as_slice()
            && capture.original_row[..5] == capture.mutant_row[..5],
        "complete actual D row has unchanged five SQL header cells"
    );
    assert_eq!(
        produced_text(&capture.original_row[5]),
        capture.original_record.as_str()
    );
    assert_eq!(
        produced_text(&capture.mutant_row[5]),
        capture.mutant_record.as_str()
    );
    let start = capture.creator_value_start;
    assert!(
        capture.original_record.len() == capture.mutant_record.len()
            && &capture.original_record[start..start + 13] == "approved-turn"
            && &capture.mutant_record[start..start + 13] == "creator-wrong"
            && capture.original_record[..start] == capture.mutant_record[..start]
            && capture.original_record[start + 13..] == capture.mutant_record[start + 13..],
        "only the owned thirteen-byte scalar changes; all syntax and other bytes remain exact"
    );
    assert!(
        capture.corrupt_before.raw.tables[7].as_slice()
            == std::slice::from_ref(&capture.mutant_row),
        "actual corrupt BEFORE retains exactly the complete owned mutant D row"
    );
    for table in 0..9 {
        if table != 7 {
            assert!(
                capture.corrupt_before.raw.tables[table] == capture.healthy.raw_after.tables[table],
                "physical fault changes no other actual table"
            );
        }
    }
    assert_eq!(capture.corrupt_before.thread, capture.healthy.read_after);
    assert_eq!(
        capture.corrupt_before.execution,
        capture.healthy.execution_after
    );
    assert_eq!(
        capture.corrupt_before.journal,
        capture.healthy.journal_after
    );
    let codec_code = capture
        .mutant_decision
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.mutant_decision.is_ok(),
        "actual persisted mutant passes a separate fresh public codec; code={codec_code}"
    );
    let mutant = capture.mutant_decision.as_ref().unwrap();
    assert!(
        mutant.record_version() == original.record_version()
            && mutant.permission_id() == original.permission_id()
            && mutant.scope_digest() == original.scope_digest()
            && mutant.scope() == original.scope()
            && mutant.creator_execution_id() == original.creator_execution_id()
            && mutant.creator_task_id() == original.creator_task_id()
            && mutant.creator_interrupt_id() == original.creator_interrupt_id()
            && mutant.creator_turn_id().as_str() == "creator-wrong"
            && mutant.creator_turn_id() != original.creator_turn_id(),
        "the actual codec-valid mutant differs only in its creator turn getter"
    );
    for usage in [capture.original_codec_usage, capture.mutant_codec_usage] {
        assert!(
            usage.0 == (0, 0)
                && usage.1 .0 > usage.0 .0
                && usage.1 .1 > usage.0 .1
                && usage.1 .0 <= 8_388_608
                && usage.1 .1 <= 65_536,
            "independent actual codec controls stay finite on fresh accounts"
        );
    }
    let healthy_code = capture
        .healthy
        .reader
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.healthy.reader.is_ok(),
        "same original-owner healthy public View control succeeds; code={healthy_code}"
    );
    assert_eq!(capture.next_loaded.thread, capture.restored_final.thread);
    assert_eq!(
        capture.next_loaded.execution,
        capture.restored_final.execution
    );
    assert_eq!(capture.next_loaded.journal, capture.restored_final.journal);
    assert!(
        capture.next_loaded.raw.tables[1..] == capture.restored_final.raw.tables[1..],
        "normal N+1 writer preserves actual restored-final nonmetadata rows"
    );
    assert_eq!(capture.restored_final.raw.tables[0].len(), 1);
    assert_eq!(capture.next_loaded.raw.tables[0].len(), 1);
    assert!(
        capture.next_loaded.raw.tables[0][0][..3] == capture.restored_final.raw.tables[0][0][..3],
        "N+1 changes no unrelated metadata cell"
    );
    assert_eq!(
        produced_u64(&capture.restored_final.raw.tables[0][0][3]),
        original_owner
    );
    assert_eq!(
        produced_u64(&capture.next_loaded.raw.tables[0][0][3]),
        next_owner
    );
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    // Complete the healthy full public View control only after normal retirement.
    assert_complete_produced_view(capture.healthy, acceptance, decision, grant, journal);
    let subject_code = capture
        .subject
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    // FIRST subject feature predicate; setup errors or Unsupported do not substitute.
    assert!(
        matches!(&capture.subject, Err(PermissionError::CorruptRecord)),
        "codec-valid wrong creator turn must be refused; code={subject_code}"
    );
    assert!(
        capture.subject_after.raw == capture.corrupt_before.raw,
        "subject read preserves all nine unmasked actual tables"
    );
    assert_eq!(capture.subject_after.thread, capture.corrupt_before.thread);
    assert_eq!(
        capture.subject_after.execution,
        capture.corrupt_before.execution
    );
    assert_eq!(
        capture.subject_after.journal,
        capture.corrupt_before.journal
    );
    assert_eq!(
        capture.repair_rows,
        Some(1),
        "exact owned D payload is restored once"
    );
    assert!(
        capture.restored_final.raw.tables[7].as_slice()
            == std::slice::from_ref(&capture.original_row),
        "actual restored final retains the complete original D row"
    );
    for table in 0..9 {
        if table != 7 {
            assert!(
                capture.restored_final.raw.tables[table] == capture.subject_after.raw.tables[table],
                "owned repair preserves every unowned actual table"
            );
        }
    }
    assert_eq!(capture.restored_final.thread, capture.subject_after.thread);
    assert_eq!(
        capture.restored_final.execution,
        capture.subject_after.execution
    );
    assert_eq!(
        capture.restored_final.journal,
        capture.subject_after.journal
    );
    let usage = capture.subject_usage;
    assert!(
        usage.0 == (0, 0)
            && usage.1 .0 > usage.0 .0
            && usage.1 .1 > usage.0 .1
            && usage.1 .0 <= 8_388_608
            && usage.1 .1 <= 65_536,
        "actual subject account remains fresh, finite and monotone"
    );
}

// Independent first-claim parents: normal admissions and real public human grants.
// These observations are data only; no result or reader receipt supplies authority.
fn first_claim_raw_nine(path: &std::path::Path) -> ProducedRawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let raw = ProducedRawNine { tables: [
        produced_raw_cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        produced_raw_cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        produced_raw_cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 2", 5, 1),
        produced_raw_cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 7", 9, 6),
        produced_raw_cells(&connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        produced_raw_cells(&connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] };
    assert!(
        connection.close().is_ok(),
        "read-only produced observer closes normally"
    );
    raw
}

struct FirstClaimCapture {
    actual_ok: bool,
    actual_error: Option<PermissionError>,
    always: bool,
    before: ThreadView,
    after: ThreadView,
    journal_before: Vec<JournalEntry>,
    journal_after: Vec<JournalEntry>,
    raw_before: ProducedRawNine,
    raw_after: ProducedRawNine,
    ready: Option<PermissionView>,
    post_reader: Option<Result<PermissionView, PermissionError>>,
    after_reader: ThreadView,
    execution_after_reader: StoredExecution,
    journal_after_reader: Vec<JournalEntry>,
    raw_after_reader: ProducedRawNine,
    subject_usage: ((usize, usize), (usize, usize)),
    reader_usage: Option<((usize, usize), (usize, usize))>,
    owner: u64,
}
fn capture_first_claim_after_normal_retirement(always: bool) -> FirstClaimCapture {
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let registered_matches = registered == definition;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: if always {
                ApprovalDecision::Always
            } else {
                ApprovalDecision::Once
            },
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };

    let producer = if always {
        accept_permission_human(
            store.clone(),
            authority.clone(),
            catalog.clone(),
            &execution,
            fence(store.as_ref(), &thread),
            request.clone(),
        )
    } else {
        service
            .accept(&execution, fence(store.as_ref(), &thread), request.clone())
            .map_err(PermissionError::Approval)
    }
    .unwrap();
    let before_subject = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = first_claim_raw_nine(&directory.0);
    assert!(
        registered_matches,
        "actual native Definition remains registered"
    );
    assert_eq!(before_subject.execution.as_ref(), Some(&producer));
    assert_eq!(execution_before, producer);
    assert_eq!(producer.snapshot.revision, 3);
    assert_eq!(producer.snapshot.status, ExecutionStatus::Running);
    assert_eq!(producer.snapshot.turn_id, request.turn_id);
    assert_eq!(producer.snapshot.owner_generation, original_owner);
    assert_eq!(producer.snapshot.definition, definition);
    assert_eq!(producer.snapshot.attempts_used, 0);
    assert!(producer.snapshot.attempt_history.is_empty());
    assert_eq!(producer.snapshot.tasks.len(), 1);
    assert_eq!(producer.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(producer.snapshot.tasks[0].approved);
    assert!(producer.snapshot.tasks[0].attempt_id.is_none());
    assert!(producer.snapshot.tasks[0].result.is_none());
    assert!(producer.snapshot.tasks[0].failure.is_none());
    assert_eq!(
        producer.snapshot.tasks[0].interrupt,
        before.execution.snapshot.tasks[0].interrupt
    );
    assert_eq!(before_subject.thread.resource_revision, 4);
    assert_eq!(journal_before.len(), 4);
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_before.tables[4].len(), 1);
    let mut expected_grant = before.grants[0].clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(expected_grant.token, pending[0].token);
    assert_eq!(expected_grant.verifier, token_verifier(&pending[0].token));
    assert_eq!(
        expected_grant.proposal_digest,
        proposal_digest(&before.execution.snapshot.tasks[0], &expected_grant.prompt).unwrap()
    );
    expected_grant.consumed = true;
    let observed_grant: GrantRecord =
        serde_json::from_str(produced_text(&raw_before.tables[4][0][7])).unwrap();
    assert_eq!(observed_grant, expected_grant);
    assert!(raw_before.tables[8].is_empty());
    assert_eq!(raw_before.tables[5].len(), usize::from(!always));
    assert_eq!(raw_before.tables[6].len(), usize::from(always));
    assert_eq!(raw_before.tables[7].len(), usize::from(always));
    b4_kernel::validate_snapshot(&producer.snapshot).unwrap();
    let subject_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: before_subject.thread.resource_revision,
    };
    let invocation_context = InvocationContext {
        thread_id: thread.clone(),
        thread_generation: before_subject.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: application.identity().revision.clone(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    catalog
        .validate_call(
            &invocation_context,
            &definition,
            &execution_before.snapshot.tasks[0],
            PermissionCallPhase::Unclaimed,
        )
        .unwrap();
    let ready = if always {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            subject_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut control_account,
        )
        .unwrap();
        assert_eq!(actual.thread(), &before_subject.thread);
        assert_eq!(actual.execution(), &execution_before);
        assert_eq!(actual.grants(), std::slice::from_ref(&observed_grant));
        assert!(actual.acceptances().is_empty() && actual.permission_uses().is_empty());
        let acceptance = PermissionAcceptance::decode_record(
            produced_text(&raw_before.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        let decision = PermissionDecision::decode_record(
            produced_text(&raw_before.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(
            actual.permission_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.permission_decisions(),
            std::slice::from_ref(&decision)
        );
        assert_eq!(
            actual.creator_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.referenced_grants(),
            std::slice::from_ref(&observed_grant)
        );
        assert!(actual.referenced_acceptances().is_empty());
        assert_eq!(actual.journal_entries(), &journal_before[2..]);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
        Some(actual)
    } else {
        None
    };
    {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = store
            .permission_resource(&execution, &mut control_account)
            .unwrap();
        assert_eq!(actual.thread, before_subject.thread);
        assert_eq!(actual.execution, execution_before);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
    }
    assert!(
        first_claim_raw_nine(&directory.0) == raw_before,
        "healthy controls preserve all nine subject BEFORE tables"
    );
    let mut subject_account = PermissionEvidenceBudget::new();
    let initial_usage = produced_usage(&subject_account);
    let actual_result = prepare_permission_dispatch_claim(
        store.as_ref(),
        authority.as_ref(),
        catalog.as_ref(),
        &execution,
        subject_fence,
        &context,
        task.clone(),
        AttemptId::new("release-attempt").unwrap(),
        facts(104),
        &mut subject_account,
    );
    let final_usage = produced_usage(&subject_account);
    // Full unmasked physical AFTER is observed before inspecting the actual result.
    let after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = first_claim_raw_nine(&directory.0);
    assert_eq!(after.execution.as_ref(), Some(&execution_after));
    let (post_reader, reader_usage) = if always {
        let mut fresh_reader_account = PermissionEvidenceBudget::new();
        let initial = produced_usage(&fresh_reader_account);
        let actual = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            ApprovalFence {
                execution: (&execution_after.snapshot).into(),
                resource_revision: after.thread.resource_revision,
            },
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut fresh_reader_account,
        );
        // Retain either reader result; it must not mask claim refusal or prevent cleanup.
        let usage = produced_usage(&fresh_reader_account);
        (Some(actual), Some((initial, usage)))
    } else {
        (None, None)
    };
    // Capture the actual post-reader state without assuming storage invariance.
    // N+1 closure below must preserve this actual state on either result branch.
    let after_reader = store.thread_view(&thread).unwrap().unwrap();
    let execution_after_reader = store.execution(&execution).unwrap().unwrap();
    let journal_after_reader = store.journal(&execution).unwrap();
    let raw_after_reader = first_claim_raw_nine(&directory.0);
    let (actual_ok, actual_error) = match actual_result {
        Ok(claim) => {
            drop(claim);
            (true, None)
        }
        Err(error) => (false, Some(error)),
    };
    drop(subject_account);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(application);
    store.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "all operation store aliases retire normally"
    );
    drop(store);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "ownership and reading never invoke native"
    );
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_view = next.thread_view(&thread).unwrap().unwrap();
    let next_execution = next.execution(&execution).unwrap().unwrap();
    let next_journal = next.journal(&execution).unwrap();
    let next_raw = first_claim_raw_nine(&directory.0);
    next.readiness().unwrap();
    drop(next);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(
        next_view, after_reader,
        "N+1 preserves full ACTUAL final reader AFTER"
    );
    assert_eq!(next_execution, execution_after_reader);
    assert_eq!(next_journal, journal_after_reader);
    assert!(
        next_raw.tables[1..] == raw_after_reader.tables[1..],
        "N+1 preserves every nonmetadata raw cell in actual AFTER"
    );
    assert_eq!(
        next_raw.tables[0][0][..3],
        raw_after_reader.tables[0][0][..3]
    );
    assert_eq!(produced_u64(&next_raw.tables[0][0][3]), next_owner);
    assert_eq!(
        produced_u64(&raw_after_reader.tables[0][0][3]),
        original_owner
    );
    drop(calls);
    let removed = directory.0.clone();
    drop(directory);
    assert!(
        !removed.exists(),
        "actual Directory retires before availability assertion"
    );
    FirstClaimCapture {
        actual_ok,
        actual_error,
        always,
        before: before_subject,
        after,
        journal_before,
        journal_after,
        raw_before,
        raw_after,
        ready,
        post_reader,
        after_reader,
        execution_after_reader,
        journal_after_reader,
        raw_after_reader,
        subject_usage: (initial_usage, final_usage),
        reader_usage,
        owner: original_owner,
    }
}
fn first_claim_journal_sequence(value: &rusqlite::types::Value) -> i64 {
    match value {
        rusqlite::types::Value::Integer(sequence) if *sequence > 0 => *sequence,
        _ => panic!("first-claim journal sequence retains positive INTEGER"),
    }
}
fn assert_first_claim_positive(capture: FirstClaimCapture) {
    let before = capture.before.execution.as_ref().unwrap();
    let after = capture.after.execution.as_ref().unwrap();
    let expected = transition(
        &before.snapshot,
        KernelCommand::Dispatch {
            task_id: TaskId::new("release").unwrap(),
            attempt_id: AttemptId::new("release-attempt").unwrap(),
        },
        facts(104),
    )
    .unwrap();
    assert_eq!(after.thread_id, before.thread_id);
    assert_eq!(
        after.snapshot, expected.snapshot,
        "complete actual Kernel Dispatch post"
    );
    assert_eq!(after.snapshot.revision, 4);
    assert_eq!(after.snapshot.owner_generation, capture.owner);
    assert_eq!(after.snapshot.status, ExecutionStatus::Running);
    assert_eq!(
        after.snapshot.turn_id,
        TurnId::new("approved-turn").unwrap()
    );
    assert_eq!(after.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        after.snapshot.tasks[0].attempt_id,
        Some(AttemptId::new("release-attempt").unwrap())
    );
    assert_eq!(after.snapshot.attempts_used, 1);
    assert_eq!(
        after.snapshot.attempt_history,
        vec![AttemptId::new("release-attempt").unwrap()]
    );
    let mut expected_thread = capture.before.thread.clone();
    expected_thread.resource_revision = expected_thread.resource_revision.checked_add(1).unwrap();
    assert_eq!(capture.after.thread, expected_thread);
    assert_eq!(capture.after.thread.resource_revision, 5);
    assert_eq!(capture.journal_before.len(), 4);
    assert_eq!(capture.journal_after.len(), 5);
    assert_eq!(
        &capture.journal_after[..4],
        capture.journal_before.as_slice()
    );
    let dispatch_entry = JournalEntry::from(&after.snapshot);
    assert_eq!(capture.journal_after[4], dispatch_entry);
    for index in [0, 4, 5, 6, 7, 8] {
        assert!(
            capture.raw_before.tables[index] == capture.raw_after.tables[index],
            "Dispatch preserves full metadata and original authority tables"
        );
    }
    assert_eq!(
        capture.raw_after.tables[1][0][..5],
        capture.raw_before.tables[1][0][..5]
    );
    assert_eq!(produced_u64(&capture.raw_after.tables[1][0][5]), 5);
    assert_eq!(
        capture.raw_after.tables[1][0][6],
        capture.raw_before.tables[1][0][6]
    );
    let row = &capture.raw_after.tables[2][0];
    assert_eq!(row[..2], capture.raw_before.tables[2][0][..2]);
    assert_eq!(produced_u64(&row[2]), after.snapshot.revision);
    assert_eq!(produced_u64(&row[3]), capture.owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
        after.snapshot
    );
    assert_eq!(capture.raw_before.tables[3].len(), 5);
    assert_eq!(capture.raw_after.tables[3].len(), 6);
    assert_eq!(
        &capture.raw_after.tables[3][..5],
        capture.raw_before.tables[3].as_slice()
    );
    let row = &capture.raw_after.tables[3][5];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&capture.raw_before.tables[3][4][0])
    );
    assert_eq!(produced_text(&row[1]), "commit");
    assert_eq!(produced_text(&row[2]), capture.after.thread.id.as_str());
    assert_eq!(produced_text(&row[3]), dispatch_entry.execution_id.as_str());
    assert_eq!(produced_u64(&row[4]), dispatch_entry.revision);
    assert_eq!(produced_u64(&row[5]), dispatch_entry.owner_generation);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&dispatch_entry.status).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), dispatch_entry.updated_at_ms);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        dispatch_entry
    );
    let usage = capture.subject_usage;
    assert!(
        usage.1 .0 > usage.0 .0
            && usage.1 .1 > usage.0 .1
            && usage.1 .0 <= 8_388_608
            && usage.1 .1 <= 65_536,
        "same original claim account stays finite and monotone"
    );
    if capture.always {
        // This separate assertion is initially unreached. A reachable reader failure
        // must be independently accepted before its future implementation.
        let reader = capture.post_reader.unwrap();
        let code = reader
            .as_ref()
            .err()
            .map(PermissionError::code)
            .unwrap_or("ok");
        assert!(
            reader.is_ok(),
            "public first-Dispatch InFlight view must be available; code={code}"
        );
        let reader = reader.unwrap();
        assert_eq!(capture.after_reader, capture.after);
        assert_eq!(&capture.execution_after_reader, after);
        assert_eq!(capture.journal_after_reader, capture.journal_after);
        assert!(
            capture.raw_after_reader == capture.raw_after,
            "fresh post-reader preserves all nine ACTUAL claim AFTER tables"
        );
        let ready = capture.ready.unwrap();
        assert_eq!(reader.thread(), &capture.after.thread);
        assert_eq!(reader.execution(), after);
        assert_eq!(
            reader.permission_acceptances(),
            ready.permission_acceptances()
        );
        assert_eq!(reader.permission_decisions(), ready.permission_decisions());
        assert_eq!(reader.grants(), ready.grants());
        assert_eq!(reader.creator_acceptances(), ready.creator_acceptances());
        assert_eq!(reader.referenced_grants(), ready.referenced_grants());
        assert!(
            reader.acceptances().is_empty()
                && reader.permission_uses().is_empty()
                && reader.referenced_acceptances().is_empty()
        );
        assert_eq!(reader.journal_entries(), &capture.journal_after[2..]);
        let usage = capture.reader_usage.unwrap();
        assert!(
            usage.1 .0 > usage.0 .0
                && usage.1 .1 > usage.0 .1
                && usage.1 .0 <= 8_388_608
                && usage.1 .1 <= 65_536,
            "fresh reader account stays finite and monotone"
        );
    }
}
#[test]
fn bounded_once_claim_is_persisted_after_normal_close_and_next_writer() {
    let capture = capture_first_claim_after_normal_retirement(false);
    let code = capture
        .actual_error
        .as_ref()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.actual_ok,
        "same opaque bounded Once claim must succeed; code={code}"
    );
    assert_first_claim_positive(capture);
}
#[test]
fn first_always_claim_is_persisted_after_normal_close_and_next_writer() {
    let capture = capture_first_claim_after_normal_retirement(true);
    let code = capture
        .actual_error
        .as_ref()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert!(
        capture.actual_ok,
        "first human Always claim must succeed; code={code}"
    );
    assert_first_claim_positive(capture);
}

// This observer forwards real opaque evidence and the original account unchanged.
// It counts reached sibling boundaries; it creates no authorization or result.
struct ReadyCancellationObservedStore {
    inner: Arc<SqliteStore>,
    captures: Arc<AtomicUsize>,
    terminal_entries: Arc<AtomicUsize>,
}
struct ReadyCancellationAuthority {
    expected_context: ApprovalContext,
    expected_resource: ApprovalResource,
    expected_target: RunCancellationTarget,
    calls: Arc<AtomicUsize>,
}
impl PermissionCancellationAuthority for ReadyCancellationAuthority {
    fn authorize_cancel(
        &self,
        context: &ApprovalContext,
        resource: &ApprovalResource,
        target: &RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        if context != &self.expected_context
            || resource != &self.expected_resource
            || target != &self.expected_target
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl Store for ReadyCancellationObservedStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit(thread_id, initial)
    }
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(execution_id, fence, plan)
    }
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(execution_id)
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.inner.discover_unfinished(cursor)
    }

    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        self.inner.confinement_view_fenced(target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        self.inner
            .confine_recovery_fenced(original, target, reason, fresh_turn, facts)
    }
}
impl ApprovalStore for ReadyCancellationObservedStore {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
impl PermissionStore for ReadyCancellationObservedStore {
    fn durable_permissions(&self) -> bool {
        self.inner.durable_permissions()
    }

    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        self.inner.permission_resource(id, budget)
    }

    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        self.inner.permission_view(id, fence, selector, budget)
    }

    fn commit_permission_acceptance(
        &self,
        prepared: PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<crate::StoredExecution, PermissionError> {
        self.inner.commit_permission_acceptance(prepared, budget)
    }

    fn capture_permission_runtime(
        &self,
        selector: &PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCapture, PermissionError> {
        self.inner.capture_permission_runtime(selector, budget)
    }

    fn commit_permission_runtime(
        &self,
        prepared: &PermissionPreparedRuntimeMutation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCommit, PermissionError> {
        self.inner.commit_permission_runtime(prepared, budget)
    }

    fn capture_permission_ready_cancellation(
        &self,
        selector: &PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCapture, PermissionError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.inner
            .capture_permission_ready_cancellation(selector, budget)
    }
    fn commit_permission_ready_cancellation(
        &self,
        prepared: &PermissionPreparedReadyCancellation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<StoredExecution, PermissionError> {
        self.terminal_entries.fetch_add(1, Ordering::SeqCst);
        self.inner
            .commit_permission_ready_cancellation(prepared, budget)
    }
}

#[test]
fn approved_ready_cancellation_is_persisted_after_normal_close_and_next_writer() {
    let always = true;
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let registered_matches = registered == definition;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: if always {
                ApprovalDecision::Always
            } else {
                ApprovalDecision::Once
            },
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };

    let producer = if always {
        accept_permission_human(
            store.clone(),
            authority.clone(),
            catalog.clone(),
            &execution,
            fence(store.as_ref(), &thread),
            request.clone(),
        )
    } else {
        service
            .accept(&execution, fence(store.as_ref(), &thread), request.clone())
            .map_err(PermissionError::Approval)
    }
    .unwrap();
    let before_subject = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = first_claim_raw_nine(&directory.0);
    assert!(
        registered_matches,
        "actual native Definition remains registered"
    );
    assert_eq!(before_subject.execution.as_ref(), Some(&producer));
    assert_eq!(execution_before, producer);
    assert_eq!(producer.snapshot.revision, 3);
    assert_eq!(producer.snapshot.status, ExecutionStatus::Running);
    assert_eq!(producer.snapshot.turn_id, request.turn_id);
    assert_eq!(producer.snapshot.owner_generation, original_owner);
    assert_eq!(producer.snapshot.definition, definition);
    assert_eq!(producer.snapshot.attempts_used, 0);
    assert!(producer.snapshot.attempt_history.is_empty());
    assert_eq!(producer.snapshot.tasks.len(), 1);
    assert_eq!(producer.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(producer.snapshot.tasks[0].approved);
    assert!(producer.snapshot.tasks[0].attempt_id.is_none());
    assert!(producer.snapshot.tasks[0].result.is_none());
    assert!(producer.snapshot.tasks[0].failure.is_none());
    assert_eq!(
        producer.snapshot.tasks[0].interrupt,
        before.execution.snapshot.tasks[0].interrupt
    );
    assert_eq!(before_subject.thread.resource_revision, 4);
    assert_eq!(journal_before.len(), 4);
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_before.tables[4].len(), 1);
    let mut expected_grant = before.grants[0].clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(expected_grant.token, pending[0].token);
    assert_eq!(expected_grant.verifier, token_verifier(&pending[0].token));
    assert_eq!(
        expected_grant.proposal_digest,
        proposal_digest(&before.execution.snapshot.tasks[0], &expected_grant.prompt).unwrap()
    );
    expected_grant.consumed = true;
    let observed_grant: GrantRecord =
        serde_json::from_str(produced_text(&raw_before.tables[4][0][7])).unwrap();
    assert_eq!(observed_grant, expected_grant);
    assert!(raw_before.tables[8].is_empty());
    assert_eq!(raw_before.tables[5].len(), usize::from(!always));
    assert_eq!(raw_before.tables[6].len(), usize::from(always));
    assert_eq!(raw_before.tables[7].len(), usize::from(always));
    b4_kernel::validate_snapshot(&producer.snapshot).unwrap();
    let subject_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: before_subject.thread.resource_revision,
    };
    let ready = if always {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            subject_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut control_account,
        )
        .unwrap();
        assert_eq!(actual.thread(), &before_subject.thread);
        assert_eq!(actual.execution(), &execution_before);
        assert_eq!(actual.grants(), std::slice::from_ref(&observed_grant));
        assert!(actual.acceptances().is_empty() && actual.permission_uses().is_empty());
        let acceptance = PermissionAcceptance::decode_record(
            produced_text(&raw_before.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        let decision = PermissionDecision::decode_record(
            produced_text(&raw_before.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(
            actual.permission_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.permission_decisions(),
            std::slice::from_ref(&decision)
        );
        assert_eq!(
            actual.creator_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.referenced_grants(),
            std::slice::from_ref(&observed_grant)
        );
        assert!(actual.referenced_acceptances().is_empty());
        assert_eq!(actual.journal_entries(), &journal_before[2..]);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
        Some(actual)
    } else {
        None
    };
    {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = store
            .permission_resource(&execution, &mut control_account)
            .unwrap();
        assert_eq!(actual.thread, before_subject.thread);
        assert_eq!(actual.execution, execution_before);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
    }
    assert!(
        first_claim_raw_nine(&directory.0) == raw_before,
        "healthy controls preserve all nine subject BEFORE tables"
    );
    assert!(store.approval_durable());
    assert!(!store.durable_permissions());
    let target = RunCancellationTarget::from_view(&before_subject).unwrap();
    let expected = transition(
        &execution_before.snapshot,
        KernelCommand::Cancel {
            turn_id: execution_before.snapshot.turn_id.clone(),
            owner_generation: original_owner,
        },
        facts(104),
    )
    .unwrap();
    validate_commit(
        &execution_before.snapshot,
        target.execution_fence,
        &expected,
        original_owner,
    )
    .unwrap();
    let authority_calls = Arc::new(AtomicUsize::new(0));
    let capture_calls = Arc::new(AtomicUsize::new(0));
    let terminal_entries = Arc::new(AtomicUsize::new(0));
    let cancellation_authority = Arc::new(ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_subject.thread.clone(),
            execution: execution_before.clone(),
        },
        expected_target: target.clone(),
        calls: authority_calls.clone(),
    });
    let observed = Arc::new(ReadyCancellationObservedStore {
        inner: store.clone(),
        captures: capture_calls.clone(),
        terminal_entries: terminal_entries.clone(),
    });
    let mut subject_account = PermissionEvidenceBudget::new();
    let initial_usage = produced_usage(&subject_account);
    let cancellation_request = PermissionReadyCancellationRequest {
        context: &context,
        target: &target,
        facts: facts(104),
    };
    let actual_result = cancel_permission_ready(
        observed.as_ref(),
        cancellation_authority.as_ref(),
        catalog.as_ref(),
        &cancellation_request,
        &mut subject_account,
    );
    let final_usage = produced_usage(&subject_account);
    // Capture every actual typed/raw AFTER before inspecting the subject result.
    let after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = first_claim_raw_nine(&directory.0);
    let actual_authority_calls = authority_calls.load(Ordering::SeqCst);
    let actual_capture_calls = capture_calls.load(Ordering::SeqCst);
    let actual_terminal_entries = terminal_entries.load(Ordering::SeqCst);
    drop(subject_account);
    drop(observed);
    drop(cancellation_authority);
    drop(ready);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(application);
    store.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "every live store alias retires normally"
    );
    drop(store);
    let actual_native_calls = calls.load(Ordering::SeqCst);
    assert_eq!(
        actual_native_calls, 0,
        "cancellation and observations invoke no native effect"
    );
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_view = next.thread_view(&thread).unwrap().unwrap();
    let next_execution = next.execution(&execution).unwrap().unwrap();
    let next_journal = next.journal(&execution).unwrap();
    let next_raw = first_claim_raw_nine(&directory.0);
    next.readiness().unwrap();
    drop(next);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(
        next_view, after,
        "N+1 preserves complete actual cancellation AFTER"
    );
    assert_eq!(next_execution, execution_after);
    assert_eq!(next_journal, journal_after);
    assert!(next_raw.tables[1..] == raw_after.tables[1..]);
    assert_eq!(next_raw.tables[0][0][..3], raw_after.tables[0][0][..3]);
    assert_eq!(produced_u64(&next_raw.tables[0][0][3]), next_owner);
    assert_eq!(produced_u64(&raw_after.tables[0][0][3]), original_owner);
    drop(calls);
    drop(authority_calls);
    drop(capture_calls);
    drop(terminal_entries);
    let removed = directory.0.clone();
    drop(directory);
    assert!(
        !removed.exists(),
        "actual Directory is gone before the feature primary"
    );

    // Setup proof is separate from the intended terminal-success assertion.
    let actual_code = actual_result
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    eprintln!(
        "ready cancellation evidence: authorities={actual_authority_calls}; captures={actual_capture_calls}; terminal_entries={actual_terminal_entries}; code={actual_code}; initial_usage={initial_usage:?}; final_usage={final_usage:?}; native={actual_native_calls}; next_owner={next_owner}; directory_removed={}",
        !removed.exists(),
    );
    assert_eq!(
        actual_authority_calls, 2,
        "real R1 and renewed R2 cancellation authorization"
    );
    assert_eq!(
        actual_capture_calls, 3,
        "real R1, sensitive C1, and fresh R2 sibling captures"
    );
    assert_eq!(
        actual_terminal_entries, 1,
        "actual opaque Prepared reaches terminal sibling"
    );
    assert_eq!(after.execution.as_ref(), Some(&execution_after));
    assert!(
        initial_usage == (0, 0)
            && final_usage.0 > initial_usage.0
            && final_usage.1 > initial_usage.1
            && final_usage.0 <= 8_388_608
            && final_usage.1 <= 65_536,
        "one fresh original cancellation account is finite and monotone",
    );
    if actual_result.is_err() {
        assert!(matches!(&actual_result, Err(PermissionError::UnsupportedProfile)), "only reached terminal Unsupported is the intended missing-feature RED; code={actual_code}");
        assert_eq!(after, before_subject);
        assert_eq!(execution_after, execution_before);
        assert_eq!(journal_after, journal_before);
        assert!(
            raw_after == raw_before,
            "terminal default refusal preserves all nine actual tables"
        );
    }
    // FIRST subject success primary, after actual result capture and all cleanup.
    assert!(
        actual_result.is_ok(),
        "approved Ready cancellation must persist; code={actual_code}"
    );
    let returned = actual_result.unwrap();
    assert_eq!(returned, execution_after);
    assert_eq!(execution_after.thread_id, execution_before.thread_id);
    assert_eq!(
        execution_after.snapshot, expected.snapshot,
        "complete actual Kernel Cancel post"
    );
    assert_eq!(execution_after.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(execution_after.snapshot.revision, 4);
    assert_eq!(execution_after.snapshot.owner_generation, original_owner);
    assert_eq!(
        execution_after.snapshot.tasks[0].status,
        TaskStatus::Cancelled
    );
    assert!(execution_after.snapshot.tasks[0].approved);
    assert_eq!(
        execution_after.snapshot.tasks[0].interrupt,
        execution_before.snapshot.tasks[0].interrupt
    );
    assert_eq!(execution_after.snapshot.attempts_used, 0);
    assert!(execution_after.snapshot.attempt_history.is_empty());
    assert!(execution_after.snapshot.tasks[0].attempt_id.is_none());
    assert!(execution_after.snapshot.tasks[0].result.is_none());
    assert!(execution_after.snapshot.tasks[0].failure.is_none());
    let mut expected_thread = before_subject.thread.clone();
    expected_thread.resource_revision = expected_thread.resource_revision.checked_add(1).unwrap();
    assert_eq!(after.thread, expected_thread);
    assert_eq!(journal_after.len(), 5);
    assert_eq!(&journal_after[..4], journal_before.as_slice());
    let cancellation_entry = JournalEntry::from(&expected.snapshot);
    assert_eq!(journal_after[4], cancellation_entry);
    for index in [0, 4, 5, 6, 7, 8] {
        assert!(
            raw_before.tables[index] == raw_after.tables[index],
            "cancellation preserves all original authority and metadata cells"
        );
    }
    assert_eq!(raw_after.tables[1][0][..5], raw_before.tables[1][0][..5]);
    assert_eq!(produced_u64(&raw_after.tables[1][0][5]), 5);
    assert_eq!(raw_after.tables[1][0][6], raw_before.tables[1][0][6]);
    let row = &raw_after.tables[2][0];
    assert_eq!(row[..2], raw_before.tables[2][0][..2]);
    assert_eq!(produced_u64(&row[2]), expected.snapshot.revision);
    assert_eq!(produced_u64(&row[3]), original_owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
        expected.snapshot
    );
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_after.tables[3].len(), 6);
    assert_eq!(&raw_after.tables[3][..5], raw_before.tables[3].as_slice());
    let row = &raw_after.tables[3][5];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&raw_before.tables[3][4][0])
    );
    assert_eq!(produced_text(&row[1]), "commit");
    assert_eq!(produced_text(&row[2]), after.thread.id.as_str());
    assert_eq!(
        produced_text(&row[3]),
        cancellation_entry.execution_id.as_str()
    );
    assert_eq!(produced_u64(&row[4]), cancellation_entry.revision);
    assert_eq!(produced_u64(&row[5]), cancellation_entry.owner_generation);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&cancellation_entry.status).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), cancellation_entry.updated_at_ms);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        cancellation_entry
    );
}

fn second_admission_raw_nine(path: &std::path::Path) -> ProducedRawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let raw = ProducedRawNine { tables: [
        produced_raw_cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        produced_raw_cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        produced_raw_cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 3", 5, 2),
        produced_raw_cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 8", 9, 7),
        produced_raw_cells(&connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        produced_raw_cells(&connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] };
    assert!(
        connection.close().is_ok(),
        "read-only produced observer closes normally"
    );
    raw
}

#[test]
fn cancelled_always_execution_releases_slot_for_fenced_second_admission_after_reopen() {
    let always = true;
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let registered_matches = registered == definition;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: if always {
                ApprovalDecision::Always
            } else {
                ApprovalDecision::Once
            },
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };

    let producer = if always {
        accept_permission_human(
            store.clone(),
            authority.clone(),
            catalog.clone(),
            &execution,
            fence(store.as_ref(), &thread),
            request.clone(),
        )
    } else {
        service
            .accept(&execution, fence(store.as_ref(), &thread), request.clone())
            .map_err(PermissionError::Approval)
    }
    .unwrap();
    let before_subject = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = first_claim_raw_nine(&directory.0);
    assert!(
        registered_matches,
        "actual native Definition remains registered"
    );
    assert_eq!(before_subject.execution.as_ref(), Some(&producer));
    assert_eq!(execution_before, producer);
    assert_eq!(producer.snapshot.revision, 3);
    assert_eq!(producer.snapshot.status, ExecutionStatus::Running);
    assert_eq!(producer.snapshot.turn_id, request.turn_id);
    assert_eq!(producer.snapshot.owner_generation, original_owner);
    assert_eq!(producer.snapshot.definition, definition);
    assert_eq!(producer.snapshot.attempts_used, 0);
    assert!(producer.snapshot.attempt_history.is_empty());
    assert_eq!(producer.snapshot.tasks.len(), 1);
    assert_eq!(producer.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(producer.snapshot.tasks[0].approved);
    assert!(producer.snapshot.tasks[0].attempt_id.is_none());
    assert!(producer.snapshot.tasks[0].result.is_none());
    assert!(producer.snapshot.tasks[0].failure.is_none());
    assert_eq!(
        producer.snapshot.tasks[0].interrupt,
        before.execution.snapshot.tasks[0].interrupt
    );
    assert_eq!(before_subject.thread.resource_revision, 4);
    assert_eq!(journal_before.len(), 4);
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_before.tables[4].len(), 1);
    let mut expected_grant = before.grants[0].clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(expected_grant.token, pending[0].token);
    assert_eq!(expected_grant.verifier, token_verifier(&pending[0].token));
    assert_eq!(
        expected_grant.proposal_digest,
        proposal_digest(&before.execution.snapshot.tasks[0], &expected_grant.prompt).unwrap()
    );
    expected_grant.consumed = true;
    let observed_grant: GrantRecord =
        serde_json::from_str(produced_text(&raw_before.tables[4][0][7])).unwrap();
    assert_eq!(observed_grant, expected_grant);
    assert!(raw_before.tables[8].is_empty());
    assert_eq!(raw_before.tables[5].len(), usize::from(!always));
    assert_eq!(raw_before.tables[6].len(), usize::from(always));
    assert_eq!(raw_before.tables[7].len(), usize::from(always));
    b4_kernel::validate_snapshot(&producer.snapshot).unwrap();
    let subject_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: before_subject.thread.resource_revision,
    };
    let ready = if always {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            subject_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut control_account,
        )
        .unwrap();
        assert_eq!(actual.thread(), &before_subject.thread);
        assert_eq!(actual.execution(), &execution_before);
        assert_eq!(actual.grants(), std::slice::from_ref(&observed_grant));
        assert!(actual.acceptances().is_empty() && actual.permission_uses().is_empty());
        let acceptance = PermissionAcceptance::decode_record(
            produced_text(&raw_before.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        let decision = PermissionDecision::decode_record(
            produced_text(&raw_before.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(
            actual.permission_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.permission_decisions(),
            std::slice::from_ref(&decision)
        );
        assert_eq!(
            actual.creator_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.referenced_grants(),
            std::slice::from_ref(&observed_grant)
        );
        assert!(actual.referenced_acceptances().is_empty());
        assert_eq!(actual.journal_entries(), &journal_before[2..]);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
        Some(actual)
    } else {
        None
    };
    {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = store
            .permission_resource(&execution, &mut control_account)
            .unwrap();
        assert_eq!(actual.thread, before_subject.thread);
        assert_eq!(actual.execution, execution_before);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
    }
    assert!(
        first_claim_raw_nine(&directory.0) == raw_before,
        "healthy controls preserve all nine subject BEFORE tables"
    );
    assert!(store.approval_durable());
    assert!(!store.durable_permissions());
    let target = RunCancellationTarget::from_view(&before_subject).unwrap();
    let expected = transition(
        &execution_before.snapshot,
        KernelCommand::Cancel {
            turn_id: execution_before.snapshot.turn_id.clone(),
            owner_generation: original_owner,
        },
        facts(104),
    )
    .unwrap();
    validate_commit(
        &execution_before.snapshot,
        target.execution_fence,
        &expected,
        original_owner,
    )
    .unwrap();
    let authority_calls = Arc::new(AtomicUsize::new(0));
    let capture_calls = Arc::new(AtomicUsize::new(0));
    let terminal_entries = Arc::new(AtomicUsize::new(0));
    let cancellation_authority = Arc::new(ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_subject.thread.clone(),
            execution: execution_before.clone(),
        },
        expected_target: target.clone(),
        calls: authority_calls.clone(),
    });
    let observed = Arc::new(ReadyCancellationObservedStore {
        inner: store.clone(),
        captures: capture_calls.clone(),
        terminal_entries: terminal_entries.clone(),
    });
    let mut subject_account = PermissionEvidenceBudget::new();
    let initial_usage = produced_usage(&subject_account);
    let cancellation_request = PermissionReadyCancellationRequest {
        context: &context,
        target: &target,
        facts: facts(104),
    };
    let actual_result = cancel_permission_ready(
        observed.as_ref(),
        cancellation_authority.as_ref(),
        catalog.as_ref(),
        &cancellation_request,
        &mut subject_account,
    );
    let final_usage = produced_usage(&subject_account);
    // Capture every actual typed/raw AFTER before inspecting the subject result.
    let after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = first_claim_raw_nine(&directory.0);
    let actual_authority_calls = authority_calls.load(Ordering::SeqCst);
    let actual_capture_calls = capture_calls.load(Ordering::SeqCst);
    let actual_terminal_entries = terminal_entries.load(Ordering::SeqCst);
    drop(subject_account);
    drop(observed);
    drop(cancellation_authority);
    drop(ready);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(application);
    store.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "every live store alias retires normally"
    );
    drop(store);
    let actual_native_calls = calls.load(Ordering::SeqCst);
    assert_eq!(
        actual_native_calls, 0,
        "cancellation and observations invoke no native effect"
    );

    // N+1 independently observes the fully retired original cancellation owner.
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_view = next.thread_view(&thread).unwrap().unwrap();
    let next_execution = next.execution(&execution).unwrap().unwrap();
    let next_journal = next.journal(&execution).unwrap();
    let next_raw = second_admission_raw_nine(&directory.0);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(next_view, after);
    assert_eq!(next_execution, execution_after);
    assert_eq!(next_journal, journal_after);
    assert!(next_raw.tables[1..] == raw_after.tables[1..]);
    assert_eq!(next_raw.tables[0][0][..3], raw_after.tables[0][0][..3]);
    assert_eq!(produced_u64(&next_raw.tables[0][0][3]), next_owner);
    assert_eq!(produced_u64(&raw_after.tables[0][0][3]), original_owner);
    let cancel_code = actual_result
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert_eq!(
        actual_authority_calls, 2,
        "real R1 and renewed R2 cancellation authorization"
    );
    assert_eq!(
        actual_capture_calls, 3,
        "real R1, sensitive C1, and fresh R2 sibling captures"
    );
    assert_eq!(
        actual_terminal_entries, 1,
        "actual opaque Prepared reaches terminal sibling"
    );
    assert_eq!(after.execution.as_ref(), Some(&execution_after));
    assert!(
        initial_usage == (0, 0)
            && final_usage.0 > initial_usage.0
            && final_usage.1 > initial_usage.1
            && final_usage.0 <= 8_388_608
            && final_usage.1 <= 65_536,
        "one fresh original cancellation account is finite and monotone",
    );

    // Cancellation is already qualified setup for this ordinary admission control.
    assert!(
        actual_result.is_ok(),
        "qualified cancellation setup failed: {cancel_code}"
    );
    let returned = actual_result.unwrap();
    assert_eq!(returned, execution_after);
    assert_eq!(execution_after.thread_id, execution_before.thread_id);
    assert_eq!(
        execution_after.snapshot, expected.snapshot,
        "complete actual Kernel Cancel post"
    );
    assert_eq!(execution_after.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(execution_after.snapshot.revision, 4);
    assert_eq!(execution_after.snapshot.owner_generation, original_owner);
    assert_eq!(
        execution_after.snapshot.tasks[0].status,
        TaskStatus::Cancelled
    );
    assert!(execution_after.snapshot.tasks[0].approved);
    assert_eq!(
        execution_after.snapshot.tasks[0].interrupt,
        execution_before.snapshot.tasks[0].interrupt
    );
    assert_eq!(execution_after.snapshot.attempts_used, 0);
    assert!(execution_after.snapshot.attempt_history.is_empty());
    assert!(execution_after.snapshot.tasks[0].attempt_id.is_none());
    assert!(execution_after.snapshot.tasks[0].result.is_none());
    assert!(execution_after.snapshot.tasks[0].failure.is_none());
    let mut expected_thread = before_subject.thread.clone();
    expected_thread.resource_revision = expected_thread.resource_revision.checked_add(1).unwrap();
    assert_eq!(after.thread, expected_thread);
    assert_eq!(journal_after.len(), 5);
    assert_eq!(&journal_after[..4], journal_before.as_slice());
    let cancellation_entry = JournalEntry::from(&expected.snapshot);
    assert_eq!(journal_after[4], cancellation_entry);
    for index in [0, 4, 5, 6, 7, 8] {
        assert!(
            raw_before.tables[index] == raw_after.tables[index],
            "cancellation preserves all original authority and metadata cells"
        );
    }
    assert_eq!(raw_after.tables[1][0][..5], raw_before.tables[1][0][..5]);
    assert_eq!(produced_u64(&raw_after.tables[1][0][5]), 5);
    assert_eq!(raw_after.tables[1][0][6], raw_before.tables[1][0][6]);
    let row = &raw_after.tables[2][0];
    assert_eq!(row[..2], raw_before.tables[2][0][..2]);
    assert_eq!(produced_u64(&row[2]), expected.snapshot.revision);
    assert_eq!(produced_u64(&row[3]), original_owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
        expected.snapshot
    );
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_after.tables[3].len(), 6);
    assert_eq!(&raw_after.tables[3][..5], raw_before.tables[3].as_slice());
    let row = &raw_after.tables[3][5];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&raw_before.tables[3][4][0])
    );
    assert_eq!(produced_text(&row[1]), "commit");
    assert_eq!(produced_text(&row[2]), after.thread.id.as_str());
    assert_eq!(
        produced_text(&row[3]),
        cancellation_entry.execution_id.as_str()
    );
    assert_eq!(produced_u64(&row[4]), cancellation_entry.revision);
    assert_eq!(produced_u64(&row[5]), cancellation_entry.owner_generation);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&cancellation_entry.status).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), cancellation_entry.updated_at_ms);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        cancellation_entry
    );
    assert_eq!(
        next_raw.tables.each_ref().map(Vec::len),
        [1, 1, 1, 6, 1, 0, 1, 1, 0]
    );
    assert_eq!(next_raw.tables[4][0][6], rusqlite::types::Value::Integer(1));
    assert!(next_raw.tables[8].is_empty());
    assert_eq!(
        next_view.thread.active_execution_id.as_ref(),
        Some(&execution)
    );

    let second_execution = ExecutionId::new("permission-second").unwrap();
    let second_turn = TurnId::new("second-turn").unwrap();
    assert!(next.execution(&second_execution).unwrap().is_none());
    assert!(next.journal(&second_execution).unwrap().is_empty());
    let second_initial = new_execution(
        second_execution.clone(),
        second_turn.clone(),
        next_owner,
        definition.clone(),
        execution_before.snapshot.values.clone(),
        facts(105),
    )
    .unwrap();
    assert_eq!(second_initial.status, ExecutionStatus::Accepted);
    assert_eq!(second_initial.revision, 0);
    assert_eq!(second_initial.owner_generation, next_owner);
    assert!(second_initial.tasks.is_empty());
    assert_eq!(second_initial.attempts_used, 0);
    assert!(second_initial.attempt_history.is_empty());
    assert!(second_initial.output.is_none());
    assert!(second_initial.failure.is_none());
    // The real CAS releases the cancelled E1 slot; no prior active-ID rewrite.
    let admission_result = next.admit_fenced(
        &thread,
        &next_view.thread.generation,
        next_view.thread.resource_revision,
        second_initial.clone(),
    );
    // Capture every actual E2 AFTER before deciding whether admission succeeded.
    let admission_view = next.thread_view(&thread).unwrap().unwrap();
    let admission_e1 = next.execution(&execution).unwrap();
    let admission_e2 = next.execution(&second_execution).unwrap();
    let admission_e1_journal = next.journal(&execution).unwrap();
    let admission_e2_journal = next.journal(&second_execution).unwrap();
    let admission_raw = second_admission_raw_nine(&directory.0);
    next.readiness().unwrap();
    drop(next);

    let persisted = SqliteStore::open(&directory.0).unwrap();
    let persisted_owner = persisted.owner_generation();
    let persisted_view = persisted.thread_view(&thread).unwrap().unwrap();
    let persisted_e1 = persisted.execution(&execution).unwrap();
    let persisted_e2 = persisted.execution(&second_execution).unwrap();
    let persisted_e1_journal = persisted.journal(&execution).unwrap();
    let persisted_e2_journal = persisted.journal(&second_execution).unwrap();
    let persisted_raw = second_admission_raw_nine(&directory.0);
    persisted.readiness().unwrap();
    drop(persisted);
    let final_native_calls = calls.load(Ordering::SeqCst);
    drop(calls);
    drop(authority_calls);
    drop(capture_calls);
    drop(terminal_entries);
    let removed = directory.0.clone();
    drop(directory);
    assert!(
        !removed.exists(),
        "Directory is gone before the E2 admission primary"
    );
    assert_eq!(persisted_owner, original_owner.checked_add(2).unwrap());
    assert_eq!(persisted_view, admission_view);
    assert_eq!(persisted_e1, admission_e1);
    assert_eq!(persisted_e2, admission_e2);
    assert_eq!(persisted_e1_journal, admission_e1_journal);
    assert_eq!(persisted_e2_journal, admission_e2_journal);
    assert!(persisted_raw.tables[1..] == admission_raw.tables[1..]);
    assert_eq!(
        persisted_raw.tables[0][0][..3],
        admission_raw.tables[0][0][..3]
    );
    assert_eq!(
        produced_u64(&persisted_raw.tables[0][0][3]),
        persisted_owner
    );
    assert_eq!(produced_u64(&admission_raw.tables[0][0][3]), next_owner);
    assert_eq!(final_native_calls, 0);
    let admission_code = admission_result
        .as_ref()
        .err()
        .map(|e| format!("{e:?}"))
        .unwrap_or_else(|| "ok".into());
    eprintln!("second admission evidence: cancellation={cancel_code}; authorities={actual_authority_calls}; captures={actual_capture_calls}; terminal_entries={actual_terminal_entries}; initial_usage={initial_usage:?}; final_usage={final_usage:?}; admission={admission_code}; owners={original_owner},{next_owner},{persisted_owner}; native={final_native_calls}; directory_removed={}", !removed.exists());
    if admission_result.is_err() {
        assert_eq!(admission_view, next_view);
        assert_eq!(admission_e1.as_ref(), Some(&next_execution));
        assert!(admission_e2.is_none());
        assert_eq!(admission_e1_journal, next_journal);
        assert!(admission_e2_journal.is_empty());
        assert!(
            admission_raw == next_raw,
            "ordinary admission refusal preserves all nine tables"
        );
    }
    // FIRST E2 primary after both owners retired, persistence observed, Directory removed.
    assert!(
        admission_result.is_ok(),
        "cancelled E1 must release the slot for fenced E2 admission; code={admission_code}"
    );
    let admitted_second = admission_result.unwrap();
    assert_eq!(admitted_second.thread_id, thread);
    assert_eq!(admitted_second.snapshot, second_initial);
    assert_eq!(admission_e2.as_ref(), Some(&admitted_second));
    assert_eq!(admission_view.execution.as_ref(), Some(&admitted_second));
    let mut expected_second_thread = next_view.thread.clone();
    expected_second_thread.active_execution_id = Some(second_execution.clone());
    expected_second_thread.resource_revision = expected_second_thread
        .resource_revision
        .checked_add(1)
        .unwrap();
    assert_eq!(admission_view.thread, expected_second_thread);
    assert_eq!(admission_e1.as_ref(), Some(&next_execution));
    assert_eq!(admission_e1_journal, next_journal);
    let second_entry = JournalEntry::from(&second_initial);
    assert_eq!(admission_e2_journal, vec![second_entry.clone()]);
    assert_eq!(
        admission_raw.tables.each_ref().map(Vec::len),
        [1, 1, 2, 7, 1, 0, 1, 1, 0]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert!(
            admission_raw.tables[index] == next_raw.tables[index],
            "ordinary admission preserves all authority and metadata cells"
        );
    }
    let row = &admission_raw.tables[1][0];
    assert_eq!(row[0], next_raw.tables[1][0][0]);
    assert_eq!(produced_text(&row[1]), second_execution.as_str());
    assert_eq!(row[2..5], next_raw.tables[1][0][2..5]);
    assert_eq!(
        produced_u64(&row[5]),
        next_view.thread.resource_revision.checked_add(1).unwrap()
    );
    assert_eq!(row[6], next_raw.tables[1][0][6]);
    assert_eq!(admission_raw.tables[2][0], next_raw.tables[2][0]);
    let row = &admission_raw.tables[2][1];
    assert_eq!(produced_text(&row[0]), second_execution.as_str());
    assert_eq!(produced_text(&row[1]), thread.as_str());
    assert_eq!(produced_u64(&row[2]), 0);
    assert_eq!(produced_u64(&row[3]), next_owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
        second_initial
    );
    assert_eq!(&admission_raw.tables[3][..6], next_raw.tables[3].as_slice());
    let row = &admission_raw.tables[3][6];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&next_raw.tables[3][5][0])
    );
    assert_eq!(produced_text(&row[1]), "admit");
    assert_eq!(produced_text(&row[2]), thread.as_str());
    assert_eq!(produced_text(&row[3]), second_execution.as_str());
    assert_eq!(produced_u64(&row[4]), 0);
    assert_eq!(produced_u64(&row[5]), next_owner);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&ExecutionStatus::Accepted).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), 105);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        second_entry
    );
}
// Draft: exact new port type will be bound to reviewed production scaffold.
struct ReadyUseObservedStore {
    inner: Arc<SqliteStore>,
    resources: Arc<AtomicUsize>,
    captures: Arc<AtomicUsize>,
    terminal_entries: Arc<AtomicUsize>,
    use_entries: Arc<AtomicUsize>,
}
struct ReadyUseAuthority {
    expected_context: ApprovalContext,
    expected_resource: ApprovalResource,
    calls: Arc<AtomicUsize>,
}
impl ApprovalAuthority for ReadyUseAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if context != &self.expected_context
            || !matches!(operation, ApprovalOperation::Dispatch)
            || resource != &self.expected_resource
        {
            return Err(ApprovalError::Unauthorized);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct ReadyUseConstraint {
    expected_context: InvocationContext,
    expected_definition: Definition,
    expected_task: TaskRecord,
    calls: Arc<AtomicUsize>,
}
impl ToolConstraint for ReadyUseConstraint {
    fn validate_call(&self, call: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        if call.context != &self.expected_context
            || call.definition != &self.expected_definition
            || call.task != &self.expected_task
            || !matches!(call.phase, PermissionCallPhase::Unclaimed)
            || call.binding.application.application != "permission-app"
            || call.binding.application.revision != "application-1"
            || call.binding.definition != self.expected_definition
            || call.binding.effect_id != self.expected_task.spec.effect_id
            || call.binding.tool_id.as_str() != "release-tool"
            || call.binding.tool_revision.as_str() != "tool-1"
            || call.binding.constraint_revision.as_str() != "constraint-1"
        {
            return Err(ToolConstraintError::Denied);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl Store for ReadyUseObservedStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit(thread_id, initial)
    }
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(execution_id, fence, plan)
    }
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(execution_id)
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.inner.discover_unfinished(cursor)
    }

    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        self.inner.confinement_view_fenced(target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        self.inner
            .confine_recovery_fenced(original, target, reason, fresh_turn, facts)
    }
}
impl ApprovalStore for ReadyUseObservedStore {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
impl PermissionStore for ReadyUseObservedStore {
    fn durable_permissions(&self) -> bool {
        self.inner.durable_permissions()
    }

    fn ready_authorization_atomic_durable(&self) -> bool {
        self.inner.ready_authorization_atomic_durable()
    }

    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        self.inner.permission_resource(id, budget)
    }

    fn permission_ready_authorization_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        self.resources.fetch_add(1, Ordering::SeqCst);
        self.inner
            .permission_ready_authorization_resource(id, budget)
    }

    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        self.inner.permission_view(id, fence, selector, budget)
    }

    fn commit_permission_acceptance(
        &self,
        prepared: PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<crate::StoredExecution, PermissionError> {
        self.inner.commit_permission_acceptance(prepared, budget)
    }

    fn capture_permission_runtime(
        &self,
        selector: &PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCapture, PermissionError> {
        self.inner.capture_permission_runtime(selector, budget)
    }

    fn commit_permission_runtime(
        &self,
        prepared: &PermissionPreparedRuntimeMutation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCommit, PermissionError> {
        self.inner.commit_permission_runtime(prepared, budget)
    }

    fn capture_permission_ready_cancellation(
        &self,
        selector: &PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCapture, PermissionError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.inner
            .capture_permission_ready_cancellation(selector, budget)
    }
    fn commit_permission_ready_cancellation(
        &self,
        prepared: &PermissionPreparedReadyCancellation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<StoredExecution, PermissionError> {
        self.terminal_entries.fetch_add(1, Ordering::SeqCst);
        self.inner
            .commit_permission_ready_cancellation(prepared, budget)
    }
    fn try_commit_permission_ready_authorization(
        &self,
        intent: &PermissionReadyAuthorizationIntent<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<Option<StoredExecution>, PermissionError> {
        self.use_entries.fetch_add(1, Ordering::SeqCst);
        self.inner
            .try_commit_permission_ready_authorization(intent, budget)
    }
}

fn ready_use_raw_nine(path: &std::path::Path) -> ProducedRawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let raw = ProducedRawNine { tables: [
        produced_raw_cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        produced_raw_cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        produced_raw_cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 3", 5, 2),
        produced_raw_cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 10", 9, 9),
        produced_raw_cells(&connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        produced_raw_cells(&connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] };
    assert!(
        connection.close().is_ok(),
        "read-only produced observer closes normally"
    );
    raw
}
#[test]
fn same_scope_ready_authorization_persists_use_after_reopen_without_new_human() {
    let always = true;
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let registered_matches = registered == definition;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: if always {
                ApprovalDecision::Always
            } else {
                ApprovalDecision::Once
            },
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };

    let producer = if always {
        accept_permission_human(
            store.clone(),
            authority.clone(),
            catalog.clone(),
            &execution,
            fence(store.as_ref(), &thread),
            request.clone(),
        )
    } else {
        service
            .accept(&execution, fence(store.as_ref(), &thread), request.clone())
            .map_err(PermissionError::Approval)
    }
    .unwrap();
    let before_subject = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = first_claim_raw_nine(&directory.0);
    assert!(
        registered_matches,
        "actual native Definition remains registered"
    );
    assert_eq!(before_subject.execution.as_ref(), Some(&producer));
    assert_eq!(execution_before, producer);
    assert_eq!(producer.snapshot.revision, 3);
    assert_eq!(producer.snapshot.status, ExecutionStatus::Running);
    assert_eq!(producer.snapshot.turn_id, request.turn_id);
    assert_eq!(producer.snapshot.owner_generation, original_owner);
    assert_eq!(producer.snapshot.definition, definition);
    assert_eq!(producer.snapshot.attempts_used, 0);
    assert!(producer.snapshot.attempt_history.is_empty());
    assert_eq!(producer.snapshot.tasks.len(), 1);
    assert_eq!(producer.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(producer.snapshot.tasks[0].approved);
    assert!(producer.snapshot.tasks[0].attempt_id.is_none());
    assert!(producer.snapshot.tasks[0].result.is_none());
    assert!(producer.snapshot.tasks[0].failure.is_none());
    assert_eq!(
        producer.snapshot.tasks[0].interrupt,
        before.execution.snapshot.tasks[0].interrupt
    );
    assert_eq!(before_subject.thread.resource_revision, 4);
    assert_eq!(journal_before.len(), 4);
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_before.tables[4].len(), 1);
    let mut expected_grant = before.grants[0].clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(expected_grant.token, pending[0].token);
    assert_eq!(expected_grant.verifier, token_verifier(&pending[0].token));
    assert_eq!(
        expected_grant.proposal_digest,
        proposal_digest(&before.execution.snapshot.tasks[0], &expected_grant.prompt).unwrap()
    );
    expected_grant.consumed = true;
    let observed_grant: GrantRecord =
        serde_json::from_str(produced_text(&raw_before.tables[4][0][7])).unwrap();
    assert_eq!(observed_grant, expected_grant);
    assert!(raw_before.tables[8].is_empty());
    assert_eq!(raw_before.tables[5].len(), usize::from(!always));
    assert_eq!(raw_before.tables[6].len(), usize::from(always));
    assert_eq!(raw_before.tables[7].len(), usize::from(always));
    b4_kernel::validate_snapshot(&producer.snapshot).unwrap();
    let subject_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: before_subject.thread.resource_revision,
    };
    let ready = if always {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            subject_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut control_account,
        )
        .unwrap();
        assert_eq!(actual.thread(), &before_subject.thread);
        assert_eq!(actual.execution(), &execution_before);
        assert_eq!(actual.grants(), std::slice::from_ref(&observed_grant));
        assert!(actual.acceptances().is_empty() && actual.permission_uses().is_empty());
        let acceptance = PermissionAcceptance::decode_record(
            produced_text(&raw_before.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        let decision = PermissionDecision::decode_record(
            produced_text(&raw_before.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(
            actual.permission_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.permission_decisions(),
            std::slice::from_ref(&decision)
        );
        assert_eq!(
            actual.creator_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.referenced_grants(),
            std::slice::from_ref(&observed_grant)
        );
        assert!(actual.referenced_acceptances().is_empty());
        assert_eq!(actual.journal_entries(), &journal_before[2..]);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
        Some(actual)
    } else {
        None
    };
    {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = store
            .permission_resource(&execution, &mut control_account)
            .unwrap();
        assert_eq!(actual.thread, before_subject.thread);
        assert_eq!(actual.execution, execution_before);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
    }
    assert!(
        first_claim_raw_nine(&directory.0) == raw_before,
        "healthy controls preserve all nine subject BEFORE tables"
    );
    assert!(store.approval_durable());
    assert!(!store.durable_permissions());
    let target = RunCancellationTarget::from_view(&before_subject).unwrap();
    let expected = transition(
        &execution_before.snapshot,
        KernelCommand::Cancel {
            turn_id: execution_before.snapshot.turn_id.clone(),
            owner_generation: original_owner,
        },
        facts(104),
    )
    .unwrap();
    validate_commit(
        &execution_before.snapshot,
        target.execution_fence,
        &expected,
        original_owner,
    )
    .unwrap();
    let authority_calls = Arc::new(AtomicUsize::new(0));
    let capture_calls = Arc::new(AtomicUsize::new(0));
    let terminal_entries = Arc::new(AtomicUsize::new(0));
    let cancellation_authority = Arc::new(ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_subject.thread.clone(),
            execution: execution_before.clone(),
        },
        expected_target: target.clone(),
        calls: authority_calls.clone(),
    });
    let observed = Arc::new(ReadyCancellationObservedStore {
        inner: store.clone(),
        captures: capture_calls.clone(),
        terminal_entries: terminal_entries.clone(),
    });
    let mut subject_account = PermissionEvidenceBudget::new();
    let initial_usage = produced_usage(&subject_account);
    let cancellation_request = PermissionReadyCancellationRequest {
        context: &context,
        target: &target,
        facts: facts(104),
    };
    let actual_result = cancel_permission_ready(
        observed.as_ref(),
        cancellation_authority.as_ref(),
        catalog.as_ref(),
        &cancellation_request,
        &mut subject_account,
    );
    let final_usage = produced_usage(&subject_account);
    // Capture every actual typed/raw AFTER before inspecting the subject result.
    let after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = first_claim_raw_nine(&directory.0);
    let actual_authority_calls = authority_calls.load(Ordering::SeqCst);
    let actual_capture_calls = capture_calls.load(Ordering::SeqCst);
    let actual_terminal_entries = terminal_entries.load(Ordering::SeqCst);
    drop(subject_account);
    drop(observed);
    drop(cancellation_authority);
    drop(ready);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(application);
    store.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "every live store alias retires normally"
    );
    drop(store);
    let actual_native_calls = calls.load(Ordering::SeqCst);
    assert_eq!(
        actual_native_calls, 0,
        "cancellation and observations invoke no native effect"
    );

    // N+1 independently observes the fully retired original cancellation owner.
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_view = next.thread_view(&thread).unwrap().unwrap();
    let next_execution = next.execution(&execution).unwrap().unwrap();
    let next_journal = next.journal(&execution).unwrap();
    let next_raw = ready_use_raw_nine(&directory.0);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(next_view, after);
    assert_eq!(next_execution, execution_after);
    assert_eq!(next_journal, journal_after);
    assert!(next_raw.tables[1..] == raw_after.tables[1..]);
    assert_eq!(next_raw.tables[0][0][..3], raw_after.tables[0][0][..3]);
    assert_eq!(produced_u64(&next_raw.tables[0][0][3]), next_owner);
    assert_eq!(produced_u64(&raw_after.tables[0][0][3]), original_owner);
    let cancel_code = actual_result
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert_eq!(
        actual_authority_calls, 2,
        "real R1 and renewed R2 cancellation authorization"
    );
    assert_eq!(
        actual_capture_calls, 3,
        "real R1, sensitive C1, and fresh R2 sibling captures"
    );
    assert_eq!(
        actual_terminal_entries, 1,
        "actual opaque Prepared reaches terminal sibling"
    );
    assert_eq!(after.execution.as_ref(), Some(&execution_after));
    assert!(
        initial_usage == (0, 0)
            && final_usage.0 > initial_usage.0
            && final_usage.1 > initial_usage.1
            && final_usage.0 <= 8_388_608
            && final_usage.1 <= 65_536,
        "one fresh original cancellation account is finite and monotone",
    );

    // Cancellation is already qualified setup for this ordinary admission control.
    assert!(
        actual_result.is_ok(),
        "qualified cancellation setup failed: {cancel_code}"
    );
    let returned = actual_result.unwrap();
    assert_eq!(returned, execution_after);
    assert_eq!(execution_after.thread_id, execution_before.thread_id);
    assert_eq!(
        execution_after.snapshot, expected.snapshot,
        "complete actual Kernel Cancel post"
    );
    assert_eq!(execution_after.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(execution_after.snapshot.revision, 4);
    assert_eq!(execution_after.snapshot.owner_generation, original_owner);
    assert_eq!(
        execution_after.snapshot.tasks[0].status,
        TaskStatus::Cancelled
    );
    assert!(execution_after.snapshot.tasks[0].approved);
    assert_eq!(
        execution_after.snapshot.tasks[0].interrupt,
        execution_before.snapshot.tasks[0].interrupt
    );
    assert_eq!(execution_after.snapshot.attempts_used, 0);
    assert!(execution_after.snapshot.attempt_history.is_empty());
    assert!(execution_after.snapshot.tasks[0].attempt_id.is_none());
    assert!(execution_after.snapshot.tasks[0].result.is_none());
    assert!(execution_after.snapshot.tasks[0].failure.is_none());
    let mut expected_thread = before_subject.thread.clone();
    expected_thread.resource_revision = expected_thread.resource_revision.checked_add(1).unwrap();
    assert_eq!(after.thread, expected_thread);
    assert_eq!(journal_after.len(), 5);
    assert_eq!(&journal_after[..4], journal_before.as_slice());
    let cancellation_entry = JournalEntry::from(&expected.snapshot);
    assert_eq!(journal_after[4], cancellation_entry);
    for index in [0, 4, 5, 6, 7, 8] {
        assert!(
            raw_before.tables[index] == raw_after.tables[index],
            "cancellation preserves all original authority and metadata cells"
        );
    }
    assert_eq!(raw_after.tables[1][0][..5], raw_before.tables[1][0][..5]);
    assert_eq!(produced_u64(&raw_after.tables[1][0][5]), 5);
    assert_eq!(raw_after.tables[1][0][6], raw_before.tables[1][0][6]);
    let row = &raw_after.tables[2][0];
    assert_eq!(row[..2], raw_before.tables[2][0][..2]);
    assert_eq!(produced_u64(&row[2]), expected.snapshot.revision);
    assert_eq!(produced_u64(&row[3]), original_owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
        expected.snapshot
    );
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_after.tables[3].len(), 6);
    assert_eq!(&raw_after.tables[3][..5], raw_before.tables[3].as_slice());
    let row = &raw_after.tables[3][5];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&raw_before.tables[3][4][0])
    );
    assert_eq!(produced_text(&row[1]), "commit");
    assert_eq!(produced_text(&row[2]), after.thread.id.as_str());
    assert_eq!(
        produced_text(&row[3]),
        cancellation_entry.execution_id.as_str()
    );
    assert_eq!(produced_u64(&row[4]), cancellation_entry.revision);
    assert_eq!(produced_u64(&row[5]), cancellation_entry.owner_generation);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&cancellation_entry.status).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), cancellation_entry.updated_at_ms);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        cancellation_entry
    );
    assert_eq!(
        next_raw.tables.each_ref().map(Vec::len),
        [1, 1, 1, 6, 1, 0, 1, 1, 0]
    );
    assert_eq!(next_raw.tables[4][0][6], rusqlite::types::Value::Integer(1));
    assert!(next_raw.tables[8].is_empty());
    assert_eq!(
        next_view.thread.active_execution_id.as_ref(),
        Some(&execution)
    );

    let second_execution = ExecutionId::new("permission-second").unwrap();
    let second_turn = TurnId::new("second-turn").unwrap();
    let changed_values = Map::from_iter([("input".into(), json!({"release": "second fixture"}))]);
    assert_ne!(changed_values, execution_before.snapshot.values);
    assert!(next.execution(&second_execution).unwrap().is_none());
    let second_initial = new_execution(
        second_execution.clone(),
        second_turn.clone(),
        next_owner,
        definition.clone(),
        changed_values.clone(),
        facts(105),
    )
    .unwrap();
    let admitted_e2 = next
        .admit_fenced(
            &thread,
            &next_view.thread.generation,
            next_view.thread.resource_revision,
            second_initial.clone(),
        )
        .unwrap();
    assert_eq!(admitted_e2.snapshot, second_initial);
    let admission_view = next.thread_view(&thread).unwrap().unwrap();
    let admission_raw = ready_use_raw_nine(&directory.0);
    assert_eq!(
        admission_view.thread.active_execution_id.as_ref(),
        Some(&second_execution)
    );
    assert_eq!(admission_view.thread.resource_revision, 6);
    assert_eq!(
        admission_raw.tables.each_ref().map(Vec::len),
        [1, 1, 2, 7, 1, 0, 1, 1, 0]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert_eq!(admission_raw.tables[index], next_raw.tables[index]);
    }
    assert_eq!(admission_raw.tables[2][0], next_raw.tables[2][0]);
    assert_eq!(&admission_raw.tables[3][..6], next_raw.tables[3].as_slice());

    // The ordinary Start is legal setup. No raw authorization plan is committed.
    let started_e2 = next
        .commit(
            &second_execution,
            (&admitted_e2.snapshot).into(),
            transition(&admitted_e2.snapshot, KernelCommand::Start, facts(106)).unwrap(),
        )
        .unwrap();
    let before_use = next.thread_view(&thread).unwrap().unwrap();
    let e1_before_use = next.execution(&execution).unwrap();
    let e2_before_use = next.execution(&second_execution).unwrap();
    let e1_journal_before_use = next.journal(&execution).unwrap();
    let e2_journal_before_use = next.journal(&second_execution).unwrap();
    let raw_before_use = ready_use_raw_nine(&directory.0);
    assert_eq!(before_use.execution.as_ref(), Some(&started_e2));
    assert_eq!(e2_before_use.as_ref(), Some(&started_e2));
    assert_eq!(e1_before_use.as_ref(), Some(&next_execution));
    assert_eq!(e1_journal_before_use, next_journal);
    assert_eq!(before_use.thread.resource_revision, 7);
    assert_eq!(started_e2.snapshot.execution_id, second_execution);
    assert_eq!(started_e2.snapshot.turn_id, second_turn);
    assert_eq!(started_e2.snapshot.owner_generation, next_owner);
    assert_eq!(started_e2.snapshot.definition, definition);
    assert_eq!(started_e2.snapshot.values, changed_values);
    assert_eq!(started_e2.snapshot.status, ExecutionStatus::Running);
    assert_eq!(started_e2.snapshot.revision, 1);
    assert_eq!(started_e2.snapshot.attempts_used, 0);
    assert!(started_e2.snapshot.attempt_history.is_empty());
    assert!(started_e2.snapshot.output.is_none());
    assert!(started_e2.snapshot.failure.is_none());
    assert_eq!(started_e2.snapshot.tasks.len(), 1);
    let ready_task = &started_e2.snapshot.tasks[0];
    assert_eq!(ready_task.spec, definition.batches[0][0]);
    assert_eq!(ready_task.input, changed_values);
    assert_eq!(ready_task.status, TaskStatus::Ready);
    assert!(!ready_task.approved);
    assert!(ready_task.interrupt.is_none());
    assert!(ready_task.attempt_id.is_none());
    assert!(ready_task.result.is_none());
    assert!(ready_task.failure.is_none());
    b4_kernel::validate_snapshot(&started_e2.snapshot).unwrap();
    assert_eq!(e2_journal_before_use.len(), 2);
    assert_eq!(
        e2_journal_before_use[0],
        JournalEntry::from(&second_initial)
    );
    assert_eq!(
        e2_journal_before_use[1],
        JournalEntry::from(&started_e2.snapshot)
    );
    assert_eq!(
        raw_before_use.tables.each_ref().map(Vec::len),
        [1, 1, 2, 8, 1, 0, 1, 1, 0]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert_eq!(raw_before_use.tables[index], admission_raw.tables[index]);
    }
    assert_eq!(raw_before_use.tables[2][0], admission_raw.tables[2][0]);
    assert_eq!(
        &raw_before_use.tables[3][..7],
        admission_raw.tables[3].as_slice()
    );

    let mut setup_control_account = PermissionEvidenceBudget::new();
    let original_decision = PermissionDecision::decode_record(
        produced_text(&raw_before_use.tables[7][0][5]).as_bytes(),
        &mut setup_control_account,
    )
    .unwrap();
    assert_eq!(original_decision.record_version(), 1);
    assert_eq!(original_decision.creator_execution_id(), &execution);
    assert_eq!(original_decision.creator_turn_id(), &request.turn_id);
    assert_eq!(original_decision.creator_task_id(), &task);
    assert_eq!(
        original_decision.scope_digest(),
        original_decision
            .scope()
            .canonical_digest(&mut setup_control_account)
            .unwrap()
    );
    let original_scope = original_decision.scope();
    assert_eq!(original_scope.thread_id(), &before_use.thread.id);
    assert_eq!(
        original_scope.thread_generation(),
        &before_use.thread.generation
    );
    assert_eq!(original_scope.origin(), &context.actor);
    assert_eq!(original_scope.application().application(), "permission-app");
    assert_eq!(original_scope.application().revision(), "application-1");
    assert_eq!(original_scope.route(), &definition.route);
    assert_eq!(original_scope.definition(), &definition);
    assert_eq!(original_scope.effect_id(), &ready_task.spec.effect_id);
    assert_eq!(original_scope.tool_id().as_str(), "release-tool");
    assert_eq!(original_scope.tool_revision().as_str(), "tool-1");
    assert_eq!(original_scope.policy_revision(), context.policy_revision);
    assert_eq!(
        original_scope.constraint_revision().as_str(),
        "constraint-1"
    );
    let expected_call_digest =
        permission_call_digest(ready_task, &mut setup_control_account).unwrap();
    let original_call_digest = permission_call_digest(
        &execution_before.snapshot.tasks[0],
        &mut setup_control_account,
    )
    .unwrap();
    assert_ne!(
        expected_call_digest, original_call_digest,
        "changed allowed input has a different actual call witness"
    );
    drop(setup_control_account);

    // Fresh native/catalog objects follow complete original-owner retirement.
    let use_resource_reads = Arc::new(AtomicUsize::new(0));
    let new_authority_calls = Arc::new(AtomicUsize::new(0));
    let new_gate_calls = Arc::new(AtomicUsize::new(0));
    let use_entries = Arc::new(AtomicUsize::new(0));
    let unused_runtime_captures = Arc::new(AtomicUsize::new(0));
    let unused_cancellation_entries = Arc::new(AtomicUsize::new(0));
    let fresh_application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let expected_invocation = InvocationContext {
        thread_id: before_use.thread.id.clone(),
        thread_generation: before_use.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: "application-1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let fresh_catalog = Arc::new(
        PermissionCatalog::new(
            fresh_application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: Arc::new(ReadyUseConstraint {
                    expected_context: expected_invocation,
                    expected_definition: definition.clone(),
                    expected_task: ready_task.clone(),
                    calls: new_gate_calls.clone(),
                }),
            }],
        )
        .unwrap(),
    );
    assert_eq!(
        fresh_application.definition(&definition.route),
        Some(&definition)
    );
    let use_authority = ReadyUseAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_use.thread.clone(),
            execution: started_e2.clone(),
        },
        calls: new_authority_calls.clone(),
    };
    // Command capture is caller-owned fixture setup, outside the service account.
    // The service only borrows this command; it cannot claim a retroactive clone payment.
    let use_id = PermissionUseId::new(
        "b4pu_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    )
    .unwrap();
    let command = b4_kernel::PermissionAuthorizationCommand::capture(
        &started_e2.snapshot,
        task.clone(),
        use_id.clone(),
    )
    .unwrap();
    let expected_authorization =
        b4_kernel::authorize_ready(&started_e2.snapshot, &command, facts(107)).unwrap();
    let use_fence = ApprovalFence {
        execution: (&started_e2.snapshot).into(),
        resource_revision: before_use.thread.resource_revision,
    };
    let next = Arc::new(next);
    let observed_use = ReadyUseObservedStore {
        inner: next.clone(),
        resources: use_resource_reads.clone(),
        captures: unused_runtime_captures.clone(),
        terminal_entries: unused_cancellation_entries.clone(),
        use_entries: use_entries.clone(),
    };
    let mut use_account = PermissionEvidenceBudget::new();
    let initial_use_usage = produced_usage(&use_account);
    let use_result = {
        let use_request = PermissionReadyAuthorizationRequest {
            context: &context,
            command: &command,
            facts: facts(107),
        };
        try_authorize_permission_ready(
            &observed_use,
            &use_authority,
            fresh_catalog.as_ref(),
            use_fence,
            &use_request,
            &mut use_account,
        )
    };
    let final_use_usage = produced_usage(&use_account);
    // Every actual AFTER is captured before unwrapping or selecting the result.
    let after_use = next.thread_view(&thread).unwrap().unwrap();
    let e1_after_use = next.execution(&execution).unwrap();
    let e2_after_use = next.execution(&second_execution).unwrap();
    let e1_journal_after_use = next.journal(&execution).unwrap();
    let e2_journal_after_use = next.journal(&second_execution).unwrap();
    let raw_after_use = ready_use_raw_nine(&directory.0);
    let actual_use_resource_reads = use_resource_reads.load(Ordering::SeqCst);
    let actual_use_authority_calls = new_authority_calls.load(Ordering::SeqCst);
    let actual_use_gate_calls = new_gate_calls.load(Ordering::SeqCst);
    let actual_use_entries = use_entries.load(Ordering::SeqCst);
    let actual_unused_captures = unused_runtime_captures.load(Ordering::SeqCst);
    let actual_unused_cancellation_entries = unused_cancellation_entries.load(Ordering::SeqCst);
    drop(use_account);
    drop(observed_use);
    drop(use_authority);
    drop(fresh_catalog);
    drop(fresh_application);
    drop(command);
    next.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&next),
        1,
        "all new store aliases retire normally"
    );
    drop(next);
    let persisted = SqliteStore::open(&directory.0).unwrap();
    let persisted_owner = persisted.owner_generation();
    let persisted_view = persisted.thread_view(&thread).unwrap().unwrap();
    let persisted_e1 = persisted.execution(&execution).unwrap();
    let persisted_e2 = persisted.execution(&second_execution).unwrap();
    let persisted_e1_journal = persisted.journal(&execution).unwrap();
    let persisted_e2_journal = persisted.journal(&second_execution).unwrap();
    let persisted_raw = ready_use_raw_nine(&directory.0);
    persisted.readiness().unwrap();
    drop(persisted);
    let final_native_calls = calls.load(Ordering::SeqCst);
    drop(calls);
    drop(authority_calls);
    drop(capture_calls);
    drop(terminal_entries);
    drop(use_resource_reads);
    drop(new_authority_calls);
    drop(new_gate_calls);
    drop(use_entries);
    drop(unused_runtime_captures);
    drop(unused_cancellation_entries);
    let removed = directory.0.clone();
    drop(directory);
    assert!(
        !removed.exists(),
        "Directory is gone before the automatic-use primary"
    );
    assert_eq!(persisted_owner, original_owner.checked_add(2).unwrap());
    assert_eq!(persisted_view, after_use);
    assert_eq!(persisted_e1, e1_after_use);
    assert_eq!(persisted_e2, e2_after_use);
    assert_eq!(persisted_e1_journal, e1_journal_after_use);
    assert_eq!(persisted_e2_journal, e2_journal_after_use);
    assert_eq!(persisted_raw.tables[1..], raw_after_use.tables[1..]);
    assert_eq!(
        persisted_raw.tables[0][0][..3],
        raw_after_use.tables[0][0][..3]
    );
    assert_eq!(
        produced_u64(&persisted_raw.tables[0][0][3]),
        persisted_owner
    );
    assert_eq!(produced_u64(&raw_after_use.tables[0][0][3]), next_owner);
    assert_eq!(final_native_calls, 0);
    assert!(
        actual_use_resource_reads >= 1,
        "the new bounded resource port was reached"
    );
    assert!(
        actual_use_authority_calls >= 1,
        "real Dispatch authority was reached"
    );
    assert!(
        actual_use_gate_calls >= 1,
        "the current allowed changed-input gate was reached"
    );
    assert_eq!(
        actual_use_entries, 1,
        "the actual new typed store seam was reached once"
    );
    assert_eq!(actual_unused_captures, 0);
    assert_eq!(actual_unused_cancellation_entries, 0);
    assert_eq!(initial_use_usage, (0, 0));
    assert!(
        final_use_usage.0 > 0
            && final_use_usage.1 > 0
            && final_use_usage.0 <= 8_388_608
            && final_use_usage.1 <= 65_536
    );
    let use_code = use_result
        .as_ref()
        .err()
        .map(|e| format!("{e}"))
        .unwrap_or_else(|| "ok".into());
    eprintln!("ready use evidence: resources={actual_use_resource_reads}; authority={actual_use_authority_calls}; gate={actual_use_gate_calls}; seam={actual_use_entries}; code={use_code}; initial_usage={initial_use_usage:?}; final_usage={final_use_usage:?}; owners={original_owner},{next_owner},{persisted_owner}; native={final_native_calls}; directory_removed={}", !removed.exists());
    if !matches!(&use_result, Ok(Some(_))) {
        if use_result.is_err() {
            assert!(matches!(&use_result, Err(PermissionError::UnsupportedProfile)), "earlier errors are setup failures, not the reached default missing-use refusal; code={use_code}");
            assert_eq!(
                actual_use_resource_reads, 1,
                "the default scaffold reaches one new resource read"
            );
            assert_eq!(
                actual_use_authority_calls, 1,
                "the default scaffold reaches one authority call"
            );
            assert_eq!(
                actual_use_gate_calls, 1,
                "the default scaffold reaches one current call gate"
            );
        }
        assert_eq!(after_use, before_use);
        assert_eq!(e1_after_use, e1_before_use);
        assert_eq!(e2_after_use, e2_before_use);
        assert_eq!(e1_journal_after_use, e1_journal_before_use);
        assert_eq!(e2_journal_after_use, e2_journal_before_use);
        assert!(
            raw_after_use == raw_before_use,
            "refusal/miss preserves all nine actual groups"
        );
        assert!(raw_after_use.tables[8].is_empty());
    }
    // FIRST automatic-use primary: after actual captures, both owners and Directory retirement.
    assert!(matches!(&use_result, Ok(Some(_))), "same-scope Ready authorization must persist one real Use without new human approval; code={use_code}");
    let returned = use_result.unwrap().unwrap();
    assert_eq!(e2_after_use.as_ref(), Some(&returned));
    assert_eq!(after_use.execution.as_ref(), Some(&returned));
    assert_eq!(returned.snapshot, expected_authorization.snapshot);
    assert_eq!(returned.thread_id, started_e2.thread_id);
    assert_eq!(returned.snapshot.status, ExecutionStatus::Running);
    assert_eq!(returned.snapshot.revision, 2);
    assert_eq!(returned.snapshot.owner_generation, next_owner);
    assert_eq!(returned.snapshot.turn_id, second_turn);
    assert_eq!(returned.snapshot.values, changed_values);
    assert_eq!(returned.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(returned.snapshot.tasks[0].approved);
    assert_eq!(
        returned.snapshot.tasks[0].interrupt,
        Some(PendingApproval {
            id: InterruptId::new(use_id.as_str()).unwrap(),
            prompt: serde_json::Value::Null
        })
    );
    assert_eq!(returned.snapshot.attempts_used, 0);
    assert!(returned.snapshot.attempt_history.is_empty());
    assert!(returned.snapshot.tasks[0].attempt_id.is_none());
    assert!(returned.snapshot.tasks[0].result.is_none());
    assert!(returned.snapshot.tasks[0].failure.is_none());
    let mut expected_thread = before_use.thread.clone();
    expected_thread.resource_revision = expected_thread.resource_revision.checked_add(1).unwrap();
    assert_eq!(after_use.thread, expected_thread);
    assert_eq!(e1_after_use, e1_before_use);
    assert_eq!(e1_journal_after_use, e1_journal_before_use);
    assert_eq!(e2_journal_after_use.len(), 3);
    assert_eq!(&e2_journal_after_use[..2], e2_journal_before_use.as_slice());
    assert_eq!(
        e2_journal_after_use[2],
        JournalEntry::from(&returned.snapshot)
    );
    assert_eq!(
        raw_after_use.tables.each_ref().map(Vec::len),
        [1, 1, 2, 9, 1, 0, 1, 1, 1]
    );
    for index in [0, 4, 5, 6, 7] {
        assert_eq!(raw_after_use.tables[index], raw_before_use.tables[index]);
    }
    assert_eq!(raw_after_use.tables[2][0], raw_before_use.tables[2][0]);
    assert_eq!(
        &raw_after_use.tables[3][..8],
        raw_before_use.tables[3].as_slice()
    );
    assert_eq!(
        raw_after_use.tables[1][0][..5],
        raw_before_use.tables[1][0][..5]
    );
    assert_eq!(produced_u64(&raw_after_use.tables[1][0][5]), 8);
    assert_eq!(
        raw_after_use.tables[1][0][6],
        raw_before_use.tables[1][0][6]
    );
    let raw_e2 = &raw_after_use.tables[2][1];
    assert_eq!(raw_e2[..2], raw_before_use.tables[2][1][..2]);
    assert_eq!(produced_text(&raw_e2[0]), second_execution.as_str());
    assert_eq!(produced_u64(&raw_e2[2]), returned.snapshot.revision);
    assert_eq!(produced_u64(&raw_e2[3]), next_owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&raw_e2[4])).unwrap(),
        returned.snapshot
    );
    let new_journal = &raw_after_use.tables[3][8];
    let authorization_entry = JournalEntry::from(&returned.snapshot);
    assert!(
        first_claim_journal_sequence(&new_journal[0])
            > first_claim_journal_sequence(&raw_before_use.tables[3][7][0])
    );
    assert_eq!(produced_text(&new_journal[1]), "commit");
    assert_eq!(
        produced_text(&new_journal[2]),
        before_use.thread.id.as_str()
    );
    assert_eq!(produced_text(&new_journal[3]), second_execution.as_str());
    assert_eq!(produced_u64(&new_journal[4]), authorization_entry.revision);
    assert_eq!(produced_u64(&new_journal[5]), next_owner);
    assert_eq!(
        produced_text(&new_journal[6]),
        serde_json::to_string(&authorization_entry.status).unwrap()
    );
    assert_eq!(produced_u64(&new_journal[7]), 107);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&new_journal[8])).unwrap(),
        authorization_entry
    );
    let raw_use = &raw_after_use.tables[8][0];
    assert_eq!(produced_text(&raw_use[0]), second_execution.as_str());
    assert_eq!(produced_text(&raw_use[1]), task.as_str());
    assert_eq!(produced_text(&raw_use[2]), use_id.as_str());
    assert_eq!(produced_text(&raw_use[3]), before_use.thread.id.as_str());
    assert_eq!(
        produced_text(&raw_use[4]),
        before_use.thread.generation.as_str()
    );
    assert_eq!(
        produced_text(&raw_use[5]),
        original_decision.permission_id().as_str()
    );
    assert_eq!(produced_text(&raw_use[6]), second_turn.as_str());
    assert_eq!(produced_u64(&raw_use[7]), 2);
    assert_eq!(raw_use[8], rusqlite::types::Value::Integer(1));
    let mut use_control_account = PermissionEvidenceBudget::new();
    let actual_use = PermissionUse::decode_record(
        produced_text(&raw_use[9]).as_bytes(),
        &mut use_control_account,
    )
    .unwrap();
    assert_eq!(actual_use.record_version(), 1);
    assert_eq!(
        actual_use.permission_version(),
        original_decision.record_version()
    );
    assert_eq!(actual_use.use_id(), &use_id);
    assert_eq!(
        actual_use.permission_id(),
        original_decision.permission_id()
    );
    assert_eq!(actual_use.thread_id(), &before_use.thread.id);
    assert_eq!(
        actual_use.thread_generation(),
        &before_use.thread.generation
    );
    assert_eq!(actual_use.execution_id(), &second_execution);
    assert_eq!(actual_use.task_id(), &task);
    assert_eq!(actual_use.before_snapshot(), &started_e2.snapshot);
    assert_eq!(actual_use.facts(), &facts(107));
    assert_eq!(actual_use.committed_revision(), returned.snapshot.revision);
    assert_eq!(
        Some(actual_use.marker()),
        returned.snapshot.tasks[0].interrupt.as_ref()
    );
    assert_eq!(actual_use.call_digest(), expected_call_digest);
    assert_eq!(actual_use.application(), original_scope.application());
    assert_eq!(actual_use.tool_id(), original_scope.tool_id());
    assert_eq!(actual_use.tool_revision(), original_scope.tool_revision());
    assert_eq!(
        actual_use.policy_revision(),
        original_scope.policy_revision()
    );
    assert_eq!(
        actual_use.constraint_revision(),
        original_scope.constraint_revision()
    );
    drop(use_control_account);
}

// Independent controls retain the real legal Always producer and normal cleanup.
#[derive(Clone, Copy)]
enum ReadyUseFault {
    JournalAbort,
    SecondDecisionHeader,
}

fn ready_use_fault_raw_nine(path: &std::path::Path) -> ProducedRawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let raw = ProducedRawNine { tables: [
        produced_raw_cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        produced_raw_cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        produced_raw_cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 3", 5, 2),
        produced_raw_cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 10", 9, 9),
        produced_raw_cells(&connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 3", 6, 2),
        produced_raw_cells(&connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] };
    assert!(
        connection.close().is_ok(),
        "read-only produced observer closes normally"
    );
    raw
}
fn capture_ready_use_fault(fault: ReadyUseFault) {
    let always = true;
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
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
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let registered_matches = registered == definition;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: if always {
                ApprovalDecision::Always
            } else {
                ApprovalDecision::Once
            },
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };

    let producer = if always {
        accept_permission_human(
            store.clone(),
            authority.clone(),
            catalog.clone(),
            &execution,
            fence(store.as_ref(), &thread),
            request.clone(),
        )
    } else {
        service
            .accept(&execution, fence(store.as_ref(), &thread), request.clone())
            .map_err(PermissionError::Approval)
    }
    .unwrap();
    let before_subject = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = first_claim_raw_nine(&directory.0);
    assert!(
        registered_matches,
        "actual native Definition remains registered"
    );
    assert_eq!(before_subject.execution.as_ref(), Some(&producer));
    assert_eq!(execution_before, producer);
    assert_eq!(producer.snapshot.revision, 3);
    assert_eq!(producer.snapshot.status, ExecutionStatus::Running);
    assert_eq!(producer.snapshot.turn_id, request.turn_id);
    assert_eq!(producer.snapshot.owner_generation, original_owner);
    assert_eq!(producer.snapshot.definition, definition);
    assert_eq!(producer.snapshot.attempts_used, 0);
    assert!(producer.snapshot.attempt_history.is_empty());
    assert_eq!(producer.snapshot.tasks.len(), 1);
    assert_eq!(producer.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(producer.snapshot.tasks[0].approved);
    assert!(producer.snapshot.tasks[0].attempt_id.is_none());
    assert!(producer.snapshot.tasks[0].result.is_none());
    assert!(producer.snapshot.tasks[0].failure.is_none());
    assert_eq!(
        producer.snapshot.tasks[0].interrupt,
        before.execution.snapshot.tasks[0].interrupt
    );
    assert_eq!(before_subject.thread.resource_revision, 4);
    assert_eq!(journal_before.len(), 4);
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_before.tables[4].len(), 1);
    let mut expected_grant = before.grants[0].clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(expected_grant.token, pending[0].token);
    assert_eq!(expected_grant.verifier, token_verifier(&pending[0].token));
    assert_eq!(
        expected_grant.proposal_digest,
        proposal_digest(&before.execution.snapshot.tasks[0], &expected_grant.prompt).unwrap()
    );
    expected_grant.consumed = true;
    let observed_grant: GrantRecord =
        serde_json::from_str(produced_text(&raw_before.tables[4][0][7])).unwrap();
    assert_eq!(observed_grant, expected_grant);
    assert!(raw_before.tables[8].is_empty());
    assert_eq!(raw_before.tables[5].len(), usize::from(!always));
    assert_eq!(raw_before.tables[6].len(), usize::from(always));
    assert_eq!(raw_before.tables[7].len(), usize::from(always));
    b4_kernel::validate_snapshot(&producer.snapshot).unwrap();
    let subject_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: before_subject.thread.resource_revision,
    };
    let ready = if always {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            subject_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut control_account,
        )
        .unwrap();
        assert_eq!(actual.thread(), &before_subject.thread);
        assert_eq!(actual.execution(), &execution_before);
        assert_eq!(actual.grants(), std::slice::from_ref(&observed_grant));
        assert!(actual.acceptances().is_empty() && actual.permission_uses().is_empty());
        let acceptance = PermissionAcceptance::decode_record(
            produced_text(&raw_before.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        let decision = PermissionDecision::decode_record(
            produced_text(&raw_before.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(
            actual.permission_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.permission_decisions(),
            std::slice::from_ref(&decision)
        );
        assert_eq!(
            actual.creator_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.referenced_grants(),
            std::slice::from_ref(&observed_grant)
        );
        assert!(actual.referenced_acceptances().is_empty());
        assert_eq!(actual.journal_entries(), &journal_before[2..]);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
        Some(actual)
    } else {
        None
    };
    {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = store
            .permission_resource(&execution, &mut control_account)
            .unwrap();
        assert_eq!(actual.thread, before_subject.thread);
        assert_eq!(actual.execution, execution_before);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
    }
    assert!(
        first_claim_raw_nine(&directory.0) == raw_before,
        "healthy controls preserve all nine subject BEFORE tables"
    );
    assert!(store.approval_durable());
    assert!(!store.durable_permissions());
    let target = RunCancellationTarget::from_view(&before_subject).unwrap();
    let expected = transition(
        &execution_before.snapshot,
        KernelCommand::Cancel {
            turn_id: execution_before.snapshot.turn_id.clone(),
            owner_generation: original_owner,
        },
        facts(104),
    )
    .unwrap();
    validate_commit(
        &execution_before.snapshot,
        target.execution_fence,
        &expected,
        original_owner,
    )
    .unwrap();
    let authority_calls = Arc::new(AtomicUsize::new(0));
    let capture_calls = Arc::new(AtomicUsize::new(0));
    let terminal_entries = Arc::new(AtomicUsize::new(0));
    let cancellation_authority = Arc::new(ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_subject.thread.clone(),
            execution: execution_before.clone(),
        },
        expected_target: target.clone(),
        calls: authority_calls.clone(),
    });
    let observed = Arc::new(ReadyCancellationObservedStore {
        inner: store.clone(),
        captures: capture_calls.clone(),
        terminal_entries: terminal_entries.clone(),
    });
    let mut subject_account = PermissionEvidenceBudget::new();
    let initial_usage = produced_usage(&subject_account);
    let cancellation_request = PermissionReadyCancellationRequest {
        context: &context,
        target: &target,
        facts: facts(104),
    };
    let actual_result = cancel_permission_ready(
        observed.as_ref(),
        cancellation_authority.as_ref(),
        catalog.as_ref(),
        &cancellation_request,
        &mut subject_account,
    );
    let final_usage = produced_usage(&subject_account);
    // Capture every actual typed/raw AFTER before inspecting the subject result.
    let after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = first_claim_raw_nine(&directory.0);
    let actual_authority_calls = authority_calls.load(Ordering::SeqCst);
    let actual_capture_calls = capture_calls.load(Ordering::SeqCst);
    let actual_terminal_entries = terminal_entries.load(Ordering::SeqCst);
    drop(subject_account);
    drop(observed);
    drop(cancellation_authority);
    drop(ready);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(application);
    store.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "every live store alias retires normally"
    );
    drop(store);
    let actual_native_calls = calls.load(Ordering::SeqCst);
    assert_eq!(
        actual_native_calls, 0,
        "cancellation and observations invoke no native effect"
    );

    // N+1 independently observes the fully retired original cancellation owner.
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_view = next.thread_view(&thread).unwrap().unwrap();
    let next_execution = next.execution(&execution).unwrap().unwrap();
    let next_journal = next.journal(&execution).unwrap();
    let next_raw = ready_use_raw_nine(&directory.0);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(next_view, after);
    assert_eq!(next_execution, execution_after);
    assert_eq!(next_journal, journal_after);
    assert!(next_raw.tables[1..] == raw_after.tables[1..]);
    assert_eq!(next_raw.tables[0][0][..3], raw_after.tables[0][0][..3]);
    assert_eq!(produced_u64(&next_raw.tables[0][0][3]), next_owner);
    assert_eq!(produced_u64(&raw_after.tables[0][0][3]), original_owner);
    let cancel_code = actual_result
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert_eq!(
        actual_authority_calls, 2,
        "real R1 and renewed R2 cancellation authorization"
    );
    assert_eq!(
        actual_capture_calls, 3,
        "real R1, sensitive C1, and fresh R2 sibling captures"
    );
    assert_eq!(
        actual_terminal_entries, 1,
        "actual opaque Prepared reaches terminal sibling"
    );
    assert_eq!(after.execution.as_ref(), Some(&execution_after));
    assert!(
        initial_usage == (0, 0)
            && final_usage.0 > initial_usage.0
            && final_usage.1 > initial_usage.1
            && final_usage.0 <= 8_388_608
            && final_usage.1 <= 65_536,
        "one fresh original cancellation account is finite and monotone",
    );

    // Cancellation is already qualified setup for this ordinary admission control.
    assert!(
        actual_result.is_ok(),
        "qualified cancellation setup failed: {cancel_code}"
    );
    let returned = actual_result.unwrap();
    assert_eq!(returned, execution_after);
    assert_eq!(execution_after.thread_id, execution_before.thread_id);
    assert_eq!(
        execution_after.snapshot, expected.snapshot,
        "complete actual Kernel Cancel post"
    );
    assert_eq!(execution_after.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(execution_after.snapshot.revision, 4);
    assert_eq!(execution_after.snapshot.owner_generation, original_owner);
    assert_eq!(
        execution_after.snapshot.tasks[0].status,
        TaskStatus::Cancelled
    );
    assert!(execution_after.snapshot.tasks[0].approved);
    assert_eq!(
        execution_after.snapshot.tasks[0].interrupt,
        execution_before.snapshot.tasks[0].interrupt
    );
    assert_eq!(execution_after.snapshot.attempts_used, 0);
    assert!(execution_after.snapshot.attempt_history.is_empty());
    assert!(execution_after.snapshot.tasks[0].attempt_id.is_none());
    assert!(execution_after.snapshot.tasks[0].result.is_none());
    assert!(execution_after.snapshot.tasks[0].failure.is_none());
    let mut expected_thread = before_subject.thread.clone();
    expected_thread.resource_revision = expected_thread.resource_revision.checked_add(1).unwrap();
    assert_eq!(after.thread, expected_thread);
    assert_eq!(journal_after.len(), 5);
    assert_eq!(&journal_after[..4], journal_before.as_slice());
    let cancellation_entry = JournalEntry::from(&expected.snapshot);
    assert_eq!(journal_after[4], cancellation_entry);
    for index in [0, 4, 5, 6, 7, 8] {
        assert!(
            raw_before.tables[index] == raw_after.tables[index],
            "cancellation preserves all original authority and metadata cells"
        );
    }
    assert_eq!(raw_after.tables[1][0][..5], raw_before.tables[1][0][..5]);
    assert_eq!(produced_u64(&raw_after.tables[1][0][5]), 5);
    assert_eq!(raw_after.tables[1][0][6], raw_before.tables[1][0][6]);
    let row = &raw_after.tables[2][0];
    assert_eq!(row[..2], raw_before.tables[2][0][..2]);
    assert_eq!(produced_u64(&row[2]), expected.snapshot.revision);
    assert_eq!(produced_u64(&row[3]), original_owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
        expected.snapshot
    );
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_after.tables[3].len(), 6);
    assert_eq!(&raw_after.tables[3][..5], raw_before.tables[3].as_slice());
    let row = &raw_after.tables[3][5];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&raw_before.tables[3][4][0])
    );
    assert_eq!(produced_text(&row[1]), "commit");
    assert_eq!(produced_text(&row[2]), after.thread.id.as_str());
    assert_eq!(
        produced_text(&row[3]),
        cancellation_entry.execution_id.as_str()
    );
    assert_eq!(produced_u64(&row[4]), cancellation_entry.revision);
    assert_eq!(produced_u64(&row[5]), cancellation_entry.owner_generation);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&cancellation_entry.status).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), cancellation_entry.updated_at_ms);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        cancellation_entry
    );
    assert_eq!(
        next_raw.tables.each_ref().map(Vec::len),
        [1, 1, 1, 6, 1, 0, 1, 1, 0]
    );
    assert_eq!(next_raw.tables[4][0][6], rusqlite::types::Value::Integer(1));
    assert!(next_raw.tables[8].is_empty());
    assert_eq!(
        next_view.thread.active_execution_id.as_ref(),
        Some(&execution)
    );

    let second_execution = ExecutionId::new("permission-second").unwrap();
    let second_turn = TurnId::new("second-turn").unwrap();
    let changed_values = Map::from_iter([("input".into(), json!({"release": "second fixture"}))]);
    assert_ne!(changed_values, execution_before.snapshot.values);
    assert!(next.execution(&second_execution).unwrap().is_none());
    let second_initial = new_execution(
        second_execution.clone(),
        second_turn.clone(),
        next_owner,
        definition.clone(),
        changed_values.clone(),
        facts(105),
    )
    .unwrap();
    let admitted_e2 = next
        .admit_fenced(
            &thread,
            &next_view.thread.generation,
            next_view.thread.resource_revision,
            second_initial.clone(),
        )
        .unwrap();
    assert_eq!(admitted_e2.snapshot, second_initial);
    let admission_view = next.thread_view(&thread).unwrap().unwrap();
    let admission_raw = ready_use_raw_nine(&directory.0);
    assert_eq!(
        admission_view.thread.active_execution_id.as_ref(),
        Some(&second_execution)
    );
    assert_eq!(admission_view.thread.resource_revision, 6);
    assert_eq!(
        admission_raw.tables.each_ref().map(Vec::len),
        [1, 1, 2, 7, 1, 0, 1, 1, 0]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert_eq!(admission_raw.tables[index], next_raw.tables[index]);
    }
    assert_eq!(admission_raw.tables[2][0], next_raw.tables[2][0]);
    assert_eq!(&admission_raw.tables[3][..6], next_raw.tables[3].as_slice());

    // The ordinary Start is legal setup. No raw authorization plan is committed.
    let started_e2 = next
        .commit(
            &second_execution,
            (&admitted_e2.snapshot).into(),
            transition(&admitted_e2.snapshot, KernelCommand::Start, facts(106)).unwrap(),
        )
        .unwrap();
    let before_use = next.thread_view(&thread).unwrap().unwrap();
    let e1_before_use = next.execution(&execution).unwrap();
    let e2_before_use = next.execution(&second_execution).unwrap();
    let e1_journal_before_use = next.journal(&execution).unwrap();
    let e2_journal_before_use = next.journal(&second_execution).unwrap();
    let raw_before_use = ready_use_raw_nine(&directory.0);
    assert_eq!(before_use.execution.as_ref(), Some(&started_e2));
    assert_eq!(e2_before_use.as_ref(), Some(&started_e2));
    assert_eq!(e1_before_use.as_ref(), Some(&next_execution));
    assert_eq!(e1_journal_before_use, next_journal);
    assert_eq!(before_use.thread.resource_revision, 7);
    assert_eq!(started_e2.snapshot.execution_id, second_execution);
    assert_eq!(started_e2.snapshot.turn_id, second_turn);
    assert_eq!(started_e2.snapshot.owner_generation, next_owner);
    assert_eq!(started_e2.snapshot.definition, definition);
    assert_eq!(started_e2.snapshot.values, changed_values);
    assert_eq!(started_e2.snapshot.status, ExecutionStatus::Running);
    assert_eq!(started_e2.snapshot.revision, 1);
    assert_eq!(started_e2.snapshot.attempts_used, 0);
    assert!(started_e2.snapshot.attempt_history.is_empty());
    assert!(started_e2.snapshot.output.is_none());
    assert!(started_e2.snapshot.failure.is_none());
    assert_eq!(started_e2.snapshot.tasks.len(), 1);
    let ready_task = &started_e2.snapshot.tasks[0];
    assert_eq!(ready_task.spec, definition.batches[0][0]);
    assert_eq!(ready_task.input, changed_values);
    assert_eq!(ready_task.status, TaskStatus::Ready);
    assert!(!ready_task.approved);
    assert!(ready_task.interrupt.is_none());
    assert!(ready_task.attempt_id.is_none());
    assert!(ready_task.result.is_none());
    assert!(ready_task.failure.is_none());
    b4_kernel::validate_snapshot(&started_e2.snapshot).unwrap();
    assert_eq!(e2_journal_before_use.len(), 2);
    assert_eq!(
        e2_journal_before_use[0],
        JournalEntry::from(&second_initial)
    );
    assert_eq!(
        e2_journal_before_use[1],
        JournalEntry::from(&started_e2.snapshot)
    );
    assert_eq!(
        raw_before_use.tables.each_ref().map(Vec::len),
        [1, 1, 2, 8, 1, 0, 1, 1, 0]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert_eq!(raw_before_use.tables[index], admission_raw.tables[index]);
    }
    assert_eq!(raw_before_use.tables[2][0], admission_raw.tables[2][0]);
    assert_eq!(
        &raw_before_use.tables[3][..7],
        admission_raw.tables[3].as_slice()
    );

    let mut setup_control_account = PermissionEvidenceBudget::new();
    let original_decision = PermissionDecision::decode_record(
        produced_text(&raw_before_use.tables[7][0][5]).as_bytes(),
        &mut setup_control_account,
    )
    .unwrap();
    assert_eq!(original_decision.record_version(), 1);
    assert_eq!(original_decision.creator_execution_id(), &execution);
    assert_eq!(original_decision.creator_turn_id(), &request.turn_id);
    assert_eq!(original_decision.creator_task_id(), &task);
    assert_eq!(
        original_decision.scope_digest(),
        original_decision
            .scope()
            .canonical_digest(&mut setup_control_account)
            .unwrap()
    );
    let original_scope = original_decision.scope();
    assert_eq!(original_scope.thread_id(), &before_use.thread.id);
    assert_eq!(
        original_scope.thread_generation(),
        &before_use.thread.generation
    );
    assert_eq!(original_scope.origin(), &context.actor);
    assert_eq!(original_scope.application().application(), "permission-app");
    assert_eq!(original_scope.application().revision(), "application-1");
    assert_eq!(original_scope.route(), &definition.route);
    assert_eq!(original_scope.definition(), &definition);
    assert_eq!(original_scope.effect_id(), &ready_task.spec.effect_id);
    assert_eq!(original_scope.tool_id().as_str(), "release-tool");
    assert_eq!(original_scope.tool_revision().as_str(), "tool-1");
    assert_eq!(original_scope.policy_revision(), context.policy_revision);
    assert_eq!(
        original_scope.constraint_revision().as_str(),
        "constraint-1"
    );
    let expected_call_digest =
        permission_call_digest(ready_task, &mut setup_control_account).unwrap();
    let original_call_digest = permission_call_digest(
        &execution_before.snapshot.tasks[0],
        &mut setup_control_account,
    )
    .unwrap();
    assert_ne!(
        expected_call_digest, original_call_digest,
        "changed allowed input has a different actual call witness"
    );
    drop(setup_control_account);

    // Fresh native/catalog objects follow complete original-owner retirement.
    let use_resource_reads = Arc::new(AtomicUsize::new(0));
    let new_authority_calls = Arc::new(AtomicUsize::new(0));
    let new_gate_calls = Arc::new(AtomicUsize::new(0));
    let use_entries = Arc::new(AtomicUsize::new(0));
    let unused_runtime_captures = Arc::new(AtomicUsize::new(0));
    let unused_cancellation_entries = Arc::new(AtomicUsize::new(0));
    let fresh_application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let expected_invocation = InvocationContext {
        thread_id: before_use.thread.id.clone(),
        thread_generation: before_use.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: "application-1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let fresh_catalog = Arc::new(
        PermissionCatalog::new(
            fresh_application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: Arc::new(ReadyUseConstraint {
                    expected_context: expected_invocation,
                    expected_definition: definition.clone(),
                    expected_task: ready_task.clone(),
                    calls: new_gate_calls.clone(),
                }),
            }],
        )
        .unwrap(),
    );
    assert_eq!(
        fresh_application.definition(&definition.route),
        Some(&definition)
    );
    let use_authority = ReadyUseAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_use.thread.clone(),
            execution: started_e2.clone(),
        },
        calls: new_authority_calls.clone(),
    };
    // Command capture is caller-owned fixture setup, outside the service account.
    // The service only borrows this command; it cannot claim a retroactive clone payment.
    let use_id = PermissionUseId::new(
        "b4pu_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    )
    .unwrap();
    let command = b4_kernel::PermissionAuthorizationCommand::capture(
        &started_e2.snapshot,
        task.clone(),
        use_id.clone(),
    )
    .unwrap();
    let use_fence = ApprovalFence {
        execution: (&started_e2.snapshot).into(),
        resource_revision: before_use.thread.resource_revision,
    };
    let next = Arc::new(next);
    let observed_use = ReadyUseObservedStore {
        inner: next.clone(),
        resources: use_resource_reads.clone(),
        captures: unused_runtime_captures.clone(),
        terminal_entries: unused_cancellation_entries.clone(),
        use_entries: use_entries.clone(),
    };
    let mut use_account = PermissionEvidenceBudget::new();
    let initial_use_usage = produced_usage(&use_account);
    // Fault injection follows the complete legal setup. It grants no authority.
    let fault_connection = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    match fault {
        ReadyUseFault::JournalAbort => {
            assert_eq!(second_execution.as_str(), "permission-second");
            assert_eq!(started_e2.snapshot.revision.checked_add(1), Some(2));
            fault_connection.execute_batch(
                "CREATE TRIGGER ready_use_journal_abort BEFORE INSERT ON journal                  WHEN NEW.kind = 'commit' AND NEW.execution_id = 'permission-second'                  AND NEW.revision = x'0000000000000002'                  BEGIN SELECT RAISE(ABORT, 'ready_use_journal_fault'); END;"
            ).unwrap();
        }
        ReadyUseFault::SecondDecisionHeader => {
            let fault_id = PermissionId::new(
                "b4pd_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            )
            .unwrap();
            assert_ne!(
                fault_id.as_str(),
                produced_text(&raw_before_use.tables[7][0][0])
            );
            let fault_digest = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
            assert_ne!(fault_digest, produced_text(&raw_before_use.tables[7][0][3]));
            let inserted = fault_connection.execute(
                "INSERT INTO permission_decisions (permission_id, thread_id, thread_generation, scope_digest, record_version, record)                  SELECT ?1, thread_id, thread_generation, ?2, record_version, record                  FROM permission_decisions WHERE permission_id = ?3",
                (fault_id.as_str(), fault_digest, produced_text(&raw_before_use.tables[7][0][0]))
            ).unwrap();
            assert_eq!(inserted, 1);
        }
    }
    let raw_fault_before = ready_use_fault_raw_nine(&directory.0);
    for index in [0, 1, 2, 3, 4, 5, 6, 8] {
        assert_eq!(raw_fault_before.tables[index], raw_before_use.tables[index]);
    }
    match fault {
        ReadyUseFault::JournalAbort => assert!(raw_fault_before == raw_before_use),
        ReadyUseFault::SecondDecisionHeader => {
            assert_eq!(raw_fault_before.tables[7].len(), 2);
            assert!(raw_fault_before.tables[7].contains(&raw_before_use.tables[7][0]));
        }
    }
    let use_result = {
        let use_request = PermissionReadyAuthorizationRequest {
            context: &context,
            command: &command,
            facts: facts(107),
        };
        try_authorize_permission_ready(
            &observed_use,
            &use_authority,
            fresh_catalog.as_ref(),
            use_fence,
            &use_request,
            &mut use_account,
        )
    };
    let final_use_usage = produced_usage(&use_account);
    // Every actual AFTER is captured before unwrapping or selecting the result.
    let after_use = next.thread_view(&thread).unwrap().unwrap();
    let e1_after_use = next.execution(&execution).unwrap();
    let e2_after_use = next.execution(&second_execution).unwrap();
    let e1_journal_after_use = next.journal(&execution).unwrap();
    let e2_journal_after_use = next.journal(&second_execution).unwrap();
    let raw_after_use = ready_use_fault_raw_nine(&directory.0);
    let actual_use_resource_reads = use_resource_reads.load(Ordering::SeqCst);
    let actual_use_authority_calls = new_authority_calls.load(Ordering::SeqCst);
    let actual_use_gate_calls = new_gate_calls.load(Ordering::SeqCst);
    let actual_use_entries = use_entries.load(Ordering::SeqCst);
    let actual_unused_captures = unused_runtime_captures.load(Ordering::SeqCst);
    let actual_unused_cancellation_entries = unused_cancellation_entries.load(Ordering::SeqCst);
    if matches!(fault, ReadyUseFault::JournalAbort) {
        fault_connection
            .execute_batch("DROP TRIGGER ready_use_journal_abort")
            .unwrap();
    }
    assert!(
        fault_connection.close().is_ok(),
        "fault connection closes before owner retirement"
    );
    drop(use_account);
    drop(observed_use);
    drop(use_authority);
    drop(fresh_catalog);
    drop(fresh_application);
    drop(command);
    next.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&next),
        1,
        "all new store aliases retire normally"
    );
    drop(next);
    let persisted = SqliteStore::open(&directory.0).unwrap();
    let persisted_owner = persisted.owner_generation();
    let persisted_view = persisted.thread_view(&thread).unwrap().unwrap();
    let persisted_e1 = persisted.execution(&execution).unwrap();
    let persisted_e2 = persisted.execution(&second_execution).unwrap();
    let persisted_e1_journal = persisted.journal(&execution).unwrap();
    let persisted_e2_journal = persisted.journal(&second_execution).unwrap();
    let persisted_raw = ready_use_fault_raw_nine(&directory.0);
    persisted.readiness().unwrap();
    drop(persisted);
    let final_native_calls = calls.load(Ordering::SeqCst);
    drop(calls);
    drop(authority_calls);
    drop(capture_calls);
    drop(terminal_entries);
    drop(use_resource_reads);
    drop(new_authority_calls);
    drop(new_gate_calls);
    drop(use_entries);
    drop(unused_runtime_captures);
    drop(unused_cancellation_entries);
    let removed = directory.0.clone();
    drop(directory);
    assert!(
        !removed.exists(),
        "Directory is gone before the automatic-use primary"
    );
    assert_eq!(persisted_owner, original_owner.checked_add(2).unwrap());
    assert_eq!(persisted_view, after_use);
    assert_eq!(persisted_e1, e1_after_use);
    assert_eq!(persisted_e2, e2_after_use);
    assert_eq!(persisted_e1_journal, e1_journal_after_use);
    assert_eq!(persisted_e2_journal, e2_journal_after_use);
    assert_eq!(persisted_raw.tables[1..], raw_after_use.tables[1..]);
    assert_eq!(
        persisted_raw.tables[0][0][..3],
        raw_after_use.tables[0][0][..3]
    );
    assert_eq!(
        produced_u64(&persisted_raw.tables[0][0][3]),
        persisted_owner
    );
    assert_eq!(produced_u64(&raw_after_use.tables[0][0][3]), next_owner);
    assert_eq!(final_native_calls, 0);
    assert!(
        actual_use_resource_reads >= 1,
        "the new bounded resource port was reached"
    );
    assert!(
        actual_use_authority_calls >= 1,
        "real Dispatch authority was reached"
    );
    assert!(
        actual_use_gate_calls >= 1,
        "the current allowed changed-input gate was reached"
    );
    assert_eq!(
        actual_use_entries, 1,
        "the actual new typed store seam was reached once"
    );
    assert_eq!(actual_unused_captures, 0);
    assert_eq!(actual_unused_cancellation_entries, 0);
    assert_eq!(initial_use_usage, (0, 0));
    assert!(
        final_use_usage.0 > 0
            && final_use_usage.1 > 0
            && final_use_usage.0 <= 8_388_608
            && final_use_usage.1 <= 65_536
    );
    let use_code = use_result
        .as_ref()
        .err()
        .map(|e| format!("{e}"))
        .unwrap_or_else(|| "ok".into());
    eprintln!("ready use evidence: resources={actual_use_resource_reads}; authority={actual_use_authority_calls}; gate={actual_use_gate_calls}; seam={actual_use_entries}; code={use_code}; initial_usage={initial_use_usage:?}; final_usage={final_use_usage:?}; owners={original_owner},{next_owner},{persisted_owner}; native={final_native_calls}; directory_removed={}", !removed.exists());
    assert_eq!(actual_use_resource_reads, 1);
    assert_eq!(actual_use_authority_calls, 1);
    assert_eq!(actual_use_gate_calls, 1);
    assert_eq!(after_use, before_use);
    assert_eq!(e1_after_use, e1_before_use);
    assert_eq!(e2_after_use, e2_before_use);
    assert_eq!(e1_journal_after_use, e1_journal_before_use);
    assert_eq!(e2_journal_after_use, e2_journal_before_use);
    assert!(
        raw_after_use == raw_fault_before,
        "fault refusal preserves all nine actual groups"
    );
    assert!(raw_after_use.tables[8].is_empty()); // The first fault primary follows all AFTER observations and normal retirement.
    match fault {
        ReadyUseFault::JournalAbort => assert!(
            matches!(&use_result, Err(PermissionError::Store(StoreError::Corrupt(message)))
                if message == "permission_acceptance_cells_invalid"),
            "the final journal write must fail through the exact fixed storage mapper; code={use_code}"
        ),
        ReadyUseFault::SecondDecisionHeader => assert!(
            matches!(&use_result, Err(PermissionError::UnsupportedProfile)),
            "a second complete Decision header must refuse the bounded singleton profile; code={use_code}"
        ),
    }
}

#[test]
fn ready_use_journal_write_failure_rolls_back_all_four_changes_after_reopen() {
    capture_ready_use_fault(ReadyUseFault::JournalAbort);
}

#[test]
fn ready_use_refuses_second_decision_header_without_changing_storage() {
    capture_ready_use_fault(ReadyUseFault::SecondDecisionHeader);
}

#[path = "persistent_permissions/use_dispatch.rs"]
mod permission_use_dispatch;

#[path = "persistent_permissions/use_renewal.rs"]
mod permission_use_renewal;

#[path = "persistent_permissions/use_callback.rs"]
mod permission_use_callback;

#[path = "persistent_permissions/use_outcome.rs"]
mod permission_use_outcome;

#[path = "persistent_permissions/use_driver.rs"]
mod use_driver;
