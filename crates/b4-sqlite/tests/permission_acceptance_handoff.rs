#![cfg(unix)]

use b4_contract::*;
use b4_kernel::{new_execution, transition, validate_snapshot};
use b4_runtime::approval::*;
use b4_runtime::permissions::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use rusqlite::types::{Value, ValueRef};
use serde_json::{json, Map};
use std::collections::BTreeMap;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const ORIGINAL_PROPOSAL_DIGEST: &str =
    "1f310be924370065c3a18764729b40e9b0f27789e948c20b99ede89f8ff41b18";
const MAX_EVIDENCE_BYTES: usize = 8_388_608;
const MAX_EVIDENCE_NODES: usize = 65_536;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-acceptance-handoff-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
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

struct CountingAuthority(Arc<AtomicUsize>);
impl ApprovalAuthority for CountingAuthority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        operation: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if operation == ApprovalOperation::Accept {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}
struct PermittingConstraint(Arc<AtomicUsize>);
impl ToolConstraint for PermittingConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct CountNativeCalls(Arc<AtomicUsize>);
impl NativeEffect for CountNativeCalls {
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

// Fixed complete typed captures are memory witnesses, never authority seeds.
#[derive(Clone, PartialEq)]
struct RawNine {
    tables: [Vec<Vec<Value>>; 9],
}
fn raw_cells(
    connection: &rusqlite::Connection,
    sql: &str,
    columns: usize,
    maximum: usize,
) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    let mut cursor = statement.query([]).unwrap();
    let mut rows = Vec::new();
    while let Some(row) = cursor.next().unwrap() {
        assert!(
            rows.len() < maximum,
            "fixed raw capture detects unexpected extra fixture rows"
        );
        let mut cells = Vec::new();
        for column in 0..columns {
            match row.get_ref(column).unwrap() {
                ValueRef::Text(bytes) => assert!(
                    bytes.len() <= 16_384 && std::str::from_utf8(bytes).is_ok(),
                    "fixed raw text is bounded UTF-8 before ownership"
                ),
                ValueRef::Blob(bytes) => {
                    assert!(bytes.len() == 8, "fixed raw integers retain exact BLOB8")
                }
                ValueRef::Null | ValueRef::Integer(_) => {}
                ValueRef::Real(_) => panic!("fixed fixture has no SQL REAL cell"),
            }
            cells.push(row.get::<_, Value>(column).unwrap());
        }
        rows.push(cells);
    }
    rows
}
fn raw_nine(connection: &rusqlite::Connection) -> RawNine {
    RawNine { tables: [
        raw_cells(connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        raw_cells(connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        raw_cells(connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 2", 5, 1),
        raw_cells(connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 6", 9, 5),
        raw_cells(connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        raw_cells(connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        raw_cells(connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] }
}
fn observer(path: &Path) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(path.join("runtime.sqlite3")).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .unwrap();
    let enabled: i64 = connection
        .pragma_query_value(None, "foreign_keys", |row| row.get(0))
        .unwrap();
    assert!(
        enabled == 1,
        "raw observation retains real foreign key checks"
    );
    connection
}
fn close_observer(connection: rusqlite::Connection) {
    assert!(connection.close().is_ok(), "raw observer closes normally");
}
fn read_raw(path: &Path) -> RawNine {
    let connection = observer(path);
    let raw = raw_nine(&connection);
    close_observer(connection);
    raw
}
type Usage = (usize, usize);
type UsagePair = (Usage, Usage);
fn usage(budget: &PermissionEvidenceBudget) -> Usage {
    let usage = budget.usage();
    (usage.encoded_bytes(), usage.structural_nodes())
}
// One finite observation of an actual checked View, never a second owned View.
#[derive(Clone, Copy)]
struct ProjectionFacts {
    resource_matches: bool,
    old_rows_match: bool,
    new_graph_empty: bool,
    purpose: PermissionReadPurpose,
    receipt_usage: Usage,
}
fn projection_facts(view: &PermissionView, expected: &ApprovalView) -> ProjectionFacts {
    ProjectionFacts {
        resource_matches: view.thread() == &expected.thread
            && view.execution() == &expected.execution
            && view.resource().thread == expected.thread
            && view.resource().execution == expected.execution,
        old_rows_match: view.grants() == expected.grants.as_slice()
            && view.acceptances() == expected.acceptances.as_slice(),
        new_graph_empty: view.permission_acceptances().is_empty()
            && view.permission_decisions().is_empty()
            && view.permission_uses().is_empty()
            && view.creator_acceptances().is_empty()
            && view.referenced_grants().is_empty()
            && view.referenced_acceptances().is_empty()
            && view.journal_entries().is_empty(),
        purpose: view.receipt().purpose(),
        receipt_usage: (
            view.receipt().usage_at_capture().encoded_bytes(),
            view.receipt().usage_at_capture().structural_nodes(),
        ),
    }
}
struct Stage {
    resource_calls: usize,
    combined_calls: usize,
    resource_usages: [Option<UsagePair>; 2],
    combined_usages: [Option<UsagePair>; 2],
    c2_facts: Option<ProjectionFacts>,
    c2_code: Option<&'static str>,
    port_calls: usize,
    port_usage: Option<UsagePair>,
    default_result: Option<Result<StoredExecution, PermissionError>>,
}
impl Stage {
    fn new() -> Self {
        Self {
            resource_calls: 0,
            combined_calls: 0,
            resource_usages: [None; 2],
            combined_usages: [None; 2],
            c2_facts: None,
            c2_code: None,
            port_calls: 0,
            port_usage: None,
            default_result: None,
        }
    }
}

// This adapter inherits the trait default even after SqliteStore gains a writer.
// All existing resource/approval/store behavior delegates to the same real store.
struct DefaultPermissionPort {
    inner: Arc<SqliteStore>,
}
struct CaptureStore {
    inner: Arc<DefaultPermissionPort>,
    expected: ApprovalView,
    stage: Mutex<Stage>,
}
macro_rules! delegate_existing_ports {
    ($ty:ty) => {
        impl Store for $ty {
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
            fn delete_thread_fenced(
                &self,
                target: &ThreadDeletionTarget,
            ) -> Result<(), StoreError> {
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
                thread: &ThreadId,
                initial: Snapshot,
            ) -> Result<StoredExecution, StoreError> {
                self.inner.admit(thread, initial)
            }
            fn admit_fenced(
                &self,
                thread: &ThreadId,
                generation: &ThreadGeneration,
                revision: u64,
                initial: Snapshot,
            ) -> Result<StoredExecution, StoreError> {
                self.inner
                    .admit_fenced(thread, generation, revision, initial)
            }
            fn readiness(&self) -> Result<(), StoreError> {
                self.inner.readiness()
            }
            fn commit(
                &self,
                id: &ExecutionId,
                fence: ExecutionFence,
                plan: CommitPlan,
            ) -> Result<StoredExecution, StoreError> {
                self.inner.commit(id, fence, plan)
            }
            fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
                self.inner.journal(id)
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
                turn: &TurnId,
                facts: TransitionFacts,
            ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
                self.inner
                    .confine_recovery_fenced(original, target, reason, turn, facts)
            }
        }
        impl ApprovalStore for $ty {
            fn approval_durable(&self) -> bool {
                self.inner.approval_durable()
            }
            fn approval_view(
                &self,
                id: &ExecutionId,
            ) -> Result<Option<ApprovalView>, ApprovalError> {
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
    };
}
delegate_existing_ports!(DefaultPermissionPort);
delegate_existing_ports!(CaptureStore);

impl PermissionStore for DefaultPermissionPort {
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
    // No commit_permission_acceptance override: exercise the actual trait default.
}
impl PermissionStore for CaptureStore {
    fn durable_permissions(&self) -> bool {
        self.inner.durable_permissions()
    }
    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        let index = {
            let mut stage = self.stage.lock().unwrap();
            let index = stage.resource_calls;
            assert!(
                index < 2,
                "this finite attempt has at most two external resource reads"
            );
            stage.resource_calls += 1;
            index
        };
        let before = usage(budget);
        let result = self.inner.permission_resource(id, budget);
        let after = usage(budget);
        self.stage.lock().unwrap().resource_usages[index] = Some((before, after));
        result
    }
    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        let index = {
            let mut stage = self.stage.lock().unwrap();
            let index = stage.combined_calls;
            assert!(
                index < 2,
                "this finite attempt has at most two combined reads"
            );
            stage.combined_calls += 1;
            index
        };
        let is_c2 = selector.purpose() == PermissionReadPurpose::Accept
            && selector.expected_view().is_some();
        let before = usage(budget);
        let result = self.inner.permission_view(id, fence, selector, budget);
        let after = usage(budget);
        {
            let mut stage = self.stage.lock().unwrap();
            stage.combined_usages[index] = Some((before, after));
            if is_c2 {
                assert!(
                    stage.c2_code.is_none(),
                    "the fixed attempt observes at most one C2"
                );
                stage.c2_code = Some(match &result {
                    Ok(_) => "ok",
                    Err(error) => error.code(),
                });
                stage.c2_facts = result
                    .as_ref()
                    .ok()
                    .map(|view| projection_facts(view, &self.expected));
            }
        }
        // The actual noncloneable View is returned unchanged, not retained twice.
        result
    }
    fn commit_permission_acceptance(
        &self,
        prepared: PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<StoredExecution, PermissionError> {
        {
            let mut stage = self.stage.lock().unwrap();
            assert!(
                stage.port_calls < 1,
                "one fixed attempt observes at most one typed handoff"
            );
            stage.port_calls += 1;
        }
        let before = usage(budget);
        let result =
            PermissionStore::commit_permission_acceptance(self.inner.as_ref(), prepared, budget);
        let after = usage(budget);
        {
            let mut stage = self.stage.lock().unwrap();
            stage.port_usage = Some((before, after));
            stage.default_result = Some(result);
        }
        // A fixed probe sentinel after the actual stable default, never a gate denial.
        Err(PermissionError::ConstraintUnavailable)
    }
}

struct Captured {
    attempted: bool,
    stage: Stage,
    facade: Option<Result<StoredExecution, PermissionError>>,
    before_public: Result<PermissionView, PermissionError>,
    before: ApprovalView,
    before_ordinary: ThreadView,
    after_ordinary: ThreadView,
    after: ApprovalView,
    before_journal: Vec<JournalEntry>,
    after_journal: Vec<JournalEntry>,
    raw_original: RawNine,
    raw_attempt_after: RawNine,
    pending: Vec<SensitivePendingApproval>,
    request: ApprovalAcceptanceRequest,
    stage_policy_calls: usize,
    constraint_calls: usize,
}

// All actual Results and finite projection facts cross complete normal retirement.
fn capture_after_normal_retirement(attempted: bool) -> Captured {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let constraint_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let thread_id = ThreadId::new("permission-thread").unwrap();
    let execution_id = ExecutionId::new("permission-execution").unwrap();
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("release").unwrap(),
            effect_id: EffectId::new("release-effect").unwrap(),
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
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountNativeCalls(native_calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    assert!(
        registered == definition,
        "real NativeApplication registers the complete exact Definition"
    );
    let constraint = Arc::new(PermittingConstraint(constraint_calls.clone()));
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: constraint.clone(),
            }],
        )
        .unwrap(),
    );
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert!(
        original_owner == 1 && store.approval_durable() && !store.durable_permissions(),
        "fresh actual durable old approval keeps the lasting permission profile false"
    );
    let thread = store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread_id, initial).unwrap();
    let started = store
        .commit(
            &execution_id,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    assert!(
        started.snapshot.status == ExecutionStatus::Running
            && started.snapshot.revision == 1
            && started.snapshot.tasks.len() == 1
            && started.snapshot.tasks[0].status == TaskStatus::Ready
            && !started.snapshot.tasks[0].approved,
        "actual Start reaches the lawful native required unapproved Ready"
    );
    let authority = Arc::new(CountingAuthority(policy_calls.clone()));
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let parked = service
        .park(
            &context,
            &execution_id,
            fence(store.as_ref(), &thread_id),
            definition.batches[0][0].id.clone(),
            InterruptId::new("release-interrupt").unwrap(),
            json!({"question": "Release fixture?", "arguments": {"nullable": null}}),
            1000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let before = store.approval_view(&execution_id).unwrap().unwrap();
    let before_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    validate_snapshot(&before.execution.snapshot).unwrap();
    assert!(
        parked == before.execution
            && parked.snapshot.revision == 2
            && parked.snapshot.status == ExecutionStatus::WaitingForApproval
            && before.thread.resource_revision == 3
            && pending.len() == 1
            && before.grants.len() == 1
            && before.acceptances.is_empty(),
        "actual one-task Park issues precisely one real grant and token"
    );
    let grant = &before.grants[0];
    let task = &parked.snapshot.tasks[0];
    assert!(
        grant.thread_id == thread_id
            && grant.thread_generation == thread.generation
            && grant.origin == context.actor
            && grant.execution_id == execution_id
            && grant.route == definition.route
            && grant.definition_revision == definition.revision
            && grant.task_id == task.spec.id
            && grant.effect_id == task.spec.effect_id
            && grant.interrupt_id == pending[0].interrupt_id
            && grant.interrupt_id == task.interrupt.as_ref().unwrap().id
            && grant.prompt == pending[0].prompt
            && grant.prompt == task.interrupt.as_ref().unwrap().prompt
            && grant.policy_revision == context.policy_revision
            && grant.task_parking_revision == 2
            && grant.issued_at_ms == 102
            && grant.expires_at_ms == 1102
            && !grant.consumed
            && grant.token == pending[0].token
            && grant.verifier == token_verifier(&pending[0].token)
            && grant.resume_key == pending[0].resume_key
            && grant.proposal_digest == ORIGINAL_PROPOSAL_DIGEST
            && grant.proposal_digest == proposal_digest(task, &grant.prompt).unwrap(),
        "the complete genuine grant/token/proposal/prompt bindings precede the staged subject"
    );
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: pending[0].interrupt_id.clone(),
            decision: ApprovalDecision::Always,
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };
    let read_fence = fence(store.as_ref(), &thread_id);
    let before_public = {
        let mut budget = PermissionEvidenceBudget::new();
        read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution_id,
            read_fence,
            &context,
            PermissionReadPurpose::Pending,
            None,
            &mut budget,
        )
    };
    let raw_original = read_raw(&directory.0);
    let default_port = Arc::new(DefaultPermissionPort {
        inner: store.clone(),
    });
    let proxy = Arc::new(CaptureStore {
        inner: default_port.clone(),
        expected: before.clone(),
        stage: Mutex::new(Stage::new()),
    });
    let policy_before = policy_calls.load(Ordering::SeqCst);
    let facade = if attempted {
        Some(accept_permission_human(
            proxy.clone(),
            authority.clone(),
            catalog.clone(),
            &execution_id,
            read_fence,
            request.clone(),
        ))
    } else {
        None
    };
    let policy_after = policy_calls.load(Ordering::SeqCst);
    let raw_attempt_after = read_raw(&directory.0);
    let stage = {
        let mut stage = proxy.stage.lock().unwrap();
        std::mem::replace(&mut *stage, Stage::new())
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(proxy);
    drop(default_port);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(constraint);
    drop(application);
    drop(thread);
    drop(admitted);
    drop(started);
    drop(parked);
    assert!(
        Arc::strong_count(&store) == 1,
        "all original services/adapters/catalog/app/store aliases retire normally"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_approval = next.approval_view(&execution_id).unwrap().unwrap();
    let next_journal = next.journal(&execution_id).unwrap();
    let raw_next = read_raw(&directory.0);
    next.readiness().unwrap();
    drop(next);
    assert!(
        next_owner == original_owner.checked_add(1).unwrap()
            && next_ordinary == after_ordinary
            && next_approval == after
            && next_journal == after_journal,
        "real next writer reads the full ACTUAL AFTER state and closes"
    );
    let mut expected_next_raw = raw_attempt_after.clone();
    expected_next_raw.tables[0][0][3] = Value::Blob(next_owner.to_be_bytes().to_vec());
    assert!(
        raw_next == expected_next_raw,
        "all ACTUAL AFTER raw cells survive next ownership except the global metadata epoch"
    );
    let constraint_count = constraint_calls.load(Ordering::SeqCst);
    assert!(
        native_calls.load(Ordering::SeqCst) == 0,
        "no setup reader or typed handoff invokes native code"
    );
    drop(native_calls);
    drop(constraint_calls);
    drop(policy_calls);
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(raw_next);
    drop(expected_next_raw);
    let directory_path = directory.0.clone();
    drop(directory);
    assert!(
        !directory_path.exists(),
        "all stores and next-writer DTOs close before directory disposal"
    );
    Captured {
        attempted,
        stage,
        facade,
        before_public,
        before,
        before_ordinary,
        after_ordinary,
        after,
        before_journal,
        after_journal,
        raw_original,
        raw_attempt_after,
        pending,
        request,
        stage_policy_calls: policy_after.checked_sub(policy_before).unwrap(),
        constraint_calls: constraint_count,
    }
}
fn assert_projection_facts(facts: ProjectionFacts, purpose: PermissionReadPurpose) {
    assert!(
        facts.resource_matches
            && facts.old_rows_match
            && facts.new_graph_empty
            && facts.purpose == purpose,
        "actual projection covers complete resource/old rows and empty new graph"
    );
    assert!(
        facts.receipt_usage.0 > 0
            && facts.receipt_usage.1 > 0
            && facts.receipt_usage.0 <= MAX_EVIDENCE_BYTES
            && facts.receipt_usage.1 <= MAX_EVIDENCE_NODES,
        "actual receipt is a finite nonauthoritative capture fact"
    );
}
fn assert_outside_controls(captured: &Captured) {
    assert!(
        captured.before_public.is_ok(),
        "separate installed None reader is a genuine successful setup control"
    );
    assert_projection_facts(
        projection_facts(captured.before_public.as_ref().unwrap(), &captured.before),
        PermissionReadPurpose::Pending,
    );
    assert!(
        validate_snapshot(&captured.before.execution.snapshot).is_ok()
            && captured.before.grants.len() == 1
            && captured.pending.len() == 1
            && captured.before.acceptances.is_empty(),
        "real native Park supplies one strict current grant and no manufactured authority"
    );
    let grant = &captured.before.grants[0];
    let decision = &captured.request.decisions[0];
    assert!(
        captured.request.decisions.len() == 1
            && captured.request.now_ms == 103
            && captured.request.context.actor == grant.origin
            && captured.request.context.policy_revision == grant.policy_revision
            && captured.request.route == grant.route
            && captured.request.definition_revision == grant.definition_revision
            && captured.request.turn_id.as_str() == "approved-turn"
            && decision.interrupt_id == grant.interrupt_id
            && decision.decision == ApprovalDecision::Always
            && !decision.cancelled
            && decision.token.as_ref() == Some(&captured.pending[0].token)
            && grant.token == captured.pending[0].token
            && grant.verifier == token_verifier(&captured.pending[0].token)
            && grant.proposal_digest == ORIGINAL_PROPOSAL_DIGEST
            && !grant.consumed
            && grant.issued_at_ms <= captured.request.now_ms
            && captured.request.now_ms < grant.expires_at_ms,
        "retained request uses complete actual live token, context, TTL and proposal bindings"
    );
}
fn assert_storage_preserved(captured: &Captured) {
    assert!(captured.before_ordinary == captured.after_ordinary
        && captured.before == captured.after
        && captured.before_journal == captured.after_journal
        && captured.raw_original == captured.raw_attempt_after,
        "default handoff and probe sentinel preserve all actual ordinary/approval/journal/raw cells");
}
fn assert_neighbor(captured: &Captured) {
    assert_outside_controls(captured);
    assert!(
        !captured.attempted
            && captured.facade.is_none()
            && captured.stage.port_calls == 0
            && captured.stage.default_result.is_none()
            && captured.stage.resource_calls == 0
            && captured.stage.combined_calls == 0
            && captured.stage.c2_code.is_none()
            && captured.stage_policy_calls == 0
            && captured.constraint_calls == 0,
        "separately genuine neighbor uses only installed None reader"
    );
    assert_storage_preserved(captured);
}
fn facade_code(result: &Option<Result<StoredExecution, PermissionError>>) -> &'static str {
    match result {
        None => "missing",
        Some(Ok(_)) => "ok",
        Some(Err(error)) => error.code(),
    }
}

#[test]
fn human_always_hands_actual_matched_view_to_the_sealed_default_port() {
    let neighbor = capture_after_normal_retirement(false);
    let subject = capture_after_normal_retirement(true);
    assert_neighbor(&neighbor);
    assert_outside_controls(&subject);
    assert!(subject.stage.port_calls == 1,
        "public Always must reach the sealed typed port after complete retirement; calls={} code={} c2={}",
        subject.stage.port_calls, facade_code(&subject.facade),
        subject.stage.c2_code.unwrap_or("missing"));
    assert!(
        matches!(
            &subject.stage.default_result,
            Some(Err(PermissionError::UnsupportedProfile))
        ),
        "the exact same Prepared and account reach the stable real trait default"
    );
    assert!(
        matches!(
            &subject.facade,
            Some(Err(PermissionError::ConstraintUnavailable))
        ),
        "the outer result is the fixed test probe sentinel after the real default"
    );
    assert!(
        subject.stage.c2_code == Some("ok"),
        "actual matched C2 succeeded before typed handoff"
    );
    assert_projection_facts(
        subject.stage.c2_facts.unwrap(),
        PermissionReadPurpose::Accept,
    );
    assert!(
        subject.stage.resource_calls == 2
            && subject.stage.combined_calls == 2
            && subject.stage_policy_calls == 2
            && subject.constraint_calls == 1,
        "actual same-account C1/R2/C2 and one permitting constraint precede the typed port"
    );
    let ordered = [
        subject.stage.resource_usages[0].unwrap(),
        subject.stage.combined_usages[0].unwrap(),
        subject.stage.resource_usages[1].unwrap(),
        subject.stage.combined_usages[1].unwrap(),
    ];
    let mut previous = (0, 0);
    for (before, after) in ordered {
        assert!(
            previous.0 <= before.0
                && previous.1 <= before.1
                && before.0 <= after.0
                && before.1 <= after.1
                && after.0 <= MAX_EVIDENCE_BYTES
                && after.1 <= MAX_EVIDENCE_NODES,
            "all four actual read phases remain monotone on the supplied finite account"
        );
        previous = after;
    }
    let (port_before, port_after) = subject.stage.port_usage.unwrap();
    assert!(
        previous.0 <= port_before.0
            && previous.1 <= port_before.1
            && port_before == port_after
            && port_after.0 <= MAX_EVIDENCE_BYTES
            && port_after.1 <= MAX_EVIDENCE_NODES,
        "stable Unsupported default forwards the same account without reset or charges"
    );
    assert_storage_preserved(&subject);
}

// This adapter receives a genuine Prepared but forwards a distinct public account.
#[derive(Clone, Copy)]
struct ForeignWarmRead {
    code: &'static str,
    projection: Option<ProjectionFacts>,
    usage: UsagePair,
}
fn warm_ordinary_accept_account(
    store: &SqliteStore,
    authority: &CountingAuthority,
    catalog: &PermissionCatalog,
    expected: &ApprovalView,
    context: &ApprovalContext,
    budget: &mut PermissionEvidenceBudget,
) -> [Option<ForeignWarmRead>; 2] {
    let actual_fence = ApprovalFence {
        execution: (&expected.execution.snapshot).into(),
        resource_revision: expected.thread.resource_revision,
    };
    let mut captured = [None; 2];
    for slot in &mut captured {
        let before = usage(budget);
        let observation = {
            let result = read_permission_view(
                store,
                authority,
                catalog,
                &expected.execution.snapshot.execution_id,
                actual_fence,
                context,
                PermissionReadPurpose::Accept,
                None,
                budget,
            );
            ForeignWarmRead {
                code: match &result {
                    Ok(_) => "ok",
                    Err(error) => error.code(),
                },
                projection: result
                    .as_ref()
                    .ok()
                    .map(|view| projection_facts(view, expected)),
                usage: (before, usage(budget)),
            }
        };
        // The actual owned View ends above; only finite observations survive.
        *slot = Some(observation);
    }
    captured
}
struct ForeignAccountStage {
    resource_calls: usize,
    combined_calls: usize,
    resource_usages: [Option<UsagePair>; 2],
    combined_usages: [Option<UsagePair>; 2],
    c2_code: Option<&'static str>,
    c2_facts: Option<ProjectionFacts>,
    port_calls: usize,
    received_usage: Option<Usage>,
    warm: [Option<ForeignWarmRead>; 2],
    forwarded_code: Option<&'static str>,
    foreign_usage: Option<UsagePair>,
}
impl ForeignAccountStage {
    fn new() -> Self {
        Self {
            resource_calls: 0,
            combined_calls: 0,
            resource_usages: [None; 2],
            combined_usages: [None; 2],
            c2_code: None,
            c2_facts: None,
            port_calls: 0,
            received_usage: None,
            warm: [None; 2],
            forwarded_code: None,
            foreign_usage: None,
        }
    }
}
struct ForeignAccountStore {
    inner: Arc<SqliteStore>,
    authority: Arc<CountingAuthority>,
    catalog: Arc<PermissionCatalog>,
    context: ApprovalContext,
    expected: ApprovalView,
    stage: Mutex<ForeignAccountStage>,
}
delegate_existing_ports!(ForeignAccountStore);
impl PermissionStore for ForeignAccountStore {
    fn durable_permissions(&self) -> bool {
        self.inner.durable_permissions()
    }
    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        let index = {
            let mut stage = self.stage.lock().unwrap();
            let index = stage.resource_calls;
            stage.resource_calls += 1;
            index
        };
        let before = usage(budget);
        let result = self.inner.permission_resource(id, budget);
        let after = usage(budget);
        if index < 2 {
            self.stage.lock().unwrap().resource_usages[index] = Some((before, after));
        }
        result
    }
    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        let index = {
            let mut stage = self.stage.lock().unwrap();
            let index = stage.combined_calls;
            stage.combined_calls += 1;
            index
        };
        let c2 = selector.purpose() == PermissionReadPurpose::Accept
            && selector.expected_view().is_some();
        let before = usage(budget);
        let result = self.inner.permission_view(id, fence, selector, budget);
        let after = usage(budget);
        {
            let mut stage = self.stage.lock().unwrap();
            if index < 2 {
                stage.combined_usages[index] = Some((before, after));
            }
            if c2 {
                stage.c2_code = Some(match &result {
                    Ok(_) => "ok",
                    Err(error) => error.code(),
                });
                stage.c2_facts = result
                    .as_ref()
                    .ok()
                    .map(|view| projection_facts(view, &self.expected));
            }
        }
        // Forward the same actual noncloneable C2; retain no second View.
        result
    }
    fn commit_permission_acceptance(
        &self,
        prepared: PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<StoredExecution, PermissionError> {
        {
            let mut stage = self.stage.lock().unwrap();
            stage.port_calls += 1;
            stage.received_usage = Some(usage(budget));
        }
        let result = {
            let mut foreign = PermissionEvidenceBudget::new();
            let warm = warm_ordinary_accept_account(
                self.inner.as_ref(),
                self.authority.as_ref(),
                self.catalog.as_ref(),
                &self.expected,
                &self.context,
                &mut foreign,
            );
            let before = usage(&foreign);
            let result = PermissionStore::commit_permission_acceptance(
                self.inner.as_ref(),
                prepared,
                &mut foreign,
            );
            let after = usage(&foreign);
            {
                let mut stage = self.stage.lock().unwrap();
                stage.warm = warm;
                stage.foreign_usage = Some((before, after));
                stage.forwarded_code = Some(match &result {
                    Ok(_) => "ok",
                    Err(error) => error.code(),
                });
            }
            result
        };
        // Both accounts end naturally; return the exact inner owned Result.
        result
    }
}
struct ForeignSetupFacts {
    definition_registered: bool,
    original_profile: bool,
    lawful_start: bool,
    lawful_park: bool,
    complete_grant: bool,
}
struct ForeignAccountCaptured {
    attempted: bool,
    setup: ForeignSetupFacts,
    stage: ForeignAccountStage,
    warm_only: [Option<ForeignWarmRead>; 2],
    facade: Option<Result<StoredExecution, PermissionError>>,
    before: ApprovalView,
    before_ordinary: ThreadView,
    before_journal: Vec<JournalEntry>,
    after_ordinary: ThreadView,
    after_approval: Result<Option<ApprovalView>, ApprovalError>,
    after_journal: Vec<JournalEntry>,
    raw_before: RawNine,
    raw_after: RawNine,
    pending: Vec<SensitivePendingApproval>,
    request: ApprovalAcceptanceRequest,
    policy_calls: usize,
    constraint_calls: usize,
}
fn capture_foreign_account_after_normal_retirement(attempted: bool) -> ForeignAccountCaptured {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let constraint_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let thread_id = ThreadId::new("permission-thread").unwrap();
    let execution_id = ExecutionId::new("permission-execution").unwrap();
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("release").unwrap(),
            effect_id: EffectId::new("release-effect").unwrap(),
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
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountNativeCalls(native_calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let definition_registered = registered == definition;
    let constraint = Arc::new(PermittingConstraint(constraint_calls.clone()));
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: constraint.clone(),
            }],
        )
        .unwrap(),
    );
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    let original_profile =
        original_owner == 1 && store.approval_durable() && !store.durable_permissions();
    let thread = store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread_id, initial).unwrap();
    let started = store
        .commit(
            &execution_id,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let lawful_start = started.snapshot.status == ExecutionStatus::Running
        && started.snapshot.revision == 1
        && started.snapshot.tasks.len() == 1
        && started.snapshot.tasks[0].status == TaskStatus::Ready
        && !started.snapshot.tasks[0].approved;
    let authority = Arc::new(CountingAuthority(policy_calls.clone()));
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let parked = service
        .park(
            &context,
            &execution_id,
            fence(store.as_ref(), &thread_id),
            definition.batches[0][0].id.clone(),
            InterruptId::new("release-interrupt").unwrap(),
            json!({"question": "Release fixture?", "arguments": {"nullable": null}}),
            1000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let before = store.approval_view(&execution_id).unwrap().unwrap();
    let before_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    let lawful_park = parked == before.execution
        && parked.snapshot.revision == 2
        && parked.snapshot.status == ExecutionStatus::WaitingForApproval
        && before.thread.resource_revision == 3
        && pending.len() == 1
        && before.grants.len() == 1
        && before.acceptances.is_empty()
        && validate_snapshot(&before.execution.snapshot).is_ok();
    let grant = &before.grants[0];
    let task = &parked.snapshot.tasks[0];
    let complete_grant = grant.thread_id == thread_id
        && grant.thread_generation == thread.generation
        && grant.origin == context.actor
        && grant.execution_id == execution_id
        && grant.route == definition.route
        && grant.definition_revision == definition.revision
        && grant.task_id == task.spec.id
        && grant.effect_id == task.spec.effect_id
        && grant.interrupt_id == pending[0].interrupt_id
        && grant.interrupt_id == task.interrupt.as_ref().unwrap().id
        && grant.prompt == pending[0].prompt
        && grant.prompt == task.interrupt.as_ref().unwrap().prompt
        && grant.policy_revision == context.policy_revision
        && grant.task_parking_revision == 2
        && grant.issued_at_ms == 102
        && grant.expires_at_ms == 1102
        && !grant.consumed
        && grant.token == pending[0].token
        && grant.verifier == token_verifier(&pending[0].token)
        && grant.resume_key == pending[0].resume_key
        && grant.proposal_digest == ORIGINAL_PROPOSAL_DIGEST
        && proposal_digest(task, &grant.prompt).is_ok_and(|digest| digest == grant.proposal_digest);
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: pending[0].interrupt_id.clone(),
            decision: ApprovalDecision::Always,
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };
    let read_fence = fence(store.as_ref(), &thread_id);
    let raw_before = read_raw(&directory.0);
    let policy_before = policy_calls.load(Ordering::SeqCst);
    let (facade, stage, warm_only) = if attempted {
        let adapter = Arc::new(ForeignAccountStore {
            inner: store.clone(),
            authority: authority.clone(),
            catalog: catalog.clone(),
            context: context.clone(),
            expected: before.clone(),
            stage: Mutex::new(ForeignAccountStage::new()),
        });
        let result = accept_permission_human(
            adapter.clone(),
            authority.clone(),
            catalog.clone(),
            &execution_id,
            read_fence,
            request.clone(),
        );
        let stage = {
            let mut slot = adapter.stage.lock().unwrap();
            std::mem::replace(&mut *slot, ForeignAccountStage::new())
        };
        drop(adapter);
        (Some(result), stage, [None; 2])
    } else {
        let warm = {
            let mut budget = PermissionEvidenceBudget::new();
            warm_ordinary_accept_account(
                store.as_ref(),
                authority.as_ref(),
                catalog.as_ref(),
                &before,
                &context,
                &mut budget,
            )
        };
        (None, ForeignAccountStage::new(), warm)
    };
    let policy_after = policy_calls.load(Ordering::SeqCst);
    let raw_after = read_raw(&directory.0);
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    // Preserve either actual approval result if a missing guard wrote new rows.
    let after_approval = store.approval_view(&execution_id);
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(service);
    drop(authority);
    drop(catalog);
    drop(constraint);
    drop(application);
    drop(thread);
    drop(admitted);
    drop(started);
    drop(parked);
    assert!(
        Arc::strong_count(&store) == 1,
        "all genuine original adapter/service/catalog/application aliases retire normally"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_approval = next.approval_view(&execution_id);
    let next_journal = next.journal(&execution_id).unwrap();
    let raw_next = read_raw(&directory.0);
    next.readiness().unwrap();
    drop(next);
    assert!(
        next_owner == original_owner.checked_add(1).unwrap()
            && next_ordinary == after_ordinary
            && next_approval == after_approval
            && next_journal == after_journal,
        "real next writer reads complete ACTUAL AFTER on either branch and retires"
    );
    let mut expected_next_raw = raw_after.clone();
    expected_next_raw.tables[0][0][3] = Value::Blob(next_owner.to_be_bytes().to_vec());
    assert!(raw_next == expected_next_raw,
        "all ACTUAL AFTER raw cells survive reopening except intentional global metadata owner advancement");
    let constraint_count = constraint_calls.load(Ordering::SeqCst);
    assert!(
        native_calls.load(Ordering::SeqCst) == 0,
        "genuine Park/public reads/forwarding never invoke the native callback"
    );
    drop(native_calls);
    drop(constraint_calls);
    drop(policy_calls);
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(raw_next);
    drop(expected_next_raw);
    let directory_path = directory.0.clone();
    drop(directory);
    assert!(
        !directory_path.exists(),
        "both writers and all next-reader DTOs close before directory disposal"
    );
    ForeignAccountCaptured {
        attempted,
        setup: ForeignSetupFacts {
            definition_registered,
            original_profile,
            lawful_start,
            lawful_park,
            complete_grant,
        },
        stage,
        warm_only,
        facade,
        before,
        before_ordinary,
        before_journal,
        after_ordinary,
        after_approval,
        after_journal,
        raw_before,
        raw_after,
        pending,
        request,
        policy_calls: policy_after.checked_sub(policy_before).unwrap(),
        constraint_calls: constraint_count,
    }
}
fn assert_foreign_account_genuine_setup(captured: &ForeignAccountCaptured) {
    assert!(captured.setup.definition_registered && captured.setup.original_profile
        && captured.setup.lawful_start && captured.setup.lawful_park && captured.setup.complete_grant,
        "the real Native/SQLite fixture supplies strict Ready/Park/current full grant before forwarding");
    let grant = &captured.before.grants[0];
    let decision = &captured.request.decisions[0];
    assert!(
        captured.request.decisions.len() == 1
            && captured.pending.len() == 1
            && captured.request.now_ms == 103
            && captured.request.turn_id.as_str() == "approved-turn"
            && captured.request.context.actor == grant.origin
            && captured.request.context.policy_revision == grant.policy_revision
            && captured.request.route == grant.route
            && captured.request.definition_revision == grant.definition_revision
            && decision.interrupt_id == grant.interrupt_id
            && decision.decision == ApprovalDecision::Always
            && !decision.cancelled
            && decision.token.as_ref() == Some(&captured.pending[0].token),
        "the human request retains the actual full token/TTL/proposal/current catalog bindings"
    );
}
fn assert_two_ordinary_warm_reads(warm: &[Option<ForeignWarmRead>; 2]) {
    let mut previous = (0, 0);
    for observation in warm {
        assert!(
            observation.is_some(),
            "both bounded ordinary warm calls were actually observed"
        );
        let observation = observation.unwrap();
        assert!(
            observation.code == "ok" && observation.projection.is_some(),
            "each actual public ordinary Accept/None warm call succeeds; code={}",
            observation.code
        );
        assert_projection_facts(
            observation.projection.unwrap(),
            PermissionReadPurpose::Accept,
        );
        let (before, after) = observation.usage;
        assert!(
            previous.0 <= before.0
                && previous.1 <= before.1
                && before.0 <= after.0
                && before.1 <= after.1
                && after.0 <= MAX_EVIDENCE_BYTES
                && after.1 <= MAX_EVIDENCE_NODES,
            "two actual ordinary reads remain monotone on their own finite account"
        );
        previous = after;
    }
}
fn assert_foreign_account_storage_preserved(captured: &ForeignAccountCaptured) {
    assert!(captured.before_ordinary == captured.after_ordinary
        && captured.after_approval.as_ref().is_ok_and(|after| after.as_ref() == Some(&captured.before))
        && captured.before_journal == captured.after_journal && captured.raw_before == captured.raw_after,
        "the foreign-account refusal preserves full BEFORE/AFTER original storage, independently of reopen epoch advancement");
}
#[test]
fn permission_acceptance_rejects_warmed_foreign_account() {
    let neighbor = capture_foreign_account_after_normal_retirement(false);
    let subject = capture_foreign_account_after_normal_retirement(true);
    assert_foreign_account_genuine_setup(&neighbor);
    assert_two_ordinary_warm_reads(&neighbor.warm_only);
    assert!(
        !neighbor.attempted
            && neighbor.facade.is_none()
            && neighbor.stage.port_calls == 0
            && neighbor.policy_calls == 2
            && neighbor.constraint_calls == 0,
        "separately genuine ordinary warm neighbor invokes no acceptance or constraint"
    );
    assert_foreign_account_storage_preserved(&neighbor);
    assert_foreign_account_genuine_setup(&subject);
    assert!(
        subject.attempted
            && subject.stage.port_calls == 1
            && subject.stage.c2_code == Some("ok")
            && subject.stage.c2_facts.is_some(),
        "actual genuine matched C2 supplies one opaque Prepared before the foreign-account attempt"
    );
    assert_projection_facts(
        subject.stage.c2_facts.unwrap(),
        PermissionReadPurpose::Accept,
    );
    assert_two_ordinary_warm_reads(&subject.stage.warm);
    assert!(
        matches!(&subject.facade, Some(Err(PermissionError::CorruptRecord))),
        "actual forwarded Prepared rejects the foreign account after complete retirement; code={}",
        facade_code(&subject.facade)
    );
    assert!(
        subject.stage.forwarded_code == Some("permission_corrupt_record"),
        "the exact inner writer result returns unchanged through the public facade"
    );
    assert_foreign_account_storage_preserved(&subject);
    assert!(subject.stage.resource_calls == 2 && subject.stage.combined_calls == 2
        && subject.policy_calls == 4 && subject.constraint_calls == 1,
        "two actual same-account acceptance reads, two separate warm reads and one permitting gate are retained");
    let mut previous = (0, 0);
    for captured in [
        subject.stage.resource_usages[0],
        subject.stage.combined_usages[0],
        subject.stage.resource_usages[1],
        subject.stage.combined_usages[1],
    ] {
        assert!(
            captured.is_some(),
            "the actual original four read phases were captured"
        );
        let (before, after) = captured.unwrap();
        assert!(
            previous.0 <= before.0
                && previous.1 <= before.1
                && before.0 <= after.0
                && before.1 <= after.1
                && after.0 <= MAX_EVIDENCE_BYTES
                && after.1 <= MAX_EVIDENCE_NODES,
            "the genuine original account read phases remain monotone and finite"
        );
        previous = after;
    }
    let received = subject.stage.received_usage.unwrap();
    assert!(
        previous.0 <= received.0
            && previous.1 <= received.1
            && received.0 <= MAX_EVIDENCE_BYTES
            && received.1 <= MAX_EVIDENCE_NODES,
        "the received Prepared retains finite actual original accounting"
    );
    let (before, after) = subject.stage.foreign_usage.unwrap();
    let final_warm = subject.stage.warm[1].unwrap().usage.1;
    assert!(final_warm.0 <= before.0 && final_warm.1 <= before.1
        && before.0 <= after.0 && before.1 <= after.1
        && after.0 <= MAX_EVIDENCE_BYTES && after.1 <= MAX_EVIDENCE_NODES,
        "the foreign account refusal has monotone finite usage, without exact fee or SQL attribution");
}
