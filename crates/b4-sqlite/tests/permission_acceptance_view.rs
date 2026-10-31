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
            "b4-acceptance-view-{}-{}",
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
        "test-owned drift retains real foreign key checks"
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
fn restore_grant(path: &Path, original: &[Value]) {
    assert!(
        original.len() == 8,
        "exact repair owns all eight original grant cells"
    );
    let connection = observer(path);
    let count = connection.execute(
        "UPDATE approval_grants SET execution_id = ?1, interrupt_id = ?2, task_id = ?3, thread_id = ?4, thread_generation = ?5, task_parking_revision = ?6, consumed = ?7, record = ?8 WHERE execution_id = ?9 AND interrupt_id = ?10",
        rusqlite::params![&original[0], &original[1], &original[2], &original[3], &original[4], &original[5], &original[6], &original[7], &original[0], &original[1]],
    ).unwrap();
    assert!(
        count == 1,
        "exact test-owned repair restores only its existing original grant"
    );
    close_observer(connection);
}

type Usage = (usize, usize);
type UsagePair = (Usage, Usage);
fn usage(budget: &PermissionEvidenceBudget) -> Usage {
    let usage = budget.usage();
    (usage.encoded_bytes(), usage.structural_nodes())
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    OrdinaryNeighbor,
    Matched,
    ConsumedDrift,
}
struct Stage {
    resource_calls: usize,
    combined_calls: usize,
    resource_usages: [Option<UsagePair>; 2],
    combined_usages: [Option<UsagePair>; 2],
    c2: Option<Result<PermissionView, PermissionError>>,
    hook_before: Option<RawNine>,
    hook_after: Option<RawNine>,
}
impl Stage {
    fn new() -> Self {
        Self {
            resource_calls: 0,
            combined_calls: 0,
            resource_usages: [None; 2],
            combined_usages: [None; 2],
            c2: None,
            hook_before: None,
            hook_after: None,
        }
    }
}

// All actual ports forward the same store and evidence account. Only the
// Accept+Some observation is moved into a finite slot before the prewrite stop.
struct CaptureStore {
    inner: Arc<SqliteStore>,
    directory: PathBuf,
    case: Case,
    original_grant: GrantRecord,
    stage: Mutex<Stage>,
}
impl Store for CaptureStore {
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
    fn admit(&self, thread: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
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
impl ApprovalStore for CaptureStore {
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
                "this fixed public attempt has at most two external resource calls"
            );
            stage.resource_calls += 1;
            index
        };
        // The second external resource read has no inner transaction open yet.
        // This is explicit owned drift, never a lawful acceptance or new row.
        if index == 1 && self.case == Case::ConsumedDrift {
            let connection = observer(&self.directory);
            let raw_before = raw_nine(&connection);
            let mut changed = self.original_grant.clone();
            assert!(
                !changed.consumed,
                "the exact actual original grant is initially unconsumed"
            );
            changed.consumed = true;
            let record = serde_json::to_string(&changed).unwrap();
            assert!(
                record.len() <= 2_048,
                "the complete real tiny grant has a finite serialization bound"
            );
            let count = connection.execute(
                "UPDATE approval_grants SET consumed = 1, record = ?1 WHERE execution_id = ?2 AND interrupt_id = ?3 AND consumed = 0",
                rusqlite::params![record, changed.execution_id.as_str(), changed.interrupt_id.as_str()],
            ).unwrap();
            assert!(
                count == 1,
                "the drift changes exactly the real original grant's consumed pair"
            );
            let raw_after = raw_nine(&connection);
            close_observer(connection);
            let mut stage = self.stage.lock().unwrap();
            stage.hook_before = Some(raw_before);
            stage.hook_after = Some(raw_after);
        }
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
                "this fixed public attempt has at most two combined calls"
            );
            stage.combined_calls += 1;
            index
        };
        let before = usage(budget);
        let is_c2 = selector.purpose() == PermissionReadPurpose::Accept
            && selector.expected_view().is_some();
        let result = self.inner.permission_view(id, fence, selector, budget);
        let after = usage(budget);
        let mut stage = self.stage.lock().unwrap();
        stage.combined_usages[index] = Some((before, after));
        if is_c2 {
            assert!(
                stage.c2.is_none(),
                "the fixed attempt captures exactly one owned second-read result"
            );
            stage.c2 = Some(result);
            Err(PermissionError::UnsupportedProfile)
        } else {
            result
        }
    }
}

struct Captured {
    case: Case,
    stage: Stage,
    facade: Option<Result<StoredExecution, PermissionError>>,
    before_public: Result<PermissionView, PermissionError>,
    historical_public: Result<PermissionView, PermissionError>,
    before: ApprovalView,
    historical: ApprovalView,
    before_ordinary: ThreadView,
    after_ordinary: ThreadView,
    after: ApprovalView,
    before_journal: Vec<JournalEntry>,
    after_journal: Vec<JournalEntry>,
    raw_original: RawNine,
    raw_attempt_after: RawNine,
    raw_historical_before: RawNine,
    raw_historical_after: RawNine,
    raw_restored: RawNine,
    pending: Vec<SensitivePendingApproval>,
    request: ApprovalAcceptanceRequest,
    stage_policy_calls: usize,
    constraint_calls: usize,
}

// No subject Result or stage presence/count is asserted until this complete
// genuine original/next-writer retirement returns all owned memory witnesses.
fn capture_after_normal_retirement(case: Case) -> Captured {
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
    let proxy = Arc::new(CaptureStore {
        inner: store.clone(),
        directory: directory.0.clone(),
        case,
        original_grant: grant.clone(),
        stage: Mutex::new(Stage::new()),
    });
    let policy_before = policy_calls.load(Ordering::SeqCst);
    let facade = if case == Case::OrdinaryNeighbor {
        None
    } else {
        Some(accept_permission_human(
            proxy.clone(),
            authority.clone(),
            catalog.clone(),
            &execution_id,
            read_fence,
            request.clone(),
        ))
    };
    let policy_after = policy_calls.load(Ordering::SeqCst);
    let raw_attempt_after = read_raw(&directory.0);
    // A separate current ordinary None read proves the historical row is legal.
    // On the bootstrap this reads the original row: no drift was reached.
    let raw_historical_before = read_raw(&directory.0);
    let historical_public = {
        let mut budget = PermissionEvidenceBudget::new();
        read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution_id,
            read_fence,
            &context,
            PermissionReadPurpose::Accept,
            None,
            &mut budget,
        )
    };
    let raw_historical_after = read_raw(&directory.0);
    let historical = store.approval_view(&execution_id).unwrap().unwrap();
    let stage = {
        let mut stage = proxy.stage.lock().unwrap();
        std::mem::replace(&mut *stage, Stage::new())
    };
    // Restore only when the actual second-resource hook ran. The missing route
    // performs no fault or repair write and is still a feature failure.
    if stage.hook_after.is_some() {
        restore_grant(&directory.0, &raw_original.tables[4][0]);
    }
    let raw_restored = read_raw(&directory.0);
    if stage.hook_after.is_some() {
        assert!(raw_restored.tables[4] == raw_original.tables[4], "a proven owned drift repair restores its complete original grant before ordinary AFTER");
    }
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(proxy);
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
        "all actual original service/proxy/catalog/application/store aliases retire normally"
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
        "a real next writer reads the full ACTUALLY captured restored AFTER and closes normally"
    );
    let mut expected_next_raw = raw_restored.clone();
    expected_next_raw.tables[0][0][3] = Value::Blob(next_owner.to_be_bytes().to_vec());
    assert!(raw_next == expected_next_raw, "all ACTUAL AFTER raw cells survive real next ownership with only the global metadata epoch changed");
    let constraint_count = constraint_calls.load(Ordering::SeqCst);
    assert!(
        native_calls.load(Ordering::SeqCst) == 0,
        "no staged acceptance read or healthy neighbor invokes native code"
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
        "all stores and DTO aliases close before the fixture directory is disposed"
    );
    Captured {
        case,
        stage,
        facade,
        before_public,
        historical_public,
        before,
        historical,
        before_ordinary,
        after_ordinary,
        after,
        before_journal,
        after_journal,
        raw_original,
        raw_attempt_after,
        raw_historical_before,
        raw_historical_after,
        raw_restored,
        pending,
        request,
        stage_policy_calls: policy_after.checked_sub(policy_before).unwrap(),
        constraint_calls: constraint_count,
    }
}

fn assert_projection(
    view: &PermissionView,
    approval: &ApprovalView,
    purpose: PermissionReadPurpose,
) {
    assert!(view.resource().thread == approval.thread && view.resource().execution == approval.execution && view.thread() == &approval.thread && view.execution() == &approval.execution && view.grants() == approval.grants.as_slice() && view.acceptances() == approval.acceptances.as_slice(), "the real complete public projection matches actual retained resource and old authority rows");
    assert!(
        view.permission_acceptances().is_empty()
            && view.permission_decisions().is_empty()
            && view.permission_uses().is_empty()
            && view.creator_acceptances().is_empty()
            && view.referenced_grants().is_empty()
            && view.referenced_acceptances().is_empty()
            && view.journal_entries().is_empty(),
        "all seven new/reference/journal graph sets remain empty in this legacy-only read"
    );
    let receipt = view.receipt();
    let usage = receipt.usage_at_capture();
    assert!(
        receipt.purpose() == purpose
            && usage.encoded_bytes() > 0
            && usage.structural_nodes() > 0
            && usage.encoded_bytes() <= MAX_EVIDENCE_BYTES
            && usage.structural_nodes() <= MAX_EVIDENCE_NODES,
        "the receipt is a bounded nonauthoritative fact for the actual reader purpose"
    );
}
fn assert_outside_genuine_controls(captured: &Captured) {
    assert!(captured.before_public.is_ok() && captured.historical_public.is_ok(), "independent installed ordinary None readers remain legal controls after complete retirement");
    assert_projection(
        captured.before_public.as_ref().unwrap(),
        &captured.before,
        PermissionReadPurpose::Pending,
    );
    assert_projection(
        captured.historical_public.as_ref().unwrap(),
        &captured.historical,
        PermissionReadPurpose::Accept,
    );
    assert!(
        captured.raw_historical_before == captured.raw_historical_after,
        "the separate structural historical control changes no raw cell"
    );
    assert!(
        validate_snapshot(&captured.before.execution.snapshot).is_ok()
            && captured.before.grants.len() == 1
            && captured.pending.len() == 1
            && captured.before.acceptances.is_empty()
            && captured.before.execution.snapshot.definition.batches.len() == 1
            && captured.before.execution.snapshot.tasks.len() == 1,
        "the complete original parked native witness is independently lawful"
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
        "the retained full human request uses exactly the genuine live token and current binding"
    );
    if let Some(hook_before) = &captured.stage.hook_before {
        let hook_after = captured.stage.hook_after.as_ref().unwrap();
        let mut expected = hook_before.clone();
        let actual_record = match &hook_before.tables[4][0][7] {
            Value::Text(record) => record,
            _ => panic!("actual hook grant record must retain its typed TEXT cell"),
        };
        let mut changed: GrantRecord = serde_json::from_str(actual_record).unwrap();
        changed.consumed = true;
        expected.tables[4][0][6] = Value::Integer(1);
        expected.tables[4][0][7] = Value::Text(serde_json::to_string(&changed).unwrap());
        assert!(captured.case == Case::ConsumedDrift && hook_after == &expected, "a reached drift is precisely the owned consumed pair and remains structurally readable");
    } else {
        assert!(
            captured.stage.hook_after.is_none(),
            "an unreached drift makes no fixture write and has zero drift credit"
        );
    }
}
fn assert_storage_preserved(captured: &Captured) {
    if let Some(hook_before) = &captured.stage.hook_before {
        assert!(hook_before == &captured.raw_original, "the first actual capture and permitting gate preserve the original cells before the owned second-read hook");
    }
    assert!(
        captured.after.execution.snapshot.owner_generation
            == captured.before.execution.snapshot.owner_generation,
        "the staged subject preserves the captured execution owner"
    );
    let expected_subject = captured
        .stage
        .hook_after
        .as_ref()
        .unwrap_or(&captured.raw_original);
    assert!(
        captured.raw_attempt_after == *expected_subject
            && captured.raw_attempt_after == captured.raw_historical_before,
        "the staged read changes no cell beyond the explicitly captured test-owned drift"
    );
    if captured.stage.hook_after.is_none() {
        assert!(
            captured.historical == captured.before,
            "an unreached drift preserves the complete original historical view"
        );
    }
    assert!(
        captured.before_ordinary == captured.after_ordinary
            && captured.before == captured.after
            && captured.before_journal == captured.after_journal
            && captured.raw_original == captured.raw_restored,
        "the prewrite sentinel and exact conditional repair preserve complete restored storage"
    );
}
fn assert_stage_reached(captured: &Captured) {
    assert!(captured.stage.resource_calls == 2 && captured.stage.combined_calls == 2 && captured.stage_policy_calls == 2 && captured.constraint_calls == 1, "only after the feature primary succeeds do two resource/policy/combined phases and one pure gate get credit");
    assert!(
        matches!(
            &captured.facade,
            Some(Err(PermissionError::UnsupportedProfile))
        ),
        "the capture proxy stops before acceptance persistence with a finite sentinel"
    );
    let ordered = [
        captured.stage.resource_usages[0].unwrap(),
        captured.stage.combined_usages[0].unwrap(),
        captured.stage.resource_usages[1].unwrap(),
        captured.stage.combined_usages[1].unwrap(),
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
            "one actual same account remains monotone and finite across the reached read phases"
        );
        previous = after;
    }
}
fn optional_code(result: &Option<Result<PermissionView, PermissionError>>) -> &'static str {
    match result {
        None => "missing",
        Some(Ok(_)) => "ok",
        Some(Err(error)) => error.code(),
    }
}
fn assert_neighbor(captured: &Captured) {
    assert_outside_genuine_controls(captured);
    assert!(
        captured.case == Case::OrdinaryNeighbor
            && captured.facade.is_none()
            && captured.stage.c2.is_none()
            && captured.stage.resource_calls == 0
            && captured.stage.combined_calls == 0
            && captured.stage_policy_calls == 0
            && captured.constraint_calls == 0,
        "the separately genuine neighbor uses only installed ordinary None readers"
    );
    assert_storage_preserved(captured);
}

#[test]
fn human_always_reaches_actual_accept_matched_view_after_normal_retirement() {
    let neighbor = capture_after_normal_retirement(Case::OrdinaryNeighbor);
    let subject = capture_after_normal_retirement(Case::Matched);
    assert_neighbor(&neighbor);
    assert_outside_genuine_controls(&subject);
    assert!(subject.stage.c2.as_ref().is_some_and(|result| result.is_ok()), "public Always must reach an actual successful second matched Accept view after full retirement; code={}", optional_code(&subject.stage.c2));
    let view = subject.stage.c2.as_ref().unwrap().as_ref().unwrap();
    assert_projection(view, &subject.before, PermissionReadPurpose::Accept);
    assert_stage_reached(&subject);
    assert_storage_preserved(&subject);
}

#[test]
fn human_always_rejects_full_old_grant_drift_at_actual_accept_matched_view() {
    let neighbor = capture_after_normal_retirement(Case::OrdinaryNeighbor);
    let subject = capture_after_normal_retirement(Case::ConsumedDrift);
    assert_neighbor(&neighbor);
    assert_outside_genuine_controls(&subject);
    assert!(subject.stage.c2.as_ref().is_some_and(|result| matches!(result, Err(PermissionError::Store(StoreError::RevisionConflict)))), "public Always must classify actual second matched Accept old-grant drift after full retirement; code={}", optional_code(&subject.stage.c2));
    assert!(subject.stage.hook_before.is_some() && subject.stage.hook_after.is_some() && subject.historical.grants[0].consumed && subject.historical.thread == subject.before.thread && subject.historical.execution == subject.before.execution, "only the reached successful primary credits exact old-row drift with unchanged complete resource");
    assert_stage_reached(&subject);
    assert_storage_preserved(&subject);
}
