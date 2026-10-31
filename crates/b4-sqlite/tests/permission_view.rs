#![cfg(unix)]

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

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const MAX_EVIDENCE_BYTES: usize = 8_388_608;
const MAX_EVIDENCE_NODES: usize = 65_536;
const ORIGINAL_PROPOSAL_DIGEST: &str =
    "ed2d7468c8534a5e7615f93b07898d15b6b843de2d838ebd7ab9c8d2e7fa62e5";

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-permission-view-fixture-{}-{}",
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

struct CountNativeCalls(Arc<AtomicUsize>);

impl NativeEffect for CountNativeCalls {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"fixture": true}),
            writes: vec![],
        }))
    }
}

struct PermittingConstraint(Arc<AtomicUsize>);

impl ToolConstraint for PermittingConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}

fn fence(store: &dyn ApprovalStore, thread_id: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread_id).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}

#[derive(Clone, Copy)]
enum ViewCase {
    Pending,
    ConsumedOnce,
}

struct Captured {
    result: Result<PermissionView, PermissionError>,
    parked_approval: ApprovalView,
    parked_journal: Vec<JournalEntry>,
    pending: Vec<SensitivePendingApproval>,
    accepted_request: Option<ApprovalAcceptanceRequest>,
    accepted_execution: Option<StoredExecution>,
    before_ordinary: ThreadView,
    before_approval: ApprovalView,
    before_journal: Vec<JournalEntry>,
    after_ordinary: ThreadView,
    after_approval: ApprovalView,
    after_journal: Vec<JournalEntry>,
    usage_before: (usize, usize),
    usage_after: (usize, usize),
}

// Both cases use one real required Pure task, one actual Park and one actual
// token. The consumed case first performs a complete lawful legacy Once.
// Neither projection dispatches, invokes a constraint or creates new authority.
fn capture_one_task_view_after_normal_retirement(case: ViewCase) -> Captured {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let constraint_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("human-gate-thread").unwrap();
    let execution_id = ExecutionId::new("human-gate-execution").unwrap();
    let origin = ThreadOrigin {
        application: "human-gate-app".into(),
        tenant: "fixture-tenant".into(),
        principal: "fixture-principal".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/human-gate#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("first").unwrap(),
            effect_id: EffectId::new("first-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 4,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: origin.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    EffectId::new("first-effect").unwrap(),
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
    assert!(registered == definition, "native registration is exact");
    let constraint = Arc::new(PermittingConstraint(constraint_calls.clone()));
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("first-effect").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: constraint.clone(),
            }],
        )
        .unwrap(),
    );
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert!(original_owner == 1, "fresh writer owns generation one");
    assert!(
        store.approval_durable(),
        "SQLite supports old durable approval"
    );
    assert!(
        !store.durable_permissions(),
        "the genuine reader fixture keeps the lasting profile false"
    );
    let thread = store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(origin),
        })
        .unwrap();
    let context = ApprovalContext {
        actor: thread.origin.as_ref().unwrap().clone(),
        policy_revision: "policy-1".into(),
    };
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("human-gate-original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("payload".into(), json!({"fixture": true, "nullable": null}))]),
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
        started.snapshot.revision == 1
            && started.snapshot.status == ExecutionStatus::Running
            && started.snapshot.tasks.len() == 1,
        "real Start initializes the one-task native Definition"
    );
    let ready = &started.snapshot.tasks[0];
    assert!(
        ready.status == TaskStatus::Ready
            && !ready.approved
            && ready.input == started.snapshot.values
            && ready.attempt_id.is_none()
            && ready.result.is_none()
            && ready.failure.is_none()
            && ready.interrupt.is_none(),
        "real Start retains an unapproved Ready task without an outcome"
    );
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let parked = service
        .park(
            &context,
            &execution_id,
            fence(store.as_ref(), &thread_id),
            ready.spec.id.clone(),
            InterruptId::new("first-interrupt").unwrap(),
            json!({"question": "First fixture?"}),
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let parked_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let parked_journal = store.journal(&execution_id).unwrap();
    assert!(
        parked == parked_approval.execution
            && parked.snapshot.revision == 2
            && parked.snapshot.status == ExecutionStatus::WaitingForApproval
            && parked_approval.thread.resource_revision == 3,
        "the real single Park reaches the stable phase"
    );
    assert!(
        parked.snapshot.tasks.len() == 1
            && pending.len() == 1
            && parked_approval.grants.len() == 1
            && parked_approval.acceptances.is_empty(),
        "a single real pending task has one actual grant and no acceptance"
    );
    let task = &parked.snapshot.tasks[0];
    let interrupt = task.interrupt.as_ref().unwrap();
    let grant = &parked_approval.grants[0];
    let projection = &pending[0];
    assert!(
        task.spec == definition.batches[0][0]
            && task.status == TaskStatus::WaitingForApproval
            && !task.approved
            && task.input == parked.snapshot.values
            && task.attempt_id.is_none()
            && task.result.is_none()
            && task.failure.is_none()
            && interrupt.id.as_str() == "first-interrupt"
            && interrupt.prompt == json!({"question": "First fixture?"}),
        "the parked task is the exact genuine native call"
    );
    assert!(
        grant.thread_id == thread_id
            && grant.thread_generation == parked_approval.thread.generation
            && grant.origin == context.actor
            && grant.execution_id == execution_id
            && grant.route == definition.route
            && grant.definition_revision == definition.revision
            && grant.task_id == task.spec.id
            && grant.effect_id == task.spec.effect_id
            && grant.interrupt_id == interrupt.id
            && grant.prompt == interrupt.prompt
            && grant.policy_revision == context.policy_revision
            && grant.proposal_digest == ORIGINAL_PROPOSAL_DIGEST,
        "the real grant has exact incarnation, definition, call and prompt bindings"
    );
    assert!(
        grant.task_parking_revision == 2
            && grant.issued_at_ms == 102
            && grant.expires_at_ms == 1_102
            && !grant.consumed
            && grant.issued_at_ms <= 104
            && 104 < grant.expires_at_ms
            && grant.token == projection.token
            && grant.verifier == token_verifier(&projection.token)
            && grant.resume_key == projection.resume_key
            && projection.task_id == task.spec.id
            && projection.interrupt_id == interrupt.id
            && projection.prompt == interrupt.prompt
            && projection.expires_at_ms == grant.expires_at_ms,
        "the pending token and verifier come from the actual lawful Park"
    );
    let (accepted_request, accepted_execution) = match case {
        ViewCase::Pending => (None, None),
        ViewCase::ConsumedOnce => {
            let request = ApprovalAcceptanceRequest {
                context: context.clone(),
                route: definition.route.clone(),
                definition_revision: definition.revision.clone(),
                turn_id: TurnId::new("human-gate-accepted-turn").unwrap(),
                now_ms: 104,
                decisions: vec![GrantDecision {
                    interrupt_id: projection.interrupt_id.clone(),
                    decision: ApprovalDecision::Once,
                    cancelled: false,
                    token: Some(projection.token.clone()),
                }],
            };
            let accepted = service
                .accept(
                    &execution_id,
                    fence(store.as_ref(), &thread_id),
                    request.clone(),
                )
                .unwrap();
            assert!(
                accepted.snapshot.revision == 3
                    && accepted.snapshot.status == ExecutionStatus::Running
                    && accepted.snapshot.tasks.len() == 1
                    && accepted.snapshot.tasks[0].status == TaskStatus::Ready
                    && accepted.snapshot.tasks[0].approved,
                "a real complete Once acceptance resumes the genuine task"
            );
            (Some(request), Some(accepted))
        }
    };
    let before_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let before_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    assert!(
        before_ordinary.thread == before_approval.thread
            && before_ordinary.execution.as_ref() == Some(&before_approval.execution)
            && before_ordinary.thread.origin.as_ref() == Some(&context.actor),
        "the full actual views retain the genuine actor and selected execution"
    );
    assert!(
        before_approval.execution.snapshot.definition == definition
            && before_approval.execution.snapshot.tasks.len() == 1,
        "the reader receives the complete current native Definition"
    );
    let read_fence = ApprovalFence {
        execution: (&before_approval.execution.snapshot).into(),
        resource_revision: before_approval.thread.resource_revision,
    };
    let actual_task_id = &before_approval.execution.snapshot.tasks[0].spec.id;
    let (result, usage_before, usage_after) = {
        let mut budget = PermissionEvidenceBudget::new();
        let before = budget.usage();
        let result = match case {
            ViewCase::Pending => read_permission_view(
                &store,
                authority.as_ref(),
                catalog.as_ref(),
                &execution_id,
                read_fence,
                &context,
                PermissionReadPurpose::Pending,
                None,
                &mut budget,
            ),
            ViewCase::ConsumedOnce => read_permission_view(
                store.as_ref(),
                authority.as_ref(),
                catalog.as_ref(),
                &execution_id,
                read_fence,
                &context,
                PermissionReadPurpose::Dispatch,
                Some(actual_task_id),
                &mut budget,
            ),
        };
        let after = budget.usage();
        (
            result,
            (before.encoded_bytes(), before.structural_nodes()),
            (after.encoded_bytes(), after.structural_nodes()),
        )
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(thread);
    drop(admitted);
    drop(started);
    drop(parked);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(constraint);
    drop(application);
    assert!(
        Arc::strong_count(&store) == 1,
        "every original writer alias retires normally"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_approval = next.approval_view(&execution_id).unwrap().unwrap();
    let next_journal = next.journal(&execution_id).unwrap();
    next.readiness().unwrap();
    drop(next);
    assert!(
        next_owner == original_owner.checked_add(1).unwrap(),
        "a genuine next writer advances the actual owner generation"
    );
    assert!(
        next_ordinary == after_ordinary
            && next_approval == after_approval
            && next_journal == after_journal,
        "the next writer reads the full ACTUAL AFTER state"
    );
    assert!(
        next_approval.execution.snapshot.owner_generation == original_owner,
        "normal reopening preserves the retained Snapshot owner"
    );
    assert!(
        native_calls.load(Ordering::SeqCst) == 0 && constraint_calls.load(Ordering::SeqCst) == 0,
        "the projection invokes no native callback or constraint"
    );
    drop(native_calls);
    drop(constraint_calls);
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(directory);
    Captured {
        result,
        parked_approval,
        parked_journal,
        pending,
        accepted_request,
        accepted_execution,
        before_ordinary,
        before_approval,
        before_journal,
        after_ordinary,
        after_approval,
        after_journal,
        usage_before,
        usage_after,
    }
}

// These checks follow each parent's own availability assertion. The borrowed
// receipt is projection data only and never reconstructs an evidence account.
fn assert_complete_projection_and_retained_state(
    captured: &Captured,
    purpose: PermissionReadPurpose,
) {
    let view = captured.result.as_ref().unwrap();
    assert!(
        view.resource().thread == captured.before_ordinary.thread
            && view.resource().execution == captured.before_approval.execution
            && view.thread() == &captured.before_ordinary.thread
            && view.execution() == &captured.before_approval.execution,
        "the complete projected resource equals the actually captured reader BEFORE"
    );
    assert!(
        view.grants() == captured.before_approval.grants.as_slice()
            && view.acceptances() == captured.before_approval.acceptances.as_slice(),
        "the full selected old rows equal the genuinely captured old approval view"
    );
    assert!(
        view.permission_acceptances().is_empty()
            && view.permission_decisions().is_empty()
            && view.permission_uses().is_empty()
            && view.creator_acceptances().is_empty()
            && view.referenced_grants().is_empty()
            && view.referenced_acceptances().is_empty()
            && view.journal_entries().is_empty(),
        "the legacy-only projection has no new or referenced authority graph"
    );
    let receipt = view.receipt();
    let receipt_usage = receipt.usage_at_capture();
    assert!(
        receipt.purpose() == purpose,
        "the receipt retains only the actual read purpose"
    );
    assert!(
        captured.usage_before.0 <= receipt_usage.encoded_bytes()
            && captured.usage_before.1 <= receipt_usage.structural_nodes()
            && receipt_usage.encoded_bytes() <= captured.usage_after.0
            && receipt_usage.structural_nodes() <= captured.usage_after.1
            && captured.usage_before.0 <= captured.usage_after.0
            && captured.usage_before.1 <= captured.usage_after.1
            && captured.usage_after.0 <= MAX_EVIDENCE_BYTES
            && captured.usage_after.1 <= MAX_EVIDENCE_NODES,
        "the non-authoritative receipt stays within the same finite monotonic account"
    );
    assert!(
        captured.before_ordinary == captured.after_ordinary
            && captured.before_approval == captured.after_approval
            && captured.before_journal == captured.after_journal,
        "the projection preserves the complete captured storage state"
    );
}

#[test]
fn permission_view_projects_one_real_pending_grant_after_normal_retirement() {
    let captured = capture_one_task_view_after_normal_retirement(ViewCase::Pending);
    assert!(
        captured.result.is_ok(),
        "one real pending permission view must be available: {}",
        captured
            .result
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert_complete_projection_and_retained_state(&captured, PermissionReadPurpose::Pending);
    assert!(
        captured.before_approval == captured.parked_approval
            && captured.before_journal == captured.parked_journal
            && captured.before_approval.execution.snapshot.revision == 2
            && captured.before_approval.execution.snapshot.status
                == ExecutionStatus::WaitingForApproval
            && captured.before_approval.thread.resource_revision == 3
            && captured.before_approval.grants.len() == 1
            && !captured.before_approval.grants[0].consumed
            && captured.before_approval.acceptances.is_empty()
            && captured.pending.len() == 1
            && captured.accepted_request.is_none()
            && captured.accepted_execution.is_none(),
        "the pending subject retains one actual unconsumed grant and no acceptance"
    );
}

#[test]
fn permission_view_projects_one_real_consumed_once_and_old_acceptance_after_normal_retirement() {
    let captured = capture_one_task_view_after_normal_retirement(ViewCase::ConsumedOnce);
    assert!(
        captured.result.is_ok(),
        "one real consumed Once permission view must be available: {}",
        captured
            .result
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert_complete_projection_and_retained_state(&captured, PermissionReadPurpose::Dispatch);
    let accepted = captured.accepted_execution.as_ref().unwrap();
    let request = captured.accepted_request.as_ref().unwrap();
    let parked = &captured.parked_approval;
    let before = &captured.before_approval;
    assert!(
        before.execution == *accepted
            && accepted.snapshot.revision == 3
            && accepted.snapshot.status == ExecutionStatus::Running
            && accepted.snapshot.turn_id == request.turn_id
            && accepted.snapshot.turn_id.as_str() == "human-gate-accepted-turn"
            && accepted.snapshot.turn_history.len() == 2
            && accepted.snapshot.turn_history[0].as_str() == "human-gate-original-turn"
            && accepted.snapshot.turn_history[1] == request.turn_id
            && accepted.snapshot.definition == parked.execution.snapshot.definition
            && accepted.snapshot.values == parked.execution.snapshot.values
            && accepted.snapshot.attempts_used == 0
            && accepted.snapshot.attempt_history.is_empty()
            && accepted.snapshot.output.is_none()
            && accepted.snapshot.failure.is_none()
            && before.thread.resource_revision == 4,
        "the separate real Once acceptance retains complete lawful Resume facts"
    );
    assert!(
        accepted.snapshot.tasks.len() == 1,
        "real Once retains the exact task set"
    );
    let task = &accepted.snapshot.tasks[0];
    let original_task = &parked.execution.snapshot.tasks[0];
    assert!(
        task.spec == original_task.spec
            && task.status == TaskStatus::Ready
            && task.approved
            && task.input == original_task.input
            && task.interrupt == original_task.interrupt
            && task.attempt_id.is_none()
            && task.result.is_none()
            && task.failure.is_none(),
        "the approved Ready call retains its original input and prompt without dispatch"
    );
    assert!(
        before.grants.len() == 1
            && before.grants[0].consumed
            && before.acceptances.len() == 1
            && captured.pending.len() == 1
            && request.decisions.len() == 1,
        "the public Once creates one genuine consumed grant and one old acceptance"
    );
    let grant = &before.grants[0];
    let old_grant = &parked.grants[0];
    let acceptance = &before.acceptances[0];
    let submitted = &request.decisions[0];
    assert!(
        grant.thread_id == old_grant.thread_id
            && grant.thread_generation == old_grant.thread_generation
            && grant.origin == old_grant.origin
            && grant.execution_id == old_grant.execution_id
            && grant.route == old_grant.route
            && grant.definition_revision == old_grant.definition_revision
            && grant.task_id == old_grant.task_id
            && grant.effect_id == old_grant.effect_id
            && grant.interrupt_id == old_grant.interrupt_id
            && grant.resume_key == old_grant.resume_key
            && grant.task_parking_revision == old_grant.task_parking_revision
            && grant.policy_revision == old_grant.policy_revision
            && grant.proposal_digest == old_grant.proposal_digest
            && grant.prompt == old_grant.prompt
            && grant.token == old_grant.token
            && grant.verifier == old_grant.verifier
            && grant.issued_at_ms == old_grant.issued_at_ms
            && grant.expires_at_ms == old_grant.expires_at_ms,
        "consumption preserves every other field of the real original grant"
    );
    assert!(
        request.context.actor == before.thread.origin.as_ref().unwrap().clone()
            && request.context.policy_revision == grant.policy_revision
            && request.route == grant.route
            && request.definition_revision == grant.definition_revision
            && request.now_ms == 104
            && submitted.interrupt_id == grant.interrupt_id
            && submitted.decision == ApprovalDecision::Once
            && !submitted.cancelled
            && submitted.token.as_ref() == Some(&grant.token),
        "the complete accepted request uses the actual original token and actor"
    );
    assert!(
        acceptance.execution_id == accepted.snapshot.execution_id
            && acceptance.thread_id == before.thread.id
            && acceptance.thread_generation == before.thread.generation
            && acceptance.context == request.context
            && acceptance.previous_revision == parked.execution.snapshot.revision
            && acceptance.previous_revision == 2
            && acceptance.turn_id == request.turn_id
            && acceptance.definition_revision == accepted.snapshot.definition.revision
            && acceptance.accepted_at_ms == request.now_ms
            && acceptance.decisions.len() == 1,
        "the old acceptance is the actual committed Once audit row"
    );
    let decision = &acceptance.decisions[0];
    assert!(
        decision.task_id == task.spec.id
            && decision.interrupt_id == grant.interrupt_id
            && decision.decision == ApprovalDecision::Once
            && decision.proposal_digest == ORIGINAL_PROPOSAL_DIGEST
            && decision.task_parking_revision == 2
            && captured.parked_journal.len() == 3
            && captured.before_journal.len() == 4
            && captured.before_journal.last().is_some_and(
                |entry| entry.revision == 3 && entry.status == ExecutionStatus::Running
            ),
        "the actual old decision and fourth journal entry match the real Resume"
    );
}

const QUALIFIER_CALL_DIGEST: &str =
    "7760372027e7209c38107597c5611b9fcc2aafcb35960a266ca9da36750a9983";
const HEADER_TURN: &str = "header-fixture-turn";
const HEADER_PERMISSION: &str =
    "b4pd_0000000000000000000000000000000000000000000000000000000000000000";
const HEADER_USE: &str = "b4pu_1111111111111111111111111111111111111111111111111111111111111111";
const HEADER_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const ZERO_GENERATION: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const ONE_GENERATION: &str = "1111111111111111111111111111111111111111111111111111111111111111";

#[derive(Clone, Copy)]
enum ReaderQualifierCase {
    DecisionGeneration,
    UseGeneration,
    SharedThirdPhase,
}

struct CountingReadAuthority(Arc<AtomicUsize>);

impl ApprovalAuthority for CountingReadAuthority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// Complete typed cells of the three bounded, fixture-owned raw table sets.
// These SQL-only header fixtures never represent accepted permission authority.
#[derive(PartialEq)]
struct HeaderRows {
    acceptances: Vec<Vec<rusqlite::types::Value>>,
    decisions: Vec<Vec<rusqlite::types::Value>>,
    uses: Vec<Vec<rusqlite::types::Value>>,
}

fn capture_header_cells(
    connection: &rusqlite::Connection,
    sql: &str,
    columns: usize,
) -> Vec<Vec<rusqlite::types::Value>> {
    assert!(matches!(columns, 6 | 8 | 10), "closed header column counts");
    let mut statement = connection.prepare(sql).unwrap();
    let mut cursor = statement.query([]).unwrap();
    let mut result = Vec::new();
    for _ in 0..2 {
        let Some(row) = cursor.next().unwrap() else {
            break;
        };
        let mut cells = Vec::new();
        for column in 0..columns {
            cells.push(row.get::<_, rusqlite::types::Value>(column).unwrap());
        }
        result.push(cells);
    }
    assert!(
        result.len() <= 1,
        "the closed fixture owns at most one row per table"
    );
    result
}

fn capture_header_rows(connection: &rusqlite::Connection) -> HeaderRows {
    HeaderRows {
        acceptances: capture_header_cells(connection,
            "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8),
        decisions: capture_header_cells(connection,
            "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6),
        uses: capture_header_cells(connection,
            "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10),
    }
}

fn insert_header_fixture(
    connection: &mut rusqlite::Connection,
    before: &ApprovalView,
    case: ReaderQualifierCase,
    faulty: bool,
    different_generation: &str,
) {
    assert!(
        before.execution.snapshot.revision == 2
            && before.execution.snapshot.tasks.len() == 1
            && matches!(
                case,
                ReaderQualifierCase::DecisionGeneration | ReaderQualifierCase::UseGeneration
            ),
        "header setup uses the exact real single-Park witness"
    );
    let turn = TurnId::new(HEADER_TURN).unwrap();
    let actual_generation = before.thread.generation.as_str();
    assert!(
        different_generation != actual_generation,
        "the fault incarnation is provably different"
    );
    let previous = 1_u64.to_be_bytes();
    let committed = 2_u64.to_be_bytes();
    let transaction = connection.transaction().unwrap();
    let inserted = transaction.execute(
        "INSERT INTO permission_acceptances (execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, '{}')",
        rusqlite::params![before.execution.snapshot.execution_id.as_str(), turn.as_str(), before.thread.id.as_str(), actual_generation, previous.as_slice(), committed.as_slice()],
    ).unwrap();
    assert!(
        inserted == 1,
        "the fixture inserts exactly one scalar A row"
    );
    let decision_generation = if faulty && matches!(case, ReaderQualifierCase::DecisionGeneration) {
        different_generation
    } else {
        actual_generation
    };
    let inserted = transaction.execute(
        "INSERT INTO permission_decisions (permission_id, thread_id, thread_generation, scope_digest, record_version, record) VALUES (?1, ?2, ?3, ?4, 1, '{}')",
        rusqlite::params![HEADER_PERMISSION, before.thread.id.as_str(), decision_generation, HEADER_DIGEST],
    ).unwrap();
    assert!(
        inserted == 1,
        "the fixture inserts exactly one scalar D row"
    );
    if matches!(case, ReaderQualifierCase::UseGeneration) {
        let use_generation = if faulty {
            different_generation
        } else {
            actual_generation
        };
        let inserted = transaction.execute(
            "INSERT INTO permission_uses (execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, '{}')",
            rusqlite::params![before.execution.snapshot.execution_id.as_str(), before.execution.snapshot.tasks[0].spec.id.as_str(), HEADER_USE, before.thread.id.as_str(), use_generation, HEADER_PERMISSION, turn.as_str(), committed.as_slice()],
        ).unwrap();
        assert!(
            inserted == 1,
            "the fixture inserts exactly one FK-linked scalar U row"
        );
    }
    transaction.commit().unwrap();
}

fn remove_header_fixture(
    connection: &mut rusqlite::Connection,
    before: &ApprovalView,
    case: ReaderQualifierCase,
) {
    let transaction = connection.transaction().unwrap();
    let removed_uses = transaction
        .execute(
            "DELETE FROM permission_uses WHERE execution_id = ?1 AND task_id = ?2 AND use_id = ?3",
            rusqlite::params![
                before.execution.snapshot.execution_id.as_str(),
                before.execution.snapshot.tasks[0].spec.id.as_str(),
                HEADER_USE
            ],
        )
        .unwrap();
    assert!(
        removed_uses == usize::from(matches!(case, ReaderQualifierCase::UseGeneration)),
        "cleanup removes only the owned U before its D"
    );
    let removed_decisions = transaction
        .execute(
            "DELETE FROM permission_decisions WHERE permission_id = ?1 AND thread_id = ?2",
            rusqlite::params![HEADER_PERMISSION, before.thread.id.as_str()],
        )
        .unwrap();
    let removed_acceptances = transaction
        .execute(
            "DELETE FROM permission_acceptances WHERE execution_id = ?1 AND turn_id = ?2",
            rusqlite::params![before.execution.snapshot.execution_id.as_str(), HEADER_TURN],
        )
        .unwrap();
    assert!(
        removed_decisions == 1 && removed_acceptances == 1,
        "cleanup removes exactly the owned D and A"
    );
    transaction.commit().unwrap();
}

struct HeaderFixtureCapture {
    original: HeaderRows,
    control_before: HeaderRows,
    control_after: HeaderRows,
    control_restored: HeaderRows,
    fault_before: HeaderRows,
    fault_after: HeaderRows,
    restored: HeaderRows,
    actual_generation: String,
    different_generation: String,
}

struct ReaderQualifierCapture {
    results: Vec<Result<PermissionView, PermissionError>>,
    fresh_digest: Result<String, PermissionError>,
    sticky_digest: Option<Result<String, PermissionError>>,
    policy_counts: Vec<usize>,
    usage: Vec<(usize, usize)>,
    headers: Option<HeaderFixtureCapture>,
    before_ordinary: ThreadView,
    before_approval: ApprovalView,
    before_journal: Vec<JournalEntry>,
    after_ordinary: ThreadView,
    after_approval: ApprovalView,
    after_journal: Vec<JournalEntry>,
}

// A finite copy of the reviewed genuine Pending setup; all previous helpers
// remain byte-exact. The five captured calls below are bounded by the three
// cases: two header reads, or three shared-account reads, plus digest controls.
fn capture_reader_qualifier_after_normal_retirement(
    case: ReaderQualifierCase,
) -> ReaderQualifierCapture {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let constraint_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("human-gate-thread").unwrap();
    let execution_id = ExecutionId::new("human-gate-execution").unwrap();
    let origin = ThreadOrigin {
        application: "human-gate-app".into(),
        tenant: "fixture-tenant".into(),
        principal: "fixture-principal".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/human-gate#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("first").unwrap(),
            effect_id: EffectId::new("first-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 4,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: origin.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    EffectId::new("first-effect").unwrap(),
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
    assert!(registered == definition, "native registration is exact");
    let constraint = Arc::new(PermittingConstraint(constraint_calls.clone()));
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("first-effect").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: constraint.clone(),
            }],
        )
        .unwrap(),
    );
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert!(original_owner == 1, "fresh writer owns generation one");
    assert!(
        store.approval_durable(),
        "SQLite supports old durable approval"
    );
    assert!(
        !store.durable_permissions(),
        "the genuine reader fixture keeps the lasting profile false"
    );
    let thread = store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(origin),
        })
        .unwrap();
    let context = ApprovalContext {
        actor: thread.origin.as_ref().unwrap().clone(),
        policy_revision: "policy-1".into(),
    };
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("human-gate-original-turn").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("payload".into(), json!({"fixture": true, "nullable": null}))]),
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
        started.snapshot.revision == 1
            && started.snapshot.status == ExecutionStatus::Running
            && started.snapshot.tasks.len() == 1,
        "real Start initializes the one-task native Definition"
    );
    let ready = &started.snapshot.tasks[0];
    assert!(
        ready.status == TaskStatus::Ready
            && !ready.approved
            && ready.input == started.snapshot.values
            && ready.attempt_id.is_none()
            && ready.result.is_none()
            && ready.failure.is_none()
            && ready.interrupt.is_none(),
        "real Start retains an unapproved Ready task without an outcome"
    );
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let parked = service
        .park(
            &context,
            &execution_id,
            fence(store.as_ref(), &thread_id),
            ready.spec.id.clone(),
            InterruptId::new("first-interrupt").unwrap(),
            json!({"question": "First fixture?"}),
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let parked_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let parked_journal = store.journal(&execution_id).unwrap();
    assert!(
        parked == parked_approval.execution
            && parked.snapshot.revision == 2
            && parked.snapshot.status == ExecutionStatus::WaitingForApproval
            && parked_approval.thread.resource_revision == 3,
        "the real single Park reaches the stable phase"
    );
    assert!(
        parked.snapshot.tasks.len() == 1
            && pending.len() == 1
            && parked_approval.grants.len() == 1
            && parked_approval.acceptances.is_empty(),
        "a single real pending task has one actual grant and no acceptance"
    );
    let task = &parked.snapshot.tasks[0];
    let interrupt = task.interrupt.as_ref().unwrap();
    let grant = &parked_approval.grants[0];
    let projection = &pending[0];
    assert!(
        task.spec == definition.batches[0][0]
            && task.status == TaskStatus::WaitingForApproval
            && !task.approved
            && task.input == parked.snapshot.values
            && task.attempt_id.is_none()
            && task.result.is_none()
            && task.failure.is_none()
            && interrupt.id.as_str() == "first-interrupt"
            && interrupt.prompt == json!({"question": "First fixture?"}),
        "the parked task is the exact genuine native call"
    );
    assert!(
        grant.thread_id == thread_id
            && grant.thread_generation == parked_approval.thread.generation
            && grant.origin == context.actor
            && grant.execution_id == execution_id
            && grant.route == definition.route
            && grant.definition_revision == definition.revision
            && grant.task_id == task.spec.id
            && grant.effect_id == task.spec.effect_id
            && grant.interrupt_id == interrupt.id
            && grant.prompt == interrupt.prompt
            && grant.policy_revision == context.policy_revision
            && grant.proposal_digest == ORIGINAL_PROPOSAL_DIGEST,
        "the real grant has exact incarnation, definition, call and prompt bindings"
    );
    assert!(
        grant.task_parking_revision == 2
            && grant.issued_at_ms == 102
            && grant.expires_at_ms == 1_102
            && !grant.consumed
            && grant.issued_at_ms <= 104
            && 104 < grant.expires_at_ms
            && grant.token == projection.token
            && grant.verifier == token_verifier(&projection.token)
            && grant.resume_key == projection.resume_key
            && projection.task_id == task.spec.id
            && projection.interrupt_id == interrupt.id
            && projection.prompt == interrupt.prompt
            && projection.expires_at_ms == grant.expires_at_ms,
        "the pending token and verifier come from the actual lawful Park"
    );
    let before_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let before_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    assert!(
        before_approval == parked_approval
            && before_ordinary.thread == before_approval.thread
            && before_ordinary.execution.as_ref() == Some(&before_approval.execution)
            && before_approval.thread.origin.as_ref() == Some(&context.actor),
        "the reader starts from the full genuine parked resource"
    );
    let read_fence = ApprovalFence {
        execution: (&before_approval.execution.snapshot).into(),
        resource_revision: before_approval.thread.resource_revision,
    };
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let read_authority = Arc::new(CountingReadAuthority(policy_calls.clone()));
    let actual_task = &before_approval.execution.snapshot.tasks[0];
    let fresh_digest = {
        let mut budget = PermissionEvidenceBudget::new();
        permission_call_digest(actual_task, &mut budget)
    };
    let mut results = Vec::new();
    let mut policy_counts = Vec::new();
    let mut usage = Vec::new();
    let (headers, sticky_digest) = match case {
        ReaderQualifierCase::DecisionGeneration | ReaderQualifierCase::UseGeneration => {
            let mut raw = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
            raw.pragma_update(None, "foreign_keys", "ON").unwrap();
            let foreign_keys: i64 = raw
                .pragma_query_value(None, "foreign_keys", |row| row.get(0))
                .unwrap();
            assert!(
                foreign_keys == 1,
                "raw negative setup preserves the real FK checks"
            );
            let original = capture_header_rows(&raw);
            assert!(
                original.acceptances.is_empty()
                    && original.decisions.is_empty()
                    && original.uses.is_empty(),
                "new scalar tables begin genuinely empty"
            );
            let actual_generation = before_approval.thread.generation.as_str().to_owned();
            let different_generation = if actual_generation == ZERO_GENERATION {
                ONE_GENERATION.to_owned()
            } else {
                ZERO_GENERATION.to_owned()
            };
            insert_header_fixture(
                &mut raw,
                &before_approval,
                case,
                false,
                &different_generation,
            );
            let control_before = capture_header_rows(&raw);
            {
                let mut budget = PermissionEvidenceBudget::new();
                let result = read_permission_view(
                    store.as_ref(),
                    read_authority.as_ref(),
                    catalog.as_ref(),
                    &execution_id,
                    read_fence,
                    &context,
                    PermissionReadPurpose::Pending,
                    None,
                    &mut budget,
                );
                let used = budget.usage();
                results.push(result);
                usage.push((used.encoded_bytes(), used.structural_nodes()));
                policy_counts.push(policy_calls.load(Ordering::SeqCst));
            }
            let control_after = capture_header_rows(&raw);
            remove_header_fixture(&mut raw, &before_approval, case);
            let control_restored = capture_header_rows(&raw);
            assert!(
                control_restored == original,
                "control cleanup restores exactly the original empty tables"
            );
            insert_header_fixture(
                &mut raw,
                &before_approval,
                case,
                true,
                &different_generation,
            );
            let fault_before = capture_header_rows(&raw);
            {
                let mut budget = PermissionEvidenceBudget::new();
                let result = read_permission_view(
                    store.as_ref(),
                    read_authority.as_ref(),
                    catalog.as_ref(),
                    &execution_id,
                    read_fence,
                    &context,
                    PermissionReadPurpose::Pending,
                    None,
                    &mut budget,
                );
                let used = budget.usage();
                results.push(result);
                usage.push((used.encoded_bytes(), used.structural_nodes()));
                policy_counts.push(policy_calls.load(Ordering::SeqCst));
            }
            let fault_after = capture_header_rows(&raw);
            remove_header_fixture(&mut raw, &before_approval, case);
            let restored = capture_header_rows(&raw);
            assert!(
                restored == original,
                "fault cleanup restores exactly the original empty tables before ordinary reads"
            );
            drop(raw);
            (
                Some(HeaderFixtureCapture {
                    original,
                    control_before,
                    control_after,
                    control_restored,
                    fault_before,
                    fault_after,
                    restored,
                    actual_generation,
                    different_generation,
                }),
                None,
            )
        }
        ReaderQualifierCase::SharedThirdPhase => {
            let sticky = {
                let mut budget = PermissionEvidenceBudget::new();
                for _ in 0..3 {
                    let result = read_permission_view(
                        store.as_ref(),
                        read_authority.as_ref(),
                        catalog.as_ref(),
                        &execution_id,
                        read_fence,
                        &context,
                        PermissionReadPurpose::Pending,
                        None,
                        &mut budget,
                    );
                    let used = budget.usage();
                    results.push(result);
                    usage.push((used.encoded_bytes(), used.structural_nodes()));
                    policy_counts.push(policy_calls.load(Ordering::SeqCst));
                }
                permission_call_digest(actual_task, &mut budget)
            };
            (None, Some(sticky))
        }
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(thread);
    drop(pending);
    drop(parked_approval);
    drop(parked_journal);
    drop(read_authority);
    drop(admitted);
    drop(started);
    drop(parked);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(constraint);
    drop(application);
    assert!(
        Arc::strong_count(&store) == 1,
        "every original writer alias retires normally"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_approval = next.approval_view(&execution_id).unwrap().unwrap();
    let next_journal = next.journal(&execution_id).unwrap();
    next.readiness().unwrap();
    drop(next);
    assert!(
        next_owner == original_owner.checked_add(1).unwrap(),
        "a genuine next writer advances the actual owner generation"
    );
    assert!(
        next_ordinary == after_ordinary
            && next_approval == after_approval
            && next_journal == after_journal,
        "the next writer reads the full ACTUAL AFTER state"
    );
    assert!(
        next_approval.execution.snapshot.owner_generation == original_owner,
        "normal reopening preserves the retained Snapshot owner"
    );
    assert!(
        native_calls.load(Ordering::SeqCst) == 0 && constraint_calls.load(Ordering::SeqCst) == 0,
        "the projection invokes no native callback or constraint"
    );
    drop(native_calls);
    drop(constraint_calls);
    drop(policy_calls);
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(directory);
    ReaderQualifierCapture {
        results,
        fresh_digest,
        sticky_digest,
        policy_counts,
        usage,
        headers,
        before_ordinary,
        before_approval,
        before_journal,
        after_ordinary,
        after_approval,
        after_journal,
    }
}

fn assert_reader_qualifier_digest_setup(captured: &ReaderQualifierCapture) {
    assert!(
        captured.fresh_digest.is_ok(),
        "the genuine task supports a fresh-account digest: {}",
        captured
            .fresh_digest
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert!(
        captured.fresh_digest.as_ref().unwrap() == QUALIFIER_CALL_DIGEST,
        "the real call matches independently canonical known-small TaskSpec/input/version1"
    );
}

fn assert_reader_qualifier_projection(view: &PermissionView, captured: &ReaderQualifierCapture) {
    assert!(
        view.resource().thread == captured.before_ordinary.thread
            && view.resource().execution == captured.before_approval.execution
            && view.thread() == &captured.before_ordinary.thread
            && view.execution() == &captured.before_approval.execution
            && view.grants() == captured.before_approval.grants.as_slice()
            && view.acceptances() == captured.before_approval.acceptances.as_slice(),
        "each successful same-account prefix has the full genuine resource and old rows"
    );
    assert!(
        view.permission_acceptances().is_empty()
            && view.permission_decisions().is_empty()
            && view.permission_uses().is_empty()
            && view.creator_acceptances().is_empty()
            && view.referenced_grants().is_empty()
            && view.referenced_acceptances().is_empty()
            && view.journal_entries().is_empty(),
        "the prefix contains no new authority graph"
    );
    assert!(
        view.receipt().purpose() == PermissionReadPurpose::Pending,
        "the prefix receipt retains the actual read purpose"
    );
}

fn assert_reader_qualifier_retained_state(captured: &ReaderQualifierCapture) {
    assert!(
        captured.before_ordinary == captured.after_ordinary
            && captured.before_approval == captured.after_approval
            && captured.before_journal == captured.after_journal,
        "the subject preserves the full genuinely retained storage state"
    );
    for &(bytes, nodes) in &captured.usage {
        assert!(
            bytes <= MAX_EVIDENCE_BYTES && nodes <= MAX_EVIDENCE_NODES,
            "captured accounts remain within their finite ceilings"
        );
    }
}

fn assert_complete_header_fixture(captured: &ReaderQualifierCapture, with_use: bool) {
    let raw = captured.headers.as_ref().unwrap();
    assert!(
        raw.actual_generation != raw.different_generation,
        "the fault changes the actual generation"
    );
    assert!(
        raw.original.acceptances.is_empty()
            && raw.original.decisions.is_empty()
            && raw.original.uses.is_empty()
            && raw.control_restored == raw.original
            && raw.restored == raw.original,
        "owned fixture repair restores exact empty new tables"
    );
    assert!(
        raw.control_before == raw.control_after && raw.fault_before == raw.fault_after,
        "both reader calls preserve every raw typed fixture cell"
    );
    assert!(
        raw.control_before.acceptances.len() == 1
            && raw.control_before.decisions.len() == 1
            && raw.control_before.uses.len() == usize::from(with_use),
        "the valid-header control contains exactly the closed row sets"
    );
    assert!(
        raw.fault_before.acceptances == raw.control_before.acceptances,
        "the early A rows remain byte and type exact"
    );
    if with_use {
        assert!(
            raw.fault_before.decisions == raw.control_before.decisions,
            "the earlier D rows remain byte and type exact"
        );
        let control = &raw.control_before.uses[0];
        let fault = &raw.fault_before.uses[0];
        assert!(
            control.len() == 10 && fault.len() == 10,
            "the full U row shape is exact"
        );
        for index in 0..10 {
            if index == 4 {
                assert!(
                    control[index] == rusqlite::types::Value::Text(raw.actual_generation.clone())
                        && fault[index]
                            == rusqlite::types::Value::Text(raw.different_generation.clone()),
                    "only U incarnation differs"
                );
            } else {
                assert!(
                    control[index] == fault[index],
                    "all other U cells stay exact"
                );
            }
        }
    } else {
        assert!(
            raw.control_before.uses.is_empty() && raw.fault_before.uses.is_empty(),
            "the D-only fixture has no U rows"
        );
        let control = &raw.control_before.decisions[0];
        let fault = &raw.fault_before.decisions[0];
        assert!(
            control.len() == 6 && fault.len() == 6,
            "the full D row shape is exact"
        );
        for index in 0..6 {
            if index == 2 {
                assert!(
                    control[index] == rusqlite::types::Value::Text(raw.actual_generation.clone())
                        && fault[index]
                            == rusqlite::types::Value::Text(raw.different_generation.clone()),
                    "only D incarnation differs"
                );
            } else {
                assert!(
                    control[index] == fault[index],
                    "all other D cells stay exact"
                );
            }
        }
    }
}

#[test]
fn permission_view_refuses_later_decision_generation_after_valid_acceptance_headers() {
    let neighbor = capture_one_task_view_after_normal_retirement(ViewCase::Pending);
    let captured =
        capture_reader_qualifier_after_normal_retirement(ReaderQualifierCase::DecisionGeneration);
    assert!(
        neighbor.result.is_ok(),
        "the separate genuine small neighbor succeeds: {}",
        neighbor
            .result
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert_complete_projection_and_retained_state(&neighbor, PermissionReadPurpose::Pending);
    assert_reader_qualifier_digest_setup(&captured);
    assert!(
        captured.results.len() == 2 && captured.policy_counts == [1, 2],
        "both reads reach the legal reader policy"
    );
    assert!(
        matches!(
            captured.results[0],
            Err(PermissionError::UnsupportedProfile)
        ),
        "all well-shaped headers reach the explicit unsupported graph boundary: {}",
        captured.results[0]
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert!(
        matches!(
            captured.results[1],
            Err(PermissionError::Store(StoreError::Corrupt(_)))
        ),
        "the later D incarnation mismatch must be corrupt: {}",
        captured.results[1]
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert_complete_header_fixture(&captured, false);
    assert_reader_qualifier_retained_state(&captured);
}

#[test]
fn permission_view_refuses_later_use_generation_after_valid_acceptance_and_decision_headers() {
    let neighbor = capture_one_task_view_after_normal_retirement(ViewCase::Pending);
    let captured =
        capture_reader_qualifier_after_normal_retirement(ReaderQualifierCase::UseGeneration);
    assert!(
        neighbor.result.is_ok(),
        "the separate genuine small neighbor succeeds: {}",
        neighbor
            .result
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert_complete_projection_and_retained_state(&neighbor, PermissionReadPurpose::Pending);
    assert_reader_qualifier_digest_setup(&captured);
    assert!(
        captured.results.len() == 2 && captured.policy_counts == [1, 2],
        "both reads reach the legal reader policy"
    );
    assert!(
        matches!(
            captured.results[0],
            Err(PermissionError::UnsupportedProfile)
        ),
        "all well-shaped headers reach the explicit unsupported graph boundary: {}",
        captured.results[0]
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert!(
        matches!(
            captured.results[1],
            Err(PermissionError::Store(StoreError::Corrupt(_)))
        ),
        "the later U incarnation mismatch must be corrupt: {}",
        captured.results[1]
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert_complete_header_fixture(&captured, true);
    assert_reader_qualifier_retained_state(&captured);
}

#[test]
fn permission_view_shared_account_refuses_third_combined_phase_after_two_real_reads() {
    let neighbor = capture_one_task_view_after_normal_retirement(ViewCase::Pending);
    let captured =
        capture_reader_qualifier_after_normal_retirement(ReaderQualifierCase::SharedThirdPhase);
    assert!(
        neighbor.result.is_ok(),
        "the separate genuine small neighbor succeeds: {}",
        neighbor
            .result
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    assert_complete_projection_and_retained_state(&neighbor, PermissionReadPurpose::Pending);
    assert_reader_qualifier_digest_setup(&captured);
    assert!(
        captured.results.len() == 3 && captured.policy_counts == [1, 2, 3],
        "three real reader policy calls reach the combined admission step"
    );
    for index in 0..2 {
        assert!(
            captured.results[index].is_ok(),
            "the actual shared-account prefix succeeds: {}",
            captured.results[index]
                .as_ref()
                .err()
                .map_or("ok", PermissionError::code)
        );
        assert_reader_qualifier_projection(captured.results[index].as_ref().unwrap(), &captured);
    }
    assert!(
        matches!(captured.results[2], Err(PermissionError::LimitExceeded)),
        "the third combined phase must refuse the same account: {}",
        captured.results[2]
            .as_ref()
            .err()
            .map_or("ok", PermissionError::code)
    );
    let sticky = captured.sticky_digest.as_ref().unwrap();
    assert!(
        matches!(sticky, Err(PermissionError::LimitExceeded)),
        "the subsequent genuine call digest observes sticky exhaustion: {}",
        sticky.as_ref().err().map_or("ok", PermissionError::code)
    );
    assert!(
        captured.headers.is_none() && captured.usage.len() == 3,
        "the shared-phase fixture inserts no raw permission cells"
    );
    for pair in captured.usage.windows(2) {
        assert!(
            pair[0].0 <= pair[1].0 && pair[0].1 <= pair[1].1,
            "the actual shared account is monotonic"
        );
    }
    assert_reader_qualifier_retained_state(&captured);
}
