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

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-human-gate-fixture-{}-{}",
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

struct CountingConstraint {
    calls: Arc<AtomicUsize>,
    denied: bool,
}

impl ToolConstraint for CountingConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.denied {
            Err(ToolConstraintError::Denied)
        } else {
            Ok(())
        }
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
enum Operation {
    Human,
    Pure,
}

struct Captured {
    human_result: Option<Result<StoredExecution, PermissionError>>,
    pure_result: Option<Result<(), PermissionError>>,
    before_ordinary: ThreadView,
    before_approval: ApprovalView,
    before_journal: Vec<JournalEntry>,
    after_ordinary: ThreadView,
    after_approval: ApprovalView,
    after_journal: Vec<JournalEntry>,
    gate_calls: usize,
}

// Three calls to this helper use only the two static tasks, prompts and inputs
// below. It never writes SQL, changes a captured snapshot, dispatches an effect
// or advertises durable permissions. RequiredDurable is the old approval port.
fn capture_after_normal_retirement(denied: bool, operation: Operation) -> Captured {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let gate_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("human-gate-thread").unwrap();
    let execution_id = ExecutionId::new("human-gate-execution").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "human-gate-app".into(),
            tenant: "fixture-tenant".into(),
            principal: "fixture-principal".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/human-gate#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![
            TaskSpec {
                id: TaskId::new("first").unwrap(),
                effect_id: EffectId::new("first-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
            TaskSpec {
                id: TaskId::new("second").unwrap(),
                effect_id: EffectId::new("second-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
        ]],
        max_attempts: 4,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([
                    (
                        EffectId::new("first-effect").unwrap(),
                        EffectBinding {
                            callback: Arc::new(CountNativeCalls(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    ),
                    (
                        EffectId::new("second-effect").unwrap(),
                        EffectBinding {
                            callback: Arc::new(CountNativeCalls(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    ),
                ]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    assert!(
        registered == definition,
        "genuine native registration is exact"
    );
    let gate = Arc::new(CountingConstraint {
        calls: gate_calls.clone(),
        denied,
    });
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            definition.batches[0]
                .iter()
                .map(|spec| ToolBindingRegistration {
                    definition: definition.clone(),
                    effect_id: spec.effect_id.clone(),
                    tool_id: StableToolId::new(spec.effect_id.as_str()).unwrap(),
                    tool_revision: StableToolRevision::new("tool-1").unwrap(),
                    constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                    constraint: gate.clone(),
                })
                .collect(),
        )
        .unwrap(),
    );
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert!(original_owner == 1, "fresh writer owns generation one");
    assert!(
        store.approval_durable(),
        "real SQLite supports durable approval"
    );
    assert!(
        !store.durable_permissions(),
        "fixture claims no lasting permission profile"
    );
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
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
        started.snapshot.status == ExecutionStatus::Running,
        "real Start is Running"
    );
    assert!(
        started.snapshot.tasks.len() == 2,
        "real Start initializes two tasks"
    );
    for task in &started.snapshot.tasks {
        assert!(
            task.status == TaskStatus::Ready && !task.approved,
            "real Start has unapproved Ready tasks"
        );
        assert!(
            task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
                && task.interrupt.is_none(),
            "genuine tasks have no attempt, outcome or interrupt"
        );
        assert!(
            task.input == started.snapshot.values,
            "genuine input is retained state"
        );
    }
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompts = [
        json!({"question": "First fixture?"}),
        json!({"question": "Second fixture?", "nullable": null}),
    ];
    for (index, name) in ["first", "second"].iter().enumerate() {
        let parked = service
            .park(
                &context,
                &execution_id,
                fence(store.as_ref(), &thread_id),
                TaskId::new(*name).unwrap(),
                InterruptId::new(format!("{name}-interrupt")).unwrap(),
                prompts[index].clone(),
                1_000,
                facts(102 + index as u64),
            )
            .unwrap();
        assert!(
            parked.snapshot.revision == 2 + index as u64,
            "each real Park advances revision"
        );
        assert!(
            parked.snapshot.status
                == if index == 0 {
                    ExecutionStatus::Running
                } else {
                    ExecutionStatus::WaitingForApproval
                },
            "all real parks reach the stable approval phase"
        );
    }
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let before_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let before_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    assert!(
        before_ordinary.thread.origin.as_ref() == Some(&context.actor),
        "context comes from the genuine retained origin"
    );
    assert!(
        before_ordinary.thread == before_approval.thread
            && before_ordinary.execution.as_ref() == Some(&before_approval.execution),
        "full genuine views agree"
    );
    assert!(
        before_approval.execution.snapshot.status == ExecutionStatus::WaitingForApproval
            && before_approval.execution.snapshot.revision == 3,
        "genuine complete batch is parked"
    );
    assert!(
        before_approval.execution.snapshot.tasks.len() == 2
            && pending.len() == 2
            && before_approval.grants.len() == 2,
        "two real pending tasks have two real grants"
    );
    assert!(
        before_approval.acceptances.is_empty(),
        "no acceptance is fabricated"
    );
    let mut decisions = Vec::with_capacity(2);
    for (index, task) in before_approval.execution.snapshot.tasks.iter().enumerate() {
        assert!(
            task.spec == definition.batches[0][index]
                && task.status == TaskStatus::WaitingForApproval
                && !task.approved,
            "real waiting tasks retain exact required specifications"
        );
        assert!(
            task.attempt_id.is_none() && task.result.is_none() && task.failure.is_none(),
            "real waiting tasks are unclaimed"
        );
        let interrupt = task.interrupt.as_ref().unwrap();
        let grant = before_approval
            .grants
            .iter()
            .find(|grant| grant.interrupt_id == interrupt.id)
            .unwrap();
        let projection = pending
            .iter()
            .find(|entry| entry.interrupt_id == interrupt.id)
            .unwrap();
        assert!(
            interrupt.prompt == prompts[index] && grant.prompt == interrupt.prompt,
            "original prompts come from real parks"
        );
        assert!(
            grant.thread_id == thread_id
                && grant.thread_generation == before_approval.thread.generation
                && grant.origin == context.actor,
            "real grant is bound to the actual thread incarnation and origin"
        );
        assert!(
            grant.execution_id == execution_id
                && grant.route == definition.route
                && grant.definition_revision == definition.revision,
            "real grant is bound to the actual execution definition"
        );
        assert!(
            grant.task_id == task.spec.id
                && grant.effect_id == task.spec.effect_id
                && grant.interrupt_id == interrupt.id
                && grant.policy_revision == context.policy_revision,
            "real grant has exact task, effect, interrupt and policy"
        );
        assert!(
            grant.task_parking_revision == 2 + index as u64
                && grant.issued_at_ms == 102 + index as u64
                && grant.expires_at_ms == 1_102 + index as u64,
            "real grant has actual park and expiry facts"
        );
        assert!(
            !grant.consumed && grant.issued_at_ms <= 104 && 104 < grant.expires_at_ms,
            "both real grants are available"
        );
        assert!(
            grant.token == projection.token
                && grant.verifier == token_verifier(&projection.token)
                && grant.resume_key == projection.resume_key,
            "real pending token, verifier and resume key agree"
        );
        assert!(
            projection.task_id == task.spec.id
                && projection.prompt == grant.prompt
                && projection.expires_at_ms == grant.expires_at_ms,
            "real pending projection is exact"
        );
        assert!(
            grant.proposal_digest == proposal_digest(task, &interrupt.prompt).unwrap(),
            "actual legacy proposal digest matches the real witness"
        );
        decisions.push(GrantDecision {
            interrupt_id: interrupt.id.clone(),
            decision: ApprovalDecision::Once,
            cancelled: false,
            token: Some(projection.token.clone()),
        });
    }
    let invocation_context = InvocationContext {
        thread_id: before_ordinary.thread.id.clone(),
        thread_generation: before_ordinary.thread.generation.clone(),
        origin: before_ordinary.thread.origin.as_ref().unwrap().clone(),
        application_revision: application.identity().revision.clone(),
        execution_revision: before_approval
            .execution
            .snapshot
            .definition
            .revision
            .clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let request = ApprovalAcceptanceRequest {
        context,
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("human-gate-resume-turn").unwrap(),
        now_ms: 104,
        decisions,
    };
    let (human_result, pure_result) = match operation {
        Operation::Human => (
            Some(accept_permission_human(
                store.clone(),
                authority.clone(),
                catalog.clone(),
                &execution_id,
                fence(store.as_ref(), &thread_id),
                request,
            )),
            None,
        ),
        Operation::Pure => (
            None,
            Some(catalog.validate_call(
                &invocation_context,
                &before_approval.execution.snapshot.definition,
                &before_approval.execution.snapshot.tasks[0],
                PermissionCallPhase::Unclaimed,
            )),
        ),
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(pending);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(gate);
    drop(application);
    assert!(
        Arc::strong_count(&store) == 1,
        "all original writer aliases retire normally"
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
        "a genuine next writer advances owner generation"
    );
    assert!(
        next_ordinary == after_ordinary && next_approval == after_approval,
        "next writer reads the actually captured after state"
    );
    assert!(
        next_journal == after_journal,
        "next writer reads the actually captured complete journal"
    );
    assert!(
        next_approval.execution.snapshot.owner_generation == original_owner,
        "normal reopen preserves the original recorded owner"
    );
    assert!(
        native_calls.load(Ordering::SeqCst) == 0,
        "no native callback is invoked"
    );
    let gate_calls = gate_calls.load(Ordering::SeqCst);
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(directory);
    Captured {
        human_result,
        pure_result,
        before_ordinary,
        before_approval,
        before_journal,
        after_ordinary,
        after_approval,
        after_journal,
        gate_calls,
    }
}

#[test]
fn human_acceptance_refuses_denied_constraint_before_consuming_two_real_once_grants() {
    let allowed = capture_after_normal_retirement(false, Operation::Human);
    let pure_denied = capture_after_normal_retirement(true, Operation::Pure);
    let subject = capture_after_normal_retirement(true, Operation::Human);

    let accepted = match &allowed.human_result {
        Some(Ok(accepted)) => accepted,
        _ => panic!("the permissive full two-Once neighbor must remain legal"),
    };
    assert!(
        allowed.after_ordinary.execution.as_ref() == Some(accepted)
            && allowed.after_approval.execution == *accepted,
        "permissive result matches the full captured after views"
    );
    assert!(
        accepted.snapshot.status == ExecutionStatus::Running
            && accepted.snapshot.revision == 4
            && accepted.snapshot.turn_id.as_str() == "human-gate-resume-turn",
        "permissive Once continuation retains real kernel facts"
    );
    assert!(
        accepted.snapshot.tasks.len() == 2
            && accepted
                .snapshot
                .tasks
                .iter()
                .all(|task| task.status == TaskStatus::Ready
                    && task.approved
                    && task.interrupt.is_some()
                    && task.attempt_id.is_none()
                    && task.result.is_none()
                    && task.failure.is_none()),
        "both permitted real tasks are Ready without an attempt"
    );
    assert!(
        accepted.snapshot.attempts_used == 0 && accepted.snapshot.attempt_history.is_empty(),
        "permissive control dispatches no native effect"
    );
    assert!(
        allowed.before_journal.len() == 4
            && allowed.after_journal.len() == 5
            && allowed.after_journal.last().is_some_and(
                |entry| entry.revision == 4 && entry.status == ExecutionStatus::Running
            ),
        "permissive control records the actual fifth revision without invoking"
    );
    assert!(
        allowed.after_approval.grants.len() == 2
            && allowed
                .after_approval
                .grants
                .iter()
                .all(|grant| grant.consumed)
            && allowed.after_approval.acceptances.len() == 1
            && allowed.after_approval.acceptances[0].decisions.len() == 2
            && allowed.after_approval.acceptances[0]
                .decisions
                .iter()
                .all(|entry| entry.decision == ApprovalDecision::Once),
        "permissive neighbor has one actual complete Once acceptance and two consumed grants"
    );
    assert!(
        matches!(
            &pure_denied.pure_result,
            Some(Err(PermissionError::ConstraintDenied))
        ),
        "direct denied gate control returns its qualified classification"
    );
    assert!(
        pure_denied.gate_calls == 1,
        "direct control actually invokes the denied constraint"
    );
    assert!(
        pure_denied.after_ordinary == pure_denied.before_ordinary
            && pure_denied.after_approval == pure_denied.before_approval,
        "direct gate control preserves complete real storage"
    );
    assert!(
        pure_denied.after_journal == pure_denied.before_journal,
        "direct gate control preserves the full real journal"
    );

    // FIRST new feature oracle. Floor forwarding accepts the real two-Once
    // request; all changed floor state is retained above through normal close.
    assert!(
        matches!(
            &subject.human_result,
            Some(Err(PermissionError::ConstraintDenied))
        ),
        "human acceptance must invoke the denied per-call constraint before consuming real grants"
    );
    // These later feature guards have zero initial credit if the oracle fails.
    assert!(
        subject.gate_calls == 1,
        "human denial comes from one actual constraint invocation"
    );
    assert!(
        subject.after_ordinary == subject.before_ordinary
            && subject.after_approval == subject.before_approval,
        "denied human acceptance preserves the complete thread, snapshot, grants and acceptances"
    );
    assert!(
        subject.after_journal == subject.before_journal,
        "denied human acceptance preserves the full journal"
    );
    assert!(
        allowed.gate_calls == 2,
        "permissive human acceptance gates both actual calls"
    );
}

// Two separate new parents use the same finite two-task, two-prompt recipe.
// The original helper and denied parent above remain an exact protected prefix.
#[derive(Clone, Copy)]
enum AdditionalGateCase {
    Unavailable,
    ForeignCatalog,
}

struct AdditionalCountingConstraint {
    calls: Arc<AtomicUsize>,
    unavailable: bool,
}

impl ToolConstraint for AdditionalCountingConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.unavailable {
            Err(ToolConstraintError::Unavailable)
        } else {
            Ok(())
        }
    }
}

// Results and actual AFTER storage are captured before retiring both genuine
// applications, both complete catalogs, all gates and every writer alias.
fn capture_additional_after_normal_retirement(
    case: AdditionalGateCase,
    operation: Operation,
) -> Captured {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let gate_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("human-gate-thread").unwrap();
    let execution_id = ExecutionId::new("human-gate-execution").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "human-gate-app".into(),
            tenant: "fixture-tenant".into(),
            principal: "fixture-principal".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/human-gate#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![
            TaskSpec {
                id: TaskId::new("first").unwrap(),
                effect_id: EffectId::new("first-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
            TaskSpec {
                id: TaskId::new("second").unwrap(),
                effect_id: EffectId::new("second-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
        ]],
        max_attempts: 4,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([
                    (
                        EffectId::new("first-effect").unwrap(),
                        EffectBinding {
                            callback: Arc::new(CountNativeCalls(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    ),
                    (
                        EffectId::new("second-effect").unwrap(),
                        EffectBinding {
                            callback: Arc::new(CountNativeCalls(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    ),
                ]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    assert!(
        registered == definition,
        "genuine native registration is exact"
    );
    let gate = Arc::new(AdditionalCountingConstraint {
        calls: gate_calls.clone(),
        unavailable: matches!(case, AdditionalGateCase::Unavailable),
    });
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            definition.batches[0]
                .iter()
                .map(|spec| ToolBindingRegistration {
                    definition: definition.clone(),
                    effect_id: spec.effect_id.clone(),
                    tool_id: StableToolId::new(spec.effect_id.as_str()).unwrap(),
                    tool_revision: StableToolRevision::new("tool-1").unwrap(),
                    constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                    constraint: gate.clone(),
                })
                .collect(),
        )
        .unwrap(),
    );
    // This is a second real native application, not a modified stored witness.
    let foreign_definition = Definition {
        revision: ExecutionRevision::new("definition-foreign").unwrap(),
        ..definition.clone()
    };
    let foreign_application = Arc::new(
        NativeApplication::new(
            application.identity().clone(),
            vec![RouteRegistration {
                definition: foreign_definition.clone(),
                effects: BTreeMap::from([
                    (
                        EffectId::new("first-effect").unwrap(),
                        EffectBinding {
                            callback: Arc::new(CountNativeCalls(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    ),
                    (
                        EffectId::new("second-effect").unwrap(),
                        EffectBinding {
                            callback: Arc::new(CountNativeCalls(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    ),
                ]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    assert!(
        foreign_application.definition(&foreign_definition.route) == Some(&foreign_definition)
            && foreign_definition.route == definition.route
            && foreign_definition.batches == definition.batches
            && foreign_definition.max_attempts == definition.max_attempts
            && foreign_definition.revision != definition.revision,
        "foreign catalog belongs to its own complete genuine current definition"
    );
    let foreign_gate = Arc::new(AdditionalCountingConstraint {
        calls: gate_calls.clone(),
        unavailable: false,
    });
    let foreign_catalog = Arc::new(
        PermissionCatalog::new(
            foreign_application.clone(),
            foreign_definition.batches[0]
                .iter()
                .map(|spec| ToolBindingRegistration {
                    definition: foreign_definition.clone(),
                    effect_id: spec.effect_id.clone(),
                    tool_id: StableToolId::new(spec.effect_id.as_str()).unwrap(),
                    tool_revision: StableToolRevision::new("tool-1").unwrap(),
                    constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                    constraint: foreign_gate.clone(),
                })
                .collect(),
        )
        .unwrap(),
    );
    let selected_catalog = match case {
        AdditionalGateCase::Unavailable => catalog.clone(),
        AdditionalGateCase::ForeignCatalog => foreign_catalog.clone(),
    };
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert!(original_owner == 1, "fresh writer owns generation one");
    assert!(
        store.approval_durable(),
        "real SQLite supports durable approval"
    );
    assert!(
        !store.durable_permissions(),
        "fixture claims no lasting permission profile"
    );
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
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
        started.snapshot.status == ExecutionStatus::Running,
        "real Start is Running"
    );
    assert!(
        started.snapshot.tasks.len() == 2,
        "real Start initializes two tasks"
    );
    for task in &started.snapshot.tasks {
        assert!(
            task.status == TaskStatus::Ready && !task.approved,
            "real Start has unapproved Ready tasks"
        );
        assert!(
            task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
                && task.interrupt.is_none(),
            "genuine tasks have no attempt, outcome or interrupt"
        );
        assert!(
            task.input == started.snapshot.values,
            "genuine input is retained state"
        );
    }
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompts = [
        json!({"question": "First fixture?"}),
        json!({"question": "Second fixture?", "nullable": null}),
    ];
    for (index, name) in ["first", "second"].iter().enumerate() {
        let parked = service
            .park(
                &context,
                &execution_id,
                fence(store.as_ref(), &thread_id),
                TaskId::new(*name).unwrap(),
                InterruptId::new(format!("{name}-interrupt")).unwrap(),
                prompts[index].clone(),
                1_000,
                facts(102 + index as u64),
            )
            .unwrap();
        assert!(
            parked.snapshot.revision == 2 + index as u64,
            "each real Park advances revision"
        );
        assert!(
            parked.snapshot.status
                == if index == 0 {
                    ExecutionStatus::Running
                } else {
                    ExecutionStatus::WaitingForApproval
                },
            "all real parks reach the stable approval phase"
        );
    }
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let before_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let before_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    assert!(
        before_ordinary.thread.origin.as_ref() == Some(&context.actor),
        "context comes from the genuine retained origin"
    );
    assert!(
        before_ordinary.thread == before_approval.thread
            && before_ordinary.execution.as_ref() == Some(&before_approval.execution),
        "full genuine views agree"
    );
    assert!(
        before_approval.execution.snapshot.status == ExecutionStatus::WaitingForApproval
            && before_approval.execution.snapshot.revision == 3,
        "genuine complete batch is parked"
    );
    assert!(
        before_approval.execution.snapshot.tasks.len() == 2
            && pending.len() == 2
            && before_approval.grants.len() == 2,
        "two real pending tasks have two real grants"
    );
    assert!(
        before_approval.acceptances.is_empty(),
        "no acceptance is fabricated"
    );
    let mut decisions = Vec::with_capacity(2);
    for (index, task) in before_approval.execution.snapshot.tasks.iter().enumerate() {
        assert!(
            task.spec == definition.batches[0][index]
                && task.status == TaskStatus::WaitingForApproval
                && !task.approved,
            "real waiting tasks retain exact required specifications"
        );
        assert!(
            task.attempt_id.is_none() && task.result.is_none() && task.failure.is_none(),
            "real waiting tasks are unclaimed"
        );
        let interrupt = task.interrupt.as_ref().unwrap();
        let grant = before_approval
            .grants
            .iter()
            .find(|grant| grant.interrupt_id == interrupt.id)
            .unwrap();
        let projection = pending
            .iter()
            .find(|entry| entry.interrupt_id == interrupt.id)
            .unwrap();
        assert!(
            interrupt.prompt == prompts[index] && grant.prompt == interrupt.prompt,
            "original prompts come from real parks"
        );
        assert!(
            grant.thread_id == thread_id
                && grant.thread_generation == before_approval.thread.generation
                && grant.origin == context.actor,
            "real grant is bound to the actual thread incarnation and origin"
        );
        assert!(
            grant.execution_id == execution_id
                && grant.route == definition.route
                && grant.definition_revision == definition.revision,
            "real grant is bound to the actual execution definition"
        );
        assert!(
            grant.task_id == task.spec.id
                && grant.effect_id == task.spec.effect_id
                && grant.interrupt_id == interrupt.id
                && grant.policy_revision == context.policy_revision,
            "real grant has exact task, effect, interrupt and policy"
        );
        assert!(
            grant.task_parking_revision == 2 + index as u64
                && grant.issued_at_ms == 102 + index as u64
                && grant.expires_at_ms == 1_102 + index as u64,
            "real grant has actual park and expiry facts"
        );
        assert!(
            !grant.consumed && grant.issued_at_ms <= 104 && 104 < grant.expires_at_ms,
            "both real grants are available"
        );
        assert!(
            grant.token == projection.token
                && grant.verifier == token_verifier(&projection.token)
                && grant.resume_key == projection.resume_key,
            "real pending token, verifier and resume key agree"
        );
        assert!(
            projection.task_id == task.spec.id
                && projection.prompt == grant.prompt
                && projection.expires_at_ms == grant.expires_at_ms,
            "real pending projection is exact"
        );
        assert!(
            grant.proposal_digest == proposal_digest(task, &interrupt.prompt).unwrap(),
            "actual legacy proposal digest matches the real witness"
        );
        decisions.push(GrantDecision {
            interrupt_id: interrupt.id.clone(),
            decision: ApprovalDecision::Once,
            cancelled: false,
            token: Some(projection.token.clone()),
        });
    }
    let invocation_context = InvocationContext {
        thread_id: before_ordinary.thread.id.clone(),
        thread_generation: before_ordinary.thread.generation.clone(),
        origin: before_ordinary.thread.origin.as_ref().unwrap().clone(),
        application_revision: application.identity().revision.clone(),
        execution_revision: before_approval
            .execution
            .snapshot
            .definition
            .revision
            .clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let request = ApprovalAcceptanceRequest {
        context,
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("human-gate-resume-turn").unwrap(),
        now_ms: 104,
        decisions,
    };
    let (human_result, pure_result) = match operation {
        Operation::Human => (
            Some(accept_permission_human(
                store.clone(),
                authority.clone(),
                selected_catalog.clone(),
                &execution_id,
                fence(store.as_ref(), &thread_id),
                request,
            )),
            None,
        ),
        Operation::Pure => (
            None,
            Some(selected_catalog.validate_call(
                &invocation_context,
                &before_approval.execution.snapshot.definition,
                &before_approval.execution.snapshot.tasks[0],
                PermissionCallPhase::Unclaimed,
            )),
        ),
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(pending);
    drop(service);
    drop(authority);
    drop(selected_catalog);
    drop(foreign_catalog);
    drop(catalog);
    drop(foreign_gate);
    drop(gate);
    drop(foreign_application);
    drop(application);
    assert!(
        Arc::strong_count(&store) == 1,
        "all original writer aliases retire normally"
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
        "a genuine next writer advances owner generation"
    );
    assert!(
        next_ordinary == after_ordinary && next_approval == after_approval,
        "next writer reads the actually captured after state"
    );
    assert!(
        next_journal == after_journal,
        "next writer reads the actually captured complete journal"
    );
    assert!(
        next_approval.execution.snapshot.owner_generation == original_owner,
        "normal reopen preserves the original recorded owner"
    );
    assert!(
        native_calls.load(Ordering::SeqCst) == 0,
        "no native callback is invoked"
    );
    let gate_calls = gate_calls.load(Ordering::SeqCst);
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(directory);
    Captured {
        human_result,
        pure_result,
        before_ordinary,
        before_approval,
        before_journal,
        after_ordinary,
        after_approval,
        after_journal,
        gate_calls,
    }
}

fn assert_additional_permissive_neighbor(allowed: &Captured) {
    let accepted = match &allowed.human_result {
        Some(Ok(accepted)) => accepted,
        _ => panic!("the permissive full two-Once neighbor must remain legal"),
    };
    assert!(
        allowed.after_ordinary.execution.as_ref() == Some(accepted)
            && allowed.after_approval.execution == *accepted,
        "permissive result matches the full captured after views"
    );
    assert!(
        accepted.snapshot.status == ExecutionStatus::Running
            && accepted.snapshot.revision == 4
            && accepted.snapshot.turn_id.as_str() == "human-gate-resume-turn",
        "permissive Once continuation retains real kernel facts"
    );
    assert!(
        accepted.snapshot.tasks.len() == 2
            && accepted
                .snapshot
                .tasks
                .iter()
                .all(|task| task.status == TaskStatus::Ready
                    && task.approved
                    && task.interrupt.is_some()
                    && task.attempt_id.is_none()
                    && task.result.is_none()
                    && task.failure.is_none()),
        "both permitted real tasks are Ready without an attempt"
    );
    assert!(
        accepted.snapshot.attempts_used == 0 && accepted.snapshot.attempt_history.is_empty(),
        "permissive control dispatches no native effect"
    );
    assert!(
        allowed.before_journal.len() == 4
            && allowed.after_journal.len() == 5
            && allowed.after_journal.last().is_some_and(
                |entry| entry.revision == 4 && entry.status == ExecutionStatus::Running
            ),
        "permissive control records the actual fifth revision without invoking"
    );
    assert!(
        allowed.after_approval.grants.len() == 2
            && allowed
                .after_approval
                .grants
                .iter()
                .all(|grant| grant.consumed)
            && allowed.after_approval.acceptances.len() == 1
            && allowed.after_approval.acceptances[0].decisions.len() == 2
            && allowed.after_approval.acceptances[0]
                .decisions
                .iter()
                .all(|entry| entry.decision == ApprovalDecision::Once),
        "permissive neighbor has one actual complete Once acceptance and two consumed grants"
    );
}

fn assert_additional_direct_storage_unchanged(pure: &Captured) {
    assert!(
        pure.after_ordinary == pure.before_ordinary
            && pure.after_approval == pure.before_approval
            && pure.after_journal == pure.before_journal,
        "direct classification control preserves all complete real storage views"
    );
}

fn assert_additional_human_storage_unchanged(subject: &Captured) {
    assert!(
        subject.after_ordinary == subject.before_ordinary
            && subject.after_approval == subject.before_approval
            && subject.after_journal == subject.before_journal,
        "refused human acceptance preserves the complete real thread, snapshot, grants, acceptances and journal"
    );
}

#[test]
fn human_acceptance_refuses_unavailable_constraint_before_consuming_two_real_once_grants() {
    let allowed = capture_after_normal_retirement(false, Operation::Human);
    let pure = capture_additional_after_normal_retirement(
        AdditionalGateCase::Unavailable,
        Operation::Pure,
    );
    let subject = capture_additional_after_normal_retirement(
        AdditionalGateCase::Unavailable,
        Operation::Human,
    );

    assert_additional_permissive_neighbor(&allowed);
    assert!(
        matches!(
            &pure.pure_result,
            Some(Err(PermissionError::ConstraintUnavailable))
        ),
        "direct genuine unavailable constraint has its qualified classification"
    );
    assert!(
        pure.gate_calls == 1,
        "direct unavailable control invokes the actual constraint once"
    );
    assert_additional_direct_storage_unchanged(&pure);

    // FIRST new feature oracle, after all three full normal retirements.
    assert!(
        matches!(&subject.human_result, Some(Err(PermissionError::ConstraintUnavailable))),
        "human acceptance must refuse the unavailable per-call constraint before consuming real grants"
    );
    // Later guards have zero initial credit if that first oracle fails.
    assert!(
        subject.gate_calls == 1,
        "unavailable human refusal comes from one actual constraint invocation"
    );
    assert_additional_human_storage_unchanged(&subject);
    assert!(
        allowed.gate_calls == 2,
        "permissive human acceptance gates both actual calls"
    );
}

#[test]
fn human_acceptance_refuses_foreign_complete_catalog_before_consuming_two_real_once_grants() {
    let allowed = capture_after_normal_retirement(false, Operation::Human);
    let pure = capture_additional_after_normal_retirement(
        AdditionalGateCase::ForeignCatalog,
        Operation::Pure,
    );
    let subject = capture_additional_after_normal_retirement(
        AdditionalGateCase::ForeignCatalog,
        Operation::Human,
    );

    assert_additional_permissive_neighbor(&allowed);
    assert!(
        matches!(
            &pure.pure_result,
            Some(Err(PermissionError::InvalidCatalog))
        ),
        "direct genuine foreign complete catalog has its qualified classification"
    );
    assert!(
        pure.gate_calls == 0,
        "foreign definition refusal precedes constraint invocation"
    );
    assert_additional_direct_storage_unchanged(&pure);

    // FIRST new feature oracle, after all three full normal retirements.
    assert!(
        matches!(&subject.human_result, Some(Err(PermissionError::InvalidCatalog))),
        "human acceptance must refuse a different genuine current definition catalog before consuming real grants"
    );
    // Later guards have zero initial credit if that first oracle fails.
    assert!(
        subject.gate_calls == 0,
        "foreign catalog refusal precedes all constraint invocations"
    );
    assert_additional_human_storage_unchanged(&subject);
    assert!(
        allowed.gate_calls == 2,
        "permissive human acceptance gates both actual calls"
    );
}
