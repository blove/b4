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
            "b4-permission-resource-fixture-{}-{}",
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

struct ResourceCapture {
    resource_result: Result<ApprovalResource, PermissionError>,
    before_ordinary: ThreadView,
    before_approval: ApprovalView,
    before_journal: Vec<JournalEntry>,
    after_ordinary: ThreadView,
    after_approval: ApprovalView,
    after_journal: Vec<JournalEntry>,
}

// A single fixed native two-task recipe captures the projection, full actual
// AFTER state before every normal writer drop.
fn capture_resource_after_normal_retirement() -> ResourceCapture {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
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
    }
    let resource_result = {
        let mut budget = PermissionEvidenceBudget::new();
        // Pass one actual evidence account through the token-free resource port.
        PermissionStore::permission_resource(store.as_ref(), &execution_id, &mut budget)
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(pending);
    drop(service);
    drop(authority);
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
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(directory);
    ResourceCapture {
        resource_result,
        before_ordinary,
        before_approval,
        before_journal,
        after_ordinary,
        after_approval,
        after_journal,
    }
}

#[test]
fn permission_resource_returns_genuine_small_token_free_parked_resource_after_normal_retirement() {
    let captured = capture_resource_after_normal_retirement();

    // Availability is checked after complete normal writer retirement.
    assert!(
        captured.resource_result.is_ok(),
        "the proposed token-free reader must provide the genuine small matched resource"
    );
    // A successful resource must match the complete retained state.
    let resource = match &captured.resource_result {
        Ok(resource) => resource,
        Err(_) => unreachable!("successful primary resource result"),
    };
    assert!(
        resource.thread == captured.before_ordinary.thread
            && captured.before_ordinary.execution.as_ref() == Some(&resource.execution)
            && resource.thread == captured.before_approval.thread
            && resource.execution == captured.before_approval.execution,
        "token-free projection is exactly the genuine full thread and stored execution"
    );
    assert!(
        captured.after_ordinary == captured.before_ordinary
            && captured.after_approval == captured.before_approval
            && captured.after_journal == captured.before_journal,
        "resource projection preserves all complete genuine storage and journal state"
    );
}

const RESOURCE_METADATA_PADDING_BYTES: usize = 262_145;
const RESOURCE_THREAD_BYTES: usize = 262_144;
const RESOURCE_ACCOUNT_BYTES: usize = 8_388_608;
const RESOURCE_ACCOUNT_NODES: usize = 65_536;
const RESOURCE_EXHAUSTION_ATTEMPTS: usize = 127;
const RESOURCE_CALL_SUCCESS_LOWER_BYTES: usize = 66_560;
const RESOURCE_CALL_FIRST_UPPER_BYTES: usize = 70_000;
const RESOURCE_CALL_SHA256: &str =
    "7760372027e7209c38107597c5611b9fcc2aafcb35960a266ca9da36750a9983";

#[derive(Clone, Copy)]
enum ResourceQuotaRecipe {
    OversizedMetadata,
    ExhaustedAccount,
}

struct StickyResourceSetup {
    first_digest: Option<String>,
    first_success_usage: Option<PermissionEvidenceUsage>,
    all_success_hashes_match: bool,
    successful_calls: usize,
    attempted_calls: usize,
    first_error: Option<PermissionError>,
    before_limit_usage: Option<PermissionEvidenceUsage>,
    after_limit_usage: Option<PermissionEvidenceUsage>,
    sticky_probe: Option<Result<String, PermissionError>>,
    after_probe_usage: PermissionEvidenceUsage,
}

struct QuotaResourceCapture {
    resource: ResourceCapture,
    sticky_setup: Option<StickyResourceSetup>,
    resource_usage_before: PermissionEvidenceUsage,
    resource_usage_after: PermissionEvidenceUsage,
}

// Two finite recipes reuse the genuine native setup and normal physical closure.
fn capture_quota_resource_after_normal_retirement(
    recipe: ResourceQuotaRecipe,
) -> QuotaResourceCapture {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
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
    let metadata = match recipe {
        ResourceQuotaRecipe::OversizedMetadata => Map::from_iter([(
            "padding".into(),
            serde_json::Value::String("x".repeat(RESOURCE_METADATA_PADDING_BYTES)),
        )]),
        ResourceQuotaRecipe::ExhaustedAccount => Map::new(),
    };
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata,
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
    }
    let (resource_result, sticky_setup, resource_usage_before, resource_usage_after) = {
        let mut budget = PermissionEvidenceBudget::new();
        let sticky_setup = match recipe {
            ResourceQuotaRecipe::OversizedMetadata => None,
            ResourceQuotaRecipe::ExhaustedAccount => {
                let task = &before_approval.execution.snapshot.tasks[0];
                let mut setup = StickyResourceSetup {
                    first_digest: None,
                    first_success_usage: None,
                    all_success_hashes_match: true,
                    successful_calls: 0,
                    attempted_calls: 0,
                    first_error: None,
                    before_limit_usage: None,
                    after_limit_usage: None,
                    sticky_probe: None,
                    after_probe_usage: budget.usage(),
                };
                for index in 0..RESOURCE_EXHAUSTION_ATTEMPTS {
                    let before_call = budget.usage();
                    setup.attempted_calls = index + 1;
                    match permission_call_digest(task, &mut budget) {
                        Ok(digest) => {
                            setup.all_success_hashes_match &= digest == RESOURCE_CALL_SHA256;
                            setup.successful_calls += 1;
                            if index == 0 {
                                setup.first_success_usage = Some(budget.usage());
                                setup.first_digest = Some(digest);
                            }
                        }
                        Err(error) => {
                            setup.first_error = Some(error);
                            setup.before_limit_usage = Some(before_call);
                            setup.after_limit_usage = Some(budget.usage());
                            break;
                        }
                    }
                }
                // The pure probe and reader use the identical live account.
                setup.sticky_probe = Some(permission_call_digest(task, &mut budget));
                setup.after_probe_usage = budget.usage();
                Some(setup)
            }
        };
        let resource_usage_before = budget.usage();
        let resource_result =
            PermissionStore::permission_resource(store.as_ref(), &execution_id, &mut budget);
        let resource_usage_after = budget.usage();
        (
            resource_result,
            sticky_setup,
            resource_usage_before,
            resource_usage_after,
        )
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(pending);
    drop(service);
    drop(authority);
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
    drop(next_ordinary);
    drop(next_approval);
    drop(next_journal);
    drop(directory);
    QuotaResourceCapture {
        resource: ResourceCapture {
            resource_result,
            before_ordinary,
            before_approval,
            before_journal,
            after_ordinary,
            after_approval,
            after_journal,
        },
        sticky_setup,
        resource_usage_before,
        resource_usage_after,
    }
}

fn assert_exact_small_resource_capture(captured: &ResourceCapture) {
    assert!(
        captured.resource_result.is_ok(),
        "the same reader genuinely returns the independent small resource"
    );
    let resource = match &captured.resource_result {
        Ok(resource) => resource,
        Err(_) => unreachable!("successful small resource control"),
    };
    assert!(
        resource.thread == captured.before_ordinary.thread
            && captured.before_ordinary.execution.as_ref() == Some(&resource.execution)
            && resource.thread == captured.before_approval.thread
            && resource.execution == captured.before_approval.execution,
        "the small control is the exact complete retained token-free projection"
    );
    assert!(
        captured.after_ordinary == captured.before_ordinary
            && captured.after_approval == captured.before_approval
            && captured.after_journal == captured.before_journal,
        "the small control preserves complete ordinary, approval and journal state"
    );
}

fn assert_quota_resource_storage_preserved(captured: &QuotaResourceCapture) {
    assert!(
        captured.resource.after_ordinary == captured.resource.before_ordinary
            && captured.resource.after_approval == captured.resource.before_approval
            && captured.resource.after_journal == captured.resource.before_journal,
        "a refused resource read preserves the full actual retained state"
    );
    assert!(
        captured.resource_usage_before.encoded_bytes()
            <= captured.resource_usage_after.encoded_bytes()
            && captured.resource_usage_after.encoded_bytes() <= RESOURCE_ACCOUNT_BYTES
            && captured.resource_usage_before.structural_nodes()
                <= captured.resource_usage_after.structural_nodes()
            && captured.resource_usage_after.structural_nodes() <= RESOURCE_ACCOUNT_NODES,
        "the same reader account remains finite and monotone"
    );
}

#[test]
fn permission_resource_rejects_genuine_oversized_metadata_after_normal_retirement() {
    let small = capture_resource_after_normal_retirement();
    let subject =
        capture_quota_resource_after_normal_retirement(ResourceQuotaRecipe::OversizedMetadata);

    assert_exact_small_resource_capture(&small);
    let padding = subject
        .resource
        .before_ordinary
        .thread
        .metadata
        .get("padding")
        .and_then(serde_json::Value::as_str);
    assert!(
        padding.is_some_and(|value| value.len() == RESOURCE_METADATA_PADDING_BYTES
            && value.bytes().all(|byte| byte == b'x'))
            && subject.resource.before_ordinary.thread.metadata.len() == 1,
        "the real stored metadata is the fixed legal oversized ASCII recipe"
    );
    assert!(
        RESOURCE_METADATA_PADDING_BYTES
            .checked_add(14)
            .is_some_and(|bytes| bytes == 262_159 && bytes > RESOURCE_THREAD_BYTES),
        "known compact metadata alone exceeds the whole-thread byte cap"
    );
    assert!(
        subject.sticky_setup.is_none(),
        "the metadata subject uses a fresh actual account"
    );

    assert!(
        matches!(
            &subject.resource.resource_result,
            Err(PermissionError::LimitExceeded)
        ),
        "the token-free reader must refuse the genuine oversized thread metadata"
    );
    // These guards receive no credit until the primary refusal is reached.
    assert_quota_resource_storage_preserved(&subject);
}

#[test]
fn permission_resource_rejects_genuinely_exhausted_account_after_normal_retirement() {
    let small = capture_resource_after_normal_retirement();
    let subject =
        capture_quota_resource_after_normal_retirement(ResourceQuotaRecipe::ExhaustedAccount);

    assert_exact_small_resource_capture(&small);
    assert!(
        subject.resource.before_ordinary.thread.metadata.is_empty(),
        "the sticky subject is independently a genuine small resource"
    );
    let setup = subject.sticky_setup.as_ref().unwrap();
    assert!(
        setup.first_digest.as_deref() == Some(RESOURCE_CALL_SHA256)
            && setup.all_success_hashes_match
            && setup.successful_calls >= 1
            && setup.successful_calls < RESOURCE_EXHAUSTION_ATTEMPTS,
        "the actual parked task first hashes successfully in the independent namespace"
    );
    assert!(
        setup
            .first_success_usage
            .as_ref()
            .is_some_and(
                |usage| usage.encoded_bytes() >= RESOURCE_CALL_SUCCESS_LOWER_BYTES
                    && usage.encoded_bytes() <= RESOURCE_CALL_FIRST_UPPER_BYTES
                    && usage.structural_nodes() > 0
                    && usage.structural_nodes() <= RESOURCE_ACCOUNT_NODES
            ),
        "the known tiny successful call fits its conservative Source bounds"
    );
    assert!(
        matches!(&setup.first_error, Some(PermissionError::LimitExceeded))
            && setup.attempted_calls <= RESOURCE_EXHAUSTION_ATTEMPTS
            && setup.successful_calls.checked_add(1) == Some(setup.attempted_calls),
        "the finite real call loop stops on its first Limit after genuine success"
    );
    assert!(
        setup
            .before_limit_usage
            .as_ref()
            .and_then(|usage| usage
                .encoded_bytes()
                .checked_add(RESOURCE_CALL_SUCCESS_LOWER_BYTES))
            .is_some_and(|required| required > RESOURCE_ACCOUNT_BYTES),
        "remaining capacity cannot fund the proven successful-call lower bound"
    );
    assert!(
        setup
            .after_limit_usage
            .as_ref()
            .is_some_and(|usage| usage.encoded_bytes() <= RESOURCE_ACCOUNT_BYTES
                && usage.structural_nodes() <= RESOURCE_ACCOUNT_NODES)
            && matches!(
                &setup.sticky_probe,
                Some(Err(PermissionError::LimitExceeded))
            ),
        "the real public call entry remains sticky on the identical actual account"
    );

    assert!(
        matches!(
            &subject.resource.resource_result,
            Err(PermissionError::LimitExceeded)
        ),
        "the token-free reader must honor the genuinely exhausted shared account"
    );
    // Storage and account-preservation guards are reached only after refusal.
    assert_quota_resource_storage_preserved(&subject);
    assert!(
        subject.resource_usage_before == setup.after_probe_usage
            && subject.resource_usage_after == subject.resource_usage_before,
        "an exhausted resource refusal preserves the already exhausted account"
    );
}

#[derive(Clone, Copy)]
enum TerminalResourceRecipe {
    Completed,
    AppendJoinFailed,
}

struct TerminalAppendEffect {
    calls: Arc<AtomicUsize>,
}

impl NativeEffect for TerminalAppendEffect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"fixture": true}),
            writes: vec![
                StateWrite::Append {
                    field: "good".into(),
                    items: vec![json!(1), json!(2)],
                },
                StateWrite::Append {
                    field: "bad".into(),
                    items: vec![json!(3)],
                },
            ],
        }))
    }
}

struct TerminalResourceCapture {
    resource: ResourceCapture,
    before_execution: StoredExecution,
    after_execution: StoredExecution,
    completed: StoredExecution,
    initial_input: Map<String, serde_json::Value>,
    prepared: PreparedEffect,
    native_outcome: EffectOutcome,
    invocation_context: InvocationContext,
    callbacks_before_resource: usize,
    callbacks_after_resource: usize,
    callbacks_after_retirement: usize,
}

// One finite real native preparation/invocation/completion reaches each status.
fn capture_terminal_resource_after_normal_retirement(
    recipe: TerminalResourceRecipe,
) -> TerminalResourceCapture {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("resource-terminal-thread").unwrap();
    let execution_id = ExecutionId::new("resource-terminal-execution").unwrap();
    let definition = Definition {
        route: RouteKey::new("/resource-terminal#workflow").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("terminal-task").unwrap(),
            effect_id: EffectId::new("terminal-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]],
        max_attempts: 4,
    };
    let origin = ThreadOrigin {
        application: "resource-terminal-app".into(),
        tenant: "fixture-tenant".into(),
        principal: "fixture-principal".into(),
    };
    let callback = Arc::new(TerminalAppendEffect {
        calls: native_calls.clone(),
    });
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: origin.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    EffectId::new("terminal-effect").unwrap(),
                    EffectBinding {
                        callback: callback.clone(),
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
        "genuine native definition is exact"
    );
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert!(
        original_owner == 1,
        "the genuine fresh writer owns generation one"
    );
    assert!(
        store.approval_durable(),
        "real SQLite has durable approval storage"
    );
    assert!(
        !store.durable_permissions(),
        "terminal controls claim no persistent profile"
    );
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(origin),
        })
        .unwrap();
    let initial_input = match recipe {
        TerminalResourceRecipe::Completed => {
            Map::from_iter([("good".into(), json!([])), ("bad".into(), json!([]))])
        }
        TerminalResourceRecipe::AppendJoinFailed => {
            Map::from_iter([("good".into(), json!([])), ("bad".into(), json!(42))])
        }
    };
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("resource-terminal-turn").unwrap(),
        original_owner,
        registered,
        initial_input.clone(),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread_id, initial).unwrap();
    let (prepared, native_outcome, completed, invocation_context) = {
        let runtime = Runtime::new(store.as_ref());
        let started = runtime
            .apply(
                &execution_id,
                (&admitted.snapshot).into(),
                KernelCommand::Start,
                facts(101),
            )
            .unwrap();
        let prepared = runtime
            .prepare_dispatch(
                &execution_id,
                (&started.snapshot).into(),
                TaskId::new("terminal-task").unwrap(),
                AttemptId::new("terminal-attempt").unwrap(),
                facts(102),
            )
            .unwrap();
        let real_thread = store.thread(&thread_id).unwrap().unwrap();
        let invocation_context = InvocationContext {
            thread_id: real_thread.id,
            thread_generation: real_thread.generation,
            origin: real_thread.origin.unwrap(),
            application_revision: application.identity().revision.clone(),
            execution_revision: prepared.execution.snapshot.definition.revision.clone(),
            policy_revision: "policy-1".into(),
        };
        let native_outcome = application
            .invoke(
                &prepared.execution.snapshot.definition,
                &prepared.invocation,
                &invocation_context,
            )
            .unwrap();
        let completed = runtime
            .complete_outcome(
                &execution_id,
                prepared.invocation.owner_generation,
                prepared.invocation.task_id.clone(),
                prepared.invocation.attempt_id.clone(),
                &native_outcome,
                facts(103),
            )
            .unwrap();
        (prepared, native_outcome, completed, invocation_context)
    };
    let before_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let before_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let before_execution = store.execution(&execution_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    let callbacks_before_resource = native_calls.load(Ordering::SeqCst);
    let resource_result = {
        let mut budget = PermissionEvidenceBudget::new();
        PermissionStore::permission_resource(store.as_ref(), &execution_id, &mut budget)
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_execution = store.execution(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    let callbacks_after_resource = native_calls.load(Ordering::SeqCst);
    drop(application);
    drop(callback);
    assert!(
        Arc::strong_count(&store) == 1,
        "all original writer aliases retire normally"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_approval = next.approval_view(&execution_id).unwrap().unwrap();
    let next_execution = next.execution(&execution_id).unwrap().unwrap();
    let next_journal = next.journal(&execution_id).unwrap();
    next.readiness().unwrap();
    drop(next);
    let callbacks_after_retirement = native_calls.load(Ordering::SeqCst);
    assert!(
        next_owner == original_owner.checked_add(1).unwrap(),
        "a real next writer advances owner generation"
    );
    assert!(
        next_ordinary == after_ordinary
            && next_approval == after_approval
            && next_execution == after_execution
            && next_journal == after_journal,
        "the real next writer reads the full actually captured AFTER state"
    );
    assert!(
        next_execution.snapshot.owner_generation == original_owner,
        "reopen retains the original committed snapshot owner"
    );
    drop(next_ordinary);
    drop(next_approval);
    drop(next_execution);
    drop(next_journal);
    drop(directory);
    TerminalResourceCapture {
        resource: ResourceCapture {
            resource_result,
            before_ordinary,
            before_approval,
            before_journal,
            after_ordinary,
            after_approval,
            after_journal,
        },
        before_execution,
        after_execution,
        completed,
        initial_input,
        prepared,
        native_outcome,
        invocation_context,
        callbacks_before_resource,
        callbacks_after_resource,
        callbacks_after_retirement,
    }
}

fn assert_terminal_native_evidence(captured: &TerminalResourceCapture) {
    let before = &captured.before_execution.snapshot;
    let prepared = &captured.prepared;
    assert!(
        captured.callbacks_before_resource == 1
            && captured.callbacks_after_resource == 1
            && captured.callbacks_after_retirement == 1,
        "the actual native callback ran once and never replayed during reads or retirement"
    );
    assert!(
        captured.before_execution == captured.completed
            && captured
                .resource
                .before_ordinary
                .thread
                .active_execution_id
                .as_ref()
                == Some(&before.execution_id)
            && captured.resource.before_ordinary.execution.as_ref()
                == Some(&captured.before_execution)
            && captured.resource.before_approval.execution == captured.before_execution
            && captured.resource.before_approval.thread == captured.resource.before_ordinary.thread
            && captured.resource.before_approval.grants.is_empty()
            && captured.resource.before_approval.acceptances.is_empty(),
        "the lawful terminal execution remains the actual selected full resource"
    );
    assert!(
        before.revision == 3
            && before.owner_generation == 1
            && before.updated_at_ms == 103
            && before.attempts_used == 1
            && before.tasks.len() == 1
            && before.definition.batches.len() == 1
            && before.definition.batches[0].len() == 1
            && before.definition.max_attempts == 4
            && before.definition.route.as_str() == "/resource-terminal#workflow"
            && before.definition.revision.as_str() == "definition-1",
        "the committed native lifecycle retains its exact tiny Definition and facts"
    );
    let task = &before.tasks[0];
    assert!(
        task.spec
            == TaskSpec {
                id: TaskId::new("terminal-task").unwrap(),
                effect_id: EffectId::new("terminal-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }
            && before.definition.batches[0][0] == task.spec
            && task.status == TaskStatus::Succeeded
            && task.failure.is_none()
            && task.interrupt.is_none()
            && !task.approved
            && task.input == captured.initial_input,
        "one actual successful native task retains the genuine registered specification and input"
    );
    assert!(
        prepared.execution.thread_id == captured.before_execution.thread_id
            && prepared.execution.snapshot.revision == 2
            && prepared.execution.snapshot.status == ExecutionStatus::Running
            && prepared.execution.snapshot.owner_generation == before.owner_generation
            && prepared.execution.snapshot.definition == before.definition
            && prepared.execution.snapshot.tasks.len() == 1
            && prepared.execution.snapshot.tasks[0].status == TaskStatus::InFlight
            && prepared.execution.snapshot.tasks[0].input == captured.initial_input
            && prepared.execution.snapshot.tasks[0].attempt_id.as_ref()
                == Some(&prepared.invocation.attempt_id),
        "actual preparation durably claimed the exact invocation before native execution"
    );
    assert!(
        prepared.invocation.execution_id == before.execution_id
            && prepared.invocation.turn_id == before.turn_id
            && prepared.invocation.owner_generation == before.owner_generation
            && prepared.invocation.task_id == task.spec.id
            && prepared.invocation.effect_id == task.spec.effect_id
            && prepared.invocation.attempt_id.as_str() == "terminal-attempt"
            && prepared.invocation.input == captured.initial_input
            && task.attempt_id.as_ref() == Some(&prepared.invocation.attempt_id)
            && before.attempt_history == vec![prepared.invocation.attempt_id.clone()]
            && before.turn_history == vec![prepared.invocation.turn_id.clone()],
        "the retained PreparedEffect invocation matches all real committed identities"
    );
    assert!(
        captured.invocation_context.thread_id == captured.resource.before_ordinary.thread.id
            && captured.invocation_context.thread_generation
                == captured.resource.before_ordinary.thread.generation
            && captured.resource.before_ordinary.thread.origin.as_ref()
                == Some(&captured.invocation_context.origin)
            && captured.invocation_context.origin.application == "resource-terminal-app"
            && captured.invocation_context.application_revision == "application-1"
            && captured.invocation_context.execution_revision == before.definition.revision
            && captured.invocation_context.policy_revision == "policy-1",
        "the native context came from the actual thread incarnation, origin and application"
    );
    let expected_result = TaskResult {
        output: json!({"fixture": true}),
        writes: vec![
            StateWrite::Append {
                field: "good".into(),
                items: vec![json!(1), json!(2)],
            },
            StateWrite::Append {
                field: "bad".into(),
                items: vec![json!(3)],
            },
        ],
    };
    let native_result = match &captured.native_outcome {
        EffectOutcome::Succeeded(result) => result,
        _ => panic!("the real fixed native callback returned Succeeded"),
    };
    assert!(
        native_result == &expected_result && task.result.as_ref() == Some(native_result),
        "the exact actual native result and ordered writes remain in the committed task"
    );
}

fn assert_terminal_resource_projection(captured: &TerminalResourceCapture) {
    assert!(
        captured.resource.resource_result.is_ok(),
        "the selected lawful terminal execution has a token-free resource projection"
    );
    let resource = match &captured.resource.resource_result {
        Ok(resource) => resource,
        Err(_) => unreachable!("successful terminal resource control"),
    };
    assert!(
        resource.thread == captured.resource.before_ordinary.thread
            && resource.thread == captured.resource.before_approval.thread
            && resource.execution == captured.before_execution
            && resource.execution == captured.resource.before_approval.execution
            && captured.resource.before_ordinary.execution.as_ref() == Some(&resource.execution),
        "the terminal resource is exactly the full retained ThreadRecord and StoredExecution"
    );
    assert!(
        captured.resource.after_ordinary == captured.resource.before_ordinary
            && captured.resource.after_approval == captured.resource.before_approval
            && captured.after_execution == captured.before_execution
            && captured.resource.after_journal == captured.resource.before_journal,
        "terminal resource reading preserves all complete actual storage and journal state"
    );
}

#[test]
fn permission_resource_projects_genuine_completed_native_execution_after_normal_retirement() {
    let captured =
        capture_terminal_resource_after_normal_retirement(TerminalResourceRecipe::Completed);

    assert_terminal_native_evidence(&captured);
    let expected_input = Map::from_iter([("good".into(), json!([])), ("bad".into(), json!([]))]);
    let expected_values =
        Map::from_iter([("good".into(), json!([1, 2])), ("bad".into(), json!([3]))]);
    let snapshot = &captured.before_execution.snapshot;
    assert!(
        captured.initial_input == expected_input
            && snapshot.status == ExecutionStatus::Completed
            && snapshot.batch_index == 1
            && snapshot.failure.is_none()
            && snapshot.values == expected_values
            && snapshot.output.as_ref() == Some(&serde_json::Value::Object(expected_values)),
        "the actual successful append join commits its known values and Completed output"
    );
    assert_terminal_resource_projection(&captured);
}

#[test]
fn permission_resource_projects_genuine_failed_native_append_join_after_normal_retirement() {
    let captured =
        capture_terminal_resource_after_normal_retirement(TerminalResourceRecipe::AppendJoinFailed);

    assert_terminal_native_evidence(&captured);
    let expected_input = Map::from_iter([("good".into(), json!([])), ("bad".into(), json!(42))]);
    let snapshot = &captured.before_execution.snapshot;
    assert!(
        captured.initial_input == expected_input
            && snapshot.status == ExecutionStatus::Failed
            && snapshot.batch_index == 0
            && snapshot.failure.as_deref() == Some("append_target_not_array")
            && snapshot.output.is_none()
            && snapshot.values == expected_input,
        "the actual failed append join preserves original values after rejecting its scratch copy"
    );
    assert_terminal_resource_projection(&captured);
}

const RESOURCE_TENANT_REPETITIONS: usize = 128;
const RESOURCE_METADATA_ARRAYS: usize = 64;

#[derive(Clone, Copy)]
enum ResourceCompatibilityRecipe {
    UnsupportedVersion,
    OwnerMismatch,
    Utf8Tenant256,
    MetadataDepth65,
}

#[derive(PartialEq)]
struct CompatibilityMetadataRow {
    id: i64,
    schema_version: i64,
    migration_checksum: String,
    owner_epoch: Vec<u8>,
    id_kind: String,
    version_kind: String,
    checksum_kind: String,
    owner_kind: String,
}

struct CompatibilityMetadataCapture {
    original: CompatibilityMetadataRow,
    poisoned_before: CompatibilityMetadataRow,
    poisoned_after: CompatibilityMetadataRow,
    restored: CompatibilityMetadataRow,
}

struct CompatibilityResourceCapture {
    resource: ResourceCapture,
    before_execution: StoredExecution,
    after_execution: StoredExecution,
    raw_metadata: Option<CompatibilityMetadataCapture>,
}

fn compatibility_resource_metadata(
    recipe: ResourceCompatibilityRecipe,
) -> Map<String, serde_json::Value> {
    if !matches!(recipe, ResourceCompatibilityRecipe::MetadataDepth65) {
        return Map::new();
    }
    let mut value = json!(true);
    for _ in 0..RESOURCE_METADATA_ARRAYS {
        value = serde_json::Value::Array(vec![value]);
    }
    Map::from_iter([("nested".into(), value)])
}

fn read_compatibility_metadata_row(connection: &rusqlite::Connection) -> CompatibilityMetadataRow {
    connection.query_row(
        "SELECT id, schema_version, migration_checksum, owner_epoch, typeof(id), typeof(schema_version), typeof(migration_checksum), typeof(owner_epoch) FROM metadata WHERE id = 1",
        [],
        |row| Ok(CompatibilityMetadataRow {
            id: row.get(0)?, schema_version: row.get(1)?,
            migration_checksum: row.get(2)?, owner_epoch: row.get(3)?,
            id_kind: row.get(4)?, version_kind: row.get(5)?,
            checksum_kind: row.get(6)?, owner_kind: row.get(7)?,
        }),
    ).unwrap()
}

// Genuine public input setup; direct metadata corruption is captured and restored.
fn capture_compatibility_resource_after_normal_retirement(
    recipe: ResourceCompatibilityRecipe,
) -> CompatibilityResourceCapture {
    let directory = Directory::new();
    let native_calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("human-gate-thread").unwrap();
    let execution_id = ExecutionId::new("human-gate-execution").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "human-gate-app".into(),
            tenant: if matches!(recipe, ResourceCompatibilityRecipe::Utf8Tenant256) {
                "é".repeat(RESOURCE_TENANT_REPETITIONS)
            } else {
                "fixture-tenant".into()
            },
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
            metadata: compatibility_resource_metadata(recipe),
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
    let before_execution = store.execution(&execution_id).unwrap().unwrap();
    let before_journal = store.journal(&execution_id).unwrap();
    assert!(
        before_ordinary.execution.as_ref() == Some(&before_execution)
            && before_approval.execution == before_execution,
        "all full genuine execution projections agree before the reader"
    );
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
    }
    let raw_connection = if matches!(
        recipe,
        ResourceCompatibilityRecipe::UnsupportedVersion
            | ResourceCompatibilityRecipe::OwnerMismatch
    ) {
        Some(rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap())
    } else {
        None
    };
    let original_metadata = raw_connection.as_ref().map(read_compatibility_metadata_row);
    if let (Some(connection), Some(original)) = (&raw_connection, &original_metadata) {
        assert!(
            original.id == 1
                && original.schema_version == 4
                && original.migration_checksum.len() == 64
                && original.owner_epoch.as_slice() == original_owner.to_be_bytes().as_slice()
                && original.id_kind == "integer"
                && original.version_kind == "integer"
                && original.checksum_kind == "text"
                && original.owner_kind == "blob",
            "the raw negative starts from the genuine typed current metadata row"
        );
        let changed = match recipe {
            ResourceCompatibilityRecipe::UnsupportedVersion => connection
                .execute(
                    "UPDATE metadata SET schema_version = ?1 WHERE id = 1",
                    [5_i64],
                )
                .unwrap(),
            ResourceCompatibilityRecipe::OwnerMismatch => {
                let mismatched_owner = 2_u64.to_be_bytes();
                connection
                    .execute(
                        "UPDATE metadata SET owner_epoch = ?1 WHERE id = 1",
                        [&mismatched_owner[..]],
                    )
                    .unwrap()
            }
            _ => unreachable!("only the two reviewed raw corruption recipes have a connection"),
        };
        assert!(
            changed == 1,
            "the single-cell raw negative updates exactly one row"
        );
    }
    let poisoned_before = raw_connection.as_ref().map(read_compatibility_metadata_row);
    let resource_result = {
        let mut budget = PermissionEvidenceBudget::new();
        // Pass one actual evidence account through the token-free resource port.
        PermissionStore::permission_resource(store.as_ref(), &execution_id, &mut budget)
    };
    let poisoned_after = raw_connection.as_ref().map(read_compatibility_metadata_row);
    // Restore the exact original row before ordinary views and real next ownership.
    let raw_metadata = match (
        &raw_connection,
        original_metadata,
        poisoned_before,
        poisoned_after,
    ) {
        (Some(connection), Some(original), Some(poisoned_before), Some(poisoned_after)) => {
            let restored_rows = connection.execute(
                "UPDATE metadata SET id = ?1, schema_version = ?2, migration_checksum = ?3, owner_epoch = ?4 WHERE id = 1",
                rusqlite::params![original.id, original.schema_version,
                    original.migration_checksum.as_str(), original.owner_epoch.as_slice()],
            ).unwrap();
            assert!(
                restored_rows == 1,
                "test cleanup restores exactly one original metadata row"
            );
            let restored = read_compatibility_metadata_row(connection);
            Some(CompatibilityMetadataCapture {
                original,
                poisoned_before,
                poisoned_after,
                restored,
            })
        }
        (None, None, None, None) => None,
        _ => unreachable!("the finite raw metadata capture is complete"),
    };
    let after_ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    let after_approval = store.approval_view(&execution_id).unwrap().unwrap();
    let after_execution = store.execution(&execution_id).unwrap().unwrap();
    let after_journal = store.journal(&execution_id).unwrap();
    store.readiness().unwrap();
    drop(pending);
    drop(service);
    drop(authority);
    drop(application);
    drop(admitted);
    drop(started);
    drop(raw_connection);
    assert!(
        Arc::strong_count(&store) == 1,
        "all original writer aliases retire normally"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_approval = next.approval_view(&execution_id).unwrap().unwrap();
    let next_execution = next.execution(&execution_id).unwrap().unwrap();
    let next_journal = next.journal(&execution_id).unwrap();
    next.readiness().unwrap();
    drop(next);
    assert!(
        next_owner == original_owner.checked_add(1).unwrap(),
        "a genuine next writer advances owner generation"
    );
    assert!(
        next_ordinary == after_ordinary
            && next_approval == after_approval
            && next_execution == after_execution,
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
    drop(next_ordinary);
    drop(next_approval);
    drop(next_execution);
    drop(next_journal);
    drop(directory);
    CompatibilityResourceCapture {
        resource: ResourceCapture {
            resource_result,
            before_ordinary,
            before_approval,
            before_journal,
            after_ordinary,
            after_approval,
            after_journal,
        },
        before_execution,
        after_execution,
        raw_metadata,
    }
}

fn assert_compatibility_resource_storage_preserved(captured: &CompatibilityResourceCapture) {
    assert!(
        captured.resource.after_ordinary == captured.resource.before_ordinary
            && captured.resource.after_approval == captured.resource.before_approval
            && captured.after_execution == captured.before_execution
            && captured.resource.after_journal == captured.resource.before_journal,
        "the resource read preserves complete restored ordinary, approval, execution and journal state"
    );
}

fn assert_compatibility_raw_metadata_preserved(captured: &CompatibilityResourceCapture) {
    let raw = captured.raw_metadata.as_ref().unwrap();
    assert!(
        raw.poisoned_after == raw.poisoned_before,
        "the subject reader preserves the entire actually captured typed poisoned row"
    );
    assert!(
        raw.restored == raw.original,
        "explicit test cleanup restores every cell and storage class before normal retirement"
    );
    assert_compatibility_resource_storage_preserved(captured);
}

#[test]
fn permission_resource_preserves_unsupported_metadata_version_refusal_after_normal_retirement() {
    let small = capture_resource_after_normal_retirement();
    let subject = capture_compatibility_resource_after_normal_retirement(
        ResourceCompatibilityRecipe::UnsupportedVersion,
    );

    assert_exact_small_resource_capture(&small);
    assert!(
        matches!(
            &subject.resource.resource_result,
            Err(PermissionError::Store(StoreError::UnsupportedSchema))
        ),
        "the same reader preserves the inherited unsupported metadata version refusal"
    );
    let raw = subject.raw_metadata.as_ref().unwrap();
    assert!(
        raw.poisoned_before.id == raw.original.id
            && raw.poisoned_before.schema_version == 5
            && raw.poisoned_before.migration_checksum == raw.original.migration_checksum
            && raw.poisoned_before.owner_epoch == raw.original.owner_epoch
            && raw.poisoned_before.id_kind == raw.original.id_kind
            && raw.poisoned_before.version_kind == raw.original.version_kind
            && raw.poisoned_before.checksum_kind == raw.original.checksum_kind
            && raw.poisoned_before.owner_kind == raw.original.owner_kind,
        "the raw negative changes only the one reviewed schema version integer cell"
    );
    assert_compatibility_raw_metadata_preserved(&subject);
}

#[test]
fn permission_resource_preserves_live_owner_mismatch_refusal_after_normal_retirement() {
    let small = capture_resource_after_normal_retirement();
    let subject = capture_compatibility_resource_after_normal_retirement(
        ResourceCompatibilityRecipe::OwnerMismatch,
    );

    assert_exact_small_resource_capture(&small);
    assert!(
        matches!(
            &subject.resource.resource_result,
            Err(PermissionError::Store(StoreError::OwnerConflict))
        ),
        "the same reader refuses metadata owner two under the actual live writer one"
    );
    let raw = subject.raw_metadata.as_ref().unwrap();
    assert!(
        raw.poisoned_before.id == raw.original.id
            && raw.poisoned_before.schema_version == raw.original.schema_version
            && raw.poisoned_before.migration_checksum == raw.original.migration_checksum
            && raw.poisoned_before.owner_epoch.as_slice() == 2_u64.to_be_bytes().as_slice()
            && raw.poisoned_before.id_kind == raw.original.id_kind
            && raw.poisoned_before.version_kind == raw.original.version_kind
            && raw.poisoned_before.checksum_kind == raw.original.checksum_kind
            && raw.poisoned_before.owner_kind == raw.original.owner_kind,
        "the raw negative changes only the one reviewed eight-byte owner blob"
    );
    assert_compatibility_raw_metadata_preserved(&subject);
}

#[test]
fn permission_resource_projects_genuine_utf8_origin_byte_boundary_after_normal_retirement() {
    let small = capture_resource_after_normal_retirement();
    let subject = capture_compatibility_resource_after_normal_retirement(
        ResourceCompatibilityRecipe::Utf8Tenant256,
    );

    assert_exact_small_resource_capture(&small);
    assert!(
        subject.resource.resource_result.is_ok(),
        "the same reader projects the genuine legal 256-byte UTF-8 origin"
    );
    assert_exact_small_resource_capture(&subject.resource);
    let tenant = &subject
        .resource
        .before_ordinary
        .thread
        .origin
        .as_ref()
        .unwrap()
        .tenant;
    assert!(
        tenant == &"é".repeat(RESOURCE_TENANT_REPETITIONS)
            && tenant.len() == 256
            && tenant.chars().count() == RESOURCE_TENANT_REPETITIONS,
        "the genuine retained tenant reaches the UTF-8 byte limit with 128 legal characters"
    );
    assert!(
        subject.raw_metadata.is_none(),
        "the lawful origin uses no raw corruption connection"
    );
    assert_compatibility_resource_storage_preserved(&subject);
}

#[test]
fn permission_resource_projects_genuine_metadata_depth65_after_normal_retirement() {
    let small = capture_resource_after_normal_retirement();
    let subject = capture_compatibility_resource_after_normal_retirement(
        ResourceCompatibilityRecipe::MetadataDepth65,
    );

    assert_exact_small_resource_capture(&small);
    assert!(
        subject.resource.resource_result.is_ok(),
        "the same reader preserves the lawful resource depth namespace beyond record depth64"
    );
    assert_exact_small_resource_capture(&subject.resource);
    let metadata = &subject.resource.before_ordinary.thread.metadata;
    assert!(
        metadata.len() == 1,
        "the genuine metadata has one independently known field"
    );
    let mut nested = metadata.get("nested").unwrap();
    for _ in 0..RESOURCE_METADATA_ARRAYS {
        nested = match nested {
            serde_json::Value::Array(items) if items.len() == 1 => &items[0],
            _ => panic!("the genuine bounded metadata retains each single-element array"),
        };
    }
    assert!(
        nested == &json!(true) && serde_json::to_vec(metadata).unwrap().len() == 143,
        "64 known arrays under the metadata object retain depth65 and 143 UTF-8 JSON bytes"
    );
    assert!(
        subject.raw_metadata.is_none(),
        "the lawful depth uses only public ThreadSeed input"
    );
    assert_compatibility_resource_storage_preserved(&subject);
}
