use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::Arc;

pub fn context() -> ApprovalContext {
    ApprovalContext {
        actor: ThreadOrigin {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    }
}
pub fn thread() -> ThreadId {
    ThreadId::new("attachment-thread").unwrap()
}
pub fn execution() -> ExecutionId {
    ExecutionId::new("attachment-execution").unwrap()
}
pub fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}
pub fn fence(view: &ThreadView) -> ApprovalFence {
    ApprovalFence {
        execution: (&view.execution.as_ref().unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}
pub struct Allow;
impl ApprovalAuthority for Allow {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}

pub fn fixture<S: ApprovalStore>(
    store: Arc<S>,
) -> (ThreadView, Vec<SensitivePendingApproval>, ApprovalView) {
    fixture_with_pending_policy(store, "policy-1")
}

pub fn fixture_with_pending_policy<S: ApprovalStore>(
    store: Arc<S>,
    pending_policy: &str,
) -> (ThreadView, Vec<SensitivePendingApproval>, ApprovalView) {
    fixture_with_history(store, pending_policy, ApprovalDecision::Once)
}

pub fn fixture_with_history<S: ApprovalStore>(
    store: Arc<S>,
    pending_policy: &str,
    first_decision: ApprovalDecision,
) -> (ThreadView, Vec<SensitivePendingApproval>, ApprovalView) {
    store
        .create_thread_with(ThreadSeed {
            id: thread(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let specs: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|name| TaskSpec {
            id: TaskId::new(name).unwrap(),
            effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        })
        .collect();
    let initial = b4_kernel::new_execution(
        execution(),
        TurnId::new("original-turn").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/attachment#workflow").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: specs.iter().cloned().map(|task| vec![task]).collect(),
            max_attempts: 2,
        },
        Map::from_iter([("input".into(), json!({"payload":"fixture"}))]),
        facts(1),
    )
    .unwrap();
    let admitted = store.admit(&thread(), initial).unwrap();
    store
        .commit(
            &execution(),
            (&admitted.snapshot).into(),
            b4_kernel::transition(&admitted.snapshot, KernelCommand::Start, facts(2)).unwrap(),
        )
        .unwrap();
    let service =
        ApprovalService::with_config(store.clone(), Allow, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    let view = store.thread_view(&thread()).unwrap().unwrap();
    service
        .park(
            &context(),
            &execution(),
            fence(&view),
            specs[0].id.clone(),
            InterruptId::new("first-interrupt").unwrap(),
            json!({"question":"first?"}),
            100,
            facts(3),
        )
        .unwrap();
    let view = store.thread_view(&thread()).unwrap().unwrap();
    let pending = service
        .pending(&context(), &execution(), fence(&view))
        .unwrap();
    let mut accepted = acceptance(&view, &pending, 4, "approved-turn");
    accepted.decisions[0].decision = first_decision;
    service
        .accept(&execution(), fence(&view), accepted)
        .unwrap();
    if first_decision == ApprovalDecision::Once {
        let view = store.thread_view(&thread()).unwrap().unwrap();
        service
            .dispatch_outcome(
                &context(),
                &execution(),
                fence(&view),
                specs[0].id.clone(),
                AttemptId::new("first-attempt").unwrap(),
                facts(5),
                || facts(6),
                |_| {
                    Ok(EffectOutcome::Succeeded(TaskResult {
                        output: json!("first-completed"),
                        writes: vec![],
                    }))
                },
            )
            .unwrap();
    }
    let view = store.thread_view(&thread()).unwrap().unwrap();
    let mut pending_context = context();
    pending_context.policy_revision = pending_policy.into();
    service
        .park(
            &pending_context,
            &execution(),
            fence(&view),
            specs[1].id.clone(),
            InterruptId::new("second-interrupt").unwrap(),
            json!({"question":"second?"}),
            1,
            facts(7),
        )
        .unwrap();
    let view = store.thread_view(&thread()).unwrap().unwrap();
    let pending = service
        .pending(&pending_context, &execution(), fence(&view))
        .unwrap();
    let records = store.approval_view(&execution()).unwrap().unwrap();
    assert_eq!(records.grants.len(), 2);
    assert_eq!(records.acceptances.len(), 1);
    (view, pending, records)
}

pub fn acceptance(
    view: &ThreadView,
    pending: &[SensitivePendingApproval],
    now_ms: u64,
    turn: &str,
) -> ApprovalAcceptanceRequest {
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    ApprovalAcceptanceRequest {
        context: context(),
        route: snapshot.definition.route.clone(),
        definition_revision: snapshot.definition.revision.clone(),
        turn_id: TurnId::new(turn).unwrap(),
        now_ms,
        decisions: pending
            .iter()
            .map(|pending| GrantDecision {
                interrupt_id: pending.interrupt_id.clone(),
                decision: ApprovalDecision::Once,
                cancelled: false,
                token: Some(pending.token.clone()),
            })
            .collect(),
    }
}

pub fn parallel_fixture<S: ApprovalStore>(store: Arc<S>) -> ThreadView {
    store
        .create_thread_with(ThreadSeed {
            id: thread(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    let specs: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|name| TaskSpec {
            id: TaskId::new(name).unwrap(),
            effect_id: EffectId::new(format!("effect-{name}")).unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        })
        .collect();
    let initial = b4_kernel::new_execution(
        execution(),
        TurnId::new("initial").unwrap(),
        store.owner_generation(),
        Definition {
            route: RouteKey::new("/attachment#workflow").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![specs.clone()],
            max_attempts: 2,
        },
        Map::new(),
        facts(1),
    )
    .unwrap();
    let admitted = store.admit(&thread(), initial).unwrap();
    store
        .commit(
            &execution(),
            (&admitted.snapshot).into(),
            b4_kernel::transition(&admitted.snapshot, KernelCommand::Start, facts(2)).unwrap(),
        )
        .unwrap();
    let writer =
        ApprovalService::with_config(store.clone(), Allow, ApprovalServiceConfig::VolatileFixture)
            .unwrap();
    for (index, spec) in specs.into_iter().enumerate() {
        let view = store.thread_view(&thread()).unwrap().unwrap();
        writer
            .park(
                &context(),
                &execution(),
                fence(&view),
                spec.id,
                InterruptId::new(format!("interrupt-{index}")).unwrap(),
                json!({"question":index}),
                100,
                facts(3 + u64::try_from(index).unwrap()),
            )
            .unwrap();
    }
    store.thread_view(&thread()).unwrap().unwrap()
}
