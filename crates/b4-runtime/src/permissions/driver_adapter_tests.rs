use super::*;
use b4_contract::{
    Definition, EffectId, ExecutionRevision, RecoveryPolicy, RouteKey, TaskResult, TaskSpec, TurnId,
};
use serde_json::{json, Map};

// Data-only kernel observations. These construct no Receipt, Request, Permit or
// outcome Commit, and confer no permission or storage authority.
fn observations() -> (PreparedEffect, CommitPlan) {
    let task = TaskId::new("task").unwrap();
    let attempt = AttemptId::new("attempt").unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("execution").unwrap(),
        TurnId::new("turn").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/test#agent").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task.clone(),
                effect_id: EffectId::new("effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }]],
            max_attempts: 3,
        },
        Map::from_iter([("input".into(), json!({"v":true}))]),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap();
    let ready = b4_kernel::transition(
        &initial,
        b4_contract::KernelCommand::Start,
        TransitionFacts { now_ms: 2 },
    )
    .unwrap()
    .snapshot;
    let claimed = b4_kernel::transition(
        &ready,
        b4_contract::KernelCommand::Dispatch {
            task_id: task.clone(),
            attempt_id: attempt.clone(),
        },
        TransitionFacts { now_ms: 3 },
    )
    .unwrap()
    .snapshot;
    let invocation = EffectInvocation {
        execution_id: claimed.execution_id.clone(),
        turn_id: claimed.turn_id.clone(),
        owner_generation: 1,
        task_id: task.clone(),
        effect_id: claimed.tasks[0].spec.effect_id.clone(),
        attempt_id: attempt.clone(),
        input: claimed.tasks[0].input.clone(),
    };
    let completed = b4_kernel::transition(
        &claimed,
        b4_contract::KernelCommand::Complete {
            task_id: task,
            attempt_id: attempt,
            result: TaskResult {
                output: json!(true),
                writes: vec![],
            },
        },
        TransitionFacts { now_ms: 4 },
    )
    .unwrap();
    (
        PreparedEffect {
            execution: StoredExecution {
                thread_id: ThreadId::new("thread").unwrap(),
                snapshot: claimed,
            },
            invocation,
        },
        completed,
    )
}

#[test]
fn candidate_identity_fence_and_full_plan_are_exact() {
    let (prepared, expected) = observations();
    let id = &prepared.invocation.execution_id;
    let fence = ExecutionFence::from(&prepared.execution.snapshot);
    assert!(candidate_matches(
        id, fence, &expected, id, fence, &expected
    ));
    assert!(!candidate_matches(
        id,
        fence,
        &expected,
        &ExecutionId::new("other").unwrap(),
        fence,
        &expected
    ));
    for changed in [
        ExecutionFence {
            revision: fence.revision + 1,
            ..fence
        },
        ExecutionFence {
            owner_generation: fence.owner_generation + 1,
            ..fence
        },
    ] {
        assert!(!candidate_matches(
            id, fence, &expected, id, changed, &expected
        ));
    }
    let mutations: Vec<fn(&mut CommitPlan)> = vec![
        |p| p.expected_revision += 1,
        |p| p.snapshot.schema_version += 1,
        |p| p.snapshot.revision += 1,
        |p| p.snapshot.execution_id = ExecutionId::new("other").unwrap(),
        |p| p.snapshot.turn_id = TurnId::new("other").unwrap(),
        |p| p.snapshot.turn_history.push(TurnId::new("other").unwrap()),
        |p| {
            p.snapshot
                .attempt_history
                .push(AttemptId::new("other").unwrap())
        },
        |p| p.snapshot.owner_generation += 1,
        |p| p.snapshot.definition.route = RouteKey::new("/other#agent").unwrap(),
        |p| p.snapshot.definition.revision = ExecutionRevision::new("v2").unwrap(),
        |p| p.snapshot.definition.batches[0][0].recovery = RecoveryPolicy::Unsafe,
        |p| p.snapshot.definition.max_attempts += 1,
        |p| p.snapshot.status = ExecutionStatus::Failed,
        |p| p.snapshot.batch_index += 1,
        |p| {
            p.snapshot.values.insert("extra".into(), json!(true));
        },
        |p| p.snapshot.tasks[0].spec.id = TaskId::new("other").unwrap(),
        |p| p.snapshot.tasks[0].spec.effect_id = EffectId::new("other").unwrap(),
        |p| p.snapshot.tasks[0].status = TaskStatus::Failed,
        |p| p.snapshot.tasks[0].approved = !p.snapshot.tasks[0].approved,
        |p| {
            p.snapshot.tasks[0]
                .input
                .insert("extra".into(), json!(true));
        },
        |p| p.snapshot.tasks[0].attempt_id = Some(AttemptId::new("other").unwrap()),
        |p| p.snapshot.tasks[0].result.as_mut().unwrap().output = json!(false),
        |p| p.snapshot.tasks[0].failure = Some("failed".into()),
        |p| {
            p.snapshot.tasks[0].interrupt = Some(b4_contract::PendingApproval {
                id: b4_contract::InterruptId::new("other").unwrap(),
                prompt: json!(null),
            })
        },
        |p| p.snapshot.attempts_used += 1,
        |p| p.snapshot.updated_at_ms += 1,
        |p| p.snapshot.output = Some(json!(false)),
        |p| p.snapshot.failure = Some("failed".into()),
        |p| p.dispatchable.push(TaskId::new("other").unwrap()),
    ];
    for change in mutations {
        let mut candidate = expected.clone();
        change(&mut candidate);
        assert!(!candidate_matches(
            id, fence, &expected, id, fence, &candidate
        ));
    }
}

#[test]
fn completion_ticket_binds_exact_facts_and_phase() {
    let (prepared, plan) = observations();
    let facts = TransitionFacts { now_ms: 4 };
    let fence = ExecutionFence::from(&prepared.execution.snapshot);
    let mut tickets = CompletionTickets::default();
    let mut account = DriverAdapterAccount::default();
    assert_eq!(
        tickets.observe(
            false,
            Some(&prepared),
            &prepared.execution,
            facts.clone(),
            &mut account
        ),
        facts
    );
    assert!(tickets.ticket.is_none());
    assert!(!tickets.fault);
    assert_eq!(
        tickets.observe(
            true,
            Some(&prepared),
            &prepared.execution,
            facts.clone(),
            &mut account
        ),
        facts
    );
    assert!(tickets
        .validate(
            &prepared.invocation.execution_id,
            fence,
            &plan,
            &mut account
        )
        .is_ok());
    let mut changed = plan.clone();
    changed.snapshot.updated_at_ms += 1;
    assert!(tickets
        .validate(
            &prepared.invocation.execution_id,
            fence,
            &changed,
            &mut account
        )
        .is_err());
    tickets.observe(
        true,
        Some(&prepared),
        &prepared.execution,
        facts.clone(),
        &mut account,
    );
    assert!(tickets.fault);
    for kind in 0..4 {
        let mut tickets = CompletionTickets::default();
        let mut account = DriverAdapterAccount::default();
        let mut current = prepared.execution.clone();
        match kind {
            0 => current.snapshot.execution_id = ExecutionId::new("other").unwrap(),
            1 => current.snapshot.tasks[0].attempt_id = Some(AttemptId::new("other").unwrap()),
            2 => current.snapshot.definition.max_attempts += 1,
            _ => {}
        }
        let actual = if kind == 3 {
            TransitionFacts { now_ms: 2 }
        } else {
            facts.clone()
        };
        assert_eq!(
            tickets.observe(
                true,
                Some(&prepared),
                &current,
                actual.clone(),
                &mut account
            ),
            actual
        );
        assert!(tickets.fault);
        assert!(tickets.ticket.is_none());
    }
}

#[test]
fn completion_ticket_install_failure_latches_fault_without_changing_facts() {
    let (prepared, plan) = observations();
    let mut healthy = CompletionTickets::default();
    healthy.observe(
        true,
        Some(&prepared),
        &prepared.execution,
        TransitionFacts { now_ms: 4 },
        &mut DriverAdapterAccount::default(),
    );
    assert!(
        healthy.ticket.is_some(),
        "healthy installation establishes the contrasting exhausted case"
    );
    let mut tickets = CompletionTickets::default();
    let mut account = DriverAdapterAccount::default();
    assert!(account.purchase(usize::MAX, 0, 0).is_err());
    let actual = TransitionFacts { now_ms: 4 };
    assert_eq!(
        tickets.observe(
            true,
            Some(&prepared),
            &prepared.execution,
            actual.clone(),
            &mut account
        ),
        actual
    );
    assert!(tickets.fault);
    assert!(tickets.ticket.is_none());
    assert!(tickets
        .validate(
            &prepared.invocation.execution_id,
            (&prepared.execution.snapshot).into(),
            &plan,
            &mut account
        )
        .is_err());
    assert_eq!(
        prepared.execution.snapshot.tasks[0].attempt_id,
        Some(prepared.invocation.attempt_id)
    );
}
