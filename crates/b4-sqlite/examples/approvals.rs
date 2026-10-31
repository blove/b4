//! A no-provider approval flow with explicit reopen and known-ID recovery.
use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct FixtureAuthority;
impl ApprovalAuthority for FixtureAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if resource.thread.origin.as_ref() == Some(&context.actor)
            && context.policy_revision == "fixture-policy-v1"
        {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}

fn fence(
    store: &SqliteStore,
    thread: &ThreadId,
) -> Result<ApprovalFence, Box<dyn std::error::Error>> {
    let view = store.thread_view(thread)?.ok_or("fixture thread missing")?;
    Ok(ApprovalFence {
        execution: ExecutionFence::from(
            &view.execution.ok_or("fixture execution missing")?.snapshot,
        ),
        resource_revision: view.thread.resource_revision,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let directory = args
        .next()
        .ok_or("usage: approvals <data-directory> <unique-run-id>")?;
    let run = args
        .next()
        .ok_or("usage: approvals <data-directory> <unique-run-id>")?;
    if args.next().is_some() {
        return Err("usage: approvals <data-directory> <unique-run-id>".into());
    }
    let execution = ExecutionId::new(run.clone())?;
    let thread = ThreadId::new(format!("thread-{run}"))?;
    let task = TaskId::new("approved-fixture")?;
    let route = RouteKey::new("/approval#agent")?;
    let revision = ExecutionRevision::new("fixture-v1")?;
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "example".into(),
            tenant: "local".into(),
            principal: "owner".into(),
        },
        policy_revision: "fixture-policy-v1".into(),
    };
    let mut time = 100_u64;
    let mut facts = || {
        let now_ms = time;
        time += 1;
        TransitionFacts { now_ms }
    };
    let (expected, generation) = {
        let store = Arc::new(SqliteStore::open(&directory)?);
        let created = store.create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })?;
        let runtime = Runtime::new(store.clone());
        let accepted = runtime.admit(
            &thread,
            b4_kernel::new_execution(
                execution.clone(),
                TurnId::new(format!("initial-{run}"))?,
                store.owner_generation(),
                Definition {
                    route: route.clone(),
                    revision: revision.clone(),
                    batches: vec![vec![TaskSpec {
                        id: task.clone(),
                        effect_id: EffectId::new(format!("effect-{run}"))?,
                        recovery: RecoveryPolicy::Pure,
                        requires_approval: true,
                    }]],
                    max_attempts: 2,
                },
                Map::from_iter([("items".into(), json!([]))]),
                facts(),
            )?,
        )?;
        runtime.apply(
            &execution,
            ExecutionFence::from(&accepted.snapshot),
            KernelCommand::Start,
            facts(),
        )?;
        let service = ApprovalService::new(store.clone(), FixtureAuthority)?;
        service.park(
            &context,
            &execution,
            fence(&store, &thread)?,
            task.clone(),
            InterruptId::new(format!("interrupt-{run}"))?,
            json!({"question":"Run this harmless fixture once?"}),
            10,
            facts(),
        )?;
        let pending = service.pending(&context, &execution, fence(&store, &thread)?)?;
        (pending, created.generation)
    };

    let pending_count = expected.len();
    let (restored, generation_restored, acceptance_count) = {
        let store = Arc::new(SqliteStore::open(&directory)?);
        let runtime = Runtime::new(store.clone());
        runtime.recover(
            &execution,
            fence(&store, &thread)?.execution,
            revision.clone(),
            TurnId::new(format!("park-recovery-{run}"))?,
            facts(),
        )?;
        let service = ApprovalService::new(store.clone(), FixtureAuthority)?;
        let pending = service.pending(&context, &execution, fence(&store, &thread)?)?;
        let restored = pending == expected;
        let generation_restored = store
            .thread(&thread)?
            .ok_or("fixture thread missing")?
            .generation
            == generation;
        let accepted = service.accept(
            &execution,
            fence(&store, &thread)?,
            ApprovalAcceptanceRequest {
                context: context.clone(),
                route: route.clone(),
                definition_revision: revision.clone(),
                turn_id: TurnId::new(format!("approved-{run}"))?,
                now_ms: facts().now_ms,
                decisions: pending
                    .into_iter()
                    .map(|entry| GrantDecision {
                        interrupt_id: entry.interrupt_id,
                        decision: ApprovalDecision::Once,
                        cancelled: false,
                        token: Some(entry.token),
                    })
                    .collect(),
            },
        )?;
        if accepted.snapshot.tasks[0].status != TaskStatus::Ready {
            return Err("approval did not prepare continuation".into());
        }
        let audit = store
            .approval_view(&execution)?
            .ok_or("approval acceptance missing")?;
        (restored, generation_restored, audit.acceptances.len())
    };
    drop(expected);
    let store = Arc::new(SqliteStore::open(&directory)?);
    let runtime = Runtime::new(store.clone());
    // The trusted fixture clock deliberately passes the original grant expiry.
    let recovery_facts = TransitionFacts { now_ms: 200 };
    runtime.recover(
        &execution,
        fence(&store, &thread)?.execution,
        revision,
        TurnId::new(format!("continuation-recovery-{run}"))?,
        recovery_facts,
    )?;
    let service = ApprovalService::new(store.clone(), FixtureAuthority)?;
    let calls = AtomicUsize::new(0);
    let completed = service.dispatch_outcome(
        &context,
        &execution,
        fence(&store, &thread)?,
        task,
        AttemptId::new(format!("attempt-{run}"))?,
        TransitionFacts { now_ms: 201 },
        || TransitionFacts { now_ms: 202 },
        |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(EffectOutcome::Succeeded(TaskResult {
                output: json!("approved"),
                writes: vec![StateWrite::Append {
                    field: "items".into(),
                    items: vec![json!("approved once")],
                }],
            }))
        },
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "profile":"durable_required_approval_example", "passed":restored && generation_restored && completed.snapshot.status == ExecutionStatus::Completed,
            "status":completed.snapshot.status, "effect_calls":calls.load(Ordering::SeqCst), "attempts_used":completed.snapshot.attempts_used,
            "pending_count":pending_count, "grant_and_prompt_restored":restored, "thread_generation_restored":generation_restored,
            "accepted_before_reopen":true, "accepted_decisions":acceptance_count, "tokens_recorded":false,
            "limitations":["Embedded SQLite fixture; explicit known-ID recovery and simulated host time; no HTTP, provider, autonomous scanning or process-kill qualification."]
        }))?
    );
    Ok(())
}
