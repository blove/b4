//! Explicit trusted embedding recovery after accepted approval and SQLite reopen.
//! No HTTP listener, provider or external business effect is used.
use axum::body::to_bytes;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::{collections::BTreeMap, sync::Arc};

struct DenyHttp;
impl Authenticator for DenyHttp {
    fn authenticate(&self, _: &axum::http::HeaderMap) -> Result<Principal, AuthenticationError> {
        Err(AuthenticationError)
    }
}
struct Owner;
impl ResourcePolicy for Owner {
    fn authorize(&self, principal: &Principal, resource: &ResourceContext) -> bool {
        resource.origin.as_ref().is_some_and(|origin| {
            origin.application == principal.application
                && origin.tenant == principal.tenant
                && origin.principal == principal.principal
        })
    }
}
struct FixtureAuthority;
impl ApprovalAuthority for FixtureAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        _: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        if context.policy_revision == "recovery-example-policy-v1"
            && resource.thread.origin.as_ref() == Some(&context.actor)
        {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
fn fence(store: &SqliteStore, thread: &ThreadId) -> Result<ApprovalFence, StoreError> {
    let view = store
        .thread_view(thread)?
        .ok_or(StoreError::ThreadNotFound)?;
    Ok(ApprovalFence {
        execution: ExecutionFence::from(
            &view
                .execution
                .ok_or(StoreError::ExecutionNotFound)?
                .snapshot,
        ),
        resource_revision: view.thread.resource_revision,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let directory = args
        .next()
        .ok_or("usage: recover_approvals <data-directory>")?;
    if args.next().is_some() {
        return Err("usage: recover_approvals <data-directory>".into());
    }
    let thread = ThreadId::new("recovery-example-thread")?;
    let execution = ExecutionId::new("recovery-example-execution")?;
    let task = TaskId::new("approved")?;
    let effect = EffectId::new("local-effect")?;
    let route_key = RouteKey::new("/recover#workflow")?;
    let definition = Definition {
        route: route_key.clone(),
        revision: ExecutionRevision::new("recovery-example-v1")?,
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            requires_approval: true,
            recovery: RecoveryPolicy::Pure,
        }]],
        max_attempts: 1,
    };
    let principal = Principal {
        application: "recovery-example".into(),
        tenant: "local".into(),
        principal: "owner".into(),
    };
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: principal.application.clone(),
            tenant: principal.tenant.clone(),
            principal: principal.principal.clone(),
        },
        policy_revision: "recovery-example-policy-v1".into(),
    };
    let prompt = json!({"question":"Approve this local fixture once?"});
    // A controlled trusted fixture clock proves that a committed Once remains
    // accepted when the recovered host uses its later wall clock.
    let preserved_authority = {
        let store = Arc::new(SqliteStore::open(&directory)?);
        store.create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })?;
        let runtime = Runtime::new(store.clone());
        let admitted = runtime.admit(
            &thread,
            b4_kernel::new_execution(
                execution.clone(),
                TurnId::new("initial-turn")?,
                store.owner_generation(),
                definition.clone(),
                Map::from_iter([("input".into(), json!({}))]),
                TransitionFacts { now_ms: 100 },
            )?,
        )?;
        runtime.apply(
            &execution,
            ExecutionFence::from(&admitted.snapshot),
            KernelCommand::Start,
            TransitionFacts { now_ms: 101 },
        )?;
        let service = ApprovalService::new(store.clone(), FixtureAuthority)?;
        service.park(
            &context,
            &execution,
            fence(&store, &thread)?,
            task.clone(),
            InterruptId::new("original-interrupt")?,
            prompt.clone(),
            10,
            TransitionFacts { now_ms: 102 },
        )?;
        let pending = service.pending(&context, &execution, fence(&store, &thread)?)?;
        let accepted = service.accept(
            &execution,
            fence(&store, &thread)?,
            ApprovalAcceptanceRequest {
                context: context.clone(),
                route: route_key.clone(),
                definition_revision: definition.revision.clone(),
                turn_id: TurnId::new("accepted-turn")?,
                now_ms: 103,
                decisions: vec![GrantDecision {
                    interrupt_id: pending[0].interrupt_id.clone(),
                    decision: ApprovalDecision::Once,
                    cancelled: false,
                    token: Some(pending[0].token.clone()),
                }],
            },
        )?;
        assert_eq!(accepted.snapshot.attempts_used, 0);
        assert_eq!(accepted.snapshot.status, ExecutionStatus::Running);
        let view = store
            .approval_view(&execution)?
            .ok_or("authority missing")?;
        (view.grants, view.acceptances)
    };
    let store = Arc::new(SqliteStore::open(&directory)?);
    let view = store.thread_view(&thread)?.ok_or("thread missing")?;
    let target = RecoveryTarget::from_view(&view).ok_or("target missing")?;
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(DenyHttp),
        Arc::new(Owner),
        HostConfig::new(vec![ScriptRoute {
            definition,
            effects: BTreeMap::from([(
                effect,
                ScriptEffect::Result(TaskResult {
                    output: json!({"approved":true}),
                    writes: vec![StateWrite::Append {
                        field: "items".into(),
                        items: vec![json!("approved")],
                    }],
                }),
            )]),
        }]),
        ApprovalHostConfig::new(
            context.policy_revision,
            BTreeMap::from([(route_key, BTreeMap::from([(task, prompt)]))]),
        ),
    )?;
    let response = host.recover_execution(principal, target).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 65_536).await?;
    assert_eq!(
        std::str::from_utf8(&bytes)?,
        "event: done\ndata: {\"output\":{\"approved\":true}}\n\n"
    );
    let final_view = store
        .approval_view(&execution)?
        .ok_or("authority missing")?;
    assert_eq!(final_view.grants, preserved_authority.0);
    assert_eq!(final_view.acceptances, preserved_authority.1);
    assert_eq!(
        final_view.execution.snapshot.status,
        ExecutionStatus::Completed
    );
    assert_eq!(final_view.execution.snapshot.attempts_used, 1);
    assert_eq!(
        final_view.execution.snapshot.values["items"],
        json!(["approved"])
    );
    println!(
        "{}",
        json!({
            "profile":"trusted_known_id_host_recovery_example",
            "passed":true,
            "status":"completed",
            "attempts_used":1,
            "accepted_before_reopen":true,
            "original_grant_and_audit_preserved":true,
            "continued_after_original_ttl":true,
            "native_done_count":1,
            "tokens_recorded":false,
            "transport":"embedding_response_body",
            "limitations":["Controlled fixture clock; no network listener, provider, automatic scan or full crash matrix."]
        })
    );
    Ok(())
}
