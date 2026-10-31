#[path = "../../b4-sqlite/tests/unfinished_discovery_support/mod.rs"]
mod directory;
mod unfinished_discovery_recovery_support;

use axum::http::StatusCode;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::ApprovalStore, *};
use b4_sqlite::SqliteStore;
use directory::Directory;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use unfinished_discovery_recovery_support::*;

#[tokio::test]
async fn discovered_current_owner_does_not_authorize_recovery() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let scripted = route(Phase::Accepted);
    let authority = seed(store.clone(), &scripted, Phase::Accepted);
    let before = directory.rows();
    let (page, target) = discovered_target(store.as_ref(), false);
    assert_eq!(directory.rows(), before);
    let host = build_host(
        store.clone(),
        scripted,
        Arc::new(Policy(AtomicBool::new(true))),
    );
    let response = recover(&host, principal(), target).await;
    assert_eq!(response.0, StatusCode::CONFLICT);
    assert!(response.1.contains("recovery_not_needed"));
    assert_eq!(directory.rows(), before);
    assert_eq!(
        store
            .approval_view(&authority.execution.snapshot.execution_id)
            .unwrap(),
        Some(authority)
    );
    retire(host, None, store, directory.path()).await;
    assert_eq!(page.owner_generation, 1);
}

#[tokio::test]
async fn discovered_prior_park_preserves_actual_grants_and_pending_identity_over_authenticated_tcp()
{
    let directory = Directory::new();
    let scripted = route(Phase::Parked);
    let original = {
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed(store, &scripted, Phase::Parked)
    };
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    assert_eq!(store.owner_generation(), 2);
    let before = directory.rows();
    let (page, target) = discovered_target(store.as_ref(), true);
    assert_eq!(page.records[0].status, ExecutionStatus::WaitingForApproval);
    assert!(!page.records[0].has_in_flight_task);
    assert!(!page.records[0].has_outcome_unknown_task);
    assert_eq!(directory.rows(), before);
    let host = build_host(
        store.clone(),
        scripted,
        Arc::new(Policy(AtomicBool::new(true))),
    );
    let server = Server::start(&host).await;
    assert_eq!(server.get("/threads/discovered-thread", false).await.0, 401);
    assert_eq!(server.get("/threads/discovered-thread", true).await.0, 200);
    let response = recover(&host, principal(), target.clone()).await;
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1.matches("event: interrupt").count(), 1);
    assert_eq!(response.1.matches("event: done").count(), 1);
    assert!(response.1.contains("original-private-interrupt"));
    assert!(response.1.contains(original.grants[0].token.as_str()));
    let after = store.approval_view(&target.execution_id).unwrap().unwrap();
    assert_eq!(after.grants, original.grants);
    assert_eq!(after.acceptances, original.acceptances);
    assert_eq!(
        after.execution.snapshot.tasks,
        original.execution.snapshot.tasks
    );
    assert_eq!(
        after.execution.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(after.execution.snapshot.attempts_used, 0);
    assert!(!after.execution.snapshot.values.contains_key("calls"));
    let pending = server
        .get("/threads/discovered-thread/pending_interrupts", true)
        .await;
    assert_eq!(pending.0, 200);
    assert!(pending.1.contains(original.grants[0].token.as_str()));
    let grants_and_acceptances = directory
        .rows()
        .into_iter()
        .filter(|(table, _)| table.starts_with("approval_"))
        .collect::<Vec<_>>();
    assert_eq!(
        grants_and_acceptances,
        before
            .into_iter()
            .filter(|(table, _)| table.starts_with("approval_"))
            .collect::<Vec<_>>()
    );
    retire(host, Some(server), store, directory.path()).await;
    assert_eq!(page.records[0].execution_fence.owner_generation, 1);
}

#[tokio::test]
async fn discovered_prior_ready_continues_committed_once_without_new_approval() {
    let directory = Directory::new();
    let scripted = route(Phase::Ready);
    let original = {
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed(store, &scripted, Phase::Ready)
    };
    assert_eq!(original.execution.snapshot.status, ExecutionStatus::Running);
    assert_eq!(
        original.execution.snapshot.tasks[0].status,
        TaskStatus::Ready
    );
    assert!(original.grants[0].consumed);
    assert_eq!(original.acceptances.len(), 1);
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = directory.rows();
    let (page, target) = discovered_target(store.as_ref(), true);
    assert_eq!(directory.rows(), before);
    let host = build_host(
        store.clone(),
        scripted,
        Arc::new(Policy(AtomicBool::new(true))),
    );
    let response = recover(&host, principal(), target.clone()).await;
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1.matches("event: done").count(), 1);
    assert_eq!(response.1.matches("event: interrupt").count(), 0);
    assert!(!response.1.contains("b4ag_"));
    let after = store.approval_view(&target.execution_id).unwrap().unwrap();
    assert_eq!(after.grants, original.grants);
    assert_eq!(after.acceptances, original.acceptances);
    assert_eq!(after.execution.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(after.execution.snapshot.attempts_used, 1);
    assert_eq!(
        after.execution.snapshot.values["calls"],
        serde_json::json!(["once"])
    );
    assert_eq!(
        after.execution.snapshot.tasks[0]
            .result
            .as_ref()
            .unwrap()
            .output,
        serde_json::json!("finite-fixture-result")
    );
    let duplicate_before = directory.rows();
    assert_eq!(
        recover(&host, principal(), target).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(directory.rows(), duplicate_before);
    retire(host, None, store, directory.path()).await;
    assert_eq!(page.records[0].execution_fence.owner_generation, 1);
}

#[tokio::test]
async fn discovered_target_requires_renewed_policy_origin_and_exact_fences() {
    let directory = Directory::new();
    let scripted = route(Phase::Accepted);
    {
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed(store, &scripted, Phase::Accepted);
    }
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = directory.rows();
    let (page, target) = discovered_target(store.as_ref(), true);
    let policy = Arc::new(Policy(AtomicBool::new(false)));
    let host = build_host(store.clone(), scripted, policy.clone());
    let denied = recover(&host, principal(), target.clone()).await;
    assert_eq!(denied.0, StatusCode::NOT_FOUND);
    assert!(denied.1.contains("resource_unavailable"));
    assert_eq!(directory.rows(), before);
    policy.0.store(true, Ordering::Release);
    let foreign = Principal {
        principal: "wrong-origin".into(),
        ..principal()
    };
    assert_eq!(
        recover(&host, foreign, target.clone()).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(directory.rows(), before);
    let mut targets = Vec::new();
    let mut changed = target.clone();
    changed.fence.resource_revision += 1;
    targets.push(changed);
    let mut changed = target.clone();
    changed.fence.execution.revision += 1;
    targets.push(changed);
    let mut changed = target.clone();
    changed.fence.execution.owner_generation += 1;
    targets.push(changed);
    let mut changed = target.clone();
    changed.thread_generation = ThreadGeneration::new("0".repeat(64)).unwrap();
    targets.push(changed);
    let mut changed = target;
    changed.execution_id = ExecutionId::new("wrong-execution").unwrap();
    targets.push(changed);
    for target in targets {
        let response = recover(&host, principal(), target).await;
        assert_eq!(response.0, StatusCode::CONFLICT);
        assert!(!response.1.contains("b4ag_"));
        assert_eq!(directory.rows(), before);
    }
    retire(host, None, store, directory.path()).await;
    assert_eq!(page.owner_generation, 2);
}

#[tokio::test]
async fn discovered_target_cannot_replace_the_full_immutable_definition() {
    let directory = Directory::new();
    let scripted = route(Phase::Accepted);
    {
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed(store, &scripted, Phase::Accepted);
    }
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = directory.rows();
    let (_, target) = discovered_target(store.as_ref(), true);
    let mut changed = scripted;
    changed.definition.max_attempts += 1;
    let host = build_host(
        store.clone(),
        changed,
        Arc::new(Policy(AtomicBool::new(true))),
    );
    let response = recover(&host, principal(), target).await;
    assert_eq!(response.0, StatusCode::CONFLICT);
    assert!(response.1.contains("recovery_definition_unavailable"));
    assert_eq!(directory.rows(), before);
    retire(host, None, store, directory.path()).await;
}

#[tokio::test]
async fn discovered_unsafe_inflight_and_blocked_work_is_observation_without_automatic_retry() {
    let directory = Directory::new();
    let scripted = route(Phase::Unsafe);
    let original = {
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed(store, &scripted, Phase::Unsafe)
    };
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let before = directory.rows();
    let (page, target) = discovered_target(store.as_ref(), true);
    assert!(page.records[0].has_in_flight_task);
    assert!(!page.records[0].has_outcome_unknown_task);
    assert_eq!(directory.rows(), before);
    let (host, calls) = native_host(store.clone(), scripted.clone());
    let response = recover(&host, principal(), target.clone()).await;
    assert_eq!(response.0, StatusCode::CONFLICT);
    assert!(response.1.contains("recovery_blocked"));
    let blocked = store.approval_view(&target.execution_id).unwrap().unwrap();
    assert_eq!(
        blocked.execution.snapshot.status,
        ExecutionStatus::RecoveryBlocked
    );
    assert_eq!(
        blocked.execution.snapshot.tasks[0].status,
        TaskStatus::OutcomeUnknown
    );
    assert_eq!(
        blocked.execution.snapshot.attempt_history,
        original.execution.snapshot.attempt_history
    );
    assert_eq!(blocked.execution.snapshot.attempts_used, 1);
    assert!(!blocked.execution.snapshot.values.contains_key("calls"));
    assert_eq!(blocked.grants, original.grants);
    assert_eq!(blocked.acceptances, original.acceptances);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    retire(host, None, store, directory.path()).await;
    let successor = Arc::new(SqliteStore::open(directory.path()).unwrap());
    assert_eq!(successor.owner_generation(), 4);
    let before = directory.rows();
    let (blocked_page, target) = discovered_target(successor.as_ref(), true);
    assert_eq!(
        blocked_page.records[0].status,
        ExecutionStatus::RecoveryBlocked
    );
    assert!(blocked_page.records[0].has_outcome_unknown_task);
    assert!(!blocked_page.records[0].has_in_flight_task);
    assert_eq!(directory.rows(), before);
    let (host, calls) = native_host(successor.clone(), scripted);
    let response = recover(&host, principal(), target.clone()).await;
    assert_eq!(response.0, StatusCode::CONFLICT);
    assert!(response.1.contains("recovery_blocked"));
    let next = successor
        .approval_view(&target.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        next.execution.snapshot.tasks,
        blocked.execution.snapshot.tasks
    );
    assert_eq!(
        next.execution.snapshot.attempt_history,
        blocked.execution.snapshot.attempt_history
    );
    assert!(!next.execution.snapshot.values.contains_key("calls"));
    assert_eq!(calls.load(Ordering::Acquire), 0);
    retire(host, None, successor, directory.path()).await;
}

#[tokio::test]
async fn discovered_old_thread_generation_cannot_adopt_a_successor_thread() {
    let directory = Directory::new();
    let scripted = route(Phase::Parked);
    {
        let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed(store, &scripted, Phase::Parked);
    }
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let (page, target) = discovered_target(store.as_ref(), true);
    let old = store.thread_view(&target.thread_id).unwrap().unwrap();
    store
        .delete_thread_fenced(&ThreadDeletionTarget::from_view(&old).unwrap())
        .unwrap();
    let replacement = store
        .create_thread_with(ThreadSeed {
            id: target.thread_id.clone(),
            created_at_ms: 9,
            metadata: serde_json::Map::new(),
            origin: Some(context().actor),
        })
        .unwrap();
    assert_ne!(replacement.generation, target.thread_generation);
    let before = directory.rows();
    let host = build_host(
        store.clone(),
        scripted,
        Arc::new(Policy(AtomicBool::new(true))),
    );
    let response = recover(&host, principal(), target).await;
    assert_eq!(response.0, StatusCode::CONFLICT);
    assert_eq!(directory.rows(), before);
    retire(host, None, store, directory.path()).await;
    assert_eq!(page.records.len(), 1);
}
