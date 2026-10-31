//! Independent V8 expectations over real HTTP, real Drivers and real SQLite writes.
#[path = "native_cooperative_support/mod.rs"]
mod cooperative;
#[path = "permission_deletion_support/mod.rs"]
mod deletion;
#[allow(dead_code)]
#[path = "permission_entered_support/mod.rs"]
mod support;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{permissions::*, *};
use b4_sqlite::SqliteStore;
use cooperative::{
    snapshot,
    tcp::{finish, Server},
    until, Callback, Cleanup, Directory, Gate, Return, Series,
};
use deletion::{
    store::{Boundary, DefaultDeclaration},
    *,
};
use serde_json::{Map, Value};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::io::AsyncReadExt;

fn quick() -> Arc<Callback> {
    let callback = Callback::new(Return::Succeed);
    callback.gate.release();
    callback
}
fn catalog(
    callback: Arc<dyn NativeEffect>,
    constraint: Arc<support::Constraint>,
) -> Arc<PermissionCatalog> {
    support::catalog(callback, constraint)
}
async fn success(server: &Server, id: &str) {
    let mut stream = support::open(
        server.address,
        "DELETE",
        &format!("/threads/{id}"),
        "",
        "owner",
    )
    .await;
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let split = bytes.windows(4).position(|b| b == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&bytes[..split]).unwrap();
    assert_eq!(head.split_whitespace().nth(1).unwrap(), "204");
    assert!(head
        .lines()
        .any(|line| line.eq_ignore_ascii_case("cache-control: no-store")));
    assert!(bytes[split + 4..].is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn genuine_once_always_multiple_uses_delete_all_eight_tables_after_reopen_preserving_other_history(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let callback = quick();
    let constraint = Arc::new(support::Constraint::default());
    let catalog = catalog(callback.clone(), constraint.clone());
    let host = deletion_host(
        inner.clone(),
        catalog.clone(),
        Arc::new(DeletePolicy::default()),
    );
    let server = Server::start(&host).await;
    history(&server, &host, "monthly-report", 1, 2).await;
    history(&server, &host, "other-report", 0, 1).await;
    let before = rows(&directory.0);
    let expected = without(&before, "monthly-report");
    for table in 1..9 {
        assert!(
            before.0[table].len() > expected.0[table].len(),
            "genuine history fills each owned table"
        );
    }
    assert_eq!(before.0[2].len() - expected.0[2].len(), 4);
    assert_eq!(
        before.0[5].len() - expected.0[5].len(),
        1,
        "real Once acceptance"
    );
    assert_eq!(
        before.0[6].len() - expected.0[6].len(),
        1,
        "real Always acceptance"
    );
    assert_eq!(
        before.0[8].len() - expected.0[8].len(),
        2,
        "multiple actual saved Uses"
    );
    let old_owner = inner.owner_generation();
    let original = snapshot(&*inner, "monthly-report");
    assert_eq!(original.status, ExecutionStatus::Completed);
    assert_eq!(
        inner
            .thread(&ThreadId::new("monthly-report").unwrap())
            .unwrap()
            .unwrap()
            .metadata,
        Map::new()
    );
    support::retire(host, server, false).await;
    drop(inner);
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(inner.owner_generation(), old_owner + 1);
    assert_eq!(
        snapshot(&*inner, "monthly-report").owner_generation,
        old_owner
    );
    assert!(rows(&directory.0).0[1..] == before.0[1..]);
    let before_generic = rows(&directory.0);
    let target = ThreadDeletionTarget::from_view(
        &inner
            .thread_view(&ThreadId::new("monthly-report").unwrap())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(
        inner.delete_thread_fenced(&target).is_err(),
        "generic Store retains permission-history guard"
    );
    unchanged(&directory.0, &before_generic);
    // Current effect authorization no longer permits execution; historical destruction
    // still depends on the independent current DeleteThread authority.
    constraint.0.store(true, Ordering::Release);
    let host = deletion_host(inner.clone(), catalog, Arc::new(DeletePolicy::default()));
    let server = Server::start(&host).await;
    let response = support::request(&server, "GET", "/b4/capabilities", "").await;
    assert_eq!(response.status, 200);
    let descriptor: CapabilityDescriptorV8 = serde_json::from_str(&response.body).unwrap();
    descriptor.validate().unwrap();
    assert!(
        descriptor
            .features
            .durable_permission_settled_thread_deletion
    );
    assert!(!descriptor.features.fenced_deletion);
    healthy(&server).await;
    let before = rows(&directory.0);
    let expected = without(&before, "monthly-report");
    success(&server, "monthly-report").await;
    assert!(
        rows(&directory.0) == expected,
        "exact physical deletion preserves metadata and other thread"
    );
    assert!(inner
        .thread_view(&ThreadId::new("other-report").unwrap())
        .unwrap()
        .is_some());
    healthy(&server).await;
    support::retire(host, server, false).await;
    drop(inner);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    reopened.readiness().unwrap();
    assert!(reopened
        .thread_view(&ThreadId::new("monthly-report").unwrap())
        .unwrap()
        .is_none());
    assert!(rows(&directory.0).0[1..] == expected.0[1..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_and_saved_cancellation_keep_claim_after_actual_terminal_commit_until_real_worker_retires(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original = Callback::new(Return::Acknowledge);
    let completed = quick();
    let saved = Callback::new(Return::Succeed);
    let _cleanup = [Cleanup(original.gate.clone()), Cleanup(saved.gate.clone())];
    let store = Arc::new(Boundary::new(inner.clone()));
    let catalog = catalog(
        Series::new(vec![original.clone(), completed, saved.clone()]),
        Arc::new(support::Constraint::default()),
    );
    let host = deletion_host(store.clone(), catalog, Arc::new(DeletePolicy::default()));
    let server = Server::start(&host).await;
    for (id, callback, is_original) in [
        ("cancel-original", original, true),
        ("cancel-saved", saved, false),
    ] {
        if !is_original {
            history(&server, &host, id, 0, 0).await;
        }
        let primary = if is_original {
            support::original(&server, id).await
        } else {
            support::reuse(&server, id).await
        };
        until(|| callback.gate.facts().entered).await;
        assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
        assert_eq!(support::cancel(&server, id).await.status, 200);
        assert!(callback.notified());
        support::terminal(&finish(primary, Vec::new()).await);
        code(&delete(&server, id).await, 409, "run_in_flight");
        let terminal_gate = Gate::new();
        let _terminal = Cleanup(terminal_gate.clone());
        *store.after_terminal.lock().unwrap() = Some(terminal_gate.clone());
        callback.gate.release();
        until(|| terminal_gate.facts().entered).await;
        let persisted = snapshot(&*inner, id);
        assert_eq!(persisted.status, ExecutionStatus::Cancelled);
        assert_eq!(
            persisted.tasks[0].status,
            if is_original {
                TaskStatus::Cancelled
            } else {
                TaskStatus::Succeeded
            }
        );
        if !is_original {
            let NativeOutcome::Succeeded(expected) = cooperative::genuine_success() else {
                unreachable!()
            };
            assert_eq!(persisted.tasks[0].result, Some(expected));
        }
        assert!(callback.returned.load(Ordering::Acquire));
        let before = rows(&directory.0);
        code(&delete(&server, id).await, 409, "run_in_flight");
        unchanged(&directory.0, &before);
        assert!(host.shutdown_handle().status().active_owners > 0);
        terminal_gate.release();
        drained(&host).await;
        cooperative::assert_positive(&terminal_gate);
        cooperative::assert_positive(&callback.gate);
        success(&server, id).await;
        assert!(rows(&directory.0) == without(&before, id));
        healthy(&server).await;
    }
    support::retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_authority_privacy_stale_target_profiles_nonterminal_and_finite_mixed_history_boundary(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let catalog = catalog(quick(), Arc::new(support::Constraint::default()));
    let legacy = Arc::new(DefaultDeclaration(inner.clone()));
    assert!(!legacy
        .as_ref()
        .permission_settled_thread_deletion_atomic_durable());
    assert!(!legacy.permission_settled_thread_deletion_atomic_durable());
    assert!(HttpHost::new_with_permission_settled_thread_deletion(
        legacy,
        Arc::new(support::Auth),
        Arc::new(DeletePolicy::default()),
        support::config(catalog.clone())
    )
    .is_err());
    let mut disabled = Boundary::new(inner.clone());
    disabled.disable_deletion = true;
    assert!(HttpHost::new_with_permission_settled_thread_deletion(
        Arc::new(disabled),
        Arc::new(support::Auth),
        Arc::new(DeletePolicy::default()),
        support::config(catalog.clone())
    )
    .is_err());
    let store = Arc::new(Boundary::new(inner.clone()));
    let policy = Arc::new(DeletePolicy::default());
    let host = deletion_host(store.clone(), catalog.clone(), policy.clone());
    let server = Server::start(&host).await;
    history(&server, &host, "protected", 1, 1).await;
    let before = rows(&directory.0);
    policy.denied.store(true, Ordering::Release);
    code(
        &delete(&server, "protected").await,
        404,
        "resource_unavailable",
    );
    unchanged(&directory.0, &before);
    policy.denied.store(false, Ordering::Release);
    let foreign = finish(
        support::open(
            server.address,
            "DELETE",
            "/threads/protected",
            "",
            "foreign",
        )
        .await,
        Vec::new(),
    )
    .await;
    code(&foreign, 404, "resource_unavailable");
    unchanged(&directory.0, &before);
    healthy(&server).await;
    // Let transport/owned-job policy pass, then deny the dedicated current SDK authority.
    let authority = Gate::new();
    let _authority = Cleanup(authority.clone());
    *store.after_capture.lock().unwrap() = Some(authority.clone());
    let pending = support::open(server.address, "DELETE", "/threads/protected", "", "owner").await;
    until(|| authority.facts().entered).await;
    policy.denied.store(true, Ordering::Release);
    authority.release();
    code(
        &finish(pending, Vec::new()).await,
        404,
        "resource_unavailable",
    );
    unchanged(&directory.0, &before);
    policy.denied.store(false, Ordering::Release);
    cooperative::assert_positive(&authority);
    healthy(&server).await;
    let captured = Gate::new();
    let _captured = Cleanup(captured.clone());
    *store.after_view.lock().unwrap() = Some(captured.clone());
    let stale = support::open(server.address, "DELETE", "/threads/protected", "", "owner").await;
    until(|| captured.facts().entered).await;
    // Adversarial fixture-only target change after a real initial metadata capture.
    let connection = db(&directory.0);
    connection
        .execute(
            "UPDATE threads SET metadata=?1 WHERE id='protected'",
            [r#"{"changed":true}"#],
        )
        .unwrap();
    connection.close().unwrap();
    let changed = rows(&directory.0);
    captured.release();
    code(&finish(stale, Vec::new()).await, 409, "resource_changed");
    unchanged(&directory.0, &changed);
    cooperative::assert_positive(&captured);
    healthy(&server).await;
    // Restore the isolated adversarial metadata before the healthy history control.
    db(&directory.0)
        .execute("UPDATE threads SET metadata='{}' WHERE id='protected'", [])
        .unwrap();
    success(&server, "protected").await;
    inner
        .create_thread_with(ThreadSeed {
            id: ThreadId::new("empty").unwrap(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(origin()),
        })
        .unwrap();
    let before = rows(&directory.0);
    code(
        &delete(&server, "empty").await,
        501,
        "permission_operation_unsupported",
    );
    unchanged(&directory.0, &before);
    let parked = support::request(&server, "POST", "/threads/parked/runs/wait", support::RUN).await;
    assert_eq!(parked.status, 200);
    drained(&host).await;
    let before = rows(&directory.0);
    code(&delete(&server, "parked").await, 409, "run_in_flight");
    unchanged(&directory.0, &before);
    healthy(&server).await;
    // Fourteen ordinary Once + one original Always + one actual saved Use = 16.
    history(&server, &host, "sixteen", 14, 1).await;
    let boundary = rows(&directory.0);
    assert_eq!(
        boundary.0[2].len() - without(&boundary, "sixteen").0[2].len(),
        16
    );
    success(&server, "sixteen").await;
    healthy(&server).await;
    history(&server, &host, "seventeen", 15, 1).await;
    let before = rows(&directory.0);
    assert_eq!(
        before.0[2].len() - without(&before, "seventeen").0[2].len(),
        17
    );
    code(
        &delete(&server, "seventeen").await,
        409,
        "permission_limit_exceeded",
    );
    unchanged(&directory.0, &before);
    healthy(&server).await;
    support::retire(host, server, false).await;
    // Old V7 still refuses deletion and publishes its unchanged descriptor version.
    let old = HttpHost::new_with_permission_withheld_cancellation(
        inner.clone(),
        Arc::new(support::Auth),
        Arc::new(DeletePolicy::default()),
        support::config(catalog),
    )
    .unwrap();
    let server = Server::start(&old).await;
    let response = support::request(&server, "GET", "/b4/capabilities", "").await;
    let v7: CapabilityDescriptorV7 = serde_json::from_str(&response.body).unwrap();
    v7.validate().unwrap();
    let value: Value = serde_json::from_str(&response.body).unwrap();
    assert_eq!(value["descriptor_version"], 7);
    assert!(value["features"]
        .get("durable_permission_settled_thread_deletion")
        .is_none());
    let before = rows(&directory.0);
    assert_eq!(delete(&server, "seventeen").await.status, 501);
    unchanged(&directory.0, &before);
    healthy(&server).await;
    support::retire(old, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_late_thread_delete_fault_rolls_back_all_nine_tables_faults_drains_then_fresh_reopen_deletes(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let store = Arc::new(Boundary::new(inner.clone()));
    let catalog = catalog(quick(), Arc::new(support::Constraint::default()));
    let host = deletion_host(
        store.clone(),
        catalog.clone(),
        Arc::new(DeletePolicy::default()),
    );
    let server = Server::start(&host).await;
    history(&server, &host, "late-delete", 1, 2).await;
    healthy(&server).await;
    let before = rows(&directory.0);
    let connection = db(&directory.0);
    // Fixture-only exact execute-phase failure, installed after full readiness.
    // The predicate requires every actual dependent removal before the abort.
    let ddl = "CREATE TRIGGER settled_deletion_late_guard BEFORE DELETE ON threads
        WHEN OLD.id = 'late-delete' AND OLD.active_execution_id IS NULL
        AND NOT EXISTS (SELECT 1 FROM executions WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM journal WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM approval_grants WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM approval_acceptances WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM permission_acceptances WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM permission_decisions WHERE thread_id = OLD.id)
        AND NOT EXISTS (SELECT 1 FROM permission_uses WHERE thread_id = OLD.id)
        BEGIN SELECT RAISE(ABORT, 'settled_deletion_after_dependents'); END";
    connection.execute_batch(ddl).unwrap();
    let actual:String=connection.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='settled_deletion_late_guard'",[],|r|r.get(0)).unwrap();
    assert_eq!(actual, ddl);
    // No readiness probe between schema fault injection and the actual DELETE.
    let response = delete(&server, "late-delete").await;
    code(&response, 503, "host_unready");
    assert_eq!(
        serde_json::from_str::<Value>(&response.body).unwrap(),
        serde_json::json!({"error":{"kind":"request_error","message":"host_unready","details":{"code":"host_unready"}}})
    );
    assert!(
        store.final_delete_failed.load(Ordering::Acquire),
        "actual writer reached guarded final DELETE"
    );
    assert_eq!(
        store.deletion_writes.load(Ordering::Acquire),
        1,
        "no automatic retry"
    );
    unchanged(&directory.0, &before);
    assert!(host.shutdown_handle().status().faulted);
    assert_eq!(
        support::request(&server, "GET", "/readyz", "").await.status,
        503
    );
    drained(&host).await;
    connection
        .execute_batch("DROP TRIGGER settled_deletion_late_guard")
        .unwrap();
    let remaining:i64=connection.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='settled_deletion_late_guard'",[],|r|r.get(0)).unwrap();
    assert_eq!(remaining, 0);
    connection.close().unwrap();
    support::retire(host, server, true).await;
    drop(store);
    drop(inner);
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    inner.readiness().unwrap();
    assert!(rows(&directory.0).0[1..] == before.0[1..]);
    let host = deletion_host(inner.clone(), catalog, Arc::new(DeletePolicy::default()));
    let server = Server::start(&host).await;
    healthy(&server).await;
    let expected = without(&rows(&directory.0), "late-delete");
    success(&server, "late-delete").await;
    assert!(rows(&directory.0) == expected);
    healthy(&server).await;
    support::retire(host, server, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_deletion_retains_claim_mutation_lease_store_and_assembly_through_real_commit_and_drain(
) {
    let directory = Directory::new();
    let inner = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let store = Arc::new(Boundary::new(inner.clone()));
    let callback = quick();
    let constraint = Arc::new(support::Constraint::default());
    let catalog = catalog(callback.clone(), constraint.clone());
    let policy = Arc::new(DeletePolicy::default());
    let host = deletion_host(store.clone(), catalog.clone(), policy.clone());
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    history(&server, &host, "abandoned-delete", 1, 2).await;
    let before = rows(&directory.0);
    let expected = without(&before, "abandoned-delete");
    let writer = Gate::new();
    let committed = Gate::new();
    let _cleanup = [Cleanup(writer.clone()), Cleanup(committed.clone())];
    *store.before_delete.lock().unwrap() = Some(writer.clone());
    *store.after_delete.lock().unwrap() = Some(committed.clone());
    let caller = support::open(
        server.address,
        "DELETE",
        "/threads/abandoned-delete",
        "",
        "owner",
    )
    .await;
    until(|| writer.facts().entered).await;
    drop(caller);
    // Admission and claim have transferred to the actual owned writer already.
    code(
        &delete(&server, "abandoned-delete").await,
        409,
        "run_in_flight",
    );
    unchanged(&directory.0, &before);
    assert_eq!(store.deletion_writes.load(Ordering::Acquire), 1);
    assert!(shutdown.status().active_owners > 0);
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    let weak_store = Arc::downgrade(&store);
    let weak_inner = Arc::downgrade(&inner);
    let weak_callback = Arc::downgrade(&callback);
    let weak_constraint = Arc::downgrade(&constraint);
    let weak_policy = Arc::downgrade(&policy);
    // The cut happens after this mutation lease was admitted and before its SQL.
    assert_eq!(shutdown.begin_shutdown().phase, ShutdownPhase::Draining);
    server.stop().await;
    drop(host);
    drop(store);
    drop(inner);
    drop(catalog);
    drop(callback);
    drop(constraint);
    drop(policy);
    assert!(weak_store.upgrade().is_some() && weak_inner.upgrade().is_some());
    assert!(weak_policy.upgrade().is_some());
    assert!(weak_callback.upgrade().is_some() && weak_constraint.upgrade().is_some());
    assert!(matches!(
        shutdown
            .wait_quiescent(Duration::from_millis(20))
            .await
            .unwrap(),
        ShutdownOutcome::Incomplete {
            active_owners: 1..,
            faulted: false
        }
    ));
    writer.release();
    until(|| committed.facts().entered).await;
    assert!(
        rows(&directory.0) == expected,
        "real delegated commit completed after admission cut"
    );
    assert!(weak_store.upgrade().is_some() && weak_inner.upgrade().is_some());
    assert!(shutdown.status().active_owners > 0);
    assert!(matches!(
        SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    committed.release();
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    cooperative::assert_positive(&writer);
    cooperative::assert_positive(&committed);
    assert!(weak_store.upgrade().is_none() && weak_inner.upgrade().is_none());
    assert!(weak_policy.upgrade().is_none());
    assert!(weak_callback.upgrade().is_none() && weak_constraint.upgrade().is_none());
    let reopened = SqliteStore::open(&directory.0).unwrap();
    reopened.readiness().unwrap();
    assert!(reopened
        .thread_view(&ThreadId::new("abandoned-delete").unwrap())
        .unwrap()
        .is_none());
    assert!(rows(&directory.0).0[1..] == expected.0[1..]);
    // This later absence is only post-commit observation; no abandoned response is synthesized.
}
