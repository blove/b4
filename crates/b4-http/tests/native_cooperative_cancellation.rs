#[path = "native_cooperative_support/fixture_tests.rs"]
mod fixture_tests;
#[path = "native_cooperative_support/mod.rs"]
mod support;
use b4_contract::*;
use std::sync::{atomic::Ordering, Arc};
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_durable_cancel_notifies_entered_native_callback() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Acknowledge);
    let _cleanup = Cleanup(callback.gate.clone());
    let app = application(definition(), vec![callback.clone()]);
    let host = host(store.clone(), app);
    let running = {
        let h = host.clone();
        tokio::spawn(async move { call(&h, "POST", "/threads/held/runs/wait", RUN).await })
    };
    until(|| callback.gate.facts().entered).await;
    let before = snapshot(store.as_ref(), "held");
    assert_eq!(before.status, ExecutionStatus::Running);
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    assert_cancel(&host, "held").await;
    let response = joined(running).await;
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "run_cancelled"
    );
    let durable = snapshot(store.as_ref(), "held");
    assert_eq!(durable.status, ExecutionStatus::CancelRequested);
    assert_eq!(durable.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(durable.attempt_history, before.attempt_history);
    let notification = callback.notified();
    let controlled = callback.controlled.load(Ordering::Acquire);
    callback.gate.release();
    until(|| snapshot(store.as_ref(), "held").status == ExecutionStatus::Cancelled).await;
    finish_host(&host, false).await;
    assert!(callback.returned.load(Ordering::Acquire));
    assert!(!callback.gate.facts().expired && !callback.gate.facts().cleanup);
    assert_eq!(
        controlled, 1,
        "accepted native callback must enter controlled method"
    );
    assert!(
        notification,
        "accepted durable cancellation must notify original callback"
    );
    let final_state = snapshot(store.as_ref(), "held");
    assert_eq!(final_state.tasks[0].status, TaskStatus::Cancelled);
    assert!(final_state.tasks[0].result.is_none() && final_state.tasks[0].failure.is_none());
    acknowledged(&callback);
}

use b4_http::HttpHost;
use b4_runtime::{approval::*, *};
use std::time::Duration;
use support::{policy::CancelPolicy, store::*};

fn running<S: Store + 'static>(
    host: &HttpHost<S>,
    id: &str,
) -> tokio::task::JoinHandle<axum::response::Response> {
    let host = host.clone();
    let path = format!("/threads/{id}/runs/wait");
    tokio::spawn(async move { call(&host, "POST", &path, RUN).await })
}
async fn cancelled_wait(run: tokio::task::JoinHandle<axum::response::Response>) {
    let response = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "run_cancelled"
    );
}
async fn owned<S: Store + 'static>(h: &HttpHost<S>, directory: &Directory, id: &str) {
    assert_eq!(
        call(h, "DELETE", &format!("/threads/{id}"), "")
            .await
            .status(),
        409
    );
    let same_thread = call(h, "POST", &format!("/threads/{id}/runs/wait"), RUN).await;
    assert_eq!(same_thread.status(), 503);
    assert_eq!(
        json_body(same_thread).await["error"]["details"]["code"],
        "capacity_exhausted"
    );
    assert_eq!(
        call(h, "POST", "/threads/capacity/runs/wait", RUN)
            .await
            .status(),
        503
    );
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_signal_waits_for_store_return_and_acknowledgement_completion() {
    let directory = Directory::new();
    let inner = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let before = Gate::new();
    let after = Gate::new();
    let completion = Gate::new();
    let _a = Cleanup(before.clone());
    let _b = Cleanup(after.clone());
    let _c = Cleanup(completion.clone());
    let mut boundary = BoundaryStore::new(inner.clone());
    boundary.before_cancel = Some(before.clone());
    boundary.after_cancel = Some(after.clone());
    boundary.before_ack = Some(completion.clone());
    let store = Arc::new(boundary);
    let callback = Callback::new(Return::Acknowledge);
    let _return = Cleanup(callback.gate.clone());
    let app = application(definition(), vec![callback.clone()]);
    let host = host(store.clone(), app);
    let run = running(&host, "barriers");
    until(|| callback.gate.facts().entered).await;
    let original = inner
        .thread_view(&ThreadId::new("barriers").unwrap())
        .unwrap()
        .unwrap();
    let snap = original.execution.as_ref().unwrap().snapshot.clone();
    let authority = inner.approval_view(&snap.execution_id).unwrap();
    assert!(authority.as_ref().unwrap().grants.is_empty());
    assert!(authority.as_ref().unwrap().acceptances.is_empty());
    let journal = inner.journal(&snap.execution_id).unwrap();
    let cancel = {
        let h = host.clone();
        tokio::spawn(async move { call(&h, "POST", "/threads/barriers/cancel", "").await })
    };
    until(|| before.facts().entered).await;
    let arguments = store.cancellation_arguments.lock().unwrap().clone();
    assert_eq!(arguments.len(), 1);
    assert_eq!(
        arguments[0].0,
        RunCancellationTarget::from_view(&original).unwrap()
    );
    assert!(arguments[0].1.now_ms >= snap.updated_at_ms);
    assert!(!callback.notified());
    assert_eq!(
        inner.thread_view(&original.thread.id).unwrap(),
        Some(original.clone())
    );
    assert_eq!(inner.approval_view(&snap.execution_id).unwrap(), authority);
    assert_eq!(inner.journal(&snap.execution_id).unwrap(), journal);
    before.release();
    until(|| after.facts().entered).await;
    let committed = snapshot(inner.as_ref(), "barriers");
    let mut expected_cancel = snap.clone();
    expected_cancel.revision += 1;
    expected_cancel.status = ExecutionStatus::CancelRequested;
    expected_cancel.updated_at_ms = arguments[0].1.now_ms;
    assert_eq!(committed, expected_cancel);
    assert_eq!(committed.status, ExecutionStatus::CancelRequested);
    assert_eq!(committed.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(committed.values, snap.values);
    assert_eq!(committed.attempt_history, snap.attempt_history);
    assert_eq!(committed.attempts_used, snap.attempts_used);
    assert_eq!(committed.updated_at_ms, arguments[0].1.now_ms);
    let mut expected_cancel_view = original.clone();
    expected_cancel_view.thread.resource_revision += 1;
    expected_cancel_view.execution.as_mut().unwrap().snapshot = expected_cancel.clone();
    let mut expected_cancel_authority = authority.as_ref().unwrap().clone();
    expected_cancel_authority.thread.resource_revision += 1;
    expected_cancel_authority.execution.snapshot = expected_cancel.clone();
    assert_eq!(
        inner.thread_view(&original.thread.id).unwrap(),
        Some(expected_cancel_view.clone())
    );
    assert_eq!(
        inner.approval_view(&snap.execution_id).unwrap(),
        Some(expected_cancel_authority.clone())
    );
    let mut expected_journal = journal.clone();
    expected_journal.push(JournalEntry::from(&expected_cancel));
    assert_eq!(inner.journal(&snap.execution_id).unwrap(), expected_journal);
    assert!(!callback.notified());
    assert!(!cancel.is_finished());
    owned(&host, &directory, "barriers").await;
    after.release();
    assert_eq!(joined(cancel).await.status(), 200);
    cancelled_wait(run).await;
    assert!(callback.notified());
    assert!(!callback.returned.load(Ordering::Acquire));
    owned(&host, &directory, "barriers").await;
    callback.gate.release();
    until(|| completion.facts().entered).await;
    assert!(callback.returned.load(Ordering::Acquire));
    let held = snapshot(inner.as_ref(), "barriers");
    assert_eq!(held, expected_cancel);
    assert_eq!(
        inner.thread_view(&original.thread.id).unwrap(),
        Some(expected_cancel_view)
    );
    assert_eq!(
        inner.approval_view(&snap.execution_id).unwrap(),
        Some(expected_cancel_authority)
    );
    assert_eq!(inner.journal(&snap.execution_id).unwrap(), expected_journal);
    assert_eq!(held.status, ExecutionStatus::CancelRequested);
    assert_eq!(held.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(held.values, snap.values);
    owned(&host, &directory, "barriers").await;
    let completion_arguments = store.acknowledgement_arguments.lock().unwrap().clone();
    assert_eq!(completion_arguments.len(), 1);
    let (completion_id, completion_fence, proposal) = &completion_arguments[0];
    assert_eq!(completion_id, &snap.execution_id);
    assert_eq!(*completion_fence, ExecutionFence::from(&committed));
    assert_eq!(proposal.expected_revision, committed.revision);
    assert!(proposal.dispatchable.is_empty());
    let mut expected_complete = expected_cancel.clone();
    expected_complete.revision += 1;
    expected_complete.status = ExecutionStatus::Cancelled;
    expected_complete.tasks[0].status = TaskStatus::Cancelled;
    assert!(proposal.snapshot.updated_at_ms >= committed.updated_at_ms);
    expected_complete.updated_at_ms = proposal.snapshot.updated_at_ms;
    assert_eq!(proposal.snapshot, expected_complete);
    completion.release();
    until(|| snapshot(inner.as_ref(), "barriers").status == ExecutionStatus::Cancelled).await;
    let final_state = snapshot(inner.as_ref(), "barriers");
    assert_eq!(final_state, expected_complete);
    expected_journal.push(JournalEntry::from(&expected_complete));
    assert_eq!(inner.journal(&snap.execution_id).unwrap(), expected_journal);
    let mut expected_complete_view = original.clone();
    expected_complete_view.thread.resource_revision += 2;
    expected_complete_view.execution.as_mut().unwrap().snapshot = expected_complete.clone();
    let mut expected_complete_authority = authority.as_ref().unwrap().clone();
    expected_complete_authority.thread.resource_revision += 2;
    expected_complete_authority.execution.snapshot = expected_complete.clone();
    assert_eq!(
        inner.thread_view(&original.thread.id).unwrap(),
        Some(expected_complete_view)
    );
    assert_eq!(
        inner.approval_view(&snap.execution_id).unwrap(),
        Some(expected_complete_authority)
    );
    assert_eq!(final_state.tasks[0].status, TaskStatus::Cancelled);
    assert!(final_state.tasks[0].result.is_none() && final_state.tasks[0].failure.is_none());
    assert_eq!(final_state.attempt_history, snap.attempt_history);
    assert_eq!(final_state.attempts_used, 1);
    assert_eq!(final_state.values, snap.values);
    assert_eq!(store.ack_calls.load(Ordering::Acquire), 1);
    assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
    finish_host(&host, false).await;
    for gate in [&before, &after, &completion, &callback.gate] {
        assert!(gate.facts().explicit && !gate.facts().expired && !gate.facts().cleanup);
    }
    acknowledged(&callback);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_and_refused_durable_cancellation_never_publish_native_signal() {
    for (mode, status, calls, faulted) in [
        (CancelBehavior::Unsupported, 501, 1, false),
        (CancelBehavior::OwnerConflict, 409, 1, false),
        (CancelBehavior::Conflict, 409, 8, false),
        (CancelBehavior::Fail, 503, 1, true),
        (CancelBehavior::Panic, 503, 1, true),
    ] {
        let inner = Arc::new(MemoryStore::new());
        let mut b = BoundaryStore::new(inner.clone());
        b.cancel_behavior = mode;
        let store = Arc::new(b);
        let callback = Callback::new(Return::Succeed);
        let _cleanup = Cleanup(callback.gate.clone());
        let host = host(
            store.clone(),
            application(definition(), vec![callback.clone()]),
        );
        let run = running(&host, "refusal");
        until(|| callback.gate.facts().entered).await;
        let before = inner
            .thread_view(&ThreadId::new("refusal").unwrap())
            .unwrap()
            .unwrap();
        let e = before.execution.as_ref().unwrap();
        let a = inner.approval_view(&e.snapshot.execution_id).unwrap();
        let j = inner.journal(&e.snapshot.execution_id).unwrap();
        let response = call(&host, "POST", "/threads/refusal/cancel", "").await;
        assert_eq!(response.status(), status);
        assert!(!callback.notified());
        assert_eq!(
            inner.thread_view(&before.thread.id).unwrap(),
            Some(before.clone())
        );
        assert_eq!(inner.approval_view(&e.snapshot.execution_id).unwrap(), a);
        assert_eq!(inner.journal(&e.snapshot.execution_id).unwrap(), j);
        assert_eq!(store.cancel_calls.load(Ordering::Acquire), calls);
        callback.gate.release();
        let response = joined(run).await;
        assert_eq!(response.status(), if faulted { 503 } else { 200 });
        finish_host(&host, faulted).await;
        assert!(!callback.notified());
        assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
        assert_positive(&callback.gate);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_sqlite_busy_write_failure_keeps_original_observer_clear() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Succeed);
    let _cleanup = Cleanup(callback.gate.clone());
    let host = host(
        store.clone(),
        application(definition(), vec![callback.clone()]),
    );
    let run = running(&host, "busy");
    until(|| callback.gate.facts().entered).await;
    let before = store
        .thread_view(&ThreadId::new("busy").unwrap())
        .unwrap()
        .unwrap();
    let e = before.execution.as_ref().unwrap();
    let authority = store.approval_view(&e.snapshot.execution_id).unwrap();
    let journal = store.journal(&e.snapshot.execution_id).unwrap();
    let lock = rusqlite::Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let response = call(&host, "POST", "/threads/busy/cancel", "").await;
    assert_eq!(response.status(), 503);
    assert!(!callback.notified());
    assert_eq!(
        store.thread_view(&before.thread.id).unwrap(),
        Some(before.clone())
    );
    assert_eq!(
        store.approval_view(&e.snapshot.execution_id).unwrap(),
        authority
    );
    assert_eq!(store.journal(&e.snapshot.execution_id).unwrap(), journal);
    lock.execute_batch("ROLLBACK").unwrap();
    drop(lock);
    callback.gate.release();
    assert_eq!(joined(run).await.status(), 503);
    finish_host(&host, true).await;
    assert!(!callback.notified());
    assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
    assert_positive(&callback.gate);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_and_renewed_policy_denial_leave_signal_and_authority_unchanged() {
    for denied in [0, 1] {
        let store = Arc::new(MemoryStore::new());
        let callback = Callback::new(Return::Succeed);
        let _cleanup = Cleanup(callback.gate.clone());
        let app = application(definition(), vec![callback.clone()]);
        let mut p = CancelPolicy::new();
        p.deny_at = Some(denied);
        let policy = Arc::new(p);
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            policy.clone(),
            config(app, false, 1),
        )
        .unwrap();
        let run = running(&host, "denied");
        until(|| callback.gate.facts().entered).await;
        let before = store
            .thread_view(&ThreadId::new("denied").unwrap())
            .unwrap()
            .unwrap();
        let id = &before.execution.as_ref().unwrap().snapshot.execution_id;
        let authority = store.approval_view(id).unwrap();
        let journal = store.journal(id).unwrap();
        assert_eq!(
            call(&host, "POST", "/threads/denied/cancel", "")
                .await
                .status(),
            404
        );
        assert_eq!(policy.calls.load(Ordering::Acquire), denied + 1);
        assert!(!callback.notified());
        assert_eq!(
            store.thread_view(&before.thread.id).unwrap(),
            Some(before.clone())
        );
        assert_eq!(store.approval_view(id).unwrap(), authority);
        assert_eq!(store.journal(id).unwrap(), journal);
        callback.gate.release();
        assert_eq!(joined(run).await.status(), 200);
        finish_host(&host, false).await;
        assert_positive(&callback.gate);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn false_and_foreign_native_acknowledgements_fault_without_task_cancellation() {
    for mode in [Return::FalseAcknowledgement, Return::ForeignSignal] {
        let store = Arc::new(MemoryStore::new());
        let callback = Callback::new(mode);
        let _cleanup = Cleanup(callback.gate.clone());
        let host = host(
            store.clone(),
            application(definition(), vec![callback.clone()]),
        );
        let run = running(&host, "false-ack");
        until(|| callback.gate.facts().entered).await;
        let before = store
            .thread_view(&ThreadId::new("false-ack").unwrap())
            .unwrap()
            .unwrap();
        let id = &before.execution.as_ref().unwrap().snapshot.execution_id;
        let a = store.approval_view(id).unwrap();
        let j = store.journal(id).unwrap();
        callback.gate.release();
        let response = joined(run).await;
        assert_eq!(response.status(), 503);
        assert_eq!(
            json_body(response).await["error"]["details"]["code"],
            "host_unready"
        );
        finish_host(&host, true).await;
        assert!(!callback.notified());
        assert_eq!(
            store.thread_view(&before.thread.id).unwrap(),
            Some(before.clone())
        );
        assert_eq!(store.approval_view(id).unwrap(), a);
        assert_eq!(store.journal(id).unwrap(), j);
        assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
        assert!(callback.returned.load(Ordering::Acquire));
        assert_positive(&callback.gate);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn false_ack_during_pending_never_publishes_after_later_store_success() {
    let inner = Arc::new(MemoryStore::new());
    let pending = Gate::new();
    let _release = Cleanup(pending.clone());
    let mut b = BoundaryStore::new(inner.clone());
    b.before_cancel = Some(pending.clone());
    let store = Arc::new(b);
    let callback = Callback::new(Return::FalseAcknowledgement);
    let _cleanup = Cleanup(callback.gate.clone());
    let host = host(
        store.clone(),
        application(definition(), vec![callback.clone()]),
    );
    let run = running(&host, "pending-fault");
    until(|| callback.gate.facts().entered).await;
    let cancel = {
        let h = host.clone();
        tokio::spawn(async move { call(&h, "POST", "/threads/pending-fault/cancel", "").await })
    };
    until(|| pending.facts().entered).await;
    assert!(!callback.notified());
    callback.gate.release();
    until(|| host.shutdown_handle().status().faulted).await;
    assert!(!callback.notified());
    pending.release();
    assert_eq!(joined(cancel).await.status(), 503);
    assert_eq!(joined(run).await.status(), 503);
    finish_host(&host, true).await;
    let state = snapshot(inner.as_ref(), "pending-fault");
    assert_eq!(state.status, ExecutionStatus::CancelRequested);
    assert_eq!(state.tasks[0].status, TaskStatus::InFlight);
    assert!(state.tasks[0].result.is_none() && state.tasks[0].failure.is_none());
    assert!(!callback.notified());
    assert_eq!(state.attempts_used, 1);
    assert_positive(&pending);
    assert_positive(&callback.gate);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_before_native_entry_invokes_neither_native_method() {
    let inner = Arc::new(MemoryStore::new());
    let dispatch = Gate::new();
    let _release = Cleanup(dispatch.clone());
    let mut b = BoundaryStore::new(inner.clone());
    b.after_dispatch = Some(dispatch.clone());
    let store = Arc::new(b);
    let callback = Callback::new(Return::Acknowledge);
    let _cleanup = Cleanup(callback.gate.clone());
    let host = host(store, application(definition(), vec![callback.clone()]));
    let run = running(&host, "pre-entry");
    until(|| dispatch.facts().entered).await;
    assert_eq!(
        snapshot(inner.as_ref(), "pre-entry").tasks[0].status,
        TaskStatus::InFlight
    );
    assert_cancel(&host, "pre-entry").await;
    cancelled_wait(run).await;
    dispatch.release();
    finish_host(&host, false).await;
    let state = snapshot(inner.as_ref(), "pre-entry");
    assert_eq!(state.status, ExecutionStatus::Cancelled);
    assert_eq!(state.tasks[0].status, TaskStatus::Cancelled);
    assert!(state.tasks[0].result.is_none());
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
    assert_eq!(callback.controlled.load(Ordering::Acquire), 0);
    assert!(callback.observer().is_none());
    assert!(!callback.gate.facts().entered);
    assert_positive(&dispatch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_cancellation_cannot_notify_original_or_successor_callback() {
    for held_policy in [0, 1] {
        let store = Arc::new(MemoryStore::new());
        let first = Callback::new(Return::Succeed);
        let second = Callback::new(Return::Succeed);
        let _a = Cleanup(first.gate.clone());
        let _b = Cleanup(second.gate.clone());
        let series = Series::new(vec![first.clone(), second.clone()]);
        let app = application(definition(), vec![series]);
        let policy_gate = Gate::new();
        let _p = Cleanup(policy_gate.clone());
        let mut p = CancelPolicy::new();
        p.hold_at = Some((held_policy, policy_gate.clone()));
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(p),
            config(app, false, 1),
        )
        .unwrap();
        let run = running(&host, "successor");
        until(|| first.gate.facts().entered).await;
        let original = snapshot(store.as_ref(), "successor");
        let stale = {
            let h = host.clone();
            tokio::spawn(async move { call(&h, "POST", "/threads/successor/cancel", "").await })
        };
        until(|| policy_gate.facts().entered).await;
        first.gate.release();
        assert_eq!(joined(run).await.status(), 200);
        let successor = {
            let h = host.clone();
            tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        let response = call(&h, "POST", "/threads/successor/runs/wait", RUN).await;
                        if response.status() == 200 {
                            break response;
                        }
                        assert!(matches!(response.status().as_u16(), 409 | 503));
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .unwrap()
            })
        };
        until(|| second.gate.facts().entered).await;
        let fresh = snapshot(store.as_ref(), "successor");
        assert_ne!(fresh.execution_id, original.execution_id);
        assert_ne!(fresh.turn_id, original.turn_id);
        policy_gate.release();
        let response = joined(stale).await;
        assert_eq!(response.status(), 409);
        assert_eq!(
            json_body(response).await["error"]["details"]["code"],
            "no_run_in_flight"
        );
        assert!(!first.notified() && !second.notified());
        assert_eq!(snapshot(store.as_ref(), "successor"), fresh);
        second.gate.release();
        assert_eq!(joined(successor).await.status(), 200);
        finish_host(&host, false).await;
        assert_eq!(first.controlled.load(Ordering::Acquire), 1);
        assert_eq!(second.controlled.load(Ordering::Acquire), 1);
        for gate in [&first.gate, &second.gate, &policy_gate] {
            assert_positive(gate);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_observer_remains_sticky_and_successor_has_fresh_signal() {
    let store = Arc::new(MemoryStore::new());
    let first = Callback::new(Return::Acknowledge);
    let second = Callback::new(Return::Acknowledge);
    let _a = Cleanup(first.gate.clone());
    let _b = Cleanup(second.gate.clone());
    let app = application(
        definition(),
        vec![Series::new(vec![first.clone(), second.clone()])],
    );
    let host = host(store.clone(), app);
    let run = running(&host, "fresh");
    until(|| first.gate.facts().entered).await;
    assert_cancel(&host, "fresh").await;
    cancelled_wait(run).await;
    let original = first.observer().expect("controlled observer");
    assert!(original.cancellation_requested());
    first.gate.release();
    until(|| host.shutdown_handle().status().active_owners == 0).await;
    let run = running(&host, "fresh");
    until(|| second.gate.facts().entered).await;
    assert!(original.cancellation_requested());
    assert!(!second.notified());
    assert_cancel(&host, "fresh").await;
    cancelled_wait(run).await;
    assert!(second.notified());
    assert!(original.cancellation_requested());
    second.gate.release();
    finish_host(&host, false).await;
    assert_eq!(
        snapshot(store.as_ref(), "fresh").tasks[0].status,
        TaskStatus::Cancelled
    );
    acknowledged(&first);
    acknowledged(&second);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_completion_retries_retain_outcome_without_callback_replay() {
    for conflicts in [2, 8] {
        let inner = Arc::new(MemoryStore::new());
        let b = BoundaryStore::new(inner.clone());
        b.completion_conflicts.store(conflicts, Ordering::Release);
        let store = Arc::new(b);
        let callback = Callback::new(Return::Acknowledge);
        let _cleanup = Cleanup(callback.gate.clone());
        let host = host(
            store.clone(),
            application(definition(), vec![callback.clone()]),
        );
        let run = running(&host, "retry");
        until(|| callback.gate.facts().entered).await;
        let before = snapshot(inner.as_ref(), "retry");
        assert_cancel(&host, "retry").await;
        cancelled_wait(run).await;
        assert!(callback.notified());
        callback.gate.release();
        finish_host(&host, conflicts == 8).await;
        let state = snapshot(inner.as_ref(), "retry");
        assert_eq!(state.attempt_history, before.attempt_history);
        assert_eq!(state.attempts_used, 1);
        assert_eq!(state.values, before.values);
        assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
        assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
        assert_eq!(
            store.ack_calls.load(Ordering::Acquire),
            if conflicts == 8 { 8 } else { 3 }
        );
        assert!(state.tasks[0].result.is_none() && state.tasks[0].failure.is_none());
        assert_eq!(
            state.status,
            if conflicts == 8 {
                ExecutionStatus::CancelRequested
            } else {
                ExecutionStatus::Cancelled
            }
        );
        assert_eq!(
            state.tasks[0].status,
            if conflicts == 8 {
                TaskStatus::InFlight
            } else {
                TaskStatus::Cancelled
            }
        );
        acknowledged(&callback);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_genuine_outcomes_keep_durable_evidence_without_reducer_or_later_batch() {
    for mode in [
        Return::Succeed,
        Return::Fail,
        Return::Uncertain,
        Return::Panic,
    ] {
        let store = Arc::new(MemoryStore::new());
        let callback = Callback::new(mode);
        let later = Callback::new(Return::Succeed);
        let _a = Cleanup(callback.gate.clone());
        let _b = Cleanup(later.gate.clone());
        let mut d = definition();
        d.batches.push(vec![TaskSpec {
            id: TaskId::new("later").unwrap(),
            effect_id: EffectId::new("later-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]);
        let host = host(
            store.clone(),
            application(d, vec![callback.clone(), later.clone()]),
        );
        let run = running(&host, "late");
        until(|| callback.gate.facts().entered).await;
        let before = snapshot(store.as_ref(), "late");
        assert_cancel(&host, "late").await;
        cancelled_wait(run).await;
        assert!(callback.notified());
        callback.gate.release();
        let faulted = matches!(mode, Return::Uncertain | Return::Panic);
        finish_host(&host, faulted).await;
        let state = snapshot(store.as_ref(), "late");
        assert_eq!(state.values, before.values);
        assert_eq!(state.batch_index, 0);
        assert_eq!(state.attempts_used, 1);
        assert_eq!(state.attempt_history, before.attempt_history);
        assert_eq!(later.controlled.load(Ordering::Acquire), 0);
        assert_eq!(later.legacy.load(Ordering::Acquire), 0);
        assert_eq!(
            state.tasks.len(),
            1,
            "later batch has not been materialized"
        );
        assert_eq!(state.definition.batches[1][0].id.as_str(), "later");
        match mode {
            Return::Succeed => {
                assert_eq!(state.tasks[0].status, TaskStatus::Succeeded);
                let NativeOutcome::Succeeded(actual) = genuine_success() else {
                    unreachable!()
                };
                assert_eq!(state.tasks[0].result, Some(actual));
                assert_eq!(state.status, ExecutionStatus::Cancelled);
            }
            Return::Fail => {
                assert_eq!(state.tasks[0].status, TaskStatus::Failed);
                assert_eq!(
                    state.tasks[0].failure.as_deref(),
                    Some("known-native-failure")
                );
                assert_eq!(state.status, ExecutionStatus::Failed);
                assert_eq!(state.failure.as_deref(), Some("known-native-failure"));
            }
            _ => {
                assert_eq!(state.tasks[0].status, TaskStatus::InFlight);
                assert_eq!(state.status, ExecutionStatus::CancelRequested);
                assert!(state.tasks[0].result.is_none() && state.tasks[0].failure.is_none());
            }
        }
        assert_eq!(
            callback.returned.load(Ordering::Acquire),
            !matches!(mode, Return::Panic)
        );
        assert!(callback.notified());
        assert_positive(&callback.gate);
        assert_eq!(
            callback.panicked.load(Ordering::Acquire),
            matches!(mode, Return::Panic)
        );
        assert!(!later.gate.facts().entered);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prior_sqlite_owner_unfinished_native_attempt_is_not_a_local_signal_target() {
    let directory = Directory::new();
    let store = b4_sqlite::SqliteStore::open(&directory.0).unwrap();
    let id = ThreadId::new("prior").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Default::default(),
            origin: Some(ThreadOrigin {
                application: "cooperative-test".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    let runtime = Runtime::new(&store);
    let accepted = runtime
        .admit(
            &id,
            b4_kernel::new_execution(
                ExecutionId::new("prior-execution").unwrap(),
                TurnId::new("prior-turn").unwrap(),
                store.owner_generation(),
                definition(),
                Default::default(),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap();
    let started = runtime
        .apply(
            &accepted.snapshot.execution_id,
            (&accepted.snapshot).into(),
            KernelCommand::Start,
            TransitionFacts { now_ms: 2 },
        )
        .unwrap();
    let dispatched = runtime
        .apply(
            &started.snapshot.execution_id,
            (&started.snapshot).into(),
            KernelCommand::Dispatch {
                task_id: TaskId::new("task").unwrap(),
                attempt_id: AttemptId::new("prior-attempt").unwrap(),
            },
            TransitionFacts { now_ms: 3 },
        )
        .unwrap();
    assert_eq!(dispatched.snapshot.status, ExecutionStatus::Running);
    assert_eq!(dispatched.snapshot.tasks[0].status, TaskStatus::InFlight);
    let before = store.thread_view(&id).unwrap();
    let authority = store
        .approval_view(&dispatched.snapshot.execution_id)
        .unwrap();
    let journal = store.journal(&dispatched.snapshot.execution_id).unwrap();
    drop(store);
    let reopened = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    assert_eq!(reopened.owner_generation(), 2);
    let callback = Callback::new(Return::Acknowledge);
    let host = host(
        reopened.clone(),
        application(definition(), vec![callback.clone()]),
    );
    let response = call(&host, "POST", "/threads/prior/cancel", "").await;
    assert_eq!(response.status(), 409);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "no_run_in_flight"
    );
    assert_eq!(reopened.thread_view(&id).unwrap(), before);
    assert_eq!(
        reopened
            .approval_view(&dispatched.snapshot.execution_id)
            .unwrap(),
        authority
    );
    assert_eq!(
        reopened.journal(&dispatched.snapshot.execution_id).unwrap(),
        journal
    );
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
    assert_eq!(callback.controlled.load(Ordering::Acquire), 0);
    assert!(!callback.gate.facts().entered);
    assert!(callback.observer().is_none());
    assert_eq!(call(&host, "GET", "/readyz", "").await.status(), 200);
    finish_host(&host, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_acceptance_sibling_fault_cannot_revoke_positive_acknowledgement() {
    let store = Arc::new(MemoryStore::new());
    let uncertain = Callback::new(Return::Uncertain);
    let acknowledged = Callback::new(Return::Acknowledge);
    let _a = Cleanup(uncertain.gate.clone());
    let _b = Cleanup(acknowledged.gate.clone());
    let mut d = definition();
    d.batches[0].push(TaskSpec {
        id: TaskId::new("sibling").unwrap(),
        effect_id: EffectId::new("sibling-effect").unwrap(),
        recovery: RecoveryPolicy::Pure,
        requires_approval: false,
    });
    let host = host(
        store.clone(),
        application(d, vec![uncertain.clone(), acknowledged.clone()]),
    );
    let run = running(&host, "sticky-fault");
    until(|| uncertain.gate.facts().entered && acknowledged.gate.facts().entered).await;
    assert_cancel(&host, "sticky-fault").await;
    cancelled_wait(run).await;
    assert!(uncertain.notified() && acknowledged.notified());
    uncertain.gate.release();
    until(|| host.shutdown_handle().status().faulted).await;
    assert!(acknowledged.notified());
    assert!(!acknowledged.returned.load(Ordering::Acquire));
    acknowledged.gate.release();
    finish_host(&host, true).await;
    let state = snapshot(store.as_ref(), "sticky-fault");
    assert_eq!(state.status, ExecutionStatus::CancelRequested);
    assert_eq!(state.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(state.tasks[1].status, TaskStatus::Cancelled);
    assert!(state
        .tasks
        .iter()
        .all(|t| t.result.is_none() && t.failure.is_none()));
    assert_eq!(state.attempts_used, 2);
    assert!(acknowledged.notified());
    assert!(acknowledged.returned.load(Ordering::Acquire));
    assert_eq!(acknowledged.controlled.load(Ordering::Acquire), 1);
    assert_positive(&uncertain.gate);
    support::acknowledged(&acknowledged);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_approval_park_and_no_local_target_are_not_signal_domains() {
    let store = Arc::new(MemoryStore::new());
    let callback = Callback::new(Return::Acknowledge);
    let _cleanup = Cleanup(callback.gate.clone());
    let mut d = definition();
    d.batches[0][0].requires_approval = true;
    let host = host(store.clone(), application(d, vec![callback.clone()]));
    assert_eq!(
        call(&host, "POST", "/threads/park/runs/wait", RUN)
            .await
            .status(),
        200
    );
    let view = store
        .thread_view(&ThreadId::new("park").unwrap())
        .unwrap()
        .unwrap();
    let id = &view.execution.as_ref().unwrap().snapshot.execution_id;
    let authority = store.approval_view(id).unwrap();
    let journal = store.journal(id).unwrap();
    assert_eq!(
        view.execution.as_ref().unwrap().snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(
        call(&host, "POST", "/threads/park/cancel", "")
            .await
            .status(),
        409
    );
    assert_eq!(
        store.thread_view(&view.thread.id).unwrap(),
        Some(view.clone())
    );
    assert_eq!(store.approval_view(id).unwrap(), authority);
    assert_eq!(store.journal(id).unwrap(), journal);
    assert!(callback.observer().is_none());
    assert_eq!(callback.controlled.load(Ordering::Acquire), 0);
    assert_eq!(callback.legacy.load(Ordering::Acquire), 0);
    assert!(!callback.gate.facts().entered);
    let empty = ThreadId::new("empty").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: empty.clone(),
            created_at_ms: 0,
            metadata: Default::default(),
            origin: Some(ThreadOrigin {
                application: "cooperative-test".into(),
                tenant: "tenant".into(),
                principal: "owner".into(),
            }),
        })
        .unwrap();
    let before = store.thread_view(&empty).unwrap();
    assert_eq!(
        call(&host, "POST", "/threads/empty/cancel", "")
            .await
            .status(),
        409
    );
    assert_eq!(store.thread_view(&empty).unwrap(), before);
    finish_host(&host, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_thread_claim_and_capacity_reuse_follow_actual_acknowledgement_commit() {
    let inner = Arc::new(MemoryStore::new());
    let completion = Gate::new();
    let _completion = Cleanup(completion.clone());
    let mut boundary = BoundaryStore::new(inner.clone());
    boundary.before_ack = Some(completion.clone());
    let store = Arc::new(boundary);
    let first = Callback::new(Return::Acknowledge);
    let spare = Callback::new(Return::Succeed);
    let successor = Callback::new(Return::Succeed);
    let _a = Cleanup(first.gate.clone());
    let _b = Cleanup(spare.gate.clone());
    let _c = Cleanup(successor.gate.clone());
    let app = application(
        definition(),
        vec![Series::new(vec![
            first.clone(),
            spare.clone(),
            successor.clone(),
        ])],
    );
    let host = HttpHost::new_with_application(
        store,
        Arc::new(Auth),
        Arc::new(Owner),
        config(app, false, 2),
    )
    .unwrap();
    let run = running(&host, "claim");
    until(|| first.gate.facts().entered).await;
    assert_cancel(&host, "claim").await;
    cancelled_wait(run).await;
    let response = call(&host, "POST", "/threads/claim/runs/wait", RUN).await;
    assert_eq!(response.status(), 409);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "thread_busy"
    );
    let spare_run = running(&host, "spare");
    until(|| spare.gate.facts().entered).await;
    let response = call(&host, "POST", "/threads/capacity/runs/wait", RUN).await;
    assert_eq!(response.status(), 503);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "capacity_exhausted"
    );
    first.gate.release();
    until(|| completion.facts().entered).await;
    assert!(first.returned.load(Ordering::Acquire));
    spare.gate.release();
    assert_eq!(joined(spare_run).await.status(), 200);
    until(|| host.shutdown_handle().status().active_owners == 1).await;
    let response = call(&host, "POST", "/threads/claim/runs/wait", RUN).await;
    assert_eq!(response.status(), 409);
    assert_eq!(
        json_body(response).await["error"]["details"]["code"],
        "thread_busy"
    );
    assert_eq!(
        snapshot(inner.as_ref(), "claim").tasks[0].status,
        TaskStatus::InFlight
    );
    completion.release();
    until(|| host.shutdown_handle().status().active_owners == 0).await;
    let prior = snapshot(inner.as_ref(), "claim");
    assert_eq!(prior.status, ExecutionStatus::Cancelled);
    assert_eq!(prior.tasks[0].status, TaskStatus::Cancelled);
    let run = running(&host, "claim");
    until(|| successor.gate.facts().entered).await;
    let fresh = snapshot(inner.as_ref(), "claim");
    assert_ne!(fresh.execution_id, prior.execution_id);
    assert!(!successor.notified());
    successor.gate.release();
    assert_eq!(joined(run).await.status(), 200);
    finish_host(&host, false).await;
    for callback in [&first, &spare, &successor] {
        assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
        assert!(callback.returned.load(Ordering::Acquire));
        assert!(
            callback.gate.facts().explicit
                && !callback.gate.facts().expired
                && !callback.gate.facts().cleanup
        );
    }
    acknowledged(&first);
    assert_positive(&completion);
}
