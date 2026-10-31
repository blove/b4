#[path = "native_cooperative_support/mod.rs"]
mod support;
use b4_contract::*;
use b4_http::{ShutdownHandle, ShutdownOutcome};
use b4_runtime::approval::ApprovalStore;
use b4_runtime::*;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use support::store::BoundaryStore;
use support::{tcp::*, *};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_accepted_cancel_notifies_without_releasing_callback_ownership() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let callback = Callback::new(Return::Acknowledge);
    let _cleanup = Cleanup(callback.gate.clone());
    let app = application(definition(), vec![callback.clone()]);
    let host = host(store.clone(), app);
    let server = Server::start(&host).await;
    let primary = open(server.address, "POST", "/threads/tcp/runs/wait", RUN).await;
    until(|| callback.gate.facts().entered).await;
    let before = snapshot(store.as_ref(), "tcp");
    assert_eq!(before.status, ExecutionStatus::Running);
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        exchange(server.address, "POST", "/threads/tcp/cancel", "")
            .await
            .status,
        200
    );
    let response = finish(primary, Vec::new()).await;
    assert_eq!(response.status, 409);
    let durable = snapshot(store.as_ref(), "tcp");
    assert_eq!(durable.status, ExecutionStatus::CancelRequested);
    assert_eq!(durable.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(durable.attempt_history, before.attempt_history);
    assert_eq!(
        exchange(server.address, "DELETE", "/threads/tcp", "")
            .await
            .status,
        409
    );
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    let notified = callback.notified();
    let controlled = callback.controlled.load(Ordering::Acquire);
    callback.gate.release();
    until(|| snapshot(store.as_ref(), "tcp").status == ExecutionStatus::Cancelled).await;
    server.stop().await;
    finish_host(&host, false).await;
    assert!(callback.returned.load(Ordering::Acquire));
    assert!(!callback.gate.facts().expired && !callback.gate.facts().cleanup);
    assert_eq!(
        controlled, 1,
        "TCP cancellation must use controlled native entry"
    );
    assert!(
        notified,
        "TCP durable accepted cancellation must notify callback"
    );
    acknowledged(&callback);
}

fn frames(response: &Response) -> Vec<(String, Value)> {
    assert_eq!(response.status, 200);
    response
        .body
        .split("\n\n")
        .filter_map(|frame| {
            let name = frame
                .lines()
                .find_map(|line| line.strip_prefix("event: "))?;
            let data = frame
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap();
            Some((name.into(), serde_json::from_str(data).unwrap()))
        })
        .collect()
}
fn cancelled_terminal(response: &Response) {
    let frames = frames(response);
    let done: Vec<_> = frames.iter().filter(|(name, _)| name == "done").collect();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].1, json!({"output":{"cancelled":true}}));
    assert!(frames
        .iter()
        .all(|(name, _)| name != "error" && name != "chunk"));
}
async fn incomplete(shutdown: &ShutdownHandle, faulted: bool) {
    match shutdown.wait_quiescent(Duration::ZERO).await.unwrap() {
        ShutdownOutcome::Incomplete {
            active_owners,
            faulted: actual,
        } => {
            assert!(active_owners > 0);
            assert_eq!(actual, faulted);
        }
        outcome => panic!("expected positively retained owner, got {outcome:?}"),
    }
}
async fn reopen(directory: &Directory, previous: u64) -> b4_sqlite::SqliteStore {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match b4_sqlite::SqliteStore::open(&directory.0) {
                Ok(store) => {
                    assert_eq!(store.owner_generation(), previous + 1);
                    store.readiness().unwrap();
                    break store;
                }
                Err(StoreError::AlreadyOwned) => tokio::time::sleep(Duration::from_millis(2)).await,
                Err(error) => panic!("unexpected owned fixture reopen error {error:?}"),
            }
        }
    })
    .await
    .unwrap()
}
fn positive(gate: &Gate) {
    let facts = gate.facts();
    assert!(facts.explicit && !facts.expired && !facts.cleanup);
}
async fn excluded(server: &Server, directory: &Directory, id: &str) {
    assert_eq!(
        exchange(server.address, "DELETE", &format!("/threads/{id}"), "")
            .await
            .status,
        409
    );
    let same_thread = exchange(
        server.address,
        "POST",
        &format!("/threads/{id}/runs/wait"),
        RUN,
    )
    .await;
    assert_eq!(same_thread.status, 503);
    assert_eq!(
        serde_json::from_str::<Value>(&same_thread.body).unwrap()["error"]["details"]["code"],
        "capacity_exhausted"
    );
    assert_eq!(
        exchange(server.address, "POST", "/threads/capacity/runs/wait", RUN)
            .await
            .status,
        503
    );
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_callback_and_completion_barriers_retain_writer_until_positive_retirement() {
    let directory = Directory::new();
    let inner = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let previous = inner.owner_generation();
    let completion = Gate::new();
    let _completion = Cleanup(completion.clone());
    let mut boundary = BoundaryStore::new(inner.clone());
    boundary.before_ack = Some(completion.clone());
    let store = Arc::new(boundary);
    let callback = Callback::new(Return::Acknowledge);
    let callback_gate = callback.gate.clone();
    let _return = Cleanup(callback_gate.clone());
    let app = application(definition(), vec![callback.clone()]);
    let weak_store = Arc::downgrade(&store);
    let weak_inner = Arc::downgrade(&inner);
    let weak_app = Arc::downgrade(&app);
    let weak_callback = Arc::downgrade(&callback);
    let host = host(store.clone(), app.clone());
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    let primary = open(server.address, "POST", "/threads/physical/runs/stream", RUN).await;
    until(|| callback_gate.facts().entered).await;
    let before = snapshot(inner.as_ref(), "physical");
    let authority = inner.approval_view(&before.execution_id).unwrap().unwrap();
    let mut attached = open(server.address, "GET", "/threads/physical/runs/stream", "").await;
    let prefix_bytes = prefix(&mut attached, b"event: state").await;
    assert_eq!(
        exchange(server.address, "POST", "/threads/physical/cancel", "")
            .await
            .status,
        200
    );
    let original = callback.observer().unwrap();
    let retained = original.clone();
    assert!(original.cancellation_requested());
    cancelled_terminal(&finish(primary, Vec::new()).await);
    let attachment = finish(attached, prefix_bytes).await;
    cancelled_terminal(&attachment);
    assert_eq!(frames(&attachment)[0].0, "state");
    let held = snapshot(inner.as_ref(), "physical");
    assert_eq!(held.status, ExecutionStatus::CancelRequested);
    assert_eq!(held.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(held.attempt_history, before.attempt_history);
    assert_eq!(held.values, before.values);
    excluded(&server, &directory, "physical").await;
    callback_gate.release();
    until(|| completion.facts().entered).await;
    assert!(callback.returned.load(Ordering::Acquire));
    acknowledged(&callback);
    assert_eq!(snapshot(inner.as_ref(), "physical"), held);
    let held_authority = inner.approval_view(&before.execution_id).unwrap().unwrap();
    assert_eq!(held_authority.grants, authority.grants);
    assert_eq!(held_authority.acceptances, authority.acceptances);
    excluded(&server, &directory, "physical").await;
    server.stop().await;
    incomplete(&shutdown, false).await;
    drop(host);
    drop(callback);
    drop(app);
    drop(store);
    drop(inner);
    assert!(
        weak_store.upgrade().is_some()
            && weak_inner.upgrade().is_some()
            && weak_app.upgrade().is_some()
    );
    assert!(weak_callback
        .upgrade()
        .unwrap()
        .returned
        .load(Ordering::Acquire));
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    assert!(retained.cancellation_requested());
    completion.release();
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(10))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert!(
        weak_store.upgrade().is_none()
            && weak_inner.upgrade().is_none()
            && weak_app.upgrade().is_none()
            && weak_callback.upgrade().is_none()
    );
    let writer = reopen(&directory, previous).await;
    let state = snapshot(&writer, "physical");
    assert_eq!(state.status, ExecutionStatus::Cancelled);
    assert_eq!(state.tasks[0].status, TaskStatus::Cancelled);
    assert!(state.tasks[0].result.is_none() && state.tasks[0].failure.is_none());
    assert_eq!(state.attempts_used, 1);
    assert_eq!(state.attempt_history, before.attempt_history);
    assert_eq!(state.values, before.values);
    assert!(original.cancellation_requested() && retained.cancellation_requested());
    positive(&callback_gate);
    positive(&completion);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_abandoned_viewers_keep_signal_clear_and_real_result_owned() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let previous = store.owner_generation();
    let callback = Callback::new(Return::Succeed);
    let gate = callback.gate.clone();
    let _cleanup = Cleanup(gate.clone());
    let app = application(definition(), vec![callback.clone()]);
    let weak_store = Arc::downgrade(&store);
    let weak_app = Arc::downgrade(&app);
    let host = host(store.clone(), app.clone());
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    let mut primary = open(
        server.address,
        "POST",
        "/threads/abandoned/runs/stream",
        RUN,
    )
    .await;
    until(|| gate.facts().entered).await;
    let _headers = prefix(&mut primary, b"\r\n\r\n").await;
    let mut attached = open(server.address, "GET", "/threads/abandoned/runs/stream", "").await;
    let _state = prefix(&mut attached, b"event: state").await;
    let observer = callback.observer().unwrap();
    let retained = observer.clone();
    let before = snapshot(store.as_ref(), "abandoned");
    drop(primary);
    drop(attached);
    assert!(!observer.cancellation_requested());
    excluded(&server, &directory, "abandoned").await;
    gate.release();
    until(|| snapshot(store.as_ref(), "abandoned").status == ExecutionStatus::Completed).await;
    assert!(callback.returned.load(Ordering::Acquire));
    server.stop().await;
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(10))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    drop(host);
    drop(app);
    drop(callback);
    drop(store);
    assert!(weak_store.upgrade().is_none() && weak_app.upgrade().is_none());
    let writer = reopen(&directory, previous).await;
    let state = snapshot(&writer, "abandoned");
    assert_eq!(state.status, ExecutionStatus::Completed);
    assert_eq!(state.tasks[0].status, TaskStatus::Succeeded);
    let NativeOutcome::Succeeded(actual) = genuine_success() else {
        unreachable!()
    };
    assert_eq!(state.tasks[0].result, Some(actual));
    assert_eq!(state.values["effect-result"], json!("actual-write"));
    assert_eq!(state.attempts_used, 1);
    assert_eq!(state.attempt_history, before.attempt_history);
    assert!(!observer.cancellation_requested() && !retained.cancellation_requested());
    positive(&gate);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_abandoned_admitted_cancel_completes_at_both_store_boundaries() {
    for after_write in [false, true] {
        let directory = Directory::new();
        let inner = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
        let boundary_gate = Gate::new();
        let _boundary = Cleanup(boundary_gate.clone());
        let mut boundary = BoundaryStore::new(inner.clone());
        if after_write {
            boundary.after_cancel = Some(boundary_gate.clone());
        } else {
            boundary.before_cancel = Some(boundary_gate.clone());
        }
        let store = Arc::new(boundary);
        let callback = Callback::new(Return::Acknowledge);
        let _return = Cleanup(callback.gate.clone());
        let host = host(
            store.clone(),
            application(definition(), vec![callback.clone()]),
        );
        let server = Server::start(&host).await;
        let primary = open(
            server.address,
            "POST",
            "/threads/cancel-cut/runs/stream",
            RUN,
        )
        .await;
        until(|| callback.gate.facts().entered).await;
        let before = snapshot(inner.as_ref(), "cancel-cut");
        let cancel = open(server.address, "POST", "/threads/cancel-cut/cancel", "").await;
        until(|| boundary_gate.facts().entered).await;
        assert!(!callback.notified());
        let held = snapshot(inner.as_ref(), "cancel-cut");
        assert_eq!(
            held.status,
            if after_write {
                ExecutionStatus::CancelRequested
            } else {
                ExecutionStatus::Running
            }
        );
        assert_eq!(held.tasks[0].status, TaskStatus::InFlight);
        assert_eq!(held.attempt_history, before.attempt_history);
        drop(cancel);
        boundary_gate.release();
        until(|| callback.notified()).await;
        cancelled_terminal(&finish(primary, Vec::new()).await);
        excluded(&server, &directory, "cancel-cut").await;
        callback.gate.release();
        until(|| snapshot(inner.as_ref(), "cancel-cut").status == ExecutionStatus::Cancelled).await;
        server.stop().await;
        finish_host(&host, false).await;
        assert!(callback.returned.load(Ordering::Acquire));
        assert_eq!(callback.controlled.load(Ordering::Acquire), 1);
        acknowledged(&callback);
        let state = snapshot(inner.as_ref(), "cancel-cut");
        assert_eq!(state.tasks[0].status, TaskStatus::Cancelled);
        assert!(state.tasks[0].result.is_none() && state.tasks[0].failure.is_none());
        assert_eq!(state.values, before.values);
        positive(&boundary_gate);
        positive(&callback.gate);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_ordinary_shutdown_retains_callback_without_cancel_intent_or_resources_in_observers() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let previous = store.owner_generation();
    let callback = Callback::new(Return::Succeed);
    let gate = callback.gate.clone();
    let normal_return = callback.returned.clone();
    let _cleanup = Cleanup(gate.clone());
    let app = application(definition(), vec![callback.clone()]);
    let weak_store = Arc::downgrade(&store);
    let weak_app = Arc::downgrade(&app);
    let weak_callback = Arc::downgrade(&callback);
    let host = host(store.clone(), app.clone());
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    let mut primary = open(server.address, "POST", "/threads/shutdown/runs/stream", RUN).await;
    until(|| gate.facts().entered).await;
    let bytes = prefix(&mut primary, b"\r\n\r\n").await;
    let before = snapshot(store.as_ref(), "shutdown");
    let observer = callback.observer().unwrap();
    let clone = observer.clone();
    incomplete(&shutdown, false).await;
    assert!(!observer.cancellation_requested());
    assert_eq!(snapshot(store.as_ref(), "shutdown"), before);
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    // Close the viewer and normally join the server before releasing the actual callback.
    drop(primary);
    drop(bytes);
    server.stop().await;
    drop(host);
    drop(app);
    drop(callback);
    drop(store);
    assert!(weak_store.upgrade().is_some() && weak_app.upgrade().is_some());
    assert!(!observer.cancellation_requested() && !clone.cancellation_requested());
    gate.release();
    until(|| normal_return.load(Ordering::Acquire)).await;
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(10))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: false }
    );
    assert!(
        weak_store.upgrade().is_none()
            && weak_app.upgrade().is_none()
            && weak_callback.upgrade().is_none()
    );
    let writer = reopen(&directory, previous).await;
    let state = snapshot(&writer, "shutdown");
    assert_eq!(state.status, ExecutionStatus::Completed);
    assert_eq!(state.tasks[0].status, TaskStatus::Succeeded);
    let NativeOutcome::Succeeded(actual) = genuine_success() else {
        unreachable!()
    };
    assert_eq!(state.tasks[0].result, Some(actual));
    assert!(!observer.cancellation_requested() && !clone.cancellation_requested());
    positive(&gate);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_late_sibling_fault_preserves_single_cancelled_terminal_and_known_acknowledgement() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let uncertain = Callback::new(Return::Uncertain);
    let ack = Callback::new(Return::Acknowledge);
    let _a = Cleanup(uncertain.gate.clone());
    let _b = Cleanup(ack.gate.clone());
    let mut definition = definition();
    definition.batches[0].push(TaskSpec {
        id: TaskId::new("sibling").unwrap(),
        effect_id: EffectId::new("sibling-effect").unwrap(),
        recovery: RecoveryPolicy::Pure,
        requires_approval: false,
    });
    let host = host(
        store.clone(),
        application(definition, vec![uncertain.clone(), ack.clone()]),
    );
    let server = Server::start(&host).await;
    let primary = open(
        server.address,
        "POST",
        "/threads/late-fault/runs/stream",
        RUN,
    )
    .await;
    until(|| uncertain.gate.facts().entered && ack.gate.facts().entered).await;
    let before = snapshot(store.as_ref(), "late-fault");
    let mut attached = open(server.address, "GET", "/threads/late-fault/runs/stream", "").await;
    let bytes = prefix(&mut attached, b"event: state").await;
    assert_eq!(
        exchange(server.address, "POST", "/threads/late-fault/cancel", "")
            .await
            .status,
        200
    );
    cancelled_terminal(&finish(primary, Vec::new()).await);
    cancelled_terminal(&finish(attached, bytes).await);
    uncertain.gate.release();
    until(|| host.shutdown_handle().status().faulted).await;
    assert!(ack.notified());
    assert!(!ack.returned.load(Ordering::Acquire));
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    ack.gate.release();
    server.stop().await;
    finish_host(&host, true).await;
    let state = snapshot(store.as_ref(), "late-fault");
    assert_eq!(state.status, ExecutionStatus::CancelRequested);
    assert_eq!(state.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(state.tasks[1].status, TaskStatus::Cancelled);
    assert!(state
        .tasks
        .iter()
        .all(|t| t.result.is_none() && t.failure.is_none()));
    assert_eq!(state.attempts_used, 2);
    assert_eq!(state.attempt_history, before.attempt_history);
    assert_eq!(state.values, before.values);
    assert!(ack.notified() && ack.returned.load(Ordering::Acquire));
    acknowledged(&ack);
    positive(&uncertain.gate);
    positive(&ack.gate);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_false_acknowledgement_retires_physical_owners_with_false_observer_clones() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let previous = store.owner_generation();
    let callback = Callback::new(Return::FalseAcknowledgement);
    let gate = callback.gate.clone();
    let returned = callback.returned.clone();
    let _cleanup = Cleanup(gate.clone());
    let app = application(definition(), vec![callback.clone()]);
    let weak_store = Arc::downgrade(&store);
    let weak_app = Arc::downgrade(&app);
    let host = host(store.clone(), app.clone());
    let shutdown = host.shutdown_handle();
    let server = Server::start(&host).await;
    let primary = open(server.address, "POST", "/threads/false-ack/runs/wait", RUN).await;
    until(|| gate.facts().entered).await;
    let original = store
        .thread_view(&ThreadId::new("false-ack").unwrap())
        .unwrap()
        .unwrap();
    let observer = callback.observer().unwrap();
    let clone = observer.clone();
    assert!(!observer.cancellation_requested());
    assert!(matches!(
        b4_sqlite::SqliteStore::open(&directory.0),
        Err(StoreError::AlreadyOwned)
    ));
    gate.release();
    let response = finish(primary, Vec::new()).await;
    assert_eq!(response.status, 503);
    assert_eq!(
        serde_json::from_str::<Value>(&response.body).unwrap()["error"]["details"]["code"],
        "host_unready"
    );
    until(|| returned.load(Ordering::Acquire)).await;
    server.stop().await;
    assert_eq!(
        shutdown
            .wait_quiescent(Duration::from_secs(10))
            .await
            .unwrap(),
        ShutdownOutcome::Quiescent { faulted: true }
    );
    assert_eq!(
        store.thread_view(&original.thread.id).unwrap(),
        Some(original.clone())
    );
    drop(host);
    drop(store);
    drop(app);
    drop(callback);
    assert!(weak_store.upgrade().is_none() && weak_app.upgrade().is_none());
    let writer = reopen(&directory, previous).await;
    assert_eq!(
        writer.thread_view(&original.thread.id).unwrap(),
        Some(original)
    );
    assert!(!observer.cancellation_requested() && !clone.cancellation_requested());
    assert!(returned.load(Ordering::Acquire));
    positive(&gate);
}
