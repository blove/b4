use crate::support::*;
use b4_contract::*;
use b4_runtime::*;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::atomic::Ordering,
    time::Duration,
};

#[test]
fn fixture_callback_panic_never_marks_normal_return() {
    let callback = Callback::new(Return::Panic);
    callback.gate.release();
    let signal = NativeCancellationSignal::new();
    signal.commit_cancellation();
    let call = EffectInvocation {
        execution_id: ExecutionId::new("execution").unwrap(),
        turn_id: TurnId::new("turn").unwrap(),
        owner_generation: 1,
        task_id: TaskId::new("task").unwrap(),
        effect_id: EffectId::new("effect").unwrap(),
        attempt_id: AttemptId::new("attempt").unwrap(),
        input: serde_json::Map::new(),
    };
    let context = InvocationContext {
        thread_id: ThreadId::new("thread").unwrap(),
        thread_generation: ThreadGeneration::new("0".repeat(64)).unwrap(),
        origin: ThreadOrigin {
            application: "cooperative-test".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        },
        application_revision: "app-v1".into(),
        execution_revision: ExecutionRevision::new("cooperative-v1").unwrap(),
        policy_revision: "policy-v1".into(),
    };
    assert!(catch_unwind(AssertUnwindSafe(|| callback.invoke_controlled(
        &call,
        &context,
        &signal.observer()
    )))
    .is_err());
    assert!(
        !callback.returned.load(Ordering::Acquire),
        "panic cannot witness positive normal callback return"
    );
    assert!(callback.panicked.load(Ordering::Acquire));
    assert_positive(&callback.gate);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_tcp_connect_and_backpressured_write_share_finite_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), receive)
            .await
            .unwrap();
        drop(stream);
    });
    let body = "x".repeat(16 * 1024 * 1024);
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        tcp::open_with_timeout(address, "POST", "/stall", &body, Duration::from_millis(20)),
    )
    .await;
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_ok(),
        "fixture open must finish under its own deadline"
    );
    assert!(
        result.unwrap().is_err(),
        "backpressured write must expire the finite setup deadline"
    );
}
