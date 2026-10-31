//! Focused controls for the explicitly opted-in native stream projection.
#[path = "native_cooperative_support/mod.rs"]
mod support;
use axum::{body::to_bytes, http::StatusCode};
use b4_contract::*;
use b4_http::*;
use b4_runtime::*;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use support::*;

struct Streaming {
    ordinary: AtomicUsize,
    ordinary_gate: Mutex<Option<Arc<Gate>>>,
    streaming: AtomicUsize,
    chunks: Vec<Value>,
    emissions: Mutex<Vec<NativeChunkResult>>,
    gate: Option<Arc<Gate>>,
    after: Option<Arc<Gate>>,
    outcome: u8,
}
impl Streaming {
    fn new(chunks: Vec<Value>) -> Self {
        Self {
            ordinary: AtomicUsize::new(0),
            ordinary_gate: Mutex::new(None),
            streaming: AtomicUsize::new(0),
            chunks,
            emissions: Mutex::new(vec![]),
            gate: None,
            after: None,
            outcome: 0,
        }
    }
    fn result(&self) -> Result<NativeOutcome, EffectUncertain> {
        match self.outcome {
            1 => Ok(NativeOutcome::Failed("expected_failure".into())),
            2 => Err(EffectUncertain),
            _ => Ok(NativeOutcome::Succeeded(TaskResult {
                output: json!({"final":42}),
                writes: vec![StateWrite::Replace {
                    field: "aggregate".into(),
                    value: json!(true),
                }],
            })),
        }
    }
}
impl NativeEffect for Streaming {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.ordinary.fetch_add(1, Ordering::SeqCst);
        let gate = self.ordinary_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            assert!(gate.hold());
        }
        self.result()
    }
    fn invoke_streaming(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
        _: &NativeCancellationObserver,
        sink: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.streaming.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            assert!(gate.hold());
        }
        for chunk in &self.chunks {
            self.emissions.lock().unwrap().push(sink.emit(chunk));
        }
        if let Some(gate) = &self.after {
            assert!(gate.hold());
        }
        self.result().map(ControlledNativeOutcome::Settled)
    }
}
fn streaming_host<S: approval::ApprovalStore + 'static>(
    store: Arc<S>,
    callback: Arc<Streaming>,
    mut limits: impl FnMut(&mut HostLimits),
) -> HttpHost<S> {
    let app = application(definition(), vec![callback]);
    let mut c = config(app, store.approval_durable(), 1);
    limits(&mut c.limits);
    HttpHost::new_with_streaming_application(store, Arc::new(Auth), Arc::new(Owner), c).unwrap()
}
async fn wire(response: axum::response::Response) -> Vec<(String, Value)> {
    let bytes = tokio::time::timeout(
        Duration::from_secs(10),
        to_bytes(response.into_body(), 1_000_000),
    )
    .await
    .unwrap()
    .unwrap();
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .split("\n\n")
        .filter_map(|frame| {
            let name = frame.lines().find_map(|s| s.strip_prefix("event: "))?;
            let data = frame.lines().find_map(|s| s.strip_prefix("data: "))?;
            Some((name.into(), serde_json::from_str(data).unwrap()))
        })
        .collect()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opt_in_stream_has_trusted_lifecycle_and_direct_chunks_while_wait_and_legacy_stay_legacy() {
    let callback = Arc::new(Streaming::new(vec![json!({"direct":[1,true]})]));
    let store = Arc::new(MemoryStore::new());
    let host = streaming_host(store.clone(), callback.clone(), |_| {});
    let events = wire(call(&host, "POST", "/threads/stream/runs/stream", RUN).await).await;
    let names: Vec<_> = events.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["tool_call", "chunk", "tool_result", "done"]);
    assert_eq!(events[0].1["name"], "effect");
    assert_eq!(events[0].1["input"], json!({"input":{"original":true}}));
    assert_eq!(events[1].1, json!({"direct":[1,true]}));
    assert_eq!(
        events[2].1,
        json!({"id":events[0].1["id"],"name":"effect","output":{"final":42}})
    );
    assert_eq!(events[3].1, json!({"output":{"final":42}}));
    assert_eq!(
        *callback.emissions.lock().unwrap(),
        vec![NativeChunkResult::Accepted]
    );
    let durable = snapshot(store.as_ref(), "stream");
    assert_eq!(
        durable.tasks[0].attempt_id.as_ref().unwrap().as_str(),
        events[0].1["id"].as_str().unwrap()
    );
    assert_eq!(durable.values["aggregate"], true);
    let ordinary_gate = Gate::new();
    let _ordinary_cleanup = Cleanup(ordinary_gate.clone());
    *callback.ordinary_gate.lock().unwrap() = Some(ordinary_gate.clone());
    let wait_host = host.clone();
    let waiting =
        tokio::spawn(async move { call(&wait_host, "POST", "/threads/wait/runs/wait", RUN).await });
    until(|| ordinary_gate.facts().entered).await;
    let attachment = call(&host, "GET", "/threads/wait/runs/stream", "").await;
    ordinary_gate.release();
    assert_eq!(json_body(joined(waiting).await).await, json!({"final":42}));
    let legacy_cut = wire(attachment).await;
    assert_eq!(
        legacy_cut
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>(),
        vec!["state", "done"]
    );
    assert_eq!(legacy_cut[0].1["turn"], json!([]));
    assert_eq!(legacy_cut[1].1, json!({"output":{"final":42}}));
    assert_eq!(callback.streaming.load(Ordering::SeqCst), 1);
    assert_eq!(callback.ordinary.load(Ordering::SeqCst), 1);
    finish_host(&host, false).await;
    let old = support::host(
        Arc::new(MemoryStore::new()),
        application(definition(), vec![callback.clone()]),
    );
    let events = wire(call(&old, "POST", "/threads/old/runs/stream", RUN).await).await;
    assert_eq!(
        events.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["done"]
    );
    assert_eq!(callback.ordinary.load(Ordering::SeqCst), 2);
    finish_host(&old, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_projection_bounds_close_only_projection_and_refuse_later_chunks() {
    let mut deep = json!(0);
    for _ in 0..65 {
        deep = json!([deep]);
    }
    for (label, chunks, payload, frames, bytes) in [
        (
            "payload",
            vec![json!("x".repeat(1024)), json!(0)],
            256,
            100,
            10000,
        ),
        ("frames", vec![json!(0), json!(1), json!(2)], 1024, 2, 10000),
        (
            "bytes",
            vec![json!("x".repeat(200)), json!(0)],
            1024,
            100,
            300,
        ),
        ("depth", vec![deep, json!(0)], 1024, 100, 10000),
    ] {
        let callback = Arc::new(Streaming::new(chunks));
        let store = Arc::new(MemoryStore::new());
        let host = streaming_host(store.clone(), callback.clone(), |l| {
            l.max_output_bytes = payload;
            l.turn_buffer_frames = frames;
            l.turn_buffer_bytes = bytes;
        });
        let _events =
            wire(call(&host, "POST", &format!("/threads/{label}/runs/stream"), RUN).await).await;
        finish_host(&host, false).await;
        let emissions = callback.emissions.lock().unwrap();
        assert!(
            emissions.contains(&NativeChunkResult::Limit),
            "{label}: {emissions:?}"
        );
        assert_eq!(emissions.last(), Some(&NativeChunkResult::Closed));
        assert_eq!(
            snapshot(store.as_ref(), label).tasks[0].status,
            TaskStatus::Succeeded
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_and_uncertain_callbacks_never_publish_a_successful_result() {
    for outcome in [1, 2] {
        let callback = Arc::new(Streaming {
            outcome,
            ..Streaming::new(vec![json!("observed")])
        });
        let host = streaming_host(Arc::new(MemoryStore::new()), callback, |_| {});
        let events = wire(call(&host, "POST", "/threads/outcome/runs/stream", RUN).await).await;
        assert!(events.iter().any(|(n, _)| n == "chunk"));
        assert!(!events.iter().any(|(n, _)| n == "tool_result"));
        finish_host(&host, outcome == 2).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_emit_refuses_promptly_then_rejected_cancel_resumes_committed_result() {
    let callback_gate = Gate::new();
    let _cleanup = Cleanup(callback_gate.clone());
    let cancel_gate = Gate::new();
    let _cleanup_cancel = Cleanup(cancel_gate.clone());
    let callback = Arc::new(Streaming {
        gate: Some(callback_gate.clone()),
        ..Streaming::new(vec![json!("refused")])
    });
    let inner = Arc::new(MemoryStore::new());
    let mut wrapper = support::store::BoundaryStore::new(inner.clone());
    wrapper.before_cancel = Some(cancel_gate.clone());
    let store = Arc::new(wrapper);
    let host = streaming_host(store.clone(), callback.clone(), |_| {});
    let response = call(&host, "POST", "/threads/pending/runs/stream", RUN).await;
    until(|| callback_gate.facts().entered).await;
    let cancel_host = host.clone();
    let cancel =
        tokio::spawn(
            async move { call(&cancel_host, "POST", "/threads/pending/cancel", "").await },
        );
    until(|| cancel_gate.facts().entered).await;
    callback_gate.release();
    until(|| !callback.emissions.lock().unwrap().is_empty()).await;
    assert_eq!(
        *callback.emissions.lock().unwrap(),
        vec![NativeChunkResult::Unavailable]
    );
    until(|| snapshot(inner.as_ref(), "pending").tasks[0].status == TaskStatus::Succeeded).await;
    assert!(
        !cancel_gate.facts().explicit,
        "callback/result settled independently of cancellation decision"
    );
    cancel_gate.release();
    assert_eq!(joined(cancel).await.status(), StatusCode::CONFLICT);
    let events = wire(response).await;
    assert_eq!(
        events.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["tool_call", "tool_result", "done"]
    );
    finish_host(&host, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_cancel_finishes_independently_of_callback_and_late_emit_is_closed() {
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let callback = Arc::new(Streaming {
        gate: Some(gate.clone()),
        ..Streaming::new(vec![json!("late")])
    });
    let store = Arc::new(MemoryStore::new());
    let host = streaming_host(store.clone(), callback.clone(), |_| {});
    let response = call(&host, "POST", "/threads/cancel/runs/stream", RUN).await;
    until(|| gate.facts().entered).await;
    assert_eq!(
        call(&host, "POST", "/threads/cancel/cancel", "")
            .await
            .status(),
        StatusCode::OK
    );
    let events = wire(response).await;
    assert_eq!(
        events.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["tool_call", "done"]
    );
    assert_eq!(events[1].1, json!({"output":{"cancelled":true}}));
    assert!(!gate.facts().explicit);
    assert_eq!(
        snapshot(store.as_ref(), "cancel").tasks[0].status,
        TaskStatus::InFlight
    );
    gate.release();
    finish_host(&host, false).await;
    assert_eq!(
        *callback.emissions.lock().unwrap(),
        vec![NativeChunkResult::Closed]
    );
    let settled = snapshot(store.as_ref(), "cancel");
    assert_eq!(settled.status, ExecutionStatus::Cancelled);
    assert_eq!(
        settled.tasks[0].result.as_ref().unwrap().output,
        json!({"final":42})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_digest_byte_limit_closes_projection_before_late_chunk() {
    // The fixed 36-byte attempt UUID gives this trusted tool_call a 122-byte
    // viewer frame but a 128-byte retained JSON array. Both must be bounded.
    let callback = Arc::new(Streaming::new(vec![json!(0)]));
    let store = Arc::new(MemoryStore::new());
    let host = streaming_host(store.clone(), callback.clone(), |limits| {
        limits.turn_buffer_bytes = 127;
    });
    let events = wire(call(&host, "POST", "/threads/digest/runs/stream", RUN).await).await;
    finish_host(&host, false).await;
    assert_eq!(
        *callback.emissions.lock().unwrap(),
        vec![NativeChunkResult::Closed]
    );
    assert!(events.is_empty());
    assert_eq!(
        snapshot(store.as_ref(), "digest").tasks[0].status,
        TaskStatus::Succeeded
    );
}
