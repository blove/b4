use b4_contract::*;
use b4_runtime::*;
use serde_json::{json, Map, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn result(output: Value) -> TaskResult {
    TaskResult {
        output,
        writes: vec![],
    }
}

fn fixture(
    callback: Arc<dyn NativeEffect>,
    output_kind: OutputKind,
) -> (
    NativeApplication,
    Definition,
    EffectInvocation,
    InvocationContext,
) {
    let task = TaskSpec {
        id: TaskId::new("task").unwrap(),
        effect_id: EffectId::new("effect").unwrap(),
        recovery: RecoveryPolicy::Pure,
        requires_approval: false,
    };
    let definition = Definition {
        route: RouteKey::new("/stream#workflow").unwrap(),
        revision: ExecutionRevision::new("execution-v1").unwrap(),
        batches: vec![vec![task.clone()]],
        max_attempts: 1,
    };
    let application = NativeApplication::new(
        ApplicationIdentity {
            application: "stream-test".into(),
            revision: "app-v1".into(),
        },
        vec![RouteRegistration {
            definition: definition.clone(),
            effects: [(
                task.effect_id.clone(),
                EffectBinding {
                    callback,
                    output_kind,
                },
            )]
            .into(),
            input_validator: None,
        }],
    )
    .unwrap();
    let call = EffectInvocation {
        execution_id: ExecutionId::new("execution").unwrap(),
        turn_id: TurnId::new("turn").unwrap(),
        owner_generation: 1,
        task_id: task.id,
        effect_id: task.effect_id,
        attempt_id: AttemptId::new("attempt").unwrap(),
        input: Map::from_iter([("query".into(), json!("hello"))]),
    };
    let context = InvocationContext {
        thread_id: ThreadId::new("thread").unwrap(),
        thread_generation: ThreadGeneration::new("ab".repeat(32)).unwrap(),
        origin: ThreadOrigin {
            application: "stream-test".into(),
            tenant: "tenant".into(),
            principal: "principal".into(),
        },
        application_revision: "app-v1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: "policy-v1".into(),
    };
    (application, definition, call, context)
}

// The sink borrows caller-owned storage; streaming does not require 'static storage.
struct RecordingSink<'a> {
    chunks: &'a mut Vec<Value>,
}
impl NativeChunkSink for RecordingSink<'_> {
    fn emit(&mut self, chunk: &Value) -> NativeChunkResult {
        self.chunks.push(chunk.clone());
        NativeChunkResult::Accepted
    }
}

struct Legacy {
    calls: AtomicUsize,
}
impl NativeEffect for Legacy {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(result(json!("legacy"))))
    }
}

#[test]
fn streaming_default_preserves_legacy_callbacks_and_leaves_sink_empty() {
    let callback = Arc::new(Legacy {
        calls: AtomicUsize::new(0),
    });
    let (app, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let mut chunks = vec![];
    let outcome = app.invoke_streaming(
        &definition,
        &call,
        &context,
        &NativeCancellationSignal::new().observer(),
        &mut RecordingSink {
            chunks: &mut chunks,
        },
    );
    assert_eq!(
        outcome,
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
            result(json!("legacy"))
        )))
    );
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert!(chunks.is_empty());
}

struct Cooperative;
impl NativeEffect for Cooperative {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        panic!("streaming default must preserve the cooperative override")
    }
    fn invoke_controlled(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        assert_eq!(call.input["query"], json!("hello"));
        assert_eq!(context.origin.principal, "principal");
        assert!(observer.cancellation_requested());
        Ok(ControlledNativeOutcome::Cancelled)
    }
}

#[test]
fn streaming_default_delegates_to_cooperative_override_with_supplied_observer() {
    let (app, definition, call, context) = fixture(Arc::new(Cooperative), OutputKind::Value);
    let signal = NativeCancellationSignal::new();
    signal.commit_cancellation();
    let mut chunks = vec![];
    assert_eq!(
        app.invoke_streaming(
            &definition,
            &call,
            &context,
            &signal.observer(),
            &mut RecordingSink {
                chunks: &mut chunks
            }
        ),
        Ok(ControlledNativeOutcome::Cancelled)
    );
    assert!(chunks.is_empty());
}

struct Streaming {
    calls: AtomicUsize,
    outcome: Result<ControlledNativeOutcome, EffectUncertain>,
}
impl NativeEffect for Streaming {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        Ok(NativeOutcome::Failed("direct entry".into()))
    }
    fn invoke_streaming(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        _: &NativeCancellationObserver,
        sink: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(call.input["query"], json!("hello"));
        assert_eq!(context.policy_revision, "policy-v1");
        assert_eq!(
            sink.emit(&json!({"delta": "hello"})),
            NativeChunkResult::Accepted
        );
        assert_eq!(
            sink.emit(&json!({"delta": " world"})),
            NativeChunkResult::Accepted
        );
        self.outcome.clone()
    }
}
fn streaming(outcome: Result<ControlledNativeOutcome, EffectUncertain>) -> Arc<Streaming> {
    Arc::new(Streaming {
        calls: AtomicUsize::new(0),
        outcome,
    })
}

#[test]
fn streaming_override_emits_ordered_chunks_and_preserves_distinct_final_result() {
    let callback = streaming(Ok(ControlledNativeOutcome::Settled(
        NativeOutcome::Succeeded(result(json!("final"))),
    )));
    let (app, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let mut chunks = vec![];
    assert_eq!(
        app.invoke_streaming(
            &definition,
            &call,
            &context,
            &NativeCancellationSignal::new().observer(),
            &mut RecordingSink {
                chunks: &mut chunks
            }
        ),
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
            result(json!("final"))
        )))
    );
    assert_eq!(
        chunks,
        vec![json!({"delta": "hello"}), json!({"delta": " world"})]
    );
    assert_eq!(
        app.invoke(&definition, &call, &context),
        Ok(EffectOutcome::Failed("direct entry".into()))
    );
    assert_eq!(
        app.invoke_controlled(
            &definition,
            &call,
            &context,
            &NativeCancellationSignal::new().observer()
        ),
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Failed(
            "direct entry".into()
        )))
    );
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn streaming_rejects_wrong_definition_binding_and_context_before_emission() {
    let callback = streaming(Ok(ControlledNativeOutcome::Cancelled));
    let (app, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let observer = NativeCancellationSignal::new().observer();
    let mut chunks = vec![];
    let mut sink = RecordingSink {
        chunks: &mut chunks,
    };
    let mut wrong_definition = definition.clone();
    wrong_definition.max_attempts = 2;
    assert_eq!(
        app.invoke_streaming(&wrong_definition, &call, &context, &observer, &mut sink),
        Err(NativeInvocationError::DefinitionUnavailable)
    );
    let mut wrong_call = call.clone();
    wrong_call.effect_id = EffectId::new("other-effect").unwrap();
    assert_eq!(
        app.invoke_streaming(&definition, &wrong_call, &context, &observer, &mut sink),
        Err(NativeInvocationError::TaskEffectMismatch)
    );
    let mut wrong_context = context.clone();
    wrong_context.origin.application = "other-application".into();
    assert_eq!(
        app.invoke_streaming(&definition, &call, &wrong_context, &observer, &mut sink),
        Err(NativeInvocationError::ContextMismatch)
    );
    wrong_context = context.clone();
    wrong_context.application_revision = "app-v2".into();
    assert_eq!(
        app.invoke_streaming(&definition, &call, &wrong_context, &observer, &mut sink),
        Err(NativeInvocationError::ContextMismatch)
    );
    wrong_context = context.clone();
    wrong_context.execution_revision = ExecutionRevision::new("execution-v2").unwrap();
    assert_eq!(
        app.invoke_streaming(&definition, &call, &wrong_context, &observer, &mut sink),
        Err(NativeInvocationError::ContextMismatch)
    );
    assert_eq!(callback.calls.load(Ordering::SeqCst), 0);
    assert!(chunks.is_empty());
}

#[test]
fn chunks_do_not_turn_invalid_void_output_into_success() {
    let callback = streaming(Ok(ControlledNativeOutcome::Settled(
        NativeOutcome::Succeeded(result(json!("not void"))),
    )));
    let (app, definition, call, context) = fixture(callback, OutputKind::Void);
    let mut chunks = vec![];
    assert_eq!(
        app.invoke_streaming(
            &definition,
            &call,
            &context,
            &NativeCancellationSignal::new().observer(),
            &mut RecordingSink {
                chunks: &mut chunks
            }
        ),
        Err(NativeInvocationError::InvalidOutput)
    );
    assert_eq!(chunks.len(), 2);
}

#[test]
fn streaming_cancellation_acknowledgement_requires_its_own_observer() {
    let callback = streaming(Ok(ControlledNativeOutcome::Cancelled));
    let (app, definition, call, context) = fixture(callback, OutputKind::Void);
    let signal = NativeCancellationSignal::new();
    let unrelated = NativeCancellationSignal::new();
    unrelated.commit_cancellation();
    let mut chunks = vec![];
    let mut sink = RecordingSink {
        chunks: &mut chunks,
    };
    assert_eq!(
        app.invoke_streaming(&definition, &call, &context, &signal.observer(), &mut sink),
        Err(NativeInvocationError::Uncertain)
    );
    signal.commit_cancellation();
    assert_eq!(
        app.invoke_streaming(&definition, &call, &context, &signal.observer(), &mut sink),
        Ok(ControlledNativeOutcome::Cancelled)
    );
}

#[test]
fn streaming_failure_and_uncertainty_remain_final_even_after_chunks() {
    for (outcome, expected) in [
        (
            Ok(ControlledNativeOutcome::Settled(NativeOutcome::Failed(
                "known failure".into(),
            ))),
            Ok(ControlledNativeOutcome::Settled(NativeOutcome::Failed(
                "known failure".into(),
            ))),
        ),
        (Err(EffectUncertain), Err(NativeInvocationError::Uncertain)),
    ] {
        let (app, definition, call, context) = fixture(streaming(outcome), OutputKind::Value);
        let mut chunks = vec![];
        assert_eq!(
            app.invoke_streaming(
                &definition,
                &call,
                &context,
                &NativeCancellationSignal::new().observer(),
                &mut RecordingSink {
                    chunks: &mut chunks
                }
            ),
            expected
        );
        assert_eq!(chunks.len(), 2);
    }
}

#[test]
fn chunk_results_are_copyable_public_feedback_values() {
    let results = [
        NativeChunkResult::Accepted,
        NativeChunkResult::Unavailable,
        NativeChunkResult::Closed,
        NativeChunkResult::Limit,
    ];
    let copied = results;
    assert_eq!(copied, results);
    assert_eq!(
        format!("{:?}", results),
        "[Accepted, Unavailable, Closed, Limit]"
    );
}
