use b4_contract::*;
use b4_runtime::*;
use serde_json::{json, Map, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::Duration;

struct OldOnlyCallback {
    calls: AtomicUsize,
    outcome: Result<NativeOutcome, EffectUncertain>,
}
impl NativeEffect for OldOnlyCallback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.outcome.clone()
    }
}

struct PanickingControlledCallback;
impl NativeEffect for PanickingControlledCallback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        Ok(NativeOutcome::Succeeded(result(Value::Null)))
    }

    fn invoke_controlled(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
        _: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        panic!("fixed controlled callback panic")
    }
}

// These exhaustive matches guard the old enums for source compatibility.
fn old_outcome(outcome: NativeOutcome) -> EffectOutcome {
    match outcome {
        NativeOutcome::Succeeded(result) => EffectOutcome::Succeeded(result),
        NativeOutcome::Failed(reason) => EffectOutcome::Failed(reason),
    }
}

fn old_error(error: NativeInvocationError) -> &'static str {
    match error {
        NativeInvocationError::DefinitionUnavailable => "definition",
        NativeInvocationError::ContextMismatch => "context",
        NativeInvocationError::TaskEffectMismatch => "binding",
        NativeInvocationError::InvalidOutput => "output",
        NativeInvocationError::Uncertain => "uncertain",
    }
}

struct DualCallback {
    old_calls: AtomicUsize,
    controlled_calls: AtomicUsize,
    old_seen: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
    controlled_seen: Mutex<Vec<(EffectInvocation, InvocationContext, bool)>>,
    retained: Mutex<Option<NativeCancellationObserver>>,
    controlled_outcome: Result<ControlledNativeOutcome, EffectUncertain>,
}
impl DualCallback {
    fn new(outcome: Result<ControlledNativeOutcome, EffectUncertain>) -> Self {
        Self {
            old_calls: AtomicUsize::new(0),
            controlled_calls: AtomicUsize::new(0),
            old_seen: Mutex::new(Vec::new()),
            controlled_seen: Mutex::new(Vec::new()),
            retained: Mutex::new(None),
            controlled_outcome: outcome,
        }
    }
}
impl NativeEffect for DualCallback {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.old_calls.fetch_add(1, Ordering::SeqCst);
        self.old_seen
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        Ok(NativeOutcome::Succeeded(result(json!("old entry"))))
    }

    fn invoke_controlled(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.controlled_calls.fetch_add(1, Ordering::SeqCst);
        self.controlled_seen.lock().unwrap().push((
            call.clone(),
            context.clone(),
            observer.cancellation_requested(),
        ));
        *self.retained.lock().unwrap() = Some(observer.clone());
        self.controlled_outcome.clone()
    }
}

fn result(output: Value) -> TaskResult {
    TaskResult {
        output,
        writes: vec![StateWrite::Replace {
            field: "value".into(),
            value: json!("real write"),
        }],
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
    let tasks: Vec<_> = (1..=2)
        .map(|i| TaskSpec {
            id: TaskId::new(format!("task-{i}")).unwrap(),
            effect_id: EffectId::new(format!("effect-{i}")).unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        })
        .collect();
    let definition = Definition {
        route: RouteKey::new("/controlled#workflow").unwrap(),
        revision: ExecutionRevision::new("definition-v1").unwrap(),
        batches: vec![tasks.clone()],
        max_attempts: 2,
    };
    let application = NativeApplication::new(
        ApplicationIdentity {
            application: "controlled-test".into(),
            revision: "application-v1".into(),
        },
        vec![RouteRegistration {
            definition: definition.clone(),
            effects: tasks
                .into_iter()
                .map(|task| {
                    (
                        task.effect_id,
                        EffectBinding {
                            callback: Arc::clone(&callback),
                            output_kind,
                        },
                    )
                })
                .collect(),
            input_validator: None,
        }],
    )
    .unwrap();
    let call = EffectInvocation {
        execution_id: ExecutionId::new("execution-1").unwrap(),
        turn_id: TurnId::new("turn-1").unwrap(),
        owner_generation: 1,
        task_id: TaskId::new("task-1").unwrap(),
        effect_id: EffectId::new("effect-1").unwrap(),
        attempt_id: AttemptId::new("attempt-1").unwrap(),
        input: Map::from_iter([("input".into(), json!({"untouched": true}))]),
    };
    let context = InvocationContext {
        thread_id: ThreadId::new("thread-1").unwrap(),
        thread_generation: ThreadGeneration::new("ab".repeat(32)).unwrap(),
        origin: ThreadOrigin {
            application: "controlled-test".into(),
            tenant: "tenant".into(),
            principal: "principal".into(),
        },
        application_revision: "application-v1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: "policy-v1".into(),
    };
    (application, definition, call, context)
}

#[test]
fn signal_commit_publication_reaches_existing_observer_clones() {
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    let clone = observer.clone();
    assert!(!observer.cancellation_requested());
    assert!(!clone.cancellation_requested());
    signal.commit_cancellation();
    assert!(observer.cancellation_requested());
    assert!(clone.cancellation_requested());
    signal.commit_cancellation();
    drop(signal);
    assert!(observer.cancellation_requested());
    assert!(clone.cancellation_requested());
}

#[test]
fn controlled_application_invokes_override_and_passes_exact_inputs() {
    let expected = ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(result(json!(
        "controlled entry"
    ))));
    let callback = Arc::new(DualCallback::new(Ok(expected.clone())));
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    assert_eq!(
        application.invoke(&definition, &call, &context),
        Ok(EffectOutcome::Succeeded(result(json!("old entry"))))
    );
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 1);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        *callback.old_seen.lock().unwrap(),
        vec![(call.clone(), context.clone())]
    );
    assert_eq!(
        application.invoke_controlled(&definition, &call, &context, &observer),
        Ok(expected)
    );
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 1);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *callback.controlled_seen.lock().unwrap(),
        vec![(call, context, false)]
    );
    let retained = callback.retained.lock().unwrap().take().unwrap();
    assert!(!retained.cancellation_requested());
    signal.commit_cancellation();
    assert!(retained.cancellation_requested());
}

#[test]
fn signal_default_and_uncommitted_producer_drop_leave_observers_clear() {
    let signal = NativeCancellationSignal::default();
    let observer = signal.observer();
    let clone = observer.clone();
    drop(signal);
    assert!(!observer.cancellation_requested());
    assert!(!clone.cancellation_requested());
}

#[test]
fn distinct_successor_signal_cannot_change_a_retained_observer() {
    let original = NativeCancellationSignal::new();
    let observer = original.observer();
    let successor = NativeCancellationSignal::new();
    let fresh = successor.observer();
    original.commit_cancellation();
    assert!(observer.cancellation_requested());
    assert!(!fresh.cancellation_requested());
    drop(original);
    successor.commit_cancellation();
    assert!(fresh.cancellation_requested());
    assert!(observer.cancellation_requested());
}

#[test]
fn observer_clone_crosses_threads_and_observes_publication() {
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (returned_tx, returned_rx) = mpsc::sync_channel(1);
    let child = std::thread::spawn(move || {
        started_tx.send(observer.cancellation_requested()).unwrap();
        if release_rx.recv_timeout(Duration::from_secs(2)).is_ok() {
            returned_tx.send(observer.cancellation_requested()).unwrap();
        }
    });
    let initial = started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    signal.commit_cancellation();
    release_tx.send(()).unwrap();
    let observed = returned_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    child.join().unwrap();
    assert!(!initial);
    assert!(observed);
}

#[test]
fn default_controlled_entry_preserves_old_success_failure_and_uncertainty() {
    for outcome in [
        Ok(NativeOutcome::Succeeded(result(Value::Null))),
        Ok(NativeOutcome::Succeeded(result(json!(false)))),
        Ok(NativeOutcome::Succeeded(result(json!(0)))),
        Ok(NativeOutcome::Succeeded(result(json!("")))),
        Ok(NativeOutcome::Failed("real known failure".into())),
        Err(EffectUncertain),
    ] {
        let callback = Arc::new(OldOnlyCallback {
            calls: AtomicUsize::new(0),
            outcome: outcome.clone(),
        });
        let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
        let signal = NativeCancellationSignal::new();
        let observer = signal.observer();
        assert_eq!(
            application.invoke(&definition, &call, &context),
            outcome
                .clone()
                .map(old_outcome)
                .map_err(|_| NativeInvocationError::Uncertain)
        );
        assert_eq!(
            application.invoke_controlled(&definition, &call, &context, &observer),
            outcome
                .clone()
                .map(ControlledNativeOutcome::Settled)
                .map_err(|_| NativeInvocationError::Uncertain)
        );
        assert!(!observer.cancellation_requested());
        signal.commit_cancellation();
        assert_eq!(
            application.invoke_controlled(&definition, &call, &context, &observer),
            outcome
                .map(ControlledNativeOutcome::Settled)
                .map_err(|_| NativeInvocationError::Uncertain)
        );
        assert_eq!(callback.calls.load(Ordering::SeqCst), 3);
    }
}

#[test]
fn default_controlled_void_result_rules_preserve_old_contract() {
    for outcome in [
        Ok(NativeOutcome::Succeeded(result(Value::Null))),
        Ok(NativeOutcome::Succeeded(result(json!(false)))),
        Ok(NativeOutcome::Failed("real void failure".into())),
        Err(EffectUncertain),
    ] {
        let callback = Arc::new(OldOnlyCallback {
            calls: AtomicUsize::new(0),
            outcome: outcome.clone(),
        });
        let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Void);
        let expected = match outcome {
            Ok(NativeOutcome::Succeeded(result)) if !result.output.is_null() => {
                Err(NativeInvocationError::InvalidOutput)
            }
            Ok(outcome) => Ok(ControlledNativeOutcome::Settled(outcome)),
            Err(EffectUncertain) => Err(NativeInvocationError::Uncertain),
        };
        assert_eq!(
            application.invoke_controlled(
                &definition,
                &call,
                &context,
                &NativeCancellationSignal::new().observer()
            ),
            expected
        );
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn controlled_cancellation_requires_the_supplied_observer_acceptance() {
    let callback = Arc::new(DualCallback::new(Ok(ControlledNativeOutcome::Cancelled)));
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    assert_eq!(
        application.invoke_controlled(&definition, &call, &context, &observer),
        Err(NativeInvocationError::Uncertain)
    );
    let unrelated = NativeCancellationSignal::new();
    unrelated.commit_cancellation();
    assert_eq!(
        application.invoke_controlled(&definition, &call, &context, &observer),
        Err(NativeInvocationError::Uncertain)
    );
    signal.commit_cancellation();
    assert_eq!(
        application.invoke_controlled(&definition, &call, &context, &observer),
        Ok(ControlledNativeOutcome::Cancelled)
    );
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 0);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *callback.controlled_seen.lock().unwrap(),
        vec![
            (call.clone(), context.clone(), false),
            (call.clone(), context.clone(), false),
            (call, context, true),
        ]
    );
}

#[test]
fn controlled_void_acknowledgement_contains_no_fabricated_result() {
    let callback = Arc::new(DualCallback::new(Ok(ControlledNativeOutcome::Cancelled)));
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Void);
    let signal = NativeCancellationSignal::new();
    signal.commit_cancellation();
    assert_eq!(
        application.invoke_controlled(&definition, &call, &context, &signal.observer()),
        Ok(ControlledNativeOutcome::Cancelled)
    );
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 0);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn controlled_uncertainty_after_acceptance_is_not_a_cancel_acknowledgement() {
    let callback = Arc::new(DualCallback::new(Err(EffectUncertain)));
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let signal = NativeCancellationSignal::new();
    signal.commit_cancellation();
    assert_eq!(
        application.invoke_controlled(&definition, &call, &context, &signal.observer()),
        Err(NativeInvocationError::Uncertain)
    );
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 0);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn controlled_callback_panic_remains_for_the_owning_host_to_classify() {
    let (application, definition, call, context) =
        fixture(Arc::new(PanickingControlledCallback), OutputKind::Value);
    let observer = NativeCancellationSignal::new().observer();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        application.invoke_controlled(&definition, &call, &context, &observer)
    }));
    assert!(result.is_err());
}

#[test]
fn controlled_full_definition_refusals_never_invoke_callbacks() {
    let callback = Arc::new(DualCallback::new(Ok(ControlledNativeOutcome::Cancelled)));
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let observer = NativeCancellationSignal::new().observer();
    for variant in 0..5 {
        let mut changed = definition.clone();
        match variant {
            0 => changed.route = RouteKey::new("/missing#workflow").unwrap(),
            1 => changed.revision = ExecutionRevision::new("different").unwrap(),
            2 => changed.max_attempts += 1,
            3 => changed.batches[0][0].recovery = RecoveryPolicy::Unsafe,
            _ => changed.batches[0][0].requires_approval = true,
        }
        let error = application
            .invoke_controlled(&changed, &call, &context, &observer)
            .unwrap_err();
        assert_eq!(error, NativeInvocationError::DefinitionUnavailable);
        assert_eq!(old_error(error), "definition");
    }
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 0);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn controlled_context_refusals_never_invoke_callbacks() {
    let callback = Arc::new(DualCallback::new(Ok(ControlledNativeOutcome::Cancelled)));
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let observer = NativeCancellationSignal::new().observer();
    for variant in 0..7 {
        let mut changed = context.clone();
        let mut changed_call = call.clone();
        match variant {
            0 => changed.origin.application = "different".into(),
            1 => changed.application_revision = "different".into(),
            2 => changed.execution_revision = ExecutionRevision::new("different").unwrap(),
            3 => changed.origin.tenant = String::new(),
            4 => changed.origin.principal = "bad\nprincipal".into(),
            5 => changed.policy_revision = "é".repeat(129),
            _ => changed_call.owner_generation = 0,
        }
        let error = application
            .invoke_controlled(&definition, &changed_call, &changed, &observer)
            .unwrap_err();
        assert_eq!(error, NativeInvocationError::ContextMismatch);
        assert_eq!(old_error(error), "context");
    }
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 0);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn controlled_task_effect_refusals_never_invoke_callbacks() {
    let callback = Arc::new(DualCallback::new(Ok(ControlledNativeOutcome::Cancelled)));
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let observer = NativeCancellationSignal::new().observer();
    for variant in 0..3 {
        let mut changed = call.clone();
        match variant {
            0 => changed.task_id = TaskId::new("missing").unwrap(),
            1 => changed.effect_id = EffectId::new("effect-2").unwrap(),
            _ => changed.effect_id = EffectId::new("missing").unwrap(),
        }
        let error = application
            .invoke_controlled(&definition, &changed, &context, &observer)
            .unwrap_err();
        assert_eq!(error, NativeInvocationError::TaskEffectMismatch);
        assert_eq!(old_error(error), "binding");
    }
    assert_eq!(callback.old_calls.load(Ordering::SeqCst), 0);
    assert_eq!(callback.controlled_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn retained_observer_does_not_retain_application_or_callback() {
    let callback = Arc::new(DualCallback::new(Ok(ControlledNativeOutcome::Settled(
        NativeOutcome::Succeeded(result(Value::Null)),
    ))));
    let callback_weak = Arc::downgrade(&callback);
    let (application, definition, call, context) = fixture(callback.clone(), OutputKind::Value);
    let application = Arc::new(application);
    let application_weak = Arc::downgrade(&application);
    let signal = NativeCancellationSignal::new();
    application
        .invoke_controlled(&definition, &call, &context, &signal.observer())
        .unwrap();
    let retained = callback.retained.lock().unwrap().take().unwrap();
    drop(application);
    drop(callback);
    assert!(application_weak.upgrade().is_none());
    assert!(callback_weak.upgrade().is_none());
    assert!(!retained.cancellation_requested());
    signal.commit_cancellation();
    drop(signal);
    assert!(retained.cancellation_requested());
}
