use b4_contract::*;
use b4_runtime::*;
use serde_json::{json, Map, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone)]
struct Callback {
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>,
    outcome: Result<NativeOutcome, EffectUncertain>,
    _captured_secret: &'static str,
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        self.outcome.clone()
    }
}
struct Validator(Arc<AtomicUsize>);
impl InputValidator for Validator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.0.fetch_add(1, Ordering::SeqCst);
        if input == &json!({"valid":true}) {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}
fn identity() -> ApplicationIdentity {
    ApplicationIdentity {
        application: "demo".into(),
        revision: "app-v1".into(),
    }
}
fn callback(output: Value) -> Callback {
    Callback {
        calls: Arc::new(AtomicUsize::new(0)),
        seen: Arc::new(Mutex::new(vec![])),
        outcome: Ok(NativeOutcome::Succeeded(TaskResult {
            output,
            writes: vec![],
        })),
        _captured_secret: "callback-captured-secret",
    }
}
fn registration(count: usize, callback: &Callback) -> RouteRegistration {
    let specs: Vec<_> = (0..count)
        .map(|i| TaskSpec {
            id: TaskId::new(format!("task-{i}")).unwrap(),
            effect_id: EffectId::new(format!("effect-{i}")).unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        })
        .collect();
    RouteRegistration {
        definition: Definition {
            route: RouteKey::new("/native#workflow").unwrap(),
            revision: ExecutionRevision::new("def-v1").unwrap(),
            batches: if count == 0 {
                vec![]
            } else {
                vec![specs.clone()]
            },
            max_attempts: count as u32,
        },
        effects: specs
            .into_iter()
            .map(|s| {
                (
                    s.effect_id,
                    EffectBinding {
                        callback: Arc::new(callback.clone()),
                        output_kind: OutputKind::Value,
                    },
                )
            })
            .collect(),
        input_validator: None,
    }
}
fn call(definition: &Definition) -> EffectInvocation {
    let spec = &definition.batches[0][0];
    EffectInvocation {
        execution_id: ExecutionId::new("execution-1").unwrap(),
        turn_id: TurnId::new("turn-1").unwrap(),
        owner_generation: 1,
        task_id: spec.id.clone(),
        effect_id: spec.effect_id.clone(),
        attempt_id: AttemptId::new("attempt-1").unwrap(),
        input: Map::from_iter([("input".into(), json!({"valid":true}))]),
    }
}
fn context(definition: &Definition) -> InvocationContext {
    InvocationContext {
        thread_id: ThreadId::new("thread-1").unwrap(),
        thread_generation: ThreadGeneration::new("ab".repeat(32)).unwrap(),
        origin: ThreadOrigin {
            application: "demo".into(),
            tenant: "tenant".into(),
            principal: "actor".into(),
        },
        application_revision: "app-v1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: "policy-v1".into(),
    }
}

#[test]
fn valid_registry_keeps_original_immutable_definition_and_exact_callback_context() {
    let effect = callback(json!({"ok":true}));
    let route = registration(1, &effect);
    let original = route.definition.clone();
    let application = NativeApplication::new(identity(), vec![route]).unwrap();
    let call = call(&original);
    let context = context(&original);
    assert_eq!(application.definition(&original.route), Some(&original));
    assert_eq!(application.definitions().count(), 1);
    assert_eq!(
        application.invoke(&original, &call, &context).unwrap(),
        EffectOutcome::Succeeded(TaskResult {
            output: json!({"ok":true}),
            writes: vec![]
        })
    );
    assert_eq!(*effect.seen.lock().unwrap(), vec![(call, context)]);
}

#[test]
fn identity_labels_count_utf8_bytes_and_reject_empty_controls_and_overflow() {
    let effect = callback(Value::Null);
    let route = registration(1, &effect);
    for label in ["".into(), "bad\nlabel".into(), "é".repeat(129)] {
        for application_label in [true, false] {
            let mut id = identity();
            if application_label {
                id.application = label.clone()
            } else {
                id.revision = label.clone()
            }
            assert!(NativeApplication::new(id, vec![route.clone()]).is_err());
        }
    }
    assert!(NativeApplication::new(
        ApplicationIdentity {
            application: "é".repeat(128),
            revision: "v".repeat(256)
        },
        vec![route]
    )
    .is_ok());
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn finite_program_and_attempt_bounds_reject_before_any_callback() {
    let effect = callback(Value::Null);
    assert!(NativeApplication::new(identity(), vec![]).is_err());
    assert!(NativeApplication::new(identity(), vec![registration(0, &effect)]).is_err());
    assert!(NativeApplication::new(identity(), vec![registration(65, &effect)]).is_err());
    assert!(NativeApplication::new(identity(), vec![registration(64, &effect)]).is_ok());
    for budget in [0, 1, 65] {
        let mut r = registration(2, &effect);
        r.definition.max_attempts = budget;
        assert!(NativeApplication::new(identity(), vec![r]).is_err());
    }
    let routes: Vec<_> = (0..65)
        .map(|i| {
            let mut r = registration(1, &effect);
            r.definition.route = RouteKey::new(format!("/native-{i}#workflow")).unwrap();
            r
        })
        .collect();
    assert!(NativeApplication::new(identity(), routes[..64].to_vec()).is_ok());
    assert!(NativeApplication::new(identity(), routes).is_err());
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn duplicate_routes_and_missing_or_extra_bindings_publish_no_registry() {
    let effect = callback(Value::Null);
    let route = registration(1, &effect);
    assert!(NativeApplication::new(identity(), vec![route.clone(), route.clone()]).is_err());
    let mut missing = route.clone();
    missing.effects.clear();
    assert!(NativeApplication::new(identity(), vec![missing]).is_err());
    let mut extra = route;
    extra.effects.insert(
        EffectId::new("extra").unwrap(),
        EffectBinding {
            callback: Arc::new(effect.clone()),
            output_kind: OutputKind::Value,
        },
    );
    assert!(NativeApplication::new(identity(), vec![extra]).is_err());
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_kernel_definitions_reject_duplicate_tasks_effects_and_empty_batches() {
    let effect = callback(Value::Null);
    for variant in 0..3 {
        let mut r = registration(2, &effect);
        match variant {
            0 => r.definition.batches[0][1].id = r.definition.batches[0][0].id.clone(),
            1 => {
                r.definition.batches[0][1].effect_id = r.definition.batches[0][0].effect_id.clone()
            }
            _ => r.definition.batches.push(vec![]),
        }
        assert!(NativeApplication::new(identity(), vec![r]).is_err());
    }
}

#[test]
fn construction_never_invokes_callback_or_validator_in_valid_or_rejected_assemblies() {
    let effect = callback(Value::Null);
    let validation_calls = Arc::new(AtomicUsize::new(0));
    let mut route = registration(1, &effect);
    route.input_validator = Some(Arc::new(Validator(validation_calls.clone())));
    NativeApplication::new(identity(), vec![route.clone()]).unwrap();
    assert!(NativeApplication::new(identity(), vec![route.clone(), route]).is_err());
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
    assert_eq!(validation_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn full_selected_definition_mismatch_never_falls_back_to_current_revision() {
    let effect = callback(Value::Null);
    let route = registration(1, &effect);
    let selected = route.definition.clone();
    let application = NativeApplication::new(identity(), vec![route]).unwrap();
    for variant in 0..4 {
        let mut different = selected.clone();
        match variant {
            0 => different.revision = ExecutionRevision::new("newer-revision").unwrap(),
            1 => different.max_attempts += 1,
            2 => different.batches[0][0].recovery = RecoveryPolicy::Unsafe,
            _ => different.route = RouteKey::new("/missing#workflow").unwrap(),
        }
        assert_eq!(
            application.invoke(&different, &call(&selected), &context(&selected)),
            Err(NativeInvocationError::DefinitionUnavailable)
        );
        assert_eq!(
            application.output_kind(&different, &call(&selected).effect_id),
            None
        );
        assert_eq!(
            application.validate_input(&different, &Value::Null),
            Err(ApplicationInputError::DefinitionUnavailable)
        );
    }
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn mismatched_origin_application_and_context_revisions_refuse_before_callback() {
    let effect = callback(Value::Null);
    let route = registration(1, &effect);
    let selected = route.definition.clone();
    let application = NativeApplication::new(identity(), vec![route]).unwrap();
    for variant in 0..6 {
        let mut context = context(&selected);
        match variant {
            0 => context.origin.application = "foreign".into(),
            1 => context.application_revision = "different-app-version".into(),
            2 => context.execution_revision = ExecutionRevision::new("different-program").unwrap(),
            3 => context.policy_revision = String::new(),
            4 => context.origin.tenant = "bad\ntenant".into(),
            _ => context.origin.principal = "x".repeat(257),
        }
        assert_eq!(
            application.invoke(&selected, &call(&selected), &context),
            Err(NativeInvocationError::ContextMismatch)
        );
    }
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn task_effect_binding_is_exact_and_has_no_first_handler_fallback() {
    let effect = callback(Value::Null);
    let route = registration(2, &effect);
    let selected = route.definition.clone();
    let application = NativeApplication::new(identity(), vec![route]).unwrap();
    for variant in 0..3 {
        let mut call = call(&selected);
        match variant {
            0 => call.task_id = TaskId::new("missing").unwrap(),
            1 => call.effect_id = selected.batches[0][1].effect_id.clone(),
            _ => call.effect_id = EffectId::new("missing").unwrap(),
        }
        assert_eq!(
            application.invoke(&selected, &call, &context(&selected)),
            Err(NativeInvocationError::TaskEffectMismatch)
        );
    }
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn native_value_bindings_preserve_null_and_falsy_values() {
    for output in [Value::Null, json!(false), json!(0), json!("")] {
        let effect = callback(output.clone());
        let route = registration(1, &effect);
        let selected = route.definition.clone();
        let application = NativeApplication::new(identity(), vec![route]).unwrap();
        assert_eq!(
            application.invoke(&selected, &call(&selected), &context(&selected)),
            Ok(EffectOutcome::Succeeded(TaskResult {
                output,
                writes: vec![]
            }))
        );
        assert_eq!(
            application.output_kind(&selected, &call(&selected).effect_id),
            Some(OutputKind::Value)
        );
    }
}

#[test]
fn void_binding_rejects_nonnull_success_but_preserves_known_failure() {
    for outcome in [
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: Value::Null,
            writes: vec![],
        })),
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!(false),
            writes: vec![],
        })),
        Ok(NativeOutcome::Failed("known public failure".into())),
        Err(EffectUncertain),
    ] {
        let mut effect = callback(Value::Null);
        effect.outcome = outcome.clone();
        let mut route = registration(1, &effect);
        let selected = route.definition.clone();
        route.effects.values_mut().next().unwrap().output_kind = OutputKind::Void;
        let application = NativeApplication::new(identity(), vec![route]).unwrap();
        let expected = match outcome {
            Ok(NativeOutcome::Succeeded(r)) if r.output.is_null() => {
                Ok(EffectOutcome::Succeeded(r))
            }
            Ok(NativeOutcome::Succeeded(_)) => Err(NativeInvocationError::InvalidOutput),
            Ok(NativeOutcome::Failed(reason)) => Ok(EffectOutcome::Failed(reason)),
            Err(_) => Err(NativeInvocationError::Uncertain),
        };
        assert_eq!(
            application.invoke(&selected, &call(&selected), &context(&selected)),
            expected
        );
        assert_eq!(
            application.output_kind(&selected, &call(&selected).effect_id),
            Some(OutputKind::Void)
        );
    }
}

#[test]
fn optional_validator_receives_original_json_only_for_exact_selected_definition() {
    let effect = callback(Value::Null);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut route = registration(1, &effect);
    let selected = route.definition.clone();
    route.input_validator = Some(Arc::new(Validator(calls.clone())));
    let application = NativeApplication::new(identity(), vec![route]).unwrap();
    let mut foreign = selected.clone();
    foreign.revision = ExecutionRevision::new("foreign").unwrap();
    assert_eq!(
        application.validate_input(&foreign, &json!({"valid":true})),
        Err(ApplicationInputError::DefinitionUnavailable)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        application.validate_input(&selected, &json!({"valid":true})),
        Ok(())
    );
    for input in [
        Value::Null,
        json!([]),
        json!(false),
        json!(1),
        json!({"valid":false}),
    ] {
        assert_eq!(
            application.validate_input(&selected, &input),
            Err(ApplicationInputError::InvalidInput)
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn no_validator_preserves_all_original_json_input_shapes() {
    let effect = callback(Value::Null);
    let route = registration(1, &effect);
    let selected = route.definition.clone();
    let application = NativeApplication::new(identity(), vec![route]).unwrap();
    for input in [
        json!({}),
        Value::Null,
        json!([]),
        json!(false),
        json!(1),
        json!(""),
    ] {
        assert_eq!(application.validate_input(&selected, &input), Ok(()));
    }
    assert_eq!(effect.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn default_debug_omits_callback_validator_captures_and_route_inventory() {
    let effect = callback(Value::Null);
    let mut route = registration(1, &effect);
    route.definition.route = RouteKey::new("/private-route-marker#workflow").unwrap();
    route.input_validator = Some(Arc::new(Validator(Arc::new(AtomicUsize::new(0)))));
    let debug = format!("{route:?} {:?}", route.effects.values().next().unwrap());
    let application = NativeApplication::new(identity(), vec![route]).unwrap();
    let debug = format!("{debug} {application:?}");
    assert!(!debug.contains("callback-captured-secret"));
    assert!(!debug.contains("private-route-marker"));
}
