use b4_contract::*;
use b4_runtime::*;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone)]
struct Callback {
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>,
    output: Value,
}

impl NativeEffect for Callback {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.seen
            .lock()
            .unwrap()
            .push((invocation.clone(), context.clone()));
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: self.output.clone(),
            writes: vec![],
        }))
    }
}

fn callback(output: Value) -> Callback {
    Callback {
        calls: Arc::new(AtomicUsize::new(0)),
        seen: Arc::new(Mutex::new(vec![])),
        output,
    }
}

fn definition(revision: &str) -> Definition {
    Definition {
        route: RouteKey::new("/history#workflow").unwrap(),
        revision: ExecutionRevision::new(revision).unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("same-task").unwrap(),
            effect_id: EffectId::new("same-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]],
        max_attempts: 1,
    }
}

fn registration(definition: Definition, callback: &Callback) -> RouteRegistration {
    RouteRegistration {
        effects: BTreeMap::from([(
            definition.batches[0][0].effect_id.clone(),
            EffectBinding {
                callback: Arc::new(callback.clone()),
                output_kind: OutputKind::Value,
            },
        )]),
        definition,
        input_validator: None,
    }
}

fn invocation(definition: &Definition) -> EffectInvocation {
    let task = &definition.batches[0][0];
    EffectInvocation {
        execution_id: ExecutionId::new("history-execution").unwrap(),
        turn_id: TurnId::new("history-turn").unwrap(),
        owner_generation: 1,
        task_id: task.id.clone(),
        effect_id: task.effect_id.clone(),
        attempt_id: AttemptId::new("history-attempt").unwrap(),
        input: Map::from_iter([("input".into(), json!({"original": true}))]),
    }
}

fn context(definition: &Definition) -> InvocationContext {
    InvocationContext {
        thread_id: ThreadId::new("history-thread").unwrap(),
        thread_generation: ThreadGeneration::new("ab".repeat(32)).unwrap(),
        origin: ThreadOrigin {
            application: "history-app".into(),
            tenant: "history-tenant".into(),
            principal: "history-actor".into(),
        },
        application_revision: "assembly-v2".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: "history-policy".into(),
    }
}

#[test]
fn retained_definition_invokes_original_callback_with_installed_assembly_context() {
    let original = definition("execution-v1");
    let current = definition("execution-v2");
    let old_callback = callback(json!({"selected": "original-v1"}));
    let new_callback = callback(json!({"selected": "current-v2"}));
    let application = NativeApplication::new_with_history(
        ApplicationIdentity {
            application: "history-app".into(),
            revision: "assembly-v2".into(),
        },
        vec![registration(current.clone(), &new_callback)],
        vec![registration(original.clone(), &old_callback)],
    )
    .expect("both independently valid registrations assemble");

    // The current route remains usable, establishing valid identity and bindings
    // before the retained-definition expectation is evaluated.
    assert_eq!(application.definition(&current.route), Some(&current));
    assert_eq!(
        application.definitions().cloned().collect::<Vec<_>>(),
        vec![current.clone()]
    );
    let current_call = invocation(&current);
    let current_context = context(&current);
    assert_eq!(
        application
            .invoke(&current, &current_call, &current_context)
            .unwrap(),
        EffectOutcome::Succeeded(TaskResult {
            output: json!({"selected": "current-v2"}),
            writes: vec![],
        })
    );
    assert_eq!(new_callback.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        *new_callback.seen.lock().unwrap(),
        vec![(current_call, current_context)]
    );
    assert_eq!(old_callback.calls.load(Ordering::Acquire), 0);

    let original_call = invocation(&original);
    let original_context = context(&original);
    let exact_original = application.definition_exact(&original).cloned();
    let observed = application.invoke(&original, &original_call, &original_context);

    let outcome =
        observed.expect("retained v1 must invoke its original callback, not refuse history");
    assert_eq!(
        outcome,
        EffectOutcome::Succeeded(TaskResult {
            output: json!({"selected": "original-v1"}),
            writes: vec![],
        })
    );
    assert_eq!(exact_original, Some(original.clone()));
    assert_eq!(application.definition_exact(&current), Some(&current));
    assert_eq!(old_callback.calls.load(Ordering::Acquire), 1);
    assert_eq!(new_callback.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        *old_callback.seen.lock().unwrap(),
        vec![(original_call, original_context)]
    );
    assert_eq!(application.definition(&original.route), Some(&current));
    assert_eq!(application.definitions().count(), 1);
}

fn installed_identity() -> ApplicationIdentity {
    ApplicationIdentity {
        application: "history-app".into(),
        revision: "assembly-v2".into(),
    }
}

fn assemble(current: RouteRegistration, history: RouteRegistration) -> NativeApplication {
    NativeApplication::new_with_history(installed_identity(), vec![current], vec![history])
        .expect("independently valid current and approval-free history assemble")
}

#[derive(Clone)]
struct ExactValidator {
    accepted: Value,
    seen: Arc<Mutex<Vec<Value>>>,
}

impl InputValidator for ExactValidator {
    fn validate(&self, value: &Value) -> Result<(), InvalidApplicationInput> {
        self.seen.lock().unwrap().push(value.clone());
        if value == &self.accepted {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}

fn validator(accepted: Value) -> ExactValidator {
    ExactValidator {
        accepted,
        seen: Arc::new(Mutex::new(vec![])),
    }
}

fn bound_registration(definition: Definition, effect: &Callback) -> RouteRegistration {
    RouteRegistration {
        effects: definition
            .batches
            .iter()
            .flatten()
            .map(|task| {
                (
                    task.effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(effect.clone()),
                        output_kind: OutputKind::Value,
                    },
                )
            })
            .collect(),
        definition,
        input_validator: None,
    }
}

#[test]
fn combined_registration_bound_and_nonempty_current_are_enforced_before_user_code() {
    let effect = callback(json!("never-called-during-assembly"));
    let validation = validator(json!("never-validated-during-assembly"));
    let make = |name: String| {
        let mut route = registration(definition(&name), &effect);
        route.definition.route = RouteKey::new(format!("/{name}#workflow")).unwrap();
        route.input_validator = Some(Arc::new(validation.clone()));
        route
    };
    let current: Vec<_> = (0..64).map(|i| make(format!("current-{i}"))).collect();
    let application =
        NativeApplication::new_with_history(installed_identity(), current.clone(), vec![]).unwrap();
    assert_eq!(application.definitions().count(), 64);
    let mut too_many = current.clone();
    too_many.push(make("current-64".into()));
    assert!(NativeApplication::new_with_history(installed_identity(), too_many, vec![]).is_err());

    let original = registration(definition("retained-0"), &effect);
    let history: Vec<_> = (1..=63)
        .map(|i| registration(definition(&format!("retained-{i}")), &effect))
        .collect();
    let combined = NativeApplication::new_with_history(
        installed_identity(),
        vec![original.clone()],
        history.clone(),
    )
    .unwrap();
    assert_eq!(combined.definitions().count(), 1);
    for route in &history {
        assert_eq!(
            combined.definition_exact(&route.definition),
            Some(&route.definition)
        );
    }
    let mut excessive = history;
    excessive.push(registration(definition("retained-64"), &effect));
    assert!(NativeApplication::new_with_history(
        installed_identity(),
        vec![original.clone()],
        excessive,
    )
    .is_err());
    assert!(NativeApplication::new_with_history(installed_identity(), vec![], vec![]).is_err());
    assert!(
        NativeApplication::new_with_history(installed_identity(), vec![], vec![original]).is_err()
    );
    assert_eq!(effect.calls.load(Ordering::Acquire), 0);
    assert!(validation.seen.lock().unwrap().is_empty());
}

#[test]
fn duplicate_pairs_refuse_even_with_changed_definition_or_callback_bindings() {
    let old = callback(json!("old"));
    let replacement = callback(json!("replacement"));
    let current = registration(definition("v2"), &replacement);
    let original = registration(definition("v1"), &old);
    for changed_content in [false, true] {
        let mut collision = registration(current.definition.clone(), &old);
        if changed_content {
            collision.definition.max_attempts = 2;
        }
        assert!(NativeApplication::new_with_history(
            installed_identity(),
            vec![current.clone()],
            vec![collision],
        )
        .is_err());
        let mut historical_collision = registration(original.definition.clone(), &replacement);
        if changed_content {
            historical_collision.definition.max_attempts = 2;
        }
        assert!(NativeApplication::new_with_history(
            installed_identity(),
            vec![current.clone()],
            vec![original.clone(), historical_collision],
        )
        .is_err());
    }
    // Different revisions cannot make two entries current for one route. The
    // protected old constructor retains its established duplicate error too.
    assert!(NativeApplication::new_with_history(
        installed_identity(),
        vec![current.clone(), original.clone()],
        vec![],
    )
    .is_err());
    assert_eq!(
        NativeApplication::new(installed_identity(), vec![current, original]).unwrap_err(),
        ApplicationConfigError("duplicate_application_route")
    );
    assert_eq!(old.calls.load(Ordering::Acquire), 0);
    assert_eq!(replacement.calls.load(Ordering::Acquire), 0);
}

#[test]
fn removed_historical_route_invokes_exact_history_without_entering_current_collection() {
    let old = callback(json!("removed-original"));
    let current_effect = callback(json!("current"));
    let current = definition("v2");
    let mut removed = definition("v1");
    removed.route = RouteKey::new("/removed#workflow").unwrap();
    let application = assemble(
        registration(current.clone(), &current_effect),
        registration(removed.clone(), &old),
    );
    assert_eq!(application.definition(&removed.route), None);
    assert_eq!(
        application.definitions().cloned().collect::<Vec<_>>(),
        vec![current]
    );
    assert_eq!(application.definition_exact(&removed), Some(&removed));
    assert_eq!(
        application
            .invoke(&removed, &invocation(&removed), &context(&removed))
            .unwrap(),
        EffectOutcome::Succeeded(TaskResult {
            output: json!("removed-original"),
            writes: vec![]
        })
    );
    assert_eq!(old.calls.load(Ordering::Acquire), 1);
    assert_eq!(current_effect.calls.load(Ordering::Acquire), 0);
}

#[test]
fn any_future_historical_approval_refuses_while_current_approval_can_coexist() {
    let effect = callback(json!("not-entered"));
    let mut current = definition("v2");
    current.batches[0][0].requires_approval = true;
    let valid = assemble(
        registration(current.clone(), &effect),
        registration(definition("v1"), &effect),
    );
    assert_eq!(valid.definition(&current.route), Some(&current));
    assert!(valid.definition_exact(&definition("v1")).is_some());
    let mut future = definition("forbidden-history");
    future.batches.push(vec![TaskSpec {
        id: TaskId::new("future-task").unwrap(),
        effect_id: EffectId::new("future-effect").unwrap(),
        recovery: RecoveryPolicy::Pure,
        requires_approval: true,
    }]);
    future.max_attempts = 2;
    for first_batch_also_requires_approval in [false, true] {
        future.batches[0][0].requires_approval = first_batch_also_requires_approval;
        // Approval is legal in current work and these distinct future task and
        // effect identities pass the unchanged constructor's kernel checks.
        assert!(NativeApplication::new(
            installed_identity(),
            vec![bound_registration(future.clone(), &effect)],
        )
        .is_ok());
        assert!(NativeApplication::new_with_history(
            installed_identity(),
            vec![registration(current.clone(), &effect)],
            vec![bound_registration(future.clone(), &effect)],
        )
        .is_err());
    }
    assert_eq!(effect.calls.load(Ordering::Acquire), 0);
}

#[test]
fn historical_and_current_validators_select_their_original_inputs_without_invocation() {
    let old = callback(json!("old"));
    let new = callback(json!("new"));
    let original = definition("v1");
    let current = definition("v2");
    let old_validation = validator(json!({"input_version": "v1"}));
    let new_validation = validator(json!({"input_version": "v2"}));
    let mut historical_registration = registration(original.clone(), &old);
    historical_registration.input_validator = Some(Arc::new(old_validation.clone()));
    let mut current_registration = registration(current.clone(), &new);
    current_registration.input_validator = Some(Arc::new(new_validation.clone()));
    let application = assemble(current_registration, historical_registration);
    assert_eq!(
        application.validate_input(&original, &old_validation.accepted),
        Ok(())
    );
    assert_eq!(
        application.validate_input(&current, &new_validation.accepted),
        Ok(())
    );
    assert_eq!(
        application.validate_input(&original, &new_validation.accepted),
        Err(ApplicationInputError::InvalidInput)
    );
    assert_eq!(
        application.validate_input(&current, &old_validation.accepted),
        Err(ApplicationInputError::InvalidInput)
    );
    assert_eq!(
        *old_validation.seen.lock().unwrap(),
        vec![
            old_validation.accepted.clone(),
            new_validation.accepted.clone()
        ]
    );
    assert_eq!(
        *new_validation.seen.lock().unwrap(),
        vec![new_validation.accepted, old_validation.accepted]
    );
    assert_eq!(old.calls.load(Ordering::Acquire), 0);
    assert_eq!(new.calls.load(Ordering::Acquire), 0);
}

#[test]
fn retained_void_output_does_not_borrow_current_value_output_kind() {
    let original = definition("v1");
    let current = definition("v2");
    let effect_id = &original.batches[0][0].effect_id;
    let original_effect = callback(Value::Null);
    let current_effect = callback(json!({"current": "value"}));
    let mut historical = registration(original.clone(), &original_effect);
    historical.effects.get_mut(effect_id).unwrap().output_kind = OutputKind::Void;
    let application = assemble(registration(current.clone(), &current_effect), historical);
    assert_eq!(
        application.output_kind(&original, effect_id),
        Some(OutputKind::Void)
    );
    assert_eq!(
        application.output_kind(&current, effect_id),
        Some(OutputKind::Value)
    );
    assert_eq!(
        application
            .invoke(&original, &invocation(&original), &context(&original))
            .unwrap(),
        EffectOutcome::Succeeded(TaskResult {
            output: Value::Null,
            writes: vec![]
        })
    );
    assert_eq!(
        application
            .invoke(&current, &invocation(&current), &context(&current))
            .unwrap(),
        EffectOutcome::Succeeded(TaskResult {
            output: json!({"current": "value"}),
            writes: vec![]
        })
    );
    let invalid_effect = callback(json!({"invalid_original": true}));
    let mut invalid = registration(original.clone(), &invalid_effect);
    invalid.effects.get_mut(effect_id).unwrap().output_kind = OutputKind::Void;
    let invalid_application = assemble(registration(current, &current_effect), invalid);
    let signal = NativeCancellationSignal::new();
    assert_eq!(
        invalid_application.invoke(&original, &invocation(&original), &context(&original)),
        Err(NativeInvocationError::InvalidOutput)
    );
    assert_eq!(
        invalid_application.invoke_controlled(
            &original,
            &invocation(&original),
            &context(&original),
            &signal.observer()
        ),
        Err(NativeInvocationError::InvalidOutput)
    );
    assert_eq!(invalid_effect.calls.load(Ordering::Acquire), 2);
    assert_eq!(original_effect.calls.load(Ordering::Acquire), 1);
    assert_eq!(current_effect.calls.load(Ordering::Acquire), 1);
}

#[derive(Clone)]
struct OutcomeEffect {
    calls: Arc<AtomicUsize>,
    direct: Result<NativeOutcome, EffectUncertain>,
}
impl NativeEffect for OutcomeEffect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.direct.clone()
    }
}

#[test]
fn retained_known_failure_and_uncertainty_preserve_direct_and_default_controlled_outcomes() {
    let original = definition("v1");
    let new = callback(json!("must-not-fallback"));
    for direct in [
        Ok(NativeOutcome::Failed("original-known-failure".into())),
        Err(EffectUncertain),
    ] {
        let effect = OutcomeEffect {
            calls: Arc::new(AtomicUsize::new(0)),
            direct: direct.clone(),
        };
        let mut historical = registration(original.clone(), &new);
        historical
            .effects
            .get_mut(&original.batches[0][0].effect_id)
            .unwrap()
            .callback = Arc::new(effect.clone());
        let application = assemble(registration(definition("v2"), &new), historical);
        let signal = NativeCancellationSignal::new();
        match direct {
            Ok(NativeOutcome::Failed(reason)) => {
                assert_eq!(
                    application.invoke(&original, &invocation(&original), &context(&original)),
                    Ok(EffectOutcome::Failed(reason.clone()))
                );
                assert_eq!(
                    application.invoke_controlled(
                        &original,
                        &invocation(&original),
                        &context(&original),
                        &signal.observer()
                    ),
                    Ok(ControlledNativeOutcome::Settled(NativeOutcome::Failed(
                        reason
                    )))
                );
            }
            Err(_) => {
                assert_eq!(
                    application.invoke(&original, &invocation(&original), &context(&original)),
                    Err(NativeInvocationError::Uncertain)
                );
                assert_eq!(
                    application.invoke_controlled(
                        &original,
                        &invocation(&original),
                        &context(&original),
                        &signal.observer()
                    ),
                    Err(NativeInvocationError::Uncertain)
                );
            }
            Ok(NativeOutcome::Succeeded(_)) => {
                unreachable!("fixture modes are failure and uncertainty")
            }
        }
        assert_eq!(effect.calls.load(Ordering::Acquire), 2);
    }
    assert_eq!(new.calls.load(Ordering::Acquire), 0);
}

#[derive(Clone)]
struct ControlledEffect {
    direct_calls: Arc<AtomicUsize>,
    controlled_seen: Arc<Mutex<Vec<(InvocationContext, bool)>>>,
    controlled: ControlledNativeOutcome,
}
impl NativeEffect for ControlledEffect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.direct_calls.fetch_add(1, Ordering::AcqRel);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: Value::Null,
            writes: vec![],
        }))
    }
    fn invoke_controlled(
        &self,
        _: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.controlled_seen
            .lock()
            .unwrap()
            .push((context.clone(), observer.cancellation_requested()));
        Ok(self.controlled.clone())
    }
}

#[test]
fn controlled_history_uses_its_override_supplied_observer_and_original_output_guard() {
    let original = definition("v1");
    let new = callback(json!("new-must-not-enter"));
    let old = ControlledEffect {
        direct_calls: Arc::new(AtomicUsize::new(0)),
        controlled_seen: Arc::new(Mutex::new(vec![])),
        controlled: ControlledNativeOutcome::Cancelled,
    };
    let mut historical = registration(original.clone(), &new);
    let binding = historical
        .effects
        .get_mut(&original.batches[0][0].effect_id)
        .unwrap();
    binding.callback = Arc::new(old.clone());
    binding.output_kind = OutputKind::Void;
    let application = assemble(registration(definition("v2"), &new), historical.clone());
    let signal = NativeCancellationSignal::new();
    let observer = signal.observer();
    let call = invocation(&original);
    let context = context(&original);
    assert_eq!(
        application.invoke(&original, &call, &context),
        Ok(EffectOutcome::Succeeded(TaskResult {
            output: Value::Null,
            writes: vec![]
        }))
    );
    assert_eq!(old.direct_calls.load(Ordering::Acquire), 1);
    assert!(old.controlled_seen.lock().unwrap().is_empty());
    assert_eq!(
        application.invoke_controlled(&original, &call, &context, &observer),
        Err(NativeInvocationError::Uncertain)
    );
    signal.commit_cancellation();
    assert_eq!(
        application.invoke_controlled(&original, &call, &context, &observer),
        Ok(ControlledNativeOutcome::Cancelled)
    );
    assert_eq!(
        *old.controlled_seen.lock().unwrap(),
        vec![(context.clone(), false), (context.clone(), true)]
    );
    assert_eq!(old.direct_calls.load(Ordering::Acquire), 1);
    let nonvoid = ControlledEffect {
        direct_calls: Arc::new(AtomicUsize::new(0)),
        controlled_seen: Arc::new(Mutex::new(vec![])),
        controlled: ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(TaskResult {
            output: json!("original-invalid-void"),
            writes: vec![],
        })),
    };
    historical
        .effects
        .get_mut(&original.batches[0][0].effect_id)
        .unwrap()
        .callback = Arc::new(nonvoid.clone());
    let invalid = assemble(registration(definition("v2"), &new), historical);
    assert_eq!(
        invalid.invoke_controlled(&original, &call, &context, &observer),
        Err(NativeInvocationError::InvalidOutput)
    );
    assert_eq!(
        *nonvoid.controlled_seen.lock().unwrap(),
        vec![(context, true)]
    );
    assert_eq!(nonvoid.direct_calls.load(Ordering::Acquire), 0);
    assert_eq!(new.calls.load(Ordering::Acquire), 0);
}

#[test]
fn altered_whole_definitions_and_missing_pairs_refuse_all_selection_before_user_code() {
    let effect = callback(json!("must-not-run"));
    let validation = validator(json!("must-not-validate"));
    let mut original = definition("v1");
    original.batches.push(vec![TaskSpec {
        id: TaskId::new("second-task").unwrap(),
        effect_id: EffectId::new("second-effect").unwrap(),
        recovery: RecoveryPolicy::Idempotent,
        requires_approval: false,
    }]);
    original.max_attempts = 2;
    let mut historical = bound_registration(original.clone(), &effect);
    historical.input_validator = Some(Arc::new(validation.clone()));
    let application = assemble(registration(definition("v2"), &effect), historical);
    let mut changed = Vec::new();
    let mut forged = original.clone();
    forged.max_attempts = 3;
    changed.push(forged);
    let mut forged = original.clone();
    forged.batches[0][0].recovery = RecoveryPolicy::Idempotent;
    changed.push(forged);
    let mut forged = original.clone();
    forged.batches[0][0].requires_approval = true;
    changed.push(forged);
    let mut forged = original.clone();
    forged.batches[0][0].id = TaskId::new("different-task").unwrap();
    changed.push(forged);
    let mut forged = original.clone();
    forged.batches[0][0].effect_id = EffectId::new("different-effect").unwrap();
    changed.push(forged);
    let mut forged = original.clone();
    forged.batches.reverse();
    changed.push(forged);
    let mut forged = original.clone();
    forged.batches = vec![forged.batches.into_iter().flatten().collect()];
    changed.push(forged);
    let mut forged = original.clone();
    forged.route = RouteKey::new("/missing#workflow").unwrap();
    changed.push(forged);
    let mut forged = original.clone();
    forged.revision = ExecutionRevision::new("missing-revision").unwrap();
    changed.push(forged);
    let signal = NativeCancellationSignal::new();
    for forged in changed {
        assert_eq!(application.definition_exact(&forged), None);
        assert_eq!(
            application.output_kind(&forged, &forged.batches[0][0].effect_id),
            None
        );
        assert_eq!(
            application.validate_input(&forged, &validation.accepted),
            Err(ApplicationInputError::DefinitionUnavailable)
        );
        assert_eq!(
            application.invoke(&forged, &invocation(&forged), &context(&forged)),
            Err(NativeInvocationError::DefinitionUnavailable)
        );
        assert_eq!(
            application.invoke_controlled(
                &forged,
                &invocation(&forged),
                &context(&forged),
                &signal.observer()
            ),
            Err(NativeInvocationError::DefinitionUnavailable)
        );
    }
    assert_eq!(effect.calls.load(Ordering::Acquire), 0);
    assert!(validation.seen.lock().unwrap().is_empty());
    assert_eq!(application.definition_exact(&original), Some(&original));
}

#[test]
fn historical_context_owner_and_task_membership_refuse_before_callback() {
    let original = definition("v1");
    let old = callback(json!("v1"));
    let new = callback(json!("v2"));
    let application = assemble(
        registration(definition("v2"), &new),
        registration(original.clone(), &old),
    );
    let call = invocation(&original);
    let valid_context = context(&original);
    let mut mismatches = Vec::new();
    let mut forged = valid_context.clone();
    forged.application_revision = "fabricated-old-assembly".into();
    mismatches.push(forged);
    let mut forged = valid_context.clone();
    forged.execution_revision = ExecutionRevision::new("v2").unwrap();
    mismatches.push(forged);
    let mut forged = valid_context.clone();
    forged.origin.application = "different-app".into();
    mismatches.push(forged);
    let mut forged = valid_context.clone();
    forged.origin.tenant.clear();
    mismatches.push(forged);
    let mut forged = valid_context.clone();
    forged.origin.principal = "invalid\nactor".into();
    mismatches.push(forged);
    let mut forged = valid_context.clone();
    forged.policy_revision.clear();
    mismatches.push(forged);
    let signal = NativeCancellationSignal::new();
    for forged in mismatches {
        assert_eq!(
            application.invoke(&original, &call, &forged),
            Err(NativeInvocationError::ContextMismatch)
        );
        assert_eq!(
            application.invoke_controlled(&original, &call, &forged, &signal.observer()),
            Err(NativeInvocationError::ContextMismatch)
        );
    }
    let mut unowned = call.clone();
    unowned.owner_generation = 0;
    assert_eq!(
        application.invoke(&original, &unowned, &valid_context),
        Err(NativeInvocationError::ContextMismatch)
    );
    assert_eq!(
        application.invoke_controlled(&original, &unowned, &valid_context, &signal.observer()),
        Err(NativeInvocationError::ContextMismatch)
    );
    for task_mismatch in [true, false] {
        let mut foreign = call.clone();
        if task_mismatch {
            foreign.task_id = TaskId::new("missing-task").unwrap();
        } else {
            foreign.effect_id = EffectId::new("missing-effect").unwrap();
        }
        assert_eq!(
            application.invoke(&original, &foreign, &valid_context),
            Err(NativeInvocationError::TaskEffectMismatch)
        );
        assert_eq!(
            application.invoke_controlled(&original, &foreign, &valid_context, &signal.observer()),
            Err(NativeInvocationError::TaskEffectMismatch)
        );
    }
    assert_eq!(old.calls.load(Ordering::Acquire), 0);
    assert_eq!(new.calls.load(Ordering::Acquire), 0);
    assert_eq!(
        application.invoke(&original, &call, &valid_context),
        Ok(EffectOutcome::Succeeded(TaskResult {
            output: json!("v1"),
            writes: vec![]
        }))
    );
    assert_eq!(old.calls.load(Ordering::Acquire), 1);
    assert_eq!(*old.seen.lock().unwrap(), vec![(call, valid_context)]);
}

#[test]
fn historical_program_and_binding_admission_preserve_per_definition_limits() {
    let effect = callback(json!("assembly-never-invokes"));
    let current = registration(definition("v2"), &effect);
    let make = |tasks: usize| Definition {
        route: RouteKey::new("/history#workflow").unwrap(),
        revision: ExecutionRevision::new("v1").unwrap(),
        batches: vec![(0..tasks)
            .map(|i| TaskSpec {
                id: TaskId::new(format!("task-{i}")).unwrap(),
                effect_id: EffectId::new(format!("effect-{i}")).unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            })
            .collect()],
        max_attempts: tasks as u32,
    };
    let valid = make(64);
    let application = assemble(current.clone(), bound_registration(valid.clone(), &effect));
    assert_eq!(application.definition_exact(&valid), Some(&valid));
    for count in [0, 65] {
        assert!(NativeApplication::new_with_history(
            installed_identity(),
            vec![current.clone()],
            vec![bound_registration(make(count), &effect)]
        )
        .is_err());
    }
    let mut invalids = Vec::new();
    for budget in [0, 65] {
        let mut malformed = registration(definition("v1"), &effect);
        malformed.definition.max_attempts = budget;
        invalids.push(malformed);
    }
    let mut missing = registration(definition("v1"), &effect);
    missing.effects.clear();
    invalids.push(missing);
    let mut extra = registration(definition("v1"), &effect);
    extra.effects.insert(
        EffectId::new("extra-effect").unwrap(),
        EffectBinding {
            callback: Arc::new(effect.clone()),
            output_kind: OutputKind::Value,
        },
    );
    invalids.push(extra);
    let mut duplicate = make(2);
    duplicate.batches[0][1].id = duplicate.batches[0][0].id.clone();
    invalids.push(bound_registration(duplicate, &effect));
    for malformed in invalids {
        assert!(NativeApplication::new_with_history(
            installed_identity(),
            vec![current.clone()],
            vec![malformed]
        )
        .is_err());
    }
    assert_eq!(effect.calls.load(Ordering::Acquire), 0);
}
