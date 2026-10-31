use b4_contract::*;
use b4_runtime::permissions::*;
use b4_runtime::*;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

struct Echo {
    revision: ExecutionRevision,
}
impl NativeEffect for Echo {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        assert_eq!(
            context.execution_revision, self.revision,
            "preserve each original callback, including history"
        );
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: invocation.input["output"].clone(),
            writes: vec![],
        }))
    }
}

struct Wrapper {
    inner: Arc<dyn NativeEffect>,
    entered: Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>,
}
impl NativeEffect for Wrapper {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.entered
            .lock()
            .unwrap()
            .push((invocation.clone(), context.clone()));
        self.inner.invoke(invocation, context)
    }

    fn invoke_controlled(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.entered
            .lock()
            .unwrap()
            .push((invocation.clone(), context.clone()));
        if observer.cancellation_requested() {
            Ok(ControlledNativeOutcome::Cancelled)
        } else {
            self.inner.invoke_controlled(invocation, context, observer)
        }
    }
}

struct ExactInput(Value);
impl InputValidator for ExactInput {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        if input == &self.0 {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}

fn definition(route: &str, revision: &str) -> Definition {
    Definition {
        route: RouteKey::new(route).unwrap(),
        revision: ExecutionRevision::new(revision).unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("task").unwrap(),
            effect_id: EffectId::new("effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]],
        max_attempts: 2,
    }
}

fn registration(definition: &Definition, kind: OutputKind) -> RouteRegistration {
    RouteRegistration {
        definition: definition.clone(),
        effects: BTreeMap::from([(
            definition.batches[0][0].effect_id.clone(),
            EffectBinding {
                callback: Arc::new(Echo {
                    revision: definition.revision.clone(),
                }),
                output_kind: kind,
            },
        )]),
        input_validator: Some(Arc::new(ExactInput(json!(definition.revision.as_str())))),
    }
}

fn application() -> (NativeApplication, Vec<(Definition, OutputKind)>) {
    let current = definition("/map#workflow", "v2");
    let history = definition("/map#workflow", "v1");
    let removed = definition("/removed#workflow", "v0");
    let cases = vec![
        (current.clone(), OutputKind::Value),
        (history.clone(), OutputKind::Void),
        (removed.clone(), OutputKind::Value),
    ];
    let app = NativeApplication::new_with_history(
        ApplicationIdentity {
            application: "mapping-app".into(),
            revision: "assembly-v2".into(),
        },
        vec![registration(&current, OutputKind::Value)],
        vec![
            registration(&history, OutputKind::Void),
            registration(&removed, OutputKind::Value),
        ],
    )
    .unwrap();
    (app, cases)
}

fn call(definition: &Definition, output: Value) -> (EffectInvocation, InvocationContext) {
    (
        EffectInvocation {
            execution_id: ExecutionId::new("execution").unwrap(),
            turn_id: TurnId::new("turn").unwrap(),
            owner_generation: 1,
            task_id: definition.batches[0][0].id.clone(),
            effect_id: definition.batches[0][0].effect_id.clone(),
            attempt_id: AttemptId::new("attempt").unwrap(),
            input: Map::from_iter([("output".into(), output)]),
        },
        InvocationContext {
            thread_id: ThreadId::new("thread").unwrap(),
            thread_generation: ThreadGeneration::new("ab".repeat(32)).unwrap(),
            origin: ThreadOrigin {
                application: "mapping-app".into(),
                tenant: "tenant".into(),
                principal: "principal".into(),
            },
            application_revision: "assembly-v2".into(),
            execution_revision: definition.revision.clone(),
            policy_revision: "policy".into(),
        },
    )
}

fn assert_mapping(
    original: &NativeApplication,
    mapped: &NativeApplication,
    cases: &[(Definition, OutputKind)],
) {
    assert_eq!(mapped.identity(), original.identity());
    assert_eq!(
        mapped.definitions().collect::<Vec<_>>(),
        original.definitions().collect::<Vec<_>>()
    );
    assert_eq!(mapped.definitions().count(), 1);
    assert!(mapped.definition(&cases[2].0.route).is_none());
    for (definition, kind) in cases {
        assert_eq!(mapped.definition_exact(definition), Some(definition));
        assert_eq!(
            mapped.definition(&definition.route),
            original.definition(&definition.route)
        );
        assert_eq!(
            mapped.output_kind(definition, &definition.batches[0][0].effect_id),
            Some(*kind)
        );
        assert_eq!(
            mapped.validate_input(definition, &json!(definition.revision.as_str())),
            Ok(())
        );
        assert_eq!(
            mapped.validate_input(definition, &json!("wrong-revision")),
            Err(ApplicationInputError::InvalidInput)
        );
        let mut forged = definition.clone();
        forged.max_attempts += 1;
        assert!(mapped.definition_exact(&forged).is_none());
        assert_eq!(
            mapped.validate_input(&forged, &Value::Null),
            Err(ApplicationInputError::DefinitionUnavailable)
        );
    }
}

#[test]
fn native_mapping_wraps_real_current_and_historical_callbacks_without_changing_selection() {
    let (original, cases) = application();
    let entered = Arc::new(Mutex::new(vec![]));
    let mut wrapped = 0;
    let mapped = original.map_native_callbacks(|inner| {
        wrapped += 1; // A stateful FnMut is part of the public mapping contract.
        Arc::new(Wrapper {
            inner,
            entered: entered.clone(),
        })
    });
    assert_eq!(wrapped, 3);
    assert!(entered.lock().unwrap().is_empty());
    assert_mapping(&original, &mapped, &cases);
    let signal = NativeCancellationSignal::new();
    signal.commit_cancellation();
    for (definition, kind) in &cases {
        let output = if *kind == OutputKind::Void {
            Value::Null
        } else {
            json!(definition.revision.as_str())
        };
        let (invocation, context) = call(definition, output.clone());
        assert_eq!(
            mapped.invoke(definition, &invocation, &context),
            Ok(EffectOutcome::Succeeded(TaskResult {
                output: output.clone(),
                writes: vec![]
            }))
        );
        assert_eq!(
            mapped.invoke_controlled(definition, &invocation, &context, &signal.observer()),
            Ok(ControlledNativeOutcome::Cancelled)
        );
        assert_eq!(
            original.invoke_controlled(definition, &invocation, &context, &signal.observer()),
            Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
                TaskResult {
                    output,
                    writes: vec![]
                }
            )))
        );
    }
    let expected: Vec<_> = cases
        .iter()
        .flat_map(|(definition, kind)| {
            let output = if *kind == OutputKind::Void {
                Value::Null
            } else {
                json!(definition.revision.as_str())
            };
            let entry = call(definition, output);
            [entry.clone(), entry]
        })
        .collect();
    assert_eq!(*entered.lock().unwrap(), expected);
    let (bad_output, context) = call(&cases[1].0, json!("not-void"));
    assert_eq!(
        mapped.invoke(&cases[1].0, &bad_output, &context),
        Err(NativeInvocationError::InvalidOutput)
    );
}

struct Constraint {
    denied: AtomicBool,
    calls: AtomicUsize,
}
impl ToolConstraint for Constraint {
    fn validate_call(&self, call: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(call.binding.application.application, "mapping-app");
        assert_eq!(call.binding.application.revision, "assembly-v2");
        assert_eq!(&call.binding.definition, call.definition);
        assert_eq!(call.binding.effect_id, call.task.spec.effect_id);
        assert_eq!(call.binding.tool_id, StableToolId::new("tool").unwrap());
        assert_eq!(
            call.binding.tool_revision,
            StableToolRevision::new("tool-v3").unwrap()
        );
        assert_eq!(
            call.binding.constraint_revision,
            ToolConstraintRevision::new("constraint-v7").unwrap()
        );
        if self.denied.load(Ordering::SeqCst) {
            Err(ToolConstraintError::Denied)
        } else {
            Ok(())
        }
    }
}

#[test]
fn catalog_mapping_preserves_shared_constraint_and_tool_metadata_and_wraps_history_too() {
    let (application, cases) = application();
    let application = Arc::new(application);
    let current = &cases[0].0;
    let constraint = Arc::new(Constraint {
        denied: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    let catalog = PermissionCatalog::new(
        application.clone(),
        vec![ToolBindingRegistration {
            definition: current.clone(),
            effect_id: current.batches[0][0].effect_id.clone(),
            tool_id: StableToolId::new("tool").unwrap(),
            tool_revision: StableToolRevision::new("tool-v3").unwrap(),
            constraint_revision: ToolConstraintRevision::new("constraint-v7").unwrap(),
            constraint: constraint.clone(),
        }],
    )
    .unwrap();
    let entered = Arc::new(Mutex::new(vec![]));
    let before = Arc::strong_count(&constraint);
    let mut wrapped = 0;
    let mapped = catalog.map_native_callbacks(|inner| {
        wrapped += 1;
        Arc::new(Wrapper {
            inner,
            entered: entered.clone(),
        })
    });
    assert_eq!(wrapped, 3);
    assert_eq!(
        Arc::strong_count(&constraint),
        before + 1,
        "retain the same constraint Arc"
    );
    assert_eq!(
        constraint.calls.load(Ordering::SeqCst),
        0,
        "mapping must not run constraints"
    );
    assert!(entered.lock().unwrap().is_empty());
    assert_mapping(catalog.application(), mapped.application(), &cases);
    let (invocation, context) = call(current, json!("current"));
    let task = TaskRecord {
        spec: current.batches[0][0].clone(),
        status: TaskStatus::Ready,
        input: invocation.input.clone(),
        attempt_id: None,
        result: None,
        failure: None,
        interrupt: None,
        approved: false,
    };
    for candidate in [&catalog, &mapped] {
        assert!(matches!(
            candidate.validate_call(&context, current, &task, PermissionCallPhase::Unclaimed),
            Ok(())
        ));
    }
    constraint.denied.store(true, Ordering::SeqCst);
    for candidate in [&catalog, &mapped] {
        assert!(matches!(
            candidate.validate_call(&context, current, &task, PermissionCallPhase::Unclaimed),
            Err(PermissionError::ConstraintDenied)
        ));
    }
    assert_eq!(constraint.calls.load(Ordering::SeqCst), 4);
    let signal = NativeCancellationSignal::new();
    signal.commit_cancellation();
    for (definition, kind) in &cases {
        let output = if *kind == OutputKind::Void {
            Value::Null
        } else {
            json!("value")
        };
        let (invocation, context) = call(definition, output);
        assert_eq!(
            mapped.application().invoke_controlled(
                definition,
                &invocation,
                &context,
                &signal.observer()
            ),
            Ok(ControlledNativeOutcome::Cancelled)
        );
        assert!(matches!(
            catalog.application().invoke_controlled(
                definition,
                &invocation,
                &context,
                &signal.observer()
            ),
            Ok(ControlledNativeOutcome::Settled(_))
        ));
    }
    assert_eq!(entered.lock().unwrap().len(), 3);
    assert!(
        Arc::ptr_eq(catalog.application(), &application),
        "original catalog remains attached to its original application"
    );
}
