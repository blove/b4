use super::*;
use crate::{EffectInvocation, EffectOutcome, ThreadOrigin};
use b4_contract::*;
use serde_json::{json, Map, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone)]
struct Probe {
    output: Value,
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>,
}
impl Probe {
    fn new(output: Value) -> Self {
        Self {
            output,
            calls: Arc::new(AtomicUsize::new(0)),
            seen: Arc::new(Mutex::new(vec![])),
        }
    }
    fn count(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}
impl NativeEffect for Probe {
    fn invoke(
        &self,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.seen
            .lock()
            .unwrap()
            .push((call.clone(), context.clone()));
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: self.output.clone(),
            writes: vec![],
        }))
    }
}
struct Validator {
    accepted: Value,
    calls: Arc<AtomicUsize>,
}
impl InputValidator for Validator {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if *input == self.accepted {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}
struct Ack {
    direct: Arc<AtomicUsize>,
    controlled: Arc<AtomicUsize>,
}
impl NativeEffect for Ack {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.direct.fetch_add(1, Ordering::AcqRel);
        Ok(NativeOutcome::Failed("old-direct".into()))
    }
    fn invoke_controlled(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
        _: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.controlled.fetch_add(1, Ordering::AcqRel);
        Ok(ControlledNativeOutcome::Cancelled)
    }
}
struct Uncertain;
impl NativeEffect for Uncertain {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        Err(EffectUncertain)
    }
}
fn identity() -> ApplicationIdentity {
    ApplicationIdentity {
        application: "private-history".into(),
        revision: "installed-v2".into(),
    }
}
fn definition(route: &str, revision: &str) -> Definition {
    Definition {
        route: RouteKey::new(route).unwrap(),
        revision: ExecutionRevision::new(revision).unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("task").unwrap(),
            effect_id: EffectId::new("effect").unwrap(),
            requires_approval: false,
            recovery: RecoveryPolicy::Pure,
        }]],
        max_attempts: 1,
    }
}
fn registration(definition: Definition, probe: &Probe) -> RouteRegistration {
    RouteRegistration {
        effects: BTreeMap::from([(
            definition.batches[0][0].effect_id.clone(),
            EffectBinding {
                callback: Arc::new(probe.clone()),
                output_kind: OutputKind::Value,
            },
        )]),
        definition,
        input_validator: None,
    }
}
fn assemble(
    current: Vec<RouteRegistration>,
    historical: Vec<RouteRegistration>,
) -> Result<NativeApplication, ApplicationConfigError> {
    NativeApplication::new_with_history(identity(), current, historical)
}
fn call(definition: &Definition) -> EffectInvocation {
    EffectInvocation {
        execution_id: ExecutionId::new("execution").unwrap(),
        turn_id: TurnId::new("turn").unwrap(),
        owner_generation: 3,
        task_id: definition.batches[0][0].id.clone(),
        effect_id: definition.batches[0][0].effect_id.clone(),
        attempt_id: AttemptId::new("attempt").unwrap(),
        input: Map::new(),
    }
}
fn context(definition: &Definition) -> InvocationContext {
    InvocationContext {
        thread_id: ThreadId::new("thread").unwrap(),
        thread_generation: ThreadGeneration::new("ef".repeat(32)).unwrap(),
        origin: ThreadOrigin {
            application: "private-history".into(),
            tenant: "tenant".into(),
            principal: "actor".into(),
        },
        application_revision: "installed-v2".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: "policy".into(),
    }
}
fn fixture() -> (Definition, Definition, Probe, Probe) {
    (
        definition("/work#workflow", "old-v1"),
        definition("/work#workflow", "current-v2"),
        Probe::new(json!("old")),
        Probe::new(json!("current")),
    )
}
fn expected(probe: &Probe) -> EffectOutcome {
    EffectOutcome::Succeeded(TaskResult {
        output: probe.output.clone(),
        writes: vec![],
    })
}

#[test]
fn old_constructor_and_current_lookup_keep_existing_semantics() {
    let (_, current, _, probe) = fixture();
    let app =
        NativeApplication::new(identity(), vec![registration(current.clone(), &probe)]).unwrap();
    assert_eq!(app.definition_exact(&current), Some(&current));
    assert_eq!(app.definition(&current.route), Some(&current));
    assert_eq!(
        app.definitions().cloned().collect::<Vec<_>>(),
        vec![current.clone()]
    );
    assert_eq!(
        app.invoke(&current, &call(&current), &context(&current)),
        Ok(expected(&probe))
    );
    let mut changed = current.clone();
    changed.max_attempts = 2;
    assert!(app.definition_exact(&changed).is_none());
    assert_eq!(
        app.invoke(&changed, &call(&changed), &context(&changed)),
        Err(NativeInvocationError::DefinitionUnavailable)
    );
    assert_eq!(probe.count(), 1);
}
#[test]
fn history_lookup_keeps_current_and_removed_routes_explicit() {
    let (old, current, old_probe, current_probe) = fixture();
    let removed = definition("/removed#workflow", "removed-v1");
    let app = assemble(
        vec![registration(current.clone(), &current_probe)],
        vec![
            registration(old.clone(), &old_probe),
            registration(removed.clone(), &old_probe),
        ],
    )
    .unwrap();
    assert_eq!(app.definition(&current.route), Some(&current));
    assert_eq!(app.definitions().count(), 1);
    assert!(app.definition(&removed.route).is_none());
    assert_eq!(app.definition_exact(&old), Some(&old));
    assert_eq!(app.definition_exact(&removed), Some(&removed));
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn combined_limit_accepts_and_selects_exactly_sixty_four() {
    let probe = Probe::new(Value::Null);
    let current = definition("/work#workflow", "current");
    let historical: Vec<_> = (0..63)
        .map(|n| registration(definition("/work#workflow", &format!("old-{n}")), &probe))
        .collect();
    let recorded: Vec<_> = historical.iter().map(|r| r.definition.clone()).collect();
    let app = assemble(vec![registration(current.clone(), &probe)], historical).unwrap();
    assert_eq!(app.definitions().count(), 1);
    assert_eq!(app.definition_exact(&current), Some(&current));
    for definition in &recorded {
        assert_eq!(app.definition_exact(definition), Some(definition));
    }
    assert_eq!(probe.count(), 0);
}
#[test]
fn combined_limit_refuses_sixty_five_before_callbacks() {
    let probe = Probe::new(Value::Null);
    let current = definition("/work#workflow", "current");
    let historical = (0..64)
        .map(|n| registration(definition("/work#workflow", &format!("old-{n}")), &probe))
        .collect();
    assert_eq!(
        assemble(vec![registration(current, &probe)], historical).unwrap_err(),
        ApplicationConfigError("invalid_application_route_limit")
    );
    assert_eq!(probe.count(), 0);
}
#[test]
fn current_bounds_and_old_identity_error_order_are_preserved() {
    let probe = Probe::new(Value::Null);
    let current: Vec<_> = (0..64)
        .map(|n| {
            registration(
                definition(&format!("/work-{n}#workflow"), "current"),
                &probe,
            )
        })
        .collect();
    assert_eq!(
        assemble(current.clone(), vec![])
            .unwrap()
            .definitions()
            .count(),
        64
    );
    let mut over = current;
    over.push(registration(
        definition("/extra#workflow", "current"),
        &probe,
    ));
    assert_eq!(
        assemble(over, vec![]).unwrap_err(),
        ApplicationConfigError("invalid_application_route_limit")
    );
    assert_eq!(
        assemble(
            vec![],
            vec![registration(definition("/work#workflow", "old"), &probe)]
        )
        .unwrap_err(),
        ApplicationConfigError("invalid_application_route_limit")
    );
    let invalid = ApplicationIdentity {
        application: String::new(),
        revision: String::new(),
    };
    assert_eq!(
        NativeApplication::new(invalid, vec![]).unwrap_err(),
        ApplicationConfigError("invalid_application_identity")
    );
    assert_eq!(probe.count(), 0);
}
#[test]
fn duplicate_current_route_keeps_original_static_reason() {
    let (old, current, old_probe, current_probe) = fixture();
    assert_eq!(
        assemble(
            vec![
                registration(old, &old_probe),
                registration(current, &current_probe)
            ],
            vec![]
        )
        .unwrap_err(),
        ApplicationConfigError("duplicate_application_route")
    );
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn history_pairs_cannot_collide_with_current_even_when_content_changes() {
    let (_, current, _, probe) = fixture();
    for changed in [false, true] {
        let mut duplicate = current.clone();
        if changed {
            duplicate.max_attempts = 2;
        }
        assert_eq!(
            assemble(
                vec![registration(current.clone(), &probe)],
                vec![registration(duplicate, &probe)]
            )
            .unwrap_err(),
            ApplicationConfigError("duplicate_application_revision")
        );
    }
    assert_eq!(probe.count(), 0);
}
#[test]
fn duplicate_history_pairs_refuse_equal_or_changed_content() {
    let (old, current, old_probe, current_probe) = fixture();
    for changed in [false, true] {
        let mut duplicate = old.clone();
        if changed {
            duplicate.max_attempts = 2;
        }
        assert_eq!(
            assemble(
                vec![registration(current.clone(), &current_probe)],
                vec![
                    registration(old.clone(), &old_probe),
                    registration(duplicate, &old_probe)
                ]
            )
            .unwrap_err(),
            ApplicationConfigError("duplicate_application_revision")
        );
    }
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn distinct_history_revisions_bind_their_own_identically_named_effects() {
    let (old, current, old_probe, current_probe) = fixture();
    let earlier = definition("/work#workflow", "earlier-v0");
    let earlier_probe = Probe::new(json!("earlier"));
    let app = assemble(
        vec![registration(current.clone(), &current_probe)],
        vec![
            registration(old.clone(), &old_probe),
            registration(earlier.clone(), &earlier_probe),
        ],
    )
    .unwrap();
    for (definition, probe) in [
        (&old, &old_probe),
        (&earlier, &earlier_probe),
        (&current, &current_probe),
    ] {
        let invocation = call(definition);
        let ctx = context(definition);
        assert_eq!(
            app.invoke(definition, &invocation, &ctx),
            Ok(expected(probe))
        );
        assert_eq!(*probe.seen.lock().unwrap(), vec![(invocation, ctx)]);
        assert_eq!(probe.count(), 1);
    }
    assert_eq!(app.definition(&old.route), Some(&current));
}
#[test]
fn history_approval_is_refused_in_current_or_future_batches() {
    let (old, current, old_probe, current_probe) = fixture();
    for future in [false, true] {
        let mut forbidden = old.clone();
        if future {
            let mut task = forbidden.batches[0][0].clone();
            task.id = TaskId::new("future-task").unwrap();
            task.effect_id = EffectId::new("future-effect").unwrap();
            task.requires_approval = true;
            forbidden.batches.push(vec![task]);
            forbidden.max_attempts = 2;
        } else {
            forbidden.batches[0][0].requires_approval = true;
        }
        let mut historical = registration(forbidden.clone(), &old_probe);
        if future {
            historical.effects.insert(
                EffectId::new("future-effect").unwrap(),
                EffectBinding {
                    callback: Arc::new(old_probe.clone()),
                    output_kind: OutputKind::Value,
                },
            );
        }
        let ordinary = NativeApplication::new(identity(), vec![historical.clone()]).unwrap();
        assert_eq!(ordinary.definition_exact(&forbidden), Some(&forbidden));
        assert_eq!(
            assemble(
                vec![registration(current.clone(), &current_probe)],
                vec![historical]
            )
            .unwrap_err(),
            ApplicationConfigError("historical_approval_unsupported")
        );
    }
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn current_approval_registration_remains_supported() {
    let (old, mut current, old_probe, current_probe) = fixture();
    current.batches[0][0].requires_approval = true;
    let app = assemble(
        vec![registration(current.clone(), &current_probe)],
        vec![registration(old, &old_probe)],
    )
    .unwrap();
    assert_eq!(app.definition(&current.route), Some(&current));
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn each_history_program_limit_is_validated() {
    let (old, current, old_probe, current_probe) = fixture();
    let mut attempts = old.clone();
    attempts.max_attempts = 65;
    let mut batches = old.clone();
    batches.batches = vec![vec![old.batches[0][0].clone()]; 65];
    let mut tasks = old.clone();
    tasks.batches = vec![(0..65)
        .map(|n| {
            let mut task = old.batches[0][0].clone();
            task.id = TaskId::new(format!("task-{n}")).unwrap();
            task
        })
        .collect()];
    tasks.max_attempts = 65;
    for invalid in [attempts, batches, tasks] {
        assert_eq!(
            assemble(
                vec![registration(current.clone(), &current_probe)],
                vec![registration(invalid, &old_probe)]
            )
            .unwrap_err(),
            ApplicationConfigError("invalid_application_program_limit")
        );
    }
    let mut bindings = registration(old, &old_probe);
    for n in 0..64 {
        bindings.effects.insert(
            EffectId::new(format!("extra-{n}")).unwrap(),
            EffectBinding {
                callback: Arc::new(old_probe.clone()),
                output_kind: OutputKind::Value,
            },
        );
    }
    assert_eq!(
        assemble(vec![registration(current, &current_probe)], vec![bindings]).unwrap_err(),
        ApplicationConfigError("invalid_application_program_limit")
    );
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn history_kernel_shape_and_binding_coverage_are_required() {
    let (old, current, old_probe, current_probe) = fixture();
    let mut invalid = old.clone();
    let duplicate = invalid.batches[0][0].clone();
    invalid.batches[0].push(duplicate);
    invalid.max_attempts = 2;
    assert_eq!(
        assemble(
            vec![registration(current.clone(), &current_probe)],
            vec![registration(invalid, &old_probe)]
        )
        .unwrap_err(),
        ApplicationConfigError("invalid_application_definition")
    );
    let mut missing = registration(old, &old_probe);
    missing.effects.clear();
    assert_eq!(
        assemble(vec![registration(current, &current_probe)], vec![missing]).unwrap_err(),
        ApplicationConfigError("application_effect_coverage")
    );
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn retained_input_validator_and_output_kind_are_version_specific() {
    let (old, current, old_probe, current_probe) = fixture();
    let old_calls = Arc::new(AtomicUsize::new(0));
    let current_calls = Arc::new(AtomicUsize::new(0));
    let mut old_reg = registration(old.clone(), &old_probe);
    old_reg
        .effects
        .get_mut(&old.batches[0][0].effect_id)
        .unwrap()
        .output_kind = OutputKind::Void;
    old_reg.input_validator = Some(Arc::new(Validator {
        accepted: json!("old-input"),
        calls: old_calls.clone(),
    }));
    let mut current_reg = registration(current.clone(), &current_probe);
    current_reg.input_validator = Some(Arc::new(Validator {
        accepted: json!("new-input"),
        calls: current_calls.clone(),
    }));
    let app = assemble(vec![current_reg], vec![old_reg]).unwrap();
    assert_eq!(app.validate_input(&old, &json!("old-input")), Ok(()));
    assert_eq!(
        app.validate_input(&old, &json!("new-input")),
        Err(ApplicationInputError::InvalidInput)
    );
    assert_eq!(app.validate_input(&current, &json!("new-input")), Ok(()));
    assert_eq!(
        app.validate_input(&current, &json!("old-input")),
        Err(ApplicationInputError::InvalidInput)
    );
    assert_eq!(old_calls.load(Ordering::Acquire), 2);
    assert_eq!(current_calls.load(Ordering::Acquire), 2);
    assert_eq!(
        app.output_kind(&old, &old.batches[0][0].effect_id),
        Some(OutputKind::Void)
    );
    assert_eq!(
        app.output_kind(&current, &current.batches[0][0].effect_id),
        Some(OutputKind::Value)
    );
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn retained_context_and_task_membership_guards_remain_exact() {
    let (old, current, old_probe, current_probe) = fixture();
    let app = assemble(
        vec![registration(current.clone(), &current_probe)],
        vec![registration(old.clone(), &old_probe)],
    )
    .unwrap();
    let ctx = context(&old);
    let invocation = call(&old);
    let mut wrong_assembly = ctx.clone();
    wrong_assembly.application_revision = "invented-old-assembly".into();
    let mut wrong_execution = ctx.clone();
    wrong_execution.execution_revision = current.revision;
    let mut wrong_actor = ctx.clone();
    wrong_actor.origin.application = "foreign".into();
    for invalid in [wrong_assembly, wrong_execution, wrong_actor] {
        assert_eq!(
            app.invoke(&old, &invocation, &invalid),
            Err(NativeInvocationError::ContextMismatch)
        );
    }
    let mut zero = invocation.clone();
    zero.owner_generation = 0;
    assert_eq!(
        app.invoke(&old, &zero, &ctx),
        Err(NativeInvocationError::ContextMismatch)
    );
    let mut wrong_task = invocation.clone();
    wrong_task.task_id = TaskId::new("other-task").unwrap();
    assert_eq!(
        app.invoke(&old, &wrong_task, &ctx),
        Err(NativeInvocationError::TaskEffectMismatch)
    );
    let mut wrong_effect = invocation;
    wrong_effect.effect_id = EffectId::new("other-effect").unwrap();
    assert_eq!(
        app.invoke(&old, &wrong_effect, &ctx),
        Err(NativeInvocationError::TaskEffectMismatch)
    );
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn altered_or_missing_history_never_falls_back_to_current() {
    let (old, current, old_probe, current_probe) = fixture();
    let app = assemble(
        vec![registration(current.clone(), &current_probe)],
        vec![registration(old.clone(), &old_probe)],
    )
    .unwrap();
    assert_eq!(app.definition_exact(&old), Some(&old));
    let mut altered = old.clone();
    altered.max_attempts = 2;
    let missing = definition("/work#workflow", "missing-v9");
    for unsupported in [altered, missing] {
        assert!(app.definition_exact(&unsupported).is_none());
        assert_eq!(
            app.validate_input(&unsupported, &Value::Null),
            Err(ApplicationInputError::DefinitionUnavailable)
        );
        assert_eq!(
            app.output_kind(&unsupported, &unsupported.batches[0][0].effect_id),
            None
        );
        assert_eq!(
            app.invoke(&unsupported, &call(&unsupported), &context(&unsupported)),
            Err(NativeInvocationError::DefinitionUnavailable)
        );
        assert_eq!(
            app.invoke_controlled(
                &unsupported,
                &call(&unsupported),
                &context(&unsupported),
                &NativeCancellationSignal::new().observer()
            ),
            Err(NativeInvocationError::DefinitionUnavailable)
        );
    }
    assert_eq!(old_probe.count() + current_probe.count(), 0);
}
#[test]
fn retained_controlled_default_and_ack_checks_preserve_old_entries() {
    let (old, current, old_probe, current_probe) = fixture();
    let app = assemble(
        vec![registration(current.clone(), &current_probe)],
        vec![registration(old.clone(), &old_probe)],
    )
    .unwrap();
    assert_eq!(
        app.invoke_controlled(
            &old,
            &call(&old),
            &context(&old),
            &NativeCancellationSignal::new().observer()
        ),
        Ok(ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(
            TaskResult {
                output: old_probe.output.clone(),
                writes: vec![]
            }
        )))
    );
    assert_eq!(old_probe.count(), 1);
    assert_eq!(current_probe.count(), 0);
    let direct = Arc::new(AtomicUsize::new(0));
    let controlled = Arc::new(AtomicUsize::new(0));
    let mut ack = registration(old.clone(), &old_probe);
    ack.effects
        .get_mut(&old.batches[0][0].effect_id)
        .unwrap()
        .callback = Arc::new(Ack {
        direct: direct.clone(),
        controlled: controlled.clone(),
    });
    let app = assemble(vec![registration(current, &current_probe)], vec![ack]).unwrap();
    let signal = NativeCancellationSignal::new();
    assert_eq!(
        app.invoke(&old, &call(&old), &context(&old)),
        Ok(EffectOutcome::Failed("old-direct".into()))
    );
    assert_eq!(
        app.invoke_controlled(&old, &call(&old), &context(&old), &signal.observer()),
        Err(NativeInvocationError::Uncertain)
    );
    signal.commit_cancellation();
    assert_eq!(
        app.invoke_controlled(&old, &call(&old), &context(&old), &signal.observer()),
        Ok(ControlledNativeOutcome::Cancelled)
    );
    assert_eq!(direct.load(Ordering::Acquire), 1);
    assert_eq!(controlled.load(Ordering::Acquire), 2);
}
#[test]
fn retained_void_output_and_uncertain_callback_keep_static_checks() {
    let (old, current, old_probe, current_probe) = fixture();
    let mut invalid = registration(old.clone(), &old_probe);
    invalid
        .effects
        .get_mut(&old.batches[0][0].effect_id)
        .unwrap()
        .output_kind = OutputKind::Void;
    let app = assemble(
        vec![registration(current.clone(), &current_probe)],
        vec![invalid],
    )
    .unwrap();
    assert_eq!(
        app.invoke(&old, &call(&old), &context(&old)),
        Err(NativeInvocationError::InvalidOutput)
    );
    assert_eq!(
        app.invoke_controlled(
            &old,
            &call(&old),
            &context(&old),
            &NativeCancellationSignal::new().observer()
        ),
        Err(NativeInvocationError::InvalidOutput)
    );
    assert_eq!(old_probe.count(), 2);
    assert_eq!(current_probe.count(), 0);
    let mut uncertain = registration(old.clone(), &old_probe);
    uncertain
        .effects
        .get_mut(&old.batches[0][0].effect_id)
        .unwrap()
        .callback = Arc::new(Uncertain);
    let app = assemble(vec![registration(current, &current_probe)], vec![uncertain]).unwrap();
    assert_eq!(
        app.invoke(&old, &call(&old), &context(&old)),
        Err(NativeInvocationError::Uncertain)
    );
    assert_eq!(
        app.invoke_controlled(
            &old,
            &call(&old),
            &context(&old),
            &NativeCancellationSignal::new().observer()
        ),
        Err(NativeInvocationError::Uncertain)
    );
}
