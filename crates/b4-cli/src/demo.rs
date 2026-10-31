use crate::config::LauncherError;
use b4_contract::{
    Definition, EffectId, ExecutionRevision, RecoveryPolicy, RouteKey, StateWrite, TaskId,
    TaskResult, TaskSpec,
};
use b4_http::{ApprovalHostConfig, HostLimits};
use b4_runtime::{
    ApplicationIdentity, EffectBinding, EffectInvocation, EffectUncertain, InputValidator,
    InvalidApplicationInput, InvocationContext, NativeApplication, NativeEffect, NativeOutcome,
    OutputKind, RouteRegistration,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(crate) struct AppAssembly {
    pub(crate) application: Arc<NativeApplication>,
    pub(crate) approval: ApprovalHostConfig,
    pub(crate) limits: HostLimits,
}

pub(crate) fn assemble() -> Result<AppAssembly, LauncherError> {
    let invalid = LauncherError::InvalidApplication;
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "native-demo".into(),
                revision: "native-demo-v1".into(),
            },
            vec![
                registration(
                    "/native#workflow",
                    "native-parallel-v1",
                    &[("first", false), ("second", false)],
                )?,
                registration(
                    "/approve#workflow",
                    "native-approval-v1",
                    &[("prepare", false), ("release", true)],
                )?,
            ],
        )
        .map_err(|_| invalid)?,
    );
    let approval = ApprovalHostConfig::new(
        "native-demo-policy-v1".into(),
        BTreeMap::from([(
            RouteKey::new("/approve#workflow").map_err(|_| invalid)?,
            BTreeMap::from([(
                TaskId::new("release").map_err(|_| invalid)?,
                json!({"kind":"approval","message":"Approve the local native release?"}),
            )]),
        )]),
    );
    Ok(AppAssembly {
        application,
        approval,
        limits: HostLimits::default(),
    })
}

struct ObjectInput;
impl InputValidator for ObjectInput {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput> {
        if input.is_null() || input.is_object() {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}

struct LocalEffect;
impl NativeEffect for LocalEffect {
    fn invoke(
        &self,
        call: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        let task = call.task_id.as_str();
        let (output, item) = match task {
            "first" | "second" => {
                // Completion may overlap; the immutable definition fixes write order.
                std::thread::sleep(Duration::from_millis(if task == "first" {
                    120
                } else {
                    60
                }));
                (
                    json!({"task":task,"input":call.input.get("input").cloned().unwrap_or(Value::Null)}),
                    task,
                )
            }
            "prepare" => (json!({"prepared":true}), "prepared"),
            "release" => (json!({"released":true}), "released"),
            _ => return Err(EffectUncertain),
        };
        Ok(NativeOutcome::Succeeded(TaskResult {
            output,
            writes: vec![StateWrite::Append {
                field: "items".into(),
                items: vec![json!(item)],
            }],
        }))
    }
}

fn registration(
    route: &str,
    revision: &str,
    tasks: &[(&str, bool)],
) -> Result<RouteRegistration, LauncherError> {
    let invalid = LauncherError::InvalidApplication;
    let specs = tasks
        .iter()
        .map(|(id, required)| {
            Ok(TaskSpec {
                id: TaskId::new(*id).map_err(|_| invalid)?,
                effect_id: EffectId::new(*id).map_err(|_| invalid)?,
                requires_approval: *required,
                recovery: RecoveryPolicy::Pure,
            })
        })
        .collect::<Result<Vec<_>, LauncherError>>()?;
    let effects = specs
        .iter()
        .map(|task| {
            (
                task.effect_id.clone(),
                EffectBinding {
                    callback: Arc::new(LocalEffect) as Arc<dyn NativeEffect>,
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    Ok(RouteRegistration {
        definition: Definition {
            route: RouteKey::new(route).map_err(|_| invalid)?,
            revision: ExecutionRevision::new(revision).map_err(|_| invalid)?,
            max_attempts: u32::try_from(specs.len()).map_err(|_| invalid)?,
            batches: vec![specs],
        },
        effects,
        input_validator: Some(Arc::new(ObjectInput)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use b4_contract::*;
    use b4_runtime::{
        approval::ApprovalServiceConfig, ApplicationInputError, EffectInvocation, EffectOutcome,
        InvocationContext, ThreadOrigin,
    };
    use serde_json::{json, Map, Value};

    #[test]
    fn fixed_application_contains_only_reviewed_native_and_approval_definitions() {
        let app = assemble().expect("fixed application");
        assert_eq!(app.application.identity().application, "native-demo");
        assert_eq!(app.application.identity().revision, "native-demo-v1");
        let definitions: Vec<_> = app.application.definitions().collect();
        assert_eq!(definitions.len(), 2);
        let native = app
            .application
            .definition(&RouteKey::new("/native#workflow").unwrap())
            .unwrap();
        assert_eq!(native.revision.as_str(), "native-parallel-v1");
        assert_eq!(native.max_attempts, 2);
        assert_eq!(native.batches.len(), 1);
        assert_eq!(
            native.batches[0]
                .iter()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert!(native.batches[0]
            .iter()
            .all(|task| !task.requires_approval && task.recovery == RecoveryPolicy::Pure));
        let approval = app
            .application
            .definition(&RouteKey::new("/approve#workflow").unwrap())
            .unwrap();
        assert_eq!(approval.revision.as_str(), "native-approval-v1");
        assert_eq!(
            approval.batches[0]
                .iter()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            ["prepare", "release"]
        );
        assert!(!approval.batches[0][0].requires_approval);
        assert!(approval.batches[0][1].requires_approval);
        assert_eq!(
            app.approval.service_config,
            ApprovalServiceConfig::RequiredDurable
        );
        assert_eq!(app.approval.policy_revision, "native-demo-policy-v1");
        assert_eq!(app.approval.ttl_ms, 300_000);
        assert_eq!(app.approval.prompts.len(), 1);
        assert_eq!(app.approval.prompts[&approval.route].len(), 1);
        assert_eq!(
            app.approval.prompts[&approval.route][&TaskId::new("release").unwrap()],
            json!({"kind":"approval","message":"Approve the local native release?"})
        );
        assert_eq!(app.limits.max_active_runs, 8);
        assert_eq!(app.limits.max_concurrent_tasks, 8);
        assert_eq!(app.limits.max_tasks_per_execution, 2);
    }

    #[test]
    fn every_demo_route_accepts_null_or_object_input_and_rejects_other_shapes() {
        let app = assemble().expect("fixed application");
        for definition in app.application.definitions() {
            for value in [
                Value::Null,
                json!({}),
                json!({"authority":"no derived authority"}),
            ] {
                assert_eq!(app.application.validate_input(definition, &value), Ok(()));
            }
            for value in [json!([]), json!(true), json!(1), json!("input")] {
                assert_eq!(
                    app.application.validate_input(definition, &value),
                    Err(ApplicationInputError::InvalidInput)
                );
            }
        }
    }

    #[test]
    fn native_callbacks_return_finite_reviewed_values_and_ordered_item_writes() {
        let app = assemble().expect("fixed application");
        for definition in app.application.definitions() {
            let context = InvocationContext {
                thread_id: ThreadId::new("thread").unwrap(),
                thread_generation: ThreadGeneration::new("ab".repeat(32)).unwrap(),
                origin: ThreadOrigin {
                    application: "native-demo".into(),
                    tenant: "local".into(),
                    principal: "owner".into(),
                },
                application_revision: "native-demo-v1".into(),
                execution_revision: definition.revision.clone(),
                policy_revision: "native-demo-policy-v1".into(),
            };
            for task in &definition.batches[0] {
                let call = EffectInvocation {
                    execution_id: ExecutionId::new("execution").unwrap(),
                    turn_id: TurnId::new("turn").unwrap(),
                    owner_generation: 1,
                    task_id: task.id.clone(),
                    effect_id: task.effect_id.clone(),
                    attempt_id: AttemptId::new("attempt").unwrap(),
                    input: Map::from_iter([("input".into(), json!({"value":7}))]),
                };
                let result = app.application.invoke(definition, &call, &context).unwrap();
                let EffectOutcome::Succeeded(result) = result else {
                    panic!("unexpected native outcome");
                };
                let (output, item) = match task.id.as_str() {
                    "first" => (json!({"task":"first","input":{"value":7}}), "first"),
                    "second" => (json!({"task":"second","input":{"value":7}}), "second"),
                    "prepare" => (json!({"prepared":true}), "prepared"),
                    "release" => (json!({"released":true}), "released"),
                    _ => panic!("unexpected task"),
                };
                assert_eq!(result.output, output);
                assert_eq!(
                    result.writes,
                    [StateWrite::Append {
                        field: "items".into(),
                        items: vec![json!(item)]
                    }]
                );
            }
        }
    }
}
