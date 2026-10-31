use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::approval::*;
use b4_runtime::permissions::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::collections::BTreeMap;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-catalog-fixture-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
pub struct AllowAuthority;
impl ApprovalAuthority for AllowAuthority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}
pub struct AllowConstraint;
impl ToolConstraint for AllowConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
struct CountCalls(Arc<AtomicUsize>);
impl NativeEffect for CountCalls {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"fixture": true}),
            writes: vec![],
        }))
    }
}
fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}

pub struct Fixture {
    directory: Directory,
    pub store: Arc<SqliteStore>,
    pub application: Arc<NativeApplication>,
    pub authority: Arc<AllowAuthority>,
    pub definition: Definition,
    pub definitions: Vec<Definition>,
    pub historical: Option<Definition>,
    pub thread: ThreadId,
    pub execution: ExecutionId,
    pub task: TaskId,
    pub effect: EffectId,
    pub context: ApprovalContext,
    calls: Arc<AtomicUsize>,
}
impl Fixture {
    pub fn new(requires_approval: bool) -> Self {
        Self::build(requires_approval, false, 0)
    }
    pub fn with_history() -> Self {
        Self::build(false, true, 0)
    }
    pub fn with_binding_count(total: usize) -> Self {
        assert!((2..=65).contains(&total));
        Self::build(false, false, total - 1)
    }
    fn build(requires_approval: bool, include_history: bool, extra_bindings: usize) -> Self {
        let directory = Directory::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let task = TaskId::new("catalog-task").unwrap();
        let effect = EffectId::new("catalog-effect").unwrap();
        let definition = Definition {
            route: RouteKey::new("/catalog-fixture#agent").unwrap(),
            revision: ExecutionRevision::new("current-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task.clone(),
                effect_id: effect.clone(),
                recovery: RecoveryPolicy::Pure,
                requires_approval,
            }]],
            max_attempts: 3,
        };
        let context = ApprovalContext {
            actor: ThreadOrigin {
                application: "catalog-app".into(),
                tenant: "tenant".into(),
                principal: "alice".into(),
            },
            policy_revision: "policy-1".into(),
        };
        let native_registration = |selected: Definition| {
            let effects = selected
                .batches
                .iter()
                .flatten()
                .map(|spec| {
                    (
                        spec.effect_id.clone(),
                        EffectBinding {
                            callback: Arc::new(CountCalls(calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            RouteRegistration {
                definition: selected,
                effects,
                input_validator: None,
            }
        };
        let mut definitions = vec![definition.clone()];
        if extra_bindings > 0 {
            let tasks = (0..extra_bindings)
                .map(|index| TaskSpec {
                    id: TaskId::new(format!("extra-task-{index}")).unwrap(),
                    effect_id: EffectId::new(format!("extra-effect-{index}")).unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: false,
                })
                .collect();
            definitions.push(Definition {
                route: RouteKey::new("/catalog-extra#agent").unwrap(),
                revision: ExecutionRevision::new("current-1").unwrap(),
                batches: vec![tasks],
                max_attempts: u32::try_from(extra_bindings).unwrap(),
            });
        }
        let identity = ApplicationIdentity {
            application: context.actor.application.clone(),
            revision: "application-1".into(),
        };
        let (application, historical) = if include_history {
            let mut historical = definition.clone();
            historical.revision = ExecutionRevision::new("historical-1").unwrap();
            let app = NativeApplication::new_with_history(
                identity,
                vec![native_registration(definition.clone())],
                vec![native_registration(historical.clone())],
            )
            .unwrap();
            (Arc::new(app), Some(historical))
        } else {
            (
                Arc::new(
                    NativeApplication::new(
                        identity,
                        definitions
                            .iter()
                            .cloned()
                            .map(native_registration)
                            .collect(),
                    )
                    .unwrap(),
                ),
                None,
            )
        };
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let thread = ThreadId::new("catalog-thread").unwrap();
        let execution = ExecutionId::new("catalog-execution").unwrap();
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(context.actor.clone()),
            })
            .unwrap();
        let selected = application.definition(&definition.route).unwrap().clone();
        assert_eq!(selected, definition);
        let initial = new_execution(
            execution.clone(),
            TurnId::new("original-turn").unwrap(),
            store.owner_generation(),
            selected,
            Map::from_iter([("input".into(), json!({"fixture": true}))]),
            facts(100),
        )
        .unwrap();
        let admitted = store.admit(&thread, initial).unwrap();
        let running = store
            .commit(
                &execution,
                (&admitted.snapshot).into(),
                transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
            )
            .unwrap();
        assert_eq!(running.snapshot.status, ExecutionStatus::Running);
        assert_eq!(running.snapshot.tasks[0].status, TaskStatus::Ready);
        Self {
            directory,
            store,
            application,
            authority: Arc::new(AllowAuthority),
            definition,
            definitions,
            historical,
            thread,
            execution,
            task,
            effect,
            context,
            calls,
        }
    }
    pub fn registration(&self, constraint: Arc<dyn ToolConstraint>) -> ToolBindingRegistration {
        ToolBindingRegistration {
            definition: self.definition.clone(),
            effect_id: self.effect.clone(),
            tool_id: StableToolId::new("catalog-tool").unwrap(),
            tool_revision: StableToolRevision::new("tool-1").unwrap(),
            constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
            constraint,
        }
    }
    pub fn registrations(
        &self,
        constraint: Arc<dyn ToolConstraint>,
    ) -> Vec<ToolBindingRegistration> {
        let mut entries = Vec::new();
        for definition in &self.definitions {
            for spec in definition.batches.iter().flatten() {
                entries.push(ToolBindingRegistration {
                    definition: definition.clone(),
                    effect_id: spec.effect_id.clone(),
                    tool_id: StableToolId::new(spec.effect_id.as_str()).unwrap(),
                    tool_revision: StableToolRevision::new("tool-1").unwrap(),
                    constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                    constraint: constraint.clone(),
                });
            }
        }
        entries
    }
    pub fn fence(&self) -> ApprovalFence {
        let view = self.store.thread_view(&self.thread).unwrap().unwrap();
        ApprovalFence {
            execution: (&view.execution.unwrap().snapshot).into(),
            resource_revision: view.thread.resource_revision,
        }
    }
    pub fn once_request(&self) -> ApprovalAcceptanceRequest {
        let service = ApprovalService::new(self.store.clone(), self.authority.clone()).unwrap();
        let interrupt = InterruptId::new("catalog-interrupt").unwrap();
        let prompt = json!({"question": "Run fixture?"});
        let parked = service
            .park(
                &self.context,
                &self.execution,
                self.fence(),
                self.task.clone(),
                interrupt.clone(),
                prompt.clone(),
                1_000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(&self.context, &self.execution, self.fence())
            .unwrap();
        let view = self.store.approval_view(&self.execution).unwrap().unwrap();
        assert_eq!(parked.snapshot.status, ExecutionStatus::WaitingForApproval);
        assert_eq!(pending.len(), 1);
        assert_eq!(view.grants.len(), 1);
        let grant = &view.grants[0];
        assert_eq!(grant.thread_id, self.thread);
        assert_eq!(grant.thread_generation, view.thread.generation);
        assert_eq!(grant.execution_id, self.execution);
        assert_eq!(grant.origin, self.context.actor);
        assert_eq!(grant.policy_revision, self.context.policy_revision);
        assert_eq!(grant.route, self.definition.route);
        assert_eq!(grant.definition_revision, self.definition.revision);
        assert_eq!(grant.task_id, self.task);
        assert_eq!(grant.effect_id, self.effect);
        assert_eq!(grant.interrupt_id, interrupt);
        assert_eq!(grant.task_parking_revision, parked.snapshot.revision);
        assert_eq!(grant.prompt, prompt);
        assert!(
            grant.token == pending[0].token,
            "real grant token must match"
        );
        assert!(
            grant.verifier == token_verifier(&pending[0].token),
            "real grant verifier must match"
        );
        assert!(
            grant.resume_key == pending[0].resume_key,
            "real pending resume key must match"
        );
        assert_eq!(
            grant.proposal_digest,
            proposal_digest(&parked.snapshot.tasks[0], &prompt).unwrap()
        );
        assert_eq!(grant.issued_at_ms, 102);
        assert_eq!(grant.expires_at_ms, 1_102);
        assert!(!grant.consumed);
        ApprovalAcceptanceRequest {
            context: self.context.clone(),
            route: self.definition.route.clone(),
            definition_revision: self.definition.revision.clone(),
            turn_id: TurnId::new("once-turn").unwrap(),
            now_ms: 103,
            decisions: vec![GrantDecision {
                interrupt_id: interrupt,
                decision: ApprovalDecision::Once,
                cancelled: false,
                token: Some(pending[0].token.clone()),
            }],
        }
    }
    pub fn close(self) -> StoredExecution {
        let Self {
            directory,
            store,
            application,
            authority,
            execution,
            calls,
            ..
        } = self;
        let old_owner = store.owner_generation();
        assert_eq!(
            Arc::strong_count(&store),
            1,
            "all fixture store aliases must retire"
        );
        drop(store);
        drop(application);
        drop(authority);
        let next = SqliteStore::open(&directory.0).unwrap();
        let next_owner = next.owner_generation();
        let persisted = next.execution(&execution).unwrap().unwrap();
        drop(next);
        assert_eq!(next_owner, old_owner.checked_add(1).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        persisted
    }
}
