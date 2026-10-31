use b4_contract::*;
use b4_kernel::new_execution;
use b4_runtime::*;
use serde_json::{json, Map};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

pub(super) struct Host {
    pub(super) store: Arc<dyn Store>,
    pub(super) attempt: AtomicUsize,
    pub(super) fault: AtomicUsize,
    pub(super) invoke: InvokeHook,
    pub(super) commit: Box<dyn Fn(&EffectInvocation) + Send + Sync>,
    pub(super) prepare: Option<PrepareHook>,
    pub(super) clock: Option<ClockHook>,
    pub(super) cancel: AtomicBool,
    pub(super) healthy_refusals: bool,
}
pub(super) type InvokeHook =
    Box<dyn Fn(&EffectInvocation) -> Result<EffectOutcome, String> + Send + Sync>;
pub(super) type PrepareHook = Box<
    dyn Fn(
            &StoredExecution,
            TaskId,
            AttemptId,
        ) -> Result<PreparedEffect, PreparationError<RuntimeError>>
        + Send
        + Sync,
>;
pub(super) type ClockHook = Box<dyn Fn(&StoredExecution) -> TransitionFacts + Send + Sync>;
impl DriverHost for Host {
    type Error = RuntimeError;
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        if let Some(clock) = &self.clock {
            return clock(current);
        }
        TransitionFacts {
            now_ms: current.snapshot.updated_at_ms + 1,
        }
    }
    fn attempt_id(&self) -> AttemptId {
        AttemptId::new(format!(
            "attempt-{}",
            self.attempt.fetch_add(1, Ordering::SeqCst)
        ))
        .unwrap()
    }
    fn prepare_dispatch(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<Self::Error>> {
        if let Some(prepare) = &self.prepare {
            return prepare(current, task_id, attempt_id);
        }
        Runtime::new(self.store.clone()).prepare_dispatch(
            &current.snapshot.execution_id,
            ExecutionFence::from(&current.snapshot),
            task_id,
            attempt_id,
            self.facts(current),
        )
    }
    fn park(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
    ) -> Result<StoredExecution, Self::Error> {
        Runtime::new(self.store.clone()).apply(
            &current.snapshot.execution_id,
            ExecutionFence::from(&current.snapshot),
            KernelCommand::Park {
                interrupt_id: InterruptId::new(format!("pending-{task_id}")).unwrap(),
                task_id,
                prompt: json!({"approval":true}),
            },
            self.facts(current),
        )
    }
    fn invoke(&self, invocation: &EffectInvocation) -> Result<EffectOutcome, String> {
        (self.invoke)(invocation)
    }
    fn after_commit(
        &self,
        invocation: &EffectInvocation,
        _: &EffectOutcome,
        _: &StoredExecution,
    ) -> Result<(), Self::Error> {
        (self.commit)(invocation);
        Ok(())
    }
    fn on_fault(&self) {
        self.fault.fetch_add(1, Ordering::SeqCst);
    }
    fn cancellation_committed(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
    fn expected_cancel_race(&self, error: &Self::Error) -> bool {
        matches!(error, RuntimeError::Store(StoreError::RevisionConflict))
    }
    fn host_error_is_fault(&self, _: &Self::Error) -> bool {
        !self.healthy_refusals
    }
}
pub(super) fn accepted(store: &dyn Store, id: &str, tasks: &[(&str, bool)]) -> StoredExecution {
    let thread = ThreadId::new(format!("thread-{id}")).unwrap();
    store.create_thread(thread.clone()).unwrap();
    Runtime::new(store)
        .admit(
            &thread,
            new_execution(
                ExecutionId::new(id).unwrap(),
                TurnId::new(format!("turn-{id}")).unwrap(),
                store.owner_generation(),
                Definition {
                    route: RouteKey::new("/parallel#agent").unwrap(),
                    revision: ExecutionRevision::new("v1").unwrap(),
                    max_attempts: 64,
                    batches: vec![tasks
                        .iter()
                        .map(|(id, approval)| TaskSpec {
                            id: TaskId::new(*id).unwrap(),
                            effect_id: EffectId::new(format!("effect-{id}")).unwrap(),
                            recovery: RecoveryPolicy::Pure,
                            requires_approval: *approval,
                        })
                        .collect()],
                },
                Map::from_iter([("input".into(), json!({"seed":0}))]),
                TransitionFacts { now_ms: 1 },
            )
            .unwrap(),
        )
        .unwrap()
}
pub(super) fn result(invocation: &EffectInvocation) -> EffectOutcome {
    EffectOutcome::Succeeded(TaskResult {
        output: json!(invocation.task_id),
        writes: vec![StateWrite::Append {
            field: "items".into(),
            items: vec![json!(invocation.task_id)],
        }],
    })
}
pub(super) fn host(
    store: Arc<dyn Store>,
    invoke: impl Fn(&EffectInvocation) -> Result<EffectOutcome, String> + Send + Sync + 'static,
) -> Host {
    Host {
        store,
        attempt: AtomicUsize::new(0),
        fault: AtomicUsize::new(0),
        invoke: Box::new(invoke),
        commit: Box::new(|_| {}),
        prepare: None,
        clock: None,
        cancel: AtomicBool::new(false),
        healthy_refusals: false,
    }
}
