use crate::*;
use b4_contract::*;
use b4_kernel::KernelError;
use serde_json::{Map, Value};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct EffectInvocation {
    pub execution_id: ExecutionId,
    pub turn_id: TurnId,
    pub owner_generation: u64,
    pub task_id: TaskId,
    pub effect_id: EffectId,
    pub attempt_id: AttemptId,
    pub input: Map<String, Value>,
}

/// An exact durable attempt prepared without invoking its effect. Hosts must
/// retain the execution owner and invocation identity through completion.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedEffect {
    pub execution: StoredExecution,
    pub invocation: EffectInvocation,
}
impl PreparedEffect {
    pub(crate) fn invocation_for(
        snapshot: &Snapshot,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<EffectInvocation, StoreError> {
        let task = snapshot
            .tasks
            .iter()
            .find(|task| task.spec.id == task_id)
            .filter(|task| {
                task.status == TaskStatus::InFlight && task.attempt_id.as_ref() == Some(&attempt_id)
            })
            .ok_or_else(|| StoreError::InvalidCommit("prepared_dispatch_task_invalid".into()))?;
        Ok(EffectInvocation {
            execution_id: snapshot.execution_id.clone(),
            turn_id: snapshot.turn_id.clone(),
            owner_generation: snapshot.owner_generation,
            task_id,
            effect_id: task.spec.effect_id.clone(),
            attempt_id,
            input: task.input.clone(),
        })
    }
}

/// A typed preparation refusal and any attempt already durably claimed.
/// `claimed` is evidence of ownership, not authority to invoke after refusal.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparationError<E> {
    pub cause: E,
    pub claimed: Option<Box<PreparedEffect>>,
}
impl<E: fmt::Display> fmt::Display for PreparationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.cause.fmt(f)
    }
}
impl<E: std::error::Error + 'static> std::error::Error for PreparationError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// A host-declared settled effect outcome. An outer callback error represents
/// uncertainty and must not be converted into `Failed` automatically.
#[derive(Debug, Clone, PartialEq)]
pub enum EffectOutcome {
    Succeeded(TaskResult),
    Failed(String),
    /// Trusted acknowledgement that the attempt has quiesced without a result.
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeError {
    Store(StoreError),
    Kernel(KernelError),
    EffectFailed(String),
    CompletionNotCommitted {
        result: TaskResult,
        cause: Box<RuntimeError>,
    },
    OutcomeNotCommitted {
        outcome: EffectOutcome,
        cause: Box<RuntimeError>,
    },
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RuntimeError {}
impl From<StoreError> for RuntimeError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
impl From<KernelError> for RuntimeError {
    fn from(e: KernelError) -> Self {
        Self::Kernel(e)
    }
}

/// A synchronous service for an already authorized host. Run blocking store and
/// effect work on the host's blocking worker; this type creates no executor.
pub struct Runtime<S> {
    store: S,
}
impl<S: Store> Runtime<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, RuntimeError> {
        Ok(self.store.admit(thread_id, initial)?)
    }

    pub fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, RuntimeError> {
        Ok(self.store.admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )?)
    }

    fn load(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
    ) -> Result<StoredExecution, RuntimeError> {
        let current = self
            .store
            .execution(execution_id)?
            .ok_or(StoreError::ExecutionNotFound)?;
        if current.snapshot.revision != fence.revision {
            return Err(StoreError::RevisionConflict.into());
        }
        if current.snapshot.owner_generation != fence.owner_generation {
            return Err(StoreError::OwnerConflict.into());
        }
        Ok(current)
    }

    /// Commit an exact InFlight attempt without invoking its effect.
    pub fn prepare_dispatch(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        task_id: TaskId,
        attempt_id: AttemptId,
        facts: TransitionFacts,
    ) -> Result<PreparedEffect, PreparationError<RuntimeError>> {
        let prepare = || -> Result<PreparedEffect, RuntimeError> {
            let current = self.load(execution_id, fence)?;
            if current.snapshot.owner_generation != self.store.owner_generation() {
                return Err(StoreError::OwnerConflict.into());
            }
            let plan = b4_kernel::transition(
                &current.snapshot,
                KernelCommand::Dispatch {
                    task_id: task_id.clone(),
                    attempt_id: attempt_id.clone(),
                },
                facts,
            )?;
            // Derive the invocation before committing, so no fallible step can
            // lose ownership evidence after a successful Dispatch transaction.
            let invocation = PreparedEffect::invocation_for(&plan.snapshot, task_id, attempt_id)?;
            let execution = self.store.commit(execution_id, fence, plan)?;
            Ok(PreparedEffect {
                execution,
                invocation,
            })
        };
        prepare().map_err(|cause| PreparationError {
            cause,
            claimed: None,
        })
    }

    /// Apply an authorized command with a caller-captured fence. Recovery uses
    /// `recover` so that an old owner can never deliver an ordinary completion.
    pub fn apply(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        command: KernelCommand,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, RuntimeError> {
        let current = self.load(execution_id, fence)?;
        if current.snapshot.owner_generation != self.store.owner_generation() {
            return Err(StoreError::OwnerConflict.into());
        }
        if matches!(command, KernelCommand::Recover { .. }) {
            return Err(KernelError::InvalidTransition("use_runtime_recover").into());
        }
        let plan = b4_kernel::transition(&current.snapshot, command, facts)?;
        Ok(self.store.commit(execution_id, fence, plan)?)
    }

    /// Explicitly recover a known execution after opening a new store owner.
    pub fn recover(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        available_revision: ExecutionRevision,
        turn_id: TurnId,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, RuntimeError> {
        let current = self.load(execution_id, fence)?;
        let plan = b4_kernel::transition(
            &current.snapshot,
            KernelCommand::Recover {
                available_revision,
                turn_id,
                owner_generation: self.store.owner_generation(),
            },
            facts,
        )?;
        Ok(self.store.commit(execution_id, fence, plan)?)
    }

    /// Persist a previously obtained result without invoking the effect again.
    /// The owner captured at invocation is mandatory even after a fresh load.
    /// Borrow the result so the host retains it across repeated commit failures.
    pub fn complete(
        &self,
        execution_id: &ExecutionId,
        owner_generation: u64,
        task_id: TaskId,
        attempt_id: AttemptId,
        result: &TaskResult,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, RuntimeError> {
        self.complete_command(
            execution_id,
            owner_generation,
            KernelCommand::Complete {
                task_id,
                attempt_id,
                result: result.clone(),
            },
            facts,
        )
    }

    /// Commit a previously obtained settled outcome without invoking its effect
    /// again. The borrowed outcome stays with the host across commit failures.
    /// A fresh revision is loaded, while invocation owner and attempt remain
    /// mandatory fences against stale delivery.
    pub fn complete_outcome(
        &self,
        execution_id: &ExecutionId,
        owner_generation: u64,
        task_id: TaskId,
        attempt_id: AttemptId,
        outcome: &EffectOutcome,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, RuntimeError> {
        let command = match outcome {
            EffectOutcome::Succeeded(result) => KernelCommand::Complete {
                task_id,
                attempt_id,
                result: result.clone(),
            },
            EffectOutcome::Failed(failure) => KernelCommand::FailAttempt {
                task_id,
                attempt_id,
                failure: failure.clone(),
            },
            EffectOutcome::Cancelled => KernelCommand::CancelAttempt {
                task_id,
                attempt_id,
            },
        };
        self.complete_command(execution_id, owner_generation, command, facts)
    }

    fn complete_command(
        &self,
        execution_id: &ExecutionId,
        owner_generation: u64,
        command: KernelCommand,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, RuntimeError> {
        let current = self
            .store
            .execution(execution_id)?
            .ok_or(StoreError::ExecutionNotFound)?;
        if owner_generation != self.store.owner_generation()
            || owner_generation != current.snapshot.owner_generation
        {
            return Err(StoreError::OwnerConflict.into());
        }
        let fence = ExecutionFence::from(&current.snapshot);
        let plan = b4_kernel::transition(&current.snapshot, command, facts)?;
        Ok(self.store.commit(execution_id, fence, plan)?)
    }

    /// Commit the InFlight attempt, invoke the host callback once, then persist
    /// its result. A result commit failure returns the obtained result for a
    /// completion-only retry; it never retries the external callback. The host
    /// supplies fresh completion facts after the callback returns successfully.
    #[allow(clippy::too_many_arguments)] // Keep explicit identities, fence and host time ports.
    pub fn dispatch<F, C>(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        task_id: TaskId,
        attempt_id: AttemptId,
        facts: TransitionFacts,
        completion_facts: C,
        effect: F,
    ) -> Result<StoredExecution, RuntimeError>
    where
        F: FnOnce(&EffectInvocation) -> Result<TaskResult, String>,
        C: FnOnce() -> TransitionFacts,
    {
        let prepared = self
            .prepare_dispatch(execution_id, fence, task_id, attempt_id, facts)
            .map_err(|error| error.cause)?;
        let invocation = prepared.invocation;
        let result = effect(&invocation).map_err(RuntimeError::EffectFailed)?;
        self.complete(
            &invocation.execution_id,
            invocation.owner_generation,
            invocation.task_id,
            invocation.attempt_id,
            &result,
            completion_facts(),
        )
        .map_err(|cause| RuntimeError::CompletionNotCommitted {
            result,
            cause: Box::new(cause),
        })
    }

    /// Commit the attempt before invoking the host callback once, then commit
    /// its explicitly settled outcome. An outer error leaves the attempt
    /// InFlight; a completion failure returns the outcome for completion-only
    /// retry and never replays the callback.
    #[allow(clippy::too_many_arguments)] // Keep explicit identities, fence and host time ports.
    pub fn dispatch_outcome<F, C>(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        task_id: TaskId,
        attempt_id: AttemptId,
        facts: TransitionFacts,
        completion_facts: C,
        effect: F,
    ) -> Result<StoredExecution, RuntimeError>
    where
        F: FnOnce(&EffectInvocation) -> Result<EffectOutcome, String>,
        C: FnOnce() -> TransitionFacts,
    {
        let prepared = self
            .prepare_dispatch(execution_id, fence, task_id, attempt_id, facts)
            .map_err(|error| error.cause)?;
        let invocation = prepared.invocation;
        let outcome = effect(&invocation).map_err(RuntimeError::EffectFailed)?;
        self.complete_outcome(
            &invocation.execution_id,
            invocation.owner_generation,
            invocation.task_id,
            invocation.attempt_id,
            &outcome,
            completion_facts(),
        )
        .map_err(|cause| RuntimeError::OutcomeNotCommitted {
            outcome,
            cause: Box::new(cause),
        })
    }
}
