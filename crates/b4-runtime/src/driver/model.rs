use crate::{
    EffectInvocation, EffectOutcome, PreparationError, PreparedEffect, RuntimeError,
    StoredExecution,
};
use b4_contract::{AttemptId, TaskId, TransitionFacts};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverConfig {
    pub max_parallel_tasks: usize,
    pub completion_attempts: usize,
    pub max_outcome_bytes: usize,
}
impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            max_parallel_tasks: 2,
            completion_attempts: 8,
            max_outcome_bytes: 65_536,
        }
    }
}
impl DriverConfig {
    pub fn validate(self) -> Result<(), DriverConfigError> {
        if !(1..=64).contains(&self.max_parallel_tasks)
            || !(1..=64).contains(&self.completion_attempts)
            || !(1..=65_536).contains(&self.max_outcome_bytes)
        {
            return Err(DriverConfigError("invalid_driver_limits"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverConfigError(pub &'static str);
impl std::fmt::Display for DriverConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for DriverConfigError {}

/// A trusted host supplies authority and invocation, not scheduling or completion.
/// prepare/park/facts/after_commit run on the coordinator. invoke/on_fault may run
/// on scoped workers. An invocation must not mutate this execution's runtime store.
pub trait DriverHost: Sync {
    type Error: Send;
    fn facts(&self, current: &StoredExecution) -> TransitionFacts;
    fn attempt_id(&self) -> AttemptId;
    fn prepare_dispatch(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
        attempt_id: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<Self::Error>>;
    fn park(
        &self,
        current: &StoredExecution,
        task_id: TaskId,
    ) -> Result<StoredExecution, Self::Error>;
    fn invoke(&self, invocation: &EffectInvocation) -> Result<EffectOutcome, String>;
    fn after_commit(
        &self,
        _: &EffectInvocation,
        _: &EffectOutcome,
        _: &StoredExecution,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Positive cancellation proof bound to this original owned turn. Never infer
    /// this from a failed store operation or callback error.
    fn cancellation_committed(&self) -> bool {
        false
    }
    fn expected_cancel_race(&self, _: &Self::Error) -> bool {
        false
    }
    fn host_error_is_fault(&self, _: &Self::Error) -> bool {
        true
    }
    /// Static, immediate notification. No panic payload or sensitive data is passed.
    fn on_fault(&self) {}
}

#[derive(Debug)]
pub enum DriverError<E> {
    Config(DriverConfigError),
    Runtime(RuntimeError),
    Host(E),
    CallbackUncertain,
    CallbackPanic,
    CoordinatorPanic,
    SpawnFailed,
    InvalidPreparedEffect,
    OutcomeLimit,
    ExecutionStopped,
    OrphanedAttempts,
    RecoveryBlocked,
    ChannelClosed,
}
#[derive(Debug, Clone, PartialEq)]
pub struct RetainedOutcome {
    pub invocation: EffectInvocation,
    pub outcome: EffectOutcome,
}
#[derive(Debug)]
pub struct DriverFailure<E> {
    pub errors: Vec<DriverError<E>>,
    pub retained_outcomes: Vec<RetainedOutcome>,
    pub unsettled_invocations: Vec<EffectInvocation>,
}
impl<E> DriverFailure<E> {
    pub(crate) fn new(error: DriverError<E>) -> Self {
        Self {
            errors: vec![error],
            retained_outcomes: vec![],
            unsettled_invocations: vec![],
        }
    }
}
