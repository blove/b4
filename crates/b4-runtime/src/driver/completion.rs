use super::{identity::OwnedTurn, DriverConfig, DriverHost, RetainedOutcome};
use crate::{Runtime, RuntimeError, Store, StoreError, StoredExecution};

pub(super) fn complete<S: Store, H: DriverHost>(
    runtime: &Runtime<S>,
    identity: &OwnedTurn,
    host: &H,
    retained: &RetainedOutcome,
    config: DriverConfig,
) -> Result<StoredExecution, RuntimeError> {
    let mut last = RuntimeError::Store(StoreError::RevisionConflict);
    for _ in 0..config.completion_attempts {
        let current = identity.reload(runtime.store())?;
        if !identity.invocation_matches(&current, &retained.invocation) {
            return Err(StoreError::Corrupt("driver_retained_invocation_changed".into()).into());
        }
        // Fresh facts follow the coherent reload; concurrent cancellation can
        // still advance either fence, so only this completion is retried.
        let facts = host.facts(&current);
        match runtime.complete_outcome(
            &retained.invocation.execution_id,
            retained.invocation.owner_generation,
            retained.invocation.task_id.clone(),
            retained.invocation.attempt_id.clone(),
            &retained.outcome,
            facts,
        ) {
            Ok(current) => return Ok(current),
            Err(
                error @ (RuntimeError::Store(StoreError::RevisionConflict)
                | RuntimeError::Kernel(b4_kernel::KernelError::ClockMovedBackwards)),
            ) => last = error,
            Err(error) => return Err(error),
        }
    }
    Err(last)
}
