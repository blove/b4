use super::{capacity::TaskPermit, completion, identity::OwnedTurn, outcome, *};
use crate::{
    EffectInvocation, EffectOutcome, ExecutionFence, PreparedEffect, Runtime, RuntimeError, Store,
    StoredExecution,
};
use b4_contract::{AttemptId, ExecutionStatus, KernelCommand, TaskStatus};
use std::{
    collections::{BTreeMap, VecDeque},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

#[derive(Clone, Copy)]
enum CallbackError {
    Uncertain,
    Panic,
    Limit,
    Stopped,
}
struct Envelope<'a> {
    invocation: EffectInvocation,
    result: Result<EffectOutcome, CallbackError>,
    _permit: TaskPermit<'a>,
}
struct State<'a, E> {
    current: StoredExecution,
    running: usize,
    owned: BTreeMap<AttemptId, EffectInvocation>,
    preparing: Option<EffectInvocation>,
    pending: VecDeque<Envelope<'a>>,
    failure: DriverFailure<E>,
}
impl<E> State<'_, E> {
    fn stopped(&self) -> bool {
        !self.failure.errors.is_empty()
    }
    fn error(&mut self, error: DriverError<E>) {
        self.failure.errors.push(error);
    }
}
fn notify_fault<H: DriverHost>(host: &H, fault: &AtomicBool) {
    fault.store(true, Ordering::Release);
    // A faulty notification implementation must not lose the worker's outcome.
    let _ = catch_unwind(AssertUnwindSafe(|| host.on_fault()));
}

/// Drive an exclusively owned admitted turn. The caller retains its store,
/// producer claim and run capacity until all scoped callbacks have quiesced.
pub fn drive_execution<S: Store, H: DriverHost>(
    runtime: &Runtime<S>,
    initial: StoredExecution,
    host: &H,
    capacity: &TaskCapacity,
    config: DriverConfig,
) -> Result<StoredExecution, DriverFailure<H::Error>> {
    config
        .validate()
        .map_err(|e| DriverFailure::new(DriverError::Config(e)))?;
    let identity = OwnedTurn::capture(runtime.store(), &initial).map_err(|e| {
        host.on_fault();
        DriverFailure::new(DriverError::Runtime(e))
    })?;
    let current = identity.reload(runtime.store()).map_err(|e| {
        host.on_fault();
        DriverFailure::new(DriverError::Runtime(e))
    })?;
    if current
        .snapshot
        .tasks
        .iter()
        .any(|t| t.status == TaskStatus::InFlight)
    {
        host.on_fault();
        return Err(DriverFailure::new(DriverError::OrphanedAttempts));
    }
    if current.snapshot.status == ExecutionStatus::RecoveryBlocked {
        return Err(DriverFailure::new(DriverError::RecoveryBlocked));
    }
    // Both the receiver and all obtained/queued evidence outlive the caught
    // coordinator closure. Its panic cannot destroy results before drainage.
    let (sender, receiver) = mpsc::channel();
    let fault = AtomicBool::new(false);
    let mut state = State {
        current,
        running: 0,
        owned: BTreeMap::new(),
        preparing: None,
        pending: VecDeque::new(),
        failure: DriverFailure {
            errors: vec![],
            retained_outcomes: vec![],
            unsettled_invocations: vec![],
        },
    };
    thread::scope(|scope| {
        let driven = catch_unwind(AssertUnwindSafe(|| {
            coordinate(
                runtime, &identity, host, capacity, config, scope, &sender, &receiver, &fault,
                &mut state,
            )
        }));
        if driven.is_err() {
            notify_fault(host, &fault);
            state.error(DriverError::CoordinatorPanic);
        }
        // A preparation port can commit before it panics or refuses. Recover
        // ownership only from the original exact durable attempt, never from
        // the intent alone, and never infer a callback outcome.
        if let Some(invocation) = state.preparing.take() {
            let reconciled = catch_unwind(AssertUnwindSafe(|| identity.reload(runtime.store())));
            match reconciled {
                Ok(Ok(current)) => {
                    if identity.invocation_matches(&current, &invocation)
                        && current.snapshot.tasks.iter().any(|task| {
                            task.spec.id == invocation.task_id
                                && task.status == TaskStatus::InFlight
                        })
                    {
                        state
                            .owned
                            .insert(invocation.attempt_id.clone(), invocation);
                    }
                }
                Ok(Err(error)) => {
                    state.error(DriverError::Runtime(error));
                    notify_fault(host, &fault);
                }
                Err(_) => {
                    state.error(DriverError::CoordinatorPanic);
                    notify_fault(host, &fault);
                }
            }
        }
        // This runs inside the scoped lifetime, after scheduling has stopped and
        // after fault notification, so an uncooperative sibling remains owned.
        while state.running > 0 || !state.pending.is_empty() {
            if state.pending.is_empty() {
                match receiver.recv() {
                    Ok(envelope) => {
                        state.running -= 1;
                        state.pending.push_back(envelope);
                    }
                    Err(_) => {
                        state.error(DriverError::ChannelClosed);
                        notify_fault(host, &fault);
                        break;
                    }
                }
            }
            let selected = state
                .pending
                .front()
                .map(|e| e.invocation.attempt_id.clone());
            let handled = catch_unwind(AssertUnwindSafe(|| {
                process_pending(runtime, &identity, host, config, &fault, &mut state)
            }));
            if handled.is_err() {
                notify_fault(host, &fault);
                state.error(DriverError::CoordinatorPanic);
                // The front remains owned until processing commits/removes it.
                // Retain a valid result even if the clock/commit port panicked.
                if state.pending.front().map(|e| &e.invocation.attempt_id) == selected.as_ref() {
                    let envelope = state.pending.pop_front().expect("selected owned outcome");
                    if let Ok(outcome) = envelope.result {
                        state.failure.retained_outcomes.push(RetainedOutcome {
                            invocation: envelope.invocation,
                            outcome,
                        });
                    }
                }
            }
        }
    });
    state
        .failure
        .unsettled_invocations
        .extend(state.owned.into_values());
    if state.failure.errors.is_empty() {
        Ok(state.current)
    } else {
        Err(state.failure)
    }
}

#[allow(clippy::too_many_arguments)] // Explicit owner, ports, scope and evidence lifetimes.
fn coordinate<'scope, 'env: 'scope, 'a: 'scope, S: Store, H: DriverHost + 'env>(
    runtime: &Runtime<S>,
    identity: &OwnedTurn,
    host: &'env H,
    capacity: &'a TaskCapacity,
    config: DriverConfig,
    scope: &'scope thread::Scope<'scope, 'env>,
    sender: &mpsc::Sender<Envelope<'a>>,
    receiver: &mpsc::Receiver<Envelope<'a>>,
    fault: &'env AtomicBool,
    state: &mut State<'a, H::Error>,
) {
    loop {
        // A returned outcome owns its permit. Process all queued outcomes before
        // any capacity wait, including after another driver frees a global slot.
        while let Ok(envelope) = receiver.try_recv() {
            state.running -= 1;
            state.pending.push_back(envelope);
        }
        while !state.pending.is_empty() {
            process_pending(runtime, identity, host, config, fault, state);
        }
        if state.stopped() {
            return;
        }
        if fault.load(Ordering::Acquire) {
            // A worker has already reported uncertainty while policy was held.
            // Drain its typed outcome; never enter another prepared callback.
            return;
        }
        state.current = match identity.reload(runtime.store()) {
            Ok(current) => current,
            Err(error) => {
                state.error(DriverError::Runtime(error));
                notify_fault(host, fault);
                return;
            }
        };
        if state.current.snapshot.status == ExecutionStatus::Accepted {
            let started = runtime.apply(
                &state.current.snapshot.execution_id,
                ExecutionFence::from(&state.current.snapshot),
                KernelCommand::Start,
                host.facts(&state.current),
            );
            match started {
                Ok(current) => state.current = current,
                Err(error) => {
                    if expected_runtime_cancel_race(&error) && host.cancellation_committed() {
                        match cancelled_current(runtime, identity) {
                            Ok(current) => {
                                state.current = current;
                                continue;
                            }
                            Err(repair) => {
                                state.error(DriverError::Runtime(repair));
                                notify_fault(host, fault);
                                return;
                            }
                        }
                    }
                    state.error(DriverError::Runtime(error));
                    notify_fault(host, fault);
                    return;
                }
            }
            continue;
        }
        let scheduling = state.current.snapshot.status == ExecutionStatus::Running
            && state.current.snapshot.failure.is_none();
        if scheduling {
            // A park is a store transition, not a callback; it must not wait for
            // either global callback capacity or the per-execution callback limit.
            let selected = state
                .current
                .snapshot
                .tasks
                .iter()
                .find(|t| t.status == TaskStatus::Ready)
                .cloned();
            let permit = if selected
                .as_ref()
                .is_some_and(|t| !t.spec.requires_approval || t.approved)
                && state.running < config.max_parallel_tasks
            {
                capacity.try_acquire()
            } else {
                None
            };
            let park = selected
                .as_ref()
                .filter(|t| t.spec.requires_approval && !t.approved)
                .or_else(|| {
                    if permit.is_none() {
                        state.current.snapshot.tasks.iter().find(|t| {
                            t.status == TaskStatus::Ready && t.spec.requires_approval && !t.approved
                        })
                    } else {
                        None
                    }
                })
                .map(|t| t.spec.id.clone());
            if let Some(task_id) = park {
                match host.park(&state.current, task_id) {
                    Ok(current) => state.current = current,
                    Err(error) => {
                        if host.expected_cancel_race(&error) && host.cancellation_committed() {
                            match cancelled_current(runtime, identity) {
                                Ok(current) => {
                                    state.current = current;
                                    continue;
                                }
                                Err(repair) => {
                                    state.error(DriverError::Runtime(repair));
                                    state.error(DriverError::Host(error));
                                    notify_fault(host, fault);
                                    return;
                                }
                            }
                        }
                        if host.host_error_is_fault(&error) {
                            notify_fault(host, fault);
                        }
                        state.error(DriverError::Host(error));
                        return;
                    }
                }
                continue;
            }
            let ready = selected.map(|t| t.spec.id);
            if let Some(task_id) = ready {
                if state.running < config.max_parallel_tasks {
                    if let Some(permit) = permit {
                        let attempt_id = host.attempt_id();
                        let task = state
                            .current
                            .snapshot
                            .tasks
                            .iter()
                            .find(|task| task.spec.id == task_id)
                            .expect("selected ready task");
                        state.preparing = Some(EffectInvocation {
                            execution_id: state.current.snapshot.execution_id.clone(),
                            turn_id: state.current.snapshot.turn_id.clone(),
                            owner_generation: state.current.snapshot.owner_generation,
                            task_id: task_id.clone(),
                            effect_id: task.spec.effect_id.clone(),
                            attempt_id: attempt_id.clone(),
                            input: task.input.clone(),
                        });
                        let prepared = match host.prepare_dispatch(
                            &state.current,
                            task_id.clone(),
                            attempt_id.clone(),
                        ) {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                if let Some(prepared) = &error.claimed {
                                    let valid_claim = register_refused_claim(
                                        runtime, identity, host, fault, state, prepared,
                                    );
                                    if !valid_claim {
                                        state.error(DriverError::Host(error.cause));
                                        return;
                                    }
                                }
                                if host.expected_cancel_race(&error.cause)
                                    && host.cancellation_committed()
                                {
                                    let repaired = repair_preparation(
                                        runtime,
                                        identity,
                                        host,
                                        config,
                                        error.claimed.as_deref(),
                                    );
                                    match repaired {
                                        Ok(current) => {
                                            if let Some(prepared) = error.claimed {
                                                state.owned.remove(&prepared.invocation.attempt_id);
                                            }
                                            state.current = current;
                                            state.preparing = None;
                                            drop(permit);
                                            continue;
                                        }
                                        Err(repair) => {
                                            if let (
                                                Some(prepared),
                                                RuntimeError::OutcomeNotCommitted {
                                                    outcome, ..
                                                },
                                            ) = (&error.claimed, &repair)
                                            {
                                                state.failure.retained_outcomes.push(
                                                    RetainedOutcome {
                                                        invocation: prepared.invocation.clone(),
                                                        outcome: outcome.clone(),
                                                    },
                                                );
                                            }
                                            state.error(DriverError::Runtime(repair));
                                            state.error(DriverError::Host(error.cause));
                                            notify_fault(host, fault);
                                            return;
                                        }
                                    }
                                }
                                if host.host_error_is_fault(&error.cause) {
                                    notify_fault(host, fault);
                                }
                                state.error(DriverError::Host(error.cause));
                                return;
                            }
                        };
                        if !identity.prepared_matches(&prepared)
                            || state.preparing.as_ref() != Some(&prepared.invocation)
                        {
                            state.error(DriverError::InvalidPreparedEffect);
                            notify_fault(host, fault);
                            return;
                        }
                        let durable = match identity.reload(runtime.store()) {
                            Ok(current) => current,
                            Err(error) => {
                                state.owned.insert(
                                    prepared.invocation.attempt_id.clone(),
                                    prepared.invocation.clone(),
                                );
                                state.error(DriverError::Runtime(error));
                                notify_fault(host, fault);
                                return;
                            }
                        };
                        let durable_claim = identity
                            .invocation_matches(&durable, &prepared.invocation)
                            && durable.snapshot.tasks.iter().any(|t| {
                                t.spec.id == prepared.invocation.task_id
                                    && t.status == TaskStatus::InFlight
                            });
                        if durable_claim {
                            state.owned.insert(
                                prepared.invocation.attempt_id.clone(),
                                prepared.invocation.clone(),
                            );
                        }
                        if !identity.prepared_matches(&prepared)
                            || !durable_claim
                            || prepared.invocation.task_id != task_id
                            || prepared.invocation.attempt_id != attempt_id
                        {
                            state.error(DriverError::InvalidPreparedEffect);
                            notify_fault(host, fault);
                            return;
                        }
                        state.current = durable;
                        state.preparing = None;
                        if fault.load(Ordering::Acquire) {
                            state.error(DriverError::ExecutionStopped);
                            return;
                        }
                        let invocation = prepared.invocation;
                        let sender = sender.clone();
                        let spawned = thread::Builder::new().name("b4-task".into()).spawn_scoped(
                            scope,
                            move || {
                                let obtained = catch_unwind(AssertUnwindSafe(|| {
                                    if fault.load(Ordering::Acquire) {
                                        return Err(CallbackError::Stopped);
                                    }
                                    let outcome = host
                                        .invoke(&invocation)
                                        .map_err(|_| CallbackError::Uncertain)?;
                                    if !outcome::bounded(&outcome, config.max_outcome_bytes) {
                                        return Err(CallbackError::Limit);
                                    }
                                    Ok(outcome)
                                }));
                                let result = obtained.unwrap_or(Err(CallbackError::Panic));
                                if result.is_err() {
                                    notify_fault(host, fault);
                                }
                                let _ = sender.send(Envelope {
                                    invocation,
                                    result,
                                    _permit: permit,
                                });
                            },
                        );
                        if spawned.is_err() {
                            state.error(DriverError::SpawnFailed);
                            notify_fault(host, fault);
                            return;
                        }
                        state.running += 1;
                        continue;
                    }
                }
                if state.running == 0 {
                    capacity.wait_for_change();
                    continue;
                }
            }
        }
        if state.running > 0 {
            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(envelope) => {
                    state.running -= 1;
                    state.pending.push_back(envelope);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    state.error(DriverError::ChannelClosed);
                    notify_fault(host, fault);
                    return;
                }
            }
            continue;
        }
        if matches!(
            state.current.snapshot.status,
            ExecutionStatus::Running | ExecutionStatus::CancelRequested
        ) {
            state.error(DriverError::OrphanedAttempts);
            notify_fault(host, fault);
        }
        return;
    }
}

fn register_refused_claim<S: Store, H: DriverHost>(
    runtime: &Runtime<S>,
    identity: &OwnedTurn,
    host: &H,
    fault: &AtomicBool,
    state: &mut State<'_, H::Error>,
    prepared: &PreparedEffect,
) -> bool {
    if state.preparing.as_ref() != Some(&prepared.invocation)
        || !identity.prepared_matches(prepared)
    {
        state.error(DriverError::InvalidPreparedEffect);
        notify_fault(host, fault);
        return false;
    }
    match identity.reload(runtime.store()) {
        Ok(current) => {
            if identity.invocation_matches(&current, &prepared.invocation)
                && current.snapshot.tasks.iter().any(|task| {
                    task.spec.id == prepared.invocation.task_id
                        && task.status == TaskStatus::InFlight
                })
            {
                state.owned.insert(
                    prepared.invocation.attempt_id.clone(),
                    prepared.invocation.clone(),
                );
                state.preparing = None;
                return true;
            } else {
                state.error(DriverError::InvalidPreparedEffect);
                notify_fault(host, fault);
            }
        }
        Err(error) => {
            // The trusted preparation API returned exact claim evidence, but
            // storage cannot confirm or disprove it. Preserve that uncertainty.
            state.owned.insert(
                prepared.invocation.attempt_id.clone(),
                prepared.invocation.clone(),
            );
            state.error(DriverError::Runtime(error));
            notify_fault(host, fault);
        }
    }
    state.preparing = None;
    false
}

fn process_pending<S: Store, H: DriverHost>(
    runtime: &Runtime<S>,
    identity: &OwnedTurn,
    host: &H,
    config: DriverConfig,
    fault: &AtomicBool,
    state: &mut State<'_, H::Error>,
) {
    let envelope = state.pending.front().expect("owned queued outcome");
    let invocation = envelope.invocation.clone();
    match envelope.result.clone() {
        Ok(outcome) => {
            let retained = RetainedOutcome {
                invocation: invocation.clone(),
                outcome,
            };
            match completion::complete(runtime, identity, host, &retained, config) {
                Ok(current) => {
                    state.current = current;
                    state.owned.remove(&invocation.attempt_id);
                    let handled_envelope = state.pending.pop_front();
                    // The outcome is durable before any projection port may fail.
                    if let Err(error) =
                        host.after_commit(&invocation, &retained.outcome, &state.current)
                    {
                        state.error(DriverError::Host(error));
                        notify_fault(host, fault);
                    }
                    // Release the task slot after postcommit handling, while a
                    // panic still cannot replay or retain an already durable result.
                    drop(handled_envelope);
                }
                Err(error) => {
                    state.failure.retained_outcomes.push(retained);
                    state.error(DriverError::Runtime(error));
                    notify_fault(host, fault);
                    state.pending.pop_front();
                }
            }
        }
        Err(error) => {
            state.error(match error {
                CallbackError::Uncertain => DriverError::CallbackUncertain,
                CallbackError::Panic => DriverError::CallbackPanic,
                CallbackError::Limit => DriverError::OutcomeLimit,
                CallbackError::Stopped => DriverError::ExecutionStopped,
            });
            notify_fault(host, fault);
            state.pending.pop_front();
        }
    }
}
fn expected_runtime_cancel_race(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::Store(crate::StoreError::RevisionConflict)
            | RuntimeError::Kernel(b4_kernel::KernelError::ClockMovedBackwards)
            | RuntimeError::Kernel(b4_kernel::KernelError::InvalidTransition(
                "execution_status" | "task_not_authorized_ready"
            ))
    )
}
fn cancelled_current<S: Store>(
    runtime: &Runtime<S>,
    identity: &OwnedTurn,
) -> Result<StoredExecution, RuntimeError> {
    let current = identity.reload(runtime.store())?;
    let cancelling = matches!(
        current.snapshot.status,
        ExecutionStatus::CancelRequested | ExecutionStatus::Cancelled | ExecutionStatus::Failed
    ) || (current.snapshot.status == ExecutionStatus::Running
        && current.snapshot.failure.is_some());
    if !cancelling {
        return Err(RuntimeError::EffectFailed(
            "driver_cancelled_state_changed".into(),
        ));
    }
    Ok(current)
}
fn repair_preparation<S: Store, H: DriverHost>(
    runtime: &Runtime<S>,
    identity: &OwnedTurn,
    host: &H,
    config: DriverConfig,
    prepared: Option<&PreparedEffect>,
) -> Result<StoredExecution, RuntimeError> {
    let current = cancelled_current(runtime, identity)?;
    let Some(prepared) = prepared else {
        return Ok(current);
    };
    if !identity.prepared_matches(prepared)
        || !identity.invocation_matches(&current, &prepared.invocation)
    {
        return Err(RuntimeError::EffectFailed(
            "driver_cancelled_attempt_changed".into(),
        ));
    }
    completion::complete(
        runtime,
        identity,
        host,
        &RetainedOutcome {
            invocation: prepared.invocation.clone(),
            outcome: EffectOutcome::Cancelled,
        },
        config,
    )
    .map_err(|cause| RuntimeError::OutcomeNotCommitted {
        outcome: EffectOutcome::Cancelled,
        cause: Box::new(cause),
    })
}
