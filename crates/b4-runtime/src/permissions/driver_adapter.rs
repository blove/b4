//! Narrow synchronous NewUse bridge with retained receipt-backed completion.
use super::driver_adapter_account::{DriverAdapterAccount, PermissionUseDriverUsage};
use super::*;
use crate::approval::{ApprovalAuthority, ApprovalContext, ApprovalFence};
use crate::*;
use b4_contract::{
    AttemptId, CommitPlan, ExecutionId, ExecutionStatus, Snapshot, TaskId, TaskStatus,
    ThreadGeneration, ThreadId, TransitionFacts,
};
use std::sync::Mutex;

pub trait PermissionUseDriverHooks: Sync {
    /// Local callback admission only; this grants no permission authority.
    /// The default admits entry and preserves existing callback behavior.
    /// Refusal or unwind retains callback work until the failure is retired.
    fn enter_callback(&self) -> bool {
        true
    }
    fn facts(&self, current: &StoredExecution) -> TransitionFacts;
    fn attempt_id(&self) -> AttemptId;
}
pub struct PermissionUseDriverBindings<
    'env,
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
> {
    pub store: &'env S,
    pub authority: &'env A,
    pub producer_catalog: &'env PermissionCatalog,
    pub current_catalog: &'env PermissionCatalog,
    pub context: &'env ApprovalContext,
    pub hooks: &'env H,
    pub capacity: &'env TaskCapacity,
}
/// Owns actual evidence after the scoped Driver has drained; Drop never settles.
pub struct PermissionUseDriverFailure<
    'env,
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
> {
    #[allow(dead_code)] // Actual Driver evidence is retained privately through retirement.
    driver: DriverFailure<PermissionError>,
    slot: Box<DriverSlot<'env, S, A>>,
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> std::fmt::Debug
    for PermissionUseDriverFailure<'_, S, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PermissionUseDriverFailure")
    }
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    PermissionUseDriverFailure<'_, S, A>
{
    /// Consume only a drained, precisely tagged preclaim failure after a typed Ready stop.
    pub fn reconcile_ready_cancellation(
        mut self,
        confirmation: &PermissionReadyCancellationConfirmation,
    ) -> Result<StoredExecution, Self> {
        // Pay coherent reread and all new comparisons/copy from the retained cumulative account.
        if self.slot.account.purchase(524_288, 4096, 1).is_err() {
            return Err(self);
        }
        let current = self.slot.store.thread_view(&self.slot.initial.thread_id);
        if let Ok(Some(view)) = &current {
            let account = &mut self.slot.account;
            let measured = (|| {
                account.json_map(&view.thread.metadata)?;
                if let Some(execution) = &view.execution {
                    account.snapshot(execution)?;
                }
                account.measure(view)?;
                account.before_copy(&confirmation.before.execution)?;
                account.before_copy(&confirmation.execution)?;
                Ok::<(), PermissionError>(())
            })();
            if measured.is_err() {
                return Err(self);
            }
        }
        let slot = &self.slot;
        let origin = if slot.original {
            super::ready_confirmation::ReadyOrigin::OriginalAlways
        } else {
            super::ready_confirmation::ReadyOrigin::NewUse
        };
        if slot.store.owner_generation() != slot.initial.snapshot.owner_generation
            || confirmation.execution.snapshot.owner_generation != slot.store.owner_generation()
            || slot.poisoned
            || slot.original
                != (confirmation.origin == super::ready_confirmation::ReadyOrigin::OriginalAlways)
            || confirmation.origin != origin
            || slot.initial != confirmation.before.execution
            || !matches!(slot.phase, Phase::Empty)
            || slot.prepared.is_some()
            || slot.tickets.fault
            || slot.tickets.ticket.is_some()
            || slot.completion_error.is_some()
            || !self.driver.retained_outcomes.is_empty()
            || !self.driver.unsettled_invocations.is_empty()
            || self.driver.errors.len() != 1
            || !matches!(
                (slot.ready_refusal, &self.driver.errors[0]),
                (
                    Some(ReadyRefusal::StaleInitial),
                    DriverError::Host(PermissionError::UnsupportedProfile)
                ) | (
                    Some(ReadyRefusal::PreclaimFence),
                    DriverError::Host(PermissionError::Store(StoreError::RevisionConflict))
                )
            )
        {
            return Err(self);
        }
        // Scoped Driver workers have drained; no slot mutex or caller-supplied view participates.
        if !matches!(current, Ok(Some(ref view)) if view.thread == confirmation.after_thread && view.execution.as_ref() == Some(&confirmation.execution))
        {
            return Err(self);
        }
        Ok(confirmation.execution.clone())
    }
    pub fn cancel_withheld(
        &mut self,
        authority: &dyn PermissionCancellationAuthority,
        catalog: &PermissionCatalog,
        request: &PermissionUseWithheldCancellationRequest<'_>,
    ) -> Result<StoredExecution, PermissionError> {
        let slot = &mut *self.slot;
        slot.account.purchase(128, 8, 1)?;
        if slot.tickets.fault {
            return Err(PermissionError::UnsupportedProfile);
        }
        let Phase::EntryRefused { _work: work } = &slot.phase else {
            return Err(PermissionError::UnsupportedProfile);
        };
        let Callback::Use(work) = work.as_ref() else {
            return Err(PermissionError::UnsupportedProfile);
        };
        // This actual guard latches unwind while Work remains in the owned slot.
        let guard = RetryPanicFault(&mut slot.tickets.fault);
        let result = work.cancel_withheld(authority, catalog, request, &mut slot.withheld);
        drop(guard);
        if result.is_ok() {
            slot.phase = Phase::Durable;
        }
        result
    }
    pub fn callback_status(&self) -> Option<PermissionUseCallbackStatus> {
        match &self.slot.phase {
            Phase::Settled(work) => Some(work.callback_status()),
            Phase::Refused(receipt) => Some(receipt.status()),
            _ => None,
        }
    }
    pub fn settled_outcome(&self) -> Option<&EffectOutcome> {
        match &self.slot.phase {
            Phase::Settled(work) => work.settled_outcome(),
            _ => None,
        }
    }
    pub fn usage(&self) -> PermissionUseDriverUsage {
        self.slot.account.usage()
    }
    /// Explicit storage-only retry after a returned storage refusal/unknown.
    /// Facts have the same trusted embedding boundary as Work.commit. No
    /// callback, Driver restart, success inference or fresh account is involved.
    pub fn retry_settlement(
        &mut self,
        facts: &TransitionFacts,
    ) -> Result<StoredExecution, PermissionError> {
        let slot = &mut *self.slot;
        slot.account.purchase(128, 8, 1)?;
        if slot.tickets.fault
            || !matches!(slot.phase, Phase::Settled(_))
            || !matches!(
                &slot.completion_error,
                Some(CompletionFailure::Outcome(PermissionError::Store(_)))
            )
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        let prepared = slot
            .prepared
            .as_ref()
            .ok_or(PermissionError::UnsupportedProfile)?;
        slot.account.snapshot(&prepared.execution)?;
        slot.account.measure(&prepared.execution)?;
        slot.account.measure(facts)?;
        let Phase::Settled(work) = &mut slot.phase else {
            return Err(PermissionError::UnsupportedProfile);
        };
        match work
            .settled_outcome()
            .ok_or(PermissionError::UnsupportedProfile)?
        {
            EffectOutcome::Succeeded(result) => {
                slot.account.measure(result)?;
            }
            EffectOutcome::Failed(message) => {
                slot.account.measure(message)?;
            }
            EffectOutcome::Cancelled => {}
        }
        // Work remains in the actual owned slot throughout this external call.
        // An unwind latches local refusal and retains the same account/Work.
        let panic_fault = RetryPanicFault(&mut slot.tickets.fault);
        let result = work.commit(facts);
        drop(panic_fault);
        match result {
            Ok(commit) => {
                slot.phase = Phase::Durable;
                slot.completion_error = None;
                Ok(commit)
            }
            Err(error) => {
                // Preserve the original typed storage error privately. The
                // caller receives the actual new refusal; local errors cannot
                // unlock another retry or be called known precommit storage.
                if !matches!(&error, PermissionError::Store(_)) {
                    slot.tickets.fault = true;
                }
                Err(error)
            }
        }
    }
}
struct RetryPanicFault<'a>(&'a mut bool);
impl Drop for RetryPanicFault<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            *self.0 = true;
        }
    }
}
use super::runtime_mutation::OriginalAlwaysWork;
// Retained carriers are boxed by Phase; inline variants avoid a second allocation.
#[allow(clippy::large_enum_variant)]
enum Callback<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    Use(PermissionUseCallbackWork<'env, 'env, 'env, S, A>),
    Original(OriginalAlwaysWork<'env, S, A>),
}
// Retained carriers are boxed by Phase; inline variants avoid a second allocation.
#[allow(clippy::large_enum_variant)]
enum Receipt<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    Use(PermissionUseCallbackReceipt<'env, 'env, 'env, S, A>),
    Original(OriginalAlwaysWork<'env, S, A>),
}
// Retained carriers are boxed by Phase; inline variants avoid a second allocation.
#[allow(clippy::large_enum_variant)]
enum Work<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    Use(PermissionUseOutcomeWork<'env, 'env, 'env, S, A>),
    Original(OriginalAlwaysWork<'env, S, A>),
}
impl<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> Callback<'env, S, A> {
    fn invoke(self) -> Receipt<'env, S, A> {
        match self {
            Self::Use(w) => Receipt::Use(w.invoke()),
            Self::Original(w) => Receipt::Original(w.invoke()),
        }
    }
}
impl<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> Receipt<'env, S, A> {
    fn status(&self) -> PermissionUseCallbackStatus {
        match self {
            Self::Use(w) => w.status(),
            Self::Original(w) => w.status(),
        }
    }
    fn into_outcome_work(self) -> Work<'env, S, A> {
        match self {
            Self::Use(w) => Work::Use(w.into_outcome_work()),
            Self::Original(w) => Work::Original(w),
        }
    }
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> Work<'_, S, A> {
    fn callback_status(&self) -> PermissionUseCallbackStatus {
        match self {
            Self::Use(w) => w.callback_status(),
            Self::Original(w) => w.callback_status(),
        }
    }
    fn settled_outcome(&self) -> Option<&EffectOutcome> {
        match self {
            Self::Use(w) => w.settled_outcome(),
            Self::Original(w) => w.settled_outcome(),
        }
    }
    fn commit(&mut self, facts: &TransitionFacts) -> Result<StoredExecution, PermissionError> {
        match self {
            Self::Use(w) => w.commit(facts).map(|c| c.into_driver_execution()),
            Self::Original(w) => w.commit(facts),
        }
    }
    fn commit_driver_candidate(
        &mut self,
        facts: &TransitionFacts,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: &CommitPlan,
        account: &mut DriverAdapterAccount,
    ) -> Result<StoredExecution, PermissionError> {
        match self {
            Self::Use(w) => w
                .commit_driver_candidate(facts, id, fence, plan, account)
                .map(|c| c.into_driver_execution()),
            Self::Original(w) => w.commit_driver_candidate(facts, id, fence, plan, account),
        }
    }
}

enum Phase<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    Empty,
    Armed(Box<Callback<'env, S, A>>),
    GatePending(Box<Callback<'env, S, A>>),
    EntryRefused { _work: Box<Callback<'env, S, A>> },
    Entering,
    Settled(Box<Work<'env, S, A>>),
    Committing,
    Durable,
    Refused(Box<Receipt<'env, S, A>>),
}
#[derive(Clone, Copy)]
enum ReadyRefusal {
    StaleInitial,
    PreclaimFence,
}
struct DriverSlot<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    account: DriverAdapterAccount,
    store: &'env S,
    original: bool,
    ready_refusal: Option<ReadyRefusal>,
    poisoned: bool,
    initial: StoredExecution,
    withheld: super::use_withheld_cancellation::WithheldAccount,
    prepared: Option<PreparedEffect>,
    phase: Phase<'env, S, A>,
    tickets: CompletionTickets,
    #[allow(dead_code)] // Actual typed error and origin remain private through retirement.
    completion_error: Option<CompletionFailure>,
}
struct Host<
    'run,
    'env,
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
> {
    bindings: &'run PermissionUseDriverBindings<'env, S, A, H>,
    slot: &'run Mutex<DriverSlot<'env, S, A>>,
    original: bool,
    observer: Option<&'run NativeCancellationObserver>,
}
struct Adapter<'run, 'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    store: &'env S,
    slot: &'run Mutex<DriverSlot<'env, S, A>>,
}

fn invocation_view(invocation: &EffectInvocation) -> impl serde::Serialize + '_ {
    (
        &invocation.execution_id,
        &invocation.turn_id,
        invocation.owner_generation,
        &invocation.task_id,
        &invocation.effect_id,
        &invocation.attempt_id,
        &invocation.input,
    )
}
fn copy_prepared(
    account: &mut DriverAdapterAccount,
    prepared: &PreparedEffect,
) -> Result<(), PermissionError> {
    account.snapshot(&prepared.execution)?;
    account.before_copy(&prepared.execution)?;
    account.json_map(&prepared.invocation.input)?;
    account.before_copy(&invocation_view(&prepared.invocation))
}
fn profile(initial: &StoredExecution, original: bool) -> Result<(), PermissionError> {
    let s = &initial.snapshot;
    if s.status != ExecutionStatus::Running
        || s.revision != if original { 3 } else { 2 }
        || s.attempts_used != 0
        || !s.attempt_history.is_empty()
        || s.tasks.len() != 1
        || s.definition.batches.len() != 1
        || s.definition.batches[0].len() != 1
        || s.tasks[0].status != TaskStatus::Ready
        || !s.tasks[0].approved
        || s.tasks[0].attempt_id.is_some()
        || s.tasks[0].result.is_some()
        || s.tasks[0].failure.is_some()
        || s.tasks[0].spec != s.definition.batches[0][0]
        || !s.tasks[0].spec.requires_approval
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    b4_kernel::validate_snapshot(s)?;
    Ok(())
}

pub fn drive_permission_use_execution<'env, S, A, H>(
    bindings: PermissionUseDriverBindings<'env, S, A, H>,
    initial: StoredExecution,
) -> Result<StoredExecution, PermissionUseDriverFailure<'env, S, A>>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
{
    drive_permission_execution(bindings, initial, false, None)
}

/// Embedded NewUse continuation with cooperative callback cancellation.
/// The host must durably request cancellation before publishing observer intent.
/// Callback entry is a caller sequencing obligation; Dispatch alone does not prove it.
pub fn drive_permission_use_cancellable_execution<'env, S, A, H>(
    bindings: PermissionUseDriverBindings<'env, S, A, H>,
    initial: StoredExecution,
    observer: &NativeCancellationObserver,
) -> Result<StoredExecution, PermissionUseDriverFailure<'env, S, A>>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
{
    drive_permission_execution(bindings, initial, false, Some(observer))
}

fn drive_permission_execution<'env, S, A, H>(
    bindings: PermissionUseDriverBindings<'env, S, A, H>,
    initial: StoredExecution,
    original: bool,
    observer: Option<&NativeCancellationObserver>,
) -> Result<StoredExecution, PermissionUseDriverFailure<'env, S, A>>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
{
    let mut account = if original {
        DriverAdapterAccount::for_original_always()
    } else {
        DriverAdapterAccount::default()
    };
    let mut ready_refusal = None;
    let checked = (|| {
        account.purchase(131_072, 16, 1)?; // fixed single-slot carriers, failure and observation owners
        account.snapshot(&initial)?;
        account.before_copy(&initial)?;
        let actor = &bindings.context.actor;
        let context_size = actor
            .application
            .len()
            .checked_add(actor.tenant.len())
            .and_then(|n| n.checked_add(actor.principal.len()))
            .and_then(|n| n.checked_add(bindings.context.policy_revision.len()))
            .ok_or(PermissionError::LimitExceeded)?;
        // Pair this NewUse bridge with the four existing 256-byte context labels.
        if context_size > 1_024 {
            return Err(PermissionError::UnsupportedProfile);
        }
        // Pay actual enlarged context work before measurement or origin comparisons.
        let context_bytes = context_size
            .checked_mul(13)
            .and_then(|n| n.checked_add(1_024))
            .ok_or(PermissionError::LimitExceeded)?;
        account.purchase(context_bytes, 8, 1)?;
        account.measure(&(actor, &bindings.context.policy_revision))?;
        if observer.is_some()
            && !(if original {
                bindings
                    .store
                    .permission_original_always_cancellation_atomic_durable()
            } else {
                bindings.store.permission_use_cancellation_atomic_durable()
            })
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        profile(&initial, original)?;
        let view = bindings
            .store
            .thread_view(&initial.thread_id)?
            .ok_or(StoreError::ThreadNotFound)?;
        account.json_map(&view.thread.metadata)?;
        if let Some(observed) = &view.execution {
            account.snapshot(observed)?;
        }
        if view.thread.origin.as_ref() != Some(&bindings.context.actor) {
            return Err(PermissionError::UnsupportedProfile);
        }
        account.measure(&view)?;
        if view.execution.as_ref() != Some(&initial) {
            ready_refusal = Some(ReadyRefusal::StaleInitial);
            return Err(PermissionError::UnsupportedProfile);
        }
        if initial.snapshot.owner_generation != bindings.store.owner_generation() {
            return Err(PermissionError::Store(StoreError::OwnerConflict));
        }
        Ok(())
    })();
    // Initial is already caller-owned. On refusal move it without any new copy.
    if let Err(error) = checked {
        if std::env::var_os("B4_WORKER_DIAGNOSTIC").is_some() {
            eprintln!("permission driver initial refusal={}", error.code());
        }
        return Err(PermissionUseDriverFailure {
            driver: DriverFailure::new(DriverError::Host(error)),
            slot: Box::new(DriverSlot {
                account,
                store: bindings.store,
                original,
                ready_refusal,
                poisoned: false,
                initial,
                withheld: Default::default(),
                prepared: None,
                phase: Phase::Empty,
                tickets: CompletionTickets::default(),
                completion_error: None,
            }),
        });
    }
    let slot = Mutex::new(DriverSlot {
        account,
        store: bindings.store,
        original,
        ready_refusal: None,
        poisoned: false,
        initial: initial.clone(),
        withheld: Default::default(),
        prepared: None,
        phase: Phase::Empty,
        tickets: CompletionTickets::default(),
        completion_error: None,
    });
    let result = {
        let runtime = Runtime::new(Adapter {
            store: bindings.store,
            slot: &slot,
        });
        let host = Host {
            bindings: &bindings,
            slot: &slot,
            original,
            observer,
        };
        drive_execution(
            &runtime,
            initial,
            &host,
            bindings.capacity,
            DriverConfig {
                max_parallel_tasks: 1,
                completion_attempts: 1,
                max_outcome_bytes: 65_536,
            },
        )
    };
    // Recovery is retirement only, after scoped workers and borrowed ports retire.
    let slot = match slot.into_inner() {
        Ok(slot) => slot,
        Err(poison) => {
            let mut slot = poison.into_inner();
            slot.poisoned = true;
            slot
        }
    };
    match result {
        Ok(stored) => Ok(stored),
        Err(driver) => {
            if std::env::var_os("B4_WORKER_DIAGNOSTIC").is_some() {
                let classes: Vec<_> = driver
                    .errors
                    .iter()
                    .map(|error| match error {
                        DriverError::Host(error) => error.code(),
                        DriverError::Runtime(_) => "runtime",
                        DriverError::Config(_) => "config",
                        DriverError::CallbackUncertain => "callback_uncertain",
                        DriverError::CallbackPanic => "callback_panic",
                        DriverError::CoordinatorPanic => "coordinator_panic",
                        DriverError::SpawnFailed => "spawn_failed",
                        DriverError::InvalidPreparedEffect => "invalid_prepared_effect",
                        DriverError::OutcomeLimit => "outcome_limit",
                        DriverError::ExecutionStopped => "execution_stopped",
                        DriverError::OrphanedAttempts => "orphaned_attempts",
                        DriverError::RecoveryBlocked => "recovery_blocked",
                        DriverError::ChannelClosed => "channel_closed",
                    })
                    .collect();
                eprintln!("permission driver failure classes={classes:?}");
            }
            Err(PermissionUseDriverFailure {
                driver,
                slot: Box::new(slot),
            })
        }
    }
}

impl<'env, S, A, H> DriverHost for Host<'_, 'env, S, A, H>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
{
    type Error = PermissionError;
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        // Trusted embedding code executes outside the state mutex. The actual
        // hook facts are returned unchanged even if ticket installation fails.
        let facts = self.bindings.hooks.facts(current);
        let Ok(mut slot) = self.slot.lock() else {
            return facts;
        };
        let DriverSlot {
            account,
            prepared,
            phase,
            tickets,
            ..
        } = &mut *slot;
        tickets.observe_controlled(
            matches!(phase, Phase::Settled(_)),
            prepared.as_ref(),
            current,
            facts,
            self.observer.is_some(),
            account,
        )
    }
    fn attempt_id(&self) -> AttemptId {
        self.bindings.hooks.attempt_id()
    }
    fn park(
        &self,
        _current: &StoredExecution,
        _task: TaskId,
    ) -> Result<StoredExecution, PermissionError> {
        Err(PermissionError::UnsupportedProfile)
    }
    fn prepare_dispatch(
        &self,
        current: &StoredExecution,
        task: TaskId,
        attempt: AttemptId,
    ) -> Result<PreparedEffect, PreparationError<PermissionError>> {
        let prepared_claim = (|| {
            {
                let mut slot = self
                    .slot
                    .lock()
                    .map_err(|_| PermissionError::CorruptRecord)?;
                if !matches!(slot.phase, Phase::Empty) {
                    return Err(PermissionError::UnsupportedProfile);
                }
                slot.account.snapshot(current)?;
                slot.account.measure(current)?;
                slot.account.purchase(0, 0, 1)?;
                if !matches!(slot.phase, Phase::Empty)
                    || current != &slot.initial
                    || task != current.snapshot.tasks[0].spec.id
                {
                    return Err(PermissionError::UnsupportedProfile);
                }
            }
            let thread = self
                .bindings
                .store
                .thread(&current.thread_id)?
                .ok_or(StoreError::ThreadNotFound)?;
            let fence = ApprovalFence {
                execution: (&current.snapshot).into(),
                resource_revision: thread.resource_revision,
            };
            let facts = self.bindings.hooks.facts(current);
            let mut budget = if self.original {
                PermissionEvidenceBudget::for_normal_original_always()
            } else {
                PermissionEvidenceBudget::for_use_dispatch()
            };
            budget.ready_driver_fence = Some(fence);
            let prepare = if self.original {
                super::runtime_mutation::prepare_permission_normal_original_dispatch_claim
            } else {
                super::runtime_mutation::prepare_permission_normal_use_dispatch_claim
            };
            let claim_result = prepare(
                self.bindings.store,
                self.bindings.authority,
                self.bindings.producer_catalog,
                &current.snapshot.execution_id,
                fence,
                self.bindings.context,
                task,
                attempt,
                facts,
                &mut budget,
            );
            if budget.ready_preclaim_fence_refused {
                self.slot
                    .lock()
                    .map_err(|_| PermissionError::CorruptRecord)?
                    .ready_refusal = Some(ReadyRefusal::PreclaimFence);
            }
            let claim = claim_result?;
            let prepared = {
                let mut slot = self
                    .slot
                    .lock()
                    .map_err(|_| PermissionError::CorruptRecord)?;
                claim.driver_prepared(&mut slot.account)?
            };
            Ok((claim, prepared))
        })();
        // Early errors may follow a committed Dispatch. The unchanged Driver
        // reconciles its preparing evidence from the actual persisted snapshot.
        let (claim, prepared) = prepared_claim.map_err(|cause| PreparationError {
            cause,
            claimed: None,
        })?;
        let renewal = (|| {
            let work = if self.original {
                Callback::Original(OriginalAlwaysWork::renew(
                    claim,
                    self.bindings.store,
                    self.bindings.authority,
                    self.bindings.current_catalog,
                    self.bindings.context,
                )?)
            } else {
                let mut account = PermissionUseRenewalAccount::for_claim(&claim)?;
                let permit = renew_permission_use_dispatch(
                    claim,
                    self.bindings.store,
                    self.bindings.authority,
                    self.bindings.current_catalog,
                    self.bindings.context,
                    &mut account,
                )?;
                Callback::Use(permit.into_callback_work())
            };
            let mut slot = self
                .slot
                .lock()
                .map_err(|_| PermissionError::CorruptRecord)?;
            copy_prepared(&mut slot.account, &prepared)?;
            slot.prepared = Some(prepared.clone());
            slot.phase = Phase::Armed(Box::new(work));
            Ok(())
        })();
        match renewal {
            Ok(()) => Ok(prepared),
            Err(cause) => Err(PreparationError {
                cause,
                claimed: Some(Box::new(prepared)),
            }),
        }
    }
    fn invoke(&self, invocation: &EffectInvocation) -> Result<EffectOutcome, String> {
        let call = (|| {
            {
                let mut slot = self
                    .slot
                    .lock()
                    .map_err(|_| PermissionError::CorruptRecord)?;
                if !matches!(slot.phase, Phase::Armed(_)) {
                    return Err(PermissionError::UnsupportedProfile);
                }
                slot.account.json_map(&invocation.input)?;
                slot.account.measure(&invocation_view(invocation))?;
                // Prepay local gate and phase bookkeeping before entry.
                slot.account.purchase(64, 4, 1)?;
                if slot.prepared.as_ref().map(|p| &p.invocation) != Some(invocation) {
                    return Err(PermissionError::UnsupportedProfile);
                }
                slot.phase = match std::mem::replace(&mut slot.phase, Phase::Entering) {
                    Phase::Armed(work) => Phase::GatePending(work),
                    other => {
                        slot.phase = other;
                        return Err(PermissionError::UnsupportedProfile);
                    }
                };
            }
            // The genuine renewed work stays in the slot across this external
            // hook. An unwind leaves it pending and ineligible for reentry.
            let admitted = self.bindings.hooks.enter_callback();
            let work = {
                let mut slot = self
                    .slot
                    .lock()
                    .map_err(|_| PermissionError::CorruptRecord)?;
                match std::mem::replace(&mut slot.phase, Phase::Entering) {
                    Phase::GatePending(work) => {
                        if !admitted {
                            slot.phase = Phase::EntryRefused { _work: work };
                            return Err(PermissionError::UnsupportedProfile);
                        }
                        work
                    }
                    other => {
                        slot.phase = other;
                        return Err(PermissionError::UnsupportedProfile);
                    }
                }
            };
            // No state mutex across the existing consuming native entry.
            let receipt = match (*work, self.observer) {
                (Callback::Use(work), Some(observer)) => {
                    Receipt::Use(work.invoke_controlled(observer))
                }
                (Callback::Original(work), Some(observer)) => {
                    Receipt::Original(work.invoke_controlled(observer))
                }
                (work, _) => work.invoke(),
            };
            let status = receipt.status();
            let (mut slot, poisoned) = match self.slot.lock() {
                Ok(slot) => (slot, false),
                Err(poison) => (poison.into_inner(), true),
            };
            if poisoned {
                slot.phase = Phase::Refused(Box::new(receipt));
                return Err(PermissionError::CorruptRecord);
            }
            if status != PermissionUseCallbackStatus::Settled {
                slot.phase = Phase::Refused(Box::new(receipt));
                return Err(PermissionError::UnsupportedProfile);
            }
            // Publish the real owner before fallible projection or clone.
            slot.phase = Phase::Settled(Box::new(receipt.into_outcome_work()));
            let DriverSlot { account, phase, .. } = &mut *slot;
            let Phase::Settled(work) = phase else {
                return Err(PermissionError::CorruptRecord);
            };
            let outcome = work
                .settled_outcome()
                .ok_or(PermissionError::CorruptRecord)?;
            match outcome {
                EffectOutcome::Succeeded(result) => account.before_copy(result)?,
                EffectOutcome::Failed(message) => account.before_copy(message)?,
                EffectOutcome::Cancelled => {}
            }
            Ok(outcome.clone())
        })();
        call.map_err(|_| "permission_driver_callback_uncertain".to_owned())
    }
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> Store for Adapter<'_, '_, S, A> {
    fn owner_generation(&self) -> u64 {
        self.store.owner_generation()
    }
    fn create_thread_with(&self, _seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        Err(StoreError::UnsupportedOperation)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.store.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.store.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.store.execution(id)
    }
    fn admit(&self, _thread: &ThreadId, _initial: Snapshot) -> Result<StoredExecution, StoreError> {
        Err(StoreError::UnsupportedOperation)
    }
    fn admit_fenced(
        &self,
        _thread: &ThreadId,
        _generation: &ThreadGeneration,
        _revision: u64,
        _initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        Err(StoreError::UnsupportedOperation)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.store.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let (mut guard, facts) = SettlementGuard::take(self.slot, id, fence, &plan)?;
        let Some(owner) = guard.owner.as_mut() else {
            return Err(StoreError::InvalidCommit("permission_driver_owner".into()));
        };
        let result =
            owner
                .work
                .commit_driver_candidate(&facts, id, fence, &plan, &mut owner.account);
        match result {
            Ok(commit) => {
                owner.account.finish_completion(true);
                guard.durable = true;
                Ok(commit)
            }
            Err(error) => {
                owner.account.finish_completion(false);
                guard.error = Some(CompletionFailure::Outcome(error));
                Err(StoreError::InvalidCommit(
                    "permission_driver_settlement_refused".into(),
                ))
            }
        }
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.store.journal(id)
    }
}

// Keep a local ticket/account fault distinct from an actual outcome-operation
// refusal. Neither variant claims backend success or a known precommit failure.
#[allow(dead_code)]
enum CompletionFailure {
    Local(PermissionError),
    Outcome(PermissionError),
}
struct SettlementOwner<'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    work: Box<Work<'env, S, A>>,
    account: DriverAdapterAccount,
}
struct SettlementGuard<'run, 'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> {
    slot: &'run Mutex<DriverSlot<'env, S, A>>,
    owner: Option<SettlementOwner<'env, S, A>>,
    durable: bool,
    error: Option<CompletionFailure>,
}
impl<'run, 'env, S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    SettlementGuard<'run, 'env, S, A>
{
    fn take(
        slot: &'run Mutex<DriverSlot<'env, S, A>>,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: &CommitPlan,
    ) -> Result<(Self, TransitionFacts), StoreError> {
        let mut state = slot
            .lock()
            .map_err(|_| StoreError::Unavailable("permission_driver_slot".into()))?;
        // The real account is owned by the outstanding guard while Committing.
        // Exclude another entry before it can touch the inert placeholder.
        if matches!(state.phase, Phase::Committing) {
            return Err(StoreError::Unavailable(
                "permission_driver_committing".into(),
            ));
        }
        state.account.begin_completion().map_err(|error| {
            state.tickets.fault = true;
            state.completion_error = Some(CompletionFailure::Local(error));
            StoreError::Unavailable("permission_driver_limit".into())
        })?;
        if !matches!(state.phase, Phase::Settled(_)) {
            state.account.unsupported_completion();
            return Err(StoreError::UnsupportedOperation);
        }
        let validation = {
            let DriverSlot {
                account, tickets, ..
            } = &mut *state;
            tickets.validate(id, fence, plan, account).cloned()
        };
        let facts = match validation {
            Ok(facts) => facts,
            Err(error) => {
                state.account.finish_completion(false);
                state.tickets.fault = true;
                state.completion_error = Some(CompletionFailure::Local(error));
                return Err(StoreError::InvalidCommit(
                    "permission_driver_completion_ticket".into(),
                ));
            }
        };
        let work = match std::mem::replace(&mut state.phase, Phase::Committing) {
            Phase::Settled(work) => work,
            other => {
                state.phase = other;
                state.tickets.fault = true;
                state.account.finish_completion(false);
                return Err(StoreError::InvalidCommit("permission_driver_phase".into()));
            }
        };
        let account = std::mem::take(&mut state.account);
        drop(state);
        Ok((
            Self {
                slot,
                owner: Some(SettlementOwner { work, account }),
                durable: false,
                error: None,
            },
            facts,
        ))
    }
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> Drop
    for SettlementGuard<'_, '_, S, A>
{
    fn drop(&mut self) {
        let Some(owner) = self.owner.take() else {
            return;
        };
        // No external port is called while holding this mutex. Poison recovery
        // only restores the actual owned carrier after the attempted operation.
        let mut state = self
            .slot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.account = owner.account;
        state.completion_error = self.error.take();
        if std::thread::panicking() {
            state.tickets.fault = true;
        }
        if self.durable {
            state.phase = Phase::Durable;
        } else {
            state.phase = Phase::Settled(owner.work);
        }
    }
}

// Exact data comparison confers no receipt or storage authority.
pub(super) fn candidate_matches(
    expected_id: &ExecutionId,
    expected_fence: ExecutionFence,
    expected: &CommitPlan,
    id: &ExecutionId,
    fence: ExecutionFence,
    candidate: &CommitPlan,
) -> bool {
    expected_id == id
        && expected_fence == fence
        && expected.expected_revision == candidate.expected_revision
        && expected == candidate
}
struct CompletionTicket {
    current: StoredExecution,
    facts: TransitionFacts,
}
#[derive(Default)]
struct CompletionTickets {
    ticket: Option<CompletionTicket>,
    fault: bool,
}
impl CompletionTickets {
    #[cfg(test)]
    fn observe(
        &mut self,
        settled: bool,
        prepared: Option<&PreparedEffect>,
        current: &StoredExecution,
        facts: TransitionFacts,
        account: &mut DriverAdapterAccount,
    ) -> TransitionFacts {
        self.observe_controlled(settled, prepared, current, facts, false, account)
    }
    fn observe_controlled(
        &mut self,
        settled: bool,
        prepared: Option<&PreparedEffect>,
        current: &StoredExecution,
        facts: TransitionFacts,
        controlled: bool,
        account: &mut DriverAdapterAccount,
    ) -> TransitionFacts {
        if !settled {
            return facts;
        }
        let installation = (|| {
            if self.fault || self.ticket.is_some() {
                return Err(PermissionError::UnsupportedProfile);
            }
            let prepared = prepared.ok_or(PermissionError::UnsupportedProfile)?;
            account.snapshot(current)?;
            account.before_copy(current)?;
            account.measure(&facts)?;
            account.json_map(&prepared.invocation.input)?;
            account.measure(&invocation_view(&prepared.invocation))?;
            if (current != &prepared.execution
                && !(controlled
                    && current.thread_id == prepared.execution.thread_id
                    && (super::use_cancellation_relation::snapshot_matches(
                        &prepared.execution.snapshot,
                        &current.snapshot,
                    ) || super::original_always_cancellation_relation::snapshot_matches(
                        &prepared.execution.snapshot,
                        &current.snapshot,
                    ))))
                || facts.now_ms < current.snapshot.updated_at_ms
                || prepared.invocation.execution_id != current.snapshot.execution_id
                || prepared.invocation.turn_id != current.snapshot.turn_id
                || prepared.invocation.owner_generation != current.snapshot.owner_generation
                || current.snapshot.tasks[0].attempt_id.as_ref()
                    != Some(&prepared.invocation.attempt_id)
                || current.snapshot.tasks[0].spec.id != prepared.invocation.task_id
                || current.snapshot.tasks[0].spec.effect_id != prepared.invocation.effect_id
                || current.snapshot.tasks[0].input != prepared.invocation.input
            {
                return Err(PermissionError::UnsupportedProfile);
            }
            self.ticket = Some(CompletionTicket {
                current: current.clone(),
                facts: facts.clone(),
            });
            Ok(())
        })();
        if installation.is_err() {
            self.fault = true;
        }
        facts
    }
    fn validate(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: &CommitPlan,
        account: &mut DriverAdapterAccount,
    ) -> Result<&TransitionFacts, PermissionError> {
        account.purchase(128, 16, 1)?;
        let ticket = self
            .ticket
            .as_ref()
            .filter(|_| !self.fault)
            .ok_or(PermissionError::UnsupportedProfile)?;
        if id != &ticket.current.snapshot.execution_id
            || fence != ExecutionFence::from(&ticket.current.snapshot)
            || plan.expected_revision != ticket.current.snapshot.revision
            || plan.snapshot.updated_at_ms != ticket.facts.now_ms
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        Ok(&ticket.facts)
    }
}
#[cfg(test)]
#[path = "driver_adapter_tests.rs"]
mod tests;

/// Same embedding hooks as NewUse, with a distinct original Always entry.
pub type PermissionOriginalAlwaysDriverBindings<'env, S, A, H> =
    PermissionUseDriverBindings<'env, S, A, H>;
pub struct PermissionOriginalAlwaysDriverFailure<
    'env,
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
>(PermissionUseDriverFailure<'env, S, A>);
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized> std::fmt::Debug
    for PermissionOriginalAlwaysDriverFailure<'_, S, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PermissionOriginalAlwaysDriverFailure")
    }
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    PermissionOriginalAlwaysDriverFailure<'_, S, A>
{
    pub fn reconcile_ready_cancellation(
        self,
        confirmation: &PermissionReadyCancellationConfirmation,
    ) -> Result<StoredExecution, Self> {
        self.0
            .reconcile_ready_cancellation(confirmation)
            .map_err(Self)
    }
    /// Withdraw only actual drained original Work explicitly refused at entry.
    pub fn cancel_withheld(
        &mut self,
        authority: &dyn PermissionCancellationAuthority,
        catalog: &PermissionCatalog,
        request: &PermissionOriginalAlwaysWithheldCancellationRequest<'_>,
    ) -> Result<StoredExecution, PermissionError> {
        let slot = &mut *self.0.slot;
        slot.account.purchase(128, 8, 1)?;
        if slot.tickets.fault {
            return Err(PermissionError::UnsupportedProfile);
        }
        let Phase::EntryRefused { _work: work } = &slot.phase else {
            return Err(PermissionError::UnsupportedProfile);
        };
        let Callback::Original(work) = work.as_ref() else {
            return Err(PermissionError::UnsupportedProfile);
        };
        let guard = RetryPanicFault(&mut slot.tickets.fault);
        let result = work.cancel_withheld(authority, catalog, request, &mut slot.withheld);
        drop(guard);
        if result.is_ok() {
            slot.phase = Phase::Durable;
        }
        result
    }
    pub fn callback_status(&self) -> Option<PermissionUseCallbackStatus> {
        self.0.callback_status()
    }
    pub fn settled_outcome(&self) -> Option<&EffectOutcome> {
        self.0.settled_outcome()
    }
    pub fn usage(&self) -> PermissionUseDriverUsage {
        self.0.usage()
    }
    pub fn retry_settlement(
        &mut self,
        facts: &TransitionFacts,
    ) -> Result<StoredExecution, PermissionError> {
        self.0.retry_settlement(facts)
    }
}
pub fn drive_permission_original_always_execution<'env, S, A, H>(
    bindings: PermissionOriginalAlwaysDriverBindings<'env, S, A, H>,
    initial: StoredExecution,
) -> Result<StoredExecution, PermissionOriginalAlwaysDriverFailure<'env, S, A>>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
{
    drive_permission_execution(bindings, initial, true, None)
        .map_err(PermissionOriginalAlwaysDriverFailure)
}

/// Drive the genuine original human Always Work through controlled native entry.
pub fn drive_permission_original_always_cancellable_execution<'env, S, A, H>(
    bindings: PermissionOriginalAlwaysDriverBindings<'env, S, A, H>,
    initial: StoredExecution,
    observer: &NativeCancellationObserver,
) -> Result<StoredExecution, PermissionOriginalAlwaysDriverFailure<'env, S, A>>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
    H: PermissionUseDriverHooks + ?Sized,
{
    drive_permission_execution(bindings, initial, true, Some(observer))
        .map_err(PermissionOriginalAlwaysDriverFailure)
}
