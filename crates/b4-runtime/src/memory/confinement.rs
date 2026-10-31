use super::{MemoryStore, State};
use crate::confinement::{
    audit, backend, RecoveryConfinementBackendError as Error, RecoveryConfinementOutcome,
    RecoveryConfinementReason, RecoveryConfinementTarget,
};
use crate::{JournalEntry, StoredExecution, ThreadRecord, ThreadView};
use b4_contract::{ExecutionStatus, TransitionFacts, TurnId};

pub(super) fn capture(
    store: &MemoryStore,
    target: &RecoveryConfinementTarget,
) -> Result<ThreadView, Error> {
    let state = store.lock().map_err(|_| Error::Unavailable)?;
    let (thread, execution) = checked(&state, target)?;
    Ok(ThreadView {
        thread: thread.clone(),
        execution: Some(execution.clone()),
    })
}

pub(super) fn mutate(
    store: &MemoryStore,
    original: &ThreadView,
    target: &RecoveryConfinementTarget,
    reason: RecoveryConfinementReason,
    fresh: &TurnId,
    facts: TransitionFacts,
) -> Result<RecoveryConfinementOutcome, Error> {
    let mut state = store.lock().map_err(|_| Error::Unavailable)?;
    let (thread, execution) = checked(&state, target)?;
    let actual = ThreadView {
        thread: thread.clone(),
        execution: Some(execution.clone()),
    };
    if &actual != original {
        return Err(Error::TargetChanged);
    }
    if execution.snapshot.status == ExecutionStatus::RecoveryBlocked {
        return Ok(RecoveryConfinementOutcome::AlreadyBlocked);
    }
    let (snapshot, receipt) = backend::prepare_change(&actual, target, reason, fresh, 1, facts)?;
    let entry = JournalEntry::from(&snapshot);
    let journal = state
        .journal
        .get_mut(&target.execution_id)
        .ok_or(Error::CorruptRecord)?;
    journal.try_reserve(1).map_err(|_| Error::Unavailable)?;
    // No mutation occurs until all audit, proposal/output guards and allocation
    // complete. Existing keys were proved under this same mutex.
    journal.push(entry);
    state
        .executions
        .get_mut(&target.execution_id)
        .expect("audited execution")
        .snapshot = snapshot;
    state
        .threads
        .get_mut(&target.thread_id)
        .expect("audited thread")
        .resource_revision = receipt.new_resource_revision;
    Ok(RecoveryConfinementOutcome::Confined(receipt))
}

fn checked<'a>(
    state: &'a State,
    target: &RecoveryConfinementTarget,
) -> Result<(&'a ThreadRecord, &'a StoredExecution), Error> {
    let mut budget = backend::AuditBudget::default();
    let thread = state
        .threads
        .get(&target.thread_id)
        .ok_or(Error::NotFound)?;
    backend::measure_thread(thread, &mut budget)?;
    if thread.id != target.thread_id {
        return Err(Error::CorruptRecord);
    }
    let active = thread
        .active_execution_id
        .as_ref()
        .ok_or(Error::NotActive)?;
    if active != &target.execution_id {
        return Err(Error::TargetChanged);
    }
    let execution = state.executions.get(active).ok_or(Error::CorruptRecord)?;
    if execution.snapshot.execution_id != *active {
        return Err(Error::CorruptRecord);
    }
    budget.add_bytes(execution.thread_id.as_str().len() + active.as_str().len() + 16)?;
    budget.borrowed(&execution.snapshot, backend::SNAPSHOT_BYTES)?;
    let grants = state.grants.get(active).map_or(&[][..], Vec::as_slice);
    let acceptances = state.acceptances.get(active).map_or(&[][..], Vec::as_slice);
    let journal = state.journal.get(active).map_or(&[][..], Vec::as_slice);
    if grants.len() > backend::AUTHORITY_ROWS
        || acceptances.len() > backend::AUTHORITY_ROWS
        || journal.len() > backend::JOURNAL_ROWS
    {
        return Err(Error::LimitExceeded);
    }
    for grant in grants {
        budget.add_bytes(
            grant.execution_id.as_str().len()
                + grant.interrupt_id.as_str().len()
                + grant.task_id.as_str().len()
                + grant.thread_id.as_str().len()
                + grant.thread_generation.as_str().len()
                + 8,
        )?;
        budget.borrowed(grant, backend::AUTHORITY_BYTES)?;
    }
    for acceptance in acceptances {
        budget.add_bytes(
            acceptance.execution_id.as_str().len()
                + acceptance.turn_id.as_str().len()
                + acceptance.thread_id.as_str().len()
                + acceptance.thread_generation.as_str().len()
                + acceptance.definition_revision.as_str().len()
                + 16,
        )?;
        budget.borrowed(acceptance, backend::AUTHORITY_BYTES)?;
    }
    let mut chain = backend::JournalAudit::new(&execution.snapshot, grants, acceptances)?;
    for entry in journal {
        budget.add_bytes(
            entry.execution_id.as_str().len()
                + thread.id.as_str().len()
                + 24
                + if entry.revision == 0 { 5 } else { 6 },
        )?;
        budget.borrowed(&entry.status, 32)?;
        budget.borrowed(entry, backend::JOURNAL_BYTES)?;
        chain.observe(entry.clone())?;
    }
    let evidence = chain.finish()?;
    let foreign = state
        .threads
        .values()
        .any(|other| other.id != thread.id && other.active_execution_id.as_ref() == Some(active));
    audit::resource(
        thread,
        execution,
        grants,
        acceptances,
        &evidence,
        target,
        1,
        foreign,
    )?;
    // Output view clone is permitted only after all borrowed input preflight.
    Ok((thread, execution))
}
