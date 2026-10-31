use super::*;
use crate::{
    lifecycle::{dispose_panic, record_store_failure, OwnedShared},
    recovery::{recover_owned, RecoveryDisposition, RecoveryRefusal, RecoveryTarget},
    supervisor::StartupCapability,
};
use b4_contract::{ExecutionStatus, TaskStatus};
use b4_runtime::{
    approval::ApprovalStore, DiscoveryError, DiscoveryOwnerRelation, ExecutionFence, ThreadView,
    UnfinishedDiscoveryPage, UnfinishedExecutionRef, DISCOVERY_SCANNED_ROWS,
};
use std::sync::atomic::Ordering;

fn candidate(progress: StartupProgress, reason: StartupIncompleteReason) -> CompletionCandidate {
    CompletionCandidate {
        progress,
        reason: Some(reason),
    }
}

pub(super) fn increment(counter: &mut usize, amount: usize, maximum: usize) -> bool {
    let Some(next) = counter.checked_add(amount).filter(|next| *next <= maximum) else {
        return false;
    };
    *counter = next;
    true
}

/// Consume every blocking completion locally while the coordinator remains
/// counted. No JoinError or arbitrary panic payload escapes into observation.
pub(super) async fn joined<S, R>(
    shared: &OwnedShared<S>,
    job: tokio::task::JoinHandle<R>,
) -> Option<R> {
    match job.await {
        Ok(result) => Some(result),
        Err(error) => {
            shared.fault.store(true, Ordering::Release);
            if error.is_panic() {
                dispose_panic(error.into_panic());
            }
            None
        }
    }
}

pub(super) async fn epoch<S: ApprovalStore + 'static>(shared: &OwnedShared<S>) -> Option<u64> {
    let result = joined(
        shared,
        shared.clone().spawn(|s| {
            s.store.readiness().map_err(|error| {
                record_store_failure(s, &error);
            })?;
            let owner = s.store.owner_generation();
            if owner == 0 {
                s.fault.store(true, Ordering::Release);
                return Err(());
            }
            Ok(owner)
        }),
    )
    .await?;
    result.ok()
}

pub(super) fn page_shape(page: &UnfinishedDiscoveryPage, owner: u64) -> bool {
    page.owner_generation == owner
        && page.scanned_rows <= DISCOVERY_SCANNED_ROWS
        && page.records.len() <= page.scanned_rows
        && (page.next_cursor.is_none() || page.scanned_rows == DISCOVERY_SCANNED_ROWS)
        && page.records.windows(2).all(|pair| {
            pair[0].execution_id.as_str().as_bytes() < pair[1].execution_id.as_str().as_bytes()
        })
        && page.records.iter().all(|record| {
            record.execution_fence.owner_generation > 0
                && record.execution_fence.owner_generation <= owner
                && record.owner_relation
                    == if record.execution_fence.owner_generation == owner {
                        DiscoveryOwnerRelation::Current
                    } else {
                        DiscoveryOwnerRelation::Prior
                    }
                && (matches!(
                    record.status,
                    ExecutionStatus::Accepted
                        | ExecutionStatus::Running
                        | ExecutionStatus::WaitingForApproval
                        | ExecutionStatus::CancelRequested
                        | ExecutionStatus::RecoveryBlocked
                ) || record.has_in_flight_task
                    || record.has_outcome_unknown_task)
        })
}

pub(super) fn matches_descriptor(view: &ThreadView, record: &UnfinishedExecutionRef) -> bool {
    let Some(execution) = &view.execution else {
        return false;
    };
    let snapshot = &execution.snapshot;
    view.thread.id == record.thread_id
        && execution.thread_id == record.thread_id
        && view.thread.active_execution_id.as_ref() == Some(&record.execution_id)
        && view.thread.generation == record.thread_generation
        && view.thread.resource_revision == record.resource_revision
        && snapshot.execution_id == record.execution_id
        && ExecutionFence::from(snapshot) == record.execution_fence
        && snapshot.definition.route == record.route
        && snapshot.definition.revision == record.definition_revision
        && snapshot.status == record.status
        && snapshot
            .tasks
            .iter()
            .any(|task| task.status == TaskStatus::InFlight)
            == record.has_in_flight_task
        && snapshot
            .tasks
            .iter()
            .any(|task| task.status == TaskStatus::OutcomeUnknown)
            == record.has_outcome_unknown_task
}

/// Bounded, sequential preparation under the embedding's isolated startup
/// covenant. Discovery is observation; each candidate is renewed from one exact
/// ordinary view, trusted authority and the original typed recovery driver.
pub(crate) async fn prepare<S: ApprovalStore + 'static>(
    shared: OwnedShared<S>,
    resolver: Arc<dyn StartupPrincipalResolver>,
    limits: StartupPreparationLimits,
    capability: StartupCapability,
) -> CompletionCandidate {
    let mut progress = StartupProgress::default();
    let Some(owner) = epoch(&shared).await else {
        return candidate(progress, StartupIncompleteReason::Faulted);
    };
    let mut cursor = None;
    loop {
        if shared.supervisor.status().phase != crate::ShutdownPhase::Serving {
            return candidate(progress, StartupIncompleteReason::Shutdown);
        }
        if shared.fault.load(Ordering::Acquire) {
            return candidate(progress, StartupIncompleteReason::Faulted);
        }
        if progress.pages >= limits.max_pages {
            return candidate(progress, StartupIncompleteReason::LimitExceeded);
        }
        let page = joined(
            &shared,
            shared.clone().spawn(move |s| {
                if let Err(error) = s.store.readiness() {
                    record_store_failure(s, &error);
                    return Err(DiscoveryError::Unavailable);
                }
                if s.store.owner_generation() != owner {
                    s.fault.store(true, Ordering::Release);
                    return Err(DiscoveryError::OwnerConflict);
                }
                let page = s.store.discover_unfinished(cursor.as_ref());
                if matches!(
                    &page,
                    Err(DiscoveryError::UnsupportedSnapshot
                        | DiscoveryError::UnsupportedSchema
                        | DiscoveryError::OwnerConflict
                        | DiscoveryError::CorruptRecord
                        | DiscoveryError::Unavailable)
                ) {
                    s.fault.store(true, Ordering::Release);
                }
                page
            }),
        )
        .await;
        let Some(Ok(page)) = page else {
            return candidate(progress, StartupIncompleteReason::DiscoveryRefused);
        };
        // Reject custom Store contract violations before counters or iteration.
        if !page_shape(&page, owner) {
            return candidate(progress, StartupIncompleteReason::DiscoveryRefused);
        }
        if !increment(&mut progress.pages, 1, limits.max_pages.min(64))
            || !increment(&mut progress.scanned_rows, page.scanned_rows, 2048)
        {
            return candidate(progress, StartupIncompleteReason::LimitExceeded);
        }
        shared
            .supervisor
            .update_startup_progress(&capability, progress);
        for record in &page.records {
            if record.owner_relation != DiscoveryOwnerRelation::Prior {
                return candidate(progress, StartupIncompleteReason::CurrentOwnerExecution);
            }
            if progress.candidates >= limits.max_recoveries {
                return candidate(progress, StartupIncompleteReason::LimitExceeded);
            }
            if !increment(&mut progress.candidates, 1, limits.max_recoveries.min(256)) {
                return candidate(progress, StartupIncompleteReason::LimitExceeded);
            }
            shared
                .supervisor
                .update_startup_progress(&capability, progress);
            let thread_id = record.thread_id.clone();
            let view = joined(
                &shared,
                shared.clone().spawn(move |s| {
                    s.store.thread_view(&thread_id).map_err(|error| {
                        record_store_failure(s, &error);
                    })
                }),
            )
            .await;
            let Some(Ok(Some(view))) = view else {
                return candidate(progress, StartupIncompleteReason::RecoveryRefused);
            };
            if !matches_descriptor(&view, record) {
                return candidate(progress, StartupIncompleteReason::RecoveryRefused);
            }
            let Some(target) = RecoveryTarget::from_view(&view) else {
                return candidate(progress, StartupIncompleteReason::RecoveryRefused);
            };
            let Some(origin) = view.thread.origin.clone() else {
                return candidate(progress, StartupIncompleteReason::ResolutionRefused);
            };
            if origin.application.is_empty()
                || origin.tenant.is_empty()
                || origin.principal.is_empty()
            {
                return candidate(progress, StartupIncompleteReason::ResolutionRefused);
            }
            let trusted = resolver.clone();
            let captured_origin = origin.clone();
            let principal = joined(
                &shared,
                shared
                    .clone()
                    .spawn(move |_| trusted.resolve(&captured_origin)),
            )
            .await;
            let Some(Ok(principal)) = principal else {
                return candidate(progress, StartupIncompleteReason::ResolutionRefused);
            };
            if !crate::transport::valid_principal(&principal)
                || !crate::application::scope_allows(&shared, &principal)
                || principal.application != origin.application
                || principal.tenant != origin.tenant
                || principal.principal != origin.principal
            {
                return candidate(progress, StartupIncompleteReason::ResolutionRefused);
            }
            // Each worker has its own counted startup role and takes its own
            // mutation lease at the original durable recovery boundary.
            let Some(worker) = OwnedShared::register_startup(shared.shared_arc(), &capability)
            else {
                return candidate(progress, StartupIncompleteReason::Shutdown);
            };
            let admission = recover_owned(worker, principal, target, Some(view), false).await;
            match admission {
                Ok(result) => {
                    if !increment(&mut progress.admitted, 1, progress.candidates) {
                        return candidate(progress, StartupIncompleteReason::LimitExceeded);
                    }
                    let counted = match result.admission.disposition {
                        RecoveryDisposition::Runnable => {
                            increment(&mut progress.runnable, 1, progress.admitted)
                        }
                        RecoveryDisposition::Parked => {
                            increment(&mut progress.parked, 1, progress.admitted)
                        }
                        RecoveryDisposition::Blocked => {
                            increment(&mut progress.blocked, 1, progress.admitted)
                        }
                        RecoveryDisposition::Completed
                        | RecoveryDisposition::Cancelled
                        | RecoveryDisposition::Failed => {
                            increment(&mut progress.terminal, 1, progress.admitted)
                        }
                        RecoveryDisposition::AttemptLimit => {
                            shared
                                .supervisor
                                .update_startup_progress(&capability, progress);
                            return candidate(progress, StartupIncompleteReason::AttemptLimit);
                        }
                    };
                    if !counted {
                        return candidate(progress, StartupIncompleteReason::LimitExceeded);
                    }
                }
                Err(error) => {
                    // Count only confirmed bridge-return evidence. None never
                    // certifies rollback of an ambiguous failed transaction.
                    if error.adoption().is_some()
                        && !increment(&mut progress.admitted, 1, progress.candidates)
                    {
                        return candidate(progress, StartupIncompleteReason::LimitExceeded);
                    }
                    shared
                        .supervisor
                        .update_startup_progress(&capability, progress);
                    return candidate(
                        progress,
                        if error.reason() == RecoveryRefusal::AdmissionClosed {
                            StartupIncompleteReason::Shutdown
                        } else {
                            StartupIncompleteReason::RecoveryRefused
                        },
                    );
                }
            }
            shared
                .supervisor
                .update_startup_progress(&capability, progress);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            if epoch(&shared).await != Some(owner) {
                shared.fault.store(true, Ordering::Release);
                return candidate(progress, StartupIncompleteReason::Faulted);
            }
            if shared.fault.load(Ordering::Acquire) {
                return candidate(progress, StartupIncompleteReason::Faulted);
            }
            return CompletionCandidate {
                progress,
                reason: None,
            };
        }
    }
}
