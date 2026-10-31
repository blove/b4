//! Bounded sequential opt-in startup: exact bootstrap, audited applicability,
//! separate confirmed confinement evidence, and original typed recovery only.

use super::{model::Counters, ownership::Arguments};
use crate::{
    confinement::startup::{self as startup_adapter, StartupCapture},
    lifecycle::{record_store_failure, OwnedShared},
    recovery::{recover_owned, RecoveryDisposition, RecoveryRefusal, RecoveryTarget},
    startup::{
        coordinator::{epoch, increment, joined, matches_descriptor, page_shape},
        CompletionCandidate, StartupIncompleteReason, StartupPreparationLimits, StartupProgress,
    },
    supervisor::StartupCapability,
    RecoveryConfinementBackendError, RecoveryConfinementOutcome, RecoveryConfinementRefusal,
    RecoveryConfinementTarget,
};
use b4_runtime::{approval::ApprovalStore, DiscoveryError, DiscoveryOwnerRelation};
use std::sync::{atomic::Ordering, Arc};

fn candidate(progress: StartupProgress, reason: StartupIncompleteReason) -> CompletionCandidate {
    CompletionCandidate {
        progress,
        reason: Some(reason),
    }
}

pub(super) async fn prepare<S: ApprovalStore + 'static>(
    shared: OwnedShared<S>,
    arguments: &Arguments,
    limits: StartupPreparationLimits,
    capability: StartupCapability,
    counters: Arc<Counters>,
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
            let Some(recovery_target) = RecoveryTarget::from_view(&view) else {
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
            let trusted = arguments.resolver().clone();
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
            // Move the complete inherited bootstrap once. The counted Job
            // receives the only second pure alias, never a full-view clone.
            let confinement_target = match RecoveryConfinementTarget::from_view(&view) {
                Ok(target) => target,
                Err(
                    RecoveryConfinementBackendError::CorruptRecord
                    | RecoveryConfinementBackendError::Unavailable,
                ) => {
                    shared.fault.store(true, Ordering::Release);
                    return candidate(progress, StartupIncompleteReason::Faulted);
                }
                Err(_) => return candidate(progress, StartupIncompleteReason::RecoveryRefused),
            };
            let original = Arc::new(view);
            let confinement = startup_adapter::confine(
                &shared,
                &capability,
                StartupCapture {
                    target: confinement_target,
                    original: original.clone(),
                    principal: principal.clone(),
                },
                arguments.resolver(),
                arguments.authority(),
            )
            .await;
            // Publish only returned verified evidence before any later await
            // or fallible trusted argument/final Shared destruction. An error
            // without such a receipt cannot certify rollback or add credit.
            if counters
                .record(&confinement, progress.candidates, limits.max_recoveries)
                .is_err()
            {
                shared.fault.store(true, Ordering::Release);
                return candidate(progress, StartupIncompleteReason::Faulted);
            }
            match confinement {
                Ok(
                    RecoveryConfinementOutcome::Confined(_)
                    | RecoveryConfinementOutcome::AlreadyBlocked,
                ) => {
                    // Confinement never supplies old typed admission counts.
                    shared
                        .supervisor
                        .update_startup_progress(&capability, progress);
                    continue;
                }
                Err(error)
                    if error.reason() == RecoveryConfinementRefusal::NotApplicable
                        && error.confirmed_receipt().is_none() => {}
                Err(error) => {
                    return candidate(
                        progress,
                        if error.reason() == RecoveryConfinementRefusal::AdmissionClosed {
                            StartupIncompleteReason::Shutdown
                        } else {
                            StartupIncompleteReason::RecoveryRefused
                        },
                    );
                }
            }
            // Join has retired the Job's pure alias plus claim/permit. Only
            // NotApplicable may reach the original typed recovery driver.
            let original = match Arc::try_unwrap(original) {
                Ok(view) => view,
                Err(_) => {
                    shared.fault.store(true, Ordering::Release);
                    return candidate(progress, StartupIncompleteReason::Faulted);
                }
            };
            let Some(worker) = OwnedShared::register_startup(shared.shared_arc(), &capability)
            else {
                return candidate(progress, StartupIncompleteReason::Shutdown);
            };
            let admission =
                recover_owned(worker, principal, recovery_target, Some(original), false).await;
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
