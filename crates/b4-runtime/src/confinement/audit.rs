use super::{backend, RecoveryConfinementBackendError as Error, RecoveryConfinementTarget};
use crate::{
    approval::{self, ApprovalAcceptance, GrantRecord},
    StoredExecution, ThreadRecord,
};
use b4_contract::{ApprovalDecision, ExecutionStatus, TaskStatus};
use std::collections::BTreeSet;

/// All borrowed inputs must have passed backend-specific byte/shape/aggregate
/// preflight before this semantic audit. This helper clones no record/view.
#[allow(clippy::too_many_arguments)] // One borrowed audited resource, without a cloned carrier.
pub fn resource(
    thread: &ThreadRecord,
    execution: &StoredExecution,
    grants: &[GrantRecord],
    acceptances: &[ApprovalAcceptance],
    journal: &backend::JournalEvidence,
    target: &RecoveryConfinementTarget,
    owner: u64,
    foreign_pointer: bool,
) -> Result<(), Error> {
    let snapshot = &execution.snapshot;
    if owner == 0 {
        return Err(Error::Unavailable);
    }
    backend::validate_origin(thread.origin.as_ref())?;
    snapshot
        .validate_version()
        .map_err(|_| Error::UnsupportedSnapshot)?;
    b4_kernel::validate_snapshot(snapshot).map_err(|_| Error::CorruptRecord)?;
    if execution.thread_id != thread.id
        || thread.active_execution_id.as_ref() != Some(&snapshot.execution_id)
        || foreign_pointer
    {
        return Err(Error::CorruptRecord);
    }
    if target.thread_id != thread.id
        || target.thread_generation != thread.generation
        || target.resource_revision != thread.resource_revision
        || target.execution_id != snapshot.execution_id
        || target.execution_fence != snapshot.into()
        || target.turn_id != snapshot.turn_id
    {
        return Err(Error::TargetChanged);
    }
    if !journal.matches(snapshot) {
        return Err(Error::CorruptRecord);
    }
    audit_authority(thread, execution, grants, acceptances, journal)?;
    if snapshot.owner_generation >= owner {
        return Err(Error::OwnerConflict);
    }
    if matches!(
        snapshot.status,
        ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
    ) {
        return Err(Error::Terminal);
    }
    Ok(())
}

fn audit_authority(
    thread: &ThreadRecord,
    execution: &StoredExecution,
    grants: &[GrantRecord],
    acceptances: &[ApprovalAcceptance],
    journal: &backend::JournalEvidence,
) -> Result<(), Error> {
    if grants.len() > backend::AUTHORITY_ROWS || acceptances.len() > backend::AUTHORITY_ROWS {
        return Err(Error::LimitExceeded);
    }
    let snapshot = &execution.snapshot;
    let mut interrupts = BTreeSet::new();
    let mut granted_tasks = BTreeSet::new();
    for grant in grants {
        approval::validate_grant_record(grant).map_err(|_| Error::CorruptRecord)?;
        let task = snapshot
            .tasks
            .iter()
            .find(|t| t.spec.id == grant.task_id)
            .ok_or(Error::CorruptRecord)?;
        let pending = task.interrupt.as_ref().ok_or(Error::CorruptRecord)?;
        let parking = journal
            .revision(grant.task_parking_revision)
            .ok_or(Error::CorruptRecord)?;
        if !task.spec.requires_approval
            || grant.thread_id != thread.id
            || grant.thread_generation != thread.generation
            || thread.origin.as_ref() != Some(&grant.origin)
            || grant.execution_id != snapshot.execution_id
            || grant.route != snapshot.definition.route
            || grant.definition_revision != snapshot.definition.revision
            || grant.effect_id != task.spec.effect_id
            || grant.interrupt_id != pending.id
            || grant.prompt != pending.prompt
            || grant.proposal_digest
                != approval::proposal_digest(task, &pending.prompt)
                    .map_err(|_| Error::CorruptRecord)?
            || grant.issued_at_ms != parking.updated_at_ms
            || !matches!(
                parking.status,
                ExecutionStatus::Running | ExecutionStatus::WaitingForApproval
            )
            || !interrupts.insert(&grant.interrupt_id)
            || !granted_tasks.insert(&grant.task_id)
            || !(grant.consumed
                || task.status == TaskStatus::WaitingForApproval
                || (task.status == TaskStatus::Cancelled && !task.approved))
        {
            return Err(Error::CorruptRecord);
        }
    }
    let mut turns = BTreeSet::new();
    let mut revisions = BTreeSet::new();
    let mut accepted_interrupts = BTreeSet::new();
    for acceptance in acceptances {
        approval::validate_acceptance_record(acceptance).map_err(|_| Error::CorruptRecord)?;
        let previous = journal
            .revision(acceptance.previous_revision)
            .ok_or(Error::CorruptRecord)?;
        let next_revision = acceptance
            .previous_revision
            .checked_add(1)
            .ok_or(Error::CorruptRecord)?;
        let next = journal
            .revision(next_revision)
            .ok_or(Error::CorruptRecord)?;
        let turn_position = snapshot
            .turn_history
            .iter()
            .position(|t| t == &acceptance.turn_id)
            .ok_or(Error::CorruptRecord)?;
        if acceptance.execution_id != snapshot.execution_id
            || acceptance.thread_id != thread.id
            || acceptance.thread_generation != thread.generation
            || thread.origin.as_ref() != Some(&acceptance.context.actor)
            || acceptance.definition_revision != snapshot.definition.revision
            || acceptance.previous_revision >= snapshot.revision
            || turn_position == 0
            || previous.status != ExecutionStatus::WaitingForApproval
            || previous.owner_generation != next.owner_generation
            || acceptance.accepted_at_ms != next.updated_at_ms
            || !turns.insert(&acceptance.turn_id)
            || !revisions.insert(acceptance.previous_revision)
        {
            return Err(Error::CorruptRecord);
        }
        for decision in &acceptance.decisions {
            let grant = grants
                .iter()
                .find(|g| g.interrupt_id == decision.interrupt_id && g.task_id == decision.task_id)
                .ok_or(Error::CorruptRecord)?;
            let task = snapshot
                .tasks
                .iter()
                .find(|t| t.spec.id == decision.task_id)
                .ok_or(Error::CorruptRecord)?;
            if !grant.consumed
                || grant.policy_revision != acceptance.context.policy_revision
                || grant.proposal_digest != decision.proposal_digest
                || grant.task_parking_revision != decision.task_parking_revision
                || grant.task_parking_revision > acceptance.previous_revision
                || grant.issued_at_ms > acceptance.accepted_at_ms
                || acceptance.accepted_at_ms >= grant.expires_at_ms
                || !accepted_interrupts.insert(&decision.interrupt_id)
                || (decision.decision == ApprovalDecision::Once
                    && (!task.approved || task.status == TaskStatus::Denied))
                || (decision.decision == ApprovalDecision::Deny
                    && (task.approved || task.status != TaskStatus::Denied))
            {
                return Err(Error::CorruptRecord);
            }
        }
        for other in acceptances {
            if other.previous_revision < acceptance.previous_revision {
                let other_position = snapshot
                    .turn_history
                    .iter()
                    .position(|t| t == &other.turn_id)
                    .ok_or(Error::CorruptRecord)?;
                if other_position >= turn_position
                    || other.accepted_at_ms > acceptance.accepted_at_ms
                {
                    return Err(Error::CorruptRecord);
                }
            }
        }
    }
    if grants
        .iter()
        .any(|g| g.consumed != accepted_interrupts.contains(&g.interrupt_id))
    {
        return Err(Error::CorruptRecord);
    }
    // Present contradictions above are corruption. Only absence can be the
    // indistinguishable legacy trusted profile, never inferred durable deletion.
    let mut unsupported = false;
    for task in snapshot.tasks.iter().filter(|t| t.spec.requires_approval) {
        let grant = grants.iter().find(|g| g.task_id == task.spec.id);
        if task.approved || task.status == TaskStatus::Denied {
            match grant {
                None => unsupported = true,
                Some(g) if g.consumed && accepted_interrupts.contains(&g.interrupt_id) => {}
                Some(_) => return Err(Error::CorruptRecord),
            }
        } else if task.status == TaskStatus::WaitingForApproval {
            match grant {
                None => unsupported = true,
                Some(g) if !g.consumed => {}
                Some(_) => return Err(Error::CorruptRecord),
            }
        } else if task.status == TaskStatus::Ready {
            if task.interrupt.is_some() || task.attempt_id.is_some() {
                unsupported = true;
            }
        } else if task.status != TaskStatus::Cancelled {
            unsupported = true;
        }
        // Unapproved Cancelled tasks may retain an unconsumed grant, or have
        // no grant after fenced cancellation; their original interrupt stays.
    }
    if unsupported {
        return Err(Error::UnsupportedAuthorityProfile);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::*;
    use crate::{MemoryStore, Store, ThreadOrigin, ThreadSeed, ThreadView};
    use b4_contract::*;
    use serde_json::{json, Map};
    use std::sync::Arc;

    struct Allow;
    impl ApprovalAuthority for Allow {
        fn authorize(
            &self,
            _: &ApprovalContext,
            _: ApprovalOperation,
            _: &ApprovalResource,
        ) -> Result<(), ApprovalError> {
            Ok(())
        }
    }
    fn context() -> ApprovalContext {
        ApprovalContext {
            actor: ThreadOrigin {
                application: "helper-app".into(),
                tenant: "helper-tenant".into(),
                principal: "helper-principal".into(),
            },
            policy_revision: "helper-policy".into(),
        }
    }
    fn started() -> Arc<MemoryStore> {
        let store = Arc::new(MemoryStore::new());
        let thread = ThreadId::new("authority-thread").unwrap();
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 1,
                metadata: Map::new(),
                origin: Some(context().actor),
            })
            .unwrap();
        let initial = b4_kernel::new_execution(
            ExecutionId::new("authority-execution").unwrap(),
            TurnId::new("authority-turn").unwrap(),
            1,
            Definition {
                route: RouteKey::new("/authority#workflow").unwrap(),
                revision: ExecutionRevision::new("authority-v1").unwrap(),
                batches: vec![vec![TaskSpec {
                    id: TaskId::new("authority-task").unwrap(),
                    effect_id: EffectId::new("authority-effect").unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: true,
                }]],
                max_attempts: 3,
            },
            Map::new(),
            TransitionFacts { now_ms: 1 },
        )
        .unwrap();
        let admitted = store.admit(&thread, initial).unwrap();
        store
            .commit(
                &admitted.snapshot.execution_id,
                (&admitted.snapshot).into(),
                b4_kernel::transition(
                    &admitted.snapshot,
                    KernelCommand::Start,
                    TransitionFacts { now_ms: 2 },
                )
                .unwrap(),
            )
            .unwrap();
        store
    }
    fn view(store: &MemoryStore) -> ApprovalView {
        store
            .approval_view(&ExecutionId::new("authority-execution").unwrap())
            .unwrap()
            .unwrap()
    }
    fn fence(view: &ApprovalView) -> ApprovalFence {
        ApprovalFence {
            execution: (&view.execution.snapshot).into(),
            resource_revision: view.thread.resource_revision,
        }
    }
    fn pending() -> (Arc<MemoryStore>, ApprovalService<Arc<MemoryStore>, Allow>) {
        let store = started();
        let service = ApprovalService::with_config(
            store.clone(),
            Allow,
            ApprovalServiceConfig::VolatileFixture,
        )
        .unwrap();
        service
            .park(
                &context(),
                &ExecutionId::new("authority-execution").unwrap(),
                fence(&view(&store)),
                TaskId::new("authority-task").unwrap(),
                InterruptId::new("authority-interrupt").unwrap(),
                json!({"question":"fixture"}),
                100,
                TransitionFacts { now_ms: 3 },
            )
            .unwrap();
        (store, service)
    }
    fn audited(view: &ApprovalView, journal: &[crate::JournalEntry]) -> Result<(), Error> {
        // These are small source-generated typed rows. Perform the shared shape
        // obligations before testing the semantic helper directly.
        let mut budget = backend::AuditBudget::default();
        backend::measure_thread(&view.thread, &mut budget)?;
        budget.borrowed(&view.execution.snapshot, backend::SNAPSHOT_BYTES)?;
        for grant in &view.grants {
            budget.borrowed(grant, backend::AUTHORITY_BYTES)?;
        }
        for acceptance in &view.acceptances {
            budget.borrowed(acceptance, backend::AUTHORITY_BYTES)?;
        }
        let mut stream =
            backend::JournalAudit::new(&view.execution.snapshot, &view.grants, &view.acceptances)?;
        for entry in journal {
            budget.borrowed(entry, backend::JOURNAL_BYTES)?;
            stream.observe(entry.clone())?;
        }
        let evidence = stream.finish()?;
        let ordinary = ThreadView {
            thread: view.thread.clone(),
            execution: Some(view.execution.clone()),
        };
        let target = RecoveryConfinementTarget::from_view(&ordinary)?;
        resource(
            &view.thread,
            &view.execution,
            &view.grants,
            &view.acceptances,
            &evidence,
            &target,
            1,
            false,
        )
    }
    fn entries(store: &MemoryStore) -> Vec<crate::JournalEntry> {
        store
            .journal(&ExecutionId::new("authority-execution").unwrap())
            .unwrap()
    }

    // Ordinary Cancel requires Running. A separately dispatched sibling keeps
    // the approval park Running; the four stable-park fixtures stay unchanged.
    fn cancellation_pending() -> (Arc<MemoryStore>, ApprovalService<Arc<MemoryStore>, Allow>) {
        let store = Arc::new(MemoryStore::new());
        let thread = ThreadId::new("authority-thread").unwrap();
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 1,
                metadata: Map::new(),
                origin: Some(context().actor),
            })
            .unwrap();
        let initial = b4_kernel::new_execution(
            ExecutionId::new("authority-execution").unwrap(),
            TurnId::new("authority-turn").unwrap(),
            1,
            Definition {
                route: RouteKey::new("/authority#workflow").unwrap(),
                revision: ExecutionRevision::new("authority-v1").unwrap(),
                batches: vec![vec![
                    TaskSpec {
                        id: TaskId::new("authority-task").unwrap(),
                        effect_id: EffectId::new("authority-effect").unwrap(),
                        recovery: RecoveryPolicy::Pure,
                        requires_approval: true,
                    },
                    TaskSpec {
                        id: TaskId::new("authority-sibling").unwrap(),
                        effect_id: EffectId::new("authority-sibling-effect").unwrap(),
                        recovery: RecoveryPolicy::Pure,
                        requires_approval: false,
                    },
                ]],
                max_attempts: 3,
            },
            Map::new(),
            TransitionFacts { now_ms: 1 },
        )
        .unwrap();
        let admitted = store.admit(&thread, initial).unwrap();
        let started = store
            .commit(
                &admitted.snapshot.execution_id,
                (&admitted.snapshot).into(),
                b4_kernel::transition(
                    &admitted.snapshot,
                    KernelCommand::Start,
                    TransitionFacts { now_ms: 2 },
                )
                .unwrap(),
            )
            .unwrap();
        store
            .commit(
                &started.snapshot.execution_id,
                (&started.snapshot).into(),
                b4_kernel::transition(
                    &started.snapshot,
                    KernelCommand::Dispatch {
                        task_id: TaskId::new("authority-sibling").unwrap(),
                        attempt_id: AttemptId::new("authority-sibling-attempt").unwrap(),
                    },
                    TransitionFacts { now_ms: 3 },
                )
                .unwrap(),
            )
            .unwrap();
        let service = ApprovalService::with_config(
            store.clone(),
            Allow,
            ApprovalServiceConfig::VolatileFixture,
        )
        .unwrap();
        service
            .park(
                &context(),
                &ExecutionId::new("authority-execution").unwrap(),
                fence(&view(&store)),
                TaskId::new("authority-task").unwrap(),
                InterruptId::new("authority-interrupt").unwrap(),
                json!({"question":"fixture"}),
                100,
                TransitionFacts { now_ms: 4 },
            )
            .unwrap();
        assert_eq!(
            view(&store).execution.snapshot.status,
            ExecutionStatus::Running
        );
        (store, service)
    }

    #[test]
    fn source_issued_pending_and_once_deny_evidence_reach_current_owner_refusal() {
        for decision in [
            None,
            Some(ApprovalDecision::Once),
            Some(ApprovalDecision::Deny),
        ] {
            let (store, service) = pending();
            if let Some(decision) = decision {
                let record = view(&store);
                service
                    .accept(
                        &record.execution.snapshot.execution_id,
                        fence(&record),
                        ApprovalAcceptanceRequest {
                            context: context(),
                            route: record.execution.snapshot.definition.route.clone(),
                            definition_revision: record
                                .execution
                                .snapshot
                                .definition
                                .revision
                                .clone(),
                            turn_id: TurnId::new("authority-accepted").unwrap(),
                            now_ms: 4,
                            decisions: vec![GrantDecision {
                                interrupt_id: record.grants[0].interrupt_id.clone(),
                                decision,
                                cancelled: false,
                                token: Some(record.grants[0].token.clone()),
                            }],
                        },
                    )
                    .unwrap();
            }
            let record = view(&store);
            assert_eq!(
                audited(&record, &entries(&store)),
                Err(Error::OwnerConflict)
            );
            let original = store.thread_view(&record.thread.id).unwrap().unwrap();
            let target = RecoveryConfinementTarget::from_view(&original).unwrap();
            assert_eq!(
                store.confinement_view_fenced(&target).unwrap_err(),
                Error::OwnerConflict
            );
            assert_eq!(
                store.thread_view(&record.thread.id).unwrap().unwrap(),
                original
            );
        }
    }
    #[test]
    fn source_cancelled_unapproved_retained_grant_is_not_a_corruption() {
        let (store, _service) = cancellation_pending();
        let record = view(&store);
        let next = b4_kernel::transition(
            &record.execution.snapshot,
            KernelCommand::Cancel {
                turn_id: record.execution.snapshot.turn_id.clone(),
                owner_generation: 1,
            },
            TransitionFacts { now_ms: 5 },
        )
        .unwrap();
        store
            .commit(
                &record.execution.snapshot.execution_id,
                (&record.execution.snapshot).into(),
                next,
            )
            .unwrap();
        let cancelled = view(&store);
        assert_eq!(
            cancelled.execution.snapshot.tasks[0].status,
            TaskStatus::Cancelled
        );
        assert!(!cancelled.grants[0].consumed);
        assert!(cancelled.acceptances.is_empty());
        assert_eq!(
            audited(&cancelled, &entries(&store)),
            Err(Error::OwnerConflict)
        );
    }
    #[test]
    fn missing_evidence_profile_refuses_after_all_present_contradictions() {
        let (store, service) = pending();
        let mut record = view(&store);
        record.grants.clear();
        assert_eq!(
            audited(&record, &entries(&store)),
            Err(Error::UnsupportedAuthorityProfile)
        );
        let record = view(&store);
        service
            .accept(
                &record.execution.snapshot.execution_id,
                fence(&record),
                ApprovalAcceptanceRequest {
                    context: context(),
                    route: record.execution.snapshot.definition.route.clone(),
                    definition_revision: record.execution.snapshot.definition.revision.clone(),
                    turn_id: TurnId::new("authority-accepted").unwrap(),
                    now_ms: 4,
                    decisions: vec![GrantDecision {
                        interrupt_id: record.grants[0].interrupt_id.clone(),
                        decision: ApprovalDecision::Once,
                        cancelled: false,
                        token: Some(record.grants[0].token.clone()),
                    }],
                },
            )
            .unwrap();
        let mut record = view(&store);
        record.grants.clear();
        assert_eq!(
            audited(&record, &entries(&store)),
            Err(Error::CorruptRecord)
        ); // Present acceptance cannot refer to a missing grant.
        record.acceptances.clear();
        assert_eq!(
            audited(&record, &entries(&store)),
            Err(Error::UnsupportedAuthorityProfile)
        );
    }
    #[test]
    fn malformed_present_binding_and_consumption_rows_are_corrupt() {
        let (store, _service) = pending();
        let record = view(&store);
        for kind in 0..4 {
            let mut bad = record.clone();
            match kind {
                0 => bad.grants[0].proposal_digest = "0".repeat(64),
                1 => bad.grants[0].task_parking_revision = 1,
                2 => bad.grants[0].consumed = true,
                _ => bad.grants.push(bad.grants[0].clone()),
            }
            assert_eq!(audited(&bad, &entries(&store)), Err(Error::CorruptRecord));
        }
    }
    #[test]
    fn journal_retains_only_unique_authority_revisions() {
        let (store, service) = pending();
        let record = view(&store);
        service
            .accept(
                &record.execution.snapshot.execution_id,
                fence(&record),
                ApprovalAcceptanceRequest {
                    context: context(),
                    route: record.execution.snapshot.definition.route.clone(),
                    definition_revision: record.execution.snapshot.definition.revision.clone(),
                    turn_id: TurnId::new("authority-accepted").unwrap(),
                    now_ms: 4,
                    decisions: vec![GrantDecision {
                        interrupt_id: record.grants[0].interrupt_id.clone(),
                        decision: ApprovalDecision::Once,
                        cancelled: false,
                        token: Some(record.grants[0].token.clone()),
                    }],
                },
            )
            .unwrap();
        let record = view(&store);
        let mut stream = backend::JournalAudit::new(
            &record.execution.snapshot,
            &record.grants,
            &record.acceptances,
        )
        .unwrap();
        for entry in entries(&store) {
            stream.observe(entry).unwrap();
        }
        let evidence = stream.finish().unwrap();
        let previous = record.acceptances[0].previous_revision;
        assert!(evidence.revision(previous).is_some());
        assert!(evidence.revision(previous + 1).is_some());
        assert!(evidence.revision(0).is_none());
        assert_eq!(
            audited(&record, &entries(&store)),
            Err(Error::OwnerConflict)
        );
    }
}
