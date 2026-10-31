use crate::approval::*;
use crate::*;
use b4_contract::*;
use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

mod confinement;

#[derive(Default)]
#[cfg_attr(test, derive(Debug, Clone, PartialEq))]
struct State {
    threads: BTreeMap<ThreadId, ThreadRecord>,
    executions: BTreeMap<ExecutionId, StoredExecution>,
    journal: BTreeMap<ExecutionId, Vec<JournalEntry>>,
    grants: BTreeMap<ExecutionId, Vec<GrantRecord>>,
    acceptances: BTreeMap<ExecutionId, Vec<ApprovalAcceptance>>,
}

#[derive(Default)]
pub struct MemoryStore {
    state: Mutex<State>,
    discovery_origin: crate::discovery::backend::DiscoveryOrigin,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>, StoreError> {
        self.state
            .lock()
            .map_err(|_| StoreError::Unavailable("memory_store_lock_poisoned".into()))
    }

    fn admit_inner(
        &self,
        thread_id: &ThreadId,
        expected_fence: Option<(&ThreadGeneration, u64)>,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        let mut state = self.lock()?;
        let thread = state
            .threads
            .get(thread_id)
            .ok_or(if expected_fence.is_some() {
                StoreError::RevisionConflict
            } else {
                StoreError::ThreadNotFound
            })?;
        if expected_fence.is_some_and(|(generation, revision)| {
            generation != &thread.generation || revision != thread.resource_revision
        }) {
            return Err(StoreError::RevisionConflict);
        }
        if state.executions.contains_key(&initial.execution_id) {
            return Err(StoreError::ExecutionExists);
        }
        if let Some(active_id) = &thread.active_execution_id {
            let active = state
                .executions
                .get(active_id)
                .ok_or_else(|| StoreError::Corrupt("active_execution_missing".into()))?;
            if !releases_slot(active.snapshot.status) {
                return Err(StoreError::ThreadBusy);
            }
        }
        validate_initial(&initial, self.owner_generation())?;
        let resource_revision = next_resource_revision(thread.resource_revision)?;
        let execution_id = initial.execution_id.clone();
        let stored = StoredExecution {
            thread_id: thread_id.clone(),
            snapshot: initial,
        };
        state.journal.insert(
            execution_id.clone(),
            vec![JournalEntry::from(&stored.snapshot)],
        );
        state
            .executions
            .insert(execution_id.clone(), stored.clone());
        let thread = state
            .threads
            .get_mut(thread_id)
            .expect("thread checked under lock");
        thread.active_execution_id = Some(execution_id);
        thread.resource_revision = resource_revision;
        Ok(stored)
    }
}

impl Store for MemoryStore {
    fn confinement_view_fenced(
        &self,
        target: &crate::RecoveryConfinementTarget,
    ) -> Result<ThreadView, crate::RecoveryConfinementBackendError> {
        confinement::capture(self, target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &crate::RecoveryConfinementTarget,
        reason: crate::RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<crate::RecoveryConfinementOutcome, crate::RecoveryConfinementBackendError> {
        confinement::mutate(self, original, target, reason, fresh_turn, facts)
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            kind: StoreKind::Memory,
            durable_runtime: false,
            fenced_cancellation: true,
            fenced_deletion: true,
        }
    }

    fn owner_generation(&self) -> u64 {
        1
    }

    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        validate_thread_seed(&seed)?;
        let mut state = self.lock()?;
        if state.threads.contains_key(&seed.id) {
            return Err(StoreError::ThreadExists);
        }
        let thread = ThreadRecord {
            id: seed.id,
            generation: generate_thread_generation()?,
            active_execution_id: None,
            created_at_ms: seed.created_at_ms,
            metadata: seed.metadata,
            origin: seed.origin,
            resource_revision: 0,
        };
        state.threads.insert(thread.id.clone(), thread.clone());
        Ok(thread)
    }

    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        Ok(self.lock()?.threads.get(id).cloned())
    }

    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        let state = self.lock()?;
        let Some(thread) = state.threads.get(id) else {
            return Ok(None);
        };
        let execution = thread
            .active_execution_id
            .as_ref()
            .map(|active| {
                let stored = state
                    .executions
                    .get(active)
                    .ok_or_else(|| StoreError::Corrupt("active_execution_missing".into()))?;
                if &stored.thread_id != id {
                    return Err(StoreError::Corrupt(
                        "active_execution_thread_mismatch".into(),
                    ));
                }
                Ok(stored.clone())
            })
            .transpose()?;
        Ok(Some(ThreadView {
            thread: thread.clone(),
            execution,
        }))
    }

    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        let mut state = self.lock()?;
        let thread = state
            .threads
            .get(&target.thread_id)
            .ok_or(StoreError::RevisionConflict)?
            .clone();
        if thread.id != target.thread_id {
            return Err(StoreError::Corrupt("thread_deletion_thread_index".into()));
        }
        let execution = thread
            .active_execution_id
            .as_ref()
            .map(|id| {
                state
                    .executions
                    .get(id)
                    .cloned()
                    .ok_or_else(|| StoreError::Corrupt("thread_deletion_active_missing".into()))
            })
            .transpose()?;
        let view = ThreadView { thread, execution };
        let executions: Vec<_> = state
            .executions
            .values()
            .filter(|execution| execution.thread_id == target.thread_id)
            .cloned()
            .collect();
        validate_thread_deletion(&view, target, &executions)?;
        let owned: BTreeMap<_, _> = executions
            .iter()
            .map(|execution| (&execution.snapshot.execution_id, execution))
            .collect();
        validate_deletion_records(&state, &view.thread, &owned)?;
        for id in owned.keys() {
            state.executions.remove(*id);
            state.journal.remove(*id);
            state.grants.remove(*id);
            state.acceptances.remove(*id);
        }
        state.threads.remove(&target.thread_id);
        Ok(())
    }

    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        let mut state = self.lock()?;
        let thread = state
            .threads
            .get(&target.thread_id)
            .ok_or(StoreError::RevisionConflict)?
            .clone();
        if thread.id != target.thread_id {
            return Err(StoreError::Corrupt("run_cancellation_thread_index".into()));
        }
        let execution =
            thread
                .active_execution_id
                .as_ref()
                .map(|id| {
                    state.executions.get(id).cloned().ok_or_else(|| {
                        StoreError::Corrupt("run_cancellation_active_missing".into())
                    })
                })
                .transpose()?;
        let view = ThreadView { thread, execution };
        let plan = validate_run_cancellation(&view, target, self.owner_generation(), facts)?;
        let owned_executions: Vec<_> = state
            .executions
            .values()
            .filter(|execution| execution.thread_id == target.thread_id)
            .cloned()
            .collect();
        let owned = owned_executions
            .iter()
            .map(|execution| (&execution.snapshot.execution_id, execution))
            .collect();
        validate_deletion_records(&state, &view.thread, &owned)?;
        let resource_revision = next_resource_revision(view.thread.resource_revision)?;
        let updated = StoredExecution {
            thread_id: target.thread_id.clone(),
            snapshot: plan.snapshot,
        };
        if let Some(grants) = state.grants.get_mut(&target.execution_id) {
            grants.retain(|grant| grant.consumed);
        }
        state
            .journal
            .get_mut(&target.execution_id)
            .expect("validated journal")
            .push(JournalEntry::from(&updated.snapshot));
        state
            .executions
            .insert(target.execution_id.clone(), updated.clone());
        state
            .threads
            .get_mut(&target.thread_id)
            .expect("thread checked under lock")
            .resource_revision = resource_revision;
        Ok(updated)
    }

    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        Ok(self.lock()?.executions.get(id).cloned())
    }

    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.admit_inner(thread_id, None, initial)
    }

    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.admit_inner(
            thread_id,
            Some((expected_thread_generation, expected_resource_revision)),
            initial,
        )
    }

    fn readiness(&self) -> Result<(), StoreError> {
        drop(self.lock()?);
        Ok(())
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        let owner = self.owner_generation();
        let after = self.discovery_origin.checked_after(owner, cursor)?;
        let state = self.state.lock().map_err(|_| DiscoveryError::Unavailable)?;
        crate::discovery::memory::discover(
            &self.discovery_origin,
            owner,
            &state.threads,
            &state.executions,
            after,
        )
    }

    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let mut state = self.lock()?;
        let current = state
            .executions
            .get(execution_id)
            .ok_or(StoreError::ExecutionNotFound)?
            .clone();
        let thread = state
            .threads
            .get(&current.thread_id)
            .ok_or_else(|| StoreError::Corrupt("execution_thread_missing".into()))?;
        if thread.active_execution_id.as_ref() != Some(execution_id) {
            return Err(StoreError::ExecutionNotActive);
        }
        reject_unrecorded_decisions(
            &current.snapshot,
            &plan.snapshot,
            state
                .grants
                .get(execution_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        )?;
        if !validate_commit(&current.snapshot, fence, &plan, self.owner_generation())? {
            return Ok(current);
        }
        let resource_revision = next_resource_revision(thread.resource_revision)?;
        let updated = StoredExecution {
            thread_id: current.thread_id,
            snapshot: plan.snapshot,
        };
        state
            .journal
            .entry(execution_id.clone())
            .or_default()
            .push(JournalEntry::from(&updated.snapshot));
        state
            .executions
            .insert(execution_id.clone(), updated.clone());
        state
            .threads
            .get_mut(&updated.thread_id)
            .expect("thread checked under lock")
            .resource_revision = resource_revision;
        Ok(updated)
    }

    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        Ok(self
            .lock()?
            .journal
            .get(execution_id)
            .cloned()
            .unwrap_or_default())
    }
}

/// Check both index and payload selection before removing any related record.
fn validate_deletion_records(
    state: &State,
    thread: &ThreadRecord,
    owned: &BTreeMap<&ExecutionId, &StoredExecution>,
) -> Result<(), StoreError> {
    let corrupt = || StoreError::Corrupt("thread_deletion_record_ownership".into());
    for (index, execution) in &state.executions {
        if (execution.thread_id == thread.id
            || owned.contains_key(&execution.snapshot.execution_id))
            && (index != &execution.snapshot.execution_id || execution.thread_id != thread.id)
        {
            return Err(corrupt());
        }
    }
    for (index, other) in &state.threads {
        if other
            .active_execution_id
            .as_ref()
            .is_some_and(|id| owned.contains_key(id))
            && (index != &thread.id || other.id != thread.id)
        {
            return Err(corrupt());
        }
    }
    for (index, entries) in &state.journal {
        let related = owned.contains_key(index)
            || entries
                .iter()
                .any(|entry| owned.contains_key(&entry.execution_id));
        if related
            && (!owned.contains_key(index)
                || entries.iter().any(|entry| &entry.execution_id != index))
        {
            return Err(corrupt());
        }
    }
    for (id, execution) in owned {
        let entries = state.journal.get(*id).ok_or_else(corrupt)?;
        if entries.first().is_none_or(|entry| entry.revision != 0)
            || entries.last() != Some(&JournalEntry::from(&execution.snapshot))
            || entries.iter().any(|entry| entry.owner_generation == 0)
            || entries.windows(2).any(|pair| {
                pair[0].revision.checked_add(1) != Some(pair[1].revision)
                    || pair[0].owner_generation > pair[1].owner_generation
                    || pair[0].updated_at_ms > pair[1].updated_at_ms
            })
        {
            return Err(corrupt());
        }
    }
    for (index, grants) in &state.grants {
        for grant in grants {
            if owned.contains_key(index)
                || grant.thread_id == thread.id
                || owned.contains_key(&grant.execution_id)
            {
                let execution = owned.get(index).ok_or_else(corrupt)?;
                let snapshot = &execution.snapshot;
                if &grant.execution_id != index
                    || grant.thread_id != thread.id
                    || grant.thread_generation != thread.generation
                    || thread.origin.as_ref() != Some(&grant.origin)
                    || grant.route != snapshot.definition.route
                    || grant.definition_revision != snapshot.definition.revision
                    || grant.task_parking_revision > snapshot.revision
                    || !snapshot.tasks.iter().any(|task| {
                        task.spec.id == grant.task_id && task.spec.effect_id == grant.effect_id
                    })
                    || validate_grant_record(grant).is_err()
                {
                    return Err(corrupt());
                }
            }
        }
    }
    for (index, acceptances) in &state.acceptances {
        for acceptance in acceptances {
            if owned.contains_key(index)
                || acceptance.thread_id == thread.id
                || owned.contains_key(&acceptance.execution_id)
            {
                let execution = owned.get(index).ok_or_else(corrupt)?;
                let snapshot = &execution.snapshot;
                if &acceptance.execution_id != index
                    || acceptance.thread_id != thread.id
                    || acceptance.thread_generation != thread.generation
                    || thread.origin.as_ref() != Some(&acceptance.context.actor)
                    || acceptance.definition_revision != snapshot.definition.revision
                    || acceptance.previous_revision >= snapshot.revision
                    || !snapshot.turn_history.contains(&acceptance.turn_id)
                    || acceptance.decisions.iter().any(|decision| {
                        !snapshot
                            .tasks
                            .iter()
                            .any(|task| task.spec.id == decision.task_id)
                    })
                    || validate_acceptance_record(acceptance).is_err()
                {
                    return Err(corrupt());
                }
            }
        }
    }
    Ok(())
}

impl ApprovalStore for MemoryStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        let state = self.lock()?;
        let Some(execution) = state.executions.get(id) else {
            return Ok(None);
        };
        let thread = state
            .threads
            .get(&execution.thread_id)
            .ok_or_else(|| StoreError::Corrupt("execution_thread_missing".into()))?;
        Ok(Some(ApprovalView {
            thread: thread.clone(),
            execution: execution.clone(),
            grants: state.grants.get(id).cloned().unwrap_or_default(),
            acceptances: state.acceptances.get(id).cloned().unwrap_or_default(),
        }))
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        let mut state = self.lock()?;
        let execution = state
            .executions
            .get(id)
            .ok_or(StoreError::ExecutionNotFound)?
            .clone();
        let thread = state
            .threads
            .get(&execution.thread_id)
            .ok_or_else(|| StoreError::Corrupt("execution_thread_missing".into()))?
            .clone();
        let view = ApprovalView {
            thread: thread.clone(),
            execution: execution.clone(),
            grants: state.grants.get(id).cloned().unwrap_or_default(),
            acceptances: state.acceptances.get(id).cloned().unwrap_or_default(),
        };
        let changes =
            validate_approval_change(&view, fence, &plan, &mutation, self.owner_generation())?;
        let resource_revision = next_resource_revision(thread.resource_revision)?;
        let updated = StoredExecution {
            thread_id: execution.thread_id,
            snapshot: plan.snapshot,
        };
        let grants = state.grants.entry(id.clone()).or_default();
        for grant in changes.grants {
            if let Some(existing) = grants
                .iter_mut()
                .find(|existing| existing.interrupt_id == grant.interrupt_id)
            {
                *existing = grant;
            } else {
                grants.push(grant);
            }
        }
        if let Some(acceptance) = changes.acceptance {
            state
                .acceptances
                .entry(id.clone())
                .or_default()
                .push(acceptance);
        }
        state
            .journal
            .entry(id.clone())
            .or_default()
            .push(JournalEntry::from(&updated.snapshot));
        state.executions.insert(id.clone(), updated.clone());
        state
            .threads
            .get_mut(&updated.thread_id)
            .expect("thread checked under lock")
            .resource_revision = resource_revision;
        Ok(updated)
    }
}

#[cfg(test)]
mod deletion_tests {
    use super::*;
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
    fn fixture() -> (
        Arc<MemoryStore>,
        ThreadDeletionTarget,
        ExecutionId,
        ThreadId,
    ) {
        let store = Arc::new(MemoryStore::new());
        let id = ThreadId::new("owned").unwrap();
        let foreign = ThreadId::new("foreign").unwrap();
        let context = ApprovalContext {
            actor: ThreadOrigin {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "alice".into(),
            },
            policy_revision: "policy-1".into(),
        };
        store
            .create_thread_with(ThreadSeed {
                id: id.clone(),
                created_at_ms: 1,
                metadata: Map::new(),
                origin: Some(context.actor.clone()),
            })
            .unwrap();
        store.create_thread(foreign.clone()).unwrap();
        let initial = b4_kernel::new_execution(
            ExecutionId::new("owned-execution").unwrap(),
            TurnId::new("turn").unwrap(),
            1,
            Definition {
                route: RouteKey::new("/delete#agent").unwrap(),
                revision: ExecutionRevision::new("v1").unwrap(),
                batches: vec![vec![TaskSpec {
                    id: TaskId::new("a").unwrap(),
                    effect_id: EffectId::new("effect").unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: true,
                }]],
                max_attempts: 8,
            },
            Map::new(),
            TransitionFacts { now_ms: 1 },
        )
        .unwrap();
        let current = store.admit(&id, initial).unwrap();
        let current = store
            .commit(
                &current.snapshot.execution_id,
                (&current.snapshot).into(),
                b4_kernel::transition(
                    &current.snapshot,
                    KernelCommand::Start,
                    TransitionFacts { now_ms: 2 },
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
        let parked = service
            .park(
                &context,
                &current.snapshot.execution_id,
                ApprovalFence {
                    execution: (&current.snapshot).into(),
                    resource_revision: store.thread(&id).unwrap().unwrap().resource_revision,
                },
                TaskId::new("a").unwrap(),
                InterruptId::new("interrupt").unwrap(),
                json!({"question":"Proceed?"}),
                1000,
                TransitionFacts { now_ms: 3 },
            )
            .unwrap();
        let view = store
            .approval_view(&parked.snapshot.execution_id)
            .unwrap()
            .unwrap();
        service
            .accept(
                &parked.snapshot.execution_id,
                ApprovalFence {
                    execution: (&parked.snapshot).into(),
                    resource_revision: view.thread.resource_revision,
                },
                ApprovalAcceptanceRequest {
                    context,
                    route: parked.snapshot.definition.route.clone(),
                    definition_revision: parked.snapshot.definition.revision.clone(),
                    turn_id: TurnId::new("accept-turn").unwrap(),
                    now_ms: 4,
                    decisions: vec![GrantDecision {
                        interrupt_id: InterruptId::new("interrupt").unwrap(),
                        decision: ApprovalDecision::Deny,
                        cancelled: false,
                        token: Some(view.grants[0].token.clone()),
                    }],
                },
            )
            .unwrap();
        let target =
            ThreadDeletionTarget::from_view(&store.thread_view(&id).unwrap().unwrap()).unwrap();
        (store, target, parked.snapshot.execution_id, foreign)
    }

    #[test]
    fn corruption_in_either_ownership_index_rejects_without_mutation() {
        for case in 0..19 {
            let (store, target, execution, foreign) = fixture();
            let other_execution = ExecutionId::new("foreign-execution").unwrap();
            let mut state = store.lock().unwrap();
            match case {
                0 => {
                    state.threads.get_mut(&foreign).unwrap().active_execution_id =
                        Some(execution.clone())
                }
                1 => {
                    state
                        .executions
                        .get_mut(&execution)
                        .unwrap()
                        .snapshot
                        .execution_id = other_execution.clone()
                }
                2 => {
                    state.journal.get_mut(&execution).unwrap()[0].execution_id =
                        other_execution.clone()
                }
                3 => {
                    let entry = state.journal[&execution][0].clone();
                    state.journal.insert(other_execution.clone(), vec![entry]);
                }
                4 => state.grants.get_mut(&execution).unwrap()[0].thread_id = foreign.clone(),
                5 => {
                    let grant = state.grants[&execution][0].clone();
                    state.grants.insert(other_execution.clone(), vec![grant]);
                }
                6 => state.acceptances.get_mut(&execution).unwrap()[0].thread_id = foreign.clone(),
                7 => {
                    let acceptance = state.acceptances[&execution][0].clone();
                    state
                        .acceptances
                        .insert(other_execution.clone(), vec![acceptance]);
                }
                8 => {
                    state.grants.get_mut(&execution).unwrap()[0].thread_generation =
                        ThreadGeneration::new("0".repeat(64)).unwrap()
                }
                9 => {
                    state.acceptances.get_mut(&execution).unwrap()[0].thread_generation =
                        ThreadGeneration::new("0".repeat(64)).unwrap()
                }
                10 => {
                    state.grants.get_mut(&execution).unwrap()[0]
                        .origin
                        .principal = "foreign".into()
                }
                11 => {
                    state.acceptances.get_mut(&execution).unwrap()[0]
                        .context
                        .actor
                        .principal = "foreign".into()
                }
                12 => {
                    state.grants.get_mut(&execution).unwrap()[0].definition_revision =
                        ExecutionRevision::new("foreign").unwrap()
                }
                13 => {
                    state.acceptances.get_mut(&execution).unwrap()[0].definition_revision =
                        ExecutionRevision::new("foreign").unwrap()
                }
                14 => {
                    state.journal.remove(&execution);
                }
                15 => {
                    state.journal.get_mut(&execution).unwrap().pop();
                }
                16 => state.journal.get_mut(&execution).unwrap()[1].revision += 1,
                17 => {
                    state.grants.get_mut(&execution).unwrap()[0].execution_id =
                        other_execution.clone()
                }
                _ => {
                    state.acceptances.get_mut(&execution).unwrap()[0].execution_id =
                        other_execution.clone()
                }
            }
            let before = state.clone();
            drop(state);
            assert!(
                matches!(
                    store.delete_thread_fenced(&target),
                    Err(StoreError::Corrupt(_))
                ),
                "case {case}"
            );
            assert_eq!(*store.lock().unwrap(), before, "case {case}");
        }
    }

    #[test]
    fn terminal_active_pointer_does_not_hide_unfinished_history() {
        let (store, target, execution, _) = fixture();
        let mut state = store.lock().unwrap();
        let current = &state.executions[&execution];
        let snapshot = b4_kernel::new_execution(
            ExecutionId::new("hidden-history").unwrap(),
            TurnId::new("hidden-turn").unwrap(),
            1,
            current.snapshot.definition.clone(),
            Map::new(),
            TransitionFacts { now_ms: 1 },
        )
        .unwrap();
        state.journal.insert(
            snapshot.execution_id.clone(),
            vec![JournalEntry::from(&snapshot)],
        );
        state.executions.insert(
            snapshot.execution_id.clone(),
            StoredExecution {
                thread_id: target.thread_id.clone(),
                snapshot,
            },
        );
        let before = state.clone();
        drop(state);
        assert_eq!(
            store.delete_thread_fenced(&target),
            Err(StoreError::Corrupt(
                "thread_deletion_history_unsettled".into()
            ))
        );
        assert_eq!(*store.lock().unwrap(), before);
    }

    #[test]
    fn successful_deletion_leaves_no_hidden_authority_records() {
        let (store, target, execution, foreign) = fixture();
        assert_eq!(store.delete_thread_fenced(&target), Ok(()));
        let state = store.lock().unwrap();
        assert!(!state.executions.contains_key(&execution));
        assert!(!state.journal.contains_key(&execution));
        assert!(!state.grants.contains_key(&execution));
        assert!(!state.acceptances.contains_key(&execution));
        assert!(!state.threads.contains_key(&target.thread_id));
        assert!(state.threads.contains_key(&foreign));
    }

    #[test]
    fn cancellation_checks_historical_authority_and_indexes_before_mutation() {
        for case in 0..10 {
            let (store, old_target, historical, foreign) = fixture();
            let previous = store.execution(&historical).unwrap().unwrap();
            let accepted = b4_kernel::new_execution(
                ExecutionId::new("cancellable").unwrap(),
                TurnId::new("cancellable-turn").unwrap(),
                1,
                previous.snapshot.definition.clone(),
                Map::new(),
                TransitionFacts { now_ms: 5 },
            )
            .unwrap();
            store.admit(&old_target.thread_id, accepted).unwrap();
            let target = RunCancellationTarget::from_view(
                &store.thread_view(&old_target.thread_id).unwrap().unwrap(),
            )
            .unwrap();
            let mut state = store.lock().unwrap();
            match case {
                0 => {
                    state.threads.get_mut(&foreign).unwrap().active_execution_id =
                        Some(historical.clone())
                }
                1 => {
                    let grant = state.grants[&historical][0].clone();
                    state
                        .grants
                        .insert(ExecutionId::new("foreign-index").unwrap(), vec![grant]);
                }
                2 => state.grants.get_mut(&historical).unwrap()[0].thread_id = foreign.clone(),
                3 => {
                    state.grants.get_mut(&historical).unwrap()[0]
                        .origin
                        .principal = "foreign".into()
                }
                4 => {
                    state.acceptances.get_mut(&historical).unwrap()[0].definition_revision =
                        ExecutionRevision::new("foreign").unwrap()
                }
                5 => {
                    let receipt = state.acceptances[&historical][0].clone();
                    state
                        .acceptances
                        .insert(ExecutionId::new("foreign-index").unwrap(), vec![receipt]);
                }
                6 => {
                    state.journal.get_mut(&historical).unwrap().pop();
                }
                7 => state.journal.get_mut(&historical).unwrap()[1].revision += 1,
                8 => state
                    .executions
                    .get_mut(&target.execution_id)
                    .unwrap()
                    .snapshot
                    .turn_history
                    .clear(),
                _ => {
                    state
                        .executions
                        .get_mut(&historical)
                        .unwrap()
                        .snapshot
                        .execution_id = ExecutionId::new("foreign-index").unwrap()
                }
            }
            let before = state.clone();
            drop(state);
            assert!(
                matches!(
                    store.cancel_run_fenced(&target, TransitionFacts { now_ms: 6 }),
                    Err(StoreError::Corrupt(_))
                ),
                "case {case}"
            );
            assert_eq!(*store.lock().unwrap(), before, "case {case}");
        }
    }

    #[test]
    fn cancellation_resource_overflow_preserves_every_record() {
        let (store, old_target, historical, _) = fixture();
        let previous = store.execution(&historical).unwrap().unwrap();
        let accepted = b4_kernel::new_execution(
            ExecutionId::new("cancellable").unwrap(),
            TurnId::new("cancellable-turn").unwrap(),
            1,
            previous.snapshot.definition.clone(),
            Map::new(),
            TransitionFacts { now_ms: 5 },
        )
        .unwrap();
        store.admit(&old_target.thread_id, accepted).unwrap();
        store
            .lock()
            .unwrap()
            .threads
            .get_mut(&old_target.thread_id)
            .unwrap()
            .resource_revision = u64::MAX;
        let target = RunCancellationTarget::from_view(
            &store.thread_view(&old_target.thread_id).unwrap().unwrap(),
        )
        .unwrap();
        let before = store.lock().unwrap().clone();
        assert!(matches!(
            store.cancel_run_fenced(&target, TransitionFacts { now_ms: 6 }),
            Err(StoreError::Unavailable(_))
        ));
        assert_eq!(*store.lock().unwrap(), before);
    }
}

#[cfg(test)]
#[path = "discovery/memory_tests.rs"]
mod discovery_tests;
