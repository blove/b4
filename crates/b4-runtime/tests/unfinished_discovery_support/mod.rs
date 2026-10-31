use b4_contract::{
    Definition, EffectId, ExecutionId, ExecutionRevision, ExecutionStatus, RecoveryPolicy,
    RouteKey, TaskId, TaskSpec, ThreadId, TransitionFacts, TurnId,
};
use b4_runtime::{
    approval::{ApprovalStore, ApprovalView},
    DiscoveryOwnerRelation, ExecutionFence, JournalEntry, Store, StoredExecution, ThreadOrigin,
    ThreadSeed, ThreadView, UnfinishedDiscoveryPage,
};
use serde_json::{json, Map};

pub mod scenarios;

pub struct AcceptedFixture {
    pub view: ThreadView,
    pub authority: ApprovalView,
    pub journal: Vec<JournalEntry>,
}

impl AcceptedFixture {
    pub fn admit(store: &impl ApprovalStore) -> Self {
        Store::readiness(store).unwrap();
        let thread_id = ThreadId::new("discovery-thread").unwrap();
        let seed = ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 3,
            metadata: json!({"note": "private discovery fixture", "nested": [1, true]})
                .as_object()
                .unwrap()
                .clone(),
            origin: Some(ThreadOrigin {
                application: "discovery-application".into(),
                tenant: "discovery-tenant".into(),
                principal: "discovery-principal".into(),
            }),
        };
        let created = store.create_thread_with(seed.clone()).unwrap();
        assert_eq!(created.id, seed.id);
        assert_eq!(created.origin, seed.origin);
        assert_eq!(created.metadata, seed.metadata);
        assert_eq!(created.created_at_ms, 3);
        assert_eq!(created.resource_revision, 0);
        assert_eq!(created.active_execution_id, None);
        let owner = store.owner_generation();
        assert!(owner > 0);
        let initial = b4_kernel::new_execution(
            ExecutionId::new("discovery-execution").unwrap(),
            TurnId::new("discovery-turn").unwrap(),
            owner,
            Definition {
                route: RouteKey::new("/discovery#workflow").unwrap(),
                revision: ExecutionRevision::new("discovery-definition-1").unwrap(),
                batches: vec![vec![TaskSpec {
                    id: TaskId::new("discovery-task").unwrap(),
                    effect_id: EffectId::new("discovery-effect").unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: false,
                }]],
                max_attempts: 3,
            },
            Map::new(),
            TransitionFacts { now_ms: 7 },
        )
        .unwrap();
        b4_kernel::validate_snapshot(&initial).unwrap();
        assert_eq!(initial.status, ExecutionStatus::Accepted);
        assert_eq!(initial.revision, 0);
        assert_eq!(initial.owner_generation, owner);
        assert!(initial.tasks.is_empty());
        let expected_execution = StoredExecution {
            thread_id: thread_id.clone(),
            snapshot: initial.clone(),
        };
        assert_eq!(
            store
                .admit_fenced(&thread_id, &created.generation, 0, initial.clone())
                .unwrap(),
            expected_execution
        );
        let mut expected_thread = created;
        expected_thread.active_execution_id = Some(initial.execution_id.clone());
        expected_thread.resource_revision = 1;
        let view = ThreadView {
            thread: expected_thread.clone(),
            execution: Some(expected_execution.clone()),
        };
        let authority = ApprovalView {
            thread: expected_thread,
            execution: expected_execution,
            grants: vec![],
            acceptances: vec![],
        };
        let journal = vec![JournalEntry {
            execution_id: initial.execution_id,
            revision: 0,
            owner_generation: owner,
            status: ExecutionStatus::Accepted,
            updated_at_ms: 7,
        }];
        let fixture = Self {
            view,
            authority,
            journal,
        };
        fixture.assert_unchanged(store);
        fixture
    }

    pub fn assert_unchanged(&self, store: &impl ApprovalStore) {
        let execution = &self.authority.execution;
        assert_eq!(
            store.thread_view(&self.view.thread.id).unwrap(),
            Some(self.view.clone())
        );
        assert_eq!(
            store.execution(&execution.snapshot.execution_id).unwrap(),
            Some(execution.clone())
        );
        assert_eq!(
            store
                .approval_view(&execution.snapshot.execution_id)
                .unwrap(),
            Some(self.authority.clone())
        );
        assert_eq!(
            store.journal(&execution.snapshot.execution_id).unwrap(),
            self.journal
        );
    }

    pub fn assert_page(
        &self,
        page: &UnfinishedDiscoveryPage,
        current_owner: u64,
        owner_relation: DiscoveryOwnerRelation,
    ) {
        assert_eq!(page.owner_generation, current_owner);
        assert_eq!(page.scanned_rows, 1);
        assert_eq!(page.records.len(), 1);
        assert!(page.next_cursor.is_none());
        let record = &page.records[0];
        let snapshot = &self.authority.execution.snapshot;
        assert_eq!(record.thread_id, self.view.thread.id);
        assert_eq!(record.thread_generation, self.view.thread.generation);
        assert_eq!(record.resource_revision, 1);
        assert_eq!(record.execution_id, snapshot.execution_id);
        assert_eq!(
            record.execution_fence,
            ExecutionFence {
                revision: 0,
                owner_generation: snapshot.owner_generation,
            }
        );
        assert_eq!(record.route, snapshot.definition.route);
        assert_eq!(record.definition_revision, snapshot.definition.revision);
        assert_eq!(record.status, ExecutionStatus::Accepted);
        assert_eq!(record.owner_relation, owner_relation);
        assert!(!record.has_in_flight_task);
        assert!(!record.has_outcome_unknown_task);
    }
}
