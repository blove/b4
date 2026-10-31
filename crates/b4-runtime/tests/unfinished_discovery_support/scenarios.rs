use b4_contract::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Map, Value};

#[derive(Clone, Copy)]
pub enum State {
    Accepted,
    Ready,
    Waiting,
    InFlight,
    Cancelling,
    Blocked,
    Unknown,
    Completed,
    Failed,
    Cancelled,
}

pub fn initial(
    store: &impl Store,
    name: &str,
    approval: bool,
    values: Map<String, Value>,
) -> Snapshot {
    b4_kernel::new_execution(
        ExecutionId::new(name).unwrap(),
        TurnId::new(format!("{name}-turn")).unwrap(),
        store.owner_generation(),
        definition(approval),
        values,
        TransitionFacts { now_ms: 7 },
    )
    .unwrap()
}

pub fn definition(approval: bool) -> Definition {
    Definition {
        route: RouteKey::new("/discovery#workflow").unwrap(),
        revision: ExecutionRevision::new("discovery-definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("discovery-task").unwrap(),
            effect_id: EffectId::new("discovery-effect").unwrap(),
            recovery: RecoveryPolicy::Unsafe,
            requires_approval: approval,
        }]],
        max_attempts: 3,
    }
}

pub fn actor() -> ThreadOrigin {
    ThreadOrigin {
        application: "discovery-application".into(),
        tenant: "discovery-tenant".into(),
        principal: "discovery-principal".into(),
    }
}

pub fn admit(
    store: &impl Store,
    name: &str,
    state: State,
    values: Map<String, Value>,
) -> StoredExecution {
    let thread = store
        .create_thread_with(ThreadSeed {
            id: ThreadId::new(format!("thread-{name}")).unwrap(),
            created_at_ms: 3,
            metadata: Map::from_iter([("private".into(), json!({"nested": [true, 1]}))]),
            origin: Some(actor()),
        })
        .unwrap();
    let waiting = matches!(state, State::Waiting);
    let snapshot = initial(store, name, waiting, values);
    let mut current = store
        .admit_fenced(&thread.id, &thread.generation, 0, snapshot)
        .unwrap();
    if matches!(state, State::Accepted) {
        return current;
    }
    current = command(store, current, KernelCommand::Start);
    if waiting {
        return command(
            store,
            current,
            KernelCommand::Park {
                task_id: TaskId::new("discovery-task").unwrap(),
                interrupt_id: InterruptId::new(format!("{name}-interrupt")).unwrap(),
                prompt: json!({"private": "approval fixture"}),
            },
        );
    }
    if matches!(state, State::Ready) {
        return current;
    }
    if matches!(state, State::Cancelled) {
        let cancel = KernelCommand::Cancel {
            turn_id: current.snapshot.turn_id.clone(),
            owner_generation: current.snapshot.owner_generation,
        };
        return command(store, current, cancel);
    }
    if matches!(state, State::Blocked) {
        // A trusted Store accepts any valid fenced CommitPlan. This fixture
        // records a revision-blocked state; it does not simulate recovery.
        let mut snapshot = current.snapshot.clone();
        snapshot.status = ExecutionStatus::RecoveryBlocked;
        snapshot.failure = Some("execution_revision_unavailable".into());
        return replacement(store, current, snapshot);
    }
    current = command(
        store,
        current,
        KernelCommand::Dispatch {
            task_id: TaskId::new("discovery-task").unwrap(),
            attempt_id: AttemptId::new(format!("{name}-attempt")).unwrap(),
        },
    );
    match state {
        State::InFlight => current,
        State::Cancelling => {
            let cancel = KernelCommand::Cancel {
                turn_id: current.snapshot.turn_id.clone(),
                owner_generation: current.snapshot.owner_generation,
            };
            command(store, current, cancel)
        }
        State::Unknown => {
            let mut snapshot = current.snapshot.clone();
            snapshot.status = ExecutionStatus::RecoveryBlocked;
            snapshot.failure = Some("effect_outcome_unknown".into());
            snapshot.tasks[0].status = TaskStatus::OutcomeUnknown;
            replacement(store, current, snapshot)
        }
        State::Completed => {
            let attempt_id = current.snapshot.tasks[0].attempt_id.clone().unwrap();
            command(
                store,
                current,
                KernelCommand::Complete {
                    task_id: TaskId::new("discovery-task").unwrap(),
                    attempt_id,
                    result: TaskResult {
                        output: json!("private result"),
                        writes: vec![],
                    },
                },
            )
        }
        State::Failed => {
            let attempt_id = current.snapshot.tasks[0].attempt_id.clone().unwrap();
            command(
                store,
                current,
                KernelCommand::FailAttempt {
                    task_id: TaskId::new("discovery-task").unwrap(),
                    attempt_id,
                    failure: "fixed_fixture_failure".into(),
                },
            )
        }
        _ => unreachable!("handled fixture state"),
    }
}

pub fn command(
    store: &impl Store,
    current: StoredExecution,
    command: KernelCommand,
) -> StoredExecution {
    let plan = b4_kernel::transition(
        &current.snapshot,
        command,
        TransitionFacts {
            now_ms: current.snapshot.updated_at_ms + 1,
        },
    )
    .unwrap();
    store
        .commit(
            &current.snapshot.execution_id,
            ExecutionFence::from(&current.snapshot),
            plan,
        )
        .unwrap()
}

fn replacement(
    store: &impl Store,
    current: StoredExecution,
    mut snapshot: Snapshot,
) -> StoredExecution {
    snapshot.revision += 1;
    snapshot.updated_at_ms += 1;
    b4_kernel::validate_snapshot(&snapshot).unwrap();
    store
        .commit(
            &current.snapshot.execution_id,
            ExecutionFence::from(&current.snapshot),
            CommitPlan {
                expected_revision: current.snapshot.revision,
                snapshot,
                dispatchable: vec![],
            },
        )
        .unwrap()
}

pub struct Captured {
    view: ThreadView,
    execution: StoredExecution,
    authority: ApprovalView,
    journal: Vec<JournalEntry>,
}

impl Captured {
    pub fn new(store: &impl ApprovalStore, execution: &StoredExecution) -> Self {
        Self {
            view: store.thread_view(&execution.thread_id).unwrap().unwrap(),
            execution: execution.clone(),
            authority: store
                .approval_view(&execution.snapshot.execution_id)
                .unwrap()
                .unwrap(),
            journal: store.journal(&execution.snapshot.execution_id).unwrap(),
        }
    }

    pub fn unchanged(&self, store: &impl ApprovalStore) {
        assert_eq!(
            store.thread_view(&self.execution.thread_id).unwrap(),
            Some(self.view.clone())
        );
        assert_eq!(
            store
                .execution(&self.execution.snapshot.execution_id)
                .unwrap(),
            Some(self.execution.clone())
        );
        assert_eq!(
            store
                .approval_view(&self.execution.snapshot.execution_id)
                .unwrap(),
            Some(self.authority.clone())
        );
        assert_eq!(
            store
                .journal(&self.execution.snapshot.execution_id)
                .unwrap(),
            self.journal
        );
    }

    pub fn descriptor(&self, actual: &UnfinishedExecutionRef, relation: DiscoveryOwnerRelation) {
        let snapshot = &self.execution.snapshot;
        assert_eq!(actual.thread_id, self.execution.thread_id);
        assert_eq!(actual.thread_generation, self.view.thread.generation);
        assert_eq!(actual.execution_id, snapshot.execution_id);
        assert_eq!(actual.execution_fence, ExecutionFence::from(snapshot));
        assert_eq!(actual.resource_revision, self.view.thread.resource_revision);
        assert_eq!(actual.route, snapshot.definition.route);
        assert_eq!(actual.definition_revision, snapshot.definition.revision);
        assert_eq!(actual.status, snapshot.status);
        assert_eq!(actual.owner_relation, relation);
    }
}

pub fn selection(store: &impl ApprovalStore) {
    let states = [
        State::Accepted,
        State::Ready,
        State::Waiting,
        State::InFlight,
        State::Cancelling,
        State::Blocked,
        State::Unknown,
        State::Completed,
        State::Failed,
        State::Cancelled,
    ];
    let expected_statuses = [
        ExecutionStatus::Accepted,
        ExecutionStatus::Running,
        ExecutionStatus::WaitingForApproval,
        ExecutionStatus::Running,
        ExecutionStatus::CancelRequested,
        ExecutionStatus::RecoveryBlocked,
        ExecutionStatus::RecoveryBlocked,
        ExecutionStatus::Completed,
        ExecutionStatus::Failed,
        ExecutionStatus::Cancelled,
    ];
    let mut before = Vec::new();
    for (index, state) in states.into_iter().enumerate() {
        let execution = admit(store, &format!("selection-{index:02}"), state, Map::new());
        assert_eq!(execution.snapshot.status, expected_statuses[index]);
        before.push(Captured::new(store, &execution));
    }
    let owner = store.owner_generation();
    let page = store.discover_unfinished(None).unwrap();
    assert_eq!(page.owner_generation, owner);
    assert_eq!(page.scanned_rows, 10);
    assert_eq!(page.records.len(), 7);
    assert!(page.next_cursor.is_none());
    for (index, record) in page.records.iter().enumerate() {
        before[index].descriptor(record, DiscoveryOwnerRelation::Current);
        assert_eq!(record.status, expected_statuses[index]);
        assert_eq!(record.has_in_flight_task, matches!(index, 3 | 4));
        assert_eq!(record.has_outcome_unknown_task, index == 6);
    }
    for captured in before {
        captured.unchanged(store);
    }
    assert_eq!(store.owner_generation(), owner);
}

pub fn pagination(store: &impl ApprovalStore, terminal_prefix: bool) {
    assert_eq!(store.discover_unfinished(None).unwrap().scanned_rows, 0);
    let mut captures = Vec::new();
    for index in 0..33 {
        let state = if terminal_prefix && index < 32 {
            State::Cancelled
        } else {
            State::Accepted
        };
        let execution = admit(store, &format!("page-{index:02}"), state, Map::new());
        captures.push(Captured::new(store, &execution));
    }
    let first = store.discover_unfinished(None).unwrap();
    assert_eq!(first.scanned_rows, 32);
    assert_eq!(first.records.len(), if terminal_prefix { 0 } else { 32 });
    if !terminal_prefix {
        for (capture, record) in captures.iter().zip(&first.records) {
            capture.descriptor(record, DiscoveryOwnerRelation::Current);
        }
    }
    let cursor = first.next_cursor.as_ref().unwrap();
    let second = store.discover_unfinished(Some(cursor)).unwrap();
    assert_eq!(second.scanned_rows, 1);
    assert_eq!(second.records.len(), 1);
    captures[32].descriptor(&second.records[0], DiscoveryOwnerRelation::Current);
    assert!(second.next_cursor.is_none());
    // Reusing a cursor is read-only and reproducible within unchanged data.
    let repeated = store.discover_unfinished(Some(cursor)).unwrap();
    assert_eq!(repeated.records, second.records);
    for capture in captures {
        capture.unchanged(store);
    }
}

pub fn interpage_mutation(store: &impl ApprovalStore) {
    let mut executions = Vec::new();
    for index in 0..33 {
        executions.push(admit(
            store,
            &format!("mutation-{index:02}"),
            State::Accepted,
            Map::new(),
        ));
    }
    let first = store.discover_unfinished(None).unwrap();
    assert_eq!(first.scanned_rows, 32);
    let cursor = first.next_cursor.unwrap();
    let earlier = admit(store, "mutation-00-earlier", State::Accepted, Map::new());
    let removed = executions.pop().unwrap();
    let removed = command(
        store,
        removed.clone(),
        KernelCommand::Cancel {
            turn_id: removed.snapshot.turn_id,
            owner_generation: store.owner_generation(),
        },
    );
    let view = store.thread_view(&removed.thread_id).unwrap().unwrap();
    store
        .delete_thread_fenced(&ThreadDeletionTarget::from_view(&view).unwrap())
        .unwrap();
    let later = admit(store, "mutation-z", State::Accepted, Map::new());
    let captured = Captured::new(store, &later);
    let page = store.discover_unfinished(Some(&cursor)).unwrap();
    assert_eq!(page.scanned_rows, 1);
    assert_eq!(page.records.len(), 1);
    captured.descriptor(&page.records[0], DiscoveryOwnerRelation::Current);
    assert!(page.next_cursor.is_none());
    captured.unchanged(store);
    assert!(store
        .execution(&removed.snapshot.execution_id)
        .unwrap()
        .is_none());
    // A fresh scan sees the earlier admission; continuation deliberately does not.
    let fresh = store.discover_unfinished(None).unwrap();
    assert!(fresh
        .records
        .iter()
        .any(|record| record.execution_id == earlier.snapshot.execution_id));
    assert!(!page
        .records
        .iter()
        .any(|record| record.execution_id == earlier.snapshot.execution_id));
    let current = store
        .execution(&later.snapshot.execution_id)
        .unwrap()
        .unwrap();
    let cancelled = command(
        store,
        current.clone(),
        KernelCommand::Cancel {
            turn_id: current.snapshot.turn_id,
            owner_generation: store.owner_generation(),
        },
    );
    let view = store.thread_view(&cancelled.thread_id).unwrap().unwrap();
    store
        .delete_thread_fenced(&ThreadDeletionTarget::from_view(&view).unwrap())
        .unwrap();
    let remaining = Captured::new(store, &earlier);
    let exhausted = store.discover_unfinished(Some(&cursor)).unwrap();
    assert_eq!(exhausted.scanned_rows, 0);
    assert!(exhausted.records.is_empty());
    assert!(exhausted.next_cursor.is_none());
    remaining.unchanged(store);
}

pub fn foreign_cursor(source: &impl ApprovalStore, foreign: &impl ApprovalStore) {
    for index in 0..33 {
        admit(
            source,
            &format!("cursor-{index:02}"),
            State::Accepted,
            Map::new(),
        );
    }
    let page = source.discover_unfinished(None).unwrap();
    let cursor = page.next_cursor.as_ref().unwrap().clone();
    let other = admit(foreign, "foreign", State::Accepted, Map::new());
    let captured = Captured::new(foreign, &other);
    assert_eq!(source.owner_generation(), foreign.owner_generation());
    assert_eq!(
        foreign.discover_unfinished(Some(&cursor)).unwrap_err(),
        DiscoveryError::CursorMismatch
    );
    captured.unchanged(foreign);
    assert_eq!(
        source
            .discover_unfinished(Some(&cursor))
            .unwrap()
            .scanned_rows,
        1
    );
}

pub fn byte_values(store: &impl Store, name: &str, target: usize) -> Map<String, Value> {
    let mut values = Map::from_iter([("padding".into(), Value::String(String::new()))]);
    let empty = initial(store, name, false, values.clone());
    let overhead = serde_json::to_vec(&empty).unwrap().len();
    values.insert(
        "padding".into(),
        Value::String("x".repeat(target.checked_sub(overhead).unwrap())),
    );
    assert_eq!(
        serde_json::to_vec(&initial(store, name, false, values.clone()))
            .unwrap()
            .len(),
        target
    );
    values
}

pub fn byte_boundary(store: &impl ApprovalStore) {
    let at_cap = admit(
        store,
        "byte-at",
        State::Accepted,
        byte_values(store, "byte-at", DISCOVERY_SNAPSHOT_BYTES),
    );
    let captured = Captured::new(store, &at_cap);
    let page = store.discover_unfinished(None).unwrap();
    captured.descriptor(&page.records[0], DiscoveryOwnerRelation::Current);
    captured.unchanged(store);
    let over = admit(
        store,
        "byte-over",
        State::Accepted,
        byte_values(store, "byte-over", DISCOVERY_SNAPSHOT_BYTES + 1),
    );
    b4_kernel::validate_snapshot(&over.snapshot).unwrap();
    let over_capture = Captured::new(store, &over);
    assert_eq!(
        store.discover_unfinished(None).unwrap_err(),
        DiscoveryError::LimitExceeded
    );
    captured.unchanged(store);
    over_capture.unchanged(store);
}

pub fn full_page_byte_cap(store: &impl ApprovalStore) {
    let mut expected = Vec::new();
    for index in 0..32 {
        let name = format!("full-cap-{index:02}");
        let execution = admit(
            store,
            &name,
            State::Accepted,
            byte_values(store, &name, DISCOVERY_SNAPSHOT_BYTES),
        );
        expected.push(Captured::new(store, &execution));
    }
    let page = store.discover_unfinished(None).unwrap();
    assert_eq!(page.scanned_rows, 32);
    assert_eq!(page.records.len(), 32);
    assert!(page.next_cursor.is_none());
    for (record, captured) in page.records.iter().zip(expected) {
        captured.descriptor(record, DiscoveryOwnerRelation::Current);
        assert_eq!(record.status, ExecutionStatus::Accepted);
        assert!(!record.has_in_flight_task);
        assert!(!record.has_outcome_unknown_task);
        captured.unchanged(store);
    }
}

fn shape(value: &Value) -> (usize, usize) {
    match value {
        Value::Array(children) => children.iter().fold((1, 1), |(depth, nodes), child| {
            let (child_depth, child_nodes) = shape(child);
            (depth.max(child_depth + 1), nodes + child_nodes)
        }),
        Value::Object(children) => {
            children
                .values()
                .fold((1, 1 + children.len()), |(depth, nodes), child| {
                    let (child_depth, child_nodes) = shape(child);
                    (depth.max(child_depth + 1), nodes + child_nodes)
                })
        }
        _ => (0, 1),
    }
}

pub fn shape_boundary(store: &impl ApprovalStore, depth: bool) {
    let limit = if depth {
        DISCOVERY_JSON_DEPTH
    } else {
        DISCOVERY_JSON_NODES
    };
    let mut values = Map::from_iter([("payload".into(), Value::Array(vec![]))]);
    let base = serde_json::to_value(initial(store, "shape-at", false, values.clone())).unwrap();
    let (base_depth, base_nodes) = shape(&base);
    if depth {
        let mut nested = Value::Array(vec![]);
        for _ in 0..(limit - 3) {
            nested = Value::Array(vec![nested]);
        }
        values.insert("payload".into(), nested);
    } else {
        values.insert(
            "payload".into(),
            Value::Array(vec![Value::Null; limit - base_nodes]),
        );
    }
    assert!(base_depth < limit);
    let snapshot = initial(store, "shape-at", false, values.clone());
    let measured = shape(&serde_json::to_value(&snapshot).unwrap());
    assert_eq!(if depth { measured.0 } else { measured.1 }, limit);
    b4_kernel::validate_snapshot(&snapshot).unwrap();
    let at = admit(store, "shape-at", State::Accepted, values.clone());
    let captured = Captured::new(store, &at);
    let page = store.discover_unfinished(None).unwrap();
    captured.descriptor(&page.records[0], DiscoveryOwnerRelation::Current);
    let payload = values.get_mut("payload").unwrap();
    if depth {
        *payload = Value::Array(vec![payload.clone()]);
    } else {
        payload.as_array_mut().unwrap().push(Value::Null);
    }
    let over = admit(store, "shape-over", State::Accepted, values);
    b4_kernel::validate_snapshot(&over.snapshot).unwrap();
    let over_capture = Captured::new(store, &over);
    assert_eq!(
        store.discover_unfinished(None).unwrap_err(),
        DiscoveryError::LimitExceeded
    );
    captured.unchanged(store);
    over_capture.unchanged(store);
}
