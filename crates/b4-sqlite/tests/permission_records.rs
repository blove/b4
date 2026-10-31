#![cfg(unix)]

use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::approval::*;
use b4_runtime::permissions::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
const MAX_FIXTURE_BYTES: usize = 8_192;
const MAX_FIXTURE_NODES: usize = 512;
const MAX_FIXTURE_DEPTH: usize = 16;
const BUSINESS_TOKEN: &str = "fictional-business-token";
const USE_ID: &str = "b4pu_1111111111111111111111111111111111111111111111111111111111111111";

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-record-first-small-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct AllowAuthority;

impl ApprovalAuthority for AllowAuthority {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Ok(())
    }
}

struct CountCalls(Arc<AtomicUsize>);

impl NativeEffect for CountCalls {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"called": true}),
            writes: vec![],
        }))
    }
}

fn facts(now_ms: u64) -> TransitionFacts {
    TransitionFacts { now_ms }
}

fn fence(store: &dyn ApprovalStore, thread_id: &ThreadId) -> ApprovalFence {
    let view = store.thread_view(thread_id).unwrap().unwrap();
    ApprovalFence {
        execution: (&view.execution.unwrap().snapshot).into(),
        resource_revision: view.thread.resource_revision,
    }
}

#[derive(Debug, Clone, Copy)]
struct FixtureMetrics {
    bytes: usize,
    nodes: usize,
    depth: usize,
}

fn fixture_metrics(value: &Value) -> FixtureMetrics {
    fn visit(value: &Value, ancestors: usize, nodes: &mut usize, depth: &mut usize) {
        *nodes += 1;
        match value {
            Value::Object(object) => {
                *depth = (*depth).max(ancestors + 1);
                *nodes += object.len();
                for child in object.values() {
                    visit(child, ancestors + 1, nodes, depth);
                }
            }
            Value::Array(array) => {
                *depth = (*depth).max(ancestors + 1);
                for child in array {
                    visit(child, ancestors + 1, nodes, depth);
                }
            }
            _ => {}
        }
    }
    let mut nodes = 0;
    let mut depth = 0;
    visit(value, 0, &mut nodes, &mut depth);
    FixtureMetrics {
        bytes: serde_json::to_vec(value).unwrap().len(),
        nodes,
        depth,
    }
}

// This independent expected-value helper is used only on these bounded fixtures.
// It neither calls a candidate digest nor creates runtime authority.
fn canonical(value: &Value) -> Vec<u8> {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut entries: Vec<_> = object.iter().collect();
                entries.sort_by(|left, right| left.0.cmp(right.0));
                Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key.clone(), sorted(value)))
                        .collect(),
                )
            }
            Value::Array(array) => Value::Array(array.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    let metrics = fixture_metrics(value);
    assert!(metrics.bytes <= MAX_FIXTURE_BYTES, "fixture byte bound");
    assert!(metrics.nodes <= MAX_FIXTURE_NODES, "fixture node bound");
    assert!(metrics.depth <= MAX_FIXTURE_DEPTH, "fixture depth bound");
    serde_json::to_vec(&sorted(value)).unwrap()
}

fn independent_digest(value: &Value) -> String {
    let hash = Sha256::digest(canonical(value));
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in hash {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 15)] as char);
    }
    encoded
}

struct Samples {
    acceptance: Value,
    once_acceptance: Value,
    decision: Value,
    use_record: Value,
    parked: Snapshot,
    ready: Snapshot,
    thread: ThreadRecord,
    scope_digest: String,
    call_digest: String,
}

fn scope_wire(thread: &ThreadRecord, definition: &Definition, first: bool) -> Value {
    let (effect, tool) = if first {
        ("first-effect", "first-tool")
    } else {
        ("second-effect", "second-tool")
    };
    json!({
        "thread_id": thread.id,
        "thread_generation": thread.generation,
        "origin": thread.origin.as_ref().unwrap(),
        "application": {"application": "record-app", "revision": "application-1"},
        "route": definition.route,
        "definition": definition,
        "effect_id": effect,
        "tool_id": tool,
        "tool_revision": "tool-1",
        "policy_revision": "policy-1",
        "constraint_revision": "constraint-1"
    })
}

fn binding_wire(first: bool) -> Value {
    let (effect, tool) = if first {
        ("first-effect", "first-tool")
    } else {
        ("second-effect", "second-tool")
    };
    json!({
        "effect_id": effect,
        "tool_id": tool,
        "tool_revision": "tool-1",
        "constraint_revision": "constraint-1"
    })
}

fn samples(ready: &Snapshot, before: &ApprovalView) -> Samples {
    let parked = &before.execution.snapshot;
    let scope = scope_wire(&before.thread, &parked.definition, true);
    let scope_digest = independent_digest(&json!({"scope": scope, "version": 1}));
    let permission_id = format!("b4pd_{scope_digest}");
    let mut decisions = vec![];
    for (index, task) in parked.tasks.iter().enumerate() {
        let interrupt = task.interrupt.as_ref().unwrap();
        let grant = before
            .grants
            .iter()
            .find(|grant| grant.interrupt_id == interrupt.id)
            .unwrap();
        decisions.push(json!({
            "task_id": task.spec.id,
            "interrupt_id": interrupt.id,
            "grant_ref": {"execution_id": grant.execution_id, "interrupt_id": grant.interrupt_id},
            "decision": if index == 0 { "always" } else { "deny" },
            "proposal_digest": grant.proposal_digest,
            "task_parking_revision": grant.task_parking_revision,
            "binding": if index == 0 { binding_wire(true) } else { Value::Null },
            "permission_id": if index == 0 { json!(permission_id) } else { Value::Null }
        }));
    }
    let acceptance = json!({
        "record_version": 1,
        "thread_id": before.thread.id,
        "thread_generation": before.thread.generation,
        "origin": before.thread.origin.as_ref().unwrap(),
        "application": {"application": "record-app", "revision": "application-1"},
        "policy_revision": "policy-1",
        "before_snapshot": parked,
        "turn_id": "record-resume-turn",
        "facts": {"now_ms": 104},
        "committed_revision": parked.revision.checked_add(1).unwrap(),
        "decisions": decisions
    });
    let mut once_acceptance = acceptance.clone();
    once_acceptance["decisions"][1]["decision"] = json!("once");
    once_acceptance["decisions"][1]["binding"] = binding_wire(false);
    let decision = json!({
        "record_version": 1,
        "permission_id": permission_id,
        "scope_digest": scope_digest,
        "scope": scope,
        "creator_execution_id": parked.execution_id,
        "creator_turn_id": "record-resume-turn",
        "creator_task_id": "first",
        "creator_interrupt_id": "first-interrupt"
    });
    let call_digest = independent_digest(&json!({
        "input": ready.tasks[0].input,
        "task": ready.tasks[0].spec,
        "version": 1
    }));
    let use_record = json!({
        "record_version": 1,
        "permission_version": 1,
        "use_id": USE_ID,
        "permission_id": permission_id,
        "thread_id": before.thread.id,
        "thread_generation": before.thread.generation,
        "execution_id": ready.execution_id,
        "task_id": "first",
        "before_snapshot": ready,
        "facts": {"now_ms": 102},
        "committed_revision": ready.revision.checked_add(1).unwrap(),
        "marker": {"id": USE_ID, "prompt": null},
        "call_digest": call_digest,
        "application": {"application": "record-app", "revision": "application-1"},
        "tool_id": "first-tool",
        "tool_revision": "tool-1",
        "policy_revision": "policy-1",
        "constraint_revision": "constraint-1"
    });
    for wire in [&acceptance, &once_acceptance, &decision, &use_record] {
        canonical(wire);
    }
    Samples {
        acceptance,
        once_acceptance,
        decision,
        use_record,
        parked: parked.clone(),
        ready: ready.clone(),
        thread: before.thread.clone(),
        scope_digest,
        call_digest,
    }
}

fn with_samples<T>(operation: impl FnOnce(&Samples) -> T) -> T {
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("record-thread").unwrap();
    let execution_id = ExecutionId::new("record-execution").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "record-app".into(),
            tenant: "fixture-tenant".into(),
            principal: "fixture-principal".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/records#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![
            TaskSpec {
                id: TaskId::new("first").unwrap(),
                effect_id: EffectId::new("first-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
            TaskSpec {
                id: TaskId::new("second").unwrap(),
                effect_id: EffectId::new("second-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
        ]],
        max_attempts: 4,
    };
    let application = NativeApplication::new(
        ApplicationIdentity {
            application: "record-app".into(),
            revision: "application-1".into(),
        },
        vec![RouteRegistration {
            definition: definition.clone(),
            effects: BTreeMap::from([
                (
                    EffectId::new("first-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                ),
                (
                    EffectId::new("second-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                ),
            ]),
            input_validator: None,
        }],
    )
    .unwrap();
    let registered = application.definition(&definition.route).unwrap();
    assert_eq!(registered, &definition);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert_eq!(original_owner, 1);
    assert!(!store.durable_permissions());
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("record-original-turn").unwrap(),
        original_owner,
        registered.clone(),
        Map::from_iter([(
            "payload".into(),
            json!({"a": true, "token": BUSINESS_TOKEN, "z": [null, 2, 1]}),
        )]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread_id, initial).unwrap();
    let started = store
        .commit(
            &execution_id,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let ready = started.snapshot;
    assert_eq!(ready.revision, 1);
    assert_eq!(ready.status, ExecutionStatus::Running);
    assert_eq!(ready.tasks.len(), 2);
    assert!(ready.output.is_none() && ready.failure.is_none());
    assert!(ready.attempt_history.is_empty() && ready.attempts_used == 0);
    for task in &ready.tasks {
        assert_eq!(task.status, TaskStatus::Ready);
        assert!(!task.approved);
        assert!(
            task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
                && task.interrupt.is_none()
        );
        assert_eq!(task.input, ready.values);
    }
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompts = [
        json!({"question": "Proceed with first?", "arguments": {"b": 2, "a": null}}),
        json!({"question": "Proceed with second?", "arguments": {"token": BUSINESS_TOKEN, "a": [2, 1]}}),
    ];
    for (index, name) in ["first", "second"].iter().enumerate() {
        let parked = service
            .park(
                &context,
                &execution_id,
                fence(store.as_ref(), &thread_id),
                TaskId::new(*name).unwrap(),
                InterruptId::new(format!("{name}-interrupt")).unwrap(),
                prompts[index].clone(),
                1_000,
                facts(102 + index as u64),
            )
            .unwrap();
        assert_eq!(parked.snapshot.revision, 2 + index as u64);
        assert_eq!(
            parked.snapshot.status,
            if index == 0 {
                ExecutionStatus::Running
            } else {
                ExecutionStatus::WaitingForApproval
            }
        );
    }
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let before = store.approval_view(&execution_id).unwrap().unwrap();
    let ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    assert_eq!(
        before.execution.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(before.execution.snapshot.revision, 3);
    assert_eq!(ordinary.execution.as_ref(), Some(&before.execution));
    assert_eq!(ordinary.thread, before.thread);
    assert_eq!(before.grants.len(), 2);
    assert_eq!(pending.len(), 2);
    assert!(before.acceptances.is_empty());
    assert_eq!(before.thread.origin.as_ref(), Some(&context.actor));
    for (index, task) in before.execution.snapshot.tasks.iter().enumerate() {
        assert_eq!(task.status, TaskStatus::WaitingForApproval);
        assert_eq!(task.spec, definition.batches[0][index]);
        assert!(
            !task.approved
                && task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
        );
        let interrupt = task.interrupt.as_ref().unwrap();
        assert_eq!(interrupt.prompt, prompts[index]);
        let grant = before
            .grants
            .iter()
            .find(|grant| grant.interrupt_id == interrupt.id)
            .unwrap();
        let projection = pending
            .iter()
            .find(|entry| entry.interrupt_id == interrupt.id)
            .unwrap();
        assert_eq!(grant.thread_id, thread_id);
        assert_eq!(grant.thread_generation, before.thread.generation);
        assert_eq!(grant.origin, context.actor);
        assert_eq!(grant.execution_id, execution_id);
        assert_eq!(grant.route, definition.route);
        assert_eq!(grant.definition_revision, definition.revision);
        assert_eq!(grant.task_id, task.spec.id);
        assert_eq!(grant.effect_id, task.spec.effect_id);
        assert_eq!(grant.policy_revision, context.policy_revision);
        assert_eq!(grant.prompt, interrupt.prompt);
        assert_eq!(grant.task_parking_revision, 2 + index as u64);
        assert_eq!(grant.issued_at_ms, 102 + index as u64);
        assert_eq!(grant.expires_at_ms, 1_102 + index as u64);
        assert!(!grant.consumed && grant.issued_at_ms <= 104 && 104 < grant.expires_at_ms);
        assert!(
            grant.token == projection.token,
            "real pending token equality"
        );
        assert!(
            grant.verifier == token_verifier(&projection.token),
            "real token verifier equality"
        );
        assert!(
            grant.resume_key == projection.resume_key,
            "real pending resume key equality"
        );
        assert_eq!(projection.task_id, task.spec.id);
        assert_eq!(projection.prompt, grant.prompt);
        assert_eq!(projection.expires_at_ms, grant.expires_at_ms);
        let expected = independent_digest(
            &json!({"input": task.input, "prompt": interrupt.prompt, "task": task.spec, "version": 1}),
        );
        assert_eq!(grant.proposal_digest, expected);
    }
    let captured = samples(&ready, &before);
    let result = operation(&captured);
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), ordinary);
    assert_eq!(store.approval_view(&execution_id).unwrap().unwrap(), before);
    store.readiness().unwrap();
    drop(pending);
    drop(service);
    drop(authority);
    drop(application);
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "all old store handles retired"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_before = next.approval_view(&execution_id).unwrap().unwrap();
    next.readiness().unwrap();
    drop(next);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(next_ordinary, ordinary);
    assert_eq!(next_before, before);
    assert_eq!(
        next_before.execution.snapshot.owner_generation,
        original_owner
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(next_before);
    drop(before);
    result
}

trait FixtureCodec: Sized + PartialEq + fmt::Debug {
    fn decode(raw: &[u8], budget: &mut PermissionEvidenceBudget) -> Result<Self, PermissionError>;
    fn encode(&self, budget: &mut PermissionEvidenceBudget) -> Result<Vec<u8>, PermissionError>;
    fn wire(samples: &Samples) -> &Value;
    fn matches_witness(&self, samples: &Samples) -> bool;
}

impl FixtureCodec for PermissionAcceptance {
    fn decode(raw: &[u8], budget: &mut PermissionEvidenceBudget) -> Result<Self, PermissionError> {
        Self::decode_record(raw, budget)
    }
    fn encode(&self, budget: &mut PermissionEvidenceBudget) -> Result<Vec<u8>, PermissionError> {
        self.encode_record(budget)
    }
    fn wire(samples: &Samples) -> &Value {
        &samples.acceptance
    }
    fn matches_witness(&self, samples: &Samples) -> bool {
        self.record_version() == 1
            && self.before_snapshot() == &samples.parked
            && self.thread_id() == &samples.thread.id
            && self.thread_generation() == &samples.thread.generation
            && self.origin() == samples.thread.origin.as_ref().unwrap()
            && self.application().application() == "record-app"
            && self.application().revision() == "application-1"
            && self.policy_revision() == "policy-1"
            && self.turn_id().as_str() == "record-resume-turn"
            && self.facts() == &facts(104)
            && self.committed_revision() == 4
            && self.decisions().len() == 2
            && self.decisions()[0].decision() == ApprovalDecision::Always
            && self.decisions()[1].decision() == ApprovalDecision::Deny
            && self.decisions().iter().enumerate().all(|(index, entry)| {
                let task = &samples.parked.tasks[index];
                let expected = &samples.acceptance["decisions"][index];
                entry.task_id() == &task.spec.id
                    && entry.interrupt_id() == &task.interrupt.as_ref().unwrap().id
                    && entry.grant_ref().execution_id() == &samples.parked.execution_id
                    && entry.grant_ref().interrupt_id() == entry.interrupt_id()
                    && entry.proposal_digest() == expected["proposal_digest"].as_str().unwrap()
                    && entry.task_parking_revision() == 2 + index as u64
                    && if index == 0 {
                        entry.binding().is_some_and(|binding| {
                            binding.effect_id() == &task.spec.effect_id
                                && binding.tool_id().as_str() == "first-tool"
                                && binding.tool_revision().as_str() == "tool-1"
                                && binding.constraint_revision().as_str() == "constraint-1"
                        }) && entry.permission_id().is_some_and(|permission| {
                            permission.as_str() == format!("b4pd_{}", samples.scope_digest)
                        })
                    } else {
                        entry.binding().is_none() && entry.permission_id().is_none()
                    }
            })
    }
}

impl FixtureCodec for PermissionDecision {
    fn decode(raw: &[u8], budget: &mut PermissionEvidenceBudget) -> Result<Self, PermissionError> {
        Self::decode_record(raw, budget)
    }
    fn encode(&self, budget: &mut PermissionEvidenceBudget) -> Result<Vec<u8>, PermissionError> {
        self.encode_record(budget)
    }
    fn wire(samples: &Samples) -> &Value {
        &samples.decision
    }
    fn matches_witness(&self, samples: &Samples) -> bool {
        let scope = self.scope();
        self.record_version() == 1
            && self.scope_digest() == samples.scope_digest
            && self.permission_id().as_str() == format!("b4pd_{}", samples.scope_digest)
            && scope.thread_id() == &samples.thread.id
            && scope.thread_generation() == &samples.thread.generation
            && scope.origin() == samples.thread.origin.as_ref().unwrap()
            && scope.definition() == &samples.parked.definition
            && scope.route() == &samples.parked.definition.route
            && scope.effect_id().as_str() == "first-effect"
            && scope.application().application() == "record-app"
            && scope.application().revision() == "application-1"
            && scope.tool_id().as_str() == "first-tool"
            && scope.tool_revision().as_str() == "tool-1"
            && scope.policy_revision() == "policy-1"
            && scope.constraint_revision().as_str() == "constraint-1"
            && self.creator_execution_id() == &samples.parked.execution_id
            && self.creator_turn_id().as_str() == "record-resume-turn"
            && self.creator_task_id().as_str() == "first"
            && self.creator_interrupt_id().as_str() == "first-interrupt"
    }
}

impl FixtureCodec for PermissionUse {
    fn decode(raw: &[u8], budget: &mut PermissionEvidenceBudget) -> Result<Self, PermissionError> {
        Self::decode_record(raw, budget)
    }
    fn encode(&self, budget: &mut PermissionEvidenceBudget) -> Result<Vec<u8>, PermissionError> {
        self.encode_record(budget)
    }
    fn wire(samples: &Samples) -> &Value {
        &samples.use_record
    }
    fn matches_witness(&self, samples: &Samples) -> bool {
        self.record_version() == 1
            && self.permission_version() == 1
            && self.before_snapshot() == &samples.ready
            && self.thread_id() == &samples.thread.id
            && self.thread_generation() == &samples.thread.generation
            && self.execution_id() == &samples.ready.execution_id
            && self.task_id().as_str() == "first"
            && self.use_id().as_str() == USE_ID
            && self.marker().id.as_str() == USE_ID
            && self.marker().prompt.is_null()
            && self.permission_id().as_str() == format!("b4pd_{}", samples.scope_digest)
            && self.call_digest() == samples.call_digest
            && self.facts() == &facts(102)
            && self.committed_revision() == 2
            && self.application().application() == "record-app"
            && self.application().revision() == "application-1"
            && self.tool_id().as_str() == "first-tool"
            && self.tool_revision().as_str() == "tool-1"
            && self.policy_revision() == "policy-1"
            && self.constraint_revision().as_str() == "constraint-1"
    }
}

fn roundtrip<R: FixtureCodec>() {
    let result = with_samples(|samples| {
        let mut budget = PermissionEvidenceBudget::new();
        let original = R::decode(&canonical(R::wire(samples)), &mut budget)?;
        let matched = original.matches_witness(samples);
        let encoded = original.encode(&mut budget)?;
        let restored = R::decode(&encoded, &mut budget)?;
        Ok::<_, PermissionError>((original == restored, matched))
    });
    let (equal, matched) =
        result.expect("known-small codec roundtrip succeeds after normal retirement");
    assert!(equal, "whole typed roundtrip equality");
    assert!(matched, "borrowed getters retain genuine witness");
}

fn canonical_encoding<R: FixtureCodec>() {
    let (result, expected) = with_samples(|samples| {
        let expected = canonical(R::wire(samples));
        let mut budget = PermissionEvidenceBudget::new();
        let result =
            R::decode(&expected, &mut budget).and_then(|record| record.encode(&mut budget));
        (result, expected)
    });
    let encoded = result.expect("known-small encoding succeeds after normal retirement");
    assert!(
        encoded == expected,
        "whole record encoding must match independent canonical bytes"
    );
}

fn rejection<R: FixtureCodec>(mutate: impl FnOnce(&mut Value)) {
    let (control, result) = with_samples(|samples| {
        let mut control_budget = PermissionEvidenceBudget::new();
        let control = R::decode(&canonical(R::wire(samples)), &mut control_budget).map(|_| ());
        let mut faulty = R::wire(samples).clone();
        mutate(&mut faulty);
        let raw = canonical(&faulty);
        let mut budget = PermissionEvidenceBudget::new();
        let result = R::decode(&raw, &mut budget).map(|_| ());
        (control, result)
    });
    assert!(
        control.is_ok(),
        "known-small positive neighbor reaches codec"
    );
    assert!(
        matches!(result, Err(PermissionError::CorruptRecord)),
        "selected new metadata fault must be CorruptRecord"
    );
}

fn add_unknown(value: &mut Value) {
    value
        .as_object_mut()
        .unwrap()
        .insert("extra_record_metadata".into(), json!(1));
}

fn replace_unique(raw: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    let positions: Vec<_> = raw
        .windows(needle.len())
        .enumerate()
        .filter_map(|(index, window)| (window == needle).then_some(index))
        .collect();
    assert_eq!(
        positions.len(),
        1,
        "source-selected raw replacement is unique"
    );
    let index = positions[0];
    let mut output = Vec::with_capacity(raw.len() - needle.len() + replacement.len());
    output.extend_from_slice(&raw[..index]);
    output.extend_from_slice(replacement);
    output.extend_from_slice(&raw[index + needle.len()..]);
    assert!(
        output.len() <= MAX_FIXTURE_BYTES,
        "duplicate fixture byte bound"
    );
    output
}

#[derive(Clone, Copy)]
enum DuplicateLocation {
    Input,
    Prompt,
    Values,
}

fn duplicate_rejection(location: DuplicateLocation) {
    let (control, result) = with_samples(|samples| {
        let original = canonical(&samples.acceptance);
        let mut control_budget = PermissionEvidenceBudget::new();
        let control =
            PermissionAcceptance::decode_record(&original, &mut control_budget).map(|_| ());
        let (container, key, spelling) = match location {
            DuplicateLocation::Input => (
                &samples.acceptance["before_snapshot"]["tasks"][0],
                "token",
                "tok\\u0065n",
            ),
            DuplicateLocation::Prompt => (
                &samples.acceptance["before_snapshot"]["tasks"][0]["interrupt"]["prompt"],
                "question",
                "quest\\u0069on",
            ),
            DuplicateLocation::Values => (
                &samples.acceptance["before_snapshot"]["values"],
                "token",
                "tok\\u0065n",
            ),
        };
        let container_raw = canonical(container);
        let scalar = if key == "question" {
            "Proceed with first?"
        } else {
            BUSINESS_TOKEN
        };
        let needle = format!("\"{key}\":{}", serde_json::to_string(scalar).unwrap());
        let replacement = format!(
            "{needle},\"{spelling}\":{}",
            serde_json::to_string(scalar).unwrap()
        );
        let duplicate_container =
            replace_unique(&container_raw, needle.as_bytes(), replacement.as_bytes());
        let raw = match location {
            DuplicateLocation::Values => {
                let needle_outer = [b"\"values\":".as_slice(), container_raw.as_slice()].concat();
                let replacement_outer =
                    [b"\"values\":".as_slice(), duplicate_container.as_slice()].concat();
                replace_unique(&original, &needle_outer, &replacement_outer)
            }
            DuplicateLocation::Input | DuplicateLocation::Prompt => {
                replace_unique(&original, &container_raw, &duplicate_container)
            }
        };
        let mut budget = PermissionEvidenceBudget::new();
        let result = PermissionAcceptance::decode_record(&raw, &mut budget).map(|_| ());
        (control, result)
    });
    assert!(
        control.is_ok(),
        "known-small positive duplicate neighbor reaches codec"
    );
    assert!(
        matches!(result, Err(PermissionError::CorruptRecord)),
        "escaped-equivalent free business keys must be refused before lossy parsing"
    );
}

#[test]
fn acceptance_known_small_roundtrip_preserves_real_parked_witness() {
    roundtrip::<PermissionAcceptance>();
}

#[test]
fn decision_known_small_roundtrip_preserves_full_scope() {
    roundtrip::<PermissionDecision>();
}

#[test]
fn use_known_small_roundtrip_preserves_real_unapproved_ready_witness() {
    roundtrip::<PermissionUse>();
}

#[test]
fn acceptance_always_once_preserves_required_present_null_permission_id() {
    let result = with_samples(|samples| {
        let mut budget = PermissionEvidenceBudget::new();
        let record =
            PermissionAcceptance::decode_record(&canonical(&samples.once_acceptance), &mut budget)?;
        let fields = record.decisions().len() == 2
            && record.decisions()[1].decision() == ApprovalDecision::Once
            && record.decisions()[1].binding().is_some()
            && record.decisions()[1].permission_id().is_none();
        let encoded = record.encode_record(&mut budget)?;
        let restored = PermissionAcceptance::decode_record(&encoded, &mut budget)?;
        Ok::<_, PermissionError>((record == restored, fields))
    });
    let (equal, fields) =
        result.expect("known-small Always+Once proposal roundtrip after normal retirement");
    assert!(
        equal && fields,
        "present nullable key and Once binding preserved"
    );
}

#[test]
fn record_debug_and_syntax_errors_remain_fixed_and_redacted() {
    let result = with_samples(|samples| {
        let mut budget = PermissionEvidenceBudget::new();
        let acceptance =
            PermissionAcceptance::decode_record(&canonical(&samples.acceptance), &mut budget)?;
        let decision =
            PermissionDecision::decode_record(&canonical(&samples.decision), &mut budget)?;
        let usage = PermissionUse::decode_record(&canonical(&samples.use_record), &mut budget)?;
        let debug = vec![
            format!("{acceptance:?}"),
            format!("{decision:?}"),
            format!("{usage:?}"),
            format!("{:?}", decision.scope()),
            format!("{:?}", decision.scope().application()),
        ];
        let nested = acceptance.decisions().first().map(|entry| {
            (
                format!("{entry:?}"),
                entry.binding().map(|binding| format!("{binding:?}")),
                format!("{:?}", entry.grant_ref()),
            )
        });
        let syntax =
            PermissionAcceptance::decode_record(b"{\"fictional-private-syntax\":", &mut budget);
        Ok::<_, PermissionError>((debug, nested, syntax.map(|_| ())))
    });
    let (debug, nested, syntax) =
        result.expect("known-small debug capture after normal retirement");
    assert_eq!(
        debug,
        [
            "PermissionAcceptance([REDACTED])",
            "PermissionDecision([REDACTED])",
            "PermissionUse([REDACTED])",
            "PermissionScope([REDACTED])",
            "PermissionApplicationIdentity([REDACTED])"
        ]
    );
    let (entry, binding, grant) =
        nested.expect("known-small first decision retained after normal retirement");
    assert_eq!(entry, "PermissionAcceptedDecision([REDACTED])");
    assert_eq!(
        binding.as_deref(),
        Some("PermissionBindingIdentity([REDACTED])")
    );
    assert_eq!(grant, "PermissionGrantReference([REDACTED])");
    let error = syntax.expect_err("malformed syntax has finite error");
    assert!(matches!(&error, PermissionError::CorruptRecord));
    assert_eq!(error.to_string(), "permission_corrupt_record");
    assert_eq!(
        format!("{error:?}"),
        "PermissionError(\"permission_corrupt_record\")"
    );
}

#[test]
fn inherited_snapshot_serde_distinguishes_absent_output_and_explicit_null() {
    let captured = with_samples(|samples| {
        let absent = serde_json::to_value(&samples.ready).unwrap();
        let mut present = absent.clone();
        present
            .as_object_mut()
            .unwrap()
            .insert("output".into(), Value::Null);
        let none = serde_json::from_value::<Snapshot>(absent);
        let null = serde_json::from_value::<Snapshot>(present);
        (none, null)
    });
    let none = captured.0.expect("inherited absent output parse");
    let null = captured.1.expect("inherited explicit null parse");
    assert!(none.output.is_none());
    assert_eq!(null.output, Some(Value::Null));
    assert!(serde_json::to_value(none).unwrap().get("output").is_none());
    assert_eq!(
        serde_json::to_value(null).unwrap().get("output"),
        Some(&Value::Null)
    );
}

#[test]
fn call_digest_matches_independent_versioned_task_input_sha256() {
    let (result, expected) = with_samples(|samples| {
        let mut budget = PermissionEvidenceBudget::new();
        (
            permission_call_digest(&samples.ready.tasks[0], &mut budget),
            samples.call_digest.clone(),
        )
    });
    let actual =
        result.expect("call digest must return finite successful result after normal retirement");
    assert!(
        actual == expected,
        "call digest must match independent exact canonical SHA256"
    );
}

#[test]
fn scope_digest_matches_independent_versioned_full_scope_sha256() {
    let (result, expected) = with_samples(|samples| {
        let mut budget = PermissionEvidenceBudget::new();
        let result = PermissionDecision::decode_record(&canonical(&samples.decision), &mut budget)
            .and_then(|record| record.scope().canonical_digest(&mut budget));
        (result, samples.scope_digest.clone())
    });
    let actual =
        result.expect("scope digest must return finite successful result after normal retirement");
    assert!(
        actual == expected,
        "scope digest must match independent exact canonical SHA256"
    );
}

#[test]
fn acceptance_encoding_matches_independent_whole_canonical_record() {
    canonical_encoding::<PermissionAcceptance>();
}

#[test]
fn decision_encoding_matches_independent_whole_canonical_record() {
    canonical_encoding::<PermissionDecision>();
}

#[test]
fn use_encoding_matches_independent_whole_canonical_record() {
    canonical_encoding::<PermissionUse>();
}

#[test]
fn acceptance_refuses_unknown_record_version() {
    rejection::<PermissionAcceptance>(|wire| wire["record_version"] = json!(2));
}

#[test]
fn decision_refuses_unknown_record_version() {
    rejection::<PermissionDecision>(|wire| wire["record_version"] = json!(2));
}

#[test]
fn use_refuses_unknown_record_version() {
    rejection::<PermissionUse>(|wire| wire["record_version"] = json!(2));
}

#[test]
fn use_refuses_unknown_permission_version() {
    rejection::<PermissionUse>(|wire| wire["permission_version"] = json!(2));
}

#[test]
fn acceptance_refuses_unknown_root_metadata() {
    rejection::<PermissionAcceptance>(add_unknown);
}

#[test]
fn decision_refuses_unknown_root_metadata() {
    rejection::<PermissionDecision>(add_unknown);
}

#[test]
fn use_refuses_unknown_root_metadata() {
    rejection::<PermissionUse>(add_unknown);
}

#[test]
fn decision_refuses_unknown_scope_metadata() {
    rejection::<PermissionDecision>(|wire| add_unknown(&mut wire["scope"]));
}

#[test]
fn acceptance_refuses_unknown_application_mirror_metadata() {
    rejection::<PermissionAcceptance>(|wire| add_unknown(&mut wire["application"]));
}

#[test]
fn acceptance_refuses_unknown_binding_metadata() {
    rejection::<PermissionAcceptance>(|wire| add_unknown(&mut wire["decisions"][0]["binding"]));
}

#[test]
fn acceptance_refuses_unknown_grant_reference_metadata() {
    rejection::<PermissionAcceptance>(|wire| add_unknown(&mut wire["decisions"][0]["grant_ref"]));
}

#[test]
fn acceptance_refuses_unknown_accepted_decision_metadata() {
    rejection::<PermissionAcceptance>(|wire| add_unknown(&mut wire["decisions"][0]));
}

#[test]
fn acceptance_requires_present_nullable_deny_binding() {
    rejection::<PermissionAcceptance>(|wire| {
        wire["decisions"][1]
            .as_object_mut()
            .unwrap()
            .remove("binding")
            .unwrap();
    });
}

#[test]
fn acceptance_requires_present_nullable_deny_permission_id() {
    rejection::<PermissionAcceptance>(|wire| {
        wire["decisions"][1]
            .as_object_mut()
            .unwrap()
            .remove("permission_id")
            .unwrap();
    });
}

#[test]
fn acceptance_refuses_invalid_decoded_application_mirror_label() {
    rejection::<PermissionAcceptance>(|wire| wire["application"]["revision"] = json!(""));
}

#[test]
fn acceptance_refuses_escaped_duplicate_free_input_key() {
    duplicate_rejection(DuplicateLocation::Input);
}

#[test]
fn acceptance_refuses_escaped_duplicate_original_prompt_key() {
    duplicate_rejection(DuplicateLocation::Prompt);
}

#[test]
fn acceptance_refuses_escaped_duplicate_snapshot_state_key() {
    duplicate_rejection(DuplicateLocation::Values);
}

// These independent intrinsic-record qualifiers reuse the entire original
// genuine fixture and finish its physical retirement before their first oracle.
#[derive(Clone, Copy)]
enum InherentSnapshotControl {
    None,
    Valid,
    Refused,
    RefusedWithOutputNull,
}

fn inherent_rejection<R: FixtureCodec>(
    select: fn(&Samples) -> &Value,
    mutate: impl FnOnce(&Samples, &mut Value),
    projection: InherentSnapshotControl,
) {
    let (control, result, flags) = with_samples(|samples| {
        let original = select(samples);
        let mut control_budget = PermissionEvidenceBudget::new();
        let control = R::decode(&canonical(original), &mut control_budget).map(|_| ());
        let mut faulty = original.clone();
        mutate(samples, &mut faulty);
        let flags = match projection {
            InherentSnapshotControl::None => None,
            _ => Some(
                match serde_json::from_value::<Snapshot>(faulty["before_snapshot"].clone()) {
                    Ok(snapshot) => (
                        true,
                        Some(b4_kernel::validate_snapshot(&snapshot).is_ok()),
                        Some(snapshot.output == Some(Value::Null)),
                    ),
                    Err(_) => (false, None, None),
                },
            ),
        };
        let mut budget = PermissionEvidenceBudget::new();
        let result = R::decode(&canonical(&faulty), &mut budget).map(|_| ());
        (control, result, flags)
    });
    assert!(
        control.is_ok(),
        "genuine positive neighbor reaches inherent codec after retirement"
    );
    match projection {
        InherentSnapshotControl::None => {}
        expected => {
            let (parsed, valid, output_null) =
                flags.expect("finite old Snapshot controls captured");
            assert!(
                parsed,
                "old Snapshot Deserialize accepts selected projection after retirement"
            );
            assert_eq!(
                valid,
                Some(matches!(expected, InherentSnapshotControl::Valid)),
                "old kernel shape control matches selected projection after retirement"
            );
            if matches!(expected, InherentSnapshotControl::RefusedWithOutputNull) {
                assert_eq!(
                    output_null,
                    Some(true),
                    "old serde preserves explicit output Some(Null)"
                );
            }
        }
    }
    assert!(
        matches!(result, Err(PermissionError::CorruptRecord)),
        "selected intrinsic record fault must be CorruptRecord after full retirement"
    );
}

fn inherent_once_neighbor(samples: &Samples) -> &Value {
    &samples.once_acceptance
}

fn inherent_acceptance_scope(wire: &Value) -> Value {
    let binding = &wire["decisions"][0]["binding"];
    json!({
        "thread_id": wire["thread_id"],
        "thread_generation": wire["thread_generation"],
        "origin": wire["origin"],
        "application": wire["application"],
        "route": wire["before_snapshot"]["definition"]["route"],
        "definition": wire["before_snapshot"]["definition"],
        "effect_id": binding["effect_id"],
        "tool_id": binding["tool_id"],
        "tool_revision": binding["tool_revision"],
        "policy_revision": wire["policy_revision"],
        "constraint_revision": binding["constraint_revision"]
    })
}

fn inherent_acceptance_digest(wire: &Value) -> String {
    independent_digest(&json!({"scope": inherent_acceptance_scope(wire), "version": 1}))
}

fn repair_inherent_acceptance(wire: &mut Value) {
    let digest = inherent_acceptance_digest(wire);
    wire["decisions"][0]["permission_id"] = json!(format!("b4pd_{digest}"));
}

fn repair_inherent_decision(wire: &mut Value) {
    let digest = independent_digest(&json!({"scope": wire["scope"], "version": 1}));
    wire["permission_id"] = json!(format!("b4pd_{digest}"));
    wire["scope_digest"] = json!(digest);
}

fn repair_inherent_call(wire: &mut Value) {
    let task = &wire["before_snapshot"]["tasks"][0];
    let digest =
        independent_digest(&json!({"input": task["input"], "task": task["spec"], "version": 1}));
    wire["call_digest"] = json!(digest);
}

// The independent SHA256 helper always yields exactly64 lowercase ASCII hex.
// Changing one digit guarantees inequality for each actual random incarnation.
fn wrong_inherent_digest(expected: &str) -> String {
    let mut wrong = expected.as_bytes().to_vec();
    wrong[0] = if wrong[0] == b'0' { b'1' } else { b'0' };
    String::from_utf8(wrong).expect("independent digest remains known ASCII")
}

// A01
#[test]
fn inherent_acceptance_refuses_wrong_committed_revision() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["committed_revision"] = json!(5),
        InherentSnapshotControl::None,
    );
}
// A02
#[test]
fn inherent_acceptance_refuses_clock_before_parked_witness() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["facts"]["now_ms"] = json!(102),
        InherentSnapshotControl::None,
    );
}
// A03
#[test]
fn inherent_acceptance_refuses_existing_resume_turn() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["turn_id"] = json!("record-original-turn"),
        InherentSnapshotControl::None,
    );
}
// A04
#[test]
fn inherent_acceptance_refuses_reversed_complete_waiting_order() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"].as_array_mut().unwrap().reverse(),
        InherentSnapshotControl::None,
    );
}
// A05
#[test]
fn inherent_acceptance_refuses_omitted_waiting_decision() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"].as_array_mut().unwrap().truncate(1),
        InherentSnapshotControl::None,
    );
}
// A06
#[test]
fn inherent_acceptance_refuses_duplicated_selected_entry() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            let repeated = wire["decisions"][0].clone();
            wire["decisions"][1] = repeated;
        },
        InherentSnapshotControl::None,
    );
}
// A07
#[test]
fn inherent_acceptance_refuses_extra_selected_entry() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            let repeated = wire["decisions"][0].clone();
            wire["decisions"].as_array_mut().unwrap().push(repeated);
        },
        InherentSnapshotControl::None,
    );
}
// A08
#[test]
fn inherent_acceptance_refuses_task_outside_waiting_set() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][0]["task_id"] = json!("absent-task"),
        InherentSnapshotControl::None,
    );
}
// A09
#[test]
fn inherent_acceptance_refuses_interrupt_outside_selected_task() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["decisions"][0]["interrupt_id"] = json!("absent-interrupt");
            wire["decisions"][0]["grant_ref"]["interrupt_id"] = json!("absent-interrupt");
        },
        InherentSnapshotControl::None,
    );
}
// A10
#[test]
fn inherent_acceptance_refuses_grant_execution_mismatch() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][0]["grant_ref"]["execution_id"] = json!("other-execution"),
        InherentSnapshotControl::None,
    );
}
// A11
#[test]
fn inherent_acceptance_refuses_grant_interrupt_mismatch() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][0]["grant_ref"]["interrupt_id"] = json!("second-interrupt"),
        InherentSnapshotControl::None,
    );
}
// A12
#[test]
fn inherent_acceptance_refuses_zero_parking_revision() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][0]["task_parking_revision"] = json!(0),
        InherentSnapshotControl::None,
    );
}
// A13
#[test]
fn inherent_acceptance_refuses_parking_revision_after_witness() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][0]["task_parking_revision"] = json!(4),
        InherentSnapshotControl::None,
    );
}
// A14
#[test]
fn inherent_acceptance_refuses_wrong_original_legacy_proposal_digest() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            let task = &wire["before_snapshot"]["tasks"][0];
            let expected = independent_digest(&json!({
                "input": task["input"], "prompt": task["interrupt"]["prompt"],
                "task": task["spec"], "version": 1
            }));
            wire["decisions"][0]["proposal_digest"] = json!(wrong_inherent_digest(&expected));
        },
        InherentSnapshotControl::None,
    );
}
// A15
#[test]
fn inherent_acceptance_refuses_binding_effect_mismatch_with_repaired_scope_id() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["decisions"][0]["binding"]["effect_id"] = json!("second-effect");
            repair_inherent_acceptance(wire);
        },
        InherentSnapshotControl::None,
    );
}
// A16
#[test]
fn inherent_acceptance_refuses_null_always_binding() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][0]["binding"] = Value::Null,
        InherentSnapshotControl::None,
    );
}
// A17
#[test]
fn inherent_acceptance_refuses_present_deny_binding() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][1]["binding"] = binding_wire(false),
        InherentSnapshotControl::None,
    );
}
// A18
#[test]
fn inherent_acceptance_refuses_null_always_permission_id() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["decisions"][0]["permission_id"] = Value::Null,
        InherentSnapshotControl::None,
    );
}
// A19
#[test]
fn inherent_acceptance_refuses_permission_id_on_once_neighbor() {
    inherent_rejection::<PermissionAcceptance>(
        inherent_once_neighbor,
        |_, wire| {
            wire["decisions"][1]["permission_id"] = wire["decisions"][0]["permission_id"].clone();
        },
        InherentSnapshotControl::None,
    );
}
// A20
#[test]
fn inherent_acceptance_refuses_permission_id_on_deny_neighbor() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["decisions"][1]["permission_id"] = wire["decisions"][0]["permission_id"].clone();
        },
        InherentSnapshotControl::None,
    );
}
// A21
#[test]
fn inherent_acceptance_refuses_wrong_derived_always_permission_id() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            let expected = inherent_acceptance_digest(wire);
            wire["decisions"][0]["permission_id"] =
                json!(format!("b4pd_{}", wrong_inherent_digest(&expected)));
        },
        InherentSnapshotControl::None,
    );
}
// A22
#[test]
fn inherent_acceptance_refuses_no_effective_always() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["decisions"][0]["decision"] = json!("once");
            wire["decisions"][0]["permission_id"] = Value::Null;
        },
        InherentSnapshotControl::None,
    );
}
// A23
#[test]
fn inherent_acceptance_refuses_origin_mirror_mismatch_with_repaired_scope_id() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["origin"]["application"] = json!("other-app");
            repair_inherent_acceptance(wire);
        },
        InherentSnapshotControl::None,
    );
}
// A24
#[test]
fn inherent_acceptance_refuses_empty_origin_label_with_repaired_scope_id() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["origin"]["tenant"] = json!("");
            repair_inherent_acceptance(wire);
        },
        InherentSnapshotControl::None,
    );
}
// A25
#[test]
fn inherent_acceptance_refuses_nonterminal_output_preserved_by_old_serde() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| wire["before_snapshot"]["output"] = Value::Null,
        InherentSnapshotControl::RefusedWithOutputNull,
    );
}
// A26
#[test]
fn inherent_acceptance_refuses_kernel_valid_running_waiting_projection() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["before_snapshot"]["status"] = json!("running");
            wire["before_snapshot"]["tasks"][1]["status"] = json!("ready");
            wire["before_snapshot"]["tasks"][1]["interrupt"] = Value::Null;
            wire["decisions"].as_array_mut().unwrap().truncate(1);
        },
        InherentSnapshotControl::Valid,
    );
}
// D01
#[test]
fn inherent_decision_refuses_route_mismatch_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["route"] = json!("/other#agent");
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D02
#[test]
fn inherent_decision_refuses_absent_effect_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["effect_id"] = json!("absent-effect");
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D03
#[test]
fn inherent_decision_refuses_nonrequired_effect_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["definition"]["batches"][0][0]["requires_approval"] = json!(false);
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D04
#[test]
fn inherent_decision_refuses_empty_batch_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["definition"]["batches"]
                .as_array_mut()
                .unwrap()
                .insert(0, json!([]));
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D05
#[test]
fn inherent_decision_refuses_duplicate_task_id_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["definition"]["batches"][0][1]["id"] = json!("first");
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D06
#[test]
fn inherent_decision_refuses_duplicate_effect_id_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["definition"]["batches"][0][1]["effect_id"] = json!("first-effect");
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D07
#[test]
fn inherent_decision_refuses_attempt_budget_below_task_count_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["definition"]["max_attempts"] = json!(1);
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D08
#[test]
fn inherent_decision_refuses_origin_mirror_mismatch_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["origin"]["application"] = json!("other-app");
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D09
#[test]
fn inherent_decision_refuses_control_policy_label_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["policy_revision"] = json!("\n");
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}
// D10
#[test]
fn inherent_decision_refuses_mutually_equal_wrong_scope_hash_and_id() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            let expected = independent_digest(&json!({"scope": wire["scope"], "version": 1}));
            let wrong = wrong_inherent_digest(&expected);
            wire["permission_id"] = json!(format!("b4pd_{wrong}"));
            wire["scope_digest"] = json!(wrong);
        },
        InherentSnapshotControl::None,
    );
}
// D11
#[test]
fn inherent_decision_refuses_id_mismatch_with_correct_scope_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            let expected = independent_digest(&json!({"scope": wire["scope"], "version": 1}));
            wire["permission_id"] = json!(format!("b4pd_{}", wrong_inherent_digest(&expected)));
        },
        InherentSnapshotControl::None,
    );
}
// U01
#[test]
fn inherent_use_refuses_wrong_committed_revision() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| wire["committed_revision"] = json!(3),
        InherentSnapshotControl::None,
    );
}
// U02
#[test]
fn inherent_use_refuses_clock_before_ready_witness() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| wire["facts"]["now_ms"] = json!(100),
        InherentSnapshotControl::None,
    );
}
// U03
#[test]
fn inherent_use_refuses_execution_mismatch() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| wire["execution_id"] = json!("other-execution"),
        InherentSnapshotControl::None,
    );
}
// U04
#[test]
fn inherent_use_refuses_absent_selected_task() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| wire["task_id"] = json!("absent-task"),
        InherentSnapshotControl::None,
    );
}
// U05
#[test]
fn inherent_use_refuses_kernel_valid_waiting_selected_task() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |samples, wire| {
            wire["before_snapshot"]["tasks"][0]["status"] = json!("waiting_for_approval");
            wire["before_snapshot"]["tasks"][0]["interrupt"] =
                samples.acceptance["before_snapshot"]["tasks"][0]["interrupt"].clone();
        },
        InherentSnapshotControl::Valid,
    );
}
// U06
#[test]
fn inherent_use_refuses_kernel_valid_already_approved_ready_task() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |samples, wire| {
            wire["before_snapshot"]["tasks"][0]["approved"] = json!(true);
            wire["before_snapshot"]["tasks"][0]["interrupt"] =
                samples.acceptance["before_snapshot"]["tasks"][0]["interrupt"].clone();
        },
        InherentSnapshotControl::Valid,
    );
}
// U07
#[test]
fn inherent_use_refuses_kernel_valid_ready_human_interrupt() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |samples, wire| {
            wire["before_snapshot"]["tasks"][0]["interrupt"] =
                samples.acceptance["before_snapshot"]["tasks"][0]["interrupt"].clone();
        },
        InherentSnapshotControl::Valid,
    );
}
// U08
#[test]
fn inherent_use_refuses_wrong_exact_call_digest() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| {
            let task = &wire["before_snapshot"]["tasks"][0];
            let expected = independent_digest(
                &json!({"input": task["input"], "task": task["spec"], "version": 1}),
            );
            wire["call_digest"] = json!(wrong_inherent_digest(&expected));
        },
        InherentSnapshotControl::None,
    );
}
// U09
#[test]
fn inherent_use_refuses_marker_id_mismatch() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| {
            wire["marker"]["id"] =
                json!("b4pu_2222222222222222222222222222222222222222222222222222222222222222")
        },
        InherentSnapshotControl::None,
    );
}
// U10
#[test]
fn inherent_use_refuses_nonnull_marker_prompt() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| wire["marker"]["prompt"] = json!(0),
        InherentSnapshotControl::None,
    );
}
// U11
#[test]
fn inherent_use_refuses_old_kernel_input_disagreement_with_repaired_call_digest() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| {
            wire["before_snapshot"]["tasks"][0]["input"]["payload"]["a"] = json!(false);
            repair_inherent_call(wire);
        },
        InherentSnapshotControl::Refused,
    );
}
// U12
#[test]
fn inherent_use_refuses_kernel_valid_nonrequired_ready_with_repaired_call_digest() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| {
            wire["before_snapshot"]["definition"]["batches"][0][0]["requires_approval"] =
                json!(false);
            wire["before_snapshot"]["tasks"][0]["spec"]["requires_approval"] = json!(false);
            repair_inherent_call(wire);
        },
        InherentSnapshotControl::Valid,
    );
}

// Raw-byte controls keep every typed value/hash exact and change only trailing
// ASCII spaces. Cumulative exhaustion does not predict a successful prefix.
const BYTE_ACCEPTANCE_CAP: usize = 65_536;
const BYTE_DECISION_CAP: usize = 65_536;
const BYTE_USE_CAP: usize = 16_384;
const BYTE_CUMULATIVE_ATTEMPTS: usize = 129;

fn record_raw_byte_boundary<R: FixtureCodec>(cap: usize) {
    let (base_len, captured) = with_samples(|samples| {
        let base = canonical(R::wire(samples));
        let base_len = base.len();
        if base_len > cap {
            return (base_len, None);
        }
        let mut exact = Vec::with_capacity(cap + 1);
        exact.extend_from_slice(&base);
        exact.resize(cap, b' ');
        let mut faulty = Vec::with_capacity(cap + 1);
        faulty.extend_from_slice(&exact);
        faulty.push(b' ');
        let mut control_budget = PermissionEvidenceBudget::new();
        let control = R::decode(&exact, &mut control_budget).map(|_| ());
        let mut budget = PermissionEvidenceBudget::new();
        let result = R::decode(&faulty, &mut budget).map(|_| ());
        (base_len, Some((exact.len(), faulty.len(), control, result)))
    });
    assert!(
        base_len <= cap,
        "actual small genuine canonical neighbor fits literal cap after retirement"
    );
    let (exact_len, faulty_len, control, result) = captured.expect("bounded raw Results captured");
    assert_eq!(exact_len, cap, "actual raw positive length reaches cap");
    assert_eq!(
        faulty_len,
        cap + 1,
        "actual raw fault adds exactly one ASCII space"
    );
    assert!(
        control.is_ok(),
        "exact-cap genuine neighbor succeeds after physical retirement"
    );
    assert!(
        matches!(result, Err(PermissionError::LimitExceeded)),
        "cap+1 must reach its own byte limit after physical retirement"
    );
}

// B-A
#[test]
fn acceptance_raw_record_byte_cap_refuses_one_extra_space() {
    record_raw_byte_boundary::<PermissionAcceptance>(BYTE_ACCEPTANCE_CAP);
}
// B-D
#[test]
fn decision_raw_record_byte_cap_refuses_one_extra_space() {
    record_raw_byte_boundary::<PermissionDecision>(BYTE_DECISION_CAP);
}
// B-U
#[test]
fn use_raw_record_byte_cap_refuses_one_extra_space() {
    record_raw_byte_boundary::<PermissionUse>(BYTE_USE_CAP);
}

// C-BYTES
#[test]
fn repeated_real_decision_exhausts_one_shared_account_within_bounded_calls() {
    let (base_len, captured) = with_samples(|samples| {
        let base = canonical(&samples.decision);
        let base_len = base.len();
        if base_len > BYTE_DECISION_CAP {
            return (base_len, None);
        }
        let mut record = Vec::with_capacity(BYTE_DECISION_CAP);
        record.extend_from_slice(&base);
        record.resize(BYTE_DECISION_CAP, b' ');
        let mut budget = PermissionEvidenceBudget::new();
        let mut successes = 0;
        let mut first_error = None;
        for index in 1..=BYTE_CUMULATIVE_ATTEMPTS {
            match PermissionDecision::decode_record(&record, &mut budget) {
                Ok(decoded) => {
                    drop(decoded);
                    successes += 1;
                }
                Err(error) => {
                    first_error = Some((index, error));
                    break;
                }
            }
        }
        let usage = budget.usage();
        (
            base_len,
            Some((
                record.len(),
                successes,
                first_error,
                usage.encoded_bytes(),
                usage.structural_nodes(),
            )),
        )
    });
    assert!(
        base_len <= BYTE_DECISION_CAP,
        "actual small genuine Decision fits literal cap after retirement"
    );
    let (raw_len, successes, first_error, bytes, nodes) =
        captured.expect("bounded shared-account capture completed");
    assert_eq!(
        raw_len, BYTE_DECISION_CAP,
        "one immutable padded real Decision reaches raw cap"
    );
    assert!(
        successes > 0 && successes <= BYTE_CUMULATIVE_ATTEMPTS,
        "first actual direct decode succeeds and bounded successful prefix is finite"
    );
    if let Some((index, _)) = &first_error {
        assert!(
            *index > 1 && *index <= BYTE_CUMULATIVE_ATTEMPTS,
            "first failure follows a success within bounded actual attempts"
        );
        assert_eq!(
            *index,
            successes + 1,
            "capture stops at its first actual error"
        );
    }
    assert!(
        matches!(first_error, Some((_, PermissionError::LimitExceeded))),
        "shared account must reach finite LimitExceeded within129 after physical retirement"
    );
    assert!(
        bytes > 0 || nodes > 0,
        "supplementary finite usage is inspected only after own exhaustion oracle"
    );
}

// Four independent boundary qualifiers. The entire original target above stays
// exact, including its smaller expected helpers and genuine physical fixture.
const MAX_BOUNDARY_FIXTURE_BYTES: usize = 65_536;
const MAX_BOUNDARY_FIXTURE_NODES: usize = 20_000;
const MAX_BOUNDARY_FIXTURE_DEPTH: usize = 65;
const BOUNDARY_SHARED_BYTES: usize = 8_388_608;
const BOUNDARY_SHARED_NODES: usize = 65_536;
const BOUNDARY_CALL_ATTEMPTS: usize = 11;
const BOUNDARY_CALL_BYTES_UPPER: usize = 524_288;
const BOUNDARY_CALL_NODES_LOWER: usize = 12_322;

#[derive(Clone, Copy)]
enum BoundaryInputCase {
    Depth64,
    Depth65,
    NodeControl1024,
    NodeFault5393,
    Call6144,
}

fn boundary_input(recipe: BoundaryInputCase) -> Map<String, Value> {
    let payload = match recipe {
        BoundaryInputCase::Depth64 | BoundaryInputCase::Depth65 => {
            let wraps = match recipe {
                BoundaryInputCase::Depth64 => 59,
                BoundaryInputCase::Depth65 => 60,
                _ => unreachable!("closed depth recipes"),
            };
            let mut nested = Value::Null;
            for _ in 0..wraps {
                nested = Value::Array(vec![nested]);
            }
            nested
        }
        BoundaryInputCase::NodeControl1024
        | BoundaryInputCase::NodeFault5393
        | BoundaryInputCase::Call6144 => {
            let zeros = match recipe {
                BoundaryInputCase::NodeControl1024 => 1_024,
                BoundaryInputCase::NodeFault5393 => 5_393,
                BoundaryInputCase::Call6144 => 6_144,
                _ => unreachable!("closed array recipes"),
            };
            Value::Array(vec![json!(0); zeros])
        }
    };
    Map::from_iter([("payload".into(), payload)])
}

fn boundary_prompts(recipe: BoundaryInputCase) -> [Value; 2] {
    match recipe {
        BoundaryInputCase::Depth64 | BoundaryInputCase::Depth65 => [
            json!({"question": "Proceed with first?", "arguments": {"b": 2, "a": null}}),
            json!({"question": "Proceed with second?", "arguments": {"token": BUSINESS_TOKEN, "a": [2, 1]}}),
        ],
        BoundaryInputCase::NodeControl1024 | BoundaryInputCase::Call6144 => {
            [Value::Null, json!([0, 0])]
        }
        BoundaryInputCase::NodeFault5393 => [Value::Null, json!([0, 0, 0])],
    }
}

// This helper only sees the five closed, independently sized fixture recipes.
// Its iterative shape bound precedes recursive expected sorting/serialization.
fn boundary_metrics(value: &Value) -> FixtureMetrics {
    let mut pending = vec![(value, 0usize)];
    let mut nodes = 0usize;
    let mut depth = 0usize;
    while let Some((value, ancestors)) = pending.pop() {
        nodes = nodes.checked_add(1).expect("finite fixture nodes");
        assert!(nodes <= MAX_BOUNDARY_FIXTURE_NODES, "boundary node bound");
        match value {
            Value::Object(object) => {
                let next = ancestors.checked_add(1).expect("finite fixture depth");
                depth = depth.max(next);
                assert!(depth <= MAX_BOUNDARY_FIXTURE_DEPTH, "boundary depth bound");
                nodes = nodes
                    .checked_add(object.len())
                    .expect("finite fixture keys");
                assert!(nodes <= MAX_BOUNDARY_FIXTURE_NODES, "boundary key bound");
                for child in object.values() {
                    pending.push((child, next));
                }
            }
            Value::Array(array) => {
                let next = ancestors.checked_add(1).expect("finite fixture depth");
                depth = depth.max(next);
                assert!(depth <= MAX_BOUNDARY_FIXTURE_DEPTH, "boundary depth bound");
                for child in array {
                    pending.push((child, next));
                }
            }
            _ => {}
        }
        assert!(
            pending.len() <= MAX_BOUNDARY_FIXTURE_NODES,
            "boundary work bound"
        );
    }
    let bytes = serde_json::to_vec(value).unwrap().len();
    assert!(bytes <= MAX_BOUNDARY_FIXTURE_BYTES, "boundary byte bound");
    FixtureMetrics {
        bytes,
        nodes,
        depth,
    }
}

fn boundary_canonical(value: &Value) -> Vec<u8> {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut entries: Vec<_> = object.iter().collect();
                entries.sort_by(|left, right| left.0.cmp(right.0));
                Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key.clone(), sorted(value)))
                        .collect(),
                )
            }
            Value::Array(array) => Value::Array(array.iter().map(sorted).collect()),
            scalar => scalar.clone(),
        }
    }
    boundary_metrics(value);
    serde_json::to_vec(&sorted(value)).unwrap()
}

fn boundary_independent_digest(value: &Value) -> String {
    let hash = Sha256::digest(boundary_canonical(value));
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in hash {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 15)] as char);
    }
    encoded
}

fn boundary_samples(ready: &Snapshot, before: &ApprovalView) -> Samples {
    let parked = &before.execution.snapshot;
    let scope = scope_wire(&before.thread, &parked.definition, true);
    let scope_digest = boundary_independent_digest(&json!({"scope": scope, "version": 1}));
    let permission_id = format!("b4pd_{scope_digest}");
    let mut decisions = vec![];
    for (index, task) in parked.tasks.iter().enumerate() {
        let interrupt = task.interrupt.as_ref().unwrap();
        let grant = before
            .grants
            .iter()
            .find(|grant| grant.interrupt_id == interrupt.id)
            .unwrap();
        decisions.push(json!({
            "task_id": task.spec.id,
            "interrupt_id": interrupt.id,
            "grant_ref": {"execution_id": grant.execution_id, "interrupt_id": grant.interrupt_id},
            "decision": if index == 0 { "always" } else { "deny" },
            "proposal_digest": grant.proposal_digest,
            "task_parking_revision": grant.task_parking_revision,
            "binding": if index == 0 { binding_wire(true) } else { Value::Null },
            "permission_id": if index == 0 { json!(permission_id) } else { Value::Null }
        }));
    }
    let acceptance = json!({
        "record_version": 1,
        "thread_id": before.thread.id,
        "thread_generation": before.thread.generation,
        "origin": before.thread.origin.as_ref().unwrap(),
        "application": {"application": "record-app", "revision": "application-1"},
        "policy_revision": "policy-1",
        "before_snapshot": parked,
        "turn_id": "record-resume-turn",
        "facts": {"now_ms": 104},
        "committed_revision": parked.revision.checked_add(1).unwrap(),
        "decisions": decisions
    });
    let mut once_acceptance = acceptance.clone();
    once_acceptance["decisions"][1]["decision"] = json!("once");
    once_acceptance["decisions"][1]["binding"] = binding_wire(false);
    let decision = json!({
        "record_version": 1,
        "permission_id": permission_id,
        "scope_digest": scope_digest,
        "scope": scope,
        "creator_execution_id": parked.execution_id,
        "creator_turn_id": "record-resume-turn",
        "creator_task_id": "first",
        "creator_interrupt_id": "first-interrupt"
    });
    let call_digest = boundary_independent_digest(&json!({
        "input": ready.tasks[0].input,
        "task": ready.tasks[0].spec,
        "version": 1
    }));
    let use_record = json!({
        "record_version": 1,
        "permission_version": 1,
        "use_id": USE_ID,
        "permission_id": permission_id,
        "thread_id": before.thread.id,
        "thread_generation": before.thread.generation,
        "execution_id": ready.execution_id,
        "task_id": "first",
        "before_snapshot": ready,
        "facts": {"now_ms": 102},
        "committed_revision": ready.revision.checked_add(1).unwrap(),
        "marker": {"id": USE_ID, "prompt": null},
        "call_digest": call_digest,
        "application": {"application": "record-app", "revision": "application-1"},
        "tool_id": "first-tool",
        "tool_revision": "tool-1",
        "policy_revision": "policy-1",
        "constraint_revision": "constraint-1"
    });
    for wire in [&acceptance, &once_acceptance, &decision, &use_record] {
        boundary_canonical(wire);
    }
    Samples {
        acceptance,
        once_acceptance,
        decision,
        use_record,
        parked: parked.clone(),
        ready: ready.clone(),
        thread: before.thread.clone(),
        scope_digest,
        call_digest,
    }
}

fn with_boundary_witness<T>(
    recipe: BoundaryInputCase,
    operation: impl FnOnce(&Snapshot, &ApprovalView) -> T,
) -> T {
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread_id = ThreadId::new("record-thread").unwrap();
    let execution_id = ExecutionId::new("record-execution").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "record-app".into(),
            tenant: "fixture-tenant".into(),
            principal: "fixture-principal".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/records#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![
            TaskSpec {
                id: TaskId::new("first").unwrap(),
                effect_id: EffectId::new("first-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
            TaskSpec {
                id: TaskId::new("second").unwrap(),
                effect_id: EffectId::new("second-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            },
        ]],
        max_attempts: 4,
    };
    let application = NativeApplication::new(
        ApplicationIdentity {
            application: "record-app".into(),
            revision: "application-1".into(),
        },
        vec![RouteRegistration {
            definition: definition.clone(),
            effects: BTreeMap::from([
                (
                    EffectId::new("first-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                ),
                (
                    EffectId::new("second-effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                ),
            ]),
            input_validator: None,
        }],
    )
    .unwrap();
    let registered = application.definition(&definition.route).unwrap();
    assert_eq!(registered, &definition);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    assert_eq!(original_owner, 1);
    assert!(!store.durable_permissions());
    store
        .create_thread_with(ThreadSeed {
            id: thread_id.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution_id.clone(),
        TurnId::new("record-original-turn").unwrap(),
        original_owner,
        registered.clone(),
        boundary_input(recipe),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread_id, initial).unwrap();
    let started = store
        .commit(
            &execution_id,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let ready = started.snapshot;
    assert_eq!(ready.revision, 1);
    assert_eq!(ready.status, ExecutionStatus::Running);
    assert_eq!(ready.tasks.len(), 2);
    assert!(ready.output.is_none() && ready.failure.is_none());
    assert!(ready.attempt_history.is_empty() && ready.attempts_used == 0);
    for task in &ready.tasks {
        assert_eq!(task.status, TaskStatus::Ready);
        assert!(!task.approved);
        assert!(
            task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
                && task.interrupt.is_none()
        );
        assert_eq!(task.input, ready.values);
    }
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompts = boundary_prompts(recipe);
    for (index, name) in ["first", "second"].iter().enumerate() {
        let parked = service
            .park(
                &context,
                &execution_id,
                fence(store.as_ref(), &thread_id),
                TaskId::new(*name).unwrap(),
                InterruptId::new(format!("{name}-interrupt")).unwrap(),
                prompts[index].clone(),
                1_000,
                facts(102 + index as u64),
            )
            .unwrap();
        assert_eq!(parked.snapshot.revision, 2 + index as u64);
        assert_eq!(
            parked.snapshot.status,
            if index == 0 {
                ExecutionStatus::Running
            } else {
                ExecutionStatus::WaitingForApproval
            }
        );
    }
    let pending = service
        .pending(&context, &execution_id, fence(store.as_ref(), &thread_id))
        .unwrap();
    let before = store.approval_view(&execution_id).unwrap().unwrap();
    let ordinary = store.thread_view(&thread_id).unwrap().unwrap();
    assert_eq!(
        before.execution.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert_eq!(before.execution.snapshot.revision, 3);
    assert_eq!(ordinary.execution.as_ref(), Some(&before.execution));
    assert_eq!(ordinary.thread, before.thread);
    assert_eq!(before.grants.len(), 2);
    assert_eq!(pending.len(), 2);
    assert!(before.acceptances.is_empty());
    assert_eq!(before.thread.origin.as_ref(), Some(&context.actor));
    for (index, task) in before.execution.snapshot.tasks.iter().enumerate() {
        assert_eq!(task.status, TaskStatus::WaitingForApproval);
        assert_eq!(task.spec, definition.batches[0][index]);
        assert!(
            !task.approved
                && task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
        );
        let interrupt = task.interrupt.as_ref().unwrap();
        assert_eq!(interrupt.prompt, prompts[index]);
        let grant = before
            .grants
            .iter()
            .find(|grant| grant.interrupt_id == interrupt.id)
            .unwrap();
        let projection = pending
            .iter()
            .find(|entry| entry.interrupt_id == interrupt.id)
            .unwrap();
        assert_eq!(grant.thread_id, thread_id);
        assert_eq!(grant.thread_generation, before.thread.generation);
        assert_eq!(grant.origin, context.actor);
        assert_eq!(grant.execution_id, execution_id);
        assert_eq!(grant.route, definition.route);
        assert_eq!(grant.definition_revision, definition.revision);
        assert_eq!(grant.task_id, task.spec.id);
        assert_eq!(grant.effect_id, task.spec.effect_id);
        assert_eq!(grant.policy_revision, context.policy_revision);
        assert_eq!(grant.prompt, interrupt.prompt);
        assert_eq!(grant.task_parking_revision, 2 + index as u64);
        assert_eq!(grant.issued_at_ms, 102 + index as u64);
        assert_eq!(grant.expires_at_ms, 1_102 + index as u64);
        assert!(!grant.consumed && grant.issued_at_ms <= 104 && 104 < grant.expires_at_ms);
        assert!(
            grant.token == projection.token,
            "real pending token equality"
        );
        assert!(
            grant.verifier == token_verifier(&projection.token),
            "real token verifier equality"
        );
        assert!(
            grant.resume_key == projection.resume_key,
            "real pending resume key equality"
        );
        assert_eq!(projection.task_id, task.spec.id);
        assert_eq!(projection.prompt, grant.prompt);
        assert_eq!(projection.expires_at_ms, grant.expires_at_ms);
        let expected = boundary_independent_digest(
            &json!({"input": task.input, "prompt": interrupt.prompt, "task": task.spec, "version": 1}),
        );
        assert_eq!(grant.proposal_digest, expected);
    }
    let result = operation(&ready, &before);
    assert_eq!(store.thread_view(&thread_id).unwrap().unwrap(), ordinary);
    assert_eq!(store.approval_view(&execution_id).unwrap().unwrap(), before);
    store.readiness().unwrap();
    drop(pending);
    drop(service);
    drop(authority);
    drop(application);
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "all old store handles retired"
    );
    drop(store);
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_ordinary = next.thread_view(&thread_id).unwrap().unwrap();
    let next_before = next.approval_view(&execution_id).unwrap().unwrap();
    next.readiness().unwrap();
    drop(next);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(next_ordinary, ordinary);
    assert_eq!(next_before, before);
    assert_eq!(
        next_before.execution.snapshot.owner_generation,
        original_owner
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(next_before);
    drop(before);
    result
}

struct BoundaryDecodeCapture {
    result: Result<(), PermissionError>,
    usage: PermissionEvidenceUsage,
    row: FixtureMetrics,
    input: FixtureMetrics,
    old_proposals_ok: bool,
}

fn boundary_decode<R: FixtureCodec>(recipe: BoundaryInputCase) -> BoundaryDecodeCapture {
    with_boundary_witness(recipe, |ready, before| {
        let samples = boundary_samples(ready, before);
        let wire = R::wire(&samples);
        let row = boundary_metrics(wire);
        let input = boundary_metrics(&json!(ready.values));
        let old_proposals_ok = before.execution.snapshot.tasks.iter().all(|task| {
            let prompt = &task.interrupt.as_ref().unwrap().prompt;
            canonical_proposal(task, prompt).is_ok()
        });
        let raw = boundary_canonical(wire);
        let mut budget = PermissionEvidenceBudget::new();
        let result = R::decode(&raw, &mut budget).map(|_| ());
        BoundaryDecodeCapture {
            result,
            usage: budget.usage(),
            row,
            input,
            old_proposals_ok,
        }
    })
}

fn assert_boundary_metrics(actual: FixtureMetrics, expected: FixtureMetrics) {
    assert_eq!(actual.bytes, expected.bytes, "exact genuine fixture bytes");
    assert_eq!(actual.nodes, expected.nodes, "exact genuine fixture nodes");
    assert_eq!(actual.depth, expected.depth, "exact genuine fixture depth");
}

fn boundary_depth_rejection<R: FixtureCodec>(
    positive: FixtureMetrics,
    fault: FixtureMetrics,
    row_cap: usize,
) {
    let control = boundary_decode::<R>(BoundaryInputCase::Depth64);
    let result = boundary_decode::<R>(BoundaryInputCase::Depth65);
    assert!(
        control.result.is_ok(),
        "genuine depth64 succeeds after both closures"
    );
    assert_boundary_metrics(control.row, positive);
    assert_boundary_metrics(result.row, fault);
    assert_boundary_metrics(
        control.input,
        FixtureMetrics {
            bytes: 134,
            nodes: 62,
            depth: 60,
        },
    );
    assert_boundary_metrics(
        result.input,
        FixtureMetrics {
            bytes: 136,
            nodes: 63,
            depth: 61,
        },
    );
    assert!(
        control.old_proposals_ok && result.old_proposals_ok,
        "both old genuine proposals remain legal"
    );
    assert!(control.row.bytes < row_cap && result.row.bytes < row_cap);
    assert!(control.row.nodes < 16_384 && result.row.nodes < 16_384);
    assert!(
        matches!(result.result, Err(PermissionError::LimitExceeded)),
        "whole record depth65 must be LimitExceeded after full genuine closures"
    );
    // These later raw-stage attribution guards have zero initial RED credit.
    assert!(result.usage.structural_nodes() > 0);
    assert!(result.usage.structural_nodes() <= result.row.nodes);
    assert!(result.usage.structural_nodes() < BOUNDARY_SHARED_NODES);
    assert!(result.usage.encoded_bytes() >= result.row.bytes);
    assert!(result.usage.encoded_bytes() < BOUNDARY_SHARED_BYTES);
}

#[test]
fn boundary_acceptance_refuses_whole_depth65_after_genuine64() {
    boundary_depth_rejection::<PermissionAcceptance>(
        FixtureMetrics {
            bytes: 2_815,
            nodes: 398,
            depth: 64,
        },
        FixtureMetrics {
            bytes: 2_821,
            nodes: 401,
            depth: 65,
        },
        65_536,
    );
}

#[test]
fn boundary_use_refuses_whole_depth65_after_genuine64() {
    boundary_depth_rejection::<PermissionUse>(
        FixtureMetrics {
            bytes: 2_146,
            nodes: 334,
            depth: 64,
        },
        FixtureMetrics {
            bytes: 2_152,
            nodes: 337,
            depth: 65,
        },
        16_384,
    );
}

#[test]
fn boundary_acceptance_refuses_whole_nodes16385_after_genuine3277() {
    let control = boundary_decode::<PermissionAcceptance>(BoundaryInputCase::NodeControl1024);
    let result = boundary_decode::<PermissionAcceptance>(BoundaryInputCase::NodeFault5393);
    assert!(
        control.result.is_ok(),
        "genuine3277-node neighbor succeeds after both closures"
    );
    assert_boundary_metrics(
        control.row,
        FixtureMetrics {
            bytes: 8_448,
            nodes: 3_277,
            depth: 6,
        },
    );
    assert_boundary_metrics(
        result.row,
        FixtureMetrics {
            bytes: 34_664,
            nodes: 16_385,
            depth: 6,
        },
    );
    assert_boundary_metrics(
        control.input,
        FixtureMetrics {
            bytes: 2_061,
            nodes: 1_027,
            depth: 2,
        },
    );
    assert_boundary_metrics(
        result.input,
        FixtureMetrics {
            bytes: 10_799,
            nodes: 5_396,
            depth: 2,
        },
    );
    assert!(
        control.old_proposals_ok && result.old_proposals_ok,
        "both actual parked proposals remain legal"
    );
    assert!(control.row.bytes < 65_536 && result.row.bytes < 65_536);
    assert!(
        matches!(result.result, Err(PermissionError::LimitExceeded)),
        "whole record16385 nodes must be LimitExceeded after full genuine closures"
    );
    // This raw-only signature is qualified only when this later guard is reached.
    assert_eq!(result.usage.structural_nodes(), 16_384);
    assert!(result.usage.structural_nodes() < BOUNDARY_SHARED_NODES);
    assert!(result.usage.encoded_bytes() >= result.row.bytes);
    assert!(result.usage.encoded_bytes() < BOUNDARY_SHARED_BYTES);
}

struct BoundaryCallCapture {
    first_ok: bool,
    successful: usize,
    error_index: Option<usize>,
    error: Option<PermissionError>,
    all_digests_match: bool,
    usage_steps: Vec<PermissionEvidenceUsage>,
    ready: FixtureMetrics,
    input: FixtureMetrics,
    call: FixtureMetrics,
    old_proposals_ok: bool,
}

#[test]
fn boundary_call_digest_exhausts_shared_nodes_after_genuine_success() {
    let captured = with_boundary_witness(BoundaryInputCase::Call6144, |ready, before| {
        // Ready-only capture: no outside-cap Acceptance/Use proposal is built.
        let task = &ready.tasks[0];
        let call = json!({"input": task.input, "task": task.spec, "version": 1});
        let expected = boundary_independent_digest(&call);
        let ready_metrics = boundary_metrics(&json!(ready));
        let input_metrics = boundary_metrics(&json!(ready.values));
        let call_metrics = boundary_metrics(&call);
        let old_proposals_ok =
            before.execution.snapshot.tasks.iter().all(|task| {
                canonical_proposal(task, &task.interrupt.as_ref().unwrap().prompt).is_ok()
            });
        let mut budget = PermissionEvidenceBudget::new();
        let mut first_ok = false;
        let mut successful = 0usize;
        let mut error_index = None;
        let mut error = None;
        let mut all_digests_match = true;
        let mut usage_steps = Vec::with_capacity(BOUNDARY_CALL_ATTEMPTS);
        for index in 0..BOUNDARY_CALL_ATTEMPTS {
            let result = permission_call_digest(task, &mut budget);
            if index == 0 {
                first_ok = result.is_ok();
            }
            usage_steps.push(budget.usage());
            match result {
                Ok(digest) => {
                    successful += 1;
                    all_digests_match &= digest == expected;
                    // Digest is normally dropped here before another actual call.
                }
                Err(caught) => {
                    error_index = Some(index);
                    error = Some(caught);
                    break;
                }
            }
        }
        BoundaryCallCapture {
            first_ok,
            successful,
            error_index,
            error,
            all_digests_match,
            usage_steps,
            ready: ready_metrics,
            input: input_metrics,
            call: call_metrics,
            old_proposals_ok,
        }
    });
    assert!(
        captured.first_ok && captured.successful > 0,
        "genuine first actual hash succeeds after full closure"
    );
    assert!(captured.successful <= BOUNDARY_CALL_ATTEMPTS);
    assert_boundary_metrics(
        captured.ready,
        FixtureMetrics {
            bytes: 37_836,
            nodes: 18_543,
            depth: 5,
        },
    );
    assert_boundary_metrics(
        captured.input,
        FixtureMetrics {
            bytes: 12_301,
            nodes: 6_147,
            depth: 2,
        },
    );
    assert_boundary_metrics(
        captured.call,
        FixtureMetrics {
            bytes: 12_415,
            nodes: 6_161,
            depth: 3,
        },
    );
    assert!(
        captured.old_proposals_ok,
        "both actual old parked proposals remain legal"
    );
    assert!(captured.usage_steps.len() <= BOUNDARY_CALL_ATTEMPTS);
    // Crucially, floor None reaches the primary error oracle; do not require Some.
    if let Some(index) = captured.error_index {
        assert_eq!(index, captured.successful);
        assert_eq!(captured.usage_steps.len(), index.checked_add(1).unwrap());
        assert!(captured.error.is_some());
    } else {
        assert!(captured.error.is_none());
        assert_eq!(captured.successful, BOUNDARY_CALL_ATTEMPTS);
        assert_eq!(captured.usage_steps.len(), BOUNDARY_CALL_ATTEMPTS);
    }
    assert!(
        matches!(captured.error, Some(PermissionError::LimitExceeded)),
        "same bounded call account must capture LimitExceeded after full genuine closure"
    );
    // Exact hash and structural-axis guards are initially unreached/zero credit.
    assert!(
        captured.all_digests_match,
        "exact independent genuine call digests"
    );
    let error_index = captured
        .error_index
        .expect("index after captured LimitExceeded");
    assert!(error_index <= 5, "six full two-pass successes cannot fit");
    let usage = captured.usage_steps.last().unwrap();
    assert!(usage.structural_nodes() <= BOUNDARY_SHARED_NODES);
    assert!(
        usage.structural_nodes()
            >= BOUNDARY_CALL_NODES_LOWER
                .checked_mul(captured.successful)
                .unwrap()
    );
    assert!(
        usage.encoded_bytes()
            <= BOUNDARY_CALL_BYTES_UPPER
                .checked_mul(captured.usage_steps.len())
                .unwrap()
    );
    assert!(usage.encoded_bytes() < BOUNDARY_SHARED_BYTES);
    for pair in captured.usage_steps.windows(2) {
        assert!(pair[0].encoded_bytes() <= pair[1].encoded_bytes());
        assert!(pair[0].structural_nodes() <= pair[1].structural_nodes());
    }
}

// These are untrusted negative proposals derived from the same genuine witnesses.
// Overflow controls retain a Snapshot accepted by the old kernel validator.
#[test]
fn inherent_decision_refuses_attempt_budget65_with_repaired_hash() {
    inherent_rejection::<PermissionDecision>(
        PermissionDecision::wire,
        |_, wire| {
            wire["scope"]["definition"]["max_attempts"] = json!(65);
            repair_inherent_decision(wire);
        },
        InherentSnapshotControl::None,
    );
}

#[test]
fn inherent_acceptance_refuses_committed_revision_overflow() {
    inherent_rejection::<PermissionAcceptance>(
        PermissionAcceptance::wire,
        |_, wire| {
            wire["before_snapshot"]["revision"] = json!(u64::MAX);
            wire["committed_revision"] = json!(0);
        },
        InherentSnapshotControl::Valid,
    );
}

#[test]
fn inherent_use_refuses_committed_revision_overflow() {
    inherent_rejection::<PermissionUse>(
        PermissionUse::wire,
        |_, wire| {
            wire["before_snapshot"]["revision"] = json!(u64::MAX);
            wire["committed_revision"] = json!(0);
        },
        InherentSnapshotControl::Valid,
    );
}
