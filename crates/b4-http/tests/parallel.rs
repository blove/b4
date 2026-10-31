use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tower::ServiceExt;

struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("owner") {
            return Err(AuthenticationError);
        }
        Ok(Principal {
            application: "app".into(),
            tenant: "tenant".into(),
            principal: "owner".into(),
        })
    }
}
struct Allow;
impl ResourcePolicy for Allow {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        true
    }
}
struct ObservedStore {
    inner: MemoryStore,
    earlier_status_at_second_dispatch: Mutex<Option<TaskStatus>>,
    park_gate: Option<Arc<Gate>>,
    first_dispatch_gate: Option<(ThreadId, Arc<Gate>)>,
    failure_commit_gate: Option<Arc<Gate>>,
}
impl Store for ObservedStore {
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, snapshot: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, snapshot)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        snapshot: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, snapshot)
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        let observed = plan
            .snapshot
            .tasks
            .get(1)
            .filter(|task| task.status == TaskStatus::InFlight)
            .and_then(|_| plan.snapshot.tasks.first())
            .map(|task| task.status);
        let first_dispatch = plan.snapshot.attempts_used == 1
            && plan
                .snapshot
                .tasks
                .first()
                .is_some_and(|task| task.status == TaskStatus::InFlight);
        let draining_failure = plan.snapshot.failure.is_some()
            && plan
                .snapshot
                .tasks
                .first()
                .is_some_and(|task| task.status == TaskStatus::InFlight);
        let result = self.inner.commit(id, fence, plan);
        if first_dispatch {
            if let (Some((thread, gate)), Ok(committed)) = (&self.first_dispatch_gate, &result) {
                if thread == &committed.thread_id {
                    gate.block();
                }
            }
        }
        if result.is_ok() {
            if draining_failure {
                if let Some(gate) = &self.failure_commit_gate {
                    gate.block();
                }
            }
            if let Some(status) = observed {
                self.earlier_status_at_second_dispatch
                    .lock()
                    .unwrap()
                    .get_or_insert(status);
            }
        }
        result
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
}
fn route() -> ScriptRoute {
    let ids = ["first", "second"];
    ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/parallel#workflow").unwrap(),
            revision: ExecutionRevision::new("parallel-v1").unwrap(),
            batches: vec![ids
                .iter()
                .map(|id| TaskSpec {
                    id: TaskId::new(*id).unwrap(),
                    effect_id: EffectId::new(*id).unwrap(),
                    requires_approval: false,
                    recovery: RecoveryPolicy::Pure,
                })
                .collect()],
            max_attempts: 2,
        },
        effects: ids
            .iter()
            .map(|id| {
                (
                    EffectId::new(*id).unwrap(),
                    ScriptEffect::Delayed {
                        duration_ms: 500,
                        result: TaskResult {
                            output: json!(id),
                            writes: vec![StateWrite::Append {
                                field: "items".into(),
                                items: vec![json!(id)],
                            }],
                        },
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
    }
}
async fn call<S: Store + 'static>(
    host: &HttpHost<S>,
    method: &str,
    path: &str,
    payload: Option<Value>,
) -> axum::response::Response {
    host.router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", "owner")
                .body(payload.map_or_else(Body::empty, |v| Body::from(v.to_string())))
                .unwrap(),
        )
        .await
        .unwrap()
}
#[tokio::test]
async fn ready_siblings_overlap_durable_attempts_and_reduce_in_definition_order() {
    let store = Arc::new(ObservedStore {
        inner: MemoryStore::new(),
        earlier_status_at_second_dispatch: Mutex::new(None),
        park_gate: None,
        first_dispatch_gate: None,
        failure_commit_gate: None,
    });
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![route()]),
    )
    .unwrap();
    let response = call(
        &host,
        "POST",
        "/threads/parallel/runs/wait",
        Some(json!({"route":"/parallel#workflow","input":"batch"})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&to_bytes(response.into_body(), 65536).await.unwrap())
            .unwrap(),
        json!("second")
    );
    let execution = store
        .inner
        .thread_view(&ThreadId::new("parallel").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(execution.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(
        execution.snapshot.values["items"],
        json!(["first", "second"])
    );
    assert_eq!(execution.snapshot.attempts_used, 2);
    for task in &execution.snapshot.tasks {
        assert_eq!(task.status, TaskStatus::Succeeded);
        assert_eq!(task.input["input"], "batch");
    }
    assert_eq!(
        call(&host, "GET", "/readyz", None).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        *store.earlier_status_at_second_dispatch.lock().unwrap(),
        Some(TaskStatus::InFlight),
        "a sibling result must not commit before the next Ready sibling is dispatched"
    );
}

impl ApprovalStore for ObservedStore {
    fn approval_durable(&self) -> bool {
        false
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        let park = matches!(&mutation, ApprovalMutation::Issue(_));
        let result = self.inner.commit_approval(id, fence, plan, mutation);
        if park && result.is_ok() {
            if let Some(gate) = &self.park_gate {
                gate.block();
            }
        }
        result
    }
}
#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, Duration::from_secs(8))
                .unwrap();
            state = next;
            assert!(
                !timeout.timed_out(),
                "parallel integration gate not released"
            );
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !self.state.lock().unwrap().0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
async fn wire(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 65536)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}
fn observed_store() -> ObservedStore {
    ObservedStore {
        inner: MemoryStore::new(),
        earlier_status_at_second_dispatch: Mutex::new(None),
        park_gate: None,
        first_dispatch_gate: None,
        failure_commit_gate: None,
    }
}
#[tokio::test]
async fn explicit_per_execution_and_global_limits_preserve_sequential_dispatch_when_selected() {
    for global in [false, true] {
        let store = Arc::new(observed_store());
        let mut config = HostConfig::new(vec![route()]);
        if global {
            config.max_concurrent_tasks = 1;
        } else {
            config.max_tasks_per_execution = 1;
        }
        let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Allow), config).unwrap();
        let response = call(
            &host,
            "POST",
            "/threads/limited/runs/wait",
            Some(json!({"route":"/parallel#workflow"})),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            *store.earlier_status_at_second_dispatch.lock().unwrap(),
            Some(TaskStatus::Succeeded)
        );
        assert_eq!(
            call(&host, "GET", "/readyz", None).await.status(),
            StatusCode::OK
        );
    }
}
#[test]
fn task_limits_reject_zero_and_unbounded_values_at_construction() {
    for global in [false, true] {
        for value in [0, usize::MAX] {
            let mut config = HostConfig::new(vec![route()]);
            if global {
                config.max_concurrent_tasks = value;
            } else {
                config.max_tasks_per_execution = value;
            }
            assert!(HttpHost::new(
                Arc::new(MemoryStore::new()),
                Arc::new(Auth),
                Arc::new(Allow),
                config
            )
            .is_err());
        }
    }
}
#[tokio::test]
async fn global_task_permit_is_shared_by_different_threads_before_attempt_admission() {
    let gate = Arc::new(Gate::default());
    let _cleanup = Release(gate.clone());
    let mut store = observed_store();
    store.first_dispatch_gate = Some((ThreadId::new("global-a").unwrap(), gate.clone()));
    let store = Arc::new(store);
    let mut config = HostConfig::new(vec![route()]);
    config.max_concurrent_tasks = 1;
    let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Allow), config).unwrap();
    let first = call(
        &host,
        "POST",
        "/threads/global-a/runs/stream",
        Some(json!({"route":"/parallel#workflow"})),
    )
    .await;
    gate.entered().await;
    let second = call(
        &host,
        "POST",
        "/threads/global-b/runs/stream",
        Some(json!({"route":"/parallel#workflow"})),
    )
    .await;
    let waiting = store
        .inner
        .thread_view(&ThreadId::new("global-b").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(waiting.snapshot.attempts_used, 0);
    gate.release();
    assert!(wire(first).await.contains("event: done"));
    assert!(wire(second).await.contains("event: done"));
    assert_eq!(
        call(&host, "GET", "/readyz", None).await.status(),
        StatusCode::OK
    );
}
#[tokio::test]
async fn required_task_parks_while_its_sibling_runs_and_resume_keeps_the_committed_result() {
    let gate = Arc::new(Gate::default());
    let _cleanup = Release(gate.clone());
    let mut store = observed_store();
    store.park_gate = Some(gate.clone());
    let store = Arc::new(store);
    let mut scripted = route();
    scripted.definition.batches[0][1].requires_approval = true;
    let mut approvals = ApprovalHostConfig::new(
        "policy-v1".into(),
        BTreeMap::from([(
            scripted.definition.route.clone(),
            BTreeMap::from([(
                TaskId::new("second").unwrap(),
                json!({"question":"Allow second?"}),
            )]),
        )]),
    );
    approvals.service_config = ApprovalServiceConfig::VolatileFixture;
    let host = HttpHost::new_with_approvals(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![scripted]),
        approvals,
    )
    .unwrap();
    let producer = call(
        &host,
        "POST",
        "/threads/park/runs/stream",
        Some(json!({"route":"/parallel#workflow","input":"batch"})),
    )
    .await;
    gate.entered().await;
    let partial = store
        .inner
        .thread_view(&ThreadId::new("park").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    let pending_before_stable = call(&host, "GET", "/threads/park/pending_interrupts", None).await;
    let pending_status = pending_before_stable.status();
    gate.release();
    let parked_wire = wire(producer).await;
    assert_eq!(partial.snapshot.status, ExecutionStatus::Running);
    assert_eq!(partial.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        partial.snapshot.tasks[1].status,
        TaskStatus::WaitingForApproval
    );
    assert_eq!(pending_status, StatusCode::CONFLICT);
    assert_eq!(parked_wire.matches("event: interrupt").count(), 1);
    let pending = call(&host, "GET", "/threads/park/pending_interrupts", None).await;
    let value: Value =
        serde_json::from_slice(&to_bytes(pending.into_body(), 65536).await.unwrap()).unwrap();
    let entry = &value["interrupts"][0];
    let resumed = call(&host,"POST","/threads/park/resume",Some(json!({"route":"/parallel#workflow","resume":[{"status":"resolved","interruptId":entry["interruptId"],"payload":"once","grant":entry["grant"]}]}))).await;
    let status = resumed.status();
    let resumed_wire = wire(resumed).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resumed_wire.matches("event: done").count(), 1);
    let final_state = store
        .inner
        .thread_view(&ThreadId::new("park").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(final_state.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(final_state.snapshot.attempts_used, 2);
    assert_eq!(
        final_state.snapshot.values["items"],
        json!(["first", "second"])
    );
    assert_eq!(
        call(&host, "GET", "/readyz", None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn cancellation_of_a_capacity_waiter_quiesces_without_borrowing_another_threads_permit() {
    let gate = Arc::new(Gate::default());
    let _cleanup = Release(gate.clone());
    let mut observed = observed_store();
    observed.first_dispatch_gate = Some((ThreadId::new("occupied").unwrap(), gate.clone()));
    let store = Arc::new(observed);
    let mut config = HostConfig::new(vec![route()]);
    config.max_concurrent_tasks = 1;
    let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Allow), config).unwrap();
    let occupied = call(
        &host,
        "POST",
        "/threads/occupied/runs/stream",
        Some(json!({"route":"/parallel#workflow"})),
    )
    .await;
    gate.entered().await;
    let waiting = call(
        &host,
        "POST",
        "/threads/waiter/runs/stream",
        Some(json!({"route":"/parallel#workflow"})),
    )
    .await;
    let cancel = call(&host, "POST", "/threads/waiter/cancel", None).await;
    assert_eq!(cancel.status(), StatusCode::OK);
    let cancelled_wire = wire(waiting).await;
    assert_eq!(cancelled_wire.matches("event: done").count(), 1);
    assert!(cancelled_wire.contains("\"cancelled\":true"));
    let cancelled = store
        .inner
        .thread_view(&ThreadId::new("waiter").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(cancelled.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(cancelled.snapshot.attempts_used, 0);
    let quiescent = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = call(&host, "DELETE", "/threads/waiter", None).await;
            if response.status() == StatusCode::NO_CONTENT {
                break;
            }
            assert_eq!(response.status(), StatusCode::CONFLICT);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    gate.release();
    assert!(wire(occupied).await.contains("event: done"));
    assert!(
        quiescent.is_ok(),
        "zero-attempt cancelled driver must exit while the other driver owns task capacity"
    );
    assert_eq!(
        call(&host, "GET", "/readyz", None).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn a_committed_sibling_failure_keeps_owned_exclusion_until_the_other_outcome_drains() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let mut observed = observed_store();
    observed.failure_commit_gate = Some(gate.clone());
    let store = Arc::new(observed);
    let mut script = route();
    script.effects.insert(
        EffectId::new("second").unwrap(),
        ScriptEffect::Failure("known sibling failure".into()),
    );
    let host = HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Allow),
        HostConfig::new(vec![script]),
    )
    .unwrap();
    let producer = call(
        &host,
        "POST",
        "/threads/failing/runs/stream",
        Some(json!({"route":"/parallel#workflow"})),
    )
    .await;
    assert_eq!(producer.status(), StatusCode::OK);
    gate.entered().await;
    let draining = store
        .inner
        .thread_view(&ThreadId::new("failing").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(draining.snapshot.status, ExecutionStatus::Running);
    assert_eq!(draining.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(draining.snapshot.tasks[1].status, TaskStatus::Failed);
    assert_eq!(
        call(&host, "DELETE", "/threads/failing", None)
            .await
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &host,
            "POST",
            "/threads/failing/runs/wait",
            Some(json!({"route":"/parallel#workflow"})),
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(&host, "GET", "/readyz", None).await.status(),
        StatusCode::OK
    );
    gate.release();
    let stream = wire(producer).await;
    assert_eq!(stream.matches("event: done").count(), 1);
    assert!(stream.contains("known sibling failure"));
    let failed = store
        .inner
        .thread_view(&ThreadId::new("failing").unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(failed.snapshot.status, ExecutionStatus::Failed);
    assert_eq!(failed.snapshot.attempts_used, 2);
    assert_eq!(failed.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(
        failed.snapshot.tasks[0].result.as_ref().unwrap().output,
        "first"
    );
    assert!(!failed.snapshot.values.contains_key("items"));
    assert_eq!(
        call(&host, "GET", "/readyz", None).await.status(),
        StatusCode::OK
    );
}

fn check_static_result_depth(effect: fn(TaskResult) -> ScriptEffect) {
    for (depth, accepted) in [(64, true), (65, false)] {
        let mut output = Value::Null;
        for _ in 0..depth {
            output = Value::Array(vec![output]);
        }
        let mut script = route();
        script.effects.insert(
            EffectId::new("first").unwrap(),
            effect(TaskResult {
                output,
                writes: vec![],
            }),
        );
        let host = HttpHost::new(
            Arc::new(MemoryStore::new()),
            Arc::new(Auth),
            Arc::new(Allow),
            HostConfig::new(vec![script]),
        );
        assert_eq!(
            host.is_ok(),
            accepted,
            "fixed result nesting boundary {depth}"
        );
    }
}
#[test]
fn result_fixture_rejects_depth_65_and_accepts_depth_64() {
    check_static_result_depth(ScriptEffect::Result);
}
#[test]
fn delayed_fixture_rejects_depth_65_and_accepts_depth_64() {
    check_static_result_depth(|result| ScriptEffect::Delayed {
        duration_ms: 1,
        result,
    });
}
#[test]
fn chunked_fixture_result_rejects_depth_65_and_accepts_depth_64() {
    check_static_result_depth(|result| ScriptEffect::Chunks {
        chunks: vec![json!("event")],
        result,
    });
}
