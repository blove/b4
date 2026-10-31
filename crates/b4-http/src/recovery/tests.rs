use super::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("b4-typed-core-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn actor() -> Principal {
    Principal {
        application: "private-core-app".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, _: &axum::http::HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(actor())
    }
}
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, principal: &Principal, _: &ResourceContext) -> bool {
        principal == &actor()
    }
}
struct Callback(Arc<AtomicUsize>);
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("finite-core-value"),
            writes: vec![],
        }))
    }
}
fn definition(policy: RecoveryPolicy, max_attempts: u32) -> Definition {
    Definition {
        route: RouteKey::new("/private-core#workflow").unwrap(),
        revision: ExecutionRevision::new("private-core-1").unwrap(),
        max_attempts,
        batches: vec![vec![TaskSpec {
            id: TaskId::new("task").unwrap(),
            effect_id: EffectId::new("effect").unwrap(),
            requires_approval: false,
            recovery: policy,
        }]],
    }
}
fn build<S: ApprovalStore + 'static>(
    store: Arc<S>,
    definition: Definition,
    calls: Arc<AtomicUsize>,
) -> HttpHost<S> {
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: actor().application,
                revision: "native-1".into(),
            },
            vec![RouteRegistration {
                definition,
                effects: BTreeMap::from([(
                    EffectId::new("effect").unwrap(),
                    EffectBinding {
                        callback: Arc::new(Callback(calls)),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    HttpHost::new_with_application(
        store,
        Arc::new(Auth),
        Arc::new(Policy),
        NativeHostConfig::new(
            application,
            ApprovalHostConfig::new("policy-1".into(), BTreeMap::new()),
        ),
    )
    .unwrap()
}
fn seed(store: &SqliteStore, definition: Definition, in_flight: bool) -> RecoveryTarget {
    let principal = actor();
    let id = ThreadId::new("private-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: id.clone(),
            created_at_ms: 1,
            metadata: Map::new(),
            origin: Some(ThreadOrigin {
                application: principal.application,
                tenant: principal.tenant,
                principal: principal.principal,
            }),
        })
        .unwrap();
    let snapshot = b4_kernel::new_execution(
        ExecutionId::new("private-execution").unwrap(),
        TurnId::new("original-turn").unwrap(),
        store.owner_generation(),
        definition,
        Map::new(),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    let mut current = store.admit(&id, snapshot).unwrap();
    if in_flight {
        let runtime = Runtime::new(store);
        current = runtime
            .apply(
                &current.snapshot.execution_id,
                ExecutionFence::from(&current.snapshot),
                KernelCommand::Start,
                TransitionFacts { now_ms: 3 },
            )
            .unwrap();
        runtime
            .apply(
                &current.snapshot.execution_id,
                ExecutionFence::from(&current.snapshot),
                KernelCommand::Dispatch {
                    task_id: TaskId::new("task").unwrap(),
                    attempt_id: AttemptId::new("lost-original-attempt").unwrap(),
                },
                TransitionFacts { now_ms: 4 },
            )
            .unwrap();
    }
    RecoveryTarget::from_view(&store.thread_view(&id).unwrap().unwrap()).unwrap()
}
async fn retire<S: Store + 'static>(host: HttpHost<S>, faulted: bool) {
    let handle = host.shutdown_handle();
    drop(host);
    assert_eq!(
        handle.wait_quiescent(Duration::from_secs(5)).await.unwrap(),
        ShutdownOutcome::Quiescent { faulted }
    );
}

#[tokio::test]
async fn typed_core_adopts_actual_unsafe_block_without_callback() {
    let directory = Directory::new();
    let definition = definition(RecoveryPolicy::Unsafe, 2);
    let target = {
        let store = SqliteStore::open(&directory.0).unwrap();
        seed(&store, definition.clone(), true)
    };
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let owner = store.owner_generation();
    let calls = Arc::new(AtomicUsize::new(0));
    let host = build(store.clone(), definition, calls.clone());
    let result = host.admit_recovery(actor(), target.clone()).await;
    let actual = store.execution(&target.execution_id).unwrap().unwrap();
    retire(host, false).await;
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(reopened.owner_generation(), owner + 1);
    let admitted = result.expect("actual unsafe recovery must return confirmed Blocked adoption");
    assert_eq!(admitted.disposition, RecoveryDisposition::Blocked);
    assert_eq!(
        admitted.adoption.execution_fence,
        ExecutionFence::from(&actual.snapshot)
    );
    assert_eq!(actual.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(actual.snapshot.owner_generation, owner);
    assert_eq!(calls.load(Ordering::Acquire), 0);
}
#[tokio::test]
async fn typed_core_adopts_actual_exhausted_ready_without_budget_reset() {
    let directory = Directory::new();
    let definition = definition(RecoveryPolicy::Pure, 1);
    let target = {
        let store = SqliteStore::open(&directory.0).unwrap();
        seed(&store, definition.clone(), true)
    };
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let host = build(store.clone(), definition, calls.clone());
    let result = host.admit_recovery(actor(), target.clone()).await;
    let actual = store.execution(&target.execution_id).unwrap().unwrap();
    retire(host, false).await;
    drop(store);
    let _reopened = SqliteStore::open(&directory.0).unwrap();
    let admitted =
        result.expect("actual exhausted recovery must return confirmed AttemptLimit adoption");
    assert_eq!(admitted.disposition, RecoveryDisposition::AttemptLimit);
    assert_eq!(
        admitted.adoption.execution_fence,
        ExecutionFence::from(&actual.snapshot)
    );
    assert_eq!(actual.snapshot.tasks[0].status, TaskStatus::Ready);
    assert_eq!(actual.snapshot.attempts_used, 1);
    assert_eq!(calls.load(Ordering::Acquire), 0);
}
#[tokio::test]
async fn typed_core_authority_refusal_is_unadopted_and_preserves_whole_view() {
    let directory = Directory::new();
    let definition = definition(RecoveryPolicy::Pure, 2);
    let target = {
        let store = SqliteStore::open(&directory.0).unwrap();
        seed(&store, definition.clone(), false)
    };
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let before = store.thread_view(&target.thread_id).unwrap().unwrap();
    let host = build(store.clone(), definition, Arc::new(AtomicUsize::new(0)));
    let mut foreign = actor();
    foreign.principal = "foreign".into();
    let result = host.admit_recovery(foreign, target.clone()).await;
    assert_eq!(
        store.thread_view(&target.thread_id).unwrap().unwrap(),
        before
    );
    retire(host, false).await;
    drop(store);
    let _reopened = SqliteStore::open(&directory.0).unwrap();
    let refused = result.unwrap_err();
    assert_eq!(refused.reason(), RecoveryRefusal::ResourceUnavailable);
    assert!(refused.adoption().is_none());
}

struct PostAdoptionStore {
    inner: SqliteStore,
    returned: std::sync::atomic::AtomicBool,
    wrong_generation: bool,
    projection: Option<ProjectionFault>,
}
#[derive(Clone, Copy)]
enum ProjectionFault {
    MissingActive,
    ForeignActive,
    ForeignThread,
}
impl Store for PostAdoptionStore {
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
        if self.returned.load(Ordering::Acquire) {
            if self.wrong_generation || self.projection.is_some() {
                let mut view = self.inner.thread_view(id)?;
                if let Some(view) = &mut view {
                    if self.wrong_generation {
                        view.thread.generation = ThreadGeneration::new("f".repeat(64)).unwrap();
                    }
                    match self.projection {
                        Some(ProjectionFault::MissingActive) => {
                            view.thread.active_execution_id = None
                        }
                        Some(ProjectionFault::ForeignActive) => {
                            view.thread.active_execution_id =
                                Some(ExecutionId::new("foreign-active").unwrap())
                        }
                        Some(ProjectionFault::ForeignThread) => {
                            view.thread.id = ThreadId::new("foreign-thread").unwrap()
                        }
                        None => {}
                    }
                }
                return Ok(view);
            }
            Err(StoreError::Corrupt(
                "private-post-adoption-diagnostic".into(),
            ))
        } else {
            self.inner.thread_view(id)
        }
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
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
    }
}
impl ApprovalStore for PostAdoptionStore {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
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
        let recovered = self.inner.commit_approval(id, fence, plan, mutation)?;
        self.returned.store(true, Ordering::Release);
        Ok(recovered)
    }
}
#[tokio::test]
async fn typed_core_confirmed_post_adoption_failure_keeps_actual_receipt_and_fault() {
    let directory = Directory::new();
    let definition = definition(RecoveryPolicy::Pure, 2);
    let target = {
        let store = SqliteStore::open(&directory.0).unwrap();
        seed(&store, definition.clone(), false)
    };
    let store = Arc::new(PostAdoptionStore {
        inner: SqliteStore::open(&directory.0).unwrap(),
        returned: std::sync::atomic::AtomicBool::new(false),
        wrong_generation: false,
        projection: None,
    });
    let host = build(store.clone(), definition, Arc::new(AtomicUsize::new(0)));
    let result = host.admit_recovery(actor(), target.clone()).await;
    let actual = store.inner.thread_view(&target.thread_id).unwrap().unwrap();
    let status = host.shutdown_handle().status();
    retire(host, status.faulted).await;
    drop(store);
    let _reopened = SqliteStore::open(&directory.0).unwrap();
    let refused = result.unwrap_err();
    assert_eq!(refused.reason(), RecoveryRefusal::Unavailable);
    let adopted = refused
        .adoption()
        .expect("bridge actually returned a committed recovered snapshot");
    assert_eq!(
        adopted.execution_fence,
        ExecutionFence::from(&actual.execution.unwrap().snapshot)
    );
    assert!(status.faulted);
    assert!(!refused
        .to_string()
        .contains("private-post-adoption-diagnostic"));
    assert!(!format!("{refused:?}").contains("private-execution"));
    assert!(std::error::Error::source(&refused).is_none());
}

#[tokio::test]
async fn typed_core_post_adoption_identity_mismatch_cannot_publish_runnable() {
    let directory = Directory::new();
    let definition = definition(RecoveryPolicy::Pure, 2);
    let target = {
        let store = SqliteStore::open(&directory.0).unwrap();
        seed(&store, definition.clone(), false)
    };
    let store = Arc::new(PostAdoptionStore {
        inner: SqliteStore::open(&directory.0).unwrap(),
        returned: std::sync::atomic::AtomicBool::new(false),
        wrong_generation: true,
        projection: None,
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let host = build(store.clone(), definition, calls.clone());
    let result = host.admit_recovery(actor(), target.clone()).await;
    let handle = host.shutdown_handle();
    drop(host);
    let final_status = handle.wait_quiescent(Duration::from_secs(5)).await.unwrap();
    let actual = store.inner.thread_view(&target.thread_id).unwrap().unwrap();
    drop(store);
    let _reopened = SqliteStore::open(&directory.0).unwrap();
    let refused =
        result.expect_err("a contradictory post-adoption incarnation cannot publish Runnable");
    assert_eq!(refused.reason(), RecoveryRefusal::Unavailable);
    assert_eq!(
        refused.adoption().unwrap().thread_generation,
        actual.thread.generation
    );
    assert_eq!(final_status, ShutdownOutcome::Quiescent { faulted: true });
    assert_eq!(calls.load(Ordering::Acquire), 0);
}

async fn post_projection_refuses(projection: ProjectionFault) {
    let directory = Directory::new();
    let definition = definition(RecoveryPolicy::Pure, 2);
    let target = {
        let store = SqliteStore::open(&directory.0).unwrap();
        seed(&store, definition.clone(), false)
    };
    let store = Arc::new(PostAdoptionStore {
        inner: SqliteStore::open(&directory.0).unwrap(),
        returned: std::sync::atomic::AtomicBool::new(false),
        wrong_generation: false,
        projection: Some(projection),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let host = build(store.clone(), definition, calls.clone());
    let result = host.admit_recovery(actor(), target.clone()).await;
    let handle = host.shutdown_handle();
    drop(host);
    let final_status = handle.wait_quiescent(Duration::from_secs(5)).await.unwrap();
    let actual = store.inner.thread_view(&target.thread_id).unwrap().unwrap();
    drop(store);
    let _reopened = SqliteStore::open(&directory.0).unwrap();
    let refused = result
        .expect_err("incoherent post-adoption thread or active identity must not publish Runnable");
    assert_eq!(refused.reason(), RecoveryRefusal::Unavailable);
    assert_eq!(refused.adoption().unwrap().thread_id, actual.thread.id);
    assert_eq!(
        refused.adoption().unwrap().execution_fence,
        ExecutionFence::from(&actual.execution.unwrap().snapshot)
    );
    assert_eq!(final_status, ShutdownOutcome::Quiescent { faulted: true });
    assert_eq!(calls.load(Ordering::Acquire), 0);
}
#[tokio::test]
async fn typed_core_post_adoption_cleared_active_pointer_cannot_publish() {
    post_projection_refuses(ProjectionFault::MissingActive).await;
}
#[tokio::test]
async fn typed_core_post_adoption_swapped_active_pointer_cannot_publish() {
    post_projection_refuses(ProjectionFault::ForeignActive).await;
}
#[tokio::test]
async fn typed_core_post_adoption_wrong_thread_record_cannot_publish() {
    post_projection_refuses(ProjectionFault::ForeignThread).await;
}
