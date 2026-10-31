use super::startup_test_support::*;
use super::*;
use crate::*;
use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde_json::Map;
use std::{
    sync::{atomic::AtomicUsize, Weak},
    time::Duration,
};

#[derive(Clone, Copy)]
enum Mode {
    Normal,
    ScannedOverflow,
    RecordOverflow,
    DescriptorFence,
    Refusal,
}
struct PageStore {
    inner: Arc<SqliteStore>,
    mode: Mode,
    reads: Arc<AtomicUsize>,
}
impl Store for PageStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
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
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(&self, id: &ThreadId, initial: Snapshot) -> Result<StoredExecution, StoreError> {
        self.inner.admit(id, initial)
    }
    fn admit_fenced(
        &self,
        id: &ThreadId,
        generation: &ThreadGeneration,
        revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(id, generation, revision, initial)
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
    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        if matches!(self.mode, Mode::Refusal) {
            return Err(DiscoveryError::UnsupportedOperation);
        }
        let mut page = self.inner.discover_unfinished(cursor)?;
        match self.mode {
            Mode::ScannedOverflow => page.scanned_rows = 33,
            Mode::RecordOverflow => page.scanned_rows = 0,
            Mode::DescriptorFence => page.records[0].resource_revision += 1,
            _ => (),
        }
        Ok(page)
    }
}
impl ApprovalStore for PageStore {
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
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
fn actor() -> Principal {
    Principal {
        application: "private-startup".into(),
        tenant: "tenant".into(),
        principal: "actor".into(),
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
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        true
    }
}
struct Resolver {
    calls: Arc<AtomicUsize>,
    hold: Option<Arc<Gate>>,
    refuse: bool,
}
impl StartupPrincipalResolver for Resolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if let Some(gate) = &self.hold {
            gate.hold();
        }
        if self.refuse {
            return Err(StartupResolutionError::Unauthorized);
        }
        let principal = actor();
        assert_eq!(
            origin,
            &ThreadOrigin {
                application: principal.application.clone(),
                tenant: principal.tenant.clone(),
                principal: principal.principal.clone()
            }
        );
        Ok(principal)
    }
}
struct SweepFixture {
    directory: Directory,
    owner: u64,
    source: Weak<SqliteStore>,
    store: Weak<PageStore>,
    application: Weak<NativeApplication>,
    carrier: Option<OwnedShared<PageStore>>,
    supervisor: ShutdownHandle,
    capability: StartupCapability,
    reads: Arc<AtomicUsize>,
    original: Option<(ThreadView, ApprovalView)>,
}
impl SweepFixture {
    fn new(mode: Mode, seed: bool) -> Self {
        let mut original = Fixture::new();
        let first = original.carrier.take().unwrap();
        let application = first.application.as_ref().unwrap().clone();
        let source = first.store.clone();
        let thread = ThreadId::new("private-coordinator-thread").unwrap();
        let execution = ExecutionId::new("private-coordinator-execution").unwrap();
        if seed {
            let principal = actor();
            source
                .create_thread_with(ThreadSeed {
                    id: thread.clone(),
                    created_at_ms: 1,
                    metadata: Map::new(),
                    origin: Some(ThreadOrigin {
                        application: principal.application,
                        tenant: principal.tenant,
                        principal: principal.principal,
                    }),
                })
                .unwrap();
            let initial = b4_kernel::new_execution(
                execution.clone(),
                TurnId::new("private-coordinator-turn").unwrap(),
                source.owner_generation(),
                application.definitions().next().unwrap().clone(),
                Map::new(),
                TransitionFacts { now_ms: 2 },
            )
            .unwrap();
            source.admit(&thread, initial).unwrap();
        }
        let prior = if seed {
            Some((
                source.thread_view(&thread).unwrap().unwrap(),
                source.approval_view(&execution).unwrap().unwrap(),
            ))
        } else {
            None
        };
        drop(first);
        drop(source);
        assert!(original.store.upgrade().is_none());
        let source = Arc::new(SqliteStore::open(&original.directory.0).unwrap());
        let owner = source.owner_generation();
        assert_eq!(owner, original.owner + 1);
        let source_weak = Arc::downgrade(&source);
        let reads = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(PageStore {
            inner: source.clone(),
            mode,
            reads: reads.clone(),
        });
        let store_weak = Arc::downgrade(&store);
        let application_weak = Arc::downgrade(&application);
        let mut host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            NativeHostConfig::new(
                application.clone(),
                ApprovalHostConfig::new("policy-1".into(), std::collections::BTreeMap::new()),
            ),
        )
        .unwrap();
        let shared = Arc::get_mut(&mut host.shared).unwrap();
        shared.supervisor = ShutdownHandle::new_preparing(shared.fault.clone());
        let supervisor = host.shutdown_handle();
        let (capability, token) = supervisor.begin_startup().unwrap();
        let carrier = OwnedShared::from_registered(host.shared.clone(), token);
        drop(host);
        drop(store);
        drop(source);
        drop(application);
        Self {
            directory: original.directory,
            owner,
            source: source_weak,
            store: store_weak,
            application: application_weak,
            carrier: Some(carrier),
            supervisor,
            capability,
            reads,
            original: prior,
        }
    }
    fn launch(
        &mut self,
        resolver: Arc<dyn StartupPrincipalResolver>,
    ) -> tokio::task::JoinHandle<()> {
        let carrier = self.carrier.take().unwrap();
        let future = crate::startup::coordinator::prepare(
            carrier.clone(),
            resolver,
            StartupPreparationLimits::default(),
            self.capability.clone(),
        );
        tokio::spawn(StartupFuture::new(future, carrier, self.capability.clone()))
    }
    async fn retire(&self, unchanged: bool) {
        assert_eq!(
            self.supervisor
                .wait_quiescent(Duration::from_secs(5))
                .await
                .unwrap(),
            ShutdownOutcome::Quiescent { faulted: false }
        );
        assert!(
            self.source.upgrade().is_none()
                && self.store.upgrade().is_none()
                && self.application.upgrade().is_none()
        );
        let reopened = SqliteStore::open(&self.directory.0).unwrap();
        assert_eq!(reopened.owner_generation(), self.owner + 1);
        reopened.readiness().unwrap();
        if unchanged {
            if let Some((view, approval)) = &self.original {
                assert_eq!(
                    reopened.thread_view(&view.thread.id).unwrap().as_ref(),
                    Some(view)
                );
                assert_eq!(
                    reopened
                        .approval_view(&approval.execution.snapshot.execution_id)
                        .unwrap()
                        .as_ref(),
                    Some(approval)
                );
            }
        }
    }
}
fn resolver(hold: Option<Arc<Gate>>, refuse: bool) -> (Arc<Resolver>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Resolver {
            calls: calls.clone(),
            hold,
            refuse,
        }),
        calls,
    )
}
async fn join(task: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn startup_coordinator_empty_owned_source_exhausts_one_real_page() {
    let mut f = SweepFixture::new(Mode::Normal, false);
    let (resolver, calls) = resolver(None, false);
    join(f.launch(resolver)).await;
    let status = f.supervisor.preparation_status();
    f.retire(true).await;
    assert_eq!(status.phase, StartupPreparationPhase::Complete);
    assert_eq!(
        status.progress,
        StartupProgress {
            pages: 1,
            ..StartupProgress::default()
        }
    );
    assert_eq!(f.reads.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_coordinator_held_trusted_resolver_keeps_real_source_before_adoption() {
    let mut f = SweepFixture::new(Mode::Normal, true);
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let (resolver, calls) = resolver(Some(gate.clone()), false);
    let mut task = f.launch(resolver);
    let entered = tokio::select! { _ = gate.entered() => true, result = &mut task => { result.unwrap(); false } };
    let held = if entered {
        let source = f.source.upgrade().unwrap();
        let original = f.original.as_ref().unwrap();
        let unchanged =
            source.thread_view(&original.0.thread.id).unwrap().as_ref() == Some(&original.0);
        let locked = matches!(
            SqliteStore::open(&f.directory.0),
            Err(StoreError::AlreadyOwned)
        );
        unchanged
            && locked
            && f.supervisor.status().active_owners == 1
            && !f.supervisor.is_serving()
    } else {
        false
    };
    gate.release();
    if entered {
        join(task).await;
    }
    let status = f.supervisor.preparation_status();
    f.retire(false).await;
    assert!(
        entered && held && gate.positive(),
        "trusted resolver was skipped or unowned before adoption"
    );
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(status.phase, StartupPreparationPhase::Complete);
    assert_eq!(
        status.progress,
        StartupProgress {
            pages: 1,
            scanned_rows: 1,
            candidates: 1,
            admitted: 1,
            runnable: 1,
            ..StartupProgress::default()
        }
    );
}

async fn malformed(mode: Mode, seed: bool) {
    let mut f = SweepFixture::new(mode, seed);
    let (resolver, calls) = resolver(None, false);
    join(f.launch(resolver)).await;
    let status = f.supervisor.preparation_status();
    f.retire(true).await;
    assert_eq!(f.reads.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(status.phase, StartupPreparationPhase::Incomplete);
    assert_eq!(
        status.incomplete_reason,
        Some(StartupIncompleteReason::DiscoveryRefused)
    );
    assert_eq!(
        status.progress,
        StartupProgress::default(),
        "malformed page changed counters before shape refusal"
    );
}
#[tokio::test]
async fn startup_coordinator_rejects_custom_scanned_overflow_before_counters() {
    malformed(Mode::ScannedOverflow, false).await;
}
#[tokio::test]
async fn startup_coordinator_rejects_custom_records_over_scanned_before_iteration() {
    malformed(Mode::RecordOverflow, true).await;
}

#[tokio::test]
async fn startup_coordinator_checks_original_full_view_fence_before_resolver() {
    let mut f = SweepFixture::new(Mode::DescriptorFence, true);
    let (resolver, calls) = resolver(None, false);
    join(f.launch(resolver)).await;
    let status = f.supervisor.preparation_status();
    f.retire(true).await;
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(status.phase, StartupPreparationPhase::Incomplete);
    assert_eq!(
        status.incomplete_reason,
        Some(StartupIncompleteReason::RecoveryRefused)
    );
    assert_eq!(
        status.progress,
        StartupProgress {
            pages: 1,
            scanned_rows: 1,
            candidates: 1,
            ..StartupProgress::default()
        }
    );
}
#[tokio::test]
async fn startup_coordinator_known_discovery_refusal_preserves_actual_source() {
    let mut f = SweepFixture::new(Mode::Refusal, true);
    let (resolver, calls) = resolver(None, false);
    join(f.launch(resolver)).await;
    let status = f.supervisor.preparation_status();
    f.retire(true).await;
    assert_eq!(status.phase, StartupPreparationPhase::Incomplete);
    assert_eq!(
        status.incomplete_reason,
        Some(StartupIncompleteReason::DiscoveryRefused)
    );
    assert_eq!(status.progress, StartupProgress::default());
    assert_eq!(f.reads.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 0);
}
