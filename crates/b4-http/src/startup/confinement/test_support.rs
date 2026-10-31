//! Private finite controls for the separate borrowed startup entry point.
//! Reused protected helpers contain zero test declarations.

#[allow(dead_code)] // Protected utility source also contains controls for known-ID cases.
#[path = "../../confinement/test_support.rs"]
mod inherited;
use crate::*;
use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
pub(super) use inherited::{
    assert_blocked_without_returned_receipt, blob, definition, principal, registration,
    seed_accepted, RawTables, State,
};
use std::{
    collections::BTreeMap,
    fs::{DirBuilder, OpenOptions},
    future::Future,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
    time::{Duration, Instant},
};

pub(super) const WAIT: Duration = Duration::from_secs(15);

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

// A distinct UUID prefix avoids colliding with the protected helper module's
// independent process-local directory counter during broad parallel tests.
pub(super) struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-startup-confinement-private-{}",
            uuid::Uuid::new_v4()
        ));
        let mut builder = DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(&path).unwrap();
        for name in ["owner.lock", "runtime.sqlite3"] {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            drop(options.open(path.join(name)).unwrap());
        }
        #[cfg(unix)]
        {
            assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o700);
            for name in ["owner.lock", "runtime.sqlite3"] {
                let metadata = std::fs::symlink_metadata(path.join(name)).unwrap();
                assert!(metadata.is_file());
                assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            }
        }
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
    fn rows(&self) -> RawTables {
        let connection = rusqlite::Connection::open_with_flags(
            self.0.join("runtime.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        [
            "metadata",
            "threads",
            "executions",
            "journal",
            "approval_grants",
            "approval_acceptances",
        ]
        .into_iter()
        .map(|table| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (table, rows)
        })
        .collect()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Capture {
    Pass,
    ChangeInitial,
    ChangeRenewal,
    Unsupported,
    ChangedTargetInitial,
    ChangedTargetRenewal,
}
#[derive(Clone, Copy, Debug)]
#[allow(clippy::enum_variant_names)] // Preserve explicit post-commit controls and assertions.
pub(super) enum Commit {
    Pass,
    ErrorAfterCommit,
    PanicAfterCommit,
}
#[derive(Clone, Copy, Debug)]
pub(super) enum Resolution {
    Match,
    ChangeAt(usize),
    UnavailableAt(usize),
}
#[derive(Clone, Copy, Debug)]
pub(super) enum Seed {
    Empty,
    Accepted,
    Blocked,
}
#[derive(Default)]
pub(super) struct Observations {
    pub captures: AtomicUsize,
    pub mutations: AtomicUsize,
    pub resolutions: AtomicUsize,
    pub policies: AtomicUsize,
    pub effects: Arc<AtomicUsize>,
    pub validators: Arc<AtomicUsize>,
    pub origins: Mutex<Vec<ThreadOrigin>>,
    pub permissions: Mutex<Vec<(ThreadView, RecoveryConfinementReason)>>,
}
impl Observations {
    pub fn no_dispatch(&self) {
        assert_eq!(self.effects.load(Ordering::SeqCst), 0);
        assert_eq!(self.validators.load(Ordering::SeqCst), 0);
    }
}

pub(super) struct ProbeStore {
    inner: Arc<SqliteStore>,
    pub observations: Arc<Observations>,
    capture: Capture,
    commit: Commit,
    discovery_gate: Option<Arc<Gate>>,
}
impl Store for ProbeStore {
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
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
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
        if let Some(gate) = &self.discovery_gate {
            gate.hold();
        }
        self.inner.discover_unfinished(cursor)
    }
    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        let call = self.observations.captures.fetch_add(1, Ordering::SeqCst) + 1;
        match self.capture {
            Capture::Unsupported => {
                return Err(RecoveryConfinementBackendError::UnsupportedOperation)
            }
            Capture::ChangedTargetInitial if call == 1 => {
                return Err(RecoveryConfinementBackendError::TargetChanged)
            }
            Capture::ChangedTargetRenewal if call == 2 => {
                return Err(RecoveryConfinementBackendError::TargetChanged)
            }
            _ => (),
        }
        let mut view = self.inner.confinement_view_fenced(target)?;
        if matches!(
            (self.capture, call),
            (Capture::ChangeInitial, 1) | (Capture::ChangeRenewal, 2)
        ) {
            // Keep every identity/fence/Definition fixed. The custom bounded
            // observation differs from the full ordinary bootstrap in metadata.
            view.thread
                .metadata
                .insert("different_whole_original".into(), serde_json::json!(true));
        }
        Ok(view)
    }
    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        self.observations.mutations.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .inner
            .confine_recovery_fenced(original, target, reason, turn, facts)?;
        match (self.commit, outcome) {
            (Commit::ErrorAfterCommit, RecoveryConfinementOutcome::Confined(_)) => {
                Err(RecoveryConfinementBackendError::Unavailable)
            }
            (Commit::PanicAfterCommit, RecoveryConfinementOutcome::Confined(_)) => {
                std::panic::panic_any(self.inner.clone())
            }
            (_, outcome) => Ok(outcome),
        }
    }
}
impl ApprovalStore for ProbeStore {
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

pub(super) struct Resolver {
    pub observations: Arc<Observations>,
    pub mode: Resolution,
}
impl StartupPrincipalResolver for Resolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        let call = self.observations.resolutions.fetch_add(1, Ordering::SeqCst) + 1;
        self.observations
            .origins
            .lock()
            .unwrap()
            .push(origin.clone());
        match self.mode {
            Resolution::ChangeAt(at) if call == at => Ok(Principal {
                principal: "different-current-actor".into(),
                ..principal()
            }),
            Resolution::UnavailableAt(at) if call == at => Err(StartupResolutionError::Unavailable),
            _ => Ok(principal()),
        }
    }
}
pub(super) struct Authority(pub Arc<Observations>);
impl RecoveryConfinementAuthority for Authority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        self.0
            .permissions
            .lock()
            .unwrap()
            .push((original.clone(), reason));
        Ok(())
    }
}
struct Policy {
    observations: Arc<Observations>,
    allow: bool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, actor: &Principal, _: &ResourceContext) -> bool {
        assert_eq!(actor, &principal());
        self.observations.policies.fetch_add(1, Ordering::SeqCst);
        self.allow
    }
}
struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, _: &axum::http::HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(principal())
    }
}

pub(super) struct Fixture {
    pub directory: Directory,
    pub host: Option<PreparingHttpHost<ProbeStore>>,
    pub store: Weak<ProbeStore>,
    pub sqlite: Weak<SqliteStore>,
    pub application: Weak<NativeApplication>,
    pub shutdown: ShutdownHandle,
    pub owner: u64,
    pub original: Option<State>,
    pub original_rows: RawTables,
    pub observations: Arc<Observations>,
}
impl Fixture {
    pub fn new(
        seed: Seed,
        exact: bool,
        capture: Capture,
        commit: Commit,
        discovery_gate: Option<Arc<Gate>>,
    ) -> Self {
        let observations = Arc::new(Observations::default());
        let directory = Directory::new();
        let prior = Arc::new(SqliteStore::open(directory.path()).unwrap());
        if !matches!(seed, Seed::Empty) {
            seed_accepted(&prior);
        }
        let prior_owner = prior.owner_generation();
        let prior_weak = Arc::downgrade(&prior);
        drop(prior);
        assert!(prior_weak.upgrade().is_none());
        let mut inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        assert_eq!(
            inner.owner_generation(),
            prior_owner.checked_add(1).unwrap()
        );
        if matches!(seed, Seed::Blocked) {
            let view = inner
                .thread_view(&ThreadId::new("prior-confinement-thread").unwrap())
                .unwrap()
                .unwrap();
            let target = RecoveryConfinementTarget::from_view(&view).unwrap();
            assert_eq!(inner.confinement_view_fenced(&target).unwrap(), view);
            let proof = inner
                .confine_recovery_fenced(
                    &view,
                    &target,
                    RecoveryConfinementReason::ExactDefinitionUnavailable,
                    &TurnId::new("prior-blocked-setup-turn").unwrap(),
                    TransitionFacts { now_ms: 3 },
                )
                .unwrap();
            assert!(matches!(proof, RecoveryConfinementOutcome::Confined(_)));
            let blocked_owner = inner.owner_generation();
            let old = Arc::downgrade(&inner);
            drop(inner);
            assert!(old.upgrade().is_none());
            inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
            assert_eq!(
                inner.owner_generation(),
                blocked_owner.checked_add(1).unwrap()
            );
        }
        let owner = inner.owner_generation();
        let original_rows = directory.rows();
        let original = if matches!(seed, Seed::Empty) {
            None
        } else {
            let view = inner
                .thread_view(&ThreadId::new("prior-confinement-thread").unwrap())
                .unwrap()
                .unwrap();
            let selected = &view.execution.as_ref().unwrap().snapshot;
            b4_kernel::validate_snapshot(selected).unwrap();
            assert!(selected.owner_generation < owner);
            assert_eq!(selected.definition, definition("v1"));
            let target = RecoveryConfinementTarget::from_view(&view).unwrap();
            assert_eq!(inner.confinement_view_fenced(&target).unwrap(), view);
            Some(State {
                approval: inner
                    .approval_view(&selected.execution_id)
                    .unwrap()
                    .unwrap(),
                journal: inner.journal(&selected.execution_id).unwrap(),
                raw: original_rows.clone(),
                view,
            })
        };
        let mut route = registration(
            observations.effects.clone(),
            observations.validators.clone(),
        );
        if exact {
            route.definition = definition("v1");
        }
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: principal().application,
                    revision: "startup-private-native-2".into(),
                },
                vec![route],
            )
            .unwrap(),
        );
        assert_eq!(
            application.definition_exact(&definition("v1")).is_some(),
            exact
        );
        let store = Arc::new(ProbeStore {
            inner: inner.clone(),
            observations: observations.clone(),
            capture,
            commit,
            discovery_gate,
        });
        let config = NativeHostConfig::new(
            application.clone(),
            ApprovalHostConfig::new("private-startup-current-policy".into(), BTreeMap::new()),
        );
        let host = HttpHost::new_with_application_preparing(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy {
                observations: observations.clone(),
                allow: !exact,
            }),
            config,
        )
        .unwrap();
        let shutdown = host.shutdown_handle();
        let fixture = Self {
            directory,
            host: Some(host),
            store: Arc::downgrade(&store),
            sqlite: Arc::downgrade(&inner),
            application: Arc::downgrade(&application),
            shutdown,
            owner,
            original,
            original_rows,
            observations,
        };
        drop(store);
        drop(inner);
        drop(application);
        fixture
    }
    pub fn host(&self) -> &PreparingHttpHost<ProbeStore> {
        self.host.as_ref().unwrap()
    }
    pub fn handles(
        &self,
        mode: Resolution,
    ) -> (
        Arc<dyn StartupPrincipalResolver>,
        Arc<dyn RecoveryConfinementAuthority>,
    ) {
        (
            Arc::new(Resolver {
                observations: self.observations.clone(),
                mode,
            }),
            Arc::new(Authority(self.observations.clone())),
        )
    }
    pub fn payload(&self, gate: Arc<Gate>, replacement: Option<Arc<Gate>>) -> Payload {
        Payload {
            store: self.sqlite.upgrade().unwrap(),
            application: self.application.upgrade().unwrap(),
            gate,
            replacement,
        }
    }
    pub fn install_final_panic(&mut self, payload: Payload) {
        Arc::get_mut(&mut self.host.as_mut().unwrap().host.shared)
            .unwrap()
            .auth = Arc::new(PanicAuth(Some(payload)));
    }
    pub fn held(&self) -> bool {
        self.shutdown.status().active_owners == 1
            && !self.shutdown.is_serving()
            && self.sqlite.upgrade().is_some()
            && self.application.upgrade().is_some()
            && matches!(
                SqliteStore::open(self.directory.path()),
                Err(StoreError::AlreadyOwned)
            )
    }
    pub async fn start(
        &self,
        resolver: &Arc<dyn StartupPrincipalResolver>,
        authority: &Arc<dyn RecoveryConfinementAuthority>,
    ) -> StartupConfinementPreparationHandle {
        bounded(self.host().begin_preparation_with_confinement(
            resolver,
            authority,
            StartupPreparationLimits::default(),
        ))
        .await
        .unwrap()
    }
    pub fn close_host(&mut self) {
        drop(self.host.take());
    }
    pub async fn retire(&mut self) -> Retired {
        self.shutdown.begin_shutdown();
        self.close_host();
        let quiescent = bounded(self.shutdown.wait_quiescent(WAIT)).await.unwrap();
        let ShutdownOutcome::Quiescent { faulted } = quiescent else {
            panic!("normal startup retirement did not finish: {quiescent:?}")
        };
        assert_eq!(self.shutdown.status().active_owners, 0);
        self.closed_writer(faulted)
    }
    pub fn retire_without_executor(&mut self) -> Retired {
        self.shutdown.begin_shutdown();
        self.close_host();
        assert_eq!(self.shutdown.status().active_owners, 0);
        self.closed_writer(self.shutdown.status().faulted)
    }
    fn closed_writer(&self, faulted: bool) -> Retired {
        assert!(self.store.upgrade().is_none());
        assert!(self.sqlite.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let writer = Arc::new(SqliteStore::open(self.directory.path()).unwrap());
        assert_eq!(
            writer.owner_generation(),
            self.owner.checked_add(1).unwrap()
        );
        writer.readiness().unwrap();
        let rows = self.directory.rows();
        let state = self.original.as_ref().map(|original| {
            let execution_id = &original
                .view
                .execution
                .as_ref()
                .unwrap()
                .snapshot
                .execution_id;
            State {
                view: writer
                    .thread_view(&original.view.thread.id)
                    .unwrap()
                    .unwrap(),
                approval: writer.approval_view(execution_id).unwrap().unwrap(),
                journal: writer.journal(execution_id).unwrap(),
                raw: rows.clone(),
            }
        });
        let weak = Arc::downgrade(&writer);
        drop(writer);
        assert!(weak.upgrade().is_none());
        eprintln!("B4_STARTUP_PRIVATE_NORMAL_RETIREMENT weak_store_none weak_sqlite_none weak_application_none next_writer_closed faulted={faulted}");
        Retired {
            faulted,
            rows,
            state,
        }
    }
    pub fn expected_rows_at_reopen(&self) -> RawTables {
        let mut rows = self.original_rows.clone();
        rows.get_mut("metadata").unwrap()[0][3] = blob(self.owner.checked_add(1).unwrap());
        rows
    }
    pub fn unchanged(&self, actual: &Retired) {
        assert_eq!(actual.rows, self.expected_rows_at_reopen());
        let expected = self.original.as_ref().map(|original| {
            let mut expected = original.clone();
            expected.raw = self.expected_rows_at_reopen();
            expected
        });
        assert_eq!(actual.state, expected);
    }
}
pub(super) struct Retired {
    pub faulted: bool,
    pub rows: RawTables,
    pub state: Option<State>,
}
pub(super) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future).await.expect(
        "finite control work did not return; this is setup/cleanup failure, not feature RED",
    )
}
pub(super) async fn finished(
    handle: &StartupConfinementPreparationHandle,
) -> StartupConfinementPreparationOutcome {
    match bounded(handle.wait(WAIT)).await.unwrap() {
        StartupConfinementWaitOutcome::Finished(outcome) => outcome,
        StartupConfinementWaitOutcome::Pending(status) => {
            panic!("startup observation remained pending: {status:?}")
        }
    }
}

#[derive(Clone, Copy, Default)]
struct GateFacts {
    entered: bool,
    explicit_release: bool,
    returned: bool,
    expired: bool,
    cleanup_release: bool,
}
pub(super) struct Gate {
    facts: Mutex<GateFacts>,
    changed: Condvar,
    notify: tokio::sync::Notify,
    deadline: Instant,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            facts: Mutex::new(GateFacts::default()),
            changed: Condvar::new(),
            notify: tokio::sync::Notify::new(),
            deadline: Instant::now() + WAIT,
        })
    }
    pub fn hold(&self) {
        let mut facts = self.facts.lock().unwrap();
        assert!(!facts.entered, "one finite gate entry");
        facts.entered = true;
        self.notify.notify_waiters();
        while !facts.explicit_release && !facts.cleanup_release {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                facts.expired = true;
                break;
            }
            facts = self.changed.wait_timeout(facts, remaining).unwrap().0;
        }
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.returned = true;
        self.notify.notify_waiters();
    }
    pub async fn entered(&self) {
        bounded(async {
            loop {
                let notify = self.notify.notified();
                tokio::pin!(notify);
                notify.as_mut().enable();
                if self.facts.lock().unwrap().entered {
                    break;
                }
                notify.await;
            }
        })
        .await;
    }
    pub fn release(&self) {
        let mut facts = self.facts.lock().unwrap();
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.explicit_release = true;
        self.changed.notify_all();
    }
    pub fn positive(&self) {
        let facts = *self.facts.lock().unwrap();
        assert!(
            facts.entered
                && facts.explicit_release
                && facts.returned
                && !facts.expired
                && !facts.cleanup_release
        );
    }
}
pub(super) struct Cleanup(pub Arc<Gate>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let mut facts = self.0.facts.lock().unwrap();
        if !facts.explicit_release {
            facts.cleanup_release = true;
            self.0.changed.notify_all();
        }
    }
}
pub(super) struct Payload {
    store: Arc<SqliteStore>,
    application: Arc<NativeApplication>,
    gate: Arc<Gate>,
    replacement: Option<Arc<Gate>>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.gate.hold();
        if let Some(next) = self.replacement.take() {
            std::panic::panic_any(Payload {
                store: self.store.clone(),
                application: self.application.clone(),
                gate: next,
                replacement: None,
            });
        }
    }
}
pub(super) struct DropResolver {
    pub observations: Arc<Observations>,
    pub payload: Option<Payload>,
}
impl StartupPrincipalResolver for DropResolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        Resolver {
            observations: self.observations.clone(),
            mode: Resolution::Match,
        }
        .resolve(origin)
    }
}
impl Drop for DropResolver {
    fn drop(&mut self) {
        std::panic::panic_any(self.payload.take().unwrap());
    }
}
pub(super) struct DropAuthority {
    pub observations: Arc<Observations>,
    pub payload: Option<Payload>,
}
impl RecoveryConfinementAuthority for DropAuthority {
    fn authorize(
        &self,
        view: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        Authority(self.observations.clone()).authorize(view, reason)
    }
}
impl Drop for DropAuthority {
    fn drop(&mut self) {
        std::panic::panic_any(self.payload.take().unwrap());
    }
}
struct PanicAuth(Option<Payload>);
impl Authenticator for PanicAuth {
    fn authenticate(&self, _: &axum::http::HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(principal())
    }
}
impl Drop for PanicAuth {
    fn drop(&mut self) {
        std::panic::panic_any(self.0.take().unwrap());
    }
}
