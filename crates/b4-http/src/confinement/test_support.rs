use crate::*;
use axum::http::HeaderMap;
use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::types::Value as SqlValue;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    fs::{DirBuilder, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub const WAIT: Duration = Duration::from_secs(15);
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
pub type RawTables = BTreeMap<&'static str, Vec<Vec<SqlValue>>>;

pub struct Directory(PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-private-confinement-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
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

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn rows(&self) -> RawTables {
        let connection = rusqlite::Connection::open(self.0.join("runtime.sqlite3")).unwrap();
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
                        .collect::<rusqlite::Result<Vec<SqlValue>>>()
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
        // This best-effort directory cleanup is not a physical-retirement witness.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn principal() -> Principal {
    Principal {
        application: "prior-confinement-fixture".into(),
        tenant: "prior-confinement-tenant".into(),
        principal: "prior-confinement-actor".into(),
    }
}

pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(principal())
    }
}

pub type AuthorityCalls = Arc<Mutex<Vec<(ThreadView, RecoveryConfinementReason)>>>;

pub struct FiniteEffect {
    pub calls: Arc<AtomicUsize>,
}
impl NativeEffect for FiniteEffect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"finite_unexpected_dispatch": true}),
            writes: vec![],
        }))
    }
}

pub struct Validator {
    pub calls: Arc<AtomicUsize>,
}
impl InputValidator for Validator {
    fn validate(&self, _: &Value) -> Result<(), InvalidApplicationInput> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub fn definition(revision: &str) -> Definition {
    Definition {
        route: RouteKey::new("/prior-confinement#workflow").unwrap(),
        revision: ExecutionRevision::new(revision).unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("original-task").unwrap(),
            effect_id: EffectId::new("original-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]],
        max_attempts: 3,
    }
}

pub fn registration(
    callback_calls: Arc<AtomicUsize>,
    validator_calls: Arc<AtomicUsize>,
) -> RouteRegistration {
    RouteRegistration {
        definition: definition("v2"),
        effects: BTreeMap::from([(
            EffectId::new("original-effect").unwrap(),
            EffectBinding {
                callback: Arc::new(FiniteEffect {
                    calls: callback_calls,
                }),
                output_kind: OutputKind::Value,
            },
        )]),
        input_validator: Some(Arc::new(Validator {
            calls: validator_calls,
        })),
    }
}

pub fn seed_accepted(store: &SqliteStore) -> (ThreadView, ApprovalView) {
    let actor = principal();
    let thread = ThreadId::new("prior-confinement-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::from_iter([("original_metadata".into(), json!({"preserved": true}))]),
            origin: Some(ThreadOrigin {
                application: actor.application,
                tenant: actor.tenant,
                principal: actor.principal,
            }),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("prior-confinement-execution").unwrap(),
        TurnId::new("prior-confinement-original-turn").unwrap(),
        store.owner_generation(),
        definition("v1"),
        Map::from_iter([("original_input".into(), json!({"preserved": true}))]),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    assert_eq!(initial.status, ExecutionStatus::Accepted);
    assert_eq!(initial.revision, 0);
    assert!(initial.tasks.is_empty());
    assert!(initial.attempt_history.is_empty());
    store.admit(&thread, initial).unwrap();
    let view = store.thread_view(&thread).unwrap().unwrap();
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Accepted);
    assert_eq!(snapshot.owner_generation, store.owner_generation());
    assert_eq!(snapshot.definition, definition("v1"));
    assert_eq!(snapshot.batch_index, 0);
    assert_eq!(snapshot.attempts_used, 0);
    assert!(snapshot.tasks.is_empty());
    let approval = store
        .approval_view(&snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(approval.grants.is_empty());
    assert!(approval.acceptances.is_empty());
    (view, approval)
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

pub fn blob(value: u64) -> SqlValue {
    SqlValue::Blob(value.to_be_bytes().to_vec())
}

pub fn expected_confined_tables(
    original: &RawTables,
    expected_view: &ThreadView,
    expected_journal: &JournalEntry,
) -> RawTables {
    let mut expected = original.clone();
    assert_eq!(expected["metadata"].len(), 1);
    assert_eq!(expected["threads"].len(), 1);
    assert_eq!(expected["executions"].len(), 1);
    assert_eq!(expected["journal"].len(), 2);
    assert!(expected["approval_grants"].is_empty());
    assert!(expected["approval_acceptances"].is_empty());
    // Protected schema column order: only resource_revision changes here.
    let threads = expected.get_mut("threads").unwrap();
    assert_eq!(threads[0].len(), 7);
    threads[0][5] = blob(expected_view.thread.resource_revision);
    let snapshot = &expected_view.execution.as_ref().unwrap().snapshot;
    let executions = expected.get_mut("executions").unwrap();
    assert_eq!(executions[0].len(), 5);
    executions[0][2] = blob(snapshot.revision);
    executions[0][3] = blob(snapshot.owner_generation);
    executions[0][4] = SqlValue::Text(serde_json::to_string(snapshot).unwrap());
    let journal = expected.get_mut("journal").unwrap();
    let SqlValue::Integer(last_sequence) = journal.last().unwrap()[0] else {
        panic!("fixture journal sequence must be an integer");
    };
    journal.push(vec![
        SqlValue::Integer(last_sequence.checked_add(1).unwrap()),
        SqlValue::Text("commit".into()),
        SqlValue::Text(expected_view.thread.id.as_str().into()),
        SqlValue::Text(snapshot.execution_id.as_str().into()),
        blob(expected_journal.revision),
        blob(expected_journal.owner_generation),
        SqlValue::Text(serde_json::to_string(&expected_journal.status).unwrap()),
        blob(expected_journal.updated_at_ms),
        SqlValue::Text(serde_json::to_string(expected_journal).unwrap()),
    ]);
    expected
}

pub fn assert_reopen_tables(original: &RawTables, reopened: &RawTables, owner: u64) {
    let mut expected = original.clone();
    let metadata = expected.get_mut("metadata").unwrap();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].len(), 4);
    // The actual reopen advances only metadata.owner_epoch among these tables.
    metadata[0][3] = blob(owner);
    assert_eq!(reopened, &expected);
}

// Private source-only draft. Root must accept backend and this source before
// adoption/compilation. Existing integration helpers are copied as source
// precedent, not imported as test declarations.
#[derive(Clone, Copy)]
pub enum ResolverMode {
    Match,
    Mismatch,
    Unavailable,
    Unauthorized,
}
#[derive(Clone, Copy)]
pub enum PolicyMode {
    Allow,
    Deny,
    AllowRenewal,
}
#[derive(Clone, Copy)]
pub enum AuthorityMode {
    Allow,
    Deny,
    Unavailable,
    DenyRenewal,
}
#[derive(Clone, Copy)]
pub enum CaptureMode {
    Pass,
    ChangeSecondView,
    Originless,
    ForeignOrigin,
    Refuse(RecoveryConfinementBackendError),
}
#[derive(Clone, Copy)]
pub enum CommitMode {
    Pass,
    CommitThenError,
    CommitThenPanic,
    ContradictoryReceipt,
}

pub struct ProbeStore {
    pub inner: Arc<SqliteStore>,
    pub capture_calls: AtomicUsize,
    pub mutation_calls: AtomicUsize,
    pub capture_mode: Mutex<CaptureMode>,
    pub commit_mode: Mutex<CommitMode>,
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
    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        let call = self.capture_calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            call <= 2,
            "a finite operation may capture only initial+renewal"
        );
        let mode = *self.capture_mode.lock().unwrap();
        if let CaptureMode::Refuse(error) = mode {
            return Err(error);
        }
        let mut view = self.inner.confinement_view_fenced(target)?;
        if matches!(mode, CaptureMode::ChangeSecondView) && call == 2 {
            // Custom Store coherent-view contradiction; underlying SQL stays
            // unchanged. Full equality must catch metadata even if fence equal.
            view.thread
                .metadata
                .insert("renewal_changed".into(), json!(true));
        }
        match mode {
            CaptureMode::Originless => view.thread.origin = None,
            CaptureMode::ForeignOrigin => {
                view.thread.origin.as_mut().unwrap().application = "foreign-application".into()
            }
            _ => (),
        }
        Ok(view)
    }
    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        assert_eq!(self.mutation_calls.fetch_add(1, Ordering::SeqCst), 0);
        let outcome = self
            .inner
            .confine_recovery_fenced(original, target, reason, fresh_turn, facts)?;
        match (*self.commit_mode.lock().unwrap(), outcome) {
            (CommitMode::CommitThenError, RecoveryConfinementOutcome::Confined(_)) => {
                Err(RecoveryConfinementBackendError::Unavailable)
            }
            (CommitMode::CommitThenPanic, RecoveryConfinementOutcome::Confined(_)) => {
                std::panic::panic_any(self.inner.clone())
            }
            (
                CommitMode::ContradictoryReceipt,
                RecoveryConfinementOutcome::Confined(mut receipt),
            ) => {
                receipt.new_resource_revision = receipt.original_resource_revision;
                Ok(RecoveryConfinementOutcome::Confined(receipt))
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

pub struct Controls {
    pub callbacks: Arc<AtomicUsize>,
    pub validators: Arc<AtomicUsize>,
    pub resolver_calls: AtomicUsize,
    pub policy_calls: AtomicUsize,
    pub authority_calls: AuthorityCalls,
    pub origins: Mutex<Vec<ThreadOrigin>>,
    pub contexts: Mutex<Vec<ResourceContext>>,
    pub resolver_mode: ResolverMode,
    pub policy_mode: PolicyMode,
    pub authority_mode: AuthorityMode,
}
impl Controls {
    pub fn new(
        resolver_mode: ResolverMode,
        policy_mode: PolicyMode,
        authority_mode: AuthorityMode,
    ) -> Arc<Self> {
        Arc::new(Self {
            callbacks: Arc::new(AtomicUsize::new(0)),
            validators: Arc::new(AtomicUsize::new(0)),
            resolver_calls: AtomicUsize::new(0),
            policy_calls: AtomicUsize::new(0),
            authority_calls: Arc::new(Mutex::new(Vec::new())),
            origins: Mutex::new(Vec::new()),
            contexts: Mutex::new(Vec::new()),
            resolver_mode,
            policy_mode,
            authority_mode,
        })
    }
    pub fn assert_no_dispatch(&self) {
        assert_eq!(self.callbacks.load(Ordering::SeqCst), 0);
        assert_eq!(self.validators.load(Ordering::SeqCst), 0);
    }
}
pub struct ControlledResolver(pub Arc<Controls>);
impl StartupPrincipalResolver for ControlledResolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        let call = self.0.resolver_calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(call <= 2);
        self.0.origins.lock().unwrap().push(origin.clone());
        match self.0.resolver_mode {
            ResolverMode::Match => Ok(principal()),
            ResolverMode::Mismatch => Ok(Principal {
                principal: "other-trusted-actor".into(),
                ..principal()
            }),
            ResolverMode::Unavailable => Err(StartupResolutionError::Unavailable),
            ResolverMode::Unauthorized => Err(StartupResolutionError::Unauthorized),
        }
    }
}
pub struct ControlledPolicy(pub Arc<Controls>);
impl ResourcePolicy for ControlledPolicy {
    fn authorize(&self, actor: &Principal, context: &ResourceContext) -> bool {
        assert_eq!(actor, &principal());
        let call = self.0.policy_calls.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(call <= 2);
        self.0.contexts.lock().unwrap().push(context.clone());
        match self.0.policy_mode {
            PolicyMode::Allow => true,
            PolicyMode::Deny => false,
            PolicyMode::AllowRenewal => call > 1,
        }
    }
}
pub struct ControlledAuthority(pub Arc<Controls>);
impl RecoveryConfinementAuthority for ControlledAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        let mut calls = self.0.authority_calls.lock().unwrap();
        assert!(calls.len() < 2);
        calls.push((original.clone(), reason));
        match self.0.authority_mode {
            AuthorityMode::Allow => Ok(()),
            AuthorityMode::Deny => Err(RecoveryConfinementAuthorityRefusal::Denied),
            AuthorityMode::Unavailable => Err(RecoveryConfinementAuthorityRefusal::Unavailable),
            AuthorityMode::DenyRenewal if calls.len() == 2 => {
                Err(RecoveryConfinementAuthorityRefusal::Denied)
            }
            AuthorityMode::DenyRenewal => Ok(()),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct State {
    pub view: ThreadView,
    pub approval: ApprovalView,
    pub journal: Vec<JournalEntry>,
    pub raw: RawTables,
}
pub struct Fixture {
    pub invocation: InvocationClock,
    pub directory: Directory,
    pub host: Option<HttpHost<ProbeStore>>,
    pub store: std::sync::Weak<ProbeStore>,
    pub sqlite: std::sync::Weak<SqliteStore>,
    pub application: std::sync::Weak<NativeApplication>,
    pub original: State,
    pub target: RecoveryConfinementTarget,
    pub controls: Arc<Controls>,
    pub shutdown: ShutdownHandle,
    pub owner: u64,
}
impl Fixture {
    pub fn new(exact: bool, controls: Arc<Controls>) -> Self {
        Self::new_with_auth(exact, controls, None)
    }
    pub fn new_with_auth(
        exact: bool,
        controls: Arc<Controls>,
        auth: Option<Arc<dyn Authenticator>>,
    ) -> Self {
        let directory = Directory::new();
        let prior = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let prior_owner = prior.owner_generation();
        let (view, approval) = seed_accepted(&prior);
        let journal = prior
            .journal(&view.execution.as_ref().unwrap().snapshot.execution_id)
            .unwrap();
        let prior_weak = Arc::downgrade(&prior);
        drop(prior);
        assert!(prior_weak.upgrade().is_none());
        let inner = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let owner = inner.owner_generation();
        assert_eq!(owner, prior_owner.checked_add(1).unwrap());
        assert_eq!(inner.thread_view(&view.thread.id).unwrap().unwrap(), view);
        assert_eq!(
            inner
                .approval_view(&view.execution.as_ref().unwrap().snapshot.execution_id)
                .unwrap()
                .unwrap(),
            approval
        );
        assert_eq!(
            inner
                .journal(&view.execution.as_ref().unwrap().snapshot.execution_id)
                .unwrap(),
            journal
        );
        let original = State {
            view,
            approval,
            journal,
            raw: directory.rows(),
        };
        let target = RecoveryConfinementTarget::from_view(&original.view).unwrap();
        let mut registered = registration(controls.callbacks.clone(), controls.validators.clone());
        if exact {
            registered.definition = definition("v1");
        }
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: principal().application,
                    revision: "assembly-v2".into(),
                },
                vec![registered],
            )
            .unwrap(),
        );
        assert_eq!(
            application.definition_exact(&definition("v1")).is_some(),
            exact
        );
        let store = Arc::new(ProbeStore {
            inner: inner.clone(),
            capture_calls: AtomicUsize::new(0),
            mutation_calls: AtomicUsize::new(0),
            capture_mode: Mutex::new(CaptureMode::Pass),
            commit_mode: Mutex::new(CommitMode::Pass),
        });
        let mut config = NativeHostConfig::new(
            application.clone(),
            ApprovalHostConfig::new("private-current-policy".into(), BTreeMap::new()),
        );
        config.limits.max_active_runs = 1;
        let host = HttpHost::new_with_application(
            store.clone(),
            auth.unwrap_or_else(|| Arc::new(Auth)),
            Arc::new(ControlledPolicy(controls.clone())),
            config,
        )
        .unwrap();
        let shutdown = host.shutdown_handle();
        let weak_store = Arc::downgrade(&store);
        let sqlite = Arc::downgrade(&inner);
        let weak_app = Arc::downgrade(&application);
        drop(inner);
        drop(store);
        drop(application);
        Self {
            invocation: InvocationClock::default(),
            directory,
            host: Some(host),
            store: weak_store,
            sqlite,
            application: weak_app,
            original,
            target,
            controls,
            shutdown,
            owner,
        }
    }
    pub fn host(&self) -> &HttpHost<ProbeStore> {
        self.host.as_ref().unwrap()
    }
    pub fn arguments(
        &self,
    ) -> (
        Arc<dyn StartupPrincipalResolver>,
        Arc<dyn RecoveryConfinementAuthority>,
    ) {
        (
            Arc::new(ControlledResolver(self.controls.clone())),
            Arc::new(ControlledAuthority(self.controls.clone())),
        )
    }
    pub fn state(&self) -> State {
        let store = self.store.upgrade().unwrap();
        let id = &self.target.execution_id;
        State {
            view: store.thread_view(&self.target.thread_id).unwrap().unwrap(),
            approval: store.approval_view(id).unwrap().unwrap(),
            journal: store.journal(id).unwrap(),
            raw: self.directory.rows(),
        }
    }
    pub fn counts(&self) -> (usize, usize) {
        let store = self.store.upgrade().unwrap();
        (
            store.capture_calls.load(Ordering::SeqCst),
            store.mutation_calls.load(Ordering::SeqCst),
        )
    }
    pub async fn retire(mut self, expected_fault: bool) -> State {
        let last = self.state();
        self.shutdown.begin_shutdown();
        drop(self.host.take());
        let observed = tokio::time::timeout(WAIT, self.shutdown.wait_quiescent(WAIT))
            .await
            .expect("normal finite physical observation")
            .unwrap();
        let actual_fault = match observed {
            ShutdownOutcome::Quiescent { faulted } => faulted,
            other => panic!("normal physical retirement was incomplete: {other:?}"),
        };
        assert!(self.store.upgrade().is_none());
        assert!(self.sqlite.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let writer = SqliteStore::open(self.directory.path()).unwrap();
        assert_eq!(
            writer.owner_generation(),
            self.owner.checked_add(1).unwrap()
        );
        writer.readiness().unwrap();
        assert_eq!(
            writer.thread_view(&self.target.thread_id).unwrap().unwrap(),
            last.view
        );
        assert_eq!(
            writer
                .approval_view(&self.target.execution_id)
                .unwrap()
                .unwrap(),
            last.approval
        );
        assert_eq!(
            writer.journal(&self.target.execution_id).unwrap(),
            last.journal
        );
        assert_reopen_tables(&last.raw, &self.directory.rows(), writer.owner_generation());
        drop(writer);
        assert_eq!(
            actual_fault, expected_fault,
            "fault classification assertion AFTER full normal Weak/writer proof"
        );
        last
    }
}
pub fn assert_refusal(
    result: Result<RecoveryConfinementOutcome, RecoveryConfinementError>,
    reason: RecoveryConfinementRefusal,
) -> RecoveryConfinementError {
    let error = result.expect_err("expected static confinement refusal");
    assert_eq!(error.reason(), reason);
    error
}
pub fn receipt(
    result: Result<RecoveryConfinementOutcome, RecoveryConfinementError>,
) -> RecoveryConfinementReceipt {
    match result.expect("finite prior-owner operation must return confined") {
        RecoveryConfinementOutcome::Confined(receipt) => receipt,
        RecoveryConfinementOutcome::AlreadyBlocked => {
            panic!("source was legal Accepted, not already blocked")
        }
    }
}

// Pure independently sampled wall-clock interval. This test control retains no
// Store/application/user handle and is never returned as confinement evidence.
#[derive(Clone, Default)]
pub struct InvocationClock {
    bounds: Arc<Mutex<(Option<u64>, Option<u64>)>>,
}
impl InvocationClock {
    pub fn begin(&self) {
        let mut bounds = self.bounds.lock().unwrap();
        assert!(bounds.0.is_none() && bounds.1.is_none());
        bounds.0 = Some(now_ms());
    }
    pub fn finish_if_open(&self) {
        let mut bounds = self.bounds.lock().unwrap();
        assert!(bounds.0.is_some());
        // A public task records its own return before outer completion is seen.
        // Preserve that first return sample if the observer also closes it.
        if bounds.1.is_none() {
            bounds.1 = Some(now_ms());
        }
    }
    pub fn assert_updated_at(&self, old: &Snapshot, actual: &Snapshot) {
        let (began, finished) = *self.bounds.lock().unwrap();
        let began = began.expect("wall-call start was independently sampled");
        let finished = finished.expect("actual invocation return was independently sampled");
        assert!(actual.updated_at_ms >= old.updated_at_ms);
        assert!((began..=finished).contains(&actual.updated_at_ms));
    }
}

pub fn assert_confined(
    original: &State,
    actual: &State,
    proof: &RecoveryConfinementReceipt,
    reason: RecoveryConfinementReason,
    owner: u64,
    invocation: &InvocationClock,
) {
    let old = &original.approval.execution.snapshot;
    // Check the observed clock leaf BEFORE deriving the complete expected
    // Snapshot, journal and six-table contents from the original.
    invocation.assert_updated_at(old, &actual.approval.execution.snapshot);
    assert_eq!(proof.thread_id, original.view.thread.id);
    assert_eq!(proof.thread_generation, original.view.thread.generation);
    assert_eq!(proof.execution_id, old.execution_id);
    assert_eq!(proof.original_fence, ExecutionFence::from(old));
    assert_eq!(
        proof.new_fence,
        ExecutionFence {
            revision: old.revision.checked_add(1).unwrap(),
            owner_generation: owner
        }
    );
    assert_eq!(
        proof.original_resource_revision,
        original.view.thread.resource_revision
    );
    assert_eq!(
        proof.new_resource_revision,
        original
            .view
            .thread
            .resource_revision
            .checked_add(1)
            .unwrap()
    );
    assert_eq!(proof.original_turn_id, old.turn_id);
    assert!(!old.turn_history.contains(&proof.fresh_turn_id));
    assert_eq!(proof.route, old.definition.route);
    assert_eq!(proof.definition_revision, old.definition.revision);
    assert_eq!(proof.reason, reason);
    let mut expected_snapshot = old.clone();
    expected_snapshot.status = ExecutionStatus::RecoveryBlocked;
    expected_snapshot.failure = Some(
        match reason {
            RecoveryConfinementReason::ExactDefinitionUnavailable => {
                "recovery_definition_unavailable"
            }
            RecoveryConfinementReason::RequiredAuthorityUnavailable => {
                "recovery_authority_unavailable"
            }
            RecoveryConfinementReason::DispatchNotAuthorized => "recovery_dispatch_not_authorized",
        }
        .into(),
    );
    expected_snapshot.owner_generation = owner;
    expected_snapshot.revision = old.revision.checked_add(1).unwrap();
    expected_snapshot.turn_id = proof.fresh_turn_id.clone();
    expected_snapshot
        .turn_history
        .push(proof.fresh_turn_id.clone());
    expected_snapshot.updated_at_ms = actual.approval.execution.snapshot.updated_at_ms;
    b4_kernel::validate_snapshot(&expected_snapshot).unwrap();
    let mut expected_view = original.view.clone();
    expected_view.thread.resource_revision = proof.new_resource_revision;
    expected_view.execution.as_mut().unwrap().snapshot = expected_snapshot.clone();
    let mut expected_approval = original.approval.clone();
    expected_approval.thread = expected_view.thread.clone();
    expected_approval.execution.snapshot = expected_snapshot.clone();
    let entry = JournalEntry::from(&expected_snapshot);
    let mut expected_journal = original.journal.clone();
    expected_journal.push(entry.clone());
    assert_eq!(actual.view, expected_view);
    assert_eq!(actual.approval, expected_approval);
    assert_eq!(actual.journal, expected_journal);
    assert_eq!(
        actual.raw,
        expected_confined_tables(&original.raw, &expected_view, &entry)
    );
}

#[derive(Clone, Copy, Default)]
pub struct GateFacts {
    pub entered: bool,
    pub explicit_release: bool,
    pub returned: bool,
    pub expired: bool,
    pub cleanup_release: bool,
}
pub struct Gate {
    pub facts: Mutex<GateFacts>,
    changed: std::sync::Condvar,
    notify: tokio::sync::Notify,
    deadline: std::time::Instant,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            facts: Mutex::new(GateFacts::default()),
            changed: std::sync::Condvar::new(),
            notify: tokio::sync::Notify::new(),
            deadline: std::time::Instant::now() + Duration::from_secs(30),
        })
    }
    pub fn hold(&self) {
        let mut facts = self.facts.lock().unwrap();
        assert!(!facts.entered, "single finite gate entry");
        facts.entered = true;
        self.notify.notify_waiters();
        while !facts.explicit_release && !facts.cleanup_release {
            let remaining = self
                .deadline
                .saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                facts.expired = true;
                break;
            }
            facts = self.changed.wait_timeout(facts, remaining).unwrap().0;
        }
        if std::time::Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.returned = true;
        self.notify.notify_waiters();
    }
    pub async fn entered(&self) {
        loop {
            let notification = self.notify.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if self.facts.lock().unwrap().entered {
                return;
            }
            notification.await;
        }
    }
    pub fn release(&self) {
        let mut facts = self.facts.lock().unwrap();
        if std::time::Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.explicit_release = true;
        self.changed.notify_all();
    }
    pub fn assert_positive(&self) {
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
pub struct Cleanup(pub Arc<Gate>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let mut facts = self.0.facts.lock().unwrap();
        if !facts.explicit_release {
            facts.cleanup_release = true;
            self.0.changed.notify_all();
        }
    }
}
pub type PureResult = Result<RecoveryConfinementOutcome, RecoveryConfinementError>;
#[allow(clippy::large_enum_variant)] // Keep the observable pure result unboxed.
pub enum Started {
    Entered,
    Complete(PureResult),
}
pub async fn reached_or_completed(
    gate: &Gate,
    task: &mut tokio::task::JoinHandle<PureResult>,
) -> Started {
    // A refused stub must complete and be asserted after normal cleanup. A
    // missing gate timeout is a fixture/execution failure, NEVER useful RED.
    tokio::time::timeout(WAIT, async {
        tokio::select! {
            () = gate.entered() => Started::Entered,
            result = &mut *task => Started::Complete(result.expect("supervised core returns pure result, not opaque JoinError")),
        }
    }).await.expect("operation neither completed nor reached finite controlled capability")
}
pub struct GateAuthority {
    pub controls: Arc<Controls>,
    pub gate: Arc<Gate>,
    pub at_call: usize,
}
impl RecoveryConfinementAuthority for GateAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        let call = {
            let mut calls = self.controls.authority_calls.lock().unwrap();
            assert!(calls.len() < 2);
            calls.push((original.clone(), reason));
            calls.len()
        };
        if call == self.at_call {
            self.gate.hold();
        }
        Ok(())
    }
}
pub struct HeldPayload {
    pub store: Arc<ProbeStore>,
    pub application: Arc<NativeApplication>,
    pub gate: Arc<Gate>,
    pub replacement: Option<Arc<Gate>>,
}
impl Drop for HeldPayload {
    fn drop(&mut self) {
        self.gate.hold();
        if let Some(next) = self.replacement.take() {
            std::panic::panic_any(HeldPayload {
                store: self.store.clone(),
                application: self.application.clone(),
                gate: next,
                replacement: None,
            });
        }
    }
}
pub struct PanicDropResolver {
    pub controls: Arc<Controls>,
    pub payload: Option<HeldPayload>,
}
impl StartupPrincipalResolver for PanicDropResolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        ControlledResolver(self.controls.clone()).resolve(origin)
    }
}
impl Drop for PanicDropResolver {
    fn drop(&mut self) {
        std::panic::panic_any(self.payload.take().unwrap());
    }
}
pub struct PanicDropAuthority {
    pub controls: Arc<Controls>,
    pub payload: Option<HeldPayload>,
}
impl RecoveryConfinementAuthority for PanicDropAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        ControlledAuthority(self.controls.clone()).authorize(original, reason)
    }
}
impl Drop for PanicDropAuthority {
    fn drop(&mut self) {
        std::panic::panic_any(self.payload.take().unwrap());
    }
}
pub struct PanicAuth(pub Option<HeldPayload>);
impl Authenticator for PanicAuth {
    fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(principal())
    }
}
impl Drop for PanicAuth {
    fn drop(&mut self) {
        std::panic::panic_any(self.0.take().unwrap());
    }
}
pub struct HeldChecks {
    pub capacity: Arc<tokio::sync::Semaphore>,
    pub claims: Arc<Mutex<crate::claim::ClaimRegistry>>,
    pub thread: ThreadId,
}
impl HeldChecks {
    pub fn from_fixture(f: &Fixture) -> Self {
        Self {
            capacity: f.host().shared.capacity.clone(),
            claims: f.host().shared.claims.clone(),
            thread: f.target.thread_id.clone(),
        }
    }
    pub fn assert_held(&self, f: &Fixture, fault: bool) {
        assert_eq!(
            f.shutdown.status().active_owners,
            1,
            "anchors/lease preserve SAME owner group"
        );
        assert_eq!(f.shutdown.status().faulted, fault);
        assert!(f.sqlite.upgrade().is_some());
        assert!(f.application.upgrade().is_some());
        assert!(matches!(
            SqliteStore::open(f.directory.path()),
            Err(StoreError::AlreadyOwned)
        ));
        assert!(self.capacity.clone().try_acquire_owned().is_err());
        assert!(matches!(
            crate::claim::ThreadClaim::acquire(self.claims.clone(), self.thread.clone()),
            Err(crate::claim::ClaimError::Busy)
        ));
    }
}
impl Fixture {
    pub fn payload(&self, gate: Arc<Gate>, replacement: Option<Arc<Gate>>) -> HeldPayload {
        HeldPayload {
            store: self.store.upgrade().unwrap(),
            application: self.application.upgrade().unwrap(),
            gate,
            replacement,
        }
    }
    pub fn install_panicking_final_auth(&mut self, payload: HeldPayload) {
        let shared = Arc::get_mut(&mut self.host.as_mut().unwrap().shared).unwrap();
        shared.auth = Arc::new(PanicAuth(Some(payload)));
    }
    pub fn take_core_arguments(
        &self,
    ) -> (
        crate::lifecycle::OwnedShared<ProbeStore>,
        tokio::sync::OwnedSemaphorePermit,
        crate::claim::ThreadClaim,
    ) {
        let owned = crate::lifecycle::OwnedShared::register(self.host().shared.clone())
            .expect("ordinary test owner");
        let permit = owned.capacity.clone().try_acquire_owned().unwrap();
        let claim =
            crate::claim::ThreadClaim::acquire(owned.claims.clone(), self.target.thread_id.clone())
                .ok()
                .unwrap();
        (owned, permit, claim)
    }
    pub async fn retire_detached(mut self, expected_fault: bool) -> State {
        // Core tests move every framework/caller resource into the Job before
        // waiting. No ordinary Store read can be taken via Weak after its join.
        self.shutdown.begin_shutdown();
        drop(self.host.take());
        let observed = tokio::time::timeout(WAIT, self.shutdown.wait_quiescent(WAIT))
            .await
            .expect("normal detached cleanup remained finite")
            .unwrap();
        let actual_fault = match observed {
            ShutdownOutcome::Quiescent { faulted } => faulted,
            other => panic!("normal physical retirement was incomplete: {other:?}"),
        };
        assert!(self.store.upgrade().is_none());
        assert!(self.sqlite.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let writer = SqliteStore::open(self.directory.path()).unwrap();
        assert_eq!(
            writer.owner_generation(),
            self.owner.checked_add(1).unwrap()
        );
        writer.readiness().unwrap();
        let actual = State {
            view: writer.thread_view(&self.target.thread_id).unwrap().unwrap(),
            approval: writer
                .approval_view(&self.target.execution_id)
                .unwrap()
                .unwrap(),
            journal: writer.journal(&self.target.execution_id).unwrap(),
            raw: self.directory.rows(),
        };
        assert_eq!(
            actual.raw["metadata"][0][3],
            blob(writer.owner_generation())
        );
        drop(writer);
        assert_eq!(
            actual_fault, expected_fault,
            "fault classification assertion AFTER full normal Weak/writer proof"
        );
        actual
    }
}
pub fn original_at_reopen(original: &State, owner: u64) -> State {
    let mut expected = original.clone();
    expected.raw.get_mut("metadata").unwrap()[0][3] = blob(owner);
    expected
}
pub async fn join_pure(task: tokio::task::JoinHandle<PureResult>) -> PureResult {
    tokio::time::timeout(WAIT, task)
        .await
        .expect("normal supervised join remained finite")
        .expect("no opaque payload escapes in JoinError")
}

// Comparison only when no backend receipt was returned (e.g. deliberate
// commit-then-error/panic or absent public waiter). This never manufactures
// confirmed receipt evidence. Original preserved fields derive only from the
// captured before state; the new random turn and wall-clock are checked leaves.
pub fn assert_blocked_without_returned_receipt(
    original: &State,
    actual: &State,
    reason: RecoveryConfinementReason,
    owner: u64,
) {
    let old = &original.approval.execution.snapshot;
    let recorded = &actual.approval.execution.snapshot;
    assert!(!old.turn_history.contains(&recorded.turn_id));
    assert!(recorded.updated_at_ms >= old.updated_at_ms && recorded.updated_at_ms <= now_ms());
    let mut expected_snapshot = old.clone();
    expected_snapshot.status = ExecutionStatus::RecoveryBlocked;
    expected_snapshot.failure = Some(
        match reason {
            RecoveryConfinementReason::ExactDefinitionUnavailable => {
                "recovery_definition_unavailable"
            }
            RecoveryConfinementReason::RequiredAuthorityUnavailable => {
                "recovery_authority_unavailable"
            }
            RecoveryConfinementReason::DispatchNotAuthorized => "recovery_dispatch_not_authorized",
        }
        .into(),
    );
    expected_snapshot.owner_generation = owner;
    expected_snapshot.revision = old.revision.checked_add(1).unwrap();
    expected_snapshot.turn_id = recorded.turn_id.clone();
    expected_snapshot
        .turn_history
        .push(recorded.turn_id.clone());
    expected_snapshot.updated_at_ms = recorded.updated_at_ms;
    b4_kernel::validate_snapshot(&expected_snapshot).unwrap();
    let mut expected_view = original.view.clone();
    expected_view.thread.resource_revision = original
        .view
        .thread
        .resource_revision
        .checked_add(1)
        .unwrap();
    expected_view.execution.as_mut().unwrap().snapshot = expected_snapshot.clone();
    let mut expected_approval = original.approval.clone();
    expected_approval.thread = expected_view.thread.clone();
    expected_approval.execution.snapshot = expected_snapshot.clone();
    let entry = JournalEntry::from(&expected_snapshot);
    let mut expected_journal = original.journal.clone();
    expected_journal.push(entry.clone());
    assert_eq!(actual.view, expected_view);
    assert_eq!(actual.approval, expected_approval);
    assert_eq!(actual.journal, expected_journal);
    assert_eq!(
        actual.raw,
        expected_confined_tables(&original.raw, &expected_view, &entry)
    );
}

pub struct PanicCallAuthority {
    pub controls: Arc<Controls>,
    pub store: Arc<ProbeStore>,
    pub application: Arc<NativeApplication>,
    pub gate: Arc<Gate>,
}
impl RecoveryConfinementAuthority for PanicCallAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        {
            let mut calls = self.controls.authority_calls.lock().unwrap();
            assert!(calls.is_empty());
            calls.push((original.clone(), reason));
        }
        // No payload exists on a refused unused stub. Only the actual reached
        // callback constructs this opaque Store+App-bearing cleanup witness.
        std::panic::panic_any(HeldPayload {
            store: self.store.clone(),
            application: self.application.clone(),
            gate: self.gate.clone(),
            replacement: None,
        });
    }
}
