use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
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
            "b4-prior-confinement-{}-{}",
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

pub struct Policy {
    pub calls: Arc<AtomicUsize>,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, actor: &Principal, _: &ResourceContext) -> bool {
        self.calls.fetch_add(1, Ordering::SeqCst);
        actor == &principal()
    }
}

pub struct Resolver {
    pub calls: Arc<AtomicUsize>,
}
impl StartupPrincipalResolver for Resolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let actor = principal();
        assert_eq!(origin.application, actor.application);
        assert_eq!(origin.tenant, actor.tenant);
        assert_eq!(origin.principal, actor.principal);
        Ok(actor)
    }
}

pub type AuthorityCalls = Arc<Mutex<Vec<(ThreadView, RecoveryConfinementReason)>>>;
pub struct Authority {
    pub calls: AuthorityCalls,
}
impl RecoveryConfinementAuthority for Authority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        self.calls.lock().unwrap().push((original.clone(), reason));
        Ok(())
    }
}

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

pub async fn quiesce(shutdown: &ShutdownHandle) {
    shutdown.begin_shutdown();
    let observed = tokio::time::timeout(WAIT, shutdown.wait_quiescent(WAIT))
        .await
        .expect("normal shutdown observation remained finite")
        .unwrap();
    assert_eq!(observed, ShutdownOutcome::Quiescent { faulted: false });
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

// Independent public embedding fixtures; appended after the exact original helper.
#[derive(Clone, Debug, PartialEq)]
pub struct PublicState {
    pub view: ThreadView,
    pub approval: ApprovalView,
    pub journal: Vec<JournalEntry>,
    pub raw: RawTables,
}
impl PublicState {
    pub fn read(store: &SqliteStore, directory: &Directory) -> Self {
        let view = store
            .thread_view(&ThreadId::new("prior-confinement-thread").unwrap())
            .unwrap()
            .unwrap();
        let id = &view.execution.as_ref().unwrap().snapshot.execution_id;
        Self {
            approval: store.approval_view(id).unwrap().unwrap(),
            journal: store.journal(id).unwrap(),
            raw: directory.rows(),
            view,
        }
    }
}

#[derive(Clone, Copy)]
pub enum PublicSource {
    PriorAccepted,
    PriorBlocked,
    CurrentAccepted,
    CurrentBlocked,
    PriorCancelled,
    StaleBeforeCancellation,
}

pub struct PublicCounters {
    pub callbacks: Arc<AtomicUsize>,
    pub validators: Arc<AtomicUsize>,
    pub policy: Arc<AtomicUsize>,
    pub resolver: Arc<AtomicUsize>,
    pub authority: AuthorityCalls,
}
impl PublicCounters {
    pub fn assert_no_dispatch_or_resolution(&self) {
        assert_eq!(self.callbacks.load(Ordering::SeqCst), 0);
        assert_eq!(self.validators.load(Ordering::SeqCst), 0);
        assert_eq!(self.policy.load(Ordering::SeqCst), 0);
        assert_eq!(self.resolver.load(Ordering::SeqCst), 0);
    }
}

pub struct PublicFixture {
    pub directory: Directory,
    pub store: Arc<SqliteStore>,
    pub application: Arc<NativeApplication>,
    pub host: HttpHost<SqliteStore>,
    pub original: PublicState,
    pub target: RecoveryConfinementTarget,
    pub counters: PublicCounters,
    pub owner: u64,
}
impl PublicFixture {
    pub fn new(source: PublicSource) -> Self {
        let directory = Directory::new();
        let mut store = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed_accepted(&store);
        let accepted = PublicState::read(&store, &directory);
        let stale = RecoveryConfinementTarget::from_view(&accepted.view).unwrap();
        match source {
            PublicSource::PriorBlocked | PublicSource::CurrentBlocked => {
                store = public_reopen(&directory, store);
                public_recover_unavailable(&store, &directory, "setup-blocked-turn", 3);
            }
            PublicSource::PriorCancelled | PublicSource::StaleBeforeCancellation => {
                let old = &accepted.approval.execution.snapshot;
                let mut expected = old.clone();
                expected.revision = old.revision.checked_add(1).unwrap();
                expected.updated_at_ms = 3;
                expected.status = ExecutionStatus::Cancelled;
                expected.tasks = old.definition.batches[0]
                    .iter()
                    .map(|spec| TaskRecord {
                        spec: spec.clone(),
                        status: TaskStatus::Cancelled,
                        input: old.values.clone(),
                        attempt_id: None,
                        result: None,
                        failure: None,
                        interrupt: None,
                        approved: false,
                    })
                    .collect();
                let plan = b4_kernel::transition(
                    old,
                    KernelCommand::Cancel {
                        turn_id: old.turn_id.clone(),
                        owner_generation: store.owner_generation(),
                    },
                    TransitionFacts { now_ms: 3 },
                )
                .unwrap();
                assert_eq!(plan.snapshot, expected);
                assert!(plan.dispatchable.is_empty());
                store.commit(&old.execution_id, old.into(), plan).unwrap();
                assert_eq!(
                    PublicState::read(&store, &directory),
                    public_expected_commit(&accepted, expected)
                );
            }
            PublicSource::PriorAccepted | PublicSource::CurrentAccepted => {}
        }
        if matches!(
            source,
            PublicSource::PriorAccepted
                | PublicSource::PriorBlocked
                | PublicSource::PriorCancelled
                | PublicSource::StaleBeforeCancellation
        ) {
            store = public_reopen(&directory, store);
        }
        let original = PublicState::read(&store, &directory);
        let snapshot = &original.approval.execution.snapshot;
        let owner = store.owner_generation();
        match source {
            PublicSource::CurrentAccepted | PublicSource::CurrentBlocked => {
                assert_eq!(snapshot.owner_generation, owner);
            }
            _ => assert!(snapshot.owner_generation < owner),
        }
        store.readiness().unwrap();
        let target = if matches!(source, PublicSource::StaleBeforeCancellation) {
            stale
        } else {
            RecoveryConfinementTarget::from_view(&original.view).unwrap()
        };
        let counters = PublicCounters {
            callbacks: Arc::new(AtomicUsize::new(0)),
            validators: Arc::new(AtomicUsize::new(0)),
            policy: Arc::new(AtomicUsize::new(0)),
            resolver: Arc::new(AtomicUsize::new(0)),
            authority: Arc::new(Mutex::new(Vec::new())),
        };
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: principal().application,
                    revision: "assembly-v2".into(),
                },
                vec![registration(
                    counters.callbacks.clone(),
                    counters.validators.clone(),
                )],
            )
            .unwrap(),
        );
        assert!(application.definition_exact(&snapshot.definition).is_none());
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy {
                calls: counters.policy.clone(),
            }),
            NativeHostConfig::new(
                application.clone(),
                ApprovalHostConfig::new("confinement-current-policy".into(), BTreeMap::new()),
            ),
        )
        .unwrap();
        assert!(matches!(
            SqliteStore::open(directory.path()),
            Err(StoreError::AlreadyOwned)
        ));
        Self {
            directory,
            store,
            application,
            host,
            original,
            target,
            counters,
            owner,
        }
    }

    pub fn resolver(&self) -> Arc<dyn StartupPrincipalResolver> {
        Arc::new(Resolver {
            calls: self.counters.resolver.clone(),
        })
    }

    pub fn authority(&self) -> Arc<dyn RecoveryConfinementAuthority> {
        Arc::new(Authority {
            calls: self.counters.authority.clone(),
        })
    }

    pub async fn retire(self) -> (PublicState, PublicCounters) {
        let after = PublicState::read(&self.store, &self.directory);
        let store_weak = Arc::downgrade(&self.store);
        let application_weak = Arc::downgrade(&self.application);
        let next_owner = self.owner.checked_add(1).unwrap();
        let Self {
            directory,
            store,
            application,
            host,
            counters,
            ..
        } = self;
        let shutdown = host.shutdown_handle();
        quiesce(&shutdown).await;
        drop(host);
        drop(application);
        drop(store);
        assert!(store_weak.upgrade().is_none());
        assert!(application_weak.upgrade().is_none());
        let successor = Arc::new(SqliteStore::open(directory.path()).unwrap());
        assert_eq!(successor.owner_generation(), next_owner);
        successor.readiness().unwrap();
        let persisted = PublicState::read(&successor, &directory);
        assert_eq!(persisted.view, after.view);
        assert_eq!(persisted.approval, after.approval);
        assert_eq!(persisted.journal, after.journal);
        assert_reopen_tables(&after.raw, &persisted.raw, next_owner);
        let successor_weak = Arc::downgrade(&successor);
        drop(successor);
        assert!(successor_weak.upgrade().is_none());
        (after, counters)
    }
}

pub fn public_reopen(directory: &Directory, store: Arc<SqliteStore>) -> Arc<SqliteStore> {
    let before = PublicState::read(&store, directory);
    let next_owner = store.owner_generation().checked_add(1).unwrap();
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_none());
    let successor = Arc::new(SqliteStore::open(directory.path()).unwrap());
    assert_eq!(successor.owner_generation(), next_owner);
    successor.readiness().unwrap();
    let actual = PublicState::read(&successor, directory);
    assert_eq!(actual.view, before.view);
    assert_eq!(actual.approval, before.approval);
    assert_eq!(actual.journal, before.journal);
    assert_reopen_tables(&before.raw, &actual.raw, next_owner);
    successor
}

pub fn public_expected_commit(original: &PublicState, snapshot: Snapshot) -> PublicState {
    let mut expected = original.clone();
    expected.view.thread.resource_revision = original
        .view
        .thread
        .resource_revision
        .checked_add(1)
        .unwrap();
    expected.view.execution.as_mut().unwrap().snapshot = snapshot.clone();
    expected.approval.thread = expected.view.thread.clone();
    expected.approval.execution.snapshot = snapshot.clone();
    let entry = JournalEntry::from(&snapshot);
    expected.journal.push(entry.clone());
    let threads = expected.raw.get_mut("threads").unwrap();
    assert_eq!(threads.len(), 1);
    threads[0][5] = blob(expected.view.thread.resource_revision);
    let executions = expected.raw.get_mut("executions").unwrap();
    assert_eq!(executions.len(), 1);
    executions[0][2] = blob(snapshot.revision);
    executions[0][3] = blob(snapshot.owner_generation);
    executions[0][4] = SqlValue::Text(serde_json::to_string(&snapshot).unwrap());
    let journal = expected.raw.get_mut("journal").unwrap();
    let SqlValue::Integer(last_sequence) = journal.last().unwrap()[0] else {
        panic!("selected fixture sequence must be integer");
    };
    journal.push(vec![
        SqlValue::Integer(last_sequence.checked_add(1).unwrap()),
        SqlValue::Text("commit".into()),
        SqlValue::Text(expected.view.thread.id.as_str().into()),
        SqlValue::Text(snapshot.execution_id.as_str().into()),
        blob(entry.revision),
        blob(entry.owner_generation),
        SqlValue::Text(serde_json::to_string(&entry.status).unwrap()),
        blob(entry.updated_at_ms),
        SqlValue::Text(serde_json::to_string(&entry).unwrap()),
    ]);
    expected
}

pub fn public_recover_unavailable(
    store: &SqliteStore,
    directory: &Directory,
    fresh_turn: &str,
    now: u64,
) -> PublicState {
    let original = PublicState::read(store, directory);
    let old = &original.approval.execution.snapshot;
    assert_eq!(old.status, ExecutionStatus::Accepted);
    assert!(old.tasks.is_empty());
    assert!(old.owner_generation < store.owner_generation());
    let turn = TurnId::new(fresh_turn).unwrap();
    assert!(!old.turn_history.contains(&turn));
    let mut expected = old.clone();
    expected.status = ExecutionStatus::RecoveryBlocked;
    expected.failure = Some("execution_revision_unavailable".into());
    expected.owner_generation = store.owner_generation();
    expected.turn_id = turn.clone();
    expected.turn_history.push(turn.clone());
    expected.revision = old.revision.checked_add(1).unwrap();
    expected.updated_at_ms = now;
    let plan = b4_kernel::transition(
        old,
        KernelCommand::Recover {
            available_revision: ExecutionRevision::new("missing-revision").unwrap(),
            turn_id: turn,
            owner_generation: store.owner_generation(),
        },
        TransitionFacts { now_ms: now },
    )
    .unwrap();
    assert_eq!(plan.snapshot, expected);
    assert!(plan.dispatchable.is_empty());
    store.commit(&old.execution_id, old.into(), plan).unwrap();
    let expected = public_expected_commit(&original, expected);
    assert_eq!(PublicState::read(store, directory), expected);
    expected
}

pub struct RecoverDuringAuthority {
    pub store: std::sync::Weak<SqliteStore>,
    pub calls: AuthorityCalls,
    pub facts: TransitionFacts,
}
impl RecoveryConfinementAuthority for RecoverDuringAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        let mut calls = self.calls.lock().unwrap();
        assert!(
            calls.is_empty(),
            "the stale source must not reach renewed authority"
        );
        calls.push((original.clone(), reason));
        drop(calls);
        let store = self.store.upgrade().expect("actual owning Store is live");
        assert_eq!(
            store.thread_view(&original.thread.id).unwrap().unwrap(),
            *original
        );
        let old = &original.execution.as_ref().unwrap().snapshot;
        assert_eq!(old.status, ExecutionStatus::Accepted);
        assert!(old.owner_generation < store.owner_generation());
        let turn = TurnId::new("external-recovery-during-authority").unwrap();
        let plan = b4_kernel::transition(
            old,
            KernelCommand::Recover {
                available_revision: ExecutionRevision::new("missing-revision").unwrap(),
                turn_id: turn,
                owner_generation: store.owner_generation(),
            },
            self.facts.clone(),
        )
        .unwrap();
        store.commit(&old.execution_id, old.into(), plan).unwrap();
        // This callback retains only Weak. Complete record/raw assertions
        // follow normal host retirement, not supplied as host evidence.
        Ok(())
    }
}

// Independent additional public qualification; existing helper remains exact above.
#[derive(Clone, Copy)]
pub enum ExtraResolution {
    Available,
    Unavailable,
    UnavailableThenAvailable,
}
pub struct ExtraCalls {
    pub origins: Mutex<Vec<ThreadOrigin>>,
    pub contexts: Mutex<Vec<(Principal, ResourceContext)>>,
    pub authority: AuthorityCalls,
    pub callbacks: Arc<AtomicUsize>,
    pub validators: Arc<AtomicUsize>,
    pub resolution: ExtraResolution,
    pub policy_allows: bool,
}
impl ExtraCalls {
    pub fn no_effects(&self) {
        assert_eq!(self.callbacks.load(Ordering::SeqCst), 0);
        assert_eq!(self.validators.load(Ordering::SeqCst), 0);
    }
    pub fn authority_calls(
        &self,
        original: &PublicState,
        reason: RecoveryConfinementReason,
        n: usize,
    ) {
        assert_eq!(
            *self.authority.lock().unwrap(),
            vec![(original.view.clone(), reason); n]
        );
    }
    pub fn recorded_contexts(&self, original: &PublicState, count: usize) {
        let contexts = self.contexts.lock().unwrap();
        assert_eq!(contexts.len(), count);
        for (actor, context) in contexts.iter() {
            assert_eq!(actor, &principal());
            assert!(matches!(context.operation, Operation::Recover));
            assert_eq!(context.thread_id, Some(original.view.thread.id.clone()));
            assert_eq!(context.origin, original.view.thread.origin);
            for selected in [&context.stored_route, &context.requested_route] {
                let selected = selected.as_ref().unwrap();
                assert_eq!(
                    selected.route,
                    original.approval.execution.snapshot.definition.route
                );
                assert_eq!(
                    selected.revision,
                    original.approval.execution.snapshot.definition.revision
                );
            }
        }
    }
}
pub struct ExtraResolver(pub Arc<ExtraCalls>);
impl StartupPrincipalResolver for ExtraResolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        let mut origins = self.0.origins.lock().unwrap();
        origins.push(origin.clone());
        let call = origins.len();
        let actor = principal();
        assert_eq!(origin.application, actor.application);
        assert_eq!(origin.tenant, actor.tenant);
        assert_eq!(origin.principal, actor.principal);
        match self.0.resolution {
            ExtraResolution::Unavailable => Err(StartupResolutionError::Unavailable),
            ExtraResolution::UnavailableThenAvailable if call == 1 => {
                Err(StartupResolutionError::Unavailable)
            }
            _ => Ok(actor),
        }
    }
}
pub struct ExtraPolicy(pub Arc<ExtraCalls>);
impl ResourcePolicy for ExtraPolicy {
    fn authorize(&self, actor: &Principal, context: &ResourceContext) -> bool {
        self.0
            .contexts
            .lock()
            .unwrap()
            .push((actor.clone(), context.clone()));
        assert_eq!(actor, &principal());
        self.0.policy_allows
    }
}
pub struct ExtraAuthority {
    pub calls: Arc<ExtraCalls>,
    pub refusal: Option<(usize, RecoveryConfinementAuthorityRefusal)>,
}
impl RecoveryConfinementAuthority for ExtraAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        let mut calls = self.calls.authority.lock().unwrap();
        calls.push((original.clone(), reason));
        match self.refusal {
            Some((at, refusal)) if calls.len() == at => Err(refusal),
            _ => Ok(()),
        }
    }
}

pub struct ExtraFixture {
    pub directory: Directory,
    pub store: std::sync::Weak<SqliteStore>,
    pub application: std::sync::Weak<NativeApplication>,
    pub host: Option<HttpHost<SqliteStore>>,
    pub shutdown: ShutdownHandle,
    pub original: PublicState,
    pub target: RecoveryConfinementTarget,
    pub owner: u64,
    pub calls: Arc<ExtraCalls>,
}
impl ExtraFixture {
    pub fn new(retained: bool, resolution: ExtraResolution, policy_allows: bool) -> Self {
        let directory = Directory::new();
        let seed = Arc::new(SqliteStore::open(directory.path()).unwrap());
        seed_accepted(&seed);
        let store = public_reopen(&directory, seed);
        store.readiness().unwrap();
        let original = PublicState::read(&store, &directory);
        let target = RecoveryConfinementTarget::from_view(&original.view).unwrap();
        let owner = store.owner_generation();
        assert!(target.execution_fence.owner_generation < owner);
        let calls = Arc::new(ExtraCalls {
            origins: Mutex::new(Vec::new()),
            contexts: Mutex::new(Vec::new()),
            authority: Arc::new(Mutex::new(Vec::new())),
            callbacks: Arc::new(AtomicUsize::new(0)),
            validators: Arc::new(AtomicUsize::new(0)),
            resolution,
            policy_allows,
        });
        let current = registration(calls.callbacks.clone(), calls.validators.clone());
        let mut old = registration(calls.callbacks.clone(), calls.validators.clone());
        old.definition = original.approval.execution.snapshot.definition.clone();
        let application = Arc::new(
            NativeApplication::new_with_history(
                ApplicationIdentity {
                    application: principal().application,
                    revision: "independent-assembly-v2".into(),
                },
                vec![current],
                if retained { vec![old] } else { vec![] },
            )
            .unwrap(),
        );
        assert_eq!(
            application
                .definition_exact(&old_definition(&original))
                .is_some(),
            retained
        );
        let configuration = NativeHostConfig::new(
            application.clone(),
            ApprovalHostConfig::new("independent-current-policy".into(), BTreeMap::new()),
        );
        assert_eq!(
            configuration.approval.service_config,
            ApprovalServiceConfig::RequiredDurable
        );
        let host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(ExtraPolicy(calls.clone())),
            configuration,
        )
        .unwrap();
        let shutdown = host.shutdown_handle();
        let weak_store = Arc::downgrade(&store);
        let weak_application = Arc::downgrade(&application);
        drop(store);
        drop(application);
        assert!(matches!(
            SqliteStore::open(directory.path()),
            Err(StoreError::AlreadyOwned)
        ));
        Self {
            directory,
            store: weak_store,
            application: weak_application,
            host: Some(host),
            shutdown,
            original,
            target,
            owner,
            calls,
        }
    }
    pub fn host(&self) -> &HttpHost<SqliteStore> {
        self.host.as_ref().unwrap()
    }
    pub fn resolver(&self) -> Arc<dyn StartupPrincipalResolver> {
        Arc::new(ExtraResolver(self.calls.clone()))
    }
    pub fn authority(
        &self,
        refusal: Option<(usize, RecoveryConfinementAuthorityRefusal)>,
    ) -> Arc<dyn RecoveryConfinementAuthority> {
        Arc::new(ExtraAuthority {
            calls: self.calls.clone(),
            refusal,
        })
    }
    pub fn state(&self) -> PublicState {
        PublicState::read(&self.store.upgrade().unwrap(), &self.directory)
    }
    pub fn assert_held(&self, faulted: bool) {
        assert_eq!(self.shutdown.status().active_owners, 1);
        assert_eq!(self.shutdown.status().faulted, faulted);
        assert!(self.store.upgrade().is_some());
        assert!(self.application.upgrade().is_some());
        assert!(matches!(
            SqliteStore::open(self.directory.path()),
            Err(StoreError::AlreadyOwned)
        ));
    }
    // This returns only state collected while the ordinary external Host alias
    // was alive; all aliases retire before the source is compared by the test.
    pub async fn retire(mut self, expected_fault: bool) -> PublicState {
        let after = self.state();
        self.shutdown.begin_shutdown();
        drop(self.host.take());
        let observed = tokio::time::timeout(WAIT, self.shutdown.wait_quiescent(WAIT))
            .await
            .expect("finite normal owner observation")
            .unwrap();
        let actual_fault = match observed {
            ShutdownOutcome::Quiescent { faulted } => faulted,
            other => panic!("not physical quiescence: {other:?}"),
        };
        assert!(self.store.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let writer = Arc::new(SqliteStore::open(self.directory.path()).unwrap());
        assert_eq!(
            writer.owner_generation(),
            self.owner.checked_add(1).unwrap()
        );
        writer.readiness().unwrap();
        let persisted = PublicState::read(&writer, &self.directory);
        assert_eq!(persisted.view, after.view);
        assert_eq!(persisted.approval, after.approval);
        assert_eq!(persisted.journal, after.journal);
        assert_reopen_tables(&after.raw, &persisted.raw, writer.owner_generation());
        let weak = Arc::downgrade(&writer);
        drop(writer);
        assert!(weak.upgrade().is_none());
        assert_eq!(actual_fault, expected_fault);
        after
    }
    // The abandoned caller owns no reported result. Observe final persistence
    // only after actual carrier quiescence and positive normal writer succession.
    pub async fn retire_abandoned(self, expected_fault: bool) -> PublicState {
        assert!(self.host.is_none());
        self.shutdown.begin_shutdown();
        let observed = tokio::time::timeout(WAIT, self.shutdown.wait_quiescent(WAIT))
            .await
            .expect("finite detached owner observation")
            .unwrap();
        let actual_fault = match observed {
            ShutdownOutcome::Quiescent { faulted } => faulted,
            other => panic!("not physical quiescence: {other:?}"),
        };
        assert!(self.store.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let writer = Arc::new(SqliteStore::open(self.directory.path()).unwrap());
        assert_eq!(
            writer.owner_generation(),
            self.owner.checked_add(1).unwrap()
        );
        writer.readiness().unwrap();
        let persisted = PublicState::read(&writer, &self.directory);
        let weak = Arc::downgrade(&writer);
        drop(writer);
        assert!(weak.upgrade().is_none());
        assert_eq!(actual_fault, expected_fault);
        persisted
    }
}
fn old_definition(original: &PublicState) -> Definition {
    original.approval.execution.snapshot.definition.clone()
}

pub fn extra_confined(
    original: &PublicState,
    after: &PublicState,
    receipt: &RecoveryConfinementReceipt,
    owner: u64,
    reason: RecoveryConfinementReason,
    started: u64,
    finished: u64,
) {
    let old = &original.approval.execution.snapshot;
    assert_eq!(receipt.thread_id, original.view.thread.id);
    assert_eq!(receipt.thread_generation, original.view.thread.generation);
    assert_eq!(receipt.execution_id, old.execution_id);
    assert_eq!(receipt.original_fence, ExecutionFence::from(old));
    assert_eq!(
        receipt.new_fence,
        ExecutionFence {
            revision: old.revision.checked_add(1).unwrap(),
            owner_generation: owner
        }
    );
    assert_eq!(
        receipt.original_resource_revision,
        original.view.thread.resource_revision
    );
    assert_eq!(
        receipt.new_resource_revision,
        original
            .view
            .thread
            .resource_revision
            .checked_add(1)
            .unwrap()
    );
    assert_eq!(receipt.original_turn_id, old.turn_id);
    assert_eq!(receipt.route, old.definition.route);
    assert_eq!(receipt.definition_revision, old.definition.revision);
    assert_eq!(receipt.reason, reason);
    assert!(!old.turn_history.contains(&receipt.fresh_turn_id));
    let expected = extra_expected_confined(
        original,
        owner,
        reason,
        &receipt.fresh_turn_id,
        after.approval.execution.snapshot.updated_at_ms,
        started,
        finished,
    );
    assert_eq!(after, &expected);
}
fn extra_expected_confined(
    original: &PublicState,
    owner: u64,
    reason: RecoveryConfinementReason,
    fresh: &TurnId,
    time: u64,
    started: u64,
    finished: u64,
) -> PublicState {
    let old = &original.approval.execution.snapshot;
    assert!(owner > old.owner_generation);
    assert!(!old.turn_history.contains(fresh));
    assert!(time >= old.updated_at_ms);
    assert!((started..=finished).contains(&time));
    let mut expected = old.clone();
    expected.status = ExecutionStatus::RecoveryBlocked;
    expected.failure = Some(
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
    expected.owner_generation = owner;
    expected.revision = old.revision.checked_add(1).unwrap();
    expected.turn_id = fresh.clone();
    expected.turn_history.push(fresh.clone());
    expected.updated_at_ms = time;
    b4_kernel::validate_snapshot(&expected).unwrap();
    public_expected_commit(original, expected)
}
pub fn extra_persisted_without_receipt(
    original: &PublicState,
    after: &PublicState,
    owner: u64,
    reason: RecoveryConfinementReason,
    started: u64,
    finished: u64,
    next_writer: bool,
) {
    let actual = &after.approval.execution.snapshot;
    let expected = extra_expected_confined(
        original,
        owner,
        reason,
        &actual.turn_id,
        actual.updated_at_ms,
        started,
        finished,
    );
    assert_eq!(after.view, expected.view);
    assert_eq!(after.approval, expected.approval);
    assert_eq!(after.journal, expected.journal);
    if next_writer {
        assert_reopen_tables(&expected.raw, &after.raw, owner.checked_add(1).unwrap());
    } else {
        assert_eq!(after.raw, expected.raw);
    }
}
pub fn extra_unchanged_after_next_writer(original: &PublicState, after: &PublicState, owner: u64) {
    assert_eq!(after.view, original.view);
    assert_eq!(after.approval, original.approval);
    assert_eq!(after.journal, original.journal);
    assert_reopen_tables(&original.raw, &after.raw, owner.checked_add(1).unwrap());
}
pub fn extra_receipt(
    result: Result<RecoveryConfinementOutcome, RecoveryConfinementError>,
) -> RecoveryConfinementReceipt {
    match result.expect("expected actual durable Confined return") {
        RecoveryConfinementOutcome::Confined(receipt) => receipt,
        other => panic!("wrong actual outcome: {other:?}"),
    }
}
pub fn extra_refused(
    result: Result<RecoveryConfinementOutcome, RecoveryConfinementError>,
    reason: RecoveryConfinementRefusal,
) {
    let error = result.expect_err("expected exact local refusal");
    assert_eq!(error.reason(), reason);
    assert!(error.confirmed_receipt().is_none());
}

#[derive(Clone, Copy, Default)]
pub struct ExtraGateFacts {
    pub entered: bool,
    pub returned: bool,
    pub explicit: bool,
    pub expired: bool,
    pub cleanup: bool,
}
pub struct ExtraGate {
    facts: Mutex<ExtraGateFacts>,
    changed: std::sync::Condvar,
    notify: tokio::sync::Notify,
    deadline: std::time::Instant,
}
impl ExtraGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            facts: Mutex::new(ExtraGateFacts::default()),
            changed: std::sync::Condvar::new(),
            notify: tokio::sync::Notify::new(),
            deadline: std::time::Instant::now() + Duration::from_secs(30),
        })
    }
    pub fn hold(&self) -> bool {
        let mut f = self.facts.lock().unwrap();
        assert!(!f.entered);
        f.entered = true;
        f.expired |= std::time::Instant::now() >= self.deadline;
        self.notify.notify_waiters();
        while !f.expired && !f.explicit && !f.cleanup {
            let remaining = self
                .deadline
                .saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                f.expired = true;
                break;
            }
            f = self.changed.wait_timeout(f, remaining).unwrap().0;
            f.expired |= std::time::Instant::now() >= self.deadline;
        }
        f.expired |= std::time::Instant::now() >= self.deadline;
        f.returned = true;
        self.notify.notify_waiters();
        f.explicit && !f.expired && !f.cleanup
    }
    pub fn release(&self) {
        let mut f = self.facts.lock().unwrap();
        f.expired |= std::time::Instant::now() >= self.deadline;
        f.explicit = true;
        self.changed.notify_all();
    }
    pub async fn entered(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.facts.lock().unwrap().entered {
                return;
            }
            notified.await;
        }
    }
    pub fn positive(&self) {
        let f = *self.facts.lock().unwrap();
        assert!(f.entered && f.returned && f.explicit && !f.expired && !f.cleanup);
    }
}
pub struct ExtraGateCleanup(pub Arc<ExtraGate>);
impl Drop for ExtraGateCleanup {
    fn drop(&mut self) {
        let mut f = self.0.facts.lock().unwrap();
        if !f.explicit {
            f.cleanup = true;
            self.0.changed.notify_all();
        }
    }
}
#[derive(Clone, Copy)]
pub enum ExtraHeldMode {
    Callback,
    Destructor,
    PanicPayload,
}
pub struct ExtraHeldAuthority {
    pub calls: Arc<ExtraCalls>,
    pub at_call: usize,
    pub call_gate: Arc<ExtraGate>,
    pub cleanup_gate: Option<Arc<ExtraGate>>,
    pub mode: ExtraHeldMode,
    pub store: std::sync::Weak<SqliteStore>,
    pub application: std::sync::Weak<NativeApplication>,
}
impl RecoveryConfinementAuthority for ExtraHeldAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        let call = {
            let mut calls = self.calls.authority.lock().unwrap();
            calls.push((original.clone(), reason));
            calls.len()
        };
        if call != self.at_call {
            return Ok(());
        }
        if !self.call_gate.hold() {
            return Err(RecoveryConfinementAuthorityRefusal::Unavailable);
        }
        if matches!(self.mode, ExtraHeldMode::PanicPayload) {
            std::panic::panic_any(ExtraOpaquePayload {
                gate: self.cleanup_gate.as_ref().unwrap().clone(),
                _store: self.store.upgrade().unwrap(),
                _application: self.application.upgrade().unwrap(),
            });
        }
        Ok(())
    }
}
impl Drop for ExtraHeldAuthority {
    fn drop(&mut self) {
        if matches!(self.mode, ExtraHeldMode::Destructor) {
            self.cleanup_gate.as_ref().unwrap().hold();
        }
    }
}
pub struct ExtraOpaquePayload {
    gate: Arc<ExtraGate>,
    _store: Arc<SqliteStore>,
    _application: Arc<NativeApplication>,
}
impl Drop for ExtraOpaquePayload {
    fn drop(&mut self) {
        self.gate.hold();
    }
}
pub async fn extra_gate_or_done(
    gate: &ExtraGate,
    task: &mut tokio::task::JoinHandle<
        Result<RecoveryConfinementOutcome, RecoveryConfinementError>,
    >,
) -> Option<Result<RecoveryConfinementOutcome, RecoveryConfinementError>> {
    tokio::time::timeout(WAIT, async { tokio::select! { _ = gate.entered() => None, completed = task => Some(completed.expect("owned finite public task joined normally")) } }).await.expect("finite actual entry or completion")
}
