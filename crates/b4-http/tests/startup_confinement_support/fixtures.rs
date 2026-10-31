use super::inherited;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::types::Value as SqlValue;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const WAIT: Duration = Duration::from_secs(15);
type RawTables = Vec<(String, Vec<Vec<SqlValue>>)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositiveCase {
    MissingDefinition,
    DeniedRecover,
}
impl PositiveCase {
    fn reason(self) -> RecoveryConfinementReason {
        match self {
            Self::MissingDefinition => RecoveryConfinementReason::ExactDefinitionUnavailable,
            Self::DeniedRecover => RecoveryConfinementReason::DispatchNotAuthorized,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct State {
    view: ThreadView,
    approval: ApprovalView,
    journal: Vec<JournalEntry>,
    raw: RawTables,
}
impl State {
    fn read(store: &SqliteStore, directory: &inherited::Directory, thread: &ThreadId) -> Self {
        let view = store.thread_view(thread).unwrap().unwrap();
        let id = &view.execution.as_ref().unwrap().snapshot.execution_id;
        Self {
            approval: store.approval_view(id).unwrap().unwrap(),
            journal: store.journal(id).unwrap(),
            raw: directory.rows(),
            view,
        }
    }
}

#[derive(Default)]
struct Calls {
    effects: AtomicUsize,
    validators: AtomicUsize,
    resolved: Mutex<Vec<ThreadOrigin>>,
    policy: Mutex<Vec<(Principal, ResourceContext)>>,
    authority: Mutex<Vec<(ThreadView, RecoveryConfinementReason)>>,
}
struct Effect(Arc<Calls>);
impl NativeEffect for Effect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.0.effects.fetch_add(1, Ordering::AcqRel);
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: serde_json::json!("unexpected-dispatch"),
            writes: vec![],
        }))
    }
}
struct Validator(Arc<Calls>);
impl InputValidator for Validator {
    fn validate(&self, _: &serde_json::Value) -> Result<(), InvalidApplicationInput> {
        self.0.validators.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
struct Policy {
    calls: Arc<Calls>,
    deny_recover: bool,
}
impl ResourcePolicy for Policy {
    fn authorize(&self, actor: &Principal, context: &ResourceContext) -> bool {
        self.calls
            .policy
            .lock()
            .unwrap()
            .push((actor.clone(), context.clone()));
        actor == &inherited::principal()
            && !(self.deny_recover && matches!(context.operation, Operation::Recover))
    }
}
struct Resolver(Arc<Calls>);
impl StartupPrincipalResolver for Resolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        self.0.resolved.lock().unwrap().push(origin.clone());
        assert_eq!(origin, &inherited::origin());
        Ok(inherited::principal())
    }
}
struct Authority(Arc<Calls>);
impl RecoveryConfinementAuthority for Authority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        self.0
            .authority
            .lock()
            .unwrap()
            .push((original.clone(), reason));
        Ok(())
    }
}

struct Fixture {
    directory: inherited::Directory,
    store: Weak<SqliteStore>,
    application: Weak<NativeApplication>,
    host: Option<PreparingHttpHost<SqliteStore>>,
    shutdown: ShutdownHandle,
    original: State,
    owner: u64,
    calls: Arc<Calls>,
}
impl Fixture {
    fn new(case: PositiveCase) -> Self {
        let directory = inherited::Directory::new();
        let seed = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let seeded = inherited::seed_named(
            seed.clone(),
            "confinement-positive",
            match case {
                PositiveCase::MissingDefinition => inherited::SeedMode::Accepted,
                PositiveCase::DeniedRecover => inherited::SeedMode::Approved,
            },
            true,
        );
        let seed_state = State::read(&seed, &directory, &seeded.thread.id);
        assert_eq!(seed_state.approval, seeded);
        b4_kernel::validate_snapshot(&seeded.execution.snapshot).unwrap();
        assert_eq!(seeded.thread.origin, Some(inherited::origin()));
        if case == PositiveCase::DeniedRecover {
            assert_eq!(seeded.execution.snapshot.status, ExecutionStatus::Running);
            assert_eq!(seeded.execution.snapshot.tasks.len(), 1);
            let approved = &seeded.execution.snapshot.tasks[0];
            assert_eq!(approved.status, TaskStatus::Ready);
            assert!(approved.approved);
            assert!(approved.spec.requires_approval);
            assert_eq!(
                approved.spec,
                seeded.execution.snapshot.definition.batches[0][0]
            );
            assert_eq!(approved.input, seeded.execution.snapshot.values);
            assert!(approved.attempt_id.is_none());
            assert!(approved.result.is_none());
            assert!(approved.failure.is_none());
            assert_eq!(seeded.grants.len(), 1);
            assert_eq!(
                approved.interrupt.as_ref().unwrap().id,
                seeded.grants[0].interrupt_id
            );
            assert!(seeded.grants[0].consumed);
            assert_eq!(seeded.acceptances.len(), 1);
            assert_eq!(seeded.acceptances[0].decisions.len(), 1);
            assert_eq!(
                seeded.acceptances[0].decisions[0].decision,
                ApprovalDecision::Once
            );
            assert_eq!(
                seeded.execution.snapshot.turn_id,
                seeded.acceptances[0].turn_id
            );
        }
        // The inherited helper drops the only owner-N alias and proves Weak absence.
        let store = inherited::reopen(seed, &directory);
        let owner = store.owner_generation();
        let original = State::read(&store, &directory, &seeded.thread.id);
        assert_eq!(original.view, seed_state.view);
        assert_eq!(original.approval, seed_state.approval);
        assert_eq!(original.journal, seed_state.journal);
        assert_owner_advance(&seed_state.raw, &original.raw, owner);
        assert!(owner > original.approval.execution.snapshot.owner_generation);
        let target = RecoveryConfinementTarget::from_view(&original.view).unwrap();
        // Actual bounded backend audit establishes legal provenance before the API.
        assert_eq!(
            store.confinement_view_fenced(&target).unwrap(),
            original.view
        );
        store.readiness().unwrap();

        let calls = Arc::new(Calls::default());
        let old = &original.approval.execution.snapshot.definition;
        let mut current = old.clone();
        if case == PositiveCase::MissingDefinition {
            current.revision = ExecutionRevision::new("startup-confinement-newer-2").unwrap();
        }
        let effects = current
            .batches
            .iter()
            .flatten()
            .map(|task| {
                (
                    task.effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(Effect(calls.clone())) as Arc<dyn NativeEffect>,
                        output_kind: OutputKind::Value,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: inherited::principal().application,
                    revision: "startup-confinement-native-2".into(),
                },
                vec![RouteRegistration {
                    definition: current,
                    effects,
                    input_validator: Some(Arc::new(Validator(calls.clone()))),
                }],
            )
            .unwrap(),
        );
        assert_eq!(
            application.definition_exact(old).is_some(),
            case == PositiveCase::DeniedRecover
        );
        assert!(application.definitions().any(|d| d.route == old.route));
        let mut configuration = inherited::config_for(application.clone());
        configuration.approval.policy_revision = "startup-confinement-current-policy-2".into();
        assert_eq!(
            configuration.approval.service_config,
            ApprovalServiceConfig::RequiredDurable
        );
        let host = HttpHost::new_with_application_preparing(
            store.clone(),
            Arc::new(inherited::Auth),
            Arc::new(Policy {
                calls: calls.clone(),
                deny_recover: case == PositiveCase::DeniedRecover,
            }),
            configuration,
        )
        .unwrap();
        let shutdown = host.shutdown_handle();
        let weak_store = Arc::downgrade(&store);
        let weak_application = Arc::downgrade(&application);
        drop(store);
        drop(application);
        inherited::assert_locked(&directory);
        Self {
            directory,
            store: weak_store,
            application: weak_application,
            host: Some(host),
            shutdown,
            original,
            owner,
            calls,
        }
    }

    fn state(&self) -> State {
        State::read(
            &self.store.upgrade().unwrap(),
            &self.directory,
            &self.original.view.thread.id,
        )
    }

    async fn retire(mut self) -> State {
        let before_retirement = self.state();
        self.shutdown.begin_shutdown();
        drop(self.host.take());
        let shutdown = tokio::time::timeout(WAIT, self.shutdown.wait_quiescent(WAIT))
            .await
            .expect("normal shutdown observation must remain bounded")
            .unwrap();
        assert_eq!(shutdown, ShutdownOutcome::Quiescent { faulted: false });
        assert_eq!(self.shutdown.status().active_owners, 0);
        assert!(self.store.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let next = Arc::new(SqliteStore::open(self.directory.path()).unwrap());
        assert_eq!(next.owner_generation(), self.owner.checked_add(1).unwrap());
        next.readiness().unwrap();
        let persisted = State::read(&next, &self.directory, &self.original.view.thread.id);
        assert_eq!(persisted.view, before_retirement.view);
        assert_eq!(persisted.approval, before_retirement.approval);
        assert_eq!(persisted.journal, before_retirement.journal);
        assert_owner_advance(
            &before_retirement.raw,
            &persisted.raw,
            next.owner_generation(),
        );
        let weak = Arc::downgrade(&next);
        drop(next);
        assert!(weak.upgrade().is_none());
        eprintln!("B4_STARTUP_CONFINEMENT_NORMAL_RETIREMENT healthy_quiescent weak_store_none weak_application_none next_writer_closed");
        before_retirement
    }
}

fn blob(number: u64) -> SqlValue {
    SqlValue::Blob(number.to_be_bytes().to_vec())
}
fn table<'a>(rows: &'a mut RawTables, name: &str) -> &'a mut Vec<Vec<SqlValue>> {
    &mut rows.iter_mut().find(|(key, _)| key == name).unwrap().1
}
fn assert_owner_advance(original: &RawTables, actual: &RawTables, owner: u64) {
    let mut expected = original.clone();
    let metadata = table(&mut expected, "metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].len(), 4);
    metadata[0][3] = blob(owner);
    assert_eq!(actual, &expected);
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
fn expected_confined(
    original: &State,
    actual: &State,
    owner: u64,
    case: PositiveCase,
    started: u64,
    ended: u64,
) -> State {
    let old = &original.approval.execution.snapshot;
    let observed = &actual.approval.execution.snapshot;
    assert!(!old.turn_history.contains(&observed.turn_id));
    assert!((started..=ended).contains(&observed.updated_at_ms));
    let mut expected = original.clone();
    let mut snapshot = old.clone();
    snapshot.status = ExecutionStatus::RecoveryBlocked;
    snapshot.failure = Some(
        match case {
            PositiveCase::MissingDefinition => "recovery_definition_unavailable",
            PositiveCase::DeniedRecover => "recovery_dispatch_not_authorized",
        }
        .into(),
    );
    snapshot.owner_generation = owner;
    snapshot.revision = old.revision.checked_add(1).unwrap();
    snapshot.turn_id = observed.turn_id.clone();
    snapshot.turn_history.push(observed.turn_id.clone());
    snapshot.updated_at_ms = observed.updated_at_ms;
    expected.view.thread.resource_revision = original
        .view
        .thread
        .resource_revision
        .checked_add(1)
        .unwrap();
    expected.view.execution.as_mut().unwrap().snapshot = snapshot.clone();
    expected.approval.thread = expected.view.thread.clone();
    expected.approval.execution.snapshot = snapshot.clone();
    let entry = JournalEntry {
        execution_id: snapshot.execution_id.clone(),
        revision: snapshot.revision,
        owner_generation: owner,
        status: ExecutionStatus::RecoveryBlocked,
        updated_at_ms: snapshot.updated_at_ms,
    };
    expected.journal.push(entry.clone());
    let threads = table(&mut expected.raw, "threads");
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].len(), 7);
    threads[0][5] = blob(expected.view.thread.resource_revision);
    let executions = table(&mut expected.raw, "executions");
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].len(), 5);
    executions[0][2] = blob(snapshot.revision);
    executions[0][3] = blob(owner);
    executions[0][4] = SqlValue::Text(serde_json::to_string(&snapshot).unwrap());
    let journal = table(&mut expected.raw, "journal");
    let SqlValue::Integer(last) = journal.last().unwrap()[0] else {
        panic!("legal SQLite journal sequence")
    };
    journal.push(vec![
        SqlValue::Integer(last.checked_add(1).unwrap()),
        SqlValue::Text("commit".into()),
        SqlValue::Text(expected.view.thread.id.as_str().into()),
        SqlValue::Text(snapshot.execution_id.as_str().into()),
        blob(entry.revision),
        blob(owner),
        SqlValue::Text(serde_json::to_string(&entry.status).unwrap()),
        blob(entry.updated_at_ms),
        SqlValue::Text(serde_json::to_string(&entry).unwrap()),
    ]);
    expected
}

pub async fn run_positive(case: PositiveCase) {
    let fixture = Fixture::new(case);
    let original = fixture.original.clone();
    let calls = fixture.calls.clone();
    let owner = fixture.owner;
    let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver(calls.clone()));
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(Authority(calls.clone()));
    let started = now_ms();
    let handle = fixture
        .host
        .as_ref()
        .unwrap()
        .begin_preparation_with_confinement(
            &resolver,
            &authority,
            StartupPreparationLimits::default(),
        )
        .await
        .expect("cleanly compiled compatibility scaffold starts normally");
    let outcome = match tokio::time::timeout(WAIT, handle.wait(WAIT))
        .await
        .expect("finite preparation wait")
        .unwrap()
    {
        StartupConfinementWaitOutcome::Finished(outcome) => outcome,
        StartupConfinementWaitOutcome::Pending(status) => {
            panic!("not a meaningful feature result: {status:?}")
        }
    };
    let ended = now_ms();
    drop(resolver);
    drop(authority);
    let actual = fixture.retire().await;
    // Everything above is setup/physical retirement. The intended semantic
    // assertion comes only after genuine normal writer succession.
    assert_eq!(calls.effects.load(Ordering::Acquire), 0);
    assert_eq!(calls.validators.load(Ordering::Acquire), 0);
    let progress = StartupProgress {
        pages: 1,
        scanned_rows: 1,
        candidates: 1,
        ..StartupProgress::default()
    };
    if matches!(
        outcome.preparation,
        StartupPreparationOutcome::Incomplete { .. }
    ) {
        assert_eq!(actual, original);
        assert_eq!(
            outcome.preparation,
            StartupPreparationOutcome::Incomplete {
                progress,
                reason: StartupIncompleteReason::RecoveryRefused
            }
        );
        assert_eq!(outcome.confinement, StartupConfinementProgress::default());
        assert!(calls.authority.lock().unwrap().is_empty());
        assert_eq!(
            calls.resolved.lock().unwrap().as_slice(),
            &[inherited::origin()]
        );
        // The protected old preflight checks policy before exact Definition.
        assert_eq!(calls.policy.lock().unwrap().len(), 1);
    }
    for (actor, context) in calls.policy.lock().unwrap().iter() {
        assert_eq!(actor, &inherited::principal());
        assert!(matches!(context.operation, Operation::Recover));
        assert_eq!(context.origin, Some(inherited::origin()));
        assert_eq!(context.thread_id, Some(original.view.thread.id.clone()));
        let requested = context.requested_route.as_ref().unwrap();
        assert_eq!(
            requested.route,
            original.approval.execution.snapshot.definition.route
        );
        assert_eq!(
            requested.revision,
            original.approval.execution.snapshot.definition.revision
        );
        let provenance = context.stored_route.as_ref().unwrap();
        assert_eq!(
            provenance.route,
            original.approval.execution.snapshot.definition.route
        );
        assert_eq!(
            provenance.revision,
            original.approval.execution.snapshot.definition.revision
        );
    }
    eprintln!("B4_STARTUP_CONFINEMENT_INTENDED_FEATURE_ASSERTION case={case:?} observed={outcome:?} physical_retirement_complete");
    assert_eq!(
        outcome.preparation,
        StartupPreparationOutcome::Complete { progress },
        "opt-in startup must durably confine the original record before activation"
    );
    assert_eq!(
        outcome.confinement,
        StartupConfinementProgress {
            confirmed_blocks: 1,
            already_blocked: 0
        }
    );
    assert_eq!(
        actual,
        expected_confined(&original, &actual, owner, case, started, ended)
    );
    assert_eq!(
        calls.authority.lock().unwrap().as_slice(),
        &[
            (original.view.clone(), case.reason()),
            (original.view.clone(), case.reason())
        ]
    );
    assert_eq!(
        calls.resolved.lock().unwrap().as_slice(),
        &[
            inherited::origin(),
            inherited::origin(),
            inherited::origin()
        ]
    );
    assert_eq!(
        calls.policy.lock().unwrap().len(),
        if case == PositiveCase::DeniedRecover {
            2
        } else {
            0
        }
    );
}

fn one_candidate_progress() -> StartupProgress {
    StartupProgress {
        pages: 1,
        scanned_rows: 1,
        candidates: 1,
        ..StartupProgress::default()
    }
}

pub async fn run_default_refusal() {
    for case in [PositiveCase::MissingDefinition, PositiveCase::DeniedRecover] {
        let fixture = Fixture::new(case);
        let original = fixture.original.clone();
        let calls = fixture.calls.clone();
        let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver(calls.clone()));
        let handle = fixture
            .host
            .as_ref()
            .unwrap()
            .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
            .unwrap();
        let outcome = match tokio::time::timeout(WAIT, handle.wait(WAIT))
            .await
            .expect("finite default preparation wait")
            .unwrap()
        {
            StartupWaitOutcome::Finished(outcome) => outcome,
            StartupWaitOutcome::Pending(status) => {
                panic!("default preparation pending: {status:?}")
            }
        };
        drop(resolver);
        let actual = fixture.retire().await;
        assert_eq!(actual, original);
        assert_eq!(
            outcome,
            StartupPreparationOutcome::Incomplete {
                progress: one_candidate_progress(),
                reason: StartupIncompleteReason::RecoveryRefused,
            }
        );
        assert_eq!(calls.effects.load(Ordering::Acquire), 0);
        assert_eq!(calls.validators.load(Ordering::Acquire), 0);
        assert!(calls.authority.lock().unwrap().is_empty());
        assert_eq!(
            calls.resolved.lock().unwrap().as_slice(),
            &[inherited::origin()]
        );
        let policy = calls.policy.lock().unwrap();
        assert_eq!(policy.len(), 1);
        let (actor, context) = &policy[0];
        assert_eq!(actor, &inherited::principal());
        assert!(matches!(context.operation, Operation::Recover));
        assert_eq!(context.origin, Some(inherited::origin()));
        assert_eq!(context.thread_id, Some(original.view.thread.id.clone()));
        let definition = &original.approval.execution.snapshot.definition;
        for route in [
            context.requested_route.as_ref().unwrap(),
            context.stored_route.as_ref().unwrap(),
        ] {
            assert_eq!(route.route, definition.route);
            assert_eq!(route.revision, definition.revision);
        }
        eprintln!(
            "B4_STARTUP_CONFINEMENT_DEFAULT_REFUSAL unchanged_state healthy_normal_retirement"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum RefusalCase {
    Unresolved,
    MismatchedPrincipal,
    AuthorityDenied,
    AuthorityUnavailable,
}
struct RefusingResolver {
    calls: Arc<Calls>,
    case: RefusalCase,
}
impl StartupPrincipalResolver for RefusingResolver {
    fn resolve(&self, origin: &ThreadOrigin) -> Result<Principal, StartupResolutionError> {
        assert_eq!(origin, &inherited::origin());
        self.calls.resolved.lock().unwrap().push(origin.clone());
        match self.case {
            RefusalCase::Unresolved => Err(StartupResolutionError::Unavailable),
            RefusalCase::MismatchedPrincipal => {
                let mut principal = inherited::principal();
                principal.principal.push_str("-different");
                Ok(principal)
            }
            RefusalCase::AuthorityDenied | RefusalCase::AuthorityUnavailable => {
                Ok(inherited::principal())
            }
        }
    }
}
struct RefusingAuthority {
    calls: Arc<Calls>,
    case: RefusalCase,
}
impl RecoveryConfinementAuthority for RefusingAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        self.calls
            .authority
            .lock()
            .unwrap()
            .push((original.clone(), reason));
        match self.case {
            RefusalCase::AuthorityDenied => Err(RecoveryConfinementAuthorityRefusal::Denied),
            RefusalCase::AuthorityUnavailable => {
                Err(RecoveryConfinementAuthorityRefusal::Unavailable)
            }
            RefusalCase::Unresolved | RefusalCase::MismatchedPrincipal => Ok(()),
        }
    }
}

pub async fn run_principal_authority_refusals() {
    for case in [
        RefusalCase::Unresolved,
        RefusalCase::MismatchedPrincipal,
        RefusalCase::AuthorityDenied,
        RefusalCase::AuthorityUnavailable,
    ] {
        let fixture = Fixture::new(PositiveCase::MissingDefinition);
        let original = fixture.original.clone();
        let calls = fixture.calls.clone();
        let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(RefusingResolver {
            calls: calls.clone(),
            case,
        });
        let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(RefusingAuthority {
            calls: calls.clone(),
            case,
        });
        let handle = fixture
            .host
            .as_ref()
            .unwrap()
            .begin_preparation_with_confinement(
                &resolver,
                &authority,
                StartupPreparationLimits::default(),
            )
            .await
            .unwrap();
        let outcome = match tokio::time::timeout(WAIT, handle.wait(WAIT))
            .await
            .expect("finite principal/authority refusal wait")
            .unwrap()
        {
            StartupConfinementWaitOutcome::Finished(outcome) => outcome,
            StartupConfinementWaitOutcome::Pending(status) => {
                panic!("refusal preparation pending: {status:?}")
            }
        };
        drop(resolver);
        drop(authority);
        let actual = fixture.retire().await;
        assert_eq!(actual, original);
        assert_eq!(calls.effects.load(Ordering::Acquire), 0);
        assert_eq!(calls.validators.load(Ordering::Acquire), 0);
        assert_eq!(outcome.confinement, StartupConfinementProgress::default());
        let unresolved = matches!(
            case,
            RefusalCase::Unresolved | RefusalCase::MismatchedPrincipal
        );
        assert_eq!(
            outcome.preparation,
            StartupPreparationOutcome::Incomplete {
                progress: one_candidate_progress(),
                reason: if unresolved {
                    StartupIncompleteReason::ResolutionRefused
                } else {
                    StartupIncompleteReason::RecoveryRefused
                },
            }
        );
        if unresolved {
            assert_eq!(
                calls.resolved.lock().unwrap().as_slice(),
                &[inherited::origin()]
            );
            assert!(calls.authority.lock().unwrap().is_empty());
            assert!(calls.policy.lock().unwrap().is_empty());
        } else {
            // The compatibility scaffold refuses before the new permission path.
            // Validate its truthful controls before any desired newguard assertion.
            if calls.authority.lock().unwrap().is_empty() {
                assert_eq!(
                    calls.resolved.lock().unwrap().as_slice(),
                    &[inherited::origin()]
                );
                assert_eq!(calls.policy.lock().unwrap().len(), 1);
            }
            eprintln!("B4_STARTUP_CONFINEMENT_INTENDED_AUTHORITY_REFUSAL case={case:?} healthy_normal_retirement unchanged_state");
            assert_eq!(calls.authority.lock().unwrap().as_slice(), &[(original.view.clone(), RecoveryConfinementReason::ExactDefinitionUnavailable)], "separate confinement permission must be consulted after exact startup principal verification");
            assert_eq!(
                calls.resolved.lock().unwrap().as_slice(),
                &[inherited::origin(), inherited::origin()]
            );
            assert!(calls.policy.lock().unwrap().is_empty());
        }
        eprintln!("B4_STARTUP_CONFINEMENT_REFUSAL_CASE_FINISHED case={case:?} normal_retirement_unchanged");
    }
}

struct GatedAuthority {
    calls: Arc<Calls>,
    permission: Arc<super::gate::Gate>,
    cleanup: Arc<super::gate::Gate>,
    held_store: Option<Arc<SqliteStore>>,
    held_application: Option<Arc<NativeApplication>>,
}
impl RecoveryConfinementAuthority for GatedAuthority {
    fn authorize(
        &self,
        original: &ThreadView,
        reason: RecoveryConfinementReason,
    ) -> Result<(), RecoveryConfinementAuthorityRefusal> {
        let first = {
            let mut calls = self.calls.authority.lock().unwrap();
            calls.push((original.clone(), reason));
            calls.len() == 1
        };
        if first && !self.permission.hold() {
            return Err(RecoveryConfinementAuthorityRefusal::Unavailable);
        }
        Ok(())
    }
}
impl Drop for GatedAuthority {
    fn drop(&mut self) {
        assert!(
            self.cleanup.hold(),
            "authority cleanup must be explicitly released without expiry"
        );
        drop(self.held_store.take());
        drop(self.held_application.take());
    }
}
async fn tcp_statuses(server: &super::server::Server, thread: &ThreadId) -> [u16; 3] {
    [
        server.get("/healthz").await.0,
        server.get("/readyz").await.0,
        server
            .request(
                "GET",
                &format!("/threads/{}", thread.as_str()),
                Some("Bearer startup-fixture"),
                &[],
                0,
            )
            .await
            .0,
    ]
}

pub async fn run_tcp_single() {
    let fixture = Fixture::new(PositiveCase::MissingDefinition);
    let original = fixture.original.clone();
    let owner = fixture.owner;
    let calls = fixture.calls.clone();
    let thread = original.view.thread.id.clone();
    let server = super::server::Server::start(fixture.host.as_ref().unwrap().router()).await;
    let before = tcp_statuses(&server, &thread).await;
    let permission = super::gate::Gate::new();
    let cleanup = super::gate::Gate::new();
    let _permission_cleanup = super::gate::Cleanup(permission.clone());
    let _drop_cleanup = super::gate::Cleanup(cleanup.clone());
    let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver(calls.clone()));
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(GatedAuthority {
        calls: calls.clone(),
        permission: permission.clone(),
        cleanup: cleanup.clone(),
        held_store: Some(fixture.store.upgrade().unwrap()),
        held_application: Some(fixture.application.upgrade().unwrap()),
    });
    let started = now_ms();
    let handle = fixture
        .host
        .as_ref()
        .unwrap()
        .begin_preparation_with_confinement(
            &resolver,
            &authority,
            StartupPreparationLimits::default(),
        )
        .await
        .unwrap();
    // Remove all caller aliases before releasing the operation or capture Drop.
    drop(resolver);
    drop(authority);
    permission.entered().await;
    let while_permission = tcp_statuses(&server, &thread).await;
    let permission_phase = handle.status().preparation.phase;
    permission.release();
    cleanup.entered().await;
    let while_cleanup = tcp_statuses(&server, &thread).await;
    let cleanup_status = handle.status();
    let cleanup_owners = fixture.shutdown.status().active_owners;
    let cleanup_store_live = fixture.store.upgrade().is_some();
    let cleanup_application_live = fixture.application.upgrade().is_some();
    let cleanup_writer_locked = matches!(
        SqliteStore::open(fixture.directory.path()),
        Err(StoreError::AlreadyOwned)
    );
    cleanup.release();
    let outcome = match tokio::time::timeout(WAIT, handle.wait(WAIT))
        .await
        .expect("finite TCP preparation completion after explicit cleanup release")
        .unwrap()
    {
        StartupConfinementWaitOutcome::Finished(outcome) => outcome,
        StartupConfinementWaitOutcome::Pending(status) => {
            panic!("TCP preparation still pending: {status:?}")
        }
    };
    let ended = now_ms();
    let after = tcp_statuses(&server, &thread).await;
    let state = server
        .request(
            "GET",
            &format!("/threads/{}/state", thread.as_str()),
            Some("Bearer startup-fixture"),
            &[],
            0,
        )
        .await
        .0;
    server.finish().await;
    let actual = fixture.retire().await;
    // All sockets/server tasks and real native resources normally retired first.
    permission.positive();
    cleanup.positive();
    assert_eq!(before, [200, 503, 503]);
    assert_eq!(while_permission, [200, 503, 503]);
    assert_eq!(while_cleanup, [200, 503, 503]);
    assert_eq!(permission_phase, StartupPreparationPhase::Preparing);
    assert_eq!(
        cleanup_status.preparation.phase,
        StartupPreparationPhase::Preparing
    );
    assert_eq!(cleanup_status.confinement.confirmed_blocks, 1);
    assert!(cleanup_owners > 0);
    assert!(cleanup_store_live && cleanup_application_live && cleanup_writer_locked);
    assert_eq!(after, [200, 200, 200]);
    assert_eq!(state, 501);
    assert_eq!(
        outcome.preparation,
        StartupPreparationOutcome::Complete {
            progress: one_candidate_progress()
        }
    );
    assert_eq!(
        outcome.confinement,
        StartupConfinementProgress {
            confirmed_blocks: 1,
            already_blocked: 0
        }
    );
    assert_eq!(
        actual,
        expected_confined(
            &original,
            &actual,
            owner,
            PositiveCase::MissingDefinition,
            started,
            ended
        )
    );
    assert_eq!(calls.effects.load(Ordering::Acquire), 0);
    assert_eq!(calls.validators.load(Ordering::Acquire), 0);
    assert_eq!(
        calls.authority.lock().unwrap().as_slice(),
        &[
            (
                original.view.clone(),
                PositiveCase::MissingDefinition.reason()
            ),
            (
                original.view.clone(),
                PositiveCase::MissingDefinition.reason()
            )
        ]
    );
    assert_eq!(
        calls.resolved.lock().unwrap().as_slice(),
        &[
            inherited::origin(),
            inherited::origin(),
            inherited::origin()
        ]
    );
    let policies = calls.policy.lock().unwrap();
    assert!(policies
        .iter()
        .all(|(actor, context)| actor == &inherited::principal()
            && !matches!(context.operation, Operation::Recover)));
    eprintln!("B4_STARTUP_CONFINEMENT_TCP_PERMISSION_AND_CAPTURE_CLEANUP_GATE_RETURNED normal_server_join healthy_weak_absence real_successor_closed");
}

#[derive(Clone, Copy, Debug)]
enum CaptureMode {
    Native,
    Unsupported,
    UnsupportedProfile,
    TargetChanged,
    WholeViewChanged,
}
struct ApiStore {
    inner: Arc<SqliteStore>,
    mode: CaptureMode,
    captures: Arc<Mutex<Vec<RecoveryConfinementTarget>>>,
    mutations: Arc<Mutex<Vec<RecoveryConfinementTarget>>>,
}
impl Store for ApiStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
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
        self.inner.commit(id, fence, plan)
    }
    fn journal(&self, id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(id)
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
    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.inner.discover_unfinished(cursor)
    }
    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        self.captures.lock().unwrap().push(target.clone());
        match self.mode {
            CaptureMode::Unsupported => {
                return Err(RecoveryConfinementBackendError::UnsupportedOperation)
            }
            CaptureMode::UnsupportedProfile => {
                return Err(RecoveryConfinementBackendError::UnsupportedAuthorityProfile)
            }
            CaptureMode::TargetChanged => {
                return Err(RecoveryConfinementBackendError::TargetChanged)
            }
            _ => (),
        }
        let mut view = self.inner.confinement_view_fenced(target)?;
        if matches!(self.mode, CaptureMode::WholeViewChanged) {
            view.thread
                .metadata
                .insert("changed_full_bootstrap".into(), serde_json::json!(true));
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
        self.mutations.lock().unwrap().push(target.clone());
        self.inner
            .confine_recovery_fenced(original, target, reason, turn, facts)
    }
}
impl ApprovalStore for ApiStore {
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

#[derive(Clone, Copy, Debug)]
enum Supplement {
    AlreadyBlocked,
    HistoricalAllowed,
    MissingThree,
    Custom(CaptureMode),
    Mixed,
}
#[derive(Default)]
struct NativeCalls {
    original: Mutex<Vec<(EffectInvocation, InvocationContext)>>,
    newer: AtomicUsize,
}
struct HeldEffect {
    calls: Arc<NativeCalls>,
    gate: Arc<super::gate::Gate>,
    newer: bool,
}
impl NativeEffect for HeldEffect {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        if self.newer {
            self.calls.newer.fetch_add(1, Ordering::AcqRel);
        } else {
            self.calls
                .original
                .lock()
                .unwrap()
                .push((invocation.clone(), context.clone()));
        }
        if !self.gate.hold() {
            return Err(EffectUncertain);
        }
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: serde_json::json!("original-typed-recovery"),
            writes: vec![],
        }))
    }
}
fn held_registration(
    definition: Definition,
    calls: Arc<Calls>,
    native: Arc<NativeCalls>,
    gate: Arc<super::gate::Gate>,
    newer: bool,
) -> RouteRegistration {
    let effects = definition
        .batches
        .iter()
        .flatten()
        .map(|task| {
            (
                task.effect_id.clone(),
                EffectBinding {
                    callback: Arc::new(HeldEffect {
                        calls: native.clone(),
                        gate: gate.clone(),
                        newer,
                    }) as Arc<dyn NativeEffect>,
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    RouteRegistration {
        definition,
        effects,
        input_validator: Some(Arc::new(Validator(calls))),
    }
}
struct ApiFixture {
    directory: inherited::Directory,
    store: Weak<ApiStore>,
    sqlite: Weak<SqliteStore>,
    application: Weak<NativeApplication>,
    host: Option<PreparingHttpHost<ApiStore>>,
    shutdown: ShutdownHandle,
    originals: Vec<State>,
    owner: u64,
    calls: Arc<Calls>,
    native: Arc<NativeCalls>,
    captures: Arc<Mutex<Vec<RecoveryConfinementTarget>>>,
    mutations: Arc<Mutex<Vec<RecoveryConfinementTarget>>>,
}
impl ApiFixture {
    fn new(kind: Supplement, effect_gate: Arc<super::gate::Gate>) -> Self {
        let directory = inherited::Directory::new();
        let prior = Arc::new(SqliteStore::open(directory.path()).unwrap());
        let count = if matches!(kind, Supplement::MissingThree) {
            3
        } else if matches!(kind, Supplement::Mixed) {
            2
        } else {
            1
        };
        let mut threads = Vec::new();
        for index in 0..count {
            let name = format!("supplement-{index}");
            if matches!(kind, Supplement::Mixed) && index == 1 {
                let thread = ThreadId::new(format!("startup-thread-{name}")).unwrap();
                prior
                    .create_thread_with(ThreadSeed {
                        id: thread.clone(),
                        created_at_ms: 1,
                        metadata: serde_json::Map::from_iter([(
                            "retained".into(),
                            serde_json::json!(name),
                        )]),
                        origin: Some(inherited::origin()),
                    })
                    .unwrap();
                let mut definition = inherited::definition();
                definition.route = RouteKey::new("/startup-mixed-available#workflow").unwrap();
                let initial = b4_kernel::new_execution(
                    ExecutionId::new(format!("startup-execution-{name}")).unwrap(),
                    TurnId::new(format!("startup-original-turn-{name}")).unwrap(),
                    prior.owner_generation(),
                    definition,
                    serde_json::Map::from_iter([("retained".into(), serde_json::json!(name))]),
                    TransitionFacts { now_ms: 2 },
                )
                .unwrap();
                prior.admit(&thread, initial).unwrap();
                threads.push(thread);
            } else {
                threads.push(
                    inherited::seed_named(
                        prior.clone(),
                        &name,
                        inherited::SeedMode::Accepted,
                        true,
                    )
                    .thread
                    .id,
                );
            }
        }
        let before: Vec<_> = threads
            .iter()
            .map(|thread| State::read(&prior, &directory, thread))
            .collect();
        let mut inner = inherited::reopen(prior, &directory);
        for original in &before {
            let after = State::read(&inner, &directory, &original.view.thread.id);
            assert_eq!(after.view, original.view);
            assert_eq!(after.approval, original.approval);
            assert_eq!(after.journal, original.journal);
            assert_owner_advance(&original.raw, &after.raw, inner.owner_generation());
        }
        if matches!(kind, Supplement::AlreadyBlocked) {
            let view = inner.thread_view(&threads[0]).unwrap().unwrap();
            let target = RecoveryConfinementTarget::from_view(&view).unwrap();
            assert_eq!(inner.confinement_view_fenced(&target).unwrap(), view);
            assert!(matches!(
                inner
                    .confine_recovery_fenced(
                        &view,
                        &target,
                        RecoveryConfinementReason::ExactDefinitionUnavailable,
                        &TurnId::new("supplement-real-blocked-turn").unwrap(),
                        TransitionFacts { now_ms: 3 }
                    )
                    .unwrap(),
                RecoveryConfinementOutcome::Confined(_)
            ));
            inner = inherited::reopen(inner, &directory);
        }
        let owner = inner.owner_generation();
        let originals: Vec<_> = threads
            .iter()
            .map(|thread| State::read(&inner, &directory, thread))
            .collect();
        for original in &originals {
            let snapshot = &original.approval.execution.snapshot;
            b4_kernel::validate_snapshot(snapshot).unwrap();
            assert!(snapshot.owner_generation < owner);
            assert_eq!(original.view.thread.origin, Some(inherited::origin()));
            let target = RecoveryConfinementTarget::from_view(&original.view).unwrap();
            assert_eq!(
                inner.confinement_view_fenced(&target).unwrap(),
                original.view
            );
        }
        if matches!(kind, Supplement::AlreadyBlocked) {
            assert_eq!(
                originals[0].approval.execution.snapshot.status,
                ExecutionStatus::RecoveryBlocked
            );
        }
        let calls = Arc::new(Calls::default());
        let native = Arc::new(NativeCalls::default());
        let mut current = originals
            .last()
            .unwrap()
            .approval
            .execution
            .snapshot
            .definition
            .clone();
        let historical = if matches!(kind, Supplement::HistoricalAllowed) {
            let old = held_registration(
                current.clone(),
                calls.clone(),
                native.clone(),
                effect_gate.clone(),
                false,
            );
            current.revision = ExecutionRevision::new("supplement-newer-registration").unwrap();
            vec![old]
        } else {
            vec![]
        };
        if !matches!(kind, Supplement::HistoricalAllowed | Supplement::Mixed) {
            current.revision =
                ExecutionRevision::new("supplement-unavailable-registration").unwrap();
        }
        let registration = held_registration(
            current,
            calls.clone(),
            native.clone(),
            effect_gate,
            matches!(kind, Supplement::HistoricalAllowed),
        );
        let application = Arc::new(
            NativeApplication::new_with_history(
                ApplicationIdentity {
                    application: inherited::principal().application,
                    revision: "supplement-current-native".into(),
                },
                vec![registration],
                historical,
            )
            .unwrap(),
        );
        for (index, original) in originals.iter().enumerate() {
            assert_eq!(
                application
                    .definition_exact(&original.approval.execution.snapshot.definition)
                    .is_some(),
                matches!(kind, Supplement::HistoricalAllowed)
                    || matches!(kind, Supplement::Mixed) && index == 1
            );
        }
        let captures = Arc::new(Mutex::new(Vec::new()));
        let mutations = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(ApiStore {
            inner: inner.clone(),
            mode: match kind {
                Supplement::Custom(mode) => mode,
                _ => CaptureMode::Native,
            },
            captures: captures.clone(),
            mutations: mutations.clone(),
        });
        let mut configuration = inherited::config_for(application.clone());
        configuration.approval.policy_revision = "supplement-current-policy".into();
        let host = HttpHost::new_with_application_preparing(
            store.clone(),
            Arc::new(inherited::Auth),
            Arc::new(Policy {
                calls: calls.clone(),
                deny_recover: false,
            }),
            configuration,
        )
        .unwrap();
        let shutdown = host.shutdown_handle();
        let result = Self {
            directory,
            store: Arc::downgrade(&store),
            sqlite: Arc::downgrade(&inner),
            application: Arc::downgrade(&application),
            host: Some(host),
            shutdown,
            originals,
            owner,
            calls,
            native,
            captures,
            mutations,
        };
        drop(store);
        drop(inner);
        drop(application);
        inherited::assert_locked(&result.directory);
        result
    }
    async fn start(&self, limits: StartupPreparationLimits) -> StartupConfinementPreparationHandle {
        let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver(self.calls.clone()));
        let authority: Arc<dyn RecoveryConfinementAuthority> =
            Arc::new(Authority(self.calls.clone()));
        let result = self
            .host
            .as_ref()
            .unwrap()
            .begin_preparation_with_confinement(&resolver, &authority, limits)
            .await
            .unwrap();
        drop(resolver);
        drop(authority);
        result
    }
    fn states(&self) -> Vec<State> {
        let store = self.sqlite.upgrade().unwrap();
        self.originals
            .iter()
            .map(|original| State::read(&store, &self.directory, &original.view.thread.id))
            .collect()
    }
    async fn settled(&self, thread: &ThreadId) {
        tokio::time::timeout(WAIT, async {
            loop {
                let view = self
                    .sqlite
                    .upgrade()
                    .unwrap()
                    .thread_view(thread)
                    .unwrap()
                    .unwrap();
                if view.execution.unwrap().snapshot.status == ExecutionStatus::Completed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("explicitly released original callback must settle normally");
    }
    async fn retire(mut self) -> Vec<State> {
        let before = self.states();
        self.shutdown.begin_shutdown();
        drop(self.host.take());
        assert_eq!(
            tokio::time::timeout(WAIT, self.shutdown.wait_quiescent(WAIT))
                .await
                .unwrap()
                .unwrap(),
            ShutdownOutcome::Quiescent { faulted: false }
        );
        assert_eq!(self.shutdown.status().active_owners, 0);
        assert!(self.store.upgrade().is_none());
        assert!(self.sqlite.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let next = Arc::new(SqliteStore::open(self.directory.path()).unwrap());
        assert_eq!(next.owner_generation(), self.owner.checked_add(1).unwrap());
        next.readiness().unwrap();
        for original in &before {
            let persisted = State::read(&next, &self.directory, &original.view.thread.id);
            assert_eq!(persisted.view, original.view);
            assert_eq!(persisted.approval, original.approval);
            assert_eq!(persisted.journal, original.journal);
            assert_owner_advance(&original.raw, &persisted.raw, next.owner_generation());
        }
        let weak = Arc::downgrade(&next);
        drop(next);
        assert!(weak.upgrade().is_none());
        eprintln!("B4_STARTUP_SUPPLEMENT_NORMAL_RETIREMENT zero_owners weak_wrapper_sqlite_application_none next_writer_closed");
        before
    }
}
async fn supplement_finished(
    handle: &StartupConfinementPreparationHandle,
) -> StartupConfinementPreparationOutcome {
    match tokio::time::timeout(WAIT, handle.wait(WAIT))
        .await
        .expect("finite supplement startup wait")
        .unwrap()
    {
        StartupConfinementWaitOutcome::Finished(outcome) => outcome,
        StartupConfinementWaitOutcome::Pending(status) => {
            panic!("supplement startup still pending: {status:?}")
        }
    }
}
fn assert_no_dispatch(calls: &Calls, native: &NativeCalls) {
    assert_eq!(calls.validators.load(Ordering::Acquire), 0);
    assert_eq!(calls.effects.load(Ordering::Acquire), 0);
    assert!(native.original.lock().unwrap().is_empty());
    assert_eq!(native.newer.load(Ordering::Acquire), 0);
}
pub async fn run_already_blocked() {
    let gate = super::gate::Gate::new();
    let _cleanup = super::gate::Cleanup(gate.clone());
    let fixture = ApiFixture::new(Supplement::AlreadyBlocked, gate);
    let original = fixture.originals.clone();
    let calls = fixture.calls.clone();
    let native = fixture.native.clone();
    let captures = fixture.captures.clone();
    let mutations = fixture.mutations.clone();
    let handle = fixture.start(StartupPreparationLimits::default()).await;
    let outcome = supplement_finished(&handle).await;
    let actual = fixture.retire().await;
    assert_eq!(actual, original);
    assert_no_dispatch(&calls, &native);
    assert!(mutations.lock().unwrap().is_empty());
    assert!(calls.authority.lock().unwrap().is_empty());
    eprintln!("B4_STARTUP_SUPPLEMENT_INTENDED_ALREADY_BLOCKED_ASSERTION normal_retirement unchanged_state");
    assert_eq!(
        outcome.preparation,
        StartupPreparationOutcome::Complete {
            progress: one_candidate_progress()
        }
    );
    assert_eq!(
        outcome.confinement,
        StartupConfinementProgress {
            confirmed_blocks: 0,
            already_blocked: 1
        }
    );
    assert_eq!(
        captures.lock().unwrap().as_slice(),
        &[RecoveryConfinementTarget::from_view(&original[0].view).unwrap()]
    );
    assert_eq!(
        calls.resolved.lock().unwrap().as_slice(),
        &[inherited::origin()]
    );
    assert!(calls.policy.lock().unwrap().is_empty());
}
pub async fn run_custom_store_refusals() {
    for mode in [
        CaptureMode::Unsupported,
        CaptureMode::UnsupportedProfile,
        CaptureMode::TargetChanged,
        CaptureMode::WholeViewChanged,
    ] {
        let gate = super::gate::Gate::new();
        let _cleanup = super::gate::Cleanup(gate.clone());
        let fixture = ApiFixture::new(Supplement::Custom(mode), gate);
        let original = fixture.originals.clone();
        let calls = fixture.calls.clone();
        let native = fixture.native.clone();
        let captures = fixture.captures.clone();
        let mutations = fixture.mutations.clone();
        let handle = fixture.start(StartupPreparationLimits::default()).await;
        let outcome = supplement_finished(&handle).await;
        let actual = fixture.retire().await;
        assert_eq!(actual, original);
        assert_no_dispatch(&calls, &native);
        assert!(mutations.lock().unwrap().is_empty());
        assert!(calls.authority.lock().unwrap().is_empty());
        assert_eq!(outcome.confinement, StartupConfinementProgress::default());
        assert_eq!(
            outcome.preparation,
            StartupPreparationOutcome::Incomplete {
                progress: one_candidate_progress(),
                reason: StartupIncompleteReason::RecoveryRefused
            }
        );
        eprintln!("B4_STARTUP_SUPPLEMENT_INTENDED_CUSTOM_CAPTURE_ASSERTION normal_retirement mode={mode:?}");
        assert_eq!(
            captures.lock().unwrap().as_slice(),
            &[RecoveryConfinementTarget::from_view(&original[0].view).unwrap()]
        );
        assert_eq!(
            calls.resolved.lock().unwrap().as_slice(),
            &[inherited::origin()]
        );
        assert!(calls.policy.lock().unwrap().is_empty());
    }
}
fn assert_original_invocation(original: &State, actual: &State, native: &NativeCalls, owner: u64) {
    let calls = native.original.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let (invocation, context) = &calls[0];
    let snapshot = &original.approval.execution.snapshot;
    assert_eq!(invocation.execution_id, snapshot.execution_id);
    assert_eq!(invocation.owner_generation, owner);
    assert_eq!(invocation.task_id, snapshot.definition.batches[0][0].id);
    assert_eq!(
        invocation.effect_id,
        snapshot.definition.batches[0][0].effect_id
    );
    assert_eq!(invocation.input, snapshot.values);
    assert_eq!(context.thread_id, original.view.thread.id);
    assert_eq!(context.thread_generation, original.view.thread.generation);
    assert_eq!(context.origin, original.view.thread.origin.clone().unwrap());
    assert_eq!(context.execution_revision, snapshot.definition.revision);
    assert_eq!(context.application_revision, "supplement-current-native");
    assert_eq!(context.policy_revision, "supplement-current-policy");
    assert_eq!(native.newer.load(Ordering::Acquire), 0);
    let selected = &actual.approval.execution.snapshot;
    assert_eq!(selected.definition, snapshot.definition);
    assert_eq!(selected.execution_id, snapshot.execution_id);
    assert_eq!(selected.values, snapshot.values);
    assert_eq!(selected.status, ExecutionStatus::Completed);
    assert_eq!(selected.tasks[0].spec, snapshot.definition.batches[0][0]);
    assert_eq!(
        selected.tasks[0].result.as_ref().unwrap().output,
        serde_json::json!("original-typed-recovery")
    );
    assert_eq!(actual.approval.grants, original.approval.grants);
    assert_eq!(actual.approval.acceptances, original.approval.acceptances);
    assert_eq!(
        &actual.journal[..original.journal.len()],
        original.journal.as_slice()
    );
}
pub async fn run_available_allowed() {
    let gate = super::gate::Gate::new();
    let _cleanup = super::gate::Cleanup(gate.clone());
    let fixture = ApiFixture::new(Supplement::HistoricalAllowed, gate.clone());
    let originals = fixture.originals.clone();
    let owner = fixture.owner;
    let calls = fixture.calls.clone();
    let native = fixture.native.clone();
    let mutations = fixture.mutations.clone();
    let handle = fixture.start(StartupPreparationLimits::default()).await;
    let outcome = supplement_finished(&handle).await;
    gate.entered().await;
    let owned_while_serving = fixture.shutdown.status().active_owners > 0;
    let held_state = fixture.states();
    gate.release();
    fixture.settled(&originals[0].view.thread.id).await;
    let actual = fixture.retire().await;
    gate.positive();
    assert!(owned_while_serving);
    assert_eq!(
        outcome.preparation,
        StartupPreparationOutcome::Complete {
            progress: StartupProgress {
                admitted: 1,
                runnable: 1,
                ..one_candidate_progress()
            }
        }
    );
    assert_eq!(outcome.confinement, StartupConfinementProgress::default());
    assert!(mutations.lock().unwrap().is_empty());
    assert!(calls.authority.lock().unwrap().is_empty());
    assert_eq!(
        held_state[0].approval.execution.snapshot.definition,
        originals[0].approval.execution.snapshot.definition
    );
    assert_original_invocation(&originals[0], &actual[0], &native, owner);
    assert_eq!(calls.validators.load(Ordering::Acquire), 0);
    for (actor, context) in calls.policy.lock().unwrap().iter() {
        assert_eq!(actor, &inherited::principal());
        assert!(matches!(context.operation, Operation::Recover));
        for route in [
            context.requested_route.as_ref().unwrap(),
            context.stored_route.as_ref().unwrap(),
        ] {
            assert_eq!(
                route.route,
                originals[0].approval.execution.snapshot.definition.route
            );
            assert_eq!(
                route.revision,
                originals[0].approval.execution.snapshot.definition.revision
            );
        }
    }
    assert!(calls
        .resolved
        .lock()
        .unwrap()
        .iter()
        .all(|origin| origin == &inherited::origin()));
    eprintln!("B4_STARTUP_SUPPLEMENT_AVAILABLE_ORIGINAL_RECOVERY normal_callback_return normal_retirement");
}
fn expected_many_blocked(
    originals: &[State],
    actual: &[State],
    count: usize,
    owner: u64,
    started: u64,
    ended: u64,
) -> RawTables {
    assert_eq!(originals.len(), actual.len());
    let mut raw = originals[0].raw.clone();
    for (index, (original, observed)) in originals.iter().zip(actual).enumerate() {
        if index >= count {
            assert_eq!(observed.view, original.view);
            assert_eq!(observed.approval, original.approval);
            assert_eq!(observed.journal, original.journal);
            continue;
        }
        // Reuse the independent one-record kernel oracle for pure data; the
        // whole-database expectation starts from all original rows, below.
        let mut one_original = original.clone();
        let thread_id = original.view.thread.id.as_str();
        let execution_id = original.approval.execution.snapshot.execution_id.as_str();
        table(&mut one_original.raw, "threads")
            .retain(|row| row[0] == SqlValue::Text(thread_id.into()));
        table(&mut one_original.raw, "executions")
            .retain(|row| row[0] == SqlValue::Text(execution_id.into()));
        table(&mut one_original.raw, "journal")
            .retain(|row| row[3] == SqlValue::Text(execution_id.into()));
        let expected = expected_confined(
            &one_original,
            observed,
            owner,
            PositiveCase::MissingDefinition,
            started,
            ended,
        );
        assert_eq!(observed.view, expected.view);
        assert_eq!(observed.approval, expected.approval);
        assert_eq!(observed.journal, expected.journal);
        let thread = table(&mut raw, "threads")
            .iter_mut()
            .find(|row| row[0] == SqlValue::Text(thread_id.into()))
            .unwrap();
        thread[5] = blob(expected.view.thread.resource_revision);
        let execution = table(&mut raw, "executions")
            .iter_mut()
            .find(|row| row[0] == SqlValue::Text(execution_id.into()))
            .unwrap();
        execution[2] = blob(expected.approval.execution.snapshot.revision);
        execution[3] = blob(owner);
        execution[4] =
            SqlValue::Text(serde_json::to_string(&expected.approval.execution.snapshot).unwrap());
        let journal = table(&mut raw, "journal");
        let SqlValue::Integer(last) = journal.last().unwrap()[0] else {
            panic!("native original journal sequence")
        };
        let mut entry = table(&mut expected.raw.clone(), "journal")
            .last()
            .unwrap()
            .clone();
        entry[0] = SqlValue::Integer(last.checked_add(1).unwrap());
        journal.push(entry);
    }
    raw
}
pub async fn run_scan_limit() {
    let gate = super::gate::Gate::new();
    let _cleanup = super::gate::Cleanup(gate.clone());
    let fixture = ApiFixture::new(Supplement::MissingThree, gate);
    let originals = fixture.originals.clone();
    let owner = fixture.owner;
    let calls = fixture.calls.clone();
    let native = fixture.native.clone();
    let captures = fixture.captures.clone();
    let mutations = fixture.mutations.clone();
    let started = now_ms();
    let handle = fixture
        .start(StartupPreparationLimits {
            max_pages: 64,
            max_recoveries: 2,
        })
        .await;
    let outcome = supplement_finished(&handle).await;
    let ended = now_ms();
    let actual = fixture.retire().await;
    assert_no_dispatch(&calls, &native);
    if outcome.confinement.confirmed_blocks == 0 {
        assert_eq!(actual, originals);
        assert!(mutations.lock().unwrap().is_empty());
    }
    eprintln!("B4_STARTUP_SUPPLEMENT_INTENDED_SCAN_LIMIT_ASSERTION normal_retirement");
    assert_eq!(
        outcome.preparation,
        StartupPreparationOutcome::Incomplete {
            progress: StartupProgress {
                pages: 1,
                scanned_rows: 3,
                candidates: 2,
                ..StartupProgress::default()
            },
            reason: StartupIncompleteReason::LimitExceeded
        }
    );
    assert_eq!(
        outcome.confinement,
        StartupConfinementProgress {
            confirmed_blocks: 2,
            already_blocked: 0
        }
    );
    let expected_rows = expected_many_blocked(&originals, &actual, 2, owner, started, ended);
    for observed in &actual {
        assert_eq!(observed.raw, expected_rows);
    }
    let targets: Vec<_> = originals[..2]
        .iter()
        .map(|original| RecoveryConfinementTarget::from_view(&original.view).unwrap())
        .collect();
    assert_eq!(mutations.lock().unwrap().as_slice(), targets.as_slice());
    assert_eq!(
        captures.lock().unwrap().as_slice(),
        &[
            targets[0].clone(),
            targets[0].clone(),
            targets[1].clone(),
            targets[1].clone()
        ]
    );
    let expected_permissions: Vec<_> = originals[..2]
        .iter()
        .flat_map(|original| {
            [
                (
                    original.view.clone(),
                    PositiveCase::MissingDefinition.reason(),
                ),
                (
                    original.view.clone(),
                    PositiveCase::MissingDefinition.reason(),
                ),
            ]
        })
        .collect();
    assert_eq!(
        calls.authority.lock().unwrap().as_slice(),
        expected_permissions.as_slice()
    );
    assert_eq!(calls.resolved.lock().unwrap().len(), 6);
    assert!(calls.policy.lock().unwrap().is_empty());
}
pub async fn run_tcp_mixed() {
    let gate = super::gate::Gate::new();
    let _cleanup = super::gate::Cleanup(gate.clone());
    let fixture = ApiFixture::new(Supplement::Mixed, gate.clone());
    let originals = fixture.originals.clone();
    let owner = fixture.owner;
    let calls = fixture.calls.clone();
    let native = fixture.native.clone();
    let server = super::server::Server::start(fixture.host.as_ref().unwrap().router()).await;
    let before = tcp_statuses(&server, &originals[1].view.thread.id).await;
    let started = now_ms();
    let handle = fixture.start(StartupPreparationLimits::default()).await;
    let outcome = supplement_finished(&handle).await;
    gate.entered().await;
    let ended = now_ms();
    let after = tcp_statuses(&server, &originals[1].view.thread.id).await;
    let callback_owned = fixture.shutdown.status().active_owners > 0;
    let store_live = fixture.sqlite.upgrade().is_some();
    let app_live = fixture.application.upgrade().is_some();
    let writer_locked = matches!(
        SqliteStore::open(fixture.directory.path()),
        Err(StoreError::AlreadyOwned)
    );
    let held_states = fixture.states();
    gate.release();
    fixture.settled(&originals[1].view.thread.id).await;
    server.finish().await;
    let actual = fixture.retire().await;
    gate.positive();
    assert_eq!(before, [200, 503, 503]);
    assert_eq!(after, [200, 200, 200]);
    assert!(callback_owned && store_live && app_live && writer_locked);
    assert_eq!(
        outcome.preparation,
        StartupPreparationOutcome::Complete {
            progress: StartupProgress {
                pages: 1,
                scanned_rows: 2,
                candidates: 2,
                admitted: 1,
                runnable: 1,
                ..StartupProgress::default()
            }
        }
    );
    assert_eq!(
        outcome.confinement,
        StartupConfinementProgress {
            confirmed_blocks: 1,
            already_blocked: 0
        }
    );
    let mut expected_rows =
        expected_many_blocked(&originals[..1], &actual[..1], 1, owner, started, ended);
    assert_eq!(held_states[0].view, actual[0].view);
    assert_eq!(held_states[0].approval, actual[0].approval);
    assert_original_invocation(&originals[1], &actual[1], &native, owner);
    // The sibling follows the unchanged typed recovery path. Bind its raw
    // representation to its separately checked ordinary/approval/journal data;
    // never copy rows from the actual database into the expected database.
    let original = &originals[1];
    let recovered = &actual[1];
    let extra = &recovered.journal[original.journal.len()..];
    assert!(!extra.is_empty());
    let mut expected_thread = original.view.thread.clone();
    expected_thread.resource_revision = expected_thread
        .resource_revision
        .checked_add(extra.len().try_into().unwrap())
        .unwrap();
    assert_eq!(recovered.view.thread, expected_thread);
    let snapshot = &recovered.approval.execution.snapshot;
    assert_eq!(snapshot.owner_generation, owner);
    assert_eq!(
        snapshot.revision,
        original
            .approval
            .execution
            .snapshot
            .revision
            .checked_add(extra.len().try_into().unwrap())
            .unwrap()
    );
    let definition = &original.approval.execution.snapshot.definition;
    assert_eq!(definition.batches.len(), 1);
    assert_eq!(snapshot.tasks.len(), definition.batches[0].len());
    assert_eq!(snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert!(snapshot.failure.is_none());
    let thread_id = original.view.thread.id.as_str();
    let execution_id = snapshot.execution_id.as_str();
    table(&mut expected_rows, "threads")
        .iter_mut()
        .find(|row| row[0] == SqlValue::Text(thread_id.into()))
        .unwrap()[5] = blob(expected_thread.resource_revision);
    let row = table(&mut expected_rows, "executions")
        .iter_mut()
        .find(|row| row[0] == SqlValue::Text(execution_id.into()))
        .unwrap();
    row[2] = blob(snapshot.revision);
    row[3] = blob(owner);
    row[4] = SqlValue::Text(serde_json::to_string(snapshot).unwrap());
    for (index, entry) in extra.iter().enumerate() {
        assert_eq!(entry.execution_id, snapshot.execution_id);
        assert_eq!(entry.owner_generation, owner);
        assert_eq!(
            entry.revision,
            original.approval.execution.snapshot.revision + u64::try_from(index).unwrap() + 1
        );
        assert_eq!(
            entry.status,
            if index == 0 {
                ExecutionStatus::Accepted
            } else if index + 1 == extra.len() {
                ExecutionStatus::Completed
            } else {
                ExecutionStatus::Running
            }
        );
        let journal = table(&mut expected_rows, "journal");
        let SqlValue::Integer(last) = journal.last().unwrap()[0] else {
            panic!("native mixed journal sequence")
        };
        journal.push(vec![
            SqlValue::Integer(last.checked_add(1).unwrap()),
            SqlValue::Text("commit".into()),
            SqlValue::Text(thread_id.into()),
            SqlValue::Text(execution_id.into()),
            blob(entry.revision),
            blob(owner),
            SqlValue::Text(serde_json::to_string(&entry.status).unwrap()),
            blob(entry.updated_at_ms),
            SqlValue::Text(serde_json::to_string(entry).unwrap()),
        ]);
    }
    for observed in &actual {
        assert_eq!(observed.raw, expected_rows);
    }
    assert_eq!(calls.validators.load(Ordering::Acquire), 0);
    assert_eq!(
        calls.authority.lock().unwrap().as_slice(),
        &[
            (
                originals[0].view.clone(),
                PositiveCase::MissingDefinition.reason()
            ),
            (
                originals[0].view.clone(),
                PositiveCase::MissingDefinition.reason()
            )
        ]
    );
    eprintln!("B4_STARTUP_CONFINEMENT_TCP_MIXED normal_server_join original_callback_return healthy_weak_absence successor_closed");
}
