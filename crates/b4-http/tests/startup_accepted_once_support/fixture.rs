use super::{
    evidence::{
        self, capture, encode, ledger, read_json, verify_store, Directory, Writer, CAPTURE_BYTES,
        CHILD_BYTES, PARENT_BYTES,
    },
    gates::{actual_expiry, until, Cleanup, Clock, Gate, GateWitness},
    model::*,
    process::{Endpoint, OwnedChild, Reader},
    server::{self, Server},
    store::{AcceptReturn, AcceptStore},
    APPLICATION, ASSEMBLY, CHILD_LANE, CHILD_ROLE, CHILD_ROOT, CREDENTIAL, POLICY, THREAD, WAIT,
};
use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
const TTL_MS: u64 = 10_000;
const WITNESS_BYTES: usize = 65_536;

fn principal() -> Principal {
    Principal {
        application: APPLICATION.into(),
        tenant: "accepted-once-tenant".into(),
        principal: "accepted-once-owner".into(),
    }
}
fn origin() -> ThreadOrigin {
    let p = principal();
    ThreadOrigin {
        application: p.application,
        tenant: p.tenant,
        principal: p.principal,
    }
}
struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, h: &HeaderMap) -> std::result::Result<Principal, AuthenticationError> {
        if h.get("authorization").and_then(|v| v.to_str().ok()) == Some(CREDENTIAL) {
            Ok(principal())
        } else {
            Err(AuthenticationError)
        }
    }
}
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        p == &principal()
            && c.origin.as_ref().is_none_or(|o| o == &origin())
            && [c.stored_route.as_ref(), c.requested_route.as_ref()]
                .into_iter()
                .flatten()
                .all(|r| r.route.as_str() == Lane::Approval.route() && r.revision == revision())
    }
}
fn revision() -> ExecutionRevision {
    ExecutionRevision::new("accepted-once-definition-1").expect("fixed revision")
}
fn definition() -> Definition {
    Definition {
        route: RouteKey::new(Lane::Approval.route()).expect("fixed route"),
        revision: revision(),
        max_attempts: 4,
        batches: vec![vec![
            TaskSpec {
                id: TaskId::new("sibling").expect("fixed task"),
                effect_id: EffectId::new("sibling").expect("fixed effect"),
                requires_approval: false,
                recovery: RecoveryPolicy::Pure,
            },
            TaskSpec {
                id: TaskId::new("release").expect("fixed task"),
                effect_id: EffectId::new("release").expect("fixed effect"),
                requires_approval: true,
                recovery: RecoveryPolicy::Pure,
            },
        ]],
    }
}
fn input() -> Value {
    json!({"case":"accepted-once","from":"original-seed"})
}
fn task_result(task: &str) -> TaskResult {
    TaskResult {
        output: json!(task),
        writes: vec![StateWrite::Append {
            field: "joined".into(),
            items: vec![json!(task)],
        }],
    }
}
#[derive(Default)]
struct Stats {
    sibling: AtomicUsize,
    release: AtomicUsize,
    validators: AtomicUsize,
    release_returned: AtomicBool,
}
impl Stats {
    fn read(&self) -> Counts {
        Counts {
            sibling: self.sibling.load(Ordering::Acquire),
            release: self.release.load(Ordering::Acquire),
            validators: self.validators.load(Ordering::Acquire),
            ..Counts::default()
        }
    }
}
struct Validator(Arc<Stats>);
impl InputValidator for Validator {
    fn validate(&self, v: &Value) -> std::result::Result<(), InvalidApplicationInput> {
        self.0.validators.fetch_add(1, Ordering::AcqRel);
        if v == &input() {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}
struct Effect {
    root: PathBuf,
    role: Role,
    owner: u64,
    stats: Arc<Stats>,
    callback_gate: Arc<Gate>,
}
impl NativeEffect for Effect {
    fn invoke(
        &self,
        i: &EffectInvocation,
        c: &InvocationContext,
    ) -> std::result::Result<NativeOutcome, EffectUncertain> {
        let task = i.task_id.as_str();
        // Count actual entry even if a later context check refuses. A fresh
        // seed observation must not conceal an unexpected release invocation.
        if task == "sibling" {
            self.stats.sibling.fetch_add(1, Ordering::AcqRel);
        } else if task == "release" {
            self.stats.release.fetch_add(1, Ordering::AcqRel);
        }
        if !matches!(task, "sibling" | "release")
            || c.thread_id.as_str() != THREAD
            || c.origin != origin()
            || c.application_revision != ASSEMBLY
            || c.execution_revision != revision()
            || c.policy_revision != POLICY
            || i.owner_generation != self.owner
            || i.effect_id.as_str() != task
            || i.input.get("input") != Some(&input())
        {
            return Err(EffectUncertain);
        }
        if task == "release" {
            // The synchronous callback is entered only after actual Dispatch.
            // Seed invocation or an expired/cleanup gate is an invalid fixture.
            if self.role != Role::Restart || !self.callback_gate.hold() {
                return Err(EffectUncertain);
            }
        }
        let path = self.root.join("effects.ledger");
        evidence::verify_file(&path).map_err(|_| EffectUncertain)?;
        let bytes = format!("{task}\n");
        if std::fs::metadata(&path)
            .map_err(|_| EffectUncertain)?
            .len()
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > 1024)
        {
            return Err(EffectUncertain);
        }
        let mut file = OpenOptions::new()
            .append(true)
            .mode(0o600)
            .open(path)
            .map_err(|_| EffectUncertain)?;
        file.write_all(bytes.as_bytes())
            .map_err(|_| EffectUncertain)?;
        file.sync_all().map_err(|_| EffectUncertain)?;
        let normal = Ok(NativeOutcome::Succeeded(task_result(task)));
        if task == "release" {
            self.stats.release_returned.store(true, Ordering::Release);
        }
        normal
    }
}
fn application(
    root: &Path,
    role: Role,
    owner: u64,
    stats: Arc<Stats>,
    callback_gate: Arc<Gate>,
) -> Result<Arc<NativeApplication>> {
    let callback: Arc<dyn NativeEffect> = Arc::new(Effect {
        root: root.to_owned(),
        role,
        owner,
        stats: stats.clone(),
        callback_gate,
    });
    let d = definition();
    let effects = d
        .batches
        .iter()
        .flatten()
        .map(|t| {
            (
                t.effect_id.clone(),
                EffectBinding {
                    callback: callback.clone(),
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    NativeApplication::new(
        ApplicationIdentity {
            application: APPLICATION.into(),
            revision: ASSEMBLY.into(),
        },
        vec![RouteRegistration {
            definition: d,
            effects,
            input_validator: Some(Arc::new(Validator(stats))),
        }],
    )
    .map(Arc::new)
    .map_err(|_| Failure::Setup)
}
fn config(app: Arc<NativeApplication>) -> NativeHostConfig {
    let prompts = BTreeMap::from([(
        definition().route,
        BTreeMap::from([(
            TaskId::new("release").expect("fixed task"),
            json!({"question":"release once"}),
        )]),
    )]);
    let mut c = NativeHostConfig::new(app, ApprovalHostConfig::new(POLICY.into(), prompts));
    c.approval.ttl_ms = TTL_MS;
    c.limits.max_active_runs = 4;
    c.limits.max_concurrent_tasks = 2;
    c.limits.max_tasks_per_execution = 2;
    c.limits.heartbeat_ms = 1000;
    c
}
struct Resolver {
    gate: Arc<Gate>,
}
impl StartupPrincipalResolver for Resolver {
    fn resolve(&self, o: &ThreadOrigin) -> std::result::Result<Principal, StartupResolutionError> {
        if o != &origin() {
            return Err(StartupResolutionError::Unauthorized);
        }
        if self.gate.hold() {
            Ok(principal())
        } else {
            Err(StartupResolutionError::Unavailable)
        }
    }
}
enum Host {
    Seed(HttpHost<AcceptStore>),
    Restart(PreparingHttpHost<SqliteStore>),
}
impl Host {
    fn router(&self) -> axum::Router {
        match self {
            Self::Seed(h) => h.router(),
            Self::Restart(h) => h.router(),
        }
    }
    fn shutdown(&self) -> ShutdownHandle {
        match self {
            Self::Seed(h) => h.shutdown_handle(),
            Self::Restart(h) => h.shutdown_handle(),
        }
    }
    fn status(&self) -> Option<StartupPreparationStatus> {
        match self {
            Self::Seed(_) => None,
            Self::Restart(h) => Some(h.preparation_status()),
        }
    }
}
struct ChildState {
    root: PathBuf,
    role: Role,
    owner: u64,
    store: Arc<SqliteStore>,
    wrapped: Option<Arc<AcceptStore>>,
    app: Arc<NativeApplication>,
    host: Host,
    server: Server,
    writer: Writer,
    stats: Arc<Stats>,
    main_gate: Arc<Gate>,
    callback_gate: Arc<Gate>,
    handle: Option<StartupPreparationHandle>,
    parked: bool,
    checkpointed: bool,
    complete: bool,
    dispatched: bool,
    final_captured: bool,
}
impl ChildState {
    async fn new(root: PathBuf, role: Role) -> Result<Self> {
        evidence::verify_directory(&root)?;
        verify_store(&root)?;
        let store = Arc::new(SqliteStore::open(root.join("data")).map_err(|_| Failure::Setup)?);
        verify_store(&root)?;
        let owner = store.owner_generation();
        if owner == 0 {
            return Err(Failure::Setup);
        }
        let writer = Writer::new(root.join("evidence").join(role.text()), CHILD_BYTES)?;
        writer.persist("initial-clock.json", &Clock::sample()?, 4096)?;
        let main_gate = Gate::new();
        let callback_gate = Gate::new();
        let stats = Arc::new(Stats::default());
        let app = application(&root, role, owner, stats.clone(), callback_gate.clone())?;
        let wrapped = if role == Role::Seed {
            Some(AcceptStore::new(store.clone(), main_gate.clone()))
        } else {
            None
        };
        let host = match role {
            Role::Seed => Host::Seed(
                HttpHost::new_with_application(
                    wrapped.as_ref().ok_or(Failure::Setup)?.clone(),
                    Arc::new(Auth),
                    Arc::new(Policy),
                    config(app.clone()),
                )
                .map_err(|_| Failure::Setup)?,
            ),
            Role::Restart => Host::Restart(
                HttpHost::new_with_application_preparing(
                    store.clone(),
                    Arc::new(Auth),
                    Arc::new(Policy),
                    config(app.clone()),
                )
                .map_err(|_| Failure::Setup)?,
            ),
        };
        let server = Server::start(host.router()).await?;
        Ok(Self {
            root,
            role,
            owner,
            store,
            wrapped,
            app,
            host,
            server,
            writer,
            stats,
            main_gate,
            callback_gate,
            handle: None,
            parked: false,
            checkpointed: false,
            complete: false,
            dispatched: false,
            final_captured: false,
        })
    }
    fn reply(&self, ordinal: u8, captured: Option<CaptureName>) -> Result<Reply> {
        Ok(Reply::from_status(
            ordinal,
            self.host.status(),
            self.main_gate.facts()?,
            captured,
            false,
        ))
    }
    fn save(&self, name: CaptureName) -> Result<Capture> {
        let c = capture(&self.store, Lane::Approval, self.role, self.stats.read())?;
        self.writer.persist(name.file(), &c, CAPTURE_BYTES)?;
        Ok(c)
    }
    async fn command(&mut self, r: &Request) -> Result<Reply> {
        match r.command {
            Command::Checkpoint {} if self.role == Role::Seed && self.checkpointed => {
                // Source evidence is already sealed. This is a fresh PURE cut
                // observation: no new seed file, capture, or mutable Store call.
                require(
                    held(self.main_gate.facts()?)
                        && self.stats.read()
                            == Counts {
                                sibling: 1,
                                validators: 1,
                                ..Counts::default()
                            }
                        && !self.stats.release_returned.load(Ordering::Acquire),
                )?;
                self.reply(r.ordinal, None)
            }
            Command::Capture {} if self.role == Role::Seed && !self.parked => {
                until(|| {
                    self.store
                        .thread_view(&ThreadId::new(THREAD).expect("fixed thread"))
                        .ok()
                        .flatten()
                        .and_then(|v| v.execution)
                        .is_some_and(|e| e.snapshot.status == ExecutionStatus::WaitingForApproval)
                })
                .await?;
                self.save(CaptureName::Parked)?;
                self.parked = true;
                self.reply(r.ordinal, Some(CaptureName::Parked))
            }
            Command::Checkpoint {}
                if self.role == Role::Seed && self.parked && !self.checkpointed =>
            {
                self.main_gate.entered().await?;
                let observation = self.wrapped.as_ref().ok_or(Failure::Setup)?.observation()?;
                let source = capture(&self.store, Lane::Approval, self.role, self.stats.read())?;
                require(source.ordinary.execution.as_ref() == Some(&observation.execution))?;
                self.writer
                    .persist("accept-return.json", &observation, CAPTURE_BYTES)?;
                self.writer
                    .persist("accept-gate.json", &self.main_gate.witness()?, 4096)?;
                self.writer
                    .persist(CaptureName::Source.file(), &source, CAPTURE_BYTES)?;
                // The child Reader carries pure IPC only; reply artifacts and
                // diagnostic log drains belong to the independent parent Writer.
                self.writer.seal()?;
                self.checkpointed = true;
                self.reply(r.ordinal, Some(CaptureName::Source))
            }
            Command::Begin {} if self.role == Role::Restart && self.handle.is_none() => {
                let Host::Restart(host) = &self.host else {
                    return Err(Failure::Protocol);
                };
                let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver {
                    gate: self.main_gate.clone(),
                });
                self.handle = Some(
                    host.begin_preparation(
                        resolver,
                        StartupPreparationLimits {
                            max_pages: 1,
                            max_recoveries: 1,
                        },
                    )
                    .map_err(|_| Failure::Setup)?,
                );
                self.main_gate.entered().await?;
                self.reply(r.ordinal, None)
            }
            Command::ReleaseResolver {} if self.role == Role::Restart && self.handle.is_some() => {
                self.writer
                    .persist("resolver-release-clock.json", &Clock::sample()?, 4096)?;
                self.main_gate.release()?;
                self.main_gate.returned().await?;
                self.reply(r.ordinal, None)
            }
            Command::AwaitComplete {}
                if self.role == Role::Restart
                    && self.main_gate.facts()?.positive()
                    && !self.complete =>
            {
                let outcome = self
                    .handle
                    .as_ref()
                    .ok_or(Failure::Protocol)?
                    .wait(WAIT)
                    .await
                    .map_err(|_| Failure::Setup)?;
                if !matches!(
                    outcome,
                    StartupWaitOutcome::Finished(StartupPreparationOutcome::Complete { .. })
                ) {
                    return Err(Failure::Semantics);
                }
                require(self.host.status().is_some_and(|s| {
                    s.phase == StartupPreparationPhase::Complete && s.incomplete_reason.is_none()
                }))?;
                self.callback_gate.entered().await?;
                self.complete = true;
                self.reply(r.ordinal, None)
            }
            Command::Capture {}
                if self.role == Role::Restart && self.complete && !self.dispatched =>
            {
                self.writer.persist(
                    "callback-entered.json",
                    &self.callback_gate.witness()?,
                    4096,
                )?;
                self.save(CaptureName::Dispatched)?;
                self.writer
                    .persist("dispatched-clock.json", &Clock::sample()?, 4096)?;
                self.dispatched = true;
                self.reply(r.ordinal, Some(CaptureName::Dispatched))
            }
            Command::ReleaseCallback {} if self.role == Role::Restart && self.dispatched => {
                self.writer
                    .persist("callback-release-clock.json", &Clock::sample()?, 4096)?;
                self.callback_gate.release()?;
                self.callback_gate.returned().await?;
                self.reply(r.ordinal, None)
            }
            Command::AwaitTerminal {}
                if self.role == Role::Restart
                    && self.dispatched
                    && self.callback_gate.facts()?.positive()
                    && !self.final_captured =>
            {
                until(|| {
                    self.stats.release_returned.load(Ordering::Acquire)
                        && self
                            .store
                            .thread_view(&ThreadId::new(THREAD).expect("fixed thread"))
                            .ok()
                            .flatten()
                            .and_then(|v| v.execution)
                            .is_some_and(|e| e.snapshot.status == ExecutionStatus::Completed)
                })
                .await?;
                self.save(CaptureName::Final)?;
                self.writer
                    .persist("final-clock.json", &Clock::sample()?, 4096)?;
                self.final_captured = true;
                self.reply(r.ordinal, Some(CaptureName::Final))
            }
            _ => Err(Failure::Protocol),
        }
    }
    async fn retire(self) -> Result<Retirement> {
        tokio::time::timeout(WAIT, async {
            let Self {
                root,
                role,
                owner,
                store,
                wrapped,
                app,
                host,
                server,
                writer,
                stats,
                main_gate,
                callback_gate,
                handle,
                parked: _,
                checkpointed: _,
                complete: _,
                dispatched: _,
                final_captured,
            } = self;
            if role != Role::Restart || !final_captured || wrapped.is_some() {
                return Err(Failure::Protocol);
            }
            let status = host.status().ok_or(Failure::Protocol)?;
            let shutdown = host.shutdown();
            let weak_store = Arc::downgrade(&store);
            let weak_app = Arc::downgrade(&app);
            shutdown.begin_shutdown();
            let (quiescent, faulted) = match shutdown
                .wait_quiescent(WAIT)
                .await
                .map_err(|_| Failure::Cleanup)?
            {
                ShutdownOutcome::Quiescent { faulted } => (true, faulted),
                ShutdownOutcome::Incomplete { faulted, .. } => (false, faulted),
            };
            if !quiescent || faulted {
                return Err(Failure::Cleanup);
            }
            server.finish().await?;
            drop(host);
            drop(wrapped);
            drop(app);
            drop(store);
            drop(shutdown);
            until(|| weak_store.upgrade().is_none() && weak_app.upgrade().is_none()).await?;
            verify_store(&root)?;
            let observed = handle.as_ref().ok_or(Failure::Setup)?.status();
            require(observed == status)?;
            let callback = callback_gate.witness()?;
            require(callback.facts.positive() && stats.release_returned.load(Ordering::Acquire))?;
            let retirement = Retirement {
                version: 1,
                owner,
                quiescent,
                faulted,
                listener_finished: true,
                weak_store_absent: weak_store.upgrade().is_none(),
                weak_application_absent: weak_app.upgrade().is_none(),
                gate: main_gate.facts()?,
                phase: status.phase.into(),
                progress: status.progress.into(),
            };
            require(retirement.positive())?;
            writer.persist("resolver-retired.json", &main_gate.witness()?, 4096)?;
            writer.persist("callback-retired.json", &callback, 4096)?;
            writer.persist("retirement.json", &retirement, 4096)?;
            writer.seal()?;
            drop(handle);
            Ok(retirement)
        })
        .await
        .map_err(|_| Failure::Timeout)?
    }
}

pub async fn child_main() -> Result<()> {
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).ok_or(Failure::Setup)?);
    if Lane::parse(&std::env::var(CHILD_LANE).map_err(|_| Failure::Setup)?)? != Lane::Approval {
        return Err(Failure::Protocol);
    }
    let role = Role::parse(&std::env::var(CHILD_ROLE).map_err(|_| Failure::Setup)?)?;
    let state = ChildState::new(root.clone(), role).await?;
    let mut cleanup = Cleanup::new(vec![state.main_gate.clone(), state.callback_gate.clone()]);
    let hello = Hello {
        version: 1,
        ordinal: 0,
        lane: Lane::Approval,
        role,
        pid: std::process::id(),
        owner: state.owner,
        port: state.server.address.port(),
    };
    let mut reader = Reader::connect(&root, &hello).await?;
    let mut state = Some(state);
    loop {
        let pending = reader.next().await?;
        if matches!(pending.request.command, Command::Stop {}) {
            let retirement = state.take().ok_or(Failure::Protocol)?.retire().await?;
            pending
                .reply
                .send(Ok(Reply {
                    version: 1,
                    ordinal: pending.request.ordinal,
                    phase: retirement.phase,
                    progress: retirement.progress,
                    gate: retirement.gate,
                    capture: None,
                    retired: true,
                }))
                .map_err(|_| Failure::Protocol)?;
            reader.finish().await?;
            cleanup.disarm();
            return Ok(());
        }
        let reply = state
            .as_mut()
            .ok_or(Failure::Protocol)?
            .command(&pending.request)
            .await;
        let failed = reply.is_err();
        pending.reply.send(reply).map_err(|_| Failure::Protocol)?;
        if failed {
            return Err(Failure::Setup);
        }
    }
}

struct HttpEvidence {
    writer: Writer,
    next: u8,
}
impl HttpEvidence {
    async fn request(
        &mut self,
        port: u16,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<server::Response> {
        self.next = self
            .next
            .checked_add(1)
            .filter(|n| *n <= 16)
            .ok_or(Failure::Bound)?;
        let response = server::exchange(port, method, path, body).await?;
        self.writer.persist_bytes(
            &format!("http-{:02}-response.bin", self.next),
            &response.raw,
        )?;
        Ok(response)
    }
    async fn readiness(&mut self, port: u16, ready: bool) -> Result<bool> {
        let health = self.request(port, "GET", "/healthz", "").await?;
        let readiness = self.request(port, "GET", "/readyz", "").await?;
        let ordinary = self
            .request(port, "GET", &format!("/threads/{THREAD}"), "")
            .await?;
        if ready {
            Ok(health.status == 200
                && health.json()? == json!({"status":"ok"})
                && readiness.status == 200
                && readiness.json()? == json!({"status":"ready"})
                && ordinary.status == 200)
        } else {
            Ok(health.status == 200
                && health.json()? == json!({"status":"ok"})
                && readiness.status == 503
                && readiness.json()? == json!({"status":"unready"})
                && ordinary.status == 503
                && ordinary.json()?["error"]["details"]["code"] == "host_draining")
        }
    }
}
fn child_capture(root: &Path, role: Role, name: CaptureName) -> Result<Capture> {
    let c: Capture = read_json(
        &root.join("evidence").join(role.text()).join(name.file()),
        CAPTURE_BYTES,
    )?;
    if c.version != 1 || c.lane != Lane::Approval || c.role != role || !c.coherent() {
        return Err(Failure::Protocol);
    }
    Ok(c)
}
fn role_json<T: serde::de::DeserializeOwned>(
    root: &Path,
    role: Role,
    file: &str,
    cap: usize,
) -> Result<T> {
    read_json(&root.join("evidence").join(role.text()).join(file), cap)
}
fn inspect(root: &Path, writer: &Writer, name: &str) -> Result<Capture> {
    let store = Arc::new(SqliteStore::open(root.join("data")).map_err(|_| Failure::Setup)?);
    verify_store(root)?;
    let weak = Arc::downgrade(&store);
    let c = capture(&store, Lane::Approval, Role::Restart, Counts::default())?;
    writer.persist(name, &c, CAPTURE_BYTES)?;
    drop(store);
    if weak.upgrade().is_some() {
        return Err(Failure::Cleanup);
    }
    writer.persist(
        &format!("{name}-retired.json"),
        &json!({"owner":c.owner,"weak_store_absent":true,"readiness":true}),
        4096,
    )?;
    Ok(c)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cut {
    intended_at_ms: u64,
    source_expires_at_ms: u64,
    owned_signal: u8,
    reaped: bool,
    received_bytes: usize,
    terminal_done_marker_present: bool,
}

pub async fn run() -> Result<()> {
    let directory = Directory::new()?;
    let root = &directory.root;
    let writer = directory.parent.clone();
    let mut http = HttpEvidence {
        writer: writer.clone(),
        next: 0,
    };
    let endpoint = Endpoint::bind(root)?;
    let mut seed = OwnedChild::start(root, Lane::Approval, Role::Seed, writer.clone())?;
    let mut seed_control = endpoint
        .accept(&seed, Lane::Approval, Role::Seed, writer.clone())
        .await?;
    let seed_owner = seed_control.hello()?.owner;
    let seed_port = seed_control.hello()?.port;
    let run_body = encode(
        &json!({"route":Lane::Approval.route(),"input":input()}),
        WITNESS_BYTES,
    )?;
    let response = http
        .request(
            seed_port,
            "POST",
            &format!("/threads/{THREAD}/runs/stream"),
            std::str::from_utf8(&run_body).map_err(|_| Failure::Protocol)?,
        )
        .await?;
    require(response.status == 200 && response.done()? == json!({"output":null}))?;
    let parked_reply = seed_control.command(Command::Capture {}).await?;
    require(parked_reply.capture == Some(CaptureName::Parked))?;
    let parked = child_capture(root, Role::Seed, CaptureName::Parked)?;
    check_parked(&parked, &ledger(root)?)?;
    let pending = http
        .request(
            seed_port,
            "GET",
            &format!("/threads/{THREAD}/pending_interrupts"),
            "",
        )
        .await?;
    let grant = parked.authority.grants.first().ok_or(Failure::Setup)?;
    let expected_pending = json!({"interrupts":[{"interruptId":grant.interrupt_id,"resumeKey":grant.resume_key,"value":grant.prompt,"grant":grant.token.as_str()}]});
    require(pending.status == 200 && pending.json()? == expected_pending)?;
    let resume = encode(
        &json!({"route":Lane::Approval.route(),"resume":[{"status":"resolved","interruptId":grant.interrupt_id,"payload":"once","grant":grant.token.as_str()}]}),
        WITNESS_BYTES,
    )?;
    writer.persist_bytes("original-resume-request.json", &resume)?;
    let request_started = Clock::sample()?;
    if request_started.observed_at_ms >= grant.expires_at_ms {
        return Err(Failure::Setup);
    }
    writer.persist("original-resume-start-clock.json", &request_started, 4096)?;
    let original_resume = server::open(
        seed_port,
        "POST",
        &format!("/threads/{THREAD}/resume"),
        std::str::from_utf8(&resume).map_err(|_| Failure::Protocol)?,
    )
    .await?;
    let checkpoint_started = Instant::now();
    let checkpoint = seed_control.command(Command::Checkpoint {}).await?;
    let source = child_capture(root, Role::Seed, CaptureName::Source)?;
    let returned: AcceptReturn = role_json(root, Role::Seed, "accept-return.json", CAPTURE_BYTES)?;
    let accept_gate: GateWitness = role_json(root, Role::Seed, "accept-gate.json", 4096)?;
    let seed_initial: Clock = role_json(root, Role::Seed, "initial-clock.json", 4096)?;
    require(
        checkpoint.capture == Some(CaptureName::Source)
            && held(checkpoint.gate)
            && held(accept_gate.facts),
    )?;
    check_source(&parked, &source, &returned, request_started.observed_at_ms)?;
    require(ledger(root)? == ["sibling"] && seed_owner == 1 && source.owner == seed_owner)?;
    // Existing Checkpoint is reused as a post-seal pure observation. Its child
    // branch checks current release-entry count0 and actual still-held facts.
    // Request/reply persistence completes under the PARENT Writer before cut.
    let fresh_cut = seed_control.command(Command::Checkpoint {}).await?;
    require(held(fresh_cut.gate) && fresh_cut.capture.is_none() && !fresh_cut.retired)?;
    let before_kill = Clock::sample()?;
    if !(before_kill.observed_at_ms >= returned.returned_at_ms.ok_or(Failure::Setup)?
        && before_kill.observed_at_ms < grant.expires_at_ms
        && seed_initial.observed_at_ms <= request_started.observed_at_ms
        && accept_gate.entered_at_ms.is_some_and(|t| {
            t >= returned.returned_at_ms.unwrap_or(u64::MAX) && t <= before_kill.observed_at_ms
        }))
    {
        return Err(Failure::Setup);
    }
    // This elapsed interval starts BEFORE the child samples remaining lifetime.
    // It conservatively includes IPC, capture, seal and all parent comparisons.
    if checkpoint_started.elapsed().as_millis() >= u128::from(accept_gate.remaining_ms) {
        return Err(Failure::Timeout);
    }
    // A validated committed intent and still-valid original grant cause this
    // intended signal. No timer or gate fallback supplies the crash trigger.
    seed.kill_and_reap().await?;
    // Persist the already sampled pre-kill fact AFTER the actual signal/reap;
    // no filesystem write delays the final validity check and intended kill.
    writer.persist("intended-cut-clock.json", &before_kill, 4096)?;
    drop(seed_control);
    let cut = server::crash_eof(original_resume).await?;
    writer.persist_bytes("original-resume-abrupt-cut.bin", &cut)?;
    let marker = b"event: done";
    let has_done = cut.windows(marker.len()).any(|w| w == marker);
    writer.persist(
        "original-resume-cut.json",
        &Cut {
            intended_at_ms: before_kill.observed_at_ms,
            source_expires_at_ms: grant.expires_at_ms,
            owned_signal: 9,
            reaped: true,
            received_bytes: cut.len(),
            terminal_done_marker_present: has_done,
        },
        4096,
    )?;
    drop(seed);
    Writer::verify_sealed(&root.join("evidence/seed"), CHILD_BYTES)?;
    let inspected = inspect(root, &writer, "post-kill-inspected.json")?;
    require(inspected.owner == 2 && inspected.same_durable(&source))?;
    let expiry = actual_expiry(grant.expires_at_ms).await?;
    writer.persist("original-grant-expiry.json", &expiry, 4096)?;
    let before_restart = Clock::sample()?;
    require(
        before_restart.observed_at_ms > grant.expires_at_ms
            && before_restart.observed_at_ms >= expiry.observed_at_ms,
    )?;
    writer.persist("pre-restart-clock.json", &before_restart, 4096)?;
    let endpoint = Endpoint::bind(root)?;
    let mut restarted = OwnedChild::start(root, Lane::Approval, Role::Restart, writer.clone())?;
    let mut control = endpoint
        .accept(&restarted, Lane::Approval, Role::Restart, writer.clone())
        .await?;
    let restart_owner = control.hello()?.owner;
    let port = control.hello()?.port;
    let restarted_initial: Clock = role_json(root, Role::Restart, "initial-clock.json", 4096)?;
    let mut semantics = !has_done
        && restart_owner == 3
        && restarted_initial.observed_at_ms > grant.expires_at_ms
        && restarted_initial.observed_at_ms >= before_restart.observed_at_ms;
    semantics &= http.readiness(port, false).await?;
    let entered = control.command(Command::Begin {}).await?;
    semantics &= entered.phase == Phase::Preparing && held(entered.gate);
    semantics &= http.readiness(port, false).await?;
    let recovery_started = Clock::sample()?;
    writer.persist("pre-resolver-release-clock.json", &recovery_started, 4096)?;
    let released = control.command(Command::ReleaseResolver {}).await?;
    semantics &= released.gate.positive();
    let complete = control.command(Command::AwaitComplete {}).await?;
    semantics &=
        complete.phase == Phase::Complete && complete.gate.positive() && complete.capture.is_none();
    semantics &= http.readiness(port, true).await?;
    let captured = control.command(Command::Capture {}).await?;
    semantics &= captured.capture == Some(CaptureName::Dispatched);
    let dispatched = child_capture(root, Role::Restart, CaptureName::Dispatched)?;
    let dispatched_finished = Clock::sample()?;
    writer.persist("post-dispatched-clock.json", &dispatched_finished, 4096)?;
    let held_ledger = ledger(root)?;
    let callback_entered: GateWitness =
        role_json(root, Role::Restart, "callback-entered.json", 4096)?;
    let callback_started = Clock::sample()?;
    writer.persist("pre-callback-release-clock.json", &callback_started, 4096)?;
    let callback_released = control.command(Command::ReleaseCallback {}).await?;
    semantics &= callback_released.gate.positive();
    let terminal = control.command(Command::AwaitTerminal {}).await?;
    semantics &= terminal.capture == Some(CaptureName::Final);
    let final_capture = child_capture(root, Role::Restart, CaptureName::Final)?;
    let final_finished = Clock::sample()?;
    writer.persist("post-final-clock.json", &final_finished, 4096)?;
    let final_ledger = ledger(root)?;
    let stopped = control.command(Command::Stop {}).await?;
    semantics &= stopped.retired;
    drop(control);
    restarted.normal_exit_and_reap().await?;
    drop(restarted);
    let retirement: Retirement = role_json(root, Role::Restart, "retirement.json", 4096)?;
    let resolver_retired: GateWitness =
        role_json(root, Role::Restart, "resolver-retired.json", 4096)?;
    let callback_retired: GateWitness =
        role_json(root, Role::Restart, "callback-retired.json", 4096)?;
    let resolver_clock: Clock =
        role_json(root, Role::Restart, "resolver-release-clock.json", 4096)?;
    let callback_clock: Clock =
        role_json(root, Role::Restart, "callback-release-clock.json", 4096)?;
    let dispatched_clock: Clock = role_json(root, Role::Restart, "dispatched-clock.json", 4096)?;
    let final_clock: Clock = role_json(root, Role::Restart, "final-clock.json", 4096)?;
    Writer::verify_sealed(&root.join("evidence/restart"), CHILD_BYTES)?;
    let persisted = inspect(root, &writer, "post-retirement-inspected.json")?;
    semantics &= persisted.owner == 4
        && persisted.same_durable(&final_capture)
        && ledger(root)? == final_ledger;
    writer.seal()?;
    Writer::verify_sealed(&root.join("evidence/parent"), PARENT_BYTES)?;
    evidence::verify_aggregate(root)?;
    // These assertions follow healthy normal producer/server retirement, both
    // Weak absences, child exit/reap, actual next writer and full durable reread.
    require(
        semantics
            && retirement.positive()
            && retirement.owner == restart_owner
            && resolver_retired.facts.positive()
            && callback_retired.facts.positive(),
    )?;
    require(
        complete.progress == expected_progress() && retirement.progress == expected_progress(),
    )?;
    require(
        resolver_retired
            .entered_at_ms
            .is_some_and(|t| t <= resolver_clock.observed_at_ms)
            && resolver_retired.returned_at_ms.is_some_and(|t| {
                t >= resolver_clock.observed_at_ms && t <= dispatched_finished.observed_at_ms
            })
            && callback_retired.entered_at_ms == callback_entered.entered_at_ms
            && callback_retired.returned_at_ms.is_some_and(|t| {
                t >= callback_clock.observed_at_ms && t <= final_clock.observed_at_ms
            }),
    )?;
    require(
        held(callback_entered.facts)
            && callback_entered.entered_at_ms.is_some_and(|t| {
                t >= recovery_started.observed_at_ms && t <= dispatched_finished.observed_at_ms
            }),
    )?;
    require(
        resolver_clock.observed_at_ms >= recovery_started.observed_at_ms
            && dispatched_clock.observed_at_ms <= dispatched_finished.observed_at_ms
            && callback_clock.observed_at_ms >= callback_started.observed_at_ms
            && final_clock.observed_at_ms <= final_finished.observed_at_ms,
    )?;
    check_dispatched(
        &source,
        &dispatched,
        restart_owner,
        recovery_started.observed_at_ms,
        callback_entered.entered_at_ms.ok_or(Failure::Setup)?,
        dispatched_finished.observed_at_ms,
        &held_ledger,
    )?;
    check_final(
        &source,
        &dispatched,
        &final_capture,
        callback_started.observed_at_ms,
        final_finished.observed_at_ms,
        &final_ledger,
    )?;
    require(
        expiry.expires_at_ms == grant.expires_at_ms && expiry.observed_at_ms > grant.expires_at_ms,
    )
}
fn held(f: GateFacts) -> bool {
    f.entered && !f.explicit && !f.returned && !f.expired && !f.cleanup
}
fn expected_progress() -> Progress {
    Progress {
        pages: 1,
        scanned_rows: 1,
        candidates: 1,
        admitted: 1,
        runnable: 1,
        ..Progress::default()
    }
}
fn check_parked(c: &Capture, entries: &[String]) -> Result<()> {
    let s = c.snapshot()?;
    require(
        c.owner == 1
            && s.owner_generation == c.owner
            && c.ordinary.thread.id.as_str() == THREAD
            && c.ordinary.thread.origin.as_ref() == Some(&origin())
            && c.ordinary.thread.active_execution_id.as_ref() == Some(&s.execution_id),
    )?;
    require(
        s.definition == definition()
            && s.status == ExecutionStatus::WaitingForApproval
            && s.tasks.len() == 2
            && s.values.get("input") == Some(&input())
            && s.attempts_used == 1
            && s.attempt_history.len() == 1
            && s.output.is_none(),
    )?;
    require(
        s.tasks[0].status == TaskStatus::Succeeded
            && s.tasks[0].result.as_ref() == Some(&task_result("sibling"))
            && s.tasks[0].attempt_id.as_ref() == s.attempt_history.first()
            && s.tasks[1].status == TaskStatus::WaitingForApproval
            && !s.tasks[1].approved
            && s.tasks[1].attempt_id.is_none()
            && s.tasks[1].result.is_none(),
    )?;
    require(
        entries == ["sibling"]
            && c.counts
                == Counts {
                    sibling: 1,
                    validators: 1,
                    ..Counts::default()
                }
            && c.authority.grants.len() == 1
            && c.authority.acceptances.is_empty()
            && c.journal.last() == Some(&JournalEntry::from(s)),
    )?;
    let g = &c.authority.grants[0];
    require(
        !g.consumed
            && g.thread_id == c.ordinary.thread.id
            && g.thread_generation == c.ordinary.thread.generation
            && g.origin == origin()
            && g.execution_id == s.execution_id
            && g.route == s.definition.route
            && g.definition_revision == s.definition.revision
            && g.task_id == s.tasks[1].spec.id
            && g.effect_id == s.tasks[1].spec.effect_id
            && g.policy_revision == POLICY
            && g.expires_at_ms.checked_sub(g.issued_at_ms) == Some(TTL_MS)
            && s.tasks[1]
                .interrupt
                .as_ref()
                .is_some_and(|i| i.id == g.interrupt_id && i.prompt == g.prompt),
    )
}
fn updated(
    before: &Capture,
    snapshot: Snapshot,
    increments: u64,
) -> Result<(ThreadView, Authority)> {
    let mut expected = before.ordinary.clone();
    expected.thread.resource_revision = expected
        .thread
        .resource_revision
        .checked_add(increments)
        .ok_or(Failure::Bound)?;
    expected.execution.as_mut().ok_or(Failure::Setup)?.snapshot = snapshot;
    let mut authority = before.authority.clone();
    authority.thread = expected.thread.clone();
    authority.execution = expected.execution.clone().ok_or(Failure::Setup)?;
    Ok((expected, authority))
}
fn check_source(
    parked: &Capture,
    source: &Capture,
    returned: &AcceptReturn,
    started: u64,
) -> Result<()> {
    let old = parked.snapshot()?;
    let original = &parked.authority.grants[0];
    require(
        source.authority.grants.len() == 1
            && source.authority.acceptances.len() == 1
            && source.journal.len() == parked.journal.len() + 1
            && source.journal.starts_with(&parked.journal),
    )?;
    let acceptance = &source.authority.acceptances[0];
    let accepted = acceptance.accepted_at_ms;
    let returned_at = returned.returned_at_ms.ok_or(Failure::Setup)?;
    if !(accepted >= started
        && accepted >= old.updated_at_ms
        && accepted <= returned_at
        && accepted < original.expires_at_ms)
    {
        return Err(Failure::Setup);
    }
    require(!old.turn_history.contains(&acceptance.turn_id))?;
    let request = ApprovalAcceptanceRequest {
        context: ApprovalContext {
            actor: origin(),
            policy_revision: POLICY.into(),
        },
        route: old.definition.route.clone(),
        definition_revision: old.definition.revision.clone(),
        turn_id: acceptance.turn_id.clone(),
        now_ms: accepted,
        decisions: vec![GrantDecision {
            interrupt_id: original.interrupt_id.clone(),
            decision: ApprovalDecision::Once,
            cancelled: false,
            token: Some(original.token.clone()),
        }],
    };
    require(
        returned.request == request
            && source.ordinary.execution.as_ref() == Some(&returned.execution),
    )?;
    let expected_acceptance = ApprovalAcceptance {
        execution_id: old.execution_id.clone(),
        thread_id: parked.ordinary.thread.id.clone(),
        thread_generation: parked.ordinary.thread.generation.clone(),
        context: request.context,
        previous_revision: old.revision,
        turn_id: acceptance.turn_id.clone(),
        definition_revision: old.definition.revision.clone(),
        accepted_at_ms: accepted,
        decisions: vec![AcceptedDecision {
            task_id: original.task_id.clone(),
            interrupt_id: original.interrupt_id.clone(),
            decision: ApprovalDecision::Once,
            proposal_digest: original.proposal_digest.clone(),
            task_parking_revision: original.task_parking_revision,
        }],
    };
    let resumed = b4_kernel::transition(
        old,
        KernelCommand::Resume {
            route: old.definition.route.clone(),
            decisions: vec![Decision {
                interrupt_id: original.interrupt_id.clone(),
                decision: ApprovalDecision::Once,
            }],
            turn_id: acceptance.turn_id.clone(),
        },
        TransitionFacts { now_ms: accepted },
    )
    .map_err(|_| Failure::Semantics)?
    .snapshot;
    let (ordinary, mut authority) = updated(parked, resumed.clone(), 1)?;
    let mut consumed = original.clone();
    consumed.consumed = true;
    authority.grants = vec![consumed];
    authority.acceptances = vec![expected_acceptance];
    let mut journal = parked.journal.clone();
    journal.push(JournalEntry::from(&resumed));
    require(
        source.ordinary == ordinary
            && source.authority == authority
            && source.journal == journal
            && source.owner == parked.owner
            && source.counts == parked.counts
            && resumed.status == ExecutionStatus::Running
            && resumed.tasks[0] == old.tasks[0]
            && resumed.tasks[1].status == TaskStatus::Ready
            && resumed.tasks[1].approved
            && resumed.tasks[1].attempt_id.is_none()
            && resumed.tasks[1].result.is_none()
            && resumed.attempts_used == 1,
    )
}
fn check_dispatched(
    source: &Capture,
    after: &Capture,
    owner: u64,
    started: u64,
    entered: u64,
    finished: u64,
    entries: &[String],
) -> Result<()> {
    let old = source.snapshot()?;
    let actual = after.snapshot()?;
    require(
        after.journal.len() == source.journal.len() + 2
            && after.journal.starts_with(&source.journal)
            && !old.turn_history.contains(&actual.turn_id),
    )?;
    let j = &after.journal[source.journal.len()..];
    require(
        j.iter().all(|e| {
            e.updated_at_ms >= started && e.updated_at_ms <= entered && e.updated_at_ms <= finished
        }) && j[1].updated_at_ms >= j[0].updated_at_ms,
    )?;
    let task = actual.tasks.get(1).ok_or(Failure::Semantics)?;
    let attempt = task.attempt_id.clone().ok_or(Failure::Semantics)?;
    require(!old.attempt_history.contains(&attempt))?;
    let recovered = b4_kernel::transition(
        old,
        KernelCommand::Recover {
            available_revision: old.definition.revision.clone(),
            turn_id: actual.turn_id.clone(),
            owner_generation: owner,
        },
        TransitionFacts {
            now_ms: j[0].updated_at_ms,
        },
    )
    .map_err(|_| Failure::Semantics)?
    .snapshot;
    let dispatched = b4_kernel::transition(
        &recovered,
        KernelCommand::Dispatch {
            task_id: old.tasks[1].spec.id.clone(),
            attempt_id: attempt,
        },
        TransitionFacts {
            now_ms: j[1].updated_at_ms,
        },
    )
    .map_err(|_| Failure::Semantics)?
    .snapshot;
    let (ordinary, authority) = updated(source, dispatched.clone(), 2)?;
    require(
        after.ordinary == ordinary
            && after.authority == authority
            && after.owner == owner
            && j == [
                JournalEntry::from(&recovered),
                JournalEntry::from(&dispatched),
            ]
            && after.counts
                == Counts {
                    release: 1,
                    ..Counts::default()
                }
            && entries == ["sibling"]
            && recovered.attempts_used == 1
            && dispatched.attempts_used == 2
            && dispatched.tasks[0] == old.tasks[0]
            && dispatched.tasks[1].status == TaskStatus::InFlight
            && dispatched.tasks[1].approved
            && dispatched.tasks[1].result.is_none()
            && dispatched
                .turn_history
                .contains(&source.authority.acceptances[0].turn_id),
    )
}
fn check_final(
    source: &Capture,
    dispatched: &Capture,
    after: &Capture,
    started: u64,
    finished: u64,
    entries: &[String],
) -> Result<()> {
    let before = dispatched.snapshot()?;
    require(
        after.journal.len() == dispatched.journal.len() + 1
            && after.journal.starts_with(&dispatched.journal),
    )?;
    let tail = &after.journal[dispatched.journal.len()];
    require(
        tail.updated_at_ms >= started
            && tail.updated_at_ms >= before.updated_at_ms
            && tail.updated_at_ms <= finished,
    )?;
    let completed = b4_kernel::transition(
        before,
        KernelCommand::Complete {
            task_id: before.tasks[1].spec.id.clone(),
            attempt_id: before.tasks[1]
                .attempt_id
                .clone()
                .ok_or(Failure::Semantics)?,
            result: task_result("release"),
        },
        TransitionFacts {
            now_ms: tail.updated_at_ms,
        },
    )
    .map_err(|_| Failure::Semantics)?
    .snapshot;
    let (ordinary, authority) = updated(dispatched, completed.clone(), 1)?;
    require(
        after.ordinary == ordinary
            && after.authority == authority
            && tail == &JournalEntry::from(&completed)
            && after.owner == dispatched.owner
            && after.counts
                == Counts {
                    release: 1,
                    ..Counts::default()
                }
            && entries == ["sibling", "release"]
            && completed.status == ExecutionStatus::Completed
            && completed.definition == definition()
            && completed.attempts_used == 2
            && completed.output.as_ref() == Some(&Value::Object(completed.values.clone()))
            && completed.values["joined"] == json!(["sibling", "release"])
            && completed.tasks[0] == source.snapshot()?.tasks[0]
            && after.authority.grants == source.authority.grants
            && after.authority.acceptances == source.authority.acceptances,
    )
}
