use super::{
    evidence::{
        self, capture, encode, ledger, now_ms, read_json, verify_store, Directory, Writer,
        CAPTURE_BYTES, CHILD_BYTES, PARENT_BYTES,
    },
    model::*,
    process::{Endpoint, OwnedChild, Reader},
    server::{self, Server},
    APPLICATION, ASSEMBLY, CHILD_LANE, CHILD_ROLE, CHILD_ROOT, CREDENTIAL, GATE_WAIT,
    HEALTH_THREAD, POLICY, THREAD,
};
use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
const INNER_WAIT: Duration = Duration::from_secs(20);
const CLEANUP_WAIT: Duration = Duration::from_secs(25);

fn principal() -> Principal {
    Principal {
        application: APPLICATION.into(),
        tenant: "restart-fixture-tenant".into(),
        principal: "restart-fixture-owner".into(),
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
                .all(|r| {
                    [
                        Lane::Approval.route(),
                        Lane::Unsafe.route(),
                        "/restart-health#workflow",
                    ]
                    .contains(&r.route.as_str())
                        && r.revision == revision(r.route.as_str())
                })
    }
}
fn revision(route: &str) -> ExecutionRevision {
    ExecutionRevision::new(match route {
        "/restart-approval#workflow" => "restart-approval-1",
        "/restart-unsafe#workflow" => "restart-unsafe-1",
        _ => "restart-health-1",
    })
    .expect("fixed revision")
}
fn definition(route: &str, tasks: &[(&str, bool, RecoveryPolicy)]) -> Definition {
    Definition {
        route: RouteKey::new(route).expect("fixed route"),
        revision: revision(route),
        max_attempts: 4,
        batches: vec![tasks
            .iter()
            .map(|(id, approval, recovery)| TaskSpec {
                id: TaskId::new(*id).expect("fixed task"),
                effect_id: EffectId::new(*id).expect("fixed effect"),
                requires_approval: *approval,
                recovery: *recovery,
            })
            .collect()],
    }
}
fn result(task: &str) -> TaskResult {
    TaskResult {
        output: json!(task),
        writes: if task == "health" {
            vec![]
        } else {
            vec![StateWrite::Append {
                field: "joined".into(),
                items: vec![json!(task)],
            }]
        },
    }
}
fn input(lane: Lane) -> Value {
    json!({"case":lane.text(),"from":"original-seed"})
}
#[derive(Default)]
struct Stats {
    sibling: AtomicUsize,
    release: AtomicUsize,
    unsafe_effect: AtomicUsize,
    health: AtomicUsize,
    validators: AtomicUsize,
    unsafe_reached: AtomicBool,
}
impl Stats {
    fn read(&self) -> Counts {
        Counts {
            sibling: self.sibling.load(Ordering::Acquire),
            release: self.release.load(Ordering::Acquire),
            unsafe_effect: self.unsafe_effect.load(Ordering::Acquire),
            health: self.health.load(Ordering::Acquire),
            validators: self.validators.load(Ordering::Acquire),
        }
    }
}
struct Validator(Arc<Stats>);
impl InputValidator for Validator {
    fn validate(&self, v: &Value) -> std::result::Result<(), InvalidApplicationInput> {
        self.0.validators.fetch_add(1, Ordering::AcqRel);
        if v == &input(Lane::Approval)
            || v == &input(Lane::Unsafe)
            || v == &json!({"from":"restart-health"})
        {
            Ok(())
        } else {
            Err(InvalidApplicationInput)
        }
    }
}
struct LedgerEffect {
    root: PathBuf,
    lane: Lane,
    role: Role,
    owner: u64,
    stats: Arc<Stats>,
}
impl NativeEffect for LedgerEffect {
    fn invoke(
        &self,
        i: &EffectInvocation,
        c: &InvocationContext,
    ) -> std::result::Result<NativeOutcome, EffectUncertain> {
        let task = i.task_id.as_str();
        let route = match task {
            "sibling" | "release" => Lane::Approval.route(),
            "unsafe" => Lane::Unsafe.route(),
            "health" => "/restart-health#workflow",
            _ => return Err(EffectUncertain),
        };
        let correct_thread = if task == "health" {
            HEALTH_THREAD
        } else {
            THREAD
        };
        let expected_input = if task == "health" {
            json!({"from":"restart-health"})
        } else {
            input(self.lane)
        };
        if c.thread_id.as_str() != correct_thread
            || c.origin != origin()
            || c.application_revision != ASSEMBLY
            || c.execution_revision != revision(route)
            || c.policy_revision != POLICY
            || i.owner_generation != self.owner
            || i.effect_id.as_str() != task
            || i.input.get("input") != Some(&expected_input)
        {
            return Err(EffectUncertain);
        }
        let counter = match task {
            "sibling" => &self.stats.sibling,
            "release" => &self.stats.release,
            "unsafe" => &self.stats.unsafe_effect,
            _ => &self.stats.health,
        };
        counter.fetch_add(1, Ordering::AcqRel);
        if task != "health" {
            let path = self.root.join("effects.ledger");
            evidence::verify_file(&path).map_err(|_| EffectUncertain)?;
            let bytes = format!("{task}\n");
            let length = std::fs::metadata(&path).map_err(|_| EffectUncertain)?.len();
            if length
                .checked_add(bytes.len() as u64)
                .is_none_or(|n| n > 1024)
            {
                return Err(EffectUncertain);
            }
            let mut f = OpenOptions::new()
                .append(true)
                .mode(0o600)
                .open(path)
                .map_err(|_| EffectUncertain)?;
            f.write_all(bytes.as_bytes()).map_err(|_| EffectUncertain)?;
            f.sync_all().map_err(|_| EffectUncertain)?;
        }
        if task == "unsafe" {
            self.stats.unsafe_reached.store(true, Ordering::Release);
            if self.role != Role::Seed {
                return Err(EffectUncertain);
            }
            // No TaskResult is returned before the owned process is SIGKILLed.
            // Finite fallback is a failed fixture, never positive effect evidence.
            let held = Mutex::new(());
            let cv = Condvar::new();
            let guard = held.lock().map_err(|_| EffectUncertain)?;
            let mut guard = guard;
            let deadline = Instant::now() + GATE_WAIT;
            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                guard = cv
                    .wait_timeout(guard, remaining)
                    .map_err(|_| EffectUncertain)?
                    .0;
            }
            drop(guard);
            return Err(EffectUncertain);
        }
        Ok(NativeOutcome::Succeeded(result(task)))
    }
}
fn application(
    root: &Path,
    lane: Lane,
    role: Role,
    owner: u64,
    stats: Arc<Stats>,
) -> Result<Arc<NativeApplication>> {
    let callback: Arc<dyn NativeEffect> = Arc::new(LedgerEffect {
        root: root.to_owned(),
        lane,
        role,
        owner,
        stats: stats.clone(),
    });
    let definitions = [
        definition(
            Lane::Approval.route(),
            &[
                ("sibling", false, RecoveryPolicy::Pure),
                ("release", true, RecoveryPolicy::Pure),
            ],
        ),
        definition(
            Lane::Unsafe.route(),
            &[("unsafe", false, RecoveryPolicy::Unsafe)],
        ),
        definition(
            "/restart-health#workflow",
            &[("health", false, RecoveryPolicy::Pure)],
        ),
    ];
    let registrations = definitions
        .into_iter()
        .map(|d| {
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
            RouteRegistration {
                definition: d,
                effects,
                input_validator: Some(Arc::new(Validator(stats.clone()))),
            }
        })
        .collect();
    NativeApplication::new(
        ApplicationIdentity {
            application: APPLICATION.into(),
            revision: ASSEMBLY.into(),
        },
        registrations,
    )
    .map(Arc::new)
    .map_err(|_| Failure::Setup)
}
fn config(app: Arc<NativeApplication>) -> NativeHostConfig {
    let prompts = BTreeMap::from([(
        RouteKey::new(Lane::Approval.route()).expect("fixed route"),
        BTreeMap::from([(
            TaskId::new("release").expect("fixed task"),
            json!({"question":"release once"}),
        )]),
    )]);
    let mut c = NativeHostConfig::new(app, ApprovalHostConfig::new(POLICY.into(), prompts));
    c.approval.ttl_ms = 300_000;
    c.limits.max_active_runs = 4;
    c.limits.max_concurrent_tasks = 2;
    c.limits.max_tasks_per_execution = 2;
    c.limits.heartbeat_ms = 1000;
    c
}
struct Gate {
    state: Mutex<(GateFacts, Option<Instant>)>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((GateFacts::default(), None)),
            changed: Condvar::new(),
        })
    }
    fn hold(&self) -> bool {
        let Ok(mut s) = self.state.lock() else {
            return false;
        };
        if s.0.entered {
            return false;
        }
        s.0.entered = true;
        s.1 = Some(Instant::now() + GATE_WAIT);
        self.changed.notify_all();
        while !s.0.explicit && !s.0.cleanup && !s.0.expired {
            let remaining =
                s.1.expect("gate initialized")
                    .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                s.0.expired = true;
                break;
            }
            let Ok(next) = self.changed.wait_timeout(s, remaining) else {
                return false;
            };
            s = next.0;
        }
        if Instant::now() >= s.1.expect("gate initialized") {
            s.0.expired = true;
        }
        s.0.returned = true;
        self.changed.notify_all();
        s.0.positive()
    }
    fn facts(&self) -> Result<GateFacts> {
        self.state.lock().map(|s| s.0).map_err(|_| Failure::Setup)
    }
    fn release(&self) -> Result<()> {
        let mut s = self.state.lock().map_err(|_| Failure::Setup)?;
        if !s.0.entered || s.0.returned {
            return Err(Failure::Protocol);
        }
        s.0.explicit = true;
        self.changed.notify_all();
        Ok(())
    }
    fn cleanup(&self) {
        if let Ok(mut s) = self.state.lock() {
            if !s.0.explicit {
                s.0.cleanup = true;
            }
            self.changed.notify_all();
        }
    }
    async fn entered(&self) -> Result<()> {
        until(|| self.facts().is_ok_and(|f| f.entered)).await?;
        if self.facts()?.expired {
            Err(Failure::Timeout)
        } else {
            Ok(())
        }
    }
    async fn returned(&self) -> Result<()> {
        until(|| self.facts().is_ok_and(|f| f.returned)).await?;
        if self.facts()?.positive() {
            Ok(())
        } else {
            Err(Failure::Timeout)
        }
    }
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
async fn until(mut predicate: impl FnMut() -> bool) -> Result<()> {
    tokio::time::timeout(INNER_WAIT, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| Failure::Timeout)
}

enum Host {
    Seed(HttpHost<SqliteStore>),
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
    lane: Lane,
    role: Role,
    owner: u64,
    store: Arc<SqliteStore>,
    app: Arc<NativeApplication>,
    host: Host,
    server: Server,
    writer: Writer,
    stats: Arc<Stats>,
    gate: Arc<Gate>,
    handle: Option<StartupPreparationHandle>,
    checkpointed: bool,
    adopted: bool,
    final_captured: bool,
}
impl ChildState {
    async fn new(root: PathBuf, lane: Lane, role: Role) -> Result<Self> {
        evidence::verify_directory(&root)?;
        verify_store(&root)?;
        let store = Arc::new(SqliteStore::open(root.join("data")).map_err(|_| Failure::Setup)?);
        verify_store(&root)?;
        let owner = store.owner_generation();
        if owner == 0 {
            return Err(Failure::Setup);
        }
        let stats = Arc::new(Stats::default());
        let app = application(&root, lane, role, owner, stats.clone())?;
        let host = match role {
            Role::Seed => Host::Seed(
                HttpHost::new_with_application(
                    store.clone(),
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
        let writer = Writer::new(root.join("evidence").join(role.text()), CHILD_BYTES)?;
        Ok(Self {
            root,
            lane,
            role,
            owner,
            store,
            app,
            host,
            server,
            writer,
            stats,
            gate: Gate::new(),
            handle: None,
            checkpointed: false,
            adopted: false,
            final_captured: false,
        })
    }
    fn reply(&self, ordinal: u8, capture: Option<CaptureName>) -> Result<Reply> {
        Ok(Reply::from_status(
            ordinal,
            self.host.status(),
            self.gate.facts()?,
            capture,
            false,
        ))
    }
    fn save(&self, name: CaptureName) -> Result<()> {
        let c = capture(&self.store, self.lane, self.role, self.stats.read())?;
        self.writer.persist(name.file(), &c, CAPTURE_BYTES)
    }
    async fn command(&mut self, request: &Request) -> Result<Reply> {
        match request.command {
            Command::Checkpoint {} if self.role == Role::Seed && !self.checkpointed => {
                until(|| {
                    self.store
                        .thread_view(&ThreadId::new(THREAD).expect("fixed thread"))
                        .ok()
                        .flatten()
                        .and_then(|v| v.execution)
                        .is_some_and(|e| match self.lane {
                            Lane::Approval => {
                                e.snapshot.status == ExecutionStatus::WaitingForApproval
                            }
                            Lane::Unsafe => {
                                e.snapshot.status == ExecutionStatus::Running
                                    && e.snapshot
                                        .tasks
                                        .first()
                                        .is_some_and(|t| t.status == TaskStatus::InFlight)
                                    && self.stats.unsafe_reached.load(Ordering::Acquire)
                            }
                        })
                })
                .await?;
                self.save(CaptureName::Source)?;
                self.writer.seal()?;
                self.checkpointed = true;
                self.reply(request.ordinal, Some(CaptureName::Source))
            }
            Command::Begin {} if self.role == Role::Restart && self.handle.is_none() => {
                let Host::Restart(host) = &self.host else {
                    return Err(Failure::Protocol);
                };
                let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver {
                    gate: self.gate.clone(),
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
                self.gate.entered().await?;
                self.reply(request.ordinal, None)
            }
            Command::ReleaseResolver {}
                if self.handle.is_some() && !self.gate.facts()?.explicit =>
            {
                self.gate.release()?;
                self.gate.returned().await?;
                self.reply(request.ordinal, None)
            }
            Command::AwaitComplete {}
                if self.handle.is_some() && self.gate.facts()?.positive() && !self.adopted =>
            {
                let outcome = self
                    .handle
                    .as_ref()
                    .ok_or(Failure::Protocol)?
                    .wait(INNER_WAIT)
                    .await
                    .map_err(|_| Failure::Setup)?;
                if matches!(outcome, StartupWaitOutcome::Pending(_)) {
                    return Err(Failure::Timeout);
                }
                self.save(CaptureName::Adopted)?;
                self.adopted = true;
                self.reply(request.ordinal, Some(CaptureName::Adopted))
            }
            Command::Capture {}
                if self.adopted && self.lane == Lane::Unsafe && !self.final_captured =>
            {
                self.save(CaptureName::Final)?;
                self.final_captured = true;
                self.reply(request.ordinal, Some(CaptureName::Final))
            }
            Command::AwaitTerminal {}
                if self.adopted && self.lane == Lane::Approval && !self.final_captured =>
            {
                until(|| {
                    self.store
                        .thread_view(&ThreadId::new(THREAD).expect("fixed thread"))
                        .ok()
                        .flatten()
                        .and_then(|v| v.execution)
                        .is_some_and(|e| {
                            matches!(
                                e.snapshot.status,
                                ExecutionStatus::Completed
                                    | ExecutionStatus::Failed
                                    | ExecutionStatus::Cancelled
                                    | ExecutionStatus::RecoveryBlocked
                                    | ExecutionStatus::WaitingForApproval
                            )
                        })
                })
                .await?;
                self.save(CaptureName::Final)?;
                self.final_captured = true;
                self.reply(request.ordinal, Some(CaptureName::Final))
            }
            _ => Err(Failure::Protocol),
        }
    }
    async fn retire(self) -> Result<Retirement> {
        tokio::time::timeout(CLEANUP_WAIT, async {
            let ChildState {
                root,
                owner,
                store,
                app,
                host,
                server,
                writer,
                stats: _,
                gate,
                handle,
                role,
                lane: _,
                checkpointed: _,
                adopted: _,
                final_captured,
            } = self;
            if role != Role::Restart || !final_captured {
                return Err(Failure::Protocol);
            }
            let status = host.status().ok_or(Failure::Protocol)?;
            let shutdown = host.shutdown();
            let weak_store = Arc::downgrade(&store);
            let weak_app = Arc::downgrade(&app);
            shutdown.begin_shutdown();
            let outcome = shutdown
                .wait_quiescent(INNER_WAIT)
                .await
                .map_err(|_| Failure::Cleanup)?;
            let (quiescent, faulted) = match outcome {
                ShutdownOutcome::Quiescent { faulted } => (true, faulted),
                ShutdownOutcome::Incomplete { faulted, .. } => (false, faulted),
            };
            if !quiescent {
                return Err(Failure::Cleanup);
            }
            server.finish().await?;
            drop(host);
            drop(app);
            drop(store);
            until(|| weak_store.upgrade().is_none() && weak_app.upgrade().is_none()).await?;
            verify_store(&root)?;
            // Observation handles remain alive during the Weak check; they own no resources.
            let _observed = handle.as_ref().map(StartupPreparationHandle::status);
            let facts = gate.facts()?;
            let retirement = Retirement {
                version: 1,
                owner,
                quiescent,
                faulted,
                listener_finished: true,
                weak_store_absent: weak_store.upgrade().is_none(),
                weak_application_absent: weak_app.upgrade().is_none(),
                gate: facts,
                phase: status.phase.into(),
                progress: status.progress.into(),
            };
            writer.persist("retirement.json", &retirement, 4096)?;
            writer.seal()?;
            drop(handle);
            Ok(retirement)
        })
        .await
        .map_err(|_| Failure::Timeout)?
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.cleanup();
    }
}

pub async fn child_main() -> Result<()> {
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).ok_or(Failure::Setup)?);
    let lane = Lane::parse(&std::env::var(CHILD_LANE).map_err(|_| Failure::Setup)?)?;
    let role = Role::parse(&std::env::var(CHILD_ROLE).map_err(|_| Failure::Setup)?)?;
    let state = ChildState::new(root.clone(), lane, role).await?;
    let hello = Hello {
        version: 1,
        ordinal: 0,
        lane,
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
            let reply = Reply {
                version: 1,
                ordinal: pending.request.ordinal,
                phase: retirement.phase,
                progress: retirement.progress,
                gate: retirement.gate,
                capture: None,
                retired: true,
            };
            pending
                .reply
                .send(Ok(reply))
                .map_err(|_| Failure::Protocol)?;
            reader.finish().await?;
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
    if c.version != 1 || c.role != role || !c.coherent() {
        return Err(Failure::Protocol);
    }
    Ok(c)
}
fn inspect(root: &Path, lane: Lane, writer: &Writer, name: &str) -> Result<Capture> {
    let store = Arc::new(SqliteStore::open(root.join("data")).map_err(|_| Failure::Setup)?);
    verify_store(root)?;
    let weak = Arc::downgrade(&store);
    let c = capture(&store, lane, Role::Restart, Counts::default())?;
    writer.persist(name, &c, CAPTURE_BYTES)?;
    drop(store);
    if weak.upgrade().is_some() {
        return Err(Failure::Cleanup);
    }
    Ok(c)
}

pub async fn run_lane(text: &str) -> Result<()> {
    let lane = Lane::parse(text)?;
    let directory = Directory::new()?;
    let root = &directory.root;
    let writer = directory.parent.clone();
    let mut http = HttpEvidence {
        writer: writer.clone(),
        next: 0,
    };
    let endpoint = Endpoint::bind(root)?;
    let mut seed = OwnedChild::start(root, lane, Role::Seed, writer.clone())?;
    let mut seed_control = endpoint
        .accept(&seed, lane, Role::Seed, writer.clone())
        .await?;
    let seed_owner = seed_control.hello()?.owner;
    let seed_port = seed_control.hello()?.port;
    let body = encode(&json!({"route":lane.route(),"input":input(lane)}), 65_536)?;
    let body = std::str::from_utf8(&body).map_err(|_| Failure::Protocol)?;
    let mut semantics = true;
    let unsafe_socket = if lane == Lane::Approval {
        let parked = http
            .request(
                seed_port,
                "POST",
                &format!("/threads/{THREAD}/runs/wait"),
                body,
            )
            .await?;
        semantics &= parked.status == 200 && parked.json()?.is_null();
        None
    } else {
        Some(
            server::open(
                seed_port,
                "POST",
                &format!("/threads/{THREAD}/runs/stream"),
                body,
            )
            .await?,
        )
    };
    let checkpoint = seed_control.command(Command::Checkpoint {}).await?;
    semantics &= checkpoint.capture == Some(CaptureName::Source);
    let source = child_capture(root, Role::Seed, CaptureName::Source)?;
    let seed_ledger = ledger(root)?;
    seed.kill_and_reap().await?;
    drop(seed_control);
    if let Some(socket) = unsafe_socket {
        let cut = server::crash_eof(socket).await?;
        writer.persist_bytes("unsafe-original-stream-cut.bin", &cut)?;
    }
    drop(seed);
    Writer::verify_sealed(&root.join("evidence/seed"), CHILD_BYTES)?;
    let inspected = inspect(root, lane, &writer, "post-kill-inspected.json")?;
    let source_owner = source.snapshot()?.owner_generation;
    semantics &= seed_owner == source_owner
        && source.owner == seed_owner
        && inspected.owner > source_owner
        && source.same_durable(&inspected);
    let endpoint = Endpoint::bind(root)?;
    let mut restarted = OwnedChild::start(root, lane, Role::Restart, writer.clone())?;
    let mut control = endpoint
        .accept(&restarted, lane, Role::Restart, writer.clone())
        .await?;
    let restart_owner = control.hello()?.owner;
    let port = control.hello()?.port;
    semantics &= restart_owner > inspected.owner && restart_owner > source_owner;
    semantics &= http.readiness(port, false).await?;
    let preparation_started = now_ms()?;
    let entered = control.command(Command::Begin {}).await?;
    semantics &= entered.phase == Phase::Preparing
        && entered.gate.entered
        && !entered.gate.returned
        && !entered.gate.expired;
    semantics &= http.readiness(port, false).await?;
    let released = control.command(Command::ReleaseResolver {}).await?;
    semantics &= released.gate.positive();
    let complete = control.command(Command::AwaitComplete {}).await?;
    let preparation_finished = now_ms()?;
    semantics &= complete.phase == Phase::Complete
        && complete.gate.positive()
        && complete.capture == Some(CaptureName::Adopted);
    let adopted = child_capture(root, Role::Restart, CaptureName::Adopted)?;
    semantics &= http.readiness(port, true).await?;
    let mut terminal = None;
    let mut resume_interval = None;
    let mut pending_projection = None;
    if lane == Lane::Approval {
        let pending = http
            .request(
                port,
                "GET",
                &format!("/threads/{THREAD}/pending_interrupts"),
                "",
            )
            .await?;
        let projection = pending.json()?;
        semantics &= pending.status == 200;
        // Submit original persisted authority; the fresh pending projection is
        // an independent oracle checked after normal physical retirement.
        let original_grant = source.authority.grants.first().ok_or(Failure::Protocol)?;
        let resume = json!({"route":lane.route(),"resume":[{"status":"resolved","interruptId":original_grant.interrupt_id,"payload":"once","grant":original_grant.token.as_str()}]});
        let body = encode(&resume, 65_536)?;
        let started = now_ms()?;
        let response = http
            .request(
                port,
                "POST",
                &format!("/threads/{THREAD}/resume"),
                std::str::from_utf8(&body).map_err(|_| Failure::Protocol)?,
            )
            .await?;
        let finished = now_ms()?;
        semantics &= response.status == 200;
        terminal = response.done().ok();
        semantics &= terminal.is_some();
        resume_interval = Some((started, finished));
        pending_projection = Some(projection);
        let captured = control.command(Command::AwaitTerminal {}).await?;
        semantics &= captured.capture == Some(CaptureName::Final);
    } else {
        let health = json!({"route":"/restart-health#workflow","input":{"from":"restart-health"}})
            .to_string();
        let response = http
            .request(
                port,
                "POST",
                &format!("/threads/{HEALTH_THREAD}/runs/wait"),
                &health,
            )
            .await?;
        semantics &= response.status == 200 && response.json()? == json!("health");
        let captured = control.command(Command::Capture {}).await?;
        semantics &= captured.capture == Some(CaptureName::Final);
    }
    let final_capture = child_capture(root, Role::Restart, CaptureName::Final)?;
    let final_ledger = ledger(root)?;
    let stopped = control.command(Command::Stop {}).await?;
    semantics &= stopped.retired;
    drop(control);
    restarted.normal_exit_and_reap().await?;
    drop(restarted);
    let retirement: Retirement = read_json(&root.join("evidence/restart/retirement.json"), 4096)?;
    Writer::verify_sealed(&root.join("evidence/restart"), CHILD_BYTES)?;
    semantics &= retirement.positive() && retirement.owner == restart_owner;
    let persisted = inspect(root, lane, &writer, "post-retirement-inspected.json")?;
    semantics &= restart_owner.checked_add(1) == Some(persisted.owner)
        && persisted.same_durable(&final_capture)
        && ledger(root)? == final_ledger;
    writer.seal()?;
    Writer::verify_sealed(&root.join("evidence/parent"), PARENT_BYTES)?;
    evidence::verify_aggregate(root)?;
    // Every semantic comparison below follows actual normal child exit, all
    // aliases gone, exact writer succession and complete persisted reread.
    require(semantics)?;
    check_source(lane, &source, &seed_ledger)?;
    check_adoption(
        lane,
        &source,
        &adopted,
        restart_owner,
        preparation_started,
        preparation_finished,
    )?;
    check_progress(lane, complete.progress)?;
    match lane {
        Lane::Approval => check_approval_final(
            &source,
            &adopted,
            &final_capture,
            pending_projection.as_ref().ok_or(Failure::Protocol)?,
            terminal.as_ref().ok_or(Failure::Protocol)?,
            resume_interval.ok_or(Failure::Protocol)?,
            &final_ledger,
        ),
        Lane::Unsafe => check_unsafe_final(&adopted, &final_capture, &final_ledger),
    }
}

fn check_source(lane: Lane, source: &Capture, entries: &[String]) -> Result<()> {
    let s = source.snapshot()?;
    require(
        s.definition
            == match lane {
                Lane::Approval => definition(
                    lane.route(),
                    &[
                        ("sibling", false, RecoveryPolicy::Pure),
                        ("release", true, RecoveryPolicy::Pure),
                    ],
                ),
                Lane::Unsafe => {
                    definition(lane.route(), &[("unsafe", false, RecoveryPolicy::Unsafe)])
                }
            },
    )?;
    require(
        s.values.get("input") == Some(&input(lane))
            && s.attempts_used == 1
            && s.attempt_history.len() == 1
            && s.definition.max_attempts == 4
            && s.output.is_none()
            && source.authority.acceptances.is_empty()
            && source.counts.validators == 1,
    )?;
    require(source.journal.last() == Some(&JournalEntry::from(s)))?;
    match lane {
        Lane::Approval => {
            require(
                s.status == ExecutionStatus::WaitingForApproval
                    && s.tasks.len() == 2
                    && entries == ["sibling"],
            )?;
            let sibling = &s.tasks[0];
            let release = &s.tasks[1];
            require(
                sibling.status == TaskStatus::Succeeded
                    && sibling.result.as_ref() == Some(&result("sibling"))
                    && release.status == TaskStatus::WaitingForApproval
                    && release.result.is_none()
                    && source.authority.grants.len() == 1
                    && source.counts
                        == Counts {
                            sibling: 1,
                            validators: 1,
                            ..Counts::default()
                        },
            )?;
            let g = &source.authority.grants[0];
            require(
                !g.consumed
                    && g.task_id == release.spec.id
                    && release
                        .interrupt
                        .as_ref()
                        .is_some_and(|i| i.id == g.interrupt_id && i.prompt == g.prompt),
            )
        }
        Lane::Unsafe => require(
            s.status == ExecutionStatus::Running
                && s.tasks.len() == 1
                && s.tasks[0].status == TaskStatus::InFlight
                && s.tasks[0].result.is_none()
                && source.authority.grants.is_empty()
                && entries == ["unsafe"]
                && source.counts
                    == Counts {
                        unsafe_effect: 1,
                        validators: 1,
                        ..Counts::default()
                    },
        ),
    }
}
fn check_progress(lane: Lane, p: Progress) -> Result<()> {
    require(
        p == Progress {
            pages: 1,
            scanned_rows: 1,
            candidates: 1,
            admitted: 1,
            parked: usize::from(lane == Lane::Approval),
            blocked: usize::from(lane == Lane::Unsafe),
            ..Progress::default()
        },
    )
}
fn check_adoption(
    lane: Lane,
    before: &Capture,
    after: &Capture,
    owner: u64,
    started: u64,
    finished: u64,
) -> Result<()> {
    let old = before.snapshot()?;
    let new = after.snapshot()?;
    require(
        owner > old.owner_generation
            && new.owner_generation == owner
            && new.updated_at_ms >= old.updated_at_ms
            && new.updated_at_ms >= started
            && new.updated_at_ms <= finished
            && !old.turn_history.contains(&new.turn_id),
    )?;
    let proposal = b4_kernel::transition(
        old,
        KernelCommand::Recover {
            available_revision: old.definition.revision.clone(),
            turn_id: new.turn_id.clone(),
            owner_generation: owner,
        },
        TransitionFacts {
            now_ms: new.updated_at_ms,
        },
    )
    .map_err(|_| Failure::Semantics)?;
    let mut expected = before.ordinary.clone();
    expected.thread.resource_revision = expected
        .thread
        .resource_revision
        .checked_add(1)
        .ok_or(Failure::Bound)?;
    expected.execution.as_mut().ok_or(Failure::Setup)?.snapshot = proposal.snapshot.clone();
    let mut authority = before.authority.clone();
    authority.thread = expected.thread.clone();
    authority.execution = expected.execution.clone().ok_or(Failure::Setup)?;
    let mut journal = before.journal.clone();
    journal.push(JournalEntry::from(&proposal.snapshot));
    require(
        after.ordinary == expected
            && after.authority == authority
            && after.journal == journal
            && after.owner == owner
            && after.counts == Counts::default(),
    )?;
    require(match lane {
        Lane::Approval => proposal.snapshot.status == ExecutionStatus::WaitingForApproval,
        Lane::Unsafe => {
            proposal.snapshot.status == ExecutionStatus::RecoveryBlocked
                && proposal.snapshot.tasks[0].status == TaskStatus::OutcomeUnknown
        }
    })
}
fn check_unsafe_final(adopted: &Capture, final_state: &Capture, entries: &[String]) -> Result<()> {
    require(
        adopted.same_durable(final_state)
            && final_state.counts
                == Counts {
                    health: 1,
                    validators: 1,
                    ..Counts::default()
                }
            && entries == ["unsafe"],
    )
}
fn check_approval_final(
    source: &Capture,
    adopted: &Capture,
    after: &Capture,
    pending: &Value,
    terminal: &Value,
    interval: (u64, u64),
    entries: &[String],
) -> Result<()> {
    let original = &source.authority.grants[0];
    let s = adopted.snapshot()?;
    let final_snapshot = after.snapshot()?;
    let interrupts = pending["interrupts"].as_array().ok_or(Failure::Protocol)?;
    require(interrupts.len() == 1)?;
    let expected_pending = json!({"interrupts":[{"interruptId":original.interrupt_id,"resumeKey":original.resume_key,"value":original.prompt,"grant":original.token.as_str()}]});
    require(pending == &expected_pending)?;
    require(
        after.authority.grants.len() == 1
            && after.authority.acceptances.len() == 1
            && after.journal.len() == adopted.journal.len() + 3
            && after.journal.starts_with(&adopted.journal),
    )?;
    let acceptance = &after.authority.acceptances[0];
    let mut consumed = original.clone();
    consumed.consumed = true;
    require(
        after.authority.grants[0] == consumed
            && acceptance.accepted_at_ms >= interval.0
            && acceptance.accepted_at_ms <= interval.1
            && !s.turn_history.contains(&acceptance.turn_id),
    )?;
    let expected_acceptance = ApprovalAcceptance {
        execution_id: s.execution_id.clone(),
        thread_id: adopted.ordinary.thread.id.clone(),
        thread_generation: adopted.ordinary.thread.generation.clone(),
        context: ApprovalContext {
            actor: origin(),
            policy_revision: POLICY.into(),
        },
        previous_revision: s.revision,
        turn_id: acceptance.turn_id.clone(),
        definition_revision: s.definition.revision.clone(),
        accepted_at_ms: acceptance.accepted_at_ms,
        decisions: vec![AcceptedDecision {
            task_id: original.task_id.clone(),
            interrupt_id: original.interrupt_id.clone(),
            decision: ApprovalDecision::Once,
            proposal_digest: original.proposal_digest.clone(),
            task_parking_revision: original.task_parking_revision,
        }],
    };
    require(acceptance == &expected_acceptance)?;
    let j = &after.journal[adopted.journal.len()..];
    require(
        j.iter()
            .all(|e| e.updated_at_ms >= interval.0 && e.updated_at_ms <= interval.1)
            && j[0].updated_at_ms == acceptance.accepted_at_ms,
    )?;
    let resumed = b4_kernel::transition(
        s,
        KernelCommand::Resume {
            route: s.definition.route.clone(),
            decisions: vec![Decision {
                interrupt_id: original.interrupt_id.clone(),
                decision: ApprovalDecision::Once,
            }],
            turn_id: acceptance.turn_id.clone(),
        },
        TransitionFacts {
            now_ms: j[0].updated_at_ms,
        },
    )
    .map_err(|_| Failure::Semantics)?
    .snapshot;
    let task = final_snapshot.tasks.get(1).ok_or(Failure::Semantics)?;
    let attempt = task.attempt_id.clone().ok_or(Failure::Semantics)?;
    require(!s.attempt_history.contains(&attempt))?;
    let dispatched = b4_kernel::transition(
        &resumed,
        KernelCommand::Dispatch {
            task_id: original.task_id.clone(),
            attempt_id: attempt.clone(),
        },
        TransitionFacts {
            now_ms: j[1].updated_at_ms,
        },
    )
    .map_err(|_| Failure::Semantics)?
    .snapshot;
    let completed = b4_kernel::transition(
        &dispatched,
        KernelCommand::Complete {
            task_id: original.task_id.clone(),
            attempt_id: attempt,
            result: result("release"),
        },
        TransitionFacts {
            now_ms: j[2].updated_at_ms,
        },
    )
    .map_err(|_| Failure::Semantics)?
    .snapshot;
    let mut expected = adopted.ordinary.clone();
    expected.thread.resource_revision = expected
        .thread
        .resource_revision
        .checked_add(3)
        .ok_or(Failure::Bound)?;
    expected.execution.as_mut().ok_or(Failure::Setup)?.snapshot = completed.clone();
    require(
        after.ordinary == expected
            && after.authority.thread == expected.thread
            && after.authority.execution == expected.execution.clone().ok_or(Failure::Setup)?
            && j == [
                JournalEntry::from(&resumed),
                JournalEntry::from(&dispatched),
                JournalEntry::from(&completed),
            ]
            && after.counts
                == Counts {
                    release: 1,
                    ..Counts::default()
                }
            && entries == ["sibling", "release"]
            && terminal == &json!({"output":"release"})
            && completed.status == ExecutionStatus::Completed
            && completed.values["joined"] == json!(["sibling", "release"])
            && completed.tasks[0] == source.snapshot()?.tasks[0],
    )
}
