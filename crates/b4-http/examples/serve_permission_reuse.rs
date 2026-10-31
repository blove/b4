//! Explicit local or HTTP Always provisioning and reuse across process owners.
//! The native callback is pure, with an optional finite cooperative cancellation wait.
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use b4_contract::*;
use b4_http::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{approval::*, permissions::*, *};
use b4_sqlite::SqliteStore;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Error = Box<dyn std::error::Error>;
const APP: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";
const ROUTE: &str = "/reports#agent";
const TASK: &str = "publish-monthly-report";
const EFFECT: &str = "reports.publish-monthly";
const POLICY: &str = "policy-1";

enum Mode {
    Seed,
    Serve {
        token: String,
        profile: ServeProfile,
    },
}
enum ServeProfile {
    ReuseOnly,
    Approvals,
    Cancellation,
    Deletion,
}
struct Config {
    mode: Mode,
    data: PathBuf,
    port: u16,
}
impl Config {
    // Complete validation precedes both SqliteStore::open and listener binding.
    fn from_environment() -> Result<Self, Error> {
        let args: Vec<_> = std::env::args_os().skip(1).collect();
        let mode = match args.as_slice() {
            [mode] if mode == "seed" => Mode::Seed,
            [mode]
                if mode == "serve"
                    || mode == "serve-approvals"
                    || mode == "serve-cancellation"
                    || mode == "serve-deletion" =>
            {
                let profile = if mode == "serve-deletion" {
                    ServeProfile::Deletion
                } else if mode == "serve-cancellation" {
                    ServeProfile::Cancellation
                } else if mode == "serve-approvals" {
                    ServeProfile::Approvals
                } else {
                    ServeProfile::ReuseOnly
                };
                let token = std::env::var("B4RUN_TOKEN").map_err(|_| {
                    if mode == "serve-deletion" {
                        "B4RUN_TOKEN is required for serve-deletion"
                    } else if mode == "serve-cancellation" {
                        "B4RUN_TOKEN is required for serve-cancellation"
                    } else if mode == "serve-approvals" {
                        "B4RUN_TOKEN is required for serve-approvals"
                    } else {
                        "B4RUN_TOKEN is required for serve"
                    }
                })?;
                if token.trim().is_empty() {
                    return Err("B4RUN_TOKEN must not be empty".into());
                }
                Mode::Serve { token, profile }
            }
            _ => {
                return Err(
                    "usage: serve_permission_reuse seed|serve|serve-approvals|serve-cancellation|serve-deletion"
                        .into(),
                );
            }
        };
        let port = match std::env::var("B4RUN_PORT") {
            Ok(value) => value.parse::<u16>().map_err(|_| "invalid B4RUN_PORT")?,
            Err(std::env::VarError::NotPresent) => 3001,
            Err(_) => return Err("invalid B4RUN_PORT".into()),
        };
        Ok(Self {
            mode,
            data: std::env::var_os("B4RUN_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| ".b4-permission-data".into()),
            port,
        })
    }
}

fn origin() -> ThreadOrigin {
    ThreadOrigin {
        application: APP.into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
fn context() -> ApprovalContext {
    ApprovalContext {
        actor: origin(),
        policy_revision: POLICY.into(),
    }
}
fn prompt() -> Value {
    json!({"question": "Publish monthly report?"})
}
struct Publish {
    cancellation: bool,
}
impl NativeEffect for Publish {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!({"published": true}),
            writes: vec![],
        }))
    }

    fn invoke_controlled(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        if self.cancellation
            && invocation
                .input
                .get("input")
                .and_then(|input| input.get("wait_for_cancel"))
                .and_then(Value::as_bool)
                == Some(true)
        {
            let deadline = Instant::now() + Duration::from_secs(30);
            if print_json(json!({
                "status": "callback_entered",
                "thread_id": context.thread_id.to_string(),
            }))
            .is_err()
            {
                return Ok(ControlledNativeOutcome::Settled(NativeOutcome::Failed(
                    "callback entry output failed".into(),
                )));
            }
            loop {
                if observer.cancellation_requested() {
                    // Returning stops our only local work; there are no descendants.
                    return Ok(ControlledNativeOutcome::Cancelled);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                std::thread::sleep(remaining.min(Duration::from_millis(10)));
            }
        }
        self.invoke(invocation, context)
            .map(ControlledNativeOutcome::Settled)
    }
}
struct LocalConstraint;
impl ToolConstraint for LocalConstraint {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        Ok(())
    }
}
// The changed callback behavior has its own immutable application revision.
fn catalog(cancellation: bool) -> Result<(Definition, Arc<PermissionCatalog>), Error> {
    let effect = EffectId::new(EFFECT)?;
    let definition = Definition {
        route: RouteKey::new(ROUTE)?,
        revision: ExecutionRevision::new("definition-1")?,
        batches: vec![vec![TaskSpec {
            id: TaskId::new(TASK)?,
            effect_id: effect.clone(),
            requires_approval: true,
            recovery: RecoveryPolicy::Pure,
        }]],
        max_attempts: 1,
    };
    let application = Arc::new(NativeApplication::new(
        ApplicationIdentity {
            application: APP.into(),
            revision: if cancellation {
                "application-cancellation-1"
            } else {
                "application-1"
            }
            .into(),
        },
        vec![RouteRegistration {
            definition: definition.clone(),
            effects: BTreeMap::from([(
                effect.clone(),
                EffectBinding {
                    callback: Arc::new(Publish { cancellation }),
                    output_kind: OutputKind::Value,
                },
            )]),
            input_validator: None,
        }],
    )?);
    let catalog = Arc::new(PermissionCatalog::new(
        application,
        vec![ToolBindingRegistration {
            definition: definition.clone(),
            effect_id: effect,
            tool_id: StableToolId::new("release-tool")?,
            tool_revision: StableToolRevision::new("tool-1")?,
            constraint_revision: ToolConstraintRevision::new("constraint-1")?,
            constraint: Arc::new(LocalConstraint),
        }],
    )?);
    Ok((definition, catalog))
}

// This authority exists only in the explicit local seed path. It checks the
// complete definition and a fresh coherent stored view; ports still validate
// their own captured execution and resource fences.
struct SeedAuthority {
    store: Arc<SqliteStore>,
    definition: Definition,
}
impl ApprovalAuthority for SeedAuthority {
    fn authorize(
        &self,
        actual: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        let current = self
            .store
            .thread_view(&resource.thread.id)?
            .ok_or(ApprovalError::Unauthorized)?;
        if actual == &context()
            && matches!(
                operation,
                ApprovalOperation::Park
                    | ApprovalOperation::Pending
                    | ApprovalOperation::Accept
                    | ApprovalOperation::Dispatch
            )
            && resource.thread.origin.as_ref() == Some(&origin())
            && resource.execution.snapshot.definition == self.definition
            && current.thread == resource.thread
            && current.execution.as_ref() == Some(&resource.execution)
        {
            Ok(())
        } else {
            Err(ApprovalError::Unauthorized)
        }
    }
}
impl PermissionCancellationAuthority for SeedAuthority {
    fn authorize_cancel(
        &self,
        actual: &ApprovalContext,
        resource: &ApprovalResource,
        target: &RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        self.authorize(actual, ApprovalOperation::Dispatch, resource)
            .map_err(PermissionError::Approval)?;
        let view = ThreadView {
            thread: resource.thread.clone(),
            execution: Some(resource.execution.clone()),
        };
        if target == &RunCancellationTarget::from_view(&view)? {
            Ok(())
        } else {
            Err(PermissionError::Store(StoreError::RevisionConflict))
        }
    }
}
fn fence(store: &SqliteStore, thread: &ThreadId) -> Result<ApprovalFence, Error> {
    let view = store.thread_view(thread)?.ok_or("missing seed thread")?;
    Ok(ApprovalFence {
        execution: (&view.execution.ok_or("missing seed execution")?.snapshot).into(),
        resource_revision: view.thread.resource_revision,
    })
}
struct Clock(u64);
impl Clock {
    fn facts(&mut self) -> Result<TransitionFacts, Error> {
        let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        self.0 = self.0.max(now);
        Ok(TransitionFacts { now_ms: self.0 })
    }
}
fn seed(
    data: PathBuf,
    definition: Definition,
    catalog: Arc<PermissionCatalog>,
) -> Result<ThreadId, Error> {
    let store = Arc::new(SqliteStore::open(data)?);
    let mut clock = Clock(0);
    let thread = ThreadId::new(uuid::Uuid::new_v4().to_string())?;
    let execution = ExecutionId::new(uuid::Uuid::new_v4().to_string())?;
    store.create_thread_with(ThreadSeed {
        id: thread.clone(),
        created_at_ms: clock.facts()?.now_ms,
        metadata: Map::new(),
        origin: Some(origin()),
    })?;
    let initial = new_execution(
        execution.clone(),
        TurnId::new(uuid::Uuid::new_v4().to_string())?,
        store.owner_generation(),
        definition.clone(),
        Map::from_iter([("input".into(), json!({"month": "2026-09"}))]),
        clock.facts()?,
    )?;
    let admitted = store.admit(&thread, initial)?;
    store.commit(
        &execution,
        (&admitted.snapshot).into(),
        transition(&admitted.snapshot, KernelCommand::Start, clock.facts()?)?,
    )?;
    let authority = Arc::new(SeedAuthority {
        store: store.clone(),
        definition: definition.clone(),
    });
    let service = ApprovalService::new(store.clone(), authority.clone())?;
    service.park(
        &context(),
        &execution,
        fence(&store, &thread)?,
        TaskId::new(TASK)?,
        InterruptId::new("publish-approval")?,
        prompt(),
        60_000,
        clock.facts()?,
    )?;
    let pending = service.pending(&context(), &execution, fence(&store, &thread)?)?;
    let grant = pending.first().ok_or("missing pending seed approval")?;
    accept_permission_always_with_normal_inputs(
        store.clone(),
        authority.clone(),
        catalog.clone(),
        &execution,
        fence(&store, &thread)?,
        ApprovalAcceptanceRequest {
            context: context(),
            route: definition.route,
            definition_revision: definition.revision,
            turn_id: TurnId::new(uuid::Uuid::new_v4().to_string())?,
            now_ms: clock.facts()?.now_ms,
            decisions: vec![GrantDecision {
                interrupt_id: grant.interrupt_id.clone(),
                decision: ApprovalDecision::Always,
                cancelled: false,
                token: Some(grant.token.clone()),
            }],
        },
    )?;
    let view = store
        .thread_view(&thread)?
        .ok_or("missing Ready seed thread")?;
    let target = RunCancellationTarget::from_view(&view)?;
    cancel_permission_ready_with_normal_inputs(
        store.as_ref(),
        authority.as_ref(),
        catalog.as_ref(),
        &PermissionReadyCancellationRequest {
            context: &context(),
            target: &target,
            facts: clock.facts()?,
        },
        &mut PermissionEvidenceBudget::new(),
    )?;
    store.readiness()?;
    // In particular, the authority and service hold real store aliases.
    drop(pending);
    drop(service);
    drop(authority);
    drop(store);
    Ok(thread)
}

struct TokenAuth(String);
impl Authenticator for TokenAuth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get_all("authorization").iter().count() != 1
            || headers.get("authorization").and_then(|v| v.to_str().ok())
                != Some(format!("Bearer {}", self.0).as_str())
        {
            return Err(AuthenticationError);
        }
        let actor = origin();
        Ok(Principal {
            application: actor.application,
            tenant: actor.tenant,
            principal: actor.principal,
        })
    }
}
struct OwnerPolicy;
impl ResourcePolicy for OwnerPolicy {
    fn authorize(&self, principal: &Principal, resource: &ResourceContext) -> bool {
        principal.application == APP
            && principal.tenant == "tenant"
            && principal.principal == "owner"
            && match resource.operation {
                Operation::ReadCapabilities | Operation::CreateThread => resource.origin.is_none(),
                _ => resource.thread_id.is_some() && resource.origin.as_ref() == Some(&origin()),
            }
            && [&resource.stored_route, &resource.requested_route]
                .into_iter()
                .all(|r| {
                    r.as_ref().is_none_or(|r| {
                        r.route.as_str() == ROUTE && r.revision.as_str() == "definition-1"
                    })
                })
    }
}
async fn browser_guard(
    State(host): State<Arc<String>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.headers().contains_key("origin")
        || request.headers().get_all("host").iter().count() != 1
        || request.headers().get("host").and_then(|v| v.to_str().ok()) != Some(host.as_str())
    {
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            r#"{"error":{"kind":"request_error","message":"request_origin_denied","details":{}}}"#,
        )
            .into_response();
    }
    next.run(request).await
}
fn print_json(value: Value) -> Result<(), Error> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
async fn serve(
    data: PathBuf,
    port: u16,
    token: String,
    profile: ServeProfile,
    definition: Definition,
    catalog: Arc<PermissionCatalog>,
) -> Result<(), Error> {
    let store = Arc::new(SqliteStore::open(data)?);
    let approval = ApprovalHostConfig::new(
        POLICY.into(),
        BTreeMap::from([(
            definition.route,
            BTreeMap::from([(TaskId::new(TASK)?, prompt())]),
        )]),
    );
    let auth = Arc::new(TokenAuth(token));
    let policy = Arc::new(OwnerPolicy);
    let host = match profile {
        ServeProfile::ReuseOnly => HttpHost::new_with_permission_uses(
            store.clone(),
            auth,
            policy,
            PermissionUseHostConfig::new(catalog, approval),
        ),
        ServeProfile::Approvals => HttpHost::new_with_permissions(
            store.clone(),
            auth,
            policy,
            PermissionHostConfig::new(catalog, approval),
        ),
        ServeProfile::Cancellation => HttpHost::new_with_permission_withheld_cancellation(
            store.clone(),
            auth,
            policy,
            PermissionHostConfig::new(catalog, approval),
        ),
        ServeProfile::Deletion => HttpHost::new_with_permission_settled_thread_deletion(
            store.clone(),
            auth,
            policy,
            PermissionHostConfig::new(catalog, approval),
        ),
    }?;
    // Install SIGINT handling before readiness; this fixture and its harness are Unix-local.
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let router = host.router().layer(middleware::from_fn_with_state(
        Arc::new(address.to_string()),
        browser_guard,
    ));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    // No fallible early return may drop the JoinHandle once the listener is owned.
    let mut outcome = print_json(json!({"status": "ready", "address": address.to_string()}));
    let mut server_result = None;
    if outcome.is_ok() {
        tokio::select! {
            signal = interrupt.recv() => {
                if signal.is_none() { outcome = Err("interrupt stream closed".into()); }
            }
            result = &mut server => {
                server_result = Some(result);
                outcome = Err("listener exited before interrupt".into());
            }
        }
    }
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    // An incomplete wait retains every owner and continues waiting. The only
    // callback here is finite and pure; timeout is never permission to abandon it.
    let faulted = loop {
        match shutdown.wait_quiescent(Duration::from_secs(5)).await {
            Ok(ShutdownOutcome::Quiescent { faulted }) => break faulted,
            Ok(ShutdownOutcome::Incomplete { .. }) => continue,
            Err(_) => {
                outcome = Err("invalid shutdown wait".into());
            }
        }
    };
    let _ = stop.send(());
    let result = match server_result {
        Some(result) => result,
        None => server.await,
    };
    drop(shutdown);
    drop(host);
    let healthy = store.readiness();
    drop(store);
    result??;
    healthy?;
    if faulted {
        return Err("HTTP host faulted".into());
    }
    outcome
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = Config::from_environment()?;
    let cancellation = matches!(
        &config.mode,
        Mode::Serve {
            profile: ServeProfile::Cancellation | ServeProfile::Deletion,
            ..
        }
    );
    let (definition, catalog) = catalog(cancellation)?;
    match config.mode {
        Mode::Seed => {
            let thread = seed(config.data, definition, catalog)?;
            print_json(json!({"thread_id": thread.to_string(), "route": ROUTE}))
        }
        Mode::Serve { token, profile } => {
            serve(
                config.data,
                config.port,
                token,
                profile,
                definition,
                catalog,
            )
            .await
        }
    }
}
