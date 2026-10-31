use axum::{http::HeaderMap, Router};
use b4_contract::*;
use b4_runtime::{approval::ApprovalStore, Store, ThreadOrigin};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc, Mutex},
};
use tokio::sync::Semaphore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub application: String,
    pub tenant: String,
    pub principal: String,
}
#[derive(Debug, Clone, Copy)]
pub struct AuthenticationError;
/// Authentication derives identity from server-trusted credentials. Implementors
/// must not return identity supplied in thread metadata or run input.
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    ReadCapabilities,
    CreateThread,
    ReadThread,
    ReadState,
    ReadPending,
    RunWait,
    RunStream,
    Resume,
    Recover,
    CancelRun,
    Attach,
    DeleteThread,
    Unsupported,
}
#[derive(Debug, Clone)]
pub struct RouteProvenance {
    pub route: RouteKey,
    pub revision: ExecutionRevision,
}
#[derive(Debug, Clone)]
pub struct ResourceContext {
    pub operation: Operation,
    pub thread_id: Option<ThreadId>,
    pub origin: Option<ThreadOrigin>,
    pub stored_route: Option<RouteProvenance>,
    pub requested_route: Option<RouteProvenance>,
}
/// Every protected operation is evaluated against one coherent stored view.
/// Origin-less legacy threads are rejected even when a policy returns true.
pub trait ResourcePolicy: Send + Sync {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool;
}
#[derive(Debug, Clone)]
pub enum ScriptEffect {
    Result(TaskResult),
    Input,
    ProbeInput,
    Void,
    /// A known, settled application failure. The message is public content
    /// authored by the application, never a caught infrastructure diagnostic.
    Failure(String),
    Delayed {
        duration_ms: u64,
        result: TaskResult,
    },
    Chunks {
        chunks: Vec<serde_json::Value>,
        result: TaskResult,
    },
}
#[derive(Debug, Clone)]
pub struct ScriptRoute {
    pub definition: Definition,
    pub effects: BTreeMap<EffectId, ScriptEffect>,
}
#[derive(Debug, Clone)]
pub struct HostConfig {
    pub routes: Vec<ScriptRoute>,
    pub max_active_runs: usize,
    pub max_concurrent_tasks: usize,
    pub max_tasks_per_execution: usize,
    pub viewer_queue_frames: usize,
    pub heartbeat_ms: u64,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_attached_viewers: usize,
    pub turn_buffer_bytes: usize,
    pub turn_buffer_frames: usize,
    pub viewer_queue_bytes: usize,
    pub attachment_snapshot_bytes: usize,
}
impl HostConfig {
    pub fn new(routes: Vec<ScriptRoute>) -> Self {
        Self {
            routes,
            max_active_runs: 8,
            max_concurrent_tasks: 8,
            max_tasks_per_execution: 2,
            viewer_queue_frames: 16,
            heartbeat_ms: 15_000,
            max_input_bytes: 65_536,
            max_output_bytes: 65_536,
            max_attached_viewers: 16,
            turn_buffer_bytes: 2_097_152,
            turn_buffer_frames: 4096,
            viewer_queue_bytes: 2_097_152,
            attachment_snapshot_bytes: 2_097_152,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct ConfigError(pub &'static str);
impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ConfigError {}

pub(crate) struct Shared<S> {
    pub store: Arc<S>,
    pub auth: Arc<dyn Authenticator>,
    pub policy: Arc<dyn ResourcePolicy>,
    pub routes: BTreeMap<RouteKey, crate::route::RegisteredRoute>,
    pub application: Option<Arc<b4_runtime::NativeApplication>>,
    pub native_streaming: bool,
    pub approval_durable: Option<bool>,
    pub config: HostConfig,
    pub capacity: Arc<Semaphore>,
    pub task_capacity: b4_runtime::TaskCapacity,
    pub fault: Arc<AtomicBool>,
    pub supervisor: crate::ShutdownHandle,
    pub approval: Option<Arc<dyn crate::approval::ApprovalBridge>>,
    pub permission_ready_cancellation: bool,
    pub permission_withheld_cancellation: bool,
    pub permission_deletion: Option<Arc<dyn crate::permission_deletion::PermissionDeletionBridge>>,
    pub permission_cancellation:
        Option<Arc<dyn crate::permission_cancellation::CancellationBridge>>,
    pub permission_uses: Option<Arc<dyn crate::permissions::PermissionUseBridge>>,
    pub claims: Arc<Mutex<crate::claim::ClaimRegistry>>,
    pub diagnostics: crate::RequestDiagnostics,
}
/// This host supplies a router, never a Tokio executor. The embedding application
/// owns its executor. An admitted blocking worker retains the store and capacity
/// permit through effect quiescence even when its HTTP viewer disconnects.
pub struct HttpHost<S> {
    pub(crate) shared: Arc<Shared<S>>,
}
impl<S> Clone for HttpHost<S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl<S: Store + 'static> HttpHost<S> {
    pub fn new(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: HostConfig,
    ) -> Result<Self, ConfigError> {
        Self::build(store, auth, policy, config, None)
    }
    fn build(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: HostConfig,
        approval: Option<Arc<dyn crate::approval::ApprovalBridge>>,
    ) -> Result<Self, ConfigError> {
        validate_config(&config, approval.is_some())?;
        let routes = config
            .routes
            .iter()
            .cloned()
            .map(|route| {
                (
                    route.definition.route.clone(),
                    crate::route::RegisteredRoute::scripted(route),
                )
            })
            .collect();
        Self::build_registered(store, auth, policy, config, routes, approval, None, None)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_registered(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: HostConfig,
        routes: BTreeMap<RouteKey, crate::route::RegisteredRoute>,
        approval: Option<Arc<dyn crate::approval::ApprovalBridge>>,
        application: Option<Arc<b4_runtime::NativeApplication>>,
        approval_durable: Option<bool>,
    ) -> Result<Self, ConfigError> {
        Self::build_registered_preparation(
            store,
            auth,
            policy,
            config,
            routes,
            approval,
            application,
            approval_durable,
            false,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_registered_preparation(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: HostConfig,
        routes: BTreeMap<RouteKey, crate::route::RegisteredRoute>,
        approval: Option<Arc<dyn crate::approval::ApprovalBridge>>,
        application: Option<Arc<b4_runtime::NativeApplication>>,
        approval_durable: Option<bool>,
        preparing: bool,
    ) -> Result<Self, ConfigError> {
        validate_limits(&config)?;
        let diagnostics = crate::RequestDiagnostics::new()?;
        store
            .readiness()
            .map_err(|_| ConfigError("store_not_ready"))?;
        let capacity = Arc::new(Semaphore::new(config.max_active_runs));
        let task_capacity = b4_runtime::TaskCapacity::new(config.max_concurrent_tasks)
            .map_err(|_| ConfigError("invalid_task_limits"))?;
        let fault = Arc::new(AtomicBool::new(false));
        let supervisor = if preparing {
            crate::ShutdownHandle::new_preparing(fault.clone())
        } else {
            crate::ShutdownHandle::new(fault.clone())
        };
        Ok(Self {
            shared: Arc::new(Shared {
                store,
                auth,
                policy,
                routes,
                application,
                native_streaming: false,
                approval_durable,
                config,
                capacity,
                task_capacity,
                fault,
                supervisor,
                approval,
                permission_uses: None,
                permission_cancellation: None,
                permission_ready_cancellation: false,
                permission_withheld_cancellation: false,
                permission_deletion: None,
                claims: Arc::new(Mutex::new(crate::claim::ClaimRegistry::default())),
                diagnostics,
            }),
        })
    }
    pub fn store(&self) -> &Arc<S> {
        &self.shared.store
    }
    /// Close admission and observe registered framework work without retaining
    /// the store. The embedding remains responsible for its listener and refs.
    pub fn shutdown_handle(&self) -> crate::ShutdownHandle {
        self.shared.supervisor.clone()
    }
    /// Read bounded best-effort request records without retaining application
    /// resources. The embedding owns any export or log delivery.
    pub fn diagnostics(&self) -> crate::RequestDiagnostics {
        self.shared.diagnostics.clone()
    }
    pub fn router(&self) -> Router {
        Router::new()
            .fallback(crate::transport::endpoint::<S>)
            .with_state(self.shared.clone())
    }
}
impl<S: ApprovalStore + 'static> HttpHost<S> {
    pub fn new_with_approvals(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: HostConfig,
        approval_config: crate::ApprovalHostConfig,
    ) -> Result<Self, ConfigError> {
        validate_config(&config, true)?;
        let routes = config
            .routes
            .iter()
            .cloned()
            .map(|route| {
                (
                    route.definition.route.clone(),
                    crate::route::RegisteredRoute::scripted(route),
                )
            })
            .collect();
        let approval_durable = Some(store.approval_durable());
        let approval = crate::approval::bridge(
            store.clone(),
            policy.clone(),
            &routes,
            config.max_output_bytes,
            approval_config,
        )?;
        Self::build_registered(
            store,
            auth,
            policy,
            config,
            routes,
            Some(approval),
            None,
            approval_durable,
        )
    }
}
pub(crate) fn validate_limits(config: &HostConfig) -> Result<(), ConfigError> {
    if !(1..=256).contains(&config.max_active_runs)
        || !(1..=256).contains(&config.max_concurrent_tasks)
        || !(1..=64).contains(&config.max_tasks_per_execution)
        || !(1..=512).contains(&config.viewer_queue_frames)
        || !(1..=60_000).contains(&config.heartbeat_ms)
        || !(1..=65_536).contains(&config.max_input_bytes)
        || !(1..=65_536).contains(&config.max_output_bytes)
        || !(1..=64).contains(&config.max_attached_viewers)
        || !(1..=8_388_608).contains(&config.turn_buffer_bytes)
        || !(1..=4096).contains(&config.turn_buffer_frames)
        || !(1..=8_388_608).contains(&config.viewer_queue_bytes)
        || !(1..=8_388_608).contains(&config.attachment_snapshot_bytes)
    {
        return Err(ConfigError("invalid_limits"));
    }
    Ok(())
}
fn validate_config(config: &HostConfig, approvals: bool) -> Result<(), ConfigError> {
    validate_limits(config)?;
    if config.routes.is_empty() || config.routes.len() > 64 {
        return Err(ConfigError("invalid_limits"));
    }
    let mut routes = std::collections::BTreeSet::new();
    for route in &config.routes {
        if !routes.insert(&route.definition.route) {
            return Err(ConfigError("duplicate_route"));
        }
        let tasks: Vec<_> = route.definition.batches.iter().flatten().collect();
        if tasks.len() > 64
            || route.definition.max_attempts > 64
            || route.effects.len() > 64
            || (route.definition.max_attempts as usize) < tasks.len()
        {
            return Err(ConfigError("definition_limit"));
        }
        for task in tasks {
            if task.requires_approval && !approvals {
                return Err(ConfigError("approval_unsupported"));
            }
            if task.recovery != RecoveryPolicy::Pure {
                return Err(ConfigError("only_pure_scripted_effects"));
            }
            if !route.effects.contains_key(&task.effect_id) {
                return Err(ConfigError("missing_effect_fixture"));
            }
        }
        for effect in route.effects.values() {
            match effect {
                ScriptEffect::Result(result)
                | ScriptEffect::Delayed { result, .. }
                | ScriptEffect::Chunks { result, .. } => {
                    b4_runtime::validate_effect_outcome(
                        &b4_runtime::EffectOutcome::Succeeded(result.clone()),
                        config.max_output_bytes,
                    )
                    .map_err(|_| ConfigError("result_limit"))?;
                    if serde_json::to_vec(result)
                        .map_err(|_| ConfigError("invalid_result"))?
                        .len()
                        > config.max_output_bytes
                    {
                        return Err(ConfigError("result_limit"));
                    }
                }
                ScriptEffect::Failure(reason) => {
                    if reason.is_empty() || reason.len() > MAX_TASK_FAILURE_BYTES {
                        return Err(ConfigError("failure_limit"));
                    }
                    let wait = json!({"error":{"kind":"execution_error","message":reason}});
                    let done = json!({"output":{"error":reason}});
                    for envelope in [&wait, &done] {
                        if serde_json::to_vec(envelope)
                            .map_err(|_| ConfigError("invalid_failure"))?
                            .len()
                            > config.max_output_bytes
                        {
                            return Err(ConfigError("failure_output_limit"));
                        }
                    }
                }
                _ => {}
            }
            if matches!(effect, ScriptEffect::Delayed { duration_ms, .. } if *duration_ms > 5_000) {
                return Err(ConfigError("delay_limit"));
            }
            if let ScriptEffect::Chunks { chunks, .. } = effect {
                if chunks.len() > 64
                    || serde_json::to_vec(chunks)
                        .map_err(|_| ConfigError("invalid_chunks"))?
                        .len()
                        > config.max_output_bytes
                {
                    return Err(ConfigError("chunk_limit"));
                }
            }
        }
        b4_kernel::new_execution(
            ExecutionId::new("validate").unwrap(),
            TurnId::new("validate").unwrap(),
            1,
            route.definition.clone(),
            serde_json::Map::new(),
            TransitionFacts { now_ms: 0 },
        )
        .map_err(|_| ConfigError("invalid_definition"))?;
    }
    Ok(())
}
