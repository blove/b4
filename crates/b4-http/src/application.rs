use crate::*;
use b4_runtime::{approval::ApprovalStore, NativeApplication};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NativeHostConfig {
    pub application: Arc<NativeApplication>,
    pub limits: HostLimits,
    pub approval: ApprovalHostConfig,
}
impl NativeHostConfig {
    pub fn new(application: Arc<NativeApplication>, approval: ApprovalHostConfig) -> Self {
        Self {
            application,
            limits: HostLimits::default(),
            approval,
        }
    }
}
/// Finite admission, callback and publication limits shared by both host forms.
#[derive(Debug, Clone)]
pub struct HostLimits {
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
impl Default for HostLimits {
    fn default() -> Self {
        HostConfig::new(Vec::new()).limits()
    }
}
impl HostConfig {
    pub fn limits(&self) -> HostLimits {
        HostLimits {
            max_active_runs: self.max_active_runs,
            max_concurrent_tasks: self.max_concurrent_tasks,
            max_tasks_per_execution: self.max_tasks_per_execution,
            viewer_queue_frames: self.viewer_queue_frames,
            heartbeat_ms: self.heartbeat_ms,
            max_input_bytes: self.max_input_bytes,
            max_output_bytes: self.max_output_bytes,
            max_attached_viewers: self.max_attached_viewers,
            turn_buffer_bytes: self.turn_buffer_bytes,
            turn_buffer_frames: self.turn_buffer_frames,
            viewer_queue_bytes: self.viewer_queue_bytes,
            attachment_snapshot_bytes: self.attachment_snapshot_bytes,
        }
    }
    pub(crate) fn with_limits(limits: HostLimits) -> Self {
        Self {
            routes: Vec::new(),
            max_active_runs: limits.max_active_runs,
            max_concurrent_tasks: limits.max_concurrent_tasks,
            max_tasks_per_execution: limits.max_tasks_per_execution,
            viewer_queue_frames: limits.viewer_queue_frames,
            heartbeat_ms: limits.heartbeat_ms,
            max_input_bytes: limits.max_input_bytes,
            max_output_bytes: limits.max_output_bytes,
            max_attached_viewers: limits.max_attached_viewers,
            turn_buffer_bytes: limits.turn_buffer_bytes,
            turn_buffer_frames: limits.turn_buffer_frames,
            viewer_queue_bytes: limits.viewer_queue_bytes,
            attachment_snapshot_bytes: limits.attachment_snapshot_bytes,
        }
    }
}
impl<S: ApprovalStore + 'static> HttpHost<S> {
    /// Construct an opt-in native host for bounded startup preparation.
    /// The preparing wrapper exposes no store or ordinary host conversion.
    pub fn new_with_application_preparing(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: NativeHostConfig,
    ) -> Result<PreparingHttpHost<S>, ConfigError> {
        if config.approval.service_config
            != b4_runtime::approval::ApprovalServiceConfig::RequiredDurable
            || !store.approval_durable()
        {
            return Err(ConfigError("startup_requires_durable_approval"));
        }
        let limits = HostConfig::with_limits(config.limits);
        crate::host::validate_limits(&limits)?;
        let routes = config
            .application
            .definitions()
            .map(|definition| {
                (
                    definition.route.clone(),
                    crate::route::RegisteredRoute::native(
                        config.application.clone(),
                        definition.clone(),
                    ),
                )
            })
            .collect();
        let approval_durable = Some(store.approval_durable());
        let approval = crate::approval::native_bridge(
            store.clone(),
            policy.clone(),
            &routes,
            limits.max_output_bytes,
            config.approval,
            config.application.clone(),
        )?;
        Self::build_registered_preparation(
            store,
            auth,
            policy,
            limits,
            routes,
            Some(approval),
            Some(config.application),
            approval_durable,
            true,
        )
        .map(PreparingHttpHost::new)
    }

    /// Opt in to transient native `tool_call`, direct `chunk`, and committed
    /// `tool_result` events for fresh POST stream runs on the ordinary driver.
    /// Wait, Resume, Recover and permission continuations retain the legacy
    /// callback entry and digest. Permission bridges may bypass the ordinary
    /// driver. A tool call records dispatch intent, not callback entry or success.
    /// Projection limits close viewers without rolling back durable execution.
    pub fn new_with_streaming_application(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: NativeHostConfig,
    ) -> Result<Self, ConfigError> {
        let mut host = Self::new_with_application(store, auth, policy, config)?;
        Arc::get_mut(&mut host.shared)
            .expect("unpublished host")
            .native_streaming = true;
        Ok(host)
    }

    /// Install one immutable native registry and the existing approval authority
    /// bridge, including renewal for ordinary tasks without approval prompts.
    pub fn new_with_application(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: NativeHostConfig,
    ) -> Result<Self, ConfigError> {
        let limits = HostConfig::with_limits(config.limits);
        crate::host::validate_limits(&limits)?;
        let routes = config
            .application
            .definitions()
            .map(|definition| {
                (
                    definition.route.clone(),
                    crate::route::RegisteredRoute::native(
                        config.application.clone(),
                        definition.clone(),
                    ),
                )
            })
            .collect();
        let approval_durable = Some(store.approval_durable());
        let approval = crate::approval::native_bridge(
            store.clone(),
            policy.clone(),
            &routes,
            limits.max_output_bytes,
            config.approval,
            config.application.clone(),
        )?;
        Self::build_registered(
            store,
            auth,
            policy,
            limits,
            routes,
            Some(approval),
            Some(config.application),
            approval_durable,
        )
    }
}
pub(crate) fn scope_allows<S>(shared: &crate::host::Shared<S>, principal: &Principal) -> bool {
    shared
        .application
        .as_ref()
        .is_none_or(|application| application.identity().application == principal.application)
}
