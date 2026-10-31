//! Protected operational metadata for the bounded native HTTP host.
//!
//! This descriptor declares the assembled mechanism, not caller permission,
//! current health, a route inventory or full core-profile conformance.
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// Unknown physical approval durability is represented by null, independently
/// of whether the approval service is installed or which mode it uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CapabilityDescriptorWire")]
pub struct CapabilityDescriptor {
    pub descriptor_version: u32,
    pub target_profile: CapabilityTargetProfile,
    pub profile_status: CapabilityProfileStatus,
    pub core_profile_frozen: bool,
    pub runtime_version: String,
    pub application: Option<CapabilityApplication>,
    pub execution: CapabilityExecution,
    pub features: CapabilityFeatures,
    pub store: CapabilityStore,
    pub readiness: CapabilityReadiness,
    pub limits: CapabilityLimits,
    pub controls: CapabilityControls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityTargetProfile {
    #[serde(rename = "b4-ap-core/0")]
    B4ApCore0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityProfileStatus {
    #[serde(rename = "native_subset")]
    NativeSubset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityExecutionMode {
    #[serde(rename = "single_process")]
    SingleProcess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityRegistration {
    #[serde(rename = "immutable")]
    Immutable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityApprovalMode {
    #[serde(rename = "disabled")]
    Disabled,
    #[serde(rename = "durable_required")]
    DurableRequired,
    #[serde(rename = "volatile_fixture")]
    VolatileFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityStoreKind {
    #[serde(rename = "unspecified")]
    Unspecified,
    #[serde(rename = "memory")]
    Memory,
    #[serde(rename = "sqlite")]
    Sqlite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityReadinessResponse {
    #[serde(rename = "status_only")]
    StatusOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityReadinessAggregation {
    #[serde(rename = "store_readiness_and_no_owned_worker_fault")]
    StoreReadinessAndNoOwnedWorkerFault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityReadinessCheck {
    #[serde(rename = "store_readiness")]
    StoreReadiness,
    #[serde(rename = "owned_worker_fault")]
    OwnedWorkerFault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityApplication {
    pub id: String,
    pub revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityExecution {
    pub mode: CapabilityExecutionMode,
    pub registration: CapabilityRegistration,
    pub native_callbacks: bool,
    pub approval_service_mode: CapabilityApprovalMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityFeatures {
    pub live_attach: bool,
    pub required_once_deny: bool,
    pub trusted_known_id_recovery: bool,
    pub fenced_cancellation: bool,
    pub fenced_deletion: bool,
    pub automatic_recovery_scan: bool,
    pub always_permissions: bool,
    pub durable_event_replay: bool,
    pub native_live_chunks: bool,
    pub built_in_model_providers: bool,
    pub memory_candidates: bool,
    pub managed_workspaces: bool,
    pub ag_ui: bool,
    pub dynamic_graphs: bool,
    pub graceful_shutdown: bool,
    pub turn_deadline: bool,
    pub native_callback_sandbox: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityStore {
    pub kind: CapabilityStoreKind,
    pub durable_runtime: bool,
    #[serde(deserialize_with = "required_nullable")]
    pub durable_approvals: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReadiness {
    pub response: CapabilityReadinessResponse,
    pub aggregation: CapabilityReadinessAggregation,
    pub checks: [CapabilityReadinessCheck; 2],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityLimits {
    pub body_bytes: u64,
    pub json_depth: u64,
    pub active_runs: u64,
    pub concurrent_tasks: u64,
    pub concurrent_tasks_per_execution: u64,
    pub viewer_queue_frames: u64,
    pub heartbeat_ms: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub max_attached_viewers: u64,
    pub turn_buffer_bytes: u64,
    pub turn_buffer_frames: u64,
    pub viewer_queue_bytes: u64,
    pub attachment_snapshot_bytes: u64,
    pub program_routes: u64,
    pub program_tasks: u64,
    pub program_attempts: u64,
    pub completion_attempts: u64,
    pub task_failure_bytes: u64,
    pub script_chunks: u64,
    pub script_delay_ms: u64,
    pub approval_bytes: u64,
    #[serde(deserialize_with = "required_nullable")]
    pub approval_ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityControls {
    #[serde(deserialize_with = "required_nullable")]
    pub callback_timeout_ms: Option<u64>,
    #[serde(deserialize_with = "required_nullable")]
    pub validator_timeout_ms: Option<u64>,
    #[serde(deserialize_with = "required_nullable")]
    pub turn_deadline_ms: Option<u64>,
    #[serde(deserialize_with = "required_nullable")]
    pub max_threads: Option<u64>,
    #[serde(deserialize_with = "required_nullable")]
    pub max_history_bytes: Option<u64>,
    #[serde(deserialize_with = "required_nullable")]
    pub parked_retention_ms: Option<u64>,
    #[serde(deserialize_with = "required_nullable")]
    pub connection_limit: Option<u64>,
    #[serde(deserialize_with = "required_nullable")]
    pub whole_process_memory_bytes: Option<u64>,
}

fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityDescriptorWire {
    descriptor_version: u32,
    target_profile: CapabilityTargetProfile,
    profile_status: CapabilityProfileStatus,
    core_profile_frozen: bool,
    runtime_version: String,
    #[serde(deserialize_with = "required_nullable")]
    application: Option<CapabilityApplication>,
    execution: CapabilityExecution,
    features: CapabilityFeatures,
    store: CapabilityStore,
    readiness: CapabilityReadiness,
    limits: CapabilityLimits,
    controls: CapabilityControls,
}

impl TryFrom<CapabilityDescriptorWire> for CapabilityDescriptor {
    type Error = CapabilityError;
    fn try_from(value: CapabilityDescriptorWire) -> Result<Self, Self::Error> {
        let descriptor = Self {
            descriptor_version: value.descriptor_version,
            target_profile: value.target_profile,
            profile_status: value.profile_status,
            core_profile_frozen: value.core_profile_frozen,
            runtime_version: value.runtime_version,
            application: value.application,
            execution: value.execution,
            features: value.features,
            store: value.store,
            readiness: value.readiness,
            limits: value.limits,
            controls: value.controls,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityError(pub &'static str);
impl fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for CapabilityError {}

impl CapabilityDescriptor {
    /// Check this descriptor version's bounds and declared assembly consistency.
    /// The publisher must validate before serializing a publicly assembled value.
    pub fn validate(&self) -> Result<(), CapabilityError> {
        let label = |value: &str| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        };
        if self.descriptor_version != 1
            || self.core_profile_frozen
            || !label(&self.runtime_version)
            || self
                .application
                .as_ref()
                .is_some_and(|a| !label(&a.id) || !label(&a.revision))
        {
            return Err(CapabilityError("invalid_capability_identity"));
        }
        let bridge = self.execution.approval_service_mode != CapabilityApprovalMode::Disabled;
        if self.execution.native_callbacks != self.application.is_some()
            || self.execution.native_callbacks && !bridge
            || self.features.required_once_deny != bridge
            || self.features.trusted_known_id_recovery != bridge
            || bridge != self.limits.approval_ttl_ms.is_some()
            || self.execution.approval_service_mode == CapabilityApprovalMode::DurableRequired
                && self.store.durable_approvals != Some(true)
            || self.execution.approval_service_mode == CapabilityApprovalMode::VolatileFixture
                && self.store.durable_approvals.is_none()
        {
            return Err(CapabilityError("invalid_capability_assembly"));
        }
        let f = &self.features;
        if !f.live_attach
            || f.automatic_recovery_scan
            || f.always_permissions
            || f.durable_event_replay
            || f.native_live_chunks
            || f.built_in_model_providers
            || f.memory_candidates
            || f.managed_workspaces
            || f.ag_ui
            || f.dynamic_graphs
            || f.graceful_shutdown
            || f.turn_deadline
            || f.native_callback_sandbox
        {
            return Err(CapabilityError("unsupported_capability_feature"));
        }
        if self.controls != CapabilityControls::default()
            || self.readiness.checks
                != [
                    CapabilityReadinessCheck::StoreReadiness,
                    CapabilityReadinessCheck::OwnedWorkerFault,
                ]
        {
            return Err(CapabilityError("unsupported_capability_controls"));
        }
        let l = &self.limits;
        if l.body_bytes != 65536
            || l.json_depth != 64
            || l.program_routes != 64
            || l.program_tasks != 64
            || l.program_attempts != 64
            || l.completion_attempts != 8
            || l.task_failure_bytes != 1024
            || l.script_chunks != 64
            || l.script_delay_ms != 5000
            || l.approval_bytes != 65536
        {
            return Err(CapabilityError("invalid_capability_fixed_limits"));
        }
        let bounds = [
            (l.active_runs, 256),
            (l.concurrent_tasks, 256),
            (l.concurrent_tasks_per_execution, 64),
            (l.viewer_queue_frames, 512),
            (l.heartbeat_ms, 60000),
            (l.input_bytes, 65536),
            (l.output_bytes, 65536),
            (l.max_attached_viewers, 64),
            (l.turn_buffer_bytes, 8388608),
            (l.turn_buffer_frames, 4096),
            (l.viewer_queue_bytes, 8388608),
            (l.attachment_snapshot_bytes, 8388608),
        ];
        if bounds
            .into_iter()
            .any(|(value, maximum)| !(1..=maximum).contains(&value))
            || l.approval_ttl_ms
                .is_some_and(|ttl| !(1..=86400000).contains(&ttl))
        {
            return Err(CapabilityError("invalid_capability_effective_limits"));
        }
        Ok(())
    }
}
