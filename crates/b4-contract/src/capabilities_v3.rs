//! Narrow opt-in durable permission reuse metadata.
//!
//! V3 declares consumption of existing durable permissions and required Once/Deny
//! fallback. It does not declare HTTP Always creation, recovery, cancellation or
//! deletion. V1/V2 retain their own unchanged validators and wire contracts.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CapabilityDescriptorV3Wire")]
pub struct CapabilityDescriptorV3 {
    pub descriptor_version: u32,
    pub target_profile: CapabilityTargetProfile,
    pub profile_status: CapabilityProfileStatus,
    pub core_profile_frozen: bool,
    pub runtime_version: String,
    pub application: Option<CapabilityApplication>,
    pub execution: CapabilityExecution,
    pub features: CapabilityFeaturesV3,
    pub store: CapabilityStore,
    pub readiness: CapabilityReadinessV2,
    pub limits: CapabilityLimits,
    pub controls: CapabilityControlsV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityFeaturesV3 {
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
    pub supervised_admission_drain: bool,
    pub durable_permission_reuse: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityDescriptorV3Wire {
    descriptor_version: u32,
    target_profile: CapabilityTargetProfile,
    profile_status: CapabilityProfileStatus,
    core_profile_frozen: bool,
    runtime_version: String,
    // Unlike V1/V2, V3 requires a non-null native application on the wire.
    application: CapabilityApplication,
    execution: CapabilityExecution,
    features: CapabilityFeaturesV3,
    store: CapabilityStore,
    readiness: CapabilityReadinessV2,
    limits: CapabilityLimits,
    controls: CapabilityControlsV2,
}

impl TryFrom<CapabilityDescriptorV3Wire> for CapabilityDescriptorV3 {
    type Error = CapabilityError;

    fn try_from(value: CapabilityDescriptorV3Wire) -> Result<Self, Self::Error> {
        let descriptor = Self {
            descriptor_version: value.descriptor_version,
            target_profile: value.target_profile,
            profile_status: value.profile_status,
            core_profile_frozen: value.core_profile_frozen,
            runtime_version: value.runtime_version,
            application: Some(value.application),
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

impl CapabilityDescriptorV3 {
    /// Opt in from a valid V2 native assembly with already effective single-task
    /// limits. Reject incompatible bases instead of silently clamping limits.
    pub fn permission_reuse(base: CapabilityDescriptorV2) -> Result<Self, CapabilityError> {
        base.validate()?;
        let f = base.features;
        let descriptor = Self {
            descriptor_version: 3,
            target_profile: base.target_profile,
            profile_status: base.profile_status,
            core_profile_frozen: base.core_profile_frozen,
            runtime_version: base.runtime_version,
            application: base.application,
            execution: base.execution,
            features: CapabilityFeaturesV3 {
                live_attach: f.live_attach,
                required_once_deny: f.required_once_deny,
                trusted_known_id_recovery: false,
                fenced_cancellation: false,
                fenced_deletion: false,
                automatic_recovery_scan: f.automatic_recovery_scan,
                always_permissions: f.always_permissions,
                durable_event_replay: f.durable_event_replay,
                native_live_chunks: f.native_live_chunks,
                built_in_model_providers: f.built_in_model_providers,
                memory_candidates: f.memory_candidates,
                managed_workspaces: f.managed_workspaces,
                ag_ui: f.ag_ui,
                dynamic_graphs: f.dynamic_graphs,
                graceful_shutdown: f.graceful_shutdown,
                turn_deadline: f.turn_deadline,
                native_callback_sandbox: f.native_callback_sandbox,
                supervised_admission_drain: f.supervised_admission_drain,
                durable_permission_reuse: true,
            },
            store: base.store,
            readiness: base.readiness,
            limits: base.limits,
            controls: base.controls,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Validate public assembly before publishing a V3 descriptor.
    pub fn validate(&self) -> Result<(), CapabilityError> {
        let f = &self.features;
        if self.descriptor_version != 3
            || !f.durable_permission_reuse
            || f.trusted_known_id_recovery
            || f.fenced_cancellation
            || f.fenced_deletion
        {
            return Err(CapabilityError("invalid_capability_v3_mechanism"));
        }
        if self.application.is_none()
            || !self.execution.native_callbacks
            || self.execution.approval_service_mode != CapabilityApprovalMode::DurableRequired
            || !self.store.durable_runtime
            || self.store.durable_approvals != Some(true)
            || self.limits.active_runs != 1
            || self.limits.concurrent_tasks != 1
            || self.limits.concurrent_tasks_per_execution != 1
        {
            return Err(CapabilityError("invalid_capability_v3_assembly"));
        }
        // Validate every unchanged bound and declaration with the frozen V2
        // validator. Its approval bridge requires recovery=true; only this local
        // validation projection restores that legacy promise. V3 explicitly
        // checks recovery=false above and never publishes this projection.
        CapabilityDescriptorV2 {
            descriptor_version: 2,
            target_profile: self.target_profile,
            profile_status: self.profile_status,
            core_profile_frozen: self.core_profile_frozen,
            runtime_version: self.runtime_version.clone(),
            application: self.application.clone(),
            execution: self.execution.clone(),
            features: CapabilityFeaturesV2 {
                live_attach: f.live_attach,
                required_once_deny: f.required_once_deny,
                trusted_known_id_recovery: true,
                fenced_cancellation: f.fenced_cancellation,
                fenced_deletion: f.fenced_deletion,
                automatic_recovery_scan: f.automatic_recovery_scan,
                always_permissions: f.always_permissions,
                durable_event_replay: f.durable_event_replay,
                native_live_chunks: f.native_live_chunks,
                built_in_model_providers: f.built_in_model_providers,
                memory_candidates: f.memory_candidates,
                managed_workspaces: f.managed_workspaces,
                ag_ui: f.ag_ui,
                dynamic_graphs: f.dynamic_graphs,
                graceful_shutdown: f.graceful_shutdown,
                turn_deadline: f.turn_deadline,
                native_callback_sandbox: f.native_callback_sandbox,
                supervised_admission_drain: f.supervised_admission_drain,
            },
            store: self.store.clone(),
            readiness: self.readiness.clone(),
            limits: self.limits.clone(),
            controls: self.controls.clone(),
        }
        .validate()
    }
}
