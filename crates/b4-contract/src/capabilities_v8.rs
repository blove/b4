//! Narrow opt-in settled permission-thread deletion metadata.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CapabilityDescriptorV8Wire")]
pub struct CapabilityDescriptorV8 {
    pub descriptor_version: u32,
    pub target_profile: CapabilityTargetProfile,
    pub profile_status: CapabilityProfileStatus,
    pub core_profile_frozen: bool,
    pub runtime_version: String,
    pub application: Option<CapabilityApplication>,
    pub execution: CapabilityExecution,
    pub features: CapabilityFeaturesV8,
    pub store: CapabilityStore,
    pub readiness: CapabilityReadinessV2,
    pub limits: CapabilityLimits,
    pub controls: CapabilityControlsV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityFeaturesV8 {
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
    pub durable_permission_creation: bool,
    pub durable_permission_entered_cancellation: bool,
    pub durable_permission_ready_cancellation: bool,
    pub durable_permission_withheld_cancellation: bool,
    pub durable_permission_settled_thread_deletion: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityDescriptorV8Wire {
    descriptor_version: u32,
    target_profile: CapabilityTargetProfile,
    profile_status: CapabilityProfileStatus,
    core_profile_frozen: bool,
    runtime_version: String,
    // V8 requires a non-null native application on the wire.
    application: CapabilityApplication,
    execution: CapabilityExecution,
    features: CapabilityFeaturesV8,
    store: CapabilityStore,
    readiness: CapabilityReadinessV2,
    limits: CapabilityLimits,
    controls: CapabilityControlsV2,
}

impl TryFrom<CapabilityDescriptorV8Wire> for CapabilityDescriptorV8 {
    type Error = CapabilityError;

    fn try_from(value: CapabilityDescriptorV8Wire) -> Result<Self, Self::Error> {
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

impl CapabilityDescriptorV8 {
    /// Opt in from a validated V7 assembly, preserving all inherited fields.
    pub fn permission_settled_thread_deletion(
        base: CapabilityDescriptorV7,
    ) -> Result<Self, CapabilityError> {
        base.validate()?;
        let f = base.features;
        let descriptor = Self {
            descriptor_version: 8,
            target_profile: base.target_profile,
            profile_status: base.profile_status,
            core_profile_frozen: base.core_profile_frozen,
            runtime_version: base.runtime_version,
            application: base.application,
            execution: base.execution,
            features: CapabilityFeaturesV8 {
                live_attach: f.live_attach,
                required_once_deny: f.required_once_deny,
                trusted_known_id_recovery: f.trusted_known_id_recovery,
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
                durable_permission_reuse: f.durable_permission_reuse,
                durable_permission_creation: f.durable_permission_creation,
                durable_permission_entered_cancellation: f.durable_permission_entered_cancellation,
                durable_permission_ready_cancellation: f.durable_permission_ready_cancellation,
                durable_permission_withheld_cancellation: f
                    .durable_permission_withheld_cancellation,
                durable_permission_settled_thread_deletion: true,
            },
            store: base.store,
            readiness: base.readiness,
            limits: base.limits,
            controls: base.controls,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Validate public assembly before publishing a V8 descriptor.
    pub fn validate(&self) -> Result<(), CapabilityError> {
        let f = &self.features;
        if self.descriptor_version != 8
            || !f.durable_permission_settled_thread_deletion
            || !f.durable_permission_withheld_cancellation
            || !f.durable_permission_ready_cancellation
            || !f.durable_permission_entered_cancellation
            || !f.durable_permission_creation
            || !f.durable_permission_reuse
            || f.always_permissions
            || f.trusted_known_id_recovery
            || f.fenced_cancellation
            || f.fenced_deletion
        {
            return Err(CapabilityError("invalid_capability_v8_mechanism"));
        }
        // Bound caller-controlled identity strings before cloning metadata into
        // the validation projection. The historical validators retain all bounds.
        let label = |value: &str| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        };
        if !label(&self.runtime_version)
            || self.application.is_none()
            || self
                .application
                .as_ref()
                .is_some_and(|a| !label(&a.id) || !label(&a.revision))
        {
            return Err(CapabilityError("invalid_capability_identity"));
        }
        // Validate the complete inherited V7 projection without repairing it.
        CapabilityDescriptorV7 {
            descriptor_version: 7,
            target_profile: self.target_profile,
            profile_status: self.profile_status,
            core_profile_frozen: self.core_profile_frozen,
            runtime_version: self.runtime_version.clone(),
            application: self.application.clone(),
            execution: self.execution.clone(),
            features: CapabilityFeaturesV7 {
                live_attach: f.live_attach,
                required_once_deny: f.required_once_deny,
                trusted_known_id_recovery: f.trusted_known_id_recovery,
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
                durable_permission_reuse: f.durable_permission_reuse,
                durable_permission_creation: f.durable_permission_creation,
                durable_permission_entered_cancellation: f.durable_permission_entered_cancellation,
                durable_permission_ready_cancellation: f.durable_permission_ready_cancellation,
                durable_permission_withheld_cancellation: f
                    .durable_permission_withheld_cancellation,
            },
            store: self.store.clone(),
            readiness: self.readiness.clone(),
            limits: self.limits.clone(),
            controls: self.controls.clone(),
        }
        .validate()
    }
}
