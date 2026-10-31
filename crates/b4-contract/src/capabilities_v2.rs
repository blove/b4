//! Supervised admission metadata with the frozen v1 semantic bounds.
//!
//! Version 2 declares admission drain and bounded embedding waits. It does not
//! promise callback interruption, listener closure or full graceful shutdown.
use crate::*;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CapabilityDescriptorV2Wire")]
pub struct CapabilityDescriptorV2 {
    pub descriptor_version: u32,
    pub target_profile: CapabilityTargetProfile,
    pub profile_status: CapabilityProfileStatus,
    pub core_profile_frozen: bool,
    pub runtime_version: String,
    pub application: Option<CapabilityApplication>,
    pub execution: CapabilityExecution,
    pub features: CapabilityFeaturesV2,
    pub store: CapabilityStore,
    pub readiness: CapabilityReadinessV2,
    pub limits: CapabilityLimits,
    pub controls: CapabilityControlsV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityFeaturesV2 {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityReadinessAggregationV2 {
    #[serde(rename = "store_readiness_and_no_owned_worker_fault_and_serving_admission")]
    StoreReadinessAndNoOwnedWorkerFaultAndServingAdmission,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityReadinessCheckV2 {
    #[serde(rename = "store_readiness")]
    StoreReadiness,
    #[serde(rename = "owned_worker_fault")]
    OwnedWorkerFault,
    #[serde(rename = "serving_admission")]
    ServingAdmission,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReadinessV2 {
    pub response: CapabilityReadinessResponse,
    pub aggregation: CapabilityReadinessAggregationV2,
    pub checks: [CapabilityReadinessCheckV2; 3],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityControlsV2 {
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
    pub shutdown_default_wait_ms: u64,
    pub shutdown_max_wait_ms: u64,
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
struct CapabilityDescriptorV2Wire {
    descriptor_version: u32,
    target_profile: CapabilityTargetProfile,
    profile_status: CapabilityProfileStatus,
    core_profile_frozen: bool,
    runtime_version: String,
    #[serde(deserialize_with = "required_nullable")]
    application: Option<CapabilityApplication>,
    execution: CapabilityExecution,
    features: CapabilityFeaturesV2,
    store: CapabilityStore,
    readiness: CapabilityReadinessV2,
    limits: CapabilityLimits,
    controls: CapabilityControlsV2,
}

impl TryFrom<CapabilityDescriptorV2Wire> for CapabilityDescriptorV2 {
    type Error = CapabilityError;

    fn try_from(value: CapabilityDescriptorV2Wire) -> Result<Self, Self::Error> {
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

impl TryFrom<CapabilityDescriptor> for CapabilityDescriptorV2 {
    type Error = CapabilityError;

    fn try_from(value: CapabilityDescriptor) -> Result<Self, Self::Error> {
        value.validate()?;
        Ok(Self {
            descriptor_version: 2,
            target_profile: value.target_profile,
            profile_status: value.profile_status,
            core_profile_frozen: value.core_profile_frozen,
            runtime_version: value.runtime_version,
            application: value.application,
            execution: value.execution,
            features: CapabilityFeaturesV2::from_v1(value.features),
            store: value.store,
            readiness: CapabilityReadinessV2 {
                response: CapabilityReadinessResponse::StatusOnly,
                aggregation: CapabilityReadinessAggregationV2::StoreReadinessAndNoOwnedWorkerFaultAndServingAdmission,
                checks: [
                    CapabilityReadinessCheckV2::StoreReadiness,
                    CapabilityReadinessCheckV2::OwnedWorkerFault,
                    CapabilityReadinessCheckV2::ServingAdmission,
                ],
            },
            limits: value.limits,
            controls: CapabilityControlsV2::from_v1(value.controls),
        })
    }
}

impl CapabilityFeaturesV2 {
    fn from_v1(value: CapabilityFeatures) -> Self {
        Self {
            live_attach: value.live_attach,
            required_once_deny: value.required_once_deny,
            trusted_known_id_recovery: value.trusted_known_id_recovery,
            fenced_cancellation: value.fenced_cancellation,
            fenced_deletion: value.fenced_deletion,
            automatic_recovery_scan: value.automatic_recovery_scan,
            always_permissions: value.always_permissions,
            durable_event_replay: value.durable_event_replay,
            native_live_chunks: value.native_live_chunks,
            built_in_model_providers: value.built_in_model_providers,
            memory_candidates: value.memory_candidates,
            managed_workspaces: value.managed_workspaces,
            ag_ui: value.ag_ui,
            dynamic_graphs: value.dynamic_graphs,
            graceful_shutdown: value.graceful_shutdown,
            turn_deadline: value.turn_deadline,
            native_callback_sandbox: value.native_callback_sandbox,
            supervised_admission_drain: true,
        }
    }

    fn to_v1(&self) -> CapabilityFeatures {
        CapabilityFeatures {
            live_attach: self.live_attach,
            required_once_deny: self.required_once_deny,
            trusted_known_id_recovery: self.trusted_known_id_recovery,
            fenced_cancellation: self.fenced_cancellation,
            fenced_deletion: self.fenced_deletion,
            automatic_recovery_scan: self.automatic_recovery_scan,
            always_permissions: self.always_permissions,
            durable_event_replay: self.durable_event_replay,
            native_live_chunks: self.native_live_chunks,
            built_in_model_providers: self.built_in_model_providers,
            memory_candidates: self.memory_candidates,
            managed_workspaces: self.managed_workspaces,
            ag_ui: self.ag_ui,
            dynamic_graphs: self.dynamic_graphs,
            graceful_shutdown: self.graceful_shutdown,
            turn_deadline: self.turn_deadline,
            native_callback_sandbox: self.native_callback_sandbox,
        }
    }
}

impl CapabilityControlsV2 {
    fn from_v1(value: CapabilityControls) -> Self {
        Self {
            callback_timeout_ms: value.callback_timeout_ms,
            validator_timeout_ms: value.validator_timeout_ms,
            turn_deadline_ms: value.turn_deadline_ms,
            max_threads: value.max_threads,
            max_history_bytes: value.max_history_bytes,
            parked_retention_ms: value.parked_retention_ms,
            connection_limit: value.connection_limit,
            whole_process_memory_bytes: value.whole_process_memory_bytes,
            shutdown_default_wait_ms: 30000,
            shutdown_max_wait_ms: 300000,
        }
    }

    fn to_v1(&self) -> CapabilityControls {
        CapabilityControls {
            callback_timeout_ms: self.callback_timeout_ms,
            validator_timeout_ms: self.validator_timeout_ms,
            turn_deadline_ms: self.turn_deadline_ms,
            max_threads: self.max_threads,
            max_history_bytes: self.max_history_bytes,
            parked_retention_ms: self.parked_retention_ms,
            connection_limit: self.connection_limit,
            whole_process_memory_bytes: self.whole_process_memory_bytes,
        }
    }
}

impl CapabilityDescriptorV2 {
    /// Validate public assembly before publishing a version 2 descriptor.
    pub fn validate(&self) -> Result<(), CapabilityError> {
        if self.descriptor_version != 2 || !self.features.supervised_admission_drain {
            return Err(CapabilityError("invalid_capability_v2_mechanism"));
        }
        if self.readiness.checks
            != [
                CapabilityReadinessCheckV2::StoreReadiness,
                CapabilityReadinessCheckV2::OwnedWorkerFault,
                CapabilityReadinessCheckV2::ServingAdmission,
            ]
        {
            return Err(CapabilityError("invalid_capability_v2_readiness"));
        }
        if self.controls.shutdown_default_wait_ms != 30000
            || self.controls.shutdown_max_wait_ms != 300000
        {
            return Err(CapabilityError("invalid_capability_v2_waits"));
        }
        // Only versioned readiness metadata is replaced. Every unchanged value
        // reaches the frozen validator, including all old features and controls.
        CapabilityDescriptor {
            descriptor_version: 1,
            target_profile: self.target_profile,
            profile_status: self.profile_status,
            core_profile_frozen: self.core_profile_frozen,
            runtime_version: self.runtime_version.clone(),
            application: self.application.clone(),
            execution: self.execution.clone(),
            features: self.features.to_v1(),
            store: self.store.clone(),
            readiness: CapabilityReadiness {
                response: self.readiness.response,
                aggregation: CapabilityReadinessAggregation::StoreReadinessAndNoOwnedWorkerFault,
                checks: [
                    CapabilityReadinessCheck::StoreReadiness,
                    CapabilityReadinessCheck::OwnedWorkerFault,
                ],
            },
            limits: self.limits.clone(),
            controls: self.controls.to_v1(),
        }
        .validate()
    }
}
