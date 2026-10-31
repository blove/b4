use crate::{
    host::Shared, lifecycle::OwnedShared, transport::*, Operation, Principal, ResourceContext,
};
use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use b4_contract::*;
use b4_runtime::{approval::ApprovalServiceConfig, Store, StoreKind};
use std::sync::atomic::Ordering;

enum Projection {
    Denied,
    Draining,
    Unavailable,
    Descriptor(Vec<u8>),
}

pub(crate) async fn read<S: Store + 'static>(
    shared: OwnedShared<S>,
    principal: Principal,
) -> Response {
    if !shared.supervisor.is_serving() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
    }
    // Keep the direct internal entry point under the same immutable app scope.
    if !valid_principal(&principal) || !crate::application::scope_allows(&shared, &principal) {
        return error(StatusCode::NOT_FOUND, "resource_unavailable");
    }
    let projection = shared
        .clone()
        .spawn(move |owned| {
            if !owned.supervisor.is_serving() {
                return Projection::Draining;
            }
            let context = ResourceContext {
                operation: Operation::ReadCapabilities,
                thread_id: None,
                origin: None,
                stored_route: None,
                requested_route: None,
            };
            if !owned.policy.authorize(&principal, &context) {
                return Projection::Denied;
            }
            if !owned.supervisor.is_serving() {
                return Projection::Draining;
            }
            // Policy may wait while another owned producer faults. Static discovery
            // metadata is published only after the aggregate readiness gate passes.
            if owned.fault.load(Ordering::Acquire) {
                return Projection::Unavailable;
            }
            if owned.store.readiness().is_err() {
                owned.fault.store(true, Ordering::Release);
                return Projection::Unavailable;
            }
            if !owned.supervisor.is_serving() {
                return Projection::Draining;
            }
            let Ok(descriptor) = descriptor(owned) else {
                owned.fault.store(true, Ordering::Release);
                return Projection::Unavailable;
            };
            if descriptor.validate().is_err() {
                owned.fault.store(true, Ordering::Release);
                return Projection::Unavailable;
            }
            let encoded = if owned.permission_uses.is_some() {
                match CapabilityDescriptorV3::permission_reuse(descriptor) {
                    Ok(descriptor) if descriptor.validate().is_ok() => {
                        if owned
                            .approval
                            .as_ref()
                            .is_some_and(|bridge| bridge.permission_approvals())
                        {
                            match CapabilityDescriptorV4::permission_approvals(descriptor) {
                                Ok(descriptor) if descriptor.validate().is_ok() => {
                                    if owned.permission_cancellation.is_some() {
                                        match CapabilityDescriptorV5::permission_entered_cancellation(descriptor) {
                                            Ok(descriptor) if descriptor.validate().is_ok() => {
                                                if owned.permission_ready_cancellation {
                                                    match CapabilityDescriptorV6::permission_ready_cancellation(descriptor) {
                                                        Ok(descriptor) if descriptor.validate().is_ok() => {
                                                            if owned.permission_withheld_cancellation {
                                                                match CapabilityDescriptorV7::permission_withheld_cancellation(descriptor) {
                                                                    Ok(descriptor) if descriptor.validate().is_ok() => {
                                                                        if owned.permission_deletion.is_some() {
                                                                            match CapabilityDescriptorV8::permission_settled_thread_deletion(descriptor) {
                                                                                Ok(descriptor) if descriptor.validate().is_ok() => serde_json::to_vec(&descriptor),
                                                                                _ => { owned.fault.store(true, Ordering::Release); return Projection::Unavailable; }
                                                                            }
                                                                        } else { serde_json::to_vec(&descriptor) }
                                                                    },
                                                                    _ => { owned.fault.store(true, Ordering::Release); return Projection::Unavailable; }
                                                                }
                                                            } else { serde_json::to_vec(&descriptor) }
                                                        },
                                                        _ => { owned.fault.store(true, Ordering::Release); return Projection::Unavailable; }
                                                    }
                                                } else { serde_json::to_vec(&descriptor) }
                                            },
                                            _ => {
                                                owned.fault.store(true, Ordering::Release);
                                                return Projection::Unavailable;
                                            }
                                        }
                                    } else {
                                        serde_json::to_vec(&descriptor)
                                    }
                                }
                                _ => {
                                    owned.fault.store(true, Ordering::Release);
                                    return Projection::Unavailable;
                                }
                            }
                        } else {
                            serde_json::to_vec(&descriptor)
                        }
                    }
                    _ => {
                        owned.fault.store(true, Ordering::Release);
                        return Projection::Unavailable;
                    }
                }
            } else {
                serde_json::to_vec(&descriptor)
            };
            let Ok(bytes) = encoded else {
                owned.fault.store(true, Ordering::Release);
                return Projection::Unavailable;
            };
            if bytes.len() > crate::parser::MAX_BODY_BYTES {
                owned.fault.store(true, Ordering::Release);
                return Projection::Unavailable;
            }
            if owned.fault.load(Ordering::Acquire) {
                return Projection::Unavailable;
            }
            if !owned.supervisor.is_serving() {
                return Projection::Draining;
            }
            Projection::Descriptor(bytes)
        })
        .await;
    // This response cut observes only supervisor state. It never reenters Store
    // after the owned metadata job has returned or when admission has closed.
    if !shared.supervisor.is_serving() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "host_draining");
    }
    match projection {
        Ok(Projection::Denied) => error(StatusCode::FORBIDDEN, "resource_unavailable"),
        Ok(Projection::Draining) => error(StatusCode::SERVICE_UNAVAILABLE, "host_draining"),
        Ok(Projection::Descriptor(bytes)) if !shared.fault.load(Ordering::Acquire) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            bytes,
        )
            .into_response(),
        _ => unavailable(&shared),
    }
}

fn descriptor<S: Store>(shared: &Shared<S>) -> Result<CapabilityDescriptorV2, CapabilityError> {
    let store = shared.store.capabilities();
    let application = shared.application.as_ref().map(|application| {
        let identity = application.identity();
        CapabilityApplication {
            id: identity.application.clone(),
            revision: identity.revision.clone(),
        }
    });
    let mode = shared
        .approval
        .as_ref()
        .map_or(CapabilityApprovalMode::Disabled, |bridge| {
            match bridge.service_config() {
                ApprovalServiceConfig::RequiredDurable => CapabilityApprovalMode::DurableRequired,
                ApprovalServiceConfig::VolatileFixture => CapabilityApprovalMode::VolatileFixture,
            }
        });
    let limits = &shared.config;
    CapabilityDescriptorV2::try_from(CapabilityDescriptor {
        descriptor_version: 1,
        target_profile: CapabilityTargetProfile::B4ApCore0,
        profile_status: CapabilityProfileStatus::NativeSubset,
        core_profile_frozen: false,
        runtime_version: env!("CARGO_PKG_VERSION").into(),
        execution: CapabilityExecution {
            mode: CapabilityExecutionMode::SingleProcess,
            registration: CapabilityRegistration::Immutable,
            native_callbacks: application.is_some(),
            approval_service_mode: mode,
        },
        application,
        features: CapabilityFeatures {
            live_attach: true,
            required_once_deny: shared.approval.is_some(),
            trusted_known_id_recovery: shared.approval.is_some(),
            fenced_cancellation: store.fenced_cancellation,
            fenced_deletion: store.fenced_deletion,
            ..CapabilityFeatures::default()
        },
        store: CapabilityStore {
            kind: match store.kind {
                StoreKind::Unspecified => CapabilityStoreKind::Unspecified,
                StoreKind::Memory => CapabilityStoreKind::Memory,
                StoreKind::Sqlite => CapabilityStoreKind::Sqlite,
            },
            durable_runtime: store.durable_runtime,
            durable_approvals: shared.approval_durable,
        },
        readiness: CapabilityReadiness {
            response: CapabilityReadinessResponse::StatusOnly,
            aggregation: CapabilityReadinessAggregation::StoreReadinessAndNoOwnedWorkerFault,
            checks: [
                CapabilityReadinessCheck::StoreReadiness,
                CapabilityReadinessCheck::OwnedWorkerFault,
            ],
        },
        limits: CapabilityLimits {
            body_bytes: crate::parser::MAX_BODY_BYTES as u64,
            json_depth: 64,
            active_runs: limits.max_active_runs as u64,
            concurrent_tasks: limits.max_concurrent_tasks as u64,
            concurrent_tasks_per_execution: limits.max_tasks_per_execution as u64,
            viewer_queue_frames: limits.viewer_queue_frames as u64,
            heartbeat_ms: limits.heartbeat_ms,
            input_bytes: limits.max_input_bytes as u64,
            output_bytes: limits.max_output_bytes as u64,
            max_attached_viewers: limits.max_attached_viewers as u64,
            turn_buffer_bytes: limits.turn_buffer_bytes as u64,
            turn_buffer_frames: limits.turn_buffer_frames as u64,
            viewer_queue_bytes: limits.viewer_queue_bytes as u64,
            attachment_snapshot_bytes: limits.attachment_snapshot_bytes as u64,
            program_routes: 64,
            program_tasks: 64,
            program_attempts: 64,
            completion_attempts: 8,
            task_failure_bytes: MAX_TASK_FAILURE_BYTES as u64,
            script_chunks: 64,
            script_delay_ms: 5000,
            approval_bytes: b4_runtime::approval::MAX_APPROVAL_BYTES as u64,
            approval_ttl_ms: shared
                .approval
                .as_ref()
                .map(|bridge| bridge.approval_ttl_ms()),
        },
        controls: CapabilityControls::default(),
    })
}
