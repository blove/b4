//! Explicit bridge to the SDK's atomic settled-history deletion operation.
use crate::{
    Authenticator, ConfigError, HttpHost, Operation, PermissionHostConfig, Principal,
    ResourceContext, ResourcePolicy, RouteProvenance,
};
use b4_runtime::{approval::*, permissions::*, ThreadDeletionTarget};
use std::sync::Arc;

pub(crate) trait PermissionDeletionBridge: Send + Sync {
    fn delete(
        &self,
        principal: &Principal,
        target: &ThreadDeletionTarget,
    ) -> Result<PermissionThreadDeletionConfirmation, PermissionError>;
}

struct Bridge<S> {
    store: Arc<S>,
    policy: Arc<dyn ResourcePolicy>,
    policy_revision: String,
}

struct Authority {
    principal: Principal,
    policy: Arc<dyn ResourcePolicy>,
    policy_revision: String,
}

impl PermissionThreadDeletionAuthority for Authority {
    fn authorize_delete(
        &self,
        context: &ApprovalContext,
        resource: &PermissionThreadDeletionResource,
        _target: &ThreadDeletionTarget,
    ) -> Result<(), PermissionError> {
        let view = resource.view();
        if context.actor != crate::approval::actor(&self.principal)
            || context.policy_revision != self.policy_revision
            || view.thread.origin.as_ref() != Some(&context.actor)
        {
            return Err(ApprovalError::Unauthorized.into());
        }
        // Historical destruction uses current resource policy, without routing
        // the captured definition through today's effect catalog or constraint.
        let resource_context = ResourceContext {
            operation: Operation::DeleteThread,
            thread_id: Some(view.thread.id.clone()),
            origin: view.thread.origin.clone(),
            stored_route: view.execution.as_ref().map(|execution| RouteProvenance {
                route: execution.snapshot.definition.route.clone(),
                revision: execution.snapshot.definition.revision.clone(),
            }),
            requested_route: None,
        };
        if !self.policy.authorize(&self.principal, &resource_context) {
            return Err(ApprovalError::Unauthorized.into());
        }
        Ok(())
    }
}

impl<S: PermissionStore + 'static> PermissionDeletionBridge for Bridge<S> {
    fn delete(
        &self,
        principal: &Principal,
        target: &ThreadDeletionTarget,
    ) -> Result<PermissionThreadDeletionConfirmation, PermissionError> {
        let context = ApprovalContext {
            actor: crate::approval::actor(principal),
            policy_revision: self.policy_revision.clone(),
        };
        let authority = Arc::new(Authority {
            principal: principal.clone(),
            policy: self.policy.clone(),
            policy_revision: self.policy_revision.clone(),
        });
        delete_settled_permission_thread(self.store.clone(), authority, &context, target)
    }
}

impl<S: PermissionStore + 'static> HttpHost<S> {
    /// Enables deletion of physically quiescent, fully settled permission history.
    pub fn new_with_permission_settled_thread_deletion(
        store: Arc<S>,
        auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>,
        config: PermissionHostConfig,
    ) -> Result<Self, ConfigError> {
        if !store.permission_settled_thread_deletion_atomic_durable() {
            return Err(ConfigError(
                "permission_settled_thread_deletion_durability_required",
            ));
        }
        let bridge = Arc::new(Bridge {
            store: store.clone(),
            policy: policy.clone(),
            policy_revision: config.approval.policy_revision.clone(),
        });
        let mut host =
            Self::new_with_permission_withheld_cancellation(store, auth, policy, config)?;
        let shared = Arc::get_mut(&mut host.shared)
            .ok_or(ConfigError("permission_settled_thread_deletion_shared"))?;
        shared.permission_deletion = Some(bridge);
        Ok(host)
    }
}
