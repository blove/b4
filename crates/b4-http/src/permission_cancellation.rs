//! Private proof of actual controlled callback entry and typed management port.
use crate::{control::RunControl, Principal};
use b4_runtime::{approval::*, permissions::*, *};
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum OriginKind {
    OriginalAlways,
    NewUse,
}
#[derive(Clone, Copy)]
pub(crate) enum CancellationKind {
    Ready(OriginKind),
    Entered(OriginKind),
    Withheld(OriginKind),
}
#[allow(clippy::large_enum_variant)] // One bounded management result retains its owned confirmation inline.
pub(crate) enum CancellationCommit {
    Ready(PermissionReadyCancellationConfirmation),
    Entered(StoredExecution),
}
struct Registration {
    control: Weak<RunControl>,
    context: InvocationContext,
    kind: OriginKind,
    initial: Option<StoredExecution>,
    entered: Option<EffectInvocation>,
}
#[derive(Default)]
pub(crate) struct Tracker {
    active: Mutex<Option<Registration>>,
}
pub(crate) struct RegistrationGuard<'a> {
    tracker: &'a Tracker,
}
impl Drop for RegistrationGuard<'_> {
    fn drop(&mut self) {
        *self
            .tracker
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}
impl Tracker {
    pub(crate) fn register(
        &self,
        control: &Arc<RunControl>,
        context: InvocationContext,
        kind: OriginKind,
        initial: Option<&StoredExecution>,
    ) -> Result<RegistrationGuard<'_>, crate::worker::WorkerError> {
        let mut slot = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_some() {
            return Err(crate::worker::WorkerError::Rejected(
                "permission_entry_occupied",
            ));
        }
        *slot = Some(Registration {
            control: Arc::downgrade(control),
            context,
            kind,
            initial: initial.cloned(),
            entered: None,
        });
        Ok(RegistrationGuard { tracker: self })
    }
    fn record(&self, invocation: &EffectInvocation, context: &InvocationContext) {
        let mut slot = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(registration) = slot.as_mut() else {
            return;
        };
        let Some(control) = registration.control.upgrade() else {
            return;
        };
        let identity = &control.identity;
        if registration.context == *context
            && identity.thread_id == context.thread_id
            && identity.thread_generation == context.thread_generation
            && identity.execution_id == invocation.execution_id
            && identity.turn_id == invocation.turn_id
            && identity.owner_generation == invocation.owner_generation
            && registration.entered.is_none()
        {
            registration.entered = Some(invocation.clone());
        }
    }
    pub(crate) fn classify(
        &self,
        control: &Arc<RunControl>,
        view: &ThreadView,
        ready: bool,
        withheld: bool,
    ) -> Option<CancellationKind> {
        if ready {
            let slot = self.active.lock().ok()?;
            let registration = slot.as_ref()?;
            let actual = registration.control.upgrade()?;
            let initial = registration.initial.as_ref()?;
            let s = &initial.snapshot;
            if Arc::ptr_eq(&actual, control)
                && control.identity.matches(view)
                && registration.entered.is_none()
                && view.execution.as_ref() == Some(initial)
                && view.thread.origin.as_ref() == Some(&registration.context.origin)
                && s.status == b4_contract::ExecutionStatus::Running
                && s.revision
                    == match registration.kind {
                        OriginKind::OriginalAlways => 3,
                        OriginKind::NewUse => 2,
                    }
                && s.tasks.len() == 1
                && s.tasks[0].status == b4_contract::TaskStatus::Ready
                && s.tasks[0].attempt_id.is_none()
                && s.output.is_none()
                && s.failure.is_none()
                && s.tasks[0].result.is_none()
                && s.tasks[0].failure.is_none()
                && s.attempts_used == 0
                && s.attempt_history.is_empty()
            {
                return Some(CancellationKind::Ready(registration.kind));
            }
        }
        if let Some(kind) = self.entered(control, view) {
            return Some(CancellationKind::Entered(kind));
        }
        if withheld {
            let slot = self.active.lock().ok()?;
            let registration = slot.as_ref()?;
            let actual = registration.control.upgrade()?;
            let execution = view.execution.as_ref()?;
            let snapshot = &execution.snapshot;
            if Arc::ptr_eq(&actual, control)
                && control.identity.matches(view)
                && view.thread.origin.as_ref() == Some(&registration.context.origin)
                && registration.entered.is_none()
                && snapshot.status == b4_contract::ExecutionStatus::Running
                && snapshot.revision
                    == match registration.kind {
                        OriginKind::OriginalAlways => 4,
                        OriginKind::NewUse => 3,
                    }
                && snapshot.tasks.len() == 1
                && snapshot.tasks[0].status == b4_contract::TaskStatus::InFlight
                && snapshot.tasks[0].attempt_id.is_some()
            {
                return Some(CancellationKind::Withheld(registration.kind));
            }
        }
        None
    }
    pub(crate) fn entered(
        &self,
        control: &Arc<RunControl>,
        view: &ThreadView,
    ) -> Option<OriginKind> {
        let (actual, context, kind, invocation) = {
            let slot = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let registration = slot.as_ref()?;
            (
                registration.control.clone(),
                registration.context.clone(),
                registration.kind,
                registration.entered.clone()?,
            )
        };
        let actual = actual.upgrade()?;
        let execution = view.execution.as_ref()?;
        let task = execution.snapshot.tasks.first()?;
        (Arc::ptr_eq(&actual, control)
            && control.identity.matches(view)
            && context.thread_id == view.thread.id
            && context.thread_generation == view.thread.generation
            && view.thread.origin.as_ref() == Some(&context.origin)
            && invocation.execution_id == execution.snapshot.execution_id
            && invocation.turn_id == execution.snapshot.turn_id
            && invocation.owner_generation == execution.snapshot.owner_generation
            && invocation.task_id == task.spec.id
            && invocation.effect_id == task.spec.effect_id
            && task.attempt_id.as_ref() == Some(&invocation.attempt_id)
            && task.status == b4_contract::TaskStatus::InFlight
            && execution.snapshot.status == b4_contract::ExecutionStatus::Running
            && execution.snapshot.revision
                == match kind {
                    OriginKind::OriginalAlways => 4,
                    OriginKind::NewUse => 3,
                })
        .then_some(kind)
    }
    pub(crate) fn wrap(self: &Arc<Self>, callback: Arc<dyn NativeEffect>) -> Arc<dyn NativeEffect> {
        Arc::new(EnteredCallback {
            tracker: self.clone(),
            callback,
        })
    }
}
struct EnteredCallback {
    tracker: Arc<Tracker>,
    callback: Arc<dyn NativeEffect>,
}
impl NativeEffect for EnteredCallback {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.callback.invoke(invocation, context)
    }
    fn invoke_controlled(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.tracker.record(invocation, context);
        self.callback
            .invoke_controlled(invocation, context, observer)
    }
}

pub(crate) trait CancellationBridge: Send + Sync {
    fn classify(&self, control: &Arc<RunControl>, view: &ThreadView) -> Option<CancellationKind>;
    fn authorize_withheld(
        &self,
        principal: &Principal,
        view: &ThreadView,
        target: &RunCancellationTarget,
    ) -> Result<(), PermissionError>;
    fn cancel(
        &self,
        principal: &Principal,
        kind: CancellationKind,
        target: &RunCancellationTarget,
        facts: b4_contract::TransitionFacts,
    ) -> Result<CancellationCommit, PermissionError>;
}

/// Management policy is evaluated on the complete current resource and never
/// borrows the effect continuation's RunStream authority or tool constraint.
pub(crate) struct Authority(pub(crate) crate::approval::Authority);
impl PermissionCancellationAuthority for Authority {
    fn authorize_cancel(
        &self,
        context: &ApprovalContext,
        resource: &ApprovalResource,
        target: &RunCancellationTarget,
    ) -> Result<(), PermissionError> {
        if self.0.operation != crate::Operation::CancelRun
            || RunCancellationTarget::from_view(&ThreadView {
                thread: resource.thread.clone(),
                execution: Some(resource.execution.clone()),
            })
            .map_err(PermissionError::Store)?
                != *target
        {
            return Err(ApprovalError::Unauthorized.into());
        }
        if context.actor != crate::approval::actor(&self.0.principal)
            || context.policy_revision != self.0.policy_revision
            || resource.thread.origin.as_ref() != Some(&context.actor)
        {
            return Err(ApprovalError::Unauthorized.into());
        }
        let definition = &resource.execution.snapshot.definition;
        let provenance = crate::RouteProvenance {
            route: definition.route.clone(),
            revision: definition.revision.clone(),
        };
        let management = crate::ResourceContext {
            operation: crate::Operation::CancelRun,
            thread_id: Some(resource.thread.id.clone()),
            origin: resource.thread.origin.clone(),
            stored_route: Some(provenance.clone()),
            requested_route: Some(provenance),
        };
        if !self.0.policy.authorize(&self.0.principal, &management) {
            return Err(ApprovalError::Unauthorized.into());
        }
        crate::route::recorded(self.0.application.as_ref(), &self.0.routes, definition)
            .ok_or(ApprovalError::RouteMismatch)?;
        Ok(())
    }
}
