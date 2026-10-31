use super::*;
use crate::{EffectInvocation, EffectOutcome};
use b4_contract::{Definition, EffectId, ExecutionRevision, RouteKey};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, sync::Arc};

/// Immutable trusted application assembly. Lookup does not authorize execution.
pub struct NativeApplication {
    identity: ApplicationIdentity,
    routes: BTreeMap<RouteKey, RouteRegistration>,
    historical: BTreeMap<RouteKey, BTreeMap<ExecutionRevision, RouteRegistration>>,
}
impl NativeApplication {
    pub fn new(
        identity: ApplicationIdentity,
        routes: Vec<RouteRegistration>,
    ) -> Result<Self, ApplicationConfigError> {
        validation::validate(&identity, &routes)?;
        Ok(Self {
            identity,
            routes: routes
                .into_iter()
                .map(|r| (r.definition.route.clone(), r))
                .collect(),
            historical: BTreeMap::new(),
        })
    }
    /// Assembles bounded current and approval-free historical registrations.
    ///
    /// A route/revision pair is unique across both sets. Selection also checks
    /// the complete Definition; assembly and lookup do not authorize execution.
    pub fn new_with_history(
        identity: ApplicationIdentity,
        current: Vec<RouteRegistration>,
        historical: Vec<RouteRegistration>,
    ) -> Result<Self, ApplicationConfigError> {
        validation::validate_history(&identity, &current, &historical)?;
        let mut retained: BTreeMap<RouteKey, BTreeMap<ExecutionRevision, RouteRegistration>> =
            BTreeMap::new();
        for registration in historical {
            retained
                .entry(registration.definition.route.clone())
                .or_default()
                .insert(registration.definition.revision.clone(), registration);
        }
        Ok(Self {
            identity,
            routes: current
                .into_iter()
                .map(|r| (r.definition.route.clone(), r))
                .collect(),
            historical: retained,
        })
    }
    /// Clones current and historical registrations, replacing only native callbacks.
    /// Metadata and shared input validators retain their existing identities.
    pub fn map_native_callbacks<F>(&self, mut wrap: F) -> Self
    where
        F: FnMut(Arc<dyn NativeEffect>) -> Arc<dyn NativeEffect>,
    {
        let mut map_registration = |registration: &RouteRegistration| {
            let mut mapped = registration.clone();
            for binding in mapped.effects.values_mut() {
                binding.callback = wrap(Arc::clone(&binding.callback));
            }
            mapped
        };
        let routes = self
            .routes
            .iter()
            .map(|(route, registration)| (route.clone(), map_registration(registration)))
            .collect();
        let historical = self
            .historical
            .iter()
            .map(|(route, revisions)| {
                let revisions = revisions
                    .iter()
                    .map(|(revision, registration)| {
                        (revision.clone(), map_registration(registration))
                    })
                    .collect();
                (route.clone(), revisions)
            })
            .collect();
        Self {
            identity: self.identity.clone(),
            routes,
            historical,
        }
    }

    pub fn identity(&self) -> &ApplicationIdentity {
        &self.identity
    }
    pub fn definitions(&self) -> impl Iterator<Item = &Definition> {
        self.routes.values().map(|r| &r.definition)
    }
    pub fn definition(&self, route: &RouteKey) -> Option<&Definition> {
        self.routes.get(route).map(|r| &r.definition)
    }
    /// Finds an exact recorded current or historical Definition without authority.
    pub fn definition_exact(&self, recorded: &Definition) -> Option<&Definition> {
        self.selected(recorded).map(|r| &r.definition)
    }
    fn selected(&self, definition: &Definition) -> Option<&RouteRegistration> {
        self.routes
            .get(&definition.route)
            .filter(|r| r.definition == *definition)
            .or_else(|| {
                self.historical
                    .get(&definition.route)?
                    .get(&definition.revision)
                    .filter(|r| r.definition == *definition)
            })
    }
    pub fn validate_input(
        &self,
        selected: &Definition,
        input: &Value,
    ) -> Result<(), ApplicationInputError> {
        let route = self
            .selected(selected)
            .ok_or(ApplicationInputError::DefinitionUnavailable)?;
        match &route.input_validator {
            Some(validator) => validator
                .validate(input)
                .map_err(|_| ApplicationInputError::InvalidInput),
            None => Ok(()),
        }
    }
    pub fn output_kind(&self, selected: &Definition, effect: &EffectId) -> Option<OutputKind> {
        self.selected(selected)?
            .effects
            .get(effect)
            .map(|b| b.output_kind)
    }
    fn invocation_binding(
        &self,
        selected: &Definition,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<&EffectBinding, NativeInvocationError> {
        let registered = self
            .selected(selected)
            .ok_or(NativeInvocationError::DefinitionUnavailable)?;
        if context.origin.application != self.identity.application
            || context.application_revision != self.identity.revision
            || context.execution_revision != selected.revision
            || !validation::label(&context.origin.tenant)
            || !validation::label(&context.origin.principal)
            || !validation::label(&context.policy_revision)
            || call.owner_generation == 0
        {
            return Err(NativeInvocationError::ContextMismatch);
        }
        if !selected
            .batches
            .iter()
            .flatten()
            .any(|task| task.id == call.task_id && task.effect_id == call.effect_id)
        {
            return Err(NativeInvocationError::TaskEffectMismatch);
        }
        registered
            .effects
            .get(&call.effect_id)
            .ok_or(NativeInvocationError::TaskEffectMismatch)
    }

    /// Invokes only the existing direct entry, even when a controlled override exists.
    pub fn invoke(
        &self,
        selected: &Definition,
        call: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<EffectOutcome, NativeInvocationError> {
        let binding = self.invocation_binding(selected, call, context)?;
        match binding
            .callback
            .invoke(call, context)
            .map_err(|_| NativeInvocationError::Uncertain)?
        {
            NativeOutcome::Succeeded(result)
                if binding.output_kind == OutputKind::Void && !result.output.is_null() =>
            {
                Err(NativeInvocationError::InvalidOutput)
            }
            NativeOutcome::Succeeded(result) => Ok(EffectOutcome::Succeeded(result)),
            NativeOutcome::Failed(reason) => Ok(EffectOutcome::Failed(reason)),
        }
    }

    /// Invokes the optional cooperative entry with the same selection/output checks.
    ///
    /// A cancellation acknowledgement while the supplied observer is clear returns
    /// static `Uncertain`. Trusted callers still own exact durable authorization.
    pub fn invoke_controlled(
        &self,
        selected: &Definition,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, NativeInvocationError> {
        let binding = self.invocation_binding(selected, call, context)?;
        let outcome = binding
            .callback
            .invoke_controlled(call, context, observer)
            .map_err(|_| NativeInvocationError::Uncertain)?;
        Self::checked_controlled_outcome(binding, observer, outcome)
    }

    /// Invokes the optional streaming entry with the same selection/output checks.
    /// Transient chunks do not change the final outcome or authorize execution.
    pub fn invoke_streaming(
        &self,
        selected: &Definition,
        call: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
        sink: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, NativeInvocationError> {
        let binding = self.invocation_binding(selected, call, context)?;
        let outcome = binding
            .callback
            .invoke_streaming(call, context, observer, sink)
            .map_err(|_| NativeInvocationError::Uncertain)?;
        Self::checked_controlled_outcome(binding, observer, outcome)
    }

    fn checked_controlled_outcome(
        binding: &EffectBinding,
        observer: &NativeCancellationObserver,
        outcome: ControlledNativeOutcome,
    ) -> Result<ControlledNativeOutcome, NativeInvocationError> {
        match outcome {
            ControlledNativeOutcome::Settled(NativeOutcome::Succeeded(result))
                if binding.output_kind == OutputKind::Void && !result.output.is_null() =>
            {
                Err(NativeInvocationError::InvalidOutput)
            }
            ControlledNativeOutcome::Cancelled if !observer.cancellation_requested() => {
                Err(NativeInvocationError::Uncertain)
            }
            outcome => Ok(outcome),
        }
    }
}
impl fmt::Debug for NativeApplication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeApplication")
            .field("identity", &self.identity)
            .field("route_count", &self.routes.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod history_tests;
