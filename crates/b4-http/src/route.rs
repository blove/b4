use crate::{ScriptEffect, ScriptRoute};
use b4_contract::{Definition, EffectId, RouteKey};
use b4_runtime::{NativeApplication, OutputKind};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub(crate) struct RegisteredRoute {
    pub definition: Definition,
    source: RouteSource,
}
#[derive(Clone)]
enum RouteSource {
    Scripted(Arc<BTreeMap<EffectId, ScriptEffect>>),
    Native(Arc<NativeApplication>),
}

/// Select only the exact recorded executable. Current routes remain the source
/// of new work and scripted registrations.
pub(crate) fn recorded(
    application: Option<&Arc<NativeApplication>>,
    current: &BTreeMap<RouteKey, RegisteredRoute>,
    definition: &Definition,
) -> Option<RegisteredRoute> {
    match application {
        Some(application) => application
            .definition_exact(definition)
            .map(|definition| RegisteredRoute::native(application.clone(), definition.clone())),
        None => current
            .get(&definition.route)
            .filter(|route| route.definition == *definition)
            .cloned(),
    }
}
impl RegisteredRoute {
    pub fn scripted(route: ScriptRoute) -> Self {
        Self {
            definition: route.definition,
            source: RouteSource::Scripted(Arc::new(route.effects)),
        }
    }
    pub fn native(application: Arc<NativeApplication>, definition: Definition) -> Self {
        Self {
            definition,
            source: RouteSource::Native(application),
        }
    }
    pub fn application(&self) -> Option<&Arc<NativeApplication>> {
        match &self.source {
            RouteSource::Native(application) => Some(application),
            _ => None,
        }
    }
    pub fn script_effect(&self, effect: &EffectId) -> Option<&ScriptEffect> {
        match &self.source {
            RouteSource::Scripted(effects) => effects.get(effect),
            _ => None,
        }
    }
    pub fn output_kind(&self, effect: &EffectId) -> Option<OutputKind> {
        match &self.source {
            RouteSource::Native(application) => application.output_kind(&self.definition, effect),
            RouteSource::Scripted(effects) => effects.get(effect).map(|effect| {
                if matches!(effect, ScriptEffect::Void) {
                    OutputKind::Void
                } else {
                    OutputKind::Value
                }
            }),
        }
    }
}
