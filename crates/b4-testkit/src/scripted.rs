use b4_contract::{EffectId, TaskResult};
use std::collections::BTreeMap;

/// This fake adapter has no provider, transport, or fallback execution path.

#[derive(Debug)]
pub struct ScriptedAdapter {
    fixtures: BTreeMap<EffectId, TaskResult>,
    invocation_counts: BTreeMap<EffectId, u64>,
}

impl ScriptedAdapter {
    pub fn new(fixtures: BTreeMap<EffectId, TaskResult>) -> Self {
        Self {
            fixtures,
            invocation_counts: BTreeMap::new(),
        }
    }

    pub fn invoke(&mut self, effect_id: &EffectId) -> Result<TaskResult, String> {
        *self.invocation_counts.entry(effect_id.clone()).or_default() += 1;
        self.fixtures
            .get(effect_id)
            .cloned()
            .ok_or_else(|| format!("missing effect fixture: {effect_id}"))
    }

    pub fn invocation_counts(&self) -> BTreeMap<EffectId, u64> {
        self.invocation_counts.clone()
    }
}
