use b4_contract::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub const SCENARIO_VERSION: u32 = 1;
pub const MAX_SCENARIO_TASKS: usize = 8;
pub const MAX_SCENARIO_STEPS: usize = 128;

/// An authored native requirement is not an observation of another runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClass {
    NativeRequirement,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub version: u32,
    pub name: String,
    pub evidence_class: EvidenceClass,
    pub description: String,
    pub initial: InitialExecution,
    pub effects: BTreeMap<EffectId, TaskResult>,
    pub steps: Vec<ScenarioStep>,
    pub expect: ScenarioExpectations,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialExecution {
    pub execution_id: ExecutionId,
    pub turn_id: TurnId,
    pub owner_generation: u64,
    pub now_ms: u64,
    pub definition: Definition,
    pub values: Map<String, Value>,
}

/// Only these bounded driver operations can appear in a version-1 fixture.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScenarioStep {
    Start {
        now_ms: u64,
    },
    Dispatch {
        task_id: TaskId,
        attempt_id: AttemptId,
        now_ms: u64,
    },
    Complete {
        task_id: TaskId,
        now_ms: u64,
    },
    Park {
        task_id: TaskId,
        interrupt_id: InterruptId,
        prompt: Value,
        now_ms: u64,
    },
    Resume {
        route: RouteKey,
        decisions: Vec<Decision>,
        turn_id: TurnId,
        now_ms: u64,
    },
    RestoreSnapshot,
    Recover {
        available_revision: ExecutionRevision,
        turn_id: TurnId,
        owner_generation: u64,
        now_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExpectation {
    pub status: TaskStatus,
    pub attempt_id: Option<AttemptId>,
    pub approved: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioExpectations {
    pub status: ExecutionStatus,
    pub values: Map<String, Value>,
    pub effect_counts: BTreeMap<EffectId, u64>,
    pub attempts_used: u32,
    pub failure: Option<String>,
    pub tasks: BTreeMap<TaskId, TaskExpectation>,
    pub snapshot_round_trips: u64,
    pub discarded_pending_results: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioObservation {
    pub name: String,
    pub evidence_class: EvidenceClass,
    pub status: ExecutionStatus,
    pub values: Map<String, Value>,
    pub effect_counts: BTreeMap<EffectId, u64>,
    pub attempts_used: u32,
    pub failure: Option<String>,
    pub tasks: BTreeMap<TaskId, TaskExpectation>,
    pub snapshot_round_trips: u64,
    pub discarded_pending_results: u64,
}

pub fn parse_scenario(json: &str) -> Result<Scenario, String> {
    let scenario: Scenario = serde_json::from_str(json).map_err(|error| error.to_string())?;
    validate_scenario(&scenario)?;
    Ok(scenario)
}

pub(crate) fn validate_scenario(scenario: &Scenario) -> Result<(), String> {
    if scenario.version != SCENARIO_VERSION {
        return Err(format!(
            "unsupported scenario version {}; supported version is {SCENARIO_VERSION}",
            scenario.version
        ));
    }
    if scenario.initial.definition.batches.iter().flatten().count() > MAX_SCENARIO_TASKS {
        return Err(format!("scenario task limit is {MAX_SCENARIO_TASKS}"));
    }
    if scenario.steps.len() > MAX_SCENARIO_STEPS {
        return Err(format!("scenario step limit is {MAX_SCENARIO_STEPS}"));
    }
    Ok(())
}
