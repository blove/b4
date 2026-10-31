//! Deterministic native fixture execution for the foundation profile.
mod driver;
mod scenario;
mod scripted;

pub use driver::{run_scenario, run_scenario_json};
pub use scenario::{
    parse_scenario, EvidenceClass, InitialExecution, Scenario, ScenarioExpectations,
    ScenarioObservation, ScenarioStep, TaskExpectation, MAX_SCENARIO_STEPS, MAX_SCENARIO_TASKS,
    SCENARIO_VERSION,
};
pub use scripted::ScriptedAdapter;

pub const FOUNDATION_SCENARIOS: &[&str] = &[
    include_str!("../../../conformance/scenarios/parallel_approval_snapshot_round_trip.json"),
    include_str!("../../../conformance/scenarios/uncertain_unsafe_effect.json"),
];
