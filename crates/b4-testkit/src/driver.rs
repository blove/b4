use crate::scenario::{
    validate_scenario, Scenario, ScenarioExpectations, ScenarioObservation, ScenarioStep,
    TaskExpectation,
};
use crate::{parse_scenario, ScriptedAdapter};
use b4_contract::*;
use b4_kernel::{new_execution, transition, validate_snapshot};
use std::collections::BTreeMap;

pub fn run_scenario_json(json: &str) -> Result<ScenarioObservation, String> {
    run_scenario(&parse_scenario(json)?)
}

pub fn run_scenario(scenario: &Scenario) -> Result<ScenarioObservation, String> {
    validate_scenario(scenario)?;
    let initial = &scenario.initial;
    let mut driver = Driver {
        snapshot: new_execution(
            initial.execution_id.clone(),
            initial.turn_id.clone(),
            initial.owner_generation,
            initial.definition.clone(),
            initial.values.clone(),
            TransitionFacts {
                now_ms: initial.now_ms,
            },
        )
        .map_err(|error| format!("initial execution: {error}"))?,
        adapter: ScriptedAdapter::new(scenario.effects.clone()),
        pending_results: BTreeMap::new(),
        snapshot_round_trips: 0,
        discarded_pending_results: 0,
    };
    for (index, step) in scenario.steps.iter().enumerate() {
        driver
            .step(step)
            .map_err(|error| format!("step {}: {error}", index + 1))?;
    }
    let observation = ScenarioObservation {
        name: scenario.name.clone(),
        evidence_class: scenario.evidence_class,
        status: driver.snapshot.status,
        values: driver.snapshot.values,
        effect_counts: driver.adapter.invocation_counts(),
        attempts_used: driver.snapshot.attempts_used,
        failure: driver.snapshot.failure,
        tasks: driver
            .snapshot
            .tasks
            .iter()
            .map(|task| {
                (
                    task.spec.id.clone(),
                    TaskExpectation {
                        status: task.status,
                        attempt_id: task.attempt_id.clone(),
                        approved: task.approved,
                    },
                )
            })
            .collect(),
        snapshot_round_trips: driver.snapshot_round_trips,
        discarded_pending_results: driver.discarded_pending_results,
    };
    check_expectations(&scenario.expect, &observation)?;
    Ok(observation)
}

struct Driver {
    snapshot: Snapshot,
    adapter: ScriptedAdapter,
    pending_results: BTreeMap<TaskId, (AttemptId, TaskResult)>,
    snapshot_round_trips: u64,
    discarded_pending_results: u64,
}

impl Driver {
    fn step(&mut self, step: &ScenarioStep) -> Result<(), String> {
        match step {
            ScenarioStep::Start { now_ms } => self.commit(KernelCommand::Start, *now_ms),
            ScenarioStep::Dispatch {
                task_id,
                attempt_id,
                now_ms,
            } => {
                self.commit(
                    KernelCommand::Dispatch {
                        task_id: task_id.clone(),
                        attempt_id: attempt_id.clone(),
                    },
                    *now_ms,
                )?;
                // Replace the proposed snapshot before the fake external effect runs.
                // This models ordering only; it is not a durable transaction.
                let task = self
                    .snapshot
                    .tasks
                    .iter()
                    .find(|task| &task.spec.id == task_id)
                    .ok_or_else(|| {
                        format!("dispatched task absent from committed snapshot: {task_id}")
                    })?;
                let result = self.adapter.invoke(&task.spec.effect_id)?;
                self.pending_results
                    .insert(task_id.clone(), (attempt_id.clone(), result));
                Ok(())
            }
            ScenarioStep::Complete { task_id, now_ms } => {
                let (attempt_id, result) = self
                    .pending_results
                    .get(task_id)
                    .cloned()
                    .ok_or_else(|| format!("missing pending result: {task_id}"))?;
                self.commit(
                    KernelCommand::Complete {
                        task_id: task_id.clone(),
                        attempt_id,
                        result,
                    },
                    *now_ms,
                )?;
                self.pending_results.remove(task_id);
                Ok(())
            }
            ScenarioStep::Park {
                task_id,
                interrupt_id,
                prompt,
                now_ms,
            } => self.commit(
                KernelCommand::Park {
                    task_id: task_id.clone(),
                    interrupt_id: interrupt_id.clone(),
                    prompt: prompt.clone(),
                },
                *now_ms,
            ),
            ScenarioStep::Resume {
                route,
                decisions,
                turn_id,
                now_ms,
            } => self.commit(
                KernelCommand::Resume {
                    route: route.clone(),
                    decisions: decisions.clone(),
                    turn_id: turn_id.clone(),
                },
                *now_ms,
            ),
            ScenarioStep::Recover {
                available_revision,
                turn_id,
                owner_generation,
                now_ms,
            } => self.commit(
                KernelCommand::Recover {
                    available_revision: available_revision.clone(),
                    turn_id: turn_id.clone(),
                    owner_generation: *owner_generation,
                },
                *now_ms,
            ),
            ScenarioStep::RestoreSnapshot => {
                // Retain the fake external ledger, but lose unrecorded adapter results.
                let encoded =
                    serde_json::to_vec(&self.snapshot).map_err(|error| error.to_string())?;
                let restored: Snapshot =
                    serde_json::from_slice(&encoded).map_err(|error| error.to_string())?;
                validate_snapshot(&restored)
                    .map_err(|error| format!("restored snapshot: {error}"))?;
                self.snapshot = restored;
                self.discarded_pending_results += self.pending_results.len() as u64;
                self.pending_results.clear();
                self.snapshot_round_trips += 1;
                Ok(())
            }
        }
    }

    fn commit(&mut self, command: KernelCommand, now_ms: u64) -> Result<(), String> {
        let plan = transition(&self.snapshot, command, TransitionFacts { now_ms })
            .map_err(|error| format!("kernel transition: {error}"))?;
        if plan.expected_revision != self.snapshot.revision {
            return Err("commit plan expected revision mismatch".into());
        }
        // Dispatchable tasks are eligibility information, never automatic invocations.
        self.snapshot = plan.snapshot;
        Ok(())
    }
}

fn check_expectations(
    expected: &ScenarioExpectations,
    observed: &ScenarioObservation,
) -> Result<(), String> {
    macro_rules! check {
        ($field:ident) => {
            if expected.$field != observed.$field {
                return Err(format!(
                    "expectation mismatch: {}; expected {:?}, observed {:?}",
                    stringify!($field),
                    expected.$field,
                    observed.$field
                ));
            }
        };
    }
    check!(status);
    check!(values);
    check!(effect_counts);
    check!(attempts_used);
    check!(failure);
    check!(tasks);
    check!(snapshot_round_trips);
    check!(discarded_pending_results);
    Ok(())
}
