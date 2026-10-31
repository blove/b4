use crate::{
    AttemptId, EffectId, ExecutionId, ExecutionRevision, InterruptId, RouteKey, TaskId, TurnId,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::fmt;

pub const SNAPSHOT_VERSION: u32 = 1;
pub const MAX_TASK_FAILURE_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Accepted,
    Running,
    WaitingForApproval,
    Completed,
    Failed,
    CancelRequested,
    Cancelled,
    RecoveryBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Ready,
    WaitingForApproval,
    InFlight,
    Succeeded,
    Failed,
    Denied,
    Cancelled,
    OutcomeUnknown,
}

/// Declared effect behavior, to be qualified by a future execution adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPolicy {
    Pure,
    Idempotent,
    Reconcilable,
    Unsafe,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    pub id: TaskId,
    pub effect_id: EffectId,
    pub recovery: RecoveryPolicy,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    pub route: RouteKey,
    pub revision: ExecutionRevision,
    pub batches: Vec<Vec<TaskSpec>>,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Snapshot {
    pub schema_version: u32,
    pub revision: u64,
    pub execution_id: ExecutionId,
    pub turn_id: TurnId,
    pub turn_history: Vec<TurnId>,
    pub attempt_history: Vec<AttemptId>,
    pub owner_generation: u64,
    pub definition: Definition,
    pub status: ExecutionStatus,
    pub batch_index: usize,
    pub values: Map<String, Value>,
    pub tasks: Vec<TaskRecord>,
    pub attempts_used: u32,
    pub updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVersionError {
    pub found: u32,
    pub supported: u32,
}

impl fmt::Display for SnapshotVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unsupported snapshot version {}; supported version is {}",
            self.found, self.supported
        )
    }
}

impl std::error::Error for SnapshotVersionError {}

impl Snapshot {
    pub fn validate_version(&self) -> Result<(), SnapshotVersionError> {
        if self.schema_version == SNAPSHOT_VERSION {
            Ok(())
        } else {
            Err(SnapshotVersionError {
                found: self.schema_version,
                supported: SNAPSHOT_VERSION,
            })
        }
    }
}

impl<'de> Deserialize<'de> for Snapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Representation {
            schema_version: u32,
            revision: u64,
            execution_id: ExecutionId,
            turn_id: TurnId,
            turn_history: Vec<TurnId>,
            attempt_history: Vec<AttemptId>,
            owner_generation: u64,
            definition: Definition,
            status: ExecutionStatus,
            batch_index: usize,
            values: Map<String, Value>,
            tasks: Vec<TaskRecord>,
            attempts_used: u32,
            updated_at_ms: u64,
            #[serde(default, deserialize_with = "present_output")]
            output: Option<Value>,
            failure: Option<String>,
        }
        let value = Representation::deserialize(deserializer)?;
        let snapshot = Self {
            schema_version: value.schema_version,
            revision: value.revision,
            execution_id: value.execution_id,
            turn_id: value.turn_id,
            turn_history: value.turn_history,
            attempt_history: value.attempt_history,
            owner_generation: value.owner_generation,
            definition: value.definition,
            status: value.status,
            batch_index: value.batch_index,
            values: value.values,
            tasks: value.tasks,
            attempts_used: value.attempts_used,
            updated_at_ms: value.updated_at_ms,
            output: value.output,
            failure: value.failure,
        };
        snapshot
            .validate_version()
            .map_err(serde::de::Error::custom)?;
        Ok(snapshot)
    }
}

fn present_output<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRecord {
    pub spec: TaskSpec,
    pub status: TaskStatus,
    pub input: Map<String, Value>,
    pub attempt_id: Option<AttemptId>,
    pub result: Option<TaskResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    pub interrupt: Option<PendingApproval>,
    pub approved: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingApproval {
    pub id: InterruptId,
    pub prompt: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskResult {
    pub output: Value,
    pub writes: Vec<StateWrite>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateWrite {
    Replace { field: String, value: Value },
    Append { field: String, items: Vec<Value> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub interrupt_id: InterruptId,
    pub decision: ApprovalDecision,
}

/// `Always` requires a future permission-policy layer for lasting scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Once,
    Always,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum KernelCommand {
    Start,
    Dispatch {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    Complete {
        task_id: TaskId,
        attempt_id: AttemptId,
        result: TaskResult,
    },
    FailAttempt {
        task_id: TaskId,
        attempt_id: AttemptId,
        failure: String,
    },
    Park {
        task_id: TaskId,
        interrupt_id: InterruptId,
        prompt: Value,
    },
    Resume {
        route: RouteKey,
        decisions: Vec<Decision>,
        turn_id: TurnId,
    },
    Cancel {
        turn_id: TurnId,
        owner_generation: u64,
    },
    CancelAttempt {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    Recover {
        available_revision: ExecutionRevision,
        turn_id: TurnId,
        owner_generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionFacts {
    pub now_ms: u64,
}

/// A proposal to compare-and-swap in a future store before dispatching effects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitPlan {
    pub expected_revision: u64,
    pub snapshot: Snapshot,
    /// Ready task candidates, not external-effect dispatch instructions.
    /// A host must authorize the candidate, propose `KernelCommand::Dispatch`
    /// with a fresh attempt identity, and commit its `InFlight` snapshot before
    /// invoking the effect. The Dispatch plan need not list that task here.
    pub dispatchable: Vec<TaskId>,
}
