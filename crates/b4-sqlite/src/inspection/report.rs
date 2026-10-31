use super::{InspectionError, InspectionReport};
use b4_contract::{ExecutionStatus, RecoveryPolicy, Snapshot, TaskStatus};
use serde_json::{json, Map, Value};
use std::io::Write;

pub(super) const EXECUTION_KEYS: [&str; 8] = [
    "accepted",
    "running",
    "waiting_for_approval",
    "completed",
    "failed",
    "cancel_requested",
    "cancelled",
    "recovery_blocked",
];
pub(super) const TASK_KEYS: [&str; 8] = [
    "ready",
    "waiting_for_approval",
    "in_flight",
    "succeeded",
    "failed",
    "denied",
    "cancelled",
    "outcome_unknown",
];
pub(super) const POLICY_KEYS: [&str; 4] = ["pure", "idempotent", "reconcilable", "unsafe"];
pub(super) const TABLE_KEYS: [&str; 6] = [
    "metadata",
    "threads",
    "executions",
    "journal",
    "approval_grants",
    "approval_acceptances",
];
const MAX_BLOCKED: usize = 64;
const MAX_REPORT: usize = 65_536;

pub(super) fn execution_index(status: ExecutionStatus) -> usize {
    match status {
        ExecutionStatus::Accepted => 0,
        ExecutionStatus::Running => 1,
        ExecutionStatus::WaitingForApproval => 2,
        ExecutionStatus::Completed => 3,
        ExecutionStatus::Failed => 4,
        ExecutionStatus::CancelRequested => 5,
        ExecutionStatus::Cancelled => 6,
        ExecutionStatus::RecoveryBlocked => 7,
    }
}

fn task_index(status: TaskStatus) -> usize {
    match status {
        TaskStatus::Ready => 0,
        TaskStatus::WaitingForApproval => 1,
        TaskStatus::InFlight => 2,
        TaskStatus::Succeeded => 3,
        TaskStatus::Failed => 4,
        TaskStatus::Denied => 5,
        TaskStatus::Cancelled => 6,
        TaskStatus::OutcomeUnknown => 7,
    }
}

fn policy_index(policy: RecoveryPolicy) -> usize {
    match policy {
        RecoveryPolicy::Pure => 0,
        RecoveryPolicy::Idempotent => 1,
        RecoveryPolicy::Reconcilable => 2,
        RecoveryPolicy::Unsafe => 3,
    }
}

fn increment(counter: &mut u64) -> Result<(), InspectionError> {
    *counter = counter
        .checked_add(1)
        .ok_or(InspectionError::LimitExceeded)?;
    Ok(())
}

fn counters<const N: usize>(keys: [&str; N], counts: [u64; N]) -> Value {
    Value::Object(
        keys.into_iter()
            .zip(counts)
            .map(|(key, count)| (key.to_owned(), Value::from(count)))
            .collect::<Map<_, _>>(),
    )
}

struct BlockedRow {
    status: usize,
    uncertain: bool,
    tasks: [u64; 8],
    policies: [u64; 4],
}

pub(super) struct Projection {
    pub(super) tables: [u64; 6],
    empty_schema4: bool,
    executions: [u64; 8],
    tasks: [u64; 8],
    policies: [u64; 4],
    uncertain: [u64; 4],
    blocked: Option<Vec<BlockedRow>>,
}

impl Projection {
    pub(super) fn new(blocked: bool) -> Self {
        Self {
            tables: [0; 6],
            empty_schema4: false,
            executions: [0; 8],
            tasks: [0; 8],
            policies: [0; 4],
            uncertain: [0; 4],
            blocked: blocked.then(Vec::new),
        }
    }

    pub(super) fn mark_empty_schema4(&mut self) {
        self.empty_schema4 = true;
    }

    pub(super) fn add(&mut self, snapshot: &Snapshot) -> Result<(), InspectionError> {
        let status = execution_index(snapshot.status);
        increment(&mut self.executions[status])?;
        let mut tasks = [0; 8];
        let mut policies = [0; 4];
        let mut has_uncertain = false;
        for batch in &snapshot.definition.batches {
            for spec in batch {
                let policy = policy_index(spec.recovery);
                increment(&mut policies[policy])?;
                increment(&mut self.policies[policy])?;
            }
        }
        for task in &snapshot.tasks {
            let task_status = task_index(task.status);
            increment(&mut tasks[task_status])?;
            increment(&mut self.tasks[task_status])?;
            if matches!(
                task.status,
                TaskStatus::InFlight | TaskStatus::OutcomeUnknown
            ) {
                has_uncertain = true;
                increment(&mut self.uncertain[policy_index(task.spec.recovery)])?;
            }
        }
        if let Some(rows) = &mut self.blocked {
            if matches!(
                snapshot.status,
                ExecutionStatus::Accepted
                    | ExecutionStatus::Running
                    | ExecutionStatus::WaitingForApproval
                    | ExecutionStatus::CancelRequested
                    | ExecutionStatus::RecoveryBlocked
            ) || has_uncertain
            {
                if rows.len() == MAX_BLOCKED {
                    return Err(InspectionError::LimitExceeded);
                }
                rows.push(BlockedRow {
                    status,
                    uncertain: has_uncertain,
                    tasks,
                    policies,
                });
            }
        }
        Ok(())
    }

    fn value(self) -> Value {
        let blocked = self.blocked.map(|rows| {
            let rows = rows.into_iter().enumerate().map(|(index, row)| json!({
                "ordinal": index + 1,
                "status": EXECUTION_KEYS[row.status],
                "recorded_recovery_blocked": row.status == 7,
                "recorded_approval_park": row.status == 2,
                "has_uncertain_task": row.uncertain,
                "initialized_tasks_by_status": counters(TASK_KEYS, row.tasks),
                "declared_definition_tasks_by_recovery_policy": counters(POLICY_KEYS, row.policies),
            })).collect::<Vec<_>>();
            json!({"selection": "recorded_unfinished_or_uncertain", "rows": rows})
        });
        let mut value = json!({
            "inspection_report_version": 1,
            "mode": "stopped_readonly",
            "redacted": true,
            "reader": {
                "package_version": env!("CARGO_PKG_VERSION"),
                "sqlite_library_version": rusqlite::version(),
                "snapshot_version": b4_contract::SNAPSHOT_VERSION,
                "store_schema_version": crate::LEGACY_SCHEMA_VERSION,
                "target_profile": "b4-ap-core/0",
                "profile_status": "native_subset",
                "capability_descriptor_version": 2,
                "core_profile_frozen": false,
            },
            "validation_scope": "schema_structure_and_execution_snapshots",
            "table_rows": counters(TABLE_KEYS, self.tables),
            "executions_by_status": counters(EXECUTION_KEYS, self.executions),
            "initialized_tasks_by_status": counters(TASK_KEYS, self.tasks),
            "declared_definition_tasks_by_recovery_policy": counters(POLICY_KEYS, self.policies),
            "recorded_uncertain_tasks_by_policy": counters(POLICY_KEYS, self.uncertain),
            "blocked": blocked,
        });
        if self.empty_schema4 {
            value["inspection_report_version"] = json!(2);
            value["reader"] = json!({
                "package_version": env!("CARGO_PKG_VERSION"),
                "sqlite_library_version": rusqlite::version(),
                "snapshot_version": b4_contract::SNAPSHOT_VERSION,
                "store_schema_version": crate::SCHEMA_VERSION,
                "target_profile": "b4-ap-core/0",
                "profile_status": "native_subset",
                "capability_descriptor_versions": [1, 2, 3],
                "core_profile_frozen": false,
            });
            value["validation_scope"] =
                json!("schema_structure_execution_snapshots_and_bounded_permission_records");
            // Each zero is established by the bounded source emptiness guards.
            value["table_rows"]["permission_acceptances"] = json!(0);
            value["table_rows"]["permission_decisions"] = json!(0);
            value["table_rows"]["permission_uses"] = json!(0);
            value["permission_authority"] = json!({
                "human_always_decisions": 0,
                "token_free_uses": 0,
            });
        }
        value
    }

    pub(super) fn encode(self) -> Result<InspectionReport, InspectionError> {
        encode(self.value(), MAX_REPORT)
    }
}

struct CappedWriter {
    bytes: Vec<u8>,
    cap: usize,
}

impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.cap.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("inspection output limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode(value: Value, cap: usize) -> Result<InspectionReport, InspectionError> {
    let mut writer = CappedWriter {
        bytes: Vec::new(),
        cap,
    };
    serde_json::to_writer(&mut writer, &value).map_err(|_| InspectionError::LimitExceeded)?;
    Ok(InspectionReport(writer.bytes))
}

#[cfg(test)]
mod tests;
