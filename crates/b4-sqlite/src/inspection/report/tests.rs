use super::*;

#[test]
fn maximum_fixed_report_width_fits_production_cap_and_exact_serializer_boundary() {
    let mut projection = Projection::new(true);
    projection.tables = [u64::MAX; 6];
    projection.executions = [u64::MAX; 8];
    projection.tasks = [u64::MAX; 8];
    projection.policies = [u64::MAX; 4];
    projection.uncertain = [u64::MAX; 4];
    projection.blocked = Some(
        (0..64)
            .map(|_| BlockedRow {
                status: 2,
                uncertain: false,
                tasks: [u64::MAX; 8],
                policies: [u64::MAX; 4],
            })
            .collect(),
    );
    let value = projection.value();
    let width = serde_json::to_vec(&value).unwrap().len();
    assert_eq!(width, 41_171);
    // Conservative fixed-width bound also allows two digits for ordinals1..9
    // and the longer false token for the status-implied approval boolean.
    assert_eq!(width + 9 + 64, 41_244);
    assert!(width < MAX_REPORT);
    assert_eq!(encode(value.clone(), width).unwrap().as_ref().len(), width);
    assert_eq!(
        encode(value, width - 1).err(),
        Some(InspectionError::LimitExceeded)
    );
}

#[test]
fn every_execution_task_and_policy_maps_to_all_fixed_keys() {
    for (index, status) in [
        ExecutionStatus::Accepted,
        ExecutionStatus::Running,
        ExecutionStatus::WaitingForApproval,
        ExecutionStatus::Completed,
        ExecutionStatus::Failed,
        ExecutionStatus::CancelRequested,
        ExecutionStatus::Cancelled,
        ExecutionStatus::RecoveryBlocked,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(execution_index(status), index);
    }
    for (index, status) in [
        TaskStatus::Ready,
        TaskStatus::WaitingForApproval,
        TaskStatus::InFlight,
        TaskStatus::Succeeded,
        TaskStatus::Failed,
        TaskStatus::Denied,
        TaskStatus::Cancelled,
        TaskStatus::OutcomeUnknown,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(task_index(status), index);
    }
    for (index, policy) in [
        RecoveryPolicy::Pure,
        RecoveryPolicy::Idempotent,
        RecoveryPolicy::Reconcilable,
        RecoveryPolicy::Unsafe,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(policy_index(policy), index);
    }
    assert_eq!(counters(TASK_KEYS, [0; 8]).as_object().unwrap().len(), 8);
    assert_eq!(counters(POLICY_KEYS, [0; 4]).as_object().unwrap().len(), 4);
}

#[test]
fn counter_overflow_refuses_without_wrapping() {
    let mut count = u64::MAX;
    assert_eq!(increment(&mut count), Err(InspectionError::LimitExceeded));
    assert_eq!(count, u64::MAX);
}
