//! Independent account-policy qualifiers; no stored authority or database setup.
use b4_contract::{EffectId, RecoveryPolicy, TaskId, TaskRecord, TaskSpec, TaskStatus};
use b4_runtime::permissions::{permission_call_digest, PermissionError, PermissionEvidenceBudget};
use serde_json::{json, Map};

fn task() -> TaskRecord {
    TaskRecord {
        spec: TaskSpec {
            id: TaskId::new("quota-task").unwrap(),
            effect_id: EffectId::new("quota-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        },
        status: TaskStatus::Ready,
        input: Map::from_iter([("input".into(), json!({"probe": "budget"}))]),
        attempt_id: None,
        result: None,
        failure: None,
        interrupt: None,
        approved: false,
    }
}

fn run_hashes(
    task: &TaskRecord,
    budget: &mut PermissionEvidenceBudget,
    limit: usize,
) -> (usize, Option<PermissionError>, bool) {
    let mut completed = 0;
    let mut failure = None;
    let mut hashes_valid = true;
    for _ in 0..limit {
        match permission_call_digest(task, budget) {
            Ok(digest) => {
                hashes_valid &= digest.len() == 64;
                completed += 1;
                drop(digest);
            }
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    (completed, failure, hashes_valid)
}

#[test]
fn use_dispatch_budget_allows_work_above_standard_byte_ceiling() {
    let task = task();
    let mut standard = PermissionEvidenceBudget::new();
    let old = run_hashes(&task, &mut standard, 150);
    let old_usage = standard.usage();
    let old_sticky = permission_call_digest(&task, &mut standard).map(drop);
    let old_after = standard.usage();
    drop(standard);

    let mut subject = PermissionEvidenceBudget::for_use_dispatch();
    let actual = run_hashes(&task, &mut subject, 150);
    let usage = subject.usage();
    drop(subject);
    drop(task);
    println!("quota old={old:?} old_usage={old_usage:?} actual={actual:?} usage={usage:?}");
    assert!(old.0 > 0 && old.0 < 150 && old.2);
    assert!(matches!(old.1, Some(PermissionError::LimitExceeded)));
    assert!(matches!(old_sticky, Err(PermissionError::LimitExceeded)));
    assert_eq!(old_usage, old_after);
    assert!(old_usage.encoded_bytes() <= 8 * 1024 * 1024);
    // First new-policy primary assertion, after all subject owners retired.
    assert!(
        actual.1.is_none(),
        "fresh UseDispatch quota permits the complete bounded work"
    );
    assert_eq!(actual.0, 150);
    assert!(actual.2);
    assert!(usage.encoded_bytes() > 8 * 1024 * 1024);
    assert!(usage.encoded_bytes() <= 16 * 1024 * 1024);
    assert!(usage.structural_nodes() <= 65_536);
}

#[test]
fn use_dispatch_budget_refuses_general_resource_sessions() {
    let mut subject = PermissionEvidenceBudget::for_use_dispatch();
    let actual = subject.resource_read().map(drop);
    let usage = subject.usage();
    drop(subject);
    println!("wrong-domain actual={actual:?} usage={usage:?}");
    // First new-policy primary assertion; any successful loan already retired.
    assert!(matches!(actual, Err(PermissionError::UnsupportedProfile)));
}

#[test]
fn use_dispatch_budget_preserves_sticky_byte_exhaustion() {
    let task = task();
    let mut standard = PermissionEvidenceBudget::new();
    let old = run_hashes(&task, &mut standard, 300);
    let old_usage = standard.usage();
    let old_sticky = permission_call_digest(&task, &mut standard).map(drop);
    let old_after = standard.usage();
    drop(standard);

    let mut subject = PermissionEvidenceBudget::for_use_dispatch();
    let actual = run_hashes(&task, &mut subject, 300);
    let usage = subject.usage();
    let sticky = permission_call_digest(&task, &mut subject).map(drop);
    let after = subject.usage();
    drop(subject);
    drop(task);
    println!("use_dispatch_budget_preserves_sticky_byte_exhaustion old={old:?} old_usage={old_usage:?} actual={actual:?} usage={usage:?}");
    assert!(old.0 > 0 && old.0 < 300 && old.2);
    assert!(matches!(old.1, Some(PermissionError::LimitExceeded)));
    assert!(matches!(old_sticky, Err(PermissionError::LimitExceeded)));
    assert_eq!(old_usage, old_after);
    assert!(old_usage.encoded_bytes() <= 8 * 1024 * 1024);
    // Subject comparisons follow all normal account/task/digest retirement.
    assert!(actual.0 > old.0 && actual.0 < 300 && actual.2);
    assert!(matches!(actual.1, Some(PermissionError::LimitExceeded)));
    assert!(matches!(sticky, Err(PermissionError::LimitExceeded)));
    assert_eq!(usage, after);
    assert!(usage.encoded_bytes() > 8 * 1024 * 1024);
    assert!(usage.encoded_bytes() <= 16 * 1024 * 1024);
    assert!(usage.structural_nodes() < 65_536);
}

#[test]
fn use_dispatch_budget_preserves_the_standard_node_ceiling() {
    let mut task = task();
    task.input = Map::from_iter([("input".into(), json!({"probe": vec![0u64; 2000]}))]);
    let mut standard = PermissionEvidenceBudget::new();
    let old = run_hashes(&task, &mut standard, 30);
    let old_usage = standard.usage();
    let old_sticky = permission_call_digest(&task, &mut standard).map(drop);
    let old_after = standard.usage();
    drop(standard);

    let mut subject = PermissionEvidenceBudget::for_use_dispatch();
    let actual = run_hashes(&task, &mut subject, 30);
    let usage = subject.usage();
    let sticky = permission_call_digest(&task, &mut subject).map(drop);
    let after = subject.usage();
    drop(subject);
    drop(task);
    println!("use_dispatch_budget_preserves_the_standard_node_ceiling old={old:?} old_usage={old_usage:?} actual={actual:?} usage={usage:?}");
    assert!(old.0 > 0 && old.0 < 30 && old.2);
    assert!(matches!(old.1, Some(PermissionError::LimitExceeded)));
    assert!(matches!(old_sticky, Err(PermissionError::LimitExceeded)));
    assert_eq!(old_usage, old_after);
    assert!(old_usage.encoded_bytes() < 8 * 1024 * 1024);
    assert_eq!(old_usage.structural_nodes(), 65_536);
    // Subject comparisons follow all normal account/task/digest retirement.
    assert_eq!(actual.0, old.0);
    assert!(actual.2);
    assert!(matches!(actual.1, Some(PermissionError::LimitExceeded)));
    assert!(matches!(sticky, Err(PermissionError::LimitExceeded)));
    assert_eq!(usage, after);
    assert!(usage.encoded_bytes() < 8 * 1024 * 1024);
    assert_eq!(usage.structural_nodes(), 65_536);
}
