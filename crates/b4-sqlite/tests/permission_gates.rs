#![cfg(unix)]

#[allow(dead_code)]
#[path = "permission_support/mod.rs"]
mod support;

use b4_contract::*;
use b4_runtime::approval::*;
use b4_runtime::permissions::*;
use b4_runtime::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use support::Fixture;

#[derive(Clone, Copy)]
enum Behavior {
    Allow,
    Denied,
    Unavailable,
    Panic,
}
struct Probe {
    calls: Arc<AtomicUsize>,
    behavior: Behavior,
}
impl ToolConstraint for Probe {
    fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.behavior {
            Behavior::Allow => Ok(()),
            Behavior::Denied => Err(ToolConstraintError::Denied),
            Behavior::Unavailable => Err(ToolConstraintError::Unavailable),
            Behavior::Panic => panic!("private fixture constraint panic"),
        }
    }
}
#[derive(Clone, Copy)]
enum Negative {
    None,
    Application,
    ApplicationRevision,
    ExecutionRevision,
    EmptyTenant,
    EmptyPrincipal,
    EmptyPolicy,
    ControlPolicy,
    LongPrincipal,
    TaskId,
    EffectId,
    Recovery,
    ApprovalRequirement,
    PhaseStatus,
    ReadyInterrupt,
    ReadyResult,
    ReadyFailure,
    WaitingApproved,
    WaitingMissingInterrupt,
}
#[derive(Clone, Copy)]
enum Expected {
    Allowed,
    Denied,
    Unavailable,
    InvalidCatalog,
}
fn check_call(
    behavior: Behavior,
    waiting: bool,
    negative: Negative,
    expected: Expected,
    expected_calls: usize,
) {
    let fixture = Fixture::new(waiting);
    let once = if waiting {
        Some(fixture.once_request())
    } else {
        None
    };
    let before = fixture.store.thread_view(&fixture.thread).unwrap().unwrap();
    let stored = before.execution.as_ref().unwrap();
    assert!(
        stored.snapshot.tasks.len() == 1,
        "real execution has one task"
    );
    let mut task = stored.snapshot.tasks[0].clone();
    assert!(
        task.attempt_id.is_none() && task.result.is_none() && task.failure.is_none(),
        "genuine unclaimed task has no attempt or outcome"
    );
    assert!(!task.approved, "genuine fixture is unapproved");
    assert!(
        task.status
            == if waiting {
                TaskStatus::WaitingForApproval
            } else {
                TaskStatus::Ready
            },
        "genuine fixture phase is current"
    );
    assert!(
        task.interrupt.is_some() == waiting,
        "genuine interrupt matches phase"
    );
    let mut context = InvocationContext {
        thread_id: before.thread.id.clone(),
        thread_generation: before.thread.generation.clone(),
        origin: before.thread.origin.clone().unwrap(),
        application_revision: fixture.application.identity().revision.clone(),
        execution_revision: fixture.definition.revision.clone(),
        policy_revision: fixture.context.policy_revision.clone(),
    };
    // These are explicit negative borrowed DTO probes. They never replace a
    // stored record or claim to establish a real claimed native invocation.
    match negative {
        Negative::None => {}
        Negative::Application => context.origin.application = "other-app".into(),
        Negative::ApplicationRevision => context.application_revision = "other-app-revision".into(),
        Negative::ExecutionRevision => {
            context.execution_revision = ExecutionRevision::new("other-definition").unwrap()
        }
        Negative::EmptyTenant => context.origin.tenant.clear(),
        Negative::EmptyPrincipal => context.origin.principal.clear(),
        Negative::EmptyPolicy => context.policy_revision.clear(),
        Negative::ControlPolicy => context.policy_revision = "policy\ninvalid".into(),
        Negative::LongPrincipal => context.origin.principal = "a".repeat(257),
        Negative::TaskId => task.spec.id = TaskId::new("foreign-task").unwrap(),
        Negative::EffectId => task.spec.effect_id = EffectId::new("foreign-effect").unwrap(),
        Negative::Recovery => task.spec.recovery = RecoveryPolicy::Unsafe,
        Negative::ApprovalRequirement => task.spec.requires_approval = !task.spec.requires_approval,
        Negative::PhaseStatus => task.status = TaskStatus::Succeeded,
        Negative::ReadyInterrupt => {
            task.interrupt = Some(PendingApproval {
                id: InterruptId::new("negative-interrupt").unwrap(),
                prompt: serde_json::json!({"negative": true}),
            })
        }
        Negative::ReadyResult => {
            task.result = Some(TaskResult {
                output: serde_json::json!({"negative": true}),
                writes: vec![],
            })
        }
        Negative::ReadyFailure => task.failure = Some("negative fixture failure".into()),
        Negative::WaitingApproved => task.approved = true,
        Negative::WaitingMissingInterrupt => task.interrupt = None,
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let constraint: Arc<dyn ToolConstraint> = Arc::new(Probe {
        calls: calls.clone(),
        behavior,
    });
    let catalog = PermissionCatalog::new(
        fixture.application.clone(),
        vec![fixture.registration(constraint)],
    )
    .unwrap();
    let result = catalog.validate_call(
        &context,
        &fixture.definition,
        &task,
        PermissionCallPhase::Unclaimed,
    );
    let after = fixture.store.thread_view(&fixture.thread).unwrap().unwrap();
    assert!(
        after == before,
        "pure gate must preserve actual complete thread and execution"
    );
    drop(catalog);
    // The pure evaluator does not grant authority. Settle a real pending Once
    // through the unchanged default service before normal writer retirement.
    let settled = if let Some(request) = once {
        let service =
            ApprovalService::new(fixture.store.clone(), fixture.authority.clone()).unwrap();
        let accepted = service
            .accept(&fixture.execution, fixture.fence(), request)
            .unwrap();
        drop(service);
        assert!(
            accepted.snapshot.tasks[0].approved
                && accepted.snapshot.tasks[0].status == TaskStatus::Ready,
            "legacy Once settlement remains legal"
        );
        accepted
    } else {
        stored.clone()
    };
    let persisted = fixture.close();
    assert!(
        persisted == settled,
        "normal successor observes exact settled execution"
    );
    let matched = match expected {
        Expected::Allowed => result.is_ok(),
        Expected::Denied => matches!(result, Err(PermissionError::ConstraintDenied)),
        Expected::Unavailable => matches!(result, Err(PermissionError::ConstraintUnavailable)),
        Expected::InvalidCatalog => matches!(result, Err(PermissionError::InvalidCatalog)),
    };
    assert!(
        matched,
        "pure gate must return the expected finite permission classification"
    );
    assert!(
        calls.load(Ordering::SeqCst) == expected_calls,
        "pure gate must invoke or bypass the actual constraint as required"
    );
}

#[test]
fn denied_constraint_refuses_genuine_nonapproval_ready() {
    check_call(Behavior::Denied, false, Negative::None, Expected::Denied, 1);
}

#[test]
fn unavailable_constraint_refuses_genuine_nonapproval_ready() {
    check_call(
        Behavior::Unavailable,
        false,
        Negative::None,
        Expected::Unavailable,
        1,
    );
}

#[test]
fn constraint_panic_maps_to_unavailable_after_normal_payload_drop() {
    check_call(
        Behavior::Panic,
        false,
        Negative::None,
        Expected::Unavailable,
        1,
    );
}

#[test]
fn denied_constraint_refuses_genuine_once_waiting() {
    check_call(Behavior::Denied, true, Negative::None, Expected::Denied, 1);
}

#[test]
fn allowed_constraint_is_actually_invoked_for_genuine_ready() {
    check_call(Behavior::Allow, false, Negative::None, Expected::Allowed, 1);
}

#[test]
fn allowed_constraint_is_actually_invoked_for_genuine_once_waiting() {
    check_call(Behavior::Allow, true, Negative::None, Expected::Allowed, 1);
}

#[test]
fn context_foreign_application_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::Application,
        Expected::Denied,
        0,
    );
}

#[test]
fn context_foreign_application_revision_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::ApplicationRevision,
        Expected::Denied,
        0,
    );
}

#[test]
fn context_foreign_execution_revision_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::ExecutionRevision,
        Expected::Denied,
        0,
    );
}

#[test]
fn context_empty_tenant_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::EmptyTenant,
        Expected::Denied,
        0,
    );
}

#[test]
fn context_empty_principal_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::EmptyPrincipal,
        Expected::Denied,
        0,
    );
}

#[test]
fn context_empty_policy_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::EmptyPolicy,
        Expected::Denied,
        0,
    );
}

#[test]
fn context_control_policy_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::ControlPolicy,
        Expected::Denied,
        0,
    );
}

#[test]
fn context_oversize_principal_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::LongPrincipal,
        Expected::Denied,
        0,
    );
}

#[test]
fn task_foreign_task_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::TaskId,
        Expected::InvalidCatalog,
        0,
    );
}

#[test]
fn task_foreign_effect_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::EffectId,
        Expected::InvalidCatalog,
        0,
    );
}

#[test]
fn task_changed_recovery_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::Recovery,
        Expected::InvalidCatalog,
        0,
    );
}

#[test]
fn task_changed_approval_requirement_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::ApprovalRequirement,
        Expected::InvalidCatalog,
        0,
    );
}

#[test]
fn unclaimed_settled_status_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::PhaseStatus,
        Expected::Denied,
        0,
    );
}

#[test]
fn unclaimed_ready_interrupt_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::ReadyInterrupt,
        Expected::Denied,
        0,
    );
}

#[test]
fn unclaimed_ready_result_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::ReadyResult,
        Expected::Denied,
        0,
    );
}

#[test]
fn unclaimed_ready_failure_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        false,
        Negative::ReadyFailure,
        Expected::Denied,
        0,
    );
}

#[test]
fn unclaimed_waiting_approved_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        true,
        Negative::WaitingApproved,
        Expected::Denied,
        0,
    );
}

#[test]
fn unclaimed_waiting_without_interrupt_refused_before_constraint() {
    check_call(
        Behavior::Allow,
        true,
        Negative::WaitingMissingInterrupt,
        Expected::Denied,
        0,
    );
}
