#![cfg(unix)]

#[allow(dead_code)]
#[path = "permission_support/mod.rs"]
mod support;

use b4_contract::*;
use b4_kernel::transition;
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
    fn validate_call(&self, call: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        assert!(
            call.task.status == TaskStatus::InFlight,
            "gate sees actual claimed phase"
        );
        assert!(
            matches!(&call.phase, PermissionCallPhase::Claimed(_)),
            "gate sees a borrowed claimed invocation"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.behavior {
            Behavior::Allow => Ok(()),
            Behavior::Denied => Err(ToolConstraintError::Denied),
            Behavior::Unavailable => Err(ToolConstraintError::Unavailable),
            Behavior::Panic => panic!("private claimed fixture constraint panic"),
        }
    }
}
#[derive(Clone, Copy)]
enum Negative {
    None,
    CallTask,
    CallEffect,
    CallAttempt,
    CallInput,
    OwnerZero,
    TaskStatus,
    TaskMissingAttempt,
    TaskResult,
    TaskFailure,
    RequiredUnapproved,
    ContextApplication,
    ContextApplicationRevision,
    ContextExecutionRevision,
}
#[derive(Clone, Copy)]
enum Expected {
    Allowed,
    Denied,
    Unavailable,
}
fn check_claim(
    behavior: Behavior,
    required: bool,
    negative: Negative,
    expected: Expected,
    expected_calls: usize,
) {
    let fixture = Fixture::new(required);
    let service = ApprovalService::new(fixture.store.clone(), fixture.authority.clone()).unwrap();
    if required {
        let request = fixture.once_request();
        let accepted = service
            .accept(&fixture.execution, fixture.fence(), request)
            .unwrap();
        assert!(
            accepted.snapshot.tasks[0].approved,
            "real Once supplies required approval before real preparation"
        );
    }
    let prepared = service
        .prepare_dispatch(
            &fixture.context,
            &fixture.execution,
            fixture.fence(),
            fixture.task.clone(),
            AttemptId::new("real-claimed-fixture-attempt").unwrap(),
            TransitionFacts { now_ms: 104 },
        )
        .unwrap_or_else(|_| panic!("real public preparation must succeed"));
    let before = fixture.store.thread_view(&fixture.thread).unwrap().unwrap();
    assert!(
        before.execution.as_ref() == Some(&prepared.execution),
        "prepared attempt is the actual complete current execution"
    );
    let original_task = &prepared.execution.snapshot.tasks[0];
    assert!(
        original_task.status == TaskStatus::InFlight
            && original_task.attempt_id.as_ref() == Some(&prepared.invocation.attempt_id),
        "real preparation supplies actual InFlight task and attempt"
    );
    assert!(
        prepared.invocation.execution_id == prepared.execution.snapshot.execution_id
            && prepared.invocation.turn_id == prepared.execution.snapshot.turn_id
            && prepared.invocation.owner_generation == prepared.execution.snapshot.owner_generation,
        "actual prepared full invocation owner and execution fences match"
    );
    assert!(
        prepared.invocation.task_id == original_task.spec.id
            && prepared.invocation.effect_id == original_task.spec.effect_id
            && prepared.invocation.input == original_task.input,
        "actual prepared task effect and input match"
    );
    assert!(
        original_task.spec.requires_approval == required && (!required || original_task.approved),
        "actual prepared approval shape is legitimate"
    );
    assert!(
        original_task.result.is_none() && original_task.failure.is_none(),
        "uninvoked actual preparation has no business outcome"
    );
    let mut context = InvocationContext {
        thread_id: before.thread.id.clone(),
        thread_generation: before.thread.generation.clone(),
        origin: before.thread.origin.clone().unwrap(),
        application_revision: fixture.application.identity().revision.clone(),
        execution_revision: fixture.definition.revision.clone(),
        policy_revision: fixture.context.policy_revision.clone(),
    };
    let mut task = original_task.clone();
    let mut call = prepared.invocation.clone();
    // Local DTO negatives borrow a real prepared attempt. The original full
    // prepared record remains unchanged and owns the cancellation fences.
    match negative {
        Negative::None => {}
        Negative::CallTask => call.task_id = TaskId::new("negative-task").unwrap(),
        Negative::CallEffect => call.effect_id = EffectId::new("negative-effect").unwrap(),
        Negative::CallAttempt => call.attempt_id = AttemptId::new("negative-attempt").unwrap(),
        Negative::CallInput => {
            call.input
                .insert("input".into(), serde_json::json!({"negative": true}));
        }
        Negative::OwnerZero => call.owner_generation = 0,
        Negative::TaskStatus => task.status = TaskStatus::Ready,
        Negative::TaskMissingAttempt => task.attempt_id = None,
        Negative::TaskResult => {
            task.result = Some(TaskResult {
                output: serde_json::json!({"negative": true}),
                writes: vec![],
            })
        }
        Negative::TaskFailure => task.failure = Some("negative fixture failure".into()),
        Negative::RequiredUnapproved => {
            assert!(required);
            task.approved = false;
        }
        Negative::ContextApplication => context.origin.application = "negative-app".into(),
        Negative::ContextApplicationRevision => {
            context.application_revision = "negative-app-revision".into()
        }
        Negative::ContextExecutionRevision => {
            context.execution_revision = ExecutionRevision::new("negative-definition").unwrap()
        }
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
        PermissionCallPhase::Claimed(&call),
    );
    let after = fixture.store.thread_view(&fixture.thread).unwrap().unwrap();
    assert!(
        after == before,
        "pure claimed evaluation preserves actual full thread and prepared execution"
    );
    drop(catalog);
    // No native worker or callback has been invoked. Settle the genuine durable
    // claim through ordinary kernel cancellation using only original fences.
    let cancellation = transition(
        &prepared.execution.snapshot,
        KernelCommand::Cancel {
            turn_id: prepared.invocation.turn_id.clone(),
            owner_generation: prepared.invocation.owner_generation,
        },
        TransitionFacts { now_ms: 105 },
    )
    .unwrap();
    let cancelling = fixture
        .store
        .commit(
            &fixture.execution,
            (&prepared.execution.snapshot).into(),
            cancellation,
        )
        .unwrap();
    let settlement = transition(
        &cancelling.snapshot,
        KernelCommand::CancelAttempt {
            task_id: prepared.invocation.task_id.clone(),
            attempt_id: prepared.invocation.attempt_id.clone(),
        },
        TransitionFacts { now_ms: 106 },
    )
    .unwrap();
    let settled = fixture
        .store
        .commit(
            &fixture.execution,
            (&cancelling.snapshot).into(),
            settlement,
        )
        .unwrap();
    assert!(
        settled.snapshot.status == ExecutionStatus::Cancelled
            && settled.snapshot.tasks[0].status == TaskStatus::Cancelled,
        "genuine uninvoked claim cancellation settles normally"
    );
    assert!(
        settled.snapshot.tasks[0].attempt_id.as_ref() == Some(&prepared.invocation.attempt_id),
        "settlement preserves the original genuine attempt identity"
    );
    drop(service);
    let persisted = fixture.close();
    assert!(
        persisted == settled,
        "genuine successor observes exact cancelled prepared attempt"
    );
    let matched = match expected {
        Expected::Allowed => result.is_ok(),
        Expected::Denied => matches!(result, Err(PermissionError::ConstraintDenied)),
        Expected::Unavailable => matches!(result, Err(PermissionError::ConstraintUnavailable)),
    };
    assert!(
        matched,
        "claimed evaluator must return the expected finite permission classification"
    );
    assert!(
        calls.load(Ordering::SeqCst) == expected_calls,
        "claimed evaluator must invoke or bypass the actual constraint as required"
    );
}

#[test]
fn actual_nonapproval_prepared_claim_invokes_allowed_constraint() {
    check_claim(Behavior::Allow, false, Negative::None, Expected::Allowed, 1);
}

#[test]
fn actual_once_prepared_required_claim_invokes_allowed_constraint() {
    check_claim(Behavior::Allow, true, Negative::None, Expected::Allowed, 1);
}

#[test]
fn actual_prepared_claim_denied_constraint_refuses() {
    check_claim(Behavior::Denied, false, Negative::None, Expected::Denied, 1);
}

#[test]
fn actual_prepared_claim_unavailable_constraint_refuses() {
    check_claim(
        Behavior::Unavailable,
        false,
        Negative::None,
        Expected::Unavailable,
        1,
    );
}

#[test]
fn actual_prepared_claim_static_panic_maps_unavailable() {
    check_claim(
        Behavior::Panic,
        false,
        Negative::None,
        Expected::Unavailable,
        1,
    );
}

#[test]
fn claimed_foreign_call_task_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::CallTask,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_foreign_call_effect_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::CallEffect,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_foreign_call_attempt_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::CallAttempt,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_changed_call_input_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::CallInput,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_zero_call_owner_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::OwnerZero,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_ready_task_status_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::TaskStatus,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_missing_task_attempt_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::TaskMissingAttempt,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_task_result_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::TaskResult,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_task_failure_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::TaskFailure,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_required_unapproved_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        true,
        Negative::RequiredUnapproved,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_foreign_context_application_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::ContextApplication,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_foreign_context_app_revision_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::ContextApplicationRevision,
        Expected::Denied,
        0,
    );
}

#[test]
fn claimed_foreign_context_definition_revision_refused_before_constraint() {
    check_claim(
        Behavior::Allow,
        false,
        Negative::ContextExecutionRevision,
        Expected::Denied,
        0,
    );
}
