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

struct CountConstraint(Arc<AtomicUsize>);
impl ToolConstraint for CountConstraint {
    fn validate_call(&self, call: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        assert!(
            call.task.approved && call.task.spec.requires_approval,
            "gate receives genuine approved required task"
        );
        assert!(
            call.task.status == TaskStatus::Ready && call.task.interrupt.is_some(),
            "gate receives preserved real approval interrupt"
        );
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn genuine_once_approved_ready_preserves_interrupt_and_invokes_constraint() {
    let fixture = Fixture::new(true);
    let request = fixture.once_request();
    let service = ApprovalService::new(fixture.store.clone(), fixture.authority.clone()).unwrap();
    let accepted = service
        .accept(&fixture.execution, fixture.fence(), request)
        .unwrap();
    drop(service);
    let before = fixture.store.thread_view(&fixture.thread).unwrap().unwrap();
    assert!(
        before.execution.as_ref() == Some(&accepted),
        "actual Once acceptance is the current stored execution"
    );
    let task = &accepted.snapshot.tasks[0];
    assert!(
        task.spec.requires_approval && task.approved && task.status == TaskStatus::Ready,
        "real Once supplied approved required Ready"
    );
    assert!(
        task.interrupt.is_some()
            && task.attempt_id.is_none()
            && task.result.is_none()
            && task.failure.is_none(),
        "real interrupt remains before dispatch"
    );
    let context = InvocationContext {
        thread_id: before.thread.id.clone(),
        thread_generation: before.thread.generation.clone(),
        origin: before.thread.origin.clone().unwrap(),
        application_revision: fixture.application.identity().revision.clone(),
        execution_revision: fixture.definition.revision.clone(),
        policy_revision: fixture.context.policy_revision.clone(),
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let constraint: Arc<dyn ToolConstraint> = Arc::new(CountConstraint(calls.clone()));
    let catalog = PermissionCatalog::new(
        fixture.application.clone(),
        vec![fixture.registration(constraint)],
    )
    .unwrap();
    let result = catalog.validate_call(
        &context,
        &fixture.definition,
        task,
        PermissionCallPhase::Unclaimed,
    );
    let after = fixture.store.thread_view(&fixture.thread).unwrap().unwrap();
    assert!(
        before == after,
        "pure approved Ready gate preserves full actual thread and execution"
    );
    drop(catalog);
    let persisted = fixture.close();
    assert!(
        persisted == accepted,
        "normal successor sees unchanged genuine Once acceptance"
    );
    assert!(
        result.is_ok(),
        "pure gate must allow a legitimate Once approved Ready phase"
    );
    assert!(
        calls.load(Ordering::SeqCst) == 1,
        "approved Ready must actually invoke its constraint"
    );
}
