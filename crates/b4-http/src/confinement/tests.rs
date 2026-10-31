// Focused confinement behavior and ownership cases.
// Their original Unsupported host/core stage produced the preserved compiling
// behavioral RED. Cleanup proofs precede the required semantic assertions.
use super::test_support::*;
use super::*;
use b4_contract::*;
use std::{
    future::Future,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

fn controls() -> Arc<Controls> {
    Controls::new(ResolverMode::Match, PolicyMode::Allow, AuthorityMode::Allow)
}
async fn call_and_retire(
    f: Fixture,
    expected_fault: bool,
) -> (
    PureResult,
    State,
    State,
    Arc<Controls>,
    (usize, usize),
    u64,
    InvocationClock,
) {
    let original = f.original.clone();
    let owner = f.owner;
    let controls = f.controls.clone();
    let (resolver, authority) = f.arguments();
    let invocation = f.invocation.clone();
    invocation.begin();
    let result = tokio::time::timeout(
        WAIT,
        f.host()
            .confine_prior_owner(f.target.clone(), &resolver, &authority),
    )
    .await
    .expect("finite operation completed");
    invocation.finish_if_open();
    let counts = f.counts();
    drop(resolver);
    drop(authority);
    let actual = f.retire(expected_fault).await;
    controls.assert_no_dispatch();
    (
        result, original, actual, controls, counts, owner, invocation,
    )
}
struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn retire_without_original_executor(f: Fixture) -> State {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(f.retire(false))
}

#[test]
fn confinement_unpolled_future_borrows_handles_without_owner_or_mutation() {
    let f = Fixture::new(false, controls());
    let original = f.original.clone();
    let (resolver, authority) = f.arguments();
    let before = (
        Arc::strong_count(&f.host().shared),
        Arc::strong_count(&resolver),
        Arc::strong_count(&authority),
    );
    let future = f
        .host()
        .confine_prior_owner(f.target.clone(), &resolver, &authority);
    assert_eq!(
        before,
        (
            Arc::strong_count(&f.host().shared),
            Arc::strong_count(&resolver),
            Arc::strong_count(&authority)
        )
    );
    assert_eq!(f.shutdown.status().active_owners, 0);
    assert_eq!(f.counts(), (0, 0));
    drop(future);
    assert_eq!(
        before,
        (
            Arc::strong_count(&f.host().shared),
            Arc::strong_count(&resolver),
            Arc::strong_count(&authority)
        )
    );
    assert_eq!(f.shutdown.status().active_owners, 0);
    assert!(f.controls.authority_calls.lock().unwrap().is_empty());
    f.controls.assert_no_dispatch();
    drop(resolver);
    drop(authority);
    let after = retire_without_original_executor(f);
    assert_eq!(after, original);
}

#[test]
fn confinement_first_poll_without_executor_refuses_before_capture() {
    let f = Fixture::new(false, controls());
    let original = f.original.clone();
    let (resolver, authority) = f.arguments();
    let result = {
        let mut future = Box::pin(f.host().confine_prior_owner(
            f.target.clone(),
            &resolver,
            &authority,
        ));
        let waker = Waker::from(Arc::new(Noop));
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("executor refusal is ready before any registration"),
        }
    };
    let counts = f.counts();
    let active = f.shutdown.status().active_owners;
    drop(resolver);
    drop(authority);
    let after = retire_without_original_executor(f);
    assert_eq!(after, original);
    assert_eq!(counts, (0, 0));
    assert_eq!(active, 0);
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::ExecutorUnavailable)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_draining_host_refuses_without_dispatch_authority_or_store_capture() {
    let f = Fixture::new(false, controls());
    f.shutdown.begin_shutdown();
    let (result, before, after, calls, counts, _, _) = call_and_retire(f, false).await;
    assert_eq!(before, after);
    assert_eq!(counts, (0, 0));
    assert!(calls.authority_calls.lock().unwrap().is_empty());
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::AdmissionClosed)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_persistent_fault_refuses_before_registration_and_capture() {
    let f = Fixture::new(false, controls());
    f.host().shared.fault.store(true, Ordering::Release);
    let (result, before, after, calls, counts, _, _) = call_and_retire(f, true).await;
    assert_eq!(before, after);
    assert_eq!(counts, (0, 0));
    assert!(calls.authority_calls.lock().unwrap().is_empty());
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::Unavailable)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_capacity_refusal_preserves_handle_counts_without_trusted_calls() {
    let f = Fixture::new(false, controls());
    let permit = f
        .host()
        .shared
        .capacity
        .clone()
        .try_acquire_owned()
        .unwrap();
    let (resolver, authority) = f.arguments();
    let counts_before = (Arc::strong_count(&resolver), Arc::strong_count(&authority));
    let result = tokio::time::timeout(
        WAIT,
        f.host()
            .confine_prior_owner(f.target.clone(), &resolver, &authority),
    )
    .await
    .expect("finite admission refusal completed; timeout is excluded execution failure");
    let observations = (
        f.counts(),
        f.shutdown.status().active_owners,
        Arc::strong_count(&resolver),
        Arc::strong_count(&authority),
    );
    drop(permit);
    drop(resolver);
    drop(authority);
    let before = f.original.clone();
    let calls = f.controls.clone();
    let after = f.retire(false).await;
    assert_eq!(after, before);
    assert_eq!(observations, ((0, 0), 0, counts_before.0, counts_before.1));
    assert!(calls.authority_calls.lock().unwrap().is_empty());
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::CapacityExhausted)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_same_thread_claim_refuses_without_capture() {
    let f = Fixture::new(false, controls());
    let claim = crate::claim::ThreadClaim::acquire(
        f.host().shared.claims.clone(),
        f.target.thread_id.clone(),
    )
    .ok()
    .unwrap();
    let (resolver, authority) = f.arguments();
    let result = tokio::time::timeout(
        WAIT,
        f.host()
            .confine_prior_owner(f.target.clone(), &resolver, &authority),
    )
    .await
    .expect("finite admission refusal completed; timeout is excluded execution failure");
    let counts = f.counts();
    drop(claim);
    drop(resolver);
    drop(authority);
    let before = f.original.clone();
    let after = f.retire(false).await;
    assert_eq!(after, before);
    assert_eq!(counts, (0, 0));
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::ThreadBusy)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_missing_exact_definition_calls_only_separate_authority_with_original() {
    let calls = controls();
    let (result, original, actual, calls, counts, owner, invocation) =
        call_and_retire(Fixture::new(false, calls), false).await;
    let proof = receipt(result);
    assert_eq!(counts, (2, 1));
    assert_eq!(calls.resolver_calls.load(Ordering::SeqCst), 0);
    assert_eq!(calls.policy_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        *calls.authority_calls.lock().unwrap(),
        vec![
            (
                original.view.clone(),
                RecoveryConfinementReason::ExactDefinitionUnavailable
            ),
            (
                original.view.clone(),
                RecoveryConfinementReason::ExactDefinitionUnavailable
            )
        ]
    );
    assert_confined(
        &original,
        &actual,
        &proof,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        owner,
        &invocation,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_trusted_resolution_failures_never_fabricate_dispatch_principal() {
    for mode in [
        ResolverMode::Mismatch,
        ResolverMode::Unavailable,
        ResolverMode::Unauthorized,
    ] {
        let calls = Controls::new(mode, PolicyMode::Allow, AuthorityMode::Allow);
        let (result, before, after, calls, counts, owner, invocation) =
            call_and_retire(Fixture::new(true, calls), false).await;
        let proof = receipt(result);
        assert_eq!(counts, (2, 1));
        assert_eq!(calls.resolver_calls.load(Ordering::SeqCst), 2);
        assert_eq!(calls.policy_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *calls.origins.lock().unwrap(),
            vec![before.view.thread.origin.clone().unwrap(); 2]
        );
        assert_eq!(
            *calls.authority_calls.lock().unwrap(),
            vec![
                (
                    before.view.clone(),
                    RecoveryConfinementReason::RequiredAuthorityUnavailable
                );
                2
            ]
        );
        assert_confined(
            &before,
            &after,
            &proof,
            RecoveryConfinementReason::RequiredAuthorityUnavailable,
            owner,
            &invocation,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_recover_policy_denial_preserves_exact_recorded_provenances() {
    let calls = Controls::new(ResolverMode::Match, PolicyMode::Deny, AuthorityMode::Allow);
    let (result, before, after, calls, counts, owner, invocation) =
        call_and_retire(Fixture::new(true, calls), false).await;
    let proof = receipt(result);
    assert_eq!(counts, (2, 1));
    assert_eq!(calls.resolver_calls.load(Ordering::SeqCst), 2);
    let contexts = calls.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    for context in contexts.iter() {
        assert!(matches!(context.operation, crate::Operation::Recover));
        assert_eq!(context.thread_id.as_ref(), Some(&before.view.thread.id));
        assert_eq!(context.origin, before.view.thread.origin);
        let expected = &before.approval.execution.snapshot.definition;
        for provenance in [&context.requested_route, &context.stored_route] {
            let provenance = provenance.as_ref().unwrap();
            assert_eq!(provenance.route, expected.route);
            assert_eq!(provenance.revision, expected.revision);
        }
    }
    assert_confined(
        &before,
        &after,
        &proof,
        RecoveryConfinementReason::DispatchNotAuthorized,
        owner,
        &invocation,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_dispatch_available_is_not_applicable_without_mutation_authority() {
    let (result, before, after, calls, counts, _, _) =
        call_and_retire(Fixture::new(true, controls()), false).await;
    assert_eq!(before, after);
    assert_eq!(counts, (1, 0));
    assert_eq!(calls.resolver_calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls.policy_calls.load(Ordering::SeqCst), 1);
    assert!(calls.authority_calls.lock().unwrap().is_empty());
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::NotApplicable)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_separate_authority_denial_or_unavailability_preserves_whole_source() {
    for (mode, refusal) in [
        (
            AuthorityMode::Deny,
            RecoveryConfinementRefusal::AuthorityDenied,
        ),
        (
            AuthorityMode::Unavailable,
            RecoveryConfinementRefusal::AuthorityUnavailable,
        ),
    ] {
        let calls = Controls::new(ResolverMode::Match, PolicyMode::Allow, mode);
        let (result, before, after, calls, counts, _, _) =
            call_and_retire(Fixture::new(false, calls), false).await;
        assert_eq!(before, after);
        assert_eq!(counts, (1, 0));
        assert_eq!(
            *calls.authority_calls.lock().unwrap(),
            vec![(
                before.view.clone(),
                RecoveryConfinementReason::ExactDefinitionUnavailable
            )]
        );
        assert!(assert_refusal(result, refusal)
            .confirmed_receipt()
            .is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_renewal_checks_whole_view_not_only_revision_and_incarnation() {
    let f = Fixture::new(false, controls());
    *f.store.upgrade().unwrap().capture_mode.lock().unwrap() = CaptureMode::ChangeSecondView;
    let (result, before, after, calls, counts, _, _) = call_and_retire(f, false).await;
    assert_eq!(before, after);
    assert_eq!(counts, (2, 0));
    assert_eq!(calls.authority_calls.lock().unwrap().len(), 1);
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::ObservationChanged)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_changed_renewed_dispatch_reason_refuses_without_substitution() {
    let calls = Controls::new(
        ResolverMode::Match,
        PolicyMode::AllowRenewal,
        AuthorityMode::Allow,
    );
    let (result, before, after, calls, counts, _, _) =
        call_and_retire(Fixture::new(true, calls), false).await;
    assert_eq!(before, after);
    assert_eq!(counts, (2, 0));
    assert_eq!(calls.policy_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *calls.authority_calls.lock().unwrap(),
        vec![(
            before.view.clone(),
            RecoveryConfinementReason::DispatchNotAuthorized
        )]
    );
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::ObservationChanged)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_renewed_authority_receives_same_original_and_can_refuse() {
    let calls = Controls::new(
        ResolverMode::Match,
        PolicyMode::Allow,
        AuthorityMode::DenyRenewal,
    );
    let (result, before, after, calls, counts, _, _) =
        call_and_retire(Fixture::new(false, calls), false).await;
    assert_eq!(before, after);
    assert_eq!(counts, (2, 0));
    assert_eq!(
        *calls.authority_calls.lock().unwrap(),
        vec![
            (
                before.view.clone(),
                RecoveryConfinementReason::ExactDefinitionUnavailable
            );
            2
        ]
    );
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::AuthorityDenied)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_backend_local_refusals_and_infrastructure_faults_are_distinct() {
    for (backend, fault) in [
        (RecoveryConfinementBackendError::OwnerConflict, false),
        (RecoveryConfinementBackendError::LimitExceeded, false),
        (RecoveryConfinementBackendError::UnsupportedSnapshot, false),
        (
            RecoveryConfinementBackendError::UnsupportedAuthorityProfile,
            false,
        ),
        (RecoveryConfinementBackendError::CorruptRecord, true),
        (RecoveryConfinementBackendError::Unavailable, true),
    ] {
        let f = Fixture::new(false, controls());
        *f.store.upgrade().unwrap().capture_mode.lock().unwrap() = CaptureMode::Refuse(backend);
        let (result, before, after, calls, counts, _, _) = call_and_retire(f, fault).await;
        assert_eq!(before, after);
        assert_eq!(counts, (1, 0));
        assert!(calls.authority_calls.lock().unwrap().is_empty());
        assert!(assert_refusal(
            result,
            if fault {
                RecoveryConfinementRefusal::Unavailable
            } else {
                RecoveryConfinementRefusal::Backend(backend)
            }
        )
        .confirmed_receipt()
        .is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_commit_then_error_or_panic_is_ambiguous_not_rollback() {
    for mode in [CommitMode::CommitThenError, CommitMode::CommitThenPanic] {
        let f = Fixture::new(false, controls());
        *f.store.upgrade().unwrap().commit_mode.lock().unwrap() = mode;
        let (result, before, after, _, counts, owner, _) = call_and_retire(f, true).await;
        assert_eq!(counts, (2, 1));
        let error = assert_refusal(result, RecoveryConfinementRefusal::Unavailable);
        assert!(error.confirmed_receipt().is_none());
        assert_blocked_without_returned_receipt(
            &before,
            &after,
            RecoveryConfinementReason::ExactDefinitionUnavailable,
            owner,
        );
        // We do not infer rollback from None. Actual committed state persisted
        // through real next-writer reopening before these failure assertions.
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_contradictory_backend_receipt_never_becomes_confirmed() {
    let f = Fixture::new(false, controls());
    *f.store.upgrade().unwrap().commit_mode.lock().unwrap() = CommitMode::ContradictoryReceipt;
    let (result, before, after, _, counts, owner, _) = call_and_retire(f, true).await;
    assert_eq!(counts, (2, 1));
    let error = assert_refusal(result, RecoveryConfinementRefusal::Unavailable);
    assert!(error.confirmed_receipt().is_none());
    assert_blocked_without_returned_receipt(
        &before,
        &after,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        owner,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_shutdown_cut_loses_before_lease_or_finishes_after_won_lease() {
    for at_call in [1, 2] {
        let mut f = Fixture::new(false, controls());
        let before = f.original.clone();
        let owner = f.owner;
        let invocation = f.invocation.clone();
        let next_owner = owner.checked_add(1).unwrap();
        let gate = Gate::new();
        let _cleanup = Cleanup(gate.clone());
        let checks = HeldChecks::from_fixture(&f);
        let host = f.host.take().unwrap();
        let resolver: Arc<dyn crate::StartupPrincipalResolver> =
            Arc::new(ControlledResolver(f.controls.clone()));
        let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(GateAuthority {
            controls: f.controls.clone(),
            gate: gate.clone(),
            at_call,
        });
        let target = f.target.clone();
        let task_clock = invocation.clone();
        let mut task = tokio::spawn(async move {
            task_clock.begin();
            let result = host
                .confine_prior_owner(target, &resolver, &authority)
                .await;
            task_clock.finish_if_open();
            result
        });
        let started = reached_or_completed(&gate, &mut task).await;
        let result = match started {
            Started::Complete(result) => result,
            Started::Entered => {
                checks.assert_held(&f, false);
                assert_eq!(f.state(), before);
                let status = f.shutdown.begin_shutdown();
                assert_eq!(status.active_owners, 1);
                gate.release();
                let result = join_pure(task).await;
                gate.assert_positive();
                result
            }
        };
        let after = f.retire_detached(false).await;
        let before = original_at_reopen(&before, next_owner);
        if at_call == 1 {
            assert_eq!(after, before);
            assert!(
                assert_refusal(result, RecoveryConfinementRefusal::AdmissionClosed)
                    .confirmed_receipt()
                    .is_none()
            );
        } else {
            let proof = receipt(result);
            assert_confined(
                &before,
                &after,
                &proof,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                owner,
                &invocation,
            );
        }
        // Missing gate timeout is excluded setup/execution failure. On the
        // Unsupported stage completion, normal retirement precedes the actual
        // required-semantic assertion above.
    }
}

fn spawn_private_core(
    f: &Fixture,
    resolver: Arc<dyn crate::StartupPrincipalResolver>,
    authority: Arc<dyn RecoveryConfinementAuthority>,
) -> tokio::task::JoinHandle<PureResult> {
    let (owned, permit, claim) = f.take_core_arguments();
    f.invocation.begin();
    super::operation::spawn_core(owned, f.target.clone(), resolver, authority, permit, claim)
        .expect("private ordinary core submission")
}
async fn finish_private_core(f: &Fixture, task: tokio::task::JoinHandle<PureResult>) -> PureResult {
    let joined = tokio::time::timeout(WAIT, task)
        .await
        .expect("normal core join remained finite");
    f.invocation.finish_if_open();
    super::operation::finish_join(&f.shutdown, joined)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_owned_core_retains_verified_receipt_through_authority_replacement_payloads() {
    let mut f = Fixture::new(false, controls());
    let before = f.original.clone();
    let owner = f.owner;
    let invocation = f.invocation.clone();
    let first = Gate::new();
    let second = Gate::new();
    let _first_cleanup = Cleanup(first.clone());
    let _second_cleanup = Cleanup(second.clone());
    let checks = HeldChecks::from_fixture(&f);
    let resolver: Arc<dyn crate::StartupPrincipalResolver> =
        Arc::new(ControlledResolver(f.controls.clone()));
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(PanicDropAuthority {
        controls: f.controls.clone(),
        payload: Some(f.payload(first.clone(), Some(second.clone()))),
    });
    let mut task = spawn_private_core(&f, resolver, authority);
    drop(f.host.take());
    match reached_or_completed(&first, &mut task).await {
        Started::Complete(result) => {
            invocation.finish_if_open();
            let after = f.retire_detached(false).await;
            // Baseline Unsupported must be exposed after normal cleanup; no
            // missing held-capability timeout qualifies as behavior RED.
            let proof = receipt(result);
            assert_confined(
                &original_at_reopen(&before, owner + 1),
                &after,
                &proof,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                owner,
                &invocation,
            );
            return;
        }
        Started::Entered => checks.assert_held(&f, true),
    }
    first.release();
    tokio::time::timeout(WAIT, second.entered())
        .await
        .expect("actual replacement payload reached");
    checks.assert_held(&f, true);
    second.release();
    let result = finish_private_core(&f, task).await;
    first.assert_positive();
    second.assert_positive();
    let after = f.retire_detached(true).await;
    let error = assert_refusal(result, RecoveryConfinementRefusal::Unavailable);
    let proof = error
        .confirmed_receipt()
        .expect("only actual verified backend result survives later destructor fault");
    assert_confined(
        &original_at_reopen(&before, owner + 1),
        &after,
        proof,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        owner,
        &invocation,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_owned_core_disposes_two_panicking_handles_independently() {
    let mut f = Fixture::new(false, controls());
    let before = f.original.clone();
    let owner = f.owner;
    let invocation = f.invocation.clone();
    let first = Gate::new();
    let second = Gate::new();
    let _first_cleanup = Cleanup(first.clone());
    let _second_cleanup = Cleanup(second.clone());
    let checks = HeldChecks::from_fixture(&f);
    let resolver: Arc<dyn crate::StartupPrincipalResolver> = Arc::new(PanicDropResolver {
        controls: f.controls.clone(),
        payload: Some(f.payload(first.clone(), None)),
    });
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(PanicDropAuthority {
        controls: f.controls.clone(),
        payload: Some(f.payload(second.clone(), None)),
    });
    let mut task = spawn_private_core(&f, resolver, authority);
    drop(f.host.take());
    match reached_or_completed(&first, &mut task).await {
        Started::Complete(result) => {
            invocation.finish_if_open();
            let after = f.retire_detached(false).await;
            let proof = receipt(result);
            assert_confined(
                &original_at_reopen(&before, owner + 1),
                &after,
                &proof,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                owner,
                &invocation,
            );
            return;
        }
        Started::Entered => checks.assert_held(&f, true),
    }
    first.release();
    tokio::time::timeout(WAIT, second.entered())
        .await
        .expect("second handle disposal reached independently");
    checks.assert_held(&f, true);
    second.release();
    let result = finish_private_core(&f, task).await;
    first.assert_positive();
    second.assert_positive();
    let after = f.retire_detached(true).await;
    let error = assert_refusal(result, RecoveryConfinementRefusal::Unavailable);
    assert_confined(
        &original_at_reopen(&before, owner + 1),
        &after,
        error
            .confirmed_receipt()
            .expect("verified result survives independent cleanup"),
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        owner,
        &invocation,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_final_shared_drop_keeps_claim_capacity_group_and_returned_receipt() {
    let mut f = Fixture::new(false, controls());
    let before = f.original.clone();
    let owner = f.owner;
    let invocation = f.invocation.clone();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let payload = f.payload(gate.clone(), None);
    f.install_panicking_final_auth(payload);
    let checks = HeldChecks::from_fixture(&f);
    let (resolver, authority) = f.arguments();
    let mut task = spawn_private_core(&f, resolver, authority);
    drop(f.host.take());
    match reached_or_completed(&gate, &mut task).await {
        Started::Complete(result) => {
            invocation.finish_if_open();
            let after = f.retire_detached(false).await;
            let proof = receipt(result);
            assert_confined(
                &original_at_reopen(&before, owner + 1),
                &after,
                &proof,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                owner,
                &invocation,
            );
            return;
        }
        Started::Entered => checks.assert_held(&f, true),
    }
    gate.release();
    let result = finish_private_core(&f, task).await;
    gate.assert_positive();
    let after = f.retire_detached(true).await;
    let error = assert_refusal(result, RecoveryConfinementRefusal::Unavailable);
    assert_confined(
        &original_at_reopen(&before, owner + 1),
        &after,
        error
            .confirmed_receipt()
            .expect("postjoin swallowed Shared Drop fault retains actual result"),
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        owner,
        &invocation,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_direct_prestart_job_disposal_is_counted_but_never_a_commit() {
    let mut f = Fixture::new(false, controls());
    let before = f.original.clone();
    let next_owner = f.owner.checked_add(1).unwrap();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let checks = HeldChecks::from_fixture(&f);
    let resolver: Arc<dyn crate::StartupPrincipalResolver> =
        Arc::new(ControlledResolver(f.controls.clone()));
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(PanicDropAuthority {
        controls: f.controls.clone(),
        payload: Some(f.payload(gate.clone(), None)),
    });
    let (owned, permit, claim) = f.take_core_arguments();
    let job =
        super::operation::Job::new(owned, f.target.clone(), resolver, authority, permit, claim);
    drop(f.host.take());
    let task = tokio::task::spawn_blocking(move || drop(job));
    tokio::time::timeout(WAIT, gate.entered())
        .await
        .expect("direct unstarted Job destructor reached");
    checks.assert_held(&f, true);
    assert_eq!(f.counts(), (0, 0));
    assert!(f.controls.authority_calls.lock().unwrap().is_empty());
    gate.release();
    tokio::time::timeout(WAIT, task)
        .await
        .expect("direct disposal joined")
        .unwrap();
    gate.assert_positive();
    let after = f.retire_detached(true).await;
    assert_eq!(after, original_at_reopen(&before, next_owner));
    // Direct Job Drop only. No actual queued Tokio cancellation, OS failure,
    // backend result or confirmed receipt is asserted by this witness.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_abandoned_public_wait_keeps_won_lease_capture_until_actual_return() {
    let mut f = Fixture::new(false, controls());
    let before = f.original.clone();
    let owner = f.owner;
    let invocation = f.invocation.clone();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let checks = HeldChecks::from_fixture(&f);
    let calls = f.controls.clone();
    let host = f.host.take().unwrap();
    let resolver: Arc<dyn crate::StartupPrincipalResolver> =
        Arc::new(ControlledResolver(f.controls.clone()));
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(GateAuthority {
        controls: f.controls.clone(),
        gate: gate.clone(),
        at_call: 2,
    });
    let target = f.target.clone();
    let task_clock = invocation.clone();
    let mut task = tokio::spawn(async move {
        task_clock.begin();
        let result = host
            .confine_prior_owner(target, &resolver, &authority)
            .await;
        task_clock.finish_if_open();
        result
    });
    match reached_or_completed(&gate, &mut task).await {
        Started::Complete(result) => {
            invocation.finish_if_open();
            let after = f.retire_detached(false).await;
            let proof = receipt(result);
            assert_confined(
                &original_at_reopen(&before, owner + 1),
                &after,
                &proof,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                owner,
                &invocation,
            );
            return;
        }
        Started::Entered => checks.assert_held(&f, false),
    }
    task.abort();
    assert!(tokio::time::timeout(WAIT, task)
        .await
        .expect("abandoned outer wait joined")
        .unwrap_err()
        .is_cancelled());
    checks.assert_held(&f, false);
    let incomplete = f.shutdown.wait_quiescent(Duration::ZERO).await.unwrap();
    assert_eq!(
        incomplete,
        crate::ShutdownOutcome::Incomplete {
            active_owners: 1,
            faulted: false
        }
    );
    gate.release();
    let after = f.retire_detached(false).await;
    gate.assert_positive();
    assert_blocked_without_returned_receipt(
        &original_at_reopen(&before, owner + 1),
        &after,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        owner,
    );
    assert_eq!(
        *calls.authority_calls.lock().unwrap(),
        vec![
            (
                before.view.clone(),
                RecoveryConfinementReason::ExactDefinitionUnavailable
            );
            2
        ]
    );
    calls.assert_no_dispatch();
    // No result/receipt observation is attributed to the absent public caller.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_scripted_host_is_explicitly_unsupported_without_store_capture() {
    let mut f = Fixture::new(false, controls());
    let store = f.store.upgrade().unwrap();
    drop(f.host.take());
    let route = crate::ScriptRoute {
        definition: definition("v2"),
        effects: std::collections::BTreeMap::from([(
            EffectId::new("original-effect").unwrap(),
            crate::ScriptEffect::Void,
        )]),
    };
    let host = crate::HttpHost::new(
        store.clone(),
        Arc::new(Auth),
        Arc::new(ControlledPolicy(f.controls.clone())),
        crate::HostConfig::new(vec![route]),
    )
    .unwrap();
    f.shutdown = host.shutdown_handle();
    f.host = Some(host);
    drop(store);
    let (result, before, after, calls, counts, _, _) = call_and_retire(f, false).await;
    assert_eq!(before, after);
    assert_eq!(counts, (0, 0));
    assert!(calls.authority_calls.lock().unwrap().is_empty());
    assert!(
        assert_refusal(result, RecoveryConfinementRefusal::UnsupportedHost)
            .confirmed_receipt()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_out_of_scope_original_origin_never_reaches_trusted_callbacks() {
    for mode in [CaptureMode::Originless, CaptureMode::ForeignOrigin] {
        let f = Fixture::new(false, controls());
        *f.store.upgrade().unwrap().capture_mode.lock().unwrap() = mode;
        let (result, before, after, calls, counts, _, _) = call_and_retire(f, false).await;
        assert_eq!(before, after);
        assert_eq!(counts, (1, 0));
        assert_eq!(calls.resolver_calls.load(Ordering::SeqCst), 0);
        assert_eq!(calls.policy_calls.load(Ordering::SeqCst), 0);
        assert!(calls.authority_calls.lock().unwrap().is_empty());
        assert!(
            assert_refusal(result, RecoveryConfinementRefusal::ResourceUnavailable)
                .confirmed_receipt()
                .is_none()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confinement_authority_callback_panic_is_faulted_counted_and_never_confirmed() {
    let mut f = Fixture::new(false, controls());
    let before = f.original.clone();
    let next_owner = f.owner.checked_add(1).unwrap();
    let calls = f.controls.clone();
    let gate = Gate::new();
    let _cleanup = Cleanup(gate.clone());
    let checks = HeldChecks::from_fixture(&f);
    let resolver: Arc<dyn crate::StartupPrincipalResolver> =
        Arc::new(ControlledResolver(f.controls.clone()));
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(PanicCallAuthority {
        controls: f.controls.clone(),
        store: f.store.upgrade().unwrap(),
        application: f.application.upgrade().unwrap(),
        gate: gate.clone(),
    });
    let mut task = spawn_private_core(&f, resolver, authority);
    drop(f.host.take());
    let result = match reached_or_completed(&gate, &mut task).await {
        Started::Complete(result) => result,
        Started::Entered => {
            checks.assert_held(&f, true);
            assert_eq!(f.counts(), (1, 0));
            assert_eq!(f.state(), before);
            gate.release();
            let result = finish_private_core(&f, task).await;
            gate.assert_positive();
            result
        }
    };
    // The baseline Unsupported branch completes without a callback-created
    // payload. It receives normal healthy cleanup before the intended result
    // assertion; missing entered-gate timeout is never positive RED.
    let fault = f.shutdown.status().faulted;
    let after = f.retire_detached(fault).await;
    assert_eq!(after, original_at_reopen(&before, next_owner));
    let error = assert_refusal(result, RecoveryConfinementRefusal::Unavailable);
    assert!(error.confirmed_receipt().is_none());
    assert!(fault);
    assert_eq!(
        *calls.authority_calls.lock().unwrap(),
        vec![(
            before.view,
            RecoveryConfinementReason::ExactDefinitionUnavailable
        )]
    );
    calls.assert_no_dispatch();
}
