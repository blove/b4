use super::{ownership::Arguments, test_support::*, *};
use crate::lifecycle::{OwnedShared, StartupFuture};
use crate::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll, Wake, Waker},
};

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(Noop));
    future.poll(&mut Context::from_waker(&waker))
}
fn assert_send<T: Send>(_: &T) {}

#[test]
fn borrowed_start_future_has_no_framework_alias_before_poll() {
    let mut fixture = Fixture::new(Seed::Empty, false, Capture::Pass, Commit::Pass, None);
    let (resolver, authority) = fixture.handles(Resolution::Match);
    let shared_count = Arc::strong_count(&fixture.host().host.shared);
    let future = fixture.host().begin_preparation_with_confinement(
        &resolver,
        &authority,
        StartupPreparationLimits::default(),
    );
    assert_send(&future);
    let unpolled = (
        Arc::strong_count(&resolver),
        Arc::strong_count(&authority),
        Arc::strong_count(&fixture.host().host.shared),
        fixture.shutdown.status(),
    );
    drop(future);
    let mut first_poll = Box::pin(fixture.host().begin_preparation_with_confinement(
        &resolver,
        &authority,
        StartupPreparationLimits::default(),
    ));
    let result = poll_once(first_poll.as_mut());
    drop(first_poll);
    let after_poll = (
        Arc::strong_count(&resolver),
        Arc::strong_count(&authority),
        fixture.shutdown.status(),
        fixture.shutdown.preparation_status(),
    );
    drop(resolver);
    drop(authority);
    let retired = fixture.retire_without_executor();
    fixture.unchanged(&retired);
    fixture.observations.no_dispatch();
    assert!(!retired.faulted);
    assert_eq!((unpolled.0, unpolled.1, unpolled.2), (1, 1, shared_count));
    assert_eq!(unpolled.3.active_owners, 0);
    assert!(matches!(
        result,
        Poll::Ready(Err(StartupStartError::ExecutorUnavailable))
    ));
    assert_eq!(
        (after_poll.0, after_poll.1, after_poll.2.active_owners),
        (1, 1, 0)
    );
    assert_eq!(after_poll.3.phase, StartupPreparationPhase::NotStarted);
    assert_eq!(fixture.observations.resolutions.load(Ordering::SeqCst), 0);
    assert!(fixture.observations.permissions.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_limits_and_single_attempt_precede_argument_clones() {
    for old_first in [false, true] {
        let discovery = Gate::new();
        let _cleanup = Cleanup(discovery.clone());
        let mut fixture = Fixture::new(
            Seed::Empty,
            false,
            Capture::Pass,
            Commit::Pass,
            Some(discovery.clone()),
        );
        let (resolver, authority) = fixture.handles(Resolution::Match);
        let mut invalid_observations = Vec::new();
        for limits in [
            StartupPreparationLimits {
                max_pages: 0,
                max_recoveries: 1,
            },
            StartupPreparationLimits {
                max_pages: 65,
                max_recoveries: 1,
            },
            StartupPreparationLimits {
                max_pages: 1,
                max_recoveries: 0,
            },
            StartupPreparationLimits {
                max_pages: 1,
                max_recoveries: 257,
            },
        ] {
            let result = bounded(
                fixture
                    .host()
                    .begin_preparation_with_confinement(&resolver, &authority, limits),
            )
            .await;
            invalid_observations.push((
                result.err(),
                Arc::strong_count(&resolver),
                Arc::strong_count(&authority),
                fixture.shutdown.status().active_owners,
            ));
        }
        let (old_handle, new_handle) = if old_first {
            (
                Some(
                    fixture
                        .host()
                        .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
                        .unwrap(),
                ),
                None,
            )
        } else {
            (None, Some(fixture.start(&resolver, &authority).await))
        };
        discovery.entered().await;
        let aliases_before = (Arc::strong_count(&resolver), Arc::strong_count(&authority));
        let already_new = bounded(fixture.host().begin_preparation_with_confinement(
            &resolver,
            &authority,
            StartupPreparationLimits::default(),
        ))
        .await
        .err();
        let already_old = fixture
            .host()
            .begin_preparation(resolver.clone(), StartupPreparationLimits::default())
            .err();
        let aliases_after = (Arc::strong_count(&resolver), Arc::strong_count(&authority));
        let closed_before =
            !fixture.shutdown.is_serving() && fixture.shutdown.register_owner().is_none();
        fixture.shutdown.begin_shutdown();
        let after_cut = bounded(fixture.host().begin_preparation_with_confinement(
            &resolver,
            &authority,
            StartupPreparationLimits::default(),
        ))
        .await
        .err();
        drop(resolver);
        drop(authority);
        fixture.close_host();
        discovery.release();
        let outcome = match (old_handle, new_handle) {
            (Some(handle), None) => match bounded(handle.wait(WAIT)).await.unwrap() {
                StartupWaitOutcome::Finished(outcome) => outcome,
                _ => panic!("old shared-attempt control did not finish"),
            },
            (None, Some(handle)) => finished(&handle).await.preparation,
            _ => unreachable!("one starting entry point"),
        };
        let retired = fixture.retire().await;
        fixture.unchanged(&retired);
        discovery.positive();
        fixture.observations.no_dispatch();
        assert!(!retired.faulted);
        for observation in invalid_observations {
            assert_eq!(
                observation,
                (Some(StartupStartError::InvalidLimits), 1, 1, 0)
            );
        }
        assert_eq!(aliases_before, aliases_after);
        assert_eq!(already_new, Some(StartupStartError::AlreadyStarted));
        assert_eq!(already_old, Some(StartupStartError::AlreadyStarted));
        assert_eq!(after_cut, Some(StartupStartError::Shutdown));
        assert!(closed_before);
        assert!(matches!(
            outcome,
            StartupPreparationOutcome::Incomplete {
                reason: StartupIncompleteReason::Shutdown,
                ..
            }
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_job_requires_whole_original_and_exact_principal() {
    // Finite counterexamples: every bootstrap is a real legal prior SQL view.
    // Metadata contradictions preserve target identity, including a prior block.
    let cases = [
        (
            Seed::Accepted,
            false,
            Capture::ChangeInitial,
            Resolution::Match,
            1,
            1,
            0,
            0,
        ),
        (
            Seed::Blocked,
            false,
            Capture::ChangeInitial,
            Resolution::Match,
            1,
            1,
            0,
            0,
        ),
        (
            Seed::Accepted,
            false,
            Capture::ChangeRenewal,
            Resolution::Match,
            2,
            2,
            1,
            0,
        ),
        (
            Seed::Accepted,
            false,
            Capture::Pass,
            Resolution::ChangeAt(2),
            1,
            2,
            0,
            0,
        ),
        (
            Seed::Accepted,
            false,
            Capture::Pass,
            Resolution::UnavailableAt(2),
            1,
            2,
            0,
            0,
        ),
        (
            Seed::Accepted,
            false,
            Capture::Pass,
            Resolution::ChangeAt(3),
            2,
            3,
            1,
            0,
        ),
        (
            Seed::Accepted,
            true,
            Capture::Pass,
            Resolution::ChangeAt(2),
            1,
            2,
            0,
            0,
        ),
        (
            Seed::Accepted,
            true,
            Capture::Pass,
            Resolution::UnavailableAt(3),
            2,
            3,
            1,
            1,
        ),
        (
            Seed::Accepted,
            false,
            Capture::Unsupported,
            Resolution::Match,
            1,
            1,
            0,
            0,
        ),
        (
            Seed::Accepted,
            false,
            Capture::ChangedTargetInitial,
            Resolution::Match,
            1,
            1,
            0,
            0,
        ),
        (
            Seed::Accepted,
            false,
            Capture::ChangedTargetRenewal,
            Resolution::Match,
            2,
            2,
            1,
            0,
        ),
    ];
    for (seed, exact, capture, resolution, captures, resolutions, permissions, policies) in cases {
        let mut fixture = Fixture::new(seed, exact, capture, Commit::Pass, None);
        let (resolver, authority) = fixture.handles(resolution);
        let handle = fixture.start(&resolver, &authority).await;
        drop(resolver);
        drop(authority);
        let outcome = finished(&handle).await;
        let retired = fixture.retire().await;
        fixture.unchanged(&retired);
        fixture.observations.no_dispatch();
        assert!(!retired.faulted);
        assert_eq!(fixture.observations.mutations.load(Ordering::SeqCst), 0);
        assert_eq!(outcome.confinement, StartupConfinementProgress::default());
        assert!(matches!(
            outcome.preparation,
            StartupPreparationOutcome::Incomplete {
                reason: StartupIncompleteReason::RecoveryRefused,
                progress: StartupProgress {
                    candidates: 1,
                    admitted: 0,
                    ..
                }
            }
        ));
        let actual_captures = fixture.observations.captures.load(Ordering::SeqCst);
        if actual_captures == 0 {
            // The accepted compatibility control refuses via old preflight.
            // It must never be mistaken for testing the new original guard.
            assert_eq!(fixture.observations.resolutions.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.observations.policies.load(Ordering::SeqCst), 1);
            assert!(fixture.observations.permissions.lock().unwrap().is_empty());
        }
        eprintln!("B4_STARTUP_PRIVATE_INTENDED_GUARD_ASSERTION whole_original seed={seed:?} capture={capture:?} resolution={resolution:?} retired");
        assert_eq!(
            actual_captures, captures,
            "bounded full-original guard must actually be observed"
        );
        assert_eq!(
            fixture.observations.resolutions.load(Ordering::SeqCst),
            resolutions
        );
        assert_eq!(
            fixture.observations.policies.load(Ordering::SeqCst),
            policies
        );
        let permission_calls = fixture.observations.permissions.lock().unwrap();
        assert_eq!(permission_calls.len(), permissions);
        let original = &fixture.original.as_ref().unwrap().view;
        for (view, reason) in permission_calls.iter() {
            assert_eq!(view, original);
            assert_eq!(
                *reason,
                if exact {
                    RecoveryConfinementReason::DispatchNotAuthorized
                } else {
                    RecoveryConfinementReason::ExactDefinitionUnavailable
                }
            );
        }
        for origin in fixture.observations.origins.lock().unwrap().iter() {
            assert_eq!(Some(origin), original.thread.origin.as_ref());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_arguments_dispose_two_panicking_handles_under_one_anchor() {
    for never_polled in [false, true] {
        let gates: Vec<_> = (0..4).map(|_| Gate::new()).collect();
        let _cleanups: Vec<_> = gates.iter().cloned().map(Cleanup).collect();
        let discovery = (!never_polled).then(Gate::new);
        let _discovery_cleanup = discovery.as_ref().map(|gate| Cleanup(gate.clone()));
        let mut fixture = Fixture::new(
            Seed::Empty,
            false,
            Capture::Pass,
            Commit::Pass,
            discovery.clone(),
        );
        let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(DropResolver {
            observations: fixture.observations.clone(),
            payload: Some(fixture.payload(gates[0].clone(), Some(gates[1].clone()))),
        });
        let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(DropAuthority {
            observations: fixture.observations.clone(),
            payload: Some(fixture.payload(gates[2].clone(), Some(gates[3].clone()))),
        });
        let (handle, dropped) = if never_polled {
            let shared = fixture.host().host.shared.clone();
            let (capability, token) = fixture.shutdown.begin_startup().unwrap();
            let carrier = OwnedShared::from_registered(shared, token);
            let arguments = Arguments::new(&carrier, &resolver, &authority);
            let inner = ownership::prepare(
                carrier.clone(),
                arguments,
                StartupPreparationLimits::default(),
                capability.clone(),
                Arc::new(model::Counters::default()),
            );
            let owned = StartupFuture::new(inner, carrier, capability);
            drop(resolver);
            drop(authority);
            fixture.close_host();
            (None, Some(tokio::task::spawn_blocking(move || drop(owned))))
        } else {
            let handle = fixture.start(&resolver, &authority).await;
            let discovery = discovery.as_ref().unwrap();
            discovery.entered().await;
            drop(resolver);
            drop(authority);
            fixture.close_host();
            discovery.release();
            (Some(handle), None)
        };
        let mut observations = Vec::new();
        for gate in &gates {
            gate.entered().await;
            observations.push((fixture.held(), fixture.shutdown.status().faulted));
            gate.release();
        }
        if let Some(handle) = handle {
            let _ = finished(&handle).await;
        }
        if let Some(task) = dropped {
            bounded(task)
                .await
                .expect("never-polled guarded disposal returns normally without opaque JoinError");
        }
        let final_status = fixture.shutdown.preparation_status();
        let retired = fixture.retire().await;
        fixture.unchanged(&retired);
        for gate in &gates {
            gate.positive();
        }
        if let Some(gate) = discovery {
            gate.positive();
        }
        assert!(observations
            .into_iter()
            .all(|(held, faulted)| held && faulted));
        assert!(retired.faulted);
        assert_eq!(final_status.phase, StartupPreparationPhase::Incomplete);
        assert_eq!(
            final_status.incomplete_reason,
            Some(StartupIncompleteReason::Faulted)
        );
        assert_eq!(fixture.observations.resolutions.load(Ordering::SeqCst), 0);
        assert!(fixture.observations.permissions.lock().unwrap().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_progress_counts_verified_receipt_but_not_ambiguous_commit() {
    for commit in [
        Commit::ErrorAfterCommit,
        Commit::PanicAfterCommit,
        Commit::Pass,
    ] {
        let verified_cleanup = matches!(commit, Commit::Pass);
        let discovery = verified_cleanup.then(Gate::new);
        let payload_gate = verified_cleanup.then(Gate::new);
        let _discovery_cleanup = discovery.as_ref().map(|gate| Cleanup(gate.clone()));
        let _payload_cleanup = payload_gate.as_ref().map(|gate| Cleanup(gate.clone()));
        let mut fixture = Fixture::new(
            Seed::Accepted,
            false,
            Capture::Pass,
            commit,
            discovery.clone(),
        );
        let (resolver, ordinary_authority) = fixture.handles(Resolution::Match);
        let authority: Arc<dyn RecoveryConfinementAuthority> = if let Some(gate) = &payload_gate {
            drop(ordinary_authority);
            Arc::new(DropAuthority {
                observations: fixture.observations.clone(),
                payload: Some(fixture.payload(gate.clone(), None)),
            })
        } else {
            ordinary_authority
        };
        let handle = fixture.start(&resolver, &authority).await;
        if let Some(gate) = &discovery {
            gate.entered().await;
        }
        drop(resolver);
        drop(authority);
        fixture.close_host();
        if let Some(gate) = &discovery {
            gate.release();
        }
        let mut held_tuple = None;
        if let Some(gate) = &payload_gate {
            gate.entered().await;
            held_tuple = Some((fixture.held(), handle.status()));
            gate.release();
        }
        let outcome = finished(&handle).await;
        let retired = fixture.retire().await;
        fixture.observations.no_dispatch();
        if let Some(gate) = discovery {
            gate.positive();
        }
        if let Some(gate) = payload_gate {
            gate.positive();
        }
        let mutations = fixture.observations.mutations.load(Ordering::SeqCst);
        if mutations == 0 {
            // Old forwarded preflight is the accepted compatibility setup;
            // no rollback/mutation/receipt is inferred from its refusal.
            fixture.unchanged(&retired);
            assert_eq!(outcome.confinement, StartupConfinementProgress::default());
            assert_eq!(fixture.observations.captures.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.observations.resolutions.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.observations.policies.load(Ordering::SeqCst), 1);
            assert!(fixture.observations.permissions.lock().unwrap().is_empty());
            assert_eq!(retired.faulted, verified_cleanup);
            let expected_reason = if verified_cleanup {
                StartupIncompleteReason::Faulted
            } else {
                StartupIncompleteReason::RecoveryRefused
            };
            assert!(
                matches!(outcome.preparation, StartupPreparationOutcome::Incomplete { reason, progress: StartupProgress { candidates: 1, admitted: 0, .. } } if reason == expected_reason)
            );
        }
        eprintln!("B4_STARTUP_PRIVATE_INTENDED_GUARD_ASSERTION receipt_vs_ambiguity commit={commit:?} retired");
        assert_eq!(
            mutations, 1,
            "the genuine backend write must be reached before progress evidence is tested"
        );
        assert!(retired.faulted);
        assert_eq!(fixture.observations.captures.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.observations.resolutions.load(Ordering::SeqCst), 3);
        assert_eq!(fixture.observations.policies.load(Ordering::SeqCst), 0);
        let permissions = fixture.observations.permissions.lock().unwrap();
        assert_eq!(permissions.len(), 2);
        for (view, reason) in permissions.iter() {
            assert_eq!(view, &fixture.original.as_ref().unwrap().view);
            assert_eq!(
                *reason,
                RecoveryConfinementReason::ExactDefinitionUnavailable
            );
        }
        drop(permissions);
        let mut original = fixture.original.as_ref().unwrap().clone();
        original.raw = fixture.expected_rows_at_reopen();
        assert_blocked_without_returned_receipt(
            &original,
            retired.state.as_ref().unwrap(),
            RecoveryConfinementReason::ExactDefinitionUnavailable,
            fixture.owner,
        );
        assert!(matches!(
            outcome.preparation,
            StartupPreparationOutcome::Incomplete {
                reason: StartupIncompleteReason::Faulted,
                progress: StartupProgress {
                    candidates: 1,
                    admitted: 0,
                    blocked: 0,
                    ..
                }
            }
        ));
        assert_eq!(
            outcome.confinement.confirmed_blocks,
            usize::from(verified_cleanup)
        );
        assert_eq!(outcome.confinement.already_blocked, 0);
        if let Some((held, tuple)) = held_tuple {
            assert!(held);
            assert_eq!(
                tuple.confinement.confirmed_blocks, 1,
                "verified evidence must be published before fallible argument cleanup"
            );
            assert_eq!(tuple.confinement.already_blocked, 0);
            assert!(
                tuple.confinement.confirmed_blocks + tuple.confinement.already_blocked
                    <= tuple.preparation.progress.candidates
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_startup_capture_drop_cannot_activate_or_escape_ownership() {
    let first = Gate::new();
    let replacement = Gate::new();
    let _first_cleanup = Cleanup(first.clone());
    let _replacement_cleanup = Cleanup(replacement.clone());
    let discovery = Gate::new();
    let _discovery_cleanup = Cleanup(discovery.clone());
    let mut fixture = Fixture::new(
        Seed::Empty,
        false,
        Capture::Pass,
        Commit::Pass,
        Some(discovery.clone()),
    );
    let payload = fixture.payload(first.clone(), Some(replacement.clone()));
    fixture.install_final_panic(payload);
    let (resolver, authority) = fixture.handles(Resolution::Match);
    let handle = fixture.start(&resolver, &authority).await;
    // Hold a real counted discovery read while every caller alias retires.
    // Only the supervised startup captures can then own last Shared Drop.
    discovery.entered().await;
    drop(resolver);
    drop(authority);
    fixture.close_host();
    discovery.release();
    let mut observations = Vec::new();
    for gate in [&first, &replacement] {
        gate.entered().await;
        observations.push((
            fixture.held(),
            fixture.shutdown.status().faulted,
            handle.status().preparation.phase,
        ));
        gate.release();
    }
    let outcome = finished(&handle).await;
    let retired = fixture.retire().await;
    fixture.unchanged(&retired);
    discovery.positive();
    first.positive();
    replacement.positive();
    assert!(observations.into_iter().all(|(held, faulted, phase)| held
        && faulted
        && phase == StartupPreparationPhase::Preparing));
    assert!(retired.faulted);
    assert!(matches!(
        outcome.preparation,
        StartupPreparationOutcome::Incomplete {
            reason: StartupIncompleteReason::Faulted,
            ..
        }
    ));
    assert_eq!(outcome.confinement, StartupConfinementProgress::default());
}
