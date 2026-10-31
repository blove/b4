mod confinement_support;

use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use confinement_support::*;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prior_owner_known_id_is_durably_confined_without_dispatch() {
    let directory = Directory::new();
    let prior = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let prior_owner = prior.owner_generation();
    let (original_view, original_approval) = seed_accepted(prior.as_ref());
    let original_snapshot = &original_approval.execution.snapshot;
    let original_journal = prior.journal(&original_snapshot.execution_id).unwrap();
    assert_eq!(
        original_journal,
        vec![JournalEntry::from(original_snapshot)]
    );
    let prior_weak = Arc::downgrade(&prior);
    drop(prior);
    assert!(prior_weak.upgrade().is_none());

    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let owner = store.owner_generation();
    assert_eq!(owner, prior_owner.checked_add(1).unwrap());
    store.readiness().unwrap();
    assert_eq!(
        store
            .thread_view(&original_view.thread.id)
            .unwrap()
            .unwrap(),
        original_view
    );
    assert_eq!(
        store
            .approval_view(&original_snapshot.execution_id)
            .unwrap()
            .unwrap(),
        original_approval
    );
    assert_eq!(
        store.journal(&original_snapshot.execution_id).unwrap(),
        original_journal
    );
    let raw_before = directory.rows();
    let target = RecoveryConfinementTarget::from_view(&original_view).unwrap();
    assert_eq!(target.thread_id, original_view.thread.id);
    assert_eq!(target.thread_generation, original_view.thread.generation);
    assert_eq!(
        target.resource_revision,
        original_view.thread.resource_revision
    );
    assert_eq!(target.execution_id, original_snapshot.execution_id);
    assert_eq!(
        target.execution_fence,
        ExecutionFence::from(original_snapshot)
    );
    assert_eq!(target.turn_id, original_snapshot.turn_id);

    let callback_calls = Arc::new(AtomicUsize::new(0));
    let validator_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let authority_calls: AuthorityCalls = Arc::new(Mutex::new(Vec::new()));
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: principal().application,
                revision: "assembly-v2".into(),
            },
            vec![registration(
                callback_calls.clone(),
                validator_calls.clone(),
            )],
        )
        .unwrap(),
    );
    assert!(application
        .definition_exact(&original_snapshot.definition)
        .is_none());
    let application_weak = Arc::downgrade(&application);
    let configuration = NativeHostConfig::new(
        application.clone(),
        ApprovalHostConfig::new("confinement-current-policy".into(), BTreeMap::new()),
    );
    assert_eq!(
        configuration.approval.service_config,
        ApprovalServiceConfig::RequiredDurable
    );
    let host = HttpHost::new_with_application(
        store.clone(),
        Arc::new(Auth),
        Arc::new(Policy {
            calls: policy_calls.clone(),
        }),
        configuration,
    )
    .unwrap();
    let resolver: Arc<dyn StartupPrincipalResolver> = Arc::new(Resolver {
        calls: resolver_calls.clone(),
    });
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(Authority {
        calls: authority_calls.clone(),
    });
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    let started_at = now_ms();
    // Retain the staged refusal as pure evidence; feature assertions come only
    // after normal shutdown, all aliases dropped and a real next writer opens.
    let observed = tokio::time::timeout(
        WAIT,
        host.confine_prior_owner(target, &resolver, &authority),
    )
    .await
    .expect("the finite known-ID confinement call remained bounded");
    let finished_at = now_ms();
    let after_view = store
        .thread_view(&original_view.thread.id)
        .unwrap()
        .unwrap();
    let after_approval = store
        .approval_view(&original_snapshot.execution_id)
        .unwrap()
        .unwrap();
    let after_journal = store.journal(&original_snapshot.execution_id).unwrap();
    let raw_after = directory.rows();

    let store_weak = Arc::downgrade(&store);
    let shutdown = host.shutdown_handle();
    quiesce(&shutdown).await;
    drop(host);
    drop(application);
    drop(store);
    drop(resolver);
    drop(authority);
    assert!(application_weak.upgrade().is_none());
    assert!(store_weak.upgrade().is_none());
    let successor = SqliteStore::open(directory.path()).unwrap();
    let next_owner = owner.checked_add(1).unwrap();
    assert_eq!(successor.owner_generation(), next_owner);
    successor.readiness().unwrap();
    assert_eq!(
        successor
            .thread_view(&original_view.thread.id)
            .unwrap()
            .unwrap(),
        after_view
    );
    assert_eq!(
        successor
            .approval_view(&original_snapshot.execution_id)
            .unwrap()
            .unwrap(),
        after_approval
    );
    assert_eq!(
        successor.journal(&original_snapshot.execution_id).unwrap(),
        after_journal
    );
    assert_reopen_tables(&raw_after, &directory.rows(), next_owner);
    drop(successor);

    assert_eq!(callback_calls.load(Ordering::SeqCst), 0);
    assert_eq!(validator_calls.load(Ordering::SeqCst), 0);
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 0);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 0);
    if let Err(error) = &observed {
        assert_eq!(
            error.reason(),
            RecoveryConfinementRefusal::Backend(
                RecoveryConfinementBackendError::UnsupportedOperation
            )
        );
        assert!(error.confirmed_receipt().is_none());
        assert_eq!(after_view, original_view);
        assert_eq!(after_approval, original_approval);
        assert_eq!(after_journal, original_journal);
        assert_eq!(raw_after, raw_before);
        assert!(authority_calls.lock().unwrap().is_empty());
    }
    eprintln!("legal Accepted owner N closed; actual owner N+1 source; normal healthy Quiescent; Store/App Weak absent; writer N+2 reread full views, journal and six tables before feature assertion");
    let outcome =
        observed.expect("explicitly authorized prior-owner work must durably become Confined");
    let RecoveryConfinementOutcome::Confined(receipt) = outcome else {
        panic!("a previously Accepted target must return actual Confined evidence");
    };
    assert_eq!(receipt.thread_id, original_view.thread.id);
    assert_eq!(receipt.thread_generation, original_view.thread.generation);
    assert_eq!(receipt.execution_id, original_snapshot.execution_id);
    assert_eq!(
        receipt.original_fence,
        ExecutionFence::from(original_snapshot)
    );
    assert_eq!(
        receipt.new_fence,
        ExecutionFence {
            revision: original_snapshot.revision.checked_add(1).unwrap(),
            owner_generation: owner,
        }
    );
    assert_eq!(
        receipt.original_resource_revision,
        original_view.thread.resource_revision
    );
    assert_eq!(
        receipt.new_resource_revision,
        original_view
            .thread
            .resource_revision
            .checked_add(1)
            .unwrap()
    );
    assert_eq!(receipt.original_turn_id, original_snapshot.turn_id);
    assert!(!original_snapshot
        .turn_history
        .contains(&receipt.fresh_turn_id));
    assert_eq!(receipt.route, original_snapshot.definition.route);
    assert_eq!(
        receipt.definition_revision,
        original_snapshot.definition.revision
    );
    assert_eq!(
        receipt.reason,
        RecoveryConfinementReason::ExactDefinitionUnavailable
    );
    assert_eq!(
        *authority_calls.lock().unwrap(),
        vec![
            (
                original_view.clone(),
                RecoveryConfinementReason::ExactDefinitionUnavailable
            ),
            (
                original_view.clone(),
                RecoveryConfinementReason::ExactDefinitionUnavailable
            ),
        ]
    );

    // Derive all preserved fields from the original, never from a later view.
    // Only the clock leaf is observed, after checking the actual call interval.
    let after_snapshot = &after_approval.execution.snapshot;
    assert!((started_at..=finished_at).contains(&after_snapshot.updated_at_ms));
    let mut expected_snapshot = original_snapshot.clone();
    expected_snapshot.status = ExecutionStatus::RecoveryBlocked;
    expected_snapshot.failure = Some("recovery_definition_unavailable".into());
    expected_snapshot.owner_generation = owner;
    expected_snapshot.revision = original_snapshot.revision.checked_add(1).unwrap();
    expected_snapshot.turn_id = receipt.fresh_turn_id.clone();
    expected_snapshot
        .turn_history
        .push(receipt.fresh_turn_id.clone());
    expected_snapshot.updated_at_ms = after_snapshot.updated_at_ms;
    b4_kernel::validate_snapshot(&expected_snapshot).unwrap();
    let mut expected_view = original_view.clone();
    expected_view.thread.resource_revision = original_view
        .thread
        .resource_revision
        .checked_add(1)
        .unwrap();
    expected_view.execution.as_mut().unwrap().snapshot = expected_snapshot.clone();
    assert_eq!(after_view, expected_view);
    let mut expected_approval = original_approval.clone();
    expected_approval.thread = expected_view.thread.clone();
    expected_approval.execution.snapshot = expected_snapshot.clone();
    assert_eq!(after_approval, expected_approval);
    let expected_entry = JournalEntry::from(&expected_snapshot);
    let mut expected_journal = original_journal;
    expected_journal.push(expected_entry.clone());
    assert_eq!(after_journal, expected_journal);
    assert_eq!(
        raw_after,
        expected_confined_tables(&raw_before, &expected_view, &expected_entry)
    );
}

// Independent public embedding extension; original positive remains exact above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audited_prior_blocked_history_stays_exact_without_any_trusted_callback() {
    let f = PublicFixture::new(PublicSource::PriorBlocked);
    let original = f.original.clone();
    let old = &original.approval.execution.snapshot;
    assert_eq!(old.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(
        old.failure.as_deref(),
        Some("execution_revision_unavailable")
    );
    assert!(old.owner_generation < f.owner);
    let resolver = f.resolver();
    let authority = f.authority();
    let observed = tokio::time::timeout(
        WAIT,
        f.host
            .confine_prior_owner(f.target.clone(), &resolver, &authority),
    )
    .await
    .expect("bounded public unchanged-history observation completed");
    drop(resolver);
    drop(authority);
    let (after, counters) = f.retire().await;
    assert_eq!(after, original);
    counters.assert_no_dispatch_or_resolution();
    assert!(counters.authority.lock().unwrap().is_empty());
    assert_eq!(
        observed.unwrap(),
        RecoveryConfinementOutcome::AlreadyBlocked
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_current_terminal_and_real_stale_targets_refuse_before_trusted_authority() {
    for (source, refusal) in [
        (
            PublicSource::CurrentAccepted,
            RecoveryConfinementBackendError::OwnerConflict,
        ),
        (
            PublicSource::CurrentBlocked,
            RecoveryConfinementBackendError::OwnerConflict,
        ),
        (
            PublicSource::PriorCancelled,
            RecoveryConfinementBackendError::Terminal,
        ),
        (
            PublicSource::StaleBeforeCancellation,
            RecoveryConfinementBackendError::TargetChanged,
        ),
    ] {
        let f = PublicFixture::new(source);
        let original = f.original.clone();
        let resolver = f.resolver();
        let authority = f.authority();
        let observed = tokio::time::timeout(
            WAIT,
            f.host
                .confine_prior_owner(f.target.clone(), &resolver, &authority),
        )
        .await
        .expect("bounded early target refusal completed");
        drop(resolver);
        drop(authority);
        let (after, counters) = f.retire().await;
        assert_eq!(after, original);
        counters.assert_no_dispatch_or_resolution();
        assert!(counters.authority.lock().unwrap().is_empty());
        let error = observed.expect_err("actual early target boundary must refuse");
        assert_eq!(error.reason(), RecoveryConfinementRefusal::Backend(refusal));
        assert!(error.confirmed_receipt().is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_sqlite_recovery_during_authority_invalidates_original_without_retry() {
    let f = PublicFixture::new(PublicSource::PriorAccepted);
    let original = f.original.clone();
    let old = &original.approval.execution.snapshot;
    let mut expected_snapshot = old.clone();
    expected_snapshot.status = ExecutionStatus::RecoveryBlocked;
    expected_snapshot.failure = Some("execution_revision_unavailable".into());
    expected_snapshot.owner_generation = f.owner;
    expected_snapshot.revision = old.revision.checked_add(1).unwrap();
    expected_snapshot.turn_id = TurnId::new("external-recovery-during-authority").unwrap();
    expected_snapshot
        .turn_history
        .push(expected_snapshot.turn_id.clone());
    expected_snapshot.updated_at_ms = 3;
    b4_kernel::validate_snapshot(&expected_snapshot).unwrap();
    let expected = public_expected_commit(&original, expected_snapshot);
    let resolver = f.resolver();
    let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(RecoverDuringAuthority {
        store: Arc::downgrade(&f.store),
        calls: f.counters.authority.clone(),
        facts: TransitionFacts { now_ms: 3 },
    });
    let observed = tokio::time::timeout(
        WAIT,
        f.host
            .confine_prior_owner(f.target.clone(), &resolver, &authority),
    )
    .await
    .expect("actual callback mutation and renewed capture remained finite");
    drop(resolver);
    drop(authority);
    let (after, counters) = f.retire().await;
    assert_eq!(after, expected);
    counters.assert_no_dispatch_or_resolution();
    assert_eq!(
        *counters.authority.lock().unwrap(),
        vec![(
            original.view,
            RecoveryConfinementReason::ExactDefinitionUnavailable
        )]
    );
    let error = observed.expect_err("the captured target became stale during authority");
    assert_eq!(
        error.reason(),
        RecoveryConfinementRefusal::Backend(RecoveryConfinementBackendError::TargetChanged)
    );
    assert!(error.confirmed_receipt().is_none());
}

// Additional independent public qualification after implementation; no new RED claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_retained_definition_unresolved_or_denied_dispatch_confines_without_effects() {
    for (resolution, allows, reason, contexts) in [
        (
            ExtraResolution::Unavailable,
            true,
            RecoveryConfinementReason::RequiredAuthorityUnavailable,
            0,
        ),
        (
            ExtraResolution::Available,
            false,
            RecoveryConfinementReason::DispatchNotAuthorized,
            2,
        ),
    ] {
        let f = ExtraFixture::new(true, resolution, allows);
        let before = f.original.clone();
        let calls = f.calls.clone();
        let owner = f.owner;
        let resolver = f.resolver();
        let authority = f.authority(None);
        let started = now_ms();
        let result = tokio::time::timeout(
            WAIT,
            f.host()
                .confine_prior_owner(f.target.clone(), &resolver, &authority),
        )
        .await
        .expect("finite public reason observation");
        let finished = now_ms();
        drop(resolver);
        drop(authority);
        let after = f.retire(false).await;
        calls.no_effects();
        calls.authority_calls(&before, reason, 2);
        assert_eq!(
            *calls.origins.lock().unwrap(),
            vec![before.view.thread.origin.clone().unwrap(); 2]
        );
        calls.recorded_contexts(&before, contexts);
        extra_confined(
            &before,
            &after,
            &extra_receipt(result),
            owner,
            reason,
            started,
            finished,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_first_and_renewed_mutation_authority_refusals_preserve_full_history() {
    for at in [1, 2] {
        for (denial, refusal) in [
            (
                RecoveryConfinementAuthorityRefusal::Denied,
                RecoveryConfinementRefusal::AuthorityDenied,
            ),
            (
                RecoveryConfinementAuthorityRefusal::Unavailable,
                RecoveryConfinementRefusal::AuthorityUnavailable,
            ),
        ] {
            let f = ExtraFixture::new(false, ExtraResolution::Available, true);
            let before = f.original.clone();
            let calls = f.calls.clone();
            let resolver = f.resolver();
            let authority = f.authority(Some((at, denial)));
            let result = tokio::time::timeout(
                WAIT,
                f.host()
                    .confine_prior_owner(f.target.clone(), &resolver, &authority),
            )
            .await
            .expect("finite separate authority refusal");
            drop(resolver);
            drop(authority);
            let after = f.retire(false).await;
            assert_eq!(after, before);
            calls.no_effects();
            assert!(calls.origins.lock().unwrap().is_empty());
            calls.recorded_contexts(&before, 0);
            calls.authority_calls(
                &before,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                at,
            );
            extra_refused(result, refusal);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_changed_renewed_dispatch_observation_never_substitutes_a_reason() {
    let f = ExtraFixture::new(true, ExtraResolution::UnavailableThenAvailable, true);
    let before = f.original.clone();
    let calls = f.calls.clone();
    let resolver = f.resolver();
    let authority = f.authority(None);
    let result = tokio::time::timeout(
        WAIT,
        f.host()
            .confine_prior_owner(f.target.clone(), &resolver, &authority),
    )
    .await
    .expect("finite changed dispatch observation");
    drop(resolver);
    drop(authority);
    let after = f.retire(false).await;
    assert_eq!(after, before);
    calls.no_effects();
    calls.authority_calls(
        &before,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
        1,
    );
    assert_eq!(
        *calls.origins.lock().unwrap(),
        vec![before.view.thread.origin.clone().unwrap(); 2]
    );
    calls.recorded_contexts(&before, 1);
    extra_refused(result, RecoveryConfinementRefusal::ObservationChanged);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_shutdown_cut_refuses_before_lease_and_preserves_a_won_commit() {
    for at in [1, 2] {
        let f = ExtraFixture::new(false, ExtraResolution::Available, true);
        let before = f.original.clone();
        let calls = f.calls.clone();
        let owner = f.owner;
        let gate = ExtraGate::new();
        let _cleanup = ExtraGateCleanup(gate.clone());
        let resolver = f.resolver();
        let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(ExtraHeldAuthority {
            calls: calls.clone(),
            at_call: at,
            call_gate: gate.clone(),
            cleanup_gate: None,
            mode: ExtraHeldMode::Callback,
            store: f.store.clone(),
            application: f.application.clone(),
        });
        let host = HttpHost::clone(f.host());
        let target = f.target.clone();
        let started = now_ms();
        let mut task = tokio::spawn(async move {
            host.confine_prior_owner(target, &resolver, &authority)
                .await
        });
        if let Some(early) = extra_gate_or_done(&gate, &mut task).await {
            let after = f.retire(false).await;
            assert_eq!(after, before);
            drop(early);
            panic!("required actual authority gate was skipped");
        }
        f.assert_held(false);
        assert_eq!(f.state(), before);
        // Default native capacity is eight. A second admitted request can
        // therefore reach the first real operation's live same-thread claim.
        let duplicate = if at == 1 {
            assert!(HostLimits::default().max_active_runs > 1);
            let second_resolver = f.resolver();
            let second_authority = f.authority(None);
            let observed = tokio::time::timeout(
                WAIT,
                f.host()
                    .confine_prior_owner(f.target.clone(), &second_resolver, &second_authority),
            )
            .await
            .expect("finite second request against actual live thread claim");
            drop(second_resolver);
            drop(second_authority);
            assert_eq!(f.state(), before);
            f.assert_held(false); // Second owner's refusal retired back to one.
            calls.no_effects();
            assert!(calls.origins.lock().unwrap().is_empty());
            calls.recorded_contexts(&before, 0);
            calls.authority_calls(
                &before,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                1,
            );
            Some(observed)
        } else {
            None
        };
        f.shutdown.begin_shutdown();
        gate.release();
        let result = tokio::time::timeout(WAIT, task)
            .await
            .expect("finite normal public completion")
            .expect("normal public join");
        let finished = now_ms();
        let after = f.retire(false).await;
        gate.positive();
        if let Some(observed) = duplicate {
            // The pure feature assertion follows original normal retirement.
            extra_refused(observed, RecoveryConfinementRefusal::ThreadBusy);
        }
        calls.no_effects();
        assert!(calls.origins.lock().unwrap().is_empty());
        calls.recorded_contexts(&before, 0);
        calls.authority_calls(
            &before,
            RecoveryConfinementReason::ExactDefinitionUnavailable,
            at,
        );
        if at == 1 {
            assert_eq!(after, before);
            extra_refused(result, RecoveryConfinementRefusal::AdmissionClosed);
        } else {
            extra_confined(
                &before,
                &after,
                &extra_receipt(result),
                owner,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                started,
                finished,
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_real_journal_abort_is_static_fault_with_full_rollback() {
    let f = ExtraFixture::new(false, ExtraResolution::Available, true);
    let before = f.original.clone();
    let calls = f.calls.clone();
    // A same-file test injection after normal host assembly. No second Store
    // writer/epoch is fabricated; remove it before genuine writer succession.
    let connection =
        rusqlite::Connection::open(f.directory.path().join("runtime.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER independent_confinement_abort BEFORE INSERT ON journal WHEN NEW.kind='commit' BEGIN SELECT RAISE(ABORT,'private-fixture-sql-text'); END").unwrap();
    let resolver = f.resolver();
    let authority = f.authority(None);
    let result = tokio::time::timeout(
        WAIT,
        f.host()
            .confine_prior_owner(f.target.clone(), &resolver, &authority),
    )
    .await
    .expect("finite real journal failure");
    let after_with_trigger = f.state();
    connection
        .execute_batch("DROP TRIGGER independent_confinement_abort")
        .unwrap();
    drop(connection);
    drop(resolver);
    drop(authority);
    let after = f.retire(true).await;
    assert_eq!(after_with_trigger, before);
    assert_eq!(after, before);
    calls.no_effects();
    assert!(calls.origins.lock().unwrap().is_empty());
    calls.recorded_contexts(&before, 0);
    calls.authority_calls(
        &before,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        2,
    );
    extra_refused(result, RecoveryConfinementRefusal::Unavailable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_abandoned_wait_keeps_callback_destructor_and_payload_owned_until_release() {
    for mode in [
        ExtraHeldMode::Callback,
        ExtraHeldMode::Destructor,
        ExtraHeldMode::PanicPayload,
    ] {
        let mut f = ExtraFixture::new(false, ExtraResolution::Available, true);
        let before = f.original.clone();
        let calls = f.calls.clone();
        let owner = f.owner;
        let entry = ExtraGate::new();
        let cleanup = ExtraGate::new();
        let _entry_cleanup = ExtraGateCleanup(entry.clone());
        let _drop_cleanup = ExtraGateCleanup(cleanup.clone());
        let resolver = f.resolver();
        let authority: Arc<dyn RecoveryConfinementAuthority> = Arc::new(ExtraHeldAuthority {
            calls: calls.clone(),
            at_call: 2,
            call_gate: entry.clone(),
            cleanup_gate: if matches!(mode, ExtraHeldMode::Callback) {
                None
            } else {
                Some(cleanup.clone())
            },
            mode,
            store: f.store.clone(),
            application: f.application.clone(),
        });
        let host = f.host.take().unwrap();
        let target = f.target.clone();
        let started = now_ms();
        let mut task = tokio::spawn(async move {
            host.confine_prior_owner(target, &resolver, &authority)
                .await
        });
        if let Some(early) = extra_gate_or_done(&entry, &mut task).await {
            let _persisted = f.retire_abandoned(false).await;
            drop(early);
            panic!("required entered public callback was skipped");
        }
        f.assert_held(false);
        assert_eq!(f.state(), before);
        task.abort();
        let cancelled = tokio::time::timeout(WAIT, task)
            .await
            .expect("finite abandoned waiter join")
            .expect_err("outer waiter was cancelled");
        assert!(cancelled.is_cancelled());
        // No external Host/Store/App/resolver/authority alias remains. The
        // callback's counted carrier alone keeps these resources and guards.
        f.assert_held(false);
        assert_eq!(f.state(), before);
        f.shutdown.begin_shutdown();
        assert_eq!(
            f.shutdown
                .wait_quiescent(std::time::Duration::ZERO)
                .await
                .unwrap(),
            ShutdownOutcome::Incomplete {
                active_owners: 1,
                faulted: false
            }
        );
        entry.release();
        if !matches!(mode, ExtraHeldMode::Callback) {
            tokio::time::timeout(WAIT, cleanup.entered())
                .await
                .expect("actual opaque payload or framework-last-handle Drop entered");
            let is_panic = matches!(mode, ExtraHeldMode::PanicPayload);
            f.assert_held(is_panic);
            if is_panic {
                assert_eq!(f.state(), before);
            } else {
                extra_persisted_without_receipt(
                    &before,
                    &f.state(),
                    owner,
                    RecoveryConfinementReason::ExactDefinitionUnavailable,
                    started,
                    now_ms(),
                    false,
                );
            }
            cleanup.release();
        }
        let expected_fault = matches!(mode, ExtraHeldMode::PanicPayload);
        let after = f.retire_abandoned(expected_fault).await;
        let finished = now_ms();
        entry.positive();
        if !matches!(mode, ExtraHeldMode::Callback) {
            cleanup.positive();
        }
        calls.no_effects();
        assert!(calls.origins.lock().unwrap().is_empty());
        calls.recorded_contexts(&before, 0);
        calls.authority_calls(
            &before,
            RecoveryConfinementReason::ExactDefinitionUnavailable,
            2,
        );
        if expected_fault {
            extra_unchanged_after_next_writer(&before, &after, owner);
        } else {
            extra_persisted_without_receipt(
                &before,
                &after,
                owner,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
                started,
                finished,
                true,
            );
        }
        // No returned result or receipt is attributed to the absent caller.
    }
}
