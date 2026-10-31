//! Host callback-entry admission through genuine Always approval and permission reuse.
use super::*;

const ENTRY_GATE_APPLICATION: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";

struct GateHooks {
    original: OriginalReuseHooks,
    allow: bool,
    entries: AtomicUsize,
}

impl GateHooks {
    fn new(attempt: AttemptId, allow: bool) -> Self {
        Self {
            original: OriginalReuseHooks(attempt),
            allow,
            entries: AtomicUsize::new(0),
        }
    }
}

impl PermissionUseDriverHooks for GateHooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        self.original.facts(current)
    }

    fn attempt_id(&self) -> AttemptId {
        self.original.attempt_id()
    }

    fn enter_callback(&self) -> bool {
        self.entries.fetch_add(1, Ordering::SeqCst);
        self.allow
    }
}

#[test]
fn permission_driver_entry_gate_allows_once_persisted_success() {
    let ready = OriginalReadyControl::from_waiting(OriginalWaitingFixture::with_identifiers(
        ENTRY_GATE_APPLICATION,
        "publish-monthly-report",
        "reports.publish-monthly",
    ));
    let (ready_fixture, attempt, after_use) = original_reuse_ready_fixture_from(ready);
    let fixture = original_driver_fixture(ready_fixture, attempt.clone(), DriverReadFault::None);
    let hooks = GateHooks::new(attempt.clone(), true);
    // Project Dispatch independently from the actual authorized Ready2, then
    // Complete from that projection, without trusting the Driver fixture claim.
    let expected_dispatch = StoredExecution {
        thread_id: fixture.ready.thread.clone(),
        snapshot: transition(
            &fixture.ready.authorized.snapshot,
            KernelCommand::Dispatch {
                task_id: fixture.ready.task.clone(),
                attempt_id: attempt.clone(),
            },
            facts(108),
        )
        .unwrap()
        .snapshot,
    };
    let expected_context = InvocationContext {
        thread_id: fixture.ready.thread.clone(),
        thread_generation: after_use.view.thread.generation.clone(),
        origin: ThreadOrigin {
            application: ENTRY_GATE_APPLICATION.into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        application_revision: "application-1".into(),
        execution_revision: ExecutionRevision::new("definition-1").unwrap(),
        policy_revision: "policy-1".into(),
    };
    let expected_invocation = EffectInvocation {
        execution_id: fixture.ready.second_execution.clone(),
        turn_id: expected_dispatch.snapshot.turn_id.clone(),
        owner_generation: expected_dispatch.snapshot.owner_generation,
        task_id: TaskId::new("publish-monthly-report").unwrap(),
        effect_id: EffectId::new("reports.publish-monthly").unwrap(),
        attempt_id: attempt.clone(),
        input: expected_dispatch.snapshot.tasks[0].input.clone(),
    };
    let canonical_identity = PermissionAcceptance::decode_record(
        produced_text(&after_use.raw.tables[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.origin() == &expected_context.origin
            && record.application().application() == ENTRY_GATE_APPLICATION
            && record.application().revision() == "application-1"
    }) && PermissionDecision::decode_record(
        produced_text(&after_use.raw.tables[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.scope().origin() == &expected_context.origin
            && record.scope().application().application() == ENTRY_GATE_APPLICATION
            && record.scope().application().revision() == "application-1"
    }) && PermissionUse::decode_record(
        produced_text(&after_use.raw.tables[8][0][9]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.application().application() == ENTRY_GATE_APPLICATION
            && record.application().revision() == "application-1"
    });
    let identity_valid = canonical_identity
        && fixture.ready.context.actor == expected_context.origin
        && fixture.callback.context == expected_context
        && fixture.constraint.context == expected_context
        && fixture.callback.invocation == expected_invocation
        && fixture.constraint.invocation == expected_invocation
        && fixture.ready.authorized.snapshot.revision == 2
        && expected_dispatch.snapshot.revision == 3
        && fixture.claimed == expected_dispatch;
    let expected_complete = StoredExecution {
        thread_id: fixture.ready.thread.clone(),
        snapshot: transition(
            &expected_dispatch.snapshot,
            KernelCommand::Complete {
                task_id: fixture.ready.task.clone(),
                attempt_id: attempt,
                result: TaskResult {
                    output: json!(true),
                    writes: vec![],
                },
            },
            facts(109),
        )
        .unwrap()
        .snapshot,
    };
    let actual = fixture.drive(&hooks);
    let after = fixture.capture();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let completion_capture = fixture.observed.captured.lock().unwrap().take();
    let counters = (
        fixture.callback_calls.load(Ordering::SeqCst),
        fixture.callback_matches.load(Ordering::SeqCst),
        fixture.ready.calls.load(Ordering::SeqCst),
        fixture.observed.entries.load(Ordering::SeqCst),
        fixture.observed.outcome_entries.load(Ordering::SeqCst),
        fixture.observed.outcome_forwards.load(Ordering::SeqCst),
        fixture.observed.raw_commits.load(Ordering::SeqCst),
    );
    let authorities = (
        fixture.authority.calls.load(Ordering::SeqCst),
        fixture.authority.matches.load(Ordering::SeqCst),
        fixture.authority.ready_matches.load(Ordering::SeqCst),
        fixture.authority.claimed_matches.load(Ordering::SeqCst),
    );
    let constraints = (
        fixture.constraint.calls.load(Ordering::SeqCst),
        fixture.constraint.matches.load(Ordering::SeqCst),
    );
    let failure_debug = actual.as_ref().err().map(|failure| format!("{failure:?}"));
    let failure_observation = actual.as_ref().err().map(|failure| {
        (
            failure.callback_status(),
            failure.settled_outcome().cloned(),
            failure.usage(),
        )
    });
    // A returned failure retains its actual private Receipt/Work. Exposed status,
    // settled outcome and cumulative account are recorded before dropping it.
    let phase = if failure_observation
        .as_ref()
        .is_some_and(|(_, outcome, _)| outcome.is_some())
    {
        "settled-work"
    } else if counters.0 > 0 {
        "callback-reached"
    } else if after.executions[1] == fixture.claimed {
        "claimed"
    } else if after == after_use {
        "preclaim"
    } else {
        "other-observed"
    };
    let complete_valid = hooks.entries.load(Ordering::SeqCst) == 1
        && identity_valid
        && actual
            .as_ref()
            .is_ok_and(|stored| stored == &expected_complete && stored == &after.executions[1])
        && expected_complete.snapshot.revision == 4
        && expected_complete.snapshot.status == ExecutionStatus::Completed
        && expected_complete.snapshot.tasks[0].status == TaskStatus::Succeeded
        && fixture.callback_once()
        && counters == (1, 1, 0, 1, 1, 1, 0)
        && authorities.0 == authorities.1
        && authorities.2 > 0
        && authorities.3 > 0
        && constraints.0 == constraints.1
        && constraints.0 > 0
        && postclaim.as_ref().is_some_and(|claimed| {
            original_reuse_commit_matches(
                &after_use,
                claimed,
                &expected_dispatch,
                None,
                OriginalReuseSnapshotEncoding::CanonicalDispatch,
            )
        })
        && completion_capture.as_ref().is_some_and(|claimed| {
            postclaim.as_ref() == Some(claimed)
                && original_reuse_commit_matches(
                    claimed,
                    &after,
                    &expected_complete,
                    None,
                    OriginalReuseSnapshotEncoding::Struct,
                )
        });
    eprintln!(
        "entry-gate-allow driver returned_ok={} phase={phase} error_debug={failure_debug:?} failure={failure_observation:?} counters={counters:?} authorities={authorities:?} constraints={constraints:?} captures={:?} state={:?} view={:?} journals={:?} raw={:?}",
        actual.is_ok(),
        fixture.observed.captures.lock().unwrap(),
        after.executions,
        after.view,
        after.journals,
        after.raw.tables
    );
    drop(actual);
    drop(postclaim);
    drop(completion_capture);
    drop(failure_observation);
    let retired = fixture.retire(&after);
    eprintln!("entry-gate-allow complete_valid={complete_valid} retired={retired}");
    assert!(
        retired,
        "actual Driver result/failure/retained Work must retire before owned close/reopen/removal"
    );
    assert!(
        complete_valid,
        "normal task/effect reuse must return and persist exact Complete4 with callback1/typed1/raw0; reached={phase}"
    );
}

#[test]
fn permission_driver_entry_gate_refuses_before_callback() {
    let ready = OriginalReadyControl::from_waiting(OriginalWaitingFixture::with_identifiers(
        ENTRY_GATE_APPLICATION,
        "publish-monthly-report",
        "reports.publish-monthly",
    ));
    let (ready_fixture, attempt, after_use) = original_reuse_ready_fixture_from(ready);
    let fixture = original_driver_fixture(ready_fixture, attempt.clone(), DriverReadFault::None);
    let hooks = GateHooks::new(attempt.clone(), false);
    // Project Dispatch independently from the actual authorized Ready2,
    // without trusting the Driver fixture claim.
    let expected_dispatch = StoredExecution {
        thread_id: fixture.ready.thread.clone(),
        snapshot: transition(
            &fixture.ready.authorized.snapshot,
            KernelCommand::Dispatch {
                task_id: fixture.ready.task.clone(),
                attempt_id: attempt.clone(),
            },
            facts(108),
        )
        .unwrap()
        .snapshot,
    };
    let expected_context = InvocationContext {
        thread_id: fixture.ready.thread.clone(),
        thread_generation: after_use.view.thread.generation.clone(),
        origin: ThreadOrigin {
            application: ENTRY_GATE_APPLICATION.into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        application_revision: "application-1".into(),
        execution_revision: ExecutionRevision::new("definition-1").unwrap(),
        policy_revision: "policy-1".into(),
    };
    let expected_invocation = EffectInvocation {
        execution_id: fixture.ready.second_execution.clone(),
        turn_id: expected_dispatch.snapshot.turn_id.clone(),
        owner_generation: expected_dispatch.snapshot.owner_generation,
        task_id: TaskId::new("publish-monthly-report").unwrap(),
        effect_id: EffectId::new("reports.publish-monthly").unwrap(),
        attempt_id: attempt.clone(),
        input: expected_dispatch.snapshot.tasks[0].input.clone(),
    };
    let canonical_identity = PermissionAcceptance::decode_record(
        produced_text(&after_use.raw.tables[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.origin() == &expected_context.origin
            && record.application().application() == ENTRY_GATE_APPLICATION
            && record.application().revision() == "application-1"
    }) && PermissionDecision::decode_record(
        produced_text(&after_use.raw.tables[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.scope().origin() == &expected_context.origin
            && record.scope().application().application() == ENTRY_GATE_APPLICATION
            && record.scope().application().revision() == "application-1"
    }) && PermissionUse::decode_record(
        produced_text(&after_use.raw.tables[8][0][9]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.application().application() == ENTRY_GATE_APPLICATION
            && record.application().revision() == "application-1"
    });
    let identity_valid = canonical_identity
        && fixture.ready.context.actor == expected_context.origin
        && fixture.callback.context == expected_context
        && fixture.constraint.context == expected_context
        && fixture.callback.invocation == expected_invocation
        && fixture.constraint.invocation == expected_invocation
        && fixture.ready.authorized.snapshot.revision == 2
        && expected_dispatch.snapshot.revision == 3
        && fixture.claimed == expected_dispatch;
    let mut actual = fixture.drive(&hooks);
    let after = fixture.capture();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let completion_capture = fixture.observed.captured.lock().unwrap().take();
    let counters = (
        fixture.callback_calls.load(Ordering::SeqCst),
        fixture.callback_matches.load(Ordering::SeqCst),
        fixture.ready.calls.load(Ordering::SeqCst),
        fixture.observed.entries.load(Ordering::SeqCst),
        fixture.observed.outcome_entries.load(Ordering::SeqCst),
        fixture.observed.outcome_forwards.load(Ordering::SeqCst),
        fixture.observed.raw_commits.load(Ordering::SeqCst),
    );
    let authorities = (
        fixture.authority.calls.load(Ordering::SeqCst),
        fixture.authority.matches.load(Ordering::SeqCst),
        fixture.authority.ready_matches.load(Ordering::SeqCst),
        fixture.authority.claimed_matches.load(Ordering::SeqCst),
    );
    let constraints = (
        fixture.constraint.calls.load(Ordering::SeqCst),
        fixture.constraint.matches.load(Ordering::SeqCst),
    );
    let failure_observation = actual.as_ref().err().map(|failure| {
        (
            failure.callback_status(),
            failure.settled_outcome().cloned(),
            failure.usage(),
        )
    });
    let refused = identity_valid
        && hooks.entries.load(Ordering::SeqCst) == 1
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status().is_none()
                && failure.settled_outcome().is_none()
                && failure.usage().completion_entries() == 0
                && failure.usage().candidate_entries() == 0
                && failure.usage().committed_completions() == 0
        })
        && counters == (0, 0, 0, 1, 0, 0, 0)
        && authorities.0 == authorities.1
        && authorities.2 > 0
        && authorities.3 > 0
        && constraints.0 == constraints.1
        && constraints.0 > 0
        && completion_capture.is_none()
        && postclaim.as_ref() == Some(&after)
        && after.executions[1] == expected_dispatch
        && original_reuse_commit_matches(
            &after_use,
            &after,
            &expected_dispatch,
            None,
            OriginalReuseSnapshotEncoding::CanonicalDispatch,
        );
    // Retry the actual failure while its real uninvoked Work is retained.
    // Storage retry cannot manufacture a settled outcome or re-enter the gate.
    let retry_refused = actual.as_mut().err().is_some_and(|failure| {
        matches!(
            failure.retry_settlement(&facts(110)),
            Err(PermissionError::UnsupportedProfile)
        ) && failure.callback_status().is_none()
            && failure.settled_outcome().is_none()
            && failure.usage().completion_entries() == 0
            && failure.usage().candidate_entries() == 0
            && failure.usage().committed_completions() == 0
    });
    let after_retry = fixture.capture();
    let retry_counters = (
        fixture.callback_calls.load(Ordering::SeqCst),
        fixture.callback_matches.load(Ordering::SeqCst),
        fixture.ready.calls.load(Ordering::SeqCst),
        fixture.observed.entries.load(Ordering::SeqCst),
        fixture.observed.outcome_entries.load(Ordering::SeqCst),
        fixture.observed.outcome_forwards.load(Ordering::SeqCst),
        fixture.observed.raw_commits.load(Ordering::SeqCst),
    );
    let retry_unchanged = retry_refused
        && hooks.entries.load(Ordering::SeqCst) == 1
        && retry_counters == counters
        && after_retry == after;
    eprintln!(
        "entry-gate-refuse refused={refused} retry_unchanged={retry_unchanged} failure={failure_observation:?} counters={counters:?} authorities={authorities:?} constraints={constraints:?} actual={actual:?}"
    );
    // Release the actual failure and retained Work before owned close/reopen.
    drop(actual);
    drop(postclaim);
    drop(completion_capture);
    drop(failure_observation);
    let retired = fixture.retire(&after_retry);
    assert!(
        retired,
        "actual failure/retained uninvoked Work must retire before owned close/reopen/removal"
    );
    assert!(
        refused,
        "entry refusal must preserve exact full Dispatch3 with gate1/callback0/outcome0/raw0 and absent callback status/outcome"
    );
    assert!(
        retry_unchanged,
        "storage-only retry must refuse the actual uninvoked Work without re-entering the hook or changing Dispatch3"
    );
}
