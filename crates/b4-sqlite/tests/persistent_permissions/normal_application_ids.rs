//! A UUID application identity through genuine Always approval and permission reuse.
use super::*;

const NORMAL_APPLICATION: &str = "07d7d468-c4f9-47e1-b421-ff889251df80";

#[test]
fn permission_normal_application_uuid_reuse_persists_success() {
    let ready = OriginalReadyControl::from_waiting(OriginalWaitingFixture::with_application(
        NORMAL_APPLICATION,
    ));
    let (ready_fixture, attempt, after_use) = original_reuse_ready_fixture_from(ready);
    let fixture = original_driver_fixture(ready_fixture, attempt.clone(), DriverReadFault::None);
    let hooks = OriginalReuseHooks(attempt.clone());
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
            application: NORMAL_APPLICATION.into(),
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
        task_id: TaskId::new("release").unwrap(),
        effect_id: EffectId::new("release-effect").unwrap(),
        attempt_id: attempt.clone(),
        input: expected_dispatch.snapshot.tasks[0].input.clone(),
    };
    let canonical_identity = PermissionAcceptance::decode_record(
        produced_text(&after_use.raw.tables[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.origin() == &expected_context.origin
            && record.application().application() == NORMAL_APPLICATION
            && record.application().revision() == "application-1"
    }) && PermissionDecision::decode_record(
        produced_text(&after_use.raw.tables[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.scope().origin() == &expected_context.origin
            && record.scope().application().application() == NORMAL_APPLICATION
            && record.scope().application().revision() == "application-1"
    }) && PermissionUse::decode_record(
        produced_text(&after_use.raw.tables[8][0][9]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.application().application() == NORMAL_APPLICATION
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
    let complete_valid = identity_valid
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
    eprintln!("normal-application driver returned_ok={} phase={phase} error_debug={failure_debug:?} failure={failure_observation:?} counters={counters:?} authorities={authorities:?} constraints={constraints:?} captures={:?} state={:?} view={:?} journals={:?} raw={:?}", actual.is_ok(), fixture.observed.captures.lock().unwrap(), after.executions, after.view, after.journals, after.raw.tables);
    drop(actual);
    drop(postclaim);
    drop(completion_capture);
    drop(failure_observation);
    let retired = fixture.retire(&after);
    eprintln!("normal-application complete_valid={complete_valid} retired={retired}");
    assert!(
        retired,
        "actual Driver result/failure/retained Work must retire before owned close/reopen/removal"
    );
    assert!(complete_valid, "normal UUID application reuse must return and persist exact Complete4 with callback1/typed1/raw0; reached={phase}");
}

#[test]
fn permission_normal_application_uuid_current_catalog_mismatch_blocks_callback() {
    let ready = OriginalReadyControl::from_waiting(OriginalWaitingFixture::with_application(
        NORMAL_APPLICATION,
    ));
    let (ready_fixture, attempt, after_use) = original_reuse_ready_fixture_from(ready);
    let fixture = original_driver_fixture(ready_fixture, attempt.clone(), DriverReadFault::None);
    let hooks = OriginalReuseHooks(attempt.clone());
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
            application: NORMAL_APPLICATION.into(),
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
        task_id: TaskId::new("release").unwrap(),
        effect_id: EffectId::new("release-effect").unwrap(),
        attempt_id: attempt.clone(),
        input: expected_dispatch.snapshot.tasks[0].input.clone(),
    };
    let canonical_identity = PermissionAcceptance::decode_record(
        produced_text(&after_use.raw.tables[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.origin() == &expected_context.origin
            && record.application().application() == NORMAL_APPLICATION
            && record.application().revision() == "application-1"
    }) && PermissionDecision::decode_record(
        produced_text(&after_use.raw.tables[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.scope().origin() == &expected_context.origin
            && record.scope().application().application() == NORMAL_APPLICATION
            && record.scope().application().revision() == "application-1"
    }) && PermissionUse::decode_record(
        produced_text(&after_use.raw.tables[8][0][9]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .is_ok_and(|record| {
        record.application().application() == NORMAL_APPLICATION
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
    // Keep the producer catalog genuine. Only the current application identity
    // differs; the allowing constraint cannot mask a missing runtime relation.
    let alternate_callback = fixture.callback.clone();
    let alternate_constraint = Arc::new(AllowConstraint);
    let alternate_application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "07d7d468-c4f9-47e1-b421-ff889251df81".into(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: fixture.ready.definition.clone(),
                effects: BTreeMap::from([(
                    fixture.ready.definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: alternate_callback.clone(),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let alternate_catalog = Arc::new(
        PermissionCatalog::new(
            alternate_application.clone(),
            vec![ToolBindingRegistration {
                definition: fixture.ready.definition.clone(),
                effect_id: fixture.ready.definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: alternate_constraint.clone(),
            }],
        )
        .unwrap(),
    );
    let alternate_catalog_weak = Arc::downgrade(&alternate_catalog);
    let alternate_application_weak = Arc::downgrade(&alternate_application);
    let alternate_constraint_weak = Arc::downgrade(&alternate_constraint);
    let actual = drive_permission_use_execution(
        PermissionUseDriverBindings {
            store: &fixture.observed,
            authority: &fixture.authority,
            producer_catalog: &fixture.catalog,
            current_catalog: &alternate_catalog,
            context: &fixture.ready.context,
            hooks: &hooks,
            capacity: &fixture.capacity,
        },
        fixture.ready.authorized.clone(),
    );
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
    let failure_debug = actual.as_ref().err().map(|failure| format!("{failure:?}"));
    let failure_observation = actual.as_ref().err().map(|failure| {
        (
            failure.callback_status(),
            failure.settled_outcome().cloned(),
            failure.usage(),
        )
    });
    // A refusal before the typed Dispatch is insufficient. Observe the actual
    // retained Running/InFlight3 state and the complete canonical commit delta.
    let refused = identity_valid
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status().is_none() && failure.settled_outcome().is_none()
        })
        && counters == (0, 0, 0, 1, 0, 0, 0)
        && after.executions[1] == expected_dispatch
        && after.executions[1].snapshot.revision == 3
        && after.executions[1].snapshot.status == ExecutionStatus::Running
        && after.executions[1].snapshot.tasks[0].status == TaskStatus::InFlight
        && completion_capture.is_none()
        && postclaim.as_ref().is_none_or(|claimed| claimed == &after)
        && original_reuse_commit_matches(
            &after_use,
            &after,
            &expected_dispatch,
            None,
            OriginalReuseSnapshotEncoding::CanonicalDispatch,
        );
    eprintln!(
        "normal-application mismatch: refused={refused} returned_ok={} error_debug={failure_debug:?} failure={failure_observation:?} counters={counters:?} captures={:?} state={:?} view={:?} journals={:?} raw={:?}",
        actual.is_ok(),
        fixture.observed.captures.lock().unwrap(),
        after.executions,
        after.view,
        after.journals,
        after.raw.tables
    );
    // The actual failure can borrow the alternate catalog and retain Work.
    // Release it first, then every alternate owner and the callback alias.
    drop(actual);
    drop(alternate_catalog);
    drop(alternate_application);
    drop(alternate_constraint);
    drop(alternate_callback);
    let alternate_retired = alternate_catalog_weak.upgrade().is_none()
        && alternate_application_weak.upgrade().is_none()
        && alternate_constraint_weak.upgrade().is_none();
    drop(postclaim);
    drop(completion_capture);
    drop(failure_observation);
    let retired = fixture.retire(&after);
    eprintln!(
        "normal-application mismatch: refused={refused} alternate_retired={alternate_retired} retired={retired}"
    );
    assert!(
        alternate_retired,
        "alternate catalog/application/constraint owners must retire after actual failure"
    );
    assert!(
        retired,
        "actual failure/retained Work and callback aliases must retire before owned close/reopen/removal"
    );
    assert!(
        refused,
        "different valid current UUID application must return failure after exact persisted Dispatch3 with callback0/outcome0 and full permission/original/Use preservation"
    );
}
