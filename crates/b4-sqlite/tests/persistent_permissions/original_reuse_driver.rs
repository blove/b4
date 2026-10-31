//! Genuine original UUID Always -> Cancel -> reopen -> NewUse -> Driver scout.
use super::*;

struct OriginalReuseHooks(AttemptId);
impl PermissionUseDriverHooks for OriginalReuseHooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        DriverHooks.facts(current)
    }
    fn attempt_id(&self) -> AttemptId {
        self.0.clone()
    }
}

enum OriginalReuseSnapshotEncoding {
    Struct,
    CanonicalDispatch,
}

// Independently project the complete typed/raw delta of one kernel commit.
// Permission source rows and the original execution are included in equality.
fn original_reuse_commit_matches(
    before: &DriverCapture,
    after: &DriverCapture,
    expected: &StoredExecution,
    additional_use: Option<Vec<rusqlite::types::Value>>,
    encoding: OriginalReuseSnapshotEncoding,
) -> bool {
    use rusqlite::types::Value;
    let mut thread = before.view.thread.clone();
    thread.resource_revision += 1;
    let mut journals = before.journals.clone();
    let entry = JournalEntry::from(&expected.snapshot);
    journals[1].push(entry.clone());
    let mut raw = before.raw.tables.clone();
    raw[1][0][5] = Value::Blob(thread.resource_revision.to_be_bytes().to_vec());
    raw[2][1][2] = Value::Blob(expected.snapshot.revision.to_be_bytes().to_vec());
    raw[2][1][3] = Value::Blob(expected.snapshot.owner_generation.to_be_bytes().to_vec());
    // Dispatch emits the alphabetical canonical walk; Ready authorization and
    // outcome settlement emit the Snapshot struct's Serialize field order.
    // Derive both from the independent kernel snapshot, preserving raw equality.
    let snapshot_payload = match encoding {
        OriginalReuseSnapshotEncoding::Struct => serde_json::to_string(&expected.snapshot).unwrap(),
        OriginalReuseSnapshotEncoding::CanonicalDispatch => {
            serde_json::to_string(&serde_json::to_value(&expected.snapshot).unwrap()).unwrap()
        }
    };
    raw[2][1][4] = Value::Text(snapshot_payload);
    let sequence = first_claim_journal_sequence(&raw[3].last().unwrap()[0]) + 1;
    raw[3].push(vec![
        Value::Integer(sequence),
        Value::Text("commit".into()),
        Value::Text(thread.id.to_string()),
        Value::Text(entry.execution_id.to_string()),
        Value::Blob(entry.revision.to_be_bytes().to_vec()),
        Value::Blob(entry.owner_generation.to_be_bytes().to_vec()),
        Value::Text(serde_json::to_string(&entry.status).unwrap()),
        Value::Blob(entry.updated_at_ms.to_be_bytes().to_vec()),
        Value::Text(serde_json::to_string(&entry).unwrap()),
    ]);
    if let Some(row) = additional_use {
        raw[8].push(row);
    }
    after.view
        == (ThreadView {
            thread,
            execution: Some(expected.clone()),
        })
        && after.executions == [before.executions[0].clone(), expected.clone()]
        && after.journals == journals
        && after.raw.tables == raw
}

// A prerequisite refusal still owns the actual two-execution state. Reuse the
// normal Driver retirement, without invoking Driver or inventing a Ready2.
fn original_reuse_retire_ready(ready: DriverReadyFixture, actual: &DriverCapture) -> bool {
    let DriverReadyFixture {
        directory,
        next,
        thread,
        execution,
        second_execution,
        calls,
        ..
    } = ready;
    let callbacks_zero = calls.load(Ordering::SeqCst) == 0 && Arc::strong_count(&calls) == 1;
    drop(calls);
    let owner2 = next.owner_generation();
    let sole = Arc::strong_count(&next) == 1;
    let readiness2 = next.readiness().is_ok();
    drop(next);
    let owner3 = SqliteStore::open(&directory.0).unwrap();
    let reopened = original_reuse_capture(
        &owner3,
        &directory.0,
        &thread,
        &execution,
        &second_execution,
    );
    let exact = reopened.view == actual.view
        && reopened.executions == actual.executions
        && reopened.journals == actual.journals
        && reopened.raw.tables[1..] == actual.raw.tables[1..]
        && reopened.raw.tables[0][0][..3] == actual.raw.tables[0][0][..3]
        && produced_u64(&actual.raw.tables[0][0][3]) == owner2
        && produced_u64(&reopened.raw.tables[0][0][3]) == owner2 + 1
        && owner3.owner_generation() == owner2 + 1;
    let readiness3 = owner3.readiness().is_ok();
    drop(reopened);
    drop(owner3);
    let path = directory.0.clone();
    drop(directory);
    callbacks_zero && sole && readiness2 && readiness3 && exact && !path.exists()
}

fn original_reuse_capture(
    store: &SqliteStore,
    directory: &std::path::Path,
    thread: &ThreadId,
    original: &ExecutionId,
    current: &ExecutionId,
) -> DriverCapture {
    DriverCapture {
        view: store.thread_view(thread).unwrap().unwrap(),
        executions: [original, current].map(|id| store.execution(id).unwrap().unwrap()),
        journals: [original, current].map(|id| store.journal(id).unwrap()),
        raw: driver_raw_nine(directory),
    }
}

fn original_reuse_ready_fixture() -> (DriverReadyFixture, AttemptId, DriverCapture) {
    original_reuse_ready_fixture_from(OriginalReadyControl::new())
}

fn original_reuse_ready_fixture_from(
    control: OriginalReadyControl,
) -> (DriverReadyFixture, AttemptId, DriverCapture) {
    let OriginalReadyControl {
        fixture,
        ready,
        context,
        acceptance,
        decision,
        ..
    } = control;
    let target = RunCancellationTarget::from_view(&ready.view).unwrap();
    let expected_cancel = StoredExecution {
        thread_id: fixture.thread.clone(),
        snapshot: transition(
            &ready.execution.snapshot,
            KernelCommand::Cancel {
                turn_id: ready.execution.snapshot.turn_id.clone(),
                owner_generation: fixture.original_owner,
            },
            facts(104),
        )
        .unwrap()
        .snapshot,
    };
    let authorities = Arc::new(AtomicUsize::new(0));
    let captures = Arc::new(AtomicUsize::new(0));
    let terminals = Arc::new(AtomicUsize::new(0));
    let authority = ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: ready.view.thread.clone(),
            execution: ready.execution.clone(),
        },
        expected_target: target.clone(),
        calls: authorities.clone(),
    };
    let observed = ReadyCancellationObservedStore {
        inner: fixture.store.clone(),
        captures: captures.clone(),
        terminal_entries: terminals.clone(),
    };
    let mut cancellation_account = PermissionEvidenceBudget::new();
    let cancellation = cancel_permission_ready_with_normal_inputs(
        &observed,
        &authority,
        fixture.catalog.as_ref(),
        &PermissionReadyCancellationRequest {
            context: &context,
            target: &target,
            facts: facts(104),
        },
        &mut cancellation_account,
    );
    let cancelled = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let cancellation_counts = (
        authorities.load(Ordering::SeqCst),
        captures.load(Ordering::SeqCst),
        terminals.load(Ordering::SeqCst),
    );
    let cancellation_usage = produced_usage(&cancellation_account);
    let cancellation_code = cancellation
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let cancellation_valid = cancellation
        .as_ref()
        .is_ok_and(|stored| stored == &expected_cancel)
        && cancelled_capture_matches(&ready, &cancelled, &expected_cancel)
        && cancellation_counts == (2, 3, 1)
        && fixture.calls.load(Ordering::SeqCst) == 0;
    drop(cancellation);
    drop(cancellation_account);
    drop(observed);
    drop(authority);
    drop(authorities);
    drop(captures);
    drop(terminals);
    if !cancellation_valid {
        let retired =
            retire_original_cancellation(fixture, &cancelled, Some((&acceptance, &decision)));
        eprintln!("original-reuse predecessor=cancellation error={cancellation_code} counts={cancellation_counts:?} usage={cancellation_usage:?} retired={retired}");
        assert!(retired, "actual cancellation predecessor state must retire");
        panic!("original reuse cancellation prerequisite failed; error={cancellation_code}");
    }
    let OriginalWaitingFixture {
        directory,
        store,
        application,
        catalog,
        calls,
        thread,
        execution,
        original_owner,
        request,
        ..
    } = fixture;
    let application_weak = Arc::downgrade(&application);
    let catalog_weak = Arc::downgrade(&catalog);
    let definition = ready.execution.snapshot.definition.clone();
    let task = definition.batches[0][0].id.clone();
    drop(request);
    drop(catalog);
    drop(application);
    let owner1_anchors = application_weak.upgrade().is_none()
        && catalog_weak.upgrade().is_none()
        && Arc::strong_count(&calls) == 1
        && calls.load(Ordering::SeqCst) == 0;
    let owner1_sole = Arc::strong_count(&store) == 1;
    let owner1_readiness = store.readiness().is_ok();
    drop(store);
    let next = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let reopened = OriginalCapture::read(next.as_ref(), &directory.0, &thread, &execution);
    let owner2_exact = owner1_anchors
        && owner1_sole
        && owner1_readiness
        && next.owner_generation() == original_owner + 1
        && reopened.view == cancelled.view
        && reopened.execution == cancelled.execution
        && reopened.journal == cancelled.journal
        && reopened.raw.tables[1..] == cancelled.raw.tables[1..]
        && reopened.raw.tables[0][0][..3] == cancelled.raw.tables[0][0][..3]
        && produced_u64(&cancelled.raw.tables[0][0][3]) == original_owner
        && produced_u64(&reopened.raw.tables[0][0][3]) == original_owner + 1
        && next.readiness().is_ok();
    let second_execution = ExecutionId::new("b81bf932-e606-4ba0-90ef-5c9eb23a2a1b").unwrap();
    let second_turn = TurnId::new("d2b2f332-4364-435e-8aee-57570303a293").unwrap();
    let attempt = AttemptId::new("e759e778-969e-4747-a207-e1ed59ad99fa").unwrap();
    let current_input: serde_json::Value = serde_json::from_str(
        r#"{"release":{"name":"normal application run","options":[{"enabled":true,"note":"caf\u00e9 \u2603 \ud83d\ude80"},null,17,-4,1.25],"metadata":{"labels":["production","weekly"],"retry":false,"description":"A modest structured application payload with nested objects and arrays."}}}"#
    ).unwrap();
    let initial = new_execution(
        second_execution.clone(),
        second_turn.clone(),
        next.owner_generation(),
        definition.clone(),
        Map::from_iter([("input".into(), current_input.clone())]),
        facts(105),
    )
    .unwrap();
    let admitted = next
        .admit_fenced(
            &thread,
            &reopened.view.thread.generation,
            reopened.view.thread.resource_revision,
            initial,
        )
        .unwrap();
    let started = next
        .commit(
            &second_execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(106)).unwrap(),
        )
        .unwrap();
    let before_use = original_reuse_capture(
        next.as_ref(),
        &directory.0,
        &thread,
        &execution,
        &second_execution,
    );
    // Recreate the same immutable application/catalog identities after owner1's
    // aliases retire. Current callback authority is assembled only after Ready.
    let setup_application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(CountCalls(calls.clone())),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let setup_catalog = PermissionCatalog::new(
        setup_application.clone(),
        vec![ToolBindingRegistration {
            definition: definition.clone(),
            effect_id: definition.batches[0][0].effect_id.clone(),
            tool_id: StableToolId::new("release-tool").unwrap(),
            tool_revision: StableToolRevision::new("tool-1").unwrap(),
            constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
            constraint: Arc::new(AllowConstraint),
        }],
    )
    .unwrap();
    let use_id = PermissionUseId::new(
        "b4pu_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    )
    .unwrap();
    let command = b4_kernel::PermissionAuthorizationCommand::capture(
        &started.snapshot,
        task.clone(),
        use_id.clone(),
    )
    .unwrap();
    let expected_ready = StoredExecution {
        thread_id: thread.clone(),
        snapshot: b4_kernel::authorize_ready(&started.snapshot, &command, facts(107))
            .unwrap()
            .snapshot,
    };
    let use_resources = Arc::new(AtomicUsize::new(0));
    let use_entries = Arc::new(AtomicUsize::new(0));
    let use_authorities = Arc::new(AtomicUsize::new(0));
    let unused_captures = Arc::new(AtomicUsize::new(0));
    let unused_terminals = Arc::new(AtomicUsize::new(0));
    let observed_use = ReadyUseObservedStore {
        inner: next.clone(),
        resources: use_resources.clone(),
        captures: unused_captures.clone(),
        terminal_entries: unused_terminals.clone(),
        use_entries: use_entries.clone(),
    };
    let use_authority = ReadyUseAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_use.view.thread.clone(),
            execution: started.clone(),
        },
        calls: use_authorities.clone(),
    };
    let mut use_account = PermissionEvidenceBudget::new();
    let initial_use_usage = produced_usage(&use_account);
    let authorization = try_authorize_permission_ready(
        &observed_use,
        &use_authority,
        &setup_catalog,
        fence(next.as_ref(), &thread),
        &PermissionReadyAuthorizationRequest {
            context: &context,
            command: &command,
            facts: facts(107),
        },
        &mut use_account,
    );
    let after_use = original_reuse_capture(
        next.as_ref(),
        &directory.0,
        &thread,
        &execution,
        &second_execution,
    );
    let use_usage = produced_usage(&use_account);
    let use_counts = (
        use_resources.load(Ordering::SeqCst),
        use_entries.load(Ordering::SeqCst),
        use_authorities.load(Ordering::SeqCst),
        unused_captures.load(Ordering::SeqCst),
        unused_terminals.load(Ordering::SeqCst),
    );
    let use_code = authorization
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or(if matches!(&authorization, Ok(None)) {
            "miss"
        } else {
            "ok"
        });
    let linked_use = after_use.raw.tables[8].first().is_some_and(|row| {
        let mut budget = PermissionEvidenceBudget::new();
        PermissionUse::decode_record(produced_text(&row[9]).as_bytes(), &mut budget).is_ok_and(
            |record| {
                let scope = decision.scope();
                let expected_digest = permission_call_digest(
                    &started.snapshot.tasks[0],
                    &mut PermissionEvidenceBudget::new(),
                )
                .unwrap();
                record.record_version() == 1
                    && record.permission_version() == decision.record_version()
                    && record.use_id() == &use_id
                    && record.permission_id() == decision.permission_id()
                    && record.thread_id() == &thread
                    && record.thread_generation() == &before_use.view.thread.generation
                    && record.execution_id() == &second_execution
                    && record.task_id() == &task
                    && record.before_snapshot() == &started.snapshot
                    && record.facts() == &facts(107)
                    && record.committed_revision() == 2
                    && Some(record.marker()) == expected_ready.snapshot.tasks[0].interrupt.as_ref()
                    && record.call_digest() == expected_digest
                    && record.application() == scope.application()
                    && record.tool_id() == scope.tool_id()
                    && record.tool_revision() == scope.tool_revision()
                    && record.policy_revision() == scope.policy_revision()
                    && record.constraint_revision() == scope.constraint_revision()
                    && record
                        .encode_record(&mut PermissionEvidenceBudget::new())
                        .is_ok_and(|encoded| encoded == produced_text(&row[9]).as_bytes())
                    && row[..9]
                        == [
                            rusqlite::types::Value::Text(second_execution.to_string()),
                            rusqlite::types::Value::Text(task.to_string()),
                            rusqlite::types::Value::Text(use_id.as_str().to_owned()),
                            rusqlite::types::Value::Text(thread.to_string()),
                            rusqlite::types::Value::Text(
                                before_use.view.thread.generation.to_string(),
                            ),
                            rusqlite::types::Value::Text(
                                decision.permission_id().as_str().to_owned(),
                            ),
                            rusqlite::types::Value::Text(second_turn.to_string()),
                            rusqlite::types::Value::Blob(2u64.to_be_bytes().to_vec()),
                            rusqlite::types::Value::Integer(1),
                        ]
            },
        )
    });
    let admission_preserved_original = before_use.executions[0] == reopened.execution
        && before_use.journals[0] == reopened.journal
        && [4, 5, 6, 7, 8]
            .iter()
            .all(|index| before_use.raw.tables[*index] == reopened.raw.tables[*index]);
    let ready_valid = owner2_exact
        && admission_preserved_original
        && authorization
            .as_ref()
            .is_ok_and(|value| value.as_ref() == Some(&expected_ready))
        && expected_ready.snapshot.revision == 2
        && expected_ready.snapshot.status == ExecutionStatus::Running
        && expected_ready.snapshot.turn_id == second_turn
        && expected_ready.snapshot.tasks[0].status == TaskStatus::Ready
        && expected_ready.snapshot.tasks[0].approved
        && expected_ready.snapshot.attempts_used == 0
        && expected_ready.snapshot.attempt_history.is_empty()
        && expected_ready.snapshot.tasks[0].input
            == Map::from_iter([("input".into(), current_input)])
        && calls.load(Ordering::SeqCst) == 0
        && linked_use
        && after_use.raw.tables[8].len() == 1
        && original_reuse_commit_matches(
            &before_use,
            &after_use,
            &expected_ready,
            after_use.raw.tables[8].first().cloned(),
            OriginalReuseSnapshotEncoding::Struct,
        )
        && initial_use_usage == (0, 0)
        && use_usage.0 > 0
        && use_usage.1 > 0
        && use_usage.0 <= 8_388_608
        && use_usage.1 <= 65_536
        && use_counts.0 > 0
        && use_counts.1 == 1
        && use_counts.2 > 0
        && use_counts.3 == 0
        && use_counts.4 == 0;
    // Preserve the actual prerequisite observation on error/miss; never turn it
    // into a Driver-gap result. All result/account/catalog/store aliases retire.
    drop(authorization);
    drop(use_account);
    drop(observed_use);
    drop(use_authority);
    drop(setup_catalog);
    drop(setup_application);
    drop(command);
    let ready_fixture = DriverReadyFixture {
        directory,
        next,
        thread,
        execution,
        second_execution,
        task,
        context,
        definition,
        authorized: after_use.executions[1].clone(),
        calls,
    };
    eprintln!("original-reuse prerequisite=ready error={use_code} counts={use_counts:?} usage={use_usage:?} owner2_exact={owner2_exact} ready_valid={ready_valid} state={:?} view={:?} journals={:?} raw={:?}", after_use.executions, after_use.view, after_use.journals, after_use.raw.tables);
    if !ready_valid {
        let retired = original_reuse_retire_ready(ready_fixture, &after_use);
        assert!(
            retired,
            "actual Ready predecessor must survive owned close/reopen/removal"
        );
        panic!("original UUID reuse Ready authorization prerequisite failed; error={use_code}; no Driver gap credit");
    }
    (ready_fixture, attempt, after_use)
}

#[test]
fn permission_original_uuid_reuse_driver_persists_success() {
    let (ready_fixture, attempt, after_use) = original_reuse_ready_fixture();
    let fixture = original_driver_fixture(ready_fixture, attempt.clone(), DriverReadFault::None);
    let hooks = OriginalReuseHooks(attempt.clone());
    let expected_complete = StoredExecution {
        thread_id: fixture.ready.thread.clone(),
        snapshot: transition(
            &fixture.claimed.snapshot,
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
    let complete_valid = actual
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
                &fixture.claimed,
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
    eprintln!("original-reuse driver returned_ok={} phase={phase} error_debug={failure_debug:?} failure={failure_observation:?} counters={counters:?} authorities={authorities:?} constraints={constraints:?} captures={:?} state={:?} view={:?} journals={:?} raw={:?}", actual.is_ok(), fixture.observed.captures.lock().unwrap(), after.executions, after.view, after.journals, after.raw.tables);
    drop(actual);
    drop(postclaim);
    drop(completion_capture);
    drop(failure_observation);
    let retired = fixture.retire(&after);
    eprintln!("original-reuse complete_valid={complete_valid} retired={retired}");
    assert!(
        retired,
        "actual Driver result/failure/retained Work must retire before owned close/reopen/removal"
    );
    assert!(complete_valid, "original UUID reuse must return and persist exact Complete4 with callback1/typed1/raw0; reached={phase}");
}

fn original_driver_fixture(
    ready: DriverReadyFixture,
    attempt: AttemptId,
    read_fault: DriverReadFault,
) -> DriverRunFixture {
    let DriverReadyFixture {
        directory,
        next,
        thread,
        execution,
        second_execution,
        task,
        context,
        definition,
        authorized,
        calls,
    } = ready;
    let before = next.thread_view(&thread).unwrap().unwrap();
    let dispatch_plan = transition(
        &authorized.snapshot,
        KernelCommand::Dispatch {
            task_id: task.clone(),
            attempt_id: attempt.clone(),
        },
        facts(108),
    )
    .unwrap();
    let mut claimed_thread = before.thread.clone();
    claimed_thread.resource_revision += 1;
    let claimed = StoredExecution {
        thread_id: thread.clone(),
        snapshot: dispatch_plan.snapshot,
    };
    let invocation = EffectInvocation {
        execution_id: second_execution.clone(),
        turn_id: claimed.snapshot.turn_id.clone(),
        owner_generation: claimed.snapshot.owner_generation,
        task_id: task.clone(),
        effect_id: definition.batches[0][0].effect_id.clone(),
        attempt_id: attempt,
        input: claimed.snapshot.tasks[0].input.clone(),
    };
    let invocation_context = InvocationContext {
        thread_id: thread.clone(),
        thread_generation: before.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: "application-1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_matches = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(DriverOracle {
        invocation: invocation.clone(),
        context: invocation_context.clone(),
        calls: callback_calls.clone(),
        matches: callback_matches.clone(),
        result: DriverNativeResult::Succeeded,
    });
    let constraint = Arc::new(DriverConstraint {
        context: invocation_context,
        definition: definition.clone(),
        ready: authorized.snapshot.tasks[0].clone(),
        claimed: claimed.snapshot.tasks[0].clone(),
        invocation,
        calls: AtomicUsize::new(0),
        matches: AtomicUsize::new(0),
    });
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    definition.batches[0][0].effect_id.clone(),
                    EffectBinding {
                        callback: callback.clone(),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: constraint.clone(),
            }],
        )
        .unwrap(),
    );
    let authority = DriverAuthority {
        context: context.clone(),
        resources: [
            ApprovalResource {
                thread: before.thread.clone(),
                execution: authorized.clone(),
            },
            ApprovalResource {
                thread: claimed_thread,
                execution: claimed.clone(),
            },
        ],
        calls: AtomicUsize::new(0),
        matches: AtomicUsize::new(0),
        ready_matches: AtomicUsize::new(0),
        claimed_matches: AtomicUsize::new(0),
        refuse_claimed: false,
    };
    let observed = DriverObservedStore {
        inner: next.clone(),
        captures: Arc::new(Mutex::new(vec![])),
        entries: Arc::new(AtomicUsize::new(0)),
        path: directory.0.clone(),
        thread: thread.clone(),
        original: execution.clone(),
        current: second_execution.clone(),
        callback_calls: callback_calls.clone(),
        captured: Mutex::new(None),
        raw_commits: AtomicUsize::new(0),
        outcome_entries: AtomicUsize::new(0),
        outcome_forwards: AtomicUsize::new(0),
        read_fault,
        fault_applied: std::sync::atomic::AtomicBool::new(false),
        postclaim: Mutex::new(None),
        substituted: Mutex::new(None),
        healthy: Mutex::new(None),
        revision_restore: Mutex::new(None),
        trigger_installed: std::sync::atomic::AtomicBool::new(false),
    };

    DriverRunFixture {
        ready: DriverReadyFixture {
            directory,
            next,
            thread,
            execution,
            second_execution,
            task,
            context,
            definition,
            authorized,
            calls,
        },
        claimed,
        callback,
        constraint,
        application,
        catalog,
        authority,
        observed,
        capacity: TaskCapacity::new(1).unwrap(),
        callback_calls,
        callback_matches,
    }
}

#[test]
fn permission_original_uuid_reuse_driver_definition_substitution_retains_outcome() {
    let (ready, attempt, after_use) = original_reuse_ready_fixture();
    let fixture = original_driver_fixture(ready, attempt.clone(), DriverReadFault::Definition);
    let hooks = OriginalReuseHooks(attempt.clone());
    let expected_outcome = b4_runtime::EffectOutcome::Succeeded(TaskResult {
        output: json!(true),
        writes: vec![],
    });
    let mut actual = fixture.drive(&hooks);
    let after = fixture.capture();
    let captured = fixture.observed.captured.lock().unwrap().take();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let substituted = fixture.observed.substituted.lock().unwrap().take();
    let kernel_valid = substituted.as_ref().is_some_and(|stored| {
        let mut expected_substitution = fixture.claimed.clone();
        expected_substitution.snapshot.definition.max_attempts = 4;
        stored == &expected_substitution
            && b4_kernel::validate_snapshot(&stored.snapshot).is_ok()
            && transition(
                &stored.snapshot,
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
            .is_ok()
    });
    let initial_usage = actual.as_ref().err().map(PermissionUseDriverFailure::usage);
    // Exercise only the actual failure's retained Work/account. A matcher/local
    // fault cannot unlock storage retry; no alternate candidate is constructed.
    let retry_refused = actual.as_mut().err().is_some_and(|failure| {
        matches!(
            failure.retry_settlement(&facts(110)),
            Err(PermissionError::UnsupportedProfile)
        ) && failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
            && failure.settled_outcome() == Some(&expected_outcome)
    });
    let usage = actual.as_ref().err().map(PermissionUseDriverFailure::usage);
    let counters = (
        fixture.observed.entries.load(Ordering::SeqCst),
        fixture.observed.outcome_entries.load(Ordering::SeqCst),
        fixture.observed.outcome_forwards.load(Ordering::SeqCst),
        fixture.observed.raw_commits.load(Ordering::SeqCst),
    );
    let refused = kernel_valid
        && fixture.observed.fault_applied.load(Ordering::SeqCst)
        && retry_refused
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
                && failure.settled_outcome() == Some(&expected_outcome)
                && failure.usage().completion_entries() == 1
                && failure.usage().candidate_entries() == 1
                && failure.usage().candidate_refusals() == 1
                && failure.usage().committed_completions() == 0
        })
        && captured.as_ref() == Some(&after)
        && postclaim.as_ref() == Some(&after)
        && after.executions[1] == fixture.claimed
        && original_reuse_commit_matches(
            &after_use,
            &after,
            &fixture.claimed,
            None,
            OriginalReuseSnapshotEncoding::CanonicalDispatch,
        )
        && fixture.callback_once()
        && counters == (1, 0, 0, 0)
        && fixture.capture() == after;
    eprintln!("original-reuse definition: kernel_valid={kernel_valid} retry_refused={retry_refused} refused={refused} usage_before={initial_usage:?} usage_after={usage:?} counters={counters:?} actual={actual:?} state={:?} view={:?} journals={:?} raw={:?}", after.executions, after.view, after.journals, after.raw.tables);
    drop(actual);
    drop(captured);
    drop(postclaim);
    drop(substituted);
    let retired = fixture.retire(&after);
    eprintln!("original-reuse definition: refused={refused} retired={retired}");
    assert!(
        retired,
        "actual failure/retained Work must retire before owned close/reopen/removal"
    );
    assert!(refused, "kernel-valid Definition substitution must retain the actual settled outcome, refuse local retry and preserve full claimed3 storage with callback1/typed0/raw0");
}

#[test]
fn permission_original_uuid_reuse_driver_late_abort_retries_retained_work() {
    let (ready, attempt, after_use) = original_reuse_ready_fixture();
    let fixture = original_driver_fixture(ready, attempt.clone(), DriverReadFault::JournalAbort);
    let hooks = OriginalReuseHooks(attempt.clone());
    let expected_outcome = b4_runtime::EffectOutcome::Succeeded(TaskResult {
        output: json!(true),
        writes: vec![],
    });
    let expected_complete = StoredExecution {
        thread_id: fixture.ready.thread.clone(),
        snapshot: transition(
            &fixture.claimed.snapshot,
            KernelCommand::Complete {
                task_id: fixture.ready.task.clone(),
                attempt_id: attempt,
                result: TaskResult {
                    output: json!(true),
                    writes: vec![],
                },
            },
            facts(110),
        )
        .unwrap()
        .snapshot,
    };
    let mut actual = fixture.drive(&hooks);
    let after_abort = fixture.capture();
    let captured = fixture.observed.captured.lock().unwrap().take();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let before_usage = actual.as_ref().err().map(PermissionUseDriverFailure::usage);
    let first_counters = (
        fixture.observed.entries.load(Ordering::SeqCst),
        fixture.observed.outcome_entries.load(Ordering::SeqCst),
        fixture.observed.outcome_forwards.load(Ordering::SeqCst),
        fixture.observed.raw_commits.load(Ordering::SeqCst),
    );
    // The backend error remains opaque. Attribute only the observed guard
    // installation, settled failure, entered typed port and full rollback.
    let first_rollback = fixture.observed.trigger_installed.load(Ordering::SeqCst)
        && captured.as_ref() == Some(&after_abort)
        && postclaim.as_ref() == Some(&after_abort)
        && after_abort.executions[1] == fixture.claimed
        && original_reuse_commit_matches(
            &after_use,
            &after_abort,
            &fixture.claimed,
            None,
            OriginalReuseSnapshotEncoding::CanonicalDispatch,
        )
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
                && failure.settled_outcome() == Some(&expected_outcome)
                && failure.usage().completion_entries() == 1
                && failure.usage().candidate_entries() == 1
                && failure.usage().candidate_refusals() == 0
                && failure.usage().committed_completions() == 0
        })
        && fixture.callback_once()
        && first_counters == (1, 1, 1, 0);
    eprintln!("original-reuse abort: first_rollback={first_rollback} usage={before_usage:?} counters={first_counters:?} actual={actual:?} state={:?} view={:?} journals={:?} raw={:?}", after_abort.executions, after_abort.view, after_abort.journals, after_abort.raw.tables);
    let removed_guard = driver_drop_abort(&fixture.ready.directory.0);
    // Explicit storage-only retry uses the actual failure and retained account;
    // facts110 are independently applied to claimed3, without callback replay.
    let retried = actual
        .as_mut()
        .err()
        .map(|failure| failure.retry_settlement(&facts(110)));
    let after = fixture.capture();
    let after_usage = actual.as_ref().err().map(PermissionUseDriverFailure::usage);
    let repeat_refused = actual.as_mut().err().is_some_and(|failure| {
        matches!(
            failure.retry_settlement(&facts(111)),
            Err(PermissionError::UnsupportedProfile)
        )
    });
    let counters = (
        fixture.observed.entries.load(Ordering::SeqCst),
        fixture.observed.outcome_entries.load(Ordering::SeqCst),
        fixture.observed.outcome_forwards.load(Ordering::SeqCst),
        fixture.observed.raw_commits.load(Ordering::SeqCst),
    );
    let retry_success = retried.as_ref().is_some_and(|result| {
        result
            .as_ref()
            .is_ok_and(|stored| stored == &expected_complete && stored == &after.executions[1])
    }) && before_usage
        .zip(after_usage)
        .is_some_and(|(before, after)| {
            after.encoded_bytes() > before.encoded_bytes()
                && after.structural_nodes() > before.structural_nodes()
                && after.operations() > before.operations()
                && after.completion_entries() == before.completion_entries()
                && after.candidate_entries() == before.candidate_entries()
                && after.candidate_refusals() == before.candidate_refusals()
                && after.refused_completions() == before.refused_completions()
        })
        && expected_complete.snapshot.revision == 4
        && expected_complete.snapshot.status == ExecutionStatus::Completed
        && expected_complete.snapshot.tasks[0].status == TaskStatus::Succeeded
        && original_reuse_commit_matches(
            &after_abort,
            &after,
            &expected_complete,
            None,
            OriginalReuseSnapshotEncoding::Struct,
        )
        && fixture.callback_once()
        && counters == (1, 2, 2, 0)
        && repeat_refused
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status().is_none() && failure.settled_outcome().is_none()
        })
        && fixture.capture() == after;
    eprintln!("original-reuse retry: removed_guard={removed_guard} retry_success={retry_success} repeat_refused={repeat_refused} usage_before={before_usage:?} usage_after={after_usage:?} counters={counters:?} retried={retried:?} state={:?} view={:?} journals={:?} raw={:?}", after.executions, after.view, after.journals, after.raw.tables);
    drop(retried);
    drop(actual);
    drop(captured);
    drop(postclaim);
    let retired = fixture.retire(&after);
    eprintln!("original-reuse retry: first_rollback={first_rollback} removed_guard={removed_guard} retry_success={retry_success} retired={retired}");
    assert!(
        retired,
        "actual retry result/failure/retained Work must retire before owned close/reopen/removal"
    );
    assert!(first_rollback && removed_guard, "guarded late outcome attempt must retain actual settled outcome and leave exact full claimed3 state");
    assert!(retry_success, "explicit retry of the actual retained Work/account must persist exact Complete4 with callback1/typed2/raw0, cumulative spending and repeat refusal");
}

#[path = "normal_application_ids.rs"]
mod normal_application_ids;

#[path = "normal_task_effect_ids.rs"]
mod normal_task_effect_ids;

#[path = "permission_driver_entry_gate.rs"]
mod permission_driver_entry_gate;

#[path = "original_always_driver.rs"]
mod original_always_driver;
