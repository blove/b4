//! Original human Always execution, then reuse of that completed creator.
use super::*;
use b4_runtime::permissions::{
    drive_permission_original_always_execution, PermissionOriginalAlwaysDriverBindings,
};

struct OriginalAlwaysHooks(AttemptId);
impl PermissionUseDriverHooks for OriginalAlwaysHooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        facts(if current.snapshot.revision == 3 {
            108
        } else {
            109
        })
    }
    fn attempt_id(&self) -> AttemptId {
        self.0.clone()
    }
}

// Observe the real persisted dispatch at callback entry. Original execution uses
// SQLite's actual typed ports directly; no NewUse forwarding grants authority.
struct OriginalAlwaysOracle {
    inner: Arc<DriverOracle>,
    store: Arc<SqliteStore>,
    path: PathBuf,
    thread: ThreadId,
    execution: ExecutionId,
    dispatch: Arc<Mutex<Option<DriverCapture>>>,
    abort_installed: Option<Arc<std::sync::atomic::AtomicBool>>,
}
impl NativeEffect for OriginalAlwaysOracle {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        *self.dispatch.lock().unwrap() = Some(original_reuse_capture(
            self.store.as_ref(),
            &self.path,
            &self.thread,
            &self.execution,
            &self.execution,
        ));
        if let Some(installed) = &self.abort_installed {
            let dispatch = self.dispatch.lock().unwrap();
            let dispatch = dispatch.as_ref().unwrap();
            installed.store(
                driver_install_abort(
                    &self.path,
                    &self.execution,
                    dispatch.executions[0].snapshot.revision + 1,
                    dispatch.view.thread.resource_revision + 1,
                ),
                Ordering::SeqCst,
            );
        }
        self.inner.invoke(invocation, context)
    }
}

// The existing Driver observer has two selectors. During the original run both
// select the same genuine execution; the database still has exactly one row.
fn original_always_commit_matches(
    before: &DriverCapture,
    after: &DriverCapture,
    expected: &StoredExecution,
    canonical: bool,
) -> bool {
    use rusqlite::types::Value;
    let mut thread = before.view.thread.clone();
    thread.resource_revision += 1;
    let entry = JournalEntry::from(&expected.snapshot);
    let mut journals = before.journals.clone();
    for journal in &mut journals {
        journal.push(entry.clone());
    }
    let mut raw = before.raw.tables.clone();
    raw[1][0][5] = Value::Blob(thread.resource_revision.to_be_bytes().to_vec());
    raw[2][0][2] = Value::Blob(expected.snapshot.revision.to_be_bytes().to_vec());
    raw[2][0][3] = Value::Blob(expected.snapshot.owner_generation.to_be_bytes().to_vec());
    raw[2][0][4] = Value::Text(if canonical {
        serde_json::to_string(&serde_json::to_value(&expected.snapshot).unwrap()).unwrap()
    } else {
        serde_json::to_string(&expected.snapshot).unwrap()
    });
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
    after.view
        == (ThreadView {
            thread,
            execution: Some(expected.clone()),
        })
        && after.executions == [expected.clone(), expected.clone()]
        && after.journals == journals
        && after.raw.tables == raw
        && after.raw.tables[2].len() == 1
        && after.raw.tables[8].is_empty()
}

fn original_always_run_fixture(
    native: DriverNativeResult,
    abort: bool,
) -> (
    DriverRunFixture,
    Arc<Mutex<Option<DriverCapture>>>,
    Arc<std::sync::atomic::AtomicBool>,
    AttemptId,
) {
    original_always_run_fixture_from_waiting(
        native,
        abort,
        original_always_waiting_fixture(json!({"release": "fixture"})),
    )
}

fn original_always_waiting_fixture(input: serde_json::Value) -> OriginalWaitingFixture {
    OriginalWaitingFixture::with_input(
        "07d7d468-c4f9-47e1-b421-ff889251df80",
        "publish-monthly-report",
        "reports.publish-monthly",
        input,
    )
}

fn original_always_run_fixture_from_waiting(
    native: DriverNativeResult,
    abort: bool,
    waiting: OriginalWaitingFixture,
) -> (
    DriverRunFixture,
    Arc<Mutex<Option<DriverCapture>>>,
    Arc<std::sync::atomic::AtomicBool>,
    AttemptId,
) {
    // This control genuinely accepts Always; it performs no Ready cancellation.
    // Its failed-approval path retires the actual Waiting state before panicking.
    let OriginalReadyControl {
        fixture,
        ready,
        context,
        ..
    } = OriginalReadyControl::from_waiting(waiting);
    let OriginalWaitingFixture {
        directory,
        store,
        application,
        catalog,
        calls,
        thread,
        execution,
        ..
    } = fixture;
    drop(catalog);
    drop(application);
    let definition = ready.execution.snapshot.definition.clone();
    let task = definition.batches[0][0].id.clone();
    let attempt = AttemptId::new("28639567-31f3-4b48-a061-32b98835636b").unwrap();
    let mut run = original_driver_fixture(
        DriverReadyFixture {
            directory,
            next: store,
            thread,
            execution: execution.clone(),
            second_execution: execution,
            task: task.clone(),
            context,
            definition,
            authorized: ready.execution,
            calls,
        },
        attempt.clone(),
        DriverReadFault::None,
    );
    // Replace only the native return; invocation and context stay independently
    // derived by the existing exact oracle from the genuine Ready3.
    run.callback = Arc::new(DriverOracle {
        invocation: run.callback.invocation.clone(),
        context: run.callback.context.clone(),
        calls: run.callback_calls.clone(),
        matches: run.callback_matches.clone(),
        result: native,
    });
    let abort_installed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dispatch_capture = Arc::new(Mutex::new(None));
    let callback = Arc::new(OriginalAlwaysOracle {
        inner: run.callback.clone(),
        store: run.ready.next.clone(),
        path: run.ready.directory.0.clone(),
        thread: run.ready.thread.clone(),
        execution: run.ready.execution.clone(),
        dispatch: dispatch_capture.clone(),
        abort_installed: abort.then(|| abort_installed.clone()),
    });
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: run.ready.context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: run.ready.definition.clone(),
                effects: BTreeMap::from([(
                    run.ready.definition.batches[0][0].effect_id.clone(),
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
                definition: run.ready.definition.clone(),
                effect_id: run.ready.definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: run.constraint.clone(),
            }],
        )
        .unwrap(),
    );
    run.catalog = catalog;
    run.application = application;
    drop(callback);
    (run, dispatch_capture, abort_installed, attempt)
}

#[test]
fn permission_original_always_driver_completes_creator_and_reuses_after_reopen() {
    original_always_completes_creator_and_reuses_after_reopen(
        original_always_waiting_fixture(json!({"release": "fixture"})),
        json!({"release": "fixture"}),
        json!({"report": {"month": "October", "publish": true}}),
    );
}

fn original_always_completes_creator_and_reuses_after_reopen(
    waiting: OriginalWaitingFixture,
    original_input: serde_json::Value,
    second_input: serde_json::Value,
) {
    let original_values = Map::from_iter([("input".into(), original_input)]);
    let second_values = Map::from_iter([("input".into(), second_input)]);
    let (run, dispatch_capture, abort_installed, attempt) =
        original_always_run_fixture_from_waiting(DriverNativeResult::Succeeded, false, waiting);
    drop(abort_installed);
    let task = run.ready.task.clone();
    let before = run.capture();
    let hooks = OriginalAlwaysHooks(attempt.clone());
    let dispatch = StoredExecution {
        thread_id: run.ready.thread.clone(),
        snapshot: transition(
            &run.ready.authorized.snapshot,
            KernelCommand::Dispatch {
                task_id: task.clone(),
                attempt_id: attempt.clone(),
            },
            facts(108),
        )
        .unwrap()
        .snapshot,
    };
    let complete = StoredExecution {
        thread_id: run.ready.thread.clone(),
        snapshot: transition(
            &dispatch.snapshot,
            KernelCommand::Complete {
                task_id: task.clone(),
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
    // The established NewUse entry remains a refusal for original Ready3.
    let old_entry = run.drive(&hooks);
    let old_entry_refused = old_entry.as_ref().err().is_some_and(|failure| {
        failure.callback_status().is_none() && failure.settled_outcome().is_none()
    }) && run.capture() == before
        && run.callback_calls.load(Ordering::SeqCst) == 0;
    drop(old_entry);
    let actual = drive_permission_original_always_execution(
        PermissionOriginalAlwaysDriverBindings {
            store: run.ready.next.as_ref(),
            authority: &run.authority,
            producer_catalog: &run.catalog,
            current_catalog: &run.catalog,
            context: &run.ready.context,
            hooks: &hooks,
            capacity: &run.capacity,
        },
        run.ready.authorized.clone(),
    );
    let after = run.capture();
    let postclaim = dispatch_capture.lock().unwrap().take();
    let failure = actual.as_ref().err().map(|error| {
        (
            error.callback_status(),
            error.settled_outcome().cloned(),
            error.usage(),
        )
    });
    let creator_valid = old_entry_refused && actual.as_ref().is_ok_and(|stored| stored == &complete)
        && before.raw.tables[2].len() == 1
        && before.raw.tables[8].is_empty()
        && run.ready.authorized.snapshot.revision == 3
        && dispatch.snapshot.revision == 4
        && complete.snapshot.revision == 5
        && complete.snapshot.status == ExecutionStatus::Completed
        && complete.snapshot.tasks[0].status == TaskStatus::Succeeded
        && complete.snapshot.attempts_used == 1
        && run.claimed == dispatch
        // DriverOracle compares every invocation/context field before matching.
        && run.callback_once()
        && run.callback.invocation.input == original_values
        && run.ready.authorized.snapshot.tasks[0].input == original_values
        && complete.snapshot.tasks[0].input == original_values
        && run.callback.invocation == (EffectInvocation {
            execution_id: run.ready.execution.clone(),
            turn_id: dispatch.snapshot.turn_id.clone(),
            owner_generation: dispatch.snapshot.owner_generation,
            task_id: TaskId::new("publish-monthly-report").unwrap(),
            effect_id: EffectId::new("reports.publish-monthly").unwrap(),
            attempt_id: hooks.0.clone(), input: dispatch.snapshot.tasks[0].input.clone(),
        })
        && run.callback.context == (InvocationContext {
            thread_id: run.ready.thread.clone(),
            thread_generation: before.view.thread.generation.clone(),
            origin: run.ready.context.actor.clone(),
            application_revision: "application-1".into(),
            execution_revision: run.ready.definition.revision.clone(),
            policy_revision: run.ready.context.policy_revision.clone(),
        })
        && postclaim.as_ref().is_some_and(|capture|
            original_always_commit_matches(&before, capture, &dispatch, true))
        && postclaim.as_ref().is_some_and(|capture|
            original_always_commit_matches(capture, &after, &complete, false))
        && after.journals[0].len() == 6
        && after.raw.tables[4..] == before.raw.tables[4..];
    if !creator_valid {
        eprintln!(
            "original-always-oracle initial_turns={} initial_prompt_null={} old_refused={} returned_revision={:?} persisted_revision={} persisted_status={:?} callback_calls={} callback_once={} postclaim={} claim_expected={} dispatch_oracle={} complete_oracle={} journals={} approvals_unchanged={} failure={:?}",
            run.ready.authorized.snapshot.turn_history.len(),
            run.ready.authorized.snapshot.tasks[0].interrupt.as_ref().is_some_and(|interrupt| interrupt.prompt.is_null()),
            old_entry_refused,
            actual.as_ref().ok().map(|stored| stored.snapshot.revision),
            after.executions[0].snapshot.revision,
            after.executions[0].snapshot.status,
            run.callback_calls.load(Ordering::SeqCst),
            run.callback_once(),
            postclaim.is_some(),
            run.claimed == dispatch,
            postclaim.as_ref().is_some_and(|capture| original_always_commit_matches(&before, capture, &dispatch, true)),
            postclaim.as_ref().is_some_and(|capture| original_always_commit_matches(capture, &after, &complete, false)),
            after.journals[0].len(),
            after.raw.tables[4..] == before.raw.tables[4..],
            failure,
        );
    }
    drop(actual);
    drop(failure);
    drop(postclaim);
    drop(dispatch_capture);
    if !creator_valid {
        let retired = run.retire(&after);
        assert!(
            retired,
            "actual original driver state must close/reopen/remove"
        );
        panic!("original Always driver must persist kernel Dispatch4/Complete5 with exact callback1 and no PermissionUse");
    }

    // Release the actual callback/catalog and all owner1 store aliases before
    // opening owner2. Keep only immutable evidence and the owned directory.
    let callback_weak = Arc::downgrade(&run.callback);
    let DriverRunFixture {
        ready,
        observed,
        callback,
        constraint,
        application,
        catalog,
        authority,
        ..
    } = run;
    drop(observed);
    drop(authority);
    drop(catalog);
    drop(application);
    drop(callback);
    drop(constraint);
    let DriverReadyFixture {
        directory,
        next,
        thread,
        execution,
        task,
        context,
        definition,
        calls,
        ..
    } = ready;
    let owner1 = next.owner_generation();
    let original_retired = callback_weak.upgrade().is_none()
        && Arc::strong_count(&next) == 1
        && Arc::strong_count(&calls) == 1
        && next.readiness().is_ok();
    drop(next);
    let next = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let reopened = OriginalCapture::read(next.as_ref(), &directory.0, &thread, &execution);
    let reopen_valid = original_retired
        && next.owner_generation() == owner1 + 1
        && reopened.execution == complete
        && reopened.view == after.view
        && reopened.journal == after.journals[0]
        && reopened.raw.tables[1..] == after.raw.tables[1..]
        && reopened.raw.tables[0][0][..3] == after.raw.tables[0][0][..3]
        && produced_u64(&reopened.raw.tables[0][0][3]) == owner1 + 1;

    let second_execution = ExecutionId::new("b81bf932-e606-4ba0-90ef-5c9eb23a2a1b").unwrap();
    let second_turn = TurnId::new("d2b2f332-4364-435e-8aee-57570303a293").unwrap();
    let initial = new_execution(
        second_execution.clone(),
        second_turn,
        next.owner_generation(),
        definition.clone(),
        second_values.clone(),
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
    // Rebuild current bindings from the same registered application identity.
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
    let before_use = original_reuse_capture(
        next.as_ref(),
        &directory.0,
        &thread,
        &execution,
        &second_execution,
    );
    let use_id = PermissionUseId::new(
        "b4pu_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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
    let mut account = PermissionEvidenceBudget::new();
    let authorization = try_authorize_permission_ready(
        next.as_ref(),
        &AllowAuthority,
        &setup_catalog,
        fence(next.as_ref(), &thread),
        &PermissionReadyAuthorizationRequest {
            context: &context,
            command: &command,
            facts: facts(107),
        },
        &mut account,
    );
    let after_use = original_reuse_capture(
        next.as_ref(),
        &directory.0,
        &thread,
        &execution,
        &second_execution,
    );
    let decision = PermissionDecision::decode_record(
        produced_text(&after.raw.tables[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let linked_use = after_use.raw.tables[8].first().is_some_and(|row| {
        PermissionUse::decode_record(
            produced_text(&row[9]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .is_ok_and(|record| {
            record.use_id() == &use_id
                && record.execution_id() == &second_execution
                && record.permission_id() == decision.permission_id()
                && record.before_snapshot() == &started.snapshot
        })
    });
    let authorized = authorization
        .as_ref()
        .is_ok_and(|value| value.as_ref() == Some(&expected_ready));
    let use_valid = reopen_valid
        && authorized
        && linked_use
        && after_use.raw.tables[8].len() == 1
        && after_use.executions[0] == complete
        && after_use.journals[0] == after.journals[0]
        && original_reuse_commit_matches(
            &before_use,
            &after_use,
            &expected_ready,
            after_use.raw.tables[8].first().cloned(),
            OriginalReuseSnapshotEncoding::Struct,
        );
    if !use_valid {
        eprintln!(
            "original-always-reuse-oracle reopen_valid={} original_retired={} owner_advanced={} reopened_execution={} reopened_view={} reopened_journal={} reopened_raw={} metadata_prefix={} metadata_owner={} authorized={} authorization_error={:?} returned_revision={:?} linked_use={} uses={} current_revision={} original_revision={} original_journals={} current_journals={} use_commit_oracle={}",
            reopen_valid, original_retired, next.owner_generation()==owner1+1,
            reopened.execution==complete, reopened.view==after.view,
            reopened.journal==after.journals[0], reopened.raw.tables[1..]==after.raw.tables[1..],
            reopened.raw.tables[0][0][..3]==after.raw.tables[0][0][..3],
            produced_u64(&reopened.raw.tables[0][0][3])==owner1+1,
            authorized, authorization.as_ref().err().map(PermissionError::code),
            authorization.as_ref().ok().and_then(|value|value.as_ref()).map(|stored|stored.snapshot.revision),
            linked_use, after_use.raw.tables[8].len(),
            after_use.executions[1].snapshot.revision, after_use.executions[0].snapshot.revision,
            after_use.journals[0].len(),after_use.journals[1].len(),
            original_reuse_commit_matches(&before_use,&after_use,&expected_ready,after_use.raw.tables[8].first().cloned(),OriginalReuseSnapshotEncoding::Struct),
        );
    }
    drop(authorization);
    drop(account);
    drop(setup_catalog);
    drop(setup_application);
    let ready = DriverReadyFixture {
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
    if !use_valid {
        let retired = original_reuse_retire_ready(ready, &after_use);
        assert!(
            retired,
            "actual reuse authorization state must close/reopen/remove"
        );
        panic!("completed original creator must remain a real NewUse source after owner reopen");
    }
    let attempt = AttemptId::new("e759e778-969e-4747-a207-e1ed59ad99fa").unwrap();
    let run = original_driver_fixture(ready, attempt.clone(), DriverReadFault::None);
    let hooks = OriginalReuseHooks(attempt.clone());
    let expected = StoredExecution {
        thread_id: run.ready.thread.clone(),
        snapshot: transition(
            &run.claimed.snapshot,
            KernelCommand::Complete {
                task_id: run.ready.task.clone(),
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
    let actual = run.drive(&hooks);
    let final_capture = run.capture();
    let postclaim = run.observed.postclaim.lock().unwrap().take();
    let presettlement = run.observed.captured.lock().unwrap().take();
    let reuse_valid = actual.as_ref().is_ok_and(|stored| stored == &expected)
        && expected.snapshot.revision == 4
        && expected.snapshot.status == ExecutionStatus::Completed
        && expected.snapshot.tasks[0].status == TaskStatus::Succeeded
        && run.callback_once()
        && run.callback.invocation.input == second_values
        && expected.snapshot.tasks[0].input == second_values
        && postclaim.as_ref().is_some_and(|capture| {
            original_reuse_commit_matches(
                &after_use,
                capture,
                &run.claimed,
                None,
                OriginalReuseSnapshotEncoding::CanonicalDispatch,
            )
        })
        && presettlement.as_ref().is_some_and(|capture| {
            postclaim.as_ref() == Some(capture)
                && original_reuse_commit_matches(
                    capture,
                    &final_capture,
                    &expected,
                    None,
                    OriginalReuseSnapshotEncoding::Struct,
                )
        })
        && final_capture.executions[0] == complete
        && final_capture.journals[0].len() == 6
        && final_capture.raw.tables[4..8] == after.raw.tables[4..8];
    drop(actual);
    drop(postclaim);
    drop(presettlement);
    let retired = run.retire(&final_capture);
    assert!(
        retired,
        "actual callbacks and stores must retire before close/reopen/removal"
    );
    assert!(
        creator_valid && reopen_valid && use_valid && reuse_valid,
        "original Always Complete5 must survive reopen and feed genuine NewUse Complete4"
    );
}

// Match HTTP's value-only walk (root depth zero) separately from permission
// canonical geometry: each object key is an additional canonical string node.
fn structured_original_geometry(value: &serde_json::Value) -> (usize, usize, usize, usize) {
    fn walk(value: &serde_json::Value, depth: usize) -> (usize, usize, usize) {
        let mut values = 1;
        let mut canonical = 1;
        let mut deepest = depth;
        let children: Vec<_> = match value {
            serde_json::Value::Array(children) => children.iter().collect(),
            serde_json::Value::Object(children) => {
                canonical += children.len();
                children.values().collect()
            }
            _ => Vec::new(),
        };
        for child in children {
            let (child_values, child_canonical, child_depth) = walk(child, depth + 1);
            values += child_values;
            canonical += child_canonical;
            deepest = deepest.max(child_depth);
        }
        (values, canonical, deepest)
    }
    let (values, canonical, depth) = walk(value, 0);
    (
        serde_json::to_vec(value).unwrap().len(),
        values,
        canonical,
        depth,
    )
}

fn structured_original_input() -> serde_json::Value {
    json!({
        "report": {
            "title": "月次 Café 🚀",
            "sections": [
                {"name": "north", "values": [1, 2, null],
                 "options": {"enabled": true, "limit": 3, "cursor": null}},
                {"name": "南", "values": [3, 4, 5],
                 "options": {"enabled": false, "limit": null, "cursor": "次"}}
            ]
        },
        "options": {"notify": ["ops", "分析"], "dryRun": false, "note": null}
    })
}

#[test]
fn permission_structured_original_always_completes_and_reuses_after_reopen() {
    let input = structured_original_input();
    let waiting = original_always_waiting_fixture(input.clone());
    let (input_bytes, input_values, _, input_depth) = structured_original_geometry(&input);
    let parked = serde_json::to_value(&waiting.before.execution.snapshot).unwrap();
    let (snapshot_bytes, _, snapshot_nodes, _) = structured_original_geometry(&parked);
    let geometry_valid = input_bytes <= 4096
        && input_values <= 256
        && input_depth <= 16
        && snapshot_bytes <= 4096
        && snapshot_nodes > 91
        && snapshot_nodes <= 512
        && waiting.before.execution.snapshot.values
            == Map::from_iter([("input".into(), input.clone())])
        && waiting.before.execution.snapshot.tasks[0].input
            == Map::from_iter([("input".into(), input.clone())]);
    println!(
        "structured-original: input_bytes={input_bytes} input_values={input_values} input_depth={input_depth} parked_bytes={snapshot_bytes} parked_canonical_nodes={snapshot_nodes}"
    );
    if !geometry_valid {
        let after = OriginalCapture::read(
            waiting.store.as_ref(),
            &waiting.directory.0,
            &waiting.thread,
            &waiting.execution,
        );
        let retired = waiting.retire(&after);
        assert!(
            retired,
            "invalid geometry must retire the real waiting fixture"
        );
        panic!("structured original must fit HTTP and normal snapshot bounds above legacy91");
    }
    original_always_completes_creator_and_reuses_after_reopen(
        waiting,
        input,
        json!({"report": {"month": "十一月", "publish": true},
               "options": {"notify": ["分析", null], "cursor": null}}),
    );
}

#[test]
fn permission_text_original_always_completes_and_reuses_after_reopen() {
    let notes = "a".repeat(2048);
    let reuse_notes = "b".repeat(2048);
    assert!(notes.is_ascii() && reuse_notes.is_ascii());
    assert_eq!(notes.len(), 2048);
    assert_eq!(reuse_notes.len(), 2048);
    assert_ne!(notes, reuse_notes);
    let input = json!({"notes": notes});
    let reused = json!({"notes": reuse_notes});
    let waiting = original_always_waiting_fixture(input.clone());
    let (input_bytes, input_values, _, input_depth) = structured_original_geometry(&input);
    let (reuse_bytes, reuse_values, _, reuse_depth) = structured_original_geometry(&reused);
    let parked = serde_json::to_value(&waiting.before.execution.snapshot).unwrap();
    let (snapshot_bytes, _, snapshot_nodes, _) = structured_original_geometry(&parked);
    let geometry_valid = input_bytes <= 4096
        && input_values <= 256
        && input_depth <= 16
        && reuse_bytes <= 4096
        && reuse_values <= 256
        && reuse_depth <= 16
        && snapshot_bytes > 4096
        && snapshot_bytes <= 8192
        && snapshot_nodes <= 512
        && waiting.waiting2
        && waiting.before.execution.snapshot.values
            == Map::from_iter([("input".into(), input.clone())])
        && waiting.before.execution.snapshot.tasks[0].input
            == Map::from_iter([("input".into(), input.clone())]);
    println!(
        "text-original: input_bytes={input_bytes} input_values={input_values} input_depth={input_depth} reuse_bytes={reuse_bytes} reuse_values={reuse_values} reuse_depth={reuse_depth} parked_bytes={snapshot_bytes} parked_canonical_nodes={snapshot_nodes}"
    );
    if !geometry_valid {
        let after = OriginalCapture::read(
            waiting.store.as_ref(),
            &waiting.directory.0,
            &waiting.thread,
            &waiting.execution,
        );
        let retired = waiting.retire(&after);
        assert!(
            retired,
            "invalid geometry must retire the real waiting fixture"
        );
        panic!("text original must fit HTTP and the enlarged normal byte/node bounds");
    }
    original_always_completes_creator_and_reuses_after_reopen(waiting, input, reused);
}

#[test]
fn permission_text_original_oversized_acceptance_preserves_waiting() {
    let notes = "c".repeat(3750);
    assert!(notes.is_ascii());
    assert_eq!(notes.len(), 3750);
    let input = json!({"notes": notes});
    let mut fixture = original_always_waiting_fixture(input.clone());
    let (input_bytes, input_values, _, input_depth) = structured_original_geometry(&input);
    let parked = serde_json::to_value(&fixture.before.execution.snapshot).unwrap();
    let (snapshot_bytes, _, snapshot_nodes, _) = structured_original_geometry(&parked);
    let geometry_valid = input_bytes <= 4096
        && input_values <= 256
        && input_depth <= 16
        && snapshot_bytes > 8192
        && snapshot_nodes <= 512
        && fixture.before.execution.snapshot.values
            == Map::from_iter([("input".into(), input.clone())])
        && fixture.before.execution.snapshot.tasks[0].input
            == Map::from_iter([("input".into(), input)]);
    let request = fixture.request.take().unwrap();
    let actual = accept_permission_always_with_normal_inputs(
        fixture.store.clone(),
        Arc::new(AllowAuthority),
        fixture.catalog.clone(),
        &fixture.execution,
        fence(fixture.store.as_ref(), &fixture.thread),
        request,
    );
    let refused = matches!(&actual, Err(PermissionError::UnsupportedProfile));
    let code = actual
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let after = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let waiting2 = fixture.waiting2;
    let rows_unchanged = fixture.before == after;
    let grant_unchanged = fixture.unchanged_grant(&after);
    let callback_count = fixture.calls.load(Ordering::SeqCst);
    let valid = geometry_valid
        && waiting2
        && refused
        && rows_unchanged
        && grant_unchanged
        && callback_count == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    println!(
        "text-original-oversized: input_bytes={input_bytes} input_values={input_values} input_depth={input_depth} parked_bytes={snapshot_bytes} parked_canonical_nodes={snapshot_nodes} error={code} refused={refused} geometry_valid={geometry_valid} waiting2={waiting2} rows_unchanged={rows_unchanged} grant_unchanged={grant_unchanged} callback_count={callback_count} valid={valid} retired={retired}"
    );
    assert!(
        valid && retired,
        "oversized text acceptance must refuse without changing any of nine tables"
    );
}

#[test]
fn permission_structured_original_oversized_acceptance_preserves_waiting() {
    // The input occurs twice in the parked snapshot (values and task.input).
    // With the finite 16MiB normal allowance, bytes fit the snapshot cap and
    // this concrete 567-node snapshot reaches the normal 512-node shape refusal.
    let input = serde_json::Value::Object(
        (0..120)
            .map(|index| (format!("{index:x}"), json!(0)))
            .collect(),
    );
    let mut fixture = original_always_waiting_fixture(input.clone());
    let (input_bytes, input_values, _, input_depth) = structured_original_geometry(&input);
    let parked = serde_json::to_value(&fixture.before.execution.snapshot).unwrap();
    let (snapshot_bytes, _, snapshot_nodes, _) = structured_original_geometry(&parked);
    let geometry_valid = input_bytes <= 4096
        && input_values <= 256
        && input_depth <= 16
        && snapshot_bytes <= 4096
        && snapshot_nodes > 512;
    let request = fixture.request.take().unwrap();
    let actual = accept_permission_always_with_normal_inputs(
        fixture.store.clone(),
        Arc::new(AllowAuthority),
        fixture.catalog.clone(),
        &fixture.execution,
        fence(fixture.store.as_ref(), &fixture.thread),
        request,
    );
    let refused = matches!(&actual, Err(PermissionError::UnsupportedProfile));
    let code = actual
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let after = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let waiting2 = fixture.waiting2;
    let rows_unchanged = fixture.before == after;
    let grant_unchanged = fixture.unchanged_grant(&after);
    let callback_count = fixture.calls.load(Ordering::SeqCst);
    let valid = geometry_valid
        && waiting2
        && refused
        && rows_unchanged
        && grant_unchanged
        && callback_count == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    println!(
        "structured-original-oversized: input_bytes={input_bytes} input_values={input_values} input_depth={input_depth} parked_bytes={snapshot_bytes} parked_canonical_nodes={snapshot_nodes} error={code} refused={refused} geometry_valid={geometry_valid} waiting2={waiting2} rows_unchanged={rows_unchanged} grant_unchanged={grant_unchanged} callback_count={callback_count} valid={valid} retired={retired}"
    );
    assert!(
        valid && retired,
        "oversized normal acceptance must refuse without changing any of nine tables"
    );
}

#[path = "original_always_boundaries.rs"]
mod original_always_boundaries;
