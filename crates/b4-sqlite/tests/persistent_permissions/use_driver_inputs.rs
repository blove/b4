//! Genuine current-run input acceptance through the owned Driver fixture.
use super::*;

struct InputHooks(AttemptId);
impl PermissionUseDriverHooks for InputHooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        DriverHooks.facts(current)
    }
    fn attempt_id(&self) -> AttemptId {
        self.0.clone()
    }
}

#[test]
fn permission_use_driver_uuid_current_execution_persists_success() {
    let config = DriverFixtureConfig {
        current_execution_id: ExecutionId::new("b81bf932-e606-4ba0-90ef-5c9eb23a2a1b").unwrap(),
        ..DriverFixtureConfig::default()
    };
    let hooks = InputHooks(config.attempt_id.clone());
    let fixture = driver_run_fixture_with(
        DriverNativeResult::Succeeded,
        DriverReadFault::None,
        false,
        &config,
    );
    let before = fixture.capture();
    let ready2 = before.executions[1] == fixture.ready.authorized
        && before.executions[1].snapshot.execution_id == config.current_execution_id
        && before.executions[1].snapshot.revision == 2
        && before.executions[1].snapshot.status == ExecutionStatus::Running
        && before.executions[1].snapshot.tasks[0].status == TaskStatus::Ready
        && before.executions[1].snapshot.tasks[0].approved
        && before.executions[1].snapshot.attempts_used == 0
        && before.executions[1].snapshot.attempt_history.is_empty()
        && fixture.ready.calls.load(Ordering::SeqCst) == 0;
    let expected = transition(
        &fixture.claimed.snapshot,
        KernelCommand::Complete {
            task_id: fixture.ready.task.clone(),
            attempt_id: config.attempt_id.clone(),
            result: TaskResult {
                output: json!(true),
                writes: vec![],
            },
        },
        facts(109),
    )
    .unwrap();
    let actual = fixture.drive(&hooks);
    let after = fixture.capture();
    let completion_capture = fixture.observed.captured.lock().unwrap().take();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let callbacks = fixture.callback_calls.load(Ordering::SeqCst);
    let typed = fixture.observed.outcome_entries.load(Ordering::SeqCst);
    let raw = fixture.observed.raw_commits.load(Ordering::SeqCst);
    let pre_dispatch_refusal = actual.as_ref().err().is_some_and(|failure| {
        failure.callback_status().is_none()
            && failure.settled_outcome().is_none()
            && failure.usage().completion_entries() == 0
            && failure.usage().candidate_entries() == 0
    }) && callbacks == 0
        && fixture.callback_matches.load(Ordering::SeqCst) == 0
        && fixture.ready.calls.load(Ordering::SeqCst) == 0
        && fixture.observed.entries.load(Ordering::SeqCst) == 0
        && fixture.observed.captures.lock().unwrap().is_empty()
        && fixture.authority.calls.load(Ordering::SeqCst) == 0
        && fixture.authority.matches.load(Ordering::SeqCst) == 0
        && fixture.authority.ready_matches.load(Ordering::SeqCst) == 0
        && fixture.authority.claimed_matches.load(Ordering::SeqCst) == 0
        && fixture.constraint.calls.load(Ordering::SeqCst) == 0
        && fixture.constraint.matches.load(Ordering::SeqCst) == 0
        && typed == 0
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 0
        && raw == 0
        && completion_capture.is_none()
        && postclaim.is_none()
        && before == after;
    let persisted_success = actual.as_ref().is_ok_and(|stored| {
        stored == &after.executions[1]
            && stored.snapshot == expected.snapshot
            && stored.snapshot.status == ExecutionStatus::Completed
            && stored.snapshot.tasks[0].status == TaskStatus::Succeeded
    }) && fixture.callback_once()
        && fixture.observed.entries.load(Ordering::SeqCst) == 1
        && typed == 1
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 1
        && raw == 0
        && same_authority_rows(&before, &after)
        && before.executions[0] == after.executions[0]
        && before.journals[0] == after.journals[0]
        && postclaim.as_ref().is_some_and(|claimed| {
            claimed.executions[1] == fixture.claimed
                && claimed.view.thread.resource_revision == before.view.thread.resource_revision + 1
        })
        && completion_capture.as_ref().is_some_and(|claimed| {
            claimed.executions[1] == fixture.claimed
                && after.view.thread.resource_revision == claimed.view.thread.resource_revision + 1
                && after.journals[1].len() == claimed.journals[1].len() + 1
                && after.journals[1][..claimed.journals[1].len()] == claimed.journals[1]
                && after.journals[1].last() == Some(&JournalEntry::from(&expected.snapshot))
        });
    // The actual failure/Receipt and every copied observation retire before
    // owner2's sole-close proof and owner3's complete reopen/readiness/removal.
    drop(actual);
    drop(completion_capture);
    drop(postclaim);
    let retired = fixture.retire(&after);
    println!("normal-input-task1: ready2={ready2} pre_dispatch_refusal={pre_dispatch_refusal} persisted_success={persisted_success} callback={callbacks} typed={typed} raw={raw} retired={retired}");
    assert!(
        ready2 && retired && (pre_dispatch_refusal || persisted_success),
        "legal Ready2 setup, exact observed path and full owned retirement must be healthy"
    );
    assert!(
        persisted_success,
        "normal UUID current execution must persist the exact kernel Complete through the genuine Driver"
    );
}

// Only current-run values change; the inherited approval source stays literal.
fn normal_current_config() -> DriverFixtureConfig {
    DriverFixtureConfig {
        current_execution_id: ExecutionId::new("b81bf932-e606-4ba0-90ef-5c9eb23a2a1b").unwrap(),
        current_turn_id: TurnId::new("d2b2f332-4364-435e-8aee-57570303a293").unwrap(),
        attempt_id: AttemptId::new("e759e778-969e-4747-a207-e1ed59ad99fa").unwrap(),
        current_input: serde_json::from_str(
            r#"{"release":{"name":"normal application run","options":[{"enabled":true,"note":"caf\u00e9 \u2603 \ud83d\ude80"},null,17,-4,1.25],"metadata":{"labels":["production","weekly"],"retry":false,"description":"A modest structured application payload with nested objects and arrays."}}}"#,
        )
        .unwrap(),
    }
}

#[test]
fn permission_use_driver_normal_current_uuids_and_nested_json_persist_success() {
    let config = normal_current_config();
    let hooks = InputHooks(config.attempt_id.clone());
    let fixture = driver_run_fixture_with(
        DriverNativeResult::Succeeded,
        DriverReadFault::None,
        false,
        &config,
    );
    let before = fixture.capture();
    let ready2 = before.executions[1] == fixture.ready.authorized
        && before.executions[1].snapshot.execution_id == config.current_execution_id
        && before.executions[1].snapshot.turn_id == config.current_turn_id
        && before.executions[1].snapshot.tasks[0].input
            == Map::from_iter([("input".into(), config.current_input.clone())])
        && before.executions[1].snapshot.revision == 2
        && before.executions[1].snapshot.status == ExecutionStatus::Running
        && before.executions[1].snapshot.tasks[0].status == TaskStatus::Ready
        && before.executions[1].snapshot.tasks[0].approved
        && before.executions[1].snapshot.attempts_used == 0
        && before.executions[1].snapshot.attempt_history.is_empty()
        && fixture.ready.calls.load(Ordering::SeqCst) == 0;
    let expected = transition(
        &fixture.claimed.snapshot,
        KernelCommand::Complete {
            task_id: fixture.ready.task.clone(),
            attempt_id: config.attempt_id.clone(),
            result: TaskResult {
                output: json!(true),
                writes: vec![],
            },
        },
        facts(109),
    )
    .unwrap();
    let actual = fixture.drive(&hooks);
    let after = fixture.capture();
    let completion_capture = fixture.observed.captured.lock().unwrap().take();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let callbacks = fixture.callback_calls.load(Ordering::SeqCst);
    let typed = fixture.observed.outcome_entries.load(Ordering::SeqCst);
    let raw = fixture.observed.raw_commits.load(Ordering::SeqCst);
    let persisted_success = actual.as_ref().is_ok_and(|stored| {
        stored == &after.executions[1]
            && stored.snapshot == expected.snapshot
            && stored.snapshot.status == ExecutionStatus::Completed
            && stored.snapshot.tasks[0].status == TaskStatus::Succeeded
    }) && fixture.callback_once()
        && after.view.thread.resource_revision == before.view.thread.resource_revision + 2
        && after.journals[1].len() == before.journals[1].len() + 2
        && fixture.observed.entries.load(Ordering::SeqCst) == 1
        && typed == 1
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 1
        && raw == 0
        && same_authority_rows(&before, &after)
        && before.executions[0] == after.executions[0]
        && before.journals[0] == after.journals[0]
        && postclaim.as_ref().is_some_and(|claimed| {
            claimed.executions[1] == fixture.claimed
                && claimed.view.thread.resource_revision == before.view.thread.resource_revision + 1
        })
        && completion_capture.as_ref().is_some_and(|claimed| {
            claimed.executions[1] == fixture.claimed
                && after.view.thread.resource_revision == claimed.view.thread.resource_revision + 1
                && after.journals[1].len() == claimed.journals[1].len() + 1
                && after.journals[1][..claimed.journals[1].len()] == claimed.journals[1]
                && after.journals[1].last() == Some(&JournalEntry::from(&expected.snapshot))
        });
    // The actual failure/Receipt and every copied observation retire before
    // owner2's sole-close proof and owner3's complete reopen/readiness/removal.
    drop(actual);
    drop(completion_capture);
    drop(postclaim);
    let retired = fixture.retire(&after);
    println!("normal-input-structured: ready2={ready2} persisted_success={persisted_success} callback={callbacks} typed={typed} raw={raw} retired={retired}");
    assert!(
        ready2 && retired,
        "legal Ready2 setup, exact observed path and full owned retirement must be healthy"
    );
    assert!(
        persisted_success,
        "normal current UUIDs and nested JSON must persist the exact kernel Complete through the genuine Driver"
    );
}

#[test]
fn permission_use_driver_normal_shape_definition_substitution_retains_outcome() {
    let config = normal_current_config();
    let hooks = InputHooks(config.attempt_id.clone());
    let fixture = driver_run_fixture_with(
        DriverNativeResult::Succeeded,
        DriverReadFault::Definition,
        false,
        &config,
    );
    let expected_outcome = b4_runtime::EffectOutcome::Succeeded(TaskResult {
        output: json!(true),
        writes: vec![],
    });
    let ready = fixture.capture();
    let mut actual = fixture.drive(&hooks);
    let after = fixture.capture();
    let claimed = fixture.observed.captured.lock().unwrap().take();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let substituted = fixture.observed.substituted.lock().unwrap().take();
    let kernel_valid = substituted.as_ref().is_some_and(|stored| {
        b4_kernel::validate_snapshot(&stored.snapshot).is_ok()
            && transition(
                &stored.snapshot,
                KernelCommand::Complete {
                    task_id: fixture.ready.task.clone(),
                    attempt_id: config.attempt_id.clone(),
                    result: TaskResult {
                        output: json!(true),
                        writes: vec![],
                    },
                },
                facts(109),
            )
            .is_ok()
    });
    // Retry on the actual failure exercises the retained Work and settled
    // outcome; no replacement candidate, permit or receipt is constructed.
    let retry_refused = actual.as_mut().err().is_some_and(|failure| {
        failure.retry_settlement(&facts(110)).is_err()
            && failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
            && failure.settled_outcome() == Some(&expected_outcome)
    });
    let refused = retry_refused
        && kernel_valid
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
                && failure.settled_outcome() == Some(&expected_outcome)
                && failure.usage().completion_entries() == 1
                && failure.usage().candidate_entries() == 1
                && failure.usage().candidate_refusals() == 1
        })
        && claimed.as_ref() == Some(&after)
        && postclaim.as_ref() == Some(&after)
        && after.executions[1] == fixture.claimed
        && fixture.callback_once()
        && same_authority_rows(&ready, &after)
        && ready.executions[0] == after.executions[0]
        && ready.journals[0] == after.journals[0]
        && after.view.thread.resource_revision == ready.view.thread.resource_revision + 1
        && after.journals[1].len() == ready.journals[1].len() + 1
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 0
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 0
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0
        && fixture.capture() == after;
    drop(actual);
    drop(claimed);
    drop(postclaim);
    drop(substituted);
    let retired = fixture.retire(&after);
    println!("normal-input-source: matcher_refused={refused} retired={retired}");
    assert!(refused && retired, "normal-shaped definition substitution must reach the matcher, retain actual settled Work and preserve full claimed storage through reopen");
}
