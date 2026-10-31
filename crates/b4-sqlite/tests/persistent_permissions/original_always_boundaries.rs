//! Boundaries of the genuine human Always creator, using the real SQLite ports.
use super::*;
use b4_runtime::permissions::PermissionOriginalAlwaysDriverFailure;

fn drive<'a, H: PermissionUseDriverHooks>(
    run: &'a DriverRunFixture,
    hooks: &'a H,
) -> Result<StoredExecution, PermissionOriginalAlwaysDriverFailure<'a, SqliteStore, DriverAuthority>>
{
    drive_permission_original_always_execution(
        PermissionOriginalAlwaysDriverBindings {
            store: run.ready.next.as_ref(),
            authority: &run.authority,
            producer_catalog: &run.catalog,
            current_catalog: &run.catalog,
            context: &run.ready.context,
            hooks,
            capacity: &run.capacity,
        },
        run.ready.authorized.clone(),
    )
}

struct EntryGate {
    hooks: OriginalAlwaysHooks,
    entries: AtomicUsize,
    admit: bool,
}
impl PermissionUseDriverHooks for EntryGate {
    fn enter_callback(&self) -> bool {
        self.entries.fetch_add(1, Ordering::SeqCst);
        self.admit
    }
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        self.hooks.facts(current)
    }
    fn attempt_id(&self) -> AttemptId {
        self.hooks.attempt_id()
    }
}

#[test]
fn permission_original_always_driver_entry_refusal_retains_dispatch_without_settlement() {
    let (run, capture, installed, attempt) =
        original_always_run_fixture(DriverNativeResult::Succeeded, false);
    let before = run.capture();
    let hooks = EntryGate {
        hooks: OriginalAlwaysHooks(attempt),
        entries: AtomicUsize::new(0),
        admit: false,
    };
    let mut actual = drive(&run, &hooks);
    let after = run.capture();
    let refused = actual.as_mut().err().is_some_and(|failure| {
        failure.callback_status().is_none()
            && failure.settled_outcome().is_none()
            && failure.retry_settlement(&facts(110)).is_err()
    });
    // Reaching the local gate follows the real current-authority renewal;
    // the failure owns that unentered work and Drop must leave Dispatch4 alone.
    let valid = refused
        && hooks.entries.load(Ordering::SeqCst) == 1
        && run.authority.claimed_matches.load(Ordering::SeqCst) > 0
        && run.callback_calls.load(Ordering::SeqCst) == 0
        && capture.lock().unwrap().is_none()
        && !installed.load(Ordering::SeqCst)
        && run.claimed.snapshot.revision == 4
        && original_always_commit_matches(&before, &after, &run.claimed, true)
        && after.raw.tables[4..] == before.raw.tables[4..];
    drop(actual);
    let drop_unchanged = run.capture() == after;
    drop(capture);
    drop(installed);
    let retired = run.retire(&after);
    assert!(valid && drop_unchanged && retired,
        "renewed but unentered original Work must retain Dispatch4 until failure retirement without settling");
}

#[test]
fn permission_original_always_driver_current_authority_refuses_real_renewal() {
    let (mut run, capture, installed, attempt) =
        original_always_run_fixture(DriverNativeResult::Succeeded, false);
    run.authority.refuse_claimed = true;
    let before = run.capture();
    let hooks = EntryGate {
        hooks: OriginalAlwaysHooks(attempt),
        entries: AtomicUsize::new(0),
        admit: true,
    };
    let actual = drive(&run, &hooks);
    let after = run.capture();
    let valid = actual.as_ref().err().is_some_and(|failure| {
        failure.callback_status().is_none() && failure.settled_outcome().is_none()
    }) && run.authority.ready_matches.load(Ordering::SeqCst) > 0
        && run.authority.claimed_matches.load(Ordering::SeqCst) > 0
        && run.authority.calls.load(Ordering::SeqCst)
            == run.authority.matches.load(Ordering::SeqCst)
        && hooks.entries.load(Ordering::SeqCst) == 0
        && run.callback_calls.load(Ordering::SeqCst) == 0
        && capture.lock().unwrap().is_none()
        && original_always_commit_matches(&before, &after, &run.claimed, true)
        && after.raw.tables[4..] == before.raw.tables[4..];
    drop(actual);
    let drop_unchanged = run.capture() == after;
    drop(capture);
    drop(installed);
    let retired = run.retire(&after);
    assert!(valid && drop_unchanged && retired,
        "trusted producer Dispatch must commit before current claimed authority denies renewal, without callback or terminal");
}

#[test]
fn permission_original_always_driver_native_failed_persists_kernel_fail_attempt() {
    let (run, capture, installed, attempt) =
        original_always_run_fixture(DriverNativeResult::Failed, false);
    let before = run.capture();
    let hooks = OriginalAlwaysHooks(attempt.clone());
    let expected = StoredExecution {
        thread_id: run.ready.thread.clone(),
        snapshot: transition(
            &run.claimed.snapshot,
            KernelCommand::FailAttempt {
                task_id: run.ready.task.clone(),
                attempt_id: attempt,
                failure: "native-failure".into(),
            },
            facts(109),
        )
        .unwrap()
        .snapshot,
    };
    let actual = drive(&run, &hooks);
    let after = run.capture();
    let dispatch = capture.lock().unwrap().take();
    let valid = actual.as_ref().is_ok_and(|stored| stored == &expected)
        && expected.snapshot.revision == 5
        && expected.snapshot.status == ExecutionStatus::Failed
        && expected.snapshot.tasks[0].status == TaskStatus::Failed
        && run.callback_once()
        && dispatch.as_ref().is_some_and(|dispatch| {
            original_always_commit_matches(&before, dispatch, &run.claimed, true)
                && original_always_commit_matches(dispatch, &after, &expected, false)
        })
        && after.raw.tables[4..] == before.raw.tables[4..];
    drop(actual);
    drop(capture);
    drop(installed);
    let retired = run.retire(&after);
    assert!(valid && retired,
        "bounded native Failed must persist exact kernel FailAttempt5 with callback1, immutable approval evidence and no Use");
}

#[test]
fn permission_original_always_driver_late_journal_abort_retries_same_settled_work() {
    let (run, capture, installed, attempt) =
        original_always_run_fixture(DriverNativeResult::Succeeded, true);
    let before = run.capture();
    let hooks = OriginalAlwaysHooks(attempt.clone());
    let outcome = EffectOutcome::Succeeded(TaskResult {
        output: json!(true),
        writes: vec![],
    });
    let mut actual = drive(&run, &hooks);
    let after_abort = run.capture();
    let dispatch = capture.lock().unwrap().take();
    let before_usage = actual.as_ref().err().map(|failure| failure.usage());
    // The exact guard is installed inside the actual callback, and tests for
    // execution5 plus the updated thread row before aborting journal insertion.
    // Completion candidate acceptance and full rollback provide writer evidence;
    // a generic Store::Corrupt error alone would not establish this provenance.
    let rollback = installed.load(Ordering::SeqCst)
        && run.callback_once()
        && dispatch.as_ref() == Some(&after_abort)
        && original_always_commit_matches(&before, &after_abort, &run.claimed, true)
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
                && failure.settled_outcome() == Some(&outcome)
                && failure.usage().completion_entries() == 1
                && failure.usage().candidate_entries() == 1
                && failure.usage().candidate_refusals() == 0
                && failure.usage().refused_completions() == 1
                && failure.usage().committed_completions() == 0
        });
    let removed = driver_drop_abort(&run.ready.directory.0);
    let retried = actual
        .as_mut()
        .err()
        .map(|failure| failure.retry_settlement(&facts(110)));
    let after = run.capture();
    let after_usage = actual.as_ref().err().map(|failure| failure.usage());
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
            facts(110),
        )
        .unwrap()
        .snapshot,
    };
    let repeat_refused = actual
        .as_mut()
        .err()
        .is_some_and(|failure| failure.retry_settlement(&facts(111)).is_err());
    let valid = rollback
        && removed
        && retried
            .as_ref()
            .is_some_and(|result| result.as_ref().is_ok_and(|stored| stored == &expected))
        && expected.snapshot.revision == 5
        && expected.snapshot.status == ExecutionStatus::Completed
        && original_always_commit_matches(&after_abort, &after, &expected, false)
        && before_usage
            .zip(after_usage)
            .is_some_and(|(before, after)| {
                after.encoded_bytes() > before.encoded_bytes()
                    && after.structural_nodes() > before.structural_nodes()
                    && after.encoded_bytes() <= 16 * 1024 * 1024
                    && after.structural_nodes() <= 131_072
            })
        && repeat_refused
        && run.capture() == after
        && run.callback_once()
        && after.raw.tables[4..] == before.raw.tables[4..];
    drop(retried);
    drop(actual);
    let drop_unchanged = run.capture() == after;
    drop(capture);
    drop(installed);
    let retired = run.retire(&after);
    assert!(valid && drop_unchanged && retired,
        "exact late journal abort must roll back all nine tables, retain result and retry storage once without callback replay");
}
