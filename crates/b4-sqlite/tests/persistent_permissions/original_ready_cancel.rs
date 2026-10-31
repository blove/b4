//! Original UUID Ready cancellation through the existing typed entry.
use super::*;

#[test]
fn permission_original_uuid_ready_cancellation_persists_cancelled() {
    let mut fixture = OriginalWaitingFixture::new();
    let request = fixture.request.take().unwrap();
    let context = request.context.clone();
    let approved_turn = request.turn_id.clone();
    let definition = fixture.before.execution.snapshot.definition.clone();
    let original_grant: GrantRecord =
        serde_json::from_str(produced_text(&fixture.before.raw.tables[4][0][7])).unwrap();
    let resume = transition(
        &fixture.before.execution.snapshot,
        KernelCommand::Resume {
            route: request.route.clone(),
            decisions: vec![Decision {
                interrupt_id: request.decisions[0].interrupt_id.clone(),
                decision: ApprovalDecision::Always,
            }],
            turn_id: approved_turn.clone(),
        },
        facts(103),
    )
    .unwrap();
    let expected_ready = StoredExecution {
        thread_id: fixture.thread.clone(),
        snapshot: resume.snapshot,
    };
    let setup_authority = Arc::new(AllowAuthority);
    let setup = accept_permission_always_with_normal_inputs(
        fixture.store.clone(),
        setup_authority.clone(),
        fixture.catalog.clone(),
        &fixture.execution,
        fence(fixture.store.as_ref(), &fixture.thread),
        request,
    );
    let ready = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let setup_code = setup
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let mut ready_thread = fixture.before.view.thread.clone();
    ready_thread.resource_revision += 1;
    let setup_valid = fixture.waiting2
        && setup.as_ref().is_ok_and(|stored| stored == &expected_ready)
        && ready.execution == expected_ready
        && ready.view
            == (ThreadView {
                thread: ready_thread,
                execution: Some(expected_ready.clone()),
            })
        && expected_ready.snapshot.revision == 3
        && expected_ready.snapshot.status == ExecutionStatus::Running
        && expected_ready.snapshot.turn_id == approved_turn
        && expected_ready.snapshot.owner_generation == fixture.original_owner
        && expected_ready.snapshot.tasks[0].status == TaskStatus::Ready
        && expected_ready.snapshot.tasks[0].approved
        && expected_ready.snapshot.attempts_used == 0
        && expected_ready.snapshot.attempt_history.is_empty()
        && ready.journal.len() == fixture.before.journal.len() + 1
        && ready.journal[..fixture.before.journal.len()] == fixture.before.journal
        && ready.journal.last() == Some(&JournalEntry::from(&expected_ready.snapshot))
        && ready.raw.tables[0] == fixture.before.raw.tables[0]
        && original_records_match(
            &fixture.before,
            &ready,
            &original_grant,
            &context,
            &definition,
            &approved_turn,
        )
        && fixture.calls.load(Ordering::SeqCst) == 0;
    drop(setup);
    drop(setup_authority);
    drop(expected_ready);
    // A producer failure is a setup failure, never the cancellation gap.
    // Retire even that actual state before reporting its distinct error.
    if !setup_valid {
        let retired = retire_original_cancellation(fixture, &ready, None);
        assert!(retired, "failed setup must still retire its actual state");
        panic!("original UUID Ready3 setup failed before cancellation; error={setup_code}");
    }
    let acceptance = PermissionAcceptance::decode_record(
        produced_text(&ready.raw.tables[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let decision = PermissionDecision::decode_record(
        produced_text(&ready.raw.tables[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    let target = RunCancellationTarget::from_view(&ready.view).unwrap();
    let cancel = transition(
        &ready.execution.snapshot,
        KernelCommand::Cancel {
            turn_id: approved_turn,
            owner_generation: fixture.original_owner,
        },
        facts(104),
    )
    .unwrap();
    validate_commit(
        &ready.execution.snapshot,
        target.execution_fence,
        &cancel,
        fixture.original_owner,
    )
    .unwrap();
    let expected = StoredExecution {
        thread_id: fixture.thread.clone(),
        snapshot: cancel.snapshot,
    };
    let authority_calls = Arc::new(AtomicUsize::new(0));
    let capture_calls = Arc::new(AtomicUsize::new(0));
    let terminal_entries = Arc::new(AtomicUsize::new(0));
    let cancellation_authority = Arc::new(ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: ready.view.thread.clone(),
            execution: ready.execution.clone(),
        },
        expected_target: target.clone(),
        calls: authority_calls.clone(),
    });
    let observed = Arc::new(ReadyCancellationObservedStore {
        inner: fixture.store.clone(),
        captures: capture_calls.clone(),
        terminal_entries: terminal_entries.clone(),
    });
    let mut account = PermissionEvidenceBudget::new();
    let initial_usage = produced_usage(&account);
    let actual = cancel_permission_ready_with_normal_inputs(
        observed.as_ref(),
        cancellation_authority.as_ref(),
        fixture.catalog.as_ref(),
        &PermissionReadyCancellationRequest {
            context: &context,
            target: &target,
            facts: facts(104),
        },
        &mut account,
    );
    // Observe result, account, all counters and complete actual AFTER before assertions.
    let after = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let final_usage = produced_usage(&account);
    let counts = (
        authority_calls.load(Ordering::SeqCst),
        capture_calls.load(Ordering::SeqCst),
        terminal_entries.load(Ordering::SeqCst),
    );
    let callbacks_zero = fixture.calls.load(Ordering::SeqCst) == 0;
    let code = actual
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let unsupported = matches!(&actual, Err(PermissionError::UnsupportedProfile));
    let unchanged_refusal = unsupported && after == ready;
    let returned_exact = actual.as_ref().is_ok_and(|stored| stored == &expected);
    let success = returned_exact
        && cancelled_capture_matches(&ready, &after, &expected)
        && counts == (2, 3, 1);
    let usage_valid = initial_usage == (0, 0)
        && final_usage.0 > 0
        && final_usage.1 > 0
        && final_usage.0 <= 8_388_608
        && final_usage.1 <= 65_536;
    let stored_revision = after.execution.snapshot.revision;
    let changed = after != ready;
    drop(actual);
    drop(account);
    drop(observed);
    drop(cancellation_authority);
    drop(authority_calls);
    drop(capture_calls);
    drop(terminal_entries);
    let retired = retire_original_cancellation(fixture, &after, Some((&acceptance, &decision)));
    eprintln!("original-uuid-ready-cancel: error={code} counts={counts:?} initial_usage={initial_usage:?} final_usage={final_usage:?} changed={changed} stored_revision={stored_revision} unchanged_refusal={unchanged_refusal} success={success} callbacks_zero={callbacks_zero} retired={retired}");
    assert!(
        callbacks_zero && retired,
        "actual cancellation state must survive owned close/reopen/removal"
    );
    assert!(
        usage_valid,
        "fresh cancellation account must remain finite; error={code}"
    );
    assert!(success || unchanged_refusal, "only exact success or actual Unsupported with complete unchanged Ready3 establishes the scout outcome; error={code}");
    assert!(
        success,
        "original UUID Ready cancellation must persist exact Cancelled4; error={code}"
    );
}

fn cancelled_capture_matches(
    ready: &OriginalCapture,
    after: &OriginalCapture,
    expected: &StoredExecution,
) -> bool {
    use rusqlite::types::Value;
    let mut thread = ready.view.thread.clone();
    thread.resource_revision += 1;
    let entry = JournalEntry::from(&expected.snapshot);
    let mut raw = ready.raw.tables.clone();
    raw[1][0][5] = Value::Blob(thread.resource_revision.to_be_bytes().to_vec());
    raw[2][0][2] = Value::Blob(expected.snapshot.revision.to_be_bytes().to_vec());
    raw[2][0][3] = Value::Blob(expected.snapshot.owner_generation.to_be_bytes().to_vec());
    raw[2][0][4] = Value::Text(serde_json::to_string(&expected.snapshot).unwrap());
    let sequence = first_claim_journal_sequence(&ready.raw.tables[3].last().unwrap()[0]) + 1;
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
    after.execution == *expected
        && after.view
            == (ThreadView {
                thread,
                execution: Some(expected.clone()),
            })
        && expected.snapshot.revision == 4
        && expected.snapshot.status == ExecutionStatus::Cancelled
        && expected.snapshot.tasks[0].status == TaskStatus::Cancelled
        && expected.snapshot.tasks[0].approved
        && expected.snapshot.attempts_used == 0
        && expected.snapshot.attempt_history.is_empty()
        && after.journal.len() == ready.journal.len() + 1
        && after.journal[..ready.journal.len()] == ready.journal
        && after.journal.last() == Some(&entry)
        && after.raw.tables == raw
}

// Own every fixture anchor through normal close. The persisted oracle is the
// actual AFTER, including failure; it never infers rollback from an error.
fn retire_original_cancellation(
    fixture: OriginalWaitingFixture,
    after: &OriginalCapture,
    records: Option<(&PermissionAcceptance, &PermissionDecision)>,
) -> bool {
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
    drop(request);
    let catalog_weak = Arc::downgrade(&catalog);
    let application_weak = Arc::downgrade(&application);
    drop(catalog);
    drop(application);
    let anchors_retired = catalog_weak.upgrade().is_none()
        && application_weak.upgrade().is_none()
        && Arc::strong_count(&calls) == 1;
    let callbacks_zero = calls.load(Ordering::SeqCst) == 0;
    drop(calls);
    let sole_owner = Arc::strong_count(&store) == 1;
    let owner1_ready = store.readiness().is_ok();
    drop(store);
    let owner2 = SqliteStore::open(&directory.0).unwrap();
    let reopened = OriginalCapture::read(&owner2, &directory.0, &thread, &execution);
    let same = reopened.view == after.view
        && reopened.execution == after.execution
        && reopened.journal == after.journal
        && reopened.raw.tables[1..] == after.raw.tables[1..]
        && reopened.raw.tables[0][0][..3] == after.raw.tables[0][0][..3]
        && produced_u64(&after.raw.tables[0][0][3]) == original_owner
        && produced_u64(&reopened.raw.tables[0][0][3]) == original_owner + 1
        && owner2.owner_generation() == original_owner + 1;
    let canonical_records = records.is_none_or(|(acceptance, decision)| {
        let actual_acceptance = PermissionAcceptance::decode_record(
            produced_text(&reopened.raw.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        );
        let actual_decision = PermissionDecision::decode_record(
            produced_text(&reopened.raw.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        );
        let grant: GrantRecord =
            serde_json::from_str(produced_text(&reopened.raw.tables[4][0][7])).unwrap();
        actual_acceptance
            .as_ref()
            .is_ok_and(|actual| actual == acceptance)
            && actual_decision
                .as_ref()
                .is_ok_and(|actual| actual == decision)
            && grant.consumed
            && reopened.raw.tables[4][0][6] == rusqlite::types::Value::Integer(1)
    });
    let owner2_ready = owner2.readiness().is_ok();
    drop(reopened);
    drop(owner2);
    let path = directory.0.clone();
    drop(directory);
    anchors_retired
        && callbacks_zero
        && sole_owner
        && owner1_ready
        && owner2_ready
        && same
        && canonical_records
        && !path.exists()
}

// Both controls start with the same genuine normal Always producer as the
// unfaulted primary. A setup failure is retired and reported before any control.
struct OriginalReadyControl {
    fixture: OriginalWaitingFixture,
    ready: OriginalCapture,
    context: ApprovalContext,
    stale_target: RunCancellationTarget,
    acceptance: PermissionAcceptance,
    decision: PermissionDecision,
}
impl OriginalReadyControl {
    fn new() -> Self {
        Self::from_waiting(OriginalWaitingFixture::new())
    }

    fn from_waiting(mut fixture: OriginalWaitingFixture) -> Self {
        let stale_target = RunCancellationTarget::from_view(&fixture.before.view).unwrap();
        let request = fixture.request.take().unwrap();
        let context = request.context.clone();
        let turn = request.turn_id.clone();
        let definition = fixture.before.execution.snapshot.definition.clone();
        let grant: GrantRecord =
            serde_json::from_str(produced_text(&fixture.before.raw.tables[4][0][7])).unwrap();
        let resume = transition(
            &fixture.before.execution.snapshot,
            KernelCommand::Resume {
                route: request.route.clone(),
                decisions: vec![Decision {
                    interrupt_id: request.decisions[0].interrupt_id.clone(),
                    decision: ApprovalDecision::Always,
                }],
                turn_id: turn.clone(),
            },
            facts(103),
        )
        .unwrap();
        let expected = StoredExecution {
            thread_id: fixture.thread.clone(),
            snapshot: resume.snapshot,
        };
        let authority = Arc::new(AllowAuthority);
        let actual = accept_permission_always_with_normal_inputs(
            fixture.store.clone(),
            authority.clone(),
            fixture.catalog.clone(),
            &fixture.execution,
            fence(fixture.store.as_ref(), &fixture.thread),
            request,
        );
        let ready = OriginalCapture::read(
            fixture.store.as_ref(),
            &fixture.directory.0,
            &fixture.thread,
            &fixture.execution,
        );
        let mut thread = fixture.before.view.thread.clone();
        thread.resource_revision += 1;
        let valid = fixture.waiting2
            && stale_target.execution_fence.revision == 2
            && actual.as_ref().is_ok_and(|stored| stored == &expected)
            && ready.execution == expected
            && ready.view
                == (ThreadView {
                    thread,
                    execution: Some(expected.clone()),
                })
            && expected.snapshot.revision == 3
            && expected.snapshot.status == ExecutionStatus::Running
            && expected.snapshot.turn_id == turn
            && expected.snapshot.owner_generation == fixture.original_owner
            && expected.snapshot.tasks[0].status == TaskStatus::Ready
            && expected.snapshot.tasks[0].approved
            && expected.snapshot.attempts_used == 0
            && expected.snapshot.attempt_history.is_empty()
            && ready.journal.len() == fixture.before.journal.len() + 1
            && ready.journal[..fixture.before.journal.len()] == fixture.before.journal
            && ready.journal.last() == Some(&JournalEntry::from(&expected.snapshot))
            && ready.raw.tables[0] == fixture.before.raw.tables[0]
            && original_records_match(
                &fixture.before,
                &ready,
                &grant,
                &context,
                &definition,
                &turn,
            )
            && fixture.calls.load(Ordering::SeqCst) == 0;
        let code = actual
            .as_ref()
            .err()
            .map(PermissionError::code)
            .unwrap_or("ok");
        drop(actual);
        drop(authority);
        drop(expected);
        if !valid {
            let retired = retire_original_cancellation(fixture, &ready, None);
            assert!(retired, "failed control setup must retire actual state");
            panic!("original UUID Ready3 control setup failed; error={code}");
        }
        let acceptance = PermissionAcceptance::decode_record(
            produced_text(&ready.raw.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        let decision = PermissionDecision::decode_record(
            produced_text(&ready.raw.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        Self {
            fixture,
            ready,
            context,
            stale_target,
            acceptance,
            decision,
        }
    }
}

#[test]
fn permission_original_uuid_ready_cancellation_stale_target_preserves_ready() {
    let OriginalReadyControl {
        fixture,
        ready,
        context,
        stale_target,
        acceptance,
        decision,
    } = OriginalReadyControl::new();
    let authorities = Arc::new(AtomicUsize::new(0));
    let captures = Arc::new(AtomicUsize::new(0));
    let terminals = Arc::new(AtomicUsize::new(0));
    let authority = ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: ready.view.thread.clone(),
            execution: ready.execution.clone(),
        },
        expected_target: stale_target.clone(),
        calls: authorities.clone(),
    };
    let observed = ReadyCancellationObservedStore {
        inner: fixture.store.clone(),
        captures: captures.clone(),
        terminal_entries: terminals.clone(),
    };
    let mut account = PermissionEvidenceBudget::new();
    let initial = produced_usage(&account);
    let actual = cancel_permission_ready_with_normal_inputs(
        &observed,
        &authority,
        fixture.catalog.as_ref(),
        &PermissionReadyCancellationRequest {
            context: &context,
            target: &stale_target,
            facts: facts(104),
        },
        &mut account,
    );
    let after = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let refused = matches!(
        &actual,
        Err(PermissionError::Store(StoreError::RevisionConflict))
    );
    let unchanged = after == ready;
    let callbacks_zero = fixture.calls.load(Ordering::SeqCst) == 0;
    let counts = (
        authorities.load(Ordering::SeqCst),
        captures.load(Ordering::SeqCst),
        terminals.load(Ordering::SeqCst),
    );
    let usage = produced_usage(&account);
    let code = actual
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    drop(actual);
    drop(account);
    drop(observed);
    drop(authority);
    drop(authorities);
    drop(captures);
    drop(terminals);
    let retired = retire_original_cancellation(fixture, &after, Some((&acceptance, &decision)));
    eprintln!("original-uuid-ready-stale: error={code} counts={counts:?} usage={usage:?} unchanged={unchanged} callbacks_zero={callbacks_zero} retired={retired}");
    assert!(
        callbacks_zero && retired,
        "stale control must close/reopen/remove its actual state"
    );
    assert!(
        initial == (0, 0)
            && usage.0 > 0
            && usage.1 > 0
            && usage.0 <= 8_388_608
            && usage.1 <= 65_536
    );
    assert!(refused && unchanged && counts == (0, 1, 0), "real Waiting2 target must hit RevisionConflict against current original Always Ready3; error={code}");
}

#[test]
fn permission_original_uuid_ready_cancellation_late_journal_fault_rolls_back() {
    let OriginalReadyControl {
        fixture,
        ready,
        context,
        acceptance,
        decision,
        ..
    } = OriginalReadyControl::new();
    let target = RunCancellationTarget::from_view(&ready.view).unwrap();
    let cancel = transition(
        &ready.execution.snapshot,
        KernelCommand::Cancel {
            turn_id: ready.execution.snapshot.turn_id.clone(),
            owner_generation: fixture.original_owner,
        },
        facts(104),
    )
    .unwrap();
    validate_commit(
        &ready.execution.snapshot,
        target.execution_fence,
        &cancel,
        fixture.original_owner,
    )
    .unwrap();
    // Guard the final journal insert on the exact candidate already written by
    // SnapshotCas and ThreadCas inside this same transaction. No fixture rows
    // are mutated by injection. The existing unfaulted primary qualifies the
    // same writer's success; the shared fixed error alone is not trigger proof.
    let snapshot_sql = serde_json::to_string(&cancel.snapshot)
        .unwrap()
        .replace('\'', "''");
    let thread = fixture.thread.as_str();
    let execution = fixture.execution.as_str();
    let resource_revision = ready.view.thread.resource_revision + 1;
    let trigger_sql = format!(
        "CREATE TRIGGER original_ready_cancel_journal_abort BEFORE INSERT ON journal \
         WHEN NEW.kind = 'commit' AND NEW.thread_id = '{thread}' \
         AND NEW.execution_id = '{execution}' AND NEW.revision = x'0000000000000004' \
         AND NEW.status = '\"cancelled\"' \
         AND EXISTS (SELECT 1 FROM executions WHERE id = '{execution}' AND thread_id = '{thread}' \
         AND revision = x'0000000000000004' AND snapshot = '{snapshot_sql}') \
         AND EXISTS (SELECT 1 FROM threads WHERE id = '{thread}' \
         AND active_execution_id = '{execution}' AND resource_revision = x'{resource_revision:016x}') \
         BEGIN SELECT RAISE(ABORT, 'original_ready_cancel_journal_fault'); END;"
    );
    let connection = driver_injection_connection(&fixture.directory.0);
    connection.execute_batch(&trigger_sql).unwrap();
    let installed = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'original_ready_cancel_journal_abort' AND sql = ?1",
        [&trigger_sql.trim_end_matches(';')], |row| row.get::<_, i64>(0),
    ).unwrap() == 1;
    drop(connection);
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
    let mut account = PermissionEvidenceBudget::new();
    let initial = produced_usage(&account);
    let actual = cancel_permission_ready_with_normal_inputs(
        &observed,
        &authority,
        fixture.catalog.as_ref(),
        &PermissionReadyCancellationRequest {
            context: &context,
            target: &target,
            facts: facts(104),
        },
        &mut account,
    );
    let after = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let rolled_back = after == ready;
    let mapped_error = matches!(&actual, Err(PermissionError::Store(StoreError::Corrupt(code))) if code == "permission_acceptance_cells_invalid");
    let callbacks_zero = fixture.calls.load(Ordering::SeqCst) == 0;
    let counts = (
        authorities.load(Ordering::SeqCst),
        captures.load(Ordering::SeqCst),
        terminals.load(Ordering::SeqCst),
    );
    let usage = produced_usage(&account);
    let code = actual
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let connection = driver_injection_connection(&fixture.directory.0);
    connection
        .execute_batch("DROP TRIGGER original_ready_cancel_journal_abort")
        .unwrap();
    let removed = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'original_ready_cancel_journal_abort'",
        [], |row| row.get::<_, i64>(0),
    ).unwrap() == 0;
    drop(connection);
    drop(actual);
    drop(account);
    drop(observed);
    drop(authority);
    drop(authorities);
    drop(captures);
    drop(terminals);
    let retired = retire_original_cancellation(fixture, &after, Some((&acceptance, &decision)));
    eprintln!("original-uuid-ready-journal-fault: error={code} counts={counts:?} usage={usage:?} installed={installed} removed={removed} rolled_back={rolled_back} callbacks_zero={callbacks_zero} retired={retired}");
    assert!(
        callbacks_zero && retired,
        "fault control must close/reopen/remove its actual state"
    );
    assert!(
        initial == (0, 0)
            && usage.0 > 0
            && usage.1 > 0
            && usage.0 <= 8_388_608
            && usage.1 <= 65_536
    );
    assert!(
        installed && removed && mapped_error && rolled_back && counts == (2, 3, 1),
        "guarded late journal failure must preserve complete original Always Ready3; error={code}"
    );
}

#[path = "original_reuse_driver.rs"]
mod original_reuse_driver;
