mod confinement_support;
use b4_contract::*;
use b4_runtime::*;
use confinement_support::*;
use rusqlite::params;
use serde_json::{json, Map, Value};

#[test]
fn sqlite_pending_durable_grant_survives_confinement_and_actual_owner_reopen() {
    let directory = Directory::new();
    let store = seed(
        &directory,
        vec![vec![spec("permission", true, RecoveryPolicy::Unsafe)]],
        Map::from_iter([("original".into(), json!({"input": true}))]),
        Map::new(),
    );
    commit(&store, KernelCommand::Start);
    park(&store, "permission");
    let records = authority(&store);
    assert_eq!(records.grants.len(), 1);
    assert!(records.acceptances.is_empty());
    assert!(!records.grants[0].consumed);
    assert!(records.grants[0].expires_at_ms < 5_000);
    assert_eq!(
        records.execution.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    let store = advance(&directory, store);
    assert_confined(
        &directory,
        store,
        RecoveryConfinementReason::RequiredAuthorityUnavailable,
    );
}

#[test]
fn sqlite_historical_once_deny_and_failure_drain_authority_remain_exact_after_expiry() {
    let directory = Directory::new();
    let store = seed(
        &directory,
        vec![
            vec![spec("history", false, RecoveryPolicy::Pure)],
            vec![
                spec("once", true, RecoveryPolicy::Pure),
                spec("deny", true, RecoveryPolicy::Unsafe),
            ],
            vec![
                spec("failed", false, RecoveryPolicy::Pure),
                spec("held", false, RecoveryPolicy::Unsafe),
            ],
            vec![spec("future", false, RecoveryPolicy::Reconcilable)],
        ],
        Map::new(),
        Map::from_iter([("original_metadata".into(), json!({"keep": true}))]),
    );
    commit(&store, KernelCommand::Start);
    dispatch(&store, "history");
    complete(&store, "history");
    park(&store, "once");
    park(&store, "deny");
    accept(
        &store,
        &[
            ("once", ApprovalDecision::Once),
            ("deny", ApprovalDecision::Deny),
        ],
    );
    dispatch(&store, "once");
    complete(&store, "once");
    dispatch(&store, "failed");
    dispatch(&store, "held");
    commit(
        &store,
        KernelCommand::FailAttempt {
            task_id: TaskId::new("failed").unwrap(),
            attempt_id: AttemptId::new("backend-attempt-failed").unwrap(),
            failure: "actual-task-cause".into(),
        },
    );
    let records = authority(&store);
    assert_eq!(records.grants.len(), 2);
    assert!(records
        .grants
        .iter()
        .all(|grant| grant.consumed && grant.expires_at_ms < 5_000));
    assert_eq!(records.acceptances.len(), 1);
    assert_eq!(records.acceptances[0].decisions.len(), 2);
    assert_eq!(records.execution.snapshot.status, ExecutionStatus::Running);
    assert_eq!(records.execution.snapshot.batch_index, 2);
    assert_eq!(
        records.execution.snapshot.tasks[0].status,
        TaskStatus::Succeeded
    );
    assert_eq!(
        records.execution.snapshot.tasks[1].status,
        TaskStatus::Succeeded
    );
    assert_eq!(
        records.execution.snapshot.tasks[2].status,
        TaskStatus::Denied
    );
    assert_eq!(
        records.execution.snapshot.tasks[4].status,
        TaskStatus::InFlight
    );
    let store = advance(&directory, store);
    assert_confined(
        &directory,
        store,
        RecoveryConfinementReason::DispatchNotAuthorized,
    );
}

#[test]
fn sqlite_cancel_requested_accepts_retained_or_legitimately_deleted_outstanding_grant() {
    for fenced in [false, true] {
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![vec![
                spec("held", false, RecoveryPolicy::Pure),
                spec("permission", true, RecoveryPolicy::Unsafe),
            ]],
            Map::new(),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        dispatch(&store, "held");
        park(&store, "permission");
        let current = ordinary(&store);
        let snapshot = &current.execution.as_ref().unwrap().snapshot;
        if fenced {
            store
                .cancel_run_fenced(
                    &RunCancellationTarget::from_view(&current).unwrap(),
                    TransitionFacts {
                        now_ms: snapshot.updated_at_ms + 1,
                    },
                )
                .unwrap();
        } else {
            commit(
                &store,
                KernelCommand::Cancel {
                    turn_id: snapshot.turn_id.clone(),
                    owner_generation: snapshot.owner_generation,
                },
            );
        }
        let records = authority(&store);
        assert_eq!(
            records.execution.snapshot.status,
            ExecutionStatus::CancelRequested
        );
        assert_eq!(
            records.execution.snapshot.tasks[0].status,
            TaskStatus::InFlight
        );
        assert_eq!(
            records.execution.snapshot.tasks[1].status,
            TaskStatus::Cancelled
        );
        assert!(records.execution.snapshot.tasks[1].interrupt.is_some());
        assert!(records.acceptances.is_empty());
        assert_eq!(records.grants.len(), usize::from(!fenced));
        assert!(records.grants.iter().all(|grant| !grant.consumed));
        let store = advance(&directory, store);
        assert_confined(
            &directory,
            store,
            RecoveryConfinementReason::ExactDefinitionUnavailable,
        );
    }
}

#[test]
fn sqlite_all_four_inflight_recovery_kinds_are_blocked_without_attempt_reset_or_settlement() {
    let directory = Directory::new();
    let store = seed(
        &directory,
        vec![vec![
            spec("pure", false, RecoveryPolicy::Pure),
            spec("idempotent", false, RecoveryPolicy::Idempotent),
            spec("reconcilable", false, RecoveryPolicy::Reconcilable),
            spec("unsafe", false, RecoveryPolicy::Unsafe),
            spec("ready", false, RecoveryPolicy::Pure),
        ]],
        Map::from_iter([("original".into(), json!(true))]),
        Map::new(),
    );
    commit(&store, KernelCommand::Start);
    for id in ["pure", "idempotent", "reconcilable", "unsafe"] {
        dispatch(&store, id);
    }
    let original = ordinary(&store);
    assert!(original.execution.as_ref().unwrap().snapshot.tasks[..4]
        .iter()
        .all(|task| task.status == TaskStatus::InFlight));
    assert_eq!(
        original.execution.as_ref().unwrap().snapshot.tasks[4].status,
        TaskStatus::Ready
    );
    let store = advance(&directory, store);
    assert_confined(
        &directory,
        store,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
    );
}

#[test]
fn sqlite_legacy_direct_once_and_always_without_durable_rows_remain_unsupported() {
    for decision in [ApprovalDecision::Once, ApprovalDecision::Always] {
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![vec![spec("legacy", true, RecoveryPolicy::Unsafe)]],
            Map::new(),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        commit(
            &store,
            KernelCommand::Park {
                task_id: TaskId::new("legacy").unwrap(),
                interrupt_id: InterruptId::new("legacy-direct-interrupt").unwrap(),
                prompt: json!({"legacy": true}),
            },
        );
        commit(
            &store,
            KernelCommand::Resume {
                route: ordinary(&store)
                    .execution
                    .as_ref()
                    .unwrap()
                    .snapshot
                    .definition
                    .route
                    .clone(),
                decisions: vec![Decision {
                    interrupt_id: InterruptId::new("legacy-direct-interrupt").unwrap(),
                    decision,
                }],
                turn_id: TurnId::new("legacy-direct-resume").unwrap(),
            },
        );
        let original = authority(&store);
        assert_eq!(original.execution.snapshot.status, ExecutionStatus::Running);
        assert!(original.execution.snapshot.tasks[0].approved);
        assert!(original.grants.is_empty());
        assert!(original.acceptances.is_empty());
        let store = advance(&directory, store);
        assert_refused(
            &directory,
            store,
            RecoveryConfinementBackendError::UnsupportedAuthorityProfile,
        );
    }
}

#[test]
fn sqlite_present_authority_contradictions_are_corrupt_without_fabricated_missing_profile() {
    for mode in [
        "grant-index",
        "grant-digest",
        "acceptance-index",
        "missing-consumed-grant",
    ] {
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![
                vec![spec("permission", true, RecoveryPolicy::Pure)],
                vec![spec("next", false, RecoveryPolicy::Pure)],
            ],
            Map::new(),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        park(&store, "permission");
        if mode.starts_with("acceptance") || mode == "missing-consumed-grant" {
            accept(&store, &[("permission", ApprovalDecision::Once)]);
            dispatch(&store, "permission");
            complete(&store, "permission");
        }
        let original = ordinary(&store);
        let connection = directory.connection();
        match mode {
            "grant-index" => {
                connection
                    .execute(
                        "UPDATE approval_grants SET consumed=1 WHERE execution_id=?1",
                        [execution_id().as_str()],
                    )
                    .unwrap();
            }
            "grant-digest" => {
                let mut grant = authority(&store).grants.remove(0);
                grant.proposal_digest = "f".repeat(64);
                connection
                    .execute(
                        "UPDATE approval_grants SET record=?1 WHERE execution_id=?2",
                        params![
                            serde_json::to_string(&grant).unwrap(),
                            execution_id().as_str()
                        ],
                    )
                    .unwrap();
            }
            "acceptance-index" => {
                connection.execute("UPDATE approval_acceptances SET previous_revision=?1 WHERE execution_id=?2", params![111_u64.to_be_bytes().to_vec(), execution_id().as_str()]).unwrap();
            }
            "missing-consumed-grant" => {
                connection
                    .execute(
                        "DELETE FROM approval_grants WHERE execution_id=?1",
                        [execution_id().as_str()],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        drop(connection);
        assert_eq!(ordinary(&store), original);
        let store = advance(&directory, store);
        assert_refused(
            &directory,
            store,
            RecoveryConfinementBackendError::CorruptRecord,
        );
    }
}

#[test]
fn sqlite_compact_raw_failure_drain_is_actually_confined_when_replacement_shrinks() {
    let build = |directory: &Directory, padding: usize| {
        let store = seed(
            directory,
            vec![vec![
                spec("failed", false, RecoveryPolicy::Pure),
                spec("held", false, RecoveryPolicy::Unsafe),
            ]],
            Map::from_iter([
                ("padding".into(), Value::String("x".repeat(padding))),
                ("numbers".into(), json!(vec![100_000_000.0_f64; 500])),
            ]),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        dispatch(&store, "failed");
        dispatch(&store, "held");
        commit(
            &store,
            KernelCommand::FailAttempt {
                task_id: TaskId::new("failed").unwrap(),
                attempt_id: AttemptId::new("backend-attempt-failed").unwrap(),
                failure: "r".repeat(MAX_TASK_FAILURE_BYTES),
            },
        );
        store
    };
    let measuring = Directory::new();
    let measure_store = build(&measuring, 0);
    let base = snapshot_raw(&measuring).len();
    drop(measure_store);
    let padding = (SNAPSHOT_BYTES + 100 - base) / 3;
    let directory = Directory::new();
    let store = build(&directory, padding);
    let original = ordinary(&store);
    let canonical = snapshot_raw(&directory);
    assert!(canonical.len() > SNAPSHOT_BYTES);
    assert!(canonical.len() <= SNAPSHOT_BYTES + 100);
    assert_eq!(canonical.matches("100000000.0").count(), 1_500);
    let compact = canonical.replace("100000000.0", "1e8");
    assert!(compact.len() <= SNAPSHOT_BYTES);
    write_snapshot_raw(&directory, &compact);
    assert_eq!(ordinary(&store), original);
    let expected = expected(
        &original,
        2,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &TurnId::new("backend-confinement-turn").unwrap(),
    );
    assert!(
        serde_json::to_vec(&expected.execution.as_ref().unwrap().snapshot)
            .unwrap()
            .len()
            <= SNAPSHOT_BYTES
    );
    let store = advance(&directory, store);
    assert_confined(
        &directory,
        store,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
    );
}

#[test]
fn sqlite_raw_snapshot_byte_limit_is_inclusive_without_conflating_replacement_bytes() {
    for extra in [0, 1] {
        let measuring = Directory::new();
        let measuring_store = seed(
            &measuring,
            vec![vec![spec("unstarted", false, RecoveryPolicy::Pure)]],
            Map::from_iter([("padding".into(), Value::String(String::new()))]),
            Map::new(),
        );
        let base = snapshot_raw(&measuring).len();
        drop(measuring_store);
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![vec![spec("unstarted", false, RecoveryPolicy::Pure)]],
            Map::from_iter([(
                "padding".into(),
                Value::String("x".repeat(SNAPSHOT_BYTES - base + extra)),
            )]),
            Map::new(),
        );
        assert_eq!(snapshot_raw(&directory).len(), SNAPSHOT_BYTES + extra);
        let store = advance(&directory, store);
        let original = ordinary(&store);
        let target = RecoveryConfinementTarget::from_view(&original).unwrap();
        let before = directory.rows();
        let capture = store.confinement_view_fenced(&target);
        let mutation = store.confine_recovery_fenced(
            &original,
            &target,
            RecoveryConfinementReason::ExactDefinitionUnavailable,
            &TurnId::new("backend-confinement-turn").unwrap(),
            TransitionFacts { now_ms: 5_000 },
        );
        assert_eq!(directory.rows(), before);
        retire(&directory, store, &original, &before);
        if extra == 0 {
            assert_eq!(capture.unwrap(), original);
        } else {
            assert_eq!(
                capture.unwrap_err(),
                RecoveryConfinementBackendError::LimitExceeded
            );
        }
        // Valid exact-size input grows beyond the separate replacement cap.
        assert_eq!(
            mutation.unwrap_err(),
            RecoveryConfinementBackendError::LimitExceeded
        );
    }
}

#[test]
fn sqlite_compact_valid_input_can_capture_but_refuse_a_large_canonical_replacement() {
    let directory = Directory::new();
    let store = seed(
        &directory,
        vec![vec![spec("unstarted", false, RecoveryPolicy::Pure)]],
        Map::from_iter([
            ("padding".into(), Value::String(String::new())),
            ("numbers".into(), json!(vec![100_000_000.0_f64; 500])),
        ]),
        Map::new(),
    );
    let mut snapshot = ordinary(&store).execution.unwrap().snapshot;
    let base = serde_json::to_vec(&snapshot).unwrap().len();
    snapshot.values.insert(
        "padding".into(),
        Value::String("x".repeat(SNAPSHOT_BYTES + 100 - base)),
    );
    let canonical = serde_json::to_string(&snapshot).unwrap();
    assert_eq!(canonical.len(), SNAPSHOT_BYTES + 100);
    let compact = canonical.replace("100000000.0", "1e8");
    assert!(compact.len() < SNAPSHOT_BYTES);
    write_snapshot_raw(&directory, &compact);
    let store = advance(&directory, store);
    let original = ordinary(&store);
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = directory.rows();
    let capture = store.confinement_view_fenced(&target);
    let mutation = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &TurnId::new("backend-confinement-turn").unwrap(),
        TransitionFacts { now_ms: 5_000 },
    );
    assert_eq!(directory.rows(), before);
    retire(&directory, store, &original, &before);
    assert_eq!(capture.unwrap(), original);
    assert_eq!(
        mutation.unwrap_err(),
        RecoveryConfinementBackendError::LimitExceeded
    );
}

#[test]
fn sqlite_thread_component_cap_counts_raw_metadata_origin_and_fixed_identity_scalars() {
    for extra in [0, 1] {
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![vec![spec("unstarted", false, RecoveryPolicy::Pure)]],
            Map::new(),
            Map::from_iter([("padding".into(), Value::String(String::new()))]),
        );
        let original = ordinary(&store);
        let scalars = original.thread.id.as_str().len()
            + original.thread.generation.as_str().len()
            + original
                .thread
                .active_execution_id
                .as_ref()
                .unwrap()
                .as_str()
                .len()
            + 16;
        let base = serde_json::to_vec(&original.thread.metadata).unwrap().len()
            + serde_json::to_vec(original.thread.origin.as_ref().unwrap())
                .unwrap()
                .len()
            + scalars;
        let metadata = Map::from_iter([(
            "padding".into(),
            Value::String("x".repeat(THREAD_BYTES - base + extra)),
        )]);
        let encoded = serde_json::to_string(&metadata).unwrap();
        assert_eq!(
            encoded.len()
                + serde_json::to_vec(original.thread.origin.as_ref().unwrap())
                    .unwrap()
                    .len()
                + scalars,
            THREAD_BYTES + extra
        );
        directory
            .connection()
            .execute(
                "UPDATE threads SET metadata=?1 WHERE id=?2",
                params![encoded, thread_id().as_str()],
            )
            .unwrap();
        let store = advance(&directory, store);
        if extra == 0 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}

#[test]
fn sqlite_depth_and_node_reader_bounds_and_fresh_turn_output_growth_are_distinct() {
    for depth in [62, 63] {
        let directory = Directory::new();
        let nested = (0..depth).fold(Value::Null, |value, _| Value::Array(vec![value]));
        let store = seed(
            &directory,
            vec![vec![spec("unstarted", false, RecoveryPolicy::Pure)]],
            Map::from_iter([("nested".into(), nested)]),
            Map::new(),
        );
        let store = advance(&directory, store);
        if depth == 62 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
    let directory = Directory::new();
    let store = seed(
        &directory,
        vec![vec![spec("unstarted", false, RecoveryPolicy::Pure)]],
        Map::from_iter([("wide".into(), Value::Array(vec![]))]),
        Map::new(),
    );
    let mut snapshot = ordinary(&store).execution.unwrap().snapshot;
    let nulls = 16_384 - shape_nodes(&snapshot);
    snapshot
        .values
        .insert("wide".into(), Value::Array(vec![Value::Null; nulls]));
    assert_eq!(shape_nodes(&snapshot), 16_384);
    write_snapshot_raw(&directory, &serde_json::to_string(&snapshot).unwrap());
    let store = advance(&directory, store);
    let original = ordinary(&store);
    let before = directory.rows();
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let capture = store.confinement_view_fenced(&target);
    let mutation = store.confine_recovery_fenced(
        &original,
        &target,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
        &TurnId::new("backend-confinement-turn").unwrap(),
        TransitionFacts { now_ms: 5_000 },
    );
    assert_eq!(directory.rows(), before);
    retire(&directory, store, &original, &before);
    assert_eq!(capture.unwrap(), original);
    assert_eq!(
        mutation.unwrap_err(),
        RecoveryConfinementBackendError::LimitExceeded
    );
}

#[test]
fn sqlite_atomic_boundary_rejects_changed_whole_original_view_with_matching_fences() {
    let directory = Directory::new();
    let store = seed(
        &directory,
        vec![vec![spec("unstarted", false, RecoveryPolicy::Pure)]],
        Map::from_iter([("original_value".into(), json!(true))]),
        Map::from_iter([("original_metadata".into(), json!(true))]),
    );
    let store = advance(&directory, store);
    let original = ordinary(&store);
    let target = RecoveryConfinementTarget::from_view(&original).unwrap();
    let before = directory.rows();
    let original_authority = authority(&store);
    let captured = store.confinement_view_fenced(&target);
    let mut changed_views = Vec::new();
    let mut changed = original.clone();
    changed
        .thread
        .metadata
        .insert("original_metadata".into(), json!(false));
    changed_views.push(changed);
    let mut changed = original.clone();
    changed.thread.created_at_ms += 1;
    changed_views.push(changed);
    let mut changed = original.clone();
    changed.thread.origin.as_mut().unwrap().principal = "different-original-actor".into();
    changed_views.push(changed);
    let mut changed = original.clone();
    changed
        .execution
        .as_mut()
        .unwrap()
        .snapshot
        .values
        .insert("original_value".into(), json!(false));
    changed_views.push(changed);
    let mut changed = original.clone();
    changed
        .execution
        .as_mut()
        .unwrap()
        .snapshot
        .definition
        .max_attempts += 1;
    changed_views.push(changed);
    let observations: Vec<_> = changed_views
        .iter()
        .map(|changed| {
            assert_eq!(
                RecoveryConfinementTarget::from_view(changed).unwrap(),
                target
            );
            b4_kernel::validate_snapshot(&changed.execution.as_ref().unwrap().snapshot).unwrap();
            store.confine_recovery_fenced(
                changed,
                &target,
                RecoveryConfinementReason::DispatchNotAuthorized,
                &TurnId::new("backend-full-view-guard").unwrap(),
                TransitionFacts { now_ms: 5_000 },
            )
        })
        .collect();
    assert_eq!(ordinary(&store), original);
    assert_eq!(authority(&store), original_authority);
    assert_eq!(directory.rows(), before);
    retire(&directory, store, &original, &before);
    assert_eq!(captured.unwrap(), original);
    for observation in observations {
        assert_eq!(
            observation.unwrap_err(),
            RecoveryConfinementBackendError::TargetChanged
        );
    }
}

#[test]
fn sqlite_actual_required_durable_pending_grant_count_has_inclusive_64_record_limit() {
    for count in [64, 65] {
        let directory = Directory::new();
        let ids: Vec<_> = (0..count)
            .map(|index| format!("permission-{index:02}"))
            .collect();
        let store = seed(
            &directory,
            vec![ids
                .iter()
                .map(|id| spec(id, true, RecoveryPolicy::Unsafe))
                .collect()],
            Map::new(),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        for id in &ids {
            park(&store, id);
        }
        let records = authority(&store);
        assert_eq!(records.grants.len(), count);
        assert!(records.grants.iter().all(|grant| !grant.consumed));
        assert!(records.acceptances.is_empty());
        assert_eq!(
            records.execution.snapshot.status,
            ExecutionStatus::WaitingForApproval
        );
        b4_kernel::validate_snapshot(&records.execution.snapshot).unwrap();
        let store = advance(&directory, store);
        if count == 64 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::RequiredAuthorityUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}

// Additional backend boundary cases.

#[test]
#[ignore = "slow real SQLite owner succession; run separately"]
fn sqlite_journal_4095_append_and_prior_blocked_4096_audit_have_distinct_reservations() {
    let directory = Directory::new();
    let store = boundary_journal_history(&directory, 4_095, Map::new());
    let store = advance(&directory, store);
    assert_eq!(store.journal(&execution_id()).unwrap().len(), 4_095);
    boundary_assert_individual_payload_caps(&directory.rows());
    assert_confined(
        &directory,
        store,
        RecoveryConfinementReason::ExactDefinitionUnavailable,
    );

    // The preceding actual block is the 4096th entry. Its helper retired the
    // owner and proved a genuine next writer. A further actual writer sees
    // this same complete prior-owner block and must append nothing.
    let store = std::sync::Arc::new(b4_sqlite::SqliteStore::open(directory.path()).unwrap());
    let original = ordinary(&store);
    let snapshot = &original.execution.as_ref().unwrap().snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert!(snapshot.owner_generation < store.owner_generation());
    assert_eq!(store.journal(&execution_id()).unwrap().len(), 4_096);
    boundary_assert_already_blocked(&directory, store);
}

#[test]
#[ignore = "slow real SQLite owner succession; run separately"]
fn sqlite_eligible_4096_entry_journal_refuses_a_reserved_confinement_append() {
    let directory = Directory::new();
    let store = boundary_journal_history(&directory, 4_096, Map::new());
    assert_eq!(
        ordinary(&store).execution.as_ref().unwrap().snapshot.status,
        ExecutionStatus::Running
    );
    boundary_assert_individual_payload_caps(&directory.rows());
    let store = advance(&directory, store);
    assert_refused(
        &directory,
        store,
        RecoveryConfinementBackendError::LimitExceeded,
    );
}

#[test]
fn sqlite_actual_64_acceptances_fit_and_65_complete_acceptances_cross_both_authority_counts() {
    for count in [64, 65] {
        let directory = Directory::new();
        let ids: Vec<_> = (0..count)
            .map(|i| format!("boundary-approved-{i:02}"))
            .collect();
        let mut batches: Vec<_> = ids
            .iter()
            .map(|id| vec![spec(id, true, RecoveryPolicy::Unsafe)])
            .collect();
        batches.push(vec![spec("boundary-future", false, RecoveryPolicy::Pure)]);
        let store = seed(&directory, batches, Map::new(), Map::new());
        commit(&store, KernelCommand::Start);
        for (index, id) in ids.iter().enumerate() {
            park(&store, id);
            boundary_accept_deny(&store, id, &format!("boundary-approved-turn-{index:02}"));
        }
        let records = authority(&store);
        assert_eq!(records.grants.len(), count);
        assert_eq!(records.acceptances.len(), count);
        assert!(records.grants.iter().all(|grant| grant.consumed));
        assert!(records
            .acceptances
            .iter()
            .all(|acceptance| acceptance.decisions.len() == 1
                && acceptance.decisions[0].decision == ApprovalDecision::Deny));
        assert_eq!(records.execution.snapshot.status, ExecutionStatus::Running);
        assert_eq!(records.execution.snapshot.batch_index, count);
        assert_eq!(records.execution.snapshot.attempts_used, 0);
        assert!(records.execution.snapshot.tasks[..count]
            .iter()
            .all(|task| task.status == TaskStatus::Denied && !task.approved));
        assert_eq!(
            records.execution.snapshot.tasks[count].status,
            TaskStatus::Ready
        );
        b4_kernel::validate_snapshot(&records.execution.snapshot).unwrap();
        let store = advance(&directory, store);
        if count == 64 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::DispatchNotAuthorized,
            );
        } else {
            // A complete 65-acceptance profile necessarily also has65 distinct
            // consumed grants. This is a joint authority-count refusal, not
            // isolated proof of the acceptance65 reader limit.
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}

#[test]
fn sqlite_real_grant_raw_128k_boundary_changes_spelling_only_and_is_inclusive() {
    for extra in [0, 1] {
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![vec![spec(
                "boundary-permission",
                true,
                RecoveryPolicy::Unsafe,
            )]],
            Map::new(),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        park(&store, "boundary-permission");
        let records = authority(&store);
        assert_eq!(records.grants.len(), 1);
        boundary_pad_grant(
            &directory,
            &records.grants[0].interrupt_id,
            BOUNDARY_AUTHORITY_BYTES + extra,
        );
        assert_eq!(authority(&store), records);
        let store = advance(&directory, store);
        if extra == 0 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::RequiredAuthorityUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}

#[test]
fn sqlite_real_acceptance_raw_128k_boundary_preserves_actual_consumed_authority() {
    for extra in [0, 1] {
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![
                vec![spec("boundary-permission", true, RecoveryPolicy::Unsafe)],
                vec![spec("boundary-future", false, RecoveryPolicy::Pure)],
            ],
            Map::new(),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        park(&store, "boundary-permission");
        boundary_accept_deny(&store, "boundary-permission", "boundary-record-limit-turn");
        let records = authority(&store);
        assert_eq!(records.acceptances.len(), 1);
        assert_eq!(records.grants.len(), 1);
        assert!(records.grants[0].consumed);
        assert_eq!(records.execution.snapshot.status, ExecutionStatus::Running);
        boundary_pad_acceptance(
            &directory,
            &records.acceptances[0].turn_id,
            BOUNDARY_AUTHORITY_BYTES + extra,
        );
        assert_eq!(authority(&store), records);
        let store = advance(&directory, store);
        if extra == 0 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::RequiredAuthorityUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}

#[test]
fn sqlite_real_admit_journal_raw_4k_boundary_preserves_typed_projection_and_indexes() {
    for extra in [0, 1] {
        let directory = Directory::new();
        let store = seed(
            &directory,
            vec![vec![spec(
                "boundary-unstarted",
                false,
                RecoveryPolicy::Pure,
            )]],
            Map::new(),
            Map::new(),
        );
        let original = ordinary(&store);
        let journal = store.journal(&execution_id()).unwrap();
        boundary_pad_admit_journal(&directory, 4_096 + extra);
        assert_eq!(ordinary(&store), original);
        assert_eq!(store.journal(&execution_id()).unwrap(), journal);
        let store = advance(&directory, store);
        if extra == 0 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}

#[test]
fn sqlite_real_pending_grants_can_cross_8mib_aggregate_while_each_row_and_count_fits() {
    for count in [63, 64] {
        let directory = Directory::new();
        let ids: Vec<_> = (0..count)
            .map(|i| format!("boundary-pending-{i:02}"))
            .collect();
        let store = seed(
            &directory,
            vec![ids
                .iter()
                .map(|id| spec(id, true, RecoveryPolicy::Unsafe))
                .collect()],
            Map::new(),
            Map::new(),
        );
        commit(&store, KernelCommand::Start);
        for id in &ids {
            park(&store, id);
        }
        let original = authority(&store);
        assert_eq!(original.grants.len(), count);
        assert!(original.acceptances.is_empty());
        assert_eq!(
            original.execution.snapshot.status,
            ExecutionStatus::WaitingForApproval
        );
        for grant in &original.grants {
            boundary_pad_grant(&directory, &grant.interrupt_id, BOUNDARY_AUTHORITY_BYTES);
        }
        assert_eq!(authority(&store), original);
        let rows = directory.rows();
        boundary_assert_individual_payload_caps(&rows);
        let authority_payload_bytes = boundary_raw_text_bytes(&rows, "approval_grants", 7);
        assert_eq!(authority_payload_bytes, count * BOUNDARY_AUTHORITY_BYTES);
        if count == 63 {
            // Count every stored scalar generously, including SQL bookkeeping
            // integers, so the below-budget proof does not need to infer which
            // fixed scalar bookkeeping the selected-input ledger omits.
            assert!(boundary_all_table_byte_upper_bound(&rows) < BOUNDARY_AGGREGATE_BYTES);
        } else {
            // The authority payloads alone equal 8MiB. The actual snapshot and
            // required owning-thread/index bytes make the full pass one-over.
            assert_eq!(authority_payload_bytes, BOUNDARY_AGGREGATE_BYTES);
            assert!(!snapshot_raw(&directory).is_empty());
        }
        let store = advance(&directory, store);
        if count == 63 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::RequiredAuthorityUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}

#[test]
#[ignore = "slow real SQLite owner succession; run separately"]
fn sqlite_individually_bounded_metadata_and_real_journal_cross_aggregate_nodes() {
    for metadata_nodes in [10_000, 16_384] {
        let directory = Directory::new();
        let metadata = Map::from_iter([(
            "boundary_nodes".into(),
            Value::Array(vec![Value::Null; metadata_nodes - 3]),
        )]);
        assert_eq!(
            boundary_json_nodes(&Value::Object(metadata.clone())),
            metadata_nodes
        );
        let store = boundary_journal_history(&directory, 4_095, metadata);
        let rows = directory.rows();
        boundary_assert_individual_payload_caps(&rows);
        assert!(boundary_all_table_byte_upper_bound(&rows) < BOUNDARY_AGGREGATE_BYTES);
        let (mandatory_payload_nodes, including_status_index_nodes) =
            boundary_json_node_interval(&rows);
        if metadata_nodes == 10_000 {
            assert!(including_status_index_nodes <= BOUNDARY_AGGREGATE_NODES);
        } else {
            assert!(mandatory_payload_nodes > BOUNDARY_AGGREGATE_NODES);
        }
        let store = advance(&directory, store);
        if metadata_nodes == 10_000 {
            assert_confined(
                &directory,
                store,
                RecoveryConfinementReason::ExactDefinitionUnavailable,
            );
        } else {
            assert_refused(
                &directory,
                store,
                RecoveryConfinementBackendError::LimitExceeded,
            );
        }
    }
}
