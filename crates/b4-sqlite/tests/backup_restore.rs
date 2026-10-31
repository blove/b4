#![cfg(unix)]

#[path = "backup_support/mod.rs"]
mod support;
use b4_contract::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::Connection;
use std::sync::{atomic::Ordering, Arc};
use support::*;

#[test]
fn controlled_parked_restore_preserves_exact_cut_and_continues_without_sibling_replay() {
    let directory = Directory::new();
    let source = directory.child("source");
    let restored = directory.child("restored");
    let archive = directory.0.join("final.sqlite3");
    let fixture = parked(&source, RecoveryPolicy::Pure);
    let captured = view(fixture.store.as_ref());
    let approvals = service(fixture.store.clone(), fixture.definition.clone());
    let original_pending = pending(&approvals);
    let original_authority = authority_rows(&rows(&source.join("runtime.sqlite3")));
    drop(approvals);
    let (cut, calls, available) = export_and_retire(fixture, &source, &archive);
    let store = open_restored(&archive, &restored, &cut);
    assert!(
        view(store.as_ref()) == captured,
        "Copied thread generation, origin, resource fence and snapshot must survive open"
    );
    let approvals = service(store.clone(), available);
    assert!(
        must(approvals.attachment_pending(&context(), &captured)) == original_pending,
        "Prior-owner disclosure must preserve exact token, prompt, resume key and TTL"
    );
    let before_recovery = rows(&restored.join("runtime.sqlite3"));
    assert!(
        matches!(
            approvals.pending(&context(), &execution(), fence(&captured)),
            Err(ApprovalError::Store(StoreError::OwnerConflict))
        ),
        "Ordinary pending must retain its current-owner requirement after restoration"
    );
    assert!(
        matches!(
            approvals.accept(
                &execution(),
                fence(&captured),
                acceptance(&captured, &original_pending, 105)
            ),
            Err(ApprovalError::Store(StoreError::OwnerConflict))
        ),
        "Prior-owner grant cannot be accepted without exact recovery"
    );
    assert!(
        rows(&restored.join("runtime.sqlite3")) == before_recovery,
        "Prior-owner refusals and disclosure must preserve original records"
    );
    let recovered = recover(&approvals, 106);
    assert_eq!(
        recovered.snapshot.status,
        ExecutionStatus::WaitingForApproval
    );
    assert!(
        pending(&approvals) == original_pending,
        "Recovery must preserve original pending authority"
    );
    assert!(
        authority_rows(&rows(&restored.join("runtime.sqlite3"))) == original_authority,
        "Recovery must not rewrite authority"
    );
    accept(&approvals, 107);
    let completed = complete(&approvals, &calls, 108);
    assert_eq!(completed.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(completed.snapshot.definition.max_attempts, 4);
    assert_eq!(completed.snapshot.attempts_used, 2);
    assert!(
        completed.snapshot.attempt_history
            == vec![attempt("sibling-attempt"), attempt("gated-attempt")],
        "Original ordered attempt history must be preserved"
    );
    assert_eq!(calls.sibling.load(Ordering::SeqCst), 1);
    assert_eq!(calls.gated.load(Ordering::SeqCst), 1);
    assert_eq!(
        completed.snapshot.values["joined"],
        serde_json::json!(["sibling", "gated"])
    );
    assert_journal_extension(&cut, &rows(&restored.join("runtime.sqlite3")), 4);
}

#[test]
fn restored_expired_park_retains_original_authority_and_refuses_acceptance_atomically() {
    let directory = Directory::new();
    let source = directory.child("source");
    let restored = directory.child("restored");
    let archive = directory.0.join("final.sqlite3");
    let fixture = parked(&source, RecoveryPolicy::Pure);
    let approvals = service(fixture.store.clone(), fixture.definition.clone());
    let original_pending = pending(&approvals);
    assert_eq!(original_pending[0].expires_at_ms, 204);
    drop(approvals);
    let (cut, calls, available) = export_and_retire(fixture, &source, &archive);
    let store = open_restored(&archive, &restored, &cut);
    let approvals = service(store, available);
    let captured = view(approvals.store().as_ref());
    assert!(
        must(approvals.attachment_pending(&context(), &captured)) == original_pending,
        "Expired prior-owner pending must retain token and original TTL"
    );
    recover(&approvals, 1_000);
    assert!(
        pending(&approvals) == original_pending,
        "Recovery must not renew original expired authority"
    );
    let before = rows(&restored.join("runtime.sqlite3"));
    let captured = view(approvals.store().as_ref());
    let result = approvals.accept(
        &execution(),
        fence(&captured),
        acceptance(&captured, &original_pending, 1_001),
    );
    assert!(
        matches!(result, Err(ApprovalError::Expired)),
        "Expired restored grant must refuse as expired"
    );
    assert!(
        rows(&restored.join("runtime.sqlite3")) == before,
        "Expired acceptance must not mutate any logical row"
    );
    assert_eq!(calls.sibling.load(Ordering::SeqCst), 1);
    assert_eq!(calls.gated.load(Ordering::SeqCst), 0);
    assert_eq!(
        view(approvals.store().as_ref())
            .execution
            .unwrap()
            .snapshot
            .attempts_used,
        1
    );
    assert_journal_extension(&cut, &rows(&restored.join("runtime.sqlite3")), 1);
}

#[test]
fn restored_accepted_once_completes_after_expiry_without_new_audit_or_sibling_replay() {
    let directory = Directory::new();
    let source = directory.child("source");
    let restored = directory.child("restored");
    let archive = directory.0.join("final.sqlite3");
    let fixture = parked(&source, RecoveryPolicy::Pure);
    let approvals = service(fixture.store.clone(), fixture.definition.clone());
    let accepted = accept(&approvals, 105);
    let authority = must(approvals.store().approval_view(&execution())).unwrap();
    assert_eq!(accepted.snapshot.status, ExecutionStatus::Running);
    assert_eq!(accepted.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(accepted.snapshot.tasks[1].status, TaskStatus::Ready);
    assert_eq!(accepted.snapshot.attempts_used, 1);
    assert!(authority.grants[0].consumed);
    assert_eq!(authority.acceptances.len(), 1);
    assert!(authority.grants[0].expires_at_ms < 1_000);
    drop(approvals);
    let (cut, calls, available) = export_and_retire(fixture, &source, &archive);
    let store = open_restored(&archive, &restored, &cut);
    let approvals = service(store.clone(), available);
    assert!(
        must(store.execution(&execution())) == Some(accepted.clone()),
        "Accepted snapshot must survive restored owner advance"
    );
    let recovered = recover(&approvals, 1_000);
    assert_eq!(recovered.snapshot.status, ExecutionStatus::Running);
    assert_eq!(recovered.snapshot.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(recovered.snapshot.tasks[1].status, TaskStatus::Ready);
    assert!(
        recovered.snapshot.attempt_history == accepted.snapshot.attempt_history,
        "Recovery cannot erase prior attempts"
    );
    assert!(
        authority_rows(&rows(&restored.join("runtime.sqlite3"))) == authority_rows(&cut),
        "Accepted continuation recovery cannot rewrite consumed grant or audit"
    );
    let completed = complete(&approvals, &calls, 1_001);
    assert_eq!(completed.snapshot.status, ExecutionStatus::Completed);
    assert_eq!(completed.snapshot.definition.max_attempts, 4);
    assert!(
        completed.snapshot.definition == accepted.snapshot.definition,
        "The full registered definition must remain immutable"
    );
    assert_eq!(completed.snapshot.attempts_used, 2);
    assert!(
        completed.snapshot.attempt_history
            == vec![attempt("sibling-attempt"), attempt("gated-attempt")],
        "Completion must preserve ordered original attempt history"
    );
    assert!(
        completed.snapshot.tasks[0] == accepted.snapshot.tasks[0],
        "Committed sibling task/result/input must remain exact"
    );
    assert_eq!(calls.sibling.load(Ordering::SeqCst), 1);
    assert_eq!(calls.gated.load(Ordering::SeqCst), 1);
    assert!(
        authority_rows(&rows(&restored.join("runtime.sqlite3"))) == authority_rows(&cut),
        "Once completion must retain original consumed record and exactly one audit"
    );
    assert_journal_extension(&cut, &rows(&restored.join("runtime.sqlite3")), 3);
    let before = rows(&restored.join("runtime.sqlite3"));
    let repeat = approvals.dispatch_outcome(
        &context(),
        &execution(),
        fence(&view(store.as_ref())),
        task("gated"),
        attempt("forbidden-replay"),
        facts(1_003),
        || facts(1_004),
        |_| {
            calls.gated.fetch_add(1, Ordering::SeqCst);
            Ok(EffectOutcome::Failed("forbidden replay".into()))
        },
    );
    assert!(
        repeat.is_err(),
        "Completed accepted effect must refuse another invocation"
    );
    assert_eq!(calls.gated.load(Ordering::SeqCst), 1);
    assert!(
        rows(&restored.join("runtime.sqlite3")) == before,
        "Refused replay must preserve all logical rows"
    );
    drop(approvals);
    drop(store);
    let reopened = must(SqliteStore::open(&restored));
    assert_eq!(reopened.owner_generation(), 3);
    assert!(
        must(reopened.execution(&execution())) == Some(completed),
        "Completed restored continuation must survive its own reopen"
    );
    assert!(
        authority_rows(&rows(&restored.join("runtime.sqlite3"))) == authority_rows(&cut),
        "Final reopen must preserve accepted authority history"
    );
}

#[test]
fn restored_recovery_rejects_changed_definition_identity_and_fences_without_disclosure_or_mutation()
{
    let directory = Directory::new();
    let source = directory.child("source");
    let restored = directory.child("restored");
    let archive = directory.0.join("final.sqlite3");
    let fixture = parked(&source, RecoveryPolicy::Pure);
    let (cut, calls, available) = export_and_retire(fixture, &source, &archive);
    let store = open_restored(&archive, &restored, &cut);
    let approvals = service(store.clone(), available.clone());
    let captured = view(store.as_ref());
    let request = recovery(&captured, available.clone(), 1_000);
    let captured_fence = fence(&captured);
    let before = rows(&restored.join("runtime.sqlite3"));
    let mut variants = Vec::new();
    let mut changed = available.clone();
    changed.route = must(RouteKey::new("/changed#workflow"));
    variants.push(changed);
    let mut changed = available.clone();
    changed.revision = must(ExecutionRevision::new("definition-2"));
    variants.push(changed);
    let mut changed = available.clone();
    changed.batches[0][1].effect_id = must(EffectId::new("changed-effect"));
    variants.push(changed);
    let mut changed = available.clone();
    changed.batches[0][1].id = task("changed-task");
    variants.push(changed);
    let mut changed = available.clone();
    changed.batches[0][1].requires_approval = false;
    variants.push(changed);
    let mut changed = available.clone();
    changed.batches[0][1].recovery = RecoveryPolicy::Unsafe;
    variants.push(changed);
    let mut changed = available.clone();
    changed.max_attempts += 1;
    variants.push(changed);
    let mut changed = available.clone();
    changed.batches[0].reverse();
    variants.push(changed);
    for changed in variants {
        let result = approvals.recover(
            &execution(),
            captured_fence,
            ApprovalRecoveryRequest {
                available_definition: changed,
                ..request.clone()
            },
        );
        assert!(
            matches!(result, Err(ApprovalError::RecoveryDefinitionMismatch)),
            "Every complete definition difference must refuse recovery"
        );
        assert!(
            rows(&restored.join("runtime.sqlite3")) == before,
            "Definition mismatch must preserve exact restored records"
        );
    }
    let mut changed = request.clone();
    changed.thread_id = must(ThreadId::new("missing-thread"));
    assert!(matches!(
        approvals.recover(&execution(), captured_fence, changed),
        Err(ApprovalError::Unauthorized)
    ));
    let mut changed = request.clone();
    changed.execution_id = must(ExecutionId::new("missing-execution"));
    assert!(matches!(
        approvals.recover(&execution(), captured_fence, changed),
        Err(ApprovalError::Store(StoreError::ExecutionNotActive))
    ));
    let mut changed = request.clone();
    let replacement = if captured.thread.generation.as_str() == "0".repeat(64) {
        "1"
    } else {
        "0"
    };
    changed.thread_generation = must(ThreadGeneration::new(replacement.repeat(64)));
    assert!(matches!(
        approvals.recover(&execution(), captured_fence, changed),
        Err(ApprovalError::Store(StoreError::RevisionConflict))
    ));
    for field in ["application", "principal", "tenant"] {
        let mut changed = request.clone();
        match field {
            "application" => changed.context.actor.application = "other-app".into(),
            "principal" => changed.context.actor.principal = "bob".into(),
            "tenant" => changed.context.actor.tenant = "other-tenant".into(),
            _ => unreachable!(),
        }
        assert!(
            matches!(
                approvals.recover(&execution(), captured_fence, changed),
                Err(ApprovalError::Unauthorized)
            ),
            "Current exact origin must authorize restoration"
        );
    }
    for (changed_fence, expected_owner) in [
        (
            ApprovalFence {
                execution: ExecutionFence {
                    owner_generation: 2,
                    ..captured_fence.execution
                },
                ..captured_fence
            },
            true,
        ),
        (
            ApprovalFence {
                execution: ExecutionFence {
                    revision: captured_fence.execution.revision - 1,
                    ..captured_fence.execution
                },
                ..captured_fence
            },
            false,
        ),
        (
            ApprovalFence {
                resource_revision: captured_fence.resource_revision - 1,
                ..captured_fence
            },
            false,
        ),
    ] {
        let result = approvals.recover(&execution(), changed_fence, request.clone());
        if expected_owner {
            assert!(matches!(
                result,
                Err(ApprovalError::Store(StoreError::OwnerConflict))
            ));
        } else {
            assert!(matches!(
                result,
                Err(ApprovalError::Store(StoreError::RevisionConflict))
            ));
        }
    }
    assert!(
        rows(&restored.join("runtime.sqlite3")) == before,
        "All target/authority/fence refusals must preserve exact restored records"
    );
    assert_eq!(calls.sibling.load(Ordering::SeqCst), 1);
    assert_eq!(calls.gated.load(Ordering::SeqCst), 0);
    let wrong_registry = service(store.clone(), {
        let mut missing = available;
        missing.revision = must(ExecutionRevision::new("unavailable-definition"));
        missing
    });
    assert!(
        matches!(
            wrong_registry.recover(&execution(), captured_fence, request),
            Err(ApprovalError::Unauthorized)
        ),
        "Unavailable current registered definition must refuse before continuation"
    );
    assert!(rows(&restored.join("runtime.sqlite3")) == before);
}

#[test]
fn restored_unsafe_inflight_remains_unknown_and_exact_recovery_never_replays_callback() {
    let directory = Directory::new();
    let source = directory.child("source");
    let restored = directory.child("restored");
    let archive = directory.0.join("final.sqlite3");
    let fixture = parked(&source, RecoveryPolicy::Unsafe);
    let approvals = service(fixture.store.clone(), fixture.definition.clone());
    accept(&approvals, 105);
    let result = approvals.dispatch_outcome(
        &context(),
        &execution(),
        fence(&view(fixture.store.as_ref())),
        task("gated"),
        attempt("unsafe-attempt"),
        facts(106),
        || facts(107),
        |_| {
            fixture.calls.gated.fetch_add(1, Ordering::SeqCst);
            Err("fixture callback returned uncertainty".into())
        },
    );
    assert!(
        matches!(
            result,
            Err(ApprovalError::Runtime(RuntimeError::EffectFailed(_)))
        ),
        "Outer callback error must preserve uncertain attempt"
    );
    let uncertain = view(fixture.store.as_ref()).execution.unwrap();
    assert_eq!(uncertain.snapshot.tasks[1].status, TaskStatus::InFlight);
    assert_eq!(uncertain.snapshot.attempts_used, 2);
    assert!(uncertain.snapshot.tasks[1].result.is_none());
    drop(approvals);
    let (cut, calls, available) = export_and_retire(fixture, &source, &archive);
    let store = open_restored(&archive, &restored, &cut);
    let approvals = service(store.clone(), available);
    let blocked = recover(&approvals, 1_000);
    assert_eq!(blocked.snapshot.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(blocked.snapshot.tasks[1].status, TaskStatus::OutcomeUnknown);
    assert!(blocked.snapshot.tasks[1].result.is_none());
    assert_eq!(blocked.snapshot.attempts_used, 2);
    assert!(blocked.snapshot.attempt_history == uncertain.snapshot.attempt_history);
    assert!(
        blocked.snapshot.tasks[0] == uncertain.snapshot.tasks[0],
        "Recovery cannot repeat or alter committed sibling"
    );
    assert!(
        authority_rows(&rows(&restored.join("runtime.sqlite3"))) == authority_rows(&cut),
        "Unsafe recovery must retain consumed authority and audit"
    );
    let before = rows(&restored.join("runtime.sqlite3"));
    let result = approvals.dispatch_outcome(
        &context(),
        &execution(),
        fence(&view(store.as_ref())),
        task("gated"),
        attempt("forbidden-replay"),
        facts(1_001),
        || facts(1_002),
        |_| {
            calls.gated.fetch_add(1, Ordering::SeqCst);
            Ok(EffectOutcome::Succeeded(TaskResult {
                output: serde_json::json!("fabricated"),
                writes: vec![],
            }))
        },
    );
    assert!(
        result.is_err(),
        "Unsafe unknown outcome must not redispatch"
    );
    assert_eq!(calls.sibling.load(Ordering::SeqCst), 1);
    assert_eq!(calls.gated.load(Ordering::SeqCst), 1);
    assert!(
        rows(&restored.join("runtime.sqlite3")) == before,
        "Refused unsafe replay must not fabricate success or mutate authority"
    );
    assert_journal_extension(&cut, &rows(&restored.join("runtime.sqlite3")), 1);
}

#[test]
fn unsupported_installed_schema_refuses_open_without_rewriting_copied_records() {
    for corruption in ["schema_version", "checksum", "extra_object"] {
        let directory = Directory::new();
        let source = directory.child("source");
        let restored = directory.child("restored");
        let archive = directory.0.join("final.sqlite3");
        let fixture = parked(&source, RecoveryPolicy::Pure);
        let (_, calls, _) = export_and_retire(fixture, &source, &archive);
        install(&archive, &restored);
        let db = must(Connection::open(restored.join("runtime.sqlite3")));
        match corruption {
            "schema_version" => {
                must(db.execute("UPDATE metadata SET schema_version=999", []));
            }
            "checksum" => {
                must(db.execute("UPDATE metadata SET migration_checksum='unsupported'", []));
            }
            "extra_object" => {
                must(db.execute(
                    "CREATE TABLE unsupported_extra (id INTEGER PRIMARY KEY)",
                    [],
                ));
            }
            _ => unreachable!(),
        }
        drop(db);
        let before = rows(&restored.join("runtime.sqlite3"));
        assert!(
            matches!(
                SqliteStore::open(&restored),
                Err(StoreError::UnsupportedSchema)
            ),
            "Unsupported restored structure must refuse before owner advancement"
        );
        assert!(
            rows(&restored.join("runtime.sqlite3")) == before,
            "Unsupported open must roll back all metadata and copied records"
        );
        assert!(
            !owner_locked(&restored),
            "Failed restored open must release actual owner lock"
        );
        assert_eq!(calls.gated.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn malformed_installed_payload_refuses_existing_load_or_recovery_without_callbacks() {
    for corruption in ["snapshot", "grant"] {
        let directory = Directory::new();
        let source = directory.child("source");
        let restored = directory.child("restored");
        let archive = directory.0.join("final.sqlite3");
        let fixture = parked(&source, RecoveryPolicy::Pure);
        let captured = view(fixture.store.as_ref());
        let (cut, calls, available) = export_and_retire(fixture, &source, &archive);
        install(&archive, &restored);
        let db = must(Connection::open(restored.join("runtime.sqlite3")));
        match corruption {
            "snapshot" => {
                must(db.execute("UPDATE executions SET snapshot='{}'", []));
            }
            "grant" => {
                must(db.execute("UPDATE approval_grants SET record='{}'", []));
            }
            _ => unreachable!(),
        }
        drop(db);
        let store = Arc::new(must(SqliteStore::open(&restored)));
        assert_eq!(store.owner_generation(), 2);
        let before = rows(&restored.join("runtime.sqlite3"));
        let approvals = service(store.clone(), available.clone());
        if corruption == "snapshot" {
            assert!(
                matches!(store.execution(&execution()), Err(StoreError::Corrupt(_))),
                "Malformed snapshot must fail existing ordinary load"
            );
        }
        let result = approvals.recover(
            &execution(),
            fence(&captured),
            recovery(&captured, available, 1_000),
        );
        assert!(
            matches!(result, Err(ApprovalError::Store(StoreError::Corrupt(_)))),
            "Malformed restored payload must refuse before any callback or adoption"
        );
        assert!(
            rows(&restored.join("runtime.sqlite3")) == before,
            "Malformed continuation refusal must retain copied authority and journal"
        );
        assert!(
            rows(&archive) == cut,
            "Installed-copy corruption cannot mutate the finalized archive"
        );
        assert_eq!(calls.sibling.load(Ordering::SeqCst), 1);
        assert_eq!(calls.gated.load(Ordering::SeqCst), 0);
    }
}
