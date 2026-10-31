//! Genuine original Always acceptance with application UUID resource IDs.
use super::*;

#[derive(PartialEq)]
struct OriginalCapture {
    view: ThreadView,
    execution: StoredExecution,
    journal: Vec<JournalEntry>,
    raw: ProducedRawNine,
}
impl OriginalCapture {
    fn read(
        store: &SqliteStore,
        path: &std::path::Path,
        thread: &ThreadId,
        execution: &ExecutionId,
    ) -> Self {
        Self {
            view: store.thread_view(thread).unwrap().unwrap(),
            execution: store.execution(execution).unwrap().unwrap(),
            journal: store.journal(execution).unwrap(),
            raw: driver_raw_nine(path),
        }
    }
}

#[test]
fn permission_original_always_uuid_resource_ids_persist_ready() {
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("f8e8bb7b-f8ac-4f29-a38f-3e0dfdb69b84").unwrap();
    let execution = ExecutionId::new("653c8f87-d983-4262-ad6e-17a08938a45f").unwrap();
    let task = TaskId::new("release").unwrap();
    let effect = EffectId::new("release-effect").unwrap();
    let interrupt = InterruptId::new("release-interrupt").unwrap();
    let context = ApprovalContext {
        actor: ThreadOrigin {
            application: "permission-app".into(),
            tenant: "tenant".into(),
            principal: "alice".into(),
        },
        policy_revision: "policy-1".into(),
    };
    let definition = Definition {
        route: RouteKey::new("/permission-first#agent").unwrap(),
        revision: ExecutionRevision::new("definition-1").unwrap(),
        batches: vec![vec![TaskSpec {
            id: task.clone(),
            effect_id: effect.clone(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: true,
        }]],
        max_attempts: 3,
    };
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: "application-1".into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    effect.clone(),
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
    let catalog = Arc::new(
        PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: effect,
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: Arc::new(AllowConstraint),
            }],
        )
        .unwrap(),
    );
    let registered = application.definition(&definition.route).unwrap().clone();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let original_owner = store.owner_generation();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 100,
            metadata: Map::new(),
            origin: Some(context.actor.clone()),
        })
        .unwrap();
    let initial = new_execution(
        execution.clone(),
        TurnId::new("446916ea-643b-4d47-8aee-cc0e995dc769").unwrap(),
        original_owner,
        registered,
        Map::from_iter([("input".into(), json!({"release": "fixture"}))]),
        facts(100),
    )
    .unwrap();
    let admitted = store.admit(&thread, initial).unwrap();
    store
        .commit(
            &execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
        )
        .unwrap();
    let authority = Arc::new(AllowAuthority);
    let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
    let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
    service
        .park(
            &context,
            &execution,
            fence(store.as_ref(), &thread),
            task.clone(),
            interrupt.clone(),
            prompt,
            1_000,
            facts(102),
        )
        .unwrap();
    let pending = service
        .pending(&context, &execution, fence(store.as_ref(), &thread))
        .unwrap();
    // The service genuinely minted this token; no setup alias survives the call.
    drop(service);
    let before = OriginalCapture::read(store.as_ref(), &directory.0, &thread, &execution);
    let grant: GrantRecord =
        serde_json::from_str(produced_text(&before.raw.tables[4][0][7])).unwrap();
    let approved_turn = TurnId::new("a7ae0611-7e54-4ccb-ab14-2d095f33908b").unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: approved_turn.clone(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: pending[0].interrupt_id.clone(),
            decision: ApprovalDecision::Always,
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };
    let expected = StoredExecution {
        thread_id: thread.clone(),
        snapshot: transition(
            &before.execution.snapshot,
            KernelCommand::Resume {
                route: definition.route.clone(),
                decisions: vec![Decision {
                    interrupt_id: pending[0].interrupt_id.clone(),
                    decision: ApprovalDecision::Always,
                }],
                turn_id: approved_turn.clone(),
            },
            facts(103),
        )
        .unwrap()
        .snapshot,
    };
    let waiting2 = before.execution.snapshot.revision == 2
        && before.execution.snapshot.status == ExecutionStatus::WaitingForApproval
        && before.execution.snapshot.turn_id.as_str() == "446916ea-643b-4d47-8aee-cc0e995dc769"
        && before.execution.snapshot.owner_generation == original_owner
        && before.execution.snapshot.tasks[0].status == TaskStatus::WaitingForApproval
        && before.view.execution.as_ref() == Some(&before.execution)
        && before.journal.len() == 3
        && before.raw.tables.iter().map(Vec::len).collect::<Vec<_>>()
            == vec![1, 1, 1, 4, 1, 0, 0, 0, 0]
        && pending.len() == 1
        && pending[0].task_id == task
        && pending[0].interrupt_id == grant.interrupt_id
        && pending[0].token == grant.token
        && pending[0].prompt == grant.prompt
        && pending[0].resume_key == grant.resume_key
        && pending[0].expires_at_ms == grant.expires_at_ms
        && grant.execution_id == execution
        && grant.thread_id == thread
        && grant.task_id == task
        && grant.thread_generation == before.view.thread.generation
        && grant.task_parking_revision == 2
        && !grant.consumed
        && calls.load(Ordering::SeqCst) == 0;
    let actual = accept_permission_always_with_normal_inputs(
        store.clone(),
        authority.clone(),
        catalog.clone(),
        &execution,
        fence(store.as_ref(), &thread),
        request,
    );
    let after = OriginalCapture::read(store.as_ref(), &directory.0, &thread, &execution);
    let error_code = actual
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let prewrite_refusal = actual
        .as_ref()
        .err()
        .is_some_and(|error| error.code() == PermissionError::UnsupportedProfile.code())
        && before == after
        && !grant.consumed;
    let mut expected_thread = before.view.thread.clone();
    expected_thread.resource_revision += 1;
    let expected_ready3 = expected.snapshot.revision == 3
        && expected.snapshot.status == ExecutionStatus::Running
        && expected.snapshot.turn_id == approved_turn
        && expected.snapshot.tasks[0].status == TaskStatus::Ready
        && expected.snapshot.tasks[0].approved
        && expected.snapshot.attempts_used == 0
        && expected.snapshot.attempt_history.is_empty();
    let storage_changed = before != after;
    let stored_revision = after.execution.snapshot.revision;
    let persisted_success = after.raw.tables[0] == before.raw.tables[0]
        && actual.as_ref().is_ok_and(|stored| stored == &expected)
        && after.execution == expected
        && after.view
            == (ThreadView {
                thread: expected_thread,
                execution: Some(expected.clone()),
            })
        && after.journal.len() == before.journal.len() + 1
        && after.journal[..before.journal.len()] == before.journal
        && after.journal.last() == Some(&JournalEntry::from(&expected.snapshot))
        && original_records_match(
            &before,
            &after,
            &grant,
            &context,
            &definition,
            &approved_turn,
        );
    let callbacks_zero = calls.load(Ordering::SeqCst) == 0;

    // Retire the actual result and every producer/application/catalog alias before
    // proving owner1 is sole-owned and reading the complete storage as owner2.
    drop(actual);
    drop(pending);
    drop(grant);
    drop(expected);
    drop(authority);
    let catalog_weak = Arc::downgrade(&catalog);
    let application_weak = Arc::downgrade(&application);
    drop(catalog);
    drop(application);
    let anchors_retired = catalog_weak.upgrade().is_none()
        && application_weak.upgrade().is_none()
        && Arc::strong_count(&calls) == 1;
    let sole_owner = Arc::strong_count(&store) == 1;
    let owner1_ready = store.readiness().is_ok();
    drop(store);
    let owner2 = SqliteStore::open(&directory.0).unwrap();
    let reopened = OriginalCapture::read(&owner2, &directory.0, &thread, &execution);
    let reopened_same = owner2.owner_generation() == original_owner + 1
        && reopened.view == after.view
        && reopened.execution == after.execution
        && reopened.journal == after.journal
        && reopened.raw.tables[1..] == after.raw.tables[1..]
        && reopened.raw.tables[0][0][..3] == after.raw.tables[0][0][..3]
        && produced_u64(&reopened.raw.tables[0][0][3]) == original_owner + 1;
    // Decode the reopened persisted records independently, using only captured
    // plain facts. These observations own no service, native or Store alias.
    let reopened_records = if persisted_success {
        let original_grant: GrantRecord =
            serde_json::from_str(produced_text(&before.raw.tables[4][0][7])).unwrap();
        original_records_match(
            &before,
            &reopened,
            &original_grant,
            &context,
            &definition,
            &approved_turn,
        )
    } else {
        true
    };
    let owner2_ready = owner2.readiness().is_ok();
    drop(reopened);
    drop(owner2);
    let path = directory.0.clone();
    drop(directory);
    let retired = anchors_retired
        && sole_owner
        && owner1_ready
        && owner2_ready
        && reopened_same
        && reopened_records
        && !path.exists();
    println!("original-always-uuid: waiting2={waiting2} expected_ready3={expected_ready3} error={error_code} storage_changed={storage_changed} stored_revision={stored_revision} prewrite_refusal={prewrite_refusal} persisted_success={persisted_success} callbacks_zero={callbacks_zero} retired={retired}");
    assert!(
        waiting2 && expected_ready3 && callbacks_zero && retired,
        "genuine Waiting2 setup and full owned persistence/retirement must be healthy"
    );
    assert!(prewrite_refusal || persisted_success, "actual producer path must be exact success or observed Unsupported with complete unchanged storage; error={error_code}");
    assert!(
        persisted_success,
        "original UUID Always must persist exact kernel Resume Ready3; error={error_code}"
    );
}

fn original_records_match(
    before: &OriginalCapture,
    after: &OriginalCapture,
    grant: &GrantRecord,
    context: &ApprovalContext,
    definition: &Definition,
    turn: &TurnId,
) -> bool {
    let raw = &after.raw.tables;
    if raw.iter().map(Vec::len).collect::<Vec<_>>() != vec![1, 1, 1, 5, 1, 0, 1, 1, 0] {
        return false;
    }
    let Ok(consumed) = serde_json::from_str::<GrantRecord>(produced_text(&raw[4][0][7])) else {
        return false;
    };
    let mut expected_grant = grant.clone();
    expected_grant.consumed = true;
    let Ok(acceptance) = PermissionAcceptance::decode_record(
        produced_text(&raw[6][0][7]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    ) else {
        return false;
    };
    let Ok(decision) = PermissionDecision::decode_record(
        produced_text(&raw[7][0][5]).as_bytes(),
        &mut PermissionEvidenceBudget::new(),
    ) else {
        return false;
    };
    if acceptance.decisions().len() != 1 {
        return false;
    }
    let accepted = &acceptance.decisions()[0];
    let Some(binding) = accepted.binding() else {
        return false;
    };
    consumed == expected_grant
        && raw[4][0][..6] == before.raw.tables[4][0][..6]
        && raw[4][0][6] == rusqlite::types::Value::Integer(1)
        && raw[3][..before.raw.tables[3].len()] == before.raw.tables[3]
        && original_journal_matches(after)
        && acceptance.before_snapshot() == &before.execution.snapshot
        && acceptance.thread_id() == &grant.thread_id
        && acceptance.thread_generation() == &grant.thread_generation
        && acceptance.turn_id() == turn
        && acceptance.facts() == &facts(103)
        && acceptance.committed_revision() == 3
        && acceptance.origin() == &context.actor
        && acceptance.policy_revision() == context.policy_revision
        && acceptance.application().application() == context.actor.application
        && acceptance.application().revision() == "application-1"
        && accepted.decision() == ApprovalDecision::Always
        && accepted.task_id() == &grant.task_id
        && accepted.interrupt_id() == &grant.interrupt_id
        && accepted.grant_ref().execution_id() == &grant.execution_id
        && accepted.grant_ref().interrupt_id() == &grant.interrupt_id
        && accepted.proposal_digest() == grant.proposal_digest
        && accepted.task_parking_revision() == grant.task_parking_revision
        && accepted.permission_id() == Some(decision.permission_id())
        && binding.effect_id() == &grant.effect_id
        && binding.tool_id().as_str() == "release-tool"
        && binding.tool_revision().as_str() == "tool-1"
        && binding.constraint_revision().as_str() == "constraint-1"
        && decision.creator_execution_id() == &grant.execution_id
        && decision.creator_turn_id() == turn
        && decision.creator_task_id() == &grant.task_id
        && decision.creator_interrupt_id() == &grant.interrupt_id
        && decision.scope().application() == acceptance.application()
        && decision.scope().definition() == definition
        && decision.scope().origin() == &grant.origin
        && decision.scope().thread_id() == &grant.thread_id
        && decision.scope().thread_generation() == &grant.thread_generation
        && decision.scope().route() == &definition.route
        && decision.scope().effect_id() == &grant.effect_id
        && decision.scope().tool_id() == binding.tool_id()
        && decision.scope().tool_revision() == binding.tool_revision()
        && decision.scope().constraint_revision() == binding.constraint_revision()
        && decision.scope().policy_revision() == grant.policy_revision
        && produced_text(&raw[6][0][0]) == grant.execution_id.as_str()
        && produced_text(&raw[6][0][1]) == turn.as_str()
        && produced_text(&raw[6][0][2]) == grant.thread_id.as_str()
        && produced_text(&raw[6][0][3]) == grant.thread_generation.as_str()
        && produced_u64(&raw[6][0][4]) == 2
        && produced_u64(&raw[6][0][5]) == 3
        && raw[6][0][6] == rusqlite::types::Value::Integer(i64::from(acceptance.record_version()))
        && produced_text(&raw[7][0][0]) == decision.permission_id().as_str()
        && produced_text(&raw[7][0][1]) == grant.thread_id.as_str()
        && produced_text(&raw[7][0][2]) == grant.thread_generation.as_str()
        && produced_text(&raw[7][0][3]) == decision.scope_digest()
        && raw[7][0][4] == rusqlite::types::Value::Integer(i64::from(decision.record_version()))
}

// Mirror both the genuine Park and final Resume in the raw journal, including
// their complete payloads, rather than checking only the row count.
fn original_journal_matches(capture: &OriginalCapture) -> bool {
    capture.journal[2..]
        .iter()
        .zip(&capture.raw.tables[3][3..])
        .all(|(entry, row)| {
            produced_text(&row[1]) == "commit"
                && produced_text(&row[2]) == capture.view.thread.id.as_str()
                && produced_text(&row[3]) == entry.execution_id.as_str()
                && produced_u64(&row[4]) == entry.revision
                && produced_u64(&row[5]) == entry.owner_generation
                && produced_text(&row[6]) == serde_json::to_string(&entry.status).unwrap()
                && produced_u64(&row[7]) == entry.updated_at_ms
                && serde_json::from_str::<JournalEntry>(produced_text(&row[8]))
                    .ok()
                    .as_ref()
                    == Some(entry)
        })
}

// The two refusal controls share the same genuine setup as the primary case.
// The primary success test remains literal so its existing oracles stay visible.
struct OriginalWaitingFixture {
    directory: Directory,
    store: Arc<SqliteStore>,
    application: Arc<NativeApplication>,
    catalog: Arc<PermissionCatalog>,
    calls: Arc<AtomicUsize>,
    thread: ThreadId,
    execution: ExecutionId,
    original_owner: u64,
    stale_fence: ApprovalFence,
    request: Option<ApprovalAcceptanceRequest>,
    before: OriginalCapture,
    waiting2: bool,
}
impl OriginalWaitingFixture {
    fn new() -> Self {
        Self::with_application("permission-app")
    }

    fn with_application(application_id: &str) -> Self {
        Self::with_identifiers(application_id, "release", "release-effect")
    }

    fn with_identifiers(application_id: &str, task_id: &str, effect_id: &str) -> Self {
        Self::with_input(
            application_id,
            task_id,
            effect_id,
            json!({"release": "fixture"}),
        )
    }

    fn with_input(
        application_id: &str,
        task_id: &str,
        effect_id: &str,
        input: serde_json::Value,
    ) -> Self {
        let directory = Directory::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let thread = ThreadId::new("f8e8bb7b-f8ac-4f29-a38f-3e0dfdb69b84").unwrap();
        let execution = ExecutionId::new("653c8f87-d983-4262-ad6e-17a08938a45f").unwrap();
        let task = TaskId::new(task_id).unwrap();
        let effect = EffectId::new(effect_id).unwrap();
        let interrupt = InterruptId::new("release-interrupt").unwrap();
        let context = ApprovalContext {
            actor: ThreadOrigin {
                application: application_id.into(),
                tenant: "tenant".into(),
                principal: "alice".into(),
            },
            policy_revision: "policy-1".into(),
        };
        let definition = Definition {
            route: RouteKey::new("/permission-first#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: task.clone(),
                effect_id: effect.clone(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            }]],
            max_attempts: 3,
        };
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: context.actor.application.clone(),
                    revision: "application-1".into(),
                },
                vec![RouteRegistration {
                    definition: definition.clone(),
                    effects: BTreeMap::from([(
                        effect.clone(),
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
        let catalog = Arc::new(
            PermissionCatalog::new(
                application.clone(),
                vec![ToolBindingRegistration {
                    definition: definition.clone(),
                    effect_id: effect,
                    tool_id: StableToolId::new("release-tool").unwrap(),
                    tool_revision: StableToolRevision::new("tool-1").unwrap(),
                    constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                    constraint: Arc::new(AllowConstraint),
                }],
            )
            .unwrap(),
        );
        let registered = application.definition(&definition.route).unwrap().clone();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let original_owner = store.owner_generation();
        store
            .create_thread_with(ThreadSeed {
                id: thread.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(context.actor.clone()),
            })
            .unwrap();
        let initial = new_execution(
            execution.clone(),
            TurnId::new("446916ea-643b-4d47-8aee-cc0e995dc769").unwrap(),
            original_owner,
            registered,
            Map::from_iter([("input".into(), input)]),
            facts(100),
        )
        .unwrap();
        let admitted = store.admit(&thread, initial).unwrap();
        store
            .commit(
                &execution,
                (&admitted.snapshot).into(),
                transition(&admitted.snapshot, KernelCommand::Start, facts(101)).unwrap(),
            )
            .unwrap();
        // Capture a real current fence before Park advances the execution to Waiting2.
        let stale_fence = fence(store.as_ref(), &thread);
        let authority = Arc::new(AllowAuthority);
        let service = ApprovalService::new(store.clone(), authority.clone()).unwrap();
        let prompt = json!({"question": "Release fixture?", "arguments": {"nullable": null}});
        service
            .park(
                &context,
                &execution,
                fence(store.as_ref(), &thread),
                task.clone(),
                interrupt.clone(),
                prompt,
                1_000,
                facts(102),
            )
            .unwrap();
        let pending = service
            .pending(&context, &execution, fence(store.as_ref(), &thread))
            .unwrap();
        // The service genuinely minted this token; no setup alias survives the call.
        drop(service);
        let before = OriginalCapture::read(store.as_ref(), &directory.0, &thread, &execution);
        let grant: GrantRecord =
            serde_json::from_str(produced_text(&before.raw.tables[4][0][7])).unwrap();
        let approved_turn = TurnId::new("a7ae0611-7e54-4ccb-ab14-2d095f33908b").unwrap();
        let request = ApprovalAcceptanceRequest {
            context: context.clone(),
            route: definition.route.clone(),
            definition_revision: definition.revision.clone(),
            turn_id: approved_turn.clone(),
            now_ms: 103,
            decisions: vec![GrantDecision {
                interrupt_id: pending[0].interrupt_id.clone(),
                decision: ApprovalDecision::Always,
                cancelled: false,
                token: Some(pending[0].token.clone()),
            }],
        };
        let waiting2 = before.execution.snapshot.revision == 2
            && before.execution.snapshot.status == ExecutionStatus::WaitingForApproval
            && before.execution.snapshot.turn_id.as_str() == "446916ea-643b-4d47-8aee-cc0e995dc769"
            && before.execution.snapshot.owner_generation == original_owner
            && before.execution.snapshot.tasks[0].status == TaskStatus::WaitingForApproval
            && before.view.execution.as_ref() == Some(&before.execution)
            && before.journal.len() == 3
            && before.raw.tables.iter().map(Vec::len).collect::<Vec<_>>()
                == vec![1, 1, 1, 4, 1, 0, 0, 0, 0]
            && pending.len() == 1
            && pending[0].task_id == task
            && pending[0].interrupt_id == grant.interrupt_id
            && pending[0].token == grant.token
            && pending[0].prompt == grant.prompt
            && pending[0].resume_key == grant.resume_key
            && pending[0].expires_at_ms == grant.expires_at_ms
            && grant.execution_id == execution
            && grant.thread_id == thread
            && grant.task_id == task
            && grant.thread_generation == before.view.thread.generation
            && grant.task_parking_revision == 2
            && !grant.consumed
            && calls.load(Ordering::SeqCst) == 0;

        drop(pending);
        drop(grant);
        drop(authority);
        Self {
            directory,
            store,
            application,
            catalog,
            calls,
            thread,
            execution,
            original_owner,
            stale_fence,
            request: Some(request),
            before,
            waiting2,
        }
    }

    fn unchanged_grant(&self, capture: &OriginalCapture) -> bool {
        let grant: GrantRecord =
            serde_json::from_str(produced_text(&capture.raw.tables[4][0][7])).unwrap();
        let original: GrantRecord =
            serde_json::from_str(produced_text(&self.before.raw.tables[4][0][7])).unwrap();
        grant == original
            && !grant.consumed
            && capture.raw.tables[4][0][6] == rusqlite::types::Value::Integer(0)
            && capture.raw.tables[6].is_empty()
            && capture.raw.tables[7].is_empty()
    }

    // Called only after the actual result has been dropped. Observe both owners
    // and remove the real fixture before either control makes its final assertion.
    fn retire(self, after: &OriginalCapture) -> bool {
        let Self {
            directory,
            store,
            application,
            catalog,
            calls,
            thread,
            execution,
            original_owner,
            before,
            request,
            ..
        } = self;
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
        let grant: GrantRecord =
            serde_json::from_str(produced_text(&reopened.raw.tables[4][0][7])).unwrap();
        let original_grant: GrantRecord =
            serde_json::from_str(produced_text(&before.raw.tables[4][0][7])).unwrap();
        let persisted_unchanged = reopened.view == before.view
            && reopened.execution == before.execution
            && reopened.journal == before.journal
            && reopened.raw.tables[1..] == before.raw.tables[1..]
            && reopened.raw.tables[1..] == after.raw.tables[1..]
            && reopened.raw.tables[0][0][..3] == before.raw.tables[0][0][..3]
            && produced_u64(&reopened.raw.tables[0][0][3]) == original_owner + 1
            && owner2.owner_generation() == original_owner + 1
            && grant == original_grant
            && !grant.consumed
            && reopened.raw.tables[4][0][6] == rusqlite::types::Value::Integer(0)
            && reopened.raw.tables[6].is_empty()
            && reopened.raw.tables[7].is_empty();
        let owner2_ready = owner2.readiness().is_ok();
        drop(grant);
        drop(original_grant);
        drop(reopened);
        drop(owner2);
        let path = directory.0.clone();
        drop(directory);
        anchors_retired
            && callbacks_zero
            && sole_owner
            && owner1_ready
            && owner2_ready
            && persisted_unchanged
            && !path.exists()
    }
}

#[test]
fn permission_original_always_uuid_stale_fence_preserves_waiting() {
    let mut fixture = OriginalWaitingFixture::new();
    let request = fixture.request.take().unwrap();
    let stale_was_real = fixture.stale_fence.execution.revision == 1
        && fixture.stale_fence.execution.owner_generation == fixture.original_owner
        && fixture.stale_fence.resource_revision + 1
            == fixture.before.view.thread.resource_revision;
    let actual = accept_permission_always_with_normal_inputs(
        fixture.store.clone(),
        Arc::new(AllowAuthority),
        fixture.catalog.clone(),
        &fixture.execution,
        fixture.stale_fence,
        request,
    );
    let refused = matches!(
        actual,
        Err(PermissionError::Store(StoreError::RevisionConflict))
    );
    let error_code = actual
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
    let valid = fixture.waiting2
        && stale_was_real
        && refused
        && fixture.before == after
        && fixture.unchanged_grant(&after)
        && fixture.calls.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    println!("original-always-uuid-stale: error={error_code} unchanged={valid} retired={retired}");
    assert!(
        valid && retired,
        "real pre-Park fence must refuse with complete Waiting2 persistence"
    );
}

fn original_install_late_abort(fixture: &OriginalWaitingFixture) -> bool {
    let connection = driver_injection_connection(&fixture.directory.0);
    let execution = fixture.execution.as_str().replace('\'', "''");
    let next_resource_revision = fixture.before.view.thread.resource_revision + 1;
    // This trigger only injects a fault. It never seeds or alters authority rows.
    // Its guards require the actual writer's snapshot/thread/grant/A/D mutations
    // to be visible within the transaction before the final journal insertion.
    let sql = format!("CREATE TRIGGER original_test_abort_journal BEFORE INSERT ON journal
        WHEN NEW.kind='commit' AND NEW.execution_id='{execution}' AND NEW.revision=X'0000000000000003'
        AND EXISTS (SELECT 1 FROM executions WHERE id=NEW.execution_id AND revision=NEW.revision)
        AND EXISTS (SELECT 1 FROM threads WHERE id=NEW.thread_id AND active_execution_id=NEW.execution_id AND resource_revision=X'{next_resource_revision:016x}')
        AND EXISTS (SELECT 1 FROM approval_grants WHERE execution_id=NEW.execution_id AND consumed=1)
        AND EXISTS (SELECT 1 FROM permission_acceptances WHERE execution_id=NEW.execution_id AND committed_revision=NEW.revision)
        AND EXISTS (SELECT 1 FROM permission_decisions WHERE thread_id=NEW.thread_id)
        BEGIN SELECT RAISE(ABORT, 'original_fixture_late_journal_abort'); END");
    connection.execute_batch(&sql).unwrap();
    let actual: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='original_test_abort_journal'",
        [], |row| row.get(0),
    ).unwrap();
    connection.close().unwrap();
    actual == sql
}
fn original_drop_late_abort(path: &std::path::Path) -> bool {
    let connection = driver_injection_connection(path);
    connection
        .execute_batch("DROP TRIGGER original_test_abort_journal")
        .unwrap();
    let remaining: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='original_test_abort_journal'",
        [], |row| row.get(0),
    ).unwrap();
    connection.close().unwrap();
    remaining == 0
}

#[test]
fn permission_original_always_uuid_late_journal_abort_rolls_back() {
    let mut fixture = OriginalWaitingFixture::new();
    let installed = original_install_late_abort(&fixture);
    let request = fixture.request.take().unwrap();
    let actual = accept_permission_always_with_normal_inputs(
        fixture.store.clone(),
        Arc::new(AllowAuthority),
        fixture.catalog.clone(),
        &fixture.execution,
        fence(fixture.store.as_ref(), &fixture.thread),
        request,
    );
    // The SQLite acceptance writer deliberately maps a rejected SQL statement
    // to this fixed Store error. This assertion concerns only this guarded fault.
    let refused = matches!(&actual, Err(PermissionError::Store(StoreError::Corrupt(code)))
        if code == "permission_acceptance_cells_invalid");
    let error_code = actual
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
    let removed = original_drop_late_abort(&fixture.directory.0);
    let after_trigger_removal = OriginalCapture::read(
        fixture.store.as_ref(),
        &fixture.directory.0,
        &fixture.thread,
        &fixture.execution,
    );
    let valid = fixture.waiting2
        && installed
        && removed
        && refused
        && fixture.before == after
        && after == after_trigger_removal
        && fixture.unchanged_grant(&after)
        && fixture.calls.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    println!(
        "original-always-uuid-abort: error={error_code} rolled_back={valid} retired={retired}"
    );
    assert!(
        valid && retired,
        "guarded late journal failure must roll back the entire original acceptance"
    );
}

#[path = "original_ready_cancel.rs"]
mod original_ready_cancel;
