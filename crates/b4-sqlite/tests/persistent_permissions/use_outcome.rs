// Independent real-producer dispatch qualification; old fixtures stay literal.
use super::*;
use std::sync::Mutex;

struct UseDispatchObservedStore {
    inner: Arc<SqliteStore>,
    captures: Arc<Mutex<Vec<&'static str>>>,
    entries: Arc<AtomicUsize>,
    outcome_control: Option<Arc<OutcomeBackendControl>>,
}
impl Store for UseDispatchObservedStore {
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    fn owner_generation(&self) -> u64 {
        self.inner.owner_generation()
    }
    fn create_thread(&self, id: ThreadId) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread(id)
    }
    fn create_thread_with(&self, seed: ThreadSeed) -> Result<ThreadRecord, StoreError> {
        self.inner.create_thread_with(seed)
    }
    fn thread(&self, id: &ThreadId) -> Result<Option<ThreadRecord>, StoreError> {
        self.inner.thread(id)
    }
    fn thread_view(&self, id: &ThreadId) -> Result<Option<ThreadView>, StoreError> {
        self.inner.thread_view(id)
    }
    fn delete_thread_fenced(&self, target: &ThreadDeletionTarget) -> Result<(), StoreError> {
        self.inner.delete_thread_fenced(target)
    }
    fn cancel_run_fenced(
        &self,
        target: &RunCancellationTarget,
        facts: TransitionFacts,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.cancel_run_fenced(target, facts)
    }
    fn execution(&self, id: &ExecutionId) -> Result<Option<StoredExecution>, StoreError> {
        self.inner.execution(id)
    }
    fn admit(
        &self,
        thread_id: &ThreadId,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit(thread_id, initial)
    }
    fn admit_fenced(
        &self,
        thread_id: &ThreadId,
        expected_thread_generation: &ThreadGeneration,
        expected_resource_revision: u64,
        initial: Snapshot,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.admit_fenced(
            thread_id,
            expected_thread_generation,
            expected_resource_revision,
            initial,
        )
    }
    fn readiness(&self) -> Result<(), StoreError> {
        self.inner.readiness()
    }
    fn commit(
        &self,
        execution_id: &ExecutionId,
        fence: ExecutionFence,
        plan: CommitPlan,
    ) -> Result<StoredExecution, StoreError> {
        self.inner.commit(execution_id, fence, plan)
    }
    fn journal(&self, execution_id: &ExecutionId) -> Result<Vec<JournalEntry>, StoreError> {
        self.inner.journal(execution_id)
    }

    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        self.inner.discover_unfinished(cursor)
    }

    fn confinement_view_fenced(
        &self,
        target: &RecoveryConfinementTarget,
    ) -> Result<ThreadView, RecoveryConfinementBackendError> {
        self.inner.confinement_view_fenced(target)
    }

    fn confine_recovery_fenced(
        &self,
        original: &ThreadView,
        target: &RecoveryConfinementTarget,
        reason: RecoveryConfinementReason,
        fresh_turn: &TurnId,
        facts: TransitionFacts,
    ) -> Result<RecoveryConfinementOutcome, RecoveryConfinementBackendError> {
        self.inner
            .confine_recovery_fenced(original, target, reason, fresh_turn, facts)
    }
}
impl ApprovalStore for UseDispatchObservedStore {
    fn approval_durable(&self) -> bool {
        self.inner.approval_durable()
    }
    fn approval_view(&self, id: &ExecutionId) -> Result<Option<ApprovalView>, ApprovalError> {
        self.inner.approval_view(id)
    }
    fn commit_approval(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        plan: CommitPlan,
        mutation: ApprovalMutation,
    ) -> Result<StoredExecution, ApprovalError> {
        self.inner.commit_approval(id, fence, plan, mutation)
    }
}
impl PermissionStore for UseDispatchObservedStore {
    fn commit_permission_use_outcome(
        &self,
        request: &PermissionUseOutcomeRequest<'_>,
        account: &mut PermissionUseOutcomeAccount,
    ) -> Result<PermissionUseOutcomeCommit, PermissionError> {
        self.entries.fetch_add(1, Ordering::SeqCst);
        if let Some(control) = &self.outcome_control {
            return control.forward(&self.inner, request, account);
        }
        self.inner.commit_permission_use_outcome(request, account)
    }

    fn capture_permission_use_renewal_resource(
        &self,
        selector: &PermissionUseRenewalResourceSelector<'_>,
        account: &mut PermissionUseRenewalAccount,
    ) -> Result<PermissionUseRenewalObservation, PermissionError> {
        self.captures.lock().unwrap().push("resource");
        let result = self
            .inner
            .capture_permission_use_renewal_resource(selector, account);
        self.captures.lock().unwrap().push(
            result
                .as_ref()
                .err()
                .map(PermissionError::code)
                .unwrap_or("ok"),
        );
        result
    }
    fn capture_permission_use_renewal_source(
        &self,
        selector: &PermissionUseRenewalSourceSelector<'_>,
        account: &mut PermissionUseRenewalAccount,
    ) -> Result<PermissionUseRenewalSourceCapture, PermissionError> {
        self.captures.lock().unwrap().push("source");
        let result = self
            .inner
            .capture_permission_use_renewal_source(selector, account);
        self.captures.lock().unwrap().push(
            result
                .as_ref()
                .err()
                .map(PermissionError::code)
                .unwrap_or("ok"),
        );
        result
    }
    fn durable_permissions(&self) -> bool {
        self.inner.durable_permissions()
    }
    fn ready_authorization_atomic_durable(&self) -> bool {
        self.inner.ready_authorization_atomic_durable()
    }
    fn permission_resource(
        &self,
        id: &ExecutionId,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<ApprovalResource, PermissionError> {
        self.inner.permission_resource(id, budget)
    }
    fn permission_view(
        &self,
        id: &ExecutionId,
        fence: ApprovalFence,
        selector: &PermissionViewSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionView, PermissionError> {
        self.inner.permission_view(id, fence, selector, budget)
    }
    fn commit_permission_acceptance(
        &self,
        prepared: PermissionPreparedAcceptance<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<StoredExecution, PermissionError> {
        self.inner.commit_permission_acceptance(prepared, budget)
    }
    fn capture_permission_runtime(
        &self,
        selector: &PermissionRuntimeSelector<'_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCapture, PermissionError> {
        let result = self.inner.capture_permission_runtime(selector, budget);
        let code = result
            .as_ref()
            .err()
            .map(PermissionError::code)
            .unwrap_or("ok");
        self.captures.lock().unwrap().push(code);
        result
    }
    fn commit_permission_runtime(
        &self,
        prepared: &PermissionPreparedRuntimeMutation<'_, '_>,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<PermissionRuntimeCommit, PermissionError> {
        self.entries.fetch_add(1, Ordering::SeqCst);
        self.inner.commit_permission_runtime(prepared, budget)
    }
}

fn use_dispatch_raw_nine(path: &std::path::Path) -> ProducedRawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let raw = ProducedRawNine { tables: [
        produced_raw_cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        produced_raw_cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        produced_raw_cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 3", 5, 2),
        produced_raw_cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 12", 9, 11),
        produced_raw_cells(&connection, "SELECT execution_id, interrupt_id, task_id, thread_id, thread_generation, task_parking_revision, consumed, record FROM approval_grants ORDER BY execution_id, interrupt_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, definition_revision, accepted_at_ms, record FROM approval_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT execution_id, turn_id, thread_id, thread_generation, previous_revision, committed_revision, record_version, record FROM permission_acceptances ORDER BY execution_id, turn_id LIMIT 2", 8, 1),
        produced_raw_cells(&connection, "SELECT permission_id, thread_id, thread_generation, scope_digest, record_version, record FROM permission_decisions ORDER BY permission_id LIMIT 2", 6, 1),
        produced_raw_cells(&connection, "SELECT execution_id, task_id, use_id, thread_id, thread_generation, permission_id, turn_id, authorization_revision, record_version, record FROM permission_uses ORDER BY execution_id, task_id LIMIT 2", 10, 1),
    ] };
    assert!(
        connection.close().is_ok(),
        "read-only produced observer closes normally"
    );
    raw
}
// Keep copied refusal branches literal; only Accepted is selected in this scaffold.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenewalScenario {
    Accepted,
    AuthorityDenied,
    ConstraintDenied,
    PolicyRevisionChanged,
    ApplicationRevisionChanged,
    ConstraintRevisionChanged,
    ExportFreshAccount,
    ExportFinishedAccount,
    ExportRefusedAccount,
    ForeignAccountOrigin,
    ReusedFinishedAccount,
    ReusedRefusedAccount,
}

struct UseRenewalAuthority {
    deny: bool,
    expected_context: ApprovalContext,
    expected_resource: ApprovalResource,
    calls: Arc<AtomicUsize>,
    matched_calls: Arc<AtomicUsize>,
    trace: Arc<Mutex<Vec<&'static str>>>,
}
impl ApprovalAuthority for UseRenewalAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.trace.lock().unwrap().push("authority");
        if context != &self.expected_context
            || !matches!(operation, ApprovalOperation::Dispatch)
            || resource != &self.expected_resource
        {
            return Err(ApprovalError::Unauthorized);
        }
        self.matched_calls.fetch_add(1, Ordering::SeqCst);
        if self.deny {
            return Err(ApprovalError::Unauthorized);
        }
        Ok(())
    }
}

struct UseRenewalConstraint {
    deny: bool,
    expected_application_revision: String,
    expected_constraint_revision: String,
    expected_context: InvocationContext,
    expected_definition: Definition,
    expected_task: TaskRecord,
    expected_invocation: EffectInvocation,
    calls: Arc<AtomicUsize>,
    matched_calls: Arc<AtomicUsize>,
}
impl ToolConstraint for UseRenewalConstraint {
    fn validate_call(&self, call: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let claimed_matches = match &call.phase {
            PermissionCallPhase::Claimed(invocation) => *invocation == &self.expected_invocation,
            PermissionCallPhase::Unclaimed => false,
        };
        if !claimed_matches
            || call.context != &self.expected_context
            || call.definition != &self.expected_definition
            || call.task != &self.expected_task
            || call.binding.application.application != "permission-app"
            || call.binding.application.revision != self.expected_application_revision
            || call.binding.definition != self.expected_definition
            || call.binding.effect_id != self.expected_task.spec.effect_id
            || call.binding.tool_id.as_str() != "release-tool"
            || call.binding.tool_revision.as_str() != "tool-1"
            || call.binding.constraint_revision.as_str() != self.expected_constraint_revision
        {
            return Err(ToolConstraintError::Denied);
        }
        self.matched_calls.fetch_add(1, Ordering::SeqCst);
        if self.deny {
            return Err(ToolConstraintError::Denied);
        }
        Ok(())
    }
}

#[test]
fn permission_use_settled_result_requires_typed_outcome_commit() {
    run_use_outcome_scenario(OutcomeScenario::Succeeded);
}

#[test]
fn permission_use_outcome_returned_failure_persists_exact_string() {
    run_use_outcome_scenario(OutcomeScenario::ReturnedFailed);
}
#[test]
fn permission_use_outcome_late_abort_rolls_back_then_explicit_retry_and_repeat_refusal() {
    run_use_outcome_scenario(OutcomeScenario::JournalAbortThenRetry);
}
#[test]
fn permission_use_outcome_actual_opaque_request_cannot_replay() {
    run_use_outcome_scenario(OutcomeScenario::ReplayActualRequest);
}
#[test]
fn permission_use_outcome_external_resource_corruption_is_unchanged() {
    run_use_outcome_scenario(OutcomeScenario::ChangedResourceRevision);
}
#[test]
fn permission_use_outcome_external_source_corruption_is_unchanged() {
    run_use_outcome_scenario(OutcomeScenario::ChangedSourceHeader);
}
#[test]
fn permission_use_outcome_external_owner_corruption_is_unchanged() {
    run_use_outcome_scenario(OutcomeScenario::CorruptOwnerMetadata);
}
#[test]
fn permission_use_outcome_foreign_physical_store_is_unchanged() {
    run_use_outcome_scenario(OutcomeScenario::ForeignPhysicalStore);
}

fn run_use_outcome_scenario(outcome: OutcomeScenario) {
    assert!(run_use_renewal_fixture(RenewalScenario::Accepted, None, outcome).is_none());
}

fn run_use_renewal_fixture(
    scenario: RenewalScenario,
    supplied_account: Option<PermissionUseRenewalAccount>,
    outcome_scenario: OutcomeScenario,
) -> Option<PermissionUseRenewalAccount> {
    let always = true;
    let directory = Directory::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let thread = ThreadId::new("permission-thread").unwrap();
    let execution = ExecutionId::new("permission-execution").unwrap();
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
    let registered_matches = registered == definition;
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
        TurnId::new("original-turn").unwrap(),
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
    let before = store.approval_view(&execution).unwrap().unwrap();
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: if always {
                ApprovalDecision::Always
            } else {
                ApprovalDecision::Once
            },
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };

    let producer = if always {
        accept_permission_human(
            store.clone(),
            authority.clone(),
            catalog.clone(),
            &execution,
            fence(store.as_ref(), &thread),
            request.clone(),
        )
    } else {
        service
            .accept(&execution, fence(store.as_ref(), &thread), request.clone())
            .map_err(PermissionError::Approval)
    }
    .unwrap();
    let before_subject = store.thread_view(&thread).unwrap().unwrap();
    let execution_before = store.execution(&execution).unwrap().unwrap();
    let journal_before = store.journal(&execution).unwrap();
    let raw_before = first_claim_raw_nine(&directory.0);
    assert!(
        registered_matches,
        "actual native Definition remains registered"
    );
    assert_eq!(before_subject.execution.as_ref(), Some(&producer));
    assert_eq!(execution_before, producer);
    assert_eq!(producer.snapshot.revision, 3);
    assert_eq!(producer.snapshot.status, ExecutionStatus::Running);
    assert_eq!(producer.snapshot.turn_id, request.turn_id);
    assert_eq!(producer.snapshot.owner_generation, original_owner);
    assert_eq!(producer.snapshot.definition, definition);
    assert_eq!(producer.snapshot.attempts_used, 0);
    assert!(producer.snapshot.attempt_history.is_empty());
    assert_eq!(producer.snapshot.tasks.len(), 1);
    assert_eq!(producer.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(producer.snapshot.tasks[0].approved);
    assert!(producer.snapshot.tasks[0].attempt_id.is_none());
    assert!(producer.snapshot.tasks[0].result.is_none());
    assert!(producer.snapshot.tasks[0].failure.is_none());
    assert_eq!(
        producer.snapshot.tasks[0].interrupt,
        before.execution.snapshot.tasks[0].interrupt
    );
    assert_eq!(before_subject.thread.resource_revision, 4);
    assert_eq!(journal_before.len(), 4);
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_before.tables[4].len(), 1);
    let mut expected_grant = before.grants[0].clone();
    assert_eq!(pending.len(), 1);
    assert_eq!(expected_grant.token, pending[0].token);
    assert_eq!(expected_grant.verifier, token_verifier(&pending[0].token));
    assert_eq!(
        expected_grant.proposal_digest,
        proposal_digest(&before.execution.snapshot.tasks[0], &expected_grant.prompt).unwrap()
    );
    expected_grant.consumed = true;
    let observed_grant: GrantRecord =
        serde_json::from_str(produced_text(&raw_before.tables[4][0][7])).unwrap();
    assert_eq!(observed_grant, expected_grant);
    assert!(raw_before.tables[8].is_empty());
    assert_eq!(raw_before.tables[5].len(), usize::from(!always));
    assert_eq!(raw_before.tables[6].len(), usize::from(always));
    assert_eq!(raw_before.tables[7].len(), usize::from(always));
    b4_kernel::validate_snapshot(&producer.snapshot).unwrap();
    let subject_fence = ApprovalFence {
        execution: (&execution_before.snapshot).into(),
        resource_revision: before_subject.thread.resource_revision,
    };
    let ready = if always {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = read_permission_view(
            store.as_ref(),
            authority.as_ref(),
            catalog.as_ref(),
            &execution,
            subject_fence,
            &context,
            PermissionReadPurpose::Dispatch,
            Some(&task),
            &mut control_account,
        )
        .unwrap();
        assert_eq!(actual.thread(), &before_subject.thread);
        assert_eq!(actual.execution(), &execution_before);
        assert_eq!(actual.grants(), std::slice::from_ref(&observed_grant));
        assert!(actual.acceptances().is_empty() && actual.permission_uses().is_empty());
        let acceptance = PermissionAcceptance::decode_record(
            produced_text(&raw_before.tables[6][0][7]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        let decision = PermissionDecision::decode_record(
            produced_text(&raw_before.tables[7][0][5]).as_bytes(),
            &mut PermissionEvidenceBudget::new(),
        )
        .unwrap();
        assert_eq!(
            actual.permission_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.permission_decisions(),
            std::slice::from_ref(&decision)
        );
        assert_eq!(
            actual.creator_acceptances(),
            std::slice::from_ref(&acceptance)
        );
        assert_eq!(
            actual.referenced_grants(),
            std::slice::from_ref(&observed_grant)
        );
        assert!(actual.referenced_acceptances().is_empty());
        assert_eq!(actual.journal_entries(), &journal_before[2..]);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
        Some(actual)
    } else {
        None
    };
    {
        let mut control_account = PermissionEvidenceBudget::new();
        let actual = store
            .permission_resource(&execution, &mut control_account)
            .unwrap();
        assert_eq!(actual.thread, before_subject.thread);
        assert_eq!(actual.execution, execution_before);
        let usage = produced_usage(&control_account);
        assert!(usage.0 > 0 && usage.1 > 0 && usage.0 <= 8_388_608 && usage.1 <= 65_536);
    }
    assert!(
        first_claim_raw_nine(&directory.0) == raw_before,
        "healthy controls preserve all nine subject BEFORE tables"
    );
    assert!(store.approval_durable());
    assert!(!store.durable_permissions());
    let target = RunCancellationTarget::from_view(&before_subject).unwrap();
    let expected = transition(
        &execution_before.snapshot,
        KernelCommand::Cancel {
            turn_id: execution_before.snapshot.turn_id.clone(),
            owner_generation: original_owner,
        },
        facts(104),
    )
    .unwrap();
    validate_commit(
        &execution_before.snapshot,
        target.execution_fence,
        &expected,
        original_owner,
    )
    .unwrap();
    let authority_calls = Arc::new(AtomicUsize::new(0));
    let capture_calls = Arc::new(AtomicUsize::new(0));
    let terminal_entries = Arc::new(AtomicUsize::new(0));
    let cancellation_authority = Arc::new(ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_subject.thread.clone(),
            execution: execution_before.clone(),
        },
        expected_target: target.clone(),
        calls: authority_calls.clone(),
    });
    let observed = Arc::new(ReadyCancellationObservedStore {
        inner: store.clone(),
        captures: capture_calls.clone(),
        terminal_entries: terminal_entries.clone(),
    });
    let mut subject_account = PermissionEvidenceBudget::new();
    let initial_usage = produced_usage(&subject_account);
    let cancellation_request = PermissionReadyCancellationRequest {
        context: &context,
        target: &target,
        facts: facts(104),
    };
    let actual_result = cancel_permission_ready(
        observed.as_ref(),
        cancellation_authority.as_ref(),
        catalog.as_ref(),
        &cancellation_request,
        &mut subject_account,
    );
    let final_usage = produced_usage(&subject_account);
    // Capture every actual typed/raw AFTER before inspecting the subject result.
    let after = store.thread_view(&thread).unwrap().unwrap();
    let execution_after = store.execution(&execution).unwrap().unwrap();
    let journal_after = store.journal(&execution).unwrap();
    let raw_after = first_claim_raw_nine(&directory.0);
    let actual_authority_calls = authority_calls.load(Ordering::SeqCst);
    let actual_capture_calls = capture_calls.load(Ordering::SeqCst);
    let actual_terminal_entries = terminal_entries.load(Ordering::SeqCst);
    drop(subject_account);
    drop(observed);
    drop(cancellation_authority);
    drop(ready);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(application);
    store.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "every live store alias retires normally"
    );
    drop(store);
    let actual_native_calls = calls.load(Ordering::SeqCst);
    assert_eq!(
        actual_native_calls, 0,
        "cancellation and observations invoke no native effect"
    );

    // N+1 independently observes the fully retired original cancellation owner.
    let next = SqliteStore::open(&directory.0).unwrap();
    let next_owner = next.owner_generation();
    let next_view = next.thread_view(&thread).unwrap().unwrap();
    let next_execution = next.execution(&execution).unwrap().unwrap();
    let next_journal = next.journal(&execution).unwrap();
    let next_raw = ready_use_raw_nine(&directory.0);
    assert_eq!(next_owner, original_owner.checked_add(1).unwrap());
    assert_eq!(next_view, after);
    assert_eq!(next_execution, execution_after);
    assert_eq!(next_journal, journal_after);
    assert!(next_raw.tables[1..] == raw_after.tables[1..]);
    assert_eq!(next_raw.tables[0][0][..3], raw_after.tables[0][0][..3]);
    assert_eq!(produced_u64(&next_raw.tables[0][0][3]), next_owner);
    assert_eq!(produced_u64(&raw_after.tables[0][0][3]), original_owner);
    let cancel_code = actual_result
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    assert_eq!(
        actual_authority_calls, 2,
        "real R1 and renewed R2 cancellation authorization"
    );
    assert_eq!(
        actual_capture_calls, 3,
        "real R1, sensitive C1, and fresh R2 sibling captures"
    );
    assert_eq!(
        actual_terminal_entries, 1,
        "actual opaque Prepared reaches terminal sibling"
    );
    assert_eq!(after.execution.as_ref(), Some(&execution_after));
    assert!(
        initial_usage == (0, 0)
            && final_usage.0 > initial_usage.0
            && final_usage.1 > initial_usage.1
            && final_usage.0 <= 8_388_608
            && final_usage.1 <= 65_536,
        "one fresh original cancellation account is finite and monotone",
    );

    // Cancellation is already qualified setup for this ordinary admission control.
    assert!(
        actual_result.is_ok(),
        "qualified cancellation setup failed: {cancel_code}"
    );
    let returned = actual_result.unwrap();
    assert_eq!(returned, execution_after);
    assert_eq!(execution_after.thread_id, execution_before.thread_id);
    assert_eq!(
        execution_after.snapshot, expected.snapshot,
        "complete actual Kernel Cancel post"
    );
    assert_eq!(execution_after.snapshot.status, ExecutionStatus::Cancelled);
    assert_eq!(execution_after.snapshot.revision, 4);
    assert_eq!(execution_after.snapshot.owner_generation, original_owner);
    assert_eq!(
        execution_after.snapshot.tasks[0].status,
        TaskStatus::Cancelled
    );
    assert!(execution_after.snapshot.tasks[0].approved);
    assert_eq!(
        execution_after.snapshot.tasks[0].interrupt,
        execution_before.snapshot.tasks[0].interrupt
    );
    assert_eq!(execution_after.snapshot.attempts_used, 0);
    assert!(execution_after.snapshot.attempt_history.is_empty());
    assert!(execution_after.snapshot.tasks[0].attempt_id.is_none());
    assert!(execution_after.snapshot.tasks[0].result.is_none());
    assert!(execution_after.snapshot.tasks[0].failure.is_none());
    let mut expected_thread = before_subject.thread.clone();
    expected_thread.resource_revision = expected_thread.resource_revision.checked_add(1).unwrap();
    assert_eq!(after.thread, expected_thread);
    assert_eq!(journal_after.len(), 5);
    assert_eq!(&journal_after[..4], journal_before.as_slice());
    let cancellation_entry = JournalEntry::from(&expected.snapshot);
    assert_eq!(journal_after[4], cancellation_entry);
    for index in [0, 4, 5, 6, 7, 8] {
        assert!(
            raw_before.tables[index] == raw_after.tables[index],
            "cancellation preserves all original authority and metadata cells"
        );
    }
    assert_eq!(raw_after.tables[1][0][..5], raw_before.tables[1][0][..5]);
    assert_eq!(produced_u64(&raw_after.tables[1][0][5]), 5);
    assert_eq!(raw_after.tables[1][0][6], raw_before.tables[1][0][6]);
    let row = &raw_after.tables[2][0];
    assert_eq!(row[..2], raw_before.tables[2][0][..2]);
    assert_eq!(produced_u64(&row[2]), expected.snapshot.revision);
    assert_eq!(produced_u64(&row[3]), original_owner);
    assert_eq!(
        serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
        expected.snapshot
    );
    assert_eq!(raw_before.tables[3].len(), 5);
    assert_eq!(raw_after.tables[3].len(), 6);
    assert_eq!(&raw_after.tables[3][..5], raw_before.tables[3].as_slice());
    let row = &raw_after.tables[3][5];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&raw_before.tables[3][4][0])
    );
    assert_eq!(produced_text(&row[1]), "commit");
    assert_eq!(produced_text(&row[2]), after.thread.id.as_str());
    assert_eq!(
        produced_text(&row[3]),
        cancellation_entry.execution_id.as_str()
    );
    assert_eq!(produced_u64(&row[4]), cancellation_entry.revision);
    assert_eq!(produced_u64(&row[5]), cancellation_entry.owner_generation);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&cancellation_entry.status).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), cancellation_entry.updated_at_ms);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        cancellation_entry
    );
    assert_eq!(
        next_raw.tables.each_ref().map(Vec::len),
        [1, 1, 1, 6, 1, 0, 1, 1, 0]
    );
    assert_eq!(next_raw.tables[4][0][6], rusqlite::types::Value::Integer(1));
    assert!(next_raw.tables[8].is_empty());
    assert_eq!(
        next_view.thread.active_execution_id.as_ref(),
        Some(&execution)
    );

    let second_execution = ExecutionId::new("permission-second").unwrap();
    let second_turn = TurnId::new("second-turn").unwrap();
    let changed_values = Map::from_iter([("input".into(), json!({"release": "changed"}))]);
    assert_ne!(changed_values, execution_before.snapshot.values);
    assert!(next.execution(&second_execution).unwrap().is_none());
    let second_initial = new_execution(
        second_execution.clone(),
        second_turn.clone(),
        next_owner,
        definition.clone(),
        changed_values.clone(),
        facts(105),
    )
    .unwrap();
    let admitted_e2 = next
        .admit_fenced(
            &thread,
            &next_view.thread.generation,
            next_view.thread.resource_revision,
            second_initial.clone(),
        )
        .unwrap();
    assert_eq!(admitted_e2.snapshot, second_initial);
    let admission_view = next.thread_view(&thread).unwrap().unwrap();
    let admission_raw = ready_use_raw_nine(&directory.0);
    assert_eq!(
        admission_view.thread.active_execution_id.as_ref(),
        Some(&second_execution)
    );
    assert_eq!(admission_view.thread.resource_revision, 6);
    assert_eq!(
        admission_raw.tables.each_ref().map(Vec::len),
        [1, 1, 2, 7, 1, 0, 1, 1, 0]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert_eq!(admission_raw.tables[index], next_raw.tables[index]);
    }
    assert_eq!(admission_raw.tables[2][0], next_raw.tables[2][0]);
    assert_eq!(&admission_raw.tables[3][..6], next_raw.tables[3].as_slice());

    // The ordinary Start is legal setup. No raw authorization plan is committed.
    let started_e2 = next
        .commit(
            &second_execution,
            (&admitted_e2.snapshot).into(),
            transition(&admitted_e2.snapshot, KernelCommand::Start, facts(106)).unwrap(),
        )
        .unwrap();
    let before_use = next.thread_view(&thread).unwrap().unwrap();
    let e1_before_use = next.execution(&execution).unwrap();
    let e2_before_use = next.execution(&second_execution).unwrap();
    let e1_journal_before_use = next.journal(&execution).unwrap();
    let e2_journal_before_use = next.journal(&second_execution).unwrap();
    let raw_before_use = ready_use_raw_nine(&directory.0);
    assert_eq!(before_use.execution.as_ref(), Some(&started_e2));
    assert_eq!(e2_before_use.as_ref(), Some(&started_e2));
    assert_eq!(e1_before_use.as_ref(), Some(&next_execution));
    assert_eq!(e1_journal_before_use, next_journal);
    assert_eq!(before_use.thread.resource_revision, 7);
    assert_eq!(started_e2.snapshot.execution_id, second_execution);
    assert_eq!(started_e2.snapshot.turn_id, second_turn);
    assert_eq!(started_e2.snapshot.owner_generation, next_owner);
    assert_eq!(started_e2.snapshot.definition, definition);
    assert_eq!(started_e2.snapshot.values, changed_values);
    assert_eq!(started_e2.snapshot.status, ExecutionStatus::Running);
    assert_eq!(started_e2.snapshot.revision, 1);
    assert_eq!(started_e2.snapshot.attempts_used, 0);
    assert!(started_e2.snapshot.attempt_history.is_empty());
    assert!(started_e2.snapshot.output.is_none());
    assert!(started_e2.snapshot.failure.is_none());
    assert_eq!(started_e2.snapshot.tasks.len(), 1);
    let ready_task = &started_e2.snapshot.tasks[0];
    assert_eq!(ready_task.spec, definition.batches[0][0]);
    assert_eq!(ready_task.input, changed_values);
    assert_eq!(ready_task.status, TaskStatus::Ready);
    assert!(!ready_task.approved);
    assert!(ready_task.interrupt.is_none());
    assert!(ready_task.attempt_id.is_none());
    assert!(ready_task.result.is_none());
    assert!(ready_task.failure.is_none());
    b4_kernel::validate_snapshot(&started_e2.snapshot).unwrap();
    assert_eq!(e2_journal_before_use.len(), 2);
    assert_eq!(
        e2_journal_before_use[0],
        JournalEntry::from(&second_initial)
    );
    assert_eq!(
        e2_journal_before_use[1],
        JournalEntry::from(&started_e2.snapshot)
    );
    assert_eq!(
        raw_before_use.tables.each_ref().map(Vec::len),
        [1, 1, 2, 8, 1, 0, 1, 1, 0]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert_eq!(raw_before_use.tables[index], admission_raw.tables[index]);
    }
    assert_eq!(raw_before_use.tables[2][0], admission_raw.tables[2][0]);
    assert_eq!(
        &raw_before_use.tables[3][..7],
        admission_raw.tables[3].as_slice()
    );

    let mut setup_control_account = PermissionEvidenceBudget::new();
    let original_decision = PermissionDecision::decode_record(
        produced_text(&raw_before_use.tables[7][0][5]).as_bytes(),
        &mut setup_control_account,
    )
    .unwrap();
    assert_eq!(original_decision.record_version(), 1);
    assert_eq!(original_decision.creator_execution_id(), &execution);
    assert_eq!(original_decision.creator_turn_id(), &request.turn_id);
    assert_eq!(original_decision.creator_task_id(), &task);
    assert_eq!(
        original_decision.scope_digest(),
        original_decision
            .scope()
            .canonical_digest(&mut setup_control_account)
            .unwrap()
    );
    let original_scope = original_decision.scope();
    assert_eq!(original_scope.thread_id(), &before_use.thread.id);
    assert_eq!(
        original_scope.thread_generation(),
        &before_use.thread.generation
    );
    assert_eq!(original_scope.origin(), &context.actor);
    assert_eq!(original_scope.application().application(), "permission-app");
    assert_eq!(original_scope.application().revision(), "application-1");
    assert_eq!(original_scope.route(), &definition.route);
    assert_eq!(original_scope.definition(), &definition);
    assert_eq!(original_scope.effect_id(), &ready_task.spec.effect_id);
    assert_eq!(original_scope.tool_id().as_str(), "release-tool");
    assert_eq!(original_scope.tool_revision().as_str(), "tool-1");
    assert_eq!(original_scope.policy_revision(), context.policy_revision);
    assert_eq!(
        original_scope.constraint_revision().as_str(),
        "constraint-1"
    );
    let expected_call_digest =
        permission_call_digest(ready_task, &mut setup_control_account).unwrap();
    let original_call_digest = permission_call_digest(
        &execution_before.snapshot.tasks[0],
        &mut setup_control_account,
    )
    .unwrap();
    assert_ne!(
        expected_call_digest, original_call_digest,
        "changed allowed input has a different actual call witness"
    );
    drop(setup_control_account);

    // Fresh native/catalog objects follow complete original-owner retirement.
    let use_resource_reads = Arc::new(AtomicUsize::new(0));
    let new_authority_calls = Arc::new(AtomicUsize::new(0));
    let new_gate_calls = Arc::new(AtomicUsize::new(0));
    let use_entries = Arc::new(AtomicUsize::new(0));
    let unused_runtime_captures = Arc::new(AtomicUsize::new(0));
    let unused_cancellation_entries = Arc::new(AtomicUsize::new(0));
    let fresh_application = Arc::new(
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
    let expected_invocation = InvocationContext {
        thread_id: before_use.thread.id.clone(),
        thread_generation: before_use.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: "application-1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let fresh_catalog = Arc::new(
        PermissionCatalog::new(
            fresh_application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: Arc::new(ReadyUseConstraint {
                    expected_context: expected_invocation,
                    expected_definition: definition.clone(),
                    expected_task: ready_task.clone(),
                    calls: new_gate_calls.clone(),
                }),
            }],
        )
        .unwrap(),
    );
    assert_eq!(
        fresh_application.definition(&definition.route),
        Some(&definition)
    );
    let use_authority = ReadyUseAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: before_use.thread.clone(),
            execution: started_e2.clone(),
        },
        calls: new_authority_calls.clone(),
    };
    // Command capture is caller-owned fixture setup, outside the service account.
    // The service only borrows this command; it cannot claim a retroactive clone payment.
    let use_id = PermissionUseId::new(
        "b4pu_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    )
    .unwrap();
    let command = b4_kernel::PermissionAuthorizationCommand::capture(
        &started_e2.snapshot,
        task.clone(),
        use_id.clone(),
    )
    .unwrap();
    let expected_authorization =
        b4_kernel::authorize_ready(&started_e2.snapshot, &command, facts(107)).unwrap();
    let use_fence = ApprovalFence {
        execution: (&started_e2.snapshot).into(),
        resource_revision: before_use.thread.resource_revision,
    };
    let next = Arc::new(next);
    let observed_use = ReadyUseObservedStore {
        inner: next.clone(),
        resources: use_resource_reads.clone(),
        captures: unused_runtime_captures.clone(),
        terminal_entries: unused_cancellation_entries.clone(),
        use_entries: use_entries.clone(),
    };
    let mut use_account = PermissionEvidenceBudget::new();
    let initial_use_usage = produced_usage(&use_account);
    let use_result = {
        let use_request = PermissionReadyAuthorizationRequest {
            context: &context,
            command: &command,
            facts: facts(107),
        };
        try_authorize_permission_ready(
            &observed_use,
            &use_authority,
            fresh_catalog.as_ref(),
            use_fence,
            &use_request,
            &mut use_account,
        )
    };
    let final_use_usage = produced_usage(&use_account);
    // Every actual AFTER is captured before unwrapping or selecting the result.
    let after_use = next.thread_view(&thread).unwrap().unwrap();
    let e1_after_use = next.execution(&execution).unwrap();
    let e2_after_use = next.execution(&second_execution).unwrap();
    let e1_journal_after_use = next.journal(&execution).unwrap();
    let e2_journal_after_use = next.journal(&second_execution).unwrap();
    let raw_after_use = ready_use_raw_nine(&directory.0);
    let actual_use_resource_reads = use_resource_reads.load(Ordering::SeqCst);
    let actual_use_authority_calls = new_authority_calls.load(Ordering::SeqCst);
    let actual_use_gate_calls = new_gate_calls.load(Ordering::SeqCst);
    let actual_use_entries = use_entries.load(Ordering::SeqCst);
    let actual_unused_captures = unused_runtime_captures.load(Ordering::SeqCst);
    let actual_unused_cancellation_entries = unused_cancellation_entries.load(Ordering::SeqCst);
    drop(use_account);
    drop(observed_use);
    drop(use_authority);
    drop(fresh_catalog);
    drop(fresh_application);
    drop(command);
    // This real, already-qualified producer is setup for a distinct Dispatch subject.
    // All producer AFTERs above were captured before selecting its result.
    let authorized = use_result
        .unwrap()
        .expect("real typed Use producer succeeds");
    assert_eq!(authorized.snapshot, expected_authorization.snapshot);
    assert_eq!(after_use.execution.as_ref(), Some(&authorized));
    assert_eq!(e2_after_use.as_ref(), Some(&authorized));
    assert_eq!(e1_after_use, e1_before_use);
    assert_eq!(e1_journal_after_use, e1_journal_before_use);
    assert_eq!(e2_journal_after_use.len(), e2_journal_before_use.len() + 1);
    assert_eq!(
        e2_journal_after_use.last(),
        Some(&JournalEntry::from(&authorized.snapshot))
    );
    assert_eq!(authorized.snapshot.revision, 2);
    assert_eq!(authorized.snapshot.owner_generation, next_owner);
    assert_eq!(authorized.snapshot.attempts_used, 0);
    assert!(authorized.snapshot.attempt_history.is_empty());
    assert_eq!(authorized.snapshot.tasks[0].status, TaskStatus::Ready);
    assert!(authorized.snapshot.tasks[0].approved);
    assert_eq!(
        authorized.snapshot.tasks[0]
            .interrupt
            .as_ref()
            .unwrap()
            .prompt,
        serde_json::Value::Null
    );
    assert_eq!(
        raw_after_use.tables.each_ref().map(Vec::len),
        [1, 1, 2, 9, 1, 0, 1, 1, 1]
    );
    assert_eq!(actual_use_resource_reads, 1);
    assert_eq!(actual_use_authority_calls, 1);
    assert_eq!(actual_use_gate_calls, 1);
    assert_eq!(actual_use_entries, 1);
    assert_eq!(actual_unused_captures, 0);
    assert_eq!(actual_unused_cancellation_entries, 0);
    assert!(final_use_usage.0 > 0 && final_use_usage.1 > 0);
    assert_eq!(initial_use_usage, (0, 0));
    let mut control = PermissionEvidenceBudget::new();
    let persisted_use = PermissionUse::decode_record(
        produced_text(&raw_after_use.tables[8][0][9]).as_bytes(),
        &mut control,
    )
    .unwrap();
    assert_eq!(persisted_use.use_id(), &use_id);
    assert_eq!(persisted_use.execution_id(), &second_execution);
    assert_eq!(persisted_use.task_id(), &task);
    assert_eq!(
        persisted_use.permission_id(),
        original_decision.permission_id()
    );
    assert_eq!(persisted_use.call_digest(), expected_call_digest);
    assert_eq!(raw_after_use.tables[4], raw_before_use.tables[4]);
    assert_eq!(raw_after_use.tables[5], raw_before_use.tables[5]);
    assert_eq!(raw_after_use.tables[6], raw_before_use.tables[6]);
    assert_eq!(raw_after_use.tables[7], raw_before_use.tables[7]);
    // R1 FirstFacts requires actual input <=31 bytes/5 nodes; distinct7-byte
    // changed keeps the original input shape while changing its call digest.
    assert_eq!(
        serde_json::to_vec(&authorized.snapshot.tasks[0].input)
            .unwrap()
            .len(),
        31
    );
    assert!(second_turn.as_str().len() <= 13);
    assert_ne!(expected_call_digest, original_call_digest);
    let before_dispatch = next.thread_view(&thread).unwrap().unwrap();
    let e1_before_dispatch = next.execution(&execution).unwrap();
    let e2_before_dispatch = next.execution(&second_execution).unwrap();
    let e1_journal_before_dispatch = next.journal(&execution).unwrap();
    let e2_journal_before_dispatch = next.journal(&second_execution).unwrap();
    let raw_before_dispatch = use_dispatch_raw_nine(&directory.0);
    let expected_resource = next
        .permission_resource(&second_execution, &mut control)
        .unwrap();
    assert_eq!(expected_resource.thread, before_dispatch.thread);
    assert_eq!(expected_resource.execution, authorized);
    assert!(
        use_dispatch_raw_nine(&directory.0) == raw_before_dispatch,
        "Resource control preserves all nine groups"
    );
    drop(control);
    let dispatch_authority_calls = Arc::new(AtomicUsize::new(0));
    let dispatch_gate_calls = Arc::new(AtomicUsize::new(0));
    let dispatch_captures = Arc::new(Mutex::new(Vec::new()));
    let dispatch_entries = Arc::new(AtomicUsize::new(0));
    let dispatch_application = Arc::new(
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
    let dispatch_application_weak = Arc::downgrade(&dispatch_application);
    let dispatch_context = InvocationContext {
        thread_id: before_dispatch.thread.id.clone(),
        thread_generation: before_dispatch.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: "application-1".into(),
        execution_revision: definition.revision.clone(),
        policy_revision: context.policy_revision.clone(),
    };
    let dispatch_catalog = Arc::new(
        PermissionCatalog::new(
            dispatch_application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: Arc::new(ReadyUseConstraint {
                    expected_context: dispatch_context,
                    expected_definition: definition.clone(),
                    expected_task: authorized.snapshot.tasks[0].clone(),
                    calls: dispatch_gate_calls.clone(),
                }),
            }],
        )
        .unwrap(),
    );
    let dispatch_catalog_weak = Arc::downgrade(&dispatch_catalog);
    let dispatch_authority = ReadyUseAuthority {
        expected_context: context.clone(),
        expected_resource,
        calls: dispatch_authority_calls.clone(),
    };
    let observed_dispatch = UseDispatchObservedStore {
        inner: next.clone(),
        captures: dispatch_captures.clone(),
        entries: dispatch_entries.clone(),
        outcome_control: None,
    };
    let attempt = AttemptId::new("release-attempt").unwrap();
    let expected_dispatch = transition(
        &authorized.snapshot,
        KernelCommand::Dispatch {
            task_id: task.clone(),
            attempt_id: attempt.clone(),
        },
        facts(108),
    )
    .unwrap();
    let dispatch_fence = ApprovalFence {
        execution: (&authorized.snapshot).into(),
        resource_revision: before_dispatch.thread.resource_revision,
    };
    let mut dispatch_account = PermissionEvidenceBudget::for_use_dispatch();
    let initial_dispatch_usage = produced_usage(&dispatch_account);
    let dispatch_result = prepare_permission_dispatch_claim(
        &observed_dispatch,
        &dispatch_authority,
        dispatch_catalog.as_ref(),
        &second_execution,
        dispatch_fence,
        &context,
        task.clone(),
        attempt.clone(),
        facts(108),
        &mut dispatch_account,
    );
    let final_dispatch_usage = produced_usage(&dispatch_account);
    // Every actual full AFTER/counter is captured before selecting the result.
    let after_dispatch = next.thread_view(&thread).unwrap().unwrap();
    let e1_after_dispatch = next.execution(&execution).unwrap();
    let e2_after_dispatch = next.execution(&second_execution).unwrap();
    let e1_journal_after_dispatch = next.journal(&execution).unwrap();
    let e2_journal_after_dispatch = next.journal(&second_execution).unwrap();
    let raw_after_dispatch = use_dispatch_raw_nine(&directory.0);
    let actual_dispatch_captures = dispatch_captures.lock().unwrap().clone();
    let actual_dispatch_entries = dispatch_entries.load(Ordering::SeqCst);
    let actual_dispatch_authorities = dispatch_authority_calls.load(Ordering::SeqCst);
    let actual_dispatch_gates = dispatch_gate_calls.load(Ordering::SeqCst);
    let actual_dispatch_code = dispatch_result
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let dispatch_accepted = dispatch_result.is_ok();
    // A real won claim, independently assembled current Claimed catalog and authority.
    let claim = dispatch_result.expect("qualified firstclaim producer must succeed");
    let claimed_execution = e2_after_dispatch.clone().unwrap();
    let renewal_resource = ApprovalResource {
        thread: after_dispatch.thread.clone(),
        execution: claimed_execution.clone(),
    };
    // Current ordinary authority accepts the actual current policy request.
    // Only fresh immutable Scope matching may reject reuse of old approval.
    let mut renewal_approval_context = context.clone();
    if scenario == RenewalScenario::PolicyRevisionChanged {
        renewal_approval_context.policy_revision = "policy-2".into();
    }
    let current_application_revision = if scenario == RenewalScenario::ApplicationRevisionChanged {
        "application-2"
    } else {
        "application-1"
    };
    let current_constraint_revision = if scenario == RenewalScenario::ConstraintRevisionChanged {
        "constraint-2"
    } else {
        "constraint-1"
    };
    let renewal_context = InvocationContext {
        thread_id: after_dispatch.thread.id.clone(),
        thread_generation: after_dispatch.thread.generation.clone(),
        origin: context.actor.clone(),
        application_revision: current_application_revision.into(),
        execution_revision: definition.revision.clone(),
        policy_revision: renewal_approval_context.policy_revision.clone(),
    };
    let claimed_task = claimed_execution.snapshot.tasks[0].clone();
    let renewal_invocation = EffectInvocation {
        execution_id: second_execution.clone(),
        turn_id: claimed_execution.snapshot.turn_id.clone(),
        owner_generation: claimed_execution.snapshot.owner_generation,
        task_id: task.clone(),
        effect_id: claimed_task.spec.effect_id.clone(),
        attempt_id: attempt.clone(),
        input: claimed_task.input.clone(),
    };
    let renewal_authority_calls = Arc::new(AtomicUsize::new(0));
    let renewal_gate_calls = Arc::new(AtomicUsize::new(0));
    let renewal_authority_matched_calls = Arc::new(AtomicUsize::new(0));
    let renewal_gate_matched_calls = Arc::new(AtomicUsize::new(0));
    let renewal_captures = Arc::new(Mutex::new(Vec::new()));
    let renewal_entries = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_matched_calls = Arc::new(AtomicUsize::new(0));
    let renewal_application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: context.actor.application.clone(),
                revision: current_application_revision.into(),
            },
            vec![RouteRegistration {
                definition: definition.clone(),
                effects: BTreeMap::from([(
                    claimed_task.spec.effect_id.clone(),
                    EffectBinding {
                        callback: Arc::new(UseCallbackOracle {
                            expected_invocation: renewal_invocation.clone(),
                            expected_context: renewal_context.clone(),
                            calls: callback_calls.clone(),
                            matched_calls: callback_matched_calls.clone(),
                            returned_failure: outcome_scenario == OutcomeScenario::ReturnedFailed,
                        }),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: None,
            }],
        )
        .unwrap(),
    );
    let renewal_application_weak = Arc::downgrade(&renewal_application);
    let renewal_catalog = Arc::new(
        PermissionCatalog::new(
            renewal_application.clone(),
            vec![ToolBindingRegistration {
                definition: definition.clone(),
                effect_id: claimed_task.spec.effect_id.clone(),
                tool_id: StableToolId::new("release-tool").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new(current_constraint_revision)
                    .unwrap(),
                constraint: Arc::new(UseRenewalConstraint {
                    deny: scenario == RenewalScenario::ConstraintDenied,
                    expected_application_revision: current_application_revision.into(),
                    expected_constraint_revision: current_constraint_revision.into(),
                    expected_context: renewal_context,
                    expected_definition: definition.clone(),
                    expected_task: claimed_task,
                    expected_invocation: renewal_invocation,
                    calls: renewal_gate_calls.clone(),
                    matched_calls: renewal_gate_matched_calls.clone(),
                }),
            }],
        )
        .unwrap(),
    );
    let renewal_catalog_weak = Arc::downgrade(&renewal_catalog);
    let renewal_authority = UseRenewalAuthority {
        deny: matches!(
            scenario,
            RenewalScenario::AuthorityDenied | RenewalScenario::ExportRefusedAccount
        ),
        expected_context: renewal_approval_context.clone(),
        expected_resource: renewal_resource,
        calls: renewal_authority_calls.clone(),
        matched_calls: renewal_authority_matched_calls.clone(),
        trace: renewal_captures.clone(),
    };
    let foreign_directory =
        (outcome_scenario == OutcomeScenario::ForeignPhysicalStore).then(Directory::new);
    let outcome_backend = Arc::new(OutcomeBackendControl {
        scenario: outcome_scenario,
        path: directory.0.clone(),
        thread: thread.clone(),
        original: execution.clone(),
        current: second_execution.clone(),
        foreign: foreign_directory
            .as_ref()
            .map(|directory| Arc::new(SqliteStore::open(&directory.0).unwrap())),
        foreign_path: foreign_directory
            .as_ref()
            .map(|directory| directory.0.clone()),
        observations: Mutex::new(Vec::new()),
        forwards: AtomicUsize::new(0),
        callback_calls: callback_calls.clone(),
    });
    let observed_renewal = UseDispatchObservedStore {
        inner: next.clone(),
        captures: renewal_captures.clone(),
        entries: renewal_entries.clone(),
        outcome_control: Some(outcome_backend.clone()),
    };
    let fresh_export = if scenario == RenewalScenario::ExportFreshAccount {
        Some(PermissionUseRenewalAccount::for_claim(&claim).unwrap())
    } else {
        None
    };
    let mut renewal_account = supplied_account.unwrap_or_else(checked_account_constructor(|| {
        PermissionUseRenewalAccount::for_claim(&claim).unwrap()
    }));
    let initial_renewal_usage = renewal_account.usage();
    let renewal_result = renew_permission_use_dispatch(
        claim,
        &observed_renewal,
        &renewal_authority,
        renewal_catalog.as_ref(),
        &renewal_approval_context,
        &mut renewal_account,
    );
    // Full AFTER, usage and actual call observations precede result selection and cleanup.
    let after_renewal = next.thread_view(&thread).unwrap().unwrap();
    let e1_after_renewal = next.execution(&execution).unwrap();
    let e2_after_renewal = next.execution(&second_execution).unwrap();
    let e1_journal_after_renewal = next.journal(&execution).unwrap();
    let e2_journal_after_renewal = next.journal(&second_execution).unwrap();
    let raw_after_renewal = use_dispatch_raw_nine(&directory.0);
    let renewal_usage = renewal_account.usage();
    let actual_renewal_captures = renewal_captures.lock().unwrap().clone();
    let actual_renewal_authorities = renewal_authority_calls.load(Ordering::SeqCst);
    let actual_renewal_gates = renewal_gate_calls.load(Ordering::SeqCst);
    let actual_renewal_authority_matched = renewal_authority_matched_calls.load(Ordering::SeqCst);
    let actual_renewal_gate_matched = renewal_gate_matched_calls.load(Ordering::SeqCst);
    let actual_renewal_entries = renewal_entries.load(Ordering::SeqCst);
    let actual_renewal_code = renewal_result
        .as_ref()
        .err()
        .map(PermissionError::code)
        .unwrap_or("ok");
    let renewal_accepted = renewal_result.is_ok();
    let renewal_unauthorized = matches!(
        &renewal_result,
        Err(PermissionError::Approval(ApprovalError::Unauthorized))
    );
    let renewal_constraint_denied =
        matches!(&renewal_result, Err(PermissionError::ConstraintDenied));
    // Consume the actual successful renewal. No raw Claim/Permit factory exists.
    let callback_receipt = match renewal_result {
        Ok(permit) => Some(permit.into_callback_work().invoke()),
        Err(error) => {
            drop(error);
            None
        }
    };
    let callback_status = callback_receipt.as_ref().map(checked_callback_observation(
        PermissionUseCallbackReceipt::status,
    ));
    let callback_usage = callback_receipt.as_ref().map(checked_callback_observation(
        PermissionUseCallbackReceipt::usage,
    ));
    let callback_settled_matches =
        outcome_scenario.matches_outcome(callback_receipt.as_ref().and_then(
            checked_callback_observation(PermissionUseCallbackReceipt::settled_outcome),
        ));

    let callback_owners_alive = callback_receipt.is_some()
        && renewal_application_weak.upgrade().is_some()
        && renewal_catalog_weak.upgrade().is_some()
        && Arc::strong_count(&next) > 1;
    let actual_callback_calls = callback_calls.load(Ordering::SeqCst);
    let actual_callback_matched_calls = callback_matched_calls.load(Ordering::SeqCst);
    let outcome_before = next.thread_view(&thread).unwrap().unwrap();
    let outcome_e1_before = next.execution(&execution).unwrap();
    let outcome_e2_before = next.execution(&second_execution).unwrap();
    let outcome_j1_before = next.journal(&execution).unwrap();
    let outcome_j2_before = next.journal(&second_execution).unwrap();
    let outcome_raw_before = use_dispatch_raw_nine(&directory.0);
    let mut outcome_work = callback_receipt
        .expect("genuine renewal must produce actual receipt")
        .into_outcome_work();
    let outcome_initial_usage = outcome_work.usage();
    let mut control_observations = Vec::new();
    let original_owner_cell = outcome_raw_before.tables[0][0][3].clone();
    let original_resource_cell = outcome_raw_before.tables[1][0][5].clone();
    // Controlled external corruption, not a supported stale-resource transition.
    // The legacy raw cancellation path intentionally refuses this permission target.
    if outcome_scenario == OutcomeScenario::ChangedResourceRevision {
        let connection = outcome_injection_connection(&directory.0);
        assert_eq!(
            connection
                .execute(
                    "UPDATE threads SET resource_revision=?1 WHERE id=?2",
                    (
                        outcome_before
                            .thread
                            .resource_revision
                            .checked_add(1)
                            .unwrap()
                            .to_be_bytes()
                            .as_slice(),
                        thread.as_str()
                    )
                )
                .unwrap(),
            1
        );
        connection.close().unwrap();
    }
    let source_task_before = if outcome_scenario == OutcomeScenario::ChangedSourceHeader {
        let connection = outcome_injection_connection(&directory.0);
        let old: String = connection
            .query_row(
                "SELECT task_id FROM permission_uses WHERE execution_id=?1",
                [second_execution.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(connection.execute("UPDATE permission_uses SET task_id='outcome-corrupt-task' WHERE execution_id=?1", [second_execution.as_str()]).unwrap(), 1);
        connection.close().unwrap();
        Some(old)
    } else {
        None
    };
    if outcome_scenario == OutcomeScenario::CorruptOwnerMetadata {
        let connection = outcome_injection_connection(&directory.0);
        assert_eq!(
            connection
                .execute(
                    "UPDATE metadata SET owner_epoch=?1 WHERE id=1",
                    [next_owner.checked_add(1).unwrap().to_be_bytes().as_slice()]
                )
                .unwrap(),
            1
        );
        connection.close().unwrap();
    }
    let trigger_installed = if outcome_scenario == OutcomeScenario::JournalAbortThenRetry {
        install_outcome_abort(
            &directory.0,
            &second_execution,
            outcome_e2_before
                .as_ref()
                .unwrap()
                .snapshot
                .revision
                .checked_add(1)
                .unwrap(),
            outcome_before
                .thread
                .resource_revision
                .checked_add(1)
                .unwrap(),
        )
    } else {
        false
    };
    let control_before =
        OutcomeCapture::read(&next, &directory.0, &thread, &execution, &second_execution);
    let first_result = outcome_work.commit(&facts(109));
    let control_after =
        OutcomeCapture::read(&next, &directory.0, &thread, &execution, &second_execution);
    let first_usage = outcome_work.usage();
    let settlement_result = if outcome_scenario == OutcomeScenario::JournalAbortThenRetry {
        control_observations.push(OutcomeControlObservation::new(
            control_before,
            control_after,
            &first_result,
            OutcomeRefusal::LateJournalAbort,
            Some((outcome_initial_usage, first_usage)),
            callback_calls.load(Ordering::SeqCst),
        ));
        let connection = outcome_injection_connection(&directory.0);
        connection
            .execute_batch("DROP TRIGGER outcome_test_abort_journal")
            .unwrap();
        let removed: i64 = connection.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='outcome_test_abort_journal'", [], |row| row.get(0)).unwrap();
        assert_eq!(removed, 0);
        connection.close().unwrap();
        let after_drop =
            OutcomeCapture::read(&next, &directory.0, &thread, &execution, &second_execution);
        assert!(after_drop == control_observations.last().unwrap().after);
        outcome_work.commit(&facts(109))
    } else {
        if outcome_scenario.refusal().is_some() {
            control_observations.push(OutcomeControlObservation::new(
                control_before,
                control_after,
                &first_result,
                outcome_scenario.refusal().unwrap(),
                Some((outcome_initial_usage, first_usage)),
                callback_calls.load(Ordering::SeqCst),
            ));
        }
        first_result
    };
    // Restore literal corrupt cells before healthy readiness/reopen. The saved
    // tampered BEFORE/AFTER captures above separately prove settlement no-write.
    if let Some(original_task) = source_task_before {
        let connection = outcome_injection_connection(&directory.0);
        assert_eq!(connection.execute("UPDATE permission_uses SET task_id=?1 WHERE execution_id=?2 AND task_id='outcome-corrupt-task'", (original_task, second_execution.as_str())).unwrap(), 1);
        connection.close().unwrap();
    }
    if outcome_scenario == OutcomeScenario::ChangedResourceRevision {
        let connection = outcome_injection_connection(&directory.0);
        assert_eq!(
            connection
                .execute(
                    "UPDATE threads SET resource_revision=?1 WHERE id=?2",
                    (&original_resource_cell, thread.as_str())
                )
                .unwrap(),
            1
        );
        connection.close().unwrap();
    }
    if outcome_scenario == OutcomeScenario::CorruptOwnerMetadata {
        let connection = outcome_injection_connection(&directory.0);
        assert_eq!(
            connection
                .execute(
                    "UPDATE metadata SET owner_epoch=?1 WHERE id=1",
                    [&original_owner_cell]
                )
                .unwrap(),
            1
        );
        connection.close().unwrap();
    }
    // Keep the genuine Commit in settlement_result through normal owner cleanup.
    // Repeating completed Work must stop before the retained Store port.
    let before_repeat_entries = renewal_entries.load(Ordering::SeqCst);
    if settlement_result.is_ok() {
        let before =
            OutcomeCapture::read(&next, &directory.0, &thread, &execution, &second_execution);
        let usage = outcome_work.usage();
        let repeated = outcome_work.commit(&facts(109));
        let after =
            OutcomeCapture::read(&next, &directory.0, &thread, &execution, &second_execution);
        control_observations.push(OutcomeControlObservation::new(
            before,
            after,
            &repeated,
            OutcomeRefusal::Completed,
            Some((usage, outcome_work.usage())),
            callback_calls.load(Ordering::SeqCst),
        ));
    }
    let after_repeat_entries = renewal_entries.load(Ordering::SeqCst);
    let settlement_code = settlement_result
        .as_ref()
        .err()
        .map(checked_outcome_code(PermissionError::code))
        .unwrap_or("ok");
    let outcome_usage = outcome_work.usage();
    let outcome_port_entries = renewal_entries.load(Ordering::SeqCst);
    let outcome_status = outcome_work.callback_status();
    let outcome_matches = outcome_scenario.matches_outcome(outcome_work.settled_outcome());
    let outcome_owners_alive = renewal_application_weak.upgrade().is_some()
        && renewal_catalog_weak.upgrade().is_some()
        && Arc::strong_count(&next) > 1;
    let outcome_after = next.thread_view(&thread).unwrap().unwrap();
    let outcome_e1_after = next.execution(&execution).unwrap();
    let outcome_e2_after = next.execution(&second_execution).unwrap();
    let outcome_j1_after = next.journal(&execution).unwrap();
    let outcome_j2_after = next.journal(&second_execution).unwrap();
    let outcome_raw_after = use_dispatch_raw_nine(&directory.0);
    let outcome_callback_calls = callback_calls.load(Ordering::SeqCst);
    let outcome_callback_matches = callback_matched_calls.load(Ordering::SeqCst);
    let outcome_authority_calls = renewal_authority_calls.load(Ordering::SeqCst);
    let outcome_gate_calls = renewal_gate_calls.load(Ordering::SeqCst);
    drop(outcome_work);
    drop(callback_calls);
    drop(callback_matched_calls);
    // Account carries prepaid resource-key owners and origin/operation nonces.
    // It never borrows Claim, catalog, native application, store or authority.
    let returned_account = if matches!(
        scenario,
        RenewalScenario::ExportFinishedAccount | RenewalScenario::ExportRefusedAccount
    ) {
        Some(renewal_account)
    } else {
        drop(renewal_account);
        fresh_export
    };
    drop(observed_renewal);
    let backend_forwards = outcome_backend.forwards.load(Ordering::SeqCst);
    let backend_observations = std::mem::take(&mut *outcome_backend.observations.lock().unwrap());
    if let Some(foreign) = &outcome_backend.foreign {
        assert_eq!(
            Arc::strong_count(foreign),
            1,
            "foreign Store has one closing owner"
        );
        foreign.readiness().unwrap();
    }
    drop(outcome_backend);
    let foreign_removed = foreign_directory
        .map(|directory| {
            let path = directory.0.clone();
            drop(directory);
            !path.exists()
        })
        .unwrap_or(true);
    drop(renewal_authority);
    drop(renewal_catalog);
    drop(renewal_application);
    drop(dispatch_account);
    drop(observed_dispatch);
    drop(dispatch_authority);
    drop(dispatch_catalog);
    drop(dispatch_application);
    let renewal_catalog_retired = renewal_catalog_weak.upgrade().is_none();
    let dispatch_aliases_retired =
        dispatch_catalog_weak.upgrade().is_none() && dispatch_application_weak.upgrade().is_none();
    let renewal_application_retired = renewal_application_weak.upgrade().is_none();
    next.readiness().unwrap();
    assert_eq!(
        Arc::strong_count(&next),
        1,
        "every live owner2 alias retires normally"
    );
    drop(next);
    let persisted = SqliteStore::open(&directory.0).unwrap();
    let persisted_owner = persisted.owner_generation();
    let persisted_view = persisted.thread_view(&thread).unwrap().unwrap();
    let persisted_e1 = persisted.execution(&execution).unwrap();
    let persisted_e2 = persisted.execution(&second_execution).unwrap();
    let persisted_e1_journal = persisted.journal(&execution).unwrap();
    let persisted_e2_journal = persisted.journal(&second_execution).unwrap();
    let persisted_raw = use_dispatch_raw_nine(&directory.0);
    persisted.readiness().unwrap();
    drop(persisted);
    let actual_native_calls = calls.load(Ordering::SeqCst);
    drop(calls);
    let removed = directory.0.clone();
    drop(directory);
    let directory_removed = !removed.exists();
    println!("permission-use dispatch evidence: code={actual_dispatch_code}; captures={actual_dispatch_captures:?}; authorities={actual_dispatch_authorities}; gates={actual_dispatch_gates}; commits={actual_dispatch_entries}; usage={final_dispatch_usage:?}; owners={original_owner},{next_owner},{persisted_owner}; native={actual_native_calls}; directory_removed={directory_removed}");
    assert!(directory_removed);
    assert!(dispatch_aliases_retired);
    assert_eq!(initial_dispatch_usage, (0, 0));
    assert!(
        final_dispatch_usage.0 > 0
            && final_dispatch_usage.1 > 0
            && final_dispatch_usage.0 <= 16_777_216
            && final_dispatch_usage.1 <= 65_536
    );
    assert_eq!((original_owner, next_owner, persisted_owner), (1, 2, 3));
    assert_eq!(actual_native_calls, 0);
    assert_eq!(persisted_view, outcome_after);
    assert_eq!(persisted_e1, outcome_e1_after);
    assert_eq!(persisted_e2, outcome_e2_after);
    assert_eq!(persisted_e1_journal, outcome_j1_after);
    assert_eq!(persisted_e2_journal, outcome_j2_after);
    assert_eq!(persisted_raw.tables[1..], outcome_raw_after.tables[1..]);
    assert_eq!(
        persisted_raw.tables[0][0][..3],
        outcome_raw_after.tables[0][0][..3]
    );
    assert_eq!(
        produced_u64(&persisted_raw.tables[0][0][3]),
        persisted_owner
    );
    assert_eq!(
        produced_u64(&raw_after_dispatch.tables[0][0][3]),
        next_owner
    );
    if !dispatch_accepted {
        // Exact reached C1 profile refusal, with all three A/D/U ranges
        // corroborated by real headers [0,1,1] and reviewed capture Source.
        assert_eq!(actual_dispatch_code, "permission_unsupported_profile");
        assert_eq!(
            actual_dispatch_captures,
            ["ok", "permission_unsupported_profile"]
        );
        assert_eq!(actual_dispatch_authorities, 1);
        assert_eq!(actual_dispatch_gates, 0);
        assert_eq!(actual_dispatch_entries, 0);
        assert_eq!(after_dispatch, before_dispatch);
        assert_eq!(e1_after_dispatch, e1_before_dispatch);
        assert_eq!(e2_after_dispatch, e2_before_dispatch);
        assert_eq!(e1_journal_after_dispatch, e1_journal_before_dispatch);
        assert_eq!(e2_journal_after_dispatch, e2_journal_before_dispatch);
        assert!(
            raw_after_dispatch == raw_before_dispatch,
            "refused dispatch preserves all nine groups"
        );
    }
    // The first feature primary follows actual normal owner3 close and removal.
    assert!(dispatch_accepted, "real PermissionUse Ready must durably claim its fresh attempt without a new human; actual={actual_dispatch_code}");
    assert_eq!(actual_dispatch_code, "ok");
    assert_eq!(actual_dispatch_authorities, 1);
    assert_eq!(actual_dispatch_gates, 1);
    assert_eq!(actual_dispatch_captures, ["ok", "ok", "ok"]);
    assert_eq!(actual_dispatch_entries, 1);
    let e2_claimed = e2_after_dispatch.clone().unwrap();
    assert_eq!(e2_claimed.snapshot, expected_dispatch.snapshot);
    assert_eq!(e2_claimed.snapshot.revision, 3);
    assert_eq!(e2_claimed.snapshot.attempts_used, 1);
    assert_eq!(e2_claimed.snapshot.tasks[0].status, TaskStatus::InFlight);
    assert_eq!(
        e2_claimed.snapshot.tasks[0].attempt_id.as_ref(),
        Some(&attempt)
    );
    assert_eq!(after_dispatch.execution.as_ref(), Some(&e2_claimed));
    assert_eq!(e1_after_dispatch, e1_before_dispatch);
    assert_eq!(e1_journal_after_dispatch, e1_journal_before_dispatch);
    let mut expected_thread = before_dispatch.thread.clone();
    expected_thread.resource_revision += 1;
    assert_eq!(after_dispatch.thread, expected_thread);
    assert_eq!(
        e2_journal_after_dispatch.len(),
        e2_journal_before_dispatch.len() + 1
    );
    assert_eq!(
        &e2_journal_after_dispatch[..e2_journal_before_dispatch.len()],
        e2_journal_before_dispatch.as_slice()
    );
    assert_eq!(
        e2_journal_after_dispatch.last(),
        Some(&JournalEntry::from(&expected_dispatch.snapshot))
    );
    assert_eq!(
        raw_after_dispatch.tables.each_ref().map(Vec::len),
        [1, 1, 2, 10, 1, 0, 1, 1, 1]
    );
    for index in [0, 4, 5, 6, 7, 8] {
        assert_eq!(
            raw_after_dispatch.tables[index],
            raw_before_dispatch.tables[index]
        );
    }
    assert_eq!(
        raw_after_dispatch.tables[2][0],
        raw_before_dispatch.tables[2][0]
    );
    assert_eq!(
        &raw_after_dispatch.tables[3][..9],
        raw_before_dispatch.tables[3].as_slice()
    );
    let row = &raw_after_dispatch.tables[3][9];
    assert!(
        first_claim_journal_sequence(&row[0])
            > first_claim_journal_sequence(&raw_before_dispatch.tables[3][8][0])
    );
    assert_eq!(produced_text(&row[1]), "commit");
    assert_eq!(produced_text(&row[2]), thread.as_str());
    assert_eq!(produced_text(&row[3]), second_execution.as_str());
    assert_eq!(produced_u64(&row[4]), 3);
    assert_eq!(produced_u64(&row[5]), next_owner);
    assert_eq!(
        produced_text(&row[6]),
        serde_json::to_string(&ExecutionStatus::Running).unwrap()
    );
    assert_eq!(produced_u64(&row[7]), 108);
    assert_eq!(
        serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
        JournalEntry::from(&expected_dispatch.snapshot)
    );
    println!("permission-use renewal evidence: code={actual_renewal_code}; captures={actual_renewal_captures:?}; authorities={actual_renewal_authorities}; gates={actual_renewal_gates}; writes={actual_renewal_entries}; usage=({}, {}); native={actual_native_calls}; directory_removed={directory_removed}", renewal_usage.encoded_bytes(), renewal_usage.structural_nodes());
    assert!(renewal_catalog_retired && renewal_application_retired);
    assert_eq!(after_renewal, after_dispatch);
    assert_eq!(e1_after_renewal, e1_after_dispatch);
    assert_eq!(e2_after_renewal, e2_after_dispatch);
    assert_eq!(e1_journal_after_renewal, e1_journal_after_dispatch);
    assert_eq!(e2_journal_after_renewal, e2_journal_after_dispatch);
    assert!(raw_after_renewal == raw_after_dispatch);
    assert!(renewal_usage.encoded_bytes() > 0 && renewal_usage.structural_nodes() > 0);
    assert_eq!(actual_renewal_entries, 0);
    let account_refusal = matches!(
        scenario,
        RenewalScenario::ForeignAccountOrigin
            | RenewalScenario::ReusedFinishedAccount
            | RenewalScenario::ReusedRefusedAccount
    );
    let expected_authorities = usize::from(!account_refusal);
    assert_eq!(actual_renewal_authorities, expected_authorities);
    assert_eq!(actual_native_calls, 0);
    assert_eq!(
        actual_renewal_authority_matched, expected_authorities,
        "ordinary authority sees exact Dispatch context and full Resource"
    );
    assert_eq!(actual_renewal_gate_matched, actual_renewal_gates, "every entered gate sees the exact Claimed invocation, fresh context, task, definition and binding before deliberate denial");
    // Denial primaries follow all nine persisted groups and normal owner cleanup.
    match scenario {
        RenewalScenario::AuthorityDenied | RenewalScenario::ExportRefusedAccount => {
            assert!(!renewal_accepted && renewal_unauthorized);
            assert_eq!(actual_renewal_code, "permission_approval_failed");
            assert_eq!(actual_renewal_captures, ["resource", "ok", "authority"]);
            assert_eq!(actual_renewal_gates, 0);
            return returned_account;
        }
        RenewalScenario::ConstraintDenied => {
            assert!(!renewal_accepted && renewal_constraint_denied);
            assert_eq!(actual_renewal_code, "permission_constraint_denied");
            assert_eq!(
                actual_renewal_captures,
                ["resource", "ok", "authority", "source", "ok"]
            );
            assert_eq!(actual_renewal_gates, 1);
            return returned_account;
        }
        RenewalScenario::PolicyRevisionChanged
        | RenewalScenario::ApplicationRevisionChanged
        | RenewalScenario::ConstraintRevisionChanged => {
            assert!(!renewal_accepted);
            assert_eq!(actual_renewal_code, "permission_unsupported_profile");
            assert_eq!(
                actual_renewal_captures,
                [
                    "resource",
                    "ok",
                    "authority",
                    "source",
                    "permission_unsupported_profile"
                ]
            );
            assert_eq!(actual_renewal_gates, 0);
            return returned_account;
        }
        RenewalScenario::ForeignAccountOrigin => {
            assert!(!renewal_accepted);
            assert_eq!(actual_renewal_code, "permission_unsupported_profile");
            assert!(actual_renewal_captures.is_empty());
            assert_eq!(actual_renewal_gates, 0);
            assert!(renewal_usage.encoded_bytes() > initial_renewal_usage.encoded_bytes());
            assert!(renewal_usage.structural_nodes() > initial_renewal_usage.structural_nodes());
            assert!(returned_account.is_none());
            return None;
        }
        RenewalScenario::ReusedFinishedAccount | RenewalScenario::ReusedRefusedAccount => {
            assert!(!renewal_accepted);
            assert_eq!(actual_renewal_code, "permission_unsupported_profile");
            assert!(actual_renewal_captures.is_empty());
            assert_eq!(actual_renewal_gates, 0);
            assert_eq!(
                renewal_usage.encoded_bytes(),
                initial_renewal_usage.encoded_bytes()
            );
            assert_eq!(
                renewal_usage.structural_nodes(),
                initial_renewal_usage.structural_nodes()
            );
            assert!(returned_account.is_none());
            return None;
        }
        RenewalScenario::Accepted
        | RenewalScenario::ExportFreshAccount
        | RenewalScenario::ExportFinishedAccount => {}
    }
    if !renewal_accepted {
        assert_eq!(actual_renewal_code, "permission_unsupported_profile");
        assert_eq!(
            actual_renewal_captures,
            [
                "resource",
                "ok",
                "authority",
                "source",
                "permission_unsupported_profile"
            ]
        );
        assert_eq!(actual_renewal_gates, 0);
    }
    // Intended feature primary follows real R3/authority/C2 and healthy owner3 cleanup.
    assert!(renewal_accepted, "genuine claimed PermissionUse needs complete provenance renewal before an owned Permit; actual={actual_renewal_code}");
    assert_eq!(
        actual_renewal_captures,
        [
            "resource",
            "ok",
            "authority",
            "source",
            "ok",
            "resource",
            "ok"
        ]
    );
    assert_eq!(actual_renewal_gates, 1);
    println!("permission-use callback evidence: status={callback_status:?}; calls={actual_callback_calls}; matched={actual_callback_matched_calls}; owners_alive={callback_owners_alive}; bounded_match={callback_settled_matches}; usage=({}, {}); owners={original_owner},{next_owner},{persisted_owner}; directory_removed={directory_removed}", callback_usage.map(checked_callback_observation(|usage: PermissionEvidenceUsage| usage.encoded_bytes())).unwrap_or(0), callback_usage.map(checked_callback_observation(|usage: PermissionEvidenceUsage| usage.structural_nodes())).unwrap_or(0));
    assert!(callback_owners_alive);
    // Callback primaries follow all old typed/raw checks and normal retirement.
    assert_eq!(
        callback_status,
        Some(PermissionUseCallbackStatus::Settled),
        "a genuinely renewed permit must reach the exact direct callback"
    );
    assert_eq!(actual_callback_calls, 1);
    assert_eq!(actual_callback_matched_calls, 1);
    assert_eq!(outcome_callback_calls, 1);
    assert_eq!(outcome_callback_matches, 1);
    assert_eq!(outcome_authority_calls, actual_renewal_authorities);
    assert_eq!(outcome_gate_calls, actual_renewal_gates);
    assert!(callback_settled_matches);
    assert_eq!(outcome_initial_usage.encoded_bytes(), 0);
    assert_eq!(outcome_initial_usage.structural_nodes(), 0);
    assert!(
        outcome_usage.encoded_bytes() > 0
            && outcome_usage.encoded_bytes() <= 4 * 1024 * 1024 * 1024
    );
    assert!(
        outcome_usage.structural_nodes() > 0
            && outcome_usage.structural_nodes() <= 16 * 1024 * 1024
    );
    assert_eq!(outcome_status, PermissionUseCallbackStatus::Settled);
    assert!(outcome_matches && outcome_owners_alive);
    assert!(renewal_catalog_retired && renewal_application_retired);
    if settlement_result.is_err() {
        let refusal = outcome_scenario
            .refusal()
            .expect("only selected refusal scenarios may refuse");
        assert!(refusal.matches(settlement_result.as_ref().err().unwrap()));
        assert_eq!(settlement_code, refusal.code());
        assert_eq!(outcome_after, outcome_before);
        assert_eq!(outcome_e1_after, outcome_e1_before);
        assert_eq!(outcome_e2_after, outcome_e2_before);
        assert_eq!(outcome_j1_after, outcome_j1_before);
        assert_eq!(outcome_j2_after, outcome_j2_before);
        assert!(outcome_raw_after == outcome_raw_before);
    } else {
        // Future GREEN-only independent oracle. This expected plan is never
        // passed to Work or to the backend and supplies no production authority.
        let claimed = outcome_e2_before.as_ref().unwrap();
        let expected_complete = transition(
            &claimed.snapshot,
            if outcome_scenario == OutcomeScenario::ReturnedFailed {
                KernelCommand::FailAttempt {
                    task_id: task.clone(),
                    attempt_id: attempt.clone(),
                    failure: OUTCOME_RETURNED_FAILURE.to_owned(),
                }
            } else {
                KernelCommand::Complete {
                    task_id: task.clone(),
                    attempt_id: attempt.clone(),
                    result: TaskResult {
                        output: serde_json::Value::Bool(true),
                        writes: Vec::new(),
                    },
                }
            },
            facts(109),
        )
        .unwrap();
        let mut expected_execution = claimed.clone();
        expected_execution.snapshot = expected_complete.snapshot;
        let mut expected_thread = outcome_before.thread.clone();
        expected_thread.resource_revision =
            expected_thread.resource_revision.checked_add(1).unwrap();
        let expected_journal = JournalEntry::from(&expected_execution.snapshot);
        assert_eq!(settlement_code, "ok");
        assert_eq!(outcome_after.thread, expected_thread);
        assert_eq!(outcome_after.execution.as_ref(), Some(&expected_execution));
        assert_eq!(outcome_e2_after.as_ref(), Some(&expected_execution));
        assert_eq!(outcome_e1_after, outcome_e1_before);
        assert_eq!(outcome_j1_after, outcome_j1_before);
        assert_eq!(outcome_j2_after.len(), outcome_j2_before.len() + 1);
        assert_eq!(
            &outcome_j2_after[..outcome_j2_before.len()],
            outcome_j2_before.as_slice()
        );
        assert_eq!(outcome_j2_after.last(), Some(&expected_journal));
        for index in [0, 4, 5, 6, 7, 8] {
            assert_eq!(
                outcome_raw_after.tables[index],
                outcome_raw_before.tables[index]
            );
        }
        assert_eq!(
            outcome_raw_after.tables[1][0][..5],
            outcome_raw_before.tables[1][0][..5]
        );
        assert_eq!(
            produced_u64(&outcome_raw_after.tables[1][0][5]),
            expected_thread.resource_revision
        );
        assert_eq!(
            outcome_raw_after.tables[1][0][6],
            outcome_raw_before.tables[1][0][6]
        );
        assert_eq!(
            outcome_raw_after.tables[2][0],
            outcome_raw_before.tables[2][0]
        );
        let row = &outcome_raw_after.tables[2][1];
        assert_eq!(row[..2], outcome_raw_before.tables[2][1][..2]);
        assert_eq!(produced_u64(&row[2]), expected_execution.snapshot.revision);
        assert_eq!(produced_u64(&row[3]), next_owner);
        assert_eq!(
            serde_json::from_str::<Snapshot>(produced_text(&row[4])).unwrap(),
            expected_execution.snapshot
        );
        let prefix = outcome_raw_before.tables[3].len();
        assert_eq!(outcome_raw_after.tables[3].len(), prefix + 1);
        assert_eq!(
            &outcome_raw_after.tables[3][..prefix],
            outcome_raw_before.tables[3].as_slice()
        );
        let row = &outcome_raw_after.tables[3][prefix];
        assert!(
            first_claim_journal_sequence(&row[0])
                > first_claim_journal_sequence(&outcome_raw_before.tables[3][prefix - 1][0])
        );
        assert_eq!(produced_text(&row[1]), "commit");
        assert_eq!(produced_text(&row[2]), thread.as_str());
        assert_eq!(produced_text(&row[3]), second_execution.as_str());
        assert_eq!(produced_u64(&row[4]), expected_journal.revision);
        assert_eq!(produced_u64(&row[5]), next_owner);
        assert_eq!(
            produced_text(&row[6]),
            serde_json::to_string(&expected_journal.status).unwrap()
        );
        assert_eq!(produced_u64(&row[7]), expected_journal.updated_at_ms);
        assert_eq!(
            serde_json::from_str::<JournalEntry>(produced_text(&row[8])).unwrap(),
            expected_journal
        );
        assert_eq!(
            outcome_raw_after.tables.each_ref().map(Vec::len),
            [1, 1, 2, 11, 1, 0, 1, 1, 1]
        );
    }
    assert_eq!(persisted_view, outcome_after);
    assert_eq!(persisted_e1, outcome_e1_after);
    assert_eq!(persisted_e2, outcome_e2_after);
    assert_eq!(persisted_e1_journal, outcome_j1_after);
    assert_eq!(persisted_e2_journal, outcome_j2_after);
    assert_eq!(persisted_raw.tables[1..], outcome_raw_after.tables[1..]);
    let expected_entries = if outcome_scenario == OutcomeScenario::JournalAbortThenRetry {
        2
    } else {
        1
    };
    assert_eq!(outcome_port_entries, expected_entries);
    assert_eq!(before_repeat_entries, after_repeat_entries);
    assert_eq!(
        backend_forwards,
        expected_entries + usize::from(outcome_scenario == OutcomeScenario::ReplayActualRequest)
    );
    assert!(foreign_removed);
    assert_eq!(
        trigger_installed,
        outcome_scenario == OutcomeScenario::JournalAbortThenRetry
    );
    for observation in control_observations
        .iter()
        .chain(backend_observations.iter())
    {
        observation.assert_refusal();
    }
    let expected_controls = usize::from(outcome_scenario.refusal().is_some())
        + usize::from(outcome_scenario == OutcomeScenario::JournalAbortThenRetry)
        + usize::from(settlement_result.is_ok());
    assert_eq!(control_observations.len(), expected_controls);
    assert_eq!(
        backend_observations.len(),
        usize::from(matches!(
            outcome_scenario,
            OutcomeScenario::ReplayActualRequest | OutcomeScenario::ForeignPhysicalStore
        ))
    );
    if outcome_scenario == OutcomeScenario::JournalAbortThenRetry {
        assert!(outcome_usage.encoded_bytes() > first_usage.encoded_bytes());
        assert!(outcome_usage.structural_nodes() > first_usage.structural_nodes());
    }
    if outcome_scenario == OutcomeScenario::ReturnedFailed {
        let stored = persisted_e2.as_ref().unwrap();
        assert_eq!(
            stored.snapshot.tasks[0].failure.as_deref(),
            Some(OUTCOME_RETURNED_FAILURE)
        );
        assert_eq!(
            stored.snapshot.failure.as_deref(),
            Some(OUTCOME_RETURNED_FAILURE)
        );
    }
    if settlement_result.is_ok() {
        assert!(outcome_usage.encoded_bytes() > 851_968);
        assert!(outcome_usage.structural_nodes() > 8192);
    }
    assert!(outcome_usage.encoded_bytes() > 0);
    assert!(outcome_usage.encoded_bytes() <= 4 * 1024 * 1024 * 1024);
    assert!(outcome_usage.structural_nodes() > 0);
    assert!(outcome_usage.structural_nodes() <= 16 * 1024 * 1024);
    println!("permission-use outcome evidence: scenario={outcome_scenario:?}; forwards={backend_forwards}; controls={}; code={settlement_code}; status={outcome_status:?}; calls={actual_callback_calls}; matched={actual_callback_matched_calls}; owners_alive={outcome_owners_alive}; bounded_match={outcome_matches}; usage=({}, {}); owners={original_owner},{next_owner},{persisted_owner}; directory_removed={directory_removed}", control_observations.len() + backend_observations.len(), outcome_usage.encoded_bytes(), outcome_usage.structural_nodes());
    assert_eq!(
        settlement_result.is_ok(),
        outcome_scenario.refusal().is_none(),
        "actual typed outcome result for {outcome_scenario:?}; actual={settlement_code}"
    );
    returned_account
}

// Root qualifier carriers are separate from production budget tickets.
// Exported account owners are opaque and contain no store/catalog/Claim borrow.
fn checked_account_constructor<F>(value: F) -> F
where
    F: FnOnce() -> PermissionUseRenewalAccount,
{
    const {
        assert!(std::mem::size_of::<F>() <= 128);
        assert!(std::mem::size_of::<(F,)>() <= 128);
    }
    value
}
const _: () = {
    assert!(std::mem::size_of::<RenewalScenario>() <= 128);
    assert!(std::mem::size_of::<(RenewalScenario,)>() <= 128);
    assert!(std::mem::size_of::<PermissionUseRenewalAccount>() <= 512);
    assert!(std::mem::size_of::<Option<PermissionUseRenewalAccount>>() <= 512);
    assert!(std::mem::size_of::<(RenewalScenario, Option<PermissionUseRenewalAccount>)>() <= 1024);
    assert!(std::mem::size_of::<Result<PermissionUseRenewalAccount, PermissionError>>() <= 512);
    assert!(
        std::mem::size_of::<Result<Option<PermissionUseRenewalAccount>, PermissionError>>() <= 512
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                Option<PermissionUseRenewalAccount>,
            >,
        >() <= 512
    );
    assert!(std::mem::size_of::<UseRenewalAuthority>() <= 2048);
    assert!(std::mem::size_of::<UseRenewalConstraint>() <= 2048);
    assert!(std::mem::size_of::<UseDispatchObservedStore>() <= 128);
    assert!(std::mem::size_of::<Arc<AtomicUsize>>() <= 128);
};

// The current application alone owns this oracle; producer callbacks stay silent.
struct UseCallbackOracle {
    expected_invocation: EffectInvocation,
    expected_context: InvocationContext,
    calls: Arc<AtomicUsize>,
    matched_calls: Arc<AtomicUsize>,
    returned_failure: bool,
}
impl NativeEffect for UseCallbackOracle {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if invocation != &self.expected_invocation || context != &self.expected_context {
            return Err(EffectUncertain);
        }
        self.matched_calls.fetch_add(1, Ordering::SeqCst);
        if self.returned_failure {
            Ok(NativeOutcome::Failed(OUTCOME_RETURNED_FAILURE.to_owned()))
        } else {
            Ok(NativeOutcome::Succeeded(TaskResult {
                output: serde_json::Value::Bool(true),
                writes: Vec::new(),
            }))
        }
    }
}
const _: () = {
    assert!(std::mem::size_of::<UseCallbackOracle>() <= 2048);
    assert!(
        std::mem::size_of::<(&UseCallbackOracle, &EffectInvocation, &InvocationContext)>() <= 128
    );
    assert!(std::mem::size_of::<Result<NativeOutcome, EffectUncertain>>() <= 128);
};

// Fresh Root observer metadata purchases apply to each actual closure incarnation.
fn checked_callback_observation<F, T, R>(value: F) -> F
where
    F: FnOnce(T) -> R,
{
    const {
        assert!(std::mem::size_of::<F>() <= 128);
        assert!(std::mem::size_of::<T>() <= 128);
        assert!(std::mem::size_of::<R>() <= 128);
        assert!(std::mem::size_of::<(F,)>() <= 128);
        assert!(std::mem::size_of::<(F, T)>() <= 256);
        assert!(std::mem::size_of::<Result<R, std::convert::Infallible>>() <= 128);
        assert!(std::mem::size_of::<std::ops::ControlFlow<std::convert::Infallible, R>>() <= 128);
    }
    value
}

const _: () = {
    assert!(std::mem::size_of::<NativeApplication>() <= 512);
    assert!(std::mem::size_of::<ApplicationIdentity>() <= 128);
    assert!(std::mem::size_of::<RouteRegistration>() <= 512);
    assert!(std::mem::size_of::<EffectBinding>() <= 128);
    assert!(std::mem::size_of::<PermissionCatalog>() <= 128);
    assert!(std::mem::size_of::<Definition>() <= 128);
    assert!(std::mem::size_of::<Snapshot>() <= 512);
    assert!(std::mem::size_of::<Result<NativeApplication, ApplicationConfigError>>() <= 512);
    assert!(std::mem::size_of::<Result<Snapshot, b4_kernel::KernelError>>() <= 512);
    assert!(std::mem::size_of::<Result<PermissionCatalog, PermissionConfigError>>() <= 128);
    assert!(std::mem::size_of::<(ApplicationIdentity, Vec<RouteRegistration>)>() <= 128);
    assert!(std::mem::size_of::<Vec<&TaskSpec>>() <= 128);
    assert!(std::mem::size_of::<BTreeMap<RouteKey, RouteRegistration>>() <= 128);
    assert!(std::mem::size_of::<BTreeMap<EffectId, EffectBinding>>() <= 128);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&RouteKey>>() <= 128);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&EffectId>>() <= 128);
};

const _: () = {
    assert!(std::mem::size_of::<EffectInvocation>() <= 512);
    assert!(std::mem::size_of::<(EffectInvocation,)>() <= 512);
    assert!(std::mem::size_of::<(&EffectInvocation,)>() <= 128);
    assert!(std::mem::size_of::<Result<EffectInvocation, std::convert::Infallible>>() <= 512);
    assert!(
        std::mem::size_of::<std::ops::ControlFlow<std::convert::Infallible, EffectInvocation>>()
            <= 512
    );
    assert!(std::mem::size_of::<InvocationContext>() <= 512);
    assert!(std::mem::size_of::<(InvocationContext,)>() <= 512);
    assert!(std::mem::size_of::<(&InvocationContext,)>() <= 128);
    assert!(std::mem::size_of::<Result<InvocationContext, std::convert::Infallible>>() <= 512);
    assert!(
        std::mem::size_of::<std::ops::ControlFlow<std::convert::Infallible, InvocationContext>>()
            <= 512
    );
};

// Direct actual API and iterator carriers used by the selected finite assembly.
const _: () = {
    assert!(
        std::mem::size_of::<(
            ExecutionId,
            TurnId,
            u64,
            Definition,
            serde_json::Map<String, serde_json::Value>,
            TransitionFacts
        )>() <= 512
    );
    assert!(std::mem::size_of::<(&Snapshot,)>() <= 128);
    assert!(std::mem::size_of::<Result<(), b4_kernel::KernelError>>() <= 512);
    assert!(std::mem::size_of::<std::ops::ControlFlow<b4_kernel::KernelError, ()>>() <= 512);
    assert!(std::mem::size_of::<(&ApplicationIdentity, &[RouteRegistration])>() <= 128);
    assert!(std::mem::size_of::<Result<(), ApplicationConfigError>>() <= 128);
    assert!(std::mem::size_of::<std::ops::ControlFlow<ApplicationConfigError, ()>>() <= 128);
    assert!(std::mem::size_of::<(&str,)>() <= 128);
    assert!(std::mem::size_of::<std::str::Chars<'static>>() <= 128);
    assert!(std::mem::size_of::<(char,)>() <= 128);
    assert!(std::mem::size_of::<Result<bool, std::convert::Infallible>>() <= 128);
    assert!(std::mem::size_of::<std::ops::ControlFlow<std::convert::Infallible, bool>>() <= 128);
    assert!(std::mem::size_of::<(RouteKey, RouteRegistration)>() <= 512);
    assert!(std::mem::size_of::<std::vec::IntoIter<RouteRegistration>>() <= 128);
    assert!(std::mem::size_of::<std::vec::IntoIter<(EffectId, EffectBinding)>>() <= 128);
    assert!(std::mem::size_of::<Result<(), SnapshotVersionError>>() <= 128);
    assert!(std::mem::size_of::<std::ops::ControlFlow<SnapshotVersionError, ()>>() <= 128);
    assert!(
        std::mem::size_of::<std::ops::ControlFlow<ApplicationConfigError, NativeApplication>>()
            <= 512
    );
    assert!(std::mem::size_of::<(Arc<NativeApplication>, Vec<ToolBindingRegistration>)>() <= 128);
    assert!(
        std::mem::size_of::<std::ops::ControlFlow<PermissionConfigError, PermissionCatalog>>()
            <= 128
    );
    assert!(std::mem::size_of::<std::collections::BTreeSet<&TurnId>>() <= 128);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&AttemptId>>() <= 128);
    assert!(std::mem::size_of::<std::collections::BTreeSet<&TaskId>>() <= 128);
    assert!(std::mem::size_of::<std::collections::btree_set::IntoIter<&EffectId>>() <= 128);
    assert!(
        std::mem::size_of::<std::collections::btree_map::IntoIter<RouteKey, RouteRegistration>>()
            <= 128
    );
    assert!(
        std::mem::size_of::<std::collections::btree_map::IntoIter<EffectId, EffectBinding>>()
            <= 128
    );
    assert!(std::mem::size_of::<std::vec::IntoIter<(RouteKey, RouteRegistration)>>() <= 128);
    assert!(std::mem::size_of::<std::slice::Iter<'static, TaskSpec>>() <= 128);
    assert!(std::mem::size_of::<std::slice::Iter<'static, Vec<TaskSpec>>>() <= 128);
    assert!(
        std::mem::size_of::<std::iter::Flatten<std::slice::Iter<'static, Vec<TaskSpec>>>>() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::iter::Take<std::iter::Flatten<std::slice::Iter<'static, Vec<TaskSpec>>>>,
        >() <= 128
    );
};

// Actual Result Try residuals at the selected successful validation boundaries.
const _: () = {
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, ApplicationConfigError>, ()>,
        >() <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, b4_kernel::KernelError>, ()>,
        >() <= 512
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, b4_kernel::KernelError>,
                Snapshot,
            >,
        >() <= 512
    );
};

// The application validator maps KernelError before applying Result Try.
const _: () = {
    assert!(std::mem::size_of::<Result<Snapshot, ApplicationConfigError>>() <= 512);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, ApplicationConfigError>,
                Snapshot,
            >,
        >() <= 512
    );
};

// Actual selected standard-library allocation, array and retirement carriers.
const _: () = {
    assert!(std::mem::size_of::<(EffectId, EffectBinding)>() <= 128);
    assert!(std::mem::size_of::<std::array::IntoIter<(EffectId, EffectBinding), 1>>() <= 128);
    assert!(
        std::mem::size_of::<std::iter::Peekable<std::vec::IntoIter<(RouteKey, RouteRegistration)>>>(
        ) <= 512
    );
    assert!(std::mem::size_of::<std::vec::IntoIter<&TaskSpec>>() <= 128);
    assert!(std::mem::size_of::<std::vec::IntoIter<&TaskId>>() <= 128);
    assert!(std::mem::size_of::<std::vec::IntoIter<&EffectId>>() <= 128);
    assert!(std::mem::size_of::<std::vec::IntoIter<&TurnId>>() <= 128);
    assert!(std::mem::size_of::<std::collections::btree_set::IntoIter<&RouteKey>>() <= 128);
    assert!(std::mem::size_of::<std::collections::btree_set::IntoIter<&TaskId>>() <= 128);
    assert!(std::mem::size_of::<std::collections::btree_set::IntoIter<&TurnId>>() <= 128);
    assert!(std::mem::size_of::<std::collections::btree_set::IntoIter<&AttemptId>>() <= 128);
    assert!(
        std::mem::size_of::<std::collections::btree_map::IntoIter<String, serde_json::Value>>()
            <= 128
    );
    assert!(
        std::mem::size_of::<std::collections::btree_map::Keys<'static, EffectId, EffectBinding>>()
            <= 128
    );
    assert!(
        std::mem::size_of::<std::collections::btree_map::Iter<'static, EffectId, EffectBinding>>()
            <= 128
    );
    assert!(
        std::mem::size_of::<std::collections::btree_map::Iter<'static, RouteKey, RouteRegistration>>(
        ) <= 128
    );
    assert!(std::mem::size_of::<std::alloc::Layout>() <= 128);
    assert!(std::mem::size_of::<std::ptr::NonNull<u8>>() <= 128);
    assert!(std::mem::size_of::<std::ptr::NonNull<[u8]>>() <= 128);
    assert!(std::mem::size_of::<(std::alloc::Layout,)>() <= 128);
    assert!(std::mem::size_of::<(std::ptr::NonNull<u8>, std::alloc::Layout)>() <= 128);
    assert!(
        std::mem::size_of::<Result<std::ptr::NonNull<[u8]>, std::collections::TryReserveError>>()
            <= 128
    );
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, std::collections::TryReserveError>,
                std::ptr::NonNull<[u8]>,
            >,
        >() <= 128
    );
    assert!(std::mem::size_of::<serde_json::Value>() <= 128);
    assert!(std::mem::size_of::<String>() <= 128);
    assert!(std::mem::size_of::<Option<serde_json::Value>>() <= 128);
    assert!(std::mem::size_of::<Option<String>>() <= 128);
    assert!(std::mem::size_of::<TaskSpec>() <= 128);
    assert!(std::mem::align_of::<UseCallbackOracle>() <= 8);
    assert!(std::mem::align_of::<NativeApplication>() <= 8);
    assert!(std::mem::align_of::<AtomicUsize>() <= 8);
};

// Direct subject carrier observations; Root exterior pricing is separate.
const _: () = {
    assert!(
        std::mem::size_of::<
            PermissionUseOutcomeWork<'_, '_, '_, UseDispatchObservedStore, UseRenewalAuthority>,
        >() <= 32_768
    );
    assert!(
        std::mem::size_of::<(
            &PermissionUseOutcomeRequest<'_>,
            &mut PermissionUseOutcomeAccount
        )>() <= 128
    );
    assert!(std::mem::size_of::<Result<PermissionUseOutcomeCommit, PermissionError>>() <= 8192);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseOutcomeCommit,
            >,
        >() <= 8192
    );
    assert!(
        std::mem::size_of::<(
            &mut PermissionUseOutcomeWork<
                '_,
                '_,
                '_,
                UseDispatchObservedStore,
                UseRenewalAuthority,
            >,
            &TransitionFacts
        )>() <= 128
    );
};

// Future GREEN-only oracle carriers; no selected RED execution credit.
const _: () = {
    assert!(std::mem::size_of::<TaskResult>() <= 128);
    assert!(std::mem::size_of::<KernelCommand>() <= 512);
    assert!(std::mem::size_of::<(&Snapshot, KernelCommand, TransitionFacts)>() <= 512);
    assert!(std::mem::size_of::<CommitPlan>() <= 512);
    assert!(std::mem::size_of::<Result<CommitPlan, b4_kernel::KernelError>>() <= 512);
    assert!(std::mem::size_of::<StoredExecution>() <= 512);
    assert!(std::mem::size_of::<ThreadRecord>() <= 512);
    assert!(std::mem::size_of::<JournalEntry>() <= 512);
};

// Concrete adapter for the actual settlement error-code function item.
fn checked_outcome_code<F>(value: F) -> F
where
    F: for<'a> FnOnce(&'a PermissionError) -> &'static str,
{
    const {
        assert!(std::mem::size_of::<F>() <= 128);
        assert!(std::mem::size_of::<(F,)>() <= 128);
        assert!(std::mem::size_of::<(F, &PermissionError)>() <= 128);
        assert!(std::mem::size_of::<Option<&PermissionError>>() <= 128);
        assert!(std::mem::size_of::<Option<&'static str>>() <= 128);
        assert!(
            std::mem::size_of::<Result<&PermissionUseOutcomeCommit, &PermissionError>>() <= 128
        );
        assert!(std::mem::size_of::<Result<F, std::convert::Infallible>>() <= 128);
        assert!(std::mem::size_of::<std::ops::ControlFlow<std::convert::Infallible, F>>() <= 128);
    }
    value
}

// Actual current owned observation headers and individual normal drop ArgsBounds.
const _: () = {
    assert!(std::mem::size_of::<ThreadView>() <= 2048);
    assert!(std::mem::size_of::<Result<Option<ThreadView>, StoreError>>() <= 2048);
    assert!(std::mem::size_of::<(ThreadView,)>() <= 2048);
    assert!(std::mem::size_of::<StoredExecution>() <= 2048);
    assert!(std::mem::size_of::<Option<StoredExecution>>() <= 2048);
    assert!(std::mem::size_of::<Result<Option<StoredExecution>, StoreError>>() <= 2048);
    assert!(std::mem::size_of::<(Option<StoredExecution>,)>() <= 2048);
    assert!(std::mem::size_of::<Vec<JournalEntry>>() <= 128);
    assert!(std::mem::size_of::<StoreError>() <= 128);
    assert!(std::mem::size_of::<Result<Vec<JournalEntry>, StoreError>>() <= 128);
    assert!(std::mem::size_of::<(Vec<JournalEntry>,)>() <= 128);
    assert!(std::mem::size_of::<ProducedRawNine>() <= 512);
    assert!(std::mem::size_of::<(ProducedRawNine,)>() <= 512);
    assert!(std::mem::size_of::<(&SqliteStore, &ThreadId)>() <= 128);
    assert!(std::mem::size_of::<(&SqliteStore, &ExecutionId)>() <= 128);
    assert!(std::mem::size_of::<(&std::path::Path,)>() <= 128);
};

// Task6 controls use genuine retained callback receipts and real SQLite stores.
const OUTCOME_RETURNED_FAILURE: &str = "fixture-returned-failure";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutcomeScenario {
    Succeeded,
    ReturnedFailed,
    JournalAbortThenRetry,
    ReplayActualRequest,
    ChangedResourceRevision,
    ChangedSourceHeader,
    CorruptOwnerMetadata,
    ForeignPhysicalStore,
}
impl OutcomeScenario {
    fn matches_outcome(self, outcome: Option<&EffectOutcome>) -> bool {
        if self == Self::ReturnedFailed {
            matches!(outcome, Some(EffectOutcome::Failed(failure)) if failure == OUTCOME_RETURNED_FAILURE)
        } else {
            matches!(outcome, Some(EffectOutcome::Succeeded(result)) if result.output == serde_json::Value::Bool(true) && result.writes.is_empty())
        }
    }
    fn refusal(self) -> Option<OutcomeRefusal> {
        match self {
            Self::ChangedResourceRevision | Self::ChangedSourceHeader => {
                Some(OutcomeRefusal::CorruptSource)
            }
            Self::CorruptOwnerMetadata => Some(OutcomeRefusal::OwnerConflict),
            Self::ForeignPhysicalStore => Some(OutcomeRefusal::MissingForeignResource),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
enum OutcomeRefusal {
    LateJournalAbort,
    Completed,
    ReplayedRequest,
    CorruptSource,
    OwnerConflict,
    MissingForeignResource,
}
impl OutcomeRefusal {
    fn matches(self, error: &PermissionError) -> bool {
        match self {
            Self::LateJournalAbort => {
                matches!(error, PermissionError::Store(StoreError::Unavailable(code)) if code == "permission_use_outcome_write_unavailable")
            }
            Self::Completed | Self::ReplayedRequest => {
                matches!(error, PermissionError::UnsupportedProfile)
            }
            Self::CorruptSource => matches!(error, PermissionError::CorruptRecord),
            Self::OwnerConflict => {
                matches!(error, PermissionError::Store(StoreError::OwnerConflict))
            }
            Self::MissingForeignResource => {
                matches!(error, PermissionError::Store(StoreError::ExecutionNotFound))
            }
        }
    }
    fn code(self) -> &'static str {
        match self {
            Self::LateJournalAbort | Self::OwnerConflict | Self::MissingForeignResource => {
                "permission_store_failed"
            }
            Self::Completed | Self::ReplayedRequest => "permission_unsupported_profile",
            Self::CorruptSource => "permission_corrupt_record",
        }
    }
}
#[derive(PartialEq)]
struct OutcomeCapture {
    view: Option<ThreadView>,
    original: Option<StoredExecution>,
    current: Option<StoredExecution>,
    original_journal: Vec<JournalEntry>,
    current_journal: Vec<JournalEntry>,
    raw: ProducedRawNine,
}
impl OutcomeCapture {
    fn read(
        store: &SqliteStore,
        path: &std::path::Path,
        thread: &ThreadId,
        original: &ExecutionId,
        current: &ExecutionId,
    ) -> Self {
        Self {
            view: store.thread_view(thread).unwrap(),
            original: store.execution(original).unwrap(),
            current: store.execution(current).unwrap(),
            original_journal: store.journal(original).unwrap(),
            current_journal: store.journal(current).unwrap(),
            raw: use_dispatch_raw_nine(path),
        }
    }
}
struct OutcomeControlObservation {
    before: OutcomeCapture,
    after: OutcomeCapture,
    expected: OutcomeRefusal,
    code: &'static str,
    exact_error: bool,
    usage: Option<(PermissionEvidenceUsage, PermissionEvidenceUsage)>,
    callback_calls: usize,
}
impl OutcomeControlObservation {
    fn new(
        before: OutcomeCapture,
        after: OutcomeCapture,
        result: &Result<PermissionUseOutcomeCommit, PermissionError>,
        expected: OutcomeRefusal,
        usage: Option<(PermissionEvidenceUsage, PermissionEvidenceUsage)>,
        callback_calls: usize,
    ) -> Self {
        Self {
            before,
            after,
            expected,
            code: result
                .as_ref()
                .err()
                .map(PermissionError::code)
                .unwrap_or("ok"),
            exact_error: result
                .as_ref()
                .err()
                .is_some_and(|error| expected.matches(error)),
            usage,
            callback_calls,
        }
    }
    fn assert_refusal(&self) {
        assert!(
            self.exact_error,
            "exact {:?} refusal required; actual={}",
            self.expected, self.code
        );
        assert_eq!(self.code, self.expected.code());
        assert!(
            self.before == self.after,
            "complete typed/raw9 equality across {:?}",
            self.expected
        );
        assert_eq!(self.callback_calls, 1);
        println!("permission-use outcome control: expected={:?}; code={}; full_typed_raw9_unchanged=true; callback_calls={}", self.expected, self.code, self.callback_calls);
        if let Some((before, after)) = self.usage {
            if matches!(self.expected, OutcomeRefusal::Completed) {
                assert_eq!(before, after);
            } else {
                assert!(after.encoded_bytes() > before.encoded_bytes());
                assert!(after.structural_nodes() > before.structural_nodes());
            }
            assert!(after.encoded_bytes() <= 4 * 1024 * 1024 * 1024);
            assert!(after.structural_nodes() <= 16 * 1024 * 1024);
        }
    }
}
struct OutcomeBackendControl {
    scenario: OutcomeScenario,
    path: PathBuf,
    thread: ThreadId,
    original: ExecutionId,
    current: ExecutionId,
    foreign: Option<Arc<SqliteStore>>,
    foreign_path: Option<PathBuf>,
    observations: Mutex<Vec<OutcomeControlObservation>>,
    forwards: AtomicUsize,
    callback_calls: Arc<AtomicUsize>,
}
impl OutcomeBackendControl {
    fn capture(&self, store: &SqliteStore, path: &std::path::Path) -> OutcomeCapture {
        OutcomeCapture::read(store, path, &self.thread, &self.original, &self.current)
    }
    fn forward(
        &self,
        inner: &SqliteStore,
        request: &PermissionUseOutcomeRequest<'_>,
        account: &mut PermissionUseOutcomeAccount,
    ) -> Result<PermissionUseOutcomeCommit, PermissionError> {
        if let Some(foreign) = &self.foreign {
            let path = self.foreign_path.as_ref().unwrap();
            let before = self.capture(foreign, path);
            self.forwards.fetch_add(1, Ordering::SeqCst);
            let result = foreign.commit_permission_use_outcome(request, account);
            let after = self.capture(foreign, path);
            self.observations
                .lock()
                .unwrap()
                .push(OutcomeControlObservation::new(
                    before,
                    after,
                    &result,
                    OutcomeRefusal::MissingForeignResource,
                    None,
                    self.callback_calls.load(Ordering::SeqCst),
                ));
            return result;
        }
        self.forwards.fetch_add(1, Ordering::SeqCst);
        let result = inner.commit_permission_use_outcome(request, account);
        if self.scenario == OutcomeScenario::ReplayActualRequest && result.is_ok() {
            // Reuse only the actual borrowed opaque Request. Keep/return the
            // first genuine Commit; never construct or substitute a success.
            let before = self.capture(inner, &self.path);
            self.forwards.fetch_add(1, Ordering::SeqCst);
            let replay = inner.commit_permission_use_outcome(request, account);
            let after = self.capture(inner, &self.path);
            self.observations
                .lock()
                .unwrap()
                .push(OutcomeControlObservation::new(
                    before,
                    after,
                    &replay,
                    OutcomeRefusal::ReplayedRequest,
                    None,
                    self.callback_calls.load(Ordering::SeqCst),
                ));
        }
        result
    }
}
fn outcome_injection_connection(path: &std::path::Path) -> rusqlite::Connection {
    rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .unwrap()
}
fn install_outcome_abort(
    path: &std::path::Path,
    execution: &ExecutionId,
    next_revision: u64,
    next_resource_revision: u64,
) -> bool {
    let connection = outcome_injection_connection(path);
    // SQLite disallows bound parameters in CREATE TRIGGER. Escape the real
    // fixture ID; big-endian revision BLOB literals have exactly sixteen hex digits.
    let execution = execution.as_str().replace('\'', "''");
    let sql = format!("CREATE TRIGGER outcome_test_abort_journal BEFORE INSERT ON journal
        WHEN NEW.kind='commit' AND NEW.execution_id='{execution}' AND NEW.revision=X'{next_revision:016x}'
        AND EXISTS (SELECT 1 FROM executions WHERE id=NEW.execution_id AND revision=NEW.revision)
        AND EXISTS (SELECT 1 FROM threads WHERE id=NEW.thread_id AND active_execution_id=NEW.execution_id AND resource_revision=X'{next_resource_revision:016x}')
        BEGIN SELECT RAISE(ABORT, 'outcome_fixture_late_journal_abort'); END");
    connection.execute_batch(&sql).unwrap();
    let installed: i64 = connection.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='outcome_test_abort_journal' AND sql=?1", [&sql], |row| row.get(0)).unwrap();
    // Inspect the exact trigger definition as well as its name.
    let actual: String = connection.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='outcome_test_abort_journal'", [], |row| row.get(0)).unwrap();
    connection.close().unwrap();
    installed == 1 && actual == sql
}
