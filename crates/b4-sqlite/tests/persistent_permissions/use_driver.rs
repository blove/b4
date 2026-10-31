// Independent real Driver fixture; old fixtures remain literal.
use super::*;
use std::sync::Mutex;

struct DriverObservedStore {
    inner: Arc<SqliteStore>,
    captures: Arc<Mutex<Vec<&'static str>>>,
    entries: Arc<AtomicUsize>,
    path: PathBuf,
    thread: ThreadId,
    original: ExecutionId,
    current: ExecutionId,
    callback_calls: Arc<AtomicUsize>,
    captured: Mutex<Option<DriverCapture>>,
    raw_commits: AtomicUsize,
    outcome_entries: AtomicUsize,
    outcome_forwards: AtomicUsize,
    read_fault: DriverReadFault,
    fault_applied: std::sync::atomic::AtomicBool,
    postclaim: Mutex<Option<DriverCapture>>,
    substituted: Mutex<Option<StoredExecution>>,
    healthy: Mutex<Option<DriverCapture>>,
    revision_restore: Mutex<Option<DriverRevisionRestore>>,
    trigger_installed: std::sync::atomic::AtomicBool,
}
impl Store for DriverObservedStore {
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
        let mut actual = self.inner.execution(id)?;
        if id == &self.current
            && self.callback_calls.load(Ordering::SeqCst) == 1
            && !self.fault_applied.swap(true, Ordering::SeqCst)
        {
            *self.captured.lock().unwrap() = Some(DriverCapture::read(self));
            if self.read_fault == DriverReadFault::Definition {
                let changed = actual.as_mut().expect("actual claimed execution");
                changed.snapshot.definition.max_attempts = 4;
                *self.substituted.lock().unwrap() = Some(changed.clone());
            }
            if self.read_fault == DriverReadFault::JournalAbort {
                let capture = DriverCapture::read(self);
                let installed = driver_install_abort(
                    &self.path,
                    &self.current,
                    capture.executions[1].snapshot.revision + 1,
                    capture.view.thread.resource_revision + 1,
                );
                self.trigger_installed.store(installed, Ordering::SeqCst);
            }
            if self.read_fault == DriverReadFault::ResourceRevision {
                let healthy = DriverCapture::read(self);
                let restore = DriverRevisionRestore::corrupt(&self.path, &self.thread);
                *self.revision_restore.lock().unwrap() = Some(restore);
                *self.healthy.lock().unwrap() = Some(healthy);
                *self.captured.lock().unwrap() = Some(DriverCapture::read(self));
            }
        }
        Ok(actual)
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
        self.raw_commits.fetch_add(1, Ordering::SeqCst);
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
impl ApprovalStore for DriverObservedStore {
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
impl PermissionStore for DriverObservedStore {
    fn commit_permission_use_outcome(
        &self,
        request: &PermissionUseOutcomeRequest<'_>,
        account: &mut PermissionUseOutcomeAccount,
    ) -> Result<PermissionUseOutcomeCommit, PermissionError> {
        self.outcome_entries.fetch_add(1, Ordering::SeqCst);
        if self.read_fault == DriverReadFault::OutcomePortPanic {
            panic!("driver_fixture_outcome_port_panic")
        }
        self.outcome_forwards.fetch_add(1, Ordering::SeqCst);
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
        if self.entries.load(Ordering::SeqCst) == 1 && self.postclaim.lock().unwrap().is_none() {
            let capture = DriverCapture::read(self);
            *self.postclaim.lock().unwrap() = Some(capture);
        }
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

fn driver_raw_nine(path: &std::path::Path) -> ProducedRawNine {
    let connection = rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let raw = ProducedRawNine { tables: [
        produced_raw_cells(&connection, "SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata ORDER BY id LIMIT 2", 4, 1),
        produced_raw_cells(&connection, "SELECT id, active_execution_id, created_at_ms, metadata, origin, resource_revision, generation FROM threads ORDER BY id LIMIT 2", 7, 1),
        produced_raw_cells(&connection, "SELECT id, thread_id, revision, owner_epoch, snapshot FROM executions ORDER BY id LIMIT 3", 5, 2),
        produced_raw_cells(&connection, "SELECT sequence, kind, thread_id, execution_id, revision, owner_epoch, status, updated_at_ms, payload FROM journal ORDER BY sequence LIMIT 13", 9, 12),
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

#[derive(PartialEq)]
struct DriverCapture {
    view: ThreadView,
    executions: [StoredExecution; 2],
    journals: [Vec<JournalEntry>; 2],
    raw: ProducedRawNine,
}
impl DriverCapture {
    fn read(store: &DriverObservedStore) -> Self {
        Self {
            view: store.inner.thread_view(&store.thread).unwrap().unwrap(),
            executions: [&store.original, &store.current]
                .map(|id| store.inner.execution(id).unwrap().unwrap()),
            journals: [&store.original, &store.current].map(|id| store.inner.journal(id).unwrap()),
            raw: driver_raw_nine(&store.path),
        }
    }
    fn assert_same(&self, other: &Self) {
        assert_eq!(self.view, other.view);
        assert_eq!(self.executions, other.executions);
        assert_eq!(self.journals, other.journals);
        assert!(self.raw == other.raw, "all nine raw groups unchanged");
    }
}

struct DriverHooks;
impl PermissionUseDriverHooks for DriverHooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        facts(if current.snapshot.revision == 2 {
            108
        } else {
            109
        })
    }
    fn attempt_id(&self) -> AttemptId {
        AttemptId::new("release-attempt").unwrap()
    }
}
struct DriverOracle {
    invocation: EffectInvocation,
    context: InvocationContext,
    calls: Arc<AtomicUsize>,
    matches: Arc<AtomicUsize>,
    result: DriverNativeResult,
}
impl NativeEffect for DriverOracle {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if invocation != &self.invocation || context != &self.context {
            return Err(EffectUncertain);
        }
        self.matches.fetch_add(1, Ordering::SeqCst);
        match &self.result {
            DriverNativeResult::Succeeded => Ok(NativeOutcome::Succeeded(TaskResult {
                output: serde_json::Value::Bool(true),
                writes: vec![],
            })),
            DriverNativeResult::Failed => Ok(NativeOutcome::Failed("native-failure".into())),
            DriverNativeResult::Uncertain => Err(EffectUncertain),
            DriverNativeResult::Panicked {
                drops,
                panic_on_drop,
            } => std::panic::panic_any(DriverPanicWitness {
                drops: drops.clone(),
                panic_on_drop: *panic_on_drop,
            }),
        }
    }
}
struct DriverAuthority {
    context: ApprovalContext,
    resources: [ApprovalResource; 2],
    calls: AtomicUsize,
    matches: AtomicUsize,
    ready_matches: AtomicUsize,
    claimed_matches: AtomicUsize,
    refuse_claimed: bool,
}
impl ApprovalAuthority for DriverAuthority {
    fn authorize(
        &self,
        context: &ApprovalContext,
        operation: ApprovalOperation,
        resource: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if context != &self.context
            || operation != ApprovalOperation::Dispatch
            || !self.resources.contains(resource)
        {
            return Err(ApprovalError::Unauthorized);
        }
        self.matches.fetch_add(1, Ordering::SeqCst);
        if resource == &self.resources[0] {
            self.ready_matches.fetch_add(1, Ordering::SeqCst);
        } else {
            self.claimed_matches.fetch_add(1, Ordering::SeqCst);
            if self.refuse_claimed {
                return Err(ApprovalError::Unauthorized);
            }
        }
        Ok(())
    }
}
struct DriverConstraint {
    context: InvocationContext,
    definition: Definition,
    ready: TaskRecord,
    claimed: TaskRecord,
    invocation: EffectInvocation,
    calls: AtomicUsize,
    matches: AtomicUsize,
}
impl ToolConstraint for DriverConstraint {
    fn validate_call(&self, call: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let phase = match call.phase {
            PermissionCallPhase::Unclaimed => call.task == &self.ready,
            PermissionCallPhase::Claimed(invocation) => {
                call.task == &self.claimed && invocation == &self.invocation
            }
        };
        if !phase
            || call.context != &self.context
            || call.definition != &self.definition
            || call.binding.application.application != self.context.origin.application
            || call.binding.application.revision != "application-1"
            || call.binding.definition != self.definition
            || call.binding.effect_id != self.ready.spec.effect_id
            || call.binding.tool_id.as_str() != "release-tool"
            || call.binding.tool_revision.as_str() != "tool-1"
            || call.binding.constraint_revision.as_str() != "constraint-1"
        {
            return Err(ToolConstraintError::Denied);
        }
        self.matches.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn permission_use_driver_settled_result_requires_receipt_commit() {
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
    } = driver_ready_fixture();
    let before = next.thread_view(&thread).unwrap().unwrap();
    let attempt = AttemptId::new("release-attempt").unwrap();
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
    let callback_weak = Arc::downgrade(&callback);
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
    let application_weak = Arc::downgrade(&application);
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
    let catalog_weak = Arc::downgrade(&catalog);
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
        read_fault: DriverReadFault::None,
        fault_applied: std::sync::atomic::AtomicBool::new(false),
        postclaim: Mutex::new(None),
        substituted: Mutex::new(None),
        healthy: Mutex::new(None),
        revision_restore: Mutex::new(None),
        trigger_installed: std::sync::atomic::AtomicBool::new(false),
    };
    let capacity = TaskCapacity::new(1).unwrap();
    let actual = drive_permission_use_execution(
        PermissionUseDriverBindings {
            store: &observed,
            authority: &authority,
            producer_catalog: catalog.as_ref(),
            current_catalog: catalog.as_ref(),
            context: &context,
            hooks: &DriverHooks,
            capacity: &capacity,
        },
        authorized,
    );
    let after = DriverCapture::read(&observed);
    let capture = observed
        .captured
        .lock()
        .unwrap()
        .take()
        .expect("actual completion read follows callback");
    assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
    assert_eq!(callback_matches.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(observed.entries.load(Ordering::SeqCst), 1);
    assert_eq!(observed.raw_commits.load(Ordering::SeqCst), 0);
    assert_eq!(
        *observed.captures.lock().unwrap(),
        vec!["ok", "ok", "ok", "resource", "ok", "source", "ok", "resource", "ok"]
    );
    assert_eq!(
        authority.calls.load(Ordering::SeqCst),
        authority.matches.load(Ordering::SeqCst)
    );
    assert_eq!(authority.ready_matches.load(Ordering::SeqCst), 1);
    assert_eq!(authority.claimed_matches.load(Ordering::SeqCst), 1);
    assert_eq!(authority.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        constraint.calls.load(Ordering::SeqCst),
        constraint.matches.load(Ordering::SeqCst)
    );
    assert!(constraint.calls.load(Ordering::SeqCst) > 0);
    let succeeded = actual.as_ref().is_ok_and(|stored| {
        stored.snapshot.status == ExecutionStatus::Completed
            && stored.snapshot.tasks[0].status == TaskStatus::Succeeded
    });
    let evidence = if let Err(failure) = &actual {
        capture.assert_same(&after);
        assert_eq!(after.executions[1], claimed);
        assert_eq!(observed.outcome_entries.load(Ordering::SeqCst), 0);
        assert_eq!(
            failure.callback_status(),
            Some(PermissionUseCallbackStatus::Settled)
        );
        assert_eq!(
            failure.settled_outcome(),
            Some(&EffectOutcome::Succeeded(TaskResult {
                output: serde_json::Value::Bool(true),
                writes: vec![]
            }))
        );
        let usage = failure.usage();
        assert_eq!(usage.completion_entries(), 1);
        assert_eq!(usage.unsupported_completions(), 1);
        assert!(usage.encoded_bytes() > 0 && usage.encoded_bytes() <= 16 * 1024 * 1024);
        assert!(usage.structural_nodes() > 0 && usage.structural_nodes() <= 131_072);
        "driver-task1: callback=1 matched=1 completion=unsupported retained=settled raw-commit=0 typed-outcome=0 reopened=1 removed=1"
    } else {
        let expected = transition(
            &claimed.snapshot,
            KernelCommand::Complete {
                task_id: task.clone(),
                attempt_id: claimed.snapshot.tasks[0].attempt_id.clone().unwrap(),
                result: TaskResult {
                    output: serde_json::Value::Bool(true),
                    writes: vec![],
                },
            },
            facts(109),
        )
        .unwrap();
        assert_eq!(actual.as_ref().unwrap(), &after.executions[1]);
        assert_eq!(after.executions[1].snapshot, expected.snapshot);
        assert_eq!(
            after.view.thread.resource_revision,
            capture.view.thread.resource_revision + 1
        );
        assert_eq!(after.journals[1].len(), capture.journals[1].len() + 1);
        assert_eq!(
            &after.journals[1][..capture.journals[1].len()],
            capture.journals[1].as_slice()
        );
        assert_eq!(
            after.journals[1].last(),
            Some(&JournalEntry::from(&expected.snapshot))
        );
        assert_eq!(after.executions[0], capture.executions[0]);
        assert_eq!(after.journals[0], capture.journals[0]);
        for index in [0, 4, 5, 6, 7, 8] {
            assert_eq!(after.raw.tables[index], capture.raw.tables[index]);
        }
        assert_eq!(observed.outcome_entries.load(Ordering::SeqCst), 1);
        "driver-task1: callback=1 matched=1 completion=durable raw-commit=0 typed-outcome=1 reopened=1 removed=1"
    };
    assert!(
        catalog_weak.upgrade().is_some()
            && application_weak.upgrade().is_some()
            && callback_weak.upgrade().is_some()
    );
    drop(actual);
    drop(observed);
    drop(authority);
    drop(catalog);
    drop(application);
    drop(callback);
    drop(constraint);
    assert!(
        catalog_weak.upgrade().is_none()
            && application_weak.upgrade().is_none()
            && callback_weak.upgrade().is_none()
    );
    assert_eq!(Arc::strong_count(&next), 1);
    let owner2 = next.owner_generation();
    drop(next);
    let owner3 = SqliteStore::open(&directory.0).unwrap();
    assert_eq!(owner3.owner_generation(), owner2 + 1);
    assert_eq!(owner3.thread_view(&thread).unwrap().unwrap(), after.view);
    assert_eq!(
        owner3.execution(&execution).unwrap().unwrap(),
        after.executions[0]
    );
    assert_eq!(
        owner3.execution(&second_execution).unwrap().unwrap(),
        after.executions[1]
    );
    assert_eq!(owner3.journal(&execution).unwrap(), after.journals[0]);
    assert_eq!(
        owner3.journal(&second_execution).unwrap(),
        after.journals[1]
    );
    let reopened = driver_raw_nine(&directory.0);
    assert!(reopened.tables[1..] == after.raw.tables[1..]);
    assert_eq!(reopened.tables[0][0][..3], after.raw.tables[0][0][..3]);
    assert_eq!(produced_u64(&reopened.tables[0][0][3]), owner2 + 1);
    owner3.readiness().unwrap();
    drop(owner3);
    drop(directory);
    println!("{evidence}");
    assert!(
        succeeded,
        "real Driver must persist its receipt-backed successful result"
    );
}

// Static trait propositions use no constructed authority or native execution.
const _: fn() = || {
    fn send<T: Send>() {}
    fn sync<T: Sync>() {}
    send::<
        PermissionUseCallbackWork<'static, 'static, 'static, DriverObservedStore, DriverAuthority>,
    >();
    send::<PermissionUseOutcomeWork<'static, 'static, 'static, DriverObservedStore, DriverAuthority>>(
    );
    sync::<
        Mutex<
            PermissionUseOutcomeWork<
                'static,
                'static,
                'static,
                DriverObservedStore,
                DriverAuthority,
            >,
        >,
    >();
    send::<PermissionUseDriverFailure<'static, DriverObservedStore, DriverAuthority>>();
};

struct DriverReadyFixture {
    directory: Directory,
    next: Arc<SqliteStore>,
    thread: ThreadId,
    execution: ExecutionId,
    second_execution: ExecutionId,
    task: TaskId,
    context: ApprovalContext,
    definition: Definition,
    authorized: StoredExecution,
    calls: Arc<AtomicUsize>,
}
struct DriverFixtureConfig {
    current_execution_id: ExecutionId,
    current_turn_id: TurnId,
    attempt_id: AttemptId,
    current_input: serde_json::Value,
}
impl Default for DriverFixtureConfig {
    fn default() -> Self {
        Self {
            current_execution_id: ExecutionId::new("permission-second").unwrap(),
            current_turn_id: TurnId::new("second-turn").unwrap(),
            attempt_id: AttemptId::new("release-attempt").unwrap(),
            current_input: json!({"release":"changed"}),
        }
    }
}
fn driver_ready_fixture() -> DriverReadyFixture {
    driver_ready_fixture_with(&DriverFixtureConfig::default())
}
fn driver_ready_fixture_with(config: &DriverFixtureConfig) -> DriverReadyFixture {
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
    let request = ApprovalAcceptanceRequest {
        context: context.clone(),
        route: definition.route.clone(),
        definition_revision: definition.revision.clone(),
        turn_id: TurnId::new("approved-turn").unwrap(),
        now_ms: 103,
        decisions: vec![GrantDecision {
            interrupt_id: interrupt,
            decision: ApprovalDecision::Always,
            cancelled: false,
            token: Some(pending[0].token.clone()),
        }],
    };

    let producer = accept_permission_human(
        store.clone(),
        authority.clone(),
        catalog.clone(),
        &execution,
        fence(store.as_ref(), &thread),
        request,
    )
    .unwrap();
    assert_eq!(producer.snapshot.revision, 3);
    let view = store.thread_view(&thread).unwrap().unwrap();
    let target = RunCancellationTarget::from_view(&view).unwrap();
    let cancel_authority = ReadyCancellationAuthority {
        expected_context: context.clone(),
        expected_resource: ApprovalResource {
            thread: view.thread.clone(),
            execution: producer,
        },
        expected_target: target.clone(),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    cancel_permission_ready(
        store.as_ref(),
        &cancel_authority,
        catalog.as_ref(),
        &PermissionReadyCancellationRequest {
            context: &context,
            target: &target,
            facts: facts(104),
        },
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap();
    drop(cancel_authority);
    drop(service);
    drop(authority);
    drop(catalog);
    drop(application);
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let next = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(next.owner_generation(), original_owner + 1);
    let second_execution = config.current_execution_id.clone();
    let second_initial = new_execution(
        second_execution.clone(),
        config.current_turn_id.clone(),
        next.owner_generation(),
        definition.clone(),
        Map::from_iter([("input".into(), config.current_input.clone())]),
        facts(105),
    )
    .unwrap();
    let next_view = next.thread_view(&thread).unwrap().unwrap();
    let admitted = next
        .admit_fenced(
            &thread,
            &next_view.thread.generation,
            next_view.thread.resource_revision,
            second_initial,
        )
        .unwrap();
    let started = next
        .commit(
            &second_execution,
            (&admitted.snapshot).into(),
            transition(&admitted.snapshot, KernelCommand::Start, facts(106)).unwrap(),
        )
        .unwrap();
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
    let command = b4_kernel::PermissionAuthorizationCommand::capture(
        &started.snapshot,
        task.clone(),
        PermissionUseId::new(
            "b4pu_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        )
        .unwrap(),
    )
    .unwrap();
    let authorized = try_authorize_permission_ready(
        next.as_ref(),
        &AllowAuthority,
        &setup_catalog,
        fence(next.as_ref(), &thread),
        &PermissionReadyAuthorizationRequest {
            context: &context,
            command: &command,
            facts: facts(107),
        },
        &mut PermissionEvidenceBudget::new(),
    )
    .unwrap()
    .unwrap();
    drop(setup_catalog);
    drop(setup_application);
    assert_eq!(authorized.snapshot.revision, 2);
    assert_eq!(authorized.snapshot.status, ExecutionStatus::Running);
    assert_eq!(authorized.snapshot.attempts_used, 0);
    assert!(authorized.snapshot.attempt_history.is_empty());
    assert!(authorized.snapshot.tasks[0].approved);
    assert_eq!(authorized.snapshot.tasks[0].status, TaskStatus::Ready);
    DriverReadyFixture {
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
    }
}

struct DriverRunFixture {
    ready: DriverReadyFixture,
    claimed: StoredExecution,
    callback: Arc<DriverOracle>,
    constraint: Arc<DriverConstraint>,
    application: Arc<NativeApplication>,
    catalog: Arc<PermissionCatalog>,
    authority: DriverAuthority,
    observed: DriverObservedStore,
    capacity: TaskCapacity,
    callback_calls: Arc<AtomicUsize>,
    callback_matches: Arc<AtomicUsize>,
}
fn driver_run_fixture(
    native: DriverNativeResult,
    read_fault: DriverReadFault,
    refuse_claimed: bool,
) -> DriverRunFixture {
    driver_run_fixture_with(
        native,
        read_fault,
        refuse_claimed,
        &DriverFixtureConfig::default(),
    )
}
fn driver_run_fixture_with(
    native: DriverNativeResult,
    read_fault: DriverReadFault,
    refuse_claimed: bool,
    config: &DriverFixtureConfig,
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
    } = driver_ready_fixture_with(config);
    let before = next.thread_view(&thread).unwrap().unwrap();
    let attempt = config.attempt_id.clone();
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
        result: native,
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
        refuse_claimed,
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
impl DriverRunFixture {
    fn drive<'a, H: PermissionUseDriverHooks + ?Sized>(
        &'a self,
        hooks: &'a H,
    ) -> Result<StoredExecution, PermissionUseDriverFailure<'a, DriverObservedStore, DriverAuthority>>
    {
        drive_permission_use_execution(
            PermissionUseDriverBindings {
                store: &self.observed,
                authority: &self.authority,
                producer_catalog: &self.catalog,
                current_catalog: &self.catalog,
                context: &self.ready.context,
                hooks,
                capacity: &self.capacity,
            },
            self.ready.authorized.clone(),
        )
    }
    fn capture(&self) -> DriverCapture {
        DriverCapture::read(&self.observed)
    }
    fn callback_once(&self) -> bool {
        self.callback_calls.load(Ordering::SeqCst) == 1
            && self.callback_matches.load(Ordering::SeqCst) == 1
            && self.ready.calls.load(Ordering::SeqCst) == 0
    }
    fn retire(self, expected: &DriverCapture) -> bool {
        let callback_weak = Arc::downgrade(&self.callback);
        let catalog_weak = Arc::downgrade(&self.catalog);
        let application_weak = Arc::downgrade(&self.application);
        let Self {
            ready,
            observed,
            catalog,
            application,
            callback,
            constraint,
            authority,
            ..
        } = self;
        drop(observed);
        drop(authority);
        drop(catalog);
        drop(application);
        drop(callback);
        drop(constraint);
        let retired = callback_weak.upgrade().is_none()
            && catalog_weak.upgrade().is_none()
            && application_weak.upgrade().is_none();
        let DriverReadyFixture {
            directory,
            next,
            thread,
            execution,
            second_execution,
            ..
        } = ready;
        assert_eq!(
            Arc::strong_count(&next),
            1,
            "all owner2 aliases retire normally"
        );
        let owner2 = next.owner_generation();
        next.readiness().unwrap();
        drop(next);
        let owner3 = SqliteStore::open(&directory.0).unwrap();
        let same = owner3.owner_generation() == owner2 + 1
            && owner3.thread_view(&thread).unwrap().as_ref() == Some(&expected.view)
            && owner3.execution(&execution).unwrap().as_ref() == Some(&expected.executions[0])
            && owner3.execution(&second_execution).unwrap().as_ref()
                == Some(&expected.executions[1])
            && owner3.journal(&execution).unwrap() == expected.journals[0]
            && owner3.journal(&second_execution).unwrap() == expected.journals[1];
        let raw = driver_raw_nine(&directory.0);
        let raw_same = raw.tables[1..] == expected.raw.tables[1..]
            && raw.tables[0][0][..3] == expected.raw.tables[0][0][..3]
            && produced_u64(&raw.tables[0][0][3]) == owner2 + 1;
        owner3.readiness().unwrap();
        drop(owner3);
        let path = directory.0.clone();
        drop(directory);
        let removed = !path.exists();
        println!("driver-control: reopened=1 removed={removed} anchors_retired={retired}");
        same && raw_same && retired && removed
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DriverReadFault {
    None,
    Definition,
    OutcomePortPanic,
    JournalAbort,
    ResourceRevision,
}
enum DriverNativeResult {
    Succeeded,
    Failed,
    Uncertain,
    Panicked {
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    },
}
struct DriverPanicWitness {
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}
impl Drop for DriverPanicWitness {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("driver_fixture_owned_payload_drop")
        }
    }
}
struct PanicCompletionHooks;
impl PermissionUseDriverHooks for PanicCompletionHooks {
    fn facts(&self, current: &StoredExecution) -> TransitionFacts {
        if current.snapshot.revision == 3 {
            panic!("driver_fixture_completion_clock")
        };
        facts(108)
    }
    fn attempt_id(&self) -> AttemptId {
        DriverHooks.attempt_id()
    }
}
fn same_authority_rows(before: &DriverCapture, after: &DriverCapture) -> bool {
    [0, 4, 5, 6, 7, 8]
        .iter()
        .all(|index| before.raw.tables[*index] == after.raw.tables[*index])
}

#[test]
fn permission_use_driver_returned_failure_is_persisted_once() {
    let fixture = driver_run_fixture(DriverNativeResult::Failed, DriverReadFault::None, false);
    let actual = fixture.drive(&DriverHooks);
    let after = fixture.capture();
    let before = fixture.observed.captured.lock().unwrap().take();
    let expected = transition(
        &fixture.claimed.snapshot,
        KernelCommand::FailAttempt {
            task_id: fixture.ready.task.clone(),
            attempt_id: fixture.claimed.snapshot.tasks[0]
                .attempt_id
                .clone()
                .unwrap(),
            failure: "native-failure".into(),
        },
        facts(109),
    )
    .unwrap();
    let valid = actual.as_ref().is_ok_and(|stored| {
        stored == &after.executions[1]
            && stored.snapshot == expected.snapshot
            && stored.snapshot.status == ExecutionStatus::Failed
            && stored.snapshot.tasks[0].status == TaskStatus::Failed
    }) && before.as_ref().is_some_and(|before| {
        same_authority_rows(before, &after)
            && after.view.thread.resource_revision == before.view.thread.resource_revision + 1
            && after.journals[1].len() == before.journals[1].len() + 1
    }) && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 1
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 1
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    assert!(
        valid && retired,
        "real Failed result must settle exact FailAttempt once before normal owner retirement"
    );
}

#[test]
fn permission_use_driver_candidate_definition_substitution_is_refused() {
    let fixture = driver_run_fixture(
        DriverNativeResult::Succeeded,
        DriverReadFault::Definition,
        false,
    );
    let mut actual = fixture.drive(&DriverHooks);
    let after = fixture.capture();
    let before = fixture.observed.captured.lock().unwrap().take();
    let substituted = fixture.observed.substituted.lock().unwrap().take();
    let kernel_valid = substituted.as_ref().is_some_and(|stored| {
        b4_kernel::validate_snapshot(&stored.snapshot).is_ok()
            && transition(
                &stored.snapshot,
                KernelCommand::Complete {
                    task_id: fixture.ready.task.clone(),
                    attempt_id: stored.snapshot.tasks[0].attempt_id.clone().unwrap(),
                    result: TaskResult {
                        output: json!(true),
                        writes: vec![],
                    },
                },
                facts(109),
            )
            .is_ok()
    });
    let retry_refused = actual.as_mut().err().is_some_and(|failure| {
        failure.retry_settlement(&facts(110)).is_err()
            && failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
            && failure.settled_outcome().is_some()
    });
    let valid = retry_refused
        && kernel_valid
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
                && failure.settled_outcome().is_some()
                && failure.usage().completion_entries() == 1
                && failure.usage().candidate_entries() == 1
                && failure.usage().candidate_refusals() == 1
        })
        && before.as_ref() == Some(&after)
        && after.executions[1] == fixture.claimed
        && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 0
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    let still_unchanged = fixture.capture() == after;
    drop(actual);
    let retired = fixture.retire(&after);
    assert!(valid && still_unchanged && retired,"kernel-valid observed definition change must reach full matcher and retain real Work before writer");
}

#[test]
fn permission_use_driver_raw_completion_remains_refused() {
    let fixture = driver_run_fixture(
        DriverNativeResult::Succeeded,
        DriverReadFault::Definition,
        false,
    );
    let actual = fixture.drive(&DriverHooks);
    let before = fixture.capture();
    let raw = actual
        .as_ref()
        .err()
        .and_then(|failure| failure.settled_outcome())
        .map(|outcome| {
            let EffectOutcome::Succeeded(result) = outcome else {
                panic!("expected genuine settled Bool result")
            };
            let plan = transition(
                &before.executions[1].snapshot,
                KernelCommand::Complete {
                    task_id: fixture.ready.task.clone(),
                    attempt_id: before.executions[1].snapshot.tasks[0]
                        .attempt_id
                        .clone()
                        .unwrap(),
                    result: result.clone(),
                },
                facts(109),
            )
            .unwrap();
            fixture.observed.commit(
                &fixture.ready.second_execution,
                (&before.executions[1].snapshot).into(),
                plan,
            )
        });
    let after = fixture.capture();
    let valid = matches!(raw, Some(Err(StoreError::UnsupportedOperation)))
        && before == after
        && fixture.callback_once()
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 1
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    assert!(valid && retired,"real raw SQLite guard must refuse the ordinary candidate observed from actual retained outcome");
}

#[test]
fn permission_use_driver_renewal_refusal_keeps_claimed_attempt() {
    let fixture = driver_run_fixture(DriverNativeResult::Succeeded, DriverReadFault::None, true);
    let actual = fixture.drive(&DriverHooks);
    let after = fixture.capture();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let valid = actual.as_ref().err().is_some_and(|failure| {
        failure.callback_status().is_none() && failure.usage().completion_entries() == 0
    }) && postclaim.as_ref() == Some(&after)
        && after.executions[1] == fixture.claimed
        && fixture.authority.ready_matches.load(Ordering::SeqCst) == 1
        && fixture.authority.claimed_matches.load(Ordering::SeqCst) == 1
        && fixture.callback_calls.load(Ordering::SeqCst) == 0
        && fixture.observed.entries.load(Ordering::SeqCst) == 1
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 0
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    assert!(
        valid && retired,
        "genuine Dispatch must remain claimed after exact claimed-resource renewal refusal"
    );
}

#[test]
fn permission_use_driver_nonsettled_receipt_retires() {
    let fixture = driver_run_fixture(DriverNativeResult::Uncertain, DriverReadFault::None, false);
    let mut actual = fixture.drive(&DriverHooks);
    let after = fixture.capture();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let valid = actual.as_mut().err().is_some_and(|failure| {
        failure.callback_status() == Some(PermissionUseCallbackStatus::Uncertain)
            && failure.settled_outcome().is_none()
            && failure.retry_settlement(&facts(110)).is_err()
    }) && postclaim.as_ref() == Some(&after)
        && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 0
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    assert!(
        valid && retired,
        "actual uncertain Receipt must remain nonsettled through ordinary owned retirement"
    );
}

#[test]
fn permission_use_driver_native_panic_payload_retires_inside_failure() {
    let drops = Arc::new(AtomicUsize::new(0));
    let fixture = driver_run_fixture(
        DriverNativeResult::Panicked {
            drops: drops.clone(),
            panic_on_drop: true,
        },
        DriverReadFault::None,
        false,
    );
    let actual = fixture.drive(&DriverHooks);
    let after = fixture.capture();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let valid = actual.as_ref().err().is_some_and(|failure| {
        failure.callback_status() == Some(PermissionUseCallbackStatus::Panicked)
            && failure.settled_outcome().is_none()
    }) && fixture.callback_once()
        && postclaim.as_ref() == Some(&after)
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 0
        && drops.load(Ordering::SeqCst) == 0;
    let drop_panicked =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(actual))).is_err();
    let owned_drop = drop_panicked && drops.load(Ordering::SeqCst) == 1;
    let retired = fixture.retire(&after);
    assert!(valid && owned_drop && retired,"the actual retained panic payload must drop inside owned failure retirement, then all owners close");
}

#[test]
fn permission_use_driver_completion_hook_panic_preserves_work() {
    let fixture = driver_run_fixture(DriverNativeResult::Succeeded, DriverReadFault::None, false);
    let mut actual = fixture.drive(&PanicCompletionHooks);
    let after = fixture.capture();
    let postclaim = fixture.observed.postclaim.lock().unwrap().take();
    let valid = actual.as_mut().err().is_some_and(|failure| {
        failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
            && failure.settled_outcome().is_some()
            && failure.retry_settlement(&facts(110)).is_err()
    }) && postclaim.as_ref() == Some(&after)
        && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 0
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    assert!(valid && retired,"trusted hook panic must preserve actual Work through generic Driver drainage and normal retirement");
}

#[test]
fn permission_use_driver_outcome_port_panic_restores_work_and_account() {
    let fixture = driver_run_fixture(
        DriverNativeResult::Succeeded,
        DriverReadFault::OutcomePortPanic,
        false,
    );
    let mut actual = fixture.drive(&DriverHooks);
    let after = fixture.capture();
    let before = fixture.observed.captured.lock().unwrap().take();
    let diagnostic = (
        actual
            .as_ref()
            .err()
            .map(|failure| (failure.callback_status(), failure.usage())),
        before.as_ref() == Some(&after),
        fixture.callback_once(),
        fixture.observed.outcome_entries.load(Ordering::SeqCst),
        fixture.observed.outcome_forwards.load(Ordering::SeqCst),
        fixture.observed.raw_commits.load(Ordering::SeqCst),
    );
    let valid = actual.as_mut().err().is_some_and(|failure| {
        failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
            && failure.settled_outcome().is_some()
            // Existing generic Driver drainage retries the completion observation;
            // the sticky local fault refuses entry two before another writer.
            && failure.usage().completion_entries() == 2
            && failure.usage().refused_completions() == 1
            && failure.usage().committed_completions() == 0
            && failure.usage().candidate_entries() == 1
            && failure.usage().candidate_refusals() == 0
            && failure.usage().encoded_bytes() > 131_072
            && failure.retry_settlement(&facts(110)).is_err()
    }) && before.as_ref() == Some(&after)
        && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 1
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 0
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    drop(actual);
    let retired = fixture.retire(&after);
    println!("driver-port-panic observations: {diagnostic:?}");
    assert!(valid && retired,"actual Work and same spent adapter account must restore on port unwind before any SQLite write");
}

fn driver_injection_connection(path: &std::path::Path) -> rusqlite::Connection {
    rusqlite::Connection::open_with_flags(
        path.join("runtime.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .unwrap()
}
fn driver_install_abort(
    path: &std::path::Path,
    execution: &ExecutionId,
    next_revision: u64,
    next_resource_revision: u64,
) -> bool {
    let connection = driver_injection_connection(path);
    let execution = execution.as_str().replace('\'', "''");
    let sql=format!("CREATE TRIGGER driver_test_abort_journal BEFORE INSERT ON journal
        WHEN NEW.kind='commit' AND NEW.execution_id='{execution}' AND NEW.revision=X'{next_revision:016x}'
        AND EXISTS (SELECT 1 FROM executions WHERE id=NEW.execution_id AND revision=NEW.revision)
        AND EXISTS (SELECT 1 FROM threads WHERE id=NEW.thread_id AND active_execution_id=NEW.execution_id AND resource_revision=X'{next_resource_revision:016x}')
        BEGIN SELECT RAISE(ABORT, 'driver_fixture_late_journal_abort'); END");
    connection.execute_batch(&sql).unwrap();
    let actual:String=connection.query_row("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='driver_test_abort_journal'",[],|row|row.get(0)).unwrap();
    connection.close().unwrap();
    actual == sql
}
fn driver_drop_abort(path: &std::path::Path) -> bool {
    let connection = driver_injection_connection(path);
    connection
        .execute_batch("DROP TRIGGER IF EXISTS driver_test_abort_journal")
        .unwrap();
    let count:i64=connection.query_row("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='driver_test_abort_journal'",[],|row|row.get(0)).unwrap();
    connection.close().unwrap();
    count == 0
}
// Test-local controlled corruption, armed before its one-field write. The
// original eight literal bytes survive through normal restoration or unwind.
struct DriverRevisionRestore {
    path: PathBuf,
    thread: ThreadId,
    original: Vec<u8>,
    armed: bool,
}
impl DriverRevisionRestore {
    fn corrupt(path: &std::path::Path, thread: &ThreadId) -> Self {
        let connection = driver_injection_connection(path);
        let original: Vec<u8> = connection
            .query_row(
                "SELECT resource_revision FROM threads WHERE id=?1",
                [thread.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        let revision = u64::from_be_bytes(original.as_slice().try_into().unwrap());
        let changed = revision.checked_add(1).unwrap().to_be_bytes();
        let restore = Self {
            path: path.to_path_buf(),
            thread: thread.clone(),
            original,
            armed: true,
        };
        let changed_rows = connection
            .execute(
                "UPDATE threads SET resource_revision=?2 WHERE id=?1",
                rusqlite::params![thread.as_str(), changed.as_slice()],
            )
            .unwrap();
        assert_eq!(changed_rows, 1);
        connection.close().unwrap();
        restore
    }
    fn restore(&mut self) -> bool {
        if !self.armed {
            return true;
        }
        let Ok(connection) = rusqlite::Connection::open_with_flags(
            self.path.join("runtime.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        ) else {
            return false;
        };
        let updated = connection.execute(
            "UPDATE threads SET resource_revision=?2 WHERE id=?1",
            rusqlite::params![self.thread.as_str(), self.original.as_slice()],
        );
        let actual = connection.query_row(
            "SELECT resource_revision FROM threads WHERE id=?1",
            [self.thread.as_str()],
            |row| row.get::<_, Vec<u8>>(0),
        );
        let restored = matches!(updated, Ok(1))
            && actual.as_ref().is_ok_and(|actual| actual == &self.original);
        let closed = connection.close().is_ok();
        if restored && closed {
            self.armed = false;
            true
        } else {
            false
        }
    }
}
impl Drop for DriverRevisionRestore {
    fn drop(&mut self) {
        let _restored = self.restore();
    }
}
impl DriverObservedStore {
    fn restore_revision(&self) -> bool {
        let mut restore = self.revision_restore.lock().unwrap().take();
        let restored = restore.as_mut().is_some_and(DriverRevisionRestore::restore);
        if !restored {
            *self.revision_restore.lock().unwrap() = restore;
        }
        restored
    }
}

#[test]
fn permission_use_driver_late_abort_retains_work_for_explicit_retry() {
    let fixture = driver_run_fixture(
        DriverNativeResult::Succeeded,
        DriverReadFault::JournalAbort,
        false,
    );
    let mut actual = fixture.drive(&DriverHooks);
    let after_abort = fixture.capture();
    let before = fixture.observed.captured.lock().unwrap().take();
    let before_usage = actual.as_ref().err().map(PermissionUseDriverFailure::usage);
    let first_rollback = fixture.observed.trigger_installed.load(Ordering::SeqCst)
        && before.as_ref() == Some(&after_abort)
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
                && failure.settled_outcome().is_some()
                && failure.usage().completion_entries() == 1
                && failure.usage().candidate_entries() == 1
                && failure.usage().candidate_refusals() == 0
        })
        && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 1
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 1
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    let removed_trigger = driver_drop_abort(&fixture.ready.directory.0);
    let retried = actual
        .as_mut()
        .err()
        .map(|failure| failure.retry_settlement(&facts(110)));
    let after = fixture.capture();
    let after_usage = actual.as_ref().err().map(PermissionUseDriverFailure::usage);
    let expected = transition(
        &fixture.claimed.snapshot,
        KernelCommand::Complete {
            task_id: fixture.ready.task.clone(),
            attempt_id: fixture.claimed.snapshot.tasks[0]
                .attempt_id
                .clone()
                .unwrap(),
            result: TaskResult {
                output: json!(true),
                writes: vec![],
            },
        },
        facts(110),
    )
    .unwrap();
    let repeat_refused = actual
        .as_mut()
        .err()
        .is_some_and(|failure| failure.retry_settlement(&facts(111)).is_err());
    let retry_success = retried.as_ref().is_some_and(|result| {
        result.as_ref().is_ok_and(|stored| {
            stored == &after.executions[1] && stored.snapshot == expected.snapshot
        })
    }) && before_usage
        .zip(after_usage)
        .is_some_and(|(before, after)| {
            after.encoded_bytes() > before.encoded_bytes()
                && after.structural_nodes() > before.structural_nodes()
        })
        && same_authority_rows(&after_abort, &after)
        && after.journals[1].len() == after_abort.journals[1].len() + 1
        && after.view.thread.resource_revision == after_abort.view.thread.resource_revision + 1
        && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 2
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 2
        && repeat_refused
        && fixture.capture() == after;
    drop(retried);
    drop(actual);
    let retired = fixture.retire(&after);
    println!("driver-retry: late_rollback={first_rollback} trigger_removed={removed_trigger} same_work_retry={retry_success} retired={retired}");
    assert!(
        first_rollback && removed_trigger && retired,
        "actual late trigger rollback and owned retirement must be healthy before retry oracle"
    );
    assert!(retry_success,"explicit storage-only retry must settle the same retained Work/account without callback replay");
}

#[test]
fn permission_use_driver_corrupt_resource_revision_preserves_work() {
    let fixture = driver_run_fixture(
        DriverNativeResult::Succeeded,
        DriverReadFault::ResourceRevision,
        false,
    );
    let actual = fixture.drive(&DriverHooks);
    let after_refusal = fixture.capture();
    let before_refusal = fixture.observed.captured.lock().unwrap().take();
    let healthy = fixture.observed.healthy.lock().unwrap().take();
    let original = fixture
        .observed
        .revision_restore
        .lock()
        .unwrap()
        .as_ref()
        .map(|restore| restore.original.clone());
    let only_revision = healthy.as_ref().is_some_and(|healthy| {
        healthy.executions == after_refusal.executions
            && healthy.journals == after_refusal.journals
            && healthy.view.thread.resource_revision + 1
                == after_refusal.view.thread.resource_revision
            && original.as_ref().is_some_and(|literal| {
                healthy.raw.tables[1][0][5] == rusqlite::types::Value::Blob(literal.clone())
            })
            && healthy.raw.tables.iter().enumerate().all(|(group, rows)| {
                rows.len() == after_refusal.raw.tables[group].len()
                    && rows.iter().enumerate().all(|(row, cells)| {
                        cells.len() == after_refusal.raw.tables[group][row].len()
                            && cells.iter().enumerate().all(|(column, cell)| {
                                (group == 1 && row == 0 && column == 5)
                                    || cell == &after_refusal.raw.tables[group][row][column]
                            })
                    })
            })
    });
    let refused = before_refusal.as_ref() == Some(&after_refusal)
        && actual.as_ref().err().is_some_and(|failure| {
            failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
                && failure.settled_outcome().is_some()
                && failure.usage().candidate_entries() == 1
                && failure.usage().candidate_refusals() == 0
        })
        && fixture.callback_once()
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 1
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    let literal_restored = fixture.observed.restore_revision();
    let restored = fixture.capture();
    let healthy_restored = healthy.as_ref() == Some(&restored);
    drop(actual);
    let retired = fixture.retire(&restored);
    println!("driver-resource-fault: one_field={only_revision} refused={refused} literal_restored={literal_restored} full_healthy_restored={healthy_restored}");
    assert!(only_revision && refused && literal_restored && healthy_restored && retired,"deliberate one-field corruption must refuse, preserve real Work, and restore literal state before normal retirement");
}

#[test]
fn permission_use_driver_repeated_storage_refusal_preserves_work_and_account() {
    let fixture = driver_run_fixture(
        DriverNativeResult::Succeeded,
        DriverReadFault::JournalAbort,
        false,
    );
    let mut actual = fixture.drive(&DriverHooks);
    let before = fixture.capture();
    let initial_usage = actual.as_ref().err().map(PermissionUseDriverFailure::usage);
    let retry = actual
        .as_mut()
        .err()
        .map(|failure| failure.retry_settlement(&facts(110)));
    let after = fixture.capture();
    let storage_refused = matches!(&retry, Some(Err(PermissionError::Store(_))));
    let retained = actual.as_ref().err().is_some_and(|failure| {
        failure.callback_status() == Some(PermissionUseCallbackStatus::Settled)
            && failure.settled_outcome().is_some()
            && initial_usage.is_some_and(|before| {
                failure.usage().encoded_bytes() > before.encoded_bytes()
                    && failure.usage().structural_nodes() > before.structural_nodes()
            })
    });
    let exact = before == after
        && fixture.callback_once()
        && fixture.observed.trigger_installed.load(Ordering::SeqCst)
        && fixture.observed.outcome_entries.load(Ordering::SeqCst) == 2
        && fixture.observed.outcome_forwards.load(Ordering::SeqCst) == 2
        && fixture.observed.raw_commits.load(Ordering::SeqCst) == 0;
    let trigger_removed = driver_drop_abort(&fixture.ready.directory.0);
    drop(retry);
    drop(actual);
    let retired = fixture.retire(&after);
    println!("driver-retry-refusal: storage_error={storage_refused} retained_same_work={retained} unchanged={exact} trigger_removed={trigger_removed}");
    assert!(storage_refused && retained && exact && trigger_removed && retired,"a second actual late storage refusal must preserve the same Work and cumulative account through healthy retirement");
}

#[path = "use_driver_inputs.rs"]
mod use_driver_inputs;

mod original_always_ids;
