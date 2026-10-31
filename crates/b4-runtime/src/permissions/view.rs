//! Owned partial permission projections; these types confer no execution authority.

use super::{PermissionAcceptance, PermissionDecision, PermissionEvidenceUsage, PermissionUse};
use crate::approval::{ApprovalAcceptance, ApprovalOperation, ApprovalResource, GrantRecord};
use crate::{InvocationContext, JournalEntry, StoredExecution, ThreadRecord};
use b4_contract::{Definition, TaskId};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionReadPurpose {
    Accept,
    Park,
    Pending,
    Dispatch,
    Recover,
}

impl PermissionReadPurpose {
    pub(crate) fn approval_operation(self) -> ApprovalOperation {
        match self {
            Self::Accept => ApprovalOperation::Accept,
            Self::Park => ApprovalOperation::Park,
            Self::Pending => ApprovalOperation::Pending,
            Self::Dispatch => ApprovalOperation::Dispatch,
            Self::Recover => ApprovalOperation::Recover,
        }
    }
}

#[derive(Clone, Copy)]
enum PermissionViewMode {
    Ordinary,
    Acceptance,
}

/// A comparison expectation constructed only by the runtime read service.
/// It cannot be deserialized or converted into permission authority.
pub struct PermissionViewSelector<'a> {
    context: &'a InvocationContext,
    definition: &'a Definition,
    purpose: PermissionReadPurpose,
    task_id: Option<&'a TaskId>,
    expected_resource: &'a ApprovalResource,
    expected_view: Option<&'a PermissionView>,
    mode: PermissionViewMode,
}

impl<'a> PermissionViewSelector<'a> {
    pub(crate) fn new(
        context: &'a InvocationContext,
        definition: &'a Definition,
        purpose: PermissionReadPurpose,
        task_id: Option<&'a TaskId>,
        expected_resource: &'a ApprovalResource,
        expected_view: Option<&'a PermissionView>,
    ) -> Self {
        Self {
            context,
            definition,
            purpose,
            task_id,
            expected_resource,
            expected_view,
            mode: PermissionViewMode::Ordinary,
        }
    }

    pub(crate) fn new_acceptance(
        context: &'a InvocationContext,
        definition: &'a Definition,
        expected_resource: &'a ApprovalResource,
        expected_view: Option<&'a PermissionView>,
    ) -> Self {
        Self {
            context,
            definition,
            purpose: PermissionReadPurpose::Accept,
            task_id: None,
            expected_resource,
            expected_view,
            mode: PermissionViewMode::Acceptance,
        }
    }

    pub fn is_acceptance_capture(&self) -> bool {
        matches!(self.mode, PermissionViewMode::Acceptance)
    }

    pub fn context(&self) -> &InvocationContext {
        self.context
    }

    pub fn definition(&self) -> &Definition {
        self.definition
    }

    pub fn purpose(&self) -> PermissionReadPurpose {
        self.purpose
    }

    pub fn task_id(&self) -> Option<&TaskId> {
        self.task_id
    }

    pub fn expected_resource(&self) -> &ApprovalResource {
        self.expected_resource
    }

    pub fn expected_view(&self) -> Option<&PermissionView> {
        self.expected_view
    }
}

impl fmt::Debug for PermissionViewSelector<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionViewSelector([REDACTED])")
    }
}

/// Immutable non-authoritative capture facts, never an account reconstruction.
pub struct PermissionViewReceipt {
    purpose: PermissionReadPurpose,
    usage_at_capture: PermissionEvidenceUsage,
}

impl PermissionViewReceipt {
    pub fn purpose(&self) -> PermissionReadPurpose {
        self.purpose
    }

    pub fn usage_at_capture(&self) -> PermissionEvidenceUsage {
        self.usage_at_capture
    }
}

impl fmt::Debug for PermissionViewReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionViewReceipt([REDACTED])")
    }
}

#[derive(Clone, Copy)]
enum PermissionViewProjection {
    LegacyEmpty,
    FirstSelectedComplete,
}
const _: () = assert!(std::mem::size_of::<PermissionViewProjection>() <= 1);

/// No public construction, cloning or serialization of sensitive projections.
/// New-reference collections contain only the future required authority graph,
/// rather than all ordinary journal or approval history.
pub struct PermissionView {
    resource: ApprovalResource,
    grants: Vec<GrantRecord>,
    acceptances: Vec<ApprovalAcceptance>,
    permission_acceptances: Vec<PermissionAcceptance>,
    permission_decisions: Vec<PermissionDecision>,
    permission_uses: Vec<PermissionUse>,
    creator_acceptances: Vec<PermissionAcceptance>,
    referenced_grants: Vec<GrantRecord>,
    referenced_acceptances: Vec<ApprovalAcceptance>,
    journal_entries: Vec<JournalEntry>,
    receipt: PermissionViewReceipt,
    capture_quote: Option<super::validation::PermissionAcceptanceCapture>,
    projection: PermissionViewProjection,
}

impl PermissionView {
    pub(super) fn from_checked_legacy(
        capture: super::validation::FinishedPermissionLegacyView,
    ) -> Self {
        let (resource, grants, acceptances, purpose, usage_at_capture, capture_quote) =
            capture.into_parts();
        Self {
            resource,
            grants,
            acceptances,
            permission_acceptances: Vec::new(),
            permission_decisions: Vec::new(),
            permission_uses: Vec::new(),
            creator_acceptances: Vec::new(),
            referenced_grants: Vec::new(),
            referenced_acceptances: Vec::new(),
            journal_entries: Vec::new(),
            receipt: PermissionViewReceipt {
                purpose,
                usage_at_capture,
            },
            capture_quote,
            projection: PermissionViewProjection::LegacyEmpty,
        }
    }

    pub(super) fn from_checked_populated(
        capture: super::validation::FinishedPermissionPopulatedView,
    ) -> Self {
        let (resource, grants, acceptances, decisions, journals, purpose, usage) =
            capture.into_parts();
        Self {
            resource,
            grants,
            acceptances: Vec::new(),
            permission_acceptances: acceptances,
            permission_decisions: decisions,
            permission_uses: Vec::new(),
            creator_acceptances: Vec::new(),
            referenced_grants: Vec::new(),
            referenced_acceptances: Vec::new(),
            journal_entries: journals,
            receipt: PermissionViewReceipt {
                purpose,
                usage_at_capture: usage,
            },
            capture_quote: None,
            projection: PermissionViewProjection::FirstSelectedComplete,
        }
    }

    pub(super) fn acceptance_capture(
        &self,
    ) -> Option<&super::validation::PermissionAcceptanceCapture> {
        self.capture_quote.as_ref()
    }

    pub fn resource(&self) -> &ApprovalResource {
        &self.resource
    }

    pub fn thread(&self) -> &ThreadRecord {
        &self.resource.thread
    }

    pub fn execution(&self) -> &StoredExecution {
        &self.resource.execution
    }

    pub fn grants(&self) -> &[GrantRecord] {
        &self.grants
    }

    pub fn acceptances(&self) -> &[ApprovalAcceptance] {
        &self.acceptances
    }

    pub fn permission_acceptances(&self) -> &[PermissionAcceptance] {
        &self.permission_acceptances
    }

    pub fn permission_decisions(&self) -> &[PermissionDecision] {
        &self.permission_decisions
    }

    pub fn permission_uses(&self) -> &[PermissionUse] {
        &self.permission_uses
    }

    pub fn creator_acceptances(&self) -> &[PermissionAcceptance] {
        match self.projection {
            PermissionViewProjection::LegacyEmpty => &self.creator_acceptances,
            PermissionViewProjection::FirstSelectedComplete => &self.permission_acceptances,
        }
    }

    pub fn referenced_grants(&self) -> &[GrantRecord] {
        match self.projection {
            PermissionViewProjection::LegacyEmpty => &self.referenced_grants,
            PermissionViewProjection::FirstSelectedComplete => &self.grants,
        }
    }

    pub fn referenced_acceptances(&self) -> &[ApprovalAcceptance] {
        &self.referenced_acceptances
    }

    pub fn journal_entries(&self) -> &[JournalEntry] {
        &self.journal_entries
    }

    pub fn receipt(&self) -> &PermissionViewReceipt {
        &self.receipt
    }
}

impl fmt::Debug for PermissionView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PermissionView([REDACTED])")
    }
}

const _: () = assert!(std::mem::size_of::<PermissionView>() <= 1_024);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::*;
    use crate::permissions::*;
    use crate::*;
    use b4_contract::*;
    use serde_json::{json, Map};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct Allow;
    impl ApprovalAuthority for Allow {
        fn authorize(
            &self,
            _: &ApprovalContext,
            _: ApprovalOperation,
            _: &ApprovalResource,
        ) -> Result<(), ApprovalError> {
            Ok(())
        }
    }
    struct CountNative(Arc<AtomicUsize>);
    impl NativeEffect for CountNative {
        fn invoke(
            &self,
            _: &EffectInvocation,
            _: &InvocationContext,
        ) -> Result<NativeOutcome, EffectUncertain> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(NativeOutcome::Succeeded(TaskResult {
                output: json!(null),
                writes: vec![],
            }))
        }
    }
    struct CountConstraint(Arc<AtomicUsize>);
    impl ToolConstraint for CountConstraint {
        fn validate_call(&self, _: &PermissionCall<'_>) -> Result<(), ToolConstraintError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    fn actual_fence(store: &MemoryStore, id: &ExecutionId) -> ApprovalFence {
        let view = store.approval_view(id).unwrap().unwrap();
        ApprovalFence {
            execution: (&view.execution.snapshot).into(),
            resource_revision: view.thread.resource_revision,
        }
    }

    #[test]
    fn combined_some_expected_view_refuses_twice_then_exhausts_the_same_account() {
        // A real volatile native Park supplies the context and old records.
        // The private expected View below is test data, not a checked capture.
        let native_calls = Arc::new(AtomicUsize::new(0));
        let constraint_calls = Arc::new(AtomicUsize::new(0));
        let origin = ThreadOrigin {
            application: "some-fixture-app".into(),
            tenant: "tenant".into(),
            principal: "principal".into(),
        };
        let definition = Definition {
            route: RouteKey::new("/some-fixture#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("first").unwrap(),
                effect_id: EffectId::new("first-effect").unwrap(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: true,
            }]],
            max_attempts: 4,
        };
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: origin.application.clone(),
                    revision: "application-1".into(),
                },
                vec![RouteRegistration {
                    definition: definition.clone(),
                    effects: BTreeMap::from([(
                        definition.batches[0][0].effect_id.clone(),
                        EffectBinding {
                            callback: Arc::new(CountNative(native_calls.clone())),
                            output_kind: OutputKind::Value,
                        },
                    )]),
                    input_validator: None,
                }],
            )
            .unwrap(),
        );
        let registered = application.definition(&definition.route).unwrap().clone();
        assert!(
            registered == definition,
            "actual native registration supplies the full Definition"
        );
        let constraint = Arc::new(CountConstraint(constraint_calls.clone()));
        let catalog = PermissionCatalog::new(
            application.clone(),
            vec![ToolBindingRegistration {
                definition: registered.clone(),
                effect_id: definition.batches[0][0].effect_id.clone(),
                tool_id: StableToolId::new("first-effect").unwrap(),
                tool_revision: StableToolRevision::new("tool-1").unwrap(),
                constraint_revision: ToolConstraintRevision::new("constraint-1").unwrap(),
                constraint: constraint.clone(),
            }],
        )
        .unwrap();
        let store = Arc::new(MemoryStore::new());
        assert!(
            !store.approval_durable(),
            "MemoryStore remains explicitly volatile"
        );
        let thread_id = ThreadId::new("some-thread").unwrap();
        let id = ExecutionId::new("some-execution").unwrap();
        let thread = store
            .create_thread_with(ThreadSeed {
                id: thread_id.clone(),
                created_at_ms: 100,
                metadata: Map::new(),
                origin: Some(origin),
            })
            .unwrap();
        let context = ApprovalContext {
            actor: thread.origin.as_ref().unwrap().clone(),
            policy_revision: "policy-1".into(),
        };
        let initial = b4_kernel::new_execution(
            id.clone(),
            TurnId::new("original-turn").unwrap(),
            store.owner_generation(),
            registered,
            Map::from_iter([("payload".into(), json!({"fixture": true, "nullable": null}))]),
            TransitionFacts { now_ms: 100 },
        )
        .unwrap();
        let admitted = store.admit(&thread_id, initial).unwrap();
        let started = store
            .commit(
                &id,
                (&admitted.snapshot).into(),
                b4_kernel::transition(
                    &admitted.snapshot,
                    KernelCommand::Start,
                    TransitionFacts { now_ms: 101 },
                )
                .unwrap(),
            )
            .unwrap();
        let service = ApprovalService::with_config(
            store.clone(),
            Allow,
            ApprovalServiceConfig::VolatileFixture,
        )
        .unwrap();
        let parked = service
            .park(
                &context,
                &id,
                actual_fence(&store, &id),
                started.snapshot.tasks[0].spec.id.clone(),
                InterruptId::new("first-interrupt").unwrap(),
                json!({"question": "Fixture?"}),
                1_000,
                TransitionFacts { now_ms: 102 },
            )
            .unwrap();
        let pending = service
            .pending(&context, &id, actual_fence(&store, &id))
            .unwrap();
        let before = store.approval_view(&id).unwrap().unwrap();
        let before_thread = store.thread_view(&thread_id).unwrap().unwrap();
        let before_journal = store.journal(&id).unwrap();
        b4_kernel::validate_snapshot(&before.execution.snapshot).unwrap();
        assert!(
            before.execution == parked
                && parked.snapshot.revision == 2
                && parked.snapshot.status == ExecutionStatus::WaitingForApproval
                && before.thread.resource_revision == 3
                && before.grants.len() == 1
                && before.acceptances.is_empty()
                && pending.len() == 1,
            "actual public Park supplies one complete legal old grant"
        );
        let task = &before.execution.snapshot.tasks[0];
        let grant = &before.grants[0];
        assert!(
            task.spec == definition.batches[0][0]
                && task.status == TaskStatus::WaitingForApproval
                && !task.approved
                && task.input == parked.snapshot.values
                && task.attempt_id.is_none()
                && task.result.is_none()
                && task.failure.is_none()
                && grant.thread_id == thread_id
                && grant.thread_generation == before.thread.generation
                && grant.origin == context.actor
                && grant.execution_id == id
                && grant.route == definition.route
                && grant.definition_revision == definition.revision
                && grant.task_id == task.spec.id
                && grant.effect_id == task.spec.effect_id
                && grant.interrupt_id == pending[0].interrupt_id
                && grant.prompt == pending[0].prompt
                && grant.token == pending[0].token
                && grant.verifier == token_verifier(&pending[0].token)
                && grant.resume_key == pending[0].resume_key
                && grant.policy_revision == context.policy_revision
                && grant.task_parking_revision == 2
                && grant.issued_at_ms == 102
                && grant.expires_at_ms == 1_102
                && !grant.consumed,
            "the actual task, token and incarnation retain their genuine public bindings"
        );
        let resource = ApprovalResource {
            thread: before.thread.clone(),
            execution: before.execution.clone(),
        };
        let invocation = InvocationContext {
            thread_id: thread_id.clone(),
            thread_generation: before.thread.generation.clone(),
            origin: context.actor.clone(),
            application_revision: catalog.application_identity().revision.clone(),
            execution_revision: definition.revision.clone(),
            policy_revision: context.policy_revision.clone(),
        };
        let fence = actual_fence(&store, &id);
        let (none_result, first, second, third, sticky, fresh_digest, usages) = {
            let mut account = PermissionEvidenceBudget::new();
            let expected = PermissionView {
                resource: resource.clone(),
                grants: before.grants.clone(),
                acceptances: before.acceptances.clone(),
                permission_acceptances: vec![],
                permission_decisions: vec![],
                permission_uses: vec![],
                creator_acceptances: vec![],
                referenced_grants: vec![],
                referenced_acceptances: vec![],
                journal_entries: vec![],
                capture_quote: None,
                projection: PermissionViewProjection::LegacyEmpty,
                receipt: PermissionViewReceipt {
                    purpose: PermissionReadPurpose::Pending,
                    usage_at_capture: account.usage(),
                },
            };
            // Only this descendant test module may construct the nonauthority DTO.
            let none = PermissionViewSelector::new(
                &invocation,
                &definition,
                PermissionReadPurpose::Pending,
                None,
                &resource,
                None,
            );
            let mut none_account = PermissionEvidenceBudget::new();
            let none_result = none_account.combined_read(&id, fence, &none).map(drop);
            let mut digest_account = PermissionEvidenceBudget::new();
            let fresh_digest = permission_call_digest(task, &mut digest_account);
            let some = PermissionViewSelector::new(
                &invocation,
                &definition,
                PermissionReadPurpose::Pending,
                None,
                &resource,
                Some(&expected),
            );
            let initial_usage = account.usage();
            let first = account.combined_read(&id, fence, &some).map(drop);
            let first_usage = account.usage();
            let second = account.combined_read(&id, fence, &some).map(drop);
            let second_usage = account.usage();
            let third = account.combined_read(&id, fence, &some).map(drop);
            let third_usage = account.usage();
            let sticky = permission_call_digest(task, &mut account);
            let final_usage = account.usage();
            (
                none_result,
                first,
                second,
                third,
                sticky,
                fresh_digest,
                [
                    initial_usage,
                    first_usage,
                    second_usage,
                    third_usage,
                    final_usage,
                ],
            )
        };
        let after = store.approval_view(&id).unwrap().unwrap();
        let after_thread = store.thread_view(&thread_id).unwrap().unwrap();
        let after_journal = store.journal(&id).unwrap();
        store.readiness().unwrap();
        drop(service);
        drop(parked);
        drop(started);
        drop(admitted);
        drop(thread);
        drop(catalog);
        drop(constraint);
        drop(application);
        assert!(
            Arc::strong_count(&store) == 1,
            "all volatile writer aliases retire normally"
        );
        drop(store);
        let native_count = native_calls.load(Ordering::SeqCst);
        let constraint_count = constraint_calls.load(Ordering::SeqCst);
        drop(native_calls);
        drop(constraint_calls);
        // Owned outputs are evaluated only after ordinary disposal. No SQLite
        // connection/order/reopening or matched-prior-view comparison is claimed.
        assert!(
            none_result.is_ok() && fresh_digest.is_ok(),
            "separate fresh admission and actual-task hash controls succeed"
        );
        assert!(
            matches!(first, Err(PermissionError::UnsupportedProfile))
                && matches!(second, Err(PermissionError::UnsupportedProfile)),
            "Some explicitly refuses both admitted attempts"
        );
        assert!(
            matches!(third, Err(PermissionError::LimitExceeded)),
            "the same account refuses the third combined admission"
        );
        assert!(
            matches!(sticky, Err(PermissionError::LimitExceeded)),
            "the exhausted account remains sticky through the installed actual-task hash"
        );
        assert!(before == after && before_thread == after_thread && before_journal == after_journal && native_count == 0 && constraint_count == 0,
            "private phase admission preserves genuine storage and invokes no native gate or effect");
        assert!(
            usages
                .windows(2)
                .all(|pair| pair[0].encoded_bytes() <= pair[1].encoded_bytes()
                    && pair[0].structural_nodes() <= pair[1].structural_nodes())
                && usages[4].encoded_bytes() <= 8_388_608
                && usages[4].structural_nodes() <= 65_536,
            "the actual single subject account stays finite and monotone"
        );
    }
}
