//! Owned NewUse renewal boundary with independent combined C2 and final R4.
#![allow(dead_code)] // C2 declarations are retained for the independently authored RED.
use super::runtime_mutation::{PermissionRuntimeOrigin, PermissionUseClaimLoan};
use super::runtime_use::{PermissionRuntimeUseSource, PermissionRuntimeUseWitness};
use super::{
    validation, PermissionCatalog, PermissionDispatchClaim, PermissionError,
    PermissionEvidenceBudget, PermissionEvidenceUsage, PermissionResourceRead, PermissionStore,
};
use crate::approval::{
    ApprovalAuthority, ApprovalContext, ApprovalError, ApprovalOperation, ApprovalResource,
};
use crate::InvocationContext;
use b4_contract::{Definition, ExecutionId};
use std::{mem::size_of, sync::Arc};

// Retained positive scaffold prices remain; each new C2/gate/R4/Permit
// incarnation has separately applied fixed and observed tickets.
pub(super) const ACCOUNT_BOUND: usize = 512;
pub(super) const ACCOUNT_RESULT_BOUND: usize = 512;
pub(super) const UNIT_RESULT_BOUND: usize = 128;
pub(super) const LOAN_BOUND: usize = 128;
pub(super) const OBSERVATION_BOUND: usize = 2048;
pub(super) const PERMIT_BOUND: usize = 8192;
pub(super) const CONSTRUCTOR_FIXED: usize =
    3 * ACCOUNT_BOUND + 3 * ACCOUNT_RESULT_BOUND + 4 * LOAN_BOUND + 6 * UNIT_RESULT_BOUND + 4 * 128;
pub(super) const OPERATION_FIXED: usize = 5 * PERMIT_BOUND
    + 10 * OBSERVATION_BOUND
    + 32 * LOAN_BOUND
    + 48 * UNIT_RESULT_BOUND
    + 12 * 256
    + 8 * 128;
pub(super) const LEAF_FIXED: usize = 4 * LOAN_BOUND + 4 * UNIT_RESULT_BOUND;
// Closed vendor actions: k<=4 builtin parameters, c<=14 borrowed cells.
// Each whole Try has nineteen independent full-weight nominal placements.
pub(super) const SDK_ACTION_FIXED: usize = 128 * 128
    + 7 * 512
    + 19 * (3 * 512 + 6 * 64)
    + (16 * 512 + 4 * 64)
    + 14 * (19 * 64 + 8 * 64 + 32 * 32)
    + 4 * (19 * (128 + 2 * 64) + 16 * 128);
// Three per-cell helpers and the separate once-per-query length aggregator.
// Decoded data owners and the single primary diagnostic remain separate.
pub(super) const ADAPTER_ACTION_FIXED: usize =
    14 * (4 * 19 * 128 + 3 * 16 * 128) + 8 * 512 + 16 * 128;
pub(super) const CODE_ONLY_FIXED: usize = 5 * OBSERVATION_BOUND
    + 6 * LOAN_BOUND
    + 3 * 128
    + 8 * UNIT_RESULT_BOUND
    + 256
    + 128
    + 4 * 32
    + 8 * (SDK_ACTION_FIXED + ADAPTER_ACTION_FIXED);
pub(super) const SOURCE_FIXED: usize = 4 * 512 + 8 * LOAN_BOUND + 8 * UNIT_RESULT_BOUND;

struct PermissionUseRenewalOrigin;
#[derive(Clone, Copy, PartialEq, Eq)]
enum AccountPhase {
    Fresh,
    Entered,
    Refused,
    Finished,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ResourcePhase {
    R3,
    R4,
}

/// One owned producer binding and one monotonically consumed renewal operation.
/// It contains no Claim, Resource or Catalog reference.
pub struct PermissionUseRenewalAccount {
    budget: PermissionEvidenceBudget,
    producer: Arc<PermissionRuntimeOrigin>,
    operation: Arc<PermissionUseRenewalOrigin>,
    phase: AccountPhase,
    resource_phase: Option<ResourcePhase>,
    source_entered: bool,
}
impl PermissionUseRenewalAccount {
    pub fn for_claim(claim: &PermissionDispatchClaim<'_>) -> Result<Self, PermissionError> {
        let mut budget = PermissionEvidenceBudget::fresh_use_renewal_production();
        // Freshness precedes the first charged helper; no debit-then-zero check.
        if budget.usage().encoded_bytes() != 0 || budget.usage().structural_nodes() != 0 {
            return Err(PermissionError::UnsupportedProfile);
        }
        budget.runtime_reserve(CONSTRUCTOR_FIXED, 24)?;
        let loan = claim.renewal_use_loan(&mut budget)?;
        // These two allocation/strong-owner constructions are paid above. The
        // genuine loan ends at publication; the Account never borrows its Claim.
        let producer = Arc::clone(loan.origin);
        let operation = Arc::new(PermissionUseRenewalOrigin);
        Ok(Self {
            budget,
            producer,
            operation,
            phase: AccountPhase::Fresh,
            resource_phase: None,
            source_entered: false,
        })
    }
    pub fn usage(&self) -> PermissionEvidenceUsage {
        self.budget.usage()
    }
    fn enter(&mut self) -> Result<(), PermissionError> {
        if self.phase != AccountPhase::Fresh {
            return Err(PermissionError::UnsupportedProfile);
        }
        // Consume eligibility before every fallible entered action, including the
        // first purchase and nonUse/wrong-origin discrimination.
        self.phase = AccountPhase::Entered;
        self.budget.runtime_reserve(OPERATION_FIXED, 128)?;
        // New owned FnOnce input/formal/output/end and future Permit transports.
        self.budget
            .runtime_reserve(8 * PERMIT_BOUND + 8 * LEAF_FIXED, 64)?;
        // The Account owns the Resource key ledger through every terminal end.
        // Buy its independent outer holder/buffer/retirement controls before R3.
        self.budget.runtime_reserve(8 * 128, 8)?;
        self.budget.enter_use_renewal_production()
    }
}

pub struct PermissionUseRenewalResourceSelector<'input> {
    execution_id: &'input ExecutionId,
    producer: &'input Arc<PermissionRuntimeOrigin>,
    operation: &'input Arc<PermissionUseRenewalOrigin>,
    phase: ResourcePhase,
}
pub struct PermissionUseRenewalResourceSession<'account, 'input> {
    account: &'account mut PermissionUseRenewalAccount,
    selector: &'account PermissionUseRenewalResourceSelector<'input>,
}
pub struct PermissionUseRenewalObservation {
    resource: ApprovalResource,
    producer: Arc<PermissionRuntimeOrigin>,
    operation: Arc<PermissionUseRenewalOrigin>,
    phase: ResourcePhase,
}
impl<'input> PermissionUseRenewalResourceSelector<'input> {
    pub fn begin<'account>(
        &'account self,
        account: &'account mut PermissionUseRenewalAccount,
    ) -> Result<PermissionUseRenewalResourceSession<'account, 'input>, PermissionError> {
        account.budget.runtime_reserve(LEAF_FIXED, 8)?;
        if account.phase != AccountPhase::Entered
            || !Arc::ptr_eq(self.producer, &account.producer)
            || !Arc::ptr_eq(self.operation, &account.operation)
            || match self.phase {
                ResourcePhase::R3 => account.resource_phase.is_some(),
                ResourcePhase::R4 => {
                    account.resource_phase != Some(ResourcePhase::R3) || !account.source_entered
                }
            }
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        account.resource_phase = Some(self.phase);
        Ok(PermissionUseRenewalResourceSession {
            account,
            selector: self,
        })
    }
}
impl<'account, 'input> PermissionUseRenewalResourceSession<'account, 'input> {
    pub fn execution_id(&mut self) -> Result<&'input ExecutionId, PermissionError> {
        self.account.budget.runtime_reserve(LEAF_FIXED, 8)?;
        Ok(self.selector.execution_id)
    }
    pub fn resource_read(&mut self) -> Result<PermissionResourceRead<'_>, PermissionError> {
        self.account.budget.use_renewal_resource_read()
    }
    pub fn before_code_only_scope(&mut self) -> Result<(), PermissionError> {
        self.account.budget.runtime_reserve(CODE_ONLY_FIXED, 40)
    }
    pub fn finish(
        self,
        resource: ApprovalResource,
    ) -> Result<PermissionUseRenewalObservation, PermissionError> {
        self.account.budget.runtime_reserve(LEAF_FIXED, 8)?;
        if &resource.execution.snapshot.execution_id != self.selector.execution_id {
            return Err(PermissionError::CorruptRecord);
        }
        Ok(PermissionUseRenewalObservation {
            resource,
            producer: Arc::clone(self.selector.producer),
            operation: Arc::clone(self.selector.operation),
            phase: self.selector.phase,
        })
    }
}

/// Independent C2 roles; old retained indices are never used positionally.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PermissionUseRenewalSource {
    CurrentAcceptance,
    CurrentLegacyAcceptance,
    CurrentGrant,
    OriginalLegacyAcceptance,
    CurrentUse,
    ThreadDecision,
    GlobalUseIdentity,
    OriginalAcceptance,
    OriginalGrant,
    Creator,
    OriginalJournal,
    CurrentJournal,
}
impl PermissionUseRenewalSource {
    pub(super) fn retained(self) -> Option<PermissionRuntimeUseSource> {
        use PermissionRuntimeUseSource as R;
        Some(match self {
            Self::CurrentAcceptance => return None,
            Self::CurrentLegacyAcceptance => R::CurrentLegacyAcceptance,
            Self::CurrentGrant => R::CurrentGrant,
            Self::OriginalLegacyAcceptance => R::OriginalLegacyAcceptance,
            Self::CurrentUse => R::CurrentUse,
            Self::ThreadDecision => R::ThreadDecision,
            Self::GlobalUseIdentity => R::GlobalUseIdentity,
            Self::OriginalAcceptance => R::OriginalAcceptance,
            Self::OriginalGrant => R::OriginalGrant,
            Self::Creator => R::OriginalCreator,
            Self::OriginalJournal => R::OriginalJournal,
            Self::CurrentJournal => R::CurrentJournal,
        })
    }
    pub(super) fn count(self) -> usize {
        match self {
            Self::CurrentAcceptance
            | Self::CurrentLegacyAcceptance
            | Self::CurrentGrant
            | Self::OriginalLegacyAcceptance => 0,
            Self::OriginalJournal => 5,
            Self::CurrentJournal => 4,
            _ => 1,
        }
    }
    // J4 requires all four validated rows; only these three match the retained J3.
    pub(super) fn retained_prefix(self) -> usize {
        if self == Self::CurrentJournal {
            3
        } else {
            self.count()
        }
    }
}
pub struct PermissionUseRenewalSourceSelector<'input> {
    resource: &'input ApprovalResource,
    witness: &'input PermissionRuntimeUseWitness,
    producer: &'input Arc<PermissionRuntimeOrigin>,
    operation: &'input Arc<PermissionUseRenewalOrigin>,
    context: &'input InvocationContext,
    invocation: &'input crate::EffectInvocation,
    current_catalog: &'input PermissionCatalog,
}
pub struct PermissionUseRenewalSourceSession<'account, 'input> {
    account: &'account mut PermissionUseRenewalAccount,
    selector: &'account PermissionUseRenewalSourceSelector<'input>,
    state: Box<RenewalSourceState>,
    headers_ended: [bool; 12],
    payloads_ended: [bool; 12],
}
// Source success is sealed only by complete C2 Session::finish.
pub struct PermissionUseRenewalSourceCapture {
    seal: PermissionUseRenewalProof,
}
struct PermissionUseRenewalProof {
    origin: Arc<PermissionUseRenewalOrigin>,
}
impl<'input> PermissionUseRenewalSourceSelector<'input> {
    pub fn begin<'account>(
        &'account self,
        account: &'account mut PermissionUseRenewalAccount,
    ) -> Result<PermissionUseRenewalSourceSession<'account, 'input>, PermissionError> {
        account.budget.runtime_reserve(SOURCE_FIXED, 16)?;
        if account.phase != AccountPhase::Entered
            || account.source_entered
            || account.resource_phase != Some(ResourcePhase::R3)
            || !Arc::ptr_eq(self.producer, &account.producer)
            || !Arc::ptr_eq(self.operation, &account.operation)
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        account.budget.runtime_reserve(SOURCE_STATE_FIXED, 128)?;
        account.source_entered = true;
        // Real, paid, borrowed retained source leaf; no key/raw clone.
        let _retained = self.witness.renewal_source_loan(&mut account.budget)?;
        Ok(PermissionUseRenewalSourceSession {
            account,
            selector: self,
            state: Box::new(RenewalSourceState::new()),
            headers_ended: [false; 12],
            payloads_ended: [false; 12],
        })
    }
}
impl PermissionUseRenewalSourceSession<'_, '_> {
    pub fn expected_count(&self, source: PermissionUseRenewalSource) -> usize {
        if source == PermissionUseRenewalSource::OriginalJournal {
            self.selector.witness.original_journal_count()
        } else {
            source.count()
        }
    }

    pub fn refuse(self) -> Result<PermissionUseRenewalSourceCapture, PermissionError> {
        self.account.budget.runtime_reserve(LEAF_FIXED, 8)?;
        Err(PermissionError::UnsupportedProfile)
    }
}

/// Owned publication retains independent producer/current/operation anchors.
/// Only the full C2 / one Claimed gate / final R4 service constructs this type.
pub struct PermissionUseDispatchPermit<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    claim: PermissionDispatchClaim<'producer>,
    catalog: &'current PermissionCatalog,
    selected_definition: &'current Definition,
    store: &'op S,
    authority: &'op A,
    context: InvocationContext,
    proof: PermissionUseRenewalProof,
}

pub fn renew_permission_use_dispatch<'producer, 'current, 'op, S, A>(
    claim: PermissionDispatchClaim<'producer>,
    store: &'op S,
    authority: &'op A,
    current_catalog: &'current PermissionCatalog,
    context: &ApprovalContext,
    account: &mut PermissionUseRenewalAccount,
) -> Result<PermissionUseDispatchPermit<'producer, 'current, 'op, S, A>, PermissionError>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    // Reuse never enters, but an entered operation is terminal even if this first
    // purchase fails. The by-value Claim is consumed on either path.
    if let Err(error) = account.enter() {
        if account.phase == AccountPhase::Entered {
            account.phase = AccountPhase::Refused;
        }
        return Err(error);
    }
    assert_permit::<S, A>();
    let result = renewal_checked_iife(|| {
        let loan: PermissionUseClaimLoan<'_> = claim.renewal_use_loan(&mut account.budget)?;
        if !Arc::ptr_eq(loan.origin, &account.producer) {
            return Err(PermissionError::UnsupportedProfile);
        }
        let producer = Arc::clone(&account.producer);
        let operation = Arc::clone(&account.operation);
        let selector = PermissionUseRenewalResourceSelector {
            execution_id: &loan.resource.execution.snapshot.execution_id,
            producer: &producer,
            operation: &operation,
            phase: ResourcePhase::R3,
        };
        let observed = store.capture_permission_use_renewal_resource(&selector, account)?;
        if observed.phase != ResourcePhase::R3
            || !Arc::ptr_eq(&producer, &observed.producer)
            || !Arc::ptr_eq(&operation, &observed.operation)
        {
            return Err(PermissionError::CorruptRecord);
        }
        validation::use_renewal_quote_resource_pair(
            loan.resource,
            &observed.resource,
            &mut account.budget,
        )?;
        if observed.resource != *loan.resource
            || store.owner_generation() != observed.resource.execution.snapshot.owner_generation
        {
            return Err(PermissionError::CorruptRecord);
        }
        validation::use_renewal_quote_context(context, &observed.resource, &mut account.budget)?;
        authority
            .authorize(context, ApprovalOperation::Dispatch, &observed.resource)
            .map_err(renewal_checked_value(|_| ApprovalError::Unauthorized))?;
        let native = current_catalog
            .current_definition(&observed.resource.execution.snapshot.definition.route)
            .ok_or(PermissionError::InvalidCatalog)?;
        let identity = current_catalog.application_identity();
        validation::use_renewal_quote_fresh_context(
            &observed.resource,
            context,
            identity,
            native,
            &mut account.budget,
        )?;
        if native != &observed.resource.execution.snapshot.definition
            || identity.application != context.actor.application
        {
            return Err(PermissionError::InvalidCatalog);
        }
        let fresh_context = InvocationContext {
            thread_id: observed.resource.thread.id.clone(),
            thread_generation: observed.resource.thread.generation.clone(),
            origin: observed
                .resource
                .thread
                .origin
                .as_ref()
                .ok_or(ApprovalError::Unauthorized)?
                .clone(),
            application_revision: identity.revision.clone(),
            execution_revision: native.revision.clone(),
            policy_revision: context.policy_revision.clone(),
        };
        let task = observed
            .resource
            .execution
            .snapshot
            .tasks
            .first()
            .ok_or(PermissionError::CorruptRecord)?;
        let source = PermissionUseRenewalSourceSelector {
            resource: &observed.resource,
            witness: loan.witness,
            producer: &producer,
            operation: &operation,
            context: &fresh_context,
            invocation: loan.invocation,
            current_catalog,
        };
        let capture = store.capture_permission_use_renewal_source(&source, account)?;
        if !Arc::ptr_eq(&capture.seal.origin, &operation) {
            return Err(PermissionError::CorruptRecord);
        }
        current_catalog.validate_use_renewal_claimed_budgeted(
            &fresh_context,
            native,
            task,
            loan.invocation,
            &mut account.budget,
        )?;
        let selector = PermissionUseRenewalResourceSelector {
            execution_id: &loan.resource.execution.snapshot.execution_id,
            producer: &producer,
            operation: &operation,
            phase: ResourcePhase::R4,
        };
        let final_resource = store.capture_permission_use_renewal_resource(&selector, account)?;
        validation::use_renewal_quote_resource_pair(
            &observed.resource,
            &final_resource.resource,
            &mut account.budget,
        )?;
        if final_resource.phase != ResourcePhase::R4
            || !Arc::ptr_eq(&producer, &final_resource.producer)
            || !Arc::ptr_eq(&operation, &final_resource.operation)
            || final_resource.resource != observed.resource
            || store.owner_generation()
                != final_resource.resource.execution.snapshot.owner_generation
        {
            return Err(PermissionError::CorruptRecord);
        }
        account
            .budget
            .runtime_reserve(PERMIT_BOUND * 8 + LEAF_FIXED * 8, 64)?;
        // Selector/source/claim loans end before the whole owned Claim moves.
        Ok(PermissionUseDispatchPermit {
            claim,
            catalog: current_catalog,
            selected_definition: native,
            store,
            authority,
            context: fresh_context,
            proof: capture.seal,
        })
    });
    let result = account.budget.runtime_finish(result);
    account.phase = if result.is_ok() {
        AccountPhase::Finished
    } else {
        AccountPhase::Refused
    };
    result
}
fn renewal_checked_iife<F, R>(value: F) -> R
where
    F: FnOnce() -> R,
{
    const {
        assert!(size_of::<F>() <= PERMIT_BOUND);
    }
    value()
}
fn renewal_checked_value<F>(value: F) -> F {
    const {
        assert!(size_of::<F>() <= 128);
    }
    value
}
const _: () = {
    assert!(size_of::<PermissionUseRenewalAccount>() <= ACCOUNT_BOUND);
    assert!(
        size_of::<Result<PermissionUseRenewalAccount, PermissionError>>() <= ACCOUNT_RESULT_BOUND
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRenewalAccount,
            >,
        >() <= ACCOUNT_RESULT_BOUND
    );
    assert!(size_of::<PermissionUseRenewalResourceSelector<'static>>() <= LOAN_BOUND);
    assert!(size_of::<PermissionUseRenewalResourceSession<'static, 'static>>() <= LOAN_BOUND);
    assert!(
        size_of::<Result<PermissionUseRenewalResourceSession<'static, 'static>, PermissionError>>()
            <= UNIT_RESULT_BOUND
    );
    assert!(size_of::<PermissionUseRenewalObservation>() <= OBSERVATION_BOUND);
    assert!(
        size_of::<Result<PermissionUseRenewalObservation, PermissionError>>() <= OBSERVATION_BOUND
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRenewalObservation,
            >,
        >() <= OBSERVATION_BOUND
    );
    assert!(size_of::<PermissionUseRenewalSourceSelector<'static>>() <= LOAN_BOUND);
    assert!(size_of::<PermissionUseRenewalSourceSession<'static, 'static>>() <= 512);
    assert!(
        size_of::<Result<PermissionUseRenewalSourceSession<'static, 'static>, PermissionError>>()
            <= 512
    );
    assert!(size_of::<PermissionUseRenewalSourceCapture>() <= UNIT_RESULT_BOUND);
    assert!(
        size_of::<Result<PermissionUseRenewalSourceCapture, PermissionError>>()
            <= UNIT_RESULT_BOUND
    );
    assert!(size_of::<PermissionUseRenewalProof>() <= UNIT_RESULT_BOUND);
    assert!(size_of::<InvocationContext>() <= 512);
    assert!(size_of::<Result<(), PermissionError>>() <= UNIT_RESULT_BOUND);
    assert!(size_of::<Arc<PermissionUseRenewalOrigin>>() <= 128);
    assert!(size_of::<PermissionUseRenewalOrigin>() <= 128);
    assert!(size_of::<Result<&PermissionUseRenewalObservation, &PermissionError>>() <= 128);
    assert!(size_of::<Result<&PermissionUseRenewalSourceCapture, &PermissionError>>() <= 128);
    assert!(size_of::<Result<&ExecutionId, PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRenewalResourceSession<'static, 'static>,
            >,
        >() <= 128
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRenewalSourceSession<'static, 'static>,
            >,
        >() <= 512
    );
};
// Sized and unsized Store/Authority references may have different layouts.
// This shared upper bound is checked on each actual API incarnation.
fn assert_permit<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>() {
    const {
        assert!(
            size_of::<(
                &S,
                &PermissionUseRenewalResourceSelector<'_>,
                &mut PermissionUseRenewalAccount
            )>() <= 128
        );
    }
    const {
        assert!(
            size_of::<(
                &S,
                &PermissionUseRenewalSourceSelector<'_>,
                &mut PermissionUseRenewalAccount
            )>() <= 128
        );
    }
    const {
        assert!(
            size_of::<(
                PermissionDispatchClaim<'_>,
                &S,
                &A,
                &PermissionCatalog,
                &ApprovalContext,
                &mut PermissionUseRenewalAccount
            )>() <= PERMIT_BOUND
        );
    }
    const {
        assert!(size_of::<PermissionUseDispatchPermit<'_, '_, '_, S, A>>() <= PERMIT_BOUND);
    }
    const {
        assert!(
            size_of::<Result<PermissionUseDispatchPermit<'_, '_, '_, S, A>, PermissionError>>()
                <= PERMIT_BOUND
        );
    }
    const {
        assert!(
            size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    PermissionUseDispatchPermit<'_, '_, '_, S, A>,
                >,
            >() <= PERMIT_BOUND
        );
    }
}

// Each new nominal control and callable argument incarnation has its own bound.
const _: () = {
    assert!(size_of::<AccountPhase>() <= 128);
    assert!(size_of::<ResourcePhase>() <= 128);
    assert!(size_of::<Option<ResourcePhase>>() <= 128);
    assert!(size_of::<PermissionUseRenewalSource>() <= 128);
    assert!(size_of::<Option<PermissionRuntimeUseSource>>() <= 128);
    assert!(size_of::<[bool; 12]>() <= 128);
    assert!(size_of::<&PermissionDispatchClaim<'static>>() <= LOAN_BOUND);
    assert!(size_of::<&PermissionUseRenewalAccount>() <= LOAN_BOUND);
    assert!(size_of::<&mut PermissionUseRenewalAccount>() <= LOAN_BOUND);
    assert!(
        size_of::<(
            &PermissionUseRenewalResourceSelector<'static>,
            &mut PermissionUseRenewalAccount
        )>() <= LOAN_BOUND
    );
    assert!(
        size_of::<(
            &PermissionUseRenewalSourceSelector<'static>,
            &mut PermissionUseRenewalAccount
        )>() <= LOAN_BOUND
    );
    assert!(size_of::<&mut PermissionUseRenewalResourceSession<'static, 'static>>() <= LOAN_BOUND);
    assert!(
        size_of::<(
            PermissionUseRenewalResourceSession<'static, 'static>,
            ApprovalResource
        )>() <= OBSERVATION_BOUND
    );
    assert!(size_of::<PermissionUseRenewalSourceSession<'static, 'static>>() <= 512);
};

// Tighter actual C2 first-refusal carriers use the earlier operation ticket.
const _: () = {
    assert!(size_of::<PermissionUseRenewalSourceSession<'static, 'static>>() <= 128);
    assert!(
        size_of::<Result<PermissionUseRenewalSourceSession<'static, 'static>, PermissionError>>()
            <= 128
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRenewalSourceSession<'static, 'static>,
            >,
        >() <= 128
    );
};

// Dedicated C2 owns only newly observed Dispatch header/typed journal state.
// Retained source operands stay borrowed under the original sealed Claim.
const SOURCE_STATE_FIXED: usize = 8 * 512 + 32 * 128;
// Each new C2 method's full Args/formals, nineteen Result/Try placements,
// checked callable/loan/continuation ends. Its const premises are independent.
pub(super) const SOURCE_CALL_FIXED: usize = 4 * 256 + 19 * (128 + 2 * 128) + 16 * 128;
struct RenewalSourceState {
    rows: [usize; 12],
    payloads: [usize; 12],
    stage: Option<(PermissionUseRenewalSource, Option<usize>)>,
    resource_matched: bool,
    payload_capture_attempted: bool,
    payload_captured: bool,
    dispatch_fields: Vec<Vec<u8>>,
    dispatch_sequence: i64,
    dispatch_bytes: usize,
    dispatch: Option<crate::JournalEntry>,
    creator_lengths: Option<(usize, usize)>,
}
impl RenewalSourceState {
    fn new() -> Self {
        Self {
            rows: [0; 12],
            payloads: [0; 12],
            stage: None,
            resource_matched: false,
            payload_capture_attempted: false,
            payload_captured: false,
            dispatch_fields: Vec::new(),
            dispatch_sequence: 0,
            dispatch_bytes: 0,
            dispatch: None,
            creator_lengths: None,
        }
    }
}
impl PermissionUseRenewalSource {
    fn index(self) -> usize {
        self as usize
    }
}
impl<'account, 'input> PermissionUseRenewalSourceSession<'account, 'input> {
    fn control(&mut self) -> Result<(), PermissionError> {
        self.account.budget.runtime_reserve(LEAF_FIXED, 8)?;
        self.account.budget.runtime_reserve(SOURCE_CALL_FIXED, 64)
    }
    pub fn before_code_only_scope(&mut self) -> Result<(), PermissionError> {
        self.control()?;
        // C2's Resource8 + twelve complete header ranges + fourteen payload
        // points are independent query incarnations. Six is the largest
        // reached header/terminal-row count (originalJ5 + sentinel).
        self.account.budget.runtime_reserve(CODE_ONLY_FIXED, 40)?;
        // Fresh owned Resource -> session match Args, moves, vacant cells and
        // retirement controls are distinct from the existing reader shim.
        self.account
            .budget
            .runtime_reserve(16 * OBSERVATION_BOUND, 64)?;
        self.account
            .budget
            .runtime_reserve(26 * (SDK_ACTION_FIXED + ADAPTER_ACTION_FIXED), 104)
    }
    pub fn current_execution_id(&mut self) -> Result<&'input ExecutionId, PermissionError> {
        self.control()?;
        Ok(&self.selector.resource.execution.snapshot.execution_id)
    }
    pub fn resource_read(&mut self) -> Result<PermissionResourceRead<'_>, PermissionError> {
        self.account.budget.use_renewal_combined_resource_read()
    }

    pub fn match_resource(&mut self, resource: ApprovalResource) -> Result<(), PermissionError> {
        self.control()?;
        validation::use_renewal_quote_resource_pair(
            self.selector.resource,
            &resource,
            &mut self.account.budget,
        )?;
        if resource != *self.selector.resource {
            return Err(PermissionError::CorruptRecord);
        }
        self.state.resource_matched = true;
        Ok(())
    }
    pub fn header_key(
        &mut self,
        source: PermissionUseRenewalSource,
    ) -> Result<&'input str, PermissionError> {
        self.control()?;
        let retained = self
            .selector
            .witness
            .renewal_source_loan(&mut self.account.budget)?;
        Ok(match source {
            PermissionUseRenewalSource::ThreadDecision => self.selector.resource.thread.id.as_str(),
            PermissionUseRenewalSource::GlobalUseIdentity => retained.use_record.use_id().as_str(),
            PermissionUseRenewalSource::OriginalAcceptance
            | PermissionUseRenewalSource::OriginalGrant
            | PermissionUseRenewalSource::OriginalLegacyAcceptance
            | PermissionUseRenewalSource::OriginalJournal
            | PermissionUseRenewalSource::Creator => {
                retained.creator.snapshot.execution_id.as_str()
            }
            _ => self
                .selector
                .resource
                .execution
                .snapshot
                .execution_id
                .as_str(),
        })
    }
    pub fn before_headers(
        &mut self,
        source: PermissionUseRenewalSource,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if !self.state.resource_matched
            || self.state.stage.is_some()
            || self.headers_ended[source.index()]
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.state.stage = Some((source, None));
        self.account.budget.runtime_reserve(10_240, 1)
    }
    pub fn before_row(&mut self) -> Result<(), PermissionError> {
        self.control()?;
        // Native row advance/cell adapters get a new independent current ticket.
        self.account
            .budget
            .runtime_reserve(SDK_ACTION_FIXED + ADAPTER_ACTION_FIXED, 4)
    }
    pub fn admit_header(
        &mut self,
        source: PermissionUseRenewalSource,
        cells: super::PermissionRuntimeUseHeaderCells<'_>,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if self.state.stage != Some((source, None)) {
            return Err(PermissionError::CorruptRecord);
        }
        let index = self.state.rows[source.index()];
        if cells.fields.len() > 8 {
            return Err(PermissionError::CorruptRecord);
        }
        // At most eight length callbacks and eight checked accumulator Try
        // incarnations; reserve independently before either traversal.
        self.account
            .budget
            .runtime_reserve(16 * SOURCE_CALL_FIXED, 16 * 64)?;
        if cells
            .fields
            .iter()
            .any(renewal_checked_value(|v: &&[u8]| v.len() > 256))
        {
            return Err(PermissionError::CorruptRecord);
        }
        let total = cells.fields.iter().try_fold(
            0usize,
            renewal_checked_value(|n: usize, v: &&[u8]| {
                n.checked_add(v.len()).ok_or(PermissionError::LimitExceeded)
            }),
        )?;
        self.account
            .budget
            .runtime_reserve(2 * total + 512, cells.fields.len())?;
        if self.expected_count(source) == 0 {
            // The adapter already checks full bounded native types. Validate
            // every reached identifier/blob/version/length before refusing.
            let identifiers = match source {
                PermissionUseRenewalSource::CurrentAcceptance => 3,
                PermissionUseRenewalSource::CurrentGrant => 4,
                _ => 2,
            };
            let expected_fields = match source {
                PermissionUseRenewalSource::CurrentAcceptance => 6,
                PermissionUseRenewalSource::CurrentGrant => 6,
                _ => 2,
            };
            if cells.fields.len() != expected_fields {
                return Err(PermissionError::CorruptRecord);
            }
            for field in &cells.fields[..identifiers] {
                // One native UTF-8 Try plus bounded per-byte character calls.
                self.account.budget.runtime_reserve(
                    SOURCE_CALL_FIXED + field.len() * (4 * 128),
                    64 + field.len(),
                )?;
                let text = std::str::from_utf8(field)
                    .map_err(renewal_checked_value(|_| PermissionError::CorruptRecord))?;
                if text.is_empty()
                    || text.len() > 256
                    || text.chars().any(renewal_checked_value(char::is_control))
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            let execution = match source {
                PermissionUseRenewalSource::OriginalLegacyAcceptance => {
                    let retained = self
                        .selector
                        .witness
                        .renewal_source_loan(&mut self.account.budget)?;
                    retained.creator.snapshot.execution_id.as_str()
                }
                _ => self
                    .selector
                    .resource
                    .execution
                    .snapshot
                    .execution_id
                    .as_str(),
            };
            if cells.fields.first().copied() != Some(execution.as_bytes()) {
                return Err(PermissionError::CorruptRecord);
            }
            if matches!(
                source,
                PermissionUseRenewalSource::CurrentAcceptance
                    | PermissionUseRenewalSource::CurrentGrant
            ) {
                if cells.record_bytes == 0 || cells.record_bytes > 65_536 {
                    return Err(PermissionError::CorruptRecord);
                }
                let generation = cells
                    .fields
                    .get(if source == PermissionUseRenewalSource::CurrentGrant {
                        4
                    } else {
                        3
                    })
                    .ok_or(PermissionError::CorruptRecord)?;
                self.account.budget.runtime_reserve(64 * 4 * 128, 64)?;
                if generation.len() != 64
                    || !generation.iter().all(renewal_checked_value(|b: &u8| {
                        b.is_ascii_digit() || (b'a'..=b'f').contains(b)
                    }))
                {
                    return Err(PermissionError::CorruptRecord);
                }
                let thread_index = if source == PermissionUseRenewalSource::CurrentGrant {
                    3
                } else {
                    2
                };
                if cells.fields[thread_index]
                    != self.selector.resource.thread.id.as_str().as_bytes()
                    || *generation != self.selector.resource.thread.generation.as_str().as_bytes()
                {
                    return Err(PermissionError::CorruptRecord);
                }
                let blob_start = if source == PermissionUseRenewalSource::CurrentGrant {
                    5
                } else {
                    4
                };
                for field in &cells.fields[blob_start..] {
                    if field.len() != 8 {
                        return Err(PermissionError::CorruptRecord);
                    }
                }
                self.account
                    .budget
                    .runtime_reserve(3 * 19 * 128 + 16 * 128, 64)?;
                if source == PermissionUseRenewalSource::CurrentAcceptance {
                    let previous = renewal_be_u64(cells.fields[4])?;
                    let committed = renewal_be_u64(cells.fields[5])?;
                    if previous.checked_add(1) != Some(committed) {
                        return Err(PermissionError::CorruptRecord);
                    }
                } else if renewal_be_u64(cells.fields[5])? == 0 {
                    return Err(PermissionError::CorruptRecord);
                }
                if source == PermissionUseRenewalSource::CurrentAcceptance && cells.scalar != 1
                    || source == PermissionUseRenewalSource::CurrentGrant
                        && !matches!(cells.scalar, 0 | 1)
                {
                    return Err(PermissionError::CorruptRecord);
                }
            }
            return Err(PermissionError::UnsupportedProfile);
        }
        if index >= self.expected_count(source) {
            return Err(PermissionError::CorruptRecord);
        }
        if source == PermissionUseRenewalSource::CurrentJournal && index == 3 {
            let snap = &self.selector.resource.execution.snapshot;
            let retained = self
                .selector
                .witness
                .renewal_source_loan(&mut self.account.budget)?;
            let (_, last, _, _) = retained.renewal_window(source, 2, &mut self.account.budget)?;
            if cells.fields.len() != 7
                || cells.scalar <= last
                || cells.fields[0] != b"commit"
                || cells.fields[1] != self.selector.resource.thread.id.as_str().as_bytes()
                || cells.fields[2] != snap.execution_id.as_str().as_bytes()
                || cells.fields[3] != snap.revision.to_be_bytes()
                || cells.fields[4] != snap.owner_generation.to_be_bytes()
                || cells.fields[5] != validation::ready_use_status(snap.status).as_bytes()
                || cells.fields[6] != snap.updated_at_ms.to_be_bytes()
                || cells.record_bytes == 0
                || cells.record_bytes > 65_536
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.account.budget.runtime_reserve(
                13 * total + 256 * cells.fields.len() + 512,
                2 * cells.fields.len() + 2,
            )?;
            // The outer allocator and each inner allocator have distinct native,
            // mapped, Try, Args and closure tickets, before their first call.
            self.account.budget.runtime_reserve(SOURCE_CALL_FIXED, 64)?;
            let mut fields = Vec::new();
            fields
                .try_reserve_exact(cells.fields.len())
                .map_err(renewal_checked_value(|_| PermissionError::LimitExceeded))?;
            for field in cells.fields {
                self.account.budget.runtime_reserve(SOURCE_CALL_FIXED, 64)?;
                let mut value = Vec::new();
                value
                    .try_reserve_exact(field.len())
                    .map_err(renewal_checked_value(|_| PermissionError::LimitExceeded))?;
                value.extend_from_slice(field);
                fields.push(value);
            }
            self.state.dispatch_fields = fields;
            self.state.dispatch_sequence = cells.scalar;
            self.state.dispatch_bytes = cells.record_bytes;
        } else {
            let retained = self
                .selector
                .witness
                .renewal_source_loan(&mut self.account.budget)?;
            let (fields, scalar, bytes, _) =
                retained.renewal_window(source, index, &mut self.account.budget)?;
            self.account.budget.runtime_reserve(8 * 4 * 128, 8)?;
            if fields.len() != cells.fields.len()
                || scalar != cells.scalar
                || bytes != cells.record_bytes
                || fields.iter().zip(cells.fields).any(renewal_checked_value(
                    |(a, b): (&Vec<u8>, &&[u8])| a.as_slice() != *b,
                ))
            {
                return Err(PermissionError::CorruptRecord);
            }
        }
        self.state.rows[source.index()] += 1;
        Ok(())
    }
    pub fn end_headers(
        &mut self,
        source: PermissionUseRenewalSource,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if self.state.stage != Some((source, None))
            || self.state.rows[source.index()] != self.expected_count(source)
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.headers_ended[source.index()] = true;
        self.state.stage = None;
        Ok(())
    }
    pub fn admit_creator_lengths(
        &mut self,
        thread: usize,
        snapshot: usize,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if self.state.stage != Some((PermissionUseRenewalSource::Creator, None))
            || self.state.creator_lengths.is_some()
        {
            return Err(PermissionError::CorruptRecord);
        }
        let retained = self
            .selector
            .witness
            .renewal_source_loan(&mut self.account.budget)?;
        let (fields, _, bytes, _) = retained.renewal_window(
            PermissionUseRenewalSource::Creator,
            0,
            &mut self.account.budget,
        )?;
        if thread > 256
            || snapshot > 65_536
            || fields.first().map(Vec::len) != Some(thread)
            || bytes != snapshot
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.account
            .budget
            .runtime_reserve(thread + snapshot + 512, 8)?;
        self.state.creator_lengths = Some((thread, snapshot));
        self.state.rows[PermissionUseRenewalSource::Creator.index()] = 1;
        Ok(())
    }
    pub fn creator_lengths(&mut self) -> Result<(usize, usize), PermissionError> {
        self.control()?;
        self.state
            .creator_lengths
            .ok_or(PermissionError::CorruptRecord)
    }
    pub fn before_payload(
        &mut self,
        source: PermissionUseRenewalSource,
        index: usize,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if !self
            .headers_ended
            .iter()
            .all(renewal_checked_value(|v: &bool| *v))
            || self.state.stage.is_some()
            || index >= self.expected_count(source)
            || self.state.payloads[source.index()] != index
            || self.state.payload_capture_attempted
            || self.state.payload_captured
            || source == PermissionUseRenewalSource::GlobalUseIdentity
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.state.stage = Some((source, Some(index)));
        self.account.budget.runtime_reserve(10_240, 1)
    }
    pub fn payload_key(
        &mut self,
        source: PermissionUseRenewalSource,
        index: usize,
    ) -> Result<(&'input str, &'input str, i64, u64, usize), PermissionError> {
        self.control()?;
        let retained = self
            .selector
            .witness
            .renewal_source_loan(&mut self.account.budget)?;
        let current = self
            .selector
            .resource
            .execution
            .snapshot
            .execution_id
            .as_str();
        if source == PermissionUseRenewalSource::CurrentJournal && index == 3 {
            return Ok((
                current,
                "",
                self.state.dispatch_sequence,
                self.selector.resource.execution.snapshot.revision,
                self.state.dispatch_bytes,
            ));
        }
        let (fields, scalar, bytes, _) =
            retained.renewal_window(source, index, &mut self.account.budget)?;
        // Each reached native conversion owns a separate nominal/Try ticket.
        // Dispatch index3 returned above has no conversions.
        let native_conversions = match source {
            PermissionUseRenewalSource::CurrentUse => 2,
            PermissionUseRenewalSource::ThreadDecision
            | PermissionUseRenewalSource::OriginalAcceptance
            | PermissionUseRenewalSource::OriginalGrant => 1,
            PermissionUseRenewalSource::OriginalJournal
            | PermissionUseRenewalSource::CurrentJournal => 2,
            _ => return Err(PermissionError::CorruptRecord),
        };
        self.account.budget.runtime_reserve(
            native_conversions * SOURCE_CALL_FIXED,
            native_conversions * 64,
        )?;
        let text = renewal_checked_value(|v: &'input [u8]| {
            std::str::from_utf8(v)
                .map_err(renewal_checked_value(|_| PermissionError::CorruptRecord))
        });
        Ok(match source {
            PermissionUseRenewalSource::CurrentUse => {
                (text(&fields[1])?, text(&fields[2])?, 0, 0, bytes)
            }
            PermissionUseRenewalSource::ThreadDecision => (text(&fields[0])?, "", 0, 0, bytes),
            PermissionUseRenewalSource::OriginalAcceptance => (
                retained.creator.snapshot.execution_id.as_str(),
                text(&fields[1])?,
                0,
                0,
                bytes,
            ),
            PermissionUseRenewalSource::OriginalGrant => (
                retained.creator.snapshot.execution_id.as_str(),
                text(&fields[1])?,
                0,
                0,
                bytes,
            ),
            PermissionUseRenewalSource::OriginalJournal
            | PermissionUseRenewalSource::CurrentJournal => {
                let rev = u64::from_be_bytes(
                    fields[3]
                        .as_slice()
                        .try_into()
                        .map_err(renewal_checked_value(|_| PermissionError::CorruptRecord))?,
                );
                (text(&fields[2])?, "", scalar, rev, bytes)
            }
            _ => return Err(PermissionError::CorruptRecord),
        })
    }
    pub fn capture_payload(
        &mut self,
        source: PermissionUseRenewalSource,
        index: usize,
        raw: &[u8],
    ) -> Result<(), PermissionError> {
        self.control()?;
        if self.state.stage != Some((source, Some(index)))
            || self.state.payload_capture_attempted
            || self.state.payload_captured
            || !matches!(
                source,
                PermissionUseRenewalSource::CurrentUse
                    | PermissionUseRenewalSource::ThreadDecision
                    | PermissionUseRenewalSource::OriginalAcceptance
                    | PermissionUseRenewalSource::OriginalGrant
                    | PermissionUseRenewalSource::OriginalJournal
                    | PermissionUseRenewalSource::CurrentJournal
            )
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.state.payload_capture_attempted = true;
        if raw.len() > 65_536 {
            return Err(PermissionError::LimitExceeded);
        }
        self.account
            .budget
            .runtime_reserve(2 * raw.len() + 256, 2)?;
        if source == PermissionUseRenewalSource::CurrentJournal && index == 3 {
            if raw.len() != self.state.dispatch_bytes {
                return Err(PermissionError::CorruptRecord);
            }
            // The retained library envelope is independently applied to this
            // new Journal decoder incarnation. Its new accepted/partial typed
            // owner endpoints have an additional explicit prepaid bound.
            self.account.budget.runtime_reserve(40_960, 8)?;
            self.account
                .budget
                .runtime_reserve(128 * 11 + 6 * 1024 + 7 * 160 + 128 * 11, 22)?;
            if self.selector.witness.input_profile()
                == super::runtime_mutation::PermissionRuntimeInputProfile::NormalNewUse
            {
                // Pay actual enlarged journal owner endpoints before decoding.
                self.account.budget.runtime_reserve(7 * raw.len(), 11)?;
            }
            let (journal, _) = validation::runtime_compat::capture_journal_with_profile(
                raw,
                self.selector.witness.input_profile(),
                &mut self.account.budget,
            )?;
            let s = &self.selector.resource.execution.snapshot;
            if journal.execution_id != s.execution_id
                || journal.revision != s.revision
                || journal.owner_generation != s.owner_generation
                || journal.status != s.status
                || journal.updated_at_ms != s.updated_at_ms
            {
                return Err(PermissionError::CorruptRecord);
            }
            self.state.dispatch = Some(journal);
        } else {
            let retained = self
                .selector
                .witness
                .renewal_source_loan(&mut self.account.budget)?;
            let (_, _, _, old) =
                retained.renewal_window(source, index, &mut self.account.budget)?;
            self.account
                .budget
                .runtime_reserve(raw.len() + old.len(), 2)?;
            if raw != old {
                return Err(PermissionError::CorruptRecord);
            }
        }
        self.state.payload_captured = true;
        Ok(())
    }
    pub fn capture_creator_raw(
        &mut self,
        thread: &[u8],
        revision: &[u8],
        owner: &[u8],
        snapshot: &[u8],
    ) -> Result<(), PermissionError> {
        self.control()?;
        if self.state.stage != Some((PermissionUseRenewalSource::Creator, Some(0)))
            || self.state.payload_capture_attempted
            || self.state.payload_captured
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.state.payload_capture_attempted = true;
        let retained = self
            .selector
            .witness
            .renewal_source_loan(&mut self.account.budget)?;
        let (fields, _, bytes, old) = retained.renewal_window(
            PermissionUseRenewalSource::Creator,
            0,
            &mut self.account.budget,
        )?;
        if thread.len() > 256 || revision.len() != 8 || owner.len() != 8 || snapshot.len() > 65_536
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.account.budget.runtime_reserve(
            2 * (thread.len() + revision.len() + owner.len() + snapshot.len()) + 512,
            4,
        )?;
        if fields.len() != 3
            || fields[0] != thread
            || fields[1] != revision
            || fields[2] != owner
            || bytes != snapshot.len()
            || old != snapshot
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.state.payload_captured = true;
        Ok(())
    }
    pub fn end_payload(
        &mut self,
        source: PermissionUseRenewalSource,
        index: usize,
    ) -> Result<(), PermissionError> {
        self.control()?;
        if self.state.stage != Some((source, Some(index)))
            || !self.state.payload_capture_attempted
            || !self.state.payload_captured
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.state.payload_capture_attempted = false;
        self.state.payload_captured = false;
        self.state.payloads[source.index()] += 1;
        self.state.stage = None;
        if self.state.payloads[source.index()] == self.expected_count(source) {
            self.payloads_ended[source.index()] = true;
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<PermissionUseRenewalSourceCapture, PermissionError> {
        self.control()?;
        for source in [
            PermissionUseRenewalSource::CurrentAcceptance,
            PermissionUseRenewalSource::CurrentLegacyAcceptance,
            PermissionUseRenewalSource::CurrentGrant,
            PermissionUseRenewalSource::OriginalLegacyAcceptance,
            PermissionUseRenewalSource::GlobalUseIdentity,
        ] {
            self.payloads_ended[source.index()] = self.headers_ended[source.index()];
        }
        if !self
            .headers_ended
            .iter()
            .all(renewal_checked_value(|v: &bool| *v))
            || !self
                .payloads_ended
                .iter()
                .all(renewal_checked_value(|v: &bool| *v))
            || !self.state.resource_matched
            || self.state.dispatch.is_none()
            || self.state.stage.is_some()
            || self.state.payload_capture_attempted
            || self.state.payload_captured
        {
            return Err(PermissionError::CorruptRecord);
        }
        let snapshot = &self.selector.resource.execution.snapshot;
        let task = snapshot
            .tasks
            .first()
            .ok_or(PermissionError::CorruptRecord)?;
        let registration = self.selector.current_catalog.ready_registration(
            &snapshot.definition,
            &task.spec.effect_id,
            &mut self.account.budget,
        )?;
        if !self.selector.witness.validate_source(
            self.selector.resource,
            self.selector.context,
            registration,
            self.selector.current_catalog.application_identity(),
            &mut self.account.budget,
        )? {
            return Err(PermissionError::UnsupportedProfile);
        }
        validate_claimed_projection(
            self.selector.witness,
            self.selector.resource,
            self.selector.invocation,
            &mut self.account.budget,
        )?;
        Ok(PermissionUseRenewalSourceCapture {
            seal: PermissionUseRenewalProof {
                origin: Arc::clone(self.selector.operation),
            },
        })
    }
}
const _: () = {
    assert!(size_of::<RenewalSourceState>() <= 512);
    assert!(size_of::<Box<RenewalSourceState>>() <= 128);
    assert!(size_of::<Result<(&str, &str, i64, u64, usize), PermissionError>>() <= 128);
    assert!(size_of::<Option<crate::JournalEntry>>() <= 256);
};
fn validate_claimed_projection(
    witness: &PermissionRuntimeUseWitness,
    current: &ApprovalResource,
    invocation: &crate::EffectInvocation,
    budget: &mut PermissionEvidenceBudget,
) -> Result<(), PermissionError> {
    use b4_contract::{ExecutionStatus as E, TaskStatus as T};
    budget.runtime_reserve(LEAF_FIXED, 8)?;
    budget.runtime_reserve(SOURCE_CALL_FIXED, 64)?;
    let retained = witness.renewal_source_loan(budget)?;
    let u = retained.use_record;
    let d = retained.decision;
    let before = u.before_snapshot();
    let now = &current.execution.snapshot;
    validation::use_renewal_quote_claimed_projection(before, now, budget)?;
    if before.tasks.len() != 1
        || now.tasks.len() != 1
        || retained.current_journal_prefix.len() != 3
        || before.definition.batches.len() != 1
        || before.definition.batches[0].len() != 1
    {
        return Err(PermissionError::UnsupportedProfile);
    }
    let a = &before.tasks[0];
    let b = &now.tasks[0];
    if before.revision != 1
        || u.committed_revision() != 2
        || now.revision != 3
        || before.status != E::Running
        || now.status != before.status
        || before.attempts_used != 0
        || !before.attempt_history.is_empty()
        || now.attempts_used != 1
        || (now.attempt_history.len() != 1
            || now.attempt_history.first() != Some(&invocation.attempt_id))
        || now.schema_version != before.schema_version
        || now.execution_id != before.execution_id
        || now.turn_id != before.turn_id
        || now.owner_generation != before.owner_generation
        || now.definition != before.definition
        || now.batch_index != before.batch_index
        || now.values != before.values
        || now.turn_history != before.turn_history
        || now.output != before.output
        || now.failure != before.failure
        || now.updated_at_ms < u.facts().now_ms
        || before.output.is_some()
        || before.failure.is_some()
        || a.spec != b.spec
        || a.input != b.input
        || a.status != T::Ready
        || a.approved
        || a.interrupt.is_some()
        || a.attempt_id.is_some()
        || a.result.is_some()
        || a.failure.is_some()
        || b.status != T::InFlight
        || !b.approved
        || b.interrupt.as_ref() != Some(u.marker())
        || b.attempt_id.as_ref() != Some(&invocation.attempt_id)
        || b.result != a.result
        || b.failure != a.failure
        || !b.spec.requires_approval
        || invocation.execution_id != now.execution_id
        || invocation.turn_id != now.turn_id
        || invocation.owner_generation != now.owner_generation
        || invocation.task_id != b.spec.id
        || invocation.effect_id != b.spec.effect_id
        || invocation.input != b.input
        || u.record_version() != 1
        || u.permission_version() != d.record_version()
        || u.permission_id() != d.permission_id()
        || u.thread_id() != &current.thread.id
        || u.thread_generation() != &current.thread.generation
        || u.execution_id() != &now.execution_id
        || u.task_id() != &b.spec.id
        || u.application() != d.scope().application()
        || u.tool_id() != d.scope().tool_id()
        || u.tool_revision() != d.scope().tool_revision()
        || u.policy_revision() != d.scope().policy_revision()
        || u.constraint_revision() != d.scope().constraint_revision()
    {
        return Err(PermissionError::CorruptRecord);
    }
    budget.runtime_reserve(3 * 16 * 128, 48)?;
    let statuses = [E::Accepted, E::Running, E::Running];
    let mut last = 0;
    for (i, j) in retained.current_journal_prefix.iter().enumerate() {
        if j.execution_id != now.execution_id
            || j.revision != i as u64
            || j.owner_generation != now.owner_generation
            || j.status != statuses[i]
            || j.updated_at_ms < last
        {
            return Err(PermissionError::CorruptRecord);
        }
        last = j.updated_at_ms;
    }
    if retained.current_journal_prefix[1].updated_at_ms != before.updated_at_ms
        || retained.current_journal_prefix[2].updated_at_ms != u.facts().now_ms
    {
        return Err(PermissionError::CorruptRecord);
    }
    validation::use_renewal_verify_original_grant(
        retained.grant,
        retained.acceptance.facts(),
        budget,
    )
}

// New actual C2 and projection interfaces; every full Args/Result/Flow is
// bounded independently, including the owned Resource consumption boundary.
const _: () = {
    assert!(size_of::<Option<(PermissionUseRenewalSource, Option<usize>)>>() <= 128);
    assert!(size_of::<[usize; 12]>() <= 128);
    assert!(size_of::<Vec<Vec<u8>>>() <= 128);
    assert!(size_of::<Option<(usize, usize)>>() <= 128);
    assert!(size_of::<Result<(&str, &str, i64, u64, usize), PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (&str, &str, i64, u64, usize),
            >,
        >() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            PermissionUseRenewalSource,
            super::PermissionRuntimeUseHeaderCells<'static>
        )>() <= 256
    );
    assert!(
        size_of::<(
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            PermissionUseRenewalSource,
            usize,
            &[u8]
        )>() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            &[u8],
            &[u8],
            &[u8],
            &[u8]
        )>() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            ApprovalResource
        )>() <= 2048
    );
    assert!(
        size_of::<(
            &PermissionRuntimeUseWitness,
            &ApprovalResource,
            &crate::EffectInvocation,
            &mut PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(size_of::<Result<crate::JournalEntry, PermissionError>>() <= 256);
    assert!(
        size_of::<Result<(crate::JournalEntry, validation::CanonicalMeasure), PermissionError>>()
            <= 256
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (crate::JournalEntry, validation::CanonicalMeasure),
            >,
        >() <= 256
    );
};

fn renewal_be_u64(value: &[u8]) -> Result<u64, PermissionError> {
    Ok(u64::from_be_bytes(value.try_into().map_err(
        renewal_checked_value(|_: std::array::TryFromSliceError| PermissionError::CorruptRecord),
    )?))
}
const _: () = {
    assert!(size_of::<std::collections::TryReserveError>() <= 128);
    assert!(size_of::<Result<(), std::collections::TryReserveError>>() <= 128);
    assert!(size_of::<Result<&str, std::str::Utf8Error>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, std::collections::TryReserveError>,
                (),
            >,
        >() <= 128
    );
    assert!(size_of::<(&mut Vec<u8>, usize)>() <= 128);
    assert!(size_of::<(&mut Vec<Vec<u8>>, usize)>() <= 128);
    assert!(size_of::<(&[u8],)>() <= 128);
    assert!(size_of::<Result<[u8; 8], std::array::TryFromSliceError>>() <= 128);
    assert!(size_of::<Result<u64, PermissionError>>() <= 128);
    assert!(
        size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, u64>>()
            <= 128
    );
    assert!(size_of::<Option<u64>>() <= 128);
};

// Actual renewal getter/conversion/receipt aggregates. Upper envelopes apply
// separately to each whole incarnation; they are not exact layout assertions.
const _: () = {
    assert!(size_of::<Result<&str, PermissionError>>() <= 128);
    assert!(
        size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &str>>()
            <= 128
    );
    assert!(size_of::<Result<&ExecutionId, PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, &ExecutionId>,
        >() <= 128
    );
    assert!(size_of::<Result<(usize, usize), PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                (usize, usize),
            >,
        >() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            PermissionUseRenewalSource,
            usize
        )>() <= 128
    );
    assert!(
        size_of::<(
            &mut PermissionUseRenewalSourceSession<'static, 'static>,
            usize,
            usize
        )>() <= 128
    );
    assert!(size_of::<std::str::Utf8Error>() <= 128);
    assert!(size_of::<std::array::TryFromSliceError>() <= 128);
    assert!(size_of::<Result<[u8; 8], PermissionError>>() <= 128);
    assert!(
        size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, [u8; 8]>>(
        ) <= 128
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, std::array::TryFromSliceError>,
                [u8; 8],
            >,
        >() <= 128
    );
    assert!(
        size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, std::str::Utf8Error>, &str>>(
        ) <= 128
    );
    assert!(size_of::<Result<PermissionUseRenewalSourceCapture, PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseRenewalSourceCapture,
            >,
        >() <= 128
    );
    assert!(size_of::<[PermissionUseRenewalSource; 11]>() <= 128);
    assert!(size_of::<[PermissionUseRenewalSource; 4]>() <= 128);
    assert!(size_of::<[PermissionUseRenewalSource; 5]>() <= 128);
    assert!(size_of::<[(PermissionUseRenewalSource, usize); 2]>() <= 128);
    assert!(size_of::<[b4_contract::ExecutionStatus; 3]>() <= 128);
    assert!(size_of::<[u8; 8]>() <= 128);
    assert!(size_of::<[Vec<u8>; 7]>() <= 256);
    assert!(size_of::<(Vec<Vec<u8>>, [Vec<u8>; 7])>() <= 256);
    assert!(size_of::<[Vec<u8>; 4]>() <= 128);
    assert!(size_of::<(Vec<Vec<u8>>, [Vec<u8>; 4])>() <= 128);
};

// Callback-only short loan of a genuinely sealed renewed Permit. The proof and
// all producer/current/operation anchors remain owned by the caller's Permit.
pub(super) struct PermissionCallbackPermitLoan<'permit> {
    pub(super) application: &'permit crate::NativeApplication,
    pub(super) definition: &'permit Definition,
    pub(super) invocation: &'permit crate::EffectInvocation,
    pub(super) context: &'permit InvocationContext,
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    PermissionUseDispatchPermit<'_, '_, '_, S, A>
{
    pub(super) fn callback_use_loan<'permit>(
        &'permit self,
        budget: &mut super::runtime_use_callback::CallbackBudget,
    ) -> Result<PermissionCallbackPermitLoan<'permit>, PermissionError> {
        const {
            assert!(size_of::<(&Self, &mut super::runtime_use_callback::CallbackBudget)>() <= 128);
        }
        budget.purchase(
            super::runtime_use_callback::CALLBACK_LOAN_FIXED,
            super::runtime_use_callback::CALLBACK_LOAN_NODES,
        )?;
        let claim = self.claim.callback_use_loan(budget)?;
        // The loan observes the genuine Claim origin; no operation-origin
        // substitution or second resource/policy fence is introduced here.
        let _origin = claim.origin;
        let application = self.catalog.callback_application(budget)?;
        Ok(PermissionCallbackPermitLoan {
            application,
            definition: self.selected_definition,
            invocation: claim.invocation,
            context: &self.context,
        })
    }
}
const _: () = {
    assert!(size_of::<PermissionCallbackPermitLoan<'static>>() <= 128);
    assert!(size_of::<Result<PermissionCallbackPermitLoan<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionCallbackPermitLoan<'static>,
            >,
        >() <= 128
    );
};

// Outcome loan keeps S outside the concrete flat request.
pub(super) struct PermissionOutcomePermitLoan<'permit> {
    pub(super) claim: super::runtime_mutation::PermissionOutcomeClaimLoan<'permit>,
    pub(super) context: &'permit crate::InvocationContext,
}
impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    PermissionUseDispatchPermit<'_, '_, '_, S, A>
{
    pub(super) fn outcome_use_loan<'permit>(
        &'permit self,
        account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<(&'permit S, PermissionOutcomePermitLoan<'permit>), PermissionError> {
        const {
            assert!(size_of::<(&Self, &mut super::PermissionUseOutcomeAccount)>() <= 128);
            assert!(size_of::<(&S, PermissionOutcomePermitLoan<'_>)>() <= 128);
            assert!(
                size_of::<Result<(&S, PermissionOutcomePermitLoan<'_>), PermissionError>>() <= 128
            );
            assert!(
                size_of::<
                    std::ops::ControlFlow<
                        Result<std::convert::Infallible, PermissionError>,
                        (&S, PermissionOutcomePermitLoan<'_>),
                    >,
                >() <= 128
            );
        }
        account.purchase(
            super::runtime_use_outcome::LOAN_BYTES,
            super::runtime_use_outcome::LOAN_NODES,
        )?;
        let claim = self.claim.outcome_use_loan(account)?;
        Ok((
            self.store,
            PermissionOutcomePermitLoan {
                claim,
                context: &self.context,
            },
        ))
    }
}
const _: () = {
    assert!(size_of::<PermissionOutcomePermitLoan<'_>>() <= 128);
    assert!(
        size_of::<(
            super::runtime_mutation::PermissionOutcomeClaimLoan<'_>,
            &crate::InvocationContext
        )>() <= 128
    );
};

impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    PermissionUseDispatchPermit<'_, '_, '_, S, A>
{
    pub(super) fn withheld_use_loan<'a>(
        &'a self,
        budget: &mut PermissionEvidenceBudget,
    ) -> Result<
        (
            &'a S,
            super::runtime_mutation::PermissionUseClaimLoan<'a>,
            &'a InvocationContext,
        ),
        PermissionError,
    > {
        budget.runtime_reserve(512, 8)?;
        Ok((
            self.store,
            self.claim.withheld_use_loan(budget)?,
            &self.context,
        ))
    }
}
