//! Receipt-owned outcome settlement with a bounded functional account.
use super::runtime_mutation::{PermissionOutcomeClaimLoan, PermissionRuntimeOrigin};
use super::runtime_use::PermissionRuntimeUseWitness;
use super::{
    validation, PermissionError, PermissionEvidenceBudget, PermissionEvidenceUsage,
    PermissionStore, PermissionUseCallbackReceipt, PermissionUseCallbackStatus,
};
use crate::approval::{ApprovalAuthority, ApprovalResource};
use crate::{EffectInvocation, EffectOutcome, InvocationContext};
use b4_contract::{CommitPlan, TransitionFacts};
use std::{mem::size_of, sync::Arc};

pub(super) const LOAN_BYTES: usize = 131072;
pub(super) const LOAN_NODES: usize = 1024;
const ENTRY_BYTES: usize = 131_072;
const ENTRY_NODES: usize = 2048;
const PORT_BYTES: usize = 196608;
const PORT_NODES: usize = 2048;

// Allocation identity is a local protocol brand, never storage authority.
pub(super) struct OutcomeOrigin;
/// Full request-bound result, constructible only through the typed writer.
/// Contains no Store, application, catalog or authority owner.
pub struct PermissionUseOutcomeCommit {
    pub(super) plan: CommitPlan,
    pub(super) resource: ApprovalResource,
    pub(super) producer: Arc<PermissionRuntimeOrigin>,
    pub(super) origin: Arc<OutcomeOrigin>,
    pub(super) attempt: u64,
    pub(super) now_ms: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Fresh,
    Entered,
    Finished,
}
/// Private cumulative account. There is no public factory, reserve or reset.
pub struct PermissionUseOutcomeAccount {
    evidence: PermissionEvidenceBudget,
    origin: Option<Arc<OutcomeOrigin>>,
    attempt: u64,
    phase: Phase,
    // Fresh Task4 append-only account ledger; producer stays bound across retries.
    producer: Option<Arc<PermissionRuntimeOrigin>>,
    source_attempt: u64,
    writer_attempt: u64,
}
impl PermissionUseOutcomeAccount {
    pub(super) fn fresh() -> Self {
        Self {
            evidence: PermissionEvidenceBudget::fresh_use_outcome_functional(),
            origin: None,
            attempt: 0,
            phase: Phase::Fresh,
            producer: None,
            source_attempt: 0,
            writer_attempt: 0,
        }
    }
    fn enter(&mut self, origin: &Arc<OutcomeOrigin>, attempt: u64) -> Result<(), PermissionError> {
        if self.phase == Phase::Entered
            || attempt == 0
            || attempt <= self.attempt
            || self.source_attempt > self.attempt
            || self.writer_attempt > self.attempt
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.attempt = attempt;
        self.phase = Phase::Entered;
        self.purchase(ENTRY_BYTES, ENTRY_NODES)?;
        // Dedicated paid header/clone/comparison ticket, before origin admission.
        self.purchase(LOAN_BYTES, LOAN_NODES)?;
        match &self.origin {
            Some(bound) if !Arc::ptr_eq(bound, origin) => Err(PermissionError::UnsupportedProfile),
            Some(_) => Ok(()),
            None => {
                self.origin = Some(Arc::clone(origin));
                Ok(())
            }
        }
    }
    pub(super) fn purchase(&mut self, bytes: usize, nodes: usize) -> Result<(), PermissionError> {
        if self.phase != Phase::Entered {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.evidence.outcome_scaffold_reserve(bytes, nodes)
    }
}

/// Flat sealed request; only a genuine receipt may produce this borrow.
pub struct PermissionUseOutcomeRequest<'input> {
    _resource: &'input ApprovalResource,
    _invocation: &'input EffectInvocation,
    _witness: &'input PermissionRuntimeUseWitness,
    _producer: &'input Arc<PermissionRuntimeOrigin>,
    _context: &'input InvocationContext,
    _actual: &'input EffectOutcome,
    controlled: bool,
    current: Option<&'input ApprovalResource>,
    _facts: &'input TransitionFacts,
    _origin: &'input Arc<OutcomeOrigin>,
    _attempt: u64,
}
impl<'input> PermissionUseOutcomeRequest<'input> {
    pub(super) fn from_receipt_loan(
        claim: PermissionOutcomeClaimLoan<'input>,
        context: &'input InvocationContext,
        actual: &'input EffectOutcome,
        controlled: bool,
        facts: &'input TransitionFacts,
        origin: &'input Arc<OutcomeOrigin>,
        attempt: u64,
    ) -> Self {
        Self {
            _resource: claim.resource,
            _invocation: claim.invocation,
            _witness: claim.witness,
            _producer: claim.origin,
            _context: context,
            _actual: actual,
            controlled,
            current: None,
            _facts: facts,
            _origin: origin,
            _attempt: attempt,
        }
    }
}

/// Keeps the entire callback receipt and every actual anchor through all errors.
pub struct PermissionUseOutcomeWork<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    receipt: PermissionUseCallbackReceipt<'producer, 'current, 'op, S, A>,
    account: PermissionUseOutcomeAccount,
    origin: Arc<OutcomeOrigin>,
    attempt: u64,
    completed: bool,
}
impl<'producer, 'current, 'op, S, A> PermissionUseOutcomeWork<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    pub(super) fn from_receipt(
        receipt: PermissionUseCallbackReceipt<'producer, 'current, 'op, S, A>,
    ) -> Self {
        assert_carriers::<S, A>();
        // Fresh account exists before the separately exterior-funded Arc allocation.
        let account = PermissionUseOutcomeAccount::fresh();
        Self {
            receipt,
            account,
            origin: Arc::new(OutcomeOrigin),
            attempt: 0,
            completed: false,
        }
    }
    /// Facts use the same trusted host boundary as Runtime::complete_outcome.
    /// Explicit retry spends this same account and never invokes the callback.
    pub fn commit(
        &mut self,
        facts: &TransitionFacts,
    ) -> Result<PermissionUseOutcomeCommit, PermissionError> {
        self.commit_impl(facts, None)
    }
    pub(super) fn commit_driver_candidate(
        &mut self,
        facts: &TransitionFacts,
        id: &b4_contract::ExecutionId,
        fence: crate::ExecutionFence,
        plan: &CommitPlan,
        account: &mut super::driver_adapter_account::DriverAdapterAccount,
    ) -> Result<PermissionUseOutcomeCommit, PermissionError> {
        self.commit_impl(
            facts,
            Some(CandidateLoan {
                id,
                fence,
                plan,
                account,
            }),
        )
    }
    fn commit_impl(
        &mut self,
        facts: &TransitionFacts,
        candidate: Option<CandidateLoan<'_>>,
    ) -> Result<PermissionUseOutcomeCommit, PermissionError> {
        assert_carriers::<S, A>();
        if self.completed {
            return Err(PermissionError::UnsupportedProfile);
        }
        let next = self
            .attempt
            .checked_add(1)
            .ok_or(PermissionError::LimitExceeded)?;
        if next == 0 {
            return Err(PermissionError::LimitExceeded);
        }
        self.attempt = next;
        if let Err(error) = self.account.enter(&self.origin, next) {
            if self.account.phase == Phase::Entered {
                self.account.phase = Phase::Finished;
            }
            return Err(error);
        }
        let result = self.commit_entered(facts, next, candidate);
        self.account.phase = Phase::Finished;
        result
    }
    fn commit_entered(
        &mut self,
        facts: &TransitionFacts,
        attempt: u64,
        candidate: Option<CandidateLoan<'_>>,
    ) -> Result<PermissionUseOutcomeCommit, PermissionError> {
        self.account.purchase(PORT_BYTES, PORT_NODES)?;
        let (store, mut request) =
            self.receipt
                .outcome_request(facts, &self.origin, attempt, &mut self.account)?;
        let current = if request.controlled {
            Some(store.capture_permission_use_outcome_current(&request, &mut self.account)?)
        } else {
            None
        };
        request.current = current.as_ref();
        // All foreseeable fallible validation and comparison debits precede the
        // backend's durable write. After acknowledgement only exact comparisons remain.
        let selector =
            super::runtime_use_outcome_source::PermissionUseOutcomeSourceSelector::from_request(
                &request,
            );
        let expected =
            super::runtime_use_outcome_source::reconstruct_plan(&selector, &mut self.account)?;
        validation::use_outcome_quote_plan_pair(
            &expected,
            &expected,
            self.account.source_budget()?,
        )?;
        validation::use_outcome_quote_resource_pair(
            request._resource,
            request._resource,
            self.account.source_budget()?,
        )?;
        self.account.source_purchase(32_768, 128)?;
        if let Some(candidate) = candidate {
            candidate.account.plan(&expected)?;
            candidate.account.plan(candidate.plan)?;
            candidate.account.before_candidate()?;
            if !super::driver_adapter::candidate_matches(
                &request.current_resource().execution.snapshot.execution_id,
                (&request.current_resource().execution.snapshot).into(),
                &expected,
                candidate.id,
                candidate.fence,
                candidate.plan,
            ) {
                candidate.account.refused_candidate();
                return Err(PermissionError::UnsupportedProfile);
            }
        }
        let commit = store.commit_permission_use_outcome(&request, &mut self.account)?;
        if commit.attempt == 0
            || commit.attempt != attempt
            || !Arc::ptr_eq(&commit.origin, request._origin)
            || !Arc::ptr_eq(&commit.producer, request._producer)
            || commit.now_ms != facts.now_ms
            || commit.resource != *request.current_resource()
            || commit.plan != expected
        {
            return Err(PermissionError::CorruptRecord);
        }
        self.completed = true;
        Ok(commit)
    }
    pub fn usage(&self) -> PermissionEvidenceUsage {
        self.account.evidence.usage()
    }
    pub fn callback_status(&self) -> PermissionUseCallbackStatus {
        self.receipt.status()
    }
    pub fn settled_outcome(&self) -> Option<&EffectOutcome> {
        self.receipt.settled_outcome()
    }
}
fn assert_carriers<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>() {
    const {
        assert!(size_of::<PermissionUseCallbackReceipt<'_, '_, '_, S, A>>() <= 16_384);
        assert!(size_of::<PermissionUseOutcomeWork<'_, '_, '_, S, A>>() <= 32_768);
        assert!(size_of::<Option<PermissionUseOutcomeWork<'_, '_, '_, S, A>>>() <= 32_768);
        assert!(size_of::<(PermissionUseCallbackReceipt<'_, '_, '_, S, A>,)>() <= 16_384);
        assert!(size_of::<(PermissionUseOutcomeWork<'_, '_, '_, S, A>,)>() <= 32_768);
        assert!(
            size_of::<Result<PermissionUseOutcomeWork<'_, '_, '_, S, A>, std::convert::Infallible>>(
            ) <= 32_768
        );
        assert!(
            size_of::<
                std::ops::ControlFlow<
                    std::convert::Infallible,
                    PermissionUseOutcomeWork<'_, '_, '_, S, A>,
                >,
            >() <= 32_768
        );
        assert!(
            size_of::<(
                &mut PermissionUseOutcomeWork<'_, '_, '_, S, A>,
                &TransitionFacts,
                u64
            )>() <= 128
        );
        assert!(size_of::<(&PermissionUseOutcomeWork<'_, '_, '_, S, A>,)>() <= 128);
        assert!(
            size_of::<(
                &mut PermissionUseOutcomeWork<'_, '_, '_, S, A>,
                &TransitionFacts
            )>() <= 128
        );
        assert!(size_of::<(&S, PermissionUseOutcomeRequest<'_>)>() <= 128);
        assert!(size_of::<Result<(&S, PermissionUseOutcomeRequest<'_>), PermissionError>>() <= 128);
        assert!(
            size_of::<
                std::ops::ControlFlow<
                    Result<std::convert::Infallible, PermissionError>,
                    (&S, PermissionUseOutcomeRequest<'_>),
                >,
            >() <= 128
        );
        assert!(
            size_of::<(
                &PermissionUseCallbackReceipt<'_, '_, '_, S, A>,
                &TransitionFacts,
                &Arc<OutcomeOrigin>,
                u64,
                &mut PermissionUseOutcomeAccount
            )>() <= 128
        );
    }
}
const _: () = {
    assert!(size_of::<OutcomeOrigin>() <= 128);
    assert!(size_of::<Arc<OutcomeOrigin>>() <= 128);
    assert!(size_of::<(OutcomeOrigin,)>() <= 128);
    assert!(size_of::<(*const OutcomeOrigin,)>() <= 128);
    assert!(size_of::<PermissionUseOutcomeAccount>() <= 1024);
    assert!(size_of::<PermissionEvidenceBudget>() <= 512);
    assert!(size_of::<PermissionUseOutcomeRequest<'static>>() <= 128);
    assert!(size_of::<PermissionUseOutcomeCommit>() <= 8192);
    assert!(size_of::<Result<PermissionUseOutcomeCommit, PermissionError>>() <= 8192);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                PermissionUseOutcomeCommit,
            >,
        >() <= 8192
    );
    assert!(size_of::<Result<(), PermissionError>>() <= 128);
    assert!(
        size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ()>>()
            <= 128
    );
    assert!(size_of::<Result<u64, PermissionError>>() <= 128);
    assert!(size_of::<Option<u64>>() <= 128);
    assert!(
        size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, u64>>()
            <= 128
    );
    assert!(size_of::<(&mut PermissionUseOutcomeAccount, usize, usize)>() <= 128);
    assert!(size_of::<(&mut PermissionUseOutcomeAccount, &Arc<OutcomeOrigin>, u64)>() <= 128);
    assert!(
        size_of::<(
            PermissionOutcomeClaimLoan<'_>,
            &InvocationContext,
            &EffectOutcome,
            &TransitionFacts,
            &Arc<OutcomeOrigin>,
            u64
        )>() <= 128
    );
    assert!(size_of::<PermissionEvidenceUsage>() <= 128);
    assert!(size_of::<PermissionUseCallbackStatus>() <= 128);
    assert!(size_of::<Option<&EffectOutcome>>() <= 128);
    assert!(size_of::<Phase>() <= 128);
    assert!(size_of::<(u64, u64)>() <= 128);
    assert!(size_of::<(usize, usize)>() <= 128);
    assert!(size_of::<bool>() <= 128);
};

// Concrete standard-library scalar/allocation and normal outer retirement carriers.
const _: () = {
    assert!(size_of::<(Arc<OutcomeOrigin>,)>() <= 128);
    assert!(size_of::<std::alloc::Layout>() <= 128);
    assert!(size_of::<(std::alloc::Layout,)>() <= 128);
    assert!(size_of::<std::ptr::NonNull<u8>>() <= 128);
    assert!(size_of::<(std::ptr::NonNull<u8>, std::alloc::Layout)>() <= 128);
    assert!(size_of::<(PermissionUseOutcomeAccount,)>() <= 1024);
    assert!(size_of::<(&PermissionUseOutcomeAccount,)>() <= 128);
    assert!(
        size_of::<(
            &PermissionUseOutcomeRequest<'_>,
            &mut PermissionUseOutcomeAccount
        )>() <= 128
    );
    assert!(size_of::<(&PermissionEvidenceBudget,)>() <= 128);
    assert!(size_of::<(&Phase, &Phase)>() <= 128);
    assert!(size_of::<(PermissionError,)>() <= 128);
    assert!(size_of::<(u64,)>() <= 128);
    assert!(size_of::<(usize,)>() <= 128);
};

const _: () = {
    assert!(size_of::<Result<PermissionUseOutcomeRequest<'_>, std::convert::Infallible>>() <= 128);
    assert!(
        size_of::<std::ops::ControlFlow<std::convert::Infallible, PermissionUseOutcomeRequest<'_>>>(
        ) <= 128
    );
    assert!(size_of::<(usize, usize, OutcomeOrigin)>() <= 128);
    assert!(size_of::<(Option<usize>, Option<usize>)>() <= 128);
};
const _: () = {
    assert!(size_of::<Option<Arc<OutcomeOrigin>>>() <= 128);
    assert!(size_of::<(&Arc<OutcomeOrigin>, &Arc<OutcomeOrigin>)>() <= 128);
    assert!(size_of::<(&Arc<OutcomeOrigin>,)>() <= 128);
    assert!(size_of::<(Option<Arc<OutcomeOrigin>>,)>() <= 128);
};

#[cfg(test)]
mod outcome_session_controls {
    use super::*;
    #[test]
    fn explicit_attempts_never_reenter_or_reset_the_account() {
        let mut account = PermissionUseOutcomeAccount::fresh();
        let origin = Arc::new(OutcomeOrigin);
        assert!(account.origin.is_none());
        assert!(account.enter(&origin, 0).is_err());
        assert!(account.enter(&origin, 1).is_ok());
        let paid = account.evidence.usage();
        assert!(account.enter(&origin, 1).is_err());
        assert!(account.enter(&origin, 2).is_err());
        assert_eq!(account.evidence.usage(), paid);
        account.phase = Phase::Finished;
        assert!(account.enter(&origin, 1).is_err());
        assert!(account.enter(&origin, 2).is_ok());
        assert_eq!(
            account.evidence.usage().encoded_bytes(),
            paid.encoded_bytes() * 2
        );
        assert_eq!(
            account.evidence.usage().structural_nodes(),
            paid.structural_nodes() * 2
        );
        account.phase = Phase::Finished;
        let foreign = Arc::new(OutcomeOrigin);
        assert!(account.enter(&foreign, 3).is_err());
        assert!(Arc::ptr_eq(account.origin.as_ref().unwrap(), &origin));
    }
}

// Append-only settlement source loans. Existing scaffold fields/bodies stay literal.
pub(super) struct OutcomeSourceLoan<'a> {
    pub(super) resource: &'a ApprovalResource,
    pub(super) invocation: &'a EffectInvocation,
    pub(super) witness: &'a PermissionRuntimeUseWitness,
    pub(super) producer: &'a Arc<PermissionRuntimeOrigin>,
    pub(super) context: &'a InvocationContext,
    pub(super) actual: &'a EffectOutcome,
    pub(super) controlled: bool,
    pub(super) current: &'a ApprovalResource,
    pub(super) facts: &'a TransitionFacts,
    pub(super) origin: &'a Arc<OutcomeOrigin>,
    pub(super) attempt: u64,
}
impl<'a> PermissionUseOutcomeRequest<'a> {
    pub(super) fn current_resource(&self) -> &'a ApprovalResource {
        self.current.unwrap_or(self._resource)
    }
    pub fn current_execution_id(&self) -> &b4_contract::ExecutionId {
        &self._resource.execution.snapshot.execution_id
    }
    pub fn current_resource_read<'b>(
        &self,
        account: &'b mut PermissionUseOutcomeAccount,
    ) -> Result<super::PermissionResourceRead<'b>, PermissionError> {
        account.source_admission(self._origin, self._producer, self._attempt)?;
        if !self.controlled {
            return Err(PermissionError::UnsupportedProfile);
        }
        account.source_resource_read()
    }
    pub(super) fn source_loan(&self) -> OutcomeSourceLoan<'a> {
        OutcomeSourceLoan {
            resource: self._resource,
            invocation: self._invocation,
            witness: self._witness,
            producer: self._producer,
            context: self._context,
            actual: self._actual,
            controlled: self.controlled,
            current: self.current_resource(),
            facts: self._facts,
            origin: self._origin,
            attempt: self._attempt,
        }
    }
    /// Sealed preparation entry; no caller plan/result is accepted.
    pub fn prepare_source<S: PermissionStore + ?Sized>(
        &self,
        store: &S,
        account: &mut PermissionUseOutcomeAccount,
    ) -> Result<super::PermissionUseOutcomePrepared, PermissionError> {
        // Fresh whole Prepared return/entry cohort precedes admission/loans.
        account.source_port_inception()?;
        // Source admission must precede selector/carrier creation.
        account.source_admission(self._origin, self._producer, self._attempt)?;
        account.source_purchase(LOAN_BYTES, LOAN_NODES)?;
        let selector =
            super::runtime_use_outcome_source::PermissionUseOutcomeSourceSelector::from_request(
                self,
            );
        store.capture_permission_use_outcome_source(&selector, account)
    }
}
impl PermissionUseOutcomeAccount {
    pub(super) fn source_admission(
        &mut self,
        origin: &Arc<OutcomeOrigin>,
        producer: &Arc<PermissionRuntimeOrigin>,
        attempt: u64,
    ) -> Result<(), PermissionError> {
        self.evidence.outcome_source_inception_reserve(
            super::runtime_use_outcome_source::ADMISSION_GUARD_FIXED,
            super::runtime_use_outcome_source::ADMISSION_GUARD_NODES,
        )?;
        if self.phase != Phase::Entered
            || attempt == 0
            || attempt != self.attempt
            || !self.origin.as_ref().is_some_and(
                super::runtime_use_outcome_source::outcome_checked_value(|owned| {
                    Arc::ptr_eq(owned, origin)
                }),
            )
        {
            return Err(PermissionError::UnsupportedProfile);
        }
        // The private Request carries the genuine producer. This move-stable
        // binding survives explicit retries and is never replaced/reset.
        match &self.producer {
            Some(bound) if !Arc::ptr_eq(bound, producer) => {
                return Err(PermissionError::CorruptRecord)
            }
            Some(_) => (),
            None => self.producer = Some(Arc::clone(producer)),
        }
        self.evidence.outcome_source_require_production()
    }
    pub(super) fn source_purchase(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), PermissionError> {
        self.evidence.outcome_source_inception_reserve(
            super::runtime_use_outcome_source::SELECTOR_GUARD_FIXED,
            super::runtime_use_outcome_source::SELECTOR_GUARD_NODES,
        )?;
        if self.phase != Phase::Entered {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.evidence.outcome_source_reserve(bytes, nodes)
    }
    pub(super) fn source_budget(
        &mut self,
    ) -> Result<&mut PermissionEvidenceBudget, PermissionError> {
        self.evidence.outcome_source_require_production()?;
        Ok(&mut self.evidence)
    }
    pub(super) fn source_resource_read(
        &mut self,
    ) -> Result<super::PermissionResourceRead<'_>, PermissionError> {
        self.evidence.use_outcome_resource_read()
    }
}
const _: () = {
    assert!(size_of::<OutcomeSourceLoan<'static>>() <= 128);
    assert!(size_of::<(&PermissionUseOutcomeRequest<'static>,)>() <= 128);
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeAccount,
            &Arc<OutcomeOrigin>,
            &Arc<PermissionRuntimeOrigin>,
            u64
        )>() <= 128
    );
};

const _: () = {
    use std::{convert::Infallible, ops::ControlFlow};
    assert!(size_of::<Result<OutcomeSourceLoan<'static>, Infallible>>() <= 128);
    assert!(size_of::<ControlFlow<Infallible, OutcomeSourceLoan<'static>>>() <= 128);
    assert!(size_of::<(&mut PermissionUseOutcomeAccount, usize, usize)>() <= 128);
    assert!(size_of::<Result<&mut PermissionEvidenceBudget, PermissionError>>() <= 128);
    assert!(
        size_of::<ControlFlow<Result<Infallible, PermissionError>, &mut PermissionEvidenceBudget>>(
        ) <= 128
    );
    assert!(size_of::<Result<super::PermissionResourceRead<'static>, PermissionError>>() <= 128);
    assert!(
        size_of::<
            ControlFlow<
                Result<Infallible, PermissionError>,
                super::PermissionResourceRead<'static>,
            >,
        >() <= 128
    );
};

impl PermissionUseOutcomeAccount {
    pub(super) fn source_port_inception(&mut self) -> Result<(), PermissionError> {
        self.evidence.outcome_source_inception_reserve(
            super::runtime_use_outcome_source::PORT_GUARD_FIXED,
            super::runtime_use_outcome_source::PORT_GUARD_NODES,
        )
    }
}

impl PermissionUseOutcomeAccount {
    pub(super) fn begin_source_attempt(
        &mut self,
        origin: &Arc<OutcomeOrigin>,
        producer: &Arc<PermissionRuntimeOrigin>,
        attempt: u64,
    ) -> Result<(), PermissionError> {
        let prior = self.source_attempt;
        // Consume the CURRENT account attempt before the first fallible debit,
        // including a foreign request or exhausted-account refusal.
        self.source_attempt = self.attempt;
        self.evidence.outcome_source_inception_reserve(
            super::runtime_use_outcome_source::LATCH_GUARD_FIXED,
            super::runtime_use_outcome_source::LATCH_GUARD_NODES,
        )?;
        if self.attempt == 0 || prior >= self.attempt {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.source_admission(origin, producer, attempt)
    }
    pub(super) fn begin_writer_attempt(
        &mut self,
        origin: &Arc<OutcomeOrigin>,
        producer: &Arc<PermissionRuntimeOrigin>,
        attempt: u64,
    ) -> Result<(), PermissionError> {
        let prior = self.writer_attempt;
        self.writer_attempt = self.attempt;
        self.evidence.outcome_source_inception_reserve(
            super::runtime_use_outcome_source::LATCH_GUARD_FIXED,
            super::runtime_use_outcome_source::LATCH_GUARD_NODES,
        )?;
        if self.attempt == 0 || prior >= self.attempt {
            return Err(PermissionError::UnsupportedProfile);
        }
        self.source_admission(origin, producer, attempt)
    }
}

#[cfg(test)]
mod source_writer_attempt_controls {
    use super::*;
    // Pure account protocol controls; no Receipt/witness/backend/result fabricated.
    #[test]
    fn each_refused_read_or_writer_consumes_its_attempt_without_reset() {
        let mut account = PermissionUseOutcomeAccount::fresh();
        // Preserve the original closed-purpose refusal control explicitly.
        account.evidence = PermissionEvidenceBudget::fresh_use_outcome_scaffold();
        let origin = Arc::new(OutcomeOrigin);
        let producer = Arc::new(PermissionRuntimeOrigin);
        assert!(account.enter(&origin, 1).is_ok());
        assert!(account.begin_source_attempt(&origin, &producer, 1).is_err());
        assert_eq!(account.source_attempt, 1);
        let first = account.evidence.usage();
        assert!(account.begin_source_attempt(&origin, &producer, 1).is_err());
        assert!(account.evidence.usage().encoded_bytes() > first.encoded_bytes());
        assert!(account.begin_writer_attempt(&origin, &producer, 1).is_err());
        assert_eq!(account.writer_attempt, 1);
        let paid = account.evidence.usage();
        account.phase = Phase::Finished;
        assert!(account.enter(&origin, 2).is_ok());
        assert!(account.evidence.usage().encoded_bytes() > paid.encoded_bytes());
        assert_eq!(account.source_attempt, 1);
        assert_eq!(account.writer_attempt, 1);
        assert!(account.begin_source_attempt(&origin, &producer, 2).is_err());
        assert_eq!(account.source_attempt, 2);
        assert!(Arc::ptr_eq(account.producer.as_ref().unwrap(), &producer));
    }
}
const _: () = {
    assert!(size_of::<PermissionUseOutcomeAccount>() <= 1024);
    assert!(size_of::<Option<Arc<PermissionRuntimeOrigin>>>() <= 128);
    assert!(size_of::<(u64, u64, u64)>() <= 128);
    assert!(
        size_of::<(
            &mut PermissionUseOutcomeAccount,
            &Arc<OutcomeOrigin>,
            &Arc<PermissionRuntimeOrigin>,
            u64
        )>() <= 128
    );
};

impl PermissionUseOutcomeAccount {
    pub(super) fn source_selector_inception(&mut self) -> Result<(), PermissionError> {
        self.evidence.outcome_source_inception_reserve(
            super::runtime_use_outcome_source::SELECTOR_GUARD_FIXED,
            super::runtime_use_outcome_source::SELECTOR_GUARD_NODES,
        )
    }
}

#[cfg(test)]
mod functional_outcome_brand_controls {
    use super::*;

    // Only private account protocol brands are constructed. There is no Claim,
    // witness, Receipt, Request, backend or successful Commit in these controls.
    #[test]
    fn functional_account_binds_origin_producer_and_each_exact_attempt() {
        let mut account = PermissionUseOutcomeAccount::fresh();
        let origin = Arc::new(OutcomeOrigin);
        let foreign_origin = Arc::new(OutcomeOrigin);
        let producer = Arc::new(PermissionRuntimeOrigin);
        let foreign_producer = Arc::new(PermissionRuntimeOrigin);
        assert!(account.enter(&origin, 1).is_ok());
        assert!(account.source_admission(&origin, &producer, 1).is_ok());
        for (brand, attempt) in [(&foreign_origin, 1), (&origin, 0), (&origin, 2)] {
            let before = account.evidence.usage();
            assert!(matches!(
                account.source_admission(brand, &producer, attempt),
                Err(PermissionError::UnsupportedProfile)
            ));
            assert!(account.evidence.usage().encoded_bytes() > before.encoded_bytes());
            assert!(account.evidence.usage().structural_nodes() > before.structural_nodes());
        }
        assert!(matches!(
            account.source_admission(&origin, &foreign_producer, 1),
            Err(PermissionError::CorruptRecord)
        ));
        assert!(Arc::ptr_eq(account.origin.as_ref().unwrap(), &origin));
        assert!(Arc::ptr_eq(account.producer.as_ref().unwrap(), &producer));
        assert!(account.begin_source_attempt(&origin, &producer, 1).is_ok());
        assert!(matches!(
            account.begin_source_attempt(&origin, &producer, 1),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(account.begin_writer_attempt(&origin, &producer, 1).is_ok());
        assert!(matches!(
            account.begin_writer_attempt(&origin, &producer, 1),
            Err(PermissionError::UnsupportedProfile)
        ));
        let paid = account.evidence.usage();
        account.phase = Phase::Finished;
        assert!(account.enter(&origin, 2).is_ok());
        assert_eq!((account.source_attempt, account.writer_attempt), (1, 1));
        // A stale incoming attempt consumes current attempt2, preventing a
        // replacement request from laundering that same account session.
        assert!(matches!(
            account.begin_source_attempt(&origin, &producer, 1),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert_eq!(account.source_attempt, 2);
        assert!(matches!(
            account.begin_source_attempt(&origin, &producer, 2),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(matches!(
            account.begin_writer_attempt(&origin, &foreign_producer, 2),
            Err(PermissionError::CorruptRecord)
        ));
        assert_eq!(account.writer_attempt, 2);
        assert!(matches!(
            account.begin_writer_attempt(&origin, &producer, 2),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(account.evidence.usage().encoded_bytes() > paid.encoded_bytes());
        assert!(account.evidence.usage().structural_nodes() > paid.structural_nodes());
        assert!(Arc::ptr_eq(account.producer.as_ref().unwrap(), &producer));
        account.phase = Phase::Finished;
        assert!(matches!(
            account.enter(&foreign_origin, 3),
            Err(PermissionError::UnsupportedProfile)
        ));
        assert!(Arc::ptr_eq(account.origin.as_ref().unwrap(), &origin));
    }

    #[test]
    fn exhausted_functional_account_never_resets_on_explicit_attempt() {
        let mut account = PermissionUseOutcomeAccount::fresh();
        let origin = Arc::new(OutcomeOrigin);
        let producer = Arc::new(PermissionRuntimeOrigin);
        assert!(account.enter(&origin, 1).is_ok());
        assert!(account.source_admission(&origin, &producer, 1).is_ok());
        let paid = account.evidence.usage();
        assert!(matches!(
            account.evidence.reserve(usize::MAX, 0),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(account.evidence.usage(), paid);
        account.phase = Phase::Finished;
        assert!(matches!(
            account.enter(&origin, 2),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(account.attempt, 2);
        assert_eq!(account.evidence.usage(), paid);
        assert!(matches!(
            account.begin_source_attempt(&origin, &producer, 2),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(account.source_attempt, 2);
        assert!(matches!(
            account.begin_writer_attempt(&origin, &producer, 2),
            Err(PermissionError::LimitExceeded)
        ));
        assert_eq!(account.writer_attempt, 2);
        assert_eq!(account.evidence.usage(), paid);
        assert!(Arc::ptr_eq(account.origin.as_ref().unwrap(), &origin));
        assert!(Arc::ptr_eq(account.producer.as_ref().unwrap(), &producer));
    }
}

// An ephemeral permissions-private data/account loan; it owns no authority and
// never aliases or replaces the receipt's cumulative outcome account.
struct CandidateLoan<'a> {
    id: &'a b4_contract::ExecutionId,
    fence: crate::ExecutionFence,
    plan: &'a CommitPlan,
    account: &'a mut super::driver_adapter_account::DriverAdapterAccount,
}

impl PermissionUseOutcomeCommit {
    // Only Work's exact returned-Commit validation can reach this private move.
    // No allocation, serialization, Store call or fallible projection follows it.
    pub(super) fn into_driver_execution(self) -> crate::StoredExecution {
        crate::StoredExecution {
            thread_id: self.resource.execution.thread_id,
            snapshot: self.plan.snapshot,
        }
    }
}
