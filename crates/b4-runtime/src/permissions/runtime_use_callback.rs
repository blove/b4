//! Consuming NewUse callback entry with privately retained authorization and result.
use super::{
    PermissionEvidenceBudget, PermissionEvidenceUsage, PermissionStore, PermissionUseDispatchPermit,
};
use crate::approval::ApprovalAuthority;
use crate::{validate_effect_outcome, EffectOutcome, NativeInvocationError};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::{any::Any, mem::size_of};

const CARRIER_BOUND: usize = 16_384;
const BUDGET_BOUND: usize = 512;
const LEAF_BOUND: usize = 128;
// Whole carrier/entry publication ticket; the selected-body purchases are separate.
const ENTRY_FIXED: usize = 8 * CARRIER_BOUND + 16 * LEAF_BOUND + 29_184;
const ENTRY_NODES: usize = 292;

/// Fixed observations of a consuming invocation; a receipt confers no replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionUseCallbackStatus {
    PreEntryRefused,
    Settled,
    NativeRejected,
    Uncertain,
    InvalidOutput,
    OutcomeRejected,
    Panicked,
}

// The budget brand and lifecycle cannot be supplied by an embedding caller.
pub(super) struct CallbackBudget {
    evidence: PermissionEvidenceBudget,
    entered: bool,
}
impl CallbackBudget {
    fn fresh() -> Self {
        Self {
            evidence: PermissionEvidenceBudget::fresh_use_callback(),
            entered: false,
        }
    }

    // Only this module can construct or enter the brand. Sibling seams can
    // purchase a short data loan after entry, never manufacture eligibility.
    pub(super) fn purchase(
        &mut self,
        bytes: usize,
        nodes: usize,
    ) -> Result<(), super::PermissionError> {
        if !self.entered {
            return Err(super::PermissionError::UnsupportedProfile);
        }
        self.evidence.callback_reserve(bytes, nodes)
    }
}

/// Owns the complete renewed permit before any fallible callback purchase.
/// Invocation consumes this carrier once; dropping it performs no settlement.
pub struct PermissionUseCallbackWork<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    permit: PermissionUseDispatchPermit<'producer, 'current, 'op, S, A>,
    budget: CallbackBudget,
}

/// Keeps the complete permit and actual result private through retirement.
/// Neither refusal nor a retained result supplies replay or storage authority.
pub struct PermissionUseCallbackReceipt<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    #[allow(dead_code)] // Owned anchor; intentionally no extraction API.
    permit: PermissionUseDispatchPermit<'producer, 'current, 'op, S, A>,
    budget: CallbackBudget,
    status: PermissionUseCallbackStatus,
    controlled: bool,
    #[allow(dead_code)] // Reserved private actual typed native error slot.
    native_error: Option<NativeInvocationError>,
    outcome: Option<EffectOutcome>,
    #[allow(dead_code)] // Retained privately; no unbounded payload observation.
    panic_payload: Option<Box<dyn Any + Send>>,
}

impl<'producer, 'current, 'op, S, A> PermissionUseDispatchPermit<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    /// Transfer the genuine permit into fresh consuming work without a debit.
    pub fn into_callback_work(self) -> PermissionUseCallbackWork<'producer, 'current, 'op, S, A> {
        assert_carriers::<S, A>();
        PermissionUseCallbackWork {
            permit: self,
            budget: CallbackBudget::fresh(),
        }
    }
}

impl<'producer, 'current, 'op, S, A> PermissionUseCallbackWork<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    /// Consume one renewed permit for the retained current application's direct entry.
    /// Consumption introduces no additional resource or policy fence after renewal.
    pub fn invoke(self) -> PermissionUseCallbackReceipt<'producer, 'current, 'op, S, A> {
        self.invoke_entry(None)
    }

    /// Consumes the renewed work through the native controlled entry.
    pub fn invoke_controlled(
        self,
        observer: &crate::NativeCancellationObserver,
    ) -> PermissionUseCallbackReceipt<'producer, 'current, 'op, S, A> {
        self.invoke_entry(Some(observer))
    }

    fn invoke_entry(
        mut self,
        observer: Option<&crate::NativeCancellationObserver>,
    ) -> PermissionUseCallbackReceipt<'producer, 'current, 'op, S, A> {
        assert_carriers::<S, A>();
        // Terminal even on the first failed debit. No receipt can re-enter.
        self.budget.entered = true;
        let mut status = PermissionUseCallbackStatus::PreEntryRefused;
        let mut native_error = None;
        let mut outcome = None;
        let mut panic_payload = None;
        if self.budget.purchase(ENTRY_FIXED, ENTRY_NODES).is_ok() {
            if let Ok(loan) = self.permit.callback_use_loan(&mut self.budget) {
                // Purchase the selected finite native graph before entry.
                if self
                    .budget
                    .purchase(NATIVE_ENTRY_FIXED, NATIVE_ENTRY_NODES)
                    .is_ok()
                {
                    let callback = checked_callback_value(|| match observer {
                        None => {
                            loan.application
                                .invoke(loan.definition, loan.invocation, loan.context)
                        }
                        Some(observer) => loan
                            .application
                            .invoke_controlled(
                                loan.definition,
                                loan.invocation,
                                loan.context,
                                observer,
                            )
                            .map(|actual| match actual {
                                crate::ControlledNativeOutcome::Settled(
                                    crate::NativeOutcome::Succeeded(result),
                                ) => EffectOutcome::Succeeded(result),
                                crate::ControlledNativeOutcome::Settled(
                                    crate::NativeOutcome::Failed(reason),
                                ) => EffectOutcome::Failed(reason),
                                crate::ControlledNativeOutcome::Cancelled => {
                                    EffectOutcome::Cancelled
                                }
                            }),
                    });
                    let actual = catch_unwind(checked_callback_value(AssertUnwindSafe(callback)));
                    match actual {
                        Ok(Ok(result)) => {
                            // Own the actual return before any subsequent fallible
                            // purchase. Rejection keeps it private in the receipt.
                            outcome = Some(result);
                            status = PermissionUseCallbackStatus::OutcomeRejected;
                            if self
                                .budget
                                .purchase(OUTCOME_ENTRY_FIXED, OUTCOME_ENTRY_NODES)
                                .is_ok()
                                && match &outcome {
                                    Some(actual) => validate_effect_outcome(actual, 65_536).is_ok(),
                                    None => false,
                                }
                            {
                                status = PermissionUseCallbackStatus::Settled;
                            }
                        }
                        Ok(Err(error)) => {
                            status = match error {
                                NativeInvocationError::Uncertain => {
                                    PermissionUseCallbackStatus::Uncertain
                                }
                                NativeInvocationError::InvalidOutput => {
                                    PermissionUseCallbackStatus::InvalidOutput
                                }
                                NativeInvocationError::DefinitionUnavailable
                                | NativeInvocationError::ContextMismatch
                                | NativeInvocationError::TaskEffectMismatch => {
                                    PermissionUseCallbackStatus::NativeRejected
                                }
                            };
                            native_error = Some(error);
                        }
                        Err(payload) => {
                            status = PermissionUseCallbackStatus::Panicked;
                            panic_payload = Some(payload);
                        }
                    }
                }
            }
        }
        PermissionUseCallbackReceipt {
            permit: self.permit,
            budget: self.budget,
            status,
            controlled: observer.is_some(),
            native_error,
            outcome,
            panic_payload,
        }
    }
}

impl<S, A> PermissionUseCallbackReceipt<'_, '_, '_, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    pub fn status(&self) -> PermissionUseCallbackStatus {
        self.status
    }

    pub fn usage(&self) -> PermissionEvidenceUsage {
        self.budget.evidence.usage()
    }

    /// Only a validated settlement exposes a borrowed result.
    pub fn settled_outcome(&self) -> Option<&EffectOutcome> {
        if self.status == PermissionUseCallbackStatus::Settled {
            self.outcome.as_ref()
        } else {
            None
        }
    }
}

fn assert_carriers<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>() {
    const {
        assert!(size_of::<PermissionUseDispatchPermit<'_, '_, '_, S, A>>() <= 8192);
        assert!(size_of::<PermissionUseCallbackWork<'_, '_, '_, S, A>>() <= CARRIER_BOUND);
        assert!(size_of::<PermissionUseCallbackReceipt<'_, '_, '_, S, A>>() <= CARRIER_BOUND);
        assert!(
            size_of::<Option<PermissionUseCallbackReceipt<'_, '_, '_, S, A>>>() <= CARRIER_BOUND
        );
        assert!(size_of::<(&PermissionUseCallbackReceipt<'_, '_, '_, S, A>,)>() <= LEAF_BOUND);
        assert!(size_of::<(PermissionUseDispatchPermit<'_, '_, '_, S, A>,)>() <= CARRIER_BOUND);
        assert!(size_of::<(PermissionUseCallbackWork<'_, '_, '_, S, A>,)>() <= CARRIER_BOUND);
    }
}
const _: () = {
    assert!(size_of::<CallbackBudget>() <= BUDGET_BOUND);
    assert!(size_of::<PermissionEvidenceBudget>() <= BUDGET_BOUND);
    assert!(size_of::<PermissionEvidenceUsage>() <= LEAF_BOUND);
    assert!(size_of::<PermissionUseCallbackStatus>() <= LEAF_BOUND);
    assert!(size_of::<Option<PermissionUseCallbackStatus>>() <= LEAF_BOUND);
    assert!(size_of::<Option<PermissionEvidenceUsage>>() <= LEAF_BOUND);
    assert!(size_of::<NativeInvocationError>() <= LEAF_BOUND);
    assert!(size_of::<Option<NativeInvocationError>>() <= LEAF_BOUND);
    assert!(size_of::<EffectOutcome>() <= LEAF_BOUND);
    assert!(size_of::<Option<EffectOutcome>>() <= LEAF_BOUND);
    assert!(size_of::<Option<&EffectOutcome>>() <= LEAF_BOUND);
    assert!(size_of::<Box<dyn Any + Send>>() <= LEAF_BOUND);
    assert!(size_of::<Option<Box<dyn Any + Send>>>() <= LEAF_BOUND);
    assert!(size_of::<Result<(), super::PermissionError>>() <= LEAF_BOUND);
    assert!(size_of::<(&mut PermissionEvidenceBudget, usize, usize)>() <= LEAF_BOUND);
};

// Separate pre-entry tickets for the finite Bool/empty-writes and Void checks.
// Each reached derived enum comparison includes both discriminant reads and Eq.
// The whole source and external inception envelope are qualified independently.
const NATIVE_ENTRY_FIXED: usize = 1_302_688_004;
const NATIVE_ENTRY_NODES: usize = 10_177_177;
const OUTCOME_ENTRY_FIXED: usize = 3_397_160_672;
const OUTCOME_ENTRY_NODES: usize = 26_534_248;
pub(super) const CALLBACK_LOAN_FIXED: usize = 48_640;
pub(super) const CALLBACK_LOAN_NODES: usize = 380;

fn checked_callback_value<F, R>(value: F) -> F
where
    F: FnOnce() -> R,
{
    const {
        assert!(size_of::<F>() <= LEAF_BOUND);
        assert!(size_of::<(F,)>() <= LEAF_BOUND);
        assert!(size_of::<(F, ())>() <= LEAF_BOUND);
        assert!(size_of::<()>() <= LEAF_BOUND);
        assert!(size_of::<R>() <= LEAF_BOUND);
        assert!(size_of::<Result<R, Box<dyn Any + Send>>>() <= LEAF_BOUND);
        assert!(size_of::<Result<R, std::convert::Infallible>>() <= LEAF_BOUND);
        assert!(size_of::<std::ops::ControlFlow<std::convert::Infallible, R>>() <= LEAF_BOUND);
        assert!(size_of::<std::mem::ManuallyDrop<F>>() <= LEAF_BOUND);
        assert!(size_of::<std::mem::ManuallyDrop<R>>() <= LEAF_BOUND);
    }
    value
}
const _: () = {
    assert!(size_of::<Result<EffectOutcome, NativeInvocationError>>() <= LEAF_BOUND);
    assert!(
        size_of::<Result<Result<EffectOutcome, NativeInvocationError>, Box<dyn Any + Send>>>()
            <= LEAF_BOUND
    );
    assert!(size_of::<Result<(), crate::DriverConfigError>>() <= LEAF_BOUND);
    assert!(size_of::<(&EffectOutcome, usize)>() <= LEAF_BOUND);
    assert!(size_of::<(&mut CallbackBudget, usize, usize)>() <= LEAF_BOUND);
};

// Actual borrowed entry operands and the unit-output purchase Try incarnation.
const _: () = {
    assert!(
        size_of::<(
            &crate::NativeApplication,
            &b4_contract::Definition,
            &crate::EffectInvocation,
            &crate::InvocationContext
        )>() <= LEAF_BOUND
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, super::PermissionError>, ()>,
        >() <= LEAF_BOUND
    );
};

// Existing native and validator bodies remain literal; these name the actual
// public operand, return and residual incarnations used by their new caller.
const _: () = {
    assert!(size_of::<(&crate::NativeApplication, &b4_contract::Definition)>() <= LEAF_BOUND);
    assert!(size_of::<Option<&crate::RouteRegistration>>() <= LEAF_BOUND);
    assert!(size_of::<Result<&crate::EffectBinding, NativeInvocationError>>() <= LEAF_BOUND);
    assert!(size_of::<b4_contract::TaskResult>() <= LEAF_BOUND);
    assert!(size_of::<crate::NativeOutcome>() <= LEAF_BOUND);
    assert!(size_of::<Result<crate::NativeOutcome, crate::EffectUncertain>>() <= LEAF_BOUND);
    assert!(size_of::<Result<crate::NativeOutcome, NativeInvocationError>>() <= LEAF_BOUND);
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, NativeInvocationError>,
                &crate::EffectBinding,
            >,
        >() <= LEAF_BOUND
    );
    assert!(
        size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, NativeInvocationError>,
                crate::NativeOutcome,
            >,
        >() <= LEAF_BOUND
    );
    assert!(size_of::<(fn(*mut u8), *mut u8, fn(*mut u8, *mut u8))>() <= LEAF_BOUND);
    assert!(size_of::<(&serde_json::Value, usize)>() <= LEAF_BOUND);
    assert!(size_of::<Vec<(&serde_json::Value, usize)>>() <= LEAF_BOUND);
    assert!(size_of::<Option<(&serde_json::Value, usize)>>() <= LEAF_BOUND);
    assert!(size_of::<std::slice::Iter<'static, b4_contract::StateWrite>>() <= LEAF_BOUND);
    assert!(size_of::<std::str::Chars<'static>>() <= LEAF_BOUND);
    assert!(size_of::<std::io::Result<usize>>() <= LEAF_BOUND);
    assert!(size_of::<std::io::Result<()>>() <= LEAF_BOUND);
    assert!(size_of::<serde_json::Result<()>>() <= LEAF_BOUND);
    assert!(
        size_of::<std::ops::ControlFlow<Result<std::convert::Infallible, serde_json::Error>, ()>>()
            <= LEAF_BOUND
    );
};

// The actual map-comparison adapter owns both iterator carriers.
const _: () = {
    type MapIter = std::collections::btree_map::Iter<'static, String, serde_json::Value>;
    type MapZip = std::iter::Zip<MapIter, MapIter>;
    assert!(size_of::<MapZip>() <= 256);
    assert!(size_of::<(MapIter, MapIter)>() <= 256);
    assert!(size_of::<(MapZip,)>() <= 256);
    assert!(size_of::<(&mut MapZip,)>() <= LEAF_BOUND);
    assert!(
        size_of::<Option<((&String, &serde_json::Value), (&String, &serde_json::Value))>>()
            <= LEAF_BOUND
    );
    assert!(size_of::<[(&serde_json::Value, usize); 1]>() == 16);
    assert!(size_of::<(&serde_json::Value, usize)>() == 16);
};

const _: () = {
    assert!(
        size_of::<std::collections::btree_map::Iter<'static, String, serde_json::Value>>()
            <= LEAF_BOUND
    );
    assert!(
        size_of::<std::iter::Flatten<std::slice::Iter<'static, Vec<b4_contract::TaskSpec>>>>()
            <= LEAF_BOUND
    );
    assert!(size_of::<std::iter::Enumerate<std::slice::Iter<'static, u8>>>() <= LEAF_BOUND);
    assert!(size_of::<Option<(usize, &u8)>>() <= LEAF_BOUND);
    assert!(
        size_of::<std::ops::ControlFlow<std::convert::Infallible, Option<(usize, &u8)>>>()
            <= LEAF_BOUND
    );
    assert!(size_of::<std::ops::ControlFlow<(), bool>>() <= LEAF_BOUND);
    assert!(size_of::<Result<(), std::io::Error>>() <= LEAF_BOUND);
    assert!(
        size_of::<Option<((&String, &serde_json::Value), (&String, &serde_json::Value))>>()
            <= LEAF_BOUND
    );
    assert!(
        size_of::<(
            &dyn crate::NativeEffect,
            &crate::EffectInvocation,
            &crate::InvocationContext
        )>() <= LEAF_BOUND
    );
};

impl<'producer, 'current, 'op, S, A> PermissionUseCallbackReceipt<'producer, 'current, 'op, S, A>
where
    S: PermissionStore + ?Sized,
    A: ApprovalAuthority + ?Sized,
{
    /// Retain the complete actual receipt in fresh outcome work, without debit.
    pub fn into_outcome_work(
        self,
    ) -> super::PermissionUseOutcomeWork<'producer, 'current, 'op, S, A> {
        const {
            assert!(size_of::<(Self,)>() <= CARRIER_BOUND);
            assert!(size_of::<super::PermissionUseOutcomeWork<'_, '_, '_, S, A>>() <= 32_768);
        }
        super::PermissionUseOutcomeWork::from_receipt(self)
    }
    pub(super) fn outcome_request<'input>(
        &'input self,
        facts: &'input b4_contract::TransitionFacts,
        origin: &'input std::sync::Arc<super::runtime_use_outcome::OutcomeOrigin>,
        attempt: u64,
        account: &mut super::PermissionUseOutcomeAccount,
    ) -> Result<(&'input S, super::PermissionUseOutcomeRequest<'input>), super::PermissionError>
    {
        const {
            assert!(
                size_of::<(
                    &Self,
                    &b4_contract::TransitionFacts,
                    &std::sync::Arc<super::runtime_use_outcome::OutcomeOrigin>,
                    u64,
                    &mut super::PermissionUseOutcomeAccount
                )>() <= 128
            );
            assert!(
                size_of::<
                    Result<(&S, super::PermissionUseOutcomeRequest<'_>), super::PermissionError>,
                >() <= 128
            );
            assert!(
                size_of::<
                    std::ops::ControlFlow<
                        Result<std::convert::Infallible, super::PermissionError>,
                        (&S, super::PermissionUseOutcomeRequest<'_>),
                    >,
                >() <= 128
            );
        }
        account.purchase(
            super::runtime_use_outcome::LOAN_BYTES,
            super::runtime_use_outcome::LOAN_NODES,
        )?;
        if self.status != PermissionUseCallbackStatus::Settled || attempt == 0 {
            return Err(super::PermissionError::UnsupportedProfile);
        }
        let actual = match self.outcome.as_ref() {
            Some(actual @ (EffectOutcome::Succeeded(_) | EffectOutcome::Failed(_))) => actual,
            Some(actual @ EffectOutcome::Cancelled) if self.controlled => actual,
            _ => return Err(super::PermissionError::UnsupportedProfile),
        };
        let (store, loan) = self.permit.outcome_use_loan(account)?;
        Ok((
            store,
            super::PermissionUseOutcomeRequest::from_receipt_loan(
                loan.claim,
                loan.context,
                actual,
                self.controlled,
                facts,
                origin,
                attempt,
            ),
        ))
    }
}
const _: () = {
    assert!(size_of::<(&Option<EffectOutcome>,)>() <= 128);
    assert!(size_of::<(&PermissionUseCallbackStatus, &PermissionUseCallbackStatus)>() <= 128);
    assert!(size_of::<super::runtime_use_renewal::PermissionOutcomePermitLoan<'_>>() <= 128);
};

impl<S: PermissionStore + ?Sized, A: ApprovalAuthority + ?Sized>
    PermissionUseCallbackWork<'_, '_, '_, S, A>
{
    pub(super) fn cancel_withheld(
        &self,
        authority: &dyn super::PermissionCancellationAuthority,
        catalog: &super::PermissionCatalog,
        request: &super::PermissionUseWithheldCancellationRequest<'_>,
        account: &mut super::use_withheld_cancellation::WithheldAccount,
    ) -> Result<crate::StoredExecution, super::PermissionError> {
        if self.budget.entered {
            return Err(super::PermissionError::UnsupportedProfile);
        }
        let (store, claim, context) = self.permit.withheld_use_loan(&mut account.budget)?;
        super::use_withheld_cancellation::cancel(
            store, authority, catalog, request, claim, context, account,
        )
    }
}
