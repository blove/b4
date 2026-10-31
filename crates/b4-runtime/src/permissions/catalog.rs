use super::{PermissionError, StableToolId, StableToolRevision, ToolConstraintRevision};
use crate::{
    ApplicationIdentity, EffectInvocation, InvocationContext, NativeApplication, NativeEffect,
};
use b4_contract::{Definition, EffectId, RouteKey, TaskRecord, TaskStatus};
use std::{
    fmt,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    sync::Arc,
};

const MAX_CATALOG_BINDINGS: usize = 64;

pub trait ToolConstraint: Send + Sync {
    fn validate_call(&self, call: &PermissionCall<'_>) -> Result<(), ToolConstraintError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolConstraintError {
    Denied,
    Unavailable,
}

impl fmt::Display for ToolConstraintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Denied => "permission_constraint_denied",
            Self::Unavailable => "permission_constraint_unavailable",
        })
    }
}

impl std::error::Error for ToolConstraintError {}

pub struct ToolBindingRegistration {
    pub definition: Definition,
    pub effect_id: EffectId,
    pub tool_id: StableToolId,
    pub tool_revision: StableToolRevision,
    pub constraint_revision: ToolConstraintRevision,
    pub constraint: Arc<dyn ToolConstraint>,
}

pub struct ToolBinding {
    pub application: ApplicationIdentity,
    pub definition: Definition,
    pub effect_id: EffectId,
    pub tool_id: StableToolId,
    pub tool_revision: StableToolRevision,
    pub constraint_revision: ToolConstraintRevision,
}

pub enum PermissionCallPhase<'a> {
    Unclaimed,
    Claimed(&'a EffectInvocation),
}

pub struct PermissionCall<'a> {
    pub context: &'a InvocationContext,
    pub definition: &'a Definition,
    pub task: &'a TaskRecord,
    pub binding: &'a ToolBinding,
    pub phase: PermissionCallPhase<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionConfigError;

impl fmt::Display for PermissionConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("permission_invalid_catalog")
    }
}

impl std::error::Error for PermissionConfigError {}

/// Exact current application mappings and borrowed constraint evaluation.
/// Assembly and evaluation never grant execution authority or install a host.
pub struct PermissionCatalog {
    application: Arc<NativeApplication>,
    entries: Vec<ToolBindingRegistration>,
}

impl PermissionCatalog {
    pub fn new(
        application: Arc<NativeApplication>,
        entries: Vec<ToolBindingRegistration>,
    ) -> Result<Self, PermissionConfigError> {
        if entries.len() > MAX_CATALOG_BINDINGS {
            return Err(PermissionConfigError);
        }
        for (index, entry) in entries.iter().enumerate() {
            if application.definition(&entry.definition.route) != Some(&entry.definition)
                || application
                    .output_kind(&entry.definition, &entry.effect_id)
                    .is_none()
                || entries[..index].iter().any(|previous| {
                    previous.definition == entry.definition && previous.effect_id == entry.effect_id
                })
            {
                return Err(PermissionConfigError);
            }
        }
        for definition in application.definitions() {
            for spec in definition.batches.iter().flatten() {
                if !entries.iter().any(|entry| {
                    entry.definition == *definition && entry.effect_id == spec.effect_id
                }) {
                    return Err(PermissionConfigError);
                }
            }
        }
        Ok(Self {
            application,
            entries,
        })
    }

    /// Clones the catalog with mapped callbacks and the same shared constraints.
    /// All registration metadata is retained; mapping grants no authority.
    pub fn map_native_callbacks<F>(&self, wrap: F) -> Self
    where
        F: FnMut(Arc<dyn NativeEffect>) -> Arc<dyn NativeEffect>,
    {
        Self {
            application: Arc::new(self.application.map_native_callbacks(wrap)),
            entries: self
                .entries
                .iter()
                .map(|entry| ToolBindingRegistration {
                    definition: entry.definition.clone(),
                    effect_id: entry.effect_id.clone(),
                    tool_id: entry.tool_id.clone(),
                    tool_revision: entry.tool_revision.clone(),
                    constraint_revision: entry.constraint_revision.clone(),
                    constraint: Arc::clone(&entry.constraint),
                })
                .collect(),
        }
    }

    pub fn application(&self) -> &Arc<NativeApplication> {
        &self.application
    }

    pub(crate) fn application_identity(&self) -> &ApplicationIdentity {
        self.application.identity()
    }

    pub(super) fn ready_registration<'catalog>(
        &'catalog self,
        definition: &Definition,
        effect: &EffectId,
        budget: &mut super::PermissionEvidenceBudget,
    ) -> Result<&'catalog ToolBindingRegistration, PermissionError> {
        super::validation::quote_ready_registration(definition, &self.entries, budget)?;
        if self.application.definition(&definition.route) != Some(definition) {
            return Err(PermissionError::InvalidCatalog);
        }
        let mut selected = None;
        for entry in &self.entries {
            if entry.definition == *definition && entry.effect_id == *effect {
                if selected.is_some() {
                    return Err(PermissionError::InvalidCatalog);
                }
                selected = Some(entry);
            }
        }
        selected.ok_or(PermissionError::InvalidCatalog)
    }

    pub(crate) fn current_definition(&self, route: &RouteKey) -> Option<&Definition> {
        self.application.definition(route)
    }

    #[allow(dead_code)] // Retained ordinary paid gate; writer capture uses a separate private seam.
                        // Paid Unclaimed evaluation only; the existing pure gate body stays exact.
    pub(crate) fn validate_call_budgeted(
        &self,
        context: &InvocationContext,
        definition: &Definition,
        task: &TaskRecord,
        phase: PermissionCallPhase<'_>,
        budget: &mut super::PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        super::validation::begin_permission_budgeted_unclaimed_gate(
            matches!(&phase, PermissionCallPhase::Claimed(_)),
            self.entries.len(),
            budget,
        )?;
        let native = self
            .application
            .definition(&definition.route)
            .ok_or(PermissionError::InvalidCatalog)?;
        super::validation::quote_permission_budgeted_unclaimed_gate(
            definition,
            native,
            task,
            self.application.identity(),
            &self.entries,
            budget,
        )?;
        self.validate_call(context, definition, task, phase)
    }

    pub fn validate_call(
        &self,
        context: &InvocationContext,
        definition: &Definition,
        task: &TaskRecord,
        phase: PermissionCallPhase<'_>,
    ) -> Result<(), PermissionError> {
        if self.application.definition(&definition.route) != Some(definition)
            || !definition.batches.iter().flatten().any(
                super::validation::ready_authorization_checked_metadata_value(
                    |spec: &b4_contract::TaskSpec| spec == &task.spec,
                ),
            )
        {
            return Err(PermissionError::InvalidCatalog);
        }
        let entry = self
            .entries
            .iter()
            .find(
                super::validation::ready_authorization_checked_metadata_value(
                    |entry: &&ToolBindingRegistration| {
                        entry.definition == *definition && entry.effect_id == task.spec.effect_id
                    },
                ),
            )
            .ok_or(PermissionError::InvalidCatalog)?;
        let identity = self.application.identity();
        if context.origin.application != identity.application
            || context.application_revision != identity.revision
            || context.execution_revision != definition.revision
            || !context_label(&context.origin.tenant)
            || !context_label(&context.origin.principal)
            || !context_label(&context.policy_revision)
        {
            return Err(PermissionError::ConstraintDenied);
        }
        let valid_phase = match &phase {
            PermissionCallPhase::Unclaimed => {
                if task.attempt_id.is_some() || task.result.is_some() || task.failure.is_some() {
                    return Err(PermissionError::ConstraintDenied);
                }
                match task.status {
                    TaskStatus::Ready => {
                        if task.approved {
                            task.spec.requires_approval && task.interrupt.is_some()
                        } else {
                            task.interrupt.is_none()
                        }
                    }
                    TaskStatus::WaitingForApproval => {
                        task.spec.requires_approval && !task.approved && task.interrupt.is_some()
                    }
                    _ => false,
                }
            }
            PermissionCallPhase::Claimed(invocation) => {
                // Complete execution/turn/current-owner linkage belongs to
                // the caller's matched StoredExecution, not these pure facts.
                let valid_approval = if task.spec.requires_approval {
                    task.approved && task.interrupt.is_some()
                } else {
                    !task.approved && task.interrupt.is_none()
                };
                task.status == TaskStatus::InFlight
                    && task.attempt_id.as_ref() == Some(&invocation.attempt_id)
                    && task.result.is_none()
                    && task.failure.is_none()
                    && valid_approval
                    && invocation.task_id == task.spec.id
                    && invocation.effect_id == task.spec.effect_id
                    && invocation.input == task.input
                    && invocation.owner_generation > 0
            }
        };
        if !valid_phase {
            return Err(PermissionError::ConstraintDenied);
        }
        let binding = ToolBinding {
            application: identity.clone(),
            definition: entry.definition.clone(),
            effect_id: entry.effect_id.clone(),
            tool_id: entry.tool_id.clone(),
            tool_revision: entry.tool_revision.clone(),
            constraint_revision: entry.constraint_revision.clone(),
        };
        let call = PermissionCall {
            context,
            definition,
            task,
            binding: &binding,
            phase,
        };
        match catch_unwind(
            super::validation::ready_authorization_checked_metadata_value(AssertUnwindSafe(
                super::validation::ready_authorization_checked_metadata_value(|| {
                    entry.constraint.validate_call(&call)
                }),
            )),
        ) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(ToolConstraintError::Denied)) => Err(PermissionError::ConstraintDenied),
            Ok(Err(ToolConstraintError::Unavailable)) => {
                Err(PermissionError::ConstraintUnavailable)
            }
            Err(payload) => {
                if let Err(replacement) = catch_unwind(
                    super::validation::ready_authorization_checked_metadata_value(
                        AssertUnwindSafe(
                            super::validation::ready_authorization_checked_metadata_value(|| {
                                drop(payload)
                            }),
                        ),
                    ),
                ) {
                    resume_unwind(replacement);
                }
                Err(PermissionError::ConstraintUnavailable)
            }
        }
    }
}

fn context_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .chars()
            .any(super::validation::ready_authorization_checked_metadata_value(char::is_control))
}

// Retention is private and data-only: no references into either moved View.
pub(super) struct PermissionCatalogCapture<'a> {
    pub(super) identity: std::sync::Arc<()>,
    pub(super) registration: &'a ToolBindingRegistration,
    pub(super) definition: super::validation::CanonicalMeasure,
}
impl PermissionCatalog {
    pub(super) fn validate_call_capture<'a>(
        &'a self,
        context: &InvocationContext,
        definition: &Definition,
        task: &TaskRecord,
        budget: &mut super::PermissionEvidenceBudget,
    ) -> Result<PermissionCatalogCapture<'a>, PermissionError> {
        let identity = super::validation::quote_permission_catalog_capture(budget)?;
        if self.entries.len() != 1 {
            return Err(PermissionError::UnsupportedProfile);
        }
        super::validation::begin_permission_budgeted_unclaimed_gate(false, 1, budget)?;
        let native = self
            .application
            .definition(&definition.route)
            .ok_or(PermissionError::InvalidCatalog)?;
        let measured = super::validation::quote_permission_captured_unclaimed_gate(
            definition,
            native,
            task,
            self.application.identity(),
            &self.entries,
            budget,
        )?;
        // The additional fixed capture quote pays this second full pair and
        // catalog-only handoffs. The existing gate still runs exactly once.
        if self.entries[0].definition != *definition
            || self.entries[0].effect_id != task.spec.effect_id
        {
            return Err(PermissionError::InvalidCatalog);
        }
        self.validate_call(context, definition, task, PermissionCallPhase::Unclaimed)?;
        Ok(PermissionCatalogCapture {
            identity,
            registration: &self.entries[0],
            definition: measured,
        })
    }
}

// Current immutable native/catalog selection and the literal Unclaimed gate
// run outside every SQL transaction. Returned measurements are actual passes.
impl PermissionCatalog {
    pub(super) fn runtime_unclaimed_capture<'catalog>(
        &'catalog self,
        context: &InvocationContext,
        definition: &Definition,
        task: &TaskRecord,
        budget: &mut super::PermissionEvidenceBudget,
    ) -> Result<
        (
            &'catalog Definition,
            &'catalog ToolBindingRegistration,
            [super::validation::CanonicalMeasure; 4],
        ),
        PermissionError,
    > {
        let result = (|| {
            super::validation::begin_permission_budgeted_unclaimed_gate(
                false,
                self.entries.len(),
                budget,
            )?;
            let native = self
                .application
                .definition(&definition.route)
                .ok_or(PermissionError::InvalidCatalog)?;
            let actual = super::validation::runtime_compat::quote_unclaimed_gate(
                definition,
                native,
                task,
                self.application.identity(),
                &self.entries,
                budget,
            )?;
            if self.entries.len() != 1 {
                return Err(PermissionError::UnsupportedProfile);
            }
            let registration = &self.entries[0];
            // The paid real gate performs its complete current definition,
            // context, binding, phase and caught constraint evaluation once.
            self.validate_call(context, definition, task, PermissionCallPhase::Unclaimed)?;
            Ok((native, registration, actual))
        })();
        budget.runtime_finish(result)
    }
}
const _: () = assert!(
    std::mem::size_of::<
        Result<
            (
                &Definition,
                &ToolBindingRegistration,
                [super::validation::CanonicalMeasure; 4]
            ),
            PermissionError,
        >,
    >() <= 256
);

// Data-only immutable cancellation selection. No Unclaimed gate or constraint call.
impl PermissionCatalog {
    pub(super) fn runtime_ready_cancellation_catalog_capture<'catalog>(
        &'catalog self,
        definition: &Definition,
        task: &TaskRecord,
        budget: &mut super::PermissionEvidenceBudget,
    ) -> Result<
        (
            &'catalog Definition,
            &'catalog ToolBindingRegistration,
            [super::validation::CanonicalMeasure; 4],
        ),
        PermissionError,
    > {
        let call = || {
            if self.entries.len() != 1 {
                return Err(PermissionError::UnsupportedProfile);
            }
            let native = self
                .application
                .definition(&definition.route)
                .ok_or(PermissionError::InvalidCatalog)?;
            let actual = super::validation::runtime_compat::quote_unclaimed_gate(
                definition,
                native,
                task,
                self.application.identity(),
                &self.entries,
                budget,
            )?;
            let registration = &self.entries[0];
            if native != definition
                || registration.definition != *definition
                || registration.effect_id != task.spec.effect_id
                || !definition
                    .batches
                    .iter()
                    .flatten()
                    .any(|spec| spec == &task.spec)
            {
                return Err(PermissionError::InvalidCatalog);
            }
            Ok((native, registration, actual))
        };
        let result = super::runtime_mutation::runtime_checked_mandatory_iife::<_, _, 128>(call);
        budget.runtime_finish(result)
    }
}

// Exact carrier premises for the new authorization caller; no callback/deep bound.
const _: () = assert!(std::mem::size_of::<ToolBinding>() <= 4_096);
const _: () = assert!(std::mem::size_of::<PermissionCall<'static>>() <= 128);
const _: () = assert!(std::mem::size_of::<ToolConstraintError>() <= 128);
const _: () = assert!(std::mem::size_of::<Result<(), ToolConstraintError>>() <= 128);
const _: () = assert!(
    std::mem::size_of::<Result<Result<(), ToolConstraintError>, Box<dyn std::any::Any + Send>>>()
        <= 128
);

// Renewal-only paid Claimed sibling. All prior gate methods remain literal.
impl PermissionCatalog {
    pub(super) fn validate_use_renewal_claimed_budgeted(
        &self,
        context: &InvocationContext,
        definition: &Definition,
        task: &TaskRecord,
        invocation: &EffectInvocation,
        budget: &mut super::PermissionEvidenceBudget,
    ) -> Result<(), PermissionError> {
        super::validation::begin_use_renewal_claimed_gate(self.entries.len(), budget)?;
        let native = self
            .application
            .definition(&definition.route)
            .ok_or(PermissionError::InvalidCatalog)?;
        super::validation::quote_permission_budgeted_unclaimed_gate(
            definition,
            native,
            task,
            self.application.identity(),
            &self.entries,
            budget,
        )?;
        let registration = self.ready_registration(definition, &task.spec.effect_id, budget)?;
        super::validation::quote_use_renewal_claimed_gate(
            task,
            invocation,
            self.application.identity(),
            registration,
            budget,
        )?;
        self.validate_call(
            context,
            definition,
            task,
            PermissionCallPhase::Claimed(invocation),
        )
    }
}
const _: () = {
    assert!(
        std::mem::size_of::<(
            &PermissionCatalog,
            &InvocationContext,
            &Definition,
            &TaskRecord,
            &EffectInvocation,
            &mut super::PermissionEvidenceBudget
        )>() <= 128
    );
    assert!(std::mem::size_of::<PermissionCallPhase<'static>>() <= 128);
    assert!(std::mem::size_of::<PermissionCall<'static>>() <= 128);
    assert!(std::mem::size_of::<ToolBinding>() <= 512);
    assert!(std::mem::size_of::<Result<(), PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<Result<std::convert::Infallible, PermissionError>, ()>,
        >() <= 128
    );
};

// Private callback-purpose borrowing only. No public application extraction.
impl PermissionCatalog {
    pub(super) fn callback_application(
        &self,
        budget: &mut super::runtime_use_callback::CallbackBudget,
    ) -> Result<&NativeApplication, PermissionError> {
        budget.purchase(
            super::runtime_use_callback::CALLBACK_LOAN_FIXED,
            super::runtime_use_callback::CALLBACK_LOAN_NODES,
        )?;
        Ok(&self.application)
    }
}
const _: () = {
    assert!(
        std::mem::size_of::<(
            &PermissionCatalog,
            &mut super::runtime_use_callback::CallbackBudget
        )>() <= 128
    );
    assert!(std::mem::size_of::<Result<&NativeApplication, PermissionError>>() <= 128);
    assert!(
        std::mem::size_of::<
            std::ops::ControlFlow<
                Result<std::convert::Infallible, PermissionError>,
                &NativeApplication,
            >,
        >() <= 128
    );
};

impl PermissionCatalog {
    pub(super) fn original_always_registration(
        &self,
        definition: &Definition,
        task: &TaskRecord,
    ) -> Result<&ToolBindingRegistration, PermissionError> {
        if self.entries.len() != 1 || self.current_definition(&definition.route) != Some(definition)
        {
            return Err(PermissionError::InvalidCatalog);
        }
        let registration = &self.entries[0];
        if registration.definition != *definition || registration.effect_id != task.spec.effect_id {
            return Err(PermissionError::InvalidCatalog);
        }
        Ok(registration)
    }
}
