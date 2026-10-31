use super::{ControlledNativeOutcome, NativeCancellationObserver};
use crate::{EffectInvocation, ThreadOrigin};
use b4_contract::{
    Definition, EffectId, ExecutionRevision, TaskResult, ThreadGeneration, ThreadId,
};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationIdentity {
    pub application: String,
    pub revision: String,
}

/// Transient trusted embedding context. This is not standalone authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationContext {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub origin: ThreadOrigin,
    pub application_revision: String,
    pub execution_revision: ExecutionRevision,
    pub policy_revision: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectUncertain;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidApplicationInput;

/// Settled outcomes of the existing direct native entry.
#[derive(Debug, Clone, PartialEq)]
pub enum NativeOutcome {
    Succeeded(TaskResult),
    Failed(String),
}

/// Feedback for transient chunk projection; this does not settle an effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeChunkResult {
    Accepted,
    Unavailable,
    Closed,
    Limit,
}

/// A borrowed destination for transient native chunks.
pub trait NativeChunkSink: Send {
    fn emit(&mut self, chunk: &Value) -> NativeChunkResult;
}

pub trait NativeEffect: Send + Sync {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain>;

    /// Optional cooperative entry; the default calls the existing entry unchanged.
    ///
    /// The observer supplies accepted intent, not completion or authorization.
    fn invoke_controlled(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
        _observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.invoke(invocation, context)
            .map(ControlledNativeOutcome::Settled)
    }

    /// Optional streaming entry; the default preserves cooperative callbacks.
    /// Chunks are transient projections, independent of the final outcome.
    fn invoke_streaming(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
        observer: &NativeCancellationObserver,
        _sink: &mut dyn NativeChunkSink,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.invoke_controlled(invocation, context, observer)
    }
}

/// Trusted pure validation; the owning host bounds concurrent calls and catches
/// panics. Implementations must not mutate business state or runtime storage.
pub trait InputValidator: Send + Sync {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OutputKind {
    #[default]
    Value,
    Void,
}

#[derive(Clone)]
pub struct EffectBinding {
    pub callback: Arc<dyn NativeEffect>,
    pub output_kind: OutputKind,
}
impl fmt::Debug for EffectBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EffectBinding")
            .field("output_kind", &self.output_kind)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct RouteRegistration {
    pub definition: Definition,
    pub effects: BTreeMap<EffectId, EffectBinding>,
    pub input_validator: Option<Arc<dyn InputValidator>>,
}
impl fmt::Debug for RouteRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouteRegistration")
            .field("effect_count", &self.effects.len())
            .field("has_input_validator", &self.input_validator.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationConfigError(pub &'static str);
impl fmt::Display for ApplicationConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ApplicationConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationInputError {
    DefinitionUnavailable,
    InvalidInput,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeInvocationError {
    DefinitionUnavailable,
    ContextMismatch,
    TaskEffectMismatch,
    InvalidOutput,
    Uncertain,
}
