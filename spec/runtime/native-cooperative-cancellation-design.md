# Native cooperative cancellation

Status: accepted scoped design for continuous local implementation. The preceding unchanged-AP-client qualification is frozen. Whole independent SPEC and QUALITY review of these adopted documents precedes implementation. No extra user approval step, commit, dependency, or original-client execution is needed.

## Goal and boundary

An already-entered native callback may observe explicit cancellation accepted for its original owned turn and acknowledge that it has actually stopped. The host still retains the callback, completion, store, application, thread claim and permits until they physically retire. No AP body, terminal, capability descriptor, persisted format, driver, kernel, or storage transaction changes.

RFC12.1 requires generation-bound cancellation, stopping future scheduling, signalling active effects and retaining committed results. Existing `RunControl` gates callback entry and publishes prompt cancellation only after exact `cancel_run_fenced` success. Entered native callbacks currently receive identity-only `InvocationContext` and can return only successful `TaskResult`, known public failure, or uncertainty. This adds the missing cooperative interface; it does not repair existing ownership behavior.

## Alternatives

1. **Observer plus separate controlled outcome (recommended).** Add a default trait entry that delegates to the existing entry, and an optional acknowledgement in a new enum. A callback can stop without fabricating a result or fault. Existing callbacks, context literals and exhaustive old enum matches remain valid.
2. **Observer only.** Smaller API, but a callback which stops with no genuine task result cannot report a clean settled cancellation. Returning `EffectUncertain` would fault readiness; reporting `Failed("cancelled")` or a made-up success is incorrect. This is coherent only as a notification-only increment with clean cooperative stopping explicitly deferred, and does not deliver the recommended capability.
3. **Change old `NativeOutcome` or `InvocationContext`.** Fewer entry methods, but a new enum variant breaks exhaustive matches and a new mandatory context field breaks struct literals. Avoid this source break.

## Proposed additive Rust interface

Names below are selected draft names, not current compiled APIs:

```rust,ignore
pub struct NativeCancellationSignal { /* private Arc<AtomicBool> */ }
#[derive(Clone)]
pub struct NativeCancellationObserver { /* same private Arc<AtomicBool> */ }

impl NativeCancellationSignal {
    pub fn new() -> Self; // initially false; Default delegates to new
    pub fn observer(&self) -> NativeCancellationObserver;
    pub fn commit_cancellation(&self); // trusted producer: monotonic Release store
}
impl Default for NativeCancellationSignal {
    fn default() -> Self { Self::new() }
}
impl NativeCancellationObserver {
    pub fn cancellation_requested(&self) -> bool; // Acquire load only
}

pub enum ControlledNativeOutcome {
    Settled(NativeOutcome),
    Cancelled,
}

pub trait NativeEffect: Send + Sync {
    // Existing required method, signature and direct-entry semantics remain.
    fn invoke(&self, call: &EffectInvocation, context: &InvocationContext)
        -> Result<NativeOutcome, EffectUncertain>;
    fn invoke_controlled(&self, call: &EffectInvocation,
        context: &InvocationContext, observer: &NativeCancellationObserver)
        -> Result<ControlledNativeOutcome, EffectUncertain>
    {
        self.invoke(call, context).map(ControlledNativeOutcome::Settled)
    }
}

impl NativeApplication {
    // Existing invoke continues invoking only NativeEffect::invoke.
    pub fn invoke_controlled(&self, selected: &Definition,
        call: &EffectInvocation, context: &InvocationContext,
        observer: &NativeCancellationObserver)
        -> Result<ControlledNativeOutcome, NativeInvocationError>;
}
```

The signal is public because HTTP and portable trusted embeddings are separate crates. It is a producer primitive, not a credential or store capability. No signal reference is passed to callbacks. An application can construct a different signal, but that cannot authorize HTTP acknowledgement: HTTP checks its own original `RunControl` signal and exact invocation identity. The observer has no mutator, cancellation callback, future, waker, event queue, closure, arbitrary I/O, thread, logger, metadata, run identity, Store, application, host, claim or permit. No Serialize/Deserialize implementation. Debug is fixed, or only includes the boolean. Dropping either type does not cancel or perform work. Retained observer clones keep only the small atomic allocation alive.

The observer reports an accepted durable decision, not task completion, physical quiescence, rollback, or an automatically interrupted external operation. Callback code polls at application-selected safe boundaries. Blocking I/O and uncooperative old callbacks keep their existing behavior and unbounded duration. A callback returning `Cancelled` promises its owned business work, including any descendants it started, is stopped before returning. Trusted callbacks are not sandboxed; the framework cannot discover or stop detached application work.

## Portable validation and old API preservation

`NativeApplication::invoke_controlled` selects the same exact full Definition and binding, validates identical origin/application/execution revisions, finite labels, owner and task/effect membership before calling native code, then applies the same Void/null result rule. Settled success/failure stays a real `NativeOutcome`. Callback `EffectUncertain`, invalid output and panic keep their existing fault semantics.

A returned `ControlledNativeOutcome::Cancelled` with the supplied observer still false returns existing static `NativeInvocationError::Uncertain`; it is not a public application error or successful cancellation. This registry check alone is not durable authorization for a trusted standalone caller. HTTP must separately validate its own signal and original invocation as below. Do not add variants or fields to old `NativeOutcome`, `InvocationContext`, `EffectBinding`, `RouteRegistration`, `NativeInvocationError`, or `EffectOutcome`. Old application `invoke` must never call the optional controlled override, even if that callback overrides it. A small private common selected-binding/context validation helper may prevent validation drift, while retaining the old entry's behavior.

## Durable publication and exact run isolation

Each original `RunControl::new` creates one initially-clear signal. All native callbacks entered for that control borrow observer clones backed by that exact signal. A successor gets a distinct fresh allocation. Do not reset or reuse a signal, use a process-global token, obtain a signal from a thread-name lookup, or attach it to mutable latest-run state.

The single HTTP publication point is inside `CancellationPending::commit`, in its existing `Gate::Pending -> Gate::Committed` branch, after `cancel_run_fenced` returned successful exact durable commit and before selecting/waking its terminal. Publish with an infallible Release atomic store while already holding the brief publication lock; do not invoke application code. `begin_cancel`, Pending, policy success, capture, retries, rejection, unsupported capability, store refusal/error, panic, viewer detach, completed turn, host admission cut, shutdown deadline and recovery never publish the signal.

If a producer fault changes Pending to Faulted before commit's gate branch, that branch refuses as today and does not publish, even if a storage mutation was already durably written. This is a conservative availability fault, not an accepted local signal. Conversely, a signal already published after successful committed gate acceptance remains sticky true after a later host or sibling fault. Fault does not retroactively revoke an accepted durable decision; readiness still remains faulted and no new callback entry becomes permitted. Keeping the sticky signal separate from `RunControl::committed()` is deliberate: the latter currently reads a gate which `fault()` changes to Faulted and must retain its old driver-race behavior.

Cancellation retains original generation/execution/turn/owner identity and registry token checks from `cancellation.rs`; no transport, policy, claim-registry or store behavior changes. Stale cancellation across a successor never publishes either old-completed or new observer. Denied/failed attempts leave their observers clear. Cancellation request abandonment after owned work was admitted may still commit and publish; merely dropping a wait/stream/attach viewer never does.

## Host acknowledgement and outcome retirement

`ScriptDriverHost::invoke` retains its existing exact invocation and Definition membership checks and entry gate. Never-entered callbacks still use the existing host `EffectOutcome::Cancelled` path, invoking neither native method.

For an entered native callback, call `NativeApplication::invoke_controlled` with the original control's observer. Convert `Settled(Succeeded/Failed)` to the same driver outcomes as today. Convert `Cancelled` only if the original control's sticky accepted-cancellation signal is true and the call still matches its immutable execution/turn/owner and selected task/effect checks. Reuse the existing static private invocation-failure channel for a false acknowledgement; do not expose callback text, fabricate a Failed reason, or turn it into success.

This mapping is not a completion commit. The unchanged driver stores its returned outcome envelope, retains task capacity through exact fenced completion and postcommit handling, and calls the existing `Runtime::complete_outcome -> KernelCommand::CancelAttempt`. Kernel checks the actual owner/task/attempt, requires its existing cancellation/failure-drain state, preserves budget/history, and creates no successful result or public failure. CAS repair remains completion-only with bounded retries; it never calls the native callback again. A later fault after signal publication does not clear that proof or erase a positively returned acknowledgement, but still faults readiness and preserves unrelated uncertain attempts. Test this interaction directly; do not broaden old `RunControl::committed()` or driver scheduling decisions.

Late actual `Settled(Succeeded)` after cancellation remains a Succeeded task result inside Cancelled execution, with no reducer application. Late actual `Settled(Failed)` retains current failure precedence and durable evidence. Both keep the already selected single wire cancellation terminal. Calling the old entry ignores cooperative override and retains ordinary real-outcome behavior. No normal terminal, known failure, uncertainty or shutdown should set the observer.

## Ownership and scope

Notification does not release a claim or permit. Hold callback return independently after it has observed true, then separately hold the acknowledgement completion transaction: public cancelled terminal may have finished, while same-thread admission/deletion, active-run capacity and SQLite ownership remain excluded. Only actual callback return plus existing completion/producer retirement permits reuse. Retained observer clones must survive final server normal join and Quiescent/WeakStore/WeakApplication absence while a real writer reopens and owner advances. Weak absence alone is insufficient evidence of lock release.

Explicit cancel is the only signal domain. Host shutdown, failure drain, lost owner, successor and recovery do not impersonate it. No deadlines, forced thread cancellation, async callback API, cancellation wait API, native chunk port, mutable registry, public diagnostics DTO, capability-version change, storage migration or dependency change is part of this increment. No additional CP/M0/core/milestone or broad release scenario is closed by these tests.

## Selected source exceptions and evidence

Propose only five existing crate exceptions after the preceding phase freezes:

- `crates/b4-runtime/src/application/model.rs`: optional default controlled entry.
- `crates/b4-runtime/src/application/mod.rs`: export new private control module/types.
- `crates/b4-runtime/src/application/registry.rs`: additive controlled invocation and common exact validation.
- `crates/b4-http/src/control.rs`: per-run resource-free signal, observer accessor, sticky commit publication.
- `crates/b4-http/src/worker.rs`: optional native controlled invocation and exact acknowledgement mapping.

Only four existing noncrate support exceptions are selected: `README.md`, `spec/runtime/compatibility.md`, `spec/runtime/requirements.yaml`, and `scripts/check_contracts.py`. Their additions preserve every existing statement/status, all338 historical requirement declarations and their old tests, and the separate unchanged-client8 group. Append a new `native_cooperative_cancellation_requirements` group, validate it separately, print its own count, and reject duplicate IDs across all groups. Never increase or merge into the protected338 count. New checker tests live in new files; old checker tests remain exact. No extra existing support-file exception is selected.

New files: `crates/b4-runtime/src/application/control.rs`, `crates/b4-runtime/tests/native_controlled.rs`, `crates/b4-http/tests/native_cooperative_cancellation.rs`, `crates/b4-http/tests/native_cooperative_cancellation_tcp.rs`, and bounded new support helpers. Existing kernel/runtime-driver/storage/transport/cancellation/lifecycle/claim code, all old tests/examples/native AP corpus/bundle/runner/auditors, Cargo manifests/lock/features, and prior receipts remain exact. Any unexpected extra exception requires source-backed justification before implementation. The preliminary private source baseline must be recaptured against the actual final prior freeze before accepting exceptions.

Meaningful tests must establish observer state around real durable store boundaries, clean acknowledged cancellation, false acknowledgement faults, late success/failure, pre-entry cancellation, fresh policy denial, infrastructure failure/panic/retry exhaustion, stale and successor isolation, abandoned cancellation versus abandoned viewers, separate shutdown, physical callback/completion retention, and retained-clone ownership absence. Use actual authenticated loopback HTTP and real SQLite alongside portable tests. Initial RED must compile through additive stubs and fail on missing notification or accepted acknowledgement behavior, never on permissions/setup. No original client execution or provider is needed.
