# Cancel a permission-use callback withheld before entry

This embedded operation is for a reusable-permission run whose Driver claimed and renewed the invocation, but whose local `enter_callback` hook explicitly returned false. Keep the returned `PermissionUseDriverFailure` alive: it uniquely retains the uninvoked Work after the scoped Driver has drained. Its `callback_status() == None` alone is not proof of eligibility.

Call the failure's `cancel_withheld` method with current cancellation authority, current catalog and a `PermissionUseWithheldCancellationRequest` containing the current approval context, exact cancellation target and trusted transition facts. The method uses the actual Work's Store and claim; the caller supplies no replacement claim or callback outcome. For an embedding that already holds the returned failure and an exact current target, the call has this shape:

```rust
let cancelled = failure.cancel_withheld(
    &cancellation_authority,
    &current_catalog,
    &PermissionUseWithheldCancellationRequest {
        context: &current_context,
        target: &current_target,
        facts: TransitionFacts { now_ms },
    },
)?;
```

Current identity, catalog, policy, source evidence and management authority are rechecked. An effect constraint revoked after the claim does not prevent an authorized stop.

On confirmed success the operation atomically persists Kernel CancelRequested4 and Cancelled5, retaining both journals and advancing the thread resource twice. The callback was never invoked, so this disposition does not claim a native cancellation acknowledgement. The saved permission and immutable Use/approval evidence remain available for a later run. The owned failure is retired and repeated cancellation refuses.

On refusal retain the same failure. A storage error leaves durable state unconfirmed. An explicit later call uses that same owned Work and cumulative account, rechecks exact fences/source and performs only storage work. It never invokes the callback. A tested precommit rollback is evidence for that injected failure only; an unknown commit outcome may make the old target stale and a later call may refuse. Do not infer rollback, create a new driver or replay the callback. Dropping the failure performs no settlement.

This path accepts only explicit local entry refusal for NewUse Work. It does not cancel actual native-entered/uncertain receipts, a callback-entry hook panic, preparation failures, original Always Work, or a running driver. For a callback still executing after confirmed entry, the embedding can use the existing entered-cancellation path and retain its actual eventual receipt. An already-returned uncertain receipt remains unresolved: a cancellation request alone cannot settle it. Preserve unresolved owners for unqualified states. This operation adds no HTTP cancellation capability or recovery/deletion support.

Four focused Rust tests pass on local macOS arm64 with Rust 1.93.1, including the same-owner late-fault retry and normal reopened reuse. The [local qualification](../../conformance/permissions/use-withheld-cancellation/README.md) records the exact checks and scope. This does not qualify arbitrary maximum-size input combinations or other platforms. The private cumulative allowance remains finite and unchanged across retries; existing source/input/history limits apply.
