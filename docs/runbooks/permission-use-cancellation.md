# Embedded cancellation after permission-use callback entry

The explicit `drive_permission_use_cancellable_execution` entry uses the native application's cooperative callback method for a genuine NewUse Ready2 execution. Existing direct and original Always drivers keep their behavior. This increment qualifies cancellation after callback entry; it does not enable HTTP or pre-entry cancellation.

The store must opt into `permission_use_cancellation_atomic_durable`. The default refuses; SQLite supplies the typed request and receipt-backed outcome ports. This declaration does not enable broad durable-permission or HTTP lifecycle capabilities.

Use the following ownership sequence:

1. Obtain the genuine Ready2 reuse execution through the normal permission producer. Construct a `NativeCancellationSignal` and retain its observer through the driver call.
2. Start the explicit cancellable driver under owned execution/capacity guards. The application implements `NativeEffect::invoke_controlled`. Use an embedding-owned callback-entry barrier to know the callback has begun; a stored Dispatch3 alone does not prove entry.
3. Capture the current coherent view and exact `RunCancellationTarget`. Call `request_permission_use_cancellation` with current management authority, catalog, context, target, monotonic facts and a bounded evidence account.
4. Only after the actual durable CancelRequested4 result, call `signal.commit_cancellation()`. The signal performs no store operation and grants no authority.
5. The callback retains its work and descendants until they stop. It may then return `ControlledNativeOutcome::Cancelled`. A clear-observer acknowledgement remains uncertain. A real success or failure remains that actual outcome even if cancellation was requested.
6. Keep the driver and its owners alive until it drains. The typed writer verifies the original sealed NewUse claim, immutable authority/Use evidence, exact current cancellation projection and fences, then commits the kernel-derived terminal revision5. A real success retains its result in a cancelled execution; a real known failure follows the kernel and can yield Failed.

The tested pre-commit terminal-journal fault leaves CancelRequested4 and the real receipt/outcome Work retained in the returned failure. An unknown storage error retains Work but leaves the durable outcome unconfirmed; it does not prove that the transaction rolled back. Inspect `callback_status`, `settled_outcome` and `usage` while retaining that owner. An explicit `retry_settlement` retries storage with the same Work and cumulative account; it never calls the callback. Successful settlement makes repeated retry refuse. Dropping failure does not settle or replay it.

Management refusal or a stale target leaves Dispatch3 unchanged. Signal intent without a valid stored request cannot manufacture a cancellation terminal. The tested pre-commit request-journal fault rolls back the atomic snapshot/thread/journal change. An unconfirmed store outcome does not prove rollback and does not justify publishing a signal or replaying a callback. Cancellation leaves the saved permission and original source evidence immutable; a later matching run can reuse it after normal close/reopen.

Original Always, Ready/pre-entry scheduling races, HTTP cancellation, recovery, deletion and general lifecycle support remain separate work. See [the scoped qualification](../../conformance/permissions/use-cancellation/README.md) for executed evidence; public metadata checks alone do not execute this path.
