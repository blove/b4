# Owned run cancellation

Status: implemented and reviewed bounded native increment; 391 Rust tests pass.
The user has authorized
continuous development. The frozen lifecycle reference remains unchanged.

## Behavior and scope

Implement bodyless `POST /threads/{id}/cancel` for a locally owned active turn.
After a durable cancellation commit, return 200
`{"status":"interrupted","thread_id":"..."}` and promptly finish the original
wait response with 409 `run_cancelled`, or its stream with
`done.output.cancelled=true`. The worker continues to own its thread claim,
capacity permit and store until its callback and completion work quiesce.
Deletion and another admission remain refused during that interval.

Missing authorized resources return 404; idle, completed, stable approval parks,
prior-owner work without a live local control, and expired captured turns return
409 `no_run_in_flight`. Immutable application/tenant/principal origin is required
even with a permissive host policy. Cancellation never adopts recovery work or
remembers an intent for a future run. A locally owned Accepted execution is
cancellable in the admission/Start gap; cancellation initializes only its first
batch and settles without invoking callbacks or consuming attempts.

Only the existing finite Pure scripted effects are exposed by this HTTP host.
Attachment, arbitrary effect adapters, shutdown deadlines, and actor/cause
diagnostics remain outside this increment. Existing snapshot/journal cancellation
state records durable intent; this is not a claim of complete RFC diagnostics or
release conformance. The schema and dependency lock remain unchanged.

## Identity and atomic persistence

`RunCancellationTarget` captures thread ID and generation, resource revision,
active execution ID, execution revision/owner fence, and turn ID from a coherent
view. A default `Store::cancel_run_fenced(target, facts)` capability returns
UnsupportedOperation for custom stores. Memory and SQLite recompute the kernel
Cancel transition inside their mutex/immediate transaction, compare every fence
and current store owner, increment resource revision and append the journal.
Non-cancellable execution state returns a typed NoRunInFlight error.

The same atomic operation validates related grant and acceptance records and
removes only unconsumed grants for the cancelled execution. Consumed grants and
acceptance receipts survive unchanged; no Deny acceptance is invented and Once
is never refunded. Validation or persistence failure leaves snapshot, resource,
journal and all authority records unchanged. Prior owners cannot cancel through
this capability. The kernel retains actual late success/failure evidence while
suppressing value reduction and later scheduling. Existing failure precedence
continues to allow a real late failure to settle durable Failed.

`EffectOutcome::Cancelled` and exact `KernelCommand::CancelAttempt` acknowledge
an InFlight attempt positively known never to have entered its underlying
callback (or a trusted cooperatively quiesced attempt). This requires committed
cancellation or the existing failure drain, exact owner/task/attempt, keeps attempt history and budget, and
creates no success result or fabricated application failure. The existing
dispatch APIs remain usable; the owned host supplies a gated callback wrapper.

## Local coordination and publication

Replace the claim set with a brief-lock registry. Each claim has a unique token
and an optional Arc run control. Guard destruction removes only its own entry;
the control itself must not retain the claim or capacity guard. After admission,
publish immutable generation/execution/turn/owner identity before exposing ready.
Stable parks and terminal workers have no cancellable target even during their
final claim-release window.

Cancellation captures the original control before the initial resource policy
callback. An owned blocking cancellation worker reloads a token-free coherent
view, renews policy, checks exact origin and the original registry entry, and
commits only that identity. Revision conflicts may reload and retry the same
identity with fresh authority; never retry by thread name or adopt a successor.
Bound retries and report healthy contention if progress prevents a commit.
No capacity permit or replacement thread claim is acquired by cancellation.
The owned worker survives request abandonment and records infrastructure faults
even when its caller disappears.

A per-run invocation gate has Active, CancellationPending, CancelCommitted and
Faulted states. After fresh authorization, cancellation enters Pending before its store
attempts. Callback entry waits for Pending to resolve, then either atomically
marks entry under Active or returns Cancelled under Committed. The invocation
linearization point is that entry mark; an entry admitted before Pending may run
to quiescence. No global registry lock is held over policy, store I/O, or effects.
Healthy rejection restores Active. Infrastructure failure and panic mark the host
unready before releasing waiters into Faulted; callbacks and normal terminals may
not proceed after that decision. Panic-safe cleanup must release a pending gate.

The same control owns bounded viewer publication, the original wait sender and
one terminal decision. Pending blocks competing terminal publication until the
store decision resolves. A committed cancellation publishes its terminal once;
later chunks, approval interrupts and success/failure terminals are suppressed.
Real late failure remains durable but does not produce a second wire terminal.
If ordinary completion commits first, cancellation returns no_run_in_flight and
ordinary publication proceeds. Recovery of already Cancelled work retains its
existing native 200 cancelled wait output.

Worker conflicts are benign only after waiting out Pending and reloading the
same immutable run identity with positively committed cancellation. An attempt
whose wrapper never ran is acknowledged only when its own recorded attempt is
still InFlight. An obtained outcome remains available for bounded completion-only
CAS retry; a retained completion-time conflict also retries only after positive
local committed cancellation and exact identity/current-owner verification.
Without that evidence, the clock conflict remains a fault. Callbacks are never
replayed. Retry exhaustion leaves the unsettled
attempt durable and faults readiness even when cancellation was already sent.
Unauthorized/revision errors alone do
not justify treating an unknown effect as cancelled. Other infrastructure errors
or panics remain host readiness faults, including after prompt cancellation.

## Qualification

Use failing tests for kernel Accepted cancellation, never-entered attempt
acknowledgement, exact fences, budget/history preservation and failure precedence.
Backend tests cover grants/receipts, validation failures, transaction rollback at
grant deletion and journal insertion, prior-owner rejection and actual SQLite
reopen/recovery. A default custom store must fail capability requests healthily.

API and real TCP gates must prove cancellation after Dispatch commit but before
underlying callback entry (zero invocations), cancellation after callback entry
but before exit (prompt terminal with held exclusion/capacity/owner), late policy
across successor admission, fresh authorization denial, repeated cancellation,
request abandonment and infrastructure failure. Include Accepted/Start races,
required-grant resume races, late success/failure, bounded viewer detach, exact
origin rejection and no later chunks/second terminal. Compare selected native
statuses and cancelled payloads with frozen lifecycle records; do not claim full
wire/event/attachment parity from those selected projections.

Fresh workspace formatting, strict Clippy, 391 Rust tests, 50 Node integrity/
projection tests, four Python checker tests and example runs pass. The separate
owned-run-cancellation-verification.json receipt records exact scoped evidence;
M0 and all 56 broad release scenarios remain open.
