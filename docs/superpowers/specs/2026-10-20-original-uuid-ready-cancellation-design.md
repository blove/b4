# Original UUID Ready Cancellation

## Purpose and prerequisite

Support cancellation before callback for the genuine UUID original Always approval
adopted in `conformance/permissions/original-ids`. The user prioritizes working
persistence, focused tests and bounded inputs, and authorizes continuous work.
Keep the existing Strict entry/defaults and all current NewUse/Driver/outcome
behavior. Later reuse of the original UUID permission is separate work.

## Approach

First exercise existing `cancel_permission_ready` from a genuine UUID Ready3.
If it succeeds, qualify that behavior without adding another mode. If it refuses
before writing, retain the actual error and complete unchanged storage, then add
an explicit `cancel_permission_ready_with_normal_inputs` using the same protocol.
This limits compatibility changes; globally broadening Strict would affect
unrelated behavior, while bypassing the typed cancellation protocol would discard
its authority and atomicity checks.

The new entry, if needed, takes the same store, cancellation authority, catalog,
request and cumulative account as the existing entry. A private immutable
NormalReadyCancellation profile is selected only at fresh ReadyCancellation state
inception beside its actual origin/kind. No public selector, reset, refund or
mid-operation mode change. Retain the standard 8 MiB/65,536-node account and all
existing fresh-state guards; any prior spending remains cumulative. Existing
Strict and NormalNewUse inception and behavior remain unchanged.

## Paired finite bounds

Only the UUID resource shape already produced is qualified: same compact task,
effect, Definition, application/context labels, input/prompt and empty metadata.
Normal cancellation uses 4,096-byte Ready/Cancelled snapshots with existing
96/100 node ceilings and depth64; unchanged compact input checks; 1,024-byte
thread/flat journal checks with existing nodes/depth. Identity fields use core
256-byte limits, turn-history aggregate512 with unchanged cardinality.

Its historical graph matches the original producer: Acceptance8192, before4096,
Decision4096, grant4096, scope body2048 and wrapped scope2070, Park/Resume
journals1024; preserve every prior structural/node/depth/semantic constraint and
prompt128/16. Pair these bounds through actual R1/C1/R2 resource/history capture,
current-scope comparison, kernel Cancel candidate, writer revalidation and output
checks. Do not pair only the first read while leaving later operands Strict.
A separate internal profile table may keep this cohesive. Existing standalone
populated-reader and unrelated dispatch/use paths stay Strict where currently so.

Preserve actual authority calls, current target/fences, complete historical
relations, catalog/constraint checks, immutable origin/phase/loan checks, complete
candidate equality and the same typed atomic SQLite writer. Pay additional
normal variable work from retained actual canonical measurements before copies,
comparisons, kernel work or writes; keep Strict fixed charges unchanged. This is
a bounded functional change, not an exhaustive Rust/std operation audit. Preserve
existing cumulative limits initially. A genuine quota refusal is evidence for a
separate finite purpose review, not permission to sweep caps or reset counters.
No new fallible work after commit/acknowledgement, SQL/schema/raw fallback,
callback execution or durable-capability enablement.

## Evidence and error handling

The fixture genuinely admits/Starts/Parks, obtains the real pending token, and
uses the adopted normal Always producer to persist exact Ready3 with linked A/D
and consumed grant. No fabricated permission rows. Construct actual
RunCancellationTarget from this view and independent kernel Cancel facts104;
require expected Cancelled revision4, exact returned/stored snapshot and thread
resource+1, journal prefix+one exact Cancel entry, unchanged permission/grant
records and callback0. Preserve complete raw mirror checks.

On an actual Strict refusal require the observed error and unchanged full state;
a setup refusal or an error with storage changes does not count as that gap.
Observe actual authority/read/terminal counters without assuming an early refusal
reached later phases. The unchanged success oracle switches only its call target
if a new entry is needed. Retire every result/account/authority/catalog/application
and Store alias, prove sole-owner readiness/close, reopen with owner advance,
verify full typed/raw storage, close and remove before the final success assertion.
Reopened core storage and canonical A/D observations do not confer current authority.

After genuine success, add one stale-target or authority refusal and one guarded
late-journal fault proving full rollback through reopen. These preserve the
already-consumed grant and existing A/D; they do not remove permission evidence.
Document shared backend error mapping honestly: a guarded fault and paired
unfaulted success need not prove unique dynamic trigger provenance or classify
arbitrary storage failures.

## Verification and delivery

Use an ordinary isolated source copy; Git is read-only. Test first, static compile,
format and strict lint; independent source review precedes native execution.
After scoped success/controls, run the full normal workspace regression including
already-authorized loopback tests, review actual results, adopt only reviewed
paths, verify exact copied code and run public contract checks. Keep historical
qualifications bound to their original heads. Continue later UUID permission
reuse separately; no HTTP, Linux, mixed/parallel or broad capability claim.
