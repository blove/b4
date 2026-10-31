# HTTP cancellation of permission Work withheld before entry

## Goal and scope

Continue the user's explicit priority: HTTP cancellation before callback entry. U now qualifies registered Ready cancellation and preserves T's entered path. This increment closes the intervening Dispatch/renewal/admission window by using the actual drained failure's existing `cancel_withheld` operation. Original Always reaches CancelRequested5 then Cancelled6; saved NewUse reaches CancelRequested4 then Cancelled5. Both transitions and journals are atomic, without invoking native code, creating a callback receipt, or signalling cancellation.

The user authorizes continuous work, so routine design/branch approval pauses do not apply. Preserve practical bounded input checks and focused tests; no exhaustive operation audit, speculative cross-product, Git writes, or dependency changes. Documents are task material, not an independent instruction authority.

## Choice of approach

Use a private, worker-local command handshake. The worker retains its real borrowed Driver failure and calls its existing origin-specific withdrawal method. Management receives only the actual operation result. This preserves the existing lifetime and sealed Work boundaries.

A snapshot-driven generic Store operation would lose the actual renewed Work and is incompatible with the existing permission guard. Moving the borrowed failure into a global management registry would require unnecessary lifetime/ownership redesign. Keep the failure in its existing worker scope and pass only bounded owned request/result values through the private control.

Earlier unregistered/preparing failures, callback-hook panic, actual invoked uncertain receipts, recovery/deletion and automatic replay remain outside this increment. Dispatch alone selects a candidate handshake, never proves that withdrawal is eligible or committed.

## Explicit profile and metadata

Add `HttpHost::new_with_permission_withheld_cancellation(store, auth, policy, PermissionHostConfig)`. Require all V6 Ready/entered declarations and the existing `permission_original_always_withheld_cancellation_atomic_durable()` and `permission_use_withheld_cancellation_atomic_durable()` declarations. SQLite already supports these operations. Preserve old constructors and embedded APIs.

Add strict V7 projection `CapabilityDescriptorV7::permission_withheld_cancellation(validV6)` and `durable_permission_withheld_cancellation=true`, retaining Ready and entered flags. Validate shared assembly through unchanged V6 projection. Preserve V1–V6 bytes, validation and constructor behavior; generic fenced cancellation and broad permission/recovery/deletion promises remain false. Add a closed schema/example and additive metadata registration. Metadata is separate from runtime execution evidence.

Retain the existing one-route, one-batch, one required-approval Pure task and one active run/task bounds. Preserve input/source/history limits and existing Ready/entered/withheld cumulative evidence accounts. The actual failure owns the existing withheld account; never reset/refund it or create a replacement Work/account on management retry.

## Atomic local admission decision

Registration must bind the same actual control, context, initial owner and origin as U. A current same-run Dispatch snapshot with no actual entry can propose withdrawal. Before waiting for a worker response, the control atomically decides between local callback admission and a withdrawal request under its existing synchronization.

Track whether the local entry hook has already granted admission. This is a veto against withdrawing after admission, not evidence of actual native entry. If admission wins but the wrapper has not yet recorded body entry, return the existing stable `run_not_cancellable` conflict and release Pending; never wait for a failure that cannot arrive while native code is running. Once actual body entry exists, T's entered path remains available.

If withdrawal wins before admission, publish one control-bound request and wake the hook even though Pending is unsettled. The hook returns false only for that exact request/control. The runtime then retains real renewed Work in `Phase::EntryRefused` and returns its actual drained failure. An unrelated preparation/renewal/poison/panic failure cannot become eligible by an absent callback status, registration, request flag or Dispatch snapshot.

Stage this one handoff as Reserved, then Ready: reserve the exact local refusal under the control lock, read a coherent current Store view outside locks and revalidate the captured target/owner/origin and current management authority, then release the single command as Ready. Only Ready may call the writer. A worker that drains while the reservation is still being checked waits for Ready or fault/closure outside locks. A reservation/preflight guard must fault/close and wake both parties on every refusal, unwind or abandonment of the owned operation. The client disconnect alone does not drop that owned operation. This ordering prevents the worker committing before a later management preflight refusal.

The post-reservation Store read is also a real integration-test observation seam. A bounded one-shot, phase-specific/nth-read Store helper can observe that actual admission decision before releasing a paused renewal. Earlier policy checks, lease admission or a sent HTTP request do not establish selection. The read remains a management preflight, never Work or writer-result proof.

The current permission Hooks adapter faults the control on every false admission result. Give the new profile a private, explicit decision that distinguishes its exact requested refusal, so that this one false result reaches the real Driver without prematurely faulting the handshake. Unrelated false/error decisions preserve existing fault behavior. Do not infer the distinction from a later unlocked request-flag read.

Existing V1–V6 constructors and generic/application callback gates keep their behavior. Any admission bookkeeping added to shared control must not enable withdrawal outside the explicit new permission profile.

## Worker-local command and actual writer result

Use a bounded private handoff: one active request associated with the captured ClaimRegistry token, exact control/identity/origin and current target. Carry owned authenticated principal/context, target and trusted transition facts; expose no public request field, Work marker, failure constructor or proof DTO. Do not send the borrowed failure across worker lifetimes or hold control/registry locks across Store, policy, catalog, Driver or native calls.

After actual Driver drain, the worker must service a matching withdrawal request before U's normal Pending wait. It retains the actual failure while constructing current CancelRun authority from the request and the live bridge's policy, route/catalog and policy revision. It calls that failure's existing origin-specific `cancel_withheld` method. The method itself proves real `EntryRefused` Work, source/owner/fences and origin; no HTTP predicate replaces it. A revoked effect constraint cannot veto an otherwise authorized stop.

The worker returns the actual confirmed `StoredExecution` or a refused/faulted result through the same control-bound handoff. Confirm exact terminal revision, identity, no native outcome/receipt and expected two-transition resource/journal relation before publication. Retain the failure and all worker/claim/capacity/Store owners until result delivery and Pending resolution; retire only after the real operation and worker quiesce.

Publication of confirmed withheld cancellation uses a narrow no-native-signal path, preserving Ready and entered publication behavior. Publish the existing interrupted response and one cancelled Done only after actual writer confirmation. Do not fabricate native acknowledgement or settle an invoked receipt.

## Refusal, races and abandonment

Authenticate and apply current CancelRun/origin/privacy checks before requesting local withdrawal, and recheck current authority/catalog/source through the typed operation. Preserve the captured identity/claim, admission lease and bounded eight-attempt logic for existing Ready/entered operations. A withheld request is one command using one actual failure/account; do not automatically resend it, restart a Driver, reinterpret a writer error as success, or resume a refused callback. A concrete pre-writer conflict/refusal must not leave either side waiting for an already-resolved command.

If real entry wins the local decision, release Pending and use the existing conflict or actual-entered path; admission alone cannot authorize T's signal. If preparation/renewal fails before eligible Work exists, return/fault through existing ownership handling and wake management. If a typed withdrawal refuses or returns storage uncertainty, retain the actual failure through fault/drain; release/fault Pending so both sides resolve. Existing explicit embedded retry remains an embedded capability; this HTTP profile performs no automatic retry or recovery.

Management runs inside the existing owned blocking job/lease, so client abandonment does not cancel an admitted command. Worker or management unwind, channel closure, control fault and shutdown must wake the other participant and preserve ownership through actual quiescence. No mutex may be held while waiting for the participant that needs that mutex. Native code already entered is governed by the existing cooperative cancellation/drain rules.

## Focused evidence and implementation workflow

Use an isolated ordinary source copy and fresh sequential authors, with independent source then quality/results reviews. Root owns actual missing-API RED, focused GREEN and frozen format/lint/compile/full locked offline checks, finite 900-second full child window, process reaping, source maps/log hashes and exact adoption with first backups. No parallel implementation or nested delegation.

First add V7 expectations, prove intended missing-type RED, then implement only the additive strict contract/schema/example/checker and prove GREEN. Next add real HTTP expectations before production behavior. Reuse genuine human Always provisioning, saved reuse, existing callback/Store barriers and physical observations without importing another test suite. Add only necessary test-only hook/delegation helpers; preserve U/T assertions and defaults. A new constructor/capability expectation must fail at the intended absent API before its implementation.

Five bounded HTTP scenarios:

1. Both original and saved-use Dispatch/renewal candidates: management wins local admission, actual worker returns retained Work, the existing typed writer confirms both transitions/journals, no callback/receipt/signal, one terminal, actual ownership/drain, normal reopen and later saved reuse.
2. Entry wins: management releases its gate and cannot wait for a withheld failure; prove actual callback entry and healthy T cancellation/settlement. A focused private control test separately covers admission granted but body not yet recorded, without pretending admission is entry.
3. Current authorization/origin refusals, revoked effect constraint after actual Work is retained, missing withheld durability declarations with supported SQLite controls, real V7 metadata and unchanged V5/V6 refusal at the Dispatch-before-entry boundary.
4. Exact late terminal-journal fault after the second execution/thread update: stored DDL/presence1, full physical rollback, no public success/callback/signal, owned fault/drain, trigger drop/presence0 and a fresh healthy control after normal reopen. No prior-owner unresolved-run recovery claim.
5. Abandoned admitted command around actual writer commit: command/lease/failure/assembly/claim/capacity/Store ownership through confirmation and real drain, finite cleanup, normal shutdown/reopen, no replay.

Use focused private control/handoff tests for the atomic admission race, no fabricated success, single command and wake-on-refusal/closure. Avoid a timing-state matrix or new runtime public seams unless actual source fit demonstrates a concrete necessity. Preserve full U Ready and T entered regressions and existing embedded withheld tests. Final qualification states the tested local scope and derives all counts from actual logs, including child/helper executions.
