# HTTP Permission Cancellation After Callback Entry

## Goal and bounded choice

Connect the persisted cancellation flow to a working opt-in HTTP path for both original Always and later NewUse callbacks that have actually entered. The user authorizes continuous development, focused tests and bounded checks. An optional next-priority question remains unanswered; proceed with the stated recommended HTTP direction while allowing later steering.

Considered approaches: route permission targets through generic Store cancellation; qualify every preparing/pre-entry/entered race together; or deliver an entered-callback vertical slice using existing qualified O/P runtime operations and real local entry evidence. Choose the third. Generic Store continues refusing permission targets. Ready and unentered InFlight cancellation remain explicitly unsupported in this increment; their later integration must solve actual retained ownership rather than infer entry from Dispatch or Hooktrue. Broad HTTP cancellation is not declared complete here.

## Explicit host/profile

Add `HttpHost::new_with_permission_cancellation` using existing PermissionHostConfig bounds: one native route, one batch, one required Pure task, one active run/task and existing finite input/outcome limits. Require existing original/reuse durability plus both entered cancellation Store capabilities. Old constructors and HTTP V1–V4 remain unchanged.

Publish a strict V5 descriptor derived from valid V4 with a new narrowly named `durable_permission_entered_cancellation` feature. Preserve generic `fenced_cancellation=false`, broad Always/recovery/deletion flags false, and all prior limits/readiness/control validation. This flag describes entered permission callbacks only. Add its closed schema/fixture and metadata registration; metadata is not execution evidence. V1–V4 validators, wire fields and fixtures remain frozen.

## Genuine entry proof

A local admission hook returning true is not actual native callback entry. Install a private NativeEffect wrapper as the actual catalog-selected callback in this opt-in host. Only the wrapper's invoke_controlled body records entry, then delegates the real callback/outcome without synthesizing acknowledgement. NativeEffect's default controlled method already delegates invoke unchanged, so ordinary callbacks retain their behavior; a later actual Success/Failure remains its real outcome even after cancellation.

Introduce small trusted assembly seams `NativeApplication::map_native_callbacks` and `PermissionCatalog::map_native_callbacks`, borrowing the original assembly and returning a mapped copy. They may change callback Arcs only. Preserve complete identity, current/historical Definitions, output kinds, validators and tool/constraint metadata and shared constraint Arcs; old assemblies remain unchanged. Both use `map_native_callbacks<F>(&self, wrap: F) -> Self` with `F: FnMut(Arc<dyn NativeEffect>) -> Arc<dyn NativeEffect>`. These helpers confer no execution, Store or management authority. Follow existing finite application/catalog bounds; no operation audit or global input cap changes.

The bridge registers a scoped private tracker binding the actual RunControl to the typed original/NewUse execution before its Driver call, and removes that registration after the scoped Driver drains. Tracker storage is bounded by the single active run. Use weak control links to avoid ownership cycles. The wrapper records exact run/thread/generation/turn/owner plus actual task/effect/attempt and invocation context, tied to that live registration. Release tracker locks before native code, Store or policy calls. There is no public entry-mark API. Ordinary untracked Once callbacks delegate unchanged and gain no permission cancellation proof.

Private bridge execute methods can accept Arc<RunControl> references, matching the existing worker owner. New mode runs the existing cancellable Drivers with that control's observer; old modes retain direct Drivers. The wrapper never marks entry from prepare, claim, hook admission or a public snapshot.

## HTTP management and publication

Keep existing authentication, policy/origin privacy, captured ClaimRegistry token, exact held RunIdentity, host lease, Pending/reject/commit gate and bounded revision-conflict retry. For the opt-in permission host, require same-run actual entered proof and its task/attempt linkage before Pending. Ready/unentered requests return a stable conflict such as `run_not_cancellable`, without gate, signal or durable mutation. Old permission modes retain their existing unsupported response. Deletion remains unsupported.

A permission bridge dispatches to the existing typed original or NewUse entered management operation, using its actual catalog and a current ApprovalContext. Dedicated PermissionCancellationAuthority must authorize Operation::CancelRun against the complete current captured resource/target and recorded route; a RunStream authority cannot substitute. Do not evaluate the effect ToolConstraint for stopping. Recheck current catalog/source/fences through existing runtime operations. No generic Store fallback and no source classification from an untrusted request field.

Only a confirmed durable CancelRequested5 (original) or4 (NewUse) permits Pending.commit, local signal and the existing interrupted response/one cancelled Done publication. Late output stays suppressed. The actual callback remains owned until it returns; only its real Ack/Success/Failure may settle through the existing controlled writers. Publication, claim/capacity release and Store retirement stay separate. Revision conflicts may retry only within the existing finite loop and captured identity; a successor must never be targeted. Other storage outcomes remain unconfirmed, fault the owned host/control as existing behavior requires, and cause no automatic callback replay or false signal/ack.

Client abandonment must not drop an admitted management operation, actual callback, lease or Store ownership early. A returned uncertain receipt remains unresolved; this increment adds neither recovery nor a public receipt-storage retry API. Existing fault/drain behavior and no-replay rules apply.

## Focused evidence

Core tests prove mapped current/historical native routing preserves identity, definitions, validation and output handling, and V5 strict round-trip/mechanism rejection without changing V1–V4. HTTP tests use real SQLite, real human Always HTTP acceptance, genuine saved reuse and actual loopback requests/streams with finite body barriers. Cover seven distinct requirements without a cross-product matrix:

1. Original and NewUse actual entered Ack cancellation, one transport terminal, no early capacity/owner release, normal reopen and preserved later permission reuse.
2. Actual late Success and Failure after a confirmed request persist their true outcomes.
3. Current policy/origin refusal leaves durable state and signal unchanged; a revoked effect constraint cannot veto an authorized stop.
4. Real Dispatch without callback entry rejects without mutation; release to actual entry then a healthy same-run cancellation. Old V1–V4 profile behavior remains unchanged.
5. Captured old cancellation cannot retarget a successor after a policy pause.
6. Exact late request-journal fault with stored guard/presence/full rollback/removal and healthy control; no signal or fake durable acknowledgement from a Store error.
7. Abandoned HTTP cancellation still retains the operation/callback/Store through actual commit and quiescence, with normal shutdown/reopen.

Use existing typed/kernel/physical SQLite observations where they establish the new boundary, finite waits/channels/servers and normal owner retirement. Existing runtime R/S tests already prove returned explicit EntryRefused withdrawal; do not re-audit them. Root owns actual RED/GREEN, format, strict lint, compile/full offline regression on frozen source; independent source→quality/results then exact adoption. Local macOS arm64 qualification only, no maximum-size/cross-platform, recovery/deletion, preparing/pre-entry retirement, broad permission lifecycle or core release claim.

## Evidence-based entered-management allowance amendment

A real HTTP NewUse cancellation following original cancellation and late Success exhausted the standard 8 MiB evidence account: a 16,512-byte/one-node debit followed 8,386,418 charged bytes and 38,535 nodes. The next byte total exceeded 8,388,608 while node work remained below 65,536. This was a cumulative validation allowance refusal, not a source-record or input limit. Temporary static diagnostics were removed with exact original file hashes restored.

Add a fresh public `PermissionEvidenceBudget::for_entered_cancellation()` account with a distinct fixed 24 MiB byte ceiling and the unchanged 65,536-node ceiling. Wire only the opt-in HTTP typed cancellation bridge. Keep all old constructors, numeric purpose guards, read ledgers and input/record/history/physical limits unchanged; the distinct ceiling must not collide with another purpose. The account is non-cloneable, cumulative and sticky on exhaustion, with no reset/refund or authority. Each bounded typed management attempt owns a fresh account, as before; the existing eight-attempt conflict bound remains. The already failing late-result HTTP scenario must verify sufficiency before qualification. This amendment does not qualify maximum-sized inputs or change recovery/replay behavior.
