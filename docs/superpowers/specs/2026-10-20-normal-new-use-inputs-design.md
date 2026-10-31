# Normal NewUse run IDs and bounded call inputs

Status: adopted local implementation, verified by focused tests and the full workspace regression. Working persistence and bounded input checks take priority over exhaustive operation auditing.

## Goal and legal scope

Make the embedded single-task NewUse Driver accept normal UUID current execution/turn/attempt IDs and small structured call JSON. Preserve actual permission provenance, authority/gates, owner/resource fences, consuming callback, complete candidate comparison and atomic typed SQLite settlement. Keep one task/batch, Ready revision2, first Dispatch, unchanged owner and explicit storage-only retry.

Inherited permission identities are a separate dependency. The genuine fixture first persists Always, then cancels that original Ready execution before closing owner1 and admitting owner2's NewUse. That cancellation still requires the original short IDs/labels and empty metadata. Generic raw cancellation refuses permission rows. Therefore this increment does not change the original thread/creator IDs, Definition/task/effect/application labels, actor/policy scope or metadata. It does not qualify all-UUID threads or normal arbitrary applications. Original human renewal/settlement or a separately legal setup path is needed for those later. No direct SQL fabrication or unsupported cancellation may bypass this dependency.

## Recommendation and alternatives

Use a private NormalNewUse profile through the existing dispatch session graph. Select it at fresh entry before R1, retain it immutably with the actual session, and require the classified evidence to be NewUse before preparing a write. Give shared resource/measurement/serialization helpers narrowly paired bounds. Preserve existing Human/Once/default predicates and all semantic checks. This minimizes duplicate protocol code.

A sibling NewUse capture/preparation/writer could preserve old helper bodies literally, but duplicates source/transaction plumbing. Defer that alternative unless the shared seam is impractical. Global numeric replacement would also change legacy behavior and is outside scope.

## Finite limits and protocol

The public Strict producer rejects current execution IDs20 and turn IDs13; other shared limits include Snapshot960, input31, thread306 and journal160 bytes. NewUse source/renewal/outcome readers already permit opaque IDs256, records65536 and journals1024. Changing only the first guard cannot work.

NormalNewUse permits current execution/turn/attempt IDs through existing nonempty UTF-8/control-character rules and MAX_ID_BYTES256, with current turn-history aggregate512 bytes and unchanged history cardinality. Keep inherited context aggregate33/task7/effect14 and existing Definition/application/context structural limits. Bound snapshot and input canonical aggregates at65536B/16384nodes/depth64; thread serialization at1024B with its existing structure and empty metadata; journal1024B with its existing flat structure. These limits accommodate longer active execution IDs and structured input while retaining source envelopes/cardinalities. They do not promise every simultaneous maximum fits the enclosing record or cumulative account.

Retain current cumulative ceilings initially: Dispatch16MiB/65536nodes, Driver16MiB/131072nodes/128coarse operations and existing renewal/callback/outcome accounts. Use actual measurements and existing charges before clones/comparisons/writes. Prepay any newly introduced handoff work before durable writes; introduce no fallible postacknowledgement projection. Exhaustion stays sticky. If a genuine selected case reaches a cumulative ceiling, retain that evidence and review a separate finite functional-purpose allowance before changing it; never silently raise old accounts or restart exhaustive operation auditing.

Profile selection is trusted and private, never a client field, metadata marker or authority inference. The public producer retains Strict; the Driver calls a private NormalNewUse entry over shared implementation. Session admission binds profile with existing origin/freshness; foreign/re-entered accounts cannot switch it. Reject classified Human/Legacy before mutation. Actual Work still owns immutable resource/input/result, and the typed SQLite port/raw guard are unchanged.

## Evidence

First use a UUID only for current execution, retaining genuine original approval setup. Require legal Ready2, existing pre-Dispatch refusal/callback0 and full unchanged storage, healthy retirement/reopen/removal, then fail only at final expected persisted-result assertion. Make that same case green.

Then use UUID current execution/turn/attempt together and small nested JSON variants including escaped Unicode, null and numbers. Verify exact invocation/context, callback1, kernel completion, typed writer1/raw0, unchanged authority rows and durable reopening. Add finite quota/limit refusals and a provenance/full-plan substitution under that shape. Retain existing12Driver/7controls/8outcome compatibility, strict legacy behavior and normal workspace regression. Report only executed capacity and preserve all broader capability/HTTP/legacy/Linux/audit limits.

## Actual qualification scope

The genuine UUID-only regression first refused before Dispatch and then passed unchanged. Current execution/turn/attempt UUIDs with modest nested JSON persist the exact kernel completion after one callback; normal-shaped Definition substitution retains the actual settled outcome and refuses the candidate writer. Existing pure Driver account controls cover byte/depth refusal and sticky exhaustion separately.

The planned end-to-end invalid-input cases were superseded. Selected 33,000- and 11,000-character values failed genuine NewUse authorization before Driver entry. PermissionUse encodes a full before_snapshot within a 16,384-byte/depth64 record, which is tighter than the new dispatch envelope. An unexecuted depth proposal would also fail this record envelope. No fabricated approval, budget increase or input sweep was used. End-to-end oversized Driver input, exact quota boundaries and every simultaneous maximum remain unqualified. Conservative Driver string escaping can refuse inputs before the nominal encoded-value limit.

See the [current qualification](../../../conformance/permissions/normal-inputs/README.md) for actual checks and preserved limits.
