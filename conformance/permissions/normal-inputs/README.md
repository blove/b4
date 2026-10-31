# Current-run UUIDs and bounded structured inputs

The embedded `drive_permission_use_execution` path accepts UUID current execution,
turn and attempt IDs and modest nested JSON for one genuine NewUse task. Its
private producer selects immutable NormalNewUse input limits at fresh dispatch
entry. Actual permission evidence, current authority and constraint checks,
resource fences, full candidate comparison and typed SQLite settlement remain
mandatory. The public Strict producer retains its earlier behavior.

Current IDs use the existing 256-byte opaque identity rules; turn-history strings
have a 512-byte aggregate cap with unchanged cardinality. Normal Ready/Dispatch
snapshots and input operands are bounded at 65,536 encoded bytes, 16,384 nodes and
depth64; threads at 1,024 bytes with the existing fixed structure and empty
metadata; Dispatch journal entries at 1,024 bytes with their existing flat shape.
All cumulative accounts remain unchanged. These are ceilings, not a guarantee
that every combination fits: PermissionUse also embeds a before_snapshot within
a 16,384-byte/depth64 record, and the Driver applies conservative string escaping
bounds before serialization.

Three genuine SQLite cases pass: UUID execution alone; all current-run UUIDs with
nested objects/arrays, decoded Unicode escapes, null and numbers; and a
kernel-valid Definition substitution that is refused after callback while the
actual settled outcome is retained. Successful paths persist the exact kernel
completion with callback1, typed completion1 and raw fallback0. Each case closes
its owners and verifies stored state after reopening.

Seven existing pure Driver validation/account controls cover byte/depth refusal
and sticky exhaustion separately. Selected 33,000- and 11,000-character end-to-end
byte cases hit the genuine approval envelope before Driver entry; they receive
no Driver refusal or retirement qualification. The depth proposal was not run
because the same record envelope preempts it. No input sweep or budget increase
was made. End-to-end oversized Driver inputs, exact quotas and simultaneous
maxima remain unqualified.

Full workspace regression, strict all-target lint, compilation and formatting passed on the final frozen source. Ignored tests were not executed. The tested source is adopted in the main workspace.
The original Always producer has a separate [UUID resource-ID extension](../original-ids/README.md).
Reuse of those original UUID permission records by NewUse remains pending.
Definition/task/effect/application/actor/policy labels and metadata retain their
earlier limits. HTTP integration,
human/legacy settlement, mixed/parallel tasks and the broad durable-permission
capability remain pending. This is local macOS arm64 qualification with Rust
1.93.1; no additional Linux or exhaustive-operation claim is made.

See the [qualification record](qualification.json) and
[design](../../../docs/superpowers/specs/2026-10-20-normal-new-use-inputs-design.md).
