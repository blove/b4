# Normal application identifiers in embedded permission use

The embedded permission flow accepts a UUID application identifier through real
Always approval, cancellation of the original Ready run, database owner reopening,
authorization of a new UUID run and one exact persisted callback completion.
Canonical Acceptance, Decision and permission-use records keep the same identity
as the callback context; application identity is never rewritten or aliased.

Existing normal paths retain nonempty, control-free labels of at most 256 UTF-8
bytes each. Their actor/policy aggregate is bounded at 1,024 bytes, application
ID/revision pair at 512, and InvocationContext at the existing 2,048. The embedded
adapter and actual NormalNewUse claim apply paired checks. Normal Ready
cancellation accepts the same application-pair bound through its actual private
profile. Strict/default paths retain their previous limits.

All record, scope, snapshot, thread, journal, input, node/depth/cardinality and
cumulative account limits remain unchanged. Additional variable work is charged
from actual lengths before scans, copies or comparisons. This adds no public
selector, schema, retry account or callback replay path. Individual ceilings do
not guarantee simultaneous maxima fit the other record/account limits.

Qualification is limited to a legal 36-byte UUID application ID with the existing
compact application revision, tenant/principal/policy, task/effect and Definition;
resource UUIDs, modest nested JSON, one task and empty metadata. HTTP, Linux,
original-human callback execution, mixed/parallel work and the broad durable
permission capability remain open.

Two SQLite cases pass: the exact successful UUID-application completion, and a
different current application refused before callback entry even with an allowing
constraint. The refusal preserves the full independently expected Dispatch3 claim;
it does not roll the claim back or invent a terminal cancellation. Both cases
prove complete typed/raw state and owned close/reopen/removal before final assertions.
The private Driver error remains opaque; this does not classify arbitrary errors.

Formatting, strict all-target lint, workspace compilation, both selected cases
and the full local Rust regression passed on the frozen source. Ignored cases
were not executed. This qualification is macOS arm64 with Rust 1.93.1.
See the [design](../../../docs/superpowers/specs/2026-10-20-normal-application-ids-design.md)
and [qualification record](qualification.json).
