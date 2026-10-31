# Original Always approval with UUID resource IDs

`accept_permission_always_with_normal_inputs` accepts an original Always approval
with UUID thread, execution, initial-turn and approved-turn IDs. It uses the same
pending token, current authority, catalog, constraints, resource fences, complete
candidate comparison and typed atomic SQLite writer as the existing producer.
The original `accept_permission_human` and Once paths retain their limits.

The explicit entry constructs a private profile on a fresh account for one
invocation. Its cumulative allowance remains 8 MiB and 65,536 nodes. Byte ceilings
are 4,096 for before/resume snapshots and grants, 8,192 for Acceptance, 4,096 for
Decision, 2,048 for a scope body, and 1,024 for thread and journal records. Existing
node, depth, cardinality, prompt, Definition and semantic checks remain. The
scope-hash wrapper adds 22 bytes to the body. The writer's 6,500-byte aggregate
output limit also remains; individual ceilings do not guarantee simultaneous
maxima fit.

The profile covers preparation, prospective graph and record checks, writer work
and the precommit capacity certificate. Extra reservations use actual measured
records before copies, comparisons and writes. The certificate describes prepaid
capacity; it does not establish that a populated reader ran after commit. The
SQLite writer commits and returns through its existing completion path. The
standalone populated permission reader retains its Strict limits.

Qualification covers compact application/context/Definition/task/effect labels,
compact input and prompt, empty metadata and four distinct UUID resource IDs.
A separate [original UUID Ready cancellation extension](../original-cancel/README.md)
now handles cancellation before callback. Reuse of these original UUID records
by a later NewUse, original human callback execution, HTTP integration and the
broad durable-permission capability remain separate work. See the [design](../../../docs/superpowers/specs/2026-10-20-original-approval-resource-ids-design.md).

Three SQLite cases pass: exact kernel Resume to Ready revision3 with persisted
Acceptance/Decision and a consumed grant; refusal of a genuine pre-Park stale
fence; and complete rollback under a guarded late-journal fault. Each checks
callback0, typed storage and all nine raw groups, then closes and reopens the
database and removes the fixture before its final assertion. The fault's shared
Store error does not provide unique journal-entry provenance; the qualification
is limited to the guarded injected case paired with the unfaulted success.

See the [qualification record](qualification.json) for the frozen source and checks.

Formatting, strict all-target lint, workspace compilation and the full local Rust
regression passed on the frozen candidate. Ignored cases were not executed. The
platform qualified here is macOS arm64 with Rust 1.93.1.
