# Original UUID Ready cancellation

`cancel_permission_ready_with_normal_inputs` cancels a genuinely persisted
original Always approval with UUID resource IDs before its callback starts. It
uses the existing cancellation authority, current target and resource fences,
complete permission history and catalog checks, whole candidate comparison and
typed atomic SQLite writer. `cancel_permission_ready` retains its Strict limits.

The private NormalReadyCancellation profile is selected only at fresh cancellation
state inception beside its actual operation and origin. It preserves prior
spending and the 8 MiB/65,536-node cumulative account. It cannot change profile
mid-operation. Additional variable work is charged from actual measured records
before copies, comparisons, kernel transitions or writes. Existing Strict and
NormalNewUse behavior and the commit/acknowledgement path remain unchanged.

Ready and Cancelled snapshots are bounded at 4,096 bytes, their existing 96/100
nodes and depth64. Thread and flat journals are bounded at 1,024 bytes with their
existing structure. Resource identities use the core 256-byte rules; turn-history
strings have a 512-byte aggregate with unchanged cardinality. Historical bounds
match the original producer: Acceptance8,192, before4,096, Decision/grant4,096,
scope body2,048/wrapped2,070 and Park/Resume journals1,024 bytes. Existing node,
depth, prompt, compact input, Definition and semantic checks remain. Individual
ceilings do not guarantee simultaneous maxima fit the cumulative account.

This source-bound qualification is limited to the original UUID Always shape
with compact labels, input/prompt and empty metadata. NewUse and legacy Once
cancellation, original callback execution, HTTP integration, Linux and the broad
durable-permission capability remain separate work. Later reuse is covered by a
[separate embedded Driver qualification](../original-reuse/README.md).

See the [design](../../../docs/superpowers/specs/2026-10-20-original-uuid-ready-cancellation-design.md)
and [qualification record](qualification.json).

Three SQLite cases pass: exact kernel Cancelled revision4 and complete persisted
raw mirrors; a real stale-target refusal; and full rollback under a guarded final
journal fault. Each checks callback0 and unchanged permission/grant records, then
proves owned close, owner-advance reopen, canonical records, readiness and removal
before final assertions. The shared Store error is not unique trigger provenance;
the fault qualification is limited to the guarded case paired with normal success.

Formatting, strict all-target lint, workspace compilation and the full local Rust
regression passed on the frozen source. Ignored cases were not executed. This
qualification is macOS arm64 with Rust1.93.1.
