# ADR-0003: Return commit proposals from a pure kernel

Status: accepted native design; durable implementation pending.

The kernel consumes a portable version-1 snapshot, command and explicit facts.
It returns a replacement snapshot and ready task candidates guarded by the
expected revision. The `dispatchable` field is eligibility information. The host
must authorize each candidate, separately propose `Dispatch` with a fresh
attempt, and commit its `InFlight` snapshot before invoking the effect. The
kernel has no ambient clock, randomness, network, SQL or
authorization dependency.

A parallel batch shares one input state. Results are retained per task; joins
reduce in definition order. Conflicting replacements fail. Approval parking waits
for in-flight siblings, full-set resume retains completed siblings, and duplicate
result delivery cannot reduce twice. Cancellation retains the slot while an
effect remains live. Budgets persist across turns.

This supports deterministic tests and separate backend qualification. A returned
plan proves neither atomicity nor durability. The snapshot is internal execution
data, not the AP checkpoint projection. See `../state-machines.md` and
`../storage-contract.md`.
