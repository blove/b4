# ADR-0004: Block uncertain effects unless replay is qualified

Status: accepted native design; process recovery implementation pending.

After an unfinished attempt, whether an external call occurred is unknown.
Pure and declared externally idempotent effects may retry using the original
logical effect identity and explicit new attempt identity. Reconcilable and
unsafe outcomes block until a future runtime supplies a verified outcome.
Completed results and pending interrupts remain intact.

Restoration also requires supported snapshot format and exact execution revision.
The latest handler is not a substitute for an unavailable recorded definition.
Kernel recovery tests verify proposals only; they do not establish actual external
idempotence, provider reconciliation or process-kill durability. No exactly-once
claim for arbitrary external effects follows. See `../storage-contract.md`.
