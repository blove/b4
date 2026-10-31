# 0014: One coordinator for real bounded parallel effects

Status: accepted and implemented within the verified native increment.

The kernel already supports parallel task batches, independent results and ordered
joins, but the HTTP worker executed each callback synchronously before selecting its
sibling. Embedded callers also had to write their own orchestration. Moving that
loop into `b4-runtime` gives both callers one scheduling and ownership boundary.

Use a synchronous single-writer coordinator and scoped native callback threads.
Workers invoke one exact prepared attempt and return an immutable bounded outcome;
they do not commit runtime state. The coordinator prepares, parks and completes
tasks. This preserves approval dispatch's strict renewed full-view authority check
without allowing unrelated sibling results to invalidate it during a policy wait.

Split preparation from callback execution. A preparation failure retains any durable
claim as evidence, never as authority to invoke after refusal. Before a successful
preparation can enter its callback, the driver reloads the original stored turn and
checks that the exact attempt really exists. Only completion may retry after a
revision/clock conflict; a callback never retries because committing its result failed.

Callbacks share finite global capacity and a finite per-execution bound. Queued
outcomes retain permits through completion and postcommit projection, so the coordinator drains them before any
capacity wait. Required parking needs no callback permit. A later required sibling
can park while an earlier ordinary task waits for capacity. Existing durable total
attempt budgets remain authoritative across resume and recovery.

Scoped ownership joins every child before releasing its producer/store/run permit.
Callback or coordinator uncertainty immediately faults uncached publication, then
drains siblings and retains their real outcomes. Receiver and retained evidence
outlive caught coordinator panics. Healthy policy refusals remain typed; a later
sibling fault still escalates readiness. Exact committed cancellation may acknowledge
a proven never-entered attempt, while actual late outcomes retain failure precedence.

At this decision's receipt boundary the HTTP registry contained only finite Pure scripts. Native embedding
callbacks are trusted application code. Providers, dynamic graphs, shutdown deadlines,
automatic recovery scans and capability discovery remain separate work. New native
barrier, transport and actual SIGKILL tests qualify this scope; they do not establish
TypeScript crash parity or the full core release profile. No dependency or SQLite
schema change is required.

Design: `spec/runtime/shared-execution-driver-design.md`.
Plan: `docs/superpowers/plans/2026-10-01-shared-execution-driver.md`.

Verification: `conformance/expected/shared-execution-driver-verification.json`.
