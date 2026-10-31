# First Rust foundation

This is the historical first-increment design. The [README](../../README.md)
describes the current workspace and subsequent runtime/HTTP increments.

The supplied B4-RUST-RUNTIME-001 draft is the architectural input. This first increment implements a subset of M0/M1, not the core-profile release. The repository was empty when work began.

Build three private crates: `b4-contract` owns portable typed identities, versioned execution snapshots, strict AP DTOs and SSE framing; `b4-kernel` owns pure transitions and ordered state reduction; `b4-testkit` drives deterministic scripted effects against that kernel. No runtime, HTTP server, SQLite durability, provider, or production authentication is claimed in this increment.

The kernel receives all identities, timestamps, attempt results and recovery facts from its caller. A transition returns a replacement snapshot and ready task candidates as a proposed commit plan. The `dispatchable` list supplies eligibility information; a host must separately authorize each candidate, propose `Dispatch` with a fresh attempt identity, and commit its `InFlight` snapshot before invoking the effect. The future store compares the expected revision for each plan. This increment models that boundary; it does not implement transactions.

An execution contains sequential batches of parallel tasks. Each batch shares one input-state snapshot. Results remain recorded per task, and patches reduce at the join in definition order. Parallel replacement of the same field fails; ordered append preserves definition order. Approvals settle the turn only when no sibling is still in flight. Resume requires the entire pending set, the recorded route, and a new turn identity. Grant validation belongs to the future authorized runtime; the kernel accepts already validated decisions.

Cancellation targets a specific turn and owner generation, prevents further dispatch, and retains the slot while an effect remains in flight. Recovery preserves completed tasks and pending interrupts. Pure and declared externally idempotent effects may retry with their original effect identity; unsafe or reconcilable uncertain outcomes block for operator/runtime reconciliation. Unavailable definition revisions and unsupported snapshot formats fail closed.

Tests cover serialization, direct SSE payloads, deterministic joins, complete-set resume, duplicate results, stale cancellation, immutable budgets and safe uncertain outcomes. A native test command runs reviewed fixture expectations and prints a receipt explicitly scoped to the foundation. Pinned reference source observations are recorded separately from runtime observations; TypeScript compatibility remains unverified until an independent execution corpus exists.
