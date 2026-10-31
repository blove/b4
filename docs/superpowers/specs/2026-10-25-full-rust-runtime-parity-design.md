# Full Rust runtime and client compatibility design

Status: proposed implementation program, approved in conversation on 2026-10-25. This document does not qualify a release scenario or assert whole-product parity.

## Objective and boundary

Rust will own B4's server and runtime capabilities. The existing TypeScript SDK, UI, and build tooling remain supported clients and authoring tools; this program does not rewrite those packages in Rust. The target is the committed `b4run` tree at `f7d89dbc6588a12f7cb41f13d43cc8defa98b244`, not its uncommitted working-tree edits. The source pin is a comparison and inventory input, not automatic evidence of a passing Rust behavior.

The present Rust release inventory is `b4-ap-core/0`: T-001–T-016 have scoped native qualifications; T-017–T-056 are pending. M0 is open, CP-001–009 are partial, CP-010 is unresolved, M1–M5 are partial, and M6 has not started. Earlier receipts retain their own source maps and narrow claims. The accepted T-016 record and its post-acceptance descriptor are immutable predecessor evidence.

Completion has two nested gates:

1. **Core profile:** the ten core AP operations, their frozen schemas and lifecycles, all 56 acceptance scenarios, M0/CP-001–010, deterministic and process-kill recovery, security, limits, native packaging, and an integrated `b4-ap-core/0` release receipt.
2. **Whole runtime product:** every committed-HEAD server/runtime capability, whether or not a selected TypeScript client currently uses it. This includes agent execution, durable state, provider and tool effects, memory, workspace and sandbox services, deployment/storage modes, client-facing projections, operator and inspection behavior, and runtime evaluation facilities. A separate machine-readable feature inventory defines each required behavior. The TypeScript SDK, UI and build tooling remain external compatible clients and authoring tools. Passing the core gate does not imply this second gate.

No percent-complete claim follows from package counts or existing Rust tests. The TypeScript source has 21 committed packages under `packages/`; package membership is a discovery aid, not a one-to-one Rust crate requirement.

## Baseline and feature catalog

Freeze the exact committed source revision, lockfile and relevant built artifact provenance. Read committed blobs rather than the modified local working tree. Inventory public capabilities from committed package exports, CLI commands, HTTP routes, events, configuration, persistence, deployment artifacts, and documented client flows. Record each capability with a stable ID, owner, behavior statement, input/output boundary, relevant TypeScript source/tests, Rust implementation status, comparison class (`COMPAT`, `NATIVE`, or `CHANGE`), acceptance fixture, platform, and evidence receipt. Mark unknowns explicitly.

Start with a factual catalog and dependency map. The catalog must distinguish these tracks:

- AP core, SSE, approvals, cancellation, security, storage and process lifecycle.
- Filesystem route discovery, generated types, HMR, build and verification artifacts consumed by the Rust runtime. TypeScript authoring/build tools remain in place.
- Agent, workflow, graph and chain execution; model providers, tool calls, retries, planning, context management, skills and subagent behavior.
- Memory and retrieval, workspace and sandbox execution, Postgres and other storage/deployment modes.
- AG-UI and other client projections, original SDK/client interoperability, testing/evaluations and inspection interfaces.
- Native binary/container operation, backup/restore, observability, diagnostics, packaging and migration/cutover.

An item may be excluded from a narrower profile only by a separately approved product decision with a reason and a protected refusal or capability signal. Such an exclusion prevents an unqualified whole-runtime parity claim; the resulting claim must name the narrower scope. An unsupported operation must not return fabricated success. Future TypeScript changes require a new baseline decision and refreshed affected comparisons; they do not silently move this pin.

## Phased execution

### Phase A: catalog and reference characterization

Produce the source-bound capability catalog, committed-HEAD reference build provenance, and a prioritized dependency graph. Refresh the older `8acac534...` comparison corpus only by running independent fixtures against the new pin. Characterize CP-001–009 gaps and the actual AP-only client corpus for CP-010. The previously selected original-client run of 19 methods across nine operations is useful evidence, but leaves AP-004, unsupported caller options, permission-envelope incompatibility and broader client/platform coverage open.

### Phase B: complete the core profile

Qualify T-017–T-056 as bounded scenarios without transferring credit from existing source declarations or scoped receipts. Start with T-017, whose heartbeat and UTF-8/network-split contract has local component tests but no release proof. Follow dependencies: approval/recovery, cancellation, security, storage/operations and final release gates. Close M0 and the RFC definition of done with full current-source checks and one integrated receipt only after every required category passes.

### Phase C: complete runtime capabilities

Add bounded provider/tool, agent/workflow/graph/chain, memory, workspace/sandbox, storage/deployment and client-projection slices. Each slice has its own design, exact contract, security and recovery behavior, native and reference fixtures, platform matrix and receipt. Preserve explicit ownership boundaries; TypeScript clients and build tools may evolve only through reviewed compatibility changes. Model/tool effects need an explicit durable identity and reconciliation policy before any retry can claim safety.

### Phase D: client and migration qualification

Run unchanged committed TypeScript clients against the Rust runtime for the declared operations and error paths. Verify the build outputs those clients consume, deployment topology, process restart, backup/restore and upgrade behavior. Compare meaningful status, error, JSON, SSE, state and causal order; normalize only declared nondeterministic values. Qualify a staged migration and rollback procedure before claiming Rust can replace the TypeScript server for a supported deployment.

## Compatibility rule

`COMPAT` means an executed pinned TypeScript/Rust comparison for named cases and fields. `CHANGE` means a reviewed, tested and user-visible intentional difference; it cannot be counted as a passing compatibility case. `NATIVE` proves an authored Rust requirement only. An unqualified whole-runtime parity claim requires passing evidence for every committed-HEAD server/runtime capability. A separately approved exclusion produces only a named narrower-profile claim. Source inspection, fixtures generated from Rust output, package compilation and a passing client happy path are insufficient as comparison oracles.

The current native stricter parsing, approval grant, token, resume-route, park envelope and authorization choices remain explicit `CHANGE` candidates. The program does not silently weaken security to mimic the predecessor. Any change to one of those decisions requires its own design and client-impact review.

## First bounded slice and controls

Create a separate T-017 design and implementation plan from RFC section 14.4, `FND-C-008`, `HTTP-H-012`, and the existing real-TCP/SQLite acceptance pattern. It must distinguish actual server heartbeat timing from a client/parser test of arbitrary TCP partitions and split UTF-8, specify deterministic finite oracles, and avoid claiming all network scheduling. Source review precedes execution; final current-map checks and independent result review precede metadata promotion. No prior T-001–T-016 proof is reclassified by this slice.

In parallel, the catalog and M0/CP-010 design may advance read-only, but no whole-core or whole-product qualification is published until its own gates pass. Keep Git writes, external service calls, package installation and production deployment out of this initial local planning slice.
