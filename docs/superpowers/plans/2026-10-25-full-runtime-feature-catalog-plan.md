# Full Rust runtime feature catalog implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Produce a source-bound, reviewable catalog of every committed-HEAD B4 server/runtime capability and its current Rust/evidence status, without granting parity credit.

**Architecture:** Read the committed `b4run` tree at `f7d89dbc6588a12f7cb41f13d43cc8defa98b244` through immutable Git blob reads, excluding working-tree edits. Inventory exports, HTTP/CLI surfaces, configuration, persistence, deployment, client projections, and observable failure behavior. Map each feature to the current Rust implementation and receipt or mark it unknown/missing. Preserve independent source and quality review before publishing the catalog.

**Tech Stack:** Pinned Python 3.12.12 `-B`, local Git read-only blob commands, YAML/JSON, the current Rust requirements and existing native/release receipts. No network, package installation, production execution, or Git writes.

**Approved design:** `docs/superpowers/specs/2026-10-25-full-rust-runtime-parity-design.md`; second-round independent review `.b4-work/full-rust-rewrite-scope/spec-review-r2.md`. Current factual seed: `.b4-work/full-rust-rewrite-scope/source-inventory.json`.

## Task 0: freeze inputs

- [ ] Verify the exact committed TypeScript HEAD and capture a content digest for `pnpm-lock.yaml`, root `package.json`, all 21 package manifests, and the committed docs/source tree list. Record the dirty checkout status only as an exclusion warning; never read modified working-tree bytes as the oracle.
- [ ] For each relevant committed-HEAD built artifact, record its source/lock/toolchain provenance and availability. If it cannot be independently built in this slice, mark its build and dependent comparison fixtures pending; do not imply a valid current-HEAD capture from source presence.
- [ ] Inventory the current Rust public source map after T-016 plan closure and the approved whole-product design. Keep the T-016 post-acceptance descriptor immutable. Record the current requirements/RFC/compatibility hashes and their declared statuses.
- [ ] Save these input bindings privately before authoring any public catalog. Fail if the expected TypeScript commit is unavailable.

## Task 1: enumerate committed capabilities

- [ ] Extract public package exports and documented runtime interfaces from all 21 committed package manifests and source entrypoints, plus root CLI commands and generated deployment artifacts. Assign stable capability IDs by behavior, not package filename. Reconcile every package and root command/artifact to at least one catalog row or a reasoned non-runtime disposition.
- [ ] Enumerate AP, AG-UI and other HTTP routes/events; authoring/build outputs; agent/workflow/graph/chain execution; provider/tool/retry behavior; memory, workspace/sandbox, storage, evaluation/inspection, lifecycle/security and operational modes. Include capabilities not currently exercised by selected clients.
- [ ] For every item, assign one surface role: Rust server/runtime capability; TypeScript client or authoring/build input with a Rust consumption/compatibility contract; or non-runtime surface with a reason. A committed server/runtime capability cannot be omitted from whole-product parity without a separately approved narrower target, documented refusal/capability signal, and narrowed claim.
- [ ] For every item, record committed source blob paths/hash, public behavior, input/output and failure boundary, package owner, dependencies, platform and relevant committed TypeScript tests/docs. Mark uncertainty and uncharacterized behavior explicitly. Reject duplicates and orphan public surfaces.

## Task 2: map Rust and evidence

- [ ] Bind each catalog row to current Rust crate/source, requirement IDs, tests and receipt hashes where evidence exists. Distinguish implementation status, comparison status and release status as separate typed fields; none is inferred from another. Every row records `COMPAT`, `NATIVE`, `CHANGE` or `uncharacterized`, an acceptance fixture identifier or `none`, a receipt identifier/hash or `none`, platform, and evidence source pin/hash where present. `COMPAT` requires an executed pinned TypeScript/Rust comparison; `CHANGE` requires a reviewed, tested, user-visible difference and gives no compatibility credit. Keep existing stricter parsing, grant, token, resume-route, park-envelope and authorization decisions as explicit `CHANGE` candidates until adjudicated.
- [ ] Record the T-001–T-016 scoped native outcomes, T-017–T-056 pending state, CP-001–009 partial, CP-010 unresolved, and M0–M6 statuses without changing predecessor receipts. List the known original-client 19-method/nine-operation result as bounded macOS evidence, explicitly retaining missing AP-004, unsupported caller options, incompatible permission-envelope handling, and broader client/platform coverage. Define the actual AP-only CP-010 corpus still required.
- [ ] Build and prioritize a dependency graph in the catalog before review: core/M0/CP-010, agent/provider execution, memory/workspace/storage, client projections, operations and migration. State which unknowns or missing builds block each group.
- [ ] Produce `spec/runtime/full-runtime-feature-inventory.yaml` with a schema/version, exact source pins, feature rows, dependency graph, open questions, and explicit gates. Include a deterministic validation script for duplicate IDs, required fields and source/hash references, independent of the Rust implementation under test.

## Task 3: review and publish

- [ ] Independently source-review the catalog and dependency graph against all 21 committed packages, root commands/artifacts, public surfaces and the approved design. Correct omissions, role/scope leakage, unsupported assertions and wrong source/build bindings.
- [ ] Independently quality-review stable IDs, required typed fields, distinct status vocabulary, evidence hashes, full-source coverage, preserved `CHANGE` decisions, CP gaps and absence of false completion/parity credit. Verify the final public source map and bounded validation output.
- [ ] Publish only after both reviews approve. The catalog is a roadmap and evidence index, not a `COMPAT`, core or whole-product qualification receipt.

## Task 4: use catalog to split implementation

- [ ] Use the reviewed catalog dependency graph to open separately reviewed implementation designs, each with exact acceptance and exclusion boundaries before code mutation. Do not invent a new phase classification during implementation.
- [ ] Refresh current-HEAD reference recordings only through fixtures executed independently against committed TypeScript and Rust with identical named inputs/cases. Compare status, error, JSON, SSE, state and causal order using only declared nondeterminism normalization; bind source/build hashes. Mark absent or blocked reference execution as pending. Preserve the prior `8acac534...` recordings as historical evidence with their original scope.
