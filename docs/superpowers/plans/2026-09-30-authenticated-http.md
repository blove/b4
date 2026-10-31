# Authenticated scripted HTTP implementation plan

> **For agentic workers:** Use subagent-driven-development and test-driven-development. User continuation authorizes this bounded increment in the current Rust workspace.

**Goal:** Run a deterministic B4run application over authenticated native HTTP and record selected incumbent wire behavior independently.

**Architecture:** Add durable thread context/coherent reads and fenced admission to the existing Store. Add one `b4-http` crate for authentication, policy, AP projection, owned scripted workers and router. Preserve kernel purity and the existing transactional runtime. Reference build/capture uses an isolated pinned temporary checkout.

**Tech stack:** Rust 1.93.1, Axum/Tokio with raw bounded JSON/SSE; pinned TypeScript/Node/pnpm for reference recording.

Scope: `spec/runtime/http-subset-design.md`.

## Task 1: Independent reference capture

Files: `conformance/reference/fixture/*`, reference recording/manifest/compatibility docs.

- [x] Materialize exact pinned Git commit without changing the existing TypeScript checkout.
- [x] Frozen-lock install with lifecycle scripts disabled; run root build before importing dist. Full build completed 25/28 tasks; three web helper-port failures are retained as a limitation. Selected CLI/runtime graph completed 13/13.
- [x] Author deterministic workflow fixture and request corpus, validate capture/normalization.
- [x] Execute parser/input/terminal/wait/stream cases and retain raw/normalized results and build provenance.
- [x] Record selected preserve/change decisions; leave unobserved CP cases open.

## Task 2: Durable HTTP resource context

Files: runtime Store/memory, SQLite schema/migration/backend/tests, existing test wrappers.

- [x] Write failing tests for metadata/origin persistence, coherent reads, resource revisions, fenced admission and readiness.
- [x] Implement explicit thread seed/context APIs while retaining trusted native calls.
- [x] Qualify known v1-to-v2 migration and fail-closed unknown/corrupt schema.
- [x] Run backend and migration tests; review atomicity and origin isolation.

## Task 3: Authenticated scripted router

Files: `crates/b4-http/{Cargo.toml,src/*,tests/*,examples/serve.rs}`; workspace manifest/dependencies.

- [x] Pin compatible dependencies and verify the resolved compiler/Serde graph.
- [x] Write failing tests for required auth/policy, durable projection and bounded strict parsing.
- [x] Implement six operations plus probes, rejecting approval definitions and unsupported operations.
- [x] Run effects through owned blocking workers; test busy admission, viewer detach, bounded SSE and committed publication.
- [x] Add loopback token-configured example and real TCP tests.

## Task 4: Compare, review and verify

Files: compatibility/security/OpenAPI/requirements/README, scoped verification receipt.

- [x] Compare 22 selected successful workflow cases from two retained 62-case recordings; document deliberate differences and exclude failure/validation/park/attach parity.
- [x] Perform independent spec/code review and fix material findings.
- [x] Run formatting, strict lint, all 135 Rust tests (including five real TCP cases), 13 fixture-tool tests, existing fixture commands, new HTTP demo and contract validation.
- [x] Save current artifact hashes and evidence; preserve historical receipts and remaining release gates. See `conformance/expected/authenticated-http-verification.json`.

No commit, push, deployment or modification of the existing TypeScript checkout is required.

Local verification covers the scripted subset. M0 remains open, CP-001–004 are partial, CP-005–010 remain unresolved, and all 56 broad release scenarios remain pending. Historical receipts retain their original scope.
