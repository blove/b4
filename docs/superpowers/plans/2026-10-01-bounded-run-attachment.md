# Bounded run attachment implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and
> test-driven development with independent spec and quality review. User authorization
> covers continuous execution. No commit or publication is requested.

**Goal:** Implement authenticated, bounded native GET run attachment as the tenth
scripted HTTP operation, including read-only parked prompt disclosure after reopen.

**Architecture:** RunControl owns transient snapshot cuts, digest, exact terminal
and subscriber queues. Attachment preflight renews immutable origin/definition
authority, binds the original turn and reads inactive snapshots. A separate approval
read operation exposes a stable park without transferring execution ownership.

**Tech Stack:** Existing Rust workspace, Tokio/Axum, MemoryStore and rusqlite;
locked offline dependencies and unchanged version-3 SQLite schema.

Design: `spec/runtime/bounded-run-attachment-design.md`.

## Task 1: Review and establish interfaces

- [x] Independently review design and plan against source and frozen lifecycle.
- [x] Preserve a preimplementation artifact inventory and agree host/service
  interfaces before parallel edits.

## Task 2: Read-only stable parked prompts

Files: runtime approval model/service/validation, new runtime and SQLite attachment
tests, HTTP approval bridge (coordinate with host author).

- [x] Write tests that prior-owner stable park disclosure fails under old current
  owner rule, denied corrupt grants are never decoded, full captured view is fenced,
  expired pending is unchanged, and ordinary pending/resume stay owner restricted.
- [x] Record meaningful RED; implement AttachmentPending and
  `attachment_pending(&context, &ThreadView)` with authority before sensitive read.
- [x] Exercise actual SQLite reopen, complete snapshot/grant/acceptance validation,
  unchanged owner/tokens/expiry/audit and no schema change; record GREEN.
- [x] Independently review spec compliance then quality and repair findings.

## Task 3: Bounded publication and authenticated attachment

Files: new HTTP viewer/publication and attachment modules, control.rs, host.rs,
transport.rs, worker.rs, lib.rs and new HTTP attachment tests. Host owns these edits.

- [x] Write API/publication tests for state-first cut/tail, non-live null done,
  capacity/disconnect/overflow/digest/state bounds, terminal cache and cancellation,
  original identity capture, renewed exact authority and restart refusal.
- [x] Record RED before source changes. Implement bounded queues for POST and GET,
  unique RAII subscriptions owned by the async future during policy waits,
  non-waiting Pending-gate cut/registration and cached terminal.
- [x] Implement GET Operation::Attach with early captured subscription, renewed
  policy/exact origin/Definition, live identity stability and inactive full fences.
- [x] Integrate bridge's read-only pending method. Preserve cancellation repair and
  owned worker lifetime. Record scoped GREEN and strict lint.
- [x] Independently review spec then quality; regress and fix findings.

## Task 4: Real transport qualification

Files: root-owned `crates/b4-http/tests/attachment_tcp.rs`, a new HTTP attachment
example checker, existing serve example only if necessary.

- [x] Test real TCP late join/chunks/done, inactive/reopened terminal and paused
  snapshots, protected refusals, selected turn across held policy/ABA, body/request
  abandonment and viewer reuse, cancellation fanout before effect quiescence.
- [x] Probe byte/frame/digest bounds with barriers; verify producer remains active
  and settles durably while detached viewers free only their own slots.
- [x] Build and run example checker against real server plus SQLite; independently
  review checks and repair meaningful omissions.

## Task 5: Contracts and evidence

Files: OpenAPI/state schema/examples, requirements.yaml, contract checker,
compatibility/README/storage/security design and new ADR/evidence receipts.

- [x] Add narrowly scoped native state schema/examples and 10-operation markers;
  keep CP-007 partial, all core release scenarios pending and native/reference
  shape differences explicit. Compare only captured fields qualified by native
  execution; never edit historical captures/fixtures/receipts.
- [x] Run fresh `cargo test --workspace --locked --offline`, format and strict
  all-target Clippy, Node/Python reference checks, contract checks and all examples.
  Loopback test execution may use the previously approved sandbox escalation.
- [x] Save a separate receipt with exact test counts, artifact hashes, selected
  comparison scope and preserved earlier evidence/schema/lock hashes.
- [x] Continue remaining core work while no manual blocker exists.

Verification:438 Rust tests,50 Node tests,8 Python verifier tests,18 schemas,
19 authored examples,58 rejection cases,126 references,145 local requirements
and eight current-build examples pass.148 historical conformance/schema/dependency
hashes are unchanged. Independent review findings were reproduced and repaired.
The separate receipt records the native scope and selected comparisons; the full
core release profile remains open. Continuous work proceeds to the next increment.
