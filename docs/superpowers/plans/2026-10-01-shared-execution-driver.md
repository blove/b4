# Shared bounded execution driver implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and
> test-driven development, with independent design/spec/quality review. The user's
> continuous-build authorization covers implementation; no commit/publication is requested.

**Goal:** Execute real bounded parallel batches through one reusable native driver
while preserving exact approval, cancellation, result and ownership boundaries.

**Architecture:** One synchronous coordinator mutates the store; scoped callback
workers return retained outcomes. Split durable preparation from invocation and
replace HTTP scheduling with the shared driver, leaving AP projection in HTTP.

**Tech Stack:** Existing Rust workspace, standard-library scoped threads/channels,
MemoryStore and SQLite; unchanged locked offline dependencies and schema3.

Design: `spec/runtime/shared-execution-driver-design.md`.

## Task 1: Review and freeze interfaces

- [x] Independently review design against kernel/store/approval/HTTP cancellation.
- [x] Capture baseline artifacts and agree PreparedEffect/driver port/capacity/error
  interfaces before parallel source edits.

## Task 2: Durable preparation boundary (service author)

Files: `crates/b4-runtime/src/runtime.rs`, approval service/model exports, new
preparation tests. Coordinate public interface with driver author.

- [x] Add failing tests proving preparation commits InFlight without invoking,
  exact invocation/input identity, current owner and full required approval checks.
- [x] Add meaningful policy/cancel/claimed-view refusal regressions; record RED.
- [x] Implement Runtime/ApprovalService prepare_dispatch and refactor dispatch APIs
  to reuse it, preserving retained outcome errors and authority ordering.
- [x] Run scoped tests/lint; independent spec and quality review; repair findings.

## Task 3: Shared driver and native tests (root)

Files: new `crates/b4-runtime/src/driver/` modules, public exports and new driver
tests. Keep capacity, coordinator and public model responsibilities focused.

- [x] Write barrier tests for overlap, reverse result join, finite shared/per-run
  capacity, stable approval park and no sibling mutation during preparation.
- [x] Write cancellation/failure/panic drainage, retained completion retries and
  exact identity/owner/attempt/refusal/exhaustion tests; record RED.
- [x] Implement bounded coordinator, callback port, capacity and typed retained
  failures; every child joins and no callback retries.
- [x] Validate result/config bounds before mutation; notify host faults immediately
  on uncertainty while keeping every entered child owned through quiescence.
- [x] Run scoped tests/lint; independent spec and quality review; repair findings.

## Task 4: HTTP integration (host author)

Files: HTTP worker/host/approval bridge, new parallel API/TCP tests, example fixture
only if useful. Coordinate driver exports before edits.

- [x] Add failing overlap/park/resume/cancel tests with actual durable effects;
  preserve attachment, exact cancellation and producer lifetime regressions.
- [x] Implement driver host port for ScriptRoute, validated global/per-run task
  limits and bridge preparation; remove independent HTTP scheduling loop.
- [x] Keep after-commit chunk projection and final AP terminal projection in HTTP.
- [x] Run all HTTP tests and strict lint; independent review and repair findings.

## Task 5: Separate-process crash qualification (root)

Files: new SQLite driver/process tests and a test-only child helper; no production
fault-injection routes or credentials.

- [x] Prove kill after sibling+park commits retains grants and successful sibling.
- [x] Prove kill after Once acceptance before callback entry loses no continuation.
- [x] Prove Unsafe ledger effect before result commit blocks without replay.
- [x] Verify new owner can acquire the store, immutable exact recovery fences,
  read-only pending exposure and callback counters after recovery.
- [x] Independently review harness synchronization, actual kill semantics and scope.

## Task 6: Documentation and evidence

Files: README/design/ADR/storage/security/requirements and separate receipts.

- [x] Document real parallel driver, limits, native callback scope and remaining
  release gates; all56 release scenarios remain pending until individually qualified.
- [x] Run fresh workspace tests, format/strict Clippy, Node/Python/contract checks,
  example builds/runs and immutability checks.
- [x] Save exact scoped native evidence and artifact hashes in a new receipt.
- [x] Continue remaining core work while no manual blocker exists.

Completed bounded increment:493 fresh Rust tests pass (55 added:11 preparation,
28 driver/repair/evidence,13 HTTP API/TCP,3 actual SIGKILL). Independent reviews
reproduced and repaired ownership/cancellation evidence gaps; formatting and strict
workspace lint pass.50 Node,8 Python,18 schemas/170 local requirements and8 rebuilt
examples pass.159 protected prior artifacts remain unchanged. Receipt:
`conformance/expected/shared-execution-driver-verification.json`. All56 release
scenarios remain pending, M0 open. Continuing native registry/capability design
under the user's continuous-build authorization.
