# Durable settled failure implementation plan

> User continuation authorizes this bounded increment in the current workspace.
> Use test-driven development and parallel agents with disjoint file ownership.

**Goal:** Persist explicit known application failures and publish the captured
wait/stream failure forms only after durable settlement.

**Design:** `spec/runtime/settled-failure-design.md`.

## 1. Contract and pure kernel

Files: `b4-contract/src/model.rs`, kernel transitions/validation and tests;
contract snapshot schema (root owns schema).

- [x] Write failing tests for failure identity/idempotency, sibling drain,
  cancellation precedence, recovery and restoration invariants.
- [x] Add optional task failure/status and FailAttempt; preserve old snapshot reads.
- [x] Implement no-new-dispatch drain, retained results and failure settlement.
- [x] Verify existing lifecycle/serialization tests and review model invariants.

## 2. Transactional outcome service

Files: runtime API and tests, SQLite runtime failure tests; no table migration.

- [x] Write failing tests for explicit versus uncertain outcomes, borrowed retry,
  owner/CAS conflicts, drain admission and persisted reopen.
- [x] Add EffectOutcome/dispatch_outcome/complete_outcome and retained error outcome.
- [x] Preserve old success-only API; qualify both backend failure commits.

## 3. Scripted HTTP

Files: HTTP host/worker/tests/example; root owns reference-comparison tests.

- [x] Write failing tests for expected failure terminals, stopping later work,
  readiness/successors and failed outcome-commit publication.
- [x] Add bounded public Failure fixture and committed Failed projection.
- [x] Qualify real TCP and expose a failure workflow in the SQLite example.

## 4. Compare, review and finish

Files: root schema/docs/requirements/checker/receipts and selected comparison.

- [x] Compare all 24 selected cases with unchanged pinned TypeScript recordings.
- [x] Review implementation and fix material findings.
- [x] Update OpenAPI, compatibility, requirements and example verification.
- [x] Run fresh formatting/lint/all Rust and fixture tests, contract integrity,
  examples and selected comparison; save current hashes and new scoped receipt.

No commit, push or modification of the original TypeScript checkout is required.

Verified locally on 2026-10-01: 178 Rust tests (one child helper ignored and
explicitly exercised by its parent), 13 fixture-tool tests, strict lint and
formatting; 24 selected reference cases, seven real TCP cases and the SQLite
HTTP example restart. Contract checks: 17 schemas, 14 authored examples, 48
rejections, 117 local references and 68 named local requirements. Read-only
implementation review found no actionable issues. No core release gate closed.
