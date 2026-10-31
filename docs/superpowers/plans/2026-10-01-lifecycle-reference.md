# Lifecycle reference capture implementation plan

> Use superpowers:subagent-driven-development for review and verification.

**Goal:** Preserve independent actual pinned Fetch observations for selected
cancellation, deletion, attachment and state behavior before implementing them.
**Architecture:** Separate provider-free fixture and two temporary app/database
captures; controlled application barrier, genuine policy/grant seams and real
runtime behavior. Raw bytes and scoped repeat projection stay separate.
**Design:** spec/runtime/lifecycle-reference-design.md.
**Stack:** Existing Node24/pinned built TypeScript runtime; no new dependencies.
No original source/lock, prior fixture/recording or native code is changed.

## 1. Review and fixture

Own only new conformance/reference/lifecycle-fixture/** until frozen. Checker and
metadata belong outside this immutable fixture after capture.

- [x] Independently review design and this plan against real pinned production paths.
- [x] Add minimal app config/package, immediate and held workflow routes, and
  genuine permission-gated route/tool only if needed for parked/checkpoint cases.
- [x] Add capture driver with exact pin/lock/source and fresh385 inventory guard,
  credentials removed, external Fetch forbidden, bounded held-effect cleanup.
- [x] Record missing/idle/completed/active/parked lifecycle requests, real live
  snapshot/tail/capacity and aborted-viewer cut, state and graceful reopen.
- [x] Author projection helper and meaningful tests for exactly declared dynamic
  identifiers/times/retry jitter; preserve event order, data and effect counts.

## 2. Capture and independent evidence review

Own only new conformance/reference/recordings/2026-10-01-lifecycle/**.

- [x] Execute actual built Fetch runtime twice with two fresh apps/databases;
  inventory before import and after; no imported fixture responses/checkpoints/stubs.
- [x] Preserve raw bytes, SSE, ledger/barrier cuts, generated values and strict
  before/after integrity; fail closed on unexpected/external calls.
- [x] Independently validate observations and handler/model/tool paths; distinguish
  application fixture failures from captured protocol refusals.
- [x] Freeze fixture/captures and save scoped repeatability/provenance, reporting
  genuine ordering/nonrepeatability without extra normalization.

## 3. Traceability and next native work

Own external checker/tests and docs/requirements metadata; never alter frozenfixture.

- [x] Add integrity validation with corruption regressions for raw bytes/projection,
  sidecar links and required truncated/complete capture metadata.
- [x] Mark only exercised CP questions partial; native operations stay planned.
- [x] Run fresh combined Node/Python checks and save separate evidence, linking the
  prior287 Rust run because this increment changes no Rust source.
- [x] Begin native design with fenced deletion and identify cancellation dispatch
  race requirements. Cancellation and attachment implementation follow separately.
