# Nested HTTP Input Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development with a fresh verification-test author and independent source then actual-results/quality reviews. Continuous user authorization overrides routine review pauses. No Git writes, nested delegation or parallel implementation. Use an ordinary isolated copy and temporary fixture data.

**Goal:** Qualify the already implemented normal original approval and saved reuse flow for nested input over real HTTP.

**Architecture:** Parameterize one existing whole TCP/SQLite scenario and its independent expected-input oracles, retaining its old default case and boundary wrappers. Add one nested case. Production stays frozen; this test-only verification may be GREEN on its first execution.

**Tech Stack:** Rust1.93.1, Tokio/real ephemeral loopback TCP, existing b4-http permission_approvals integration target and SQLite fixtures, locked offline dependencies.

## Task 1: One-file verification extension

**Modify only:** crates/b4-http/tests/permission_approvals.rs.

- [x] Root publishes approved design/plan as docs/superpowers/specs/2026-10-21-http-structured-inputs-design.md and docs/superpowers/plans/2026-10-21-http-structured-inputs.md, captures actual post-AA public baseline including vendor lock, creates equal ordinary source copy/shared build cache and a finite reviewed source-map/log/reaping controller.
- [x] Fresh author extracts the existing http_always_creates_completes_and_reuses_after_reopen body into a neutral scenario taking independent original/reused inputs. Old test calls it with exact old month payloads. Parameterize original_kernel, reused_kernel and callback oracle using expected maps; retain unchanged default callback_matches wrapper for the boundary module caller. A compact local context may avoid an unwieldy eight-argument helper. No global mode/input mutation, duplicated whole suite, snapshot-derived expected input or unrelated refactor.
- [x] Add http_structured_original_completes_and_reuses_after_reopen with AA's deterministic nested objects/arrays/options/Unicode/null/boolean input, optionally a related distinct saved-use input. Measure actual serialized HTTP input bytes/value nodes/depth and parked canonical snapshot geometry above91 and within the existing normal limits. Preserve genuine pending grant and HTTP Always resume; exact kernel original Dispatch4/Complete5 and saved Use Complete4; one actual callback each; original/reuse full stored/callback input equality, provenance, complete nine-table projections and normal owner/server retirement/reopen/removal.
- [x] Retain existing response-read/quiescence/listener5s waits and finite raw observer cardinality. Existing connect/write calls have no separate5s timeout; the outer process deadline supplies the complete run bound. No universal request-time guarantee, cancellation/deletion enlargement or runtime/capability/schema/dependency/example/probe changes. If source setup reveals a real prerequisite issue, report it before changing scope or assertions.
- [x] Root pinned formatting and exact snapshot. Independent source/spec review. Root runs the new filtered HTTP case freshly; a first GREEN verifies existing implementation, not an invented RED. Run complete permission_approvals and permission_uses targets against the same source. Any real failure is retained and diagnosed; production changes require a focused design amendment/fresh production task rather than weakened oracles.
- [x] Independent actual-results/quality gate verifies transport input propagation, real callback counts, durable provenance and normal drain/reopen, source maps/logs/exit/reaping/timeout. It must distinguish actual HTTP coverage from AA's earlier embedded-only result.

## Task 2: Scoped verification and reviewed delivery

- [x] Root runs frozen-source cargo fmt --all -- --check, cargo clippy -p b4-http --test permission_approvals --locked --offline -- -D warnings, and separate contract metadata validation. Record complete source maps before/after every finite command, exact command/exit/log hash/timeout/reaping. Scoped HTTP checks above are the new runtime evidence. No fresh full-workspace suite or unchanged example process probes are required for this test-only delta; AA's1664/0/11 full result remains separately linked historical evidence.
- [x] Prepare a concise conformance README/qualification and root README link describing actual nested HTTP proof, current descriptor and unchanged transport/runtime/cancellation limits, input geometry, lifecycle bounds and actual platform. Do not claim all HTTP maximum combinations, process-example coverage, universal network-time bounds or a new runtime implementation.
- [x] Independent integrated/results and exact delivery/controller reviews, explicit actual-report/artifact/source hash manifest and independent manifest gate. Verify baseline and reversible selective copy with first backups, complete native equality before prose, separate public metadata and independent public integrity before Root acceptance. Only then close completed plan boxes as separately recorded doc state.
- [ ] Continue authorized practical work after closure; the runnable nested-input example or setup may follow user preference.
