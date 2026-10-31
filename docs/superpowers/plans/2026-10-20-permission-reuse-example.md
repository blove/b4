# Runnable Permission-Reuse Example Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development with fresh implementers per task and independent source then quality/results review. Continuous user authorization; ordinary isolated source copy, no Git writes or routine approval loops.

**Goal:** Run a genuine saved permission through the Rust example across separate seed and server processes and repeated owner reopening.

**Architecture:** Explicit seed/serve modes share one immutable local application/catalog. Seed uses genuine embedded approval and Ready cancellation; serve uses the adopted opt-in HTTP host, environment authentication, loopback listener and owned signal drain. A bounded external-process harness checks actual durable state and normal child retirement.

**Tech Stack:** Rust1.93.1, pinned/offline Cargo, existing Axum/Tokio/SQLite, Python3.12 standard library. Enable existing locked Tokio signal for example/dev target; no new packages/lock changes.

## Task1: Acceptance harness before implementation

- [x] Create a dedicated ordinary copy after design/plan review, record baseline and reuse bounded pinned verification controller.
- [x] Fresh author adds `scripts/check_permission_reuse_example.py` only. Accept an explicitly supplied built example path; no hidden Cargo/build/network actions. Validate invalid arguments/missing token; seed a temporary database with genuine record linkage; start server on ephemeral loopback; authenticate one request, stop by interrupt and await successful exit; restart and repeat with the same thread, preserving original permission provenance and distinct actual Use/attempt IDs.
- [x] Assert real Complete4 snapshots/journals, required-use linkage, exact known output and immutable original permission records. Confirm normal listener retirement, final SQLite integrity and removal. Bound subprocess/request waits, retain/reap child handles and avoid logging tokens.
- [x] Source review; Root runs the harness against the absent example to record the actual gap. Compilation absence is not an executed application behavior claim.

## Task2: Example implementation

- [x] Fresh author adds `crates/b4-http/examples/serve_permission_reuse.rs` and focused support module(s) if needed. Explicit seed/serve arguments; fixed local app/catalog/actor/policy; one Pure required task, descriptive IDs, UUID resources, small JSON and no external effect.
- [x] Seed uses actual park/pending token/normal Always acceptance/Ready cancellation, prints thread_id/route JSON and releases actual store aliases. Serve derives its app from the same catalog, uses environment bearer token, validates mode/config before store where applicable, binds loopback and prints actual readiness address JSON without secrets.
- [x] Add Tokio signal under b4-http dev-dependencies using its existing locked package. Serve stops admission on interrupt, drains workers, retires listener and releases store owners; no callback abandonment or automatic recovery/permission creation.
- [x] Source review then Root targeted build and actual cross-process acceptance. Diagnose bounded failures; do not weaken storage/retirement assertions. Add focused example unit tests only for material behavior not covered by the external harness.

## Task3: Verify and deliver

- [x] Independent spec/source then quality/results review. Run formatting, targeted build/test, warning-denying all-target lint, actual process acceptance and appropriate HTTP regression. Use locked/offline toolchain, no dependency changes.
- [x] Document exact seed/serve/request commands, fixed local demo actor and supported bounds in `docs/runbooks/permission-reuse-example.md`; link from README. Keep broad Always/recovery/release scope open.
- [x] Adopt only reviewed tested files with exact backups/hash checks, verify public contract references, record actual local evidence and normal process/owner retirement, then continue useful next work.
