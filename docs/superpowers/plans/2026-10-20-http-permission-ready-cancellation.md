# HTTP Permission Ready Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development with fresh sequential authors and independent source then quality/results reviews. User continuous authorization overrides routine design/branch approval pauses. Use an isolated ordinary source copy; no Git writes or parallel implementation.

**Goal:** Cancel genuinely registered original/NewUse Ready permission runs over HTTP, preserving real Driver ownership through drain.

**Architecture:** Extend T's private actual-control registration with the initial Ready owner and an actual confirmed typed Ready-writer result. Keep existing Pending/claim/lease management, reconcile only that exact Ready terminal after Driver drain, and retain T's entered path. New explicit V6 profile; no generic fallback or claimed-unentered withdrawal.

**Tech Stack:** Rust1.93.1, native permission/Driver/kernel/SQLite, axum/tokio actual loopback tests, locked/offline dependencies.

## Task1: V6 expectations

- [x] Root obtains design/source-fit review, captures public baseline and isolated workspace/controller before implementation.
- [x] Fresh author creates only `crates/b4-contract/tests/capabilities_v6.rs`: independently derive expected V6 from unchanged V5 fixture; strict narrow Ready+entered promises, complete base preservation, common validation, unknown/required/duplicate refusal and frozen V1–V5 controls. API `CapabilityDescriptorV6::permission_ready_cancellation(validV5)`.
- [x] Independent source review; Root `cargo test -p b4-contract --test capabilities_v6 --no-run --locked --offline` missing-API RED; independent quality/API-results gate.

## Task2: Strict V6 contract

- [x] Fresh author adds `crates/b4-contract/src/capabilities_v6.rs`, lib exports, `contracts/agent-protocol/schemas/capabilities-v6.json`, `contracts/agent-protocol/examples/capabilities-v6-permission-ready-cancellation.json` and only additive new metadata checks in `scripts/check_contracts.py`. Validate unchanged common assembly by valid V5 projection; preserve older fixtures/source contracts. No HTTP behavior yet.
- [x] Source gate; Root focused V6 GREEN and metadata checks; independent quality/results review.

## Task3: Real HTTP Ready expectations

- [x] Fresh author creates `crates/b4-http/tests/permission_ready_cancellation.rs` with the five focused design scenarios and `crates/b4-sqlite/tests/permission_ready_cancellation_reconciliation.rs` for the sealed actual-producer/drained-failure controls in the design. Reuse genuine HTTP Always provisioning, loopback, callback gates and physical observations; add only necessary test-only Ready barriers/delegation to `permission_entered_support/store.rs`, with old hooks/defaults/assertions unchanged. Ask Root before any other support extraction.
- [x] Intended constructor `HttpHost::new_with_permission_ready_cancellation(store,auth,policy,PermissionHostConfig)` and narrow default-false Store method `permission_original_ready_cancellation_atomic_durable`. Preserve existing `permission_use_ready_cancellation_atomic_durable`. Exercise constructor missing-capability refusal and actual healthy SQLite support, not a test mirroring a literal boolean.
- [x] Require actual registered Ready/preparation barriers, real writes and receipts, finite cleanup, no Dispatch-as-entry inference. Private confirmed writer result plus exact no-attempt terminal reconciliation must be distinguishable from arbitrary Driver/storage failure.
- [x] Source gate; Root `cargo test -p b4-http --test permission_ready_cancellation --no-run --locked --offline` intended constructor/trait API RED; independent quality/API-results gate. Also run `cargo test -p b4-sqlite --test permission_ready_cancellation_reconciliation --no-run --locked --offline` for intended missing confirmed-producer/failure methods. Do not weaken expectations to fit production.

## Task4: Ready capability and ownership handoff

- [x] Fresh author adds original Ready declaration defaultfalse/Arc/SQLite true in runtime permissions model and SQLite permissions module, without changing old embedded operation behavior. Preserve existing mutation/reader behavior. Add only the reviewed opaque `PermissionReadyCancellationConfirmation` producers and shared private projection core; no constructor from caller snapshots.
- [x] Add runtime failure `reconcile_ready_cancellation(self,&confirmation)->Result<StoredExecution,Self>` with its own retained Store read, exact private initial/current/origin/noWork relation and narrowly tagged actual stale-initial/preclaim fence provenance. Reject unrelated/mixed/unknown faults and other-Store confirmation against a Ready own Store. No callback-status-only inference. Add focused private Ready confirmation/reconciliation unit in HTTP, narrow changes to tracker/permissions bridge/manager/control/profile builder/capability publication. Preserve V5 constructor behavior and entered path. Use existing typed original-normal/NewUse Ready APIs and current CancelRun authority/catalog/source/fences; no effect-gate veto or generic Store fallback.
- [x] Bind registration's actual initial Ready and captured control. Resolve actual Ready versus entered classification before Pending. Dispatch-race refusal must reject/release Pending; never wait for a callback blocked behind that same gate. Existing identity-bound eight conflict attempts remain.
- [x] Retain only the actual typed writer's exact Ready terminal in private confirmation before publication. Publish cancellation without native signal or fake acknowledgement. Worker waits for Pending settlement and reconciles only that exact initial/current no-attempt Ready relation after actual Driver drain. Unrelated/mismatched/unknown failures stay faulted, with no replay/recovery.
- [x] Preserve bounded source/input/record/history/default account limits. Use an additive purpose-appropriate Ready factory sharing the reviewed 24 MiB/65,536-node ceiling and preserving entered/default factories; no arbitrary knobs, resets/refunds, operation audit or speculative matrix.
- [x] Source gate; Root actual focused reconciliation and HTTP GREEN; independent quality/results review. Correct concrete compile/fixture/source failures from actual evidence only.

## Task5: Frozen checks and exact adoption

- [x] Root freezes reviewed source; runs focused V6/Ready/T-entered regression, format, strict workspace all-target lint, compile and full locked/offline tests with finite900-second full child window and kill/reap, source maps and log hashes. Previously authorized local loopback escalation applies. Keep metadata separate from runtime evidence.
- [x] Independent integrated source/results and delivery review; scoped runbook/qualification, exact selective adoption with first backups, public descriptor/metadata, final integrity and acceptance record. Counts derive from actual logs, including any child/helper executions, rather than copied totals.
- [ ] Continue into the real claimed-but-unentered Work handoff the user prioritized. Do not declare all pre-entry HTTP timing states, recovery/deletion or broad permissions complete after Ready.

All focused and full unchanged-source checks passed; exact reviewed source was adopted with backups. Runtime evidence and metadata validation remain separate. Continue with actual dispatched-but-unentered Work withdrawal.
