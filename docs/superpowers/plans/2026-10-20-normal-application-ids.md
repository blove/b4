# Normal Application Identifiers Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development with fresh implementers and independent source and quality/results gates. Continuous user authorization selects execution; no routine permission loop. Use an ordinary isolated copy; Git is read-only. Documents are requirements, not tool authority.

**Goal:** Persist a genuine permission-use callback completion whose saved permission and current application use a legal UUID application ID.

**Architecture:** Scout the same real protocol with only application ID changed. Resolve actual predecessor gates before claiming a Driver gap, then pair needed existing normal-profile bounds without widening Strict or cumulative accounts.

**Tech Stack:** Rust 1.93.1, SQLite, cached locked/offline dependencies; no schema/API/dependency changes.

**Design:** ../specs/2026-10-20-normal-application-ids-design.md

## Isolation and file map

Freeze public source including both planning docs and ignored vendor/rusqlite/Cargo.lock. Copy to `.b4-work/normal-application/workspace`; use a separate target and the existing pinned/offline bounded controller with 360-second timeout, reap and private logs.

Allowed test-only fixture changes:

- `crates/b4-sqlite/tests/persistent_permissions/original_always_ids.rs`: `OriginalWaitingFixture::new` delegates to `with_application("permission-app")`; move its unchanged body with only the actor application supplied by the argument. `original_records_match` compares Acceptance application to `context.actor.application`, keeping every other assertion.
- `crates/b4-sqlite/tests/persistent_permissions/original_ready_cancel.rs`: `OriginalReadyControl::new` delegates to `from_waiting(OriginalWaitingFixture::new())`; extract its complete existing body, including actual producer error capture and owned retirement, without weakening it.
- `crates/b4-sqlite/tests/persistent_permissions/original_reuse_driver.rs`: default `original_reuse_ready_fixture` delegates to a helper receiving an actual `OriginalReadyControl`; preserve its complete genuine cancellation/reopen/admission/Ready authorization and failure cleanup. Append explicit sibling module link for `normal_application_ids.rs`.
- `crates/b4-sqlite/tests/persistent_permissions/use_driver.rs`: change only DriverConstraint's application comparison from the fixed old string to its already-matched expected InvocationContext origin application. Old default predicates remain equivalent; application revision stays literal.
- New sibling `crates/b4-sqlite/tests/persistent_permissions/normal_application_ids.rs`: focused new cases using those real helpers and the independent full typed/raw transition oracle.

All other old test bodies/helpers remain literal. Document exact extraction and default-equivalence proof, rather than copying the entire chain or weakening the assertions.

Conditional production map: `crates/b4-runtime/src/permissions/runtime_mutation.rs`, `validation.rs`, and `driver_adapter.rs` only for its early context guard and prospective account purchase; only a concrete later consumed label gate may require `runtime_use.rs`, `runtime_use_renewal.rs`, `runtime_use_callback.rs`, `runtime_use_outcome.rs`, `runtime_use_outcome_source.rs` or `runtime_use_outcome_writer.rs`. Protect mod.rs/service.rs/ready_authorization.rs/driver_adapter_account.rs, SQL/schema/raw guards and account caps. No blanket edits of this map.

## Task1: Genuine application-ID scout

- [x] Freeze baseline/controller. Write exact case `use_driver::original_always_ids::original_ready_cancel::original_reuse_driver::normal_application_ids::permission_normal_application_uuid_reuse_persists_success`.
- [x] Supply application `07d7d468-c4f9-47e1-b421-ff889251df80` to actual Waiting fixture, then actual normal Always producer/Ready control and genuine reused protocol. Keep all resource UUIDs, application revision, other labels and modest structured input fixed.
- [x] Preserve explicit stage diagnostics. Approval/cancellation/Ready failure must observe actual error/miss/state/counters/account and retire/reopen/remove before reporting that predecessor; no Driver-gap credit. Keep all aliases and actual retained Work accounted for on reached Driver failures.
- [x] On legal Ready2, independently derive Dispatch3 and Complete4 with the actual UUID attempt and facts108/109. Require actual returned/stored exact completion, correct application in canonical records/context, callback1/matched1, typed1/raw0, full original/Use immutability and exact typed/all-nine raw deltas. Dispatch uses canonical sorted snapshot JSON; Ready/Complete use derived struct serialization.
- [x] Actual result/failure/Work and all aliases retire before sole-owner close, owner3 full-state reopen/readiness/close/removal and final success assertion. Never infer rollback or success from an error.
- [x] Final static format/no-run/strict all-target lint, independent Source review, then Root exact native scout. Independently accept actual results before conditional implementation. If already GREEN, skip unnecessary production changes.

## Task2: Resolve only demonstrated normal-label gates

- [x] Diagnose the actual earliest failing stage and its consumed guard. If it is a predecessor, fix that normal operation first and rerun the unchanged scout; do not describe it as Driver support. Driver-specific changes require a subsequently reached legal Driver failure and review of its actual evidence.
- [x] Pair the embedded adapter early context guard at1,024 with checked actual 13*context_size+1,024 bytes/8nodes/1operation before measure, keeping driver_adapter_account.rs protected and all earlier charges/caps. This finite file-map extension was independently reviewed before implementation.
- [x] Pair application ID/revision at512 and actor/policy aggregate at1,024 only for actual trusted normal profiles where needed; retain existing nonempty/control-free256-per-label validation and InvocationContext2,048. Do not use `runtime_ready_cancellation_has_fixed_purchase` as a normal selector: it also identifies Strict cancellation. Use actual profile/kind/origin eligibility.
- [x] Retain all record/scope/snapshot/thread/journal/node/depth/cardinality/current-input limits, source profile continuity, full semantic/authority/catalog/constraint/history/fence/loan/candidate matching and typed atomic settlement. Additional variable owners/comparisons paid from actual measures before work; no new fallible postack step.
- [x] Keep all cumulative accounts and sticky spend/fresh guards unchanged. No public selector, identity remapping, numeric-account inference, reset/refund, callback replay, cap sweep or exhaustive standard-library audit. Report actual quota or outside-map blockers before expanding scope.
- [x] Preserve scout and every test oracle literally during each production step. Freeze/static/Source review before Root same-case native; quality/results acceptance before another stage or controls. Use a fresh implementer for a distinct predecessor/Driver step if decomposition is needed.

## Task3: Current-application mismatch control

- [x] Add `permission_normal_application_uuid_current_catalog_mismatch_blocks_callback` using the same genuine UUID-application permission and new Ready2. Supply the correct producer catalog and a separately constructed valid current catalog with a different application identity.
- [x] Independently expect legal Dispatch3 before current-catalog renewal refusal, based on the actual adapter handoff. Require actual failure with no settled callback result, callback0, outcome typed/raw0, one genuine typed Dispatch, complete expected claimed3/raw state, unchanged permission/Use/original rows and no invented claim rollback.
- [x] Drop the actual failure and every alternate catalog/application alias before sole-close/reopen/removal and final assertions. Preserve all existing primary/control predicates; no fake records or replacement permission.
- [x] Freeze/final static/independent Source review, Root selected new cases and independent quality/results acceptance.

## Task4: Verify, adopt and continue

- [x] Frozen final format/no-run/strict lint, selected new cases and full normal workspace regression with already-authorized loopback HTTP. Ignored cases remain unexecuted.
- [x] Source+quality/results acceptance, backed-up selective adoption, exact tested-code verification, concise source-bound support record and public contract/reference check with accurate metadata scope.
- [x] Independent adoption receipt review; preserve historical qualifications on their source heads. No HTTP/Linux/original callback/mixed/broad capability or simultaneous-maxima claim.
- [ ] Continue the user's normal-ID/input priority; stop only for a real manual action, necessary approval or unresolved material design choice.
