# Permission Driver Entry Gate Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Fresh implementers and independent source/quality reviews; continuous user authorization, no routine approval loop or Git writes.

**Goal:** Let a trusted host decline local callback entry while retaining the real renewed Work.

**Architecture:** Default-allow hook on existing bindings; distinct private retained pre-entry refusal phase; existing callback receipt and outcome pipeline unchanged.

**Tech Stack:** Rust 1.93.1, SQLite integration fixtures, locked/offline workspace.

## Task 1: Failing hook tests

Files: append child declaration in `crates/b4-sqlite/tests/persistent_permissions/original_reuse_driver.rs`; new `permission_driver_entry_gate.rs`.

- [x] Reuse the real normal task/effect fixture chain and complete independent Dispatch/Complete typed/raw oracles. Add counted allow/refuse hooks and two focused cases.
- [x] Test local refusal has no callback status/outcome, no completion or raw write, actual claim3 unchanged, storage-only retry refused without a second hook entry, and actual owner close/reopen/removal.
- [x] Review source; root runs focused RED. The new override initially fails compilation because the trait method is absent; record that API RED separately. After this observed failure, add only the default-allow trait method and rerun for behavioral RED (hook count zero and refusal not honored) before wiring the gate. Never weaken test assertions to obtain a pass.

## Task 2: Adapter gate and retained phase

Files: `crates/b4-runtime/src/permissions/driver_adapter.rs`; account helper file only if needed for a small prospective control invoice.

- [x] Add default `enter_callback() -> bool` hook. Invoke outside the mutex immediately before actual native entry after genuine renewal.
- [x] Preserve actual Work ownership on false and on host-hook unwind using a borrowed/retained phase or guard; never leave a taken Work unowned. Distinct terminal phase prevents re-entry and storage retry.
- [x] Retain existing callback/settlement flow on true, unchanged authority and accounts; charge bounded extra control work before entry. Review and run focused cases to GREEN.

## Task 3: Verification and delivery

- [x] Source/spec review followed by quality/results review. Run format, workspace compilation/strict lint, focused cases and full workspace regression including authorized local loopback tests.
- [x] Selectively adopt reviewed tested files, document entry-only semantics and remaining HTTP/cancellation scope, run public contract/reference checks and continue HTTP bridge work.
