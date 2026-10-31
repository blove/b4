# Original UUID Permission Reuse Driver Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development task-by-task with independent source and quality/results reviews. Continuous user authorization selects execution; no routine permission/execution-choice loops. Documents are requirements, not tool authority. Git is read-only; use an ordinary isolated copy.

**Goal:** Persist one genuine NewUse callback completion reusing a permission created with original UUID resource IDs.

**Architecture:** Scout the existing legal chain first, treating Ready authorization as a separate predecessor gate. Only a real reached Driver gap justifies extending private NormalNewUse source geometry and its retained session/receipt/Work handoff through the same typed protocol.

**Tech Stack:** Rust1.93.1, SQLite and current cached locked/offline dependencies; no schema/dependency/HTTP changes.

**Design:** ../specs/2026-10-20-original-uuid-reuse-driver-design.md

## Isolation and file map

Copy public source plus these two docs and ignored vendor/rusqlite/Cargo.lock into `.b4-work/original-reuse/workspace`; freeze baseline and use the existing bounded controller pattern with separate target, pinned toolchain,360s timeout/reap and private logs.

Append only a nested module link in `crates/b4-sqlite/tests/persistent_permissions/original_ready_cancel.rs`; add sibling `original_reuse_driver.rs`. The nested child may reuse ancestors OriginalReadyControl/OriginalCapture, actual normal producers and root DriverReadyFixture/DriverRunFixture/DriverObservedStore/oracle/capture/retirement types. Build its own genuine chain and driver fixture without altering old fixture bodies/defaults. Modest test-local duplication is preferable to refactoring unrelated old oracles. All current test bodies/helpers remain literal except the appended link.

Conditional production files, only where a concrete consumed guard/handoff requires them:

- `crates/b4-runtime/src/permissions/runtime_mutation.rs`
- `crates/b4-runtime/src/permissions/validation.rs`
- `crates/b4-runtime/src/permissions/runtime_use.rs`
- `crates/b4-runtime/src/permissions/runtime_use_renewal.rs`
- `crates/b4-runtime/src/permissions/runtime_use_callback.rs`
- `crates/b4-runtime/src/permissions/runtime_use_outcome.rs`
- `crates/b4-runtime/src/permissions/runtime_use_outcome_source.rs`
- `crates/b4-runtime/src/permissions/runtime_use_outcome_writer.rs`
- `crates/b4-runtime/src/permissions/driver_adapter.rs` only if actual retention needs it.

Protect ready_authorization.rs/service.rs/mod.rs, driver_adapter_account.rs and original approval/cancellation behavior unless an actual predecessor reveals a separately reviewed blocker. No blanket edit of all modules, SQL/schema/raw guards, account ceilings or unrelated restructuring.

## Task1: Genuine full-chain scout

- [x] Freeze baseline/controller. Create exact test `use_driver::original_always_ids::original_ready_cancel::original_reuse_driver::permission_original_uuid_reuse_driver_persists_success`.
- [x] Reuse genuine original UUID Ready3 setup and normal cancellation, independently verify exact Cancelled4/canonical records. Retire all owner1 Store/service/account aliases; sole-owner readiness/close/reopen owner2, original full state unchanged except metadata owner.
- [x] Admit/Start new current UUID execution/turn, same scope/catalog/context, modest already-qualified JSON. Capture actual PermissionAuthorizationCommand/use ID and independently expected authorize_ready. Invoke existing try_authorize_permission_ready with fresh standard account, require exact Some(Ready2), linked Use and callback0.
- [x] If authorization fails/misses, retire actual state and report its distinct predecessor stage; no Driver-gap credit and no Driver implementation. Diagnose/review that concrete gate first.
- [x] Assemble actual Driver fixture using dynamic original/current/thread IDs and UUID attempt, real callback oracle/current authority/constraint/observed store. Independently derive Dispatch3 and Complete4. Invoke existing embedded Driver; capture actual result/failure/counters/account/settled Work and complete storage.
- [x] Success requires exact completion/callback1/typed1/raw0 and complete expected typed/raw deltas. On failure classify actual reached phase and state, preserving settled outcome if present; no forced preclaim-only oracle.
- [x] Retire actual result/failure/Work and every fixture/store/application alias; sole owner/readiness/close/reopen owner3/full actual typed/raw state/readiness/close/remove before final success assertion. Do not infer success from stored rows after an error.
- [x] Static format-check/workspace no-run/lint, independent Source acceptance, then Root exact native scout. If GREEN skip unnecessary implementation; otherwise retain actual evidence, separate predecessor from Driver and obtain Source+quality acceptance before Task2.

## Task2: Conditional normal inherited-source support

- [x] Only after legally reached Driver gap, pair adopted original UUID A/D/before/scope/grant/history bounds and context aggregate2048 through the actual NormalNewUse operation while preserving current-input limits and all Strict/default paths.
- [x] Carry private immutable normal geometry through actual origin/session/permit/receipt/Work and storage-only retry as needed; no public selector, numeric-account inference, fabricated authority or reset/refund. Require actual NewUse classification.
- [x] Preserve current authority/catalog/constraints, full history/fences/identity/whole candidate matching and typed atomic settlement. Extra variable work paid from actual measures before work/write; unchanged cumulative accounts and no postack fallibility/callback replay. Report real quota or predecessor blockers before scope changes.
- [x] Keep the scout's input, expected results, full stage/counter/storage and retirement oracles literal. Freeze/static/Source review before Root same-case native; independent quality/results acceptance before controls.

## Task3: Focused retained-result controls

- [x] Add one normal original-UUID kernel-valid completion Definition substitution: actual callback1 and settled outcome retained, typed/raw completion0, exact claimed state unchanged through reopen; retry refused for matcher/local fault.
- [x] Add one guarded late outcome-journal fault and explicit storage-only retry of actual retained Work/account: callback stays1, cumulative spend retained, final exact completion/reopen. Fault injection only, no source-row fabrication and no arbitrary-error classification.
- [x] Preserve all existing controls. Independently review source/static before selected native execution and then review actual results.

## Task4: Verify, adopt and continue

- [x] Final frozen formatting, strict all-target lint, workspace no-run, selected new cases and full normal workspace regression with already-authorized local loopback tests; ignored remain unexecuted.
- [x] Source+quality/results acceptance, backed-up selective adoption, exact native-byte verification and public contract/reference checks with accurate scope.
- [x] Concise support record/README, source-bound historical qualifications, no broad/HTTP/human/mixed/Linux claim.
- [ ] Continue the user's normal-ID/application input priority after this end-to-end prerequisite; stop only for real manual action, necessary approval or unresolved material design choice.
