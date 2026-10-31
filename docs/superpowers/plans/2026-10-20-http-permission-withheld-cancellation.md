# HTTP Permission Withheld Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development with fresh sequential authors and independent source then quality/results reviews. The user's continuous authorization overrides routine design/branch approval pauses. Use an isolated ordinary source copy; no Git writes, parallel implementation or nested delegation.

**Goal:** Cancel original Always and saved-use permission Work after Dispatch but before local callback admission, over HTTP, using the actual worker-owned drained failure.

**Architecture:** Atomically choose local admission or one private withdrawal command on the same registered control. The worker keeps its borrowed actual failure, services that command before ordinary Pending waits, invokes existing `cancel_withheld`, and delivers the actual result before no-signal terminal publication. Preserve V6 Ready and T entered behavior through a new explicit V7 profile.

**Tech Stack:** Rust 1.93.1, existing Driver/permission/kernel/SQLite, axum/tokio actual loopback tests, locked offline dependencies.

## Task 1: Strict V7 expectations

**Create:** `crates/b4-contract/tests/capabilities_v7.rs`.

- [x] Root obtains design/plan review, captures the public baseline, exact isolated workspace and bounded controller under `.b4-work/http-permission-withheld/` before implementation. Retain the shared ignored build target; full child window 900 seconds, other steps 600, kill/reap and before/after source maps/log hashes.
- [x] Fresh author adds only the V7 tests. API is `CapabilityDescriptorV7::permission_withheld_cancellation(validV6)`. Independently derive expected wire from the frozen V6 fixture, require only the new narrow true flag plus preserved Ready/entered/base fields, and preserve generic/broad false flags.
- [x] Cover closed/required/duplicate wire, common validation and invalid public assembly; independently freeze every existing V1–V6 fixture byte sequence. Keep six focused tests rather than a matrix.
- [x] Independent source gate, then Root runs `cargo test -p b4-contract --test capabilities_v7 --no-run --locked --offline` for intended missing-type RED. Independent quality/API-results gate precedes production metadata.

## Task 2: Additive V7 contract

**Create:** `crates/b4-contract/src/capabilities_v7.rs`, `contracts/agent-protocol/schemas/capabilities-v7.json`, `contracts/agent-protocol/examples/capabilities-v7-permission-withheld-cancellation.json`.

**Modify:** contract `src/lib.rs` exports and additive registration in `scripts/check_contracts.py`.

- [x] Fresh author implements strict V7 by validated V6 projection; expose `durable_permission_withheld_cancellation=true` while preserving shared behavior and old fixture/source bytes. Do not modify runtime or HTTP yet.
- [x] Independent source gate; Root runs the V7 target without `--no-run` and runs metadata separately. Independent quality/results gate closes this prerequisite.

## Task 3: Real HTTP handoff expectations

**Create:** `crates/b4-http/tests/permission_withheld_cancellation.rs`.

**Modify only as needed:** additive test-only barriers/delegation in `crates/b4-http/tests/permission_entered_support/store.rs` and focused shared helpers; ask Root before extracting another helper. Existing assertions/defaults remain unchanged.

- [x] Fresh author implements the design's five bounded business scenarios using real human Always provisioning and saved reuse. Include existing helper controls as distinct counted executions. Do not import an existing test file or fabricate Work/receipt/confirmation.
- [x] API expectation is `HttpHost::new_with_permission_withheld_cancellation(store, auth, policy, PermissionHostConfig)`. Constructor must reject either missing existing withheld durability declaration, with real SQLite support controls. Observe actual V7 metadata and unchanged V5/V6 Dispatch-before-entry refusal.
- [x] Use actual post-Dispatch renewal barriers. NewUse's existing renewal-source seam is available; original can use a one-shot phase-specific final `capture_permission_original_always` return, because its withheld writer reuses that port. Observe selection through the new profile's coherent post-reservation Store preflight, using a finite one-shot phase-specific/nth-read helper; earlier policy/read/lease events are insufficient. Release renewal after that actual decision and before waiting for the cancellation response. A paused renewal or preflight cannot itself prove actual Work or withdrawal success.
- [x] Prove both-origin exact two-transition Kernel/journal/thread result, no native callback/receipt/signal, one terminal, owners through real operation/drain, normal reopen/later saved reuse. Exercise actual entry-winning cancellation, current authorization/revoked constraint after Work retention, exact late second-journal rollback and fresh healthy control, abandoned admitted command/ownership.
- [x] Preserve finite gates/network cleanup and meaningful observations. No automatic storage retry, prior-owner recovery or maximum-size qualification is implied.
- [x] Independent source gate; Root runs `cargo test -p b4-http --test permission_withheld_cancellation --no-run --locked --offline` for intended missing constructor RED. Independent quality/API-results gate precedes HTTP production.

## Task 4: Atomic admission and worker-local withdrawal

**Likely modify:** `crates/b4-http/src/{permissions,permission_cancellation,cancellation,control,capabilities,host}.rs`.

**Create if useful:** a focused private `permission_withheld.rs` handoff module and private unit-test module. Register modules narrowly. Existing runtime/Store writers already supply the required operations; a new public runtime seam requires concrete source evidence and Root review.

- [x] Fresh author adds the explicit new constructor/profile, requiring all U declarations and both existing withheld declarations. Publish V7 only there; V1–V6 constructors, descriptors and old runtime APIs remain unchanged. Preserve callback/catalog/constraint ownership and input/source/account limits.
- [x] Under control synchronization, atomically decide local admission versus one same-control withdrawal request. Admission granted is only a withdrawal veto, never actual body-entry proof. If admission already won, reject/release Pending without waiting for a withheld failure; use T only after real entered evidence exists.
- [x] Add a private explicit hook decision for exact requested refusal, bypassing the current Hooks adapter's unconditional false-result fault only for this valid new handshake. Preserve unrelated false/error fault behavior. Wake the hook through Pending; do not infer the reason through a later unlocked request-flag read.
- [x] Bind bounded owned command data to captured claim/control/identity/origin/target. Keep the actual borrowed failure in the worker. No Store/policy/native calls or cross-participant waits under registry/control locks.
- [x] Stage one handoff as Reserved then Ready. After atomically reserving exact entry refusal, do one coherent current Store preflight outside locks and revalidate the same captured target/owner/origin/current authority before releasing Ready. Only Ready may execute the worker writer. A drained worker waits outside locks for Ready or fault/closure; reservation/preflight guard drop on refusal/unwind faults/closes and wakes both parties. This meaningful post-reservation Store read supplies the integration observation seam without a public test API or fabricated proof. A client disconnect does not drop the existing owned job.
- [x] Service the command after actual Driver drain and BEFORE BOTH original/NewUse `control.ready_confirmation()` waits. Construct current CancelRun authority/catalog/context from live bridge plus authenticated command, and invoke the actual origin-specific failure's existing `cancel_withheld`. Preparation/renewal/panic/uncertain receipt failures must not become eligible from a flag or absent callback status.
- [x] Deliver exactly one actual command outcome; retain it/failure/worker owners until Pending resolves. On confirmed result verify exact same-run origin-specific terminal and no native outcome. Publish interrupted/one cancelled Done via a no-native-signal path; preserve Ready and entered publication behavior.
- [x] Resolve refusal/fault/unwind/closure in both participants. Crucially, withheld errors must not enter `cancellation.rs`'s generic RevisionConflict resend loop: preserve old Ready/entered eight-attempt behavior, but never repeat a withdrawn Work command or create a replacement account/Driver. No automatic callback resume/replay or settlement/storage retry.
- [x] Retain management lease and real failure/assembly/claim/capacity/Store through confirmed commit and actual worker quiescence, including abandonment and shutdown. No returned uncertainty is settlement proof.
- [x] Add focused private control/handoff tests for requested refusal versus ordinary fault, admission granted before body entry, single outcome and wake-on-refusal/closure. These must establish atomic local rules without fabricating actual native-entry or writer proof. Keep the business suite bounded.
- [x] Independent source gate; Root runs actual focused HTTP GREEN and U Ready/T entered regressions, plus the private control/handoff tests. Correct only concrete compile/fixture/source issues demonstrated by evidence. Independent quality/results gate closes production.

## Task 5: Frozen verification, documentation and exact adoption

- [x] Root freezes reviewed source; runs final V7, withheld HTTP/private controls, Ready and entered regression, format, strict workspace all-target lint, compile, full locked offline suite and separate metadata. Existing local loopback escalation is authorized. Poll at most 45 seconds and communicate within 60; never count a timeout/partial run as a full pass.
- [x] Derive actual counts from log suffix summaries, including child/helper executions; record exact commands/source maps/log hashes/process reaping. Preserve locks/dependencies and public baseline while qualification is pending.
- [x] Add scoped runbook/conformance/qualification and README/plan closure. Obtain independent integrated source/results and delivery reviews; require explicit source/report/file-hash-bound approvals before exact selective adoption with first backups. Verify exact native equality before documentation closure and separate unchanged public metadata afterward.
- [x] Independent public integrity review and final acceptance close only this increment. Report no recovery/deletion, uncertain-receipt settlement, broad lifecycle, maximum-size or cross-platform qualification.
- [ ] Continue building toward the user's broader Rust runtime objective; ask only for a real missing decision/manual action, rather than stopping at this milestone.

All focused and full unchanged-source checks passed. Exact reviewed source and delivery were adopted with reversible first backups. Separate public metadata and independent public integrity passed, and final acceptance closed this increment. Continue with the runnable cancellation example.
