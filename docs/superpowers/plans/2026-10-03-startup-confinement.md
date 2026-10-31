# Bounded Opt-In Startup Confinement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans after the explicit implementation dispatch. Steps use checkbox syntax. Source-plan acceptance alone authorizes no repository/code/scaffold, compilation or runtime action.

**Goal:** Add one explicitly opted-in native Preparing mode that can safely confine selected prior-owner missing-Definition or policy-denied records without changing default preparation or recovery admission counts.

**Architecture:** A borrowed preparation method registers the existing startup role before trusted-handle clones. A separate coordinator uses inherited ordinary-view/principal bootstrap, a bounded whole-original confinement applicability path under an internal startup capability, and the old typed recovery path only for NotApplicable. Separate pure counters track confirmed confinement and audited AlreadyBlocked; existing StartupFuture completes all opaque cleanup before final activation.

**Tech Stack:** Existing Rust workspace/Tokio/SQLite/serde through existing Store ports. No dependency, schema, CLI/default, AP envelope or TypeScript change. The scoped implementation and actual qualification are complete; commands below preserve the accepted execution plan and are not instructions to rerun completed work.

**Status:** Scoped implementation completed, 2026-10-03. The actual accepted-Once predecessor, fresh protected baseline, production and qualifier source reviews, native platform runs, support checks and final source binding are recorded externally. Each platform passed 106 ordinary outer targets with 1,232 ordinary test executions plus three separately selected slow tests: 1,235 distinct executions, zero failures and eight historical default ignores; two nested CLI child summaries are excluded. The 16 new cases comprise six private, eight API and two TCP declarations. Supporting checks passed three focused metadata methods, all 114 Python tests and the contract checker with 460 named requirements. Broad56/M0/CP010 and full RFC11.4 remain open. Read the [accepted source design](../../../spec/runtime/startup-confinement-design.md) and [scoped verification record](../../../conformance/expected/startup-confinement-verification.json). Exact source/review/final freeze bindings remain private external records. User continuous build authorization covers routine scoped work; the original spec commands are reference material.

---

## File map and ownership

Five selected existing HTTP production paths only: `src/lib.rs`, `src/startup/mod.rs`, `src/startup/coordinator.rs`, `src/confinement/mod.rs`, `src/confinement/operation.rs` under `crates/b4-http`. New production: `src/startup/confinement/{mod,model,coordinator,ownership}.rs` and `src/confinement/startup.rs`. New private tests: `src/startup/confinement/{tests,test_support}.rs`. Independent qualifiers: `tests/startup_confinement.rs`, `tests/startup_confinement_tcp.rs`, `tests/startup_confinement_support/{mod,fixtures,gate,server}.rs`. No existing test or helper is an edit exception. All helper modules have zero test declarations.

Root separately owns future additive README/compatibility/requirements/checker support and new traceability test/runbook/safe receipt. No runtime/kernel/SQLite or Store DTO/port change, lifecycle/supervisor/claim/worker change, dependency/manifests/lock change, old constructor or public enum change is planned. No Git action is included in this private drafting dispatch. Implementation integration will follow the user's then-current Git instructions.

### Task 1: Close predecessor and adopt reviewed design

- [x] Finish the current accepted-Once increment's actual independent source/execution/support/final binding gates; bind its exact final freeze rather than today’s historical 709 baseline.
- [x] Have Root review the entire design/plan against source; have a different author independently review both. Resolve substantive findings in immutable private successors with exact inverses. No implementation before both semantic gates.
- [x] Create the actual fresh inventory, complete copies, protected map, selected five production/four later support exceptions and exact manifest/lock/package graph. Counts are derived; do not predict old source counts after accepted-Once additions.
- [x] Adopt only reviewed public design/plan with status/link inverses and record the before/after baseline. Assign production and independent qualifier authors separately; neither approves their own source.

### Task 2: A source-reviewed compatibility scaffold, not feature implementation

**Files:** new startup child mod/model/ownership; additive old startup registration and lib reexports. Existing supervisor/lifecycle remain protected.

- [x] Draft the five pure observation types and exact borrowed `begin_preparation_with_confinement` signature from the design. Use existing limits, StartupStartError and StartupWaitError unchanged. Do not add old enum variants or config fields.
- [x] Implement only reviewed first-poll startup registration/ownership: check limits, executor, same-mutex single attempt; construct counted carrier BEFORE resolver/authority clones; no await before supervised handoff. Unpolled borrowed futures must not own framework clones.
- [x] Add the independent per-handle `Arguments` cleanup guard before it can hold two custom-drop handles. Each take/drop has its own catch; first/replacement payloads stay under existing outer carrier/anchor. Do not drop a tuple or add a lifecycle workaround.
- [x] Forward the scaffold to the original `coordinator::prepare`, publish zero new counters, and return the new pure handle. This scaffold deliberately has the old missing-Definition/policy-denial behavior. Keep old `begin_preparation` body byte exact and retain all original source/test bodies with a literal insertion inverse.
- [x] Root and different author whole-read the scaffold before repository adoption. Then run the coordinated clean locked/offline compile. A missing API or compile failure is setup, never meaningful behavioral RED; preserve/exclude and repair narrow source setup only.

### Task 3: Independent first real positive behavioral RED

**Files:** new independent API target plus fixture/gate/support files; production paused on accepted compatibility scaffold.

- [x] Author two genuine positive cases, `opt_in_missing_exact_definition_confines_original_prior_owner` and `opt_in_denied_recover_policy_preserves_accepted_once`. Use actual SQLite admitted legal snapshots/native immutable origin/trusted fixed principal; no direct fabricated owner/revision or missing-grant Waiting fixture.
- [x] For the second fixture, commit a genuine full Once acceptance through existing approval service/AP semantics before owner closure. Capture complete original grant and acceptance records, task/input/results and journal. Denial is current Recover policy, independent of the accepted human decision.
- [x] Normally drop ALL owner aliases, reopen at observed M > original N, start Preparing with missing recorded full Definition or actual Recover denial, and call the new opt-in method. Newer executable support never counts as the old full Definition.
- [x] Capture result/counters/original whole authority/storage; finish bounded normal healthy shutdown, all joins, Weak Store/application absence and actual next writer owner M+1 BEFORE the intended feature assertion. On the scaffold, old Incomplete and zero confirmed blocks should fail only the required Complete/RecoveryBlocked assertion. Do not wait for an unreachable gated authority callback as RED.
- [x] Freeze all source, actual logs, before/after input maps and complete copies. Root + nonauthor fixture review must accept legal setup, real cleanup and intended runtime failures before production GREEN dispatch. Preserve/exclude compilation, fixture legality, timeout and cleanup defects separately.

### Task 4: Startup-capability Job adapter and original authority guards

**Files:** new `src/confinement/startup.rs`; selected operation.rs and registration only.

- [x] Write private cases from the catalogue below first, against safe source-reviewed stub behavior if needed. Capture actual runtime assertion failures on cleanly compiled tests before implementing new guards. Already-correct inherited controls need no invented RED.
- [x] Add private Job startup mode with the moved original ThreadView wrapped in a private pure Arc and exact verified Principal. The coordinator retains one pure view alias without a deep clone; no public handle retains it. The ordinary constructor defaults to the current known-ID mode. The bounded capture must match the ENTIRE original before AlreadyBlocked/applicability/policy/authority; the renewed view must match it again. No identity-only comparison is sufficient.
- [x] Register a startup-role carrier with the current capability; take capacity and same-thread claim before per-operation trusted-handle clones. Reuse owner.try_admit so the existing same-attempt/shutdown mutation barrier remains decisive.
- [x] Restrict Startup mode to ExactDefinitionUnavailable and DispatchNotAuthorized. On EACH initial and renewed startup reason observation, freshly resolve/validate the current principal against valid_principal, installed native scope and exact origin, and require exact bootstrap-Principal equality BEFORE either missing-full-Definition or Recover-policy branch. Unavailable/invalid/different principal is unchanged refusal before separate permission, never RequiredAuthorityUnavailable permission. Separate authority is required twice for an actual mutation against the same original/reason. Preserve the ordinary known-ID observer body/branch order/third reason byte and semantics; audited AlreadyBlocked retains the original bootstrap and has no new applicability/reason/permission callbacks.
- [x] Keep initial capture, one renewal and atomic recapture maximum. Return original backend TargetChanged as-is; changed returned full view/reason is ObservationChanged. Check original globally fresh turn and all twelve fields before retaining only a returned verified receipt. Do not retry or re-read after ambiguous errors to fabricate confirmation.
- [x] Share Job submission/catch/anchor teardown through a minimal private helper extraction. Preserve ordinary public confine behavior and explicit carrier-before-claim/permit/lease ordering. No supervisor/lifecycle edits; keep all existing private26 and public confinement tests byte exact.
- [x] Root and different author whole-review this adapter/core source and its actual tests; restore every selected old file with exact inverse. Do not selfapprove new HTTP production.

### Task 5: Separate coordinator and progress

**Files:** new startup confinement coordinator; old coordinator changes only five helper visibilities; original prepare function byte exact.

- [x] Reuse `increment`, `joined`, `epoch`, `page_shape`, `matches_descriptor` with visibility-only changes. Keep source error classification/readiness/owner/cursor/order/candidate limits equivalent; avoid unrelated refactoring.
- [x] Retain inherited counted ordinary `thread_view`, full descriptor and exact trusted-principal bootstrap. Document its ordinary allocation and first resolver callback as outside confinement audit limits. No new global startup heap/payload bound or turn_id/Store DTO field.
- [x] Share the original pure view with the capability adapter for bounded applicability without a deep clone. After joined Job teardown, recover its owned value with Arc::try_unwrap; unexpected alias retention faults with no cloning fallback. For NotApplicable only, carry the SAME original view/target/principal to old recover_owned after the former claim/permit retire. No newer-definition substitution, generic error-to-confinement fallback, retarget, post-adoption race handling or retry.
- [x] Count old admitted/dispositions only from actual recovery evidence. Count verified Confined, including verified receipt on fault, only in confirmed_blocks; count successful audited AlreadyBlocked only in already_blocked. Check arithmetic and publish before any later await/opaque teardown. Errors with no verified receipt have no confirmed count and do not prove rollback. For status, sample the confinement tuple before supervisor status and reread it after a Finished snapshot; wait Finished reads the tuple after the old wait returns Finished (or prove equivalent ordering), preserving tuple sum <= sampled candidates and stable final counters without supervisor/DTO changes.
- [x] Finish all confinement jobs and guarded argument disposal before emitting the pure CompletionCandidate. End epoch/readiness checks remain mandatory. Existing StartupFuture performs final fault/shutdown activation check after cleanup. Complete is still an ordinary recovery admission milestone, not Runnable callback settlement.
- [x] Freeze complete production/private source, all actual minimal changes/inverses and actual RED/GREEN logs. Root whole SPEC and different-author whole QUALITY must pass before independent complete qualifier execution and broader qualification.

## Exact proposed declaration catalogue

Six private declarations in NEW `src/startup/confinement/tests.rs`:

| Function | Specific new invariant |
| --- | --- |
| `borrowed_start_future_has_no_framework_alias_before_poll` | Unpolled borrowed future holds no trusted Arc clone; first poll without executor refuses without registration/callbacks. |
| `start_limits_and_single_attempt_precede_argument_clones` | Invalid limits before registration; old/new share one attempt; shutdown cut does not open ordinary admission. |
| `startup_job_requires_whole_original_and_exact_principal` | Initial and renewed full-original contradictions/changed principal refuse before permission/mutation; custom capture contract violations fail closed. |
| `startup_arguments_dispose_two_panicking_handles_under_one_anchor` | Independently caught resolver/authority destructors and first/replacement held Store-bearing payloads; no double panic, no false quiescence. |
| `startup_progress_counts_verified_receipt_but_not_ambiguous_commit` | Actual commit-then-Err/panic contributes zero confirmed evidence; verified returned receipt survives later cleanup fault and contributes one; no rollback assertion. |
| `final_startup_capture_drop_cannot_activate_or_escape_ownership` | Last coordinator/Shared capture destructor retains real Store+application under counted anchor until actual release; final fault forbids activation. |

Eight independent API declarations in NEW `tests/startup_confinement.rs`:

| Function | Specific result |
| --- | --- |
| `opt_in_missing_exact_definition_confines_original_prior_owner` | Actual prior-owner SQLite missing full Definition, with newer version present, blocks original exactly; zero native/input/output callbacks; Complete with confirmed_blocks1 and admitted0. |
| `opt_in_denied_recover_policy_preserves_accepted_once` | Genuine consumed Once/acceptance/ready intent preserved under current denial, separate permission, original-derived one blocked transition/journal. |
| `audited_prior_block_counts_without_adoption_or_rewrite` | Actual prior RecoveryBlocked is completely audited and unchanged, already_blocked1/admitted0, no confinement-authority callback. |
| `default_preparation_refusal_remains_unchanged` | Separate actual directories exercise old begin_preparation for the selected missing-definition and denied-policy fixtures; unchanged Incomplete/state and unchanged old counts. |
| `available_allowed_record_uses_original_typed_recovery` | Original exact available definition/allow policy reaches real Runnable admission/callback hold; confine counters0 and correct old admission counts; no latest fallback. |
| `unresolved_principal_and_denied_confinement_authority_refuse` | Finite source-valid resolver missing/mismatch and independent Denied/Unavailable cases; no anonymous authority, unchanged full data, static incomplete. |
| `unsupported_bounded_capture_and_changed_target_fail_closed` | Actual default-unsupported custom Store, explicit authority-profile refusal and whole original/renewal change cases; zero mutation, no unbounded fallback or retarget. |
| `bounded_opt_in_scan_stops_without_extra_mutations` | Three legal prior candidates, max_recoveries2, truthful partial progress and Incomplete LimitExceeded; no third callback/audit/write. |

Two independent real TCP declarations in NEW `tests/startup_confinement_tcp.rs`:

| Function | Specific result |
| --- | --- |
| `preparing_listener_stays_closed_through_confinement_authority_and_cleanup` | Actual held separate permission/capture cleanup: health200, readiness503, ordinary503 before/during; after successful cleanup Complete and ReadThread200. Blocked ReadState remains actual501, not forced200. |
| `mixed_recovery_and_confinement_activate_with_separate_counts` | Actual two prior records, one confined/one held Runnable: candidates2, old admitted1/runnable1/blocked0, confirmed_blocks1. Readiness opens only after confinement cleanup; Runnable callback may remain owned until explicit return. |

These 16 declarations are a proposed source catalogue, not pass credit or a prediction that coverage suffices. Keep finite gated helpers, ≤15s awaited control work/≤20s thread joins/≤120s whole test watchdogs unless actual source/cap evidence justifies a reviewed change. A timeout is failure, never permission or retirement. Every gate classifies entered, explicit release, returned and expired separately; positive witnesses require nonexpired explicit release and actual joined return. Negative cleanup is recorded as cleanup, not a success proof. No existing private test is counted again through a wrapper/helper.

### Task 6: Actual focused qualifier and compatibility commands

Only after exact reviewed sources are adopted and frozen, execute coordinated future commands (no concurrent author Cargo):

* `cargo test --locked --offline -p b4-http --lib startup::confinement::tests -- --test-threads=1` — require all actual declared private cases executed with truthful controls/failures; filtered old tests are not new credit.
* `cargo test --locked --offline -p b4-http --test startup_confinement -- --test-threads=1`.
* `cargo test --locked --offline -p b4-http --test startup_confinement_tcp -- --test-threads=1` — loopback escalation follows existing explicit local test authorization; preserve any environment denial as setup history.
* `cargo test --locked --offline -p b4-http --test startup_preparation --test startup_preparation_tcp --test recovery_confinement -- --test-threads=1` and the then-current accepted-Once/restart targets unchanged. Their actual names/counts come from frozen source/logs, not this historical plan.
* `cargo test --locked --offline -p b4-http --lib` and `cargo clippy --locked --offline -p b4-http --all-targets -- -D warnings`.
* Owned-file-only `rustfmt --edition 2021 --config skip_children=true --check` on the five selected and new Rust paths; do not cargo-fmt shared protected source.

- [x] Before/after every command, bind exact all current inputs and compiled context copies; verify protected old tests, five complete reversible selected-file deltas, manifests/lock and package graph. Record setup/style corrections and failures accurately; fresh source changes invalidate stale command applicability.
- [x] Independently inspect complete logs and actual test-name sets. Actual authored counts override the proposed16; no helper selectors or nested historical summaries count as behavioral tests.
- [x] For positive physical proofs drop every caller/framework alias, require explicit normal worker/gate/listener/coordinator returns, healthy Quiescent where success expects health, real Weak Store/application absence, and successful real next-writer owner=current+1. Do not label process/server abort, forced cleanup or timed wait as retirement.

### Task 7: Frozen actual broad platforms and support

- [x] Root captures all-source frozen inputs and accepts whole production/qualifier SPEC+different-author QUALITY. Then run actual broad macOS and isolated pinned Linux qualification from unchanged frozen sources; no network, no mounts, nonroot/cap-drop containment according to reviewed driver. Count actual names and disclose historical ignores/nested exclusions; no predicted totals.
- [x] Root authors minimal additive docs/requirements/checker + new runbook/safe receipt with scoped factual evidence. New traceability metadata has meaningful separately executed RED/GREEN and no Rust credit. Preserve every old requirement/status/group including broad56/M0/CP010 and current accepted-Once/two-lane scopes.
- [x] Different authors whole-review actual support, privacy, counts, inverse prefixes and source/evidence applicability before promotion. Keep full RFC11.4, actor reconstruction, automatic CLI readiness, delete/reincarnation qualification and original TypeScript scope open.
- [x] Run actual post-support Python/contracts and final narrow status bridge only after all semantic gates. Capture external immutable final source freeze without cyclic self-hash. No current/private draft checkbox is checked in advance.

## Plan acceptance notes

The accepted source design and plan were adopted after the actual accepted-Once final freeze and fresh baseline. Production and independent qualifiers had separate authors and passed source and execution review. The proposed catalogue above is retained as planning history; exact final declaration names and scoped outcomes are in the verification record and external completion bindings. The final narrow status/checkbox bridge preserves all other plan requirements and source scope. An immutable external source freeze binds the final public files without a cyclic self-hash. Whole native source and Cargo inputs remain those qualified on both platforms; the two documentation status changes add no new runtime credit. Broader scopes remain open as stated above.
