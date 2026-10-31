# Normal Native Stream Terminal Implementation Plan

Use the subagent-driven-development workflow: a fresh test author, a separate metadata author, and independent source/spec then quality/results gates. User autonomy authorizes routine reversible work; no Git operations, routine permission pauses or milestone final responses.

Goal: independently qualify the selected four normal native terminal cases for T-011 without changing production behavior.

Architecture: use T-010's existing opt-in native streaming host and default callback delegation. One new real-TCP/owned-SQLite acceptance test checks complete stream closure and independently fixed terminal shapes. Existing production/API/control bytes and all twenty prior acceptance-test/receipt paths remain untouched.

## Exact source and process policy

Test task adds only crates/b4-http/tests/terminal_frame_tcp.rs, exact release_t011_normal_native_streams_end_with_one_done. Metadata task modifies only scripts/check_contracts.py, scripts/test_core_release_traceability.py, spec/runtime/requirements.yaml, README.md, spec/runtime/compatibility.md and the eleven established headline scripts. No other Rust/source/fixture/dependency/kernel/schema edits. Final baseline delta17paths with1addition and no removal. Eighteen first backups cover16existingmetadata and two published design/plan documents; do not create a backup for the new test. Preserve all existing baseline bytes outside that exact delta, especially T-010 production, API/control/wire tests, its separate feature proof and all20prior test/receipt paths.

Published documents are docs/superpowers/specs/2026-10-21-eleventh-release-terminal-frame-design.md and docs/superpowers/plans/2026-10-21-eleventh-release-terminal-frame-plan.md. Stage delivery copies as eleventh-thread-design.md/eleventh-thread-plan.md. Expected baseline1160/native1161/public1163 only if actual T-010 acceptance1158 plus these2published documents agrees. Use actual inventories including ignored vendor/rusqlite/Cargo.lock. Ordinary private copy only; public freezes during authoring/checks, .git stays read-only, shared target remains outside source.

Controllers pin Rust1.93.1, locked/offline Cargo, current commands only, before/after complete maps, actual log/receipt hashes and exit/reaping/timeout/no-history records. Every native/format/lint/guard step600seconds. On timeout TERM the original process group, preserve a full monotonic5second grace even if leader exits, guarded group KILL and final reap. Source freezes for each execution. Poll <=45seconds, useful user updates <=60seconds. No prior runtime result transfer.

## Task0: reviewed preparation

- [x] After actual T-010 Root acceptance, independently approve design and plan, publish exact reviewed docs, prepare/review setup/fixed-step qualifier/adopter/manifest/public-metadata collectors, and capture an equal isolated baseline. Freeze exact17delta/18backups/20protected paths before source authors run.

Qualification uses separate initial new-test map and final metadata map, no receipt cycle. Final policy must contain17 categories below, not mechanically reuse T-010's16policy or production27delta adopter. There is no new full-workspace feature gate for this test/metadata-only increment; T-010 history remains independently bound and unmodified.

## Task1: independent terminal proof

- [x] Dispatch a fresh author for only the new acceptance file. Root formats only it, freezes task1-frozen-snapshot.json, obtains source approval, runs honest first native proof, records task1-native-results.json and obtains quality/results approval.

One async acceptance case exercises four distinct approval-free Pure routes sequentially under one immutable opt-in application/host: structured Value, null Value, Void, known failure. Record original validator input, genuine callback entry/return and exact invocation/context for each. Default streaming delegation may be used; no chunks or forged lifecycle frames are necessary. Positive execution and independent literals must distinguish result/output/null/void/failure semantics.

Read every full real TCP response through EOF with finite size and whole-request deadlines, preserving bytes read with headers. Strictly parse AP grammar/JSON, explicitly recognize legal heartbeat comments and reject malformed/extra frames. Successful cases expect trusted tool_call/tool_result/done; known failure expects tool_call/done with existing committed failure shape and no successful tool_result. Require exactly one done per response and that no application frame follows it. Structured done.output is its independent value, null Value is {output:null}, Void is {}, known failure uses the existing done.output.error message. Do not use encoder fixtures or received bodies to derive expectations; no quiet-time or exact packet-boundary claim.

Require exact independent typed/durable task results, states/statuses and complete7/5/9raw rows including creation journals, provenance/revisions/attempts and bounded monotonic timestamps. Trace stable source before fixing failure journal/output literals. Distinguish aggregate state from task output, including null/Void. No test hooks or existing helper edits. Healthy quiescence precedes fixture listener abort/join; destroy actual sole owner synchronously, reopen at owner+1, fresh GET-only observations of all4 histories, no replay or mutation, then positive cleanup. All gates/requests/lifecycle waits finite. Existing behavior may pass on first run; never manufacture RED.

## Task2: metadata and bounded guards

- [x] Dispatch a separate fresh author for only16metadata paths. Obtain honest metadata-only RED/GREEN, then independent source and quality approval before final checks.

Promote T-011 only to NATIVE/implemented/passed_native_scope/implemented_native_scope, scope native_normal_stream_terminal_tcp_sqlite, exact new test and conformance/release/t011-normal-stream-terminal/qualification.json. Eleven proofs/45pending; preserve ten prior rows/descriptors/5–16policies/receipts, twenty prior acceptance paths and T-010 feature_validation including final-alias rejection. Retain75prior guard meanings and add7bounded T011 guards(82expected). Eleven headline scripts get literal-only count/list changes; all other assertions unchanged. Keep M0/core/COMPATopen, CP001–009partial/CP010unresolved and historical foundation counts unchanged.

New public schema checks actual delivered T011 receipt shape/identity/test hash, initial-proof no-extra-credit, exact17command policy and credit flags without recomputing a checkout digest. Do not invent a T011 full-feature field or copy T010's historical feature object into a new claim. New negatives are bounded field/command/path/credit mutations with an accepted valid baseline fixture. No whole checker until the actual receipt is delivered. Fixed guard target uses pinned Python -B, metadata-only.

## Task3: frozen final verification and receipt

- [x] Root freezes/reviews native-source-descriptor.json, runs all17fresh fixed checks on that map, generates qualification only afterward, and obtains integrated actual-results approval.

Ordered final categories:
1 scenario-test: cargo test -p b4-http --test terminal_frame_tcp release_t011_normal_native_streams_end_with_one_done --locked --offline -- --nocapture --test-threads=1
2 terminal-frame-tests: complete terminal_frame_tcp target
3 native-stream-api-tests: complete b4-runtime native_streaming target
4 native-stream-control-tests: complete b4-http native_streaming target
5–14 t001-regression through t010-regression: complete native_tcp, thread_state_tcp, thread_admission_tcp, thread_route_tcp, thread_json_tcp, input_shape_tcp, thread_concurrency_tcp, different_thread_tcp, wait_output_tcp, stream_payload_tcp targets
15 format-check: cargo fmt --all -- --check
16 scoped-lint: cargo clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings
17 guard-tests: pinned Python -B scripts/test_core_release_traceability.py

Only first14categories carry runtime credit. Formatting/lint and82authored guards add no native credit. Report actual summary executions, not unique inventory. Named initial proof retains its own map and no duplicate credit. Earlier T010 whole-feature proof is historical and not a new full-suite result on later bytes. Fix actual failures with narrow debugging and fresh source/results gates; do not broaden matrices.

## Task4: exact delivery and reversible copy

- [x] Freeze/review seven artifacts and actual eleven-review manifest semantically, then adopt only the reviewed bytes with complete backups and equality checks.

Required actual reports: design-review, plan-review, controller-review, qualification-controller-review, task1-source-review, task1-quality-review, task2-source-review, task2-quality-review, copy-controller-review, integrated-results-review, delivery-review (all .md). Seven artifacts: qualifier/adopter/public-metadata controller source plus generated qualification/actual README/reviewed design/reviewed plan. Read final decisions/scopes and any earlier corrections, not an APPROVED substring alone. No unresolved pre-copy conditions. Complete public baseline and private native equality must precede copying. Back up18existinginputs, adopt exact17delta, prove every native byte equal BEFORE adding generated proof/prose, then preserve exact public native+2 map. Both generated destinations must be new.

## Task5: public metadata and Root acceptance

- [x] Run whole public checker and twelve established modules, aggregate actual records, obtain independent public integrity review, and write Root acceptance before closing completed plan boxes.

No runtime credit for public metadata. Whole checker60second bound, each module600seconds, max3active, finite group cleanup and source unchanged maps/log/command/status/reaping evidence. Start long CLI/accepted-Once/startup-confinement modules early and refill slots promptly. Twelve memberships unchanged: accepted_once_startup, cli_startup_recovery, diagnostic, historical_native_definitions, native_cooperative_cancellation, prior_owner_confinement, process_startup_restart, startup_confinement, startup_recovery_preparation, unchanged_ap_client, unfinished_discovery, core_release (test_*_traceability.py). Collector validates one actual receipt per module plus whole checker. Independent integrity checks exact18backups/17delta/protected20/native/public maps/actual17checks/all approvals/no scope drift. Root writes acceptance, closes first6of7task checkboxes and captures post-accept source before publishing next docs.

## Task6: continue

- [ ] Continue T-012 unless the user steers or a genuine manual action/approval is required. Do not stop for a milestone confirmation or unsupported Git operation.
