# T-017 Heartbeat and Split-Stream Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute this plan task-by-task, with fresh authors for mutation tasks and independent source, quality, integrated and public-integrity reviews. Steps use checkbox syntax.

**Goal:** Qualify one bounded native TCP/owned-SQLite T-017 stream whose quiet heartbeat and Unicode application frames decode identically under deterministic arbitrary byte cuts.

**Architecture:** Hold an opt-in `RunStream` callback long enough to observe an actual `: ping\n\n` before selected Unicode publication. Capture the complete raw HTTP response from one real loopback connection, validate its true event sequence and durable result, then replay its bytes through an independent bounded incremental HTTP/chunk/SSE/UTF-8 decoder at every single cut and byte-wise delivery. Promote only T-017 after reviewed current-map proof; current-HEAD TypeScript `COMPAT` is a separate lane.

**Tech stack:** Rust 1.93.1, Tokio/Axum, owned SQLite, pinned Python 3.12.12 `-B`, locked offline Cargo, full source maps, existing reviewed release-controller pattern. No package/schema/dependency change or Git write is planned.

**Design:** `docs/superpowers/specs/2026-10-25-seventeenth-release-heartbeat-splits-design.md`, independently approved in `.b4-work/full-rust-rewrite-scope/t017-design-review.md`. The full-runtime program design and catalog plan are separately approved. The exact accepted predecessor is `.b4-work/sixteenth-release-viewer-overflow/root-acceptance.json` plus `post-acceptance-source-descriptor.json`.

## Exact source scope and arithmetic

Add only `crates/b4-http/tests/heartbeat_splits_tcp.rs` for the runtime acceptance proof. Its exact named test is `release_t017_heartbeat_and_split_utf8_tcp_preserve_one_terminal`. Later change only the established sixteen metadata paths: `README.md`, `spec/runtime/compatibility.md`, `spec/runtime/requirements.yaml`, `scripts/check_contracts.py`, and the twelve established `scripts/test_*traceability.py` modules named by the T-016 plan. No production edit is anticipated. A real behavior contradiction requires design amendment and renewed review before production repair.

The baseline-to-native delta is one added test plus sixteen metadata modifications, no removal. The final public tree adds the qualification and README at `conformance/release/t017-heartbeat-splits/`. Eighteen backups cover sixteen original metadata paths plus the two reviewed T-017 documents. Protect the thirty-two T-001–T-016 acceptance and qualification paths. Setup records actual counts and maps; do not rely on predicted counts because the approved full-runtime program documents and any separately reviewed catalog publication may precede setup.

## Task 0: reviewed preparation and controllers

- [x] Bind the accepted T-016 status/scope, post-acceptance public descriptor, final T-017 design/plan and their independent reviews by exact hash. Before setup, require a complete public map equal to the accepted post map plus **exactly four** reviewed additions: `docs/superpowers/specs/2026-10-25-full-rust-runtime-parity-design.md`, `docs/superpowers/plans/2026-10-25-full-runtime-feature-catalog-plan.md`, `docs/superpowers/specs/2026-10-25-seventeenth-release-heartbeat-splits-design.md`, and this plan. Record each path/hash/review in setup; preserve every preexisting T-016 byte, including `spec/runtime/requirements.yaml`. Keep the catalog inventory and its optional validation script private until after T-017 acceptance. If any other publication occurs before setup or after setup but before adoption, stop; review the exact changed-path/hash delta and freshly rebind baseline, controllers and affected reviews before copying. Require the T-017 acceptance and generated qualification/README paths absent at setup.
- [x] Create an isolated private base `.b4-work/seventeenth-release-heartbeat-splits/` with a fresh workspace, baseline and shared external build-target pointer. Do not import T-016 superseded pre-amendment material or old runtime result credit.
- [x] Freshly author bounded setup, runner, qualifier, manifest freezer, adopter, public checker and post-copy collector against the final source/review hashes. Independently review setup/runner, qualification and copy-controller groups before executing any controller. Use pinned tools, exact roots, full maps, positive child PID/process group, new sessions, 600-second child deadlines except 60-second whole public checker, monotonic TERM grace, guarded KILL/reap and Root maximum three active children.

## Task 1: source-only acceptance proof

- [x] A fresh author writes only `heartbeat_splits_tcp.rs`, with one opt-in approval-free `Pure` task. Ordinary/controlled callback entries panic. Install all finite gates and panic releases before POST `RunStream` admission. Freeze route/input, selected Unicode `chunk`, task result, aggregate writes, event names and expected JSON independently of callback and wire data. Derive expected trusted `tool_call` and `tool_result` from the separately read durable attempt ID and assert its identity/serialization premise.
- [x] Set a finite positive heartbeat interval and ample queue/digest/snapshot/input/output/depth/capacity bounds. While the callback is held after the complete trusted `tool_call`, observe exact raw `: ping\n\n` on the real TCP response before selected publication. At least one heartbeat is required; repeated legal preterminal comments are allowed. Positively assert the callback and durable `Running/InFlight` state remain held without a result.
- [x] Publish the frozen Unicode chunk and require `NativeChunkResult::Accepted`, release the callback, consume the same response through EOF, and require exactly `tool_call`, `chunk`, `tool_result`, `done` in order as the only application events. Require independent durable result equality; reject duplicate/extra/reordered application events, postterminal comments/data, missing terminal, malformed fields and transport framing. EOF alone never proves settlement.
- [x] Preserve complete raw HTTP bytes. Validate actual status, content type, cache/buffering headers, transfer framing, complete chunks and SSE syntax with a test-owned bounded byte parser. Assert the selected non-ASCII UTF-8 bytes appear in the JSON data field while quotes/newline/backslash remain escaped within that field. Reject invalid UTF-8, duplicate JSON keys, unknown fields, trailers and trailing bytes.
- [x] Replay the same captured raw HTTP transcript through the independent incremental decoder for all `N+1` two-piece byte cuts and one-byte-at-a-time delivery, including explicit cuts inside every multi-byte scalar, chunk boundary, CR/LF and SSE labels. Assert each replay against independently frozen expected events, durable attempt identity, required preterminal heartbeat and terminal/EOF. Do not call the server encoder/`Frame::new`, derive expected payloads from the first parse, or assert an OS packetization schedule.
- [x] Verify exact typed/raw SQLite histories, one callback/attempt, durable completed success and aggregate result. Retire server, destroy sole store owner, reopen at owner+1 and use GET-only checks to exclude replay. Every socket, parser, gate, poll, join and cleanup operation has a finite bound.
- [x] From the isolated workspace, run pinned rustfmt directly on only the new test with `--edition 2021 --config skip_children=true`. Prove the full baseline-plus-only-test map, then freeze `task1-frozen-snapshot.json` with every source hash. Obtain independent complete source review before execution.
- [x] Through the reviewed runner, execute the exact named test with locked/offline Cargo, `--nocapture --test-threads=1`, then strict `cargo clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings`. Require one pass/zero failures, reaped/no-timeout/no-drift receipts on the frozen map. Mark initial proof and early lint `additional_runtime_credit:false`. Obtain independent quality/results approval before metadata mutation.

## Task 2: metadata and guard sensitivity

- [x] A fresh author promotes only T-017 to `NATIVE`, scope `native_heartbeat_and_split_utf8_tcp_sqlite`, exact acceptance path/name and `conformance/release/t017-heartbeat-splits/qualification.json`. Advance headlines to seventeen scoped proofs, thirty-nine pending and T-018 next. Keep M0, CP-010, COMPAT, full core, current-HEAD TypeScript, arbitrary network scheduling and prior release limits open.
- [x] Before editing `scripts/check_contracts.py`, run the fixed core guard suite against the new metadata and record an honest old-policy RED that T-017 must remain pending. This is metadata sensitivity, never runtime credit.
- [x] Extend the checker by one exact ordered T-017 policy: named scenario, complete new target, native-stream API/control, HTTP library, T-001–T-016 regressions, format, strict dual-crate lint and guards. That is 24 categories with only the first 21 carrying runtime credit. Bind typed distinct initial/final maps and exact acceptance source. Preserve 117 prior guard meanings; add seven focused T-017 methods for exact acceptance, missing receipt, wrong identity/broad fields, aliased initial map/proof, command/process/drift/credit mutations, wrong acceptance path/name/hash and changed/missing source declaration. Expected core guard count: 124. Do not add `feature_validation` or COMPAT credit.
- [x] Run pinned Python 3.12.12 `-B scripts/test_core_release_traceability.py` to GREEN, require `Ran 124 tests` and `OK`, then freeze `native-source-descriptor.json` with the full current 1-added/16-modified map. Obtain independent metadata source and quality/results approvals.

## Task 3: final qualification and delivery

- [x] Run all 24 ordered final categories once on the final descriptor with the reviewed runner and at most three active children, using the exact command policy below. Require current full before/after maps, exit zero, reaping, no timeout/history transfer/drift and positive expected summaries. Keep the initial named proof separate with no added final runtime credit.
- [x] Run the reviewed qualifier to create `delivery/qualification.json` only after validating every selected receipt/log/map and 21-runtime boundary. Stage a narrow delivery README and byte-identical reviewed design/plan copies. Qualification states one fixed healthy transcript and its deterministic partitions, with no universal network-schedule, current-HEAD COMPAT, core, CP-010 or whole-product claim.
- [x] Obtain independent integrated-results and delivery reviews. Reject stale/repeated results, transferred initial credit or prose broader than observed behavior.

## Task 4: semantic manifest and reversible adoption

- [x] Root reads and decides the exact eleven report roles: design review, plan review, setup/runner controller review, qualification-controller review, Task 1 source review, Task 1 quality review, Task 2 metadata source review, Task 2 quality review, copy-controller review, integrated-results review, and delivery review. Each report binds final document/controller hashes as applicable. Freeze seven exact artifacts: reviewed qualifier source, adopter source, public-check controller source, actual delivery qualification, actual delivery README, byte-identical reviewed design copy, and byte-identical reviewed plan copy. Root's explicit report/hash decisions precede freezer execution; an independent semantic manifest review precedes Root's manifest decision, and that decision precedes adoption.
- [x] Require complete current public equality to setup baseline and complete workspace equality to native. Create and verify all eighteen original backups before the first public mutation. Copy the exact seventeen-path source delta and prove public equals native, then install exactly two generated T-017 files. Require native-plus-two public equality, unchanged thirty-two protected predecessor paths, stable private workspace and preserved manifest hashes.

## Task 5: public verification and acceptance

- [x] Run exactly thirteen public metadata checks using the exact policy below: whole checker under 60 seconds and the twelve established traceability modules under 600 seconds, each with full unchanged public maps, successful logs, reaped/no-timeout children and no runtime credit. Collect exactly one current receipt per category.
- [x] Independently review seventeen-path delta, eighteen backups, thirty-two protected paths, native/public maps, all 24 final checks, initial proof, eleven reports, seven artifacts, public receipts, narrow claim and open core/COMPAT/M0/CP-010.
- [x] Root records T-017 acceptance only after reading approved public integrity. Close only completed plan tasks, leave T-018/COMPAT continuation open, then capture the post-acceptance public source descriptor before later documents.

## Task 6: continue

- [ ] Continue the separately reviewed full-runtime catalog and M0/CP-010 work, then T-018, without treating a T-017 native proof as whole-core or TypeScript compatibility.

## Exact final command policy

`CARGO` below is `/Users/blove/.rustup/toolchains/1.93.1-aarch64-apple-darwin/bin/cargo`; `PYTHON` is `/Users/blove/.pyenv/versions/3.12.12/bin/python3`. Run all Cargo commands in the isolated T-017 workspace with the reviewed external target and offline environment. The labels and vectors are ordered and normative:

```text
01 scenario-test              CARGO test -p b4-http --test heartbeat_splits_tcp release_t017_heartbeat_and_split_utf8_tcp_preserve_one_terminal --locked --offline -- --nocapture --test-threads=1
02 heartbeat-splits-tests     CARGO test -p b4-http --test heartbeat_splits_tcp --locked --offline -- --test-threads=1
03 native-stream-api-tests    CARGO test -p b4-runtime --test native_streaming --locked --offline -- --test-threads=1
04 native-stream-control-tests CARGO test -p b4-http --test native_streaming --locked --offline -- --test-threads=1
05 http-control-tests         CARGO test -p b4-http --lib --locked --offline -- --test-threads=1
06 t001-regression            CARGO test -p b4-http --test native_tcp --locked --offline -- --test-threads=1
07 t002-regression            CARGO test -p b4-http --test thread_state_tcp --locked --offline -- --test-threads=1
08 t003-regression            CARGO test -p b4-http --test thread_admission_tcp --locked --offline -- --test-threads=1
09 t004-regression            CARGO test -p b4-http --test thread_route_tcp --locked --offline -- --test-threads=1
10 t005-regression            CARGO test -p b4-http --test thread_json_tcp --locked --offline -- --test-threads=1
11 t006-regression            CARGO test -p b4-http --test input_shape_tcp --locked --offline -- --test-threads=1
12 t007-regression            CARGO test -p b4-http --test thread_concurrency_tcp --locked --offline -- --test-threads=1
13 t008-regression            CARGO test -p b4-http --test different_thread_tcp --locked --offline -- --test-threads=1
14 t009-regression            CARGO test -p b4-http --test wait_output_tcp --locked --offline -- --test-threads=1
15 t010-regression            CARGO test -p b4-http --test stream_payload_tcp --locked --offline -- --test-threads=1
16 t011-regression            CARGO test -p b4-http --test terminal_frame_tcp --locked --offline -- --test-threads=1
17 t012-regression            CARGO test -p b4-http --test disconnect_completion_tcp --locked --offline -- --test-threads=1
18 t013-regression            CARGO test -p b4-http --test attachment_state_tcp --locked --offline -- --test-threads=1
19 t014-regression            CARGO test -p b4-http --test attachment_cut_tcp --locked --offline -- --test-threads=1
20 t015-regression            CARGO test -p b4-http --test attachment_truncation_tcp --locked --offline -- --test-threads=1
21 t016-regression            CARGO test -p b4-http --test viewer_overflow_tcp --locked --offline -- --test-threads=1
22 format-check               CARGO fmt --all -- --check
23 scoped-lint                CARGO clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings
24 guard-tests                PYTHON -B scripts/test_core_release_traceability.py
```

Only positions 01–21 carry runtime credit. Only `guard-tests` is metadata-only. Formatting, lint and guards carry no runtime credit. The initial named proof is distinct and supplies no additional final credit.

## Exact public command policy

Run `PYTHON -B scripts/check_contracts.py` once under a 60-second deadline, then for each of the twelve filenames below run `PYTHON -B -m unittest discover -s scripts -p FILENAME` once under a 600-second deadline. The ordered filenames are `test_accepted_once_startup_traceability.py`, `test_cli_startup_recovery_traceability.py`, `test_diagnostic_traceability.py`, `test_historical_native_definitions_traceability.py`, `test_native_cooperative_cancellation_traceability.py`, `test_prior_owner_confinement_traceability.py`, `test_process_startup_restart_traceability.py`, `test_startup_confinement_traceability.py`, `test_startup_recovery_preparation_traceability.py`, `test_unchanged_ap_client_traceability.py`, `test_unfinished_discovery_traceability.py`, and `test_core_release_traceability.py`. These thirteen commands are metadata-only and have no runtime credit.
