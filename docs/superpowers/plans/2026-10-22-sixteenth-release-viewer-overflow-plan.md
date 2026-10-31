# Viewer Capacity and Queue Overflow Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute this plan task-by-task, with a fresh author for each mutation task and independent source then quality/results review.

**Goal:** Qualify selected T-016 behavior by proving that one authorized attached-viewer capacity refusal and one attached-viewer byte-queue overflow do not stop or corrupt the same authoritative native TCP/SQLite run.

**Architecture:** Add one dedicated acceptance test over the existing bounded viewer implementation. The test uses a held native callback, an individually oversized live frame, and an independently sized two-frame `tool_result`/`done` burst to remove socket scheduling from the oracle. No production change is planned; a genuine contradiction requires design amendment and re-review before repair.

**Tech Stack:** Rust 1.93.1, Tokio, existing native HTTP/runtime and owned SQLite, pinned Python 3.12.12 `-B`, locked offline Cargo, existing release controllers and source-map discipline.

---

## Exact files and evidence policy

Create `crates/b4-http/tests/viewer_overflow_tcp.rs` with exact test
`release_t016_viewer_capacity_and_queue_overflow_do_not_stop_run`.

Modify only the established sixteen metadata paths after the initial acceptance proof:

- `README.md`
- `spec/runtime/compatibility.md`
- `spec/runtime/requirements.yaml`
- `scripts/check_contracts.py`
- `scripts/test_core_release_traceability.py`
- `scripts/test_accepted_once_startup_traceability.py`
- `scripts/test_cli_startup_recovery_traceability.py`
- `scripts/test_diagnostic_traceability.py`
- `scripts/test_historical_native_definitions_traceability.py`
- `scripts/test_native_cooperative_cancellation_traceability.py`
- `scripts/test_prior_owner_confinement_traceability.py`
- `scripts/test_process_startup_restart_traceability.py`
- `scripts/test_startup_confinement_traceability.py`
- `scripts/test_startup_recovery_preparation_traceability.py`
- `scripts/test_unchanged_ap_client_traceability.py`
- `scripts/test_unfinished_discovery_traceability.py`

Publish this reviewed design and plan at their current paths. The accepted T-015 public map has 1,183 files. The anticipated setup baseline is therefore 1,185 files after these two documents; native source is 1,186 after the new acceptance file; generated T-016 qualification and README produce 1,188 public files. Setup must capture actual maps before enforcing these predicted counts.

Baseline-to-native must be exactly seventeen paths: one added test and sixteen metadata modifications, no removal. Eighteen original backups cover the sixteen metadata files and the two published documents. Protect all thirty predecessor acceptance/qualification paths and every other baseline byte. Use one shared external build target, no Git writes, no dependency or schema changes, and no feature/full-workspace result.

## Task 0: reviewed preparation and bounded controllers

- [x] **Step 1: Bind the approved design**

Record the exact hashes of:

- `docs/superpowers/specs/2026-10-22-sixteenth-release-viewer-overflow-design.md`
- `.b4-work/t016-spec-review.md`
- `.b4-work/fifteenth-release-attachment-truncation/root-acceptance.json`
- `.b4-work/fifteenth-release-attachment-truncation/post-acceptance-source-descriptor.json`

Require T-015 status `accepted`, scenario `T-015`, and the accepted public map before any T-016 setup or source mutation.

- [x] **Step 2: Independently review this plan**

The reviewer checks the complete design binding, exact files, qualification-only logic, initial/final evidence separation, commands, map arithmetic, controller gates, review roles, reversible copy, and public verification. Amend and re-review until approved.

- [x] **Step 3: Retire every pre-amendment preparation artifact**

Treat the original seven controllers, all three controller reviews, `workspace/`,
`baseline.json`, and `build-target` as superseded because they bind the rejected
RunWait architecture or its document hashes. Preserve the generated workspace,
baseline, and build-target reversibly under unambiguous
`superseded-pre-amendment-*` names before any replacement setup. They provide no
T-016 evidence, and no source file from the incomplete workspace may be imported
into the replacement workspace.

- [x] **Step 4: Freshly author and review seven private controllers**

Under `.b4-work/sixteenth-release-viewer-overflow/controller/`, create reviewed equivalents of:

- `setup.py`
- `run_step.py`
- `qualify.py`
- `freeze_manifest.py`
- `adopt.py`
- `public_metadata.py`
- `collect_postcopy.py`

The fresh controller source must bind the final amended design, plan, and both
approved review hashes. Its qualification language must describe the RunStream
producer, deliberate primary disconnect, replacement terminal sequence, and
independent durable-result read, while granting no credit to unobserved
disconnected-primary sender behavior. Independently renew the setup/runner,
qualification, and copy-controller reviews against all seven final controller
files. Every renewed review must bind the final document and controller hashes.

Controllers use exact absolute roots, pinned tools, full source maps, log/receipt hashes, positive PIDs/process groups, `start_new_session`, 600-second child deadlines except the 60-second public whole checker, monotonic five-second TERM grace, guarded KILL, reaping, no historical transfer, and a Root maximum of three active children. No replacement controller may execute until all three renewed controller reviews approve it.

## Task 1: isolated setup

- [x] **Step 1: Capture the actual accepted public baseline**

Run the freshly reviewed replacement setup controller only after plan and all
three renewed controller approvals. It must create a new `workspace/`,
`baseline.json`, and `build-target`; reusing or modifying any superseded setup
artifact is forbidden. Inventory every public file excluding `.git/**` and
`.b4-work/**`, including `vendor/rusqlite/Cargo.lock`. Require exact equality
with the accepted T-015 post-acceptance descriptor plus only the two reviewed
T-016 documents at their approved hashes.

- [x] **Step 2: Create the private workspace**

Copy the complete public source into `.b4-work/sixteenth-release-viewer-overflow/workspace`, record `baseline.json`, and bind `build-target` to the reviewed shared build target. Prove complete public/workspace/baseline equality and absence of the new acceptance and generated T-016 receipt paths before authorship.

## Task 2: dedicated acceptance proof

- [x] **Step 1: Fresh author writes only the acceptance file**

Create only `crates/b4-http/tests/viewer_overflow_tcp.rs`. Start one opt-in
approval-free `Pure` callback through POST `RunStream`, the only operation that
selects the native streaming entry; ordinary and controlled entries panic.
Install all finite gates and panic releases before admission. Completely validate
the primary stream's exact trusted `tool_call`, then deliberately disconnect that
client before the selected oversized frame. Do not infer when the server drops
its internal sender and do not claim later primary-viewer overflow or terminal
behavior.

Freeze test-owned input, route, task/effect, `tool_call`, selected live
event/payload, task result, aggregate writes, `tool_result`, and `done`. Observe
the actual attempt ID independently from durable state and assert its
serialized-length premise. Construct expected `tool_call` and `tool_result`
payloads from that durable ID plus the frozen effect/input/result, never from
callback-returned or captured wire data.

Compute hard-coded independent frame sizes without `Frame::new` or captured wire data. Require:

```text
selected live frame bytes > viewer_queue_bytes
viewer_queue_bytes >= tool_call frame bytes
viewer_queue_bytes >= tool_result frame bytes + done frame bytes
viewer_queue_frames >= 2
```

Use hard-coded independent sizes and checked addition. Keep every
native/digest/payload/depth/snapshot/input/output limit ample. The `tool_call`
fit is a prerequisite for its exact primary delivery; the selected frame alone
must exceed the queue, while the two terminal-path frames must fit cumulatively.

- [x] **Step 2: Prove capacity, overflow, replacement, and settlement**

While the callback is held and durable state is `Running/InFlight`:

1. After the primary `tool_call` is fully validated and the primary client is
   closed, positively prove the same run remains held. Capture the complete typed
   and raw SQLite histories plus the held
   `Running/InFlight`, one-attempt, expected-owner, no-result, and
   one-active-callback boundary.
2. Open and fully validate the first authorized attachment state, including the
   exact trusted `tool_call` once in its non-truncated turn digest.
3. Require a second authorized GET to return exactly HTTP 200, one capacity
   detachment, and EOF with no state or terminal. Before publishing anything,
   positively require the first attachment remains live, the callback remains
   held, and the complete durable state, result absence, callback/attempt counts,
   and typed/raw histories remain equal to the initial boundary.
4. Publish the selected oversized frame and require `NativeChunkResult::Accepted`.
5. Require the first attachment to return exactly its state, one overflow
   detachment, and EOF without terminal. Require the expected native publication
   count, unchanged held durable state and result absence, and exact typed/raw
   history-prefix equality with the initial boundary.
6. Open a replacement attachment, fully consume and validate exactly one state
   prefix before release, and require its digest to contain the trusted
   `tool_call` followed by the selected frame exactly once.
7. Release the callback. After the already consumed state, require exactly one
   `tool_result`, exactly one `done`, and EOF, with no overflow, duplicate, extra,
   reordered, second-state, or post-terminal data. The complete replacement
   sequence is `state`, `tool_result`, `done`, EOF. Reassert that both earlier
   streams remain at EOF and cannot receive later terminal data.
8. Require the replacement terminal and independently read durable task result
   to equal the same frozen value. The disconnected primary contributes no
   success, overflow, or terminal evidence.

Verify exact typed/raw SQLite histories, one callback, one task attempt, durable success and aggregate values. Drain and retire the server, destroy the sole inner SQLite owner, reopen at owner+1, and use GET-only checks to prove preservation with no replay.

- [x] **Step 3: Format only the new file and freeze the initial map**

From the isolated T-016 workspace, run the pinned formatter directly on the one
new file with child traversal disabled:

```bash
/Users/blove/.rustup/toolchains/1.93.1-aarch64-apple-darwin/bin/rustfmt \
  --edition 2021 --config skip_children=true \
  crates/b4-http/tests/viewer_overflow_tcp.rs
```

Compare complete before/after maps and require the only added or changed path is
`crates/b4-http/tests/viewer_overflow_tcp.rs`. Verify every baseline byte remains
unchanged, then record `task1-frozen-snapshot.json` with the full 1,186-file map
and the test hash. Retain the later workspace-wide `cargo fmt --all -- --check`
as a read-only final verification category.

- [x] **Step 4: Independent source review before execution**

Review exact design compliance, size-oracle independence, authorization/privacy, deterministic queue decisions, complete HTTP/SSE parsing, finite cleanup, durable histories, restart proof, and absence of production/metadata edits. Reject callback-derived expectations, socket-backpressure dependence, missing cumulative burst sizing, or EOF-as-settlement inference.

- [x] **Step 5: Run the initial named proof and early strict lint**

Run with the reviewed runner:

```bash
cargo test -p b4-http --test viewer_overflow_tcp \
  release_t016_viewer_capacity_and_queue_overflow_do_not_stop_run \
  --locked --offline -- --nocapture --test-threads=1
```

Expected: one passed, zero failed, zero ignored, exit zero, reaped, no timeout, no source drift.

Then run:

```bash
cargo clippy -p b4-runtime -p b4-http --all-targets \
  --locked --offline -- -D warnings
```

Both runs bind the initial full map and carry `additional_runtime_credit:false`; final qualification reruns the scenario on the metadata-complete map. A genuine behavioral failure triggers design amendment and re-review before any production change. Compile, fixture, parser, deadline, or unrelated lifecycle failures are repaired only within the acceptance file and re-reviewed.

- [x] **Step 6: Independent quality/results review**

Review the complete logs, receipts, source maps, exact event sequence, queue inequalities, durable state, lifecycle, lint, and narrow scope. No metadata work begins until approved.

## Task 3: metadata and guard sensitivity

- [x] **Step 1: Fresh metadata author changes exactly sixteen paths**

Promote only T-016 to:

- classification `NATIVE`
- release scope `native_attached_viewer_capacity_and_byte_overflow_tcp_sqlite`
- acceptance file `crates/b4-http/tests/viewer_overflow_tcp.rs`
- acceptance name `release_t016_viewer_capacity_and_queue_overflow_do_not_stop_run`
- receipt `conformance/release/t016-viewer-overflow/qualification.json`

Advance prose and headline literals from fifteen proofs/41 pending/T-016 to sixteen proofs/40 pending/T-017. Preserve M0, CP-010, full core, COMPAT, feature, primary-viewer, frame-overflow, arbitrary-backpressure, cancellation, provider, and TypeScript limitations.

- [x] **Step 2: Record the honest old-policy RED**

Before changing `scripts/check_contracts.py`, run the fixed guard module against the new metadata and record the actual old-policy failure that T-016 must remain pending. This RED is metadata sensitivity only and carries no runtime credit.

- [x] **Step 3: Extend the checker and guard suite**

Add the exact ordered 23-check/20-runtime T-016 policy and typed `initial_proof` binding. Preserve all 110 predecessor guard meanings and add seven focused T-016 methods, for 117 expected tests:

1. accept exact T-016 policy with distinct initial/final maps;
2. reject missing T-016 receipt;
3. reject wrong T-016 identity, qualification, or broad-credit fields;
4. reject invalid/aliased initial proof and source maps;
5. reject command order, process, drift, metadata, and runtime-credit mutations;
6. reject wrong acceptance path, name, or hash;
7. reject changed acceptance source or missing named declaration.

Do not add `feature_validation`. T-017 through T-056 remain pending.

- [x] **Step 4: Run the metadata GREEN**

Run:

```bash
/Users/blove/.pyenv/versions/3.12.12/bin/python3 -B \
  scripts/test_core_release_traceability.py
```

Expected: `Ran 117 tests`, `OK`, exit zero, reaped, no timeout, and no source drift. Freeze the proper final `native-source-descriptor.json` with file count and full file map.

- [x] **Step 5: Independent metadata source and quality reviews**

Verify exactly sixteen metadata modifications beyond the frozen Task 2 map, correct old-policy RED/final GREEN, preserved predecessor semantics, exact T-016 identity, narrow prose, and final descriptor relationships.

## Task 4: final qualification

- [x] **Step 1: Run all 23 final categories on the final descriptor**

Run each once with the reviewed runner and at most three active children:

1. `scenario-test`: named T-016 command from Task 2;
2. `viewer-overflow-tests`: full `viewer_overflow_tcp` target;
3. `native-stream-api-tests`: `cargo test -p b4-runtime --test native_streaming --locked --offline -- --test-threads=1`;
4. `native-stream-control-tests`: `cargo test -p b4-http --test native_streaming --locked --offline -- --test-threads=1`;
5. `http-control-tests`: `cargo test -p b4-http --lib --locked --offline -- --test-threads=1`;
6–20. T-001 through T-015 targets, in order:
   `native_tcp`, `thread_state_tcp`, `thread_admission_tcp`, `thread_route_tcp`,
   `thread_json_tcp`, `input_shape_tcp`, `thread_concurrency_tcp`,
   `different_thread_tcp`, `wait_output_tcp`, `stream_payload_tcp`,
   `terminal_frame_tcp`, `disconnect_completion_tcp`, `attachment_state_tcp`,
   `attachment_cut_tcp`, `attachment_truncation_tcp`;
21. `format-check`: `cargo fmt --all -- --check`;
22. `scoped-lint`: strict dual-crate all-target lint from Task 2;
23. `guard-tests`: pinned Python guard command from Task 3.

Only steps 1–20 carry runtime credit. Only `guard-tests` is metadata-only. Every record must have the exact command, final descriptor, full identical before/after maps, exit zero, reaped child, no timeout/history transfer/drift, and positive expected summaries.

- [x] **Step 2: Generate qualification and delivery prose**

Run the reviewed qualifier to create `delivery/qualification.json` only after all 23 records pass. Bind the final descriptor, exact acceptance hash, initial proof receipt/map with no additional credit, current records, 20-runtime boundary, T-016-only scope, and open core/COMPAT limits.

Create `delivery/README.md` describing exact capacity/overflow/replacement/durable behavior and all exclusions. Stage exact reviewed design and plan copies.

- [x] **Step 3: Independent integrated-results and delivery reviews**

Inspect every selected receipt/log/map and delivery artifact. Reject stale or repeated results, transferred initial credit, inflated scope, or any mismatch between evidence and prose.

## Task 5: semantic manifest and reversible adoption

- [x] **Step 1: Freeze eleven semantic reports and seven artifacts**

Require these report roles:

- design review
- plan review
- controller review
- qualification-controller review
- Task 2 source review
- Task 2 quality review
- Task 3 source review
- Task 3 quality review
- copy-controller review
- integrated-results review
- delivery review

Freeze seven artifacts: qualifier, adopter, public-metadata controller, actual qualification, actual README, reviewed design, and reviewed plan. Root reads each decision and scope, writes exact report hashes, and runs the reviewed freezer. An independent reviewer semantically approves the manifest before Root issues the manifest decision.

- [x] **Step 2: Create all backups before mutation**

The adopter requires complete current public equality with the T-016 baseline and complete workspace equality with native. Create and verify all eighteen backups before the first public mutation.

- [x] **Step 3: Adopt exact bytes**

Copy the exact seventeen-path source delta and prove complete public equals native before installing the two absent generated T-016 files. Then require exact native-plus-two public equality, unchanged thirty protected predecessor paths, unchanged private workspace, and preserved manifest/artifact hashes.

## Task 6: public verification and Root acceptance

- [x] **Step 1: Run exactly thirteen public metadata checks**

Run the whole checker with a 60-second deadline plus the established twelve traceability modules with 600-second deadlines and at most three active processes. Require exact public before/after maps, successful logs, reaped processes, no timeout/history/drift, metadata-only classification, and no runtime credit.

- [x] **Step 2: Collect and independently review public integrity**

The collector requires exactly one current receipt per category. The final reviewer independently verifies the seventeen-path delta, eighteen backups, thirty protected paths, native/public maps, all 23 final checks, initial proof, eleven reports, seven artifacts, semantic bindings, public records, narrow claim, and open core/COMPAT limits.

- [x] **Step 3: Root acceptance and plan closure**

Root writes exact T-016 acceptance only after reading the approved integrity report. Close only the first seven completed plan tasks, leave “continue T-017” open, and capture the post-acceptance public source descriptor before later documents.

## Task 7: continue

- [ ] Continue T-017 unless the user steers or a genuine design/manual decision is required.
