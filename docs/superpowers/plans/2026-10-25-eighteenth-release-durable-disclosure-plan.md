# T-018 Durable Approval Before Disclosure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task, with an independent acceptance source review before execution and independent metadata, results and public-integrity reviews.

**Goal:** Qualify the bounded T-018 native claim that one required approval grant and its park state are committed before a real TCP interrupt or authorized pending response discloses them, and that precommit faults disclose neither.

**Architecture:** Add one opt-in integration test using a test-owned server child, real loopback HTTP/SSE and owned SQLite. Separate fresh fixtures inject grant and park-journal insert failures; a healthy fixture witnesses the committed park through an independent read-only connection before hard-killing the child and reopening the store. Promote only T-018 after isolated current-source proof, reviewed metadata and public verification.

**Tech Stack:** Rust 1.93.1, Tokio/Axum, rusqlite/SQLite, pinned Python 3.12.12, locked offline Cargo, full SHA-256 source maps.

**Design:** `.b4-work/full-rust-rewrite-scope/t018-durable-before-disclosure-design-draft.md` SHA-256 `86c972ff50aac170c0513c133d05318ad04cea4dbbdf92b8e4646355f88755c5`, independently approved in `.b4-work/full-rust-rewrite-scope/t018-design-review-r2.md`; user approved 2026-10-25.

---

## Task 0: predecessor and isolated setup

**Files:** Private `.b4-work/eighteenth-release-durable-disclosure/` controllers/workspace/receipts; eventual public design/plan under `docs/superpowers/`.

- [ ] Recheck T-017 root acceptance SHA `5e19d3d95502180132a533000622cee88234c84897b3773a98552c4ded85927e`, its 1,195-file post-acceptance map SHA `73fd5b6b332fb491eddb5288ca5e3e8aa2419936a62b3e0a356ea4074d9db293`, and catalog 1,197-file publication map SHA `bfbd1aa65739d93520402ab4f483c496bfe34d441d8f1e2f83ca4200ece6043a`. Prove the current public map matches catalog publication exactly before any T-018 public addition. Record any later reviewed documents as explicit new baseline additions; never treat the catalog validator's immutable 1,197-file publication phase as a current-tree test after later additions.
- [ ] Rehash all behavior-bearing source pins named by the design, including RFC/requirements, approval service, worker, transport, control, viewer and SQLite transaction. A changed pin requires design amendment before authorship.
- [ ] Publish reviewed T-018 design and plan as exact copies only after plan review; independently bind their hashes and the then-current public map. Create a private isolated workspace copied from that map and a shared external target pointer. Keep `superseded-pre-amendment-*` material excluded. Record full baseline file/hash map and verify public/workspace equality.
- [ ] Author or adapt bounded setup, runner, qualifier, manifest, adopter, public checker and collector controllers in private. Each has exact roots, allowed path deltas, 600-second child deadline (60 seconds for whole checker), new process group/session, finite TERM/KILL/reap, before/after complete maps, log hashes and no inherited receipt credit. Obtain independent controller source reviews before any controller executes. Do not mutate Rust public source during private acceptance.

## Task 1: acceptance source only

**Files:** Create `crates/b4-http/tests/approval_durable_disclosure_tcp.rs` only in isolated workspace.

- [ ] Write one named opt-in test, `release_t018_approval_park_is_durable_before_tcp_disclosure`, with explicit child-mode argument/environment guard so the test executable cannot recursively spawn. Freeze route, task, input, prompt, auth principal, grant policy, expected nonsensitive state and exact HTTP requests before any response. Ordinary and streaming effect callbacks panic if called before valid resume.
- [ ] Implement bounded child supervisor and wire parser in the test: private SQLite directory per subcase, loopback-only listener, explicit child PID and exit/reap guard, no detached helper, and cleanup even when an assertion panics. Spawn the current test executable with `--exact release_t018_approval_park_is_durable_before_tcp_disclosure --nocapture --test-threads=1` and `B4_T018_CHILD_MODE=<grant_abort|journal_abort|healthy>` plus a fresh 128-bit nonce. The child binds port zero, then atomically renames a readiness JSON file containing that nonce, its PID and bound port; the parent rejects wrong nonce/PID, waits at most 10 seconds, then connects. Each child has a 120-second hard lifetime, each HTTP request/read and EOF wait a 20-second deadline, each SQLite poll a 10-second deadline with at most 500 iterations, and TERM/KILL/reap at most 5 seconds each. Cap one response at 2 MiB, 1,024 chunks, 1,024 SSE frames, 64 KiB per frame, JSON nesting at 64, and all captured diagnostic output at 2 MiB. Exceeding any cap fails the proof. Do not infer SSE frames from TCP packet boundaries.
- [ ] Implement fresh **grant-abort** fixture: install `BEFORE INSERT ON approval_grants` abort trigger before POST. Require admitted HTTP 200 SSE, then bounded closure without complete `interrupt` or `done`, while comments are allowed. Scan all wire/log bytes for absence of frozen prompt and `b4ag_`; require committed admission and Running journal before the failed park, no grant, no Park journal, no WaitingForApproval snapshot and authorized pending GET `503` JSON `details.code=host_unready` without prompt/grant.
- [ ] Implement independent fresh **journal-abort** fixture with `BEFORE INSERT ON journal WHEN NEW.status = '"waiting_for_approval"'` abort trigger installed before POST. Apply the same 200/closed/no disclosure, positive Running, rollback, and 503 fault-gate oracles. A pre-admission failure, timeout, malformed stream or missing Running row fails either negative fixture.
- [ ] Implement fresh healthy fixture: capture the first complete `interrupt` SSE frame, parse strict event/JSON shape and compare frozen prompt plus linked identity, opaque grant and resume key. Before any resume, use a separate read-only SQLite connection to verify the unconsumed grant, task/interrupt/prompt/token, matching park snapshot, generation/route, revision increment and coherent Park journal. Require authorized pending GET to return the same identity/prompt/grant with `cache-control: no-store`; require ordinary state GET to omit the token.
- [ ] After the independent SQLite and authorized pending/ordinary GET checks, hard-kill and reap the healthy server child without waiting for `done` or resuming. Only then reopen the same SQLite directory; require owner generation +1 and identical typed/raw park, grant, prompt/token and journal data. Do not invoke recovery or claim post-restart HTTP disclosure.
- [ ] Run pinned rustfmt directly on the one test with `--edition 2021 --config skip_children=true`; verify baseline plus only that file, freeze every file/hash in `task1-frozen-snapshot.json`. Obtain independent full source review of child cleanup, trigger timing, transaction linkage, parser and secret oracles before executing the test.
- [ ] Run exactly the named test through reviewed bounded runner using `cargo test -p b4-http --test approval_durable_disclosure_tcp release_t018_approval_park_is_durable_before_tcp_disclosure --locked --offline -- --nocapture --test-threads=1`; then strict `cargo clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings`. Require exit zero, no timeout, all children reaped and unchanged frozen map. These initial checks carry no extra final runtime credit; independently review source-bound results.

## Task 2: metadata RED/GREEN

**Files:** Modify only `README.md`, `spec/runtime/compatibility.md`, `spec/runtime/requirements.yaml`, `scripts/check_contracts.py`, and the twelve established traceability test modules named in the T-017 plan, in isolated workspace.

- [ ] Change T-018 alone from pending to scoped `NATIVE` with `native_approval_park_durable_before_disclosure_tcp_sqlite`, exact test path/name and `conformance/release/t018-durable-disclosure/qualification.json`. Set headlines to eighteen scoped native proofs and thirty-eight pending. Keep T-019–T-056, M0, CP-001–010, COMPAT, core and whole-product parity open.
- [ ] Run the fixed old-policy core guard suite after metadata edit and before checker policy edit; require the intended RED for T-018 still pending. No runtime credit.
- [ ] Extend `scripts/check_contracts.py` with the exact ordered 27-category T-018 policy in the command table below. Positions 01–24 carry runtime credit; 25–27 carry none. Add focused mutation guard tests for wrong scope/acceptance path, missing or aliased proof, changed source, receipt command/process/map drift, and broadened claims. Preserve all 124 existing T-017 guard meanings.
- [ ] Run pinned Python core guard suite to GREEN and freeze the complete native source descriptor. Obtain independent metadata source and quality review. Any production source change requires a new design, acceptance map and review; it cannot be hidden in metadata.

## Task 3: final private qualification

- [ ] Run each exact reviewed final category once on the final descriptor, using the command table below, with current full before/after maps, positive output checks, no timeout/unreaped child/drift, and no transfer from Task 1. Positions 01–24 alone carry runtime credit.
- [ ] Produce `delivery/qualification.json` from only those actual receipts. State the exact fixed required-durable park/disclosure/hard-kill/reopen scope and limitations from the design. Stage README and byte-identical reviewed design/plan copies. Independently review integrated results and delivery text.

## Task 4: reversible adoption and public checks

- [ ] Root reads and decides eleven exact report roles before manifest freeze: design review, plan review, setup/runner controller review, qualification-controller review, Task 1 source review, Task 1 quality/results review, Task 2 metadata source review, Task 2 metadata quality review, copy-controller review, integrated-results review and delivery review. Freeze seven exact artifacts: reviewed qualifier controller source, adopter source, public-check controller source, actual delivery qualification, delivery README, byte-identical reviewed design copy and byte-identical reviewed plan copy. An independent semantic-manifest review follows Root's report/hash decisions; Root then records a manifest decision before any adoption. Prove public baseline equality and private native descriptor equality.
- [ ] Back up every public path to be overwritten and verify bytes before first copy. Adopt only the one acceptance file, exact reviewed metadata delta and two T-018 delivery files; prove copied file hashes, full public map and protected T-001–T-017 acceptance/qualification paths. Roll back on a failed equality gate.
- [ ] Run whole checker and the established twelve traceability modules once each on unchanged adopted public map, with bounded/reaped receipts. Obtain independent public integrity review of backups, source maps, final runtime checks, metadata guards, manifest and limited claim.
- [ ] Record T-018 root acceptance only after public review approves. Close completed plan tasks and capture a post-acceptance map. Do not mark M0, CP or whole runtime complete from this scoped native result.

## Task 5: continuation

- [ ] Continue the separate pinned current-HEAD M0 comparison lane, CP-010 client-baseline decision and T-019 onward. No historical TypeScript client receipt becomes current-HEAD COMPAT by inheritance.

No Git write is required by this local release-qualification workflow.

## Exact final command policy

`CARGO` is `/Users/blove/.rustup/toolchains/1.93.1-aarch64-apple-darwin/bin/cargo` and `PYTHON` is `/Users/blove/.pyenv/versions/3.12.12/bin/python3`. All commands run in the isolated T-018 workspace with the reviewed external target, locked/offline Cargo and no other accepted result reuse. Labels and vectors are ordered:

```text
01 scenario-test              CARGO test -p b4-http --test approval_durable_disclosure_tcp release_t018_approval_park_is_durable_before_tcp_disclosure --locked --offline -- --nocapture --test-threads=1
02 durable-disclosure-tests   CARGO test -p b4-http --test approval_durable_disclosure_tcp --locked --offline -- --test-threads=1
03 approval-http-tests        CARGO test -p b4-http --test approval_tcp --locked --offline -- --test-threads=1
04 approval-sqlite-tests      CARGO test -p b4-sqlite --test approvals --locked --offline -- --test-threads=1
05 native-stream-api-tests    CARGO test -p b4-runtime --test native_streaming --locked --offline -- --test-threads=1
06 native-stream-control-tests CARGO test -p b4-http --test native_streaming --locked --offline -- --test-threads=1
07 http-control-tests         CARGO test -p b4-http --lib --locked --offline -- --test-threads=1
08 t001-regression            CARGO test -p b4-http --test native_tcp --locked --offline -- --test-threads=1
09 t002-regression            CARGO test -p b4-http --test thread_state_tcp --locked --offline -- --test-threads=1
10 t003-regression            CARGO test -p b4-http --test thread_admission_tcp --locked --offline -- --test-threads=1
11 t004-regression            CARGO test -p b4-http --test thread_route_tcp --locked --offline -- --test-threads=1
12 t005-regression            CARGO test -p b4-http --test thread_json_tcp --locked --offline -- --test-threads=1
13 t006-regression            CARGO test -p b4-http --test input_shape_tcp --locked --offline -- --test-threads=1
14 t007-regression            CARGO test -p b4-http --test thread_concurrency_tcp --locked --offline -- --test-threads=1
15 t008-regression            CARGO test -p b4-http --test different_thread_tcp --locked --offline -- --test-threads=1
16 t009-regression            CARGO test -p b4-http --test wait_output_tcp --locked --offline -- --test-threads=1
17 t010-regression            CARGO test -p b4-http --test stream_payload_tcp --locked --offline -- --test-threads=1
18 t011-regression            CARGO test -p b4-http --test terminal_frame_tcp --locked --offline -- --test-threads=1
19 t012-regression            CARGO test -p b4-http --test disconnect_completion_tcp --locked --offline -- --test-threads=1
20 t013-regression            CARGO test -p b4-http --test attachment_state_tcp --locked --offline -- --test-threads=1
21 t014-regression            CARGO test -p b4-http --test attachment_cut_tcp --locked --offline -- --test-threads=1
22 t015-regression            CARGO test -p b4-http --test attachment_truncation_tcp --locked --offline -- --test-threads=1
23 t016-regression            CARGO test -p b4-http --test viewer_overflow_tcp --locked --offline -- --test-threads=1
24 t017-regression            CARGO test -p b4-http --test heartbeat_splits_tcp --locked --offline -- --test-threads=1
25 format-check               CARGO fmt --all -- --check
26 scoped-lint                CARGO clippy -p b4-runtime -p b4-http -p b4-sqlite --all-targets --locked --offline -- -D warnings
27 guard-tests                PYTHON -B scripts/test_core_release_traceability.py
```

Only positions 01–24 carry runtime credit. The initial named proof in Task 1 is distinct and adds no final credit. Public checks run `PYTHON -B scripts/check_contracts.py` once within 60 seconds, followed by each of the twelve established traceability modules named in the T-017 plan once within 600 seconds, against an unchanged public map; all are metadata-only.
