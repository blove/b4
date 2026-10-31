# Native Attachment Cut and Live Tail Implementation Plan

> For agentic workers: use superpowers:subagent-driven-development with a fresh author per task and independent source/spec followed by quality/results review.

**Goal:** Qualify the selected T-014 partition: chunk A is retained once in an attached snapshot and chunk B arrives once in its live tail.

**Architecture:** One genuine native stream callback, real TCP, an owned SQLite Store behind a transparent test-local read-return adapter, and a finite first-Attach policy gate establish both sides of the actual subscription cut. No production changes or timing stress loops.

**Tech stack:** Rust 1.93.1, Tokio, existing native HTTP/runtime and SQLite interfaces, pinned Python metadata checks. Actual T-013 Root acceptance precedes implementation. User authorizes continuous reversible local work; no Git writes, routine approval pauses or milestone final responses.

## Source policy and files

Create only crates/b4-http/tests/attachment_cut_tcp.rs, exact test release_t014_attachment_cut_and_live_tail_preserve_each_event_once. Separate metadata author changes exactly scripts/check_contracts.py, scripts/test_core_release_traceability.py, spec/runtime/requirements.yaml, README.md, spec/runtime/compatibility.md and the same eleven headline scripts used by T-013. Headline changes are one literal each. Every other baseline byte is protected, including 26 previous acceptance-test/receipt paths and T-010's distinct feature evidence/alias guard. Exact delta: 17 paths, one addition, no removals. Backups: 16 existing metadata files plus two published documents, all 18 before mutation.

Publish exact reviewed documents at docs/superpowers/specs/2026-10-21-fourteenth-release-attachment-cut-design.md and docs/superpowers/plans/2026-10-21-fourteenth-release-attachment-cut-plan.md; stage fourteenth-thread-design.md and fourteenth-thread-plan.md. Actual inventories control anticipated baseline1175/native1176/public1178 after accepted T0131173 plus two documents. Ordinary isolated copy only; external shared build target and read-only Git. Public freezes during private work.

## Task 0: reviewed preparation

- [x] After actual T-013 acceptance, approve design/plan and seven controllers, publish exact documents and capture complete baseline/workspace equality.

Controllers: setup, run_step, qualify, adopt, freeze_manifest, public_metadata and collect_postcopy (all .py). Setup checks actual predecessor accepted/T-013 before copying. Fixed runner pins all Rust tools and Python-B, Cargo locked/offline, complete before/after maps, log/receipt hashes, command/status/reaping/no-timeout/no-history records. Each child600s; timeout uses process-group TERM, full monotonic5s grace despite leader exit, guarded KILL/reap. Root runs max3 and communicates at most60s apart. No feature/full-workspace step.

## Task 1: independent cut/live-tail proof

- [x] Fresh author adds the sole test; Root formats only it, freezes baseline+test initial map, obtains source approval, runs named native proof AND early strict lint, then obtains independent quality/results approval.

One immutable Pure approval-free application and one real ordinary RunStream use explicit streaming entry, original validator input/context and clear observer, one callback/task/attempt. Fixed producer literals emit distinct A and B JSON chunks with Accepted feedback, then return an independent nonnull structured result and state writes. Expected wire/task/aggregate literals are separate. Capacity1 and sufficient fixed viewer/digest budgets avoid overflow confounds. Every callback/Store/policy gate is finite with panic release guards before admission.

Use a local Store adapter that forwards actual SQLite capabilities, owner generation, create/read/admit/fenced commit/journal/readiness/delete/cancel and required ApprovalStore methods unchanged. Its sole pause is after actual inner.thread_view returns an owned result for the one armed target, before returning it to the caller. No retained SQL lock, fake view, invented commit/error or production hook. Other observations use the inner Store directly, with no competing request that can consume the arm.

Start primary stream and positively hold actual callback entry/return0. Observe exact Running/InFlight revision2/raw4 history, owner/context/input and active owned work. Arm capture, start one GET attachment and require actual inner capture gate entry. Emit A while that return is held, require Accepted plus next positive callback hold/return0 and unchanged durable history. Release capture. Source-backed attach then subscribes under the coordinator mutex before its first ResourcePolicy authorize call. That exact operation/thread/provenance call blocks on the one policy gate; later renewal calls allow normally. Require positive policy entry before emitting B, then Accepted and final held callback gate/return0, unchanged histories again.

Release policy. Before final callback release, parse complete attached state and queued B over actual TCP. Exact state: busy/live true/resume false/empty interrupts, independently observed execution anchor, original values/input, independently bounded canonical host timestamp and turn exactly [trusted tool_call, chunk A]. Next application event is B. Count A exactly once in snapshot/zero in live tail and B zero in snapshot/once in live tail. Sink feedback and substring matching cannot substitute for wire equality. Source gate verifies genuine one-shot phase positions and counts; no guessed read ordinal or quiet-time inference.

Explicitly release callback. Complete both raw responses through EOF, preserving all prefixes. Primary vector: tool_call/A/B/tool_result/done. Attached vector: state/B/tool_result/done. Pin trusted attempt/effect/wrapped input and independently committed task result; exactly one terminal last. Strict bounded incremental HTTP/chunk/AP/unique-key JSON parsing rejects malformed, unknown, duplicate, missing or postterminal application data and recognizes only explicit legal controls/comments. No arbitrary packet-split or all-scheduler proof.

Require independent full typed snapshot/task/attempt/provenance, all7/5/9 raw columns including creation, exact owner/revision/status/kind/payload and bounded monotonic timestamps. Chunks do not change held durable history; final revision3/raw5 rows has one Succeeded present result and Completed aggregate writes. One validator/callback/attempt throughout, no replay/cancellation. Positive Serving/nonfaulted/active_owners0 before quiescence/listener abort/join. Drop adapter and all aliases, synchronously destroy the actual sole inner Store, reopen exactly owner+1, fresh-host GET-only equal histories/counters, positive final owner/application/listener/directory cleanup. No graceful shutdown claim. Existing behavior first GREEN is honest; real discrepancies need narrow source/design repair, never weaker assertions or manufactured RED. Function-local documented helper lint allowances are acceptable; no global suppression.

## Task 2: metadata and bounded guards

- [x] A fresh metadata author changes only16authorized paths, records honest RED/GREEN, then passes source and quality review.

Promote only T014 NATIVE/native_attachment_cut_live_tail_tcp_sqlite, exact file/name and conformance/release/t014-attachment-cut/qualification.json. Fourteen scoped proofs/42pending; T015–056 remain pending. Keep96prior guard meanings and add7bounded methods (103 expected): valid exact policy/distinct descriptors, missing receipt, identity fields, typed proof/initial-map/process/digests, exact command/order/drift/credit, acceptance path/name/hash and altered source/declaration. Fixture has no inherited feature_validation. Old thirteen row/policy/receipt identities, T010 separate feature/alias and historical/global flags remain unchanged. M0/core/COMPAT open, CP001–009 partial, CP010 unresolved.

Stage tests before checker branch for actual old-schema RED; report precisely which valid fixtures fail, not unsupported per-negative sensitivity. Use reviewed fixed guard-tests runner on frozen maps, pinned Python-B, metadata-only/no runtime credit. No whole checker or real current receipt inside workspace before actual delivery. Prose limits selected controlled pre-cut A/post-cut B partition, transient versus durable history and owner+1 preservation; no full-suite, universal race, overflow or provider/TypeScript claim.

## Task 3: final verification and qualification

- [x] Freeze final descriptor with file_count and files, run all20fresh categories on that map, generate real receipt outside source and obtain integrated results approval.

Ordered categories:
1. scenario-test: cargo test -p b4-http --test attachment_cut_tcp release_t014_attachment_cut_and_live_tail_preserve_each_event_once --locked --offline -- --nocapture --test-threads=1
2. attachment-cut-tests: complete attachment_cut_tcp target, locked/offline, test-threads1
3. native-stream-api-tests: b4-runtime native_streaming target, same options
4. native-stream-control-tests: b4-http native_streaming target, same options
5–17. t001-regression through t013-regression: complete native_tcp, thread_state_tcp, thread_admission_tcp, thread_route_tcp, thread_json_tcp, input_shape_tcp, thread_concurrency_tcp, different_thread_tcp, wait_output_tcp, stream_payload_tcp, terminal_frame_tcp, disconnect_completion_tcp, attachment_state_tcp targets; each cargo test -p b4-http --test TARGET --locked --offline -- --test-threads=1
18. format-check: cargo fmt --all -- --check
19. scoped-lint: cargo clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings
20. guard-tests: pinned Python-B scripts/test_core_release_traceability.py

Only first17categories carry runtime credit. Derive actual summary executions, not unique inventory. Initial own-map proof and early lint add no duplicate final credit. No new full-workspace/feature claim; earlier proofs remain historical. Complete current source must match final maps before/after each check and receipt assembly. Fix actual failures narrowly with honest preserved records and renewed gates/source-bound checks.

## Task 4: reviewed reversible copy

- [x] Freeze seven artifacts and eleven actual review decisions, obtain independent semantic manifest approval, then copy exact bytes with all backups and equality gates.

Required reports: design-review, plan-review, controller-review, qualification-controller-review, task1-source-review, task1-quality-review, task2-source-review, task2-quality-review, copy-controller-review, integrated-results-review, delivery-review (.md). Seven artifacts: qualifier, adopter, public-metadata controller, real qualification, actual README, reviewed design and plan. Root reads real decisions/scopes and writes explicit approved/report hashes bound to native descriptor. Freezer binds Root input hash and seven artifacts. Independent manifest review and explicit Root manifest decision bind actual report/manifest hashes; no approval keyword inference or routine human pause.

All18verified baseline backups precede first mutation. Copy exact17delta, prove full public==native before installing new qualification/README, then exact native+2. Both generated destinations were absent. Reviewed document copies already equal baseline/native bytes; all26prior pairs and every non-delta byte remain fixed. No crash-atomic/automatic rollback claim.

## Task 5: public verification and acceptance

- [x] Run whole public checker plus the same twelve modules, collect all13actual records, obtain independent public integrity review and Root acceptance before plan closure.

Whole60s/modules600s/max3active, long accepted-Once/CLI/startup-confinement modules early and prompt slot refill. Exact pinned commands, complete unchanged maps, actual log/receipt hashes, exit0/reaped/no-timeout/no-history and positive unittest counts/OK are metadata-only/no runtime credit. Collector enforces one actual record per member. Integrity covers exact17delta/18backups/26protections/native/public maps/reviews/artifacts/current20checks/claim limits. Root accepts, closes first6of7boxes and captures post-acceptance source before next documents.

## Task 6: continue

- [ ] Continue T-015 unless user steers or genuine manual action/approval is required; no milestone final.
