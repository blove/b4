# Disconnected Native Run Completion Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development task by task with fresh authors, independent source/spec then quality/results gates.

**Goal:** Qualify T-012 through one stream and one wait client closing after genuine callback entry while admitted work completes once.

**Architecture:** One new real-TCP/owned-SQLite test uses the existing opted-in native host, finite callback gates and independent durable oracles. No production behavior change is planned. Stream must exercise its streaming entry; wait its ordinary controlled entry. Actual T-011 acceptance and reviewed design/controllers precede implementation.

**Tech Stack:** Rust1.93.1, Tokio, existing B4 native application/HTTP runtime, owned SQLite, pinned Python metadata checks.

User authorizes continuous reversible local work. No Git writes, routine permission pauses or milestone finals. Public source freezes during isolated authoring/checks; build target stays outside source.

## Files and bounded policy

Add only crates/b4-http/tests/disconnect_completion_tcp.rs, named release_t012_disconnected_stream_and_wait_preserve_admitted_work. Separate metadata author may change only scripts/check_contracts.py, scripts/test_core_release_traceability.py, spec/runtime/requirements.yaml, README.md, spec/runtime/compatibility.md and the same eleven headline scripts as T-011. Every other baseline byte is protected, including22prior acceptance-test/receipt paths and T010's own feature evidence/alias rejection. Delta17paths/oneaddition/no removals,18backups(16metadata+2published docs). Capture actual inventory; expected1165baseline/1166native/1168public depends on acceptedT0111163 plus2published documents.

Publish exact reviewed docs to docs/superpowers/specs/2026-10-21-twelfth-release-disconnect-completion-design.md and docs/superpowers/plans/2026-10-21-twelfth-release-disconnect-completion-plan.md. Delivery copies twelfth-thread-design.md and twelfth-thread-plan.md. Use ordinary isolated copy, no Git changes. Commands lock/offline Cargo and pin all Rust tools/Python-B. Each final child600seconds with complete maps/loghash/command/status/reaping/no-timeout/no-history records; timeout processgroup TERM/full monotonic5second grace regardlessleaderexit/guardedKILL/reap. Max3parallel checks, poll<=45s and useful commentary<=60s.

## Task0: preparation

- [x] After actual T011Rootacceptance, approve design/plan and seven controllers, publish exact docs and capture baseline/workspace equality.

Controllers: setup.py, run_step.py, qualify.py, adopt.py, freeze_manifest.py, public_metadata.py, collect_postcopy.py. Review command/map/backup/delivery/semantic-decision policy before execution. No new full-workspace feature command, copied T010 feature_validation or historical runtime transfer.

## Task1: admitted disconnected work proof

- [x] Fresh author adds sole test. Root formats only it, freezes initial map baseline+test, source-gates, runs honest first proof, records actual results and quality-gates.

Two sequential IDs under capacity1, original independent inputs/results/state writes, one immutable Pure application. Explicit streaming vs controlled entry counters establish correct routes. Each callback records original invocation/context, entry once and return0 while finite gate held. Retain supplied cancellation observer and require clear before/after client close and return. Callback independently returns fixed result/writes, not values derived from response/store/expected object. Panic release guards before admission.

Client connects/writes bounded actual POST; positive callback entry establishes admission. Stream may read bounded header prefix preserving adjacent bytes; wait must not wait for response headers. Close/drop whole owned socket while actual callback is held and returned0. Do not claim server-observed close from quiet time, require a lost result, or synthesize terminal receipt. Before release inspect exact InFlight task/attempt, Runningexecution, admitted originalinput, identity/provenance and positive activeowners. Host-held Store weakreference is supporting lifetime evidence only.

Release before finite callback deadline, poll for independently expected Succeededtask/presentTaskResult/Completedexecution/aggregate writes and same oneattempt/owner/context. Exactly one validator/callback/return and unchanged cancellation observer. Require Serving/nonfaulted/activeowners0 before next admission. Source-trace held and completed revisions/journal literals; assert complete typed and raw7/5/9columns including creation, monotonic bounded timestamps and existing histories unchanged. No cancellation mutation or extra concurrency/capacity matrix.

Healthy quiescence before fixture listener abort/join, actual soleSqliteStore destruction then ownergeneration exactly+1reopen, fresh host GET-only equal HTTP/typed/raw records and no validator/callback/attempt replay, then positive listener/owner/directorycleanup. Every connection/write/read/gate/poll/lifecycle bounded; no production hook/sharedhelper edit or graceful shutdown claim. Existingbehavior firstGREEN is honest; actual mismatch requires narrow debugging/sourcegate, not weakened oracle or fakeRED.

## Task2: metadata and guards

- [x] Fresh metadata author changes only16paths, obtains actual metadataRED/GREEN, then source and quality approval.

Promote only T012NATIVE/native_disconnected_run_completion_tcp_sqlite, exact test and conformance/release/t012-disconnect-completion/qualification.json. Twelveproofs44pending. Preserve82prior guard meanings and add7bounded new(89expected); fixture has no inherited T010feature_validation. Validate actual receipt/testidentity/hash, exact18commands/first15runtime, process/map/log/digest/status/credit fields and initial proof ownmap/no duplicatecredit. New bounded negatives cover missing/wrongidentity/proof/command/credit/path/hash/source/declaration with validbaseline. Elevenheadline modules literal-only. Globalcore/COMPAT/M0open/CP001–009partial/CP010unresolved and historicalfoundation counts retained. No wholechecker until actualreceipt delivery.

## Task3: final verification

- [x] Freeze final descriptor initial+16metadata; run all18fresh categories, generate real receipt outside source only afterward, obtain integratedresults review.

Ordered commands (all pinned cargo locked/offline except fmt):
1 scenario-test: test -p b4-http --test disconnect_completion_tcp release_t012_disconnected_stream_and_wait_preserve_admitted_work --locked --offline -- --nocapture --test-threads=1
2 disconnect-completion-tests: test -p b4-http --test disconnect_completion_tcp --locked --offline -- --test-threads=1
3 native-stream-api-tests: test -p b4-runtime --test native_streaming --locked --offline -- --test-threads=1
4 native-stream-control-tests: test -p b4-http --test native_streaming --locked --offline -- --test-threads=1
5–15 t001-regression through t011-regression: complete native_tcp, thread_state_tcp, thread_admission_tcp, thread_route_tcp, thread_json_tcp, input_shape_tcp, thread_concurrency_tcp, different_thread_tcp, wait_output_tcp, stream_payload_tcp, terminal_frame_tcp targets, each test -p b4-http --test TARGET --locked --offline -- --test-threads=1
16 format-check: fmt --all -- --check
17 scoped-lint: clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings
18 guard-tests: pinned Python-B scripts/test_core_release_traceability.py.

First15runtimecategories only; actual reportedsummaryexecutions not unique inventory. Initial proof ownmap/noextra credit. No new fullsuite/feature claim. Protect all22earlier receipt/testpaths and all baseline non-delta bytes.

## Task4: review and reversible adoption

- [x] Freeze sevenactualartifacts, read elevenactualreviewdecisions/scopes, write explicitRootdecisions and freeze/review semanticmanifest, then reversiblecopy.

Required reports design-review,plan-review,controller-review,qualification-controller-review,task1-source-review,task1-quality-review,task2-source-review,task2-quality-review,copy-controller-review,integrated-results-review,delivery-review(all.md). Sevenartifacts qualifier/adopter/publicmetadata source plus generatedqualification/actualREADME/revieweddesign/plan. root-review-decisions.json binds exact11explicitapproved/hashentries and native descriptor; review-approvals.json binds decisionsdigest+7artifacts. Independent manifest-review plus root-manifest-decision bind actual contents/conditions, no keywordinference or humanpause. Eighteenoriginalbackups before firstmutation;17delta copy then completepublic==native BEFORE adding generatedq/README; exactpublicnative+2, bothnewdestinations absent, allworkspace/protectedbytes preserved.

## Task5: public checks and acceptance

- [x] Wholepublicchecker+same12metadata modules, aggregate actual13records, independentintegrityreview and Rootacceptance before planclosure.

Whole60s/modules600s/max3active. Start long acceptedOnce/CLI/startupconfinement early/refillslots. Same12modulemembershipasT011; exactoneactualrecordeach+whole, fullunchangedmaps/loghash/command/status/reaping/no-timeout/no-history. Metadataonly/no runtimecredit. Finalintegrity validates17delta18backups22protected/complete nativepublicmaps/actual18checks/allapprovals/scope. Rootaccepts, closesfirst6of7boxes, capturespostacceptsource before nextdocs.

## Task6: continue

- [ ] ContinueT013 unless usersteers or genuine manualaction/approvalrequired; no milestonefinal.
