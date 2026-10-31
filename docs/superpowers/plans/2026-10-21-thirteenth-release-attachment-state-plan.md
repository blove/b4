# Read-only Native Attachment State Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development task by task with fresh authors, independent source/spec then quality/results gates.

**Goal:** Qualify T-013 with live and settled real-TCP attachment starting with state and never invoking new execution.

**Architecture:** One new real-TCP/owned-SQLite test uses the existing opted-in native host, finite callback gates and independent durable oracles. No production behavior change is planned. One genuine wait uses its ordinary controlled entry; attachment remains read-only. Actual T-012 acceptance and reviewed design/controllers precede implementation.

**Tech Stack:** Rust1.93.1, Tokio, existing B4 native application/HTTP runtime, owned SQLite, pinned Python metadata checks.

User authorizes continuous reversible local work. No Git writes, routine permission pauses or milestone finals. Public source freezes during isolated authoring/checks; build target stays outside source.

## Files and bounded policy

Add only crates/b4-http/tests/attachment_state_tcp.rs, named release_t013_live_and_settled_attach_start_with_state_without_execution. Separate metadata author may change only scripts/check_contracts.py, scripts/test_core_release_traceability.py, spec/runtime/requirements.yaml, README.md, spec/runtime/compatibility.md and the same eleven headline scripts as T-012. Every other baseline byte is protected, including24prior acceptance-test/receipt paths and T010's own feature evidence/alias rejection. Delta17paths/oneaddition/no removals,18backups(16metadata+2published docs). Capture actual inventory; expected1170baseline/1171native/1173public depends on acceptedT0121168 plus2published documents.

Publish exact reviewed docs to docs/superpowers/specs/2026-10-21-thirteenth-release-attachment-state-design.md and docs/superpowers/plans/2026-10-21-thirteenth-release-attachment-state-plan.md. Delivery copies thirteenth-thread-design.md and thirteenth-thread-plan.md. Use ordinary isolated copy, no Git changes. Commands lock/offline Cargo and pin all Rust tools/Python-B. Each final child600seconds with complete maps/loghash/command/status/reaping/no-timeout/no-history records; timeout processgroup TERM/full monotonic5second grace regardlessleaderexit/guardedKILL/reap. Max3parallel checks, poll<=45s and useful commentary<=60s.

## Task0: preparation

- [x] After actual T012Rootacceptance, approve design/plan and seven controllers, publish exact docs and capture baseline/workspace equality.

Controllers: setup.py, run_step.py, qualify.py, adopt.py, freeze_manifest.py, public_metadata.py, collect_postcopy.py. Review command/map/backup/delivery/semantic-decision policy before execution. No new full-workspace feature command, copied T010 feature_validation or historical runtime transfer.

## Task1: read-only state-first attachment proof

- [x] Fresh author adds the sole attachment_state_tcp.rs test. Root formats only it, freezes baseline+test initial map, source-reviews, runs honest first proof, records actual results and quality-reviews.

One immutable Pure approval-free route, one independent original input/direct result/state write, one actual controlled callback under a finite gate. Primary wait opens/writes without waiting for response headers. Capture exact invocation/context, original validator input and entry1/return0; supplied cancellation observer stays clear. Panic release guards exist before admission. Independently establish Running/InFlight/oneattempt/owner/input and active owned work, complete typed and raw held history.

Open a genuine GET attachment while callback remains held. Bounded incremental HTTP/chunk/AP parsing preserves all bytes adjacent to headers and all subsequent prefix data. Positively receive the complete first application event, require state and full independent payload equality before releasing callback. Live state: busy/live true/resume false/interrupts [], anchor actual independently observed execution ID, values initial admitted map, input original literal, turn []. Source confirms wait uses controlled entry and no native auxiliary frames. Host-generated run_started_at is not durable-created time: independently validate actual canonical timestamp and local clock bounds, then retain that one value consistently; do not merely derive all expectations from the received state.

After complete state receipt while returned0 and gateheld, exact typed/raw store view must equal pre-attach baseline and counts stay validation1/entry1/return0/attempt1. No extra execution admission or storage mutation can hide behind an incomplete state substring. Explicitly release before deadline; collect primary wait and entire live attach through EOF with bounded size/time. Primary result is independent literal; complete attached application vector is captured state then exact done output for that result. Strictly validate every field/frame/UTF8/chunk terminator; reject duplicate keys, malformed/unknown application frames or postterminal data. Recognize exact legal ping comments/retry fields separately; no arbitrary split/race/quiet inference claim.

After actual Succeeded/presentTaskResult/Completed/aggregate writes and healthy Serving/nonfaulted/activeowners0, capture final complete baseline then GET settled attach. Exact idle state has null anchor/input/run_started_at/turn, live false/resume false/interrupts [], final durable values; followed by exact retry2000 and done {output:null}. This null terminal is attachment presentation, distinct from direct task result. Storage and allcounts/history remain unchanged before/after attach.

Assert full typed Snapshot/task/attempt/provenance/revisions and all7thread/5execution/9journalcolumns including creation, exact independent payloads/status/kind/owner/monotonicboundedtimestamps. Heldrev2/raw4rows and finalrev3/raw5 follow source. Healthy quiescence before fixture listener abort/join, synchronous actual soleStore destruction then exactlyowner+1reopen. Freshhost GET-only equal histories, including settled attach if bounded, no validation/callback/attempt replay; positive listener/owner/directory cleanup. Every gate/connect/write/header/body/poll/lifecycle finite, with panic guards. No production/helper edit or graceful listener guarantee. FirstGREEN on existing behavior is honest; actual discrepancy triggers narrow source/design repair, no manufacturedRED/weakened oracle.

## Task2: metadata and guards

- [x] Fresh metadata author changes only16paths, obtains actual metadataRED/GREEN, then source and quality approval.

Promote only T013NATIVE/native_read_only_attachment_state_tcp_sqlite, exact test and conformance/release/t013-attachment-state/qualification.json. Thirteenproofs43pending. Preserve89prior guard meanings and add7bounded new(96expected); fixture has no inherited T010feature_validation. Validate actual receipt/testidentity/hash, exact19commands/first16runtime, process/map/log/digest/status/credit fields and initial proof ownmap/no duplicatecredit. New bounded negatives cover missing/wrongidentity/proof/command/credit/path/hash/source/declaration with validbaseline. Elevenheadline modules literal-only. Globalcore/COMPAT/M0open/CP001–009partial/CP010unresolved and historicalfoundation counts retained. No wholechecker until actualreceipt delivery.

## Task3: final verification

- [x] Freeze final descriptor initial+16metadata; run all19fresh categories, generate real receipt outside source only afterward, obtain integratedresults review.

Ordered commands (all pinned cargo locked/offline except fmt):
1 scenario-test: test -p b4-http --test attachment_state_tcp release_t013_live_and_settled_attach_start_with_state_without_execution --locked --offline -- --nocapture --test-threads=1
2 attachment-state-tests: test -p b4-http --test attachment_state_tcp --locked --offline -- --test-threads=1
3 native-stream-api-tests: test -p b4-runtime --test native_streaming --locked --offline -- --test-threads=1
4 native-stream-control-tests: test -p b4-http --test native_streaming --locked --offline -- --test-threads=1
5–16 t001-regression through t012-regression: complete native_tcp, thread_state_tcp, thread_admission_tcp, thread_route_tcp, thread_json_tcp, input_shape_tcp, thread_concurrency_tcp, different_thread_tcp, wait_output_tcp, stream_payload_tcp, terminal_frame_tcp, disconnect_completion_tcp targets, each test -p b4-http --test TARGET --locked --offline -- --test-threads=1
17 format-check: fmt --all -- --check
18 scoped-lint: clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings
19 guard-tests: pinned Python-B scripts/test_core_release_traceability.py.

First16runtimecategories only; actual reportedsummaryexecutions not unique inventory. Initial proof ownmap/noextra credit. No new fullsuite/feature claim. Protect all24earlier receipt/testpaths and all baseline non-delta bytes.

## Task4: review and reversible adoption

- [x] Freeze sevenactualartifacts, read elevenactualreviewdecisions/scopes, write explicitRootdecisions and freeze/review semanticmanifest, then reversiblecopy.

Required reports design-review,plan-review,controller-review,qualification-controller-review,task1-source-review,task1-quality-review,task2-source-review,task2-quality-review,copy-controller-review,integrated-results-review,delivery-review(all.md). Sevenartifacts qualifier/adopter/publicmetadata source plus generatedqualification/actualREADME/revieweddesign/plan. root-review-decisions.json binds exact11explicitapproved/hashentries and native descriptor; review-approvals.json binds decisionsdigest+7artifacts. Independent manifest-review plus root-manifest-decision bind actual contents/conditions, no keywordinference or humanpause. Eighteenoriginalbackups before firstmutation;17delta copy then completepublic==native BEFORE adding generatedq/README; exactpublicnative+2, bothnewdestinations absent, allworkspace/protectedbytes preserved.

## Task5: public checks and acceptance

- [x] Wholepublicchecker+same12metadata modules, aggregate actual13records, independentintegrityreview and Rootacceptance before planclosure.

Whole60s/modules600s/max3active. Start long acceptedOnce/CLI/startupconfinement early/refillslots. Same12modulemembershipasT012; exactoneactualrecordeach+whole, fullunchangedmaps/loghash/command/status/reaping/no-timeout/no-history. Metadataonly/no runtimecredit. Finalintegrity validates17delta18backups24protected/complete nativepublicmaps/actual19checks/allapprovals/scope. Rootaccepts, closesfirst6of7boxes, capturespostacceptsource before nextdocs.

## Task6: continue

- [ ] ContinueT014 unless usersteers or genuine manualaction/approvalrequired; no milestonefinal.
