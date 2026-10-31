# Normal Native Stream Terminal Design

Planning draft; implementation begins only after T-010 Root acceptance and independent design/plan/controller gates.

## Scope

Qualify T-011 through one bounded, independent real-TCP/owned-SQLite test of four selected normal native stream outcomes: structured Value success, explicit null Value success, Void success and known application failure. Read each complete response through EOF, require exactly one terminal done, require it is last, and compare independently declared payloads. The whole core/COMPAT profile and all-outcome generalization remain open.

Canonical input is core section14.4 T-011 plus SSE-003/004/005 and existing frozen native/compatibility behavior. Socket failure or projection overflow can prevent terminal delivery; this proof uses healthy attached primary POST streams within configured bounds. Permission parks, cancellation, uncertain callbacks, disconnect, GET attachment, arbitrary packet splits and provider behavior retain their own scenarios. Do not invent a quiet-socket absence oracle.

## Application and independent wire expectations

Use the existing explicitly opted-in HttpHost::new_with_streaming_application and one immutable NativeApplication with four distinct Pure, approval-free routes. Each route has its own original input, validator counter, callback entry/return counter and independently recorded invocation/context. All runs execute sequentially under one host with capacity1; this adds no concurrency qualification. Callbacks may use the existing default streaming delegation and return the selected outcome without chunks. No callback or fixture may forge tool/done lifecycle frames.

Expected application frame sequence for each successful route is trusted tool_call, successful tool_result, done. Known failure is trusted tool_call, done with its existing committed failure output shape, and no successful tool_result. Count done explicitly and reject every application frame after it. Independently declare trusted attempt/effect/wrapped-input relationships, direct task output and terminal shapes: structured output uses done.output equal to its literal value; null Value uses {output:null}; Void uses {}; known application failure uses the existing done.output.error message. Derive no oracle from SseEvent::encode or the received response. Read source before fixing durable failure literals.

Strictly consume actual AP SSE grammar and JSON, explicitly handle legal heartbeat comments, reject malformed or unknown application frames/fields and duplicate data/event fields, preserve any body bytes read with the HTTP header prefix, and use finite whole-response size/time bounds through EOF. This is not a claim about exact TCP chunks or every possible serialization split.

## Durable and physical lifecycle evidence

Each admitted execution has one genuine validator/callback/task/attempt and exact trusted context. Successful results and writes must match independent literals; retained aggregate state/output is distinct from the task output, including null and Void cases. Known failure must retain its actual failed task/execution semantics and healthy host state, without an invented success result. Require complete typed views and full7/5/9-column raw thread/execution/journal records, including creation rows, exact stable revision/status/kind chains, original input, provenance and bounded monotonic timestamps. Establish literal journal expectations from existing source, not the response. No production test hook is needed.

After all four complete streams, require healthy work quiescence before fixture listener abort/join. Destroy the actual sole SQLite owner synchronously, reopen with owner generation exactly+1, and use a fresh host for GET-only observations of all four histories. Assert unchanged callbacks, attempts and durable records, then positive listener/owner/directory cleanup. Transient streams are not replayed after reopen.

## Exact source and verification boundaries

No production or existing Rust test edit is planned. Add only crates/b4-http/tests/terminal_frame_tcp.rs with release_t011_normal_native_streams_end_with_one_done. Root formats only the new file, freezes baseline plus new test initial map, obtains independent source review, runs honest first proof and obtains quality/results review. First GREEN is appropriate for existing implemented behavior; do not manufacture RED. Concrete source-fit or behavior discrepancies reopen the design instead of silently expanding scope.

A separate author changes the established sixteen metadata files only: requirements/checker/core guards/README/compatibility and eleven headline scripts. Promote T-011 only, scope native_normal_stream_terminal_tcp_sqlite, receipt conformance/release/t011-normal-stream-terminal/qualification.json. Eleven proofs/45pending, retain75 prior guard meanings plus7 bounded new guards(82expected), preserve all ten prior rows/policies/descriptors/receipts, T-010 separate feature_validation and global open flags. Headline scripts get only their count/list literal update. No current checkout digest recomputation or receipt cycle.

Final policy has17ordered checks: named scenario-test, complete terminal-frame-tests, runtime native-stream-api-tests, HTTP native-stream-control-tests, t001-regression through t010-regression, format-check, strict two-crate scoped-lint, guard-tests. First14 categories carry runtime credit. t010-regression is the complete stream_payload_tcp target; earlier policies remain byte-identical. Private selector/full feature tests are not additional current qualification categories or duplicate credit. A new full workspace run is not required for this test/metadata-only increment; preserve T-010's historical own-map feature proof without transfer.

After fresh final-map checks, generate the real receipt, review actual results and frozen delivery, semantically inspect actual decisions/hashes, then perform reversible exact adoption. Preserve all twenty prior test/receipt paths and every baseline byte outside the17-path delta (one new test plus16metadata), with18first backups for16existingmetadata+2published design/plan documents, no fictitious backup for the new test. Require complete native equality before generated qualification/prose files; public count adds2. Expected baseline1160/native1161/public1163 only if actual T-010 acceptance count1158 and two published documents agree; assert actual inventory rather than assume counts.

Whole public metadata and the same twelve modules run afterward, at most3active,600seconds per module with whole-metadata60seconds. Keep maps/log/commands/process receipts and no native credit for metadata. Independent public integrity and Root acceptance precede closing completed plan boxes and moving to T-012. User autonomy authorizes routine reversible work; no Git writes or milestone permission pauses. All public source stays frozen during private authoring/verification.
