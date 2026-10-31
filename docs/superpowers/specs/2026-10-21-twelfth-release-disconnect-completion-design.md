# Disconnected Native Run Completion Design

Prospective planning only. Implementation follows actual T-011 Root acceptance and reviewed plan/controllers.

## Selected contract and scope

Qualify T-012 with two fixed real-TCP/owned-SQLite cases: one admitted primary stream client and one admitted wait client disconnect before the genuine callback returns, while authoritative work still completes once. Use the existing opt-in native host; stream selects its streaming entry and wait preserves its ordinary controlled entry. No production change is planned. This is durable completion evidence after client close, not evidence that a disconnected client received a result or a terminal frame. Other transport, cancellation, permission, attachment, retry/idempotency, provider and full-core/COMPAT scenarios remain separate.

## Positive admission, close and completion evidence

Use two sequential distinct thread IDs with independently declared original inputs/results/state writes and exact captured invocation/context. One immutable application exposes a Pure approval-free route; callbacks record actual entry/return and use finite local gates. Require actual callback entry while its gate is held and returned count remains zero before closing each whole client socket. Stream can additionally collect a bounded header prefix as admission evidence; wait must not await response headers, which settle after completion. Preserve honest distinction between positive callback entry and header/admission intent.

Dropping the client socket is the concrete close action. Do not infer a universal server-observed disconnect from a quiet interval, demand specific packet boundaries or synthesize a successful response. While callback remains positively held, independently observe its exact InFlight task/attempt, Running execution, unchanged admitted input, zero callback return and positive active owned work. Any observed store reference is supporting lifetime evidence, not sole proof of worker ownership while the host itself retains the store. No extra admission/capacity matrix or cancellation mutation is needed.

Release explicitly before the finite callback deadline. Independently observe one committed Succeeded task result and state write, Completed execution, the same attempt/owner/context, exactly one callback/validation and healthy physical retirement. An optional supplied cancellation observer must remain clear; client close is not accepted cancellation. Wait for actual active_owners0 in Serving/nonfaulted state before admitting the second case under capacity1. Store reads and positive counters establish completion, not the lost HTTP response. Trace stable source before fixing literal revision/journal expectations.

## Durable and physical lifecycle

Require complete typed views and full7/5/9-column raw thread/execution/journal records including creation rows, independent task/output/aggregate literals, provenance, stable revision/status/kind chains and bounded monotonic timestamps. Each ordinary single-task case is expected to retain one attempt and the normal five raw journal rows after success; positively held dispatch has its pre-completion history. Existing histories stay unchanged when the next case completes.

After both cases settle, require healthy quiescence before fixture listener abort/join. Destroy the actual sole SQLite owner synchronously, reopen at owner generation exactly+1 and use a fresh host for GET-only unchanged histories. Assert no callback/validator replay, new attempt or journal mutation; finish with positive listener/owner/directory cleanup. All gates, callback holds, request/connect/header reads, store polling and lifecycle waits are finite, with panic cleanup. No production test hooks or shared fixture edits.

## Exact increment and evidence policy

Add only crates/b4-http/tests/disconnect_completion_tcp.rs, exact release_t012_disconnected_stream_and_wait_preserve_admitted_work. A fresh author delivers source; Root formats only it, freezes baseline+test initial map, obtains source approval, runs honest first proof and obtains quality/results review. Existing implemented behavior may first pass; no manufactured RED. Actual implementation discrepancies reopen the source/design gate rather than weakening the durable oracle.

Separate metadata author changes only the established16metadata paths. Promote T-012 only to NATIVE/native_disconnected_run_completion_tcp_sqlite, receipt conformance/release/t012-disconnect-completion/qualification.json. Twelve proofs/44pending, retain82prior guard meanings plus7bounded new guards(89expected), preserve eleven prior receipt/source/policy scopes including T-010's separate feature evidence and final-alias rejection. Eleven headline scripts literal-only; global flags/history remain open and unchanged.

Final policy18ordered categories: named scenario-test, complete disconnect-completion-tests, runtime native-stream-api-tests, HTTP native-stream-control-tests, t001-regression through t011-regression, formatting, strict two-crate all-target lint and metadata guards. Only first15runtime categories count. Earlier policies are unmodified; no new full-suite/feature proof or transferred historical credit. Initial proof retains its own source snapshot without duplicate credit. Actual receipts/logs/maps/process records bind final checks before real qualification generation outside source.

Exact baseline delta17paths (one new test+16metadata),18backups (16existingmetadata+2already-published design/plan documents),22protected prior acceptance-test/receipt paths and every other baseline byte preserved. Assert actual inventories; planning expectation baseline1165/native1166/public1168 assumes actual acceptedT0111163 plus2published documents. Complete native equality precedes adding generated proof/prose. Eleven actual review reports and seven delivery artifacts are frozen with explicit Root semantic decision bindings, independent manifest gate, reversible copy and public source equality.

Whole public metadata plus the same twelve modules follow, max3active, whole60seconds/module600seconds, finite process-group cleanup and complete unchanged maps/log/command/status records, no runtime credit. Independent final public integrity and Root acceptance precede plan closure and T-013 continuation. No Git writes, routine permission pauses or milestone finals; public source freezes during private authoring/verification.
