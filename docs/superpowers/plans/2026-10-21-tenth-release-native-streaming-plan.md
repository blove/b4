# Native Stream Events Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development with a fresh author per task and independent source/spec then quality/results reviews. Do not inherit the coordinator's full history. User autonomy authorizes routine reversible work; no Git writes, routine permission pauses or milestone finals.

**Goal:** Add explicitly opted-in ordinary native stream events and qualify the representative T-010 TCP/SQLite payload case.

**Architecture:** Preserve existing constructors and callback entry behavior. A new host constructor selects a private stream-only mode; a borrowed bounded chunk sink projects direct chunk values, trusted tool lifecycle events use invocation identity, and successful tool_result is published exclusively after durable task completion. Cancellation and retained-work ownership remain authoritative.

**Tech Stack:** Pinned Rust 1.93.1, existing Tokio/Axum/serde_json/rusqlite, offline locked Cargo. No dependencies, kernel or durable schema changes.

## Exact source map and constraints

Task 1 modifies only `crates/b4-runtime/src/application/model.rs` and `registry.rs`; creates `crates/b4-runtime/tests/native_streaming.rs`. Existing model wildcard exports make new public types available without another export file.

Task 2 modifies only `crates/b4-http/src/application.rs`, `host.rs`, `control.rs`, `worker.rs`, `lib.rs`; creates `crates/b4-http/src/native_stream.rs` and `crates/b4-http/tests/native_streaming.rs`. The new private module isolates the bounded sink/serialization adapter. Existing control tests may be extended in control.rs; shared fixtures and prior tests are untouched. No broad file split/refactor.

Task 4 adds only `crates/b4-http/tests/stream_payload_tcp.rs`. Task 5 modifies the established sixteen metadata files. Thus final baseline delta is seven existing production files, sixteen existing metadata files and four new files: twenty-seven paths, no removals. First backups cover twenty-three existing native inputs plus the two already-published design/plan files (twenty-five), with no fictitious backups for four new files. Re-evaluate and review this exact map before implementation if source fit exposes a genuine missing surface; do not silently expand it.

Published documents are `docs/superpowers/specs/2026-10-21-tenth-release-native-streaming-design.md` and `docs/superpowers/plans/2026-10-21-tenth-release-native-streaming-plan.md`; stage them as delivery/tenth-thread-design.md and delivery/tenth-thread-plan.md. The plan has eight task checkboxes; after final Root acceptance close the first seven, leaving continuation open.

All eighteen T-001–T-009 test/receipt paths remain byte-identical throughout. Public source remains frozen while the ordinary private workspace is used. No branch/commit/worktree operations; .git is read-only. Use existing shared build cache outside source, and current full inventory including vendor/rusqlite/Cargo.lock.

## Task 0: reviewed preparation

- [x] After T-009 Root acceptance, publish the reviewed design/plan, capture an equal ordinary isolated baseline and prepare/review setup, fixed-step verification, qualification, copy, manifest and public-metadata controllers. Do not reuse the old seventeen-path adopter. Freeze exact twenty-seven-path policy and eighteen predecessor protections before source authors run.

Controllers use pinned commands, full before/after maps and log hashes, exit/reaping/timeout records and no historical result transfer. Normal steps have 600-second deadlines. The one full workspace feature step has 1800 seconds, justified by the historical ~973-second run; poll no longer than 45 seconds and keep user updates within 60 seconds. Timeout group cleanup uses TERM, a full monotonic five-second grace even if the leader exits, guarded group KILL and final reap. A timed-out or drifted run supplies no credit.

## Task 1: additive runtime callback API

- [x] A fresh author first writes failing public API tests in runtime/tests/native_streaming.rs, runs the fixed API target and records honest RED, then implements the smallest model/registry addition and records GREEN. Source freezes during every execution. Obtain independent source and quality/results approval before Task 2.

Freeze this interface (names may be revised only during review before implementation):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeChunkResult { Accepted, Unavailable, Closed, Limit }
pub trait NativeChunkSink: Send {
    fn emit(&mut self, chunk: &serde_json::Value) -> NativeChunkResult;
}
// New default method on NativeEffect:
fn invoke_streaming(
    &self,
    invocation: &EffectInvocation,
    context: &InvocationContext,
    observer: &NativeCancellationObserver,
    _sink: &mut dyn NativeChunkSink,
) -> Result<ControlledNativeOutcome, EffectUncertain> {
    self.invoke_controlled(invocation, context, observer)
}
```

The matching NativeApplication entry selects/validates the same binding/context, calls this new method and applies the same output-kind and cancellation-acknowledgement checks. Factor only directly shared validation if needed; keep old entries and public struct literals intact. Tests prove a legacy invoke still runs, a cooperative override is called instead of falling back to invoke, genuine chunk delivery to a recording borrowed sink, and rejection of invalid identity/output/cancellation without false callback success. Use independently declared expectations, no implementation-mirroring operation matrix.

Exact target: `cargo test -p b4-runtime --test native_streaming --locked --offline -- --test-threads=1`. Initial undefined-API compilation is honest initial RED; subsequent meaningful delegation/validation assertions must actually pass. No manufactured failures or claims that an existing-behavior first GREEN was RED.

## Task 2: compatible HTTP event bridge

- [x] A fresh HTTP author writes focused failing controls/host tests, runs them through the bounded controller, then implements the reviewed bridge and records GREEN. Obtain independent source and quality/results approval; protect Task 1 source and prior evidence unless a concrete reviewed integration correction is necessary.

Add `new_with_streaming_application` beside the existing native constructor, building the existing host and setting its private mode before escape. All old builders initialize mode off; no public config/effect/result struct field is added. Select the new callback entry only when this mode is on, operation is exactly RunStream, this is a new run and execution reaches the ordinary driver purpose. Do not use streaming(operation), which includes Resume/Recover. Same-host RunWait remains invoke_controlled with legacy digest; permission continuation/Resume/Recover paths remain legacy. Document these exact limits in rustdoc.

Trusted entry/tool_call publication is one coherent gate decision. Construct bounded fields from attempt_id/effect_id/wrapped input; never accept an event name or lifecycle frame from callback code. No lock survives into callback code. The borrowed adapter submits direct Chunk Values. Pending/lock contention returns Unavailable without condition waits or hidden queue; settled/closed refuses. Per-payload encoded cap is max_output_bytes, cumulative auxiliary frame/byte caps are existing turn-buffer limits, depth is 64 and preparation work/serialization are finite before cloning. Limit closes the projection/viewers and truncates digest without rolling back work. Keep actual callback/task/global/Store retirement unchanged.

After a committed successful result, the worker uses its exact StoredExecution task/attempt identity to publish tool_result. This is outside callback and persistence retry. The postcallback trusted publication wait may use the existing settled gate, backed by the focused independent cancellation-decision proof; no hidden unbounded lifecycle queue. Failed/cancelled/uncertain outcomes have no successful tool_result. Existing terminal projection alone emits done.

Tests must cover opt-in stream versus old host and same-host wait entry/digest behavior, exact purpose predicate exclusion, accepted direct chunk, bounded payload/cumulative overflow/late refusal, Pending submission returning before its held decision resolves, rejected cancellation preserving committed-result projection, and accepted cancellation suppressing false success while retiring actual work. Keep a small set of concrete cases; no full race/event/scalar matrix. Use real positive gates with finite deadlines, not sleeps or quiet-time universal absence assertions.

Exact host target: `cargo test -p b4-http --test native_streaming --locked --offline -- --test-threads=1`. Any private helper/control unit tests run explicitly during this task and in Task 3's full feature suite. They are not later misreported as final-map unit executions.

## Task 3: feature regression on its own map

- [x] After approved API/bridge GREEN, Root formats/checks source, freezes feature-source-descriptor.json, reviews exact feature delta (seven existing files plus three new files), then runs one fresh full workspace suite and obtains integrated feature approval.

Use `cargo test --workspace --locked --offline -- --test-threads=1` with the separately reviewed 1800-second deadline. Each actual reported summary/failed/ignored count, command/log hash, complete unchanged map and process result is retained. Do not call reported passes unique tests. No metadata edits or independent wire test exists in this feature map. Fix only actual failures with systematic debugging and fresh author/review loops; do not transfer old suite results. No source changes after this gate except the independent wire test and established metadata, unless feature review/full-suite gate is explicitly reopened.

## Task 4: independent genuine T-010 framing proof

- [x] A fresh author adds only stream_payload_tcp.rs, exact `release_t010_native_stream_payloads_are_direct_after_commit`. Root formats/freezes, reviews source, runs its honest first proof on feature-plus-test map, records task acceptance-test results, then obtains quality approval.

Use one Pure callback, explicit opt-in primary RunStream, one fixed Unicode/quote/newline chunk, a distinct structured output and independently declared tool_call/chunk/tool_result/done payloads. Strictly parse actual application frame grammar/JSON; preserve header-prefix body bytes and bound complete TCP collection. The callback receives no permission to forge tool frames. Exact trusted tool id/name/input must agree with actual admitted attempt/effect/wrapped input.

A local delegating Store holds after successful inner SQLite completion commit but before returning to the driver. Independently observe committed Succeeded result/journal at this positive boundary, release it, observe tool_result and require the same durable identity/result. No production test hooks or universal no-bytes timing claim. Require one actual callback/validator/task/attempt, independent task/direct versus aggregate output, complete raw/typed records, healthy quiescence before fixture stop, synchronous sole owner release, owner+1 reopen and GET-only/no replay/final cleanup. This case does not qualify T-011/T-012/T-013/T-017 or provider/TypeScript/full-core behavior.

## Task 5: release metadata

- [x] A fresh author changes only requirements, checker, core guards, README, compatibility and the eleven existing headline scripts. Preserve nine prior rows/eighteen test-receipt paths, original descriptors/5–13 command lists/history and global open flags. Promote only T-010, scope native_stream_event_payloads_tcp_sqlite, exact new test and conformance/release/t010-native-stream-payloads/qualification.json. Ten independent proofs/46 pending; retain 68 prior guard meanings and add seven bounded guards (75 expected), with honest metadata-only RED/GREEN.

Final T-010 command policy is exactly sixteen checks, in order:
1. scenario-test (named new wire case)
2. stream-payload-tests (complete new wire target)
3. native-stream-api-tests (runtime/tests/native_streaming.rs)
4. native-stream-control-tests (HTTP/tests/native_streaming.rs)
5–13. t001-regression through t009-regression (existing complete native targets)
14. format-check (`cargo fmt --all -- --check`)
15. scoped-lint (`cargo clippy -p b4-runtime -p b4-http --all-targets --locked --offline -- -D warnings`)
16. guard-tests (pinned Python -B scripts/test_core_release_traceability.py)

Only first thirteen categories carry runtime credit. The full workspace feature proof keeps its own descriptor and no additional final-qualification credit; do not claim it used later metadata bytes. Eleven headline literals advance from nine proofs/47 pending to ten proofs/46 pending, every other assertion unchanged. Do not run whole metadata before real receipt delivery.

## Task 6: final proof and exact adoption

- [x] Freeze/review final native source; run all sixteen fresh checks on that map. Generate qualification only afterward, binding feature gate/independent proof to their own maps and same final production/test digests. Review actual results, staged prose, exact production/API/test/metadata delta, new qualifier/adopter and full review manifest semantically. Adopt with complete baseline equality, twenty-five backups and full final native equality BEFORE generated proof/prose; then whole public metadata plus twelve modules (max three active/600 each), final integrity and Root acceptance. Close completed checkboxes only after acceptance.

The final semantic approval manifest requires seventeen actual reports: design-review.md, plan-review.md, controller-review.md, qualification-controller-review.md, api-source-review.md, api-quality-review.md, bridge-source-review.md, bridge-quality-review.md, feature-source-review.md, feature-results-review.md, task1-source-review.md, task1-quality-review.md, task2-source-review.md, task2-quality-review.md, copy-controller-review.md, integrated-results-review.md and delivery-review.md. API/bridge gates cover their author tasks; feature source/results cover the combined feature map and actual full suite; task1 is the independent wire case; task2 is metadata. A review report from one stage cannot substitute for a later actual result. Freeze and semantically inspect all seventeen decisions/hashes and the seven delivery artifacts before copying.

Expected ordinary baseline after T-009 acceptance and two published T-010 documents is 1,152 files; four new source/test files produce 1,156 native files; two generated conformance files produce 1,158 public files. Counts are checked against actual inventory rather than assumed. New exact-delta controllers and review manifest must account for all feature gates, without substring-only approvals. Keep core/COMPAT unqualified, M0 open, CP-001–009 partial and CP-010 unresolved. Separate provider/header/graceful-listener/max-capacity/replay guarantees remain out of scope.

## Task 7: continue

- [ ] Continue the next core release scenario unless the user steers or a real manual action/approval is required. Do not stop for milestone confirmations or Git operations that this workspace cannot perform.
