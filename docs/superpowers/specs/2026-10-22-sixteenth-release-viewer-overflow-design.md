# Viewer Capacity and Queue Overflow Isolation Design

Prospective T-016 design. T-015 is accepted at
`.b4-work/fifteenth-release-attachment-truncation/root-acceptance.json`. This
document defines a selected native proof; it does not itself qualify T-016.

## Contract and selected scope

T-016 requires viewer capacity and queue overflow to affect viewers rather than
the authoritative run. ATTACH-003 and ATTACH-004 require bounded viewer counts,
bounded per-viewer frame and byte queues, terminal detachment for a limited
viewer, and continued authoritative work. The current implementation already
has these mechanisms: `RunControl::subscribe` returns a finite capacity
detachment, `ViewerSender::send` closes only the queue that exceeds its bound,
and the subscription guard removes a detached viewer from the run. Existing
attachment tests exercise pieces of this behavior, but T-016 lacks one dedicated
native TCP/SQLite release proof that binds the capacity and overflow effects to
the same still-running execution and its eventual durable result.

Select a qualification-only approach. Add one dedicated acceptance test and no
production change unless that test exposes a genuine contract defect. The
selected scope is one attached-viewer count refusal followed by one deterministic
attached-viewer byte-queue overflow. Prove that each decision terminates only its
viewer, that overflow releases the occupied viewer slot, and that a replacement
viewer observes the same run's later authoritative terminal. Do not claim every
frame/byte combination, primary POST viewer overflow, arbitrary operating-system
socket backpressure, or a universal scheduling result.

A broad matrix across primary and attached viewers, frame and byte limits, and
multiple socket-pressure patterns would support a larger claim but would add
several independent races and obscure the release scenario. Adding production
test hooks would make queue timing easier to control but would expand the public
implementation solely for qualification. Neither alternative is needed for this
bounded scenario.

## Acceptance architecture

Create `crates/b4-http/tests/viewer_overflow_tcp.rs` with the exact test
`release_t016_viewer_capacity_and_queue_overflow_do_not_stop_run`. Use the
existing opt-in native streaming application boundary with one approval-free
`Pure` task, a real loopback HTTP listener, and an owned SQLite store. Ordinary
and controlled callback entries must fail the fixture; only the explicit native
streaming entry is valid. The callback uses finite gates installed before
admission so panic cleanup can always release it.

Configure `max_attached_viewers` to one. Freeze a positive test-owned
`viewer_queue_bytes` budget, the trusted `tool_call` event name and expected JSON
payload, the selected live event name and JSON literal, the successful
`tool_result` event name and expected JSON payload, and the authoritative `done`
event name and JSON literal. Construct the expected `tool_call` and
`tool_result` payloads from the independently observed durable attempt ID, fixed
effect name, frozen input, and frozen task result; do not derive them from
callback-returned data. Assert the attempt ID's contract-constrained serialized
length before relying on its fixed expected frame sizes. Independently calculate
all four encoded frame sizes and their checked terminal-burst sum, then assert:

`selected live frame bytes > viewer_queue_bytes >= tool_call frame bytes`

`viewer_queue_bytes >= tool_result frame bytes + done frame bytes`

Set `viewer_queue_frames` to at least two so both terminal-path frames can coexist
without frame-count overflow. Keep turn-buffer bounds, snapshot bounds,
input/output bounds, payload depth, active-run capacity, and task capacity ample.
The selected frame must fit the turn digest and every native/payload bound while
exceeding the attached viewer byte queue. The disconnected primary sender may
still exist internally and may also overflow; that behavior is intentionally
unobserved and earns no release credit. This separates the observed attached-
viewer queue overflow from turn truncation, frame-count overflow, snapshot
failure, native publication closure, terminal-burst overflow, and execution-
capacity refusal.

The size oracle is test-owned. Serialize the four frozen JSON values locally,
apply the independently stated SSE frame accounting formula to their fixed event
names, and compare every calculation plus the checked `tool_result`/`done` sum
with hard-coded expected sizes before using the queue budget. Do not call the
crate-private production encoder, `Frame::new`, derive an expected size from
callback-returned data, or measure a captured wire frame. The positive
`NativeChunkResult::Accepted`, exact digest, overflow detachment, exact
`tool_result`, and later exact terminal remain separate behavioral oracles.

Start the native callback through POST `RunStream`, the only operation that
selects the native streaming entry and provides its chunk sink. While the
callback is held before the selected live frame, completely validate the primary
stream's trusted `tool_call`, then deliberately close that client connection.
Positively recheck that the same stored execution remains `Running`, its sole task
remains `InFlight`, and the expected owner, attempt, and active callback remain
present. The primary client is abandoned before the selected oversized frame and
supplies no later overflow or terminal evidence. Do not infer when the server
drops its internal sender and do not claim primary-viewer queue behavior from
this scenario. No viewer response or EOF may be used as proof of execution
settlement.

## Ordered data flow

1. Start the native run through POST `RunStream` and hold its callback before the
   selected live frame. The frozen size relation must prove that the complete
   trusted `tool_call` fits the primary queue. Completely parse that exact event,
   close the primary client, and positively prove the same work remains held.
   Capture the complete typed and raw SQLite histories at this boundary. The
   disconnected primary contributes no success, overflow, or terminal claim.
2. Open the first authorized GET attachment. Read its complete initial `state`
   frame and leave the live stream open. The state must identify the selected
   execution and show the exact trusted `tool_call` once in live,
   non-truncated history at the captured cut.
3. While the first attachment owns the sole slot, issue a second independently
   authorized GET. It must return HTTP 200 with exactly one
   `detached {"reason":"capacity"}` event and EOF. It must disclose no state,
   route, queue limit, or run terminal. The first viewer, callback, and durable
   `Running/InFlight` state remain unchanged.
4. Release the callback only far enough to publish the selected frame whose
   encoded byte size exceeds the first viewer queue. Require the native callback
   publication result to remain accepted: viewer overflow is not native digest
   overflow. The first attachment must then yield exactly one
   `detached {"reason":"overflow"}` event and EOF after its already validated
   state, with no `done` and no fabricated failure.
5. Keep the callback held after publication. Positively recheck
   `Running/InFlight`, unchanged result absence, the expected publication count,
   and the typed/raw history prefix. Open a replacement GET attachment and prove
   it receives an exact live state for the same execution. Its turn digest must
   contain the trusted `tool_call` followed by the selected frame exactly once,
   demonstrating that the attached overflow discarded no authoritative digest.
   Fully consume and validate this initial state prefix before releasing the
   callback, leaving the replacement's bounded live queue empty.
6. Release the callback. The replacement attachment must receive exactly its
   already validated `state` prefix, one exact `tool_result`, one exact `done`,
   and EOF, in that order. Reject overflow detachment, missing or reordered
   frames, duplicates, extra application frames, and post-terminal bytes. The
   frozen frame-count and cumulative-byte inequalities must establish
   independently that the complete `tool_result`/`done` burst fits the empty
   replacement queue, so a missing terminal cannot be excused as a second
   overflow. The replacement terminal and independently read durable task result
   must equal the same frozen value. The capacity-refused and overflowed viewers
   remain closed and receive no later terminal.
7. Verify durable `Succeeded` task state, present exact task result, completed
   execution, aggregate values, one callback, one task attempt, and the full
   expected histories. Retire the server normally, destroy the sole inner SQLite
   owner, reopen at owner generation plus one, and perform GET-only preservation
   checks proving no callback, attempt, or result replay.

Every callback gate, socket operation, header/body read, durable-state poll,
server join, and cleanup wait is finite and has an independently installed panic
release guard. Parse complete HTTP chunking and SSE frames with prefix-preserving
logic. Reject malformed or duplicate JSON fields, extra application events,
post-terminal bytes, and unknown framing. Legal heartbeat comments may be
ignored only where explicitly allowed.

## Error handling and privacy

The capacity path is an authorized HTTP 200 finite SSE response containing only
the capacity detachment. Authorization renewal must complete before that
diagnostic is visible. A denied or stale request must retain its protected 404 or
409 behavior and must not reveal viewer occupancy.

The queue-overflow signal is best-effort for a viewer that still reads. The
acceptance test deliberately reads it, but the product claim remains that a
stalled socket may observe only EOF. Queue overflow clears and closes only that
viewer's buffered projection; it must not close native publication, clear the
turn digest, fault the host, cancel the callback, mutate durable state, or select
a terminal result.

Any genuine pre-metadata failure must be classified before repair. A failure in
existing production behavior may justify a narrowly reviewed fix. Compile
errors, fixture timeouts, parser mistakes, unrelated lifecycle failures, or a
weakened oracle are not evidence of a runtime defect. If a production change is
needed, amend and re-review this design before implementing it.

## Qualification and release boundaries

The dedicated acceptance test is the only new runtime source anticipated.
Metadata later promotes only T-016 with release scope
`native_attached_viewer_capacity_and_byte_overflow_tcp_sqlite` and receipt
`conformance/release/t016-viewer-overflow/qualification.json`. The established
sixteen metadata paths advance the inventory to sixteen scoped native proofs and
40 pending scenarios while preserving all earlier scenario identities, receipts,
and meanings.

The final verification set has 23 ordered categories: the named T-016 scenario,
its complete test target, native-stream API tests, native-stream control tests,
the complete `b4-http` library tests, all fifteen T-001 through T-015 regression
targets, formatting, strict dual-crate all-target lint, and the metadata guards.
Only the first twenty runtime categories carry runtime credit. Formatting, lint,
guards, metadata fixtures, design review, and copied historical results add no
native runtime credit.

After accepted T-015, publishing the reviewed T-016 design and plan adds two
documents to the 1,183-file public map. The anticipated setup baseline therefore
has 1,185 files. The sole new acceptance file produces a 1,186-file native map;
the sixteen metadata edits do not change membership. Baseline-to-native is an
exact 17-path delta: one addition and sixteen modifications, with no removals.
Eighteen backups cover the sixteen existing metadata paths and the two published
documents. The thirty prior acceptance-test and qualification paths remain
byte-protected. Installing the new qualification and README yields an anticipated
1,188-file public map before later plan-checkbox closure.

Use the established private-workspace, full-source-map, bounded-process,
semantic-review, manifest, reversible-adoption, thirteen-public-check, integrity,
and explicit Root-acceptance gates. Exact counts remain predictions until setup
captures the actual maps. No Git writes are permitted. The release does not
qualify primary-viewer overflow, frame-count overflow, all queue sizes, arbitrary
network splits or backpressure, cancellation, providers, TypeScript behavior,
feature parity, full-workspace behavior, the full core profile, M0, CP-010, or
COMPAT.
