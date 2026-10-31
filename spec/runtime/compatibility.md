# Compatibility evidence and remaining characterization

Status: **authenticated scripted subset with required HTTP approvals; scoped
workflow comparison and independent grant observations**.
`b4-ap-core/0` remains a proposed release profile. Nine native HTTP operations run;
T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017 and T-018 are native-qualified for create/read thread metadata and timestamps,
empty-thread state unavailability, authorized implicit thread creation, unknown/missing-kind
route rejection without execution, malformed JSON AP errors without execution, and omitted/null/false
input shape preservation over real TCP and owned SQLite across reopen, plus same-thread
concurrent admission with one winner and different-thread concurrent execution within global limits
over real TCP and owned SQLite, plus successful wait returning direct output and JSON null
for void output, plus one representative opt-in ordinary primary native RunStream
case with direct tool/chunk/result/done payload framing, with successful tool_result after
independently observed SQLite commit, plus owner reopen; four selected normal
stream terminal cases; two fixed admitted stream/wait disconnect completion cases; and read-only live/settled
attachment starting with complete state without execution; one controlled attachment
cut with state A and live B exactly once; and one selected cumulative native frame
overflow with a new read-only null/truncated attachment and authoritative terminal;
one authorized attached-viewer capacity refusal and one attached-viewer byte-queue
overflow with a replacement viewer receiving the same durable terminal;
one selected real TCP quiet heartbeat and Unicode stream invariant under every byte cut;
and one required-durable approval park committed before TCP interrupt and authorized pending
disclosure, with separate grant/journal precommit faults disclosing neither prompt nor grant;
T-019–T-056 (38 scenarios) remain pending.
M0, CP-010, COMPAT and the full core profile remain open; CP-001–009 remain partial.
The [scoped T-001 evidence](../../conformance/release/t001-thread-roundtrip/README.md)
[T-002 qualification](../../conformance/release/t002-empty-thread-state/qualification.json)
[T-003 qualification](../../conformance/release/t003-authorized-implicit-thread/qualification.json)
[T-004 qualification](../../conformance/release/t004-invalid-routes/qualification.json)
[T-005 qualification](../../conformance/release/t005-malformed-json/qualification.json)
[T-006 qualification](../../conformance/release/t006-input-shapes/qualification.json)
[T-007 qualification](../../conformance/release/t007-same-thread-admission/qualification.json)
[T-008 qualification](../../conformance/release/t008-different-thread-concurrency/qualification.json)
[T-009 qualification](../../conformance/release/t009-wait-output/qualification.json)
[T-010 qualification](../../conformance/release/t010-native-stream-payloads/qualification.json)
[T-011 qualification](../../conformance/release/t011-normal-stream-terminal/qualification.json)
[T-012 qualification](../../conformance/release/t012-disconnect-completion/qualification.json)
[T-013 qualification](../../conformance/release/t013-attachment-state/qualification.json)
[T-014 qualification](../../conformance/release/t014-attachment-cut/qualification.json)
[T-015 qualification](../../conformance/release/t015-attachment-truncation/qualification.json)
[T-016 qualification](../../conformance/release/t016-viewer-overflow/qualification.json)
the [T-017 qualification](../../conformance/release/t017-heartbeat-splits/qualification.json)
and [T-018 qualification](../../conformance/release/t018-durable-disclosure/qualification.json)
bind fresh native checks separately from authored metadata guard fixtures, which carry
no native runtime credit. The eighteen scoped native proofs retain independent source descriptors and test hashes;
regression checks do not requalify predecessors. Historical increment sections and receipts below retain their
original scope and counts.
T-010 uses new opt-in streaming entry points; existing constructors, same-host Wait and excluded
purposes retain legacy behavior. Its fresh full workspace evidence belongs to the separate feature
source map before the wire proof and metadata, adds no final runtime credit, and makes no full-suite
claim on final metadata bytes. The scoped proof does not qualify all terminal outcomes, replay,
providers, TypeScript or the full core profile.

T-011 covers four selected complete healthy primary native streams: structured Value
uses the literal `done.output` value, null Value uses `{"output":null}`, Void uses
`{}`, and known failure uses `done.output.error`. Each ends with exactly one `done`
as its last data frame. Actual owner retirement and one physical SQLite reopen
verify settled state through GET-only reads, without replay. This bounded proof
does not cover every terminal outcome, cancellation, disconnect, parked turns,
attachment, providers, TypeScript or full core qualification. It adds no new full
workspace or feature-validation claim; metadata fixtures carry no native credit.

T-012 covers two fixed real TCP cases on owned SQLite: the whole local stream
or wait client closes after actual callback entry while the callback is held and
has returned zero times. The stream case enters the streaming callback; wait
enters the controlled callback. Cancellation observers stay clear. Explicit release
allows each admitted callback to return once, with durable Succeeded/Completed
state and the direct result distinct from aggregate state. Full typed and raw
7/5/9 histories agree. Healthy actual worker retirement and physical sole-owner
closure precede one fresh generation+1 owner with GET-only verification, without
replay, followed by cleanup. This bounded proof does not establish result delivery
to the closed client, server-observed FIN timing, arbitrary network failures,
cancellation, attachment, idempotency, graceful listener shutdown or full core
qualification. It changes no production behavior and adds no new full workspace
or feature-validation claim; metadata fixtures carry no native runtime credit.

T-013 covers one controlled RunWait callback held after entry while a read-only
live attachment over real TCP yields a complete first `state` before gate release.
The first live frame has exact busy state and no preceding retry control;
release produces the independent direct wait result and a single live `done`
with that exact output through EOF. After durable success and actual worker
retirement, a settled attachment starts with exact complete idle state and yields
retry 2000 then `done.output:null` through EOF. Full typed and raw 7/5/9 history,
including creation rows, and validation/entry/return/attempt counts remain
unchanged by attachment. The actual sole SQLite owner is destroyed and exactly
one owner+1 generation reopens for GET-only observations without replay.
This bounded proof adds no boundary-race, overflow, arbitrary-split, cancellation,
provider, TypeScript, full-workspace, feature-validation, full core or COMPAT
qualification; metadata fixtures carry no native runtime credit.

T-014 covers one selected controlled attachment cut with a real streaming
callback. The actual inner SQLite owned read-return capture gate holds after
releasing the SQL lock; A is Accepted while that capture is held. The real
coordinator then subscribes, and B is Accepted while the first exact Attach
policy call is held. Before the final callback release, the complete independent
attached `state.turn` is positively parsed as `[trusted tool_call, A]`, followed
by live B. The primary tool_call/A/B/tool_result/done and attached
state/B/tool_result/done streams are exact through EOF; A and B occur exactly
once on their selected sides. Held durable history stays unchanged. Independent
task result, aggregate state and context agree with full typed and raw 7/5/9
histories including creation. Healthy worker retirement and destruction of the
actual sole inner SQLite owner precede exactly one owner+1 fresh host with
GET-only preservation checks and no replay.
Committed storage observed before final wire consumption establishes observation
order; it does not independently instrument publication after commit. T-010's
separate ordering proof supplies no new T-014 credit. This controlled partition
adds no universal race, overflow, capacity, arbitrary-split, cancellation,
provider, TypeScript, graceful listener shutdown, full-workspace, full core or
COMPAT qualification, and no new feature-validation or transferred runtime
credit. Metadata fixtures carry no native runtime credit.

T-015 covers one selected cumulative native frame overflow. After trusted
`tool_call` and chunk A fill the two-frame projection, chunk B returns `Limit`
and later chunk C returns `Closed`. The producer stays closed and the original
viewer closes with its earlier complete frames. While owned work remains active,
a new read-only attachment starts with `turn:null` and
`turn_truncated:true`, then receives the authoritative terminal after the
callback settles. The proof does not generalize to every projection failure,
byte/depth or viewer-capacity limit, arbitrary network split, cancellation,
provider, TypeScript, full-workspace, feature-validation, full core or COMPAT
qualification. Metadata fixtures and the separate pre-fix and initial runs add
no final runtime credit.

T-016 covers one authorized attached-viewer capacity refusal and one deterministic
attached-viewer byte-queue overflow during the same native RunStream execution.
The primary client closes after its trusted `tool_call`; the refused viewer receives
only `detached {"reason":"capacity"}` and EOF. The first attachment receives
`detached {"reason":"overflow"}` after its state, while native publication remains
accepted and durable work stays Running/InFlight. A replacement attachment sees
the complete live digest, then exact `tool_result` and `done` frames through EOF;
an independent durable read confirms the same successful result. This selected
proof does not qualify primary-viewer overflow, frame-count overflow, all queue
sizes, arbitrary network backpressure, denied or stale authorization, cancellation,
providers, TypeScript, feature parity, full-workspace behavior, full core, M0,
CP-010 or COMPAT. Its initial proof and metadata fixtures carry no final runtime
credit.


T-017 covers one healthy native POST RunStream over real loopback TCP and owned
SQLite. A complete `: ping\n\n` comment arrives while the callback is held
after trusted `tool_call` and before its frozen Unicode `chunk`. One independent
bounded HTTP/chunk/SSE/UTF-8 parser consumes the full raw response, then replays
that same transcript across every single byte cut and bytewise delivery. Exact
application frames are `tool_call`, `chunk`, `tool_result`, and one terminal `done`;
durable result and owner+1 GET-only reopen agree. This fixed healthy transcript
does not prove arbitrary network schedules, other payloads, proxies, compression,
TypeScript clients, current-HEAD COMPAT, M0, CP-010 or full-core qualification.
The initial proof and metadata fixtures add no final runtime credit.

T-018 covers one required-durable approval park over real loopback TCP and owned
SQLite. Separate grant-insert and park-journal-insert precommit faults each leave
committed admission/Running evidence but disclose no prompt or grant; the accepted
stream closes without `interrupt` or `done`, and authorized pending reads return
fault-gated 503. In the healthy case, a complete `interrupt` and an authorized
pending response disclose a grant that an independent SQLite read confirms was
committed with the matching park snapshot and journal. A hard-killed server child
is reaped before owner+1 reopen confirms the same durable park and grant. This
fixed case does not qualify arbitrary crash points or recovery, post-restart HTTP
disclosure, all grant modes, resume, current-HEAD COMPAT, M0, CP-001–010, full
core or whole-product parity. Initial proof and metadata fixtures carry no final
runtime credit.

The architectural input is [B4-RUST-RUNTIME-001](0001-agent-protocol-core.md).
The current scope is [http-subset-design.md](http-subset-design.md) plus
[settled-failure-design.md](settled-failure-design.md), the embedded
[durable approval service](durable-approval-design.md) and its
[authenticated HTTP bridge](http-approval-design.md). The incumbent
source pin is `cacheplane/b4run` at
`8acac534ba6eb960b34de217b0da45b8405023af`. Source inspection guides authorship;
an independent executed comparison establishes only the behavior it exercises.

## Evidence classes

| Label | Evidence allowed | Meaning |
|---|---|---|
| `source_observation` | Pinned source and line ranges | Authoring input, never a runtime result |
| `source_test_observation` | Inspected pinned test expectations | Source evidence, not a test executed here |
| `NATIVE` | Reviewed design and independently authored expectations | Native subset requirement |
| `CHANGE` | Recorded restriction or deliberate difference | Visible difference requiring later qualification |
| `runtime_observation` | Independently executed pinned TypeScript fixture | Selected actual reference behavior |
| `COMPAT` | Executed native/reference comparison | Preserved behavior for the named cases and fields |

Examples under `contracts/agent-protocol/examples/` remain authored **NATIVE design
inputs**, distinct from reference recordings. Historical foundation and
transactional receipts retain their original scope and hashes. New HTTP evidence
does not retroactively qualify those artifacts or the broad release scenarios.

## Executed reference fixture

The fixture has seven independently authored exported workflow functions: input
probe, void, zero, false, empty string, fixed failure, and input validation. Its
62 request bodies cover selected CP-001–004 parsing/input/terminal cases. The
capture imports the actual freshly built TypeScript Fetch handler from an isolated
checkout of the exact pin. Raw bytes, SHA-256 hashes, statuses, headers, decoded SSE
frames, and post-run thread reads are retained. A second boot retains another raw
recording and repeats all normalized results. Only generated top-level thread IDs
and timestamps are normalized; application output and metadata stay intact.

[Recordings](../../conformance/reference/recordings/2026-09-30-workflows/workflow-observations.json),
[repeat recording](../../conformance/reference/recordings/2026-09-30-workflows/repeat/workflow-observations.json),
[repeatability receipt](../../conformance/reference/recordings/2026-09-30-workflows/workflow-repeatability.json),
and [build provenance](../../conformance/reference/recordings/2026-09-30-workflows/build-provenance.json)
record these observations. There were zero external Fetch attempts. The frozen
install disabled lifecycle scripts. The complete root build completed 25 of 28
tasks; three web/inspector builds could not bind helper ports. The scoped CLI/runtime
dependency graph completed 13 of 13 tasks using outputs built in that same isolated
checkout. This is runtime fixture evidence, not whole-monorepo validation.

The TypeScript capture uses `in_process_fetch`, without a TCP listener. Native
router comparisons select 24 cases: 14 omitted/null/scalar/array/object input
cases, eight void/zero/false/empty-string cases, and two known-failure wait/POST-stream cases. They
compare response status, media type, JSON output and decoded terminal event/data.
Those historical workflow comparisons do not compare raw stream formatting,
generated resource fields, request errors, parks/cancellation, arbitrary failures,
or every response header. Native TCP tests
separately check authentication, policy, projection, wait/stream, viewer
disconnection, and failed result persistence. Their transport evidence cannot be
attributed to the reference recording.

## Executed required-grant reference fixture

A separate [provider-free agent/tool fixture](../../conformance/reference/grant-fixture/README.md)
executes the actual unchanged pinned Fetch handler, production agent adapter,
permission gate, grant minter, SQLite persistence, pending projection and resume
handler. The production model-import seam supplies a deterministic local chat
model; the approved tool writes only a temporary local ledger. No pending result,
grant or handler response is stubbed. Both fresh credential-free captures make
zero provider calls and zero external Fetch attempts.

The [first recording](../../conformance/reference/recordings/2026-10-01-grants/grant-observations.json)
and [repeat recording](../../conformance/reference/recordings/2026-10-01-grants/repeat/grant-observations.json)
each retain 39 actual requests, raw response bytes/headers, decoded SSE frames,
generated identities and ledger order. The fresh isolated checkout restored the
frozen lockfile with lifecycle scripts disabled and built the selected runtime
graph in 13/13 tasks with zero cache hits. Both captures inventory 385 newly built
JavaScript/JSON artifacts before importing the runtime and verify the identical
inventory, unchanged tracked source and exact pinned lockfile afterward. This
new provenance is separate from the earlier workflow build's post-capture inventory.
See [build provenance](../../conformance/reference/recordings/2026-10-01-grants/build-provenance.json)
and [recording scope](../../conformance/reference/recordings/2026-10-01-grants/README.md).

The named limited projection retains statuses, refusal messages/codes, pending
prompt shape, token prefix/length and envelope consistency, event names and local
ledger order. It omits opaque generated identities and output message internals.
[Repeatability](../../conformance/reference/recordings/2026-10-01-grants/grant-repeatability.json)
is **37/39 in order**. The only differences are ordering of two independent pending
prompts in `pair.pending` and `pair.pending-after-refusals`; **39/39 match only when
those two pending arrays are compared as sets**. Every other projected value and
ordering remains unchanged. Raw ordering differences remain recorded. This is
limited reference repeatability, neither whole-response equality nor Rust parity.

The fixture observes repeated pending disclosure, byte-identical prompt/grant/
resume-key restoration across graceful handler close/reopen, Once execution,
Deny/cancelled without tool invocation, complete two-interrupt set validation,
missing/malformed/wrong grants, settled replay and stored route selection.
Missing grant is `400 grant_required`; malformed/wrong grants are both
`403 grant_invalid`; settled replay is `409 stale_interrupt`. Unauthorized pending
is 404 and unauthorized resume is 403. A valid resume supplying `/other#agent`
succeeds with 200 because the source selects recorded `/park#agent` provenance.
These are executed source observations, including differences from the native
contract. They do not cover Always/off/optional modes, TTL expiry, concurrent
consumption, abrupt termination, accepted-resume crash boundaries or reference TCP.

Current native verification and selected comparison scope are recorded in
[http-approval-verification.json](../../conformance/expected/http-approval-verification.json)
and [http-approval-reference-comparison.json](../../conformance/expected/http-approval-reference-comparison.json).
Historical workflow and embedded-service receipts keep their earlier scope.

## Wire decisions

| Contract | Native host choice | Evidence/status |
|---|---|---|
| Run body | Strict object; registered literal RouteKey with `#agent` or `#workflow` kind required | `CHANGE`: reference accepts unknown root keys; native rejects them; only workflow fixtures compared |
| Duplicate JSON keys | Reject at every nesting level | `CHANGE`: reference records last-key behavior |
| Content type | JSON parsing does not require `application/json` | Recorded reference tolerance; native separately tested |
| Input omission | Omitted becomes `{}`; present null/scalar/array/object stays present | `COMPAT` for 14 probe wait/stream cases; custom route-validation parity remains open |
| Void output | Wait `null`; POST stream terminal `done {}` | `COMPAT` for two workflow cases |
| Falsy output | Preserve `0`, `false` and `""`; stream wraps them as `{"output":value}` | `COMPAT` for six workflow cases |
| Known application failure | Wait 500 execution_error without details; stream 200 with `done {output:{error:message}}`; thread idle | `COMPAT` for two fixed fixture terminals; native durable Failed and healthy readiness qualified separately |
| SSE chunk | Direct JSON `data:` value, without a type wrapper | Native scripted transport test; reference chunking remains open |
| SSE settlement | Successful pure workflow sends one committed `done`; no invented tool events | Selected successful terminal comparison; viewer overflow closes without a terminal completeness claim |
| Request errors | `request_error` with fixed public message and `details.code` | `NATIVE`; selected grant status/code comparisons have separate scope; exhaustive precedence remains open |
| Create metadata | Object only; empty/whitespace body becomes `{}`; unknown fields rejected | `CHANGE`: reference captures broader array/root/metadata acceptance |
| Bounds | 65,536 body bytes; nesting depth 64; configurable bounded input/output and capacity | `NATIVE` host limits, not reference parity |
| Thread/state projections | Durable server timestamps/metadata; latest native execution projected separately | `NATIVE`; complete CP-008 state/channel comparison remains open |
| Required approvals | Explicit durable constructor; exact server-authored prompt coverage and policy revision; Once/Deny only | `NATIVE`; ordinary constructor still rejects approval definitions; off/optional/Always remain unsupported |
| Park transport | Wait `200 null`; stream native `{interruptId,resumeKey,value,grant}` interrupts followed by `done {output:null}` | `CHANGE`: source interrupt carries its permission-request envelope; whole envelope/output parity remains open |
| Park thread status | Stable native park is `interrupted` for wait and stream | `CHANGE`: source inspection suggests wait leaves thread idle; the new grant corpus exercises stream parks, so source wait parking remains uncharacterized by execution. Native persists execution status without transport provenance |
| Denial output | Final Denied task yields `{denied:true}` without callback | `NATIVE`; source model/message output has a different shape |
| Grant token | Native `b4ag_` plus 64 lowercase hex characters, 69 bytes total | `CHANGE`: source fixture mints 48-byte tokens with a 43-character base64url suffix; source structural shape permits broader strings |
| Resume route | Exact submitted and recorded route/current definition required; mismatch 409 | `CHANGE`: captured source accepts a differing supplied route with valid grant and resumes recorded route with 200 |
| Unauthorized resume | Generic `404 resource_unavailable`, matching native protected-resource visibility | `CHANGE`: source capture returns 403 for unauthorized resume; unauthorized pending is 404 |
| Grant refusal | Missing 400, invalid 403, expiry/consumed/unavailable 409; fixed reason-free `grant_invalid` | Selected source missing/invalid statuses/codes observed; native expiry/concurrency evidence is separate and source precedence remains partial |
| Delete | Authorized missing/idle/settled/stable park 204; exact fenced atomic history/authority cleanup; active claims/uncertainty 409 | Selected statuses compared; native stronger origin/corruption/stale-target rules |
| Cancel | Exact owned turn; prompt wait409 or cancelled stream done; claim/capacity/store held until quiescence | Selected lifecycle statuses, receipt and cancelled done compared; arbitrary interruption and complete wire parity open |
| Attach | Atomic native snapshot/tail cut, bounded viewers, inactive retry/null done, read-only prior-owner park | Selected statuses/state fields/terminals; raw digest and values remain native projection differences |

The historical `b4_contract::SseEvent::Done { output }` variant and `schemas/sse-done.json` require explicit
`output`. The HTTP worker intentionally uses a separate wire projection to retain
the observed JavaScript void `done {}` variant. `scripts/check_contracts.py`
continues rejecting `{}` against that historical DTO schema. OpenAPI records the
host variant explicitly; the draft schema collection is **not a complete schema
for every implemented HTTP response**. Request errors use the historical AP
envelope with native protocol codes in `error.details.code`. The void wire choice
does not silently change portable persisted formats.

The explicit failure fixture stores a Failed task with a bounded reason, no task
result and no success output. It stops later steps, retains in-flight sibling
results without reducing the failed batch, and releases admission after drainage.
The host publishes the failure terminal only after commitment. Its wait envelope
omits `details`, so OpenAPI documents this observed variant separately from the
historical error DTO. Callback/worker/store errors still leave uncertainty and make
readiness fail. Custom route validation and arbitrary adapter failure qualification
remain open. Historical receipts retain their earlier scope; the earlier failure
qualification is retained in
[settled-failure-verification.json](../../conformance/expected/settled-failure-verification.json).

JSON Schemas describe native draft DTOs. `x-max-utf8-bytes` records byte bounds
that standard JSON Schema code-point length cannot enforce. Internal snapshot
version 1 remains portable native data, not a LangGraph checkpoint/import or
public replay cursor. Grant parsing and resume DTO tests establish no grant
authority. The [durable approval service](durable-approval-design.md) and explicit
[HTTP bridge](http-approval-design.md) supply native required issuance and atomic
Once/Deny acceptance. Native pending is no-store, origin-bound and complete only
after stable park; ordinary AP state does not project approval prompts/tokens.
Fresh authority precedes protected diagnostics, and the service repeats authority,
exact definition/binding checks and resource/execution fences. Capacity and a
per-thread worker claim precede consumption. Every callback in an approval-enabled
host renews policy. Expected grant/state/policy rejection stays healthy; corruption,
failed commit or worker fault makes readiness fail. Policy rejection after stream
admission closes the viewer without a successful done. Post-claim revocation leaves
the accepted decision and conservative InFlight state while invoking nothing.

Native multi-fault order follows authentication/strict structural parsing,
coherent resource policy and renewed actor/policy checks, current owner/state/
route/definition, complete unique set, unsupported Always, definition-order token
conversion, then service grant checks and atomic commitment. Capacity/active-claim
rejection may precede grant validation; no consumption occurs. This is native
precedence, not a complete recorded source precedence contract. Missing grant is
`400 grant_required`; malformed or wrong native token is `403 grant_invalid`
without its diagnostic reason. Expiry, consumption and unavailable grant are
409 with `grant_expired`, `grant_consumed`, `grant_unavailable`; set/route mismatch
are 409; unsupported Always is `400 always_unsupported`. Repeated settled resume
may fail the earlier native state check as `409 resume_required`, rather than
reaching a consumed-grant check.

Prior-owner unfinished work fails closed. Trusted explicit known-ID recovery
allows restored parked grants to be resumed through HTTP with their original
identities. Accepted Ready work can continue through the embedded service after
explicit recovery without another approval. The supervised HTTP embedding API
can launch known-ID accepted work; no public AP launch endpoint is added. Automatic
scanning, Always/off/optional modes, persistent permissions and
full crash/restart qualification remain outside this increment.

## M0 characterization queue

| ID | Status | Remaining observation/decision |
|---|---|---|
| CP-001 | partial | Selected body/content-type/duplicate behavior recorded; broader headers, bounds and transport corpus remain open |
| CP-002 | partial | Probe input cases recorded/selected comparison; complete route-validation timing remains open |
| CP-003 | partial | Selected workflow void/falsy/known-failure terminals compared; reference grant parks/denials observed; arbitrary failures and attach output remain open |
| CP-004 | partial | Selected workflow wait/POST-stream and reference grant park/resume terminals recorded; complete transport/settlement corpus remains open |
| CP-005 | partial | Required-grant missing/invalid/set/replay/route refusals observed; concurrent-claim, expiry and complete multi-fault precedence remain open |
| CP-006 | partial | Executed missing/idle/completed/parked/live deletion and quiescence refusal; partially missing/corrupt resources and transaction failure remain open |
| CP-007 | partial | Executed live/settled/parked attach, retry hints, capacity detach and viewer abort; full merge/overflow/client corpus remains open |
| CP-008 | partial | Executed workflow absence and genuine parked checkpoint/state across graceful reopen; full channels/client dependence remains open |
| CP-009 | partial | Actual required-grant issuance/pending/Once/Deny/cancelled/graceful restore observed; modes, expiry/races/crash boundaries and whole native parity remain open |
| CP-010 | unresolved | Actual AP-only client corpus to qualify |

[OpenAPI](../../contracts/agent-protocol/openapi.yaml) inventories ten implemented
scripted-subset operations, with probes documented
separately. It is a scope document, not a frozen core compatibility contract.
Run `python3 scripts/check_contracts.py` to validate native schema artifacts,
named local test declarations and reference recording integrity. Merely finding
a test declaration is traceability; fresh execution receipts establish its pass.

## Explicit trusted restart continuation

The approval-enabled host now exposes an embedding-only recover_execution method. It accepts a complete trusted target and verified Principal, renews exact current authority and atomically adopts prior-owner work before supervised continuation. Accepted Once continues beyond original TTL; parked identities persist; blocked and budget-exhausted dispositions remain healthy static refusals. Recovering cancellation may emit native done {output:{cancelled:true}}. This is native embedding behavior, not a new AP endpoint or an executed TypeScript restart-launch comparison. Seven public scripted operations and all characterization/release gates retain their scope. See trusted-restart-continuation-design.md and the separate trusted-restart-verification.json receipt.

## Executed lifecycle reference

The separate [lifecycle recordings](../../conformance/reference/recordings/2026-10-01-lifecycle/README.md)
contain 77 requests and 11 assertions on each of two fresh captures. All 77 limited
ordered response/ledger projections repeat; raw bytes, retry-only SSE blocks and
the explicitly incomplete aborted-viewer prefix remain retained. The capture
reuses the exact unchanged 385 artifacts from the documented grant build, with
source/lock/artifacts checked before import and afterward; it performs no new
build. One genuine agent park uses the exported deterministic model importer.
No provider or external Fetch calls occur.

Actual deletion returns 204 for missing, idle, completed and parked threads, and
409 run_in_flight while held work remains active. Wait cancellation returns 409
run_cancelled; primary and attached streams receive done.output.cancelled. These
terminals precede the held effect's exit: deletion and replacement stay refused
until quiescence. A cancellation held in the real asynchronous policy seam stays
bound to the prior turn and cannot cancel its successor.

Live attachment captures state then terminal tail; capacity emits detached with
reason capacity. An attached viewer's abort preserves its state-only prefix and
leaves the producer running. Plain workflow completion has no checkpoint; a real
approval park has checkpoint values, provenance and pending channels that survive
graceful reopen. Owner-policy missing-resource probes and the separate ungated
profile are identified explicitly. Foreign policy probes are not evidence of
natural endpoint absence behavior.

This evidence makes CP-006–008 partial. Native DELETE, cancellation and GET
attachment still remain planned at this receipt boundary. It establishes selected
in-process Fetch behavior, without TCP, whole-byte equality, full suites, abrupt
crash, full channel/client characterization or native lifecycle parity.

## Native fenced deletion

DELETE is implemented after the reference-only boundary above. It follows the
selected 204 missing/idle/completed/parked and 409 active behavior, while preserving
exact immutable origin, fresh current policy, captured generation/resource/
execution fences and conservative prior-owner uncertainty. Native policy denials
use 404 resource_unavailable; the captured reference's owner policy uses 403.
Changed or disappeared captured targets use 409 resource_changed and cannot
delete a successor. Stable prior-owner parked/settled snapshots need no adoption.

Memory and SQLite validate all historical snapshots and related journal/authority
ownership before atomic removal. Inactive unfinished history or corrupt cross-
thread relationships fail closed. A live owned worker still blocks deletion after
durable terminal/park commitment until it releases its claim. Deletion uses no
execution capacity and retains store/claim ownership if its caller disappears.
Failures inside that owned worker mark readiness failed even without a viewer.
At this deletion receipt boundary, seven selected lifecycle status comparisons
were narrower than full wire parity and HTTP cancellation was still planned.
GET attachment remained open at that historical deletion receipt. Abrupt crash
and complete client qualification remain open.

## Native owned cancellation

The host now captures an exact locally owned generation/execution/turn/owner
before resource policy and never mixes an old view with a successor control.
Renewed authority precedes atomic cancellation and unconsumed-grant invalidation;
consumed Once and acceptance records remain unchanged. Missing resources use
native404, stable parks/unowned or prior-owner work409 no_run_in_flight, custom
stores501 unsupported and persistence/worker faults503 unready. Native protected
policy denials remain404, differing from the captured owner policy's403.

After commit, original wait returns409 run_cancelled and a stream receives one
cancelled Done before effect quiescence. Capacity, claim and store stay held until
the actual invocation and completion work finish. Actual late results remain
durable without state reduction; real failure can settle Failed without changing
the earlier wire cancellation. Known never-entered attempts use CancelAttempt,
without a fabricated result or budget refund. Retained revision/clock conflicts
were limited to exact positive local cancellation at this historical receipt.
The shared driver now retries exact real retained outcomes after CAS/clock
conflicts without requiring local cancellation proof; exhaustion still faults.

391 Rust tests include13 new API and10 real TCP cancellation tests. Nine selected
lifecycle statuses, two complete cancel receipts after thread-ID normalization
and one exact cancelled Done payload are compared with frozen Fetch captures.
Finite scripted callback entry and retained-outcome source review qualify these
tests; no arbitrary effect interruption, complete wire/state/client parity,
automatic recovery scan or full process-kill claim is made. See the separate
[verification](../../conformance/expected/owned-run-cancellation-verification.json).


## Native bounded attachment increment

The tenth scripted operation is read-only GET attachment. Live snapshot cut and
viewer registration are atomic in the selected RunControl; queues and digest have
finite UTF-8 byte/frame bounds. Viewer capacity and overflow detach only that
viewer. Renewed authority rechecks stored identity after policy returns and never
retargets a successor. Required parked tokens are readable after reopen through a
separate read-only authority operation; normal pending/resume owner restrictions
remain intact. No owner/token/expiry/audit mutation occurs on attachment.

Inactive attachment uses state, retry2000 and exact `done.output:null`; it never
reconstructs the original producer's result. Existing non-detached live viewers
retain their exact selected terminal across cancellation drain and late host fault.
A new viewer during known local drain reads inactive state and null done. Orphan
unfinished work and unknown registered definitions receive healthy protected
refusals, without implicit recovery. Recovered local publication honestly marks
unavailable history rather than inventing old tokens.

Comparisons deliberately omit native values, anchor identifiers, raw event digest,
approval token/envelope and timestamps from full shape parity. CP-007 stays partial;
LangGraph message merging, arbitrary provider frames, slow network delivery
and the full client corpus remain open. The prior391-test cancellation receipt and
all frozen workflow/grant/lifecycle recordings are retained unchanged.

The [438-test receipt](../../conformance/expected/bounded-run-attachment-verification.json)
adds47 native tests, including13 actual TCP tests. The
[selected comparison](../../conformance/expected/bounded-run-attachment-reference-comparison.json)
qualifies nine attachment statuses, seven fields in four inactive state frames and
five exact Done payloads. Eight current-build examples pass. These checks preserve
148 prior conformance/dependency/schema hashes and leave all56 broad release
scenarios pending.

## Native shared execution driver increment

The shared driver adds real bounded parallel native callbacks and replaces HTTP's
serial scripted scheduling loop. It preserves the existing ten public operations,
selected reference comparisons and full-definition approval/recovery fences.
This increment records native scheduling and ownership evidence; it adds no new
TypeScript capture or broader wire-parity claim. At that receipt HTTP registered only Pure
scripts, while trusted embeddings could implement native `DriverHost` callbacks.

Native callback barriers prove overlap and definition-order joins. Real TCP tests
qualify parallel cancellation, attachment terminal fanout, abandoned viewers and
park/Once continuation. Three actual SQLite SIGKILL boundaries qualify committed
sibling preservation, accepted Once continuation and blocked Unsafe uncertainty.
The separate [receipt](../../conformance/expected/shared-execution-driver-verification.json)
records fresh scoped checks. Existing historical receipts remain unchanged; M0,
CP-010 and all56 release scenarios remain open.

## Immutable native application and capability increment

Authenticated HTTP now accepts an immutable NativeApplication in addition to the
legacy finite Pure script registry. The same ten AP operations reuse the existing
execution, approval, cancellation, attachment and recovery fences. Native callbacks
return real outcomes and cannot emit lifecycle frames or acknowledge entered work
as never-entered cancellation. This is native execution evidence; it adds no new
TypeScript capture, client corpus or broader wire/state parity claim.

Protected GET /b4/capabilities is a separate authored native operational contract.
Its typed version1 descriptor reports installed mechanisms, effective finite limits
and explicit false/null absent subsystems/controls without route inventory or
authority. Nullable physical approval durability distinguishes an unavailable
ordinary Store declaration from declared false storage, independently of service
mode. Existing readyz status-only behavior is unchanged. JSON Schema label lengths
count code points; Rust additionally enforces the annotated256 UTF-8 byte bound.

The native SQLite example qualifies deterministic callbacks, definition-order
joins, Once continuation, rejected input before mutation, capabilities and actual
process reopening. Settled state is unchanged; prior-owner ordinary unfinished
reads still require recovery, while read-only attachment preserves authorized
parked prompts and grants. Separate native TCP and SIGKILL qualification is tracked
in the new receipt. The historical493-test driver receipt, older receipts and
frozen reference fixtures remain unchanged. M0 and all56 release scenarios stay open.

## Supervised admission-drain contract

This is an authored embedding-only native mechanism with no new TypeScript capture
or parity claim. It closes protected admission monotonically, makes readyz unready
without a Store probe, and permits bounded waits for registered framework jobs.
The immediate post-cut protected response is uniformly503 host_draining before
auth/body/policy/Store work; healthz stays200. Existing leased mutations and durably
admitted turns continue, including real entered outcomes after prompt cancellation.

Descriptor v1 DTO/schema/authored examples remain frozen. Current protected
capability responses use explicit v2 with supervised_admission_drain true, the
third serving_admission readiness input and fixed30000/300000ms wait controls.
The old eight absent controls remain null and graceful_shutdown stays false.

Quiescent can be faulted and covers registered framework work only. Listener and
executor closure, forced viewer closure, callback signaling, persisted shutdown
classification and application-owned reference disposal remain the embedding's
responsibility or future work. This does not close broad RFC12.3/T047 or M0.


## SQLite-safe backup scope

The mutable private export and controlled final-cut restore are authored native
storage operations with no TypeScript capture or parity claim. They add no public
HTTP endpoint and leave descriptor v2, the ten AP operations, dependency and schema
bytes unchanged. Controlled restore qualifies exact copied generations, definitions,
approval authority/TTL/audits, committed history and conservative uncertainty through
existing open and known-ID recovery. It is not a general historical rollback or
live-clone contract. The broad T049 scenario, B4N013 packaging/operations, M0 and all
56 release scenarios remain open until their complete acceptance gates are executed.

## Scoped native process executable

b4run serve/config-check --demo and --version/--help are newly authored native
commands, not captured TypeScript CLI compatibility. The compiled native-demo
registry preserves the existing example identities and effects. Strict command,
recognized-environment and private-storage constraints are explicit launcher
policy; no dynamic handler loading or read-only database inspection is implied.

SIGINT/SIGTERM drive the existing synchronous admission cut while the owned process
awaits both registered physical Quiescent and actual Axum connection retirement.
Repeated observation timeouts/signals retain resources; slow peers or a lost
transport witness can keep drain indefinite. Static nonpanicking diagnostics and
retained opaque failed results prevent unrelated cleanup from bypassing that rule.

## Scoped local Linux packaging

The Linux/arm64 nonroot profile is an authored native operational contract, with
no new TypeScript capture or CLI/container parity claim. It changes no HTTP/SSE
behavior, capability descriptor, SQLite schema or dependency versions. Existing
SSE attachment uses Cache-Control no-store, no-transform; JSON uses no-store.
Original parked authority is projected without adoption after clean restart.

Two existing cfg(test) files now observe actual Linux TCP write Pending using a
finite pipeline. All696 Rust entries and the same12 release cases pass on both
platforms. Compiler Location line changes explain new release checksums, with
executable code compared independently; byte identity is not claimed. Historical
receipts remain unchanged. The [new receipt](../../conformance/expected/linux-container-verification.json)
does not close M0, partial CP observations, M5, all56 broad scenarios or the core
profile release.

## Authored stopped inspection

Native inspect and inspect --blocked add a local operator contract under report
version1, outside the twenty AP schemas and ten core operations. There is no new
HTTP route, live CapabilityDescriptor, TypeScript CLI capture or parity claim.
Only DATA_DIR is consulted, while serving/config-check preserve their existing
configuration behavior. Existing packages, dependency identities/checksums,
Cargo.lock, Store/schema/backup/approval and HTTP behavior remain exact; only
rusqlite0.40.2 safe limits/hooks features are added, with full regression required.

The qualified report counts schema3/Snapshot1 stored history and selected
relationships, with complete full-definition policy counts and initialized
uncertainty. It defers thread/journal/grant/acceptance payload semantics and
stable diagnostic identities. Kernel-invalid Failed active uncertainty refuses;
valid failure drains remain Running or RecoveryBlocked. Report-local ordinals
cannot identify a recoverable target. Raw Unix parsing is preserved on both
targets; actual existing nonUTF8 filenames are Linux-qualified because the macOS
filesystem refuses those fixture names. Broader M0/CP/M5/full-operations/release
gates remain open. See [ADR0020](decisions/0020-stopped-readonly-inspection.md).

## Authored request references and embedding diagnostics

The native fallback adds an available server-generated x-b4-request-id while
preserving existing AP status/body/authentication/policy behavior. A separate
version1 diagnostic batch contract is outside the twenty AP schemas, twenty-three
AP examples and ten core operations; it adds no HTTP endpoint or capability
field. Full legacy-envelope correlation remains separate from a reference header.

The embedding-only fixed buffer observes response construction or actual pending
guard destruction, without execution/approval/stream completion authority. Its
public host namespace/counter permit linkage and sequence observations. Memory
records are best effort and are not automatically delivered as launcher logs.
Full identities, lifecycle events and production delivery remain deferred.

One HTTP dependency edge selects existing locked getrandom0.3.4. Removing only
its HTTP Cargo.lock dependency listing reconstructs the prior protected lock;
all package identities, checksums, versions and features remain exact. Historical
inspection/AP schemas, reference captures and receipts remain unchanged. This
adds no TypeScript client execution or parity evidence. M0, CP001–009 partial
observations, CP010, M5 and all56 broad scenarios remain open. See
[ADR0021](decisions/0021-request-correlation-and-bounded-diagnostics.md).

## Source-pinned original AP clients: native macOS corpus

Two fresh unchanged controller/AP-worker client runs cover all 19 selected methods,
nine core operations and 63 invocations each. Both pass the complete native-control,
passive HTTP/SSE, durable-state/authority and positive physical-retirement audits.
Finite Value, true Void and known-failure callbacks characterize the transport.
The original default controller authentication refusal, pending ZodError and forced
actual-ID grantless resume refusals remain part of the results.

See the [runbook](unchanged-ap-client-corpus-runbook.md) and
[scoped receipt](../../conformance/expected/unchanged-ap-client-native-verification.json).
This adds macOS arm64 original-caller observations; CP010 and the full software
factory, TypeScript-host parity, other clients/platforms and broad release matrix
remain unresolved. Existing native state and protocol behavior are preserved.

## Authored cooperative native cancellation

The optional Rust callback entry and separate controlled outcome preserve the
required old callback, old outcome variants, context fields, direct invocation
behavior and all existing wire/storage contracts. Default implementations return
their original genuine result, even after cancellation notification.

Each original run owns a distinct initially clear observer signal. Only successful
durable explicit cancellation accepted by that control publishes it; a successor,
viewer disconnect, shutdown, failed policy or Store operation cannot publish it.
Accepted intent remains true after a later fault while readiness remains faulted.
A false acknowledgement remains uncertainty, and the unchanged kernel validates
exact completion before marking a task cancelled without a manufactured result.

A retained observer carries no execution resources. Notification is not completion,
rollback or physical retirement. Trusted callbacks must stop their owned work before
acknowledging; the framework does not discover detached application work. These
native tests add no new TypeScript host parity or original-client execution evidence
and leave broad release, CP010, M0, M5 and core profile gates open.

Fresh macOS and Linux Rust checks each pass 888 entries across 88
Cargo targets with three historical helper ignores, including the frozen AP
example's 30 Rust entries. Python checks pass 93 cases. The 42 distinct new
Rust declarations produce 44 entries (15 portable, 20 API, nine TCP); shared gate
tests compile in both HTTP targets. Existing original-client and Node evidence
remains its preceding frozen macOS scope. See
[the native scoped receipt](../../conformance/expected/native-cooperative-cancellation-verification.json).

## Authored bounded unfinished-execution discovery

The optional Store page method and its existing Arc/reference delegates add an
embedding-only read contract. They change no AP route, capability descriptor,
wire schema, stored schema, package dependency or original TypeScript client.
Existing known-ID storage access keeps its previous limits.

Built-in pages validate selected index, snapshot and thread relationships before
terminal filtering. They inspect at most 32 full rows plus identity-only
lookahead under one coherent read. Raw SQLite JSON is accounted before typed
decoding; Memory uses a direct nonbuffered counting writer before kernel work.
Source/owner-bound cursors and returned records carry no execution resource.
Cross-page consistency, whole-store integrity, bounded CPU/I/O and startup
completion are not promised by scan exhaustion.

Separately authorized recovery tests preserve real parked identifiers and
consumed Once continuations, reject current-owner/stale/foreign/incompatible
targets and retain unsafe uncertainty without callback replay. Authenticated TCP
is used for existing views and pending grants; recovery itself uses the trusted
embedding method, with authority supplied independently of discovery.

Fresh exact-source macOS and Linux Rust verification each passes 966 entries
across 91 targets, with 3 historical ignores and 78 new
declarations. Python verification passes 96 cases. The separate 19-row
discovery group preserves the historical 338 foundation, eight unchanged-client
and 15 native cancellation rows and all broader open statuses. This adds no
original-client execution, full startup algorithm, crash-matrix or core release
claim. See the [scoped receipt](../../conformance/expected/unfinished-discovery-verification.json).

## Authored bounded startup recovery preparation

Typed recovery admission and the preparing host are opt-in embedding APIs. They
reuse the existing approval bridge, worker and response adapter, preserving old
static conflicts and SSE behavior. The preparing constructor requires actual
durable approval storage and RequiredDurable configuration. Existing host
construction, public ShutdownPhase variants, native configuration fields, wire
schemas, stored schemas, package dependencies and original clients remain
unchanged.

One finite coordinator uses a private startup capability under the same mutex as
shutdown and ordinary admission. It renews current trusted authority, policy, the
complete registered Definition and original coherent fences before adoption.
Custom page shape violations are refused before counter updates. External writer
isolation and stable execution-ID membership are trusted embedding requirements;
the gate cannot confine arbitrary Store aliases.

Expected policy, Definition and stale-target refusals remain incomplete and closed
without creating durable recovery blocks. Earlier accepted work can finish after
a later refusal or shutdown cut. Some adoption evidence means an actual returned
receipt; None is an unconfirmed outcome and cannot establish rollback or license
a retry. Owned panic and capture cleanup completes before serving activation or
retirement. Private asynchronous cancellation probes expose no public coordinator
abort or fault authority.

Preparation completion, callback settlement, observation timeout and physical
writer succession have separate evidence. This increment adds no automatic CLI
startup, original-client corpus or Node unit-suite execution, full RFC11.4 restart algorithm,
provider reconciliation, historical-Definition coexistence or broad core-release
closure. See the [runbook](../../docs/runbooks/startup-recovery-preparation.md) and
[scoped receipt](../../conformance/expected/startup-recovery-preparation-verification.json).

## Authored explicit retained native Definitions

The additive immutable assembly installs current and retained executable versions
without changing wire or storage schemas, native configuration fields, package
dependencies or original clients. Current-only route collections select new work.
Recorded continuations require route/execution revision plus full Definition
equality and use their original callback bindings, validator and output kinds.
Installed assembly revision remains distinct from recorded execution revision.

The aggregate assembly bound is 64 registrations with nonempty current routes
and unique route/revision pairs. Historical-only routes may drain, but cannot
accept new work. Any approval-bearing historical task, including an uninitialized
future batch, refuses assembly. Current approval prompt coverage remains current
only, with whole current Definition checks before prompt access.

Recovery and attachment preserve original resource views, authority renewal,
fences, publisher identity and counted ownership. Matching native resume policy
uses recorded provenance; scripted requested provenance retains its preceding
behavior. New selection metadata leaving a counted policy job is resource-free.
Input validation is unchanged for new work and is not added to recovery or resume.

Missing or altered support retains healthy no-mutation refusals and startup
Incomplete. Durable incompatible/revoked-policy confinement and broader RFC11.4,
CP010, M0, M5 and core release gates remain open. Original-client and Node unit
suite evidence remains historical. See the
[runbook](../../docs/runbooks/historical-native-definitions.md).

Actual scoped verification: macOS and Linux each passed 1084 Rust test entries across 98 targets, with zero failures and three existing ignores. The 48 new named Rust tests ran once per platform; two nested historical child results are excluded. macOS passed 102 Python tests, strict lint, formatting and contract checks. See the [scoped verification receipt](../../conformance/expected/historical-native-definitions-verification.json). Original TypeScript client and Node unit-suite evidence remains historical; the existing release gates remain open.

## Authored explicit prior-owner confinement

The additive native embedding method records a durable block for a known original
execution from an earlier actual SQLite owner. Separate trusted mutation authority
receives the complete ordinary original view and one fixed reason. The immutable
whole Definition, actual original principal resolution and current Recover policy
determine that reason; dispatch refusal alone does not authorize confinement.
The host recaptures once, renews the same reason and permission, and submits the
original to an atomic backend audit. It does not retarget or retry.

The bounded proposal preserves original tasks, inputs, outputs, attempts and
approval history while changing only the fixed blocked status/failure, current
owner, fresh turn/history, execution revision/time, thread resource revision and
one journal append. Actual complete current Once/Deny evidence is audited;
legacy missing evidence remains unsupported and present contradictions remain
corrupt. A fully audited prior block returns unchanged before trusted callbacks.
Memory's ordinary owner remains one, so its qualified behavior is bounded capture
and refusal; durable prior-owner positives use real SQLite succession.

Wire and storage schemas, configuration fields, package dependencies, existing
KernelCommand variants, original clients and old host lifecycle bodies remain
unchanged. New target/receipt/error Debug and Display output omit resource data.
SQL raw input and Memory borrowed canonical accounting, the pure safe-clone
budget and output caps remain separate. Conservative aggregate crossings do not
qualify exact-edge aggregate inclusivity or an isolated acceptance-only cap.

Actual returned receipts are independently checked before they can survive later
faults. Missing receipt is an ambiguous outcome after commit-then-error/panic,
not rollback proof. Counted cleanup retains the original owner group, capacity,
claim and won mutation lease through cloned handles and opaque payload disposal.
Physical Store/application retirement and real next-writer ownership are separate
from readiness, quiescence counters and caller completion.

The new requirements group is scoped independently; all previous groups and broad
release statuses remain exact. Automatic RFC11.4 readiness, effect reconciliation,
legacy-profile repair, permanent revocation/unblock, unfinished-work deletion
changes, broad crash qualification and core release closure remain open.
Original TypeScript and Node unit-suite evidence remains historical. See the
[runbook](../../docs/runbooks/prior-owner-confinement.md) and
[scoped receipt](../../conformance/expected/prior-owner-confinement-verification.json).

Actual macOS arm64 and isolated offline Linux aarch64 checks passed 1,213
top-level Rust workspace entries across 102 Cargo targets per platform, plus three
separately selected slow SQLite cases (1,216 qualified case executions). All 132
new Rust declarations passed as 129 normal and three separate slow cases; 86
implementation-private and 46 independent public declarations retain distinct
scopes. Six default ignored markers include three historical unrun helpers and
three slow cases subsequently passed separately; two nested child summaries are
excluded. macOS Python 105 and contracts passed. Qualification bound 693 inputs
and 273 crate files, preserving 653 prior files and nine Cargo inputs. Original
Node 221 and TypeScript client evidence was not rerun. Final support review,
post-documentation checks and external freeze remain separate.

## Authored two-lane process startup restart qualification

The two new parent cases exercise existing native and SQLite interfaces across
actual SIGKILL/reap and fresh-child Preparing startup. A stable required Once
park preserves the original successful sibling, grant and pending identity;
only the subsequently approved release runs. An Unsafe synced ledger effect
with no committed result is retained as OutcomeUnknown/RecoveryBlocked without
replay, while unrelated finite Pure work can serve after preparation.

Original HTTP schemas and status behavior are preserved. Resume submits original
recorded interrupt identity and grant, while the new pending projection remains
an independent oracle. The readiness ordinary probe uses ReadThread, because
RecoveryBlocked ReadState retains its existing unsupported status. A killed
native POST stream requires no initial frame and is not fabricated into a
complete terminal response.

The parent inspector's normal open consumes an owner epoch. Source, inspector,
restarted-child and final writer generations are actual observations; only the
final writer is required to be child owner plus one after normal healthy
retirement. Full sensitive ordinary/approval/journal captures remain private.
Role evidence and external ledger reservations jointly fit 32 MiB; no serialized
capture, authority value, database row, private capture digest, child identifier
or socket path belongs in the public verification receipt.

Production bodies, launcher defaults, schemas, manifests, lockfile, all earlier
requirement records and existing test bodies remain exact. The new trace group
has only RESTART-001/002; the two parents, one ignored helper, four roles and
three Python metadata tests have distinct credit. Metadata validity does not
establish runtime execution.

Recorded macOS arm64 verification observed 1,215 normal
outer workspace passes across 103 targets and three separate slow passes, 1,218
distinct qualified executions, zero failures and 7 default ignored markers,
excluding two nested CLI results. Formatting, strict lint, 105 Python tests and
contracts passed with 704 inputs and 280 crate files. Linux aarch64 passed the same recorded Rust totals, formatting and strict lint;
both independent platform reviews are complete. New metadata RED/GREEN is
complete for three declarations; its original 705-input stage used two pending
requirements. RESTART-001/002 are implemented and passed within the bounded
native macOS/Linux scope. Post-documentation checks and final source reviews
are recorded separately. See the [runbook](../../docs/runbooks/startup-process-restart.md)
and [scoped verification receipt](../../conformance/expected/startup-process-restart-verification.json).

Full RFC11.4, accepted-before-dispatch Preparing restart, automatic policy
confinement, persistent Always, recovered token history, external reconciliation,
actual confinement deletion/reincarnation race and general CLI startup exposure
remain open, along with all 56 broad release scenarios, M0 and CP010. Original
TypeScript and Node unit-suite evidence remains historical. This is authored
bounded native evidence, not a change to the incumbent protocol or arbitrary
external-effect exactly-once behavior.


## Authored accepted Once before dispatch through Preparing restart

This subsequent one-parent qualification covers the finite accepted-before-dispatch window excluded by the preceding two-lane receipt. Actual SQLite Accept commits with the original grant consumed and full acceptance retained, then the owned seed is killed before worker success delivery or release dispatch. Automatic Preparing recovery after real original expiry preserves the accepted intent, original input/Definition and successful sibling; only the approved release runs. No replacement grant, second resume, recovery input validation or whole-startup algorithm is introduced. The original resume is an abrupt EOF/reset cut without Done, not a normally framed completed SSE response. Preparation Complete, callback return, physical retirement and real writer succession are distinct observations.

Six new qualifier Rust files and one target preserve old protected sources/tests, three reused helpers, production APIs/defaults, dependencies and wire/storage schemas. macOS and Linux each completed 1,216 outer normal passes over 104 targets plus three separate slow passes, 1,219 distinct executions, zero failures and eight default ignores; the new parent counts once and its ignored helper/repeats/nested summaries add no credit. The first inherited macOS CLI failure remains retained with unresolved cause; unchanged isolated and full reruns passed independently. macOS 108 Python passes and the unchanged 458-row metadata inventory precede the new group. Independent final platform reviews are complete. Three new metadata methods passed after meaningful missing-group and ignored-guard RED; one separately scoped requirement record preserves every prior row and both old process-restart records. Post-documentation checks and final external source acceptance are recorded separately.

The preceding two-lane open-window language remains exact historical scope. Current bounded-window evidence does not close persistent Always, automatic incompatible/revoked-policy confinement, the actual confinement deletion/reincarnation race, full RFC11.4, all 56 broad scenarios, M0 or CP010. Serialized fixture/artifact caps do not bound whole-startup heap or resources; TypeScript/client and Node unit-suite evidence remains historical. See the [runbook](../../docs/runbooks/startup-accepted-once.md) and [scoped receipt](../../conformance/expected/startup-accepted-once-verification.json).


## Authored bounded opt-in native startup confinement

A separate Preparing entry point binds the entire prior-owner original view and exact trusted principal through initial bounded capture, renewed observation and fenced atomic audit. It supports only missing exact Definition and current Recover-policy denial, with two independent confinement-permission observations. Successful confinement preserves all original task, approval and journal history. A fully audited existing prior block is unchanged; NotApplicable alone can use the same owned original for typed recovery. Verified receipts and audited prior blocks have separate counters, and commit ambiguity supplies no fabricated receipt evidence. Readiness, callback settlement and physical retirement remain distinct. Existing default preparation, known-ID observer, wire/storage schemas, dependencies and CLI defaults retain their behavior.

Five selected HTTP production files and thirteen new Rust files introduce two Cargo targets with exactly six private, eight API and two TCP declarations. Actual macOS and isolated Linux qualification each closed 106 outer targets with 1,232 normal passes and three separate slow passes, 1,235 distinct qualified Rust executions, zero failures and eight default ignored markers. Two nested CLI summaries, helper branches and repeats add no credit. Formatting and strict workspace lint passed. Three metadata methods passed after meaningful missing-group and ignored-guard RED; later unreached initial assertions receive no retrospective RED credit. The sole STARTUP-CONFINE-001 record uses `explicit_bounded_opt_in_native_startup_confinement_only` and its own scoped pass token and receipt. Every prior requirement/status and accepted-Once/two-lane receipt remains unchanged. Post-support Python/contracts and final source acceptance are recorded separately.

Selected serialized-data and work-structure bounds exclude ordinary bootstrap and later typed recovery; no whole-startup heap, CPU, I/O, storage or hard deadline bound is claimed. Automatic CLI exposure, actor/secret reconstruction, persistent Always, actual confinement deletion/reincarnation races, full RFC11.4, all 56 broad scenarios, M0 and CP010 remain open. Original TypeScript/client and Node unit-suite evidence remains historical. See the [runbook](../../docs/runbooks/startup-confinement.md) and [scoped receipt](../../conformance/expected/startup-confinement-verification.json).

## Authored explicit native demo CLI startup recovery

`b4run serve --demo --recover` selects ordinary bounded Preparing startup for only the fixed native-demo/local/owner identity. Both option orders and configuration-only checking are supported; default direct serve retains its policy. The actual listener is owned before one preparation begins; health/listening, readiness after Complete, callback settlement and physical retirement remain distinct. Signal-first shutdown retains work until normal return, while opaque lost-server payload/executor ownership remains retained. The flag supplies no confinement authority or arbitrary actor/secret reconstruction.

Twelve genuine new Rust declarations cover four parser, four private startup, one compiled-CLI parked-work, one historical-definition and two controls. Clean setup precedes meaningful parser/native behavior RED; the missing-trait compiler failure and later 400 Resume-route fixture failure remain separate history, with corrected clean build/complete-parent GREEN. Actual CLI qualification passed 87 outer entries; each macOS/Linux normal workspace run passed 1,244 entries across 109 outer targets, 0 failures / 9 ignored, excluding two nested CLI summaries. The three previous slow SQLite controls retain independent exact-source/Cargo/platform applicability only; no fresh ignored rerun or new credit is claimed. Formatting and strict workspace lint passed. Metadata 3 and full Python 117 methods passed before final support, with Node retained-artifact integrity checks distinct from historical client/unit evidence.

The new CLI-RECOVERY-001 record uses `explicit_fixed_native_demo_cli_startup_recovery_only` and its own scoped pass token/receipt. All 460 preceding records and all historical support spans remain exact. Final post-support checks and source acceptance are recorded externally. Arbitrary application loading, persistent Always, automatic confinement, actual deletion/reincarnation races, full RFC11.4, broad 56 / M0 / CP010 and whole-startup heap/CPU/I/O/storage/deadline guarantees remain open. See the [runbook](../../docs/runbooks/cli-startup-recovery.md) and [scoped receipt](../../conformance/expected/cli-startup-recovery-verification.json).
