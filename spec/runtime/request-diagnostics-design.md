# Request correlation and bounded embedding diagnostics

Status: implemented native request-boundary subset. Independent SPEC/QUALITY and fresh macOS/Linux arm64 source/release checks pass; the scoped candidate evidence review passes. The independent final freeze decision is recorded separately.

## Selection and authorization

The continuous-build request authorizes this local reversible Rust increment.
The attached RFC is requirements input, not a separate instruction to publish,
deploy, contact anyone or obtain credentials. RFC13.1 requires diagnostic
correlation without changing legacy error bodies; RFC13.2 and M5 remain partial.
No Git mutation, external service, provider or logging delivery is selected.

Three approaches were considered. A bounded request record ring is selected:
it supplies a directly testable response reference and useful local embedding
export with a small disclosure surface. An exact stopped identity inventory
needs a separate policy because valid opaque IDs can contain secrets. Actual
AP-only client characterization is valuable and has concrete pinned controller
and worker candidates, but needs independent build/capture work and explicit
pending/grant differences. CP010 remains unresolved. The frozen inspection
version1, its implementation and historical artifacts are preserved.

## Public behavior and ownership

Every response produced by the existing b4-http fallback gets one
`x-b4-request-id` header when a correlation is available. It is server generated;
inbound correlation headers, bearer values, cookies and request IDs are never
read or copied by the observer. Other response headers, bodies, status, AP
authentication/policy order and storage/kernel/worker behavior remain unchanged.
The observer does not parse a body, URI, thread ID, route, principal or error.
Only a four-value request-method label is read before entering the unchanged
endpoint: GET -> get, POST -> post, DELETE -> delete, everything else -> other.

The observer starts when the fallback future is first polled. An unpolled future
has no event. HTTP parsing failures before fallback, proxy/TLS errors and responses
introduced by outer embedding middleware are outside this boundary. An actual
fallback response records `response_ready`; this measures response construction,
not complete body delivery, stream EOF, task completion or physical retirement.
If the polled fallback future drops before a response and its guard destructor runs, a guard attempts to record
`request_future_dropped` with null status. This event does not claim a disconnect,
execution cancellation, rollback, worker stop, stream detach or accepted action.
Unexpected unwinding follows the same best-effort guard-drop path.
An abort/timeout is not proof that the guard already ran: existing trusted
OwnedShared destruction can retain disposal and block future cleanup. Destructor
order, physical ownership and best-effort loss still apply.

The outer wrapper moves the original State/Arc<Shared<S>> into the unchanged
inner endpoint. It retains only the resource-free observation guard across await
and finish, never an extra Shared clone. A private held-finish witness with a
weak Store reference at the direct transport-wrapper boundary must prove no
uncounted outer Store capture remains. This direct witness excludes the router:
pinned axum Handler::call retains its own existing state capture across handler
await. Actual router/server Store release needs the separate positive-retirement
and writer-reacquisition qualification; do not attribute axum state to diagnostics.

`HttpHost::diagnostics() -> RequestDiagnostics` returns a cloneable handle owning
only an Arc of the correlation seed/counter, bounded ring and loss flags. It has
no Store/authenticator/policy/Principal/config/route/router/application/worker,
Tokio executor, listener, owner lock, shutdown lease or callback. The handle and
exported batches can outlive the host without keeping application resources alive.
There is no new HTTP endpoint, public HostConfig field, logger thread, sink
callback, stdout/stderr/file I/O, background task or automatic CLI export.

## Correlations and failure

Each host construction obtains a new independent 16-byte OS entropy seed using
the already locked getrandom0.3.4 package. Failure returns static
`ConfigError("diagnostics_random_unavailable")` before Store readiness access.
All existing configuration validation still precedes this seed creation.
One deliberate b4-http dependency edge to that exact existing package is added;
no package/version/checksum/features change. The lockfile changes only the
b4-http dependency listing. Avoid infallible UUID entropy inside requests.

The captured210-entry protected inventory includes the current root Cargo.lock.
Its selected one-entry change is therefore an explicit preservation exception:
209 other protected paths remain byte-exact, and removing only the b4-http
getrandom listing reconstructs the original protected lockfile hash. Historical
receipts/reference captures, including their original hashes, are not rewritten.
Every package identity/version/checksum/feature and all other dependency edges
remain exact. Do not describe all210 paths as byte-unchanged after this edit.

The ID syntax is exactly `b4r_<32 lowerhex seed>_<16 lowerhex counter>` (53 ASCII
bytes), counter1 through u64MAX. Checked atomic increment never wraps or reuses
an ID within a host. Independent host seeds provide probabilistic separation,
not an authenticity or cryptographic uniqueness guarantee. The seed is only a
diagnostic namespace and never a Store incarnation, grant or bearer secret.
Correlation is not authority, idempotency, execution identity, an admission fence
or a recovery target.
The public namespace and sequence allow same-host linkage and reveal sequence
metadata; they are intentionally not anonymous or secret references.
Counter exhaustion sets persistent loss/exhaustion flags; requests continue
normally without a correlation header or record. Private
deterministic tests cover exhaustion without exposing public seed/counter knobs.
Any unexpected header construction or fallible HeaderMap::try_insert failure
likewise marks loss and omits this observation as well as the unavailable header,
without changing status/body or fault readiness. Finish consumes its pending
observation before this attempt, so Drop never fabricates a future-drop event.
Generated ASCII and the existing bounded response headers normally exclude those
branches. Actual library capacity faults and private entropy injections must be
distinguished from an OS entropy outage; no infallible header insert is used.

## Exact exported version1 JSON

`RequestDiagnostics::drain() -> Result<DiagnosticBatch, DiagnosticsReadError>`
destructively takes all currently retained events, at most256. It takes no caller
limit, callback or I/O object. `DiagnosticBatch: AsRef<[u8]>` owns only a compact
UTF8 JSON byte vector, without newline. The public constants are REQUEST_ID_HEADER, REQUEST_DIAGNOSTIC_CAPACITY=256 and MAX_DIAGNOSTIC_BATCH_BYTES=65536. Static reader errors are a Copy enum implementing code()/Display/Error; they carry no source or payload text. The public encoded cap is65536 bytes.
The schema is `spec/runtime/request-diagnostic-batch.schema.json`; it is an
authored embedding contract outside the20 AP schemas/23 AP examples.

All top-level keys are required, additional properties forbidden:

```
diagnostic_batch_version: 1
scope: "request_boundary"
redacted: true
capacity: 256
record_loss: boolean
total_evicted_records: integer 0..u64MAX
correlation_exhausted: boolean
records: array, 0..256 elements
```

Each record has exactly five required keys, no additional properties:

```
request_id: matching the53-byte ID syntax
event: "response_ready" | "request_future_dropped"
method: "get" | "post" | "delete" | "other"
status_code: integer100..999 for response_ready; null for request_future_dropped
elapsed_ms: integer0..u64MAX
```

Elapsed time is monotonic since observer start, truncating to milliseconds and
saturating at u64MAX. It excludes transport before first poll and all body work
after the response is constructed. Record order is insertion/completion order,
not request-start/counter order, wall-clock order or execution order. Concurrent
drains partition retained events; cloned handles share one destructive ring.
The schema has conditional status/event constraints. No request URI, address,
tenant/principal/route/thread/execution/task/effect/owner identity, wall clock,
payload, error text, SQL, panic, token, grant, prompt/output or source digest is
retained. Even valid application-selected opaque identities are excluded.

## Bounded, best-effort insertion and export

The preallocated fixed256-entry ring stores Copy records with inline53-byte IDs,
not arbitrary strings. It evicts its oldest record before appending event257.
Successful eviction increments a saturating cumulative `total_evicted_records`
and sets persistent `record_loss=true`. Producer and drain use `Mutex::try_lock`,
never wait for the ring lock. Producer contention/poison drops that observation
and sets an atomic persistent loss flag. Such misses are not included in the
eviction count: `record_loss` is the conservative signal, not a complete count.
No ring-lock retry loop, I/O, await or caller code runs during insertion or while locked. Checked atomic sequence allocation may retry a compare-and-swap under contention; no hard CPU latency is promised.
Loss flags and counter exhaustion are not authoritative host faults.

Drain contention returns static `diagnostics_busy`; poison returns static
`diagnostics_unavailable`. Both leave the ring untouched. A successful drain
takes the bounded queue and replaces it with an empty bounded queue under the
lock, then releases the lock before encoding. Atomic loss flags are sampled
best-effort; no globally atomic history cut or durable log completeness is
claimed. A later concurrent miss can first appear in the next drain. Persistent
loss/exhaustion metadata and cumulative evictions are never reset by draining.
Taking/copying the at-most256 Copy records uses preallocated fixed arrays and
clears occupied entries under the lock; it never reallocates a replacement queue
there. Encoding can allocate only after releasing the lock.
Encoding uses only private fixed-field DTOs and a capped writer. Static
`diagnostics_encode_failed` or `diagnostics_limit_exceeded` after taking events
marks loss; the removed events are not restored. No raw serialization error is
returned. This is destructive best-effort diagnostic export, not durable delivery.

The request guard resolves its pending observation exactly once: finish marks
it consumed before inserting a response-ready event; Drop only inserts the
still-pending future-drop event. Header insertion does not depend on ring capacity
or lock availability. Never serialize/inspect Response body to select a code.
The record object is constructed before ring try_lock and contains fixed-size
method/event/status/duration plus one bounded server ID. This is a logical
record/output bound, not a hard total RSS/allocator/CPU/IO latency guarantee.

## Verification and remaining scope

Capture a meaningful compiling baseline RED: an actual response lacks the new
header. Module stubs permit behavioral RED for positive event/drain expectations;
compile/setup failures are excluded. Exercise header-to-event equality across
success, probes, auth/scope/policy/parser/input/unsupported/missing/draining and
infrastructure errors without modifying legacy envelopes. Exercise SSE response
construction while an effect remains held, aborting a polled wait future while
owned work still retains exclusion, independent request IDs/hosts and 256/257
buffer boundaries. Private controls prove actual contention, poison, saturating
eviction, counter exhaustion and exact encoding width/cap N/N-1.

Use actual MemoryStore and SQLite hosts, resource weak references and actual
writer reacquisition after positive host/server retirement with diagnostics/batch
still alive. Add real loopback response/header tests and a shipped native demo
process test, including response-ready headers on SSE and safe refusal bodies.
Keep raw secrets/authority and database-derived hashes out of public receipts.
Preserve all old implementation tests and historical captures/receipts exactly;
only HTTP host/lib/transport/manifest and its dependency-list lock entry change.

Root SPEC then independent full QUALITY review each implementation/qualifier,
fresh complete format/lint/workspace/release/retained Python/Node/contracts and
updated nonroot Linuxarm64 checks precede new scoped evidence. Count new unique
entries separately from repeated focused/release/platform checks. Keep all56
broad scenarios pending, M0open, CP001–009partial/CP010unresolved/M5partial and
core_profile_frozen=false. No stopped-inspection version or endpoint expansion.

This increment supplies usable structured request records and correlation.
Automatic CLI export, durable/production log delivery, execution/turn/task/effect
identities, lifecycle/attempt/cancellation/recovery/approval receipt diagnostics,
SSE detach/overflow/EOF events, complete RFC13.2, client parity, deployment and
full operations qualification remain open. Durable approval acceptance continues
to use the existing Store and is never inferred from these records.
