# Request references and embedding diagnostics

Status: implemented native request-boundary subset; independent SPEC/QUALITY and
fresh complete macOS/Linux arm64 source/release checks pass. The scoped candidate evidence review passes; the independent final freeze
decision is recorded separately. This is the request boundary subset of
RFC13.1/13.2 and M5. The [accepted design](request-diagnostics-design.md) defines
the exact scope; [ADR0021](decisions/0021-request-correlation-and-bounded-diagnostics.md)
records the decision.

## Find a request reference

Responses constructed by the native HTTP fallback include `x-b4-request-id`
when a reference is available. This includes probes and refusal responses.
Existing response bodies, status, authentication and resource policy behavior
are preserved. The observer ignores inbound request references, cookies and
authorization values; it reads only a fixed method label.

A reference is exactly 53 ASCII bytes:
`b4r_<32 lowercase hexadecimal namespace characters>_<16 lowercase hexadecimal counter>`.
The namespace encodes a fresh 16-byte OS random seed for each constructed host;
the counter runs from1 through u64MAX without wrapping. Cloned hosts share their
namespace and counter. Separate constructors have probabilistic separation.
References allow same-host linkage and expose sequence metadata. They are public
diagnostic references, never credentials, authority, idempotency keys, execution
IDs, recovery targets or an authenticity guarantee.

Construction can fail with static `diagnostics_random_unavailable` if OS entropy
is unavailable. Existing central configuration validation precedes seed creation,
and seed creation precedes the central build's Store readiness call. Earlier
constructor durability queries retain their existing ordering. No request performs
a new OS entropy call. Counter exhaustion leaves requests running normally with
no new header/event and sets persistent loss/exhaustion flags. Unexpected header
construction or fallible insertion failure likewise omits the header/event and
marks loss, preserving the original body/status.

## Drain records in an embedding

Call `HttpHost::diagnostics()` to obtain a cloneable `RequestDiagnostics`. It owns
only its seed, counter, fixed buffer and loss flags. It captures no Store,
application, authentication, policy, executor, listener, owner descriptor or
shutdown lease. An encoded `DiagnosticBatch` owns only its bytes. Both can remain
alive after the embedding positively retires its other resources.

For an already constructed host:

```rust
let diagnostics = host.diagnostics();
let batch = diagnostics.drain()?;
let json_bytes: &[u8] = batch.as_ref();
```

`drain()` takes all currently retained records, with no caller-selected limit or
sink. Successful drains clear the queue. All handle clones share that destructive
queue: concurrent successful drains partition the retained records. Loss flags
and cumulative evictions persist. Encoding follows the take; encoding refusal
does not restore the removed records.

The embedding chooses how and when to deliver the bytes. The library creates no
logger, file, stdout/stderr output, callback sink or background task. The shipped
launcher supplies response references but does not automatically export its
buffer as production logs. There is no new diagnostics HTTP endpoint or public
host configuration field.

## Interpret the two events

Observation starts when the fallback future is first polled. Unpolled futures
produce nothing. Parsing failures before this boundary, outer middleware,
proxies and TLS are outside it.

| Event | Meaning | Status |
| --- | --- | --- |
| `response_ready` | The fallback constructed the response, before body/SSE delivery | Original integer HTTP status100–999 |
| `request_future_dropped` | The pending guard destructor actually ran before a response and attempted insertion | `null` |

Neither event establishes effect completion, durable approval acceptance,
cancellation, rollback, stream EOF/detachment, successful network delivery or
physical worker/server retirement. An abort alone is not proof that a guard has
already run: existing trusted owned-resource disposal can delay future cleanup.
Dropping or completing a body after response construction creates no new event.

`elapsed_ms` uses monotonic time from first poll to observation, truncating to
milliseconds and saturating at u64MAX. It excludes earlier transport work and
later response body delivery. Record order follows insertion/completion, which
may differ from counter allocation order. No wall clock or task timing is given.
Method labels are `get`, `post`, `delete`, and `other` for every other method.

## Bounds and loss

The preallocated buffer holds256 fixed records. Insertion257 evicts the oldest
retained record and increases a saturating cumulative eviction counter. Producers
and drains attempt the buffer lock once and never wait for it. Contention or
poison can drop an observation while its response still carries a reference.
These misses mark persistent `record_loss` but do not increment the eviction
counter. A header therefore need not have a retained record.

Atomic loss/exhaustion flags and the taken queue are sampled separately. A batch
is a best-effort observation, not a globally atomic history cut. A concurrent
miss can first appear in the next drain. There is no complete lost-record count,
durable log history or delivery acknowledgement. Diagnostic failures do not fault
host readiness or alter runtime authority.

Only fixed fields are retained. No URI/address, route, tenant/principal,
thread/execution/turn/task/effect/owner identity, policy revision, wall clock,
input/output, prompt/grant/token, cookie, raw error, SQL, panic or source digest
appears. Even valid application-selected opaque IDs are excluded.

The public constants are `REQUEST_ID_HEADER`, `REQUEST_DIAGNOSTIC_CAPACITY=256`
and `MAX_DIAGNOSTIC_BATCH_BYTES=65536`. Encoding is compact UTF8 JSON with no
newline, capped at65536 bytes, after releasing the buffer lock. The widest fixed
private serializer DTO is44995 bytes. Its maximum numbers and repeated IDs are a
width proof, not a genuine host-history fixture. Fixed record/output bounds do
not promise total RSS, allocator latency or hard CPU/I/O limits; atomic sequence
allocation may retry and constructor OS entropy can block.

| Drain error code | Queue behavior |
| --- | --- |
| `diagnostics_busy` | Contention; leaves retained records untouched |
| `diagnostics_unavailable` | Poison; leaves retained records untouched |
| `diagnostics_encode_failed` | Records already taken; marks loss, no restoration |
| `diagnostics_limit_exceeded` | Records already taken; marks loss, no restoration |

`DiagnosticsReadError` is a Copy enum whose code, display and debug text are only
the fixed strings above. No raw serialization error is returned.

## Version1 contract and checks

The [separate schema](request-diagnostic-batch.schema.json) requires exactly eight
top-level keys: `diagnostic_batch_version:1`, `scope:"request_boundary"`,
`redacted:true`, `capacity:256`, `record_loss`, `total_evicted_records`,
`correlation_exhausted`, and `records`. Each record has exactly `request_id`,
`event`, `method`, `status_code`, and `elapsed_ms`. All counters use u64 bounds;
conditional status/event rules and additional-property refusal are required.

Three [authored examples](diagnostic-examples/empty.json) cover empty, ready and
dropped forms. They are synthetic contract values outside the20 AP schemas and23
AP examples. `scripts/check_diagnostic_batches.py` checks strict UTF8/JSON numbers,
duplicate keys, schema/bounds, compact lexical framing, unique references and one
namespace within a batch. Object-key order may differ. These necessary output
conditions do not establish authentic events, global uniqueness or atomic metadata.
The full `scripts/check_contracts.py` runs this separate check and refuses invalid
examples with only static diagnostic output.

The direct held-finish WeakStore test establishes that the new wrapper has no
extra Shared capture. Its finite gate must exit by explicit release, never by
timeout. Axum's existing Handler state captures are qualified separately through
actual positive server retirement and SQLite writer reacquisition. Actual HTTP,
held native effects, loopback and shipped process tests are distinct from private
seed/counter/encoding seams. No actual OS entropy outage is claimed.

Complete execution/task/effect identities, lifecycle/attempt/cancellation/recovery/
approval receipt events, stream detach/overflow/EOF, automatic CLI export,
production log delivery and full RFC13.2 remain open. Durable approval evidence
continues to live in the Store. M0, CP001–009 partial observations, CP010, M5 and
all56 broad release scenarios remain open; the core profile is unfrozen.
