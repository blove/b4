# ADR0021: Server request references with bounded embedding records

Status: implemented native request-boundary subset; independent SPEC/QUALITY and
fresh macOS/Linux arm64 source/release checks pass. The scoped candidate evidence review passes; the independent final freeze decision is recorded separately. See the [design](../request-diagnostics-design.md) and
[embedding runbook](../request-diagnostics.md).

RFC13.1 requires correlation without changing legacy public error envelopes.
RFC13.2 and M5 still need broader operational diagnostics. The existing native
launcher and stopped inspection have narrow fixed disclosures; neither supplies
an execution identity/log-delivery system. A request boundary ring provides a
useful initial reference with testable ownership and bounded disclosure.

Add one server-generated `x-b4-request-id` to available native fallback responses.
A fallible128-bit OS seed is created once per host and checked counter1..u64MAX
never wraps. One direct dependency edge uses already pinned getrandom0.3.4;
package/version/checksum/features and all other edges remain exact. The selected
one-entry Cargo.lock difference reconstructs the prior protected hash when removed;
209 other protected artifacts remain byte-exact. Historical receipts stay unchanged.

Move the original Shared into the byte-preserved inner handler. Across its await,
the new wrapper owns only a resource-free diagnostics guard. Record fixed method,
reference, elapsed milliseconds and either constructed response/status or actual
pending-guard drop/null. These events describe the request future boundary, never
task completion, accepted approval, cancellation, body delivery or retirement.
The direct wrapper ownership test uses a finite private finish gate with an
explicit-release witness. Existing Axum Handler state is qualified separately.

Expose `HttpHost::diagnostics()` as a cloneable nongeneric handle and destructive
`drain()` returning capped owned compact UTF8 bytes. Store at most256 Copy records
with inline IDs in a preallocated ring. Attempt its mutex once; drop on contention
or poison, evict oldest at257, and persist loss/exhaustion plus saturating cumulative
evictions. Take/copy under lock, then serialize only fixed private DTOs outside it.
Encoding refusals mark loss without restoring removed records. Separate atomic
flag samples do not establish a globally atomic history cut.

Freeze the version1 eight-key batch/five-key record contract in a separate schema.
Exclude request/private/application identities and content, including even valid
opaque IDs, credentials, prompts, grants, inputs/outputs, raw errors and SQL.
The public namespace/counter disclose same-host linkage and sequence metadata;
they provide neither anonymity nor authority/authenticity. Diagnostic failures
cannot change readiness, runtime admission, shutdown ownership or durable state.

There is no new endpoint/configuration field, logger/sink callback, background task,
automatic launcher export or production delivery claim. Handles/batches may outlive
actual host/server resources. Fixed logical bounds do not promise allocator/RSS,
CPU/I/O deadlines or interruption of trusted callbacks. Existing stopped inspection
and AP contracts remain frozen. Full identity/lifecycle/stream diagnostics, durable
log delivery, real client parity and broader M0/CP/M5/core qualification are deferred.

Meaningful working-host missing-header RED and compiling event/drain stubs precede
implementation. Actual headers/bodies/authority refusals, issued-grant redaction,
held effects/future drop, buffer boundaries, contention/poison and positive actual
SQLite/native retirement are checked independently. Exact numerical/encoding and
entropy seams are labeled separately from real OS failures and genuine history.
