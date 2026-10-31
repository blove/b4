# Authenticated scripted HTTP subset

This increment records selected parser/input/terminal behaviors from the pinned
TypeScript runtime and adds a bounded native HTTP host. It does not freeze M0 or
claim the ten-operation core profile. Reference capture invokes the actual built
TypeScript Fetch handler with deterministic workflow fixtures; native transport
tests additionally use TCP. These are distinct evidence classes.

## Durable projection and policy

Extend stored threads with creation time, user metadata, immutable optional
server-owned origin (application, tenant, principal), and a resource revision.
Legacy native threads have unknown origin and cannot be exposed through HTTP.
Thread plus latest execution is read coherently. Admission can compare a captured
resource revision after policy evaluation. Context is never taken from user
metadata. Preserve existing native Store calls for trusted internal callers.

SQLite upgrades the known version-1 schema transactionally to version 2, after
validating the old checksum/schema. Preserve snapshots, owner generation and
journal. Unknown versions or extra schema objects still fail closed. Each changed
execution increments its thread resource revision in the same transaction.
Readiness checks store accessibility/schema/owner as well as the host fault flag.

The HTTP constructor requires an authenticator and a resource policy. The
authenticator yields server-derived application/tenant/principal identity.
Policy receives operation, stored origin and stored route provenance before a
protected resource is disclosed or mutated. No anonymous/allow-all fallback is
provided. Unauthorized existence reads return a generic denial. The demo uses an
explicit bearer token from the environment, binds loopback, and rejects browser
Origins and unexpected Hosts. Credentials are never saved or logged.

## Operations and work ownership

Implement POST /threads, GET /threads/{id}, GET /threads/{id}/state,
GET /threads/{id}/pending_interrupts, POST /threads/{id}/runs/wait and
POST /threads/{id}/runs/stream, plus /healthz and /readyz. Other operations return
an authorized unsupported response. A new thread has no checkpoint; AP state is
a separate projection, never a serialized internal Snapshot. Pending responses
are no-store and contain an empty interrupt list for this no-approval subset.
Persisted approval-bearing/parked/recovery-blocked and unsettled prior-owner work
is rejected as unsupported. Completed historical work remains readable; HTTP
does not recover old-owner work.

Applications register immutable bounded scripted routes and effects. Reject
approval-bearing definitions at construction; do not select grants-off implicitly.
Use literal registered route lookup and finite task/attempt/result limits. Custom
application input-validation parity remains excluded. All runtime
work uses blocking workers, and the worker owns the runtime/store through effect
quiescence. Wait/SSE viewer disconnection does not cancel work or release its
thread slot. Fixed global admission capacity is checked before a run is admitted.

Effects execute only after committed InFlight. Results are published only after
their completion commits. SSE uses direct JSON payloads, a bounded non-blocking
viewer queue and heartbeat. A connected viewer receives one terminal done after
the successful run settles. Queue
overflow detaches the viewer; it cannot stall persistence. Unexpected effect,
store or worker failure marks the host unready and retains uncertain durable
state; an error response must not claim a settled success. The subsequent
[settled failure increment](settled-failure-design.md) adds an explicit bounded
application failure fixture. Its terminal derives from committed Failed and keeps
readiness healthy; caught callback/store errors cannot become application failures.
Per-request/definition/run/active-worker/viewer-queue bounds do not qualify total
retained history, threads, connections or slow settled viewer retention.

## Characterization and differences

Capture selected CP-001–004 cases: empty/malformed/unknown/duplicate request keys,
content-type tolerance, absent/null/scalar/array input, void and falsy output,
fixed workflow failure, wait versus POST stream. Preserve observed decisions only
after executing the independent fixture. Native strict root fields, duplicate
key rejection, bounds, auth and disabled unsupported routes are explicit changes.
Grant precedence, deletion, attachment, full state-channel behavior, grant
disclosure, client corpus and park behavior remain open characterization work.

The selected comparison covers 24 probe/void/falsy/known-failure cases against the
62-case independent recording. Void wait returns `null`; void POST stream sends
`done {}`. This host wire variant is separate from the historical explicit-output
`b4_contract::SseEvent::Done { output }` variant and its unchanged JSON Schema.
Known failure wait returns 500 with an execution_error message; stream sends
`done {output:{error:message}}`. These two terminals match retained observations.
Custom validation observations remain excluded from native comparison.

Thread metadata/timestamps and AP state projection are native contracts until
their independent comparison exists. Selected wait/stream bodies can be compared
without claiming complete TypeScript compatibility. HTTP/readiness/shutdown,
grants, safe continuation, container and crash qualification remain partial.
