# ADR-0006: Authenticated bounded scripted HTTP and scoped reference evidence

Status: accepted and implemented for the scripted HTTP increment.

Add `b4-http` around the shared Store/runtime rather than introducing a second
execution engine. Require explicit authenticator and resource policy dependencies
at construction. Persist immutable server-derived application/tenant/principal
origin separately from user metadata. Read thread/current execution coherently
and compare the policy-captured resource revision during admission. Unknown
legacy origin fails closed. Known schema version 1 upgrades transactionally to
version 2 without reinterpreting snapshot version 1.

Expose six operations and health/readiness probes. Reject approval-bearing and
non-pure definitions, missing fixtures and configurations exceeding finite
limits. Delete, GET attachment, resume and cancel remain protected unsupported
operations. Persisted parked/approval work cannot produce a fabricated empty
pending set; HTTP does not recover old-owner work.

Own each admitted blocking worker through effect quiescence. Its runtime/store
and global capacity permit outlive the wait or SSE viewer. Commit attempts before
effects and results before publication. Use direct JSON SSE payloads and a
bounded non-blocking queue. Overflow detaches the viewer without cancelling work
or claiming complete delivery. Successful pure workflows emit terminal `done`
without invented tool frames. Store/worker faults make the host unready and leave
uncertain state rather than falsely claiming success. Arbitrary callback failures
are excluded until a durable failure transition is qualified.

Record actual independent execution of the pinned TypeScript Fetch handler with
an authored 62-request workflow fixture. Preserve raw bytes and repeat recordings;
compare 22 successful input/void/falsy wait/POST-stream cases in Rust. Distinguish
that handler evidence from native TCP tests. Three web helper-port failures mean
the complete TypeScript build is not verified, despite the selected CLI/runtime
graph succeeding. Keep CP-001–004 partial, CP-005–010 unresolved, M0 open and all
56 broad release scenarios pending.

Preserve observed void semantics: wait JSON `null`, POST stream `done {}`. This is
a separate host wire projection from the existing strict explicit-output
`SseEvent::Done` variant/schema. Do not claim the historical draft schema collection fully describes
the service. Strict root fields, recursive duplicate rejection, object metadata,
bounds, auth and disabled routes are visible native changes. Complete error,
state, park/failure, attach and client-corpus parity remains open.

The example requires an explicit environment bearer token, binds IPv4 loopback,
checks exact Host and rejects Origin. Production applications must supply and
qualify their own integrations. Per-request/per-run bounds do not establish a
whole-process memory/retention bound or shutdown/restart qualification.
