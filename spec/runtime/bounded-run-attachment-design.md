# Bounded run attachment

Status: implemented and independently reviewed in the native scripted scope, with
438 passing Rust tests and eight current-build examples. Evidence is recorded in
`conformance/expected/bounded-run-attachment-verification.json`. The user authorized
continuous implementation. The 391-test cancellation receipt remains an immutable
earlier boundary. No schema, provider or dependency change was introduced.

## Behavior and alternatives

Implement GET `/threads/{thread_id}/runs/stream` as a read-only attachment. A live
attachment begins with a native state snapshot and receives subsequent events and
the selected turn's single terminal. An inactive attachment returns current durable
state, a retry hint of 2000 ms, and `done` with `{ "output": null }`. Missing threads
return protected 404; an idle thread with no recorded execution returns
409 `thread_route_unknown`. This agrees with the frozen lifecycle observations.

Use transient per-run publication plus existing durable snapshots. Reconstructing
old terminals from snapshots would contradict the captured null inactive terminal;
persisting token streams would add replay semantics and schema work without a
qualified requirement. Native scripted values, raw `{event,data}` turn frames and
approval envelopes remain explicit differences from TypeScript/LangGraph state and
merged messages. CP-007 remains partial and the core release scenarios stay pending.

## Publication and lifetime

Extend RunControl with a bounded native digest, immutable anchor values/input/start
time/resume metadata, bounded attached viewers, and cached exact terminal. A trusted
recovered publisher starts with unavailable history (`turn:null`,
`turn_truncated:true`); it never invents prior stream frames. Snapshot
cut and subscriber registration occur under the same brief publication mutex before
initial resource policy can wait. The unique subscription guard stays in the async
request future throughout both policy waits; blocking preflight receives only an
immutable cut and identity, never the guard. Thus an abandoned request releases its
slot even while preflight is blocked. Only registry capture happens under its mutex;
release it before subscription or any policy/store/effect work. A captured control
must match the initial coherent view's generation, execution, turn and owner. Never
select a successor after a policy wait.

Publication is logically live until its first terminal or fault, independently of
the worker claim. Existing attached viewers retain that exact terminal even after
late cancellation drain failure, registry removal or a successor run. A new viewer
after publication closes takes the inactive path, including during cancellation
drain. No viewer owns a claim, capacity permit, store or worker. Request abandonment,
denial and body drop release the unique subscriber immediately with RAII. Preflight
blocking work may retain the store only until its checks return.

The snapshot precedes the queued tail. Digest frames end at registration; tail
frames start after that serialization point, with no duplication or gap. Pending
cancellation blocks publication decisions under the existing gate. Cut/registration
uses a non-waiting publication lock even under Pending: publication is already
frozen and the subscriber receives the eventual decision. It never uses the settled
gate, holds a registry lock or drops a guard while holding publication's mutex.
This prevents policy-induced attachment/cancellation deadlocks. The cancelled
terminal uses the current RunControl arbitration exactly once. A fault before
terminal closes viewers without inventing successful output. A late fault preserves
the already selected cached/queued terminal for existing non-detached viewers.

## Bounds and detachment

Defaults: 16 GET viewers per turn, independent of the primary POST viewer and
execution capacity; 2 MiB digest; 4096 digest frames; 2 MiB encoded state; 2 MiB
queued bytes per viewer. Existing viewer_queue_frames also bounds each queue. All
limits have finite validated configurable maxima. Encoded UTF-8 bytes count toward
byte bounds, including event envelopes and terminal payloads.

Digest overflow discards the entire digest and permanently projects `turn:null`
and `turn_truncated:true` for that turn while continuing tail delivery. GET capacity
refusal returns HTTP 200 with only `detached` `{reason:"capacity"}` then EOF. Queue
overflow releases that viewer immediately and uses an independent signal so the
stream can discard buffered frames and yield one `detached` `{reason:"overflow"}`
then EOF; a client that stops reading has no delivery guarantee. The primary POST
viewer uses the same byte/frame bounds and overflow mechanism. Oversized initial
state produces healthy 409 `attachment_snapshot_limit`, releases its slot and
does not fault the producer. Capacity, overflow and snapshot-limit decisions remain
private until renewed exact origin/definition authority passes; unauthorized callers
receive no capacity hint. Quiet live connections retain configured ping cadence.
Set no-store/no-transform and proxy buffering headers. No event IDs or durable replay;
Last-Event-ID is ignored as a cursor request.

## Authority and durable reads

Add Operation::Attach. Authenticate first and require exact immutable application,
tenant and principal origin even under permissive policy. Both initial and renewed
policy use recorded route provenance. Before disclosure, check immutable registered
Definition equality. Unknown recorded origin/route yields protected refusal without
revealing provenance. A missing registered exact definition yields healthy
409 `thread_route_unknown`. A changed generation, execution, turn, owner, origin or
definition yields 409 `resource_changed`; never retarget a successor. Live progress
within the same immutable identity may advance resource/execution revisions because
the publication cut is carried by the captured control. Inactive reads require the
complete captured durable view/fence to remain equal after renewed authority.

Read stable parked prompts through a dedicated read-only
ApprovalService::attachment_pending(context, captured ThreadView) operation. Resolve
fresh token-free thread resource by captured thread ID, authorize AttachmentPending,
then compare the complete captured view/generation/fences and validate the snapshot.
Permit only stable WaitingForApproval with a nonzero stored owner no newer than the
current owner. Read sensitive ApprovalView only after authority and compare its
thread/execution to the authorized view; validate using the stored original owner,
including the shape and identity of every grant and acceptance, historical consumed
grant binding under its original policy revision, exact audit-to-consumed-grant
binding and no extra or duplicate unconsumed authority. Current pending proposals
must use current policy revision. Do not adopt, issue, consume,
extend TTL, refresh a token or write an audit. Existing pending/resume rules remain
current-owner-only. Pending disclosure preserves expired tokens and their original
expiry just as existing pending does; acceptance still checks expiry. Exact route
and current authority context must pass before sensitive row decoding; stored grant
policy revision is checked against that current context after the authorized read.

Inactive prior-owner terminal reads are safe. Prior-owner unfinished Accepted,
Running or CancelRequested without a matching local publisher, RecoveryBlocked,
or locally unfinished work without any matching local control return healthy 409
`recovery_required`/`attachment_unavailable`. Attachment never starts or recovers
execution. A matching locally closed control is distinct from an orphan: its known
terminal/drain allows the inactive durable-state/null-done path. A parked
required-approval route without an approval bridge is a healthy
unsupported state. SQLite stays schema version 3; all frozen migration files stay
unchanged.

## Qualification

Test missing/idle/foreign/origin-less and unknown/mismatched definition refusals;
renewed authority before sensitive rows, denied corrupt grants, first/renewed policy
held across successor or delete/recreate ABA; snapshot/tail cut and state ordering;
viewer capacity, disconnect and request abandonment; multibyte digest/frame/byte
bounds and primary overflow; cancellation fanout before quiescence and late durable
failure without a second wire terminal; new attachment after terminal; actual SQLite
completed and stable park reopen with unchanged owner/grant/expiry/audit; orphan
unfinished recovery refusal. Use controlled barriers and real TCP, plus full fresh
workspace, strict lint, reference checkers and example qualification. Add a separate
10-operation evidence receipt, retaining earlier receipts and captures unchanged.
