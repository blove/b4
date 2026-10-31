# Shared bounded execution driver

Status: implemented and verified within the bounded native increment. The attachment receipt remains an immutable438-test boundary. This increment
adds real parallel execution without providers, new dependencies or a schema change.

## Outcome and scope

Move the execution scheduling loop from the HTTP worker into `b4-runtime`. Embedded
and HTTP hosts use the same synchronous coordinator to start admitted work, park
required tasks, prepare durable attempts, execute bounded concurrent callbacks,
commit their retained outcomes and drain all callbacks before returning. HTTP keeps
authentication, admission, per-thread exclusion, run capacity, publication and AP
terminal projection. A trusted callback port supports native embedded effects;
the existing HTTP application remains an immutable registry of finite Pure scripts.

Use a single writer coordinator with scoped standard-library worker threads. The
alternative of independently mutating callback futures would force approval
dispatch to tolerate sibling revision changes during its final authority check.
Instead, workers return immutable outcomes over a channel and never call the store.
Sibling results wait while the coordinator prepares another attempt or calls policy,
so the existing complete claimed-view approval check remains strict. This introduces
no ambient executor or Tokio dependency into the runtime crate.

The scope includes actual callback overlap, stable ordered joins, parking beside
running siblings, cancellation/failure drainage, shared task capacity, retained
completion-only retries and separate-process crash qualification. It does not add
automatic recovery scanning, graceful shutdown/deadlines, Always permissions,
dynamic graphs, provider adapters or full core-profile compatibility.

## Ownership and identity

The caller owns the admitted execution, store lifetime and exclusive per-thread
producer claim throughout `drive_execution`. A driver instance captures the initial
thread generation, execution, turn, owner and full immutable Definition from a
coherent ThreadView. Every fresh reload must preserve those identities and current
store ownership. Resource/execution revisions may advance within that same run.
The driver must refuse a changed generation/turn/owner/definition rather than
deliver retained evidence into another execution.

One coordinator performs Start, Park, Dispatch and completion commits. External
cancellation remains a separately fenced actor. The host's callback-entry gate
continues to serialize entry against Pending/Committed/Faulted cancellation. A
worker does not hold a coordinator mutex, store transaction or publication lock
while it calls a native effect. A policy wait does not hold a store transaction or
publication lock. The host must not mutate this execution through another driver
while its producer claim is owned.

Every spawned worker is joined through `std::thread::scope`, including coordinator
errors/panics. Catch coordinator panics inside the scoped lifetime, notify the host
immediately, then drain/join children; an outer HTTP catch would run too late behind
an uncooperative sibling. Child callback panic is caught as unknown outcome using a static
diagnostic; the host fault notification runs immediately, before waiting for any
uncooperative sibling. No caught panic becomes an application failure or successful
terminal. The store, thread claim and active-run permit remain owned until every
callback has returned. Native effects are trusted code, not a sandbox.
The channel receiver, active invocation records and retained outcome state live
outside the caught coordinator closure and are borrowed by it. A coordinator panic
must not destroy already obtained or queued real outcomes before postpanic drainage.

## Driver ports and preparation

Introduce public `PreparedEffect { execution: StoredExecution, invocation:
EffectInvocation }` plus `Runtime::prepare_dispatch` and
`ApprovalService::prepare_dispatch`. Refactor existing synchronous dispatch APIs to
use these methods and retain their current behavior. Approval preparation performs
the existing authority-before-sensitive-read, consumed Once validation, durable
InFlight claim, renewed authority and complete claimed-view validation before it
returns. Preparation alone never invokes a callback. The returned invocation must
match the claimed task, attempt, input, logical effect, execution, turn and owner.
Preparation failures carry their typed cause and an optional token-free claimed
PreparedEffect: absent before Dispatch commit, present after a successful claim.
This preserves exact never-entered attempt ownership across renewed-policy refusal
or worker-spawn failure without inferring that a claim occurred.
The optional claim is evidence only; it never authorizes callback invocation after
preparation failed.

The public driver host port supplies fresh time facts and attempt IDs, preparation,
parking, callback invocation/entry gate, optional postcommit event projection,
positive exact local cancellation observation and static fault notification. Host
errors remain typed and are distinguished from kernel/store/invocation uncertainty.
The driver owns task selection, scheduling bounds, durable completion and drainage.
The host classifies its typed preparation/park refusals as healthy or infrastructure
faults. The driver always stops scheduling and drains children on either kind;
healthy policy/refusal and recovered-budget errors preserve their HTTP classification.
Callback uncertainty, panics and failed retained completion always notify a fault.
Plain native hosts can delegate preparation to Runtime; approval-enabled hosts
delegate to ApprovalService. The port must not embed AP, SQL or provider types.

Existing HTTP cancellation repair is represented by an explicit expected-race
classification plus positive exact local cancellation proof. A failed preparation
can acknowledge a never-entered attempt only after reloading the original run and
finding its exact task/attempt InFlight under cancellation. A Ready task cancelled
before its Dispatch claim has no fabricated outcome or refunded attempt. Ordinary
policy refusal or a faulted gate never becomes a cancellation acknowledgement.

## Scheduling and finite capacity

`DriverConfig` has a finite per-execution callback limit, completion retry limit and
settled result byte bound. Defaults are two concurrent callbacks, eight completion
attempts and65536 serialized result bytes. Validate ranges before Start or Dispatch.
A shared `TaskCapacity` bounds callbacks across drivers; HTTP defaults to eight
global callbacks and permits explicit validated configuration. No new persisted
configuration or budget reset is introduced; Definition.max_attempts remains the
durable total attempt budget across resume/recovery.

Acquire a task permit before committing Dispatch. Transfer it to the worker and
its returned outcome; release only after callback quiescence, completion and postcommit projection handling.
With no capacity, the coordinator waits without scheduling, periodically rechecks
the original run and cancellation/fault state, and never creates a waiting thread
per task. The number of running workers and queued outcomes cannot exceed the
per-execution/global limits. The fixed Definition already bounds total task count.
Before any capacity wait, process queued returned outcomes, which themselves own
permits. Use nonblocking acquisition plus channel polling/timed receive so a driver
cannot wait for a permit held by its own completed queued callback. No capacity-only
blocking acquisition is allowed in the coordinator.

Select Ready tasks in Definition order. Required parking is considered before
callback-capacity/per-run-limit gating; parking does not need a callback permit.
If the first Ready ordinary task has callback capacity, claim it before later
required tasks. If it is blocked by callback capacity, park the first Ready required
sibling before waiting, then retry the earlier ordinary task. This preserves ordinary
claim order and permits durable approval parking without a callback slot.
Required unapproved tasks may park while other siblings run, but pending disclosure
and terminal park occur only after the kernel reaches a stable WaitingForApproval
state and all callbacks have quiesced.
All tasks in a batch keep their original committed batch input. Workers may finish
in any order; each result commits independently and the kernel reduces state in
stable Definition order only at the join. Failure stops further scheduling and
drains already entered siblings. No state reduction occurs for late cancelled
results. The next sequential batch starts only after the existing kernel join.

## Retained outcomes and error handling

A worker invokes its exact callback once and returns the actual settled outcome or
unknown-outcome error. Validate payload depth and serialized byte size in the worker
before channel enqueue using a bounded counting writer; the queue must not retain an
arbitrarily large native result. The coordinator retains each valid bounded outcome
until it commits or is returned in a typed driver failure. An invalid/oversized result
is dropped with a typed limit error and exact unsettled invocation; its real attempt
remains uncertain and the host faults. It never becomes an invented application
failure. Native code can allocate its own unbounded memory; this is not a sandbox.
Continue draining every child after the first error and preserve later obtained
valid outcomes.
Accumulate typed causes; a later sibling uncertainty/completion fault escalates the
overall host fault immediately even when the first cause was a healthy policy
refusal. Do not let first-error classification hide later infrastructure failure.

Retry only completion on RevisionConflict or ClockMovedBackwards, after a fresh
coherent reload positively matches the original run, current owner and exact
task/attempt. Obtain fresh time after that reload. A real retained outcome supplies
positive evidence independent of local cancellation, so ordinary concurrent actors
may cause these bounded retries. Never retry the callback, prepare another attempt,
infer an outcome, broaden the target or reset its budget. Corruption, owner/identity
changes, unexpected task states and retry exhaustion fault the host and preserve
durable InFlight uncertainty plus returned retained evidence. Exact duplicate
completion remains governed by the existing kernel checks.

Returned preparation evidence is checked against the original requested invocation
before a failed read can retain it. A fresh readable view must prove the exact
InFlight claim. A rejected refusal claim cannot enter cancellation repair. If a
preparation port panics after committing, reconcile its original staged intent
against exact durable identity without inferring an outcome.

Postcommit projection receives an invocation/outcome only after its durable commit.
Remove the committed envelope from retry ownership before calling projection, but
retain its task permit locally through the hook. Projection panic cannot replay or
report an already durable outcome as unsettled.
For the current HTTP scripts, chunks retain their existing after-commit publication
semantics. Cancellation suppresses late chunks/interrupts/terminals through the
existing RunControl. HTTP terminal projection runs once on the driver's returned
durable terminal; its cancellation cache and attachment cut remain unchanged.

## Qualification

Use barriers and durable callback counters, not timing-only assertions, to prove
two callbacks overlap, reverse completion retains Definition-order append, shared
global permits hold across drivers, preparation sees no sibling store mutation,
and stable park preserves successful siblings. Inject actual completion CAS/clock
conflicts and verify callback count one and retained outcome identity. Negative
identity/owner/attempt/corruption/retry-exhaustion cases remain faulted.

Exercise existing HTTP APIs plus real TCP parallel park/resume, cancellation fanout
before both siblings quiesce, viewer abandonment and unchanged readiness/refusals.
Use separate child processes with a local effect ledger and explicit synchronization
to kill after committed sibling+park, after accepted decision before callback entry,
and after an Unsafe effect before result commit. Reopen with a new owner; expose the
same pending grants read-only, recover through the trusted exact target, accept once,
and prove no committed sibling replay. Unsafe unknown outcomes remain RecoveryBlocked
without a second ledger effect. This is native crash evidence, not TypeScript crash
parity or a pass for all56 broad release scenarios.

Preserve all previous receipts/fixtures/captures and dependency/schema hashes. Save
new exact-count verification separately after independent design, spec and quality
reviews, fresh workspace/contract/reference checks and built examples. Continue
remaining core work while no manual blocker exists.

## Verification boundary

The separate493-test workspace receipt records55 new Rust tests:11 preparation,
28 driver/repair/evidence,13 HTTP API/TCP and3 actual SIGKILL cases. Fresh formatting,
strict workspace lint,50 reference fixture tests,8 Python verifier tests,18 schemas,
170 named local requirements and8 rebuilt examples pass. Historical receipts and
159 protected prior artifacts remain unchanged. No schema/dependency change, new
TypeScript capture, provider call, full crash parity or core release is asserted.
