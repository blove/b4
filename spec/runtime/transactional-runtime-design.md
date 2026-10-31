# Transactional execution increment

This increment extends the accepted native foundation with a synchronous runtime
service and one store contract implemented by memory and SQLite. The supplied RFC
is architectural input. M0 characterization remains open; this work does not
claim Agent Protocol compatibility or a core-profile release.

## Boundary and records

`b4-runtime` owns `Store`, `MemoryStore` and `Runtime<S>`. `b4-sqlite` implements
the same store using a single serialized connection and an OS-held exclusive
data-directory lock. All kernel commands pass through the runtime; effects run
only after a successful `Dispatch` transaction. This synchronous API is intended
for a host's blocking worker; it creates no executor or background tasks.

`ThreadRecord` holds a typed thread identity and the latest execution identity.
The later [HTTP increment](http-subset-design.md) adds creation time, user metadata,
immutable optional server origin, coherent views and a resource revision with
fenced admission. Known SQLite schema version 1 migrates transactionally to 2;
portable execution snapshot version 1 is unchanged.
`StoredExecution` associates a thread with its portable kernel snapshot. Previous
terminal executions remain loadable. Admission rejects a busy, parked, cancelling
or recovery-blocked execution, and accepts a successor only after completion,
failure or cancellation. Execution identities are globally unique within a store.
Admission and each changed snapshot append a journal entry in the same
transaction. Thread creation has no execution journal entry. The journal stores execution revision, owner generation,
status and timestamp; it is not public SSE replay.

`ExecutionFence { revision, owner_generation }` must match the stored snapshot.
A commit validates snapshot shape, immutable execution identity and definition,
and exactly one revision increment. An identical kernel no-op may retain its
revision without adding journal entries. Ready tasks and persisted `InFlight`
attempts in the snapshot represent dispatch eligibility and claims; no separate
queue or transport acknowledgement is provided in this increment.

## Ownership and recovery

The memory backend has owner generation 1. Each successful SQLite open increments
a durable generation under the exclusive lock. Normal runtime commands require
the stored generation to match the current store owner. Recovery accepts an old
fence, uses the caller's available definition revision and fresh turn identity,
and commits the current owner generation before further dispatch. Completions
from an earlier owner are rejected even if their task/attempt identities match.
The store never releases its OS lock before its connection is dropped.

SQLite uses verified WAL, FULL synchronization, foreign keys and a finite busy
timeout. A versioned schema and migration identity fail closed on an unsupported
database. Loads deserialize and semantically validate snapshots, checking indexed
revision/owner/execution identity against the payload. Corrupt data is an error,
not an empty thread. SQL transaction failure must roll back the snapshot, active
slot and journal together.

## Effects and limits

The runtime receives explicit time and identifiers, plus a trusted host effect
callback. The callback sees the original effect ID, fresh attempt ID and recorded
input. It executes at most once per invocation of the runtime dispatch method.
Failure to commit Dispatch invokes nothing. Callback failure leaves `InFlight`;
failure to persist its result returns an error and publishes no successful
completion. Dispatch requests fresh completion facts from the host after the
callback succeeds. Completion-only retry borrows the result so repeated
persistence failures cannot consume the host's only copy. The runtime never
retries a callback automatically. Recovery applies
the kernel's declared policy; unsafe or reconcilable uncertain calls block.

Recovery is explicit by a known execution identity; automatic startup scanning
and scheduling of unfinished executions remain future work.
Approval decisions remain trusted host inputs. This increment adds no grants,
permissions, authentication, HTTP, thread deletion, durable external-effect
reconciliation, async cancellation/shutdown, backups, filesystem qualification,
disk-full simulation or full process-kill crash matrix. In-memory tests prove
transaction semantics only. Reopen and separate-process lock tests establish a
bounded SQLite persistence/ownership claim, not arbitrary exactly-once effects.

## Acceptance evidence

Run a common backend suite for admission, stale revisions/owners, retained
terminal history, journal atomicity and duplicate no-ops. Runtime tests prove
commit-before-call, zero calls on failed commit, result persistence failure,
parallel completion, approval resume and safe recovery. SQLite tests add reopen,
owner-generation fencing, corrupt snapshots, transaction rollback, schema
rejection, PRAGMA verification and a competing process lock test.
