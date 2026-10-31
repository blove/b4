# Runtime-store boundary

Status: **bounded native transactions implemented**. `b4-runtime` supplies the
shared synchronous Store contract, memory backend and trusted-host execution
service; `b4-sqlite` persists portable snapshots and a transition journal. Durable
thread context, coherent views and fenced HTTP admission are implemented. This is
a subset of the RFC storage design. See `transactional-runtime-design.md` and
`decisions/0005-transactional-snapshot-store.md` for exact scope.

Threads retain their latest execution identity; earlier terminal executions
remain loadable. Atomic admission, expected revision/owner fencing, snapshot
validation and journal updates share a backend contract. The runtime commits
InFlight before calling an effect, returns obtained results on failed completion
publication, and supports completion-only retry without invoking the callback.
Ready task records and committed attempts represent durable eligibility and
claims. The shared execution driver now schedules caller-admitted turns with one
store coordinator and bounded scoped callbacks. Automatic unfinished-execution
discovery and durable autonomous intent queues remain open.

## Commit-before-dispatch invariant

A kernel transition proposes a replacement snapshot, expected revision and
ready task candidates in `dispatchable`. Committing a Start or Resume plan does
not make those candidates externally executable. The host must authorize each
candidate and propose `Dispatch` with a fresh attempt identity. The store
persists that attempt's `InFlight` snapshot before the adapter is invoked.
Every proposal requires revision and owner-generation checks. A stale revision
or failed commit must invoke nothing. Reloading after a conflict cannot replay a recorded
successful effect.

The store is responsible for atomicity, compare-and-swap, uniqueness and durable
ordering. The kernel is responsible for semantic transitions. Transport and
authorization adapters must use the same runtime service; they cannot write
task or checkpoint records independently.

| Full RFC transaction (partially implemented) | Atomic records/invariant |
|---|---|
| Admission | Authorize actual thread generation; capture route/revision/config; claim turn; enqueue intents |
| Attempt start | Commit task/attempt `InFlight` before invoking the adapter |
| Completion | Accept matching result once, retain per-task patch, join/checkpoint when ready |
| Task park | Commit prompt, parking provenance, task and required grant before disclosure |
| Turn park | Establish stable pending set after siblings settle; release active segment only |
| Resume | Consume all grants/decisions and enqueue continuation in one commit |
| Cancel | Compare captured turn and owner generation; prohibit future dispatch |
| Settle | Persist output/state and terminal lifecycle before releasing execution slot |
| Delete | Invalidate native state and approvals under one consistent thread-generation rule |

The foundation `CommitPlan` models this boundary. Store commit success establishes
the implemented transaction. Returning a kernel plan or serializing a snapshot
alone does not establish durability. Authenticated scripted admission uses this
boundary. The embedded approval service and explicit approval-enabled HTTP host
commit required grants and Once/Deny acceptance with continuation through the
same Store/ApprovalStore instance. Fenced deletion is implemented as described
below. Persistent permissions and autonomous dispatch records remain future work.

## Resource context and policy fencing

`ThreadSeed` stores creation milliseconds, user metadata and optional immutable
`ThreadOrigin` with application/tenant/principal. Components are nonempty,
control-free and bounded to 256 UTF-8 bytes. Legacy `create_thread` calls retain
creation time zero, empty metadata and unknown origin. HTTP does not expose such
origin-less records. Origin is distinct from arbitrary metadata.

`ThreadView` returns the thread and latest execution coherently. Its checked
unsigned `resource_revision` advances with each admission or changed execution
commit; an identical no-op commit does not advance it. `admit_fenced` compares the
policy-captured generation and resource revision atomically before admission. A conflict or
overflow rolls back the complete transaction. Trusted internal admission remains
available separately. `readiness` checks accessibility and current ownership;
SQLite also revalidates its schema/checksum.

## Owned cancellation transaction

`Store::cancel_run_fenced` is a trusted optional capability with an unsupported
default. Its target binds thread generation, resource revision, active execution,
execution revision/owner and turn. Memory and SQLite validate the exact current
owner and recompute Cancel under one lock/Immediate transaction; only Accepted
or Running work without a failure drain can accept a new request. Stable parks,
terminals and prior-owner work are refused. Snapshot/resource/journal and target
unconsumed-grant deletion commit atomically. Consumed grants and acceptance
receipts remain unchanged; cancellation does not fabricate Deny or refund Once.

`EffectOutcome::Cancelled` acknowledges a positively never-entered or trusted
cooperatively quiesced attempt through exact CancelAttempt. It retains attempts
and history and creates no result or application failure. Actual late results
remain recorded without state reduction; late known failure retains precedence.
Persistence uncertainty stays durable and faults host readiness, including after
a prompt cancelled terminal. This capability does not itself prove a local
worker quiesced; the host retains its claim/capacity/store through that boundary.

## Portable records

Internal snapshot schema version is `1`. Route identity, exact execution
definition revision, immutable budgets, task/effect/attempt identities, results,
pending interrupts, cancellation state and owner generation survive the
implemented snapshot store. Formats cannot embed Rust futures, trait objects, HTTP sockets or
library-owned engine classes. AP state remains a separate projection.

A store must validate supported snapshot format and definition revision before
restoration. Unknown format, malformed continuation or unavailable definition
fails closed. Existing TypeScript/LangGraph checkpoints are not importable by
this foundation.

## Native SQLite profile

The release design is one authoritative SQLite database and one native process
per application data directory, protected by an OS-held exclusive owner lock.
Owner generation guards stale completions; it does not enable multiple replicas.
Closing a store explicitly releases its owner lock after closing the database
connection. A descriptor temporarily inherited by an unrelated spawned child
cannot keep the closed store's ownership alive.
The implemented connection uses WAL, `synchronous=FULL`, `foreign_keys=ON` and
a verified five-second busy timeout. Each successful open advances its durable
owner generation; ordinary commands and old-owner completions fail until explicit
recovery transfers an execution to the new owner. Separate databases
cannot form the resume transaction. Network-filesystem deployment is excluded.

Required logical records include threads, executions, turns, checkpoints,
tasks/attempts, pending writes, interrupts, approval grants/decisions, permission
decisions, dispatch intents, migration checksums and a transition journal.
The version-3 schema extends durable resource context/revision with immutable
thread generation and dedicated approval-grant/acceptance records. Exact known
version-1/version-2 DDL and checksums are preserved. Their transactional upgrade
retains snapshots/journal/history, creates legacy thread generations once and
commits with the next owner generation. Origin-less legacy records remain denied
by approval authority. Failed validation or exhausted ownership rolls back the
upgrade. Unexpected versions, objects or checksum changes fail closed. Normalized
task/permission/dispatch tables, further migration paths and backups remain open.
The journal is an internal recovery record, not public SSE replay.

Grant issuance commits with the exact Park snapshot; complete decision-set
consumption and non-secret actor/decision acceptance commit with the exact Resume
snapshot, resource revision and journal. The service rechecks ownership and both
captured revision fences. Ordinary commit cannot approve or deny grant-bearing
pending tasks without an acceptance record. Tokens live only in the sensitive
grant table and dedicated pending output, so the whole database/backups are
sensitive even though comparisons use a verifier hash. Accepted turns and Ready
task snapshots survive reopen for explicit known-ID recovery/dispatch with fresh
policy. The HTTP bridge uses the same atomic acceptance transaction; separate
capacity and per-thread active-worker claims are held before consumption and
through quiescence. Those in-memory claims do not replace the durable execution's
thread admission slot. Stable park releases an active worker permit while keeping
that slot. A detached viewer cannot dispose accepted work or its guards.

HTTP does not adopt prior-owner executions automatically. A trusted host may
explicitly recover a known parked execution with its exact definition and fresh
authority, preserving its prompt/grant/resume identities, then resume it through
HTTP. Accepted Ready work remains a durable continuation after reopening and
requires no new approval. The trusted embedding recovery API can launch that
known accepted continuation through the shared HTTP driver; there is no public
launch operation or automatic discovery. Three actual SIGKILL tests qualify
selected driver boundaries; the full process-kill matrix remains open. See
[durable-approval-design.md](durable-approval-design.md) and
[http-approval-design.md](http-approval-design.md).

## Uncertain effects and failures

An `InFlight` record after restart does not reveal whether the external call
occurred. Pure or qualified externally idempotent effects may retry under their
policy and original effect ID. Reconcilable effects need an authoritative query;
unsafe/unknown outcomes block. This boundary provides no exactly-once guarantee
for arbitrary external effects.

Explicit settled application failures use the same snapshot/journal transaction
and owner/revision fences as successful results. `dispatch_outcome` persists the
attempt first, then commits Succeeded or Failed. An outer callback error keeps
InFlight uncertainty. `OutcomeNotCommitted` retains the obtained outcome, and
`complete_outcome` borrows it for completion-only retry without replaying effects.
Failure drain holds admission until siblings settle; recovery cancels safe
uncertain siblings instead of redispatching. Task failure is an optional additive
snapshot-v1 field; the settled-failure increment needed no SQLite DDL change.
The subsequent grant service upgrades storage to v3. Legacy snapshots
without task failure fields still load. New failure-bearing snapshots fail closed in old binaries.

Persistence failure must prevent successful result/terminal publication and new
dispatch for affected work. Backup qualification must use a SQLite-safe procedure
and restore parked prompts and accepted continuations. Current bounded evidence
covers memory/SQLite contract parity, rollback through
injected SQLite trigger failures, stale revisions/owners, corrupt snapshots/schema,
reopening parked continuations, authenticated HTTP acceptance rollback across
grant/snapshot/journal/acceptance records, separate-process ownership contention and recovery
after killing an idle child with committed work. Disk full, backup/restore,
filesystem qualification and the full separate-process crash matrix remain open.
These local tests do not complete the core release gate.

## Fenced trusted recovery

ApprovalMutation::Recover reconstructs the exact kernel plan under the Memory mutex or SQLite immediate transaction, comparing the complete target identity/generation and execution/resource fences. Owner/snapshot/resource/journal change together; grant rows and acceptance audit remain unchanged. No schema migration is required. Actual prior-owner restart and fault rollback are qualified through SQLite reopen; Memory has no owner-advance API and is qualified only for current-owner refusal and shared validation. Raw Runtime remains trusted and its ordinary recover method alone is not the resource-policy CAS supplied by this service.
## Fenced thread removal

Store::delete_thread_fenced is a trusted optional atomic capability; its default
returns UnsupportedOperation. Memory and SQLite compare the complete captured
thread incarnation/resource/active-execution fence under their transaction lock.
They validate all owned historical snapshots, dependent indexes/payload identities
and foreign active pointers before removal. Only idle/settled/exact active stable
parks are safe; active uncertainty is busy and inactive unfinished history is
corruption. SQLite clears the active pointer then removes grants, acceptances,
journal, executions and thread in its immediate transaction. Any failure rolls
all rows back. Recreating an ID gets a fresh random generation. Schema3 and prior
schema migration bytes are unchanged. These are scoped deletion guarantees;
backup/restore, cross-replica deletion and whole crash qualification remain open.


## Bounded attachment read boundary

Attachment authenticates an exact immutable origin and recorded registered
Definition under Operation::Attach. It captures the original generation/execution/
turn/owner before initial policy, renews policy and reloads ordinary identity after
policy returns before disclosure. Async request ownership holds the unique viewer
guard; a blocked policy worker receives no guard. Capacity/overflow/state limits
remain private until authority passes. A subscription holds no store, execution
permit or worker claim. It receives only the selected turn's snapshot and tail.

Stable parked approval disclosure after reopen uses AttachmentPending authority
on a fresh token-free captured view before sensitive rows. The original stored
owner is validated without adoption; full generation/resource/execution fencing,
all historical grants/acceptances and current pending bindings must be coherent.
Reading preserves expired tokens, original TTL and audit; acceptance still rejects
expiry. Ordinary pending/resume owner rules remain unchanged. Unknown origin,
unavailable definition, stale target, orphan unfinished work and recovery-blocked
state never cause attachment to start or recover work. No SQLite schema change or
durable token-stream replay is introduced; schema version3 and migrations remain
unchanged. See bounded-run-attachment-design.md.

## Shared driver ownership

Preparation commits an exact InFlight task before callback entry. One coordinator
per owned turn performs all preparation, parking and completion writes; workers
return bounded immutable outcomes. Queued outcomes own their task slot through
completion and postcommit projection. Revision and clock retries renew the original
coherent identity and attempt without replaying callbacks or resetting budgets.

A preparation panic is reconciled against the original exact durable attempt.
Returned claim evidence cannot override a positively conflicting ordinary view or
authorize an entered sibling cancellation. No outcome is inferred from a panic.
Actual obtained results survive coordinator faults and failed completion; an
already durable result is never retained or replayed because projection failed.
All scoped workers join before the embedding releases producer/store ownership.
See [shared-execution-driver-design.md](shared-execution-driver-design.md).

## Native assembly metadata and immutable callback recovery

Store::capabilities declares finite backend kind, runtime durability and optional
fenced cancellation/deletion without probing or mutation. Defaults are
Unspecified/false; Arc/reference wrappers forward exact declarations. Memory and
SQLite advertise their implemented ports. This trusted metadata does not replace
readiness or detect a custom optional-method override. No new persisted record,
SQLite migration or dependency is introduced.

Physical approval durability remains ApprovalStore::approval_durable, independent
of configured service mode. A host built only through Store cannot query that port
and reports null for unavailable approval durability, even if the declared backend
kind is SQLite. Explicit approval/native hosts report the physical bool and
Disabled/DurableRequired/VolatileFixture separately.

The immutable registry is process configuration, while the complete persisted
Definition and execution revision remain recovery fences. Replacing callback,
validator or output behavior requires a new declared execution revision. Matching
labels do not prove identical binaries. Known-ID recovery still renews current
authority and compares the original generation/origin/full definition; missing or
changed registrations cannot select a newer callback. Native callback uncertainty
retains conservative InFlight evidence and follows its declared recovery policy.

## Supervised drain ownership

Shutdown ownership includes blocking auth/policy/readiness/preflight and Store jobs
as well as admitted execution producers. A counted carrier retains Shared/Store
through actual call return; execution producers also retain ownership through
scoped joins, completion/projection and claim/permit release. Framework Store
captures drop before final owner retirement. Request abandonment and a bounded
wait expiring cannot release SQLite's owner while those jobs survive.

The ShutdownHandle and returned owned wait future retain only supervisor/fault
state, not Store. Quiescent is a stable registered-work observation after admission
closure, not a promise that external host/router/Store/auth/callback references
have been dropped. Physical release must be proved by actual destructor/reopen.
Unpolled external router futures may retain such references but cannot start Store
work after the cut. No persisted shutdown state, SQL, migration, checkpoint or
automatic recovery behavior is added.


## Private SQLite-safe export

The trusted mutable export serializes safe access to one SqliteStore instance and
copies committed WAL content through bound VACUUM INTO. It keeps the source owner
lock and does not change source records or owner generation. Private staging and
atomic no-overwrite hard-link publication precede success; final-file and parent
synchronization follow readonly structural validation. All error diagnostics are
static. A postpublication failure preserves the final file and reports uncertainty;
prepublication stage cleanup is best-effort. Archives contain sensitive grant data.

The controlled restoration contract is a final frozen cut: close admission,
observe Quiescent, stop raw writers, dispose aliases, uniquely own/export the
Store and permanently retire the source before installing a fresh copy. Existing
open advances the copied metadata epoch while stored executions retain their old
owner until exact trusted recovery. Parked grants keep original TTL; already
accepted Ready work retains its Once consumption/audit across expiry. Unsafe
InFlight work remains uncertain and follows conservative recovery blocking.

Schema/checksum/copied-owner, quick_check and foreign-key validation are structural
checks, not a full payload scan. The trusted synchronous operation has no duration,
archive-size/disk budget or whole-memory bound. No production restore/install,
network-filesystem, Windows ACL, arbitrary historical rollback, live-clone fencing,
failover or full backup/power-loss qualification is introduced. Broad T049 remains
open; new scoped qualification is recorded separately.

## Standalone launcher ownership

b4-cli prepares one private Unix data leaf under an existing trusted canonical
parent, with fresh0700 directory and create_new0600 runtime.sqlite3/owner.lock.
Restrictive umask repair applies only to newly created entries; existing exact
private files are reused unchanged. Config-check inspects metadata only and does
not open SQLite, migrate, acquire the owner lock, validate database integrity or
advance the epoch. Listener binding precedes Store open so a bind refusal cannot
advance existing ownership. The underlying SqliteStore API/schema is unchanged.

The compiled process owns its host/application/auth/policy/executor and exposes
no raw Store clone. Its first signal closes host admission before Axum shutdown
notification. It retains resources across registered-work and transport incomplete
waits, requiring both physical Quiescent and successful Axum completion before
normal capture/executor destruction. A lost server witness retains indefinitely,
including its owned opaque error payload; zero owners alone is insufficient.
Actual subprocess tests reacquire the real lock after runner return before child
exit. Physical retirement preserves parked authority and uncertain InFlight
evidence; faulted Quiescent returns1 after both positive witnesses. It does not
automatically adopt/replay prior-owner unfinished work. Ordinary
pending still refuses prior-owner unsupported state while readonly parked
attachment can project the unchanged original grant under existing authority.

This scoped native macOS process evidence does not establish Linux volume
packaging, UID ownership, Windows ACLs, network-filesystem durability, power-loss
recovery, forced callback termination or full operational shutdown. Those require
separate direct qualification and immutable receipts.

## Scoped Linux persistent volume

The local Linux/arm64 receipt independently qualifies the nonroot runtime with a
verified private named volume, actual file modes, live owner exclusion and reuse
after positive SIGTERM/SIGINT retirement. A qualification-only observer uses one
explicit read-only transaction over all six logical tables, including live WAL.
It closes promptly before retirement. Clean stopped immutable observations require
absent WAL/SHM/journal, no observer and positive retirement of every known writer.

Private comparisons preserve all logical rows byte-for-byte except one metadata
owner-epoch advance on each reopen. Original execution owner, snapshots, prompts,
grants, expiry and acceptance remain exact. Read-only prior-owner parked attachment
does not adopt work; ordinary pending retains501. This is not WAL/SHM byte
preservation, full semantic-authority validation or a shipped inspection API.
Other architectures, power loss, network filesystems, production restore and full
operations remain open. See [Linux evidence](../../conformance/expected/linux-container-verification.json).

## Stopped readonly operator reader

The separate native inspect_stopped API never calls Store startup, migration,
checkpoint, recovery or mutation. It uses an existing real0700 directory,
regular nonsymlink0600 DB/owner lock and refuses every sidecar entry. A verified
existing read-only owner descriptor holds nonblocking exclusive flock across
immutable readonly SQLite, bounded exact schema comparison, integrity/FK and
Snapshot1/index/thread validation. Bytes/epoch/inodes/modes/mtimes/entries stay
unchanged; atimes are excluded. Trusted stable local paths and cooperating owners
on the same lock inode are required.

One guard owns source and expected in-memory connections plus the exact lock.
Actual explicit close of all connections precedes explicit unlock on normal
success/refusal/unwind. Source witnesses and sidecars are checked around close.
Actual close failure retains the returned live connection and exact owner lock
inseparably in an intentional process-lifetime quarantine; expected-schema close
uncertainty follows the same rule. Explicit unlock uncertainty retains its exact
FD. Neither destructor invocation nor OS process cleanup is positive close proof.
A report contains encoded safe bytes only and returns after positive retirement.

Exact six-table structure/FK and execution Snapshot1 semantics are the declared
scope. Thread metadata/origin and journal/grant/acceptance JSON semantics are
explicitly deferred. Counts do not establish whole-store validity, recovery
eligibility or source authenticity. Fixed independent caps and a shared cooperative
SQL/CPU budget do not guarantee hard wall time or total RSS. See the
[operator runbook](stopped-readonly-inspection.md) and [ADR0020](decisions/0020-stopped-readonly-inspection.md).

## Ephemeral request observations

Request-boundary diagnostics are independent in-memory host observations. They
write no SQLite snapshot, journal, prompt, grant, acceptance or owner metadata,
and add no schema/migration/Store operation. The cloneable handle and encoded
batch retain no Store, owner descriptor or shutdown lease. Actual retired server
qualification requires positive transport/work witnesses and real writer
reacquisition; a diagnostic event or OS teardown cannot substitute for them.

The destructive256-record buffer is best effort, with persistent loss flags and
saturating cumulative evictions. It provides neither durable delivery nor an
atomic history cut. References cannot identify a recovery target or authorize
replay. Approval acceptance and recovery evidence remain in the existing durable
Store. The stopped-inspection contract and historical receipts stay frozen. See
[ADR0021](decisions/0021-request-correlation-and-bounded-diagnostics.md).
