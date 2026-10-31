# ADR0020: Bounded redacted inspection of clean stopped SQLite sources

Status: accepted; scoped implementation passes independent SPEC and QUALITY review.
Fresh macOS/Linux arm64 workspace and release qualification pass; final evidence
freeze is pending. Results belong to the [separate inspection receipt](../../../conformance/expected/stopped-readonly-inspection-verification.json).

RFC sections 13.2/15.2 requests native operational status, blocked-work and version
inspection. The launcher and Linux container increment supply an executable and
local ownership profile, while their historical receipts leave production
readonly inspection open. A source-preserving local report must not start the
Store, increment ownership, recover a WAL or disclose approval authority merely
to count recorded state.

Add native `b4run inspect` and `b4run inspect --blocked` with a separate synchronous
configuration branch. Only `B4RUN_DATA_DIR` is consulted, defaulting to existing
`.b4run-data`. Token, port and shutdown configuration are ignored. Inspection
needs no `--demo` and creates no application/provider/callback, HTTP listener,
Tokio runtime, signal receiver or Store. The exact grammar offers no live/force,
positional path, generic SQL or caller URI mode. Version/help remain
environment-free. This is an authored native operational contract, not
TypeScript CLI parity or a new authenticated network operation.

Require a real existing 0700 data directory, nonempty regular nonsymlink 0600
`runtime.sqlite3` and existing regular nonsymlink 0600 `owner.lock`. The lock may
be empty. Retain device/inode/mode/size/mtime witnesses; reading may change atime.
Open the existing owner file read-only and take a nonblocking exclusive flock
after descriptor comparison, then recheck under ownership. Never create,
truncate, chmod, unlink, migrate, checkpoint, repair or recover source entries.
Any WAL/SHM/journal entry refuses, including empty files, broken symlinks and
directories; repeat the check around close. A clean closed WAL-header database
without sidecars is accepted without claiming historical WAL completeness.

The local parent must remain trusted and stable, and all cooperating owners
must lock the same existing inode. Immutable SQLite reads intentionally depend
on those assumptions. A raw writer or administrator bypassing that lock,
hostile ancestor replacement, network filesystems and UID/ACL auditing are
outside this profile. Positively retired known writers remain an operator
precondition; absence of sidecars, a timeout or forced process cleanup cannot
establish runtime/effect retirement.

Preserve bounded raw Unix path bytes and construct an internal percent-encoded
absolute `file:` URI with fixed `mode=ro&immutable=1`. Open SQLite with explicit
READ_ONLY, URI, NOFOLLOW and PRIVATE_CACHE and verify actual read-only state.
Install and read back safe engine limits, DEFENSIVE=true and TRUSTED_SCHEMA=false
before any prepare; guarded fixed PRAGMAs select query-only source access,
memory temp storage and a small cache. Register/invoke no application SQL
functions or extensions and accept no source virtual tables.

Select a bounded execution-snapshot reader. A schema/history-only summary could
not honestly validate task state or bound early SQLite materialization. A full
authority/history audit would require separate semantic checks and disclosure
policy and remains deferred. Source schema must exactly match bounded
in-memory schema-v1+migration-v2/v3 reconstruction before integrity/data queries.
Metadata must contain exactly id 1/schema 3/current checksum/nonzero eight-byte
epoch. Budgeted quick_check(1) and foreign-key checks apply structurally to
metadata, threads, executions, journal, approval_grants and approval_acceptances.

Read at most 256 execution rows in primary-key order, validating snapshot 1/kernel
invariants, indexed identity/revision/owner agreement, nonzero owner<=metadata,
thread existence and same-thread active pointers. Guard borrowed JSON with a
fixed depth 64 stack and 16384 lexical nodes before direct Snapshot decoding, then
drop each owned snapshot after projection. Thread metadata/origin and
journal/grant/acceptance payload semantics are not audited. Success therefore
means the fixed `schema_structure_and_execution_snapshots` scope, not whole-store
validity, authority health, source authenticity or safe recovery.

Freeze report version 1 and exact redacted fields in
[stopped-inspection-report.schema.json](../stopped-inspection-report.schema.json).
Count all execution history, initialized tasks, complete Definition-policy
declarations including uninitialized specs, and initialized InFlight/OutcomeUnknown
records by declared policy. Policies are not replay-safety proofs. Reader metadata
is package 0.1.0/SQLite 3.53.2/snapshot 1/store-schema 3 and target
b4-ap-core/0/native_subset/descriptor 2/core_profile_frozen=false. It does not
identify the original writer or fabricate live readiness/capability metadata.

Aggregate mode has blocked:null. Optional rows select recorded Accepted,
Running, WaitingForApproval, CancelRequested or RecoveryBlocked executions,
or initialized uncertainty. Each row has only a report-local ordinal 1..N,
stored status, three fixed booleans and task/policy counters. Ordinals supply
neither persistent correlation nor a recoverable target. A valid failed-effect
drain remains Running, or RecoveryBlocked with uncertainty; contradictory
Failed-with-active snapshots refuse as corrupt. No path/hash/epoch, stable
identity, timestamp, value, prompt, token, failure text or authority-derived
digest appears in the report.

Use fixed conservative ceilings: paths 4096 bytes/no NUL, main 64 MiB,
execution 256/fetch 257, snapshot 1 MiB each/16 MiB cumulative, schema 32/fetch33,
DDL 64 KiB, JSON depth 64/nodes 16384, blocked 64 and encoded JSON 65536 bytes excluding
the CLI newline. Safe SQLite limits are LENGTH 2 MiB, SQL_LENGTH 16 KiB,
COLUMN 64, EXPR_DEPTH 64, PARSER_DEPTH 128, ATTACHED 0 and worker threads 0.
Kernel 1024-batch/4096-task bounds remain distinct from HTTP registration 64.
Refuse rather than truncate and expose no budget overrides.

One cumulative budget covers both source and expected-schema connections plus
bounded CPU phases. The pure nonpanicking progress callback runs approximately
every 1000 VM operations and interrupts by callback 2000; positive own exhaustion
or a 5s cumulative elapsed check yields LimitExceeded. Approximate VM intervals
are not an exact instruction ceiling. Cooperative checking cannot cancel
filesystem/close/output blocking and the independent caps do not guarantee hard
wall time or total RSS, or sandbox arbitrary hostile images.

One private guard owns source and temporary expected-schema connections plus
the exact owner descriptor. Statements/rows have lexical lifetimes. Normal
refusals and unwinding close before unlock. Successful report return additionally
requires final source/sidecar witnesses and actual successful explicit unlock.
Safe report serialization finishes under its cap before retirement; the returned
report contains only encoded bytes and no resource or source authority.

On actual close failure, rusqlite returns the live connection. Retain that exact
connection, any other still-owned inspection connection and the owner descriptor
inseparably in a private safe process-lifetime quarantine. Expected-schema BUSY
is covered by the same guard as source BUSY. Explicit unlock failure retains
the exact descriptor after verified close. Both suppress the report and return
CloseUncertain/exit 1; this intentional rare resource retention can hold a
long-lived embedding's ownership until process teardown. Destructor invocation
or OS cleanup is never a positive close witness. No Store-global latch or public
fault seam is added.

The CLI writes complete safe JSON only after retirement/unlock, then one newline
and flush. Refusals emit only their fixed static error code and no JSON. Existing
StorageOwned remains exit 1; usage/storage-version/limit/platform preconditions
use exit 2 and corrupt/infrastructure/retirement faults exit 1. Output write/flush
failure uses b4run_inspection_output_failed/exit 1 after reader retirement; safe
partial external output is possible, so atomic stdout delivery is not promised.
Exact codes and limits are in the [runbook](../stopped-readonly-inspection.md)
and [accepted design](../stopped-readonly-inspection-design.md).

Enable only rusqlite 0.40.2 bundled/limits/hooks on the existing SQLite dependency.
No package/dependency additions, lockfile/root-manifest changes, Store/schema/
backup/approval behavior or HTTP capability changes are selected. Historical
verification artifacts remain exact; fresh feature and platform results belong
in a new receipt. Full workspace regression is required because feature
unification changes compiled rusqlite elsewhere in the workspace.

Selected local tests and root SPEC review have verified actual readonly locking,
source preservation, bounded schema/snapshot checks, projection and actual
SQLite TOOBIG/progress interruption. Actual source and expected-schema close-BUSY
tests forget real statements only under cfg(test), proving locked resource
retention while the child remains alive. Positive close/unlock and inherited
descriptor tests permit a writer to reopen while the reader child still lives.
The pre-unlock retention test uses a real FD after verified close; it is branch
evidence, not an actual OS unlock fault.

The current macOS filesystem rejected rawFF filename creation with filesystem errors before product inspection.
Actual metacharacter/non-ASCII inspection and private raw-byte URI encoding
passed there. Linux has a required actual non-UTF-8 source fixture; fresh Linux
qualification passed against the exact current source. This does not promise every filesystem accepts
all Unix byte names.

The decision adds narrow native status/blocked/version inspection. Stable
diagnostic identities, complete semantic authority/history auditing, live
inspection, automatic scanning/recovery, production restore installation,
TypeScript CLI parity, complete graceful shutdown and broader M0/CP/operations/
core release qualification remain open. No deployment or publication is selected.
