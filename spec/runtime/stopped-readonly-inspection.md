# Stopped readonly inspection

Status: scoped implementation accepted after independent SPEC and QUALITY reviews.
Fresh macOS/Linux arm64 checks pass; independent evidence freeze is in progress.
Each platform passes769 Rust entries and the12 existing plus19 inspection release
cases; the3 historical ignored helpers remain. The73 new Rust entries include one
reader child-protocol helper. Python71 and Node50 retained/developer checks pass. The earlier native launcher and
Linux container receipts remain historical evidence for their own source. They do
not qualify this new reader. This runbook does not close the full operations or
core release gates.

`b4run inspect` reads an existing clean, stopped native SQLite directory and emits
one bounded redacted JSON report. `b4run inspect --blocked` adds report-local rows
for recorded unfinished or uncertain executions. Inspection neither starts the
application nor authorizes recovery, replay, adoption, repair or source reuse.
The report describes stored records and the reader's validated scope; it is not
a live readiness or capability response.

## Commands and configuration

Use a built `b4run` executable and an existing private source directory:

```sh
B4RUN_DATA_DIR='/absolute/path/to/existing/private-data' b4run inspect
B4RUN_DATA_DIR='/absolute/path/to/existing/private-data' b4run inspect --blocked
```

Without `B4RUN_DATA_DIR`, the source is the existing `.b4run-data` directory
relative to the current working directory:

```sh
b4run inspect
b4run inspect --blocked
```

Those are the complete inspection forms. A positional path, duplicate flag,
`--demo`, `--force`, `--live` and SQL or URI arguments are refused. Only
`B4RUN_DATA_DIR` is consulted. Inspection ignores `B4RUN_TOKEN`, `B4RUN_PORT` and
`B4RUN_SHUTDOWN_WAIT_MS`, needs no demo selection or bearer token, and constructs
no application, provider, callback, HTTP listener, Tokio runtime or signal
receiver. `b4run --version` and `b4run --help` still read no environment.

The CLI preserves Unix path bytes rather than requiring UTF-8. It rejects an
empty configured path, NUL or a configured path longer than 4096 bytes. The
reader also bounds resolved absolute paths before opening SQLite. It creates
the percent-encoded `file:` URI internally with fixed `mode=ro&immutable=1`;
the operator supplies a filesystem path, never a caller-authored SQLite URI.

## Source and ownership conditions

Before inspection, establish that all known writers have positively retired.
For the native launcher, this means its actual owned-worker and transport
retirement contract. For the existing container profile, actual clean
exit 0, stopped and not OOM evidence is required. A timeout, force-kill or merely absent
sidecars does not establish clean runtime or effect retirement.

The source must already contain:

| Entry | Required state |
|---|---|
| Data directory | Existing real nonsymlink directory, exact Unix mode 0700 |
| `runtime.sqlite3` | Existing nonempty regular nonsymlink file, exact mode 0600 |
| `owner.lock` | Existing regular nonsymlink file, exact mode 0600; it may be empty |

The reader does not create, truncate, chmod, unlink, checkpoint, migrate, recover
or repair any source entry. Missing or unsuitable sources refuse unchanged.
Successful and refused reads preserve source bytes, owner epoch, directory
entries, devices/inodes, modes, sizes and modification times. Reading may change
access times; atime preservation is not promised.

The parent path must remain trusted and stable. Cooperating runtime writers and
inspectors must lock the same existing `owner.lock` inode. Inspection opens that
lock read-only, verifies the descriptor against the source witness and takes an
actual nonblocking exclusive OS flock. An existing owner causes an immediate
static refusal; inspection does not wait or retry. The lock excludes cooperating
owners, not a raw SQLite writer or filesystem administrator that bypasses it.
Hostile ancestor replacement, network filesystems, UID/ACL ownership auditing
and Windows are outside this profile.

Under the acquired lock, any entry named `runtime.sqlite3-wal`,
`runtime.sqlite3-shm` or `runtime.sqlite3-journal` refuses inspection. This includes
an empty file, nonempty file, broken symlink or directory. Sidecars are checked
before opening SQLite and again around close. Do not delete or rewrite a
sidecar to make inspection succeed. A clean closed WAL-header main database with
no sidecars is accepted; sidecar absence cannot prove that historical WAL was
never lost or establish crash-recovery integrity.

## What the report validates

The report contract is [stopped-inspection-report.schema.json](stopped-inspection-report.schema.json).
Its fixed scope is `schema_structure_and_execution_snapshots`.

Before integrity or data queries, bounded schema enumeration must exactly match
the reader's in-memory reconstruction of schema-v1 plus migrations-v2/v3. The
source is never migrated. Unexpected tables, changed DDL, views, triggers and
virtual tables are refused. Metadata must be exactly one id 1 row with store
schema 3, the current schema checksum and an eight-byte nonzero owner epoch.
The reader then performs budgeted `quick_check(1)` and foreign-key checks.
`quick_check(1)` bounds diagnostics, not scan work, and is not a complete
UNIQUE/index or authenticity audit.

The six structurally checked and counted tables are `metadata`, `threads`,
`executions`, `journal`, `approval_grants` and `approval_acceptances`. Each bounded
execution snapshot must use snapshot version 1 and pass kernel invariants. The
execution's indexed identity, revision and owner must agree with the snapshot;
its owner must be nonzero and no greater than the metadata epoch. Its thread
must exist. Every non-null active execution pointer must name an execution in
that same thread.

Thread metadata/origin JSON and journal/grant/acceptance payload semantics are
deferred. Such payloads may be malformed while this narrower report succeeds.
Success therefore does not mean whole-store-valid, complete approval-authority
health, tamper authenticity, qualified effect safety or permission to recover.
The full source remains sensitive even though this report is redacted.

## Report fields and interpretation

Every report contains `inspection_report_version:1`, `mode:"stopped_readonly"`,
`redacted:true`, the fixed validation scope, six table row counters, all eight
execution-status counters, all eight initialized-task-status counters, four
declared-definition-policy counters and four recorded-uncertainty-policy counters.
Counters include all execution rows, including terminal history, rather than
only the thread's current active pointer.

The `reader` object contains:

| Field | Value |
|---|---|
| `package_version` | `0.1.0` |
| `sqlite_library_version` | `3.53.2` |
| `snapshot_version` | `1` |
| `store_schema_version` | `3` |
| `target_profile` | `b4-ap-core/0` |
| `profile_status` | `native_subset` |
| `capability_descriptor_version` | `2` |
| `core_profile_frozen` | `false` |

These values identify the reader, validated format and target metadata. They do
not identify the executable that originally wrote the database and do not claim
a live host capability or a frozen core profile.

`initialized_tasks_by_status` counts persisted `Snapshot.tasks`: `ready`,
`waiting_for_approval`, `in_flight`, `succeeded`, `failed`, `denied`, `cancelled`
and `outcome_unknown`. `declared_definition_tasks_by_recovery_policy` counts
the complete persisted Definition across every batch, including not-yet-initialized
tasks. An Accepted execution can therefore have definition-policy counts with
zero initialized tasks. The four policy keys are `pure`, `idempotent`,
`reconcilable` and `unsafe`; they are declarations, not proof of safe replay.
`recorded_uncertain_tasks_by_policy` counts initialized InFlight and OutcomeUnknown
records under their validated declared policy. It cannot establish whether an
external effect occurred or a callback is currently running.

By default, `blocked` is null. With `--blocked`, it is an object whose `selection`
is `recorded_unfinished_or_uncertain` and whose `rows` contains every selected row
in execution primary-key order. Each row has a newly assigned ordinal 1..N, stored
status, `recorded_recovery_blocked`, `recorded_approval_park`, `has_uncertain_task`,
initialized task-status counts and complete Definition-policy counts. Ordinals
are local to this report. They are neither stable identities nor recovery targets
or a correlation promise.

| Stored execution status | Interpretation in selection |
|---|---|
| `accepted` | Recorded unfinished; initialized tasks may be absent |
| `running` | Recorded unfinished, including a valid application-failure drain |
| `waiting_for_approval` | Recorded stable approval park |
| `cancel_requested` | Recorded unfinished cancellation drain |
| `recovery_blocked` | Recorded kernel state; no private failure reason is disclosed |
| `completed`, `failed`, `cancelled` | Counted as terminal history; current kernel-valid terminal states contain no active uncertainty |

Any initialized InFlight or OutcomeUnknown record also meets the selection.
A Failed snapshot with active/uncertain effects contradicts kernel invariants
and is refused as corrupt. A valid application-failure drain remains Running,
or RecoveryBlocked when uncertainty is recorded. Inclusion does not imply that
every Accepted or Running execution needs replay.

The report includes no source path/hash, epoch, timestamps, execution/thread/task/
effect/turn/interrupt/grant/acceptance identity, generation, route or definition/
policy revision. It includes no values, inputs, outputs, writes, prompts,
decisions, tokens, failure text, source snippets or authority-derived digest.

## Fixed ceilings

There are no operator budget overrides. A ceiling refuses the entire requested
report without truncation. More than 64 selected rows can refuse `--blocked`
while aggregate inspection remains available within its other ceilings.

| Boundary | Ceiling |
|---|---|
| Raw input and resolved absolute paths |4096 bytes; NUL forbidden |
| Main database file |64 MiB |
| Execution rows |256; fetch at most 257 |
| Snapshot UTF-8 bytes |1 MiB each;16 MiB cumulative |
| Source schema objects |32; fetch at most 33 and require exact expected schema |
| One schema DDL string |64 KiB; an engine limit may refuse earlier |
| JSON structural depth |64 using a fixed stack before owned Snapshot decoding |
| JSON lexical nodes per snapshot |16384; all strings including keys, container starts and primitive tokens count |
| Persisted definition/kernel shape |Existing 1024 batches/4096 tasks; other inspection limits still apply |
| Selected blocked rows |64 |
| Complete encoded JSON |65536 bytes, excluding one CLI newline |
| SQLite LENGTH |2 MiB; applies to a full row as well as scalars |
| SQLite SQL_LENGTH |16 KiB |
| SQLite COLUMN / EXPR_DEPTH / PARSER_DEPTH |64 /64 /128 |
| SQLite ATTACHED / worker threads |0 /0 |
| Cooperative progress |Approximately 1000 VM operations per hook; interrupt by the 2000th callback |
| Cooperative elapsed budget |5 seconds cumulative across source and expected-schema SQL plus bounded CPU phases |

SQLite limits, defensive mode and untrusted-schema mode are installed and read
back before the first prepared statement. Fixed guarded source PRAGMAs enforce
query-only mode, memory temp storage and a small connection-local cache. Only
fixed SQL is used. JSON shape is checked before decoding one owned Snapshot,
which is dropped before advancing to the next execution. The cooperative budget
is shared by source and expected-schema connections; only a positive own-exhaustion
flag classifies an interruption as the inspection limit.

Five seconds is a cooperative elapsed check, not a hard execution deadline.
Filesystem calls, SQLite close and output I/O may block. The independent source,
row, value, shape and output ceilings do not promise a hard total RSS bound or
sandbox arbitrary hostile database images.

## Completion, output and static refusals

The reader owns the exact source connection, its temporary expected-schema
connection when present and the existing owner-lock descriptor under one guard.
Normal success and ordinary refusals explicitly close SQLite before unlocking.
Final source/sidecar witness checks run while ownership remains held. A report
can return only after successful close, valid final witnesses and successful
explicit unlock; an inherited duplicate file description does not keep normal
ownership alive after that unlock. Unwinding follows the same close-before-unlock
ordering.

A rare actual close failure returns the live connection. The guard intentionally
quarantines that exact connection, any other still-owned inspection connection
and the exact owner-lock descriptor together until process teardown. This covers
both source and expected-schema close uncertainty. An explicit unlock failure
likewise retains the exact descriptor after verified close. Both suppress the
report and return `b4run_inspection_close_uncertain`. A long-lived embedding may
therefore retain ownership until its process ends. Dropping a handle or OS process
cleanup is not a positive library close witness. No Store-global latch or public
fault seam is added.

The CLI writes the completed encoded report only after reader retirement and
lock release, then adds one newline and flushes. Successful delivery exits 0.
Inspection refusal emits no JSON and one static stderr code. An output I/O
failure exits 1 after reader retirement; a partial external write of already-safe
report bytes is possible. Output delivery is not atomic.

| Static stderr code | Exit | Meaning |
|---|---:|---|
| `b4run_invalid_arguments` |2| Unsupported inspection grammar |
| `b4run_inspection_invalid_source` |2| Invalid path/configuration, missing entry, unsuitable type/mode or changed source witness |
| `b4run_storage_owned` |1| Another cooperating owner holds the existing lock |
| `b4run_inspection_sidecars_present` |2| Any WAL/SHM/journal entry is present |
| `b4run_inspection_unsupported_schema` |2| Unsupported version/checksum or unexpected schema structure |
| `b4run_inspection_unsupported_snapshot` |2| Unsupported snapshot version |
| `b4run_inspection_limit_exceeded` |2| Source, row, value, shape, output or cooperative work ceiling exceeded |
| `b4run_inspection_corrupt_source` |1| Malformed database, metadata, snapshot, index or thread relationship |
| `b4run_inspection_unavailable` |1| Infrastructure/read/configuration-control failure |
| `b4run_inspection_close_uncertain` |1| Physical close or explicit unlock uncertainty; report suppressed and resources retained |
| `b4run_unsupported_platform` |2| Platform outside macOS/Linux; reader accesses no source |
| `b4run_inspection_output_failed` |1| Safe report could not be fully written/flushed after reader retirement |

Errors expose no path, nested SQLite error, SQL, source version value or payload.
Existing storage-owned exit 1 is preserved.

## Evidence boundary

The frozen local reader tests cover source preservation, exact schema/snapshot
checks, all status/policy projections, ceilings, actual SQLite TOOBIG and
cooperative interruption, actual source and expected-schema close-BUSY retention,
and alive-child writer exclusion/reacquisition with a duplicate lock description.
The private pre-unlock test retains a real descriptor after verified close; it
is exceptional-branch evidence, not an actual operating-system unlock-fault claim.

The current macOS filesystem refused creation of a filename containing rawFF
bytes with filesystem errors before product inspection. Actual metacharacter/non-ASCII source inspection and private
raw-byte URI encoding passed there. The Linux test requires a real raw non-UTF-8
filename and successful inspection; this passed in the fresh nonroot Linux qualification. This distinction is not a promise that every filesystem accepts every
Unix byte name.

Fresh complete platform, release-binary and contract checks belong to the
[separate inspection receipt](../../conformance/expected/stopped-readonly-inspection-verification.json). Live inspection, detailed stable identities, whole-store
authority/history auditing, automatic recovery scanning, TypeScript CLI parity,
complete graceful shutdown and broader M0/CP/operations/core release gates remain
open. See [the accepted design](stopped-readonly-inspection-design.md) and
[ADR0020](decisions/0020-stopped-readonly-inspection.md).
