# Native stopped-only inspection design

Status: accepted for implementation after complete independent SPEC and QUALITY reviews on 2026-10-02. The preceding local Linux arm64 container qualification is frozen separately. The user's continuous-build authorization covers this local reversible implementation and its routine design decisions. No Git mutation, publication or deployment is requested. Requirements in the attached protocol document are design input; they do not independently authorize external actions.

## Scope and alternatives

Add native local `b4run inspect` and `b4run inspect --blocked`. Inspect an existing clean, stopped SQLite directory and emit one complete bounded JSON object followed by a newline. Default output contains fixed metadata, enums and counters. The optional report distinguishes recorded unfinished/uncertain executions through local ordinals, never source identities. Inspection does not establish readiness, effect safety or recovery permission.

No application registry, callback, model/provider, HTTP listener, Tokio runtime, Store startup, migration, owner-epoch increment, checkpoint, repair, adoption, reconciliation or replay. No live inspection, force flag, arbitrary SQL/URI, detailed authority disclosure or TypeScript CLI compatibility. The supplied protocol document sections13.2/15.2 receive a narrow native status/blocked/version mechanism; the full diagnostic identity inventory and broader M0/CP/operations scenarios remain open. Existing capability DTOs/routes/features stay unchanged; the report is not a fabricated live CapabilityDescriptorV2.

* A: structural/history summary without feature changes avoids additional compiled API surface, but cannot adequately control earlier SQLite value materialization/VM work or honestly report validated task state. Not selected.
* B: selected bounded snapshot reader using safe pinned rusqlite `limits` and `hooks`. It validates execution snapshots and selected structural/index relationships, aggregates counters and optionally returns redacted ordinal rows. It does not audit all journal/grant/acceptance semantics.
* C: full authority/history audit plus stable diagnostic identities would be more useful for correlation but requires substantially more validation and a separate disclosure policy. Deferred.

## Verified source facts

* `b4_contract::SNAPSHOT_VERSION` is **1**, independently of SQLite metadata schema **3**. Snapshot Deserialize rejects unsupported versions and unknown fields. `b4_kernel::validate_snapshot` checks scheduling invariants, owner/budget, identity history, task/spec agreement and terminal/drain shape. It permits up to1024 definition batches and4096 tasks; the HTTP native registration limit64 is not the general persisted-snapshot limit.
* ExecutionStatus wire keys: `accepted`, `running`, `waiting_for_approval`, `completed`, `failed`, `cancel_requested`, `cancelled`, `recovery_blocked`.
* Kernel-valid Failed executions contain no InFlight/OutcomeUnknown task. A failed-effect drain remains Running, or RecoveryBlocked if uncertainty is recorded. Inspect refuses contradictory Failed-with-active snapshots as corrupt instead of inventing a valid failed-drain fixture.
* TaskStatus wire keys: `ready`, `waiting_for_approval`, `in_flight`, `succeeded`, `failed`, `denied`, `cancelled`, `outcome_unknown`.
* RecoveryPolicy wire keys: `pure`, `idempotent`, `reconcilable`, `unsafe`. They are declarations, not proven safe replay classifications.
* Current package/runtime version is0.1.0. Current capability target is`b4-ap-core/0`, status`native_subset`, descriptor version2 and core_profile_frozen=false. There is no independent contract semantic-version constant. Report those as reader/target metadata, never the version that originally wrote the database or live host readiness/capabilities.
* CLI success is exit0; usage/config/storage/unsupported-platform preconditions are exit2; infrastructure/faults are exit1. **Existing StorageOwned is exit1**, so an inspect owner refusal should stay exit1 unless an explicitly authorized future change selects a new convention. Version/help read no environment. Current config-check requires demo/token; inspect requires a separate parser/config branch.
* Production b4-sqlite already depends on exact rusqlite0.40.2 (`bundled`), fs2, sha2, serde_json and b4-kernel. b4-cli has production serde_json but rusqlite only as a dev dependency. Cached rusqlite Cargo features `limits=[]` and `hooks=[]` introduce no package/dependency edges by themselves. Enabling only these on the b4-sqlite dependency is selected; do not enable `modern-full`, `functions`, `vtab`, `load_extension` or default features. Verify the actual locked graph/package versions/checksums remain exact before claiming this; feature unification will affect compiled rusqlite in other workspace targets, so full fresh regression is required.
* Safe APIs are `Connection::set_limit`/`limit` (Result), `progress_handler` (Result, FnMut()->bool + Send + 'static), `set_db_config`, `is_readonly(MAIN_DB)`, `open_with_flags`, and `close`. Hook work must be pure, nonpanicking budget arithmetic/clock checking. Cached rusqlite catches hook panic and returns false, so panic cannot be an exhaustion signal.
* `Connection::close(self)` returns `Err((Connection, Error))` while retaining the live connection. InnerConnection Drop calls close but discards its result. A destructor call after failed close is therefore not a positive physical-retirement witness.

## API and files

Selected new b4-sqlite public surface:

```rust
pub struct InspectionOptions { pub blocked: bool }
pub struct InspectionReport { /* private bounded encoded JSON only */ }
impl AsRef<[u8]> for InspectionReport { /* complete JSON without newline */ }
pub enum InspectionError {
    InvalidSource, AlreadyOwned, SidecarsPresent, UnsupportedSchema,
    UnsupportedSnapshot, LimitExceeded, CorruptSource, Unavailable,
    CloseUncertain, UnsupportedPlatform,
}
pub fn inspect_stopped(
    data_dir: impl AsRef<std::path::Path>, options: InspectionOptions,
) -> Result<InspectionReport, InspectionError>;
```

All error Display/Debug/code text is static and carries no nested error/path/version/id/SQL/payload. Options contain no authority, budget overrides or custom callbacks. The report contains only encoded safe DTO data; it cannot retain a Connection, File, snapshot, grant, callback or store. Avoid a new serde dependency solely for DTO derives: existing serde_json can build the fixed report privately. The fixed report shape is authored in spec/runtime/stopped-inspection-report.schema.json. It is an operator-report contract, outside the AP HTTP schema inventory.

Keep modules small: `crates/b4-sqlite/src/inspection/mod.rs` orchestration/public surface; `source.rs` Unix path/metadata/read-only lock/URI/reader retirement; `validation.rs` fixed bounded SQL/schema/index/snapshot reading and budget; `report.rs` safe counters/ordinal projection and capped JSON. Export narrowly from lib.rs. Add only `limits`,`hooks` to the existing b4-sqlite manifest. Reuse private schema constants/checksum/expected-schema reconstruction; do not reuse unbounded source `schema_objects` or eager unbounded `read_execution`, and do not alter production SqliteStore startup/read/write/drop logic.

CLI: new private `inspection.rs`; parser `Command::Inspect(InspectConfig {data_dir, blocked})` in config.rs; thin run branch in lib.rs and updated help. Only B4RUN_DATA_DIR is consulted (default existing`.b4run-data`); bearer token, port and shutdown interval are ignored. Preserve --version/--help environment-free behavior. Initially no positional path, --demo, --force, --live or generic SQL. Support Unix path bytes in this branch through OsString/PathBuf rather than silently routing through serve's UTF8 string parser; enforce a4096-byte path cap and reject NUL. This does not change existing serve/config-check behavior.

The successful branch writes the completed report after the reader has retired and released the lock. Handle output write failure without panic and return static infrastructure exit1; a broken/stalled output sink can cause a partial external write, so promise no partial JSON for *inspection refusal*, not atomic stdout delivery under I/O failure. The existing serving emit_status intentionally ignores pipe errors; inspect should use a separate fallible output helper rather than silently reporting successful inspection delivery. No sensitive error details are printed.

## Source/ownership protocol

1. Resolve a filesystem directory under the existing trusted stable local-parent covenant. Use symlink_metadata: existing real0700 directory; existing real0600 runtime.sqlite3 and owner.lock; nonzero database size. Do not create/chmod/unlink/repair/truncate/reserve. Before SQLite open, cap main-file bytes. Retain dev/inode/mode/size/mtime witnesses; access times are excluded from preservation.
2. Open the **existing** owner.lock read-only, without create/truncate; compare opened descriptor metadata with the preflight inode and take nonblocking exclusive fs2 flock. Actual macOS/Linux read-only-descriptor lock behavior must be proven. Refuse live writer/other inspector; do not wait/retry lock acquisition. Revalidate source entries under the acquired lock. Never recreate/unlink the lock, and never call SqliteStore::open. Exclusive flock protects only cooperating writers on the same lock inode.
3. Under ownership reject *any* `runtime.sqlite3-wal`, `-shm` or `-journal` entry, including empty files, broken symlinks and directories. Repeat before/after SQLite close. No sidecar cleanup/checkpoint. A clean closed WAL-header database without sidecars is accepted. Sidecar absence is not proof that no historical WAL was lost; report no crash-recovery integrity or tamper-authenticity claim.
4. Build an absolute ASCII `file:` URI internally from raw Unix path bytes, percent-encoding metacharacters/nonASCII bytes so no query/fragment/authority injection occurs. Fixed `mode=ro&immutable=1`; explicit READ_ONLY|URI|NOFOLLOW|PRIVATE_CACHE and no READ_WRITE/CREATE. Check is_readonly(MAIN_DB). Never accept a caller URI. Immutable skips SQLite locking/change detection, so the same-lock cooperating-writer and trusted stable-path assumptions are mandatory; outside SQLite writers/admin path replacement/network filesystems are excluded.
5. Install safe API limits/progress hook immediately after connection creation **before first prepared statement**. The following fixed PRAGMA configuration/readback statements are the first guarded prepares: query_only=ON, temp_store=MEMORY and a small connection-local cache. Set SQLITE_DBCONFIG_DEFENSIVE=true and SQLITE_DBCONFIG_TRUSTED_SCHEMA=false through the safe pinned API and verify readback before the first prepare. Apply the parser-depth limit at the same boundary; source schema loading occurs before enumeration. These controls do not establish safety for arbitrary hostile database images. Register/invoke no user-defined SQLite functions or extensions; accept no source virtual table. The bundled SQLite engine may contain such facilities. SQLite source/temp writes remain forbidden; engine/statement controls are connection-local, never registered on SqliteStore.
6. Bound and validate using only fixed SQL. Keep every row/statement in inner lexical scopes; consume/drop the one bounded Snapshot after projection before advancing. Do not retain authority in the output or any result carrier. Serialize only the safe report under its cap before success retirement so an allocation/format refusal still releases safely.
7. Explicitly close SQLite after all statements/rows are gone. Revalidate source/sidecar witnesses while the existing owner lock remains held. Only a successful close and final witness check permit normal unlock and report return. Normal errors also close before unlocking. Guard field order plus an explicit finish path must cover early returns/unwind, not just success.
8. **Close failure exceptional quarantine, selected by root:** if close returns the live connection, retain that exact connection, any still-owned source/expected-schema connection and OwnerLock together in a private process-lifetime quarantine using safe ownership (`Box::leak` of the inseparable guard is the minimal path). Return CloseUncertain/exit1, stdout empty. Do not expose an opaque close-error handle, format the SQLite error, release the lock or call drop and claim physical close. OS process teardown eventually releases resources; that is not library positive-retirement proof. A long-lived embedding can retain one failed reader/source until process end, so this is an intentional narrow resource leak, not a general destructor-unlock guarantee. No source mutation or SqliteStore-global latch is introduced. This exceptional resource-retention tradeoff is selected for this increment. An explicit unlock failure likewise retains the exact lock descriptor, suppresses the report and returns CloseUncertain. A separate guard handles all normal errors and unwinding; it never relies on the existing OwnerLock destructor ignoring unlock errors.

No hard wall-clock guarantee: filesystem calls/SQLite close/stdout may block; cooperative SQL time checks cannot cancel them. No hard total RSS guarantee: SQLite/schema/deserializer allocations have overhead within multiple independent byte/shape caps. This is bounded source/row/value/output/cooperative-work inspection of trusted local files, not a sandbox for adversarial DB images.

## Validation and fixed initial ceilings

Use fixed conservative ceilings, not promises about maximum valid runtime-store size. Larger sources refuse without truncation/partial report; overrides are deferred.

| Boundary | Proposed fixed ceiling |
|---|---:|
| Main database file |64 MiB|
| Execution rows |256 (fetch LIMIT257)|
| Snapshot UTF8 bytes |1 MiB each;16 MiB cumulative|
| Source schema objects |32 (fetch at most33), exact expected schema required|
| One DDL string |64 KiB (engine SQL-length may refuse earlier)|
| Parsed JSON |depth64;16384 lexical nodes per snapshot (all string tokens, including object keys, all container starts and all primitive value tokens count); structural preflight before owned Snapshot decode|
| Persisted definition/tasks |existing kernel1024 batches/4096 tasks; no artificial HTTP64 assumption|
| Opaque blocked rows |64; overflow refuses --blocked, aggregate-only remains available|
| Serialized JSON |64 KiB, excluding one CLI newline|
| SQLite LENGTH |2 MiB (also bounds a full row, not only snapshot scalar)|
| SQLite SQL_LENGTH |16 KiB|
| SQLite COLUMN / EXPR_DEPTH / PARSER_DEPTH |64 /64 /128, verified against exact schema and fixed queries|
| SQLite ATTACHED / worker threads |0 /0|
| Cooperative progress |every approximately1000 VM operations; at most2000 hook callbacks plus5s elapsed checks|

Verify each set_limit result through limit readback; approximate progress intervals must not be labeled an exact instruction bound. Use checked/saturating pure hook accounting and a positive exhaustion flag to classify own interruption as LimitExceeded. Unexplained SQLITE_INTERRUPT is an infrastructure/read failure, not silently forgiven. Maintain one cumulative progress/deadline budget across schema load/checks/queries and inspect's other bounded CPU phases; check between phases. Size limits apply before open and before Rust copies; do not use `COUNT(*) LIMIT...` as a row-work bound.

Before any integrity or data query, use bounded source schema enumeration and compare to the fixed expected in-memory schema-v1+migration-v2+v3 reconstruction. Validate metadata exactly one id1 row, schema3, checksum(SCHEMA), eight-byte nonzero epoch. Run quick_check(1) and foreign_key_check within the same budget; quick_check limits diagnostics, not scan work and is not complete UNIQUE/index verification. Cap scan/work via source size + hook, suppress raw errors. No source DDL/view/trigger/virtual-table additions are accepted.

Enumerate executions in deterministic primary-key order with LIMIT257; validate row id/thread/revision/owner, eight-byte integers, nonzero execution owner <= current metadata epoch, Snapshot version1 and kernel invariants. Read snapshot as a borrowed text/value reference after SQLite length guard, check per/cumulative byte limits, run a nonallocating/shape-bounded JSON preflight, then decode one Snapshot. Validate execution thread exists; any threads.active_execution_id points to an execution in that same thread; no missing/foreign active references. Exact schema/FK checks apply to all six tables structurally. Thread metadata/origin JSON and journal/grant/acceptance payload semantic auditing are deferred; do not report whole-store-valid or complete authority health.

Projection counts all execution rows, including historical terminal rows; it is not only the active-thread pointer. `initialized_tasks_by_status` counts Snapshot.tasks. `declared_definition_tasks_by_recovery_policy` counts full Definition task specs, including not-yet-initialized tasks. `recorded_uncertain_tasks_by_policy` counts initialized InFlight/OutcomeUnknown records under their validated declared spec, without claiming a currently active callback or qualified replay. WaitingForApproval is a recorded stable park; RecoveryBlocked is a recorded kernel state, not an inferred reason from a private failure string.

For --blocked, include rows whose stored execution status is Accepted/Running/WaitingForApproval/CancelRequested/RecoveryBlocked, or which contain InFlight/OutcomeUnknown task records (a valid failed-effect drain remains Running until its active effects settle). Label selection `recorded_unfinished_or_uncertain`. Every row has a newly assigned report-local ordinal1..N, stored execution status, three fixed booleans (`recorded_recovery_blocked`, `recorded_approval_park`, `has_uncertain_task`), initialized task-status counts and declared-definition-policy counts. No ordinal is a stable ID, correlation promise or recoverable target. Do not claim all nonterminal statuses currently require replay; Accepted/Running Ready are included as recorded unfinished state. Refuse on row/report limits, never silently truncate.

## Output/error contract

Fixed top-level fields (exact names below): inspection_report_version1, mode`stopped_readonly`, redacted=true; reader/version metadata (reader package0.1.0, bundled SQLite library3.53.2, snapshot version1, observed store schema3, target profile b4-ap-core/0/native_subset/current descriptor2/core_profile_frozen=false); validation_scope`schema_structure_and_execution_snapshots`; fixed table row counters, execution status counters, initialized task-status counters, declared definition-policy counters, recorded uncertain-task-policy counters; `blocked` null by default or a selection/rows object containing the complete bounded ordinal array under --blocked. The SQLite/library version describes the reader, not the source's creator. No fake capability object, health or readiness status.

Absent in every mode: database path/hash/owner epoch; execution/thread/generation/turn/task/effect/interrupt/grant/acceptance IDs; route/application/policy/definition revisions; timestamps; values, inputs, outputs, writes, prompts, decisions, tokens, JSON snippets, failure strings or stable authority-derived digest. Local operator possession of the private directory is the intended authorization; there is no bearer-token bypass into network APIs.

| Error | Static code | CLI exit |
|---|---|---:|
| InvalidSource |b4run_inspection_invalid_source|2|
| AlreadyOwned |b4run_storage_owned|1 (preserves current convention)|
| SidecarsPresent |b4run_inspection_sidecars_present|2|
| UnsupportedSchema |b4run_inspection_unsupported_schema|2|
| UnsupportedSnapshot |b4run_inspection_unsupported_snapshot|2|
| LimitExceeded |b4run_inspection_limit_exceeded|2|
| CorruptSource |b4run_inspection_corrupt_source|1|
| Unavailable |b4run_inspection_unavailable|1|
| CloseUncertain |b4run_inspection_close_uncertain|1|
| UnsupportedPlatform |b4run_unsupported_platform|2|

InvalidArguments retains current b4run_invalid_arguments/2. Inspection refusal emits no report, exactly one static error code and no raw diagnostic. Output I/O failure is static exit1 after reader retirement; some already-written safe report bytes can remain externally. Error classes are selected for implementation, rather than claims of existing behavior. The exact report shape and ceiling values are frozen below before implementation. Output I/O failure is b4run_inspection_output_failed/exit1.

## Implementation ownership

1. **Root design/model owner:** publish design/plan after the Linux receipt freezes; own exact DTO/limits/error protocol, new named requirements and authored JSON schema. Own root docs/traceability/evidence, feature-only dependency graph audit and final platform/full/release checks. No broad T047/T055/M0/core/operations closure.
2. **SQLite reader author:** own new inspection directory, narrow lib exports and b4-sqlite Cargo feature line; new tests/inspection.rs + inspection_support module. TDD source preflight, actual read-only flock exclusion, schema/snapshot/index integrity, caps, pure output, explicit successful close and exceptional inseparable quarantine. Never edit schema.sql/Store implementations/approval APIs/backup code.
3. **CLI author:** own config.rs/lib.rs/new inspection.rs and private parser/output unit tests. Independent process qualification owns the separate new tests/inspection_cli.rs/helpers exclusively. TDD exact argument/environment isolation, stdout/exit semantics, version metadata/redaction, real shipped subprocess preservation and alive-child lock proof. No process/HTTP/demo/launcher fixture change.
4. Independent SPEC then QUALITY review for reader and CLI. Real physical/SQLite tests follow source interfaces, no synthetic lock flags or mocked close-result-only proof. Full macOS/Linux fmt/clippy/workspace and repeated release CLI suites plus new inspect cases; frozen historical receipts remain exact. New receipt records actual feature/code/hash/count changes and limitations.

## Meaningful behavioral test matrix

* RED before implementation: actual native `inspect` command refuses expected complete report; library executable refusal stub fails a genuine stopped-store report assertion. Setup errors/permission failures do not count as feature RED.
* Build real clean Store-generated schema3 datasets with each supported execution status and each initialized task status/policy; Accepted has no initialized tasks, Running failure drains/CancelRequested and unstarted RecoveryBlocked keep their actual kernel-valid shapes. Validate full-definition versus initialized-task counter distinction. --blocked ordinals reveal only selected fixed status/counters; cap64 and cap+1 are meaningful, no invented opaque-ID identity guarantee.
* Put distinct secrets in all snapshot data/failure/interrupt and thread/journal/grant/acceptance material. Successful summary/blocked stdout and all static refusals exclude raw secrets and their digests/source IDs; unchanged before/after file bytes, epoch, modes, mtimes and directory entries. Do not promise atime preservation.
* Actual separate process owns writer lock: inspect refuses1, no SQLite open or mutation. Actual reader held before close: writer contender AlreadyOwned; after positive close normal unlock and reopen while reader child is still alive. Duplicate inherited lock description test confirms explicit unlock behavior. Every ordinary error path retires connection before lock release.
* Actual close-BUSY private test fixture forgets a real prepared statement only under cfg(test), calls actual Connection::close and proves exceptional connection+lock quarantine retains AlreadyOwned while child remains alive; no report. OS child termination is cleanup, never successful-close proof. No production forget-statement seam, unsafe callback or public connection handle.
* A cfg(test)-only pre-unlock refusal seam retains a real locked descriptor and proves physical writer exclusion after verified reader close. It tests the exceptional retention branch, not a genuine operating-system unlock fault. Production explicit unlock failures use that same quarantine path.
* Missing directory/DB/lock never creates files; empty/invalid/truncated DB; wrong modes, symlink roots/files, URI metacharacters/odd nonUTF8 path bytes; any zero/nonzero/symlink/directory sidecar refuses unchanged. A clean no-sidecar WAL-header database is positive.
* Version1/2/future SQLite, wrong checksum/epoch/extra/missing/changed schema, injected views/triggers/virtual tables, future Snapshot1+1, malformed JSON, index/payload disagreement, thread missing/foreign active pointer all refuse statically without migration/authority disclosure. Structurally valid but malformed deferred grant JSON does not falsely become a whole-store semantic-validation claim.
* Source-size, row256/257, snapshot1MiB/cap+1, cumulative16MiB/cap+1, JSON depth/node and blocked64/65 boundaries. The exact accepted schema has fewer than32 objects: test its bounded enumerator with a smaller private N/N+1 ceiling and actual extra-object rejection, rather than claiming a valid32-object accepted source. Production still fetches at most33 and refuses above32. Prove maximum compact report width with64 rows/widest counters/longest enums fits64KiB. Test serializer exact-cap/cap-minus-one behavior under a smaller private ceiling; do not fabricate impossible source payloads to reach the production report cap. Fixtures must stay structurally valid where testing a resource limit. Actual oversized SQLite cells and real expensive fixed integrity/query work hit safe engine limits/own progress interruption; no pretend time budget inferred from elapsed tests. Close/lock ordering stays correct on limit paths.
* Shipped CLI ignores B4RUN_TOKEN/B4RUN_PORT/B4RUN_SHUTDOWN_WAIT_MS, needs no --demo, creates no HTTP socket/callback. --version/--help still environment-free. Closed stdout is nonpanicking exit1 after reader close; demonstrably backpressured stdout does not retain the owner once the reader has closed (an unread pipe alone does not prove backpressure). Safe partial output under I/O failure is not conflated with partial success on refusal.

## Frozen report and platform decisions

The qualified platform branch is macOS and Linux only; other targets return UnsupportedPlatform without source access. This does not broaden the existing Store platform claim. Input and resolved absolute Unix paths are each capped at4096 bytes and reject NUL; raw path bytes are preserved in internal URI encoding. Source preservation covers bytes, entries, epoch, inode, size, permissions and mtimes; reading may change atimes.

Path qualification preserves raw Unix bytes on both targets. This macOS filesystem refuses creating an invalid-UTF8 filename before the product can inspect it: the process test instead proves byte-preserving configuration reaches a static InvalidSource refusal without changing the original source. Actual existing nonUTF8-source success is qualified on Linux; UTF8 nonASCII and URI-metacharacter source success is qualified on both. Private URI encoding tests also cover raw invalid-UTF8 bytes. This is a filesystem qualification limit, not a parser UTF8 restriction.

The JSON preflight uses a fixed64-entry structural stack, quotes/escape state and a16384 lexical-node ceiling. Each string token (keys included), object/array start, number token and literal token counts once. Escaped punctuation inside strings does not count. It guards shape before decoding directly into Snapshot, without an intermediate owned Value. The full decoder validates syntax and version; no claim of source JSON canonicalization is made.

`blocked` is null for aggregate mode. With --blocked it is an object with exact fields `selection: "recorded_unfinished_or_uncertain"` and `rows`, containing all selected rows, ordered by source execution primary key and assigned report-local ordinals1..N. These ordinals are not persistent or recoverable identifiers. At most64 rows are allowed;65 refuses the entire blocked report while aggregate inspection remains available.

The exact top-level fields are:

* `inspection_report_version: 1`, `mode: "stopped_readonly"`, `redacted: true`.
* `reader`: `package_version: "0.1.0"`, `sqlite_library_version: "3.53.2"`, `snapshot_version: 1`, `store_schema_version: 3`, `target_profile: "b4-ap-core/0"`, `profile_status: "native_subset"`, `capability_descriptor_version: 2`, `core_profile_frozen: false`. Version values describe this qualified reader and its target; they do not identify the writer of the database.
* `validation_scope: "schema_structure_and_execution_snapshots"`.
* `table_rows`: fixed counters `metadata`, `threads`, `executions`, `journal`, `approval_grants`, `approval_acceptances`.
* `executions_by_status`: all eight fixed execution-status counters.
* `initialized_tasks_by_status`: all eight fixed task-status counters.
* `declared_definition_tasks_by_recovery_policy`: all four fixed declared-policy counters.
* `recorded_uncertain_tasks_by_policy`: all four fixed declared-policy counters for initialized InFlight/OutcomeUnknown tasks.
* `blocked` as above. Each row has only `ordinal`, `status`, `recorded_recovery_blocked`, `recorded_approval_park`, `has_uncertain_task`, `initialized_tasks_by_status`, `declared_definition_tasks_by_recovery_policy`.

All counters are nonnegative u64 checked for overflow. Every object rejects additional fields in the authored report schema. Compact deterministic serialization contains no borrowed source string; the complete encoded report is at most65536 bytes excluding the CLI newline. The schema version is this report's contract version, independent of SQLite and Snapshot versions.

Actual source or expected-schema close failure and explicit unlock failure all return CloseUncertain and retain the exact uncertain resources in a private process-lifetime quarantine. This is intentional resource retention for a rare physical-retirement uncertainty, not a claim that destructor calls or OS teardown prove successful close. Normal errors and unwind close before unlock. No global Store latch is added.

The exact implementation plan is docs/superpowers/plans/2026-10-02-stopped-readonly-inspection.md. Historic Linux/native receipts remain immutable; fresh feature evidence belongs in a new receipt. Full semantic approval/history auditing, live inspection, stable diagnostic identities, recovery/restore installation and broader release qualification remain open.
