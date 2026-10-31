# Stopped readonly inspection implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and
> superpowers:test-driven-development, followed by independent SPEC then QUALITY
> review. The user's continuous-build authorization covers local reversible work
> and routine decisions. No routine permission handoff, Git mutation, deployment
> or publication is requested. Work in the existing authorized workspace.

**Goal:** Ship native `b4run inspect` and `b4run inspect --blocked` that emit a bounded
redacted report from an existing clean, stopped store, preserving its recorded data.

**Architecture:** A separate synchronous b4-sqlite reader owns the existing read-only
lock descriptor through explicit SQLite close and verified unlock. Fixed bounded SQL,
engine controls and borrowed JSON shape checks precede Snapshot validation and a
pure report. A thin CLI branch reads only the data-directory setting and writes safe
JSON after physical reader retirement. Close/unlock uncertainty suppresses output
and retains uncertain resources until process termination.

**Tech Stack:** Rust1.93.1, existing exact rusqlite0.40.2/bundled SQLite3.53.2, fs2,
serde_json, b4-contract and b4-kernel. Enable only rusqlite limits/hooks on b4-sqlite;
no package/version/checksum addition. No production Store, schema or HTTP change.

Design: `spec/runtime/stopped-readonly-inspection-design.md`.
Exact JSON shape: `spec/runtime/stopped-inspection-report.schema.json`.
All eight execution/task enums and four declared policies are fixed there. Snapshot
version1 differs from SQLite schema3 and report version1. Report target metadata is
not a live capability object or database-writer identity. Qualified targets are
macOS/Linux only; unsupported platforms refuse before source access.

## Task1: Freeze history, design, API and dependency baseline (root)

**Files:** new design/schema above; this plan. Private baseline/protected captures
and dependency graph under `/private/tmp`, not public database-derived hashes.

- [x] Verify frozen Linux main receipt SHA and its final independent audit. Capture
  all492 current artifact hashes and209 protected historical artifacts before these
  public design files. No historical receipt/example/reference mutation.
- [x] Publish design/schema/plan and independently review the complete documents.
  Repair concrete findings, then mark design accepted for implementation.
- [x] Verify JSON schema with the local Draft202012Validator; calculate worst-case
  compact field width for64 rows/max u64 counters/longest enums. This width proof
  does not fabricate a valid database or replace actual64/65-row qualification.
- [x] Capture `cargo metadata --locked --offline --format-version 1` and
  `cargo tree --locked --offline -e features` before feature changes. Later compare
  exact packages/versions/checksums and deliberate feature-only changes.

## Task2: Existing source ownership and reader retirement (SQLite author)

**Exclusive files:** create `crates/b4-sqlite/src/inspection/mod.rs`, `source.rs`,
`validation.rs`, `report.rs` and focused private test modules as needed; create
`crates/b4-sqlite/tests/inspection.rs` and `inspection_support` files. Modify only
`crates/b4-sqlite/src/lib.rs` for module plus narrow exports and
`crates/b4-sqlite/Cargo.toml` for features `["bundled", "limits", "hooks"]`.
Existing Store methods, backup, SQL files, approvals and prior tests stay unchanged.

- [x] Add a compiling public refusal stub plus actual clean stopped Store fixture
  (700 directory/600 DB+owner lock, no sidecars). Capture meaningful behavioral RED:
  `cargo test --locked --offline -p b4-sqlite --test inspection stopped_store_returns_complete_redacted_report_without_mutation`
  must fail because the stub refuses an expected valid report, not setup or compile.
- [x] Implement API `InspectionOptions { pub blocked: bool }`, private encoded-only
  `InspectionReport: AsRef<[u8]>`, static Copy/Debug/Display/Error `InspectionError`,
  and `inspect_stopped(impl AsRef<Path>, InspectionOptions)`. Options expose no SQL,
  URI, callbacks, budgets or authority. Report cannot retain source/resource handles.
- [x] Preflight existing real0700 directory, real0600 nonempty DB and real0600 owner lock
  (the owner lock may be empty);
  cap raw and resolved paths4096 bytes/NUL, preserve nonUTF8 bytes. Capture stable
  dev/inode/mode/size/mtime witnesses, excluding atime. Never create/chmod/unlink.
- [x] Open existing owner.lock read-only/no create/truncate, compare descriptor
  inode and take nonblocking exclusive lock. Revalidate under lock. Reject ANY WAL,
  SHM or journal entry including empty files, broken symlinks and directories.
- [x] Generate absolute percent-encoded file URI from raw path bytes with fixed
  `mode=ro&immutable=1`; explicit READ_ONLY|URI|NOFOLLOW|PRIVATE_CACHE, verified readonly.
  Install engine controls before first prepare; no arbitrary source SQL execution.
- [x] Guard owns Connection and lock. All statements/rows are inner-scope; actual
  Connection::close Ok, final witness check and explicit unlock Ok precede return.
  Normal errors/unwind follow the same ordering. Close Err returns live Connection:
  safe inseparable process-lifetime quarantine with exact lock, static CloseUncertain.
  Unlock Err retains the exact lock descriptor and refuses likewise. Never infer
  successful close/unlock from destructors or child termination.
- [x] Prove actual read-only lock exclusion on macOS/Linux: writer held ->AlreadyOwned;
  reader held before close ->real writer contender AlreadyOwned; after positive close
  +unlock ->writer succeeds while reader child is alive. Prove explicit unlock with
  inherited duplicate lock description. Error paths retain ordering too.
- [x] Use a cfg(test)-only forgotten REAL prepared statement to obtain actual close
  BUSY and prove quarantine keeps physical owner exclusion while child is alive.
  OS termination is cleanup only. Add a cfg(test)-only pre-unlock refusal seam
  retaining a real locked descriptor after verified close; prove physical writer
  exclusion. Label it exceptional branch evidence, not an actual OS unlock fault.
  Do not add public connection/fault injection hooks.
- [x] Test missing/empty/corrupt paths, permissions, symlinks, URI metacharacters,
  nonUTF8 directory on Linux and all sidecar kinds unchanged. The macOS filesystem refuses creating the rawFF source; qualify byte-preserving parse and static source refusal there, with UTF8 metacharacter/nonASCII source success on both targets. Successful no-sidecar WAL-header
  source is allowed. Fresh identical inspection preserves bytes/epoch/entries/modes/
  mtimes; no source checksum is included in public report.

## Task3: Fixed validation, limits and safe report (same SQLite author)

- [x] Set/read back safe API DEFENSIVE=true/TRUSTED_SCHEMA=false before first
  prepare. Fixed query_only=ON/temp_store=MEMORY/cache configuration and readback
  are the first guarded SQL prepares. Limits: LENGTH2MiB, SQL_LENGTH16KiB,
  COLUMN64, EXPR_DEPTH64, PARSER_DEPTH128, ATTACHED0, WORKER_THREADS0.
- [x] Apply main64MiB, schema32/fetch33, DDL64KiB, executions256/fetch257,
  snapshot1MiB each/16MiB total, JSON depth64/16384 lexical nodes, blocked64,
  encoded65536 bytes excluding newline. Checked counter overflow refuses.
- [x] Use cumulative approximately1000-VM-step progress callbacks, max2000 callbacks
  and5s elapsed cooperative checks across SQL plus bounded CPU phases. Hook arithmetic
  is pure/saturating/nonpanicking with positive own-exhaustion flag. Unexpected interrupt
  is not forgiven. No hard I/O/close/stdout latency or total RSS claim.
- [x] Bounded exact schema comparison precedes quick_check/FK/data queries. Reconstruct
  expected schema privately from existing SCHEMA_V1/migrations with same limits/budget;
  no unbounded source schema_objects/read_execution reuse. Unknown objects/schema3
  checksum mismatch refuse. Metadata exactly id1/schema3/eight-byte nonzero epoch.
- [x] quick_check(1) and foreign_key_check under budget; exact six-table structure.
  Validate bounded execution row id/thread/revision/owner <=metadata and Snapshot1,
  kernel invariants, index agreement, thread existence, active pointer same-thread.
  Other tables receive structural/FK checks, not full payload/history semantic audit.
- [x] Scan borrowed JSON before decode with fixed64-entry stack/quotes/escapes and
  exact node definition: all strings including keys, container starts, primitive tokens.
  Guard limits before owned Snapshot decoding; syntax remains decoder responsibility.
  Drop one Snapshot before next row. No intermediate owned Value/source string report.
- [x] Project fixed table/status/declared-policy/uncertain-policy counters. Definition
  counts include uninitialized tasks; actual initialized counts do not. --blocked selects
  Accepted/Running/WaitingForApproval/CancelRequested/RecoveryBlocked OR initialized
  InFlight/OutcomeUnknown, including valid Running failure drains. Failed-with-active
  snapshots are invalid and refuse rather than becoming report fixtures. Exact schema fields only, report-local
  ordinals1..N, default blocked=null, complete selected array inside selection/rows object.
- [x] Meaningful boundary RED/GREEN: all valid enum/policy shapes and secrets, schema
  mismatch/future snapshot/checksum/index/thread inconsistency; private schema
  enumerator N/N+1 ceiling and actual extra-object rejection (accepted exact schema
  has fewer than32 objects, so do not invent a production-success32-object fixture); main/rows/snapshot/total/
  depth/nodes/blocked boundaries. Valid caps use actual structurally valid fixtures.
  Oversized SQLite cells and actual expensive SQL exercise engine/own hook refusal.
  Prove width and private serializer N/N-1 refusal, not impossible production cap fixture.
- [x] Run `cargo test --locked --offline -p b4-sqlite` and focused strict lint/format.
  Root independently checks SPEC; another agent reviews full QUALITY. Repair and freeze.

## Task4: CLI parse/output branch (CLI author, parallel after Task1)

**Exclusive files:** modify `crates/b4-cli/src/config.rs`, `lib.rs`; create private
`src/inspection.rs`, `src/inspection_tests.rs` as useful. Do not edit process/demo/
storage or old fixture tests. Independent qualification owner creates the separate
integration test file named in Task5; avoid concurrent edits there.

- [x] Add failing parser tests: exact `inspect` and `inspect --blocked` accepted while
  invalid/duplicate/missing/arbitrary options refuse before env; only DATA_DIR read,
  default existing.b4run-data, raw Unix paths/NUL4096 bounds, Debug redacts paths.
  Existing informational/serve/config-check behavior and error conventions remain.
- [x] Implement `Command::Inspect(InspectConfig { data_dir, blocked })`; InspectConfig
  stores PathBuf and bool only. Main chooses separate synchronous reader branch;
  no app/Store/Runtime/Tokio/HTTP/port/token/signal/callback/recovery construction.
- [x] Map static InspectionError codes/exits exactly from design. Use separate fallible
  stdout writer: complete safe JSON+newline only AFTER reader return/verified close/unlock.
  Broken output returns `b4run_inspection_output_failed`/1 without panic; partial safe
  bytes are possible on I/O failure, no output on inspection refusal. Diagnostic errors
  remain ignored safely under existing main panic policy.
- [x] Update help to advertise inspect/[--blocked] and only DATA_DIR requirement for it.
  Version/help still inspect no environment. No hidden error payload/path formatting.
- [x] Run actual parser/unit RED then GREEN and existing9 parser cases unchanged.
  Coordinate with qualification author for shipped command expected outputs.
- [x] Root SPEC review then independent full QUALITY review. Repair/freeze exclusive files.

## Task5: Shipped-process qualification (independent qualification author)

**Exclusive files:** create `crates/b4-cli/tests/inspection_cli.rs` and separate
`crates/b4-cli/tests/inspection_support` helper files. No parser/reader implementation.

- [x] Before CLI implementation, run actual built subprocess inspect on a clean private
  Store-generated fixture; save meaningful behavior RED expected report vs existing usage
  refusal. Qualifier may synchronize author to pause until this RED is captured.
- [x] Real shipped binary success aggregate/blocked validates exact JSON schema shape,
  reader versions, all counters/ordinals and redaction. Populate authority/value/identity
  secrets in snapshots/journal/thread/grant/acceptance payloads; test no leak/rawdigest.
- [x] Twice inspect preserving actual source file bytes/modes/mtimes/entries/epoch,
  no sidecars created. Separate real live writer rejects1/no report. Unsupported schema,
  corruption/limit/sidecars/wrong modes/missing source give exact static exit/stdout/stderr.
- [x] Test no token required and malformed TOKEN/PORT/WAIT ignored; raw nonUTF8/URI
  paths work where representable (actual rawFF source success on Linux; macOS filesystem-refusal branch preserves raw parsing and source). Unsupported options refuse; no runtime listener/provider/callback starts.
- [x] Closed stdout is nonpanic1 after close/unlock. Stalled output cannot retain owner
  lock; a real writer reopens while child is alive. Demonstrate actual backpressure
  (for example, a private pre-filled safe UnixStream sink, then actual report output
  begins while the child remains blocked); never assume an unread pipe is full. Do not infer atomic stdout delivery.
- [x] Qualify debug then shipped release alongside existing12 release cases. Individual
  repeats are not new unique tests; all old ignored/helper scope remains honest.

## Task6: Fresh platforms, docs, trace and evidence freeze (root)

**Files:** README, runtime/storage/security/compatibility docs, new ADR0020/runbook,
new inspection section in spec/runtime/requirements.yaml, scripts/check_contracts.py,
new authored report fixtures compared with actual qualified output, new verification receipt.

- [x] Compare exact locked dependency graph/checksums; only selected rusqlite feature
  flags change. Rust package versions and Cargo.lock/root manifest remain exact.
- [x] Run current `cargo fmt --all -- --check`, `cargo clippy --locked --offline
  --workspace --all-targets -- -D warnings`, `cargo test --locked --offline --workspace`
  on macOS and actual nonroot Linux/arm64 updated source container. Release build and
  old12/newinspect integration cases both platforms. Selected file access/lock proofs
  must pass both. Do not reuse old images to claim new source qualification.
- [x] Run complete retained Node50/Python56/contracts plus report-schema checks. Map
  only new named inspection requirements to actually executed functions. Keep56broad
  scenarios pending, M0open, CP001–009partial/CP010unresolved, M5partial/fullopsopen.
- [x] Update user docs with actual command/report/scope/error/static limits/read-only
  source/lock/sidecar/cleanup restrictions; describe close-error intentional quarantine,
  reader-vs-writer versions, deferred semantic checks, no exact latency/RSS guarantee.
- [x] Build new receipt: fresh source/package/feature/platform/count/RED-GREEN/log hashes,
  report fixtures, scoped proof/limitations and all209 protected hashes. Do not record
  raw source data/secrets/source digests. Independently audit full evidence/privacy and
  unique test counts, repair and freeze. Continue next authorized requirement.

Progress: Task1 complete. Reader45, CLI9 and independent process19 implementations are frozen after meaningful behavioral RED/GREEN. Complete root SPEC and independent QUALITY reviews pass for reader, CLI and process qualification. A one-line cfg(test) import-format correction has an exact reconstruction/fresh9-test bridge. Checker13 passes after independent review repairs. Fresh macOS769/0/3historicalignored and release12+19 pass; retained Node50, Python71 and318 named contracts pass. Actual shipped clean-retired-store reports match both authored examples without source changes. Fresh nonroot Linux769/0/3historicalignored and release12+19 pass against206 exact host source/fixture hashes; the13 retained Linux lifecycle cases pass anew against the current image and clean up all owned resources. Independent complete documentation QUALITY and candidate evidence audits pass without findings. Task6 is complete; the rebuilt final receipt is subject to the separate full final integrity audit before the next public increment begins. No manual action or additional permission is currently needed.
