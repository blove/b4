# SQLite-safe backup export implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development and test-driven-development, with
> independent specification review followed by quality review. The user's
> continuous-build authorization covers this increment. No git mutation,
> publication, new checkout or routine human approval is requested.

**Goal:** Export a private, consistent SQLite backup into a fresh artifact and
qualify controlled final-cut restoration of parked, accepted and uncertain work.

**Architecture:** A trusted mutable SqliteStore export reserves a private stage,
uses the existing SQLite connection's VACUUM INTO, validates the readonly result
structurally, and atomically publishes it without replacing any destination.
Existing open and exact known-ID recovery qualify a fresh installed copy after
retiring the source. No production restore/install command is added.

**Tech Stack:** Existing bundled SQLite3.53.2, rusqlite0.40.2, standard filesystem
APIs and the existing random generation helper; locked offline dependencies,
portable kernel/snapshot and SQLite schema3 unchanged.

Design: `spec/runtime/sqlite-safe-backup-design.md`.
Frozen prior receipt: `conformance/expected/supervised-admission-drain-verification.json`.
Private inventories `/private/tmp/b4-sqlite-backup-baseline.json` (440 files) and
`/private/tmp/b4-sqlite-backup-protected.json` (188 immutable artifacts) were captured
after the prior receipt froze and before feature sources/docs were edited.

## Task 1: Freeze design, ownership and reviewed plan (root)

- [x] Incorporate independent design precision: unique mutable Store borrow;
  canonical parent plus one filename; source-directory/subtree exclusion; private
  staging from creation; readonly structural validation; exact quick_check result
  and empty FK rows; postpublication uncertainty; explicit final-cut covenant.
- [x] Record the actual pinned-library mechanism probe: SQLite3.53.2 accepts a
  precreated zero-byte0600 stage with WAL/DELETE sources; output is standalone
  DELETE-journal mode. A separately observed filesystem check supports the design;
  final publication/directory-sync evidence will come from implementation tests. System
  Python's different SQLite result is outside the pinned native profile.
- [x] Freeze public API and static errors, capture baseline/protected hashes,
  and independently review this complete plan before implementation. Host approved
  the complete design and plan; service/review agents separately approved design.

Public API, exported by b4-sqlite:

```rust,ignore
pub enum BackupError {
    InvalidDestination, DestinationExists, InvalidDatabase, Unavailable,
    PublicationUncertain, UnsupportedPlatform,
}
impl SqliteStore {
    pub fn export_backup(&mut self, destination: impl AsRef<Path>)
        -> Result<(), BackupError>;
}
```

Display codes are fixed: backup_destination_invalid, backup_destination_exists,
backup_database_invalid, backup_unavailable, backup_publication_uncertain,
backup_platform_unsupported. Unit enum Debug and Display never retain paths,
SQL diagnostics, snapshots, prompts or tokens.

This trusted synchronous administration operation has no work deadline, archive
size/disk budget or whole-memory bound. It is not a new Store capability, HTTP
operation or capability-descriptor version. NonUnix returns UnsupportedPlatform
before filesystem/DB access; Windows ACL and network filesystem support are not
claimed. No schema/dependency/recovery-policy/callback semantics change.

## Task 2: Export source and API tests (service author)

Create `crates/b4-sqlite/src/backup.rs` and focused source/API tests
`crates/b4-sqlite/tests/backup.rs` as useful. Exclusively own SQLite `src/lib.rs`
for narrow module/error exports, canonical source-directory PathBuf recording,
and source open-path use. Preserve connection-before-owner-lock destruction and
all existing transaction/open behavior. Do not edit SQL/Cargo files, runtime/HTTP,
existing tests, docs, receipts or other authors' new qualification files.

- [x] Add the smallest compiling API shell and write meaningful behavioral tests
  first. Record RED caused by missing export/publication behavior, not compilation
  or fixture mistakes. Initial focus: WAL-inclusive fresh archive, no overwrite,
  source unchanged and private mode.
- [x] Implement canonical existing parent + single filename; reject invalid UTF8,
  missing/invalid parent and source directory/subtree including canonical aliases.
  Detect an existing file, empty file, directory or dangling symlink without
  following it; final publication must independently retain no-overwrite semantics.
- [x] Reserve random same-parent stage using existing generation entropy, a finite
  collision retry count and create_new0600. Account for restrictive umask if exact
  mode0600 is promised. Keep stage/sidecars private. Never create a caller-requested
  final file before the archive is ready.
- [x] Uniquely access the source connection and verify exact current schema3,
  checksum and Store owner. Bind the absolute UTF8 stage path to VACUUM main INTO;
  never concatenate caller text into SQL or interpret it as a URI.
- [x] Open stage READ_ONLY without URI flags, require exact schema/checksum/copied
  owner, exactly one quick_check(1) row equal to ok and zero foreign_key_check rows.
  This is structural/database validation, not full execution/grant payload scanning.
  Close validation connections/statements before sync or publication.
- [x] Synchronize the stage; hard_link to the fresh final path; remove stage and
  synchronize parent before success. Before link, clean the owned stage best-effort.
  After link, cleanup/sync failure preserves final and returns PublicationUncertain.
  Never roll back a visible final artifact and never overwrite on retry.
- [x] Test actual committed WAL inclusion against an insufficient main-file copy;
  exact logical source rows and owner unchanged; fresh repeated exports; quoted/
  nonASCII path binding; permissions; existing/racing destinations; all path refusals;
  structural corruption and static diagnostics. Use boolean or hashed comparisons
  for issued grant material so failing assertions do not print it.
- [x] Add meaningful fault tests for prepublication SQLite/validation/sync/link
  errors and postlink unlink/parent-sync uncertainty. Minimal private filesystem
  seams may inject the one failed operation; retain actual SQLite/filesystem work.
  Do not use fake durable Stores or introduce production fault controls.
- [x] Run scoped SQLite tests and strict lint/format. Obtain independent SPEC
  review (host) followed by QUALITY review (root), resolve findings, freeze source.

## Task 3: Independent controlled restore qualification (review author)

Create `crates/b4-sqlite/tests/backup_restore.rs` and a new
`tests/backup_support` helper if needed. Exclusively own those files. Do not modify
production, prior tests/helpers, historical receipts or other authors' files.
Tests may start once the public API shell exists; record meaningful RED only if
real absent export behavior causes it. Root reviews SPEC; host reviews QUALITY.

- [x] Create a real source SQLite cut with a committed sibling and stable parked
  required grant. Stop all writers, uniquely own Store, export, drop source, install
  the finalized standalone archive into a fresh private test directory, then open it.
  Compare exact thread generation/origin/resource revision, definition, snapshots,
  task/attempt histories/budgets, pending/grant/acceptance records and ordered journal.
  Compare logical values/order, not whole file bytes: VACUUM may repack row storage.
- [x] Preserve original parked prompt/token/TTL and the rules for expired acceptance.
  Recover only the exact trusted known ID; verify changed full definition/identity
  refuses without callbacks or sensitive-record changes.
- [x] Export accepted Ready continuation before its callback. After source retirement,
  restore and recover beyond the original grant expiry. Complete the accepted Once
  exactly once without a new approval/audit and without rerunning the committed
  sibling. Keep immutable budget and prior attempt history accurate.
- [x] Export Unsafe InFlight evidence, restore and recover to RecoveryBlocked;
  preserve unknown outcome and prohibit callback replay or fabricated success.
- [x] Qualify malformed/unsupported restored records through existing open/load
  refusal and recovery fencing. Do not advertise a new production installer or
  imply export's structural checks validate every execution payload.
- [x] Prove actual source owner lock remains held during export and releases only
  on drop; restored directory has its own real owner lock and next copied epoch.
  No assertion that an epoch fences a different live directory.
- [x] Use real SQLite only, private temporary directories and failure-safe cleanup.
  Record no issued token or raw sensitive assertion output. Obtain SPEC then QUALITY
  review and a fresh qualification run after final source freezes.

## Task 4: Documentation, traceability and frozen evidence (root)

Root owns README, new ADR0017, security/storage/compatibility docs, requirements,
plan progress and new final receipts. Existing wire contract/descriptor/examples,
Cargo.lock/Cargo.toml and all SQL/reference/historical receipts remain unchanged.

- [x] Document API/error meanings, final-cut unique ownership/source retirement,
  private artifacts, structural validation limits, atomic no-overwrite publication,
  prepublication cleanup limitations and PublicationUncertain semantics.
- [x] Document no production restore command, no historical rollback, dual-live
  clone/failover, external-effect exactly-once or source-retirement proof. Add scoped
  named backup requirements; keep all56 broad scenarios/T049, M0 and CP gates open.
- [x] Fresh workspace tests, format, strict all-target lint, retained Node/Python
  integrity, current contract checker and fresh final builds/qualification for the
  nine existing examples. Do not rerun upstream installation/build or rewrite prior
  receipts. Record actual test counts and meaningful RED hashes only.
- [x] Save a separate sqlite-safe-backup-verification receipt with current source
  hashes and unchanged protected historical hashes; independently audit counts,
  permissions/ownership/restore claims and static diagnostics before freezing.
- [x] Continue authorized runtime work until a real manual action/approval is needed.

Current progress: implementation and independent specification/quality reviews
are approved. Fresh workspace verification passes 651 Rust tests (23 new: 11 export
API, 5 private publication-fault and 7 independent controlled-restore tests),
50 retained reference integrity checks and 13 Python tests. Format and strict
all-target lint pass. All nine existing examples were rebuilt and freshly
qualified. New traceability maps every new Rust test to 15 scoped requirements;
all broad release scenarios remain pending. The separate immutable receipt is
approved by independent audit and frozen.
