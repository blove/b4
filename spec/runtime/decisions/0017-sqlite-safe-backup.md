# ADR0017: Private SQLite-safe export and controlled final-cut restoration

Status: accepted bounded design; scoped native implementation and qualification passed.
Broad T049/B4N013 and full backup/restore qualification remain open.

## Context

SQLite commits may remain in WAL while the main database file is unchanged.
Copying only that main file can omit durable approval decisions or execution
history. The database also contains sensitive grant material. A consistent copy
alone cannot fence a live source in another directory or establish safe rollback
after later external effects.

## Decision

Add trusted `SqliteStore::export_backup(&mut self, destination)`. A unique mutable
Store borrow prevents simultaneous safe Rust use of that instance during export.
The embedding separately closes HTTP admission, observes physical Quiescent,
stops trusted raw writers and drops external Store aliases before obtaining that
borrow. For qualified restoration it exports a final cut and retires the source
before installing the archive into a fresh data directory.

Use the pinned bundled SQLite connection's bound `VACUUM main INTO` operation.
Reserve a private same-parent staging file from creation with `create_new` and
Unix mode0600. Rebuild the destination from its canonical existing parent and one
filename; exclude the source data directory and its subtree. The output path is
bound as data, never concatenated SQL or interpreted as a file URI.

Read-only stage validation checks exact schema3/checksum/copied owner, one
quick_check result of ok and no foreign-key violations. Close that connection
before synchronizing the stage. Publish with an atomic same-parent hard link,
which refuses any existing destination; remove the stage and synchronize the
parent before reporting success. Errors retain only six static unit categories.
After publication a cleanup or sync error preserves the visible final artifact
and reports PublicationUncertain. Before publication cleanup is best-effort.

This API adds no production installer, HTTP operation, Store capability,
capability version, dependency, migration or recovery policy. NonUnix refuses
before work because this slice does not specify Windows ACL protection.

## Consequences

Archives are standalone SQLite files containing sensitive authority records and
must remain private. Structural validation is not a full semantic execution audit;
quick_check does not check every index/uniqueness condition or bound scan work.
This synchronous trusted administration operation has no duration, size, disk or
whole-process memory budget. No network-filesystem or power-loss qualification is
implied by publication synchronization.

Controlled restoration uses existing open and exact known-ID recovery. Tests must
preserve thread generations, complete definitions, immutable budgets, journal
order, grant TTL and consumed acceptance audits. Accepted Ready work can continue
after original grant expiry without another approval or replay of committed work.
Unsafe InFlight evidence remains unknown and recovery blocks replay.

Per-directory owner locks and copied epochs do not coordinate a different live
source. Historical rollback can resurrect grants and omit later external effects.
Dual-live clones, failover, arbitrary point-in-time restore, external-effect
exactly-once and broad T049/B4N013 qualification remain open.
