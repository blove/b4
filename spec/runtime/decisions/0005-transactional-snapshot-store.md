# ADR-0005: Shared snapshot transactions and explicit owner recovery

Status: accepted and implemented for the bounded native transactional increment.

Keep the kernel independent of persistence. `b4-runtime::Store` accepts fenced
commit proposals and stores portable snapshots with a transition journal;
memory and SQLite run the same backend contract. Admission occupies a thread
until completion, failure or cancellation. Parked, cancelling and blocked work
keeps the slot, and previous terminal executions remain loadable.

Use one serialized synchronous SQLite connection per native data directory.
Acquire an OS advisory exclusive lock before database access, never unlink its
stable lock file, and drop the connection before releasing ownership. Every
successful open advances a durable unsigned owner generation. Only explicit
runtime recovery transfers an older execution to the current owner. Ordinary
commands and completions reject older owners.

Persist each InFlight attempt before invoking a host callback. Never replay that
callback on a persistence conflict. Request fresh completion facts from the host
after its callback succeeds. Return the obtained result on failed publication,
and borrow it during completion-only retries so the host retains it across
repeated failures. Pure/externally idempotent uncertain effects follow the
kernel's declared recovery policy; unsafe/reconcilable uncertainty blocks.

Use exact rusqlite 0.40.2 with bundled SQLite 3.53.2, fs2 0.4.3 and sha2 0.10.9.
The selected SQLite includes the upstream WAL-reset fix. Rust 1.81 fails on
generated `unsafe extern` bindings; qualify and pin installed Rust 1.93.1 rather
than claim an unsupported minimum. The schema checksum covers exact embedded
DDL bytes; verify the supported schema before advancing the owner epoch.

This snapshot/journal representation is a subset of the RFC's future logical
records. Ready tasks and InFlight attempts supply caller-driven eligibility and
claims; an autonomous durable dispatch queue, recovery scanning, grants, HTTP,
backups and complete crash qualification remain future work. Memory transactions
are not durable evidence. Local SQLite reopening and an idle-child process-kill
test do not establish arbitrary exactly-once effects or every crash boundary.

Primary dependency and durability references:

- [rusqlite 0.40.2 manifest](https://raw.githubusercontent.com/rusqlite/rusqlite/v0.40.2/Cargo.toml)
- [Bundled SQLite version](https://raw.githubusercontent.com/rusqlite/rusqlite/v0.40.2/libsqlite3-sys/sqlite3/sqlite3.h)
- [SQLite WAL qualification](https://www.sqlite.org/wal.html)
- [SQLite connection settings](https://www.sqlite.org/pragma.html)
- [SQLite transactions](https://www.sqlite.org/lang_transaction.html)
- [fs2 OS file locks](https://docs.rs/fs2/0.4.3/fs2/trait.FileExt.html)
