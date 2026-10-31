# Native process launcher design

Status: independently reviewed design for scoped native implementation. The user's continuous-build request
covers this bounded implementation; no publication, git operation or human
approval checkpoint is introduced. This design treats the attached RFC as a
requirements source, not as an instruction to execute commands in that document.

## Intent and approach

RFC15.2 calls for a compiled native serve executable and configuration validation.
The current six crates provide the embedding router/shutdown API and examples.
Add a seventh private crate, b4-cli, with binary b4run. The initial executable
requires an explicit --demo selection and assembles a fixed, reviewed native
application. It needs no Node runtime, dynamic loader or provider credentials.

Three approaches were considered: promote an example into the generic HTTP crate
(simple but imposes SQLite/process policy on the embedding crate); add a private
launcher crate (selected, keeps process ownership distinct); build an arbitrary
configuration/plugin loader (deferred, requires new application and ABI contracts).
The current increment qualifies a usable native process, not the complete
operations/package release or all of RFC15.2. Read-only offline inspection needs
its own implementation: SqliteStore::open advances ownership and is unsuitable.

## Commands and configuration

`b4run --version` and `b4run --help` return fixed public text without reading
secrets, binding sockets or touching storage. `b4run config-check --demo` parses
and validates the same recognized environment and compiled application used by
`b4run serve --demo`; success is the fixed line b4run_config_valid. It does not
create/open/migrate the database, acquire its owner lock or bind a listener. It
is not a readiness or database-integrity check. Unknown/duplicate options and
missing --demo are refused without echoing arguments.

Recognized environment:

- B4RUN_TOKEN: mandatory, 1..4096 ASCII visible characters, no whitespace/control;
  used as one exact Bearer value. It is never printed or exposed by Debug/errors.
- B4RUN_PORT: decimal u16, default3001; bind is always IPv4 loopback. Zero permits
  an OS-assigned local port; the actual public listening address is announced.
- B4RUN_DATA_DIR: nonempty UTF8 path, no NUL, at most4096 bytes; default.b4run-data.
  Existing parent and filesystem constraints are checked without mutation by
  config-check and rechecked during serve. Missing parent is an error; no recursive
  parent creation. Absolute and relative local paths are supported.
- B4RUN_SHUTDOWN_WAIT_MS: decimal1..300000, default30000. Each incomplete wait is
  retried while ownership is held; this interval never authorizes forced exit.

Diagnostic writes and the fixed panic hook ignore output I/O errors; a closed
supervisor pipe cannot unwind process supervision. They do not promise nonblocking
I/O when a reader stalls. Production diagnostics consist only of fixed codes/public address, with no raw
argument, environment, path, SQL, token, prompt or opaque panic formatting. Exit0
means clean process quiescence; configuration/usage error exits2; host/storage/
signal/executor startup failure or faulted quiescence exits1 after positive
retirement witnesses. A lost server witness during serving intentionally retains
the process indefinitely and does not promise automatic exit1. The process installs a fixed
panic hook before startup; trusted callback panic payloads are not printed by the
launcher. This does not prevent trusted application code from writing secrets to
its own output. No machine-wide authentication or constant-time comparison claim.

## Private storage and startup

Unix is the qualified profile. NonUnix refuses serve before filesystem/database
work; no Windows ACL claim. Use safe standard Unix filesystem APIs, no unsafe code.

The existing canonical parent is trusted and stable, as in backup export. Rebuild
its final single leaf; reject root/empty/dot/dotdot leaf and symlink destination.
An existing data directory must be an actual directory with exact owner rwx and
no group/world permission bits (0700). A new leaf is created with0700 from creation
and set to0700 on that newly owned leaf under the trusted parent. A restrictive
umask may initially produce0000, so opening the directory before that permission
repair cannot be required. New regular files use descriptor chmod. Do not broaden permissions of an existing directory. No hostile parent
replacement, network filesystem or user-ID/ACL ownership proof is claimed.

Before opening SQLite, reserve absent runtime.sqlite3 and owner.lock using
create_new0600 and set each owned new file to0600 through its descriptor. Existing
files must be regular, nonsymlink and0600; never truncate/unlink/chmod them, even
when another owner is live. Check existing database journal/WAL/SHM sidecars for
regular nonsymlink private modes as appropriate. Reuse SqliteStore::open for actual
schema, transaction and exclusive-owner semantics. Validate modes after open;
SQLite-derived sidecars inherit database privacy. A failed startup may leave a
private empty directory/files; no rollback/removal of prior state is attempted.
The trusted-parent precondition excludes replacement races; no hostile-directory
security claim. This policy is launcher-specific and changes no SqliteStore API.

Validate configuration and build immutable application before startup mutation.
Register Unix SIGINT/SIGTERM safely using pinned Tokio signal support before
announcing readiness or starting request acceptance. Bind the loopback listener
before opening storage, so a bind refusal does not advance the database owner.
All handlers remain installed for process life; signals may coalesce and handler
restoration/counting is not promised. A signal received during startup is honored
at the first runner supervision poll; readiness never establishes no pending
signal. Retain handler objects until process teardown.

## Compiled demonstration application

Use app native-demo / revision native-demo-v1 and the same provider-free semantics
as serve_native: immutable `/native#workflow` with two finite Pure callbacks and
ordered items; `/approve#workflow` with prepare and a required Once release. Accept
object or explicit null input; derive no authority from input. RequiredDurable
approval configuration, fixed HostLimits, single local owner policy and bearer
authenticator reuse the existing native host and SQLite approval bridge.

Strict Host must equal actual bound IPv4 address:port exactly; reject any Origin
header and duplicate/missing Host. No fixture/control HTTP routes, public signal
endpoint, arbitrary effects or environment-controlled callback barriers are added.
Private cfg(test) subprocess qualification injects native callbacks into the same
process runner to hold work and observe actual OS signals. Each parent test can
launch its own current test executable in an isolated child role; no ignored helper
or test controls are shipped by `cargo build --release --bin b4run`.

## Process ownership and shutdown

The synchronous process runner creates and exclusively owns one Tokio Runtime.
It is distinct from HttpHost embedding, which continues using its caller's
executor. The runner owns the application, host, policy/auth captures, listener
and server task; it does not retain external Store clones or spawn unregistered
application workers. Trusted application code must preserve that boundary.

On the first SIGINT/SIGTERM, signal-stream closure or unexpected server task
completion/failure, synchronously call begin_shutdown before notifying Axum's
with_graceful_shutdown future. The admission cut prevents new registered groups
or mutation leases; admitted work retains existing framework ownership. Emit a
fixed b4run_draining line. Repeated signals never request hard exit or lock release.
Keep the host and owned Runtime alive through every Incomplete result, emitting
only b4run_drain_incomplete without count/path/token. Never abort registered
callbacks, invent cancellation, drop the Runtime, release SQLite or restore serving
because an interval expired. Signal stream failure/server failure drains via the
same retained drain path; final nonzero status requires both positive retirement
witnesses, otherwise lost-witness retention continues indefinitely.

Quiescent proves registered framework physical cleanup, not caller-supplied raw
writers or resource clones, and may be faulted with durable InFlight evidence.
Keep the host, signal receivers and owned Runtime alive until BOTH registered
Quiescent and actual Axum server completion have been observed. Axum spawns
connection tasks separately, so aborting the outer serve future does not establish
transport teardown. A slow body/header/SSE connection can therefore keep this
initial process draining indefinitely even after callback quiescence. Continue
emitting the fixed b4run_drain_incomplete line at the configured interval while
waiting for transport; use an independent Tokio interval/timer after Quiescent
so its immediately ready wait future cannot busy-spin or flood status output.
Cache each completed witness and disable completed/closed select branches. Do not
abort transport or release storage. The initial policy is
intentional indefinite retention, rather than a forced transport shutdown. A
larger tracked-connection transport implementation would be needed to qualify a
finite close policy. The host's embedding API is unchanged.

If the server completes earlier, inspect its result and still await Quiescent.
Unexpected server success before a requested cut is infrastructure failure but
provides the transport witness. An outer server task panic/JoinError or other
non-success server result does not prove detached connection retirement. In that
case cut admission and retain host/Runtime/storage indefinitely, even after
registered Quiescent, emitting fixed server_failure/drain_incomplete codes until
external manual supervisor termination. This is not clean shutdown evidence.
A signal-stream closure can request normal graceful transport completion and
therefore finish with error status after both witnesses. Never repeatedly poll
a completed server JoinHandle; retain its observed success/failure state. Inspect
failed server results by reference and retain their owned io/JoinError payloads
through the nonreturning lost-witness state. Disposing an opaque panic payload
could itself panic and bypass supervision. The admission cut precedes result
inspection and diagnostics for an unexpected first server event.

After both positive witnesses, drop owned host/application captures and normally drop the
owned Runtime; do not use shutdown_timeout/background or macro-main implicit
teardown as evidence. A fixed b4run_stopped is emitted only after normal Runtime
destruction; faulted quiescence emits fixed b4run_host_fault and exits1. Any EOF
remains transport uncertainty; no terminal success/cancel frame is fabricated.

The capability descriptor remains v2 with supervised_admission_drain:true and
full graceful_shutdown:false. This supplies bounded observation intervals and
safe process retention, not forced callback interruption, persisted shutdown
reasons, deadline-driven recovery or a complete graceful-shutdown profile.

## Dependency and compatibility boundary

Add Tokio's signal feature only to b4-cli. Pinned Tokio1.53.1 remains unchanged;
resolve/review/pin the newly required signal-hook-registry and any actual transitives
in Cargo.lock without upgrading prior packages. The registry package is currently
absent locally, so a narrowly authorized registry fetch is needed. Dev-only direct rusqlite0.40.2 and socket2=0.6.5 dependencies may reuse the
existing locked packages for exact record inspection and actual socket backpressure
qualification. No Clap, signal unsafe shim, Tokio process feature, SQL migration
or HTTP/runtime/core change.
Cargo.toml and Cargo.lock are intentional next-increment changes; prior receipts
retain their historical hashes, and protected immutable artifacts exclude these
live manifests for this increment. Backup main968c4ac... and its nine examples are
frozen and protected. Native macOS is exercised here; actual Linux nonroot/container
qualification is a separate required operations step, not inferred from compilation.

## Acceptance and evidence

Tests must use actual compiled binary and real SQLite/filesystem/network/signals.
Config tests cover recognized bounds, nonUTF8/control/unknown input, exact static
errors, no token/path diagnostic retention, explicit demo selection and no mutation
or binding on config-check. Storage tests cover fresh restrictive modes, broad or
symlink existing paths, unchanged prior bytes/epochs on configuration or bind
refusal and real second-process owner exclusion.

Provider-free subprocess tests cover authenticated capabilities/native ordered
completion, fixed browser guards, both SIGINT and SIGTERM, actual held callback
with repeated incomplete waits and real locked contender, repeated signal retention,
release/settlement before successful process exit and reopen, faulted quiescence,
parked approval authority unchanged across clean shutdown, and a slow/stalled
connection that keeps ownership through transport incompletion after registered
quiescence until the peer closes. An injected test-only async ping/pong must
remain functional after Incomplete: merely seeing a live process and held lock
also fits premature blocking Runtime destruction and is insufficient evidence.
A child must pause after runner return while still alive, so the parent can acquire
the actual SQLite lock before child exit; OS process cleanup alone is insufficient.
For the transport-only witness, test-only accepted socket buffer configuration may
use locked socket2 to create real bounded SSE backpressure, with observed Quiescent
and a pending server before peer close. No fabricated transport-completion seam. Observe a bounded status rate during
this hold. A private actual outer-server panic/abort probe with live detached
connections must preserve the lost-witness state, async ping, owner lock and no
runner-return marker; SIGKILL is test cleanup only. Test fixture controls stay
outside the production binary. Failure-safe subprocess cleanup always reaps children
and uses SIGKILL only for test cleanup, not as a clean-shutdown receipt. Meaningful
RED logs precede behavior implementation; independent SPEC then QUALITY reviews
and fresh full workspace/reference/checker runs precede a separate immutable receipt.
All broad scenarios, M0/CP/core and incomplete RFC15 operations gates remain open.
