# Linux container packaging and persistent-volume qualification

Status: accepted amended design; scoped local implementation and qualification complete.
The original candidate was independently reviewed after native-launcher freeze;
the two-file Linux fixture repair and Docker adapter corrections were separately
reviewed before execution. The separate Linux receipt records actual evidence. User continuous-build
authorization covers reversible local image/container/test-volume work; no push,
deployment, unrelated resource removal or Git mutation is requested.

RFC15.3 asks for actual native and Linux nonroot container execution with private
persistent local storage and probes. Native launcher696 qualification is macOS
arm64 only. This increment packages and qualifies the same production binary on local
Docker Linux/arm64; it does not introduce a new bind option, protocol, Store API,
inspection endpoint, SQL migration or Rust dependency.

## Selected boundaries and alternatives

Use a two-stage official Rust1.93.1 builder and Debian bookworm-slim runtime. A
curl readiness helper is smaller than adding another Rust operational subcommand.
Keep IPv4 loopback semantics: --network none container probes use inner loopback;
there is no host ingress/published-port claim. Bind-mounted preowned storage is a
possible operator follow-on. Selected named-volume-nocopy plus a narrowly scoped
fresh-empty initializer provides an explicit measured ownership boundary; implicit
Docker volume auto-population would obscure that proof. No Dockerfile VOLUME or
EXPOSE is needed.

Pin immutable official base indices:
- rust:1.93.1-slim-bookworm@sha256:5b9332190bb3b9ece73b810cd1f1e9f06343b294ce184bcb067f0747d7d333ea
  linux/arm64/v8 child c4aff1f5fe1b20c1a2652776235f4d81ee041a722045b771634361ff4c933ba3.
- debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
  linux/arm64/v8 child0c8bbb8e987a035fe1d9704eb2e571b7e9a836e1caa46345290674b45b69e417.

Apt additions are not pinned by a base digest. Record actual added/all installed
package versions, final content-addressed image ID, executable SHA, ldd/architecture
and package verification results. Rebuilding later may select new Debian packages;
no reproducible-build/security-audit claim. Builder adds procps for actual /bin/kill
subprocess tests and exact-toolchain rustfmt/clippy; existing gcc/libc6-dev support
bundled SQLite. Cargo fetch uses the unchanged Cargo.lock checksums then locked
offline build/tests. No host cache/private registry credentials copied to image.

## Files and assembly

Dockerfile has builder, qualification and runtime stages. Copy only reviewed
manifest/toolchain/crates plus contracts/spec/conformance material required by
include_str! tests; exclude target/.git/private caches/node_modules/.env/data/SQLite
and local receipts/logs through explicit .dockerignore allowlist. Historical
reference recordings needed by Rust tests are retained fixtures, not fresh captures.

Builder creates UID/GID10001 and owned work/cache/output directories. Run fmt,
strict all-target lint and actual all-target workspace suites as this nonroot user;
log UID/OS/arch and parse actual unique696 entries/3 prior ignored without nested
helper doublecount. Build release and run the same12 independent CLI cases against
its path; record as repeated executions, not additional unique tests. Qualification
stage additionally includes /opt/b4/container_snapshot.py and
/opt/b4/container_initialize_volume.sh; neither is copied to runtime. The test-only
read-only SQLite snapshot utility (Python
standard-library sqlite3 is acceptable if installed only there) accepts only fixed source data dir, opens existing DB URI mode=ro with
query_only and returns a private canonical logical-row digest/exact comparisons;
all authority bytes stay in private captured subprocess pipes/memory, never
companion PID1 log files or public receipts. Public snapshot summaries contain
counts/booleans, no authority-derived digest or row diff. Readonly promises logical
row preservation, not WAL/SHM byte preservation. A live read-only
connection must coexist with the owner's WAL, not copy only the main file while
live. Use one explicit readonly transaction for a coherent WAL snapshot across all
schema3 tables/epoch; compare every logical row unchanged apart
from documented owner metadata epoch on reopening, with original snapshot owner
unchanged. No production repair/adoption/recovery command is added.

A distinct stopped-only readonly mode may add immutable=1 ONLY after the driver
positively observes all task volume writers exited, closes every observer promptly
before retirement, and verifies absent WAL/SHM/journal. The companion independently
refuses stopped mode if any sidecar exists. Never use immutable for a live or
unclean source, keep a reader alive through stop, or ignore existing WAL. This
permits private inspection of a clean WAL-header database on a readonly volume
without recreating sidecars; [SQLite WAL documentation](https://www.sqlite.org/wal.html)
explains those readonly preconditions. Qualify missing/sidecar/unclean refusal and
actual clean-source reads separately. The isolated task-volume writer covenant is
explicit; arbitrary administrators changing that source remain outside the profile.

Runtime copies only b4run release and installs curl with its required libs. Direct
exec ENTRYPOINT ["/usr/local/bin/b4run"], CMD ["serve","--demo"], USER10001:10001,
STOPSIGNAL SIGTERM. Set fixed operational defaults PORT3001, DATA_DIR/var/lib/b4run/data;
main image parent private0700/UID10001. No token build ARG/ENV, provider credentials,
Node/Cargo/Rust compiler or test instrumentation in runtime. HEALTHCHECK exec-array
curl --disable --fail --silent --output /dev/null --noproxy '*' --connect-timeout2 --max-time3
http://127.0.0.1:3001/readyz. URL supplies exact Host; no Origin/Auth. Bound Docker
probe timeout independently and document this fixed-port profile; overriding port
requires an operator healthcheck override and is outside this initial profile.

## Volume and secret boundaries

A qualification driver creates only unique labeled task-owned local named volumes
and containers. Always mount volume-nocopy at /var/lib/b4run. Inspect exact allowlisted
mount source/name/type/RW and local driver plus label; do not read raw Config.Env.
Separate one-shot initialization runs UID0 ONLY for its own fresh empty volume root,
with no other writers, no network/privilege/socket mount. Check actual exact mounted
root and every entry including hidden entries BEFORE any mutation, reject nonempty
without chmod/chown, then change only empty root UID/GID10001 and mode0700. No
recursive repair. Main runtime and Rust qualification processes always nonroot;
image build-stage root and isolated initializer are explicitly separate.

Check actual mounted parent, fresh data leaf0700 and DB/owner/existing sidecars0600,
nonsymlink regular files, effective UID10001, PID1 actual executable. Qualification-driver preflight rejects a mounted parent not exactly0700/UID10001
before starting the runtime. The unchanged launcher itself requires only an
existing trusted parent; it refuses an inaccessible parent or unsafe data leaf/files,
not every broad parent. No new parent guard is claimed for the binary. A
nonempty initializer input remains byte/mode/owner unchanged. Refuse unrelated
volume names/labels before mutation; cleanup only driver-created identified resources.

Generate token in memory/private0600 env file, pass runtime-only to Docker; token
is visible to authorized Docker daemon administrators in Config.Env, no daemon
secret-isolation claim. No token command argument printed in logs. Requests use
in-container curl config via stdin for Authorization, suppress token/grant bodies
from public logs, retain private response data only in memory/private files. Never
collect unfiltered env/inspect/HTTP bodies and then attempt receipt redaction;
allowlist public status/counts/booleans/hashes/identities before writing receipts.

## Actual qualification and retirement

Build/run Linux suites then separately run the shipped runtime image --version/help
and readonly config-check; prove no Node executable/runtime package. Start nonroot
serve --demo with network none and named private volume. Independently verify
/healthz liveness, /readyz readiness and Docker healthy status, exactHost/Origin/auth
refusals, capabilitiesv2 conservative graceful=false, ordered native completion and
required durable approval park. Private exact snapshots include original grant
prompt/token/TTL, acceptance rows and prepared task without join-state fabrication.

A second runtime container with the same volume fails StorageOwned; prove unchanged
owner epoch/snapshot/rows while first process remains alive. Test distinct config/bind
preconditions readonly where applicable without upgrading generic HTTP behavior.
Send actual SIGTERM directly to PID1 and poll positive exited status0 plus fixed
stopped marker and OOMKilled=false. Process suites separately prove async liveness
and locked ownership during incomplete held work/transport, with child-alive reopen
before OS cleanup. Container removal/stop timeout/SIGKILL does not establish drain.
On failure, bound driver waiting and record failure before force-kill/reap cleanup;
never describe forced cleanup as positive evidence. No automatic restart on timeout.

Recreate a fresh container using SAME named volume only after actual exit. Exact
logical-row/authority/snapshot comparisons prove persisted park; only database
metadata epoch advances on open. GET/SSE readonly prior-owner pending projection
preserves original prompt/token/expiry, ends with null output, does not adopt or
replay; ordinary prior-owner pending remains existing501. Inspect acceptance rows
remain unchanged. Clean SIGINT second retirement and third-start/private volume
reuse check may be added if independently qualified, never inferred. Clean private
resources at end after clean exit; preserve only allowlisted evidence.

## Failure semantics and remaining scope

External Docker stop deadline can SIGKILL retained incomplete work. Document
SIGTERM request plus indefinite wait/positive exit verification for clean retirement;
manual forced stop means uncertain recovery, not success. Keep graceful_shutdown:false.
Qualify local Linux/arm64 DockerDesktop27.4.0 only, not amd64/other OS, Docker host
network ingress, Kubernetes, network filesystems, power loss or production migration.
Image contains a compiled provider-free native demo only. Offline inspection,
production restore installation, raw workers/external Store clones, whole AP/parity,
M0/CP/all56 broad scenarios and full operational/core release remain open.

## Reviewed Linux test-fixture exception

The first actual Linux workspace run passed formatting and strict lint but failed
one of33 CLI library entries: a single unread60KB SSE response did not keep the
server pending. Actual socket probes showed that Linux accepted the entire response
into its kernel queue despite the configured small send buffer; nominal buffer
size does not bound accepted application bytes in this environment. Production
retirement after a successfully joined connection is valid. Do not weaken that
requirement or skip this test. The initial failing log remains retained evidence.

Permit a test-only exception in exactly two existing files:
`crates/b4-cli/src/process.rs` changes only its `#[cfg(test)]` listener wiring;
`crates/b4-cli/src/process_tests.rs` contains the private listener/IO observer and
finite workload. The observer delegates every read, write, vectored write, flush
and shutdown directly to the real Tokio TCP stream, marking actual write Pending
only after positive writes. It introduces no fake Pending result, throttled write,
production route, runtime environment control, dependency or production behavior.

The three unread-transport tests send at most32 valid SSE requests on one real
keepalive connection, with each unchanged callback output60000 bytes below the
65536 bound. They wait for observed real write Pending before signal or outer-task
fault injection. They do not claim all32 requests were admitted; they retain the
actual durable settlement check and all registered-quiescence, pending-server,
interval-rate, executor-ping, owner-exclusion, peer-close, successful-join and
child-alive reopen checks. Lost outer-task abort/panic still retain ownership and
the opaque failure after peer closure. No existing assertion, timeout or test is
removed. Private diagnostic printing is excluded from permanent source.

Independent review precedes this scoped repair. Then rerun all696 unique Rust
entries and the same12 release cases on both macOS and Linux, plus formatting and
strict lint. Record release binary hashes and independently explain any difference from the
pre-repair builds; do not infer byte identity from cfg(test) source scope. All153 other
existing crate files, both Cargo files and208 protected historical/lock files must
remain exact. The historical native receipt remains immutable; the new Linux
receipt records this source exception and fresh evidence on both platforms.

The initial equality gate was stronger than the test-only scope permits: shortening
the cfg(test) listener block by12 lines changes compiler-embedded source Location
metadata below that block. Independent private macOS A/B/C builds reproduced the
historical checksum with frozen source, the new checksum with current source, and
the historical checksum again when only private diagnostic padding restored line
positions. The repository contains no padding. Independent Linux section comparison
found identical executable code and other loaded sections except13 Location line
fields shifted by exactly12 lines and the resulting build ID. Symbol/string tables
also differ. Record actual new macOS/Linux hashes and both-platform fresh release12
checks; do not claim byte identity or unchanged panic/source-location metadata.
This measured mechanism and unchanged production code satisfy the scoped gate.

## Qualification command working directory

The qualification image inherits the builder's UID10001-owned0700 /workspace.
Actual restricted UID0 initialization failed there: GNU find could not access its
starting directory. Changing only the command working directory to / produced
successful empty-volume initialization with the existing cap-drop ALL plus CHOWN.
Every driver-created container therefore explicitly uses / and validates the
allowlisted actual Config.WorkingDir before starting or executing commands. No
helper needs the builder workspace, and all data/tool paths are absolute. Two
captured-command regressions supplement the actual Docker RED/GREEN comparison.
This adds no runtime privilege or production initializer behavior.
