# Linux container qualification implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and
> test-driven-development, followed by independent SPEC then QUALITY review.
> Continuous-build user authorization covers local reversible Docker work. No
> publication, deployment, Git mutation or routine approval handoff is requested.

**Goal:** Package and directly qualify unchanged native b4run on Linux/arm64 as a
nonroot process with a private persistent local volume and actual shutdown evidence.

**Architecture:** A pinned builder/qualification/runtime Dockerfile keeps build and
read-only inspection tools outside runtime. A bounded host qualification driver owns
only its labeled disposable resources, initialized empty volume and runtime-only
secret. It records allowlisted observations, exact private logical-row comparisons
and actual clean exits; it cannot treat forced cleanup as success.

**Tech Stack:** Existing Rust1.93.1/Cargo.lock, Linux/arm64 Docker27.4.0; pinned
Rust/Debian bases, builder procps+rustfmt+clippy, runtime curl, qualification-only
Python sqlite3. No new Rust dependency or production Rust/SQL/protocol change; exactly two reviewed cfg(test) fixture files are the source exception.

Design: `spec/runtime/linux-container-qualification-design.md`, reviewed design candidate SHA
39dcbac4557240cd8ca3c05a0af1e6a2b04f213493f1e4dc97cf0d06501efcc9.
Native receipt immutable SHA3242f9c103fab1a263914b329671c5ee480e6ea333f84aca9db82c4e9219031e.
Private next baseline `/private/tmp/b4-linux-container-baseline.json`:480 files;
protected `/private/tmp/b4-linux-container-protected.json`:208 historical/lock files.
All153 other existing crate files and both Cargo files must remain exact; the
reviewed Linux fixture exception below permits exactly two test-only file changes.

## Task1: Complete reviewed boundaries (root)

- [x] Independently review complete design and publish the exact approved bytes.
- [x] Freeze native receipt and capture next baseline/protected hashes before design.
- [x] Independently review this entire implementation plan; resolve findings before
  code. Assign exclusive ownership below and agree fixed image/mount/script paths.

## Task2: Pinned packaging and empty-volume initializer (packaging author)

Exclusive new files: `Dockerfile`, `.dockerignore`,
`scripts/container_initialize_volume.sh`, `scripts/test_container_initialize_volume.py`.
No edits to Rust/Cargo, existing fixtures/tests/receipts, root docs or driver files.

- [x] Write meaningful behavioral tests against a compiling/executable refusal shell
  before implementing initializer. Host nonroot unit tests qualify only safe
  refusals/validation: visible/hidden entries, invalid owner, symlink/nonregular
  root, unchanged mode/owner/bytes. A path-only copied-script seam is permitted,
  never fake UID/chown controls or production test environment. Empty acceptance
  and actual0700/10001 metadata mutation must have meaningful RED then GREEN in
  an actual isolated UID0 container against the executable refusal shell then
  implementation. A pinned Debian scaffold with the exact script readonly-bound
  may run before full Rust image assembly. Before that root probe, prove unique
  task-created labeled local volume, no remote options, exact nocopy mount and no
  other writer. Record absence-of-behavior RED, not setup/permission denial.
- [x] Implement production script fixed at `/var/lib/b4run`, fixed public diagnostics,
  with `set -eu`. Require actual nonsymlink directory/root owner0:0 and absence of
  every child including hidden entries BEFORE chmod/chown. Then chmod0700 followed
  by chown10001:10001 only the empty root and verify exact metadata. No recursive
  change, data-leaf creation or existing repair. Orchestrator separately proves exact
  task-owned local named mount/label/nocopy and no other writer; emptiness alone
  does not authorize initialization.
- [x] Add explicit .dockerignore allowlist for manifest/toolchain/crates/contracts/
  required conformance fixtures/spec and packaging/qualification scripts; exclude
  .git/target/node_modules/env/private caches/data/SQLite and private logs/evidence.
  Use explicit COPY paths; runtime stage copies no fixture or source tree.
- [x] Dockerfile pins these official immutable base indices:
  `rust:1.93.1-slim-bookworm@sha256:5b9332190bb3b9ece73b810cd1f1e9f06343b294ce184bcb067f0747d7d333ea`
  and `debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251`.
  Builder root installs procps, exact1.93.1 rustfmt/clippy and verifies cc/ar/bin/kill.
  Create UID/GID10001 with owned workspace/Cargo cache; COPY --chown then USER10001
  before locked fetch/build/qualification. Preserve global toolchain readonly.
- [x] Fetch locked crates without copying host credentials/cache; subsequent checks
  offline. Build release native binary. Qualification stage retains source/Cargo and
  adds Python+SQLite only for the separate readonly snapshot utility, installed at
  /opt/b4/container_snapshot.py, with /opt/b4/container_initialize_volume.sh for
  isolated initialization. Neither script is copied to runtime. Runtime stage
  installs curl and libs, copies only release b4run, USER10001:10001, exec ENTRYPOINT `/usr/local/bin/b4run`, CMD serve --demo,
  STOPSIGNAL SIGTERM, fixed port3001/data/var/lib/b4run/data and private image parent.
  No token ARG/ENV, VOLUME, EXPOSE, Node, Cargo, compiler or instrumentation.
- [x] Exec HEALTHCHECK uses curl --disable FIRST, --fail --silent --output /dev/null
  --noproxy '*', finite connect/max time and outer probe timeout, URL exact
  http://127.0.0.1:3001/readyz. Fixed-port override outside qualified profile.
- [x] Scoped initializer tests GREEN, shell syntax and packaging static inspection;
  independent SPEC then QUALITY review, fix and freeze exclusive files.
- [x] Build actual linux/arm64 qualification and runtime images, local tags only:
  `docker build --platform linux/arm64 --target qualification -t b4run-linux-qualification:local .`
  and `docker build --platform linux/arm64 --target runtime -t b4run-linux-runtime:local .`.
  Retain private build logs, actual base child digests and resulting content imageIDs;
  no image push. Apt packages are not frozen by base digest: record actual installed
  versions/dpkg verification and final image IDs, no reproducibility claim.

## Task3: Independent qualification driver and readonly companion (qualification author)

Exclusive new files: `scripts/qualify_linux_container.py`,
`scripts/container_snapshot.py`, `scripts/test_qualify_linux_container.py`.
No Rust/Cargo/docs/packaging edits. Coordinate fixed image/script interfaces with root.

- [x] Write meaningful unit RED tests first for orchestration boundaries using a
  narrow captured subprocess runner: reject foreign volume labels/nonlocal driver/
  options/wrong mount/nocopy/broad root before any mutator; allowlist inspect fields;
  never include token/env/grant/raw-response in failures or public evidence;
  positive retirement requires exit0+stopped+notOOM; timed-out/failed/forced cleanup
  is failure. Fixture tests do not qualify actual Docker/SQLite behavior.
- [x] Implement finite per-command/overall driver waits, static failure codes, private
 0600 secret/env/temp files, token-generated in memory, in-container curl config via
  stdin. Capture only explicit approved image/process/mount/health fields, never raw
  inspect/env then redact. Track created resources immediately; cleanup only own
  exact IDs/labels. Record failure before bounded force kill/reap; force cleanup is
  never success. No prune or deletion of unrelated resources/volumes.
- [x] Implement readonly companion fixed `/var/lib/b4run/data/runtime.sqlite3`,
  Python sqlite3 URI mode=ro for LIVE sources, query_only with readback,
  isolation_level=None then explicit BEGIN DEFERRED across exact
  six schema3 tables including metadata, primary-key row order and canonical lossless
  tagged NULL/INTEGER/TEXT/BLOB framing; preserve raw JSON TEXT byte-for-byte.
  First read establishes snapshot; materialize before rollback/close promptly.
  Capture current owner epoch and real snapshots/grant/acceptance/journal privately;
  missing DB refuses without creation; unit tests must use actual local SQLite WAL
  commits interleaved between table reads to prove coherent readonly observations.
  preserve full row identity/order. Compare full exact private rows; public summaries expose counts/booleans only, no
  authority-derived snapshot digest or row diffs. Readonly means no logical DB
  mutation; do not claim WAL/SHM byte preservation. Compare only documented metadata epoch increment
  by EXACTLYone on reopen (unsigned big-endian8 bytes), original execution
  owner/authority/snapshot all exact. No mutation,
  Store open/migration/recovery, live-main-file copy or shipped inspection API.
  Run companion10001 with read-only root and same named-volume readonly/nocopy;
  invoke /opt/b4/container_snapshot.py via private captured pipes, never publish
  raw rows as container PID1 logs. Distinct stopped-only immutable=1 is allowed
  only after positive exit of every task-volume writer and no open observers;
  independently refuse it if WAL/SHM/journal exists. Unit tests qualify stopped
  clean-read and sidecar refusal. Never use immutable on live/unclean DB or keep
  a reader through retirement merely to preserve sidecars.
- [x] Actual driver first checks Linux image OS/arch, configured and effective UID10001,
  PID1 executable, no Node/Cargo/Rust, actual ldd dependencies/package versions and
  executable hash. Version/help and config-check must run without a Node/provider;
  readonly config-check must not create/migrate/advance DB state.
- [x] Create unique labeled local volume absent before creation; verify exact local
  driver/options/labels and mount before separate initializer. Run qualification image initializer at
  /opt/b4/container_initialize_volume.sh with
  readonly root, network none, cap-drop ALL+cap-add CHOWN, no other writer. Validate
  parent exact0700/UID10001 BEFORE runtime start; this guard belongs to packaging
  preflight, not an invented frozen launcher parent-mode guard. Qualify actual empty
  initialization and nonempty/hidden refusal without mutation on disposable volumes.
- [x] Start runtime image10001, exec PID1, network none, readonly root and explicit
  RW named volume-nocopy. Check actual private leaf/files/sidecars, Docker healthy,
  /healthz and /readyz public responses and failing HTTP-error health probe semantics.
  Qualify exactHost/noOrigin/Bearer refusals, capabilitiesv2 graceful=false, no fixture/
  control route, actual native ordered completion and required Once parked grant.
  Never write real response/issued authority to public logs.
- [x] Take coherent private rows while first owner live. Actual second runtime on same
  volume refuses StorageOwned; rows and epoch remain exact. Existing config-check
  while live remains readonly. Registered prepared task is retained without falsely
  expecting its writes joined while whole batch is parked.
- [x] Deliver actual SIGTERM to PID1, poll successful stopped exit0/notOOM (no docker
  stop deadline as proof), then compare exact rows. Recreate separate container on
  SAME volume and verify epoch-only metadata change, exact original grant prompt/
  token/TTL/acceptance/snapshot/journal. Authenticated readonly prior-owner GET/SSE
  pending projection returns original authority and null done without adoption or
  callback replay; ordinary prior-owner pending retains existing501 behavior.
- [x] Deliver SIGINT for second positive exit and verify reusable volume and exact
  logical state. The stronger held work/unread SSE/lost-server physical cleanup proof
  is separately executed by Linux process suites with the reviewed test-only fixture repair; no Docker signal count
  or external SIGKILL fabricated clean-shutdown claim. Finish own resource cleanup.
- [x] Unit GREEN, independent SPEC then QUALITY review; freeze new scripts. Run actual
  shipped image qualification and save allowlisted private receipt with each named
  case/result/identity, not invented unit-test or broad release scenario counts.

## Task4: Actual Linux source/release checks (root and authors)

- [x] Run actual qualification image as10001, network none for already-fetched tests,
  writable owned workspace/temp; no privileged/root test runner. Save UID/OS/arch:
  `cargo fmt --all -- --check`
  `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
  `cargo test --workspace --all-targets --locked --offline`
  `B4RUN_QUALIFICATION_BINARY=/workspace/target/release/b4run cargo test -p b4-cli --test cli --locked --offline`.
  Expected actual696 unique Rust entries/0failed/3 prior ignored; exclude filtered
  child-helper summaries. The release12 repeat same12, no new Rust tests added.
- [x] Any actual platform failure triggers systematic diagnosis, review and scoped
  repair. If unchanged Rust requires a source repair, stop current evidence claims,
  revise design/plan/source scope explicitly and requalify native plus Linux before
  receipt freeze. Never silently rewrite a frozen prior receipt or skip Linux tests.

## Task5: Traceability and immutable evidence (root)

- [x] Add ADR0019, README/runbook exact commands for local fixed-loopback profile,
  safe fresh-volume initialization, private runtime-only token, same-volume reuse,
  explicit operator parent preflight and direct signal/positive clean exit. Explain
  Docker deadline forced kill as uncertainty, no public ingress or secret-daemon
  isolation claim. New doc `spec/runtime/linux-container-qualification.md`.
- [x] Add named scoped container requirements mapping permanent unit/actual driver
  functions and executed Linux existing Rust tests. Extend collector only one section.
  M5 remainspartial with offline inspection/full operations, all56/M0/CP gates open.
- [x] Fresh formatting/strict lint/current Node50/Python13+new unit entries/contracts;
  no upstream reinstall/capture/dependency upgrade. Independently inspect frozen
  native main/examples,208 protected and all153 other crate files/both Cargo hashes, with the two fixture changes explicitly recorded.
- [x] Save separate linux-container verification with actual build/run logs+hashes,
  base index/children/runtime+qualification image IDs/package inventory/releaseSHA,
  nonroot/modes/owner/signal/persistent-row/probe/secret boundaries, actual counts,
  fresh scoped unit RED/GREEN and limitations. No token/env/grant/raw data in receipt.
- [x] Independent complete evidence/privacy/count/hash audit; repair and freeze.
- [x] Select readonly local inspection as the next authorized implementation; no
  routine end-of-increment permission request or user action is currently needed.

Progress: packaging, initializer, qualification helper/driver and the scoped two-file
Linux fixture repair are implemented and independently reviewed. Fresh macOS and
nonroot Linux696 checks plus same12 release cases pass. The actual13 Docker cases
now pass and all task resources are cleaned up. Initial Linux/Docker adapter
failures and meaningful RED/GREEN repairs are preserved. Documentation, traceability and the complete independent receipt audit pass.
Freeze this scoped result before publishing the next inspection design and plan.
No image publication or deployment is requested.

## Linux fixture repair after actual platform failure

The initial actual Linux workspace run failed only the unread SSE fixture because
the kernel queued its single60KB response. Preserve
`/private/tmp/b4-container-linux-workspace-tests-initial-failure.log` and the private
probe/candidate diagnostic; neither is a Linux pass. The private finite-pipeline
candidate has demonstrated real TCP write Pending and all10 process cases on
Linux, sequentially and in parallel; these experiments do not replace final checks.

Exclusive repair author: process author. Allowed files only
`crates/b4-cli/src/process.rs` (`#[cfg(test)]` listener wiring) and
`crates/b4-cli/src/process_tests.rs` (delegated observer + finite32 keepalive SSE
requests + real Pending waits in three tests). Production code, dependencies,
assertions, callback output bounds and timeouts remain unchanged. Remove private
accepted-buffer/byte-count printing from the permanent candidate.

- [x] Independent SPEC/QUALITY review of measured diagnosis and candidate.
- [x] Apply only the reviewed two-file test exception and format it.
- [x] Root SPEC and independent QUALITY review of permanent source.
- [x] Fresh macOS formatting/strict lint/all696 and same12 release cases.
- [x] Rebuild Linux qualification source image and run formatting/strict lint/all696
      and same12 release cases as UID10001 with no network.
- [x] Verify production executable-code scope and independently account for the measured
      source-location metadata change; record actual new release hashes. All153
      other crate files/both Cargo and208 protected files exact; historical receipts
      unchanged. Record two changed source hashes and initial failure honestly.

Package verification must distinguish expected slim documentation/locale/info/
lintian/apt-list omissions from unexpected missing paths or checksum mismatch.
Known omissions are not a pristine-package or security-audit claim. Qualifier units
require meaningful RED/GREEN for this actual-image boundary before the Docker run.

The pre-repair binary-equality assertion was experimentally disproved: the cfg(test)
block shifts downstream embedded source locations by12 lines. Private same-path
macOS A/B/C builds reproduced both historical/current checksums and historical
equality when line positions alone were restored. Linux executable sections are
exact;13 Location line fields and resulting build-id/nonloaded symbol tables differ.
Root independently verified the binary mechanism. The amended gate records actual
new hashes and fresh both-platform696/release12, with production code/other153
sources/Cargo/protected208 unchanged. No repository padding or historical receipt
rewrite is allowed. This is a scope correction supported by measured artifacts, not
a claim of byte-identical or unchanged diagnostic-location metadata.

## Actual Docker adapter corrections

Actual execution exposed absent optional Go projection fields, default PID1 signal
handling in auxiliary sleep processes, a missing false mount flag, the restricted
initializer's inaccessible inherited workspace and the existing SSE cache header.
Each correction has preserved actual/static RED and reviewed GREEN evidence.
Optional projections retain strict required fields; observers handle signals;
all commands use / without added capabilities; the SSE check now requires the
unchanged exact no-store, no-transform header. No Rust production/HTTP contract
change or forced cleanup is counted as runtime retirement.
