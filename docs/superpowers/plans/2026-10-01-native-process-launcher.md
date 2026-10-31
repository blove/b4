# Native process launcher implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development and test-driven-development, independent
> specification review followed by quality review. The user's continuous-build
> authorization covers this increment. No git mutation, publication, new checkout
> or routine human approval is requested.

**Goal:** Add the compiled b4run executable with explicit native demo selection,
configuration validation, private SQLite assembly and positively witnessed process
drain while retaining ownership through incomplete work or transport.

**Architecture:** A new private b4-cli crate isolates process policy from b4-http.
Pure configuration/demo assembly precedes storage mutation. A synchronous runner
owns a Tokio Runtime and registers safe Unix signal receivers. It cuts host
admission before signaling Axum and keeps host/Runtime alive until actual transport
completion AND registered runtime quiescence, then drops owned captures and executor.
An outer server failure without positive connection retirement retains the process
and lock indefinitely; it is not a clean shutdown or automatic release path.

**Tech Stack:** Rust1.93.1, existing pinned Tokio1.53.1/Axum0.8.9/SQLite3.53.2;
add Tokio signal feature only in launcher and explicitly review/lock its actual
new registry transitives without upgrading existing external packages. Standard
argument/filesystem APIs; no Clap, unsafe handlers or Tokio process feature.

Design: `spec/runtime/native-process-launcher-design.md`.
Frozen backup main SHA968c4ac234beb3d78f2790dad872e2813842b472caed022afa05638cd7222707.
Private baseline `/private/tmp/b4-native-launcher-baseline.json`:457 source/evidence
files before launcher edits. Protected `/private/tmp/b4-native-launcher-protected.json`:
196 historical artifacts; live Cargo.toml/Cargo.lock intentionally excluded from
immutability so their reviewed launcher changes are not falsely reported unchanged.

## Task1: Design, scaffold and dependency boundary (root)

- [x] Obtain independent complete design reviews from host and service; incorporate
  precision before code. Review the complete implementation plan separately.
- [x] Add crates/b4-cli workspace member; create private package/library/binary
  targets with existing workspace lints and path dependencies on b4-contract,
  b4-runtime, b4-http, b4-sqlite, serde_json, axum, tokio. Add signal feature only
  to this package's tokio dependency. Dev-only rusqlite=0.40.2 bundled and
  socket2=0.6.5 reuse locked versions for records/real socket backpressure. Define
  bin name b4run, version0.1.0.
- [x] Resolve the missing signal registry dependency using a narrowly authorized
  registry fetch. Record newly added versions/checksums/licenses and inspect its
  pinned source plus existing Tokio signal integration. Compare lock graph against
  baseline: no existing external version or checksum may change. Continue locked
  offline builds thereafter. No dependency download from arbitrary sites.
- [x] Expose only narrowly needed launcher library entrypoint(s); do not create an
  embedding executor API or fixture endpoint. Establish module ownership before
  parallel authoring. Code skeletons return static Unavailable/Invalid errors so
  behavioral RED tests compile; missing-symbol compilation is not RED evidence.

## Task2: Pure configuration, demo and secure assembly (config author)

Exclusive files: new src/config.rs, src/demo.rs, src/storage.rs and focused unit
or new tests/configuration.rs/storage.rs in crates/b4-cli. Root owns manifest/lib
module wiring; process author owns process/main. Never edit existing HTTP/SQLite/
runtime source, SQL, prior tests/helpers, reference artifacts or historical receipts.

- [x] Write behavioral tests first against compiling shells. Cover serve/config-check
  mandatory --demo; --version/help requiring no env; exact recognized options;
  unknown/duplicate/missing options; nonUTF8, token/control/bounds, decimal port/wait
  boundaries; no secret/path/argument contents in Debug/Display/static errors.
- [x] Implement pure parsed Command/Config types with secret token encapsulated
  without derived Debug. Preserve env recognized values as data; default port3001,
  data.b4run-data, shutdown wait30000ms; wait1..300000ms. config-check validates
  application and filesystem preconditions readonly, without opening SQLite or
  binding. Production main prints only fixed usage/errors/success.
- [x] Assemble fixed native-demo/native-demo-v1, native/approval definitions and
  required Once policy from reviewed existing serve_native semantics. Finite Pure
  two-task native callback with ordered writes; null/object input; no provider or
  external effect, dynamic registration, fixture/control route or callback barrier.
- [x] Implement launcher-specific trusted-parent path validation, fresh0700 directory,
  fresh0600 database/owner.lock through create_new plus owned descriptor chmod;
  newly owned directory path chmod may repair restrictive umask before directory
  handle open; existing exact private nonsymlink regular files/dir refusal, including existing
  SQLite journal/WAL/SHM sidecars. Never chmod/truncate/unlink existing state.
  No recursive parent creation, root/dot/dotdot or missing parent, hostile replacement
  or raw UID claim. Readonly config-check and mutating prepare must share constraints.
- [x] Test actual fresh/restrictive-umask modes, sibling/unrelated preservation,
  existing broad/symlink/nonregular refusals, missing parent/no create, repeated
  owner exclusion, fixed errors. Use subprocess umask shell rather than unsafe
  global process umask; no ignored test helper or fabricated permissions claim.
- [x] Run scoped tests/format/strict lint; independent SPEC then QUALITY review;
  repair findings and freeze owned source/tests before final evidence.

## Task3: Owned process supervision and entrypoint (process author)

Exclusive files: new src/process.rs, src/main.rs, new src/process_tests.rs and any
new process-only cfg(test) helper. Read config/demo/storage interfaces agreed before
implementation. No shared-source edits or production environment test controls.

- [x] Write actual process supervision RED tests first using compiling runner shell,
  recording absent behavior rather than setup/build errors. Reuse real SqliteStore,
  native application, HttpHost and Axum; no fake ownership counter/store.
- [x] Main installs fixed panic hook; parses pure command; --version/help/config-check
  avoid executor/storage/socket; serve invokes synchronous runner owning Runtime.
  Use static error/status output and mapped exit codes0/1/2, never Box error dumps.
- [x] Register SIGINT and SIGTERM receivers inside owned Runtime before Store or
  request acceptance. Build/validate demo before mutation, bind listener before
  storage open (port refusal must not advance owner), then secure prepare/open,
  authenticated RequiredDurable host with strict actual Host/Origin guard.
  Move Store/application into owned host; no external clones/raw application workers.
- [x] Start server and inspect first signal/closed signal stream/unexpected server
  completion. Synchronously begin_shutdown BEFORE Axum shutdown notification.
  No early `?`/return from serving state. Emit fixed b4run_draining. Distinguish
  server success witness from failure; a non-success/JoinError lacks connection
  retirement evidence and retains process/lock indefinitely even after Quiescent.
  Record completed server state instead of repeatedly polling a ready JoinHandle.
  Inspect failures by reference and retain the entire owned failed result through
  Lost: opaque panic-payload disposal must not unwind the supervisor. Cut admission
  before first-result inspection/diagnostics; ignore diagnostic output I/O errors.
- [x] Repeated valid wait intervals retain host+Runtime+Store through Incomplete;
  repeat signals never escalate. Track server result independently. Require BOTH
  Quiescent and actual Axum server completion before dropping owned captures and
  normally dropping Runtime. No server abort, shutdown_timeout/background, implicit
  macro-main teardown or timeout→exit. Transport incompletion can remain indefinite. After Quiescent use an independent
  Tokio timer for transport-only intervals; disable completed/closed future branches
  and cache witnesses to avoid busy-spin/status floods. Qualify status-rate bounds.
- [x] After actual normal Runtime destruction emit fixed b4run_stopped; faulted
  quiescence/startup or signal infrastructure emits static error and exit1 once
  witnesses exist; lost server witness retains indefinitely with no automatic exit. Never fabricate terminal
  SSE success/cancel or rewrite durable uncertain work during drain.
- [x] Private cfg(test) parent/child selflaunch tests run identical runner with injected
  immutable callbacks and typed test-only observer. Prove real SIGINT/SIGTERM during
  held callback, repeated Incomplete, alive process, actual contender AlreadyOwned,
  async ping/pong after Incomplete (exclude blocked Runtime::drop false positive),
  repeated signal retention, release→actual durable settlement→exit→reopen.
- [x] Qualify faulted Quiescent preserves actual Unsafe InFlight evidence and no
  callback replay; held auth/policy or readiness ownership without a run permit;
  parked grant exact prompt/token/expiry/acceptance unchanged across clean shutdown;
  actual slow body/header/idle/keepalive/unread SSE as applicable retains resources
  through transport incompletion even after registered Quiescent until peer closes.
  Cover server-result/error path via actual outer-server panic/abort with a live detached
  connection if possible, or a minimal private typed seam retaining real server work;
  require async ping, locked contender and no runner-return marker. SIGKILL is
  cleanup only. Retain real production listener/host/Store work; no test endpoint
  in the shipped binary.
- [x] Failure-safe child/socket cleanup always reaps; cleanup SIGKILL is not clean
  shutdown evidence. After runner return pause child alive; parent must acquire actual owner lock
  before allowing child exit. For transport-only proof use cfg(test) accepted socket
  buffer settings (locked socket2), actual bounded unread SSE and observed Quiescent
  plus pending server, then close peer. No issued token/raw sensitive assertion
  output. Scoped tests,
  lint/format, independent SPEC then QUALITY review; repair and freeze source.

## Task4: Independent shipped-binary qualification (qualification author)

Exclusive files: new crates/b4-cli/tests/cli.rs and new tests/cli_support helper.
No production edits. Start against compiling CLI shell; root reviews SPEC and host
reviews QUALITY after final production source freezes.

- [x] Actual CARGO_BIN_EXE_b4run provider-free version/help, mandatorydemo/config-check
  success/static malformed config, no DB/owner/socket mutation (including existing
  real source epoch/rows/bytes), no Node/credentials dependency.
- [x] Actual serve startup/capabilitiesv2, exact host/origin/auth refusals, finite native
  ordered completion and required grant park. Actual second-process owner exclusion;
  configuration/bind failure must not alter existing source epoch or rows.
- [x] Observe private data modes and preserved stored state across actual OS shutdown
  and reopen. Verify shipped executable exposes no /fixture or process-control routes,
  no unknown-option echo, token/path diagnostics or capability graceful overclaim.
- [x] Qualify fresh release native binary separately from debug test binary. OS/arch
  scope recorded honestly; Linux/container qualification remains unrun unless an
  actual separate build/run is completed. No inferred deployment parity.
- [x] Scoped tests then independent SPEC/QUALITY reviews and final fresh qualification.

## Task5: Documentation, traceability and final evidence (root)

- [x] Add ADR0018 and launcher README usage with explicit native demo identity/limits,
  private path policy, token env, config-check scope, indefinite incomplete waits,
  both quiescence witnesses, repeated-signal rule and exact exit meanings.
- [x] Add named scoped launcher requirements mapped to actual permanent tests;
  update checker collector only if needed. Keep all56 broad scenarios/M0/CP gates
  open; T047/T055/operations packaging/native inspection/container incomplete.
- [x] Fresh locked offline full workspace tests, format, strict lint, retained Node50/
  Python13/current contracts; all existing examples rebuilt and freshly checked.
  Add actual release binary qualification without dependency upgrades or upstream
  reference reinstall/build. Record precise new totals and meaningful RED hashes.
- [x] Save separate native-process-launcher verification + fresh example receipts,
  current source/lock diff inventory and unchanged protected historical hashes.
  Independently audit logs/counts/process ownership/privacy/scope before freeze.
- [x] Continue authorized runtime work until actual manual action/approval is needed.

Current progress: all implementation tasks passed independent SPEC then QUALITY
review. Root QUALITY findings for diagnostic-pipe I/O and opaque JoinError payload
destruction were reproduced by actual behavioral RED tests, repaired, and approved
by final host SPEC and root QUALITY review. Fresh final workspace696/0failed/3prior
ignored, launcher45 (two ordinary helper entries), release12, Node50, Python13,
contracts274 local requirements, formatting and strict lint all pass. All nine
retained examples were rebuilt and freshly rerun. There are31 scoped launcher
requirements referencing45 unique permanent entries. Independent docs/evidence and count/privacy/dependency audits passed. The final
receipt inventory is verified separately at freeze with479 artifact hashes and
196 protected hashes. All145 prior crate files remain unchanged. Linux container
work follows this immutable receipt; continuous-build authorization remains active.
