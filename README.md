# B4run in Rust

The native Rust foundation for B4run: portable contracts, a pure execution kernel,
transactional memory/SQLite stores, and an authenticated HTTP host for immutable
native Rust applications and deterministic scripted effects. This
implements a bounded subset of the
[runtime RFC](spec/runtime/0001-agent-protocol-core.md), with M0 characterization
still open. Required grants and atomic Once/Deny continuation now run through an
explicit authenticated HTTP host as well as the embedded durable service. A shared native driver executes real bounded parallel callbacks. The
full core profile remains open.
T-001, T-002, T-003, T-004, T-005, T-006, T-007, T-008, T-009, T-010, T-011, T-012, T-013, T-014, T-015, T-016, T-017 and T-018 are native-qualified for create/read thread metadata and timestamps,
empty-thread state unavailability, authorized implicit thread creation, unknown/missing-kind
route rejection without execution, malformed JSON AP errors without execution, and omitted/null/false
input shape preservation over real TCP and owned SQLite across reopen, plus same-thread
concurrent admission with one winner and different-thread concurrent execution within global limits
over real TCP and owned SQLite, plus successful wait returning direct output and JSON null
for void output, plus one representative opt-in ordinary primary native RunStream
case with direct tool/chunk/result/done payload framing, with successful tool_result after
independently observed SQLite commit, plus owner reopen; four selected normal
stream terminal cases; two fixed admitted stream/wait disconnect completion cases; and read-only live/settled
attachment starting with complete state without execution; one controlled attachment
cut with state A and live B exactly once; and one selected cumulative native frame
overflow with a new read-only null/truncated attachment and authoritative terminal;
one authorized attached-viewer capacity refusal and one attached-viewer byte-queue
overflow with a replacement viewer receiving the same durable terminal;
one selected real TCP quiet heartbeat and Unicode stream invariant under every byte cut;
and one required-durable approval park committed before TCP interrupt and authorized pending
disclosure, with separate grant/journal precommit faults disclosing neither prompt nor grant;
the other 38 release scenarios (T-019–T-056) remain pending.
M0, CP-010, COMPAT and the full core profile remain open; CP-001–009 remain partial.
See the [scoped T-001 evidence](conformance/release/t001-thread-roundtrip/README.md)
[T-002 qualification](conformance/release/t002-empty-thread-state/qualification.json)
[T-003 qualification](conformance/release/t003-authorized-implicit-thread/qualification.json)
[T-004 qualification](conformance/release/t004-invalid-routes/qualification.json)
[T-005 qualification](conformance/release/t005-malformed-json/qualification.json)
[T-006 qualification](conformance/release/t006-input-shapes/qualification.json)
[T-007 qualification](conformance/release/t007-same-thread-admission/qualification.json)
[T-008 qualification](conformance/release/t008-different-thread-concurrency/qualification.json)
[T-009 qualification](conformance/release/t009-wait-output/qualification.json)
[T-010 qualification](conformance/release/t010-native-stream-payloads/qualification.json)
[T-011 qualification](conformance/release/t011-normal-stream-terminal/qualification.json)
[T-012 qualification](conformance/release/t012-disconnect-completion/qualification.json)
[T-013 qualification](conformance/release/t013-attachment-state/qualification.json)
[T-014 qualification](conformance/release/t014-attachment-cut/qualification.json)
[T-015 qualification](conformance/release/t015-attachment-truncation/qualification.json)
[T-016 qualification](conformance/release/t016-viewer-overflow/qualification.json)
the [T-017 qualification](conformance/release/t017-heartbeat-splits/qualification.json)
and [T-018 qualification](conformance/release/t018-durable-disclosure/qualification.json).
Metadata guard tests use authored temporary receipts and carry no native runtime credit;
The eighteen scoped native proofs retain independent source descriptors and test hashes; regression checks do not
requalify predecessors. Historical increment receipts retain their original scope and counts.
T-010 uses new opt-in streaming entry points; existing constructors, same-host Wait and excluded
purposes retain legacy behavior. Its fresh full workspace evidence belongs to the separate feature
source map before the wire proof and metadata, adds no final runtime credit, and makes no full-suite
claim on final metadata bytes. The scoped proof does not qualify all terminal outcomes, replay,
providers, TypeScript or the full core profile.

T-011 covers four selected complete healthy primary native streams: structured Value
uses the literal `done.output` value, null Value uses `{"output":null}`, Void uses
`{}`, and known failure uses `done.output.error`. Each ends with exactly one `done`
as its last data frame. Actual owner retirement and one physical SQLite reopen
verify settled state through GET-only reads, without replay. This bounded proof
does not cover every terminal outcome, cancellation, disconnect, parked turns,
attachment, providers, TypeScript or full core qualification. It adds no new full
workspace or feature-validation claim; metadata fixtures carry no native credit.

T-012 covers two fixed real TCP cases on owned SQLite: the whole local stream
or wait client closes after actual callback entry while the callback is held and
has returned zero times. The stream case enters the streaming callback; wait
enters the controlled callback. Cancellation observers stay clear. Explicit release
allows each admitted callback to return once, with durable Succeeded/Completed
state and the direct result distinct from aggregate state. Full typed and raw
7/5/9 histories agree. Healthy actual worker retirement and physical sole-owner
closure precede one fresh generation+1 owner with GET-only verification, without
replay, followed by cleanup. This bounded proof does not establish result delivery
to the closed client, server-observed FIN timing, arbitrary network failures,
cancellation, attachment, idempotency, graceful listener shutdown or full core
qualification. It changes no production behavior and adds no new full workspace
or feature-validation claim; metadata fixtures carry no native runtime credit.

T-013 covers one controlled RunWait callback held after entry while a read-only
live attachment over real TCP yields a complete first `state` before gate release.
The first live frame has exact busy state and no preceding retry control;
release produces the independent direct wait result and a single live `done`
with that exact output through EOF. After durable success and actual worker
retirement, a settled attachment starts with exact complete idle state and yields
retry 2000 then `done.output:null` through EOF. Full typed and raw 7/5/9 history,
including creation rows, and validation/entry/return/attempt counts remain
unchanged by attachment. The actual sole SQLite owner is destroyed and exactly
one owner+1 generation reopens for GET-only observations without replay.
This bounded proof adds no boundary-race, overflow, arbitrary-split, cancellation,
provider, TypeScript, full-workspace, feature-validation, full core or COMPAT
qualification; metadata fixtures carry no native runtime credit.

T-014 covers one selected controlled attachment cut with a real streaming
callback. The actual inner SQLite owned read-return capture gate holds after
releasing the SQL lock; A is Accepted while that capture is held. The real
coordinator then subscribes, and B is Accepted while the first exact Attach
policy call is held. Before the final callback release, the complete independent
attached `state.turn` is positively parsed as `[trusted tool_call, A]`, followed
by live B. The primary tool_call/A/B/tool_result/done and attached
state/B/tool_result/done streams are exact through EOF; A and B occur exactly
once on their selected sides. Held durable history stays unchanged. Independent
task result, aggregate state and context agree with full typed and raw 7/5/9
histories including creation. Healthy worker retirement and destruction of the
actual sole inner SQLite owner precede exactly one owner+1 fresh host with
GET-only preservation checks and no replay.
Committed storage observed before final wire consumption establishes observation
order; it does not independently instrument publication after commit. T-010's
separate ordering proof supplies no new T-014 credit. This controlled partition
adds no universal race, overflow, capacity, arbitrary-split, cancellation,
provider, TypeScript, graceful listener shutdown, full-workspace, full core or
COMPAT qualification, and no new feature-validation or transferred runtime
credit. Metadata fixtures carry no native runtime credit.

T-015 covers one selected cumulative native frame overflow. After trusted
`tool_call` and chunk A fill the two-frame projection, chunk B returns `Limit`
and later chunk C returns `Closed`. The producer stays closed and the original
viewer closes with its earlier complete frames. While owned work remains active,
a new read-only attachment starts with `turn:null` and
`turn_truncated:true`, then receives the authoritative terminal after the
callback settles. The proof does not generalize to every projection failure,
byte/depth or viewer-capacity limit, arbitrary network split, cancellation,
provider, TypeScript, full-workspace, feature-validation, full core or COMPAT
qualification. Metadata fixtures and the separate pre-fix and initial runs add
no final runtime credit.

T-016 covers one authorized attached-viewer capacity refusal and one deterministic
attached-viewer byte-queue overflow during the same native RunStream execution.
The primary client closes after its trusted `tool_call`; the refused viewer receives
only `detached {"reason":"capacity"}` and EOF. The first attachment receives
`detached {"reason":"overflow"}` after its state, while native publication remains
accepted and durable work stays Running/InFlight. A replacement attachment sees
the complete live digest, then exact `tool_result` and `done` frames through EOF;
an independent durable read confirms the same successful result. This selected
proof does not qualify primary-viewer overflow, frame-count overflow, all queue
sizes, arbitrary network backpressure, denied or stale authorization, cancellation,
providers, TypeScript, feature parity, full-workspace behavior, full core, M0,
CP-010 or COMPAT. Its initial proof and metadata fixtures carry no final runtime
credit.


T-017 covers one healthy native POST RunStream over real loopback TCP and owned
SQLite. A complete `: ping\n\n` comment arrives while the callback is held
after trusted `tool_call` and before its frozen Unicode `chunk`. One independent
bounded HTTP/chunk/SSE/UTF-8 parser consumes the full raw response, then replays
that same transcript across every single byte cut and bytewise delivery. Exact
application frames are `tool_call`, `chunk`, `tool_result`, and one terminal `done`;
durable result and owner+1 GET-only reopen agree. This fixed healthy transcript
does not prove arbitrary network schedules, other payloads, proxies, compression,
TypeScript clients, current-HEAD COMPAT, M0, CP-010 or full-core qualification.
The initial proof and metadata fixtures add no final runtime credit.

T-018 covers one required-durable approval park over real loopback TCP and owned
SQLite. Separate grant-insert and park-journal-insert precommit faults each leave
committed admission/Running evidence but disclose no prompt or grant; the accepted
stream closes without `interrupt` or `done`, and authorized pending reads return
fault-gated 503. In the healthy case, a complete `interrupt` and an authorized
pending response disclose a grant that an independent SQLite read confirms was
committed with the matching park snapshot and journal. A hard-killed server child
is reaped before owner+1 reopen confirms the same durable park and grant. This
fixed case does not qualify arbitrary crash points or recovery, post-restart HTTP
disclosure, all grant modes, resume, current-HEAD COMPAT, M0, CP-001–010, full
core or whole-product parity. Initial proof and metadata fixtures carry no final
runtime credit.

The [runnable text-input probe](conformance/permissions/http-text-input-example/README.md) exercises original Always approval and saved reuse with distinct 2 KiB note fields.

The [bounded text-input qualification](conformance/permissions/text-original-inputs/README.md) records the original Always and saved-reuse path with representative 2 KiB text fields, including an oversized-input refusal.

New to the local Rust example? Follow the [first saved permission quickstart](docs/quickstart.md)
to build it, approve a harmless report task, and reuse its permission after restart.

Two additional [lifecycle captures](conformance/reference/recordings/2026-10-01-lifecycle/README.md)
each exercise 77 actual TypeScript requests for cancellation, deletion, state and
attachment. Their limited ordered projections repeat, including the interval
between cancellation response and effect quiescence. CP-001–009 are partial;
CP-010 and the broader core gate remain open.

The seven private crates have distinct responsibilities:

| Crate | Responsibility |
| --- | --- |
| `b4-contract` | Validated identities, versioned snapshots, strict run/resume and capabilities DTOs, AP types and SSE framing |
| `b4-kernel` | Sequential batches of parallel tasks, approval continuation, ordered joins, cancellation and recovery decisions |
| `b4-testkit` | Scripted effects, independently authored native scenarios, a scoped conformance receipt |
| `b4-runtime` | Immutable native application registry, fenced transactions, shared parallel driver and grant-aware approval service |
| `b4-sqlite` | Durable snapshots/journal/approvals, exclusive ownership and exact schema migrations |
| `b4-http` | Authenticated native/scripted operations, protected capabilities, approvals, fenced deletion, owned cancellation and live SSE |
| `b4-cli` | Standalone native demo launcher, configuration validation, stopped readonly inspection and owned process supervision |

The kernel receives time, identifiers and effect results from the caller. It
returns a proposed commit plan with an expected revision. Its `dispatchable` list
contains ready candidates. A host must authorize each candidate; the runtime
proposes a separate `Dispatch` command with a new attempt identity, and persists
the resulting `InFlight` state before invoking its effect. No kernel function
opens a database, reads an ambient clock, or contacts a provider.

## Run the native demonstration

The private `b4-cli` package builds a standalone `b4run` executable. Explicit
`--demo` selects the compiled `native-demo` application; there is no provider or
Node runtime dependency. The [native launcher evidence](conformance/expected/native-process-launcher-verification.json)
records 696 passing Rust test entries, including 45 launcher entries, plus 12
separate checks of the release executable on macOS arm64. See the
[reviewed design](spec/runtime/native-process-launcher-design.md).

```sh
cargo build --release --locked --bin b4run
export B4RUN_TOKEN='your-local-secret'
export B4RUN_DATA_DIR="$PWD/.b4run-data"
./target/release/b4run config-check --demo
./target/release/b4run serve --demo
```

The default address is `127.0.0.1:3001`; `B4RUN_PORT` selects a decimal port, with
zero selecting an available local port. The executable announces the actual
address. The immutable routes are `/native#workflow` (two finite Pure callbacks
with ordered items) and `/approve#workflow` (prepare and required Once approval).
Protected requests require the exact bearer token and actual address as Host;
any Origin header is refused. Demo input may be an object or explicit null.
The required approval prompt uses the existing five-minute TTL and durable ledger.

`--version` and `--help` read no secret or storage configuration. `config-check`
validates recognized configuration, application and existing filesystem
preconditions without binding, opening/migrating SQLite, advancing ownership or
creating files. It does not establish database readiness. The data directory's
parent must exist. An existing data directory must be a real private Unix
0700 directory; existing database/owner/sidecar files must be private regular
0600 files, with no symlink or automatic permission repair. Fresh files are
reserved privately before SQLite opens. Failed startup can leave private empty
files/directories. These constraints assume a trusted stable local parent.

SIGINT or SIGTERM first closes admission and requests transport shutdown.
`B4RUN_SHUTDOWN_WAIT_MS` selects observation intervals from1 through300000ms
(default30000ms). Incomplete waits retain the executor, host and SQLite owner
lock; repeated signals never request forced exit. Slow callbacks or peers can
keep this initial process draining indefinitely. Clean retirement requires both
registered-work quiescence and successful Axum connection completion. A lost
server-completion witness also retains the process indefinitely until external
termination. No terminal success/cancellation is invented by teardown.

Exit0 indicates clean witnessed retirement; usage/configuration or private-storage
precondition errors return2;
startup infrastructure errors or faulted retirement return1. Diagnostics use
fixed codes and the public listening address; closed diagnostic pipes cannot
unwind supervision. Capability v2 retains
`supervised_admission_drain:true` and `graceful_shutdown:false`. Full graceful
shutdown, production deployment and the core release profile
remain open; the separate local Linux container scope is recorded below. See [ADR0018](spec/runtime/decisions/0018-native-process-launcher.md).

## Linux container profile

The local Linux/arm64 container profile passes nonroot identity, private-volume
initialization and permissions, health checks, owner exclusion, exact persisted
state across restart, and positive SIGTERM/SIGINT retirement. All696 Rust tests and
the same12 release-binary cases pass on both macOS and Linux. The13 actual Docker
cases are recorded separately from unit tests and the frozen macOS launcher
receipt. See the [verification receipt](conformance/expected/linux-container-verification.json),
[container runbook](spec/runtime/linux-container-qualification.md) and
[design](spec/runtime/linux-container-qualification-design.md). The API stays on
inner loopback with an explicitly owned private local volume. Deployment, other
architectures and the full core release remain open. Stopped operator inspection is described below.

## Inspect a stopped store

`b4run inspect` emits a bounded redacted JSON summary of an existing clean, stopped
SQLite store. `--blocked` adds complete report-local rows for recorded unfinished
or uncertain executions. Only the data-directory setting is read:

```sh
B4RUN_DATA_DIR="$PWD/.b4run-data" ./target/release/b4run inspect
B4RUN_DATA_DIR="$PWD/.b4run-data" ./target/release/b4run inspect --blocked
```

The directory must already be real0700, with regular nonsymlink0600 database and
owner-lock files and no WAL/SHM/journal entries. Inspection acquires the existing
owner lock without starting or migrating the Store, validates exact schema3 and
bounded Snapshot1 records, then closes and unlocks before writing the report.
It creates no listener and needs no token or demo selection. Never remove
sidecars or repair permissions to make inspection succeed. Trusted stable local
parents and cooperating writers on the same lock inode are required.

The stopped-inspection increment passed769 Rust entries per platform and the
12 existing plus19 inspection release cases. The73 new Rust entries include one
reader child helper;3 historical ignored helpers remain. Python71 and Node50 pass.
Results are recorded in the [inspection receipt](conformance/expected/stopped-readonly-inspection-verification.json).

Counters cover stored execution history, initialized tasks and complete declared
policies. Report-local ordinals are not recovery targets. No IDs, epochs, paths,
prompts, values, tokens or source digests appear. Successful inspection does not
validate every authority/history payload or authorize replay. Larger sources
refuse without truncation; cooperative work limits do not promise hard I/O time
or total memory bounds. See the [operator runbook](spec/runtime/stopped-readonly-inspection.md),
[separate report schema](spec/runtime/stopped-inspection-report.schema.json) and
[ADR0020](spec/runtime/decisions/0020-stopped-readonly-inspection.md).

## Request references and embedding records

Native fallback responses now carry `x-b4-request-id` when a server reference is
available, including probes and refusals. The reference preserves existing
response bodies/status and supplies no authentication, execution identity or
recovery authority. Its host namespace and counter allow same-host linkage.

An embedding can clone `host.diagnostics()` and call `drain()` for bounded
redacted JSON bytes. The fixed256-record ring retains only the generated
reference, four-way method label, response-ready or pending-future-drop event,
status and elapsed milliseconds. It uses best-effort insertion and destructive
drains, with persistent loss flags and cumulative evictions. Response-ready
precedes body/SSE delivery; future drop cannot establish cancellation or physical
retirement. Handles and encoded batches retain no Store or shutdown resources.

The launcher supplies response references but does not automatically deliver the
buffer as logs. Full identity/lifecycle diagnostics and production delivery remain
open. The recorded diagnostics macOS/Linux arm64 checks pass814 Rust entries per platform with3
historical ignored helpers, plus33 release-executable cases each. Python88,
Node50 and338 named contract requirements pass; the13 retained local container
lifecycle cases also pass on rebuilt images. The
[verification receipt](conformance/expected/request-diagnostics-verification.json)
records the scoped results; the independent final freeze decision is recorded separately. See the
[embedding runbook](spec/runtime/request-diagnostics.md),
[separate batch schema](spec/runtime/request-diagnostic-batch.schema.json) and
[ADR0021](spec/runtime/decisions/0021-request-correlation-and-bounded-diagnostics.md).

## Run the checks

Install Rust through rustup. `rust-toolchain.toml` selects Rust 1.93.1 with rustfmt
and Clippy; `Cargo.lock` fixes the dependency graph.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo run --locked -p b4-testkit --bin b4-conformance -- --profile foundation
```

The foundation command exercises an approval snapshot round trip while retaining
a completed sibling, and a recovery decision that blocks an uncertain unsafe
effect. Its JSON receipt is scoped to those scenarios. Snapshot serialization is
not evidence of process restart or SQLite durability. A missing scripted effect
fails without a live-provider fallback.

The same checks run with `--offline` after dependencies are cached. Neither the
libraries nor the conformance executable requires Node.js.

Validate the draft JSON Schemas, examples and requirement inventory separately:

```sh
python3 -m venv .contract-tools
.contract-tools/bin/python -m pip install -r requirements-dev.txt
.contract-tools/bin/python scripts/check_contracts.py
```

These Python packages are development tools; they are not runtime dependencies.

## Current boundaries

Run requests preserve omitted input as `{}` and explicit null/scalar/array input
as supplied. Resume entries reject unknown fields and legacy shapes. The native
wire drafts also make restrictions that require review during characterization;
see [compatibility](spec/runtime/compatibility.md).

Approval decisions entering the kernel must already be authorized and validated
by a host. The kernel does not authenticate callers, validate or consume grants,
or persist `always` permissions. Recovery classes are application declarations;
an external idempotency contract still needs qualification. Unknown unsafe and
reconcilable effects block rather than replay automatically.

[Reference evidence](conformance/reference/README.md) captures the pinned
TypeScript source and hashes. The existing TypeScript checkout is `~/repos/dawn`
(the project was renamed to b4run); its Git history contains the pinned baseline.
The Rust workspace remains here in `~/repos/b4`.
The pinned TypeScript runtime has been built and independently executed against
62 workflow requests, with raw responses retained and normalized results repeated
on a second boot. A selected Rust comparison covers 24 input/void/falsy and known
failure wait and POST stream cases. This evidence is narrower than core-profile compatibility:
the TypeScript recordings use its Fetch handler, while native TCP tests separately
exercise the service. The complete TypeScript monorepo build did not pass; three
web builds could not bind helper ports. Its selected CLI/runtime graph built
successfully. See [compatibility](spec/runtime/compatibility.md) for precise scope.

Ten operations in [OpenAPI](contracts/agent-protocol/openapi.yaml) run in the
scripted host, including bounded GET attachment and authenticated approval resume
for explicit approval hosts.

## Try authenticated HTTP

The example requires an explicit bearer token and stores threads/executions in
SQLite. It listens on `127.0.0.1:3001`, accepts that exact Host, and rejects browser
Origin headers. Choose a token locally and export it as `B4RUN_TOKEN` before
starting the example.

```sh
export B4RUN_TOKEN='replace-with-your-local-token'
B4RUN_DATA_DIR=/tmp/b4run-http cargo run --locked -p b4-http --example serve
```

In another terminal with the same token, run a workflow and stream a falsy result:

```sh
curl -sS -H "Authorization: Bearer $B4RUN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"route":"/probe#workflow","input":{"message":"hello"}}' \
  http://127.0.0.1:3001/threads/example-input/runs/wait
curl -N -H "Authorization: Bearer $B4RUN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"route":"/false#workflow"}' \
  http://127.0.0.1:3001/threads/example-false/runs/stream
```

The wait response is `{"input":{"message":"hello"},"kind":"object"}`;
the stream ends with `event: done` and `data: {"output":false}`.
The example also exposes `/zero#workflow`, `/empty#workflow` and `/void#workflow`.
`/delay#workflow` runs a two-second Pure script for observing cancellation.
Void wait returns `null`; void POST stream sends `done` with `{}`. A missing thread
is created only after authentication, policy and admission-capacity checks.
Read it with `GET /threads/example-input`, its separate AP projection with
`GET /threads/example-input/state`, or the empty no-approval pending set with
`GET /threads/example-input/pending_interrupts`. `/healthz` and `/readyz` are probes.

The reusable host requires an authenticator and resource policy at construction.
Ordinary `HttpHost::new` accepts finite pure scripted definitions and rejects
approval-bearing definitions; `new_with_approvals` enables required grants on the
same store. Both preserve workers/store ownership after viewer disconnection. User metadata
cannot supply ownership. Store or worker faults make readiness fail and cannot
publish a successful terminal. The host is a bounded development increment;
automatic recovery scanning, full restart/shutdown qualification and arbitrary
application effects through HTTP remain open. Trusted known-ID restart continuation
is qualified within the scope described below.

`POST /threads/{id}/cancel` captures the locally owned turn before resource
policy, renews authority and atomically records cancellation. It returns
`{"status":"interrupted","thread_id":"..."}`. The original wait receives
409 `run_cancelled`; a stream receives one `done` with
`{"output":{"cancelled":true}}`. The worker retains its capacity, thread claim
and store through actual effect quiescence. Consumed approval receipts survive,
late outcomes remain evidence, and cancelled writes do not change state. Stable
approval parks and prior-owner work without a local worker return
409 `no_run_in_flight`. See the [design](spec/runtime/owned-run-cancellation-design.md).

Exercise the running example with
`python3 -B scripts/check_cancel_http_example.py` after building examples. The
checker starts its own temporary local server and records no token.

The `/fail#workflow` fixture saves a known failure and returns an execution error:
wait returns 500; POST stream sends one `done` with an error output. Readiness stays
healthy, the thread returns to idle, and later workflow steps do not run. Public
failure reasons are explicitly configured application content; infrastructure
errors remain uncertain and make the host unready.

Check the built example's token/browser guards and settled-failure process restart:

```sh
cargo build --locked -p b4-http --example serve
python3 scripts/check_http_example.py
```

The check uses a temporary database and generated local token. Evidence for the
earlier settled-failure increment is recorded in its
[verification receipt](conformance/expected/settled-failure-verification.json)
and [selected reference comparison](conformance/expected/settled-failure-reference-comparison.json).

## Try a persisted execution

The SQLite example uses a local scripted effect, commits its attempt before the
callback, then prints the completed snapshot and journal. Use a new run ID each
time; the directory retains previous executions.

```sh
cargo run --locked -p b4-sqlite --example scripted -- /tmp/b4run-demo run-1
```

SQLite opens one serialized connection with WAL, FULL synchronization, foreign
keys and a five-second busy timeout. An OS-held exclusive lock prevents a second
native process from owning the directory. Each open advances the durable owner
generation. Recovery is explicit by a known execution ID; old-owner completions
fail, pure/idempotent uncertain attempts may retry under their declared policy,
and unsafe/reconcilable uncertainty blocks. The SQLite driver bundles SQLite
3.53.2; the workspace is verified with Rust 1.93.1.

Both backends use the same admission, snapshot validation and revision checks.
Every changed snapshot and its journal entry commit together. Completed sibling
results, parked prompts and identity/budget history survive reopening. A failed
attempt commit invokes nothing. A successful effect whose result cannot commit
returns its result for completion-only retry; the runtime never invokes that
callback again automatically. The host supplies fresh completion time, and retry
borrows the result so repeated persistence failures retain it.

`dispatch_outcome` also accepts an explicit settled application failure. It saves
the task's failure reason without a success result and retains the outcome if its
commit fails. A failed batch stops new dispatch and holds admission while existing
siblings drain; late results are retained without applying that batch's writes.
Recovery cancels safe uncertain siblings of a failed batch instead of replaying
them. Unsafe uncertainty still blocks recovery.

## Try durable approval

The embedded approval service saves each prompt and required grant in the same
transaction as its paused task. It accepts the full Once/Deny decision set in one
transaction with the continuation snapshot and an actor/decision receipt. It
requires a host authority implementation and server-derived actor/policy context;
the default constructor requires durable storage. MemoryStore supports an
explicit volatile fixture configuration. Always and off/optional modes are
currently rejected or unavailable.

```sh
cargo build --locked -p b4-sqlite --example approvals
python3 scripts/check_approval_example.py
```

The check uses a temporary database and prints no grant tokens. The example
restores the same grant/prompt after reopening, commits approval, reopens before
execution, then explicitly recovers the known execution and runs the harmless
effect once. It uses a simulated host clock to show an accepted decision still
works after its original grant expires. The service rechecks policy
and the recorded call before dispatch. This remains evidence for the embedded
service boundary; automatic recovery remains separate work.

Grant tokens are stored in dedicated sensitive records for restart disclosure;
the whole database and its backups are sensitive. Tokens do not enter ordinary
snapshots, state projections, journals, acceptance receipts or default Debug
output. [Design](spec/runtime/durable-approval-design.md) and
[verification](conformance/expected/durable-approval-verification.json) describe
the bounded authority and transaction checks.

## Try authenticated approval HTTP

The provider-free approval example uses SQLite and the same bearer-token,
IPv4-loopback, exact Host and rejected Origin protections as the ordinary example.
It registers `/approve#workflow`, saves a completed preparation sibling, and
parks a harmless local release fixture for approval.

```sh
export B4RUN_TOKEN='replace-with-your-local-token'
B4RUN_DATA_DIR=/tmp/b4run-approval-http cargo run --locked -p b4-http --example serve_approvals
```

Run the route through `POST /threads/{id}/runs/wait` or `runs/stream`. Wait park
returns `200 null`; stream park sends `interrupt` events containing
`{interruptId,resumeKey,value,grant}` and one `done {output:null}`. Read the stable
set through `GET /threads/{id}/pending_interrupts`, then submit every interrupt
and its grant to `POST /threads/{id}/resume` using the strict resume DTO. Resume
is SSE: Once runs the approved fixture; Deny or a cancelled entry skips it, with
native final output `{denied:true}` when the final task is denied. The host does
not add approval grant/prompt fields to ordinary state or terminal/error bodies. Treat pending
responses and interrupt frames as sensitive.

`ApprovalHostConfig` supplies an immutable policy revision, bounded positive TTL
and exact route/task prompt map. Durable storage is the default; MemoryStore
requires explicit `ApprovalServiceConfig::VolatileFixture`. Only required
Once/Deny approval is supported; Always, off/optional modes and persistent
permissions remain open. Configuration and combined caller input/prompt proposals
are bounded before admission.

Capacity and a per-thread worker claim are reserved before grant consumption.
Fresh origin and resource policy checks precede admission and protected grant
diagnostics; every callback in this host renews policy. The complete decision set,
consumption and continuation commit together. A disconnected viewer cannot cancel
accepted work or release its worker permit early. Stable park releases the active
worker permit while retaining the execution's admission slot. Partial pending
sets are not disclosed.

```sh
cargo build --locked -p b4-http --example serve_approvals
python3 scripts/check_http_approval_example.py
```

The verifier uses temporary storage and suppresses tokens in its
[receipt](conformance/expected/http-approval-example.json). Current scope and
checks are recorded in [HTTP approval verification](conformance/expected/http-approval-verification.json)
and its [selected reference comparison](conformance/expected/http-approval-reference-comparison.json). HTTP
does not adopt prior-owner work. A trusted host can explicitly recover a known
parked execution with the exact definition and fresh actor/policy authority, then
resume it through HTTP. Accepted Ready continuations survive restart and can run
through the embedded service without another approval; this HTTP slice has no
public launch operation for those already accepted continuations; trusted embedding recovery is described below.

The new [reference grant recordings](conformance/reference/recordings/2026-10-01-grants/README.md)
contain 39 actual pinned TypeScript requests with a deterministic local model and
tool, zero provider calls and zero external Fetch attempts. Their limited
projections repeat in order for 37/39 cases; all 39 match when only two independent
pending arrays are treated as sets. This is reference repeatability, not native
grant parity. Native envelopes, parked thread status, token shape, resume route
validation and unauthorized-resume status have recorded differences; see
[compatibility](spec/runtime/compatibility.md).

## Try trusted restart continuation

An embedding application can call `HttpHost::recover_execution(principal, target)`
on an approval-enabled host. Construct `RecoveryTarget` from a coherent ordinary
`ThreadView`; it binds the thread, immutable generation, execution, prior owner,
execution revision and resource revision. Supply verified server-owned identity.
The host renews current origin/policy and exact definition authority before taking
ownership. Recovery commits those checks atomically through the durable service.
There is no public recovery endpoint or automatic unfinished-execution scan.

Committed Once continues after reopen without another approval or expiry check.
Parked recovery preserves the original grant/prompt; blocked work remains blocked.
Owned workers retain capacity, thread claims and SQLite ownership after the caller
or response detaches. Recovery retains the original attempt budget; exhausted
retry budgets reject without an effect or readiness failure. Current-owner work
cannot be recovered through this API.

```sh
cargo build --locked -p b4-http --example recover_approvals
python3 scripts/check_recovery_example.py
```

The example uses a controlled trusted fixture clock and actual SQLite close/reopen,
then consumes the embedding response. It opens no listener and makes no provider
call. See its [receipt](conformance/expected/trusted-restart-example.json),
[design](spec/runtime/trusted-restart-continuation-design.md) and
[verification](conformance/expected/trusted-restart-verification.json). Separate
native TCP tests qualify the supervised host over a test-only embedding route.
This adds no public Agent Protocol operation.

## Shared native execution driver

`b4-runtime::drive_execution` now runs parallel task batches through one store
coordinator and scoped callback workers. `Runtime::prepare_dispatch` and
`ApprovalService::prepare_dispatch` commit an exact attempt before its callback
can enter. Returned outcomes pass bounded completion retries; callbacks do not
replay when persistence fails. Ordered joins retain definition order even when
callbacks complete in reverse order.

The HTTP host uses this same driver for finite Pure scripts and immutable native applications. Its defaults are
eight shared task slots and two callbacks per execution; `HostConfig` accepts
1–256 shared slots and 1–64 callbacks per execution. A task slot remains owned
through callback quiescence, queued result and completion projection. Required
approval parking needs no task slot. Producer capacity, per-thread exclusion and
store ownership remain held through every entered callback, including cancellation
and viewer disconnection.

Native embeddings implement `DriverHost` to supply authority, time, identifiers
and trusted application callbacks. Driver configuration bounds completion attempts
to 1–64 and serialized outcomes to 1–65,536 bytes, with nesting depth64. Unknown
callback errors, panics or oversized results stop scheduling and fault the host;
real sibling outcomes still drain. `DriverFailure` returns actual retained outcomes
and exact unsettled invocation evidence. This API does not sandbox native code or
terminate an uncooperative callback.

Three separate-process SQLite tests use actual SIGKILL after a committed sibling
and approval park, after Once acceptance before callback entry, and after an Unsafe
effect before its result commit. They qualify preserved continuation and blocked
Unsafe recovery at those boundaries. The broader crash matrix, automatic recovery
scan and shutdown deadlines remain open. See the
[design](spec/runtime/shared-execution-driver-design.md) and separate
[verification receipt](conformance/expected/shared-execution-driver-verification.json).

## Native Rust applications and capabilities

`NativeApplication` binds one immutable full execution definition per literal
route to exact Rust effect callbacks, an optional input validator and explicit
Value/Void output policy. Construction validates finite program limits without
running application code. Callbacks receive the committed invocation and transient
server-derived thread/application/policy context. They return real success, public
known failure or uncertainty; only the host can acknowledge a never-entered
cancelled attempt. Application and execution revision labels are trusted version
declarations, so callback or validator changes require a new execution revision.

`HttpHost::new_with_application` takes the registry, store, authentication, resource
policy and explicit approval configuration. Every protected native operation checks
application scope before private reads. Input validation runs in an owned producer
after authority and capacity reservation, before any implicit thread creation or
admission. Its guards remain held if the client disconnects; authority and the
original thread incarnation are rechecked when validation returns. Native effects
reuse durable preparation, approvals, cancellation and exact recovery fences.

`GET /b4/capabilities` requires authentication, matching application scope and
ReadCapabilities policy. It reports installed mechanisms, effective finite limits,
explicitly absent controls and current status-only readiness semantics. It contains
no route inventory, prompts, grants, policy revision, callback captures or storage
paths. Approval storage durability is null when an ordinary Store port cannot
declare it; service mode is reported separately. The response is no-store and
bounded to65,536 bytes, independently of application output limits. This operational
endpoint is separate from the ten core AP operations and asserts no full profile
or TypeScript capability parity.

An embedding can close admission through `host.shutdown_handle()`. Call
`begin_shutdown()` for an immediate cut, or create a `wait_quiescent(timeout)`
future: a valid call closes admission synchronously before the future is polled.
The default wait recommendation is30 seconds, the maximum is300 seconds, and zero
returns a snapshot without waiting. A longer duration returns InvalidTimeout and
leaves admission unchanged. Keep the handle while dropping the embedding's host,
router and store references; the handle itself retains no store.

Quiescent reports that registered framework jobs returned. Incomplete reports
surviving owner groups; dropping or timing out a wait leaves their ownership
intact. Already admitted turns and operations whose mutation lease won the cut
continue through actual callback return and durable settlement. Quiescence can be
faulted and does not resolve uncertain effects or promise execution success.
After the cut, new protected requests return503 host_draining before authentication
or store access; healthz remains200 and readyz becomes503 without a store probe.
The library owns no listener or executor and leaves their closure to the embedding.

The current capability response uses descriptor v2: it declares supervised admission
drain, includes serving admission in readiness, and reports the fixed wait bounds.
The authored v1 DTO/schema/examples retain their original semantics. Full graceful
shutdown, callback interruption, forced viewer closure and turn deadlines remain
absent. See the [drain design](spec/runtime/supervised-admission-drain-design.md).

Try the provider-free SQLite native example:

```sh
B4RUN_TOKEN=local-demo-token B4RUN_DATA_DIR=/tmp/b4run-native \
  cargo run -p b4-http --example serve_native --locked --offline
```

Its `/native#workflow` callbacks preserve submitted object/null input and join
parallel results in definition order; `/approve#workflow` parks for required Once
or Deny. The example retains the loopback, Host and Origin guards. Build it and run
`python3 -B scripts/check_native_http_example.py` to check native execution,
approval, protected capabilities and process reopening. Prior-owner ordinary
unfinished reads still require recovery; read-only inactive attachment can disclose
the unchanged authorized parked prompt without adopting the execution.

Native callbacks and validators are trusted Rust code. They can block, allocate or
perform their own external work. No native live chunk publisher, sandbox, provider
adapter, automatic recovery scan, deadline or graceful shutdown is added here.
See the [design](spec/runtime/native-application-registry-design.md) and
[implementation plan](docs/superpowers/plans/2026-10-01-native-application-registry.md).

The retained native-registry increment passes561 Rust tests, including68 new tests, plus
50 retained reference integrity tests and10 Python verifier tests. Five additional
actual TCP tests and three native HTTP SIGKILL scenarios qualify selected
disconnect, completion, approval and Unsafe recovery boundaries. Schema checks
cover19 schemas,21 authored examples and192 rejection cases. These checks leave
all56 broad release scenarios pending; the separate native receipt records
the final reviewed scope.

The supervised-drain increment passes 628 Rust tests, including 67 new tests,
plus 50 retained reference integrity tests and 13 Python tests. Eight new actual
TCP tests verify request abandonment, continued admitted work, cancellation
drain, uncertain outcomes and physical SQLite release/reopening. Descriptor v2
checks cover 20 schemas, 23 authored examples and 427 refusal cases. The separate
[verification receipt](conformance/expected/supervised-admission-drain-verification.json)
records the bounded mechanism and preserves the earlier receipts. Full graceful
shutdown and all 56 broad release scenarios remain open.

## Private SQLite backup export

`SqliteStore::export_backup(&mut store, destination)` exports the current database
through SQLite, including committed WAL data. The destination must be a fresh file
in an existing trusted local directory outside the source data directory and its
subtree. Export reserves a private Unix0600 stage, validates the readonly SQLite
structure, and publishes without replacing an existing file. It synchronizes the
file and parent before returning success. The trusted synchronous operation has
no time, size, disk or whole-memory budget; nonUnix reports UnsupportedPlatform.

For controlled restoration, first close admission and observe Quiescent, stop
trusted raw writers, dispose host/router/callback aliases and obtain unique Store
ownership, for example through `Arc::try_unwrap`. Export the final cut, then drop
and permanently retire the source before installing the standalone archive into
a fresh private data directory. Existing open and exact known-ID recovery retain
the original execution and approval fences. No production install command is added.

Backups contain sensitive grant material. Error values and messages are static.
PublicationUncertain means a final artifact may already exist after a cleanup or
synchronization failure; inspect it and avoid overwrite retries. Earlier failures
clean the owned stage best-effort and can leave a private partial file. The readonly
checks validate SQLite structure, schema/checksum and copied ownership metadata;
they do not audit every execution or authority payload.

Historical rollback can omit later effects or resurrect grants. A copied owner
epoch cannot fence another live directory, so clone/failover and arbitrary
point-in-time restore remain open. See the [backup design](spec/runtime/sqlite-safe-backup-design.md)
and [implementation plan](docs/superpowers/plans/2026-10-01-sqlite-safe-backup.md).

The backup increment passes 651 Rust tests, including 23 new export/failure/restore
tests, plus 50 retained reference integrity checks and 13 Python tests. Nine rebuilt
examples pass. The [scoped verification receipt](conformance/expected/sqlite-safe-backup-verification.json)
records actual WAL inclusion, publication and physical owner-lock evidence while
keeping the broader backup/restore and release gates open.

## Remaining core work

Extend the partial CP-001–009 observations to the remaining grant modes,
expiry/races, full attachment frame merging and a real AP client corpus. Qualify automatic discovery and broader process-kill boundaries beyond the explicit
known-ID accepted continuation now supported.
Automatic unfinished-execution scanning, external reconciliation,
arbitrary-effect cancellation and shutdown deadlines, backup/restore, disk-full qualification and the full
process-kill crash matrix remain open. The scoped driver tests cover three actual SIGKILL boundaries in addition to the
earlier idle child; they do not qualify every crash boundary. CI is configured
for macOS/Linux. Local macOS and Linux/arm64 checks are recorded in the separate
[container verification](conformance/expected/linux-container-verification.json);
this does not claim a completed remote CI run or other Linux architectures.

DELETE now removes authorized idle, settled and stable parked threads through an
exact generation/resource/execution fence. It validates historical work and
related ownership, removes snapshots/journal/grants/audit atomically, and refuses
active or uncertain work. Owned worker claims remain held through quiescence and
abandoned deletion requests. See the [design](spec/runtime/fenced-thread-deletion-design.md) and separate
[verification](conformance/expected/fenced-thread-deletion-verification.json).

Owned HTTP cancellation now passes the scoped
[391-test verification](conformance/expected/owned-run-cancellation-verification.json).
The local example checker also verifies prompt wait/stream terminals, continued
admission/deletion refusal through drain, retained late results and healthy
readiness. That receipt is retained as the cancellation boundary; the attachment
increment follows below and the full core release profile remains open.

See the [transactional design](spec/runtime/transactional-runtime-design.md),
[HTTP subset design](spec/runtime/http-subset-design.md),
[settled failure design](spec/runtime/settled-failure-design.md),
[durable approval design](spec/runtime/durable-approval-design.md),
[HTTP approval design](spec/runtime/http-approval-design.md),
[implementation plan](docs/superpowers/plans/2026-10-01-http-approvals.md), and
[requirement inventory](spec/runtime/requirements.yaml) for scope and remaining gates.


## Bounded run attachment

`GET /threads/{id}/runs/stream` joins an authorized active run. Its initial `state`
contains the native anchor values, input and bounded event digest; later chunks,
interrupts and the original terminal follow. Snapshot and subscriber registration
share one publication cut. Permission checks bind the original thread generation,
execution, turn, owner and registered definition, with a fresh check after policy
returns. A waiting permission check cannot retarget a successor.

An inactive attachment emits durable state, retry2000 and `done` with
`{"output":null}`. Stable approval prompts remain readable after SQLite reopen
without adopting the owner, consuming a token or changing expiry/audit records.
An idle thread without a recorded execution returns409 `thread_route_unknown`.
Orphan unfinished work requires the explicit trusted recovery interface.

The default limit is16 attached viewers per run, independent of execution capacity
and the primary POST viewer. Turn history, initial state and each viewer queue have
finite byte/frame bounds. A full viewer set yields only `detached.capacity`; an
overflowed queue yields `detached.overflow` and the producer continues. Evicted or
unavailable history is `turn:null` with `turn_truncated:true`. No durable cursor or
transcript replay is promised. Reading or abandoning a viewer never releases
producer exclusion. Existing viewers receive the selected cancellation terminal;
new viewers during its drain use the inactive state/null-terminal path.

Run `python3 -B scripts/check_attach_http_example.py` against the built example to
check live cancellation fanout, inactive state, retained ownership and completed
state after process restart. Native values/raw event digest remain explicit
projection differences from TypeScript/LangGraph. See the
[design](spec/runtime/bounded-run-attachment-design.md) and
[implementation plan](docs/superpowers/plans/2026-10-01-bounded-run-attachment.md).
The separate [438-test verification](conformance/expected/bounded-run-attachment-verification.json)
records 13 new TCP attachment tests, eight current-build examples and the exact
[selected reference comparisons](conformance/expected/bounded-run-attachment-reference-comparison.json).
The immutable native registry also uses these ten operations; the full
core release profile stays open.

## Original AP clients against Rust

Two fresh source-pinned controller and worker client runs now pass against the Rust
native fixture on macOS arm64. Each invokes all 19 selected methods in eight groups,
with 63 calls and nine core operations. The original request serialization and
response/SSE parsing remain unchanged. Expected authentication, pending-schema and
grantless-resume refusals are recorded alongside successful finite fixtures.

The [runbook](spec/runtime/unchanged-ap-client-corpus-runbook.md) and
[verification receipt](conformance/expected/unchanged-ap-client-native-verification.json)
record source/build provenance, both safe summaries, physical retirement and scope.
Current checks pass 844 Rust entries (three historical helper ignores), 221 Node
checks and 90 Python checks. The earlier diagnostics Linux and container results
remain separately recorded. Original-client execution here covers macOS only;
CP010, M0 and the broad 56 release scenarios remain open.

## Cooperative native cancellation

Native callbacks may override `NativeEffect::invoke_controlled` to receive a
`NativeCancellationObserver`. It becomes true only after the host accepts durable
explicit cancellation for that original run. Poll it at safe boundaries and return
`ControlledNativeOutcome::Cancelled` after the callback and any work it owns have
stopped. Genuine success and known failure remain real outcomes; uncertainty and
panic retain the existing readiness fault behavior.

Existing implementations need only `invoke`; the default controlled entry wraps
its original outcome. `NativeApplication::invoke` continues to call the original
entry. The observer carries no Store, application, host or cancellation authority.
Standalone trusted publishers must supply their own durable authorization.

Notification and the public cancellation response do not release execution
ownership. Callback return, fenced completion and producer retirement must finish
before thread or capacity reuse. Shutdown and viewer disconnect do not set the
observer. Retained clones keep only the small signal allocation alive.

See the [design](spec/runtime/native-cooperative-cancellation-design.md) and
[implementation plan](docs/superpowers/plans/2026-10-02-native-cooperative-cancellation.md).

Fresh Rust checks pass 888 entries across 88 Cargo targets on each of macOS and
Linux, with 3 historical helper ignores. The 93 Python checks pass.
The native additions have 42 distinct Rust declarations and 44 executed entries:
15 portable, 20 API and 9 TCP entries; two shared gate tests compile in both HTTP
targets. The 888-entry workspace total includes the frozen AP example's 30 Rust tests.

The 844 Rust, 221 Node and 90 Python counts in the preceding original-client
section describe its earlier frozen qualification. Its original-client runs remain
in their macOS scope. This increment adds 15 separately counted native requirement
rows while preserving the historical 338 and unchanged-client eight.
See the [scoped verification receipt](conformance/expected/native-cooperative-cancellation-verification.json).

## Bounded unfinished-execution discovery

Trusted Rust embeddings can use `Store::discover_unfinished` to read typed
unfinished execution observations in pages of at most 32 rows. Memory and SQLite
validate selected snapshots and their indexed thread relationships before
returning any page. The API performs no adoption, callback dispatch or readiness
change. Custom stores may retain the default unsupported-operation refusal.

Pages preserve Current/Prior ownership, exact fences and uncertainty flags. A
process-local cursor belongs to its original owned Store; returned pages and
cursors retain no Store or writer lock. Snapshot reads have 1 MiB, depth 64 and
16,384-node caps, with a 32 MiB page cap. SQLite counts actual persisted bytes;
Memory counts compact serialization. Empty terminal pages can advance, and
separate pages do not form a global snapshot.

Recovery still requires an ordinary coherent view, an independently verified
principal, renewed policy, the full immutable Definition and exact fences.
Uncertain work does not gain replay authority from discovery. Automatic startup
coordination and the broad release gates remain open. See the
[runbook](docs/runbooks/unfinished-discovery.md),
[design](spec/runtime/unfinished-discovery-design.md) and
[scoped verification receipt](conformance/expected/unfinished-discovery-verification.json).

Fresh macOS and Linux checks each pass 966 Rust entries across 91 Cargo
targets with 3 historical helper ignores. The 78 new declarations
comprise 30 runtime private, 10 SQLite private and 38 independent integration
tests (13 Memory/API, 18 SQLite, seven trusted-recovery). Python checks pass
96 cases. Nineteen separate discovery requirement rows preserve all previous
requirement groups. Original-client and Node results remain their earlier frozen
macOS evidence; they were not rerun for this increment.

## Opt-in bounded startup recovery preparation

Native embeddings can construct `HttpHost::new_with_application_preparing` with
an actual durable approval Store and `RequiredDurable` approval configuration.
The wrapper closes ordinary admission before exposing a router. Liveness remains
available; readiness opens only after a successful preparation pass. Existing
host constructors keep their current behavior.

Call `begin_preparation` on the existing executor with a trusted
`StartupPrincipalResolver`. It must resolve stored origin labels against current
server-controlled authority. Recovery renews policy, the entire immutable
Definition and the original fences. The embedding must isolate the stable set of
execution IDs from other writers during the pass. Discovery labels and cursors
provide no recovery authority.

Defaults allow 32 pages and 64 candidates; maxima are 64 pages and 256 candidates.
Refusal, stale observations, capacity exhaustion and attempt limits leave admission
closed. Earlier admitted work retains its actual ownership and may finish.
Preparation handles observe progress and bounded waits; their timeout or removal
does not cancel work. Complete can precede callback completion and database
retirement. Use shutdown, normal callback/listener completion and physical Store
retirement separately.

The typed `admit_recovery` method shares the existing recovery driver. A successful
admission records an actual returned adoption. An error may also carry confirmed
adoption through `RecoveryAdmissionError::adoption`; its absence does not establish
rollback when a custom source commits and then fails. No HTTP recovery route or
automatic CLI startup pass is added. Full restart-policy persistence, historical
Definition coexistence and broad release gates remain open. See the
[runbook](docs/runbooks/startup-recovery-preparation.md),
[design](spec/runtime/bounded-startup-recovery-design.md) and
[scoped verification receipt](conformance/expected/startup-recovery-preparation-verification.json).

## Explicit retained native workflow versions

`NativeApplication::new_with_history` installs immutable current and historical
registrations. New work uses the declared current version. Recovery, resume and
attachment require the exact recorded route, execution revision and complete
Definition, using that version's own callbacks and output kinds. Installed
application revision and recorded execution revision remain distinct.

Current registrations stay nonempty; the combined registration count is at most
64. Historical-only removed routes can drain but cannot accept new work.
Historical registrations must be approval-free, including future batches.
Current approval prompts and the existing Once, Deny and TTL behavior remain
available. Input validation continues to run for new work; recovery does not
reinterpret previously admitted input.

Missing executable support keeps the existing healthy refusal and startup
Incomplete. This adds explicit historical drain without completing durable
incompatible/revoked-policy confinement or the full restart algorithm. See the
[runbook](docs/runbooks/historical-native-definitions.md) and
[design](spec/runtime/historical-native-definitions-design.md).

Actual scoped verification: macOS and Linux each passed 1084 Rust test entries across 98 targets, with zero failures and three existing ignores. The 48 new named Rust tests ran once per platform; two nested historical child results are excluded. macOS passed 102 Python tests, strict lint, formatting and contract checks. See the [scoped verification receipt](conformance/expected/historical-native-definitions-verification.json). Original TypeScript client and Node unit-suite evidence remains historical; the existing release gates remain open.

## Explicit prior-owner confinement

Native embeddings can call `HttpHost::confine_prior_owner` for a known unfinished
execution from an earlier SQLite writer. The operation requires its original
fenced target, current trusted observation and separate permission to record a
durable block. It handles missing exact executable support, unavailable original
authority and denied recovery policy without invoking effects or changing task
results, grants or acceptances.

The host renews the same whole original view and permission once before the
atomic storage change. Changed observations refuse without retry. Confirmed
receipts describe verified returned durable mutation; an absent receipt does not
prove rollback, and receipt presence does not establish readiness or physical
retirement. Counted ownership continues through trusted handle and panic-payload
cleanup even if the caller abandons its wait.

This adds an explicit bounded native operation. Automatic startup confinement,
unblocking, permanent revocation, legacy authority migration, effect reconciliation
and the broad restart/release gates remain separate. See the
[runbook](docs/runbooks/prior-owner-confinement.md),
[design](spec/runtime/prior-owner-confinement-design.md) and
[scoped verification receipt](conformance/expected/prior-owner-confinement-verification.json).

Actual macOS arm64 and isolated offline Linux aarch64 checks passed 1,213
top-level Rust workspace entries across 102 Cargo targets per platform, plus three
separately selected slow SQLite cases (1,216 qualified case executions). All 132
new Rust declarations passed as 129 normal and three separate slow cases; 86
implementation-private and 46 independent public declarations retain distinct
scopes. Six default ignored markers include three historical unrun helpers and
three slow cases subsequently passed separately; two nested child summaries are
excluded. macOS Python 105 and contracts passed. Qualification bound 693 inputs
and 273 crate files, preserving 653 prior files and nine Cargo inputs. Original
Node 221 and TypeScript client evidence was not rerun. Final support review,
post-documentation checks and external freeze remain separate.

## Two-lane process startup restart qualification

Two real-process parent cases connect the existing opt-in Preparing host to a
parked required Once continuation and an Unsafe effect ambiguity. The approval
case preserves its successful sibling and original full grant, then runs only
the approved release. The Unsafe case syncs one effect, is killed before its
result commits, and restarts as RecoveryBlocked without replay; unrelated Pure
work remains available after preparation.

Independent SQLite reads after the kill and after normal child retirement anchor
complete selected state, authority and journal. The inspector open consumes an
owner epoch, so the restarted owner is observed rather than guessed. Healthy
normal quiescence, listener completion, actual Store/application Weak absence,
normal exit/reap and a real next writer remain separate from readiness.

This adds seven qualifier files and one Cargo target, with two credited parent
cases and one ignored child selector. It changes no production API, CLI default,
schema, dependency or existing test body. The accepted-before-dispatch restart
window, actual confinement deletion/reincarnation race, full RFC11.4 and broader
release gates remain open; original TypeScript and Node unit-suite evidence is
historical. See the [design](spec/runtime/startup-process-restart-design.md) and
the [runbook](docs/runbooks/startup-process-restart.md) and
[scoped verification receipt](conformance/expected/startup-process-restart-verification.json).

Recorded macOS arm64 qualification passed 1,215 outer workspace entries across 103
targets, plus three separate slow SQLite cases (1,218 distinct qualified
executions), with zero failures and 7 default ignored markers; two nested CLI
child summaries are excluded. macOS 105 Python tests, strict lint, formatting and
contracts passed on 704 inputs/280 crate files. Linux aarch64 passed the same recorded Rust totals, formatting and strict lint;
independent source/execution reviews for both platforms are complete. The three
new metadata declarations passed after genuine missing-group RED and exact
checker adoption with initially pending rows. RESTART-001/002 are implemented
and passed within the bounded native macOS/Linux scope recorded here.
Post-documentation checks and final source reviews are recorded separately.


## Accepted Once before dispatch through Preparing restart

One subsequent native parent qualifies the bounded accepted-before-dispatch window left open by the preceding two-lane increment. A real SQLite Once acceptance consumes the original grant while the release remains approved Ready; the owned seed is killed before success delivery or dispatch. After actual original expiry, automatic Preparing recovery preserves the acceptance and successful sibling, dispatches only the approved release and completes without replacement approval. The interrupted original resume ends abruptly without Done and is not claimed as a normally framed complete response. Explicit callback settlement, healthy restart retirement and a genuine next writer remain separate from preparation Complete.

Actual frozen-source macOS and isolated Linux execution each passed 1,216 outer normal entries across 104 targets plus three separate slow cases, 1,219 distinct executions, with zero failures and eight default ignored markers. One new parent and one ignored infrastructure selector add one case; repeats and two nested CLI summaries add none. Six new Rust qualifier files reuse three unchanged helpers without production API, CLI default, dependency or schema changes. The initial inherited macOS CLI failure remains retained and unresolved; unchanged isolated and full reruns subsequently passed. macOS completed 108 Python tests and the existing 458-row checker inventory. Independent platform reviews are complete. Three metadata methods passed after genuine missing-group and ignored-guard RED, with one separately scoped requirement record and every preceding row preserved. Post-documentation checks and final source acceptance are recorded separately.

This does not close full RFC11.4, persistent Always, automatic confinement, the actual deletion/reincarnation race, all 56 broad scenarios, M0 or CP010. Fixture/artifact caps are not whole-startup heap or resource bounds; original-client and Node evidence remains historical. See the [runbook](docs/runbooks/startup-accepted-once.md) and [scoped receipt](conformance/expected/startup-accepted-once-verification.json). All preceding spans retain their historical scope.


## Bounded opt-in native startup confinement

The Preparing host now offers `begin_preparation_with_confinement` for embeddings that supply a trusted principal resolver and separate confinement authority. It can block coherent prior-owner work when the exact recorded Definition is unavailable or current Recover authorization denies dispatch. It preserves original input, approval and journal history, including consumed Once intent. Audited prior blocks and verified new receipts have separate counters; ambiguous commits receive no invented confirmation. Existing default preparation and ordinary known-ID confinement retain their behavior. Readiness, callback settlement and physical retirement remain separate.

Six private, eight API and two real TCP declarations passed. Actual frozen-source macOS arm64 and isolated Linux aarch64 qualification each passed 1,232 normal cases across 106 outer Cargo targets and three separately selected slow cases, 1,235 distinct Rust executions, with zero failures and eight default ignores. Two nested CLI summaries and helpers add no credit. Formatting and strict workspace lint passed. Three new traceability methods passed after genuine missing-group and ignored-guard RED; their initial later assertions remain uncredited as RED. STARTUP-CONFINE-001 records only this explicit bounded native scope and preserves every preceding requirement. Post-support Python/contracts and final external source acceptance are recorded separately.

The new serialized audit caps do not bound whole-startup heap, CPU, I/O or duration. Automatic CLI startup, actor/secret reconstruction, persistent Always, actual confinement deletion/reincarnation races, full RFC11.4, all 56 broad scenarios, M0 and CP010 remain open. TypeScript/client and Node unit-suite evidence remains historical. See the [runbook](docs/runbooks/startup-confinement.md) and [scoped receipt](conformance/expected/startup-confinement-verification.json). All preceding spans retain their historical scope.

## Recover the native demo at startup

After building the current source and setting the token and data directory above, opt in to startup recovery:

```sh
./target/release/b4run config-check --demo --recover
./target/release/b4run serve --demo --recover
```

Both `--demo --recover` and `--recover --demo` are accepted. Configuration checking validates the same configuration without opening storage or a listener. Serving keeps health available while ordinary admission and readiness remain closed, prepares compatible saved demo work, then emits `b4run_ready` after preparation completes. A recovered callback may still be running at that point. `serve --demo` retains direct serving.

Recovery recognizes only the compiled demo's fixed application, tenant and principal. It preserves original parked approvals and requires their existing grant to resume approved work. A missing historical Definition ends with `b4run_preparation_failed` after normal owned retirement. This flag grants ordinary bounded recovery and supplies no confinement authority. Signals during preparation use the same owned drain: work must return normally and the actual listener must finish before the process releases its database owner.

Twelve new Rust acceptance cases passed focused checks. Each platform recorded 1,244 normal Rust passes; the CLI suite recorded 87, and 117 Python methods passed. Three prior slow SQLite controls retain separately reviewed source-bound evidence without a fresh rerun. The qualification boundaries are recorded in the [runbook](docs/runbooks/cli-startup-recovery.md) and [scoped receipt](conformance/expected/cli-startup-recovery-verification.json). Earlier sections retain their historical scope. Arbitrary application and actor reconstruction, persistent Always, automatic confinement, full RFC11.4, the 56 broad scenarios, M0 and CP010 remain open.

## Receipt-owned permission outcome storage

Embedded callers can consume a settled permission-use callback receipt into
`PermissionUseOutcomeWork` and persist its actual success or application failure.
Storage errors retain the result for an explicit retry with the same Work and
cumulative account; the callback runs once. SQLite updates the snapshot, thread
revision and journal atomically. See the [supported scope and fresh local
checks](conformance/permissions/use-outcome/README.md). The explicit
[embedded Driver bridge](conformance/permissions/use-driver/README.md) now handles
single-task NewUse settlement and storage-only retry. The embedded path supports
[current-run UUIDs and bounded structured JSON](conformance/permissions/normal-inputs/README.md).
Original Always approval now separately supports [UUID thread, execution and turn IDs](conformance/permissions/original-ids/README.md)
through `accept_permission_always_with_normal_inputs`. An explicit
[original UUID Ready cancellation path](conformance/permissions/original-cancel/README.md)
handles cancellation before callback with the same finite account and typed writer.
The embedded Driver now supports [reuse of those original UUID permission records](conformance/permissions/original-reuse/README.md)
by a new UUID run, including retained-result refusal and storage-only retry.
[UUID application identifiers](conformance/permissions/normal-application/README.md)
are also supported through the same flow, including current-application mismatch refusal.
Descriptive [task and effect identifiers](conformance/permissions/normal-task-effect/README.md)
now pass through that flow, with a changed current effect refused before callback entry.
The embedded original approval and saved-use flow also supports [nested structured inputs](conformance/permissions/structured-original-inputs/README.md), with finite evidence limits and storage-preserving oversized refusal.
A default-allow [local callback-entry hook](conformance/permissions/driver-entry-gate/README.md)
lets an embedding refuse entry while retaining the real uninvoked Work.
The opt-in [HTTP permission-reuse host](conformance/permissions/http-reuse/README.md) now
consumes genuinely provisioned permissions through authenticated runs, with existing
Once/Deny fallback. The separate opt-in [HTTP Always host](docs/runbooks/http-always-approvals.md)
now creates permissions through authenticated approval, completes the original run,
and reuses them after reopening. Its [scoped qualification](conformance/permissions/http-always/README.md)
keeps broad durable permissions and permission-aware recovery/cancellation/deletion pending.

The [runnable Rust permission-reuse example](docs/runbooks/permission-reuse-example.md)
provides explicit `seed`, `serve` and `serve-approvals` modes. The new
[HTTP process check](conformance/permissions/http-always-example/README.md) starts
from an empty database, approves through HTTP and verifies original completion
and reuse after restart. The existing seed-and-reuse check also remains passing.

The embedded [original Always Driver](conformance/permissions/original-always-driver/README.md)
now executes the original human-approved run and persists its actual outcome.
Completed permissions remain reusable after reopening SQLite; a storage failure
retains the result for an explicit retry without repeating the callback.


The explicit [embedded permission-use cancellation path](docs/runbooks/permission-use-cancellation.md)
now persists a management request after actual callback entry and settles the real
controlled receipt. Cancellation preserves the saved permission for later reuse.
The [local qualification](conformance/permissions/use-cancellation/README.md) records
eight focused passes and a final full workspace run of 1,575 passed, zero failed
and 11 existing ignored tests. Pre-entry scheduling races and HTTP pre-entry permission
cancellation remain separate work.


The explicit [original Always cancellation path](docs/runbooks/original-always-cancellation.md)
now supports an already-entered original approved callback, including its actual
success/failure outcome and retained-result storage retry. The saved permission
remains reusable after normal reopen through an exact seven-entry creator history.
The [local qualification](conformance/permissions/original-always-cancellation/README.md)
records eight focused passes and a final full workspace run of 1,583 passed,
zero failed and 11 existing ignored tests. HTTP pre-entry permission cancellation and
already-claimed pre-entry withdrawal remain separate work.

The explicit [permission-use Ready cancellation path](docs/runbooks/permission-use-ready-cancellation.md)
now cancels a reusable-permission run before dispatch, preserving its saved permission.
The [local qualification](conformance/permissions/use-ready-cancellation/README.md) records
four focused passes and a full workspace run of 1,587 passed, zero failed and
11 existing ignored tests. If a competing dispatch wins, Ready cancellation refuses without an extra write.
Other pre-entry failure retirement and HTTP pre-entry permission cancellation remain separate work.

The explicit [permission-use withheld callback cancellation path](docs/runbooks/permission-use-withheld-cancellation.md)
now retires actual renewed work retained after a local callback-entry refusal.
It atomically persists both cancellation transitions without invoking the callback,
preserving the saved permission for later reuse. The [local qualification](conformance/permissions/use-withheld-cancellation/README.md)
records four focused passes and 1,591 reported full-workspace passes, zero failures
and 11 existing ignored tests. Other pre-entry failure retirement and HTTP pre-entry permission cancellation
remain separate work.

The explicit [original Always withheld callback cancellation path](docs/runbooks/original-always-withheld-cancellation.md)
now retires actual original work retained after a local callback-entry refusal,
without invoking the callback or creating a reuse record for the original run.
Both cancellation transitions persist atomically, and the saved permission
remains reusable after reopen. The [local qualification](conformance/permissions/original-always-withheld-cancellation/README.md)
records four focused passes and 1,595 reported full-workspace passes, zero failures
and 11 existing ignored tests. HTTP pre-entry permission cancellation and other pre-entry
failure retirement remain separate work.

The opt-in [HTTP entered permission cancellation profile](docs/runbooks/http-permission-entered-cancellation.md)
now stops an actually entered original Always or saved-permission callback through the existing cancel endpoint.
The durable request commits before the local signal and interrupted response; actual callback outcomes and ownership remain retained through quiescence.
V5 advertises this narrow capability. Ready and unentered HTTP cancellation, recovery and deletion remain separate work.
The [local qualification](conformance/permissions/http-entered-cancellation/README.md) records seven HTTP scenarios, two included fixture controls,
1,612 reported workspace passes, zero failures and 11 existing ignored tests.

The opt-in [HTTP Ready permission cancellation profile](docs/runbooks/http-permission-ready-cancellation.md)
now cancels genuinely registered original Always and saved-permission runs before Dispatch.
V6 advertises Ready and entered cancellation. Ready publication follows the confirmed atomic write without a native signal;
worker ownership remains held through actual drain. Dispatched-but-unentered withdrawal, recovery and deletion remain separate work.
The [local qualification](conformance/permissions/http-ready-cancellation/README.md) records five HTTP scenarios, two included fixture controls,
four runtime reconciliation tests, 1,629 reported workspace passes, zero failures and 11 existing ignored tests.

The opt-in [HTTP withheld permission cancellation profile](docs/runbooks/http-permission-withheld-cancellation.md)
now withdraws actual dispatched original Always and saved-permission Work before callback admission.
V7 preserves Ready and entered cancellation. The real worker-owned failure drives the atomic two-transition write,
then publishes one terminal without invoking or signalling native code. Ownership remains held through actual drain.
The [local qualification](conformance/permissions/http-withheld-cancellation/README.md) records five HTTP scenarios,
five private handoff controls, 1,646 reported workspace passes, zero failures and 11 existing ignored tests.

Try cooperative permission cancellation with the explicit `serve-cancellation` mode of
`cargo run -p b4-http --example serve_permission_reuse --locked --offline -- serve-cancellation`.
The [runnable instructions](docs/runbooks/permission-reuse-example.md#try-cooperative-cancellation) show real callback entry,
authenticated cancellation and normal restart/reuse. The [example qualification](conformance/permissions/http-cancellation-example/README.md)
records three real server owners, original and saved-use cancellation, later saved completion,
and both unchanged older process checks. Libraries and contracts remain unchanged in this example increment.

The explicit Rust `delete_settled_permission_thread` entry removes a fully settled
permission-bearing thread and its approval/permission history in one transaction.
The embedding holds thread exclusion and drains actual workers before invoking its
dedicated deletion authority. Exact historical targets remain valid after normal
Store reopen, with current Store ownership checked separately.
See the [embedded instructions](docs/runbooks/permission-settled-thread-deletion.md)
and [local qualification](conformance/permissions/settled-thread-deletion/README.md).
This embedded operation preserves the existing generic deletion guards and HTTP profiles.


The opt-in [HTTP settled permission-thread deletion profile](docs/runbooks/http-permission-settled-thread-deletion.md)
connects that Rust entry through `HttpHost::new_with_permission_settled_thread_deletion`.
V8 advertises only `durable_permission_settled_thread_deletion`; earlier HTTP profiles retain their behavior.
The host holds its thread claim and mutation lease through the actual writer and cleanup,
including after the caller disconnects. A persisted terminal state still refuses deletion
while the real run worker owns the thread. See the [local qualification](conformance/permissions/http-settled-thread-deletion/README.md)
for genuine histories, current authority, bounded refusals, atomic rollback and abandonment checks.


The report example now offers explicit `serve-deletion` using the V8 host profile.
The [runnable instructions](docs/runbooks/permission-reuse-example.md#try-settled-history-deletion)
show authenticated settled deletion and normal restart. The [process qualification](conformance/permissions/http-deletion-example/README.md)
records three real owners: genuine permission history and live refusal, reopened settled deletion preserving another history,
and surviving saved reuse after another reopen. Existing modes, libraries and contracts remain unchanged in this example increment.


The [nested-input HTTP qualification](conformance/permissions/http-structured-inputs/README.md)
verifies original Always execution and saved permission reuse after reopen using distinct nested inputs,
with independent checks of callback inputs, stored records and permission provenance.


The runnable approval example also has a [nested-input process check](conformance/permissions/http-structured-input-example/README.md).
It exercises nested original Always execution and distinct saved reuse after restart,
with independent durable-input and permission-provenance checks.
