# RFC: b4 Native Runtime — Initial Agent Protocol Implementation

**Document ID:** B4-RUST-RUNTIME-001  
**Revision:** Draft 0.1  
**Date:** 30 September 2026  
**Prepared for:** Brian Love / Cacheplane  
**Status:** Proposed specification; not an implemented or qualified runtime  
**Reference implementation:** `cacheplane/b4run` at `8acac534ba6eb960b34de217b0da45b8405023af`  
**Parent research:** `b4dotrun-rust-feasibility-and-migration-report-2026-09-30.md`

---

## 1. Decision and objective

Build a **Rust-native, single-process, SQLite-backed runtime that implements a declared core profile of b4’s Agent Protocol**. Make its behavior an executable specification: typed contracts, explicit state transitions, a real HTTP server, deterministic execution fixtures, and recovery/conformance tests.

Do **not** begin by implementing a general-purpose LangGraph replacement, an expansive agent SDK, every model provider, or the complete b4 platform. The initial runtime must prove the boundaries on which those systems will depend: admission, execution identity, persisted state, streaming, authorization, approvals, cancellation, and recovery.

The decisive demonstration is not a chatbot. It is this sequence:

> A native execution completes one tool, parks another for approval, survives process termination, exposes the outstanding approval through Agent Protocol, accepts an authorized decision exactly once, and completes without rerunning the already committed tool.

A second version of the demonstration must crash **after an external effect but before its result is committed**. The runtime must either reconcile that effect under an explicit policy or stop with an unknown-outcome condition. It must not silently repeat an unsafe operation.

This specification chooses a small b4-owned transition kernel for the initial implementation. It does not settle the long-term choice between an expanded native scheduler and reuse of an execution library. A later candidate must pass the same contracts without leaking its internal types into the protocol or persistence format. This follows the parent report’s separation of b4-owned semantics from reusable machinery. [R1]

### 1.1 What “Agent Protocol” means here

The target is **b4’s existing HTTP/SSE API**, including `{ route, input }` requests and route keys such as `/research#agent`. It is not a claim of compatibility with every API described as Agent Protocol, the full LangGraph/LangSmith deployment API, AG-UI, MCP, or A2A. b4’s documentation explicitly distinguishes its route-based body from LangSmith’s `assistant_id` body. [S01]

The initial compatibility profile is named **`b4-ap-core/0`** in project artifacts. That name is a proposed b4 profile identifier, not an upstream standard or existing published version.

The supported core consists of ten endpoint operations covering threads, state, runs, attachment, approvals, and cancellation. Memory-candidate administration and managed-workspace endpoints are inventoried but deferred. The release must therefore say **“implements the b4 Agent Protocol core profile”**, not “drop-in replacement for all of b4.”

### 1.2 Normative language

**MUST**, **MUST NOT**, **SHOULD**, and **MAY** express requirements of this proposed implementation. They do not imply that the existing TypeScript runtime already provides those guarantees.

Requirements have three evidence classifications:

| Classification | Meaning |
|---|---|
| `COMPAT` | Preserve a selected observable behavior from the pinned TypeScript implementation. |
| `NATIVE` | New internal or operational requirement for the Rust implementation. |
| `CHANGE` | Deliberate deviation, restriction, or stronger default that must be documented and tested. |

A compatibility claim requires an executed comparison. Source inspection alone is evidence for authoring a test, not a passing conformance result.

## 2. Initial scope

### 2.1 Required for the initial release

The initial release MUST include:

- A native library and standalone executable, with static Rust route registration and no production dependency on Node.js, LangChain, LangGraph.js, or a JavaScript engine.
- A deterministic transition kernel and scripted execution adapter supporting sequential steps, a bounded parallel tool batch, a join, state updates, approval parking, continuation, and terminal outcomes.
- One transactional SQLite runtime store, plus an in-memory implementation of the same semantic contract for tests.
- The ten core Agent Protocol operations, SSE encoding, bounded live attachment, liveness/readiness probes, and a machine-readable capability description.
- Authentication/authorization integration, required approval grants by default, durable approval consumption, explicit cancellation, and restart recovery.
- Black-box HTTP tests, deterministic kernel tests, storage conformance, process-kill tests, and a TypeScript/Rust comparison corpus for preserved behavior.

The scripted executor is an actual execution adapter, not an HTTP mock. It must pass through admission, storage, authorization, task dispatch, streaming, cancellation, and recovery. Its program is registered by trusted application code; clients cannot upload arbitrary scripts or executor instructions.

### 2.2 Deferred, with explicit boundaries

| Area | Initial disposition | Preserved extension point |
|---|---|---|
| Live model providers | Not required for core acceptance; follow-on milestone | Typed model-effect port and recording format |
| General graph DSL, dynamic sends, arbitrary reducers | Not implemented | Versioned execution definition and kernel commands |
| LangGraph checkpoint import or raw graph execution | Not supported | Offline migration boundary; no shared DB |
| File-based Rust discovery and procedural macros | Deferred | Explicit registry and immutable route keys |
| AG-UI, CopilotKit, Hashbrown integrations | Deferred; raw AP clients only | Separate event projection over the same run service |
| Subagents and nested graph namespaces | Deferred | Root execution identity and future child linkage |
| Memory, summarization, skills, planning, offload | Deferred | Capability and effect interfaces, not placeholder behavior |
| Docker/Kubernetes workspaces, shell execution | Deferred | Native tool adapter boundary |
| Postgres and multiple execution replicas | Deferred | Transactional store contract and owner generation |
| Public event replay and `Last-Event-ID` cursors | Not supported | Internal transition sequence is not a public cursor |
| Full evaluation product | Deferred | Deterministic behavior gates are required now |
| Production serverless/edge targets | Deferred | Native process/container qualification first |

Do not create a large set of empty crates for these future areas. Do not publish no-op adapters or return fabricated successful responses for unsupported capabilities.

### 2.3 First application surface

Applications register native execution definitions explicitly. A fixture application SHOULD expose `/echo#workflow`, `/stream#agent`, `/approval#agent`, and `/parallel-approval#agent` for testing. The names are examples, not framework-reserved routes.

The `#agent` label in the fixture application describes the route key being exercised; it does not establish a live-model integration. Production builds MUST NOT accidentally include fixture-only routes or fault-injection controls.

## 3. Deliverables for authoring the executable specification

The first pull requests must produce these artifacts before implementation breadth grows:

```text
spec/runtime/0001-agent-protocol-core.md
spec/runtime/compatibility.md
spec/runtime/requirements.yaml
spec/runtime/state-machines.md
spec/runtime/storage-contract.md
spec/runtime/security-boundaries.md
spec/runtime/decisions/
contracts/agent-protocol/openapi.yaml
contracts/agent-protocol/schemas/
contracts/agent-protocol/examples/
conformance/reference/manifest.json
conformance/reference/http/
conformance/reference/sse/
conformance/scenarios/
conformance/expected/
```

These are proposed repository paths. This draft does not claim to have generated those artifacts.

**SPEC-001 — Traceability.** Each normative requirement MUST have a stable ID, classification, source or design rationale, owning component, acceptance test, and implementation status. A source observation MUST be distinguished from a recorded runtime observation.

**SPEC-002 — Baseline.** The reference manifest MUST record the source commit, lockfile digest, reference build instructions, fixture application revision, effective configuration, dependency versions, and fixture-file hashes. Do not silently update the oracle to a later `main`.

**SPEC-003 — Schemas.** OpenAPI and JSON Schema MUST specify bodies, response statuses, omission/null rules, strictness, and bounded sizes. SSE payload schemas and lifecycle rules MUST be defined separately; an OpenAPI declaration of `text/event-stream` is insufficient.

**SPEC-004 — No circular oracle.** Golden fixtures MUST come from either an independently executed TypeScript reference or explicit, reviewed native requirements. Generating “expected” results from the Rust implementation under test is not conformance.

**SPEC-005 — Conflict resolution.** For a preserved contract, executed pinned behavior is the comparison oracle. A discrepancy between source, documentation, and execution MUST become a recorded decision: preserve, intentionally change, or exclude. The new Rust implementation must never become its own retroactive authority.

**SPEC-006 — Completeness gate.** The characterization items in Section 18 must be resolved before the core profile is frozen. They are implementation tasks, not permission to leave ambiguous behavior in the released contract.

## 4. Architecture and ownership

### 4.1 Component model

```text
Native application registry
          |
HTTP adapter / embedded host
          |
Authentication -> resource authorization -> route admission
          |
Run service and per-thread coordinator
          |
Pure transition kernel
          |
Transactional commit -> authorized effect dispatch
          |                        |
     SQLite store             Native/test adapters
          |
Committed state + bounded live projection -> HTTP/SSE viewers
```

Transport handlers MUST call the same run service as embedded callers. HTTP routes MUST NOT directly modify checkpoint, approval, or task tables.

### 4.2 Suggested Cargo workspace

| Crate | Owns | Dependency restrictions |
|---|---|---|
| `b4-contract` | IDs, internal commands/outcomes, serializable snapshot formats | No network, SQL, ambient clock, or HTTP framework |
| `b4-kernel` | Pure transitions, task scheduling decisions, reducers, invariants | Depends on contracts, not Tokio or storage |
| `b4-runtime` | Admission, coordinators, execution drivers, ports, lifecycle | No provider-specific request formats |
| `b4-sqlite` | Migrations, transactions, recovery queries, local store lock | No authorization decisions or SSE formatting |
| `b4-agent-protocol` | AP DTOs, router, errors, SSE and attach projection | No direct SQL and no independent execution loop |
| `b4-testkit` | Scripted adapters, fixtures, clocks, failure controls, assertions | Network calls disabled unless explicit test mode |
| `b4d` | Executable assembly, configuration, probes, operator commands | Thin wrapper over the same libraries |

Seven crates is a suggested ceiling for the initial workspace, not a publishing requirement. Small private modules may be preferable during the first implementation.

### 4.3 Implementation foundations

Use Tokio for asynchronous driving and cancellation, Axum/Tower for native HTTP, Serde for explicit wire DTOs, and SQLx or another qualified SQLite driver for storage. Axum supplies SSE primitives; Tokio documents cancellation and tracked shutdown. Neither supplies b4’s ownership or durability semantics. Pin versions and the compiler in the implementation, rather than using this document as a dependency lock. [E01, E02]

The kernel MUST receive time, random identifiers, and externally observed results as explicit inputs. It MUST NOT read the environment, obtain the current time, make model calls, or open database connections.

Illustrative boundary, not existing or compiled b4 API:

```rust,ignore
pub fn transition(
    snapshot: &ExecutionSnapshot,
    command: KernelCommand,
    facts: TransitionFacts,
) -> Result<CommitPlan, KernelError>;

pub trait RuntimeStore: Send + Sync {
    fn commit<'a>(
        &'a self,
        plan: CommitPlan,
    ) -> BoxFuture<'a, Result<CommitReceipt, StoreError>>;
}
```

`CommitPlan` contains expected revisions, task changes, state changes, interrupt/grant changes, and newly dispatchable intents. Effect dispatch begins only after the corresponding commit succeeds. A failed compare-and-swap causes state to be reloaded; it MUST NOT cause an already completed external effect to run again.

## 5. Domain model and lifecycle

### 5.1 Identities are not interchangeable

Use newtypes for application scope, tenant scope, principal, route key, execution revision, thread ID, thread generation, execution ID, turn ID, task ID, effect ID, attempt ID, checkpoint ID, and interrupt ID.

**Thread:** Durable conversation and application-state container.

**Execution:** Logical work initiated by a run request. It may span an approval pause and multiple turns.

**Turn:** One active execution segment started by `runs/wait`, `runs/stream`, or `resume`. A parked turn is finished as an HTTP interaction, even though its execution is not complete.

**Attempt:** One try at a particular external effect. Attempts can change; the logical effect ID does not.

**Viewer:** A response body subscribing to a turn or snapshot. Losing the viewer does not end the execution.

The current b4 server’s public API does not require a client-supplied `run_id`; internal native IDs MUST NOT change the existing `{ route, input }` request body. [S06]

### 5.2 Execution lifecycle

```text
Accepted -> Running -> Completed
                    -> WaitingForApproval -> Running
                    -> Failed
                    -> CancelRequested -> Cancelled
                    -> RecoveryBlocked
```

`RecoveryBlocked` means execution cannot proceed safely, for example because an effect’s outcome is unknown or its exact executable revision is unavailable. It is not a successful completion.

A process shutdown is not automatically a user cancellation. Persist its reason separately; restart recovery may continue safe work. An explicit user cancellation MUST NOT be undone by restart.

### 5.3 Task lifecycle

```text
Ready -> WaitingForApproval -> Ready
      -> Denied
      -> Cancelled
      -> InFlight -> Succeeded
                  -> RetryScheduled -> Ready
                  -> Failed
                  -> Cancelled
                  -> OutcomeUnknown
```

Permission and per-call constraint checks occur before effect dispatch. `WaitingForApproval` is a pre-effect state, not permission to call the tool and then ask for approval. A resumed task rechecks applicable policy using its committed decision; `Denied` settles the task with a structured denial result and no tool invocation. Arbitrary suspension inside a native tool's Rust future is not supported.

A task can have only one accepted result per logical completion. Repeated delivery of the same result is idempotent; a different result for the same completed attempt is an integrity error.

A pending approval must be committed before its prompt is disclosed. A successful task result must be committed before a `tool_result` frame claiming success is emitted.

### 5.4 Thread wire status

The existing thread representation uses `idle`, `busy`, and `interrupted`. It does not expose the richer execution lifecycle. [S02]

The AP adapter MUST retain those wire values. Internally it MUST distinguish waiting for approval, explicit cancellation, failure, and recovery blockage.

For the selected compatibility behavior:

| Condition | Thread wire status |
|---|---|
| Active turn | `busy` |
| Settled successful/failed turn | `idle` |
| Approval park reached through stream/resume | `interrupted` |
| Approval park reached through wait | `idle`, preserving the documented quirk |
| Explicit cancellation | `interrupted` |
| Recovery-blocked execution | `interrupted`; local diagnostics explain the reason |

The wait-versus-stream distinction is documented in b4. Admission MUST use internal execution state, never `status == idle`, to decide whether new work is safe. Pending approvals remain authoritative even when the wire status is `idle`. [S01]

## 6. Agent Protocol surface

### 6.1 Required operations

| ID | Method/path | Required behavior |
|---|---|---|
| AP-001 | `POST /threads` | Accept an empty body, `{}`, or `{ "metadata": {...} }`; create and return a thread with `200`. |
| AP-002 | `GET /threads/{thread_id}` | Return authorized thread with `200`; absent/inaccessible behavior is policy-controlled, normally `404`. |
| AP-003 | `DELETE /threads/{thread_id}` | Delete native thread state with `204` when safe; reject an active turn with `409 run_in_flight`. |
| AP-004 | `GET /threads/{thread_id}/state` | Return the AP checkpoint projection; `404` when no checkpoint exists. |
| AP-005 | `GET /threads/{thread_id}/pending_interrupts` | Return all pending root prompts; check parking-route authority; `cache-control: no-store`. |
| AP-006 | `POST /threads/{thread_id}/runs/wait` | Admit a route invocation; wait for that turn to settle; return its output, not a wrapper invented by Rust. |
| AP-007 | `POST /threads/{thread_id}/runs/stream` | Admit the same invocation and stream AP SSE. |
| AP-008 | `GET /threads/{thread_id}/runs/stream` | Read-only snapshot plus bounded live attachment; never starts or resumes execution. |
| AP-009 | `POST /threads/{thread_id}/resume` | Validate the complete pending set and grants; accept a continuation and return SSE. |
| AP-010 | `POST /threads/{thread_id}/cancel` | Cancel the exact active turn observed by the request; preserve committed state. |

These operations are grounded in b4’s documented surface. Detailed native safety requirements and deliberate restrictions below are additional. [S01, S06]

### 6.2 Operational endpoints

`GET /healthz` MUST return `200 {"status":"ready"}` while the listener is functioning, without consulting SQLite. The wording is retained for compatibility; it means liveness, not readiness. [S06]

`GET /readyz` MUST test the runtime’s configured store schema and readiness state, returning `200` or `503` with `{status, checks}`. The native check names may differ from the TypeScript store names and MUST be specified in the native capability contract. Shutdown, failed migration, or lost store access MUST make readiness fail.

Proposed `GET /b4/capabilities` MUST return the profile, runtime version, application revision, registered capability flags, and limits. Production access requires authentication unless the host explicitly permits public discovery. It MUST NOT expose secrets, routes the caller cannot discover, or absolute filesystem paths.

Example capability fragment:

```json
{
  "protocol": "b4-ap-core/0",
  "execution": "single-process",
  "persistence": "sqlite",
  "features": {
    "approval_grants": true,
    "live_attach": true,
    "durable_event_replay": false,
    "memory_candidates": false,
    "managed_workspaces": false,
    "ag_ui": false
  }
}
```

### 6.3 Unsupported surfaces

Memory-candidate endpoints MUST not return empty successful results as substitutes for an absent subsystem. For authorized callers, the initial native profile returns `501` with `error.details.code = "capability_not_supported"`. This is an explicit profile restriction, not full memory API compatibility.

Disabled workspace upload/inspection paths SHOULD preserve the incumbent’s disabled-feature `404` behavior after the appropriate access gate. A valid `workspace` field on thread creation MUST return `400 workspace_not_accepted`; it MUST NOT be silently discarded. [S01, S06]

AG-UI, assistant management, background-run creation, thread search, state mutation, cron jobs, and time-travel APIs are not implemented by this profile. Do not add LangGraph-shaped endpoints merely because they seem associated with the name Agent Protocol.

### 6.4 JSON parsing and route identity

**AP-011.** Route keys are literal registered strings with the kind suffix. A missing route is a request error; an unregistered key, including a key without its required suffix, is `404`. A request cannot replace the route registry or select an arbitrary executable path. [S01]

**AP-012.** A run body is an object with a required route string and optional application input. Omitted input defaults to `{}`. The schema MUST specify present-null and scalar input behavior independently; Serde’s default handling of `Option<Value>` MUST NOT accidentally erase that distinction. Section 18 requires characterization before freeze.

**AP-013.** Application input is not trusted execution configuration. Ownership, tenant identity, policy, checkpoint pointers, pending tasks, and grants come from server-owned records. Route schemas validate application fields before dispatch.

**AP-014.** `resume` bodies are strict at the root and entry levels. Legacy `{interrupt_id, decision}` input is rejected. Unknown executable-control fields MUST never take effect through permissive deserialization.

**AP-015.** Malformed JSON and body-validation errors use the AP error envelope, normally with `400`. Axum’s default rejection body/status is not the protocol contract. No rejected request may create work or consume a grant.

**AP-016.** The initial host applies explicit request-size, JSON-depth, and parsing-time limits. These are native resource protections and must be listed as compatibility limits, not inferred incumbent defaults.

### 6.5 Thread representation

Preserve these public fields: [S02]

```json
{
  "thread_id": "t-example",
  "created_at": "2026-09-30T20:00:00.000Z",
  "updated_at": "2026-09-30T20:00:00.000Z",
  "metadata": {},
  "status": "idle"
}
```

Native-generated IDs SHOULD contain at least 128 bits of randomness. Clients MUST treat them as opaque; the old implementation’s shorter random-ID format is not a native security requirement. Recheck uniqueness and ownership; never use an upsert that returns a different caller’s existing thread.

A run may implicitly create an unknown thread after authorization. If a concurrent creator wins, the runtime MUST authorize the actual row before proceeding. It MUST NOT overwrite that row’s ownership metadata.

Keep server-owned route/provenance/access fields separate from user metadata internally. Project selected legacy fields such as `metadata.route` when needed. Client-written metadata is never authoritative provenance.

### 6.6 State representation: preserve the projection, not the engine

Return `{config, created_at, metadata, next, parent_config, values}` as the AP projection. The native checkpoint format is separate.

The inspected TypeScript implementation sets `created_at` to the **read time** and builds `next` from **pending-write channel names**, not from scheduled graph nodes. Its root checkpoint lookup uses an empty namespace. These are important adapter details. [S06]

The initial compatibility projection MUST retain the read-time field and ordered pending-channel list for characterized scenarios. Native checkpoints MUST separately store their real creation time and actual ready tasks. Neither scheduling nor recovery may depend on AP `next`.

`parent_config` is `null` when absent. The state projection MUST not fabricate a checkpoint for a thread that has never produced one. Internal checkpoints and clients’ state views require separate schema versions.

## 7. Streaming and attachment

### 7.1 SSE wire format

The response uses `text/event-stream`. Each AP frame consists of an `event:` name and JSON payload on `data:`, followed by a blank line. b4’s encoder emits a chunk’s `data` directly and named tool/done fields as objects; it does not add another `{data: ...}` wrapper. [S03]

```text
event: chunk
data: "partial text"

event: tool_call
data: {"id":"call-1","name":"search","input":{"query":"b4"}}

event: tool_result
data: {"id":"call-1","name":"search","output":{"found":true}}

event: done
data: {"output":{"messages":[]}}

```

**SSE-001.** The encoder MUST be independent of HTTP chunk boundaries and correctly escape strings, Unicode, quotes, and embedded newlines. Clients’ parsers must be tested with arbitrary byte splits. Use the actual SSE format, not newline-delimited JSON labeled as SSE. [E03]

**SSE-002.** Raw AP `chunk` payloads MUST remain compatible with the baseline even when internal events carry richer message/attempt identity. Do not silently switch AP to AG-UI event names. [S03] Event names come from a validated registry, never untrusted strings containing CR/LF; extensions cannot impersonate lifecycle frames such as `done`.

**SSE-003.** A normally completed attached turn emits one terminal `done` and closes. A permission park also settles the current turn: prompts precede its terminal output. Terminal payloads for park, failure, and null output MUST be frozen from the fixture corpus before profile publication.

**SSE-004.** Explicit cancellation of stream/resume ends with `done` containing `{"output":{"cancelled":true}}`. Cancellation of wait returns `409 run_cancelled`. A runtime failure after SSE headers are sent uses the established terminal output-error shape, not a new HTTP status or an unannounced `event:error`. [S01]

**SSE-005.** A socket failure may prevent delivery of any terminal frame. Client EOF without a terminal is an unknown transport outcome, not proof of successful or cancelled execution.

**SSE-006.** Emit `: ping\n\n` during quiet periods, with a 15-second default. Disable proxy buffering where the host permits, and do not let response compression or a buffering middleware destroy streaming behavior. [S01, S06]

### 7.2 Viewer lifetime is not execution lifetime

Disconnecting wait, stream, or resume MUST detach that viewer and leave the admitted run active. Explicit cancel and server lifecycle controls remain separate. This preserves the documented Agent Protocol behavior; AG-UI has a different lifecycle and is not included here. [S01]

Runtime-owned tasks MUST outlive an HTTP handler future. Dropping a response body MUST NOT release the thread’s execution slot, dispose run-owned dependencies, or drop the only handle supervising an effect.

If a wait client disconnects, the runtime must still commit its eventual outcome and checkpointed state. A later attach/state read recovers the available result; restarting the same POST is not a safe retry strategy.

The core profile does not promise run-creation idempotency or implement an `Idempotency-Key` header. Repeating a start request after its earlier execution has settled may create a new execution. Clients must inspect/attach to the known thread after an ambiguous response rather than infer that the request was rejected. Durable run-start deduplication requires a separately specified extension.

### 7.3 Snapshot attachment

Every successful AP attach begins with a `state` frame. For a live turn it includes the anchor state, turn input, accumulated frames, and live status. For no active turn, it includes the latest durable state and pending prompts, followed by a retry hint and `done` with null output. [S01]

```json
{
  "status": "busy",
  "live": true,
  "anchor": "checkpoint-example",
  "run_started_at": "2026-09-30T20:00:00.000Z",
  "resume": false,
  "values": {},
  "input": {},
  "turn": [],
  "interrupts": []
}
```

**ATTACH-001.** Snapshot construction and live subscription MUST share one serialization point in the per-thread coordinator. Capture the snapshot cut and register the subscriber before allowing the next live event to pass that cut. Do not “read snapshot, then subscribe” across an uncontrolled gap.

**ATTACH-002.** Resolve and authorize every provenance-bearing route whose content the snapshot reveals. Use stored checkpoint origin, current turn origin, and parking origin, not caller-provided metadata. Revalidate if the captured version changes while authorization awaits. Unknown origin returns `409 thread_route_unknown` without revealing the route. [S01]

**ATTACH-003.** Keep a 2 MiB per-thread turn buffer and a default maximum of 16 attached viewers. On digest overflow, expose `turn: null` and `turn_truncated: true`; do not pretend the retained suffix is a complete transcript. Capacity and slow-viewer detach frames are terminal for that viewer. [S01, S06]

**ATTACH-004.** Subscriber queues MUST have both frame-count and byte bounds, including the original POST stream's viewer as well as GET attach viewers. No slow viewer may stall authoritative commits or cause an unbounded producer queue. Reserve terminal-control capacity or close a stalled socket by deadline; do not promise that an overflow notification can always reach a client that has stopped reading.

**ATTACH-005.** Snapshot/history buffers are disposable projections, not authoritative state. A restarted process cannot reconstruct an uncommitted token stream. A recovered live turn must indicate truncation/unavailable history rather than invent old frames.

**ATTACH-006.** This profile provides no durable event replay, no cursor retention promise, and no `id:` sequence for resumption. `Last-Event-ID` must not be interpreted as a replay request; attaching always obtains a new snapshot. Internal journal sequence numbers are not public cursors.

A reconnect cannot restore bytes already evicted from a live buffer. It restores the available authoritative state and current view, not guaranteed transcript completeness.

## 8. Authorization and security

### 8.1 Host admission modes

`development` mode may use an explicit local principal but MUST bind to loopback by default and apply Host/Origin protections suitable for browser access. It must not expose unrestricted local tools to arbitrary websites.

`production` mode MUST require a configured authenticator and resource policy before serving any non-probe API, including on loopback. A non-loopback listener is forbidden without those gates in any mode. Network exposure cannot silently imply allow-all. TLS may terminate at a trusted reverse proxy; forwarded identity is trusted only from explicitly configured proxies.

The initial framework needs an authentication integration point, not a new identity-provider product. Test identities must be injected through the same verified-principal boundary used by real authentication.

### 8.2 Gate order

For sensitive resource operations:

1. Apply transport limits and authenticate the caller.
2. Resolve the resource within the server-derived application/tenant scope.
3. Authorize the operation and required route provenance.
4. Validate current execution/interrupt state, and only then inspect grants.
5. Commit admission or mutation, then dispatch effects.

Structural parsing may occur before a resource is resolved, but no sensitive grant/existence diagnostics may precede access authorization. Authentication does not replace route policy; possession of a grant does not replace either.

The existing b4 execution middleware is not blanket HTTP authentication. The native production profile deliberately adds an outer gate over all non-probe APIs. [S01]

### 8.3 Resource policy

Authorization MUST cover create, read, delete, state, attach, pending prompts, run, resume, cancel, and capability discovery. Later memory/workspace endpoints must not bypass it.

The policy receives trusted identity, operation, resource snapshot, and immutable route identity. It does not receive an unverified grant as an authorization credential. An inaccessible resource may be represented as `404`; the policy decides that visibility rule consistently.

Recheck a resource version after an asynchronous policy evaluation and before mutation. A captured cancel or resume must not operate on a replacement turn that appeared during the policy check.

### 8.4 Native tools and secrets

Native tools are trusted application code, not sandboxed code. Rust’s type system does not prevent a trusted tool from opening a network connection or reading host files. Untrusted execution remains out of scope.

Secrets MUST NOT be stored in model fixtures, ordinary checkpoints, public error details, route metadata, or logs. Approval tokens are a special sensitive datum addressed in Section 9; their recoverability must be honest about storage exposure.

## 9. Approval and resume contract

### 9.1 Request shape

```json
{
  "route": "/parallel-approval#agent",
  "resume": [
    {
      "interruptId": "perm-example",
      "status": "resolved",
      "payload": "once",
      "grant": "b4ag_opaque-example"
    }
  ]
}
```

A resolved entry carries `once`, `always`, or `deny`. A cancelled entry has no payload and means denial; it may still carry its grant. The grants extension is part of the current source surface even though the basic protocol guide’s simple examples omit it. [S04, S05]

**APR-001.** Entries MUST identify each pending interrupt exactly once. Partial, duplicate, stale, or extra sets fail without applying any decisions.

**APR-002.** Resume addresses the execution’s recorded parking route. The body cannot redirect a parked execution. In this native profile, a mismatched body route is rejected with `409 route_mismatch` rather than used as a fallback. This is a deliberate tightening of the incumbent route-resolution behavior. [S01]

**APR-003.** Public pending entries retain `interruptId`, `resumeKey`, `value`, and grant when present. Native resume keys are opaque projection identifiers; no LangGraph write parser is required to resolve them.

**APR-004.** Waiting prompts MUST survive restart. Reading them or attaching does not consume a grant. Their generic `value` is renderable data, not executable code.

### 9.2 Grant modes and consumption

Support `off`, `optional`, and `required` in the configuration contract. `required` is the native default; the incumbent default is `off`. Use explicit `off` only for migration/reference tests or deliberately configured trusted applications. [S04]

`optional` is per-interrupt: an interrupt with a grant row still requires its grant. Omitting a grant must not bypass enforcement. A required-mode park without a functioning durable grant store MUST fail before a prompt is published.

Generate high-entropy opaque tokens with a cryptographic random source. Store a hash for comparison and consumption identity. Bind each grant record to thread generation, execution, interrupt, task, parked revision, and a canonical digest of the runtime tool proposal. That digest covers the runtime call; it does not certify the version of an external business object unless the application supplied and validates that version.

The initial implementation may retain the token in a dedicated sensitive record to allow re-disclosure after restart. If retained without encryption, the **entire SQLite database and backups are sensitive**. A hash-only consumption column does not protect plaintext stored elsewhere. Never claim otherwise. The incumbent documentation explicitly acknowledges plaintext grant material in checkpoints. [S04]

Grant-bearing values MUST NOT leak through the ordinary state projection or logs. In-memory token caches alone are insufficient because pending prompts must remain answerable after restart. Encrypted token storage can be added through a separate, qualified key-management design; it is not assumed to exist here.

### 9.3 Atomic resume acceptance

The native store MUST perform the following as one transaction:

```text
verify current thread/execution/parked revision
verify exact pending set
verify all grant states, expiration, and supplied tokens
record all decisions and their verified actor/audit context
consume all relevant grants conditionally
record any scoped persistent permission decisions
mark continuation tasks ready
record continuation turn identity and pending dispatch intent
commit
```

No network calls, tool execution, or live token emission occur inside this transaction. If any check fails, all changes roll back.

This closes the incumbent’s documented gap between consuming a grant and delivering resume to the graph. It is a **new native guarantee**, not an existing exactly-once guarantee. [S04]

After commit, losing the response or process does not lose the authorized continuation. Recovery dispatches the committed intent. Repeated client submission does not dispatch it again; it returns the established consumed/stale result. This profile does not change replayed approvals into a fabricated successful `200` response.

### 9.4 Decision and scope semantics

`once` authorizes only the recorded call. `deny` and `cancelled` are consuming decisions. The tool adapter receives a denial result rather than executing the denied effect.

For the initial native tool profile, `always` persists an exact-tool permission limited to the **same application, tenant, thread generation, route, and policy revision**. It never bypasses per-call constraints or new identity authorization. Application-wide patterns and shell-command matching are deferred.

This narrowly scoped `always` is a deliberate safety restriction and requires a compatibility note. It must not be marketed as full parity with the existing permissions subsystem. A future policy adapter may support broader scopes only through explicit authorization and conformance tests.

### 9.5 Approval errors

Preserve `error.details.code` placement and the distinction between validation, authorization, and conflict:

| HTTP | Code | Meaning |
|---|---|---|
| 400 | `invalid_resume_payload` | Invalid decision or entry shape |
| 400 | `grant_required` | Required grant omitted |
| 403 | `grant_invalid` | Invalid/mismatched grant, without cause-specific disclosure |
| 409 | `interrupt_set_mismatch` | Request does not match the current pending set |
| 409 | `resume_required` | Ordinary work attempted while decisions are outstanding |
| 409 | `resume_in_progress` | Another resume admission owns the thread claim |
| 409 | `grant_consumed` | Already answered; no redelivery |
| 409 | `grant_expired` | Grant expired before acceptance |
| 409 | `grant_unavailable` | Required grant cannot be obtained/recorded |
| 409 | `stale_interrupt` | Prompt no longer addresses the parked call |
| 409 | `malformed_checkpoint` | Pending state cannot be interpreted safely |

The exact precedence for requests with multiple faults must be captured from the pinned grant and resume implementation, then recorded as a test table. Uniform `grant_invalid` responses must not grow a diagnostic `reason` field. [S04, S05]

An accepted decision does not expire retroactively. A restart after acceptance must not demand a second human approval merely because the original grant TTL has since elapsed.

## 10. Execution, parallel work, and effects

### 10.1 Minimum execution semantics

The initial kernel supports a fixed execution program with sequential transitions and a bounded parallel batch. Each task receives the same committed batch input snapshot. Task results are committed independently; aggregate state reduction occurs in stable task-definition order at the join.

Support two initial state operations: replace a field and append ordered items. Conflicting parallel replacement writes MUST fail deterministically unless the definition provides an explicit ordering/ownership rule. Arbitrary user-supplied reducers are deferred.

A successful sibling remains successful while another task parks. Its result and pending writes survive restart. A resumed sibling MUST NOT cause the whole batch to be reexecuted.

The initial batch does not release a join until its selected tasks have settled. If one task requests approval while a sibling is still running, the coordinator may expose the prompt, but it only admits the resume after the turn has parked with a stable pending set. Before then the active-turn collision rules apply.

### 10.2 Effect envelope

Each effect intent includes a stable effect ID, task ID, execution revision, canonical input/digest, required authorization scope, retry class, budget, and optional reconciliation capability. Attempts add a separate attempt ID and start/finish timestamps.

Persist the attempt's `InFlight` transition before invoking its external adapter. There is an unavoidable window between that commit and the external call: an unfinished attempt after restart may have run or may never have reached the provider. Recovery must apply the declared effect policy rather than assume either outcome. Every dispatch claim and accepted result is conditional on the expected task/attempt and owner generation.

Tool execution returns a user-visible result plus an optional state patch. These are separate from diagnostic logs and transport events.

Use these initial recovery classes:

| Class | Automatic replay after uncertain attempt |
|---|---|
| Pure deterministic | Allowed under the pinned implementation/inputs |
| Externally idempotent | Allowed only with a stable key and a qualified external contract |
| Reconcilable | Query the effect’s status before deciding whether to dispatch |
| Non-retryable/unknown | Stop in `OutcomeUnknown` / `RecoveryBlocked` |

Declarations are not proofs. The test suite must verify reference adapters, and application tools remain responsible for their external contracts.

### 10.3 Retry and streaming rules

Retries are per effect, not per entire execution. A later model failure cannot replay a successful tool. Delays are bounded, cancellation-aware, and persisted when needed for recovery.

For the initial live-model follow-on, a partially delivered model stream MUST NOT be silently restarted as if its tokens had never been shown. Without a qualified resumable-provider mechanism, that attempt fails or blocks safely. Provider billing can be repeated even when a model call has no application mutation; this is not free replay.

The default test adapter uses no live provider. A fixture miss MUST fail; it MUST NOT fall through to a network call.

### 10.4 Budget enforcement

Enforce finite limits on active executions, parked executions, queued intents, concurrent tasks, steps, tool attempts, input/output bytes, retry duration, and active turn duration. The contract capture must assign explicit admission errors and defaults to each configurable limit; admission must check capacity before committing new work. Approval waiting does not consume an active worker or hold a SQLite transaction open.

Budgets must be part of the immutable execution configuration. A resume cannot silently reset the total execution step budget and permit unbounded work.

## 11. Persistence, ownership, and recovery

### 11.1 One database and one owner

The initial deployment is one native process per application data directory, with all authoritative runtime records in one SQLite database. Use an OS-held exclusive owner lock. A second process targeting the same directory MUST refuse startup; a timestamp-based “lock file exists” check is insufficient.

This is not distributed execution. An internal owner generation and conditional writes protect against stale in-process completions and support future storage implementations; they do not make multiple replicas supported.

SQLite WAL requires same-host coordination and does not make separate attached databases one atomic transaction. Keep resume-critical records in the same database and reject unsupported network-filesystem deployments. The initial durable profile MUST use WAL mode, `synchronous=FULL`, `foreign_keys=ON`, and a finite busy timeout, and verify that the configured driver applies those settings to the relevant connections. Durability still depends on the qualified filesystem and storage stack; this is not a claim to survive arbitrary hardware failure. [E04]

### 11.2 Logical tables

| Table/group | Required content |
|---|---|
| `schema_migrations` | Applied migration IDs, checksums, format version |
| `threads` | Thread generation, ownership scope, user metadata, public status |
| `executions` | Route/revision, lifecycle, configuration digest, cancel/recovery reason |
| `turns` | Turn identity, transport origin, start/settle time, owner generation |
| `checkpoints` | Actual timestamp, schema version, values, parent, route provenance |
| `tasks` / `task_attempts` | Ready/pending/completed work, attempt lifecycle, recorded results |
| `pending_writes` | Ordered per-task state writes retained until reduction |
| `interrupts` | Parked task/revision, prompt, current pending state |
| `approval_grants` / `decisions` | Token verification state, consumption, actor, accepted command |
| `permission_decisions` | Narrow persisted `always` scope |
| `dispatch_intents` | Committed work still needing local dispatch |
| `transition_journal` | Ordered recovery/audit records; not a public token event log |

The exact DDL is a deliverable, not fixed by this table. Every mutable record must have the necessary expected-version or unique-key checks. Store revisions and effect results must remain portable Rust data, not serialized trait objects, futures, sockets, or library-owned classes.

### 11.3 Commit boundaries

At minimum, transactions MUST cover:

- Admission: create/recheck thread, record execution/turn, claim ownership, capture route/config revision, enqueue initial work.
- Task completion: record result exactly once, append pending writes, update readiness, advance checkpoint when appropriate.
- Task park: persist prompt, grant, pending task, and parking provenance before disclosure.
- Turn park: settle the active segment only after its selected work has completed or reached a durable wait state; record the stable pending set and turn outcome. A single waiting task must not mark the whole turn parked while a sibling is still mutating state.
- Resume: consume decisions and schedule continuation atomically, as defined above.
- Cancel: record cancellation against the captured turn generation; block future task dispatch.
- Settle: checkpoint/output, terminal execution/turn state, and slot release.
- Delete: remove the idle/parked native thread’s authoritative data and invalidate its outstanding approvals under one consistent lifecycle rule.

No task may be executed merely because it appeared in an in-memory queue before the admission transaction committed.

### 11.4 Restart algorithm

Before reporting readiness:

1. Acquire the data-directory owner lock and verify schema compatibility.
2. Load unfinished executions and validate their stored execution/route revisions.
3. Preserve parked executions without recreating their grants or pending identifiers.
4. Redispatch committed ready intents and accepted continuations.
5. Resolve in-flight attempts according to their effect recovery class.
6. Mark unsafe or incompatible work `RecoveryBlocked`; never silently use the latest handler revision.
7. Rebuild transient registries and live projections; record unavailable stream history explicitly.

Persist server-derived application/tenant scope and an actor/authorization reference for admitted work, not raw HTTP credentials or request headers. Recovery resolves secrets through the host and retains that scope. If a required policy check cannot be reconstructed or a current policy revokes further dispatch, block the affected work rather than downgrade to anonymous execution. An already accepted human decision remains recorded; policy revocation is not grant expiry or a refund of consumption.

A blocked thread does not necessarily make the whole service unready. The service may safely serve other threads while operator diagnostics expose the blocked execution.

### 11.5 Revision policy

The initial runtime supports restoration only with an execution definition whose recorded revision and snapshot schema are supported by the running binary. Deploying a new binary does not grant permission to reinterpret old continuations.

An application may embed old definitions to drain work. Otherwise the affected execution remains blocked. Do not import existing LangGraph checkpoints in this release. Existing TypeScript threads stay on their existing runtime and database.

### 11.6 JSON and binary data

The native contract must define finite numeric values, absent versus null fields, ordering where meaningful, and serialization failures. Rust strings cannot represent every JavaScript UTF-16 string losslessly; escaped lone surrogates must be handled by an explicit policy rather than silently normalized.

For this initial profile, reject invalid Unicode scalar input and non-JSON numeric representations. Binary tool output must use an explicit encoding or bounded artifact reference. Record these limits as native-profile restrictions. Full fidelity for arbitrary historical JavaScript payloads belongs to the future migration design, not an implicit claim in this runtime.

### 11.7 Backup and storage failure

Qualify backups through a SQLite-safe backup procedure, not a blind copy of only the main file while WAL writes are active. Restore tests must include parked prompts and accepted continuations.

If a persistence commit fails, do not emit a successful task result or terminal success. Stop scheduling affected work, surface a safe execution/storage failure, and preserve an operator-visible diagnosis. Disk-full, locked database, corrupt snapshot, and unsupported schema are required tests.

## 12. Cancellation and shutdown

### 12.1 Cancellation targets a generation

Capture the current `(execution_id, turn_id, owner_generation)` before any asynchronous authorization work. After authorization, cancel only that captured turn if it is still active. A late request must not cancel a newer run on the same thread. The existing implementation already defends this class of race. [S06]

Return `200 {"thread_id":"...","status":"interrupted"}` when accepted, `404 thread_not_found` when absent, and `409 no_run_in_flight` when the observed turn is no longer active. Do not turn the latter into idempotent success.

Cancellation is cooperative and not rollback. Stop scheduling new tasks, signal active effects, and retain committed results. Do not release the execution slot while an uncooperative native effect is still running and capable of mutation. At the configured deadline, mark an unresolved outcome accurately; do not claim the effect was undone.

A parked execution with no active turn is not an active cancel target in the compatibility profile. It can be denied through resume or deleted through authorized thread deletion. A future “abandon parked execution” command would be a separate capability.

### 12.2 Race rules

The coordinator defines a linearization point for admission, resume acceptance, completion, and cancellation. Whichever transition commits first wins within the specified state machine.

A cancellation accepted after resume admission cancels that continuation, but does not refund consumed grants. A cancel arriving before a parked execution has an active continuation may return `no_run_in_flight`; it must not be remembered and later applied to unrelated work.

A late effect completion may still contain evidence of a real external effect. Record it for reconciliation under its original task/attempt identity, but do not let it advance a cancelled execution or a newer turn.

### 12.3 Shutdown

Shutdown MUST stop admission, fail readiness, signal/drain active work, settle or classify unfinished attempts, close viewers where possible, and flush transactions. Close owned dependencies and release the owner lock only after no runtime-owned task can still use or mutate them.

Reaching the shutdown deadline is not proof of quiescence. If a non-cooperative tool remains active, an embedded runtime must report incomplete shutdown and retain the necessary ownership/resources; it must not release the lock so a replacement runtime can race that tool. A standalone host may escalate to process termination under its supervisor, after which restart recovery handles uncertain attempts. Dropping a future or timing out a join does not establish that an external effect stopped.

Embedded mode MUST use the host’s async runtime and MUST NOT call process exit or close externally owned shared resources. Expose explicit shutdown and ownership options. Tokio’s cancellation and task-tracking patterns provide mechanics, not the domain policy. [E02]

The shutdown deadline must be bounded and tested. A hard kill remains a distinct recovery case.

## 13. Error contract and observability

### 13.1 Error envelope

Preserve b4’s envelope and code placement: [S07]

```json
{
  "error": {
    "kind": "request_error",
    "message": "A run is already in flight for this thread",
    "details": { "code": "run_in_flight" }
  }
}
```

`error.kind` is `request_error` or `execution_error`. Registry-level `error.code` and `docsUrl` are separate optional fields. Do not invent a registered `B4_E...` code or documentation URL for a new native error.

The native profile additionally defines detail codes for `route_mismatch`, `recovery_required`, `capability_not_supported`, `payload_too_large`, and `shutting_down`. These are proposed details codes until incorporated into the project’s formal error inventory.

Public errors must not contain secrets, raw SQL, arbitrary panic messages, stack traces, or unverified tenant/route information. Correlate diagnostics through a response header and structured logs rather than changing every legacy response body.

### 13.2 Required diagnostics

Record execution/turn/task/effect identities, revision, lifecycle transition, duration, attempts, cancellation cause, approval decision receipt, recovery classification, and stream truncation/detach events.

Prompt content, model content, tool arguments, tool outputs, and grants MUST be redacted by default. Diagnostic logging must not be the only durable record of an accepted approval.

Provide local operator commands for inspecting blocked executions and printing the capability/contract version. A dangerous “force replay unknown effect” command is not part of the initial release. Operator recovery must not bypass effect safety through an undocumented flag.

## 14. Conformance and testing

### 14.1 Test layers

| Layer | Required evidence |
|---|---|
| Kernel | Pure transition tests, deterministic schedules, invalid transitions, budget accounting |
| Serialization | DTO/schema agreement, omission/null cases, Unicode, SSE encoding |
| Storage | Same semantic suite against memory and SQLite; rollback, stale revisions, restart |
| HTTP | Actual requests against an in-process router and a real loopback server |
| Compatibility | Pinned TypeScript and Rust against shared supported scenarios |
| Concurrency | Controlled race schedules for admission, resume, cancel, attach, completion |
| Recovery | Separate process, durable files, deterministic failpoints, hard termination |
| Security | Cross-tenant access, route provenance, forged metadata, grant replay, malformed input |
| Packaging | Clean native/container execution without Node or a JS engine |

Mock-only handler tests are not sufficient. A task panic caught in the same process is not a substitute for process-kill recovery.

### 14.2 Fixture format

Use versioned JSON or JSONL fixtures independent of TypeScript and Rust libraries. Record canonical commands/inputs, scripted effect outcomes, expected observable events, expected persisted invariants, and explicit failure points.

Illustrative fixture outline:

```json
{
  "schema_version": 1,
  "id": "parallel_approval_restart",
  "profile": "b4-ap-core/0",
  "route": "/parallel-approval#agent",
  "steps": [
    { "op": "start_stream", "input": {} },
    { "op": "await", "signal": "tool_a_result_committed" },
    { "op": "await", "signal": "approval_park_committed" },
    { "op": "kill_process" },
    { "op": "restart_same_revision" },
    { "op": "read_pending_interrupts" },
    { "op": "resume", "decision": "once" },
    { "op": "assert", "invariant": "tool_a_effect_count_equals_one" }
  ]
}
```

This is a proposed fixture grammar, not an already available command language. Freeze it through SPEC-003 and validate fixtures before execution.

### 14.3 Comparison rules

Compare status codes, error categories/codes, JSON structure and values, meaningful omissions, persisted results, and causal event order. Normalize only explicitly nondeterministic values such as timestamps and random IDs, while checking their format and cross-message identity consistency.

Do not demand identical TCP chunk boundaries or provider token segmentation. Do not sort arrays whose order is meaningful. Do not normalize away duplicate tool execution, missing approval, or different terminal outcomes.

A test result must be `pass`, `fail`, or `excluded_by_profile` with a reason. An excluded extension must not inflate a full-protocol compatibility percentage.

### 14.4 Required acceptance scenarios

| Test ID | Scenario and required result |
|---|---|
| T-001 | Create/read thread; metadata and timestamps round-trip. |
| T-002 | Empty thread has no fabricated state checkpoint. |
| T-003 | Run implicitly creates a thread only after successful authorization. |
| T-004 | Unknown/missing-kind route is rejected without execution. |
| T-005 | Malformed JSON uses AP errors, not framework defaults. |
| T-006 | Omitted/null/scalar input behavior matches the frozen contract. |
| T-007 | Same-thread concurrent admissions have one winner. |
| T-008 | Different threads can execute concurrently within global limits. |
| T-009 | Successful wait returns the output directly; no output is JSON null. |
| T-010 | Stream events have correct direct payload framing. |
| T-011 | Every normal stream turn has one terminal frame. |
| T-012 | Disconnect stream and wait; admitted work still completes. |
| T-013 | Attach starts with a state frame and never triggers execution. |
| T-014 | Event arriving at snapshot/subscription boundary is not lost. |
| T-015 | Attach overflow marks truncation instead of false completeness. |
| T-016 | Viewer capacity/queue overflow affects viewers, not the run. |
| T-017 | Heartbeats and arbitrary UTF-8/network splits parse correctly. |
| T-018 | Pending approval is durable before disclosure. |
| T-019 | Reload/restart returns the same pending identity and grant. |
| T-020 | Complete approval set succeeds; partial/extra/duplicate sets do not mutate state. |
| T-021 | Resolved/cancelled entries have strict key and payload validation. |
| T-022 | Missing, invalid, expired, consumed, and unavailable grants have correct responses. |
| T-023 | Denial consumes the grant and never executes the denied tool. |
| T-024 | Optional mode does not permit omission for a grant-bearing interrupt. |
| T-025 | Concurrent resume has one accepted decision batch and correct conflict classification. |
| T-026 | Crash before resume transaction commit leaves every decision pending. |
| T-027 | Crash after resume commit but before dispatch loses no accepted continuation. |
| T-028 | Replayed resume executes no additional tool. |
| T-029 | Parallel sibling result survives an approval park and restart. |
| T-030 | Crash after external effect but before result commit does not replay unsafe work. |
| T-031 | Qualified idempotent/reconcilable effect uses the same logical effect ID on recovery. |
| T-032 | Repeated result delivery cannot duplicate state reduction. |
| T-033 | Parallel conflicting replacement writes fail deterministically. |
| T-034 | Cancel preserves committed state and stops new scheduling. |
| T-035 | Late cancel cannot target a replacement turn. |
| T-036 | Cancel/resume races obey committed order without reusing approval. |
| T-037 | Uncooperative effect cannot free the slot or data-directory ownership while still mutating. |
| T-038 | Wait cancellation and SSE cancellation have their distinct terminal contracts. |
| T-039 | Wrong-tenant reads, streams, resumes, cancels, and deletes disclose no protected data. |
| T-040 | Forged user metadata cannot alter route origin or ownership. |
| T-041 | Grant validation is not an existence oracle before authorization. |
| T-042 | Mismatched route cannot redirect a parked execution. |
| T-043 | Unknown checkpoint origin fails closed on attachment. |
| T-044 | New binary with unavailable old revision blocks rather than reinterprets continuation. |
| T-045 | Store failure/disk full never produces false successful completion. |
| T-046 | Second process opening the same runtime directory refuses ownership. |
| T-047 | Shutdown releases resources only after quiescence; deadline with a live tool reports incomplete shutdown and retains ownership. |
| T-048 | Hard kill does not leave an unrecoverable stale busy flag. |
| T-049 | Database backup/restore retains parked and accepted decisions. |
| T-050 | Disabled memory/workspace capabilities never report fabricated success. |
| T-051 | Production boot without authentication/policy is rejected. |
| T-052 | Logs, errors, and ordinary state never disclose approval tokens or credentials. |
| T-053 | Deterministic fixture miss cannot make a live provider request. |
| T-054 | Active/parked limits and total execution budgets survive resume. |
| T-055 | Clean binary/container run requires no Node installation. |
| T-056 | Thread deletion removes/inactivates native state consistently; stale IDs cannot inherit prior authority. |

Property-based tests SHOULD explore valid command sequences and assert invariants after each transition. Seeded concurrency tests must record the schedule needed to reproduce a failure. Longer randomized/soak lanes supplement, not replace, deterministic CI gates.

### 14.5 Evaluation versus infrastructure correctness

A deterministic scenario passing is evidence about runtime behavior, not model quality. Later model evaluations must separately assess prompt/provider fidelity, task success, tool choices, cost, and latency.

The initial release gate contains no LLM judge. Approval correctness, isolation, or absence of duplicate effects cannot be decided by an LLM grader.

## 15. Configuration, resource limits, and deployment

### 15.1 Initial configuration

Use a typed configuration with explicit application revision, bind address, data directory, authentication mode, grant mode, resource limits, and registered definitions. Configuration is scoped to a runtime instance; do not reproduce process-global mutable identity or model configuration.

Suggested initial defaults, all **proposed** except the cited attach/heartbeat values:

| Setting | Default |
|---|---|
| Bind address | Loopback only |
| Authoritative store | One local SQLite database |
| Approval grants | `required` |
| Run/resume/create body bound | 1 MiB |
| JSON nesting bound | 64 levels |
| Active turns | 32 per runtime |
| Concurrent tasks | 8 per runtime, lower per execution permitted |
| Active-turn deadline | 5 minutes; configurable |
| Turn frame buffer | 2 MiB [S06] |
| Attached viewers | 16 per thread [S06] |
| Viewer queue | 256 frames and 1 MiB, whichever is reached first |
| SSE quiet heartbeat | 15 seconds [S06] |
| Graceful shutdown deadline | 30 seconds |
| Grant TTL | Explicit app option; omitted means no automatic expiry |

These are starting safety limits, not measured capacity claims. Verify whole-runtime memory bounds, including active threads, viewers, task outputs, and queued serialization. A bounded per-viewer queue does not establish a bounded process if the number of viewers or threads is unbounded.

Oversized tool output must fail or use a future artifact mechanism; the initial runtime cannot silently truncate authoritative tool results. Wire-history truncation is a different operation.

### 15.2 Binary and embedding

Provide a native `serve` command, a config validation command, and read-only local inspection of runtime status/blocked executions. Command names such as `b4d serve` and `b4d inspect` are proposed; they are not claims about existing CLI compatibility.

A compiled application registers its Rust routes and assembles the runtime. Do not require dynamic-library loading or assume Rust has a stable plugin ABI.

The embeddable API returns a router/service plus an explicit shutdown handle. It does not create a second Tokio runtime. Ownership of externally supplied stores/authentication dependencies must be declared.

### 15.3 Qualification targets

Initial required deployment qualification is a native process and a Linux container with a persistent local data volume. Qualify developer execution on macOS where the team develops; other OS/architecture builds are supported only after their suites pass.

Use a non-root runtime identity, owner-restricted data files, health/readiness probes, and a persistent volume. Do not expose the SQLite file or use an ephemeral filesystem while advertising restart durability.

No Kubernetes orchestration, serverless runtime, or multi-region claim is included in the core release. A container running successfully is not evidence for those additional targets.

## 16. Implementation sequence and gates

| Milestone | Work | Exit criterion |
|---|---|---|
| M0 — Contract capture | Reference manifest, schemas, characterization, decisions, test inventory | Preserved versus changed behavior is explicit; no unresolved core parsing/terminal shapes |
| M1 — Pure kernel | IDs, lifecycle, task batches, reducers, fake clock, snapshot versioning | Deterministic transition suite; no I/O dependencies |
| M2 — Transactional runtime | Memory/SQLite stores, owner lock, admission, dispatch intents, recovery | Backend conformance and crash-boundary tests |
| M3 — AP server | Threads/state/wait/stream/attach/probes/errors, auth gates | Real HTTP/SSE tests and selected TypeScript comparisons |
| M4 — Safe continuation | Grants, full-set resume, permission scope, cancel races, parallel recovery | Decisive kill/restart/approval test plus uncertain-effect test |
| M5 — Qualification | Limits, logging/redaction, packaging, backup/restore, final conformance | Core-profile release gate passes with native artifacts |
| M6 — Live-agent follow-on | First provider adapter, then a contrasting provider; actual model/tool loop | Same protocol/recovery tests plus provider fidelity tests |

M1–M4 may overlap once their contracts exist, but M6 must not redefine the protocol to accommodate an early provider choice. A live chat demo before M5 is not evidence that the native foundation is ready.

### 16.1 First implementation backlog

| Work item | Depends on | Deliverable |
|---|---|---|
| B4N-001 | — | Capture pinned AP request/response and SSE examples |
| B4N-002 | 001 | OpenAPI, JSON Schemas, and behavior-decision register |
| B4N-003 | 002 | Rust DTO/newtype and schema conformance tests |
| B4N-004 | 003 | Pure kernel and lifecycle property tests |
| B4N-005 | 004 | In-memory transactional store and scripted adapter |
| B4N-006 | 005 | SQLite migrations, transaction contract, owner lock |
| B4N-007 | 006 | Admission/dispatch/recovery service and failure points |
| B4N-008 | 003, 007 | Authenticated thread/state/wait endpoints |
| B4N-009 | 008 | SSE encoder, stream, bounded attach, snapshot cut |
| B4N-010 | 006, 007 | Durable grants and atomic resume batch |
| B4N-011 | 009, 010 | Cancellation, shutdown, late-result handling |
| B4N-012 | 011 | Decisive parallel-approval recovery suite |
| B4N-013 | 012 | Packaging, backup/restore, capability report, release receipt |

Each completed item requires code, tests, contract updates where relevant, and a verification receipt. Parallel contributors must not independently invent error shapes, identity schemes, or task state machines.

## 17. Definition of done

The initial runtime is complete only when:

**Protocol:** All ten core operations and operational endpoints satisfy their frozen schemas and lifecycle tests. Deferred endpoints are accurately reported. Raw SSE and reconnect behavior pass with an independent parser.

**Semantics:** Completed tasks are not repeated on approval resume; decisions are atomically accepted with continuation work; cancellation cannot retarget a later run; malformed or unsupported persisted state fails closed.

**Recovery:** The decisive process-kill tests pass, including the external-effect/commit ambiguity case. No claim of exactly-once arbitrary external effects appears in the API, docs, or tests.

**Security:** Production requires authentication/resource policy; protected reads and mutations are gated; grants are protected against replay and accidental disclosure; provenance is not derived from client metadata.

**Operations:** The binary starts, stops, backs up, restores, and runs in a qualified container without Node. Limits, logging, dependency ownership, migrations, and revision incompatibilities have explicit behavior.

**Evidence:** A machine-readable result lists every requirement and test as pass/fail/excluded. No critical core requirement is “assumed passed.” No live-provider success substitutes for infrastructure correctness.

Suggested verification commands below are an intended tooling interface, not commands already available in a scaffold:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
cargo run --locked -p b4-testkit --bin b4-conformance -- --target native --profile b4-ap-core/0
cargo run --locked -p b4-testkit --bin b4-conformance -- --compare-reference conformance/reference/manifest.json
cargo run --locked -p b4-testkit --bin b4-conformance -- --suite process-recovery
```

The proposed `b4-conformance` executable belongs to `b4-testkit`; it need not become another published crate.

## 18. Characterization tasks and deliberate differences

### 18.1 Must resolve during M0

The following were not established completely by the selected source reads and must be captured before freezing the implementation contract:

| Item | Required observation/decision |
|---|---|
| CP-001 | Exact HTTP parsing/headers/status for empty body, wrong content type, unknown root keys, and duplicate JSON keys |
| CP-002 | Run input absent versus null versus scalar/array; route-specific validation timing |
| CP-003 | Exact done/output body for permission parks, runtime failures, and void outputs |
| CP-004 | Exact terminal/settle behavior of the wait route versus stream for each fixture |
| CP-005 | Error precedence across invalid grant, stale pending set, concurrent claim, and expired token |
| CP-006 | Delete behavior for missing, active, parked, and partially missing native resources |
| CP-007 | Full attach state shape, frame merging, retry hint, and subscriber detach ordering |
| CP-008 | Which public state metadata fields and pending-write channels real AP clients depend on |
| CP-009 | Exact token shape validation and disclosure paths in the grant-enabled source profile |
| CP-010 | The actual AP-only client corpus to qualify; no AG-UI client is silently counted as AP |

These are bounded executable-characterization tasks. The draft is sufficiently specific to start M0 and the kernel design, but calling it already frozen wire compatibility would overstate the evidence.

### 18.2 Deliberate native-profile differences

| Difference | Reason and required visibility |
|---|---|
| Grants required by default | Stronger default; legacy mode is explicit |
| Atomic decision plus continuation commit | Avoid losing accepted approval between stores/dispatch |
| Persisted execution/task/effect identities | Recovery independent of JavaScript runtime internals |
| Outer production authentication required | Cover the entire exposed service |
| Body route mismatch rejected on resume | Prevent redirection/fallback ambiguity |
| Narrow thread-scoped `always` decisions | Initial permission profile, not broad subsystem parity |
| New requests blocked while a logical execution awaits approval/recovery | Preserve one safe continuation rather than trusting coarse wire status |
| Explicit body/JSON/output limits | Bound resource use |
| Native JSON/Unicode restrictions | Avoid silently corrupting values Rust cannot represent |
| Larger opaque generated thread IDs | Do not preserve a weak entropy choice as a compatibility obligation |
| Single process/data-directory lock | Enforce the supported ownership model |
| New native storage schema; no old checkpoint import | Honest migration boundary |
| Memory API explicitly unsupported | Do not fake platform coverage |

Any additional behavior discovered during implementation must join this register before release. It must not be explained away as a minor consequence of changing languages.

## 19. Guidance for the implementation agent or engineering team

Implement this as a sequence of verifiable contracts, not a source translation.

Start by capturing the pinned AP behavior and building the deterministic fixtures. Add a pure kernel and transaction-oriented store. Make the scripted adapter exercise the real runtime. Then expose the HTTP surface through the same services and prove the recovery/cancellation/approval invariants with real process failures.

Do not start by adding provider SDKs, implementing every graph primitive, creating browser components, or replacing the existing TypeScript deployment. Do not couple the persisted continuation to Rig, LangGraph, a Rust future, or an HTTP connection. Do not hide unsupported capabilities behind successful placeholders.

The first reviewable success is a **native execution substrate with an executable protocol contract**. A live model/tool loop can then be added without redefining thread identity, approval authority, commit ordering, or recovery semantics.

## 20. Evidence and references

The parent research and selected current source files informed this draft. No Rust implementation, TypeScript reference suite, provider call, database migration, or deployment was executed in preparing it. The code and command examples are proposed interfaces, not tested APIs.

All b4 source references below use the same pinned commit. Live external documentation was consulted on 30 September 2026. The draft deliberately separates observed compatibility details from new design requirements.

| ID | Source | Reference |
|---|---|---|
| R1 | Parent research: architecture, durable effects, client protocols, and initial backlog | `b4dotrun-rust-feasibility-and-migration-report-2026-09-30.md`, particularly Sections 6–7, 10–13, 18, and 21 |
| S01 | b4 Agent Protocol guide; endpoint inventory and lifecycle | `https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/apps/web/content/docs/dev-server/agent-protocol.mdx` |
| S02 | Thread DTO and storage interface | `https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/sqlite-storage/src/threads/store.ts` |
| S03 | Raw AP stream types and SSE encoding | `https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/runtime/stream-types.ts` |
| S04 | Approval grant modes, client schema, errors, storage limitations | `https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/apps/web/content/docs/approval-grants.mdx` |
| S05 | Pending interrupt parsing and resume resolution | `https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/pending-interrupts.ts` |
| S06 | Runtime fetch core; selected boot, probe, thread, cancel, state, and stream-admission ranges | `https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/runtime-fetch-core.ts` |
| S07 | Runtime HTTP error body construction | `https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/server-errors.ts` |
| E01 | Axum SSE primitives; implementation foundation only | `https://docs.rs/axum/latest/axum/response/sse/index.html` |
| E02 | Tokio graceful shutdown and task tracking | `https://tokio.rs/tokio/topics/shutdown` |
| E03 | WHATWG Server-Sent Events format | `https://html.spec.whatwg.org/multipage/server-sent-events.html` |
| E04 | SQLite WAL transaction/deployment constraints | `https://www.sqlite.org/wal.html` |

---

**Proposed release criterion:** a Rust-native Agent Protocol core runtime whose state, authority, side effects, and recovery behavior are specified and tested—not merely a server that streams plausible text.
