# Request correlation and bounded diagnostics implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and
> superpowers:test-driven-development, followed by independent SPEC then QUALITY
> review. The user authorizes continuous local reversible work and routine
> decisions. No routine approval handoff, Git mutation, deployment or publication
> is selected. Use the existing authorized workspace and exclusive file ownership.

**Goal:** Add server-generated request references and a bounded redacted diagnostic
export API without changing AP bodies or request/worker/shutdown authority.

**Architecture:** A resource-free diagnostics handle owns an entropy seed, checked
sequence, try-lock ring and loss flags. A thin fallback wrapper observes response
construction or future drop without reading body/URI/private context. Destructive
drain encodes fixed DTOs outside the ring lock into a capped owned byte vector.

**Tech Stack:** Existing Rust1.93.1, axum0.8.9, serde/serde_json, std primitives,
and one new HTTP dependency edge to already locked getrandom0.3.4. No new package,
version, checksum or feature. Design: spec/runtime/request-diagnostics-design.md.
Schema: spec/runtime/request-diagnostic-batch.schema.json.

## Task1: Baseline and independently reviewed design (root)

- [x] Read the final stopped-inspection independent audit and freeze its exact
  receipt89dfb6a...; capture518 baseline artifacts/210 protected artifacts before
  any public next-increment write. Preserve all inspection/reference receipts.
  Current root Cargo.lock is in that protected set: Task4's already-selected
  one-entry edge is its only explicit exception;209 other protected paths stay
  byte-exact, with the lock independently reversed to its original protected hash.
- [x] Capture fresh locked/offline Cargo metadata and feature tree privately.
- [x] Examine RFC13.1/13.2/M5 and current host/transport/lifecycle plus pinned client
  sources. Select request boundary records; CP010/identity/log-delivery stay open.
- [x] Independently review the complete design/schema/plan for correctness and
  implementability, repair findings and mark design accepted before implementation.
- [x] Validate complete schema with local Draft202012Validator; independently
  calculate256-record worst-case compact width below65536. The width proof is a
  serializer bound, not a fabricated valid Store fixture or total RSS guarantee.

## Task2: Baseline observable RED (qualification author)

**Exclusive files:** new crates/b4-http/tests/request_diagnostics.rs and its
request_diagnostics_support helpers; new request_diagnostics_tcp.rs if useful.
New crates/b4-cli/tests/request_diagnostics_cli.rs and separate support helpers.
No existing qualifier or production file modifications.

- [x] Add a compiling baseline test using the current real MemoryStore HttpHost
  and fallback response: expect x-b4-request-id and fail because it is absent.
  Record private meaningful RED before root wrapper or module production changes.
  Run: cargo test --locked --offline -p b4-http --test request_diagnostics
  response_has_server_generated_request_reference
  Expected: behavior assertion failure, working host/setup/compilation.
- [x] Coordinate the author barrier after saving RED. No API-not-found/setup/
  permission/entropy compilation failures count as meaningful RED.

## Task3: Correlation, ring, observation guard and export (diagnostics author)

**Exclusive files:** create crates/b4-http/src/diagnostics/mod.rs, buffer.rs,
observation.rs and private focused test files. Root owns existing lib/host/transport,
HTTP manifest and Cargo.lock. Keep producer/export units small and auditable.

- [x] After baseline RED, agree exact public/crate interfaces from design:
  RequestDiagnostics::new()->Result<Self, ConfigError> (crate-only);
  start(&Method)->RequestObservation (crate-only); finish(self,&mut Response);
  public RequestDiagnostics::drain()->Result<DiagnosticBatch,DiagnosticsReadError>;
  DiagnosticBatch:AsRef<[u8]>; public constants/header/error code inventory.
- [x] Supply compiling refusal/empty-observation stubs, then add behavior tests
  expecting header/one real record, FIFO eviction and real encoded drain. Capture
  meaningful RED with no implementation behavior. Root exposes stub module/edge.
- [x] Implement fallible independent128-bit host seed, checked monotonic sequence
  1..u64MAX, exact53 ASCII ID, persistent exhaustion/loss. Never read inbound IDs.
  Private deterministic seed/failure/exhaustion controls only; no public authority
  or caller-supplied record/seed/counter/cap/sink API.
- [x] Implement fixed method/event/status/elapsed DTOs and observer guard. Pending
  drop records once with null status; finish consumes before best-effort insertion.
  Header remains available if ring is full/busy/poisoned. Future drop is not
  execution cancellation or stream/physical completion. No body/URI/private reads.
  Use fallible HeaderMap::try_insert; unexpected construction/insertion failure
  drops the already-consumed observation and sets loss, with unchanged status/body.
- [x] Implement fixed256 try-lock ring, oldest eviction, locked saturating cumulative
  eviction count and atomic persistent loss flags. Exact256/257/drop/drain order;
  producer contention and poison drop observation without changing readiness.
  Use preallocated fixed arrays and Copy records with inline53-byte IDs; no
  allocation or replacement queue reallocation while the ring lock is held.
- [x] Implement drain try-lock Busy/Unavailable static errors; take all at most256
  under ring lock, release lock before fixed-DTO compact serialization. Owned bytes
  cap65536/no newline/no resources. Encoding refusal marks loss after destructive
  take and emits no arbitrary serde text. Metadata persists across drain.
- [x] Verify private actual held-lock contention, actual poison, sequence exhaustion,
  counter saturation, guard exactly-once drop, seed failure mapping and max width/
  capped writer N/N-1. Diagnostic-only injection is labeled separately from an
  actual OS entropy/header-allocation failure.
- [x] Run focused tests then strict HTTP all-target lint/format. Root reads complete
  module/tests for SPEC and assigns different author complete QUALITY review.
  Freeze exact files and RED/GREEN logs. Do not self-approve implementation.

## Task4: Thin host/transport integration and unchanged graph (root)

**Existing files:** crates/b4-http/src/lib.rs/host.rs/transport.rs/Cargo.toml and
Cargo.lock. No other existing production/test/schema/SQL/CLI source changes.

- [x] After Task2 RED, add only getrandom="=0.3.4" to HTTP dependencies and only
  its HTTP dependency listing in Cargo.lock. Existing package/checksum/features
  stay exact; validate with locked offline metadata before compiling stubs.
- [x] Register/export module and types/constants, add only diagnostics field to
  private Shared and public HttpHost::diagnostics() returning resource-free clone.
  HostConfig/NativeHostConfig struct layouts remain unchanged. Constructor runs
  validation before seed generation and seed before build_registered readiness.
- [x] Keep complete old endpoint body byte-exact in endpoint_inner; wrapper starts
  observation from request.method(), awaits old body and finishes with Response.
  No new Store/authority/kernel/task/admission branch or body/classifier/parsing.
  Move original State/Shared into inner without retaining any outer Shared clone.
  Add a private controlled finish hold at the DIRECT transport-wrapper boundary
  proving weak Store drops while the guard remains alive, without a Router state
  capture. Pinned axum Handler::call retains its own existing state across await;
  qualify real router/server retirement separately. No extra wrapper Store capture.
- [x] Before guard implementation, record baseline header test still RED against
  compiling wiring/stub. Implement only after relevant component RED evidence.
- [x] Run existing HTTP suite and focused module/integration checks. Independently
  reconstruct old endpoint by removing only wrapper/renaming function; compare
  all171 prior crate paths, with exactly four authorized HTTP changes, and all
  protected artifacts against baseline:209 byte-exact plus the independently
  reconstructed selected Cargo.lock exception. Cargo.lock is the fifth edit.
- [x] Independent SPEC/QUALITY of root integration/manifest/lock, with exact graph
  edge normalization (new integration targets are metadata, not packages). Repair
  and freeze source. No infallible request-path random call or logging I/O.

## Task5: Independent actual request, privacy and retirement qualification

**Exclusive files:** Task2 qualification files, separate from module/root source.

- [x] Expand actual header/event equality tests for probes/success/auth denial,
  policy denial/scope mismatch/invalid IDs/parser/input bounds/unsupported/missing,
  host draining and infrastructure faults, preserving exact existing JSON bodies.
- [x] Supply caller correlation header/Authorization/cookies/secret-like path and
  payload/route/identity/grant/failure/SQL values in real source fixtures; encoded
  batches must include none. Method records only four fixed labels.
- [x] Concurrent real requests and independently constructed hosts have unique
  correlation IDs; clone/drain partitions records with no duplicate/double event.
  FIFO follows actual insertion/completion, not sequence-start order.
- [x] Actual256 successes retain256;257th evicts exactly oldest, loss true/count1.
  Drain resets only queue; loss/eviction metadata persists. Empty/one/full exports
  validate exact schema, ASCII ID, conditional status, length/framing and cap.
- [x] Use controlled real owned worker holds: SSE response-ready record before body/
  effect settles; drop polled wait request future before effect release and observe
  exactly one request_future_dropped/null event while real exclusion persists.
  Assert no diagnostic event claims execution/transport termination.
  An abort alone does not guarantee a record before guard Drop actually runs;
  existing trusted destructor disposal can retain future cleanup.
- [x] Keep handle/batch alive while MemoryStore weak reference expires and actual
  private SQLite host/server positively retires; actual writer then reacquires.
  Full buffer/consumer activity cannot add shutdown leases or delay quiescence.
- [x] Real loopback + shipped native demo release process responses carry the safe
  header, early errors/probes unchanged, no raw secret diagnostics. No CLI export
  claim: launcher buffer is bounded but not automatically delivered as logs.
- [x] Root SPEC then independent full QUALITY of all qualifier code/assertions,
  fresh debug/release checks, exact freeze; old tests untouched and repeats labeled.

## Task6: Fresh scope, docs and evidence (root)

**Files:** additive runtime docs/new ADR/runbook, scoped requirement section,
schema checks as needed and a new verification receipt. Never rewrite history.

- [x] Qualify complete current Mac format/strictlint/workspace + actual shipped
  release old12/inspection19/new request cases. Repeat on current-source nonroot
  Linuxarm64 container, verify compiled/fixture hashes and exact image identity.
- [x] Retained Python71/Node50/contracts318 plus only necessary new schema/contract
  checks pass; trace actual executed new function declarations only. Keep56broad
  scenarios/M0/CP/M5/core open and distinguish memory records from delivered logs.
- [x] Document API/header/method/future-drop semantics/loss/entropy/exhaustion/
  nonauthority/record bounds/drain errors/resources/CLI export deferred. Preserve
  prior inspection schema/examples/implementation and209 byte-exact protected
  paths plus the exact one-entry reversible root Cargo.lock exception.
- [x] Build new source/package-edge/platform/count/RED/GREEN/review receipt; no raw
  source/authority/token values or database-derived digests. Independently audit
  entire inventory/privacy/scope/counts, repair and freeze. Continue authorized work.

Progress: Tasks1–6 implementation, qualification, documentation and scoped
candidate evidence review completed. Independent SPEC/QUALITY approves the current
module/wiring,21 HTTP/2 native CLI qualifiers, batch checker and20 requirement
mappings. Fresh macOS/Linux arm64 tests pass814 Rust entries per platform with3
unchanged ignored helpers,33 release cases each,88 Python and50 retained Node
checks. The13 retained actual Linux container cases pass on rebuilt images,
with217 required source/fixture hashes and182 crate paths exact, positive runtime
retirement and owned resources cleaned up. The complete independent candidate
evidence review passes, including the separately bound exact README relabel
reconstruction. The final main inventory and status bridge are rebuilt for the
separate non-author freeze audit; its decision is recorded externally without a
self-referential hash. Request-boundary/log-delivery limits and broad open gates
remain documented. Inspection stays frozen at89dfb6a with final audit2eed169a.
Continue authorized local work after the final independent freeze decision; no
manual action or additional permission is needed.
