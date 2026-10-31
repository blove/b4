# Native cooperative cancellation implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement task-by-task after accepted design and the preceding phase's final freeze. Continuous local implementation is authorized; whole independent document SPEC and QUALITY review precedes implementation.

**Goal:** Let an entered native callback observe its exact accepted durable cancellation and honestly acknowledge positive cooperative return without breaking existing native APIs or releasing ownership early.

**Architecture:** A per-original-RunControl signal shares only an AtomicBool with read-only observer clones. An additive default native entry wraps old outcomes; optional Cancelled acknowledgement is separately validated by the exact host and committed through the existing driver and CancelAttempt. Old invocation, enums, context fields, storage, AP projection and physical ownership remain.

**Tech stack:** Existing Rust/std atomics, Arc, finite test gates, existing runtime/HTTP/Axum/Tokio/Memory/SQLite dependencies; no manifest/lock/feature additions.

## Task 1: Finish scope and preserve the predecessor

- [x] Root completes the current unchanged-AP-client receipt/final freeze before public design adoption or any implementation.
- [x] Recapture the current complete repository/protected baseline and all existing crate sources; retain only the five explicit existing crate exceptions and four existing noncrate support exceptions from the design: `README.md`, `spec/runtime/compatibility.md`, `spec/runtime/requirements.yaml`, `scripts/check_contracts.py`. The draft baseline is provisional because predecessor documentation/final receipts are still parent-owned.
- [x] Independently review the whole public design/plan, including sticky accepted-signal behavior after a late fault and the cross-crate trusted publisher. Correct concrete findings; continuous user authorization already covers routine local choices.
- [x] Keep old NativeOutcome/InvocationContext/error shapes and old invoke semantics as explicit acceptance assertions; preserve old schemas/examples/dependency graph. No Git operation is needed.

## Task 2: Portable additive API and meaningful RED

**Files:** New `crates/b4-runtime/src/application/control.rs`, new `crates/b4-runtime/tests/native_controlled.rs`; selected existing application model/mod/registry only.

- [x] Add compiling minimal API stubs: initially-false observer/signal pair, new controlled outcome, default native method mapping old invoke to Settled, and additive application method with unchanged exact validation. Deliberately stub signal publication so a real state assertion can fail; record stub source privately before GREEN.
- [x] New tests use old required-only trait implementation and exhaustive old enum matches, unchanged InvocationContext/registration literals. Assert old NativeApplication::invoke calls only old invoke even when controlled override differs; controlled default returns exactly the old success/failure and Void/null behavior.
- [x] Write positive signal monotonicity and clone tests: requested false initially and after producer drop; expected true after explicit producer publication, including retained clones. Run `cargo test --locked --offline -p b4-runtime --test native_controlled`; require compiling assertion RED at missing publication, not unresolved types.
- [x] Implement only Arc<AtomicBool> state and Release/Acquire producer/observer methods. `NativeCancellationSignal::default()` delegates to the initially-false `new()` to satisfy existing strict lint. No source-capture, identity, closure, asynchronous wait or I/O. Add private/fixed Debug, no serialization or observer mutator.
- [x] Test controlled application refusal before callback for full Definition/context/task/effect mismatch; success/failure/output bounds continue through existing driver contract. False observer + Cancelled returns existing static Uncertain. Real observer true + Cancelled passes portable contract, while docs state trusted caller still owns durable authorization.
- [x] Run focused runtime tests, format, strict lint; freeze portable source for whole independent SPEC and different-author QUALITY. Do not claim HTTP behavior yet.

## Task 3: Independent actual-native/TCP missing-notification RED

**Files:** New `crates/b4-http/tests/native_cooperative_cancellation.rs`, `native_cooperative_cancellation_tcp.rs` and new support files only. An independent qualifier author owns these; production author cannot generate their expected snapshots from implementation.

- [x] A controlled native callback stores an observer clone and announces actual entry. It can poll the observer and has an independently held finite return gate. Use real existing Memory/SQLite/native host and authenticated loopback `POST /threads/{id}/cancel`.
- [x] Capture compiling positive behavior RED before HTTP wiring: durable cancel succeeds and prompt wire terminal occurs, but callback's supplied observer remains false or new method is not invoked. Assert the missing notification after bounded observation; then explicitly release and normally join test work. Cleanup release/expiry never counts as positive return. Preserve RED log and stub/source state.
- [x] Gates use one absolute lifetime per generation and sticky expiry, explicit-release and actual-return markers. Give all waits finite bounds; mark teardown releases separately; no production test seam. No timing-only assumption of entry—prove callback entered, durable task InFlight and accepted exact cancellation.

## Task 4: Narrow host notification and acknowledgement mapping

**Files:** Selected `crates/b4-http/src/control.rs`, `worker.rs`; no edits to cancellation.rs/driver/kernel/store/transport/lifecycle/claim.

- [x] Each original RunControl owns one signal; expose only a cloned observer for controlled callbacks. Successor gets a new signal. Keep legacy script adapter and existing never-entered Cancelled path.
- [x] Publish in the existing successful Pending->Committed branch only after Store success, before its single terminal. Atomic publication must be infallible and call no application code. Reject/fault/Pending/shutdown/viewer paths never publish; accepted signal is sticky after later fault.
- [x] Native wrapper preserves exact call identity/membership and entry gate, invokes the additive application method, and maps genuine settled old variants as before. Map Cancelled only with the wrapper's original accepted signal and unchanged exact identity checks. False acknowledgement returns static uncertainty/fault, not known failure/success.
- [x] Keep `RunControl::committed()` semantics untouched. A sticky accepted signal may validate a positively returned acknowledgement after a later sibling fault; driver readiness/unknown outcomes remain independently faulted. No callback replay or bypass of kernel owner/task/attempt checks.
- [x] Run focused GREEN for independent qualifier's first positive and new host private gate tests. Preserve exact selected source changes and freeze for full independent SPEC/QUALITY before widening execution.

## Task 5: Meaningful portable, API, TCP and physical qualification

All assertions use hand-selected native contract outcomes and actual snapshots. Introduce no old-test edits and no generated expected projection. New fixture wrapper forwards all production Store/ApprovalStore behavior and intercepts only named boundaries.

1. **Durable-before-observation:** Hold real cancellation before fenced Store call: Pending observer false and original snapshot Running. Delegate actual SQLite commit, then hold Store return: durable snapshot CancelRequested, observer still false. Release Store return: observer true and200cancel/409wait or exact single cancelled SSE done. Assert exact target, attempts/history/values/grants/journal and no new scheduling.
2. **Acknowledged cancellation:** Callback sees true, completes its owned work and returns new Cancelled. Actual persisted task Cancelled/resultNone/failureNone, execution Cancelled, attempts/history preserved. Public terminal remains selected once. Same-thread and capacity reuse succeed only after actual producer completion, without a new callback attempt.
3. **Two physical barriers:** Hold callback after observation and then hold acknowledgement completion. In both intervals, task is InFlight/CancelRequested, deletion/newrun/capacity remain refused and a second real Store open is AlreadyOwned. Positive gate return alone is not overall retirement.
4. **Real cancellation failure:** Use a new fixture-owned forwarding store to fail/retry/panic before cancellation persistence and a real SQLite busy-lock failure lane if bounded native busy timeout is feasible. Preserve full thread/approval/journal authority bytes and observer false. Host readiness fault or healthy typed refusal follows existing classification. Never turn a failed durable operation into notification.
5. **Healthy refusal:** Initial or renewed policy denial, UnsupportedOperation, no local target, stable approval park, prior owner and exhausted healthy RevisionConflict retries leave relevant observer false and no fabricated authority. Counter and whole-snapshot checks distinguish each domain.
6. **Stale and successor:** Hold cancel's initial/renewed policy against original control, let first actual callback finish, positively reacquire same-thread admission for a successor, release stale request;409no_run_in_flight, both observed signals clear. Separately accept first cancellation and retain its true clone through successor; new observer stays false until its own cancellation.
7. **Viewer versus cancellation abandonment:** Drop wait/stream/attach sockets while callback held; observer false, actual run remains owned and later real result commits. Drop an already admitted cancel socket at Store/policy hold, then release it: durable cancellation and observer notification still complete through owned worker.
8. **Late real outcomes:** Controlled callback intentionally ignores true and returns genuine success, known failure, uncertainty or panic. Preserve exact old result/failure precedence and unscheduled later batch, no reducer after cancel/no second terminal; uncertainty/panic fault readiness. Retained completion retries invoke callback once, and exhaustion preserves actual InFlight evidence.
9. **False acknowledgement:** Native controlled callback returns Cancelled without accepted original signal, including during Pending or using a separately fabricated signal. Registry/host faults statically; no task is falsely marked Cancelled or successful. Exact original task remains uncertain InFlight, physical owners retire only after actual callback and producer drainage.
10. **Post-acceptance late fault:** Two real native siblings; accept cancellation, then one faults while other observes sticky true and positively acknowledges. Its exact known outcome is committed or retained under existing bounded completion semantics; never cleared or fabricated. Host stays faulted and unrelated uncertain attempt remains durable. Kernel/driver behavior is not relaxed.
11. **Shutdown separation:** Begin ordinary shutdown without explicit accepted cancel while callback entered; observer remains false and shutdown Incomplete retains resources. Release actual callback and obtain positive normal Quiescent; no cancelled intent appears solely from shutdown. After accepted cancellation then shutdown, observer stays true without changing its cause.
12. **Observer clone resource proof:** Retain multiple clone observers externally beyond actual callback, completion, all accepted socket closure, normal server join, positive Quiescent and dropping all host/router/store/application aliases. Require both WeakStore and WeakApplication absence while clones remain. Then actual Store writer reopen succeeds, owner prior+1, readiness succeeds; retained observer remains readable. Include failure/abandoned-viewer versions where feasible. No abort/timeout or force cleanup is accepted as success.
13. **Old API guard:** New tests exercise old invoke and existing callbacks unchanged; run all existing native/cancel/shutdown/driver suites and frozen original AP fixture sources remain byte exact. No original client rerun is needed to establish the new Rust-only cooperative API.

Suggested focused commands (only after accepted design/implementation authorization):

```text
cargo test --locked --offline -p b4-runtime --test native_controlled
cargo test --locked --offline -p b4-http --test native_cooperative_cancellation
cargo test --locked --offline -p b4-http --test native_cooperative_cancellation_tcp
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo test --workspace --all-targets --locked --offline
```

Run real loopback with existing authorized local permission and save actual failing setup separately if the environment refuses bind. Clean only fixture-owned private directories/resources. No manual gate or external permission request for routine authorized local work.

## Task 6: Trace, review and scoped final verification

- [x] Independent complete source SPEC and different-author QUALITY cover public API compatibility, exact durable signal point, false acknowledgement, sticky late-fault interaction and resource-free clones. Test authors never self-approve their implementation.
- [x] Root uses only the four selected existing noncrate support files (`README.md`, `spec/runtime/compatibility.md`, `spec/runtime/requirements.yaml`, `scripts/check_contracts.py`) for narrow additive documentation/trace handling; new design/plan or checker-test files may be added. Preserve every old statement/status, all338 historical requirement declarations and old tests byte exact. Append a NEW separate `native_cooperative_cancellation_requirements` group with its own validation and printed count, analogous to the separate unchanged-client8 group; reject duplicate IDs across historical, client and new native groups. Do not increase/merge into the protected338 count or edit the old tests which assert338. Frozen evidence, schemas/examples, 56 broad not_run scenarios and milestones remain. Add no capability-descriptor flag/version merely to advertise an in-process API.
- [x] Fresh macOS Rust and Linux Rust qualification use `--workspace --all-targets --locked --offline`, so the frozen AP example30 tests remain included. Linux reuses the existing retained qualification environment with the current exact source; install no dependency or tool. Reuse required existing platform commands, derive distinct harness counts and preserve old ignored cases. Keep original-client execution/provenance strictly in its existing macOS scope; Linux Rust results cannot establish original-client parity. Release/native packaging behavior remains separate from this callback API. Do not invent an expected new total before tests exist.
- [x] Verify all nonselected source/manifest/lock/dependency/feature bytes and previous frozen artifacts exact. Reconstruct selected old five bodies against baseline where review needs proof of retained paths. Preserve genuine REDs, classify setup failures, and record fresh exact scoped results without reproducing source-sensitive fixture data publicly.
- [x] Freeze the actual bounded increment only after all required implementation, tests, nonauthor reviews and current evidence are complete. No broad RFC/M0/CP/core completion claim follows from this feature.

## Verified implementation progress

Portable API, HTTP wiring, the complete independent qualifier and checker hook
passed whole source review. Fresh macOS and Linux checks each pass
888 Rust entries across 88 targets, with three historical helper ignores;
Python passes 93 checks. All protected source, old tests, schema/examples,
manifests, lock and dependency/feature graph remain exact. The native scoped
verification receipt and 15 separate requirement rows are authored. Final
post-append traceability checks pass; whole support SPEC and separate QUALITY
reviews are accepted. The exact bounded increment is externally frozen with
its actual execution inputs, final sources and preserved evidence. Broad
RFC/M0/CP/core completion remains open.
