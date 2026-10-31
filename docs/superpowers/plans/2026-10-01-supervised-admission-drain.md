# Supervised admission drain implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development and test-driven-development, with
> independent specification review followed by quality review. Continuous-build
> authorization covers implementation; no git mutation, publication, new checkout
> or routine human approval is requested.

**Goal:** Let an embedding synchronously close HTTP admission and wait a bounded
time for registered framework work to finish, while preserving actual worker
ownership, admitted turns and durable evidence.

**Architecture:** One monotonic supervisor serializes owner registration and
operation leases against Serving -> Draining. Cloneable owner groups follow every
asynchronous request and detached blocking job; an explicit Shared carrier drops
framework captures before retiring the final group. The handle and owned wait
future retain only supervisor/fault state. Descriptor v2 declares this partial
mechanism without changing descriptor v1 or claiming full graceful shutdown.

**Tech Stack:** Existing Axum/Tokio, standard Arc/Mutex/atomics, MemoryStore and
SQLite; locked offline dependencies, kernel and SQLite schema3 unchanged.

Design: `spec/runtime/supervised-admission-drain-design.md`.
Frozen prior receipt: `conformance/expected/native-application-registry-verification.json`.
Private baseline/protected inventories: `/private/tmp/b4-supervised-drain-baseline.json`
(416 files) and `/private/tmp/b4-supervised-drain-protected.json` (178 artifacts).

## Task 1: Freeze design, ownership and plan (root)

- [x] Incorporate independent design corrections: synchronous wait creation,
  exact operation leases, grandfathered internal service retries, explicit
  owner/drop order, and honest external-reference/quiescence scope.
- [x] Freeze public names and descriptor v2 wire fields in the design.
- [x] Capture baseline and protected hashes before editing repository source.
- [x] Independently review this plan and ownership before implementation.
  restart_review approved; include unpolled wait construction outside Tokio and
  the exact MAX+1ns rejection boundary in core qualification.

No kernel automatic cancellation, callback signals, viewer/listener closure,
persisted shutdown reason/deadline, background recovery scan, providers, new
dependencies or SQL changes. All56 broad release scenarios and full T047 remain
pending. Public healthz remains200; readyz becomes503 immediately on Draining.

## Task 2: Store-free supervisor core (root)

Create `crates/b4-http/src/supervisor.rs`. Root temporarily owns only the narrow
`mod supervisor; pub use supervisor::{...};` export in HTTP lib.rs; hand lib.rs to
host author once core export is inserted. Host author owns HttpHost integration.

Frozen API:

```rust,ignore
pub enum ShutdownPhase { Serving, Draining }
pub struct ShutdownStatus {
    pub phase: ShutdownPhase, pub active_owners: usize, pub faulted: bool,
}
pub enum ShutdownOutcome {
    Quiescent { faulted: bool },
    Incomplete { active_owners: usize, faulted: bool },
}
pub enum ShutdownWaitError { InvalidTimeout }
pub const DEFAULT_SHUTDOWN_WAIT: Duration = Duration::from_secs(30);
pub const MAX_SHUTDOWN_WAIT: Duration = Duration::from_secs(300);
impl ShutdownHandle {
    pub fn begin_shutdown(&self) -> ShutdownStatus;
    pub fn status(&self) -> ShutdownStatus;
    pub fn wait_quiescent(&self, timeout: Duration)
        -> impl Future<Output = Result<ShutdownOutcome, ShutdownWaitError>> + Send + 'static;
}
```

Private construction takes Arc<AtomicBool>, never Shared/Store. Private owner
registration returns Option<OwnerToken>, serializing against the cut. OwnerToken
clones share one group/count; its last drop decrements once and notifies waiters.
An operation lease is separately acquired under the same short mutex and retains
the existing owner group. A lease refuses after cut even if its request owner
registered earlier. No policy, Store, claim, Pending or await while holding gate.

- [x] Write behavior tests against a minimal nonfunctional API shell: idempotent
  cut; post-cut registration/lease refusal; pre-cut lease still retains ownership;
  final clone retirement; healthy/faulted zero snapshots; valid wait creation
  closes admission before polling; invalid >300s leaves Serving unchanged; zero
  wait returns immediate incomplete; timeout leaves owners; dropped waits leave
  admission closed; concurrent waits wake on final owner; race linearization.
- [x] Record meaningful behavioral RED, then implement minimal monotonic state,
  Arc owner groups, short poison-safe gate and bounded Notify wait. Enable the
  notification before reading count to avoid a lost final-owner wakeup.
- [x] Validate Send+'static owned future and no Store references by construction.
  Run `cargo test -p b4-http --lib supervisor --locked --offline`; strict scoped
  lint once integration compile state permits. Independent spec review passed14
  focused tests; host author independently approved core quality. Final all-target
  lint remains Task6 verification.

## Task 3: Strict descriptor v2 (service author, independent of Task 2)

Create `crates/b4-contract/src/capabilities_v2.rs`,
`crates/b4-contract/tests/capabilities_v2.rs`,
`contracts/agent-protocol/schemas/capabilities-v2.json`, and authored v2 native/
scripted examples. Root owns contract/lib.rs export glue, manifest/OpenAPI/checker
and docs. Do not edit v1 DTO/schema/authored examples.

Exact v2 interface is the design's frozen flat root with new strict feature,
readiness and controls types. Common validation should project to frozen v1
semantics, then check v2 constants; preserve byte/label and assembly bounds.

- [x] Write rejection tests first for wrong version, false drain, true graceful
  shutdown, missing/null/extra fields, nullable omissions, wrong/duplicate/extra
  readiness checks, wrong wait constants, relaxed v1 limits/assembly/feature flags.
- [x] Record meaningful behavioral RED, implement DTO + strict schema, run v1
  unchanged tests and new v2 tests, obtain independent spec then quality review.
  V2 11+unchangedV1 9 pass; schema34positive/232refusal/2cross-version checks pass.
  Independent spec and root quality review approved the frozen artifacts.
- [x] After host ownership APIs freeze, own ONLY HTTP `src/capabilities.rs` and
  `tests/capabilities_api.rs`/`tests/capabilities_tcp.rs` + new v2-specific tests.
  Adapt read(shared:OwnedShared<S>,principal) to the counted carrier agreed with
  host author. Every detached readiness/policy job retains a carrier; job-local
  panic/infrastructure fault is recorded even if request disappears. Publish v2;
  check the admission phase again at metadata response cut without Store reentry.
- [x] Update existing endpoint assertions to v2 with truthful mechanism fields,
  preserving auth/privacy/status/response bounds; add closed/refusal and held
  capabilities policy/readiness quiescence tests. Historical receipts stay frozen.

## Task 4: Complete HTTP ownership and operation admission (host author)

Create focused private `crates/b4-http/src/lifecycle.rs` (or equivalent carrier)
and `tests/shutdown.rs`/helpers. Exclusively own all HTTP integration source:
host/lib/transport/worker/application/approval/attachment/cancellation/deletion/
recovery/control/claim/route/viewer as needed, EXCEPT root supervisor.rs and service
capabilities.rs. Coordinate narrow service signature integration before editing.

`OwnedShared<S>` is an explicit carrier with Shared dropped before OwnerToken;
clone carries both. It dereferences to Shared for existing helpers. Do not allow
raw Shared/store captures to survive final owner retirement. Register protected
endpoint before FIRST await; post-cut refusal is healthy503 host_draining before
auth/body/policy/Store work. Serving normal auth/app-scope behavior is unchanged.
Register readiness work; post-cut readyz returns status-only503 without probing.
The trusted embedding recovery path similarly registers before first await.

Frozen mutation leases: New after last host preflight/optional validator and before
implicit create/admit; Resume before ApprovalBridge.accept; Recover before adopt;
Create/Delete before mutation; Cancel BEFORE begin_cancel creates Pending. Leased
operations and durably admitted driver preparation/dispatch/completion continue
through service renewed policy/CAS retries. A losing preflight has no side effect.

- [x] Write meaningful RED tests for cut refusal with auth/policy/read counters0,
  health200/ready503 without probe, held body/auth/policy/validator pipeline counting,
  new/implicit/resume/create/delete/recover losing lease without mutation, and cancel
  losing lease without Pending/fault. Keep normal Serving tests intact.
- [x] Thread counted carrier through every blocking job and async gap; audit all
  spawn_blocking sites. Preserve existing run permits, claims, cancellation/control
  gates and actual outcome retention. Ensure producer/claims/permits drop before
  the final carrier; document field/drop order rather than closure capture guesses.
- [x] Record faults INSIDE abandoned owned jobs for Store/approval failures/panics;
  no private diagnostics. Add held job fault tests proving quiescent{faulted:true}.
- [x] Add real gates for pre-cut mutation lease continuing internal policy/CAS
  after cut, admitted Ready tasks continuing after sibling completion, accepted
  Once finishing, parked settlement quiescence, and prompt cancelled Done with
  wait stillIncomplete until callbacks/scoped joins/result commit finish.
- [x] Run new shutdown and existing HTTP tests. Independently review source spec
  then quality. Freeze production for independent transport qualification.

## Task 5: Independent physical qualification (review author)

After source freeze, create `crates/b4-http/tests/shutdown_tcp.rs`/helpers and, if
needed, separate SQLite ownership tests. Do not edit production or prior receipts.

- [x] Qualify actual TCP post-cut refusal, liveness/readiness, held callback and
  validator abandonment/timeout, no run-permit preflight jobs, multiple/cancelled
  waits and no early completion after cancellation's viewer Done.
- [x] Drop host/router/caller store references while a native callback or blocking
  Store/policy job survives; prove actual SQLite owner remains locked. Release
  gate, obtain Quiescent, and prove actual destructor/reopen with owner2. Handle
  remains live without pinning Store. Do not infer release from Weak or count only.
- [x] Qualify faulted physical quiescence with durable uncertain evidence and no
  fabricated completion/callback replay. If recovered jobs are exercised, use
  exact trusted known ID and existing recovery policy; no automatic scan.
- [x] Review actual barriers, static diagnostics and cleanup (gates release, child
  kill/reap when applicable), then obtain independent spec and quality review.

## Task 6: Wire evidence, docs and immutable receipt (root)

Root owns README/ADR/security/storage/compatibility/requirements, OpenAPI/manifest/
contract checker, native example verifier/assertions and all final receipts. Host
may update serve_native example only after prior production tests and agreement.

- [x] Update schema manifest/DTO export/OpenAPI v2 endpoint reference, retaining
  ten core AP operations plus the one native capabilities operation. Add v2 schema
  checker cases without rewriting authored v1 examples or old receipts.
- [x] Document API use, valid wait creation cut, lease semantics, external refs,
  incomplete waits and faulted quiescence. Keep graceful_shutdown:false and full
  RFC12.3/T047/core conformance open; no listener/signals/viewer closure claims.
- [x] Update provider-free native example verifier to v2 truthfully. Use a fresh
  compiled example for current evidence; preserve its existing privacy/cleanup tests.
- [x] Fresh workspace tests, format and strict all-target lint; retained Node/Python
  integrity checks; schema checks and examples. Record exact suite/case counts and
  new meaningful RED digests. Verify all178 protected prior hashes unchanged.
- [x] Save new immutable supervised-admission-drain verification receipt with
  explicit partial scope and independently audit hashes/counts/assertions.
- [x] Continue authorized core work until a real manual action/approval is required.

Reviewers may run scoped read-only tests concurrently with source authors, but
source ownership and dependent integration edits are sequential. Shared cargo
build locks are acceptable. No broad formatting during independent source writes;
authors format only their owned files. Frozen historical receipts never updated.

Current progress: all implementation and independent specification/quality reviews
are approved. Fresh verification passes 628 Rust tests (67 new), 50 retained reference
integrity tests and 13 Python tests, plus format and strict all-target lint. All nine
examples are rebuilt and freshly qualified. Two real cleanup ownership findings
were reproduced independently and repaired: replacement panic-payload destruction
and Shared-capture destructor panics stay counted until actual cleanup returns.
The immutable evidence receipt passed independent hash/count/scope audit; full graceful
shutdown and the broad conformance release remain open.
