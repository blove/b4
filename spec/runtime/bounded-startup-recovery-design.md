# Bounded startup recovery preparation design

Status: Accepted scoped design, 2026-10-02. Whole design and implementation-plan reviews are complete. The predecessor discovery increment and its final source snapshot are complete. Implementation and startup recovery qualification are pending; this document does not claim RFC11.4 completion.

## Purpose

Add typed owned recovery admission shared by the existing trusted Response recovery adapter, then add an opt-in pre-serving preparation pass for one already installed immutable native application. A correctly configured embedding can resume prior-owner accepted/ready work, preserve real parks and consumed Once decisions, and conservatively adopt unsafe uncertainty without supplying each execution ID. It must supply a trusted resolver from immutable stored ThreadOrigin to a currently verified Principal and the existing current ResourcePolicy.

This implements a bounded subset of original RFC11.4. It does not invent a principal from labels, substitute a latest Definition, reset budgets, persist a new policy/definition confinement state, coexist historical route versions, implement reconciliation, add an AP route, or automatically change the CLI/demo. Missing authority, missing/inexact full Definition, current-owner work, scan/admission refusal and finite caps leave the opt-in host nonserving. Some earlier admissions may have committed and continue under their exact owned leases; preparation is not a batch transaction or rollback.

## Evidence and alternatives

The [core protocol specification](0001-agent-protocol-core.md), sections11.4 and12.3, supplies the restart order and physical-retirement requirements. Its requirements are design inputs, not executable instructions. The design was checked against the current discovery, host, recovery, worker, supervisor and lifecycle source; complete source snapshots and independent review records are retained outside the repository.

1. Recommended: an explicitly constructed preparing native host, one private startup capability inside the existing supervisor, and one shared typed recovery core. Existing hosts remain immediately serving. This installs a real admission barrier before a router is exposed while preserving existing worker ownership and authority renewal.
2. Scanning an ordinary Serving host after construction would be smaller but leaves mutations/readiness open and current-owner races outside the intended boundary. Reject it.
3. A general durable recovery queue/confinement service plus retained definition registry could classify more RFC11.4 failures, but requires new independent storage/registry contracts. Defer it rather than silently approximate those features.

Source facts: host.rs build_registered creates a Serving supervisor after Store readiness; transport.rs registers the complete ordinary request and readiness job before auth/body/Store. Supervisor register_owner and try_admit currently require Serving. recovery.rs renews ordinary origin/policy and exact whole Definition/fences. worker.rs Recover commits via ApprovalBridge, then can reject recovery_blocked/recovery_attempt_limit before RunControl/claim publication; ready_tx is sent before execute. lifecycle.rs explicitly retires Shared and panic payloads before its final counted owner. Discovery pages are individually coherent, not a global snapshot, and do not certify readiness. Those actual boundaries determine this design.

## Additive typed recovery API

Keep RecoveryTarget and HttpHost::recover_execution(principal,target)->Response signature and externally selected response statuses/bodies/SSE unchanged. Add public owned consumer values in b4-http; no serde representation or HTTP endpoint is added:

```rust,ignore
pub struct RecoveryAdoption {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub turn_id: TurnId,
    pub execution_fence: ExecutionFence,
}
pub enum RecoveryDisposition {
    Runnable, Parked, Completed, Cancelled, Failed, Blocked, AttemptLimit,
}
pub struct RecoveryAdmission {
    pub adoption: RecoveryAdoption,
    pub disposition: RecoveryDisposition,
}
pub enum RecoveryRefusal {
    AdmissionClosed, Unsupported, ResourceUnavailable, CapacityExhausted,
    ThreadBusy, ResourceChanged, ExecutionChanged, RecoveryRequired,
    RecoveryNotNeeded, DefinitionUnavailable, RouteMismatch, ResumeRequired,
    InterruptSetMismatch, AlwaysUnsupported, GrantRequired, GrantExpired,
    GrantConsumed, GrantUnavailable, ProposalLimit, GrantInvalid, Unavailable,
}
pub struct RecoveryAdmissionError {
    // private static refusal and optional actual committed RecoveryAdoption
}
impl RecoveryAdmissionError {
    pub fn reason(&self) -> RecoveryRefusal;
    pub fn adoption(&self) -> Option<&RecoveryAdoption>;
}
impl<S: Store + 'static> HttpHost<S> {
    pub async fn admit_recovery(
        &self, principal: Principal, target: RecoveryTarget,
    ) -> Result<RecoveryAdmission, RecoveryAdmissionError>;
}
```

All values contain bounded typed identities/enums/fences only. They own no Snapshot, ThreadView, Route registration, callback, Principal, Store, RunControl, worker claim, permit, listener or opaque source error. Adoption is populated immediately from the actual StoredExecution returned by the durable bridge plus the exact thread incarnation validated by that same fenced recovery request; no invented revision or owner is computed. There is deliberately no new resource revision in this DTO. The existing request captures the resource fence and the transaction still checks/advances it. Adoption execution_fence uses the actual returned snapshot's revision/owner.

Use static Display and redacted/static Debug on RecoveryAdmissionError, Error::source=None, with no underlying SQL/serde/panic text. Its explicit adoption accessor returns sensitive embedding data; do not automatically log it. Some(adoption) means the bridge returned an actual committed StoredExecution and the framework captured that confirmed evidence. None means only NO CONFIRMED returned adoption evidence. It never proves no mutation, rollback or no successful commit: a custom Store can commit and then fail/panic before returning, and a receipt can be lost at an infrastructure boundary. Such ambiguity selects static Unavailable, startup Incomplete/Faulted and no retry/current-owner skip/invented fence. Neither the DTO nor counters are a transaction rollback certificate. Map the static refusal to this exact existing Response code/status mapping. No arbitrary string or underlying cause is retained:

| RecoveryRefusal | HTTP | Existing static details.code |
|---|---:|---|
| AdmissionClosed | 503 | host_draining |
| Unsupported | 501 | recovery_unsupported |
| ResourceUnavailable | 404 | resource_unavailable |
| CapacityExhausted | 503 | capacity_exhausted |
| ThreadBusy | 409 | thread_busy |
| ResourceChanged | 409 | resource_changed |
| ExecutionChanged | 409 | execution_changed |
| RecoveryRequired | 409 | recovery_required |
| RecoveryNotNeeded | 409 | recovery_not_needed |
| DefinitionUnavailable | 409 | recovery_definition_unavailable |
| RouteMismatch | 409 | route_mismatch |
| ResumeRequired | 409 | resume_required |
| InterruptSetMismatch | 409 | interrupt_set_mismatch |
| AlwaysUnsupported | 400 | always_unsupported |
| GrantRequired | 400 | grant_required |
| GrantExpired | 409 | grant_expired |
| GrantConsumed | 409 | grant_consumed |
| GrantUnavailable | 409 | grant_unavailable |
| ProposalLimit | 400 | approval_proposal_limit |
| GrantInvalid | 403 | grant_invalid |
| Unavailable | 503 | host_unready |

Some approval presentation failures are unusual for recovery but remain explicit to preserve the existing total static mapping. Infrastructure classification takes precedence and sets fault before selecting Unavailable; an unfamiliar noninfrastructure ApprovalError maps GrantInvalid exactly as today. Actual admitted Blocked/AttemptLimit remain dispositions with legacy409 recovery_blocked/recovery_attempt_limit, not fabricated unadopted errors. All old ordinary-Serving mapping remains exact. Preparing hosts use the same closed-admission503 host_draining envelope, without adding an AP response contract.

The common internal operation yields a typed admission plus an optional private ViewerStream for the Response presentation. The existing Response adapter requests its bounded SSE presentation, consumes that same typed result, and preserves409 recovery_blocked/attempt_limit, healthy expected refusals and existing successful park/terminal framing. The new public typed method requests detached presentation and returns only the DTO. Startup uses detached presentation too: it never reads an SSE body to infer adoption and never projects/discloses grant tokens merely to determine preparation. Existing Response-specific pending disclosure remains behind fresh ReadPending policy; detached park preparation does not read pending content solely for presentation. Reuse the real worker/driver/bridge; do not create a second effect loop.

A public typed call on an ordinary host retains current semantics: registration and preflight happen on polling, no job exists if the entire async call is unpolled, and an already launched owned producer survives caller abandonment. A preparing host rejects public known-ID admission because only the sealed coordinator has startup authority.

## Typed milestones and exact classifications

Recover preflight resolves the ordinary coherent view and authorizes actual immutable origin/application scope/current Recover policy before sensitive mismatch diagnostics. It requires exact registered whole Definition, thread generation/active execution/ID/execution revision/prior owner/resource revision. After every trusted resolver/policy wait and before mutation, renew those same exact captured values. The unchanged approval service transaction validates again and preserves grants/acceptances.

The producer reserves existing nonwaiting global capacity and per-thread claim before durable adoption. Record an adoption DTO immediately after recover returns. Any later infrastructure/identity/publication failure returns a static error WITH that adoption DTO; it must not falsely describe the work as unadopted or silently retry it as a prior-owner record. Host fault remains classified inside the counted job, even if the receiver vanished.

- Runnable: recovered Accepted/Running state with eligible work/budget; the exact RunControl and local claim have been installed/published before successful typed admission. execute begins after that admission signal and retains the same producer/lease/permit/claim until real completion.
- Parked: existing stable WaitingForApproval adopted unchanged except the existing recovery owner/turn/revision transition. Original tasks, grant/pending IDs/tokens/acceptances are preserved. Detached preparation does not create a callback or remint/disclose pending material; its carrier can retire after actual no-effect preparation. Response presentation keeps its existing authorized interrupts and null done.
- Completed/Cancelled/Failed: an actual terminal result produced by the existing recovery transition, never synthesized from transport success. Existing response terminal semantics are preserved.
- Blocked: an actual durable RecoveryBlocked outcome from the registered exact Definition, including existing unsafe/reconcilable uncertainty. No callback/retry occurs. This is safe preparation classification, not successful execution; the legacy Response remains static409 recovery_blocked. Already blocked work remains blocked. This is not a way to fabricate a missing Definition or revoked-policy block.
- AttemptLimit: actual adoption occurred, but remaining declared attempts cannot cover immediately eligible ready dispatches. The legacy Response remains409 recovery_attempt_limit. The startup pass ends incomplete/nonserving. Never reset max_attempts, manufacture Failed/Cancelled, or assume current ownership means published runnable work. Later-batch exhaustion remains the old admitted worker's expected rejection and is not retroactive terminal success.

Successful admission is not callback entry, terminal publication, result commit or physical retirement. It conveys a bounded durable preparation decision and, for Runnable, local producer publication. Policy can revoke after that milestone; the existing per-dispatch renewal still stops unauthorized work. Startup completion is not perpetual authorization or a guarantee every later callback succeeds.

## Opt-in host and authority API

```rust,ignore
pub trait StartupPrincipalResolver: Send + Sync {
    fn resolve(&self, origin: &ThreadOrigin)
        -> Result<Principal, StartupResolutionError>;
}
pub enum StartupResolutionError { Unavailable, Unauthorized }
pub struct StartupPreparationLimits {
    pub max_pages: usize,       // default32, valid1..=64
    pub max_recoveries: usize,  // default64, valid1..=256
}
pub struct PreparingHttpHost<S> { /* private HttpHost */ }
impl<S: ApprovalStore + 'static> HttpHost<S> {
    pub fn new_with_application_preparing(
        store: Arc<S>, auth: Arc<dyn Authenticator>,
        policy: Arc<dyn ResourcePolicy>, config: NativeHostConfig,
    ) -> Result<PreparingHttpHost<S>, ConfigError>;
}
impl<S: ApprovalStore + 'static> PreparingHttpHost<S> {
    pub fn router(&self) -> Router;
    pub fn shutdown_handle(&self) -> ShutdownHandle;
    pub fn preparation_status(&self) -> StartupPreparationStatus;
    pub fn begin_preparation(
        &self, resolver: Arc<dyn StartupPrincipalResolver>,
        limits: StartupPreparationLimits,
    ) -> Result<StartupPreparationHandle, StartupStartError>;
    pub fn serving_host(&self) -> Option<HttpHost<S>>;
}
```

Preparing construction requires approval.service_config==RequiredDurable and the Store's actual approval durability, retaining the existing native registration/prompt/Store readiness validation. Explicit volatile fixtures are not a claim of restart preparation; fail constructor statically rather than silently downgrade. This wrapper has no public store accessor or public conversion into an ordinary host until successful activation. router may be installed before preparation to expose liveness/nonready503, but all non-probe ordinary requests remain closed before authentication/body/Store. healthz retains the old liveness200. Existing constructors, NativeHostConfig fields, registry and limits remain unchanged.

The resolver is a trusted host integration port, not an authenticator shortcut. Stored labels are only an immutable authorization reference. The resolver must consult server-controlled current authority and return a verified current Principal; merely copying ThreadOrigin labels is not a library default. Fixtures use an explicit server-owned lookup that verifies the exact expected origin. Validate returned principal bounds/application scope and require ALL three labels equal the immutable origin, then renew current ResourcePolicy. Wrong/missing origin, unavailable/revoked resolver, origin-less legacy work or permissive policy with a foreign returned actor cannot adopt. Do not pass user metadata/input/grants to the resolver or retain credentials in a report.

## Private supervisor preparation barrier

Preserve public ShutdownPhase::{Serving,Draining}, ShutdownStatus and old ShutdownHandle method signatures/exhaustive matches. Add a private preparation substate under the SAME supervisor Gate mutex: None for old hosts; NotStarted/Running/Complete/Incomplete for opt-in hosts, plus a private nonforgeable startup capability tied to that exact supervisor/attempt. Public shutdown phase still reports whether the monotonic shutdown cut occurred; it does not by itself assert startup completion. Preparation status is the separate explicit observation. Document this for preparing hosts without changing old Serving-host semantics.

Ordinary register_owner, try_admit and is_serving require phase==Serving AND preparation is None or Complete. Startup coordinator registration and startup mutation lease require phase==Serving AND preparation==Running AND the exact private capability. Private role is created only by the accepted begin operation; no public token/getter/cast bypass exists. A startup owner cannot authorize an ordinary operation. Counts use checked arithmetic, short mutex scopes and no policy/Store/RunControl locks or awaits under that mutex.

begin_preparation validates finite limits and current Tokio Handle availability without starting work. At one mutex linearization point it admits exactly one attempt, installs its counted coordinator owner before returning, and marks Running. It then spawns the owned async coordinator on the embedding's existing executor. Handle::try_current failure or invalid limits is a static start refusal without activation; duplicate start is AlreadyStarted. No second runtime, OS thread executor or process exit is introduced.

The coordinator uses a NEW private counted async wrapper in the selected lifecycle.rs scope, not a plain tokio::spawn of an inner future that merely captures OwnedShared. Existing OwnedShared::spawn catches blocking closure panics only; Tokio can otherwise retain a Store-bearing async panic payload in JoinHandle after an inner OwnedShared retired. This wrapper closes that specific ownership gap using only std::future::Future, Pin<Box<F>>, catch_unwind/AssertUnwindSafe and existing Tokio spawn/handles. No new package/feature/unsafe is needed.

Its concrete fields are Option<Pin<Box<private coordinator Future>>>, Option<OwnedShared<S>>, an Option<OwnerToken> anchor cloned from the SAME coordinator owner group, and resource-free preparation/fault/attempt state. The anchor is created before spawning, adds no separate group count, and stays outside the inner future. The wrapper manually polls the pinned inner future inside catch_unwind; the private Future output is a value-only completion candidate (prepared progress or static incomplete reason), never a Store-bearing result/JoinError. The inner future does not activate serving or publish Complete.

On each normal Ready, first destroy the completed inner Future and ALL its captures inside catch_unwind while carrier AND anchor remain counted. Dispose any arbitrary panic payload and replacement panic payload through the existing bounded-state dispose_panic loop while those owners survive; the cleanup itself is allowed to remain held indefinitely. Then drop the OwnedShared carrier (which explicitly drops Shared before its own owner and disposes its own destructor panics); the separate same-group anchor still prevents zero owners. Only after all these captures/payloads retired and the fault state remains clear may the resource-free finishing state accept the pure candidate and perform the existing one-mutex activation. If cleanup faulted, publish Incomplete/Faulted instead. Finally drop the anchor before returning the pure result. This ordering prevents normal future/destructor cleanup from publishing Complete and then faulting or retaining an uncounted Store capture.

On poll panic, set fault and Incomplete/Faulted under short state locks BEFORE payload disposal, thereby closing startup admission/activation. Destroy the caught arbitrary payload and then the inner future/captures while carrier+anchor survive, including replacement payloads produced by destructors. Drop carrier Shared and anchor last; Tokio receives only a resource-free static result. Drop of an unfinished wrapper (runtime cancellation, submission cleanup or a private test abort) performs the same synchronous fault/incomplete and counted disposal path. It marks the attempt final without activating, destroys the inner future under catch_unwind, and disposes all arbitrary panic payloads while the anchor survives. A wrapper already finished has empty resource fields and does not manufacture a cancellation fault. No caller-visible observation handle exposes AbortHandle or can abort the coordinator.

Every awaited blocking JoinHandle is consumed inside the inner coordinator while its counted carrier survives: if JoinError::is_panic(), take and dispose its payload through the same owned panic disposer; cancellation otherwise selects static Unavailable/Incomplete/Faulted and drops the JoinError locally. Never format, return, cache, log or leave an arbitrary JoinError/panic payload in the public preparation handle. Existing blocking jobs already reduce panics to a static payload; the coordinator still follows this total disposal contract. The new async wrapper is the ownership carrier for inner Future cancellation/panic, so no uncounted watcher is needed. No arbitrary application code is evaluated outside the caught poll/disposal scopes. Abort-process/OOM/double-panic abort behavior is not made survivable by this design; actual process termination remains distinct.

Resolver and blocking Store jobs clone the exact counted startup role; worker launches obtain private startup owners and operation leases using the capability. Holding a trusted future/resolver/payload destructor remains counted even if all observer waiters/host/router callers detach. Dropping an observation handle, aborting a waiter or timing out a wait cannot release a surviving job's claim/Store or abort callbacks. Only actually registered and authorized jobs may continue. Externally held host/router/Store refs remain the embedding's lifetime. Targeted tests must actually poll a future that panics with a Store-bearing opaque payload, hold its destructor, and show Faulted/Incomplete plus nonzero owners/Weak alive/real writer locked before explicit return. Separate cancellation and normal-Ready Future-capture destructor holds prove that zero-owner quiescence and activation cannot win before cleanup. A replacement panic-payload hold exercises the disposal loop. All are future tests, not evidence already executed.

Activation is a single compare-and-change under the same Gate mutex as begin_shutdown/owner registration/leases: require Running/exact attempt, no host fault, and completed finite scan/classifications, then set Complete/open ordinary admission. Draining, prior incomplete result, poisoned gate or source fault prevents activation. begin_shutdown wins monotonically in either NotStarted or Running; it closes ordinary AND startup leases immediately and prevents later activation. Existing leases that won the cut may finish their exact operation/driver; shutdown does not undo accepted intent or become user cancellation. A held resolver/preflight returning after a shutdown cut cannot obtain a new startup mutation lease. Finish publishes a value-only final outcome; it must not reopen Draining.

## Finite pass and consistency covenant

The embedding must isolate this owned source during preparation: no direct Store/ApprovalService/Runtime mutations by aliases, other coordinator, external raw writer or provider-owned executor. The library's private gate closes its ordinary host paths but cannot prove this covenant for arbitrary external Store references. Supply it explicitly as a documented trusted embedding precondition; do not add a fake confinement primitive/lock or advertise Store-wide isolation.

Under that covenant, the only permitted changes during the pass are coordinator-owned recovery and already accepted recovered drivers updating EXISTING execution/thread records, never inserting/deleting/reordering execution IDs or changing another candidate's actor/Definition. Existing one-active-execution-per-thread plus local claims/fenced recovery supplies exclusion. This is the concrete reason the discovery cursor's per-page consistency is sufficient for this restricted pass. Without that covenant, exhaustion cannot certify preparation. A stale candidate fails incomplete rather than skipping/restarting a scan; there is no repair/rescan that papers over a violated assumption.

Algorithm, sequential admission and bounded working set:

1. Use the registered coordinator to read Store owner/readiness and discover_unfinished(None); retain at most one32-row descriptor page and one resource-free cursor. Refusal/zero or changed owner is incomplete; do not use a separate raw SQL scan.
2. Count each page and its scanned_rows with checked arithmetic. max_pages includes terminal-only/empty advancing pages. Only absence of next_cursor within the permitted page count is exhaustion; a continuation at the cap is incomplete even if a later page might be terminal-only.
3. For each selected record require Prior. Current is incomplete, not proof of preparation or safe-to-skip. Before mutation, enforce max_recoveries (all intended adoption candidates count, including parks/blocked). No unbounded collected target queue/history is built.
4. Read one fresh ordinary coherent ThreadView in a registered blocking job. Require its active ID, thread generation, execution fence, resource revision, route/full definition revision and status/uncertainty match the observed descriptor; derive RecoveryTarget from THAT ordinary view. No target/principal is assembled only from a discovery row. The descriptor is observation, not authority.
5. Resolve its real nonempty immutable origin through trusted resolver, validate exact Principal and renew existing preflight. Preserve captured view across resolver wait so resource replacement/stale generation cannot be silently selected. Compare whole registered Definition; never use requested/latest revision as fallback.
6. Launch through the same typed detached recovery core and private startup lease. Existing capacity acquisition is try_acquire_owned, not an unbounded admission queue. Sequentially wait for the typed adoption/publication milestone, then drop its value-only result and proceed. Capacity or same-thread refusal ends incomplete; do not wait for callbacks to free capacity, reset budgets or abort them to make room.
7. Count accepted dispositions. Blocked/parked/actual terminal classification is sufficient; Runnable publication is sufficient while its callback may remain held. AttemptLimit or any pre/post-adoption refusal is incomplete. No rollback of earlier accepted work is promised. Existing accepted workers keep per-dispatch authority checks.
8. At exhaustion and before activation verify same owned epoch and no fault, then use the one-mutex activation transition. Published Complete means this bounded isolated candidate set was prepared. It is neither global Store validation nor discovery/recovery of work outside the covenant/budget.

## Progress and waits

StartupPreparationHandle is Clone and owns only supervisor/fault/progress/Notify state, with no Store/application/resolver/closure. Status and outcomes contain static enums plus checked cumulative counts (pages, scanned_rows, candidates, runnable, parked, blocked, terminal), not IDs, origins, prompts, tokens, full snapshots or underlying errors. No Serialize/automatic logging/operator endpoint is needed.

Use these exact additive resource-free types, with bounded checked counters and no serde/custom payload fields:

```rust,ignore
pub struct StartupProgress {
    pub pages: usize,
    pub scanned_rows: usize,
    pub candidates: usize,
    pub admitted: usize,
    pub runnable: usize,
    pub parked: usize,
    pub blocked: usize,
    pub terminal: usize,
}
pub enum StartupPreparationPhase { NotStarted, Preparing, Complete, Incomplete }
pub enum StartupIncompleteReason {
    Shutdown, LimitExceeded, DiscoveryRefused, CurrentOwnerExecution,
    ResolutionRefused, RecoveryRefused, AttemptLimit, Faulted,
}
pub struct StartupPreparationStatus {
    pub phase: StartupPreparationPhase,
    pub progress: StartupProgress,
    pub incomplete_reason: Option<StartupIncompleteReason>,
}
pub enum StartupPreparationOutcome {
    Complete { progress: StartupProgress },
    Incomplete { progress: StartupProgress, reason: StartupIncompleteReason },
}
pub enum StartupWaitOutcome {
    Finished(StartupPreparationOutcome), Pending(StartupPreparationStatus),
}
pub enum StartupStartError {
    InvalidLimits, ExecutorUnavailable, AlreadyStarted, Shutdown, Faulted,
}
pub enum StartupWaitError { InvalidTimeout }
impl StartupPreparationHandle {
    pub fn status(&self) -> StartupPreparationStatus;
    pub fn wait(&self, timeout: Duration)
        -> impl Future<Output=Result<StartupWaitOutcome, StartupWaitError>>
            + Send + 'static;
}
```

Counters never exceed configured accepted work: pages<=64, scanned_rows<=2048 and candidates/admitted<=256. candidates counts a prior-owner candidate accepted for processing BEFORE its authority/admission work; exceeding max_recoveries ends incomplete without incrementing or beginning an extra candidate. admitted counts CONFIRMED returned adoption observations only, including an error carrying Some(adoption); disposition counters increment only after typed successful classification. An ambiguous commit with None is unconfirmed and therefore not counted as admitted, but that zero/partial count cannot certify rollback or absence of durable mutations. terminal combines actual Completed/Cancelled/Failed. Thus incomplete counts do not pretend rejected/unpublished work was successfully prepared. Status/Report/error enums use static Debug/Display with no identity or source cause. No rejected identity is included in the aggregate. NotStarted/Preparing have no incomplete_reason; Complete has none; Incomplete has exactly one reason. A final Complete/Incomplete outcome is immutable once set. Physical registered ownership is independently read through ShutdownHandle.

Expose handle.wait(timeout)->Send+'static owned Future with timeout0..=MAX_SHUTDOWN_WAIT (300s), default30s. Invalid wait changes nothing. It returns Finished(final outcome) or Pending(current progress); timeout/waiter drop does not mark completed work, change admission, abort jobs, trigger anonymous recovery or certify retirement. Preparing remains closed until actual completion or explicit shutdown. No hard duration bound is promised for trusted resolver/Store/policy/callback code: finite page/candidate/call counts and observer waits are bounded, while blocked callbacks/destructors can retain resources indefinitely. No automatic watchdog/time-based mutation is in this slice.

## Failure and resource proof

Expected authority/definition/cap/stale/refusal ends incomplete but does not fabricate a durable block. Infrastructure/commit/decode/panic failure also marks the host fault using the existing owned boundaries. If adoption was confirmed by a returned StoredExecution before later failure, the typed error reports that captured adoption and startup remains incomplete. If a failed/panicking transaction has no confirmed receipt, error.adoption may be None even if the source committed; select Unavailable/Incomplete/Faulted without claiming rollback. Neither case permits current-owner retry/skip or an invented fence. Failure may leave safe earlier callbacks active under their exact leases. Incomplete is not physical shutdown; call begin_shutdown and wait_quiescent separately, then stop/join listener and drop external references.

A completed startup handle does not keep Store ownership; it remains alive during the real writer-reopen test. Conversely, an admitted held callback must keep Store, claim, permit and application captures alive even after all startup callers/viewers disappear. Positive qualification requires explicit nonexpired finite callback release and actual normal completion, normal server join where present, Quiescent with zero owners, Weak<Store>/Weak<NativeApplication> absence after external aliases are dropped, and actual SqliteStore writer successor owner+1. Timed waits, callback markers, zero active count alone, directory deletion and forced cleanup are not that proof.

## Preservation and planned scope

After discovery final freeze, capture its complete baseline/protected inventory. Select ONLY8 existing HTTP production paths: src/lib.rs,host.rs,application.rs,recovery.rs,worker.rs,supervisor.rs,lifecycle.rs,transport.rs. New modules hold typed admission/static mapping and startup DTO/coordinator/helper tests. Narrowly preserve old constructor/mutation/driver/authority behavior and all historical test bodies; reconstruct complete old bytes by reversing only reviewed insertions/refactors. Runtime discovery/helpers, kernel, approval service/transactions, SQLite DDL/storage, manifests/lock/dependency graph, CLI/demo/launcher and frozen original-client corpus remain exact.

Root may separately select README.md, spec/runtime/compatibility.md, spec/runtime/requirements.yaml, scripts/check_contracts.py for additive support/trace hooks. Existing requirement groups/counts/statements/statuses remain exact; add a separate startup_recovery_preparation_requirements group with cross-group duplicate checks and separately printed count. No new full-core/capability assertion follows from an opt-in embedding API. Use a new static safe verification receipt only after real final scoped/wide qualification; no raw adoption/authority data or authority-derived digests. Historical receipts and original-client claims remain immutable. M0, CP010, broad process-kill/crash matrix, M2/M4/M5 partial status and full RFC11.4 completion remain open.

## Required independent qualification

Stage compiling stubs first; capture TWO actual positive behavior REDs before implementation: real prior-owner SQLite typed admission is Unsupported instead of actual adoption, and a real preparing-host TCP ready request is incorrectly200 because the constructor still delegates the old Serving supervisor. Demonstrate successful fixture creation, all owner handles closed/reopen owner+1, exact Definition/principal setup and normal cleanup before counting RED. No compilation or fixture failure counts.

Then qualify complete typed dispositions/pre-vs-post adoption/static errors; unchanged Response409/SSE; all pre-serving operations blocked before auth/body/Store with health200; held resolver and source/CAS gates; duplicate begin, waiter drop/unpolled wait/timeout and shutdown activation race; terminal-prefix pages/caps/current-owner refusal; exact resolver/whole Definition/stale generation/fences/epoch/policy; real parks and accepted Once TTL; actual unsafe no-callback block; capacity with an earlier live owned callback; failure-after-confirmed-adoption, custom Store commit-then-error/panic ambiguity and independently observed actual durable trigger rollback; async poll panic/cancellation/normal-capture destructor holds with opaque replacement payloads; startup handle resource freedom and actual final writer reopening. Fixtures use explicit absolute bounded gates, actual entry/return facts, no cleanup release counted positive, complete raw private ThreadView/ApprovalView equality at each authority boundary and real loopback requests. These are planned tests, not executed evidence.
