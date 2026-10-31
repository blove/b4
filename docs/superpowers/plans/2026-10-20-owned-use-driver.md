# Owned NewUse Driver adapter implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development task-by-task, with independent source/result reviews. Continuous user authorization already selects this workflow; do not ask another execution-choice or routine approval question. Do not begin this plan before Task 6 and narrow outcome adoption are accepted.

**Goal:** Let the existing real execution driver settle one genuinely permission-authorized NewUse callback through its retained receipt, while preserving that receipt/Work on every unsettled failure.

**Architecture:** An explicit embedded entry builds a private DriverHost and private Store adapter over external Store/authority/catalog references. One bounded mutex slot owns the real callback work, then the real Receipt/OutcomeWork; ordinary driver outcomes are observations only. The adapter accepts only the exact completion candidate reconstructed from that Work and a private trusted-facts ticket, and uses the existing typed outcome writer.

**Tech Stack:** Rust 1.93.1; existing runtime/kernel/SQLite and vendor rusqlite; no new dependencies, schema, threads beyond the existing scoped driver, or async API.

**Design:** [accepted narrow design](../specs/2026-10-20-owned-use-driver-design.md), accepted with the refinement that a consuming validated Commit moves its thread ID and snapshot into StoredExecution infallibly. `spec/runtime/persistent-always-design.md:91-101` remains the broader boundary; this plan implements only the named narrow embedded slice.

---

**Current status:** The narrow embedded Driver bridge is implemented, reviewed and adopted. Twelve genuine Driver integration cases, seven validation/account controls and eight existing outcome cases pass, together with formatting, strict lint, compilation and the normal workspace regression. The [qualification record](../../../conformance/permissions/use-driver/qualification.json) binds the tested code. Dedicated poisoned-slot and one-consume behavioral controls remain unqualified; actual callback-once, native/hook/backend panic retirement and concrete compile-time Send/Sync checks passed. HTTP/generated UUID support and the broader permission profile remain open. Detailed local evidence is retained under `.b4-work/use-driver`.

The original task descriptions below are retained as design history; the current scope and qualification record identify the executed checks and remaining limits.

## Prerequisite and protected boundaries

- Outcome Task 6 controls, regressions and exact narrow adoption must be complete first. Base this work on those adopted bodies, not an unadopted earlier workspace. Keep current reconstruction/evidence until its adoption process says otherwise.
- Use an isolated ordinary source copy and the existing reviewed local execution controller conventions; no Git writes are requested in this repository with unborn/read-only Git state. Save source heads, short actual before/after records and native receipts under the driver work directory with mode 0600. No exhaustive Rust-operation fee graph is a prerequisite or product claim.
- Preserve generic `DriverHost`, `drive_execution`, coordinator/completion/failure bodies, `Store` trait, SQLite raw commit guard, existing producer/renewal/callback/outcome limits, and existing fixture bodies. Exact exceptions are: new modules/exports; one append-only private Claim scheduling projection; a private outcome entry refactor sharing the existing commit protocol; a private consuming validated Commit projection. The new adapter uses the adopted typed writer unchanged.
- Initial input is actual Running/approved Ready NewUse rev2, single task/single batch, attempts_used0/empty attempt history, current owner and same original turn; real Dispatch becomes rev3. One callback, max_parallel_tasks1, completion_attempts1. No automatic retry, new turn, owner adoption, cancellation, Start, permission authorization/Park, mixed tasks, human/legacy, recovery or HTTP integration. Failed callbacks returning an actual EffectOutcome::Failed settle normally: the existing kernel FailAttempt marks the task Failed and settles; it does not retry (kernel/transition.rs:171-222).
- Unsupported/mutated resource cases refuse and retain truthful evidence. Do not weaken exact outcome resource matching or synthesize a Cancelled Receipt. Do not advertise durable_permissions or broad conformance completion.

## File map

All paths below are repository-relative in the isolated implementation copy.

Create:
- `crates/b4-runtime/src/permissions/driver_adapter.rs`: public explicit entry, hooks/bindings, opaque failure, private pair and single-slot lifecycle.
- `crates/b4-runtime/src/permissions/driver_adapter_account.rs`: independent bounded functional overhead account and copied observations; bounded projection/candidate traversals.
- `crates/b4-runtime/src/permissions/driver_adapter_tests.rs`: internal pure slot/candidate/facts/retirement controls using genuine fixtures only where authority is needed.
- `crates/b4-sqlite/tests/persistent_permissions/use_driver.rs`: independent real SQLite driver fixtures.

Append/refactor only:
- `crates/b4-runtime/src/permissions/mod.rs`: module linkage and selected public adapter exports.
- `crates/b4-runtime/src/permissions/runtime_mutation.rs`: private `PermissionDispatchClaim::driver_prepared` observation, budgeted by adapter account.
- `crates/b4-runtime/src/permissions/runtime_use_outcome.rs`: private candidate admission attached to the existing Work attempt, shared existing commit ordering, and private consuming Commit projection.
- `crates/b4-sqlite/tests/persistent_permissions.rs`: sibling fixture linkage.

After qualification/adoption only: create `conformance/permissions/use-driver/README.md` and update this plan with actual completion status; update an existing status reference only if needed and truthful. Do not edit broader capability declarations.

## Concrete API and ownership choices

Use one external lifetime initially: producer and current catalog may be distinct references, but both outlive the synchronous call and returned failure. The entry cannot own a catalog that its own slot borrows.

```rust
pub trait PermissionUseDriverHooks: Sync {
    // Trusted embedding clock boundary, equivalent to DriverHost::facts.
    fn facts(&self, current: &StoredExecution) -> TransitionFacts;
    // Trusted embedding attempt identity, equivalent to DriverHost::attempt_id.
    fn attempt_id(&self) -> b4_contract::AttemptId;
}

pub struct PermissionUseDriverBindings<'env, S, A, H>
where S: PermissionStore + ?Sized,
      A: ApprovalAuthority + ?Sized,
      H: PermissionUseDriverHooks + ?Sized
{
    pub store: &'env S,
    pub authority: &'env A,
    pub producer_catalog: &'env PermissionCatalog,
    pub current_catalog: &'env PermissionCatalog,
    pub context: &'env ApprovalContext,
    pub hooks: &'env H,
    pub capacity: &'env TaskCapacity,
}

pub fn drive_permission_use_execution<'env, S, A, H>(
    bindings: PermissionUseDriverBindings<'env, S, A, H>,
    initial: StoredExecution,
) -> Result<StoredExecution, PermissionUseDriverFailure<'env, S, A>>
where S: PermissionStore + ?Sized,
      A: ApprovalAuthority + ?Sized,
      H: PermissionUseDriverHooks + ?Sized;
```

The bindings intentionally expose only already-trusted external references, not an adapter/Runtime. No configurable scheduler limits: the entry fixes `{ max_parallel_tasks:1, completion_attempts:1, max_outcome_bytes:65_536 }`. Public failure is opaque/non-Clone/non-revealing Debug. It owns ordinary `DriverFailure<PermissionError>`, adapter account and actual retained Receipt/Work, with bounded observations `callback_status`, `settled_outcome`, `usage`, and `retry_settlement(&mut self, facts: &TransitionFacts) -> Result<StoredExecution, PermissionError>`. Retry is available only when a genuine Settled Work remains; it uses the SAME Work/account, never driver restart or callback entry. Facts on explicit retry come from the same trusted embedding boundary as public Work.commit. No reset, raw extraction, inner adapter getter, or auto-settlement on Drop.

Private pair: a stack-owned `Mutex<DriverSlot<'env,S,A>>`; private Host and Store adapter each borrow it during drive_execution. Both copy the original external catalog/Store/authority references. Store adapter's retained Work borrows underlying S, never the adapter. Runtime owns the adapter by value. After drive_execution and scoped joins, retire Host and Runtime before consuming the mutex into the public failure. Scope these borrows explicitly; do not make a host own a mutex containing Work borrowing that host.

Private phases: Empty -> Armed(actual callback work, actual PreparedEffect) -> Entering(actual prepared observation only) -> Settled(actual Work, exact completion-facts ticket) or Refused(actual Receipt) -> Durable. Bound to one run/task/attempt/input. A fixed enum/Option slot has capacity1, no map, Vec of attempts, or detached task. Keep the entered local owner under a small guard until publication; native panic is already caught in the existing consuming callback. Publish the actual Receipt/Work before cloning its observed outcome for DriverHost.invoke. If later cloning/projection panics, the slot still owns the result. Uncertainty and nonsettled native errors return generic host callback uncertainty while retaining truthful Receipt. Do not reinterpret a retained rejected outcome as Settled.

Never hold the slot mutex while calling hooks, authority, constraints, native callback or SQLite/Store ports. A temporary guard owns borrowed/moved callback work during invoke. For settlement, take Work into a local return guard; restore it before returning any refusal. A per-run coordination lock/state can exclude another commit while Work is out, but external work runs without holding the state mutex. Mutex poisoning is a static fault; after real driver drainage recover the actual inner owner for retirement (using PoisonError::into_inner), never discard it to hide poisoning. Do not retry or invoke during poison recovery. Include held-owner panic/drop tests.

Functional overhead: distinct `DriverAdapterAccount` in its new file, no reuse of old budgets or increase of their limits. Start with fixed ceilings **16 MiB charged copied/visited bytes, 131,072 structural nodes, 128 phase/entry operations, one slot**. Use checked additions, inclusive ceilings and sticky exhaustion; no public constructor/reset. Preflight bounds each newly traversed/copyable JSON value at 65,536 encoded bytes, depth64, nodes16,384; bound snapshots/definitions/thread/context/candidate aggregates under this account before duplication. Reuse existing immutable limits and bounded validators where their actual interfaces allow it; do not pass an adapter account to old branded sessions. Move opaque Claim/Work/Receipt without decoding their private authority. Reserve actual scheduling/outcome observation clones and candidate/facts comparisons before doing them. Charge again on explicit retry, preserving both adapter and outcome cumulative accounts. These are functional limits on the new adapter's work, not whole-process memory bounds or an exhaustive standard-library audit. If the genuine fixture cannot fit, report measured traversal/clone demand and choose a reviewed finite ceiling before native execution; do not silently widen an older account.

## Task 1: Genuine real-driver completion RED

**Files:** new account/adapter modules and test linkage above; append private Claim projection only. No outcome writer/refactor yet.

- Record adopted outcome head and protected files; create isolated copy. Read actual current trait/method signatures, source profile and prior working SQLite setup. Reconcile these API sketches with that head before introducing code.
- Write independent `use_driver::permission_use_driver_settled_result_requires_receipt_commit` success test before the scaffold. Copy genuine healthy Always owner1 -> next-owner2 new-use setup from the adopted outcome fixture into this new sibling, stopping at actual authorized approved Ready rev2; do not modify/reuse the old fixture body through behavior switches. Keep exact legal catalog/context/constraint/input bounds. Retain fixture's existing Definition.max_attempts if desired; entry itself allows only first attempt and no retry. Kernel validates definition as usual.
- Write final expected-Succeeded assertion plus all setup/capture/cleanup checks first. Test hooks supply real deterministic facts (the existing fixture chronology ending at completion109) and a fresh actual AttemptId. Expected observed callback Bool(true)/empty writes, calls1/matches1. Use genuine producer/renewal in the host, not a fixture-manufactured permit.
- Implement independent bounded account and public entry/failure scaffold. Preflight the actual initial profile and bind exact thread/execution/owner/turn/task/spec/input to the single run. No resource/policy authority may be inferred from the initial Snapshot; actual producer and renewal perform existing authorization and full checks.
- Append private Claim scheduling projection built from `claim.resource.execution` and `claim.invocation`, charged before clone. It returns only PreparedEffect and neither extracts nor creates eligibility. Host prepares using actual ordinary thread fences, original producer catalog/context, trusted attempt/facts, fresh existing dispatch budget; then creates real renewal account and consumes the claim using existing renewal against current catalog. Capture actual PreparedEffect before that consumption. Renewal refusal returns `PreparationError { cause, claimed: Some(Box::new(actual_prepared)) }`; producer preclaim error has no claimed projection. Do not claim a producer error cannot have committed: preserve the generic driver's existing preparing reconciliation on panic/error.
- Host stores actual renewed callback work as Armed and returns actual prepared scheduling evidence. Invoke requires exact invocation equality, consumes Armed once outside mutex, invokes existing consuming callback, stores entire actual Work/Receipt, and returns an observed clone only for Settled Succeeded/Failed. New slot/account/observation work stays separately bounded.
- Private Store adapter delegates coherent reads (`thread`, `thread_view`, `execution`, `journal`, readiness, owner generation) needed by real driver identity and completion. Its capabilities remain conservative (no fenced cancellation/deletion claim). Mutating creation/admit/admit_fenced and unsupported operations refuse; **all Store.commit initially return UnsupportedOperation**. No underlying raw commit delegation. The host's unsupported park returns PermissionError::UnsupportedProfile; the profile preflight ensures success never selects it.
- The test enters actual drive_execution. Add an observation wrapper around underlying typed dispatch/renewal/outcome ports, and a completion-read capture point in the test Store wrapper after callback/when the driver reloads for completion. Capture typed and raw nine groups at this actual postcallback/precompletion point; do not run fixture capture logic inside native callback. After return, assert driver completion refusal and identical storage from that capture, real Dispatch rev3, callback1, Settled failure Work/outcome and all external anchors alive. Typed outcome port entries0 on RED; raw Store.commit entries0; private adapter completion entries1.
- Retire failure and all adapter/catalog/application/authority/Store aliases normally before owner2 sole-owner close; owner3 reopens and verifies exact postclaim state, closes; remove directory. Print one static evidence line. Only then assert expected successful driver result. Expected RED is **completion UnsupportedOperation after one genuine callback and normal retirement**, not missing API/compile failure, callback0, unsupported initial profile, renewal error or cleanup failure.
- Independently review complete scaffold/current fixture and protected diff; run formatter/no-run/lint as below, then selected test once through the local controller. Save actual RED and fresh source identity. Accept this RED before Task 2.

## Task 2: Pure candidate admission and trusted completion ticket

**Files:** new adapter tests; adapter/account; private refactor runtime_use_outcome only.

- Write pure `candidate_identity_fence_and_full_plan_are_exact` around a data-only `candidate_matches(expected_id, expected_fence, expected_plan, candidate...)` comparator. Change each ID/fence/expected_revision/snapshot field/dispatchable independently; every changed comparison refuses. Pure expected plans confer no permission authority and cannot create Work or Commit. This control supplements genuine integration, not substitutes for it.
- Write `completion_ticket_binds_exact_facts_and_phase`: ticket cannot be installed before Settled, for a different run/attempt or twice; stale timestamp/nonmatching snapshot refuses. Add `completion_ticket_install_failure_latches_fault_without_changing_facts`: force the independent adapter account to refuse installation, assert the exact actual trusted facts are returned, sticky latch remains set, actual Work survives, and completion refuses before the typed writer. Cover a facts call outside Settled and a hook panic followed by normal owned retirement. Ticket constructor stays private. Host.facts calls trusted public hooks **outside mutex**, then only on Settled attempts to reserve/validate and install exact facts bound to this run/attempt and the current complete snapshot/fence it was given. Calls outside Settled never mint a completion ticket. Because the unchanged DriverHost::facts returns TransitionFacts, not Result, a failed reserve/validation/installation sets a **private sticky fault/error latch**, preserves actual Work and returns the actual trusted hook facts unchanged. Do not panic, unwrap, fabricate a time/ticket or claim success. Adapter completion checks the latch and ticket before taking the typed writer path; absent ticket or latched fault refuses with zero writer entries. Do not invoke the clock hook while holding the slot mutex. Generic driver catch handles a hook panic; after driver drainage recover the actual retained owner for retirement. No clock facts are accepted solely from candidate.updated_at_ms. Reserve/validate the ticket before returning facts; any failure uses the latch protocol rather than a nonexistent fallible facts return.
- Add internal Work candidate entry **without enabling adapter green yet**. Proposed private signature includes actual execution_id, ExecutionFence, `&CommitPlan`, `&TransitionFacts` and `&mut DriverAdapterAccount` for the newly introduced expected/candidate traversal and comparison overhead. Refactor public commit and this private entry through one shared `commit_impl(facts, Option<CandidateLoan>)`. `CandidateLoan` is permissions-private, borrowed, never a public/raw constructor; it holds the candidate references and a short mutable adapter-account loan, separate from the mutable OutcomeAccount. Complete both expected/candidate bounded traversals before comparison and before the typed writer. Old outcome processing uses its unchanged account/ceilings.
- Preserve Work ordering: checked nonzero attempt -> same account.enter -> real receipt request -> reconstruct expected exact plan -> existing bounded plan/resource validation -> **when a candidate exists compare exact execution ID, current owner/revision fence, candidate expected_revision and whole CommitPlan against the receipt-derived expected plan** -> existing typed Store port -> existing exact returned Commit validation -> completed=true. Candidate refusal must precede the port and must finish the entered account phase with Work retained. Public commit calls the same helper with None; preserve its observable semantics, limits and sticky attempt accounting.
- Ticket-aware private Store path first verifies ticket and candidate facts/time under adapter account; then calls the private Work entry. Do not add a public candidate constructor, outcome substitution or fresh outcome account. Even a matching origin/attempt cannot replace full candidate/returned plan validation.
- Pure controls initially fail on deliberately unsupported matcher/ticket entry, then pass with exact validation. Review the internal refactor against adopted outcome public-commit controls and run those focused compatibility controls before enabling driver writes. No silent relocation of fallible purchases after durable commit.

## Task 3: Enable exact receipt-backed completion GREEN

**Files:** adapter and private runtime_use_outcome append-only Commit projection.

- Add private consuming `PermissionUseOutcomeCommit::into_driver_execution(self) -> StoredExecution`: move `self.resource.execution.thread_id` and `self.plan.snapshot` into StoredExecution after Work already validated Commit. No clone, serialization, new allocation, Store call or Result occurs after acknowledgement. Fields left in Commit retire normally. Keep projection private; do not expose full raw plan or unvalidated Commit construction.
- Change only adapter Store.commit's supported branch: take the actual Settled Work under guard, require exact ticket, use same Work candidate entry, restore actual Work on any error, mark Durable only after successful exact seal validation and return consuming projection. Refuse all other phases/commands/targets with no underlying raw fallback. Record typed outcome port1/raw commit0.
- Preserve error truth in opaque failure even though Store.commit must map failures into StoreError (e.g. static InvalidCommit/Unavailable classification). Save actual PermissionError in private slot before returning mapped StoreError; distinguish a private adapter/ticket/account fault latch from an actual typed storage-port failure. A facts/ticket fault must not be labelled a known precommit storage error or used to enable the trigger-abort retry path; failure retains Work and truthful local-fault classification. Do not expose sensitive payloads or mislabel an ambiguous backend error as known precommit. The scaffold RED has no such typed error yet.
- Run the **same Task 1 test** and require actual GREEN: true result, exact revision4/resource+1/journal+1, callback1, expected kernel Complete snapshot and byte-identical permission/grant/acceptance/use rows. Verify owner3 persistence and ordinary cleanup. Driver/scheduler/facts port/underlying raw guard remain unchanged.
- Independently review actual source, GREEN result, account behavior and protected diff. Record first supported scope honestly. A build alone is not green behavior evidence.

## Task 4: Genuine refusals, returned failure and owned retirement

**Files:** sibling SQLite fixture and adapter internal tests; no existing fixture edits.

Each integration test obtains its own genuine healthy producer/renewal/Receipt graph, captures full typed/raw storage before operation, and completes owner retirement/reopen/removal before its final feature assertion. Native controls use no fake successful backend or forged receipt. Reuse a new sibling-local setup helper only once its operation points and ownership are explicit.

- `permission_use_driver_returned_failure_is_persisted_once`: actual native returns existing bounded Failed("native-failure"); real driver persists exact kernel FailAttempt, callback1, original authority rows unchanged and durable Failed after reopen. Establish RED for this real callback behavior before fixing any newly uncovered support issue. Kernel's existing failure semantics need no retry implementation.
- `permission_use_driver_candidate_definition_substitution_is_refused`: wrap actual underlying SQLite Store with a test-local faulting read adapter. After actual callback, change only the next completion `Store.execution` result: increment actual Definition.max_attempts from3 to4 in that observed cloned snapshot (or another legal finite value if the adopted fixture differs). Persisted SQLite and actual Claim/Receipt are untouched. `Driver::OwnedTurn` reads thread_view, while Runtime.complete_command uses execution; the intended fault path lets the driver derive a candidate carrying the substituted definition. Future reachability gate: first verify the substituted observation passes actual kernel validation and reaches the private full-plan matcher; do not promise that from document inspection. Require matcher refusal before typed outcome port entry, full storage equality, callback1 and same actual Work retained. A kernel/read rejection is setup failure and earns no candidate-matcher credit; choose another explicitly bounded kernel-valid observation substitution only after current-source review if this one cannot reach the matcher. Pure controls separately vary execution/fence/outcome/task/attempt/input/facts/full-plan fields. Do not add a production candidate injection hook or fake a successful Store write.
- `permission_use_driver_raw_completion_remains_refused`: construct the ordinary kernel completion candidate from real current claimed state and actual observed outcome, call underlying raw SQLite commit directly; unchanged refusal/equality. Driver adapter succeeds only with its genuine retained Work. No copying receipt-derived authority into raw Store.
- `permission_use_driver_renewal_refusal_keeps_claimed_attempt`: real policy/gate/fence refusal after genuine Dispatch, callback0, actual claimed PreparedEffect/invocation reported by generic driver, no invented receipt or cancellation cleanup. Normal retirement with exact claimed storage preserved.
- `permission_use_driver_nonsettled_receipt_retires`: existing native uncertain or invalid-output return, callback1, no typed outcome writer, truthful Receipt status retained with anchors until failure Drop. Add native panic variant if needed for existing receipt payload retention, and a bounded panic-on-drop payload witness whose disposal remains inside owned failure retirement; do not infer cleanup from a catch alone.
- `driver_adapter_account_is_cumulative_and_sticky`, `driver_slot_invokes_once`, `driver_slot_recovers_poisoned_owner_for_retirement`: pure account/slot tests. Verify inclusive ceilings/overflow, no reset on refusal/retry, one consume, state mismatch refusal and recovered owner normal drop. Add `Send`/`Sync` compile-time assertions for concrete callback Work/OutcomeWork/Mutex pair and scoped failure transfer; no unsafe/static tricks.

## Task 5: Late rollback and explicit storage-only retry

**Files:** new sibling fixture; opaque failure retry method; account/slot controls.

- `permission_use_driver_late_abort_retains_work_for_explicit_retry`: install real journal trigger after callback setup so actual typed settlement fails late and rolls back. First real driver result is failure, callback1, same settled Work alive, full typed/raw state equal to postcallback/precompletion capture. Normal driver drain releases its ordinary capacity; failure retains all receipt authorities until retired.
- Clear the known precommit trigger using the fixture's existing controlled direct-SQL mechanism. Call failure.retry_settlement once with trusted fresh facts. It uses existing Work.commit and the same Work/account plus cumulative adapter overhead; no callback, new driver invocation, new dispatch/renewal or new use. Require exact persisted result and callback still1, then normal reopen/cleanup.
- Initial explicit retry method can refuse to establish its own RED after the same trigger-aborted setup; make only this storage-only method green. Refuse retry on nonsettled/no Work/already durable/sticky exhausted state. Ambiguous response loss is preserved as failure/unknown, never inferred success or automatic retry advice.
- `permission_use_driver_corrupt_resource_revision_preserves_work`: use the fixture's explicitly controlled direct-SQL fault mechanism to change only the thread resource_revision to another valid encoded revision **after callback and before settlement**. This is deliberate corruption/fault injection, not a genuine cancellation or supported resource-changing transition: raw fenced cancellation refuses this permission target and deletion does not handle its new children. Capture full typed/raw nine groups before corruption; save the literal original resource_revision bytes; apply the one-field corruption; capture full state after corruption/immediately before settlement; require exact settlement refusal, callback1, actual Work retained and full storage equality before/after that refused settlement. Restore the literal original revision bytes, verify full typed/raw state equals the pre-corruption capture, then normally retire failure/anchors, reopen owner3 and remove storage. Guard restoration on setup/test failure so corruption does not silently persist. Do not retry, fake a successful mutation, or infer cancellation capability from this control. A genuine independently changing resource/cancellation transition remains open for separate design; no unsupported mutation API is a fixture prerequisite.

## Task 6: Verification and narrow adoption

- Independent whole-current-source review checks ownership/lifetimes, complete candidate/ticket/Commit binding before writer, guard restoration on error/panic, callback count, normal retirement, bounded independent account, failure error truth and absence of production test escape. Review the named exact old-body exceptions/inverse diff. Do this before native behavioral runs at each stage, without repetitive fee-packet bureaucracy.
- Use the existing reviewed local controller for finite-time process launch, kill/reap and sensitive mode0600 logs; command intents below are commands for future execution, **none were run when writing this plan**. Build flags/environment/target/dependency/SDK bindings must be those actually reviewed for the new copy. Source changes invalidate the corresponding prior behavior receipt.

```text
cargo +1.93.1 fmt --all -- --check
cargo +1.93.1 test --workspace --locked --offline --no-run
cargo +1.93.1 clippy --workspace --all-targets --locked --offline -- -D warnings
cargo +1.93.1 test -p b4-sqlite --test persistent_permissions --locked --offline use_driver::permission_use_driver_settled_result_requires_receipt_commit -- --exact --nocapture --test-threads=1
```

Obtain `--list` once, verify the exact emitted `use_driver::...` test name and save it before the intended RED. Do not accept a run with zero tests. Apply the same exact-name rule to all controls. Expected initial scaffold run: one test FAILED at final expected success, actual completion UnsupportedOperation after cleanup. Expected green: same one test PASSED.

```text
cargo +1.93.1 test -p b4-runtime --lib --locked --offline driver_adapter -- --nocapture --test-threads=1
cargo +1.93.1 test -p b4-sqlite --test persistent_permissions --locked --offline use_driver:: -- --nocapture --test-threads=1
cargo +1.93.1 test -p b4-sqlite --test persistent_permissions --locked --offline use_outcome:: -- --test-threads=1
cargo +1.93.1 test --workspace --locked --offline
```

- Focused adopted producer/renewal/callback/outcome compatibility and existing generic-driver tests pass. Run strict format/no-run/lint and full workspace regression, with already approved local loopback access where HTTP regressions need it. Broaden/rerun only when new source changes or unresolved failures justify it. No old receipt transfer to a modified head.
- Independently accept actual success/failure/rollback/retry/refusal/retirement evidence at the exact selected head; verify all protected generic bodies and raw SQLite guard unchanged. Narrow adoption copies only accepted new files and explicit private seams with ordinary before/inverse/after checks. No Git/commit assumption.
- Publish concise scope/evidence pointers after adoption; keep broader cancellation/recovery/HTTP/mixed-task/human/legacy/lost-response work open and conservative capability declarations unchanged. Include no unsupported claim of exhaustive fees or whole-process bound.

## Concrete API status

The production ownership path uses external references, a borrowed private slot, trusted public hooks and existing synchronous scoped driver APIs; no unresolved lifetime/authority escape is intended. Wrong-candidate integration uses a deliberately inconsistent **read observation** from a test-local wrapper over real SQLite, allowing a genuine callback/Receipt to reach the actual matcher without a production injection API. Before adopting that test, verify kernel validation accepts the substituted definition and that the recorded failure is the matcher before typed writer entry; a kernel/read failure is setup failure. No implementation or native execution is authorized by merely writing this file before its prerequisite is met.
