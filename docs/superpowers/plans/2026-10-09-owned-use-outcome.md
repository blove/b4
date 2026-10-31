# Owned NewUse outcome settlement implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task-by-task. The continuing user authorization selects local execution with independent source and result reviews. Do not ask for another routine execution choice.

**Goal:** Save the actual validated success/failure retained by a genuine NewUse callback receipt without replaying the callback or accepting replacement authority.

**Architecture:** A consuming Work retains the entire Receipt. A private flat request borrows the actual claim resource, invocation, use witness, origin, current context, outcome and trusted facts. A new object-safe typed port initially refuses; a subsequent separately reviewed runtime/SQLite writer checks the full current resource/source and exact kernel plan under Immediate before atomically storing snapshot, resource revision and journal.

**Tech Stack:** Pinned Rust1.93.1, existing b4-contract/kernel/runtime/sqlite, rusqlite, serde_json and vendor/defaultstd dependency profile. No new dependencies or schema changes.

**Current status, 2026-10-20:** Tasks 1–6 are complete for the narrow functional scope governed by the continuation below: fresh intended RED, receipt-owned settlement, atomic SQLite writer, eight integration cases, fifteen protocol controls, strict static checks, full normal workspace regression and exact fifteen-file code adoption. Independent source, quality, result and adoption-map reviews passed. See the [adopted scope and fresh evidence](../../../conformance/permissions/use-outcome/README.md). Original exhaustive-operation-audit checklist items remain unfinished and unqualified. Supported changing-resource projection and live-Work owner handoff also remain open; external-corruption controls provide no such credit. Continue with the separately planned Driver bridge.

**Design:** `/private/tmp/b4-root-use-outcome-design-v1.md`. Follow its narrow scope and the accepted `/Users/blove/repos/b4/spec/runtime/persistent-always-design.md`. This plan grants no native execution, fee, adoption or broad capability credit. Each gate requires a current whole-source review and actual receipt.

## Protected baseline and isolation

The callback source was adopted with eight selected code files and six documentation files. Verify the qualification record's current eight code hashes plus current public inventory before creating an isolated copy. Preserve old public bodies/fields/test fixtures exactly; the trait/Arc new port and module exports, new append-only sibling loans and new files are explicit exceptions. No Git writes are requested: this repository has an unborn branch and read-only Git storage, so use a private current-tree copy under `/private/tmp` rather than attempting a worktree or committing. Existing passing current-source workspace receipts supply baseline evidence; a redundant baseline native run supplies no additional feature credit.

## Task 1: Test-first reachable settlement baseline

**Files in the private workspace only:**
- Create `crates/b4-sqlite/tests/persistent_permissions/use_outcome.rs`.
- Append test module linkage to `crates/b4-sqlite/tests/persistent_permissions.rs`.
- Do not change the existing `use_callback.rs` or `use_renewal.rs` fixture bodies.

- [ ] Copy the genuine healthy producer/renewal/callback fixture into an independent sibling. Retain actual original owner1 acceptance, owner2 use/Dispatch, complete C2/gate/R4 renewal, fixed exact callback invocation/context and true Bool/empty-writes return. Use no fabricated claim/permit/source, bypassed old guard or replacement fake Store success.
- [ ] Write the intended settlement-success assertion before any green backend code. Consume actual Receipt into proposed Work, then call Work::commit with trusted facts(109). Save result/error code and copied usage/status observations without unwrapping the expected missing feature.
- [ ] Capture typed thread/executions/journals and all raw nine storage groups immediately before and after settlement, before selecting the feature result. On the baseline unsupported path, assert full before/after equality; all permission/grant/acceptance/use records stay byte-identical.
- [ ] Observe actual callback calls/matches1 and owner anchors alive while Work exists. Retire Work before remaining application/catalog/authority/Store aliases. Close owner2 with sole strong owner, reopen owner3, verify typed/raw capture and owner epoch handling, close it and remove storage before the final settlement assertion.
- [ ] Print one fixed non-sensitive evidence line before the final assertion with actual port/error result, callback count, retained status/outcome match, owner ids and directory removal. Do not print raw payloads, grants or sensitive tokens.
- [ ] Keep setup/callback assertions separate: the intended RED is a genuine settled receipt reaching the fixed unsupported settlement port after normal cleanup. Missing API/compiler error, renewal failure, timeout or cleanup failure is setup failure.
- [ ] Save a private fixture draft and exact test-first descriptor before introducing the minimal interface scaffold.

## Task 2: Minimal sealed unsupported interface scaffold

**Files in private workspace:**
- Create `crates/b4-runtime/src/permissions/runtime_use_outcome.rs`.
- Append exports/module linkage in `permissions/mod.rs`.
- Append paid sibling loans to `runtime_mutation.rs`, `runtime_use_renewal.rs`, `runtime_use_callback.rs`.
- Add typed default port and Arc delegation in `permissions/model.rs`.
- Append isolated purpose constructor/debit facades and pure controls in `permissions/validation.rs`; preserve all original bodies/fields/limits.
- SqliteStore port implementation remains absent and defaults to UnsupportedProfile.

The public API shape is:

```rust
pub struct PermissionUseOutcomeWork<'producer, 'current, 'op, S, A> {
    // private: whole genuine callback receipt, fresh cumulative account,
    // separate private Work-origin, checked monotone attempt number,
    // successful-completion phase; no Clone or revealing Debug.
}
pub struct PermissionUseOutcomeRequest<'request> {
    // private: actual flat retained permit loan, actual outcome, trusted facts,
    // borrowed Work-origin plus exact nonzero attempt number.
}
pub struct PermissionUseOutcomeCommit {
    // private: actual stored execution plus request-bound successful writer seal.
    // No public/raw constructor. Scaffold has no constructor at all.
}
// Consuming, nonfallible conversion initializes a fresh settlement account.
impl<S, A> PermissionUseCallbackReceipt<'_, '_, '_, S, A> {
    pub fn into_outcome_work(self) -> PermissionUseOutcomeWork<'_, '_, '_, S, A>;
}
impl<S, A> PermissionUseOutcomeWork<'_, '_, '_, S, A> {
    pub fn commit(&mut self, facts: &TransitionFacts)
        -> Result<PermissionUseOutcomeCommit, PermissionError>;
    pub fn usage(&self) -> PermissionEvidenceUsage;
    pub fn callback_status(&self) -> PermissionUseCallbackStatus;
    pub fn settled_outcome(&self) -> Option<&EffectOutcome>;
}
// New object-safe PermissionStore method; private factory makes Request opaque.
fn commit_permission_use_outcome(
    &self,
    request: &PermissionUseOutcomeRequest<'_>,
    account: &mut PermissionUseOutcomeAccount,
) -> Result<PermissionUseOutcomeCommit, PermissionError>;
```

Explicit implementation lifetimes and existing PermissionStore/ApprovalAuthority bounds must be named; the abbreviated outline is not literal compiling source. The supplied &TransitionFacts comes from the same trusted embeddedhost/runtime boundary as existing Runtime::complete_outcome; the fixture uses its genuine host facts(109). Bind those exact facts to Request and the complete kernel plan. Apply the existing kernel monotonic-clock validation, never historical grant expiry or new policy/constraint reauthorization against completion time.

OutcomeAccount contains no public authority or reset API. It supplies private cumulative purchases and public copied usage only. No independently constructible account is needed: Work initializes it before its first debit.

- [ ] Implement the account with zero initial usage, sticky checked addition and a purpose distinct from ordinary8MiB/dispatch16MiB/renewal64MiB/callback8GiB. Review and choose a small scaffold-only fixed ceiling from its actual finite graph; do not claim it funds future readers/writers. Nodes/bytes, failure inception, full Work/Receipt moves and retirement are paid separately.
- [ ] Implement Claim::settlement_use_loan by paying its own new ticket, matching actual NewUse witness and same producer origin, and returning only private borrowed resource/invocation/witness/origin. Do not call an old session to fabricate a new settlement seal.
- [ ] Implement Permit::settlement_use_loan by paying a separate new ticket and adding the renewed InvocationContext, with Store borrow returned separately. The whole Permit retains catalog/application/authority anchors.
- [ ] Implement Receipt::settlement_request in its owning module by paying the actual request/branch tickets, accepting only Settled with actual Succeeded/Failed, constructing the opaque nongeneric flat request and returning the retained Store borrow. Reject absent/nonsettled/Cancelled without synthesizing a result.
- [ ] Give Work its own private move-stable origin, separate from the mutably borrowed Account, and a checked nonzero monotone attempt number. Overflow refuses; every request binds that exact origin+attempt. Account admits one fresh session for that exact attempt and rejects reused entered/finished seals. Request borrows Receipt and the separate immutable Work origin, avoiding a borrow of Account origin overlapping the mutable port loan. Fund every actual origin allocation/clone, header, attempt primitive and normal end; neither this nonce nor the scaffold origin is storage authority.
- [ ] Implement consuming Receipt::into_outcome_work as a pure owner transfer. Work::commit pays attempt entry before inspection; it constructs the genuine Request and calls the typed retained Store port. It never invokes callback, new permission authorization or a constraint. Errors preserve Receipt and cumulative account; there is no reset/automatic retry.
- [ ] Leave typed Commit construction impossible in the unsupported scaffold. The future green writer alone constructs it after validating and committing a complete transaction. Work success phase validation is added with that sealed writer and corresponding test; the scaffold must not fabricate success or invoke deep green code.
- [ ] Add default UnsupportedProfile and Arc delegation, with concrete Args/return/Result/ControlFlow and closure/carrier size assertions for every real source site. Keep old trait methods/delegation unchanged.
- [ ] Add pure controls for fresh zero authority/usage, distinct purpose refusal, inclusive limits/sticky failure and checked overflow. These controls do not mint a fake Receipt or execute a callback.
- [ ] Independently review whole current scaffold/fixture source, complete private/public inverse maps and protected existing diffs before running any native test.

## Task 3: Static qualification and actual intended RED

- [ ] Freeze a new full Source packet with current public/private inventory, selected predecessor diff and exact private/source heads. Account for six documentation transitions as history; the current public inventory governs. Preserve all adopted callback source bodies except explicitly appended sibling methods/linkage.
- [ ] Review protected static operators with finite subprocess timeouts, group kill/reap and exclusive0600 before/log/after/receipt files. Check original flags/environment/SDK/defaultstd dependency heads and immutable full-source guards. A fmt/no-run/lint success is not execution credit.
- [ ] Run formatting check, workspace compilation without execution, strict workspace all-target lint and focused sqlite package compilation without execution. Record current actual binary/target and concrete carrier widths; no old-image layout/PC count transfer.
- [ ] Price the complete selected baseline fixture exterior and all new scaffold transports/primitive children and normal retirement. Predecessor source invoices may be used only for literal source/call subgraphs with exact current bindings; every new owner/move/branch/loan has its own fee. No remaining callback capacity or diagnostic/assertion reporting pays settlement work.
- [ ] Obtain independent current whole-source and fee/operator eligibility for precisely one new settlement-success test, then run it once in a fresh process using locked/offline sqlite persistent_permissions --exact --nocapture --test-threads=1.
- [ ] Verify the process is reaped without timeout and the test fails at the intended settlement-success assertion, with real successful setup, callback exactly1, receipt retained, full unchanged storage, normal owner3 reopen/close and directory removal already established. Independently accept this actual RED before Task4.

## Task 4: Separate green runtime preparation and bounded source sessions

**Files:** create a focused `runtime_use_outcome_source.rs` if source validation would make outcome module unwieldy; append narrowly named validation facades; add backend object-safe typed read APIs only as needed. Preserve old Dispatch/renewal sessions and producers.

- [ ] Introduce new branded outcome read/write stages bound to the genuine request/account and original producer origin. Validate complete resource and all necessary use, decision, acceptance, grant, creator and journal ranges with header/type/length/count/cap checks before payload decoding.
- [ ] Initial supported projection is single-task NewUse at unchanged original owner, turn and full resource/claim fence; altered concurrent/cancellation projection is refused. Validate actual requested attempt/effect/input/definition, marker, thread incarnation and full original/current source identity. No new authority/gate call.
- [ ] Reconstruct Complete or FailAttempt exclusively from actual retained outcome and invocation with trusted facts. Validate existing outcome bounds and kernel transition; bind complete CommitPlan and post resource/journal. Reject CancelAttempt/nonsettled results.
- [ ] Independently review the complete new source and fees/actual carrier bounds before allowing the backend to consume its seals. No successful writer seal exists yet.

## Task 5: Separate SQLite Immediate writer and GREEN

**Files:** create `crates/b4-sqlite/src/permissions/use_outcome.rs`; link it in permissions/mod.rs and implement only the new typed port. Existing raw Store::commit refusal remains unchanged.

- [ ] Under one Immediate transaction independently reread resource and full required provenance using the new sealed sessions. No authority, constraint or callback runs inside the transaction.
- [ ] Reconstruct exact command and compare the full CommitPlan with candidate, siblings, state/output writes, turn/history, attempts, owner, definition/call identity and all fences before any update. Validate actual SQLite owner.
- [ ] Apply exact execution snapshot CAS, thread resource-revision CAS and completion journal append. Check affected rows; all permission/grant/acceptance/use sources remain literal. Only after commit return the privately sealed outcome Commit bound to this request. Work validates the exact same private Work-origin, nonzero attempt number, full expected committed plan and owner/resource/attempt fences before recording completion. An origin match alone is insufficient across explicit attempts.
- [ ] Review new whole Source, fee/capacity and protected operator eligibility. Run the same selected test GREEN; prove actual persisted result and full expected snapshot/resource/journal after owner3 reopen, no callback replay and normal retirement.

## Task 6: Failure controls and narrow adoption

- [ ] Add returned-failure persistence test, journal-trigger late rollback test, and known precommit-failure storage-only explicit retry. Each gets a genuine legal setup and intended RED before required new behavior, without changing existing tested parents.
- [ ] Add stale/foreign owner/attempt/resource/source refusals with complete typed/raw storage equality. No fake successful backend or invented authority.
- [ ] Price each own fresh fixture graph/loan/transport/retirement and any second explicit commit from the same cumulative Work budget. Retry refuses after sticky exhaustion or known successful completion. Lost-response confirmation remains open; do not turn an ambiguous error into success.
- [ ] Run selected controls only after their own whole-source eligibility; independently review actual results, then current-source compatibility tests, strict lint/format/no-run and full workspace regression with approved local loopback access when necessary.
- [ ] Independently review exact narrow public adoption, adopt only qualified selected bodies with before/inverse/after verification and ordinary rollback protection. Write truthful conformance/outcome docs with exact source/evidence hashes and local-evidence availability limits.
- [ ] Keep broad58 unchecked, durable capability false, Driver adapter/cancellation/recovery/human/legacy/outcome unknown separate. Continue into separately planned Driver adapter only after this narrow adoption is accepted. Do not stop merely because a local milestone is reached.

## Continuation on 2026-10-20

Temporary-file cleanup removed the unadopted outcome workspace and its private review packets. Recover literal source and historical evidence from this chat's saved records into `.b4-work/outcome-recovery`; preserve provenance and distinguish exact hash-matched restoration from reconstruction. Do not execute historical commands or transfer their test results to reconstructed source. The adopted callback baseline remains intact and supplies the reconstruction base.

Continue the existing receipt-owned settlement architecture and transaction safeguards. Prioritize a working, narrowly tested implementation with explicit input, depth, row-count and cumulative-account limits. The exhaustive audit assigning fees to individual Rust standard-library operations remains unfinished and is not a prerequisite for functional testing. Do not claim that audit, its speculative capacity totals, or its missing artifacts as a product guarantee or qualification result.

Before enabling persistence, independently review the recovered current source, attempt/origin binding, complete provenance checks, cumulative limits, full-plan comparison and commit ordering. Use a distinct bounded outcome account; preserve old account limits and refusals. Establish fresh formatter, compiler and focused-test results for the actual reconstructed source. Then complete Tasks 5 and 6 with genuine SQLite persistence, rollback, explicit retry and no callback replay. Keep broad conformance and durable capability declarations conservative until their actual qualification requirements are met.

Keep reconstruction drafts and execution evidence in the durable work directory until narrow adoption. Preserve the earlier negative reviews and original RED receipt as historical evidence; record new source hashes and actual results separately.
