# Embedded Settled Permission-Thread Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development, fresh sequential authors and independent source then actual-result quality gates. User continuous authorization overrides routine approval/Git pauses. Isolated ordinary source copy, no Git writes, nested delegation or parallel implementation. Never delete actual user data; tests own all fixtures.

**Goal:** Atomically delete an entirely settled permission-bearing thread through an explicitly authorized bounded embedded Rust/SQLite operation.

**Architecture:** Exact historical ThreadDeletionTarget plus separate current Store owner anchor -> bounded complete typed capture -> current dedicated deletion authority outside locks -> private prepared request -> one Immediate recapture/validate/delete/count/commit -> immutable actual success confirmation. Generic deletion and all current HTTP profiles remain unchanged.

**Tech Stack:** Rust1.93.1, current schema4/SQLite/permission record codecs, locked offline dependencies, finite native verification.

## Stable entry API

Export from b4_runtime::permissions:

```rust
pub trait PermissionThreadDeletionAuthority: Send + Sync {
    fn authorize_delete(
        &self,
        context: &ApprovalContext,
        resource: &PermissionThreadDeletionResource,
        target: &ThreadDeletionTarget,
    ) -> Result<(), PermissionError>;
}

pub fn delete_settled_permission_thread<S, A>(
    store: Arc<S>,
    authority: Arc<A>,
    context: &ApprovalContext,
    target: &ThreadDeletionTarget,
) -> Result<PermissionThreadDeletionConfirmation, PermissionError>
where
    S: PermissionStore + 'static,
    A: PermissionThreadDeletionAuthority + ?Sized + 'static;
```

The immutable Resource exposes `view() -> &ThreadView` and `store_owner_generation() -> u64`; it conveys no permission to execute a callback. Confirmation has private fields and immutable `target()`, `store_owner_generation()`, `removed_execution_ids() -> &[ExecutionId]` and `removed_rows() -> usize` getters, no public constructor/deserialization. Removed rows count the eight owned tables, excluding unchanged metadata. Authority may use fresh current policy independent of historical permission policy, but context actor must match recorded thread origin. No credential/header material is stored or returned.

New PermissionStore declaration: `permission_settled_thread_deletion_atomic_durable() -> bool`, defaultfalse, Arc-forwarded, real SQLitetrue. Separate `capture_permission_settled_thread_deletion` and `commit_permission_settled_thread_deletion` ports use new typed selector/capture/prepared/commit/account types in the new module; production author owns their internal signature details. Defaults return UnsupportedProfile. No generic Store fallback. Capture/commit values are partial source evidence, never execution authority or fabricated success. Fixed account has no reset/refund or public mutable inner codec budget.

## Task 1: Independent real deletion expectations

**Create:** `crates/b4-sqlite/tests/permission_settled_thread_deletion.rs`; allow one new dedicated support module if it improves clarity, with Root informed before creation. Do not import whole existing test files or edit old helpers/assertions.

- [x] Root publishes approved design/plan at `docs/superpowers/specs/2026-10-21-permission-settled-deletion-design.md` and `docs/superpowers/plans/2026-10-21-permission-settled-deletion.md`, captures exact public baseline/ordinary isolated source/shared build target/bounded controller. Full suite900s, other commands600s, kill/reap/source maps/log hashes. W is closed before setup.
- [x] Fresh author writes four bounded business tests against stable API: complete genuine original Always plus multiple genuine saved uses deletion/other-thread preservation/healthy reopen; actual settled cancelled original and saved-use history (including real acknowledgement or retained late outcome); focused current-authority/origin/stale-target/nonterminal/default-capability/overflow/reverse-foreign-reference refusal controls with unchanged durable rows; exact late DELETE fault after prior dependent deletions with complete nine-table rollback, exact trigger presence/drop and fresh healthy deletion control.
- [x] Provision every positive permission/Use through actual embedded APIs with normal identifiers and nontrivial bounded input. Reuse neutral support in crates/b4-sqlite/tests/permission_support/mod.rs where valid; source examples permission_original_always_cancellation.rs, permission_use_cancellation.rs and persistent_permissions normal-input helpers are references only. No direct SQL provisioning or forged writer confirmation. Clearly label only necessary adversarial stale/overflow/foreign/trigger SQL setup on owned data.
- [x] Include legitimate prior-owner settled history after normal reopen and separately mismatched target historical owner fence. New operation rejects every nonterminal execution even though generic validate_thread_deletion allows active WaitingForApproval. Verify generic deletion still refuses valid permission targets. At least one meaningful normal row-limit boundary and overflow refusal; no maximum-composition or corruption matrix.
- [x] All waits/threads/native callbacks have finite barriers/drain. Snapshot all nine physical table rows after setup; compare equality/digests without dumping raw grants or sensitive records on assertion failure. Never log grant/token payloads. Trigger DDL/presence and safe counts may be reported. No maximum-size, live retirement, recovery or cross-platform claim.
- [x] Independent source gate, Root runs `cargo test -p b4-sqlite --test permission_settled_thread_deletion --no-run --locked --offline` for actual missing-entry/types/declaration RED, then independent quality/API-results gate. No production before this gate.

## Task 2: Dedicated runtime and SQLite operation

**Create:** `crates/b4-runtime/src/permissions/thread_deletion.rs`, `crates/b4-sqlite/src/permissions/thread_deletion.rs` (focused private submodules allowed only with Root informed).
**Modify:** runtime permissions/mod.rs export, permissions/model.rs default ports/declaration/Arc delegation, existing validation.rs only a small dedicated fixed codec-budget initializer or pure reusable visibility if concretely needed; SQLite permissions/mod.rs module/trait delegation. Any further existing-file change needs concrete source reason and Root agreement. No HTTP/contracts/schema/dependency changes.

- [x] Fresh author implements stable entry/authority/resource/confirmation and dedicated typed ports/account. Defaultfalse before capture; actor/origin/current exact target authorization; all current-owned and legitimate historical settled statuses; separate current Store owner bound while exact target retains historical execution owner/revision. Policy outside locks/transactions. No live approval owner-equality gate, current effect constraint/latest-handler reinterpretation, callback, Kernel transition or reused cancellation authority.
- [x] Bound captured rows:16 executions;64 each approval grants/approval acceptances/permission acceptances/decisions/uses;512 journal rows. Fixed64MiB cumulative evidence and65,536nodes with existing snapshot/thread/record/depth caps. LIMIT+1 refusal, raw type/length before owning payload, one account for capture/copy/codec/authority-resource/recapture/equality work without resets/refunds. Reuse actual pure codecs through a sealed account; do not expose mutable inner budget. Finite evidence does not promise all row maxima compose or bound total database SQL scan work. No exhaustive std allocation/operation audit.
- [x] Capture complete thread/execution/journal/approval/permission source through both thread and execution indexes. Preserve physical header/payload equality, generations/origins, exact final journal, contiguous revisions, grant/Once-Deny relations and genuine Always acceptance/decision/Use historical witnesses. Reuse pure validation logic from existing readers using bounded new SQL, never their unbounded loops or legacy permission refusal. Use needs actual matching decision/binding/old before-snapshot and committed journal point; decision needs actual original creator/accepted-decision linkage.
- [x] Reject foreign active pointers/dependent execution ownership, indexed foreign Uses of owned permission IDs, and foreign decisions with payload-only creator_execution_id pointing at an owned execution. No creator SQL column exists. Use guarded raw type/size/JSON/creator checks and bounded returned evidence, no global owned in-memory table. Unknown/oversize/malformed foreign creator representations that cannot rule out the owned creator conservatively refuse without writes; do not silently skip them. Document this conservative refusal and unclaimed SQL scan/time scope. No schema migration.
- [x] One Immediate writer recaptures exact complete source and target/current Store owner, rechecks all settled states and relationships, verifies schema-version/checksum metadata anchors (normal open retains exact schema-object validation), clears active pointer, removes Uses before Decisions plus all acceptance/grant/journal rows before executions/thread. Verify each deleted count and absence before commit; preserve metadata/other threads. Actual confirmation only after successful commit. Unknown outcome remains error, absence alone never manufactures prior-success proof; no auto retry.
- [x] Late-trigger fault installed after normal Store open must reach actual dependent deletes, so do not add a live sqlite_schema full-object scan that turns it into an early schema refusal. Keep metadata anchors/currentowner and concrete SQL validation. Rollback complete durable rows on actual error, fixed safe diagnostics/with_code_only_errors, no raw record Debug/logging or grant disclosure.
- [x] Independent source gate; Root runs actual new four-business-test GREEN and unchanged permission_legacy_guards/permission_records regressions, then quality/results gate. Correct only concrete demonstrated failures with the same author and narrow reapproval; no weakened expectations or new matrix.

## Task 3: Frozen verification, scoped delivery and adoption

- [x] Root freezes final reviewed source and runs new target, legacy guards/record regressions, fmt, strict workspace all-target lint, compile and full locked/offline suite. Record actual child/helper summary counts, commands, source maps, log hashes, exit/reaping/timeout and metadata separately. Existing loopback escalation is authorized. Poll<=45s/commentary<=60s. Failed/partial/timeout runs do not qualify.
- [x] Add embedded runbook, scoped conformance README/qualification, root README and staged plan. Describe exact API/limits/current authority/complete settled set/foreign-creator conservative refusal/actual atomic deletion; no HTTP capability, automatic recovery/replay, live retirement, global SQL-time, maximum-size or platform overclaim. All source/deps/current HTTP profiles/example modes remain frozen except authorized runtime/SQLite changes.
- [x] Independent integrated source/results and delivery reviews, explicit report/source/file-hash approvals, exact selective adoption with first backups, native equality before docs, separate public metadata and independent public integrity. Keep future adoption/acceptance unchecked until actually completed; close only completed items afterward as subsequent documented state.
- [ ] Continue into HTTP wiring only after this embedded increment closes; ask for real missing input/manual action rather than milestone finals.
