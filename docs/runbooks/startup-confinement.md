# Bounded opt-in native startup confinement

Qualified scope, 2026-10-03: six private invariant declarations, eight independent API declarations and two real TCP declarations passed focused runtime qualification and independent review. Complete macOS and isolated Linux qualification each passed 1,235 distinct Rust cases. Three separate focused metadata methods passed after meaningful initial failures. The scoped receipt binds 18 public Rust files and the final checker, metadata test and scoped requirement Source. Post-support whole Python/contracts and the immutable external final source freeze are recorded separately in external acceptance; this scoped record does not report their results.

This separate embedding API can finish one bounded Preparing pass after confining coherent prior-owner native work whose exact recorded Definition is unavailable or whose current Recover policy denies dispatch. The embedding supplies a trusted principal resolver and an independent confinement authority. Existing `begin_preparation`, ordinary known-ID confinement, constructors, routes and CLI defaults retain their behavior.

## Start the explicit mode

1. Construct `HttpHost::new_with_application_preparing` with an actually durable approval Store, RequiredDurable approval configuration, immutable native application configuration, authenticator and current ResourcePolicy. Isolate direct Store, service and writer aliases throughout preparation. The host barrier cannot prevent arbitrary external Store mutations.
2. On the existing Tokio executor, call `begin_preparation_with_confinement` once with borrowed Arcs of `StartupPrincipalResolver` and `RecoveryConfinementAuthority`, plus `StartupPreparationLimits`. An unpolled call clones neither trusted handle. First poll checks limits and executor availability, registers counted startup ownership, then clones the handles. Old and new entry points share one attempt; a second start is AlreadyStarted.
3. Observe `StartupConfinementPreparationHandle::status` or `wait`. Its status/outcome contains the existing preparation values and two separate pure counters: `confirmed_blocks` and `already_blocked`. The handle and wait retain no Store, application, callback, claim, permit or owner token. Pending or dropping a wait does not cancel preparation or retire resources.
4. Obtain `serving_host` only after actual Complete. Health remains available during Preparing; readiness and ordinary admission remain closed through confinement jobs and final framework capture disposal. A normally recovered Runnable callback can still be running after Complete. Completion is an admission milestone, not callback settlement or physical retirement.

## Resolve and preserve the original

Discovery supplies no original turn ID. The counted coordinator therefore retains the existing ordinary whole `ThreadView` bootstrap, descriptor checks and trusted origin resolution. The resolver must consult current server-controlled authority. Stored origin labels cannot authenticate a principal. All three origin fields, native application scope and the complete returned Principal must agree.

The inherited ordinary view materialization and bootstrap resolver are outside the new confinement capture's serialized-data bounds. The opt-in path then requires the existing bounded confinement ports and auditable native authority profile for each applicability candidate, including a candidate that later follows ordinary recovery. Unsupported custom Store ports or legacy profiles refuse unchanged; the default preparation API remains available with its existing behavior.

The first bounded capture and its single renewal must each equal the entire original view. A matching descriptor or target alone is insufficient. Each initial and renewed reason observation resolves and validates the original principal again before checking the exact Definition or current Recover policy. Missing, changed, invalid or out-of-scope authority never becomes permission to confine. This startup mode does not use the ordinary known-ID observer's third reason, RequiredAuthorityUnavailable.

Exactly two reasons are supported here. ExactDefinitionUnavailable means the recorded complete Definition cannot be supplied, even if a newer route revision exists. DispatchNotAuthorized means the exact Definition is available but current Operation::Recover policy denies dispatch, with stored and requested provenance taken from the original route/revision. Neither a newer validator nor a newer callback substitutes for the original.

Independent confinement permission is requested twice against that same whole original and fixed reason. The existing backend performs the third bounded equality audit within its fenced atomic commit. The host verifies the complete returned receipt, fresh turn uniqueness, actual owner advance and checked revision/resource increments before confirming evidence. No confined candidate invokes an input/output validator or native callback.

Confinement retains the complete Definition, input, execution/task/attempt/result/interrupt state, accepted decisions, consumed and unconsumed grants, acceptances and prior journal history. It applies the existing no-dispatch RecoveryBlocked transformation and one fresh commit journal entry. Accepted Once consumption is not refunded; expiry is not revocation. This operation does not reconcile or cancel an external effect.

An already blocked prior-owner candidate must still pass the bounded native authority/journal audit, whole-original equality and trusted bootstrap checks. Its unchanged audit counts `already_blocked`; it does not re-adopt, request fresh confinement permission or mutate rows. Generic status inspection, a current-owner block or an over-budget profile does not qualify this shortcut.

Only NotApplicable without a confirmed receipt can fall through to existing typed recovery. The joined confinement job first releases its pure original alias and claims. The coordinator unwraps the same owned original view and passes it to the unchanged recovery path. It does not clone a replacement view, select a latest Definition, retarget, retry or confine after a later recovery failure.

## Interpret progress and incomplete results

Existing pages, scanned rows and candidates cover all considered discovery records. Existing admitted, runnable, parked, blocked and terminal counters continue to describe confirmed typed recovery outcomes. A confinement receipt does not increment typed admitted or typed blocked.

`confirmed_blocks` increases only for a verified returned Confined receipt, including the same verified receipt retained through a later cleanup fault. `already_blocked` increases only for a successful fully audited unchanged result. Their sum cannot exceed considered candidates or the configured candidate limit. During Preparing, separate observations are monotonic lower bounds rather than a global atomic snapshot. Finished includes the stable final tuple.

Commit-then-error or panic without a verified returned receipt is ambiguous. The focused controls observe a durable block in these cases while confirmed progress remains zero and preparation becomes fault-classified Incomplete. Absence of a receipt is not rollback evidence. Conversely, a verified receipt remains counted through later trusted-handle disposal failure without allowing Serving to open after fault.

Local refusals leave ordinary admission closed with Incomplete RecoveryRefused; shutdown and sticky faults retain their existing Shutdown/Faulted final classification. Earlier verified blocks or admissions remain committed. There is no batch rollback, automatic retry, rescan or budget reset. A mutation lease that already won the shutdown cut can finish counted work, but cannot reopen admission.

## Keep work bounded

| Bound | Maximum |
| --- | --- |
| Discovery pages / rows per page / scans | 64 / 32 / 2048 |
| Considered recovery candidates | 256 |
| Confinement audits per candidate | Initial capture, one renewal, atomic commit capture |
| Snapshot input / thread components | 1 MiB / 256 KiB |
| Grants and acceptances | 64 each; 128 KiB per encoded record |
| Selected journal | 4096 rows including a reserved append; 4 KiB per row |
| Aggregate selected input per audit | 8 MiB / 65536 lexical nodes |
| Each encoded row | Depth 64 / 16384 nodes |

Configured discovery/candidate limits may be smaller. Eligible appends therefore allow at most 4095 existing selected journal rows; unchanged prior blocks may audit 4096. SQL uses raw persisted input and Memory uses canonical borrowed input. The kernel's separate canonical safe-clone and replacement caps retain their existing rules.

These are selected serialized-data and work-structure bounds. Individually coherent pages do not form a global snapshot. Ordinary bootstrap and later typed recovery retain their inherited reads. No whole-startup heap, CPU, I/O, database, storage, RSS or hard completion-time bound is claimed. Fixture gates use finite watchdogs, including 15-second gate/I/O waits, 120-second whole TCP watchdogs and a 64 KiB response cap; those test limits are not production deadline guarantees.

## Retire ownership separately

The coordinator and confinement job retain counted owners, claims, permits and mutation leases through independently fenced resolver/authority disposal, opaque replacement panic payload disposal and final Shared teardown. Never-polled cancellation is also covered. A held trusted destructor keeps resources and the startup barrier live.

Explicitly release trusted gates, allow normal callback/coordinator/cleanup returns, begin shutdown and await normal Quiescent with the expected healthy or intentionally fault-classified outcome. Normally stop and join any listener. Drop all caller and framework Store, application, host, router, resolver, authority and callback aliases. Require absent relevant Weak references, a genuine next SQLite writer with actual owner=current+1, readiness and whole persisted state comparison, followed by normal successor closure.

The TCP qualifiers separately observe closed admission through actual confinement permission and final capture cleanup. The mixed case observes readiness open after one confinement and original typed recovery while the real callback remains held, then requires explicit callback return before normal listener and resource retirement. Timeouts, forced cleanup, aborts and container removal do not supply positive retirement evidence.

## Reproduce the focused boundaries

After source and environment gates are accepted:

```text
cargo test -p b4-http --lib --locked --offline -- startup::confinement::tests::
cargo test -p b4-http --test startup_confinement --locked --offline
cargo test -p b4-http --test startup_confinement_tcp --locked --offline
```

The six private declarations cover borrowed/unpolled ownership, attempt/limit order, whole-original/principal guards, independent trusted-handle disposal, verified versus ambiguous progress, and final capture cleanup. The eight API declarations cover both positive block reasons, default compatibility, resolver/separate-authority refusals, audited prior blocks, original typed recovery, custom capture refusals and bounded scan stopping. The two TCP declarations exercise actual listeners and normal closure. Helpers, loop modes, retirement markers and reruns add no declarations.

Four private inherited controls passed against the compatibility scaffold. Two meaningful initial guard failures occurred only after legal state and normal physical retirement: the first whole-original capture was not reached, and the first actual confinement write was not reached. Later principal/original and receipt-fault branches received no initial RED credit; they passed in current GREEN execution.

The eight-case scaffold target had two compatibility passes and six intended first-assertion failures. Missing-definition and denied-Recover failures repeated the original positive RED observations. The authority case passed unresolved/mismatched-principal controls before failing the first denied-authority call; unavailable-authority and later assertions were initially unreached. AlreadyBlocked failed Complete before its later counter/capture assertions. The first Unsupported custom case failed the expected capture-call vector before any port error was reached; later profile, target and whole-view cases were initially unreached. Scan stopping failed the expected candidate/outcome assertion before the later two-block and third-record checks. These later branches gain current GREEN coverage, not retrospective RED credit.

The first TCP attempt failed local loopback bind permission setup: zero passed, two failed, and no behavior or retirement credit. The same source and selection then passed both cases under authorized local execution. Compilation, source/style setup corrections and parsing bookkeeping are retained separately from behavioral failures. The optional evidence parser initially refused a protected test-body configuration before producing output; its narrow reviewed successor later reconciled both completed platform logs. Neither parsing attempt executed a Rust case or changed the independently closed counts.

## Record only the qualified scope

Focused qualification and independent review accepted exactly 6 private + 8 API + 2 TCP declarations, all passed. Compatibility checks passed five old targets with 40 passes and two ignored helpers; the complete HTTP library passed 101 entries. These focused observations do not substitute for final all-workspace platform qualification.

Complete frozen-source macOS arm64 and isolated Linux aarch64 qualification each closed 106 outer Cargo targets with 1,232 normal passes, zero failures and eight default ignores. Three separately selected ignored SQLite cases passed, giving 1,235 distinct qualified Rust cases on each platform. Two nested CLI probe summaries were excluded; helpers, repeated selections and the 16 new feature declarations add no duplicate credit. All format, strict workspace lint and test checks passed on unchanged qualified Rust source. The Linux run used a pinned image, a fresh target, a nonroot identity, no network or host mounts and all capabilities dropped; normal idle container cleanup remains separate from runtime retirement. Complete-source and actual execution evidence received independent review.

The pre-metadata macOS Python suite passed 111 methods and the prior checker passed its unchanged 459 named requirements. The three separate startup metadata methods initially produced 12 intended failure entries, zero errors and no timeout in 20.003 seconds: the actual YAML group was missing, ten malformed scope/status/evidence variants were ignored, and the injected empty inventory was ignored. The first missing-group assertion stopped before later declaration/output checks; the first empty-inventory assertion stopped before five acceptance variants and all 26 prior-group duplicate checks. Those initially unreached branches receive no retrospective RED credit.

The exact scoped checker and pending one-row YAML then passed all three focused methods with zero failures/errors and no timeout in 54.381 seconds. This successful run reached all ten malformed variants, five acceptance variants, 26 duplicate variants and the full declaration/output checks. The final scoped row preserves all 459 prior named requirements and binds the current 16 public test declarations; the resulting source inventory is 460 named requirements. The trace group is `startup_confinement_requirements`, with exactly STARTUP-CONFINE-001, component startup-confinement, source `spec/runtime/startup-confinement-design.md` and release scope `explicit_bounded_opt_in_native_startup_confinement_only`. Its bounded pass token is `passed_bounded_startup_confinement_native_macos_linux_scope`. Metadata validates traceability, scope and evidence presence separately from runtime behavior.

The receipt binds only the 18 qualified Rust source hashes and three final public checker/metadata hashes. The qualified inputs comprise 299 crate files including manifests and 287 Rust `.rs` files. Post-support whole Python/contracts and immutable final source acceptance are recorded separately in external acceptance. This scoped record reports neither those whole-suite counts nor their outcomes.

The predecessor's unresolved inherited macOS CLI transport-timer failure remains retained in its own history; unchanged diagnostics and later passes neither erase it nor establish its cause. Existing accepted-Once and two-lane receipts retain their original scope. All 56 broad release scenarios, M0 and CP010 remain open. Persistent Always, actual confinement deletion/reincarnation races, actor/secret reconstruction, legacy authority migration, unblock/permanent revocation, provider/effect reconciliation, native chunks, AP parity, automatic default or CLI startup and full RFC11.4 remain outside this increment. Original TypeScript/client and Node unit-suite evidence remains historical; retained recording integrity checks are separate from rerunning those suites. No arbitrary exactly-once external-effect guarantee is made.

Private identifiers, authority material, original storage captures, control addresses, raw logs and their digests stay private. The public record binds only public code/metadata hashes, with no self-referential document/final-freeze hash.

See the [design](../../spec/runtime/startup-confinement-design.md), [implementation plan](../superpowers/plans/2026-10-03-startup-confinement.md) and [scoped receipt](../../conformance/expected/startup-confinement-verification.json). Post-support whole-suite checks and final immutable source acceptance are recorded separately in external acceptance.
