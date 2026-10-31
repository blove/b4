# Explicit prior-owner confinement

Status: native implementation and actual macOS/isolated offline Linux qualification accepted; final support SPEC and QUALITY, post-documentation checks and external source freeze with independent binding audit complete.

A native embedding can explicitly block a known unfinished execution owned by an earlier SQLite writer when its recorded Definition is unavailable, trusted original authority cannot be resolved, or current recovery policy denies dispatch. Call `HttpHost::confine_prior_owner` with the original `RecoveryConfinementTarget`, the existing `StartupPrincipalResolver`, and a separate `RecoveryConfinementAuthority`. This operation records a durable block without invoking an effect, settling a task, or activating startup readiness.

## Choose the original resource

Use server-controlled diagnosis to select a known execution. Preserve the complete original thread incarnation, active execution, turn and execution/resource revisions. `RecoveryConfinementTarget::from_view` checks the supplied ordinary view's identity relationship; it does not bound that earlier read or authorize mutation. The operation performs its own bounded capture and audits the actual current source. Discovery records and labels do not grant permission.

Use the real host-owned SQLite Store and its normal immutable native application. Its actual writer generation must be nonzero and strictly newer than the execution's recorded owner. A current/future-owner execution refuses. Memory's ordinary owner remains one; no prior-owner Memory success is qualified.

The audit requires complete current authority evidence when the recorded tasks require it. Direct historical Park/Once/Always records with missing required durable evidence remain `UnsupportedAuthorityProfile`; changing configuration does not manufacture missing grants or acceptances. Contradictory present records remain corruption. A previously committed acceptance does not regain authority because its grant later expired or current policy changed.

## Supply separate mutation authority

The existing resolver checks the original stored application, tenant and principal against current trusted authority. It supplies an actual matching principal or refuses; the operation never substitutes a principal derived from labels alone.

The reason has fixed precedence:

1. The immutable application lacks the complete recorded Definition: `ExactDefinitionUnavailable`.
2. Exact executable support exists, but trusted original resolution is unavailable or invalid: `RequiredAuthorityUnavailable`.
3. Resolution succeeds, but current `Recover` policy denies dispatch using the recorded route and execution revision: `DispatchNotAuthorized`.

Supported and dispatch-authorized work returns `NotApplicable`. Dispatch refusal itself does not permit confinement. The separate authorizer receives the entire original ordinary `ThreadView` and selected reason, and must explicitly permit this durable mutation.

After the first permission, the operation wins the existing shutdown mutation lease, recaptures once, compares the whole original view, reobserves the same reason and renews the same separate authority once. Trusted hooks run outside Store locks and SQL transactions. Changed view, target, authority or reason refuses; the operation does not retry, refresh or substitute a new target. A fully audited prior-owner `RecoveryBlocked` observation returns `AlreadyBlocked` unchanged before resolver, policy or mutation-authority callbacks.

The returned future borrows caller handles until polled. First poll requires an existing executor and healthy host, then ordinary registration, capacity and the same-thread claim precede framework handle clones. A lease won before shutdown may finish across the admission cut. A cut won first prevents mutation.

## Interpret durable results

A confirmed confinement changes only the execution's blocked status, fixed failure reason, owner, fresh turn/history, revision and timestamp; it increments the owning thread's resource revision and appends one journal entry. Task inputs, effects, results, attempts, grants and acceptances remain unchanged. SQL updates and the single journal insert must all succeed in one transaction before a receipt can be returned.

`Confined(receipt)` means the host verified the actual returned receipt's original identities, new fences, fresh turn and reason. `AlreadyBlocked` confirms an unchanged audited prior block and supplies no new receipt. The journal does not preserve a full immutable policy/resolver/authorizer audit, and replacing the execution failure does not create a permanent history of the old failure.

Expected static backend refusals are local. Corruption, infrastructure unavailability, panic or a contradictory backend receipt faults the host. `RecoveryConfinementError::confirmed_receipt` may retain a previously verified actual receipt through later cleanup faults. Its absence never establishes rollback when a backend committed and then failed or panicked. A receipt does not establish readiness, callback settlement or physical retirement.

## Keep resource limits separate

| Selected data | Input limit |
| --- | --- |
| SQLite Snapshot spelling | 1 MiB raw UTF-8 JSON |
| Memory Snapshot | 1 MiB measured borrowed canonical JSON |
| Owning thread components | 256 KiB summed metadata, origin, identity strings and fixed integers |
| Grants / acceptances | 64 of each, at most 128 KiB per JSON record |
| Selected execution journal | 4096 records including a reserved append; at most 4 KiB each |
| Complete selected input | 8 MiB and 65536 JSON nodes |
| Each JSON record | Depth 64 and 16384 nodes |

Eligible work therefore has at most 4095 existing selected journal records; an unchanged prior block may audit all 4096. Raw SQL JSON spelling and plain indexed bytes are accounted separately without selecting an overflowing payload. The pure kernel has a separate 8 MiB canonical safe-clone guard. Replacement output has separate canonical Snapshot 1 MiB and journal 4 KiB caps, including shape limits. A compact raw input can legally have larger original canonical bytes and shrink into a permitted replacement.

Limits bound selected data, parsing and cloning; they do not establish a hard heap, CPU, database scan or completion-time ceiling. Otherwise valid larger histories refuse unchanged. Exact aggregate-edge inclusivity and isolated acceptance-only count 65 are not qualified by the conservative public crossing cases.

## Retire the actual owners

Trusted resolvers, authorizers and destructors can hold resources until they return. The counted job retains claim, capacity, mutation lease and its owner group through independent disposal of each cloned handle, opaque replacement panic payloads and final Shared resources. Caller cancellation or dropping an awaiting future does not release those captures.

Explicitly release trusted gates, allow normal completion and cleanup, begin shutdown, wait for normal quiescence, stop and join any listener, and drop every external host, router, Store and application alias. Prove physical retirement with absent Store/application Weak references and a successful real next-owner SQLite writer open/readiness check. Counters, timeouts, forced cleanup and container removal are not that proof. A caller that abandoned its wait has no returned receipt to invent.

## Scope and verification

This is an explicit bounded native operation. It adds no HTTP route, CLI startup scan, automatic RFC11.4 readiness, unblock, permanent revocation, provider reconciliation or legacy authority migration. Existing unfinished-work deletion behavior remains unchanged. Full release gates, the broad crash matrix and core profile remain open; original TypeScript and Node unit-suite evidence remains historical.

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

The [scoped verification receipt](../../conformance/expected/prior-owner-confinement-verification.json) binds actual qualification source/log digests and independently reviewed platform evidence. Implementation-private tests and independent public qualifiers remain separately identified; the three slow cases passed under actual separate selectors.
