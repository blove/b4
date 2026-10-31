# Two-lane process startup restart qualification

Status: qualified bounded native scope, 2026-10-03. The two real-process restart lanes passed on macOS arm64 and Linux aarch64, and the separate metadata wiring checks passed on their recorded sources. Post-documentation checks and final source reviews are recorded separately.

This harness qualifies two finite restart scenarios using existing native HTTP and SQLite APIs. It adds one integration target, seven test/helper files, two credited parent cases and one ignored child selector. The four child role/lane selections are infrastructure and add no test credit. Production APIs, launcher defaults, schemas, dependencies and existing test bodies remain unchanged.

## Read the two observations

The approval lane first records a successful Pure sibling and a stable required Once approval park in a real native child. After actual SIGKILL and reap, the parent independently opens SQLite, compares the complete original ordinary view, approval records and ordered journal with the child's private checkpoint, then drops every Store alias before the restarted child begins preparation.

The restarted child uses `HttpHost::new_with_application_preparing` and a fixed trusted `StartupPrincipalResolver`. Health remains available; readiness and ordinary admission remain closed before preparation and while the resolver is explicitly held. A completed preparation preserves the original pending identity, prompt, resume key and grant. The original full Once interrupt identity and token are submitted through the existing resume request. Only the release task runs; the sibling result, writes, immutable Definition, input and attempt budget remain preserved except for the expected continuation changes.

The Unsafe lane syncs one actual external ledger entry while its attempt is durably InFlight, then kills and reaps the seed process before a result commits. The independent SQLite reread anchors the original source. Restart preparation classifies the attempt OutcomeUnknown and the execution RecoveryBlocked without invoking the Unsafe callback again. Readiness then opens, a thread read remains available, and unrelated finite Pure work completes. The selected block and single ledger entry persist.

No initial frame is required from the original native POST stream. Its killed response is treated as a bounded EOF/reset cut rather than a normally completed chunked/SSE response. Successful approval continuation requires the real complete terminal stream. The readiness ordinary probe uses `GET /threads/{id}`; RecoveryBlocked state reads keep their existing unsupported response.

## Interpret ownership separately

The parent inspector open after the kill consumes an owner epoch. The restarted child must have an actual owner strictly newer than both the source and inspector; it is not promised to be seed owner plus one. No parent Store or membership writer exists during preparation.

Preparation Complete and readiness do not prove callback settlement or physical ownership release. Both lanes require healthy normal quiescence, normal listener completion, all Store/application/host aliases dropped, absent Store and application Weak references while observation handles remain alive, normal child exit and reap. Only then does the parent open a real next writer and require its owner to equal the observed restarted child owner plus one. Complete selected state, approval and journal captures are compared again after that open. Forced cleanup, timeouts and container removal supply no positive retirement evidence.

## Keep private evidence bounded

Use fresh directories with mode 700 and create-new data/lock/evidence files with mode 600. Check present SQLite sidecar modes. Control sockets use short private paths checked before bind. Public failures use a fixed taxonomy; child stdout/stderr and sensitive captures remain private.

| Item | Fixed maximum |
| --- | --- |
| Capture before encoding/persistence and before parsing | 1 MiB |
| Selected fixture collections | 1 grant, 1 acceptance, 64 journal entries |
| Control request / reply / commands | 1024 bytes / 2048 bytes / 16 |
| Control I/O / held gate / parent lane watchdog | 30 s / 60 s / 120 s |
| Parent evidence reservation | 16 MiB minus 1024 bytes |
| Seed and restarted child evidence | 8 MiB each |
| External effects ledger reservation | 1024 bytes |
| All persisted role evidence and external ledger | 32 MiB total |
| Each child stdout/stderr capture | 1 MiB each |

Reservations are disjoint with no borrowing. Each sealed role ledger includes its own serialized bytes; the parent independently sums all actual sealed role file sizes and actual external ledger bytes before success. SQLite data/sidecars and control sockets are outside this serialized-evidence accounting. Diagnostic overflow sets persistent loss while subsequent output is drained and discarded; it cannot count as complete evidence. Existing Store collection loaders are used only with trusted finite fixtures, not advertised as a general bounded capture API. These limits do not promise hard CPU, I/O or RSS deadlines.

## Reproduce the selected parents

Run only after the source and execution gates for the environment are accepted:

```text
cargo test -p b4-http --test startup_restart --locked --offline -- process_startup_park_preserves_once_and_sibling_without_replay --exact
cargo test -p b4-http --test startup_restart --locked --offline -- process_startup_unsafe_effect_is_blocked_before_readiness_without_replay --exact
```

The ignored `startup_restart_child_helper` is selected only by an owned parent. Running it directly is not qualification. Compiler, fixture, protocol, permission and cleanup failures are distinguished from runtime invariant failures; an existing API passing does not create a retrospective behavioral RED. The separate three Python metadata tests check trace wiring and evidence-file presence, not native behavior.

## Current evidence and remaining scope

Completed macOS arm64 checks bound 704 inputs and 280 crate files. The outer workspace passed 1,215 entries across 103 targets, with zero failures and 7 default ignored markers. Three selected slow SQLite cases separately passed, giving 1,218 distinct qualified case executions. Two nested CLI child summaries are excluded. Both new parent cases passed once in their target; the helper and four roles add no case credit. macOS formatting, strict lint, 105 Python tests and contract checks passed on those sources.

Linux aarch64 checks also completed formatting, strict lint and the frozen Rust suites with the same recorded totals, and independent source/execution review passed. Three new metadata declarations had a genuine missing-group RED (13 failed entries; later branches are not all credited as RED), then all three passed after the checker and two initially pending RESTART rows were adopted at the recorded 705-input metadata stage. The prior 456 requirement records remain exact. RESTART-001/002 are implemented and passed within the bounded native macOS/Linux scope. Post-documentation checks and final source reviews are recorded separately.

This two-lane qualification does not cover accepted approval commit before dispatch through Preparing startup, automatic incompatible/revoked-policy confinement, recovered stream-history signaling, provider reconciliation, persistent Always, the actual confinement deletion/reincarnation race, CLI production startup exposure or full RFC11.4. The 56 broad release scenarios, M0 and CP010 remain open. Original TypeScript client and Node unit-suite evidence remains historical. SIGKILL proves that process stopped; it proves neither external rollback nor arbitrary exactly-once effects. See the [design](../../spec/runtime/startup-process-restart-design.md) and the [scoped verification receipt](../../conformance/expected/startup-process-restart-verification.json).
