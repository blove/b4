# Accepted Once before dispatch: Preparing restart qualification

Status: bounded runtime and metadata qualification passed, 2026-10-03. macOS and Linux source/execution reviews and the three metadata methods are complete. Whole support review precedes public adoption; post-documentation checks and final external source freeze are recorded separately.

This harness qualifies one finite real-process restart boundary using existing native HTTP and SQLite APIs. Six new Rust files supply one integration target, one credited parent and one ignored child selector. It reuses three unchanged process, transport and evidence helpers. Child roles, repeated runs and helper summaries add no case credit.

## Follow the accepted intent

A real native seed child first parks a required durable approval after a parallel Pure sibling succeeds. The initial stream completes with the original interrupt and a terminal output of null. The parent uses that original full Once envelope for the existing resume request.

A test-only Store wrapper forwards every existing Store and ApprovalStore method. For successful Accept only, it waits after the actual SQLite commit and returned StoredExecution, before delivering success to the worker. The task is approved Ready, the original grant is consumed, and its full acceptance is durable. No release callback has run. The hook performs no capture or ledger I/O and holds no SQL transaction.

The parent independently derives the entire accepted state from the parked capture and original approval. It seals the checkpoint, checks the gate again, verifies the original grant is still valid, then kills and reaps its owned seed process. The interrupted resume response is an EOF/reset cut without a terminal completion. It supplies no completed SSE framing claim.

Only after reap does an independent SQLite inspector open, compare the full snapshot, thread, grant, acceptance and journal, and drop every Store alias. The fixture then waits until actual wall time exceeds the original recorded grant expiry. It never changes that expiry or requests a replacement approval.

## Observe real automatic preparation

The restarted child uses the same immutable native Definition, input, prompts and policy with direct SQLite storage. Health responds while ordinary admission and readiness stay closed before preparation and during a deliberately held trusted resolver. The resolver validates the original native origin and returns a trusted fixed principal after explicit normal release.

Startup preparation automatically recovers the already accepted intent. A separate release callback gate holds after real Dispatch while preparation finishes Complete. Complete is an admission milestone; it does not prove callback settlement. At this boundary the release callback has entered once, the sibling has not replayed, no recovery input validation runs, and the external ledger contains only the original sibling entry.

The complete dispatched state derives from Recover and Dispatch of the original accepted state. Attempts advance from one to two within a maximum of four. Full consumed grant and acceptance records, sibling result and writes, input and Definition remain preserved.

After explicit callback release, the callback writes its one release ledger entry and returns the fixed result. The full terminal snapshot derives from Complete. Its reduced joined values contain sibling then release, and all original approval history remains unchanged. The fixture sends no second resume during preparation.

## Prove retirement separately

Final durable state alone does not prove resource release. The restarted child must reach healthy normal quiescence; its listener must finish; all Store, application, host, route and callback aliases must drop; Store and application Weak references must be absent. Resolver and release callback gates must have returned normally after explicit release, without expiry or cleanup release.

A resource-free control reader sends the retired Stop reply, then joins before the child helper returns. The parent requires normal exit, reap and completed log drains before opening a genuine final SQLite writer. The actual observed owner sequence is seed, inspector, restart, final writer; the final writer must be the restart owner plus one. It rereads the complete final state and drops its aliases.

The intended seed SIGKILL is distinct from this positive restart retirement proof. Forced cleanup, timeouts, container removal, an empty Weak reference in isolation or directory existence do not replace the complete sequence.

## Keep evidence finite and private

| Item | Maximum |
| --- | --- |
| Each capture before encoding and parsing | 1 MiB |
| Fixture grants / acceptances / journal entries | 1 / 1 / 64 |
| Control request / reply / commands | 1024 bytes / 2048 bytes / 16 |
| Control I/O / gate / parent watchdog | 30 s / 60 s / 120 s |
| Original grant lifetime / expiry polling bound | 10 s / 30 s |
| Parent evidence reservation | 16 MiB minus 1024 bytes |
| Seed and restart evidence | 8 MiB each |
| External ledger reservation | 1024 bytes |
| Aggregate persisted evidence | 32 MiB |
| Child stdout and stderr, each | 1 MiB |

Fresh directories are mode700; owned data and evidence files are mode600, with no symlinks. Each role ledger counts its own serialized bytes. Reservations are disjoint and the parent verifies actual file totals including the external ledger. Private authority, prompt, identifiers, control addresses and database captures stay outside public receipts.

These are finite fixture and serialized-evidence bounds. SQLite files, source and build outputs are excluded. Existing collection loaders operate on trusted small fixtures; this is not a general bounded capture API or a hard CPU, I/O, storage or RSS guarantee.

## Reproduce the selected boundary

After the relevant source and environment gates are accepted:

```text
cargo test -p b4-http --test startup_accepted_once --locked --offline --no-run
cargo test -p b4-http --test startup_accepted_once --locked --offline -- process_startup_accepted_once_after_expiry_preserves_intent_and_sibling --exact
cargo test -p b4-http --test startup_accepted_once --locked --offline
```

The ignored startup_accepted_once_child_helper is infrastructure selected by the owned parent. Run it only through that parent. Compile, setup, timing, permission and abnormal cleanup failures are distinguished from behavior failures. The first actual behavioral observation passed using existing runtime APIs; no retrospective behavioral RED is claimed.

## Recorded runtime execution and remaining support gates

The warning-free focused source passed no-run compilation, the exact selected parent, the default target, strict HTTP lint and formatting. Exact and default execution count one unique parent. The retained artifacts were independently checked against original-derived states, full consumed approval history, real original expiry, readiness cuts, explicit normal gate release, normal restart retirement and genuine next-writer ownership.

The frozen source adds exactly six Rust qualifier files and one Cargo target while preserving the three reused process, transport and evidence helpers and all old protected sources/test bodies. No production API, CLI default, dependency, wire schema or storage schema changes are introduced. This subsequent finite case qualifies the accepted-before-dispatch Preparing restart window left open by the preceding two-lane increment; the preceding two-lane receipt and statements retain their historical scope.

Actual full-source macOS arm64 and isolated offline Linux aarch64 execution each completed 1,216 outer normal workspace passes across 104 Cargo targets, plus three separately selected slow SQLite passes: 1,219 distinct qualified case executions, zero failures and eight default ignored markers. Two nested CLI summaries are excluded. The new parent ran once per platform; its ignored child selector, two roles, focused repeats and helper summaries supply no extra case credit. Formatting and strict workspace lint completed with exit zero on both platforms. Independent final source/execution reviews for both platforms are accepted.

The first macOS workspace attempt failed an unchanged inherited CLI transport-timer assertion before the new parent was reached. That failed/incomplete run is retained, its root cause remains unresolved, and it is not classified as an environment exclusion or a new-feature or metadata RED. An unchanged isolated run of that existing CLI test and an unchanged complete macOS rerun subsequently passed. Those observations neither erase the original failure nor add case credit or establish its exact cause.

macOS completed 108 Python tests and the unchanged contract checker with its existing 458 named requirements, including the exact separate two-row process startup restart group. The three new metadata methods first ran with the old checker and missing actual group: all three failed through eleven assertion/subtest entries, with no errors. Nine scope/status/evidence mutations and the empty inventory were ignored; the third method stopped before its later substitution/duplicate loops, which receive no initial RED credit. After four minimal checker changes and one initially pending YAML record, all three methods passed. Exact parent identity, helper substitution, duplicate IDs, narrow scope/status and evidence-file presence are validated separately from runtime behavior. ONCE-STARTUP-001 records the already qualified bounded native scope, with the old 458 records and exact two-row process restart group preserved. The current single-record inventory contains 459 named requirements. Subsequent full Python/checker execution, final source review and external freeze are recorded separately after support adoption; no future suite total is credited here.

This case does not qualify automatic incompatible or revoked-policy confinement, recovered stream-history signaling, provider reconciliation, persistent Always, the actual confinement deletion/reincarnation race, production CLI startup exposure or full RFC11.4. All 56 broad release scenarios, M0 and CP010 remain open. Original TypeScript client and Node unit-suite evidence remains historical. The fixed Pure callback and ledger do not promise arbitrary exactly-once external effects. Serialized fixture/artifact caps do not bound the whole startup heap, database, CPU, I/O, storage or RSS.

See the [design](../../spec/runtime/startup-accepted-once-design.md) and the [scoped receipt](../../conformance/expected/startup-accepted-once-verification.json). The public receipt records this finite runtime and metadata scope; post-documentation execution and final source acceptance are external records.
