# Bounded unfinished-execution discovery implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans after root accepts the public design and current predecessor freeze. Work task-by-task with independent review gates.

**Goal:** Add a read-only bounded Store page API that exposes typed recorded unfinished observations for trusted embeddings, without adoption, dispatch, policy reconstruction or readiness changes.

**Architecture:** Common runtime DTOs, cursor provenance and borrowed snapshot validation/accounting define semantics. Memory holds one mutex; SQLite holds its existing mutex and one read transaction, fetching at most32 snapshots plus a separate identity-only lookahead. Optional default refusal preserves custom stores.

**Tech stack:** Existing Rust, b4-contract/kernel/runtime/sqlite, serde/serde_json and rusqlite only. No new dependency, schema, manifest, lock, AP production, kernel, driver or capability change.

The exact scoped design, implementations and independent qualifiers passed whole SPEC and separate QUALITY review. Fresh macOS/Linux Rust and macOS Python qualification passed against an exact input freeze. Final public-support SPEC and QUALITY reviews and the immutable external source freeze are complete. This increment uses the shared checkout under the current filesystem constraints and performs no Git mutation, deployment or publication.

## Proposed source ownership

Existing narrow exceptions after a fresh root baseline: crates/b4-runtime/src/lib.rs (module/consumer exports), store.rs (optional trait method + existing Arc/& delegation only), memory.rs (resource-free origin field + discovery delegation), crates/b4-sqlite/src/lib.rs (new module, origin field/constructor and Store delegation). Preserve exact old bodies and all old tests. No current selected source baseline is changed by this draft.

New runtime files: crates/b4-runtime/src/discovery/mod.rs (consumer types/errors), backend.rs (source marker/shared projection/page budget and public decode_snapshot byte/shape/version/typed decoder), shape.rs (new fixed-state JSON accounting; protected inspection uses validation.rs, not a shape.rs), tests.rs or clearly named private test modules. New portable integration: crates/b4-runtime/tests/unfinished_discovery.rs and unfinished_discovery_support/*.rs.

New SQLite files: crates/b4-sqlite/src/discovery.rs and private focused test module(s); crates/b4-sqlite/tests/unfinished_discovery.rs and unfinished_discovery_support/*.rs. New independent integration qualifier: crates/b4-http/tests/unfinished_discovery_recovery.rs and new support files only, using existing b4-sqlite dev dependency. Source path sharing/ownership must be assigned explicitly by root before parallel authors work.

Public design/plan/requirements, new docs/runbooks/unfinished-discovery.md and the scoped receipt are root-owned and separate from production authors. The cooperative-cancellation external freeze is complete. No speculative public receipt or new requirement passed status is written before actual qualification.

## Task1 — accept exact design and baseline

- [x] Root completes predecessor actual qualification, public support review and external freeze.
- [x] Read exact public adopted design/plan; pin source-backed limits, API/type/error names, existing Arc/& scope and explicit cursor/backend helper lifetimes. Independent full SPEC then different QUALITY reviews, root reads both; capture accepted-doc bridge.
- [x] Root captures a fresh exact baseline/protected list and package/feature graph with only four production existing-file exceptions plus explicitly authorized additive support docs/hooks. Old stopped-inspection and all predecessors stay byte exact. No manifest/lock changes.
- [x] Assign runtime/Memory author, SQLite author and independent new qualifier author exclusive files. Publish shared compiling API first; avoid formatting another author's existing files.

## Task2 — compiling API and meaningful behavioral REDs

- [x] Add consumer constants/types/static errors and resource-free origin/cursor with private fields. Register optional Store::discover_unfinished returning UnsupportedOperation; add delegation only to existing Arc/& wrappers, with no new wrapper implementation or Box-related tests. Existing Arc/reference qualification remains required.
- [x] Add successful MemoryStore-generated Accepted fixture with origin, thread generation and full immutable Definition. Expected returned record must contain actual execution/thread/resource fences, Current and false uncertainty flags.
- [x] Run `cargo test --locked --offline -p b4-runtime --test unfinished_discovery memory_admitted_execution_is_discovered -- --exact`; require setup succeeds and only observed default refusal versus expected record fails. Preserve log/hash and compiling stub sources before green code.
- [x] Add actual SqliteStore fixture: create/admit under ownerN, capture actual IDs/fences/grants where relevant, close all handles, reopen under N+1. Expected discovery record is Prior with original revision/owner and unchanged thread resource/generation; no recovery occurs.
- [x] Run `cargo test --locked --offline -p b4-sqlite --test unfinished_discovery reopened_unfinished_execution_is_discovered -- --exact`; require real open/admit/reopen succeed and stub refusal causes the behavioral assertion. Preserve meaningful RED, not compile/setup output.
- [x] Root verifies both red receipts before implementation; notify other authors when exact stubs compile. No original client import/native fixture execution is relevant to this primitive.

## Task3 — shared semantics and Memory GREEN

- [x] Implement cursor source/owner checks using private Arc<()> marker identity; private helper fields retain no Store/resources. Capture cross-Memory sameowner, different SQLite sameepoch and actual successor reopen refusal tests.
- [x] Add raw-byte shape guard and streaming counting writer tests first: byte/depth/node boundaries, escaped quotes/backslashes split at every boundary, Unicode, string-contained brackets, partial primitive tokens and own-limit flag classification. Run focused private tests to capture meaningful boundary REDs before helper code.
- [x] Implement exact compact JSON counting with serde_json::to_writer directly into the fixed-state bounded lexical writer; no temporary JSON buffer, BufWriter, buffering adapter or children vector. Memory measures the borrowed typed Snapshot before kernel validation/common projection. Aggregate successful row bytes with checked counters capped at32MiB =32 ×1MiB. A compliant32-row page cannot fail solely for aggregate bytes; preserve progress. Implement runtime discovery::backend::decode_snapshot(&[u8]) for byte/shape checks before the bounded u64/IgnoredAny version probe and direct Snapshot decoder. Runtime owns the serde probe using its existing serde dependency.
- [x] Implement shared indexed snapshot/thread projection: supported version, kernel validity, index/owner/thread identity, current/prior relation and both task uncertainty flags. Classify budget violations LimitExceeded, malformed relationships CorruptRecord and future owner OwnerConflict. Validate terminal rows before dropping them.
- [x] Memory method locks once, checks marker/owner, collects at most33 borrowed map IDs without copying snapshots, and examines up to32 rows in order. Validate active/foreign pointer relationships under the same lock, consume the serialization budget and emit only small descriptors. Hidden inactive unfinished must fail; no partial page escapes error.
- [x] Preserve empty advancing pages for terminal prefixes; cursor uses last examined ID and only a real identity lookahead. Document per-page consistency and cross-page race/missed-pre-cursor insertion semantics.
- [x] Run `cargo test --locked --offline -p b4-runtime --test unfinished_discovery`; require the actual admitted-record GREEN and all portable/API/shape negative cases. Preserve new source/test names/logs/hashes and precise old-file inverses.

## Task4 — SQLite GREEN and exact read-only preservation

- [x] Private SQLite helper takes the existing locked connection. Cursor mismatch happens before enumeration. Begin a deferred read transaction and check bounded exact metadata/schema/checksum/owner, without migration/repair/unbounded schema collection or modifying connection configuration.
- [x] Fixed identity-only SQL selects <=33 IDs after cursor in BINARY primary-key order, with raw UTF-8 byte/type preflight before owned typed IDs. Fetch full indexed rows only for the first32; use guarded CASE payload projections to prevent oversized text/blob results being owned. No arbitrary SQL or status filter.
- [x] Preflight indexed IDs,8-byte revision/owner, snapshot length<=1MiB and checked aggregate<=32MiB, thread generation64/active ID/resource revision before decoding/cloning. Borrow payload and call runtime discovery::backend::decode_snapshot after ValueRef/length preflight; then kernel validate, compare index, project, and drop Snapshot before next row. SQLite defines no serde probe or derive because it has no direct serde dependency; preserve both manifests and all dependency edges.
- [x] Validate existing thread and actual active-pointer membership plus foreign pointer rejection using fixed queries within the same transaction. No journal/grant payload decoder is needed and no integrity claim is added. Relationship scans have no hard VM/I/O bound; decoded results and allocations are bounded.
- [x] Commit the read transaction successfully before returning page. Map all internal SQL/serde errors to static DiscoveryError; neither paths/payloads nor grants appear in errors. No mutation/savepoint/recovery/owner increment is performed by this method.
- [x] Add actual32/33 row pagination, terminal-prefix empty page, wrong/corrupt indexed metadata, unsupported Snapshot, futureowner, missing/foreign thread, inactive hiddenunfinished and per-row/depth/node JSON budget tests plus a32-row page of per-row-cap snapshots. Do not invent a builtin aggregate-exceed test because32 ×1MiB is the exact32MiB cap. Private small-cap counting-helper N/N+1 and overflow tests may cover defensive aggregate refusal without public policy configurability or a real backend-cap claim. Use otherwise-valid per-row overbudget fixtures and preserve ordinary known-ID Store access; do not claim global write caps.
- [x] Compare all actual SQL table rows and typed views before/after calls including grant/acceptance/journal data in private evidence. Account owner increments separately as Store::open effects. Hold page/cursor after actual Store drop and prove Weak Store release + a real SqliteStore writer successor opens; old cursor refuses the successor.
- [x] Run `cargo test --locked --offline -p b4-sqlite --test unfinished_discovery`; then required existing runtime/sqlite tests. Freeze this author's source only after all focused checks pass.

## Task5 — independent portable/real recovery integration qualification

- [x] New-only qualifier files exercise both concrete backends and custom default UnsupportedOperation, trait object calls and existing Arc/& forwarding. Retain old underlying capabilities/deletion/cancellation behavior; add no new wrapper implementation or related tests. Do not mutate old tests.
- [x] Demonstrate current-owner descriptors do not authorize existing recovery. After actual SQLite close/reopen, use discovered identities only to fetch an ordinary ThreadView and build its exact RecoveryTarget; supply trusted verified principal separately.
- [x] Existing recover_execution of parked work preserves original grants/pending IDs; accepted Ready continuation completes without another approval; unsafe/incompatible/RecoveryBlocked work cannot be called automatically retryable. Check revoked policy, wrong origin/full Definition, stale resource/execution fences and successor thread do not adopt/dispatch.
- [x] Keep full authority rows and raw private fixtures private. Public results contain only test names, static classifications/counts and code/provenance; no token/DB digest/prompt/ID leak.
- [x] Run `cargo test --locked --offline -p b4-http --test unfinished_discovery_recovery`; no new provider/network/client or production HTTP change is required. Bounded test waits must distinguish cleanup fallback from positive callback/Store retirement; retain pages/cursors while positive successor reopen happens.
- [x] Root complete SPEC of whole API/Memory/SQLite implementation and all qualifier cases; different nonauthor complete QUALITY with fresh focused checks, adversarial probes and source preservation. Authors do not approve their own implementation.

## Task6 — final qualification, docs and external freeze

- [x] Run appropriate all-target runtime/sqlite/http tests and strict scoped clippy. Check formatting for exact selected/new files only during authorship; root full check after freeze. Preserve manifests, versions, package graph/features, old tests, schemas/migrations and protected artifacts.
- [x] Root fresh macOS/Linux platform qualification, actual close/reopen/ownership tests, Python/contract hook tests and exact declaration counts; qualify only current admitted sources. No stale historical count reuse or new full profile claim.
- [x] Root adds a separate scoped discovery requirement group and exact named acceptance mappings with meaningful trace-hook RED/GREEN. Leave old requirement entries and all broader M0/M2/M4/M5/core profile statuses open. Append README/compatibility and the new docs/runbooks/unfinished-discovery.md explanation of trusted identities, page limits, cursor/source and cross-page consistency, current-owner/uncertainty distinctions and no automatic readiness guarantee.
- [x] Root safe source-bound receipt records only actual qualification, private evidence bindings and explicit exclusions. Independent final public-support SPEC and different QUALITY precede narrow status bridge/external freeze. Avoid receipt self-hash cycles and public hashes of raw authority data.

## Required test inventory direction

Use exact final named declarations, not invented result claims: admitted Memory observation; real SQLite prior-owner observation; five unfinished statuses; all task uncertainties; terminal and hidden history; same-source current/prior/futureowner; cursor sameepochdifferentstore/reopen;32/33 scans; empty advancing page; exhausted page; actual per-row byte boundary and compliant full-page accounting; private small-cap aggregate counter/overflow boundary; depth/node boundary; serialization split escape equivalence; unsupported version; malformed indexed/thread/active/foreign data; coherent transaction; interpage mutation/stalefence; default customStore and wrappers; read-only rows; page/cursor resource release; separately authorized actual parked/Ready/blocked recovery integration.

No fixed test count is promised until declared tests exist. Setup/compile failures, synthetic-only faults and true OS/runtime qualification are classified separately. Implementation follows this accepted scoped design and plan after exact public adoption review.
