# Bounded unfinished-execution discovery design

Status: accepted scoped implementation design, 2026-10-02. The completed cooperative-cancellation qualification and fresh source baseline are preserved. This read-only increment follows the user's continuous local Rust-build request, its implementation plan and independent reviews. Design acceptance does not mean implemented or qualified. This document supplies requirements and grants no external-action authority or broad milestone closure.

## Purpose and boundary

Add one synchronous read-only Store primitive that lets a trusted embedding discover recorded unfinished work without knowing execution IDs. It returns small typed observations and an opaque process-local continuation cursor. It does not adopt work, reconstruct policy, dispatch effects, consume approval decisions, change readiness, or interpret a discovery result as recovery authority.

Original specification section11.4 requires unfinished discovery before startup readiness and then revision/policy checks, parked-grant preservation, redispatch/reconciliation and transient-state rebuilding. This primitive advances only discovery. Requirements M2/M4 and compatibility192–199 keep automatic scanning/full restart qualification open. Existing HttpHost::recover_execution remains a separately authorized known-ID operation with principal/origin/policy renewal, full immutable Definition equality and atomic thread/execution/resource fences.

The original attachment provides requirements, not executable agent instructions or permission to run its embedded commands. No original clients, provider, network service, new dependency, migration, public AP route, HTTP production, kernel or driver behavior is needed.

## Approaches considered

1. **Recommended: optional Store discovery pages with Memory and SQLite semantics.** Embeddings can enumerate observations and independently retrieve/authorize an ordinary resource for existing recovery. Default UnsupportedOperation preserves custom implementations. This is a useful, bounded prerequisite without committing to an automatic startup coordinator.
2. SQLite-only raw enumeration would be smaller initially but would hide the backend contract and not support portable embedding/tests. Reject this approach.
3. An automatic startup recovery coordinator would satisfy more section11.4 behavior, but requires persisted actor resolution, current policy reconstruction, route/Definition selection, retry/reconciliation and readiness orchestration. Defer it.

Persistent exact-tool Always grants and native chunk publication remain separate alternatives described in the scout; neither belongs in this primitive.

## Consumer API

Add a public runtime `discovery` module and re-export its consumer types at the runtime root. Proposed exact names:

```rust
pub const DISCOVERY_SCANNED_ROWS: usize = 32;
pub const DISCOVERY_SNAPSHOT_BYTES: usize = 1_048_576;
pub const DISCOVERY_TOTAL_SNAPSHOT_BYTES: usize = 33_554_432;
pub const DISCOVERY_JSON_DEPTH: usize = 64;
pub const DISCOVERY_JSON_NODES: usize = 16_384;

pub enum DiscoveryOwnerRelation { Current, Prior }

pub struct UnfinishedExecutionRef {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub execution_fence: ExecutionFence,
    pub resource_revision: u64,
    pub route: RouteKey,
    pub definition_revision: ExecutionRevision,
    pub status: ExecutionStatus,
    pub owner_relation: DiscoveryOwnerRelation,
    pub has_in_flight_task: bool,
    pub has_outcome_unknown_task: bool,
}

pub struct UnfinishedDiscoveryPage {
    pub owner_generation: u64,
    pub scanned_rows: usize,
    pub records: Vec<UnfinishedExecutionRef>,
    pub next_cursor: Option<UnfinishedDiscoveryCursor>,
}

// Clone; private fields; no Serialize/Deserialize, pointer getter,
// public constructor or persisted/source-path representation.
// No special Debug contract or custom formatting machinery is required.
pub struct UnfinishedDiscoveryCursor { /* origin marker, owner, last ID */ }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    UnsupportedOperation,
    CursorMismatch,
    LimitExceeded,
    UnsupportedSnapshot,
    UnsupportedSchema,
    OwnerConflict,
    CorruptRecord,
    Unavailable,
}

pub trait Store {
    // existing methods are retained exactly
    fn discover_unfinished(
        &self,
        cursor: Option<&UnfinishedDiscoveryCursor>,
    ) -> Result<UnfinishedDiscoveryPage, DiscoveryError> {
        Err(DiscoveryError::UnsupportedOperation)
    }
}
```

DiscoveryError has static Display strings and Error implementation only; it never embeds a rejected identity, JSON, SQL/serde message, path or grant. There is no new CLI exit/error contract. Status/relations are observations; `Prior` does not mean safe to retry or authorized. DTOs contain no Snapshot, task inputs/results, user metadata, origin/actor, prompt, token, grant, approval acceptance, authorization decision, timestamp, source handle or application callback. IDs/fences are sensitive trusted embedding data, unlike the aggregate stopped-inspection report; callers must not log or publish them automatically.

Existing Store forwarding implementations are Arc<S> and &S. Add this method only to both existing forwarders, preserving their complete old method bodies. No new wrapper implementation, Box forwarding, boxed module, or related tests belongs in this increment.

## Source-local cursor identity

Each built-in backend owns a unique nonclone `DiscoveryOrigin`, whose private representation is Arc<()>. Default constructs a fresh marker without entropy, dependencies, paths or fallible OS I/O. MemoryStore retains its current infallible new/default API; SqliteStore constructs the marker with the owned store. Moving the backend preserves marker identity.

The cursor retains only a clone of this marker, the current owner generation and the last fully examined ExecutionId. Arc::ptr_eq validates origin, not Arc value equality. A marker remains alive through cursors, so a freed allocation cannot be reused while a cursor exists. A cursor from another MemoryStore with owner1, another SQLite source at the same epoch, or a reopened instance fails CursorMismatch before enumeration. A cursor is not a Store/connection/owner-lock/application/host reference. Drop of the Store releases physical ownership while pages/cursors remain alive.

Use a narrow backend helper API within `b4_runtime::discovery::backend`: `DiscoveryOrigin::default()` and the exact borrowed cursor helper below. The output borrows the input cursor, not the backend origin; no reference escapes from a Store lock/transaction. Check source-marker identity and captured owner against the current owned backend before borrowing the last ID. Owner zero fails OwnerConflict. `cursor(current_owner, last_id: &ExecutionId) -> UnfinishedDiscoveryCursor` clones only the bounded last ID and resource-free marker. Helpers cannot recover a builtin backend's private marker. A custom backend can maintain its own origin and use the same contract, or retain the default refusal.

```rust,ignore
impl DiscoveryOrigin {
    pub fn checked_after<'cursor>(
        &self,
        current_owner: u64,
        cursor: Option<&'cursor UnfinishedDiscoveryCursor>,
    ) -> Result<Option<&'cursor ExecutionId>, DiscoveryError>;
}
```

## Page consistency and pagination

Each call holds one Memory mutex or one SQLite connection mutex plus a deferred read transaction through its last check. SQLite verifies bounded metadata: exactly one id1 row, current schema version3/checksum, nonzero eight-byte owner equal to the owned backend. Discovery does not increment the owner, migrate, repair or call unbounded Store startup/schema reconstruction. All source reads finish before the page escapes the method.

Order rows by ExecutionId, using Rust BTreeMap order and SQLite BINARY text order. Valid portable identities are UTF-8 with no controls and at most256 bytes; these orderings compare their UTF-8 bytes consistently. Start strictly after the cursor ID. Examine at most32 full execution rows. A separate identity-only fetch of at most33 IDs supplies at most one lookahead; the extra row's snapshot is not fetched/decoded or counted as an examined row. Preflight each selected/lookahead identity's byte length and type before allocating it.

`scanned_rows` counts all fully examined rows, including terminal history, not records returned. `next_cursor` is present precisely when the same coherent page read saw a valid lookahead ID. It contains the last examined ID, not the lookahead. An all-terminal prefix can produce records=[] and an advancing cursor. Zero rows yields scanned_rows0, records[] and no cursor. No caller-selectable cap or SQL is exposed. If any selected row fails or a cumulative limit is exceeded, return only an error, never a partial page/cursor.

Across pages there is deliberately no global snapshot: executions can change or disappear, and an admission with an ID <= the last cursor can be missed by that scan. End-of-page exhaustion says only that this page observed no later row. It does not certify quiescence, full Store integrity, restart completion or readiness. Newer owner/source invalidates continuation. Embeddings may start a new scan or separately fence/authorize a captured candidate.

## Validation and classification

Validate every fully selected snapshot before using execution status to discard terminal history. Check its supported version, kernel validity, map/SQL execution ID, indexed revision and owner agreement, nonzero owner <= current owner, and owning thread existence. Check the owning thread's typed ID, immutable generation64 lowercase hex, resource revision eight-byte integer, bounded optional active ID, and actual active execution-to-thread relationship. Reject any foreign thread that points at the selected execution, including a selected historical terminal execution.

Compute both InFlight and OutcomeUnknown presence from validated task records. The selection is nonterminal execution status OR recorded task uncertainty; it is not a SQL status-only filter. Existing kernel validation rejects terminal snapshots with active/unknown tasks in its relevant state branches. Such input is CorruptRecord, never a resumable target. The stopped-inspection `recorded_unfinished_or_uncertain` selector is not recovery eligibility.

A selected unfinished/uncertain record must be the exact active execution of its owning thread; hidden inactive unfinished history fails CorruptRecord. Terminal nonactive history is still validated but is not returned. Active current-owner unfinished work is returned with Current. Active strictly older unfinished work is returned with Prior. Future-owner data fails OwnerConflict even if terminal. RecoveryBlocked and unsafe/unknown tasks remain observations; this call never retries them. Full recorded Definition matching, actor reconstruction and approval/grant semantics remain the existing recovery service's responsibility. Discovery does not audit journal/grant/acceptance payload semantics or claim whole-store validity.

Relationship checks may require a scan of other thread pointers: active_execution_id has no existing index. Memory may scan its borrowed thread map and SQLite may use a fixed EXISTS query. This design bounds selected snapshots/results/allocation and JSON traversal; it does not bound all database VM work or physical I/O. No schema/index change is introduced and no hard CPU/I/O/RSS/deadline promise is made.

## Read limits and decoding

The 256-byte identity limit and generation64 syntax are existing b4-contract rules. The 1MiB per-snapshot and JSON depth64/nodes16384 values reuse stopped-inspection policy as new discovery read limits. The32-row page limit is a new deliberate limit; aggregate32MiB is exactly32 rows ×1MiB, rather than inspection's separate16MiB whole-source budget. Therefore a page of32 otherwise-valid snapshots within each per-row byte limit cannot fail only for aggregate bytes and leave an unadvanceable cursor. Keep checked defensive aggregate accounting at32MiB; no caller configurability is exposed. Existing kernel limits1024 batches/4096 task specifications remain required. No old Store write/acceptance limit is tightened: a otherwise-valid large stored Snapshot can remain accessible through its known-ID API while discovery refuses LimitExceeded. Never call a bounded-read limit violation corruption.

Use new common runtime backend validation/budget helpers rather than editing protected stopped-inspection modules. The actual protected inspection shape guard is in crates/b4-sqlite/src/inspection/validation.rs with validation/tests.rs; no inspection/shape.rs exists. Any new discovery/shape.rs is a new runtime module. The shared backend helper projects borrowed selected data into the small descriptor. Backend thread input includes only typed ID/generation/active ID/resource revision; it does not require decoding ThreadRecord metadata/origin. SQLite preflights text/blob byte length with guarded CASE projections before owned ID or Snapshot decoding: length(CAST(text AS BLOB)), not Unicode character length; oversized payload must not be retrieved as String. Revision/owner BLOBs must be exactly8 bytes; generation must be exactly64; IDs <=MAX_ID_BYTES. Use borrowed rusqlite ValueRef to check type/length before cloning bounded fields.

The new shared runtime discovery::backend owns `pub fn decode_snapshot(payload: &[u8]) -> Result<Snapshot, DiscoveryError>`, which performs the per-row byte/shape checks, bounded u64/IgnoredAny schema-version probe and direct typed Snapshot decoding. b4-runtime already has direct serde and serde_json dependencies. SQLite has serde_json but no direct serde dependency: after SQL length/type and ValueRef preflight it calls this shared helper, never defines or derives a serde probe locally. Kernel validation and common indexed/thread projection follow typed decoding; Memory measures its borrowed typed Snapshot before kernel validation/common projection. No manifest, lockfile or dependency edge changes are needed.

SQL raw JSON is shape-preflighted using a fixed64-level stack and checked counters before any recursive decoder. Count strings INCLUDING object keys, container starts and primitive tokens. Then deserialize a bounded `schema_version: u64` probe using IgnoredAny for other fields, not Value; unsupported numeric version yields UnsupportedSnapshot. Malformed/missing/duplicate version or malformed JSON yields CorruptRecord. Decode Snapshot directly and run kernel validation. The probe handles escaped root keys and avoids classifying formatted serde errors. No intermediate generic Value, collected execution vector, whole-schema/journal scan or unbounded read_execution helper is used.

For Memory, the Snapshot is already typed but arbitrary Values are not depth-bounded by kernel validation. Serialize the borrowed Snapshot with serde_json::to_writer directly into a fixed-state counting/lexical writer, with no Vec/String JSON buffer, BufWriter or other buffering adapter. Opening delimiters must reach the lexical guard before child serialization; buffering them would defeat the early depth refusal. Its streaming lexical guard tracks quote/escape/primitive/container state across write boundaries and stops before the65th container opening, node16385 or byte1,048,577. The locked serde_json1.0.138 serializer attempts a container opening before recursively visiting its children. Rejecting opening65 can enter a constant65th serializer frame but stops before visiting its children; it is not a claim that no65th frame exists. Its escaped-string formatter uses borrowed fragments and at most6-byte escape scratch, not a full escaped allocation. It can scan an entire unescaped resident string before passing its borrowed fragment to the writer, so the1MiB guard bounds emitted/accounted bytes, not all characters scanned or CPU work. No hard CPU/RSS/I/O guarantee is introduced. A positive own refusal flag maps the serializer's I/O failure to LimitExceeded; other serialization failure maps to static CorruptRecord. Check final lexical state and count exact compact serde_json wire bytes. Successful per-row byte counts consume the shared32MiB page budget using checked arithmetic. Do not clone the borrowed full Snapshot or build temporary children vectors. Drop each SQLite decoded Snapshot before the next row.

Private helper tests must compare streaming accounting at every chunk boundary (including escaped quotes/backslashes, Unicode and brackets in strings) with raw-byte accounting and serde_json::to_vec ONLY in test fixtures. Memory and SQLite enforce identical serialized-byte/depth/node rules, subject to SQLite's persisted raw spelling: whitespace/escapes count as actual persisted bytes. Equivalent data with longer valid raw spelling can hit SQLite's read budget; this is honest persisted-byte accounting, not an exact canonical-byte promise.

## Preservation and validation

Preserve all old method/test bodies, schemas/migrations, manifests, Cargo.lock, contract/kernel/driver/AP production and stopped-inspection source. Only runtime lib/store/memory and SQLite lib need narrow existing-file changes; new modules/tests carry implementation. No capability declaration is added: capability changes are a separate compatibility choice.

TDD requires compiling consumer/helper stubs first, then real behavior REDs with successful fixture setup. Portable Memory RED returns UnsupportedOperation instead of one real admitted observation. Real SQLite RED closes an original owner, reopens the source, then fails to discover a persisted prior-owner record with exact captured fences. Preserve failures/logs/source hashes before implementation; compilation/setup failures do not count.

Qualify status/uncertainty selection, relationship corruptions, current/prior/future owners, exact scopes/order, terminal-prefix empty advancing pages,32/33 scans/lookahead, actual per-row byte N/N+1 boundaries, depth64/65 and node16384/16385, stream escape boundaries and no partial output. Include otherwise-valid overbudget snapshots and show ordinary Store access remains unchanged. Test a full32-row page at the per-row cap to prove aggregate accounting does not stall progress. No actual builtin page can exceed32MiB while also obeying32 scanned rows and1MiB each, so do not invent an impossible aggregate-exceed backend fixture. A private counting-helper seam may use a small total cap to test exact aggregate N/N+1 refusal and checked overflow independently; classify this as helper evidence, not a real production-cap refusal or public policy configurability. Existing custom Store implementations still compile/default UnsupportedOperation; only existing Arc/& wrappers forward discovery unchanged.

Read-only preservation compares complete actual thread/execution/journal/grant/acceptance rows before/after discovery, without public hashes of authority data. Actual Store close/reopen tests distinguish owner changes caused by opening a store from discovery itself. Retain page/cursor after Store drop; use Weak witness and a real SqliteStore successor writer reopen, then show the old cursor rejects the successor.

A new HTTP test-only integration file may take discovered exact identities into an ordinary coherent view and separately construct RecoveryTarget. Exercise the existing trusted recovery with real SQLite parks and accepted Ready work, immutable Definition, renewed policy/principal, stale-fence/successor/refused unsafe cases and original grants/pending IDs unchanged. No production HTTP edits or new dependency are needed; b4-http already has b4-sqlite as a dev dependency. Do not use discovery descriptors as an authorization shortcut.

Root performs independent SPEC, a different nonauthor QUALITY, required fresh Mac/Linux qualification and external freeze. Public scope reports discovery only; original11.4 startup/readiness, M2/M4, crash matrix, policy reconstruction, persistent Always, native chunks, CP010 and full core-profile release stay open.
