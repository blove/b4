# HTTP settled permission-thread deletion qualification

The explicit `HttpHost::new_with_permission_settled_thread_deletion` assembly exposes the adopted settled-history writer through AP-003. See the [runbook](../../../docs/runbooks/http-permission-settled-thread-deletion.md) and [machine-readable record](qualification.json). Existing constructors retain their behavior and contracts.

On macOS arm64 with Rust 1.93.1, the exact 1,074-file native descriptor was `248ce71bb54cfc9fcbdb6906429242cb4610a8552bca791a81263a615bb87d08`. Every recorded check used this unchanged before/after map, locked offline dependencies, finite limits and confirmed process reaping. Runtime, SQLite, earlier contract/schema/example bytes, dependencies and frozen expectation files remain unchanged.

| Check | Result |
| --- | --- |
| New V8 contract |4 passed|
| New HTTP deletion |5 business cases and 2 gate helper checks passed|
| Existing embedded deletion |4 passed|
| Existing V7 contract |6 passed|
| Existing withheld cancellation |7 passed|
| Existing generic permission guards |9 passed|
| Existing permission records |89 passed|
| Formatting, strict workspace all-target lint, compile |Passed|
| Full workspace |1,662 reported passes;0 failures;11 existing ignored tests|
| Metadata |Passed separately; no runtime execution credit|

The full run completed in 776.772 seconds without timeout or source changes. Its log SHA-256 is `b422f1d26f5181af7778121ed3546c8c5d0d6d71911c4331c8d70399df14d860`. Counts derive from 153 actual summary executions, including child/helper executions; they are not a unique test inventory. Separate metadata validation checked 26 schemas, 27 authored examples, 2,134 rejection cases and 230 local references; schema validation does not establish runtime behavior or full protocol conformance.

The fixtures use normal identifiers, month-only input, empty thread metadata and bounded nontrivial outputs matching the established producer profiles. They do not qualify arbitrary larger structured inputs or maximal combinations.

The five HTTP business cases exercise actual requests, real Drivers and the adopted SQLite deletion writer:

- Genuine ordinary Once, original Always and two saved Uses populate all eight owned tables. Normal owner reopen preserves historical execution ownership; actual deletion preserves metadata and another thread's bytes. Current effect-constraint denial does not block separately authorized historical destruction. The generic Store guard still refuses permission-bearing deletion.
- Original cancellation acknowledgement and a saved Use's retained late success reach actual durable terminal commits. A gate inside each real worker demonstrates that deletion still returns 409 until that worker releases its claim and cleanup finishes. A saved terminal state alone provides no drain proof.
- Current initial policy, origin privacy, dedicated SDK authority renewal, a stale captured view, default-false durability, coherent no-active/nonterminal states and the existing V7 assembly refuse as specified. Genuine mixed sixteen-execution success and seventeen-execution limit refusal exercise the practical row ceiling while preserving healthy readiness and all durable rows on refusal.
- A guarded final thread DELETE trigger is installed after normal readiness. Its predicate requires active-pointer clearing and all dependent removals before the actual writer fails. Static 503, the real final-delete error code, exactly one attempt, complete rollback across nine tables, fault/drain, exact trigger removal and fresh normal-owner deletion establish this concrete failure result. This does not generalize uncertain commit outcomes into rollback guarantees.
- A real caller disconnects while the delegated writer is paused. Thread exclusion, mutation admission, Store ownership and registered assembly objects remain held after admission closes and external references drop. Actual SQL then commits, ownership remains until cleanup is released, and normal owner reopen observes absence. That later observation never manufactures an abandoned response's committed confirmation.

Four strict V8 contract tests independently derive the new wire value from frozen V7 metadata. They cover preserved valid boundary fields, required true narrow declaration, inherited validation, missing/false/type/duplicate/unknown rejection and invalid public assemblies without repair. Older contracts and examples remain byte-identical. V8 advertises only `durable_permission_settled_thread_deletion`; broad `fenced_deletion`, general Always and recovery declarations remain false.

The explicit HTTP constructor selects the dedicated durable Store mechanism. Authentication, application scope and current resource policy retain normal privacy. The existing owned worker and mutation lease survive caller abandonment. A present-resource204/no-store requires the actual opaque SDK confirmation matching the exact target. Authorized initial absence retains the existing idempotent204 semantics, which is separate from a committed confirmation. Changed or disappeared captured targets return409; no generic fallback, storage retry or callback replay occurs.

Fixed inherited SDK limits are 16 executions, 64 rows per record table, 512 journal rows, 64 MiB cumulative charged evidence/work and 65,536 nodes, plus existing record, snapshot and depth caps. All maxima need not compose. Unknown foreign creator representations conservatively refuse. The operation does not establish global database SQL-time or end-to-end HTTP allocation bounds. Embeddings remain responsible for coordinating unrelated direct SDK users under the embedded ownership contract.

This qualification covers the explicit settled-history HTTP subset on the recorded platform. It excludes live work retirement, forced corrupt-data deletion, recovery, automatic callback replay, maximum-size combinations and other platforms. Existing runnable example modes are unchanged in this increment. Tests use owned fixtures and do not delete actual user data.
