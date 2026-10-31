# T-001: native thread create/read round trip

This qualifies one RFC acceptance scenario within a native scope: create/read a thread, preserve nested metadata and stored timestamps, retire the SQLite owner and read the same thread through a fresh native HTTP host after reopening.

The independently authored test sends real authenticated loopback TCP requests. It requires the exact five-field thread response and independently declared Unicode/null/array/boolean metadata. It bounds the stored creation instant by the request clock and independently converts that stored epoch to RFC3339. Repeated reads and the reopened host must return the same response. The durable thread origin, incarnation, revision and creation journal remain unchanged while the owner generation advances exactly once. No execution or callback is admitted. Host quiescence, joined listener teardown, released ownership and temporary-directory removal are checked.

```sh
cargo test -p b4-http --test native_tcp \
  release_t001_create_read_thread_round_trips_metadata_and_timestamps_after_reopen \
  --locked --offline -- --test-threads=1
```

The final source-bound checks are the named scenario, complete native TCP target, formatting, scoped strict lint and focused metadata guard tests. Actual final results: the named scenario passed (1/0/0), the complete native TCP target passed (6/0/0), formatting and strict scoped lint passed, and all 12 focused metadata guards passed. All five checks used the same 1,108-file frozen source. The [qualification receipt](qualification.json) records exact source, commands, logs and process results. Authored metadata fixtures, formatting and lint carry no native runtime credit. The generated receipt is delivered after frozen source equality; the whole public contract-metadata check, all twelve traceability modules and final integrity are recorded separately.

Only T-001 receives `NATIVE` / `passed_native_scope`. The other 55 scenarios remain pending. M0 remains open, CP-001–009 partial, CP-010 unresolved, and the full core profile and TypeScript compatibility remain unqualified. Historical foundation receipts keep their original counts and scope. This is not a new full-workspace run, graceful-shutdown qualification, provider test or maximum-input guarantee. Scope is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
