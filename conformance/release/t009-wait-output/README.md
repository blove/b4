# T-009: direct wait output and successful Void null

This qualifies RFC T-009 within a native scope: a successful structured Value output is the direct wait response, and an explicitly registered successful Void result is literal JSON null.

Two sequential authenticated TCP requests use independently selected thread IDs, separate explicit Value/Void registrations in one immutable application, and owned SQLite. Independent expectations distinguish direct task results from durable aggregate input state. Both cases require real completed executions and succeeded tasks, exact inputs and trusted identities, one callback each, full committed records/journal, healthy retirement and physical owner generation+1 reacquisition. Fresh-host GET reads preserve durable results without replay. Complete requests and lifecycle waits are bounded, with final sole-store destruction and directory removal.

```sh
cargo test -p b4-http --test wait_output_tcp \
  release_t009_wait_returns_direct_value_and_void_null \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named case, complete new target, eight predecessor native targets, formatting, scoped strict lint and metadata guards. All thirteen checks passed on one frozen 1,148-file source: the named case and complete new target each reported one pass; the eight prior targets reported 6/1/1/1/1/1/1/1 passes, and all 68 metadata guards passed. Formatting and scoped strict lint passed, with no source drift, timeout or unreaped process. The [qualification receipt](qualification.json) binds actual source, commands, logs and process results. Predecessor scenarios retain original receipts and descriptors; regressions do not qualify them again. Metadata fixtures, formatting and lint carry no native runtime credit. Generated proof follows frozen native equality; whole public metadata, twelve modules and final integrity follow separately.

T-001–T-009 have separate scoped native evidence; 47 scenarios remain pending. M0 remains open, CP-001–009 partial and CP-010 unresolved; core profile and TypeScript compatibility remain unqualified. Historical receipts retain original counts and scope. This covers one structured Value and one explicit Void result, with no streaming-framing, response-replay, output-size, concurrency, TypeScript comparison, header, provider, full-workspace or graceful-listener claim. Platform is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
