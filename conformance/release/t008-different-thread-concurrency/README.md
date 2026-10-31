# T-008: concurrent callbacks on distinct threads

This qualifies RFC T-008 within a native scope: two specific distinct threads physically execute callbacks concurrently within explicitly configured active-run and callback capacity two, using one immutable application and owned SQLite.

Two independent callback gates, trusted identities and inputs/results establish both callbacks entered and neither returned before either release. Actual held execution records show two Running executions with InFlight tasks. Both retained authenticated TCP requests then complete with independent expected results. Per-execution journal chains avoid assumptions about cross-thread ordering. Complete durable records remain unchanged across healthy retirement, physical owner generation+1 reacquisition and fresh-host reads without replay. Complete requests and lifecycle waits are bounded, with positive final owner destruction and directory removal.

```sh
cargo test -p b4-http --test different_thread_tcp \
  release_t008_different_threads_execute_concurrently \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named case, complete new target, seven predecessor native targets, formatting, scoped strict lint and metadata guards. All twelve checks passed on one frozen 1,143-file source: the named case and complete new target each reported one pass; the seven prior targets reported 6/1/1/1/1/1/1 passes, and all 61 metadata guards passed. Formatting and scoped strict lint passed, with no source drift, timeout or unreaped process. The [qualification receipt](qualification.json) binds actual source, commands, logs and process results. Prior scenarios retain their original receipts and historical descriptors; regressions do not qualify them again. Metadata fixtures, formatting and lint carry no native runtime credit. Generated proof follows frozen native equality; whole public metadata, twelve modules and final integrity follow separately.

T-001–T-008 have separate scoped native evidence; 48 scenarios remain pending. M0 remains open, CP-001–009 partial and CP-010 unresolved; core profile and TypeScript compatibility remain unqualified. Historical receipts retain their original counts and scope. No maximum capacity, overflow, performance, universal scheduling, cross-host, idempotency, TypeScript comparison, header, provider, full-workspace or graceful-listener claim is made. Platform is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
