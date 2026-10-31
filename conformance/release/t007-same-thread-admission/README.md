# T-007: one winner for overlapping same-thread admissions

This qualifies RFC T-007 within a native scope: a real callback remains held while a second same-thread admission receives the complete independent 409 thread_busy AP error. Capacity two allows the loser to reach thread exclusion rather than a global-capacity refusal.

The independently authored test retains the connected winner request, positively observes callback entry and captures its Running/InFlight durable state. It fully observes the loser response before releasing the callback gate, then requires unchanged held records/journal, one validator/callback, and zero callback returns. Only afterward does it release and finish the winner with exact direct output and one completed execution/task/attempt. Task/HTTP output is checked separately from aggregate snapshot output. Actual physical owner release/reopen and fresh-host reads preserve completion without replay. Whole requests/waits are bounded, healthy quiescence precedes listener abort/join, and final sole-store destruction/directory removal are asserted.

```sh
cargo test -p b4-http --test thread_concurrency_tcp \
  release_t007_same_thread_overlap_has_one_winner \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named case, complete new target, six prior native targets, formatting, scoped strict lint and metadata guards. All eleven checks passed on one frozen 1,138-file source: the named case and complete new target each reported one pass; the six prior targets reported 6/1/1/1/1/1 passes, and all 54 metadata guards passed. Formatting and scoped strict lint passed, with no source drift, timeout or unreaped process. The [qualification receipt](qualification.json) records actual source, commands, logs and process results. Prior scenarios retain original receipts and historical descriptors; regressions do not qualify them again. Metadata fixtures, formatting and lint carry no native runtime credit. Generated proof is delivered after frozen native equality; whole public metadata, twelve modules and final integrity follow separately.

T-001–T-007 have separate scoped native evidence; the other 49 scenarios remain pending. M0 remains open, CP-001–009 partial and CP-010 unresolved; core profile and TypeScript compatibility remain unqualified. Historical receipts retain original counts and scope. This is one controlled overlap, not every interleaving, cross-host exclusion, global capacity, disconnect behavior or run idempotency. No full-workspace, provider, header, graceful-listener shutdown or maximum-input guarantee is claimed. Platform is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
