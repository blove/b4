# T-004: reject unknown and missing-kind routes without execution

This qualifies RFC T-004 within a native scope: an unknown valid-kind route returns the complete 404 resource_unavailable AP error, while a route missing its required kind suffix returns the complete 400 invalid_request AP error. Both requests leave the selected absent thread, executions and journal absent.

The independently authored test uses authenticated loopback TCP, a genuine immutable native application and owned SQLite. It checks both full independent error envelopes, zero validator/callback calls and zero durable admission state before and after each request. A fresh host repeats the cases after actual physical owner release/reacquisition advances generation once. Healthy host quiescence precedes listener abort/join; all complete requests and waits are bounded. Final sole-store destruction and temporary-directory removal are asserted.

```sh
cargo test -p b4-http --test thread_route_tcp \
  release_t004_unknown_and_missing_kind_routes_do_not_execute \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named case, complete new target, all three prior native targets, formatting, scoped strict lint and metadata guards. Actual results: named case 1/0/0, full route target 1/0/0, native TCP regression 6/0/0, state regression 1/0/0 and admission regression 1/0/0. Formatting and strict scoped lint passed; all 33 metadata guards passed. All eight checks used the same 1,123-file frozen source. The [qualification receipt](qualification.json) records actual source, commands, logs and process results. Prior scenarios retain their original receipts and historical descriptors; regressions do not qualify them again. Metadata fixtures, formatting and lint carry no native runtime credit. Generated proof is delivered after exact frozen native source equality; whole public metadata, twelve traceability modules and final integrity follow separately.

T-001–T-004 have separate scoped native evidence; the other 52 scenarios remain pending. M0 remains open, CP-001–009 partial and CP-010 unresolved; core profile and TypeScript compatibility remain unqualified. Historical receipts retain original counts and scope. This covers only these two route failures, without a parser or authorization matrix. No full-workspace, provider, header, graceful-listener shutdown or maximum-input guarantee is claimed. Platform is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
