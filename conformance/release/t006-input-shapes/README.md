# T-006: preserve input shape semantics

This qualifies RFC T-006 within a native scope: omitted input becomes `{}`, explicit null remains null, and representative scalar false remains false through native validation, callback invocation, direct HTTP output and durable execution state.

The independently authored test sends three sequential authenticated TCP requests on independent thread IDs to one genuine immutable native application backed by owned SQLite. Independently declared expectations and explicit key-presence checks distinguish absent keys from null and preserve false. Callback/task output is checked separately from aggregate Snapshot.output. Actual completed records and journal remain stable through healthy retirement, physical SQLite owner release/reacquisition and fresh-host reads, without validator or callback replay. Complete requests and waits are bounded; final sole-store destruction and directory removal are asserted.

```sh
cargo test -p b4-http --test input_shape_tcp \
  release_t006_omitted_null_and_false_inputs_round_trip \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named case, complete new target, five prior native targets, formatting, scoped strict lint and metadata guards. All ten checks passed on one frozen 1,133-file source: the named case and complete new target each reported one pass; the five prior targets reported 6/1/1/1/1 passes, and all 47 metadata guards passed. Formatting and scoped strict lint passed, with no source drift, timeout or unreaped process. The [qualification receipt](qualification.json) records actual source, commands, logs and process results. Prior scenarios retain original receipts and historical descriptors; regressions do not qualify them again. Authored metadata fixtures, formatting and lint carry no native runtime credit. Generated proof is delivered after frozen native source equality; whole public metadata, twelve modules and final integrity follow separately.

T-001–T-006 have separate scoped native evidence; the other 50 scenarios remain pending. M0 remains open, CP-001–009 partial and CP-010 unresolved; core profile and TypeScript compatibility remain unqualified. Historical receipts retain original counts and scope. This covers the three specified input forms, without all scalar types or size limits. No TypeScript runtime comparison, full-workspace, provider, header, graceful-listener shutdown or maximum-input guarantee is claimed. Platform is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
