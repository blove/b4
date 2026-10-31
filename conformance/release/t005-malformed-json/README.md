# T-005: malformed JSON receives an AP error

This qualifies RFC T-005 within a native scope: a valid authenticated HTTP run/wait request with raw malformed JSON body `{` receives the complete independently declared 400 invalid_json AP error envelope.

The independently authored test registers a genuine immutable native application backed by owned SQLite. It requires zero native validator/callback calls and no thread, execution or journal rows before and after rejection. A fresh host repeats the same single syntax case after actual physical owner release/reacquisition advances generation once. Healthy host quiescence precedes listener abort/join; complete requests and lifecycle waits are bounded. Final sole-store destruction and temporary-directory removal are asserted.

```sh
cargo test -p b4-http --test thread_json_tcp \
  release_t005_malformed_json_uses_ap_error_without_execution \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named case, complete new target, four prior native targets, formatting, scoped strict lint and metadata guards. Actual results: named case 1/0/0, full JSON target 1/0/0, native TCP regression 6/0/0, and each state, admission and route regression 1/0/0. Formatting and strict scoped lint passed; all 40 metadata guards passed. All nine checks used the same 1,128-file frozen source. The [qualification receipt](qualification.json) records actual source, commands, logs and process results. Prior scenarios retain original receipts and historical descriptors; regressions do not qualify them again. Authored metadata fixtures, formatting and lint carry no native runtime credit. Generated proof is delivered after frozen native source equality; whole public metadata, twelve traceability modules and final integrity follow separately.

T-001–T-005 have separate scoped native evidence; the other 51 scenarios remain pending. M0 remains open, CP-001–009 partial and CP-010 unresolved; core profile and TypeScript compatibility remain unqualified. Historical receipts retain original counts and scope. This is one representative malformed JSON syntax case, without a parser, transport, authorization, header or size-limit matrix. No full-workspace, provider, graceful-listener shutdown or maximum-input guarantee is claimed. Platform is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
