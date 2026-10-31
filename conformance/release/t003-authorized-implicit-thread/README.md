# T-003: authorize before implicit thread creation

This qualifies RFC T-003 within a native scope: an initially denied run leaves its selected absent thread and all execution state absent. Allowing the same request then creates and completes one genuine native run.

The independently authored test sends authenticated loopback TCP requests to an immutable native application backed by owned SQLite. It requires the complete independently declared resource_unavailable error for initial denial, with zero validator/callback calls and zero thread/execution/journal rows. The allowed request uses the same selected ID and input; it requires the exact expected output, one validator and callback, one native-origin thread with empty metadata and one completed execution. Durable input/output, identities and complete captured journal remain unchanged across healthy host retirement and physical SQLite owner release/reopen. The fresh host does not replay the callback. Complete requests and waits are bounded; final sole-store destruction and directory removal are asserted.

```sh
cargo test -p b4-http --test thread_admission_tcp \
  release_t003_implicit_thread_creation_requires_authorization \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named case, complete new target, fresh complete T-001 and T-002 native targets, formatting, scoped strict lint and metadata guards. Actual results: named case 1/0/0, full admission target 1/0/0, native TCP regression 6/0/0 and state-target regression 1/0/0. Formatting and strict scoped lint passed; all 26 metadata guards passed. All seven checks used the same 1,118-file frozen source. The [qualification receipt](qualification.json) records actual source, commands, logs and process results. Prior scenarios retain their original receipt bytes and historical source descriptors; these fresh regressions do not qualify them again. Authored metadata fixtures, formatting and lint carry no native runtime credit. Generated proof is delivered after exact frozen native source equality; whole public metadata, twelve traceability modules and final integrity follow separately.

T-001–T-003 have separate scoped native evidence; the other 53 scenarios remain pending. M0 remains open, CP-001–009 partial and CP-010 unresolved; core profile and TypeScript compatibility remain unqualified. Historical foundation receipts retain original counts and scope. This qualifies initial admission authorization, not every possible later authority revocation. No new full-workspace, provider, header, graceful-listener shutdown or maximum-input guarantee is claimed. Platform is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
