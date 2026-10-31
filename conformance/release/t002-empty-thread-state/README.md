# T-002: empty threads have no fabricated checkpoint

This qualifies RFC T-002 within a native scope: a newly created thread has no state checkpoint, including after the owned SQLite database is reopened through a fresh HTTP host.

The independently authored test sends real authenticated loopback TCP requests. Repeated GET state requests must return 404 with the complete independently declared checkpoint_unavailable error envelope. Ordinary thread reads remain 200/idle with the same exact body. Full durable thread/view and raw thread/journal rows remain unchanged, execution rows stay empty, and the counted application callback stays zero. Both hosts positively reach healthy quiescence before the fixture listener is aborted/joined. Actual owner release/reacquisition advances the generation exactly once; final sole-store destruction and temporary-directory removal are asserted.

```sh
cargo test -p b4-http --test thread_state_tcp \
  release_t002_empty_thread_state_stays_unavailable_after_reopen \
  --locked --offline -- --test-threads=1
```

Final source-bound checks are the named scenario, complete thread_state_tcp target, fresh complete native_tcp T-001 regression, formatting, scoped strict lint and focused metadata guards. Actual results: the named scenario passed (1/0/0), the complete state target passed (1/0/0), and the existing native TCP target passed (6/0/0). Formatting and strict scoped lint passed; all 19 metadata guards passed. All six checks used the same 1,113-file frozen source. The [qualification receipt](qualification.json) records exact source, commands, logs and process results. The regression preserves T-001's original accepted receipt and historical source descriptor; it does not close T-001 again. Authored metadata fixtures, formatting and lint carry no native runtime credit. The generated receipt is delivered after frozen source equality; whole public metadata, twelve traceability modules and final integrity follow separately.

T-001 and T-002 now have separately scoped NATIVE evidence; the other 54 scenarios remain pending. M0 remains open, CP-001–009 partial, CP-010 unresolved; full core profile and TypeScript compatibility remain unqualified. Historical foundation receipts retain original counts and scope. This is not a new full-workspace run, graceful-listener shutdown qualification, provider test or maximum-input guarantee. Scope is pinned Rust 1.93.1 on local macOS arm64 with owned SQLite and loopback TCP.
