# Runnable HTTP Always example

`serve_permission_reuse serve-approvals` starts the opt-in HTTP permission host without embedded seeding. Its fresh process check creates a real HTTP thread, runs to Waiting2, submits the actual pending token with Always and observes original Complete5, a committed task result and no original Use. After normal SIGINT shutdown and owner-lock/listener retirement, a second process owner completes a fresh run through a real linked Use at revision4 without replacement approval.

The checker verifies SQL/body provenance, original immutable records/history, owner and execution identities, expected cardinalities, integrity and final temporary cleanup. It has bounded process/HTTP/output waits and suppresses sensitive output. It observes committed attempts/results rather than instrumenting callback counts. Forced child termination is failure cleanup, never success evidence.

The same built binary also passes the unchanged seed/serve checker: original Cancelled4 followed by two owner-reopened Complete4 Uses. Formatting, example build and strict example-target lint passed against the same source. Production libraries, contracts, dependencies, locks and the original checker are unchanged. The underlying host's full 1,567/0/11 regression remains separate historical evidence; no fresh full workspace rerun is claimed here. This process qualification covers macOS arm64 and Rust1.93.1 with Unix signals/file locks.

See the [runbook](../../../docs/runbooks/permission-reuse-example.md) and [exact qualification record](qualification.json). Public schema/reference checks establish metadata evidence only.
