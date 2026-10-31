# Runnable HTTP permission cancellation example qualification

The explicit `serve-cancellation` mode of `serve_permission_reuse` runs the qualified V7 host with a finite Pure cooperative callback and immutable application-cancellation-1 revision. See the [runnable instructions](../../../docs/runbooks/permission-reuse-example.md#try-cooperative-cancellation) and [machine-readable evidence](qualification.json).

Fresh local checks passed on macOS arm64 with Rust 1.93.1. Native source descriptor: `877fd90bfc6ff40b28a1c1d7496503d2c31b22aca069c3e030cf0745b10dd19a` (1,054 files). Actual executable SHA-256: `a801e4a4ce2265db6ab85ba5ba086feb096b93f319e03d7f8da1a3ca7be8b628`. All six final steps used this same unchanged before/after source map and executable, finite child limits, confirmed reaping and locked offline dependencies. Only the example and new Python process check differ from the baseline; libraries, contracts, dependencies and both existing probes remain unchanged.

| Check | Result |
| --- | --- |
| Locked offline example build | Passed |
| New cancellation process probe | Original Cancelled6, saved NewUse Cancelled5, later saved Complete4 |
| New probe lifecycle | 3 normal server owners, 2 genuine Use records, database integrity and owned cleanup |
| Existing seed/reuse process probe | Passed unchanged |
| Existing HTTP Always process probe | Passed unchanged |
| Formatting and strict example-target lint | Passed |

The new process probe creates its thread and original Always approval over real authenticated HTTP. It consumes flushed actual callback-entry events, then requests cancellation and checks the interrupted response, one cancelled SSE Done and the waiting-run cancellation response. After each normal server retirement it acquires the owner lock and performs bounded query-only SQLite inspection. Exact journal versions0..6,0..5 and0..4, native acknowledgement state without invented result/output, fresh owner/turn/attempt/use identities and immutable permission provenance are checked. The third owner proves later normal reuse. Readiness, anonymous denial, bounded configuration refusal, listener/owner release, credential-safe output and temporary removal are also checked.

The missing-mode RED was observed against the unchanged old executable before implementation. One later reader-only correction aligned SQLite connection opening with the existing query-only probes after a read-only URI could not inspect a normally closed WAL database on this host. Query-only checks, deadlines, all business assertions and the missing-mode branch remained unchanged. The six final passes above all use the corrected final source; historical RED and the amendment are separately recorded.

The new probe passed in 1.09 seconds. Its full log SHA-256 is `53c42373d1e8c4531e1195b13b7d852dec4f66b8350b037e9148f62a12549e06`. The old seed/reuse and Always logs are `c53a61631328d93f522250865915718db784fb8156e1faea91760d32f9fc6431` and `1cd884bb02e0bf3e5ba17537d989b389397fa452f57a7008a0e6af73dc5bbc7c`. A forced kill or timeout is never counted as normal retirement or success.

This process evidence proves actual entered cancellation and later saved reuse. Ready/withheld behavior comes from the separate [V7 library qualification](../http-withheld-cancellation/README.md); timing in this example does not prove deterministic pre-entry withdrawal. Stored attempts are not treated as callback-count instrumentation. The finite local wait expires after 30 seconds without accepted cancellation; normal SIGINT drain allows that remaining wait.

No fresh full workspace suite is attributed to this example-only increment. The preceding library qualification retains its own 1,646 reported passes, zero failures and 11 existing ignored tests. Metadata is checked separately after public adoption and supplies no runtime credit. This is a Unix-local process fixture, with no recovery/deletion, automatic callback/storage replay, maximum-size or cross-platform qualification.
