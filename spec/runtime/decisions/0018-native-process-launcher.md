# ADR0018: Native process launcher and positively witnessed shutdown

Status: accepted; scoped native macOS arm64 implementation qualified.

RFC15.2 requests a standalone native process and configuration validation. Existing
HttpHost supplies an embeddable router and a shutdown handle; its registered-owner
quiescence does not own the caller's executor or prove transport teardown.

Add private workspace crate b4-cli and binary b4run. Explicit --demo selects one
immutable provider-free native application. Config-check validates recognized
configuration/application/filesystem preconditions without binding or opening the
SQLite database. The launcher requires a private Unix data directory and reserves
new database/lock files privately before reusing existing SqliteStore semantics.
No dynamic handler loader, production restore command or readonly-inspection API
is added. The generic HTTP/SQLite/kernel APIs and schema remain unchanged.

The synchronous process runner owns its Runtime, host, application/auth/policy
captures and listener. Safe Tokio SIGINT/SIGTERM receivers register before request
acceptance. The first shutdown event cuts admission synchronously before notifying
Axum. A configured wait interval is observation only: surviving work/transport keeps
the Runtime, host and SQLite owner lock alive. Repeated signals never release
ownership or request forced exit. After registered Quiescent, an independent timer
paces transport-only incomplete waits.

Clean teardown requires registered physical Quiescent plus successful joined Axum
server completion, which waits for its detached connection tasks. Slow peers can
keep the initial runner draining indefinitely. A failed outer server task lacks a
positive transport witness and therefore retains process/resources indefinitely
until external manual termination. It cannot claim clean shutdown, infer retired
connections from listener close, or release the database because its owner count
is zero. Failed server results stay owned and are inspected by reference; opaque
panic payload disposal must not unwind the retained supervisor. The admission cut
precedes first-result inspection/diagnostics, and diagnostic I/O failures do not
panic. Signal-stream failure can request ordinary Axum shutdown and finish with
error status after both positive witnesses.

Only after both witnesses does the runner drop owned captures and normally destroy
its Runtime. It does not use executor shutdown_timeout/background or abort the
server to establish resource cleanup. Faulted Quiescent may preserve Unsafe
InFlight evidence; no callback replay, fabricated terminal frame or rollback is
introduced. Capability v2 keeps supervised_admission_drain:true and complete
graceful_shutdown:false.

The only newly fetched registry packages are signal-hook-registry1.4.8 and
errno0.3.14, both MIT OR Apache-2.0. Tokio1.53.1 remains pinned, signal is enabled
only for the launcher, and every previous external version/checksum is unchanged.
Dev-only rusqlite0.40.2 and socket2=0.6.5 reuse the prior locked graph for actual
record and network-backpressure qualification. No unsafe launcher code is allowed.

Private child-process tests must show responsive async ping after Incomplete,
retained real SQLite exclusion, actual OS signals and peer/worker release. A child
pauses alive after the runner returns so reopening SQLite proves resource cleanup
before OS exit. Shipped binary tests qualify explicit config/native/auth/storage
behavior. The full native workspace passes696 test entries (45 new launcher entries), and
12 shipped-binary checks also pass against the separately built release binary.
Two ordinary selflaunch helpers are included in45; they are not independent
behavioral proofs. No new test is ignored. Linux nonroot/container, readonly inspection, complete T047/T055,
B4N013 packaging, full graceful shutdown, M0/CP/core and broad AP parity remain open.
