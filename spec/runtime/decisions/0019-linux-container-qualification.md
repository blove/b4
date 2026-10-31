# ADR0019: Linux nonroot container with explicit local volume ownership

Status: accepted; scoped local Linux/arm64 implementation and qualification complete.
Actual results are recorded in [linux-container-verification.json](../../../conformance/expected/linux-container-verification.json).

RFC15.3 requires an executed Linux container profile with a nonroot identity,
owner-restricted persistent data and probes. The native macOS launcher receipt
cannot supply Linux evidence. Package the same production Rust/Cargo/SQL behavior on
Linux/arm64 using pinned official Rust and Debian image indices.

The runtime is exec PID1 b4run with UID/GID10001, fixed inner loopback port3001 and
a private data leaf under /var/lib/b4run. Bounded public curl readiness probing
includes fail-on-HTTP-error, no proxy/default configuration and no authentication
secret. There is no host ingress, published port or implicit anonymous volume.

An explicitly owned local named volume uses nocopy. Only a separate initializer
may run UID0, on its verified fresh empty task-owned volume, changing only that
empty root to0700/10001:10001. The runtime never repairs existing ownership or
permissions. The packaging preflight verifies mounted-parent metadata; the frozen
launcher retains its existing trusted-parent contract without a new parent guard.

Qualification-only tools observe six logical SQLite tables in one explicit
readonly transaction. Live reads preserve WAL visibility; clean stopped immutable
reads require positively retired isolated writers, no open observer and absent
WAL/SHM/journal. No reader is held through stop or existing WAL ignored. Complete
private row comparisons permit only exactly one metadata epoch advance on reopen;
original execution owner, snapshot, prompt, token, expiry and acceptance stay exact.
Raw authority remains in private pipes/memory. Public evidence uses allowlisted
counts/booleans/artifact metadata, without raw env/inspect or authority-derived hashes.

Actual SIGTERM/SIGINT plus stopped marker/exit0/notOOM establish container clean
exit. Docker's grace deadline may force SIGKILL and therefore is not cleanup proof.
Stronger registered-worker/transport/executor ownership checks run as
actual nonroot Linux Rust tests. A reviewed two-file cfg(test) exception observes
actual socket-write Pending with a finite pipeline, preserving every physical
retirement assertion; production behavior and historical receipts remain unchanged. Capability graceful_shutdown remains false.

Base digests do not freeze Debian package additions. Record actual package versions,
resulting image IDs, executable/architecture/dependencies and fresh tests; no
reproducible-build, other-architecture, network-filesystem, Kubernetes or full core
qualification claim. Offline inspection and the remaining M0/CP/operations/broad
release gates remain open. No image publication or deployment is requested.
