# Local Linux container profile

The local Linux/arm64 nonroot profile passes all13 actual lifecycle cases.
Both macOS and Linux pass all696 Rust tests, strict lint/formatting and the same12
release-binary cases. The [scoped receipt](../../conformance/expected/linux-container-verification.json)
records the actual image identities, packages, executable hashes and results under
the [reviewed design](linux-container-qualification-design.md) and
[implementation plan](../../docs/superpowers/plans/2026-10-01-linux-container-qualification.md).
The frozen [native launcher receipt](../../conformance/expected/native-process-launcher-verification.json)
remains historical macOS evidence. This is local qualification, with deployment
and broader release gates still open.

Build the local images on an available Docker engine:

```sh
docker build --platform linux/arm64 --target qualification -t b4run-linux-qualification:local .
docker build --platform linux/arm64 --target runtime -t b4run-linux-runtime:local .
```

The official base indices are pinned in Dockerfile. Production Rust behavior and Cargo.lock are
unchanged. Two existing cfg(test) fixture files have an explicitly reviewed
exception to observe real Linux socket backpressure; historical evidence stays
immutable. Compiler source-location metadata changes the release checksum; the
new receipt records actual hashes and fresh qualification, with unchanged executable
code verified separately. Apt additions may change on a later rebuild, so a qualified run records
actual package versions, final image IDs and the executable hash. The runtime has
b4run and bounded curl probes; Rust/Cargo/Python/test fixtures belong only to the
separate qualification image. There is no Node or provider runtime.

Run the independent local qualification after building both images:

```sh
python3 -B scripts/qualify_linux_container.py \
  --runtime-image b4run-linux-runtime:local \
  --qualification-image b4run-linux-qualification:local \
  --receipt /private/tmp/b4-linux-container-runtime-qualification-new.json
```

Choose a previously unused receipt path: the driver creates it exclusively and
refuses to overwrite an existing result. Every driver-created container explicitly
uses working directory /, avoiding the private builder workspace without additional
privileges. Auxiliary observers install a signal handler and retire separately;
the shipped runtime remains the Rust exec PID1 process.

The driver generates a private runtime-only token and owns labeled disposable
containers/volumes. It compares actual stored rows privately, records only public
observations and cleans up only those resources. Generated tokens and issued grants
must not be copied into shared logs or receipts. Docker daemon administrators can
read runtime environment configuration; this profile does not isolate secrets
from a trusted daemon administrator.

For an operator-managed run, first provide an explicitly owned local named volume
whose mounted root is exactly0700/UID10001:GID10001. Existing data must already meet
the launcher's0700 leaf/0600 regular nonsymlink database/owner/sidecar requirements.
Do not use a privileged runtime entrypoint to repair existing files. The separate
qualification-only initializer at /opt/b4/container_initialize_volume.sh accepts
only a verified fresh empty volume: unique creation/label/local driver/no remote
options/exact nocopy mount/no other writer must be established before UID0 runs.
It changes only the empty root. A nonempty volume is refused without repair.

Run b4run as configured UID10001 with an explicit RW volume-nocopy mount at
/var/lib/b4run, a runtime-only0600 env file containing B4RUN_TOKEN, read-only root
and network none. Use in-container requests to127.0.0.1:3001. The existing listener
binds inner loopback; published Docker ports do not create host access to that
address. Host ingress is outside this profile. The fixed-port readiness probe is
/readyz, liveness /healthz. Override port only with an independently specified
healthcheck; it is outside this initial qualified profile.

Request shutdown directly with SIGTERM or SIGINT to the exec PID1 process. Wait
for actual exited status0, b4run_stopped and no OOM before reusing the volume.
Incomplete workers or transport keep the runtime/database alive indefinitely;
repeated signals do not escalate. Docker stop's finite grace period may eventually
SIGKILL, which is forced termination and uncertain recovery. A timeout, removal or
force-kill is never positive clean-shutdown evidence.

Reuse the SAME volume only after positive retirement. Reopening advances database
owner metadata but does not adopt unfinished work or refresh approval authority.
A prior-owner parked approval can be read through authenticated SSE attachment
without replay or altered prompt/token/expiry. Ordinary pending_interrupts still
returns501 for unsupported prior-owner state. SSE preserves the existing exact
Cache-Control no-store, no-transform; JSON protected responses use no-store. The qualification-only
snapshot utility at /opt/b4/container_snapshot.py is not a production inspect API.
It closes each observer promptly before shutdown; stopped immutable reads require
absent sidecars and all known volume writers retired, while live reads include WAL.

This local profile targets Linux/arm64 on Docker Desktop. Other architectures,
public ingress, orchestrators, network filesystems, power-loss recovery, complete
graceful shutdown, offline operator inspection and the full core release remain
open. See [ADR0019](decisions/0019-linux-container-qualification.md).
