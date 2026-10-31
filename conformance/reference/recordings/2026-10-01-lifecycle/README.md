# Pinned lifecycle observations

The separate lifecycle fixture ran twice through the actual pinned production
Fetch handler. Both captures contain 77 requests and 11 behavioral assertions,
using fresh owner-policy and ungated applications/databases. They reuse the
unchanged scoped runtime built for the grant capture: 13 successful build tasks,
zero cache hits and 385 retained JavaScript/JSON artifacts. All artifact hashes,
source status, source pin and lockfile bytes were checked before import and after
each capture. `build-provenance.json` explicitly links the prior build receipt;
these runs performed no new install or build.

`lifecycle-observations.json` and its independent `repeat/` counterpart preserve
raw request/response bytes, headers, hashes, SSE events and retry-only blocks in
wire order, completion/abort cuts, application ledger, and derived limited
projections. Both capture one real permission park through the deterministic
model importer seam; no provider or external Fetch call occurred. Its actual
checkpoint identity, provenance, values, prompt, grant and resume addressing
survive graceful reopen before actual deletion. The parked tool never executes.

Six held local workflow effects each enter and exit exactly once. Actual wait
cancellation returns before quiescence; actual streaming cancellation sends the
same cancelled terminal to primary and attached viewers. Deletion and replacement
remain refused until the held effect exits. Live workflow attachment honestly
captures an empty digest before the actual terminal tail. Viewer capacity emits
`detached` with `reason: capacity`. An attached viewer aborted after its state
frame retains an explicitly incomplete `viewer_abort` raw prefix, while its
producer remains active. A cancel held in the public asynchronous policy seam
cannot interrupt a successor that starts after the observed turn finishes.

All 77 response/cut-time-ledger projections match in their original order, with
only the declared generated identifiers, resource/live timestamps and bounded
retry jitter normalized. Application data, arrays, snapshot cuts, event order,
terminal outcomes and ledger counts are unchanged. Raw bytes differ and remain
available. `lifecycle-repeatability.json` records individual projection hashes
and scope; it does not claim whole-runtime or whole-protocol equality.

The owner policy permits its owner when the resource is missing, so those probes
reach natural endpoint behavior; foreign actors are denied before protected
diagnostics. The ungated missing-resource observations use a separate fresh
database. Idle attachment without a recorded route is a distinct refusal. Plain
workflow completion creates no checkpoint, while actual permission parking does.

This is selected in-process Fetch evidence for cancellation, deletion, state and
attachment. TCP, full upstream suites, abrupt crash, deletion transaction failure,
attachment overflow, cross-replica behavior, native Rust parity and arbitrary
adapter exactly-once execution remain unqualified. Earlier workflow/grant fixtures
and recordings are preserved unchanged.
