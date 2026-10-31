# Independent lifecycle reference fixture

This application exercises the production Fetch handler at the retained source
pin `8acac534ba6eb960b34de217b0da45b8405023af`. It uses the unchanged scoped
13-task, zero-cache runtime build recorded for the grant fixture. All 385
JavaScript/JSON artifacts, the source checkout and lockfile are checked before
import and after capture. This fixture performs no new build and changes no
upstream source. Each invocation creates two fresh temporary application roots
and databases: an owner-policy profile and a separate ungated missing-resource
profile. The driver records 77 requests and checks 11 behavioral assertions.

The immediate and held routes are ordinary workflows. Production workflows emit
only their terminal event: the captured live digest is honestly empty and its
tail contains the actual terminal. The held workflow appends an entry to a local
ledger, waits for its own release file, and appends its exit. It deliberately
ignores cancellation signals. Cancellation acknowledgements and terminals are
captured before this effect becomes quiescent; deletion and replacement remain
blocked until it exits. Every held effect is released and drained before cleanup.
No business service, provider API, runtime handler, registry, checkpoint, grant,
pending result or attachment is mocked.

The parked route uses the actual exported agent builder, permission gate and
SQLite checkpoint/grant implementation. Only its model import uses the existing
public deterministic importer seam. The real adapter proposes one local tool,
and the real permission gate parks it. The driver never approves that tool.
Its checkpoint, addressing keys, prompt and grant survive a graceful handler
reopen and are then deleted through the actual endpoint. Retained raw fixture
grants belong exclusively to temporary databases destroyed at capture completion.

The owner policy accepts the fixture owner header, stamps creations through the
server policy result, checks that stamp on existing resources, and permits that
owner when a resource is absent. Thus owner missing-resource requests reach the
endpoint's natural response. Other callers are denied before protected existence
or activity diagnostics. The separate ungated profile preserves missing-resource
behavior without any policy. A genuine asynchronous `thread.cancel` policy check
holds a cancel request while the observed turn finishes and a successor starts;
the driver then releases the policy and checks that the successor survives.

The per-thread viewer cap is the actual handler option set to one. A second
viewer receives the actual capacity-detachment event. The first viewer's request
is aborted after its initial state frame; its raw body is an explicitly
incomplete `viewer_abort` prefix with no invented terminal or complete-EOF claim.
The producer remains active and completes after its application barrier releases.

Raw request/response bytes, headers, hashes, every SSE block and cut-time ledger
are retained. Unlike the legacy event-only parser, this fixture separately keeps
retry-only blocks in their wire position. Actual durable attachment retry hints
range from 1500 to 2500 milliseconds. The limited repeat projection masks only
those hints, resource timestamps, the generated idle-thread identifier (including
its endpoint diagnostics), live start times, known checkpoint/provenance IDs,
serialized HumanMessage IDs in checkpoint task/message locations, and genuine
permission interrupt/grant/resume identifiers. It preserves application data,
array order, live digest cut, event order, null/omission, terminal outcome,
completion/abort marker and every ledger entry. Unexpected retry values remain
visible. Projection tests protect these boundaries.

The observations qualify selected cancellation, deletion, state and attachment
behavior of this in-process production Fetch runtime. They do not establish TCP,
whole-upstream-suite or native Rust parity, abrupt restart, deletion transaction
failure, attachment overflow, cross-replica behavior or arbitrary provider/adapter
exactly-once execution. Existing workflow and grant evidence remains unchanged.
