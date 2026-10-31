# ADR0022: Unchanged repository AP callers against a finite native fixture

Status: accepted for the bounded macOS arm64 corpus. Two fresh original-caller
native runs, their stable comparison and independent actual-evidence audit pass.
See [the scoped receipt](../../../conformance/expected/unchanged-ap-client-native-verification.json). See the [design](../unchanged-ap-client-corpus-design.md),
[runbook](../unchanged-ap-client-corpus-runbook.md) and
[public schema](../unchanged-ap-client-corpus.schema.json).

CP010 needs evidence from actual AP callers. Handwritten HTTP probes omit original
serialization and parsing; the full software factory also requires business,
workspace and provider behavior outside this increment. Select the repository's
controller and AP worker factories at commit
`8acac534ba6eb960b34de217b0da45b8405023af`, build their two entries with the existing
installed tools and execute all 19 selected methods in eight groups / 63 invocations
per fresh native run. Initial original-client execution scope is macOS arm64 only.
Two fresh bundle/store runs are required; source builds alone do not supply calls.

Use a real SqliteStore, immutable NativeApplication, HttpHost, native auth/resource
policy, worker/kernel and RequiredDurable bridge. Finite Value/Void/known-failure
route registrations qualify transport outcomes, without factory business effects.
Keep native envelopes and original factories/wire/SSE unchanged. Controller default
fetch must observe HTTP 401; its documented fetch injection adds only the fixture bearer
header and guards exact origin/redirects. Declare these seams rather than adapting
responses, bodies, URLs, signals, permission schemas or grants.

Retain the actual permission counterexample. Empty pending parses, but the native
RequiredDurable pending envelope fails the original permission-request schema with
ZodError. Normal caller flow stops there. A forced probe passes only the actual
private interruptId to the original grantless resume with once/deny; both require
400 grant_required. Empty resolutions require HTTP 400 invalid_request. Full selected
thread/execution/grant/acceptance state must remain unchanged through refusals and
writer reopening. This does not establish a compatible durable approval handshake.

Capture successful socket-accepted read/write bytes without a second body consumer.
Correlate bounded pooled HTTP/SSE framing with actual caller results/frames/EOF and
declared cuts privately. Socket acceptance and consumer delivery remain distinct.
The lazy original iterator must be polled: viewer return follows an actual live GET
state frame; actual signal can settle a blocked held POST before awaited return.
Cancellation and logical terminal state never substitute for actual held callback
return or continued-exclusion checks.

Require explicit nonexpired gate return, normal Node exit 0/reap/control+stderr EOF and
joins, then admission closure/graceful server join/Quiescent and independent socket
retirement. Drop all observer aliases, prove Store/application Weak absence while
capture bytes survive, and actually reopen the SQLite writer with owner advancement
and selected terminal/park state. Failure kill/abort/cleanup release remains failure.

Own fresh 0700 directories/create-new 0600 artifacts, including reserved empty Store
database/lock files before open and verified actual sidecars. Bound sockets 16,
bytes 262144 per direction/slot, frames 64, control 256 commands / 8192 bytes per line
and stderr 1 MiB.
Reserve disjoint 24 MiB native plus 8 MiB client evidence bytes before writes; failed
writes consume quota, no borrowing or third writer is allowed, and final accounting
must agree. Runtime SQLite/source/compiler/bundle files are outside that serialization
cap. Finite 30-second gates, 120-second watchdog and at most 10-second retirement-stage
waits are logical deadlines, without hard scheduling/I/O or total-disk guarantees.

Apply independent offline native and client/capture audits before the closed public
projection. Freeze static method/group/AP labels, statuses, known error/result/frame
shapes, counts, 16 retirement fields, seams and open gates. Private raw authority,
tokens, grants, IDs, errors, paths, timestamps and their derived hashes stay private;
only separately admitted code/compiler/package/bundle provenance is public. Schema
validity is necessary structure, not runtime authenticity. Synthetic unit evidence,
historical artifacts, fresh bundle provenance and actual caller runs remain distinct.

All 182 preceding crate paths, manifests/lock/dependencies, AP contracts and retained
historical evidence stay protected. Selected later README/compatibility/requirements/
checker edits require exact reversal bridges; no product behavior or protocol repair
is selected. Independent SPEC then QUALITY gates precede original execution and the
final evidence freeze.

M0 remains open, CP001–009 partial, CP010 unresolved and M5 partial; all 56 broad
scenarios remain not_run and the core profile unfrozen. AP-004, other callers,
factory business, workspace/provider paths, TypeScript-host parity, broad parser/
rechunk/retry matrices and original-client Linux execution are deferred.
