# Supervised admission drain and quiescence design

Status: reviewed bounded increment under continuous-build authorization.
Baseline: immutable native application verification receipt
`conformance/expected/native-application-registry-verification.json`
(SHA256 `8bf5409b5e1b831066e62d99a6fbcdab651d6a823eb1db344b5bc276269fb7b1`).

This is the next bounded increment after immutable native registration; it adds no
persisted deadline, callback interruption, automatic cancellation/recovery, SQL or
dependency. It is native RFC12.3 design input, not broad T047/core closure.

Choose one monotonic Serving -> Draining supervisor with counted RAII ownership;
closing only active-run capacity cannot account for create/delete/cancel, policy,
readiness or detached preflight jobs. Callback signals/persisted shutdown semantics
are deferred. The HTTP library still owns no listener or executor.

Proposed additive embedding API: HttpHost::shutdown_handle()->ShutdownHandle;
handle.begin_shutdown() idempotently cuts admission synchronously; status() reads
state/count/fault; wait_quiescent(Duration) asynchronously returns Quiescent{faulted}
or Incomplete{active_owners,faulted}. Bound waits to <=300seconds, default30seconds,
and permit zero as a nonwaiting snapshot. wait_quiescent is a regular function
returning an owned Future: validate the duration, then synchronously begin shutdown
even if the valid returned future is never polled. InvalidTimeout (>300s) changes
nothing. Status alone may observe transient Serving idle and is not a drain proof.
A cancelled/timedout wait never reopens
admission, aborts actual workers or releases their Store. Handle owns no Store.

One short mutex serializes owner entry/admission lease against begin_shutdown.
Never hold it across policy/Store/RunControl Pending/claim locks/callbacks/await.
Register the complete protected request before its first await, including body,
authentication, policy and Store phases; clone its counted owner into every
detached blocking job. Count each registered owner group until actual return. Cloneable Arc guard
groups outlive abandoned async callers; producer retirement follows scoped join,
completion/projection, then claims/permits and all framework Shared/Store captures.
Use an explicit owner carrier/drop order rather than closure capture ordering. Post-cut protected requests return the same healthy503 host_draining before
authentication/body/policy/Store work. This explicitly changes only closed-host
operational status: public readyz already reveals Draining, and no protected data
or action is performed. Serving authentication/application scope stays unchanged.
Healthz remains200 without Store; readyz503
immediately without probing Store. Await readiness/policy before postwait mutation
lease: a held preflight/validator losing the cut returns healthy503 without create,
admit, grant consume, adopt, delete or beginCancel. A lease that wins the mutex is
grandfathered across the cut and retains counted ownership until actual completion.

Already durably admitted turns continue normal dispatch to terminal/park; do not
strand Ready work or accepted grants. Existing leased cancellation commits/rejects
normally, preserving Pending entry gates and real late outcomes. Shutdown never
waits on Pending while holding its supervisor mutex. Known uncertainty can be
physically quiescent with durable InFlight: report Quiescent{faulted:true}, preserve
evidence and recovery policy. Infrastructure faults/panics are marked inside the
owned job even if its caller disappears; some create/preflight errors currently
map only after await, so audit those boundaries explicitly.

Quiescent proves framework-owned jobs exited; it neither proves successful
executions nor drops external host/router/Store refs or promises a lock release
instant. The embedding drops its refs/listener/executor only after coordinating
this API. Real SQLite owner remains held while any owned job survives.

Capability v1 remains its frozen false-shutdown/two-readiness-check contract. Add
explicit descriptor v2 with supervised_admission_drain:true and serving_admission
in the readiness logical aggregate, plus shutdown_default_wait_ms:30000 and
shutdown_max_wait_ms:300000. Keep graceful_shutdown:false because this slice does
not signal callbacks, close viewers/listeners or close application-owned resources.
Callback/validator/turn timeouts remainnull and sandboxfalse. Review the simplest compatible DTO/schema treatment
before code; do not silently repurpose v1's fixed constants.

Tests: linearized lease/cut races; held policy/validator abandonment+timeout/no
admission; create/delete/cancel/recovery/Once jobs with no run permit; Pending
cancel/entered callback/no deadlock; cancelled Done still Incomplete; real native
result after timeout; faulted physical quiescence retaining unknown task; repeated
and cancelled waits; real TCP refusals and SQLite last-ref retention/release.

## Frozen operation lease boundaries

The group proves lifetime; a separate lease proves pre-cut operation admission.
New acquires its mutation lease after last host preflight/input validator and before
implicit creation/admission. Resume acquires before ApprovalBridge.accept; Recover
before authority-bearing adoption; Create/Delete before mutation; Cancel BEFORE
begin_cancel creates Pending. Service-internal renewed policy/CAS retries after an
operation lease won remain grandfathered. The contract does not claim that every
held internal policy prevents post-cut mutation; the linearized lease decides it.
Already durably admitted driver preparation/completion continues under that lease.
Readiness begun in Serving has counted ownership, probes outside gate mutex and
rechecks Draining before returning; new Draining readyz performs no Store probe.

Public names: ShutdownPhase {Serving,Draining}; ShutdownStatus {phase,active_owners,
faulted}; ShutdownOutcome {Quiescent{faulted},Incomplete{active_owners,faulted}};
ShutdownWaitError::InvalidTimeout; DEFAULT_SHUTDOWN_WAIT=Duration::from_secs(30);
MAX_SHUTDOWN_WAIT=Duration::from_secs(300). begin_shutdown returns ShutdownStatus;
status returns ShutdownStatus; wait_quiescent returns Send+'static owned Future.
The handle/future retain supervisor and fault state, never Shared/Store.

Quiescent concerns registered framework jobs. Unpolled router futures/external
host/router/authentication/callback captures remain application-owned references;
they cannot enter Store/mutation work after the cut and may delay physical Store
destruction until their caller drops them. Tests prove actual reopen separately,
not from a zero count alone.

## Descriptor v2 interface freeze

Publish `CapabilityDescriptorV2` with the same flat root fields as v1 and
`descriptor_version:2`. Reuse v1 identity/execution/store/limits types. Use strict
new `CapabilityFeaturesV2`, `CapabilityReadinessV2` and `CapabilityControlsV2`
objects: features adds required `supervised_admission_drain:true`; readiness has
exact ordered checks `store_readiness`, `owned_worker_fault`, `serving_admission`
and aggregation `store_readiness_and_no_owned_worker_fault_and_serving_admission`;
controls adds required fixed integer `shutdown_default_wait_ms:30000` and
`shutdown_max_wait_ms:300000` alongside the original eight required null fields.
All v1 fields, enum domains, validations, authored v1 examples and schema remain
unchanged. V2 validation delegates common semantic bounds to an explicit v1
projection, then checks the new constants. V2 accepts no unknown nested/root fields,
missing nullable fields, extra readiness checks or relaxed feature flags.

The endpoint publishes v2 only after supervised ownership is assembled. Its
existing authorization, finite response bound and no-store treatment remain.
This operational descriptor describes a mechanism, not a successful drain, caller
permission or full graceful shutdown. No shutdown endpoint is added.
