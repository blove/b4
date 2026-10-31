# Native immutable application registry and capability discovery

Status: independently reviewed and accepted for implementation. This is the next bounded native increment after the
shared execution driver receipt. The user's continuous-build request authorizes
design work; this document does not close M0 or the core release gate.

## Outcome and scope

An application can bind trusted Rust callbacks to an immutable fixed execution
definition and run them through the existing authenticated HTTP host. Native and
scripted applications use the same shared driver, durable preparation, approval
service, cancellation gate, publication, attachment and producer lifetime. The
new capability endpoint describes the configured subset and its actual limits.

The original RFC sections2.1,4,6.2,8,10 and15 motivate this surface. Its proposed
capability endpoint and illustrative Rust API are design inputs, not recorded
TypeScript behavior or executable instructions supplied by the user. This work
adds reviewed native contracts. Existing COMPAT evidence and all previous receipts
remain unchanged.

This scope adds no provider, dependency, schema change, dynamic graph, runtime
plugin loading, hot replacement, automatic recovery scan, Always permission,
deadline or shutdown implementation. Native callbacks are trusted code; they can
perform external work and may fail to return. The driver retains ownership in
that case. Callback/validator duration and whole-process memory are not newly bounded.

## Approaches and decision

1. **One immutable native registry and the existing HTTP driver port** is the
   recommended approach. `b4-runtime` owns portable definitions, callback bindings
   and validation. HTTP owns authority and AP projection. An additive native host
   constructor uses that registry; legacy script constructors keep their behavior.
   This provides a real application surface to both embedding and HTTP callers
   without another scheduler. It requires a small common definition-manifest port
   because approval and recovery currently accept `ScriptRoute` maps.
2. **Add callbacks directly to HTTP configuration** would require fewer initial
   public types, but embedding would still have a different registration surface.
   It would couple application identity, effect validation and capability metadata
   to AP and encourage separate copies of definition/handler maps.
3. **A registry with historical revisions, hot loading or plugin discovery** could
   drain multiple deployed versions, but introduces selection, lifetime and rollout
   rules that are unnecessary here. The current approval/recovery maps are keyed by
   one route and compare the full stored definition. Keep that exact behavior first.

## Registry and portable ports

Use a private-module implementation in `b4-runtime`, with public types exported
from its existing library. No new crate is needed. Proposed API names:

```rust,ignore
pub struct ApplicationIdentity {
    pub application: String,
    pub revision: String,
}

pub struct InvocationContext {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub origin: ThreadOrigin,
    pub application_revision: String,
    pub execution_revision: ExecutionRevision,
    pub policy_revision: String,
}

pub struct EffectUncertain;
pub struct InvalidApplicationInput;

pub enum NativeOutcome {
    Succeeded(TaskResult),
    Failed(String),
}

pub enum NativeInvocationError {
    DefinitionUnavailable,
    ContextMismatch,
    TaskEffectMismatch,
    InvalidOutput,
    Uncertain,
}

pub trait NativeEffect: Send + Sync {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain>;
}

pub trait InputValidator: Send + Sync {
    fn validate(&self, input: &Value) -> Result<(), InvalidApplicationInput>;
}

pub enum OutputKind { Value, Void }

pub struct EffectBinding {
    pub callback: Arc<dyn NativeEffect>,
    pub output_kind: OutputKind,
}

pub struct RouteRegistration {
    pub definition: Definition,
    pub effects: BTreeMap<EffectId, EffectBinding>,
    pub input_validator: Option<Arc<dyn InputValidator>>,
}

impl NativeApplication {
    pub fn new(
        identity: ApplicationIdentity,
        routes: Vec<RouteRegistration>,
    ) -> Result<Self, ApplicationConfigError>;

    pub fn invoke(
        &self,
        definition: &Definition,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, NativeInvocationError>;
}
```

The names are proposed; the ownership and validation guarantees are the contract.
`NativeApplication` owns private immutable maps and exposes borrowed metadata and
trusted lookup/invocation methods. It has no mutation API. Hosts retain an `Arc`
for their entire lifetime, and owned callbacks borrow or retain that same registry.
Portable invocation selects the exact registered full Definition/revision and
validates task/effect membership, context.origin.application against registry
identity, and context execution/application revisions against that selection.
Mismatches return static typed errors before invoking any callback. Lookup and
successful context matching are not authorization. Callbacks, validators and
captured configuration are never serialized or included in default Debug output.

`InvocationContext` is transient context. HTTP verifies and derives it from the
original accepted thread generation/origin, the exact registered definition and
application revision and current configured policy identity. A public embedding
caller supplies trusted context and remains responsible for authenticating and
authorizing that caller; the registry does not authenticate supplied fields.
HTTP binds policy revision through its existing authority configuration; the
portable registry does not treat a supplied policy label as permission. Context
labels retain the same finite validation as registered identity. It contains no
headers, raw credentials, grant token, callback object or mutable `ThreadView`. Client input and metadata
cannot populate it. Existing preparation/authority checks still decide whether an
invocation may enter. It is not a credential that permits a callback to invoke the
store or to bypass a later policy check.

Callbacks receive the exact durable `EffectInvocation`; the driver remains the
only scheduler/completion writer. They must not mutate this execution's runtime
store. Native callbacks return `NativeOutcome::Succeeded(TaskResult)` or
`NativeOutcome::Failed(String)`. The host converts them to the existing driver
outcomes. `Failed` is an explicitly public application reason. `EffectUncertain`,
panic and invalid output fault the host; they never become an invented application failure.
The host supplies the existing never-entered `Cancelled` acknowledgement when its
entry gate sees committed cancellation. An entered native callback is not forcibly
interrupted. It returns its real outcome; a `Cancelled` acknowledgement is valid
only under the kernel's existing committed cancellation/failure-drain checks.
`NativeOutcome` has no Cancelled variant, so entered native code cannot fabricate
that host acknowledgement.

`OutputKind` preserves explicit void versus present JSON null without changing the
snapshot format. Value is the default. When a Void binding returns Succeeded,
its output must be null; non-null output contradicts its binding and is a
static callback-contract fault. AP continues to use the final committed task's
output, including false/zero/empty/null, and its existing denial/failure terminals.
Void wait is null and void stream done is `{}`. The output policy is trusted
versioned program metadata, not a client field.
Void restricts only a successful output; a Failed outcome still produces the
normal durable public application failure. An inherited live attachment terminal
preserves the original void `{}`. Inactive snapshot-only attachment always sends
`done {output:null}`, including a void execution after reopen.

Native callbacks do not receive a raw SSE publisher in this slice. They cannot
impersonate lifecycle frames or claim success before durable completion. Existing
script chunks keep their current after-commit projection through the private
script adapter. A richer native event port requires a separate contract.

## Atomic construction and exact revisions

Validate all registration input into temporary maps, then publish the application
only after every route passes. Validation invokes no callback or validator, opens
no store, and admits no execution. A failed constructor exposes no partial registry.

Use the current finite program limits: 1..64 routes, 1..64 tasks/effect
bindings per route, and 1..64 total attempts. Each definition
must pass the kernel's structural validation; its attempt budget must cover its
declared tasks. Route strings and opaque IDs keep their existing typed validation.
Application/revision labels are nonempty, bounded to256 UTF-8 bytes and contain no
control characters. Native zero-task programs reject construction; legacy empty
program behavior remains unchanged. Duplicate route keys, missing or extra native
effect bindings,
duplicate task/effect identities and invalid output modes reject construction.
All configured fixed script results continue to use the same byte/node/depth
validator as driver outcomes.

Each route has exactly one full immutable `Definition` and `ExecutionRevision`.
Callback bindings resolve only by that selected route's exact `EffectId`; there is
no wildcard, global fallback, filesystem lookup or live-provider fallback. Task
recovery policy remains an explicit declaration in its persisted `TaskSpec`, not
an inference from callback type. Pure and Idempotent declarations are not proofs of
determinism or an external contract. Unsafe/Reconcilable uncertainty retains the
current blocked behavior; no reconciliation adapter is added.

Application revision describes the compiled application assembly. The persisted
execution revision remains the recovery fence. Changing a callback, validator,
output policy or execution behavior requires a new execution revision; changing
the application assembly requires a new application revision. Rust function
identity is not serializable and this registry does not prove that two binaries
using the same declared revision contain identical code. Revision discipline is
a trusted application obligation.

Resume, recovery, pending disclosure and attachment compare the full stored
definition against the registry, after ordinary authority. An absent route, a
different revision or any other definition difference is refused with the current
static typed behavior before dispatch or protected disclosure. The host never
silently chooses a newer callback. An old binary can continue its registered
version; selecting multiple embedded versions is deferred.

## HTTP assembly and authority

Add `HttpHost::new_with_application` for `S: ApprovalStore`. It requires an explicit
native application, authenticator, resource policy, finite host limits and existing
`ApprovalHostConfig`. Durable configuration and explicit MemoryStore
`VolatileFixture` retain their current meanings. The existing ApprovalBridge is
installed even when no task requires approval and the exact prompt map is empty.
This renews resource authority and verifies the complete claimed view before every
native callback, rather than duplicating that logic in a new ordinary dispatcher.

Keep `HttpHost::new` and `new_with_approvals`, `HostConfig::new` and `ScriptRoute`
source-compatible. Factor their existing finite limit values into a common
`HostLimits` representation; legacy public fields and constructors retain their
defaults. A proposed `NativeHostConfig` holds the application `Arc`, those limits
and approval configuration. A private application adapter exposes a common
definition manifest and exact effect source to all HTTP consumers. Script adapters
retain Pure-only validation and existing fixture semantics. Native construction
does not relax that legacy restriction.

Native application identity must match the authenticated principal's application
at every protected native entrypoint, including public embedding
`recover_execution`, before any ordinary/sensitive Store lookup, resource/grant
diagnostic, decode or native capability disclosure. Existing
tenant/principal origin checks and resource policy remain mandatory. Unknown
application scope fails closed even if a policy permits the operation. Public
healthz/readyz retain their existing probe semantics. Fresh unknown native routes
and denied private routes return the same404 `resource_unavailable`; legacy
scripted route errors retain `route_not_found`.

The normal data flow is:

1. Apply existing transport bounds and authenticate; check native application scope.
2. Resolve the literal route within the immutable application and authorize the
   resource/requested provenance before returning route/input diagnostics.
3. Reserve the existing active-run permit and thread claim before launching the
   bounded owned blocking producer. Run its optional pure input validator there,
   after ordinary authority but before implicit thread creation, admission, grant
   consumption or any effect. A known refusal is400 `invalid_application_input`;
   a validator panic is a static infrastructure fault.
4. Retain the permit, claim, registry and store through validator return, including
   caller abandonment. Renew origin/policy and the exact captured target before
   implicit creation/admission, then admit through the existing generation fence.
5. Prepare through ApprovalService, retaining all Once/Deny and renewed-view checks.
   The shared driver verifies the actual durable claim before callback entry.
6. Enter the original RunControl gate, then invoke the exact registered callback
   with server-derived context. The driver bounds outcomes and commits them.
7. Perform existing postcommit projection and terminal selection. All children,
   retained outcome handling and projection finish before producer guards release.

Validators are trusted application code but must be pure and side-effect free.
They never run inline on the async request executor or in a separate unbounded
blocking job. Their duration is explicitly unbounded; the existing active-run
limit bounds the number of owned validators. A dropped request releases neither
their permit nor their thread claim early. If another actor changes the target
while validation waits, renewed authority/identity refuses admission afterward.
They validate application fields, not executable controls or identity. Omitted
input and explicit JSON null/scalars/arrays retain the current parsing semantics;
an application can intentionally narrow them. Continuation uses committed batch
input and the accepted decisions; it does not reinterpret submitted resume input.

Cancellation captures the original local control before resource policy, commits
its existing fence and wakes publication promptly. Callback return, retained
completion-only repair and late failure precedence remain unchanged. No callback
is retried merely because its completion failed. The host retains store, registry,
thread claim, active-run permit and task permits for their existing qualified
lifetimes, including abandoned requests and held policy. Attachment has independent
bounded subscriptions and never takes ownership of execution.

## Honest capability descriptor

Add authenticated, resource-policy-protected `GET /b4/capabilities` and
`Operation::ReadCapabilities`. The initial policy context has no thread and no
requested route. Native application scope is checked first. There is no public
discovery mode in this slice. Existing host-unready refusal still applies.

Return a typed, versioned native descriptor with bounded labels and fixed keys:

```json
{
  "descriptor_version": 1,
  "target_profile": "b4-ap-core/0",
  "profile_status": "native_subset",
  "core_profile_frozen": false,
  "runtime_version": "0.1.0",
  "application": { "id": "demo", "revision": "native-demo-v1" },
  "execution": { "mode": "single_process", "registration": "immutable", "native_callbacks": true, "approval_service_mode": "durable_required" },
  "features": {
    "live_attach": true,
    "required_once_deny": true,
    "trusted_known_id_recovery": true,
    "fenced_cancellation": true,
    "fenced_deletion": true,
    "automatic_recovery_scan": false,
    "always_permissions": false,
    "durable_event_replay": false,
    "native_live_chunks": false,
    "built_in_model_providers": false,
    "memory_candidates": false,
    "managed_workspaces": false,
    "ag_ui": false,
    "dynamic_graphs": false,
    "graceful_shutdown": false,
    "turn_deadline": false
  },
  "store": { "kind": "sqlite", "durable_runtime": true, "durable_approvals": true },
  "readiness": { "response": "status_only", "aggregation": "store_readiness_and_no_owned_worker_fault" },
  "limits": { "active_runs": 8, "concurrent_tasks": 8, "concurrent_tasks_per_execution": 2, "max_attached_viewers": 16 }
}
```

This is illustrative, not a fixture oracle; optional feature values are derived.
The final descriptor lists all current effective finite input/result/program/
publication limits, including completion attempts and depth, and explicitly records
unbounded or absent deadline/thread/history/parked-retention controls. It does not
claim a bounded whole process. Runtime version comes from package metadata, not a
secret-bearing environment dump. Legacy script hosts report native_callbacks false
and application identity/revision null rather than inventing an application label.
False subsystem flags describe the framework's installed adapters. An opaque
trusted callback can itself use a network or file API; the descriptor does not
inspect its code or claim to sandbox it.

The descriptor contains no route list or route count. Global discovery policy
therefore does not accidentally reveal an inaccessible route. It also contains no
effect IDs, prompts, grants, credentials, captured callback Debug output, SQL
location or absolute paths. Configuration presence is not a statement that a
caller may invoke every operation or route. Responses use `Cache-Control: no-store`.

Keep current `/readyz` wire behavior: a200/503 status-only response aggregates
`store.readiness()` and absence of the shared owned-worker fault. The descriptor
names the logical checks `store_readiness` and `owned_worker_fault` without
inventing per-check statuses, a checks response object or schema-table diagnostics.
It does not alter readiness to match the RFC's illustrative richer response.

Avoid backend type names, downcasts and mutating capability probes. Add an additive
`Store::capabilities() -> StoreCapabilities` with explicit conservative defaults:
unspecified backend and false advertised durability/cancellation/deletion flags.
`StoreKind` is the fixed enum Unspecified/Memory/Sqlite. It accepts no arbitrary
backend String, type-name, Debug value or filesystem path.
Memory and SQLite explicitly report the abilities they implement. Arc/reference
forwarding preserves the wrapped descriptor. Custom backends can opt in through
this trusted semantic declaration; default false means unadvertised, not a fabricated
success or detection of an override. Actual optional methods still return their
typed501 when unavailable. Physical approval durability comes from the existing
`ApprovalStore::approval_durable` declaration, independently of the configured
service mode. Report that mode separately as Disabled/DurableRequired/VolatileFixture.
VolatileFixture on SQLite still reports durable physical approval storage; it does
not change the backend's declaration. Installing the native bridge with an empty
prompt map supports the required Once/Deny mechanism without revealing route count.

HTTP derives operation flags from the installed host port and store declaration.
It never advertises optional cancellation/deletion solely because its router has a
path. Recovery is explicitly the trusted known-ID embedding operation, not an HTTP
recovery route or scan. A corrupt/lost store still makes readiness fail; static
capability metadata is not a health result. No new persisted record is needed.

### Frozen descriptor v1 fields

`CapabilityDescriptor` uses the illustrated top-level fields. Version is1,
target/profile/mode/registration/readiness strings are finite enums, and
core_profile_frozen isfalse. `CapabilityApplication` has id/revision bounded
to256 UTF-8 bytes with no controls. Runtime version has the same label bound.
Script assembly has application:null and native_callbacks:false.
`CapabilityExecution.approval_service_mode` is disabled/durable_required/
volatile_fixture. Store kind is unspecified/memory/sqlite; physical approval
durability is separate from service mode. durable_approvals is nullable: null
means the assembled ordinary Store port has no ApprovalStore declaration; native/
approval hosts report Some(store.approval_durable()). An ordinary SQLite host
therefore reports runtime durability true and approval durability unknown, rather
than a false physical assertion. Optional mechanisms derive from installed
ports and explicit store declarations, never a route inventory.

Features use all16 illustrated keys plus native_callback_sandbox:false. Readiness
additionally contains checks:[store_readiness,owned_worker_fault], naming aggregate
inputs without reporting their individual status. Unimplemented feature flags
remainfalse for this descriptor version.

`CapabilityLimits` contains body_bytes,json_depth,active_runs,concurrent_tasks,
concurrent_tasks_per_execution,viewer_queue_frames,heartbeat_ms,input_bytes,output_bytes,
max_attached_viewers,turn_buffer_bytes,turn_buffer_frames,viewer_queue_bytes,
attachment_snapshot_bytes,program_routes,program_tasks,program_attempts,
completion_attempts,task_failure_bytes,script_chunks,script_delay_ms,
approval_bytes and nullable approval_ttl_ms. Effective configurable values use
the existing constructor ranges; fixed limits are65536 body/approval bytes,
64 depth/program limits/script chunks,8 total completion attempts,1024 failure bytes,
5000 script delay milliseconds. Approval TTL isnull without a bridge, otherwise
the configured1..86400000 milliseconds. The descriptor response itself uses the
fixed body bound, independently of the configurable application output bound.

`CapabilityControls` explicitly hasnull for callback_timeout_ms,
validator_timeout_ms,turn_deadline_ms,max_threads,max_history_bytes,
parked_retention_ms,connection_limit andwhole_process_memory_bytes. These nulls
describe absent controls, rather than claiming a bounded whole process. All
objects reject unknown fields. DTO/schema validation refuses changed constants,
unimplemented flags, zero/out-of-range effective limits and contradictory
application/native-callback or bridge/TTL declarations. DurableRequired requires
approval durability true; VolatileFixture requires an explicit physical bool. The descriptor contains
no policy revision, callback duration estimate, route count or private inventory.

## Errors and fault handling

Registry/configuration errors occur before constructing a host or admitting work
and use bounded static codes. Known invalid input, denied authority, missing routes,
unsupported optional ports and exact revision refusals remain healthy request
errors under the current visibility rules. No rejected admission invokes a callback,
creates an implicit thread or consumes a grant.

Known callback application failure is durable and retains existing500 wait/SSE
error projection. Outer callback uncertainty, callback/validator/coordinator panic,
oversized or invalid outcome, corrupt claim, changed incarnation/definition, owner
loss and exhausted completion persistence fault uncached publication before draining.
No sensitive error or panic text becomes a public reason. Cached cancellation
terminals continue to win for existing viewers despite a later infrastructure fault.
Completion-only retries retain the exact known outcome and original task/attempt;
there is no callback replay, new attempt or budget reset during those retries.

## Example and meaningful qualification

Add a provider-free native example assembled from the same library/host API. It
uses the existing SQLite and explicit bearer-token/loopback/Host/Origin guards,
registers a deterministic native parallel route and an approval route, and exposes
the capability descriptor. Its native callbacks compute bounded results from
their committed input. Any local ledger path is server configuration and is absent
from protocol output. Fixture fault controls stay in tests. No listener is created
by the library and no second Tokio executor is created.

Tests must cover these independent behaviors:

- Registry construction is all-or-nothing: duplicate/missing/extra bindings,
  malformed identity/revision, invalid definition and finite boundary failures
  publish nothing and invoke no callback/validator. Native empty programs reject
  while legacy behavior remains unchanged. Literal lookup has no fallback.
  Portable invocation refuses changed full definitions, task/effect identity,
  origin application and execution/application revisions with callback count zero.
- HTTP genuinely invokes a native callback through durable preparation; two barrier
  callbacks overlap and reduce state in Definition order. A fixture miss never
  invokes another callback or network adapter.
- Invalid application input creates no implicit thread/attempt/grant. Explicit
  null/array/scalar semantics, healthy validator refusal and static validator panic
  are distinguished. A held validator and abandoned request retain run permit,
  registry, store and claim through return; other work sees the configured bound,
  and no implicit thread/admission/consumption occurs while held. Fresh policy or
  incarnation changes during the hold refuse afterward. User fields cannot alter
  native InvocationContext.
- Foreign-application requests, including exact known-ID embedding recovery,
  perform zero ordinary/sensitive Store reads and zero callbacks even with a
  permissive policy and matching definition. Native unknown/private denied routes
  both produce404 resource_unavailable without a registration side channel.
- Current origin/policy are renewed before every ordinary and approved native
  callback. Held initial/renewed policy cannot retarget a successor or permit a
  denied callback. Required Once consumption and known never-entered cancellation
  keep their existing exact evidence rules.
- Finite native callback barriers qualify cancellation prompt terminals, actual
  late success/failure, dropped request/primary/attached viewers, capacity/claim/store
  retention and fault-before-drain. Native null/void/falsy results remain distinct.
  Void failure remains a normal known failure; primary/cached live void terminals
  differ intentionally from inactive/reopened attachment's null terminal.
  Native callbacks have no API to manufacture Cancelled or lifecycle events.
- A separate-process native application ledger proves committed sibling preservation
  across a parked reopen/Once resume, accepted continuation without another approval,
  and Unsafe post-effect/precommit kill refusing replay. Exact absent/different
  revision blocks before invocation even with a permissive resource policy.
- Capabilities require authentication and discovery policy; application mismatch
  hides metadata. Ordinary/native/approval/Memory/SQLite/custom-default configurations
  produce the actual declared flags and limits. Arc/reference wrappers preserve
  metadata. Known backend descriptors agree with their actual optional-port tests;
  a custom backend's metadata is a trusted declaration, not detected functionality.
  SQLite VolatileFixture reports physical approval durability true and fixture
  mode separately, while Memory reports nondurable storage regardless of mode.
- Capability JSON/schema and real TCP responses contain no route inventory, token,
  secret, callback capture or path. Reads admit no execution, call no effect and do
  not change owner/fences/grants. Unsupported providers/replay/Always/scans remain
  explicit false, and the descriptor never claims a frozen core profile.
  The backend kind is a fixed enum, and `/readyz` remains status-only; descriptor
  check names do not invent per-check result fields. AG-UI/dynamic-graph flags are
  explicitly false, and callback/validator duration remains unbounded.

Use native requirements for new registry/descriptor behavior, real TCP for the new
HTTP surface, and the existing independently authored/reference corpus to detect
regressions. Keep prior source pins, fixtures, dependency/schema hashes and receipts.
The new increment receives its own reviewed design, plan and verification receipt.
No broad release scenario becomes passed merely because this bounded slice passes.

## Files and review boundaries

Expected implementation locations are new `b4-runtime/src/application.rs` (or small
private submodules), existing runtime exports and additive store metadata, new
`b4-http/src/application.rs`/`capabilities.rs`, and narrow host/worker/approval/
transport/recovery/attachment integration with the shared definition manifest.
Memory/SQLite only add explicit static capability declarations; migrations and
transaction semantics do not change. Add typed capability DTO/schema/OpenAPI and
new registry/native HTTP/TCP tests plus one compiled example.

The plan must keep registry/portable ports, HTTP integration, capability contract
and independent security/quality qualification separate enough to review. Driver
and prior receipt sources stay frozen except a separately demonstrated regression.
