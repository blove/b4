# Authenticated HTTP required approvals

Status: implemented native increment, 2026-10-01. Continuous implementation is authorized
by the user. The architectural RFC is input, not authorization for external
business actions. Existing TypeScript files and prior evidence remain intact.

## Choice and scope

Bridge the existing ApprovalService into the scripted HTTP host. Keep ordinary
`HttpHost::new<S: Store>` unchanged, including rejection of approval definitions.
Add an explicit `new_with_approvals` constructor for `S: ApprovalStore`, with
immutable policy revision, bounded positive grant TTL and server-authored prompts
for every required task. Durable storage is the default; MemoryStore requires the
same explicitly named volatile fixture mode as the embedded service.

A globally tightened Store bound would unnecessarily break ordinary embedding
applications. A separate router with duplicated authentication/worker behavior
would create two security boundaries. An internal object-safe approval bridge
keeps one host while requiring the durable service for approval work. Off,
optional and Always modes remain unsupported. Tools are still finite pure local
script fixtures, not provider integrations or a general sandbox.

## Port and configuration

`ApprovalHostConfig` has policy_revision, ttl_ms, per-route/per-task prompt maps
and ApprovalServiceConfig (default RequiredDurable). Validate exact prompt/task
coverage, prompt/canonical depth/bytes, revision/TTL bounds and projected aggregate
pending/interrupt response size before constructing the host. Prompts are trusted
application data; callers never submit them as execution configuration.

`Shared<S>` gains an optional internal `Arc<dyn ApprovalBridge>` created by the
constructor from the same Arc<S>, policy and immutable registry. No independently
supplied store can split the transaction. The shared host also has a bounded
per-thread worker claim set, used together with the existing global semaphore.

The internal object-safe port is:

```text
park(principal, originating_operation, current_execution, task_id) -> StoredExecution
pending(principal, disclosure_operation, captured_ThreadView) -> Vec<Value>
accept(principal, captured_ThreadView, ResumeRequest) -> StoredExecution
dispatch(principal, originating_operation, current_execution, task_id, attempt_id,
         &mut dyn FnMut(&EffectInvocation) -> Result<EffectOutcome,String>)
    -> StoredExecution
```

Each operation creates an ApprovalService with a request/worker-scoped authority
adapter retaining verified Principal, ResourcePolicy, policy revision, immutable
registered definitions and originating operation. The adapter verifies actor and
revision, exact current definition and immutable origin, and reauthorizes against
fresh token-free ApprovalResource. It never retains Authorization headers.
Pending disclosure uses ReadPending; accepted continuation uses Resume; park and
dispatch renew the originating RunWait/RunStream/Resume permission. Authority
failures have fixed public errors and are distinct from store/worker failures.
The bridge also validates the initial caller input against each configured
required task's canonical proposal before admission. Individually bounded input
and prompt values can exceed the combined proposal bound; rejection must happen
before creating a thread or starting an execution.

## Run, park and continuation

Workers commit Start for ordinary Accepted runs. Iterate Ready tasks: required
unapproved tasks park through the service, approved required tasks dispatch
through the service. Every callback in an approval-enabled host renews policy
through the bridge, including ordinary siblings/later steps; ordinary hosts keep
the existing runtime path. New admission also rechecks actor and fresh token-free
resource policy before mutation, so a permissive policy cannot strand a foreign
owner's thread when the stricter origin-only grant scope later rejects work.
Continue eligible siblings until the kernel reports stable WaitingForApproval;
there are no parallel callbacks in this bounded host. Successful siblings and
their pending writes remain committed and are not reexecuted on resume.

An approval park settles the HTTP turn and releases its active worker permit,
while the durable execution retains the thread's admission slot. Wait returns
200 null. Stream emits authorized `interrupt` events, each with the native
envelope {interruptId,resumeKey,value,grant}, followed by one done {output:null}.
Grant/prompt disclosure follows stable commitment and renewed ReadPending
authority. Tokens never enter done, ordinary GET state or error bodies. A detached
or overflowing viewer has no terminal delivery guarantee. GET pending returns
{interrupts:[...]} with no-store through the same service and captured fences.
Wait park does not read sensitive pending records merely to return null.

Expose the RFC path POST /threads/{id}/resume as authenticated SSE. Structural
parsing is strict and bounded using ResumeRequest and recursive duplicate/depth
checks. Authorize coherent ordinary context before grant-specific checks. Require
current ownership, stable WaitingForApproval, exact recorded route/current
registered definition, complete unique nonempty pending set and supported
decisions before converting native token shapes. The service repeats authority,
fences and all binding/expiry/consumption checks in its atomic transaction.

Acquire global capacity and a per-thread claim before acceptance. The owned
blocking worker holds both guards, commits accept, signals successful admission,
then executes the returned snapshot without Start. All-Deny may already be
Completed; a final Denied task returns native {denied:true}, never a callback or
missing-result infrastructure error. Response/body cancellation cannot dispose
this worker. Claim release occurs after stable park or effect quiescence.

Invalid decisions, consumed/expired/missing grants, conflicts and policy denial
do not make readiness fail. Infrastructure corruption, failed commitment and
worker panics do. Failed acceptance publishes no interrupt/done and spends no
authority. Post-admission infrastructure errors retain uncertainty and cannot
claim a completed approval. Post-claim policy revocation retains the embedded
service's conservative InFlight boundary without refund or effect invocation.
Preserve typed ApprovalError through the worker. A policy rejection after stream
admission closes the viewer without a successful done or secret; it leaves
readiness healthy rather than converting rejection into an infrastructure fault.

## Restart boundary

Prior-owner unfinished executions continue to fail closed through HTTP. This
increment does not silently adopt or dispatch them from a read request. A trusted
host may explicitly recover a known execution using the exact registered
definition and current actor/policy authority before exposing it through HTTP.
Restored pending grants/prompts keep their identities; restored accepted Ready
work does not require approval again when dispatched by the embedded service.
This HTTP slice resumes restored parked work only after explicit owner recovery;
it supplies no HTTP launch path for already accepted Ready continuations yet.
Trusted host continuation and automatic discovery/recovery supervision are
separate increments and must preserve these rules.

## Native wire decisions and evidence

Pinned source sends event:interrupt with its original permission envelope, and
wait drains those events and returns output or null. The native wrapper above is
documented native behavior until independently compared. Native thread status
remains interrupted for every stable park; preserving the source wait-idle quirk
across restart would require durable transport provenance absent from Store.
Denial output is likewise native. These differences do not close CP-005/009.

Native multi-fault order: authentication/structural parsing; coherent resource
policy; owner/state/route/current definition; complete set; unsupported Always;
native token conversion; service grant checks and atomic commit. Capacity and
thread claim are reserved before consumption. Record this table in tests; it is
not pinned TypeScript precedence. Unknown/inaccessible resources use the existing
generic404, and uniform grant_invalid has no diagnostic reason or token text.

Keep all historical receipts, raw workflow observations and 24 existing
comparisons. New provider-free grant reference recordings, if produced, are
separate actual pinned runtime evidence with exact source/build provenance and
before/after artifact inventory, never inferred from source or Rust tests.

## Qualification

Write meaningful failing tests first, then qualify constructor/mode/bounds,
stable whole-set pending disclosure, preserved siblings, Once/Deny/cancelled,
wrong-principal/policy/definition/route/set/token/TTL precedence, one concurrent
winner, capacity-before-consumption, actual transaction failure, disconnect
ownership, policy renewal and restart. Run actual TCP tests, original workflow
comparisons, strict lint/format, fixture/contract integrity and no-token example
verification. Core release and all broad scenarios remain open until their full
evidence exists.
