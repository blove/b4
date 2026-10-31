# Authorized scripted runtime boundary

Status: **required authentication and resource policy implemented for the bounded
scripted HTTP subset, including explicit required-grant continuation**. `HttpHost`
requires both dependencies at construction; there is no anonymous or allow-all
fallback. Arbitrary native tools, provider integrations and a tool sandbox remain
future work.

## Trusted inputs

Applications register literal route keys and immutable execution definitions.
Client input is data and cannot supply executable programs, provenance, ownership,
tenant scope, policy, checkpoint pointers or authoritative task identities. User
metadata is distinct from server-owned origin/access records.

Native tools are trusted application code with host privileges. Rust types do
not sandbox file or network access. Untrusted execution and provider integrations
are outside this scripted host. Its registered effects are finite local scripts
and every constructor rejects non-pure definitions. Ordinary `HttpHost::new`
also rejects approval-bearing definitions; `new_with_approvals` enables required
approvals using the same store and immutable registry.

## Implemented admission and disclosure

1. Apply transport bounds and authenticate the caller.
2. Resolve the resource in server-derived application/tenant scope.
3. Authorize the operation and all stored route provenance it reveals.
4. Validate the registered scripted definition and current resource revision.
5. Commit admission/mutation, then dispatch effects.

Authentication precedes body parsing for protected endpoints. Structural parsing
may precede resource resolution; sensitive resource diagnostics follow policy.
Thread and latest execution are read coherently, with immutable server-owned
application/tenant/principal origin and recorded route/revision available to
policy. Origin-less legacy threads fail closed even if a policy permits them.
Inaccessible and missing thread reads and unsupported scoped operations return
the same generic 404. No caller metadata can alter origin. Explicit approval
hosts additionally require the originating actor for run, pending and resume,
even if the supplied policy permits another actor. Ordinary GET visibility still
follows the supplied resource policy.

Admission compares the resource revision captured during policy evaluation in
the same transaction that admits the new execution. A competing admission or
execution change returns a conflict. Capacity is reserved before implicit
creation. Explicit approval hosts renew authority against a fresh token-free view
before new admission and compare the captured resource view. This prevents policy
revocation or a permissive policy from admitting work that later fails the stricter
origin-only grant boundary. The worker commits InFlight before invoking each scripted effect and
commits completion before output publication. Viewer detach and queue overflow
do not release the worker's permit/store ownership before quiescence.

Every non-probe router operation, including unsupported operations, requires
authentication and policy. The example requires `B4RUN_TOKEN`, binds IPv4 loopback,
permits exactly its listener Host and rejects Origin headers. Embedding
applications supply and qualify their own authenticator, resource policy,
listener and browser protections. A caller can deliberately supply a permissive
policy; the library cannot establish that policy's production adequacy.

Authentication headers are passed transiently to the authenticator and are not
stored in origin or snapshots. Public host errors use fixed generic codes rather
than raw store errors, callback failures or credentials. Authentication/policy
panics and execution/store faults fail readiness. Logging/redaction qualification
for future integrations remains open.

`ScriptEffect::Failure` messages are explicitly configured public application
content, bounded to 1,024 UTF-8 bytes and both escaped JSON terminal sizes. They
are saved as known settled outcomes before publication. Infrastructure exception
text, caller diagnostics and credentials never enter this path. Expected failures
keep readiness healthy; failed outcome commits make it fail and publish no known
failure terminal.

Ordinary hosts keep approval-bearing stored work unsupported on state/pending.
Explicit approval hosts disclose the complete stable pending set through
`GET /threads/{id}/pending_interrupts` after fresh origin/policy checks and both
resource/execution fences. A partial park returns a conflict rather than an empty
set or incomplete prompts. Pending JSON is `no-store`; approval-enabled SSE is
`no-store, no-transform`. Ordinary state has no prompt/grant fields.
Recovery-blocked and unsettled prior-owner executions fail closed on state/pending;
completed historical executions remain readable. HTTP does not perform recovery.
At that historical subset boundary, cancel, delete and GET attachment were
unsupported. Current deletion and owned cancellation have the explicit boundaries below;
Current bounded attachment has its separate authority boundary below.

## Owned cancellation authority

Authentication precedes capture of an identity-bound local control. That control
must match the initial coherent view before resource policy evaluation, and
renewed policy plus exact immutable application/tenant/principal origin precede
each atomic cancellation attempt. Neither a delayed view nor a delayed policy
may select a successor. Prior-owner work, stable parks and unowned durable work
cannot be adopted by this endpoint. Approval rows are validated and changed only
inside the store capability after the renewed authority boundary; consumed Once
and acceptance evidence stay intact.

Cancellation response and effect quiescence are separate boundaries. A committed
cancelled terminal suppresses later chunks and terminals, while the original
worker retains exclusion/capacity/store until its actual completion work ends.
Infrastructure faults put the gate into Faulted before waking callbacks, and
still make readiness fail after a cancelled response. Arbitrary callback
interruption, shutdown deadlines and actor/cause diagnostics remain unqualified.

Body/depth, route/task/attempt/output, active worker and viewer queue limits are
enforced. These per-request/per-run bounds do not bound retained execution
history, thread count, connections or every slow settled viewer for a long-lived
deployment. Those retention and operational limits remain open.

## Grants and continuation

The embedded approval service supports required grants; durable storage is the
default, with an explicit volatile fixture choice for MemoryStore tests. The
explicit HTTP constructor exposes required approval through this service, with
server-authored prompts, immutable policy revision and bounded TTL. Constructor
checks cover exact prompt/task coverage, canonical depth/bytes and aggregate
pending response size; caller input plus each prompt is checked before admission.
Off/optional modes remain future work; optional
mode must still require a token for an interrupt with a grant row when implemented.
High-entropy tokens bind thread generation, execution, interrupt, task, parked
revision and a canonical tool-proposal digest. All decisions and grant consumption
commit atomically with continuation. Reading pending does not consume a grant;
Current bounded attachment has its separate authority boundary below.

The kernel accepts already validated decisions. Its complete-set validation is
not grant validation, user authorization, replay protection or a durable receipt.
The separate native approval service supplies bounded Once/Deny transaction checks
after a required host authority port authorizes server-derived origin and policy
revision. It reads token-free resource context before grant records, then checks
resource/execution fences. Sensitive-record decode errors are fixed and redacted.
Approved dispatch renews policy and validates the consumed call binding.
HTTP resume first renews actor and policy against fresh token-free context, then
checks owner/state/route/current definition, exact unique pending set and supported
decisions. Native token conversion follows definition order; the service repeats
authorization and validates every grant before committing any consumption.
Capacity and a per-thread worker claim are acquired before acceptance. Conflicts,
missing/invalid/expired/consumed grants and policy denial keep readiness healthy
and disclose no sensitive diagnostic. Public statuses/codes are documented in
[compatibility](compatibility.md); exhaustive reference precedence remains open.

Wait park returns null without reading pending records merely to settle the turn.
Stream park emits authorized native interrupt envelopes before one null terminal.
Resume launches the accepted snapshot without another Start. Completed siblings
remain recorded and are not repeated. Deny/cancelled tasks do not invoke callbacks;
a final denied task has native `{denied:true}` output. Grant tokens never appear
in terminal/error envelopes. Detached or overflowing viewers have no terminal
delivery guarantee; owned work retains its permit and claim until stable park or
effect quiescence. An expected policy rejection after SSE admission closes the
viewer without a successful done and keeps readiness healthy.
`always` permission, when implemented, is limited to one thread generation,
principal/scope, tool and policy revision; it cannot bypass per-call checks.

If plaintext tokens are retained to support re-disclosure after restart, the
entire database and its backups are sensitive. A separate hash column does not
protect plaintext elsewhere. Tokens, credentials, raw tool input/output and
prompt content must be redacted from default logs and public errors.

## Disclosure and recovery

Protected reads, state, pending prompts, attachment, mutations and capability
discovery all need authorization. Attachment must authorize checkpoint, current
turn and parking provenance; unknown origin fails closed. A grant is not an
authorization credential and cannot become an existence oracle.

Admitted work retains server-derived scope and an actor/policy reference, never
raw HTTP credentials. A future recovering runtime must resolve current secrets
and recheck dispatch policy; unavailable or revoked authority blocks work. An
accepted decision remains recorded and is not refunded by policy revocation.
If policy revokes after the dispatch claim commits, the callback is not invoked
and the durable attempt remains conservatively InFlight. Recovery follows its
declared policy rather than refunding or silently replacing the accepted decision.
Known parked work may be explicitly recovered by a trusted host using the exact
definition and fresh authority before HTTP resume. Already accepted Ready work
can continue through the embedded service or supervised HTTP worker after trusted
known-ID recovery. There is no public AP launch endpoint or automatic scan.

Local HTTP/TCP tests exercise wrong-principal denial, forged metadata, protected
unsupported-route diagnostics, fresh policy renewal, stable complete-set grant
disclosure, refusal precedence, one concurrent resume winner, capacity before
consumption, viewer disconnection, explicit parked recovery, and actual SQLite
acceptance rollback. Separate provider-free pinned TypeScript captures exercise
39 selected grant requests; their limited repeatability projection does not
establish full native grant parity.
These do not complete the broad cross-tenant, grant disclosure/replay, permission
scope, production integration, restart authorization or shutdown release tests.

## Trusted known-ID recovery

The embedding application supplies verified server-owned Principal and an exact RecoveryTarget from a token-free coherent view. Recovery renews Operation::Recover policy before protected identity/definition diagnostics or sensitive rows. The transaction compares thread/execution IDs, immutable generation, exact Definition, old-owner execution fence and resource revision under the same lock/transaction. Only prior-owner unfinished work may be adopted; current live ownership, stale requests and policy refusal change nothing. Every resumed callback renews authority; committed Once is neither reissued nor refunded. Public HTTP reads and routes do not recover prior-owner work.
## Fenced deletion authority

DELETE authenticates and evaluates a coherent token-free resource context before
protected diagnostics. Existing threads require exact immutable application,
tenant and principal even under a permissive policy. Initially missing resources
use None origin/routes and require explicit missing-resource policy authorization.
The owned deletion worker renews current policy and compares the original view;
its transaction binds generation and both resource/execution fences. It cannot
retarget a successor. Expected refusals stay healthy. Corruption, panic or failed
cleanup sets the host fault inside the owned worker, including abandoned callers.
Success has an empty no-store204 body; grants and accepted audit are removed with
the complete deleted thread history.


## Bounded attachment read boundary

Attachment authenticates an exact immutable origin and recorded registered
Definition under Operation::Attach. It captures the original generation/execution/
turn/owner before initial policy, renews policy and reloads ordinary identity after
policy returns before disclosure. Async request ownership holds the unique viewer
guard; a blocked policy worker receives no guard. Capacity/overflow/state limits
remain private until authority passes. A subscription holds no store, execution
permit or worker claim. It receives only the selected turn's snapshot and tail.

Stable parked approval disclosure after reopen uses AttachmentPending authority
on a fresh token-free captured view before sensitive rows. The original stored
owner is validated without adoption; full generation/resource/execution fencing,
all historical grants/acceptances and current pending bindings must be coherent.
Reading preserves expired tokens, original TTL and audit; acceptance still rejects
expiry. Ordinary pending/resume owner rules remain unchanged. Unknown origin,
unavailable definition, stale target, orphan unfinished work and recovery-blocked
state never cause attachment to start or recover work. No SQLite schema change or
durable token-stream replay is introduced; schema version3 and migrations remain
unchanged. See bounded-run-attachment-design.md.

## Shared native callback authority

At the shared-driver receipt, HTTP permitted only configured finite Pure scripts. Trusted native embeddings
may supply `DriverHost` callbacks, which run only after a durably prepared exact
attempt. Preparation renews existing policy and full claimed-view authority on
the coordinator; sibling workers cannot commit this execution while authority is
being checked. The driver verifies the original thread generation, origin, full
definition, owner and invocation before callback entry and completion retries.

Callback errors and panics use static diagnostics, stop new scheduling and fault
publication before waiting for blocked siblings. Outcomes are bounded by serialized
UTF8 bytes and depth before entering the result queue. Entered callbacks and their
real results remain owned through drainage. Rejected preparation evidence cannot
authorize a cancellation acknowledgement for an entered sibling. Native callbacks
are trusted code and can allocate or block; there is no native-code sandbox or
shutdown deadline in this increment.

## Immutable native application authority and discovery

Every protected native HTTP and embedding recovery entrypoint verifies the
authenticated application's scope against the immutable registry before Store
reads, resource diagnostics or capability disclosure. Exact full definitions bind
callback selection; there is no latest-handler fallback. InvocationContext derives
from original accepted thread generation/origin and configured application,
execution and policy revisions. Client metadata/input never populate authority.

Optional native input validation runs only after ordinary resource authority,
active-run reservation and thread exclusion. The owned producer holds Store,
registry and guards through validator return even if its client disappears, then
renews policy and reloads original identity before mutation. The same approval
bridge renews authority before every native dispatch, including empty-prompt apps.
Validator/callback panic and uncertainty produce static faults; known public
application failures remain durable. Trusted Rust code is not sandboxed and can
block or allocate; no interruption or duration guarantee is implied.

Capability discovery additionally requires a route-free ReadCapabilities policy.
Denied or foreign callers receive no metadata or backend health diagnostics. A
worker fault during a held policy wait is rechecked before disclosure. The response
is no-store, bounded to65536 bytes and contains no routes/counts, policy revision,
prompts, tokens, captures or paths. Static declarations are neither health nor
permission promises. The native example verifier also suppresses malformed schema
instances and forcibly reaps an owned child that ignores termination.

## Admission closure and abandoned jobs

The supervised admission cut is a monotonic process-local operational boundary.
New protected requests after it receive healthy503 host_draining before auth, body,
policy or Store work; no identity, resource or capability disclosure occurs. Public
healthz stays200 and readyz returns status-only503 without touching the Store.
Serving requests retain normal authentication and native application scope checks.

Each protected request registers ownership before its first await. Authentication,
policy, readiness, token-free view reads, pending disclosure and mutation producers
retain the same group after caller abandonment. Infrastructure faults and panics
are classified and latched inside the job, with static public diagnostics. Healthy
denial, invalid application input and a lost mutation lease do not become faults.

Lifetime registration is distinct from mutation admission. Operations whose lease
won the cut retain their service-internal authority renewal and CAS behavior; other
preflights cannot create/admit, accept a grant, adopt, delete or begin Pending cancel.
The supervisor mutex is never held across application code, Store calls, claim locks
or RunControl Pending waits. Durably admitted driver work finishes under existing
authority, outcome and recovery fences; shutdown does not invent Cancelled results.

Bounded waiting neither interrupts trusted callbacks nor resolves their uncertainty.
A faulted quiescent host may retain durable InFlight evidence. Descriptor v2 reports
the partial drain mechanism and fixed wait bounds while graceful_shutdown remains
false. No new shutdown route, callback signal, deadline or sandbox is introduced.


## Sensitive backup artifacts

SQLite backups carry retained prompt/grant material as well as consumed acceptance
records. Export protects stage/final files from creation with Unix0600 and never
records path, SQL, snapshot, prompt or token data in error values. The canonical
existing trusted parent must lie outside the source data directory/subtree; bound
absolute paths avoid SQL or URI interpretation. Atomic hard-link publication
refuses existing files, empty files, directories and symlinks, including concurrent
creation. Private cleanup is best-effort; a postlink failure retains the final
artifact and reports PublicationUncertain. Callers must protect any retained stage
and must not infer absent output or overwrite on retry.

Unique mutable Store access does not establish the embedding's source-retirement
contract or exclude trusted raw SQLite connections. Controlled restoration requires
a final quiescent cut, cessation of those writers and permanent source retirement
before opening a copy. Copied epochs and separate directory locks cannot fence a
live source elsewhere. Restored authority still requires exact generation/full
Definition and current policy; historical copies can resurrect prior unconsumed
grants and omit later external effects. No arbitrary rollback or clone/failover
safety claim is made. Structural backup validation does not validate every semantic
payload or prove artifact authenticity against a filesystem administrator. This
slice assumes a trusted local filesystem parent and does not defend hostile ancestor
replacement or claim Windows ACL/network-filesystem protection.

## Standalone native demo process

The private b4run launcher requires an explicit compiled --demo selection and one
bounded visible-ASCII B4RUN_TOKEN supplied by environment. Its loopback browser
middleware requires one exact actual address:port Host and no Origin; protected
operations require one exact Bearer value. Input never supplies authority. No
arbitrary loader, process-control endpoint, token flag/file, remote ingress or
machine-wide authentication guarantee is added.

Config-check is readonly filesystem/application/configuration validation; it
neither binds nor opens SQLite or acquires ownership. Private Unix0700 data and
0600 regular nonsymlink database/owner/sidecar files are launcher preconditions.
Only newly reserved files and a newly created leaf are permission-repaired, under
a trusted stable canonical parent. Existing entries are never truncated, unlinked
or chmod-repaired. No hostile-directory replacement, UID/ACL ownership or Windows
profile is inferred from these local mode checks.

The process owns its executor and waits for registered physical Quiescent plus
successful joined Axum transport retirement before dropping host captures and
normally destroying that executor. Incomplete work or transport retains SQLite;
repeated signals and expired observation intervals never force release. A lost
server witness retains indefinitely until external manual termination. Failed
server results remain owned without opaque panic-payload disposal; first-event
admission closure precedes result inspection/diagnostics. Closed diagnostic pipes
cannot unwind the supervisor or panic hook. Diagnostic writes may block on a
stalled reader; no bounded diagnostic/shutdown latency is promised.

Static diagnostics avoid argument/environment/path/SQL/prompt/token/panic-payload
formatting. Trusted callback code remains able to write its own output. Tests use
private child fixtures, actual signals/network/SQLite and responsive async pings;
none of their barriers, abort controls or paths are compiled into the executable.
No clean retirement claim arises from forced SIGKILL cleanup or OS process exit
alone: tests reacquire SQLite after runner return while the child is still alive.

## Scoped local Linux container

The separate Linux/arm64 profile qualifies the native exec PID1 at UID/GID10001,
read-only root, network none and inner127.0.0.1:3001. The exact task-owned local
named volume is explicitly mounted nocopy; packaging verifies its0700/10001 parent
before runtime start, then actual data0700 and regular nonsymlink files0600. A
separate qualification-only UID0 initializer with CHOWN changes only a verified
fresh empty root; it refuses hidden/nonempty data without repair. Driver commands
use / as their explicit working directory, with no additional capabilities.

Generated bearer tokens remain runtime-only in private0600 temporary configuration
and captured request pipes. Qualification selects metadata at the Docker boundary;
it never publishes raw inspect/env/responses/grants or authority-derived digests.
Daemon administrators can access environment secrets. Trusted daemon/local stable
paths are assumed, with no hostile-path, public-ingress or deployment claim.

Actual SIGTERM/SIGINT exit0/stopped/notOOM qualify clean runtime retirement. Forced
cleanup is recorded as failure or diagnostic cleanup only. Packages are inventoried
and expected slim omissions classified; this is not a security audit or proof of
pristine packages. See the [separate receipt](../../conformance/expected/linux-container-verification.json).

## Redacted stopped inspection

Local inspect possession of a private directory selects a separate filesystem
reader, never a network authorization bypass. It consults only B4RUN_DATA_DIR;
it creates no application, token policy, callback, listener or signal receiver.
Existing source0700/regular600 entries and same-inode cooperating ownership are
required. Any sidecar entry refuses unchanged; immutable mode cannot protect
against raw writers or administrators bypassing the covenant. Stable trusted
parents are assumed, without hostile-image, ancestor-replacement, UID/ACL,
network-filesystem or Windows protection claims.

Before preparing SQL, the reader installs verified safe engine limits,
DEFENSIVE=true/TRUSTED_SCHEMA=false and a shared nonpanicking progress budget.
Only fixed SQL and an internally percent-encoded URI are accepted. Exact schema
comparison precedes data queries; borrowed JSON shape/byte guards precede one
owned Snapshot decode. The report uses only fixed enum/metadata constants,
booleans and counters. It exposes no path, source digest, epoch, stable identity,
values, prompts, tokens, decisions, failure text or authority-derived digest.
Error Display/Debug and CLI stderr remain static without nested source errors.

Reader close/unlock uncertainty suppresses output and intentionally retains the
exact uncertain resources and owner descriptor until process teardown. Normal
safe output starts only after reader retirement; broken output uses a static
nonpanic exit1 and may leave already-written safe bytes. Actual stalled-output
qualification proves a real writer can acquire ownership while the inspector
still lives. No atomic output, hard I/O latency or total RSS guarantee is made.
Deferred payload semantics and redacted counters cannot authorize recovery or
prove effect safety. See the [runbook](stopped-readonly-inspection.md).

## Request references and bounded records

The native fallback observer reads only a fixed method label and constructed
response status. Server-generated references use one fallible OS seed per host
and a nonwrapping checked counter; inbound references and authority values are
never copied. Public namespace/counter values permit same-host linkage and
sequence observations, without authentication or authenticity guarantees.

The fixed256-record ring and compact65536-byte export exclude URI, address,
application/tenant/principal/execution/task identities, prompts, grants, tokens,
inputs/outputs, raw errors, SQL and panic payloads. No diagnostic endpoint, sink
callback or automatic launcher log delivery is added. The embedding controls
where encoded bytes go. Contention/poison/eviction/encoding loss is best effort;
separate atomic metadata samples do not provide complete or atomic history.

Response construction and actual pending-guard destruction are distinct from
body delivery, approved actions, cancellation and physical retirement. Neither
event authorizes runtime or storage work. Handles/batches capture no Store or
shutdown lease. Static errors and private entropy/encoding seams are documented
without OS outage, total RSS or hard I/O timing claims. Full operational identity,
lifecycle and delivery qualification remain open. See the
[embedding runbook](request-diagnostics.md).
