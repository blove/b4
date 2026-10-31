# Unchanged repository AP client corpus

Status: implemented and qualified for two fresh original-client/native runs on
macOS arm64 after complete independent component and actual-evidence reviews.
See [the scoped receipt](../../conformance/expected/unchanged-ap-client-native-verification.json).

## Selection and scope

The user's continuous-build request authorizes local reversible development.
The attached RFC supplies requirements, including CP-010, rather than separate
instructions to deploy, publish, contact anyone or obtain credentials. The optional
corpus preference is unanswered; use the repository-owned callers already pinned
to commit8acac534ba6eb960b34de217b0da45b8405023af. The user's Dawn checkout remains
untouched. No Git mutation, provider call or package download is selected.

Execute the original controller and worker factories against a real Rust native
host. Equivalent handwritten HTTP probes would exercise the API but miss caller
serialization and parsing. A full software-factory app would also require business,
workspace and provider behavior outside this increment. Selected original callers
provide a bounded first characterization of actual AP use.

Call all11 controller methods and8 AP worker methods:19 selected factory methods across
nine core operations. AP-004 ReadState, worker uploadSource/workspace options,
capabilities, AG-UI, consumeTurn, other callers, full factory business behavior,
TypeScript-host parity and broad parser/rechunk/retry matrices remain separate.
Not every method is expected to succeed. This corpus measures incompatible
permission parsing and grantless resume explicitly without changing the protocol.

Canonical operation mapping is worker create/get/delete/pending/start/reattach/
resume/cancel -> AP-001/002/003/005/007/008/009/010; controller workflow methods
use AP-006 and interrupt uses AP-010. Resume is AP-009, cancellation AP-010.

Actual original-client execution initially covers macOS arm64 with the installed
Node24.20.0, esbuild0.28.1 and Zod4.4.3. Rust fixture/unit and retained checks can
also cover Linux arm64; they do not imply original-client execution on Linux.
No other runtime/version/platform or deployed service qualification is inferred.

The predecessor diagnostics receipt4917f278... has final independent audit
ae3ee204... . Before this public write, a542-artifact baseline and538 protected
artifacts were captured. All182 existing crate paths, manifests, lock/toolchain,
production code, old qualifiers, AP contracts, recordings and historical receipts
stay exact. Only README, compatibility, requirements and checker may receive
explicitly reversible selected documentary/integration edits. No dependency or
feature change is planned. The stopped-inspection receipt89dfb6a... stays frozen.

## Original source and separate bundle provenance

Build exactly these two entrypoints from the source-pinned private checkout:

- examples/software-factory/controller/src/lib/client.ts
- examples/software-factory/controller/src/lib/worker/client.ts

Import their actual exported factories and error classes. Worker wire.ts and
sse.ts remain original transitive inputs. Type-only imports are erased by esbuild;
successful bundling is not a controller app typecheck or business validation.
Use two fresh private output directories for two complete builds, with explicit
installed compiler/runtime paths, platform=node, format=esm, bundle=true,
no plugin, alias, source rewrite, minification, source map or response adaptation.
Zod inputs must be the exact installed4.4.3 files; permitted external imports are
selected Node builtins. Verify the Git pin/blobs, lock/package/tool versions,
compiler binary, every metafile input and output, and unchanged relevant inputs.

The full pinned tree and385 historical built artifacts retain independent private
before/after integrity checks. New client bundle manifests describe only the
new builds; they never inherit the385-artifact runtime provenance claim. Public
provenance contains admitted source/compiler/package/bundle digests only.

## Real native fixture and fixed outcomes

A new qualification example owns a real SqliteStore, immutable NativeApplication,
HttpHost, authenticator, resource policy, worker/kernel and RequiredDurable approval
bridge. It binds127.0.0.1 at an OS-assigned port and launches one Node runner per
fresh corpus run. Two runs use fresh private stores and distinct client bundles.
There is no product control endpoint, mock HTTP response or new production API.

Register all ten literal factory workflow routes and dedicated worker value,
void, failure, held and approval-park routes. Nine factory routes return authored
RouteOutcome-shaped values. The fixed /work-orders/cancel#workflow binding is
OutputKind::Void and returns null through original controller.cancel. These are
fixture transport outcomes, not factory business semantics. Factory create uses
catalog-shaped taskId/operationKey input; approval inputs use finite synthetic
revision/digest-shaped values, retained privately.

The single immutable dispatch callback has a finite authored operationKey table:
ordinary returns Value; fixture-known-failure returns NativeOutcome::Failed.
Read that table from EffectInvocation.input["input"]["operationKey"], the actual
native invocation envelope, rather than a fabricated top-level business input.
Private control can arm the next fixed dispatch callback to hold for cancellation
or abandonment cases. It cannot upload code or change the registry. OutputKind
is immutable per binding: true Void is qualified through controller.cancel and a
worker Void route, never silently replaced by Value-null. Native Void wait is
JSONnull and stream done{}; known failure wait is500 execution_error with message
and no details.code. Observe original caller values/errors rather than inventing
an error code or translating a body.

The worker_held registration has one native held callback. Native Value callbacks
do not publish ScriptEffect::Chunks frames. Poll the original held POST iterator
and settle its pending next using its actual signal or explicit release/terminal,
then await iterator return or consume EOF. First-frame cuts while work remains
held use original live reattach and its actual state prefix. Do not assume POST
begins with a chunk or reattach's state frame, or return an unpolled POST as proof.
Fixture max_active_runs/max_concurrent_tasks are4 so a second same-thread request
can reach actual claim refusal rather than only global capacity exhaustion.
Record the native thread_busy code as returned; never rename it run_in_flight.
Input/output limits are65536 and task counts are finite; no product defaults change.

Controller default fetch has no bearer option: execute a direct local401 first.
Positive calls use its documented fetch injection to add only the generated
fixture Authorization header. Record original and forwarded headers privately.
Worker uses its own original token handling. The injected transport checks the
exact fixture origin and rejects redirects, declared as a harness safety setting;
it does not rewrite URLs, bodies, signals, returned Response objects or streams.
Direct default fetch is limited to the known nonredirecting loopback request;
this is not universal egress isolation. Child environment is explicitly cleared
and populated without provider credentials, proxy settings or Node preload flags.

## Eight executed groups

| Group | Original caller observations and actual native witnesses |
|---|---|
| C1 factory method map | All ten factory workflow methods retain exact literal route/input production; nine return authored Value and controller.cancel returns true Void null. |
| C2 controller auth | Direct default fetch receives401; bearer-only documented injection succeeds. Original versus forwarded request headers are retained privately. |
| C3 wait terminals | Original dispatch receives Value and known failure500; original controller.cancel receives Void null. Failure keeps original ControllerHttpError with undefined code. |
| C4 held controller | Actual dispatch callback entry/InFlight before busy refusal, signal abandonment and selected original interrupt. Abort/cancel settlement is separate from held physical ownership; explicit release and later retirement are required. Authorized idle/missing cancel409 and policy-concealed404 reductions are separate. |
| W1 worker CRUD/auth | Original createThread metadata-only, getThread and deleteThread; wrong token, missing/concealed reads and held deletion refusal use real auth/policy/store paths. Workspace/upload is absent. |
| W2 finite SSE | Original startRun and parseSse consume actual Value, Void and known-failure streams through EOF, preserving native frame shapes. |
| W3 attachment/cancel | Original live/settled reattach, a declared viewer cut or abort, original cancel and continued exclusion while the actual callback remains held. ParseSse must actually acquire/poll its reader; do not return an unstarted or blocked iterator as a cleanup witness. |
| W4 durable park/refusal | Real RequiredDurable park: empty pending parses, nonempty native pending rejects the original permission-request schema. Normal caller flow stops at that rejection. A separately labeled forced probe passes only the actual interruptId from private token-free Store observation to original resume once/deny; it has no grant and expects400 grant_required. Empty resolutions expect400 invalid_request. Real parked/acceptance state remains unchanged; no authority is repaired or submitted. |

The original generator is lazy: return before first next acquires no body reader,
and return behind a blocked next cannot cancel that read immediately. For viewer
return cuts, consume an actual frame first. An actual AbortSignal may instead
settle a polled blocked POST read before awaited return; no first POST frame is
required for this signal path. Settle every pending next before return.
Watchdog expiry is failed cleanup,
not proof of successful caller cancellation or retirement. Method invocations,
eight groups, subcases, two runs and unit tests have distinct counts.

For W1, obtain the original held POST iterable, observe entry/delete refusal,
explicitly release, then fully consume its original terminal/EOF. W3 starts a
real pending POST read plus a live GET attachment; its first state frame precedes
an awaited attachment iterator return. Resolve the original pending POST via
its actual AbortSignal before awaited return, while the native callback remains
held. A separate held generation tests original cancel, consuming its actual
terminal/EOF before explicit callback release. Later settled reattach is separate.

## Passive bounded socket capture

Use the locked axum Listener interface around the actual TcpListener/TcpStream.
The wrapper delegates original accept/local_addr and AsyncRead/AsyncWrite poll
results, including vectored writes, flush and shutdown. Copy only newly filled
ReadBuf bytes after successful read and only the successful first-n write prefix;
Pending, error and zero-byte writes add no bytes. No Response clone/tee, second
body consumer, reconstruction, adapter or extra stream pull is allowed.

Preallocate16 cumulative connection slots per entire run, each with262144 bytes
per direction:8MiB payload storage shared between active and retained captures.
Slots never recycle in that run. Local poll copying does not allocate, grow a
buffer, acquire a global capture lock, await a sink or perform persistence I/O.
Exceeding the byte/slot cap sets sticky evidence loss while preserving original
transport behavior; it fails the harness rather than refusing product traffic.

CaptureIo owns Option<TcpStream>. On Drop, explicitly take/drop the stream before
try_send of its owned buffers through a capacity16 std sync_channel. Successful
transfer precedes the retired counter increment. Full/disconnected channel is
sticky failure. This is nonblocking capacity refusal, not wait-free synchronization.
Axum's close_rx marker can close before connection-local object destruction, so
server join is not a socket disposal witness. Dispose all listener/server/sender
aliases; require accepted==retired<=16, exactly that many received records and
sticky_loss=false. Captures contain no Host/Store/application resources and may
remain alive during the later Weak and writer-reacquisition checks.

Captured write bytes prove local socket acceptance, not remote receipt. Original
caller results, errors, consumed frames, EOF and declared cuts are separate facts.
Offline bounded HTTP1 parsing handles pooled multiple messages, request/response
queues, Content-Length, chunked data and final trailer CRLF, bounded headers and
no-body statuses. Attribute overlapping held/busy/cancel/attach calls using private
case/thread/input keys and per-connection ordinals, not request=connection or timing.
Do not reuse old close-only decoders that stop early at the zero chunk.
Capture completeness differs from message completeness: planned cuts can leave
partial HTTP/SSE framing with a complete capture; unexpected partial/malformed
framing fails. Offline decoded bytes are never fed to the original client.

## Bounded process control and positive retirement

Rust main owns std::process::Child and periodically uses try_wait under an
independent monotonic watchdog. A dedicated control std thread owns its pipe
reader/writer; a separate bounded stderr reader prevents child pipe blockage.
No Tokio process/io-std features are added. Node stdout is only version1 control;
Node stdin carries replies. One outstanding request, increasing checked ordinal,
fixed operation/case enums, at most256 commands,8192 bytes including newline per
direction/line. Incremental reads and serialization enforce the cap; unbounded
read_line/read_until/read_to_end followed by a length check is invalid. Reject
unknown fields, duplicate/out-of-order ordinals, partial final lines and chatter.

Operations are ready, arm, await-entered, inspect-thread, inspect-pending,
gate-status, release and final-stop. Selected Store reads are token-free/fixed
metadata; no arbitrary SQL, row writes, recovery, grant submission or execution.
The control path stays independent of held callbacks and HTTP promises. Gate state
tracks generation, actual entry/return, explicit versus cleanup release and sticky
expiry using an absolute deadline across wakes. Timeout followed by release must
still fail. RAII releases all gates on failures, classified solely as cleanup.

The concrete private request envelope is {version:1, ordinal, case, command};
case is setup/C1/C2/C3/C4/W1/W2/W3/W4/retirement. command is exactly one externally
tagged object: ready:{}, arm:{gate}, await_entered:{gate,generation},
inspect_thread:{thread_id}, inspect_pending:{thread_id},
gate_status:{gate,generation}, release:{gate,generation}, or final_stop:{}.
Gate enum is controller_dispatch/worker_held. Empty and payload command objects
reject unknown fields; no flatten-plus-deny_unknown_fields ambiguity is introduced.
The reply is {version:1, ordinal, ok:true, result} or
{version:1, ordinal, ok:false, error:<fixed control code>}. No dynamic error text.
ready occurs once in setup; final_stop occurs once in retirement, after all gates
are explicitly released/returned. Generation and case/phase checks reject stale
or incorrectly selected commands. Gate states include actual entry/return,
explicit_release, cleanup_release and sticky expired flags.

ready result supplies base_url, token and the five static worker route keys.
The routes object keys are worker_value, worker_void, worker_failure, worker_held
and worker_park, with respective literal values /fixture/worker-value#workflow,
/fixture/worker-void#workflow, /fixture/worker-failure#workflow,
/fixture/worker-held#workflow and /fixture/worker-park#workflow.
Rust example arguments are node_executable, runner, bundle_dir, output_dir;
it starts Node as runner bundle_dir output_dir/evidence/client. The approved
builder writes controller-client.mjs, worker-client.mjs and bundle-manifest.json
in each fresh bundle_dir. Runner imports exactly those entries and verifies the
manifest. Child token/origin travel only through the private ready reply.
arm returns generation. Entry/status/release replies supply that generation and
fixed gate flags. Thread/pending replies supply found, bounded execution/task
status/revision/owner metadata and actual interrupt_ids only. Full thread/approval
views may be recorded in bounded private snapshot files by the parent for exact
before/after park/acceptance comparison, never in a control reply or client grant.
All request/reply fields, raw snapshots and returned dynamic IDs remain private.

After positive retirement, capture files are conn-NNNN.read.bin and
conn-NNNN.write.bin with a version1 capture-manifest.json. Each entry has ordinal,
read_file, write_file, exact read_bytes/write_bytes, read_eof and write_shutdown;
top-level metadata has accepted, retired and sticky_loss. Neither shutdown flag
nor framing completion alone means socket retirement. Node writes version1
client-observations.json containing eight named groups, static invocation ordinals,
private method arguments/results/errors/consumed frames and EOF/cut acknowledgments.
Native proof and snapshot files are separate. Root offline qualification validates
these interfaces against actual bytes and constructs public allowlisted summaries.

Limits are64 parsed frames per client stream,1MiB stderr per run,32MiB total
persisted private evidence serialization per run,30-second gate lifetime and120-second run watchdog.
Control entry/return waits and final server/socket/Weak/writer stages each use a
finite at most10-second deadline; no hard OS scheduling/I/O timing guarantee is
inferred. Cap/timeout/persistence failures abort evidence success and clean up only
the owned child and fixture resources.

The evidence quota has disjoint pre-write authorities: Rust owns24MiB under
evidence/native and Node owns8MiB under evidence/client. Neither writer borrows
the other's unused quota. Both reserve exact encoded/append bytes before opening
or writing a create-new artifact; failed writes consume the reservation and fail
the run. Rust's quota includes capture payloads/manifests, control transcripts,
stderr, authority snapshots, native proofs and all native evidence metadata;
Node's includes arguments/returns/errors/frames and all client evidence metadata.
No third artifact writer is permitted. Before success, independent directory
accounting agrees with both ledgers and their sum<=32MiB. A cross-component
overflow regression must show one exhausted role refuses its next byte before a
write even when the other has unused space. The separate SQLite runtime store and
source/compiler/bundle directories are outside this evidence serialization cap;
no filesystem-wide or SQLite total-disk bound is inferred. They remain private.

Success requires this order:

1. Positively observe each held callback and actual Store InFlight while admission
   is Serving; observe real exclusion rather than constructing wait_quiescent.
2. Settle original caller promises/readers/cuts, explicitly release each gate and
   witness callback return with expired=false and cleanup_release=false.
3. Node writes bounded private results and final-stop completion/ack, then exits
   normally and is reaped0. Final-stop does not close host admission. Require real
   pipe EOF and positive control/stderr thread joins; dispose observer aliases.
4. Close admission, signal graceful server shutdown, normally join the server and
   require Quiescent{faulted:false}. Independently require capture completion.
5. Drop all Host/Router/Store/application/config observer aliases and resolved
   futures; both Weak references must be absent while capture bytes remain alive.
6. Actually reopen SqliteStore, retrying only AlreadyOwned within the destruction
   gap deadline, and verify owner generation advancement/readiness and real selected
   terminal/park state. Persist captures only after transport/control/server retirement.

Failure kill/reap, server abort or process teardown never substitutes for success.

## Privacy, evidence and verification

Parent creates fresh0700 directories and create-new0600 private files. Raw HTTP,
Authorization, IDs, prompts/grants, client errors/stacks, timestamps, control and
authority snapshots and their derived hashes stay private. Public summaries are
constructed from closed typed allowlists of static group/method/AP names, statuses,
known error classes/codes, frame/result shapes, counts and declared seams. Unknown
or arbitrary fields fail publication; substring scrubbing is insufficient.
Only admitted code/compiler/package/bundle/source artifact hashes are public.

Before SqliteStore::open, reserve the fresh runtime store's owner.lock and
runtime.sqlite3 as empty create-new0600 files. Existing Store open does not choose
private modes itself. Verify actual directory/file modes and any live SQLite
journal/WAL/SHM sidecars after open and at final reopen; never chmod or replace
an existing foreign artifact. The runtime store remains outside serialization
quotas while its actual privacy modes and grant-bearing bytes stay private.

Implement the new harness units through meaningful compiling behavior RED/GREEN,
then complete independent SPEC followed by QUALITY review of every authored unit.
Run the original clients twice with fresh bundles/stores after fixture review;
validate passive captures and compare only stable selected observation shapes after
actual caller consumption. Preserve raw counterexamples privately. New native
observations do not imply TypeScript-host parity or19 positive-method successes.
Fresh appropriate retained Rust/Python/Node/contracts and separate actual platform
checks bind final sources; count declarations without helper/platform/run repeats.

Keep all56 broad scenarios not_run, M0 open, CP001–009 partial, CP010 unresolved,
M5 partial and core profile unfrozen. Publish only safe local artifacts, with a
separate independent final receipt audit and no self-referential checksum. The
next work continues after the scoped increment; no routine human handoff is needed.
