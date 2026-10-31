# Unchanged AP callers against the native fixture

Status: two fresh original-client/native runs qualified on macOS arm64, including
their stable consumed-shape comparison and independent actual-evidence audit.
The [scoped receipt](../../conformance/expected/unchanged-ap-client-native-verification.json)
links both safe summaries. Synthetic harness checks remain separate evidence. See the [accepted design](unchanged-ap-client-corpus-design.md),
[implementation plan](../../docs/superpowers/plans/2026-10-02-unchanged-ap-client-corpus.md)
and [ADR0022](decisions/0022-unchanged-ap-client-native-corpus.md).

## Selected callers and meaning

This corpus uses the original controller and AP worker factories from source
commit `8acac534ba6eb960b34de217b0da45b8405023af`. The selected runtime is macOS
arm64 with Node 24.20.0, esbuild 0.28.1 and Zod 4.4.3. Original-client execution on
Linux is not run; Linux Rust harness checks, if retained separately, do not change
that status. Other platforms, package versions and deployed hosts are outside
this corpus.

Each run selects all 11 controller methods and 8 worker methods: 19 methods across
nine core operations, with 63 method invocations in eight groups. These counts
describe selected calls, including expected refusals. They are not 19 successful
methods, independent test declarations or a full software-factory scenario.

| Group | Calls per run | Selected observations |
| --- | ---: | --- |
| C1 | 10 | Ten literal factory workflow routes and their original request bodies |
| C2 | 2 | Controller default HTTP 401 and documented bearer injection |
| C3 | 3 | Native Value, true Void and known-failure wait results |
| C4 | 11 | Held controller work, busy refusal, actual signal, interrupt and reductions |
| W1 | 11 | Worker metadata CRUD, auth, missing/concealed reads and held-delete refusal |
| W2 | 6 | Create plus original finite Value, Void and failure SSE consumption |
| W3 | 13 | Live/settled attachment, viewer cut, actual signal, cancel and continued exclusion |
| W4 | 7 | Durable park, original pending-schema rejection and grantless resume refusals |

Worker create/get/delete/pending/start/reattach/resume/cancel map to
AP-001/002/003/005/007/008/009/010. Controller workflow calls use AP-006 and
controller interrupt uses AP-010. AP-004 ReadState, worker upload/workspace
options, capabilities, AG-UI, consumeTurn and other callers remain separate.

The fixture owns a real native application, SqliteStore, HTTP host, authenticator,
resource policy, worker/kernel and RequiredDurable approval bridge. Its finite
route registrations return authored transport values. They do not perform factory
business, workspace or provider operations. True Void is an immutable route
binding: controller wait returns JSON null and the worker done frame has an empty object.
Known failures retain the original caller error and native body rather than a
rewritten response. Held deletion returns native `run_in_flight`; a competing
run reaches native `thread_busy`. Those refusals have distinct meanings.

## Source, caller transport and private execution

Two fresh bundle directories and two fresh stores are required. Each offline
bundle build admits the exact original entrypoints and transitive worker wire/SSE
and installed Zod inputs. Compiler options contain no source rewrite, alias,
plugin, minification, source map or response adaptation. The new manifests bind
their own source/tool/input/output bytes. Full pinned-tree and 385 historical
artifact checks are separate; historical build provenance does not become new
execution evidence.

The native example takes four absolute positional arguments: the Node executable,
the reviewed runner, one fresh reviewed bundle directory and one fresh private
output directory. It binds an OS-assigned loopback port and launches Node with an
explicitly cleared environment. Its private ready reply supplies the fixture
origin, token and fixed worker routes. Review and actual-run authorization gates
precede invoking it. The runner's control stdout is not a public report channel.

Controller default fetch supplies no bearer token and must first observe the real
401 response. Positive controller calls use the factory's documented fetch
injection, adding only the generated fixture Authorization header. The injected
transport checks the exact fixture origin and refuses redirects; this is a
declared harness setting. It preserves URLs, bodies, signals and returned Response
objects/streams. Direct default fetch is limited to the known nonredirecting
loopback call, without a general egress-isolation claim. Worker token handling
remains original. No provider credentials, package download or external service
is needed.

Raw requests, forwarded headers, results/errors, IDs, consumed frames, control
history and full authority snapshots remain in owner-only private evidence.
Directories must be fresh 0700 and evidence files create-new 0600. Before opening
the runtime Store, reserve empty 0600 owner and main database files; verify actual
database/lock and any SQLite sidecar modes at open and final reopen. Existing
ambient Store creation modes alone are insufficient. No foreign path is repaired
or chmodded to obtain admission.

## Preserve the permission counterexample

The approval-park route uses actual RequiredDurable native authority. Empty pending
interrupts parse through the original worker client. Its nonempty native envelope
has `interruptId`, `resumeKey`, `value` and `grant`; the original caller expects its
permission-request schema and must reject with ZodError. Normal caller flow stops
at that rejection. This is a retained compatibility counterexample, not a passing
permission handshake.

A separately declared forced probe receives only the actual interruptId from a
private token-free Store observation. It passes that ID to the original resume
method with payload once or deny and no grant. Both calls must return HTTP 400
`grant_required`; empty resolutions must return HTTP 400 `invalid_request`. Schemas,
request bodies and responses remain unchanged. The probe submits the interruptId
through the original request body and supplies no grant or resumeKey. Complete selected
thread/execution/grant/acceptance views must remain equal before and after every
refusal, and the parked views must survive final writer reopening. A metadata-only
check or the presence of a 400 status alone is insufficient.

## Reader cuts and physical retirement

Passive capture delegates real native socket reads/writes and copies only bytes
accepted by successful operations. It never clones, tees or independently drains
a Response. Captured write bytes establish local socket acceptance; original
caller results, consumed SSE frames, EOF and declared cuts establish separate
facts. A complete capture can contain an intentionally partial message after a
declared cut. Unexpected malformed/partial framing or evidence loss fails the run.
Pooled connections can carry multiple requests; call and connection counts differ.

Original async generators are lazy. Returning before the first next acquires no
body reader, and return queued behind a blocked next cannot cancel that read by
itself. A viewer-return cut consumes an actual live GET state frame first. The
held native POST supplies no promised initial frame: its actual AbortSignal may
settle a polled pending next, after which iterator return is awaited. A separate
cancel case consumes terminal/EOF before the still-held callback is explicitly
released. Watchdog expiry, abort request or stream detachment does not establish
physical callback return or owner release.

Success requires the following order:

1. Observe real held callback entry and Store InFlight while the host is Serving,
   plus actual exclusion/refusals.
2. Settle original promises/readers/cuts. Explicitly release every gate and observe
   its return with no expiry or cleanup release.
3. Finish bounded client evidence and the final-stop acknowledgment. Node then
   exits normally and is reaped with exit code 0; actual control/stderr EOF and thread joins follow.
   Final-stop does not close HTTP admission.
4. Close admission, gracefully shut down and normally join the server. Require
   Quiescent with faulted=false and independently complete socket capture.
5. Drop host/router/Store/application/config observers and resolved futures.
   Both Store and application Weak references must be absent while captured bytes
   remain alive.
6. Actually reopen the SQLite writer, verify owner advancement/readiness and the
   selected terminal/park state, then persist retired captures.

Kill/reap, server abort, cleanup release or process teardown is failure cleanup,
never a replacement for these positive witnesses.

## Bounds and public artifact

The run has at most 16 cumulative socket slots, each capped at 262144 bytes per
direction, and 64 parsed frames per client stream. Control has at most 256 commands,
one outstanding command and 8192 bytes including LF per line/direction; stderr is
capped at 1 MiB. Gates have 30-second lifetimes and the run watchdog is 120 seconds.
Entry/return and final server/socket/Weak/writer waits have deadlines of at most 10
seconds. These logical deadlines do not promise hard OS scheduling or I/O bounds.

Private evidence serialization uses disjoint pre-write quotas: 24 MiB for Rust/native
and 8 MiB for Node/client. Exact encoded/append bytes are reserved before artifact
opening/writing. Failed writes consume their reservation and fail evidence; roles
cannot borrow spare quota and there is no third writer under these directories.
Independent final byte accounting must match both ledgers and total at most 32 MiB.
Runtime SQLite, source/compiler and bundle directories are outside this cap.
It is not a total SQLite, filesystem or memory bound.

The read-only qualifier admits both private roles and validates full native
control/authority/retirement and client-versus-capture facts before calling the
closed [projector](../../conformance/reference/client-fixture/projection.mjs).
The [public schema](unchanged-ap-client-corpus.schema.json) specializes its output
to this fixed 63-call catalogue. It requires eight ordered groups, all 19 selected
methods, fixed group counts, closed observation/frame labels, all 16 retirement
fields and accepted=retired in 1..16. JSON arrays have an item at every position;
the projector separately refuses sparse/accessor JavaScript arrays before mapping.

Only fixed names, statuses, known error classes/codes, result/frame shapes, counts,
declared seams and scope remain public. The fixed source commit is code provenance.
No raw credential, ID, prompt, grant, error message/stack, private path, timestamp,
authority content or digest of those inputs appears. Separate admitted
source/compiler/package/bundle provenance may be published. Schema conformance
alone does not authenticate an original call, capture, authority or retirement,
and synthetic positive/negative artifact checks are not original-client runs.

M0 remains open, CP001–009 partial, CP010 unresolved and M5 partial. All 56 broad
scenarios remain not_run and the core profile remains unfrozen. Full factory
business behavior, TypeScript-host parity, parser/rechunk/retry matrices and Linux
original-client execution remain outside this increment.

## Recorded cancellation drain

Entered fixture callbacks return their exact successful task results after explicit
release, even when cancellation has already committed. The execution remains
cancelled with no value reduction or workflow output. The initial audit incorrectly
expected a cancelled task with no result; its failed attempt is retained privately.
A strict source-backed regression and independent reviews corrected that expectation
before the two final fresh runs. Runtime and original caller behavior did not change.
