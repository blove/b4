# Unchanged AP client corpus implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and
> superpowers:test-driven-development, with independent SPEC then QUALITY review.
> The user authorizes continuous local reversible development and routine choices.
> Existing authorized workspace, exclusive file ownership and available retained
> agents are selected. No routine human handoff, Git mutation, package download,
> external provider, deployment or publication is selected. Independent units may
> proceed concurrently under the developer's delegation instructions.

**Goal:** Execute source-pinned original controller/worker methods against the
actual Rust native API and record bounded safe observations, including refusals.

**Architecture:** Separate offline bundle provenance, real native fixture with
passive TCP capture and private stdio control, original Node caller orchestration,
and offline framing/safe projection. Two fresh bundles and stores qualify one
bounded corpus without caller rewriting or protocol/authority repair.

**Tech Stack:** Locked Rust1.93.1/axum0.8.9/Tokio1.53.1/SQLite, existing std process
and threads; installed Node24.20.0/esbuild0.28.1/Zod4.4.3. No package/feature edge.
Design: spec/runtime/unchanged-ap-client-corpus-design.md. Actual caller platform
is initially macOS arm64; Linux Rust checks are separately scoped.

## File ownership and interfaces

- Bundle author: new conformance/reference/client-fixture/bundle.mjs,
  bundle.test.mjs and bundle support only. Exports buildClientBundles(options);
  options name the exact checkout/compiler and fresh absolute output directory.
  Returns separate version1 source/tool/input/output manifest. No client execution.
  Outputs controller-client.mjs, worker-client.mjs and bundle-manifest.json.
- Native fixture author: new crates/b4-http/examples/qualify_ap_clients.rs and
  ap_client_support/{mod,capture,gate,application,control,process,files}.rs, with
  focused private unit modules as useful. CLI positional inputs are absolute Node
  executable, runner, bundle directory and fresh private output directory. Rust
  owns host/store/server and launches Node. No existing crate edits.
- Caller author: new conformance/reference/client-fixture/runner.mjs,
  controller-cases.mjs, worker-cases.mjs, control.mjs and private-output.mjs plus
  focused tests. Runner imports actual bundles and executes eight named groups.
  Node stdout is version1 control only; raw results use create-new0600 files.
  Node argv are bundle_dir and output_dir/evidence/client; ready.routes keys and
  fixed /fixture/worker-…#workflow values follow the explicit design interface.
- Root: new client-fixture/wire.mjs, wire.test.mjs, projection.mjs,
  projection.test.mjs and qualify.mjs orchestration if needed; new runbook/ADR/safe
  observation artifacts and scoped requirements/checker integration. No shared
  helper or other author's file edits without an explicit ownership transfer.

Control operations/enums and raw file/manifest interfaces follow the design.
At most256 commands,8192 bytes including newline, one outstanding command.
ready supplies the exact private base URL/token; inspect-pending supplies only
actual interrupt IDs plus bounded token-free state. final-stop acknowledges
finished client work but does not close host admission. Native final proof has
normal Node0/EOF/joins, Quiescent, actual socket retirement, Weak absence and
reopened writer. Raw captures are connection ordinals with read/write byte files
and exact transfer/completion metadata; no HTTP body is fed back to the client.

## Task1: Frozen baseline and concrete design acceptance (root)

- [x] Fully read independent predecessor final audit and verify exact main4917f278
  and audit ae3ee204, then capture542 baseline artifacts/538 protected paths before
  any next public write. All182 prior crate paths and root lock stay protected.
- [x] Fresh locked/offline Cargo metadata captured privately. Verify Dawn branch/
  HEAD/status unchanged; read both original factories/wire/SSE and exact native
  output-kind/approval/retirement APIs. Pin source8acac534, existing tools and the
  prior385 artifacts separately. No imports/builds/caller execution yet.
- [x] Select original factories versus handwritten probes/full factory app; source
  feasibility and independent capture/outline critiques inform the concrete design.
- [x] Complete independent whole-design SPEC then QUALITY/plan review. Correct
  route selection, canonical AP-009 resume/AP-010 cancel, output-kind immutability,
  lazy-reader cleanup, caps, private authority and actual retirement obligations.
  Root repairs findings and bridges accepted status before implementation.

## Task2: Offline original-client bundle builder (bundle author)

- [x] Add compiling builder stubs and meaningful tests: valid pinned source/tool
  graph should yield a complete two-entry manifest but stub refuses/omits it.
  Capture behavior RED, then implement only needed builder/provenance behavior.
  Run: node --test conformance/reference/client-fixture/bundle.test.mjs
  Expected first: assertion failure after valid fixture/compiler setup, not missing
  package/import/API or network errors. Synthetic source graphs are labeled unit
  fixtures, separate from actual original-source builds.
- [x] Implement exact two pinned entries, existing compiler/Zod input admission,
  explicit Node builtins, no aliases/plugins/transforms/minify/sourcemaps, no output
  overwrite/symlink escape; hash all actual metafile inputs and outputs. Enforce
  fresh private directories and relevant before/after source/tool bytes.
- [x] Tests reject changed source pin/blob, wrong installed versions/inputs,
  unexpected input/external dependency, reused output/symlink target and incomplete
  manifests. Run positive behavior and refusal tests GREEN; freeze sources/logs.
- [x] Root complete SPEC then independent QUALITY by a different author. Only
  after approval build actual original sources twice into fresh private directories.
  Preserve full private pinned-tree and385 old build inventories before/after.

## Task3: Real native capture/control fixture (native fixture author)

- [x] Compile delegate-only CaptureIo/Listener stubs with positive transfer tests
  expecting actual read/write records; save meaningful behavior RED. No unsafe,
  product changes or dependency additions. Run:
  cargo test --locked --offline -p b4-http --example qualify_ap_clients
  A compiling empty recorder must fail actual byte assertions before implementation.
- [x] Implement cumulative16 slots/two262144-byte buffers, safe original poll
  delegation, prefilled ReadBuf suffix, Pending/error/zero result preservation,
  first-n vectored writes and original flush/shutdown/is_write_vectored. Local poll
  copying allocates no buffer, locks no global capture state or sink, and never
  independently reads an HTTP body. Bound overflow preserves transport but fails
  evidence. Test exact/N-1 bytes, slot overflow and sticky refusal.
- [x] Implement Option<TcpStream> close-before-try_send Drop, capacity16 completion
  channel and accepted/retired/receiver cardinality. Controlled IO drop witnesses
  qualify ordering separately from real hosted socket/writer tests. No proof from
  axum close_rx or channel closure alone; all sender aliases positively retire.
- [x] Add absolute-deadline gate stubs/behavior RED and sticky expiry/explicit vs
  cleanup release implementation, including timeout-then-release regression.
  Native application fixed ten factory plus value/void/failure/held/park routes,
  true Void binding and nested finite dispatch input table follow the design.
  Real owner policy/auth/RequiredDurable/native worker paths remain production.
  Worker held route uses one native callback and no pre-hold POST chunk/state.
  Live original reattach provides its actual state prefix for a first-frame cut;
  poll held POST and resolve next with actual signal or release/terminal before
  iterator return/EOF. Fixed fixture capacities4 allow actual same-thread refusal;
  original native thread_busy stays unchanged. Input/output limits65536.
- [x] Add bounded version1 control reading/serialization stubs and positive RED;
  implement strict fields/ordinals/ops/line caps without unbounded line allocation.
  Selected public Store reads provide actual token-free interrupt IDs. Control
  runs independently of held callbacks. Test oversized/partial/duplicate/unknown
  lines, invalid phases and bounded selected replies.
- [x] Implement std child/control/stderr ownership and independent watchdog;
  normal Node exit/reap0, real EOF/control+stderr joins before admission closure,
  normally joined graceful server, Quiescent and separate socket completion.
  Drop all Store/app observers, prove both Weak absent and actually reopen writer
  with owner advancement/readiness while capture bytes survive. Private files
  create-new0600 under0700 directories; Rust evidence/native pre-write quota24MiB
  includes captures/control/stderr/snapshots/proofs/manifests; stderr also<=1MiB.
  Node evidence/client pre-write quota8MiB is disjoint and cannot borrow. Runtime
  SQLite/source/bundle directories are private and outside serialization quotas.
  Independent final artifact accounting matches ledgers; cross-component overflow
  refuses before write, never an after-the-fact total check or two32MiB budgets.
  Reserve new empty runtime.sqlite3/owner.lock600 before real Store open; verify
  parent700 and actual files/sidecars600 at open/reopen without touching foreign
  paths. Existing SqliteStore::open ambient file modes are not a privacy witness.
  Failure RAII release/owned kill+reap is never positive retirement.
- [x] Focused unit/control/gate/capture checks plus strict format/lint pass. Root
  reads every new fixture file for SPEC, then different author complete QUALITY.
  No actual corpus execution until source review passes and Node runner is ready.

## Task4: Original caller groups and cleanup (caller author)

- [x] Define eight named exported async group functions C1–C4/W1–W4 before real
  execution; exact source factory imports and fixed original method invocation
  table, no method wrapper reimplementation. Use baseline expectations from design;
  captured differences are not compatibility repairs or whole-app success.
- [x] Compiling bounded-control/private-output helpers with positive RED precede
  implementation; use useful unit behavior tests for incremental line bounds,
  ordinal matching, output caps/permissions and existing-reader cleanup ordering.
  Run: node --test conformance/reference/client-fixture/runner.test.mjs
  Unit doubles are helper qualification, not original-client/native observations.
- [x] Implement actual groups: factory ten URLs/input bodies, direct401/bearer-only
  injected positive seam, Value/Void/known failure, held wait/busy/abort/interrupt,
  worker metadata CRUD/auth/refusals, original finite SSE, live/settled reattach/
  declared viewer cuts and cancel, actual RequiredDurable pending mismatch and
  separately forced actual-ID grantless resume once/deny/empty refusals.
- [x] Record private original versus forwarded request arguments, client returns/
  errors/frame outputs/EOF/cuts with controlled static invocation ordinals. Caller
  Response is never cloned, replaced or independently drained. Worker original
  parser is actually polled; settle blocked next before awaited iterator return.
  W1 releases held work before consuming original POST terminal/EOF; W3 uses
  original live GET first-state cut plus actual POST signal, then a separate held
  generation for original cancel terminal before physical callback release.
  Explicitly release every held gate and require its actual nonexpired return.
- [x] Bound frames64/stream, Node evidence/client8MiB pre-write quota within the
  disjoint32MiB native+client serialization total, no raw console stdout,
  no provider env or redirect/body/URL/schema/grant translation. final-stop only
  after all promises/iterators settle, before normal child0. Root full SPEC and
  independent QUALITY; freeze before actual two-run execution.

## Task5: Offline bounded framing and typed public projection (root)

- [x] Write compiling empty/refusal parser/projection stubs and positive behavior
  tests; preserve actual assertion RED before implementation. Run:
  node --test conformance/reference/client-fixture/wire.test.mjs
  node --test conformance/reference/client-fixture/projection.test.mjs
- [x] Implement bounded persistent HTTP1 request/response queues, Content-Length,
  chunked framing including final trailer CRLF, no-body statuses, checked lengths
  and header limits. Tests cover fragmented/multiple messages, vectored boundaries,
  final chunk/trailer leftovers, ambiguity/malformed/overflow and only explicitly
  allowed partial cuts. These are harness framing tests, not TS parser parity.
- [x] Correlate actual private request/response/client invocation IDs with raw
  socket accepted bytes and control witnesses. Keep capture complete, message
  complete and caller EOF/cut separate; no one-request/one-connection assumption.
- [x] Closed typed projection emits static names/enums/statuses/counts/result and
  frame shapes/seam declarations only. Reject unknown fields/dynamic IDs/errors/
  prompts/token/grant/raw authority hashes, rather than scrub substrings. Public
  provenance admits source/compiler/package/bundle hashes only. Complete non-author
  SPEC and QUALITY approval before projecting or publishing any actual observations.

## Task6: Actual two-run execution and source preservation (root)

- [x] Build two fresh original-client bundles after their review; independent
  provenance audit confirms exact8ac pin/transitive wire/SSE/Zod/tools and no
  overwrite/source edits. Preserve385 old artifacts and private whole-tree inputs.
- [x] Compile current reviewed Rust example locked/offline. Run actual fixture
  twice with explicit absolute Node/runner/bundle/output arguments under normal
  loopback authorization, fresh stores and cleared child environments. Record real
  client/network/authority/control/retirement evidence privately. Any failure is
  diagnosed and repaired through meaningful regression tests, not response repair.
- [x] Require all eight groups/all19 selected methods actually invoked each run;
  expected permission/grant counterexamples remain refusals. Validate all actual
  connection captures/framing/client outputs and positive lifecycle witnesses,
  compare only stable selected shapes after actual caller consumption.
- [x] Reverify all538 protected files, existing182 crate paths, full Cargo
  package/features/resolver graph and original pinned inputs. Only new targets
  may change metadata; no product/dependency/feature/source rewrite is allowed.
- [x] Run appropriate fresh full current format/strict all-target lint/Rust,
  Python/Node/contracts; qualify Linux Rust scope separately if built there.
  Existing release/container receipts remain historical, never fresh caller proof.
  Count shipped declarations, group invocations, subcases and runs distinctly.

## Task7: Scoped traceability, docs and independent freeze (root)

- [x] Add safe local observation artifacts/runbook/ADR. Selected README/compatibility
  append/relabels and requirements/checker changes have exact private reversal
  bridges to the542 baseline. Preserve all historical docs/tests/receipts elsewhere.
- [x] Add only necessary named scoped client-corpus requirements. Checker may
  recognize mjs named function declarations for actual executed acceptance groups;
  a meaningful missing-function/new-group hook regression proves enforcement.
  Named existence/schema integrity never asserts fresh execution.
- [x] Explain original pending schema rejection, forced-ID seam and grantless
  refusals, fixture outcome/business limits, passive socket acceptance versus client
  delivery and initial macOS-only original-client scope. Keep56not_run/M0/CP/M5/
  core gates open;19 methods do not mean19 successes or full CP010 qualification.
- [x] Build final source/graph/bundle/count/execution/review/preservation receipt
  with no raw authority/private-output digests. Independently review complete
  docs/mappings/evidence, exact final bridges and external final receipt freeze.
  Continue authorized local work after that scoped decision.

Historical design-acceptance progress: predecessor diagnostics is independently frozen. Baseline and source-only
feasibility are captured; complete independent SPEC and QUALITY approve the design
and plan for implementation. No new harness behavior, original-client bundle/import/
execution or compatibility result is claimed. No manual action or additional
permission is currently needed.

## Completed scope

Two final fresh original bundles/stores pass the complete 63-call corpus each on
macOS arm64. The first failed historical audit and its exact strict source-backed
repair remain separate evidence. Current checks: Rust844/three historical ignored
helpers, Node221 and Python90. No new Linux original-client execution is claimed.
Broad56/M0/CP010/M5/core gates remain unchanged. See
[the verification receipt](../../../conformance/expected/unchanged-ap-client-native-verification.json).

Final public-support SPEC and QUALITY passed; the external source-bound freeze records their exact bindings and this narrow status bridge.
