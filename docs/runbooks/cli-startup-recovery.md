# Explicit native demo CLI startup recovery

Status: bounded explicit native demo CLI recovery qualified on macOS arm64 and isolated Linux aarch64. Final post-support checks and immutable source acceptance are recorded externally after this artifact, without a self-referential hash.

## Run the fixed demo

Build the current source using the launcher instructions in the [README](../../README.md). Keep the existing bearer token, private data directory and loopback configuration. The new option forms are:

```sh
b4run config-check --demo --recover
b4run serve --demo --recover
```

`--recover --demo` is equivalent. Duplicate, missing, foreign, extra and non-UTF8 options fail before environment access. No new environment variable or application loader is added. Configuration checking validates the existing configuration and filesystem preconditions without creating a data leaf, opening SQLite, advancing ownership, creating an executor or binding a listener. It does not prove that stored work can recover.

Ordinary `serve --demo` keeps its direct launch policy. Explicit recovery selects the existing ordinary Preparing API with its default bounded scan/recovery limits. The resolver accepts only application `native-demo`, tenant `local`, principal `owner` and returns a fixed current principal. Stored arbitrary labels and bearer tokens are not reconstructed into authority. Exact historical Definition availability and the current resource policy still decide recovery.

## Readiness and shutdown

The real listener is started and retained before one synchronous preparation begins on the existing executor. `b4run_listening` identifies transport availability; `b4run_preparing` identifies the selected startup pass. While preparation is pending, health remains available and ordinary admission/readiness stay closed. Pending waits renew observation without retrying preparation or cancelling its coordinator. Only actual Complete emits `b4run_ready`; readiness means that recovery admission finished, not that each recovered callback returned.

An actual start/wait/Incomplete error closes admission and enters owned draining. A missing exact historical Definition yields the fixed `b4run_preparation_failed` diagnostic and exit 1 after witnessed normal retirement. Usage/configuration errors remain exit 2. Existing HostFault and actual infrastructure Server errors retain precedence. A signal that wins while preparing remains a signal trigger even if shutdown subsequently makes preparation incomplete.

SIGINT/SIGTERM close admission before asking the transport to stop. Observation intervals, incomplete waits and repeated signals do not release the executor, host or SQLite owner. Positive retirement requires registered-work quiescence and the successful actual outer-server completion witness. Held recovery/callback work must return normally. An opaque lost-server result remains in the supervising owner cache with its payload and executor; explicit kill/reap in the negative qualifier is cleanup only and supplies zero positive retirement proof.

## Qualified case boundaries

Four parser cases cover exact option selection, configuration-only checking, malformed grammar before environment access, unchanged defaults and redaction. The fixed-resolver case is pure. Two held-work cases use legal durable approvals, explicit normal work return, actual quiescence/listener completion, relevant Weak absence, normal executor/process return and a real next SQLite writer at actual current owner plus one. The lost-server case proves retained ownership only.

The compiled-CLI parked-work case creates a real required approval, closes the seed normally, restarts with explicit recovery and preserves the full original view and journal before approving the original grant. A later restart resumes through the existing `/resume` route and finite SSE, observes exactly one done frame with its expected output, then requires normal exit/reap and a genuine next writer before checking completion and durable consumed-grant/acceptance fields. Its final field assertions do not imply an independently retained complete final snapshot.

The compiled-CLI missing-definition case admits a historical native Definition legally, requires healthy seed closure, then observes closed readiness, fixed preparation refusal and normal retirement before a genuine next writer verifies the original full view/journal and approval tables. Configuration checks in both orders run with the configured port genuinely occupied and acquire neither storage nor listener. Empty-store startup in the reverse order reaches readiness, retires normally and leaves work tables empty after real writer succession.

These are twelve genuine Rust declarations. Five are pure parser/resolver cases. Owned infrastructure selectors, repeated selections, loop branches, child summaries and Python metadata add no Rust case count. Configuration-only proof, health/listening, readiness, callback settlement and physical retirement remain separate.

## Retained development history

Clean parser setup preceded two intended InvalidArguments failures and two compatibility passes. A later clean parser GREEN ran all four new cases; a separate unchanged parser suite also passed. The parked-work baseline failed its intended owner-adoption assertion after normal seed/restart retirement and real writers. The historical-definition baseline failed its static refusal assertion after normal closure and full original-state preservation. Later branches were unreached in those initial RED selections and receive current GREEN coverage only.

The first production compile failed because the new parent test module lacked Store/ApprovalStore trait imports; it ran no tests. A narrow import correction produced a clean eight-executable CLI build. A subsequent parked-work run reached its later approval branch and failed 400 because the fixture sent a Resume payload to `/runs/wait` and expected JSON. A private fixture-only correction selected `/resume` and finite SSE; the complete parent subsequently passed. These setup/protocol failures remain distinct from the meaningful initial behavior RED and do not imply a production algorithm fault.

Three separate metadata methods initially failed with twelve assertion-failure entries and zero setup errors: one missing actual group, ten foreign-scope/status/evidence subtests in one method, and one empty inventory. Later inventory/helper/global-ID branches were initially unreached. The checker adds one separate CLI group to the existing global ID check, requires exact native classification/design/component/scope, exact ordered twelve cases and its own existing JSON for a scoped pass. Metadata validates bookkeeping and supplies no runtime or retirement proof.

## Evidence and remaining scope

The complete CLI all-target run passed 87 outer cases across 8 Cargo targets with 0 failures and 1 ignored private child; two nested storage-probe summaries are excluded. Workspace formatting and strict all-target lint passed. Each platform's normal workspace selection passed 1,244 outer entries across 109 Cargo targets with 0 failures and 9 default ignored markers; the new 12 repeat within those totals, and two nested CLI summaries add no credit. Linux used an isolated unprivileged, offline container and a verified fresh build target. Removing that idle owned container gives no runtime-retirement proof.

The three earlier slow SQLite controls remain separately inherited historical evidence. Their complete 138-file sqlite/runtime/kernel/contract dependency closure and all 9 Cargo/lock inputs are unchanged, with platform/configuration applicability independently checked. Linux retains the same pinned compiler context; macOS binds the current toolchain observation and unchanged normal-workspace context. The historical package-selected slow binary remains separate, without a historical compiler checksum or binary-equality claim. No fresh `--ignored` selection ran, and those 3 are not added to the normal counts or treated as new CLI cases.

Actual metadata GREEN passed the 3 genuine methods. Completion reaches ten field mutations, empty inventory plus five substitutions, and 27 prior-group duplicate-ID iterations; successful subtests are inferred from the passing bound bodies rather than separate raw pass lines. The pre-support full Python run passed 117 ordinary methods, and the checker validated 461 named requirements: every prior 460 plus the separate CLI record. Its 20 schemas, 23 authored examples, 427 rejection cases and 170 references retain the existing contract scope. Node readers check retained grant/lifecycle artifact integrity; fresh TypeScript reference/client and Node caller/unit suites were not qualified here.

Final post-support Python/contracts and the narrow 23-step plan/status bridge are recorded externally after these source artifacts. The final immutable all-source freeze binds every source byte/mode and protected input. These forward stages have no invented count, exit or circular final-document hash in the scoped receipt.

Arbitrary applications, actor/secret reconstruction, automatic confinement, persistent Always, actual confinement deletion/reincarnation races, full RFC11.4, the 56 broad release scenarios, M0 and CP010 remain open. Selected fixture/data caps give no whole-startup heap, CPU, I/O, storage or hard deadline guarantee. TypeScript reference/client and Node caller/unit evidence remains historical; current Node tooling checks retained artifact integrity. See the [design](../../spec/runtime/cli-startup-recovery-design.md), [implementation plan](../superpowers/plans/2026-10-03-cli-startup-recovery.md) and [scoped receipt](../../conformance/expected/cli-startup-recovery-verification.json).
