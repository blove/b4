# HTTP settled permission-thread deletion design

Connect the adopted embedded `delete_settled_permission_thread` operation to AP-003 `DELETE /threads/{thread_id}`. This is an explicit additive native subset. User continuous authorization covers implementation and owned fixtures; never delete actual user data during development. X is adopted/closed with full qualification before this wiring begins.

## Approach and boundary

Use a new `HttpHost::new_with_permission_settled_thread_deletion` constructor and a strict V8 capability descriptor with one narrow `durable_permission_settled_thread_deletion` feature. It inherits the existing V7 permission approvals/reuse/Ready/entered/withheld cancellation assembly and configured limits. Existing V1–V7 contracts/constructors and their deletion refusal remain frozen. The broad `fenced_deletion`/Always/recovery flags stay false. The new flag promises only complete settled histories under the embedded operation's fixed limits.

A thin private permission deletion bridge is preferable to relaxing generic Store deletion, which deliberately refuses permission records, or silently broadening V7. Reuse the existing V7 constructor from the new module, check the new Store declaration, then install the bridge in the uniquely owned Shared assembly. This avoids extending the old builder's boolean matrix. A new optional Shared field defaults to None in all existing assemblies. No runtime/SQLite writer change, schema/dependency migration or new cancellation mode is needed.

## Owned request path

Keep existing authentication, immutable application scope and initial current DeleteThread policy/privacy gates. Only the explicitly configured bridge lifts the existing permission-profile DeleteThread refusal. Missing-resource behavior retains the existing authorized transport semantics: any absent-resource HTTP204 is an idempotent absence response, not a fabricated typed commit confirmation. Existing inaccessible-resource masking remains404.

For a present resource, reuse `deletion::delete`: acquire ThreadClaim, transfer it without an await gap to the existing OwnedShared blocking job, re-read/current-authorize the resource and compare its exact captured view. The claim is retained until the actual run worker and captures have retired; a stored terminal alone does not permit deletion. The job takes and retains the existing mutation lease through the real SDK call, result handling and cleanup. Caller abandonment cannot drop the worker, claim, lease or Store ownership early; normal shutdown waits for actual quiescence. No new global registry, SDK lease object or native interruption is introduced.

The bridge constructs the immutable ApprovalContext from the trusted request principal and host policy revision, captures the exact ThreadDeletionTarget, and calls the verified embedded function using its Arc<PermissionStore>. Its dedicated authority adapter invokes the current ResourcePolicy for Operation::DeleteThread on the resource supplied by the SDK, outside Store/backend locks, retaining independent actor/origin confinement. Historical permission policy and latest tool constraints are not effect-execution authorization for destruction. The host's current catalogue is not used to reinterpret old records. Legitimate prior-owner terminal executions remain supported through separate current Store epoch anchoring.

Return204/no-store for a present resource only after a matching actual opaque committed confirmation. No generic Store fallback, fake receipt, success inferred from later absence, storage resend or callback replay. Ordinary generic deletion remains unchanged for hosts without the bridge.

## Eligibility and failures

The new profile supports a present thread only with an active Completed, Failed or Cancelled execution and an entirely settled validated history. A live ThreadClaim always gives409 run_in_flight, including a terminal persisted while the worker still owns its claim. Current nonterminal views should refuse409 without poisoning readiness. Existing empty/no-active threads remain outside the new settled-history subset; select a static unsupported-profile response rather than pretending a commit occurred. Do not force-delete parked/recovery work.

Source-fit decision: SQLite's dedicated capture currently uses a generic CorruptRecord refusal for its early nonterminal check, whereas the runtime typed constructor uses ThreadBusy. After current policy/origin checks and exact current-view equality, construct ThreadDeletionTarget to verify coherence before classifying eligibility. The HTTP adapter classifies the coherent current view before calling the SDK, so valid Ready/Waiting/CancelRequested/recovery states return409 and do not become an infrastructure fault. Historical contradictory/corrupt relations remain infrastructure failures. Do not alter the adopted runtime/SQLite implementation merely to map these ordinary HTTP states.

Current authority denial/origin masking returns404; stale captured views/targets return409 resource_changed. Limit exhaustion is a static bounded-profile refusal (409 permission_limit_exceeded) preserving rows and healthy readiness. Missing durability is a constructor failure; UnsupportedProfile maps501 permission_operation_unsupported without fallback. Actual Store infrastructure/corrupt errors map through existing readiness-fault/error handling and never expose record/native diagnostic text. An actual late thread DELETE failure remains503 after full rollback, owned fault/drain and no automatic retry.

The deletion account remains64MiB cumulative charged evidence/work and65,536nodes,16executions,64rows per grant/acceptance/decision/Use table,512journal rows plus existing snapshot/record/depth caps. All maxima need not compose. These are the SDK capture/recapture limits, not a new end-to-end HTTP allocation/time guarantee. Foreign creator checks may visit unrelated rows; malformed/unknown creator evidence conservatively refuses. Existing initial HTTP metadata reads and global SQL scan work gain no new claimed bound.

## Capability contract

Add new contract module/export, strict V8 wire schema/example and focused tests. V8 derives from a valid V7 assembly, preserves all existing fields and effective limits, and changes only version plus the narrow deletion flag. Required non-null native application, unknown-field rejection, true required mechanism and all inherited validation remain enforced. Old V7 parsing rejects V8; V1–V7 schemas/examples remain byte-identical. Capability truth is conditioned on explicit constructor selection and the atomic durable Store declaration. Metadata does not constitute execution proof.

## Focused acceptance before implementation

Use independently authored bounded real SQLite/HTTP expectations with genuine API-provisioned history and normal IDs/month-only input/empty thread metadata matching already qualified producer/Driver profiles. Do not expand old input limits or import whole old test suites. Reuse neutral support or one dedicated small support module with informed Root; avoid duplicate helpers where practical.

1. Genuine Once + original Always + multiple saved Uses produce all eight owned tables; normal owner reopen and actual HTTP204 delete exact rows, preserve another thread/metadata and allow healthy reopen. New V8 discovery is true only in the explicit assembly. Current tool constraint denial must not block currently authorized destruction of historical records.
2. Real HTTP original/saved cancellation and a paused post-terminal worker demonstrate that persisted terminal alone still gives409 while the real ThreadClaim remains owned. Release actual work/cleanup, then deletion succeeds only after real drain; retained late outcomes are observable before deletion.
3. Focused current-policy/privacy/stale-target/constructor/default-false/old-V7/nonterminal/no-active/finite row-overflow refusals preserve all nine durable tables and correct readiness. No broad request/parser or corruption matrix; existing layers already cover those.
4. Exact post-open guarded late DELETE trigger reaches dependent removals, produces static503, preserves all nine tables, faults/drains host; remove exact trigger, normally reopen and obtain a fresh healthy deletion control. No blanket unknown-commit rollback claim.
5. Abandoned deletion management paused at the actual backend commit keeps thread exclusion, mutation lease and Store/assembly ownership through commit/drain despite the dropped HTTP caller. A later normal owner observes the actual committed absence; it is not used to synthesize the abandoned response's confirmation.

Keep all barriers/timeouts/threads/processes finite and reaped, safe boolean row comparisons, no grant/payload logging, no fabricated writer confirmation or direct SQL positive permission provisioning. Existing embedded deletion, HTTP cancellation, contracts, generic guards and record regressions remain unchanged.

After independent source/actual missing-API RED/quality gates, a fresh sequential author implements only the new contract/bridge and necessary HTTP assembly/dispatch/capability wiring. Frozen focused, format/lint/compile/full locked-offline checks and scoped docs/qualification/exact reviewed adoption follow. No Git writes, nested delegation, parallel implementation, actual-user-data deletion, automatic retry, recovery, global SQL-time, maximum-size or cross-platform claim.

## Confirmed source fit

The private object-safe bridge accepts trusted Principal and exact ThreadDeletionTarget and returns the real opaque SDK confirmation. Its dedicated authority builds current DeleteThread ResourceContext from the SDK view with requested_route=None and uses approval::actor, without approval::Authority's latest-catalogue gate. Compose the existing V7 constructor, then install the optional bridge through uniquely owned Shared. One production Shared initializer defaults the field to None; no extra permission-builder boolean or ownership mechanism is required.

Existing run-worker claim ownership extends beyond durable terminal persistence through actual Driver completion and cleanup. The existing deletion owned job and mutation admission retain the required ownership through the real write and result even if the caller disappears. SDK/SQLite APIs remain unchanged. Current readiness checks full schema objects, so the injected late DELETE trigger test exercises DELETE directly after installation; probing readiness first would deliberately detect the injected schema object before the writer.
