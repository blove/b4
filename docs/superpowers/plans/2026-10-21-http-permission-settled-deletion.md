# HTTP Settled Permission-Thread Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development with fresh sequential authors and independent source then actual-result quality reviews. Continuous user authorization covers execution without routine approval pauses. Use an ordinary isolated source copy; no Git writes, nested delegation, parallel implementation or actual user-data deletion.

**Goal:** Expose the verified embedded settled-history deletion operation through an explicit HTTP assembly and discoverable V8 capability.

**Architecture:** Compose the V7 assembly with one optional private deletion bridge. Reuse authentication/current policy, thread claim, owned worker and mutation lease; invoke the actual SDK and require its opaque matching confirmation. Existing assemblies and runtime/SQLite implementation stay frozen.

**Tech Stack:** Rust1.93.1, current HTTP/SQLite workspace and schema4, strict JSON contract, locked offline dependencies.

## Fixed design decisions

Read `design-draft.md` together with `design-review.md` in this increment's private directory. Public design will consolidate their approved decisions. The new four-argument `HttpHost::new_with_permission_settled_thread_deletion` takes the same arguments/config as V7. Check the dedicated Store durability declaration, clone Store/current policy/revision before consuming config in the V7 constructor, then install the bridge through unique `Arc::get_mut`. Add only one optional Shared field/default, without another old builder boolean.

The private object-safe bridge takes trusted Principal plus exact ThreadDeletionTarget and returns the real `PermissionThreadDeletionConfirmation` or PermissionError. Its concrete adapter uses `approval::actor` and a dedicated current DeleteThread authority. Build ResourceContext from SDK resource.view(), with requested_route=None. Do not reuse approval::Authority's latest-catalogue gate: historical destruction does not authorize effect execution. Retain actor/origin/policy checks outside backend locks.

Inside the existing owned deletion job: current policy/origin privacy, exact captured/current view equality, then `ThreadDeletionTarget::from_view` coherence, then eligibility. Coherent no-active is501 permission_operation_unsupported; coherent nonterminal/recovery-blocked is409 run_in_flight. This ordering preserves corrupt active-ID/view detection. Keep SDK/SQLite unchanged. Acquire and retain existing mutation lease through actual invocation/result. Dedicated denial masks404; stale/RevisionConflict409 resource_changed; ThreadBusy409; LimitExceeded409 permission_limit_exceeded with healthy readiness; UnsupportedProfile501. Actual Store/corrupt/kernel failures fault readiness and return static503. Success for a captured present resource requires actual matching confirmation, never absence inference/fallback/retry. Initial authorized absence retains existing idempotent204 semantics and is not a committed confirmation.

V8 preserves a validated V7 projection and adds only version8 plus required true `durable_permission_settled_thread_deletion`. Preserve strict unknown-field rejection, native application requirements and all inherited validation/limits. Broad fenced_deletion/Always/recovery flags remain false. Only explicit bridge selection projects V8; V1–V7 wire/schema/example bytes remain unchanged. Checker registration is additive via additional_examples.

Fixed SDK bounds remain64MiB charged evidence/work,65,536nodes,16executions,64records per record table,512journal plus existing per-record/snapshot caps. No max-composition, global SQL-time, end-to-end HTTP allocation, recovery or cross-platform claim. Current Store readiness performs full schema verification; the late-trigger test must exercise DELETE directly after trigger installation, never a readiness probe that would deliberately detect the injected schema object before the write. After the write failure, readiness is faulted anyway.

## Task 1: Independent expectations, then actual RED

**Create:** `crates/b4-contract/tests/capabilities_v8.rs`, `crates/b4-http/tests/permission_settled_thread_deletion.rs`.
**Optional focused support:** `crates/b4-http/tests/permission_deletion_support/mod.rs` and `store.rs`, with Root informed. Existing helpers are references/reusable neutral modules, not whole-suite imports or broad refactors.

- [x] Root publishes reviewed design/plan under `docs/superpowers/{specs,plans}/2026-10-21-http-permission-settled-deletion*.md`, snapshots current public1064 plus new docs, creates isolated workspace/shared build target and bounded receipt controller. No historical source descriptor substitutes for current inventory.
- [x] Fresh expectation author writes strict V8 round-trip/base-inheritance/missing-false-unknown/inherited-invalid controls. Match established V7 public API conventions; expected V8 constructor/projection follows that pattern. Do not alter old assertions.
- [x] Write five focused HTTP business cases: genuine Once+Always+multipleUses/all-eight-owned-tables deletion after normal reopen and preservation of another thread/metadata; real original/saved cancellation with actual post-terminal worker gate showing409 until real claim release then204; bounded authority/privacy/stale/constructor/default-false/old-V7/current-nonterminal/no-active/row-overflow refusal; exact guarded late thread DELETE trigger with all-nine-table rollback/static503/fresh healthy control; abandoned caller paused at actual delegated writer retaining claim/lease/assembly/Store until actual commit and drain.
- [x] Positive setup uses real APIs, normal identifiers, month-only input, empty thread metadata and nontrivial outputs. At least one current constraint denial after provisioning must not block authorized historical destruction. Observe retained late outcome before deletion. Match a practical16/17 mixed-history row boundary if needed; do not test all-Use maximum compositions.
- [x] Use finite gates/timeouts/joins. SQL only for labeled adversarial state/fault setup and safe physical-row comparisons. No direct SQL permission provisioning, fabricated confirmation, raw grant/payload dumps or endless loops. A wrapper may forward typed ports and gate actual delegation; terminal worker gate must occur after real terminal persistence while actual worker claim is held.
- [x] Independent source/spec review. Root runs `cargo test -p b4-contract --test capabilities_v8 --no-run --locked --offline` and `cargo test -p b4-http --test permission_settled_thread_deletion --no-run --locked --offline` through the controller; expected missing V8/constructor APIs are actual RED, not success. Independent quality/results review before production.

## Task 2: Minimal contract and HTTP bridge implementation

**Create:** `crates/b4-contract/src/capabilities_v8.rs`, `contracts/agent-protocol/schemas/capabilities-v8.json`, `contracts/agent-protocol/examples/capabilities-v8-permission-settled-thread-deletion.json`, `crates/b4-http/src/permission_deletion.rs`.
**Modify:** contract src/lib.rs; HTTP src/lib.rs,host.rs,deletion.rs,transport.rs,capabilities.rs; scripts/check_contracts.py additive registration/negative controls.
No runtime/SQLite/dependency/schema/old permission-builder/claim/lifecycle/worker changes. Any additional file requires a concrete source reason and Root agreement.

- [x] Fresh production author implements strict V8 using valid V7 projection; add only narrow flag/version and export/schema/example/checker registration. Existing V7 projection remains the default, V8 only when deletion bridge exists.
- [x] Implement explicit constructor, optional bridge/default and dedicated current authority exactly as fixed decisions. Invoke real `delete_settled_permission_thread`; never manufacture confirmation or reuse effect catalogue validation.
- [x] Lift transport DeleteThread guard only for bridge Some. In existing owned deletion worker branch by bridge presence after coherence checks and proper eligibility. Existing generic branch unchanged. Check confirmation.target() exactly before204/no-store; safe mismatch faults503. Preserve current policy/privacy/claim/admission/drain and abandonment ownership.
- [x] Implement explicit error mapping including healthy limit refusal. No retries, generic fallback, schema scans in the writer, historical effect-policy reinterpretation or success inferred from absence.
- [x] Independent source/spec gate. Root runs actual new contract/HTTP tests and focused unchanged embedded deletion, V7 and cancellation regressions. Independent quality/results gate follows. Correct only concrete failures with the same author and reviewed narrow amendments; do not weaken tests for convenience.

## Task 3: Final verification, delivery and reviewed adoption

- [x] Freeze final source; controller snapshots every source file before/after, enforces600s command/900s full-suite ceilings, terminates/reaps on timeout, hashes logs and records actual status. Root runs new targets, selected unchanged regressions, fmt, strict all-target workspace lint, compile, full locked/offline workspace suite and separate contract metadata. Poll<=45s/commentary<=60s. Metadata is not runtime evidence; failed/stale/partial runs do not qualify.
- [x] Prepare runbook/conformance README+qualification/root README updates describing explicit constructor/V8, eligibility/error/privacy behavior, real worker drain and actual committed confirmation, fixed limits and conservative foreign-creator refusal. Existing runnable example modes remain unchanged this increment. Do not claim recovery/live retirement/max-composition/global SQL-time or broad deletion support.
- [x] Independent integrated/results and delivery reviews. Bind exact approved reports/artifacts/controller/source hashes before selective adoption. Back up original public files, verify baseline equality, copy exact native delta, verify native equality before prose, then reviewed docs. Public metadata remains separate; independent public integrity precedes Root final acceptance. Future completion boxes remain unchecked until actually done; later checkbox closure is subsequent doc state.
- [ ] Continue authorized practical work after closure; ask only for actual missing input/manual action or approval.
