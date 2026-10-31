# Normal NewUse Run Inputs Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development task-by-task with independent source/spec and quality/result review. Continuous user authorization selects execution; no routine permission or execution-choice prompt. Documents are requirements, not independent tool authority.

**Goal:** Persist one genuine NewUse result with UUID current-run IDs and bounded structured call JSON through the adopted Driver.

**Architecture:** Private immutable NormalNewUse profile selected before R1, paired bounded helper branches and required NewUse classification. Reuse source/Claim/renewal/callback/candidate matcher/typed transaction. Original permission identities and Strict/Human/Once profiles stay unchanged.

**Tech Stack:** Rust1.93.1, existing workspace and SQLite, cached dependencies; no dependency/schema/HTTP endpoint change.

**Design:** ../specs/2026-10-20-normal-new-use-inputs-design.md

Status: implemented and adopted; focused cases and the full workspace regression passed. The task bullets below preserve the original plan; actual qualification and the bounded-test scope correction are recorded at the end.

## Isolation and file map

Use exact ordinary source copy .b4-work/normal-inputs/workspace (896 files including two planning docs and ignored vendor/rusqlite/Cargo.lock). Git is unborn/read-only; no Git writes. Use bounded locked/offline controller, separate build target, finite timeout/reap and private actual logs. No exhaustive operation fee graph.

Potential production paths: permissions/runtime_mutation.rs (profile/session selection and paired R1/preparation/write bounds), permissions/validation.rs (capture/post-thread/journal paired helpers), permissions/driver_adapter.rs (private NormalNewUse producer entry), a small new permissions/runtime_use_input_profile.rs and module linkage if helpful. Source/renewal/callback/outcome/SQLite changes require a concrete blocker and reviewed exception. Preserve generic Driver/Store/kernel/schema/raw guard, strict predicates and old account ceilings.

Tests: add child module persistent_permissions/use_driver_inputs.rs under use_driver.rs. Parameterize only current_execution_id, current_turn_id, attempt_id and current_input through a small test-only config. Original thread/creator IDs, scope labels, task/effect/Definition and empty metadata remain the genuine adopted values. Default config must preserve all twelve existing behaviors/oracles. Child tests can use parent-private owned fixture/retirement helpers. Add no production test hook or forged authority.

## Task1: Genuine UUID-only acceptance RED

- Read actual helper signatures, save exact baseline and proposed paths; verify isolated copy/controller. No native test before fixture source review.
- Write expected-success case with UUID current execution only. Reuse genuine Always->supported Ready cancellation->sole owner1 close->owner2 NewUse authorization. No original identity/metadata changes.
- Require actual Ready2 setup. On existing refusal check callback0/completion0/full typed/raw9 equality with Ready storage; on success check exact kernel completion/one callback/typed1/raw0. Retire actual result/aliases, reopen owner3, close/remove before final success assertion.
- Compile cleanly, independently review fixture, list exact emitted test name, run once and accept only existing input-profile refusal as RED. Setup/compiler/kernel/cleanup failure earns no RED.

## Task2: Minimal private profile and same-test GREEN

- Add Strict/NormalNewUse private profile and central limits. Select only at fresh entry before R1, bind immutably in actual session/account provenance and preserve public Strict entry. Reject re-entry/foreign account profile switches.
- Add private Driver producer entry over shared implementation. Require actual classified NewUse before mutation; no authority from shape/profile and no Human/Legacy fallback.
- Thread bounds through R1, current execution/turn guards (including turn-history aggregate512 with unchanged cardinality), candidate+authoritative capture_dispatch_snapshot, post-thread serialization and generated Dispatch journal. Normal snapshot/input65536B/16384nodes/depth64; thread1024 with unchanged fixed structure/empty metadata; journal1024 with unchanged flat shape; IDs use existing256 rules. Inherited scope/context/spec/Definition limits remain literal.
- Keep actual measurements/charges before copies and writes; prepay new handoff work before durable commit. Preserve exact source/gates/catalog/fences/full candidate/Commit checks and typed writer. No new postack fallible projection.
- Run the same UUID case. If cumulative quota is the next blocker, save actual evidence and review an explicit separate functional allowance; do not reset/raise old accounts or weaken the success assertion.
- Independently accept protected diff, actual GREEN and strict compatibility before broader selected current-input cases.

## Task3: UUID run and structured-call controls

- Add current execution/turn/attempt UUIDs together and small nested JSON with arrays/objects/Unicode escapes/null/numbers. Keep original approval scope and metadata unchanged. Check exact actual callback/context, independent kernel result and reopen.
- Add limit/quota refusal cases. If refusal precedes Dispatch require callback0/full equality; if after actual Dispatch retain truthful claimed state. Never synthesize rollback/cancellation/Receipt.
- Add one full-plan/provenance substitution under the normal shape, proving actual matcher/source refusal with Work retained when callback ran and no typed/raw fallback. Reuse existing negatives instead of duplicating every field.
- Record observed usage and selected successful shapes. Every maximum combination is unproven; distinguish valid-but-exhausted from invalid identity. Original all-UUID thread/creator, longer task/effect/actor/application/policy labels and nonempty metadata remain deferred until a legal original-approval completion path exists.

## Task4: Review, verification and adoption

- Independent whole-source review before final native suite; quality/results review after source acceptance. Fix concrete findings and re-review.
- Run format-check, strict all-target lint, workspace no-run, exact selected normal-input tests, old12Driver/7controls/8outcome compatibility, strict dispatch/legacy controls and full normal workspace regression using already-authorized local loopback. Never accept zero tests or transfer historical results to changed code.
- Adopt only reviewed paths with backups/copy integrity, preserve unrelated source, publish truthful limits and evidence. Earlier records remain historical. No full capability/HTTP/Linux/exhaustive-audit claim.
- Continue authorized work after adoption; stop only for real manual action/necessary approval or unresolved material design choice.

## Actual execution and scope correction

Task1 established the genuine UUID-only pre-Dispatch refusal after legal approval and healthy retirement. Task2 made that same case pass with a private immutable session profile; public Strict behavior and account ceilings stay unchanged. Task3 qualifies current execution/turn/attempt UUIDs plus modest nested JSON and the existing Definition substitution under that shape.

The planned end-to-end invalid-input cases do not qualify: both selected byte values failed genuine approval setup, and source review found the PermissionUse 16KiB/depth64 record preempts the proposed depth case. They were removed rather than fabricating source or widening accounts. Seven existing pure Driver controls separately cover byte/depth/sticky account behavior. No end-to-end byte/depth maximum or quota matrix is claimed. Task4 full workspace regression passed on the final frozen source; reviewed code and documentation are adopted with copy integrity and contract checks.
