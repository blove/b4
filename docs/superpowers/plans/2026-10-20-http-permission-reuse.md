# HTTP Permission Reuse Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development, fresh implementers per task and independent source then quality/results review. Continuous user authorization; no Git writes or routine permission loops.

**Goal:** Complete one authenticated HTTP run using a genuine persisted permission and receipt-backed callback settlement.

**Architecture:** Opt-in single-route host/profile; catalog supplies immutable application; existing native approval authority and Once/Deny fallback; typed Ready2/NewUse Driver branch with local RunControl hook. Accurate V3 capabilities gate unsupported operations; legacy hosts and descriptors remain unchanged.

**Tech Stack:** Rust 1.93.1, Axum/Tokio, SQLite, current locked/offline dependencies, JSON Schema.

## File map

- Runtime: `crates/b4-runtime/src/permissions/catalog.rs` readonly native-application accessor only; existing typed services/Driver are consumers, not targets for blanket widening.
- Host configuration/bridge: new `crates/b4-http/src/permissions.rs`; `application.rs`, `host.rs`, `lib.rs`, `approval.rs` for explicit construction/shared authority; `worker.rs` for one Start and typed branch/local hook/real terminal projection; `transport.rs`, `recovery.rs`, `confinement/mod.rs` for authenticated unsupported-operation guards; `capabilities.rs` for profile selection.
- Contracts: new `crates/b4-contract/src/capabilities_v3.rs`, export in `lib.rs`; new `contracts/agent-protocol/schemas/capabilities-v3.json` and `examples/capabilities-v3-permission-use.json`, manifest entry; `scripts/check_contracts.py` targeted V3 validation. Existing V1/V2 wire files remain literal. Shared validation extraction only if required to preserve their exact accepted/rejected semantics.
- Tests: new `crates/b4-http/tests/permission_uses.rs`, targeted contract tests and existing native host suites as regression. Documentation adds a narrowly scoped conformance record/runbook after actual success.

## Task 1: Genuine HTTP RED

- [x] Add a lean real SQLite provisioning fixture using actual normal Always approval, original Ready cancellation and owner close/reopen. Use UUID actor/resources, descriptive task/effect and modest JSON. No direct manufactured permission inserts.
- [x] Add actual HTTP wait test expecting one callback, real Use/Complete4/journal, immutable original records, exact persisted state and listener/worker/store close/reopen/removal. Target `PermissionUseHostConfig` and `HttpHost::new_with_permission_uses`.
- [x] Review source; root runs API RED for absent constructor/config. Then only minimal readonly accessor/config/delegating constructor scaffolding in the isolated copy, source review and behavioral RED showing the legacy host parks instead of consuming the existing permission. Do not deliver that scaffolding as working support.

## Task 2: Explicit profile contract

- [x] Test V3 accepts the declared single-task permission-reuse profile, requires real durable storage and rejects misleading broad Always/recovery/cancel/delete flags, unknown fields, bad assembly and illegal bounds. Preserve all V1/V2 tests and fixtures unchanged.
- [x] Add the V3 DTO/schema/example and host response selection. V3 declares narrow reuse and required Once/Deny, but no cancellation/recovery/deletion. Preserve supervised ownership/drain declarations and actual limits. Add checker coverage without claiming product execution from metadata.
- [x] Source review, then targeted contract tests/checker. Keep all changes isolated until host execution is complete.

## Task 3: Typed HTTP bridge

- [x] Validate one route/batch/Pure required task, durable permission-store ports and single-run/task limits. Derive the exact application from catalog. Share existing principal/origin/policy/native-route authority logic.
- [x] For a new run persist Start1 once; capture a real authorization command and fresh Use ID, call typed Ready authorization against current fences. Only Some enters the receipt-backed Driver from real Ready2. Only Ok(None) falls back to the existing Driver on Start1. Every error refuses without callback or silent fallback.
- [x] Use current monotonic facts, fresh attempts and the local RunControl callback gate. Keep actual Work/store/capacity/worker owners through invocation and retirement. Storage retries, if added, reuse the same failure/account. No raw fallback completion or invented terminal result.
- [x] Guard cancel/recover/delete/operator-confinement before mutations after normal authentication/authorization; preparing startup is not supported by this constructor. Preserve existing host behavior. Source review before root focused GREEN.

## Task 4: Boundaries and delivery

- [x] Add meaningful wrong-origin/policy refusal, real missing-permission Once/Deny park/resume, unchanged-state unsupported-operation/capability checks, request-limit refusal before admission, and viewer detach/owned-drain cases. Add a storage-refusal case if retries/fault mapping are introduced. Avoid an exhaustive matrix.
- [x] Spec review then quality/results review. Run format, workspace compilation/strict lint, selected HTTP/contract tests, full workspace regression (authorized local loopback), and public checker with new contract inventory.
- [x] Adopt only reviewed tested files after exact backup/hash checks; document actual macOS embedded-to-HTTP subset and genuine provisioning. Keep HTTP Always creation, permission-aware cancellation/recovery/deletion, Linux and broad release qualification open. Continue useful next work.
