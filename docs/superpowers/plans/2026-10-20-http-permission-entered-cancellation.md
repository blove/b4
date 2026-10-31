# HTTP Entered Permission Cancellation Implementation Plan

> Use superpowers:subagent-driven-development with fresh sequential authors and independent source then quality/results reviews. User continuous authorization applies; isolated ordinary source copy, no Git writes or routine approval pauses.

**Goal:** Ship opt-in HTTP cancellation for actually entered original Always/NewUse callbacks with truthful narrow metadata and retained ownership.

**Architecture:** Native callback-body tracker installed through metadata-preserving mapping; explicit host V5 profile and dedicated current management bridge to existing O/P typed runtime operations. Preserve existing captured-run/gate/publication/owned drain machinery; unentered cancellation remains rejected.

**Tech Stack:** Rust1.93.1, existing native application/permission/kernel/SQLite, axum/tokio and actual loopback tests, locked offline dependencies.

## Task1: Core mapping and metadata expectations

- [x] Review approved design/source fit; Root captures baseline/isolated workspace/controller. Preserve old files/tests.
- [x] Fresh author adds meaningful core tests in crates/b4-runtime/tests/native_callback_mapping.rs and crates/b4-contract/tests/capabilities_v5.rs. APIs map_native_callbacks on NativeApplication/PermissionCatalog; CapabilityDescriptorV5::permission_entered_cancellation(validV4). Prove current/history/validators/output-kind/identity/constraints unchanged while actual callbacks are wrapped, and V5 closed mechanism/roundtrip with V1–V4 frozen.
- [x] Independent source gate, Root missing-API RED, independent fixture/results review.

## Task2: Small core assembly and V5

- [x] Fresh author adds native/catalog callback mapping (registry.rs/catalog.rs, focused helper if needed), preserving every metadata field and constraint Arc; callbacks only change. No grant/Store authority, no old bound/default change.
- [x] Add crates/b4-contract/src/capabilities_v5.rs + exports, contracts/agent-protocol/schemas/capabilities-v5.json and contracts/agent-protocol/examples/capabilities-v5-permission-entered-cancellation.json following V4 patterns, a narrow entered flag while generic fenced_cancellation stays false. Metadata validation delegates unchanged common checks to V4 projection. Register only new metadata checks without editing V1–V4 fixtures.
- [x] Source review, Root focused GREEN and quality/results review. No HTTP behavior change in this task.

## Task3: Genuine HTTP expectations

- [x] Fresh author creates crates/b4-http/tests/permission_entered_cancellation.rs using existing native approval/permission HTTP fixtures and real SQLite. Seven distinct scenarios in design: both-origin Ack/reopen; true late outcomes; current authorization plus revoked constraint; actual Dispatch-without-entry rejection/healthy control plus old mode; stale captured successor; exact late request fault; abandoned ownership/drain. Use finite channels/TCP/servers, real human acceptance, actual body observations and bounded typed/kernel/physical observations, not fabricated grants/receipts or a broad matrix.
- [x] Intended constructor HttpHost::new_with_permission_cancellation(store,auth,policy,PermissionHostConfig), V5 descriptor. Existing endpoints are used; no new public request fields selecting source/mode. Old constructors remain unchanged.
- [x] Source gate, Root intended-API/behavior RED, independent fixture/results review. Correct only concrete test/source-fit problems.

## Task4: Scoped HTTP bridge and ownership

- [x] Fresh single author adds focused private permission cancellation/entry tracker module. Narrow edits permissions.rs, control.rs if needed, cancellation.rs, transport.rs, capabilities.rs, private bridge control Arc signatures and builder/export wiring. Keep supervisor/claim/viewer behavior unchanged unless a concrete integration seam requires a reviewed adjustment.
- [x] Map the opt-in host's private catalog to a real NativeEffect wrapper. Scoped tracker binds actual Arc RunControl + typed original/NewUse kind; body alone marks exact entered run/task/attempt/context; weak links and bounded one-run map, no locks across external calls. Untracked ordinary native callbacks delegate unchanged. New mode uses cancellable Drivers; old modes remain direct.
- [x] New constructor requires both entered Store capabilities. V5 publication only new mode. Preentry/Ready or wrong identity returns stable conflict before Pending, no mutation/signal; old permission modes preserve unsupported response/deletion.
- [x] Current CancelRun authority/catalog/context + typed original/NewUse request, exact captured identity and runtime source/fences, no effect gate or generic fallback. Confirmed commit precedes local signal/terminal; actual callback Ack/Success/Failure settlement and quiescence retain ownership. Preserve bounded conflict retries, unknown outcome fault/no replay, abandonment, successor exclusion and one terminal/late-output suppression.
- [x] Source gate, Root focused GREEN and independent quality/results gate; fixes based on actual evidence only.

## Task5: Final checks and adoption

- [x] Add boundary control only for a concrete missing requirement. Root format, strict workspace all-target lint, compile/full locked offline tests with finite controller timeout (900s full child after an incomplete 600s attempt; poll45s) on unchanged reviewed bytes.
- [x] Independent integrated source/results review, scoped runbook/qualification, exact selective adoption/backups, public metadata separate from runtime, final artifact integrity/closure review.
- [x] Continue toward Ready/pre-entry HTTP integration or user steering. Do not declare all HTTP cancellation timing cases, recovery/deletion or broad durable permissions qualified by this increment.

## Task4 evidence-based allowance amendment

- [x] Preserve actual byte-limit RED and diagnostic receipts; verify temporary diagnostic removal.
- [x] Add only a fresh 24 MiB entered-cancellation evidence factory with unchanged 65,536 nodes, old account defaults/purpose guards/ledgers and source limits; use only the new HTTP typed management bridge.
- [x] Verify the existing late-result scenario and all seven HTTP scenarios, review the narrow source delta, and include it in final unchanged-source checks. No additional operation audit or test matrix.

All focused and full unchanged-source checks passed; exact reviewed source was adopted with backups. Runtime evidence and metadata validation remain separate. Continue toward the remaining HTTP timing states.
