# HTTP Original Always Implementation Plan

> Use fresh implementation agents per task with independent source then quality/results review. Continuous user authorization; use an ordinary isolated source copy, no Git writes and no routine human approval loops.

**Goal:** A new opt-in HTTP host creates a real human Always permission, executes the original callback once, then reuses that saved permission after owner reopening.

**Architecture:** Explicit creation-plus-reuse builder; actual producer acceptance returns a private continuation kind; original resumes use the verified NewHuman Driver and fresh runs use genuine reuse authorization. A separate V4 descriptor declares the narrow feature while V1/V2/V3 and old builders remain unchanged.

## Task1: V4 contract expectations

- [x] Obtain design/plan review and create isolated source copy, baseline and pinned/offline bounded controller.
- [x] Fresh test author adds focused V4 DTO tests using an existing valid V3 fixture and proposed `CapabilityDescriptorV4::permission_approvals(base)`. Cover exact creation/reuse flags, required app/limits/drain, serde unknown/missing/null fields and preservation of V3.
- [x] Independent source review, Root absent-API RED, then quality/results review. Missing API compilation is not executed metadata behavior.

## Task2: V4 contract implementation

- [x] Fresh implementer adds the narrow V4 DTO/validator, schema, example/manifest/checker registration. Reuse an unpublished V3 validation projection; do not refactor old versions.
- [x] Source review before Root focused V4 and legacy contract execution; public checker separately qualifies schema/metadata only. Independent quality/results gate.

## Task3: Genuine HTTP expectations

- [x] Fresh author adds a real TCP test for new PermissionHostConfig/new_with_permissions: fresh DB -> actual Waiting2 -> authenticated pending token -> Always -> original Complete5/callback1/no Use.
- [x] Verify actual immutable records and typed/raw kernel state, normal server retirement/store close/reopen, then actual fresh reuse Complete4 without another approval. Preserve existing tests.
- [x] Source review, Root absent-host-API RED and quality review. No manufactured permission records or raw fallback.

## Task4: Opt-in approval and original execution bridge

- [x] Fresh implementer adds the new constructor/config and required default-false `original_always_atomic_durable` store opt-in (Arc forwarding, SQLite true). Existing ordinary/reuse constructors keep behavior and metadata versions.
- [x] Share real approval preflight/token conversion and current native authority; call the normal Always producer and carry its actual accepted-continuation kind into the owned worker.
- [x] Dispatch genuine original resumes through the original Always Driver with current catalog/authority and RunControl entry hook; preserve real Work/failure ownership, host fault and no automatic retry/replay. Fresh new runs retain existing reuse/verified-miss fallback.
- [x] Publish V4 only for the new mode; retain unsupported permission recovery/cancel/delete guards and scoped broad flags. Source review, Root genuine TCP GREEN, then quality/results review.

## Task5: Focused acceptance and settlement boundaries

- [x] Fresh author adds actual authentication/current-policy and invalid-token acceptance checks with complete unchanged storage, plus old-builder Always refusal.
- [x] Add an exact acceptance-journal guard and prove complete rollback to Waiting2; actual token/grant/permission evidence stays unchanged. Remove the owned guard and complete through the real accepted path where useful.
- [x] Add exact original terminal storage fault: actual callback1/result, full Dispatch4 rollback, retained host fault, no callback replay or fake terminal, normal drain/owner retirement. Reuse existing lifecycle controls rather than exhaustive matrices.
- [x] Source review, Root focused execution and independent quality/results gate. Preserve real server/store owners and bounded waits.

## Task6: Verify and deliver

- [x] Format, workspace compile, strict all-target lint, focused V4/HTTP and legacy cases, then full locked/offline workspace regression with authorized loopback.
- [x] Independent integrated source/results review; exact selective adoption with backups and scoped qualification/runbook/core-spec amendment. Public metadata/schema/reference checker remains distinct from runtime execution.
- [x] Record final actual source/results and continue useful next work. Keep the seed/serve example, broad durable permissions and lifecycle extensions outside this increment.
