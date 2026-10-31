# Durable approval service implementation plan

User continuation authorizes execution here. Use the test-driven-development
skill and parallel agents with disjoint files. No commit or push is requested.

**Goal:** Atomically park with required grants and accept Once/Deny decisions
with durable continuation, authority checks and completion without reapproval.

**Design:** `spec/runtime/durable-approval-design.md`.

## 1. Runtime models, service and memory store

Files: contract IDs/tests, runtime approval modules, Store generation/MemoryStore,
runtime manifest and new reusable backend/service approval tests.

- [x] Add failing identity, generation, redaction, canonical digest, authority,
  park/pending/exact-set, grant error, expiry, denial and acceptance tests.
- [x] Add ThreadGeneration and the agreed approval interfaces; publish API details
  to the SQLite worker before its implementation.
- [x] Implement OS-random grants, bounded bindings, exact transaction validation,
  required-durable constructor and explicit volatile-fixture mode.
- [x] Implement MemoryStore atomic issuance/acceptance and prevent unrecorded
  decisions for grant-bearing tasks; preserve prior trusted Store behavior.
- [x] Verify fresh fences, concurrent acceptance, approved dispatch policy,
  secret isolation, no callback on rejection and current existing tests.

## 2. SQLite storage and recovery

Files: SQLite module, preserved v2 schema/new v3 migration, focused SQLite tests.

- [x] Write failing v1/v2 migration, issuance/acceptance rollback, corruption,
  repeated pending-read/reopen and accepted-before-dispatch restart tests.
- [x] Assign immutable generations and add dedicated strict grant/acceptance
  records through exact transactional v3 migration.
- [x] Implement ApprovalStore using shared validation; update execution/resource
  revisions and journal in the same transaction as grant/decision records.
- [x] Qualify both backend semantics and actual trigger-aborted rollback;
  reopen and explicitly recover accepted work without another approval.

## 3. Review and runnable example

Files: root-owned SQLite approval example, design/ADR/storage/security/compatibility,
requirements/checker and new scoped receipts; Cargo.lock root-owned.

- [x] Review design and implementation independently; fix material findings.
- [x] Add a no-provider park/approve/reopen/dispatch example without token output.
- [x] Document sensitive database handling and native-only grant decisions;
  keep HTTP continuation disabled and all broad release gates open.
- [x] Run fresh format/lint/full Rust tests (real TCP enabled), Node fixture tests,
  contract integrity, unchanged 24 reference comparisons and examples.
- [x] Save current artifact hashes and a new durable-approval verification receipt.

Completed with 217 local Rust tests, 13 Node fixture tests and four Python disclosure
regressions. Independent review found and verified repairs for excessive approval
JSON depth, escaped-token checker disclosure and inherited owner-lock lifetime.
HTTP continuation, broad release scenarios and CI execution remain open.
