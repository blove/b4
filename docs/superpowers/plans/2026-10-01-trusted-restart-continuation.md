# Trusted restart continuation implementation plan

> Use superpowers:subagent-driven-development and test-driven-development.

**Goal:** Explicitly recover a known prior-owner execution and continue its
committed required approval through the supervised scripted host.

**Architecture:** Same-store approval Recover mutation atomically fences resource,
execution and incarnation; a trusted embedding target enters the existing owned
worker after current authority and capacity/claim checks. No public recovery
endpoint, scanner or schema migration.

**Design:** spec/runtime/trusted-restart-continuation-design.md.
**Stack:** Existing Rust/SQLite/Axum/Tokio, locked cached dependencies.
Continuous implementation is authorized; no commit/push/publication requested.

## 1. Durable service and native storage

Own crates/b4-runtime/src/approval/{model,service,validation}.rs and new
crates/b4-runtime/tests/approval_recovery.rs; only touch native storage files if
required by the new mutation, coordinate that ownership before edits. New SQLite
tests in crates/b4-sqlite/tests/approval_recovery.rs.

- [x] Write failing recovery service/target/fences/definition and prior-owner tests.
- [x] Add Recover operation/request/mutation and prior-owner authorized path.
- [x] Under existing backend transaction validation recompute exact recovery plan,
  checking target generation/IDs/execution/resource fences, preserving authority rows.
- [x] Qualify resource-only conflict, forged plan, rollback, stable grants/audit,
  expired accepted Once, Pure retry/Unsafe block/Cancelled and same-owner refusal.
- [x] Run scoped runtime/SQLite tests, lint and freeze service API for host.

## 2. Trusted host and owned worker

Own crates/b4-http/src/{host,approval,worker,lib}.rs and new
crates/b4-http/tests/recovery.rs and recovery_tcp.rs. Do not edit service/backends.

- [x] Write failing explicit target/API/current-policy/no-mutation tests.
- [x] Add Operation::Recover, trusted coherent-view target constructor, bridge
  port and async embedding recovery entry point with fixed errors.
- [x] Extend owned Work with recovery, reserve capacity/claim before durable commit,
  renew fresh tokenfree identity/definition/policy before admission and worker mutation.
- [x] Handle Accepted/Running/parked/blocked/cancelled/failed dispositions; retain
  guards and owner lock through disconnect and quiescence.
- [x] Run meaningful actual TCP accepted-before-reopen/TTL/park/disconnect tests,
  dispatch renewal and benign conflicts; freeze scoped source after lint/format.

## 3. Independent review and full verification

- [x] Review design/plan before implementation; address material findings.
- [x] Independently review service/host transaction, authority, lifecycle and errors.
- [x] Update docs/ADR/traceability/example only for newly qualified behavior.
- [x] Run full workspace tests with real loopback, strict lint/format, Node/Python
  checks, contract integrity and relevant examples. Retain original reference evidence.
- [x] Save separate trusted-restart verification receipt and artifact hashes.
- [x] Continue core work after this slice verifies until user/manual action needed.

Verified native increment: 287 Rust tests passed (one subprocess helper invoked by its parent), 25 retained Node tests and four Python checker tests passed. Strict workspace lint/format, original workflow24 and grant14 comparisons, new embedding recovery example and three retained examples passed. Service/store and host independently reviewed with no outstanding findings. No public HTTP operation or schema/dependency change. New receipt: conformance/expected/trusted-restart-verification.json. Next: lifecycle characterization and native cancellation/deletion/attachment.
