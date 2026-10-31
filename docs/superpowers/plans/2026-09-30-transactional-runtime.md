# Transactional runtime implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and test-driven-development. The user authorized continuation; implement and review this bounded increment in the current Rust workspace.

**Goal:** Persist kernel transitions through shared memory/SQLite transactions and invoke scripted effects only after committing their attempts.

**Architecture:** Add `b4-runtime` for store records, fences, validation, memory transactions and the synchronous host service. Add `b4-sqlite` for the same contract, exclusive ownership and durable snapshots. Preserve the pure kernel and existing foundation profile.

**Tech stack:** Rust, Serde, a qualified bundled SQLite driver, an OS file-lock library; explicit caller time/identities.

See `spec/runtime/transactional-runtime-design.md` for exact scope and exclusions.

## Task 1: Shared store and memory backend

Files: `crates/b4-runtime/{Cargo.toml,src/lib.rs,src/store.rs,src/memory.rs,tests/store.rs}`; root `Cargo.toml`.

- [x] Define the public store records, fence, error and synchronous trait.
- [x] Write common backend assertions for one-winner admission, stale commit/owner rejection, immutable identity/definition, duplicate no-op, retained terminal history and ordered journal.
- [x] Observe failures against the unimplemented memory backend.
- [x] Implement mutex-backed transactions without exposing mutable state.
- [x] Run `cargo test -p b4-runtime --test store` and review contract compliance.

## Task 2: SQLite backend

Files: `crates/b4-sqlite/{Cargo.toml,src/lib.rs,src/schema.sql,tests/persistence.rs}`; dependency lock.

- [x] Qualify exact dependency versions and Rust compatibility against primary sources.
- [x] Write failing common-contract, reopen, rollback, corruption, unsupported-schema and process-lock tests.
- [x] Implement exclusive lock, durable owner epoch, schema initialization and verified PRAGMAs.
- [x] Implement all Store transactions; validate payload/index consistency on reads.
- [x] Run `cargo test -p b4-sqlite` and review ownership/transaction behavior.

## Task 3: Runtime and effects

Files: `crates/b4-runtime/{src/runtime.rs,tests/runtime.rs}` and `crates/b4-sqlite/tests/runtime.rs`.

- [x] Write failing tests for persisted InFlight observed by callback, failed-commit zero calls, callback/completion failure, approval sibling retention and stale-owner completion after reopen.
- [x] Implement admission, fenced commands, recovery and one-call dispatch through Store.
- [x] Keep callback errors uncertain; never replay callbacks on completion conflicts.
- [x] Run focused runtime tests with both backends.

## Task 4: Review and evidence

Files: `README.md`, `spec/runtime/{storage-contract.md,requirements.yaml}`, `spec/runtime/decisions/0005-transactional-snapshot-store.md`, `conformance/expected/transactional-runtime-verification.json`.

- [x] Perform independent spec and code reviews; reproduce and fix material findings.
- [x] Run pinned-toolchain formatting, lint, all workspace tests, foundation CLI and contract validation.
- [x] Record exact bounded evidence, dependency versions, local platform and remaining release gates.
- [x] Update scope documentation and plan checkboxes; preserve all original reference/Foundation evidence.

No commit, push or reference-checkout mutation is required by this increment.

Verification: Rust 1.93.1 on local macOS arm64; 101 tests passed, zero failed,
one ignored subprocess helper exercised by its parent. Formatting, strict lint,
draft contract validation, both unchanged foundation scenarios and the SQLite
scripted example passed. Native M2 remains partial. No reference execution, CI
run, HTTP/grants or full crash matrix is claimed. Detailed evidence is in
`conformance/expected/transactional-runtime-verification.json`.
