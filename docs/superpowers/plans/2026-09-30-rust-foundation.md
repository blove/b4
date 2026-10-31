# B4run Rust Foundation Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for isolated tasks and review the resulting code before completion. Steps use checkbox syntax for tracking.

**Goal:** Start the empty B4run repository with an executable, deterministic contract/kernel foundation for the supplied native runtime RFC.

**Architecture:** Portable types and strict wire serialization live in `b4-contract`. The pure kernel returns proposed commit plans and contains no I/O. A scripted testkit exercises the kernel and reports narrowly scoped verification evidence.

**Tech Stack:** Rust 1.81, Serde 1.0.217, serde_json 1.0.138. No async, HTTP, provider or SQL dependencies in this increment.

---

### Task 1: Workspace and source evidence

Files: root Cargo manifests/toolchain/gitignore; `spec/runtime/0001-agent-protocol-core.md`; `conformance/reference/manifest.json`; reference source captures.

- [x] Inspect the empty repository and supplied RFC; choose a bounded first increment.
- [x] Pin compiler/dependencies and retain the supplied RFC as design input.
- [x] Record the reference commit and capture hashes without claiming TypeScript execution.
- [x] Record unresolved CP-001 through CP-010 and the remaining milestone gates.

### Task 2: Contracts

Files: `crates/b4-contract/src/{lib,ids,model,wire}.rs`, `crates/b4-contract/tests/{contracts,wire,examples}.rs`; `contracts/agent-protocol/{schemas,examples}`.

- [x] Write failing tests for opaque IDs, snapshot round trips/version rejection, omitted vs null input, strict resolved/cancelled resume entries and direct SSE payloads.
- [x] Run `cargo test -p b4-contract --offline`; confirm absent APIs fail before implementation.
- [x] Implement portable types, DTO parsing and fixed event-name encoding.
- [x] Re-run the contract tests; preserve error details code placement and reject invalid Unicode/legacy resume inputs.

### Task 3: Pure execution kernel

Files: `crates/b4-kernel/src/{lib,transition,reducer,validation}.rs`, `crates/b4-kernel/tests/lifecycle.rs`.

- [x] Write failing deterministic tests for sequential batches, parallel join ordering/conflicts, stable approval parking, full-set resume, duplicate result delivery, exact-turn cancel, pinned recovery and budget preservation.
- [x] Run `cargo test -p b4-kernel --offline`; confirm missing transitions fail.
- [x] Implement `transition(snapshot, command, facts) -> CommitPlan`, returning an expected revision and a replacement snapshot without mutating the input.
- [x] Validate persisted invariants on every transition; block unsafe uncertain effects and reject unknown snapshot versions.
- [x] Run the lifecycle suite and review the commit/dispatch boundary.

### Task 4: Deterministic fixture driver and receipt

Files: `crates/b4-testkit/src/{lib,scripted,scenario,driver}.rs`, `crates/b4-testkit/src/bin/b4-conformance.rs`, integration tests and `conformance/scenarios/*.json`.

- [x] Write a failing fixture-miss test; ensure no fallback or network port exists.
- [x] Write scenarios with independently authored expectations for approval snapshot round trip and unknown effects.
- [x] Commit a separate Dispatch transition before invoking each fixture effect; count dispatches and fail unsupported scenario versions/commands.
- [x] Run `cargo run --locked -p b4-testkit --bin b4-conformance -- --profile foundation`; emit a machine-readable receipt and nonzero status on failure.
- [x] Name snapshot restoration as serialization testing, not SQLite/process restart durability.

### Task 5: Review, documentation and verification

Files: `README.md`, `spec/runtime/{compatibility,state-machines,requirements}.md` or structured equivalents, CI workflow, verification receipt.

- [x] Review code against the bounded design, then review integrity/maintainability.
- [x] Fix material findings and rerun affected tests.
- [x] Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`, `cargo test --workspace --locked --offline`, and the foundation conformance command.
- [x] Save results with explicit limits; document M2 transactional memory/SQLite as the next increment.

The broad RFC is intentionally not marked implemented. M0 remains open until the TypeScript characterization corpus resolves core wire ambiguities. This plan is scoped to starting development, as requested, using the supplied design.

Verification: 67 Rust tests pass, both authored foundation scenarios pass, and the schema/reference artifacts have explicit evidence boundaries. Review findings were fixed with failing regression tests before final validation. Local changes remain on `blove/rust-foundation`; no remote publication or production release is part of this increment.
