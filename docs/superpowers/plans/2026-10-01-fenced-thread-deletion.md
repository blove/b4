# Fenced thread deletion implementation plan

Use the retained subagent development, test-driven development and independent
review workflows. User authorization covers this native continuation.

Design: spec/runtime/fenced-thread-deletion-design.md. No schema/dependency change.

- [x] Independently review exact target, safety classification, policy/claim and
  atomic cleanup design against current stores/worker and frozen reference.
- [x] Add typed target/default Store capability, shared exact validation and
  MemoryStore deletion with meaningful failing tests before implementation.
- [x] Add SQLite immediate transaction cleanup with FK-safe order, fault rollback,
  actual reopen, fence/ABA/uncertainty and authority-record tests.
- [x] Share an owned per-thread claim helper with HTTP workers; add authenticated
  deletion, renewed origin/policy and healthy typed errors with API/TCP/race tests.
- [x] Independently review each implementation and the integrated increment;
  repair demonstrated findings with regressions.
- [x] Reproduce the admission ABA found in integrated review, extend atomic fenced
  admission with generation and renew new-worker policy on ordinary hosts; direct
  backend and both HTTP policy-boundary regressions pass.
- [x] Update selected OpenAPI/requirements/compatibility scope and add native
  lifecycle status comparison without modifying frozen reference evidence.
- [x] Run relevant tests then fresh workspace format/Clippy/tests and existing
  examples/checkers; preserve historical receipts and save a separate receipt.
- [x] Continue cancellation and attachment design from the captured evidence;
  owned cancellation design and plan now proceed independently.
