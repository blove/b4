# Owned run cancellation implementation plan

Use the retained design, subagent development, test-driven development and
independent review workflows. User authorization covers continuous execution.

Design: spec/runtime/owned-run-cancellation-design.md. No schema/dependency change.

- [x] Independently review exact turn capture, gate, terminal and atomic grant
  invalidation design against the current code and frozen reference.
- [x] Add kernel Accepted cancellation and exact never-entered CancelAttempt,
  typed store target/default capability and Memory implementation with tests.
- [x] Implement SQLite atomic cancellation with grant/receipt preservation,
  rollback, exact fences and actual reopen/recovery tests.
- [x] Add claim registry, run control, renewed authenticated cancellation endpoint
  and worker coordination with meaningful API tests before implementation.
- [x] Qualify real TCP prompt wait/stream, held capacity/store ownership, stale
  policy, abandoned requests and callback-entry boundaries.
- [x] Independently review implementation and integration; repair findings with
  regressions, including conflicts and late settled outcomes.
- [x] Update selected OpenAPI, requirements and compatibility scope; add native
  cancellation reference comparisons without modifying frozen evidence.
- [x] Run fresh workspace checks and examples; save a separate evidence receipt.

Review reproduced and repaired the completion-time/cancellation reload race with
positive and negative API regressions, a real TCP regression and the unchanged
independent probe. Source author also repaired Faulted-gate acknowledgement and
initial-view/control successor capture races before integration review. Checker
review repaired startup-line credential scanning and proved the previous checker
accepted the isolated generated token while the fixed checker rejects it.
- [x] Continue attachment and remaining core work while no manual blocker exists;
  bounded attachment is implemented and separately qualified in
  `conformance/expected/bounded-run-attachment-verification.json`.
