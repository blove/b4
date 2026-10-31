# HTTP approval implementation plan

Continuous work is authorized. Use test-driven-development with disjoint agent
ownership and independent design/implementation review. No commit, push or
publication is requested.

**Goal:** Run, park, read and resume required-grant scripted work through the
authenticated HTTP host without losing durable continuation on disconnect.

**Design:** spec/runtime/http-approval-design.md.

## 1. Host, approval bridge and transport

Files: crates/b4-http/src/{host,approval,transport,lib}.rs and
crates/b4-http/tests/approvals.rs. Worker changes belong to the second task.

- [x] Write failing explicit-constructor/storage-mode/prompt bounds and secure
  pending/resume/route/set/token/authority tests.
- [x] Add ApprovalHostConfig, object-safe same-store bridge and scoped authority;
  preserve ordinary Host constructor and publish agreed worker port details.
- [x] Add optional shared approval bridge and per-thread claim set.
- [x] Add authenticated strict POST /threads/{id}/resume and authorized pending;
  preserve ordinary state/no-approval/unsupported behavior.
- [x] Map fixed approval errors and distinguish expected rejection from host
  failures; test precedence, capacity, concurrency and transaction rollback.
- [x] Verify scoped tests and lint; freeze source for full review.

## 2. Worker and real transport

Files: crates/b4-http/src/worker.rs and new
crates/b4-http/tests/approval_tcp.rs. Host/bridge files belong to the first task.

- [x] Write failing stable park, sibling preservation, Once/Deny terminal,
  resume disconnect and permit/claim ownership tests.
- [x] Share owned launch/terminal handling for initial and accepted continuation;
  acquire capacity and claim before acceptance inside a supervised worker.
- [x] Park required tasks via bridge and renew approved dispatch via bridge;
  skip Start for accepted resumes and settle stable park with authorized
  interrupt events before done {output:null}.
- [x] Handle committed Denied output without callback or readiness failure.
- [x] Qualify real TCP resume rejection/disconnect/healthy successor and current
  durable SQLite behavior; freeze source after scoped tests/lint.

## 3. Reference and independent review

Files: new conformance/reference/grant-fixture and a separate grant-recording
directory only. Preserve the workflow fixture and historical recordings.

- [x] Review written native bridge design and implementation independently.
- [x] Inspect/build exact pinned source in isolated temporary checkout with
  frozen no-script dependencies and unchanged source/lockfile.
- [x] Author a deterministic provider-free model/tool fixture through production
  exports and the actual Fetch handler; execute only if genuine grants are
  produced, recording raw bytes and before/after artifact inventory.
- [x] Keep source-only observations and executed reference evidence distinct;
  record any remaining fixture/environment gaps without closing broad gates.

## 4. Documentation, example and fresh evidence

Files: example/verifier, OpenAPI/schema metadata, README, security/storage/
compatibility/ADR/requirements/checker and new scoped verification receipts.

- [x] Add a no-provider authenticated approval example/check that emits no
  tokens to logs/receipts and exercises the public pending/resume path.
- [x] Document native interrupt envelope, wait status divergence, denial output,
  mode/definition/restart/authority limits and remaining core work.
- [x] Run full Rust tests with real loopback transport, strict lint/format,
  Python/Node fixture checks, contract integrity and retained24 comparisons.
- [x] Save new artifact hashes and receipts without changing prior evidence.
- [x] Continue to the next supported increment after this slice verifies.

Fresh verification: 248 Rust tests passed, one subprocess helper ignored but invoked by its parent; 25 Node and four Python tests passed. Formatting, strict workspace lint, contract integrity, original24 workflow and selected14 grant status comparisons, old/new examples passed. Historical evidence is preserved. See conformance/expected/http-approval-verification.json. Continue to explicit trusted known-ID restart continuation.
