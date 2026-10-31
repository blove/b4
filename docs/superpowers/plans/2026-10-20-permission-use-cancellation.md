# Permission-Use Cancellation Implementation Plan

> Use superpowers:subagent-driven-development with fresh author per task and independent source then quality/results review. Continuous authorization; isolated ordinary source copy, no Git writes or routine approval pauses.

**Goal:** Persist cancellation for an already-entered permission-use callback and settle its actual controlled receipt without replay.

**Architecture:** Explicit cancellable driver entry, dedicated current management producer and default-refusing typed atomic store ports. Extend only the real NewUse CancelRequested4→terminal5 relation; preserve default/direct/original and HTTP behavior.

**Tech Stack:** Rust1.93.1, current runtime/kernel/native controlled callback, SQLite immediate transactions; pinned offline verification.

## Task1: Genuine cancellation expectations

- [x] Independent design/plan review, isolated source baseline and bounded verification controller.
- [x] Fresh author adds genuine NewUse callback-entry cancellation tests using proposed request/driver APIs. Include acknowledgement, actual settled outcome after cancellation, normal controlled success, current/stale refusal, signal-only refusal and exact request/terminal journal rollback/retained-work retry controls without an exhaustive matrix.
- [x] Preserve existing tests. Source gate, Root actual absent-API RED, independent quality/results gate. Test callbacks/threads/store owners have finite waits and normal retirement/reopen/removal.

## Task2: Controlled receipt and durable request/settlement

- [x] Fresh implementer adds explicit driver entry and controlled receipt route while preserving existing direct entry. Add dedicated request type/producer and mandatory narrow default-false store opt-in with Arc forwarding/SQLite true.
- [x] Implement typed current NewUse Dispatch3→CancelRequested4 request writer and actual receipt-backed CancelRequested4→terminal5 source/write relation, including Driver completion/candidate tickets and exact immutable authority/Use/index/history/fence checks. Preserve actual kernel success/failure semantics after cancellation; callback entry is qualified caller sequencing, not inferred from Dispatch3. No raw fallback, fabricated receipts or signal authority.
- [x] Preserve real Work on storage failure; storage-only explicit retry and repeat-after-success refusal. Keep old profiles and HTTP guards/metadata untouched, bounded existing accounting, no operation audit.
- [x] Source gate, Root focused native GREEN, quality/results review; fix only concrete failures.

## Task3: Integrated verification

- [x] Fresh focused boundary author closes any missing planned controls using actual producers, callback outcomes and exact transaction guards; source→native→quality review.
- [x] Format, strict all-target lint, workspace compile and full locked/offline workspace regression. Independent integrated source and results review before adoption.

## Task4: Document and adopt

- [x] Exact selective adoption with backups, scoped qualification/API usage notes and explicit exclusions (original/pre-entry/HTTP cancellation still disabled).
- [x] Public metadata checks remain separate from runtime evidence. Record actual final source/results, close this increment and continue the next cancellation stage unless the user steers elsewhere.

Final implementation and integration evidence: eight focused tests; strict all-target lint, formatting, workspace compile and full workspace regression pass (1,575 passed, zero failed, 11 existing ignored). Exact adoption and scoped qualification are recorded in `.b4-work/permission-use-cancellation/adoption/`; public metadata is checked separately at adoption. No Git writes or broader cancellation capability was enabled.
