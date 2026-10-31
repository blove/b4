# Two-lane process startup restart qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task by task. Steps use checkboxes and separate complete source reviewers.

**Goal:** Qualify two real-process restart/readiness scenarios using existing native startup/recovery APIs.

**Architecture:** Two parent tests own seed/restarted children and a bounded private Unix control channel. Child2 performs isolated bounded preparation; independent post-kill and post-retirement SQLite rereads anchor complete typed state/authority/journal evidence. Production bodies and current launcher defaults remain exact.

**Tech Stack:** Existing b4-http/dev b4-sqlite, Rust/Tokio/Axum/serde; locked/offline. No new API/manifest/lock/schema/provider/direct rusqlite code, Git mutation or original-client execution.

---

Status: qualified implementation plan, 2026-10-03. Both real-process restart lanes passed on macOS/Linux, with separate metadata RED/GREEN, reviewed support and actual post-documentation Python 108/contracts checks. Read the [qualified design](../../../spec/runtime/startup-process-restart-design.md). Final source bindings are recorded separately; broader restart/release scopes remain open.

## File ownership and tests

One qualifier author owns only NEW crates/b4-http/tests/startup_restart.rs and NEW startup_restart_support/{mod,model,process,fixture,server,evidence}.rs. Target contains exactly two credited parent tests process_startup_park_preserves_once_and_sibling_without_replay and process_startup_unsafe_effect_is_blocked_before_readiness_without_replay, plus one ignored startup_restart_child_helper selector with four finite role/lane values. No helper file has test declarations. No existing tests/helper expectation/body changes. Root owns any later support/trace/runbook/receipt in a separate reviewed scope.

## Task1: accepted scope and next baseline

- [x] Finish current confinement final platform/support/freeze. Preserve all preceding evidence and original partial delete/reincarnation qualification.
- [x] Root and a different author whole-review this exact design/plan, source descriptor packet, source trust/caps/cleanup and IPC model. Correct concrete findings privately with exact immutable inverses before public adoption.
- [x] Root captures actual next baseline/protected/source/dependency maps before new files. Production exceptions are zero for this two-lane qualification; any material production gap gets a separately authorized design/delta/TDD gate.
- [x] Re-read actual Store/ApprovalStore/Preparing APIs. Confirm capture wrapper fields are serializable and no ApprovalView derive is added. Reuse semantic patterns, not existing test declarations or fixture recovery route.

## Task2: new bounded fixture/helpers and source-review barrier

- [x] Author the seven NEW files privately first. Implement Lane{Approval,Unsafe}, Role{Seed,Restart}, closed version1 hello/ordinal commands/replies and fixed socket/capture names. Pin max1024 request/max2048 reply/max16 commands/30s IO waits/60s gate/120s lane watchdog.
- [x] Define private Capture from complete ThreadView plus approval thread/execution/grants/acceptances and ordered journal. Use capped serde writer to enforce1MiB before persistence and check1MiB before parse. Check fixed fixture<=1 grant/acceptance,<=64 journal entries. Explicitly label old Store loaders as trusted finite fixture collection, not a general bounded capture API.
- [x] Implement private700 root/600 create-new evidence and actor-controlled current Definitions. Approval route has Pure sibling+required Pure release; Unsafe route has a true synchronized local external ledger then no returned result; unrelated Pure route is independent. Fixed exact server-owned origin/policy, RequiredDurable, max_attempts4/capacity4/concurrent2. No callbacks or input-dependent script fixtures masquerade as effects.
- [x] Implement UnixListener-before-spawn private IPC, actual PID/role/lane/owner/loopback hello, owned-child kill/reap/normal-exit state, bounded stdout/stderr drains and sticky loss. Keep diagnostic harness output out of protocol and parent Cargo stdout. Ensure control reader owns only pure messages, exits normally on stop and joins; no Store/app captures leak through it.
- [x] Implement actual plain preparing/ordinary router listener, bounded original HTTP content-length/chunk decoding, JSON/SSE inspection and finite EOF. Do not add /fixture/recover, Response mocks/clones/adaptation or a manufactured initial stream frame.
- [x] Implement seed stable milestones, actual full capture and explicit resolver gate witnesses; Child2 begin_preparation/max_pages1/max_recoveries1 reads only its selected unfinished row. No parent aliases/membership writes exist during this pass. Complete separately precedes terminal/stop/Weak proof.
- [x] Root SPEC then another author QUALITY whole-read all seven private sources against exact design before adoption/compilation. Preserve full source copies and old/protected inputs. Existing APIs need no artificial Unsupported behaviorRED; no future pass is claimed.

## Task3: approval parent lane

- [x] Create real seed child/current native assembly; issue original authenticated wait request. Require actual stable park, succeeded sibling/result/ordered writes, real unconsumed grant and exact attempt budget. Save private checkpoint before SIGKILL.
- [x] Parent kills/reaps actual signal9 after persisted marker, closes original client, opens real SqliteStore once, validates readiness/ordinary/full approval/journal and source equality. Record inspector owner, then normally drop ALL aliases and check Weak absent before Child2. Do not hardcode Child2=seed+1.
- [x] Start actual Child2 with Preparing router and recorded actual owner newer than source/inspector. Probe real health200/ready503/ordinary503 before begin; begin, wait actual held resolver entry and repeat closed probes. Release explicitly/nonexpired and observe actual Finished(Complete) with park counters, then ready200.
- [x] Capture adopted complete view/authority and journal. Independently validate new owner/fresh turn/time interval/revision against original; tasks/grants/attempt budget/results/Definition/input remain preserved. Do not derive expected preserved fields from the post-state itself.
- [x] Obtain same pending interrupt/grant and submit original full resolved Once through authenticated resume. Read real response/terminal EOF, wait actual Completed and save complete final capture/ledger. Require sibling once and release once; only expected grant consumption/new acceptance/attempt/reduction changes.
- [x] Stop normally: actual healthy quiescence, listener join, drop every Store/app/Host/caller alias, assert both Weak absent while observation handles remain pure, then normal runtime/helper exit0/reap. Parent opens real next writer equal observed Child2+1, verifies readiness/full final selected authority/journal/ledger and closes. Semantic assertion follows physical cleanup; failed cleanup cannot pass.

## Task4: Unsafe parent lane

- [x] Send original stream request and retain real pending body/socket; do not require initial frame. Confirm synced external ledger1 and actual Running/InFlight/no result/attempt1 capture before intentional SIGKILL/reap and actual EOF/handle cleanup.
- [x] Independently open real SqliteStore after kill, compare complete source/approval/journal with child export, bind actual epoch and normally drop aliases before Child2. No parent writer/probe Store remains during preparation.
- [x] Repeat real Preparing/held resolver/ready503 cuts, release and require Complete blocked1/ready200. Capture conservative RecoveryBlocked/OutcomeUnknown with original attempt identity/history/budget/input/Definition, no fabricated result and zero Unsafe dispatch.
- [x] Complete unrelated Pure work after preparation without changing selected ledger. Normal healthy Child2 stop/listener/Weak/exit/reap and parent next writer+1 persist the block and exactly one Unsafe ledger entry. No external-effect rollback/exactly-once claim.

## Task5: first actual compilation and focused evidence

- [x] After source gates and exact adoption, Root schedules compilation on frozen inputs: cargo test -p b4-http --test startup_restart --locked --offline --no-run. Preserve compiler/setup output separately; not a behavioral RED. No concurrent Cargo/source edits.
- [x] Run exactly first parent approval case: cargo test -p b4-http --test startup_restart --locked --offline -- process_startup_park_preserves_once_and_sibling_without_replay --exact. Expected future successful result is1 parent pass; actual result is recorded, never asserted in advance. Loopback escalation is routine locally authorized if sandbox needs it.
- [x] Run exactly Unsafe parent: cargo test -p b4-http --test startup_restart --locked --offline -- process_startup_unsafe_effect_is_blocked_before_readiness_without_replay --exact. Archive all selected source/input maps and private child evidence before/after each command.
- [x] Classify actual failures: setup/IPC/codec/permission/compiler/timeout/cleanup defects get private source-backed corrections with complete initial sources/logs preserved; runtime invariant mismatches require independent root-cause review and meaningful compiling regression before production repair. Do not create fake featureRED merely because an existing API works.
- [x] Run target once without helper selections, strict lint cargo clippy -p b4-http --all-targets --locked --offline -- -D warnings and owned-only rustfmt --edition 2021 --config skip_children=true --check over seven new files. Derive actual2 parent/ignored helper counts; focused repeats/private child summaries never add credited cases. Existing old targets are later covered by frozen broad suites rather than gratuitous repeats.
- [x] Freeze whole new sources, exact old/protected/dependency invariants, source-bound actual logs, observations, epoch chain, private quota/modes, normal/negative cleanup classification. Root SPEC and different-author final source/execution QUALITY inspect full two lanes before broad platform or public passing claims.

## Task6: qualified support and final platform gate

- [x] Root freezes actual complete next inputs and performs fresh Mac/Linux qualified workspace/lint/format runs with the accepted isolated offline drivers. New helper ignored marker is not a pass; children stdout remains private so nested results must not be double counted. Platform result totals come from actual parser and name proof only.
- [x] Root drafts separate narrow two-lane requirements/runbook/safe receipt from actual evidence. Preserve all preceding groups/M0/CP010/broad56/CLI/default-source and client evidence. No automatically invented full RFC11.4 status, third-lane success or human confirmation requirement.
- [x] Whole independent support/privacy/inverse/evidence review precedes adoption, actual post-support metadata/Python/contracts if needed, and immediate final source freeze. No commits, external publication or automatic next increment.
