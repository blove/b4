# Bounded historical native Definitions Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans task by task.

**Goal:** Drain exact recorded native Definitions from explicit approval-free history; new work stays current.

**Architecture:** Immutable exact registration tables reuse owned workers. Resume returns pure policy metadata and reselects resources under counted ownership.

**Tech Stack:** Existing Rust/Tokio/Axum/SQLite; locked/offline, no dependencies.

Status: accepted implementation plan, 2026-10-02. The predecessor startup increment is verified and its final source is preserved. Follow the [accepted design](../../../spec/runtime/historical-native-definitions-design.md). Execution is staged below; checkboxes record completed work.

## Ownership and fixed boundaries

Portable author: ONLY `crates/b4-runtime/src/application/registry.rs` and `validation.rs`; NEW private `application/history_tests.rs` registered from selected registry.rs. HTTP author: ONLY `crates/b4-http/src/route.rs`, `host.rs`, `application.rs`, `recovery.rs`, `approval.rs`, `transport.rs`, `attachment.rs`. These are nine existing production exceptions.

Different qualifier author: NEW `crates/b4-runtime/tests/native_history.rs` and optional `native_history_support/*`; NEW `crates/b4-http/tests/native_history_recovery.rs`, `native_history_startup.rs`, `native_history_tcp.rs` and `native_history_support/*`. Implementers never edit these expectations. Root owns later support.

Preserve all old tests/APIs/config/errors, worker/lifecycle/startup/admission/mapping/SQL, schemas/dependencies. No Git/provider/client execution or baseline before dispatch.

## Task 1: compiling API and independent positive REDs

- [x] Root accepts post-freeze baseline/design/ownership.
- [x] Add inherent `new_with_history(identity, current: Vec<RouteRegistration>, historical: Vec<RouteRegistration>) -> Result<Self, ApplicationConfigError>` and `definition_exact(&Definition) -> Option<&Definition>`. Stage delegates to old `new`, deliberately ignores history and recognizes only whole-equal current definitions.
- [x] `cargo check -p b4-runtime --locked --offline`; freeze compiling stage and pause production.
- [x] Independent portable positive installs current-v2/retained-v1 with same effect spelling and distinct results; require exact v1 invocation. Capture compiling missing-history assertion RED.
- [x] Real SQLite positive seeds legal v1 Accepted/Pure-inflight with origin/full views, drops ALL owners and reopens owner+1. Trusted recovery under current-v2/retained-v1 must adopt v1; current-only HTTP gives RED. Cleanup precedes assertion; exclude setup failures.
- [x] Root accepts both immutable actual REDs before either implementation GREEN.

## Task 2: portable exact registry and validation

- [x] Private REDs: nonempty current; combined64/65; pair collisions despite changed content; duplicate history; removed routes; ANY historical approval batch; current approval unchanged.
- [x] Preserve old `new` validation/error order. Additive constructor checks aggregate size before maps and validates every entry. Current key is RouteKey; history key `(RouteKey, ExecutionRevision)`, unique across both tables. Historical approval gets static config refusal.
- [x] Borrowed exact selector checks pair then full Definition equality for `definition_exact`, `validate_input`, `output_kind`, `invoke`, `invoke_controlled`. Callback/validator/output bindings come only from the selected entry. Old `definitions()`/`definition(route)` remain current-only.
- [x] Test assembly-v2/execution-v1, output/validator/membership, pre-callback refusal and old outcomes; no historical assembly revision.
- [x] Run private/independent targets, `cargo test -p b4-runtime --all-targets --locked --offline`, strict scoped lint/owned format. Freeze source/results/old-body inverses; complete Root SPEC and nonauthor QUALITY before HTTP GREEN.

## Task 3: common recorded HTTP selector

- [x] route.rs helper takes explicit immutable native app, current routes and recorded Definition. Native uses `definition_exact` before constructing RegisteredRoute; scripted preserves lookup/full equality.
- [x] Reuse helper in recovery preflight/later lookup, resume, attachment checks and ApprovalBridge authority. Preserve the existing scripted `bridge(...)` builder and caller behavior; add a separate native-aware private builder if needed. Public config forms, renewal/fences and existing driver stay exact.
- [x] Resume policy job uses the matching original view. Return pure ThreadView/provenance/support metadata only; no registration/application/callback/validator/Store through JoinHandle. Reselect after await under endpoint OwnedShared; retain original view and move resources into existing worker carrier.
- [x] New work and prompt coverage remain current-only; prompt lookup checks current whole Definition. No history prompt borrowing, recovery input revalidation, route fallback or second effect loop. No selected startup/admission/mapping/worker changes.
- [x] Independent positives to GREEN, old recovery/resume/approval/attachment targets, strict scoped lint/owned format. Freeze seven files/full inverses for complete Root SPEC and nonauthor QUALITY.

## Task 4: independent actual qualification

- [x] Real SQLite v1-to-v2 startup and typed/legacy known-ID recovery prove v1 callback/output/attempts/context; authenticated new work proves v2 callback/validator. Include removed-route drain/new-route refusal and exact live/settled attachment.
- [x] Missing/altered v1, foreign actor, revoked policy, stale fence/generation, successor/current-owner and approval history refuse without latest fallback/confinement. Compare whole ThreadView/ApprovalView and private six-table authority state.
- [x] Current-v2 durable park/Once/Deny/TTL/pending remain exact alongside approval-free history with matching labels. Portable old validator selection is separate from HTTP NEW-only validation.
- [x] Abandon held Resume policy with Store-bearing callback/validator captures; detach held callback Response/waiter. Prove explicit nonexpired return, normal join/Quiescent, both Weak absences and writer owner+1 with pure values alive; timeout/forced cleanup is not retirement.
- [x] Freeze qualifier sources/results; different-author complete review and dependency binding, without implementer self-approval.

## Task 5: final bounded checks and support

- [x] After source gates freeze inputs. Run focused qualifiers and workspace `--all-targets --locked --offline`, strict lint/format on macOS and retained isolated offline Linux. Include AP Rust example, exclude nested repeats; no original TS/Node corpus rerun.
- [x] After actual evidence add a separately counted historical-definition requirement group and factual support/receipt. Preserve old groups/statuses/receipts; RFC11.4 confinement/reconciliation/incompatible-policy persistence remain incomplete.
- [x] Independent support/evidence audits and final freeze; no automatic next source increment, publication or commit.

## Actual completion record, 2026-10-02

All 24 tasks above are complete. Independent source and evidence reviews accepted the exact portable registry, HTTP selection and qualifier changes. Actual macOS and isolated offline Linux runs each passed 1,084 top-level Rust test entries across 98 targets, with zero failures and three historical ignored child helpers exercised by their parent tests. The raw 1,086 pass entries include two nested child repeats, excluded from the top-level entry count. All 48 added Rust declarations passed once in their intended targets on each platform. Strict workspace lint and format checks passed. The initial restricted macOS attempt was an environment failure at listener binding; it is preserved separately and excluded from passing or feature-RED evidence.

The final reviewed support changes add 31 implemented requirements qualified only for explicit approval-free retained native Definitions. Actual checks after that documentation adoption passed 102 Python tests and the contract checks. The platform compilation input contained 662 files with those rows still pending; the final support input contains 663 files, including the new [verification receipt](../../../conformance/expected/historical-native-definitions-verification.json). All 246 compiled crate files, nine manifest/lock inputs and 636 protected inputs are unchanged. This final plan checkbox/record update adds no code change or new platform execution claim.

The old requirement groups and verification receipts remain preserved. The 56 core cases still awaiting execution, M0, CP-010 and the broader RFC11.4 restart/readiness, confinement, reconciliation and policy-history work remain open. Further source increments follow the user's explicit continuing request and require their own current baseline and reviewed design; this completed plan records the retained-definition increment only.
