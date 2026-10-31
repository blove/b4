# Native application registry and capabilities implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development and
> test-driven-development with independent spec and quality review. The user's
> continuous-build authorization covers implementation. No commit/publication
> or new checkout is requested; preserve the current shared workspace ownership.

**Goal:** Run immutable registered native Rust applications through authenticated
HTTP and expose an honest, protected description of the assembled host.

**Architecture:** Runtime owns immutable definitions, input validators and exact
native callback bindings. HTTP adapts the registry to its existing single driver,
approval authority, producer lifetime and AP projection. Store metadata is an
explicit conservative declaration. Typed capabilities describe this native subset.

**Tech Stack:** Existing six Rust crates, standard-library Arc/maps/scoped threads,
Axum/Tokio, MemoryStore and SQLite; locked offline dependencies and schema3 unchanged.

Design: `spec/runtime/native-application-registry-design.md`.
Prior immutable receipt: `conformance/expected/shared-execution-driver-verification.json`.

## Task 1: Freeze reviewed interfaces and evidence (root)

- [x] Resolve independent registry/authority/descriptor design findings.
- [x] Freeze exact portable APIs, common route manifest, HostLimits and descriptor
  fields before agents edit shared source. Keep current script constructors/fields.
- [x] Capture baseline hashes, including the493-test receipt and eight example
  receipts. All existing conformance artifacts stay byte-for-byte unchanged.
- [x] Independently review this plan and explicit file ownership before execution.

## Task 2: Immutable native registry (root)

Files: create `crates/b4-runtime/src/application/{mod,model,registry,validation}.rs`,
modify runtime exports; create `crates/b4-runtime/tests/application.rs` and helpers.

The portable boundary is:

```rust,ignore
pub enum NativeOutcome { Succeeded(TaskResult), Failed(String) }
pub trait NativeEffect: Send + Sync {
    fn invoke(&self, call: &EffectInvocation, context: &InvocationContext)
        -> Result<NativeOutcome, EffectUncertain>;
}
pub trait InputValidator: Send + Sync {
    fn validate(&self, input: &serde_json::Value) -> Result<(), InvalidApplicationInput>;
}
```

`ApplicationIdentity`, `InvocationContext`, `EffectBinding`, `RouteRegistration`
and `NativeApplication` use the design's checked identity/revision and immutable
ownership. Context includes original thread/generation/origin, exact execution
revision, application revision and policy identity; it has no credential fields.
Native outcome deliberately cannot represent the host's Cancelled acknowledgement.

- [x] Write construction refusal tests with callback/validator counters zero:
  malformed/bounded identity,0/65 routes,0/65 tasks, bad budget, duplicate route,
  invalid kernel definition and missing/extra effect bindings.
- [x] Add exact invocation tests selecting full registered Definition: route,
  revision, task/effect and application/context mismatches cannot use a fallback.
- [x] Record meaningful behavioral RED with a minimal API stub or deficient
  validation, then implement atomic temporary-map validation and immutable lookup.
- [x] Add void/null/falsy and known Failed/uncertain outcome tests. Void restricts
  only success output null: non-null Void success faults before durable completion;
  Void Failed remains an ordinary durable known failure. Callback objects/validator
  captures are absent from Debug.
- [x] Run `cargo test -p b4-runtime --test application --locked --offline` and
  strict scoped lint; obtain independent spec review, then quality review.

## Task 3: Explicit store capability declarations (service author)

Files: runtime `store.rs`/`memory.rs`, SQLite `lib.rs`; new runtime/SQLite capability
metadata tests. Runtime exports already forward public store types.

- [x] Define finite `StoreKind` and `StoreCapabilities` with conservative default
  Unspecified/false; add default `Store::capabilities` and Arc/reference forwarding.
- [x] Write RED tests proving default metadata does not advertise optional ports,
  and wrappers preserve explicitly declared metadata without probing/mutation.
- [x] Implement Memory and SQLite declarations for actual fenced cancellation,
  deletion and runtime durability; preserve existing optional methods/501 behavior.
- [x] Approval durability uses the existing ApprovalStore declaration/config mode,
  not backend names or a router path. Test metadata against actual qualified ports.
- [x] Run scoped tests/lint and independent spec/quality review; do not edit schema,
  migration SQL, dependency manifests or shared HTTP source.

## Task 4: Native HTTP assembly and callback execution (host author)

Files: new `crates/b4-http/src/application.rs` and private route adapter, existing
host/worker/approval/transport/recovery/attachment and HTTP exports. Add separate
`tests/native_application.rs`/helpers. Own these integration files exclusively.

- [x] Add common immutable route manifest/adapter so all authority/recovery and
  attachment checks compare the same full Definition for script and native routes.
- [x] Extract effective `HostLimits` while retaining existing HostConfig public
  fields/defaults and script constructors. Add explicit NativeHostConfig and
  `new_with_application<S: ApprovalStore>`; reuse ApprovalBridge even with no prompts.
- [x] Before implementing behavior, add meaningful RED native wait/stream invocation,
  exact null/void/falsy, native parallel overlap and approval park/Once tests.
- [x] Guard native application scope on every protected router/embedding recovery
  entrypoint before Store/route diagnostics; preserve public liveness/readiness.
  Native missing/private fresh routes both404 resource_unavailable; legacy unchanged.
- [x] Run optional input validator in owned bounded blocking producer after resource
  authority and run permit/thread claim reservation, before implicit creation,
  admission or grant consumption. Refusal is400 invalid_application_input; panic
  statically faults. Retain all guards through validator quiescence/disconnection.
- [x] Build transient context from original server-owned accepted thread and exact
  application/definition/policy; never from submitted metadata/input. Driver gate
  alone returns Cancelled before callback entry; NativeOutcome cannot supply it.
- [x] Reuse shared preparation/invoke/completion and existing terminal projection.
  No native raw SSE port. Inactive attachment preserves null Done even for Void.
- [x] Test renewed policy/ABA refusal, empty prompt/native ordinary dispatch renewal,
  input refusal/panic no implicit thread, exact revision absence/difference, recovery
  foreign-application reads0/callback0, wrong-app/denied validator0/effect0, and
  no latest-handler fallback.
- [x] Test cancellation/failure/uncertainty drainage, retained actual results, thread
  exclusion/capacity and request abandonment through actual native callback/validator
  barriers. Run existing HTTP suite and independent spec then quality review.

## Task 5: Capability contract and protected endpoint (root + service after task3)

Root owns `crates/b4-contract/src/capabilities.rs`/exports, new contract tests,
`contracts/agent-protocol/schemas/capabilities.json`, OpenAPI/examples/manifest and
checker changes. Service owns new HTTP `capabilities.rs` and its own capability
API/TCP tests; host author integrates endpoint routing/export only after agreement.

- [x] Freeze typed descriptor fields in design: native subset/profile freeze false,
  version, nullable script application identity, immutable native identity, installed
  mechanism flags, StoreKind/durabilities and all effective finite resource limits.
- [x] Explicitly mark scans/Always/providers/event replay/native live chunks/memory/
  workspaces/AGUI/dynamic graphs/deadlines/shutdown false or absent. Describe
  unbounded callback/validator duration and thread/history retention honestly.
- [x] Describe current readyz status-only aggregate; avoid changing its wire response
  or inventing per-check results merely for capability metadata.
- [x] Write authored schema/DTO examples and rejection tests before implementation;
  capability data contains no route inventory/count, prompts/tokens, captures or paths.
- [x] Implement GET /b4/capabilities with auth, app scope, ReadCapabilities resource
  policy and no-store. Readiness fault remains503; metadata is not a health promise.
- [x] Qualify script/native/approval/Memory/SQLite/custom-default flags, Arc/& metadata,
  effective limits, denied/foreign caller response, reads/mutation/callback0 and
  real TCP response bounds/private content. Store-unavailable and held-policy-to-
  fault reads return static503 without metadata. Required grants reflect installed bridge
  and explicit durable/volatile mode, never guessed from backend type.
- [x] Run contract/API/TCP checks and independent spec then quality review.

## Task 6: Real transport, restart and compiled example (review author)

After host production freezes, author independent native real TCP tests and a
separate-process ledger harness in new test files. Do not edit shared production.
Host author creates `examples/serve_native.rs`; root creates its verifier script.

- [x] Prove actual native callback/validator barriers retain capacity/store/thread
  ownership after request, primary or attached viewer disconnects.
- [x] Verify prompt cancellation fanout and actual late success/failure/unknown
  behavior using callback counters/ledger; no premature Cancelled acknowledgement.
- [x] Qualify native full-registry exact prior-owner park/Once and accepted known-ID
  continuation. Missing/changed declared execution revision invokes nothing.
- [x] Use actual SIGKILL ledger qualification for Unsafe post-effect/precommit
  uncertainty; no full crash matrix/TypeScript parity assertion.
- [x] Compile/run provider-free native SQLite example with existing local auth/Host/
  Origin guards, deterministic registered parallel and approval routes/capabilities.
- [x] Independently review markers, physical callback entry, no result fabrication,
  no credential output and exact declared scope. Preserve all prior receipts.

## Task 7: Final evidence and continued work (root)

- [x] Update README/ADR/requirements/storage/security/compatibility for this bounded
  native application surface and operational capability endpoint. Keep ten core AP
  operations distinct from the added native operational endpoint.
- [x] Fresh workspace tests, format/strict lint, Node/Python/contracts, all examples,
  exact counts and prior protected hashes. Broaden tests only for changed behavior.
- [x] Save a new immutable verification receipt with honest native/reference scope;
  all56 broad release scenarios remain pending until individually qualified.
- [ ] Continue core work under user authorization until a manual action or approval
  is genuinely required; no routine increment handoff is needed.

Current status: registry14 tests and Store metadata7 tests are independently
specification/quality approved. Native HTTP20 and capabilities API7/TCP3 are independently specification/quality
approved. Descriptor9 tests and schemas are independently approved. Native TCP/
SIGKILL qualification is independently spec/quality approved. Fresh561 Rust,
50 Node,10 Python and all9 examples pass; final receipt records200 local requirements
with all56 broad scenarios stillpending. Continued core work follows receipt freeze.
The separate493-test shared-driver receipt is complete and immutable.

## Task1 interface and ownership freeze

Reviewed portable API names:

```rust,ignore
impl NativeApplication {
    pub fn identity(&self) -> &ApplicationIdentity;
    pub fn definitions(&self) -> impl Iterator<Item = &Definition>;
    pub fn definition(&self, route: &RouteKey) -> Option<&Definition>;
    pub fn validate_input(&self, selected: &Definition, input: &Value)
        -> Result<(), ApplicationInputError>;
    pub fn invoke(&self, selected: &Definition, call: &EffectInvocation,
                  context: &InvocationContext)
        -> Result<EffectOutcome, NativeInvocationError>;
    pub fn output_kind(&self, selected: &Definition, effect: &EffectId)
        -> Option<OutputKind>;
}
```

`ApplicationInputError` has DefinitionUnavailable/InvalidInput; `NativeInvocationError`
uses the design's static variants. Registry invocation converts only NativeOutcome
success/failure to EffectOutcome; host entry gate is the Cancelled supplier. Callback
and validator panics propagate to the qualified owning host/driver catch boundary.
Application context is transient and trusted for embedding, not standalone authority.

`StoreCapabilities` has kind:StoreKind and bool durable_runtime/fenced_cancellation/
fenced_deletion. StoreKind is Unspecified(default)/Memory/Sqlite. Default optional
flags arefalse; Arc/& forwarding calls the underlying declaration. ApprovalStore
durability stays separate from explicit service mode.

`HostLimits` contains the current HostConfig effective limit fields with identical
defaults. `NativeHostConfig` contains application:Arc<NativeApplication>, limits:
HostLimits and approval:ApprovalHostConfig; its new(application,approval) uses default
limits. `HttpHost::new_with_application(store,auth,policy,config)` is additive.
The host author owns private `crates/b4-http/src/route.rs` plus all shared integration
files, including HTTP exports/routing. Service owns NEW capabilities.rs and NEW
`tests/capabilities_api.rs`/`tests/capabilities_tcp.rs`; it requests narrow export/
routing glue from host author. Root owns capability DTO/schema/manifest/contract tests.
No two agents edit runtime/lib.rs or existing shared HTTP files concurrently.

Design independently approved by restart_review; plan independently approved by
registry_plan_review. Service authority/metadata review corrections incorporated.
No user confirmation needed under the explicit continuous-build authorization.
