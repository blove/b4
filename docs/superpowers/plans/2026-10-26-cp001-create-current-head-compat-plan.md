# CP-001 Current-HEAD Create Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Rust `POST /threads` create path match the 19 reviewed current-HEAD TypeScript cases, including durable array metadata, while preserving explicit finite host limits and scoped claims.

**Architecture:** Keep strict parsing for non-create routes. Add a bounded create parser and response path, share an object-or-array metadata type across the contract and stores, fail closed in object-dependent permission flows, and publish a version-9 capability envelope with create-specific limits around the existing active descriptor. Re-run the frozen request corpus with a corrected, independently reviewed array-aware comparator in fresh stores.

**Tech Stack:** Rust 1.93.1, serde/serde_json, Axum/Tokio, in-memory and SQLite stores, pinned Python 3.12.12, locked offline Cargo, SHA-256 source maps.

**Design:** `docs/superpowers/specs/2026-10-26-cp001-create-current-head-compat-design.md`, SHA-256 `0c0b78e983c21b38c83794c3eb9380c28e63153f5195dedea0c998eb4c18f142`. The user approved its prior SHA-256 `0805beb2f6187afa080b098170c1aa0a0048b59542081442db876aae2b6b8d13`; the version-9 capability and refusal-shape corrections await user review before execution. Historical attempt-06 manifest SHA-256 `8f20eb5e628ffcc8c35afdf637297359ccbf74679a0878bd4bebea1764b56207` and results review SHA-256 `818609357c70cbd2a52d10f0829a94b72db7d4ccc6bcab8dd97f618e63d4daad` are immutable baseline evidence, not passing credit.

---

## File and claim map

Work in a newly frozen private copy under `.b4-work/full-rust-rewrite-scope/m0-cp001-create-remediation/` until source and results reviews approve an adoption. Do not read, copy, modify or use `superseded-pre-amendment-*` paths. Do not write Git state. Exact Rust production responsibilities:

| Unit | Files | Responsibility |
| --- | --- | --- |
| Metadata model | `crates/b4-contract/src/wire.rs`, `crates/b4-runtime/src/store.rs` | One shared JSON object-or-array type; preserve object defaults and typed wire serialization. |
| Storage and bounded shape | `crates/b4-runtime/src/memory.rs`, `crates/b4-sqlite/src/{lib.rs,confinement.rs}`, `crates/b4-runtime/src/confinement/{shape.rs,backend.rs}` | Persist both shapes, decode only object/array, admit depth 96 on both metadata write and raw SQLite read while other record shapes retain depth 64. |
| Object-dependent consumers | `crates/b4-runtime/src/permissions/{driver_adapter.rs,driver_adapter_account.rs,validation.rs,runtime_mutation.rs}` and any other compiled map consumer found by search | Explicit object branch or unsupported-profile refusal before side effects; never coerce arrays to empty maps. |
| Create transport | `crates/b4-http/src/parser.rs`, `crates/b4-http/src/transport.rs` | 131,072-byte/depth-96 create parsing, last-key-wins, TypeScript-observed request errors and create-only headers; other routes remain strict. |
| Capabilities and authored schemas | Create `crates/b4-contract/src/capabilities_v9.rs`, `contracts/agent-protocol/schemas/capabilities-v9.json`; modify `crates/b4-contract/src/lib.rs`, `crates/b4-http/src/capabilities.rs`, `contracts/agent-protocol/schemas/{create-thread-request,thread}.json` | Version-9 envelope around the exact active validated V2–V8 descriptor; keep all V1–V8 wire types and validators unchanged. |
| Acceptance and comparison | New `crates/b4-http/tests/create_compat_tcp.rs`, focused tests in existing crate test files, and a new private comparison source map | Independent RED/GREEN expectations, fresh TCP and durable comparison, reviewed process ownership. |
| Status records | `spec/runtime/{compatibility.md,requirements.yaml}` and new private receipts | Record actual named-case results and finite-limit `CHANGE`; do not close CP-001, M0, full core or whole product by inference. |

Existing T-001–T-018 qualification files and attempt-01–06 receipts are read-only. Public-source edits occur only after a reviewed private implementation map and controlled adoption. New source cannot inherit earlier qualifications.

## Task 0: freeze the starting state and private execution boundary

**Files:** Create private `m0-cp001-create-remediation/{baseline,workspace,source,review,receipts,runs}`; read approved spec, attempt-06 receipts and current public source only.

- [ ] Rehash the approved spec, results review, current public source and current committed TypeScript HEAD. Record an exact sorted relative-path/SHA-256 baseline map and file count; verify the private workspace copy equals that map before adding any file. Exclude prior private attempts and every `superseded-pre-amendment-*` path.
- [ ] Pin the same committed TypeScript source and built output used by attempt 06, the exact ordered 19 request bodies, significant-header list, normalization rules and process-owner oracle. Copy only reviewed reusable harness source into a new source namespace; create fresh receipt/run/store namespaces. Any source alteration gets a new hash and independent review before execution.
- [ ] Review the new runner/controller source for finite deadlines, full-host PID/PGID retirement, raw observation retention and no mixed historical credit. Preserve the approved attempt-06 observer fix. Do not launch a TypeScript or Rust server until the new source binding is independently approved.

## Task 1: author acceptance and correct the array comparator before implementation

**Files:** Create `crates/b4-http/tests/create_compat_tcp.rs` in the private workspace; modify only a copied new-namespace `compare.py` and its tests in private harness source.

- [ ] Independently author TCP expectations from the approved design and literal 19-case corpus, without reading the proposed Rust implementation. Assert status, semantic JSON, significant headers, no callback, and durable row count/metadata. Add focused reopen/read checks for `metadata:[1,2]`, object legacy rows, top-level array, last-wins duplicates, malformed/null/scalar roots, metadata-null, 65,537 bytes and depth 65. Use owned loopback server/store, bounded reads and verified child cleanup.
- [ ] Give the comparator a case-specific expected metadata shape. Replace its unconditional `isinstance(metadata, dict)` gate with `dict` or `list` only when the frozen case expects that shape. Keep generated-ID/timestamp validation and response/live/SQLite linkage; a malformed array, unmatched row or wrong side shape still fails. Do not edit attempt-06 comparator or receipts.
- [ ] Write negative comparator tests for wrong array contents, array accepted in an object case, response/store mismatch, missing row and malformed row. Run the new source-only tests; expected GREEN. Freeze the new comparator and acceptance file hashes; obtain independent source reviews of wire parsing, process cleanup, test independence and comparator constraints before any live paired run.
- [ ] On the unchanged Rust baseline, run the exact new acceptance test through the reviewed bounded runner, or use the already reviewed attempt-06 baseline result if the acceptance file shares its literal requests and comparison rules. Require at least one observed expected mismatch, and record all statuses as RED evidence; never mark baseline failures as later passes.

## Task 2: model and store JSON object-or-array metadata

**Files:** Modify `crates/b4-contract/src/wire.rs`, `crates/b4-runtime/src/store.rs`, `crates/b4-runtime/src/memory.rs`, `crates/b4-sqlite/src/lib.rs`; test `crates/b4-contract/tests/wire.rs`, `crates/b4-sqlite/tests/context.rs`, and focused runtime store tests.

- [ ] Write tests that object metadata serializes exactly as before, `[1,2]` survives memory and SQLite create/read/reopen, and stored null/scalar metadata is rejected as corrupt rather than coerced. Run `cargo test -p b4-contract --test wire --locked --offline` and `cargo test -p b4-sqlite --test context --locked --offline`; require meaningful RED on new assertions.
- [ ] Define one shared type in `b4-contract`, used by runtime and wire DTOs:

  ```rust
  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  #[serde(untagged)]
  pub enum ThreadMetadata {
      Object(serde_json::Map<String, serde_json::Value>),
      Array(Vec<serde_json::Value>),
  }
  // Default is Object(Map::new()); From<Map<...>> is lossless.
  // as_object() returns None for Array; it never supplies an empty fallback.
  ```

- [ ] Change `ThreadSeed`, `ThreadRecord` and public `Thread` metadata fields to this type, update existing object-valued struct initializers explicitly, and keep SQLite's existing JSON column/schema. Reject null/scalar on decode. Run the same focused tests to GREEN, then `cargo check --workspace --all-targets --locked --offline`; fix every compile error by explicit object handling, not broad coercion or a silent `is_empty` shim.

## Task 3: bounded metadata shape and fail-closed consumers

**Files:** Modify `crates/b4-runtime/src/confinement/shape.rs`, `crates/b4-runtime/src/confinement/backend.rs`, `crates/b4-sqlite/src/confinement.rs` if required by the changed decoder signature, and object-dependent permission/recovery files listed in the file map; add focused runtime and SQLite permission tests.

- [ ] Write a RED depth test proving existing depth-64 accounting refuses depth-65 metadata on both `measure_thread` and raw `backend::decode_metadata`, while non-metadata snapshot/origin/journal decoding retains depth 64. Add a SQLite create/read/reopen depth-65 test through the confined row reader. Add RED tests showing array-bearing threads cannot enter object-dependent permission work or emit effects/grants before an explicit unsupported-profile refusal.
- [ ] Extend the existing bounded `shape` guard with a per-call depth limit and fixed maximum stack of 96; retain `measure(...)`, `check_raw(...)` and generic `decode_record(...)` at depth 64. Add paired thread-metadata measure and raw-check/decode entries using depth 96 and `backend::THREAD_BYTES` (262,144). Change `backend::decode_metadata` to return `ThreadMetadata` through that bounded raw path; `crates/b4-sqlite/src/confinement.rs` must use it for the metadata cell. Keep the existing node limit and bounded writer. The create path calls the metadata measure before `create_thread_with`; a refusal writes no row.
- [ ] Audit all production uses of `.metadata` that assume a map. Use `as_object().ok_or(UnsupportedProfile)?` at permission/recovery entry boundaries before mutation or disclosure; object paths keep their previous canonicalization and budgets. In both raw `PermissionThreadCells` capture paths in `permissions/validation.rs`, preflight metadata bytes with the depth-96 metadata profile, decode as `ThreadMetadata`, and return explicit `UnsupportedProfile` for an array before any effect. Keep origin and all other raw profiles at depth 64. For generic JSON size/canonical comparisons, handle the array value explicitly and charge the same bounded account. Do not model an array as `{}` or treat its numeric indexes as authority.
- [ ] Run focused permission, confinement and storage tests to GREEN, then `cargo check --workspace --all-targets --locked --offline`. Independently review the array security boundary and old object behavior before accepting this task.

## Task 4: create parser and route response parity

**Files:** Modify `crates/b4-http/src/parser.rs`, `crates/b4-http/src/transport.rs`; test `crates/b4-http/tests/parser.rs`, `crates/b4-http/tests/request_diagnostics.rs`, and the frozen create acceptance.

- [ ] Add parser tests for strict non-create duplicate/depth/byte behavior and separate create last-key-wins, depth 65 accepted/depth 97 refused, 131,072 bytes accepted/131,073 refused. Add create response tests for both exact TypeScript error messages and absent `cache-control` on success/request-validation errors. Run targeted tests against unchanged parser/transport; require RED.
- [ ] Refactor the existing recursive visitor to take `max_depth` and duplicate policy, leaving `parse_json` at 65,536/64/reject. Add `parse_create_json` at 131,072/96/last-wins. The create body is read with `to_bytes(..., 131_072)` before parsing; other routes keep `to_bytes(..., MAX_BODY_BYTES)`. Do not parse an unbounded body into `Value` first.
- [ ] On create, map empty/whitespace to `{}`; require root object or array; take the root object's final `metadata` key after last-wins parsing; accept object/array metadata; ignore other root keys. Map malformed/scalar/null roots to `Malformed request body`, and present null/scalar metadata to `metadata must be an object`, both HTTP 400 with no `details`. A top-level array yields empty-object metadata. Use a create-only JSON response helper for successful and request-validation responses, retaining the shared helper for infrastructure/auth/store errors and other operations.
- [ ] Check the metadata shape budget before store admission. Body over-limit returns HTTP 413 `body_too_large`; depth over-limit returns HTTP 400 `invalid_json`; both retain the existing native request-error JSON shape and omit `cache-control` under the create-only header policy. Metadata budget failure returns HTTP 413 `{"error":{"kind":"request_error","message":"metadata_limit","details":{"code":"metadata_limit"}}}` without `cache-control` and writes no row. Run parser, diagnostics, and private TCP tests to GREEN; recheck unchanged run/resume parser tests and header behavior. No success claim from a test that skipped its named case.

## Task 5: capability version 9 and authored schemas

**Files:** Create `crates/b4-contract/src/capabilities_v9.rs`, `contracts/agent-protocol/schemas/capabilities-v9.json`, `crates/b4-contract/tests/capabilities_v9.rs`; modify `crates/b4-contract/src/lib.rs`, `crates/b4-http/src/capabilities.rs`, `contracts/agent-protocol/schemas/create-thread-request.json`, `contracts/agent-protocol/schemas/thread.json`; test existing `crates/b4-contract/tests/capabilities_v2.rs` through `capabilities_v8.rs`, `crates/b4-http/tests/capabilities_api.rs`, `crates/b4-http/tests/capabilities_tcp.rs`, and schema checks.

- [ ] Add RED contract tests: every historical V1–V8 descriptor still decodes and validates unchanged; V9 requires exactly `descriptor_version=9`, `base_descriptor` with a validated V2–V8 payload, and `create_limits` with `body_bytes=131072` and `json_depth=96`. Missing, extra, wrong-version or contradictory fields fail. A V1–V8-only consumer rejects top-level V9 rather than reading the nested base as current limits.
- [ ] Define `CapabilityDescriptorV9` as a strict envelope and `BaseDescriptor` as a bounded tagged V2–V8 union selected by the nested `descriptor_version`. Delegate base validation to the existing version-specific validator; reject V1 or unknown nested versions. Refactor the host's current V2–V8 construction to return the same validated typed descriptor instead of serializing at each branch, then wrap it in V9. The nested base contains the unchanged common `limits.body_bytes=65536` and `limits.json_depth=64` and all installed feature flags. Keep the outer response cap and no-store policy. Do not modify historical V1–V8 wire schemas or validators.
- [ ] Add `capabilities-v9.json` with an envelope and a `oneOf` for the existing V2–V8 schemas; require the exact create limits. Keep old schema files byte-identical. Add `crates/b4-http/tests/capabilities_v9_output.rs`: for each supported active V2–V8 configuration, call the real protected GET, retain its raw JSON body in a fresh private capture directory with source/config hashes, and assert nested-base equality to that configuration's pre-wrapper descriptor. Add `scripts/validate_capabilities_v9_output.py` using pinned `jsonschema` Draft 2020-12 and the contract registry to validate **each actual captured host body** against `capabilities-v9.json`, require seven distinct nested base versions and exact create limits, and reject missing/extra/duplicate captures. Run the Rust capture test under the reviewed bounded process controller, then the Python validator on that exact capture directory; a static authored example cannot substitute for actual output.
- [ ] Update the two authored JSON Schemas for root array/unknown root acceptance and metadata object-or-array, and for public thread metadata object-or-array. Preserve their `x-compatibility-status` and authored-input provenance. Run contract, HTTP capability and schema checks to GREEN.

## Task 6: integrated reviewed comparison and regression

**Files:** New private source map, manifest, case receipts, results review and ledger under `m0-cp001-create-remediation/`; no historical evidence mutation.

- [ ] Format only changed Rust files with pinned rustfmt, then run `cargo fmt --all -- --check`. Freeze the complete implementation workspace map, accepted test sources, TypeScript built-output map, comparator and owner controllers. Obtain independent source and quality reviews before live execution; repair and re-freeze on any finding.
- [ ] Run focused crate tests and `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` on the reviewed source with the private target. Run the relevant existing HTTP, contract, runtime, SQLite, permission, confinement and capability regressions. Record exact commands, exit status, logs, source hashes and process ownership. Any existing test whose expectation intentionally changed must be updated with an explicit old-vs-new assertion; do not delete it to get GREEN.
- [ ] Run fresh no-server config/preflight and a fresh two-sided smoke in disposable stores. Obtain independent smoke-results review. Freeze a new manifest and execute the ordered 19-case paired sequence once, with fresh stores per case and the reviewed full-host retirement oracle. Stop on missing source, unsafe cleanup, timeout, incomparable capture or case-chain break. Preserve raw responses, SQLite snapshots, normalized projections and every process receipt.
- [ ] Independently review all 19 comparisons and the complete receipt chain. Correct the array comparator only in this new source map, never by rewriting result JSON. If any case fails, record the actual failure; if all pass, grant `COMPAT` only to those named create cases and compared fields. A collector writes a new private ledger only after independent results approval.
- [ ] If a source or parity failure needs a repair, retain that attempt unchanged. Make the smallest reviewed source amendment, freeze a new manifest and fresh attempt namespace, and rerun the entire ordered corpus in new stores. A one-time sequence rule applies per frozen manifest; no partial retry, receipt rewrite or cross-attempt pass aggregation is permitted.

## Task 7: limited documentation and adoption

**Files:** Modify `spec/runtime/compatibility.md`, `spec/runtime/requirements.yaml` only to reflect actual reviewed results; adopt only reviewed Rust/schema/test changes and those status edits to public source.

- [ ] Record the result per case, explicit 131,072/96 finite-limit `CHANGE`, and the continued partial status of CP-001. Keep CP-002–010, M0, T-019–T-056, full core and whole-product parity open. Do not transfer attempt-06 failures into passes or rename earlier T-001–T-018 evidence.
- [ ] Obtain independent documentation/source-map review. Back up public paths before a controlled copy; verify copied hashes, protected historical paths and the full public map. Roll back on any failed equality gate. No Git write is required.
- [ ] Run final public `cargo fmt --all -- --check`, focused tests, strict lint and contract/schema checks on the exact adopted source, with bounded/reaped process receipts. Independently review final public integrity and claim wording. Only then publish a scoped CP-001 create-subset result; continue the separate CP-001 remainder and broader full-core/whole-runtime backlog.

## Exact local verification commands

Run from the private Rust workspace during Tasks 2–6 and from adopted public source during Task 7. Use `/Users/blove/.rustup/toolchains/1.93.1-aarch64-apple-darwin/bin/cargo` and a private `CARGO_TARGET_DIR`; pass `--locked --offline` on Cargo compilation commands. The named commands below must exit zero on the reviewed final source; RED commands in earlier tasks must fail for their stated reason. Do not infer success from an old receipt.

```text
cargo test -p b4-contract --test wire --locked --offline
cargo test -p b4-contract --test capabilities --locked --offline
cargo test -p b4-contract --test capabilities_v9 --locked --offline
cargo test -p b4-http --test capabilities_v9_output --locked --offline -- --test-threads=1
cargo test -p b4-runtime --lib --locked --offline
cargo test -p b4-sqlite --test context --locked --offline
cargo test -p b4-http --test parser --locked --offline
cargo test -p b4-http --test request_diagnostics --locked --offline
cargo test -p b4-http --test capabilities_api --locked --offline
cargo test -p b4-http --test capabilities_tcp --locked --offline
cargo test -p b4-http --test create_compat_tcp --locked --offline -- --test-threads=1
cargo check --workspace --all-targets --locked --offline
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
python3 -B scripts/check_contracts.py
python3 -B scripts/validate_capabilities_v9_output.py <fresh-private-capture-directory>
```

The plan's case comparison is an additional controlled TypeScript/Rust TCP run, not a substitute for these Rust checks. Each command's fresh exit status, test count and source map is evidence only for that command's scope. The accepted source and results reviews determine whether a named `COMPAT` result can be recorded.
