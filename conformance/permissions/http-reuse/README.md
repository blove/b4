# HTTP reuse of saved permissions

`HttpHost::new_with_permission_uses` connects an authenticated HTTP run to the existing durable permission-use Driver. Supply `PermissionUseHostConfig::new(catalog, approval)` and a durable `PermissionStore`; the host derives its native application from that exact immutable catalog. Genuine permissions must already have been provisioned through the embedded approval path. HTTP Always creation remains unsupported.

The opt-in profile accepts one route, one batch and one Pure task requiring approval, with one active run and one concurrent task. Input defaults to 4,096 serialized bytes and additionally permits at most 256 JSON values and value depth 16. The ordinary request-body parser retains its 65,536-byte/depth-64 bounds. Output must use the Driver's fixed 65,536-byte ceiling. These admission bounds do not promise that every combination fits the later independent permission-record and cumulative-account limits.

A new run commits Start once and attempts typed authorization against its actual current execution and resource fences. A matching permission commits a real Use and Ready2 before the receipt-backed Driver invokes the native callback. A verified miss passes actual Start1 to the existing Once/Deny approval flow. Authorization errors refuse without a fallback or callback. Resuming an ordinary approval keeps its existing behavior.

The callback-entry hook uses the host's local RunControl admission after permission renewal. The worker retains its actual store, capacity and claim until callbacks drain. A Driver failure retains its real Work through inspection, faults the host and never replays the callback or fabricates completion. This first HTTP bridge performs no automatic storage retry.

Only this opt-in host publishes capability descriptor V3 with `durable_permission_reuse: true`. Broad `always_permissions` remains false. Cancellation, recovery, deletion and operator confinement mutations are refused; their capability flags are false. Existing host constructors and V1/V2 descriptor formats and validators retain their behavior.

The macOS arm64/Rust 1.93.1 qualification includes genuine end-to-end SQLite reuse through actual TCP, full typed and nine-table persistence checks, immutable original records, callback once, owned shutdown and owner reopening/removal. Seven router controls cover constructor/capability/authentication/policy/input bounds, genuine Once/Deny fallback and unsupported cancel/delete. Three lifecycle cases cover prior-owner recovery/operator refusal, detached TCP viewers with retained capacity, and an exact Complete-journal abort that preserves Dispatch3 and faults the host without callback replay.

Formatting, workspace compilation, strict all-target lint and the full final-source regression passed: 1,545 tests passed and 11 marked ignored were not executed. The twelve V3 contract tests and all eleven new HTTP cases executed. Schema/reference validation passed separately and does not establish runtime behavior. No Linux, HTTP Always, broad core release or maximum-size qualification is claimed here.

The opt-in construction is:

```rust
let config = PermissionUseHostConfig::new(catalog, approval);
let host = HttpHost::new_with_permission_uses(store, auth, policy, config)?;
```

The catalog must contain the same Definition and native effect used when provisioning the saved permission. Use the existing authenticated run, pending-interrupt and resume endpoints. A permission miss returns the existing parked response; resolve its actual token with Once or Deny. This constructor supplies no startup-preparation wrapper and does not automatically recover unfinished prior-owner work.

See the [design](../../../docs/superpowers/specs/2026-10-20-http-permission-reuse-design.md), [plan](../../../docs/superpowers/plans/2026-10-20-http-permission-reuse.md), and [qualification record](qualification.json).
