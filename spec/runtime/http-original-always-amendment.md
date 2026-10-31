# Scoped HTTP original Always implementation amendment

Date: 20 October 2026. Scope: explicit `HttpHost::new_with_permissions`, not a general promotion of the proposed persistent-Always subsystem.

The opt-in host accepts an actual authenticated human Always decision through the normal bounded producer, persists its acceptance and permission decision atomically, and continues the original execution through the verified original driver. Its private continuation discriminator selects the driver; sealed stored human evidence, current authority/catalog, tool constraints and resource fences establish permission. The original path produces no synthetic PermissionUse.

The supported profile is one immutable native route, one batch, one Pure required-approval task and one active/concurrent task, with finite existing request/input/context/evidence/outcome bounds. Later matching runs use genuine Ready authorization and the existing permission-use driver, including after owner reopening. Only a verified permission miss may take the ordinary approval fallback.

The store separately declares `original_always_atomic_durable`, default false with Arc forwarding and explicit SQLite opt-in. The HTTP constructor requires this and the existing durable approval/runtime/Ready writer declarations. General `durable_permissions` remains false.

The new host publishes version4 discovery with narrow `durable_permission_creation=true` and `durable_permission_reuse=true`. Broad `always_permissions`, trusted recovery, cancellation and deletion remain false. Ordinary and reuse-only builders preserve their earlier behavior and descriptor versions. Existing clients are not automatically qualified for version4.

An actual callback outcome that cannot be committed faults the HTTP host and leaves the last committed Dispatch state. HTTP neither retries settlement automatically nor replays a callback. Embedded explicit retained-result retry remains a separate API.

This amendment records a bounded installed subset of [the persistent-Always proposal](persistent-always-design.md); its broader wrapper types, lifecycle support, startup reconstruction and release qualification remain proposed. See the [usage notes](../../docs/runbooks/http-always-approvals.md) and [scoped qualification](../../conformance/permissions/http-always/README.md) for executed evidence. Schema checks establish metadata evidence only.
