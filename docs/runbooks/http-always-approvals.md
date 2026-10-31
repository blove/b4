# HTTP Always approvals

Use `HttpHost::new_with_permissions` when an application needs to create a saved permission from a human Always approval and reuse it on later runs. The application supplies the immutable native application/catalog, authenticator, resource policy and durable approval prompts.

```rust
use b4_http::{HttpHost, PermissionHostConfig};

let config = PermissionHostConfig::new(catalog, approval);
let host = HttpHost::new_with_permissions(store, auth, policy, config)?;
let router = host.router();
```

The store must explicitly support durable runtime, durable approvals, atomic Ready authorization and atomic original Always persistence. SQLite provides these ports. A custom `PermissionStore` defaults to refusing the original capability; declaring reuse support alone is insufficient.

This profile accepts one immutable native route containing one batch and one Pure task that requires approval. It allows one active run and one concurrent task. Default input is at most 4096 bytes with additional depth/node checks; output is fixed at 65536 bytes. Existing evidence/context bounds still apply, and individual maxima do not all fit together.

1. Submit an authenticated run to `/threads/{thread}/runs/wait`, using the registered route and input. With no matching permission, it parks for approval.
2. Read `/threads/{thread}/pending_interrupts` with the same authenticated actor. Preserve the returned `interruptId` and sensitive `grant` token.
3. Submit `/threads/{thread}/resume` with the actual returned values:

   ```json
   {"route":"/reports#agent","resume":[{"status":"resolved","interruptId":"<returned interruptId>","payload":"always","grant":"<returned grant>"}]}
   ```

4. The original run executes through the permission driver. Its acceptance and permission decision persist together; it produces no permission-use record for that original execution. A later matching run creates its own use record and executes without another approval, including after normal server/store shutdown and reopening.

Current actor, origin, policy revision, application/catalog, complete Definition, tool constraints and owner/resource fences are checked again at the relevant boundaries. A permission error cannot silently become ordinary execution. A callback result that cannot be stored faults the HTTP host; HTTP does not automatically retry settlement or replay the callback.

Discovery returns descriptor version4 with `durable_permission_creation` and `durable_permission_reuse` true. Broad `always_permissions`, recovery, cancellation and deletion remain false for this profile. Existing ordinary builders and `new_with_permission_uses` retain their behavior and earlier descriptor versions. The `serve_permission_reuse` example now has an explicit [`serve-approvals` mode](permission-reuse-example.md) for this flow. Its original `seed` and reuse-only `serve` modes retain their behavior.

See the scoped qualification for executed checks. Metadata/schema validation alone is not runtime evidence.
