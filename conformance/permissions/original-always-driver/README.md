# Execute the original Always-approved run

The embedded `drive_permission_original_always_execution` entry continues a genuine original Always approval from Ready revision3 through Dispatch4 to kernel-derived terminal revision5. `PermissionOriginalAlwaysDriverBindings` uses the same store, current authority, producer/current catalogs, approval context, hooks and capacity as the existing permission-reuse Driver.

The original approval produces no PermissionUse record. The Driver consumes its actual sealed human approval evidence, renews current authority/catalog/tool constraints and owner/resource fences, then enters the local callback hook. The hook runs outside the state mutex and can refuse local entry while real uninvoked Work remains retained. Dropping a failure does not settle or invoke it.

SQLite atomically writes the terminal snapshot, thread revision and journal while comparing actual Dispatch4 and unchanged original acceptance/decision/consumed-grant evidence. The actual callback result is retained through a storage failure. `retry_settlement` retries storage with the same Work and cumulative account; it never invokes the callback. Successful settlement makes another retry refuse.

The original entry supports the scoped single-task Pure/required profile with UUID application/resource IDs, descriptive task/effect IDs and bounded structured JSON. Its genuine human flow admits two turns and a bounded retained prompt. Existing strict and NewUse entry semantics remain unchanged. A completed creator has six journal versions; reuse validates its actual kernel-derived history throughout authorization, dispatch, renewal and settlement after reopening.

Five focused cases and the full workspace regression passed locally on macOS arm64 with Rust1.93.1: 1,550 passed, zero failed and 11 ignored. Formatting, workspace compilation and strict all-target lint also passed against the same source. The [qualification record](qualification.json) binds the exact source, commands and results. Public metadata checks do not execute runtime behavior.

HTTP Always acceptance, permission-aware recovery/cancellation/deletion and the broad durable-permission capability remain separate work. This entry does not change HTTP capabilities. Historical qualification records retain their original scope.
