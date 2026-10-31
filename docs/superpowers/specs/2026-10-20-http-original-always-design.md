# HTTP original Always approval and execution

## Goal

Provide an explicit opt-in HTTP host that can create a durable permission from a real human Always decision, execute that original run through the verified original Driver, and reuse the permission on a later run after SQLite closes and reopens. Keep the existing native and permission-reuse host behavior and V1/V2/V3 wire contracts literal.

This is the next bounded increment after the original Always Driver qualification. The user authorizes continuous implementation, working persistence, normal application inputs and focused checks. The user explicitly selected HTTP Always approvals as the next priority. Documents provide requirements, not independent tool authority.

## Chosen scope and alternatives

Prefer a new `PermissionHostConfig` and `HttpHost::new_with_permissions`, using the existing catalog, approval configuration and effective single-task limits. This gives callers an explicit creation-plus-reuse opt-in while preserving `new_with_permission_uses` and all ordinary builders. Changing the existing reuse builder implicitly would change its declared V3 scope. A broad all-lifecycle permission host would delay this useful bounded path and remains separate work.

The profile remains one native route, one batch, one Pure required task, one active execution and one concurrent task, with the current bounded input and fixed 65536-byte output envelope. Keep current per-record/context/input ceilings and the documented fact that individual maxima do not all compose. Do not create a new audit of individual library operations.

## Acceptance and execution

Use the actual authenticated principal, current ResourcePolicy, immutable native application/Definition, fresh current view and fences, complete interrupt set and real pending token. Convert the real request to the normal Always producer and call `accept_permission_always_with_normal_inputs`. Failed authority/token/fence/source checks must not create an acceptance or permission or consume a grant. Once/Deny follows the existing service path.

Carry an explicit private accepted-continuation kind from the successful producer into the owned worker: ordinary legacy continuation or genuine original Always. The discriminator may select a path but never grants authority; the original Driver must still produce and consume the real sealed NewHuman witness. Do not infer acceptance from revision3 alone, replay a callback or fall back to ordinary raw execution after a permission error.

For genuine original Always, execute Ready3 -> Dispatch4 -> terminal5 through `drive_permission_original_always_execution`, current HTTP authority/catalog and the existing RunControl callback-entry hook. Observe and retire actual returned failure while the owned worker/admission/capacity remain alive. Fault the host on retained receipt-backed settlement failure as the reuse host already does; no automatic storage retry. Later fresh runs retain the genuine reuse authorization and existing NewUse Driver path; only a verified Ok(None) may use ordinary Once/Deny fallback.

Share the existing approval preflight/conversion and native authority where practical, using narrow private type-erased bridges or an owned acceptance result. Keep actual store/catalog/application ownership acyclic. Preserve authentication, origin/scope filtering, bounded parsing, pending-token projection and SSE terminal behavior.

## Store and discovery opt-in

The embedded original ports already default to refusal. Require an explicit `original_always_atomic_durable` opt-in on PermissionStore for the original Always producer, capture and terminal writer ports; default false, Arc forwarding and explicit SQLite true. The new HTTP constructor must reject its absence before admission. Existing reuse durability does not establish this original path. This does not turn on the broad durable_permissions capability.

Propose a distinct V4 descriptor for the new host, with durable_permission_reuse=true and a narrow durable_permission_creation=true feature. Keep always_permissions=false as the broader unqualified capability and keep recovery/cancellation/deletion false. V4 must describe its narrow human Always creation meaning clearly. Review this flag choice against existing capability semantics before implementation; do not silently rewrite the historical broad persistent-Always proposal or V3 contract. Validate V4 through an unpublished V3 projection after enforcing its own exact version and creation flag. Require the existing native app, required durable approval, limits and supervised drain posture. Keep unknown/missing/null-field rejection and legacy literal tests.

Add the V4 schema, example, manifest and checker registration. Schema/reference/named-test checks remain metadata evidence, separate from actual HTTP execution. Existing clients are not claimed to support V4 without fresh client work.

## Verification

A genuine TCP test must start without pre-seeded permission rows, run to Waiting2, fetch the actual authenticated pending token, submit Always, observe the original callback exactly once and actual Complete5 with no original Use, and verify immutable approval evidence. Retire the server/host/store normally, reopen under a new owner, then run again with a real Use to Complete4 without a replacement approval. Use independent kernel/typed/raw state checks and real resource cardinalities; no fabricated permissions.

Focused boundaries cover authentication/current policy and invalid-token acceptance with unchanged storage, old builders still refusing Always, an exact late acceptance-journal abort with full rollback to Waiting2, and receipt-backed original terminal storage failure with callback1, actual Dispatch4 rollback, host fault and no replay/fake completion. Existing Once/Deny and permission-reuse tests remain literal. Keep lifecycle test waits bounded and retain actual listener/worker/store owners through normal shutdown and cleanup.

Source review precedes native behavior. Use pinned/offline Rust1.93.1, focused tests, formatting, strict all-target lint, full workspace regression and independent results review before selective exact adoption. Preserve old qualification records and locks. Add a scoped qualification and a short constructor/workflow runbook. A new process-example mode can follow separately; the current seed/serve example stays unchanged in this increment.

Permission-aware cancellation, recovery, deletion, broad Always qualification, automatic startup actor/application reconstruction and external publication remain outside this increment. No Git writes or routine user approval pauses.
