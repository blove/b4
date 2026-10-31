# Permission Driver callback-entry gate

## Purpose

Prepare the existing receipt-backed embedded Driver for HTTP worker control. HTTP currently uses a local RunControl callback admission check, while the permission-use adapter directly invokes a renewed callback. A host must be able to refuse local entry after renewal without fabricating a callback receipt, replaying a callback or losing the actual retained permit. This is a prerequisite to HTTP integration, not an HTTP support claim.

## Design

Add a default-allow local entry hook to `PermissionUseDriverHooks`, preserving existing implementers and behavior. It is a trusted host scheduling/cancellation hook, not an authority or resource-policy substitute. Existing authority/catalog/constraint/source renewal remains required and unchanged. Invoke the hook exactly once per actual callback-entry attempt, after a real renewed permit is armed and immediately before consuming callback Work. Hold no adapter state mutex while calling host code.

A false result retains the real uninvoked callback Work in a distinct private terminal adapter phase. Return the existing failure owner; callback status and settled outcome remain absent because native callback entry did not occur. No new resume or replay API is added; storage-only retry still requires actual settled Work. A true result follows the existing consuming callback/receipt/outcome path. Preserve cumulative accounts and prospectively pay any additional bounded adapter control work, without resets or cap increases. Host-hook panics must follow existing Driver fault ownership behavior; never discard a taken Work on unwinding.

Prefer this small adapter hook over wrapping/re-registering the application's native callbacks or weakening permission renewal. The gate only controls entry; it does not interrupt a callback already running or add an authorization fence after renewal.

## Verification

Use genuine original Always approval/cancellation/reopen and normal permission reuse, with descriptive identifiers and modest JSON. Add two focused cases: an explicit allowing hook called once produces callback1 and exact Complete4; a refusing hook called once produces callback0/outcome0/raw0 and exact independently projected persisted Dispatch3, preserving all original and permission records. The refusing failure must retain its actual Work through inspection and release it before owned close/reopen/removal. Confirm storage-only retry is unavailable for refusal without re-entering the hook. Preserve all existing tests.

Work in an ordinary isolated copy with pinned locked/offline Rust; test first, independent source and quality/results reviews, then bounded regression and selective adoption. No Git writes, schema/dependency change, HTTP capability change, generic cancellation implementation or exhaustive operation audit. User standing authorization permits continuous work.
