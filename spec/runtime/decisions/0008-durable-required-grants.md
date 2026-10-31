# Durable required grants before HTTP continuation

Status: accepted for the bounded native increment, 2026-10-01.

The trusted kernel already models approval decisions, but a kernel Resume alone
establishes no grant authority. Add an embedded service requiring a host authority
port and server-derived actor/policy context. Authenticate outside this embedded
boundary, authorize a token-free coherent resource view, then inspect grants and
atomically fence all persisted changes.

Use OS-random opaque grants with hashed verifiers and recoverable sensitive tokens
outside snapshots, journals and ordinary projections. Issue a grant with its Park
snapshot; consume the entire decision set with its Resume snapshot and a durable
actor/decision receipt. Immutable thread generation and canonical proposal binding
prevent the token from authorizing another recorded call. Task parking revision
is immutable; the current acceptance fence advances during sibling progress and
recovery. Dispatch renews policy and checks the consumed Once binding.

The resumed Ready snapshot and accepted turn are the durable continuation marker
for an explicitly invoked known-ID host driver. An independent queue and automatic
scanner are unnecessary for this bounded service. Once and Deny are supported;
Always remains rejected until exact-tool persistent policy scope exists. Durable
storage is required by default; volatile backend operation is an explicit fixture
choice. Retained plaintext makes the whole database and backups sensitive.

This advances native M4 work, not HTTP resume or TypeScript grant compatibility.
The existing workflow recordings do not cover grants. Preserve their bytes and
all earlier receipts; CP-005/009 and full release scenarios stay open. See
[durable approval design](../durable-approval-design.md).
