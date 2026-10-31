# Authenticated HTTP required approvals through the durable service

Status: accepted for the bounded native increment, 2026-10-01.

Expose required Once/Deny approval through `HttpHost::new_with_approvals` without
changing the ordinary `HttpHost::new<S: Store>` embedding contract. An internal
bridge uses the same store, immutable registered definitions and scoped authority
for parking, pending disclosure, complete-set acceptance and every callback.
Durable storage is the default; MemoryStore requires an explicit volatile fixture
choice. Always, off/optional modes and persistent permissions remain unsupported.

Configuration supplies exact server-authored prompt coverage, policy revision
and bounded positive TTL. Validate canonical prompt/input bounds and aggregate
pending response size before admission. Origin-only run/pending/resume checks
apply even with a permissive policy. Renew policy against fresh token-free
resource context before admission, protected diagnostics, sensitive records and
dispatch; compare captured resource/execution fences and exact definitions.

Use the RFC `POST /threads/{id}/resume` path as strict authenticated SSE. Reserve
capacity and a per-thread active-worker claim before consumption. The durable
service consumes the whole decision set atomically with its continuation and
actor/decision record. Accepted work skips Start and preserves completed siblings.
Viewer cancellation cannot dispose the owned worker. Stable park settles wait
with null; stream publishes authorized native
`{interruptId,resumeKey,value,grant}` envelopes and one `done {output:null}`.
Pending disclosure is no-store and never exposes a partial set. Deny/cancelled
skips callbacks; final denied output is native `{denied:true}`.

Expected grant, state, conflict and policy rejections preserve healthy readiness;
store/worker faults do not publish a successful terminal. Revocation after a
dispatch claim prevents callback invocation while retaining conservative InFlight
state and the accepted decision. No grant refund is implied.

HTTP requires explicit trusted known-ID recovery of prior-owner parked work.
Restored accepted Ready continuations can dispatch through the embedded service
without another approval, but this HTTP increment supplies no launch path for
them. Automatic discovery/recovery and the complete crash matrix remain open.

Retain historical recordings and receipts. The new genuine provider-free pinned
TypeScript fixture captures 39 grant requests on two fresh boots; 37 limited
projections match in order and all 39 match only when two independent pending
arrays are treated as sets. This establishes limited reference repeatability,
not native grant parity. Native envelope/status/token/route choices and observed
source differences are explicit in [compatibility](../compatibility.md).
CP-001–005/009 remain partial, other CP questions unresolved, M0 open and all
56 broad release scenarios pending. See [HTTP approval design](../http-approval-design.md).
