# 0013: Bounded snapshot and live run attachment

Accepted for the native scripted HTTP increment after independent design review.

GET attachment reads a captured turn; it never starts, resumes or adopts execution.
RunControl atomically registers a viewer and copies its bounded native digest. The
async request owns the subscription while permission workers receive immutable
identity/cut data. Renewal reloads token-free identity after policy returns, binding
the original generation, execution, turn, owner and exact registered definition.

Viewer count is independent of execution capacity and the primary POST viewer.
All viewer queues have byte/frame bounds and an independent overflow signal. Slow
viewers detach while authoritative work continues. The transient digest has a whole
array UTF-8 byte/frame bound; truncated/recovered history is explicitly unavailable.
No cursor or durable replay semantics are introduced.

Logical publication ends at the selected terminal, while the worker keeps exclusion
through actual quiescence. Existing non-detached viewers retain the exact terminal
even after a late fault. Known inactive publication returns durable state, retry2000
and null done, preserving the frozen reference's selected inactive behavior. Faulted
or rejected publication without a proven terminal cannot manufacture that success.

A dedicated read-only AttachmentPending service may disclose a prior-owner stable
park after fresh authority and complete captured-view fencing. It validates every
grant/acceptance and all historical required-task decisions before projection or
policy-specific refusal. It never writes owner, token, expiry, grant consumption or
audit. Ordinary pending/resume remain current-owner-only. SQLite stays version3.

Native values, anchor IDs and raw event/data digest differ from the pinned
TypeScript/LangGraph projections. Selected status, inactive fields and terminal
comparisons are qualified separately; CP-007 and the core release remain incomplete.
The retained cancellation receipt and all reference captures/migrations remain
unchanged. See bounded-run-attachment-design.md for limits and qualification scope.
