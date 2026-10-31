# Trusted prior-owner continuation through the approval transaction

Status: accepted for the bounded native increment, 2026-10-01.

Add ApprovalService Recover and an explicit trusted embedding target rather than
exposing recovery through read or public HTTP endpoints. Carry the complete
thread/generation/execution identity, old-owner execution fence, resource revision,
exact available Definition, actor/policy context, new turn and trusted clock.
Authorize a token-free coherent thread before protected diagnostics or sensitive
rows; recompute the exact kernel recovery plan inside the existing approval
transaction. Grants and acceptance records are preserved without another decision.

A bare Runtime::recover wrapper does not atomically fence the policy-observed
resource revision. A general RecoveryStore abstraction can follow for other host
profiles; this bounded increment reuses the existing same-store transaction and
requires no new schema or dependency.

HttpHost::recover_execution renews Operation::Recover authority and reserves
capacity/thread claims before mutation. Its supervised worker retains ownership
through detached response/future and effect quiescence. Accepted commits Start;
Running accepted intent skips Start; parked work retains its original prompts;
Cancelled/Failed project their committed terminal; adopted blocked/exhausted work
returns static healthy refusal. Attempts and declared budgets are retained.

Current-owner work, stale/settled targets and different exact definitions cannot
be adopted. The scripted registry remains Pure-only. Embedded SQLite tests qualify
conservative Unsafe behavior separately. Actual prior-owner success,
rollback and reopen evidence uses SQLite; Memory has no restored owner API.

No automatic scanner, persisted general queue, unsafe reconciliation, full crash
matrix, provider or additional public Agent Protocol operation is claimed. See
[design](../trusted-restart-continuation-design.md) and the separate new receipt.
