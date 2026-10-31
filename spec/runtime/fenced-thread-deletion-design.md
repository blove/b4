# Fenced thread deletion

Status: implemented and independently verified native increment, 2026-10-01.
Fresh workspace337 Rust tests pass; selected native lifecycle comparisons cover
seven statuses, without a whole-protocol parity claim.

Implement authenticated DELETE /threads/{thread_id} for the existing bounded
scripted host and a trusted Store deletion primitive. The independently executed
lifecycle reference shows 204 for missing/idle/completed/parked deletion and 409
run_in_flight while a held effect still runs, including after cancellation has
already produced a terminal response. The native implementation must preserve its
stronger immutable origin and conservative uncertainty rules.

Add ThreadDeletionTarget captured from a coherent token-free ThreadView. It binds
thread ID and generation, resource revision, optional active execution ID and
optional execution revision/owner fence. Store::delete_thread_fenced defaults to
a typed UnsupportedOperation for existing custom Store implementors; MemoryStore
and SqliteStore implement it. Arc/reference delegation preserves the capability.
This trusted primitive does not receive public DTO identity or perform policy;
the HTTP host supplies already authorized targets.

Independent integration review reproduced an admission ABA after deletion: a
delayed run authorized against an idle thread at resource revision0 could enter
a replacement incarnation at revision0. Fenced admission therefore also takes
the exact ThreadGeneration and compares generation plus resource revision inside
the same memory mutex/SQLite immediate transaction. Fenced missing resources are
RevisionConflict; unfenced trusted admission retains ThreadNotFound. The private
Store/Runtime v0 admit_fenced signature changes accordingly; all callers/wrappers
forward generation. Every new HTTP worker renews current policy and exact origin
before stale-view diagnostics or implicit creation, then admits with its captured
or newly created generation. Even replacement during that renewed policy cannot
bypass the transaction fence.

Under the memory mutex or SQLite immediate transaction, reload the complete
current view, validate snapshot/coherence, compare every target field and reject
changed incarnations/resources/executions. Idle threads, terminal Completed,
Failed and Cancelled snapshots, and stable WaitingForApproval snapshots with no
InFlight/Unknown work may be deleted. Accepted, Running, CancelRequested and
RecoveryBlocked are busy. A stable parked or settled prior-owner snapshot may be
deleted without recovery because it has no unfinished effect; an uncertain
prior-owner snapshot stays busy. A changed active execution must never be
retargeted. A corrupt or partially missing relationship fails closed. Validate
every historical execution owned by the thread, not only its active pointer:
historical executions must be well formed and terminal; only the exact active
snapshot may be a stable park. An idle or terminal pointer must not hide an
unfinished historical effect. Under the same transaction, validate every related
journal/grant/acceptance relationship selected by either its thread or execution
index. Mismatched indexed/payload identity or cross-thread ownership is corruption
and rejects before mutation; cleanup cannot erase another thread's authority.
An inactive unfinished historical snapshot is corrupt lifecycle ownership;
active unfinished work is an ordinary busy refusal.

Successful deletion removes every execution belonging to the thread and their
journal, grant and acceptance records, then the thread, atomically. SQLite clears
the thread's active pointer before deleting children to respect its FK cycle.
Do not change schema version or dependencies. Any SQL failure rolls everything
back. Recreating the same ID produces a new OS-random generation, so retained
targets cannot delete that incarnation even when its revision resets to zero.

Expose Operation::DeleteThread. Authenticate and evaluate the coherent ordinary
view before revealing busy/conflict details. Existing threads require exact
server-owned application/tenant/principal origin even under a permissive policy.
The host takes the same per-thread owned claim used by new/resume/recover workers
before the deletion worker begins; this is a temporary exclusion claim and does
not take global execution capacity. Renew policy and complete exact captured view
in the owned blocking task before commit. Hold the claim and store until deletion
quiesces even if the request future is dropped. Never hold a claim mutex while
executing policy, SQLite or application code. A worker's existing claim returns
409 run_in_flight even if its durable snapshot has just become terminal or parked.

For an absent thread, authenticate and authorize its missing-resource context,
then return 204 without invoking deletion or creating state. The missing context
has None origin/stored_route/requested_route; never invent the caller's origin.
This follows the
selected reference behavior. An actor denied by policy receives the ordinary
native 404 resource_unavailable, including missing-resource probes. Existing
foreign-origin threads receive 404 before protected diagnostics. Successful 204
responses have an empty body and Cache-Control: no-store. Stale resource/identity
fences, including disappearance after capture, return 409 resource_changed; durable
busy work returns 409 run_in_flight;
unsupported custom stores return 501 unsupported_operation. Corruption, panic or
failed transaction marks host readiness failed through fixed host_unready.

Verify memory/SQLite idle, historical executions, stable grants and accepted
audit removal, exact fence mismatch, ABA, corrupt relations, active/uncertain
refusal, SQL deletion fault rollback and reopen. Verify the real HTTP route,
policy renewal races, held-worker claim including disconnected viewers, abandoned
deletion future retaining store/claim, empty no-store 204 and healthy expected
conflicts. Add selected status comparisons against frozen lifecycle recordings;
these do not establish full deletion parity or release qualification. Cancellation
and GET attachment remain separate increments.
