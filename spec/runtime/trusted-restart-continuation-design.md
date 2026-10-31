# Trusted known-ID restart continuation

Status: implemented native increment, 2026-10-01. Continuous implementation is authorized.
This is a local embedding API for the existing approval-enabled finite scripted
host; it adds no public HTTP operation or automatic discovery/scanner.

## Choice

Extend ApprovalService and its same-store ApprovalMutation transaction with an
explicit Recover request. Reuse the existing execution/resource fences and add
exact thread ID, immutable generation and execution ID binding. Reuse the owned
HTTP worker for eligible recovered work, preserving capacity, same-thread claims,
policy renewal, effect quiescence and detached viewer behavior.

A wrapper around Runtime::recover is smaller but does not atomically compare the
policy-observed resource revision. A general RecoveryStore trait is useful later
for ordinary hosts, but adds an independent backend/API surface now. The existing
approval transaction gives both native stores one exact atomic recovery boundary
without schema changes or a falsely fenced read-then-commit operation.

## Durable service boundary

Add ApprovalOperation::Recover and ApprovalRecoveryRequest containing context,
thread_id, thread_generation, execution_id, the exact available Definition,
new turn_id and trusted facts/now_ms. ApprovalFence still captures execution
revision/old owner and resource revision. The service accepts only an active
unfinished execution belonging to a strictly older store owner; duplicate,
already-current-owner, settled or stale requests are expected conflicts.

Resolve the target thread_id through token-free ThreadView, then authorize that
resource before comparing execution/generation identities or missing execution
diagnostics. Authorize its immutable origin through the
explicit Recover operation before decoding sensitive grant/acceptance rows or
returning protected state/identity/definition diagnostics. Verify exact full
registered Definition, identities and captured fences; recheck after sensitive
read. Recovery is the only operation that permits a prior-owner fence. Ordinary
park/pending/accept/dispatch continue requiring the current owner.

ApprovalMutation::Recover carries this complete request. Under the MemoryStore
mutex or SQLite immediate transaction, recheck thread ID, generation, active
execution ID, old execution fence and resource revision; compare the exact
available Definition; recompute the KernelCommand::Recover plan using the current
snapshot and new store owner, and require exact equality with the proposed plan.
Persist owner/snapshot/resource revision/journal atomically. Grant records and
acceptance audit remain byte/identity-equivalent; no issuance, consumption,
refund, reapproval or acceptance audit is added by recovery. No DDL migration.

The transaction must reject a changed resource fence even if execution revision
is unchanged, and reject thread-incarnation mismatch. It must reject forged
non-Recover/Resume plans and wrong definitions before committing. SQLite journal
failure rolls the entire owner/resource/snapshot transition back.

## Embedding API and worker

Add a documented trusted target carrying thread_id, thread_generation,
execution_id and ApprovalFence, constructible from an ordinary coherent
ThreadView. Principal is verified server-owned identity supplied by the embedding
application; user metadata/body cannot provide it. Add explicit Operation::Recover
to ResourcePolicy. Public authentication and routes remain unchanged.

Expose async HttpHost::recover_execution(principal, target) -> Response only on
the explicit approval-enabled host path. A host without the bridge rejects with
a fixed unsupported response. This response is for embedding/recovery orchestration
and is not a new Agent Protocol endpoint. Use fixed static errors without token,
prompt, database or raw authority diagnostics.

Resolve the target's thread view, require immutable-origin equality and current
Recover policy before protected diagnostics, require exact registered route and
full Definition, and compare every target identity/fence. Reserve global capacity
and the per-thread claim before committing recovery. The owned blocking worker
repeats fresh ordinary authority/coherence checks and calls the bridge's durable
service recovery. No mutation occurs on policy/capacity/claim/stale rejection.

After commitment, disposition determines behavior:

- Accepted: existing worker commits Start, then dispatches.
- Running Ready, including accepted Once: skip Start and dispatch every callback
  through the bridge, renewing current policy and exact accepted-call binding.
  Original grant expiry no longer requires another approval for committed Once.
  Recovery retains the original attempts budget and history. A recovered Pure
  retry needs spare declared budget; exhaustion is a benign static409
  recovery_attempt_limit after adoption, with no callback/done, budget reset or
  readiness failure. Count eligible Ready dispatches against the remaining budget
  before running recovered work; later-batch BudgetExceeded also remains an
  expected rejection, never an infrastructure fault. Never alter the recorded Definition.
- WaitingForApproval: remain parked, never re-mint or invoke an effect. A stream
  may disclose the same native interrupts only after current ReadPending policy,
  then one null done. Dedicated GET pending becomes available under current owner.
- RecoveryBlocked: retain the committed adopted blocked state and return static
  409 recovery_blocked before successful stream admission; no callback/done and
  readiness stays healthy. The host registry supports Pure fixtures only; it
  cannot adopt an unregistered Unsafe/Reconcilable definition. Embedded service
  tests qualify those conservative kernel dispositions separately.
- Recover settles Cancelled: emit one native done {output:{cancelled:true}};
  no callback or uncertainty failure. Failed uses the existing known-failure
  terminal. Already-settled targets reject before mutation.

Expected authorization/stale/owner/definition/state rejection stays healthy;
failed commitment, corruption or panic make readiness unready and preserve the
uncertain durable boundary. Guards remain owned through effect quiescence even
when the embedding caller abandons the future after launch or drops the returned
response/body. No arbitrary recovery of a currently live owner's attempts.

## Qualification and limits

Meaningful red-first tests cover service prior-owner authorization and no sensitive
read before denial, complete target identity/fences, resource-only races, exact
plan/definition, original accepted audit/grants, expired accepted continuation,
parked unchanged identity, same-owner duplicate, safe retry/unsafe block/cancel
and SQLite transaction failure/reopen.
MemoryStore has no public owner-advance/restoration API: qualify same-owner
rejection and shared validator there, and actual prior-owner success/rollback
through SQLite reopen. Do not add an artificial Memory restore API or claim
Memory process-restart evidence.

Host tests cover no public recovery route, unsupported ordinary host, current
policy before mutation, exact registered definition, stale/capacity/claim no
mutation, accepted-before-reopen completion once over actual TCP, stable parked
recovery, disconnect guard/owner retention, cancelled/blocked healthy disposition
and renewed dispatch policy. Existing examples, grant/workflow evidence, full
Rust tests, lint/format and contract integrity remain required.

No automatic scan/queue, total-history limits, arbitrary adapter exactly-once,
unsafe reconciliation, backup/disk-full or full crash matrix is claimed. This
increment changes no seven-operation inventory, release gate or raw reference
capture. Historical verification receipts remain immutable; new evidence is saved
separately.
