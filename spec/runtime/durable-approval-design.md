# Durable required-grant approval service

Status: bounded native increment, 2026-10-01. User continuation authorizes
implementation in this workspace. The RFC supplies architectural requirements;
it does not authorize external actions or modify the TypeScript checkout.

Build an embedded approval service before enabling HTTP resume. Required grants,
complete-set acceptance and committed continuation snapshots become working
native behavior. HTTP approval definitions and resume remain disabled. The
existing 62-case reference recordings contain no grant/park evidence; CP-005 and
CP-009 remain unresolved. Native error ordering is an explicit decision.

## Choice and scope

Use opaque random tokens and dedicated sensitive records in the same database
transaction as the execution snapshot. Signed tokens alone cannot provide atomic
consumption or restart disclosure; an independent grant database would split the
continuation transaction. No signing or encryption key is needed for this design.
Retained plaintext makes the entire database and backups sensitive.

Support Once and Deny (including cancelled entries). Reject Always explicitly;
the current task model lacks stable exact-tool permission scope. Required is the
only supported grant mode. A durable service constructor rejects volatile stores;
an explicit volatile-fixture configuration supports MemoryStore qualification.
No provider, arbitrary external adapter, automatic scan, persistent permissions,
HTTP grant endpoint or complete core profile is qualified here.

## Authority and binding

Add a server-generated immutable ThreadGeneration to ThreadRecord, generated
from 32 OS-random bytes. ThreadSeed never accepts it. Known SQLite v1/v2 databases
upgrade transactionally to v3, assigning legacy thread generations once. Preserve
v1/v2 DDL/checksums and reject unknown schemas before advancing ownership.

ApprovalContext contains the verified server-derived ThreadOrigin actor and a
bounded policy revision. ApprovalAuthority is a required host port receiving that
context, an operation and a coherent thread/execution resource, never tokens.
This bounded service permits only the thread's immutable originating principal.
Authority runs before sensitive records are disclosed or grant errors considered.
Resource and execution fences are captured before policy and rechecked at commit.
Policy is rechecked before the service dispatches an accepted approved task.

Authorize using a token-free coherent Store thread/execution view first. Loading
ApprovalView can reject corrupt grant rows; that sensitive read must follow
authority and then recheck both captured fences. Invalid or corrupt rows cannot
produce grant-specific diagnostics to an unauthorized caller. Before approved
dispatch, match current task specification/input/prompt against the consumed
grant's canonical binding and its Once acceptance, rather than trusting only
the kernel's approved flag.

GrantRecord binds thread/generation/origin, execution, recorded route/definition
revision, task/effect, interrupt, opaque resume key, original task parking revision,
policy revision and a versioned canonical proposal digest. The digest covers exact
task specification, committed task input and prompt; recursively sorted objects
preserve arrays/nulls. It certifies no external business-object version. Token is
`b4ag_` plus 64 lowercase hex characters from 32 OS-random bytes; SHA-256 is the
verifier. Tokens and verifier material are redacted from Debug and errors.

The original per-task parking revision is distinct from the current stable-park
fence. Later sibling completion and explicit recovery advance revision/turn/owner
while preserving the grant and recorded call. Verify the current waiting task
against its immutable binding, then fence the current snapshot separately.
Expiry uses supplied trusted time, checked addition, bounded positive TTL (up to
24 hours), and rejects `now >= expires_at`. Accepted decisions do not expire later.
Prompt and canonical proposal bytes are bounded to 65,536, with JSON nesting
limited to 64 before canonicalization or persistence. This keeps saved snapshots
and sensitive records within the decoder's supported depth.

## Shared transaction interface

Put models, validation and service in `b4-runtime::approval`; implement the same
ApprovalStore extension in MemoryStore and SqliteStore. Preserve the existing
Store/Runtime APIs for trusted native work. Dedicated required-grant operations
never downgrade to legacy unrecorded approval. Ordinary Store commits reject
approval/denial changes for grant-bearing pending tasks unless the acceptance
is recorded through the approval transaction; recovery and cancellation remain
available under their existing trusted boundaries.

The agreed interface is:

```text
ApprovalFence { execution: ExecutionFence, resource_revision: u64 }
ApprovalView { thread, execution, grants, acceptances }
ApprovalStore: Store
  approval_durable() -> bool
  approval_view(execution_id) -> Result<Option<ApprovalView>, ApprovalError>
  commit_approval(execution_id, ApprovalFence, CommitPlan, ApprovalMutation)
      -> Result<StoredExecution, ApprovalError>
ApprovalMutation = Issue(GrantRecord) | Accept(ApprovalAcceptanceRequest)
ApprovalChanges { grants: Vec<GrantRecord>, acceptance: Option<ApprovalAcceptance> }
validate_approval_change(view, fence, plan, mutation, current_owner) -> ApprovalChanges
reject_unrecorded_decisions(current, proposed, grants) -> Result<(), StoreError>
```

Issue validates an exact kernel Park proposal and its bound grant, then commits
snapshot, thread revision, journal and grant together. The service's park method
returns StoredExecution; sensitive pending entries are disclosed only from a
stable WaitingForApproval snapshot, after commit and renewed authorization.

Accept validates an exact kernel Resume proposal for the captured current state,
the complete pending set and all relevant grant records before mutating anything.
Consume all records, save a non-secret ApprovalAcceptance containing decisions,
actor/policy context, old snapshot revision, new turn and definition revision, and
commit the resumed snapshot/resource revision/journal in one transaction. Store
validation reconstructs the kernel proposal rather than trusting an arbitrary
CommitPlan. No effects or authority/network callbacks run inside the transaction.

The accepted turn and its Ready tasks form the durable continuation marker;
there is no independent in-memory queue requirement. A trusted host may reopen,
explicitly recover that known execution under the exact definition, reconstruct
authority and dispatch through the service without asking for another approval.
No autonomous intent scanner or full scheduler is claimed. Denial creates no
approved task and never invokes the effect. Resubmission cannot accept twice.

Dispatch persists InFlight before its final authority/current-view check. If that
check fails before callback invocation, the conservative record remains InFlight;
grant consumption is not refunded. Explicit reopened-owner recovery applies the
declared Pure/Idempotent retry or Unsafe/Reconcilable blocking policy. A special
known-not-invoked attempt-abort transition remains future work.

Native validation order: authority/scope; owner/execution/resource fences; stable
park and recorded route; exact nonempty pending set; unsupported Always; grant
availability, missing token, uniform invalid binding/token, consumed, expiry;
kernel proposal and transaction. Rows are evaluated in definition order, giving
deterministic multi-fault precedence. This is not executed TypeScript parity.

## Persistence and qualification

SQLite v3 adds generation, approval_grants and approval_acceptances. Index fields
must agree with strict serialized records on load. Grant tokens never enter
Snapshot, ordinary state projection, journal or acceptance records. Pending reads
are dedicated sensitive outputs. Both stores test the same atomic behavior;
SQLite additionally tests failed inserts/updates/journal rollback, exact migration,
corrupt records, reopening identical prompts/grants, and acceptance then restart
before dispatch. Concurrent acceptance has one winner. Failed park publishes no
prompt; failed acceptance spends no authority and makes no task ready.

Keep all historical receipts and reference bytes. Write a new scoped receipt
after fresh formatting, strict lint, all Rust/fixture/contract checks, original
24 comparisons and a no-provider approval example. This increment advances the
native M4 work without closing M0 or any broad T-001–T-056 release scenario.
