# Durable settled application failures

The next bounded Rust increment adds explicit known application-failure outcomes
to the shared kernel/runtime and scripted HTTP host. User continuation authorizes
this implementation. The supplied RFC remains architectural input; this does not
close M0, qualify arbitrary adapters, or enable grants/continuation endpoints.

## Representation and lifecycle

Add `TaskStatus::Failed` and optional task `failure: String`. Older version-1
snapshots without that field remain readable through a default; serialization
omits absent failure fields. New failure-bearing snapshots remain version 1 in
this unreleased format. Older binaries reject their unknown status/field rather
than silently dropping it. No SQLite table/schema migration is needed.

`KernelCommand::FailAttempt {task_id, attempt_id, failure}` accepts an explicitly
settled failure from its matching InFlight attempt. Reasons must be nonempty and
at most 1,024 UTF-8 bytes. A failed task has an attempt, no success result, and a
failure reason; other task statuses cannot carry that field. Repeated identical
failure delivery is a no-op. Changed failure or success/failure conflicts reject.

The first committed failure reason becomes `Snapshot.failure`. Cancel Ready and
WaitingForApproval siblings, retain successful and InFlight siblings, and stop
Dispatch/Park/ready candidates. Use `Running` plus a failure reason as the bounded
drain phase while another attempt remains InFlight. On quiescence, enter Failed
with no success output. Do not reduce the failed batch or initialize later batches;
successful late results remain recorded without applying their writes.

Known application failure takes precedence over cooperative cancellation:
FailAttempt during CancelRequested enters failure drain, while Cancel rejects an
existing failure drain. This is a native rule, not qualified cancellation parity.
On explicit recovery of a failure drain, safe uncertain siblings become Cancelled
rather than replay candidates. Unsafe/reconcilable uncertainty remains blocked.
Per-task failure records retain the application cause if recovery changes the
execution-level blocking reason. Existing settled reduction failures still validate.

## Runtime boundary

Add explicit `EffectOutcome::{Succeeded(TaskResult), Failed(String)}` and
`dispatch_outcome`/`complete_outcome`. The callback returns
`Result<EffectOutcome, String>`: outer errors still indicate uncertainty and leave
InFlight. Expected failure is an explicit declaration that the effect settled.
Commit the attempt before invoking the callback. Commit the outcome before any
public terminal. Return `OutcomeNotCommitted {outcome, cause}` if completion cannot
persist; borrowed outcome completion permits retry without invoking the callback
again. Refresh revision fences and require the captured/current owner and attempt.
Preserve the existing success-only dispatch/complete API and uncertainty semantics.

## Scripted HTTP and reference comparison

Register a bounded explicit `ScriptEffect::Failure(String)` fixture. Its message
is application-authored public content, not a caught infrastructure exception or
user-provided diagnostic. Validate the reason and encoded wait/stream envelopes
against host output limits. The host executes it after committed InFlight and
derives its terminal only from committed Failed. Expected failures keep readiness
healthy and release the settled thread for a successor. Outcome commit failure
makes the host unready and sends no settled expected-failure terminal.

Preserve the independently recorded pinned TypeScript fixture: wait returns 500
with `{"error":{"kind":"execution_error","message":"fixture execution failure"}}`;
POST stream returns 200 and one
`done {"output":{"error":"fixture execution failure"}}`. Thread status is idle
afterward. Expand selected semantic comparisons from 22 to 24 cases; add native
durable Failed/no-success-output assertions. Existing historical error DTO requires
details; document this observed execution-error host variant in OpenAPI.

## Qualification

Test failed-attempt identity/conflicts, stopped scheduling and later batches,
retained sibling results, drain-held admission, cancellation precedence, safe and
unsafe recovery, restored snapshot validation and prior snapshot compatibility.
Both stores must atomically persist failure/snapshot/journal and owner fences;
test retained outcomes across failed commits and reopen. Test real TCP failure
terminals, healthy readiness, successor admission and commit-failure unknown output.
Retain the existing 62-case recordings and prior receipts unchanged. Save a new
scoped receipt after formatting, strict lint, all Rust/fixture tests, contract
checks, selected comparison and example verification. Grants, arbitrary effect
qualification, custom validation, full crash matrix and full profile remain open.
