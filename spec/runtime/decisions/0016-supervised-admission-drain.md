# ADR0016: Counted admission drain for the embedded HTTP host

Status: accepted bounded design; full RFC12.3/T047 qualification remains open.

## Context

HTTP authentication, policy, readiness, store operations, validators and execution
coordinators can continue after their caller disconnects. Active-run capacity alone
cannot prove their physical quiescence: create, delete, cancel, pending disclosure
and preflight jobs do not all hold a run permit. A cancelled viewer's Done also
does not prove that entered callbacks or outcome commits have returned.

## Decision

One monotonic Serving -> Draining supervisor linearizes owner registration,
mutation leases and admission closure under a short mutex. Each protected request
registers before its first await; its cloneable owner group follows every detached
framework job. An explicit carrier drops Shared/store captures before retiring
the final owner, after scoped callback joins, commits, claims and permits.

Mutation admission is a separate lease. New leases follow final host preflight and
validation; Resume precedes grant acceptance; recovery precedes adoption;
Create/Delete precede mutation; cancellation precedes creation of Pending. A
winning operation and its service-internal policy/CAS retries are grandfathered.
Durably admitted execution continues to terminal or stable park across the cut.

`ShutdownHandle` contains no Shared or Store. `begin_shutdown` cuts synchronously;
valid `wait_quiescent` calls cut before returning an owned future. Invalid durations
above300 seconds leave admission unchanged. Zero means a nonwaiting snapshot;
the recommended bound is30 seconds. Dropped or expired waits never abort workers,
release their resources, fabricate outcomes or reopen admission.

New protected requests after the cut receive healthy503 `host_draining` before
authentication/body/policy/Store work. This uniform operational status discloses
no protected data. Liveness remains200; readiness becomes status-only503 without
a Store probe. Pre-cut readiness jobs recheck phase before reporting readiness.
Infrastructure faults are latched inside owned jobs, including abandoned jobs.

Descriptor v1 stays frozen. Descriptor v2 explicitly reports
`supervised_admission_drain:true`, the third serving-admission readiness input and
fixed recommended/maximum wait bounds. `graceful_shutdown:false` remains truthful.

## Consequences

Quiescent means registered framework jobs have returned; it does not prove success,
resolve durable uncertainty, or release application-owned host/router/Store/auth/
callback references. It may report `faulted:true` while retaining uncertain durable
evidence. Tests must prove actual SQLite destructor/reopen separately from a count
or Weak pointer observation. Unpolled external router futures cannot enter Store
work after the cut but may retain external references until their caller drops them.

This host owns no listener or executor. Callback signaling, forced viewer closure,
listener closure, persisted shutdown reasons/deadlines, automatic recovery and
closure of externally owned resources are deferred. No SQL, kernel cancellation
policy, dependency or provider changes are needed for this increment.
