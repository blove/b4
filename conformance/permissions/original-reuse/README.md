# Original UUID permission reuse through the embedded Driver

The embedded `drive_permission_use_execution` path accepts a genuine saved
Always permission created with UUID thread, execution and turn IDs. A new run
can reuse it after the original run is cancelled and its SQLite owner closes.
Ready authorization, dispatch, callback authorization and typed completion keep
their existing complete authority, catalog, constraint, history and fence checks.

The existing private NormalNewUse profile now carries its input bounds on the
verified source witness. Renewal, callback receipt, retained outcome Work and an
explicit storage-only retry keep that same witness. Strict callers and original
approval/cancellation operations retain their behavior. No new public selector,
database schema or callback replay path is introduced.

The normal context aggregate is bounded at 2,048 bytes. Inherited Acceptance is
bounded at 8,192 bytes; its before snapshot, Decision, grant and original Cancelled
snapshot at 4,096; scope body at 2,048 and wrapped scope at 2,070; flat journals at
1,024. Existing structural limits and compact application/task/effect/Definition
checks remain. Current UUID/input bounds and the 16,384-byte PermissionUse envelope
are unchanged. Additional variable work is charged before the work, using the
same cumulative accounts. Individual bounds do not promise simultaneous maxima.

Qualification covers one genuine original Always approval/cancellation/reopen,
then a new UUID execution/turn/attempt with modest nested JSON and one callback.
It does not cover executing the original approval's callback, HTTP, Linux,
mixed/parallel work or the broad durable-permission capability.

Three SQLite cases pass: exact completion after reuse, kernel-valid Definition
substitution with a retained settled result, and guarded late journal rollback
followed by an explicit retry on the same retained Work and cumulative account.
Each proves full stored-state equality and owned close/reopen/removal before its
final assertions. The guarded case is paired with normal success; the opaque
initial Driver error is not unique backend-fault provenance.

Formatting, strict all-target lint, workspace compilation, the three selected
cases and the full local Rust regression passed on the frozen source. Ignored
cases were not executed. This qualification is macOS arm64 with Rust 1.93.1.
See the [design](../../../docs/superpowers/specs/2026-10-20-original-uuid-reuse-driver-design.md)
and [qualification record](qualification.json).

Later [UUID application identifiers](../normal-application/README.md) are covered
by a separate source-bound qualification.
