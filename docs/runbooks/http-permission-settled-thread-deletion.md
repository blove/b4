# HTTP settled permission-thread deletion

Select `HttpHost::new_with_permission_settled_thread_deletion` explicitly to enable deletion of complete settled permission histories. It takes the same Store, authenticator, resource policy and PermissionHostConfig arguments as the withheld-cancellation constructor. The Store must declare the dedicated atomic durable deletion mechanism. This assembly inherits the existing permission approval, reuse and cancellation support.

Discover the assembly through its strict V8 descriptor. Its narrow `durable_permission_settled_thread_deletion` feature is true; the broad `fenced_deletion` feature remains false. Earlier constructors retain their existing descriptors and refusal behavior.

Send authenticated AP-003 `DELETE /threads/{thread_id}` through the normal application scope. Current DeleteThread policy and recorded origin must allow access. The current tool catalogue or effect constraint does not authorize destruction of old records; deletion uses its own current resource authority.

A present thread must have an active Completed, Failed or Cancelled execution and an entirely settled valid history. A real run worker still holding the thread claim prevents deletion, including after it has saved a terminal state. Wait for actual worker completion and cleanup before attempting a new request. The host owns the deletion worker, thread claim and mutation lease through the actual write and result handling even when the HTTP caller disconnects. Embeddings must still coordinate unrelated direct SDK users under the embedded ownership contract.

For a captured present resource,204/no-store follows only an actual committed SDK confirmation matching the exact deletion target. Authorized initial absence retains the protocol's existing idempotent204 semantics; that response is not a typed committed confirmation. A resource that changes or disappears after capture returns 409. The host never retries the writer or replays a callback automatically.

| Condition | HTTP response |
| --- | --- |
| Current authority or origin does not permit access |404 resource_unavailable|
| Actual thread claim held or coherent nonterminal state |409 run_in_flight|
| Captured target changed or disappeared |409 resource_changed|
| Fixed deletion evidence limit exceeded |409 permission_limit_exceeded|
| Coherent present thread without an active execution, or unsupported profile |501 permission_operation_unsupported|
| Host admission closed |503 host_draining|
| Storage, corrupt-history or other infrastructure failure |Static 503; host readiness faults|

Busy, bounded-limit and unsupported-profile refusals preserve healthy readiness. Infrastructure failure requires investigation and normal owner drain; a later absence does not reconstruct an uncertain prior confirmation. A demonstrated execute-phase SQLite error rolls back its transaction, but this does not promise rollback for every uncertain commit outcome.

The inherited SDK ceilings are 64 MiB cumulative charged evidence/work, 65,536 nodes, 16 executions, 64 rows per grant/acceptance/decision/Use table, 512 journal rows and existing snapshot, record and depth caps. All maxima need not fit together. The operation validates the complete owned history and reverse foreign references; malformed or unknown foreign decision creator evidence conservatively refuses. These bounds do not establish an end-to-end HTTP allocation limit or a global SQL-time bound.

The writer removes the thread's rows from the eight owned thread/execution/journal/approval/permission tables atomically while preserving metadata and unrelated threads. It does not delete application files, invoke effects, recover parked work, interrupt callbacks or force removal of corrupt records. The existing generic Store guard continues to refuse permission-bearing deletion.

See the [local qualification](../../conformance/permissions/http-settled-thread-deletion/README.md) for the exact source snapshot, commands, results and practical limits.
