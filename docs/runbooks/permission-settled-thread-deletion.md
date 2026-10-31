# Delete a settled permission-bearing thread through Rust

Use the explicit `b4_runtime::permissions::delete_settled_permission_thread` entry with a Store declaring `permission_settled_thread_deletion_atomic_durable()`, current immutable `ApprovalContext`, a dedicated `PermissionThreadDeletionAuthority` and the exact `ThreadDeletionTarget` captured from a coherent view. Real SQLite supplies the new typed atomic writer. Generic deletion retains its permission guard.

The embedding retains per-thread operation exclusion and waits for actual Driver/callback quiescence before calling this entry, holding exclusion through the result. A stored terminal is not proof that a live worker has drained. The dedicated authority checks that embedding precondition along with current DeleteThread policy and origin. This embedded API does not inspect an HTTP run registry or terminate native code; later HTTP wiring uses the host's existing exclusion/ownership path.

```rust
use b4_runtime::{Store, StoreError, ThreadDeletionTarget};
use b4_runtime::permissions::delete_settled_permission_thread;

// After the embedding has drained this thread and holds its exclusion guard:
let view = store.thread_view(&thread_id)?.ok_or(StoreError::ThreadNotFound)?;
let target = ThreadDeletionTarget::from_view(&view)?;
let confirmation = delete_settled_permission_thread(
    store.clone(), deletion_authority.clone(), &context, &target,
)?;
```

Every owned execution must be Completed, Failed or Cancelled with a valid settled task/history relation. The full thread, execution, journal, approval and permission set is validated and removed atomically. Legitimate prior-owner terminal snapshots remain deletable after normal reopen: the target preserves their exact historical execution fence, while writer freshness binds separately to the current Store owner. Current deletion authority may approve destruction of historical permission records without invoking their effect constraints or reinterpreting them through the latest handler.

The fixed operation limits are 16 executions, 64 rows per grant/acceptance/decision/Use table, 512 journal rows, 64 MiB cumulative charged evidence and work and 65,536 structural nodes, alongside existing per-record/snapshot/depth caps. Initial capture and writer recapture spend the same account. Limit exhaustion refuses without writes; individual maxima do not guarantee every combination fits. The limits bound returned evidence and decoding, not whole-database SQL scan time.

Both directions of ownership are checked, including foreign Uses of owned permission IDs and foreign decision payloads whose creator points at an owned execution. The schema has no indexed creator column. A malformed, oversized or unknown foreign creator representation that cannot safely rule out this reference conservatively refuses deletion. Other thread records and metadata remain intact.

The writer rechecks schema metadata/current owner, full target and source under one Immediate transaction, clears the active pointer, deletes Uses before Decisions and every dependent row before executions/thread, checks actual row counts/absence and commits. The opaque confirmation exposes only the target, current Store owner, removed execution identities and row count. Success comes from the actual commit; missing state or an uncertain writer outcome does not fabricate a successful receipt. No automatic retry occurs.

The operation does not retire Ready, approval-waiting, active, cancel-requested or recovery-blocked work. It adds no HTTP capability, callback interruption, recovery, automatic replay or force deletion of corrupt records. Use only intentionally selected data. The scoped conformance record will identify the actual tested platform and fresh execution evidence separately from metadata validation.
