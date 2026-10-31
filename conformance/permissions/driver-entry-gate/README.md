# Local callback-entry control for the permission Driver

`PermissionUseDriverHooks::enter_callback` lets a trusted embedding admit or refuse local callback entry. Existing implementations default to allowing entry. Permission authority, current catalog/constraints and durable source renewal still run first; this hook grants no permission authority.

The adapter keeps the real renewed callback Work in its private slot while calling the hook outside the mutex. False retains that uninvoked Work in a distinct refusal phase. The returned failure has no callback status or settled outcome, and storage-only retry is refused. There is no callback replay or local resume API. The pending phase also retains Work if the host hook unwinds; that ownership property was source-reviewed, without a separate native panic qualification.

True continues through the existing consuming callback, receipt and typed atomic completion path. This gate controls entry only; it cannot interrupt an already-running callback and does not report a durable cancellation. It prepares the HTTP worker boundary, but HTTP integration and the broad durable-permission capability remain pending.

The two focused SQLite cases use genuine original Always approval/cancellation/reopening and normal permission reuse with UUID application/resource IDs, descriptive task/effect names, one task, modest JSON and empty metadata. Allowance calls the gate once and persists exact Complete4 with callback1/typed1/raw0. Refusal calls it once, preserves exact Dispatch3 with callback0/outcome0/raw0, and rejects retry without another entry or store change. Both retain full typed/raw state checks and owned close/reopen/removal.

The added framework control debit is prospective; all account ceilings and prior record/input/profile limits remain unchanged. Host-hook implementations retain responsibility for their own work, like the existing facts/identifier hooks. Qualification is limited to macOS arm64 with Rust 1.93.1 and this embedded shape.

Formatting, workspace compilation, strict all-target lint, both focused cases and the full local workspace regression passed on the frozen source. Ignored tests were not executed.

See the [design](../../../docs/superpowers/specs/2026-10-20-permission-driver-entry-gate-design.md), [plan](../../../docs/superpowers/plans/2026-10-20-permission-driver-entry-gate.md), and [qualification record](qualification.json).
