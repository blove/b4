# Embedded settled permission-thread deletion qualification

The explicit Rust entry `delete_settled_permission_thread` authorizes and atomically removes one entirely settled permission-bearing thread. See the [runbook](../../../docs/runbooks/permission-settled-thread-deletion.md) and [machine-readable record](qualification.json). Existing generic deletion guards and HTTP profiles remain unchanged.

On macOS arm64 with Rust 1.93.1, the exact 1,061-file native descriptor was `98298daf956d5f227f3a41bf6bcf8c710600b0fe77c72d5a400d21d356ab6e62`. All recorded checks used this unchanged before/after map, locked offline dependencies, finite limits and confirmed process reaping. Dependencies and lockfiles remain unchanged.

| Check | Result |
| --- | --- |
| New settled-thread deletion | 4 business tests passed |
| Existing generic permission guards | 9 passed |
| Existing permission records | 89 passed |
| Formatting, strict workspace all-target lint, compile | Passed |
| Full workspace | 1,651 reported passes; 0 failures; 11 existing ignored tests |
| Metadata | Passed separately; no runtime execution credit |

The full run completed in 595.914 seconds without timeout or source changes. Its log SHA-256 is `44e06785f6bcaffa712fff6be4d74412964ef4aae864542e3a0756fc22562ca8`. Counts derive from 151 actual summary executions, including child/helper executions; they are not a unique test inventory. Temporary budget measurement motivated the deletion-only 64 MiB account; only the fresh production runs in the machine-readable record qualify the delivered source.

The four real SQLite business tests cover:

- Genuine ordinary Once approval, original Always approval and multiple saved Uses, including all eight owned tables, exact removed identities/counts, another thread's bytes, unchanged metadata and healthy reopen. Historical settled execution owners remain valid after normal owner reopen; the writer separately binds the current Store owner.
- Actual cancelled original history, a saved Use retaining real late native success, and a failed saved Use. Deletion follows actual synchronous Driver return.
- Current authority, origin confinement, stale target, mismatched historical owner, default-false capability, foreign payload-only creator references, unknown creator representations, foreign Uses and Ready history refusal. Genuine sixteen-execution success and seventeen-execution overflow provide a focused limit control.
- A guarded final thread DELETE trigger, installed after normal Store open, fires after dependent removals. Exact DDL/presence, fixed safe final-delete diagnostic, complete nine-table rollback, trigger removal and fresh healthy deletion establish this concrete atomicity result.

The caller retains per-thread exclusion and actual worker quiescence through the result. A stored terminal alone is not a worker-drain witness. The embedding's dedicated authority combines that precondition with current deletion policy; this entry owns no HTTP run registry and creates no native interruption.

Fixed limits cover sixteen executions, sixty-four records in each grant/acceptance/decision/Use table, 512 journal rows, 64 MiB cumulative charged evidence and work and 65,536 structural nodes, alongside existing record/snapshot/depth caps. Initial capture and writer recapture share one account. Row maxima do not guarantee every composition fits. Foreign creator checks may scan unrelated rows; the bounds apply to captured evidence and decoding, not whole-database SQL time. Unknown foreign creator representations conservatively refuse.

Success confirmation follows actual commit. Errors do not imply successful prior deletion, and no automatic retry occurs. This qualification excludes HTTP exposure, live work retirement, forced corrupt-data deletion, recovery, automatic callback replay, maximum-size combinations and other platforms. Tests use owned fixtures; no actual user data is deleted.
