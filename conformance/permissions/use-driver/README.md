# Receipt-owned single-task Driver settlement

Embedded callers can explicitly select `drive_permission_use_execution` for one
already-authorized NewUse task at approved Ready revision 2. The existing Driver
performs genuine Dispatch, renewal and one callback. A private adapter retains
the actual callback receipt and settles its success or returned application
failure through the typed SQLite outcome writer.

Trusted completion facts are bound to the actual claimed execution and attempt.
The adapter accepts only the complete candidate reconstructed from the retained
receipt. It compares identity, owner/revision fences and the entire plan before
the write; ordinary raw `Store::commit` remains refused. SQLite atomically updates
the snapshot, thread revision and journal. The acknowledged execution is moved
from the validated commit without another allocation or fallible projection.

Failure retains the actual receipt/Work and cumulative accounts. Explicit
`retry_settlement` is available only for retained storage refusals/unknowns with
no local fault. It uses the same Work and trusted fresh facts; it never restarts
the Driver or invokes the callback. A returned storage error does not establish
that commit failed before becoming durable. Ambiguous acknowledgement remains
unresolved, and a completed Work refuses another write. Uncertain or panicked
callbacks do not become settled success or failure results.

The independent adapter account has inclusive limits of 16 MiB charged visited
or copied bytes, 131,072 nodes and 128 coarse operations. JSON limits are 65,536
encoded bytes, depth 64 and 16,384 nodes. Existing producer/renewal/callback/outcome
limits stay unchanged. The private NewUse producer now supports
[current-run UUIDs and bounded structured JSON](../normal-inputs/README.md); the
public Strict producer retains its original limits. Inherited actor/policy labels
remain limited to 33 combined bytes and task IDs to 7 bytes. Original approval
identities and HTTP integration remain outside this embedded extension.
These functional limits do not bound total process memory or all Rust operations.

Twelve SQLite Driver integration cases, seven private validation/account controls
and eight existing outcome cases pass on macOS arm64 with Rust 1.93.1. They cover
success, actual returned failure, full-plan substitution refusal, raw-store refusal,
renewal refusal, uncertain/native-panic receipts, completion-hook/backend panic
retirement, guarded late rollback, explicit retry, repeated storage refusal and
controlled resource-revision corruption with literal restoration. Each integration
case closes its owners normally and verifies persisted state after reopening.
Compile-time Send/Sync checks cover the concrete Work/failure carriers. Dedicated
poisoned-slot and one-consume behavioral controls are not separately qualified.

Workspace compilation, strict lint, formatting and the full normal workspace
regression passed. Ignored tests were not executed by that regression.

The supported owner, turn and full resource remain unchanged. Broader resource
transitions/cancellation, recovery, human/legacy paths, mixed or parallel tasks,
HTTP integration, broader original approval identities, lost-response confirmation and Linux
qualification remain open. The broad durable-permission capability stays disabled.
See the [qualification record](qualification.json) for the exact code and checks.

The original qualification record below is historical to its recorded source.
The current-run input extension has a separate qualification record linked above.
