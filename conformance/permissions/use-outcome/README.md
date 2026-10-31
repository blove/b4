# Receipt-owned permission-use outcome settlement

A settled `PermissionUseCallbackReceipt` can be consumed into
`PermissionUseOutcomeWork`. Work retains the genuine permit, callback result and
owner anchors. Calling `commit` records that retained success or application
failure; it never invokes the callback, authorizes another use or accepts a
replacement result.

The SQLite adapter checks the complete claimed resource and retained permission
provenance, reconstructs the kernel transition and compares the complete plan.
One Immediate transaction updates the execution snapshot and thread revision and
appends the journal. Each write must affect exactly one row. The adapter returns
the opaque commit only after SQLite acknowledges the transaction commit.

An error preserves Work and its cumulative account. The host may explicitly
retry storage with the same Work; no automatic retry or callback replay occurs.
After a known successful commit, Work refuses another commit. An ambiguous
commit error supplies no invented success or lost-response confirmation.

The supported projection is a single-task NewUse claim with its original owner,
turn and complete resource fence unchanged. Cancellation, concurrent resource
changes, recovery, human and legacy renewal require separate integration. A
[single-task embedded Driver bridge](../use-driver/README.md) is now qualified
separately, including fresh outcome compatibility checks. The raw `Store::commit`
guard stays in effect. The broad
durable-permission capability remains disabled.

The separate outcome account has fixed logical limits of 4 GiB and 16 Mi nodes,
checked addition and sticky exhaustion. Existing input, depth and row limits
also apply. These accounting limits are not a bound on process memory, execution
time or all Rust library operations. The earlier exhaustive operation audit is
unfinished and is not claimed by this implementation.

Eight SQLite integration tests and fifteen private protocol controls passed on
macOS arm64 with pinned Rust 1.93.1. They cover actual success and returned
failure, guarded late-journal rollback, explicit storage retry, completed Work
refusal, opaque-request replay, controlled source/resource/owner corruption and
a genuine foreign physical store. Each integration case verifies complete typed
and raw storage, one matching callback, normal owner retirement and reopening.
Corruption cases restore their original cells; they do not qualify a supported
cancellation or live-owner handoff.

Workspace compilation, strict lint, formatting and the full normal workspace
regression passed. Ignored tests were not executed by that regression. These are
selected local functional results; Linux and the exhaustive operation audit are
not qualified here.

[Qualification record](qualification.json) binds the adopted code and fresh
results. Detailed logs and reviews are retained in the local ignored
`.b4-work/outcome-recovery` directory and are not distributed with the source.
Recovered older records remain historical evidence; no historical result was
transferred to this implementation.
