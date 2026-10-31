# PermissionUse first Dispatch claim

Qualified and adopted on 2026-10-08. This embedded single-task slice persists a
Dispatch claim from a genuine earlier Always permission and a later token-free
PermissionUse. It does not invoke a tool callback.

The positive SQLite test verifies the exact claimed attempt, resource transition,
new journal, and unchanged original permission provenance. The late journal fault
test verifies transaction rollback and complete storage preservation. Both close
all aliases, reopen under a third owner, and remove their temporary directory
before the final behavior assertion.

No-run compilation, formatting, strict lint checks, both focused tests and the
full workspace suite passed. The full suite requires loopback listener access.
Reported test totals include repeated child fixture results.

The fresh Dispatch account has an immutable 16 MiB byte limit. Ordinary accounts
retain 8 MiB; both retain the 65,536-node limit and existing record/depth caps.
The corrected retained-charge upper is 9,764,279 bytes and 56,052 nodes, including
the previously omitted catalog entry purchase of 82,048 bytes and 322 nodes.
Observed positive usage was 9,407,702 bytes / 56,037 nodes; rollback usage was
9,397,433 bytes / 56,036 nodes.

[Qualification record](qualification.json) binds the actual commands, source and
evidence hashes. Original failed build and sandbox listener attempts remain in
the external evidence history.

[Owned NewUse claim renewal](../use-renewal/README.md) is separately qualified.
[Owned callback consumption](../use-callback/README.md) is separately qualified.
Human and legacy renewal, outcome integration, recovery and
broad `durable_permissions()` capability remain open. These increments do not
close the persistent permission plan or broad release scenarios.
