# PermissionUse owned claim renewal

Qualified on 2026-10-09 for the embedded single-task NewUse path on macOS arm64.
Renewal consumes a genuine persisted Dispatch claim and returns an opaque owned
permit after current authorization, complete durable provenance validation, one
Claimed constraint gate, and an independent final resource reread.

The SQLite combined read completes all twelve selected header ranges before
reading provenance payloads. It checks the original approval, grant, decision,
use, creator and journal records against the genuine retained claim. Changed
policy, application or constraint revisions cannot reuse an earlier scope.

Nine integration cases cover success, authority denial, constraint denial,
three changed-scope refusals, a foreign claim account, and reuse of finished or
refused accounts. Each preserves all captured typed and raw table groups,
retires aliases, closes and reopens real owners, and removes temporary storage
before its final behavior assertion. Renewal performs no writes or callbacks.

Seven budget controls cover fresh purpose domains, inclusive byte and node
boundaries, sticky exhaustion, wrong-purpose refusal and once-only entry.
Ordinary accounts retain 8 MiB and first-claim accounts retain 16 MiB. The
separate private renewal account has an immutable 64 MiB limit; all retain the
65,536-node limit and existing record and depth limits. The reviewed renewal
upper is 54,361,479 bytes / 45,077 nodes. The final focused healthy case used
54,324,077 bytes / 45,002 nodes. A larger budget supplies no authority.

Formatting, compilation without execution, strict lint checks, all sixteen
focused tests and the full workspace regression passed. The full suite used
local loopback listener access. Its reported totals include repeated child
fixture results and are not a count of unique tests.

[Qualification record](qualification.json) binds the selected source bodies,
commands and evidence hashes. [First Dispatch claim](../use-dispatch/README.md)
records the separately qualified producer and rollback behavior.

This qualification uses the selected local arm64 build, default/std SHA-256
features, no assembly feature, and empty Rust flags. Accounting covers the
selected finite fixture authority and constraint bodies. Arbitrary foreign
implementations and their panic payload or destructor behavior require separate
qualification, as do other build targets and features.
[Owned callback consumption](../use-callback/README.md) is separately qualified
for eight finite local cases and six pure controls. Human and legacy renewal,
outcome integration, recovery and the broad durable permission capability remain open.
