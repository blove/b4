# Owned permission-use callback

A renewed NewUse permit transfers into consuming callback work. Invoking that
work once produces an opaque receipt that retains the complete permit and the
actual returned outcome, typed native error or panic payload until retirement.
The receipt exposes a fixed status, accounting usage and a borrowed outcome only
when the returned outcome passed the existing validation limits.

The selected macOS arm64 qualification covers eight genuine persisted-claim
cases: success, a returned application failure, uncertainty, Void mismatch,
65-level output, a 65,537-byte string, the fixed u8 panic payload and a callback
held in a scoped worker. Every case renews a real claim, matches the actual
invocation and context, verifies retained owners, retires the receipt, closes and
reopens the SQLite owners, checks the captured typed and raw storage groups and
removes temporary storage before its final behavioral assertions. Callback
consumption performs no outcome transaction or storage writes.

Each case runs in its own fresh process. The held case verifies that owners are
alive before release, then releases and joins the real worker before retirement.
The panic case qualifies only the selected Rust payload and the reviewed current
unwind image, exception tables, payload layouts and cleanup path.

The separate private callback account has an immutable logical limit of
8,589,934,592 bytes and 67,108,864 nodes. This is accounting capacity; creating an
account does not allocate that quantity of memory or grant authorization.
The selected entry, three borrowed-data loans and maximum native and outcome
purchases total 4,700,156,900 bytes / 36,712,857 nodes. Uncertainty, Void mismatch
and panic do not enter outcome validation and use 1,302,996,228 bytes / 10,178,609
nodes. Each case retains its own unused quoted capacity. Existing ordinary,
first-claim and renewal account domains retain their original limits.

Six pure controls cover zero initial usage and authority, rejection of other
account purposes, inclusive byte/node boundaries, sticky exhaustion and addition
overflow. A separate private configuration lowers the fresh byte limit to one
byte below the first entry purchase. Its genuine renewed permit is retained in
a refusal receipt with zero usage and zero callback calls. That reduced
configuration is not part of the adopted production source.

Qualification is limited to the selected local ARM64 Rust build, default/std
cryptographic features, empty Rust flags and the exact finite fixture bodies.
Arbitrary foreign callbacks, panic payloads, authority implementations,
contention and other targets require separate qualification. Operating-system
and diagnostic-reporting internals remain explicit external boundaries.

No storage outcome settlement, new resource or policy fence at callback entry,
cooperative cancellation, recovery integration or Driver adapter is qualified by
this increment. The broad durable-permission capability remains disabled.

[Qualification record](qualification.json) binds source bodies, commands and local
evidence hashes. The full workspace regression passed with local loopback access;
reported pass totals include repeated child fixture results.
