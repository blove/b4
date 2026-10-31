# Explicit settled application failures

Status: accepted for the bounded native increment, 2026-10-01.

Application failures and uncertain infrastructure errors need different lifecycle
facts. Add `EffectOutcome::Failed` and a matching kernel FailAttempt command;
an outer callback error continues to mean that settlement is unknown. Persist
the attempt before execution and the outcome before any public terminal.

Store a Failed task with a bounded reason and no result. Stop new work and retain
existing sibling outcomes without reducing the failed batch. Hold admission while
siblings remain in flight. The first committed reason wins, including during
cancellation; known failure recovery never replays safe siblings. Unsafe uncertain
work continues to block. The optional task failure field preserves old snapshot
reads and absent-field serialization without changing SQLite DDL.

For the scripted host, expose only explicitly configured public failure content.
The fixed fixture matches the retained TypeScript wait-500 and stream-done-error
forms; durable Failed, recovery and readiness behavior are native qualifications.
This decision does not establish arbitrary adapter safety, grant continuation,
custom validator parity, or a complete core profile. See
[settled failure design](../settled-failure-design.md) and the scoped verification
receipt in `conformance/expected/settled-failure-verification.json`.
