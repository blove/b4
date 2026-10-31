# ADR 0012: Fence cancellation to the owned turn and retain work through drain

Status: implemented; fresh verification is scoped to 391 native Rust tests and
the named examples/reference comparisons, with the full release gate open.

The TypeScript lifecycle captures show cancellation acknowledging before the
effect exits, with deletion and another admission refused until quiescence. A
request held in asynchronous policy cannot cancel a successor. Stable approval
parks are not active cancellation targets.

The native host captures an immutable run control before policy, renews exact
origin/authority and persists cancellation through a typed full-fence backend
transaction. It removes unconsumed grants atomically while preserving consumed
Once receipts and acceptance evidence. Kernel cancellation also handles locally
owned Accepted work in the admission/Start gap. An explicit CancelAttempt
acknowledges a positively never-entered or cooperatively quiesced invocation
without inventing success/failure or refunding its attempt budget.

A per-run gate serializes invocation admission with cancellation intent, and a
single terminal publisher returns wait409 run_cancelled or streamed cancelled
output promptly after commit. Producer ownership of claim, store and capacity
survives that response until actual quiescence. Actual late outcomes stay durable;
cancelled writes do not reduce state, and a real failure retains precedence while
being suppressed as a second wire terminal. Infrastructure failure remains an
unready host condition even after cancellation publication.

This bounded scripted increment does not qualify arbitrary callback interruption,
shutdown deadlines, automatic restart scanning, actor/cause diagnostics, GET
attachment or the full core release profile. It needs no schema migration or new
dependency. See owned-run-cancellation-design.md for implementation fences and
the cancellation plan for fresh review and verification evidence.
