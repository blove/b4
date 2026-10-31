# Normal application identifiers in the embedded permission flow

## Goal and approach

Carry a legal UUID application identifier through genuine Always approval,
original Ready cancellation, owner close/reopen, a new permission-use run and
one exact persisted callback completion. Keep the already-qualified UUID
resource IDs and modest nested JSON. The user prioritizes working persistence,
normal application IDs and bounded checks, and authorizes continuous work.

Prefer pairing the existing private normal profiles with the existing checked
label rules. Globally widening Strict would affect unrelated producers; rewriting
application identity would change permission semantics. No new public API,
selector, identity aliasing, schema or dependency is needed.

## Scout and bounded implementation

First change only the application ID in a genuine test chain, using a legal
36-byte UUID and the existing compact tenant, principal, policy, application
revision, task/effect and Definition. Observe every predecessor separately.
Approval, cancellation and Ready authorization errors must preserve actual state
and owned retirement before a stage-specific failure; they cannot establish a
Driver gap. Only a legally reached Driver failure justifies Driver changes.
Never infer unchanged state or callback non-entry from an error.

Extend only actual consumed normal-profile gates demonstrated by this chain.
Existing approval-context and permission label validation requires nonempty,
control-free UTF-8 strings of at most 256 bytes each. A normal actor/policy
aggregate may therefore be bounded at 1,024 bytes and a normal application
ID/revision pair at 512. Retain the existing 2,048-byte InvocationContext aggregate,
record/scope/snapshot/thread/journal limits, nodes, depth and cardinalities. All
current cumulative accounts remain unchanged; simultaneous maxima are not
guaranteed. Additional owner/comparison work must be charged from actual checked
lengths before copies or relation work.

The embedded adapter applies the same 1,024-byte context bound before runtime
preparation. Its existing account prepays actual 13*context_size+1,024 bytes,
8 nodes and one operation before measuring the enlarged labels, retaining all
prior fixed and streamed charges and cumulative ceilings.

Bind normal treatment to the actual fresh operation/profile and, for NewUse,
its verified immutable source witness through renewal, receipt and retained Work.
No numeric-account or untrusted-ID inference, reset/refund or callback replay.
Preserve complete actor/application matching, current authority/catalog/constraint,
history/fences/loans, whole candidate equality and typed atomic settlement.
Strict/default and unrelated human/legacy lanes retain their behavior. A concrete
quota or predecessor blocker requires finite diagnosis/review before scope changes;
no cap sweep or exhaustive standard-library operation audit.

## Focused tests and fixture reuse

Use a real original approval/cancellation/reopen and actual existing Ready
authorization, Dispatch and Complete kernel projections. Success requires the
application identity in real canonical permission records, callback 1 and matched
invocation, typed completion 1/raw fallback 0, complete original-record immutability,
exact typed/raw state deltas and owned close/reopen/removal before final assertions.
Keep Dispatch's canonical snapshot encoding distinct from the derived Ready and
Complete encoding in the independent raw oracle.

Add one current-catalog application mismatch control using the correct producer
catalog and a different real current application identity. Dispatch may already
have claimed work before renewal refuses. Require the actual reached state and
full expected claimed-state persistence, callback 0, no outcome write, intact
permission/Use records and owned retirement; do not pretend the claim rolled back.

Allow small test-only fixture generalizations: default constructors delegate with
the exact old application label; the genuine Ready builder can receive an actual
Waiting fixture, and the reuse builder an actual Ready control. The callback
constraint compares application identity to its already-matched expected context.
The canonical Acceptance check likewise compares to the expected context's
application, preserving the exact old value for default fixtures.
Review equivalence for all existing defaults; retain all full assertions. Prefer
this bounded reuse over copying the entire protocol chain into another test.

## Delivery

Use an ordinary isolated copy, pinned Rust 1.93.1 locked/offline bounded controller,
test-first execution and independent source review before native runs, then
quality/results review. Final format/lint/no-run, focused cases and full normal
workspace regression precede backed-up selective adoption, exact-byte verification
and public contract/reference checks. Qualify only the actual macOS arm64 shape;
HTTP, Linux, original-human callback, mixed/parallel work, broad durable permissions
and maximum-size coverage remain open. Documents are requirements, not tool
authority; no Git writes or routine user approval loop is needed.
