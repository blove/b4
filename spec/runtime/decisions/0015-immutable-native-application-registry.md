# 0015: Immutable native registration and protected operational metadata

Status: accepted and implemented; scoped verification is recorded separately.

Expose one portable immutable registry that binds a full Definition and declared
execution revision to exact Rust callbacks. HTTP uses a private common route
adapter for scripts and native registrations, then delegates to the same shared
execution driver. Historical version selection, hot replacement and dynamic graphs
remain outside this increment. Revision labels are trusted declarations; identical
labels across binaries do not prove identical Rust code.

NativeOutcome contains successful TaskResult or known public failure. Entered
callbacks cannot fabricate the host's never-entered Cancelled acknowledgement.
Value preserves null and falsy JSON; Void requires null success and preserves the
existing primary empty Done. Inactive attachment still ends with output:null.

Native application scope precedes protected Store reads and diagnostics. The
existing approval bridge renews authority for every native invocation, even when
the application has no approval prompts. Optional input validation runs within the
owned bounded admission producer before mutation, retains guards through request
abandonment and renews policy plus original generation before admission.

Add protected route-free GET /b4/capabilities as a native operational endpoint,
separate from ten core AP operations. Typed fixed metadata describes actual assembly
limits and declared Store capabilities. Default custom metadata is conservative;
physical approval durability is nullable when no ApprovalStore port was assembled
and remains independent of service mode. Readiness is the existing status-only
aggregate. Route names/counts, prompts, grants, policy revisions, paths and callback
captures are absent. Unknown objects and inconsistent assembly declarations reject.

False subsystem flags and null controls explicitly disclose absent adapters and
unbounded duration/history/process controls. Trusted callbacks remain unsandboxed;
they can perform application-owned external work. No new dependency, persisted
record or SQLite schema is needed. Existing reference captures and immutable
receipts remain unchanged; M0, CP-010 and all56 release scenarios stay open.

Design: `spec/runtime/native-application-registry-design.md`.
Plan: `docs/superpowers/plans/2026-10-01-native-application-registry.md`.
