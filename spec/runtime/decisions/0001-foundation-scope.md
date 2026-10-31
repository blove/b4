# ADR-0001: Build a portable foundation before runtime breadth

Status: accepted native design for this increment; core-profile release pending.

The architectural RFC requires a durable native runtime. An empty repository
cannot establish that release in one foundation increment. Build three private
crates: portable contracts, a pure transition kernel and deterministic scripted
fixtures. Keep time, identifiers, observed outcomes and recovery facts explicit.

No HTTP server, runtime coordinator, store, SQLite, authentication, grant
consumption, provider, container qualification or TypeScript conformance is
included. M0 remains open and M1 is covered only by named local requirements.
The authored examples and fixtures are native design inputs, never reference
runtime observations. Future work must satisfy the unchanged release inventory.

Rationale: verify semantic boundaries without publishing successful placeholders
for deferred capabilities. See `../foundation-design.md` and
`../requirements.yaml`.
