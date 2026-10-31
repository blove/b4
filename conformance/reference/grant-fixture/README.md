# Independent required-grant reference fixture

This application exercises the actual TypeScript Agent Protocol Fetch handler at
`8acac534ba6eb960b34de217b0da45b8405023af`. It uses an ordinary `agent()` descriptor
and approve-gated tool. Production `seedModelImporter` supplies a deterministic
local `BaseChatModel` constructor; the production agent adapter, permission gate,
grant minter, SQLite checkpointer, pending projection and resume handler run
without source modifications. The model calls no provider. The tool appends to a
temporary local ledger and calls no business API.

Run each capture in a fresh credential-free process against a fresh pinned build:

```sh
env -i PATH=/path/to/node/bin:/usr/bin:/bin TMPDIR=/private/tmp \
  node conformance/reference/grant-fixture/capture.mjs \
  --reference-root /private/tmp/b4run-grant-reference-8acac534 \
  --output /path/to/new/recording-directory
```

The app receives the fresh reference checkout's dependency tree through a
temporary `node_modules` symlink. No installed or built artifact from the original
TypeScript working tree is used. Provider credential/configuration and tracing
environment variables are removed before model construction. The constructor
accepts only the fixture's model and retry options. All execution-time `fetch`
attempts throw, and a successful receipt requires zero such attempts.

The capture verifies the exact Git commit, unchanged tracked sources and pinned
lockfile hash. It inventories the newly built package JavaScript/JSON artifacts
before importing the runtime, then checks the same inventory and source identity
again afterward. Output creation refuses to replace existing recordings.

The 39 requests characterize required-grant park, repeated pending disclosure,
unauthorized disclosure/resume, missing/malformed/wrong grants, strict decision
and entry parsing, exact pending set, handler restart, Once/Deny/cancelled,
two independent pending calls, replay and recorded route provenance. A complete
handler close/reopen restores the same raw pending body. This is a graceful
restart, not a process-kill or post-consumption crash test.

The route field in a valid resume is accepted even when it differs from the
recorded parked route: the actual pinned handler selects the recorded route.
This observed behavior is retained rather than replaced with a Rust expectation.
The settled replay returns `stale_interrupt`; this capture does not characterize
the concurrent `grant_consumed` race.

Raw requests, HTTP status/headers/body bytes, SSE frames and local ledger snapshots
are retained. The separately named comparison projection omits opaque generated
identifiers and output message internals while retaining exact refusals, prompt
shape, event names and ledger order. Projection equality is deliberately narrower
than whole-response equality. Raw fixture tokens identify destroyed local test
databases and are not credentials for a running service.

Run the projection's behavioral checks with:

```sh
node --test conformance/reference/grant-fixture/projection.test.mjs
```

Always/off/optional, TTL expiry, races, abrupt termination, accepted-resume crash
boundaries, TCP transport and whole-protocol parity remain outside this fixture.
It contributes runtime evidence to CP-005/009; it does not close those questions.
