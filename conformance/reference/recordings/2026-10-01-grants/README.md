# Required-grant runtime observations, 2026-10-01

`grant-observations.json` contains 39 actual requests against the unchanged built
TypeScript Fetch handler at `8acac534ba6eb960b34de217b0da45b8405023af`.
`repeat/grant-observations.json` is a separate successful capture from a new
temporary app and database in a fresh credential-free Node process.

An ordinary `agent()` route runs the production agent adapter, permission gate,
generated grant minter, SQLite checkpoint/grant persistence, pending projection
and resume handler. The exported production `seedModelImporter` seam supplies a
deterministic local `BaseChatModel`. The local tool appends to a temporary ledger;
the captures make zero provider calls and zero external `fetch` attempts. No
handler, checkpoint, pending result or grant response is stubbed or seeded.

`build-provenance.json`, `install.log` and `build.log` describe the fresh isolated
checkout, frozen lockfile restoration with lifecycle scripts disabled and forced
13/13-task runtime build with zero cache hits. Both captures inventory all 385
fresh package JavaScript/JSON artifacts before importing the runtime, check the
same inventory afterward and verify unchanged tracked sources and the exact
pinned lockfile hash. Each capture has its own `runtime-artifacts.json` sidecar.
The original TypeScript working tree and historical workflow recordings remain
unchanged.

Raw HTTP request/response bytes, headers, SSE frames, generated identifiers and
local tool-ledger snapshots are retained. The limited comparison projection
matches for 37/39 requests in order. Only the two independently checkpointed
pending prompt lists reverse order between runs. `grant-repeatability.json`
records this difference: all 39 limited projections match when **only those two
pending arrays** are compared as sets. This preserves and reports raw ordering;
it makes no whole-response equality claim.

The capture demonstrates stable repeated pending disclosure and byte-identical
prompt/grant/resume-key restoration after graceful handler close/reopen. Once
executes the tool once; Deny/cancelled do not execute it; settled replay returns
`409 stale_interrupt`. The exact two-interrupt set and every presented grant are
checked before delivery. Missing grant is `400 grant_required`; malformed and
wrong grants are the same `403 grant_invalid`. Unauthorized pending disclosure
is `404`; unauthorized resume is `403`.

An additional case retains an observed route detail: a valid resume with supplied
route `/other#agent` succeeds by selecting the recorded parked `/park#agent`
route. The fixture records the TypeScript behavior independently of Rust.

This is new runtime evidence for selected CP-005/009 behavior, not closure of
either question. Always/off/optional modes, expiry, concurrent consumption races,
abrupt termination, accepted-resume crash boundaries, TCP transport and full
Agent Protocol parity remain unqualified. Raw grants belong to destroyed local
fixture databases and are not credentials for an active service.
