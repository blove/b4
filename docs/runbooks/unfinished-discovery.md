# Discovering unfinished executions

Qualification status: bounded discovery passed on macOS and Linux, including
966 Rust entries on each platform. Python checks passed 96 cases.
Automatic startup recovery and the broader release gates remain open.

`Store::discover_unfinished` lets a trusted embedding find recorded unfinished
executions without supplying their IDs. A page contains typed execution and
thread identities, captured fences, route and definition revision, status,
owner relation and task uncertainty flags. These are sensitive internal data.
Keep them out of automatic logs, public diagnostics and AP responses.

## Reading pages

Start with `discover_unfinished(None)`. Process its observations and pass the
returned cursor by reference to the same owned Store instance for the next
page. Keep going when a page has no records but still has a cursor: it scanned
terminal history and advanced. Stop when the cursor is absent.

Each page examines up to 32 full execution rows, with one additional identity
for lookahead. The cursor refers to the last examined row. Selected snapshots
are limited to 1 MiB each, JSON depth 64 and 16,384 counted nodes, including
object keys. The defensive total byte limit is 32 MiB, so a page of 32 compliant
snapshots can advance. SQLite counts persisted bytes, including whitespace and
escapes; Memory counts compact serialization of its existing typed snapshot.

A page uses one coherent Store read. Separate pages do not share a snapshot:
records may change or disappear, and new IDs ordered before the cursor can be
missed. Exhaustion does not establish quiescence, whole-store integrity,
startup completion or readiness. Restart a scan when a fresh observation is
needed, then use ordinary fences for any operation.

Cursors belong to one process-local Store origin and owner generation. Moving
or sharing that Store retains its origin; another Store or a reopened owner
refuses the old cursor. Start with no cursor after reopening. Pages and cursors
retain neither the Store nor its writer lock, application or execution work.
They are not serializable restart checkpoints.

## Separately authorizing recovery

A Current record observes work recorded under the current owner. A Prior record
observes an earlier owner. Neither relation authorizes recovery or makes an
uncertain effect safe to retry.

For a candidate, retrieve its ordinary coherent thread/execution view and build
an exact `RecoveryTarget`. Independently supply the verified principal and
origin, renew policy, select the complete immutable recorded Definition and
use the captured thread, resource and execution fences. The existing trusted
`HttpHost::recover_execution` performs those checks. Refetch or refuse stale
candidates; do not replace a captured fence with the latest one merely to
make an operation succeed.

Preserve parked grants and pending identifiers. Accepted continuations do not
need a second approval solely because a grant's original expiry has passed.
RecoveryBlocked and OutcomeUnknown remain observations until existing recovery
rules and the application's effect contract permit further work. Discovery
neither dispatches an effect nor reconstructs credentials or policy.

## Refusals and ownership

Custom stores may return `UnsupportedOperation`. Built-in discovery errors
carry static classifications only. `CursorMismatch` requires a new scan of the
intended owned Store. `LimitExceeded` reports the discovery read policy; it does
not turn otherwise-valid stored data into corruption or tighten existing
known-ID write/read contracts.

A malformed selected snapshot, index or thread relationship refuses the entire
page. No partial records or advancing cursor escape an error. Even terminal
history is checked before omission. Hidden inactive unfinished work and foreign
active pointers are refused. Future ownership is refused rather than adopted.
These checks do not audit all journal/grant payloads or all unselected records.

The limits bound selected data and JSON accounting. They promise no hard CPU,
physical I/O, RSS or deadline bound: relationship queries can scan thread
pointers and serialization can scan an already-resident string before emitting
it. Discovery does not change readiness or implement the complete startup
algorithm. Actor/policy reconstruction, automatic recovery coordination, the
crash matrix and broader release gates remain separate work.

See [the design](../../spec/runtime/unfinished-discovery-design.md) and
[the implementation plan](../superpowers/plans/2026-10-02-unfinished-discovery.md).
