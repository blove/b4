# Fenced thread deletion

Implement AP DELETE with an exact server-captured generation/resource/execution
target and the same owned exclusion claim as run, resume and recovery. Missing
authorized resources return empty no-store 204. Existing resources require exact
immutable origin and renewed policy. Native denied resources use 404 to preserve
the established disclosure boundary; selected reference policy denials use 403.

Deletion of idle, settled and stable parked state removes all historical
executions, journals, grants and accepted audit atomically. SQLite keeps schema3
and clears the active pointer before child cleanup. Every historical snapshot
and related ownership index is validated first; hidden unfinished historical work
or cross-thread dependencies are corruption. Active or uncertain work returns
409 run_in_flight. A worker's claim remains authoritative through quiescence,
including a committed terminal or park before worker return.

A captured resource that changes or disappears returns 409 resource_changed,
without targeting a successor. Recreating the ID changes its random generation.
Owned deletion continues when its caller disappears, retains store ownership and
records infrastructure failure in host readiness before dropping its claim.
Custom stores inherit a healthy unsupported default until they implement the
atomic capability. Cancellation and GET attachment are separate increments.

Integration review demonstrated that deletion makes revision-only admission
insufficient: recreating an idle ID resets revision0. Store/Runtime v0 fenced
admission now requires ThreadGeneration as well as resource revision and verifies
both atomically. All new workers renew current policy/origin, including ordinary
hosts. A stale request cannot admit into a foreign or same-origin replacement.
