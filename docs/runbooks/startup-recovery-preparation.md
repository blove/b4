# Bounded startup recovery preparation

Status: verified within the opt-in bounded native preparation scope on macOS and Linux. See the [scoped verification receipt](../../conformance/expected/startup-recovery-preparation-verification.json).

Use this opt-in embedding API to prepare prior-owner work for one installed native application before opening ordinary admission. It requires an actually durable approval Store, RequiredDurable approval configuration, the exact registered complete Definitions, current ResourcePolicy and a trusted StartupPrincipalResolver. Existing host constructors keep their current behavior. The CLI does not start this pass automatically.

The resolver must consult server-controlled current authority. Stored ThreadOrigin labels are a reference to that authority; copying them into a Principal is not authentication. All three returned labels must match the original immutable origin and the native application scope, and current policy is checked again before recovery. Missing authority, a changed complete Definition, stale fences and an unexpected current-owner execution leave preparation incomplete. They do not create a new durable policy or definition block.

## Start and observe

1. Construct HttpHost::new_with_application_preparing using the actual owned durable Store, authenticator, current policy and immutable native application configuration. Keep direct Store and service aliases isolated from mutation throughout preparation.
2. Install its router if liveness is needed during preparation. GET /healthz remains live; readiness and all ordinary requests stay closed before authentication, body consumption or Store work. ShutdownPhase::Serving describes the absence of a shutdown cut; use preparation_status to observe startup completion.
3. On the embedding's existing Tokio executor, call begin_preparation once with the trusted resolver and finite StartupPreparationLimits. Defaults are 32 pages and 64 recovery candidates; valid maxima are 64 pages and 256 candidates. A page examines at most 32 rows, including terminal history. A continuation at the page cap is incomplete, even when the next page could contain only terminal records.
4. Observe the resource-free handle or wait for a bounded duration up to 300 seconds. A zero wait samples the state. Invalid waits do not change admission. Pending, a timed wait or a dropped waiter does not finish preparation, cancel callbacks or release physical ownership. Trusted application code and destructors can remain held indefinitely.
5. Obtain serving_host only after actual Complete. Runnable means durable adoption and owned local producer publication; the callback can still be running. Parked work retains its real approval material; conservative Blocked work retains its unknown outcome. AttemptLimit leaves preparation incomplete. Complete is neither terminal settlement nor perpetual authority.

## Isolation and incomplete outcomes

This is one bounded pass through individually coherent pages, not a global Store snapshot or a durable recovery queue. The embedding must prevent direct Store, service, raw-writer and other-coordinator mutations during preparation. Only this coordinator and its already accepted workers may update their existing rows; they must not insert, delete or reorder execution IDs or change another candidate's actor or Definition. The host's private gate closes its own paths and cannot enforce isolation against arbitrary external Store aliases. A stale observation stops incomplete rather than silently refreshing, skipping or rescanning it.

Incomplete leaves ordinary admission closed. Some earlier admissions may have committed and can continue under their actual owned leases. Capacity exhaustion does not wait for callbacks, abort them or reset budgets to make the pass succeed. There is no batch rollback or automatic retry. After a shutdown cut, a held resolver returning later cannot obtain a new recovery lease or reopen admission; a lease that won the cut can finish its earlier exact operation.

The admitted counter counts confirmed returned adoption observations only. The candidate counter counts each prior-owner candidate before authority resolution; successful disposition counters classify confirmed results separately. RecoveryAdmissionError::adoption returning Some records an actual returned durable receipt. None means no confirmed receipt, not no commit: a custom Store can commit and then fail or panic before returning. Such ambiguity faults the host and leaves preparation incomplete. Do not infer rollback, skip current-owner work, retry or invent a replacement fence from the absent receipt. Recovery identities are sensitive embedding data and are not automatic diagnostic output.

## Retire resources

Call begin_shutdown and wait_quiescent separately from preparation observation. Stop and normally join the listener, allow actual finite callbacks and cleanup to return, and drop all external host/router/resolver/application/Store aliases. A retained startup handle, shutdown handle or recovery DTO carries no Store ownership. Actual next-owner writer opening requires physical retirement; a callback marker, timeout, zero active run count, directory deletion or forced cleanup does not establish it.

The implementation uses the same owned recovery driver as the existing Response adapter. No new HTTP recovery route or serialized startup report is introduced. Direct asynchronous poll/cancellation/destructor ownership boundaries are qualified with private lifecycle tests and independent review; public tests cover the reachable resolver, Store, callback and observation boundaries. Aborting a public waiter is not cancellation of the private coordinator.

This scope does not close full RFC 11.4, historical-definition coexistence, durable actor/policy/definition confinement, provider reconciliation, the broad crash matrix, M0, CP-010 or other open core milestones. Original TypeScript client and Node evidence remains historical; it is not extended by this startup API.
