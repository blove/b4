# Explicit retained native Definitions

Status: verified within explicit approval-free retained native scope on macOS and Linux; broader restart and release gates remain open.

Install retained executable registrations when unfinished work must continue with the complete Definition originally recorded in storage. Construct `NativeApplication::new_with_history(identity, current, historical)` using the existing `RouteRegistration` type. The application is immutable after construction. Current registrations supply new work; retained registrations supply exact continuations.

## Assemble versions

Keep current nonempty, with one current registration per route. The combined current and historical count is at most 64. Route and execution revision pairs must be unique across both collections. Each registration retains the existing kernel validation, effect coverage and limits of 64 batches, tasks, bindings and attempts.

Historical registrations may include a removed route, but may not declare any task requiring approval, including a future batch. Current registrations can still require approvals. Configure current prompts through the existing `ApprovalHostConfig`; retained versions cannot borrow them.

Each retained registration contains its own callback bindings, optional input validator and output kinds. Reusing an effect name across versions does not share those bindings. `definition(route)` and `definitions()` expose only current registrations. `definition_exact(recorded)` requires the recorded route and execution revision pair followed by equality of the entire Definition. Changed batches, task policies, effect identities, order or attempt budgets refuse selection even when the revision label matches.

Use a new execution revision when executable behavior or its bindings change, and a new application revision when the installed assembly changes. Labels alone cannot prove that two deployments contain identical executable code; the embedding must retain the correct original implementation.

## Recover and accept new work

1. Install the immutable application through `HttpHost::new_with_application` or the existing opt-in `new_with_application_preparing` constructor. Native configuration fields and approval durability requirements remain unchanged.
2. For known-ID recovery, supply the original coherent `RecoveryTarget` and an independently verified principal. For bounded startup preparation, use the existing trusted resolver and isolate the stable execution-ID set from other writers throughout the pass.
3. Recovery selects the exact complete recorded Definition after renewing ordinary origin, application scope, current policy and original fences. A matching retained version can drain even when its route was removed from current registrations. Missing or changed support keeps the existing healthy refusal; startup remains Incomplete. It does not create a new durable definition or policy block.
4. New authenticated run requests select current registrations only. New work on a historical-only removed route refuses. Input validation runs for new work as before; recovery and resume do not revalidate previously admitted input. The portable `validate_input(recorded, input)` method can explicitly select a retained validator.
5. Matching native resume policy receives the original recorded route and execution revision. Executable selection and all existing stable-park, actor, policy and fence checks still apply. Historical approval-free Accepted work does not become a valid approval park. Actual current-version Once, Deny, pending grants and TTL handling use the existing current approval service.

Invocation context distinguishes the installed assembly from the recorded executable. A retained execution-v1 callback in installed assembly-v2 receives `application_revision=assembly-v2` and `execution_revision=v1`. The Store does not persist an original assembly revision. Actor, tenant, owner generation and task membership checks continue to apply.

## Observe and retire

Recovery uses the selected version's output kinds. Live attachment forwards that producer's publication, while inactive attachment retains its fixed null terminal. A Void native primary run or recovery projection is `{}`; inactive attachment retains its finite `{"output":null}` terminal projection. Live attachment remains bound to its original publisher, coherent Definition and renewed authority. Reconnect does not retarget a successor or provide an earlier publisher's missing history.

A dropped recovery caller, response, resume request or startup waiter does not release a running policy job or callback. Counted policy jobs return only workflow metadata for the new selection; retained registrations are selected under the host's owned request or worker carrier. Trusted callbacks, validators and destructors can retain resources until they actually return.

Use the existing `begin_shutdown` and separate `wait_quiescent`, normally stop and join the listener, allow callbacks and cleanup to return, and drop every external application, host, router and Store alias. Physical retirement is established by absent Store/application Weak references and successful opening of the real next-owner writer. A callback marker, timeout, zero counter or forced cleanup alone does not establish retirement. Resource-free startup and recovery observations can remain live afterward.

This capability provides explicit historical native drain within RFC11.5. Durable confinement of incompatible or revoked-policy work, the full RFC11.4 restart algorithm, provider reconciliation, the broad crash matrix and open core release gates remain separate work. Pure and Idempotent recovery policies are trusted declarations, not external exactly-once guarantees. Original TypeScript client and Node unit-suite evidence remains historical.

## Scoped verification

Actual scoped verification: macOS and Linux each passed 1084 Rust test entries across 98 targets, with zero failures and three existing ignores. The 48 new named Rust tests ran once per platform; two nested historical child results are excluded. macOS passed 102 Python tests, strict lint, formatting and contract checks. See the [verification receipt](../../conformance/expected/historical-native-definitions-verification.json). The first restricted macOS attempt could not bind local sockets; the unchanged source passed the authorized rerun. The actual compilation input kept the new traceability rows pending; final status promotion follows accepted evidence.
