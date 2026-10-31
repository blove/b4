# Runnable HTTP permission deletion qualification

The explicit `serve-deletion` report example selects the adopted V8 HTTP settled-history assembly. See the [runnable instructions](../../../docs/runbooks/permission-reuse-example.md#try-settled-history-deletion) and [machine-readable record](qualification.json).

On macOS arm64 with Rust 1.93.1, every final check used the exact 1,080-file descriptor `1490ff5309b19853958f4385a89a6ba5f6f85a9670e7ebdf1f943e9195987783`. The build produced executable SHA-256 `008fb3dd955f54c3707b252d48668cfae05750e845558f02ab94d2a5b965a91a`; all subsequent checks retained that exact binary before and after execution. Complete before/after source maps match for every check. All commands finished within their finite limits, with confirmed process reaping and no source changes. The native delta is one example file and one new process probe; libraries, contracts, dependencies and the earlier probes remain unchanged.

| Check | Result |
| --- | --- |
| Locked/offline example build | Passed |
| New deletion process probe | Three normal owners; all eight children reaped; no forced cleanup |
| Existing seed/reuse probe | Passed; saved completion across normal owners |
| Existing HTTP Always probe | Passed; original execution and reopened saved Use |
| Existing cancellation probe | Passed; original and saved cancellation, later saved completion |
| Formatting and strict example-target lint | Passed |
| Metadata | Passed separately; no runtime execution credit |

The new process probe finished in 1.328 seconds. Its log SHA-256 is `6f04434e6520e14ee2af684b0c979ca26e74ae54e5db3083a78946fd2bd3d063`. The three real server owners establish:

1. Genuine HTTP ordinary Once, original Always, saved completion and entered saved cancellation populate every target-owned table. The target has four executions, 24 journal rows, two grants, one ordinary acceptance, one permission acceptance, one decision and two Uses. Another thread has a genuine permission history. Actual callback-entry output precedes a live DELETE 409 refusal and real cancellation. Normal SIGINT retirement supplies actual drain before SQL inspection.
2. A normal owner reopen retains historical provenance while advancing the current owner epoch. Authenticated DELETE returns 204/no-store with an empty body. After normal retirement, all target rows are absent from all eight owned tables, every surviving history row is byte-identical and metadata changes only by the expected normal owner-epoch advance. Missing/wrong auth and browser-origin guards refuse deletion. The example's fixed policy masks absent resources 404.
3. Another normal reopen observes the target's absence and executes a genuine saved Use on the surviving thread without a new approval. Its original permission provenance and old execution/journal rows remain byte-identical. The survivor grows from one execution/seven journal rows/no Uses to two executions/twelve journal rows/one Use, with the actual committed report result. Later absence is observation of the earlier committed deletion, not a reconstructed confirmation.

All three owners retire normally; all eight launched children and HTTP workers finish, owner locks and listeners release, database integrity passes and owned temporary data is removed. Five additional child launches check missing/empty/whitespace token and invalid/out-of-range port refusal before storage or readiness side effects. The probe bounds network deadlines, output/events, physical values/table bytes/rows and normal retirement. The 45-second retirement deadline belongs to the harness; the application retains its existing ownership-preserving quiescence loop. SQLite inspection uses query-only connections only after successful owner retirement and owner-lock acquisition. Positive permission data comes exclusively from real HTTP APIs.

Actual TDD used a freshly checked unchanged example executable, preserved separately at SHA-256 `f39576a61212d7e66df54b4c31d7847b818cd811d29b038af74834fc4cf85586`. The new probe first failed with the explicit missing-mode diagnostic before readiness. Its single child was reaped and temporary data removed. Failure cleanup is distinct from a qualifying normal owner; none of the positive business bodies executed in that RED. The exact corrected expectation source is unchanged between RED and final GREEN.

The example uses the same application-cancellation-1 identity and cooperative callback behavior as cancellation mode. Earlier modes retain their behavior. Month-only input, the existing controlled wait flag and empty thread metadata match established profiles. The callback returns a bounded published result and performs no external file or network effect.

This example increment does not claim a fresh full-workspace run. The unchanged libraries' separate [HTTP settled deletion qualification](../http-settled-thread-deletion/README.md) records that evidence and deterministic worker-retention, abandoned-caller and late-fault checks. The process probe proves entered live refusal and settled deletion across normal owners; it does not establish deterministic pre-entry timing, live work retirement, recovery, arbitrary larger inputs, maximum-size combinations, global SQL-time bounds or other platforms. No actual user data is deleted by these probes.
