# Try durable permissions in Rust

The `serve_permission_reuse` Cargo example uses a small local report callback and SQLite. Its `serve-approvals` mode accepts HTTP Always approval and executes the original run. The separate `seed` command provisions through embedded approval; `serve` remains reuse-only. The explicit `serve-cancellation` mode adds cooperative permission cancellation under the V7 host profile. The separate `serve-deletion` mode selects V8 settled-history deletion with the same cooperative callback. The callback returns `{"published":true}` and performs no file or network effect.

## Start with HTTP approval

Start the explicit approval mode without running `seed`:

```sh
export B4RUN_DATA_DIR="$PWD/.b4-permission-data"
export B4RUN_TOKEN='choose-a-local-token'
cargo run -p b4-http --example serve_permission_reuse --locked --offline -- serve-approvals
```

The server prints its loopback address (port3001 by default; `B4RUN_PORT=0` selects a free port). Use that address and the bearer token to create a thread with `POST /threads` and body `{"metadata":{}}`. Submit a run to `/threads/{thread_id}/runs/wait` with `{"route":"/reports#agent","input":{"month":"2026-09"}}`. It returns null while parked. Read the actual pending interrupt/token and submit Always using the [HTTP approval workflow](http-always-approvals.md).

The original run completes with the report result. Press Ctrl+C and wait for normal server exit. Restart `serve-approvals` with the same directory/token and run the same thread with another month: the stored permission is reused without another approval. This mode publishes version4 capabilities. It keeps permission-aware cancellation, deletion and recovery unsupported.

Nested JSON works through the same approval flow. For example, submit this body to the run endpoint above:

```json
{"route":"/reports#agent","input":{"report":{"title":"月次 Café 🚀","sections":[{"name":"north","values":[1,2,null],"options":{"enabled":true,"limit":3,"cursor":null}},{"name":"南","values":[3,4,5],"options":{"enabled":false,"limit":null,"cursor":"次"}}]},"options":{"notify":["ops","分析"],"dryRun":false,"note":null}}}
```

After Always completes and the server retires normally, restart with the same directory and submit the distinct reuse body `{"route":"/reports#agent","input":{"report":{"month":"十一月","publish":true},"options":{"notify":["分析",null],"cursor":null}}}` to the same thread. The focused nested process probe below exercises both requests. It measures compact UTF-8 semantic input (294 bytes/30 values/depth 5 and 98 bytes/9 values/depth 3), the actual default-ASCII/spaced HTTP request bodies, and the real parked acceptance snapshot after retirement. Parked canonical accounting includes object keys; it must exceed 91 nodes and remain within 512 nodes/4,096 bytes. This qualifies these particular inputs with committed attempts/results; individual maxima, larger cancellation/deletion/recovery and callback instrumentation are outside this probe's scope.

## Provision through the embedded API

From the repository root, choose a data directory and local bearer token:

```sh
export B4RUN_DATA_DIR="$PWD/.b4-permission-data"
export B4RUN_TOKEN='choose-a-local-token'
cargo run -p b4-http --example serve_permission_reuse --locked --offline -- seed
```

Copy the returned `thread_id`. Seed creates a new UUID thread each time it is invoked, approves its one report task, cancels the original Ready run before callback entry, and closes the database. The saved permission remains available for later runs of that thread.

Start the server in the same terminal:

```sh
cargo run -p b4-http --example serve_permission_reuse --locked --offline -- serve
```

It prints JSON containing its actual loopback address. The default port is 3001; `B4RUN_PORT=0` asks the operating system to select a free port. In another terminal, use the same token and the returned thread ID:

```sh
export B4RUN_TOKEN='choose-a-local-token'
export B4RUN_THREAD_ID='paste-the-seeded-thread-id'
curl --fail-with-body \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"route":"/reports#agent","input":{"month":"2026-09"}}' \
  "http://127.0.0.1:3001/threads/$B4RUN_THREAD_ID/runs/wait"
```

A matching saved permission returns `{"published":true}` without parking for another human decision. Press Ctrl+C in the server terminal to stop admission, drain owned work and retire the listener before releasing the database. Start `serve` again with the same data directory and token, then repeat the request with another month. Each request has a fresh execution, turn, attempt and permission-use identity while retaining the original permission provenance.

The example's fixed local actor and policy are shared by all modes. Seed, reuse-only and approval modes retain application-1; cancellation and deletion modes use the distinct application-cancellation-1 revision. It accepts one Pure task requiring approval and uses the host's default 4,096-byte input limit, value depth 16, 256 values and fixed 65,536-byte output ceiling. Later permission-record and cumulative-account checks can still refuse inputs. A verified permission miss follows the existing Once/Deny pending/resume flow. HTTP Always creation is outside the reuse-only `serve` profile. Cancellation, recovery, deletion and confinement mutations remain outside the reuse-only and approval serving profiles. The explicit cancellation profile adds only the qualified permission cancellation paths.

## Run the process acceptance check

Build the example and pass its actual executable path to the check:

```sh
cargo build -p b4-http --example serve_permission_reuse --locked --offline
python3 -B scripts/check_permission_reuse_example.py \
  --example target/debug/examples/serve_permission_reuse
python3 -B scripts/check_http_always_example.py \
  --example target/debug/examples/serve_permission_reuse
python3 -B scripts/check_http_always_example.py --nested-inputs \
  --example target/debug/examples/serve_permission_reuse
```

For the finite 2 KiB ASCII text case, build freshly and pass the explicit executable:

```sh
cargo build -p b4-http --example serve_permission_reuse --locked --offline
python3 -B scripts/check_http_always_example.py --text-inputs \
  --example "$PWD/target/debug/examples/serve_permission_reuse"
```

This profile submits distinct `notes` strings of 2,048 ASCII/UTF-8 bytes (`a` for the original run, `b` for reuse). Each compact input is 2,060 bytes, two values and depth 1. It exercises HTTP Always, normal owner retirement, reopening the same database and saved permission reuse under a second normally retired owner. After the first retirement it inspects the actual acceptance snapshot, including both input copies, and requires more than 4,096 and at most 8,192 compact UTF-8 bytes, at most 512 canonical nodes and depth at most 16. Its single success JSON reports semantic geometry, exact default `json.dumps` transport-body byte counts and actual parked geometry, with committed attempts/results as evidence. The `--text-inputs` and `--nested-inputs` profiles are mutually exclusive. This finite case does not qualify cancellation, deletion or larger maxima.

Use the corresponding executable path if `CARGO_TARGET_DIR` changes your build location. Neither check builds anything. The existing reuse check seeds a temporary database, runs two separate server owners with authenticated requests, checks real Complete4/Use records and unchanged original permission records, requires normal interrupt shutdown and listener/database-lock retirement, and removes the database after integrity checks. It also checks invalid configuration and anonymous requests.

Both process checks passed freshly on macOS arm64 with Rust1.93.1, along with the locked/offline example build, formatting and strict example-target lint. The new check starts from an empty directory, creates its thread and Always permission over HTTP, observes original Complete5 with a committed task result, retires owner1 normally, and verifies a genuine Use/Complete4 under owner2. It checks immutable provenance, database integrity, owner-lock/listener release and temporary cleanup. Process checks use Unix signals/file locks; other platforms are not qualified. They observe committed attempts/results and do not instrument callback counts.

The [HTTP example qualification](../../conformance/permissions/http-always-example/qualification.json) binds these fresh process checks and the actual binary. Production libraries/contracts are unchanged in this increment. Their separate [HTTP Always qualification](../../conformance/permissions/http-always/README.md) records the full 1,567-pass regression; this example increment does not claim a fresh full workspace rerun. The earlier [seed/serve qualification](../../conformance/permissions/reuse-example/qualification.json) retains its original scope.

## Try cooperative cancellation

Start the explicit cancellation mode with its own local data directory:

```sh
export B4RUN_DATA_DIR="$PWD/.b4-permission-cancellation-data"
export B4RUN_TOKEN='choose-a-local-token'
cargo run -p b4-http --example serve_permission_reuse --locked --offline -- serve-cancellation
```

This mode publishes V7 capabilities and uses application-cancellation-1. Create a thread, submit a run and approve Always through the same [HTTP approval workflow](http-always-approvals.md), using input `{"month":"2026-09","wait_for_cancel":true}`. Keep the resume stream open. The server prints `{"status":"callback_entered","thread_id":"..."}` when the callback body starts waiting. In another terminal, send an authenticated cancellation request to that actual thread:

```sh
export B4RUN_TOKEN='choose-a-local-token'
export B4RUN_THREAD_ID='paste-the-thread-id'
curl --fail-with-body \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  -X POST "http://127.0.0.1:3001/threads/$B4RUN_THREAD_ID/cancel"
```

Use the actual printed address if the configured port differs. The cancel endpoint returns an interrupted response; an attached stream receives one cancelled Done, while a waiting run request returns `run_cancelled`. The callback acknowledges only accepted cancellation observed through its native observer. It has no report side effect or descendants. Without accepted cancellation, the wait expires after 30 seconds and returns the ordinary published result. Omit `wait_for_cancel` for an immediate normal result.

Press Ctrl+C and wait for normal server exit before restart. Shutdown waits for actual callback drain; Ctrl+C does not itself accept CancelRun, so an uncancelled waiting callback can take the remainder of its 30-second window. Restart the same mode and directory to reuse the genuine saved permission with another month. Its distinct application revision does not rewrite existing old-mode permissions. The original seed/reuse/approval modes retain their profiles and behavior.

V7 enables genuinely registered Ready, actual withheld Work and entered cancellation. The runnable process check observes real body entry and proves entered cancellation; the [library qualification](../../conformance/permissions/http-withheld-cancellation/README.md) separately proves the pre-entry paths. Process timing alone does not demonstrate withdrawal before entry. Recovery, deletion and automatic callback or storage replay remain outside `serve-cancellation`.

Build the example, then pass its actual executable to the new check:

```sh
cargo build -p b4-http --example serve_permission_reuse --locked --offline
python3 -B scripts/check_permission_cancellation_example.py \
  --example "$PWD/target/debug/examples/serve_permission_reuse"
```

Pass an absolute executable path; if `CARGO_TARGET_DIR` changes the output location, use that absolute location instead. The check uses three normally retired local server owners: HTTP Always with original cancellation, genuine saved-use cancellation, then normal saved reuse. SQLite inspection occurs after each owner exits. It checks real callback-entry events, terminal/journal revisions, permission provenance, owner/listener release and owned temporary cleanup. See the [example qualification](../../conformance/permissions/http-cancellation-example/README.md) for actual execution evidence and limits.

## Try settled-history deletion

Start the explicit deletion mode with a fresh demo directory:

```sh
export B4RUN_DATA_DIR="$PWD/.b4-permission-deletion-data"
export B4RUN_TOKEN='choose-a-local-token'
cargo run -p b4-http --example serve_permission_reuse --locked --offline -- serve-deletion
```

This mode publishes V8 and its narrow `durable_permission_settled_thread_deletion` capability. It shares application-cancellation-1 and the cooperative callback behavior with cancellation mode. The earlier serving modes retain their capabilities. Use the printed loopback address; port 3001 is the default and `B4RUN_PORT=0` selects an available port.

Create a thread and approve Always through the [HTTP approval workflow](http-always-approvals.md), using `{"month":"2026-09"}` for an immediate report result. Run it again to create a genuine saved Use. To try cancellation before deletion, use `wait_for_cancel:true` and follow the cancellation section above. A DELETE while that callback is active returns 409. A saved terminal state can also briefly remain busy until the actual worker releases its claim.

After the worker has drained, delete that thread using its actual ID:

```sh
export B4RUN_THREAD_ID='paste-the-thread-id'
curl --include --fail-with-body \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  -X DELETE "http://127.0.0.1:3001/threads/$B4RUN_THREAD_ID"
```

Use the actual printed address if it differs. An authorized settled deletion returns 204 with `Cache-Control: no-store` and an empty body only after the actual writer commits. It removes the thread's execution, journal, approval and permission history, including saved Uses. Other threads and their permissions remain available. Under this example's fixed resource policy, reads or repeated deletion of the now-absent thread return 404. That policy masking is separate from a committed confirmation.

Press Ctrl+C and wait for normal server exit before reopening the same directory. Shutdown retains ownership until workers finish. The deleted thread remains absent after restart, and another thread's saved permission can still be reused. The report callback publishes no external files or network changes for deletion to undo.

Run the focused process acceptance check against an already-built executable:

```sh
cargo build -p b4-http --example serve_permission_reuse --locked --offline
python3 -B scripts/check_permission_deletion_example.py \
  --example "$PWD/target/debug/examples/serve_permission_reuse"
```

Use an absolute executable path and adjust it for CARGO_TARGET_DIR when needed. The check owns a temporary directory and three normally retired server owners: genuine Once/Always/Uses plus live entered refusal and cancellation; normal reopen and actual settled deletion with exact preservation of a control history; then normal reopen and surviving saved reuse. SQL inspection occurs only after owner retirement. The harness gives normal retirement 45 seconds and bounds its requests, output, physical rows and cleanup.

The [example qualification](../../conformance/permissions/http-deletion-example/README.md) records the actual process scope. The [library qualification](../../conformance/permissions/http-settled-thread-deletion/README.md) separately establishes deterministic worker-retention, abandoned-caller and late-fault behavior. This process check does not establish pre-entry timing, live work retirement, recovery, larger structured inputs, maximum-size combinations, global SQL-time bounds or other platforms. The new example increment does not claim a fresh full-workspace suite.
