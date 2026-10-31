# Your first saved permission

Build a local Rust example, approve its report task with Always, then reuse that
permission after a server restart. The callback returns the literal
`{"published":true}`; it publishes no files and makes no network calls. No provider
account or Node installation is needed.

## Build and run the automatic check

Run all commands from the repository root. Install the pinned Rust **1.93.1**
toolchain (see `rust-toolchain.toml`) and Python 3. This example and its process
checks use Unix signals and file locks; existing process qualification is on
macOS arm64, not every platform. The optional manual walkthrough also uses curl.

```sh
cargo build -p b4-http --example serve_permission_reuse --locked
export B4RUN_EXAMPLE="$(python3 -c 'import os; print(os.path.abspath(os.path.join(os.environ.get("CARGO_TARGET_DIR", "target"), "debug/examples/serve_permission_reuse")))')"
python3 -B scripts/check_http_always_example.py --nested-inputs \
  --example "$B4RUN_EXAMPLE"
```

The first build may fetch dependencies. Add `--offline` to the build only when
those dependencies are already cached. The executable path above handles an
exported `CARGO_TARGET_DIR`, including a relative path. If Cargo configuration or
other build options select a different target directory, set `B4RUN_EXAMPLE` to
that built executable's absolute path instead. The Python check does not build.

A successful check prints JSON with `status: "ok"`, `original: "Complete5"`,
`reuse: "Complete4"`, `normal_owners: 2` and `removed: true`. It starts with its
own temporary database, creates a thread and a genuine Always permission over
HTTP, completes the original nested-input run, and normally retires that server.
A second server reopens the database and reuses the saved permission with a
different nested input. The check verifies committed attempts/results, integrity
and ownership release, then removes its temporary directory. This is the scoped
local example check; see the [detailed example runbook](runbooks/permission-reuse-example.md)
for qualification limits.

## Optional: approve it yourself in two terminals

Keep both terminals open. Do not enable shell tracing, curl verbose/trace output,
or log authorization values. The commands generate a demo bearer token and keep
the pending grant in memory without printing either secret.

### Terminal 1: start a fresh server

Use the same repository-root terminal as the build above, so `B4RUN_EXAMPLE` is
still set. This creates a fresh owned directory in the system temporary location,
separate from the checkout, and explicitly selects its initially absent `data`
subdirectory; it does not use the example's default persistent directory.

```sh
umask 077
export B4RUN_DEMO_DIR="$(python3 -c 'import tempfile; print(tempfile.mkdtemp(prefix="b4-quickstart-"))')"
python3 -c 'import os, pathlib, secrets; pathlib.Path(os.environ["B4RUN_DEMO_DIR"], "token").write_text(secrets.token_urlsafe(32))'
export B4RUN_TOKEN="$(cat "$B4RUN_DEMO_DIR/token")"
export B4RUN_DATA_DIR="$B4RUN_DEMO_DIR/data"
export B4RUN_PORT=0
printf 'Demo directory: %s\n' "$B4RUN_DEMO_DIR"
"$B4RUN_EXAMPLE" serve-approvals
```

The server announces `{"status":"ready","address":"127.0.0.1:..."}`.
Copy the nonsecret demo directory path and the actual announced address to
Terminal 2. Port zero chooses a free port, so do not assume port 3001.

### Terminal 2: create a thread and approve the parked run

Start at the same repository root. Separate terminals do **not** inherit each
other's variables: replace both placeholders below with Terminal 1's actual
values. Read the token from the private demo file, without displaying it.

```sh
export B4RUN_DEMO_DIR='/absolute/path/from-terminal-1'
export B4RUN_URL='http://127.0.0.1:actual-port'
export B4RUN_TOKEN="$(cat "$B4RUN_DEMO_DIR/token")"
export B4RUN_THREAD_ID="$(curl --silent --show-error --fail \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  -H 'Content-Type: application/json' -d '{"metadata":{}}' \
  "$B4RUN_URL/threads" | python3 -c 'import json, sys; print(json.load(sys.stdin)["thread_id"])')"
curl --silent --show-error --fail \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"route":"/reports#agent","input":{"month":"2026-09"}}' \
  "$B4RUN_URL/threads/$B4RUN_THREAD_ID/runs/wait"
```

The run returns `null` because it is waiting for approval. Fetch its actual pending
interrupt and grant, then send the exact Always resume shape. Approving Always
here authorizes only this example's harmless report task.

```sh
B4RUN_PENDING="$(curl --silent --show-error --fail \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  "$B4RUN_URL/threads/$B4RUN_THREAD_ID/pending_interrupts")"
printf '%s' "$B4RUN_PENDING" | python3 -c '
import json, sys
pending = json.load(sys.stdin)["interrupts"]
assert len(pending) == 1
item = pending[0]
json.dump({"route": "/reports#agent", "resume": [{"status": "resolved", "interruptId": item["interruptId"], "payload": "always", "grant": item["grant"]}]}, sys.stdout)
' | curl --silent --show-error --fail --no-buffer \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  -H 'Content-Type: application/json' -H 'Accept: text/event-stream' \
  --data-binary @- "$B4RUN_URL/threads/$B4RUN_THREAD_ID/resume"
unset B4RUN_PENDING
```

Wait for the stream to finish. Its `done` event contains
`{"output":{"published":true}}`: the original approved run has completed.

### Restart and reuse

In **Terminal 1**, press Ctrl+C and wait for the process to exit normally and the
shell prompt to return. Shutdown drains owned work before releasing the database.
Then restart in that same terminal, retaining its directory and token:

```sh
"$B4RUN_EXAMPLE" serve-approvals
```

In **Terminal 2**, update `B4RUN_URL` to the newly announced address; keep its
existing token and `B4RUN_THREAD_ID`. Submit another month to the same thread:

```sh
export B4RUN_URL='http://127.0.0.1:new-actual-port'
curl --silent --show-error --fail \
  -H "Authorization: Bearer $B4RUN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"route":"/reports#agent","input":{"month":"2026-10"}}' \
  "$B4RUN_URL/threads/$B4RUN_THREAD_ID/runs/wait"
```

This returns `{"published":true}` without another approval: a new run uses the
saved permission. Stop Terminal 1 with Ctrl+C and wait for normal exit again.
Your manual demo directory is retained for later reopening until you or the
operating system clean it up; these commands do not delete it.

For request and permission scope, see [HTTP Always approvals](runbooks/http-always-approvals.md).
The [example runbook](runbooks/permission-reuse-example.md) covers embedded seeding,
reuse-only serving, and the separate cancellation and deletion modes. Those modes
have distinct capabilities; this `serve-approvals` walkthrough does not enable them.
