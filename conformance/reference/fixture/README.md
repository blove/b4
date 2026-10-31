# Independent reference fixture

The `app` directory is an independently authored deterministic application with
seven plain workflow routes. It exercises the actual pinned runtime's discovery,
input forwarding, execution, errors, settlement and Agent Protocol responses.
There are no models, provider calls or mutable remote dependencies in this app.
`cases.json` contains exact authored request bodies, including duplicate keys;
it does not prescribe the incumbent's responses.

Run after installing and building a clean complete pinned checkout:

```sh
node --test conformance/reference/fixture/*.test.mjs
node conformance/reference/fixture/capture.mjs \
  --reference-root /path/to/pinned/b4run \
  --output /path/to/new/recording-directory
```

The harness refuses a different commit, tracked-source modifications, or changed
lockfile bytes before importing `packages/cli/dist/lib/dev/runtime-fetch-handler.js`.
That Node entry supplies the incumbent's normal filesystem and SQLite fallbacks.
The build must be fresh; entry and fixture hashes are included in the receipt.

The capture invokes the real handler with Fetch `Request` objects and records
status, all exposed headers and exact response-body bytes, their SHA-256 digests,
and decoded SSE frames. For each run it also reads the resulting thread resource.
This is **in-process Fetch execution**, not TCP or Node-server adapter qualification.
Body bytes are supplied as UTF-8 buffers so an absent `content-type` stays absent.
Any external Fetch attempt fails the capture.

Future captures also collect every regular JavaScript/JSON file under
`packages/*/dist` before import and verify the inventory again after execution.
They emit `runtime-artifacts.json`, including the wrapper and runtime core hashes.
This is a provenance superset; it does not claim every inventoried file is reached
by the fixture. For the original retained recordings, the separately generated
inventory is explicitly marked as observed after capture, with wrapper hashes
checked against both recordings.

Compare two retained recordings without executing the runtime again:

```sh
node conformance/reference/fixture/compare-recordings.mjs \
  --first conformance/reference/recordings/2026-09-30-workflows/workflow-observations.json \
  --second conformance/reference/recordings/2026-09-30-workflows/repeat/workflow-observations.json \
  --output conformance/reference/recordings/2026-09-30-workflows/workflow-repeatability.json
```

The comparison validates raw byte encodings and digests, then compares requests,
status, headers, normalized bodies, decoded SSE frames and resulting thread
resources. The receipt retains both input paths and hashes, script/dependency
hashes and the exact JSON projection formula. Application payload, header,
inventory or raw-byte corruption makes the comparison fail.

Only top-level generated `thread_id`, `created_at` and `updated_at` fields in thread
resources receive a normalized companion representation. Original bytes remain
available; similarly named application metadata and route output are preserved.
All workflow SSE bytes remain unchanged. Existing output files are never replaced.

The selected corpus covers CP-001 parser cases, CP-002 input forwarding and local
route validation, CP-003 void/failure output, and CP-004 paired workflow wait/stream
settlement. Permission parks, cancellation, client qualification and later CP
areas remain outside this capture. Neither authored fixture files nor passing
harness tests are reference-runtime evidence until `capture.mjs` completes against
the built pinned runtime. M0 and the full CP-001–010 tasks remain open.
