# Retained workflow evidence

`workflow-observations.json` is the first actual pinned runtime capture;
`repeat/workflow-observations.json` is a separately booted retained repeat.
Their 62 normalized observations agree according to `workflow-repeatability.json`.
The immutable raw recordings retain generated identities, timestamps, exact
request/response bytes, Fetch headers, status and SSE data.

Reproduce the comparison from the Rust workspace root:

```sh
node conformance/reference/fixture/compare-recordings.mjs \
  --first conformance/reference/recordings/2026-09-30-workflows/workflow-observations.json \
  --second conformance/reference/recordings/2026-09-30-workflows/repeat/workflow-observations.json \
  --output conformance/reference/recordings/2026-09-30-workflows/workflow-repeatability.json
```

`runtime-artifacts.json` inventories the runtime wrapper, the core and all regular
workspace `dist` JavaScript/JSON artifacts from the same isolated pinned checkout.
The wrapper hashes agree with both recordings. This inventory was collected after
the original captures, whose contemporaneous provenance recorded the wrapper
alone. Future captures verify a full inventory before and after execution.

Regenerate the post-capture inventory while that built checkout is available:

```sh
node conformance/reference/fixture/runtime-artifacts.mjs \
  --reference-root /path/to/pinned/b4run \
  --recording conformance/reference/recordings/2026-09-30-workflows/workflow-observations.json \
  --recording conformance/reference/recordings/2026-09-30-workflows/repeat/workflow-observations.json \
  --output conformance/reference/recordings/2026-09-30-workflows/runtime-artifacts.json
```

`build-provenance.json` distinguishes the failed full monorepo build (25 of 28
tasks passed; three web/inspector helper-port failures) from the successful scoped
CLI dependency build (13 of 13). The recordings qualify selected workflow paths
through in-process Fetch. Permission parks, cancellation, TCP transport and client
qualification remain open, as do the full characterization tasks and M0.
