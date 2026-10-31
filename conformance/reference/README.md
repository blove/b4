# Pinned TypeScript reference evidence

This partial source bundle pins [`cacheplane/b4run` at `8acac534ba6eb960b34de217b0da45b8405023af`](https://github.com/cacheplane/b4run/commit/8acac534ba6eb960b34de217b0da45b8405023af). Each captured file matches its upstream Git blob SHA; [manifest.json](manifest.json) also records its SHA-256 and byte length. The captured lockfile digest is `46be0b081db024914c6da5d861c9416392abeea3bd44cbc10b6fba0840494ca0`. The upstream [MIT license](source/LICENSE) accompanies the source.

**The pinned runtime has now been built and executed for 62 deterministic workflow cases. M0 remains open.** The [recording](recordings/2026-09-30-workflows/workflow-observations.json) retains exact request/response bytes, status, exposed Fetch headers, SSE frames, fixture revision and installed versions. A [second boot](recordings/2026-09-30-workflows/workflow-repeatability.json) reproduced all normalized outcomes. See [build provenance](recordings/2026-09-30-workflows/build-provenance.json): the runtime dependency graph passed; the full monorepo web build did not.

The capture invokes the actual built Node Fetch handler. It covers selected CP-001–004 workflow behavior; permission parks, grants, attachment, cancellation, client corpus and reference TCP transport remain unqualified. Source observations remain labeled separately and never become runtime observations merely because some fixture cases ran. Selected native comparisons are recorded separately by the HTTP increment.

The retained [repeat recording](recordings/2026-09-30-workflows/repeat/workflow-observations.json)
can be compared with the first using [compare-recordings.mjs](fixture/compare-recordings.mjs).
The repeatability receipt identifies both exact files, the script and dependency
hashes, and the JSON projection formula. A separate
[runtime artifact inventory](recordings/2026-09-30-workflows/runtime-artifacts.json)
hashes the built wrapper, runtime core and all 414 workspace `dist` JavaScript/JSON
files. It was collected after the retained recordings and verifies their wrapper
hashes; the original recordings did not contemporaneously hash transitive
artifacts. Future captures inventory these artifacts before and after execution.

## Existing local checkout

The TypeScript project lives at `/Users/blove/repos/dawn` (`~/repos/dawn`), with
remote `git@github.com:cacheplane/b4run.git`. The project name changed to b4run;
the local directory remains `dawn`. The Rust workspace is `/Users/blove/repos/b4`.

On 30 September 2026, the local working checkout was at
`260920ca2b6bf13ddd15839f6e9ec32ba4c90b3a`, while its Git object database also
contained the frozen baseline `8acac534ba6eb960b34de217b0da45b8405023af`.
All 23 captured files were verified against that exact local commit by Git blob
identity, SHA-256 and byte length. This adds source verification, not runtime
execution evidence. See [the local verification receipt](../expected/local-reference-verification.json).

The checkout has pre-existing local edits. Future characterization should obtain
the pinned commit in a separate checkout from these local Git objects, with
dependencies installed for that revision. Do not use the older working files or
existing build output as the frozen oracle. The local directory is a discovery
hint, not a required path on other machines.

## Source observations

The tables and early workflow discussion below describe the historical workflow
capture boundary. Later [grant](recordings/2026-10-01-grants/README.md) and
[lifecycle](recordings/2026-10-01-lifecycle/README.md) captures add selected executed
evidence without changing those original recordings. Lifecycle captures execute
77 requests twice with 11 assertions each and matching limited ordered
projections; cancellation/quiescence, deletion and attachment are now partially
characterized. CP-001–009 remain partial and CP-010 unresolved. Full suites,
provider execution, abrupt crash and complete client parity remain open.

| Area | Pinned evidence | Contract consequence |
| --- | --- | --- |
| Run parsing | [`validateApRunBody`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/runtime-fetch-core.ts#L4155) | Only `route` is validated at this layer. Absent `input` becomes `{}`; present null, scalar, or array is preserved to route execution. Unknown run root fields are ignored. |
| Create parsing | [`POST /threads`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/runtime-fetch-core.ts#L1489) | Empty/whitespace bodies are accepted. Source reads text and parses JSON without checking content type. The shared `isRecord` helper admits arrays; strict native object validation needs a recorded difference and runtime characterization. HTTP creation passes metadata to the store and generates the ID. |
| Errors | [`server-errors.ts`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/server-errors.ts#L5) | Envelope is `{error:{kind,message,details?,code?,docsUrl?}}`. AP operational codes commonly reside in `error.details.code`; the optional top-level registry code is a separate field. |
| SSE | [`stream-types.ts`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/runtime/stream-types.ts#L46) | `chunk` emits its data directly, without `messageId`. Tool and done events emit their named fields without `type`; tool `id` is optional. Frames end in a blank line. |
| Void output | [`runs-wait-output.test.ts`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/test/runs-wait-output.test.ts#L63) | **`source_test_observation`:** upstream tests specify wait `200` JSON `null`, while an equivalent POST stream drops undefined `output` and the encoder yields done `{}`. Those tests were read, not run. Idle attach instead explicitly yields done `{output:null}`. |
| State and pending | [`state`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/runtime-fetch-core.ts#L2051), [`pending projection`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/runtime-fetch-core.ts#L3459) | State uses read time for `created_at` and pending channel names for `next`. Pending response is `{interrupts:[{interruptId,resumeKey,value?,grant?}]}`, with `no-store`; internal aliases and malformed flags are not exposed. |
| Attach | [`attachment frames`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/runtime-fetch-core.ts#L3610) | Live state carries anchor values, input, internal merged turn frames, and empty interrupts. Idle state uses null anchor/input/start/turn, durable values or null, and prompts, followed by retry 1500–2500 ms and done. Capacity can emit `detached` before state. No public event cursor exists. |
| Resume and grants | [`strict resume body`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/cli/src/lib/dev/runtime-fetch-core.ts#L4249), [`grant shape`](https://github.com/cacheplane/b4run/blob/8acac534ba6eb960b34de217b0da45b8405023af/packages/sdk/src/interrupt-grants.ts#L242) | Exact root keys are `route` and `resume`. Resolved entries require once/always/deny payload; cancelled entries forbid payload. Grants are optional string fields structurally; later grant checks enforce authority. Wire shape checks prefix `b4ag_` and total length 6–128, not exact base64url syntax. |

The workflow recording confirms stream failure is done `{output:{error:string}}`
with HTTP 200, while the same exception produces an HTTP 500 execution error on
wait. It also confirms the void output distinction in the source-test observation
above. Explicit stream cancellation remains source-only: done
`{output:{cancelled:true}}`; wait cancellation instead uses `409` and
`details.code = run_cancelled`. Cancellation and permission parks still need
executed recordings for the remaining CP-003/004 work.

## Reproducing the baseline later

Use a complete isolated checkout at the pinned commit, preferably sourced from
the existing `~/repos/dawn` Git objects; this source subset omits transitive
workspace files. The remote-clone alternative below also obtains the baseline.
The pinned manifests declare Node `>=24.0.0`, pnpm `10.33.0`, TypeScript `7.0.2`,
and Vitest `4.1.11`. Frozen installation with lifecycle scripts disabled succeeded
with 1,626 packages in the isolated checkout. The root monorepo build completed
25 of 28 tasks, with three web/inspector helper-port failures. The scoped root
CLI dependency build then passed all 13 tasks using outputs freshly created in
that same checkout. These executed steps are recorded in the build receipt:

```sh
git clone https://github.com/cacheplane/b4run.git b4run-reference
git -C b4run-reference checkout --detach 8acac534ba6eb960b34de217b0da45b8405023af
cd b4run-reference
pnpm install --frozen-lockfile --ignore-scripts
CI=1 TURBO_TELEMETRY_DISABLED=1 pnpm build
CI=1 TURBO_TELEMETRY_DISABLED=1 pnpm build --filter=@b4run/cli...
```

The independent fixture invocation and reproducible comparison are documented in
[fixture/README.md](fixture/README.md). The original checkout's existing outputs
were not used. The full monorepo build is not qualified by the successful runtime
dependency build.

Captured upstream test sources cover direct SSE payloads, message identity
merging, void/falsy/unserializable wait output, strict resume envelopes,
attachment snapshots, and fetch handler behavior. Those upstream suites were
read, not executed here. Their source-derived test command remains unexecuted:

```sh
pnpm --filter @b4run/cli test -- stream-types.test.ts stream-message-identity.test.ts runs-wait-output.test.ts resume-endpoint.test.ts ap-attach-endpoint.test.ts
```

Additional pinned suites available upstream include `pending-interrupts-endpoint.test.ts`, `approval-grants-endpoint.test.ts`, `ap-attach-safety.test.ts`, and `runtime-fetch-parity.test.ts` under `packages/cli/test/`. The captured `test/runtime/run-agent-protocol.test.ts` contains an opt-in real-provider approval test and creates dependency manifests using `latest`; it needs a pinned fixture overlay before it can serve as a deterministic comparison oracle.

## Open characterization

CP-001–004 are `runtime_partial` after the 62 workflow cases; CP-005–010 remain
`runtime_unresolved`. Recorded parsing, input forwarding, route-local validation,
failure and void terminals resolve only the selected workflow subset. Remaining
work includes permission parks, cancellation, other route kinds and transport
boundaries, grant/error precedence, deletion, attachment, client state-field
dependence, grant disclosure and an explicitly AP-only client corpus. Preserve
the fixture revision/configuration and hash each additional recording before
qualifying it. M0 remains open.

## Executed workflow observations

The independently authored [fixture](fixture/README.md) has no model/provider calls.
Omitted run input becomes `{}`; explicit null/scalars/arrays survive. The incumbent
ignores unknown run root keys, accepts duplicate keys with the last value, and
does not require an application/json content type. Create accepts array root and
array metadata; those remain explicit native restrictions. Void wait returns
JSON null, while POST stream emits done `{}`. Falsy outputs are preserved.
A fixed workflow exception produces wait500 and stream200 with done.output.error;
both resulting threads settle idle. Raw records preserve generated identities and
timestamps; companion normalization replaces only top-level thread fields.
