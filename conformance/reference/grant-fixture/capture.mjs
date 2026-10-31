import assert from "node:assert/strict"
import { createRequire } from "node:module"
import { cp, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"
import { recordResponse, sha256 } from "../fixture/harness.mjs"
import { verifyReferenceRoot } from "../fixture/capture.mjs"
import { collectRuntimeArtifacts, createRuntimeArtifactReceipt } from "../fixture/runtime-artifacts.mjs"
import { summarizeGrantObservation } from "./projection.mjs"

const fixtureRoot = fileURLToPath(new URL(".", import.meta.url))

async function hashFiles(root, prefix = "") {
  const files = []
  for (const entry of (await readdir(join(root, prefix), { withFileTypes: true })).sort((a, b) => a.name.localeCompare(b.name))) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name
    if (entry.isDirectory()) files.push(...await hashFiles(root, path))
    else if (entry.isFile()) {
      const bytes = await readFile(join(root, path))
      files.push({ path, sha256: sha256(bytes), bytes: bytes.length })
    }
  }
  return files
}

export async function capture({ referenceRoot, output }) {
  const reference = await verifyReferenceRoot(referenceRoot)
  const artifacts = await createRuntimeArtifactReceipt({ referenceRoot, observationTiming: "capture_time_inventory" })
  const runtimeEntry = "packages/cli/dist/lib/dev/runtime-fetch-handler.js"
  const runtimeBytes = await readFile(join(referenceRoot, runtimeEntry))
  const effectiveDependencyVersions = {}
  for (const [name, path] of [
    ["@b4run/cli", "packages/cli/package.json"],
    ["@b4run/sdk", "packages/cli/node_modules/@b4run/sdk/package.json"],
    ["@b4run/core", "packages/cli/node_modules/@b4run/core/package.json"],
    ["@b4run/langchain", "packages/cli/node_modules/@b4run/langchain/package.json"],
    ["@b4run/sqlite-storage", "packages/cli/node_modules/@b4run/sqlite-storage/package.json"],
    ["@langchain/core", "packages/cli/node_modules/@langchain/core/package.json"],
    ["@langchain/langgraph", "packages/cli/node_modules/@langchain/langgraph/package.json"],
    ["langchain", "packages/cli/node_modules/langchain/package.json"],
    ["typescript", "node_modules/typescript/package.json"],
  ]) effectiveDependencyVersions[name] = JSON.parse(await readFile(join(referenceRoot, path), "utf8")).version
  const require = createRequire(join(referenceRoot, "packages/cli/package.json"))
  const { BaseChatModel } = await import(pathToFileURL(require.resolve("@langchain/core/language_models/chat_models")))
  const { AIMessage, isToolMessage } = await import(pathToFileURL(require.resolve("@langchain/core/messages")))
  const { seedModelImporter } = await import(pathToFileURL(join(referenceRoot, "packages/langchain/dist/index.js")))
  const { createRuntimeFetchHandler } = await import(pathToFileURL(join(referenceRoot, runtimeEntry)))
  const appFiles = await hashFiles(join(fixtureRoot, "app"))
  const fixtureFiles = await hashFiles(fixtureRoot)
  const fixtureRevision = sha256(JSON.stringify(fixtureFiles))
  const temporaryRoot = await mkdtemp(join(tmpdir(), "b4-grant-reference-app-"))
  const appRoot = join(temporaryRoot, "app")
  const ledgerPath = join(temporaryRoot, "deployments.log")
  await cp(join(fixtureRoot, "app"), appRoot, { recursive: true })
  await symlink(join(referenceRoot, "node_modules"), join(appRoot, "node_modules"), "dir")
  await writeFile(ledgerPath, "")
  const previousLedger = process.env.B4_GRANT_REFERENCE_LEDGER
  process.env.B4_GRANT_REFERENCE_LEDGER = ledgerPath
  // Provider credentials/configuration must not reach the deterministic model.
  for (const name of ["OPENAI_API_KEY", "OPENAI_BASE_URL", "LANGSMITH_API_KEY", "LANGCHAIN_API_KEY", "LANGSMITH_TRACING", "LANGCHAIN_TRACING_V2"])
    delete process.env[name]
  let modelCalls = 0
  let importedModelPackages = 0
  class DeterministicChatModel extends BaseChatModel {
    constructor(options) {
      assert.deepEqual(Object.keys(options).sort(), ["maxRetries", "model"])
      super({})
    }
    _llmType() { return "independent-local-grant-reference" }
    bindTools() { return this }
    async _generate(messages) {
      modelCalls++
      let message
      if (messages.some(isToolMessage)) {
        message = new AIMessage({ id: "fixture-ai-final", content: "Decision recorded." })
      } else {
        const input = messages.at(-1)?.content
        assert.ok(input === "deploy to staging" || input === "deploy pair", "Unexpected fixture model input")
        const environments = input === "deploy pair" ? ["pair-a", "pair-b"] : ["staging"]
        message = new AIMessage({
          id: "fixture-ai-proposal",
          content: "",
          tool_calls: environments.map(env => ({ id: `fixture-call-${env}`, name: "deployProd", args: { env }, type: "tool_call" })),
        })
      }
      return { generations: [{ text: String(message.content), message }] }
    }
  }
  seedModelImporter(async specifier => {
    assert.equal(specifier, "@langchain/openai")
    importedModelPackages++
    return { ChatOpenAI: DeterministicChatModel }
  })
  const originalFetch = globalThis.fetch
  let externalFetchAttempts = 0
  globalThis.fetch = async () => {
    externalFetchAttempts++
    throw new Error("Grant reference fixture forbids all external fetch calls")
  }
  let handler
  const observations = []
  const assertions = []
  const ledger = async () => (await readFile(ledgerPath, "utf8")).split("\n").filter(Boolean)
  const threadAccess = {
    fallback: ({ headers, action, thread }) => {
      if (headers["x-fixture-caller"] !== "owner") return { decision: "deny" }
      if (action === "create") return { decision: "allow", stamp: { owner: "owner" } }
      return thread?.access?.owner === "owner" ? { decision: "allow" } : { decision: "deny" }
    },
  }
  const open = async () => {
    handler = await createRuntimeFetchHandler({ appRoot, threadAccess, drainDeadlineMs: 250, apSseHeartbeatIntervalMs: 60_000 })
  }
  async function request(id, method, path, body, caller = "owner") {
    const text = body === undefined ? undefined : JSON.stringify(body)
    const req = new Request(`http://reference.invalid${path}`, {
      method,
      headers: { "x-fixture-caller": caller, ...(text === undefined ? {} : { "content-type": "application/json" }) },
      ...(text === undefined ? {} : { body: text }),
    })
    const response = await recordResponse(await handler.fetch(req))
    const observation = {
      id, evidence_classification: "runtime_observation",
      request: { method, path, headers: [...req.headers.entries()], ...(text === undefined ? {} : { body_utf8: text, body_base64: Buffer.from(text).toString("base64"), body_sha256: sha256(text) }) },
      response, ledger: await ledger(), model_calls: modelCalls,
    }
    observations.push(observation)
    return observation
  }
  const pending = (id, thread) => request(id, "GET", `/threads/${thread}/pending_interrupts`)
  const entriesOf = observation => JSON.parse(observation.response.body_utf8).interrupts
  const resume = (id, thread, entries, route = "/park#agent", caller) => request(id, "POST", `/threads/${thread}/resume`, { route, resume: entries }, caller)
  const decision = (entry, payload = "once") => ({ interruptId: entry.interruptId, status: "resolved", payload, grant: entry.grant })
  async function park(id, thread, content = "deploy to staging") {
    const result = await request(id, "POST", `/threads/${thread}/runs/stream`, { route: "/park#agent", input: { messages: [{ id: "fixture-user", role: "user", content }] } })
    assert.equal(result.response.status, 200)
    assert.ok(result.response.sse_frames.some(frame => frame.event === "interrupt"), "Real permission gate must emit an interrupt")
  }
  try {
    await open()
    const thread = "t-reference-grant-once"
    await park("once.park", thread)
    const first = await pending("once.pending", thread)
    const again = await pending("once.pending-again", thread)
    assert.equal(first.response.body_base64, again.response.body_base64, "Pending disclosure must be stable and re-readable")
    const [entry] = entriesOf(first)
    assert.ok(entry?.grant?.startsWith("b4ag_"))
    assert.equal(entry.value.grant, entry.grant)
    assert.equal(entry.value.interruptId, entry.interruptId)
    assert.equal((await ledger()).length, 0)
    assertions.push("actual_permission_gate_parked_with_generated_grant_before_local_tool_execution", "pending_disclosure_is_byte_identical_on_repeat")
    await request("once.thread-parked", "GET", `/threads/${thread}`)
    await request("once.pending-unauthorized", "GET", `/threads/${thread}/pending_interrupts`, undefined, "other")
    await resume("once.resume-unauthorized", thread, [decision(entry)], "/park#agent", "other")
    await resume("once.resume-missing", thread, [{ interruptId: entry.interruptId, status: "resolved", payload: "once" }])
    await resume("once.resume-malformed", thread, [{ ...decision(entry), grant: "not-a-grant" }])
    await resume("once.resume-wrong", thread, [{ ...decision(entry), grant: "b4ag_not-the-generated-grant" }])
    await resume("once.resume-wrong-decision", thread, [{ ...decision(entry), payload: "unexpected" }])
    await resume("once.resume-wrong-id", thread, [{ ...decision(entry), interruptId: "perm-not-pending" }])
    await resume("once.resume-extra-field", thread, [{ ...decision(entry), extra: true }])
    assert.equal((await ledger()).length, 0, "All refusal probes must leave the local tool unexecuted")
    const preRestart = await pending("once.pending-before-restart", thread)
    await handler.close()
    handler = undefined
    await open()
    const postRestart = await pending("once.pending-after-restart", thread)
    assert.equal(preRestart.response.body_base64, postRestart.response.body_base64, "Real persisted checkpoint must restore the original prompt/grant/resume pair")
    assertions.push("full_handler_restart_restores_original_pending_prompt_grant_and_resume_key")
    await resume("once.resume-accepted", thread, [decision(entry)])
    assert.deepEqual(await ledger(), ["staging"])
    await pending("once.pending-completed", thread)
    await resume("once.resume-replay", thread, [decision(entry)])
    assert.deepEqual(await ledger(), ["staging"], "Replay must not run the local tool twice")
    await request("once.thread-completed", "GET", `/threads/${thread}`)
    assertions.push("once_calls_local_tool_exactly_once_and_replay_does_not_execute")

    const denyThread = "t-reference-grant-deny"
    await park("deny.park", denyThread)
    const [denyEntry] = entriesOf(await pending("deny.pending", denyThread))
    await resume("deny.resume", denyThread, [decision(denyEntry, "deny")])
    await pending("deny.pending-completed", denyThread)
    await resume("deny.resume-replay", denyThread, [decision(denyEntry, "deny")])
    assert.deepEqual(await ledger(), ["staging"])
    assertions.push("deny_and_deny_replay_do_not_execute_local_tool")

    const cancelThread = "t-reference-grant-cancelled"
    await park("cancelled.park", cancelThread)
    const [cancelEntry] = entriesOf(await pending("cancelled.pending", cancelThread))
    await resume("cancelled.resume", cancelThread, [{ interruptId: cancelEntry.interruptId, status: "cancelled", grant: cancelEntry.grant }])
    await pending("cancelled.pending-completed", cancelThread)
    await resume("cancelled.resume-replay", cancelThread, [{ interruptId: cancelEntry.interruptId, status: "cancelled", grant: cancelEntry.grant }])
    assert.deepEqual(await ledger(), ["staging"])
    assertions.push("cancelled_without_payload_maps_to_deny_and_does_not_execute_local_tool")

    const pairThread = "t-reference-grant-pair"
    await park("pair.park", pairThread, "deploy pair")
    const pair = entriesOf(await pending("pair.pending", pairThread))
    assert.equal(pair.length, 2)
    await resume("pair.resume-incomplete", pairThread, [decision(pair[0])])
    await resume("pair.resume-one-invalid", pairThread, [decision(pair[0]), { ...decision(pair[1]), grant: "b4ag_invalid-second-grant" }])
    assert.deepEqual(await ledger(), ["staging"])
    await pending("pair.pending-after-refusals", pairThread)
    await resume("pair.resume-accepted", pairThread, pair.map(item => decision(item)))
    assert.deepEqual((await ledger()).slice(1).sort(), ["pair-a", "pair-b"])
    await resume("pair.resume-replay", pairThread, pair.map(item => decision(item)))
    assert.equal((await ledger()).length, 3)
    assertions.push("exact_pending_set_and_all_grants_are_verified_before_delivery", "two_valid_decisions_execute_two_independent_local_calls_once")

    const routeThread = "t-reference-grant-route-provenance"
    await park("route.park", routeThread)
    const [routeEntry] = entriesOf(await pending("route.pending", routeThread))
    const routeResume = await resume("route.resume-supplied-route-differs", routeThread, [decision(routeEntry)], "/other#agent")
    assert.equal(routeResume.response.status, 200)
    assert.equal((await ledger()).length, 4)
    await request("route.thread-completed", "GET", `/threads/${routeThread}`)
    assertions.push("supplied_resume_route_is_ignored_in_favor_of_recorded_parked_route_provenance")
  } finally {
    try { await handler?.close() } finally {
      globalThis.fetch = originalFetch
      if (previousLedger === undefined) delete process.env.B4_GRANT_REFERENCE_LEDGER
      else process.env.B4_GRANT_REFERENCE_LEDGER = previousLedger
      await rm(temporaryRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
    }
  }
  assert.equal(externalFetchAttempts, 0)
  assert.equal((await collectRuntimeArtifacts(referenceRoot)).inventory_sha256, artifacts.inventory_sha256, "Built runtime artifact inventory changed during capture")
  await verifyReferenceRoot(referenceRoot)
  const summary = observations.map(summarizeGrantObservation)
  const recording = {
    format_version: 1, evidence_classification: "runtime_observation", transport: "in_process_fetch",
    protocol_surface: "Actual pinned Agent Protocol Fetch handler, required approval grants, ordinary agent route",
    reference, runtime_entry: { path: runtimeEntry, sha256: sha256(runtimeBytes) },
    runtime_artifacts: { sidecar_file: "runtime-artifacts.json", inventory_sha256: artifacts.inventory_sha256, runtime_core: artifacts.runtime_core },
    execution: { node_version: process.version, platform: process.platform, arch: process.arch, external_fetch_attempts: externalFetchAttempts, provider_calls: 0, model_calls: modelCalls, deterministic_model_imports: importedModelPackages },
    effective_dependency_versions: effectiveDependencyVersions,
    fixture_application: { revision: fixtureRevision, files: appFiles, fixture_files: fixtureFiles, configuration: { approvals: { grants: "required", grantTtlMs: 60_000 } }, routes: ["/park#agent"], provider_seam: "Production seedModelImporter returns deterministic local BaseChatModel constructor; no provider module imported", dependency_resolution: "Fixture app/node_modules symlinks to the fresh pinned checkout node_modules; no original dawn artifacts" },
    normalization: "Raw bytes are untouched. comparison_projection is a separately named limited projection retaining status, exact refusals, prompt shape, event names and local ledger. It omits opaque generated grant/interrupt/checkpoint identifiers and output message internals; it is not whole-response equality.",
    characterization_scope: "Genuine production park/minter/checkpointer/pending/resume paths for selected required Once/Deny/cancelled, refusal, exact-set and handler-restart cases. No Always/off/optional, expiry, races, process-kill or post-consumption crash claims; CP-005/009 and whole-protocol equivalence remain open.",
    assertions, observations, comparison_projection: summary,
    comparison_projection_sha256: sha256(JSON.stringify(summary)),
  }
  await mkdir(output, { recursive: true })
  const bytes = Buffer.from(`${JSON.stringify(recording, null, 2)}\n`)
  await writeFile(join(output, "grant-observations.json"), bytes, { flag: "wx" })
  artifacts.recording_links = [{ path: "grant-observations.json", path_base: "sidecar_directory", sha256: sha256(bytes), runtime_wrapper_sha256: recording.runtime_entry.sha256 }]
  await writeFile(join(output, "runtime-artifacts.json"), `${JSON.stringify(artifacts, null, 2)}\n`, { flag: "wx" })
  return { observations: observations.length, recording_sha256: sha256(bytes), fixture_revision: fixtureRevision, comparison_projection_sha256: recording.comparison_projection_sha256, inventory_sha256: artifacts.inventory_sha256, assertions: assertions.length }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2)
  assert.deepEqual([args[0], args[2]], ["--reference-root", "--output"])
  assert.equal(args.length, 4)
  capture({ referenceRoot: resolve(args[1]), output: resolve(args[3]) })
    .then(receipt => process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`))
    .catch(error => { process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1 })
}
